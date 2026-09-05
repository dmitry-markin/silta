//! Shared daemon state: the client, the routing tables, the session registry.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use matrix_sdk::Client;
use silta::{
    config::Routing,
    protocol::{Cmd, CmdKind, CmdResult, DaemonMessage},
};
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::info;

use crate::outbound;

pub struct Daemon {
    pub client: Client,
    pub routing: Routing,
    pub registry: Registry,
    /// Messages older than this (from the first sync's replay) are ignored.
    pub started_at_ms: u64,
}

pub type Shared = Arc<Daemon>;

impl Daemon {
    pub fn new(client: Client, routing: Routing) -> Daemon {
        let started_at_ms =
            SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);
        Daemon { client, routing, registry: Registry::default(), started_at_ms }
    }

    /// Execute one command on behalf of a connected session.
    pub async fn execute(&self, session: &str, cmd: Cmd) -> CmdResult {
        match cmd.kind {
            CmdKind::Reply(reply) => outbound::reply(self, session, cmd.id, reply).await,
        }
    }
}

/// Outbound queue size per connected session.
const SESSION_QUEUE: usize = 256;

#[derive(Debug, Error)]
pub enum DeliverError {
    #[error("session is not connected")]
    NotConnected,
    #[error("session's outbound queue is full")]
    QueueFull,
}

/// Which sessions are connected, and the queue to each. A session name is claimed by
/// at most one connection; the claim is released when the connection's [`Claim`] drops.
#[derive(Clone, Default)]
pub struct Registry {
    inner: Arc<Mutex<HashMap<String, mpsc::Sender<DaemonMessage>>>>,
}

impl Registry {
    /// Claim a session for a connection. `None` if another connection holds it.
    pub fn claim(&self, session: &str) -> Option<(mpsc::Receiver<DaemonMessage>, Claim)> {
        let mut map = self.inner.lock().unwrap();
        if map.contains_key(session) {
            return None;
        }
        let (tx, rx) = mpsc::channel(SESSION_QUEUE);
        map.insert(session.to_owned(), tx);
        Some((rx, Claim { registry: self.clone(), session: session.to_owned() }))
    }

    pub fn is_connected(&self, session: &str) -> bool {
        self.inner.lock().unwrap().contains_key(session)
    }

    /// Queue a message for a session without waiting.
    pub fn deliver(&self, session: &str, message: DaemonMessage) -> Result<(), DeliverError> {
        let map = self.inner.lock().unwrap();
        let Some(tx) = map.get(session) else {
            return Err(DeliverError::NotConnected);
        };
        tx.try_send(message).map_err(|_| DeliverError::QueueFull)
    }
}

/// Held by the connection that owns a session; dropping it frees the name.
pub struct Claim {
    registry: Registry,
    session: String,
}

impl Drop for Claim {
    fn drop(&mut self) {
        self.registry.inner.lock().unwrap().remove(&self.session);
        info!(session = %self.session, "session released");
    }
}
