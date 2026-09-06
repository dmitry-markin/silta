//! Shared daemon state: the client, the routing tables, the session registry with its
//! reconnect backlogs, the delivery watermarks, and the typing refresh tasks.

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use matrix_sdk::{
    ruma::{OwnedRoomId, RoomId},
    Client, Room,
};
use silta::{
    backlog::Backlog,
    config::Routing,
    protocol::{Cmd, CmdKind, CmdResult, DaemonMessage, Event, EventKind, ResultError},
    replay::Watermark,
};
use thiserror::Error;
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{outbound, spool::Spool};

/// Messages kept for a session that is not connected, per session.
const BACKLOG_MAX: usize = 100;
/// How long the typing indicator is kept alive for one delivery at most.
const TYPING_MAX: Duration = Duration::from_secs(600);
/// The SDK re-sends a notice only after 3 s and the server expires it after 4 s; a
/// 1 s poll keeps it continuous at no network cost between sends.
const TYPING_POLL: Duration = Duration::from_secs(1);
const MARKS_FILE: &str = "delivered.json";

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

pub struct Daemon {
    pub client: Client,
    pub routing: Routing,
    pub registry: Registry,
    pub started_at_ms: u64,
    pub replay_window_ms: u64,
    pub spool: Spool,
    /// Told to every session in `welcome`: how long its plugin keeps received files.
    pub inbox_max_age_days: u64,
    /// The uid each session's plugin must connect as, by session name.
    pub users: HashMap<String, u32>,
    marks: Marks,
    typing: Arc<Mutex<HashMap<OwnedRoomId, TypingTask>>>,
    typing_generation: Mutex<u64>,
}

pub type Shared = Arc<Daemon>;

/// What happened to an inbound message.
#[derive(Debug)]
pub enum Dispatch {
    Delivered,
    /// The session is not connected; the message waits (this many are waiting now).
    Queued(usize),
    Dropped(String),
}

impl Daemon {
    pub fn new(
        client: Client,
        routing: Routing,
        state_dir: &Path,
        replay_window_secs: u64,
        spool: Spool,
        inbox_max_age_days: u64,
        users: HashMap<String, u32>,
    ) -> Daemon {
        let replay_window_ms = replay_window_secs.saturating_mul(1000);
        Daemon {
            client,
            routing,
            registry: Registry::new(replay_window_ms),
            started_at_ms: now_ms(),
            replay_window_ms,
            spool,
            inbox_max_age_days,
            users,
            marks: Marks::load(state_dir.join(MARKS_FILE)),
            typing: Arc::new(Mutex::new(HashMap::new())),
            typing_generation: Mutex::new(0),
        }
    }

    /// Execute one command on behalf of a connected session. `send_file` needs the
    /// transfer the connection received and is dispatched by the server instead.
    pub async fn execute(&self, session: &str, cmd: Cmd) -> CmdResult {
        match cmd.kind {
            CmdKind::Reply(reply) => outbound::reply(self, session, cmd.id, reply).await,
            CmdKind::React(react) => outbound::react(self, session, cmd.id, react).await,
            CmdKind::Edit(edit) => outbound::edit(self, session, cmd.id, edit).await,
            CmdKind::SendFile(_) => CmdResult::err(cmd.id, ResultError::BadRequest, "send_file is handled per connection"),
            CmdKind::FetchMessages(fetch) => outbound::fetch_messages(self, session, cmd.id, fetch).await,
            CmdKind::FetchMessage(fetch) => outbound::fetch_message(self, session, cmd.id, fetch).await,
            CmdKind::SearchMessages(search) => outbound::search_messages(self, session, cmd.id, search).await,
            CmdKind::Typing(typing) => outbound::typing(self, session, cmd.id, typing).await,
        }
    }

    pub fn watermark(&self, room_id: &str) -> Option<Watermark> {
        self.marks.get(room_id)
    }

    /// Hand a message to its session, or queue it while the session is away.
    pub fn dispatch(&self, session: &str, room: &Room, event: Event, ts_ms: u64) -> Dispatch {
        match self.registry.deliver(session, event.clone(), ts_ms) {
            Ok(()) => {
                self.handed_over(session, &event);
                Dispatch::Delivered
            }
            Err(DeliverError::NotConnected) => {
                let (waiting, evicted) = self.registry.enqueue(session, ts_ms, event, now_ms());
                if evicted > 0 {
                    warn!(session, evicted, "dropped queued messages beyond the backlog limits");
                }
                let _ = room;
                Dispatch::Queued(waiting)
            }
            Err(err) => Dispatch::Dropped(err.to_string()),
        }
    }

    /// The session acknowledged an event, so it is delivered: the room's watermark
    /// moves past it and its attachments leave the spool.
    pub fn acked(&self, session: &str, event: &Event, ts_ms: u64) {
        self.marks.set(&event.room_id, Watermark { ts_ms, event_id: event.event_id.clone() });
        for attachment in &event.attachments {
            let _ = fs::remove_file(self.spool.inbox_path(&attachment.transfer));
        }
        debug!(session, event_id = %event.event_id, "acknowledged");
    }

    /// Once an event is on its way to a session: for a message, keep the typing
    /// indicator alive until the session's first visible action. A reaction expects no
    /// answer. The watermark waits for the acknowledgement.
    pub fn handed_over(&self, session: &str, event: &Event) {
        if event.kind != EventKind::Message {
            return;
        }
        match RoomId::parse(&event.room_id).ok().and_then(|id| self.client.get_room(&id)) {
            Some(room) => self.typing_start(session, room),
            None => debug!(room = %event.room_id, "no room object for the typing notice"),
        }
    }

    /// Keep sending the typing notice in a room until the reply goes out or the cap.
    pub fn typing_start(&self, session: &str, room: Room) {
        let mut tasks = self.typing.lock().unwrap();
        if tasks.contains_key(room.room_id()) {
            return;
        }
        let generation = {
            let mut g = self.typing_generation.lock().unwrap();
            *g += 1;
            *g
        };
        let cancel = CancellationToken::new();
        tasks.insert(room.room_id().to_owned(), TypingTask { session: session.to_owned(), generation, cancel: cancel.clone() });
        drop(tasks);

        let tasks = self.typing.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            loop {
                if let Err(err) = room.typing_notice(true).await {
                    debug!(room = %room.room_id(), "typing notice failed: {err}");
                }
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(TYPING_POLL) => {}
                }
                if started.elapsed() > TYPING_MAX {
                    debug!(room = %room.room_id(), "typing notice cap reached");
                    break;
                }
            }
            {
                let mut tasks = tasks.lock().unwrap();
                if tasks.get(room.room_id()).map(|t| t.generation) == Some(generation) {
                    tasks.remove(room.room_id());
                }
            }
            let _ = room.typing_notice(false).await;
        });
    }

    /// Stop the typing refresh for a room (the reply is out, or failed).
    pub fn typing_stop(&self, room_id: &RoomId) {
        if let Some(task) = self.typing.lock().unwrap().remove(room_id) {
            task.cancel.cancel();
        }
    }

    /// Stop every typing refresh started for a session (it disconnected).
    pub fn typing_stop_session(&self, session: &str) {
        let mut tasks = self.typing.lock().unwrap();
        let rooms: Vec<OwnedRoomId> =
            tasks.iter().filter(|(_, t)| t.session == session).map(|(r, _)| r.clone()).collect();
        for room in rooms {
            if let Some(task) = tasks.remove(&room) {
                task.cancel.cancel();
            }
        }
    }
}

struct TypingTask {
    session: String,
    generation: u64,
    cancel: CancellationToken,
}

/// Per-room watermarks of the last delivered message, persisted so a restart neither
/// re-delivers nor loses the messages of a short downtime.
struct Marks {
    path: PathBuf,
    map: Mutex<HashMap<String, Watermark>>,
}

impl Marks {
    fn load(path: PathBuf) -> Marks {
        let map = match fs::read_to_string(&path) {
            Ok(text) => match serde_json::from_str::<HashMap<String, Watermark>>(&text) {
                Ok(map) => {
                    info!(rooms = map.len(), "delivery watermarks loaded from {}", path.display());
                    map
                }
                Err(err) => {
                    warn!("ignoring unreadable {}: {err}", path.display());
                    HashMap::new()
                }
            },
            Err(_) => HashMap::new(),
        };
        Marks { path, map: Mutex::new(map) }
    }

    fn get(&self, room_id: &str) -> Option<Watermark> {
        self.map.lock().unwrap().get(room_id).cloned()
    }

    /// Watermarks only move forward: an attachment delivered after a newer text
    /// message (its download took longer) must not pull the mark back.
    fn set(&self, room_id: &str, mark: Watermark) {
        let snapshot = {
            let mut map = self.map.lock().unwrap();
            if map.get(room_id).is_some_and(|current| current.ts_ms > mark.ts_ms) {
                return;
            }
            map.insert(room_id.to_owned(), mark);
            serde_json::to_string_pretty(&*map).unwrap_or_default()
        };
        let tmp = self.path.with_extension("json.tmp");
        if let Err(err) = fs::write(&tmp, snapshot).and_then(|_| fs::rename(&tmp, &self.path)) {
            warn!("cannot persist delivery watermarks to {}: {err}", self.path.display());
        }
    }
}

/// Outbound queue size per connected session.
const SESSION_QUEUE: usize = 256;

/// Per session, the events handed to its connection and not acknowledged yet, in the
/// order handed over, with their timestamps.
type InFlight = HashMap<String, Vec<(u64, Event)>>;

#[derive(Debug, Error)]
pub enum DeliverError {
    #[error("session is not connected")]
    NotConnected,
    #[error("session's outbound queue is full")]
    QueueFull,
}

/// Which sessions are connected, the queue to each, the events each connection has
/// been handed and not acknowledged yet, and the backlog of a session that is away. A
/// session name is claimed by at most one connection; the claim is released when the
/// connection's [`Claim`] drops, and what it had in flight goes back to the backlog.
#[derive(Clone)]
pub struct Registry {
    inner: Arc<Mutex<HashMap<String, mpsc::Sender<DaemonMessage>>>>,
    backlogs: Arc<Mutex<HashMap<String, Backlog<Event>>>>,
    inflight: Arc<Mutex<InFlight>>,
    /// The age cap of a queued event: the replay window.
    max_age_ms: u64,
}

impl Registry {
    pub fn new(max_age_ms: u64) -> Registry {
        Registry { inner: Default::default(), backlogs: Default::default(), inflight: Default::default(), max_age_ms }
    }

    /// Claim a session for a connection: the sender the connection answers its own
    /// commands through, the queue its writer drains, and the claim. `None` if another
    /// connection holds the session.
    pub fn claim(&self, session: &str) -> Option<(mpsc::Sender<DaemonMessage>, mpsc::Receiver<DaemonMessage>, Claim)> {
        let mut map = self.inner.lock().unwrap();
        if map.contains_key(session) {
            return None;
        }
        let (tx, rx) = mpsc::channel(SESSION_QUEUE);
        map.insert(session.to_owned(), tx.clone());
        Some((tx, rx, Claim { registry: self.clone(), session: session.to_owned() }))
    }

    pub fn is_connected(&self, session: &str) -> bool {
        self.inner.lock().unwrap().contains_key(session)
    }

    /// Hand an event to a connected session without waiting; it is in flight until the
    /// session acknowledges it.
    pub fn deliver(&self, session: &str, event: Event, ts_ms: u64) -> Result<(), DeliverError> {
        let map = self.inner.lock().unwrap();
        let Some(tx) = map.get(session) else {
            return Err(DeliverError::NotConnected);
        };
        let mut inflight = self.inflight.lock().unwrap();
        let list = inflight.entry(session.to_owned()).or_default();
        list.push((ts_ms, event.clone()));
        tx.try_send(DaemonMessage::Event(event)).map_err(|err| {
            list.pop();
            match err {
                TrySendError::Full(_) => DeliverError::QueueFull,
                // The connection is on its way out: the backlog takes it.
                TrySendError::Closed(_) => DeliverError::NotConnected,
            }
        })
    }

    /// Keep a message for a session that is away. Returns how many are waiting and how
    /// many were evicted to make room.
    pub fn enqueue(&self, session: &str, ts_ms: u64, event: Event, now_ms: u64) -> (usize, usize) {
        let mut backlogs = self.backlogs.lock().unwrap();
        let backlog = backlogs.entry(session.to_owned()).or_insert_with(|| Backlog::new(BACKLOG_MAX, self.max_age_ms));
        let evicted = backlog.push(ts_ms, event, now_ms);
        (backlog.len(), evicted)
    }

    /// Take the messages waiting for a session, oldest first, to hand them to its
    /// connection: they are in flight from here. Also the number that expired meanwhile.
    pub fn take_backlog(&self, session: &str, now_ms: u64) -> (Vec<(u64, Event)>, usize) {
        let (items, evicted) = match self.backlogs.lock().unwrap().get_mut(session) {
            Some(backlog) => backlog.drain(now_ms),
            None => (Vec::new(), 0),
        };
        if !items.is_empty() {
            self.inflight.lock().unwrap().entry(session.to_owned()).or_default().extend(items.iter().cloned());
        }
        (items, evicted)
    }

    /// The session acknowledged an event: no longer in flight.
    pub fn ack(&self, session: &str, event_id: &str) -> Option<(u64, Event)> {
        let mut inflight = self.inflight.lock().unwrap();
        let list = inflight.get_mut(session)?;
        let at = list.iter().position(|(_, event)| event.event_id == event_id)?;
        Some(list.remove(at))
    }

    /// A connection went away: what it had not acknowledged goes back in front of the
    /// backlog for the next one. Returns how many went back and how many of those were
    /// evicted.
    fn restore(&self, session: &str, now_ms: u64) -> (usize, usize) {
        let items = self.inflight.lock().unwrap().remove(session).unwrap_or_default();
        if items.is_empty() {
            return (0, 0);
        }
        let count = items.len();
        let mut backlogs = self.backlogs.lock().unwrap();
        let backlog = backlogs.entry(session.to_owned()).or_insert_with(|| Backlog::new(BACKLOG_MAX, self.max_age_ms));
        (count, backlog.restore(items, now_ms))
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
        let (returned, evicted) = self.registry.restore(&self.session, now_ms());
        if returned > 0 {
            warn!(session = %self.session, returned, evicted, "events the session never acknowledged wait for its next connection");
        }
        info!(session = %self.session, "session released");
    }
}
