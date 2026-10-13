//! The gate a room passes through while the bot joins it.
//!
//! A person's first messages can precede the invite: Element creates a DM and sends
//! into it before it invites anyone. When the bot's homeserver joins over federation it
//! receives the room state but not that history, so those messages never come through
//! sync and must be fetched. When the homeserver already has them, they come through
//! the first sync after the join as well. Delivering in whatever order the two paths
//! produce would duplicate messages, and delivering a later message before an earlier
//! one loses the earlier: once the session acknowledges the later one, the watermark
//! is past the earlier.
//!
//! So from the invite until the catch-up is done, the sync handlers hold the room's
//! events here, and the join task delivers held and fetched events together, once each
//! and oldest first, before it lets the room through.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use matrix_sdk::ruma::{
    events::{reaction::OriginalSyncReactionEvent, room::message::OriginalSyncRoomMessageEvent},
    EventId, OwnedEventId, OwnedRoomId, RoomId,
};
use tokio::sync::Notify;

/// An inbound event that waits for the room's catch-up.
#[derive(Debug, Clone)]
pub enum Held {
    Message(Box<OriginalSyncRoomMessageEvent>),
    Reaction(OriginalSyncReactionEvent),
}

impl Held {
    pub fn event_id(&self) -> &EventId {
        match self {
            Held::Message(e) => &e.event_id,
            Held::Reaction(e) => &e.event_id,
        }
    }

    pub fn ts(&self) -> u64 {
        match self {
            Held::Message(e) => e.origin_server_ts.get().into(),
            Held::Reaction(e) => e.origin_server_ts.get().into(),
        }
    }
}

impl From<OriginalSyncRoomMessageEvent> for Held {
    fn from(event: OriginalSyncRoomMessageEvent) -> Held {
        Held::Message(Box::new(event))
    }
}

impl From<OriginalSyncReactionEvent> for Held {
    fn from(event: OriginalSyncReactionEvent) -> Held {
        Held::Reaction(event)
    }
}

struct Gate {
    held: Vec<Held>,
    /// Signalled once a processed sync has the room as joined: by then the sync
    /// handlers have seen, and held, whatever that sync brought from before the join.
    synced: Arc<Notify>,
}

/// The rooms being joined.
#[derive(Default)]
pub struct Joins {
    gates: Mutex<HashMap<OwnedRoomId, Gate>>,
}

impl Joins {
    /// Open the gate for a room the bot was invited to. `None` when it is already
    /// open: a repeated invite must not start a second join.
    pub fn open(&self, room: &RoomId) -> Option<Arc<Notify>> {
        let mut gates = self.gates.lock().unwrap();
        if gates.contains_key(room) {
            return None;
        }
        let synced = Arc::new(Notify::new());
        gates.insert(
            room.to_owned(),
            Gate {
                held: Vec::new(),
                synced: synced.clone(),
            },
        );
        Some(synced)
    }

    /// Hold an event for a room being joined; hand it back when the room is not.
    pub fn hold<T: Into<Held>>(&self, room: &RoomId, event: T) -> Result<(), T> {
        match self.gates.lock().unwrap().get_mut(room) {
            Some(gate) => {
                gate.held.push(event.into());
                Ok(())
            }
            None => Err(event),
        }
    }

    /// A processed sync had the room as joined. The permit is kept when nobody waits
    /// yet, so a sync that comes before the join request returns is not missed.
    pub fn synced(&self, room: &RoomId) {
        if let Some(gate) = self.gates.lock().unwrap().get(room) {
            gate.synced.notify_one();
        }
    }

    /// Take what the room holds. When it holds nothing the gate closes in the same
    /// step, so no event can be held after the last take and then never delivered.
    pub fn take_or_close(&self, room: &RoomId) -> Vec<Held> {
        let mut gates = self.gates.lock().unwrap();
        let Some(gate) = gates.get_mut(room) else {
            return Vec::new();
        };
        let held = std::mem::take(&mut gate.held);
        if held.is_empty() {
            gates.remove(room);
        }
        held
    }
}

/// Merge fetched and held events: once each, skipping those in `seen` (and adding the
/// rest to it), oldest first. Server timestamps order them; ties keep arrival order.
pub fn merge(
    seen: &mut HashSet<OwnedEventId>,
    batches: impl IntoIterator<Item = Vec<Held>>,
) -> Vec<Held> {
    let mut out: Vec<Held> = batches
        .into_iter()
        .flatten()
        .filter(|e| seen.insert(e.event_id().to_owned()))
        .collect();
    out.sort_by_key(Held::ts);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use matrix_sdk::ruma::owned_room_id;
    use serde_json::json;

    fn message(id: &str, ts: u64) -> Held {
        Held::Message(Box::new(
            serde_json::from_value(json!({
                "type": "m.room.message",
                "event_id": id,
                "sender": "@alice:a.test",
                "origin_server_ts": ts,
                "content": { "msgtype": "m.text", "body": id },
            }))
            .unwrap(),
        ))
    }

    fn ids(events: &[Held]) -> Vec<&str> {
        events.iter().map(|e| e.event_id().as_str()).collect()
    }

    #[test]
    fn holds_only_while_open() {
        let joins = Joins::default();
        let room = owned_room_id!("!r:a.test");
        assert!(joins.hold(&room, message("$early", 1)).is_err());
        assert!(joins.open(&room).is_some());
        assert!(
            joins.open(&room).is_none(),
            "a repeated invite opens nothing"
        );
        assert!(joins.hold(&room, message("$a", 2)).is_ok());
        assert_eq!(ids(&joins.take_or_close(&room)), ["$a"]);
        // Still open: the take found something, so the caller must come back.
        assert!(joins.hold(&room, message("$b", 3)).is_ok());
        assert_eq!(ids(&joins.take_or_close(&room)), ["$b"]);
        assert!(joins.take_or_close(&room).is_empty());
        assert!(joins.hold(&room, message("$c", 4)).is_err(), "closed");
        assert!(joins.open(&room).is_some(), "a later invite opens it again");
    }

    #[tokio::test]
    async fn a_sync_before_the_wait_is_not_missed() {
        let joins = Joins::default();
        let room = owned_room_id!("!r:a.test");
        let synced = joins.open(&room).unwrap();
        joins.synced(&room);
        tokio::time::timeout(std::time::Duration::from_secs(1), synced.notified())
            .await
            .expect("the permit was kept");
    }

    #[test]
    fn merge_dedupes_and_orders() {
        let mut seen = HashSet::new();
        // Fetched history and a sync that brought part of it too, the sync's later
        // message first among the held.
        let fetched = vec![message("$2", 20), message("$1", 10)];
        let held = vec![message("$3", 30), message("$2", 20)];
        let out = merge(&mut seen, [fetched, held]);
        assert_eq!(ids(&out), ["$1", "$2", "$3"]);
        // A later drain skips what was delivered already.
        let out = merge(&mut seen, [vec![message("$3", 30), message("$4", 40)]]);
        assert_eq!(ids(&out), ["$4"]);
    }
}
