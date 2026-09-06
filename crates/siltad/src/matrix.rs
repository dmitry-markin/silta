//! Event handlers and the sync loop.

use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use matrix_sdk::{
    config::SyncSettings,
    event_handler::Ctx,
    ruma::{
        api::error::ErrorKind,
        events::room::{
            encrypted::OriginalSyncRoomEncryptedEvent,
            member::{MembershipState, StrippedRoomMemberEvent},
            message::{sanitize::remove_plain_reply_fallback, MessageType, OriginalSyncRoomMessageEvent, Relation},
        },
    },
    Client, Room, RoomState,
};
use silta::{
    config::Inbound,
    protocol::{Event, EventKind},
    replay::{verdict, Verdict},
    time::rfc3339_utc,
};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::daemon::{Dispatch, Shared};

pub fn register_handlers(daemon: &Shared) {
    let client = daemon.client.clone();
    client.add_event_handler_context(daemon.clone());
    client.add_event_handler(on_invite);
    client.add_event_handler(on_message);
    client.add_event_handler(on_undecryptable);
}

/// Join rooms registered people invite the bot to; ignore everyone else.
async fn on_invite(event: StrippedRoomMemberEvent, room: Room, client: Client, Ctx(daemon): Ctx<Shared>) {
    if Some(event.state_key.as_ref()) != client.user_id() {
        return;
    }
    if event.content.membership != MembershipState::Invite {
        return;
    }
    let inviter = event.sender.as_str();
    let Some(person) = daemon.routing.person_for(inviter) else {
        warn!(room = %room.room_id(), %inviter, "ignoring an invite from an unregistered account");
        return;
    };
    info!(room = %room.room_id(), %inviter, person = %person.name, "joining on invite");
    tokio::spawn(async move {
        let mut delay = Duration::from_secs(2);
        loop {
            match room.join().await {
                Ok(()) => {
                    info!(room = %room.room_id(), "joined");
                    return;
                }
                Err(err) if delay < Duration::from_secs(300) => {
                    warn!(room = %room.room_id(), "join failed ({err}), retrying in {delay:?}");
                    sleep(delay).await;
                    delay *= 2;
                }
                Err(err) => {
                    warn!(room = %room.room_id(), "giving up joining: {err}");
                    return;
                }
            }
        }
    });
}

/// Deliver text messages and emotes from registered people to the session that owns
/// the room; queue them while the session is away; skip what an earlier run delivered.
async fn on_message(event: OriginalSyncRoomMessageEvent, room: Room, Ctx(daemon): Ctx<Shared>) {
    if room.state() != RoomState::Joined {
        return;
    }
    let ts: u64 = event.origin_server_ts.get().into();
    let room_id = room.room_id().as_str();
    let sender = event.sender.as_str();
    let event_id = event.event_id.as_str();

    if let Some(Relation::Replacement(_)) = &event.content.relates_to {
        debug!(room = room_id, sender, "skipping an edit");
        return;
    }
    let in_reply_to = match &event.content.relates_to {
        Some(Relation::Reply(reply)) => Some(reply.in_reply_to.event_id.to_string()),
        _ => None,
    };
    let text: String = match &event.content.msgtype {
        MessageType::Text(text) if in_reply_to.is_some() => remove_plain_reply_fallback(&text.body).to_owned(),
        MessageType::Text(text) => text.body.clone(),
        MessageType::Emote(emote) => format!("/me {}", emote.body),
        other => {
            info!(room = room_id, sender, msgtype = other.msgtype(), "skipping a non-text message");
            return;
        }
    };

    let (session, person) = match daemon.routing.inbound(sender, room_id) {
        Inbound::Drop(reason) => {
            info!(room = room_id, sender, "dropping a message: {reason}");
            return;
        }
        Inbound::Deliver { session, person } => (session, person),
    };

    match verdict(ts, event_id, daemon.watermark(room_id).as_ref(), daemon.started_at_ms, daemon.replay_window_ms) {
        Verdict::AlreadyDelivered => {
            debug!(room = room_id, event_id, "skipping a message delivered by an earlier run");
            return;
        }
        Verdict::TooOld => {
            debug!(room = room_id, event_id, "skipping a message older than the replay window");
            return;
        }
        Verdict::Deliver { behind_start_ms: 0 } => {}
        Verdict::Deliver { behind_start_ms } => {
            info!(room = room_id, person = %person.name, "message from {} s before the daemon started, within the replay window", behind_start_ms / 1000);
        }
    }

    let message = Event {
        kind: EventKind::Message,
        person: person.name.clone(),
        role: person.role,
        sender: sender.to_owned(),
        room_id: room_id.to_owned(),
        event_id: event_id.to_owned(),
        ts: rfc3339_utc(ts),
        in_reply_to,
        text: text.clone(),
        transcribed: false,
        attachments: Vec::new(),
    };
    match daemon.dispatch(session, &room, message, ts) {
        Dispatch::Delivered => info!(room = room_id, person = %person.name, session, bytes = text.len(), "delivered"),
        Dispatch::Queued(waiting) => {
            warn!(room = room_id, person = %person.name, session, waiting, "session is not connected, message queued")
        }
        Dispatch::Dropped(why) => warn!(room = room_id, person = %person.name, session, "dropping a message: {why}"),
    }
}

/// Events the SDK could not decrypt reach handlers in their encrypted form.
async fn on_undecryptable(event: OriginalSyncRoomEncryptedEvent, room: Room) {
    warn!(
        room = %room.room_id(),
        sender = %event.sender,
        event_id = %event.event_id,
        "cannot decrypt an event (no room key for it yet)"
    );
}

/// The sync backoff cap. It is also the worst case for how long the bot stays deaf after
/// the homeserver comes back, so it is kept short.
const MAX_SYNC_RETRY_DELAY: Duration = Duration::from_secs(15);

/// Sync until cancelled. Transient errors are retried with backoff; an invalid token is
/// fatal because only a new login can fix it.
pub async fn sync_loop(daemon: Shared, cancel: CancellationToken) -> Result<()> {
    let mut delay = Duration::from_secs(1);
    loop {
        let settings = SyncSettings::default().timeout(Duration::from_secs(30));
        let started = Instant::now();
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            result = daemon.client.sync(settings) => match result {
                Ok(()) => return Ok(()),
                Err(err) => {
                    if is_token_error(&err) {
                        bail!(
                            "the server rejected the access token ({err}); delete session.json and \
                             store/ together and restart to log in again"
                        );
                    }
                    if started.elapsed() > Duration::from_secs(60) {
                        delay = Duration::from_secs(1);
                    }
                    warn!("sync failed: {err}; retrying in {delay:?}");
                    tokio::select! {
                        _ = cancel.cancelled() => return Ok(()),
                        _ = sleep(delay) => {}
                    }
                    delay = (delay * 2).min(MAX_SYNC_RETRY_DELAY);
                }
            }
        }
    }
}

fn is_token_error(err: &matrix_sdk::Error) -> bool {
    matches!(err.client_api_error_kind(), Some(ErrorKind::UnknownToken { .. } | ErrorKind::MissingToken))
}
