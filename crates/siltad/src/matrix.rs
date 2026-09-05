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
    protocol::{DaemonMessage, Event, EventKind},
    time::rfc3339_utc,
};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::daemon::Shared;

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

/// Deliver text messages from registered people to the session that owns the room.
async fn on_message(event: OriginalSyncRoomMessageEvent, room: Room, Ctx(daemon): Ctx<Shared>) {
    if room.state() != RoomState::Joined {
        return;
    }
    let ts: u64 = event.origin_server_ts.get().into();
    let room_id = room.room_id().as_str();
    let sender = event.sender.as_str();
    if ts < daemon.started_at_ms {
        debug!(room = room_id, sender, "skipping a message from before the daemon started");
        return;
    }
    if let Some(Relation::Replacement(_)) = event.content.relates_to {
        debug!(room = room_id, sender, "skipping an edit");
        return;
    }
    let text = match &event.content.msgtype {
        MessageType::Text(text) => text.body.as_str(),
        other => {
            info!(room = room_id, sender, msgtype = other.msgtype(), "skipping a non-text message");
            return;
        }
    };
    let text = if let Some(Relation::Reply { .. }) = event.content.relates_to {
        remove_plain_reply_fallback(text)
    } else {
        text
    };

    match daemon.routing.inbound(sender, room_id) {
        Inbound::Drop(reason) => {
            info!(room = room_id, sender, "dropping a message: {reason}");
        }
        Inbound::Deliver { session, person } => {
            let message = Event {
                kind: EventKind::Message,
                person: person.name.clone(),
                role: person.role,
                sender: sender.to_owned(),
                room_id: room_id.to_owned(),
                event_id: event.event_id.to_string(),
                ts: rfc3339_utc(ts),
                text: text.to_owned(),
                transcribed: false,
                attachments: Vec::new(),
            };
            match daemon.registry.deliver(session, DaemonMessage::Event(message)) {
                Ok(()) => {
                    info!(room = room_id, person = %person.name, session, bytes = text.len(), "delivered");
                    if let Err(err) = room.typing_notice(true).await {
                        debug!(room = room_id, "typing notice failed: {err}");
                    }
                }
                Err(err) => {
                    warn!(room = room_id, person = %person.name, session, "dropping a message: {err}");
                }
            }
        }
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
                    delay = (delay * 2).min(Duration::from_secs(60));
                }
            }
        }
    }
}

fn is_token_error(err: &matrix_sdk::Error) -> bool {
    matches!(err.client_api_error_kind(), Some(ErrorKind::UnknownToken { .. } | ErrorKind::MissingToken))
}
