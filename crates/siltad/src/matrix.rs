//! Event handlers and the sync loop.

use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use matrix_sdk::{
    config::SyncSettings,
    event_handler::Ctx,
    ruma::{
        api::error::ErrorKind,
        events::{
            reaction::OriginalSyncReactionEvent,
            room::{
                encrypted::OriginalSyncRoomEncryptedEvent,
                member::{MembershipState, StrippedRoomMemberEvent},
                message::OriginalSyncRoomMessageEvent,
            },
        },
    },
    Client, Room, RoomState,
};
use silta::{
    config::{Inbound, PersonConfig},
    protocol::{Attachment, Event, EventKind},
    replay::{verdict, Verdict},
    time::rfc3339_utc,
};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{
    content::{self, Body, Media},
    daemon::{Daemon, Dispatch, Shared},
    inbox::{self, human_size, DownloadError},
};

/// A download that has not finished by then is given up.
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(600);

pub fn register_handlers(daemon: &Shared) {
    let client = daemon.client.clone();
    client.add_event_handler_context(daemon.clone());
    client.add_event_handler(on_invite);
    client.add_event_handler(on_message);
    client.add_event_handler(on_reaction);
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

/// The routing and replay decisions shared by messages and reactions: the registered
/// sender, the owning session, and whether an earlier run delivered the event already.
/// `None` means drop, already logged.
fn admit<'a>(daemon: &'a Daemon, room_id: &str, sender: &str, event_id: &str, ts: u64, what: &str) -> Option<(&'a str, &'a PersonConfig)> {
    let (session, person) = match daemon.routing.inbound(sender, room_id) {
        Inbound::Drop(reason) => {
            info!(room = room_id, sender, "dropping a {what}: {reason}");
            return None;
        }
        Inbound::Deliver { session, person } => (session, person),
    };
    match verdict(ts, event_id, daemon.watermark(room_id).as_ref(), daemon.started_at_ms, daemon.replay_window_ms) {
        Verdict::AlreadyDelivered => {
            debug!(room = room_id, event_id, "skipping a {what} delivered by an earlier run");
            None
        }
        Verdict::TooOld => {
            debug!(room = room_id, event_id, "skipping a {what} older than the replay window");
            None
        }
        Verdict::Deliver { behind_start_ms: 0 } => Some((session, person)),
        Verdict::Deliver { behind_start_ms } => {
            info!(room = room_id, person = %person.name, "{what} from {} s before the daemon started, within the replay window", behind_start_ms / 1000);
            Some((session, person))
        }
    }
}

/// Deliver text, emotes and attachments from registered people to the session that
/// owns the room; queue them while the session is away; skip what an earlier run
/// delivered. Attachments are downloaded first, off the sync loop.
async fn on_message(event: OriginalSyncRoomMessageEvent, room: Room, Ctx(daemon): Ctx<Shared>) {
    if room.state() != RoomState::Joined {
        return;
    }
    let ts: u64 = event.origin_server_ts.get().into();
    let room_id = room.room_id().as_str();
    let sender = event.sender.as_str();
    let event_id = event.event_id.as_str();

    let parsed = content::parse(&event.content);
    match &parsed.body {
        Body::Edit => {
            debug!(room = room_id, sender, "skipping an edit");
            return;
        }
        Body::Other(msgtype) => {
            info!(room = room_id, sender, msgtype, "skipping a message type the bridge does not carry");
            return;
        }
        Body::Text(_) | Body::Media(_) => {}
    }
    let Some((session, person)) = admit(&daemon, room_id, sender, event_id, ts, "message") else {
        return;
    };

    let mut message = Event {
        kind: EventKind::Message,
        person: person.name.clone(),
        role: person.role,
        sender: sender.to_owned(),
        room_id: room_id.to_owned(),
        event_id: event_id.to_owned(),
        ts: rfc3339_utc(ts),
        in_reply_to: parsed.in_reply_to,
        thread: parsed.thread,
        reacts_to: None,
        text: String::new(),
        transcribed: false,
        attachments: Vec::new(),
    };
    match parsed.body {
        Body::Text(text) => {
            message.text = text;
            deliver(&daemon, session, &room, message, ts);
        }
        Body::Media(media) => {
            message.text = media.caption.clone();
            let session = session.to_owned();
            let daemon = daemon.clone();
            tokio::spawn(async move {
                fetch_attachment(&daemon, &mut message, media).await;
                deliver(&daemon, &session, &room, message, ts);
            });
        }
        Body::Edit | Body::Other(_) => {}
    }
}

/// Download one attachment into the inbox and record it on the event, or explain in
/// the text why the file is not there.
async fn fetch_attachment(daemon: &Daemon, message: &mut Event, media: Media) {
    let Media { name, mime, size, source, .. } = media;
    let max = daemon.inbox.max_bytes;
    let describe = |detail: &str| -> String {
        let size = size.map(|s| format!(", {}", human_size(s))).unwrap_or_default();
        format!("[attachment \"{name}\" ({mime}{size}) {detail}]")
    };
    if size.is_some_and(|s| s > max) {
        info!(room = %message.room_id, name, "attachment not downloaded: over the {} limit", human_size(max));
        note(message, describe(&format!("not downloaded: over the {} limit", human_size(max))));
        return;
    }
    let path = inbox::path_for(&daemon.inbox.dir, &message.event_id, &name, &mime);
    match timeout(DOWNLOAD_TIMEOUT, inbox::download(&daemon.client, source, &path, max)).await {
        Ok(Ok(bytes)) => {
            info!(room = %message.room_id, name, bytes, "attachment downloaded to {}", path.display());
            let path = path.to_string_lossy().into_owned();
            message.attachments.push(Attachment { name, mime, size: bytes, path });
        }
        Ok(Err(DownloadError::TooLarge(bytes))) => {
            info!(room = %message.room_id, name, bytes, "attachment not downloaded: over the {} limit", human_size(max));
            note(message, describe(&format!("not downloaded: {} is over the {} limit", human_size(bytes), human_size(max))));
        }
        Ok(Err(DownloadError::Failed(err))) => {
            warn!(room = %message.room_id, name, "attachment download failed: {err:#}");
            note(message, describe(&format!("could not be downloaded: {err}")));
        }
        Err(_) => {
            warn!(room = %message.room_id, name, "attachment download timed out after {DOWNLOAD_TIMEOUT:?}");
            note(message, describe("could not be downloaded: timed out"));
        }
    }
}

fn note(message: &mut Event, line: String) {
    if !message.text.is_empty() {
        message.text.push('\n');
    }
    message.text.push_str(&line);
}

/// Reactions on the bot's own messages, from registered people, to the session that
/// owns the room. Reactions on anything else are not the assistant's business.
async fn on_reaction(event: OriginalSyncReactionEvent, room: Room, Ctx(daemon): Ctx<Shared>) {
    if room.state() != RoomState::Joined {
        return;
    }
    let sender = event.sender.as_str();
    if daemon.routing.is_bot(sender) {
        return;
    }
    let ts: u64 = event.origin_server_ts.get().into();
    let room_id = room.room_id().as_str();
    let event_id = event.event_id.as_str();
    let target = event.content.relates_to.event_id.clone();
    let key = event.content.relates_to.key.clone();

    let Some((session, person)) = admit(&daemon, room_id, sender, event_id, ts, "reaction") else {
        return;
    };
    match room.load_or_fetch_event(&target, None).await {
        Ok(target_event) => match target_event.sender() {
            Some(s) if daemon.routing.is_bot(s.as_str()) => {}
            _ => {
                debug!(room = room_id, sender, target = %target, "ignoring a reaction on a message that is not the bot's");
                return;
            }
        },
        Err(err) => {
            warn!(room = room_id, sender, target = %target, "cannot fetch the target of a reaction: {err}");
            return;
        }
    }

    let message = Event {
        kind: EventKind::Reaction,
        person: person.name.clone(),
        role: person.role,
        sender: sender.to_owned(),
        room_id: room_id.to_owned(),
        event_id: event_id.to_owned(),
        ts: rfc3339_utc(ts),
        in_reply_to: None,
        thread: None,
        reacts_to: Some(target.to_string()),
        text: key,
        transcribed: false,
        attachments: Vec::new(),
    };
    deliver(&daemon, session, &room, message, ts);
}

fn deliver(daemon: &Daemon, session: &str, room: &Room, event: Event, ts: u64) {
    let what = event.kind.as_str();
    let (room_id, person, bytes, files) = (event.room_id.clone(), event.person.clone(), event.text.len(), event.attachments.len());
    match daemon.dispatch(session, room, event, ts) {
        Dispatch::Delivered => info!(room = %room_id, %person, session, bytes, files, "delivered {what}"),
        Dispatch::Queued(waiting) => {
            warn!(room = %room_id, %person, session, waiting, "session is not connected, {what} queued")
        }
        Dispatch::Dropped(why) => warn!(room = %room_id, %person, session, "dropping a {what}: {why}"),
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
