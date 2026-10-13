//! Event handlers and the sync loop.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{bail, Result};
use matrix_sdk::{
    config::SyncSettings,
    event_handler::Ctx,
    room::MessagesOptions,
    ruma::{
        api::error::ErrorKind,
        events::{
            reaction::OriginalSyncReactionEvent,
            room::{
                encrypted::OriginalSyncRoomEncryptedEvent,
                member::{MembershipState, StrippedRoomMemberEvent},
                message::OriginalSyncRoomMessageEvent,
            },
            AnySyncMessageLikeEvent, AnySyncStateEvent, AnySyncTimelineEvent, SyncMessageLikeEvent,
            SyncStateEvent,
        },
        UInt, UserId,
    },
    Client, LoopCtrl, Room, RoomState,
};
use silta::{
    config::{DropReason, Inbound, PersonConfig},
    protocol::{Attachment, Event, EventKind, RoomKind},
    replay::{verdict, Verdict},
    time::rfc3339_utc,
    transfer::{human_size, safe_id},
};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{
    content::{self, Body, Media},
    daemon::{now_ms, Daemon, Dispatch, Shared},
    joining::{oldest_first, Held},
    outbound::{member_ids, room_shape},
    spool::{self, DownloadError},
};

/// A download that has not finished by then is given up.
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(600);
/// How long a join waits for the sync to show the room as joined before it delivers
/// what preceded the join anyway. A join is an event, so the sync long-poll returns
/// with it at once; this only bounds a stalled sync.
const JOIN_SYNC_WAIT: Duration = Duration::from_secs(60);
/// How far before the invite a message still counts as the start of a conversation
/// with the bot rather than room history. Element sends a new DM's first message
/// seconds before its invite.
const CATCH_UP_WINDOW: Duration = Duration::from_secs(300);
/// The backward scan for messages sent before the join reads at most this much.
const CATCH_UP_PAGE: u32 = 50;
const CATCH_UP_MAX_PAGES: usize = 5;

pub fn register_handlers(daemon: &Shared) {
    let client = daemon.client.clone();
    client.add_event_handler_context(daemon.clone());
    client.add_event_handler(on_invite);
    client.add_event_handler(on_message);
    client.add_event_handler(on_reaction);
    client.add_event_handler(on_undecryptable);
}

/// Join rooms registered people invite the bot to; ignore everyone else. The room's
/// events wait at the gate until what was sent before the join is delivered: see
/// `crate::joining`.
async fn on_invite(
    event: StrippedRoomMemberEvent,
    room: Room,
    client: Client,
    Ctx(daemon): Ctx<Shared>,
) {
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
    if room.state() == RoomState::Joined {
        debug!(room = %room.room_id(), %inviter, "ignoring an invite to a room already joined");
        return;
    }
    let Some(synced) = daemon.joins.open(room.room_id()) else {
        debug!(room = %room.room_id(), %inviter, "already joining on an earlier invite");
        return;
    };
    info!(room = %room.room_id(), %inviter, person = %person.name, "joining on invite");
    let invited_at_ms = now_ms();
    tokio::spawn(async move {
        let mut delay = Duration::from_secs(2);
        loop {
            match room.join().await {
                Ok(()) => {
                    info!(room = %room.room_id(), "joined");
                    break;
                }
                // The server joined us but the response was lost: the sync already
                // shows the room joined, and a retry would fail on that.
                Err(err) if room.state() == RoomState::Joined => {
                    info!(room = %room.room_id(), "joined (the join request failed: {err})");
                    break;
                }
                Err(err) if delay < Duration::from_secs(300) => {
                    warn!(room = %room.room_id(), "join failed ({err}), retrying in {delay:?}");
                    sleep(delay).await;
                    delay *= 2;
                }
                Err(err) => {
                    warn!(room = %room.room_id(), "giving up joining: {err}");
                    daemon.joins.close(room.room_id());
                    return;
                }
            }
        }
        catch_up(&daemon, &room, &synced, invited_at_ms).await;
    });
}

/// Deliver what a registered person sent before the bot joined, then whatever the gate
/// held meanwhile, oldest first, and only then let the room's events through directly.
/// An event both fetched and held is delivered once: `admit` skips the second copy.
async fn catch_up(daemon: &Shared, room: &Room, synced: &tokio::sync::Notify, invited_at_ms: u64) {
    let room_id = room.room_id();
    let (mut fetched, since_ms) = fetch_before_join(daemon, room, invited_at_ms).await;
    // The scan reads newest first; reversed, the sort keeps room order among equal
    // timestamps.
    fetched.reverse();
    // A homeserver that already had the earlier messages sends them in the join's sync
    // too. Waiting for that sync to be processed puts them among the held, so they are
    // ordered with the fetched ones rather than coming through after the gate.
    if timeout(JOIN_SYNC_WAIT, synced.notified()).await.is_err() {
        warn!(
            room = %room_id,
            "the sync has not shown the room as joined after {JOIN_SYNC_WAIT:?}, delivering what preceded the join without it"
        );
    }
    if !fetched.is_empty() {
        info!(room = %room_id, events = fetched.len(), "found events sent before the join");
    }
    let mut batch = [fetched, daemon.joins.take_or_close(room_id)].concat();
    while !batch.is_empty() {
        // The join's sync can bring history from before `since_ms` too, when the
        // homeserver already had it; it is cut here as the scan cuts it.
        batch.retain(|event| event.ts() >= since_ms);
        for event in oldest_first(batch) {
            receive(daemon, room, event).await;
        }
        batch = daemon.joins.take_or_close(room_id);
    }
}

/// Page backwards past the bot's own join and collect the messages and reactions sent
/// before it: all of those after the bot's invite, and those up to `CATCH_UP_WINDOW`
/// before it, stopping at the room's creation or the page cap. The cut is measured from
/// the invite event, so a daemon that was down when the invite came still finds the
/// person's first messages; without the invite in sight it is measured from
/// `invited_at_ms`, when the daemon saw it. Returns the events, newest first, and the
/// cut. Over federation this is also what makes the bot's homeserver fetch that
/// history at all.
async fn fetch_before_join(daemon: &Daemon, room: &Room, invited_at_ms: u64) -> (Vec<Held>, u64) {
    let room_id = room.room_id();
    let window_ms = CATCH_UP_WINDOW.as_millis() as u64;
    let fallback_ms = invited_at_ms.saturating_sub(window_ms);
    let Some(own) = daemon.client.user_id() else {
        return (Vec::new(), fallback_ms);
    };
    let mut out: Vec<Held> = Vec::new();
    let mut past_join = false;
    // Set at the bot's invite; nothing between the join and the invite is cut.
    let mut since_ms = None;
    let mut from = None;
    let mut capped = true;
    'scan: for _ in 0..CATCH_UP_MAX_PAGES {
        let mut options = MessagesOptions::backward();
        options.limit = UInt::from(CATCH_UP_PAGE);
        options.from = from.take();
        let page = match room.messages(options).await {
            Ok(page) => page,
            Err(err) => {
                warn!(room = %room_id, "cannot read what was sent before the join: {err}");
                let since_ms = since_ms.unwrap_or(fallback_ms);
                out.retain(|event| event.ts() >= since_ms);
                return (out, since_ms);
            }
        };
        for event in &page.chunk {
            let Ok(any) = event.raw().deserialize() else {
                continue;
            };
            // What follows the join comes through sync.
            if !past_join {
                past_join = is_own_membership(&any, own, MembershipState::Join);
                continue;
            }
            let ts = u64::from(any.origin_server_ts().get());
            match since_ms {
                None if is_own_membership(&any, own, MembershipState::Invite) => {
                    since_ms = Some(ts.saturating_sub(window_ms));
                    continue;
                }
                Some(since) if ts < since => {
                    capped = false;
                    break 'scan;
                }
                _ => {}
            }
            match any {
                AnySyncTimelineEvent::MessageLike(AnySyncMessageLikeEvent::RoomMessage(
                    SyncMessageLikeEvent::Original(m),
                )) => out.push(Held::Message(Box::new(m))),
                AnySyncTimelineEvent::MessageLike(AnySyncMessageLikeEvent::Reaction(
                    SyncMessageLikeEvent::Original(r),
                )) => out.push(Held::Reaction(r)),
                AnySyncTimelineEvent::MessageLike(AnySyncMessageLikeEvent::RoomEncrypted(
                    SyncMessageLikeEvent::Original(e),
                )) => warn!(
                    room = %room_id,
                    sender = %e.sender,
                    event_id = %e.event_id,
                    "cannot decrypt an event sent before the join (no room key for it)"
                ),
                AnySyncTimelineEvent::State(AnySyncStateEvent::RoomCreate(_)) => {
                    capped = false;
                    break 'scan;
                }
                _ => {}
            }
        }
        match page.end {
            Some(end) if !page.chunk.is_empty() => from = Some(end),
            _ => {
                capped = false;
                break;
            }
        }
    }
    if !past_join {
        warn!(room = %room_id, "cannot find the bot's own join in the room's history");
    } else if capped {
        warn!(room = %room_id, "stopped looking for events sent before the join after {CATCH_UP_MAX_PAGES} pages");
    }
    let since_ms = since_ms.unwrap_or_else(|| {
        if past_join {
            warn!(room = %room_id, "cannot find the bot's invite in the room's history, cutting from when the daemon saw it");
        }
        fallback_ms
    });
    out.retain(|event| event.ts() >= since_ms);
    (out, since_ms)
}

/// The bot's own membership changing to `membership`, not an update that keeps it
/// (a profile change is a `join` after a `join`).
fn is_own_membership(
    event: &AnySyncTimelineEvent,
    own: &UserId,
    membership: MembershipState,
) -> bool {
    let AnySyncTimelineEvent::State(AnySyncStateEvent::RoomMember(SyncStateEvent::Original(m))) =
        event
    else {
        return false;
    };
    m.state_key == own
        && m.content.membership == membership
        && m.unsigned
            .prev_content
            .as_ref()
            .is_none_or(|prev| prev.membership != membership)
}

/// Where a received attachment is downloaded: off the sync loop, or in the join's
/// catch-up, which must not deliver a later event first.
#[derive(Clone, Copy)]
enum Download {
    Background,
    Inline,
}

async fn receive(daemon: &Shared, room: &Room, event: Held) {
    match event {
        Held::Message(event) => receive_message(daemon, room, *event, Download::Inline).await,
        Held::Reaction(event) => receive_reaction(daemon, room, event).await,
    }
}

/// The routing and replay decisions shared by messages and reactions: the registered
/// sender, the owning session (a group room goes to the hub, a DM to the person's
/// mind: the members, the DM flag and the name decide, see `silta::config::is_group`),
/// whether the room is a DM or a group by the same rule, and whether an earlier run
/// delivered the event already. `None` means drop, already logged.
async fn admit<'a>(
    daemon: &'a Daemon,
    room: &Room,
    sender: &str,
    event_id: &str,
    ts: u64,
    what: &str,
) -> Option<(&'a str, &'a PersonConfig, RoomKind)> {
    let room_id = room.room_id().as_str();
    // Without the members a group room cannot be told from a DM, and a group room must
    // never reach a personal mind, so the message goes no further. It is gone: the SDK
    // commits the sync token before it calls this handler, so the next sync — this run's
    // or a later run's — starts after the message. The replay window covers the daemon's
    // downtime, not a failure inside a run.
    let members = match member_ids(room).await {
        Ok(members) => members,
        Err((_, reason)) => {
            warn!(
                dir = "in",
                room = room_id,
                sender,
                "permanently lost an incoming {what}: {reason}"
            );
            return None;
        }
    };
    let shape = room_shape(room).await;
    debug!(
        room = room_id,
        direct = shape.direct,
        named = shape.named,
        members = members.len(),
        "room shape"
    );
    let (session, person, kind) =
        match daemon
            .routing
            .inbound(sender, room_id, members.iter().map(String::as_str), shape)
        {
            // Our own event coming back through sync is not traffic for anyone; the other
            // two reasons are a deliberate refusal, not a loss.
            Inbound::Drop(DropReason::OwnMessage) => {
                debug!(
                    dir = "in",
                    room = room_id,
                    "ignoring our own {what} coming back through sync"
                );
                return None;
            }
            Inbound::Drop(reason) => {
                info!(
                    dir = "in",
                    room = room_id,
                    sender,
                    "{what} not routed to a session: {reason}"
                );
                return None;
            }
            Inbound::Deliver {
                session,
                person,
                room: kind,
            } => (session, person, kind),
        };
    if !daemon.recent.first_sight(event_id) {
        debug!(
            dir = "in",
            room = room_id,
            event_id,
            "not delivering a {what} that came by another path already"
        );
        return None;
    }
    match verdict(
        ts,
        event_id,
        daemon.watermark(room_id).as_ref(),
        daemon.started_at_ms,
        daemon.replay_window_ms,
    ) {
        Verdict::AlreadyDelivered => {
            debug!(
                dir = "in",
                room = room_id,
                event_id,
                "not delivering a {what} an earlier run already delivered"
            );
            None
        }
        Verdict::TooOld => {
            debug!(
                dir = "in",
                room = room_id,
                event_id,
                "not delivering a {what} older than the replay window"
            );
            None
        }
        Verdict::Deliver { behind_start_ms: 0 } => Some((session, person, kind)),
        Verdict::Deliver { behind_start_ms } => {
            info!(dir = "in", room = room_id, person = %person.name, "received a {what} from {} s before the daemon started, within the replay window", behind_start_ms / 1000);
            Some((session, person, kind))
        }
    }
}

/// Deliver text, emotes and attachments from registered people to the session that
/// owns the room; queue them while the session is away; skip what an earlier run
/// delivered. Attachments are downloaded first, off the sync loop.
async fn on_message(event: OriginalSyncRoomMessageEvent, room: Room, Ctx(daemon): Ctx<Shared>) {
    if let Err(event) = daemon.joins.hold(room.room_id(), event) {
        receive_message(&daemon, &room, event, Download::Background).await;
    }
}

async fn receive_message(
    daemon: &Shared,
    room: &Room,
    event: OriginalSyncRoomMessageEvent,
    download: Download,
) {
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
            debug!(
                dir = "in",
                room = room_id,
                sender,
                "not delivering an edit of an earlier message, the bridge does not carry edits"
            );
            return;
        }
        Body::Other(msgtype) => {
            info!(
                dir = "in",
                room = room_id,
                sender,
                msgtype,
                "not delivering a message the bridge does not carry this type of"
            );
            return;
        }
        Body::Text(_) | Body::Media(_) => {}
    }
    let Some((session, person, kind)) = admit(daemon, room, sender, event_id, ts, "message").await
    else {
        return;
    };

    let mut message = Event {
        kind: EventKind::Message,
        person: person.name.clone(),
        role: person.role,
        sender: sender.to_owned(),
        room_id: room_id.to_owned(),
        room: kind,
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
            deliver(daemon, session, room, message, ts);
        }
        Body::Media(media) => {
            message.text = media.caption.clone();
            let (daemon, room, session) = (Arc::clone(daemon), room.clone(), session.to_owned());
            let job = async move {
                fetch_attachment(&daemon, &mut message, media).await;
                deliver(&daemon, &session, &room, message, ts);
            };
            match download {
                Download::Inline => job.await,
                Download::Background => {
                    tokio::spawn(job);
                }
            }
        }
        Body::Edit | Body::Other(_) => {}
    }
}

/// Download one attachment into the spool and record it on the event as a transfer,
/// or explain in the text why the file is not there.
async fn fetch_attachment(daemon: &Daemon, message: &mut Event, media: Media) {
    let Media {
        name,
        mime,
        size,
        source,
        ..
    } = media;
    let max = daemon.spool.max_bytes;
    // One attachment per Matrix message, so the transfer id is the event id.
    let transfer = format!("{}-1", safe_id(&message.event_id));
    let describe = |detail: &str| -> String {
        let size = size
            .map(|s| format!(", {}", human_size(s)))
            .unwrap_or_default();
        format!("[attachment \"{name}\" ({mime}{size}) {detail}]")
    };
    if size.is_some_and(|s| s > max) {
        info!(dir = "in", room = %message.room_id, transfer, "attachment not received: over the {} limit", human_size(max));
        note(
            message,
            describe(&format!(
                "not downloaded: over the {} limit",
                human_size(max)
            )),
        );
        return;
    }
    let path = daemon.spool.inbox_path(&transfer);
    match timeout(
        DOWNLOAD_TIMEOUT,
        spool::download(&daemon.client, source, &path, max),
    )
    .await
    {
        Ok(Ok(bytes)) => {
            info!(dir = "in", room = %message.room_id, transfer, bytes, "received an attachment into the spool");
            message.attachments.push(Attachment {
                transfer,
                name,
                mime,
                size: bytes,
            });
        }
        Ok(Err(DownloadError::TooLarge(bytes))) => {
            info!(dir = "in", room = %message.room_id, transfer, bytes, "attachment not received: over the {} limit", human_size(max));
            note(
                message,
                describe(&format!(
                    "not downloaded: {} is over the {} limit",
                    human_size(bytes),
                    human_size(max)
                )),
            );
        }
        Ok(Err(DownloadError::Failed(err))) => {
            warn!(dir = "in", room = %message.room_id, transfer, "attachment not received: the download failed: {err:#}");
            note(
                message,
                describe(&format!("could not be downloaded: {err}")),
            );
        }
        Err(_) => {
            warn!(dir = "in", room = %message.room_id, transfer, "attachment not received: the download timed out after {DOWNLOAD_TIMEOUT:?}");
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
    if let Err(event) = daemon.joins.hold(room.room_id(), event) {
        receive_reaction(&daemon, &room, event).await;
    }
}

async fn receive_reaction(daemon: &Shared, room: &Room, event: OriginalSyncReactionEvent) {
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

    let Some((session, person, kind)) = admit(daemon, room, sender, event_id, ts, "reaction").await
    else {
        return;
    };
    match room.load_or_fetch_event(&target, None).await {
        Ok(target_event) => match target_event.sender() {
            Some(s) if daemon.routing.is_bot(s.as_str()) => {}
            _ => {
                debug!(dir = "in", room = room_id, sender, target = %target, "ignoring a reaction on a message that is not ours");
                return;
            }
        },
        Err(err) => {
            warn!(dir = "in", room = room_id, sender, target = %target, "permanently lost an incoming reaction, cannot fetch what it reacts to: {err}");
            return;
        }
    }

    let message = Event {
        kind: EventKind::Reaction,
        person: person.name.clone(),
        role: person.role,
        sender: sender.to_owned(),
        room_id: room_id.to_owned(),
        room: kind,
        event_id: event_id.to_owned(),
        ts: rfc3339_utc(ts),
        in_reply_to: None,
        thread: None,
        reacts_to: Some(target.to_string()),
        text: key,
        transcribed: false,
        attachments: Vec::new(),
    };
    deliver(daemon, session, room, message, ts);
}

fn deliver(daemon: &Daemon, session: &str, room: &Room, event: Event, ts: u64) {
    let what = event.kind.as_str();
    let (room_id, person, bytes, files) = (
        event.room_id.clone(),
        event.person.clone(),
        event.text.len(),
        event.attachments.len(),
    );
    match daemon.dispatch(session, room, event, ts) {
        Dispatch::Delivered => {
            info!(dir = "in", room = %room_id, %person, session, bytes, files, "delivered a {what} to the session")
        }
        Dispatch::Queued(waiting) => {
            warn!(dir = "in", room = %room_id, %person, session, waiting, "queued a {what} for the session, which is not connected")
        }
        Dispatch::Dropped(reason) => {
            warn!(dir = "in", room = %room_id, %person, session, "permanently lost an incoming {what}: {reason}")
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
            // The callback runs once the response is processed and its event handlers
            // have run, which is what a join's catch-up waits for.
            result = daemon.client.sync_with_callback(settings, |response| {
                for room in response.rooms.joined.keys() {
                    daemon.joins.synced(room);
                }
                async { LoopCtrl::Continue }
            }) => match result {
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
    matches!(
        err.client_api_error_kind(),
        Some(ErrorKind::UnknownToken { .. } | ErrorKind::MissingToken)
    )
}
