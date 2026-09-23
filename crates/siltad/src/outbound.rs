//! Outbound commands: policy checks, Markdown rendering, chunking, reactions, edits,
//! files and history.

use matrix_sdk::{
    attachment::{
        AttachmentConfig, AttachmentInfo, BaseAudioInfo, BaseFileInfo, BaseImageInfo, BaseVideoInfo,
    },
    room::{
        reply::{EnforceThread, Reply as ReplyConfig, ReplyError},
        MessagesOptions,
    },
    ruma::{
        events::{
            reaction::ReactionEventContent,
            relation::Annotation,
            room::message::{
                AddMentions, Relation, ReplacementMetadata, ReplyWithinThread,
                RoomMessageEventContent, RoomMessageEventContentWithoutRelation,
                TextMessageEventContent,
            },
            AnySyncMessageLikeEvent, AnySyncTimelineEvent, SyncMessageLikeEvent,
        },
        EventId, OwnedEventId, RoomId, UInt,
    },
    Room, RoomMemberships, RoomState,
};
use regex::RegexBuilder;
use silta::{
    config::RoomShape,
    protocol::{
        CmdResult, Edit, FetchMessage, FetchMessages, HistoryAttachment, HistoryMessage, React,
        Reply, ResultError, SearchMessages, SendFile, Typing,
    },
    text::chunk_text,
    time::rfc3339_utc,
    transfer::{human_size, safe_name, Received},
};
use tracing::{info, warn};

use crate::{
    content::{self, Body},
    daemon::Daemon,
};

/// Matrix events are capped at 64 KiB, and a Markdown reply is sent twice in one event:
/// once as `body` and once as the rendered `formatted_body`. Chunk well below a quarter
/// of the cap so the rendered copy cannot push an event over it.
const CHUNK_BYTES: usize = 8 * 1024;
const HISTORY_DEFAULT: u32 = 20;
const HISTORY_MAX: u32 = 100;

type Fail = (ResultError, String);

fn bad(message: impl Into<String>) -> Fail {
    (ResultError::BadRequest, message.into())
}

pub async fn reply(daemon: &Daemon, session: &str, id: u64, reply: Reply) -> CmdResult {
    finish(id, do_reply(daemon, session, reply).await)
}

pub async fn react(daemon: &Daemon, session: &str, id: u64, react: React) -> CmdResult {
    finish(id, do_react(daemon, session, react).await)
}

pub async fn edit(daemon: &Daemon, session: &str, id: u64, edit: Edit) -> CmdResult {
    finish(id, do_edit(daemon, session, edit).await)
}

pub async fn fetch_messages(
    daemon: &Daemon,
    session: &str,
    id: u64,
    cmd: FetchMessages,
) -> CmdResult {
    match do_fetch_messages(daemon, session, cmd).await {
        Ok((messages, more)) => CmdResult::history(id, messages, more),
        Err((code, message)) => CmdResult::err(id, code, message),
    }
}

pub async fn search_messages(
    daemon: &Daemon,
    session: &str,
    id: u64,
    cmd: SearchMessages,
) -> CmdResult {
    match do_search_messages(daemon, session, cmd).await {
        Ok(scan) => {
            CmdResult::history(id, scan.messages, scan.more).with_scan(scan.scanned, scan.until)
        }
        Err((code, message)) => CmdResult::err(id, code, message),
    }
}

/// Bring the typing indicator back in a room the session may write to: for a session
/// that decided to send another message after a send that ended the indicator.
pub async fn typing(daemon: &Daemon, session: &str, id: u64, cmd: Typing) -> CmdResult {
    match writable_room(daemon, session, &cmd.room_id).await {
        Ok(room) => {
            info!(dir = "out", session, room = %room.room_id(), "showing the typing indicator in the room again");
            daemon.typing_start(session, room, false);
            CmdResult::done(id)
        }
        Err((code, message)) => CmdResult::err(id, code, message),
    }
}

pub async fn fetch_message(
    daemon: &Daemon,
    session: &str,
    id: u64,
    cmd: FetchMessage,
) -> CmdResult {
    match do_fetch_message(daemon, session, cmd).await {
        Ok(message) => CmdResult::history(id, vec![message], None),
        Err((code, message)) => CmdResult::err(id, code, message),
    }
}

fn finish(id: u64, result: Result<OwnedEventId, Fail>) -> CmdResult {
    match result {
        Ok(event_id) => CmdResult::ok(id, event_id.to_string()),
        Err((code, message)) => CmdResult::err(id, code, message),
    }
}

/// The joined room behind a room id string.
fn joined_room(daemon: &Daemon, room_id: &str) -> Result<Room, Fail> {
    let room_id =
        RoomId::parse(room_id).map_err(|_| bad(format!("{room_id:?} is not a room id")))?;
    let room = daemon.client.get_room(&room_id).ok_or_else(|| {
        (
            ResultError::RoomUnknown,
            format!("the bot is not in {room_id}"),
        )
    })?;
    if room.state() != RoomState::Joined {
        return Err((
            ResultError::RoomUnknown,
            format!("the bot is not joined to {room_id}"),
        ));
    }
    Ok(room)
}

pub async fn member_ids(room: &Room) -> Result<Vec<String>, Fail> {
    let members = room.members(RoomMemberships::ACTIVE).await.map_err(|e| {
        (
            ResultError::SendFailed,
            format!("cannot list the members of {}: {e}", room.room_id()),
        )
    })?;
    Ok(members.iter().map(|m| m.user_id().to_string()).collect())
}

/// What the routing needs to know about a room besides its members: the DM flag the
/// SDK keeps in the bot's `m.direct` (set when the bot joined an invite marked
/// `is_direct`), and whether the room has a name or an alias. A store failure on the
/// flag counts as "no flag", which the rule treats as the DM side when the room is
/// unnamed, and is logged.
pub async fn room_shape(room: &Room) -> RoomShape {
    let direct = room.is_direct().await.unwrap_or_else(|err| {
        warn!(room = %room.room_id(), "cannot read the DM flag: {err}");
        false
    });
    RoomShape {
        direct,
        named: room.name().is_some() || room.canonical_alias().is_some(),
    }
}

/// A room the session's send policy allows.
async fn writable_room(daemon: &Daemon, session: &str, room_id: &str) -> Result<Room, Fail> {
    let room = joined_room(daemon, room_id)?;
    let members = member_ids(&room).await?;
    let shape = room_shape(&room).await;
    if daemon
        .routing
        .may_send(
            session,
            room.room_id().as_str(),
            members.iter().map(String::as_str),
            shape,
        )
        .is_err()
    {
        warn!(dir = "out", session, room = %room.room_id(), "refusing to post: the session may not write to this room");
        return Err((
            ResultError::RoomNotAllowed,
            format!("session {session} may not send to {}", room.room_id()),
        ));
    }
    Ok(room)
}

/// A room the session owns, for reading history.
async fn readable_room(daemon: &Daemon, session: &str, room_id: &str) -> Result<Room, Fail> {
    let room = joined_room(daemon, room_id)?;
    let members = member_ids(&room).await?;
    let shape = room_shape(&room).await;
    if daemon
        .routing
        .may_read(
            session,
            room.room_id().as_str(),
            members.iter().map(String::as_str),
            shape,
        )
        .is_err()
    {
        warn!(dir = "in", session, room = %room.room_id(), "refusing to read: the session may not read this room");
        return Err((
            ResultError::RoomNotAllowed,
            format!("session {session} may not read {}", room.room_id()),
        ));
    }
    Ok(room)
}

fn parse_event_id(s: &str) -> Result<OwnedEventId, Fail> {
    EventId::parse(s).map_err(|_| bad(format!("{s:?} is not an event id")))
}

fn parse_optional_event_id(s: Option<&str>) -> Result<Option<OwnedEventId>, Fail> {
    s.map(parse_event_id).transpose()
}

/// What a quote and a thread mean for the relation of an outgoing message. A quote
/// alone follows the quoted message into its thread if it is in one, as Element does.
fn reply_config(
    reply_to: Option<OwnedEventId>,
    thread: Option<OwnedEventId>,
) -> Option<ReplyConfig> {
    let (event_id, enforce_thread) = match (reply_to, thread) {
        (Some(quoted), Some(_)) => (quoted, EnforceThread::Threaded(ReplyWithinThread::Yes)),
        (Some(quoted), None) => (quoted, EnforceThread::MaybeThreaded),
        (None, Some(root)) => (root, EnforceThread::Threaded(ReplyWithinThread::No)),
        (None, None) => return None,
    };
    Some(ReplyConfig {
        event_id,
        enforce_thread,
        add_mentions: AddMentions::No,
    })
}

async fn with_relation(
    room: &Room,
    content: RoomMessageEventContentWithoutRelation,
    config: Option<ReplyConfig>,
) -> Result<RoomMessageEventContent, Fail> {
    match config {
        None => Ok(content.with_relation(None)),
        Some(config) => room
            .make_reply_event(content, config)
            .await
            .map_err(|e| match e {
                ReplyError::Fetch(_) => (
                    ResultError::NotFound,
                    format!("cannot fetch the event to relate to: {e}"),
                ),
                other => bad(other.to_string()),
            }),
    }
}

/// The typing indicator runs from the acknowledgement until the session's first visible action
/// in the room; every command that sends something ends it.
async fn stop_typing(daemon: &Daemon, room: &Room) {
    daemon.typing_stop(room.room_id());
    let _ = room.typing_notice(false).await;
}

/// After a successful send: end the indicator, or keep it going when the session said
/// another message is coming (`more`). The homeserver clears the sender's typing state
/// when a message is sent, and the SDK would not repeat an unchanged notice for a few
/// seconds, so the notice is sent off and on again at once; the refresh task (kept if
/// already running, so its ten-minute cap still counts from the delivery) takes over.
async fn after_send(daemon: &Daemon, session: &str, room: &Room, more: bool) {
    if more {
        let _ = room.typing_notice(false).await;
        let _ = room.typing_notice(true).await;
        daemon.typing_start(session, room.clone(), false);
    } else {
        stop_typing(daemon, room).await;
    }
}

async fn do_reply(daemon: &Daemon, session: &str, reply: Reply) -> Result<OwnedEventId, Fail> {
    if reply.text.trim().is_empty() {
        return Err(bad("text is empty"));
    }
    let reply_to = parse_optional_event_id(reply.reply_to.as_deref())?;
    let thread = parse_optional_event_id(reply.thread.as_deref())?;
    let room = writable_room(daemon, session, &reply.room_id).await?;

    let chunks = chunk_text(&reply.text, CHUNK_BYTES);
    let mut last: Option<OwnedEventId> = None;
    let mut in_thread = false;
    for (i, chunk) in chunks.iter().enumerate() {
        let plain = RoomMessageEventContentWithoutRelation::text_markdown(chunk);
        // Later chunks of a threaded reply must stay in the thread; each follows the
        // previous chunk, which is by then the latest message of the thread.
        let config = if i == 0 {
            reply_config(reply_to.clone(), thread.clone())
        } else if in_thread {
            Some(ReplyConfig {
                event_id: last.clone().expect("a chunk was sent"),
                enforce_thread: EnforceThread::MaybeThreaded,
                add_mentions: AddMentions::No,
            })
        } else {
            None
        };
        let content = with_relation(&room, plain, config).await?;
        if i == 0 {
            in_thread = matches!(content.relates_to, Some(Relation::Thread(_)));
        }
        match room.send(content).await {
            Ok(sent) => last = Some(sent.response.event_id),
            Err(e) => {
                stop_typing(daemon, &room).await;
                return Err((
                    ResultError::SendFailed,
                    format!(
                        "sending to {} failed after {i} of {} chunks: {e}",
                        room.room_id(),
                        chunks.len()
                    ),
                ));
            }
        }
    }
    after_send(daemon, session, &room, reply.more).await;
    info!(dir = "out", session, room = %room.room_id(), chunks = chunks.len(), bytes = reply.text.len(), in_thread, more = reply.more, "posted a reply to the room");
    Ok(last.expect("at least one chunk"))
}

async fn do_react(daemon: &Daemon, session: &str, react: React) -> Result<OwnedEventId, Fail> {
    if react.emoji.trim().is_empty() {
        return Err(bad("emoji is empty"));
    }
    let target = parse_event_id(&react.event_id)?;
    let room = writable_room(daemon, session, &react.room_id).await?;
    let content = ReactionEventContent::new(Annotation::new(target.clone(), react.emoji.clone()));
    let sent = room.send(content).await.map_err(|e| {
        (
            ResultError::SendFailed,
            format!("reacting in {} failed: {e}", room.room_id()),
        )
    })?;
    after_send(daemon, session, &room, react.more).await;
    info!(dir = "out", session, room = %room.room_id(), %target, emoji = %react.emoji, more = react.more, "posted a reaction to the room");
    Ok(sent.response.event_id)
}

async fn do_edit(daemon: &Daemon, session: &str, edit: Edit) -> Result<OwnedEventId, Fail> {
    if edit.text.trim().is_empty() {
        return Err(bad("text is empty"));
    }
    if edit.text.len() > CHUNK_BYTES {
        return Err(bad(format!(
            "the new text is {} bytes; an edit must fit one message of {CHUNK_BYTES} bytes, send a new message instead",
            edit.text.len()
        )));
    }
    let target = parse_event_id(&edit.event_id)?;
    let room = writable_room(daemon, session, &edit.room_id).await?;

    let original = room
        .load_or_fetch_event(&target, None)
        .await
        .map_err(|e| (ResultError::NotFound, format!("cannot fetch {target}: {e}")))?;
    match original.sender() {
        Some(s) if daemon.routing.is_bot(s.as_str()) => {}
        _ => {
            return Err((
                ResultError::NotFound,
                format!("{target} is not one of the bot's own messages"),
            ))
        }
    }
    match original.raw().deserialize() {
        Ok(AnySyncTimelineEvent::MessageLike(AnySyncMessageLikeEvent::RoomMessage(
            SyncMessageLikeEvent::Original(m),
        ))) => {
            if matches!(m.content.relates_to, Some(Relation::Replacement(_))) {
                return Err(bad(format!(
                    "{target} is itself an edit; edit the original message"
                )));
            }
        }
        Ok(_) => return Err(bad(format!("{target} is not a message"))),
        Err(e) => return Err((ResultError::NotFound, format!("cannot read {target}: {e}"))),
    }

    let content = RoomMessageEventContent::text_markdown(&edit.text)
        .make_replacement(ReplacementMetadata::new(target.clone(), None));
    let sent = room.send(content).await.map_err(|e| {
        (
            ResultError::SendFailed,
            format!("editing {target} in {} failed: {e}", room.room_id()),
        )
    })?;
    after_send(daemon, session, &room, edit.more).await;
    info!(dir = "out", session, room = %room.room_id(), %target, bytes = edit.text.len(), more = edit.more, "posted an edit of our earlier message");
    Ok(sent.response.event_id)
}

/// Upload a file a session streamed over the socket. The spool copy is deleted
/// afterwards whatever happened.
pub async fn send_file(
    daemon: &Daemon,
    session: &str,
    id: u64,
    cmd: SendFile,
    received: Received,
) -> CmdResult {
    let result = do_send_file(daemon, session, cmd, &received).await;
    let _ = tokio::fs::remove_file(&received.path).await;
    finish(id, result)
}

async fn do_send_file(
    daemon: &Daemon,
    session: &str,
    cmd: SendFile,
    received: &Received,
) -> Result<OwnedEventId, Fail> {
    let file_error = |message: String| (ResultError::FileError, message);
    let name = safe_name(&received.header.name, &received.header.mime);
    let reply_to = parse_optional_event_id(cmd.reply_to.as_deref())?;
    let thread = parse_optional_event_id(cmd.thread.as_deref())?;
    let room = writable_room(daemon, session, &cmd.room_id).await?;

    let size = received.header.size;
    match daemon.client.load_or_fetch_max_upload_size().await {
        Ok(max) => {
            let max = u64::from(max);
            if size > max {
                return Err(file_error(format!(
                    "{name} is {} and the server accepts at most {}",
                    human_size(size),
                    human_size(max)
                )));
            }
        }
        Err(e) => warn!("cannot learn the server's upload limit, sending anyway: {e}"),
    }
    let data = tokio::fs::read(&received.path)
        .await
        .map_err(|e| file_error(format!("cannot read the received file: {e}")))?;
    let mime: mime_guess::Mime = received
        .header
        .mime
        .parse()
        .unwrap_or(mime_guess::mime::APPLICATION_OCTET_STREAM);

    // The size in the event's `info`, so history and clients can show it; the SDK
    // ignores an info whose kind does not match the content type.
    let info_size = UInt::new(data.len() as u64);
    let info = match mime.type_() {
        mime_guess::mime::IMAGE => AttachmentInfo::Image(BaseImageInfo {
            size: info_size,
            ..Default::default()
        }),
        mime_guess::mime::AUDIO => AttachmentInfo::Audio(BaseAudioInfo {
            size: info_size,
            ..Default::default()
        }),
        mime_guess::mime::VIDEO => AttachmentInfo::Video(BaseVideoInfo {
            size: info_size,
            ..Default::default()
        }),
        _ => AttachmentInfo::File(BaseFileInfo { size: info_size }),
    };
    let mut config = AttachmentConfig::new().info(info);
    if let Some(caption) = cmd.caption.filter(|c| !c.trim().is_empty()) {
        config = config.caption(Some(TextMessageEventContent::markdown(caption)));
    }
    config = config.reply(reply_config(reply_to, thread));
    let bytes = data.len();
    let response = room
        .send_attachment(&name, &mime, data, config)
        .await
        .map_err(|e| {
            (
                ResultError::SendFailed,
                format!("sending {name} to {} failed: {e}", room.room_id()),
            )
        })?;
    after_send(daemon, session, &room, cmd.more).await;
    info!(dir = "out", session, room = %room.room_id(), name, bytes, mime = %mime, more = cmd.more, "uploaded a file to the room");
    Ok(response.event_id)
}

/// Raw pages read for one history command at most, so a room full of reactions
/// cannot keep the daemon paging.
const HISTORY_MAX_PAGES: usize = 5;
/// A search reads bigger pages and more of them: up to 1000 events per call.
const SEARCH_PAGE: u32 = 100;
const SEARCH_MAX_PAGES: usize = 10;

/// What a backward scan of a room's history produced.
struct Scan {
    messages: Vec<HistoryMessage>,
    /// The token to continue from, absent once the start of the room was reached.
    more: Option<String>,
    /// Events examined, messages or not.
    scanned: u64,
    /// The timestamp of the oldest event examined.
    until: Option<String>,
}

/// Page backwards from `from` (or the end of the room) collecting the messages `keep`
/// accepts, until `wanted` are collected, `max_pages` pages are read, or the history
/// ends. A server-side type filter would not do: in an encrypted room every event is
/// `m.room.encrypted` on the server, and conduit ignores the filter anyway. The last
/// page is used whole, so a call may bring a few more than `wanted`.
async fn scan_history(
    daemon: &Daemon,
    room: &Room,
    from: Option<String>,
    page_size: u32,
    wanted: usize,
    max_pages: usize,
    mut keep: impl FnMut(&mut HistoryMessage) -> bool,
) -> Result<Scan, Fail> {
    let mut from = from;
    let mut out = Vec::new();
    let mut scanned = 0u64;
    let mut until = None;
    let mut pages = 0usize;
    let more = loop {
        let mut options = MessagesOptions::backward();
        options.limit = UInt::from(page_size);
        options.from = from.take();
        let page = room.messages(options).await.map_err(|e| {
            (
                ResultError::SendFailed,
                format!("history request for {} failed: {e}", room.room_id()),
            )
        })?;
        pages += 1;
        for event in &page.chunk {
            scanned += 1;
            if let Ok(any) = event.raw().deserialize() {
                until = Some(rfc3339_utc(any.origin_server_ts().get().into()));
            }
            if let Some(mut message) = history_message(daemon, event) {
                if keep(&mut message) {
                    out.push(message);
                }
            }
        }
        match page.end {
            None => break None,
            Some(_) if page.chunk.is_empty() => break None,
            Some(end) if out.len() >= wanted || pages >= max_pages => break Some(end),
            Some(end) => from = Some(end),
        }
    };
    Ok(Scan {
        messages: out,
        more,
        scanned,
        until,
    })
}

async fn do_fetch_messages(
    daemon: &Daemon,
    session: &str,
    cmd: FetchMessages,
) -> Result<(Vec<HistoryMessage>, Option<String>), Fail> {
    let room = readable_room(daemon, session, &cmd.room_id).await?;
    let limit = cmd.limit.unwrap_or(HISTORY_DEFAULT).clamp(1, HISTORY_MAX);
    let scan = scan_history(
        daemon,
        &room,
        cmd.from,
        limit,
        limit as usize,
        HISTORY_MAX_PAGES,
        |_| true,
    )
    .await?;
    info!(dir = "in", session, room = %room.room_id(), count = scan.messages.len(), scanned = scan.scanned, more = scan.more.is_some(), "read history back to the session");
    Ok((scan.messages, scan.more))
}

/// Search with a regular expression over the text and the attachment names. The regex
/// crate matches in linear time, so no pattern can stall the daemon; an invalid one is
/// a bad request with the parser's message.
async fn do_search_messages(
    daemon: &Daemon,
    session: &str,
    cmd: SearchMessages,
) -> Result<Scan, Fail> {
    let regex = RegexBuilder::new(&cmd.pattern)
        .case_insensitive(true)
        .size_limit(1 << 20)
        .build()
        .map_err(|e| bad(format!("invalid regular expression: {e}")))?;
    let limit = cmd.limit.unwrap_or(HISTORY_DEFAULT).clamp(1, HISTORY_MAX);
    let room = readable_room(daemon, session, &cmd.room_id).await?;
    let scan = scan_history(
        daemon,
        &room,
        cmd.from,
        SEARCH_PAGE,
        limit as usize,
        SEARCH_MAX_PAGES,
        |m| {
            if let Some(found) = regex.find(&m.text) {
                m.match_start = Some(m.text[..found.start()].chars().count());
                true
            } else {
                m.attachments.iter().any(|a| regex.is_match(&a.name))
            }
        },
    )
    .await?;
    info!(dir = "in", session, room = %room.room_id(), pattern = %cmd.pattern, matches = scan.messages.len(), scanned = scan.scanned, more = scan.more.is_some(), "searched history for the session");
    Ok(scan)
}

/// One message by id, whole, under the same ownership rule as history. The SDK fetches
/// it from the event cache or the server and decrypts it.
async fn do_fetch_message(
    daemon: &Daemon,
    session: &str,
    cmd: FetchMessage,
) -> Result<HistoryMessage, Fail> {
    let target = parse_event_id(&cmd.event_id)?;
    let room = readable_room(daemon, session, &cmd.room_id).await?;
    let event = room
        .load_or_fetch_event(&target, None)
        .await
        .map_err(|e| (ResultError::NotFound, format!("cannot fetch {target}: {e}")))?;
    let message = history_message(daemon, &event).ok_or_else(|| {
        (
            ResultError::NotFound,
            format!("{target} is not a message from a registered person or the bot"),
        )
    })?;
    info!(dir = "in", session, room = %room.room_id(), %target, bytes = message.text.len(), "read one message back to the session");
    Ok(message)
}

/// One raw timeline event as a history message: registered senders and the bot only,
/// messages only (not reactions, edits or state), decrypted.
fn history_message(
    daemon: &Daemon,
    event: &matrix_sdk::deserialized_responses::TimelineEvent,
) -> Option<HistoryMessage> {
    let Ok(AnySyncTimelineEvent::MessageLike(AnySyncMessageLikeEvent::RoomMessage(
        SyncMessageLikeEvent::Original(m),
    ))) = event.raw().deserialize()
    else {
        return None;
    };
    let sender = m.sender.as_str();
    let own = daemon.routing.is_bot(sender);
    let person = if own {
        None
    } else {
        Some(daemon.routing.person_for(sender)?)
    };
    let parsed = content::parse(&m.content);
    let (text, attachments) = match parsed.body {
        Body::Text(text) => (text, Vec::new()),
        Body::Media(media) => (
            media.caption,
            vec![HistoryAttachment {
                name: media.name,
                mime: media.mime,
                size: media.size.unwrap_or(0),
            }],
        ),
        Body::Edit | Body::Other(_) => return None,
    };
    Some(HistoryMessage {
        event_id: m.event_id.to_string(),
        sender: sender.to_owned(),
        person: person.map(|p| p.name.clone()),
        role: person.map(|p| p.role),
        own,
        ts: rfc3339_utc(m.origin_server_ts.get().into()),
        in_reply_to: parsed.in_reply_to,
        thread: parsed.thread,
        text,
        match_start: None,
        attachments,
    })
}
