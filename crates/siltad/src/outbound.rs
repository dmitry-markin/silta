//! Outbound messages: policy check, Markdown rendering, chunking, sending.

use matrix_sdk::{
    ruma::{
        events::{
            relation::{InReplyTo, Reply as ReplyRelation},
            room::message::{Relation, RoomMessageEventContent},
        },
        EventId, RoomId,
    },
    RoomMemberships, RoomState,
};
use silta::{
    protocol::{CmdResult, Reply, ResultError},
    text::chunk_text,
};
use tracing::{info, warn};

use crate::daemon::Daemon;

/// Matrix events are capped at 64 KiB, and a Markdown reply is sent twice in one event:
/// once as `body` and once as the rendered `formatted_body`. Chunk well below a quarter
/// of the cap so the rendered copy cannot push an event over it.
const CHUNK_BYTES: usize = 8 * 1024;

pub async fn reply(daemon: &Daemon, session: &str, id: u64, reply: Reply) -> CmdResult {
    let err = |code, message: String| CmdResult::err(id, code, message);

    if reply.text.trim().is_empty() {
        return err(ResultError::BadRequest, "text is empty".into());
    }
    let Ok(room_id) = RoomId::parse(&reply.room_id) else {
        return err(ResultError::BadRequest, format!("{:?} is not a room id", reply.room_id));
    };
    let reply_to = match &reply.reply_to {
        Some(s) => match EventId::parse(s) {
            Ok(event_id) => Some(event_id),
            Err(_) => return err(ResultError::BadRequest, format!("{s:?} is not an event id")),
        },
        None => None,
    };
    let Some(room) = daemon.client.get_room(&room_id) else {
        return err(ResultError::RoomUnknown, format!("the bot is not in {room_id}"));
    };
    if room.state() != RoomState::Joined {
        return err(ResultError::RoomUnknown, format!("the bot is not joined to {room_id}"));
    }

    let members = match room.members(RoomMemberships::ACTIVE).await {
        Ok(members) => members,
        Err(e) => return err(ResultError::SendFailed, format!("cannot list the members of {room_id}: {e}")),
    };
    let member_ids = members.iter().map(|m| m.user_id().as_str());
    if daemon.routing.may_send(session, room_id.as_str(), member_ids).is_err() {
        warn!(session, room = %room_id, "send refused by policy");
        return err(ResultError::RoomNotAllowed, format!("session {session} may not send to {room_id}"));
    }

    let chunks = chunk_text(&reply.text, CHUNK_BYTES);
    let mut last_event_id = None;
    for (i, chunk) in chunks.iter().enumerate() {
        let mut content = RoomMessageEventContent::text_markdown(chunk);
        if i == 0 {
            if let Some(event_id) = &reply_to {
                content.relates_to = Some(Relation::Reply(ReplyRelation::new(InReplyTo::new(event_id.clone()))));
            }
        }
        match room.send(content).await {
            Ok(sent) => last_event_id = Some(sent.response.event_id),
            Err(e) => {
                daemon.typing_stop(&room_id);
                let _ = room.typing_notice(false).await;
                return err(ResultError::SendFailed, format!("sending to {room_id} failed after {i} of {} chunks: {e}", chunks.len()));
            }
        }
    }
    daemon.typing_stop(&room_id);
    let _ = room.typing_notice(false).await;
    let event_id = last_event_id.map(|e| e.to_string()).unwrap_or_default();
    info!(session, room = %room_id, chunks = chunks.len(), bytes = reply.text.len(), "sent");
    CmdResult::ok(id, event_id)
}
