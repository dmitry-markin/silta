//! Reading the content of `m.room.message` events: relation, text, media. Shared by the
//! inbound handler and the history command so both describe a message the same way.

use matrix_sdk::ruma::{
    events::room::{
        message::{sanitize::remove_plain_reply_fallback, MessageType, Relation, RoomMessageEventContent},
        MediaSource,
    },
    UInt,
};

pub struct Parsed {
    /// The quoted event: an ordinary reply, or an explicit reply inside a thread. The
    /// thread fallback (`is_falling_back`) does not count.
    pub in_reply_to: Option<String>,
    /// The thread root when the message is in a thread.
    pub thread: Option<String>,
    pub body: Body,
}

pub enum Body {
    Text(String),
    Media(Media),
    /// An edit of an earlier message (`m.replace`); not carried by the bridge.
    Edit,
    /// A message type the bridge does not carry (its `msgtype`).
    Other(String),
}

pub struct Media {
    pub name: String,
    pub mime: String,
    /// From the event's `info`, if the sender gave one.
    pub size: Option<u64>,
    /// The `body` when it is a caption rather than the file name; empty otherwise.
    pub caption: String,
    pub source: MediaSource,
}

pub fn parse(content: &RoomMessageEventContent) -> Parsed {
    let mut in_reply_to = None;
    let mut thread = None;
    let mut fallback = false;
    match &content.relates_to {
        Some(Relation::Replacement(_)) => return Parsed { in_reply_to: None, thread: None, body: Body::Edit },
        Some(Relation::Reply(reply)) => {
            in_reply_to = Some(reply.in_reply_to.event_id.to_string());
            fallback = true;
        }
        Some(Relation::Thread(t)) => {
            thread = Some(t.event_id.to_string());
            if let Some(reply) = &t.in_reply_to {
                fallback = true;
                if !t.is_falling_back {
                    in_reply_to = Some(reply.event_id.to_string());
                }
            }
        }
        _ => {}
    }
    let strip = |body: &str| -> String {
        if fallback {
            remove_plain_reply_fallback(body).to_owned()
        } else {
            body.to_owned()
        }
    };
    let body = match &content.msgtype {
        MessageType::Text(text) => Body::Text(strip(&text.body)),
        MessageType::Emote(emote) => Body::Text(format!("/me {}", strip(&emote.body))),
        MessageType::Image(c) => Body::Media(media(
            &c.body,
            c.filename.as_deref(),
            c.info.as_ref().and_then(|i| i.mimetype.as_deref()),
            c.info.as_ref().and_then(|i| i.size),
            &c.source,
        )),
        MessageType::File(c) => Body::Media(media(
            &c.body,
            c.filename.as_deref(),
            c.info.as_ref().and_then(|i| i.mimetype.as_deref()),
            c.info.as_ref().and_then(|i| i.size),
            &c.source,
        )),
        MessageType::Audio(c) => Body::Media(media(
            &c.body,
            c.filename.as_deref(),
            c.info.as_ref().and_then(|i| i.mimetype.as_deref()),
            c.info.as_ref().and_then(|i| i.size),
            &c.source,
        )),
        MessageType::Video(c) => Body::Media(media(
            &c.body,
            c.filename.as_deref(),
            c.info.as_ref().and_then(|i| i.mimetype.as_deref()),
            c.info.as_ref().and_then(|i| i.size),
            &c.source,
        )),
        other => Body::Other(other.msgtype().to_owned()),
    };
    Parsed { in_reply_to, thread, body }
}

/// The spec's rule: with a `filename`, `body` is a caption when it differs from it;
/// without one, `body` is the file name.
fn media(body: &str, filename: Option<&str>, mimetype: Option<&str>, size: Option<UInt>, source: &MediaSource) -> Media {
    let (name, caption) = match filename {
        Some(f) if f != body => (f.to_owned(), body.to_owned()),
        Some(f) => (f.to_owned(), String::new()),
        None => (body.to_owned(), String::new()),
    };
    let mime = mimetype
        .map(str::to_owned)
        .unwrap_or_else(|| mime_guess::from_path(&name).first_raw().unwrap_or("application/octet-stream").to_owned());
    Media { name, mime, size: size.map(u64::from), caption, source: source.clone() }
}
