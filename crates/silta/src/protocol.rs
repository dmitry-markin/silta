//! Socket protocol v1 between `siltad` (server) and a session plugin (client).
//!
//! JSON lines over a Unix stream socket, one object per line, UTF-8, `\n` terminated,
//! at most [`MAX_LINE_BYTES`] per line. The wire format is pinned by the tests at the
//! bottom of this file, not by the derives.

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

/// Protocol revision carried in `hello` and `welcome`.
pub const PROTOCOL_VERSION: u32 = 1;

/// Hard cap on one line, a guard against a runaway peer rather than a buffer size.
pub const MAX_LINE_BYTES: usize = 16 * 1024 * 1024;

/// Messages from a plugin to the daemon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientMessage {
    Hello(Hello),
    Cmd(Cmd),
}

/// Messages from the daemon to a plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonMessage {
    Welcome(Welcome),
    Error(ProtocolError),
    Event(Event),
    Result(CmdResult),
}

/// First line on a connection: which session this client is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    pub protocol: u32,
    pub session: String,
    pub client: String,
}

/// The daemon's answer to a valid `hello`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Welcome {
    pub protocol: u32,
    pub session: String,
    /// The bot's Matrix user id.
    pub user_id: String,
    pub people: Vec<Person>,
}

/// A registered person as announced in `welcome`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Person {
    pub name: String,
    pub role: Role,
}

/// Role of a person, from the daemon's registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Owner,
    Family,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Owner => "owner",
            Role::Family => "family",
        }
    }
}

/// A fatal handshake error; the daemon closes the connection after sending it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolError {
    pub code: ErrorCode,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    ProtocolMismatch,
    UnknownSession,
    SessionBusy,
    BadRequest,
}

impl ErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::ProtocolMismatch => "protocol_mismatch",
            ErrorCode::UnknownSession => "unknown_session",
            ErrorCode::SessionBusy => "session_busy",
            ErrorCode::BadRequest => "bad_request",
        }
    }
}

/// One inbound Matrix event the session owns: a message, or a reaction on one of the
/// bot's own messages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    pub kind: EventKind,
    pub person: String,
    pub role: Role,
    pub sender: String,
    pub room_id: String,
    pub event_id: String,
    /// RFC 3339 UTC, from `origin_server_ts`.
    pub ts: String,
    /// The event id this message quotes (`m.in_reply_to`), when it is a reply. Inside
    /// a thread only an explicit reply sets it, never the thread fallback.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<String>,
    /// The thread root, when the message is in a thread.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread: Option<String>,
    /// For a reaction: the bot's message the person reacted to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reacts_to: Option<String>,
    /// The text; an emote arrives as `/me <text>`; a reaction carries its emoji.
    pub text: String,
    pub transcribed: bool,
    /// Files the daemon downloaded into its inbox; always present.
    pub attachments: Vec<Attachment>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Message,
    Reaction,
}

impl EventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            EventKind::Message => "message",
            EventKind::Reaction => "reaction",
        }
    }
}

/// A file the daemon downloaded into its inbox.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attachment {
    pub name: String,
    pub mime: String,
    pub size: u64,
    pub path: String,
}

/// A command from the plugin. `id` is chosen by the client, unique per connection;
/// the daemon answers every command exactly once, in order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cmd {
    pub id: u64,
    #[serde(flatten)]
    pub kind: CmdKind,
}

/// The command payload, externally tagged so the command name is the key.
/// Still reserved and rejected with `bad_request`: `typing`, `permission_request`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CmdKind {
    Reply(Reply),
    React(React),
    Edit(Edit),
    SendFile(SendFile),
    FetchMessages(FetchMessages),
    FetchMessage(FetchMessage),
}

impl CmdKind {
    pub fn name(&self) -> &'static str {
        match self {
            CmdKind::Reply(_) => "reply",
            CmdKind::React(_) => "react",
            CmdKind::Edit(_) => "edit",
            CmdKind::SendFile(_) => "send_file",
            CmdKind::FetchMessages(_) => "fetch_messages",
            CmdKind::FetchMessage(_) => "fetch_message",
        }
    }
}

/// Send a text message to a room, Markdown rendered by the daemon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reply {
    pub room_id: String,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<String>,
    /// Thread root to answer in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread: Option<String>,
}

/// React to a message with an emoji.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct React {
    pub room_id: String,
    pub event_id: String,
    pub emoji: String,
}

/// Replace the text of one of the bot's own messages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Edit {
    pub room_id: String,
    pub event_id: String,
    pub text: String,
}

/// Send a file from the daemon's host into a room.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendFile {
    pub room_id: String,
    /// Absolute path of a regular file.
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caption: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread: Option<String>,
}

/// Fetch recent messages of a room, newest first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchMessages {
    pub room_id: String,
    /// At least this many when the room has them (the last page is returned whole).
    /// Default 20, at most 100.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /// The `more` token of an earlier result, to page further back.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
}

/// Fetch one message by id, whole. Answered like `fetch_messages`, with one message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchMessage {
    pub room_id: String,
    pub event_id: String,
}

/// The daemon's answer to a command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CmdResult {
    pub id: u64,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ResultError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// `fetch_messages`: the messages, newest first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub messages: Option<Vec<HistoryMessage>>,
    /// `fetch_messages`: pass as `from` to page further back; absent at the start
    /// of the room's history.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub more: Option<String>,
}

impl CmdResult {
    pub fn ok(id: u64, event_id: impl Into<String>) -> Self {
        Self {
            id,
            ok: true,
            event_id: Some(event_id.into()),
            error: None,
            message: None,
            messages: None,
            more: None,
        }
    }

    pub fn history(id: u64, messages: Vec<HistoryMessage>, more: Option<String>) -> Self {
        Self { id, ok: true, event_id: None, error: None, message: None, messages: Some(messages), more }
    }

    pub fn err(id: u64, error: ResultError, message: impl Into<String>) -> Self {
        Self {
            id,
            ok: false,
            event_id: None,
            error: Some(error),
            message: Some(message.into()),
            messages: None,
            more: None,
        }
    }
}

/// One message from a room's history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryMessage {
    pub event_id: String,
    pub sender: String,
    /// The registered person, absent for the bot's own messages.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub person: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<Role>,
    /// Sent by the bot itself.
    pub own: bool,
    pub ts: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread: Option<String>,
    pub text: String,
    /// Listed, not downloaded.
    pub attachments: Vec<HistoryAttachment>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryAttachment {
    pub name: String,
    pub mime: String,
    pub size: u64,
}

/// Error codes a command can fail with. `Unknown` catches codes from a newer daemon.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultError {
    RoomNotAllowed,
    RoomUnknown,
    SendFailed,
    BadRequest,
    /// The event does not exist, or is not the bot's own message when it must be.
    NotFound,
    /// The path is not a readable regular file, or the file is too large.
    FileError,
    #[serde(other)]
    Unknown,
}

impl ResultError {
    pub fn as_str(self) -> &'static str {
        match self {
            ResultError::RoomNotAllowed => "room_not_allowed",
            ResultError::RoomUnknown => "room_unknown",
            ResultError::SendFailed => "send_failed",
            ResultError::BadRequest => "bad_request",
            ResultError::NotFound => "not_found",
            ResultError::FileError => "file_error",
            ResultError::Unknown => "unknown",
        }
    }
}

/// How the daemon should treat one line from a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Incoming {
    /// A well-formed message.
    Message(ClientMessage),
    /// Valid JSON carrying `cmd.id` but not a command this daemon supports: answer
    /// with a `bad_request` result and keep the connection.
    BadRequest { id: u64, message: String },
    /// Valid JSON that is neither: log and ignore.
    Ignored { message: String },
}

/// Classify one client line. `Err` means the line is not JSON at all, which closes
/// the connection.
pub fn parse_client_line(line: &str) -> Result<Incoming, serde_json::Error> {
    let value: Value = serde_json::from_str(line)?;
    match ClientMessage::deserialize(&value) {
        Ok(message) => Ok(Incoming::Message(message)),
        Err(err) => {
            let cmd_id = value.get("cmd").and_then(|c| c.get("id")).and_then(Value::as_u64);
            let message = if let Some(cmd) = value.get("cmd").and_then(Value::as_object) {
                let names: Vec<&str> =
                    cmd.keys().filter(|k| k.as_str() != "id").map(String::as_str).collect();
                format!("unsupported or malformed command {:?}: {err}", names)
            } else {
                err.to_string()
            };
            match cmd_id {
                Some(id) => Ok(Incoming::BadRequest { id, message }),
                None => Ok(Incoming::Ignored { message }),
            }
        }
    }
}

/// Parse one daemon line on the client side.
pub fn parse_daemon_line(line: &str) -> Result<DaemonMessage, serde_json::Error> {
    parse_line(line)
}

fn parse_line<T: DeserializeOwned>(line: &str) -> Result<T, serde_json::Error> {
    serde_json::from_str(line)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_client(json: &str) -> ClientMessage {
        let msg: ClientMessage = serde_json::from_str(json).expect("deserialize");
        assert_eq!(serde_json::to_string(&msg).unwrap(), json, "serialization must match the pinned wire format");
        msg
    }

    fn roundtrip_daemon(json: &str) -> DaemonMessage {
        let msg: DaemonMessage = serde_json::from_str(json).expect("deserialize");
        assert_eq!(serde_json::to_string(&msg).unwrap(), json, "serialization must match the pinned wire format");
        msg
    }

    #[test]
    fn hello() {
        let msg = roundtrip_client(r#"{"hello":{"protocol":1,"session":"hub","client":"silta-claude/0.1.0"}}"#);
        assert_eq!(
            msg,
            ClientMessage::Hello(Hello {
                protocol: 1,
                session: "hub".into(),
                client: "silta-claude/0.1.0".into()
            })
        );
    }

    #[test]
    fn welcome() {
        let msg = roundtrip_daemon(
            r#"{"welcome":{"protocol":1,"session":"hub","user_id":"@silta:silta.test","people":[{"name":"Bob","role":"owner"},{"name":"Alice","role":"family"}]}}"#,
        );
        let DaemonMessage::Welcome(w) = msg else { panic!("not welcome") };
        assert_eq!(w.people[0].role, Role::Owner);
        assert_eq!(w.people[1], Person { name: "Alice".into(), role: Role::Family });
    }

    #[test]
    fn handshake_errors() {
        for code in ["protocol_mismatch", "unknown_session", "session_busy", "bad_request"] {
            let json = format!(r#"{{"error":{{"code":"{code}","message":"..."}}}}"#);
            let msg = roundtrip_daemon(&json);
            let DaemonMessage::Error(e) = msg else { panic!("not error") };
            assert_eq!(e.code.as_str(), code);
        }
    }

    #[test]
    fn event() {
        let msg = roundtrip_daemon(
            r#"{"event":{"kind":"message","person":"Alice","role":"family","sender":"@alice:silta.test","room_id":"!abc:silta.test","event_id":"$xyz","ts":"2026-09-05T18:02:11Z","text":"hello","transcribed":false,"attachments":[]}}"#,
        );
        let DaemonMessage::Event(e) = msg else { panic!("not event") };
        assert_eq!(e.kind, EventKind::Message);
        assert_eq!(e.person, "Alice");
        assert!(e.attachments.is_empty());
    }

    #[test]
    fn event_with_in_reply_to() {
        let json = r#"{"event":{"kind":"message","person":"Bob","role":"owner","sender":"@bob:silta.test","room_id":"!abc:silta.test","event_id":"$q","ts":"2026-09-06T03:00:00Z","in_reply_to":"$xyz","text":"yes, that one","transcribed":false,"attachments":[]}}"#;
        let msg = roundtrip_daemon(json);
        let DaemonMessage::Event(e) = msg else { panic!("not event") };
        assert_eq!(e.in_reply_to.as_deref(), Some("$xyz"));
    }

    #[test]
    fn event_with_attachments() {
        let json = r#"{"event":{"kind":"message","person":"Alice","role":"family","sender":"@alice:silta.test","room_id":"!abc:silta.test","event_id":"$xyz","ts":"2026-09-05T18:02:11Z","text":"","transcribed":true,"attachments":[{"name":"a.ogg","mime":"audio/ogg","size":12,"path":"/var/lib/silta/inbox/a.ogg"}]}}"#;
        let msg = roundtrip_daemon(json);
        let DaemonMessage::Event(e) = msg else { panic!("not event") };
        assert_eq!(e.attachments[0].size, 12);
        assert_eq!(e.attachments[0].path, "/var/lib/silta/inbox/a.ogg");
    }

    #[test]
    fn thread_and_reaction_events() {
        let json = r#"{"event":{"kind":"message","person":"Alice","role":"family","sender":"@alice:silta.test","room_id":"!abc:silta.test","event_id":"$t2","ts":"2026-09-06T10:00:00Z","thread":"$root","text":"in the thread","transcribed":false,"attachments":[]}}"#;
        let DaemonMessage::Event(e) = roundtrip_daemon(json) else { panic!("not event") };
        assert_eq!(e.thread.as_deref(), Some("$root"));
        assert_eq!(e.in_reply_to, None);

        let json = r#"{"event":{"kind":"reaction","person":"Bob","role":"owner","sender":"@bob:silta.test","room_id":"!abc:silta.test","event_id":"$r","ts":"2026-09-06T10:00:01Z","reacts_to":"$bot","text":"👍","transcribed":false,"attachments":[]}}"#;
        let DaemonMessage::Event(e) = roundtrip_daemon(json) else { panic!("not event") };
        assert_eq!(e.kind, EventKind::Reaction);
        assert_eq!(e.kind.as_str(), "reaction");
        assert_eq!(e.reacts_to.as_deref(), Some("$bot"));
        assert_eq!(e.text, "👍");
    }

    #[test]
    fn reply_command() {
        let msg = roundtrip_client(
            r#"{"cmd":{"id":7,"reply":{"room_id":"!abc:silta.test","text":"**hi**","reply_to":"$xyz"}}}"#,
        );
        let ClientMessage::Cmd(cmd) = msg else { panic!("not cmd") };
        assert_eq!(cmd.id, 7);
        let CmdKind::Reply(r) = cmd.kind else { panic!("not reply") };
        assert_eq!(r.reply_to.as_deref(), Some("$xyz"));

        let msg = roundtrip_client(r#"{"cmd":{"id":8,"reply":{"room_id":"!abc:silta.test","text":"plain"}}}"#);
        let ClientMessage::Cmd(cmd) = msg else { panic!("not cmd") };
        let CmdKind::Reply(r) = cmd.kind else { panic!("not reply") };
        assert_eq!(r.reply_to, None);

        let msg = roundtrip_client(r#"{"cmd":{"id":9,"reply":{"room_id":"!abc:silta.test","text":"t","thread":"$root"}}}"#);
        let ClientMessage::Cmd(cmd) = msg else { panic!("not cmd") };
        let CmdKind::Reply(r) = cmd.kind else { panic!("not reply") };
        assert_eq!(r.thread.as_deref(), Some("$root"));
    }

    #[test]
    fn phase2_commands() {
        let msg = roundtrip_client(r#"{"cmd":{"id":1,"react":{"room_id":"!r","event_id":"$e","emoji":"👀"}}}"#);
        let ClientMessage::Cmd(cmd) = msg else { panic!("not cmd") };
        assert_eq!(cmd.kind.name(), "react");
        let CmdKind::React(r) = cmd.kind else { panic!("not react") };
        assert_eq!(r.emoji, "👀");

        let msg = roundtrip_client(r#"{"cmd":{"id":2,"edit":{"room_id":"!r","event_id":"$own","text":"done"}}}"#);
        let ClientMessage::Cmd(cmd) = msg else { panic!("not cmd") };
        assert_eq!(cmd.kind.name(), "edit");

        let msg = roundtrip_client(
            r#"{"cmd":{"id":3,"send_file":{"room_id":"!r","path":"/abs/report.md","caption":"the report","reply_to":"$e","thread":"$root"}}}"#,
        );
        let ClientMessage::Cmd(cmd) = msg else { panic!("not cmd") };
        let CmdKind::SendFile(f) = cmd.kind else { panic!("not send_file") };
        assert_eq!(f.caption.as_deref(), Some("the report"));
        let msg = roundtrip_client(r#"{"cmd":{"id":4,"send_file":{"room_id":"!r","path":"/abs/a.png"}}}"#);
        assert!(matches!(msg, ClientMessage::Cmd(Cmd { kind: CmdKind::SendFile(_), .. })));

        let msg = roundtrip_client(r#"{"cmd":{"id":5,"fetch_messages":{"room_id":"!r","limit":20,"from":"t1"}}}"#);
        let ClientMessage::Cmd(cmd) = msg else { panic!("not cmd") };
        let CmdKind::FetchMessages(f) = cmd.kind else { panic!("not fetch_messages") };
        assert_eq!(f.limit, Some(20));
        let msg = roundtrip_client(r#"{"cmd":{"id":6,"fetch_messages":{"room_id":"!r"}}}"#);
        assert!(matches!(msg, ClientMessage::Cmd(Cmd { kind: CmdKind::FetchMessages(_), .. })));

        let msg = roundtrip_client(r#"{"cmd":{"id":7,"fetch_message":{"room_id":"!r","event_id":"$e"}}}"#);
        let ClientMessage::Cmd(cmd) = msg else { panic!("not cmd") };
        assert_eq!(cmd.kind.name(), "fetch_message");
        let CmdKind::FetchMessage(f) = cmd.kind else { panic!("not fetch_message") };
        assert_eq!(f.event_id, "$e");
    }

    #[test]
    fn results() {
        let msg = roundtrip_daemon(r#"{"result":{"id":7,"ok":true,"event_id":"$new"}}"#);
        assert_eq!(msg, DaemonMessage::Result(CmdResult::ok(7, "$new")));

        let msg = roundtrip_daemon(
            r#"{"result":{"id":8,"ok":false,"error":"room_not_allowed","message":"session hub may not send to !..."}}"#,
        );
        assert_eq!(
            msg,
            DaemonMessage::Result(CmdResult::err(
                8,
                ResultError::RoomNotAllowed,
                "session hub may not send to !..."
            ))
        );
        for code in ["not_found", "file_error"] {
            let DaemonMessage::Result(r) =
                roundtrip_daemon(&format!(r#"{{"result":{{"id":1,"ok":false,"error":"{code}","message":"m"}}}}"#))
            else {
                panic!("not result")
            };
            assert_eq!(r.error.unwrap().as_str(), code);
        }

        // A newer daemon's error code is tolerated.
        let msg: DaemonMessage =
            serde_json::from_str(r#"{"result":{"id":9,"ok":false,"error":"quota","message":"m"}}"#).unwrap();
        let DaemonMessage::Result(r) = msg else { panic!("not result") };
        assert_eq!(r.error, Some(ResultError::Unknown));
    }

    #[test]
    fn history_result() {
        let json = r#"{"result":{"id":5,"ok":true,"messages":[{"event_id":"$e","sender":"@alice:x","person":"Alice","role":"family","own":false,"ts":"2026-09-06T10:00:00Z","in_reply_to":"$q","thread":"$root","text":"hi","attachments":[{"name":"a.pdf","mime":"application/pdf","size":10}]},{"event_id":"$b","sender":"@silta:x","own":true,"ts":"2026-09-06T09:59:00Z","text":"hello","attachments":[]}],"more":"t123"}}"#;
        let DaemonMessage::Result(r) = roundtrip_daemon(json) else { panic!("not result") };
        let messages = r.messages.unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].attachments[0].size, 10);
        assert!(messages[1].own);
        assert_eq!(r.more.as_deref(), Some("t123"));

        // The end of history: no token.
        let DaemonMessage::Result(r) = roundtrip_daemon(r#"{"result":{"id":6,"ok":true,"messages":[]}}"#) else {
            panic!("not result")
        };
        assert_eq!(r, CmdResult::history(6, Vec::new(), None));
    }

    #[test]
    fn classify_client_lines() {
        let good = parse_client_line(r#"{"hello":{"protocol":1,"session":"hub","client":"x"}}"#).unwrap();
        assert!(matches!(good, Incoming::Message(ClientMessage::Hello(_))));

        // A reserved command with an id: bad_request.
        let reserved = parse_client_line(r#"{"cmd":{"id":3,"typing":{"room_id":"!r","on":true}}}"#).unwrap();
        match reserved {
            Incoming::BadRequest { id, message } => {
                assert_eq!(id, 3);
                assert!(message.contains("typing"), "{message}");
            }
            other => panic!("unexpected {other:?}"),
        }

        // A reply missing a required field is also a bad request, not a disconnect.
        let malformed = parse_client_line(r#"{"cmd":{"id":4,"reply":{"text":"no room"}}}"#).unwrap();
        assert!(matches!(malformed, Incoming::BadRequest { id: 4, .. }));

        // Unknown top-level key without a command id: ignored.
        let unknown = parse_client_line(r#"{"ping":{}}"#).unwrap();
        assert!(matches!(unknown, Incoming::Ignored { .. }));

        // A command without an id cannot be answered: ignored.
        let no_id = parse_client_line(r#"{"cmd":{"reply":{"room_id":"!r","text":"t"}}}"#).unwrap();
        assert!(matches!(no_id, Incoming::Ignored { .. }));

        // Not JSON: the caller closes the connection.
        assert!(parse_client_line("hello there").is_err());
    }

    #[test]
    fn extra_fields_are_tolerated() {
        // A newer plugin may add fields; the daemon must not disconnect over them.
        let msg: ClientMessage =
            serde_json::from_str(r#"{"hello":{"protocol":1,"session":"hub","client":"x","features":["a"]}}"#).unwrap();
        assert!(matches!(msg, ClientMessage::Hello(_)));
        let msg: DaemonMessage = serde_json::from_str(
            r#"{"event":{"kind":"message","person":"A","role":"owner","sender":"@a:x","room_id":"!r","event_id":"$e","ts":"t","text":"x","transcribed":false,"attachments":[],"thread":"$t"}}"#,
        )
        .unwrap();
        assert!(matches!(msg, DaemonMessage::Event(_)));
    }
}
