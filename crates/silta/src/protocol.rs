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

/// One inbound Matrix message the session owns.
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
    pub text: String,
    pub transcribed: bool,
    /// Always present so that items can be added later without a protocol bump.
    pub attachments: Vec<Attachment>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Message,
}

/// A file the daemon downloaded into its inbox (not populated yet).
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
/// Reserved and rejected with `bad_request` until implemented: `react`, `edit`,
/// `send_file`, `fetch_messages`, `typing`, `permission_request`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CmdKind {
    Reply(Reply),
}

impl CmdKind {
    pub fn name(&self) -> &'static str {
        match self {
            CmdKind::Reply(_) => "reply",
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
}

impl CmdResult {
    pub fn ok(id: u64, event_id: impl Into<String>) -> Self {
        Self { id, ok: true, event_id: Some(event_id.into()), error: None, message: None }
    }

    pub fn err(id: u64, error: ResultError, message: impl Into<String>) -> Self {
        Self { id, ok: false, event_id: None, error: Some(error), message: Some(message.into()) }
    }
}

/// Error codes a command can fail with. `Unknown` catches codes from a newer daemon.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultError {
    RoomNotAllowed,
    RoomUnknown,
    SendFailed,
    BadRequest,
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
    fn event_with_attachment_is_forward_compatible() {
        let json = r#"{"event":{"kind":"message","person":"Alice","role":"family","sender":"@alice:silta.test","room_id":"!abc:silta.test","event_id":"$xyz","ts":"2026-09-05T18:02:11Z","text":"","transcribed":true,"attachments":[{"name":"a.ogg","mime":"audio/ogg","size":12,"path":"/var/lib/silta/inbox/a.ogg"}]}}"#;
        let msg = roundtrip_daemon(json);
        let DaemonMessage::Event(e) = msg else { panic!("not event") };
        assert_eq!(e.attachments[0].size, 12);
    }

    #[test]
    fn reply_command() {
        let msg = roundtrip_client(
            r#"{"cmd":{"id":7,"reply":{"room_id":"!abc:silta.test","text":"**hi**","reply_to":"$xyz"}}}"#,
        );
        let ClientMessage::Cmd(cmd) = msg else { panic!("not cmd") };
        assert_eq!(cmd.id, 7);
        let CmdKind::Reply(r) = cmd.kind;
        assert_eq!(r.reply_to.as_deref(), Some("$xyz"));

        let msg = roundtrip_client(r#"{"cmd":{"id":8,"reply":{"room_id":"!abc:silta.test","text":"plain"}}}"#);
        let ClientMessage::Cmd(cmd) = msg else { panic!("not cmd") };
        let CmdKind::Reply(r) = cmd.kind;
        assert_eq!(r.reply_to, None);
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

        // A newer daemon's error code is tolerated.
        let msg: DaemonMessage =
            serde_json::from_str(r#"{"result":{"id":9,"ok":false,"error":"quota","message":"m"}}"#).unwrap();
        let DaemonMessage::Result(r) = msg else { panic!("not result") };
        assert_eq!(r.error, Some(ResultError::Unknown));
    }

    #[test]
    fn classify_client_lines() {
        let good = parse_client_line(r#"{"hello":{"protocol":1,"session":"hub","client":"x"}}"#).unwrap();
        assert!(matches!(good, Incoming::Message(ClientMessage::Hello(_))));

        // Reserved command with an id: bad_request.
        let reserved = parse_client_line(r#"{"cmd":{"id":3,"react":{"room_id":"!r","event_id":"$e","emoji":"x"}}}"#).unwrap();
        match reserved {
            Incoming::BadRequest { id, message } => {
                assert_eq!(id, 3);
                assert!(message.contains("react"), "{message}");
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
