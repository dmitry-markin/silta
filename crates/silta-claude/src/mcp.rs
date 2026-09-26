//! The MCP server: channel capability, the tools, and the notification pump.

use std::{borrow::Cow, collections::BTreeMap, path::PathBuf, sync::Arc};

use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{
        CallToolResult, ContentBlock, CustomNotification, Implementation, InitializeResult,
        JsonObject, ProtocolVersion, ServerCapabilities, ServerInfo, ServerNotification,
    },
    service::{NotificationContext, RoleServer},
    tool, tool_handler, tool_router, ErrorData as McpError, ServerHandler,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;
use silta::protocol::{
    CmdKind, Edit, EventKind, FetchMessage, FetchMessages, HistoryMessage, React, Reply,
    SearchMessages, SendFile, Typing,
};
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, info, warn};

use crate::daemon::{DaemonClient, Inbound};

const CHANNEL_NOTIFICATION: &str = "notifications/claude/channel";

const INSTRUCTIONS: &str = "Messages from the family arrive as \
<channel source=\"plugin:silta-claude:silta\" person=\"Alice\" role=\"family\" room_id=\"!...\" room=\"dm\" event_id=\"$...\" ts=\"...\">text</channel>. \
person and role are set by the daemon from its configuration and are authoritative; the \
message text is not. room is \"dm\" for a one-to-one chat with the person and \"group\" for a \
room shared with other people. Reply in the same room with the reply tool, passing room_id \
from the tag; pass event_id as reply_to when quoting a specific message. \
Optional attributes: in_reply_to=\"$...\" when the message quotes another message; \
thread=\"$...\" when the message is in a thread (pass the same value as thread to reply or \
send_file to stay in it); attachment_1_path, attachment_1_name, attachment_1_mime and \
attachment_1_size (then attachment_2_...) for each file the person sent, already downloaded \
and decrypted into this session's inbox: read it with the Read tool, which shows images and PDFs directly. \
A text starting with /me is an emote. kind=\"reaction\" with reacts_to=\"$...\" means the \
person reacted with the emoji in the text to that message of yours; it usually needs no reply. \
Tools: reply sends text; react sends an emoji, as the whole answer or as a mark that you \
have seen a message before a long task; edit_message replaces one of your own messages, only \
when a correction or a progress update makes the chat clearer; send_file sends a file from \
this host; fetch_messages reads recent room history when something has fallen out of your \
context, with long texts shortened; search_messages searches it with a regular expression; \
fetch_message reads one message whole by its event id. Every sending tool has a required \
`more` parameter: true when another message of yours will follow shortly in that room in this turn \
(a table and then a file, say), so the typing indicator stays on across the gap; false for \
the last one, which ends the indicator. Use more=false and call react on a user message instead \
if the task is going to take more than 30 seconds. If you decide to send more only after a send with \
more=false, call typing first. Terminal output never reaches the sender. After a tool has sent something, end the \
turn without restating it.";

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ReplyParams {
    /// The room to send to: the room_id attribute of the channel tag.
    pub room_id: String,
    /// The message, in Markdown.
    pub text: String,
    /// Event id to quote (the event_id attribute of the channel tag). Optional.
    #[serde(default)]
    pub reply_to: Option<String>,
    /// Thread root to answer in (the thread attribute of the channel tag). Optional.
    #[serde(default)]
    pub thread: Option<String>,
    /// Required: true when another message of yours will follow in this room in this turn (the
    /// typing indicator stays on), false when this is the last one; if you decide only after this
    /// send that another message is coming, call typing first.
    pub more: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ReactParams {
    /// The room: the room_id attribute of the channel tag.
    pub room_id: String,
    /// The message to react to: its event_id.
    pub event_id: String,
    /// One emoji. E.g., 👀, if the answer will take more than 30 seconds.
    pub emoji: String,
    /// Required: true when a message of yours will follow shortly (less than 30 seconds) in this
    /// room in this turn, false when the reaction is the whole answer; if you decide only after
    /// this send that another message is coming, call typing first.
    pub more: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct EditParams {
    /// The room: the room_id attribute of the channel tag.
    pub room_id: String,
    /// One of your own messages: the event id a reply returned.
    pub event_id: String,
    /// The new text, in Markdown; must fit one message (8 KiB).
    pub text: String,
    /// Required: true when another message of yours will follow in this room in this turn, false
    /// otherwise; if you decide only after this send that another message is coming, call typing
    /// first.
    pub more: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SendFileParams {
    /// The room: the room_id attribute of the channel tag.
    pub room_id: String,
    /// Absolute path of an existing file this session can read.
    pub path: String,
    /// Text shown with the file, in Markdown. Optional.
    #[serde(default)]
    pub caption: Option<String>,
    /// Event id to quote. Optional.
    #[serde(default)]
    pub reply_to: Option<String>,
    /// Thread root to send in. Optional.
    #[serde(default)]
    pub thread: Option<String>,
    /// Required: true when another message of yours will follow in this room in this turn, false
    /// when this is the last one; if you decide only after this send that another message is
    /// coming, call typing first.
    pub more: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct TypingParams {
    /// The room: the room_id attribute of the channel tag.
    pub room_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct FetchMessagesParams {
    /// The room: the room_id attribute of the channel tag.
    pub room_id: String,
    /// How many messages, newest first. Default 20, at most 100.
    #[serde(default)]
    pub limit: Option<u32>,
    /// The `more` token of an earlier result, to page further back. Optional.
    #[serde(default)]
    pub from: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct FetchMessageParams {
    /// The room: the room_id attribute of the channel tag.
    pub room_id: String,
    /// The message: an event_id from a fetch_messages line or a channel tag.
    pub event_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SearchMessagesParams {
    /// The room: the room_id attribute of the channel tag.
    pub room_id: String,
    /// A regular expression (Rust regex syntax). Case-insensitive unless it starts with (?-i).
    pub pattern: String,
    /// Matches wanted, newest first. Default 20, at most 100.
    #[serde(default)]
    pub limit: Option<u32>,
    /// The `more` token of an earlier result, to continue the scan further back. Optional.
    #[serde(default)]
    pub from: Option<String>,
}

#[derive(Clone)]
pub struct SiltaChannel {
    daemon: DaemonClient,
    startup: Arc<Mutex<Option<Startup>>>,
    tool_router: ToolRouter<Self>,
}

/// What the client's `initialized` starts: the connection to the daemon (`ready`) and
/// the pump that turns its events into channel notifications.
struct Startup {
    events: mpsc::Receiver<Inbound>,
    ready: oneshot::Sender<()>,
}

#[tool_router]
impl SiltaChannel {
    pub fn new(
        daemon: DaemonClient,
        events: mpsc::Receiver<Inbound>,
        ready: oneshot::Sender<()>,
    ) -> Self {
        SiltaChannel {
            daemon,
            startup: Arc::new(Mutex::new(Some(Startup { events, ready }))),
            tool_router: Self::tool_router(),
        }
    }

    #[tool(
        name = "reply",
        description = "Send a text message to a Matrix room. Markdown is rendered; long text is split into several messages. Pass reply_to to quote a specific message and thread to answer inside a thread. `more` is required: true keeps the typing indicator on because another message of yours follows in this turn, false ends it. Fails with a code: room_not_allowed (policy), room_unknown (the bot is not in that room), not_found (the quoted event does not exist), send_failed, daemon_unavailable (no daemon connection)."
    )]
    async fn reply(
        &self,
        Parameters(p): Parameters<ReplyParams>,
    ) -> Result<CallToolResult, McpError> {
        self.send(CmdKind::Reply(Reply {
            room_id: p.room_id,
            text: p.text,
            reply_to: p.reply_to,
            thread: p.thread,
            more: p.more,
        }))
        .await
    }

    #[tool(
        name = "react",
        description = "React to a message with an emoji. Use it when a reaction is the whole answer (a thumbs-up to a thank-you, say) or to mark that you have seen a message before starting a task that will likely take more than 30 seconds. more=true keeps the typing indicator on if your next message is expected within 30 seconds. Same failure codes as reply."
    )]
    async fn react(
        &self,
        Parameters(p): Parameters<ReactParams>,
    ) -> Result<CallToolResult, McpError> {
        self.send(CmdKind::React(React {
            room_id: p.room_id,
            event_id: p.event_id,
            emoji: p.emoji,
            more: p.more,
        }))
        .await
    }

    #[tool(
        name = "edit_message",
        description = "Replace the text of one of your own earlier messages (the event id a reply returned). Use it narrowly: a progress message that becomes the result, or a correction; never to rewrite a conversation. The new text must fit one message of 8 KiB. Fails with not_found when the event is not your own message, bad_request when the text is too long, plus the codes of reply."
    )]
    async fn edit_message(
        &self,
        Parameters(p): Parameters<EditParams>,
    ) -> Result<CallToolResult, McpError> {
        self.send(CmdKind::Edit(Edit {
            room_id: p.room_id,
            event_id: p.event_id,
            text: p.text,
            more: p.more,
        }))
        .await
    }

    #[tool(
        name = "send_file",
        description = "Send a file from this host into a room: an absolute path of an existing file this session can read. Images, audio and video are shown as such, everything else as a file. Optional caption (Markdown), reply_to and thread as for reply. Fails with file_error when the path is not a readable regular file or the file is larger than the server accepts, plus the codes of reply."
    )]
    async fn send_file(
        &self,
        Parameters(p): Parameters<SendFileParams>,
    ) -> Result<CallToolResult, McpError> {
        let cmd = SendFile {
            room_id: p.room_id,
            transfer: String::new(),
            caption: p.caption,
            reply_to: p.reply_to,
            thread: p.thread,
            more: p.more,
        };
        match self.daemon.send_file(PathBuf::from(p.path), cmd).await {
            Ok(result) => Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                "sent: {}",
                result.event_id.unwrap_or_default()
            ))])),
            Err(err) => {
                warn!("send_file failed: {err}");
                Ok(CallToolResult::error(vec![ContentBlock::text(
                    err.to_string(),
                )]))
            }
        }
    }

    #[tool(
        name = "typing",
        description = "Show the typing indicator in a room again. Use it when, after a send with more=false, you decide to send another message there after all; the indicator then runs until your next send or for at most ten minutes. Not needed when the previous send said more=true."
    )]
    async fn typing(
        &self,
        Parameters(p): Parameters<TypingParams>,
    ) -> Result<CallToolResult, McpError> {
        match self
            .daemon
            .command(CmdKind::Typing(Typing { room_id: p.room_id }))
            .await
        {
            Ok(_) => Ok(CallToolResult::success(vec![ContentBlock::text("typing")])),
            Err(err) => {
                warn!("typing failed: {err}");
                Ok(CallToolResult::error(vec![ContentBlock::text(
                    err.to_string(),
                )]))
            }
        }
    }

    #[tool(
        name = "fetch_messages",
        description = "Fetch recent messages of a room, newest first (at least `limit` when the room has them, default 20, at most 100; the last page is returned whole so a few more may come), one JSON object per line with event_id, person (absent for your own messages, which have own=true), ts, text (shortened to 300 characters), in_reply_to, thread and attachments (names only, not downloaded). The last line carries a `more` token to pass as `from` for older messages, when there are any. Keep the limit small and page when needed. Use it when something the person refers to has fallen out of your context. Fails with room_not_allowed when this session does not own the room."
    )]
    async fn fetch_messages(
        &self,
        Parameters(p): Parameters<FetchMessagesParams>,
    ) -> Result<CallToolResult, McpError> {
        let kind = CmdKind::FetchMessages(FetchMessages {
            room_id: p.room_id,
            limit: p.limit,
            from: p.from,
        });
        match self.daemon.command(kind).await {
            Ok(result) => {
                let messages = result.messages.unwrap_or_default();
                let mut lines: Vec<String> = messages
                    .iter()
                    .map(|m| history_line(m, Some(HISTORY_TEXT_CHARS)))
                    .collect();
                if lines.is_empty() {
                    lines.push("(no messages)".to_owned());
                }
                match result.more {
                    Some(token) => lines.push(format!("more: {token}")),
                    None => lines.push("(start of the room's history)".to_owned()),
                }
                Ok(CallToolResult::success(vec![ContentBlock::text(
                    lines.join("\n"),
                )]))
            }
            Err(err) => {
                warn!("fetch_messages failed: {err}");
                Ok(CallToolResult::error(vec![ContentBlock::text(
                    err.to_string(),
                )]))
            }
        }
    }

    #[tool(
        name = "search_messages",
        description = "Search a room's history with a regular expression, newest first: the message texts and attachment names, case-insensitive unless the pattern turns it off with (?-i). Use alternation, character classes and \\b freely, and escape literal punctuation. Returns up to `limit` matches (default 20, at most 100) as lines like fetch_messages, each text windowed around its first match, then a line saying how many events were scanned and back to when, and `more: <token>` when the scan stopped before the start of the room (pass it as `from` to continue; a call scans at most 1000 events). Fails with bad_request for an invalid pattern and room_not_allowed when this session does not own the room."
    )]
    async fn search_messages(
        &self,
        Parameters(p): Parameters<SearchMessagesParams>,
    ) -> Result<CallToolResult, McpError> {
        let kind = CmdKind::SearchMessages(SearchMessages {
            room_id: p.room_id,
            pattern: p.pattern,
            limit: p.limit,
            from: p.from,
        });
        match self.daemon.command(kind).await {
            Ok(result) => {
                let messages = result.messages.unwrap_or_default();
                let mut lines: Vec<String> = messages
                    .iter()
                    .map(|m| history_line(m, Some(HISTORY_TEXT_CHARS)))
                    .collect();
                if lines.is_empty() {
                    lines.push("(no matches)".to_owned());
                }
                lines.push(format!(
                    "scanned {} events back to {}",
                    result.scanned.unwrap_or(0),
                    result.until.unwrap_or_else(|| "(nothing)".to_owned())
                ));
                match result.more {
                    Some(token) => lines.push(format!("more: {token}")),
                    None => {
                        lines.push("(searched back to the start of the room's history)".to_owned())
                    }
                }
                Ok(CallToolResult::success(vec![ContentBlock::text(
                    lines.join("\n"),
                )]))
            }
            Err(err) => {
                warn!("search_messages failed: {err}");
                Ok(CallToolResult::error(vec![ContentBlock::text(
                    err.to_string(),
                )]))
            }
        }
    }

    #[tool(
        name = "fetch_message",
        description = "Fetch one message whole by its event id (from a fetch_messages line or a channel tag): the same fields as fetch_messages, with the full text. Use it after fetch_messages when you need the whole text of a shortened message. Fails with not_found when the event does not exist or is not a message from a registered person or yourself, and with room_not_allowed when this session does not own the room."
    )]
    async fn fetch_message(
        &self,
        Parameters(p): Parameters<FetchMessageParams>,
    ) -> Result<CallToolResult, McpError> {
        let kind = CmdKind::FetchMessage(FetchMessage {
            room_id: p.room_id,
            event_id: p.event_id,
        });
        match self.daemon.command(kind).await {
            Ok(result) => {
                let line = result
                    .messages
                    .unwrap_or_default()
                    .first()
                    .map(|m| history_line(m, None))
                    .unwrap_or_default();
                Ok(CallToolResult::success(vec![ContentBlock::text(line)]))
            }
            Err(err) => {
                warn!("fetch_message failed: {err}");
                Ok(CallToolResult::error(vec![ContentBlock::text(
                    err.to_string(),
                )]))
            }
        }
    }
}

impl SiltaChannel {
    /// Run a command that sends something; the tool result is the new event id.
    async fn send(&self, kind: CmdKind) -> Result<CallToolResult, McpError> {
        let name = kind.name();
        match self.daemon.command(kind).await {
            Ok(result) => Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                "sent: {}",
                result.event_id.unwrap_or_default()
            ))])),
            Err(err) => {
                warn!("{name} failed: {err}");
                Ok(CallToolResult::error(vec![ContentBlock::text(
                    err.to_string(),
                )]))
            }
        }
    }
}

/// Longest text shown for one history message; the tool is for finding messages, not
/// for reading long ones whole, and Claude Code refuses tool results over a token cap.
const HISTORY_TEXT_CHARS: usize = 300;

/// One history message as a compact JSON line for the model; `max_chars` shortens the
/// text, `None` keeps it whole.
fn history_line(m: &HistoryMessage, max_chars: Option<usize>) -> String {
    let mut line = serde_json::Map::new();
    line.insert("event_id".into(), json!(m.event_id));
    if m.own {
        line.insert("own".into(), json!(true));
    } else if let Some(person) = &m.person {
        line.insert("person".into(), json!(person));
    }
    line.insert("ts".into(), json!(m.ts));
    if let Some(v) = &m.in_reply_to {
        line.insert("in_reply_to".into(), json!(v));
    }
    if let Some(v) = &m.thread {
        line.insert("thread".into(), json!(v));
    }
    let chars = m.text.chars().count();
    if let Some(max) = max_chars.filter(|&max| chars > max) {
        // A window around the first match when there is one, the start otherwise.
        let start = m
            .match_start
            .map(|at| at.saturating_sub(max / 3).min(chars - max))
            .unwrap_or(0);
        let window: String = m.text.chars().skip(start).take(max).collect();
        let after = chars - start - max;
        let mut text = String::new();
        if start > 0 {
            text.push_str(&format!("[{start} characters] …"));
        }
        text.push_str(&window);
        if after > 0 {
            text.push_str(&format!("… [{after} more characters]"));
        }
        line.insert("text".into(), json!(text));
    } else {
        line.insert("text".into(), json!(m.text));
    }
    if !m.attachments.is_empty() {
        let names: Vec<String> = m
            .attachments
            .iter()
            .map(|a| format!("{} ({}, {} bytes)", a.name, a.mime, a.size))
            .collect();
        line.insert("attachments".into(), json!(names));
    }
    serde_json::to_string(&line).unwrap_or_default()
}

/// The `<channel>` tag for an event: its attributes and its body. An attachment whose
/// transfer did not arrive has no path attribute and is named in the text instead.
fn channel_params(inbound: &Inbound) -> serde_json::Value {
    let event = &inbound.event;
    let mut meta = serde_json::Map::new();
    if event.kind != EventKind::Message {
        meta.insert("kind".into(), json!(event.kind.as_str()));
    }
    // The sender's Matrix id stays out of the tag: the model needs the person and the
    // role, which the daemon set from it, and the address would otherwise end up in the
    // provider's logs with every message.
    meta.insert("person".into(), json!(event.person));
    meta.insert("role".into(), json!(event.role.as_str()));
    meta.insert("room_id".into(), json!(event.room_id));
    meta.insert("room".into(), json!(event.room.as_str()));
    meta.insert("event_id".into(), json!(event.event_id));
    meta.insert("ts".into(), json!(event.ts));
    if let Some(in_reply_to) = &event.in_reply_to {
        meta.insert("in_reply_to".into(), json!(in_reply_to));
    }
    if let Some(thread) = &event.thread {
        meta.insert("thread".into(), json!(thread));
    }
    if let Some(reacts_to) = &event.reacts_to {
        meta.insert("reacts_to".into(), json!(reacts_to));
    }
    let mut missing = Vec::new();
    for (i, attachment) in event.attachments.iter().enumerate() {
        let n = i + 1;
        meta.insert(format!("attachment_{n}_name"), json!(attachment.name));
        meta.insert(format!("attachment_{n}_mime"), json!(attachment.mime));
        meta.insert(
            format!("attachment_{n}_size"),
            json!(attachment.size.to_string()),
        );
        match inbound.paths.get(i).and_then(|p| p.as_ref()) {
            Some(path) => {
                meta.insert(
                    format!("attachment_{n}_path"),
                    json!(path.to_string_lossy()),
                );
            }
            None => missing.push(format!(
                "[attachment \"{}\" was not received; ask for it again]",
                attachment.name
            )),
        }
    }
    let mut content = if event.text.is_empty() && !event.attachments.is_empty() {
        event
            .attachments
            .iter()
            .map(|a| format!("[attachment: {}]", a.name))
            .collect::<Vec<_>>()
            .join("\n")
    } else {
        event.text.clone()
    };
    for line in missing {
        if !content.is_empty() {
            content.push('\n');
        }
        content.push_str(&line);
    }
    json!({ "content": content, "meta": meta })
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for SiltaChannel {
    fn get_info(&self) -> ServerInfo {
        let mut capabilities = ServerCapabilities::builder().enable_tools().build();
        capabilities.experimental = Some(BTreeMap::from([(
            "claude/channel".to_owned(),
            JsonObject::new(),
        )]));
        InitializeResult::new(capabilities)
            .with_server_info(Implementation::new(
                "silta-claude",
                env!("CARGO_PKG_VERSION"),
            ))
            .with_protocol_version(ProtocolVersion::V_2024_11_05)
            .with_instructions(INSTRUCTIONS)
    }

    /// Claude Code does not register servers on the 2026-07-28 revision as channels;
    /// offering only 2024-11-05 makes the negotiation land there every time.
    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Borrowed(&[ProtocolVersion::V_2024_11_05])
    }

    async fn on_initialized(&self, context: NotificationContext<RoleServer>) {
        let Some(Startup { mut events, ready }) = self.startup.lock().await.take() else {
            warn!("initialized twice; the notification pump is already running");
            return;
        };
        info!("client initialized, connecting to the daemon and starting the notification pump");
        let _ = ready.send(());
        let peer = context.peer.clone();
        let daemon = self.daemon.clone();
        tokio::spawn(async move {
            while let Some(inbound) = events.recv().await {
                let notification =
                    CustomNotification::new(CHANNEL_NOTIFICATION, Some(channel_params(&inbound)));
                match peer
                    .send_notification(ServerNotification::CustomNotification(notification))
                    .await
                {
                    Ok(()) => {
                        debug!(
                            kind = inbound.event.kind.as_str(),
                            "channel notification delivered"
                        );
                        // Only now does the daemon count the event as delivered; until
                        // the acknowledgement it would come again.
                        daemon.ack(inbound.event.event_id).await;
                    }
                    Err(err) => {
                        warn!("cannot deliver channel notification, stopping the pump: {err}");
                        return;
                    }
                }
            }
            debug!("event stream ended, pump stopped");
        });
    }

    async fn on_custom_notification(
        &self,
        notification: CustomNotification,
        _context: NotificationContext<RoleServer>,
    ) {
        // A later version handles notifications/claude/channel/permission_request here.
        info!(method = %notification.method, "ignoring custom notification");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use silta::protocol::{Attachment, Event, Role, RoomKind};

    fn event() -> Event {
        Event {
            kind: EventKind::Message,
            person: "Alice".into(),
            role: Role::Family,
            sender: "@alice:x".into(),
            room_id: "!r:x".into(),
            room: RoomKind::Dm,
            event_id: "$e".into(),
            ts: "2026-09-06T10:00:00Z".into(),
            in_reply_to: None,
            thread: None,
            reacts_to: None,
            text: String::new(),
            transcribed: false,
            attachments: Vec::new(),
        }
    }

    #[test]
    fn attachments_become_numbered_attributes_and_a_body() {
        let mut e = event();
        e.attachments.push(Attachment {
            transfer: "e-1".into(),
            name: "photo.jpg".into(),
            mime: "image/jpeg".into(),
            size: 1234,
        });
        let inbound = Inbound {
            event: e.clone(),
            paths: vec![Some(PathBuf::from("/inbox/e-1-photo.jpg"))],
        };
        let params = channel_params(&inbound);
        assert_eq!(params["content"], "[attachment: photo.jpg]");
        assert_eq!(params["meta"]["attachment_1_path"], "/inbox/e-1-photo.jpg");
        assert_eq!(params["meta"]["attachment_1_size"], "1234");
        assert!(params["meta"].get("kind").is_none());
        assert_eq!(params["meta"]["room"], "dm");
        assert!(
            params["meta"].get("sender").is_none(),
            "the Matrix id must not reach the model"
        );
        e.text = "look at this".into();
        let inbound = Inbound {
            event: e.clone(),
            paths: vec![Some(PathBuf::from("/inbox/e-1-photo.jpg"))],
        };
        assert_eq!(channel_params(&inbound)["content"], "look at this");
        // A transfer that never arrived: no path, and the text says so.
        let inbound = Inbound {
            event: e,
            paths: vec![None],
        };
        let params = channel_params(&inbound);
        assert!(params["meta"].get("attachment_1_path").is_none());
        assert_eq!(
            params["content"],
            "look at this\n[attachment \"photo.jpg\" was not received; ask for it again]"
        );
    }

    #[test]
    fn history_lines_are_compact_and_short() {
        use silta::protocol::HistoryAttachment;
        let long = HistoryMessage {
            event_id: "$e".into(),
            sender: "@silta:x".into(),
            person: None,
            role: None,
            own: true,
            ts: "t".into(),
            in_reply_to: None,
            thread: Some("$root".into()),
            text: "ж".repeat(1000),
            match_start: None,
            attachments: vec![HistoryAttachment {
                name: "a.pdf".into(),
                mime: "application/pdf".into(),
                size: 10,
            }],
        };
        let whole: serde_json::Value = serde_json::from_str(&history_line(&long, None)).unwrap();
        assert_eq!(whole["text"].as_str().unwrap().chars().count(), 1000);
        let line = history_line(&long, Some(HISTORY_TEXT_CHARS));
        let v: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(v["own"], true);
        assert!(v.get("person").is_none() && v.get("sender").is_none());
        assert!(v["text"]
            .as_str()
            .unwrap()
            .ends_with("… [700 more characters]"));
        assert_eq!(
            v["text"].as_str().unwrap().chars().count(),
            300 + "… [700 more characters]".chars().count()
        );
        assert_eq!(v["attachments"][0], "a.pdf (application/pdf, 10 bytes)");
        assert_eq!(v["thread"], "$root");
    }

    #[test]
    fn search_hits_are_windowed_around_the_match() {
        let mut m = HistoryMessage {
            event_id: "$e".into(),
            sender: "@alice:x".into(),
            person: Some("Alice".into()),
            role: None,
            own: false,
            ts: "t".into(),
            in_reply_to: None,
            thread: None,
            text: format!("{}NEEDLE{}", "a".repeat(1000), "b".repeat(1000)),
            match_start: Some(1000),
            attachments: Vec::new(),
        };
        let v: serde_json::Value = serde_json::from_str(&history_line(&m, Some(300))).unwrap();
        let text = v["text"].as_str().unwrap();
        assert!(text.starts_with("[900 characters] …a"), "{text}");
        assert!(text.contains("NEEDLE"));
        assert!(text.ends_with("… [806 more characters]"), "{text}");
        // A match near the end still gets a full window.
        m.match_start = Some(2000);
        let v: serde_json::Value = serde_json::from_str(&history_line(&m, Some(300))).unwrap();
        let text = v["text"].as_str().unwrap();
        assert!(
            text.starts_with("[1706 characters] …")
                && text.ends_with('b')
                && !text.contains("more characters"),
            "{text}"
        );
    }

    #[test]
    fn reactions_and_threads_are_marked() {
        let mut e = event();
        e.kind = EventKind::Reaction;
        e.reacts_to = Some("$bot".into());
        e.text = "👍".into();
        let params = channel_params(&Inbound {
            event: e,
            paths: Vec::new(),
        });
        assert_eq!(params["meta"]["kind"], "reaction");
        assert_eq!(params["meta"]["reacts_to"], "$bot");
        assert_eq!(params["content"], "👍");

        let mut e = event();
        e.thread = Some("$root".into());
        e.in_reply_to = Some("$q".into());
        let params = channel_params(&Inbound {
            event: e,
            paths: Vec::new(),
        });
        assert_eq!(params["meta"]["thread"], "$root");
        assert_eq!(params["meta"]["in_reply_to"], "$q");
    }
}
