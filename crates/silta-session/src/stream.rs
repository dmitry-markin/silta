//! Claude Code's stream-json output, read twice from one line: reduced to a journal
//! line, and parsed for what the supervisor needs to know.
//!
//! `--verbose` is mandatory with `--output-format stream-json` and nothing turns the
//! message content off, so each JSON line becomes one short line that keeps what an
//! operator needs (session start, API retries and their status, the result of every
//! turn with its usage, which tools a message used, an error's text) and drops the
//! texts of the conversation. A line that is not JSON (Claude Code's own stderr, when it
//! is fed through here) passes unchanged.

use serde_json::{Map, Value};

/// What a line means to the supervisor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// The `system init` line of a start.
    Init { session_id: String, version: String },
    /// An assistant message. `subagent` marks a line of a subagent (a
    /// `parent_tool_use_id`). `context_tokens` is the size of the prompt of the API call
    /// that produced it (input plus cache read plus cache creation); `None` on a
    /// main-line message means the usage is missing, which the contract check reports.
    Assistant { subagent: bool, context_tokens: Option<u64> },
    /// A user message: a tool result, a timer wakeup, the compaction's own lines or a
    /// delivered message; a turn is in progress. `person` marks a delivery through the
    /// channel (the text starts with the `<channel` tag), the one kind that counts as
    /// a person's presence for the threshold's idle gap.
    User { person: bool },
    /// The end of a turn. `person` marks a turn a channel delivery started (the
    /// line's `origin`), which counts as a person's presence like the delivery's own
    /// `user` line and does not depend on that line being written.
    Result { is_error: bool, person: bool },
    /// Every live background task after a change, replacing what was known before
    /// (Claude Code's level signal, which cannot be wedged by a missed edge). Ambient
    /// tasks, which live as long as the session, are left out.
    BackgroundTasks { ids: Vec<String> },
    /// Claude Code compacted the conversation; `auto` when on its own.
    Compacted { auto: bool },
    /// Claude Code reported that a compaction did not go through (a `status` line
    /// with a `compact_result` other than `success`, seen when a hook blocks it).
    CompactionFailed,
    /// Anything else.
    Other,
}

/// The journal line and the event of one line of output.
pub fn read(line: &str) -> (String, Event) {
    let Ok(Value::Object(map)) = serde_json::from_str::<Value>(line) else {
        return (line.to_owned(), Event::Other);
    };
    let kind = map.get("type").and_then(Value::as_str).unwrap_or("?");
    match kind {
        "system" => (system(&map), system_event(&map)),
        "assistant" => (message("assistant", &map), Event::Assistant { subagent: subagent(&map), context_tokens: context_of(&map) }),
        "user" => (message("user", &map), Event::User { person: person_of(&map) }),
        "result" => {
            let is_error = map.get("is_error").and_then(Value::as_bool).unwrap_or(false);
            (result(&map), Event::Result { is_error, person: channel_origin(&map) })
        }
        "stream_event" => {
            let event = map.get("event").and_then(|e| e.get("type")).and_then(Value::as_str).unwrap_or("?");
            (format!("stream_event {event}"), Event::Other)
        }
        other => {
            let summary = match map.get("subtype").and_then(Value::as_str) {
                Some(subtype) => format!("{other} {subtype}"),
                None => other.to_owned(),
            };
            (summary, Event::Other)
        }
    }
}

/// One journal line for one line of output.
#[cfg(test)]
fn summarize(line: &str) -> String {
    read(line).0
}

fn system_event(map: &Map<String, Value>) -> Event {
    match map.get("subtype").and_then(Value::as_str) {
        Some("init") => Event::Init {
            session_id: str_of(map, "session_id").to_owned(),
            version: str_of(map, "claude_code_version").to_owned(),
        },
        Some("compact_boundary") => Event::Compacted {
            auto: map.get("compact_metadata").and_then(|m| m.get("trigger")).and_then(Value::as_str) == Some("auto"),
        },
        Some("status") => match map.get("compact_result").and_then(Value::as_str) {
            Some(result) if result != "success" => Event::CompactionFailed,
            _ => Event::Other,
        },
        Some("background_tasks_changed") => Event::BackgroundTasks {
            ids: map
                .get("tasks")
                .and_then(Value::as_array)
                .map(|tasks| {
                    tasks
                        .iter()
                        .filter(|t| t.get("ambient").and_then(Value::as_bool) != Some(true))
                        .filter_map(|t| t.get("task_id").and_then(Value::as_str).map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default(),
        },
        _ => Event::Other,
    }
}

/// The line's `origin` names the channel: Claude Code puts it on the replayed `user`
/// line of a delivery and on the `result` of the turn the delivery started.
fn channel_origin(map: &Map<String, Value>) -> bool {
    map.get("origin").and_then(|o| o.get("kind")).and_then(Value::as_str) == Some("channel")
}

/// A message delivered through the channel: its `origin` says so, or its text starts
/// with the `<channel` tag Claude Code wraps a channel notification in
/// (`docs/design.md`). A timer wakeup, a tool result and the compaction's own lines
/// do neither.
fn person_of(map: &Map<String, Value>) -> bool {
    if channel_origin(map) {
        return true;
    }
    let text = match map.get("message").and_then(|m| m.get("content")) {
        Some(Value::String(s)) => Some(s.as_str()),
        Some(Value::Array(blocks)) => blocks.iter().find_map(|b| b.get("text").and_then(Value::as_str)),
        _ => None,
    };
    text.is_some_and(|t| t.trim_start().starts_with("<channel"))
}

/// A line of a subagent's conversation rather than the main one.
fn subagent(map: &Map<String, Value>) -> bool {
    map.get("parent_tool_use_id").is_some_and(|v| !v.is_null())
}

/// The prompt size of the call behind an assistant message, if the line carries usage.
fn context_of(map: &Map<String, Value>) -> Option<u64> {
    let usage = map.get("message")?.get("usage")?;
    let n = |key: &str| usage.get(key).and_then(Value::as_u64).unwrap_or(0);
    Some(n("input_tokens") + n("cache_read_input_tokens") + n("cache_creation_input_tokens"))
}

fn system(map: &Map<String, Value>) -> String {
    let subtype = map.get("subtype").and_then(Value::as_str).unwrap_or("?");
    if subtype == "init" {
        return format!(
            "system init: session {}, model {}, Claude Code {}, permission mode {}, {} tools, {} mcp servers, {} plugins",
            str_of(map, "session_id"),
            str_of(map, "model"),
            str_of(map, "claude_code_version"),
            str_of(map, "permissionMode"),
            len_of(map, "tools"),
            len_of(map, "mcp_servers"),
            len_of(map, "plugins"),
        );
    }
    // Other system lines are short and operational (api_retry with its status, compaction
    // boundaries); keep their scalar fields and the size of lists, drop nested content
    // and the ids. A string stays only when it is an identifier or an enum (no
    // whitespace): a task notification's summary or a hook's output is prose that may
    // carry the conversation's content.
    let mut parts = Vec::new();
    // A compaction's numbers are one level down.
    let nested = map.get("compact_metadata").and_then(Value::as_object).into_iter().flatten();
    for (key, value) in map.iter().chain(nested) {
        if matches!(key.as_str(), "type" | "subtype" | "session_id" | "uuid" | "compact_metadata") {
            continue;
        }
        match value {
            Value::String(s) if !s.contains(char::is_whitespace) => parts.push(format!("{key}={}", shorten(s, 200))),
            Value::Number(n) => parts.push(format!("{key}={n}")),
            Value::Bool(b) => parts.push(format!("{key}={b}")),
            Value::Array(items) => parts.push(format!("{key}={}", items.len())),
            _ => {}
        }
    }
    format!("system {subtype}: {}", parts.join(" "))
}

/// A message: what kinds of blocks it holds and how big they are, never their text.
fn message(role: &str, map: &Map<String, Value>) -> String {
    let content = map.get("message").and_then(|m| m.get("content"));
    let mut texts = 0usize;
    let mut bytes = 0usize;
    let mut tools: Vec<String> = Vec::new();
    let mut results = 0usize;
    let mut errors = 0usize;
    let mut other = 0usize;
    match content {
        Some(Value::String(s)) => {
            texts += 1;
            bytes += s.len();
        }
        Some(Value::Array(blocks)) => {
            for block in blocks {
                match block.get("type").and_then(Value::as_str) {
                    Some("text") => {
                        texts += 1;
                        bytes += block.get("text").and_then(Value::as_str).map_or(0, str::len);
                    }
                    Some("tool_use") => {
                        tools.push(block.get("name").and_then(Value::as_str).unwrap_or("?").to_owned());
                    }
                    Some("tool_result") => {
                        results += 1;
                        if block.get("is_error").and_then(Value::as_bool) == Some(true) {
                            errors += 1;
                        }
                        bytes += match block.get("content") {
                            Some(Value::String(s)) => s.len(),
                            Some(Value::Array(items)) => items.iter().map(|i| i.get("text").and_then(Value::as_str).map_or(0, str::len)).sum(),
                            _ => 0,
                        };
                    }
                    _ => other += 1,
                }
            }
        }
        _ => {}
    }
    let mut parts = Vec::new();
    if texts > 0 {
        parts.push(format!("{texts} text ({bytes} bytes)"));
    }
    if !tools.is_empty() {
        parts.push(format!("tool_use {}", tools.join(",")));
    }
    if results > 0 {
        let flag = if errors > 0 { format!(", {errors} failed") } else { String::new() };
        parts.push(format!("{results} tool_result ({bytes} bytes{flag})"));
    }
    if other > 0 {
        parts.push(format!("{other} other"));
    }
    let message = map.get("message");
    if let Some(model) = message.and_then(|m| m.get("model")).and_then(Value::as_str) {
        parts.push(format!("model {model}"));
    }
    if let Some(stop) = message.and_then(|m| m.get("stop_reason")).and_then(Value::as_str) {
        parts.push(format!("stop {stop}"));
    }
    if role == "assistant" && !subagent(map) {
        if let Some(context) = context_of(map) {
            parts.push(format!("context {context}"));
        }
    }
    if role == "user" && person_of(map) {
        parts.insert(0, "channel".to_owned());
    }
    format!("{role}: {}", parts.join(", "))
}

/// The end of a turn: its outcome and cost, and the error text when it failed (an
/// error is operational, "Failed to authenticate" above all).
fn result(map: &Map<String, Value>) -> String {
    let subtype = map.get("subtype").and_then(Value::as_str).unwrap_or("?");
    let is_error = map.get("is_error").and_then(Value::as_bool).unwrap_or(false);
    let usage = map.get("usage");
    let tokens = |key: &str| usage.and_then(|u| u.get(key)).and_then(Value::as_u64).unwrap_or(0);
    let mut line = format!(
        "result {subtype}{}: {} turns, {} ms (api {} ms), tokens in {} out {} cache read {} write {}, cost ${:.4}",
        if is_error { " ERROR" } else { "" },
        num_of(map, "num_turns"),
        num_of(map, "duration_ms"),
        num_of(map, "duration_api_ms"),
        tokens("input_tokens"),
        tokens("output_tokens"),
        tokens("cache_read_input_tokens"),
        tokens("cache_creation_input_tokens"),
        map.get("total_cost_usd").and_then(Value::as_f64).unwrap_or(0.0),
    );
    if channel_origin(map) {
        line.push_str(", channel turn");
    }
    if let Some(denials) = map.get("permission_denials").and_then(Value::as_array).filter(|d| !d.is_empty()) {
        line.push_str(&format!(", {} permission denials", denials.len()));
    }
    if is_error {
        if let Some(text) = map.get("result").and_then(Value::as_str) {
            line.push_str(&format!("; {}", shorten(text, 500)));
        }
    }
    line
}

fn str_of<'a>(map: &'a Map<String, Value>, key: &str) -> &'a str {
    map.get(key).and_then(Value::as_str).unwrap_or("?")
}

fn num_of(map: &Map<String, Value>, key: &str) -> u64 {
    map.get(key).and_then(Value::as_u64).unwrap_or(0)
}

fn len_of(map: &Map<String, Value>, key: &str) -> usize {
    map.get(key).and_then(Value::as_array).map_or(0, Vec::len)
}

fn shorten(text: &str, max: usize) -> String {
    let flat: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= max {
        flat
    } else {
        format!("{}…", flat.chars().take(max).collect::<String>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_line_keeps_the_identifiers_and_drops_the_lists() {
        let line = r#"{"type":"system","subtype":"init","cwd":"/w","session_id":"af92","tools":["A","B","C"],"mcp_servers":[{"name":"silta","status":"connected"}],"model":"claude-opus-5","permissionMode":"auto","claude_code_version":"2.1.263","plugins":[]}"#;
        assert_eq!(
            summarize(line),
            "system init: session af92, model claude-opus-5, Claude Code 2.1.263, permission mode auto, 3 tools, 1 mcp servers, 0 plugins"
        );
        assert_eq!(read(line).1, Event::Init { session_id: "af92".into(), version: "2.1.263".into() });
    }

    #[test]
    fn api_retries_show_the_status_and_the_error_kind() {
        let line = r#"{"type":"system","subtype":"api_retry","attempt":1,"max_retries":10,"retry_delay_ms":599,"error_status":401,"error":"authentication_failed","session_id":"af92","uuid":"39c1"}"#;
        assert_eq!(
            summarize(line),
            "system api_retry: attempt=1 error=authentication_failed error_status=401 max_retries=10 retry_delay_ms=599"
        );
        assert_eq!(read(line).1, Event::Other);
        let task = r#"{"type":"system","subtype":"task_notification","task_id":"t1","status":"completed","summary":"Agent finished: the private answer","session_id":"af92"}"#;
        assert_eq!(summarize(task), "system task_notification: status=completed task_id=t1");
        assert_eq!(read(task).1, Event::Other);
        let compacted = r#"{"type":"system","subtype":"compact_boundary","compact_metadata":{"trigger":"auto","pre_tokens":167000},"session_id":"af92"}"#;
        assert_eq!(summarize(compacted), "system compact_boundary: pre_tokens=167000 trigger=auto");
        assert_eq!(read(compacted).1, Event::Compacted { auto: true });
        let manual = r#"{"type":"system","subtype":"compact_boundary","compact_metadata":{"trigger":"manual","pre_tokens":19922,"post_tokens":884,"duration_ms":8574,"preserved_segment":{"head_uuid":"e8"}},"session_id":"af92","uuid":"c1"}"#;
        assert_eq!(summarize(manual), "system compact_boundary: duration_ms=8574 post_tokens=884 pre_tokens=19922 trigger=manual");
        assert_eq!(read(manual).1, Event::Compacted { auto: false });
        let compacting = r#"{"type":"system","subtype":"status","status":"compacting","session_id":"af92","uuid":"4b"}"#;
        assert_eq!(summarize(compacting), "system status: status=compacting");
        assert_eq!(read(compacting).1, Event::Other);
        let done = r#"{"type":"system","subtype":"status","status":null,"compact_result":"success","session_id":"af92","uuid":"e1"}"#;
        assert_eq!(read(done).1, Event::Other);
        let blocked = r#"{"type":"system","subtype":"status","status":null,"compact_result":"failed","compact_error":"skipped: Compaction blocked by PreCompact hook: [x]: y","session_id":"af92","uuid":"e2"}"#;
        assert_eq!(summarize(blocked), "system status: compact_result=failed");
        assert_eq!(read(blocked).1, Event::CompactionFailed);
        let changed = r#"{"type":"system","subtype":"background_tasks_changed","tasks":[{"task_id":"t1","task_type":"local_agent","description":"look things up"},{"task_id":"m1","task_type":"monitor_ws","description":"watch","ambient":true}],"session_id":"af92"}"#;
        assert_eq!(summarize(changed), "system background_tasks_changed: tasks=2");
        assert_eq!(read(changed).1, Event::BackgroundTasks { ids: vec!["t1".into()] });
        let none = r#"{"type":"system","subtype":"background_tasks_changed","tasks":[],"session_id":"af92"}"#;
        assert_eq!(read(none).1, Event::BackgroundTasks { ids: vec![] });
    }

    #[test]
    fn messages_are_reduced_to_block_kinds_tools_and_sizes() {
        let assistant = r#"{"type":"assistant","message":{"model":"claude-opus-5","role":"assistant","stop_reason":"tool_use","content":[{"type":"text","text":"Sending the answer now."},{"type":"tool_use","name":"mcp__plugin_silta-claude_silta__reply","input":{"text":"secret"}}],"usage":{"input_tokens":2,"cache_read_input_tokens":24908,"cache_creation_input_tokens":12340,"output_tokens":236}},"parent_tool_use_id":null}"#;
        let (s, event) = read(assistant);
        assert_eq!(s, "assistant: 1 text (23 bytes), tool_use mcp__plugin_silta-claude_silta__reply, model claude-opus-5, stop tool_use, context 37250");
        assert!(!s.contains("secret"));
        assert_eq!(event, Event::Assistant { subagent: false, context_tokens: Some(37250) });
        let subagent = r#"{"type":"assistant","message":{"role":"assistant","content":[],"usage":{"input_tokens":5,"cache_read_input_tokens":100,"cache_creation_input_tokens":0}},"parent_tool_use_id":"toolu_1"}"#;
        let (s, event) = read(subagent);
        assert_eq!(event, Event::Assistant { subagent: true, context_tokens: Some(105) });
        assert!(!s.contains("context"), "{s}");
        let no_usage = r#"{"type":"assistant","message":{"role":"assistant","content":[]},"parent_tool_use_id":null}"#;
        assert_eq!(read(no_usage).1, Event::Assistant { subagent: false, context_tokens: None });
        let user = r#"{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"x","content":"sent","is_error":false},{"type":"tool_result","tool_use_id":"y","content":[{"type":"text","text":"boom"}],"is_error":true}]}}"#;
        assert_eq!(summarize(user), "user: 2 tool_result (8 bytes, 1 failed)");
        assert_eq!(read(user).1, Event::User { person: false });
        let channel = r#"{"type":"user","message":{"role":"user","content":"<channel person=\"Alice\">hello</channel>"}}"#;
        let s = summarize(channel);
        assert_eq!(s, "user: channel, 1 text (39 bytes)");
        assert!(!s.contains("hello"));
        assert_eq!(read(channel).1, Event::User { person: true });
        let blocks = r#"{"type":"user","message":{"role":"user","content":[{"type":"text","text":"<channel person=\"Bob\">hi</channel>"}]}}"#;
        assert_eq!(read(blocks).1, Event::User { person: true });
        // The replayed line of a delivery carries the origin, whatever its text.
        let origin = r#"{"type":"user","message":{"role":"user","content":"hello"},"isReplay":true,"origin":{"kind":"channel","server":"plugin:silta-claude:silta"}}"#;
        assert_eq!(read(origin).1, Event::User { person: true });
        let turn = r#"{"type":"result","subtype":"success","is_error":false,"num_turns":1,"usage":{},"origin":{"kind":"channel","server":"plugin:silta-claude:silta"}}"#;
        let (s, event) = read(turn);
        assert!(s.ends_with(", channel turn"), "{s}");
        assert_eq!(event, Event::Result { is_error: false, person: true });
        // A timer wakeup and the compaction's summary are user lines, not a person.
        let timer = r#"{"type":"user","message":{"role":"user","content":"Keep-warm: send nothing, answer ack."}}"#;
        assert_eq!(summarize(timer), "user: 1 text (36 bytes)");
        assert_eq!(read(timer).1, Event::User { person: false });
    }

    #[test]
    fn results_carry_usage_and_the_error_text_only_on_failure() {
        let ok = r#"{"type":"result","subtype":"success","is_error":false,"num_turns":3,"duration_ms":8123,"duration_api_ms":7000,"result":"the private answer","total_cost_usd":0.0421,"usage":{"input_tokens":12,"output_tokens":345,"cache_read_input_tokens":250000,"cache_creation_input_tokens":1000},"permission_denials":[]}"#;
        let (s, event) = read(ok);
        assert_eq!(s, "result success: 3 turns, 8123 ms (api 7000 ms), tokens in 12 out 345 cache read 250000 write 1000, cost $0.0421");
        assert!(!s.contains("private"));
        assert_eq!(event, Event::Result { is_error: false, person: false });
        let err = r#"{"type":"result","subtype":"success","is_error":true,"num_turns":1,"duration_ms":2000,"duration_api_ms":0,"result":"Failed to authenticate. API Error: 401 OAuth access token is invalid.","total_cost_usd":0,"usage":{},"permission_denials":[{"tool_name":"Bash"}]}"#;
        assert_eq!(
            summarize(err),
            "result success ERROR: 1 turns, 2000 ms (api 0 ms), tokens in 0 out 0 cache read 0 write 0, cost $0.0000, 1 permission denials; Failed to authenticate. API Error: 401 OAuth access token is invalid."
        );
        assert_eq!(read(err).1, Event::Result { is_error: true, person: false });
    }

    #[test]
    fn other_lines_pass_or_name_their_type() {
        assert_eq!(summarize("Shell cwd was reset to /home"), "Shell cwd was reset to /home");
        assert_eq!(summarize(r#"{"type":"stream_event","event":{"type":"content_block_delta","delta":{"text":"x"}}}"#), "stream_event content_block_delta");
        assert_eq!(summarize(r#"{"type":"control_response","subtype":"ack"}"#), "control_response ack");
        assert_eq!(summarize("[1, 2]"), "[1, 2]");
    }
}
