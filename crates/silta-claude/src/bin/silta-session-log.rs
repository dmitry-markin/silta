//! `silta-session-log`: Claude Code's stream-json output, reduced for the journal.
//!
//! The launcher pipes Claude Code's stdout and stderr through this filter. `--verbose`
//! is mandatory with `--output-format stream-json` and nothing turns the message
//! content off, so each JSON line becomes one short line that keeps what an operator
//! needs (session start, API retries and their status, the result of every turn with
//! its usage, which tools a message used, an error's text) and drops the texts of the
//! conversation. A line that is not JSON (Claude Code's own stderr) passes unchanged.

use std::io::{self, BufRead, Write};

use serde_json::Value;

fn main() {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut out = stdout.lock();
    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        if writeln!(out, "{}", summarize(&line)).and_then(|_| out.flush()).is_err() {
            break;
        }
    }
}

/// One journal line for one line of output.
fn summarize(line: &str) -> String {
    let Ok(Value::Object(map)) = serde_json::from_str::<Value>(line) else {
        return line.to_owned();
    };
    let kind = map.get("type").and_then(Value::as_str).unwrap_or("?");
    match kind {
        "system" => system(&map),
        "assistant" => message("assistant", &map),
        "user" => message("user", &map),
        "result" => result(&map),
        "stream_event" => {
            let event = map.get("event").and_then(|e| e.get("type")).and_then(Value::as_str).unwrap_or("?");
            format!("stream_event {event}")
        }
        other => match map.get("subtype").and_then(Value::as_str) {
            Some(subtype) => format!("{other} {subtype}"),
            None => other.to_owned(),
        },
    }
}

fn system(map: &serde_json::Map<String, Value>) -> String {
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
    // boundaries); keep their scalar fields, drop nested content and the ids. A string
    // stays only when it is an identifier or an enum (no whitespace): a task
    // notification's summary or a hook's output is prose that may carry the
    // conversation's content.
    let mut parts = Vec::new();
    for (key, value) in map {
        if matches!(key.as_str(), "type" | "subtype" | "session_id" | "uuid") {
            continue;
        }
        match value {
            Value::String(s) if !s.contains(char::is_whitespace) => parts.push(format!("{key}={}", shorten(s, 200))),
            Value::Number(n) => parts.push(format!("{key}={n}")),
            Value::Bool(b) => parts.push(format!("{key}={b}")),
            _ => {}
        }
    }
    format!("system {subtype}: {}", parts.join(" "))
}

/// A message: what kinds of blocks it holds and how big they are, never their text.
fn message(role: &str, map: &serde_json::Map<String, Value>) -> String {
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
    format!("{role}: {}", parts.join(", "))
}

/// The end of a turn: its outcome and cost, and the error text when it failed (an
/// error is operational, "Failed to authenticate" above all).
fn result(map: &serde_json::Map<String, Value>) -> String {
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

fn str_of<'a>(map: &'a serde_json::Map<String, Value>, key: &str) -> &'a str {
    map.get(key).and_then(Value::as_str).unwrap_or("?")
}

fn num_of(map: &serde_json::Map<String, Value>, key: &str) -> u64 {
    map.get(key).and_then(Value::as_u64).unwrap_or(0)
}

fn len_of(map: &serde_json::Map<String, Value>, key: &str) -> usize {
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
    }

    #[test]
    fn api_retries_show_the_status_and_the_error_kind() {
        let line = r#"{"type":"system","subtype":"api_retry","attempt":1,"max_retries":10,"retry_delay_ms":599,"error_status":401,"error":"authentication_failed","session_id":"af92","uuid":"39c1"}"#;
        assert_eq!(
            summarize(line),
            "system api_retry: attempt=1 error=authentication_failed error_status=401 max_retries=10 retry_delay_ms=599"
        );
        let task = r#"{"type":"system","subtype":"task_notification","task_id":"t1","status":"completed","summary":"Agent finished: the private answer","session_id":"af92"}"#;
        assert_eq!(summarize(task), "system task_notification: status=completed task_id=t1");
    }

    #[test]
    fn messages_are_reduced_to_block_kinds_tools_and_sizes() {
        let assistant = r#"{"type":"assistant","message":{"model":"claude-opus-5","role":"assistant","stop_reason":"tool_use","content":[{"type":"text","text":"Sending the answer now."},{"type":"tool_use","name":"mcp__plugin_silta-claude_silta__reply","input":{"text":"secret"}}]}}"#;
        let s = summarize(assistant);
        assert_eq!(s, "assistant: 1 text (23 bytes), tool_use mcp__plugin_silta-claude_silta__reply, model claude-opus-5, stop tool_use");
        assert!(!s.contains("secret"));
        let user = r#"{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"x","content":"sent","is_error":false},{"type":"tool_result","tool_use_id":"y","content":[{"type":"text","text":"boom"}],"is_error":true}]}}"#;
        assert_eq!(summarize(user), "user: 2 tool_result (8 bytes, 1 failed)");
        let channel = r#"{"type":"user","message":{"role":"user","content":"<channel person=\"Alice\">hello</channel>"}}"#;
        let s = summarize(channel);
        assert_eq!(s, "user: 1 text (39 bytes)");
        assert!(!s.contains("hello"));
    }

    #[test]
    fn results_carry_usage_and_the_error_text_only_on_failure() {
        let ok = r#"{"type":"result","subtype":"success","is_error":false,"num_turns":3,"duration_ms":8123,"duration_api_ms":7000,"result":"the private answer","total_cost_usd":0.0421,"usage":{"input_tokens":12,"output_tokens":345,"cache_read_input_tokens":250000,"cache_creation_input_tokens":1000},"permission_denials":[]}"#;
        let s = summarize(ok);
        assert_eq!(s, "result success: 3 turns, 8123 ms (api 7000 ms), tokens in 12 out 345 cache read 250000 write 1000, cost $0.0421");
        assert!(!s.contains("private"));
        let err = r#"{"type":"result","subtype":"success","is_error":true,"num_turns":1,"duration_ms":2000,"duration_api_ms":0,"result":"Failed to authenticate. API Error: 401 OAuth access token is invalid.","total_cost_usd":0,"usage":{},"permission_denials":[{"tool_name":"Bash"}]}"#;
        assert_eq!(
            summarize(err),
            "result success ERROR: 1 turns, 2000 ms (api 0 ms), tokens in 0 out 0 cache read 0 write 0, cost $0.0000, 1 permission denials; Failed to authenticate. API Error: 401 OAuth access token is invalid."
        );
    }

    #[test]
    fn other_lines_pass_or_name_their_type() {
        assert_eq!(summarize("Shell cwd was reset to /home"), "Shell cwd was reset to /home");
        assert_eq!(summarize(r#"{"type":"stream_event","event":{"type":"content_block_delta","delta":{"text":"x"}}}"#), "stream_event content_block_delta");
        assert_eq!(summarize(r#"{"type":"control_response","subtype":"ack"}"#), "control_response ack");
        assert_eq!(summarize("[1, 2]"), "[1, 2]");
    }
}
