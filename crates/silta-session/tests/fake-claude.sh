#!/usr/bin/env bash
# A stand-in for claude in the supervisor's tests: speaks enough stream-json for the
# supervisor, keeps a transcript per session id under HOME/.claude/projects like Claude
# Code, and logs what it was started with and every line it received to HOME/fake.log.
# HOME/fake-mode (read per turn) is ok (default), error (the turn ends with an error
# result), toolong (Claude Code's prompt-too-long message, then an error result), hang
# (the turn never ends), agents (the turn starts a foreground agent that never reports
# a completion and a background one that reports it two seconds after the turn, through
# the level signal alone), busy (a turn that writes another memory note ends first, as
# if a person's message had come with the line, and the handoff follows four seconds
# later) or noresume (a --resume is refused). HOME/fake-context is the context size
# reported in every assistant line.
set -u
id=""; resume=""
while [ $# -gt 0 ]; do
  case $1 in --session-id) id=$2; shift;; --resume) resume=$2; shift;; esac
  shift
done
proj="$HOME/.claude/projects/-workspace"
mkdir -p "$proj"
mode() { cat "$HOME/fake-mode" 2>/dev/null || echo ok; }
if [ -n "$resume" ]; then
  if [ ! -f "$proj/$resume.jsonl" ] || [ "$(mode)" = noresume ]; then
    echo "No conversation found with session ID: $resume" >&2
    exit 1
  fi
  id=$resume
fi
echo "start $id ${resume:+resumed}" >> "$HOME/fake.log"
echo "{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"$id\",\"model\":\"fake\",\"claude_code_version\":\"0\",\"permissionMode\":\"auto\",\"tools\":[],\"mcp_servers\":[],\"plugins\":[]}"
touch "$proj/$id.jsonl"
while IFS= read -r line; do
  text=$(printf '%s' "$line" | sed 's/.*"content":"//; s/"}}$//')
  echo "line $text" >> "$HOME/fake.log"
  echo "$line" >> "$proj/$id.jsonl"
  context=$(cat "$HOME/fake-context" 2>/dev/null || echo 1000)
  echo "{\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"ok\"}],\"usage\":{\"input_tokens\":1,\"cache_read_input_tokens\":$context,\"cache_creation_input_tokens\":0}},\"parent_tool_use_id\":null}"
  case "$(mode)" in
    hang) sleep 30; exit 0 ;;
    agents)
      echo '{"type":"system","subtype":"task_started","task_id":"fg1","description":"foreground","is_backgrounded":false}'
      echo '{"type":"system","subtype":"background_tasks_changed","tasks":[{"task_id":"bg1","task_type":"local_agent","description":"background"}]}'
      echo '{"type":"system","subtype":"task_started","task_id":"bg1","description":"background","is_backgrounded":true}'
      ( sleep 2; echo "background done" >> "$HOME/fake.log"; echo '{"type":"system","subtype":"background_tasks_changed","tasks":[]}' ) &
      echo ok > "$HOME/fake-mode"
      ;;
    busy)
      mkdir -p "$proj/memory"
      echo "note of $id" > "$proj/memory/self-and-alice.md"
      echo "aside $id" >> "$HOME/fake.log"
      echo '{"type":"result","subtype":"success","is_error":false,"num_turns":1,"duration_ms":1,"duration_api_ms":1,"total_cost_usd":0.001,"usage":{}}'
      sleep 4
      echo ok > "$HOME/fake-mode"
      ;;
    error) echo '{"type":"result","subtype":"error_during_execution","is_error":true,"num_turns":1,"result":"API Error: 529 overloaded","usage":{}}'; continue ;;
    toolong)
      echo '{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"Prompt is too long · automatic compaction failed: There is an issue with the selected model (opus5)."}],"usage":{"input_tokens":0,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}},"parent_tool_use_id":null}'
      echo '{"type":"result","subtype":"error_during_execution","is_error":true,"num_turns":1,"result":"Prompt is too long","usage":{}}'
      continue ;;
  esac
  case "$text" in *"Write your handoff now"*)
    mkdir -p "$proj/memory"
    echo "handoff of $id" > "$proj/memory/handoff.md"
    echo "handoff $id" >> "$HOME/fake.log"
    ;;
  esac
  echo "{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"num_turns\":1,\"duration_ms\":1,\"duration_api_ms\":1,\"total_cost_usd\":0.001,\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"cache_read_input_tokens\":$context,\"cache_creation_input_tokens\":0}}"
done
echo "eof $id" >> "$HOME/fake.log"
exit 0
