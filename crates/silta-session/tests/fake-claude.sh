#!/usr/bin/env bash
# A stand-in for claude in the supervisor's tests: speaks enough stream-json for the
# supervisor, keeps a transcript per session id under HOME/.claude/projects like Claude
# Code, and logs what it was started with and every line it received to HOME/fake.log.
# HOME/fake-mode (read per turn) is ok (default), error (the turn ends with an error
# result), hang (the turn never ends) or noresume (a --resume is refused). HOME/fake-context
# is the context size reported in every assistant line.
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
    error) echo '{"type":"result","subtype":"error_during_execution","is_error":true,"num_turns":1,"result":"API Error: 529 overloaded","usage":{}}'; continue ;;
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
