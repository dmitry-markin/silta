#!/usr/bin/env bash
# A stand-in for claude in the supervisor's tests: speaks enough stream-json for the
# supervisor, keeps a transcript per session id under HOME/.claude/projects like Claude
# Code, and logs what it was started with (the credential variables too) and every line
# it received to HOME/fake.log.
# HOME/fake-mode (read per line) is ok (default), error (the turn ends with an error
# result), hang (the turn never ends), hangonce (the same, once: the mode is reset to ok
# first), agents (the turn starts a background agent that the level signal drops two
# seconds after the turn), busy (a turn that writes another memory note ends first, as
# if a person's message had come with the line, and the handoff follows four seconds
# later), noresume (a --resume is refused), nocompact (a /compact is reported blocked),
# compacthang (a /compact never ends), compacthangonce (the same, once: the mode is
# reset to ok first) or busycompact (an unrelated turn ends before the compaction, as
# if a person's message had been queued ahead of the command).
# HOME/fake-context is the context size reported in every assistant line; a compaction
# sets it to 100, as the real one shrinks the context.
set -u
id=""; resume=""
while [ $# -gt 0 ]; do
  case $1 in --session-id) id=$2; shift;; --resume) resume=$2; shift;; esac
  shift
done
proj="$HOME/.claude/projects/-workspace"
mkdir -p "$proj"
mode() { cat "$HOME/fake-mode" 2>/dev/null || echo ok; }
init() { echo "{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"$id\",\"model\":\"fake\",\"claude_code_version\":\"0\",\"permissionMode\":\"auto\",\"tools\":[],\"mcp_servers\":[],\"plugins\":[]}"; }
assistant() { echo "{\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"$1\"}],\"usage\":{\"input_tokens\":1,\"cache_read_input_tokens\":$context,\"cache_creation_input_tokens\":0}},\"parent_tool_use_id\":null}"; }
result() { echo "{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"num_turns\":1,\"duration_ms\":1,\"duration_api_ms\":1,\"total_cost_usd\":0.001,\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"cache_read_input_tokens\":$context,\"cache_creation_input_tokens\":0}}"; }
if [ -n "$resume" ]; then
  if [ ! -f "$proj/$resume.jsonl" ] || [ "$(mode)" = noresume ]; then
    echo "No conversation found with session ID: $resume" >&2
    exit 1
  fi
  id=$resume
fi
echo "start $id ${resume:+resumed}" >> "$HOME/fake.log"
echo "auth oauth=${CLAUDE_CODE_OAUTH_TOKEN-unset} base=${ANTHROPIC_BASE_URL-unset} token=${ANTHROPIC_AUTH_TOKEN-unset} key=${ANTHROPIC_API_KEY-unset} discovery=${CLAUDE_CODE_ENABLE_GATEWAY_MODEL_DISCOVERY-unset}" >> "$HOME/fake.log"
context=$(cat "$HOME/fake-context" 2>/dev/null || echo 1000)
# What the plugin relies on: SILTA_READY_FILE is gone at the start and appears once the
# supervisor has read the init line.
[ -e "${SILTA_READY_FILE:-}" ] && echo "ready before init" >> "$HOME/fake.log"
init
for _ in 1 2 3 4 5 6 7 8 9 10; do
  [ -e "${SILTA_READY_FILE:-}" ] && { echo "ready after init" >> "$HOME/fake.log"; break; }
  sleep 0.2
done
touch "$proj/$id.jsonl"
while IFS= read -r line; do
  text=$(printf '%s' "$line" | sed 's/.*"content":"//; s/"}}$//')
  echo "line $text" >> "$HOME/fake.log"
  echo "$line" >> "$proj/$id.jsonl"
  context=$(cat "$HOME/fake-context" 2>/dev/null || echo 1000)
  case "$text" in "/compact "*)
    # The compaction exchange of Claude Code 2.1.268: two status lines, a new init, the
    # boundary, the summary as a user line, and a result of its own.
    case "$(mode)" in
      compacthang) sleep 30; exit 0 ;;
      compacthangonce) echo ok > "$HOME/fake-mode"; sleep 30; exit 0 ;;
      busycompact)
        assistant ok; result
        echo "aside $id" >> "$HOME/fake.log"
        sleep 2
        ;;
    esac
    echo '{"type":"system","subtype":"status","status":"compacting"}'
    if [ "$(mode)" = nocompact ]; then
      echo '{"type":"system","subtype":"status","status":null,"compact_result":"failed","compact_error":"skipped: Compaction blocked by PreCompact hook: [x]: y"}'
      assistant "Compaction blocked by PreCompact hook"
      echo "compact-failed $id" >> "$HOME/fake.log"
      result
      continue
    fi
    echo '{"type":"system","subtype":"status","status":null,"compact_result":"success"}'
    init
    echo "{\"type\":\"system\",\"subtype\":\"compact_boundary\",\"compact_metadata\":{\"trigger\":\"manual\",\"pre_tokens\":$context,\"post_tokens\":100}}"
    echo 100 > "$HOME/fake-context"
    echo '{"type":"user","message":{"role":"user","content":"This session is being continued from a previous conversation that ran out of context."}}'
    echo '{"type":"user","message":{"role":"user","content":"<local-command-stdout>Compacted </local-command-stdout>"}}'
    echo "compact $id" >> "$HOME/fake.log"
    echo '{"type":"result","subtype":"success","is_error":false,"num_turns":0,"duration_ms":0,"duration_api_ms":0,"total_cost_usd":0.001,"usage":{}}'
    continue
    ;;
  esac
  assistant ok
  case "$(mode)" in
    hang) sleep 30; exit 0 ;;
    hangonce) echo ok > "$HOME/fake-mode"; sleep 30; exit 0 ;;
    agents)
      echo '{"type":"system","subtype":"background_tasks_changed","tasks":[{"task_id":"bg1","task_type":"local_agent","description":"background"}]}'
      ( sleep 2; echo "background done" >> "$HOME/fake.log"; echo '{"type":"system","subtype":"background_tasks_changed","tasks":[]}' ) &
      echo ok > "$HOME/fake-mode"
      ;;
    busy)
      mkdir -p "$proj/memory"
      echo "note of $id" > "$proj/memory/self-and-alice.md"
      echo "aside $id" >> "$HOME/fake.log"
      result
      sleep 4
      echo ok > "$HOME/fake-mode"
      ;;
    error) echo '{"type":"result","subtype":"error_during_execution","is_error":true,"num_turns":1,"result":"API Error: 529 overloaded","usage":{}}'; continue ;;
  esac
  case "$text" in *"Write your handoff now"*)
    mkdir -p "$proj/memory"
    echo "handoff of $id" > "$proj/memory/handoff.md"
    echo "handoff $id" >> "$HOME/fake.log"
    ;;
  esac
  result
done
echo "eof $id" >> "$HOME/fake.log"
exit 0
