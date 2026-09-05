#!/usr/bin/env bash
# Run the hub session headless with the silta-claude channel plugin.
#
#   dev/run-session.sh              new session (named silta-hub)
#   dev/run-session.sh <session-id> resume an earlier session
#
# stdin is a FIFO held open by this script; one initial user message
# starts the first turn, after which channel events arrive as turns. Output goes to
# dev/state/hub.log (stream-json) and dev/state/hub.err (Claude Code's stderr). The
# session id is printed and saved to dev/state/hub.session-id for later resume.
set -euo pipefail
repo=$(cd "$(dirname "$0")/.." && pwd)
state=$repo/dev/state
mkdir -p "$state"
fifo=$state/hub.fifo
[ -p "$fifo" ] || mkfifo "$fifo"

export SILTA_SESSION="${SILTA_SESSION:-hub}"
export SILTA_SOCKET="${SILTA_SOCKET:-$state/siltad.sock}"
export SILTA_CLAUDE_BIN="${SILTA_CLAUDE_BIN:-$repo/target/release/silta-claude}"
if [ ! -x "$SILTA_CLAUDE_BIN" ]; then
  echo "plugin binary not found at $SILTA_CLAUDE_BIN; run: cargo build --release -p silta-claude" >&2
  exit 1
fi

# The prompt-less form, and --channels last because it is variadic.
args=(-p --input-format stream-json --output-format stream-json --verbose
      --permission-mode auto --permission-prompts none)
if [ $# -ge 1 ]; then
  args+=(--resume "$1")
else
  args+=(--name silta-hub)
fi
args+=(--channels plugin:silta-claude@silta-local)

initial='{"type":"user","message":{"role":"user","content":"You are connected to the family chat through the silta channel. Wait for messages and answer each one through the reply tool."}}'

# When started from inside another Claude Code session, drop that session's variables so
# the nested session starts clean (harmless from a plain shell).
for v in $(env | grep -E '^CLAUDE' | cut -d= -f1); do
  case $v in CLAUDE_CODE_OAUTH_TOKEN|CLAUDE_CONFIG_DIR) ;; *) unset "$v" ;; esac
done

cd "$repo/dev/workspace"
: > "$state/hub.log"
claude "${args[@]}" < "$fifo" > "$state/hub.log" 2> "$state/hub.err" &
pid=$!
exec 3> "$fifo"
printf '%s\n' "$initial" >&3

id=""
for _ in $(seq 1 60); do
  id=$(grep -o '"session_id":"[^"]*"' "$state/hub.log" 2>/dev/null | head -1 | cut -d'"' -f4 || true)
  [ -n "$id" ] && break
  kill -0 "$pid" 2>/dev/null || break
  sleep 1
done
if [ -n "$id" ]; then
  echo "session id: $id"
  echo "$id" > "$state/hub.session-id"
else
  echo "no session id seen in $state/hub.log yet" >&2
fi
echo "claude pid $pid; log $state/hub.log; press Ctrl-C or close the FIFO to stop"
trap 'kill "$pid" 2>/dev/null || true' INT TERM
wait "$pid"
