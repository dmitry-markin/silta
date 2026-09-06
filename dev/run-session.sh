#!/usr/bin/env bash
# Run the hub session headless with the silta-claude channel plugin.
#
#   dev/run-session.sh              new session (named silta-hub)
#   dev/run-session.sh <session-id> resume an earlier session
#
# stdin is a FIFO held open by this script; one initial user message
# starts the first turn, after which channel events arrive as turns. SILTA_SESSION picks
# the session (default hub); the persona comes from assistant/persona.md. Output goes to
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
# One persona for every session, in the system prompt; no CLAUDE.md in the workspace.
# Attachments land in the workspace's inbox, written by the plugin.
export SILTA_INBOX="${SILTA_INBOX:-$repo/dev/workspace/inbox}"
args+=(--append-system-prompt-file "$repo/assistant/persona.md")
args+=(--channels plugin:silta-claude@silta-local)

# Set Opus 5 high for now.
args+=(--model opus --effort high)

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
[ $# -ge 1 ] && id=$1   # a resumed session prints no init line
for _ in $(seq 1 60); do
  [ -n "$id" ] && break
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
echo "claude pid $pid; log $state/hub.log; press Ctrl-C to stop"
# Stop by closing stdin (EOF ends a -p session cleanly); SIGTERM makes Claude Code
# linger for minutes with a FIFO on stdin, and resuming the same session id while the
# old process is still alive leaves the channel unregistered in the new one.
stop() {
  exec 3>&-
  for _ in $(seq 1 150); do kill -0 "$pid" 2>/dev/null || return 0; sleep 0.1; done
  echo "claude did not exit on stdin EOF within 15 s, killing it" >&2
  kill -9 "$pid" 2>/dev/null || true
}
trap stop INT TERM
wait "$pid"
echo "session exited"
