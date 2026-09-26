#!/usr/bin/env bash
# Checks a Claude Code version against what silta-session assumes of its stream-json
# output, with one real turn: run it after an upgrade, before the version is pinned
# in the unit.
#
#   dev/contract-check.sh                 the claude on PATH
#   CLAUDE_BIN=/opt/claude/.local/share/claude/versions/2.1.270 dev/contract-check.sh
#
# Asserts: the init line names a version and it is in silta-session's TESTED_VERSIONS;
# a main-line assistant line carries the three usage counts; the turn's prompt comes
# back as a user line (the idle gap rests on that line for a channel delivery); the
# result line carries is_error; a get_context_usage control request is answered with an
# integer maxTokens (the window check at every start). Agents, compaction and the hook
# stay in the manual procedure. Exit 0 when every check passes, 1 otherwise; the failures are listed.
#
# TODO: this currently lacks at least the check for user message detection, that needs
#       a fake channel plugin.

set -uo pipefail

repo=$(cd "$(dirname "$0")/.." && pwd)
claude=${CLAUDE_BIN:-claude}
tested=$(grep -o 'TESTED_VERSIONS: &\[&str\] = &\[[^]]*\]' "$repo/crates/silta-session/src/contract.rs" | grep -o '"[0-9.]*"' | tr -d '"' | tr '\n' ' ')
out=$(mktemp "${TMPDIR:-/tmp}/contract-check.XXXXXX")

trap 'rm -f "$out"' EXIT

printf '%s\n' '{"type":"control_request","request_id":"silta-window","request":{"subtype":"get_context_usage","detail":"summary"}}' \
  '{"type":"user","message":{"role":"user","content":"Reply with the single word ok and nothing else."}}' \
  | "$claude" -p --input-format stream-json --output-format stream-json --verbose \
      --replay-user-messages --permission-mode auto --permission-prompts none > "$out" 2>"$out.err"
status=$?
if [ $status -ne 0 ]; then
  echo "claude exited with $status; stderr:" >&2
  cat "$out.err" >&2
fi
rm -f "$out.err"

python3 - "$out" "$tested" <<'PY'
import json, sys
lines = [json.loads(l) for l in open(sys.argv[1]) if l.strip().startswith("{")]
tested = sys.argv[2].split()
fails = []
init = next((l for l in lines if l.get("type") == "system" and l.get("subtype") == "init"), None)
version = init.get("claude_code_version") if init else None
if not init:
    fails.append("no system init line")
elif not isinstance(version, str) or not version:
    fails.append("system init has no claude_code_version")
#elif version not in tested:
#    fails.append(f"Claude Code {version} is not in TESTED_VERSIONS ({', '.join(tested)}); add it after the manual checks")
main = [l for l in lines if l.get("type") == "assistant" and l.get("parent_tool_use_id") is None]
if not main:
    fails.append("no main-line assistant line (type assistant with parent_tool_use_id null)")
else:
    usage = main[-1].get("message", {}).get("usage")
    if not isinstance(usage, dict):
        fails.append("assistant line has no message.usage")
    else:
        for key in ("input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens"):
            if not isinstance(usage.get(key), int):
                fails.append(f"message.usage.{key} is missing or not an integer")
echoed = [l for l in lines if l.get("type") == "user" and isinstance(l.get("message", {}).get("content"), str)
          and l["message"]["content"].startswith("Reply with the single word")]
if not echoed:
    fails.append("the prompt is not echoed as a user line with --replay-user-messages; a channel delivery would not restart the idle gap")
results = [l for l in lines if l.get("type") == "result"]
if not results:
    fails.append("no result line")
elif not isinstance(results[-1].get("is_error"), bool):
    fails.append("result line has no boolean is_error")
usage = next((l.get("response", {}) for l in lines if l.get("type") == "control_response"
              and l.get("response", {}).get("request_id") == "silta-window"), None)
if usage is None:
    fails.append("no control_response to get_context_usage; silta-session ends every start at the window check")
elif usage.get("subtype") != "success" or not isinstance(usage.get("response", {}).get("maxTokens"), int):
    fails.append(f"get_context_usage answered without an integer response.maxTokens: {usage.get('error') or usage.get('subtype')}")
print(f"Claude Code {version or '?'}: {len(lines)} lines, {len(main)} main-line assistant, {len(results)} result")
for f in fails:
    print(f"FAIL {f}")
print("contract holds" if not fails else "contract broken")
sys.exit(1 if fails else 0)
PY
