#!/usr/bin/env bash
# Checks a Claude Code version against what silta-session assumes of its stream-json
# output (docs/claude-code-contract.md, section 1), with one real turn: run it after an
# upgrade, before the version is pinned in the unit. One API call, a few cents.
#
#   dev/contract-check.sh                 the claude on PATH
#   CLAUDE_BIN=/opt/claude/.local/share/claude/versions/2.1.270 dev/contract-check.sh
#
# Asserts: the init line names a version and it is in silta-session's TESTED_VERSIONS;
# a main-line assistant line carries the three usage counts; the result line carries
# is_error. Agents, compaction and the hook stay in the manual procedure of the doc's
# last section. Exit 0 when every check passes, 1 otherwise; the failures are listed.
set -uo pipefail
repo=$(cd "$(dirname "$0")/.." && pwd)
claude=${CLAUDE_BIN:-claude}
tested=$(grep -o 'TESTED_VERSIONS: &\[&str\] = &\[[^]]*\]' "$repo/crates/silta-session/src/contract.rs" | grep -o '"[0-9.]*"' | tr -d '"' | tr '\n' ' ')
out=$(mktemp "${TMPDIR:-/tmp}/contract-check.XXXXXX")
trap 'rm -f "$out"' EXIT
printf '%s\n' '{"type":"user","message":{"role":"user","content":"Reply with the single word ok and nothing else."}}' \
  | "$claude" -p --input-format stream-json --output-format stream-json --verbose \
      --permission-mode auto --permission-prompts none > "$out" 2>"$out.err"
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
elif version not in tested:
    fails.append(f"Claude Code {version} is not in TESTED_VERSIONS ({', '.join(tested)}); add it after the manual checks")
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
results = [l for l in lines if l.get("type") == "result"]
if not results:
    fails.append("no result line")
elif not isinstance(results[-1].get("is_error"), bool):
    fails.append("result line has no boolean is_error")
print(f"Claude Code {version or '?'}: {len(lines)} lines, {len(main)} main-line assistant, {len(results)} result")
for f in fails:
    print(f"FAIL {f}")
print("contract holds" if not fails else "contract broken: re-check docs/claude-code-contract.md")
sys.exit(1 if fails else 0)
PY
