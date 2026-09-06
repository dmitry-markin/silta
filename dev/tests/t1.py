#!/usr/bin/env python3
"""T1: drive silta-claude over stdio as Claude Code would, while playing the daemon on a Unix socket."""
import json, os, socket, subprocess, sys, time
REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SOCK = f"{REPO}/dev/state/t1.sock"
BIN = f"{REPO}/target/release/silta-claude"
LOG = sys.argv[1] if len(sys.argv) > 1 else "/dev/null"
if os.path.exists(SOCK): os.unlink(SOCK)
srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM); srv.bind(SOCK); srv.listen(1)
env = dict(os.environ, SILTA_SESSION="hub", SILTA_SOCKET=SOCK, RUST_LOG="info")
p = subprocess.Popen([BIN], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=open(LOG, "w"), env=env)
conn, _ = srv.accept(); f = conn.makefile("rw", encoding="utf-8")
print("daemon<-", f.readline().strip())
def dsend(o): f.write(json.dumps(o, ensure_ascii=False) + "\n"); f.flush()
def dread(): return json.loads(f.readline())
def msend(o): p.stdin.write((json.dumps(o, ensure_ascii=False) + "\n").encode()); p.stdin.flush()
def mread(): return json.loads(p.stdout.readline())
dsend({"welcome": {"protocol": 1, "session": "hub", "user_id": "@silta:localhost", "people": [{"name": "Bob", "role": "owner"}, {"name": "Alice", "role": "family"}]}})
msend({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "t1", "version": "0"}}})
init = mread()["result"]
print("initialize:", init["protocolVersion"], "experimental", init["capabilities"].get("experimental"), "instructions", len(init.get("instructions", "")), "chars")
msend({"jsonrpc": "2.0", "method": "notifications/initialized"})
msend({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
tools = mread()["result"]["tools"]
for t in tools:
    s = t["inputSchema"]; print("tool", t["name"], "required", s.get("required"), "props", sorted(s["properties"].keys()))
base = {"kind": "message", "person": "Alice", "role": "family", "sender": "@alice:localhost", "room_id": "!r:localhost", "ts": "2026-09-06T10:00:00Z", "transcribed": False, "attachments": []}
# 1: a message with two attachments and no text
dsend({"event": dict(base, event_id="$a1", text="", attachments=[{"name": "photo.jpg", "mime": "image/jpeg", "size": 1234, "path": "/inbox/a1-photo.jpg"}, {"name": "notes.md", "mime": "text/markdown", "size": 99, "path": "/inbox/a1-notes.md"}])})
n = mread(); print("notification", n["method"], "content", repr(n["params"]["content"]), "meta", json.dumps(n["params"]["meta"], ensure_ascii=False))
# 2: an unknown event kind from a newer daemon must be skipped, not kill the connection
dsend({"event": dict(base, event_id="$a2", kind="poll", text="x")})
# 3: a thread message with an explicit reply, then a reaction
dsend({"event": dict(base, event_id="$a3", thread="$root", in_reply_to="$q", text="in the thread")})
n = mread(); print("notification", "content", repr(n["params"]["content"]), "meta", json.dumps(n["params"]["meta"]))
dsend({"event": dict(base, event_id="$a4", kind="reaction", person="Bob", role="owner", sender="@bob:localhost", reacts_to="$bot", text="👍")})
n = mread(); print("notification", "content", repr(n["params"]["content"]), "meta", json.dumps(n["params"]["meta"], ensure_ascii=False))
rid = 10
def call(name, args, result):
    global rid; rid += 1
    msend({"jsonrpc": "2.0", "id": rid, "method": "tools/call", "params": {"name": name, "arguments": args}})
    cmd = dread(); print("daemon<-", json.dumps(cmd, ensure_ascii=False))
    dsend({"result": dict(id=cmd["cmd"]["id"], **result)})
    r = mread()["result"]; text = r["content"][0]["text"]; print("tool", name, "->", "isError" if r.get("isError") else "ok", "|", text.replace("\n", " / ")[:300])
    if name == "fetch_message" and not r.get("isError"): print("  PASS whole text" if "x" * 500 in text and "more characters" not in text else "  FAIL text shortened")
call("reply", {"room_id": "!r:localhost", "text": "**hi**", "thread": "$root"}, {"ok": True, "event_id": "$new1"})
call("react", {"room_id": "!r:localhost", "event_id": "$a3", "emoji": "👀"}, {"ok": True, "event_id": "$new2"})
call("edit_message", {"room_id": "!r:localhost", "event_id": "$new1", "text": "done"}, {"ok": True, "event_id": "$new3"})
call("send_file", {"room_id": "!r:localhost", "path": "/abs/report.md", "caption": "the report", "reply_to": "$a3"}, {"ok": True, "event_id": "$new4"})
call("fetch_messages", {"room_id": "!r:localhost", "limit": 5}, {"ok": True, "messages": [{"event_id": "$e", "sender": "@alice:localhost", "person": "Alice", "role": "family", "own": False, "ts": "t", "text": "hi", "attachments": []}, {"event_id": "$b", "sender": "@silta:localhost", "own": True, "ts": "t", "text": "hello", "attachments": [{"name": "a.pdf", "mime": "application/pdf", "size": 10}]}], "more": "tok1"})
call("fetch_messages", {"room_id": "!r:localhost", "from": "tok1"}, {"ok": True, "messages": []})
call("fetch_message", {"room_id": "!r:localhost", "event_id": "$long"}, {"ok": True, "messages": [{"event_id": "$long", "sender": "@silta:localhost", "own": True, "ts": "t", "text": "x" * 500, "attachments": []}]})
call("fetch_message", {"room_id": "!r:localhost", "event_id": "$gone"}, {"ok": False, "error": "not_found", "message": "$gone is not a message"})
call("edit_message", {"room_id": "!r:localhost", "event_id": "$a3", "text": "x"}, {"ok": False, "error": "not_found", "message": "$a3 is not one of the bot's own messages"})
call("send_file", {"room_id": "!r:localhost", "path": "relative.txt"}, {"ok": False, "error": "file_error", "message": "relative.txt is not an absolute path"})
p.stdin.close(); t0 = time.time()
try: code = p.wait(timeout=5); print(f"plugin exited {code} {int((time.time()-t0)*1000)} ms after stdin EOF")
except subprocess.TimeoutExpired: p.kill(); print("plugin did not exit on stdin EOF")
os.unlink(SOCK)
