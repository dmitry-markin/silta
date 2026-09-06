#!/usr/bin/env python3
"""T1: drive silta-claude over stdio as Claude Code would, while playing the daemon on a Unix socket."""
import base64, json, os, shutil, socket, subprocess, sys, time
REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SOCK = f"{REPO}/dev/state/t1.sock"
BIN = f"{REPO}/target/release/silta-claude"
INBOX = f"{REPO}/dev/state/t1-inbox"
shutil.rmtree(INBOX, ignore_errors=True)
LOG = sys.argv[1] if len(sys.argv) > 1 else "/dev/null"
if os.path.exists(SOCK): os.unlink(SOCK)
srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM); srv.bind(SOCK); srv.listen(1)
env = dict(os.environ, SILTA_SESSION="hub", SILTA_SOCKET=SOCK, SILTA_INBOX=INBOX, RUST_LOG="info")
p = subprocess.Popen([BIN], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=open(LOG, "w"), env=env)
def msend(o): p.stdin.write((json.dumps(o, ensure_ascii=False) + "\n").encode()); p.stdin.flush()
def mread(): return json.loads(p.stdout.readline())
msend({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "t1", "version": "0"}}})
init = mread()["result"]
print("initialize:", init["protocolVersion"], "experimental", init["capabilities"].get("experimental"), "instructions", len(init.get("instructions", "")), "chars")
# The plugin connects to the daemon only after `initialized`: a socket
# connection before this point would be a failure.
srv.settimeout(2)
try:
    srv.accept(); print("FAIL the plugin connected before the MCP handshake")
except socket.timeout:
    print("PASS no daemon connection before initialized")
srv.settimeout(None)
msend({"jsonrpc": "2.0", "method": "notifications/initialized"})
conn, _ = srv.accept(); f = conn.makefile("rw", encoding="utf-8")
hello = json.loads(f.readline()); print("daemon<-", json.dumps(hello))
print("  PASS hello says protocol 3" if hello["hello"]["protocol"] == 3 else f"  FAIL protocol {hello['hello']['protocol']}")
def dsend(o): f.write(json.dumps(o, ensure_ascii=False) + "\n"); f.flush()
def dread(): return json.loads(f.readline())
def expect_ack(event_id):
    """After each notification the plugin acknowledges the event to the daemon."""
    a = dread(); print("  PASS acked" if a == {"ack": {"event_id": event_id}} else f"  FAIL expected the ack for {event_id}, got {a}")
dsend({"welcome": {"protocol": 3, "session": "hub", "user_id": "@silta:localhost", "people": [{"name": "Bob", "role": "owner"}, {"name": "Alice", "role": "family"}], "inbox_max_age_days": 30}})
msend({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
tools = mread()["result"]["tools"]
for t in tools:
    s = t["inputSchema"]; print("tool", t["name"], "required", s.get("required"), "props", sorted(s["properties"].keys()))
    if t["name"] in ("reply", "react", "edit_message", "send_file"): print("  PASS more is required" if "more" in (s.get("required") or []) else "  FAIL more is optional")
base = {"kind": "message", "person": "Alice", "role": "family", "sender": "@alice:localhost", "room_id": "!r:localhost", "ts": "2026-09-06T10:00:00Z", "transcribed": False, "attachments": []}
# 1: a message with two attachments and no text; the files arrive as chunked transfers first
photo = os.urandom(1024 * 1024 + 777); notes = b"# notes\n"
def dfile(transfer, name, mime, data):
    dsend({"file": {"transfer": transfer, "name": name, "mime": mime, "size": len(data)}})
    for i in range(0, len(data), 1024 * 1024): dsend({"chunk": {"transfer": transfer, "data": base64.b64encode(data[i:i + 1024 * 1024]).decode()}})
    dsend({"file_end": {"transfer": transfer}})
dfile("a1-1", "../photo.jpg", "image/jpeg", photo); dfile("a1-2", "notes.md", "text/markdown", notes)
dsend({"event": dict(base, event_id="$a1", text="", attachments=[{"transfer": "a1-1", "name": "../photo.jpg", "mime": "image/jpeg", "size": len(photo)}, {"transfer": "a1-2", "name": "notes.md", "mime": "text/markdown", "size": len(notes)}])})
n = mread(); meta = n["params"]["meta"]; print("notification", n["method"], "content", repr(n["params"]["content"]), "meta", json.dumps(meta, ensure_ascii=False))
p1 = meta.get("attachment_1_path", ""); p2 = meta.get("attachment_2_path", "")
print("  PASS attachments written to the inbox under safe names" if p1 == f"{INBOX}/a1-1-photo.jpg" and open(p1, "rb").read() == photo and open(p2, "rb").read() == notes else f"  FAIL inbox paths {p1} {p2}")
print("  PASS inbox file is 0600" if oct(os.stat(p1).st_mode & 0o777) == "0o600" else "  FAIL inbox file mode")
print("  PASS no Matrix id on the tag" if "sender" not in meta and meta["person"] == "Alice" else f"  FAIL sender on the tag: {meta}")
expect_ack("$a1")
# 1b: an attachment whose transfer never came (swept from the daemon's spool)
dsend({"event": dict(base, event_id="$a1b", text="see this", attachments=[{"transfer": "gone-1", "name": "old.pdf", "mime": "application/pdf", "size": 5}])})
n = mread(); print("notification", "content", repr(n["params"]["content"]))
print("  PASS missing transfer named in the text, no path" if "was not received" in n["params"]["content"] and "attachment_1_path" not in n["params"]["meta"] else "  FAIL missing transfer")
expect_ack("$a1b")
# 2: an unknown event kind from a newer daemon must be skipped, not kill the connection
dsend({"event": dict(base, event_id="$a2", kind="poll", text="x")})
# 3: a thread message with an explicit reply, then a reaction
dsend({"event": dict(base, event_id="$a3", thread="$root", in_reply_to="$q", text="in the thread")})
n = mread(); print("notification", "content", repr(n["params"]["content"]), "meta", json.dumps(n["params"]["meta"]))
expect_ack("$a3")
dsend({"event": dict(base, event_id="$a4", kind="reaction", person="Bob", role="owner", sender="@bob:localhost", reacts_to="$bot", text="👍")})
n = mread(); print("notification", "content", repr(n["params"]["content"]), "meta", json.dumps(n["params"]["meta"], ensure_ascii=False))
expect_ack("$a4")
rid = 10
def call(name, args, result):
    global rid; rid += 1
    msend({"jsonrpc": "2.0", "id": rid, "method": "tools/call", "params": {"name": name, "arguments": args}})
    cmd = dread(); print("daemon<-", json.dumps(cmd, ensure_ascii=False))
    dsend({"result": dict(id=cmd["cmd"]["id"], **result)})
    r = mread()["result"]; text = r["content"][0]["text"]; print("tool", name, "->", "isError" if r.get("isError") else "ok", "|", text.replace("\n", " / ")[:300])
    if name == "fetch_message" and not r.get("isError"): print("  PASS whole text" if "x" * 500 in text and "more characters" not in text else "  FAIL text shortened")
    if name == "search_messages" and not r.get("isError"): print("  PASS windowed hit and scan line" if "needle" in text and "[900 characters]" in text and "scanned 57 events back to 2026-09-05" in text and "more: tok2" in text else "  FAIL search rendering")
call("reply", {"room_id": "!r:localhost", "text": "**hi**", "thread": "$root", "more": True}, {"ok": True, "event_id": "$new1"})
call("react", {"room_id": "!r:localhost", "event_id": "$a3", "emoji": "👀", "more": False}, {"ok": True, "event_id": "$new2"})
call("edit_message", {"room_id": "!r:localhost", "event_id": "$new1", "text": "done", "more": False}, {"ok": True, "event_id": "$new3"})
# send_file streams the file as chunks, then the command naming the transfer
report = f"{INBOX}/report.md"; open(report, "wb").write(b"# report\n" * 200000)  # 1.8 MB, two chunks
rid += 1; msend({"jsonrpc": "2.0", "id": rid, "method": "tools/call", "params": {"name": "send_file", "arguments": {"room_id": "!r:localhost", "path": report, "caption": "the report", "reply_to": "$a3", "more": False}}})
hdr = dread(); chunks = []; nxt = dread()
while "chunk" in nxt: chunks.append(nxt["chunk"]); nxt = dread()
end = nxt; cmd = dread(); print("daemon<-", json.dumps(hdr), len(chunks), "chunks", json.dumps(end), json.dumps(cmd, ensure_ascii=False))
data = b"".join(base64.b64decode(c["data"]) for c in chunks)
print("  PASS file streamed whole before the command" if hdr["file"]["size"] == len(data) == 1800000 and hdr["file"]["mime"] == "text/markdown" and len(chunks) == 2 and cmd["cmd"]["send_file"]["transfer"] == hdr["file"]["transfer"] == end["file_end"]["transfer"] else "  FAIL send_file stream")
dsend({"result": {"id": cmd["cmd"]["id"], "ok": True, "event_id": "$new4"}}); r = mread()["result"]; print("tool send_file ->", r["content"][0]["text"])
call("typing", {"room_id": "!r:localhost"}, {"ok": True})
msend({"jsonrpc": "2.0", "id": 99, "method": "tools/call", "params": {"name": "reply", "arguments": {"room_id": "!r:localhost", "text": "no more flag"}}})
r = mread(); print("reply without more ->", "rejected" if r.get("error") or r.get("result", {}).get("isError") else "ACCEPTED (FAIL)")
call("fetch_messages", {"room_id": "!r:localhost", "limit": 5}, {"ok": True, "messages": [{"event_id": "$e", "sender": "@alice:localhost", "person": "Alice", "role": "family", "own": False, "ts": "t", "text": "hi", "attachments": []}, {"event_id": "$b", "sender": "@silta:localhost", "own": True, "ts": "t", "text": "hello", "attachments": [{"name": "a.pdf", "mime": "application/pdf", "size": 10}]}], "more": "tok1"})
call("fetch_messages", {"room_id": "!r:localhost", "from": "tok1"}, {"ok": True, "messages": []})
call("fetch_message", {"room_id": "!r:localhost", "event_id": "$long"}, {"ok": True, "messages": [{"event_id": "$long", "sender": "@silta:localhost", "own": True, "ts": "t", "text": "x" * 500, "attachments": []}]})
call("fetch_message", {"room_id": "!r:localhost", "event_id": "$gone"}, {"ok": False, "error": "not_found", "message": "$gone is not a message"})
call("search_messages", {"room_id": "!r:localhost", "pattern": "needle", "limit": 5}, {"ok": True, "messages": [{"event_id": "$s", "sender": "@alice:localhost", "person": "Alice", "role": "family", "own": False, "ts": "t", "text": "a" * 1000 + "needle" + "b" * 1000, "match_start": 1000, "attachments": []}], "scanned": 57, "until": "2026-09-05T00:00:00Z", "more": "tok2"})
call("search_messages", {"room_id": "!r:localhost", "pattern": "("}, {"ok": False, "error": "bad_request", "message": "invalid regular expression: ..."})
call("edit_message", {"room_id": "!r:localhost", "event_id": "$a3", "text": "x", "more": False}, {"ok": False, "error": "not_found", "message": "$a3 is not one of the bot's own messages"})
for bad in ["relative.txt", "/nonexistent/x.txt", INBOX]:
    rid += 1; msend({"jsonrpc": "2.0", "id": rid, "method": "tools/call", "params": {"name": "send_file", "arguments": {"room_id": "!r:localhost", "path": bad, "more": False}}})
    r = mread()["result"]; print("tool send_file", bad, "->", "isError" if r.get("isError") else "ok", "|", r["content"][0]["text"][:80])
    print("  PASS refused in the plugin with file_error" if r.get("isError") and r["content"][0]["text"].startswith("file_error") else "  FAIL bad path")
p.stdin.close(); t0 = time.time()
try: code = p.wait(timeout=5); print(f"plugin exited {code} {int((time.time()-t0)*1000)} ms after stdin EOF")
except subprocess.TimeoutExpired: p.kill(); print("plugin did not exit on stdin EOF")
os.unlink(SOCK)
