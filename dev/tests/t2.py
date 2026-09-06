#!/usr/bin/env python3
"""T2: the daemon against conduit. This script is the `test` session on the socket; Alice is the test client.

Needs: the daemon running with a `test` session that receives from Alice with send = "own",
user = "bob" and attachment_max_mb = 1 in dev/siltad.toml, the test client built
(cargo build -p siltad --examples), and dev/state/accounts.env. Test files are generated under
dev/state/testfiles. Attachments arrive as chunked transfers and are kept in memory here."""
import base64, json, os, queue, socket, subprocess, sys, threading, time
REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SOCK = f"{REPO}/dev/state/siltad.sock"
TC = f"{REPO}/target/debug/examples/testclient"
FILES = f"{REPO}/dev/state/testfiles"

def fixtures():
    """The test files: a PNG with a red circle and a blue square, a Markdown note, a fake 20 KB ogg, a 2 MB blob."""
    import zlib, struct, math
    os.makedirs(FILES, exist_ok=True)
    def png(w, h, pix):
        raw = b"".join(b"\x00" + b"".join(bytes(pix(x, y)) for x in range(w)) for y in range(h))
        def chunk(t, d): return struct.pack(">I", len(d)) + t + d + struct.pack(">I", zlib.crc32(t + d) & 0xffffffff)
        return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", w, h, 8, 2, 0, 0, 0)) + chunk(b"IDAT", zlib.compress(raw, 9)) + chunk(b"IEND", b"")
    def pix(x, y):
        if math.hypot(x - 45, y - 60) < 30: return (220, 30, 30)
        if 100 <= x < 150 and 35 <= y < 85: return (30, 60, 220)
        return (255, 255, 255)
    files = {"shapes.png": lambda: png(180, 120, pix), "notes.md": lambda: b"# Notes for the test\n\n- one\n- two\n\nSent by the assistant as a file.\n",
             "voice.ogg": lambda: os.urandom(20 * 1024), "big.bin": lambda: os.urandom(2 * 1024 * 1024)}
    for name, make in files.items():
        path = f"{FILES}/{name}"
        if not os.path.exists(path): open(path, "wb").write(make())
fixtures()
env = dict(os.environ)
for line in open(f"{REPO}/dev/state/accounts.env"):
    line = line.strip()
    if "=" in line and not line.startswith("#"):
        k, v = line.split("=", 1); env[k] = v.strip().strip('"').strip("'")
env.pop("TESTCLIENT_STATE", None)
def alice(*args, state="testclient-sender", device="sender"):
    e = dict(env, TESTCLIENT_STATE=f"{REPO}/dev/state/{state}", TESTCLIENT_DEVICE=device)
    r = subprocess.run([TC, *args], capture_output=True, text=True, env=e, timeout=60)
    if r.returncode != 0: print("  testclient failed:", r.stderr.strip()[-300:])
    return r.stdout.strip()
def ok(cond, what): print(("  PASS " if cond else "  FAIL ") + what)

dm = alice("dm", "@silta:localhost", state="testclient", device="testclient")
marks = json.load(open(f"{REPO}/dev/state/silta/delivered.json"))
family = next((r for r in marks if r != dm), None)
print("dm", dm, "family", family)
watch_out = open(f"{REPO}/dev/state/t2-watch.out", "w")
watch = subprocess.Popen([TC, "watch"], stdout=watch_out, stderr=subprocess.STDOUT, env=dict(env, TESTCLIENT_STATE=f"{REPO}/dev/state/testclient"))
time.sleep(4)

s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM); s.connect(SOCK); f = s.makefile("rw", encoding="utf-8")
events, results = queue.Queue(), queue.Queue()
received, partial = {}, {}
lock = threading.Lock()
def send(o):
    with lock: f.write(json.dumps(o, ensure_ascii=False) + "\n"); f.flush()
def reader():
    for line in f:
        o = json.loads(line)
        if "file" in o: partial[o["file"]["transfer"]] = [o["file"], bytearray()]
        elif "chunk" in o: partial[o["chunk"]["transfer"]][1] += base64.b64decode(o["chunk"]["data"])
        elif "file_end" in o: received[o["file_end"]["transfer"]] = partial.pop(o["file_end"]["transfer"])
        elif "event" in o: events.put(o); send({"ack": {"event_id": o["event"]["event_id"]}})  # as the plugin does once the notification is out
        else: results.put(o)
threading.Thread(target=reader, daemon=True).start()
send({"hello": {"protocol": 3, "session": "test", "client": "t2/0"}})
print("welcome:", json.dumps(results.get(timeout=5))[:120])
n = 0
def cmd(kind, **args):
    global n; n += 1
    send({"cmd": {"id": n, kind: args}})
    r = results.get(timeout=120)["result"]; assert r["id"] == n
    short = {k: v for k, v in r.items() if k not in ("id", "messages")}
    if "messages" in r: short["messages"] = len(r["messages"])
    print(f"  {kind} -> {json.dumps(short, ensure_ascii=False)[:220]}")
    return r
tn = 0
def send_file(path, declared=None, **args):
    """Stream a file as a transfer, then the send_file command for it."""
    global tn; tn += 1; t = f"t{tn}"; data = open(path, "rb").read()
    send({"file": {"transfer": t, "name": os.path.basename(path), "mime": {"md": "text/markdown", "png": "image/png"}.get(path.rsplit(".", 1)[-1], "application/octet-stream"), "size": declared if declared is not None else len(data)}})
    for i in range(0, len(data), 1024 * 1024): send({"chunk": {"transfer": t, "data": base64.b64encode(data[i:i + 1024 * 1024]).decode()}})
    send({"file_end": {"transfer": t}})
    return cmd("send_file", transfer=t, **args)
def event(timeout=25):
    try: e = events.get(timeout=timeout)["event"]
    except queue.Empty: return None
    print("  event:", json.dumps({k: v for k, v in e.items() if k not in ("sender", "role", "room_id", "ts")}, ensure_ascii=False)[:300])
    return e

print("1. photo with a caption")
eid_photo = alice("sendfile", dm, f"{FILES}/shapes.png", "a picture")
e = event(); a = e["attachments"][0] if e and e["attachments"] else None
ok(e and e["text"] == "a picture" and a and a["mime"] == "image/png" and a["name"] == "shapes.png", "caption, mime and name")
got = received.get(a["transfer"]) if a else None
ok(got and bytes(got[1]) == open(f"{FILES}/shapes.png", "rb").read() and got[0]["size"] == a["size"], "file arrived as a transfer before the event, byte-identical to the original (decrypted)")

print("2. audio without a caption")
alice("sendfile", dm, f"{FILES}/voice.ogg")
e = event(); a = e["attachments"][0] if e and e["attachments"] else None
ok(e and e["text"] == "" and a and a["mime"] == "audio/ogg" and a["size"] == 20480, "audio attachment with empty text")

print("3. a file over the cap")
alice("sendfile", dm, f"{FILES}/big.bin")
e = event()
ok(e and not e["attachments"] and "not downloaded" in e["text"] and "1.0 MB limit" in e["text"], "over-cap file described in the text, not downloaded")

print("4. a thread message")
alice("thread", dm, eid_photo, "in a thread")
e = event()
ok(e and e.get("thread") == eid_photo and e.get("in_reply_to") is None and e["text"] == "in a thread", "thread root set, no fallback, no in_reply_to")

print("5. reply in the thread")
r = cmd("reply", room_id=dm, text="**answer in thread**", thread=eid_photo); bot_eid = r.get("event_id")
ok(r["ok"], "threaded reply accepted")

print("6. reactions: Alice on her own message (must not arrive), then on the bot's")
alice("react", dm, eid_photo, "🙂")
e = event(timeout=6)
ok(e is None, "reaction on Alice's own message is not delivered")
reaction_eid = alice("react", dm, bot_eid, "👍")
e = event()
ok(e and e["kind"] == "reaction" and e.get("reacts_to") == bot_eid and e["text"] == "👍", "reaction on the bot's message delivered")

print("7. the bot reacts and edits")
r = cmd("react", room_id=dm, event_id=eid_photo, emoji="👀"); ok(r["ok"], "react accepted")
r = cmd("edit", room_id=dm, event_id=bot_eid, text="**answer in thread**, edited"); ok(r["ok"], "edit of own message accepted")
r = cmd("edit", room_id=dm, event_id=eid_photo, text="x"); ok(r.get("error") == "not_found", "editing Alice's message refused with not_found")
r = cmd("edit", room_id=dm, event_id=bot_eid, text="x" * 9000); ok(r.get("error") == "bad_request", "over-long edit refused with bad_request")
r = cmd("edit", room_id=dm, event_id="$nonexistent:localhost", text="x"); ok(r.get("error") == "not_found", "unknown event refused with not_found")

print("8. files from the bot")
r = send_file(f"{FILES}/notes.md", room_id=dm, caption="the *notes*", reply_to=eid_photo); ok(r["ok"], "text file with caption and quote sent through a transfer")
r = send_file(f"{FILES}/shapes.png", room_id=dm); ok(r["ok"], "image sent through a transfer")
r = cmd("send_file", room_id=dm, transfer="never-sent"); ok(r.get("error") == "file_error" and "no such transfer" in r.get("message", ""), "a command without its transfer refused")
r = send_file(f"{FILES}/notes.md", declared=10, room_id=dm); ok(r.get("error") == "file_error" and "declared size" in r.get("message", ""), "a transfer whose size does not match its header refused")
r = send_file(f"{FILES}/big.bin", room_id=dm); ok(r.get("error") == "file_error" and "limit" in r.get("message", ""), "a 2 MB transfer refused by the 1 MB cap")
spool_out = os.listdir(f"{REPO}/dev/state/silta/outbox"); ok(not spool_out, f"outbox spool empty after the sends ({spool_out})")

print("9. history")
r = cmd("fetch_messages", room_id=dm, limit=5); m = r.get("messages", [])
ok(r["ok"] and len(m) >= 5 and all(m[i]["ts"] >= m[i + 1]["ts"] for i in range(len(m) - 1)), "at least five messages, newest first")
ok(any(x["own"] for x in m) and any(not x["own"] for x in m), "both the bot's and Alice's messages listed")
ok(any(x["attachments"] for x in m), "attachments listed by name")
for x in m[:5]: print("   ", x["ts"][11:19], "own" if x["own"] else x.get("person"), repr(x["text"][:40]), [a["name"] for a in x["attachments"]], "thread" if x.get("thread") else "", "reply" if x.get("in_reply_to") else "")
more = r.get("more"); ok(bool(more), "a more token is present")
r2 = cmd("fetch_messages", room_id=dm, limit=5, **{"from": more}); m2 = r2.get("messages", [])
ok(r2["ok"] and m2 and not {x["event_id"] for x in m} & {x["event_id"] for x in m2}, "second page has different, older messages")
r = cmd("fetch_messages", room_id=family); ok(r.get("error") == "room_not_allowed", "history of the family room refused for the test session")
r = cmd("reply", room_id=family, text="x"); ok(r.get("error") == "room_not_allowed", "writing to the family room refused for the test session")

print("10. a quote of a thread message follows it into the thread")
thread_msg = [x for x in m if x.get("thread") and not x["own"]]
if thread_msg:
    r = cmd("reply", room_id=dm, text="quoted from the thread", reply_to=thread_msg[0]["event_id"]); ok(r["ok"], "quote accepted")
print("11. one message whole")
r = cmd("reply", room_id=dm, text="z" * 3000); long_eid = r.get("event_id")
r = cmd("fetch_message", room_id=dm, event_id=long_eid); m1 = (r.get("messages") or [{}])[0]
ok(r["ok"] and m1.get("own") and m1.get("text") == "z" * 3000, "the bot's long message comes back whole")
r = cmd("fetch_message", room_id=dm, event_id=eid_photo); m1 = (r.get("messages") or [{}])[0]
ok(r["ok"] and m1.get("person") == "Alice" and m1.get("text") == "a picture" and m1["attachments"][0]["name"] == "shapes.png", "Alice's photo message with its attachment")
r = cmd("fetch_message", room_id=dm, event_id="$nonexistent:localhost"); ok(r.get("error") == "not_found", "unknown event: not_found")
r = cmd("fetch_message", room_id=dm, event_id=reaction_eid); ok(r.get("error") == "not_found", "a reaction is not a message: not_found")
r = cmd("fetch_message", room_id=family, event_id=eid_photo); ok(r.get("error") == "room_not_allowed", "a room the session does not own: room_not_allowed")

print("12. search with a regular expression")
alice("send", dm, "reminder to self: the secret token is Q7X-ALPHA, do not lose it"); event()
r = cmd("search_messages", room_id=dm, pattern=r"q7x-\w+"); m = r.get("messages") or []
ok(r["ok"] and m and "Q7X-ALPHA" in m[0]["text"] and m[0].get("match_start") == 38 and m[0].get("person") == "Alice", "case-insensitive match with its offset")
ok(r.get("scanned", 0) > 0 and r.get("until"), "scan count and oldest timestamp reported")
r = cmd("search_messages", room_id=dm, pattern="^z{3000}$"); m = r.get("messages") or []
ok(r["ok"] and m and m[0].get("own") and m[0]["event_id"] == long_eid, "anchored pattern finds the bot's long message")
r = cmd("search_messages", room_id=dm, pattern=r"shapes\.png"); m = r.get("messages") or []
ok(r["ok"] and m and any(a["name"] == "shapes.png" for x in m for a in x["attachments"]), "attachment names are searched")
r = cmd("search_messages", room_id=dm, pattern="NOPE_[0-9]{9}"); ok(r["ok"] and not r.get("messages") and r.get("more") is None, "no match, scanned to the start of the room")
r = cmd("search_messages", room_id=dm, pattern="("); ok(r.get("error") == "bad_request" and "regular expression" in (r.get("message") or ""), "invalid pattern: bad_request")
r = cmd("search_messages", room_id=family, pattern="x"); ok(r.get("error") == "room_not_allowed", "a room the session does not own: room_not_allowed")

print("13. the typing indicator across a multi-message answer")
def typing_lines(since):
    out = []
    for l in open(f"{REPO}/dev/state/t2-watch.out"):
        if "typing:" in l and dm in l:
            hhmmss = l.split("] ")[1].split(" typing: ")[0]
            if hhmmss >= since: out.append((hhmmss, "on" if "@silta" in l else "off"))
    return out
def now(): return time.strftime("%H:%M:%S", time.gmtime())
alice("send", dm, "give me two parts please"); event()
t_first = now(); r = cmd("reply", room_id=dm, text="part one", more=True); time.sleep(6)
after_first = [x for x in typing_lines(t_first) if x[0] > t_first]
ok(any(s == "on" for _, s in after_first) and not any(s == "off" for _, s in after_first), f"indicator kept on after a send with more=true ({after_first})")
t_last = now(); r = cmd("reply", room_id=dm, text="part two", more=False); time.sleep(5)
after_last = [x for x in typing_lines(t_last) if x[0] >= t_last]  # the off can land in the same second as the send
ok(any(s == "off" for _, s in after_last), f"indicator ends after the last send ({after_last})")
t_typing = now(); r = cmd("typing", room_id=dm); ok(r["ok"], "typing command accepted"); time.sleep(5)
after_typing = [x for x in typing_lines(t_typing) if x[0] > t_typing]
ok(any(s == "on" for _, s in after_typing), f"indicator back after the typing command ({after_typing})")
r = cmd("reply", room_id=dm, text="part three, after a change of mind", more=False)
r = cmd("typing", room_id=family); ok(r.get("error") == "room_not_allowed", "typing in a room the session may not write to: room_not_allowed")
time.sleep(4)
watch.terminate(); watch.wait(timeout=10); watch_out.close()
print("--- watch (Alice's view, last 16 lines) ---")
lines = [l for l in open(f"{REPO}/dev/state/t2-watch.out") if "typing" not in l]
print("".join(lines[-16:]))
spool_in = os.listdir(f"{REPO}/dev/state/silta/inbox"); ok(not spool_in, f"inbox spool empty after the transfers ({spool_in})")
