//! The Unix socket server: one connection per session, hello handshake with a check
//! of the peer's Unix user, then commands and files in, events and files out.

use std::{
    collections::{hash_map::Entry, HashMap},
    fs,
    io,
    os::unix::fs::{FileTypeExt, PermissionsExt},
    path::PathBuf,
    time::Duration,
};

use anyhow::{bail, Context, Result};
use silta::{
    line::{write_line, LineReader},
    protocol::{
        ClientMessage, Cmd, CmdKind, CmdResult, DaemonMessage, ErrorCode, Event, FileHeader, Hello, Incoming,
        ProtocolError, ResultError, Welcome, PROTOCOL_VERSION,
    },
    transfer::{self, Piece, Received, Receiver},
};
use tokio::{
    net::{unix::OwnedWriteHalf, UnixListener, UnixStream},
    sync::mpsc,
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{
    daemon::{now_ms, Daemon, Shared},
    outbound,
};

const HELLO_TIMEOUT: Duration = Duration::from_secs(5);

/// Band-aid (2026-09-12): a restarted session's plugin accepted a message delivered
/// straight after the session started, and the model never answered it. Every event to
/// a fresh connection, queued or live, waits this long; command results do not. Remove
/// once the cause is found and fixed.
pub const EVENT_HOLD: Duration = Duration::from_secs(15);

/// Bind the socket and accept connections until cancelled; removes the socket file
/// on the way out.
pub async fn run(daemon: Shared, path: PathBuf, cancel: CancellationToken) -> Result<()> {
    let path = path.as_path();
    if let Ok(meta) = fs::symlink_metadata(path) {
        if !meta.file_type().is_socket() {
            bail!("{} exists and is not a socket", path.display());
        }
        fs::remove_file(path).with_context(|| format!("cannot remove the stale socket {}", path.display()))?;
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("cannot create {}", parent.display()))?;
    }
    let listener = UnixListener::bind(path).with_context(|| format!("cannot bind {}", path.display()))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o660))?;
    info!("listening on {}", path.display());

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    tokio::spawn(handle(daemon.clone(), stream, cancel.clone()));
                }
                Err(err) => {
                    warn!("accept failed: {err}");
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
    let _ = fs::remove_file(path);
    info!("socket closed");
    Ok(())
}

async fn handle(daemon: Shared, stream: UnixStream, cancel: CancellationToken) {
    // Who is on the other end, from the kernel, before any byte is trusted.
    let peer_uid = match stream.peer_cred() {
        Ok(cred) => Some(cred.uid()),
        Err(err) => {
            warn!("cannot read the peer credentials of a connection: {err}");
            None
        }
    };
    let (read_half, mut writer) = stream.into_split();
    let mut reader = LineReader::new(read_half);

    let hello = match handshake(&mut reader, &mut writer).await {
        Ok(hello) => hello,
        Err(err) => {
            warn!("handshake failed: {err}");
            return;
        }
    };
    let session = hello.session;
    if hello.protocol != PROTOCOL_VERSION {
        refuse(&mut writer, ErrorCode::ProtocolMismatch, format!("this daemon speaks protocol {PROTOCOL_VERSION}, the client {}", hello.protocol)).await;
        return;
    }
    if daemon.routing.session(&session).is_none() {
        refuse(&mut writer, ErrorCode::UnknownSession, format!("no session {session:?} in the daemon configuration")).await;
        return;
    }
    match (daemon.users.get(&session), peer_uid) {
        (Some(&uid), Some(peer)) if uid == peer => {}
        (Some(&uid), Some(peer)) => {
            refuse(&mut writer, ErrorCode::WrongUser, format!("session {session:?} runs as uid {uid}, the connecting process as uid {peer}")).await;
            return;
        }
        _ => {
            refuse(&mut writer, ErrorCode::WrongUser, format!("cannot verify the user behind session {session:?}")).await;
            return;
        }
    }
    let Some((results, outbound, _claim)) = daemon.registry.claim(&session) else {
        refuse(&mut writer, ErrorCode::SessionBusy, format!("session {session:?} is already connected")).await;
        return;
    };
    let welcome = DaemonMessage::Welcome(Welcome {
        protocol: PROTOCOL_VERSION,
        session: session.clone(),
        user_id: daemon.routing.bot_user_id().to_owned(),
        people: daemon.routing.people_for(&session),
        inbox_max_age_days: daemon.inbox_max_age_days,
    });
    if let Err(err) = write_line(&mut writer, &welcome).await {
        warn!(session, "cannot send welcome: {err}");
        return;
    }
    info!(session, client = %hello.client, uid = ?peer_uid, "session connected");
    if let Some(alert) = daemon.session_connected(&session) {
        let daemon = daemon.clone();
        tokio::spawn(async move { crate::alert::send(&daemon, alert).await });
    }

    // Messages that arrived while the session was away, oldest first. They are in flight
    // from here, and the writer hands them over when the hold is past.
    let (queued, evicted) = daemon.registry.take_backlog(&session, now_ms());
    if evicted > 0 {
        warn!(dir = "in", session, evicted, "permanently lost incoming messages queued longer than the replay window");
    }
    let queued = queued.into_iter().map(|(_, event)| event).collect();

    // From here on one task writes the socket and this one reads it, so a file streaming
    // out never stops the reading side. Were both done in turn, a file crossing each way
    // at the same moment would fill both socket buffers with neither side reading, and
    // the connection would hang for good.
    let mut writer_task = tokio::spawn(write_loop(daemon.clone(), writer, session.clone(), outbound, queued));

    // Files the session streams for `send_file`, spooled until the command names them.
    let mut receiver = Receiver::new(daemon.spool.outbox.clone(), format!("{session}-"), daemon.spool.max_bytes);
    let mut received: HashMap<String, Received> = HashMap::new();
    let mut failed: HashMap<String, String> = HashMap::new();

    loop {
        let line = tokio::select! {
            _ = cancel.cancelled() => break,
            _ = &mut writer_task => break,
            line = reader.next_line() => match line {
                Ok(Some(line)) => line,
                Ok(None) => break,
                Err(err) => {
                    warn!(session, "closing: {err}");
                    break;
                }
            },
        };
        let result = match silta::protocol::parse_client_line(&line) {
            Ok(Incoming::Message(ClientMessage::Cmd(cmd))) => Some(match cmd.kind {
                CmdKind::SendFile(file) => match received.remove(&file.transfer) {
                    Some(file_received) => outbound::send_file(&daemon, &session, cmd.id, file, file_received).await,
                    None => {
                        let why = failed.remove(&file.transfer).unwrap_or_else(|| "no such transfer was received before the command".to_owned());
                        CmdResult::err(cmd.id, ResultError::FileError, format!("transfer {:?}: {why}", file.transfer))
                    }
                },
                kind => daemon.execute(&session, Cmd { id: cmd.id, kind }).await,
            }),
            Ok(Incoming::Message(ClientMessage::File(header))) => {
                debug!(dir = "out", session, transfer = %header.transfer, name = %header.name, size = header.size, "the session begins sending a file");
                let transfer = header.transfer.clone();
                if let Err(err) = receiver.begin(header).await {
                    warn!(dir = "out", session, transfer, "refusing a file the session offered: {err}");
                    failed.insert(transfer, err.to_string());
                }
                None
            }
            Ok(Incoming::Message(ClientMessage::Chunk(chunk))) => {
                let transfer = chunk.transfer.clone();
                if let Err(err) = receiver.chunk(chunk).await {
                    // Logged once per transfer, not once per chunk.
                    if let Entry::Vacant(slot) = failed.entry(transfer) {
                        warn!(dir = "out", session, transfer = %slot.key(), "a file the session is sending failed: {err}");
                        slot.insert(err.to_string());
                    }
                }
                None
            }
            Ok(Incoming::Message(ClientMessage::FileEnd(end))) => {
                let transfer = end.transfer.clone();
                match receiver.end(end).await {
                    Ok(done) => {
                        debug!(dir = "out", session, transfer, bytes = done.header.size, "spooled a file from the session, waiting for the send that names it");
                        received.insert(transfer, done);
                    }
                    Err(err) => {
                        if let Entry::Vacant(slot) = failed.entry(transfer) {
                            warn!(dir = "out", session, transfer = %slot.key(), "a file the session sent failed: {err}");
                            slot.insert(err.to_string());
                        }
                    }
                }
                None
            }
            Ok(Incoming::Message(ClientMessage::Ack(ack))) => {
                match daemon.registry.ack(&session, &ack.event_id) {
                    Some((ts_ms, event)) => daemon.acked(&session, &event, ts_ms),
                    None => warn!(dir = "in", session, event_id = %ack.event_id, "the session acknowledged an event that is not in flight"),
                }
                None
            }
            Ok(Incoming::Message(ClientMessage::Hello(_))) => {
                warn!(session, "ignoring a second hello");
                None
            }
            Ok(Incoming::BadRequest { id, message }) => {
                warn!(session, id, "bad request: {message}");
                Some(CmdResult::err(id, ResultError::BadRequest, message))
            }
            Ok(Incoming::Ignored { message }) => {
                warn!(session, "ignoring an unknown message: {message}");
                None
            }
            Err(err) => {
                warn!(session, "closing: line is not JSON: {err}");
                break;
            }
        };
        if let Some(result) = result {
            // Fails only once the writer is gone, which ends the loop above as well.
            if results.send(DaemonMessage::Result(result)).await.is_err() {
                break;
            }
        }
    }
    writer_task.abort();
    receiver.abort_all().await;
    for (_, file) in received.drain() {
        let _ = tokio::fs::remove_file(&file.path).await;
    }
    daemon.typing_stop_session(&session);
    daemon.session_disconnected(&session);
    info!(session, "session disconnected");
}

/// The writing side of a connection: command results as they come, events with their
/// attachments in order, until the queue closes or a write fails. For the connection's
/// first `event_hold` the events wait behind the queued ones the connection started
/// with, then all go in order.
async fn write_loop(
    daemon: Shared,
    mut writer: OwnedWriteHalf,
    session: String,
    mut outbound: mpsc::Receiver<DaemonMessage>,
    queued: Vec<Event>,
) {
    let count = queued.len();
    let mut held = Some(queued);
    let hold = sleep(daemon.event_hold);
    tokio::pin!(hold);
    loop {
        let written = tokio::select! {
            biased;
            () = &mut hold, if held.is_some() => {
                let mut written = Ok(());
                for event in held.take().unwrap_or_default() {
                    written = write_event(&daemon, &mut writer, &session, &event).await;
                    if written.is_err() {
                        break;
                    }
                }
                if written.is_ok() && count > 0 {
                    info!(dir = "in", session, count, "delivered the messages queued while the session was away");
                }
                written
            }
            message = outbound.recv() => match message {
                None => return,
                Some(DaemonMessage::Event(event)) => match &mut held {
                    Some(held) => {
                        held.push(event);
                        Ok(())
                    }
                    None => write_event(&daemon, &mut writer, &session, &event).await,
                },
                Some(other) => write_line(&mut writer, &other).await,
            },
        };
        if let Err(err) = written {
            warn!(session, "write failed: {err}");
            return;
        }
    }
}

fn wrap(piece: Piece) -> DaemonMessage {
    match piece {
        Piece::Header(h) => DaemonMessage::File(h),
        Piece::Chunk(c) => DaemonMessage::Chunk(c),
        Piece::End(e) => DaemonMessage::FileEnd(e),
    }
}

/// Write an event to a session: each attachment streams from the spool first, then
/// the event line. The spool copy stays until the event is acknowledged, so a second
/// delivery still has the file. An attachment missing from the spool (swept, or the
/// daemon restarted meanwhile) is logged and the event goes without it; the plugin
/// tells the model.
async fn write_event(daemon: &Daemon, writer: &mut OwnedWriteHalf, session: &str, event: &Event) -> io::Result<()> {
    for attachment in &event.attachments {
        let path = daemon.spool.inbox_path(&attachment.transfer);
        let header = FileHeader {
            transfer: attachment.transfer.clone(),
            name: attachment.name.clone(),
            mime: attachment.mime.clone(),
            size: attachment.size,
        };
        match transfer::send(writer, header, &path, wrap).await {
            Ok(()) => debug!(session, transfer = %attachment.transfer, bytes = attachment.size, "attachment transferred"),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                warn!(dir = "in", session, transfer = %attachment.transfer, "attachment is gone from the spool; the event goes to the session without it");
            }
            Err(err) => return Err(err),
        }
    }
    write_line(writer, &DaemonMessage::Event(event.clone())).await
}

async fn handshake(reader: &mut LineReader<tokio::net::unix::OwnedReadHalf>, writer: &mut OwnedWriteHalf) -> Result<Hello> {
    let line = match timeout(HELLO_TIMEOUT, reader.next_line()).await {
        Err(_) => {
            refuse(writer, ErrorCode::BadRequest, "no hello within 5 s".into()).await;
            bail!("no hello within {HELLO_TIMEOUT:?}");
        }
        Ok(Err(err)) => bail!("cannot read the hello line: {err}"),
        Ok(Ok(None)) => bail!("connection closed before hello"),
        Ok(Ok(Some(line))) => line,
    };
    match silta::protocol::parse_client_line(&line) {
        Ok(Incoming::Message(ClientMessage::Hello(hello))) => Ok(hello),
        Ok(_) => {
            refuse(writer, ErrorCode::BadRequest, "the first line must be a hello".into()).await;
            bail!("first line is not a hello");
        }
        Err(err) => bail!("first line is not JSON: {err}"),
    }
}

async fn refuse(writer: &mut OwnedWriteHalf, code: ErrorCode, message: String) {
    warn!(code = code.as_str(), "refusing connection: {message}");
    let _ = write_line(writer, &DaemonMessage::Error(ProtocolError { code, message })).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{daemon::Daemon, spool::Spool};
    use silta::{
        config::{Config, Routing},
        protocol::{Ack, Attachment, EventKind, Role, RoomKind, SendFile, Typing},
        transfer::CHUNK_BYTES,
    };
    use std::sync::Arc;
    use tokio::{net::unix::OwnedReadHalf, task::JoinHandle};

    fn wrap_client(piece: Piece) -> ClientMessage {
        match piece {
            Piece::Header(h) => ClientMessage::File(h),
            Piece::Chunk(c) => ClientMessage::Chunk(c),
            Piece::End(e) => ClientMessage::FileEnd(e),
        }
    }

    /// A daemon with one session, `alice`, run by this process's user, and a client
    /// that has no homeserver to talk to. Events to a fresh connection wait `hold`.
    async fn test_daemon(tag: &str, hold: Duration) -> (Shared, PathBuf) {
        let dir = std::env::temp_dir().join(format!("siltad-server-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let config = Config::parse(&format!(
            r#"
socket = "{0}/sock"
state_dir = "{0}"
[matrix]
homeserver_url = "http://127.0.0.1:1"
user_id = "@silta:silta.test"
password = "x"
device_id = "d"
[[people]]
name = "Alice"
role = "family"
addresses = ["@alice:silta.test"]
[[sessions]]
name = "alice"
receive = {{ people = ["Alice"] }}
user = "whoever"
"#,
            dir.display()
        ))
        .unwrap();
        let client = matrix_sdk::Client::builder().homeserver_url("http://127.0.0.1:1").build().await.unwrap();
        let spool = Spool::new(&dir, u64::MAX);
        spool.prepare().unwrap();
        let users = HashMap::from([("alice".to_owned(), nix::unistd::getuid().as_raw())]);
        let (silence, _) = tokio::sync::mpsc::unbounded_channel();
        let settings =
            crate::daemon::Settings { replay_window_secs: 300, inbox_max_age_days: 30, alert_grace_secs: 600, event_hold: hold };
        (Arc::new(Daemon::new(client, Routing::new(&config), &dir, spool, users, settings, silence)), dir)
    }

    /// Alice's message `$ev` with one attachment, transfer `ev-1`, of `size` bytes.
    fn event(size: u64) -> Event {
        Event {
            kind: EventKind::Message,
            person: "Alice".into(),
            role: Role::Family,
            sender: "@alice:silta.test".into(),
            room_id: "!r:silta.test".into(),
            room: RoomKind::Dm,
            event_id: "$ev".into(),
            ts: "t".into(),
            in_reply_to: None,
            thread: None,
            reacts_to: None,
            text: String::new(),
            transcribed: false,
            attachments: vec![Attachment { transfer: "ev-1".into(), name: "a.bin".into(), mime: "application/octet-stream".into(), size }],
        }
    }

    type Plugin = (JoinHandle<()>, LineReader<OwnedReadHalf>, OwnedWriteHalf);

    /// A fake plugin connected as `alice`, past the welcome.
    async fn connect(daemon: &Shared, cancel: &CancellationToken) -> Plugin {
        let (plugin, server) = UnixStream::pair().unwrap();
        let task = tokio::spawn(handle(daemon.clone(), server, cancel.clone()));
        let (read_half, mut writer) = plugin.into_split();
        let mut reader = LineReader::new(read_half);
        let hello = Hello { protocol: PROTOCOL_VERSION, session: "alice".into(), client: "test".into() };
        write_line(&mut writer, &ClientMessage::Hello(hello)).await.unwrap();
        assert!(matches!(reader.next_json::<DaemonMessage>().await.unwrap(), Some(DaemonMessage::Welcome(_))));
        (task, reader, writer)
    }

    async fn next(reader: &mut LineReader<OwnedReadHalf>) -> DaemonMessage {
        let next = timeout(Duration::from_secs(10), reader.next_json::<DaemonMessage>()).await.expect("the daemon stopped writing");
        next.unwrap().expect("the daemon closed the connection")
    }

    /// An event preceded by its attachment's transfer; returns the chunk count too.
    async fn read_event(reader: &mut LineReader<OwnedReadHalf>) -> (usize, Event) {
        let mut chunks = 0;
        loop {
            match next(reader).await {
                DaemonMessage::File(h) => assert_eq!(h.transfer, "ev-1"),
                DaemonMessage::Chunk(_) => chunks += 1,
                DaemonMessage::FileEnd(_) => {}
                DaemonMessage::Event(e) => return (chunks, e),
                other => panic!("unexpected {other:?}"),
            }
        }
    }

    async fn read_result(reader: &mut LineReader<OwnedReadHalf>) -> CmdResult {
        match next(reader).await {
            DaemonMessage::Result(r) => r,
            other => panic!("unexpected {other:?}"),
        }
    }

    /// A file crossing each way at the same moment must not hang the connection. The
    /// fake plugin streams its file without reading meanwhile, as the real one does,
    /// while the daemon streams an attachment to it; with one task doing both the
    /// daemon's writing and reading, both socket buffers filled and nothing moved.
    #[tokio::test]
    async fn files_crossing_both_ways_do_not_deadlock() {
        let (daemon, dir) = test_daemon("crossing", Duration::ZERO).await;
        // Three chunks each way: an attachment waiting in the spool, a file to send.
        let content: Vec<u8> = (0..3 * CHUNK_BYTES).map(|i| (i % 253) as u8).collect();
        std::fs::write(daemon.spool.inbox_path("ev-1"), &content).unwrap();
        let upload = dir.join("upload.bin");
        std::fs::write(&upload, &content).unwrap();
        let cancel = CancellationToken::new();
        let (task, mut reader, mut writer) = connect(&daemon, &cancel).await;

        daemon.registry.deliver("alice", event(content.len() as u64), 1).unwrap();
        let header = FileHeader { transfer: "t1".into(), name: "up.bin".into(), mime: "application/octet-stream".into(), size: content.len() as u64 };
        let streamed = timeout(Duration::from_secs(10), transfer::send(&mut writer, header, &upload, wrap_client)).await;
        assert!(streamed.is_ok(), "the plugin's transfer hung: the daemon stopped reading while it was writing");
        streamed.unwrap().unwrap();
        let send_file = SendFile { room_id: "!r:silta.test".into(), transfer: "t1".into(), caption: None, reply_to: None, thread: None, more: false };
        write_line(&mut writer, &ClientMessage::Cmd(Cmd { id: 1, kind: CmdKind::SendFile(send_file) })).await.unwrap();

        // The attachment and its event, then the answer to the command (there is no
        // such room, so a refusal, but an answer).
        let (chunks, e) = read_event(&mut reader).await;
        assert_eq!((chunks, e.attachments[0].transfer.as_str()), (3, "ev-1"));
        let result = read_result(&mut reader).await;
        assert_eq!((result.id, result.error), (1, Some(ResultError::RoomUnknown)));
        // The upload was consumed; the attachment waits for the acknowledgement.
        assert!(std::fs::read_dir(&daemon.spool.outbox).unwrap().next().is_none());
        assert!(daemon.spool.inbox_path("ev-1").exists());
        cancel.cancel();
        let _ = task.await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An event is delivered on the plugin's acknowledgement, not on the write: one a
    /// connection never acknowledged comes again, attachment included, on the next
    /// connection, and the watermark moves only on the ack.
    #[tokio::test]
    async fn unacknowledged_events_come_back_on_the_next_connection() {
        let (daemon, dir) = test_daemon("ack", Duration::ZERO).await;
        let content = vec![7u8; 10];
        std::fs::write(daemon.spool.inbox_path("ev-1"), &content).unwrap();
        let cancel = CancellationToken::new();

        // The first connection takes the event and goes away without acknowledging it.
        // (A current timestamp: the backlog keeps nothing older than the replay window.)
        let ts = now_ms();
        let (task, mut reader, writer) = connect(&daemon, &cancel).await;
        daemon.registry.deliver("alice", event(content.len() as u64), ts).unwrap();
        let (_, e) = read_event(&mut reader).await;
        assert_eq!(e.event_id, "$ev");
        assert!(daemon.watermark("!r:silta.test").is_none());
        drop(writer);
        drop(reader);
        task.await.unwrap();
        assert!(daemon.spool.inbox_path("ev-1").exists(), "the spool keeps the attachment until the ack");

        // The next connection gets it right after the welcome, then acknowledges it;
        // a command after the ack proves the ack was processed before its answer.
        let (task, mut reader, mut writer) = connect(&daemon, &cancel).await;
        let (chunks, e) = read_event(&mut reader).await;
        assert_eq!((chunks, e.event_id.as_str()), (1, "$ev"));
        write_line(&mut writer, &ClientMessage::Ack(Ack { event_id: "$ev".into() })).await.unwrap();
        let typing = ClientMessage::Cmd(Cmd { id: 1, kind: CmdKind::Typing(Typing { room_id: "!r:silta.test".into() }) });
        write_line(&mut writer, &typing).await.unwrap();
        assert_eq!(read_result(&mut reader).await.id, 1);
        let mark = daemon.watermark("!r:silta.test").expect("acknowledged");
        assert_eq!((mark.ts_ms, mark.event_id.as_str()), (ts, "$ev"));
        assert!(!daemon.spool.inbox_path("ev-1").exists());
        drop(writer);
        drop(reader);
        task.await.unwrap();

        // Acknowledged: a third connection gets nothing.
        let (task, mut reader, _writer) = connect(&daemon, &cancel).await;
        assert!(timeout(Duration::from_millis(300), reader.next_line()).await.is_err(), "nothing should be queued");
        cancel.cancel();
        let _ = task.await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The band-aid hold: a fresh connection gets no event before it is past, then the
    /// queued ones and those delivered meanwhile, in order; a command's result does not
    /// wait for it.
    #[tokio::test]
    async fn events_to_a_fresh_connection_wait_for_the_hold() {
        let hold = Duration::from_secs(1);
        let (daemon, dir) = test_daemon("hold", hold).await;
        let cancel = CancellationToken::new();
        let without_file = |event_id: &str| Event { event_id: event_id.into(), attachments: Vec::new(), ..event(0) };
        daemon.registry.enqueue("alice", now_ms(), without_file("$queued"), now_ms());

        // Timed from before the connection, so the hold cannot have started earlier.
        let start = std::time::Instant::now();
        let (task, mut reader, mut writer) = connect(&daemon, &cancel).await;
        daemon.registry.deliver("alice", without_file("$live"), now_ms()).unwrap();
        let typing = ClientMessage::Cmd(Cmd { id: 1, kind: CmdKind::Typing(Typing { room_id: "!r:silta.test".into() }) });
        write_line(&mut writer, &typing).await.unwrap();
        assert_eq!(read_result(&mut reader).await.id, 1);
        assert!(start.elapsed() < hold, "the command's result waited for the hold");
        let (_, first) = read_event(&mut reader).await;
        assert!(start.elapsed() >= hold, "an event came before the hold was past");
        let (_, second) = read_event(&mut reader).await;
        assert_eq!((first.event_id.as_str(), second.event_id.as_str()), ("$queued", "$live"));
        cancel.cancel();
        let _ = task.await;
        let _ = std::fs::remove_dir_all(&dir);
    }
}
