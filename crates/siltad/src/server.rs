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
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{
    daemon::{now_ms, Daemon, Shared},
    outbound,
};

const HELLO_TIMEOUT: Duration = Duration::from_secs(5);

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
    let Some((mut outbound, _claim)) = daemon.registry.claim(&session) else {
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

    // Messages that arrived while the session was away, oldest first.
    let (queued, evicted) = daemon.registry.take_backlog(&session, now_ms());
    if evicted > 0 {
        warn!(session, evicted, "dropped queued messages older than the replay window");
    }
    let count = queued.len();
    for (ts_ms, event) in queued {
        if let Err(err) = write_event(&daemon, &mut writer, &session, &event).await {
            warn!(session, "write failed while delivering the backlog: {err}");
            return;
        }
        daemon.after_delivery(&session, &event, ts_ms);
    }
    if count > 0 {
        info!(session, count, "delivered the messages queued while the session was away");
    }

    // Files the session streams for `send_file`, spooled until the command names them.
    let mut receiver = Receiver::new(daemon.spool.outbox.clone(), format!("{session}-"), daemon.spool.max_bytes);
    let mut received: HashMap<String, Received> = HashMap::new();
    let mut failed: HashMap<String, String> = HashMap::new();

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            message = outbound.recv() => match message {
                Some(message) => {
                    let written = match &message {
                        DaemonMessage::Event(event) => write_event(&daemon, &mut writer, &session, event).await,
                        other => write_line(&mut writer, other).await,
                    };
                    if let Err(err) = written {
                        warn!(session, "write failed: {err}");
                        break;
                    }
                }
                None => break,
            },
            line = reader.next_line() => match line {
                Ok(Some(line)) => match silta::protocol::parse_client_line(&line) {
                    Ok(Incoming::Message(ClientMessage::Cmd(cmd))) => {
                        let result = match cmd.kind {
                            CmdKind::SendFile(file) => match received.remove(&file.transfer) {
                                Some(file_received) => outbound::send_file(&daemon, &session, cmd.id, file, file_received).await,
                                None => {
                                    let why = failed.remove(&file.transfer).unwrap_or_else(|| "no such transfer was received before the command".to_owned());
                                    CmdResult::err(cmd.id, ResultError::FileError, format!("transfer {:?}: {why}", file.transfer))
                                }
                            },
                            kind => daemon.execute(&session, Cmd { id: cmd.id, kind }).await,
                        };
                        if let Err(err) = write_line(&mut writer, &DaemonMessage::Result(result)).await {
                            warn!(session, "write failed: {err}");
                            break;
                        }
                    }
                    Ok(Incoming::Message(ClientMessage::File(header))) => {
                        debug!(session, transfer = %header.transfer, name = %header.name, size = header.size, "file transfer begins");
                        let transfer = header.transfer.clone();
                        if let Err(err) = receiver.begin(header).await {
                            warn!(session, transfer, "file transfer refused: {err}");
                            failed.insert(transfer, err.to_string());
                        }
                    }
                    Ok(Incoming::Message(ClientMessage::Chunk(chunk))) => {
                        let transfer = chunk.transfer.clone();
                        if let Err(err) = receiver.chunk(chunk).await {
                            // Logged once per transfer, not once per chunk.
                            if let Entry::Vacant(slot) = failed.entry(transfer) {
                                warn!(session, transfer = %slot.key(), "file transfer failed: {err}");
                                slot.insert(err.to_string());
                            }
                        }
                    }
                    Ok(Incoming::Message(ClientMessage::FileEnd(end))) => {
                        let transfer = end.transfer.clone();
                        match receiver.end(end).await {
                            Ok(done) => {
                                debug!(session, transfer, bytes = done.header.size, "file transfer complete");
                                received.insert(transfer, done);
                            }
                            Err(err) => {
                                if let Entry::Vacant(slot) = failed.entry(transfer) {
                                    warn!(session, transfer = %slot.key(), "file transfer failed: {err}");
                                    slot.insert(err.to_string());
                                }
                            }
                        }
                    }
                    Ok(Incoming::Message(ClientMessage::Hello(_))) => {
                        warn!(session, "ignoring a second hello");
                    }
                    Ok(Incoming::BadRequest { id, message }) => {
                        warn!(session, id, "bad request: {message}");
                        let result = CmdResult::err(id, ResultError::BadRequest, message);
                        if let Err(err) = write_line(&mut writer, &DaemonMessage::Result(result)).await {
                            warn!(session, "write failed: {err}");
                            break;
                        }
                    }
                    Ok(Incoming::Ignored { message }) => {
                        warn!(session, "ignoring an unknown message: {message}");
                    }
                    Err(err) => {
                        warn!(session, "closing: line is not JSON: {err}");
                        break;
                    }
                },
                Ok(None) => break,
                Err(err) => {
                    warn!(session, "closing: {err}");
                    break;
                }
            },
        }
    }
    receiver.abort_all().await;
    for (_, file) in received.drain() {
        let _ = tokio::fs::remove_file(&file.path).await;
    }
    daemon.typing_stop_session(&session);
    info!(session, "session disconnected");
}

fn wrap(piece: Piece) -> DaemonMessage {
    match piece {
        Piece::Header(h) => DaemonMessage::File(h),
        Piece::Chunk(c) => DaemonMessage::Chunk(c),
        Piece::End(e) => DaemonMessage::FileEnd(e),
    }
}

/// Write an event to a session: each attachment streams from the spool first (and
/// leaves it), then the event line. An attachment missing from the spool (swept, or
/// the daemon restarted meanwhile) is logged and the event goes without it; the
/// plugin tells the model.
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
            Ok(()) => {
                debug!(session, transfer = %attachment.transfer, bytes = attachment.size, "attachment transferred");
                let _ = tokio::fs::remove_file(&path).await;
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                warn!(session, transfer = %attachment.transfer, "attachment is gone from the spool; the event goes without it");
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
