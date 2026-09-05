//! The Unix socket server: one connection per session, hello handshake, then
//! commands in and events out.

use std::{fs, os::unix::fs::{FileTypeExt, PermissionsExt}, path::PathBuf, time::Duration};

use anyhow::{bail, Context, Result};
use silta::{
    line::{write_line, LineReader},
    protocol::{
        ClientMessage, CmdResult, DaemonMessage, ErrorCode, Hello, Incoming, ProtocolError, ResultError,
        Welcome, PROTOCOL_VERSION,
    },
};
use tokio::{
    net::{unix::OwnedWriteHalf, UnixListener, UnixStream},
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::daemon::Shared;

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
    let Some((mut outbound, _claim)) = daemon.registry.claim(&session) else {
        refuse(&mut writer, ErrorCode::SessionBusy, format!("session {session:?} is already connected")).await;
        return;
    };
    let welcome = DaemonMessage::Welcome(Welcome {
        protocol: PROTOCOL_VERSION,
        session: session.clone(),
        user_id: daemon.routing.bot_user_id().to_owned(),
        people: daemon.routing.people_for(&session),
    });
    if let Err(err) = write_line(&mut writer, &welcome).await {
        warn!(session, "cannot send welcome: {err}");
        return;
    }
    info!(session, client = %hello.client, "session connected");

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            message = outbound.recv() => match message {
                Some(message) => {
                    if let Err(err) = write_line(&mut writer, &message).await {
                        warn!(session, "write failed: {err}");
                        break;
                    }
                }
                None => break,
            },
            line = reader.next_line() => match line {
                Ok(Some(line)) => match silta::protocol::parse_client_line(&line) {
                    Ok(Incoming::Message(ClientMessage::Cmd(cmd))) => {
                        let result = daemon.execute(&session, cmd).await;
                        if let Err(err) = write_line(&mut writer, &DaemonMessage::Result(result)).await {
                            warn!(session, "write failed: {err}");
                            break;
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
    info!(session, "session disconnected");
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
