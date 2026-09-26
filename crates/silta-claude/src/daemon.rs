//! The socket client: connects to `siltad`, keeps the connection alive with backoff,
//! forwards events and multiplexes commands. Files come and go as chunked transfers:
//! an attachment lands in the session's inbox before its event, and a file for
//! `send_file` is streamed to the daemon before the command.

use std::{
    collections::HashMap,
    hash::{BuildHasher, Hasher, RandomState},
    io,
    path::{Path, PathBuf},
    time::Duration,
};

use silta::{
    line::{write_line, LineReader},
    protocol::{
        parse_daemon_line, Ack, ClientMessage, Cmd, CmdKind, CmdResult, DaemonMessage, Event,
        FileHeader, Hello, ResultError, SendFile, Welcome, PROTOCOL_VERSION,
    },
    transfer::{self, sweep, Piece, Receiver},
};
use thiserror::Error;
use tokio::{
    net::{
        unix::{OwnedReadHalf, OwnedWriteHalf},
        UnixStream,
    },
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

pub const CLIENT_NAME: &str = concat!("silta-claude/", env!("CARGO_PKG_VERSION"));

/// How long a command may wait for its result.
const RESULT_TIMEOUT: Duration = Duration::from_secs(60);
/// A file has to cross the socket and be uploaded first.
const SEND_FILE_TIMEOUT: Duration = Duration::from_secs(300);
/// How long the daemon has to answer `hello`.
const WELCOME_TIMEOUT: Duration = Duration::from_secs(10);
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
const SWEEP_EVERY: Duration = Duration::from_secs(24 * 3600);
/// How often the ready file is looked for.
const READY_POLL: Duration = Duration::from_millis(100);

#[derive(Debug, Error)]
pub enum DaemonError {
    /// No connection to the daemon, or it was lost before a result arrived.
    #[error("daemon_unavailable: {0}")]
    Unavailable(String),
    #[error("timeout: no result from the daemon within {0:?}")]
    Timeout(Duration),
    /// The daemon answered and refused.
    #[error("{}: {message}", code.as_str())]
    Refused { code: ResultError, message: String },
}

/// An event with the local path of each attachment, `None` when its transfer did not
/// arrive (the model is told so).
#[derive(Debug)]
pub struct Inbound {
    pub event: Event,
    pub paths: Vec<Option<PathBuf>>,
}

struct Command {
    kind: CmdKind,
    /// For `send_file`: the file to stream before the command, with its header (the
    /// transfer id is filled in on the connection).
    file: Option<(PathBuf, FileHeader)>,
    done: oneshot::Sender<Result<CmdResult, DaemonError>>,
}

enum Outgoing {
    Command(Box<Command>),
    /// The channel notification for this event reached Claude Code.
    Ack(String),
}

/// Handle to the connection task. Cheap to clone.
#[derive(Clone)]
pub struct DaemonClient {
    tx: mpsc::Sender<Outgoing>,
}

impl DaemonClient {
    /// Spawn the connection task. It connects once `ready` fires (the MCP client has
    /// sent `initialized`) and `ready_file`, if given, exists; events from the daemon
    /// then go to `events`, attachments are written under `inbox`.
    pub fn start(
        socket: PathBuf,
        session: String,
        inbox: PathBuf,
        ready_file: Option<PathBuf>,
        events: mpsc::Sender<Inbound>,
        ready: oneshot::Receiver<()>,
        cancel: CancellationToken,
    ) -> DaemonClient {
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            if wait_until_ready(ready, ready_file.as_deref(), &cancel).await {
                run(socket, session, inbox, events, rx, cancel).await;
            }
        });
        DaemonClient { tx }
    }

    /// Send a command and wait for the daemon's verdict. A refusal is an error with
    /// the daemon's code; a successful result is returned whole.
    pub async fn command(&self, kind: CmdKind) -> Result<CmdResult, DaemonError> {
        self.submit(kind, None, RESULT_TIMEOUT).await
    }

    /// Stream a file from this host to the daemon, then the `send_file` command for it.
    /// The path is checked here, as this process's user, which is the whole point: a
    /// session can only send what it can read.
    pub async fn send_file(
        &self,
        path: PathBuf,
        mut cmd: SendFile,
    ) -> Result<CmdResult, DaemonError> {
        let refuse = |message: String| DaemonError::Refused {
            code: ResultError::FileError,
            message,
        };
        if !path.is_absolute() {
            return Err(refuse(format!(
                "{} is not an absolute path",
                path.display()
            )));
        }
        let meta = tokio::fs::metadata(&path)
            .await
            .map_err(|e| refuse(format!("cannot read {}: {e}", path.display())))?;
        if !meta.is_file() {
            return Err(refuse(format!("{} is not a regular file", path.display())));
        }
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file")
            .to_owned();
        let mime = mime_guess::from_path(&path)
            .first_or_octet_stream()
            .to_string();
        let header = FileHeader {
            transfer: String::new(),
            name,
            mime,
            size: meta.len(),
        };
        cmd.transfer = String::new();
        self.submit(
            CmdKind::SendFile(cmd),
            Some((path, header)),
            SEND_FILE_TIMEOUT,
        )
        .await
    }

    /// Tell the daemon that an event's channel notification reached Claude Code. Best
    /// effort: without a connection the daemon delivers the event again anyway.
    pub async fn ack(&self, event_id: String) {
        if self.tx.send(Outgoing::Ack(event_id)).await.is_err() {
            debug!("acknowledgement dropped: connection task stopped");
        }
    }

    async fn submit(
        &self,
        kind: CmdKind,
        file: Option<(PathBuf, FileHeader)>,
        timeout: Duration,
    ) -> Result<CmdResult, DaemonError> {
        let (done, wait) = oneshot::channel();
        let out = Outgoing::Command(Box::new(Command { kind, file, done }));
        self.tx
            .send(out)
            .await
            .map_err(|_| DaemonError::Unavailable("connection task stopped".into()))?;
        let result = time::timeout(timeout, wait)
            .await
            .map_err(|_| DaemonError::Timeout(timeout))?
            .map_err(|_| DaemonError::Unavailable("connection lost".into()))??;
        if result.ok {
            Ok(result)
        } else {
            Err(DaemonError::Refused {
                code: result.error.unwrap_or(ResultError::Unknown),
                message: result.message.unwrap_or_default(),
            })
        }
    }
}

/// Wait until the plugin may take events: the MCP client has sent `initialized` and,
/// when a ready file is given, it exists. False if the process shuts down first.
///
/// The daemon counts an event as delivered once this process has handed its
/// notification to Claude Code, and Claude Code drops a notification it has no handler
/// for without a word. Waiting for `initialized` keeps the messages in the daemon while
/// a session's handshake never completes (one started while its predecessor was still
/// shutting down). In `-p` mode Claude Code installs the channel's handler later still,
/// when its command loop starts the first turn, and the daemon sends its backlog at
/// once: the supervisor creates the ready file when a model has answered that turn,
/// so a session whose model cannot serve takes no message.
async fn wait_until_ready(
    ready: oneshot::Receiver<()>,
    ready_file: Option<&Path>,
    cancel: &CancellationToken,
) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => return false,
        ready = ready => if ready.is_err() {
            return false;
        },
    }
    let Some(path) = ready_file else {
        return true;
    };
    info!(
        "waiting for {} before connecting to the daemon",
        path.display()
    );
    let started = time::Instant::now();
    while !path.exists() {
        tokio::select! {
            _ = cancel.cancelled() => return false,
            _ = time::sleep(READY_POLL) => {}
        }
    }
    info!(
        waited_ms = started.elapsed().as_millis() as u64,
        "Claude Code has registered the channel, connecting to the daemon"
    );
    true
}

async fn run(
    socket: PathBuf,
    session: String,
    inbox: PathBuf,
    events: mpsc::Sender<Inbound>,
    mut rx: mpsc::Receiver<Outgoing>,
    cancel: CancellationToken,
) {
    let mut backoff = Backoff::new();
    let mut sweeper: Option<JoinHandle<()>> = None;
    loop {
        if cancel.is_cancelled() {
            break;
        }
        match connect(&socket, &session).await {
            Ok((reader, writer, welcome)) => {
                info!(
                    session = %welcome.session,
                    bot = %welcome.user_id,
                    people = welcome.people.len(),
                    "connected to daemon"
                );
                backoff.reset();
                if sweeper.is_none() {
                    let max_age =
                        Duration::from_secs(welcome.inbox_max_age_days.saturating_mul(86_400));
                    sweeper = Some(spawn_sweeper(inbox.clone(), max_age));
                }
                let reason = serve(reader, writer, &inbox, &events, &mut rx, &cancel).await;
                if cancel.is_cancelled() {
                    break;
                }
                warn!("disconnected from daemon: {reason}");
            }
            Err(err) => warn!("cannot connect to daemon at {}: {err}", socket.display()),
        }

        // Wait before reconnecting; commands arriving meanwhile fail at once.
        let delay = backoff.next();
        debug!("reconnecting in {delay:?}");
        let sleep = time::sleep(delay);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = &mut sleep => break,
                out = rx.recv() => match out {
                    Some(Outgoing::Command(command)) => {
                        let _ = command.done.send(Err(DaemonError::Unavailable("not connected".into())));
                    }
                    // Nobody to acknowledge to: the daemon delivers the event again.
                    Some(Outgoing::Ack(_)) => {}
                    None => break,
                },
            }
        }
        if cancel.is_cancelled() || rx.is_closed() {
            break;
        }
    }
    if let Some(sweeper) = sweeper {
        sweeper.abort();
    }
}

/// Delete inbox files older than `max_age`, at start and once a day.
fn spawn_sweeper(inbox: PathBuf, max_age: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match sweep(&inbox, max_age).await {
                Ok(0) => debug!("inbox sweep: nothing to remove"),
                Ok(removed) => info!(
                    removed,
                    "inbox sweep: removed files older than {} days",
                    max_age.as_secs() / 86_400
                ),
                Err(err) => warn!("inbox sweep of {} failed: {err}", inbox.display()),
            }
            time::sleep(SWEEP_EVERY).await;
        }
    })
}

type Reader = LineReader<OwnedReadHalf>;

/// Connect and complete the handshake. The reader that read the `welcome` is handed
/// on: the daemon may follow the welcome with queued events at once, and a throwaway
/// buffered reader would swallow them.
async fn connect(
    socket: &Path,
    session: &str,
) -> Result<(Reader, OwnedWriteHalf, Welcome), String> {
    let stream = UnixStream::connect(socket)
        .await
        .map_err(|e| e.to_string())?;
    let (read_half, mut writer) = stream.into_split();
    let mut reader = LineReader::new(read_half);
    let hello = ClientMessage::Hello(Hello {
        protocol: PROTOCOL_VERSION,
        session: session.to_owned(),
        client: CLIENT_NAME.to_owned(),
    });
    write_line(&mut writer, &hello)
        .await
        .map_err(|e| format!("cannot send hello: {e}"))?;

    let first = time::timeout(WELCOME_TIMEOUT, reader.next_json::<DaemonMessage>())
        .await
        .map_err(|_| "no welcome within the timeout".to_owned())?
        .map_err(|e| format!("bad first line from daemon: {e}"))?;
    match first {
        Some(DaemonMessage::Welcome(welcome)) => {
            if welcome.protocol != PROTOCOL_VERSION {
                return Err(format!(
                    "daemon speaks protocol {}, this plugin speaks {PROTOCOL_VERSION}",
                    welcome.protocol
                ));
            }
            Ok((reader, writer, welcome))
        }
        Some(DaemonMessage::Error(err)) => {
            error!(
                code = err.code.as_str(),
                "daemon refused the session: {}", err.message
            );
            Err(format!("{}: {}", err.code.as_str(), err.message))
        }
        Some(other) => Err(format!("expected welcome, got {other:?}")),
        None => Err("daemon closed the connection during the handshake".into()),
    }
}

fn wrap(piece: Piece) -> ClientMessage {
    match piece {
        Piece::Header(h) => ClientMessage::File(h),
        Piece::Chunk(c) => ClientMessage::Chunk(c),
        Piece::End(e) => ClientMessage::FileEnd(e),
    }
}

/// Multiplex one live connection until it breaks. Returns the reason.
async fn serve(
    mut reader: Reader,
    mut write_half: OwnedWriteHalf,
    inbox: &Path,
    events: &mpsc::Sender<Inbound>,
    rx: &mut mpsc::Receiver<Outgoing>,
    cancel: &CancellationToken,
) -> String {
    let mut pending: HashMap<u64, oneshot::Sender<Result<CmdResult, DaemonError>>> = HashMap::new();
    let mut next_id: u64 = 1;
    // Attachments arrive before their event; the daemon caps their size.
    let mut receiver = Receiver::new(inbox.to_path_buf(), "", u64::MAX);
    let mut received: HashMap<String, PathBuf> = HashMap::new();

    let reason = loop {
        tokio::select! {
            _ = cancel.cancelled() => break "shutdown".to_owned(),
            line = reader.next_line() => match line {
                Ok(Some(line)) => match parse_daemon_line(&line) {
                    Ok(DaemonMessage::Event(event)) => {
                        debug!(room = %event.room_id, person = %event.person, "event");
                        let paths = event.attachments.iter().map(|a| received.remove(&a.transfer)).collect();
                        if events.send(Inbound { event, paths }).await.is_err() {
                            break "event consumer gone".to_owned();
                        }
                    }
                    Ok(DaemonMessage::File(header)) => {
                        debug!(transfer = %header.transfer, name = %header.name, size = header.size, "attachment transfer begins");
                        if let Err(err) = receiver.begin(header).await {
                            warn!("attachment transfer refused: {err}");
                        }
                    }
                    Ok(DaemonMessage::Chunk(chunk)) => {
                        if let Err(err) = receiver.chunk(chunk).await {
                            warn!("attachment transfer failed: {err}");
                        }
                    }
                    Ok(DaemonMessage::FileEnd(end)) => match receiver.end(end).await {
                        Ok(done) => {
                            info!(transfer = %done.header.transfer, bytes = done.header.size, "attachment received as {}", done.path.display());
                            received.insert(done.header.transfer, done.path);
                        }
                        Err(err) => warn!("attachment transfer failed: {err}"),
                    },
                    Ok(DaemonMessage::Result(result)) => match pending.remove(&result.id) {
                        Some(done) => {
                            let _ = done.send(Ok(result));
                        }
                        None => warn!(id = result.id, "result for an unknown command"),
                    },
                    Ok(DaemonMessage::Error(err)) => {
                        break format!("daemon error {}: {}", err.code.as_str(), err.message);
                    }
                    Ok(DaemonMessage::Welcome(_)) => warn!("unexpected second welcome"),
                    // A newer daemon may send messages this plugin does not know; that
                    // is no reason to drop the connection.
                    Err(err) => warn!("ignoring a daemon message this plugin cannot parse: {err}"),
                },
                Ok(None) => break "daemon closed the connection".to_owned(),
                Err(err) => break format!("read error: {err}"),
            },
            out = rx.recv() => match out {
                Some(Outgoing::Ack(event_id)) => {
                    if let Err(err) = write_line(&mut write_half, &ClientMessage::Ack(Ack { event_id })).await {
                        break format!("write error: {err}");
                    }
                }
                Some(Outgoing::Command(command)) => {
                    let Command { kind, file, done } = *command;
                    let id = next_id;
                    next_id += 1;
                    let kind = match (kind, file) {
                        (CmdKind::SendFile(mut cmd), Some((path, mut header))) => {
                            let transfer = format!("t{id}");
                            header.transfer = transfer.clone();
                            cmd.transfer = transfer;
                            match transfer::send(&mut write_half, header, &path, wrap).await {
                                Ok(()) => {}
                                // The file is opened before anything is written, so a local
                                // error leaves the connection clean.
                                Err(err) if is_local(&err) => {
                                    let message = format!("cannot read {}: {err}", path.display());
                                    let _ = done.send(Err(DaemonError::Refused { code: ResultError::FileError, message }));
                                    continue;
                                }
                                Err(err) => {
                                    let _ = done.send(Err(DaemonError::Unavailable(format!("write failed: {err}"))));
                                    break format!("write error: {err}");
                                }
                            }
                            CmdKind::SendFile(cmd)
                        }
                        (kind, _) => kind,
                    };
                    if let Err(err) = send_cmd(&mut write_half, id, kind).await {
                        let _ = done.send(Err(DaemonError::Unavailable(format!("write failed: {err}"))));
                        break format!("write error: {err}");
                    }
                    pending.insert(id, done);
                }
                None => break "client handle dropped".to_owned(),
            },
        }
    };

    receiver.abort_all().await;
    for (_, done) in pending.drain() {
        let _ = done.send(Err(DaemonError::Unavailable("connection lost".into())));
    }
    reason
}

fn is_local(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied | io::ErrorKind::IsADirectory
    )
}

async fn send_cmd(writer: &mut OwnedWriteHalf, id: u64, kind: CmdKind) -> std::io::Result<()> {
    debug!(id, cmd = kind.name(), "sending command");
    write_line(writer, &ClientMessage::Cmd(Cmd { id, kind })).await
}

/// Exponential backoff with jitter, no external RNG.
struct Backoff {
    current: Duration,
}

impl Backoff {
    fn new() -> Self {
        Backoff {
            current: BACKOFF_MIN,
        }
    }

    fn reset(&mut self) {
        self.current = BACKOFF_MIN;
    }

    fn next(&mut self) -> Duration {
        let base = self.current;
        self.current = (self.current * 2).min(BACKOFF_MAX);
        // +/- 25 % jitter from the std hasher's random seed.
        let r = RandomState::new().build_hasher().finish() % 1000;
        let factor = 0.75 + (r as f64 / 1000.0) * 0.5;
        base.mul_f64(factor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn initialized() -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        tx.send(()).unwrap();
        rx
    }

    /// Past `initialized`, the connection waits for the ready file; a shutdown ends the
    /// wait.
    #[tokio::test]
    async fn the_connection_waits_for_the_ready_file() {
        let path = std::env::temp_dir().join(format!("silta-claude-ready-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let cancel = CancellationToken::new();
        let wait = tokio::spawn({
            let (path, cancel) = (path.clone(), cancel.clone());
            async move { wait_until_ready(initialized(), Some(&path), &cancel).await }
        });
        time::sleep(Duration::from_millis(300)).await;
        assert!(!wait.is_finished(), "went on without the ready file");
        std::fs::write(&path, "").unwrap();
        assert!(time::timeout(Duration::from_secs(2), wait)
            .await
            .unwrap()
            .unwrap());
        std::fs::remove_file(&path).unwrap();

        let wait = tokio::spawn({
            let (path, cancel) = (path.clone(), cancel.clone());
            async move { wait_until_ready(initialized(), Some(&path), &cancel).await }
        });
        cancel.cancel();
        assert!(!time::timeout(Duration::from_secs(2), wait)
            .await
            .unwrap()
            .unwrap());
    }
}
