//! The socket client: connects to `siltad`, keeps the connection alive with backoff,
//! forwards events and multiplexes commands.

use std::{
    collections::HashMap,
    hash::{BuildHasher, Hasher, RandomState},
    path::{Path, PathBuf},
    time::Duration,
};

use silta::{
    line::{write_line, LineReader},
    protocol::{
        parse_daemon_line, Cmd, CmdKind, CmdResult, ClientMessage, DaemonMessage, Event, Hello, Reply,
        ResultError, Welcome, PROTOCOL_VERSION,
    },
};
use thiserror::Error;
use tokio::{
    net::{
        unix::{OwnedReadHalf, OwnedWriteHalf},
        UnixStream,
    },
    sync::{mpsc, oneshot},
    time,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

pub const CLIENT_NAME: &str = concat!("silta-claude/", env!("CARGO_PKG_VERSION"));

/// How long a command may wait for its result.
const RESULT_TIMEOUT: Duration = Duration::from_secs(60);
/// How long the daemon has to answer `hello`.
const WELCOME_TIMEOUT: Duration = Duration::from_secs(10);
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

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

struct Outgoing {
    kind: CmdKind,
    done: oneshot::Sender<Result<CmdResult, DaemonError>>,
}

/// Handle to the connection task. Cheap to clone.
#[derive(Clone)]
pub struct DaemonClient {
    tx: mpsc::Sender<Outgoing>,
}

impl DaemonClient {
    /// Spawn the connection task. Events from the daemon go to `events`.
    pub fn start(
        socket: PathBuf,
        session: String,
        events: mpsc::Sender<Event>,
        cancel: CancellationToken,
    ) -> DaemonClient {
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(run(socket, session, events, rx, cancel));
        DaemonClient { tx }
    }

    /// Send a reply and wait for the daemon's verdict. Returns the event id of the
    /// last message sent.
    pub async fn reply(&self, reply: Reply) -> Result<String, DaemonError> {
        let (done, wait) = oneshot::channel();
        self.tx
            .send(Outgoing { kind: CmdKind::Reply(reply), done })
            .await
            .map_err(|_| DaemonError::Unavailable("connection task stopped".into()))?;
        let result = time::timeout(RESULT_TIMEOUT, wait)
            .await
            .map_err(|_| DaemonError::Timeout(RESULT_TIMEOUT))?
            .map_err(|_| DaemonError::Unavailable("connection lost".into()))??;
        if result.ok {
            Ok(result.event_id.unwrap_or_default())
        } else {
            Err(DaemonError::Refused {
                code: result.error.unwrap_or(ResultError::Unknown),
                message: result.message.unwrap_or_default(),
            })
        }
    }
}

async fn run(
    socket: PathBuf,
    session: String,
    events: mpsc::Sender<Event>,
    mut rx: mpsc::Receiver<Outgoing>,
    cancel: CancellationToken,
) {
    let mut backoff = Backoff::new();
    loop {
        if cancel.is_cancelled() {
            return;
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
                let reason = serve(reader, writer, &events, &mut rx, &cancel).await;
                if cancel.is_cancelled() {
                    return;
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
                _ = cancel.cancelled() => return,
                _ = &mut sleep => break,
                out = rx.recv() => match out {
                    Some(out) => {
                        let _ = out.done.send(Err(DaemonError::Unavailable("not connected".into())));
                    }
                    None => return,
                },
            }
        }
    }
}

type Reader = LineReader<OwnedReadHalf>;

/// Connect and complete the handshake. The reader that read the `welcome` is handed
/// on: the daemon may follow the welcome with queued events at once, and a throwaway
/// buffered reader would swallow them.
async fn connect(socket: &Path, session: &str) -> Result<(Reader, OwnedWriteHalf, Welcome), String> {
    let stream = UnixStream::connect(socket).await.map_err(|e| e.to_string())?;
    let (read_half, mut writer) = stream.into_split();
    let mut reader = LineReader::new(read_half);
    let hello = ClientMessage::Hello(Hello {
        protocol: PROTOCOL_VERSION,
        session: session.to_owned(),
        client: CLIENT_NAME.to_owned(),
    });
    write_line(&mut writer, &hello).await.map_err(|e| format!("cannot send hello: {e}"))?;

    let first = time::timeout(WELCOME_TIMEOUT, reader.next_json::<DaemonMessage>())
        .await
        .map_err(|_| "no welcome within the timeout".to_owned())?
        .map_err(|e| format!("bad first line from daemon: {e}"))?;
    match first {
        Some(DaemonMessage::Welcome(welcome)) => {
            if welcome.protocol != PROTOCOL_VERSION {
                return Err(format!("daemon speaks protocol {}, this plugin speaks {PROTOCOL_VERSION}", welcome.protocol));
            }
            Ok((reader, writer, welcome))
        }
        Some(DaemonMessage::Error(err)) => {
            error!(code = err.code.as_str(), "daemon refused the session: {}", err.message);
            Err(format!("{}: {}", err.code.as_str(), err.message))
        }
        Some(other) => Err(format!("expected welcome, got {other:?}")),
        None => Err("daemon closed the connection during the handshake".into()),
    }
}

/// Multiplex one live connection until it breaks. Returns the reason.
async fn serve(
    mut reader: Reader,
    mut write_half: OwnedWriteHalf,
    events: &mpsc::Sender<Event>,
    rx: &mut mpsc::Receiver<Outgoing>,
    cancel: &CancellationToken,
) -> String {
    let mut pending: HashMap<u64, oneshot::Sender<Result<CmdResult, DaemonError>>> = HashMap::new();
    let mut next_id: u64 = 1;

    let reason = loop {
        tokio::select! {
            _ = cancel.cancelled() => break "shutdown".to_owned(),
            line = reader.next_line() => match line {
                Ok(Some(line)) => match parse_daemon_line(&line) {
                    Ok(DaemonMessage::Event(event)) => {
                        debug!(room = %event.room_id, person = %event.person, "event");
                        if events.send(event).await.is_err() {
                            break "event consumer gone".to_owned();
                        }
                    }
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
                    // A newer daemon may send messages this plugin does not know (a new
                    // event kind, say); that is no reason to drop the connection.
                    Err(err) => warn!("ignoring a daemon message this plugin cannot parse: {err}"),
                },
                Ok(None) => break "daemon closed the connection".to_owned(),
                Err(err) => break format!("read error: {err}"),
            },
            out = rx.recv() => match out {
                Some(out) => {
                    let id = next_id;
                    next_id += 1;
                    if let Err(err) = send_cmd(&mut write_half, id, out.kind).await {
                        let _ = out.done.send(Err(DaemonError::Unavailable(format!("write failed: {err}"))));
                        break format!("write error: {err}");
                    }
                    pending.insert(id, out.done);
                }
                None => break "client handle dropped".to_owned(),
            },
        }
    };

    for (_, done) in pending.drain() {
        let _ = done.send(Err(DaemonError::Unavailable("connection lost".into())));
    }
    reason
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
        Backoff { current: BACKOFF_MIN }
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
