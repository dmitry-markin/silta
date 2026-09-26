//! `silta-claude`: the Claude Code channel plugin for the silta bridge.
//!
//! An MCP stdio server that connects to `siltad`'s Unix socket, announces its session
//! name, turns daemon events into `notifications/claude/channel`, and forwards the
//! `reply` tool to the daemon. It exits on stdin EOF, on SIGTERM/SIGINT, and when its
//! parent dies, so it never outlives its session.

mod daemon;
mod mcp;
mod transport;

use std::{path::PathBuf, process::ExitCode, time::Duration};

use clap::Parser;
use rmcp::ServiceExt;
use tokio::{
    signal::unix::{signal, SignalKind},
    sync::{mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

/// Claude Code channel plugin for the silta bridge. Reads its session name and the
/// daemon socket from the environment; holds no Matrix credentials.
#[derive(Parser, Debug)]
#[command(name = "silta-claude", version)]
struct Args {
    /// Session name announced to the daemon (which rooms this session owns is the
    /// daemon's decision).
    #[arg(long, env = "SILTA_SESSION")]
    session: String,

    /// Path of the daemon's Unix socket.
    #[arg(long, env = "SILTA_SOCKET", default_value = silta::config::DEFAULT_SOCKET)]
    socket: PathBuf,

    /// Directory attachments are written to, created if missing; relative to the
    /// working directory.
    #[arg(long, env = "SILTA_INBOX", default_value = "inbox")]
    inbox: PathBuf,

    /// A file the supervisor creates once Claude Code has registered the channel; when
    /// set, the plugin connects to the daemon only after it exists. Unset, it connects
    /// right after the MCP handshake.
    #[arg(long, env = "SILTA_READY_FILE")]
    ready_file: Option<PathBuf>,
}

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_target(false)
        .init();

    let args = match Args::try_parse() {
        Ok(args) => args,
        Err(err) => {
            let _ = err.print();
            if err.kind() == clap::error::ErrorKind::MissingRequiredArgument {
                eprintln!("silta-claude: set SILTA_SESSION (and optionally SILTA_SOCKET) in the session's environment");
            }
            return ExitCode::from(2);
        }
    };

    // Die with the parent: SIGTERM when it exits, plus a poll in case the parent was
    // already gone before prctl took effect or the signal was lost.
    let parent = unsafe { libc::getppid() };
    unsafe {
        libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
    }
    if unsafe { libc::getppid() } != parent {
        error!("parent process is gone, exiting");
        return ExitCode::FAILURE;
    }

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(err) => {
            error!("cannot start runtime: {err}");
            return ExitCode::FAILURE;
        }
    };
    let code = runtime.block_on(run(args, parent));
    // Do not wait for the blocking stdin reader; the process is done.
    std::process::exit(code);
}

async fn run(args: Args, parent: libc::pid_t) -> i32 {
    let cancel = CancellationToken::new();

    tokio::spawn({
        let cancel = cancel.clone();
        async move {
            let mut ticks = tokio::time::interval(Duration::from_secs(5));
            loop {
                ticks.tick().await;
                if unsafe { libc::getppid() } != parent {
                    warn!("parent process is gone, exiting");
                    cancel.cancel();
                    return;
                }
            }
        }
    });

    tokio::spawn({
        let cancel = cancel.clone();
        async move {
            let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
            let mut int = signal(SignalKind::interrupt()).expect("SIGINT handler");
            tokio::select! {
                _ = term.recv() => info!("SIGTERM received"),
                _ = int.recv() => info!("SIGINT received"),
            }
            cancel.cancel();
        }
    });

    let inbox = std::env::current_dir()
        .map(|cwd| cwd.join(&args.inbox))
        .unwrap_or(args.inbox);
    if let Err(err) = std::fs::create_dir_all(&inbox) {
        error!("cannot create the inbox {}: {err}", inbox.display());
        return 1;
    }
    info!(session = %args.session, socket = %args.socket.display(), inbox = %inbox.display(), "silta-claude {} starting", env!("CARGO_PKG_VERSION"));
    let (events_tx, events_rx) = mpsc::channel(256);
    let (ready_tx, ready_rx) = oneshot::channel();
    let daemon = daemon::DaemonClient::start(
        args.socket,
        args.session,
        inbox,
        args.ready_file,
        events_tx,
        ready_rx,
        cancel.clone(),
    );
    let handler = mcp::SiltaChannel::new(daemon, events_rx, ready_tx);

    // The handshake waits for the client's initialize request; a shutdown signal must
    // end that wait too, not only the serving phase.
    let service = tokio::select! {
        result = handler.serve(transport::stdio()) => match result {
            Ok(service) => service,
            Err(err) => {
                error!("MCP initialization failed: {err}");
                cancel.cancel();
                return 1;
            }
        },
        _ = cancel.cancelled() => {
            info!("shutdown requested during the MCP handshake, exiting");
            return 0;
        }
    };
    let service_cancel = service.cancellation_token();

    let code = tokio::select! {
        result = service.waiting() => {
            match result {
                Ok(reason) => info!("MCP transport closed ({reason:?}), exiting"),
                Err(err) => error!("MCP service task failed: {err}"),
            }
            0
        }
        _ = cancel.cancelled() => {
            info!("shutdown requested, exiting");
            service_cancel.cancel();
            0
        }
    };
    cancel.cancel();
    code
}
