//! `siltad`: owns the bot's Matrix device and routes rooms to assistant sessions.

use std::{collections::HashMap, path::PathBuf, process::ExitCode, sync::Arc};

use anyhow::Context;
use clap::{Parser, Subcommand};
use silta::config::{Config, Routing};
use siltad::{
    alert, backup,
    daemon::{now_ms, Daemon, Settings},
    matrix, server, session,
    spool::{self, Spool},
};
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

// The default filter, unless `RUST_LOG` says otherwise. `matrix_sdk_crypto::backups`
// is at `error` because the SDK's backup upload task warns "no backup key was found"
// at every new room key while server-side key backups are off, which they are on
// purpose (the store is backed up as files instead).
const DEFAULT_LOG_FILTER: &str = "info,\
     matrix_sdk=warn,\
     matrix_sdk_base=warn,\
     matrix_sdk_crypto=warn,\
     matrix_sdk_crypto::backups=error,\
     matrix_sdk::http_client=off";

/// silta daemon: one Matrix device for the family assistant, one Unix socket for its
/// sessions.
#[derive(Parser, Debug)]
#[command(name = "siltad", version = env!("SILTA_VERSION"))]
struct Args {
    /// Configuration file, or `-` for standard input (to run a command as the daemon's
    /// user, who cannot read the root-owned file).
    #[arg(long, default_value = "/etc/silta/siltad.toml")]
    config: PathBuf,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Copy the store consistently into a dated directory under --to (the daemon may
    /// be running) and remove the oldest copies beyond --keep.
    Backup {
        #[arg(long)]
        to: PathBuf,
        #[arg(long, default_value_t = 7)]
        keep: usize,
    },
    /// Sign this device with a cross-signing identity whose keys siltad holds, replacing
    /// the account's identity if needed (stop the daemon first). On matrix.org and other OAuth 2.0
    /// homeservers, approve at the printed URL within two minutes.
    ResetCrossSigning,
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_FILTER)),
        )
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_target(false)
        .init();

    match run(Args::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            error!("{err:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> anyhow::Result<()> {
    let config = Config::load(&args.config)?;
    match args.command {
        Some(Command::Backup { to, keep }) => {
            backup::run(&config.state_dir, &to, keep, &backup::stamp(now_ms()))?;
            return Ok(());
        }
        Some(Command::ResetCrossSigning) => return reset_cross_signing(&config).await,
        None => {}
    }
    for warning in config.warnings() {
        warn!("{warning}");
    }
    let routing = Routing::new(&config);
    let users = resolve_users(&routing)?;
    info!(
        "siltad {} starting: {} people, {} sessions ({}), replay window {} s, attachments up to {} MB, inbox files kept {} days, owner alerts after {} s, socket {}",
        env!("SILTA_VERSION"),
        config.people.len(),
        config.sessions.len(),
        routing.session_users().map(|(s, u)| format!("{s} as {u}")).collect::<Vec<_>>().join(", "),
        config.replay_window_secs,
        config.attachment_max_mb,
        config.inbox_max_age_days,
        config.alert_grace_secs,
        config.socket.display()
    );
    let state_dir = session::prepare_state_dir(&config.state_dir)?;
    let client = session::build_client(&config.matrix.homeserver_url, &state_dir).await?;
    session::login_or_restore(&client, &state_dir, &credentials(&config)).await?;
    tokio::spawn({
        let client = client.clone();
        async move { session::report_cross_signing(&client).await }
    });
    if let Some(name) = &config.matrix.display_name {
        session::ensure_display_name(&client, name).await;
    }

    let spool = Spool::new(
        &config.state_dir,
        config.attachment_max_mb.saturating_mul(1024 * 1024),
    );
    spool.prepare()?;
    let (silence_tx, silence_rx) = tokio::sync::mpsc::unbounded_channel();
    let settings = Settings {
        replay_window_secs: config.replay_window_secs,
        inbox_max_age_days: config.inbox_max_age_days,
        alert_grace_secs: config.alert_grace_secs,
    };
    let daemon = Arc::new(Daemon::new(
        client,
        routing,
        &config.state_dir,
        spool,
        users,
        settings,
        silence_tx,
    ));
    matrix::register_handlers(&daemon);

    let cancel = CancellationToken::new();
    spool::spawn_sweeper(daemon.spool.clone(), cancel.clone());
    alert::spawn_watcher(daemon.clone(), silence_rx, cancel.clone());
    tokio::spawn({
        let cancel = cancel.clone();
        async move {
            let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
            let mut int = signal(SignalKind::interrupt()).expect("SIGINT handler");
            tokio::select! {
                _ = term.recv() => info!("SIGTERM received, shutting down"),
                _ = int.recv() => info!("SIGINT received, shutting down"),
            }
            cancel.cancel();
        }
    });

    // Either task ending ends the daemon: a socket that cannot be bound is as fatal as a
    // rejected token, and must not leave a syncing daemon that no session can reach.
    let server = tokio::spawn({
        let daemon = daemon.clone();
        let socket = config.socket.clone();
        let cancel = cancel.clone();
        async move {
            let result = server::run(daemon, socket, cancel.clone()).await;
            cancel.cancel();
            result
        }
    });
    let sync = matrix::sync_loop(daemon.clone(), cancel.clone()).await;
    cancel.cancel();
    server.await.context("socket server task failed")??;
    sync?;
    info!("stopped");
    Ok(())
}

fn credentials(config: &Config) -> session::Credentials<'_> {
    session::Credentials {
        homeserver_url: &config.matrix.homeserver_url,
        user_id: &config.matrix.user_id,
        password: &config.matrix.password,
        device_id: &config.matrix.device_id,
        device_name: &config.matrix.device_name,
    }
}

/// `siltad reset-cross-signing`, on the daemon's saved session: before the first login
/// it would sign a device the daemon never used.
async fn reset_cross_signing(config: &Config) -> anyhow::Result<()> {
    // Before preparing the directory, which would create it as whoever runs this.
    let session_file = config.state_dir.join(session::SESSION_FILE);
    if !session_file
        .try_exists()
        .with_context(|| format!("cannot read {}", session_file.display()))?
    {
        anyhow::bail!(
            "no saved session in {}; start siltad once before resetting",
            config.state_dir.display()
        );
    }
    let state_dir = session::prepare_state_dir(&config.state_dir)?;
    let client = session::build_client(&config.matrix.homeserver_url, &state_dir).await?;
    session::login_or_restore(&client, &state_dir, &credentials(config)).await?;
    session::reset_cross_signing(&client, &config.matrix.password).await
}

/// The uid behind each session's `user`, looked up once: a session whose user does not
/// exist on this host cannot be checked at `hello`, so the daemon does not start.
fn resolve_users(routing: &Routing) -> anyhow::Result<HashMap<String, u32>> {
    let mut users = HashMap::new();
    for (session, user) in routing.session_users() {
        let entry = nix::unistd::User::from_name(user)
            .with_context(|| format!("cannot look up user {user:?} of session {session:?}"))?
            .with_context(|| {
                format!(
                    "session {session:?} runs as user {user:?}, which does not exist on this host"
                )
            })?;
        users.insert(session.to_owned(), entry.uid.as_raw());
    }
    Ok(users)
}
