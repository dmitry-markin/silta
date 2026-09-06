//! `siltad`: owns the bot's Matrix device and routes rooms to assistant sessions.

use std::{path::PathBuf, process::ExitCode, sync::Arc, time::Duration};

use anyhow::Context;
use clap::Parser;
use silta::config::{Config, Routing};
use siltad::{daemon::Daemon, inbox, matrix, server, session};
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

/// silta daemon: one Matrix device for the family assistant, one Unix socket for its
/// sessions.
#[derive(Parser, Debug)]
#[command(name = "siltad", version)]
struct Args {
    /// Configuration file.
    #[arg(long, default_value = "/etc/silta/siltad.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,matrix_sdk=warn,matrix_sdk_base=warn,matrix_sdk_crypto=warn,matrix_sdk::http_client=off")),
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
    for warning in config.warnings() {
        warn!("{warning}");
    }
    info!(
        "siltad {} starting: {} people, {} sessions, replay window {} s, inbox files up to {} MB kept {} days, socket {}",
        env!("CARGO_PKG_VERSION"),
        config.people.len(),
        config.sessions.len(),
        config.replay_window_secs,
        config.inbox_max_file_mb,
        config.inbox_max_age_days,
        config.socket.display()
    );
    let inbox_config = inbox::InboxConfig {
        dir: config.state_dir.join("inbox"),
        max_age: Duration::from_secs(config.inbox_max_age_days.saturating_mul(86_400)),
        max_bytes: config.inbox_max_file_mb.saturating_mul(1024 * 1024),
    };

    let client = session::build_client(&config.matrix.homeserver_url, &config.state_dir).await?;
    session::login_or_restore(
        &client,
        &config.state_dir,
        &session::Credentials {
            homeserver_url: &config.matrix.homeserver_url,
            user_id: &config.matrix.user_id,
            password: &config.matrix.password,
            device_id: &config.matrix.device_id,
            device_name: &config.matrix.device_name,
        },
    )
    .await?;

    inbox::prepare(&inbox_config.dir)?;
    let daemon = Arc::new(Daemon::new(client, Routing::new(&config), &config.state_dir, config.replay_window_secs, inbox_config));
    matrix::register_handlers(&daemon);

    let cancel = CancellationToken::new();
    inbox::spawn_sweeper(daemon.inbox.dir.clone(), daemon.inbox.max_age, cancel.clone());
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
