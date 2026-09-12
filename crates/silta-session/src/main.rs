//! The entry point: the configuration from the unit's environment, the signals, the
//! runtime. Everything else is in the library.

use std::{path::PathBuf, process::ExitCode, time::Duration};

use clap::Parser;
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;

use silta_session::{rotation::Limits, Config};

/// Runs one Silta session headless for silta-session@<name>.service.
#[derive(Parser, Debug)]
#[command(name = "silta-session", version)]
struct Args {
    /// The session name, the unit's instance name.
    #[arg(long, env = "SILTA_SESSION")]
    session: String,

    /// The state directory of the session: its HOME, with the workspace below it.
    #[arg(long, env = "HOME")]
    state: PathBuf,

    /// The persona appended to the system prompt.
    #[arg(long, env = "SILTA_PERSONA", default_value = "/etc/silta/persona.md")]
    persona: PathBuf,

    /// Claude Code; a versions/<n> path pins one.
    #[arg(long, env = "CLAUDE_BIN", default_value = "/opt/claude/.local/bin/claude")]
    claude_bin: PathBuf,

    /// The channel plugin's binary, for the plugin's .mcp.json.
    #[arg(long, env = "SILTA_CLAUDE_BIN", default_value = "/usr/bin/silta-claude")]
    plugin_bin: PathBuf,

    /// The daemon's socket, for the plugin.
    #[arg(long, env = "SILTA_SOCKET", default_value = silta::config::DEFAULT_SOCKET)]
    socket: PathBuf,

    #[arg(long, env = "SILTA_MODEL")]
    model: Option<String>,

    #[arg(long, env = "SILTA_EFFORT")]
    effort: Option<String>,

    #[arg(long, env = "SILTA_FALLBACK_MODEL")]
    fallback_model: Option<String>,

    /// Seconds to wait after closing stdin before killing claude; below the unit's
    /// TimeoutStopSec, so that the supervisor and not systemd ends the session.
    #[arg(long, env = "SILTA_STOP_GRACE", default_value_t = 120)]
    stop_grace: u64,

    /// Rotate the session (0/1).
    #[arg(long, env = "SILTA_ROTATE", default_value = "1", value_parser = parse_switch)]
    rotate: bool,

    /// Context size at or above which an idle session is rotated.
    #[arg(long, env = "SILTA_ROTATE_CONTEXT_TOKENS", default_value_t = 300_000)]
    rotate_context_tokens: u64,

    /// Seconds without a message from a person (a channel delivery; timer wakeups and
    /// the mind's own work do not count) before a large session counts as idle.
    #[arg(long, env = "SILTA_ROTATE_IDLE_SECONDS", default_value_t = 14_400)]
    rotate_idle_seconds: u64,

    /// Cap in seconds on the wait for a quiet moment once a rotation is pending.
    #[arg(long, env = "SILTA_ROTATE_QUIET_SECONDS", default_value_t = 1_800)]
    rotate_quiet_seconds: u64,

    /// Cap in seconds on the handoff turn.
    #[arg(long, env = "SILTA_ROTATE_HANDOFF_SECONDS", default_value_t = 900)]
    rotate_handoff_seconds: u64,

    /// Cap in seconds on the compaction that follows the handoff turn.
    #[arg(long, env = "SILTA_ROTATE_COMPACT_SECONDS", default_value_t = 2_700)]
    rotate_compact_seconds: u64,

    /// Seconds before the next attempt after a handoff turn that failed.
    #[arg(long, env = "SILTA_ROTATE_RETRY_SECONDS", default_value_t = 900)]
    rotate_retry_seconds: u64,

    /// Rotations whose snapshots (memory and transcript, two per rotation) are kept
    /// under the state directory's backups/.
    #[arg(long, env = "SILTA_BACKUPS_KEEP", default_value_t = 10)]
    backups_keep: usize,
}

fn parse_switch(s: &str) -> Result<bool, String> {
    match s.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(format!("expected 0 or 1, got {other:?}")),
    }
}

fn main() -> ExitCode {
    let args = match Args::try_parse() {
        Ok(args) => args,
        Err(err) => {
            let _ = err.print();
            return ExitCode::from(2);
        }
    };
    let oauth_token = std::env::var("CREDENTIALS_DIRECTORY")
        .ok()
        .and_then(|dir| std::fs::read_to_string(PathBuf::from(dir).join("oauth-token")).ok())
        .map(|t| t.split_whitespace().collect::<String>())
        .filter(|t| !t.is_empty());
    if oauth_token.is_none() {
        eprintln!("no oauth-token credential: the session cannot authenticate (see /etc/silta/oauth-token)");
    }
    let cfg = Config {
        session: args.session,
        state: args.state,
        persona: args.persona,
        claude_bin: args.claude_bin,
        plugin_bin: args.plugin_bin,
        socket: args.socket,
        model: args.model,
        effort: args.effort,
        fallback_model: args.fallback_model,
        oauth_token,
        stop_grace: Duration::from_secs(args.stop_grace),
        limits: Limits {
            enabled: args.rotate,
            context_tokens: args.rotate_context_tokens,
            idle: Duration::from_secs(args.rotate_idle_seconds),
            quiet: Duration::from_secs(args.rotate_quiet_seconds),
            handoff: Duration::from_secs(args.rotate_handoff_seconds),
            compact: Duration::from_secs(args.rotate_compact_seconds),
            retry_pause: Duration::from_secs(args.rotate_retry_seconds),
        },
        backups_keep: args.backups_keep,
    };

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("cannot start runtime: {err}");
            return ExitCode::FAILURE;
        }
    };
    let code = runtime.block_on(async {
        let shutdown = CancellationToken::new();
        let token = shutdown.clone();
        tokio::spawn(async move {
            let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
            let mut int = signal(SignalKind::interrupt()).expect("SIGINT handler");
            tokio::select! {
                _ = term.recv() => {}
                _ = int.recv() => {}
            }
            token.cancel();
        });
        silta_session::run(&cfg, shutdown).await
    });
    ExitCode::from(code.clamp(0, 255) as u8)
}
