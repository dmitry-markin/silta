//! `silta-session`: the launcher and supervisor of one Silta session, run by
//! `silta-session@<name>.service`.
//!
//! It runs Claude Code headless in stream-json mode, owns its stdin and stdout, sends
//! the host line that starts the first turn, reduces every output line to a journal
//! line (the conversation's texts never reach the journal), resumes the saved session
//! id across restarts as long as its transcript exists, and rotates the session into a
//! fresh one when its context has grown and it is idle, or when Claude Code wanted to
//! compact on its own (`docs/session-rotation-design.md`). Closing stdin is the
//! graceful stop; a session that does not exit within the grace is killed.

pub mod rotation;
pub mod state;
pub mod stream;

use std::{
    os::unix::process::ExitStatusExt,
    path::PathBuf,
    process::Stdio,
    time::{Duration, Instant},
};

use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, Command},
    sync::mpsc,
    time::{interval, timeout, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

use rotation::{Action, Limits, Reason, Tracker, HANDOFF_ATTEMPTS};
use state::{HandoffWatch, Next, Paths, HANDOFF_NOTE};

/// The channel plugin, last on the command line because `--channels` is variadic.
pub const CHANNEL: &str = "plugin:silta-claude@silta-local";

/// The stderr line of a `--resume` whose transcript Claude Code does not know.
const NO_CONVERSATION: &str = "No conversation found with session ID";

#[derive(Debug, Clone)]
pub struct Config {
    pub session: String,
    /// The state directory, the session's `HOME`.
    pub state: PathBuf,
    pub persona: PathBuf,
    pub claude_bin: PathBuf,
    /// The channel plugin's binary, `SILTA_CLAUDE_BIN` for the plugin's `.mcp.json`.
    pub plugin_bin: PathBuf,
    pub socket: PathBuf,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub fallback_model: Option<String>,
    pub oauth_token: Option<String>,
    /// How long to wait after closing stdin before killing claude.
    pub stop_grace: Duration,
    pub limits: Limits,
    pub backups_keep: usize,
}

/// Which host line starts the session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Fresh,
    Resumed,
    AfterRotation { handoff: bool },
    /// Resumed after a cut turn: the start line is the handoff request.
    Retry,
}

#[derive(Debug)]
struct Start {
    id: String,
    resume: bool,
    kind: Kind,
    /// Failed handoff turns of the pending rotation so far.
    failures: u32,
}

enum Outcome {
    /// The supervisor exits with this code; systemd decides about a restart.
    Exit(i32),
    /// Start claude again at once (a rotation step), with a pause before the next
    /// handoff attempt if the last one failed.
    Again { not_before: Option<Instant> },
}

/// Runs sessions until claude exits on its own or `shutdown` is cancelled; the exit
/// code for the unit. Every way out is one journal line naming the code and the
/// reason, so that an exit the unit's restart would otherwise hide can be read back.
pub async fn run(cfg: &Config, shutdown: CancellationToken) -> i32 {
    let (code, reason) = supervise(cfg, &shutdown).await;
    eprintln!("supervisor exiting with code {code}: {reason}");
    code
}

async fn supervise(cfg: &Config, shutdown: &CancellationToken) -> (i32, String) {
    let paths = Paths::new(&cfg.state);
    let workspace = paths.workspace();
    for dir in [workspace.join("inbox"), workspace.join("out")] {
        if let Err(err) = std::fs::create_dir_all(&dir) {
            return (1, format!("cannot create {}: {err}", dir.display()));
        }
    }
    paths.prune_cache();
    let mut not_before = None;
    loop {
        if shutdown.is_cancelled() {
            return (0, "stopped between two runs of claude".to_owned());
        }
        let start = match plan_start(cfg, &paths) {
            Ok(start) => start,
            Err(err) => return (1, format!("cannot prepare the session: {err}")),
        };
        match run_once(cfg, &paths, &start, not_before, shutdown).await {
            Outcome::Exit(code) if shutdown.is_cancelled() => return (code, "stopped".to_owned()),
            Outcome::Exit(code) => return (code, format!("claude exited on its own with {code}")),
            Outcome::Again { not_before: pause } => not_before = pause,
        }
    }
}

/// Decides between resuming the saved session and starting a new one, from the saved id,
/// its transcript, and the step a rotation left behind.
fn plan_start(cfg: &Config, paths: &Paths) -> std::io::Result<Start> {
    let saved = paths.read_id();
    let resumable = saved.clone().filter(|id| paths.transcript(id).is_some());
    let fresh = |paths: &Paths, kind: Kind| -> std::io::Result<Start> {
        let id = state::new_id()?;
        paths.write_id(&id)?;
        Ok(Start { id, resume: false, kind, failures: 0 })
    };
    let rotated = |paths: &Paths, handoff: bool| -> std::io::Result<Start> {
        match paths.backup_memory(cfg.backups_keep) {
            Ok(0) => eprintln!("rotation: no memory directory to back up"),
            Ok(n) => eprintln!("rotation: memory backed up ({n} files)"),
            Err(err) => eprintln!("rotation: memory backup failed: {err}"),
        }
        let start = fresh(paths, Kind::AfterRotation { handoff })?;
        paths.clear_rotation();
        eprintln!(
            "rotation: session {} rotated out{}; new session {}",
            saved.as_deref().unwrap_or("?"),
            if handoff { "" } else { " without a finished handoff" },
            start.id
        );
        Ok(start)
    };
    let next = paths.read_next();
    match next {
        Next::Fresh { handoff } => rotated(paths, handoff),
        Next::Retry => match resumable {
            Some(id) => {
                eprintln!("resuming session {id} for the handoff");
                Ok(Start { id, resume: true, kind: Kind::Retry, failures: 0 })
            }
            None => {
                eprintln!("rotation: the session cannot be resumed for its handoff");
                rotated(paths, false)
            }
        },
        Next::Normal | Next::Postponed { .. } => match resumable {
            Some(id) => {
                eprintln!("resuming session {id}");
                let failures = match next {
                    Next::Postponed { failures } if paths.marker_exists() => failures,
                    _ => {
                        // A postponed rotation whose marker is gone was cancelled by hand.
                        let _ = paths.write_next(Next::Normal);
                        0
                    }
                };
                Ok(Start { id, resume: true, kind: Kind::Resumed, failures })
            }
            None => {
                if let Some(id) = &saved {
                    eprintln!("session {id} has no transcript under {}/.claude/projects; starting a new one", cfg.state.display());
                }
                // Whatever rotation was pending concerned the lost session.
                paths.clear_rotation();
                let start = fresh(paths, Kind::Fresh)?;
                eprintln!("new session {}", start.id);
                Ok(start)
            }
        },
    }
}

enum Msg {
    Out(String),
    Err(String),
}

/// One run of claude, from spawn to exit.
async fn run_once(cfg: &Config, paths: &Paths, start: &Start, not_before: Option<Instant>, shutdown: &CancellationToken) -> Outcome {
    let workspace = paths.workspace();
    let mut cmd = Command::new(&cfg.claude_bin);
    cmd.args(["-p", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose", "--permission-mode", "auto", "--permission-prompts", "none"]);
    cmd.arg("--append-system-prompt-file").arg(&cfg.persona);
    if let Some(model) = &cfg.model {
        cmd.arg("--model").arg(model);
    }
    if let Some(effort) = &cfg.effort {
        cmd.arg("--effort").arg(effort);
    }
    if let Some(fallback) = &cfg.fallback_model {
        cmd.arg("--fallback-model").arg(fallback);
    }
    if start.resume {
        cmd.arg("--resume").arg(&start.id);
    } else {
        cmd.arg("--session-id").arg(&start.id).arg("--name").arg(format!("silta-{}", cfg.session));
    }
    cmd.arg("--channels").arg(CHANNEL);
    cmd.current_dir(&workspace)
        .env("HOME", &cfg.state)
        .env("SILTA_CLAUDE_BIN", &cfg.plugin_bin)
        .env("SILTA_SOCKET", &cfg.socket)
        .env("SILTA_INBOX", workspace.join("inbox"))
        .env("DISABLE_AUTOUPDATER", "1");
    if let Some(token) = &cfg.oauth_token {
        cmd.env("CLAUDE_CODE_OAUTH_TOKEN", token);
    }
    cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true);
    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(err) => {
            eprintln!("cannot start {}: {err}", cfg.claude_bin.display());
            return Outcome::Exit(1);
        }
    };
    let (tx, mut rx) = mpsc::channel::<Msg>(256);
    if let Some(out) = child.stdout.take() {
        tokio::spawn(pump(out, tx.clone(), Msg::Out));
    }
    if let Some(err) = child.stderr.take() {
        tokio::spawn(pump(err, tx, Msg::Err));
    }

    let now = Instant::now();
    let mut run = Run {
        cfg,
        paths,
        stdin: child.stdin.take(),
        stopping: None,
        after: None,
        tracker: Tracker::new(cfg.limits.clone(), now),
        watch: None,
    };
    if start.kind == Kind::Retry {
        run.watch = Some(paths.watch_handoff());
    }
    run.send(&intro(&cfg.session, start.kind)).await;
    match start.kind {
        Kind::Retry => run.tracker = run.tracker.retrying(now),
        _ if cfg.limits.enabled && paths.marker_exists() => {
            run.tracker = run.tracker.pending(now, not_before, start.failures);
            let pause = not_before.map(|t| t.saturating_duration_since(now).as_secs()).unwrap_or(0);
            eprintln!(
                "rotation: pending from the start; rotating at the next quiet moment{}{}",
                if pause > 0 { format!(" after {pause} s") } else { String::new() },
                if start.failures > 0 { format!(" ({} of {HANDOFF_ATTEMPTS} handoff turns failed)", start.failures) } else { String::new() }
            );
        }
        _ => {}
    }

    let mut readers_open = true;
    let mut no_conversation = false;
    let mut ticks: u64 = 0;
    let mut tick = interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut killed = false;
    let status = loop {
        tokio::select! {
            _ = shutdown.cancelled(), if !matches!(run.after, Some(Outcome::Exit(_))) => {
                eprintln!("stopping: closing claude's stdin");
                run.after = Some(Outcome::Exit(0));
                run.begin_stop();
            }
            msg = rx.recv(), if readers_open => match msg {
                Some(Msg::Out(line)) => {
                    let (summary, event) = stream::read(&line);
                    println!("{summary}");
                    if run.stopping.is_none() {
                        if matches!(event, stream::Event::Result { .. }) && run.tracker.awaiting_handoff() {
                            let written = run.watch.as_ref().is_some_and(|w| paths.handoff_written(w));
                            run.tracker.set_note_written(written);
                        }
                        let actions = run.tracker.event(&event, Instant::now());
                        run.act(actions).await;
                    }
                }
                Some(Msg::Err(line)) => {
                    eprintln!("{line}");
                    if line.contains(NO_CONVERSATION) {
                        no_conversation = true;
                    }
                }
                None => readers_open = false,
            },
            _ = tick.tick() => {
                match child.try_wait() {
                    Ok(Some(status)) => break status,
                    Ok(None) => {}
                    Err(err) => {
                        eprintln!("cannot wait for claude: {err}");
                        break wait_or_kill(&mut child).await;
                    }
                }
                let now = Instant::now();
                if let Some(deadline) = run.stopping {
                    if now >= deadline && !killed {
                        eprintln!("claude did not exit on stdin EOF within {} s, killing it", cfg.stop_grace.as_secs());
                        let _ = child.kill().await;
                        killed = true;
                    }
                } else {
                    ticks += 1;
                    if cfg.limits.enabled && ticks.is_multiple_of(5) {
                        let marker = paths.marker_exists();
                        if marker && !run.tracker.is_pending() {
                            eprintln!("rotation: marker found; rotating at the next quiet moment");
                            let actions = run.tracker.marker_seen(now);
                            run.act(actions).await;
                        } else if !marker && run.tracker.cancel() {
                            eprintln!("rotation: marker removed; the pending rotation is cancelled");
                            let _ = paths.write_next(Next::Normal);
                        }
                    }
                    let actions = run.tracker.tick(now);
                    run.act(actions).await;
                }
            }
        }
    };
    // What the readers still hold after the exit.
    while readers_open {
        match timeout(Duration::from_millis(500), rx.recv()).await {
            Ok(Some(Msg::Out(line))) => println!("{}", stream::read(&line).0),
            Ok(Some(Msg::Err(line))) => {
                eprintln!("{line}");
                if line.contains(NO_CONVERSATION) {
                    no_conversation = true;
                }
            }
            Ok(None) | Err(_) => readers_open = false,
        }
    }
    let code = status.code().unwrap_or_else(|| 128 + status.signal().unwrap_or(0));
    if code != 0 {
        eprintln!("claude exited with {code}");
    }
    match run.after {
        Some(outcome) => outcome,
        None => {
            // Only a saved id that no longer resumes is dropped, so that a start that
            // fails for any other reason (an expired token, the network) keeps the
            // conversation.
            if start.resume && no_conversation {
                eprintln!("session {} cannot be resumed; a new session starts on the next run", start.id);
                paths.drop_id();
            }
            Outcome::Exit(code)
        }
    }
}

async fn wait_or_kill(child: &mut Child) -> std::process::ExitStatus {
    let _ = child.kill().await;
    child.wait().await.unwrap_or_else(|_| std::process::ExitStatus::from_raw(1 << 8))
}

/// The mutable side of one run: claude's stdin, the stop in progress, and the tracker.
struct Run<'a> {
    cfg: &'a Config,
    paths: &'a Paths,
    stdin: Option<ChildStdin>,
    stopping: Option<Instant>,
    after: Option<Outcome>,
    tracker: Tracker,
    /// What the handoff request found on disk, while the handoff turn is awaited.
    watch: Option<HandoffWatch>,
}

impl Run<'_> {
    async fn send(&mut self, text: &str) {
        let Some(stdin) = self.stdin.as_mut() else { return };
        let line = serde_json::json!({"type": "user", "message": {"role": "user", "content": text}});
        let mut bytes = line.to_string().into_bytes();
        bytes.push(b'\n');
        if let Err(err) = async { stdin.write_all(&bytes).await?; stdin.flush().await }.await {
            eprintln!("cannot write to claude's stdin: {err}");
        }
    }

    /// The graceful stop: EOF on stdin, the grace, then the kill from the tick.
    fn begin_stop(&mut self) {
        self.stdin = None;
        if self.stopping.is_none() {
            self.stopping = Some(Instant::now() + self.cfg.stop_grace);
        }
    }

    async fn act(&mut self, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::MarkPending => {
                    let why = format!("threshold: context {} tokens, idle {} s", self.tracker.context(), self.cfg.limits.idle.as_secs());
                    if let Err(err) = self.paths.write_marker(&why) {
                        eprintln!("rotation: cannot write the marker: {err}");
                    }
                    eprintln!("rotation: {why}; rotating at the next quiet moment");
                }
                Action::RequestHandoff => {
                    let watch = self.paths.watch_handoff();
                    eprintln!(
                        "rotation: requesting the handoff (context {} tokens); waiting for {}",
                        self.tracker.context(),
                        if watch.note_existed() { format!("{HANDOFF_NOTE} to be rewritten") } else { format!("a memory write, there is no {HANDOFF_NOTE} yet") }
                    );
                    self.watch = Some(watch);
                    self.send(handoff()).await;
                }
                Action::AwaitHandoff => {
                    eprintln!("rotation: a turn ended without the handoff note written; still waiting for the handoff turn");
                }
                Action::Rotate => {
                    eprintln!("rotation: handoff written; stopping the session for a fresh start");
                    self.finish(true);
                }
                Action::GiveUp { reason } => {
                    let why = match reason {
                        Reason::CutTwice => "the turn did not end within the cap again".to_owned(),
                        Reason::PromptTooLong => "the handoff turn failed because the prompt exceeds the model's window, which no retry can fix".to_owned(),
                        Reason::Failures => format!("{HANDOFF_ATTEMPTS} handoff turns failed"),
                    };
                    eprintln!("rotation: {why}; giving the handoff up and stopping the session for a fresh start");
                    self.finish(false);
                }
                Action::Retry => {
                    let agents = self.tracker.agents();
                    let outstanding = if agents.is_empty() { String::new() } else { format!(" (agents outstanding: {})", agents.iter().cloned().collect::<Vec<_>>().join(", ")) };
                    eprintln!("rotation: the turn did not end within the cap{outstanding}; cutting it and resuming once for the handoff");
                    if let Err(err) = self.paths.write_next(Next::Retry) {
                        eprintln!("rotation: cannot record the next step: {err}");
                    }
                    self.after = Some(Outcome::Again { not_before: None });
                    self.begin_stop();
                }
                Action::Postpone { failures } => {
                    let pause = self.cfg.limits.retry_pause;
                    eprintln!("rotation: the handoff turn failed ({failures} of {HANDOFF_ATTEMPTS}); resuming the session and retrying in {} s", pause.as_secs());
                    if let Err(err) = self.paths.write_next(Next::Postponed { failures }) {
                        eprintln!("rotation: cannot record the next step: {err}");
                    }
                    self.after = Some(Outcome::Again { not_before: Some(Instant::now() + pause) });
                    self.begin_stop();
                }
            }
        }
    }

    /// The old session is done: record the fresh start and stop claude.
    fn finish(&mut self, handoff: bool) {
        if let Err(err) = self.paths.write_next(Next::Fresh { handoff }) {
            eprintln!("rotation: cannot record the next step: {err}");
        }
        self.after = Some(Outcome::Again { not_before: None });
        self.begin_stop();
    }
}

/// Lines of one of claude's output pipes, into the channel; ends at EOF.
async fn pump<R: AsyncRead + Unpin>(reader: R, tx: mpsc::Sender<Msg>, wrap: fn(String) -> Msg) {
    let mut reader = BufReader::new(reader);
    let mut buf = Vec::new();
    loop {
        buf.clear();
        match reader.read_until(b'\n', &mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(_) => {
                while buf.last().is_some_and(|b| *b == b'\n' || *b == b'\r') {
                    buf.pop();
                }
                if tx.send(wrap(String::from_utf8_lossy(&buf).into_owned())).await.is_err() {
                    break;
                }
            }
        }
    }
}

/// `YYYY-MM-DD HH:MM` in UTC, the form of the host lines.
fn stamp() -> String {
    let s = silta::time::rfc3339_utc(state::unix_millis());
    format!("{} {}", &s[..10], &s[11..16])
}

/// The host line that starts the first turn. Without one, no channel event is ever
/// delivered. It is sent on every start, so a restart also seeds a turn in
/// which the mind can notice a tool that broke while it was down. On a resume it lands
/// in the middle of the existing conversation, which is why it names the host as its
/// sender and says that nothing is waiting.
fn intro(session: &str, kind: Kind) -> String {
    let now = stamp();
    let channel = "You are connected to the family chat through the silta channel.";
    match kind {
        Kind::Fresh => format!(
            "Session {session} started at {now} UTC: a new conversation, no history. {channel} This line comes from the host, not from a person, and no one is waiting on you yet, so use this turn to look around the workspace and check that your tools work. Bash commands run in a sandbox, so some paths and hosts are refused by design and that is not a fault. Then end the turn and stay idle until a message arrives."
        ),
        Kind::Resumed => format!(
            "Session {session} restarted at {now} UTC and resumed its history. {channel} This line comes from the host at every start, not from a person. If there is no work to do from before the session restart, end this turn and stay idle until a message arrives."
        ),
        Kind::AfterRotation { handoff: true } => format!(
            "Session {session} started at {now} UTC after a rotation: a new conversation, and the previous session's handoff is in memory. {channel} This line comes from the host, not from a person. Read the memory note `handoff` first and act on what it says is pending, then rewrite it to say that nothing is pending. Then end the turn and stay idle until a message arrives."
        ),
        Kind::AfterRotation { handoff: false } => format!(
            "Session {session} started at {now} UTC after a rotation: a new conversation. The previous session could not finish its handoff, so the handoff note in memory may be stale. {channel} This line comes from the host, not from a person. Read the handoff note, then look for dangling work as after a restart: unanswered messages in the room, a promised step, an agent worth rerunning. Say what may have been interrupted, rewrite the handoff note to say that nothing is pending, and stay idle until a message arrives."
        ),
        Kind::Retry => format!(
            "Session {session} restarted at {now} UTC and resumed its history. Its last turn was cut by the host because it ran too long, and the session is about to be rotated. This line comes from the host, not from a person. Write your handoff now into the memory note `handoff` (the file handoff.md in your memory directory; the host waits for that file to change and takes the turn that rewrites it as your handoff turn): the task in progress and its state, questions waiting on the person, promises made, background agents worth resuming. Do not resume the work; end your turn as soon as the note is written."
        ),
    }
}

/// The host line that asks for the handoff at the rotation's quiet moment (design
/// section 6). It names the note's file and says why: the supervisor takes the turn that
/// rewrites it as the handoff turn.
pub fn handoff() -> &'static str {
    "Write your handoff now: the session is about to be rotated. This line comes from the host, not from a person. Rewrite the memory note `handoff` (the file handoff.md in your memory directory) even if nothing is pending: the host waits for that file to change and takes the turn that rewrites it as your handoff turn. End your turn when it is written."
}
