//! The supervisor against a fake claude (`tests/fake-claude.sh`): starts, resumes, the
//! resume rule, the graceful stop, and the rotation paths with caps of a few seconds.

use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use silta_session::{rotation::Limits, Config, HANDOFF_LINE};
use tokio_util::sync::CancellationToken;

struct Fixture {
    home: PathBuf,
}

impl Fixture {
    fn new(name: &str) -> Self {
        let home = std::env::temp_dir().join(format!("silta-session-test-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&home);
        fs::create_dir_all(&home).unwrap();
        Self { home }
    }

    fn config(&self) -> Config {
        let tests = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
        Config {
            session: "test".into(),
            state: self.home.clone(),
            persona: tests.join("fake-claude.sh"),
            claude_bin: tests.join("fake-claude.sh"),
            plugin_bin: PathBuf::from("/nonexistent/silta-claude"),
            socket: PathBuf::from("/nonexistent/sock"),
            model: None,
            effort: None,
            fallback_model: None,
            oauth_token: None,
            stop_grace: Duration::from_secs(2),
            limits: Limits {
                enabled: true,
                context_tokens: 500,
                idle: Duration::from_secs(2),
                quiet: Duration::from_secs(3),
                handoff: Duration::from_secs(3),
                retry_pause: Duration::from_secs(3),
            },
            backups_keep: 3,
        }
    }

    fn log(&self) -> String {
        fs::read_to_string(self.home.join("fake.log")).unwrap_or_default()
    }

    fn set(&self, file: &str, content: &str) {
        fs::write(self.home.join(file), content).unwrap();
    }

    fn id(&self) -> String {
        fs::read_to_string(self.home.join("session-id")).unwrap().trim().to_owned()
    }

    /// Waits until the fake's log satisfies `pred`, up to `secs`.
    async fn until(&self, secs: u64, pred: impl Fn(&str) -> bool) -> String {
        let deadline = Instant::now() + Duration::from_secs(secs);
        loop {
            let log = self.log();
            if pred(&log) {
                return log;
            }
            assert!(Instant::now() < deadline, "timed out waiting; log so far:\n{log}");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

fn starts(log: &str) -> Vec<&str> {
    log.lines().filter(|l| l.starts_with("start ")).collect()
}

#[tokio::test]
async fn fresh_start_resume_and_graceful_stop() {
    let fx = Fixture::new("resume");
    let cfg = fx.config();
    let stop = CancellationToken::new();
    let run = tokio::spawn({
        let cfg = cfg.clone();
        let stop = stop.clone();
        async move { silta_session::run(&cfg, stop).await }
    });
    let log = fx.until(10, |l| l.contains("line Session test started")).await;
    let id = fx.id();
    assert_eq!(starts(&log), vec![format!("start {id} ").as_str()]);
    assert!(log.contains("a new conversation, no history"));
    assert!(fx.home.join("workspace/inbox").is_dir());
    stop.cancel();
    assert_eq!(run.await.unwrap(), 0);
    assert!(fx.log().contains(&format!("eof {id}")));

    // The second run resumes, the third finds no transcript and starts anew, the
    // fourth is refused by claude and drops the id.
    let stop = CancellationToken::new();
    let handle = tokio::spawn({
        let cfg = cfg.clone();
        let stop = stop.clone();
        async move { silta_session::run(&cfg, stop).await }
    });
    fx.until(10, |l| l.contains("restarted at") && l.contains("resumed its history")).await;
    assert_eq!(starts(&fx.log())[1], format!("start {id} resumed"));
    stop.cancel();
    assert_eq!(handle.await.unwrap(), 0);

    fs::remove_file(fx.home.join(format!(".claude/projects/-workspace/{id}.jsonl"))).unwrap();
    let stop = CancellationToken::new();
    let handle = tokio::spawn({
        let cfg = cfg.clone();
        let stop = stop.clone();
        async move { silta_session::run(&cfg, stop).await }
    });
    fx.until(10, |l| starts(l).len() == 3).await;
    let id2 = fx.id();
    assert_ne!(id2, id);
    assert!(starts(&fx.log())[2].ends_with(&format!("{id2} ")));
    stop.cancel();
    assert_eq!(handle.await.unwrap(), 0);

    fx.set("fake-mode", "noresume");
    assert_eq!(silta_session::run(&cfg, CancellationToken::new()).await, 1);
    assert!(!fx.home.join("session-id").exists(), "a refused resume drops the id");
    fs::remove_dir_all(&fx.home).unwrap();
}

#[tokio::test]
async fn threshold_rotation_with_handoff() {
    let fx = Fixture::new("threshold");
    fx.set("fake-context", "1000");
    let cfg = fx.config();
    let stop = CancellationToken::new();
    let run = tokio::spawn({
        let cfg = cfg.clone();
        let stop = stop.clone();
        async move { silta_session::run(&cfg, stop).await }
    });
    // Large context, and idle for 2 s after the start turn: the handoff is requested,
    // the fake writes the note and ends the turn, a fresh session starts. The fresh
    // session must report a small context, or it rotates too.
    fx.until(20, |l| l.contains(HANDOFF_LINE)).await;
    fx.set("fake-context", "10");
    let log = fx.until(20, |l| starts(l).len() == 2).await;
    let first = starts(&log)[0].split(' ').nth(1).unwrap().to_owned();
    let second = fx.id();
    assert_ne!(first, second);
    assert!(log.contains(&format!("line {HANDOFF_LINE}")));
    assert!(log.contains(&format!("handoff {first}")));
    assert!(log.contains(&format!("eof {first}")));
    assert!(starts(&log)[1].ends_with(&format!("{second} ")), "fresh, not resumed: {log}");
    fx.until(10, |l| l.contains("after a rotation: a new conversation, and the previous session's handoff is in memory")).await;
    assert!(!fx.home.join("rotate-requested").exists());
    assert!(!fx.home.join("rotation.json").exists());
    let backups: Vec<_> = fs::read_dir(fx.home.join("backups")).unwrap().flatten().collect();
    assert_eq!(backups.len(), 1);
    assert!(backups[0].path().join("-workspace/handoff.md").is_file());
    // The new session is small: no second rotation.
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(starts(&fx.log()).len(), 2);
    stop.cancel();
    assert_eq!(run.await.unwrap(), 0);
    fs::remove_dir_all(&fx.home).unwrap();
}

#[tokio::test]
async fn marker_with_a_hanging_handoff_retries_once_then_gives_up() {
    let fx = Fixture::new("hang");
    fx.set("fake-context", "10");
    let cfg = fx.config();
    let stop = CancellationToken::new();
    let run = tokio::spawn({
        let cfg = cfg.clone();
        let stop = stop.clone();
        async move { silta_session::run(&cfg, stop).await }
    });
    fx.until(10, |l| l.contains("line Session test started")).await;
    let first = fx.id();
    // The hook's marker appears while the session is quiet; the handoff turn hangs.
    fx.set("fake-mode", "hang");
    fx.set("rotate-requested", "test");
    // Handoff cap 3 s, grace 2 s, then the resume with the combined line, which hangs
    // again: cap 3 s, grace 2 s, then the fresh start without a handoff.
    let log = fx.until(30, |l| starts(l).len() == 3).await;
    let s = starts(&log);
    assert_eq!(s[1], format!("start {first} resumed"));
    let third = fx.id();
    assert_ne!(third, first);
    assert_eq!(s[2], format!("start {third} "));
    assert!(log.contains("Its last turn was cut by the host"));
    assert!(!log.contains(&format!("handoff {first}")));
    fx.set("fake-mode", "ok");
    fx.until(10, |l| l.contains("could not finish its handoff")).await;
    assert!(!fx.home.join("rotate-requested").exists());
    assert!(!fx.home.join("rotation.json").exists());
    stop.cancel();
    assert_eq!(run.await.unwrap(), 0);
    fs::remove_dir_all(&fx.home).unwrap();
}

#[tokio::test]
async fn a_failed_handoff_turn_is_retried_after_the_pause() {
    let fx = Fixture::new("error");
    fx.set("fake-context", "10");
    let cfg = fx.config();
    let stop = CancellationToken::new();
    let run = tokio::spawn({
        let cfg = cfg.clone();
        let stop = stop.clone();
        async move { silta_session::run(&cfg, stop).await }
    });
    fx.until(10, |l| l.contains("line Session test started")).await;
    let first = fx.id();
    fx.set("fake-mode", "error");
    fx.set("rotate-requested", "test");
    // The handoff turn fails: the session is resumed normally, the marker stays, and
    // the failure is on record.
    let log = fx.until(15, |l| starts(l).len() == 2).await;
    assert_eq!(starts(&log)[1], format!("start {first} resumed"));
    assert!(fx.home.join("rotate-requested").exists());
    assert_eq!(fs::read_to_string(fx.home.join("rotation.json")).unwrap().trim(), r#"{"next":"postponed","failures":1}"#);
    fx.until(10, |l| l.contains("resumed its history. You are connected")).await;
    // After the pause the handoff is requested again and succeeds.
    fx.set("fake-mode", "ok");
    let log = fx.until(20, |l| starts(l).len() == 3).await;
    assert_eq!(log.matches(HANDOFF_LINE).count(), 2);
    assert!(log.contains(&format!("handoff {first}")));
    assert_ne!(fx.id(), first);
    stop.cancel();
    assert_eq!(run.await.unwrap(), 0);
    fs::remove_dir_all(&fx.home).unwrap();
}

#[tokio::test]
async fn two_failed_handoff_turns_give_the_handoff_up() {
    let fx = Fixture::new("failures");
    fx.set("fake-context", "10");
    let cfg = fx.config();
    let stop = CancellationToken::new();
    let run = tokio::spawn({
        let cfg = cfg.clone();
        let stop = stop.clone();
        async move { silta_session::run(&cfg, stop).await }
    });
    fx.until(10, |l| l.contains("line Session test started")).await;
    let first = fx.id();
    fx.set("fake-mode", "error");
    fx.set("rotate-requested", "test");
    // First failure: resumed. Second, after the pause: fresh without a handoff.
    let log = fx.until(30, |l| starts(l).len() == 3).await;
    let s = starts(&log);
    assert_eq!(s[1], format!("start {first} resumed"));
    assert_ne!(fx.id(), first);
    assert_eq!(s[2], format!("start {} ", fx.id()));
    assert_eq!(log.matches(HANDOFF_LINE).count(), 2);
    assert!(!log.contains(&format!("handoff {first}")));
    fx.set("fake-mode", "ok");
    fx.until(10, |l| l.contains("could not finish its handoff")).await;
    assert!(!fx.home.join("rotate-requested").exists());
    assert!(!fx.home.join("rotation.json").exists());
    stop.cancel();
    assert_eq!(run.await.unwrap(), 0);
    fs::remove_dir_all(&fx.home).unwrap();
}

#[tokio::test]
async fn prompt_too_long_is_final_in_the_handoff_turn_and_inert_elsewhere() {
    let fx = Fixture::new("toolong");
    fx.set("fake-context", "10");
    fx.set("fake-mode", "toolong");
    let cfg = fx.config();
    let stop = CancellationToken::new();
    let run = tokio::spawn({
        let cfg = cfg.clone();
        let stop = stop.clone();
        async move { silta_session::run(&cfg, stop).await }
    });
    // The start turn itself fails with the message, and nothing is pending: the
    // session stays as it is.
    fx.until(10, |l| l.contains("line Session test started")).await;
    let first = fx.id();
    tokio::time::sleep(Duration::from_secs(4)).await;
    let log = fx.log();
    assert_eq!(starts(&log).len(), 1);
    assert!(!log.contains(HANDOFF_LINE));
    assert!(!fx.home.join("rotate-requested").exists());
    // Now a rotation is pending: the handoff turn fails the same way, and that is
    // final at once, no pause, no resume.
    fx.set("rotate-requested", "test");
    let log = fx.until(15, |l| starts(l).len() == 2).await;
    assert_eq!(log.matches(HANDOFF_LINE).count(), 1);
    assert_ne!(fx.id(), first);
    assert_eq!(starts(&log)[1], format!("start {} ", fx.id()), "fresh, not resumed");
    // The fake logs its start before it reads the start line, so wait for the line.
    fx.until(10, |l| l.contains("could not finish its handoff")).await;
    assert!(!fx.home.join("rotate-requested").exists());
    assert!(!fx.home.join("rotation.json").exists());
    stop.cancel();
    assert_eq!(run.await.unwrap(), 0);
    fs::remove_dir_all(&fx.home).unwrap();
}

#[tokio::test]
async fn a_unit_stop_during_the_wait_keeps_the_rotation_pending() {
    let fx = Fixture::new("pending");
    fx.set("fake-context", "10");
    let cfg = fx.config();
    // Rotation off: the marker is ignored entirely.
    let mut off = cfg.clone();
    off.limits.enabled = false;
    let stop = CancellationToken::new();
    let run = tokio::spawn({
        let off = off.clone();
        let stop = stop.clone();
        async move { silta_session::run(&off, stop).await }
    });
    fx.until(10, |l| l.contains("line Session test started")).await;
    fx.set("rotate-requested", "test");
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(!fx.log().contains(HANDOFF_LINE));
    stop.cancel();
    assert_eq!(run.await.unwrap(), 0);
    let first = fx.id();
    // Rotation on, the marker still there: the resumed session rotates at its first
    // quiet moment, right after the start turn.
    let stop = CancellationToken::new();
    let run = tokio::spawn({
        let cfg = cfg.clone();
        let stop = stop.clone();
        async move { silta_session::run(&cfg, stop).await }
    });
    let log = fx.until(15, |l| starts(l).len() == 3).await;
    assert_eq!(starts(&log)[1], format!("start {first} resumed"));
    assert!(log.contains(&format!("handoff {first}")));
    stop.cancel();
    assert_eq!(run.await.unwrap(), 0);
    fs::remove_dir_all(&fx.home).unwrap();
}
