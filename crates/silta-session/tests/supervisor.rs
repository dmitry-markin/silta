//! The supervisor against a fake claude (`tests/fake-claude.sh`): starts, resumes, the
//! resume rule, the graceful stop, and the rotation paths with caps of a few seconds:
//! the compaction in place, the fall-backs to a fresh id, and the two snapshots.

use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use silta_session::{compact, handoff, rotation::Limits, Auth, Config, EXIT_MODEL};
use tokio_util::sync::CancellationToken;

struct Fixture {
    home: PathBuf,
}

impl Fixture {
    fn new(name: &str) -> Self {
        let home =
            std::env::temp_dir().join(format!("silta-session-test-{name}-{}", std::process::id()));
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
            // The fake answers the window check with 500 000 unless told otherwise.
            window: Some(500_000),
            window_wait: Duration::from_secs(3),
            auth: None,
            stop_grace: Duration::from_secs(2),
            limits: Limits {
                enabled: true,
                context_tokens: 500,
                idle: Duration::from_secs(2),
                quiet: Duration::from_secs(3),
                handoff: Duration::from_secs(3),
                compact: Duration::from_secs(3),
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
        fs::read_to_string(self.home.join("session-id"))
            .unwrap()
            .trim()
            .to_owned()
    }

    /// The snapshot directories under `backups/`, oldest first; the fake's handoff is
    /// so quick that both snapshots of a rotation share a second, so the moment
    /// breaks the tie.
    fn snapshots(&self) -> Vec<PathBuf> {
        let mut dirs: Vec<PathBuf> = fs::read_dir(self.home.join("backups"))
            .map(|d| d.flatten().map(|e| e.path()).collect())
            .unwrap_or_default();
        dirs.sort_by_key(|p| {
            let n = name(p);
            (n[..20].to_owned(), n.ends_with("-after-handoff"))
        });
        dirs
    }

    fn transcript(&self, id: &str) -> PathBuf {
        self.home
            .join(format!(".claude/projects/-workspace/{id}.jsonl"))
    }

    /// Touches the marker and waits for the `nth` compaction in place of `id`, then
    /// for the host line that follows it.
    async fn rotate_by_marker_and_compact(&self, id: &str, nth: usize) -> String {
        self.set("rotate-requested", "test");
        self.until(20, |l| l.matches(&format!("compact {id}")).count() >= nth)
            .await;
        self.until(10, |l| {
            l.matches("line Context was compacted at").count() >= nth
        })
        .await
    }

    /// Waits until the fake's log satisfies `pred`, up to `secs`.
    async fn until(&self, secs: u64, pred: impl Fn(&str) -> bool) -> String {
        let deadline = Instant::now() + Duration::from_secs(secs);
        loop {
            let log = self.log();
            if pred(&log) {
                return log;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting; log so far:\n{log}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

fn starts(log: &str) -> Vec<&str> {
    log.lines().filter(|l| l.starts_with("start ")).collect()
}

fn name(path: &Path) -> String {
    path.file_name().unwrap().to_string_lossy().into_owned()
}

#[tokio::test]
async fn fresh_start_resume_and_graceful_stop() {
    let fx = Fixture::new("resume");
    let cfg = fx.config();
    // A ready file left by an earlier run must not let the plugin in early.
    fx.set("channel-ready", "stale");
    let stop = CancellationToken::new();
    let run = tokio::spawn({
        let cfg = cfg.clone();
        let stop = stop.clone();
        async move { silta_session::run(&cfg, stop).await }
    });
    let log = fx
        .until(10, |l| l.contains("line Session test started"))
        .await;
    assert!(
        log.contains("ready after init") && !log.contains("ready before init"),
        "the ready file is cleared at the start and written at the init line:\n{log}"
    );
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
    let log = fx
        .until(10, |l| {
            l.contains("restarted at") && l.contains("resumed its history")
        })
        .await;
    assert_eq!(starts(&log)[1], format!("start {id} resumed"));
    assert!(
        log.matches("ready after init").count() == 2 && !log.contains("ready before init"),
        "{log}"
    );
    stop.cancel();
    assert_eq!(handle.await.unwrap(), 0);

    fs::remove_file(
        fx.home
            .join(format!(".claude/projects/-workspace/{id}.jsonl")),
    )
    .unwrap();
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
    assert!(
        !fx.home.join("session-id").exists(),
        "a refused resume drops the id"
    );
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
    // the fake writes the note and ends the turn, and the session is compacted in
    // place like after any trigger. The fake reports a small context afterwards, so
    // there is no second rotation.
    let log = fx
        .until(20, |l| l.contains("line Context was compacted at"))
        .await;
    let first = fx.id();
    assert_eq!(
        starts(&log),
        vec![format!("start {first} ").as_str()],
        "no restart: {log}"
    );
    assert!(log.contains(&format!("line {}", handoff())));
    assert!(log.contains(&format!("handoff {first}")));
    assert!(log.contains(&format!("compact {first}")));
    assert!(!fx.home.join("rotate-requested").exists());
    assert!(!fx.home.join("rotation.json").exists());
    // Two snapshots: before the handoff (no memory directory existed yet, the
    // transcript did) and after it (the note and the transcript).
    let snaps = fx.snapshots();
    assert_eq!(snaps.len(), 2, "{snaps:?}");
    assert!(name(&snaps[0]).ends_with("-before-handoff"), "{snaps:?}");
    assert!(snaps[0].join(format!("-workspace/{first}.jsonl")).is_file());
    assert!(!snaps[0].join("-workspace/memory").exists());
    assert!(name(&snaps[1]).ends_with("-after-handoff"), "{snaps:?}");
    assert_eq!(
        fs::read_to_string(snaps[1].join("-workspace/memory/handoff.md"))
            .unwrap()
            .trim(),
        format!("handoff of {first}")
    );
    assert!(snaps[1].join(format!("-workspace/{first}.jsonl")).is_file());
    // The compacted session is small: no second rotation.
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(fx.log().matches(handoff()).count(), 1);
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
    fx.until(10, |l| l.contains("line Session test started"))
        .await;
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
    fx.until(10, |l| l.contains("could not finish its handoff"))
        .await;
    assert!(!fx.home.join("rotate-requested").exists());
    assert!(!fx.home.join("rotation.json").exists());
    // The clean snapshot was taken at the request; the retry took none, and no
    // handoff turn ended, so there is no after-handoff copy.
    let snaps: Vec<String> = fx.snapshots().iter().map(|p| name(p)).collect();
    assert_eq!(snaps.len(), 1, "{snaps:?}");
    assert!(snaps[0].ends_with("-before-handoff"));
    stop.cancel();
    assert_eq!(run.await.unwrap(), 0);
    fs::remove_dir_all(&fx.home).unwrap();
}

#[tokio::test]
async fn a_cut_before_the_handoff_takes_the_clean_snapshot_at_the_retry() {
    let fx = Fixture::new("cutwaiting");
    fx.set("fake-context", "10");
    let cfg = fx.config();
    let stop = CancellationToken::new();
    let run = tokio::spawn({
        let cfg = cfg.clone();
        let stop = stop.clone();
        async move { silta_session::run(&cfg, stop).await }
    });
    fx.until(10, |l| l.contains("line Session test started"))
        .await;
    stop.cancel();
    assert_eq!(run.await.unwrap(), 0);
    let first = fx.id();
    // The rotation is pending at the next start and the start turn hangs: the quiet
    // cap (3 s) cuts it before any handoff line went out, the session is resumed with
    // the combined line, and the clean snapshot is taken just before that line. The
    // handoff then succeeds and the session is compacted in place.
    fx.set("fake-mode", "hangonce");
    fx.set("rotate-requested", "test");
    let stop = CancellationToken::new();
    let run = tokio::spawn({
        let cfg = cfg.clone();
        let stop = stop.clone();
        async move { silta_session::run(&cfg, stop).await }
    });
    let log = fx
        .until(30, |l| l.contains("line Context was compacted at"))
        .await;
    let resumed = format!("start {first} resumed");
    assert_eq!(
        starts(&log),
        vec![
            format!("start {first} ").as_str(),
            resumed.as_str(),
            resumed.as_str()
        ],
        "{log}"
    );
    assert!(log.contains("Its last turn was cut by the host"));
    assert!(log.contains(&format!("handoff {first}")));
    assert!(log.contains(&format!("compact {first}")));
    let snaps: Vec<String> = fx.snapshots().iter().map(|p| name(p)).collect();
    assert_eq!(snaps.len(), 2, "{snaps:?}");
    assert!(snaps[0].ends_with("-before-handoff"));
    assert!(snaps[1].ends_with("-after-handoff"));
    assert_eq!(fx.id(), first);
    stop.cancel();
    assert_eq!(run.await.unwrap(), 0);
    fs::remove_dir_all(&fx.home).unwrap();
}

#[tokio::test]
async fn a_marker_compacts_the_session_in_place() {
    let fx = Fixture::new("compact");
    fx.set("fake-context", "10");
    let cfg = fx.config();
    let stop = CancellationToken::new();
    let run = tokio::spawn({
        let cfg = cfg.clone();
        let stop = stop.clone();
        async move { silta_session::run(&cfg, stop).await }
    });
    fx.until(10, |l| l.contains("line Session test started"))
        .await;
    let first = fx.id();
    // A note from before, so that the clean snapshot has something to show.
    let memory = fx.home.join(".claude/projects/-workspace/memory");
    fs::create_dir_all(&memory).unwrap();
    fs::write(memory.join("handoff.md"), "nothing pending").unwrap();
    // The marker right after a turn: the handoff is followed by the compaction, the
    // session keeps its id and claude is not restarted.
    let log = fx.rotate_by_marker_and_compact(&first, 1).await;
    assert_eq!(starts(&log).len(), 1, "no restart: {log}");
    assert_eq!(fx.id(), first);
    assert!(log.contains(&format!("line {}", handoff())));
    assert!(log.contains(&format!("line {}", compact())));
    let handoff_at = log.find(&format!("handoff {first}")).unwrap();
    let compact_at = log.find(&format!("compact {first}")).unwrap();
    assert!(handoff_at < compact_at);
    assert!(log.contains("the handoff note is in memory. This line comes from the host"));
    assert!(!fx.home.join("rotate-requested").exists());
    assert!(!fx.home.join("rotation.json").exists());
    // The snapshots: the clean note before, the handoff after; the transcript copied
    // after the handoff lacks the compact line the live one has.
    let snaps = fx.snapshots();
    assert_eq!(snaps.len(), 2, "{snaps:?}");
    assert!(name(&snaps[0]).ends_with("-before-handoff"));
    assert_eq!(
        fs::read_to_string(snaps[0].join("-workspace/memory/handoff.md")).unwrap(),
        "nothing pending"
    );
    assert!(name(&snaps[1]).ends_with("-after-handoff"));
    assert_eq!(
        fs::read_to_string(snaps[1].join("-workspace/memory/handoff.md"))
            .unwrap()
            .trim(),
        format!("handoff of {first}")
    );
    let copy = fs::read_to_string(snaps[1].join(format!("-workspace/{first}.jsonl"))).unwrap();
    assert!(
        copy.contains("Write your handoff now") && !copy.contains("/compact"),
        "{copy}"
    );
    assert!(fs::read_to_string(fx.transcript(&first))
        .unwrap()
        .contains("/compact"));
    // A second rotation, with a person's turn queued ahead of the command: its result
    // does not end the compaction, which follows.
    fx.set("fake-mode", "busycompact");
    let log = fx.rotate_by_marker_and_compact(&first, 2).await;
    assert_eq!(log.matches(&format!("compact {first}")).count(), 2);
    assert_eq!(log.matches("line Context was compacted at").count(), 2);
    let aside_at = log
        .find(&format!("aside {first}"))
        .expect("the aside turn ran");
    assert!(aside_at < log.rfind(&format!("compact {first}")).unwrap());
    assert_eq!(starts(&log).len(), 1, "no restart: {log}");
    assert_eq!(fx.snapshots().len(), 4);
    stop.cancel();
    assert_eq!(run.await.unwrap(), 0);
    fs::remove_dir_all(&fx.home).unwrap();
}

#[tokio::test]
async fn a_failed_or_hanging_compaction_is_retried_from_the_handoff() {
    let fx = Fixture::new("retrycompact");
    fx.set("fake-context", "10");
    let cfg = fx.config();
    let stop = CancellationToken::new();
    let run = tokio::spawn({
        let cfg = cfg.clone();
        let stop = stop.clone();
        async move { silta_session::run(&cfg, stop).await }
    });
    fx.until(10, |l| l.contains("line Session test started"))
        .await;
    let first = fx.id();
    // Claude Code reports the compaction failed: like a failed handoff turn, the
    // session is resumed, the marker stays, the failure is on record.
    fx.set("fake-mode", "nocompact");
    fx.set("rotate-requested", "test");
    let log = fx.until(15, |l| starts(l).len() == 2).await;
    assert!(log.contains(&format!("handoff {first}")));
    assert!(log.contains(&format!("compact-failed {first}")));
    assert_eq!(starts(&log)[1], format!("start {first} resumed"));
    assert!(fx.home.join("rotate-requested").exists());
    assert_eq!(
        fs::read_to_string(fx.home.join("rotation.json"))
            .unwrap()
            .trim(),
        r#"{"next":"postponed","attempts":1,"handoff":true}"#
    );
    fx.until(10, |l| l.contains("resumed its history. You are connected"))
        .await;
    // After the pause: the handoff again (no second clean snapshot), then the
    // compaction, which succeeds this time; the session keeps its id.
    fx.set("fake-mode", "ok");
    let log = fx
        .until(20, |l| l.contains("line Context was compacted at"))
        .await;
    assert_eq!(log.matches(handoff()).count(), 2);
    assert_eq!(log.matches(&format!("compact {first}")).count(), 1);
    assert_eq!(starts(&log).len(), 2);
    assert_eq!(fx.id(), first);
    assert!(!fx.home.join("rotate-requested").exists());
    assert!(!fx.home.join("rotation.json").exists());
    let snaps: Vec<String> = fx.snapshots().iter().map(|p| name(p)).collect();
    assert_eq!(snaps.len(), 3, "{snaps:?}");
    assert!(
        snaps[0].ends_with("-before-handoff")
            && snaps[1].ends_with("-after-handoff")
            && snaps[2].ends_with("-after-handoff"),
        "{snaps:?}"
    );
    // A compaction that never ends is cut at the compaction cap (3 s) plus the grace
    // (2 s) and the session resumed once with the combined line, whose handoff is
    // followed by the compaction again.
    fx.set("fake-mode", "compacthangonce");
    let log = fx.rotate_by_marker_and_compact(&first, 2).await;
    assert_eq!(starts(&log).len(), 3, "{log}");
    assert_eq!(starts(&log)[2], format!("start {first} resumed"));
    assert!(log.contains("Its last turn was cut by the host"));
    assert_eq!(log.matches(handoff()).count(), 3);
    assert_eq!(fx.id(), first);
    // A new rotation: its own clean snapshot, and one after each of its two handoffs.
    assert_eq!(fx.snapshots().len(), 6);
    stop.cancel();
    assert_eq!(run.await.unwrap(), 0);
    fs::remove_dir_all(&fx.home).unwrap();
}

#[tokio::test]
async fn two_failed_or_hanging_compactions_start_a_fresh_session() {
    let fx = Fixture::new("nocompact");
    fx.set("fake-context", "10");
    let cfg = fx.config();
    let stop = CancellationToken::new();
    let run = tokio::spawn({
        let cfg = cfg.clone();
        let stop = stop.clone();
        async move { silta_session::run(&cfg, stop).await }
    });
    fx.until(10, |l| l.contains("line Session test started"))
        .await;
    let first = fx.id();
    // The compaction fails, is retried after the pause and fails again: the handoff
    // is done, so the fresh session gets the start line that points at the note.
    fx.set("fake-mode", "nocompact");
    fx.set("rotate-requested", "test");
    let log = fx.until(30, |l| starts(l).len() == 3).await;
    assert_eq!(starts(&log)[1], format!("start {first} resumed"));
    assert_eq!(log.matches(&format!("compact-failed {first}")).count(), 2);
    let second = fx.id();
    assert_ne!(second, first);
    assert_eq!(
        starts(&log)[2],
        format!("start {second} "),
        "fresh, not resumed: {log}"
    );
    fx.until(10, |l| {
        l.contains(
            "after a rotation: a new conversation, and the previous session's handoff is in memory",
        )
    })
    .await;
    assert!(!fx.home.join("rotate-requested").exists());
    assert!(!fx.home.join("rotation.json").exists());
    // A compaction that hangs twice: cut and resumed once, then the same fresh start.
    fx.set("fake-mode", "compacthang");
    fx.set("rotate-requested", "test");
    let log = fx.until(40, |l| starts(l).len() == 5).await;
    assert_eq!(starts(&log)[3], format!("start {second} resumed"));
    assert_eq!(log.matches(&format!("handoff {second}")).count(), 2);
    assert!(!log.contains(&format!("compact {second}")));
    let third = fx.id();
    assert_ne!(third, second);
    assert_eq!(starts(&log)[4], format!("start {third} "));
    fx.until(10, |l| {
        l.matches("the previous session's handoff is in memory")
            .count()
            == 2
    })
    .await;
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
    fx.until(10, |l| l.contains("line Session test started"))
        .await;
    let first = fx.id();
    fx.set("fake-mode", "error");
    fx.set("rotate-requested", "test");
    // The handoff turn fails: the session is resumed normally, the marker stays, and
    // the failure is on record.
    let log = fx.until(15, |l| starts(l).len() == 2).await;
    assert_eq!(starts(&log)[1], format!("start {first} resumed"));
    assert!(fx.home.join("rotate-requested").exists());
    assert_eq!(
        fs::read_to_string(fx.home.join("rotation.json"))
            .unwrap()
            .trim(),
        r#"{"next":"postponed","attempts":1,"handoff":false}"#
    );
    fx.until(10, |l| l.contains("resumed its history. You are connected"))
        .await;
    // After the pause the handoff is requested again and succeeds, and the session is
    // compacted in place. The second request takes no clean snapshot.
    fx.set("fake-mode", "ok");
    let log = fx
        .until(20, |l| l.contains("line Context was compacted at"))
        .await;
    assert_eq!(log.matches(handoff()).count(), 2);
    assert!(log.contains(&format!("handoff {first}")));
    assert!(log.contains(&format!("compact {first}")));
    assert_eq!(starts(&log).len(), 2);
    assert_eq!(fx.id(), first);
    assert!(!fx.home.join("rotate-requested").exists());
    assert!(!fx.home.join("rotation.json").exists());
    let snaps: Vec<String> = fx.snapshots().iter().map(|p| name(p)).collect();
    assert_eq!(snaps.len(), 2, "{snaps:?}");
    assert!(
        snaps[0].ends_with("-before-handoff") && snaps[1].ends_with("-after-handoff"),
        "{snaps:?}"
    );
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
    fx.until(10, |l| l.contains("line Session test started"))
        .await;
    let first = fx.id();
    fx.set("fake-mode", "error");
    fx.set("rotate-requested", "test");
    // First failure: resumed. Second, after the pause: fresh without a handoff.
    let log = fx.until(30, |l| starts(l).len() == 3).await;
    let s = starts(&log);
    assert_eq!(s[1], format!("start {first} resumed"));
    assert_ne!(fx.id(), first);
    assert_eq!(s[2], format!("start {} ", fx.id()));
    assert_eq!(log.matches(handoff()).count(), 2);
    assert!(!log.contains(&format!("handoff {first}")));
    fx.set("fake-mode", "ok");
    fx.until(10, |l| l.contains("could not finish its handoff"))
        .await;
    assert!(!fx.home.join("rotate-requested").exists());
    assert!(!fx.home.join("rotation.json").exists());
    stop.cancel();
    assert_eq!(run.await.unwrap(), 0);
    fs::remove_dir_all(&fx.home).unwrap();
}

#[tokio::test]
async fn the_rotation_waits_for_background_tasks() {
    let fx = Fixture::new("agents");
    fx.set("fake-context", "10");
    let cfg = fx.config();
    let stop = CancellationToken::new();
    let run = tokio::spawn({
        let cfg = cfg.clone();
        let stop = stop.clone();
        async move { silta_session::run(&cfg, stop).await }
    });
    fx.until(10, |l| l.contains("line Session test started"))
        .await;
    stop.cancel();
    assert_eq!(run.await.unwrap(), 0);
    let first = fx.id();
    // A rotation is pending at the next start, whose start turn launches a background
    // agent that the level signal drops two seconds later, within the quiet cap. The
    // handoff must follow that signal: not requested while the agent runs, and not
    // after a cut.
    fx.set("fake-mode", "agents");
    fx.set("rotate-requested", "test");
    let stop = CancellationToken::new();
    let run = tokio::spawn({
        let cfg = cfg.clone();
        let stop = stop.clone();
        async move { silta_session::run(&cfg, stop).await }
    });
    let log = fx
        .until(20, |l| l.contains("line Context was compacted at"))
        .await;
    let done = log
        .find("background done")
        .expect("the background task reported");
    let handoff = log
        .find(&format!("line {}", handoff()))
        .expect("the handoff was requested");
    assert!(
        handoff > done,
        "the handoff was requested before the background task ended:\n{log}"
    );
    assert!(log.contains(&format!("handoff {first}")));
    assert!(log.contains(&format!("compact {first}")));
    assert_eq!(
        starts(&log),
        vec![
            format!("start {first} ").as_str(),
            format!("start {first} resumed").as_str()
        ],
        "compacted, not cut: {log}"
    );
    assert_eq!(fx.id(), first);
    stop.cancel();
    assert_eq!(run.await.unwrap(), 0);
    fs::remove_dir_all(&fx.home).unwrap();
}

#[tokio::test]
async fn the_handoff_turn_is_the_one_that_rewrites_the_note() {
    let fx = Fixture::new("note");
    fx.set("fake-context", "10");
    let mut cfg = fx.config();
    // Room for the fake's aside turn, its four seconds, and the handoff.
    cfg.limits.handoff = Duration::from_secs(10);
    let stop = CancellationToken::new();
    let run = tokio::spawn({
        let cfg = cfg.clone();
        let stop = stop.clone();
        async move { silta_session::run(&cfg, stop).await }
    });
    fx.until(10, |l| l.contains("line Session test started"))
        .await;
    let first = fx.id();
    // The note exists from before: only its rewrite ends the handoff. The fake answers
    // the handoff line with an unrelated turn first, which writes another note, and
    // ends it; the supervisor must not take that as the handoff, or the fake is killed
    // in the stop grace before it writes the note.
    let memory = fx.home.join(".claude/projects/-workspace/memory");
    fs::create_dir_all(&memory).unwrap();
    let note = memory.join("handoff.md");
    fs::write(&note, "nothing pending").unwrap();
    fs::File::open(&note)
        .unwrap()
        .set_modified(std::time::SystemTime::now() - Duration::from_secs(60))
        .unwrap();
    fx.set("fake-mode", "busy");
    let log = fx.rotate_by_marker_and_compact(&first, 1).await;
    let aside = log
        .find(&format!("aside {first}"))
        .expect("the aside turn ran");
    let handoff = log
        .find(&format!("handoff {first}"))
        .expect("the handoff was written after the aside turn");
    assert!(aside < handoff);
    assert_eq!(starts(&log).len(), 1, "compacted, not restarted: {log}");
    assert_eq!(fx.id(), first);
    assert_eq!(
        fs::read_to_string(&note).unwrap().trim(),
        format!("handoff of {first}")
    );
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
    fx.until(10, |l| l.contains("line Session test started"))
        .await;
    fx.set("rotate-requested", "test");
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(!fx.log().contains(handoff()));
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
    let log = fx
        .until(15, |l| l.contains("line Context was compacted at"))
        .await;
    assert_eq!(
        starts(&log),
        vec![
            format!("start {first} ").as_str(),
            format!("start {first} resumed").as_str()
        ]
    );
    assert!(log.contains(&format!("handoff {first}")));
    assert!(log.contains(&format!("compact {first}")));
    assert_eq!(fx.id(), first);
    stop.cancel();
    assert_eq!(run.await.unwrap(), 0);
    fs::remove_dir_all(&fx.home).unwrap();
}

#[tokio::test]
async fn a_window_below_the_configured_one_ends_the_session_before_its_first_turn() {
    let fx = Fixture::new("window");
    let cfg = Config {
        limits: Limits {
            enabled: false,
            ..fx.config().limits
        },
        ..fx.config()
    };
    // Claude Code caps the window of a model it does not know at 200 000 tokens.
    fx.set("fake-window", "200000");
    assert_eq!(
        silta_session::run(&cfg, CancellationToken::new()).await,
        EXIT_MODEL
    );
    let log = fx.log();
    assert!(log.contains("control silta-window"), "{log}");
    assert!(!log.contains("\nline "), "no turn before the check: {log}");

    // An unanswered check ends it as well, after the wait.
    fx.set("fake-window", "500000");
    fx.set("fake-mode", "nowindow");
    let started = Instant::now();
    assert_eq!(
        silta_session::run(&cfg, CancellationToken::new()).await,
        EXIT_MODEL
    );
    assert!(started.elapsed() >= cfg.window_wait);
    assert!(!fx.log().contains("\nline "), "{}", fx.log());

    // The configured window starts the turn.
    fx.set("fake-mode", "ok");
    let stop = CancellationToken::new();
    let run = tokio::spawn({
        let cfg = cfg.clone();
        let stop = stop.clone();
        async move { silta_session::run(&cfg, stop).await }
    });
    fx.until(10, |l| l.contains("line Session test restarted"))
        .await;
    stop.cancel();
    assert_eq!(run.await.unwrap(), 0);
    fs::remove_dir_all(&fx.home).unwrap();
}

#[tokio::test]
async fn a_fallback_runs_through_an_outage_and_ends_the_session_otherwise() {
    let fx = Fixture::new("fallback");
    let cfg = Config {
        fallback_model: Some("fake-fallback".into()),
        limits: Limits {
            enabled: false,
            ..fx.config().limits
        },
        ..fx.config()
    };
    fx.set("fake-mode", "fallback-overloaded");
    let stop = CancellationToken::new();
    let run = tokio::spawn({
        let cfg = cfg.clone();
        let stop = stop.clone();
        async move { silta_session::run(&cfg, stop).await }
    });
    fx.until(10, |l| {
        l.contains("fallback ") && l.contains("line Session test started")
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!run.is_finished(), "an overload keeps the session");
    stop.cancel();
    assert_eq!(run.await.unwrap(), 0);

    // A fallback that is not an outage ends the session, and it is not started again.
    for trigger in [
        "last_resort",
        "model_not_found",
        "permission_denied",
        "unheard_of",
    ] {
        fx.set("fake-mode", &format!("fallback-{trigger}"));
        let runs = starts(&fx.log()).len();
        assert_eq!(
            silta_session::run(&cfg, CancellationToken::new()).await,
            EXIT_MODEL,
            "{trigger}"
        );
        assert_eq!(starts(&fx.log()).len(), runs + 1, "{trigger}");
    }
    fs::remove_dir_all(&fx.home).unwrap();
}

/// Starts a session with `auth` and returns the credential variables claude saw.
async fn auth_line(name: &str, auth: Auth) -> String {
    let fx = Fixture::new(name);
    let cfg = Config {
        auth: Some(auth),
        ..fx.config()
    };
    let stop = CancellationToken::new();
    let run = tokio::spawn({
        let stop = stop.clone();
        async move { silta_session::run(&cfg, stop).await }
    });
    let log = fx.until(10, |l| l.contains("ready after init")).await;
    stop.cancel();
    run.await.unwrap();
    log.lines()
        .find(|l| l.starts_with("auth "))
        .unwrap()
        .to_owned()
}

#[tokio::test]
async fn each_credential_sets_its_own_variables_and_no_others() {
    assert_eq!(
        auth_line("auth-oauth", Auth::OAuth("sub".into())).await,
        "auth oauth=sub base=unset token=unset key=unset discovery=unset"
    );
    assert_eq!(
        auth_line("auth-api-key", Auth::ApiKey("key".into())).await,
        "auth oauth=unset base=unset token=unset key=key discovery=unset"
    );
    let gateway = Auth::Gateway {
        url: "https://gateway.example/api".into(),
        token: "gw".into(),
    };
    assert_eq!(
        auth_line("auth-gateway", gateway).await,
        "auth oauth=unset base=https://gateway.example/api token=gw key= discovery=1"
    );
}

#[test]
fn the_credential_is_exactly_one_token_file() {
    let fx = Fixture::new("auth-load");
    let load = |url| Auth::load(&fx.home, url).map(|a| format!("{a:?}"));
    assert!(load(None).is_err());
    fx.set("auth_oauth-token", "sub-\ntoken\n");
    assert_eq!(load(None).unwrap(), "a subscription token");
    fx.set("auth_gateway-token", "gw\n");
    assert!(load(Some("https://g")).is_err());
    fs::remove_file(fx.home.join("auth_oauth-token")).unwrap();
    fx.set("auth_api-key", "key\n");
    assert!(load(Some("https://g")).is_err());
    fs::remove_file(fx.home.join("auth_gateway-token")).unwrap();
    assert_eq!(load(None).unwrap(), "a Console API key");
    fs::remove_file(fx.home.join("auth_api-key")).unwrap();
    fx.set("auth_gateway-token", "gw\n");
    assert!(load(None).is_err());
    assert_eq!(
        load(Some("https://g")).unwrap(),
        "a gateway token for https://g"
    );
}
