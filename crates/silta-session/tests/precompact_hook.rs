//! The PreCompact hook (`deploy/silta-precompact-block`), run as Claude Code runs it: the
//! hook input on stdin, the unit's environment. It blocks the main session's automatic
//! compaction and leaves the `rotate-requested` marker the supervisor rotates on, and lets
//! a subagent compact its own context without a marker. Input that is not a JSON object counts
//! as the main session's.

use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

/// The main thread's input, as Claude Code 2.1.325 writes it: no `agent_id`.
const MAIN: &str = r#"{"session_id":"08d29791-6283-45d1-9ba8-a4f0f178ac80","transcript_path":"/home/m/.claude/projects/-w/08d29791-6283-45d1-9ba8-a4f0f178ac80.jsonl","cwd":"/w","prompt_id":"7fcd7c91-adbd-4fbf-8386-679b94486163","effort":{"level":"high"},"hook_event_name":"PreCompact","trigger":"auto","custom_instructions":null}"#;

/// The main thread of an `--agent` session: `agent_type` without `agent_id`.
const MAIN_AGENT_TYPE: &str = r#"{"session_id":"08d29791-6283-45d1-9ba8-a4f0f178ac80","transcript_path":"/t.jsonl","cwd":"/w","agent_type":"persona","hook_event_name":"PreCompact","trigger":"auto","custom_instructions":null}"#;

/// A subagent's input: the main session's id and transcript, plus `agent_id`.
const SUBAGENT: &str = r#"{"session_id":"08d29791-6283-45d1-9ba8-a4f0f178ac80","transcript_path":"/t.jsonl","cwd":"/w","agent_id":"a4f2c9e1b7d3","agent_type":"general-purpose","hook_event_name":"PreCompact","trigger":"auto","custom_instructions":null}"#;

struct Hook {
    state: PathBuf,
}

impl Hook {
    fn new(name: &str) -> Self {
        let state = std::env::temp_dir().join(format!(
            "silta-precompact-test-{name}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&state);
        fs::create_dir_all(&state).unwrap();
        Self { state }
    }

    fn script() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/silta-precompact-block")
    }

    /// Runs the hook with `input` on stdin and the unit's environment plus `env`; the
    /// exit code and stderr.
    fn run(&self, input: &str, env: &[(&str, &str)]) -> (i32, String) {
        let mut cmd = Command::new(Self::script());
        cmd.env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", &self.state)
            .env("STATE_DIRECTORY", &self.state)
            .env("SILTA_SESSION", "test")
            .envs(env.iter().copied())
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn().expect("deploy/silta-precompact-block");
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
        let out = child.wait_with_output().unwrap();
        (
            out.status.code().expect("exit code"),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    }

    fn set(&self, file: &str, content: &str) {
        fs::write(self.state.join(file), content).unwrap();
    }

    fn marker(&self) -> Option<String> {
        fs::read_to_string(self.state.join("rotate-requested")).ok()
    }

    /// The names in the state directory, sorted.
    fn files(&self) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(&self.state)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        names
    }
}

impl Drop for Hook {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.state);
    }
}

#[test]
fn main_session_compaction_is_blocked_and_asks_for_a_rotation() {
    for (name, input) in [("main", MAIN), ("main-agent-type", MAIN_AGENT_TYPE)] {
        let hook = Hook::new(name);
        let (code, stderr) = hook.run(input, &[]);
        assert_eq!(code, 2, "{name}: stderr {stderr}");
        assert!(
            stderr.contains("automatic compaction blocked"),
            "{name}: {stderr}"
        );
        assert_eq!(
            hook.marker().as_deref(),
            Some(format!("{input}\n").as_str()),
            "{name}"
        );
        assert_eq!(hook.files(), ["rotate-requested"], "{name}");
    }
}

#[test]
fn subagent_compaction_proceeds_without_a_marker() {
    let pretty = SUBAGENT.replace(",\"", ",\n  \"").replace("\":", "\": ");
    let numeric = SUBAGENT.replace(r#""agent_id":"a4f2c9e1b7d3""#, r#""agent_id":7"#);
    for (name, input) in [
        ("subagent", SUBAGENT),
        ("pretty", pretty.as_str()),
        // Not a string today, but any value but null is a subagent's id.
        ("numeric", numeric.as_str()),
    ] {
        let hook = Hook::new(name);
        let (code, stderr) = hook.run(input, &[]);
        assert_eq!(code, 0, "{name}: stderr {stderr}");
        assert!(stderr.is_empty(), "{name}: {stderr}");
        assert_eq!(hook.marker(), None, "{name}");
    }
}

#[test]
fn only_a_top_level_agent_id_is_a_subagent() {
    let quoted = MAIN.replace(
        r#""custom_instructions":null"#,
        r#""custom_instructions":"keep \"agent_id\": \"x\" out""#,
    );
    let nested = MAIN.replace(
        r#""effort":{"level":"high"}"#,
        r#""effort":{"level":"high","agent_id":"a4f2c9e1b7d3"}"#,
    );
    let null = MAIN.replace(r#""trigger":"auto""#, r#""trigger":"auto","agent_id":null"#);
    for (name, input) in [
        ("quoted", quoted.as_str()),
        ("nested", nested.as_str()),
        ("null", null.as_str()),
    ] {
        assert_ne!(input, MAIN, "{name}: the replacement missed");
        let hook = Hook::new(name);
        let (code, stderr) = hook.run(input, &[]);
        assert_eq!(code, 2, "{name}: stderr {stderr}");
        assert_eq!(
            hook.marker().as_deref(),
            Some(format!("{input}\n").as_str()),
            "{name}"
        );
    }
}

#[test]
fn input_that_is_not_a_json_object_counts_as_the_main_session() {
    // Too deep for Python's JSON parser, which raises RecursionError, not ValueError.
    let deep = format!("{}{}", "[".repeat(100_000), "]".repeat(100_000));
    for (name, input) in [
        ("garbage", "not json\n"),
        ("array", r#"[{"agent_id":"a4f2c9e1b7d3"}]"#),
        ("empty", ""),
        ("deep", deep.as_str()),
    ] {
        let hook = Hook::new(name);
        let (code, stderr) = hook.run(input, &[]);
        assert_eq!(code, 2, "{name}: stderr {stderr}");
        assert_eq!(
            hook.marker().as_deref(),
            Some(format!("{}\n", input.trim_end_matches('\n')).as_str()),
            "{name}"
        );
    }
}

#[test]
fn the_supervisors_temporary_file_is_left_alone() {
    let hook = Hook::new("supervisor-tmp");
    // The supervisor's write_atomic, caught between its write and its rename.
    hook.set(
        "rotate-requested.tmp",
        "rotation requested by the supervisor\n",
    );
    let (code, stderr) = hook.run(MAIN, &[]);
    assert_eq!(code, 2, "stderr {stderr}");
    assert_eq!(hook.marker().as_deref(), Some(format!("{MAIN}\n").as_str()));
    assert_eq!(
        fs::read_to_string(hook.state.join("rotate-requested.tmp")).unwrap(),
        "rotation requested by the supervisor\n"
    );
}

#[test]
fn compaction_proceeds_when_the_marker_cannot_be_written() {
    let hook = Hook::new("unwritable");
    let missing = hook.state.join("missing");
    let (code, stderr) = hook.run(MAIN, &[("STATE_DIRECTORY", missing.to_str().unwrap())]);
    assert_eq!(code, 0, "stderr {stderr}");
    assert!(stderr.is_empty(), "{stderr}");
    assert_eq!(fs::read_dir(&hook.state).unwrap().count(), 0);
}

#[test]
fn outside_a_mind_or_with_rotation_off_compaction_proceeds() {
    let hook = Hook::new("off");
    for env in [
        &[("SILTA_SESSION", "")][..],
        &[("SILTA_ROTATE", "0")],
        &[("SILTA_ROTATE", "Off")],
        &[("SILTA_ROTATE", "FALSE")],
        &[("SILTA_ROTATE", "no")],
        // The supervisor trims the value.
        &[("SILTA_ROTATE", " 0\n")],
    ] {
        let (code, stderr) = hook.run(MAIN, env);
        assert_eq!(code, 0, "{env:?}: stderr {stderr}");
        assert_eq!(hook.marker(), None, "{env:?}");
    }
    // Any other value leaves rotation on.
    let (code, _) = hook.run(MAIN, &[("SILTA_ROTATE", "1")]);
    assert_eq!(code, 2);
}
