//! What the supervisor keeps in the state directory, the session's `HOME`: the saved
//! session id, the rotation marker and the rotation's next step, the memory backups,
//! and the pruning of Claude Code's cache.

use std::{
    ffi::OsString,
    fs, io,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

/// What the next start of claude is, persisted so that a unit stop in the middle of a
/// rotation does not lose it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(tag = "next", rename_all = "kebab-case")]
pub enum Next {
    /// Resume the saved session, or start one if it cannot be resumed.
    #[default]
    Normal,
    /// Resume the saved session with the combined cut-and-handoff line.
    Retry,
    /// Resume the saved session normally; the rotation stays pending after this many
    /// failed handoff turns.
    Postponed { failures: u32 },
    /// The saved session is rotated out: start a new one, and say whether its handoff
    /// turn ended normally.
    Fresh { handoff: bool },
}

/// The handoff note the persona keeps, in the memory directory of the workspace's
/// project.
pub const HANDOFF_NOTE: &str = "handoff.md";

/// What a handoff request found on disk, for `Paths::handoff_written`.
#[derive(Debug, Clone, Copy)]
pub struct HandoffWatch {
    /// The request, in whole seconds since the epoch: the coarsest mtime any file
    /// system keeps, so a write in the request's own second still counts.
    since_secs: u64,
    note_existed: bool,
}

impl HandoffWatch {
    pub fn note_existed(&self) -> bool {
        self.note_existed
    }
}

pub struct Paths {
    state: PathBuf,
}

impl Paths {
    pub fn new(state: &Path) -> Self {
        Self { state: state.to_path_buf() }
    }

    pub fn workspace(&self) -> PathBuf {
        self.state.join("workspace")
    }

    pub fn id_file(&self) -> PathBuf {
        self.state.join("session-id")
    }

    /// The rotation marker, written by the pre-compaction hook or by the supervisor.
    pub fn marker(&self) -> PathBuf {
        self.state.join("rotate-requested")
    }

    fn rotation(&self) -> PathBuf {
        self.state.join("rotation.json")
    }

    fn projects(&self) -> PathBuf {
        self.state.join(".claude").join("projects")
    }

    fn backups(&self) -> PathBuf {
        self.state.join("backups")
    }

    /// The transcript of a session, if Claude Code has one under any project.
    pub fn transcript(&self, id: &str) -> Option<PathBuf> {
        let dirs = fs::read_dir(self.projects()).ok()?;
        dirs.flatten().map(|d| d.path().join(format!("{id}.jsonl"))).find(|p| p.is_file())
    }

    pub fn read_id(&self) -> Option<String> {
        let id = fs::read_to_string(self.id_file()).ok()?;
        let id = id.trim();
        (!id.is_empty()).then(|| id.to_owned())
    }

    pub fn write_id(&self, id: &str) -> io::Result<()> {
        write_atomic(&self.id_file(), format!("{id}\n").as_bytes())
    }

    pub fn drop_id(&self) {
        let _ = fs::remove_file(self.id_file());
    }

    pub fn marker_exists(&self) -> bool {
        self.marker().exists()
    }

    pub fn write_marker(&self, why: &str) -> io::Result<()> {
        write_atomic(&self.marker(), format!("{why}\n").as_bytes())
    }

    pub fn read_next(&self) -> Next {
        fs::read_to_string(self.rotation())
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    }

    pub fn write_next(&self, next: Next) -> io::Result<()> {
        if next == Next::Normal {
            return match fs::remove_file(self.rotation()) {
                Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
                _ => Ok(()),
            };
        }
        let json = serde_json::to_string(&next).map_err(io::Error::other)?;
        write_atomic(&self.rotation(), format!("{json}\n").as_bytes())
    }

    /// The rotation is complete: neither the marker nor the next step applies.
    pub fn clear_rotation(&self) {
        let _ = fs::remove_file(self.marker());
        let _ = fs::remove_file(self.rotation());
    }

    /// The memory directory of every project, with the project's slug.
    fn memory_dirs(&self) -> Vec<(OsString, PathBuf)> {
        let mut dirs = Vec::new();
        if let Ok(entries) = fs::read_dir(self.projects()) {
            for dir in entries.flatten() {
                let memory = dir.path().join("memory");
                if memory.is_dir() {
                    dirs.push((dir.file_name(), memory));
                }
            }
        }
        dirs
    }

    /// Taken when the handoff is requested.
    pub fn watch_handoff(&self) -> HandoffWatch {
        HandoffWatch {
            since_secs: unix_millis() / 1000,
            note_existed: self.memory_dirs().iter().any(|(_, m)| m.join(HANDOFF_NOTE).is_file()),
        }
    }

    /// Whether the handoff has been written since the request. When the note existed,
    /// only its own rewrite counts: a session that has the note reuses it. When it did
    /// not, any write under a memory directory counts (the note created under that
    /// name, or under another), a looser test than a wait for the exact name that a
    /// session might never use.
    pub fn handoff_written(&self, watch: &HandoffWatch) -> bool {
        let written = |path: &Path| {
            fs::metadata(path)
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .is_some_and(|d| d.as_secs() >= watch.since_secs)
        };
        self.memory_dirs().iter().any(|(_, memory)| {
            if watch.note_existed {
                written(&memory.join(HANDOFF_NOTE))
            } else {
                any_file(memory, &written)
            }
        })
    }

    /// A dated copy of every project's memory directory under `backups/`, keeping the
    /// last `keep`. Returns the number of files copied.
    pub fn backup_memory(&self, keep: usize) -> io::Result<usize> {
        let sources = self.memory_dirs();
        if sources.is_empty() {
            return Ok(0);
        }
        let target = self.backups().join(format!("memory-{}", silta::time::rfc3339_utc(unix_millis())));
        let mut copied = 0;
        for (slug, memory) in sources {
            copied += copy_dir(&memory, &target.join(slug))?;
        }
        let mut old: Vec<PathBuf> = fs::read_dir(self.backups())?
            .flatten()
            .map(|d| d.path())
            .filter(|p| p.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.starts_with("memory-")))
            .collect();
        old.sort();
        for dir in old.iter().rev().skip(keep) {
            let _ = fs::remove_dir_all(dir);
        }
        Ok(copied)
    }

    /// Claude Code keeps one log file per start of an MCP server and never removes them.
    pub fn prune_cache(&self) {
        let cutoff = SystemTime::now() - Duration::from_secs(30 * 86_400);
        prune_older(&self.state.join(".cache").join("claude-cli-nodejs"), cutoff);
    }
}

fn prune_older(dir: &Path, cutoff: SystemTime) {
    let Ok(entries) = fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(meta) = entry.metadata() else { continue };
        if meta.is_dir() {
            prune_older(&path, cutoff);
        } else if meta.modified().is_ok_and(|m| m < cutoff) {
            let _ = fs::remove_file(path);
        }
    }
}

/// Whether any file below `dir` satisfies `pred`.
fn any_file(dir: &Path, pred: &dyn Fn(&Path) -> bool) -> bool {
    let Ok(entries) = fs::read_dir(dir) else { return false };
    entries.flatten().any(|entry| {
        let path = entry.path();
        if path.is_dir() {
            any_file(&path, pred)
        } else {
            pred(&path)
        }
    })
}

fn copy_dir(src: &Path, dst: &Path) -> io::Result<usize> {
    fs::create_dir_all(dst)?;
    let mut copied = 0;
    for entry in fs::read_dir(src)?.flatten() {
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if from.is_dir() {
            copied += copy_dir(&from, &to)?;
        } else if from.is_file() {
            fs::copy(&from, &to)?;
            copied += 1;
        }
    }
    Ok(copied)
}

fn write_atomic(path: &Path, content: &[u8]) -> io::Result<()> {
    let tmp = PathBuf::from(format!("{}.tmp", path.display()));
    fs::write(&tmp, content)?;
    fs::rename(&tmp, path)
}

pub fn unix_millis() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

/// A random version-4 UUID, as Claude Code expects for `--session-id`.
pub fn new_id() -> io::Result<String> {
    let mut bytes = [0u8; 16];
    use std::io::Read;
    fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: Vec<String> = bytes.iter().map(|b| format!("{b:02x}")).collect();
    let hex = hex.concat();
    Ok(format!("{}-{}-{}-{}-{}", &hex[0..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..32]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("silta-session-state-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn next_round_trips_and_normal_removes_the_file() {
        let dir = scratch("next");
        let paths = Paths::new(&dir);
        assert_eq!(paths.read_next(), Next::Normal);
        paths.write_next(Next::Fresh { handoff: true }).unwrap();
        assert_eq!(fs::read_to_string(dir.join("rotation.json")).unwrap(), "{\"next\":\"fresh\",\"handoff\":true}\n");
        assert_eq!(paths.read_next(), Next::Fresh { handoff: true });
        paths.write_next(Next::Retry).unwrap();
        assert_eq!(paths.read_next(), Next::Retry);
        paths.write_next(Next::Postponed { failures: 1 }).unwrap();
        assert_eq!(fs::read_to_string(dir.join("rotation.json")).unwrap(), "{\"next\":\"postponed\",\"failures\":1}\n");
        assert_eq!(paths.read_next(), Next::Postponed { failures: 1 });
        paths.write_next(Next::Normal).unwrap();
        assert!(!dir.join("rotation.json").exists());
        paths.write_next(Next::Normal).unwrap();
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn ids_transcripts_and_markers() {
        let dir = scratch("ids");
        let paths = Paths::new(&dir);
        assert_eq!(paths.read_id(), None);
        let id = new_id().unwrap();
        assert_eq!(id.len(), 36);
        assert_eq!(&id[14..15], "4");
        paths.write_id(&id).unwrap();
        assert_eq!(paths.read_id().as_deref(), Some(id.as_str()));
        assert!(paths.transcript(&id).is_none());
        let project = dir.join(".claude/projects/-w");
        fs::create_dir_all(&project).unwrap();
        fs::write(project.join(format!("{id}.jsonl")), "").unwrap();
        assert!(paths.transcript(&id).is_some());
        assert!(!paths.marker_exists());
        paths.write_marker("threshold").unwrap();
        assert!(paths.marker_exists());
        paths.clear_rotation();
        assert!(!paths.marker_exists());
        paths.drop_id();
        assert_eq!(paths.read_id(), None);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn the_handoff_check_wants_the_note_when_it_exists_and_any_memory_write_otherwise() {
        let dir = scratch("handoff");
        let paths = Paths::new(&dir);
        let memory = dir.join(".claude/projects/-w/memory");
        fs::create_dir_all(&memory).unwrap();
        let old = SystemTime::now() - Duration::from_secs(120);
        let touch = |name: &str, when: SystemTime| {
            let path = memory.join(name);
            fs::write(&path, "x").unwrap();
            fs::File::open(&path).unwrap().set_modified(when).unwrap();
        };
        // No note yet: nothing written, then another note written, then the note itself.
        let watch = paths.watch_handoff();
        assert!(!paths.handoff_written(&watch));
        touch("MEMORY.md", old);
        assert!(!paths.handoff_written(&watch));
        touch("self-and-alice.md", SystemTime::now());
        assert!(paths.handoff_written(&watch));
        // The note exists: only its own rewrite counts.
        touch(HANDOFF_NOTE, old);
        let watch = paths.watch_handoff();
        touch("self-and-alice.md", SystemTime::now());
        assert!(!paths.handoff_written(&watch));
        touch(HANDOFF_NOTE, SystemTime::now());
        assert!(paths.handoff_written(&watch));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn memory_backups_copy_every_project_and_keep_the_last_n() {
        let dir = scratch("backup");
        let paths = Paths::new(&dir);
        assert_eq!(paths.backup_memory(2).unwrap(), 0);
        let memory = dir.join(".claude/projects/-w/memory");
        fs::create_dir_all(memory.join("sub")).unwrap();
        fs::write(memory.join("MEMORY.md"), "index").unwrap();
        fs::write(memory.join("sub/note.md"), "note").unwrap();
        for stamp in ["2020-01-01T00:00:00Z", "2020-01-02T00:00:00Z"] {
            fs::create_dir_all(dir.join("backups").join(format!("memory-{stamp}"))).unwrap();
        }
        assert_eq!(paths.backup_memory(2).unwrap(), 2);
        let mut names: Vec<String> = fs::read_dir(dir.join("backups")).unwrap().flatten().map(|d| d.file_name().to_string_lossy().into_owned()).collect();
        names.sort();
        assert_eq!(names.len(), 2, "{names:?}");
        assert_eq!(names[0], "memory-2020-01-02T00:00:00Z");
        let latest = dir.join("backups").join(&names[1]);
        assert_eq!(fs::read_to_string(latest.join("-w/sub/note.md")).unwrap(), "note");
        assert_eq!(fs::read_to_string(latest.join("-w/MEMORY.md")).unwrap(), "index");
        fs::remove_dir_all(dir).unwrap();
    }
}
