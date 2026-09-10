//! What the supervisor keeps in the state directory, the session's `HOME`: the saved
//! session id, the rotation marker and the rotation's next step, the memory backups,
//! and the pruning of Claude Code's cache.

use std::{
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

    /// A dated copy of every project's memory directory under `backups/`, keeping the
    /// last `keep`. Returns the number of files copied.
    pub fn backup_memory(&self, keep: usize) -> io::Result<usize> {
        let mut sources = Vec::new();
        if let Ok(dirs) = fs::read_dir(self.projects()) {
            for dir in dirs.flatten() {
                let memory = dir.path().join("memory");
                if memory.is_dir() {
                    sources.push((dir.file_name(), memory));
                }
            }
        }
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
