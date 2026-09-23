//! `siltad backup`: a consistent copy of the store while the daemon runs.
//!
//! Each SQLite file of the store is copied with `VACUUM INTO`, which reads one
//! transaction-consistent snapshot even while the daemon writes (the stores are in WAL
//! mode); `session.json` and `delivered.json` are copied as files. The copy lands in a
//! directory named after the time, the oldest copies beyond `--keep` are removed. The
//! crypto store is the part that matters: losing it means new device keys, which every
//! family member would have to verify again.

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{bail, Context, Result};
use rusqlite::Connection;
use tracing::{info, warn};

use crate::{
    daemon::MARKS_FILE,
    session::{SESSION_FILE, STORE_DIR},
};

/// The copy's directory name: the RFC 3339 time with `-` for `:`, so it sorts by time
/// and needs no quoting.
pub fn stamp(now_ms: u64) -> String {
    silta::time::rfc3339_utc(now_ms).replace(':', "-")
}

fn is_stamp(name: &str) -> bool {
    name.len() == 20
        && name.ends_with('Z')
        && name
            .chars()
            .all(|c| c.is_ascii_digit() || matches!(c, '-' | 'T' | 'Z'))
}

/// Copy the store of `state_dir` into `to/<stamp>` and keep the newest `keep` copies.
/// Returns the new copy's path.
pub fn run(state_dir: &Path, to: &Path, keep: usize, stamp: &str) -> Result<PathBuf> {
    if !is_stamp(stamp) {
        bail!("{stamp:?} is not a backup stamp");
    }
    fs::create_dir_all(to).with_context(|| format!("cannot create {}", to.display()))?;
    fs::set_permissions(to, fs::Permissions::from_mode(0o700))?;
    let target = to.join(stamp);
    if target.exists() {
        bail!("{} exists already", target.display());
    }
    let tmp = to.join(format!(".{stamp}.tmp"));
    if tmp.exists() {
        fs::remove_dir_all(&tmp)?;
    }
    fs::create_dir(&tmp).with_context(|| format!("cannot create {}", tmp.display()))?;

    let store = state_dir.join(STORE_DIR);
    let mut files = 0usize;
    let mut bytes = 0u64;
    match fs::read_dir(&store) {
        Ok(entries) => {
            let mut names: Vec<_> = entries
                .filter_map(Result::ok)
                .map(|e| e.file_name())
                .filter(|n| n.to_string_lossy().ends_with(".sqlite3"))
                .collect();
            names.sort();
            if names.is_empty() {
                warn!("no database in {}; nothing to copy", store.display());
            }
            for name in names {
                let dest = tmp.join(&name);
                copy_database(&store.join(&name), &dest)?;
                bytes += fs::metadata(&dest).map(|m| m.len()).unwrap_or(0);
                files += 1;
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            warn!("no store at {} yet; nothing to copy", store.display());
        }
        Err(err) => return Err(err).with_context(|| format!("cannot read {}", store.display())),
    }
    for name in [SESSION_FILE, MARKS_FILE] {
        let src = state_dir.join(name);
        if src.is_file() {
            bytes += fs::copy(&src, tmp.join(name))
                .with_context(|| format!("cannot copy {}", src.display()))?;
            files += 1;
        }
    }
    fs::rename(&tmp, &target)
        .with_context(|| format!("cannot move the copy to {}", target.display()))?;
    let removed = prune(to, keep)?;
    info!(
        files,
        bytes,
        removed,
        "store copied to {}",
        target.display()
    );
    Ok(target)
}

/// One consistent snapshot of a live database into a new file.
fn copy_database(src: &Path, dest: &Path) -> Result<()> {
    let conn = Connection::open(src).with_context(|| format!("cannot open {}", src.display()))?;
    conn.busy_timeout(Duration::from_secs(60))?;
    let dest = dest.to_str().context("the destination path is not UTF-8")?;
    conn.execute("VACUUM INTO ?1", [dest])
        .with_context(|| format!("VACUUM INTO {dest} failed for {}", src.display()))?;
    Ok(())
}

/// Remove the oldest copies beyond `keep` and any leftover temporary directory.
/// Returns how many directories went.
fn prune(to: &Path, keep: usize) -> Result<usize> {
    let mut copies = Vec::new();
    let mut removed = 0;
    for entry in fs::read_dir(to)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') && name.ends_with(".tmp") {
            fs::remove_dir_all(entry.path())?;
            removed += 1;
        } else if is_stamp(&name) {
            copies.push(name);
        }
    }
    copies.sort();
    while copies.len() > keep {
        let oldest = copies.remove(0);
        fs::remove_dir_all(to.join(&oldest))
            .with_context(|| format!("cannot remove the old copy {oldest}"))?;
        removed += 1;
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("siltad-backup-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn stamps_are_sortable_file_names() {
        assert_eq!(stamp(1_788_631_331_999), "2026-09-05T18-02-11Z");
        assert!(is_stamp("2026-09-05T18-02-11Z"));
        assert!(!is_stamp("2026-09-05T18:02:11Z"));
        assert!(!is_stamp(".2026-09-05T18-02-11Z.tmp"));
    }

    #[test]
    fn copies_an_open_wal_database_and_the_state_files_and_prunes() {
        let dir = temp("copy");
        let state = dir.join("state");
        let store = state.join(STORE_DIR);
        fs::create_dir_all(&store).unwrap();
        // A database in WAL mode, held open with uncheckpointed writes, as the daemon's are.
        let live = Connection::open(store.join("matrix-sdk-crypto.sqlite3")).unwrap();
        live.execute_batch(
            "PRAGMA journal_mode=WAL; CREATE TABLE keys(id INTEGER PRIMARY KEY, v TEXT);",
        )
        .unwrap();
        for i in 0..50 {
            live.execute("INSERT INTO keys(v) VALUES (?1)", [format!("key-{i}")])
                .unwrap();
        }
        fs::write(state.join(SESSION_FILE), "{\"token\":\"t\"}").unwrap();
        fs::write(state.join(MARKS_FILE), "{}").unwrap();
        fs::write(store.join("not-a-db.txt"), "ignored").unwrap();

        let to = dir.join("backups");
        let first = run(&state, &to, 2, "2026-09-06T01-00-00Z").unwrap();
        assert_eq!(first, to.join("2026-09-06T01-00-00Z"));
        let copy = Connection::open(first.join("matrix-sdk-crypto.sqlite3")).unwrap();
        let n: i64 = copy
            .query_row("SELECT count(*) FROM keys", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 50);
        assert_eq!(
            fs::read_to_string(first.join(SESSION_FILE)).unwrap(),
            "{\"token\":\"t\"}"
        );
        assert!(first.join(MARKS_FILE).is_file());
        assert!(!first.join("not-a-db.txt").exists());
        // The live database is untouched and still writable.
        live.execute("INSERT INTO keys(v) VALUES ('later')", [])
            .unwrap();

        // A leftover temporary directory and the oldest copies beyond `keep` go.
        fs::create_dir(to.join(".2026-09-06T00-30-00Z.tmp")).unwrap();
        run(&state, &to, 2, "2026-09-06T02-00-00Z").unwrap();
        run(&state, &to, 2, "2026-09-06T03-00-00Z").unwrap();
        let mut left: Vec<_> = fs::read_dir(&to)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        left.sort();
        assert_eq!(left, vec!["2026-09-06T02-00-00Z", "2026-09-06T03-00-00Z"]);
        // The same stamp twice is refused rather than overwritten.
        assert!(run(&state, &to, 2, "2026-09-06T03-00-00Z").is_err());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_missing_store_is_not_an_error() {
        let dir = temp("empty");
        let state = dir.join("state");
        fs::create_dir_all(&state).unwrap();
        let copy = run(&state, &dir.join("backups"), 7, "2026-09-06T01-00-00Z").unwrap();
        assert!(copy.is_dir());
        assert_eq!(fs::read_dir(&copy).unwrap().count(), 0);
        assert!(run(&state, &dir.join("backups"), 7, "not a stamp").is_err());
        fs::remove_dir_all(&dir).unwrap();
    }
}
