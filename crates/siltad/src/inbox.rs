//! The inbox: attachments downloaded from rooms, under safe names, with a retention
//! sweep.

use std::{
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use anyhow::Context;
use matrix_sdk::{
    media::{MediaFormat, MediaRequestParameters},
    ruma::events::room::MediaSource,
    Client,
};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Longest file name the inbox writes, in bytes, event id prefix aside.
const NAME_MAX: usize = 100;
const SWEEP_EVERY: Duration = Duration::from_secs(24 * 3600);

#[derive(Debug, Clone)]
pub struct InboxConfig {
    pub dir: PathBuf,
    pub max_age: Duration,
    pub max_bytes: u64,
}

pub enum DownloadError {
    /// The content is larger than the cap (its size in bytes).
    TooLarge(u64),
    Failed(anyhow::Error),
}

/// Create the inbox directory, readable by the daemon's user only for now.
pub fn prepare(dir: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(dir).with_context(|| format!("cannot create {}", dir.display()))?;
    std::fs::set_permissions(dir, std::os::unix::fs::PermissionsExt::from_mode(0o700))?;
    Ok(())
}

/// A file name for an attachment of an event: the event id (made path-safe) and the
/// sender's name reduced to a safe basename, so names are unique and deterministic.
pub fn path_for(dir: &Path, event_id: &str, name: &str, mime: &str) -> PathBuf {
    dir.join(format!("{}-{}", safe_event_id(event_id), safe_name(name, mime)))
}

fn safe_event_id(event_id: &str) -> String {
    event_id
        .trim_start_matches('$')
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect()
}

/// The last path component of an untrusted name with separators and control
/// characters replaced, no leading dots, at most [`NAME_MAX`] bytes; `file` with an
/// extension from the mime type when nothing usable is left.
pub fn safe_name(name: &str, mime: &str) -> String {
    let base = name.rsplit(['/', '\\']).next().unwrap_or("");
    let mut out: String = base
        .chars()
        .map(|c| if c.is_control() || matches!(c, '/' | '\\' | ':') { '_' } else { c })
        .collect();
    out = out.trim().trim_start_matches('.').to_owned();
    while out.len() > NAME_MAX {
        out.pop();
    }
    if out.is_empty() {
        let ext = mime_guess::get_mime_extensions_str(mime).and_then(|e| e.first()).map(|e| format!(".{e}")).unwrap_or_default();
        return format!("file{ext}");
    }
    out
}

/// Download (and decrypt) an attachment into `path`, mode 0600. The content is
/// checked against `max_bytes` before anything is written.
pub async fn download(client: &Client, source: MediaSource, path: &Path, max_bytes: u64) -> Result<u64, DownloadError> {
    let request = MediaRequestParameters { source, format: MediaFormat::File };
    let data = client.media().get_media_content(&request, false).await.map_err(|e| DownloadError::Failed(e.into()))?;
    let len = data.len() as u64;
    if len > max_bytes {
        return Err(DownloadError::TooLarge(len));
    }
    let write = async {
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)
            .await
            .with_context(|| format!("cannot create {}", path.display()))?;
        file.write_all(&data).await.with_context(|| format!("cannot write {}", path.display()))?;
        file.flush().await?;
        anyhow::Ok(())
    };
    write.await.map_err(DownloadError::Failed)?;
    Ok(len)
}

/// Delete inbox files older than `max_age`. Returns how many were removed.
pub async fn sweep(dir: &Path, max_age: Duration) -> anyhow::Result<usize> {
    let cutoff = SystemTime::now().checked_sub(max_age).unwrap_or(SystemTime::UNIX_EPOCH);
    let mut removed = 0;
    let mut entries = tokio::fs::read_dir(dir).await.with_context(|| format!("cannot list {}", dir.display()))?;
    while let Some(entry) = entries.next_entry().await? {
        let meta = entry.metadata().await?;
        if !meta.is_file() {
            continue;
        }
        if meta.modified()? < cutoff {
            match tokio::fs::remove_file(entry.path()).await {
                Ok(()) => removed += 1,
                Err(err) => warn!("cannot remove {}: {err}", entry.path().display()),
            }
        }
    }
    Ok(removed)
}

/// Sweep at start and once a day until cancelled.
pub fn spawn_sweeper(dir: PathBuf, max_age: Duration, cancel: CancellationToken) {
    tokio::spawn(async move {
        loop {
            match sweep(&dir, max_age).await {
                Ok(0) => debug!("inbox sweep: nothing to remove"),
                Ok(removed) => info!(removed, "inbox sweep: removed files older than {} days", max_age.as_secs() / 86400),
                Err(err) => warn!("inbox sweep failed: {err:#}"),
            }
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(SWEEP_EVERY) => {}
            }
        }
    });
}

pub fn human_size(bytes: u64) -> String {
    const K: f64 = 1024.0;
    let b = bytes as f64;
    if bytes < 1024 {
        format!("{bytes} B")
    } else if b < K * K {
        format!("{:.0} KB", b / K)
    } else if b < K * K * K {
        format!("{:.1} MB", b / K / K)
    } else {
        format!("{:.2} GB", b / K / K / K)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_reduced_to_a_safe_basename() {
        assert_eq!(safe_name("photo.jpg", "image/jpeg"), "photo.jpg");
        assert_eq!(safe_name("../../etc/passwd", "text/plain"), "passwd");
        assert_eq!(safe_name("C:\\Users\\x\\report.pdf", "application/pdf"), "report.pdf");
        assert_eq!(safe_name(".hidden", "text/plain"), "hidden");
        assert_eq!(safe_name("a\u{0}b\nc", "text/plain"), "a_b_c");
        assert_eq!(safe_name("kesä kuva ö.png", "image/png"), "kesä kuva ö.png");
        assert_eq!(safe_name("", "image/png"), "file.png");
        assert_eq!(safe_name("..", "application/x-unknown-thing"), "file");
        let long = safe_name(&"ж".repeat(200), "text/plain");
        assert!(long.len() <= NAME_MAX && long.chars().all(|c| c == 'ж'));
    }

    #[test]
    fn paths_are_unique_per_event() {
        let dir = Path::new("/var/lib/silta/inbox");
        assert_eq!(
            path_for(dir, "$abc-DEF_123:localhost", "a.png", "image/png"),
            dir.join("abc-DEF_123_localhost-a.png")
        );
        assert_ne!(path_for(dir, "$one", "a.png", "image/png"), path_for(dir, "$two", "a.png", "image/png"));
    }

    #[test]
    fn sizes_read_well() {
        assert_eq!(human_size(12), "12 B");
        assert_eq!(human_size(345 * 1024), "345 KB");
        assert_eq!(human_size(1_300_000), "1.2 MB");
        assert_eq!(human_size(100 * 1024 * 1024), "100.0 MB");
    }
}
