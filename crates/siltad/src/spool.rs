//! The daemon's spool: an attachment downloaded from a room waits in `inbox/` until
//! its session takes it over the socket; a file arriving from a session waits in
//! `outbox/` until it is uploaded. A file is deleted once used; leftovers (a session
//! that never came back, a crash) are swept after a day.

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::Context;
use matrix_sdk::{
    media::{MediaFormat, MediaRequestParameters},
    ruma::events::room::MediaSource,
    Client,
};
use silta::transfer::sweep;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

const SWEEP_EVERY: Duration = Duration::from_secs(24 * 3600);
const LEFTOVER_MAX_AGE: Duration = Duration::from_secs(24 * 3600);

#[derive(Debug, Clone)]
pub struct Spool {
    pub inbox: PathBuf,
    pub outbox: PathBuf,
    /// The cap on one attachment in either direction.
    pub max_bytes: u64,
}

impl Spool {
    pub fn new(state_dir: &Path, max_bytes: u64) -> Spool {
        Spool {
            inbox: state_dir.join("inbox"),
            outbox: state_dir.join("outbox"),
            max_bytes,
        }
    }

    /// Create both directories, readable by the daemon's user only.
    pub fn prepare(&self) -> anyhow::Result<()> {
        for dir in [&self.inbox, &self.outbox] {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("cannot create {}", dir.display()))?;
            std::fs::set_permissions(dir, std::os::unix::fs::PermissionsExt::from_mode(0o700))?;
        }
        Ok(())
    }

    /// Where a downloaded attachment waits, named by its transfer id.
    pub fn inbox_path(&self, transfer: &str) -> PathBuf {
        self.inbox.join(transfer)
    }
}

pub enum DownloadError {
    /// The content is larger than the cap (its size in bytes).
    TooLarge(u64),
    Failed(anyhow::Error),
}

/// Download (and decrypt) an attachment into `path`, mode 0600. The content is
/// checked against `max_bytes` before anything is written.
pub async fn download(
    client: &Client,
    source: MediaSource,
    path: &Path,
    max_bytes: u64,
) -> Result<u64, DownloadError> {
    let request = MediaRequestParameters {
        source,
        format: MediaFormat::File,
    };
    let data = client
        .media()
        .get_media_content(&request, false)
        .await
        .map_err(|e| DownloadError::Failed(e.into()))?;
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
        file.write_all(&data)
            .await
            .with_context(|| format!("cannot write {}", path.display()))?;
        file.flush().await?;
        anyhow::Ok(())
    };
    write.await.map_err(DownloadError::Failed)?;
    Ok(len)
}

/// Sweep leftovers at start and once a day until cancelled.
pub fn spawn_sweeper(spool: Spool, cancel: CancellationToken) {
    tokio::spawn(async move {
        loop {
            for dir in [&spool.inbox, &spool.outbox] {
                match sweep(dir, LEFTOVER_MAX_AGE).await {
                    Ok(0) => debug!("spool sweep of {}: nothing to remove", dir.display()),
                    Ok(removed) => info!(
                        removed,
                        "spool sweep of {}: removed leftovers older than a day",
                        dir.display()
                    ),
                    Err(err) => warn!("spool sweep of {} failed: {err:#}", dir.display()),
                }
            }
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(SWEEP_EVERY) => {}
            }
        }
    });
}
