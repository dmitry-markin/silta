//! Files over the socket, the same in both directions: a `file` header, `chunk` lines
//! of [`CHUNK_BYTES`] of base64 each, a `file_end`, then the event or command that
//! names the transfer. Both sides stream to disk, so memory is bounded by the chunk
//! size whatever the file size. Also the safe file names and the retention sweep the
//! daemon's spool and a session's inbox share.

use std::{
    collections::HashMap,
    io,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use serde::Serialize;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{
    line::write_line,
    protocol::{FileChunk, FileEnd, FileHeader},
};

/// Bytes of file per `chunk` line (1.4 MB of base64), well under the line cap.
pub const CHUNK_BYTES: usize = 1024 * 1024;
/// Longest file name written, in bytes, transfer prefix aside.
const NAME_MAX: usize = 100;

/// One line of a transfer, wrapped by each side into its own message type.
pub enum Piece {
    Header(FileHeader),
    Chunk(FileChunk),
    End(FileEnd),
}

/// Stream the file at `path` as a transfer. `header.size` is what the receiver checks
/// the byte count against, so it must be the file's size.
pub async fn send<W, M>(writer: &mut W, header: FileHeader, path: &Path, wrap: impl Fn(Piece) -> M) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
    M: Serialize,
{
    let mut file = tokio::fs::File::open(path).await?;
    let transfer = header.transfer.clone();
    write_line(writer, &wrap(Piece::Header(header))).await?;
    let mut buf = vec![0u8; CHUNK_BYTES];
    loop {
        let n = read_chunk(&mut file, &mut buf).await?;
        if n == 0 {
            break;
        }
        let chunk = FileChunk { transfer: transfer.clone(), data: BASE64.encode(&buf[..n]) };
        write_line(writer, &wrap(Piece::Chunk(chunk))).await?;
    }
    write_line(writer, &wrap(Piece::End(FileEnd { transfer }))).await
}

async fn read_chunk(file: &mut tokio::fs::File, buf: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = file.read(&mut buf[filled..]).await?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    Ok(filled)
}

#[derive(Debug, Error)]
pub enum TransferError {
    #[error("transfer {0} is not open")]
    Unknown(String),
    #[error("transfer {0} is already open")]
    Duplicate(String),
    #[error("{name} is {size} bytes, over the {max} byte limit")]
    TooLarge { name: String, size: u64, max: u64 },
    #[error("transfer {transfer}: {received} bytes received for a declared size of {declared}")]
    SizeMismatch { transfer: String, received: u64, declared: u64 },
    #[error("transfer {0}: a chunk is not base64: {1}")]
    Base64(String, base64::DecodeError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// A transfer that arrived whole.
#[derive(Debug)]
pub struct Received {
    pub header: FileHeader,
    pub path: PathBuf,
}

/// The receiving side of one connection: transfers in progress, written into `dir`
/// as `<prefix><transfer>-<safe name>` with mode 0600. A failed transfer is removed
/// and forgotten.
pub struct Receiver {
    dir: PathBuf,
    prefix: String,
    max_bytes: u64,
    open: HashMap<String, Open>,
}

struct Open {
    header: FileHeader,
    file: tokio::fs::File,
    path: PathBuf,
    received: u64,
}

impl Receiver {
    pub fn new(dir: PathBuf, prefix: impl Into<String>, max_bytes: u64) -> Self {
        Receiver { dir, prefix: prefix.into(), max_bytes, open: HashMap::new() }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Where a transfer lands: deterministic, so the sender's name cannot choose it.
    pub fn path_for(&self, header: &FileHeader) -> PathBuf {
        self.dir.join(format!("{}{}-{}", self.prefix, safe_id(&header.transfer), safe_name(&header.name, &header.mime)))
    }

    pub async fn begin(&mut self, header: FileHeader) -> Result<(), TransferError> {
        if self.open.contains_key(&header.transfer) {
            return Err(TransferError::Duplicate(header.transfer));
        }
        if header.size > self.max_bytes {
            return Err(TransferError::TooLarge { name: header.name, size: header.size, max: self.max_bytes });
        }
        tokio::fs::create_dir_all(&self.dir).await?;
        let path = self.path_for(&header);
        let file = tokio::fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(&path).await?;
        self.open.insert(header.transfer.clone(), Open { header, file, path, received: 0 });
        Ok(())
    }

    pub async fn chunk(&mut self, chunk: FileChunk) -> Result<(), TransferError> {
        let transfer = chunk.transfer;
        if !self.open.contains_key(&transfer) {
            return Err(TransferError::Unknown(transfer));
        }
        let bytes = match BASE64.decode(chunk.data) {
            Ok(bytes) => bytes,
            Err(err) => {
                self.discard(&transfer).await;
                return Err(TransferError::Base64(transfer, err));
            }
        };
        let open = self.open.get_mut(&transfer).expect("checked above");
        let received = open.received + bytes.len() as u64;
        let declared = open.header.size;
        if received > declared {
            self.discard(&transfer).await;
            return Err(TransferError::SizeMismatch { transfer, received, declared });
        }
        if let Err(err) = open.file.write_all(&bytes).await {
            self.discard(&transfer).await;
            return Err(err.into());
        }
        open.received = received;
        Ok(())
    }

    pub async fn end(&mut self, end: FileEnd) -> Result<Received, TransferError> {
        let Some(mut open) = self.open.remove(&end.transfer) else {
            return Err(TransferError::Unknown(end.transfer));
        };
        if let Err(err) = open.file.flush().await {
            let _ = tokio::fs::remove_file(&open.path).await;
            return Err(err.into());
        }
        drop(open.file);
        if open.received != open.header.size {
            let _ = tokio::fs::remove_file(&open.path).await;
            return Err(TransferError::SizeMismatch { transfer: end.transfer, received: open.received, declared: open.header.size });
        }
        Ok(Received { header: open.header, path: open.path })
    }

    /// Forget every transfer in progress and delete its partial file (the connection
    /// is gone).
    pub async fn abort_all(&mut self) {
        let transfers: Vec<String> = self.open.keys().cloned().collect();
        for transfer in transfers {
            self.discard(&transfer).await;
        }
    }

    async fn discard(&mut self, transfer: &str) {
        if let Some(open) = self.open.remove(transfer) {
            drop(open.file);
            let _ = tokio::fs::remove_file(&open.path).await;
        }
    }
}

/// A transfer or event id reduced to `[A-Za-z0-9_-]`, without a leading `$`.
pub fn safe_id(id: &str) -> String {
    id.trim_start_matches('$')
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

/// Delete the regular files in `dir` older than `max_age`. Returns how many were
/// removed; a missing directory counts as empty.
pub async fn sweep(dir: &Path, max_age: Duration) -> io::Result<usize> {
    let cutoff = SystemTime::now().checked_sub(max_age).unwrap_or(SystemTime::UNIX_EPOCH);
    let mut removed = 0;
    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(err),
    };
    while let Some(entry) = entries.next_entry().await? {
        let meta = entry.metadata().await?;
        if meta.is_file() && meta.modified()? < cutoff && tokio::fs::remove_file(entry.path()).await.is_ok() {
            removed += 1;
        }
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{line::LineReader, protocol::DaemonMessage};
    use std::os::unix::fs::PermissionsExt;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("silta-transfer-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn wrap(piece: Piece) -> DaemonMessage {
        match piece {
            Piece::Header(h) => DaemonMessage::File(h),
            Piece::Chunk(c) => DaemonMessage::Chunk(c),
            Piece::End(e) => DaemonMessage::FileEnd(e),
        }
    }

    async fn feed(receiver: &mut Receiver, lines: &[u8]) -> Result<Vec<Received>, TransferError> {
        let mut reader = LineReader::new(lines);
        let mut done = Vec::new();
        while let Some(message) = reader.next_json::<DaemonMessage>().await.unwrap() {
            match message {
                DaemonMessage::File(h) => receiver.begin(h).await?,
                DaemonMessage::Chunk(c) => receiver.chunk(c).await?,
                DaemonMessage::FileEnd(e) => done.push(receiver.end(e).await?),
                other => panic!("unexpected {other:?}"),
            }
        }
        Ok(done)
    }

    #[tokio::test]
    async fn round_trip_in_chunks() {
        let dir = temp_dir("roundtrip");
        let content: Vec<u8> = (0..(2 * CHUNK_BYTES + 12345)).map(|i| (i % 251) as u8).collect();
        let source = dir.join("source.bin");
        std::fs::write(&source, &content).unwrap();
        let header = FileHeader { transfer: "$ev:x-1".into(), name: "../photo.jpg".into(), mime: "image/jpeg".into(), size: content.len() as u64 };

        let mut lines = Vec::new();
        send(&mut lines, header, &source, wrap).await.unwrap();
        assert_eq!(lines.iter().filter(|&&b| b == b'\n').count(), 5, "header, three chunks, end");

        let mut receiver = Receiver::new(dir.join("inbox"), "", 100 * 1024 * 1024);
        let received = feed(&mut receiver, &lines).await.unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].path, dir.join("inbox").join("ev_x-1-photo.jpg"));
        assert_eq!(std::fs::read(&received[0].path).unwrap(), content);
        assert_eq!(std::fs::metadata(&received[0].path).unwrap().permissions().mode() & 0o777, 0o600);

        // The daemon prefixes its outbox files with the session name.
        let mut receiver = Receiver::new(dir.join("outbox"), "alice-", u64::MAX);
        let received = feed(&mut receiver, &lines).await.unwrap();
        assert_eq!(received[0].path, dir.join("outbox").join("alice-ev_x-1-photo.jpg"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn broken_transfers_leave_nothing_behind() {
        let dir = temp_dir("broken");
        let inbox = dir.join("inbox");
        let header = |size: u64| FileHeader { transfer: "t1".into(), name: "a.bin".into(), mime: "application/octet-stream".into(), size };
        let mut receiver = Receiver::new(inbox.clone(), "", 10);

        assert!(matches!(receiver.begin(header(11)).await, Err(TransferError::TooLarge { max: 10, .. })));
        assert!(matches!(receiver.chunk(FileChunk { transfer: "t1".into(), data: "AQ==".into() }).await, Err(TransferError::Unknown(_))));

        // More bytes than declared: refused at the chunk, the file removed.
        receiver.begin(header(2)).await.unwrap();
        assert!(inbox.join("t1-a.bin").exists());
        assert!(matches!(receiver.chunk(FileChunk { transfer: "t1".into(), data: "AQID".into() }).await, Err(TransferError::SizeMismatch { .. })));
        assert!(!inbox.join("t1-a.bin").exists());

        // Fewer bytes than declared: refused at the end.
        receiver.begin(header(3)).await.unwrap();
        receiver.chunk(FileChunk { transfer: "t1".into(), data: "AQ==".into() }).await.unwrap();
        assert!(matches!(receiver.end(FileEnd { transfer: "t1".into() }).await, Err(TransferError::SizeMismatch { received: 1, declared: 3, .. })));
        assert!(!inbox.join("t1-a.bin").exists());

        // Not base64.
        receiver.begin(header(3)).await.unwrap();
        assert!(matches!(receiver.chunk(FileChunk { transfer: "t1".into(), data: "!!".into() }).await, Err(TransferError::Base64(..))));

        // A connection that drops mid-transfer.
        receiver.begin(header(3)).await.unwrap();
        receiver.abort_all().await;
        assert!(!inbox.join("t1-a.bin").exists());
        assert!(matches!(receiver.end(FileEnd { transfer: "t1".into() }).await, Err(TransferError::Unknown(_))));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn sweep_removes_old_files_only() {
        let dir = temp_dir("sweep");
        std::fs::write(dir.join("old"), b"x").unwrap();
        std::fs::write(dir.join("new"), b"y").unwrap();
        let old = SystemTime::now() - Duration::from_secs(3 * 86_400);
        std::fs::File::open(dir.join("old")).unwrap().set_modified(old).unwrap();
        assert_eq!(sweep(&dir, Duration::from_secs(86_400)).await.unwrap(), 1);
        assert!(!dir.join("old").exists() && dir.join("new").exists());
        assert_eq!(sweep(&dir.join("missing"), Duration::from_secs(1)).await.unwrap(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

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
        assert_eq!(safe_id("$abc-DEF_123:localhost"), "abc-DEF_123_localhost");
    }

    #[test]
    fn sizes_read_well() {
        assert_eq!(human_size(12), "12 B");
        assert_eq!(human_size(345 * 1024), "345 KB");
        assert_eq!(human_size(1_300_000), "1.2 MB");
        assert_eq!(human_size(100 * 1024 * 1024), "100.0 MB");
    }
}
