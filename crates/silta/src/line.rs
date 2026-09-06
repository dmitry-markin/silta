//! JSON-lines framing over tokio streams with a hard line cap.

use std::io;

use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};

use crate::protocol::MAX_LINE_BYTES;

#[derive(Debug, Error)]
pub enum LineError {
    #[error("line exceeds {max} bytes")]
    TooLong { max: usize },
    #[error("line is not valid UTF-8")]
    Utf8,
    #[error("line is not valid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Reads `\n`-terminated lines, rejecting any longer than the cap before buffering it.
pub struct LineReader<R> {
    inner: BufReader<R>,
    buf: Vec<u8>,
    max: usize,
}

impl<R: AsyncRead + Unpin> LineReader<R> {
    pub fn new(reader: R) -> Self {
        Self::with_max(reader, MAX_LINE_BYTES)
    }

    pub fn with_max(reader: R, max: usize) -> Self {
        LineReader { inner: BufReader::new(reader), buf: Vec::new(), max }
    }

    /// The next line without its terminator, `None` at EOF. A final unterminated line
    /// is returned as a line.
    ///
    /// Cancellation safe: the callers poll this inside `select!`, and a partial line
    /// read before the future was dropped is kept for the next call.
    pub async fn next_line(&mut self) -> Result<Option<String>, LineError> {
        loop {
            let available = self.inner.fill_buf().await?;
            if available.is_empty() {
                if self.buf.is_empty() {
                    return Ok(None);
                }
                return finish(std::mem::take(&mut self.buf)).map(Some);
            }
            match available.iter().position(|&b| b == b'\n') {
                Some(i) => {
                    if self.buf.len() + i > self.max {
                        return Err(LineError::TooLong { max: self.max });
                    }
                    self.buf.extend_from_slice(&available[..i]);
                    self.inner.consume(i + 1);
                    return finish(std::mem::take(&mut self.buf)).map(Some);
                }
                None => {
                    let n = available.len();
                    if self.buf.len() + n > self.max {
                        return Err(LineError::TooLong { max: self.max });
                    }
                    self.buf.extend_from_slice(available);
                    self.inner.consume(n);
                }
            }
        }
    }

    /// The next line parsed as JSON.
    pub async fn next_json<T: DeserializeOwned>(&mut self) -> Result<Option<T>, LineError> {
        match self.next_line().await? {
            Some(line) => Ok(Some(serde_json::from_str(&line)?)),
            None => Ok(None),
        }
    }
}

fn finish(mut bytes: Vec<u8>) -> Result<String, LineError> {
    if bytes.last() == Some(&b'\r') {
        bytes.pop();
    }
    String::from_utf8(bytes).map_err(|_| LineError::Utf8)
}

/// Serialize one message as a single JSON line and flush it.
pub async fn write_line<W: AsyncWrite + Unpin, T: Serialize + ?Sized>(
    writer: &mut W,
    message: &T,
) -> io::Result<()> {
    let mut bytes = serde_json::to_vec(message)?;
    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    writer.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reads_lines_and_handles_eof() {
        let input = b"first\r\nsecond\nthird".as_slice();
        let mut r = LineReader::new(input);
        assert_eq!(r.next_line().await.unwrap().as_deref(), Some("first"));
        assert_eq!(r.next_line().await.unwrap().as_deref(), Some("second"));
        assert_eq!(r.next_line().await.unwrap().as_deref(), Some("third"));
        assert_eq!(r.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn rejects_over_long_lines() {
        let input = b"0123456789abcdef\nshort\n".as_slice();
        let mut r = LineReader::with_max(input, 8);
        assert!(matches!(r.next_line().await, Err(LineError::TooLong { max: 8 })));

        // Exactly the cap is fine.
        let input = b"12345678\n".as_slice();
        let mut r = LineReader::with_max(input, 8);
        assert_eq!(r.next_line().await.unwrap().as_deref(), Some("12345678"));
    }

    #[tokio::test]
    async fn keeps_a_partial_line_across_a_dropped_future() {
        let (mut client, server) = tokio::io::duplex(64);
        let mut r = LineReader::new(server);
        client.write_all(b"hel").await.unwrap();
        // The first attempt reads "hel", then waits for more and is dropped by the timeout.
        assert!(tokio::time::timeout(std::time::Duration::from_millis(50), r.next_line()).await.is_err());
        client.write_all(b"lo\nnext\n").await.unwrap();
        assert_eq!(r.next_line().await.unwrap().as_deref(), Some("hello"));
        assert_eq!(r.next_line().await.unwrap().as_deref(), Some("next"));
    }

    #[tokio::test]
    async fn json_roundtrip() {
        let mut out = Vec::new();
        write_line(&mut out, &serde_json::json!({"a": "line\nbreak"})).await.unwrap();
        assert_eq!(out.iter().filter(|&&b| b == b'\n').count(), 1);
        let mut r = LineReader::new(out.as_slice());
        let v: serde_json::Value = r.next_json().await.unwrap().unwrap();
        assert_eq!(v["a"], "line\nbreak");
        assert!(r.next_json::<serde_json::Value>().await.unwrap().is_none());

        let mut r = LineReader::new(b"not json\n".as_slice());
        assert!(matches!(r.next_json::<serde_json::Value>().await, Err(LineError::Json(_))));
    }
}
