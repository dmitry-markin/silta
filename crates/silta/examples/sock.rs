//! Development tool: pipe stdin/stdout lines to a Unix socket.
//!
//! `sock listen <path>` binds the socket and serves one connection at a time (a fake
//! daemon for testing a plugin). `sock connect <path>` connects to it (a fake plugin
//! for testing the daemon). Lines from stdin go to the peer; lines from the peer go to
//! stdout; peer events are reported on stderr.

use std::{path::PathBuf, process::ExitCode};

use silta::line::LineReader;
use tokio::{
    io::{AsyncWriteExt, Stdin},
    net::{UnixListener, UnixStream},
};

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let (mode, path) = match args.as_slice() {
        [_, mode, path] if mode == "listen" || mode == "connect" => (mode.as_str(), PathBuf::from(path)),
        _ => {
            eprintln!("usage: sock listen <socket-path> | sock connect <socket-path>");
            return ExitCode::from(2);
        }
    };
    let mut stdin = LineReader::new(tokio::io::stdin());
    let result = match mode {
        "listen" => listen(&path, &mut stdin).await,
        _ => connect(&path, &mut stdin).await,
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("sock: {err}");
            ExitCode::FAILURE
        }
    }
}

async fn listen(path: &PathBuf, stdin: &mut LineReader<Stdin>) -> anyhow::Result<()> {
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    let listener = UnixListener::bind(path)?;
    eprintln!("sock: listening on {}", path.display());
    loop {
        let (stream, _) = listener.accept().await?;
        eprintln!("sock: peer connected");
        if !pump(stream, stdin).await? {
            break;
        }
        eprintln!("sock: peer closed, waiting for the next connection");
    }
    let _ = std::fs::remove_file(path);
    Ok(())
}

async fn connect(path: &PathBuf, stdin: &mut LineReader<Stdin>) -> anyhow::Result<()> {
    let stream = UnixStream::connect(path).await?;
    eprintln!("sock: connected to {}", path.display());
    pump(stream, stdin).await?;
    Ok(())
}

/// Returns `Ok(true)` when the peer closed while stdin is still open, `Ok(false)` once
/// stdin has closed (the peer is drained until it closes too, so a piped one-shot
/// `printf ... | sock connect` still prints the answers).
async fn pump(stream: UnixStream, stdin: &mut LineReader<Stdin>) -> anyhow::Result<bool> {
    let (read_half, mut write_half) = stream.into_split();
    let mut peer = LineReader::new(read_half);
    let mut stdout = tokio::io::stdout();
    let mut stdin_open = true;
    loop {
        tokio::select! {
            line = peer.next_line() => match line? {
                Some(line) => {
                    stdout.write_all(line.as_bytes()).await?;
                    stdout.write_all(b"\n").await?;
                    stdout.flush().await?;
                }
                None => return Ok(stdin_open),
            },
            line = stdin.next_line(), if stdin_open => match line? {
                Some(line) => {
                    write_half.write_all(line.as_bytes()).await?;
                    write_half.write_all(b"\n").await?;
                    write_half.flush().await?;
                }
                None => {
                    let _ = write_half.shutdown().await;
                    eprintln!("sock: stdin closed, draining the peer");
                    stdin_open = false;
                }
            },
        }
    }
}
