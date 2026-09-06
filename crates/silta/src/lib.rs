//! Shared pieces of the silta bridge: the socket protocol between `siltad` and its
//! session plugins, the daemon configuration with its routing rules, JSON-lines
//! framing, files over the socket, and small text helpers. Everything here is pure and unit-tested; the
//! network and Matrix code lives in the binaries.

pub mod alert;
pub mod backlog;
pub mod config;
pub mod line;
pub mod protocol;
pub mod replay;
pub mod text;
pub mod time;
pub mod transfer;
