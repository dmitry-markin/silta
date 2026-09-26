//! The stdio transport, with the 2026-07-28 discover probe answered before rmcp sees it.
//!
//! Claude Code opens with `server/discover` when it is instructed so by a remote flag for
//! the account (or with `MCP_PROTOCOL_NEGOTIATION=auto`), and falls back to `initialize`
//! when the server rejects the 2026-07-28 revision. rmcp (3.2 to 3.4) takes a discover as
//! the first request for the start of an inline-lifecycle session and keeps requiring the
//! per-request `_meta` after the fallback `initialize`: the plain `tools/list` that
//! follows is refused and the session has no tools. Answering the probe here, with the
//! error rmcp itself would send, makes `initialize` the first request rmcp sees, and the
//! session stays on the legacy lifecycle the channel needs.
//!
//! TODO: temporary, a workaround for rmcp. Remove this module (and serve rmcp's own
//! `stdio()` again) once an rmcp release, after a rejected first `server/discover`,
//! serves a following `initialize` session without per-request `_meta`, and answers a
//! discover lacking `_meta` with an error instead of ending the server. The test below,
//! without the `LegacyOnly` wrapping, checks the first. Keep `PROTOCOL_VERSIONS` at
//! 2024-11-05 regardless, as long as Claude Code registers channels only on legacy
//! connections. When silta-claude moves to 2026-07-28, this module must go in any case:
//! it declines every discover, so the connection would never leave the legacy protocol.

use std::future::Future;

use rmcp::{
    model::{
        ClientJsonRpcMessage, ClientRequest, ErrorData, GetMeta, ProtocolVersion,
        ServerJsonRpcMessage,
    },
    service::RoleServer,
    transport::{async_rw::AsyncRwTransport, Transport},
};
use tokio::io::{Stdin, Stdout};
use tracing::{debug, warn};

use crate::mcp::PROTOCOL_VERSIONS;

/// The MCP transport over stdin and stdout.
pub fn stdio() -> LegacyOnly<AsyncRwTransport<RoleServer, Stdin, Stdout>> {
    LegacyOnly(AsyncRwTransport::new_server(
        tokio::io::stdin(),
        tokio::io::stdout(),
    ))
}

/// Answers every `server/discover` itself with "unsupported protocol version" and passes
/// everything else through.
pub struct LegacyOnly<T>(pub T);

impl<T: Transport<RoleServer>> Transport<RoleServer> for LegacyOnly<T> {
    type Error = T::Error;

    fn send(
        &mut self,
        item: ServerJsonRpcMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        self.0.send(item)
    }

    async fn receive(&mut self) -> Option<ClientJsonRpcMessage> {
        loop {
            let message = self.0.receive().await?;
            let ClientJsonRpcMessage::Request(request) = &message else {
                return Some(message);
            };
            if !matches!(request.request, ClientRequest::DiscoverRequest(_)) {
                return Some(message);
            }
            let requested = request
                .request
                .get_meta()
                .protocol_version()
                .unwrap_or(ProtocolVersion::V_2026_07_28);
            debug!(%requested, "declining server/discover, only the initialize handshake is served");
            let error = ErrorData::unsupported_protocol_version(requested, PROTOCOL_VERSIONS);
            if let Err(err) = self
                .0
                .send(ServerJsonRpcMessage::error(error, Some(request.id.clone())))
                .await
            {
                warn!("cannot answer server/discover: {err}");
                return None;
            }
        }
    }

    fn close(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.0.close()
    }
}

#[cfg(test)]
mod tests {
    use rmcp::ServiceExt;
    use serde_json::Value;
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        sync::{mpsc, oneshot},
    };
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::{daemon::DaemonClient, mcp::SiltaChannel};

    /// Claude Code 2.1.283's opening, trimmed: the discover probe, the fallback
    /// `initialize`, then `tools/list` without `_meta`.
    const OPENING: &str = r#"{"jsonrpc":"2.0","id":"server-discover-probe-1","method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientInfo":{"name":"claude-code","version":"2.1.283"},"io.modelcontextprotocol/clientCapabilities":{"roots":{"listChanged":true}}}}}
{"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{"roots":{"listChanged":true}},"clientInfo":{"name":"claude-code","version":"2.1.283"}},"jsonrpc":"2.0","id":0}
{"jsonrpc":"2.0","method":"notifications/initialized"}
{"method":"tools/list","jsonrpc":"2.0","id":1}
"#;

    #[tokio::test]
    async fn a_declined_discover_leaves_a_legacy_session_with_tools() {
        let (client, server) = tokio::io::duplex(1 << 16);
        let (server_read, server_write) = tokio::io::split(server);
        let (client_read, mut client_write) = tokio::io::split(client);

        // The ready file never appears, so the plugin never contacts a daemon.
        let never = std::env::temp_dir().join(format!("silta-claude-never-{}", std::process::id()));
        let cancel = CancellationToken::new();
        let (events_tx, events_rx) = mpsc::channel(1);
        let (ready_tx, ready_rx) = oneshot::channel();
        let daemon = DaemonClient::start(
            never.clone(),
            "test".to_owned(),
            never.clone(),
            Some(never),
            events_tx,
            ready_rx,
            cancel.clone(),
        );
        let handler = SiltaChannel::new(daemon, events_rx, ready_tx);

        client_write.write_all(OPENING.as_bytes()).await.unwrap();
        let service = handler
            .serve(LegacyOnly(AsyncRwTransport::new_server(
                server_read,
                server_write,
            )))
            .await
            .unwrap();

        let mut lines = BufReader::new(client_read).lines();
        let mut next = async || -> Value {
            let line = tokio::time::timeout(std::time::Duration::from_secs(5), lines.next_line())
                .await
                .expect("no answer within 5 s")
                .unwrap()
                .unwrap();
            serde_json::from_str(&line).unwrap()
        };

        let discover = next().await;
        assert_eq!(discover["id"], "server-discover-probe-1");
        assert_eq!(discover["error"]["code"], -32022);

        let initialize = next().await;
        assert_eq!(initialize["id"], 0);
        assert_eq!(initialize["result"]["protocolVersion"], "2024-11-05");
        assert!(initialize["result"]["capabilities"]["experimental"]["claude/channel"].is_object());

        let tools = next().await;
        assert_eq!(tools["id"], 1);
        let names: Vec<&str> = tools["result"]["tools"]
            .as_array()
            .unwrap_or_else(|| panic!("tools/list failed: {tools}"))
            .iter()
            .filter_map(|tool| tool["name"].as_str())
            .collect();
        assert!(names.contains(&"reply"), "no reply tool in {names:?}");

        cancel.cancel();
        let _ = service.cancel().await;
    }
}
