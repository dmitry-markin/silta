# silta-claude plugin

Channel plugin that connects a Claude Code session to the `siltad` daemon over its Unix
socket. The MCP server is the `silta-claude` binary from `crates/silta-claude`; it takes no
arguments and reads its configuration from the session's environment:

| Variable | Required | Meaning |
|---|---|---|
| `SILTA_SESSION` | yes | Session name as configured in `siltad.toml` (`[[sessions]] name`) |
| `SILTA_SOCKET` | no | Daemon socket, default `/run/silta/siltad.sock` |
| `SILTA_CLAUDE_BIN` | no | Path of the binary; default `silta-claude` on `PATH` (`cargo install --path silta/silta-claude`) |
| `RUST_LOG` | no | Log filter, default `info`; the log goes to stderr, which Claude Code keeps in `~/.claude/debug/<session-id>.txt` |

Install from the repository's local marketplace and run a session with the channel:

    claude plugin marketplace add /path/to/silta/assistant/marketplace   # once, registers silta-local
    claude plugin install silta-claude@silta-local
    SILTA_SESSION=hub claude --channels plugin:silta-claude@silta-local

The plugin must be on the machine's channel allowlist (`assistant/managed-settings.json`).
The plugin holds no Matrix credentials; the daemon decides which rooms the session sees
and may write to.
