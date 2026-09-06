# silta-claude plugin

Channel plugin that connects a Claude Code session to the `siltad` daemon over its Unix
socket. The MCP server is the `silta-claude` binary from `crates/silta-claude`; it takes no
arguments and reads its configuration from the session's environment:

| Variable | Required | Meaning |
|---|---|---|
| `SILTA_SESSION` | yes | Session name as configured in `siltad.toml` (`[[sessions]] name`) |
| `SILTA_SOCKET` | no | Daemon socket, default `/run/silta/siltad.sock` |
| `SILTA_CLAUDE_BIN` | no | Path of the binary; default `silta-claude` on `PATH` (`cargo install --path silta/silta-claude`) |
| `RUST_LOG` | no | Log filter, default `info`; the log goes to stderr, which Claude Code keeps under `~/.cache/claude-cli-nodejs/<cwd-slug>/mcp-logs-plugin-silta-claude-silta/` |

Install from the repository's local marketplace, at project scope inside the session's
workspace, and run the session from there with the channel:

    claude plugin marketplace add /path/to/silta/assistant/marketplace   # once, registers silta-local
    cd /path/to/workspace && claude plugin install silta-claude@silta-local --scope project
    SILTA_SESSION=hub claude --add-dir /var/lib/silta/inbox --channels plugin:silta-claude@silta-local

Project scope matters: a user-scope install makes every Claude Code session on the host
try to start the plugin, fail without `SILTA_SESSION`, and poison
`~/.claude/mcp-needs-auth-cache.json`. `--add-dir` names the daemon's inbox, where
attachments are downloaded, so the session may read them.

Tools: `reply`, `react`, `edit_message`, `send_file`, `fetch_messages`, `fetch_message`,
`search_messages`. Events: messages
(with attachments, replies and threads) and reactions on the bot's own messages.

The plugin must be on the machine's channel allowlist (`assistant/managed-settings.json`).
The plugin holds no Matrix credentials; the daemon decides which rooms the session sees
and may write to.
