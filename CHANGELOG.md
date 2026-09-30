# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 0.6.0 - 2026-09-30

This is the first public release of Silta, a family AI assistant running on Claude Code
and speaking Matrix. This release is tested with Claude Code 2.1.283.

Silta remembers tasks, people, and the thread of the conversation across context limits,
so that conversations continue without visible session boundaries. It is designed to be
deployed on a Debian VM, with multiple sessions serving multiple users from the same Matrix
account, and a hub session serving group chats. Installation: see
[docs/deployment.md](https://github.com/dmitry-markin/silta/blob/v0.6.0/docs/deployment.md).
Known issues are listed in the [README](https://github.com/dmitry-markin/silta#known-issues).

### Added

- Supervisor `silta-session` that runs Claude Code for each session. Each person uses their
  own Claude subscription (via `claude setup-token`) or Anthropic API key.
- Channel plugin `silta-claude` with MCP tools for interaction with Matrix rooms.
- Matrix daemon `siltad` that delivers room messages to the sessions and back, supporting
  direct & group rooms and implementing read receipts, typing indicators, reactions, attachments,
  quoting, and threads.
- Custom compaction procedure that makes the assistant remember people, tasks, and conversation
  state across context limits.
- Web search & research, with results delivered as PDFs (including phone-sized rendering).
- Periodic & scheduled tasks.
- Sandboxing to protect the API token and the harness's own state from the agent.
- systemd hardening to protect the host and sessions from each other.
- A tool `silta-contract-check` for checking new versions of Claude Code for compatibility.
- Debian package with systemd units, `silta-session-add` and a daily backup of the Matrix
  encryption keys.
- Prebuilt `.deb` packages of Silta and typst built by a GitHub workflow at the release tag,
  attached to every release with their SHA-256 hashes.

## 0.5.1 - 2026-09-20

First version published to crates.io to reserve the crate names. Not intended for use.
