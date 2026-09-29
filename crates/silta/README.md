# silta

Part of [Silta](https://github.com/dmitry-markin/silta), a family AI assistant that runs on Claude Code and speaks Matrix.

The crates are not meant to be used on their own. Install Silta as a whole as described in the [deployment guide](https://github.com/dmitry-markin/silta/blob/master/docs/deployment.md) in the repository.

This is the shared crate of all the components: the socket protocol between the daemon and the channel plugin, the configuration types and the paths.

## Silta features

- Claude Code as the harness: one long-lived session per person, plus a shared session for the family rooms. Each person uses their own Claude subscription (via `claude setup-token`) or Anthropic API key.
- Remembers people, tasks, and conversation state across context limits and restarts (see [Context compaction and continuity](https://github.com/dmitry-markin/silta#context-compaction-and-continuity)).
- Direct & group Matrix rooms with read receipts, typing indicators, reactions, attachments, quoting, and threads.
- Web search & research, with results delivered as PDFs (including phone-sized rendering).
- Periodic & scheduled tasks.

## Read more

See the project's [README.md](https://github.com/dmitry-markin/silta/blob/master/README.md) and [deployment.md](https://github.com/dmitry-markin/silta/blob/master/docs/deployment.md).

## License

Licensed under either the Apache License 2.0 or the MIT license, at your option (SPDX: `Apache-2.0 OR MIT`).
