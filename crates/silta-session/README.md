# silta-session

Part of [Silta](https://github.com/dmitry-markin/silta), a family AI assistant that runs on Claude Code and speaks Matrix.

The crates are not meant to be used on their own. Install Silta as a whole as described in the [deployment guide](https://github.com/dmitry-markin/silta/blob/master/docs/deployment.md) in the repository.

This crate is the supervisor of one session: either a personal one serving a direct chat, or a hub session serving group rooms. It runs Claude Code headless with the channel plugin `silta-claude`, resumes the transcript across restarts, and rotates the session when the context grows: asks the assistant to write a handoff note, then compacts the conversation. Launched by the `silta-session@.service` unit.

## Read more

See the project's [README.md](https://github.com/dmitry-markin/silta/blob/master/README.md) and [deployment.md](https://github.com/dmitry-markin/silta/blob/master/docs/deployment.md).

## License

Licensed under either the Apache License 2.0 or the MIT license, at your option (SPDX: `Apache-2.0 OR MIT`).
