# silta-claude

Part of [Silta](https://github.com/dmitry-markin/silta), a family AI assistant that runs on Claude Code and speaks Matrix.

The crates are not meant to be used on their own. Install Silta as a whole as described in the [deployment guide](https://github.com/dmitry-markin/silta/blob/master/docs/deployment.md) in the repository.

This crate is the Claude Code channel plugin: an MCP server that receives messages from `siltad` and provides the assistant with tools to interact with Matrix rooms (reply, react, send a file, read and search room history). Installed by the package under `/usr/lib/silta` and enabled through managed settings.

## Read more

See the project's [README.md](https://github.com/dmitry-markin/silta/blob/master/README.md) and [deployment.md](https://github.com/dmitry-markin/silta/blob/master/docs/deployment.md).

## License

Licensed under either the Apache License 2.0 or the MIT license, at your option (SPDX: `Apache-2.0 OR MIT`).
