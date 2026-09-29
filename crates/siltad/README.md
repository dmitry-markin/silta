# siltad

Part of [Silta](https://github.com/dmitry-markin/silta), a family AI assistant that runs on Claude Code and speaks Matrix.

The crates are not meant to be used on their own. Install Silta as a whole as described in the [deployment guide](https://github.com/dmitry-markin/silta/blob/master/docs/deployment.md) in the repository.

This crate is the assistant's Matrix device, implementing login, key management and room access. It delivers messages to sessions over Unix sockets and sends their replies and files back, and it enforces which sessions may read and write which rooms. It logs only message and attachment sizes, never content. Configured by `/etc/silta/siltad.toml`.

## Read more

See the project's [README.md](https://github.com/dmitry-markin/silta/blob/master/README.md) and [deployment.md](https://github.com/dmitry-markin/silta/blob/master/docs/deployment.md).

## License

Licensed under either the Apache License 2.0 or the MIT license, at your option (SPDX: `Apache-2.0 OR MIT`).
