#!/usr/bin/env bash
# Build the silta Debian package: a release build of the whole workspace, then cargo-deb
# on the daemon's manifest, whose [package.metadata.deb] lists everything (the plugin
# and the log filter too) as assets. The result is target/debian/silta_<version>_amd64.deb.
set -euo pipefail
cd "$(dirname "$0")"
cargo build --release --workspace
cargo deb -p siltad --no-build "$@"
