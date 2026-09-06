#!/usr/bin/env bash
# Run the daemon against the loopback conduit with the gitignored dev/siltad.toml.
set -euo pipefail
repo=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$repo/dev/state"
cd "$repo"
export RUST_LOG="${RUST_LOG:-info,matrix_sdk=warn,matrix_sdk_base=warn,matrix_sdk_crypto=warn,matrix_sdk_crypto::backups=error,matrix_sdk::http_client=off}"
# Minds under other Unix users reach the socket through the silta group; when this user
# is in it, the daemon runs with that group so the socket gets it.
if id -nG "$(id -un)" | tr ' ' '\n' | grep -qx silta; then
  exec sg silta -c "cargo run -q -p siltad -- --config '$repo/dev/siltad.toml' $*"
fi
exec cargo run -q -p siltad -- --config "$repo/dev/siltad.toml" "$@"
