#!/usr/bin/env bash
# Run the daemon against the loopback conduit with the gitignored dev/siltad.toml.
set -euo pipefail
repo=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$repo/dev/state"
cd "$repo"
export RUST_LOG="${RUST_LOG:-info,matrix_sdk=warn}"
exec cargo run -q -p siltad -- --config "$repo/dev/siltad.toml" "$@"
