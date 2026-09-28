#!/usr/bin/env bash
# Build the silta Debian package: a release build of the whole workspace, then cargo-deb
# on the daemon's manifest, whose [package.metadata.deb] lists everything (the plugin
# and the session supervisor too) as assets. The result is target/debian/silta_<version>_amd64.deb.
#
# The package version names the commit, as the binaries' --version does
# (crates/build-version.rs): 0.5.1 on the tag v0.5.1, 0.5.1+19.g9c5140a nineteen commits
# after it, which sorts after 0.5.1 and before 0.5.2. With the crate's version already
# bumped past the last tag, `~` instead of `+` sorts the build before the release
# (0.5.2~3.g1a2b3c4). Outside a git checkout the version is the crate's alone. No `-1`
# revision: a native package (crates/siltad/Cargo.toml).
set -euo pipefail
cd "$(dirname "$0")"
version=$(cargo pkgid -p siltad)
version=${version##*[#@]}
deb_version=()
if describe=$(git describe --tags --long 2>/dev/null); then
    # v0.5.1-19-g9c5140a, split from the right: a tag may contain a hyphen.
    hash=${describe##*-}
    rest=${describe%-*}
    count=${rest##*-}
    tag=${rest%-*}
    if [[ $tag == "v$version" ]]; then
        [[ $count == 0 ]] || deb_version=(--deb-version "$version+$count.$hash")
    else
        deb_version=(--deb-version "$version~$count.$hash")
    fi
elif hash=$(git rev-parse --short HEAD 2>/dev/null); then
    # A checkout without tags (a shallow clone): the commit alone.
    deb_version=(--deb-version "$version+g$hash")
fi
cargo build --release --workspace
cargo deb -p siltad --no-build "${deb_version[@]}" "$@"
