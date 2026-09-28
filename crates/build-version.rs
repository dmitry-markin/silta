//! The build script of siltad, silta-session and silta-claude (`build =` in their
//! Cargo.toml): sets `SILTA_VERSION` to the crate's version and the commit it is built
//! from, as `git describe --tags --always` prints it: like `0.5.1 (v0.5.1-19-g9c5140a)`,
//! or `0.5.1 (v0.5.1)` on a tag. `--version` and the startup log lines print it, so a journal
//! excerpt names its build; the contract checker names its report after the plugin's.
//! Outside a git checkout (a source tarball) the version is the crate's alone.
//!
//! The script reruns when HEAD moves (a commit, a checkout) or a tag is added; an edit to
//! the working tree does not change the commit, so no `--dirty` mark is added.

use std::{path::Path, process::Command};

fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    Some(String::from_utf8(out.stdout).ok()?.trim().to_owned())
}

fn main() {
    let version = env!("CARGO_PKG_VERSION");
    // Relative to the package being built, as the `build =` key.
    println!("cargo:rerun-if-changed=../build-version.rs");
    match git(&["describe", "--tags", "--always"]) {
        Some(describe) => {
            println!("cargo:rustc-env=SILTA_VERSION={version} ({describe})");
            let mut watched = vec![
                "HEAD".to_owned(),
                "packed-refs".to_owned(),
                "refs/tags".to_owned(),
            ];
            watched.extend(git(&["symbolic-ref", "-q", "HEAD"]));
            for name in watched {
                // A path that does not exist would make cargo rerun the script every build.
                if let Some(path) =
                    git(&["rev-parse", "--git-path", &name]).filter(|p| Path::new(p).exists())
                {
                    println!("cargo:rerun-if-changed={path}");
                }
            }
        }
        None => println!("cargo:rustc-env=SILTA_VERSION={version}"),
    }
}
