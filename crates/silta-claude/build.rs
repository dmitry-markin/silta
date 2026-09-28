//! Adds the commit the plugin is built from to `--version`, as `git describe --tags
//! --always` prints it: `silta-claude 0.5.1 (v0.5.1-19-g9c5140a)`, or `(v0.5.1)` on a
//! tag. The contract checker names its report after it. Outside a git checkout (a source
//! tarball) the version is the crate's alone.
//!
//! The build reruns when HEAD moves (a commit, a checkout) or a tag is added; an edit to
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
    println!("cargo:rerun-if-changed=build.rs");
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
