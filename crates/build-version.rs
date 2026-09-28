//! The build script of siltad, silta-session and silta-claude, each linking it as its
//! build.rs (a link and not `build =`, which cargo package refuses outside the crate; a
//! packaged crate gets a copy): sets `SILTA_VERSION` to the crate's version and the commit
//! it is built from, as `git describe --tags --always --dirty` prints it: like
//! `0.5.1 (v0.5.1-19-g9c5140a)`, `0.5.1 (v0.5.1-19-g9c5140a-dirty)` with uncommitted
//! changes, or `0.5.1 (v0.5.1)` on a tag. `--version` and the startup log lines print it,
//! so a journal excerpt names its build; the contract checker names its report after the
//! plugin's. Outside a git checkout (a source tarball) the version is the crate's alone.
//!
//! The script reruns when HEAD moves (a commit, a checkout), a tag is added or a tracked
//! file changes (which may make the tree dirty or clean again), so each of these rebuilds
//! the three crates. A change only staged, such as `git add` of a new file, shows at the
//! next rerun.

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
    // The link; cargo follows it to this file's time.
    println!("cargo:rerun-if-changed=build.rs");
    match git(&["describe", "--tags", "--always", "--dirty"]) {
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
            // Every tracked file, for `-dirty`; one deleted but not yet committed reruns
            // the script at every build, while the tree is dirty anyway. Not the index,
            // which `git status` rewrites.
            if let Some(top) = git(&["rev-parse", "--show-toplevel"]) {
                // From the top: in a crate's directory ls-files lists that crate's alone.
                for file in git(&["-C", &top, "ls-files", "-z"])
                    .iter()
                    .flat_map(|f| f.split('\0'))
                {
                    if !file.is_empty() {
                        println!("cargo:rerun-if-changed={top}/{file}");
                    }
                }
            }
        }
        None => println!("cargo:rustc-env=SILTA_VERSION={version}"),
    }
}
