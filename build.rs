//! Build script that bakes the current git commit into the binary so
//! `paceflow --version` can identify exactly which source revision was built.
//!
//! Emits three `rustc-env` variables that `src/cli.rs` consumes via `env!()`:
//!
//! - `PACEFLOW_GIT_HASH`: short 12-char commit SHA, or `unknown`.
//! - `PACEFLOW_GIT_DIRTY`: `clean` or `dirty` (set when the working tree has
//!   uncommitted changes), or `unknown` if git is unavailable.
//! - `PACEFLOW_GIT_COMMIT_TIME`: HEAD commit time in ISO 8601, or `unknown`.
//!
//! `git` is invoked at build time. Missing git or missing `.git/` (e.g. when
//! building from a downloaded source tarball) is not fatal; the corresponding
//! env vars fall back to `unknown` and the rest of the build proceeds.

use std::process::Command;

fn main() {
    let git_hash =
        git_text(&["rev-parse", "--short=12", "HEAD"]).unwrap_or_else(|| "unknown".into());

    let dirty = match git_raw(&["status", "--porcelain", "--untracked-files=no"]) {
        Some(out) if !out.trim().is_empty() => "dirty",
        Some(_) => "clean",
        None => "unknown",
    };

    let commit_time =
        git_text(&["log", "-1", "--format=%cI", "HEAD"]).unwrap_or_else(|| "unknown".into());

    println!("cargo:rustc-env=PACEFLOW_GIT_HASH={git_hash}");
    println!("cargo:rustc-env=PACEFLOW_GIT_DIRTY={dirty}");
    println!("cargo:rustc-env=PACEFLOW_GIT_COMMIT_TIME={commit_time}");

    // Rebuild when HEAD moves (commit, branch switch, rebase) or when the
    // staged index changes (commits, stashes, partial adds).
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/index");
    println!("cargo:rerun-if-changed=build.rs");
}

/// Returns the trimmed stdout of a git invocation, or `None` if git is
/// unavailable, the command fails, or stdout is empty after trimming.
/// Use this for commands like `rev-parse` where empty output means failure.
fn git_text(args: &[&str]) -> Option<String> {
    let value = git_raw(args)?.trim().to_string();
    if value.is_empty() { None } else { Some(value) }
}

/// Returns the raw stdout of a git invocation, or `None` if git is
/// unavailable or the command exited non-zero. Use this for commands like
/// `status --porcelain` where empty output is a meaningful success signal.
fn git_raw(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()
}
