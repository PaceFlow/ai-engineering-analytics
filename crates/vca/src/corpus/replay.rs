//! Rebuild a snapshot's numbers from its files alone and compare them with
//! what the reviewer approved.
//!
//! The snapshot is laid out as a fake home; a child `vca ingest` runs with every
//! agent path, the data home and the price map pointed into a temporary copy.
//! The repository is restored from the git bundle with its refs, HEAD and
//! origin as they were. The copied sources are not edited (token estimates
//! depend on their exact text); instead `VCA_REPLAY_REPOS` tells the child to
//! run git and read repository files in the restored copy whenever a source
//! names the original path. Replays must run on the same OS family as the
//! snapshot (paths are not translated between Windows and Unix).

use anyhow::{Context, Result, bail};
use std::path::Path;
use std::process::Command;

use super::{BUNDLE, EXPECTATIONS, Manifest, PRICES};

#[derive(Debug, Clone, serde::Serialize)]
pub struct ReplayResult {
    pub key: String,
    pub passed: bool,
    pub approved: usize,
    /// Lines from `vca review --check`: one per changed number.
    pub report: Vec<String>,
}

pub fn replay(dir: &Path, exe: &Path, keep: bool) -> Result<ReplayResult> {
    let manifest = super::read_manifest(dir)?;
    verify(dir, &manifest)?;
    let temp = tempfile::Builder::new().prefix("vca-replay-").tempdir()?;
    let work = temp.path();
    let home = work.join("home");
    copy_dir(&dir.join("home"), &home)?;
    std::fs::create_dir_all(&home)?;

    let cursor_state = dir.join("cursor").join("state.vscdb");
    let cursor_dir = work.join("cursor");
    std::fs::create_dir_all(cursor_dir.join("History"))?;
    if cursor_state.exists() {
        std::fs::copy(&cursor_state, cursor_dir.join("state.vscdb"))?;
    }

    let mut repos = serde_json::Map::new();
    if let (Some(original), true) = (&manifest.repo_root, dir.join(BUNDLE).exists()) {
        let repo = work.join("repo");
        restore_repo(&dir.join(BUNDLE), &repo, &manifest)?;
        let restored = crate::path_utils::canonicalize(&repo)?;
        repos.insert(
            original.clone(),
            serde_json::Value::String(restored.to_string_lossy().to_string()),
        );
    }

    let vca_home = work.join("vca");
    let pricing = vca_home.join(".vca").join("pricing");
    std::fs::create_dir_all(&pricing)?;
    if dir.join(PRICES).exists() {
        std::fs::copy(dir.join(PRICES), pricing.join(PRICES))?;
    }
    std::fs::copy(dir.join(EXPECTATIONS), work.join(EXPECTATIONS))?;

    let run = |args: &[&str]| {
        let mut command = Command::new(exe);
        command.args(args).current_dir(work);
        for key in [
            "VCA_OPENCODE_DB_PATH",
            "OPENCODE_DB_PATH",
            "VCA_GITHUB_TOKEN",
            "PACEFLOW_HOME",
            "AIENG_HOME",
            "AEA_HOME",
            "XDG_DATA_HOME",
        ] {
            command.env_remove(key);
        }
        command
            .env("VCA_HOME", &vca_home)
            .env("HOME", &home)
            .env("USERPROFILE", &home)
            .env("CODEX_HOME", home.join(".codex"))
            .env("APPDATA", home.join("AppData").join("Roaming"))
            .env("LOCALAPPDATA", home.join("AppData").join("Local"))
            .env("XDG_CONFIG_HOME", home.join(".config"))
            .env("VCA_CURSOR_HISTORY_PATH", cursor_dir.join("History"))
            .env("VCA_PRICING_OFFLINE", "1")
            .env(
                crate::path_utils::REPLAY_REPOS_ENV,
                serde_json::Value::Object(repos.clone()).to_string(),
            );
        if cursor_state.exists() {
            command.env("VCA_CURSOR_STATE_PATH", cursor_dir.join("state.vscdb"));
        } else {
            command.env_remove("VCA_CURSOR_STATE_PATH");
        }
        command.output()
    };

    let ingest = run(&["ingest", "--offline-pricing"])?;
    if !ingest.status.success() {
        bail!(
            "ingest of {} failed:\n{}",
            manifest.key,
            String::from_utf8_lossy(&ingest.stderr)
        );
    }
    let expectations = work.join(EXPECTATIONS).to_string_lossy().to_string();
    let check = run(&["review", "--check", &expectations])?;
    let mut report: Vec<String> = String::from_utf8_lossy(&check.stdout)
        .lines()
        .map(str::to_owned)
        .collect();
    let stderr = String::from_utf8_lossy(&check.stderr);
    report.extend(
        stderr
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(str::to_owned),
    );
    let approved = read_approved(dir)?;
    if keep {
        let kept = temp.keep();
        report.push(format!("replay kept at {}", kept.display()));
    }
    Ok(ReplayResult {
        key: manifest.key,
        passed: check.status.success(),
        approved,
        report,
    })
}

fn read_approved(dir: &Path) -> Result<usize> {
    let text = std::fs::read_to_string(dir.join(EXPECTATIONS))?;
    let expectations: crate::explore::review::Expectations = serde_json::from_str(&text)?;
    Ok(expectations.approved.len())
}

/// Every file listed in the manifest is present and unchanged.
fn verify(dir: &Path, manifest: &Manifest) -> Result<()> {
    for file in &manifest.files {
        let bytes = std::fs::read(dir.join(&file.path))
            .with_context(|| format!("{} is missing from {}", file.path, dir.display()))?;
        let md5 = format!("{:x}", md5::compute(&bytes));
        if md5 != file.md5 {
            bail!("{} changed since the snapshot was saved", file.path);
        }
    }
    Ok(())
}

fn copy_dir(from: &Path, to: &Path) -> Result<()> {
    if !from.exists() {
        return Ok(());
    }
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)?.flatten() {
        let target = to.join(entry.file_name());
        if entry.path().is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}

/// Recreate the repository as it was: every ref from the bundle, the same
/// HEAD (branch or detached commit), origin's URL and default branch.
fn restore_repo(bundle: &Path, repo: &Path, manifest: &Manifest) -> Result<()> {
    let git = |args: &[&str]| -> Result<()> {
        let out = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .context("git is needed to restore the repository")?;
        if !out.status.success() {
            bail!(
                "git {} failed: {}",
                args.join(" "),
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Ok(())
    };
    std::fs::create_dir_all(repo)?;
    git(&["init", "--quiet"])?;
    let bundle = bundle.to_string_lossy().to_string();
    git(&[
        "fetch",
        "--quiet",
        "--update-head-ok",
        &bundle,
        "+refs/*:refs/*",
    ])?;
    if let Some(url) = &manifest.repo_remote {
        git(&["remote", "add", "origin", url])?;
    }
    if let Some(target) = &manifest.repo_origin_head {
        git(&["symbolic-ref", "refs/remotes/origin/HEAD", target])?;
    }
    match (&manifest.repo_head_ref, &manifest.repo_head) {
        (Some(branch), _) => {
            git(&["symbolic-ref", "HEAD", branch])?;
            git(&["reset", "--quiet", "--hard"])?;
        }
        (None, Some(head)) => git(&["checkout", "--quiet", "--detach", head])?,
        (None, None) => {}
    }
    Ok(())
}
