//! Rebuild snapshots' numbers from their files alone.
//!
//! Snapshots of the same repository replay together, because some numbers
//! (commits shared between sessions, rework, the corrections habit) depend on
//! the project's other sessions. A group is laid out as one temporary home:
//! transcripts side by side, Cursor and OpenCode extracts merged into one
//! database each, the repository restored from the newest bundle with its refs,
//! HEAD and origin as they were. A child `vca ingest` runs there with every
//! agent path, the data home and the corpus' pinned price map pointed into it.
//!
//! The copied sources are not edited (token estimates depend on their exact
//! text); `VCA_REPLAY_REPOS` tells the child to run git and read repository
//! files in the restored copy whenever a source names the original path.
//! Replays must run on the same OS family as the snapshots.

use anyhow::{Context, Result, bail};
use rusqlite::Connection;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::process::Command;

use super::{BUNDLE, EXPECTATIONS, Manifest, PRICES, SavedSnapshot};
use crate::explore::review::{self, Expectations};

#[derive(Debug, Clone, serde::Serialize)]
pub struct ReplayResult {
    pub key: String,
    pub passed: bool,
    pub approved: usize,
    /// One line per changed number.
    pub report: Vec<String>,
}

/// Snapshots that replay together: one per repository; snapshots without a
/// repository each replay alone.
#[derive(Debug, Clone)]
pub struct Group {
    pub repo_root: Option<String>,
    pub snapshots: Vec<SavedSnapshot>,
}

pub fn groups(snapshots: Vec<SavedSnapshot>) -> Vec<Group> {
    let mut by_repo: BTreeMap<String, Vec<SavedSnapshot>> = BTreeMap::new();
    let mut alone = Vec::new();
    for snapshot in snapshots {
        match snapshot.manifest.repo_root.clone() {
            Some(repo) => by_repo.entry(repo).or_default().push(snapshot),
            None => alone.push(Group {
                repo_root: None,
                snapshots: vec![snapshot],
            }),
        }
    }
    let mut out: Vec<Group> = by_repo
        .into_iter()
        .map(|(repo, snapshots)| Group {
            repo_root: Some(repo),
            snapshots,
        })
        .collect();
    out.extend(alone);
    out
}

/// A group ingested in a temporary home; numbers are read from it with `stats`.
pub struct GroupReplay {
    temp: tempfile::TempDir,
    exe: PathBuf,
    repos: serde_json::Map<String, Value>,
    has_cursor: bool,
}

impl GroupReplay {
    pub fn build(group: &Group, exe: &Path, prices: Option<&Path>) -> Result<Self> {
        let temp = tempfile::Builder::new().prefix("vca-replay-").tempdir()?;
        let work = temp.path().to_path_buf();
        let home = work.join("home");
        std::fs::create_dir_all(&home)?;
        let cursor_dir = work.join("cursor");
        std::fs::create_dir_all(cursor_dir.join("History"))?;
        let mut has_cursor = false;
        for snapshot in &group.snapshots {
            verify(&snapshot.dir, &snapshot.manifest)?;
            copy_dir(&snapshot.dir.join("home"), &home)?;
            let opencode = snapshot.dir.join("home/.local/share/opencode/opencode.db");
            if opencode.exists() {
                merge_sqlite(&home.join(".local/share/opencode/opencode.db"), &opencode)?;
            }
            let cursor = snapshot.dir.join("cursor").join("state.vscdb");
            if cursor.exists() {
                merge_sqlite(&cursor_dir.join("state.vscdb"), &cursor)?;
                has_cursor = true;
            }
        }

        let mut repos = serde_json::Map::new();
        // The newest bundle holds the most history; older snapshots' commits are in it
        // unless history was rewritten.
        let newest = group
            .snapshots
            .iter()
            .filter(|s| s.dir.join(BUNDLE).exists())
            .max_by(|a, b| a.manifest.created_at.cmp(&b.manifest.created_at));
        if let (Some(original), Some(snapshot)) = (&group.repo_root, newest) {
            let repo = work.join("repo");
            restore_repo(&snapshot.dir.join(BUNDLE), &repo, &snapshot.manifest)?;
            let restored = crate::path_utils::canonicalize(&repo)?;
            repos.insert(
                original.clone(),
                Value::String(restored.to_string_lossy().to_string()),
            );
        }

        let pricing = work.join("vca").join(".vca").join("pricing");
        std::fs::create_dir_all(&pricing)?;
        let prices = prices.map(Path::to_path_buf).or_else(|| {
            group
                .snapshots
                .iter()
                .map(|s| s.dir.join(PRICES))
                .rfind(|p| p.exists())
        });
        if let Some(prices) = prices {
            std::fs::copy(prices, pricing.join(PRICES))?;
        }

        let replay = Self {
            temp,
            exe: exe.to_path_buf(),
            repos,
            has_cursor,
        };
        let ingest = replay.run(&["ingest", "--offline-pricing"])?;
        if !ingest.status.success() {
            bail!(
                "replay ingest failed:\n{}",
                String::from_utf8_lossy(&ingest.stderr)
            );
        }
        Ok(replay)
    }

    fn run(&self, args: &[&str]) -> Result<std::process::Output> {
        let work = self.temp.path();
        let home = work.join("home");
        let cursor_dir = work.join("cursor");
        let mut command = Command::new(&self.exe);
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
            .env("VCA_HOME", work.join("vca"))
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
                Value::Object(self.repos.clone()).to_string(),
            );
        if self.has_cursor {
            command.env("VCA_CURSOR_STATE_PATH", cursor_dir.join("state.vscdb"));
        } else {
            command.env_remove("VCA_CURSOR_STATE_PATH");
        }
        Ok(command.output()?)
    }

    /// Every number the review page shows for one session, as replayed.
    pub fn stats(&self, key: &str) -> Result<BTreeMap<String, Value>> {
        let out = self.run(&["review", key, "--json"])?;
        if !out.status.success() {
            bail!(
                "{key} is missing from the replay: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        let data: Value = serde_json::from_slice(&out.stdout)?;
        Ok(data["stats"]
            .as_array()
            .map(|stats| {
                stats
                    .iter()
                    .filter_map(|s| Some((s["id"].as_str()?.to_string(), s["value"].clone())))
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Keep the temporary home for inspection; returns its path.
    pub fn keep(self) -> PathBuf {
        self.temp.keep()
    }
}

/// Replay one group and compare every snapshot's approved numbers.
pub fn check_group(
    group: &Group,
    exe: &Path,
    prices: Option<&Path>,
    keep: bool,
) -> Result<Vec<ReplayResult>> {
    let replay = GroupReplay::build(group, exe, prices)?;
    let mut results = Vec::new();
    for snapshot in &group.snapshots {
        let expected = read_expectations(&snapshot.dir)?;
        let stats = replay.stats(&snapshot.manifest.key)?;
        let actual: HashMap<&str, &Value> = stats.iter().map(|(k, v)| (k.as_str(), v)).collect();
        let report = review::compare(&expected, &actual)
            .iter()
            .map(review::Mismatch::line)
            .collect::<Vec<_>>();
        results.push(ReplayResult {
            key: snapshot.manifest.key.clone(),
            passed: report.is_empty(),
            approved: expected.approved.len(),
            report,
        });
    }
    if keep {
        let kept = replay.keep();
        if let Some(last) = results.last_mut() {
            last.report
                .push(format!("replay kept at {}", kept.display()));
        }
    }
    Ok(results)
}

pub fn read_expectations(dir: &Path) -> Result<Expectations> {
    let text = std::fs::read_to_string(dir.join(EXPECTATIONS))?;
    Ok(serde_json::from_str(&text)?)
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
        } else if entry
            .path()
            .extension()
            .is_none_or(|e| e != "db" && e != "vscdb")
        {
            std::fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}

/// Add every row of `src` to `dest` (creating it, or missing tables, as needed).
fn merge_sqlite(dest: &Path, src: &Path) -> Result<()> {
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(dest)?;
    conn.execute_batch("PRAGMA foreign_keys = OFF;")?;
    conn.execute("ATTACH DATABASE ?1 AS src", [src.to_string_lossy()])?;
    let tables: Vec<(String, String)> = conn
        .prepare("SELECT name, sql FROM src.sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    for (name, sql) in tables {
        let exists: bool = conn.query_row(
            "SELECT COUNT(*) > 0 FROM main.sqlite_master WHERE type = 'table' AND name = ?1",
            [&name],
            |row| row.get(0),
        )?;
        if !exists {
            conn.execute_batch(&sql)?;
        }
        conn.execute(
            &format!("INSERT OR IGNORE INTO main.\"{name}\" SELECT * FROM src.\"{name}\""),
            [],
        )?;
    }
    conn.execute("DETACH DATABASE src", [])?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn saved(key: &str, repo: Option<&str>) -> SavedSnapshot {
        SavedSnapshot {
            dir: PathBuf::from(key),
            manifest: Manifest {
                key: key.to_string(),
                repo_root: repo.map(str::to_string),
                ..Manifest::default()
            },
            approved: 0,
            rejected: 0,
        }
    }

    #[test]
    fn groups_by_repository_and_keeps_repoless_alone() {
        let groups = groups(vec![
            saved("a", Some("/r1")),
            saved("b", None),
            saved("c", Some("/r1")),
            saved("d", None),
            saved("e", Some("/r2")),
        ]);
        let shape: Vec<(Option<&str>, Vec<&str>)> = groups
            .iter()
            .map(|g| {
                (
                    g.repo_root.as_deref(),
                    g.snapshots
                        .iter()
                        .map(|s| s.manifest.key.as_str())
                        .collect(),
                )
            })
            .collect();
        assert_eq!(
            shape,
            [
                (Some("/r1"), vec!["a", "c"]),
                (Some("/r2"), vec!["e"]),
                (None, vec!["b"]),
                (None, vec!["d"]),
            ]
        );
    }

    #[test]
    fn merges_rows_from_several_extracts() -> Result<()> {
        let dir = tempfile::tempdir()?;
        for (name, key) in [("a.vscdb", "composerData:a"), ("b.vscdb", "composerData:b")] {
            let conn = Connection::open(dir.path().join(name))?;
            conn.execute_batch(
                "CREATE TABLE cursorDiskKV (key TEXT UNIQUE ON CONFLICT REPLACE, value BLOB);",
            )?;
            conn.execute("INSERT INTO cursorDiskKV VALUES (?1, '{}')", [key])?;
        }
        let merged = dir.path().join("merged.vscdb");
        merge_sqlite(&merged, &dir.path().join("a.vscdb"))?;
        merge_sqlite(&merged, &dir.path().join("b.vscdb"))?;
        merge_sqlite(&merged, &dir.path().join("b.vscdb"))?;
        let n: i64 = Connection::open(&merged)?.query_row(
            "SELECT COUNT(*) FROM cursorDiskKV",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(n, 2);
        Ok(())
    }
}
