//! Rebuild a snapshot's numbers from its files alone and compare them with
//! what the reviewer approved.
//!
//! The snapshot is laid out as a fake home; a child `vca ingest` runs with every
//! agent path, the data home and the price map pointed into a temporary copy.
//! The repository comes from the git bundle, cloned to a temporary folder; the
//! original repository path inside the copied sources is rewritten to it, so
//! commit association and delivery numbers do not depend on the live checkout.
//! Replays must run on the same OS family as the snapshot (paths are not
//! translated between Windows and Unix).

use anyhow::{Context, Result, bail};
use rusqlite::Connection;
use std::path::{Path, PathBuf};
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

    // Repository from the bundle, with the original path rewritten to it.
    if let (Some(original), true) = (&manifest.repo_root, dir.join(BUNDLE).exists()) {
        let repo = work.join("repo");
        let status = Command::new("git")
            .args(["clone", "--quiet"])
            .arg(dir.join(BUNDLE))
            .arg(&repo)
            .status()
            .context("git is needed to restore the repository")?;
        if !status.success() {
            bail!("git clone of {} failed", dir.join(BUNDLE).display());
        }
        if let Some(head) = &manifest.repo_head {
            let _ = Command::new("git")
                .arg("-C")
                .arg(&repo)
                .args(["checkout", "--quiet", "--detach", head])
                .status();
        }
        let canonical = crate::path_utils::canonicalize(&repo)?;
        let pairs = path_variants(original, &canonical.to_string_lossy());
        rewrite_tree(&home, &pairs)?;
        if cursor_state.exists() {
            rewrite_db(&cursor_dir.join("state.vscdb"), &pairs)?;
        }
        let opencode = home.join(".local/share/opencode/opencode.db");
        if opencode.exists() {
            rewrite_db(&opencode, &pairs)?;
        }
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
            .env("VCA_PRICING_OFFLINE", "1");
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

/// Spellings of one path as it appears in transcripts, JSON and file URIs.
pub fn path_variants(original: &str, replacement: &str) -> Vec<(String, String)> {
    let trim = |s: &str| s.trim_end_matches(['/', '\\']).to_string();
    let (from, to) = (trim(original), trim(replacement));
    let forward = |s: &str| s.replace('\\', "/");
    let json = |s: &str| s.replace('\\', "\\\\");
    let lower_drive = |s: &str| {
        let mut chars = s.chars();
        match (chars.next(), chars.next()) {
            (Some(d), Some(':')) => format!("{}:{}", d.to_ascii_lowercase(), chars.as_str()),
            _ => s.to_string(),
        }
    };
    let uri = |s: &str| lower_drive(&forward(s)).replacen(':', "%3A", 1);
    let mut pairs = vec![
        (json(&from), json(&to)),
        (from.clone(), to.clone()),
        (forward(&from), forward(&to)),
        (lower_drive(&from), lower_drive(&to)),
        (lower_drive(&forward(&from)), lower_drive(&forward(&to))),
        (json(&lower_drive(&from)), json(&lower_drive(&to))),
        (uri(&from), uri(&to)),
    ];
    pairs.retain(|(a, _)| !a.is_empty());
    pairs.dedup();
    pairs
}

fn rewrite_text(text: &str, pairs: &[(String, String)]) -> String {
    pairs.iter().fold(text.to_string(), |acc, (from, to)| {
        acc.replace(from.as_str(), to)
    })
}

fn rewrite_tree(dir: &Path, pairs: &[(String, String)]) -> Result<()> {
    for entry in std::fs::read_dir(dir)?.flatten() {
        let path: PathBuf = entry.path();
        if path.is_dir() {
            rewrite_tree(&path, pairs)?;
        } else if matches!(
            path.extension().and_then(|e| e.to_str()),
            Some("jsonl" | "json")
        ) {
            let text = std::fs::read_to_string(&path)?;
            let rewritten = rewrite_text(&text, pairs);
            if rewritten != text {
                std::fs::write(&path, rewritten)?;
            }
        }
    }
    Ok(())
}

/// Rewrite the path in every text column of every table.
fn rewrite_db(path: &Path, pairs: &[(String, String)]) -> Result<()> {
    let conn = Connection::open(path)?;
    let tables: Vec<String> = conn
        .prepare(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
        )?
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    for table in tables {
        let cols: Vec<String> = conn
            .prepare(&format!("PRAGMA table_info({table})"))?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<rusqlite::Result<_>>()?;
        for col in cols {
            for (from, to) in pairs {
                conn.execute(
                    &format!(
                        "UPDATE {table} SET {col} = replace(CAST({col} AS TEXT), ?1, ?2)
                         WHERE typeof({col}) IN ('text', 'blob') AND instr(CAST({col} AS TEXT), ?1) > 0"
                    ),
                    [from, to],
                )?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_windows_path_spellings() {
        let pairs = path_variants(r"C:\Work\game", r"C:\Temp\r\repo");
        let text = r#"{"cwd":"C:\\Work\\game","uri":"file:///c%3A/Work/game/src/a.rs","p":"C:/Work/game/x","raw":"C:\Work\game\y"}"#;
        assert_eq!(
            rewrite_text(text, &pairs),
            r#"{"cwd":"C:\\Temp\\r\\repo","uri":"file:///c%3A/Temp/r/repo/src/a.rs","p":"C:/Temp/r/repo/x","raw":"C:\Temp\r\repo\y"}"#
        );
    }

    #[test]
    fn rewrites_unix_paths() {
        let pairs = path_variants("/home/me/game/", "/tmp/r/repo");
        assert_eq!(
            rewrite_text(
                r#"{"cwd":"/home/me/game","f":"/home/me/game/a.rs"}"#,
                &pairs
            ),
            r#"{"cwd":"/tmp/r/repo","f":"/tmp/r/repo/a.rs"}"#
        );
    }

    #[test]
    fn rewrites_text_columns_in_sqlite() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("db.sqlite");
        let conn = Connection::open(&path)?;
        conn.execute_batch(
            "CREATE TABLE session (id TEXT, directory TEXT, n INTEGER);
             INSERT INTO session VALUES ('s1', '/home/me/game', 3);",
        )?;
        drop(conn);
        rewrite_db(&path, &path_variants("/home/me/game", "/tmp/repo"))?;
        let dir_now: String =
            Connection::open(&path)?
                .query_row("SELECT directory FROM session", [], |row| row.get(0))?;
        assert_eq!(dir_now, "/tmp/repo");
        Ok(())
    }
}
