use anyhow::{Result, anyhow};
use chrono::{DateTime, SecondsFormat, Utc};
use std::collections::HashMap;
use std::process::Command;

use crate::change_intel::line_hash::hash_line;
use crate::change_intel::types::{LineHashCount, LineSide};

use super::types::{GitCommitDiff, GitFileDiff};

#[derive(Debug, Clone)]
struct WorkingFileDiff {
    old_path: Option<String>,
    new_path: Option<String>,
    change_type: String,
    added_lines: i64,
    removed_lines: i64,
    hash_counts: HashMap<(LineSide, String), i64>,
}

pub fn validate_git_repo(repo_root: &str) -> Result<Option<String>> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .arg("rev-parse")
        .arg("--show-toplevel")
        .output()?;

    if !output.status.success() {
        return Ok(None);
    }

    let top = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if top.is_empty() {
        return Ok(None);
    }

    Ok(Some(top))
}

pub fn current_head_sha(repo_root: &str) -> Result<String> {
    let out = run_git_capture(repo_root, &["rev-parse".to_string(), "HEAD".to_string()])?;
    let sha = out.trim().to_string();
    if sha.is_empty() {
        return Err(anyhow!("git rev-parse HEAD returned empty output"));
    }
    Ok(sha)
}

pub fn is_ancestor(repo_root: &str, ancestor: &str, descendant: &str) -> Result<bool> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .arg("merge-base")
        .arg("--is-ancestor")
        .arg(ancestor)
        .arg(descendant)
        .output()?;

    if output.status.success() {
        return Ok(true);
    }

    if output.status.code() == Some(1) {
        return Ok(false);
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    Err(anyhow!(
        "git merge-base --is-ancestor failed for {ancestor}..{descendant}: {stderr}"
    ))
}

pub fn list_commits_since(
    repo_root: &str,
    since_ts: &str,
    include_merges: bool,
    max_commits: Option<usize>,
) -> Result<Vec<String>> {
    let mut args = vec!["rev-list".to_string(), "--reverse".to_string()];
    if !include_merges {
        args.push("--no-merges".to_string());
    }
    if let Some(max) = max_commits {
        args.push(format!("--max-count={max}"));
    }
    args.push(format!("--since={since_ts}"));
    args.push("HEAD".to_string());

    list_from_rev_list(repo_root, &args)
}

pub fn list_commits_range(
    repo_root: &str,
    from_exclusive: &str,
    to_inclusive: &str,
    include_merges: bool,
    max_commits: Option<usize>,
) -> Result<Vec<String>> {
    let mut args = vec!["rev-list".to_string(), "--reverse".to_string()];
    if !include_merges {
        args.push("--no-merges".to_string());
    }
    if let Some(max) = max_commits {
        args.push(format!("--max-count={max}"));
    }
    args.push(format!("{from_exclusive}..{to_inclusive}"));

    list_from_rev_list(repo_root, &args)
}

pub fn load_commit_metadata(
    repo_root: &str,
    commit_sha: &str,
) -> Result<(String, String, Option<String>)> {
    let fmt = "--format=%cI%x1f%s%x1f%P".to_string();
    let out = run_git_capture(
        repo_root,
        &[
            "show".to_string(),
            "-s".to_string(),
            fmt,
            commit_sha.to_string(),
        ],
    )?;

    let line = out.trim_end_matches('\n').trim();
    let mut parts = line.splitn(3, '\u{1f}');

    let raw_time = parts
        .next()
        .ok_or_else(|| anyhow!("missing commit time in git show output for {commit_sha}"))?;
    let subject = parts
        .next()
        .ok_or_else(|| anyhow!("missing commit subject in git show output for {commit_sha}"))?
        .to_string();
    let parents = parts
        .next()
        .ok_or_else(|| anyhow!("missing commit parent info in git show output for {commit_sha}"))?;

    let commit_time = normalize_git_time(raw_time)?;
    let parent_sha = parents
        .split_whitespace()
        .next()
        .map(ToOwned::to_owned)
        .filter(|s| !s.is_empty());

    Ok((commit_time, subject, parent_sha))
}

pub fn load_commit_diff(repo_root: &str, commit_sha: &str) -> Result<GitCommitDiff> {
    let (commit_time, subject, parent_sha) = load_commit_metadata(repo_root, commit_sha)?;

    let patch = run_git_capture(
        repo_root,
        &[
            "show".to_string(),
            "--format=".to_string(),
            "--patch".to_string(),
            "--unified=0".to_string(),
            "--no-color".to_string(),
            "--no-ext-diff".to_string(),
            "--find-renames".to_string(),
            "--find-copies".to_string(),
            commit_sha.to_string(),
        ],
    )?;

    let file_diffs = parse_patch_file_diffs(&patch);

    Ok(GitCommitDiff {
        commit_sha: commit_sha.to_string(),
        parent_sha,
        commit_time,
        subject,
        file_diffs,
    })
}

fn list_from_rev_list(repo_root: &str, args: &[String]) -> Result<Vec<String>> {
    let out = run_git_capture(repo_root, args)?;
    Ok(out
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}

fn run_git_capture(repo_root: &str, args: &[String]) -> Result<String> {
    let output = Command::new("git")
        .arg("-c")
        .arg("core.quotepath=false")
        .arg("-C")
        .arg(repo_root)
        .args(args)
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let rendered_args = args.join(" ");
        return Err(anyhow!("git {rendered_args} failed: {stderr}"));
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn normalize_git_time(raw: &str) -> Result<String> {
    let dt = DateTime::parse_from_rfc3339(raw)
        .map_err(|e| anyhow!("invalid commit timestamp '{raw}': {e}"))?;
    Ok(dt
        .with_timezone(&Utc)
        .to_rfc3339_opts(SecondsFormat::Millis, true))
}

fn parse_patch_file_diffs(patch: &str) -> Vec<GitFileDiff> {
    let mut results = Vec::new();
    let mut current: Option<WorkingFileDiff> = None;

    for line in patch.lines() {
        if let Some(rest) = line.strip_prefix("diff --git ") {
            if let Some(done) = finalize_file_diff(current.take()) {
                results.push(done);
            }

            let (old_path, new_path) = parse_diff_git_paths(rest);
            current = Some(WorkingFileDiff {
                old_path,
                new_path,
                change_type: "modify".to_string(),
                added_lines: 0,
                removed_lines: 0,
                hash_counts: HashMap::new(),
            });
            continue;
        }

        let Some(cur) = current.as_mut() else {
            continue;
        };

        if line.starts_with("new file mode ") {
            cur.change_type = "add".to_string();
            continue;
        }
        if line.starts_with("deleted file mode ") {
            cur.change_type = "delete".to_string();
            continue;
        }

        if let Some(rest) = line.strip_prefix("rename from ") {
            cur.change_type = "rename".to_string();
            cur.old_path = parse_patch_path(rest);
            continue;
        }
        if let Some(rest) = line.strip_prefix("rename to ") {
            cur.change_type = "rename".to_string();
            cur.new_path = parse_patch_path(rest);
            continue;
        }
        if let Some(rest) = line.strip_prefix("copy from ") {
            cur.change_type = "copy".to_string();
            cur.old_path = parse_patch_path(rest);
            continue;
        }
        if let Some(rest) = line.strip_prefix("copy to ") {
            cur.change_type = "copy".to_string();
            cur.new_path = parse_patch_path(rest);
            continue;
        }

        if let Some(rest) = line.strip_prefix("--- ") {
            cur.old_path = parse_patch_path(rest);
            continue;
        }
        if let Some(rest) = line.strip_prefix("+++ ") {
            cur.new_path = parse_patch_path(rest);
            continue;
        }

        if line.starts_with('+') && !line.starts_with("+++") {
            cur.added_lines += 1;
            let h = hash_line(line.strip_prefix('+').unwrap_or(""));
            *cur.hash_counts.entry((LineSide::Added, h)).or_insert(0) += 1;
            continue;
        }

        if line.starts_with('-') && !line.starts_with("---") {
            cur.removed_lines += 1;
            let h = hash_line(line.strip_prefix('-').unwrap_or(""));
            *cur.hash_counts.entry((LineSide::Removed, h)).or_insert(0) += 1;
        }
    }

    if let Some(done) = finalize_file_diff(current.take()) {
        results.push(done);
    }

    results
}

fn parse_diff_git_paths(rest: &str) -> (Option<String>, Option<String>) {
    let mut parts = rest.split_whitespace();
    let old_path = parts.next().and_then(parse_patch_path);
    let new_path = parts.next().and_then(parse_patch_path);
    (old_path, new_path)
}

fn parse_patch_path(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }

    let token = raw.split('\t').next().unwrap_or(raw).trim();
    let token = strip_wrapping_quotes(token);

    if token == "/dev/null" {
        return None;
    }

    let token = token
        .strip_prefix("a/")
        .or_else(|| token.strip_prefix("b/"))
        .unwrap_or(token);

    if token.is_empty() {
        return None;
    }

    Some(token.to_string())
}

fn strip_wrapping_quotes(input: &str) -> &str {
    if input.len() >= 2 {
        let bytes = input.as_bytes();
        let first = bytes[0] as char;
        let last = bytes[input.len() - 1] as char;
        if (first == '\'' && last == '\'') || (first == '"' && last == '"') {
            return &input[1..input.len() - 1];
        }
    }
    input
}

fn finalize_file_diff(file: Option<WorkingFileDiff>) -> Option<GitFileDiff> {
    let file = file?;
    let rel_path = if file.change_type == "delete" {
        file.old_path.as_ref().or(file.new_path.as_ref())
    } else {
        file.new_path.as_ref().or(file.old_path.as_ref())
    }?
    .trim()
    .to_string();

    if rel_path.is_empty() {
        return None;
    }

    let line_hashes = file
        .hash_counts
        .into_iter()
        .map(|((side, line_hash), count)| LineHashCount {
            side,
            line_hash,
            count,
        })
        .collect();

    Some(GitFileDiff {
        rel_path,
        change_type: file.change_type,
        added_lines: file.added_lines,
        removed_lines: file.removed_lines,
        line_hashes,
    })
}
