use anyhow::Result;
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use rusqlite::Connection;
use std::collections::HashSet;
use std::path::PathBuf;

pub mod git_scan;
pub mod matcher;
pub mod storage;
pub mod types;

use git_scan::{
    current_head_sha, is_ancestor, list_commits_range, list_commits_since, load_commit_diff,
    load_commit_metadata, validate_git_repo,
};
use matcher::compute_commit_attribution;
use storage::{
    branch_scope_head, get_commit_cursor, insert_commit_assoc_error, list_repo_roots_from_change_ops,
    min_ai_timestamp, upsert_commit_attribution, upsert_commit_cursor, upsert_git_commit_with_diffs,
};
use types::{AssociationSummary, RepoSummary, RunOptions};

pub fn run(conn: &Connection, options: RunOptions, verbose: bool) -> Result<AssociationSummary> {
    let discovered = list_repo_roots_from_change_ops(conn)?;
    let mut summary = AssociationSummary {
        repos_considered: discovered.len(),
        ..AssociationSummary::default()
    };

    let repos = select_scoped_repos(&discovered, &options.repos);
    summary.repos_selected = repos.len();

    for repo_root in repos {
        let mut repo_summary = RepoSummary {
            repo_root: repo_root.clone(),
            ..RepoSummary::default()
        };

        match validate_git_repo(&repo_root) {
            Ok(Some(_)) => {}
            Ok(None) => {
                repo_summary.skipped_non_git = true;
                repo_summary.errors += 1;
                summary.errors += 1;
                summary.repos_skipped_non_git += 1;
                insert_commit_assoc_error(
                    conn,
                    &repo_root,
                    None,
                    "validate_repo",
                    "path is not a git repository",
                )?;
                summary.repo_summaries.push(repo_summary);
                continue;
            }
            Err(e) => {
                repo_summary.errors += 1;
                summary.errors += 1;
                insert_commit_assoc_error(
                    conn,
                    &repo_root,
                    None,
                    "validate_repo",
                    &e.to_string(),
                )?;
                summary.repo_summaries.push(repo_summary);
                continue;
            }
        }

        summary.repos_processed += 1;

        let Some(ai_min_ts) = min_ai_timestamp(conn, &repo_root)? else {
            summary.repo_summaries.push(repo_summary);
            continue;
        };

        let head_sha = match current_head_sha(&repo_root) {
            Ok(v) => v,
            Err(e) => {
                repo_summary.errors += 1;
                summary.errors += 1;
                insert_commit_assoc_error(conn, &repo_root, None, "head_sha", &e.to_string())?;
                summary.repo_summaries.push(repo_summary);
                continue;
            }
        };

        let cursor = get_commit_cursor(conn, &repo_root, branch_scope_head())?;
        let commit_shas = determine_commit_scope(
            &repo_root,
            &head_sha,
            cursor.as_ref(),
            &ai_min_ts,
            &options,
            conn,
        )?;

        let mut last_commit_time: Option<String> = None;
        for commit_sha in commit_shas {
            repo_summary.commits_scanned += 1;
            summary.commits_scanned += 1;

            let commit = match load_commit_diff(&repo_root, &commit_sha) {
                Ok(v) => v,
                Err(e) => {
                    repo_summary.errors += 1;
                    summary.errors += 1;
                    insert_commit_assoc_error(
                        conn,
                        &repo_root,
                        Some(&commit_sha),
                        "load_commit_diff",
                        &e.to_string(),
                    )?;
                    continue;
                }
            };
            last_commit_time = Some(commit.commit_time.clone());

            let commit_id = match upsert_git_commit_with_diffs(conn, &repo_root, &commit) {
                Ok(v) => v,
                Err(e) => {
                    repo_summary.errors += 1;
                    summary.errors += 1;
                    insert_commit_assoc_error(
                        conn,
                        &repo_root,
                        Some(&commit_sha),
                        "persist_commit_diff",
                        &e.to_string(),
                    )?;
                    continue;
                }
            };

            let (attribution, provider_rows) =
                match compute_commit_attribution(conn, &repo_root, commit_id) {
                    Ok(v) => v,
                    Err(e) => {
                        repo_summary.errors += 1;
                        summary.errors += 1;
                        insert_commit_assoc_error(
                            conn,
                            &repo_root,
                            Some(&commit_sha),
                            "compute_attribution",
                            &e.to_string(),
                        )?;
                        continue;
                    }
                };

            if let Err(e) = upsert_commit_attribution(conn, commit_id, &attribution, &provider_rows)
            {
                repo_summary.errors += 1;
                summary.errors += 1;
                insert_commit_assoc_error(
                    conn,
                    &repo_root,
                    Some(&commit_sha),
                    "persist_attribution",
                    &e.to_string(),
                )?;
                continue;
            }

            if verbose {
                eprintln!(
                    "[commit-assoc] {} {} ai_share={:.3} heavy={}",
                    repo_root, commit.commit_sha, attribution.ai_share, attribution.heavy_ai
                );
            }

            repo_summary.commits_attributed += 1;
            summary.commits_attributed += 1;
            if attribution.heavy_ai {
                repo_summary.heavy_commits += 1;
                summary.heavy_commits += 1;
            }
        }

        if last_commit_time.is_none() {
            if let Ok((head_time, _, _)) = load_commit_metadata(&repo_root, &head_sha) {
                last_commit_time = Some(head_time);
            }
        }

        if let Err(e) = upsert_commit_cursor(
            conn,
            &repo_root,
            branch_scope_head(),
            &head_sha,
            last_commit_time.as_deref(),
            Some(&ai_min_ts),
        ) {
            repo_summary.errors += 1;
            summary.errors += 1;
            insert_commit_assoc_error(conn, &repo_root, None, "upsert_cursor", &e.to_string())?;
        }

        summary.repo_summaries.push(repo_summary);
    }

    Ok(summary)
}

fn determine_commit_scope(
    repo_root: &str,
    head_sha: &str,
    cursor: Option<&types::CommitCursor>,
    ai_min_ts: &str,
    options: &RunOptions,
    conn: &Connection,
) -> Result<Vec<String>> {
    let ai_scan_from = loosen_scan_start(ai_min_ts, 1);

    if options.recompute {
        return list_commits_since(
            repo_root,
            &ai_scan_from,
            options.include_merges,
            options.max_commits,
        );
    }

    let Some(cursor) = cursor else {
        return list_commits_since(
            repo_root,
            &ai_scan_from,
            options.include_merges,
            options.max_commits,
        );
    };

    if cursor.min_ai_ts_seen.as_deref() != Some(ai_min_ts) {
        return list_commits_since(
            repo_root,
            &ai_scan_from,
            options.include_merges,
            options.max_commits,
        );
    }

    if is_ancestor(repo_root, &cursor.last_head_sha, head_sha)? {
        match list_commits_range(
            repo_root,
            &cursor.last_head_sha,
            head_sha,
            options.include_merges,
            options.max_commits,
        ) {
            Ok(v) => return Ok(v),
            Err(e) => {
                insert_commit_assoc_error(
                    conn,
                    repo_root,
                    None,
                    "incremental_range",
                    &e.to_string(),
                )?;
                return list_commits_since(
                    repo_root,
                    &ai_scan_from,
                    options.include_merges,
                    options.max_commits,
                );
            }
        }
    }

    let _last_commit_time_hint = cursor.last_commit_time.as_deref();

    list_commits_since(
        repo_root,
        &ai_scan_from,
        options.include_merges,
        options.max_commits,
    )
}

fn select_scoped_repos(discovered: &[String], requested: &[String]) -> Vec<String> {
    if requested.is_empty() {
        return discovered.to_vec();
    }

    let requested_norm: HashSet<String> = requested.iter().map(|s| normalize_path(s)).collect();
    discovered
        .iter()
        .filter(|repo| {
            let norm = normalize_path(repo);
            requested_norm.contains(repo.as_str()) || requested_norm.contains(&norm)
        })
        .cloned()
        .collect()
}

fn normalize_path(path: &str) -> String {
    let pb = PathBuf::from(path);
    let abs = if pb.is_absolute() {
        pb
    } else if let Ok(cwd) = std::env::current_dir() {
        cwd.join(pb)
    } else {
        PathBuf::from(path)
    };

    std::fs::canonicalize(&abs)
        .unwrap_or(abs)
        .to_string_lossy()
        .to_string()
}

fn loosen_scan_start(ai_min_ts: &str, days: i64) -> String {
    let Ok(dt) = DateTime::parse_from_rfc3339(ai_min_ts) else {
        return ai_min_ts.to_string();
    };

    let adjusted = dt.with_timezone(&Utc) - Duration::days(days);
    adjusted.to_rfc3339_opts(SecondsFormat::Millis, true)
}
