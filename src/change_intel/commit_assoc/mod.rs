use anyhow::Result;
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use rusqlite::{Connection, TransactionBehavior};

pub mod git_scan;
pub mod matcher;
pub mod task_matcher;
pub mod storage;
pub mod types;

use git_scan::{list_commits_since_all_local_branches, load_commit_diff, validate_git_repo};
use matcher::{compute_commit_attribution, preload_repo_match_data};
use task_matcher::{RepoBranchContext, attribute_commit_to_task, load_repo_branch_context};
use storage::{
    insert_commit_assoc_error, list_repo_roots_from_session_facts, min_ai_timestamp,
    upsert_commit_attribution, upsert_commit_task_attribution, upsert_git_commit_with_diffs,
};
use types::{AssociationSummary, RepoSummary};

pub fn run(conn: &mut Connection, verbose: bool) -> Result<AssociationSummary> {
    let repos = list_repo_roots_from_session_facts(conn)?;
    let mut summary = AssociationSummary {
        repos_considered: repos.len(),
        repos_selected: repos.len(),
        ..AssociationSummary::default()
    };

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
            Err(err) => {
                repo_summary.errors += 1;
                summary.errors += 1;
                insert_commit_assoc_error(
                    conn,
                    &repo_root,
                    None,
                    "validate_repo",
                    &err.to_string(),
                )?;
                summary.repo_summaries.push(repo_summary);
                continue;
            }
        }

        summary.repos_processed += 1;
        let branch_ctx = match load_repo_branch_context(&repo_root) {
            Ok(value) => value,
            Err(err) => {
                insert_commit_assoc_error(
                    conn,
                    &repo_root,
                    None,
                    "load_repo_branches",
                    &err.to_string(),
                )?;
                RepoBranchContext::default()
            }
        };

        let Some(ai_min_ts) = min_ai_timestamp(conn, &repo_root)? else {
            summary.repo_summaries.push(repo_summary);
            continue;
        };

        let commit_shas = match determine_commit_scope(&repo_root, &ai_min_ts) {
            Ok(value) => value,
            Err(err) => {
                repo_summary.errors += 1;
                summary.errors += 1;
                insert_commit_assoc_error(
                    conn,
                    &repo_root,
                    None,
                    "determine_commit_scope",
                    &err.to_string(),
                )?;
                summary.repo_summaries.push(repo_summary);
                continue;
            }
        };

        if let Err(err) = process_repo_commits(
            conn,
            &repo_root,
            &branch_ctx,
            &commit_shas,
            &mut repo_summary,
            &mut summary,
            verbose,
        ) {
            repo_summary.errors += 1;
            summary.errors += 1;
            insert_commit_assoc_error(
                conn,
                &repo_root,
                None,
                "commit_transaction",
                &err.to_string(),
            )?;
        }

        summary.repo_summaries.push(repo_summary);
    }

    Ok(summary)
}

fn determine_commit_scope(repo_root: &str, ai_min_ts: &str) -> Result<Vec<String>> {
    let ai_scan_from = loosen_scan_start(ai_min_ts, 1);
    list_commits_since_all_local_branches(repo_root, &ai_scan_from, false, None)
}

fn process_repo_commits(
    conn: &mut Connection,
    repo_root: &str,
    branch_ctx: &RepoBranchContext,
    commit_shas: &[String],
    repo_summary: &mut RepoSummary,
    summary: &mut AssociationSummary,
    verbose: bool,
) -> Result<()> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let match_preload = preload_repo_match_data(&tx, repo_root)?;

    for commit_sha in commit_shas {
        repo_summary.commits_scanned += 1;
        summary.commits_scanned += 1;

        let commit = match load_commit_diff(repo_root, commit_sha) {
            Ok(value) => value,
            Err(err) => {
                repo_summary.errors += 1;
                summary.errors += 1;
                insert_commit_assoc_error(
                    &tx,
                    repo_root,
                    Some(commit_sha),
                    "load_commit_diff",
                    &err.to_string(),
                )?;
                continue;
            }
        };

        if let Err(err) = upsert_git_commit_with_diffs(&tx, repo_root, &commit) {
            repo_summary.errors += 1;
            summary.errors += 1;
            insert_commit_assoc_error(
                &tx,
                repo_root,
                Some(commit_sha),
                "persist_commit_diff",
                &err.to_string(),
            )?;
            continue;
        }

        let (attribution, session_rows) =
            match compute_commit_attribution(&commit, &match_preload) {
                Ok(value) => value,
                Err(err) => {
                    repo_summary.errors += 1;
                    summary.errors += 1;
                    insert_commit_assoc_error(
                        &tx,
                        repo_root,
                        Some(commit_sha),
                        "compute_attribution",
                        &err.to_string(),
                    )?;
                    continue;
                }
            };

        if let Err(err) =
            upsert_commit_attribution(&tx, repo_root, &commit.commit_sha, &attribution, &session_rows)
        {
            repo_summary.errors += 1;
            summary.errors += 1;
            insert_commit_assoc_error(
                &tx,
                repo_root,
                Some(commit_sha),
                "persist_attribution",
                &err.to_string(),
            )?;
            continue;
        }

        match attribute_commit_to_task(repo_root, &commit.commit_sha, branch_ctx) {
            Ok(task_attr) => {
                if let Err(err) =
                    upsert_commit_task_attribution(&tx, repo_root, &commit.commit_sha, &task_attr)
                {
                    repo_summary.errors += 1;
                    summary.errors += 1;
                    insert_commit_assoc_error(
                        &tx,
                        repo_root,
                        Some(commit_sha),
                        "persist_task_attribution",
                        &err.to_string(),
                    )?;
                } else if verbose {
                    eprintln!(
                        "[commit-task] {} {} branch={} task={} fallback={} confidence={:.2}",
                        repo_root,
                        commit.commit_sha,
                        task_attr.branch_name,
                        task_attr.task_key,
                        task_attr.is_fallback,
                        task_attr.confidence
                    );
                }
            }
            Err(err) => {
                repo_summary.errors += 1;
                summary.errors += 1;
                insert_commit_assoc_error(
                    &tx,
                    repo_root,
                    Some(commit_sha),
                    "compute_task_attribution",
                    &err.to_string(),
                )?;
            }
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

    tx.commit()?;
    Ok(())
}

fn loosen_scan_start(ai_min_ts: &str, days: i64) -> String {
    let Ok(dt) = DateTime::parse_from_rfc3339(ai_min_ts) else {
        return ai_min_ts.to_string();
    };

    let adjusted = dt.with_timezone(&Utc) - Duration::days(days);
    adjusted.to_rfc3339_opts(SecondsFormat::Millis, true)
}
