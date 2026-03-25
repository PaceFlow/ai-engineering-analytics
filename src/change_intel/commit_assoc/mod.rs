use anyhow::Result;
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use rusqlite::{Connection, TransactionBehavior};
use std::collections::HashMap;

pub mod git_scan;
pub mod matcher;
pub mod storage;
pub mod task_matcher;
pub mod types;

use crate::change_intel::storage as change_storage;
use crate::ingest_progress::IngestProgressObserver;

use git_scan::{
    list_commits_since_all_local_branches, list_local_head_branch_tips, load_commit_diff,
    validate_git_repo,
};
use matcher::{compute_commit_attribution, preload_repo_match_data};
use storage::{
    insert_commit_assoc_error, list_cached_commit_shas, list_dirty_cached_commit_shas,
    list_repo_roots_from_session_facts, load_cached_commit_diff, min_ai_timestamp,
    upsert_commit_attribution, upsert_commit_task_attribution, upsert_git_commit_with_diffs,
};
use task_matcher::{RepoBranchContext, attribute_commit_to_task, load_repo_branch_context};
use types::{
    AssociationSummary, AssociationWorkPlan, CommitAssociationWorkItem, CommitDiffSource,
    GitCommitDiff, RepoAssociationPlan, RepoSummary,
};

#[allow(dead_code)]
pub fn run(conn: &mut Connection, verbose: bool) -> Result<AssociationSummary> {
    let plan = plan(conn)?;
    run_with_plan(conn, &plan, verbose, None)
}

pub fn plan(conn: &Connection) -> Result<AssociationWorkPlan> {
    let repos = list_repo_roots_from_session_facts(conn)?;
    let mut repo_plans = Vec::with_capacity(repos.len());

    for repo_root in repos {
        let mut repo_plan = RepoAssociationPlan {
            repo_root: repo_root.clone(),
            ..RepoAssociationPlan::default()
        };

        match validate_git_repo(&repo_root) {
            Ok(Some(_)) => {}
            Ok(None) => {
                repo_plan.skipped_non_git = true;
                repo_plans.push(repo_plan);
                continue;
            }
            Err(err) => {
                repo_plan.planning_error_stage = Some("validate_repo".to_string());
                repo_plan.planning_error_message = Some(err.to_string());
                repo_plans.push(repo_plan);
                continue;
            }
        }

        let Some(ai_min_ts) = min_ai_timestamp(conn, &repo_root)? else {
            repo_plans.push(repo_plan);
            continue;
        };

        let repo_state = change_storage::get_repo_assoc_state(conn, &repo_root)?;
        repo_plan.session_facts_version = repo_state.session_facts_version;

        match compute_branch_fingerprint(&repo_root) {
            Ok(fingerprint) => {
                repo_plan.branch_fingerprint_changed =
                    repo_state.task_branch_fingerprint != fingerprint;
                repo_plan.branch_fingerprint = fingerprint;
            }
            Err(err) => {
                repo_plan.planning_error_stage = Some("compute_branch_fingerprint".to_string());
                repo_plan.planning_error_message = Some(err.to_string());
                repo_plans.push(repo_plan);
                continue;
            }
        }

        let dirty_hashes = change_storage::list_dirty_hashes(conn, &repo_root)?;
        repo_plan.has_dirty_hashes = !dirty_hashes.is_empty();

        let commit_shas = match determine_commit_scope(&repo_root, &ai_min_ts) {
            Ok(commit_shas) => commit_shas,
            Err(err) => {
                repo_plan.planning_error_stage = Some("determine_commit_scope".to_string());
                repo_plan.planning_error_message = Some(err.to_string());
                repo_plans.push(repo_plan);
                continue;
            }
        };

        let cached_commit_shas = list_cached_commit_shas(conn, &repo_root)?;
        let cached_lookup: std::collections::HashSet<String> =
            cached_commit_shas.iter().cloned().collect();

        let mut work_index: HashMap<String, usize> = HashMap::new();
        let mut commits = Vec::new();

        merge_work_items(
            &mut commits,
            &mut work_index,
            commit_shas
                .into_iter()
                .filter(|commit_sha| !cached_lookup.contains(commit_sha)),
            true,
            true,
            CommitDiffSource::Git,
        );

        if repo_plan.session_facts_version > 0 && repo_plan.has_dirty_hashes {
            let dirty_commits =
                list_dirty_cached_commit_shas(conn, &repo_root, repo_plan.session_facts_version)?;
            merge_work_items(
                &mut commits,
                &mut work_index,
                dirty_commits,
                true,
                true,
                CommitDiffSource::Cache,
            );
        }

        if repo_plan.branch_fingerprint_changed {
            merge_work_items(
                &mut commits,
                &mut work_index,
                cached_commit_shas,
                false,
                true,
                CommitDiffSource::None,
            );
        }

        repo_plan.commits = commits;
        repo_plans.push(repo_plan);
    }

    Ok(AssociationWorkPlan { repo_plans })
}

pub fn run_with_plan(
    conn: &mut Connection,
    plan: &AssociationWorkPlan,
    verbose: bool,
    mut progress: Option<&mut dyn IngestProgressObserver>,
) -> Result<AssociationSummary> {
    let mut summary = AssociationSummary {
        repos_considered: plan.repo_plans.len(),
        repos_selected: plan
            .repo_plans
            .iter()
            .filter(|repo_plan| should_process_repo(repo_plan))
            .count(),
        ..AssociationSummary::default()
    };

    for repo_plan in &plan.repo_plans {
        let repo_root = repo_plan.repo_root.clone();
        let mut repo_summary = RepoSummary {
            repo_root: repo_root.clone(),
            ..RepoSummary::default()
        };

        if let Some(observer) = progress.as_mut() {
            observer.advance(&format!("repo {}", repo_root));
        }

        if repo_plan.skipped_non_git {
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

        if let (Some(stage), Some(message)) = (
            repo_plan.planning_error_stage.as_deref(),
            repo_plan.planning_error_message.as_deref(),
        ) {
            repo_summary.errors += 1;
            summary.errors += 1;
            insert_commit_assoc_error(conn, &repo_root, None, stage, message)?;
            summary.repo_summaries.push(repo_summary);
            continue;
        }

        if !should_process_repo(repo_plan) {
            continue;
        }

        summary.repos_processed += 1;

        if let Err(err) = process_repo_plan(
            conn,
            repo_plan,
            &mut repo_summary,
            &mut summary,
            verbose,
            &mut progress,
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

fn process_repo_plan(
    conn: &mut Connection,
    repo_plan: &RepoAssociationPlan,
    repo_summary: &mut RepoSummary,
    summary: &mut AssociationSummary,
    verbose: bool,
    progress: &mut Option<&mut dyn IngestProgressObserver>,
) -> Result<()> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let repo_root = repo_plan.repo_root.as_str();

    let branch_ctx = if repo_plan.commits.iter().any(|work| work.needs_task_refresh) {
        match load_repo_branch_context(repo_root) {
            Ok(value) => Some(value),
            Err(err) => {
                insert_commit_assoc_error(
                    &tx,
                    repo_root,
                    None,
                    "load_repo_branches",
                    &err.to_string(),
                )?;
                Some(RepoBranchContext::default())
            }
        }
    } else {
        None
    };

    let match_preload = if repo_plan
        .commits
        .iter()
        .any(|work| work.needs_match_refresh)
    {
        Some(preload_repo_match_data(&tx, repo_root)?)
    } else {
        None
    };

    for work in &repo_plan.commits {
        repo_summary.commits_scanned += 1;
        summary.commits_scanned += 1;

        let result = (|| -> Result<()> {
            let commit = load_commit_for_work(&tx, repo_root, work)?;

            if work.needs_match_refresh {
                let commit = commit.as_ref().ok_or_else(|| {
                    anyhow::anyhow!("missing commit diff for {}", work.commit_sha)
                })?;
                if work.diff_source == CommitDiffSource::Git {
                    upsert_git_commit_with_diffs(&tx, repo_root, commit)?;
                    repo_summary.new_commits += 1;
                    summary.new_commits += 1;
                } else if work.diff_source == CommitDiffSource::Cache {
                    repo_summary.cached_diff_reuses += 1;
                    summary.cached_diff_reuses += 1;
                    repo_summary.dirty_recomputed_commits += 1;
                    summary.dirty_recomputed_commits += 1;
                }

                let (attribution, session_rows) = compute_commit_attribution(
                    commit,
                    match_preload
                        .as_ref()
                        .expect("match preload missing for match-refresh work"),
                )?;

                upsert_commit_attribution(
                    &tx,
                    repo_root,
                    &work.commit_sha,
                    &attribution,
                    &session_rows,
                    repo_plan.session_facts_version,
                )?;

                repo_summary.commits_attributed += 1;
                summary.commits_attributed += 1;
                if attribution.heavy_ai {
                    repo_summary.heavy_commits += 1;
                    summary.heavy_commits += 1;
                }

                if verbose {
                    eprintln!(
                        "[commit-assoc] {} {} ai_share={:.3} heavy={}",
                        repo_root, work.commit_sha, attribution.ai_share, attribution.heavy_ai
                    );
                }
            } else if work.needs_task_refresh {
                repo_summary.task_only_commits += 1;
                summary.task_only_commits += 1;
            }

            if work.needs_task_refresh {
                let task_attr = attribute_commit_to_task(
                    repo_root,
                    &work.commit_sha,
                    branch_ctx
                        .as_ref()
                        .expect("branch context missing for task-refresh work"),
                )?;
                upsert_commit_task_attribution(&tx, repo_root, &work.commit_sha, &task_attr)?;
                if verbose {
                    eprintln!(
                        "[commit-task] {} {} branch={} task={} fallback={} confidence={:.2}",
                        repo_root,
                        work.commit_sha,
                        task_attr.branch_name,
                        task_attr.task_key,
                        task_attr.is_fallback,
                        task_attr.confidence
                    );
                }
            }

            Ok(())
        })();

        if let Err(err) = result {
            repo_summary.errors += 1;
            summary.errors += 1;
            let stage = if work.needs_match_refresh {
                "refresh_commit_match"
            } else if work.needs_task_refresh {
                "refresh_task_attribution"
            } else {
                "process_commit"
            };
            insert_commit_assoc_error(
                &tx,
                repo_root,
                Some(&work.commit_sha),
                stage,
                &err.to_string(),
            )?;
        }

        if let Some(observer) = progress.as_mut() {
            observer.advance(&format!(
                "{} {}",
                repo_root,
                short_commit_sha(&work.commit_sha)
            ));
        }
    }

    if repo_plan.has_dirty_hashes {
        change_storage::clear_dirty_hashes(&tx, repo_root)?;
    }
    if repo_plan.branch_fingerprint_changed || repo_plan.branch_fingerprint.is_some() {
        change_storage::upsert_repo_branch_fingerprint(
            &tx,
            repo_root,
            repo_plan.branch_fingerprint.as_deref(),
        )?;
    }

    tx.commit()?;
    Ok(())
}

fn load_commit_for_work(
    conn: &Connection,
    repo_root: &str,
    work: &CommitAssociationWorkItem,
) -> Result<Option<GitCommitDiff>> {
    match work.diff_source {
        CommitDiffSource::Git => Ok(Some(load_commit_diff(repo_root, &work.commit_sha)?)),
        CommitDiffSource::Cache => load_cached_commit_diff(conn, repo_root, &work.commit_sha),
        CommitDiffSource::None => Ok(None),
    }
}

fn compute_branch_fingerprint(repo_root: &str) -> Result<Option<String>> {
    let tips = list_local_head_branch_tips(repo_root)?;
    if tips.is_empty() {
        return Ok(None);
    }
    Ok(Some(
        tips.into_iter()
            .map(|(branch, sha)| format!("{branch}:{sha}"))
            .collect::<Vec<_>>()
            .join("|"),
    ))
}

fn should_process_repo(repo_plan: &RepoAssociationPlan) -> bool {
    !repo_plan.skipped_non_git
        && repo_plan.planning_error_stage.is_none()
        && (!repo_plan.commits.is_empty()
            || repo_plan.branch_fingerprint_changed
            || repo_plan.has_dirty_hashes)
}

fn merge_work_items<I>(
    commits: &mut Vec<CommitAssociationWorkItem>,
    work_index: &mut HashMap<String, usize>,
    commit_shas: I,
    needs_match_refresh: bool,
    needs_task_refresh: bool,
    diff_source: CommitDiffSource,
) where
    I: IntoIterator<Item = String>,
{
    for commit_sha in commit_shas {
        if let Some(index) = work_index.get(&commit_sha).copied() {
            let work = &mut commits[index];
            work.needs_match_refresh |= needs_match_refresh;
            work.needs_task_refresh |= needs_task_refresh;
            work.diff_source = merge_diff_source(work.diff_source, diff_source);
            continue;
        }

        work_index.insert(commit_sha.clone(), commits.len());
        commits.push(CommitAssociationWorkItem {
            commit_sha,
            needs_match_refresh,
            needs_task_refresh,
            diff_source,
        });
    }
}

fn merge_diff_source(current: CommitDiffSource, next: CommitDiffSource) -> CommitDiffSource {
    match (current, next) {
        (CommitDiffSource::Git, _) | (_, CommitDiffSource::Git) => CommitDiffSource::Git,
        (CommitDiffSource::Cache, _) | (_, CommitDiffSource::Cache) => CommitDiffSource::Cache,
        _ => CommitDiffSource::None,
    }
}

fn loosen_scan_start(ai_min_ts: &str, days: i64) -> String {
    let Ok(dt) = DateTime::parse_from_rfc3339(ai_min_ts) else {
        return ai_min_ts.to_string();
    };

    let adjusted = dt.with_timezone(&Utc) - Duration::days(days);
    adjusted.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn short_commit_sha(commit_sha: &str) -> &str {
    &commit_sha[..commit_sha.len().min(8)]
}
