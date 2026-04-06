use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{Duration, SecondsFormat, Utc};
use rusqlite::{Connection, TransactionBehavior, params};
use tokio::runtime::Builder;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::change_intel::line_hash::hash_line;
use crate::github::auth::github_token;
use crate::github::client::{GitHubApi, GitHubApiErrorKind, ReqwestGitHubApi};
use crate::github::types::{
    CommitLookupResult, CommitLookupStatus, GitHubRepo, GitHubSyncSummary, GitHubSyncWorkPlan,
    IssueRecord, IssueTimelineEvent, PullRequestFileRecord, PullRequestRecord,
    PullRequestRefreshResult,
};
use crate::ingest_progress::IngestProgressObserver;
use crate::sync_identity;

const GLOBAL_CONCURRENCY: usize = 8;
const PER_REPO_CONCURRENCY: usize = 2;
const OPEN_PR_REFRESH_TTL_HOURS: i64 = 6;
const ISSUE_SCAN_TTL_HOURS: i64 = 6;

type PullRequestKey = (String, i64);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CommitLookupWorkItem {
    repo: GitHubRepo,
    repo_root: String,
    commit_sha: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PullRequestRefreshWorkItem {
    repo: GitHubRepo,
    pr_number: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct IssueScanWorkItem {
    repo: GitHubRepo,
    since: String,
}

#[derive(Debug, Clone)]
struct IssueFixPullRequestLink {
    pr_number: i64,
    linked_at: Option<String>,
}

#[derive(Debug, Clone)]
struct IssueSyncRecord {
    issue: IssueRecord,
    fix_pull_requests: Vec<IssueFixPullRequestLink>,
}

#[derive(Debug, Clone)]
struct RemovedLineHashRecord {
    rel_path: String,
    line_hash: String,
    count: i64,
}

#[derive(Debug, Clone)]
struct PullRequestRemovedHashRecordSet {
    removed_hashes: Vec<RemovedLineHashRecord>,
    complete: bool,
}

#[derive(Debug, Clone)]
struct PullRequestRemovedHashRecord {
    pull_request: PullRequestRecord,
    removed_hashes: Vec<RemovedLineHashRecord>,
    removed_hashes_complete: bool,
}

#[derive(Debug, Clone)]
struct IssueScanResult {
    repo: GitHubRepo,
    issues: Vec<IssueSyncRecord>,
    pull_requests: Vec<PullRequestRemovedHashRecord>,
    last_error: Option<String>,
}

pub fn sync_github_pull_requests(
    conn: &mut Connection,
    _verbose: bool,
    progress: Option<&mut dyn IngestProgressObserver>,
) -> Result<GitHubSyncSummary> {
    let mut summary = GitHubSyncSummary {
        repos_considered: count_github_repos(conn)?,
        ..GitHubSyncSummary::default()
    };

    let Some(token) = github_token()? else {
        rebuild_commit_pr_outcomes(conn)?;
        return Ok(summary);
    };

    let api = Arc::new(ReqwestGitHubApi::new(&token)?);
    summary = sync_github_pull_requests_with_api(conn, api, progress)?;
    summary.repos_considered = count_github_repos(conn)?;
    Ok(summary)
}

pub fn plan_github_pull_requests(conn: &Connection) -> Result<GitHubSyncWorkPlan> {
    let stale_cutoff = stale_open_pr_refresh_cutoff();
    let issue_stale_cutoff = stale_issue_scan_cutoff();
    Ok(GitHubSyncWorkPlan {
        commit_lookup_units: dedupe_commit_lookup_work_items(load_commit_lookup_work_items(conn)?)
            .len(),
        pull_request_refresh_units: dedupe_pull_request_refresh_work_items(
            load_open_pull_request_refresh_items(conn, &HashSet::new(), &stale_cutoff)?,
        )
        .len(),
        issue_scan_units: dedupe_issue_scan_work_items(load_issue_scan_work_items(
            conn,
            &issue_stale_cutoff,
        )?)
        .len(),
    })
}

fn sync_github_pull_requests_with_api<A>(
    conn: &mut Connection,
    api: Arc<A>,
    mut progress: Option<&mut dyn IngestProgressObserver>,
) -> Result<GitHubSyncSummary>
where
    A: GitHubApi + Send + Sync + 'static,
{
    let mut summary = GitHubSyncSummary {
        repos_considered: count_github_repos(conn)?,
        ..GitHubSyncSummary::default()
    };

    let lookup_items = dedupe_commit_lookup_work_items(load_commit_lookup_work_items(conn)?);
    let stale_cutoff = stale_open_pr_refresh_cutoff();
    let planned_refresh_items = dedupe_pull_request_refresh_work_items(
        load_open_pull_request_refresh_items(conn, &HashSet::new(), &stale_cutoff)?,
    );
    let issue_stale_cutoff = stale_issue_scan_cutoff();
    let planned_issue_scan_items =
        dedupe_issue_scan_work_items(load_issue_scan_work_items(conn, &issue_stale_cutoff)?);
    summary.commit_lookups_enqueued = lookup_items.len();
    summary.issue_scans_enqueued = planned_issue_scan_items.len();
    let mut lookup_results = Vec::new();
    if !lookup_items.is_empty() {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .context("build GitHub sync runtime")?;
        lookup_results = if let Some(observer) = progress.as_mut() {
            runtime.block_on(run_commit_lookup_jobs(
                Arc::clone(&api),
                lookup_items.clone(),
                Some(&mut **observer),
            ))
        } else {
            runtime.block_on(run_commit_lookup_jobs(
                Arc::clone(&api),
                lookup_items.clone(),
                None,
            ))
        };
        summary.commit_lookups_completed = lookup_results.len();
        persist_commit_lookup_results(conn, &lookup_items, &lookup_results, &mut summary)?;
    }
    let touched_pull_requests = touched_open_pull_request_keys(&lookup_results);
    let refresh_items = dedupe_pull_request_refresh_work_items(
        load_open_pull_request_refresh_items(conn, &touched_pull_requests, &stale_cutoff)?,
    );
    if let Some(observer) = progress.as_mut() {
        observer.replace_future_units(planned_refresh_items.len(), refresh_items.len());
    }
    if !refresh_items.is_empty() {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .context("build GitHub refresh runtime")?;
        let refresh_results = if let Some(observer) = progress.as_mut() {
            runtime.block_on(run_pull_request_refresh_jobs(
                Arc::clone(&api),
                refresh_items.clone(),
                Some(&mut **observer),
            ))
        } else {
            runtime.block_on(run_pull_request_refresh_jobs(
                Arc::clone(&api),
                refresh_items.clone(),
                None,
            ))
        };
        persist_pull_request_refresh_results(conn, &refresh_items, &refresh_results, &mut summary)?;
    }
    let issue_scan_items =
        dedupe_issue_scan_work_items(load_issue_scan_work_items(conn, &issue_stale_cutoff)?);
    if let Some(observer) = progress.as_mut() {
        observer.replace_future_units(planned_issue_scan_items.len(), issue_scan_items.len());
    }
    if !issue_scan_items.is_empty() {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .context("build GitHub issue-scan runtime")?;
        let issue_scan_results = if let Some(observer) = progress.as_mut() {
            runtime.block_on(run_issue_scan_jobs(
                Arc::clone(&api),
                issue_scan_items.clone(),
                Some(&mut **observer),
            ))
        } else {
            runtime.block_on(run_issue_scan_jobs(
                Arc::clone(&api),
                issue_scan_items.clone(),
                None,
            ))
        };
        persist_issue_scan_results(conn, &issue_scan_items, &issue_scan_results, &mut summary)?;
    }

    rebuild_commit_pr_outcomes(conn)?;
    Ok(summary)
}

async fn run_commit_lookup_jobs<A>(
    api: Arc<A>,
    items: Vec<CommitLookupWorkItem>,
    mut progress: Option<&mut dyn IngestProgressObserver>,
) -> Vec<CommitLookupResult>
where
    A: GitHubApi + Send + Sync + 'static,
{
    let global = Arc::new(Semaphore::new(GLOBAL_CONCURRENCY));
    let mut per_repo = HashMap::<String, Arc<Semaphore>>::new();
    let mut jobs = JoinSet::new();

    for item in items {
        let api = Arc::clone(&api);
        let global_limit = Arc::clone(&global);
        let repo_limit = Arc::clone(
            per_repo
                .entry(item.repo.repo_key.clone())
                .or_insert_with(|| Arc::new(Semaphore::new(PER_REPO_CONCURRENCY))),
        );
        jobs.spawn(async move {
            let _global_permit = global_limit.acquire_owned().await.ok();
            let _repo_permit = repo_limit.acquire_owned().await.ok();
            match api.fetch_commit_pulls(&item.repo, &item.commit_sha).await {
                Ok(pulls) => {
                    let owning_pr_number = choose_owning_pr(&pulls).map(|pr| pr.number);
                    let status = if pulls.is_empty() {
                        CommitLookupStatus::NoPr
                    } else {
                        CommitLookupStatus::Resolved
                    };
                    CommitLookupResult {
                        repo: item.repo,
                        commit_sha: item.commit_sha,
                        status,
                        owning_pr_number,
                        pull_requests: pulls,
                        last_error: None,
                    }
                }
                Err(err) => CommitLookupResult {
                    repo: item.repo,
                    commit_sha: item.commit_sha,
                    status: if err.kind == GitHubApiErrorKind::RateLimited {
                        CommitLookupStatus::RateLimited
                    } else {
                        CommitLookupStatus::Failed
                    },
                    owning_pr_number: None,
                    pull_requests: Vec::new(),
                    last_error: Some(err.message),
                },
            }
        });
    }

    let mut results = Vec::new();
    while let Some(result) = jobs.join_next().await {
        if let Ok(value) = result {
            if let Some(observer) = progress.as_deref_mut() {
                observer.advance(&format_commit_lookup_progress_label(&value));
            }
            results.push(value);
        }
    }
    results
}

async fn run_pull_request_refresh_jobs<A>(
    api: Arc<A>,
    items: Vec<PullRequestRefreshWorkItem>,
    mut progress: Option<&mut dyn IngestProgressObserver>,
) -> Vec<PullRequestRefreshResult>
where
    A: GitHubApi + Send + Sync + 'static,
{
    let global = Arc::new(Semaphore::new(GLOBAL_CONCURRENCY));
    let mut per_repo = HashMap::<String, Arc<Semaphore>>::new();
    let mut jobs = JoinSet::new();

    for item in items {
        let api = Arc::clone(&api);
        let global_limit = Arc::clone(&global);
        let repo_limit = Arc::clone(
            per_repo
                .entry(item.repo.repo_key.clone())
                .or_insert_with(|| Arc::new(Semaphore::new(PER_REPO_CONCURRENCY))),
        );
        jobs.spawn(async move {
            let _global_permit = global_limit.acquire_owned().await.ok();
            let _repo_permit = repo_limit.acquire_owned().await.ok();
            let pull_request = api
                .fetch_pull_request(&item.repo, item.pr_number)
                .await
                .ok();
            (item, pull_request)
        });
    }

    let mut results = Vec::new();
    while let Some(result) = jobs.join_next().await {
        if let Ok((item, pull_request)) = result {
            if let Some(observer) = progress.as_deref_mut() {
                observer.advance(&format_pull_request_refresh_progress_label(&item));
            }
            if let Some(pull_request) = pull_request {
                results.push(PullRequestRefreshResult {
                    repo: item.repo,
                    pull_request,
                });
            }
        }
    }
    results
}

async fn run_issue_scan_jobs<A>(
    api: Arc<A>,
    items: Vec<IssueScanWorkItem>,
    mut progress: Option<&mut dyn IngestProgressObserver>,
) -> Vec<IssueScanResult>
where
    A: GitHubApi + Send + Sync + 'static,
{
    let global = Arc::new(Semaphore::new(GLOBAL_CONCURRENCY));
    let mut per_repo = HashMap::<String, Arc<Semaphore>>::new();
    let mut jobs = JoinSet::new();

    for item in items {
        let api = Arc::clone(&api);
        let global_limit = Arc::clone(&global);
        let repo_limit = Arc::clone(
            per_repo
                .entry(item.repo.repo_key.clone())
                .or_insert_with(|| Arc::new(Semaphore::new(PER_REPO_CONCURRENCY))),
        );
        jobs.spawn(async move {
            let _global_permit = global_limit.acquire_owned().await.ok();
            let _repo_permit = repo_limit.acquire_owned().await.ok();
            scan_bug_issues_for_repo(api.as_ref(), item).await
        });
    }

    let mut results = Vec::new();
    while let Some(result) = jobs.join_next().await {
        if let Ok(value) = result {
            if let Some(observer) = progress.as_deref_mut() {
                observer.advance(&format_issue_scan_progress_label(&value.repo));
            }
            results.push(value);
        }
    }
    results
}

fn format_commit_lookup_progress_label(result: &CommitLookupResult) -> String {
    format!(
        "{}/{} {}",
        result.repo.owner,
        result.repo.name,
        short_commit_sha(&result.commit_sha)
    )
}

fn format_pull_request_refresh_progress_label(item: &PullRequestRefreshWorkItem) -> String {
    format!("{}/{} #{}", item.repo.owner, item.repo.name, item.pr_number)
}

fn format_issue_scan_progress_label(repo: &GitHubRepo) -> String {
    format!("{}/{} issues", repo.owner, repo.name)
}

fn short_commit_sha(commit_sha: &str) -> &str {
    &commit_sha[..commit_sha.len().min(8)]
}

fn stale_open_pr_refresh_cutoff() -> String {
    (Utc::now() - Duration::hours(OPEN_PR_REFRESH_TTL_HOURS))
        .to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn stale_issue_scan_cutoff() -> String {
    (Utc::now() - Duration::hours(ISSUE_SCAN_TTL_HOURS))
        .to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn touched_open_pull_request_keys(results: &[CommitLookupResult]) -> HashSet<PullRequestKey> {
    let mut touched = HashSet::new();
    for result in results {
        let Some(pr_number) = result.owning_pr_number else {
            continue;
        };
        let is_open = result.pull_requests.iter().any(|pull_request| {
            pull_request.number == pr_number
                && pull_request.state == "open"
                && pull_request.merged_at.is_none()
        });
        if is_open {
            touched.insert((result.repo.repo_key.clone(), pr_number));
        }
    }
    touched
}

fn persist_commit_lookup_results(
    conn: &mut Connection,
    work_items: &[CommitLookupWorkItem],
    results: &[CommitLookupResult],
    summary: &mut GitHubSyncSummary,
) -> Result<()> {
    let checked_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let mut upsert_pr = tx.prepare_cached(
        "INSERT INTO fact_github_pull_request (
            repo_key, pr_number, state, draft_flag, created_at, updated_at, closed_at,
            merged_at, base_ref, head_ref, html_url, removed_hashes_complete_flag
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
         ON CONFLICT(repo_key, pr_number) DO UPDATE SET
            state = excluded.state,
            draft_flag = excluded.draft_flag,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at,
            closed_at = excluded.closed_at,
            merged_at = excluded.merged_at,
            base_ref = excluded.base_ref,
            head_ref = excluded.head_ref,
            html_url = excluded.html_url,
            removed_hashes_complete_flag = COALESCE(
                excluded.removed_hashes_complete_flag,
                fact_github_pull_request.removed_hashes_complete_flag
            )",
    )?;
    let mut upsert_pr_commit = tx.prepare_cached(
        "INSERT INTO fact_github_pull_request_commit (repo_key, pr_number, commit_sha, commit_position)
         VALUES (?1, ?2, ?3, NULL)
         ON CONFLICT(repo_key, pr_number, commit_sha) DO UPDATE SET
            commit_position = excluded.commit_position",
    )?;
    let mut upsert_lookup = tx.prepare_cached(
        "INSERT INTO fact_github_commit_pr_lookup (
            repo_key, commit_sha, status, owning_pr_number, last_checked_at, last_error
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(repo_key, commit_sha) DO UPDATE SET
            status = excluded.status,
            owning_pr_number = excluded.owning_pr_number,
            last_checked_at = excluded.last_checked_at,
            last_error = excluded.last_error",
    )?;

    let mut repo_errors = HashMap::<String, Option<String>>::new();

    for result in results {
        for pull_request in &result.pull_requests {
            upsert_pull_request(&mut upsert_pr, &result.repo.repo_key, pull_request, None)?;
            summary.pull_requests_upserted += 1;
            upsert_pr_commit.execute(params![
                result.repo.repo_key,
                pull_request.number,
                result.commit_sha
            ])?;
            summary.pull_request_commits_upserted += 1;
        }

        upsert_lookup.execute(params![
            result.repo.repo_key,
            result.commit_sha,
            result.status.as_str(),
            result.owning_pr_number,
            checked_at,
            result.last_error
        ])?;

        match result.status {
            CommitLookupStatus::Resolved => summary.resolved_commits += 1,
            CommitLookupStatus::NoPr => summary.no_pr_commits += 1,
            CommitLookupStatus::Failed => summary.failed_commits += 1,
            CommitLookupStatus::RateLimited => summary.rate_limited_commits += 1,
        }
        repo_errors.insert(result.repo.repo_key.clone(), result.last_error.clone());
    }

    for item in work_items {
        repo_errors
            .entry(item.repo.repo_key.clone())
            .or_insert(None);
    }
    upsert_sync_state_commit_scan(&tx, &repo_errors, &checked_at)?;
    drop(upsert_pr);
    drop(upsert_pr_commit);
    drop(upsert_lookup);
    tx.commit()?;
    Ok(())
}

fn persist_pull_request_refresh_results(
    conn: &mut Connection,
    work_items: &[PullRequestRefreshWorkItem],
    results: &[PullRequestRefreshResult],
    summary: &mut GitHubSyncSummary,
) -> Result<()> {
    if work_items.is_empty() && results.is_empty() {
        return Ok(());
    }

    let refreshed_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let mut upsert_pr = tx.prepare_cached(
        "INSERT INTO fact_github_pull_request (
            repo_key, pr_number, state, draft_flag, created_at, updated_at, closed_at,
            merged_at, base_ref, head_ref, html_url, removed_hashes_complete_flag
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
         ON CONFLICT(repo_key, pr_number) DO UPDATE SET
            state = excluded.state,
            draft_flag = excluded.draft_flag,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at,
            closed_at = excluded.closed_at,
            merged_at = excluded.merged_at,
            base_ref = excluded.base_ref,
            head_ref = excluded.head_ref,
            html_url = excluded.html_url,
            removed_hashes_complete_flag = COALESCE(
                excluded.removed_hashes_complete_flag,
                fact_github_pull_request.removed_hashes_complete_flag
            )",
    )?;
    for result in results {
        upsert_pull_request(
            &mut upsert_pr,
            &result.repo.repo_key,
            &result.pull_request,
            None,
        )?;
        summary.open_pull_requests_refreshed += 1;
    }

    let repo_errors = work_items
        .iter()
        .map(|item| (item.repo.repo_key.clone(), None))
        .collect::<HashMap<_, _>>();
    upsert_sync_state_open_refresh(&tx, &repo_errors, &refreshed_at)?;
    drop(upsert_pr);
    tx.commit()?;
    Ok(())
}

fn persist_issue_scan_results(
    conn: &mut Connection,
    work_items: &[IssueScanWorkItem],
    results: &[IssueScanResult],
    summary: &mut GitHubSyncSummary,
) -> Result<()> {
    if work_items.is_empty() && results.is_empty() {
        return Ok(());
    }

    let scanned_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let mut upsert_issue = tx.prepare_cached(
        "INSERT INTO fact_github_issue (
            repo_key, issue_number, state, created_at, updated_at, closed_at, is_pull_request_flag,
            bug_candidate_flag
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT(repo_key, issue_number) DO UPDATE SET
            state = excluded.state,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at,
            closed_at = excluded.closed_at,
            is_pull_request_flag = excluded.is_pull_request_flag,
            bug_candidate_flag = excluded.bug_candidate_flag",
    )?;
    let mut delete_issue_links = tx.prepare_cached(
        "DELETE FROM fact_github_issue_fix_pull_request
         WHERE repo_key = ?1 AND issue_number = ?2",
    )?;
    let mut insert_issue_link = tx.prepare_cached(
        "INSERT INTO fact_github_issue_fix_pull_request (
            repo_key, issue_number, pr_number, linked_at
         ) VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(repo_key, issue_number, pr_number) DO UPDATE SET
            linked_at = excluded.linked_at",
    )?;
    let mut upsert_pr = tx.prepare_cached(
        "INSERT INTO fact_github_pull_request (
            repo_key, pr_number, state, draft_flag, created_at, updated_at, closed_at,
            merged_at, base_ref, head_ref, html_url, removed_hashes_complete_flag
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
         ON CONFLICT(repo_key, pr_number) DO UPDATE SET
            state = excluded.state,
            draft_flag = excluded.draft_flag,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at,
            closed_at = excluded.closed_at,
            merged_at = excluded.merged_at,
            base_ref = excluded.base_ref,
            head_ref = excluded.head_ref,
            html_url = excluded.html_url,
            removed_hashes_complete_flag = COALESCE(
                excluded.removed_hashes_complete_flag,
                fact_github_pull_request.removed_hashes_complete_flag
            )",
    )?;
    let mut delete_removed_hashes = tx.prepare_cached(
        "DELETE FROM fact_github_pull_request_removed_line_hash
         WHERE repo_key = ?1 AND pr_number = ?2",
    )?;
    let mut insert_removed_hash = tx.prepare_cached(
        "INSERT INTO fact_github_pull_request_removed_line_hash (
            repo_key, pr_number, rel_path, line_hash, count
         ) VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(repo_key, pr_number, rel_path, line_hash) DO UPDATE SET
            count = excluded.count",
    )?;

    let mut repo_errors = HashMap::<String, Option<String>>::new();
    for result in results {
        repo_errors.insert(result.repo.repo_key.clone(), result.last_error.clone());
        if result.last_error.is_some() {
            continue;
        }
        summary.issue_scans_completed += 1;
        summary.issue_scans_refreshed += 1;

        for issue in &result.issues {
            upsert_issue.execute(params![
                result.repo.repo_key,
                issue.issue.number,
                issue.issue.state,
                issue.issue.created_at,
                issue.issue.updated_at,
                issue.issue.closed_at,
                i64::from(issue.issue.is_pull_request),
                i64::from(is_bug_issue_candidate(&issue.issue))
            ])?;
            summary.issues_upserted += 1;

            delete_issue_links.execute(params![result.repo.repo_key, issue.issue.number])?;
            for link in &issue.fix_pull_requests {
                insert_issue_link.execute(params![
                    result.repo.repo_key,
                    issue.issue.number,
                    link.pr_number,
                    link.linked_at
                ])?;
                summary.issue_fix_pull_requests_upserted += 1;
            }
        }

        for pull_request in &result.pull_requests {
            upsert_pull_request(
                &mut upsert_pr,
                &result.repo.repo_key,
                &pull_request.pull_request,
                Some(pull_request.removed_hashes_complete),
            )?;
            summary.pull_requests_upserted += 1;

            delete_removed_hashes.execute(params![
                result.repo.repo_key,
                pull_request.pull_request.number
            ])?;
            for hash in &pull_request.removed_hashes {
                insert_removed_hash.execute(params![
                    result.repo.repo_key,
                    pull_request.pull_request.number,
                    hash.rel_path,
                    hash.line_hash,
                    hash.count
                ])?;
                summary.pull_request_removed_hashes_upserted += 1;
            }
        }
    }

    for item in work_items {
        repo_errors
            .entry(item.repo.repo_key.clone())
            .or_insert(None);
    }

    upsert_sync_state_issue_scan(&tx, &repo_errors, &scanned_at)?;
    drop(upsert_issue);
    drop(delete_issue_links);
    drop(insert_issue_link);
    drop(upsert_pr);
    drop(delete_removed_hashes);
    drop(insert_removed_hash);
    tx.commit()?;
    Ok(())
}

fn upsert_pull_request(
    statement: &mut rusqlite::CachedStatement<'_>,
    repo_key: &str,
    pull_request: &PullRequestRecord,
    removed_hashes_complete_flag: Option<bool>,
) -> Result<()> {
    statement.execute(params![
        repo_key,
        pull_request.number,
        pull_request.state,
        i64::from(pull_request.draft),
        pull_request.created_at,
        pull_request.updated_at,
        pull_request.closed_at,
        pull_request.merged_at,
        pull_request.base_ref,
        pull_request.head_ref,
        pull_request.html_url,
        removed_hashes_complete_flag.map(i64::from)
    ])?;
    Ok(())
}

fn upsert_sync_state_commit_scan(
    tx: &rusqlite::Transaction<'_>,
    repo_errors: &HashMap<String, Option<String>>,
    checked_at: &str,
) -> Result<()> {
    let mut statement = tx.prepare_cached(
        "INSERT INTO fact_github_sync_state (
            repo_key, last_commit_scan_at, last_open_pr_refresh_at, last_error, updated_at
         ) VALUES (?1, ?2, NULL, ?3, ?4)
         ON CONFLICT(repo_key) DO UPDATE SET
            last_commit_scan_at = excluded.last_commit_scan_at,
            last_error = excluded.last_error,
            updated_at = excluded.updated_at",
    )?;
    for (repo_key, error) in repo_errors {
        statement.execute(params![repo_key, checked_at, error, checked_at])?;
    }
    Ok(())
}

fn upsert_sync_state_open_refresh(
    tx: &rusqlite::Transaction<'_>,
    repo_errors: &HashMap<String, Option<String>>,
    refreshed_at: &str,
) -> Result<()> {
    let mut statement = tx.prepare_cached(
        "INSERT INTO fact_github_sync_state (
            repo_key, last_commit_scan_at, last_open_pr_refresh_at, last_error, updated_at
         ) VALUES (?1, NULL, ?2, ?3, ?4)
         ON CONFLICT(repo_key) DO UPDATE SET
            last_open_pr_refresh_at = excluded.last_open_pr_refresh_at,
            last_error = excluded.last_error,
            updated_at = excluded.updated_at",
    )?;
    for (repo_key, error) in repo_errors {
        statement.execute(params![repo_key, refreshed_at, error, refreshed_at])?;
    }
    Ok(())
}

fn upsert_sync_state_issue_scan(
    tx: &rusqlite::Transaction<'_>,
    repo_errors: &HashMap<String, Option<String>>,
    scanned_at: &str,
) -> Result<()> {
    let mut statement = tx.prepare_cached(
        "INSERT INTO fact_github_sync_state (
            repo_key, last_commit_scan_at, last_open_pr_refresh_at, last_issue_scan_at, last_error, updated_at
         ) VALUES (?1, NULL, NULL, ?2, ?3, ?4)
         ON CONFLICT(repo_key) DO UPDATE SET
            last_issue_scan_at = CASE
                WHEN excluded.last_error IS NULL THEN excluded.last_issue_scan_at
                ELSE fact_github_sync_state.last_issue_scan_at
            END,
            last_error = excluded.last_error,
            updated_at = excluded.updated_at",
    )?;
    for (repo_key, error) in repo_errors {
        statement.execute(params![repo_key, scanned_at, error, scanned_at])?;
    }
    Ok(())
}

fn rebuild_commit_pr_outcomes(conn: &Connection) -> Result<()> {
    conn.execute("DELETE FROM event_commit_pr_outcome", [])?;
    conn.execute(
        "INSERT INTO event_commit_pr_outcome (
            repo_root, repo_key, commit_sha, lookup_status, pr_number, pr_opened_flag,
            pr_merged_flag, pr_created_at, pr_merged_at
         )
         SELECT
            o.repo_root,
            o.repo_key,
            o.commit_sha,
            lu.status,
            lu.owning_pr_number,
            CASE WHEN lu.status = 'resolved' THEN 1 ELSE 0 END,
            CASE WHEN lu.status = 'resolved' AND pr.merged_at IS NOT NULL THEN 1 ELSE 0 END,
            pr.created_at,
            pr.merged_at
         FROM event_commit_outcome o
         JOIN fact_github_commit_pr_lookup lu
           ON lu.repo_key = o.repo_key
          AND lu.commit_sha = o.commit_sha
         LEFT JOIN fact_github_pull_request pr
           ON pr.repo_key = lu.repo_key
          AND pr.pr_number = lu.owning_pr_number
         WHERE o.heavy_ai_flag = 1
           AND o.repo_key LIKE 'git:github.com/%'
           AND lu.status IN ('resolved', 'no_pr')",
        [],
    )?;
    Ok(())
}

fn count_github_repos(conn: &Connection) -> Result<usize> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(DISTINCT repo_key)
         FROM event_commit_outcome
         WHERE heavy_ai_flag = 1
           AND repo_key LIKE 'git:github.com/%'",
        [],
        |row| row.get(0),
    )?;
    Ok(count.max(0) as usize)
}

fn load_commit_lookup_work_items(conn: &Connection) -> Result<Vec<CommitLookupWorkItem>> {
    let mut statement = conn.prepare(
        "SELECT DISTINCT o.repo_root, o.repo_key, o.commit_sha
         FROM event_commit_outcome o
         LEFT JOIN fact_github_commit_pr_lookup lu
           ON lu.repo_key = o.repo_key
          AND lu.commit_sha = o.commit_sha
         WHERE o.heavy_ai_flag = 1
           AND o.repo_key LIKE 'git:github.com/%'
           AND COALESCE(lu.status, '') != 'resolved'
         ORDER BY o.repo_key, o.commit_sha",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;

    let mut items = Vec::new();
    for row in rows {
        let (repo_root, repo_key, commit_sha) = row?;
        let Some((owner, name)) = sync_identity::github_repo_from_repo_key(&repo_key) else {
            continue;
        };
        items.push(CommitLookupWorkItem {
            repo: GitHubRepo {
                repo_key,
                owner,
                name,
            },
            repo_root,
            commit_sha,
        });
    }
    Ok(items)
}

fn load_open_pull_request_refresh_items(
    conn: &Connection,
    touched_pull_requests: &HashSet<PullRequestKey>,
    stale_cutoff: &str,
) -> Result<Vec<PullRequestRefreshWorkItem>> {
    let mut statement = conn.prepare(
        "SELECT pr.repo_key, pr.pr_number, ss.last_open_pr_refresh_at
         FROM fact_github_pull_request pr
         LEFT JOIN fact_github_sync_state ss
           ON ss.repo_key = pr.repo_key
         WHERE pr.repo_key LIKE 'git:github.com/%'
           AND pr.state = 'open'
           AND pr.merged_at IS NULL
         ORDER BY pr.repo_key, pr.pr_number",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, Option<String>>(2)?,
        ))
    })?;

    let mut items = Vec::new();
    for row in rows {
        let (repo_key, pr_number, last_open_pr_refresh_at) = row?;
        let key = (repo_key.clone(), pr_number);
        let repo_is_stale = last_open_pr_refresh_at
            .as_deref()
            .map(|value| value < stale_cutoff)
            .unwrap_or(true);
        if !repo_is_stale && !touched_pull_requests.contains(&key) {
            continue;
        }
        let Some((owner, name)) = sync_identity::github_repo_from_repo_key(&repo_key) else {
            continue;
        };
        items.push(PullRequestRefreshWorkItem {
            repo: GitHubRepo {
                repo_key,
                owner,
                name,
            },
            pr_number,
        });
    }
    Ok(items)
}

fn load_issue_scan_work_items(
    conn: &Connection,
    stale_cutoff: &str,
) -> Result<Vec<IssueScanWorkItem>> {
    let mut statement = conn.prepare(
        "SELECT o.repo_key, MIN(o.commit_time), ss.last_issue_scan_at
         FROM event_commit_outcome o
         LEFT JOIN fact_github_sync_state ss
           ON ss.repo_key = o.repo_key
         WHERE o.heavy_ai_flag = 1
           AND o.merged_to_mainline_flag = 1
           AND o.repo_key LIKE 'git:github.com/%'
         GROUP BY o.repo_key, ss.last_issue_scan_at
         ORDER BY o.repo_key",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
        ))
    })?;

    let mut items = Vec::new();
    for row in rows {
        let (repo_key, earliest_commit_time, last_issue_scan_at) = row?;
        let repo_is_stale = last_issue_scan_at
            .as_deref()
            .map(|value| value < stale_cutoff)
            .unwrap_or(true);
        if !repo_is_stale {
            continue;
        }
        let Some((owner, name)) = sync_identity::github_repo_from_repo_key(&repo_key) else {
            continue;
        };
        items.push(IssueScanWorkItem {
            repo: GitHubRepo {
                repo_key,
                owner,
                name,
            },
            since: issue_scan_since(last_issue_scan_at.as_deref(), &earliest_commit_time),
        });
    }
    Ok(items)
}

fn dedupe_commit_lookup_work_items(items: Vec<CommitLookupWorkItem>) -> Vec<CommitLookupWorkItem> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::new();
    for item in items {
        let key = (item.repo.repo_key.clone(), item.commit_sha.clone());
        if seen.insert(key) {
            deduped.push(item);
        }
    }
    deduped
}

fn dedupe_pull_request_refresh_work_items(
    items: Vec<PullRequestRefreshWorkItem>,
) -> Vec<PullRequestRefreshWorkItem> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::new();
    for item in items {
        let key = (item.repo.repo_key.clone(), item.pr_number);
        if seen.insert(key) {
            deduped.push(item);
        }
    }
    deduped
}

fn dedupe_issue_scan_work_items(items: Vec<IssueScanWorkItem>) -> Vec<IssueScanWorkItem> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::new();
    for item in items {
        if seen.insert(item.repo.repo_key.clone()) {
            deduped.push(item);
        }
    }
    deduped
}

fn choose_owning_pr(pull_requests: &[PullRequestRecord]) -> Option<&PullRequestRecord> {
    pull_requests.iter().min_by(|left, right| {
        left.created_at
            .as_deref()
            .unwrap_or("9999-12-31T23:59:59Z")
            .cmp(
                right
                    .created_at
                    .as_deref()
                    .unwrap_or("9999-12-31T23:59:59Z"),
            )
            .then_with(|| left.number.cmp(&right.number))
    })
}

fn issue_scan_since(last_issue_scan_at: Option<&str>, earliest_commit_time: &str) -> String {
    last_issue_scan_at
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .map(|value| {
            (value.with_timezone(&Utc) - Duration::days(1))
                .to_rfc3339_opts(SecondsFormat::Millis, true)
        })
        .unwrap_or_else(|| earliest_commit_time.to_string())
}

fn is_bug_issue_candidate(issue: &IssueRecord) -> bool {
    issue.label_names.iter().any(|label| {
        matches!(
            label.trim().to_ascii_lowercase().as_str(),
            "bug" | "type:bug" | "defect"
        )
    })
}

fn extract_fix_pull_request_links(events: &[IssueTimelineEvent]) -> Vec<IssueFixPullRequestLink> {
    let mut links = BTreeMap::<i64, Option<String>>::new();
    for event in events {
        if event.event != "cross-referenced" {
            continue;
        }
        let Some(pr_number) = event.source_pr_number else {
            continue;
        };
        let entry = links
            .entry(pr_number)
            .or_insert_with(|| event.created_at.clone());
        if entry.is_none() {
            *entry = event.created_at.clone();
        }
    }
    links
        .into_iter()
        .map(|(pr_number, linked_at)| IssueFixPullRequestLink {
            pr_number,
            linked_at,
        })
        .collect()
}

fn collect_removed_hashes(files: &[PullRequestFileRecord]) -> PullRequestRemovedHashRecordSet {
    let mut counts = BTreeMap::<(String, String), i64>::new();
    let mut complete = true;
    for file in files {
        let Some(rel_path) = removed_hash_rel_path(file) else {
            continue;
        };
        let Some(patch) = file.patch.as_deref() else {
            if file_requires_patch_for_removed_hashes(file) {
                complete = false;
            }
            continue;
        };
        for line in patch.lines() {
            if line.starts_with('-') && !line.starts_with("---") {
                let line_hash = hash_line(line.strip_prefix('-').unwrap_or(line));
                *counts.entry((rel_path.to_string(), line_hash)).or_insert(0) += 1;
            }
        }
    }

    PullRequestRemovedHashRecordSet {
        removed_hashes: counts
            .into_iter()
            .map(|((rel_path, line_hash), count)| RemovedLineHashRecord {
                rel_path,
                line_hash,
                count,
            })
            .collect(),
        complete,
    }
}

fn removed_hash_rel_path(file: &PullRequestFileRecord) -> Option<&str> {
    match file.status.as_str() {
        "removed" | "renamed" => file
            .previous_filename
            .as_deref()
            .or(Some(file.filename.as_str())),
        _ => Some(file.filename.as_str()),
    }
}

fn file_requires_patch_for_removed_hashes(file: &PullRequestFileRecord) -> bool {
    file.status != "added"
}

async fn scan_bug_issues_for_repo<A>(api: &A, item: IssueScanWorkItem) -> IssueScanResult
where
    A: GitHubApi + Send + Sync + 'static,
{
    let issues = match api.fetch_closed_issues(&item.repo, Some(&item.since)).await {
        Ok(issues) => issues,
        Err(err) => {
            return IssueScanResult {
                repo: item.repo,
                issues: Vec::new(),
                pull_requests: Vec::new(),
                last_error: Some(err.message),
            };
        }
    };

    let mut synced_issues = Vec::with_capacity(issues.len());
    let mut needed_pull_requests = BTreeMap::<i64, ()>::new();

    for issue in issues {
        let fix_pull_requests = if !issue.is_pull_request && is_bug_issue_candidate(&issue) {
            match api.fetch_issue_timeline(&item.repo, issue.number).await {
                Ok(events) => extract_fix_pull_request_links(&events),
                Err(err) => {
                    return IssueScanResult {
                        repo: item.repo,
                        issues: Vec::new(),
                        pull_requests: Vec::new(),
                        last_error: Some(err.message),
                    };
                }
            }
        } else {
            Vec::new()
        };

        for link in &fix_pull_requests {
            needed_pull_requests.insert(link.pr_number, ());
        }

        synced_issues.push(IssueSyncRecord {
            issue,
            fix_pull_requests,
        });
    }

    let mut synced_pull_requests = Vec::new();
    for pr_number in needed_pull_requests.keys().copied() {
        let pull_request = match api.fetch_pull_request(&item.repo, pr_number).await {
            Ok(pull_request) => pull_request,
            Err(err) => {
                return IssueScanResult {
                    repo: item.repo,
                    issues: Vec::new(),
                    pull_requests: Vec::new(),
                    last_error: Some(err.message),
                };
            }
        };
        let files = match api.fetch_pull_request_files(&item.repo, pr_number).await {
            Ok(files) => files,
            Err(err) => {
                return IssueScanResult {
                    repo: item.repo,
                    issues: Vec::new(),
                    pull_requests: Vec::new(),
                    last_error: Some(err.message),
                };
            }
        };
        let removed_hashes = collect_removed_hashes(&files);
        synced_pull_requests.push(PullRequestRemovedHashRecord {
            pull_request,
            removed_hashes: removed_hashes.removed_hashes,
            removed_hashes_complete: removed_hashes.complete,
        });
    }

    IssueScanResult {
        repo: item.repo,
        issues: synced_issues,
        pull_requests: synced_pull_requests,
        last_error: None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::change_intel::line_hash::hash_line;
    use crate::db::init_app_schema;
    use crate::ingest_progress::IngestProgressObserver;

    type CommitLookupResponse =
        std::result::Result<Vec<PullRequestRecord>, crate::github::client::GitHubApiError>;
    type PullRefreshResponse =
        std::result::Result<PullRequestRecord, crate::github::client::GitHubApiError>;
    type IssueListResponse =
        std::result::Result<Vec<IssueRecord>, crate::github::client::GitHubApiError>;
    type IssueTimelineResponse =
        std::result::Result<Vec<IssueTimelineEvent>, crate::github::client::GitHubApiError>;
    type PullFilesResponse =
        std::result::Result<Vec<PullRequestFileRecord>, crate::github::client::GitHubApiError>;

    #[derive(Default)]
    struct FakeGitHubApi {
        commit_calls: Mutex<Vec<(String, String)>>,
        pull_calls: Mutex<Vec<(String, i64)>>,
        issue_calls: Mutex<Vec<(String, String)>>,
        timeline_calls: Mutex<Vec<(String, i64)>>,
        pull_file_calls: Mutex<Vec<(String, i64)>>,
        commit_pulls: Mutex<HashMap<(String, String), CommitLookupResponse>>,
        pull_responses: Mutex<HashMap<(String, i64), PullRefreshResponse>>,
        issue_responses: Mutex<HashMap<(String, String), IssueListResponse>>,
        timeline_responses: Mutex<HashMap<(String, i64), IssueTimelineResponse>>,
        pull_file_responses: Mutex<HashMap<(String, i64), PullFilesResponse>>,
    }

    impl FakeGitHubApi {
        fn insert_commit_response(
            &self,
            repo_key: &str,
            commit_sha: &str,
            response: CommitLookupResponse,
        ) {
            self.commit_pulls
                .lock()
                .unwrap()
                .insert((repo_key.to_string(), commit_sha.to_string()), response);
        }

        fn insert_issue_response(&self, repo_key: &str, since: &str, response: IssueListResponse) {
            self.issue_responses
                .lock()
                .unwrap()
                .insert((repo_key.to_string(), since.to_string()), response);
        }

        fn insert_timeline_response(
            &self,
            repo_key: &str,
            issue_number: i64,
            response: IssueTimelineResponse,
        ) {
            self.timeline_responses
                .lock()
                .unwrap()
                .insert((repo_key.to_string(), issue_number), response);
        }

        fn insert_pull_files_response(
            &self,
            repo_key: &str,
            pr_number: i64,
            response: PullFilesResponse,
        ) {
            self.pull_file_responses
                .lock()
                .unwrap()
                .insert((repo_key.to_string(), pr_number), response);
        }
    }

    impl GitHubApi for FakeGitHubApi {
        async fn fetch_commit_pulls(
            &self,
            repo: &GitHubRepo,
            commit_sha: &str,
        ) -> std::result::Result<Vec<PullRequestRecord>, crate::github::client::GitHubApiError>
        {
            self.commit_calls
                .lock()
                .unwrap()
                .push((repo.repo_key.clone(), commit_sha.to_string()));
            self.commit_pulls
                .lock()
                .unwrap()
                .remove(&(repo.repo_key.clone(), commit_sha.to_string()))
                .unwrap_or_else(|| Ok(Vec::new()))
        }

        async fn fetch_pull_request(
            &self,
            repo: &GitHubRepo,
            pr_number: i64,
        ) -> std::result::Result<PullRequestRecord, crate::github::client::GitHubApiError> {
            self.pull_calls
                .lock()
                .unwrap()
                .push((repo.repo_key.clone(), pr_number));
            self.pull_responses
                .lock()
                .unwrap()
                .remove(&(repo.repo_key.clone(), pr_number))
                .unwrap_or_else(|| {
                    Ok(PullRequestRecord {
                        number: pr_number,
                        state: "open".to_string(),
                        draft: false,
                        created_at: Some("2026-03-01T10:00:00Z".to_string()),
                        updated_at: Some("2026-03-01T10:00:00Z".to_string()),
                        closed_at: None,
                        merged_at: None,
                        base_ref: Some("main".to_string()),
                        head_ref: Some("feature".to_string()),
                        html_url: Some("https://github.com/PaceFlow/repo/pull/1".to_string()),
                    })
                })
        }

        async fn fetch_closed_issues(
            &self,
            repo: &GitHubRepo,
            since: Option<&str>,
        ) -> std::result::Result<Vec<IssueRecord>, crate::github::client::GitHubApiError> {
            let since = since.unwrap_or_default().to_string();
            self.issue_calls
                .lock()
                .unwrap()
                .push((repo.repo_key.clone(), since.clone()));
            self.issue_responses
                .lock()
                .unwrap()
                .remove(&(repo.repo_key.clone(), since))
                .unwrap_or_else(|| Ok(Vec::new()))
        }

        async fn fetch_issue_timeline(
            &self,
            repo: &GitHubRepo,
            issue_number: i64,
        ) -> std::result::Result<Vec<IssueTimelineEvent>, crate::github::client::GitHubApiError>
        {
            self.timeline_calls
                .lock()
                .unwrap()
                .push((repo.repo_key.clone(), issue_number));
            self.timeline_responses
                .lock()
                .unwrap()
                .remove(&(repo.repo_key.clone(), issue_number))
                .unwrap_or_else(|| Ok(Vec::new()))
        }

        async fn fetch_pull_request_files(
            &self,
            repo: &GitHubRepo,
            pr_number: i64,
        ) -> std::result::Result<Vec<PullRequestFileRecord>, crate::github::client::GitHubApiError>
        {
            self.pull_file_calls
                .lock()
                .unwrap()
                .push((repo.repo_key.clone(), pr_number));
            self.pull_file_responses
                .lock()
                .unwrap()
                .remove(&(repo.repo_key.clone(), pr_number))
                .unwrap_or_else(|| Ok(Vec::new()))
        }
    }

    fn open_test_db() -> Result<Connection> {
        let conn = Connection::open_in_memory()?;
        init_app_schema(&conn)?;
        Ok(conn)
    }

    fn insert_open_pull_request(conn: &Connection, repo_key: &str, pr_number: i64) -> Result<()> {
        conn.execute(
            "INSERT INTO fact_github_pull_request (
                repo_key, pr_number, state, draft_flag, created_at, updated_at, closed_at,
                merged_at, base_ref, head_ref, html_url
             ) VALUES (?1, ?2, 'open', 0, '2026-03-10T10:00:00Z', '2026-03-12T10:00:00Z',
                NULL, NULL, 'main', 'feature-a', 'https://github.com/PaceFlow/repo/pull/12')",
            params![repo_key, pr_number],
        )?;
        Ok(())
    }

    fn upsert_repo_refresh_state(
        conn: &Connection,
        repo_key: &str,
        last_open_pr_refresh_at: &str,
    ) -> Result<()> {
        conn.execute(
            "INSERT INTO fact_github_sync_state (
                repo_key, last_commit_scan_at, last_open_pr_refresh_at, last_error, updated_at
             ) VALUES (?1, NULL, ?2, NULL, ?2)
             ON CONFLICT(repo_key) DO UPDATE SET
                last_open_pr_refresh_at = excluded.last_open_pr_refresh_at,
                updated_at = excluded.updated_at",
            params![repo_key, last_open_pr_refresh_at],
        )?;
        Ok(())
    }

    #[derive(Default)]
    struct RecordingProgress {
        labels: Vec<String>,
        replaced_units: Vec<(usize, usize)>,
    }

    impl IngestProgressObserver for RecordingProgress {
        fn advance(&mut self, item_label: &str) {
            self.labels.push(item_label.to_string());
        }

        fn replace_future_units(&mut self, old_units: usize, new_units: usize) {
            self.replaced_units.push((old_units, new_units));
        }
    }

    #[test]
    fn choose_owning_pr_prefers_earliest_created_at_then_lower_number() {
        let earliest = PullRequestRecord {
            number: 4,
            state: "open".to_string(),
            draft: false,
            created_at: Some("2026-03-01T09:00:00Z".to_string()),
            updated_at: None,
            closed_at: None,
            merged_at: None,
            base_ref: Some("main".to_string()),
            head_ref: Some("feature-a".to_string()),
            html_url: None,
        };
        let later = PullRequestRecord {
            number: 2,
            state: "open".to_string(),
            draft: false,
            created_at: Some("2026-03-01T10:00:00Z".to_string()),
            updated_at: None,
            closed_at: None,
            merged_at: None,
            base_ref: Some("main".to_string()),
            head_ref: Some("feature-b".to_string()),
            html_url: None,
        };

        assert_eq!(
            choose_owning_pr(&[later.clone(), earliest.clone()]).map(|record| record.number),
            Some(4)
        );
    }

    #[test]
    fn sync_deduplicates_commit_lookup_requests_and_persists_pr_outcomes() -> Result<()> {
        let mut conn = open_test_db()?;
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, repo_key, commit_sha, commit_time, heavy_ai_flag,
                merged_to_mainline_flag, reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
             ) VALUES
                ('/tmp/repo', 'git:github.com/PaceFlow/repo', 'abc', '2026-03-18T10:00:00Z', 1, 0, 0, 40, 50)",
            [],
        )?;

        let api = Arc::new(FakeGitHubApi::default());
        api.insert_commit_response(
            "git:github.com/PaceFlow/repo",
            "abc",
            Ok(vec![
                PullRequestRecord {
                    number: 12,
                    state: "closed".to_string(),
                    draft: false,
                    created_at: Some("2026-03-10T10:00:00Z".to_string()),
                    updated_at: Some("2026-03-12T10:00:00Z".to_string()),
                    closed_at: Some("2026-03-12T10:00:00Z".to_string()),
                    merged_at: Some("2026-03-12T10:00:00Z".to_string()),
                    base_ref: Some("main".to_string()),
                    head_ref: Some("feature-a".to_string()),
                    html_url: None,
                },
                PullRequestRecord {
                    number: 18,
                    state: "open".to_string(),
                    draft: false,
                    created_at: Some("2026-03-11T10:00:00Z".to_string()),
                    updated_at: Some("2026-03-11T10:00:00Z".to_string()),
                    closed_at: None,
                    merged_at: None,
                    base_ref: Some("main".to_string()),
                    head_ref: Some("feature-b".to_string()),
                    html_url: None,
                },
            ]),
        );

        let summary = sync_github_pull_requests_with_api(&mut conn, api.clone(), None)?;
        assert_eq!(summary.commit_lookups_enqueued, 1);
        assert_eq!(summary.resolved_commits, 1);

        let call_count = api.commit_calls.lock().unwrap().len();
        assert_eq!(call_count, 1);

        let outcome: (String, i64, i64) = conn.query_row(
            "SELECT lookup_status, pr_opened_flag, pr_merged_flag
             FROM event_commit_pr_outcome
             WHERE repo_root = '/tmp/repo' AND commit_sha = 'abc'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        assert_eq!(outcome, ("resolved".to_string(), 1, 1));

        let owning_pr: i64 = conn.query_row(
            "SELECT owning_pr_number
             FROM fact_github_commit_pr_lookup
             WHERE repo_key = 'git:github.com/PaceFlow/repo' AND commit_sha = 'abc'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(owning_pr, 12);
        Ok(())
    }

    #[test]
    fn plan_counts_pending_commit_lookups_and_open_pr_refreshes() -> Result<()> {
        let conn = open_test_db()?;
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, repo_key, commit_sha, commit_time, heavy_ai_flag,
                merged_to_mainline_flag, reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
             ) VALUES
                ('/tmp/repo', 'git:github.com/PaceFlow/repo', 'abc', '2026-03-18T10:00:00Z', 1, 0, 0, 40, 50)",
            [],
        )?;
        insert_open_pull_request(&conn, "git:github.com/PaceFlow/repo", 12)?;
        upsert_repo_refresh_state(
            &conn,
            "git:github.com/PaceFlow/repo",
            "9999-03-18T10:05:00.000Z",
        )?;

        let plan = plan_github_pull_requests(&conn)?;
        assert_eq!(plan.commit_lookup_units, 1);
        assert_eq!(plan.pull_request_refresh_units, 0);
        assert_eq!(plan.total_units(), 1);
        Ok(())
    }

    #[test]
    fn plan_counts_stale_open_pr_refresh_work() -> Result<()> {
        let conn = open_test_db()?;
        insert_open_pull_request(&conn, "git:github.com/PaceFlow/repo", 12)?;
        upsert_repo_refresh_state(
            &conn,
            "git:github.com/PaceFlow/repo",
            "2000-03-18T10:05:00.000Z",
        )?;

        let plan = plan_github_pull_requests(&conn)?;
        assert_eq!(plan.commit_lookup_units, 0);
        assert_eq!(plan.pull_request_refresh_units, 1);
        assert_eq!(plan.total_units(), 1);
        Ok(())
    }

    #[test]
    fn sync_skips_open_pr_refresh_for_fresh_repo_without_new_lookups() -> Result<()> {
        let mut conn = open_test_db()?;
        insert_open_pull_request(&conn, "git:github.com/PaceFlow/repo", 12)?;
        upsert_repo_refresh_state(
            &conn,
            "git:github.com/PaceFlow/repo",
            "9999-03-18T10:05:00.000Z",
        )?;

        let api = Arc::new(FakeGitHubApi::default());
        let summary = sync_github_pull_requests_with_api(&mut conn, api.clone(), None)?;

        assert_eq!(summary.commit_lookups_completed, 0);
        assert_eq!(summary.open_pull_requests_refreshed, 0);
        assert!(api.pull_calls.lock().unwrap().is_empty());
        Ok(())
    }

    #[test]
    fn sync_refreshes_open_prs_for_stale_repo() -> Result<()> {
        let mut conn = open_test_db()?;
        insert_open_pull_request(&conn, "git:github.com/PaceFlow/repo", 12)?;
        upsert_repo_refresh_state(
            &conn,
            "git:github.com/PaceFlow/repo",
            "2000-03-18T10:05:00.000Z",
        )?;

        let api = Arc::new(FakeGitHubApi::default());
        let summary = sync_github_pull_requests_with_api(&mut conn, api.clone(), None)?;

        assert_eq!(summary.open_pull_requests_refreshed, 1);
        assert_eq!(api.pull_calls.lock().unwrap().len(), 1);
        Ok(())
    }

    #[test]
    fn sync_advances_progress_for_commit_lookup_and_open_pr_refresh() -> Result<()> {
        let mut conn = open_test_db()?;
        upsert_repo_refresh_state(
            &conn,
            "git:github.com/PaceFlow/repo",
            "9999-03-18T10:05:00.000Z",
        )?;
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, repo_key, commit_sha, commit_time, heavy_ai_flag,
                merged_to_mainline_flag, reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
             ) VALUES
                ('/tmp/repo', 'git:github.com/PaceFlow/repo', 'abc', '2026-03-18T10:00:00Z', 1, 0, 0, 40, 50)",
            [],
        )?;

        let api = Arc::new(FakeGitHubApi::default());
        api.insert_commit_response(
            "git:github.com/PaceFlow/repo",
            "abc",
            Ok(vec![PullRequestRecord {
                number: 12,
                state: "open".to_string(),
                draft: false,
                created_at: Some("2026-03-10T10:00:00Z".to_string()),
                updated_at: Some("2026-03-12T10:00:00Z".to_string()),
                closed_at: None,
                merged_at: None,
                base_ref: Some("main".to_string()),
                head_ref: Some("feature-a".to_string()),
                html_url: Some("https://github.com/PaceFlow/repo/pull/12".to_string()),
            }]),
        );

        let mut progress = RecordingProgress::default();
        let summary =
            sync_github_pull_requests_with_api(&mut conn, api.clone(), Some(&mut progress))?;

        assert_eq!(summary.commit_lookups_completed, 1);
        assert_eq!(summary.open_pull_requests_refreshed, 1);
        assert_eq!(api.pull_calls.lock().unwrap().len(), 1);
        assert_eq!(progress.labels.len(), 2);
        assert!(progress.replaced_units.contains(&(0, 1)));
        assert!(progress.labels.iter().any(|label| label.contains("abc")));
        assert!(progress.labels.iter().any(|label| label.contains("#12")));
        Ok(())
    }

    #[test]
    fn sync_advances_progress_for_issue_scan_refresh() -> Result<()> {
        let mut conn = open_test_db()?;
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, repo_key, commit_sha, commit_time, heavy_ai_flag,
                merged_to_mainline_flag, reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
             ) VALUES
                ('/tmp/repo', 'git:github.com/PaceFlow/repo', 'abc', '2026-03-01T10:00:00Z', 1, 1, 0, 40, 50)",
            [],
        )?;
        conn.execute(
            "INSERT INTO fact_github_commit_pr_lookup (
                repo_key, commit_sha, status, owning_pr_number, last_checked_at, last_error
             ) VALUES
                ('git:github.com/PaceFlow/repo', 'abc', 'resolved', 12, '2026-03-18T10:05:00Z', NULL)",
            [],
        )?;

        let api = Arc::new(FakeGitHubApi::default());
        api.insert_issue_response(
            "git:github.com/PaceFlow/repo",
            "2026-03-01T10:00:00Z",
            Ok(vec![IssueRecord {
                number: 7,
                state: "closed".to_string(),
                created_at: Some("2026-03-10T09:00:00Z".to_string()),
                updated_at: Some("2026-03-12T09:00:00Z".to_string()),
                closed_at: Some("2026-03-12T09:00:00Z".to_string()),
                is_pull_request: false,
                label_names: vec!["bug".to_string()],
            }]),
        );
        api.insert_timeline_response(
            "git:github.com/PaceFlow/repo",
            7,
            Ok(vec![IssueTimelineEvent {
                event: "cross-referenced".to_string(),
                created_at: Some("2026-03-11T08:00:00Z".to_string()),
                source_pr_number: Some(19),
            }]),
        );
        api.pull_responses.lock().unwrap().insert(
            ("git:github.com/PaceFlow/repo".to_string(), 19),
            Ok(PullRequestRecord {
                number: 19,
                state: "closed".to_string(),
                draft: false,
                created_at: Some("2026-03-10T08:00:00Z".to_string()),
                updated_at: Some("2026-03-11T09:00:00Z".to_string()),
                closed_at: Some("2026-03-11T09:00:00Z".to_string()),
                merged_at: Some("2026-03-11T09:00:00Z".to_string()),
                base_ref: Some("main".to_string()),
                head_ref: Some("fix-bug".to_string()),
                html_url: Some("https://github.com/PaceFlow/repo/pull/19".to_string()),
            }),
        );
        api.insert_pull_files_response(
            "git:github.com/PaceFlow/repo",
            19,
            Ok(vec![PullRequestFileRecord {
                filename: "src/lib.rs".to_string(),
                previous_filename: None,
                status: "modified".to_string(),
                patch: Some("@@ -1 +1 @@\n-buggy();\n+fixed();\n".to_string()),
            }]),
        );

        let mut progress = RecordingProgress::default();
        let summary =
            sync_github_pull_requests_with_api(&mut conn, api.clone(), Some(&mut progress))?;

        assert_eq!(summary.issue_scans_enqueued, 1);
        assert_eq!(summary.issue_scans_completed, 1);
        assert_eq!(summary.issue_scans_refreshed, 1);
        assert!(progress.replaced_units.contains(&(1, 1)));
        assert!(progress.labels.iter().any(|label| label.contains("issues")));
        Ok(())
    }

    #[test]
    fn plan_counts_pending_issue_scans_for_merged_heavy_ai_repos() -> Result<()> {
        let conn = open_test_db()?;
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, repo_key, commit_sha, commit_time, heavy_ai_flag,
                merged_to_mainline_flag, reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
             ) VALUES
                ('/tmp/repo', 'git:github.com/PaceFlow/repo', 'abc', '2026-03-18T10:00:00Z', 1, 1, 0, 40, 50)",
            [],
        )?;
        conn.execute(
            "INSERT INTO fact_github_commit_pr_lookup (
                repo_key, commit_sha, status, owning_pr_number, last_checked_at, last_error
             ) VALUES
                ('git:github.com/PaceFlow/repo', 'abc', 'resolved', 12, '2026-03-18T10:05:00Z', NULL)",
            [],
        )?;

        let plan = plan_github_pull_requests(&conn)?;
        assert_eq!(plan.commit_lookup_units, 0);
        assert_eq!(plan.pull_request_refresh_units, 0);
        assert_eq!(plan.issue_scan_units, 1);
        assert_eq!(plan.total_units(), 1);
        Ok(())
    }

    #[test]
    fn sync_persists_issue_linked_fix_pr_removed_hashes() -> Result<()> {
        let mut conn = open_test_db()?;
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, repo_key, commit_sha, commit_time, heavy_ai_flag,
                merged_to_mainline_flag, reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
             ) VALUES
                ('/tmp/repo', 'git:github.com/PaceFlow/repo', 'abc', '2026-03-01T10:00:00Z', 1, 1, 0, 40, 50)",
            [],
        )?;
        conn.execute(
            "INSERT INTO fact_github_commit_pr_lookup (
                repo_key, commit_sha, status, owning_pr_number, last_checked_at, last_error
             ) VALUES
                ('git:github.com/PaceFlow/repo', 'abc', 'resolved', 12, '2026-03-18T10:05:00Z', NULL)",
            [],
        )?;

        let api = Arc::new(FakeGitHubApi::default());
        api.insert_issue_response(
            "git:github.com/PaceFlow/repo",
            "2026-03-01T10:00:00Z",
            Ok(vec![IssueRecord {
                number: 7,
                state: "closed".to_string(),
                created_at: Some("2026-03-10T09:00:00Z".to_string()),
                updated_at: Some("2026-03-12T09:00:00Z".to_string()),
                closed_at: Some("2026-03-12T09:00:00Z".to_string()),
                is_pull_request: false,
                label_names: vec!["bug".to_string()],
            }]),
        );
        api.insert_timeline_response(
            "git:github.com/PaceFlow/repo",
            7,
            Ok(vec![IssueTimelineEvent {
                event: "cross-referenced".to_string(),
                created_at: Some("2026-03-11T08:00:00Z".to_string()),
                source_pr_number: Some(19),
            }]),
        );
        api.pull_responses.lock().unwrap().insert(
            ("git:github.com/PaceFlow/repo".to_string(), 19),
            Ok(PullRequestRecord {
                number: 19,
                state: "closed".to_string(),
                draft: false,
                created_at: Some("2026-03-10T08:00:00Z".to_string()),
                updated_at: Some("2026-03-11T09:00:00Z".to_string()),
                closed_at: Some("2026-03-11T09:00:00Z".to_string()),
                merged_at: Some("2026-03-11T09:00:00Z".to_string()),
                base_ref: Some("main".to_string()),
                head_ref: Some("fix-bug".to_string()),
                html_url: Some("https://github.com/PaceFlow/repo/pull/19".to_string()),
            }),
        );
        api.insert_pull_files_response(
            "git:github.com/PaceFlow/repo",
            19,
            Ok(vec![PullRequestFileRecord {
                filename: "src/lib.rs".to_string(),
                previous_filename: None,
                status: "modified".to_string(),
                patch: Some("@@ -1 +1 @@\n-buggy();\n+fixed();\n".to_string()),
            }]),
        );

        let summary = sync_github_pull_requests_with_api(&mut conn, api, None)?;
        assert_eq!(summary.issue_scans_enqueued, 1);
        assert_eq!(summary.issue_scans_completed, 1);
        assert_eq!(summary.issue_scans_refreshed, 1);
        assert_eq!(summary.issues_upserted, 1);
        assert_eq!(summary.issue_fix_pull_requests_upserted, 1);
        assert_eq!(summary.pull_request_removed_hashes_upserted, 1);

        let issue_flags: (i64, i64) = conn.query_row(
            "SELECT is_pull_request_flag, bug_candidate_flag
             FROM fact_github_issue
             WHERE repo_key = 'git:github.com/PaceFlow/repo' AND issue_number = 7",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(issue_flags, (0, 1));

        let linked_pr: i64 = conn.query_row(
            "SELECT pr_number
             FROM fact_github_issue_fix_pull_request
             WHERE repo_key = 'git:github.com/PaceFlow/repo' AND issue_number = 7",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(linked_pr, 19);

        let removed_hash: (String, i64) = conn.query_row(
            "SELECT line_hash, count
             FROM fact_github_pull_request_removed_line_hash
             WHERE repo_key = 'git:github.com/PaceFlow/repo' AND pr_number = 19 AND rel_path = 'src/lib.rs'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(removed_hash, (hash_line("buggy();"), 1));
        Ok(())
    }

    #[test]
    fn sync_marks_issue_fix_pr_with_missing_patch_as_incomplete_evidence() -> Result<()> {
        let mut conn = open_test_db()?;
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, repo_key, commit_sha, commit_time, heavy_ai_flag,
                merged_to_mainline_flag, reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
             ) VALUES
                ('/tmp/repo', 'git:github.com/PaceFlow/repo', 'abc', '2026-03-01T10:00:00Z', 1, 1, 0, 40, 50)",
            [],
        )?;
        conn.execute(
            "INSERT INTO fact_github_commit_pr_lookup (
                repo_key, commit_sha, status, owning_pr_number, last_checked_at, last_error
             ) VALUES
                ('git:github.com/PaceFlow/repo', 'abc', 'resolved', 12, '2026-03-18T10:05:00Z', NULL)",
            [],
        )?;

        let api = Arc::new(FakeGitHubApi::default());
        api.insert_issue_response(
            "git:github.com/PaceFlow/repo",
            "2026-03-01T10:00:00Z",
            Ok(vec![IssueRecord {
                number: 7,
                state: "closed".to_string(),
                created_at: Some("2026-03-10T09:00:00Z".to_string()),
                updated_at: Some("2026-03-12T09:00:00Z".to_string()),
                closed_at: Some("2026-03-12T09:00:00Z".to_string()),
                is_pull_request: false,
                label_names: vec!["bug".to_string()],
            }]),
        );
        api.insert_timeline_response(
            "git:github.com/PaceFlow/repo",
            7,
            Ok(vec![IssueTimelineEvent {
                event: "cross-referenced".to_string(),
                created_at: Some("2026-03-11T08:00:00Z".to_string()),
                source_pr_number: Some(19),
            }]),
        );
        api.pull_responses.lock().unwrap().insert(
            ("git:github.com/PaceFlow/repo".to_string(), 19),
            Ok(PullRequestRecord {
                number: 19,
                state: "closed".to_string(),
                draft: false,
                created_at: Some("2026-03-10T08:00:00Z".to_string()),
                updated_at: Some("2026-03-11T09:00:00Z".to_string()),
                closed_at: Some("2026-03-11T09:00:00Z".to_string()),
                merged_at: Some("2026-03-11T09:00:00Z".to_string()),
                base_ref: Some("main".to_string()),
                head_ref: Some("fix-bug".to_string()),
                html_url: Some("https://github.com/PaceFlow/repo/pull/19".to_string()),
            }),
        );
        api.insert_pull_files_response(
            "git:github.com/PaceFlow/repo",
            19,
            Ok(vec![PullRequestFileRecord {
                filename: "src/lib.rs".to_string(),
                previous_filename: None,
                status: "modified".to_string(),
                patch: None,
            }]),
        );

        let summary = sync_github_pull_requests_with_api(&mut conn, api, None)?;
        assert_eq!(summary.issue_scans_enqueued, 1);
        assert_eq!(summary.issue_scans_completed, 1);

        let removed_hash_count: i64 = conn.query_row(
            "SELECT COUNT(*)
             FROM fact_github_pull_request_removed_line_hash
             WHERE repo_key = 'git:github.com/PaceFlow/repo' AND pr_number = 19",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(removed_hash_count, 0);

        let complete_flag: i64 = conn.query_row(
            "SELECT removed_hashes_complete_flag
             FROM fact_github_pull_request
             WHERE repo_key = 'git:github.com/PaceFlow/repo' AND pr_number = 19",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(complete_flag, 0);
        Ok(())
    }

    #[test]
    fn rebuild_commit_pr_outcomes_tracks_no_pr_commits_without_marking_them_opened() -> Result<()> {
        let conn = open_test_db()?;
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, repo_key, commit_sha, commit_time, heavy_ai_flag,
                merged_to_mainline_flag, reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
             ) VALUES
                ('/tmp/repo', 'git:github.com/PaceFlow/repo', 'abc', '2026-03-18T10:00:00Z', 1, 0, 0, 40, 50)",
            [],
        )?;
        conn.execute(
            "INSERT INTO fact_github_commit_pr_lookup (
                repo_key, commit_sha, status, owning_pr_number, last_checked_at, last_error
             ) VALUES ('git:github.com/PaceFlow/repo', 'abc', 'no_pr', NULL, '2026-03-18T10:05:00Z', NULL)",
            [],
        )?;

        rebuild_commit_pr_outcomes(&conn)?;

        let flags: (i64, i64) = conn.query_row(
            "SELECT pr_opened_flag, pr_merged_flag
             FROM event_commit_pr_outcome
             WHERE repo_root = '/tmp/repo' AND commit_sha = 'abc'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(flags, (0, 0));
        Ok(())
    }
}
