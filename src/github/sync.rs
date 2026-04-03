use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{SecondsFormat, Utc};
use rusqlite::{Connection, TransactionBehavior, params};
use tokio::runtime::Builder;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::github::auth::github_token;
use crate::github::client::{GitHubApi, GitHubApiErrorKind, ReqwestGitHubApi};
use crate::github::types::{
    CommitLookupResult, CommitLookupStatus, GitHubRepo, GitHubSyncSummary, GitHubSyncWorkPlan,
    PullRequestRecord, PullRequestRefreshResult,
};
use crate::ingest_progress::IngestProgressObserver;
use crate::sync_identity;

const GLOBAL_CONCURRENCY: usize = 8;
const PER_REPO_CONCURRENCY: usize = 2;

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
    Ok(GitHubSyncWorkPlan {
        commit_lookup_units: dedupe_commit_lookup_work_items(load_commit_lookup_work_items(conn)?)
            .len(),
        pull_request_refresh_units: dedupe_pull_request_refresh_work_items(
            load_open_pull_request_refresh_items(conn)?,
        )
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
    summary.commit_lookups_enqueued = lookup_items.len();
    if !lookup_items.is_empty() {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .context("build GitHub sync runtime")?;
        let lookup_results = if let Some(observer) = progress.as_mut() {
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
    let refresh_items =
        dedupe_pull_request_refresh_work_items(load_open_pull_request_refresh_items(conn)?);
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

fn short_commit_sha(commit_sha: &str) -> &str {
    &commit_sha[..commit_sha.len().min(8)]
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
            merged_at, base_ref, head_ref, html_url
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
         ON CONFLICT(repo_key, pr_number) DO UPDATE SET
            state = excluded.state,
            draft_flag = excluded.draft_flag,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at,
            closed_at = excluded.closed_at,
            merged_at = excluded.merged_at,
            base_ref = excluded.base_ref,
            head_ref = excluded.head_ref,
            html_url = excluded.html_url",
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
            upsert_pull_request(&mut upsert_pr, &result.repo.repo_key, pull_request)?;
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
            merged_at, base_ref, head_ref, html_url
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
         ON CONFLICT(repo_key, pr_number) DO UPDATE SET
            state = excluded.state,
            draft_flag = excluded.draft_flag,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at,
            closed_at = excluded.closed_at,
            merged_at = excluded.merged_at,
            base_ref = excluded.base_ref,
            head_ref = excluded.head_ref,
            html_url = excluded.html_url",
    )?;
    for result in results {
        upsert_pull_request(&mut upsert_pr, &result.repo.repo_key, &result.pull_request)?;
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

fn upsert_pull_request(
    statement: &mut rusqlite::CachedStatement<'_>,
    repo_key: &str,
    pull_request: &PullRequestRecord,
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
        pull_request.html_url
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
) -> Result<Vec<PullRequestRefreshWorkItem>> {
    let mut statement = conn.prepare(
        "SELECT repo_key, pr_number
         FROM fact_github_pull_request
         WHERE repo_key LIKE 'git:github.com/%'
           AND state = 'open'
           AND merged_at IS NULL
         ORDER BY repo_key, pr_number",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;

    let mut items = Vec::new();
    for row in rows {
        let (repo_key, pr_number) = row?;
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::db::init_app_schema;
    use crate::ingest_progress::IngestProgressObserver;

    type CommitLookupResponse =
        std::result::Result<Vec<PullRequestRecord>, crate::github::client::GitHubApiError>;
    type PullRefreshResponse =
        std::result::Result<PullRequestRecord, crate::github::client::GitHubApiError>;

    #[derive(Default)]
    struct FakeGitHubApi {
        commit_calls: Mutex<Vec<(String, String)>>,
        pull_calls: Mutex<Vec<(String, i64)>>,
        commit_pulls: Mutex<HashMap<(String, String), CommitLookupResponse>>,
        pull_responses: Mutex<HashMap<(String, i64), PullRefreshResponse>>,
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
    }

    fn open_test_db() -> Result<Connection> {
        let conn = Connection::open_in_memory()?;
        init_app_schema(&conn)?;
        Ok(conn)
    }

    #[derive(Default)]
    struct RecordingProgress {
        labels: Vec<String>,
    }

    impl IngestProgressObserver for RecordingProgress {
        fn advance(&mut self, item_label: &str) {
            self.labels.push(item_label.to_string());
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
        conn.execute(
            "INSERT INTO fact_github_pull_request (
                repo_key, pr_number, state, draft_flag, created_at, updated_at, closed_at,
                merged_at, base_ref, head_ref, html_url
             ) VALUES
                ('git:github.com/PaceFlow/repo', 12, 'open', 0, '2026-03-10T10:00:00Z',
                 '2026-03-12T10:00:00Z', NULL, NULL, 'main', 'feature-a',
                 'https://github.com/PaceFlow/repo/pull/12')",
            [],
        )?;

        let plan = plan_github_pull_requests(&conn)?;
        assert_eq!(plan.commit_lookup_units, 1);
        assert_eq!(plan.pull_request_refresh_units, 1);
        assert_eq!(plan.total_units(), 2);
        Ok(())
    }

    #[test]
    fn sync_advances_progress_for_commit_lookup_and_open_pr_refresh() -> Result<()> {
        let mut conn = open_test_db()?;
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, repo_key, commit_sha, commit_time, heavy_ai_flag,
                merged_to_mainline_flag, reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
             ) VALUES
                ('/tmp/repo', 'git:github.com/PaceFlow/repo', 'abc', '2026-03-18T10:00:00Z', 1, 0, 0, 40, 50)",
            [],
        )?;
        conn.execute(
            "INSERT INTO fact_github_pull_request (
                repo_key, pr_number, state, draft_flag, created_at, updated_at, closed_at,
                merged_at, base_ref, head_ref, html_url
             ) VALUES
                ('git:github.com/PaceFlow/repo', 12, 'open', 0, '2026-03-10T10:00:00Z',
                 '2026-03-12T10:00:00Z', NULL, NULL, 'main', 'feature-a',
                 'https://github.com/PaceFlow/repo/pull/12')",
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
        let summary = sync_github_pull_requests_with_api(&mut conn, api, Some(&mut progress))?;

        assert_eq!(summary.commit_lookups_completed, 1);
        assert_eq!(summary.open_pull_requests_refreshed, 1);
        assert_eq!(progress.labels.len(), 2);
        assert!(progress.labels.iter().any(|label| label.contains("abc")));
        assert!(progress.labels.iter().any(|label| label.contains("#12")));
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
