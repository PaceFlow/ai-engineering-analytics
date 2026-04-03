#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GitHubRepo {
    pub repo_key: String,
    pub owner: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullRequestRecord {
    pub number: i64,
    pub state: String,
    pub draft: bool,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub closed_at: Option<String>,
    pub merged_at: Option<String>,
    pub base_ref: Option<String>,
    pub head_ref: Option<String>,
    pub html_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitLookupStatus {
    Resolved,
    NoPr,
    Failed,
    RateLimited,
}

impl CommitLookupStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            CommitLookupStatus::Resolved => "resolved",
            CommitLookupStatus::NoPr => "no_pr",
            CommitLookupStatus::Failed => "failed",
            CommitLookupStatus::RateLimited => "rate_limited",
        }
    }
}

#[derive(Debug, Clone)]
pub struct CommitLookupResult {
    pub repo: GitHubRepo,
    pub commit_sha: String,
    pub status: CommitLookupStatus,
    pub owning_pr_number: Option<i64>,
    pub pull_requests: Vec<PullRequestRecord>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PullRequestRefreshResult {
    pub repo: GitHubRepo,
    pub pull_request: PullRequestRecord,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct GitHubSyncWorkPlan {
    pub commit_lookup_units: usize,
    pub pull_request_refresh_units: usize,
}

impl GitHubSyncWorkPlan {
    pub fn total_units(&self) -> usize {
        self.commit_lookup_units + self.pull_request_refresh_units
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct GitHubSyncSummary {
    pub repos_considered: usize,
    pub commit_lookups_enqueued: usize,
    pub commit_lookups_completed: usize,
    pub pull_requests_upserted: usize,
    pub pull_request_commits_upserted: usize,
    pub resolved_commits: usize,
    pub no_pr_commits: usize,
    pub failed_commits: usize,
    pub rate_limited_commits: usize,
    pub open_pull_requests_refreshed: usize,
}
