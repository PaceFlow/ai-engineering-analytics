use crate::change_intel::types::LineHashCount;

#[derive(Debug, Clone)]
pub struct RunOptions {
    pub repos: Vec<String>,
    pub recompute: bool,
    pub max_commits: Option<usize>,
    pub include_merges: bool,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            repos: Vec::new(),
            recompute: false,
            max_commits: None,
            include_merges: false,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct AssociationSummary {
    pub repos_considered: usize,
    pub repos_selected: usize,
    pub repos_processed: usize,
    pub repos_skipped_non_git: usize,
    pub commits_scanned: usize,
    pub commits_attributed: usize,
    pub heavy_commits: usize,
    pub errors: usize,
    pub repo_summaries: Vec<RepoSummary>,
}

#[derive(Debug, Clone, Default)]
pub struct RepoSummary {
    pub repo_root: String,
    pub commits_scanned: usize,
    pub commits_attributed: usize,
    pub heavy_commits: usize,
    pub errors: usize,
    pub skipped_non_git: bool,
}

#[derive(Debug, Clone)]
pub struct GitCommitDiff {
    pub commit_sha: String,
    pub parent_sha: Option<String>,
    pub commit_time: String,
    pub subject: String,
    pub file_diffs: Vec<GitFileDiff>,
}

#[derive(Debug, Clone)]
pub struct GitFileDiff {
    pub rel_path: String,
    pub change_type: String,
    pub added_lines: i64,
    pub removed_lines: i64,
    pub line_hashes: Vec<LineHashCount>,
}

#[derive(Debug, Clone)]
pub struct CommitCursor {
    pub last_head_sha: String,
    pub last_commit_time: Option<String>,
    pub min_ai_ts_seen: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CommitHashRow {
    pub rel_path: String,
    pub side: String,
    pub line_hash: String,
    pub count: i64,
}

#[derive(Debug, Clone)]
pub struct CommitAttribution {
    pub commit_total_lines: i64,
    pub matched_total_lines: i64,
    pub matched_added_lines: i64,
    pub matched_removed_lines: i64,
    pub ai_share: f64,
    pub heavy_ai: bool,
}

#[derive(Debug, Clone)]
pub struct ProviderAttributionRow {
    pub provider: String,
    pub matched_lines: f64,
    pub share_of_commit: f64,
    pub share_of_ai: f64,
}
