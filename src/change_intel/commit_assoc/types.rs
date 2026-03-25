use crate::change_intel::types::LineHashCount;

#[derive(Debug, Clone, Default)]
pub struct AssociationWorkPlan {
    pub repo_plans: Vec<RepoAssociationPlan>,
}

impl AssociationWorkPlan {
    pub fn total_units(&self) -> usize {
        self.repo_plans.len()
            + self
                .repo_plans
                .iter()
                .map(|repo| repo.commits.len())
                .sum::<usize>()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CommitDiffSource {
    #[default]
    None,
    Git,
    Cache,
}

#[derive(Debug, Clone, Default)]
pub struct CommitAssociationWorkItem {
    pub commit_sha: String,
    pub needs_match_refresh: bool,
    pub needs_task_refresh: bool,
    pub diff_source: CommitDiffSource,
}

#[derive(Debug, Clone, Default)]
pub struct RepoAssociationPlan {
    pub repo_root: String,
    pub commits: Vec<CommitAssociationWorkItem>,
    pub session_facts_version: i64,
    pub branch_fingerprint: Option<String>,
    pub branch_fingerprint_changed: bool,
    pub has_dirty_hashes: bool,
    pub skipped_non_git: bool,
    pub planning_error_stage: Option<String>,
    pub planning_error_message: Option<String>,
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
    pub new_commits: usize,
    pub dirty_recomputed_commits: usize,
    pub task_only_commits: usize,
    pub cached_diff_reuses: usize,
    pub errors: usize,
    pub repo_summaries: Vec<RepoSummary>,
}

#[derive(Debug, Clone, Default)]
pub struct RepoSummary {
    pub repo_root: String,
    pub commits_scanned: usize,
    pub commits_attributed: usize,
    pub heavy_commits: usize,
    pub new_commits: usize,
    pub dirty_recomputed_commits: usize,
    pub task_only_commits: usize,
    pub cached_diff_reuses: usize,
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
pub struct CommitAttribution {
    pub matched_total_lines: i64,
    pub matched_added_lines: i64,
    pub matched_removed_lines: i64,
    pub ai_share: f64,
    pub heavy_ai: bool,
}

#[derive(Debug, Clone)]
pub struct SessionAttributionRow {
    pub provider: String,
    pub session_id: String,
    pub matched_lines: f64,
    pub share_of_commit: f64,
    pub share_of_ai: f64,
}

#[derive(Debug, Clone)]
pub struct CommitTaskAttribution {
    pub branch_name: String,
    pub task_key: String,
    pub source: String,
    pub is_fallback: bool,
    pub candidate_count: i64,
    pub distance_to_tip: Option<i64>,
    pub confidence: f64,
}
