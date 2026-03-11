use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};

use super::matcher::ALGO_VERSION;
use super::types::{
    CommitAttribution, CommitCursor, CommitTaskAttribution, GitCommitDiff, ProviderAttributionRow,
    SessionAttributionRow,
};

const BRANCH_SCOPE_HEAD: &str = "head";
const WINDOW_DAYS_UNUSED: i64 = 0;
pub const TASK_ALGO_VERSION: &str = "task_assoc_v1";

pub fn branch_scope_head() -> &'static str {
    BRANCH_SCOPE_HEAD
}

pub fn list_repo_roots_from_change_ops(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT DISTINCT repo_root
         FROM change_ops
         WHERE repo_root IS NOT NULL AND TRIM(repo_root) != ''
         ORDER BY repo_root",
    )?;

    let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

pub fn min_ai_timestamp(conn: &Connection, repo_root: &str) -> Result<Option<String>> {
    conn.query_row(
        "SELECT MIN(timestamp)
         FROM change_ops
         WHERE repo_root = ?1 AND timestamp IS NOT NULL",
        params![repo_root],
        |r| r.get::<_, Option<String>>(0),
    )
    .optional()
    .map(|v| v.flatten())
    .map_err(Into::into)
}

pub fn get_commit_cursor(
    conn: &Connection,
    repo_root: &str,
    branch_scope: &str,
) -> Result<Option<CommitCursor>> {
    conn.query_row(
        "SELECT last_head_sha, last_commit_time, min_ai_ts_seen
         FROM commit_assoc_cursors
         WHERE repo_root = ?1 AND branch_scope = ?2",
        params![repo_root, branch_scope],
        |r| {
            Ok(CommitCursor {
                last_head_sha: r.get(0)?,
                last_commit_time: r.get(1)?,
                min_ai_ts_seen: r.get(2)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

pub fn upsert_commit_cursor(
    conn: &Connection,
    repo_root: &str,
    branch_scope: &str,
    last_head_sha: &str,
    last_commit_time: Option<&str>,
    min_ai_ts_seen: Option<&str>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO commit_assoc_cursors (
            repo_root, branch_scope, last_head_sha, last_commit_time, min_ai_ts_seen
         ) VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(repo_root, branch_scope) DO UPDATE SET
            last_head_sha = excluded.last_head_sha,
            last_commit_time = excluded.last_commit_time,
            min_ai_ts_seen = excluded.min_ai_ts_seen,
            updated_at = datetime('now')",
        params![
            repo_root,
            branch_scope,
            last_head_sha,
            last_commit_time,
            min_ai_ts_seen
        ],
    )?;
    Ok(())
}

pub fn upsert_git_commit_with_diffs(
    conn: &Connection,
    repo_root: &str,
    commit: &GitCommitDiff,
) -> Result<i64> {
    let total_added: i64 = commit.file_diffs.iter().map(|f| f.added_lines).sum();
    let total_removed: i64 = commit.file_diffs.iter().map(|f| f.removed_lines).sum();

    conn.execute(
        "INSERT INTO git_commits (
            repo_root, commit_sha, parent_sha, commit_time, subject, total_added, total_removed
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT(repo_root, commit_sha) DO UPDATE SET
            parent_sha = excluded.parent_sha,
            commit_time = excluded.commit_time,
            subject = excluded.subject,
            total_added = excluded.total_added,
            total_removed = excluded.total_removed",
        params![
            repo_root,
            commit.commit_sha,
            commit.parent_sha,
            commit.commit_time,
            commit.subject,
            total_added,
            total_removed
        ],
    )?;

    let commit_id: i64 = conn.query_row(
        "SELECT id
         FROM git_commits
         WHERE repo_root = ?1 AND commit_sha = ?2",
        params![repo_root, commit.commit_sha],
        |r| r.get(0),
    )?;

    conn.execute(
        "DELETE FROM git_commit_line_hashes
         WHERE file_diff_id IN (
            SELECT id FROM git_commit_file_diffs WHERE commit_id = ?1
         )",
        params![commit_id],
    )?;
    conn.execute(
        "DELETE FROM git_commit_file_diffs WHERE commit_id = ?1",
        params![commit_id],
    )?;

    for file in &commit.file_diffs {
        conn.execute(
            "INSERT INTO git_commit_file_diffs (
                commit_id, rel_path, change_type, added_lines, removed_lines
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                commit_id,
                file.rel_path,
                file.change_type,
                file.added_lines,
                file.removed_lines
            ],
        )?;
        let file_diff_id = conn.last_insert_rowid();

        for lh in &file.line_hashes {
            conn.execute(
                "INSERT INTO git_commit_line_hashes (
                    file_diff_id, side, line_hash, count
                 ) VALUES (?1, ?2, ?3, ?4)",
                params![file_diff_id, lh.side.as_str(), lh.line_hash, lh.count],
            )?;
        }
    }

    Ok(commit_id)
}

pub fn upsert_commit_attribution(
    conn: &Connection,
    commit_id: i64,
    attribution: &CommitAttribution,
    provider_rows: &[ProviderAttributionRow],
    session_rows: &[SessionAttributionRow],
) -> Result<()> {
    conn.execute(
        "INSERT INTO commit_ai_attributions (
            commit_id, algo_version, window_days, commit_total_lines, matched_total_lines,
            matched_added_lines, matched_removed_lines, ai_share, heavy_ai
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
         ON CONFLICT(commit_id, algo_version) DO UPDATE SET
            window_days = excluded.window_days,
            commit_total_lines = excluded.commit_total_lines,
            matched_total_lines = excluded.matched_total_lines,
            matched_added_lines = excluded.matched_added_lines,
            matched_removed_lines = excluded.matched_removed_lines,
            ai_share = excluded.ai_share,
            heavy_ai = excluded.heavy_ai,
            computed_at = datetime('now')",
        params![
            commit_id,
            ALGO_VERSION,
            WINDOW_DAYS_UNUSED,
            attribution.commit_total_lines,
            attribution.matched_total_lines,
            attribution.matched_added_lines,
            attribution.matched_removed_lines,
            attribution.ai_share,
            attribution.heavy_ai as i64
        ],
    )?;

    conn.execute(
        "DELETE FROM commit_ai_provider_attributions
         WHERE commit_id = ?1 AND algo_version = ?2",
        params![commit_id, ALGO_VERSION],
    )?;

    for row in provider_rows {
        conn.execute(
            "INSERT INTO commit_ai_provider_attributions (
                commit_id, algo_version, provider, matched_lines, share_of_commit, share_of_ai
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                commit_id,
                ALGO_VERSION,
                row.provider,
                row.matched_lines,
                row.share_of_commit,
                row.share_of_ai
            ],
        )?;
    }

    conn.execute(
        "DELETE FROM commit_ai_session_attributions
         WHERE commit_id = ?1 AND algo_version = ?2",
        params![commit_id, ALGO_VERSION],
    )?;

    for row in session_rows {
        conn.execute(
            "INSERT INTO commit_ai_session_attributions (
                commit_id, algo_version, provider, session_id, matched_lines, share_of_commit, share_of_ai
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                commit_id,
                ALGO_VERSION,
                row.provider,
                row.session_id,
                row.matched_lines,
                row.share_of_commit,
                row.share_of_ai
            ],
        )?;
    }

    Ok(())
}

pub fn upsert_commit_task_attribution(
    conn: &Connection,
    commit_id: i64,
    task: &CommitTaskAttribution,
) -> Result<()> {
    conn.execute(
        "INSERT INTO commit_task_attributions (
            commit_id, algo_version, branch_name, task_key, source, is_fallback,
            candidate_count, distance_to_tip, confidence
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
         ON CONFLICT(commit_id, algo_version) DO UPDATE SET
            branch_name = excluded.branch_name,
            task_key = excluded.task_key,
            source = excluded.source,
            is_fallback = excluded.is_fallback,
            candidate_count = excluded.candidate_count,
            distance_to_tip = excluded.distance_to_tip,
            confidence = excluded.confidence,
            computed_at = datetime('now')",
        params![
            commit_id,
            TASK_ALGO_VERSION,
            task.branch_name,
            task.task_key,
            task.source,
            task.is_fallback as i64,
            task.candidate_count,
            task.distance_to_tip,
            task.confidence
        ],
    )?;
    Ok(())
}

pub fn insert_commit_assoc_error(
    conn: &Connection,
    repo_root: &str,
    commit_sha: Option<&str>,
    stage: &str,
    error: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO commit_assoc_errors (
            repo_root, commit_sha, stage, error
         ) VALUES (?1, ?2, ?3, ?4)",
        params![repo_root, commit_sha, stage, error],
    )?;
    Ok(())
}
