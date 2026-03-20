use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};

use crate::db;

use super::types::{CommitAttribution, CommitTaskAttribution, GitCommitDiff, SessionAttributionRow};

pub fn list_repo_roots_from_session_facts(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT DISTINCT repo_root
         FROM fact_session_code_change
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
        "SELECT MIN(change_ts)
         FROM fact_session_code_change
         WHERE repo_root = ?1 AND change_ts IS NOT NULL",
        params![repo_root],
        |r| r.get::<_, Option<String>>(0),
    )
    .optional()
    .map(|v| v.flatten())
    .map_err(Into::into)
}

pub fn upsert_git_commit_with_diffs(
    conn: &Connection,
    repo_root: &str,
    commit: &GitCommitDiff,
) -> Result<()> {
    db::upsert_metadata_repository(conn, repo_root)?;
    let total_added: i64 = commit.file_diffs.iter().map(|f| f.added_lines).sum();
    let total_removed: i64 = commit.file_diffs.iter().map(|f| f.removed_lines).sum();
    db::upsert_fact_commit(
        conn,
        repo_root,
        &commit.commit_sha,
        commit.parent_sha.as_deref(),
        &commit.commit_time,
        &commit.subject,
        total_added,
        total_removed,
    )?;

    let fact_files: Vec<(String, String, i64, i64)> = commit
        .file_diffs
        .iter()
        .map(|file| {
            (
                file.rel_path.clone(),
                file.change_type.clone(),
                file.added_lines,
                file.removed_lines,
            )
        })
        .collect();
    db::replace_fact_commit_file_changes(conn, repo_root, &commit.commit_sha, &fact_files)?;

    for file in &commit.file_diffs {
        db::upsert_metadata_file(conn, repo_root, &file.rel_path)?;
        db::replace_fact_commit_file_change_line_hashes(
            conn,
            repo_root,
            &commit.commit_sha,
            &file.rel_path,
            &file.line_hashes,
        )?;
    }

    Ok(())
}

pub fn upsert_commit_attribution(
    conn: &Connection,
    repo_root: &str,
    commit_sha: &str,
    attribution: &CommitAttribution,
    session_rows: &[SessionAttributionRow],
) -> Result<()> {
    db::update_fact_commit_attribution(
        conn,
        repo_root,
        commit_sha,
        attribution.matched_total_lines,
        attribution.matched_added_lines,
        attribution.matched_removed_lines,
        attribution.ai_share,
        attribution.heavy_ai,
    )?;

    let fact_rows: Vec<(String, String, f64, f64, f64)> = session_rows
        .iter()
        .map(|row| {
            (
                row.provider.clone(),
                row.session_id.clone(),
                row.matched_lines,
                row.share_of_commit,
                row.share_of_ai,
            )
        })
        .collect();
    db::replace_fact_commit_session_matches(conn, repo_root, commit_sha, &fact_rows)?;

    Ok(())
}

pub fn upsert_commit_task_attribution(
    conn: &Connection,
    repo_root: &str,
    commit_sha: &str,
    task: &CommitTaskAttribution,
) -> Result<()> {
    db::upsert_metadata_branch(conn, repo_root, &task.branch_name, Some(&task.task_key))?;
    db::upsert_fact_task_commit_assignment(
        conn,
        repo_root,
        commit_sha,
        &task.branch_name,
        &task.task_key,
        &task.source,
        task.is_fallback,
        task.candidate_count,
        task.distance_to_tip,
        task.confidence,
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
