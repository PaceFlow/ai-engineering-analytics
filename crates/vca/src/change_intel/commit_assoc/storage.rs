use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::BTreeMap;

use crate::change_intel::types::{LineHashCount, LineSide};
use crate::db;

use super::types::{
    CommitAttribution, CommitTaskAttribution, GitCommitDiff, SessionAttributionRow,
};

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
    assoc_session_facts_version: i64,
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
        assoc_session_facts_version,
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

pub fn list_cached_commit_shas(conn: &Connection, repo_root: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT commit_sha
         FROM fact_commit
         WHERE repo_root = ?1
         ORDER BY commit_time, commit_sha",
    )?;
    let rows = stmt.query_map(params![repo_root], |row| row.get(0))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

pub fn list_dirty_cached_commit_shas(
    conn: &Connection,
    repo_root: &str,
    session_facts_version: i64,
) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT DISTINCT fc.commit_sha
         FROM fact_commit fc
         JOIN fact_commit_file_change fcf
           ON fcf.repo_root = fc.repo_root
          AND fcf.commit_sha = fc.commit_sha
         JOIN fact_commit_file_change_line_hashes fclh
           ON fclh.file_change_id = fcf.id
         JOIN commit_assoc_dirty_hash dh
           ON dh.repo_root = fc.repo_root
          AND dh.side = fclh.side
          AND dh.line_hash = fclh.line_hash
         WHERE fc.repo_root = ?1
           AND fc.assoc_session_facts_version < ?2
         ORDER BY fc.commit_time, fc.commit_sha",
    )?;
    let rows = stmt.query_map(params![repo_root, session_facts_version], |row| row.get(0))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

pub fn load_cached_commit_diff(
    conn: &Connection,
    repo_root: &str,
    commit_sha: &str,
) -> Result<Option<GitCommitDiff>> {
    let commit_meta = conn
        .query_row(
            "SELECT parent_sha, commit_time, subject
             FROM fact_commit
             WHERE repo_root = ?1 AND commit_sha = ?2",
            params![repo_root, commit_sha],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?;

    let Some((parent_sha, commit_time, subject)) = commit_meta else {
        return Ok(None);
    };

    let mut stmt = conn.prepare(
        "SELECT id, rel_path, change_type, added_lines, removed_lines
         FROM fact_commit_file_change
         WHERE repo_root = ?1 AND commit_sha = ?2
         ORDER BY rel_path",
    )?;
    let rows = stmt.query_map(params![repo_root, commit_sha], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, i64>(4)?,
        ))
    })?;

    let mut file_diffs = Vec::new();
    for row in rows {
        let (file_change_id, rel_path, change_type, added_lines, removed_lines) = row?;
        file_diffs.push(super::types::GitFileDiff {
            rel_path,
            change_type,
            added_lines,
            removed_lines,
            line_hashes: load_commit_file_line_hashes(conn, file_change_id)?,
        });
    }

    Ok(Some(GitCommitDiff {
        commit_sha: commit_sha.to_string(),
        parent_sha,
        commit_time,
        subject,
        file_diffs,
    }))
}

fn load_commit_file_line_hashes(
    conn: &Connection,
    file_change_id: i64,
) -> Result<Vec<LineHashCount>> {
    let mut stmt = conn.prepare(
        "SELECT side, line_hash, count
         FROM fact_commit_file_change_line_hashes
         WHERE file_change_id = ?1
         ORDER BY side, line_hash",
    )?;
    let rows = stmt.query_map(params![file_change_id], |row| {
        let side_raw: String = row.get(0)?;
        let side = match side_raw.as_str() {
            "+" => LineSide::Added,
            "-" => LineSide::Removed,
            other => {
                return Err(rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("unsupported line side '{other}'"),
                    )),
                ));
            }
        };
        Ok(LineHashCount {
            side,
            line_hash: row.get(1)?,
            count: row.get(2)?,
        })
    })?;

    let mut merged: BTreeMap<(LineSide, String), i64> = BTreeMap::new();
    for row in rows {
        let row = row?;
        *merged.entry((row.side, row.line_hash)).or_insert(0) += row.count;
    }

    Ok(merged
        .into_iter()
        .map(|((side, line_hash), count)| LineHashCount {
            side,
            line_hash,
            count,
        })
        .collect())
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
