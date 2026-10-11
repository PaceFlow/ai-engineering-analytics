use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::change_intel::types::{
    ChangeOpCandidate, LineHashCount, LineSide, ParseError, SessionInfo,
};
use crate::db;

#[derive(Debug, Clone)]
pub struct IngestCursor {
    pub file_mtime: i64,
    pub file_size: i64,
}

pub fn upsert_change_session(conn: &Connection, session: &SessionInfo) -> Result<()> {
    db::upsert_metadata_session(
        conn,
        &session.provider,
        &session.session_id,
        session.session_cwd.as_deref(),
        None,
        None,
        Some(&session.source_file),
    )?;
    Ok(())
}

pub fn upsert_change_op(conn: &Connection, op: &ChangeOpCandidate) -> Result<i64> {
    if let (Some(repo_root), Some(rel_path)) = (op.repo_root.as_deref(), op.rel_path.as_deref()) {
        db::upsert_metadata_file(conn, repo_root, rel_path)?;
    } else if let Some(repo_root) = op.repo_root.as_deref() {
        db::upsert_metadata_repository(conn, repo_root)?;
    }
    db::upsert_fact_tool_write(conn, op)
}

pub fn replace_line_hashes(
    conn: &Connection,
    op_id: i64,
    line_hashes: &[LineHashCount],
) -> Result<()> {
    db::replace_fact_session_code_change_line_hashes(conn, op_id, line_hashes)
}

#[derive(Debug, Clone)]
pub struct RepoAssocState {
    pub session_facts_version: i64,
    pub task_branch_fingerprint: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct SourceReconcileSummary {
    pub ops_upserted: usize,
    pub repos_marked_dirty: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct OpKey {
    session_id: String,
    call_id: String,
    op_index: i32,
}

#[derive(Debug, Clone)]
struct ToolWriteSnapshot {
    id: i64,
    repo_root: Option<String>,
    rel_path: Option<String>,
    line_hashes: Vec<LineHashCount>,
}

pub fn reconcile_source_tool_writes(
    conn: &Connection,
    provider: &str,
    source_file: &str,
    ops: &[ChangeOpCandidate],
) -> Result<SourceReconcileSummary> {
    let existing_rows = db::list_fact_tool_writes_by_source(conn, provider, source_file)?;
    let mut existing_by_key: BTreeMap<OpKey, ToolWriteSnapshot> = existing_rows
        .into_iter()
        .map(|row| {
            let key = OpKey {
                session_id: row.session_id,
                call_id: row.call_id,
                op_index: row.op_index,
            };
            (
                key.clone(),
                ToolWriteSnapshot {
                    id: row.id,
                    repo_root: row.repo_root,
                    rel_path: row.rel_path,
                    line_hashes: row.line_hashes,
                },
            )
        })
        .collect();

    let mut incoming_keys = BTreeSet::new();
    let mut dirty_by_repo: HashMap<String, BTreeSet<(String, String)>> = HashMap::new();
    let mut summary = SourceReconcileSummary::default();

    for op in ops {
        let key = OpKey {
            session_id: op.session_id.clone(),
            call_id: op.call_id.clone(),
            op_index: op.op_index,
        };
        if !incoming_keys.insert(key.clone()) {
            continue;
        }

        let previous = existing_by_key.remove(&key);
        let new_snapshot = ToolWriteSnapshot {
            id: -1,
            repo_root: op.repo_root.clone(),
            rel_path: op.rel_path.clone(),
            line_hashes: normalized_line_hashes(&op.line_hashes),
        };

        let op_id = upsert_change_op(conn, op)?;
        replace_line_hashes(conn, op_id, &op.line_hashes)?;
        summary.ops_upserted += 1;

        if tool_write_changed(previous.as_ref(), &new_snapshot) {
            record_dirty_snapshot(previous.as_ref(), &mut dirty_by_repo);
            record_dirty_snapshot(Some(&new_snapshot), &mut dirty_by_repo);
        }
    }

    for stale in existing_by_key.into_values() {
        record_dirty_snapshot(Some(&stale), &mut dirty_by_repo);
        db::delete_fact_session_code_change_by_id(conn, stale.id)?;
    }

    for (repo_root, hashes) in dirty_by_repo {
        if hashes.is_empty() {
            continue;
        }
        bump_repo_session_facts_version(conn, &repo_root)?;
        insert_dirty_hashes(conn, &repo_root, &hashes)?;
        summary.repos_marked_dirty += 1;
    }

    Ok(summary)
}

pub fn insert_parse_error(conn: &Connection, err: &ParseError) -> Result<()> {
    conn.execute(
        "INSERT INTO change_parse_errors (
            provider, session_id, source_file, call_id, timestamp, parser_name, error
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            err.provider,
            err.session_id,
            err.source_file,
            err.call_id,
            err.timestamp,
            err.parser_name,
            err.error,
        ],
    )?;
    Ok(())
}

pub fn get_ingest_cursor(
    conn: &Connection,
    provider: &str,
    source_file: &str,
) -> Result<Option<IngestCursor>> {
    let row = conn
        .query_row(
            "SELECT file_mtime, file_size FROM ingest_cursors
             WHERE provider = ?1 AND source_file = ?2",
            params![provider, source_file],
            |r| {
                Ok(IngestCursor {
                    file_mtime: r.get(0)?,
                    file_size: r.get(1)?,
                })
            },
        )
        .optional()?;

    Ok(row)
}

pub fn upsert_ingest_cursor(
    conn: &Connection,
    provider: &str,
    source_file: &str,
    file_mtime: i64,
    file_size: i64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO ingest_cursors (provider, source_file, file_mtime, file_size)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(provider, source_file) DO UPDATE SET
            file_mtime = excluded.file_mtime,
            file_size = excluded.file_size,
            last_ingested_at = datetime('now')",
        params![provider, source_file, file_mtime, file_size],
    )?;
    Ok(())
}

pub fn get_repo_assoc_state(conn: &Connection, repo_root: &str) -> Result<RepoAssocState> {
    let row = conn
        .query_row(
            "SELECT session_facts_version, task_branch_fingerprint
             FROM commit_assoc_repo_state
             WHERE repo_root = ?1",
            params![repo_root],
            |row| {
                Ok(RepoAssocState {
                    session_facts_version: row.get(0)?,
                    task_branch_fingerprint: row.get(1)?,
                })
            },
        )
        .optional()?;

    Ok(row.unwrap_or(RepoAssocState {
        session_facts_version: 0,
        task_branch_fingerprint: None,
    }))
}

pub fn bump_repo_session_facts_version(conn: &Connection, repo_root: &str) -> Result<i64> {
    conn.execute(
        "INSERT INTO commit_assoc_repo_state (repo_root, session_facts_version)
         VALUES (?1, 1)
         ON CONFLICT(repo_root) DO UPDATE SET
            session_facts_version = commit_assoc_repo_state.session_facts_version + 1,
            updated_at = datetime('now')",
        params![repo_root],
    )?;

    conn.query_row(
        "SELECT session_facts_version
         FROM commit_assoc_repo_state
         WHERE repo_root = ?1",
        params![repo_root],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

pub fn upsert_repo_branch_fingerprint(
    conn: &Connection,
    repo_root: &str,
    fingerprint: Option<&str>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO commit_assoc_repo_state (repo_root, session_facts_version, task_branch_fingerprint)
         VALUES (?1, 0, ?2)
         ON CONFLICT(repo_root) DO UPDATE SET
            task_branch_fingerprint = excluded.task_branch_fingerprint,
            updated_at = datetime('now')",
        params![repo_root, fingerprint],
    )?;
    Ok(())
}

pub fn insert_dirty_hashes(
    conn: &Connection,
    repo_root: &str,
    hashes: &BTreeSet<(String, String)>,
) -> Result<()> {
    for (side, line_hash) in hashes {
        conn.execute(
            "INSERT INTO commit_assoc_dirty_hash (repo_root, side, line_hash)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(repo_root, side, line_hash) DO NOTHING",
            params![repo_root, side, line_hash],
        )?;
    }
    Ok(())
}

pub fn list_dirty_hashes(conn: &Connection, repo_root: &str) -> Result<Vec<(String, String)>> {
    let mut stmt = conn.prepare(
        "SELECT side, line_hash
         FROM commit_assoc_dirty_hash
         WHERE repo_root = ?1
         ORDER BY side, line_hash",
    )?;
    let rows = stmt.query_map(params![repo_root], |row| Ok((row.get(0)?, row.get(1)?)))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

pub fn clear_dirty_hashes(conn: &Connection, repo_root: &str) -> Result<()> {
    conn.execute(
        "DELETE FROM commit_assoc_dirty_hash WHERE repo_root = ?1",
        params![repo_root],
    )?;
    Ok(())
}

fn tool_write_changed(previous: Option<&ToolWriteSnapshot>, next: &ToolWriteSnapshot) -> bool {
    let Some(previous) = previous else {
        return next.repo_root.is_some();
    };

    previous.repo_root != next.repo_root
        || previous.rel_path != next.rel_path
        || normalized_line_hashes(&previous.line_hashes)
            != normalized_line_hashes(&next.line_hashes)
}

fn record_dirty_snapshot(
    snapshot: Option<&ToolWriteSnapshot>,
    dirty_by_repo: &mut HashMap<String, BTreeSet<(String, String)>>,
) {
    let Some(snapshot) = snapshot else {
        return;
    };
    let Some(repo_root) = snapshot.repo_root.as_ref() else {
        return;
    };

    let entry = dirty_by_repo.entry(repo_root.clone()).or_default();
    for line_hash in normalized_line_hashes(&snapshot.line_hashes) {
        entry.insert((line_hash.side.as_str().to_string(), line_hash.line_hash));
    }
}

fn normalized_line_hashes(line_hashes: &[LineHashCount]) -> Vec<LineHashCount> {
    let mut counts: BTreeMap<(LineSide, String), i64> = BTreeMap::new();
    for line_hash in line_hashes {
        *counts
            .entry((line_hash.side, line_hash.line_hash.clone()))
            .or_insert(0) += line_hash.count;
    }

    counts
        .into_iter()
        .map(|((side, line_hash), count)| LineHashCount {
            side,
            line_hash,
            count,
        })
        .collect()
}
