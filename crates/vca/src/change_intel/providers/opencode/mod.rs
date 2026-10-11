use anyhow::Result;
use rusqlite::{Connection, TransactionBehavior};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use crate::change_intel::line_hash::hash_line;
use crate::change_intel::path_resolver::{detect_repo_root, path_to_string, to_rel_path};
use crate::change_intel::pipeline::ProviderCodeChangeSummary;
use crate::change_intel::storage;
use crate::change_intel::types::{
    ChangeOpCandidate, LineHashCount, LineSide, ParseError, SessionInfo, WriteMode,
};
use crate::ingest_progress::IngestProgressObserver;
use crate::providers::opencode::{ms_to_iso, open_readonly_db};

const PROVIDER: &str = "opencode";
const OPENCODE_CURSOR_NAMESPACE: &str = "opencode_core_v1";
const OPENCODE_PARSER_NAME: &str = "opencode_session_diff_v1";

#[derive(Debug, Clone)]
struct OpenCodeSessionRef {
    session_id: String,
    directory: Option<String>,
    source_file: String,
    last_seen_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct SessionDiffEntry {
    file: String,
    patch: String,
    #[serde(default)]
    additions: Option<i64>,
    #[serde(default)]
    deletions: Option<i64>,
}

#[derive(Debug, Clone)]
struct PatchLineSummary {
    added_lines: i64,
    removed_lines: i64,
    line_hashes: Vec<LineHashCount>,
}

pub fn plan_db_files() -> Result<Vec<PathBuf>> {
    crate::providers::opencode::plan_db_files()
}

pub fn ingest_opencode_code_changes_from_sources(
    conn: &mut Connection,
    sources: &[PathBuf],
    _verbose: bool,
    mut progress: Option<&mut dyn IngestProgressObserver>,
) -> Result<ProviderCodeChangeSummary> {
    let mut summary = ProviderCodeChangeSummary {
        provider: PROVIDER.to_string(),
        ..ProviderCodeChangeSummary::default()
    };

    for source in sources {
        let source_label = source.to_string_lossy().to_string();
        let result = ingest_source_db(conn, source, &mut summary);
        if let Some(observer) = progress.as_mut() {
            observer.advance(&source_label);
        }
        result?;
    }

    Ok(summary)
}

fn ingest_source_db(
    conn: &mut Connection,
    db_path: &Path,
    summary: &mut ProviderCodeChangeSummary,
) -> Result<()> {
    let sessions = load_sessions(db_path)?;
    let root = db_path.parent().ok_or_else(|| {
        anyhow::anyhow!("OpenCode database has no parent directory: {:?}", db_path)
    })?;
    let diff_root = root.join("storage").join("session_diff");

    for session in sessions {
        let diff_file = diff_root.join(format!("{}.json", session.session_id));
        if !diff_file.exists() {
            continue;
        }

        summary.sources_discovered += 1;
        let source_file = diff_file.to_string_lossy().to_string();
        let sig = file_signature(&diff_file)?;

        let result = (|| -> Result<()> {
            if let Some((mtime, size)) = sig {
                let cursor =
                    storage::get_ingest_cursor(conn, OPENCODE_CURSOR_NAMESPACE, &source_file)?;
                if let Some(cursor) = cursor
                    && cursor.file_mtime == mtime
                    && cursor.file_size == size
                {
                    summary.sources_skipped += 1;
                    return Ok(());
                }
            }

            let entries = parse_diff_file(&diff_file)?;
            summary.tool_calls_inspected += entries.len();

            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            storage::upsert_change_session(
                &tx,
                &SessionInfo {
                    provider: PROVIDER.to_string(),
                    session_id: session.session_id.clone(),
                    source_file: session.source_file.clone(),
                    session_cwd: session.directory.clone(),
                    last_seen_at: session.last_seen_at.clone(),
                },
            )?;

            let mut ops = Vec::new();
            for (index, entry) in entries.iter().enumerate() {
                match build_change_op(&session, &source_file, entry, index) {
                    Ok(Some(op)) => ops.push(op),
                    Ok(None) => {}
                    Err(error) => {
                        storage::insert_parse_error(
                            &tx,
                            &ParseError {
                                provider: PROVIDER.to_string(),
                                session_id: session.session_id.clone(),
                                source_file: source_file.clone(),
                                call_id: format!("session_diff:{}", session.session_id),
                                timestamp: session.last_seen_at.clone(),
                                parser_name: OPENCODE_PARSER_NAME.to_string(),
                                error,
                            },
                        )?;
                        summary.parse_errors += 1;
                    }
                }
            }

            let reconcile =
                storage::reconcile_source_tool_writes(&tx, PROVIDER, &source_file, &ops)?;
            summary.ops_upserted += reconcile.ops_upserted;

            if let Some((mtime, size)) = sig {
                storage::upsert_ingest_cursor(
                    &tx,
                    OPENCODE_CURSOR_NAMESPACE,
                    &source_file,
                    mtime,
                    size,
                )?;
            }

            tx.commit()?;
            Ok(())
        })();

        result?;
    }

    Ok(())
}

fn load_sessions(db_path: &Path) -> Result<Vec<OpenCodeSessionRef>> {
    let source = open_readonly_db(db_path)?;
    let db_source_file = db_path.to_string_lossy().to_string();
    let mut stmt = source.prepare(
        "SELECT s.id, s.directory, s.time_updated, p.worktree
         FROM session s
         LEFT JOIN project p ON p.id = s.project_id
         ORDER BY s.time_updated, s.id",
    )?;

    stmt.query_map([], |row| {
        let directory: Option<String> = row.get(1)?;
        let worktree: Option<String> = row.get(3)?;
        Ok(OpenCodeSessionRef {
            session_id: row.get(0)?,
            directory: directory.or(worktree),
            source_file: db_source_file.clone(),
            last_seen_at: ms_to_iso(row.get::<_, i64>(2)?),
        })
    })?
    .collect::<std::result::Result<Vec<_>, _>>()
    .map_err(Into::into)
}

fn parse_diff_file(path: &Path) -> Result<Vec<SessionDiffEntry>> {
    let raw = std::fs::read_to_string(path)?;
    let entries = serde_json::from_str(&raw)?;
    Ok(entries)
}

fn build_change_op(
    session: &OpenCodeSessionRef,
    source_file: &str,
    entry: &SessionDiffEntry,
    index: usize,
) -> std::result::Result<Option<ChangeOpCandidate>, String> {
    let patch = parse_unified_patch_lines(&entry.patch)?;
    if patch.added_lines == 0 && patch.removed_lines == 0 {
        return Ok(None);
    }

    let _metadata_counts_match =
        entry
            .additions
            .zip(entry.deletions)
            .map(|(additions, deletions)| {
                additions == patch.added_lines && deletions == patch.removed_lines
            });

    let abs_path = resolve_diff_path(session.directory.as_deref(), &entry.file);
    let repo_root = detect_repo_root(&abs_path).or_else(|| {
        session
            .directory
            .as_deref()
            .map(PathBuf::from)
            .and_then(|cwd| detect_repo_root(&cwd))
    });
    let rel_path = to_rel_path(repo_root.as_deref(), &abs_path);
    let abs_path_string = path_to_string(&abs_path);

    Ok(Some(ChangeOpCandidate {
        provider: PROVIDER.to_string(),
        session_id: session.session_id.clone(),
        source_file: source_file.to_string(),
        call_id: format!("session_diff:{}", session.session_id),
        op_index: index as i32,
        timestamp: session.last_seen_at.clone(),
        repo_root: repo_root.as_deref().map(path_to_string),
        abs_path: abs_path_string,
        rel_path,
        write_mode: WriteMode::Patch,
        before_known: true,
        added_lines: patch.added_lines,
        removed_lines: patch.removed_lines,
        parser_name: OPENCODE_PARSER_NAME.to_string(),
        line_hashes: patch.line_hashes,
    }))
}

fn resolve_diff_path(session_dir: Option<&str>, file: &str) -> PathBuf {
    let path = Path::new(file);
    if path.is_absolute() {
        path.to_path_buf()
    } else if let Some(session_dir) = session_dir {
        Path::new(session_dir).join(path)
    } else {
        path.to_path_buf()
    }
}

fn parse_unified_patch_lines(patch: &str) -> std::result::Result<PatchLineSummary, String> {
    let mut added_lines = 0i64;
    let mut removed_lines = 0i64;
    let mut line_hashes = Vec::new();
    let mut in_hunk = false;

    for line in patch.lines() {
        if line.starts_with("@@") {
            in_hunk = true;
            continue;
        }
        if !in_hunk {
            continue;
        }

        if let Some(content) = line.strip_prefix('+') {
            added_lines += 1;
            line_hashes.push(LineHashCount {
                side: LineSide::Added,
                line_hash: hash_line(content),
                count: 1,
            });
        } else if let Some(content) = line.strip_prefix('-') {
            removed_lines += 1;
            line_hashes.push(LineHashCount {
                side: LineSide::Removed,
                line_hash: hash_line(content),
                count: 1,
            });
        }
    }

    if !in_hunk && !patch.trim().is_empty() {
        return Err("patch has no unified diff hunks".to_string());
    }

    Ok(PatchLineSummary {
        added_lines,
        removed_lines,
        line_hashes: collapse_hash_counts(line_hashes),
    })
}

fn collapse_hash_counts(line_hashes: Vec<LineHashCount>) -> Vec<LineHashCount> {
    use std::collections::BTreeMap;

    let mut counts: BTreeMap<(LineSide, String), i64> = BTreeMap::new();
    for line_hash in line_hashes {
        *counts
            .entry((line_hash.side, line_hash.line_hash))
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

fn file_signature(path: &Path) -> Result<Option<(i64, i64)>> {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let modified = metadata.modified()?;
    let duration = modified.duration_since(UNIX_EPOCH).unwrap_or_default();
    Ok(Some((duration.as_secs() as i64, metadata.len() as i64)))
}

#[cfg(test)]
mod tests {
    use super::{ingest_opencode_code_changes_from_sources, parse_unified_patch_lines};
    use crate::db::init_app_schema;
    use anyhow::Result;
    use rusqlite::{Connection, params};
    use serde_json::json;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn parses_patch_counts_and_line_hashes() -> Result<()> {
        let summary = parse_unified_patch_lines(
            "Index: src/lib.rs\n\
             ===================================================================\n\
             --- src/lib.rs\n\
             +++ src/lib.rs\n\
             @@ -1,2 +1,2 @@\n\
             -old line\n\
             +new line\n\
              same\n",
        )
        .map_err(anyhow::Error::msg)?;

        assert_eq!(summary.added_lines, 1);
        assert_eq!(summary.removed_lines, 1);
        assert_eq!(summary.line_hashes.len(), 2);
        Ok(())
    }

    #[test]
    fn ingests_session_diff_as_tool_writes_and_skips_unchanged_sources() -> Result<()> {
        let tempdir = tempdir()?;
        let share = tempdir.path().join(".local/share/opencode");
        fs::create_dir_all(share.join("storage/session_diff"))?;
        let db_path = share.join("opencode.db");
        let source = Connection::open(&db_path)?;
        create_opencode_schema(&source)?;
        seed_project(&source, "p1", tempdir.path().to_str().unwrap())?;
        seed_session(
            &source,
            "s1",
            "p1",
            tempdir.path().to_str().unwrap(),
            1_777_365_898_000,
            1_777_365_904_000,
        )?;
        fs::write(
            share.join("storage/session_diff/s1.json"),
            json!([
                {
                    "file": "src/lib.rs",
                    "status": "modified",
                    "additions": 1,
                    "deletions": 1,
                    "patch": "Index: src/lib.rs\n===================================================================\n--- src/lib.rs\n+++ src/lib.rs\n@@ -1,1 +1,1 @@\n-old line\n+new line\n"
                }
            ])
            .to_string(),
        )?;

        let mut analytics = Connection::open_in_memory()?;
        init_app_schema(&analytics)?;
        let summary1 = ingest_opencode_code_changes_from_sources(
            &mut analytics,
            std::slice::from_ref(&db_path),
            false,
            None,
        )?;
        assert_eq!(summary1.sources_discovered, 1);
        assert_eq!(summary1.ops_upserted, 1);
        assert_eq!(summary1.parse_errors, 0);

        let row: (String, i64, i64, String) = analytics.query_row(
            "SELECT abs_path, lines_added, lines_removed, parser_name
             FROM fact_session_code_change
             WHERE provider='opencode' AND session_id='s1' AND source_kind='tool_write'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        assert_eq!(
            row,
            (
                tempdir
                    .path()
                    .join("src/lib.rs")
                    .to_string_lossy()
                    .to_string(),
                1,
                1,
                "opencode_session_diff_v1".to_string()
            )
        );
        let hash_count: i64 = analytics.query_row(
            "SELECT COUNT(*)
             FROM fact_session_code_change_line_hashes h
             JOIN fact_session_code_change c ON c.id = h.code_change_id
             WHERE c.provider='opencode' AND c.session_id='s1'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(hash_count, 2);

        let summary2 = ingest_opencode_code_changes_from_sources(
            &mut analytics,
            std::slice::from_ref(&db_path),
            false,
            None,
        )?;
        assert_eq!(summary2.sources_skipped, 1);
        Ok(())
    }

    fn create_opencode_schema(conn: &Connection) -> Result<()> {
        conn.execute_batch(
            "CREATE TABLE project (
                id TEXT PRIMARY KEY,
                worktree TEXT NOT NULL
            );
            CREATE TABLE session (
                id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                directory TEXT NOT NULL,
                time_created INTEGER NOT NULL,
                time_updated INTEGER NOT NULL
            );",
        )?;
        Ok(())
    }

    fn seed_project(conn: &Connection, id: &str, worktree: &str) -> Result<()> {
        conn.execute(
            "INSERT INTO project (id, worktree) VALUES (?1, ?2)",
            params![id, worktree],
        )?;
        Ok(())
    }

    fn seed_session(
        conn: &Connection,
        id: &str,
        project_id: &str,
        directory: &str,
        created: i64,
        updated: i64,
    ) -> Result<()> {
        conn.execute(
            "INSERT INTO session (id, project_id, directory, time_created, time_updated)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![id, project_id, directory, created, updated],
        )?;
        Ok(())
    }
}
