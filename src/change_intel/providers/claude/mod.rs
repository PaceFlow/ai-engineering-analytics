use anyhow::Result;
use rusqlite::{Connection, TransactionBehavior};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use crate::change_intel::path_resolver::{detect_repo_root, path_to_string, to_rel_path};
use crate::change_intel::pipeline::ProviderCodeChangeSummary;
use crate::change_intel::storage;
use crate::change_intel::types::{ChangeOpCandidate, SessionInfo};
use crate::ingest_progress::IngestProgressObserver;
use crate::providers::claude::shared::{self, ClaudeStructuredWrite};

const CLAUDE_CURSOR_NAMESPACE: &str = "claude_core_v1";

pub fn ingest_claude_code_changes_from_sources(
    conn: &mut Connection,
    sources: &[PathBuf],
    _verbose: bool,
    mut progress: Option<&mut dyn IngestProgressObserver>,
) -> Result<ProviderCodeChangeSummary> {
    let mut summary = ProviderCodeChangeSummary {
        provider: "claude".to_string(),
        ..ProviderCodeChangeSummary::default()
    };

    for source in sources {
        summary.sources_discovered += 1;
        let source_file = source.to_string_lossy().to_string();

        let result = (|| -> Result<()> {
            let sig = file_signature(source)?;
            if let Some((mtime, size)) = sig {
                let cursor =
                    storage::get_ingest_cursor(conn, CLAUDE_CURSOR_NAMESPACE, &source_file)?;
                if let Some(cursor) = cursor
                    && cursor.file_mtime == mtime
                    && cursor.file_size == size
                {
                    summary.sources_skipped += 1;
                    return Ok(());
                }
            }

            let parsed = shared::parse_session_file(source)?;
            if parsed.session_id.trim().is_empty() {
                return Ok(());
            }

            summary.tool_calls_inspected += parsed.tool_call_count;

            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let session = SessionInfo {
                provider: "claude".to_string(),
                session_id: parsed.session_id.clone(),
                source_file: parsed.source_file.clone(),
                session_cwd: parsed.session_cwd.clone(),
                last_seen_at: parsed.ended_at.clone().or(parsed.started_at.clone()),
            };
            storage::upsert_change_session(&tx, &session)?;

            let ops = parsed
                .structured_writes
                .iter()
                .map(build_change_op)
                .collect::<Vec<_>>();
            let reconcile =
                storage::reconcile_source_tool_writes(&tx, "claude", &source_file, &ops)?;
            summary.ops_upserted += reconcile.ops_upserted;

            if let Some((mtime, size)) = sig {
                storage::upsert_ingest_cursor(
                    &tx,
                    CLAUDE_CURSOR_NAMESPACE,
                    &source_file,
                    mtime,
                    size,
                )?;
            }

            tx.commit()?;
            Ok(())
        })();

        if let Some(observer) = progress.as_mut() {
            observer.advance(&source_file);
        }
        result?;
    }

    Ok(summary)
}

fn build_change_op(write: &ClaudeStructuredWrite) -> ChangeOpCandidate {
    let abs_path = PathBuf::from(&write.abs_path);
    let repo_root = detect_repo_root(&abs_path).or_else(|| {
        write
            .session_cwd
            .as_deref()
            .map(PathBuf::from)
            .and_then(|cwd| detect_repo_root(&cwd))
    });
    let rel_path = to_rel_path(repo_root.as_deref(), &abs_path);

    ChangeOpCandidate {
        provider: "claude".to_string(),
        session_id: write.session_id.clone(),
        source_file: write.source_file.clone(),
        call_id: write.call_id.clone(),
        op_index: 0,
        timestamp: write.timestamp.clone(),
        repo_root: repo_root.as_deref().map(path_to_string),
        abs_path: write.abs_path.clone(),
        rel_path,
        write_mode: write.write_mode,
        before_known: write.before_known,
        added_lines: write.added_lines,
        removed_lines: write.removed_lines,
        parser_name: write.parser_name.to_string(),
        line_hashes: write.line_hashes.clone(),
    }
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
    use super::ingest_claude_code_changes_from_sources;
    use crate::db::init_app_schema;
    use anyhow::Result;
    use rusqlite::Connection;
    use serde_json::json;
    use std::fs;
    use tempfile::tempdir;

    fn write_session(
        lines: &[serde_json::Value],
        file_name: &str,
    ) -> Result<(tempfile::TempDir, std::path::PathBuf)> {
        let tempdir = tempdir()?;
        let path = tempdir.path().join(file_name);
        fs::write(
            &path,
            lines
                .iter()
                .map(serde_json::Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )?;
        Ok((tempdir, path))
    }

    fn load_tool_write_rows(
        conn: &Connection,
        session_id: &str,
    ) -> Result<Vec<(String, i64, i64, String)>> {
        let mut stmt = conn.prepare(
            "SELECT abs_path, lines_added, lines_removed, parser_name
             FROM fact_session_code_change
             WHERE provider='claude' AND session_id=?1 AND source_kind='tool_write'
             ORDER BY abs_path",
        )?;
        stmt.query_map([session_id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
    }

    #[test]
    fn edit_tool_result_creates_tool_write_hashes() -> Result<()> {
        let (_tempdir, path) = write_session(
            &[
                json!({
                    "type": "user",
                    "timestamp": "2026-04-22T09:00:00Z",
                    "sessionId": "claude-edit",
                    "cwd": "/tmp/repo",
                    "message": {"role": "user", "content": "update matcher"}
                }),
                json!({
                    "type": "assistant",
                    "timestamp": "2026-04-22T09:00:01Z",
                    "sessionId": "claude-edit",
                    "cwd": "/tmp/repo",
                    "message": {"model": "claude-opus-4-6", "role": "assistant", "content": [
                        {"type": "tool_use", "id": "toolu_edit", "name": "Edit", "input": {
                            "file_path": "/tmp/repo/src/lib.rs",
                            "old_string": "old line",
                            "new_string": "new line"
                        }}
                    ]}
                }),
                json!({
                    "type": "user",
                    "timestamp": "2026-04-22T09:00:02Z",
                    "sessionId": "claude-edit",
                    "cwd": "/tmp/repo",
                    "message": {"role": "user", "content": [
                        {"type": "tool_result", "tool_use_id": "toolu_edit", "content": "ok"}
                    ]},
                    "toolUseResult": {
                        "filePath": "/tmp/repo/src/lib.rs",
                        "oldString": "old line",
                        "newString": "new line",
                        "originalFile": "old line\n"
                    }
                }),
            ],
            "claude-edit.jsonl",
        )?;

        let mut conn = Connection::open_in_memory()?;
        init_app_schema(&conn)?;
        let summary = ingest_claude_code_changes_from_sources(&mut conn, &[path], false, None)?;

        assert_eq!(summary.tool_calls_inspected, 1);
        assert_eq!(summary.ops_upserted, 1);
        let rows = load_tool_write_rows(&conn, "claude-edit")?;
        assert_eq!(
            rows,
            vec![(
                "/tmp/repo/src/lib.rs".to_string(),
                1,
                1,
                "claude_edit_v1".to_string(),
            )]
        );
        Ok(())
    }

    #[test]
    fn write_create_and_update_emit_tool_writes() -> Result<()> {
        let (_tempdir, path) = write_session(
            &[
                json!({
                    "type": "assistant",
                    "timestamp": "2026-04-22T09:00:01Z",
                    "sessionId": "claude-write",
                    "cwd": "/tmp/repo",
                    "message": {"model": "claude-sonnet-4-5-20250929", "role": "assistant", "content": [
                        {"type": "tool_use", "id": "toolu_create", "name": "Write", "input": {
                            "file_path": "/tmp/repo/src/new.rs",
                            "content": "pub fn created() {}\n"
                        }}
                    ]}
                }),
                json!({
                    "type": "user",
                    "timestamp": "2026-04-22T09:00:02Z",
                    "sessionId": "claude-write",
                    "cwd": "/tmp/repo",
                    "message": {"role": "user", "content": [
                        {"type": "tool_result", "tool_use_id": "toolu_create", "content": "ok"}
                    ]},
                    "toolUseResult": {
                        "type": "create",
                        "filePath": "/tmp/repo/src/new.rs",
                        "content": "pub fn created() {}\n"
                    }
                }),
                json!({
                    "type": "assistant",
                    "timestamp": "2026-04-22T09:00:03Z",
                    "sessionId": "claude-write",
                    "cwd": "/tmp/repo",
                    "message": {"model": "claude-sonnet-4-5-20250929", "role": "assistant", "content": [
                        {"type": "tool_use", "id": "toolu_update", "name": "Write", "input": {
                            "file_path": "/tmp/repo/src/new.rs",
                            "content": "pub fn created() {\n    println!(\"hi\");\n}\n"
                        }}
                    ]}
                }),
                json!({
                    "type": "user",
                    "timestamp": "2026-04-22T09:00:04Z",
                    "sessionId": "claude-write",
                    "cwd": "/tmp/repo",
                    "message": {"role": "user", "content": [
                        {"type": "tool_result", "tool_use_id": "toolu_update", "content": "ok"}
                    ]},
                    "toolUseResult": {
                        "type": "update",
                        "filePath": "/tmp/repo/src/new.rs",
                        "content": "pub fn created() {\n    println!(\"hi\");\n}\n",
                        "originalFile": "pub fn created() {}\n",
                        "structuredPatch": [{
                            "oldStart": 1,
                            "oldLines": 1,
                            "newStart": 1,
                            "newLines": 3,
                            "lines": [
                                "-pub fn created() {}",
                                "+pub fn created() {",
                                "+    println!(\"hi\");",
                                "+}"
                            ]
                        }]
                    }
                }),
            ],
            "claude-write.jsonl",
        )?;

        let mut conn = Connection::open_in_memory()?;
        init_app_schema(&conn)?;
        let summary = ingest_claude_code_changes_from_sources(&mut conn, &[path], false, None)?;

        assert_eq!(summary.tool_calls_inspected, 2);
        assert_eq!(summary.ops_upserted, 2);
        let rows = load_tool_write_rows(&conn, "claude-write")?;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, "/tmp/repo/src/new.rs");
        assert_eq!(rows[0].1, 1);
        assert_eq!(rows[0].2, 0);
        assert_eq!(rows[1].0, "/tmp/repo/src/new.rs");
        assert_eq!(rows[1].1, 3);
        assert_eq!(rows[1].2, 1);
        Ok(())
    }

    #[test]
    fn failed_or_rejected_writes_are_ignored() -> Result<()> {
        let (_tempdir, path) = write_session(
            &[
                json!({
                    "type": "assistant",
                    "timestamp": "2026-04-22T09:00:01Z",
                    "sessionId": "claude-fail",
                    "cwd": "/tmp/repo",
                    "message": {"model": "claude-opus-4-6", "role": "assistant", "content": [
                        {"type": "tool_use", "id": "toolu_fail", "name": "Edit", "input": {
                            "file_path": "/tmp/repo/src/lib.rs",
                            "old_string": "old",
                            "new_string": "new"
                        }}
                    ]}
                }),
                json!({
                    "type": "user",
                    "timestamp": "2026-04-22T09:00:02Z",
                    "sessionId": "claude-fail",
                    "cwd": "/tmp/repo",
                    "message": {"role": "user", "content": [
                        {"type": "tool_result", "tool_use_id": "toolu_fail", "content": "rejected", "is_error": true}
                    ]},
                    "toolUseResult": "User rejected tool use"
                }),
            ],
            "claude-fail.jsonl",
        )?;

        let mut conn = Connection::open_in_memory()?;
        init_app_schema(&conn)?;
        let summary = ingest_claude_code_changes_from_sources(&mut conn, &[path], false, None)?;

        assert_eq!(summary.ops_upserted, 0);
        assert!(load_tool_write_rows(&conn, "claude-fail")?.is_empty());
        Ok(())
    }

    #[test]
    fn nested_agent_progress_writes_are_counted() -> Result<()> {
        let (_tempdir, path) = write_session(
            &[
                json!({
                    "type": "progress",
                    "timestamp": "2026-04-22T09:00:01Z",
                    "sessionId": "claude-agent",
                    "cwd": "/tmp/repo",
                    "data": {
                        "type": "agent_progress",
                        "message": {
                            "type": "assistant",
                            "timestamp": "2026-04-22T09:00:01Z",
                            "message": {
                                "model": "claude-haiku-4-5-20251001",
                                "role": "assistant",
                                "content": [{
                                    "type": "tool_use",
                                    "id": "toolu_nested",
                                    "name": "Edit",
                                    "input": {
                                        "file_path": "/tmp/repo/src/agent.rs",
                                        "old_string": "old",
                                        "new_string": "new"
                                    }
                                }]
                            }
                        }
                    }
                }),
                json!({
                    "type": "progress",
                    "timestamp": "2026-04-22T09:00:02Z",
                    "sessionId": "claude-agent",
                    "cwd": "/tmp/repo",
                    "data": {
                        "type": "agent_progress",
                        "message": {
                            "type": "user",
                            "timestamp": "2026-04-22T09:00:02Z",
                            "message": {
                                "role": "user",
                                "content": [{
                                    "type": "tool_result",
                                    "tool_use_id": "toolu_nested",
                                    "content": "ok"
                                }]
                            }
                        }
                    },
                    "toolUseResult": {
                        "filePath": "/tmp/repo/src/agent.rs",
                        "oldString": "old",
                        "newString": "new",
                        "originalFile": "old\n"
                    }
                }),
            ],
            "claude-agent.jsonl",
        )?;

        let mut conn = Connection::open_in_memory()?;
        init_app_schema(&conn)?;
        let summary = ingest_claude_code_changes_from_sources(&mut conn, &[path], false, None)?;

        assert_eq!(summary.tool_calls_inspected, 1);
        assert_eq!(summary.ops_upserted, 1);
        assert_eq!(load_tool_write_rows(&conn, "claude-agent")?.len(), 1);
        Ok(())
    }

    #[test]
    fn non_project_claude_plan_writes_are_ignored() -> Result<()> {
        let (_tempdir, path) = write_session(
            &[
                json!({
                    "type": "assistant",
                    "timestamp": "2026-04-22T09:00:01Z",
                    "sessionId": "claude-plan",
                    "cwd": "/tmp/repo",
                    "message": {"model": "claude-opus-4-6", "role": "assistant", "content": [
                        {"type": "tool_use", "id": "toolu_plan", "name": "Write", "input": {
                            "file_path": "/home/demo/.claude/plans/plan.md",
                            "content": "hello\n"
                        }}
                    ]}
                }),
                json!({
                    "type": "user",
                    "timestamp": "2026-04-22T09:00:02Z",
                    "sessionId": "claude-plan",
                    "cwd": "/tmp/repo",
                    "message": {"role": "user", "content": [
                        {"type": "tool_result", "tool_use_id": "toolu_plan", "content": "ok"}
                    ]},
                    "toolUseResult": {
                        "type": "create",
                        "filePath": "/home/demo/.claude/plans/plan.md",
                        "content": "hello\n"
                    }
                }),
            ],
            "claude-plan.jsonl",
        )?;

        let mut conn = Connection::open_in_memory()?;
        init_app_schema(&conn)?;
        let summary = ingest_claude_code_changes_from_sources(&mut conn, &[path], false, None)?;

        assert_eq!(summary.ops_upserted, 0);
        assert!(load_tool_write_rows(&conn, "claude-plan")?.is_empty());
        Ok(())
    }
}
