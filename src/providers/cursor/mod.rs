use anyhow::{Context, Result};
use chrono::TimeZone;
use rusqlite::{Connection, OpenFlags, params};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use super::utils::diff_line_counts;
use crate::cursor_paths::{cursor_history_path, cursor_state_path};
pub(crate) mod shared;

use shared::{
    CursorBubbleRole, CursorSessionGraph, aggregate_file_edits,
    load_cursor_session_graphs_from_rows, resolve_tool_call_edits,
};
use crate::db;
use crate::ingest_progress::IngestProgressObserver;
use crate::path_utils::{
    detect_repo_root, normalize_filesystem_path, path_to_string, strip_file_scheme,
};

pub fn plan_composer_rows() -> Result<Vec<(String, String)>> {
    let vscdb_path = match cursor_vscdb_path()? {
        Some(path) => path,
        None => return Ok(Vec::new()),
    };

    let vscdb = Connection::open_with_flags(
        &vscdb_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("Failed to open {:?} — is Cursor running?", vscdb_path))?;

    let mut stmt =
        vscdb.prepare("SELECT key, value FROM cursorDiskKV WHERE key LIKE 'composerData:%'")?;

    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })?
        .filter_map(|row| match row {
            Ok((key, Some(value))) => Some((key, value)),
            Ok((_key, None)) => None,
            Err(_) => None,
        })
        .collect();

    Ok(rows)
}

pub fn ingest_planned_sessions(
    db: &Connection,
    composer_rows: &[(String, String)],
    verbose: bool,
    mut progress: Option<&mut dyn IngestProgressObserver>,
) -> Result<usize> {
    if composer_rows.is_empty() {
        return Ok(0);
    }

    let vscdb_path = cursor_vscdb_path()?.with_context(|| {
        "Cursor state.vscdb disappeared between planning and ingest".to_string()
    })?;
    let history_root = match cursor_history_root()? {
        Some(path) => path,
        None => {
            if verbose {
                eprintln!("[cursor] History directory not found");
            }
            PathBuf::new()
        }
    };

    if verbose {
        eprintln!("[cursor] opening {:?}", vscdb_path);
    }

    let vscdb = Connection::open_with_flags(
        &vscdb_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("Failed to open {:?} — is Cursor running?", vscdb_path))?;

    let history_index = if history_root.as_os_str().is_empty() {
        HashMap::new()
    } else {
        build_history_index(&history_root)
    };

    if verbose {
        eprintln!("[cursor] history index: {} file(s)", history_index.len());
    }

    ingest_planned_sessions_from_source(
        db,
        &vscdb,
        &vscdb_path.to_string_lossy(),
        composer_rows,
        &history_index,
        verbose,
        progress,
    )
}

pub(crate) fn ingest_planned_sessions_from_source(
    db: &Connection,
    vscdb: &Connection,
    source_file: &str,
    composer_rows: &[(String, String)],
    history_index: &HistoryIndex,
    verbose: bool,
    mut progress: Option<&mut dyn IngestProgressObserver>,
) -> Result<usize> {
    let graphs = load_cursor_session_graphs_from_rows(vscdb, source_file, composer_rows)?;
    let by_session: HashMap<_, _> = graphs
        .into_iter()
        .map(|graph| (graph.composer_id.clone(), graph))
        .collect();

    let mut total_rows = 0usize;
    for (key, _value) in composer_rows {
        let session_id = key.strip_prefix("composerData:").unwrap_or(key);
        let Some(graph) = by_session.get(session_id) else {
            continue;
        };

        match ingest_session_graph(db, graph, history_index, verbose) {
            Ok(0) => {}
            Ok(n) => total_rows += n,
            Err(e) => {
                if verbose {
                    eprintln!("[cursor] skipping session: {}", e);
                }
            }
        }

        if let Some(observer) = progress.as_mut() {
            observer.advance(key);
        }
    }

    Ok(total_rows)
}

// ── Serde types ──────────────────────────────────────────────────────────────

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ComposerData {
    composer_id: String,
    #[serde(default)]
    conversation: Vec<ConversationBubble>,
    #[serde(default)]
    full_conversation_headers_only: Vec<ConversationHeader>,
    total_lines_added: Option<i64>,
    total_lines_removed: Option<i64>,
    context: Option<ComposerContext>,
    created_at: Option<i64>,
    last_updated_at: Option<i64>,
    #[serde(default)]
    code_block_data: HashMap<String, Vec<CodeBlock>>,
    #[serde(default)]
    all_attached_file_code_chunks_uris: Vec<String>,
    #[serde(default)]
    original_file_states: HashMap<String, JsonValue>,
}

#[derive(Deserialize)]
struct ConversationBubble {
    #[serde(rename = "type")]
    bubble_type: Option<i64>, // 1 = user, 2 = assistant
    text: Option<String>,
}

#[derive(Deserialize)]
struct ConversationHeader {
    #[serde(rename = "bubbleId")]
    bubble_id: String,
    #[serde(rename = "type")]
    bubble_type: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ComposerContext {
    #[serde(default)]
    file_selections: Vec<FileSelection>,
}

#[derive(Deserialize)]
struct FileSelection {
    uri: Option<FileUri>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FileUri {
    fs_path: Option<String>,
}

#[derive(Deserialize)]
struct CodeBlock {
    status: Option<String>, // "accepted" | "rejected" | "cancelled"
}

// ── History index ─────────────────────────────────────────────────────────────

pub(crate) struct HistoryEntry {
    id: String,
    source: String,
    timestamp: i64,
    dir_path: PathBuf,
}

pub(crate) type HistoryIndex = HashMap<String, Vec<HistoryEntry>>;

fn build_history_index(history_root: &Path) -> HistoryIndex {
    let mut index: HistoryIndex = HashMap::new();

    let dir_iter = match std::fs::read_dir(history_root) {
        Ok(it) => it,
        Err(_) => return index,
    };

    for entry in dir_iter.flatten() {
        let dir_path = entry.path();
        if !dir_path.is_dir() {
            continue;
        }

        let entries_json = dir_path.join("entries.json");
        let raw = match std::fs::read_to_string(&entries_json) {
            Ok(s) => s,
            Err(_) => continue,
        };

        #[derive(Deserialize)]
        struct EntriesFile {
            resource: Option<String>,
            #[serde(default)]
            entries: Vec<RawEntry>,
        }

        #[derive(Deserialize)]
        struct RawEntry {
            id: Option<String>,
            source: Option<String>,
            timestamp: Option<i64>,
        }

        let parsed: EntriesFile = match serde_json::from_str(&raw) {
            Ok(p) => p,
            Err(_) => continue,
        };

        let resource = match parsed.resource {
            Some(r) => r,
            None => continue,
        };

        // Strip "file://" prefix to get a plain absolute path
        let file_path = strip_file_scheme(&resource);

        let mut entries: Vec<HistoryEntry> = parsed
            .entries
            .into_iter()
            .filter_map(|e| {
                Some(HistoryEntry {
                    id: e.id?,
                    source: e.source.unwrap_or_default(),
                    timestamp: e.timestamp.unwrap_or(0),
                    dir_path: dir_path.clone(),
                })
            })
            .collect();

        // Sort ascending by timestamp so we can find the "next" entry easily
        entries.sort_by_key(|e| e.timestamp);

        index.entry(file_path).or_default().extend(entries);
    }

    // Sort each file's entries by timestamp after merging (shouldn't have multiple
    // dirs per file in practice, but be safe)
    for v in index.values_mut() {
        v.sort_by_key(|e| e.timestamp);
    }

    index
}

// ── Session ingestion ─────────────────────────────────────────────────────────

fn ingest_composer(
    _db: &Connection,
    _vscdb: &Connection,
    _json_text: &str,
    _history_index: &HistoryIndex,
    _verbose: bool,
) -> Result<usize> {
    Ok(0)
}

fn ingest_session_graph(
    db: &Connection,
    graph: &CursorSessionGraph,
    history_index: &HistoryIndex,
    verbose: bool,
) -> Result<usize> {
    let visible_message_count = graph
        .messages()
        .iter()
        .filter(|bubble| bubble.text.as_deref().is_some_and(|text| !text.is_empty()))
        .count();
    if visible_message_count == 0 {
        return Ok(0);
    }

    let session_id = &graph.composer_id;
    let timestamp = graph.started_at.clone();
    let last_updated = graph.ended_at.clone();
    let project_path = graph.project_path.clone();
    let model_name = graph.model_name.clone();

    if db::session_exists(db, session_id)? {
        db::upsert_metadata_session_with_model(
            db,
            "cursor",
            session_id,
            project_path.as_deref(),
            timestamp.as_deref(),
            last_updated.as_deref().or(timestamp.as_deref()),
            Some("cursor"),
            Some("cursor"),
            model_name.as_deref(),
        )?;
        return Ok(0);
    }

    let session_start_ms = graph.created_at_ms.unwrap_or(0);
    let session_end_ms = graph.last_updated_at_ms.unwrap_or(i64::MAX);

    if verbose {
        eprintln!(
            "[cursor] ingesting session {} ({})",
            &session_id[..session_id.len().min(8)],
            timestamp.as_deref().unwrap_or("?")
        );
    }

    db::begin_session_with_model(
        db,
        "cursor",
        session_id,
        project_path.as_deref(),
        timestamp.as_deref(),
        last_updated.as_deref().or(timestamp.as_deref()),
        Some("cursor"),
        model_name.as_deref(),
    )?;
    let mut written = 1usize;

    for bubble in graph.messages() {
        let role = match bubble.role {
            Some(CursorBubbleRole::User) => "user",
            Some(CursorBubbleRole::Assistant) => "assistant",
            None => continue,
        };
        let text = match &bubble.text {
            Some(text) if !text.is_empty() => text,
            _ => continue,
        };
        let words = text.split_whitespace().count();
        db::ingest_session_message(
            db,
            "cursor",
            session_id,
            role,
            text,
            words as i64,
            timestamp.as_deref(),
        )?;
        written += 1;
    }

    let mut loc_events_pushed = 0usize;
    let resolved_file_edits = aggregate_file_edits(&resolve_tool_call_edits(graph));
    for edit in &resolved_file_edits {
        db::ingest_accepted_code_change(
            db,
            "cursor",
            session_id,
            &edit.abs_path,
            edit.added_lines,
            edit.removed_lines,
            timestamp.as_deref(),
        )?;
        loc_events_pushed += 1;
        written += 1;
    }

    if loc_events_pushed == 0 {
        let mut fallback_paths: Vec<String> = graph
            .legacy_targets
            .iter()
            .map(|target| target.abs_path.clone())
            .chain(
                graph
                    .partial_targets
                    .iter()
                    .map(|target| target.abs_path.clone()),
            )
            .collect();
        let mut seen = HashSet::new();
        fallback_paths.retain(|path| seen.insert(path.clone()));
        fallback_paths.sort();

        for file_path in fallback_paths {
            let (lines_added, lines_removed) =
                loc_from_history(&file_path, session_start_ms, session_end_ms, history_index);
            db::ingest_accepted_code_change(
                db,
                "cursor",
                session_id,
                &file_path,
                lines_added,
                lines_removed,
                timestamp.as_deref(),
            )?;
            loc_events_pushed += 1;
            written += 1;
        }
    }

    if loc_events_pushed == 0
        && let (Some(added), Some(removed)) = (graph.total_lines_added, graph.total_lines_removed)
        && (added > 0 || removed > 0)
    {
        db::ingest_accepted_code_change(
            db,
            "cursor",
            session_id,
            "__total__",
            added,
            removed,
            timestamp.as_deref(),
        )?;
        written += 1;
    }

    Ok(written)
}

// ── LOC from File History ─────────────────────────────────────────────────────

fn loc_from_history(
    file_path: &str,
    session_start_ms: i64,
    session_end_ms: i64,
    index: &HistoryIndex,
) -> (i64, i64) {
    let entries = match index.get(file_path) {
        Some(e) => e,
        None => return (0, 0),
    };

    let mut total_added = 0i64;
    let mut total_removed = 0i64;

    for (i, entry) in entries.iter().enumerate() {
        // Only consider "Undo *" entries (before-states) within the session range
        if !entry.source.starts_with("Undo") {
            continue;
        }
        if entry.timestamp < session_start_ms || entry.timestamp > session_end_ms {
            continue;
        }

        // before-state
        let before_path = entry.dir_path.join(&entry.id);
        let before_content = std::fs::read_to_string(&before_path).unwrap_or_default();

        // after-state: next entry in the sorted list
        let after_content = if let Some(next) = entries.get(i + 1) {
            let after_path = next.dir_path.join(&next.id);
            std::fs::read_to_string(&after_path).unwrap_or_default()
        } else {
            String::new()
        };

        let (added, removed) = diff_line_counts(&before_content, &after_content);
        total_added += added;
        total_removed += removed;
    }

    (total_added, total_removed)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn cursor_vscdb_path() -> Result<Option<PathBuf>> {
    cursor_state_path()
}

fn cursor_history_root() -> Result<Option<PathBuf>> {
    cursor_history_path()
}

/// Convert millisecond epoch to ISO-8601 string.
fn ms_to_iso(ms: i64) -> String {
    let secs = ms / 1000;
    match chrono::Utc.timestamp_opt(secs, 0) {
        chrono::LocalResult::Single(dt) => dt.to_rfc3339(),
        _ => format!("{}", ms),
    }
}

/// Return the common ancestor directory of all file paths referenced in the
/// composer context. Falls back to the first `codeBlockData` key on failure.
fn extract_project_path(data: &ComposerData) -> Option<String> {
    let mut paths: Vec<String> = Vec::new();

    if let Some(ctx) = &data.context {
        for sel in &ctx.file_selections {
            if let Some(uri) = &sel.uri
                && let Some(p) = &uri.fs_path
            {
                paths.push(normalize_filesystem_path(p));
            }
        }
    }

    // Fallback: use allAttachedFileCodeChunksUris (new-schema sessions)
    if paths.is_empty() {
        for uri in &data.all_attached_file_code_chunks_uris {
            paths.push(strip_file_scheme(uri));
        }
    }

    // Fallback: use originalFileStates keys (file URIs of files edited in session)
    if paths.is_empty() {
        for uri in data.original_file_states.keys() {
            paths.push(strip_file_scheme(uri));
        }
    }

    // Fallback: use codeBlockData keys (file URIs)
    if paths.is_empty() {
        for key in data.code_block_data.keys() {
            paths.push(strip_file_scheme(key));
        }
    }

    if paths.is_empty() {
        return None;
    }

    let raw_path = common_ancestor_path(&paths).unwrap_or_else(|| PathBuf::from(&paths[0]));
    let path = raw_path.as_path();
    if let Some(root) = detect_repo_root(path) {
        return Some(path_to_string(&root));
    }

    if path.extension().is_some() {
        path.parent()
            .map(|parent| normalize_filesystem_path(parent.to_string_lossy().as_ref()))
    } else {
        Some(normalize_filesystem_path(
            raw_path.to_string_lossy().as_ref(),
        ))
    }
}

fn common_ancestor_path(paths: &[String]) -> Option<PathBuf> {
    let mut common: Vec<_> = Path::new(paths.first()?).components().collect();

    for raw_path in paths.iter().skip(1) {
        let components: Vec<_> = Path::new(raw_path).components().collect();
        let shared_len = common
            .iter()
            .zip(components.iter())
            .take_while(|(left, right)| left == right)
            .count();
        common.truncate(shared_len);
        if common.is_empty() {
            break;
        }
    }

    if common.is_empty() {
        return None;
    }

    let mut out = PathBuf::new();
    for component in common {
        out.push(component.as_os_str());
    }

    Some(out)
}

fn extract_model_name(raw: &JsonValue) -> Option<String> {
    const MODEL_KEYS: &[&str] = &[
        "model",
        "modelName",
        "currentModel",
        "selectedModel",
        "defaultModel",
        "defaultModelSlug",
        "chatModel",
    ];

    fn visit(value: &JsonValue, include_default: bool) -> Option<String> {
        match value {
            JsonValue::Object(map) => {
                if let Some(candidate) = usage_data_model_key(map, include_default) {
                    return Some(candidate);
                }
                for key in MODEL_KEYS {
                    if let Some(candidate) = map
                        .get(*key)
                        .and_then(|value| model_string_from_value(value, include_default))
                    {
                        return Some(candidate);
                    }
                }
                for value in map.values() {
                    if let Some(candidate) = visit(value, include_default) {
                        return Some(candidate);
                    }
                }
                None
            }
            JsonValue::Array(values) => values
                .iter()
                .find_map(|value| visit(value, include_default)),
            _ => None,
        }
    }

    visit(raw, false).or_else(|| visit(raw, true))
}

fn extract_model_name_from_bubbles(
    vscdb: &Connection,
    session_id: &str,
    headers: &[ConversationHeader],
) -> Option<String> {
    headers
        .iter()
        .filter(|header| header.bubble_type == 2)
        .find_map(|header| {
            let key = format!("bubbleId:{}:{}", session_id, header.bubble_id);
            let raw: String = vscdb
                .query_row(
                    "SELECT value FROM cursorDiskKV WHERE key = ?1",
                    params![key],
                    |row| row.get(0),
                )
                .ok()?;
            let bubble: JsonValue = serde_json::from_str(&raw).ok()?;
            extract_bubble_model_name(&bubble)
        })
}

fn extract_bubble_model_name(raw: &JsonValue) -> Option<String> {
    raw.get("modelInfo")
        .and_then(|value| value.get("modelName"))
        .and_then(|value| value.as_str())
        .and_then(|value| {
            let trimmed = value.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        })
}

fn usage_data_model_key(
    map: &serde_json::Map<String, JsonValue>,
    include_default: bool,
) -> Option<String> {
    map.get("usageData")
        .and_then(|value| value.as_object())
        .and_then(|usage_data| {
            usage_data
                .keys()
                .find_map(|key| model_string_from_candidate(key, include_default))
        })
}

fn model_string_from_value(value: &JsonValue, include_default: bool) -> Option<String> {
    match value {
        JsonValue::String(value) => model_string_from_candidate(value.trim(), include_default),
        JsonValue::Object(map) => {
            for key in ["name", "slug", "id", "model"] {
                if let Some(candidate) = map
                    .get(key)
                    .and_then(|value| model_string_from_value(value, include_default))
                {
                    return Some(candidate);
                }
            }
            None
        }
        _ => None,
    }
}

fn model_string_from_candidate(candidate: &str, include_default: bool) -> Option<String> {
    if candidate.is_empty() {
        return None;
    }

    let lowered = candidate.to_ascii_lowercase();
    if lowered == "default" {
        return include_default.then(|| "default".to_string());
    }

    if lowered.contains("gpt")
        || lowered.contains("claude")
        || lowered.contains("gemini")
        || lowered.contains("sonnet")
        || lowered.contains("haiku")
        || lowered.contains("opus")
        || lowered.contains("o1")
        || lowered.contains("o3")
        || lowered.contains("deepseek")
        || lowered.contains("llama")
        || lowered.contains("qwen")
    {
        return Some(candidate.to_string());
    }

    None
}

#[cfg(test)]
mod tests {
    use super::{
        ComposerData, extract_bubble_model_name, extract_model_name, extract_project_path,
        ingest_planned_sessions_from_source,
    };
    use crate::db::init_app_schema;
    use rusqlite::{Connection, params};
    use serde_json::json;
    use std::collections::HashMap;
    use tempfile::NamedTempFile;

    fn create_cursor_db(rows: &[(String, String)]) -> NamedTempFile {
        let file = NamedTempFile::new().expect("temp cursor db should be created");
        let conn = Connection::open(file.path()).expect("cursor temp db should open");
        conn.execute_batch(
            "CREATE TABLE cursorDiskKV (
                key   TEXT PRIMARY KEY,
                value TEXT
            );",
        )
        .expect("cursor temp db schema should be created");
        for (key, value) in rows {
            conn.execute(
                "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
                params![key, value],
            )
            .expect("cursor temp db row should insert");
        }
        file
    }

    #[test]
    fn extracts_default_when_it_is_the_only_model_hint() {
        let raw = json!({
            "modelConfig": {
                "modelName": "default"
            }
        });

        assert_eq!(extract_model_name(&raw).as_deref(), Some("default"));
    }

    #[test]
    fn prefers_usage_data_model_key_over_default_literal() {
        let raw = json!({
            "modelConfig": {
                "modelName": "default"
            },
            "usageData": {
                "claude-3.5-sonnet": {
                    "costInCents": 4,
                    "amount": 1
                }
            }
        });

        assert_eq!(
            extract_model_name(&raw).as_deref(),
            Some("claude-3.5-sonnet")
        );
    }

    #[test]
    fn extracts_model_from_nested_usage_data_keys() {
        let raw = json!({
            "nested": {
                "usageData": {
                    "claude-3.7-sonnet-thinking": {
                        "costInCents": 12
                    }
                }
            }
        });

        assert_eq!(
            extract_model_name(&raw).as_deref(),
            Some("claude-3.7-sonnet-thinking")
        );
    }

    #[test]
    fn extracts_model_from_bubble_model_info() {
        let raw = json!({
            "modelInfo": {
                "modelName": "gpt-5.2-codex-xhigh"
            }
        });

        assert_eq!(
            extract_bubble_model_name(&raw).as_deref(),
            Some("gpt-5.2-codex-xhigh")
        );
    }

    #[test]
    fn extract_project_path_decodes_percent_encoded_windows_file_uris() {
        let data: ComposerData = serde_json::from_value(json!({
            "composerId": "c1",
            "conversation": [{"type": 1, "text": "hi"}],
            "allAttachedFileCodeChunksUris": [
                "file:///c%3A/dev/paceflow/paceflow-backend/src/lib.rs",
                "file:///c%3A/dev/paceflow/paceflow-backend/tests/session.rs"
            ]
        }))
        .expect("cursor test fixture should deserialize");

        assert_eq!(
            extract_project_path(&data).as_deref(),
            Some("C:/dev/paceflow/paceflow-backend")
        );
    }

    #[test]
    fn session_ingest_uses_shared_tool_edits_for_latest_style_sessions() {
        let source = create_cursor_db(&[
            (
                "composerData:c_tool".to_string(),
                json!({
                    "composerId": "c_tool",
                    "conversation": [{"type": 1, "text": "refactor these files"}],
                    "filesChangedCount": 2,
                    "totalLinesAdded": 3,
                    "totalLinesRemoved": 2,
                    "originalFileStates": {
                        "file:///tmp/repo/src/plan.ts": {
                            "firstEditBubbleId": "bubble-plan",
                            "content": "old\n"
                        },
                        "file:///tmp/repo/src/assignment.ts": {
                            "firstEditBubbleId": "bubble-assign",
                            "content": "before\n"
                        }
                    }
                })
                .to_string(),
            ),
            (
                "bubbleId:c_tool:bubble-plan".to_string(),
                json!({
                    "type": 2,
                    "text": "",
                    "toolFormerData": {
                        "status": "completed",
                        "name": "edit_file_v2",
                        "toolCallId": "call-plan",
                        "params": "{\"relativeWorkspacePath\":\"/tmp/repo/src/plan.ts\",\"streamingContent\":\"@@\\n-old\\n+new\\n+another\\n\"}"
                    }
                })
                .to_string(),
            ),
            (
                "bubbleId:c_tool:bubble-assign".to_string(),
                json!({
                    "type": 2,
                    "text": "",
                    "toolFormerData": {
                        "status": "completed",
                        "name": "edit_file_v2",
                        "toolCallId": "call-assign",
                        "params": "{\"relativeWorkspacePath\":\"/tmp/repo/src/assignment.ts\",\"streamingContent\":\"@@\\n-before\\n+after\\n\"}"
                    }
                })
                .to_string(),
            ),
        ]);

        let analytics = Connection::open_in_memory().expect("analytics db should open");
        init_app_schema(&analytics).expect("analytics schema should initialize");

        let rows = vec![(
            "composerData:c_tool".to_string(),
            json!({
                "composerId": "c_tool",
                "conversation": [{"type": 1, "text": "refactor these files"}],
                "filesChangedCount": 2,
                "totalLinesAdded": 3,
                "totalLinesRemoved": 2,
                "originalFileStates": {
                    "file:///tmp/repo/src/plan.ts": {
                        "firstEditBubbleId": "bubble-plan",
                        "content": "old\n"
                    },
                    "file:///tmp/repo/src/assignment.ts": {
                        "firstEditBubbleId": "bubble-assign",
                        "content": "before\n"
                    }
                }
            })
            .to_string(),
        )];

        let source_conn = Connection::open(source.path()).expect("cursor temp db should reopen");
        let written = ingest_planned_sessions_from_source(
            &analytics,
            &source_conn,
            source.path().to_string_lossy().as_ref(),
            &rows,
            &HashMap::new(),
            false,
            None,
        )
        .expect("latest-style session should ingest");
        assert!(written >= 3);

        let mut stmt = analytics
            .prepare(
                "SELECT abs_path, lines_added, lines_removed
                 FROM fact_session_code_change
                 WHERE provider='cursor' AND session_id='c_tool' AND source_kind='accepted_change'
                 ORDER BY abs_path",
            )
            .expect("accepted change query should prepare");
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .expect("accepted change rows should map")
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("accepted change rows should collect");

        assert_eq!(
            rows,
            vec![
                ("/tmp/repo/src/assignment.ts".to_string(), 1, 1),
                ("/tmp/repo/src/plan.ts".to_string(), 2, 1),
            ]
        );
    }

    #[test]
    fn session_ingest_uses_shared_apply_patch_edits_for_older_sessions() {
        let source = create_cursor_db(&[
            (
                "composerData:c_patch".to_string(),
                json!({
                    "composerId": "c_patch",
                    "conversation": [{"type": 1, "text": "update the page object"}],
                    "filesChangedCount": 1,
                    "originalFileStates": {
                        "file:///tmp/repo/tests/pages/user-support-po.ts": {
                            "firstEditBubbleId": "bubble-patch",
                            "content": "old line\n"
                        }
                    }
                })
                .to_string(),
            ),
            (
                "bubbleId:c_patch:bubble-patch".to_string(),
                json!({
                    "type": 2,
                    "text": "",
                    "toolFormerData": {
                        "status": "completed",
                        "name": "apply_patch",
                        "toolCallId": "call-patch",
                        "params": "{\"relativeWorkspacePath\":\"tests/pages/user-support-po.ts\"}",
                        "rawArgs": "*** Begin Patch\n*** Update File: tests/pages/user-support-po.ts\n@@\n-old line\n+new line\n*** End Patch"
                    }
                })
                .to_string(),
            ),
        ]);

        let analytics = Connection::open_in_memory().expect("analytics db should open");
        init_app_schema(&analytics).expect("analytics schema should initialize");

        let rows = vec![(
            "composerData:c_patch".to_string(),
            json!({
                "composerId": "c_patch",
                "conversation": [{"type": 1, "text": "update the page object"}],
                "filesChangedCount": 1,
                "originalFileStates": {
                    "file:///tmp/repo/tests/pages/user-support-po.ts": {
                        "firstEditBubbleId": "bubble-patch",
                        "content": "old line\n"
                    }
                }
            })
            .to_string(),
        )];

        let source_conn = Connection::open(source.path()).expect("cursor temp db should reopen");
        ingest_planned_sessions_from_source(
            &analytics,
            &source_conn,
            source.path().to_string_lossy().as_ref(),
            &rows,
            &HashMap::new(),
            false,
            None,
        )
        .expect("older apply_patch session should ingest");

        let row: (String, i64, i64) = analytics
            .query_row(
                "SELECT abs_path, lines_added, lines_removed
                 FROM fact_session_code_change
                 WHERE provider='cursor' AND session_id='c_patch' AND source_kind='accepted_change'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("older accepted change should be present");

        assert_eq!(row.0, "/tmp/repo/tests/pages/user-support-po.ts");
        assert_eq!(row.1, 1);
        assert_eq!(row.2, 1);
    }
}
