use anyhow::Result;
use chrono::TimeZone;
use rusqlite::{Connection, OptionalExtension, params};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::change_intel::line_hash::hash_line;
use crate::change_intel::types::{LineHashCount, LineSide, WriteMode};
use crate::path_utils::{
    detect_repo_root, normalize_filesystem_path, path_to_string, strip_file_scheme,
};

#[derive(Debug, Clone)]
pub(crate) struct CursorSessionGraph {
    pub composer_id: String,
    pub source_file: String,
    pub created_at_ms: Option<i64>,
    pub last_updated_at_ms: Option<i64>,
    pub started_at: Option<String>,
    pub ended_at: Option<String>,
    pub project_path: Option<String>,
    pub model_name: Option<String>,
    pub subtitle: Option<String>,
    pub files_changed_count: Option<i64>,
    pub total_lines_added: Option<i64>,
    pub total_lines_removed: Option<i64>,
    pub conversation_messages: Vec<CursorBubbleEvent>,
    pub bubble_events: Vec<CursorBubbleEvent>,
    pub tool_calls: Vec<CursorToolCall>,
    pub checkpoint_paths: HashSet<String>,
    pub strong_path_hints: HashSet<String>,
    pub weak_path_hints: HashSet<String>,
    pub original_file_states: HashMap<String, CursorOriginalFileState>,
    pub partial_targets: Vec<CursorPartialTarget>,
    pub legacy_targets: Vec<CursorLegacyTarget>,
    pub inline_hints: Vec<CursorInlineHint>,
    pub inline_undo_rows: Vec<CursorInlineUndoRow>,
    pub partial_fates: HashMap<String, JsonValue>,
    pub legacy_diff_payloads: HashMap<String, JsonValue>,
}

impl CursorSessionGraph {
    pub fn last_seen_at(&self) -> Option<String> {
        self.ended_at.clone().or(self.started_at.clone())
    }

    pub fn messages(&self) -> &[CursorBubbleEvent] {
        if self.conversation_messages.is_empty() {
            &self.bubble_events
        } else {
            &self.conversation_messages
        }
    }

    pub fn first_edit_path_by_bubble(&self) -> HashMap<String, String> {
        let mut out = HashMap::new();
        for (path, state) in &self.original_file_states {
            if let Some(bubble_id) = &state.first_edit_bubble_id {
                out.entry(bubble_id.clone()).or_insert_with(|| path.clone());
            }
        }
        out
    }

    pub fn is_candidate_edit_session(&self) -> bool {
        let subtitle = self.subtitle.as_deref().unwrap_or("").to_ascii_lowercase();

        self.files_changed_count.unwrap_or(0) > 0
            || self.total_lines_added.unwrap_or(0) > 0
            || self.total_lines_removed.unwrap_or(0) > 0
            || subtitle.contains("edited ")
            || subtitle.contains("updated ")
            || !self.partial_targets.is_empty()
            || !self.legacy_targets.is_empty()
            || !self.inline_hints.is_empty()
            || !self.inline_undo_rows.is_empty()
            || self.tool_calls.iter().any(|tool| tool.looks_like_write())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CursorBubbleEvent {
    pub role: Option<CursorBubbleRole>,
    pub text: Option<String>,
    pub order_key: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CursorBubbleRole {
    User,
    Assistant,
}

#[derive(Debug, Clone)]
pub(crate) struct CursorToolCall {
    pub bubble_id: String,
    pub name: String,
    pub call_id: String,
    pub status: Option<String>,
    pub timestamp: Option<String>,
    pub path_hints: Vec<String>,
    pub patch_texts: Vec<String>,
}

impl CursorToolCall {
    pub fn looks_like_write(&self) -> bool {
        matches!(
            self.name.as_str(),
            "edit_file_v2" | "apply_patch" | "write_file" | "write_file_v2"
        )
    }

    pub fn can_emit_edit(&self) -> bool {
        if !self.looks_like_write() {
            return false;
        }

        if self.name == "apply_patch" {
            return self.status.as_deref() == Some("completed")
                || self
                    .patch_texts
                    .iter()
                    .any(|text| patch_has_real_changes(text));
        }

        self.status.as_deref() == Some("completed")
            && self
                .patch_texts
                .iter()
                .any(|text| patch_has_real_changes(text))
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct CursorOriginalFileState {
    pub content: Option<String>,
    pub first_edit_bubble_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CursorPartialTarget {
    pub partial_id: String,
    pub abs_path: String,
}

#[derive(Debug, Clone)]
pub(crate) struct CursorLegacyTarget {
    pub diff_id: Option<String>,
    pub abs_path: String,
    pub version: i32,
    pub code_block_idx: i32,
    pub timestamp: Option<String>,
    pub content: Option<String>,
    pub bubble_id: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct CursorInlineHint {
    pub call_id: String,
    pub abs_path: String,
    pub timestamp: Option<String>,
    pub original_text_lines: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct CursorInlineUndoRow {
    pub call_id: String,
    pub abs_path: String,
    pub timestamp: Option<String>,
    pub payload: JsonValue,
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedFileEdit {
    pub abs_path: String,
    pub call_id: String,
    pub op_index: i32,
    pub timestamp: Option<String>,
    pub write_mode: WriteMode,
    pub before_known: bool,
    pub added_lines: i64,
    pub removed_lines: i64,
    pub parser_name: String,
    pub line_hashes: Vec<LineHashCount>,
}

#[derive(Debug, Clone)]
pub(crate) struct AggregatedFileEdit {
    pub abs_path: String,
    pub added_lines: i64,
    pub removed_lines: i64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ComposerData {
    composer_id: String,
    #[serde(default)]
    conversation: Vec<ConversationBubble>,
    #[serde(default)]
    full_conversation_headers_only: Vec<ConversationHeader>,
    total_lines_added: Option<i64>,
    total_lines_removed: Option<i64>,
    files_changed_count: Option<i64>,
    subtitle: Option<String>,
    context: Option<ComposerContext>,
    created_at: Option<i64>,
    last_updated_at: Option<i64>,
    #[serde(default)]
    code_block_data: HashMap<String, JsonValue>,
    #[serde(default)]
    all_attached_file_code_chunks_uris: Vec<String>,
    #[serde(default)]
    original_file_states: HashMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
struct ConversationBubble {
    #[serde(rename = "type")]
    bubble_type: Option<i64>,
    text: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ConversationHeader {
    #[serde(rename = "bubbleId")]
    bubble_id: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ComposerContext {
    #[serde(default)]
    file_selections: Vec<FileSelection>,
}

#[derive(Debug, Clone, Deserialize)]
struct FileSelection {
    uri: Option<FileUri>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FileUri {
    fs_path: Option<String>,
}

pub(crate) fn load_cursor_session_graphs_from_rows(
    vscdb: &Connection,
    source_file: &str,
    composer_rows: &[(String, String)],
) -> Result<Vec<CursorSessionGraph>> {
    let mut graphs = Vec::new();
    let mut session_ids = HashSet::new();

    for (key, raw) in composer_rows {
        let Some(graph) = build_seed_graph(key, raw, source_file) else {
            continue;
        };
        session_ids.insert(graph.composer_id.clone());
        graphs.push(graph);
    }

    populate_graph_details(vscdb, &mut graphs, &session_ids)?;
    Ok(graphs)
}

pub(crate) fn load_cursor_session_graphs(
    vscdb: &Connection,
    source_file: &str,
) -> Result<Vec<CursorSessionGraph>> {
    let mut stmt =
        vscdb.prepare("SELECT key, value FROM cursorDiskKV WHERE key LIKE 'composerData:%'")?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })?
        .filter_map(|row| match row {
            Ok((key, Some(value))) => Some(Ok((key, value))),
            Ok((_key, None)) => None,
            Err(err) => Some(Err(err)),
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;

    load_cursor_session_graphs_from_rows(vscdb, source_file, &rows)
}

pub(crate) fn resolve_tool_call_edits(graph: &CursorSessionGraph) -> Vec<ResolvedFileEdit> {
    let first_edit_paths = graph.first_edit_path_by_bubble();
    let mut edits = Vec::new();

    for tool in &graph.tool_calls {
        let mut path_hints = tool.path_hints.clone();
        if path_hints.is_empty()
            && let Some(path) = first_edit_paths.get(&tool.bubble_id)
        {
            path_hints.push(path.clone());
        }
        if !tool.can_emit_edit() {
            continue;
        }
        if path_hints.len() != 1 {
            continue;
        }

        let abs_path = path_hints[0].clone();
        let mut added_lines = Vec::new();
        let mut removed_lines = Vec::new();
        for patch in &tool.patch_texts {
            let (patch_added, patch_removed) = extract_patch_lines(patch);
            added_lines.extend(patch_added);
            removed_lines.extend(patch_removed);
        }

        if added_lines.is_empty() && removed_lines.is_empty() {
            continue;
        }

        let mut line_hashes = hash_counts_for_lines(&added_lines, LineSide::Added);
        line_hashes.extend(hash_counts_for_lines(&removed_lines, LineSide::Removed));

        edits.push(ResolvedFileEdit {
            abs_path,
            call_id: tool.call_id.clone(),
            op_index: 0,
            timestamp: tool.timestamp.clone().or_else(|| graph.last_seen_at()),
            write_mode: WriteMode::Patch,
            before_known: true,
            added_lines: added_lines.len() as i64,
            removed_lines: removed_lines.len() as i64,
            parser_name: format!("cursor_tool_{}_v1", tool.name),
            line_hashes,
        });
    }

    edits
}

pub(crate) fn aggregate_file_edits(edits: &[ResolvedFileEdit]) -> Vec<AggregatedFileEdit> {
    let mut by_path: HashMap<String, AggregatedFileEdit> = HashMap::new();
    for edit in edits {
        let entry = by_path
            .entry(edit.abs_path.clone())
            .or_insert(AggregatedFileEdit {
                abs_path: edit.abs_path.clone(),
                added_lines: 0,
                removed_lines: 0,
            });
        entry.added_lines += edit.added_lines;
        entry.removed_lines += edit.removed_lines;
    }

    let mut out: Vec<_> = by_path.into_values().collect();
    out.sort_by_key(|edit| edit.abs_path.clone());
    out
}

pub(crate) fn build_seed_graph(
    key: &str,
    raw: &str,
    source_file: &str,
) -> Option<CursorSessionGraph> {
    let raw_json: JsonValue = serde_json::from_str(raw).ok()?;
    let data: ComposerData = serde_json::from_str(raw).ok()?;

    let composer_id = raw_json
        .get("composerId")
        .and_then(|value| value.as_str())
        .map(ToOwned::to_owned)
        .or_else(|| key.strip_prefix("composerData:").map(ToOwned::to_owned))
        .unwrap_or(data.composer_id);

    let started_at = data.created_at.and_then(ms_to_iso);
    let ended_at = data.last_updated_at.and_then(ms_to_iso);

    let mut conversation_messages = Vec::new();
    for (idx, bubble) in data.conversation.iter().enumerate() {
        let role = match bubble.bubble_type {
            Some(1) => Some(CursorBubbleRole::User),
            Some(2) => Some(CursorBubbleRole::Assistant),
            _ => None,
        };
        conversation_messages.push(CursorBubbleEvent {
            role,
            text: bubble.text.clone(),
            order_key: idx as i64,
        });
    }

    let original_file_states = extract_original_file_states(&data.original_file_states);
    let mut strong_path_hints = extract_strong_path_hints(
        data.context.as_ref(),
        &data.all_attached_file_code_chunks_uris,
        &data.code_block_data,
    );
    let weak_path_hints = extract_weak_path_hints(&original_file_states);
    let project_path = extract_project_path(
        data.context.as_ref(),
        &data.all_attached_file_code_chunks_uris,
        &data.original_file_states,
        &data.code_block_data,
    );

    for path in original_file_states.keys() {
        if !is_cursor_plan_path(path) {
            strong_path_hints.insert(path.clone());
        }
    }

    Some(CursorSessionGraph {
        composer_id,
        source_file: source_file.to_string(),
        created_at_ms: data.created_at,
        last_updated_at_ms: data.last_updated_at,
        started_at,
        ended_at,
        project_path: project_path.clone(),
        model_name: extract_model_name(&raw_json),
        subtitle: data.subtitle,
        files_changed_count: data.files_changed_count,
        total_lines_added: data.total_lines_added,
        total_lines_removed: data.total_lines_removed,
        conversation_messages,
        bubble_events: Vec::new(),
        tool_calls: Vec::new(),
        checkpoint_paths: HashSet::new(),
        strong_path_hints,
        weak_path_hints,
        original_file_states,
        partial_targets: extract_partial_targets(&data.code_block_data),
        legacy_targets: extract_legacy_targets(
            &data.code_block_data,
            ended_at_from_ms(data.last_updated_at, data.created_at),
        ),
        inline_hints: Vec::new(),
        inline_undo_rows: Vec::new(),
        partial_fates: HashMap::new(),
        legacy_diff_payloads: HashMap::new(),
    })
}

fn populate_graph_details(
    vscdb: &Connection,
    graphs: &mut [CursorSessionGraph],
    session_ids: &HashSet<String>,
) -> Result<()> {
    if graphs.is_empty() {
        return Ok(());
    }

    let mut by_session: HashMap<String, usize> = HashMap::new();
    for (idx, graph) in graphs.iter().enumerate() {
        by_session.insert(graph.composer_id.clone(), idx);
    }

    populate_bubbles(vscdb, graphs, &by_session)?;
    populate_checkpoints(vscdb, graphs, &by_session)?;
    populate_inline_undo_rows(vscdb, graphs, &by_session)?;
    populate_inline_hints(vscdb, graphs, &by_session)?;
    populate_partial_fates(vscdb, graphs, &by_session)?;
    populate_legacy_diffs(vscdb, graphs, session_ids, &by_session)?;
    Ok(())
}

fn populate_bubbles(
    vscdb: &Connection,
    graphs: &mut [CursorSessionGraph],
    by_session: &HashMap<String, usize>,
) -> Result<()> {
    let header_orders: HashMap<String, HashMap<String, i64>> = graphs
        .iter()
        .map(|graph| {
            let Some(raw_headers) = query_conversation_headers(vscdb, &graph.composer_id)
                .ok()
                .flatten()
            else {
                return (graph.composer_id.clone(), HashMap::new());
            };
            (graph.composer_id.clone(), raw_headers)
        })
        .collect();

    let mut stmt =
        vscdb.prepare("SELECT rowid, key, value FROM cursorDiskKV WHERE key LIKE 'bubbleId:%'")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
        ))
    })?;

    for row in rows {
        let (rowid, key, raw) = match row {
            Ok(row) => row,
            Err(_) => continue,
        };
        let Some(raw) = raw else {
            continue;
        };
        let Some((session_id, bubble_id)) = bubble_key_parts(&key) else {
            continue;
        };
        let Some(graph_idx) = by_session.get(session_id).copied() else {
            continue;
        };
        let bubble: JsonValue = match serde_json::from_str(&raw) {
            Ok(value) => value,
            Err(_) => continue,
        };

        let role = bubble
            .get("type")
            .and_then(|value| value.as_i64())
            .and_then(map_bubble_role);
        let text = bubble
            .get("text")
            .and_then(|value| value.as_str())
            .map(ToOwned::to_owned);
        let order_key = header_orders
            .get(session_id)
            .and_then(|headers| headers.get(bubble_id))
            .copied()
            .unwrap_or(rowid);
        let model_name = bubble
            .get("modelInfo")
            .and_then(|value| value.get("modelName"))
            .and_then(|value| value.as_str())
            .map(ToOwned::to_owned);

        graphs[graph_idx].bubble_events.push(CursorBubbleEvent {
            role,
            text: text.clone(),
            order_key,
        });

        if graphs[graph_idx].model_name.is_none() {
            graphs[graph_idx].model_name = model_name;
        }

        if let Some(tool_call) = parse_tool_call(
            bubble_id,
            &bubble,
            graphs[graph_idx].project_path.as_deref(),
            graphs[graph_idx].last_seen_at(),
            &graphs[graph_idx].first_edit_path_by_bubble(),
        ) {
            for path in &tool_call.path_hints {
                if !is_cursor_plan_path(path) {
                    graphs[graph_idx].strong_path_hints.insert(path.clone());
                }
            }
            graphs[graph_idx].tool_calls.push(tool_call);
        }
    }

    for graph in graphs.iter_mut() {
        graph.bubble_events.sort_by_key(|event| event.order_key);
    }

    Ok(())
}

fn populate_checkpoints(
    vscdb: &Connection,
    graphs: &mut [CursorSessionGraph],
    by_session: &HashMap<String, usize>,
) -> Result<()> {
    let mut stmt =
        vscdb.prepare("SELECT key, value FROM cursorDiskKV WHERE key LIKE 'checkpointId:%'")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
    })?;

    for row in rows {
        let (key, raw) = match row {
            Ok(row) => row,
            Err(_) => continue,
        };
        let Some(raw) = raw else {
            continue;
        };
        let Some(session_id) = composer_id_from_prefixed_key(&key, "checkpointId") else {
            continue;
        };
        let Some(graph_idx) = by_session.get(session_id).copied() else {
            continue;
        };

        let parsed: JsonValue = match serde_json::from_str(&raw) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let Some(files) = parsed.get("files").and_then(|value| value.as_array()) else {
            continue;
        };
        for file in files {
            let Some(path) = file.get("uri").and_then(extract_file_path_from_uri_value) else {
                continue;
            };
            if is_cursor_plan_path(&path) {
                continue;
            }
            graphs[graph_idx].checkpoint_paths.insert(path.clone());
            graphs[graph_idx].strong_path_hints.insert(path);
        }
    }

    Ok(())
}

fn populate_inline_undo_rows(
    vscdb: &Connection,
    graphs: &mut [CursorSessionGraph],
    by_session: &HashMap<String, usize>,
) -> Result<()> {
    let mut stmt = vscdb
        .prepare("SELECT key, value FROM cursorDiskKV WHERE key LIKE 'inlineDiffUndoRedo-%'")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
    })?;

    for row in rows {
        let (key, raw) = match row {
            Ok(row) => row,
            Err(_) => continue,
        };
        let Some(raw) = raw else {
            continue;
        };
        let payload: JsonValue = match serde_json::from_str(&raw) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let Some(session_id) = payload
            .get("composerMetadata")
            .and_then(|value| value.get("composerId"))
            .and_then(|value| value.as_str())
        else {
            continue;
        };
        let Some(graph_idx) = by_session.get(session_id).copied() else {
            continue;
        };
        let Some(abs_path) = payload
            .get("uri")
            .and_then(extract_file_path_from_uri_value)
            .filter(|path| !is_cursor_plan_path(path))
        else {
            continue;
        };

        graphs[graph_idx]
            .inline_undo_rows
            .push(CursorInlineUndoRow {
                call_id: key,
                abs_path,
                timestamp: payload
                    .get("createdAt")
                    .and_then(|value| value.as_i64())
                    .and_then(ms_to_iso),
                payload,
            });
    }

    Ok(())
}

fn populate_inline_hints(
    vscdb: &Connection,
    graphs: &mut [CursorSessionGraph],
    by_session: &HashMap<String, usize>,
) -> Result<()> {
    let mut stmt = vscdb.prepare("SELECT value FROM cursorDiskKV WHERE key LIKE 'inlineDiff:%'")?;
    let rows = stmt.query_map([], |row| row.get::<_, Option<String>>(0))?;

    for row in rows {
        let Some(raw) = row? else {
            continue;
        };
        let payload: JsonValue = match serde_json::from_str(&raw) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let Some(session_id) = payload
            .get("composerMetadata")
            .and_then(|value| value.get("composerId"))
            .and_then(|value| value.as_str())
        else {
            continue;
        };
        let Some(graph_idx) = by_session.get(session_id).copied() else {
            continue;
        };
        let Some(abs_path) = payload
            .get("uri")
            .and_then(extract_file_path_from_uri_value)
            .filter(|path| !is_cursor_plan_path(path))
        else {
            continue;
        };

        graphs[graph_idx].inline_hints.push(CursorInlineHint {
            call_id: payload
                .get("composerMetadata")
                .and_then(|value| value.get("toolCallId"))
                .and_then(|value| value.as_str())
                .or_else(|| {
                    payload
                        .get("composerMetadata")
                        .and_then(|value| value.get("codeblockId"))
                        .and_then(|value| value.as_str())
                })
                .or_else(|| payload.get("diffId").and_then(|value| value.as_str()))
                .unwrap_or("inlineDiff")
                .to_string(),
            abs_path: abs_path.clone(),
            timestamp: payload
                .get("createdAt")
                .and_then(|value| value.as_i64())
                .and_then(ms_to_iso),
            original_text_lines: parse_string_array(payload.get("originalTextLines")),
        });
        graphs[graph_idx].strong_path_hints.insert(abs_path);
    }

    Ok(())
}

fn populate_partial_fates(
    vscdb: &Connection,
    graphs: &mut [CursorSessionGraph],
    by_session: &HashMap<String, usize>,
) -> Result<()> {
    let mut stmt = vscdb.prepare(
        "SELECT key, value FROM cursorDiskKV WHERE key LIKE 'codeBlockPartialInlineDiffFates:%'",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
    })?;

    for row in rows {
        let (key, raw) = match row {
            Ok(row) => row,
            Err(_) => continue,
        };
        let Some(raw) = raw else {
            continue;
        };
        let Some((session_id, partial_id)) = partial_fates_key_parts(&key) else {
            continue;
        };
        let Some(graph_idx) = by_session.get(session_id).copied() else {
            continue;
        };
        let payload: JsonValue = match serde_json::from_str(&raw) {
            Ok(value) => value,
            Err(_) => continue,
        };
        graphs[graph_idx]
            .partial_fates
            .insert(partial_id.to_string(), payload);
    }

    Ok(())
}

fn populate_legacy_diffs(
    vscdb: &Connection,
    graphs: &mut [CursorSessionGraph],
    session_ids: &HashSet<String>,
    by_session: &HashMap<String, usize>,
) -> Result<()> {
    let mut stmt =
        vscdb.prepare("SELECT key, value FROM cursorDiskKV WHERE key LIKE 'codeBlockDiff:%'")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
    })?;

    for row in rows {
        let (key, raw) = match row {
            Ok(row) => row,
            Err(_) => continue,
        };
        let Some(raw) = raw else {
            continue;
        };
        let Some((session_id, diff_id)) = code_block_diff_key_parts(&key) else {
            continue;
        };
        if !session_ids.contains(session_id) {
            continue;
        }
        let Some(graph_idx) = by_session.get(session_id).copied() else {
            continue;
        };
        let payload: JsonValue = match serde_json::from_str(&raw) {
            Ok(value) => value,
            Err(_) => continue,
        };
        graphs[graph_idx]
            .legacy_diff_payloads
            .insert(diff_id.to_string(), payload);
    }

    Ok(())
}

fn parse_tool_call(
    bubble_id: &str,
    bubble: &JsonValue,
    project_path: Option<&str>,
    default_timestamp: Option<String>,
    first_edit_paths: &HashMap<String, String>,
) -> Option<CursorToolCall> {
    let tool = bubble.get("toolFormerData")?.as_object()?;
    let name = tool.get("name")?.as_str()?.to_string();
    let status = tool
        .get("status")
        .and_then(|value| value.as_str())
        .map(ToOwned::to_owned);
    let call_id = tool
        .get("toolCallId")
        .and_then(|value| value.as_str())
        .or_else(|| tool.get("callId").and_then(|value| value.as_str()))
        .unwrap_or(bubble_id)
        .to_string();

    let params_json = tool.get("params").and_then(jsonish_to_value);
    let result_json = tool.get("result").and_then(jsonish_to_value);
    let raw_args = tool.get("rawArgs").and_then(|value| value.as_str());

    let mut path_hints = Vec::new();
    if let Some(params_json) = params_json.as_ref() {
        collect_tool_paths(params_json, project_path, &mut path_hints);
    }
    if path_hints.is_empty()
        && let Some(path) = first_edit_paths.get(bubble_id)
    {
        path_hints.push(path.clone());
    }
    dedupe_vec(&mut path_hints);

    let mut patch_texts = Vec::new();
    if name == "edit_file_v2"
        && let Some(params_json) = params_json.as_ref()
        && let Some(streaming) = params_json
            .get("streamingContent")
            .and_then(|value| value.as_str())
    {
        patch_texts.push(streaming.to_string());
    }
    if name == "apply_patch" {
        if status.as_deref() == Some("completed")
            && let Some(raw_args) = raw_args
        {
            patch_texts.push(raw_args.to_string());
        }
        if let Some(result_json) = result_json.as_ref() {
            collect_result_diff_strings(result_json, &mut patch_texts);
        }
    }
    if matches!(name.as_str(), "write_file" | "write_file_v2")
        && let Some(result_json) = result_json.as_ref()
    {
        collect_result_diff_strings(result_json, &mut patch_texts);
    }
    dedupe_vec(&mut patch_texts);

    Some(CursorToolCall {
        bubble_id: bubble_id.to_string(),
        name,
        call_id,
        status,
        timestamp: default_timestamp,
        path_hints,
        patch_texts,
    })
}

fn collect_tool_paths(value: &JsonValue, project_path: Option<&str>, out: &mut Vec<String>) {
    match value {
        JsonValue::Object(map) => {
            for key in [
                "relativeWorkspacePath",
                "targetFile",
                "effectiveUri",
                "path",
            ] {
                if let Some(raw) = map.get(key).and_then(|value| value.as_str())
                    && let Some(path) = normalize_tool_path(raw, project_path)
                {
                    out.push(path);
                }
            }
            if let Some(paths) = map.get("paths").and_then(|value| value.as_array()) {
                for path in paths {
                    if let Some(raw) = path.as_str()
                        && let Some(path) = normalize_tool_path(raw, project_path)
                    {
                        out.push(path);
                    } else if let Some(raw) = path
                        .get("relativeWorkspacePath")
                        .and_then(|value| value.as_str())
                        && let Some(path) = normalize_tool_path(raw, project_path)
                    {
                        out.push(path);
                    }
                }
            }
        }
        JsonValue::Array(values) => {
            for value in values {
                collect_tool_paths(value, project_path, out);
            }
        }
        _ => {}
    }
}

fn collect_result_diff_strings(value: &JsonValue, out: &mut Vec<String>) {
    match value {
        JsonValue::Object(map) => {
            if let Some(chunks) = map
                .get("diff")
                .and_then(|value| value.get("chunks"))
                .and_then(|value| value.as_array())
            {
                for chunk in chunks {
                    if let Some(diff) = chunk.get("diffString").and_then(|value| value.as_str()) {
                        out.push(diff.to_string());
                    }
                }
            }
            for value in map.values() {
                collect_result_diff_strings(value, out);
            }
        }
        JsonValue::Array(values) => {
            for value in values {
                collect_result_diff_strings(value, out);
            }
        }
        _ => {}
    }
}

fn jsonish_to_value(value: &JsonValue) -> Option<JsonValue> {
    match value {
        JsonValue::Object(_) | JsonValue::Array(_) => Some(value.clone()),
        JsonValue::String(raw) => serde_json::from_str(raw).ok(),
        _ => None,
    }
}

fn normalize_tool_path(raw: &str, project_path: Option<&str>) -> Option<String> {
    let normalized = if raw.starts_with("file://") {
        strip_file_scheme(raw)
    } else {
        normalize_filesystem_path(raw)
    };

    let path = Path::new(&normalized);
    if path.is_absolute() {
        return Some(normalized);
    }

    let project_path = project_path?;
    Some(join_project_relative_path(project_path, path))
}

fn join_project_relative_path(project_path: &str, relative_path: &Path) -> String {
    let base_components: Vec<_> = Path::new(project_path).components().collect();
    let rel_components: Vec<_> = relative_path.components().collect();

    let mut overlap = 0usize;
    let max_overlap = base_components.len().min(rel_components.len());
    for len in 1..=max_overlap {
        let base_suffix = &base_components[base_components.len() - len..];
        let rel_prefix = &rel_components[..len];
        if base_suffix == rel_prefix {
            overlap = len;
        }
    }

    let mut joined = PathBuf::new();
    for component in &base_components[..base_components.len().saturating_sub(overlap)] {
        joined.push(component.as_os_str());
    }
    for component in &rel_components {
        joined.push(component.as_os_str());
    }

    normalize_filesystem_path(joined.to_string_lossy().as_ref())
}

fn extract_original_file_states(
    original_file_states: &HashMap<String, JsonValue>,
) -> HashMap<String, CursorOriginalFileState> {
    let mut out = HashMap::new();
    for (raw_path, state) in original_file_states {
        let path = strip_file_scheme(raw_path);
        if is_cursor_plan_path(&path) {
            continue;
        }

        out.insert(
            path,
            CursorOriginalFileState {
                content: state
                    .get("content")
                    .and_then(|value| value.as_str())
                    .map(ToOwned::to_owned),
                first_edit_bubble_id: state
                    .get("firstEditBubbleId")
                    .and_then(|value| value.as_str())
                    .map(ToOwned::to_owned),
            },
        );
    }
    out
}

fn extract_strong_path_hints(
    context: Option<&ComposerContext>,
    attached_uris: &[String],
    code_block_data: &HashMap<String, JsonValue>,
) -> HashSet<String> {
    let mut out = HashSet::new();

    if let Some(context) = context {
        for selection in &context.file_selections {
            if let Some(uri) = &selection.uri
                && let Some(fs_path) = &uri.fs_path
            {
                let path = normalize_filesystem_path(fs_path);
                if !is_cursor_plan_path(&path) {
                    out.insert(path);
                }
            }
        }
    }

    for uri in attached_uris {
        let path = strip_file_scheme(uri);
        if !is_cursor_plan_path(&path) {
            out.insert(path);
        }
    }

    for file_key in code_block_data.keys() {
        if let Some(path) = extract_file_path_from_string(file_key)
            && !is_cursor_plan_path(&path)
        {
            out.insert(path);
        }
    }

    out
}

fn extract_weak_path_hints(
    original_file_states: &HashMap<String, CursorOriginalFileState>,
) -> HashSet<String> {
    original_file_states.keys().cloned().collect()
}

fn extract_partial_targets(
    code_block_data: &HashMap<String, JsonValue>,
) -> Vec<CursorPartialTarget> {
    let mut out = HashSet::new();

    for (file_key, value) in code_block_data {
        for entry in code_block_entries(value) {
            let Some(entry_obj) = entry.as_object() else {
                continue;
            };
            if entry_obj.get("status").and_then(|value| value.as_str()) != Some("accepted") {
                continue;
            }
            let Some(partial_id) = entry_obj
                .get("partialInlineDiffFatesId")
                .and_then(|value| value.as_str())
                .map(ToOwned::to_owned)
            else {
                continue;
            };
            let abs_path = entry_obj
                .get("uri")
                .and_then(extract_file_path_from_uri_value)
                .or_else(|| extract_file_path_from_string(file_key));
            let Some(abs_path) = abs_path else {
                continue;
            };
            if is_cursor_plan_path(&abs_path) {
                continue;
            }

            out.insert(CursorPartialTarget {
                partial_id,
                abs_path,
            });
        }
    }

    out.into_iter().collect()
}

fn extract_legacy_targets(
    code_block_data: &HashMap<String, JsonValue>,
    default_timestamp: Option<String>,
) -> Vec<CursorLegacyTarget> {
    let mut targets = Vec::new();

    for (file_key, value) in code_block_data {
        for entry in code_block_entries(value) {
            let Some(entry_obj) = entry.as_object() else {
                continue;
            };
            if entry_obj.get("status").and_then(|value| value.as_str()) != Some("accepted") {
                continue;
            }
            if entry_obj
                .get("isNotApplied")
                .and_then(|value| value.as_bool())
                == Some(true)
            {
                continue;
            }
            if entry_obj.get("isNoOp").and_then(|value| value.as_bool()) == Some(true) {
                continue;
            }

            let abs_path = entry_obj
                .get("uri")
                .and_then(extract_file_path_from_uri_value)
                .or_else(|| extract_file_path_from_string(file_key));
            let Some(abs_path) = abs_path else {
                continue;
            };
            if is_cursor_plan_path(&abs_path) {
                continue;
            }

            let diff_id = entry_obj
                .get("diffId")
                .and_then(|value| value.as_str())
                .map(ToOwned::to_owned);
            let content = entry_obj
                .get("content")
                .and_then(|value| value.as_str())
                .map(ToOwned::to_owned);
            if diff_id.is_none() && content.is_none() {
                continue;
            }

            targets.push(CursorLegacyTarget {
                diff_id,
                abs_path,
                version: entry_obj
                    .get("version")
                    .and_then(|value| value.as_i64())
                    .unwrap_or(0) as i32,
                code_block_idx: entry_obj
                    .get("codeBlockIdx")
                    .and_then(|value| value.as_i64())
                    .unwrap_or(0) as i32,
                timestamp: default_timestamp.clone(),
                content,
                bubble_id: entry_obj
                    .get("bubbleId")
                    .and_then(|value| value.as_str())
                    .map(ToOwned::to_owned),
            });
        }
    }

    targets
}

fn code_block_entries(value: &JsonValue) -> Vec<&JsonValue> {
    if let Some(list) = value.as_array() {
        return list.iter().collect();
    }
    if let Some(map) = value.as_object() {
        return map.values().collect();
    }
    Vec::new()
}

fn extract_project_path(
    context: Option<&ComposerContext>,
    attached_uris: &[String],
    original_file_states: &HashMap<String, JsonValue>,
    code_block_data: &HashMap<String, JsonValue>,
) -> Option<String> {
    let mut paths = Vec::new();

    if let Some(context) = context {
        for selection in &context.file_selections {
            if let Some(uri) = &selection.uri
                && let Some(fs_path) = &uri.fs_path
            {
                paths.push(normalize_filesystem_path(fs_path));
            }
        }
    }

    if paths.is_empty() {
        for uri in attached_uris {
            paths.push(strip_file_scheme(uri));
        }
    }

    if paths.is_empty() {
        for path in original_file_states.keys() {
            paths.push(strip_file_scheme(path));
        }
    }

    if paths.is_empty() {
        for path in code_block_data.keys() {
            paths.push(strip_file_scheme(path));
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

fn query_conversation_headers(
    vscdb: &Connection,
    composer_id: &str,
) -> Result<Option<HashMap<String, i64>>> {
    let raw: Option<String> = vscdb
        .query_row(
            "SELECT value FROM cursorDiskKV WHERE key = ?1",
            params![format!("composerData:{composer_id}")],
            |row| row.get(0),
        )
        .optional()?;
    let Some(raw) = raw else {
        return Ok(None);
    };
    let data: ComposerData = match serde_json::from_str(&raw) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };

    let mut out = HashMap::new();
    for (idx, header) in data.full_conversation_headers_only.iter().enumerate() {
        out.insert(header.bubble_id.clone(), idx as i64);
    }
    Ok(Some(out))
}

fn ended_at_from_ms(last_updated_at: Option<i64>, created_at: Option<i64>) -> Option<String> {
    last_updated_at
        .and_then(ms_to_iso)
        .or_else(|| created_at.and_then(ms_to_iso))
}

fn map_bubble_role(value: i64) -> Option<CursorBubbleRole> {
    match value {
        1 => Some(CursorBubbleRole::User),
        2 => Some(CursorBubbleRole::Assistant),
        _ => None,
    }
}

fn bubble_key_parts(key: &str) -> Option<(&str, &str)> {
    let mut parts = key.splitn(3, ':');
    if parts.next()? != "bubbleId" {
        return None;
    }
    Some((parts.next()?, parts.next()?))
}

fn composer_id_from_prefixed_key<'a>(key: &'a str, prefix: &str) -> Option<&'a str> {
    let mut parts = key.splitn(3, ':');
    if parts.next()? != prefix {
        return None;
    }
    parts.next()
}

fn partial_fates_key_parts(key: &str) -> Option<(&str, &str)> {
    let mut parts = key.splitn(3, ':');
    if parts.next()? != "codeBlockPartialInlineDiffFates" {
        return None;
    }
    Some((parts.next()?, parts.next()?))
}

fn code_block_diff_key_parts(key: &str) -> Option<(&str, &str)> {
    let mut parts = key.splitn(3, ':');
    if parts.next()? != "codeBlockDiff" {
        return None;
    }
    Some((parts.next()?, parts.next()?))
}

pub(crate) fn parse_string_array(value: Option<&JsonValue>) -> Vec<String> {
    let Some(value) = value else {
        return Vec::new();
    };
    let Some(arr) = value.as_array() else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|value| value.as_str().map(ToOwned::to_owned))
        .collect()
}

pub(crate) fn extract_patch_lines(text: &str) -> (Vec<String>, Vec<String>) {
    let mut added = Vec::new();
    let mut removed = Vec::new();
    for line in text.lines() {
        if line.starts_with("+++") || line.starts_with("---") || line.starts_with("@@") {
            continue;
        }
        if line.starts_with("*** ") {
            continue;
        }
        if line.starts_with("\\ No newline at end of file") {
            continue;
        }
        if let Some(rest) = line.strip_prefix('+') {
            added.push(rest.to_string());
        } else if let Some(rest) = line.strip_prefix('-') {
            removed.push(rest.to_string());
        }
    }
    (added, removed)
}

pub(crate) fn patch_has_real_changes(text: &str) -> bool {
    let (added, removed) = extract_patch_lines(text);
    !added.is_empty() || !removed.is_empty()
}

pub(crate) fn hash_counts_for_lines(lines: &[String], side: LineSide) -> Vec<LineHashCount> {
    let mut counts: HashMap<String, i64> = HashMap::new();
    for line in lines {
        *counts.entry(hash_line(line)).or_insert(0) += 1;
    }
    counts
        .into_iter()
        .map(|(line_hash, count)| LineHashCount {
            side,
            line_hash,
            count,
        })
        .collect()
}

pub(crate) fn extract_file_path_from_uri_value(uri: &JsonValue) -> Option<String> {
    match uri {
        JsonValue::Object(map) => {
            if let Some(scheme) = map.get("scheme").and_then(|value| value.as_str())
                && scheme != "file"
            {
                return None;
            }

            if let Some(fs_path) = map.get("fsPath").and_then(|value| value.as_str()) {
                return Some(normalize_filesystem_path(fs_path));
            }
            if let Some(external) = map.get("external").and_then(|value| value.as_str()) {
                return extract_file_path_from_string(external);
            }
            if let Some(path) = map.get("path").and_then(|value| value.as_str()) {
                return Some(normalize_filesystem_path(path));
            }
            None
        }
        JsonValue::String(raw) => extract_file_path_from_string(raw),
        _ => None,
    }
}

pub(crate) fn extract_file_path_from_string(raw: &str) -> Option<String> {
    if raw.starts_with("file://") {
        return Some(strip_file_scheme(raw));
    }
    if raw.starts_with("vscode-notebook-cell:") {
        return None;
    }
    if raw.contains("://") {
        return None;
    }
    Some(normalize_filesystem_path(raw))
}

pub(crate) fn is_cursor_plan_path(path: &str) -> bool {
    let normalized = normalize_filesystem_path(path);
    normalized.contains("/.cursor/plans/") || normalized.contains("/.cursor/plans\\")
}

pub(crate) fn ms_to_iso(ms: i64) -> Option<String> {
    chrono::Utc
        .timestamp_millis_opt(ms)
        .single()
        .map(|dt| dt.to_rfc3339())
}

fn dedupe_vec(values: &mut Vec<String>) {
    let mut seen = HashSet::new();
    values.retain(|value| seen.insert(value.clone()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::{Connection, params};
    use serde_json::json;
    use tempfile::NamedTempFile;

    #[test]
    fn resolves_relative_tool_paths_against_project_root() {
        assert_eq!(
            normalize_tool_path("tests/pages/user-support-po.ts", Some("/tmp/repo")).as_deref(),
            Some("/tmp/repo/tests/pages/user-support-po.ts")
        );
    }

    #[test]
    fn avoids_double_joining_overlapping_relative_tool_paths() {
        assert_eq!(
            normalize_tool_path(
                "tests/pages/user-support-po.ts",
                Some("/tmp/repo/tests/pages"),
            )
            .as_deref(),
            Some("/tmp/repo/tests/pages/user-support-po.ts")
        );
    }

    #[test]
    fn parses_edit_file_patch_lines() {
        let (added, removed) = extract_patch_lines(
            "@@\n import { A } from './a';\n-import { B } from './b';\n+import { C } from './c';\n",
        );
        assert_eq!(added, vec!["import { C } from './c';"]);
        assert_eq!(removed, vec!["import { B } from './b';"]);
    }

    #[test]
    fn resolve_tool_call_edits_uses_first_edit_bubble_as_path_bridge() {
        let graph = CursorSessionGraph {
            composer_id: "c1".to_string(),
            source_file: "/tmp/state.vscdb".to_string(),
            created_at_ms: None,
            last_updated_at_ms: None,
            started_at: None,
            ended_at: Some("2026-04-18T11:07:54Z".to_string()),
            project_path: Some("/tmp/repo".to_string()),
            model_name: None,
            subtitle: None,
            files_changed_count: Some(1),
            total_lines_added: Some(1),
            total_lines_removed: Some(1),
            conversation_messages: Vec::new(),
            bubble_events: Vec::new(),
            tool_calls: vec![CursorToolCall {
                bubble_id: "bubble-1".to_string(),
                name: "edit_file_v2".to_string(),
                call_id: "call-1".to_string(),
                status: Some("completed".to_string()),
                timestamp: None,
                path_hints: Vec::new(),
                patch_texts: vec!["@@\n-old\n+new\n".to_string()],
            }],
            checkpoint_paths: HashSet::new(),
            strong_path_hints: HashSet::new(),
            weak_path_hints: HashSet::new(),
            original_file_states: HashMap::from([(
                "/tmp/repo/src/app.ts".to_string(),
                CursorOriginalFileState {
                    content: Some("old\n".to_string()),
                    first_edit_bubble_id: Some("bubble-1".to_string()),
                },
            )]),
            partial_targets: Vec::new(),
            legacy_targets: Vec::new(),
            inline_hints: Vec::new(),
            inline_undo_rows: Vec::new(),
            partial_fates: HashMap::new(),
            legacy_diff_payloads: HashMap::new(),
        };

        let edits = resolve_tool_call_edits(&graph);
        assert_eq!(edits.len(), 1);
        assert_eq!(edits[0].abs_path, "/tmp/repo/src/app.ts");
        assert_eq!(edits[0].added_lines, 1);
        assert_eq!(edits[0].removed_lines, 1);
    }

    #[test]
    fn load_cursor_session_graphs_skips_null_composer_rows() {
        let file = NamedTempFile::new().expect("temp db should be created");
        let conn = Connection::open(file.path()).expect("temp db should open");
        conn.execute_batch(
            "CREATE TABLE cursorDiskKV (
                key   TEXT PRIMARY KEY,
                value TEXT
            );",
        )
        .expect("schema should be created");

        conn.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (?1, NULL)",
            params!["composerData:null-session"],
        )
        .expect("null composer row should insert");
        conn.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
            params![
                "composerData:real-session",
                json!({
                    "composerId": "real-session",
                    "conversation": [{"type": 1, "text": "hello"}],
                    "createdAt": 1,
                    "lastUpdatedAt": 2,
                })
                .to_string()
            ],
        )
        .expect("real composer row should insert");

        let graphs =
            load_cursor_session_graphs(&conn, "/tmp/state.vscdb").expect("graph load should work");
        assert_eq!(graphs.len(), 1);
        assert_eq!(graphs[0].composer_id, "real-session");
    }

}
