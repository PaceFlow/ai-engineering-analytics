use std::collections::HashMap;

use serde_json::Value;

use crate::change_intel::line_hash::hash_line;
use crate::change_intel::path_resolver::{
    detect_repo_root, path_to_string, resolve_path, to_rel_path,
};
use crate::change_intel::session_context::SessionContext;
use crate::change_intel::types::{
    ChangeOpCandidate, LineHashCount, LineSide, ParseError, ParseOutcome, PatternParser,
    ToolCallEvent, WriteMode,
};

#[derive(Debug, Clone)]
struct PatchFileBlock {
    raw_path: String,
    added_lines: Vec<String>,
    removed_lines: Vec<String>,
}

pub struct ApplyPatchParser;

impl ApplyPatchParser {
    pub const PARSER_NAME: &'static str = "apply_patch_v1";

    fn parse_input_text(input_json: &str) -> Option<String> {
        let trimmed = input_json.trim();
        if trimmed.is_empty() {
            return None;
        }

        if trimmed.starts_with("*** Begin Patch") {
            return Some(trimmed.to_string());
        }

        let parsed: Value = serde_json::from_str(trimmed).ok()?;
        match parsed {
            Value::String(s) => Some(s),
            Value::Object(map) => map
                .get("patch")
                .and_then(|v| v.as_str())
                .map(ToOwned::to_owned)
                .or_else(|| {
                    map.get("input")
                        .and_then(|v| v.as_str())
                        .map(ToOwned::to_owned)
                }),
            _ => None,
        }
    }

    fn parse_patch_blocks(patch: &str) -> Result<Vec<PatchFileBlock>, String> {
        let mut blocks = Vec::new();
        let mut current: Option<PatchFileBlock> = None;

        for line in patch.lines() {
            if let Some(rest) = line.strip_prefix("*** Update File: ") {
                if let Some(prev) = current.take() {
                    blocks.push(prev);
                }
                current = Some(PatchFileBlock {
                    raw_path: rest.trim().to_string(),
                    added_lines: Vec::new(),
                    removed_lines: Vec::new(),
                });
                continue;
            }

            if let Some(rest) = line.strip_prefix("*** Add File: ") {
                if let Some(prev) = current.take() {
                    blocks.push(prev);
                }
                current = Some(PatchFileBlock {
                    raw_path: rest.trim().to_string(),
                    added_lines: Vec::new(),
                    removed_lines: Vec::new(),
                });
                continue;
            }

            if let Some(rest) = line.strip_prefix("*** Delete File: ") {
                if let Some(prev) = current.take() {
                    blocks.push(prev);
                }
                current = Some(PatchFileBlock {
                    raw_path: rest.trim().to_string(),
                    added_lines: Vec::new(),
                    removed_lines: Vec::new(),
                });
                continue;
            }

            if let Some(rest) = line.strip_prefix("*** Move to: ") {
                if let Some(block) = current.as_mut() {
                    block.raw_path = rest.trim().to_string();
                }
                continue;
            }

            if line.starts_with("*** End Patch") {
                break;
            }

            if let Some(block) = current.as_mut() {
                if line.starts_with("+++") || line.starts_with("---") {
                    continue;
                }
                if let Some(rest) = line.strip_prefix('+') {
                    block.added_lines.push(rest.to_string());
                } else if let Some(rest) = line.strip_prefix('-') {
                    block.removed_lines.push(rest.to_string());
                }
            }
        }

        if let Some(last) = current.take() {
            blocks.push(last);
        }

        if blocks.is_empty() {
            return Err("apply_patch input did not contain any file blocks".to_string());
        }

        Ok(blocks)
    }

    fn hash_counts(lines: &[String], side: LineSide) -> Vec<LineHashCount> {
        let mut counts: HashMap<String, i64> = HashMap::new();
        for line in lines {
            let hash = hash_line(line);
            *counts.entry(hash).or_insert(0) += 1;
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
}

impl PatternParser for ApplyPatchParser {
    fn name(&self) -> &'static str {
        Self::PARSER_NAME
    }

    fn parse(&self, event: &ToolCallEvent, ctx: &mut SessionContext) -> ParseOutcome {
        let mut out = ParseOutcome::default();

        if event.tool_name != "apply_patch" {
            return out;
        }

        let Some(patch_text) = Self::parse_input_text(&event.input_json) else {
            out.errors.push(ParseError {
                provider: event.provider.clone(),
                session_id: event.session_id.clone(),
                source_file: event.source_file.clone(),
                call_id: event.call_id.clone(),
                timestamp: event.timestamp.clone(),
                parser_name: Self::PARSER_NAME.to_string(),
                error: "failed to parse apply_patch input".to_string(),
            });
            return out;
        };

        let blocks = match Self::parse_patch_blocks(&patch_text) {
            Ok(v) => v,
            Err(e) => {
                out.errors.push(ParseError {
                    provider: event.provider.clone(),
                    session_id: event.session_id.clone(),
                    source_file: event.source_file.clone(),
                    call_id: event.call_id.clone(),
                    timestamp: event.timestamp.clone(),
                    parser_name: Self::PARSER_NAME.to_string(),
                    error: e,
                });
                return out;
            }
        };

        for (idx, block) in blocks.into_iter().enumerate() {
            let abs_path = resolve_path(&block.raw_path, None, ctx.session_cwd.as_deref());
            let abs_path_s = path_to_string(&abs_path);
            let repo_root = detect_repo_root(&abs_path);
            let rel_path = to_rel_path(repo_root.as_deref(), &abs_path);

            let mut line_hashes = Self::hash_counts(&block.added_lines, LineSide::Added);
            line_hashes.extend(Self::hash_counts(&block.removed_lines, LineSide::Removed));

            out.ops.push(ChangeOpCandidate {
                provider: event.provider.clone(),
                session_id: event.session_id.clone(),
                source_file: event.source_file.clone(),
                call_id: event.call_id.clone(),
                op_index: idx as i32,
                timestamp: event.timestamp.clone(),
                repo_root: repo_root.as_deref().map(path_to_string),
                abs_path: abs_path_s.clone(),
                rel_path,
                write_mode: WriteMode::Patch,
                before_known: true,
                added_lines: block.added_lines.len() as i64,
                removed_lines: block.removed_lines.len() as i64,
                parser_name: Self::PARSER_NAME.to_string(),
                line_hashes,
            });

            ctx.file_cache.remove(&abs_path_s);
        }

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn event(input: &str) -> ToolCallEvent {
        ToolCallEvent {
            provider: "codex".to_string(),
            session_id: "s1".to_string(),
            source_file: "/tmp/s1.jsonl".to_string(),
            timestamp: Some("2026-03-04T10:00:00Z".to_string()),
            call_id: "call_patch".to_string(),
            tool_name: "apply_patch".to_string(),
            input_json: input.to_string(),
            output_json: None,
        }
    }

    fn expected_abs(path: &str) -> PathBuf {
        resolve_path(path, None, Some("/tmp/repo"))
    }

    #[test]
    fn parses_single_update_file() {
        let parser = ApplyPatchParser;
        let mut ctx = SessionContext::new(Some("/tmp/repo".to_string()));

        let patch = "*** Begin Patch\n*** Update File: src/a.txt\n@@\n-old\n+new\n*** End Patch\n";
        let out = parser.parse(&event(patch), &mut ctx);

        assert!(out.errors.is_empty());
        assert_eq!(out.ops.len(), 1);
        let op = &out.ops[0];
        assert_eq!(op.write_mode, WriteMode::Patch);
        assert_eq!(op.added_lines, 1);
        assert_eq!(op.removed_lines, 1);
        assert_eq!(PathBuf::from(&op.abs_path), expected_abs("src/a.txt"));
    }

    #[test]
    fn parses_add_file_and_multifile() {
        let parser = ApplyPatchParser;
        let mut ctx = SessionContext::new(Some("/tmp/repo".to_string()));

        let patch = "*** Begin Patch\n*** Add File: src/new.txt\n+hello\n+world\n*** Update File: src/a.txt\n@@\n-old\n+new\n*** End Patch\n";
        let out = parser.parse(&event(patch), &mut ctx);

        assert!(out.errors.is_empty());
        assert_eq!(out.ops.len(), 2);
        assert_eq!(out.ops[0].added_lines, 2);
        assert_eq!(out.ops[0].removed_lines, 0);
        assert_eq!(out.ops[1].added_lines, 1);
        assert_eq!(out.ops[1].removed_lines, 1);
    }

    #[test]
    fn malformed_patch_records_error() {
        let parser = ApplyPatchParser;
        let mut ctx = SessionContext::new(Some("/tmp/repo".to_string()));

        let out = parser.parse(
            &event("*** Begin Patch\n@@\n+no file header\n*** End Patch\n"),
            &mut ctx,
        );
        assert!(out.ops.is_empty());
        assert_eq!(out.errors.len(), 1);
    }

    #[test]
    fn touched_file_cache_is_invalidated() {
        let parser = ApplyPatchParser;
        let mut ctx = SessionContext::new(Some("/tmp/repo".to_string()));
        let abs_path = expected_abs("src/a.txt").to_string_lossy().to_string();
        ctx.file_cache.insert(abs_path.clone(), "old".to_string());

        let patch = "*** Begin Patch\n*** Update File: src/a.txt\n@@\n-old\n+new\n*** End Patch\n";
        let _ = parser.parse(&event(patch), &mut ctx);

        assert!(!ctx.file_cache.contains_key(&abs_path));
    }
}
