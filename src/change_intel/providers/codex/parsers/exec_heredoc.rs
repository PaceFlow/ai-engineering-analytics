use serde_json::Value;

use crate::change_intel::line_hash::{diff_with_hashes, hashes_for_text, line_count};
use crate::change_intel::path_resolver::{detect_repo_root, resolve_path, to_rel_path};
use crate::change_intel::session_context::SessionContext;
use crate::change_intel::types::{
    ChangeOpCandidate, LineSide, ParseError, ParseOutcome, PatternParser, ToolCallEvent, WriteMode,
};

#[derive(Debug, Clone)]
struct HeredocSegment {
    write_mode: WriteMode,
    raw_path: String,
    body: String,
}

#[derive(Debug, Clone)]
struct ExecArgs {
    cmd: String,
    workdir: Option<String>,
}

pub struct ExecHeredocWriteParser;

impl ExecHeredocWriteParser {
    pub const PARSER_NAME: &'static str = "exec_heredoc_v1";

    fn parse_exec_args(input_json: &str) -> Option<ExecArgs> {
        let raw: Value = serde_json::from_str(input_json).ok()?;
        let args_obj = match raw {
            Value::Object(map) => Value::Object(map),
            Value::String(inner) => serde_json::from_str::<Value>(&inner).ok()?,
            _ => return None,
        };

        let cmd = args_obj.get("cmd")?.as_str()?.to_string();
        let workdir = args_obj
            .get("workdir")
            .and_then(|v| v.as_str())
            .map(ToOwned::to_owned);

        Some(ExecArgs { cmd, workdir })
    }

    fn parse_output_text(output_json: &str) -> Option<String> {
        let parsed: Value = serde_json::from_str(output_json).ok()?;
        let output = parsed.get("output")?.as_str()?;

        if let Some(pos) = output.find("\nOutput:\n") {
            return Some(output[pos + "\nOutput:\n".len()..].to_string());
        }
        if let Some(pos) = output.find("Output:\n") {
            return Some(output[pos + "Output:\n".len()..].to_string());
        }

        Some(output.to_string())
    }

    fn parse_plain_cat_read(cmd: &str) -> Option<String> {
        let trimmed = cmd.trim();
        if trimmed.contains('\n') {
            return None;
        }
        if trimmed.contains('|')
            || trimmed.contains(';')
            || trimmed.contains("&&")
            || trimmed.contains("||")
        {
            return None;
        }
        if !trimmed.starts_with("cat ") {
            return None;
        }

        let rest = trimmed[4..].trim();
        if rest.contains('>') || rest.contains('<') || rest.is_empty() {
            return None;
        }

        if (rest.starts_with('\'') && rest.ends_with('\''))
            || (rest.starts_with('"') && rest.ends_with('"'))
        {
            return Some(rest[1..rest.len() - 1].to_string());
        }

        if rest.contains(char::is_whitespace) {
            return None;
        }

        Some(rest.to_string())
    }

    fn is_boundary(ch: char) -> bool {
        ch.is_whitespace() || ch == '&' || ch == '|' || ch == ';'
    }

    fn extract_heredoc_segments(cmd: &str) -> Result<Vec<HeredocSegment>, String> {
        let mut segments = Vec::new();
        let mut cursor = 0usize;

        while cursor < cmd.len() {
            let remainder = &cmd[cursor..];
            let Some(found) = remainder.find("cat ") else {
                break;
            };
            let start = cursor + found;

            if start > 0 {
                let prev = cmd.as_bytes()[start - 1] as char;
                if !Self::is_boundary(prev) {
                    cursor = start + 4;
                    continue;
                }
            }

            let mut i = start + 3;
            let bytes = cmd.as_bytes();
            while i < cmd.len() && (bytes[i] as char).is_whitespace() {
                i += 1;
            }

            let (write_mode, mut j) = if cmd[i..].starts_with(">>") {
                (WriteMode::Append, i + 2)
            } else if cmd[i..].starts_with('>') {
                (WriteMode::Overwrite, i + 1)
            } else {
                cursor = start + 4;
                continue;
            };

            while j < cmd.len() && (bytes[j] as char).is_whitespace() {
                j += 1;
            }
            if j >= cmd.len() {
                break;
            }

            let (raw_path, mut k) = if bytes[j] as char == '\'' || bytes[j] as char == '"' {
                let quote = bytes[j] as char;
                let mut end = j + 1;
                while end < cmd.len() && bytes[end] as char != quote {
                    end += 1;
                }
                if end >= cmd.len() {
                    return Err("unterminated quoted path in heredoc command".to_string());
                }
                (cmd[j + 1..end].to_string(), end + 1)
            } else {
                let mut end = j;
                while end < cmd.len() {
                    let ch = bytes[end] as char;
                    if ch.is_whitespace() || ch == '<' {
                        break;
                    }
                    end += 1;
                }
                if end == j {
                    return Err("missing file path in heredoc command".to_string());
                }
                (cmd[j..end].to_string(), end)
            };

            while k < cmd.len() && (bytes[k] as char).is_whitespace() {
                k += 1;
            }

            if k + 2 > cmd.len() || &cmd[k..k + 2] != "<<" {
                cursor = start + 4;
                continue;
            }
            k += 2;

            while k < cmd.len() && (bytes[k] as char).is_whitespace() {
                k += 1;
            }
            if k >= cmd.len() {
                return Err("missing heredoc marker".to_string());
            }

            let (marker, marker_end) = if bytes[k] as char == '\'' || bytes[k] as char == '"' {
                let quote = bytes[k] as char;
                let mut end = k + 1;
                while end < cmd.len() && bytes[end] as char != quote {
                    end += 1;
                }
                if end >= cmd.len() {
                    return Err("unterminated quoted heredoc marker".to_string());
                }
                (cmd[k + 1..end].to_string(), end + 1)
            } else {
                let mut end = k;
                while end < cmd.len() && !(bytes[end] as char).is_whitespace() {
                    end += 1;
                }
                (cmd[k..end].to_string(), end)
            };

            let after_header = &cmd[marker_end..];
            let Some(nl_rel) = after_header.find('\n') else {
                return Err("heredoc header missing newline before body".to_string());
            };
            let body_start = marker_end + nl_rel + 1;

            let mut search_pos = body_start;
            let mut body_end = None;
            for line in cmd[body_start..].split_inclusive('\n') {
                let trimmed = line.trim_end_matches('\n').trim_end_matches('\r');
                if trimmed == marker {
                    body_end = Some(search_pos);
                    search_pos += line.len();
                    break;
                }
                search_pos += line.len();
            }

            let Some(body_end) = body_end else {
                return Err(format!("heredoc terminator '{}' not found", marker));
            };

            let body = cmd[body_start..body_end].to_string();
            segments.push(HeredocSegment {
                write_mode,
                raw_path,
                body,
            });

            cursor = search_pos;
        }

        Ok(segments)
    }
}

impl PatternParser for ExecHeredocWriteParser {
    fn name(&self) -> &'static str {
        Self::PARSER_NAME
    }

    fn parse(&self, event: &ToolCallEvent, ctx: &mut SessionContext) -> ParseOutcome {
        let mut out = ParseOutcome::default();

        if event.tool_name != "exec_command" {
            return out;
        }

        let Some(args) = Self::parse_exec_args(&event.input_json) else {
            out.errors.push(ParseError {
                provider: event.provider.clone(),
                session_id: event.session_id.clone(),
                source_file: event.source_file.clone(),
                call_id: event.call_id.clone(),
                timestamp: event.timestamp.clone(),
                parser_name: Self::PARSER_NAME.to_string(),
                error: "failed to parse exec_command arguments JSON".to_string(),
            });
            return out;
        };

        if let Some(read_path) = Self::parse_plain_cat_read(&args.cmd)
            && let Some(output_json) = &event.output_json
            && let Some(content) = Self::parse_output_text(output_json)
        {
            let abs_path = resolve_path(
                &read_path,
                args.workdir.as_deref(),
                ctx.session_cwd.as_deref(),
            );
            ctx.file_cache
                .insert(abs_path.to_string_lossy().to_string(), content);
        }

        let segments = match Self::extract_heredoc_segments(&args.cmd) {
            Ok(segments) => segments,
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

        for (idx, segment) in segments.into_iter().enumerate() {
            let abs_path = resolve_path(
                &segment.raw_path,
                args.workdir.as_deref(),
                ctx.session_cwd.as_deref(),
            );
            let abs_path_s = abs_path.to_string_lossy().to_string();
            let repo_root = detect_repo_root(&abs_path);
            let rel_path = to_rel_path(repo_root.as_deref(), &abs_path);

            let before = ctx.file_cache.get(&abs_path_s).cloned();

            let (before_known, added_lines, removed_lines, line_hashes) =
                match (segment.write_mode, before.as_deref()) {
                    (WriteMode::Append, _) => {
                        let hashes = hashes_for_text(&segment.body, LineSide::Added);
                        (before.is_some(), line_count(&segment.body), 0, hashes)
                    }
                    (WriteMode::Overwrite, Some(before_text)) => {
                        let diff = diff_with_hashes(before_text, &segment.body);
                        (true, diff.added_lines, diff.removed_lines, diff.line_hashes)
                    }
                    (WriteMode::Overwrite, None) => {
                        let hashes = hashes_for_text(&segment.body, LineSide::Added);
                        (false, line_count(&segment.body), 0, hashes)
                    }
                    (WriteMode::Patch, _) => {
                        continue;
                    }
                };

            let new_cache_value = match segment.write_mode {
                WriteMode::Overwrite => segment.body.clone(),
                WriteMode::Append => {
                    if let Some(existing) = before {
                        format!("{}{}", existing, segment.body)
                    } else {
                        segment.body.clone()
                    }
                }
                WriteMode::Patch => continue,
            };
            ctx.file_cache.insert(abs_path_s.clone(), new_cache_value);

            out.ops.push(ChangeOpCandidate {
                provider: event.provider.clone(),
                session_id: event.session_id.clone(),
                source_file: event.source_file.clone(),
                call_id: event.call_id.clone(),
                op_index: idx as i32,
                timestamp: event.timestamp.clone(),
                repo_root: repo_root.map(|p| p.to_string_lossy().to_string()),
                abs_path: abs_path_s,
                rel_path,
                write_mode: segment.write_mode,
                before_known,
                added_lines,
                removed_lines,
                parser_name: Self::PARSER_NAME.to_string(),
                line_hashes,
            });
        }

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::PathBuf;

    fn mk_event(cmd: &str, output: Option<&str>) -> ToolCallEvent {
        ToolCallEvent {
            provider: "codex".to_string(),
            session_id: "s1".to_string(),
            source_file: "/tmp/f.jsonl".to_string(),
            timestamp: Some("2026-03-03T10:00:00Z".to_string()),
            call_id: "call_1".to_string(),
            tool_name: "exec_command".to_string(),
            input_json: json!({ "cmd": cmd, "workdir": "/tmp/repo" }).to_string(),
            output_json: output.map(ToOwned::to_owned),
        }
    }

    fn expected_abs(path: &str) -> PathBuf {
        resolve_path(path, Some("/tmp/repo"), Some("/tmp/repo"))
    }

    #[test]
    fn parses_plain_overwrite_heredoc() {
        let parser = ExecHeredocWriteParser;
        let mut ctx = SessionContext::new(Some("/tmp/repo".to_string()));

        let event = mk_event("cat > src/a.txt <<EOF\nhello\nworld\nEOF", None);
        let outcome = parser.parse(&event, &mut ctx);
        assert!(outcome.errors.is_empty());
        assert_eq!(outcome.ops.len(), 1);
        let op = &outcome.ops[0];
        assert_eq!(op.write_mode, WriteMode::Overwrite);
        assert!(!op.before_known);
        assert_eq!(op.added_lines, 2);
        assert_eq!(op.removed_lines, 0);
        assert_eq!(PathBuf::from(&op.abs_path), expected_abs("src/a.txt"));
    }

    #[test]
    fn parses_and_prefix_with_quoted_marker() {
        let parser = ExecHeredocWriteParser;
        let mut ctx = SessionContext::new(Some("/tmp/repo".to_string()));

        let event = mk_event(
            "mkdir -p src && cat > src/index.ts <<'EOF'\nconst x = 1;\nEOF",
            None,
        );
        let outcome = parser.parse(&event, &mut ctx);
        assert!(outcome.errors.is_empty());
        assert_eq!(outcome.ops.len(), 1);
        assert_eq!(outcome.ops[0].added_lines, 1);
        assert_eq!(
            PathBuf::from(&outcome.ops[0].abs_path),
            expected_abs("src/index.ts")
        );
    }

    #[test]
    fn append_mode_adds_only() {
        let parser = ExecHeredocWriteParser;
        let mut ctx = SessionContext::new(Some("/tmp/repo".to_string()));

        let event = mk_event("cat >> src/a.txt <<EOF\nnew line\nEOF", None);
        let outcome = parser.parse(&event, &mut ctx);
        assert!(outcome.errors.is_empty());
        assert_eq!(outcome.ops.len(), 1);
        let op = &outcome.ops[0];
        assert_eq!(op.write_mode, WriteMode::Append);
        assert_eq!(op.added_lines, 1);
        assert_eq!(op.removed_lines, 0);
    }

    #[test]
    fn overwrite_with_known_before_computes_diff() {
        let parser = ExecHeredocWriteParser;
        let mut ctx = SessionContext::new(Some("/tmp/repo".to_string()));

        let read_event = ToolCallEvent {
            provider: "codex".to_string(),
            session_id: "s1".to_string(),
            source_file: "/tmp/f.jsonl".to_string(),
            timestamp: Some("2026-03-03T09:59:00Z".to_string()),
            call_id: "call_read".to_string(),
            tool_name: "exec_command".to_string(),
            input_json: json!({ "cmd": "cat src/a.txt", "workdir": "/tmp/repo" }).to_string(),
            output_json: Some(
                json!({
                    "output": "Chunk ID: 1\nOutput:\nold\nline\n"
                })
                .to_string(),
            ),
        };
        let _ = parser.parse(&read_event, &mut ctx);

        let write_event = mk_event("cat > src/a.txt <<EOF\nold\nnew\nEOF", None);
        let outcome = parser.parse(&write_event, &mut ctx);
        assert!(outcome.errors.is_empty());
        assert_eq!(outcome.ops.len(), 1);
        let op = &outcome.ops[0];
        assert!(op.before_known);
        assert_eq!(op.added_lines, 1);
        assert_eq!(op.removed_lines, 1);
    }

    #[test]
    fn malformed_unclosed_heredoc_records_error() {
        let parser = ExecHeredocWriteParser;
        let mut ctx = SessionContext::new(Some("/tmp/repo".to_string()));

        let event = mk_event("cat > src/a.txt <<EOF\nhello\n", None);
        let outcome = parser.parse(&event, &mut ctx);
        assert!(outcome.ops.is_empty());
        assert_eq!(outcome.errors.len(), 1);
    }
}
