//! Deterministic classification of tool calls and validation commands.

use regex::Regex;
use std::sync::OnceLock;

/// Broad category of a tool call, used for context and activity breakdowns.
pub fn tool_category(tool_name: &str, target: Option<&str>) -> &'static str {
    let name = tool_name.to_ascii_lowercase();
    if name.starts_with("mcp__") || name.starts_with("mcp_") || name.contains("mcp:") {
        return "mcp";
    }
    match name.as_str() {
        "read" | "read_file" | "view" | "notebookread" | "cat" | "read_file_v2" => "read",
        "grep" | "glob" | "ls" | "list_dir" | "find" | "search" | "codebase_search"
        | "grep_search" | "file_search" | "list" | "toolsearch" => "search",
        "edit" | "multiedit" | "write" | "notebookedit" | "apply_patch" | "edit_file"
        | "edit_file_v2" | "write_file" | "write_file_v2" | "patch" | "str_replace_editor"
        | "delete_file" => "edit",
        "bash"
        | "exec_command"
        | "shell"
        | "run_terminal_cmd"
        | "run_terminal_command"
        | "local_shell"
        | "write_stdin"
        | "bashoutput"
        | "killshell" => {
            if target.and_then(classify_command).is_some() {
                "validate"
            } else {
                "exec"
            }
        }
        "webfetch" | "websearch" | "web_search" | "web_fetch" | "fetch" | "web_search_call" => {
            "web"
        }
        "task" | "agent" | "subagent" | "spawn_agent" => "agent",
        "todowrite" | "update_plan" | "exitplanmode" | "enterplanmode" | "todo_write"
        | "taskcreate" | "taskupdate" => "plan",
        _ => "other",
    }
}

/// Classify a shell command as a validation run.
///
/// Returns `test|build|typecheck|lint|run`, or `None` for other commands.
pub fn classify_command(command: &str) -> Option<&'static str> {
    static PATTERNS: OnceLock<Vec<(&'static str, Regex)>> = OnceLock::new();
    let patterns = PATTERNS.get_or_init(|| {
        let table: [(&str, &str); 5] = [
            (
                "test",
                r"(?x)\b(
                    cargo\s+(test|nextest)
                  | pytest | python3?\s+-m\s+(pytest|unittest)
                  | go\s+test
                  | (npm|pnpm|yarn|bun)\s+(run\s+)?test
                  | npx\s+(jest|vitest|playwright\s+test)
                  | jest | vitest
                  | mvn\s+(-\S+\s+)*test | gradlew?\s+test
                  | rspec | dotnet\s+test | phpunit | mix\s+test | swift\s+test
                  | deno\s+test | ctest | make\s+(test|check)
                )\b",
            ),
            (
                "typecheck",
                r"(?x)\b(
                    tsc\s+(.*\s)?--noEmit | mypy | pyright | (npm|pnpm|yarn)\s+(run\s+)?(typecheck|type-check|tsc)
                  | cargo\s+check
                )\b",
            ),
            (
                "lint",
                r"(?x)\b(
                    cargo\s+(clippy|fmt\s+(.*\s)?--check) | eslint | ruff | flake8 | pylint
                  | golangci-lint | prettier\s+(.*\s)?--check | (npm|pnpm|yarn)\s+(run\s+)?lint
                  | rubocop | shellcheck | black\s+(.*\s)?--check
                )\b",
            ),
            (
                "build",
                r"(?x)\b(
                    cargo\s+build | (npm|pnpm|yarn|bun)\s+(run\s+)?build | tsc\b | go\s+build
                  | make\b | cmake\s+--build | mvn\s+(-\S+\s+)*(package|compile|install)
                  | gradlew?\s+(build|assemble) | dotnet\s+build | swift\s+build | xcodebuild
                  | docker\s+build
                )",
            ),
            (
                "run",
                r"(?x)\b(
                    cargo\s+run | go\s+run | (npm|pnpm|yarn)\s+(run\s+)?(start|dev)
                  | curl\s+(-\S+\s+)*https?://(localhost|127\.0\.0\.1)
                )\b",
            ),
        ];
        table
            .into_iter()
            .map(|(kind, pattern)| (kind, Regex::new(pattern).expect("valid validation regex")))
            .collect()
    });

    // Only the first command of a chain decides, unless it is a `cd`.
    let normalized = command.trim();
    if normalized.is_empty() {
        return None;
    }
    let segments: Vec<&str> = normalized
        .split("&&")
        .flat_map(|segment| segment.split(';'))
        .map(str::trim)
        .filter(|segment| !segment.is_empty() && !segment.starts_with("cd "))
        .collect();
    for segment in segments {
        for (kind, regex) in patterns {
            if regex.is_match(segment) {
                return Some(kind);
            }
        }
    }
    None
}

/// Normalized first error line of tool output, for loop detection.
pub fn error_signature(text: &str) -> Option<String> {
    let line = crate::analytics::first_error_line(text)
        .or_else(|| text.lines().map(str::trim).find(|line| !line.is_empty()))?;
    let signature = crate::analytics::normalize_signature(line);
    (!signature.is_empty()).then_some(signature)
}

/// Directory key for a path target, relative to `repo_root` when possible,
/// truncated to `depth` components.
pub fn target_dir(target: &str, repo_root: Option<&str>, depth: usize) -> String {
    let mut path = target
        .trim()
        .trim_matches('"')
        .trim_matches('\'')
        .to_string();
    if let Some(root) = repo_root {
        let root = root.trim_end_matches('/');
        if let Some(stripped) = path.strip_prefix(root) {
            path = stripped.trim_start_matches('/').to_string();
        }
    }
    let parts: Vec<&str> = path.split('/').filter(|part| !part.is_empty()).collect();
    if parts.len() <= 1 {
        return if path.starts_with('/') {
            "/".to_string()
        } else {
            ".".to_string()
        };
    }
    let dir_parts = &parts[..parts.len() - 1];
    let take = dir_parts.len().min(depth);
    let prefix = if path.starts_with('/') { "/" } else { "" };
    format!("{prefix}{}/", dir_parts[..take].join("/"))
}

/// MCP server name from a tool name such as `mcp__github__get_file_contents`.
pub fn mcp_server(tool_name: &str) -> Option<String> {
    let rest = tool_name.strip_prefix("mcp__")?;
    rest.split("__").next().map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_validation_commands() {
        let cases = [
            ("cargo test --workspace", Some("test")),
            ("cd crates/vca && cargo test parses_", Some("test")),
            ("pytest -x tests/", Some("test")),
            ("npm run test -- --watch=false", Some("test")),
            ("pnpm test", Some("test")),
            ("go test ./...", Some("test")),
            ("npx tsc --noEmit", Some("typecheck")),
            ("cargo check -p vca", Some("typecheck")),
            ("cargo clippy --workspace --all-targets", Some("lint")),
            ("cargo fmt --check", Some("lint")),
            ("ruff check .", Some("lint")),
            ("cargo build --release", Some("build")),
            ("npm run build", Some("build")),
            ("cargo run -- ingest", Some("run")),
            ("ls -la", None),
            ("git status", None),
            ("cat src/main.rs", None),
        ];
        for (command, expected) in cases {
            assert_eq!(classify_command(command), expected, "{command}");
        }
    }

    #[test]
    fn categorizes_tools() {
        assert_eq!(tool_category("Read", None), "read");
        assert_eq!(tool_category("Bash", Some("cargo test")), "validate");
        assert_eq!(tool_category("Bash", Some("ls")), "exec");
        assert_eq!(tool_category("mcp__github__get_file", None), "mcp");
        assert_eq!(tool_category("Task", None), "agent");
        assert_eq!(tool_category("apply_patch", None), "edit");
    }

    #[test]
    fn target_dir_is_repo_relative_and_truncated() {
        assert_eq!(
            target_dir("/repo/crates/vca/src/db/mod.rs", Some("/repo"), 3),
            "crates/vca/src/"
        );
        assert_eq!(target_dir("/repo/README.md", Some("/repo"), 3), ".");
        assert_eq!(target_dir("docs/a.md", None, 3), "docs/");
    }

    #[test]
    fn extracts_mcp_server() {
        assert_eq!(
            mcp_server("mcp__github__get_file_contents").as_deref(),
            Some("github")
        );
        assert_eq!(mcp_server("Read"), None);
    }

    #[test]
    fn error_signature_normalizes_numbers_and_paths() {
        let a = error_signature("error[E0308]: mismatched types at src/a.rs:12").unwrap();
        let b = error_signature("error[E0308]: mismatched types at src/b.rs:40").unwrap();
        assert_eq!(a, b);
    }
}
