# Architecture

## Purpose
`vibe-coding-analytics` is a Rust CLI that turns local AI session history plus git history into a small set of productivity and quality metrics. The program has three major phases:

1. Ingest provider data into a normalized SQLite database.
2. Associate AI-generated code changes with real git commits.
3. Compute repo-level, task-level, and session-level numbers from those stored associations.

The entry point is [`src/main.rs`](/home/tadas/Work/paceflow/vibe-coding-analytics/src/main.rs). It parses CLI arguments and dispatches to one of four command handlers:

- `ingest`
- `associate-commits`
- `task-stats`
- `stats`

## High-Level Flow
### 1. Source data discovery
The ingest command ([`src/commands/ingest.rs`](/home/tadas/Work/paceflow/vibe-coding-analytics/src/commands/ingest.rs)) opens `~/.vibe/vca.db`, loads all registered providers from [`src/providers/mod.rs`](/home/tadas/Work/paceflow/vibe-coding-analytics/src/providers/mod.rs), and processes each provider in turn.

Providers are responsible for reading raw local history and converting it into a shared `Event` model from [`src/events.rs`](/home/tadas/Work/paceflow/vibe-coding-analytics/src/events.rs):

- `ChatStart`
- `Message`
- `ChangesAccepted`

That is the first normalization layer. It captures session boundaries, prompt/response text, and accepted line changes without exposing provider-specific storage formats to the rest of the program.

### 2. Event storage
[`src/db/mod.rs`](/home/tadas/Work/paceflow/vibe-coding-analytics/src/db/mod.rs) creates the main `events` table and inserts provider events into it. While inserting, it enriches rows with:

- `project_path`
- `repo_root`
- `rel_path`
- `file_name`
- derived timestamps

This is the base dataset used for session-level reporting and for some quality signals.

### 3. Code-change extraction
After a provider finishes writing `events`, `ingest` immediately calls [`change_intel::pipeline::ingest_provider_code_changes`](/home/tadas/Work/paceflow/vibe-coding-analytics/src/change_intel/pipeline.rs). This is the second normalization layer.

`change_intel` looks at provider-specific tool-call logs and extracts concrete file write operations. For Codex, it scans session JSONL files and parses successful `exec_command` and `apply_patch` calls. For Cursor, it uses provider-specific change extraction code.

Each extracted code operation becomes a `change_ops` row, plus hashed line-content fingerprints in `change_op_line_hashes`. These hashes are the bridge between AI session edits and later git commits.

## Core Data Model
The main tables are defined across [`src/db/mod.rs`](/home/tadas/Work/paceflow/vibe-coding-analytics/src/db/mod.rs) and [`src/change_intel/schema.rs`](/home/tadas/Work/paceflow/vibe-coding-analytics/src/change_intel/schema.rs).

### `events`
Normalized session activity. One row per chat start, message, or accepted edit. This powers session stats and session quality signals.

### `change_sessions`
One row per provider session discovered by `change_intel`. Stores source file, cwd, and timing metadata for code-change extraction.

### `change_ops`
One row per extracted file write operation. Key fields are provider, session, file path, timestamp, added/removed line counts, and parser metadata.

### `git_commits`
One row per scanned git commit, including commit timestamp and total added/removed lines.

### `commit_ai_attributions`
One row per commit summarizing how much of that commit matched previously extracted AI code changes. Important outputs are:

- `matched_total_lines`
- `ai_share`
- `heavy_ai`

### `commit_ai_session_attributions`
Breaks a commit’s AI-attributed lines down by `(provider, session_id)`. This is what lets later reporting connect a task or commit back to specific sessions.

### `commit_task_attributions`
Assigns each commit to a task key and branch context. `task-stats` uses this to aggregate commits and sessions by task.

## Commit Association Flow
[`src/commands/associate_commits.rs`](/home/tadas/Work/paceflow/vibe-coding-analytics/src/commands/associate_commits.rs) calls [`change_intel::commit_assoc::run`](/home/tadas/Work/paceflow/vibe-coding-analytics/src/change_intel/commit_assoc/mod.rs).

For each repo seen in `change_ops`, the program:

1. Validates the repo and picks a commit scan range.
2. Reads commit diffs from git.
3. Stores commit metadata and per-file diff data.
4. Hashes commit lines in the same format used for `change_ops`.
5. Matches commit line hashes against prior AI change hashes.
6. Writes commit-level, provider-level, and session-level attribution rows.
7. Assigns the commit to a task/branch for later task reporting.

The important idea is simple: if line-content hashes extracted from AI tool calls later appear in a git commit, that commit gets AI attribution.

## How Numbers Are Produced
### `vca stats`
[`src/commands/stats.rs`](/home/tadas/Work/paceflow/vibe-coding-analytics/src/commands/stats.rs) combines two outputs:

- `db::query_stats()`: session totals such as user words, accepted LOC, added lines, and removed lines.
- `metrics::quality::compute_quality_metrics()`: repo-level quality metrics built from heavy AI commits.

`compute_quality_metrics` in [`src/metrics/quality.rs`](/home/tadas/Work/paceflow/vibe-coding-analytics/src/metrics/quality.rs) derives:

- `S4`: debug-loop rate from session conversations in `events`
- `L4`: revert rate for heavy AI commits
- `C2`: merge-through rate of heavy AI commits to mainline
- `L1`: 14-day churn rate after merge

### `vca task-stats`
[`src/commands/task_stats.rs`](/home/tadas/Work/paceflow/vibe-coding-analytics/src/commands/task_stats.rs) starts from `commit_task_attributions`, joins commit/session attribution rows, and then combines:

- commit counts per task
- weighted session counts per task
- session quality signals from `events`
- git diff vs staging for the dominant task branch

The resulting task rows answer: which sessions contributed to a task, how many commits landed, how noisy the sessions were, and how large the branch diff is.

## Mental Model
If you want one sentence for the architecture, it is:

`provider history -> normalized events and code ops -> git commit matching -> attribution tables -> quality and productivity metrics`
