# Data Model Migration Proposal

## Goal
Replace the current Rust-centric metric calculation flow with a data model that separates:

- `metadata_*` tables for enrichment context
- `event_*` tables for analytic occurrences at explicit grain
- `view_*` SQL views for metric outputs

Rust should ingest raw data, attach context, and materialize derived events. SQL should aggregate those events into descriptive metrics.

## Why Change
Today, metric logic is spread across Rust code that mixes:

- source parsing
- context derivation
- git inspection
- attribution logic
- aggregation
- report formatting

This makes metrics harder to test, harder to evolve, and harder to recompute at different scopes.

The replacement model is:

`raw sources -> metadata tables -> event tables -> SQL metric views`

## Design Principles
### 1. Every table has one declared grain
Do not mix session-grain, commit-grain, and task-session-grain rows in the same table.

### 2. Metadata is not metric input
`metadata_*` tables describe entities and reusable context. They are joined onto events but should not usually be aggregated directly.

### 3. Events are metric input
`event_*` tables represent one analytic occurrence each and should already contain the dimensions needed for grouping.

### 4. SQL owns ratios and rollups
Rust produces rows. SQL computes counts, sums, averages, rates, and status thresholds.

## Proposed Schema
### Metadata tables
These are the main enrichment tables.

- `metadata_repositories`
  - one row per repo root
- `metadata_sessions`
  - one row per AI session
- `metadata_files`
  - one row per canonical file path within a repo
- `metadata_commits`
  - one row per git commit
- `metadata_commit_file_changes`
  - one row per `(commit, file)`
- `metadata_tasks`
  - one row per recognized task key
- `metadata_branches`
  - one row per observed branch

### Event tables
These are the main analytic inputs.

- `event_session_messages`
  - grain: one row per ingested message
- `event_session_code_changes`
  - grain: one row per accepted or extracted file write operation
- `event_session_quality`
  - grain: one row per session
  - includes user turn count, repeated-debug-loop flag, mid-session-error-paste flag
- `event_commit_attributions`
  - grain: one row per `(commit, provider, session)`
  - includes matched lines and attribution shares
- `event_commit_outcomes`
  - grain: one row per commit
  - includes heavy-AI flag, merged-to-mainline flag, reverted-later flag
- `event_commit_churn`
  - grain: one row per commit
  - includes AI-added lines reaching mainline and churned lines within the churn window
- `event_task_commits`
  - grain: one row per `(task, commit)`
  - includes branch name, fallback flag, confidence
- `event_task_sessions`
  - grain: one row per `(task, session)`
  - includes session weight and commit-within-window flag

### SQL views
Views expose metric-ready outputs with descriptive names.

- `view_repository_commit_quality`
- `view_global_session_quality`
- `view_task_execution_quality`
- `view_repository_churn_summary`
- `view_session_productivity`

## Mapping From Current Tables
Current tables can be migrated roughly as follows:

- `events` -> split into `metadata_sessions`, `event_session_messages`, `event_session_code_changes`
- `change_sessions` -> merge into `metadata_sessions` or keep as provider-source metadata
- `change_ops` -> `event_session_code_changes`
- `git_commits` -> `metadata_commits`
- `git_commit_file_diffs` -> `metadata_commit_file_changes`
- `commit_ai_session_attributions` -> `event_commit_attributions`
- `commit_task_attributions` -> `event_task_commits`
- derived Rust outputs from `metrics/quality.rs` and `task_stats.rs` -> replace with `event_*` builders plus `view_*`

## Implementation Plan
### Phase 1: Introduce the new schema
Add the new `metadata_*`, `event_*`, and `view_*` schema alongside the current one. Do not remove old tables yet.

### Phase 2: Split existing ingestion outputs by grain
Refactor ingest so provider parsing still writes raw events, but then materialization jobs populate:

- `metadata_sessions`
- `event_session_messages`
- `event_session_code_changes`

### Phase 3: Move commit association outputs into event tables
Keep the existing commit-matching logic, but change persistence targets to:

- `metadata_commits`
- `metadata_commit_file_changes`
- `event_commit_attributions`
- `event_task_commits`

### Phase 4: Materialize session and commit outcome events
Move logic that currently lives in Rust metric code into event builders:

- session behavior builder -> `event_session_quality`
- commit outcome builder -> `event_commit_outcomes`
- churn builder -> `event_commit_churn`
- task-session linker -> `event_task_sessions`

At this stage, Rust still computes expensive booleans such as “merged to mainline” or “reverted later”, but it stores them as event fields rather than final metrics.

### Phase 5: Replace Rust aggregations with SQL views
Rewrite reporting commands to query SQL views instead of computing metrics in Rust.

Examples:

- `stats` reads from `view_global_session_quality`, `view_repository_commit_quality`, and `view_session_productivity`
- `task-stats` reads from `view_task_execution_quality`

### Phase 6: Remove old metric code
After parity is verified, retire:

- direct aggregation logic in [`src/metrics/quality.rs`](/home/tadas/Work/paceflow/vibe-coding-analytics/src/metrics/quality.rs)
- task metric aggregation logic in [`src/commands/task_stats.rs`](/home/tadas/Work/paceflow/vibe-coding-analytics/src/commands/task_stats.rs)

Keep only:

- ingestion
- enrichment
- materialization
- rendering

## Concrete Responsibility Split
### Rust responsibilities
- parse provider data
- inspect git history
- resolve repo, file, branch, and task context
- compute expensive derived flags and windows once
- write `metadata_*` and `event_*` rows

### SQL responsibilities
- counts
- sums
- averages
- rates
- filtered rollups
- grouping by repo, task, branch, provider, and time
- optional grading/status logic

## Example View Shapes
### `view_global_session_quality`
Built from `event_session_quality`.

- total sessions
- average user prompts per session
- repeated-debug-loop session rate
- mid-session-error-paste session rate

### `view_repository_commit_quality`
Built from `event_commit_outcomes` and `event_commit_churn`.

- heavy-AI commit count
- heavy-AI reverted-later rate
- heavy-AI merged-to-mainline rate
- churn rate for AI-added code after merge

### `view_task_execution_quality`
Built from `event_task_sessions` and `event_task_commits`.

- average user prompts for task-linked sessions
- repeated-debug-loop rate for task-linked sessions
- mid-session-error-paste rate for task-linked sessions
- session-with-commit-within-window rate
- commit count per task

## Migration Risks
- Reusing the current `events` table as both metadata and event source will keep the model muddy unless it is clearly split.
- Some current report outputs are not metrics and should stay outside views.

Examples:

- dominant branch selection
- `vs staging` diff output
- display formatting

- Backfill logic must preserve row grain exactly, or SQL views will overcount.

## Recommendation
Implement the new model incrementally, with old and new outputs running in parallel until the SQL views match today’s reports.

The target architecture should be:

`provider and git inputs -> metadata_* tables -> event_* tables -> view_* metric outputs -> CLI rendering`

This gives a clean separation between enrichment data and analytic events, keeps each table at one grain, and moves metric logic into SQL where it is easier to inspect, test, and extend.
