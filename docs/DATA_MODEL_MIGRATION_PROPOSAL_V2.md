# Data Model Migration Proposal V2

## Goal
Rework the current analytics pipeline into four explicit layers:

- `metadata_*` for enrichment context
- `fact_*` for atomic source-derived records
- `event_*` for enriched analytic events at reporting grain
- `view_*` for SQL metric outputs

The target architecture is:

`raw sources -> metadata_* + fact_* -> Rust-built event_* -> SQL-built view_* -> CLI rendering`

This replaces the current approach where Rust both derives facts and computes final metrics.

## Operating Model
There should be two clearly separated phases.

### Build phase: Rust
Rust owns ingestion, source interpretation, and event materialization.

Flow:

1. run `ingest`
2. read provider files, repositories, git history, and local source artifacts
3. normalize the raw inputs
4. write or update `metadata_*` tables
5. write or update `fact_*` tables
6. read `metadata_*` and `fact_*`
7. build or refresh `event_*`

Output of the build phase:

- reference context in `metadata_*`
- atomic source-derived records in `fact_*`
- enriched analytic rows in `event_*`

Rust still does not calculate final metric ratios or reporting rollups.

### Read phase: SQL query layer
SQL owns aggregation and final metric calculation.

Flow:

1. read from `event_*`
2. create or refresh `view_*`
3. read from `view_*` to derive metrics
4. display query results in the CLI

Output of the read phase:

- `view_*` as final reporting surfaces

## Why This Version Is Better
The earlier proposal split data into metadata and events. This version adds a separate `fact_*` layer, which is important because “source-derived truth” and “report-ready analytic event” are not the same thing.

The separation becomes:

- `metadata_*`: what things are
- `fact_*`: what was directly observed or derived from one source pass
- `event_*`: what happened in business/analytic terms after enrichment
- `view_*`: aggregated outputs for reporting

This gives:

- cleaner grain boundaries
- easier recomputation when enrichment rules change
- better remote-sync options
- better auditability

## Layer Definitions
### `metadata_*`
Reference and enrichment tables. These should be stable and reusable across many pipelines.

Examples:

- `metadata_repositories`
- `metadata_sessions`
- `metadata_files`
- `metadata_commits`
- `metadata_tasks`
- `metadata_branches`

These tables are not metric inputs by themselves. They provide keys, labels, classification, and context.

### `fact_*`
Atomic source-derived records. These are close to the original observed data and should preserve one clear grain.

Examples:

- `fact_session_message`
  - grain: one row per ingested message
- `fact_session_code_change`
  - grain: one row per accepted or extracted file write
- `fact_commit`
  - grain: one row per scanned commit
- `fact_commit_file_change`
  - grain: one row per `(commit, file)`
- `fact_commit_session_match`
  - grain: one row per `(commit, provider, session)` raw attribution result

These tables should avoid unnecessary denormalization. They are the durable analytic source of truth after parsing.

### `event_*`
Curated, enriched, denormalized event tables used for metrics and remote analytics.

Examples:

- `event_session_quality`
  - grain: one row per session
- `event_commit_outcome`
  - grain: one row per commit
- `event_commit_churn`
  - grain: one row per commit
- `event_task_commit`
  - grain: one row per `(task, commit)`
- `event_task_session`
  - grain: one row per `(task, session)`

These tables should already carry the business dimensions needed for reporting, such as repo, provider, task key, branch name, and timestamps.

### `view_*`
SQL views that compute counts, averages, rates, filters, and presentation-friendly rollups.

Examples:

- `view_global_session_quality`
- `view_repository_commit_quality`
- `view_repository_churn_summary`
- `view_task_execution_quality`
- `view_session_productivity`

## Design Principles
### 1. One table, one grain
Every table must declare its grain. Mixed-grain tables cause double counting and ambiguous SQL.

### 2. Metrics come from `event_*`, not from raw `fact_*`
`fact_*` tables preserve source truth. `event_*` tables encode the canonical enriched interpretation for analysis.

### 3. SQL owns aggregation
Rust prepares `metadata_*`, `fact_*`, and `event_*`. SQL computes rates and rollups and exposes final reporting views.

### 4. Expensive interpretation happens once
Rust should do source-specific work once and persist the result into `metadata_*`, `fact_*`, and `event_*`.

Examples:

- repeated debug loop
- mid-session error paste
- heavy AI commit
- merged to mainline
- reverted later
- commit within session window

## Proposed Schema
### Metadata tables
- `metadata_repositories`
  - grain: one row per repository
- `metadata_sessions`
  - grain: one row per session
- `metadata_files`
  - grain: one row per `(repository, relative_path)`
- `metadata_tasks`
  - grain: one row per task key
- `metadata_branches`
  - grain: one row per `(repository, branch_name)`

### Fact tables
- `fact_session_message`
  - grain: one row per message
  - keys: session id, provider, timestamp
- `fact_session_code_change`
  - grain: one row per code change operation
  - keys: session id, provider, file, timestamp, parser/call id
- `fact_commit`
  - grain: one row per commit
  - keys: repository, commit sha
- `fact_commit_file_change`
  - grain: one row per `(commit, file)`
- `fact_commit_session_match`
  - grain: one row per `(commit, provider, session)`
  - includes matched lines and attribution shares
- `fact_task_commit_assignment`
  - grain: one row per `(task, commit)`
  - includes source, fallback flag, confidence

### Event tables
- `event_session_quality`
  - grain: one row per session
  - fields:
    - repository
    - provider
    - session id
    - task key if known
    - session start/end
    - user turn count
    - repeated debug loop flag
    - mid-session error paste flag
- `event_session_productivity`
  - grain: one row per session
  - fields:
    - accepted lines added
    - accepted lines removed
    - accepted total changed lines
    - user word count
- `event_commit_outcome`
  - grain: one row per commit
  - fields:
    - repository
    - commit sha
    - heavy AI flag
    - merged to mainline flag
    - reverted later flag
    - total matched AI lines
    - commit total changed lines
- `event_commit_churn`
  - grain: one row per commit
  - fields:
    - repository
    - commit sha
    - AI-added lines that reached mainline
    - AI-added lines removed within churn window
- `event_task_commit`
  - grain: one row per `(task, commit)`
  - fields:
    - repository
    - task key
    - branch name
    - commit sha
    - fallback flag
    - confidence
- `event_task_session`
  - grain: one row per `(task, session)`
  - fields:
    - repository
    - task key
    - provider
    - session id
    - attribution weight
    - commit-within-window flag
    - session quality fields copied in for simpler SQL

## Mapping From Current Implementation
### Current `events`
Split into:

- `metadata_sessions`
- `fact_session_message`
- `fact_session_code_change`

### Current `change_sessions`
Move into session-related metadata or keep as provider-source metadata if needed for lineage.

### Current `change_ops`
Becomes `fact_session_code_change`.

### Current `git_commits`
Becomes `fact_commit` or `metadata_commits` depending on whether you want commit rows treated as source facts or reference entities. I recommend `fact_commit`, since commit ingestion is itself an observed dataset.

### Current `git_commit_file_diffs`
Becomes `fact_commit_file_change`.

### Current `commit_ai_session_attributions`
Becomes `fact_commit_session_match`.

### Current `commit_task_attributions`
Becomes `fact_task_commit_assignment`, then feeds `event_task_commit`.

### Current Rust metric code
Replace with Rust-built `event_*` materializers and SQL `view_*` objects:

- [`src/metrics/quality.rs`](/home/tadas/Work/paceflow/ai-engineering-analytics/src/metrics/quality.rs) -> mostly replaced by builders for `event_session_quality`, `event_commit_outcome`, `event_commit_churn`
- [`src/commands/task_stats.rs`](/home/tadas/Work/paceflow/ai-engineering-analytics/src/commands/task_stats.rs) -> mostly replaced by builder for `event_task_session` plus SQL view queries

## Migration Plan
### Phase 1: Add new schema side by side
Create the new `metadata_*`, `fact_*`, `event_*`, and `view_*` objects without removing current tables.

### Phase 2: Materialize fact tables from existing ingestion
Keep the current provider and git parsing flow, but persist outputs into `fact_*` tables with explicit grain.

### Phase 3: Build event materializers
Add Rust materializers that read `metadata_*` and `fact_*` and create or refresh:

- `event_session_quality`
- `event_session_productivity`
- `event_commit_outcome`
- `event_commit_churn`
- `event_task_commit`
- `event_task_session`

These materializers are where reporting-grain joins and denormalization happen.

### Phase 4: Add SQL metric views
Create descriptive `view_*` objects such as:

- `view_global_session_quality`
- `view_repository_commit_quality`
- `view_repository_churn_summary`
- `view_task_execution_quality`
- `view_session_productivity`

These views should compute counts, averages, and rates only from `event_*`.

### Phase 5: Switch CLI commands to read-only mode
Update `stats` and `task-stats` to query `view_*` outputs and format results. The SQL layer becomes the source of truth for metric values.

### Phase 6: Remove old Rust metric aggregation
After parity is verified, retire direct metric computation in Rust.

## Remote Sync / Data Lake Compatibility
This design is friendly to remote sync.

### What to sync by default
- all `event_*` tables
- key `metadata_*` tables needed for joins and labels

Recommended minimum metadata sync:

- `metadata_repositories`
- `metadata_sessions`
- `metadata_tasks`
- `metadata_branches`

Add `metadata_files` if remote file-level analysis matters.

### What to sync optionally
- `fact_*` tables for audit, replay, debugging, or model rebuilds

### What not to sync unless needed
- `view_*` tables
- local ingest cursors
- parse error logs
- transient caches

The main sync contract should be `event_*`, because those rows are already enriched and analytically meaningful.

## Example Responsibility Split
### Rust
- parse provider data
- scan git history
- attach source-level repo/task/branch/file/session context where needed
- write `metadata_*`
- write `fact_*`
- build or refresh `event_*`

### SQL
- compute averages
- compute rates
- group by repository, task, provider, branch, and time
- expose final reporting views

## Recommendation
Adopt this four-layer model.

It gives the clearest separation of concerns:

- `metadata_*` for context
- `fact_*` for source-derived truth
- `event_*` for canonical analytic events
- `view_*` for metric outputs

This is the best fit for future SQL-driven reporting and later remote data lake sync.
