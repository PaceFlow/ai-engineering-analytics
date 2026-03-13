# Conversation Notes

## Topic
Designing a new analytics data model for `vibe-coding-analytics` that separates source ingestion, enrichment, analytic events, and metric calculation.

## Main Direction
We moved away from Rust computing final metrics directly and toward a layered model:

- `metadata_*` for reusable context and entity data
- `fact_*` for atomic source-derived records
- `event_*` for enriched analytic events at explicit grain
- `view_*` for SQL metric outputs

The agreed target architecture is:

`raw sources -> metadata_* + fact_* -> Rust-built event_* -> SQL-built view_* -> CLI rendering`

## Key Decisions
### Metrics should be calculated in SQL
Rust should ingest source data, attach context, and materialize event rows. SQL should compute:

- counts
- sums
- averages
- rates
- grouped rollups

This keeps the reporting layer transparent and easier to evolve.

### Keep metadata separate from metric inputs
We established a strict separation:

- metadata describes entities or context used for joins
- events represent analytic occurrences that should be aggregated

Rule of thumb:
If deleting the row removes evidence that something happened, it is an event. If it only removes descriptive context, it is metadata.

### Use explicit grain
We discussed why mixed-grain tables are dangerous. Session-grain, commit-grain, and task-session-grain rows should not live in one table because that causes:

- double counting
- ambiguous `count(*)`
- weak constraints
- brittle joins

Each table should have one declared grain.

### `fact_*` and `event_*` are different layers
This refinement was added later in the discussion.

- `fact_*` preserves source-derived truth with minimal interpretation
- `event_*` is the curated, denormalized analytics layer built from `metadata_*` and `fact_*`

This helps with:

- auditability
- recomputation when enrichment rules change
- remote sync
- clearer downstream SQL

### Build phase vs read phase
We clarified the intended runtime split:

Build phase in Rust:

1. run ingest
2. read provider files, repos, git history, and source artifacts
3. write or update `metadata_*`
4. write or update `fact_*`
5. build or refresh `event_*`

Read phase in SQL:

1. read from `event_*`
2. create or refresh `view_*`
3. read `view_*` for metrics
4. display results in the CLI

### Remote sync
We discussed future sync to a remote data lake.

Conclusion:

- `event_*` should be the main sync contract
- some `metadata_*` tables should also be synced for joins and labels
- `fact_*` is optional for replay, audit, and debugging
- `view_*` usually should not be synced

### Do not create a new event table for every metric
A new metric should usually be:

- a new `view_*`
- or a new column on an existing `event_*`

Only create a new `event_*` table when the new metric introduces a genuinely new analytic grain or business event.

### Do not keep all events in one table
We considered a single event table with a `type` column and rejected it for curated analytics because:

- event grains differ too much
- most columns would be sparse
- constraints would weaken
- downstream SQL would become error-prone

A single typed event table can make sense for raw source events, but not for the curated reporting layer in this project.

## Event Table Discussion
We initially listed:

- `event_session_quality`
- `event_session_productivity`
- `event_commit_outcome`
- `event_commit_churn`
- `event_task_commit`
- `event_task_session`

We then discussed that these may be too split and that a more compact base could be:

- `event_session`
- `event_commit`
- `event_task_commit`
- `event_task_session`

The deciding factor should be grain and lifecycle, not metric names.

## dbt / Modeling Guidance We Borrowed
We checked dbt modeling ideas and aligned with these principles:

- keep clear layers between raw/staging/intermediate/marts
- define one grain per table
- separate reusable entities/dimensions from metric inputs
- keep metric logic above prepared semantic inputs

That reinforced the chosen model.

## Documents Created During This Discussion
- [AGENTS.md](/home/tadas/Work/paceflow/vibe-coding-analytics/AGENTS.md)
- [ARCHITECTURE.md](/home/tadas/Work/paceflow/vibe-coding-analytics/docs/ARCHITECTURE.md)
- [METRIC_ENGINE_MIGRATION.md](/home/tadas/Work/paceflow/vibe-coding-analytics/docs/METRIC_ENGINE_MIGRATION.md)
- [DATA_MODEL_MIGRATION_PROPOSAL.md](/home/tadas/Work/paceflow/vibe-coding-analytics/docs/DATA_MODEL_MIGRATION_PROPOSAL.md)
- [DATA_MODEL_MIGRATION_PROPOSAL_V2.md](/home/tadas/Work/paceflow/vibe-coding-analytics/docs/DATA_MODEL_MIGRATION_PROPOSAL_V2.md)

## Current Recommended Direction
The current best proposal is the V2 data model with this shape:

- Rust builds `metadata_*`
- Rust builds `fact_*`
- Rust materializes `event_*`
- SQL defines `view_*`
- CLI reads `view_*`

And the main modeling rule is:

Use separate tables for separate grains; use SQL views for metrics; treat `event_*` as the portable analytics contract.
