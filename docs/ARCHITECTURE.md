# Architecture

The workspace publishes two products from one analytics implementation. [VCA](../crates/vca) provides a Rust library and the `vca` executable. [Paceflow](../crates/paceflow) depends on that library and provides `paceflow`, adding backend authentication, upload, scheduling, and Git hooks.

## Commands and product context

Each executable has its own top-level Clap command. VCA owns the shared argument types and dispatch. An explicit `AppContext` carries command name, title, version, and environment prefix into shared handlers. Paceflow-specific arguments and handlers live in the Paceflow crate. VCA has no dependency on Paceflow and cannot execute its backend commands.

## Discovery and storage

Both executables resolve the same analytics home, preferring `VCA_HOME`, then historical home overrides, then the operating-system home. The database is `.vca/vca.db` beneath that home. Before opening it, the shared resolver validates canonical storage or migrates a recognized historical database. Migration uses SQLite backup to include committed WAL contents, upgrades an isolated copy, and retains originals. [Upgrade and recovery details](../README.md#automatic-history-discovery-in-030).

A `Database` owns its SQLite connection and a cross-process analytics lock. The connection closes before the lock releases. Reports, ingestion, backend upload bookkeeping, migration, and fresh rebuilds therefore coordinate across both commands. Help and version parsing happens before any data discovery.

## Ingestion and analytics

Providers normalize Claude Code, Codex, Cursor, and OpenCode history into `metadata_*` and `fact_session_*` tables. The change-intelligence pipeline parses accepted edits and stores line hashes. Commit association scans git history and matches session changes to commits and task branches. Optional GitHub requests enrich PR and follow-through evidence.

Analytics materializes normalized `event_*` tables and creates reporting views. Session, delivery, quality, cost, event export, and TUI commands query the same shared data. Repository, member, and device identity helpers remain in the shared library so event keys and Paceflow payloads stay compatible.

Schema initialization creates tables, upgrades missing columns, and then creates dependent indexes. Migrated databases are integrity-checked and their reporting views validated before replacing the canonical destination.

## Session store and insights

Providers build one normalized `SessionRecord` per session (`db/session_store.rs`): messages, per-call usage, tool calls with exit codes, events (compaction, clear, API errors), context injections and rate-limit snapshots. Records are stored by natural keys inside a transaction, and a per-source fingerprint (`session_source_state`) re-parses a session whenever its file changes. Sessions whose source disappears keep their facts. Schema version 2 adds these tables and columns additively; one-time data fixes are recorded in `schema_migrations`.

`insights/` rebuilds deterministic derived tables after each ingest:

- `structure.rs`: turns, tasks and attempts. It allocates every usage event to one turn and task, times tasks, and detects loops. Task boundaries come from `task_turn_assignment` and completion from `task_completion_mark` (overrides first, then classifier answers). Without either, each session is one task completed by commit or validation evidence.
- `context.rs`: the context ledger (API calls, context items, write/carry cost, cache rebuilds, instruction references).
- `quota.rs`: observed and estimated quota windows, with each observed rise allocated to the usage that preceded it.
- `relations.rs`: task relations where later AI work removed earlier AI lines, and same-branch follow-ups.

`explore/` holds the typed queries (usage, sessions, tasks, groups, quota, context, delivery ladder, rework, practices, compare). The CLI commands and the TUI use them, the TUI through a background loader thread.

`classify/` holds heuristic labels and the optional Jev classifier. The classifier consists of question definitions, state builders with redaction and size limits, a `DecisionClient` trait (OpenRouter implementation plus a scripted test client), and run orchestration with caching, budgets and overrides. Results live in `classification_result` and overrides in `classification_override`, and `view_label_effective` picks override > classifier > heuristic.

## Backend integration and releases

Paceflow stores authentication, schedule files, and logs separately under `.paceflow`. Its sync commands read shared event streams and record upload cursors in the shared database. Its hooks and schedules continue invoking `paceflow`.

One version tag releases both crates. The workflow tests Linux, Windows, and macOS, validates the packaged sources, builds both binary archives, publishes VCA before Paceflow, and creates the GitHub release after publication succeeds.
