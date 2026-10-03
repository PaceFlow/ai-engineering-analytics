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

## Backend integration and releases

Paceflow stores authentication, schedule files, and logs separately under `.paceflow`. Its sync commands read shared event streams and record upload cursors in the shared database. Its hooks and schedules continue invoking `paceflow`.

One version tag releases both crates. The workflow tests Linux, Windows, and macOS, validates the packaged sources, builds both binary archives, publishes VCA before Paceflow, and creates the GitHub release after publication succeeds.
