# Repository Guidelines

## Project Structure & Module Organization
The Cargo workspace has two members: `crates/vca` provides the shared analytics library and `vca` binary; `crates/paceflow` provides the `paceflow` binary and backend sync, schedules, and hooks. Shared command handlers, providers, analytics, and SQLite access live under `crates/vca/src/`. Shared regression fixtures live in `tests/fixtures/`. Build artifacts are written to `target/` and should not be committed.

## Build, Test, and Development Commands
Use Cargo for all local workflows:

- `cargo build --workspace` builds both binaries.
- `cargo run -- --help` shows CLI usage during development.
- `cargo run -- ingest` ingests local session data, associates commits, and materializes analytics events.
- `cargo run -- session` shows session KPIs and breakdowns.
- `cargo run -- delivery` shows delivery KPIs and breakdowns.
- `cargo run -- quality` shows quality KPIs and breakdowns.
- `cargo test --workspace --all-targets` runs both products’ unit and regression suites.
- `cargo fmt` applies standard Rust formatting.
- `cargo clippy --workspace --all-targets --all-features` checks for common Rust issues before review.

## Coding Style & Naming Conventions
Follow standard Rust style: 4-space indentation, `snake_case` for functions/modules/files, `PascalCase` for types and enums, and concise `SCREAMING_SNAKE_CASE` constants. Keep modules focused and prefer small helper functions over long command handlers. Use `rustfmt` for layout and derive-based `clap` definitions for CLI flags and subcommands.

## Testing Guidelines
Tests are a mix of inline unit tests under `#[cfg(test)]` blocks in the owning module and integration/regression coverage under `tests/`. Add focused tests next to the code you change, especially around parser behavior, ingestion idempotency, commit association logic, and report/query behavior. Use descriptive test names such as `parses_single_update_file` or `ingest_is_idempotent_with_cursor_skip`. Run `cargo test --workspace --all-targets` before submitting changes.

## Commit & Pull Request Guidelines
Recent history uses short, imperative commit subjects such as `add task stats and task to commit association` and `ingest code change by default`. Keep commits narrowly scoped and write subjects in that style. Pull requests should explain the user-visible or analytics-impacting change, note schema or command changes, link related issues, and include example CLI output when behavior changes.

## Data & Configuration Notes
This project analyzes local editor/session history and git metadata. Avoid committing local databases or generated reports, and document any new provider-specific assumptions in `README.md`.
