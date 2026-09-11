# Development

Development-oriented workflows, profiling notes, and source-level commands live here so [README.md](README.md) can stay focused on end-user value and usage.

## Provider Metric Regression Tests

`cargo test --test provider_metrics_e2e` creates small provider-shaped SQLite databases and Codex JSONL files, runs the actual `vca ingest` command, and asserts derived metrics. It never seeds analytics tables. Cases cover both OpenCode diff formats, exact changed-line/token/cost totals, 50% cost coverage with a zero-token priced session, first-change latency, atomic failure/retry, changed-source refresh, Cursor edits, Codex cumulative usage, and idempotency. A real Git fixture verifies one AI-heavy mainline commit and churn changing from 0/2 to 1/2 lines after a later fix, including after cache warmup.

Private source copies are kept outside version control under `.local-fixtures/2026-09-10`. Layout:

```text
sources/cursor/state.vscdb
sources/cursor/History/
sources/opencode/opencode.db
sources/opencode/storage/session_diff/
home/.codex/sessions/
```

To run the optional E2E checks against those exact copies (including a second ingestion):

```bash
VBA_LOCAL_FIXTURES="$PWD/.local-fixtures/2026-09-10" cargo test --release --test provider_metrics_e2e ingest_copied_local_databases_then_known_metrics_match -- --ignored --nocapture
```

Copy preparation is local test setup, not application functionality. To analyze an already prepared Cursor copy directly, configure its source paths and use a separate analytics home:

```bash
PACEFLOW_HOME="$PWD/.local-fixtures/cursor-analysis" \
PACEFLOW_CURSOR_STATE_PATH="$PWD/.local-fixtures/2026-09-10/sources/cursor/state.vscdb" \
PACEFLOW_CURSOR_HISTORY_PATH="$PWD/.local-fixtures/2026-09-10/sources/cursor/History" \
cargo run --release -- ingest --provider cursor
```

The application reads the configured source directly; it does not copy databases, maintain snapshot caches, or discover Windows profiles from WSL.

The local case expects 9 OpenCode sessions, 232 changed lines and 51,866,246 tokens; 141 Codex sessions and 1,758,526,143 tokens; and 482 Cursor sessions. These expectations describe the dated copies, not future live history. Git-dependent attribution can vary as the original repositories change, so those metrics use the small deterministic fixtures instead. Original databases/history are never modified. Copy active SQLite databases with their WAL (or use SQLite backup); copying only the main file may omit recent data.

## Run From Source

Use Cargo for local development:

```bash
cargo run -- ingest
cargo run -- session
cargo run -- delivery
cargo run -- quality
```

## Build And Validation

Use the standard Rust workflow:

```bash
cargo build
cargo test
cargo fmt
cargo clippy --all-targets --all-features
```

## Live GitHub Test

The live GitHub integration test is ignored by default. The fixture repo, commit, PR number, and negative commit case are hardcoded in `tests/github_live.rs`; only the token comes from the environment:

```bash
export PACEFLOW_GITHUB_TOKEN=github_pat_...

cargo test --test github_live -- --ignored --nocapture
```

What it verifies:

- commit to PR lookup against the live GitHub API
- persisted PR metadata and lookup status
- derived `event_commit_pr_outcome` flags used by PR reach and PR merge delivery metrics

## Profiling Setup

Release builds are configured for profiler-friendly output:

- [`Cargo.toml`](Cargo.toml) sets `[profile.release] debug = "line-tables-only"` so profilers can resolve source lines in this crate.
- [`.cargo/config.toml`](.cargo/config.toml) enables frame pointers and v0 symbol mangling for better stack traces and cleaner symbol names.

The repository also includes a [`Taskfile.yml`](Taskfile.yml) with helper tasks:

```bash
task build-release
task install-profiler
task profile -- ingest
task profile -- session
task install-live-profiler
task profile-live PID=$(pgrep -n vca)
```

### Browser-Based Profiling With Samply

Use `samply` when you want to profile a fresh run of the CLI and inspect it in Firefox Profiler:

```bash
task install-profiler
task profile -- ingest
```

### Live Terminal Profiling With Perf

Use `perf top` when you want to attach to an already-running process and watch samples live in the terminal:

```bash
task install-live-profiler
task profile-live PID=12345
```

This uses call stacks based on frame pointers:

```bash
perf top -p <pid> -g --call-graph fp
```

If you need to find the newest `vca` process first:

```bash
pgrep -n vca
```

### Environment Notes

- `samply` works well for Linux release profiling when you launch the program from the profiler.
- `perf` is the better fit for attach-to-process and live terminal inspection.
- On WSL2, `/usr/bin/perf` may be a wrapper that fails if kernel-matched packages are unavailable. The `profile-live` task works around that by invoking the real `perf` binary from `/usr/lib/linux-tools/...` when present.
- If `task install-live-profiler` succeeds but `perf` still fails, the remaining issue is usually a mismatch between the running WSL kernel and the Ubuntu packages available in your apt sources.
- Standard library frames may still have limited source-level detail unless you build a custom Rust toolchain or use `-Z build-std` on nightly.
