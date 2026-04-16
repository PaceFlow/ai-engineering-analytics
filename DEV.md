# Development

Development-oriented workflows, profiling notes, and source-level commands live here so [README.md](README.md) can stay focused on end-user value and usage.

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
task profile-live PID=$(pgrep -n paceflow)
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

If you need to find the newest `paceflow` process first:

```bash
pgrep -n paceflow
```

### Environment Notes

- `samply` works well for Linux release profiling when you launch the program from the profiler.
- `perf` is the better fit for attach-to-process and live terminal inspection.
- On WSL2, `/usr/bin/perf` may be a wrapper that fails if kernel-matched packages are unavailable. The `profile-live` task works around that by invoking the real `perf` binary from `/usr/lib/linux-tools/...` when present.
- If `task install-live-profiler` succeeds but `perf` still fails, the remaining issue is usually a mismatch between the running WSL kernel and the Ubuntu packages available in your apt sources.
- Standard library frames may still have limited source-level detail unless you build a custom Rust toolchain or use `-Z build-std` on nightly.
