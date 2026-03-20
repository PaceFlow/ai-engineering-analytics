# vibe-coding-analytics

CLI for measuring AI-assisted coding productivity from local session history + git.

## Install

```bash
cargo install --path . --force
```

## Profiling Setup

Release builds are configured for profiler-friendly output:

- [`Cargo.toml`](/home/tadas/Work/paceflow/vibe-coding-analytics/Cargo.toml) sets `[profile.release] debug = "line-tables-only"` so profilers can resolve source lines in this crate.
- [`.cargo/config.toml`](/home/tadas/Work/paceflow/vibe-coding-analytics/.cargo/config.toml) enables frame pointers and v0 symbol mangling for better stack traces and cleaner symbol names.

The repository also includes a [`Taskfile.yml`](/home/tadas/Work/paceflow/vibe-coding-analytics/Taskfile.yml) with helper tasks:

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

This records `./target/release/vca ...` and opens the result in the Firefox Profiler UI. `samply` is best when you can start the command from the profiler.

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

## Help

```bash
vca --help
vca ingest --help
vca session --help
vca change --help
vca lifecycle --help
vca event-stream --help
```

## Manager Test Flow

1. Ingest session and code-change events.

```bash
vca ingest
```

2. View session metrics and session rows.

```bash
vca session
vca session --list-sessions
```

3. View change and lifecycle quality metrics.

```bash
vca change
vca lifecycle
```

Optional grouped cuts:

```bash
vca session --weekly --group-by provider
vca change --group-by repo
vca lifecycle --group-by task --task ABC-123
```

Manual validation:

```bash
vca event-stream --stream session-base
vca event-stream --stream task-commit-base --task ABC-123
vca event-stream --limit 10
```

## Notes

- `session`, `change`, and `lifecycle` share the same filtering interface: `--weekly`, `--group-by`, `--from`, `--to`, `--repo`, `--provider`, `--task`, `--model`, and `--limit`.
- `event-stream` is a read-only NDJSON export of the reporting base views for manual metric validation. It supports `--category`, `--stream`, `--from`, `--to`, `--repo`, `--provider`, `--task`, `--model`, and `--limit`.
- Task-grouped rows only show ticket-style task keys (for example `ABC-123`) and exclude integration branches such as `main`, `staging`, `master`, and `develop`.
- `change --group-by task` includes `vs Staging`, derived from `git diff staging...<branch>` for non-integration branches.
