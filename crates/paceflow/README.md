# Paceflow

[![Website](https://img.shields.io/badge/website-paceflow.io-blue)](https://www.paceflow.io)
[![GitHub](https://img.shields.io/badge/github-PaceFlow-black)](https://github.com/PaceFlow)

`paceflow` is a local-first CLI for understanding whether coding-agent work is actually helping.

It reads local Claude Code, Codex, Cursor, and OpenCode history plus git metadata, then turns that evidence into four practical report views:

- `session`: were you getting leverage, or just steering and retrying?
- `delivery`: did AI-heavy work turn into commits that reached review or mainline?
- `quality`: did accepted AI-generated code hold up, or did it churn out later?
- `cost`: what did useful work cost in tokens, compute estimates, and accepted output?

The point is not to count prompts or accepted lines for their own sake. The point is to help individual engineers improve how they work with coding agents.

## Backend synchronization

Paceflow includes all VCA analytics commands and adds:

```bash
paceflow sync config
paceflow sync status
paceflow sync push --all-projects
paceflow sync schedule install
paceflow hooks install
```

Synchronization is optional until configured. Backend credentials, schedule files, logs, and hook identifiers retain their `~/.paceflow` locations and `PACEFLOW_SYNC_*` overrides. Schedules and hooks continue invoking `paceflow`.

Provider and GitHub overrides use `PACEFLOW_*` first, then the corresponding shared `VCA_*` variable. VCA itself uses only `VCA_*` provider and GitHub overrides. Both applications use the analytics-home precedence described below.

## What It Does

Most AI coding workflows feel productive in the moment. That does not mean they were useful.

`paceflow` helps spot patterns that are easy to miss:

- sessions that felt busy but produced little accepted output
- AI-heavy work that never made it to mainline
- accepted code that landed quickly and was removed soon after
- costly sessions that produced little accepted or mainline output

If those patterns show up repeatedly, you usually need tighter task slicing, better upfront constraints, earlier validation, or stricter review before accepting generated code.

## Quick Start

Run `paceflow` from a git repository you want to analyze:

```bash
paceflow ingest
paceflow session
paceflow delivery
paceflow quality
paceflow cost
```

The first ingest reads local assistant history, scans git metadata, and creates the local analytics database at `~/.vca/vca.db`.

Use this loop when you are trying the tool for the first time:

1. Run `paceflow ingest` after you have local Claude Code, Codex, Cursor, or OpenCode history on the machine.
2. Run `paceflow session` to see whether sessions are producing accepted code and commits.
3. Run `paceflow delivery` to see whether AI-heavy commits reached PRs or mainline.
4. Run `paceflow quality` to see whether AI-heavy code churned, drew follow-up fixes, or was reverted.
5. Run `paceflow cost` to compare API-equivalent cost against accepted and mainline output.
6. Re-run `paceflow ingest` whenever you have new sessions, commits, or GitHub PR metadata to refresh.

For GitHub PR reach and PR merge metrics, save a token and ingest again:

```bash
paceflow github token
paceflow ingest
```

`PACEFLOW_GITHUB_TOKEN` can be used for CI or one-off overrides.

## Automatic history discovery in 0.3.0

Both commands use the same analytics database at `~/.vca/vca.db`. On the first data-using command, either executable automatically discovers and copies previous history from these locations:

- `~/.paceflow/paceflow.db`
- `~/.aieng/aieng.db`
- `~/.aea/aea.db`
- `~/.vibe/vca.db` (original VCA)

Switching directly from an older Paceflow installation to VCA preserves your history without requiring you to run the new Paceflow first. Help and version commands do not migrate or create data.

An existing valid `~/.vca/vca.db` always wins. Otherwise, the most recent usable historical database is selected by recorded session, code-change, or commit activity. If no activity timestamp exists, file modification time is used. Ties follow the order above. The application prints its selection and retains every original; databases are not merged.

Migration includes committed SQLite WAL data, upgrades and validates a temporary copy, and installs it atomically under a cross-process lock. Interrupted migration is retryable. If historical files exist but none can be read or upgraded, the command explains the failure rather than starting with empty history. A saved GitHub token is copied from the selected installation only if no shared token exists. Backend credentials stay separate under `~/.paceflow`.

Analytics-home precedence is `VCA_HOME`, `PACEFLOW_HOME`, `AIENG_HOME`, `AEA_HOME`, then your normal home. Each override names a **parent home directory**, not a database file; discovery is limited to that home. For example, `VCA_HOME=/tmp/analytics` puts shared data in `/tmp/analytics/.vca/vca.db`.

Upgrade older executables before using them again: they continue writing to their old databases, and changes made there after migration are not automatically merged. Both 0.3.0 commands share ingestion, reports, caches, and GitHub credentials. `ingest --fresh` rebuilds this shared data and resets upload cursors; it preserves credentials and original source history.

### Recovery

Keep the original historical databases until you have verified the upgrade. If migration fails, fix permissions or restore a valid backup at the reported source path and retry. If a canonical database already exists, it is never overwritten automatically, including when it is corrupt. Stop running analytics commands and move the canonical database and any `-wal`/`-shm` sidecars aside to a backup location before restoring a complete backup or retrying discovery. Do not delete the historical originals to troubleshoot migration.

## Installation

Install a prebuilt binary with `cargo-binstall` (recommended if you have cargo):

```bash
cargo binstall paceflow
```

Or compile from crates.io:

```bash
cargo install --locked paceflow
```

Build and install from a local checkout:

```bash
git clone https://github.com/PaceFlow/ai-engineering-analytics.git
cd ai-engineering-analytics
cargo install --path crates/paceflow --locked --force
```

Prefer not to build from source? Download a prebuilt release from [GitHub Releases](https://github.com/PaceFlow/ai-engineering-analytics/releases).

Supported release targets:

| Platform | Asset |
| --- | --- |
| Windows x86_64 | `paceflow-x86_64-pc-windows-msvc.zip` |
| Linux x86_64 (glibc) | `paceflow-x86_64-unknown-linux-gnu.tar.gz` |
| macOS Apple Silicon | `paceflow-aarch64-apple-darwin.tar.gz` |

See [installation guide](https://github.com/PaceFlow/ai-engineering-analytics/blob/main/packaging/INSTALL.md) for platform-specific install commands, macOS Gatekeeper notes, and optional path overrides.

If team hooks or setup scripts will call `paceflow`, make sure the binary is available on `PATH`; see [installation guide](https://github.com/PaceFlow/ai-engineering-analytics/blob/main/packaging/INSTALL.md).

Requirements:

- `git` must be installed and available on `PATH`
- local Claude Code, Codex, Cursor, or OpenCode history must exist on the machine you run `paceflow` on
- GitHub PR sync requires `paceflow github token` or `PACEFLOW_GITHUB_TOKEN`

## Reports

By default, the four report commands compare outcomes by model.

```bash
paceflow session
paceflow delivery
paceflow quality
paceflow cost
```

Use `--overall` when you want one rolled-up summary row:

```bash
paceflow session --overall
paceflow delivery --overall
paceflow quality --overall
paceflow cost --overall
```

Use `--model <provider/name>` to keep the same report but narrow it to one model:

```bash
paceflow session --model codex/gpt-5.4
paceflow delivery --model codex/gpt-5.4
paceflow quality --model codex/gpt-5.4
paceflow cost --model codex/gpt-5.4
```

The reports answer four questions:

- `session`: which models or providers are efficient, noisy, or stuck in loops?
- `delivery`: which AI-heavy changes actually turn into shipped work?
- `quality`: which AI-heavy changes remain durable versus needing cleanup?
- `cost`: which sessions or groups produce useful output for the spend?

For metric definitions, status bands, grouped report behavior, and interpretation guidance, see [docs/USER_GUIDE.md](https://github.com/PaceFlow/ai-engineering-analytics/blob/main/docs/USER_GUIDE.md).

## Common Options

`session`, `delivery`, `quality`, and `cost` share the same filter interface:

- `--from YYYY-MM-DD --to YYYY-MM-DD` focuses a time window.
- `--repo /path/to/repo` analyzes a specific repository.
- `--all-projects` shows results across all tracked projects instead of defaulting to the current repo.
- `--provider codex`, `--provider cursor`, `--provider claude`, or `--provider opencode` filters by provider.
- `--group-by provider`, `--group-by model`, `--group-by branch`, or `--group-by task` changes the comparison dimension.
- `--branch <name>` or `--task ABC-123` narrows reports to a branch or ticket-like task key.
- `--limit <n>` controls the number of grouped rows shown.

Useful examples:

```bash
paceflow session --list-sessions
paceflow session --group-by provider
paceflow delivery --group-by task
paceflow delivery --group-by branch
paceflow quality --group-by provider
paceflow quality --group-by branch
paceflow cost --group-by provider
paceflow cost --group-by task
```

Use `paceflow <command> --help` to see command-specific options and metric notes.

## Troubleshooting

### Reports Say There Are No Rows

Run a fresh ingest from a repository that has local assistant history and git commits:

```bash
paceflow ingest
paceflow session --all-projects
```

If `session --all-projects` has rows but the plain report does not, you are probably running `paceflow` from a repo that has no matched sessions yet. Use `--all-projects`, run from the target repo, or pass `--repo /path/to/repo`.

### GitHub PR Metrics Are Empty Or Stale

GitHub PR reach and PR merge metrics need a token and a fresh ingest:

```bash
paceflow github token
paceflow ingest
paceflow delivery
```

The saved token lives locally under the `PACEFLOW_HOME` base directory. `PACEFLOW_GITHUB_TOKEN` can be used for CI or one-off overrides.

### Start Over With A Clean Database

Run `paceflow ingest --fresh` to rebuild shared derived analytics from local history and git. This resets backend upload cursors but preserves credentials, source history, and historical database backups. Both installed commands see the rebuilt data.

### Cursor Data Is Missing

Paceflow looks for Cursor state/history in the OS config directory under `Cursor/User`. If your Cursor data lives somewhere else, point Paceflow at it before ingesting:

```bash
export PACEFLOW_CURSOR_STATE_PATH=/path/to/state.vscdb
export PACEFLOW_CURSOR_HISTORY_PATH=/path/to/History
paceflow ingest
```

## More Documentation

- [User Guide](USER_GUIDE.md): report interpretation, metric definitions, status bands, and grouping behavior
- [Install Notes](https://github.com/PaceFlow/ai-engineering-analytics/blob/main/packaging/INSTALL.md): release assets, platform commands, and local data requirements
- [Development Notes](https://github.com/PaceFlow/ai-engineering-analytics/blob/main/DEV.md): source workflows, tests, profiling, and validation commands
- [Architecture](https://github.com/PaceFlow/ai-engineering-analytics/blob/main/docs/ARCHITECTURE.md): ingestion, storage, and analytics pipeline internals
