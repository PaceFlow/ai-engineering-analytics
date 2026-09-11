# AI Engineering Analytics

`vba` is a local-first CLI for understanding whether coding-agent work is actually helping.

It reads local Claude Code, Codex, Cursor, and OpenCode history plus git metadata, then turns that evidence into four practical report views:

- `session`: were you getting leverage, or just steering and retrying?
- `delivery`: did AI-heavy work turn into commits that reached review or mainline?
- `quality`: did accepted AI-generated code hold up, or did it churn out later?
- `cost`: what did useful work cost in tokens, compute estimates, and accepted output?

The point is not to count prompts or accepted lines for their own sake. The point is to help individual engineers improve how they work with coding agents.

## What It Does

Most AI coding workflows feel productive in the moment. That does not mean they were useful.

`vba` helps spot patterns that are easy to miss:

- sessions that felt busy but produced little accepted output
- AI-heavy work that never made it to mainline
- accepted code that landed quickly and was removed soon after
- costly sessions that produced little accepted or mainline output

If those patterns show up repeatedly, you usually need tighter task slicing, better upfront constraints, earlier validation, or stricter review before accepting generated code.

## Quick Start

Run `vba` from a git repository you want to analyze:

```bash
vba ingest
vba session
vba delivery
vba quality
vba cost
```

The first ingest reads local assistant history, scans git metadata, and creates the local analytics database at `~/.paceflow/paceflow.db`.

To refresh just one source, use `vba ingest --provider cursor` (also `codex`, `opencode`, or `claude`). Provider ingestion failures return a nonzero exit status. `--fresh` rebuilds all providers and cannot be combined with `--provider`.

`PACEFLOW_CURSOR_STATE_PATH` and `PACEFLOW_CURSOR_HISTORY_PATH` override Cursor discovery and can point at local test copies. Database copying is an external preparation step, not an application feature. When testing Windows Cursor data from WSL, use a consistent local SQLite copy (including committed WAL data) rather than opening the live Windows database. `PACEFLOW_CODEX_SESSIONS_PATH` can point both Codex ingestion paths at a copied session directory.

OpenCode accepts both unified `patch` diffs and `before`/`after` snapshots. Changed OpenCode sessions refresh atomically on the next ingest, including repair of previously partial imports. After upgrading Cursor parser behavior, run `vba ingest --fresh` to rebuild existing session facts. Historical Cursor edits with no unique file mapping remain in the parse diagnostics and are excluded from file attribution; resolved edits still contribute to metrics.

Use this loop when you are trying the tool for the first time:

1. Run `vba ingest` after you have local Claude Code, Codex, Cursor, or OpenCode history on the machine.
2. Run `vba session` to see whether sessions are producing accepted code and commits.
3. Run `vba delivery` to see whether AI-heavy commits reached PRs or mainline.
4. Run `vba quality` to see whether AI-heavy code churned, drew follow-up fixes, or was reverted.
5. Run `vba cost` to compare API-equivalent cost against accepted and mainline output.
6. Re-run `vba ingest` whenever you have new sessions, commits, or GitHub PR metadata to refresh.

For GitHub PR reach and PR merge metrics, save a token and ingest again:

```bash
vba github token
vba ingest
```

`PACEFLOW_GITHUB_TOKEN` can be used for CI or one-off overrides.

## Installation

The primary executable is `vba`; `paceflow` remains available as a compatibility alias with the same commands. The Cargo package and release archive names remain `paceflow`. Both executables share the existing `~/.paceflow` data and `PACEFLOW_*` configuration.

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
cargo install --path . --force
```

Prefer not to build from source? Download a prebuilt release from [GitHub Releases](https://github.com/PaceFlow/ai-engineering-analytics/releases).

Supported release targets:

| Platform | Asset |
| --- | --- |
| Windows x86_64 | `paceflow-x86_64-pc-windows-msvc.zip` |
| Linux x86_64 (glibc) | `paceflow-x86_64-unknown-linux-gnu.tar.gz` |
| macOS Apple Silicon | `paceflow-aarch64-apple-darwin.tar.gz` |

See [packaging/INSTALL.md](packaging/INSTALL.md) for platform-specific install commands, macOS Gatekeeper notes, and optional path overrides.

If team hooks or setup scripts will call `vba`, make sure the binary is available on `PATH`; see [Add `vba` To `PATH`](packaging/INSTALL.md#add-vba-to-path).

Requirements:

- `git` must be installed and available on `PATH`
- local Claude Code, Codex, Cursor, or OpenCode history must exist on the machine you run `vba` on
- GitHub PR sync requires `vba github token` or `PACEFLOW_GITHUB_TOKEN`

## Reports

By default, the four report commands compare outcomes by model.

```bash
vba session
vba delivery
vba quality
vba cost
```

Use `--overall` when you want one rolled-up summary row:

```bash
vba session --overall
vba delivery --overall
vba quality --overall
vba cost --overall
```

Use `--model <provider/name>` to keep the same report but narrow it to one model:

```bash
vba session --model codex/gpt-5.4
vba delivery --model codex/gpt-5.4
vba quality --model codex/gpt-5.4
vba cost --model codex/gpt-5.4
```

The reports answer four questions:

- `session`: which models or providers are efficient, noisy, or stuck in loops?
- `delivery`: which AI-heavy changes actually turn into shipped work?
- `quality`: which AI-heavy changes remain durable versus needing cleanup?
- `cost`: which sessions or groups produce useful output for the spend?

For metric definitions, status bands, grouped report behavior, and interpretation guidance, see [docs/USER_GUIDE.md](docs/USER_GUIDE.md).

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
vba session --list-sessions
vba session --group-by provider
vba delivery --group-by task
vba delivery --group-by branch
vba quality --group-by provider
vba quality --group-by branch
vba cost --group-by provider
vba cost --group-by task
```

Use `vba <command> --help` to see command-specific options and metric notes.

## Troubleshooting

### Reports Say There Are No Rows

Run a fresh ingest from a repository that has local assistant history and git commits:

```bash
vba ingest
vba session --all-projects
```

If `session --all-projects` has rows but the plain report does not, you are probably running `vba` from a repo that has no matched sessions yet. Use `--all-projects`, run from the target repo, or pass `--repo /path/to/repo`.

### GitHub PR Metrics Are Empty Or Stale

GitHub PR reach and PR merge metrics need a token and a fresh ingest:

```bash
vba github token
vba ingest
vba delivery
```

The saved token lives locally under the `PACEFLOW_HOME` base directory. `PACEFLOW_GITHUB_TOKEN` can be used for CI or one-off overrides.

### Start Over With A Clean Database

If reports look stale, an ingest was interrupted, or you want to rebuild everything from local source data, delete the local analytics database and ingest again.

```bash
rm -f ~/.paceflow/paceflow.db ~/.paceflow/paceflow.db-wal ~/.paceflow/paceflow.db-shm
vba ingest
vba session
```

If you use a custom `PACEFLOW_HOME`, remove the database under that directory instead:

```bash
rm -f "$PACEFLOW_HOME/.paceflow/paceflow.db" "$PACEFLOW_HOME/.paceflow/paceflow.db-wal" "$PACEFLOW_HOME/.paceflow/paceflow.db-shm"
vba ingest
```

This only removes Paceflow's derived analytics database. It does not delete Claude Code, Codex, Cursor, OpenCode, git, or GitHub source data.

> Note: After upgrading to a build that changes how lines are matched (for example, the whitespace-insensitive line normalization that makes matching tolerant of reformatting), start over with a clean database so stored session line hashes are recomputed. Otherwise old hashes will not match newly scanned commits and metrics like Mainline Reach can read low.

### Cursor Data Is Missing

Paceflow looks for Cursor state/history in the OS config directory under `Cursor/User`. If your Cursor data lives somewhere else, point Paceflow at it before ingesting:

```bash
export PACEFLOW_CURSOR_STATE_PATH=/path/to/state.vscdb
export PACEFLOW_CURSOR_HISTORY_PATH=/path/to/History
vba ingest
```

## More Documentation

- [User Guide](docs/USER_GUIDE.md): report interpretation, metric definitions, status bands, and grouping behavior
- [Install Notes](packaging/INSTALL.md): release assets, platform commands, and local data requirements
- [Development Notes](DEV.md): source workflows, tests, profiling, and validation commands
- [Architecture](docs/ARCHITECTURE.md): ingestion, storage, and analytics pipeline internals
