# Troubleshooting

[Back to the README](../README.md)


### Reports Say There Are No Rows

Run a fresh ingest from a repository that has local assistant history and git commits:

```bash
vca ingest
vca session --all-projects
```

If `session --all-projects` has rows but the plain report does not, you are probably running `vca` from a repo that has no matched sessions yet. Use `--all-projects`, run from the target repo, or pass `--repo /path/to/repo`.

### GitHub PR Metrics Are Empty Or Stale

GitHub PR reach and PR merge metrics need a token and a fresh ingest:

```bash
vca github token
vca ingest
vca delivery
```

The saved token lives locally under the `PACEFLOW_HOME` base directory. `PACEFLOW_GITHUB_TOKEN` can be used for CI or one-off overrides.

### Start Over With A Clean Database

If reports look stale, an ingest was interrupted, or parser behavior changed after an upgrade, rebuild the derived analytics database:

```bash
vca ingest --fresh
vca session
```

`--fresh` rebuilds all providers and cannot be combined with `--provider`. It uses the configured database location, including a custom `PACEFLOW_HOME`, and preserves local configuration. It does not delete assistant history or git source data.

Rebuilding is especially useful after changes to line matching: stored session hashes need to be recomputed before they can match newly scanned commits reliably.

### Cursor Data Is Missing

Paceflow looks for Cursor state/history in the OS config directory under `Cursor/User`. If your Cursor data lives somewhere else, point Paceflow at it before ingesting:

```bash
export PACEFLOW_CURSOR_STATE_PATH=/path/to/state.vscdb
export PACEFLOW_CURSOR_HISTORY_PATH=/path/to/History
vca ingest
```


## Provider ingestion notes

`PACEFLOW_CURSOR_STATE_PATH` and `PACEFLOW_CURSOR_HISTORY_PATH` override Cursor discovery and can point at local test copies. Database copying is an external preparation step, not an application feature. When testing Windows Cursor data from WSL, use a consistent local SQLite copy (including committed WAL data) rather than opening the live Windows database. `PACEFLOW_CODEX_SESSIONS_PATH` can point both Codex ingestion paths at a copied session directory.

OpenCode accepts both unified `patch` diffs and `before`/`after` snapshots. Changed OpenCode sessions refresh atomically on the next ingest, including repair of previously partial imports. After upgrading Cursor parser behavior, run `vca ingest --fresh` to rebuild existing session facts. Historical Cursor edits with no unique file mapping remain in the parse diagnostics and are excluded from file attribution; resolved edits still contribute to metrics.


To refresh just one source, run `vca ingest --provider cursor` (also `codex`, `opencode`, or `claude`). Provider ingestion failures return a nonzero exit status.
