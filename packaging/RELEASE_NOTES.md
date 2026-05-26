# `paceflow` v0.2.3

This is a small UX release that makes empty reports more discoverable when data was ingested while working in a different repo.

Download the archive for your platform from the Assets section below, extract it, and run `paceflow --help`.

## Highlights

- When a report (`paceflow session`, `delivery`, `quality`, or `cost`) is run inside a git repo and returns no rows, the empty-state message now points users at `--all-projects`, instead of only suggesting `paceflow ingest`. The hint only appears when the current-repo scope was auto-injected, so explicit `--repo` and `--all-projects` runs are unaffected.

## Upgrade Notes

- No database migration is required.
- No new commands or flags; behavior only changes when reports return zero rows under the default current-repo scope.
- Existing commands continue to work:
  - `paceflow ingest`
  - `paceflow session`
  - `paceflow delivery`
  - `paceflow quality`
  - `paceflow cost`
  - `paceflow sync`

## Requirements

- Git must be installed and available on `PATH`.
- Cursor, Codex, Claude Code, or OpenCode local session data must exist on the machine.

If Cursor data lives in a non-standard location, use:

- `PACEFLOW_CURSOR_STATE_PATH`
- `PACEFLOW_CURSOR_HISTORY_PATH`
