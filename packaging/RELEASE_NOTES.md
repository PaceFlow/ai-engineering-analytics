# `paceflow` v0.2.1

This release focuses on Cursor ingest reliability, newer Cursor session support, and clearer ingest progress.

Download the archive for your platform from the Assets section below, extract it, and run `paceflow --help`.

## Highlights

- Unified Cursor session and code-change parsing around a shared `CursorSessionGraph`.
- Added Cursor tool-call code-change parsers for newer session structures, including explicit file edits from Cursor tool calls.
- Improved ingest planning and progress output with more granular stages.
- Reduced repeated materialization work during ingest.
- Fixed task key extraction used by task and commit association.

## Fixes

- Fixed task key extraction for branch/task association.
- Fixed ingest progress test behavior.
- Removed release-blocking lint warnings.

## Upgrade Notes

- No database migration or CLI flag changes are required.
- Re-run `paceflow ingest` after upgrading to refresh Cursor-derived session and code-change facts.
- Existing commands continue to work:
  - `paceflow ingest`
  - `paceflow session`
  - `paceflow delivery`
  - `paceflow quality`

## Requirements

- Git must be installed and available on `PATH`.
- Cursor and/or Codex local session data must exist on the machine.

If Cursor data lives in a non-standard location, use:

- `PACEFLOW_CURSOR_STATE_PATH`
- `PACEFLOW_CURSOR_HISTORY_PATH`
