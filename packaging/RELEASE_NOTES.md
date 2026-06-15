# `paceflow` v0.2.4

This release adds an interactive terminal dashboard, improves Cursor edit ingestion for newer agent formats, and tightens first-change and mainline attribution.

Download the archive for your platform from the Assets section below, extract it, and run `paceflow --help`.

## Highlights

- **`paceflow tui`**: new interactive terminal dashboard for exploring session, delivery, quality, and cost analytics with keyboard navigation.
- **Cursor `edit_file_v2` support**: ingest edits that reference before/after content blobs by id, not just inline streaming patches.
- **First-change timing**: more accurate first-change attribution and a `--fresh` ingest option to rebuild from scratch.
- **Mainline matching**: improved line matching and mainline update detection for commit association.

## Upgrade Notes

- No database migration is required.
- New command: `paceflow tui`
- New ingest flag: `paceflow ingest --fresh` (rebuilds analytics from scratch)
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
