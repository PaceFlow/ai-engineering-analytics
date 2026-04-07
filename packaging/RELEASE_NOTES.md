# `paceflow` release

This is the first stable public release of `paceflow`.

Download the archive for your platform from the Assets section below, extract it, and run `paceflow --help`.

Highlights in `v0.1.0`:

- GitHub-backed delivery metrics for `C1` and `C3`
- GitHub PR sync with saved token setup flow
- `L3` bug-after-merge tracking for quality reporting
- Streamlined ingest progress output

Quick start:

- `paceflow ingest`
- `paceflow session`
- `paceflow delivery`
- `paceflow quality`

Requirements:

- Git must be installed and available on `PATH`
- Cursor and/or Codex local session data must exist on the machine

If Cursor data lives in a non-standard location, use:

- `PACEFLOW_CURSOR_STATE_PATH`
- `PACEFLOW_CURSOR_HISTORY_PATH`
