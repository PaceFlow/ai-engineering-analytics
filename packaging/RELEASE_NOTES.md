# `paceflow` v0.2.2

This release focuses on the new analytics sync flow, periodic background scheduling, and reliability fixes on Windows and macOS.

Download the archive for your platform from the Assets section below, extract it, and run `paceflow --help`.

## Highlights

- Added `paceflow sync` commands for manually pushing local analytics events to PaceFlow.
- Added a periodic sync schedule that runs in the background via `launchd` on macOS, the Task Scheduler on Windows, and `systemd` user units on Linux.
- Added a `paceflow hook setup` gate so analytics sync is opt-in and tied to a configured person.
- Added a person sync setup flow for first-time configuration.
- Filtered sync cost metrics to committed work so unmerged sessions no longer skew totals.
- Published `paceflow` to crates.io via a new CI workflow, with package publishing metadata in `Cargo.toml`.

## Fixes

- Wrote Windows scheduled-task XML as UTF-16 LE with a BOM so Task Scheduler accepts it.
- Avoided showing a console window for the background sync task on Windows by launching it through `wscript`.
- Updated the macOS `launchd` config so the periodic sync agent installs and reloads cleanly.
- Tightened `paceflow hook` and sync tests after the new gate and schedule logic landed.

## Upgrade Notes

- No database migration is required.
- Re-run `paceflow ingest` after upgrading to keep analytics events current.
- To enable background sync, run `paceflow hook setup` and follow the person sync setup prompts.
- Existing commands continue to work:
  - `paceflow ingest`
  - `paceflow session`
  - `paceflow delivery`
  - `paceflow quality`
  - `paceflow sync` (new)

## Requirements

- Git must be installed and available on `PATH`.
- Cursor, Codex, Claude Code, or OpenCode local session data must exist on the machine.

If Cursor data lives in a non-standard location, use:

- `PACEFLOW_CURSOR_STATE_PATH`
- `PACEFLOW_CURSOR_HISTORY_PATH`
