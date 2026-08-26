# `paceflow` v0.2.5

This release makes the Git pre-commit hook coexist cleanly with the [pre-commit](https://pre-commit.com) framework and other existing hooks, and repairs a Windows setup failure.

Download the archive for your platform from the Assets section below, extract it, and run `paceflow --help`.

## Highlights

- **Composite pre-commit hook**: `paceflow hooks install` now installs a hook that runs the Paceflow setup gate **and then** your repository's own pre-commit checks. A `.pre-commit-config.yaml` (ruff, formatting, etc.) is invoked on commit instead of being silently skipped, so local commits no longer pass checks that CI later fails.
- **Coexistence with existing hooks**: an existing non-Paceflow hook is now backed up and chained instead of refused. Pass `paceflow hooks install --force` to overwrite it instead; `paceflow hooks uninstall` restores the original.
- **Windows repair**: because Paceflow owns a single composite hook, you no longer need to run `pre-commit install` — avoiding the `ExecutableNotFoundError: Executable /bin/sh not found` failure its migration caused. If that migration already happened, `paceflow hooks install` detects and repairs it.
- **Clearer status**: `paceflow hooks status` reports whether your `.pre-commit-config.yaml` will actually run and warns when it won't (e.g. `pre-commit` is not on `PATH`, or a legacy hook is still installed).

## Upgrade Notes

- No database migration is required.
- Re-run `paceflow hooks install` in each repo to upgrade an existing Paceflow hook to the composite form.
- Existing commands continue to work:
  - `paceflow ingest`
  - `paceflow session`
  - `paceflow delivery`
  - `paceflow quality`
  - `paceflow cost`
  - `paceflow sync`
  - `paceflow tui`

## Requirements

- Git must be installed and available on `PATH`.
- Cursor, Codex, Claude Code, or OpenCode local session data must exist on the machine.
- To run pre-commit-framework checks from the hook, install [`pre-commit`](https://pre-commit.com) and ensure it is on `PATH`.

If Cursor data lives in a non-standard location, use:

- `PACEFLOW_CURSOR_STATE_PATH`
- `PACEFLOW_CURSOR_HISTORY_PATH`
