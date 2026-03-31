# `paceflow` Install Notes

`paceflow` is a single-file CLI binary. Download the archive for your platform, extract it, and run:

```text
paceflow --help
paceflow ingest
```

Requirements:

- Git must be installed and available on `PATH`
- Cursor/Codex local session data must exist on the machine you run `paceflow` on

Optional support overrides:

- `PACEFLOW_CURSOR_STATE_PATH`
- `PACEFLOW_CURSOR_HISTORY_PATH`
