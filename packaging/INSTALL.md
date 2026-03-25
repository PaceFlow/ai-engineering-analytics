# `vca` Install Notes

`vca` is a single-file CLI binary. Download the archive for your platform, extract it, and run:

```text
vca --help
vca ingest
```

Requirements:

- Git must be installed and available on `PATH`
- Cursor/Codex local session data must exist on the machine you run `vca` on

Optional support overrides:

- `VCA_CURSOR_STATE_PATH`
- `VCA_CURSOR_HISTORY_PATH`
