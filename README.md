# vibe-coding-analytics

CLI for measuring AI-assisted coding productivity from local session history + git.

## Install

```bash
cargo install --path . --force
```

## Help

```bash
vca --help
vca ingest --help
vca associate-commits --help
vca task-stats --help
vca stats --help
```

## Manager Test Flow

1. Ingest session and code-change events.

```bash
vca ingest
```

2. Associate AI-attributed commits for the target repo.

```bash
vca associate-commits --repo /absolute/path/to/repo
```

3. View task-level quality metrics.

```bash
vca task-stats --limit 20
```

Optional single task:

```bash
vca task-stats --task ABC-123
```

4. View global quality + session stats.

```bash
vca stats
```

## Notes

- `task-stats` only shows task branches with ticket-style task keys (for example `ABC-123`).
- Integration fallback branches (`main`, `staging`, `master`, `develop`) are excluded from task rows.
- `vs Staging` uses `git diff staging...<branch>` for non-integration branches.
