# vibe-coding-analytics

`vca` is a CLI for understanding how AI-assisted coding sessions turn into code changes, commits, and longer-term outcomes.

It reads local Codex/Cursor history plus git metadata, then gives you three views:

- `session`: how your sessions behaved
- `change`: how commits were attributed and whether they reached mainline
- `lifecycle`: what happened to heavy AI commits later, including churn and reverts

## Install

The easiest install path is a prebuilt release from [GitHub Releases](https://github.com/PaceFlow/vibe-coding-analytics/releases).

Supported release targets:

| Platform | Asset |
| --- | --- |
| Windows x86_64 | `vca-x86_64-pc-windows-msvc.zip` |
| Linux x86_64 (glibc) | `vca-x86_64-unknown-linux-gnu.tar.gz` |
| macOS Intel | `vca-x86_64-apple-darwin.tar.gz` |
| macOS Apple Silicon | `vca-aarch64-apple-darwin.tar.gz` |

Windows (PowerShell):

```powershell
$version = "v0.1.0"
$asset = "vca-x86_64-pc-windows-msvc.zip"
Invoke-WebRequest `
  -Uri "https://github.com/PaceFlow/vibe-coding-analytics/releases/download/$version/$asset" `
  -OutFile $asset
Expand-Archive .\$asset -DestinationPath .\vca
.\vca\vca.exe --help
```

macOS/Linux:

```bash
version="v0.1.0"
asset="vca-x86_64-unknown-linux-gnu.tar.gz"
curl -L "https://github.com/PaceFlow/vibe-coding-analytics/releases/download/${version}/${asset}" -o "${asset}"
tar -xzf "${asset}"
./vca-x86_64-unknown-linux-gnu/vca --help
```

Requirements:

- `git` must be installed and available on `PATH`
- `vca` reads local Codex sessions from `~/.codex/sessions`
- `vca` reads local Cursor state/history from the OS config directory under `Cursor/User`
- If Cursor data lives elsewhere, set `VCA_CURSOR_STATE_PATH` and/or `VCA_CURSOR_HISTORY_PATH`

Source install:

```bash
cargo install --path . --force
```

## Quick Start

```bash
vca ingest
vca session
vca change
vca lifecycle
```

Useful follow-ups:

- `vca session --list-sessions`
- `vca session --group-by provider`
- `vca change --group-by provider`
- `vca lifecycle --group-by provider`
- `vca event-stream --stream commit-session-base --provider human`

## What The Reports Mean

- `session`: session quality and throughput metrics. This is where you look for prompts per session, time to first accepted change, debug loops, error-paste sessions, commit follow-through, and no-output sessions.
- `change`: commit-level attribution and outcomes. When grouped by provider, unmatched commits appear as provider `human`.
- `lifecycle`: post-commit follow-through for heavy AI commits, especially churn and reverts.

## Metric Glossary

- Average user prompts: average number of user prompts per session.
- Avg time to first accepted change: minutes from session start to the first accepted code change.
- Debug loop rate: share of sessions that look like repeated fix-retry cycles.
- Error paste rate: share of sessions where an error message was pasted mid-session.
- Session-to-commit rate: share of sessions followed by a commit within 4 hours.
- No-output session rate: share of sessions with no accepted code changes.
- Heavy commits: commits where matched AI-attributed lines are at least half of changed lines.
- C2 merge rate: share of heavy AI commits that later reached mainline.
- L1 code churn rate: share of AI-added lines on heavy AI commits that were removed again within the churn window.
- L4 revert rate: share of heavy AI commits that were later reverted.

## Notes

- `session`, `change`, and `lifecycle` share the same filter interface: `--weekly`, `--group-by`, `--from`, `--to`, `--repo`, `--provider`, `--task`, `--model`, and `--limit`.
- `event-stream` is a read-only NDJSON export for validating the report base views. It supports `--category`, `--stream`, `--from`, `--to`, `--repo`, `--provider`, `--task`, `--model`, and `--limit`.
- Provider `human` means a commit had no matched AI session attribution at all.
- Task-grouped rows only show ticket-style task keys such as `ABC-123` and exclude integration branches such as `main`, `staging`, `master`, and `develop`.
- `change --group-by task` includes `vs Staging`, derived from `git diff staging...<branch>` for non-integration branches.

## For Contributors

- Release builds are configured for profiler-friendly output in `Cargo.toml` and `.cargo/config.toml`.
- `Taskfile.yml` contains helper tasks for release profiling and live profiling.
