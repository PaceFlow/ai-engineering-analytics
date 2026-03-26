# vibe-coding-analytics

Local-first CLI for understanding whether your AI-assisted development sessions are actually helping you ship useful code.

`vca` reads local Codex/Cursor history plus git metadata and turns that evidence into three user-facing views:

- `session`: were your sessions efficient or stuck in loops?
- `change`: did AI-heavy work turn into real commits and reach mainline?
- `lifecycle`: did that code stick, or was it churned out and reverted later?

The point is not just to count prompts or accepted lines. The point is to help you answer:

- Was this AI-assisted work useful?
- Where did it break down?
- What should I do differently next time?

## Who It's For

This is primarily for individual developers who want to improve how they work with coding agents and editor assistants.

It is useful when you want to spot patterns like:

- lots of prompting but little accepted output
- sessions that turned into repeated fix-retry loops
- heavy AI commits that never reached mainline
- code that landed but was quickly removed again

## Quick Start

Install `vca`, ingest your local history, then open the three report views:

```bash
vca ingest
vca session
vca change
vca lifecycle
```

Useful follow-ups:

- `vca session --list-sessions`
- `vca session --group-by provider`
- `vca change --group-by task`
- `vca lifecycle --group-by provider`

## What You Get

- `vca session` shows session quality and throughput metrics such as prompts per session, time to first accepted change, debug loops, error-paste sessions, commit follow-through, and no-output sessions.
- `vca change` shows commit-level attribution and merge outcomes. When grouped by provider, unmatched commits appear as provider `human`.
- `vca lifecycle` shows post-commit follow-through for heavy AI commits, especially churn and reverts.

## Example: `vca session`

```text
Session Metrics
Average user prompts = average number of user prompts per session
Avg time to first accepted change = minutes from session start to the first accepted code change
Debug loop rate = share of sessions that look like repeated fix-retry loops
Error paste rate = share of sessions where an error message was pasted mid-session
Session-to-commit rate = share of sessions followed by a commit within 4 hours
No-output session rate = share of sessions with no accepted code changes

Sessions: 43
Average User Prompts: 4.21
Avg Time to First Accepted Change (min): 8.30
Debug Loop Rate: 18.60% (8/43)
Error Paste Rate: 11.63% (5/43)
Session-to-Commit Rate: 67.44% (29/43)
No-Output Session Rate: 13.95% (6/43)
```

Why this matters:

- High prompt counts and long time-to-first-change usually mean you are spending too much effort steering the model.
- High debug-loop and error-paste rates usually mean missing context, weak constraints, or bad initial task slicing.
- Low session-to-commit rate means a lot of session activity did not turn into delivery.

What to do with it:

- If prompts are high, narrow the task and front-load more concrete constraints.
- If loops are high, stop earlier and verify assumptions before another retry cycle.
- If no-output sessions are common, split exploration from implementation more explicitly.

## Example: `vca change --group-by task`

```text
Change Metrics
Heavy commits = commits where matched AI-attributed lines are at least half of changed lines
C2 merge rate = share of heavy AI commits that later reached mainline

Group                         Branch                      Commits     Heavy     C2(merge)   vs Staging
API-142                       API-142-agent-auth               7         5         80.0%     +184/-41
WEB-203                       WEB-203-checkout-fixes           5         4         50.0%      +96/-88
OPS-88                        OPS-88-deploy-cleanup            4         3         33.3%      +41/-73
```

Why this matters:

- Heavy commits tell you where AI meaningfully influenced the shipped diff instead of just assisting around the edges.
- `C2 merge rate` tells you whether that work made it into mainline.
- `vs Staging` gives a quick sense of branch size and cleanup cost.

What to do with it:

- If `C2` is low, the AI-heavy work is not consistently surviving review or integration.
- If a task shows large diffs with weak merge outcomes, reduce branch size and tighten review before accepting generated code.

## Example: `vca lifecycle`

```text
Lifecycle Metrics
L1 code churn rate = share of AI-added lines on heavy AI commits that were later removed within the churn window
L4 revert rate = share of heavy AI commits that were later reverted

Heavy commits: 52
L1 Code Churn Rate: 12.40% (98/790)
L4 Revert Rate: 1.92% (1/52)
```

Why this matters:

- `L1` shows whether accepted AI-generated code actually lasted.
- `L4` shows the most obvious failures: heavy AI commits that had to be reverted.

What to do with it:

- High churn usually means the code landed but was the wrong solution, poor fit, or too lightly reviewed.
- Reverts are the strongest signal that the workflow produced costly mistakes rather than leverage.

## How To Read The Reports

Use the reports to answer three practical questions:

### 1. Were my sessions efficient?

- Average user prompts
- Avg time to first accepted change
- Debug loop rate
- Error paste rate
- No-output session rate

These are workflow-quality signals, not just activity counters.

### 2. Did the work turn into shipped changes?

- Heavy commits
- C2 merge rate
- Session-to-commit rate

These tell you whether session effort turned into commits and whether those commits made it into mainline history.

### 3. Did the code hold up?

- L1 code churn rate
- L4 revert rate

These are the strongest signals for whether AI-assisted work created durable value or follow-up cleanup.

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

## Notes

- `session`, `change`, and `lifecycle` share the same filter interface: `--weekly`, `--group-by`, `--from`, `--to`, `--repo`, `--provider`, `--task`, `--model`, and `--limit`.
- Provider `human` means a commit had no matched AI session attribution at all.
- Task-grouped rows only show ticket-style task keys such as `ABC-123` and exclude integration branches such as `main`, `staging`, `master`, and `develop`.
- `change --group-by task` includes `vs Staging`, derived from `git diff staging...<branch>` for non-integration branches.

Development notes, profiling setup, and source-oriented workflows live in [DEV.md](DEV.md).
