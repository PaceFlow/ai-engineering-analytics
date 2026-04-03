# AI Engineering Analytics

Local-first CLI for answering the questions that matter after you use coding agents:

- Was this work actually useful?
- Did it ship?
- Did it hold up?
- What should I do differently next time?

`paceflow` reads local Codex/Cursor history plus git metadata and turns that evidence into three practical views:

- `session`: were you getting leverage, or just steering and retrying?
- `delivery`: did AI-heavy work turn into real commits that reached mainline?
- `quality`: did accepted AI-generated code stick, or did it get churned out later?

The point is not to count prompts or accepted lines for their own sake. The point is to help an individual engineer improve how they work with coding agents.

## Why You Should Care

Most AI coding workflows feel productive in the moment. That does not mean they were useful.

AI Engineering Analytics gives you a local evidence trail for three failure modes that are easy to miss:

- sessions that felt busy but produced little accepted output
- AI-heavy work that never made it to mainline
- accepted code that landed quickly and was removed soon after

If those patterns show up repeatedly, you usually need tighter task slicing, better upfront constraints, earlier validation, or stricter review before accepting generated code.

## What You Learn Quickly

- `paceflow session` tells you whether your sessions were efficient, noisy, or stuck in loops.
- `paceflow delivery` tells you whether AI-assisted work actually turned into shipped change.
- `paceflow quality` tells you whether accepted code created durable value or follow-up cleanup.

## Example: Session Report

```text
Session Metrics
Sessions: 43
Average User Prompts: 4.21
Avg Time to First Accepted Change (min): 8.30
Debug Loop Rate: 18.60% (8/43)
No-Output Session Rate: 13.95% (6/43)
```

Why it matters:

- High prompt counts and slow first changes usually mean you are spending too much effort steering the model.
- High debug-loop or no-output rates usually mean the task was underspecified, too broad, or missing key repo context.

What to do differently:

- Narrow the task before you prompt.
- Put constraints and acceptance criteria in the first request.
- Stop a failing retry cycle earlier and verify assumptions manually.

## Example: Delivery Report

```text
Delivery Metrics

Group                         Branch                      Commits     Heavy   C2(merge)   vs Staging
API-142                       API-142-agent-auth               7         5       80.0%     +184/-41
WEB-203                       WEB-203-checkout-fixes           5         4       50.0%      +96/-88
OPS-88                        OPS-88-deploy-cleanup            4         3       33.3%      +41/-73
```

Why it matters:

- Heavy commits tell you where AI materially influenced the diff instead of just assisting around the edges.
- C1 tells you whether AI-heavy work even made it to review as a pull request.
- Merge rate tells you whether that work survived review and integration.
- C3 tells you whether PR-linked AI-heavy work actually cleared the PR funnel and merged.

What to do differently:

- If AI-heavy tasks have weak merge rates, reduce branch size and tighten review before accepting generated code.
- If a task has large diffs and weak outcomes, split it into smaller units with clearer verification points.

## Example: Quality Report

```text
Quality Metrics
Heavy commits: 52
L1 Code Churn Rate: 12.40% (98/790)
L4 Revert Rate: 1.92% (1/52)
```

Why it matters:

- Churn means accepted code landed, but was the wrong fit or needed cleanup soon after.
- Reverts are the clearest signal that the workflow created cost, not leverage.

What to do differently:

- Review AI-heavy code more aggressively before merge.
- Treat generated code as a draft when the surrounding system constraints are unclear.

## Quick Start

Once `paceflow` is installed, ingest your local history and open the three report views:

```bash
paceflow ingest
paceflow session
paceflow delivery
paceflow quality
```

Useful follow-ups:

- `paceflow session --list-sessions`
- `paceflow session --group-by provider`
- `paceflow delivery --group-by task`
- `paceflow quality --group-by provider`

Optional GitHub PR sync setup:

```bash
paceflow github token
paceflow ingest
```

Rerun `paceflow github token` to replace or delete the saved token. For CI or one-off overrides, `PACEFLOW_GITHUB_TOKEN` still takes precedence over the saved local token.

## Installation

Build and install from source:

```bash
git clone https://github.com/PaceFlow/ai-engineering-analytics.git
cd ai-engineering-analytics
cargo install --path . --force
```

Prefer not to build from source? Download a prebuilt release from [GitHub Releases](https://github.com/PaceFlow/ai-engineering-analytics/releases).

Supported release targets:

| Platform | Asset |
| --- | --- |
| Windows x86_64 | `paceflow-x86_64-pc-windows-msvc.zip` |
| Linux x86_64 (glibc) | `paceflow-x86_64-unknown-linux-gnu.tar.gz` |
| macOS Apple Silicon | `paceflow-aarch64-apple-darwin.tar.gz` |

Windows (PowerShell):

```powershell
$version = "v0.1.0-beta.1"
$asset = "paceflow-x86_64-pc-windows-msvc.zip"
Invoke-WebRequest `
  -Uri "https://github.com/PaceFlow/ai-engineering-analytics/releases/download/$version/$asset" `
  -OutFile $asset
Expand-Archive .\$asset -DestinationPath .\paceflow
.\paceflow\paceflow.exe --help
```

macOS/Linux:

```bash
version="v0.1.0-beta.1"
asset="paceflow-x86_64-unknown-linux-gnu.tar.gz"
curl -L "https://github.com/PaceFlow/ai-engineering-analytics/releases/download/${version}/${asset}" -o "${asset}"
tar -xzf "${asset}"
./paceflow-x86_64-unknown-linux-gnu/paceflow --help
```

macOS note for internal builds:

- If Gatekeeper blocks `paceflow`, go to `System Settings > Privacy & Security` and click `Open Anyway`, then rerun the binary.
- Fresh extractions can inherit quarantine from the downloaded archive. If needed, clear quarantine on the extracted folder:

```bash
xattr -dr com.apple.quarantine paceflow-aarch64-apple-darwin
./paceflow-aarch64-apple-darwin/paceflow --help
```

Requirements:

- `git` must be installed and available on `PATH`
- `paceflow` reads local Codex sessions from `~/.codex/sessions`
- `paceflow` reads local Cursor state/history from the OS config directory under `Cursor/User`
- If Cursor data lives elsewhere, set `PACEFLOW_CURSOR_STATE_PATH` and/or `PACEFLOW_CURSOR_HISTORY_PATH`
- To enable GitHub PR sync during ingest, either run `paceflow github token` once or set `PACEFLOW_GITHUB_TOKEN`
- To enable GitHub-backed `C1` and `C3` metrics for `github.com` repos, set `PACEFLOW_GITHUB_TOKEN` with at least `Pull requests: read`

## Who It's For

This tool is primarily for individual engineers who want to improve how they work with coding agents and editor assistants.

It can also support team or manager conversations later, but the default framing is personal workflow improvement:

- where am I wasting time steering the model?
- where is AI help actually turning into shipped code?
- where am I accepting code that does not last?

## How To Read The Reports

Use the reports to answer three practical questions:

### 1. Were my sessions actually useful?

- Average user prompts
- Avg time to first accepted change
- Debug loop rate
- Error paste rate
- No-output session rate

These are workflow-quality signals, not just activity counters.

### 2. Did the work turn into shipped change?

- Heavy commits
- PR reach rate
- Merge rate
- PR merge rate
- Session-to-commit rate

These tell you whether session effort turned into commits and whether those commits made it into mainline history.

### 3. Did the code hold up?

- Code churn rate
- Revert rate

These are the strongest signals for whether AI-assisted work created durable value or follow-up cleanup.

## Metric Reference

The reports are built from normalized session events, matched commit/session attribution, and live git history.

### Session Metrics

- `Average User Prompts`: average `user_turn_count` across included sessions with at least one non-empty user turn
- `Avg Time to First Accepted Change`: average minutes between session start and the first accepted code change; sessions without an accepted change are excluded
- `Debug Loop Rate`: share of sessions flagged as repeated error-fix loops; a session is flagged when the same normalized error signature appears in 5 or more user turns with assistant replies in between
- `Error Paste Rate`: share of sessions where a user pasted an error-like message after the first user message
- `Session-to-Commit Rate`: share of sessions with at least one matched commit between session start and session end plus 4 hours
- `No-Output Session Rate`: share of sessions with user turns but no accepted code-change output

### Session List Fields

- `LOC`: total accepted changed lines for the session (`accepted_lines_added + accepted_lines_removed`)
- `+Lines`: accepted added lines for the session
- `-Lines`: accepted removed lines for the session
- `Words/LOC`: user-word count divided by accepted changed lines; if accepted changed lines are zero, this is `N/A`

### Delivery Metrics

- `Commits`: count of included commits in the current filter or group
- `Heavy Commits`: commits where the AI-attributed share of changed lines is at least 50%
- `Merge Rate`: share of heavy commits that later reached mainline, including squash-aware content matching
- `vs Staging`: live diff size from `git diff staging...<branch>` for task-grouped rows; computed at render time

### Quality Metrics

- `Code Churn Rate`: share of AI-added lines from heavy commits that reached mainline and were removed from mainline within a 14-day window
- `Revert Rate`: share of heavy commits later reverted by a commit body containing `This reverts commit <sha>`

### Grouped Report Weighting

- Repo and weekly rollups use ordinary counts and averages over included sessions or commits
- Provider/model grouped delivery and quality reports work from commit-session attribution rows so unmatched commits can appear as `human`
- Task-grouped session reports use attribution-weighted averages and rates
- Task-grouped delivery and quality reports exclude non-ticket task keys plus integration branches such as `main`, `staging`, `master`, and `develop`

## Notes

- `session`, `delivery`, and `quality` share the same filter interface: `--weekly`, `--group-by`, `--from`, `--to`, `--repo`, `--provider`, `--task`, `--model`, and `--limit`
- Provider `human` means a commit had no matched AI session attribution at all
- Task-grouped rows only show ticket-style task keys such as `ABC-123` and exclude integration branches such as `main`, `staging`, `master`, and `develop`
- `delivery --group-by task` includes `vs Staging`, derived from `git diff staging...<branch>` for non-integration branches
- Local analytics state lives under `~/.paceflow/paceflow.db` by default; override the base home with `PACEFLOW_HOME`

Development notes, profiling setup, and source-oriented workflows live in [DEV.md](DEV.md).
