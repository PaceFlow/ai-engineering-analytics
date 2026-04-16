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

- `paceflow session` compares which models are efficient, noisy, or stuck in loops.
- `paceflow delivery` compares which models actually turn into shipped change.
- `paceflow quality` compares which models produce durable code versus cleanup.

## Example: Session Report

```text
Session Metrics
Model                         Sessions     Prompts    First Chg            Loop           Error       To Commit       No Output
codex/gpt-5.4                      33       12.67        237.5            3.0%           15.2%           45.5%           39.4%
codex/gpt-5.3-codex                12        5.10         42.1            0.0%            8.3%           75.0%           16.7%
cursor/default                      6        2.30          9.4            0.0%            0.0%           33.3%            0.0%
Legend:
- Prompts = avg prompts per session.
- First Chg = avg minutes to first accepted code change.
- Loop, Error, To Commit, and No Output = percentage rates.
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
Model                          Commits     Heavy    PR Reach  Mainline Reach    PR Merge
codex/gpt-5.4                      50        31       66.7%            96.8%       83.3%
codex/gpt-5.3-codex                33        18       41.7%            94.4%       80.0%
Legend:
- PR Reach, Mainline Reach, and PR Merge = percentage rates.
```

Why it matters:

- Heavy commits tell you where AI materially influenced the diff instead of just assisting around the edges.
- PR reach tells you whether AI-heavy work even made it to review as a pull request.
- Mainline reach tells you whether that work survived review and integration into mainline.
- PR merge tells you whether PR-linked AI-heavy work actually cleared the PR funnel and merged.

What to do differently:

- If AI-heavy tasks have weak mainline reach, reduce branch size and tighten review before accepting generated code.
- If a task has large diffs and weak outcomes, split it into smaller units with clearer verification points.

## Example: Quality Report

```text
Quality Metrics
Model                            Heavy   Churn Rate   Bug Rate  Revert Rate
codex/gpt-5.4                       31         19.9%      30.4%         0.0%
codex/gpt-5.3-codex                 18         53.3%      85.7%         0.0%
Legend:
- Churn Rate, Bug Rate, and Revert Rate = percentage rates.
```

Why it matters:

- Churn means accepted code landed, but was the wrong fit or needed cleanup soon after.
- Bug-after-merge rate catches code that shipped but triggered later fix work.
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

By default those three views compare outcomes by model. Use `--overall` when you want the rolled-up one-row summary instead.
Use `--model <provider/name>` when you want to keep the same report but narrow it to one model.

Useful follow-ups:

- `paceflow session --model codex/gpt-5.4`
- `paceflow session --overall`
- `paceflow session --list-sessions`
- `paceflow session --group-by provider`
- `paceflow delivery --model codex/gpt-5.4`
- `paceflow delivery --overall`
- `paceflow delivery --group-by task`
- `paceflow quality --model codex/gpt-5.4`
- `paceflow quality --overall`
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
$version = "v0.2.0"
$asset = "paceflow-x86_64-pc-windows-msvc.zip"
Invoke-WebRequest `
  -Uri "https://github.com/PaceFlow/ai-engineering-analytics/releases/download/$version/$asset" `
  -OutFile $asset
Expand-Archive .\$asset -DestinationPath .\paceflow
.\paceflow\paceflow.exe --help
```

macOS/Linux:

```bash
version="v0.2.0"
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
- To enable GitHub-backed PR reach and PR merge metrics for `github.com` repos, set `PACEFLOW_GITHUB_TOKEN` with at least `Pull requests: read`

## Who It's For

This tool is primarily for individual engineers who want to improve how they work with coding agents and editor assistants.

It can also support team or manager conversations later, but the default framing is personal workflow improvement:

- where am I wasting time steering the model?
- where is AI help actually turning into shipped code?
- where am I accepting code that does not last?

## How To Read The Reports

Use the reports to answer three practical questions:

By default, `session`, `delivery`, and `quality` answer them by comparing models side by side. Use `--overall` if you want the old rolled-up summary instead of the trust comparison view.

When you want to stay in the same report but inspect one model only, use `--model <provider/name>`. For example, `paceflow delivery --model codex/gpt-5.4` keeps the delivery view and filters it down to that one model.

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
- Mainline reach rate
- PR merge rate
- Session-to-commit rate

These tell you whether session effort turned into commits and whether those commits made it into mainline history.

### 3. Did the code hold up?

- Code churn rate
- Bug-after-merge rate
- Revert rate

These are the strongest signals for whether AI-assisted work created durable value or follow-up cleanup.

### Status Bands

`--overall` summaries attach `good`, `watch`, and `risk` labels using opinionated thresholds so you can scan a rolled-up report before you read the metric definitions in the footer.

- Session: `Avg prompts <4 / <=7 / >7`, `Time to first change <15 / <=45 / >45 min`, `Debug loops <10 / <=25 / >25%`, `Error pastes <10 / <=25 / >25%`, `Sessions to commit >=50 / >=25 / <25%`, `No-output sessions <15 / <=35 / >35%`
- Delivery: `PR reach >=70 / >=40 / <40%`, `Mainline reach >=75 / >=50 / <50%`, `PR merge >=75 / >=50 / <50%`
- Quality: `Code churn <15 / <=30 / >30%`, `Bug-after-merge <15 / <=30 / >30%`, `Reverts <2 / <=5 / >5%`

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
- `PR Reach`: share of heavy GitHub AI commits that reached a pull request
- `Mainline Reach`: share of heavy commits that later reached mainline, including squash-aware content matching
- `PR Merge`: share of PR-linked heavy GitHub AI commits whose PR merged
- `vs Staging`: live diff size from `git diff staging...<branch>` for task-grouped rows; computed at render time

### Quality Metrics

- `Code Churn Rate`: share of AI-added lines from heavy commits that reached mainline and were removed from mainline within a 14-day window
- `Bug-After-Merge Rate`: share of merged heavy AI commits that drew at least one later fix-like commit touching the same files within a 60-day window
- `Revert Rate`: share of heavy commits later reverted by a commit body containing `This reverts commit <sha>`

### Grouped Report Weighting

- Repo and weekly rollups use ordinary counts and averages over included sessions or commits
- Provider/model grouped delivery and quality reports work from commit-session attribution rows so unmatched commits can appear as `human`
- Task-grouped session reports use attribution-weighted averages and rates
- Task-grouped delivery and quality reports exclude non-ticket task keys plus integration branches such as `main`, `staging`, `master`, and `develop`

## Notes

- `session`, `delivery`, and `quality` share the same filter interface: `--weekly`, `--group-by`, `--from`, `--to`, `--repo`, `--provider`, `--task`, `--model`, and `--limit`
- `session`, `delivery`, and `quality` default to `group-by model` when you do not pass `--group-by` or `--overall`
- `--model` filters the current report to one model without changing the active view
- `--overall` swaps the default model comparison for the rolled-up one-row summary and conflicts with `--group-by`
- Provider `human` means a commit had no matched AI session attribution at all
- Task-grouped rows only show ticket-style task keys such as `ABC-123` and exclude integration branches such as `main`, `staging`, `master`, and `develop`
- `delivery --group-by task` includes `vs Staging`, derived from `git diff staging...<branch>` for non-integration branches
- Local analytics state lives under `~/.paceflow/paceflow.db` by default; override the base home with `PACEFLOW_HOME`

Development notes, profiling setup, and source-oriented workflows live in [DEV.md](DEV.md).
