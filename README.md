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

Get the CLI installed from source:

```bash
git clone https://github.com/PaceFlow/vibe-coding-analytics.git
cd vibe-coding-analytics
cargo install --path . --force
```

Then ingest your local history and open the three report views:

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

- `vca session` helps you decide whether your sessions were actually productive or just noisy. It makes reprompt churn, slow starts, debug loops, error-paste sessions, weak follow-through, and no-output sessions visible quickly.
- `vca change` helps you decide whether AI-assisted work turned into meaningful shipped changes. It shows where AI had real influence on commits and whether those commits reached mainline.
- `vca lifecycle` helps you decide whether accepted AI-generated code held up after landing. It surfaces churn and reverts so you can tell the difference between short-term output and durable value.

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
Merge rate = share of heavy AI commits that later reached mainline

Group                         Branch                      Commits     Heavy   Merge Rate   vs Staging
API-142                       API-142-agent-auth               7         5         80.0%     +184/-41
WEB-203                       WEB-203-checkout-fixes           5         4         50.0%      +96/-88
OPS-88                        OPS-88-deploy-cleanup            4         3         33.3%      +41/-73
```

Why this matters:

- Heavy commits tell you where AI meaningfully influenced the shipped diff instead of just assisting around the edges.
- Merge rate tells you whether that work made it into mainline.
- `vs Staging` gives a quick sense of branch size and cleanup cost.

What to do with it:

- If merge rate is low, the AI-heavy work is not consistently surviving review or integration.
- If a task shows large diffs with weak merge outcomes, reduce branch size and tighten review before accepting generated code.

## Example: `vca lifecycle`

```text
Lifecycle Metrics
Code churn rate = share of AI-added lines on heavy AI commits that were later removed within the churn window
Revert rate = share of heavy AI commits that were later reverted

Heavy commits: 52
Code Churn Rate: 12.40% (98/790)
Revert Rate: 1.92% (1/52)
```

Why this matters:

- Code churn rate shows whether accepted AI-generated code actually lasted.
- Revert rate shows the most obvious failures: heavy AI commits that had to be reverted.

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
- Merge rate
- Session-to-commit rate

These tell you whether session effort turned into commits and whether those commits made it into mainline history.

### 3. Did the code hold up?

- Code churn rate
- Revert rate

These are the strongest signals for whether AI-assisted work created durable value or follow-up cleanup.

## How Metrics Are Calculated

The reports are built from normalized session events, matched commit/session attribution, and live git history. These are the current calculations used by the CLI.

### Session Metrics

- `Average User Prompts` - Average `user_turn_count` across included sessions. A session only counts if it has at least one non-empty user turn.
- `Avg Time to First Accepted Change` - Average minutes between session start and the first accepted code change timestamp. Sessions without an accepted change timestamp are excluded from this average.
- `Debug Loop Rate` - *Numerator* = sessions flagged as repeated error-fix loops. *Denominator* = sessions with a known debug-loop flag. A session is flagged when the same normalized error signature appears in 5 or more user turns with assistant replies in between.
- `Error Paste Rate` - *Numerator* = sessions where a user pasted an error-like message after the first user message. *Denominator* = sessions with a known mid-session error-paste flag. Detection looks for signals such as stack traces, `error:`, `traceback`, numbered error counts, build/test failure phrases, and similar markers.
- `Session-to-Commit Rate` - *Numerator* = sessions with at least one matched commit whose timestamp falls between session start and session end plus 4 hours. *Denominator* = sessions with enough timing data to evaluate that window.
- `No-Output Session Rate` - *Numerator* = sessions with user turns but no accepted code-change output. *Denominator* = sessions with user turns.

### Session List Fields

- `LOC` - Total accepted changed lines for the session. This is `accepted_lines_added + accepted_lines_removed`.
- `+Lines` - Accepted added lines for the session.
- `-Lines` - Accepted removed lines for the session.
- `Words/LOC` - User-word count divided by accepted changed lines. If accepted changed lines are zero, this is shown as `N/A`.

### Change Metrics

- `Commits` - Count of included commits in the current filter or group.
- `Heavy Commits` - Count of commits where the AI-attributed share of changed lines is at least 50%. Internally this is derived from matched AI lines divided by total changed lines in the commit.
- `Merge Rate` - *Numerator* = heavy commits marked as having reached mainline. *Denominator* = heavy commits. A commit counts as merged if either it is directly present on the repo's mainline ref (`main`, `master`, or remote equivalent), or at least 30 matched AI-added lines can be found on mainline with at least 80% content match after line-hash normalization. This makes the metric squash-aware instead of relying only on commit ancestry.
- `vs Staging` - Live diff size from `git diff staging...<branch>` for task-grouped rows. It is not stored in the analytics tables; it is computed at render time.

### Lifecycle Metrics

- `Code Churn Rate` - *Numerator* = AI-added lines from heavy commits that reached mainline and were later removed from mainline within a 14-day window. *Denominator* = AI-added lines from heavy commits that reached mainline. Matching is line-hash based, not full-file snapshot based.
- `Revert Rate` - *Numerator* = heavy commits later reverted. *Denominator* = heavy commits. Reverts are detected from git history by scanning commit bodies for `This reverts commit <sha>`.

### Grouped Report Weighting

- `Repo and Weekly Rollups` - Use ordinary counts and averages over included sessions or commits.
- `Provider/Model Grouped Change and Lifecycle Reports` - Work from commit-session attribution rows so unmatched commits can appear as `human`.
- `Task-Grouped Session Reports` - Use attribution-weighted averages and rates. The weight for a session-task row is the total matched lines linking that session to commits on the task; if that weight is zero, it falls back to `1`.
- `Task-Grouped Change and Lifecycle Reports` - Use task-attributed commit rows and exclude non-ticket task keys plus integration branches such as `main`, `staging`, `master`, and `develop`.

## Prebuilt Binaries

Prefer not to build from source? Download a prebuilt release from [GitHub Releases](https://github.com/PaceFlow/vibe-coding-analytics/releases).

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

## Notes

- `session`, `change`, and `lifecycle` share the same filter interface: `--weekly`, `--group-by`, `--from`, `--to`, `--repo`, `--provider`, `--task`, `--model`, and `--limit`.
- Provider `human` means a commit had no matched AI session attribution at all.
- Task-grouped rows only show ticket-style task keys such as `ABC-123` and exclude integration branches such as `main`, `staging`, `master`, and `develop`.
- `change --group-by task` includes `vs Staging`, derived from `git diff staging...<branch>` for non-integration branches.

Development notes, profiling setup, and source-oriented workflows live in [DEV.md](DEV.md).
