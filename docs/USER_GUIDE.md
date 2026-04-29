# Paceflow User Guide

This guide explains how to read `paceflow` reports after you have run:

```bash
paceflow ingest
```

For the shortest setup path, start with the [README](../README.md).

## Report Workflow

Use the reports to answer three practical questions:

```bash
paceflow session
paceflow delivery
paceflow quality
```

By default, `session`, `delivery`, and `quality` compare outcomes by model. Use `--overall` for a rolled-up summary, or `--model <provider/name>` to keep the same report but inspect one model only.

Examples:

```bash
paceflow session --model codex/gpt-5.4
paceflow session --overall
paceflow delivery --group-by branch
paceflow quality --group-by provider
```

## How To Read The Reports

### Session

`paceflow session` shows whether your sessions were actually useful.

Key signals:

- Average user prompts
- Avg time to first accepted change
- Debug loop rate
- Error paste rate
- Session-to-commit rate
- No-output session rate

High prompt counts and slow first changes usually mean you are spending too much effort steering the model. High debug-loop or no-output rates usually mean the task was underspecified, too broad, or missing key repo context.

### Delivery

`paceflow delivery` shows whether AI-heavy work turned into shipped change.

Key signals:

- Heavy commits
- PR reach rate
- Mainline reach rate
- PR merge rate
- Session-to-commit rate

Heavy commits tell you where AI materially influenced the diff instead of just assisting around the edges. PR reach and mainline reach show whether that work survived review and integration.

### Quality

`paceflow quality` shows whether the code held up after landing.

Key signals:

- Code churn rate
- Bug-after-merge rate
- Revert rate

Churn means accepted code landed but was removed soon after. Bug-after-merge catches merged code that drew later fix work. Reverts are the clearest signal that the workflow created cost, not leverage.

## Status Bands

`--overall` summaries attach `good`, `watch`, and `risk` labels using opinionated thresholds so you can scan a report quickly.

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
- `± LOC commits` in task-grouped delivery: sum of lines added and removed from ingested per-commit git stats (`fact_commit`) for commits attributed to that task branch

### Quality Metrics

- `Code Churn Rate`: share of AI-added lines from heavy commits that reached mainline and were removed from mainline within a 14-day window
- `Bug-After-Merge Rate`: share of merged heavy AI commits that drew at least one later fix-like commit touching the same files within a 60-day window
- `Revert Rate`: share of heavy commits later reverted by a commit body containing `This reverts commit <sha>`

### Grouped Report Weighting

- Repo and weekly rollups use ordinary counts and averages over included sessions or commits
- Provider/model grouped delivery and quality reports work from commit-session attribution rows so unmatched commits can appear as `human`
- Task-grouped session reports use attribution-weighted averages and rates
- Branch-grouped session reports use attribution-weighted averages and rates
- Task-grouped delivery and quality reports exclude non-ticket task keys plus integration branches such as `main`, `staging`, `master`, and `develop`

## Options And Grouping Notes

- `session`, `delivery`, and `quality` share the same filter interface: `--weekly`, `--group-by`, `--from`, `--to`, `--repo`, `--provider`, `--task`, `--branch`, `--model`, and `--limit`
- `session`, `delivery`, and `quality` default to `group-by model` when you do not pass `--group-by` or `--overall`
- `--model` filters the current report to one model without changing the active view
- Use `--group-by branch` or `--branch <name>` when the work lives on literal branches like `fix/...`, `perf/...`, `codex/...`, or `main`
- `--overall` swaps the default model comparison for the rolled-up one-row summary and conflicts with `--group-by`
- Provider `human` means a commit had no matched AI session attribution at all
- Task-grouped rows only show ticket-style task keys such as `ABC-123` and exclude integration branches such as `main`, `staging`, `master`, and `develop`
- `branch` grouping keeps literal branch names and is the right view when task-grouped reports are empty but you know work happened on a non-ticket branch
