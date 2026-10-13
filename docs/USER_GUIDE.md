# Paceflow User Guide

This guide explains how to read `paceflow` reports after you have run:

```bash
paceflow ingest
```

For the shortest setup path, start with the [README](../README.md).

## Report Workflow

Use the reports to answer four practical questions:

```bash
paceflow session
paceflow delivery
paceflow quality
paceflow cost
```

By default, `session`, `delivery`, `quality`, and `cost` compare outcomes by model. Use `--overall` for a rolled-up summary, or `--model <provider/name>` to keep the same report but inspect one model only.

Examples:

```bash
paceflow session --model codex/gpt-5.4
paceflow session --overall
paceflow delivery --group-by branch
paceflow quality --group-by provider
paceflow cost --group-by task
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

### Cost

`paceflow cost` shows token usage and estimated spend per useful output.

Key signals:

- Sessions with token usage
- Priced cost coverage
- Total tokens
- Total API-equivalent cost
- Cost per accepted session
- Cost per accepted LOC
- Cost per mainline change

Cost uses provider-reported token buckets when available. When a provider does not store token usage, Paceflow estimates text tokens from local session text and applies known model pricing where it can identify the model. Unknown models stay unpriced instead of inventing a dollar value.

## Conversations, Usage, Quota And Tasks

The commands below look at the conversations themselves: where tokens and money went, how close you are to subscription limits, what each conversation tried to do, and how long and how much each piece of work took.

```bash
paceflow usage --group-by model        # tokens and API-equivalent $ by tool, model, project, session, agent, day or hour
paceflow quota                         # quota windows: observed % (Codex) or token burn (others)
paceflow quota mark claude --kind limit_hit   # record that you just hit a limit
paceflow sessions --sort cost          # root sessions with subagents rolled in
paceflow tasks --completed-only --sort cost-to-complete
paceflow show <session-id>             # tasks, agents, usage by model, context, commits
paceflow context --group-by dir        # what filled the context and what carrying it cost
paceflow rework                        # later work that changed earlier AI lines
paceflow tui                           # interactive explorer with drill-down
```

All of them take `--days N` (default 30, `0` = all time), `--from/--to`, `--provider`, `--model`, `--repo` or `--here`, `--search`, `--intent`, `--outcome`, `--min-cost`, `--limit` and `--json`.

### Three ledgers that are never mixed

- **Tokens** are what the tool logged per API call (Claude: once per message id; Codex: per `token_count` delta, reasoning counted inside output; OpenCode: per message; Cursor: `tokenCount` when present, otherwise a text estimate marked `~`).
- **Money** is an *API list-price equivalent* computed from [LiteLLM's public model price map](https://github.com/BerriAI/litellm/blob/main/model_prices_and_context_window.json) (`~$`), or the provider-reported cost where one exists (OpenCode). On a subscription this is value consumed, not an invoice. Set how each tool is billed in `~/.vca/billing.json`. `paceflow ingest` refreshes the price map at most once a day (a bundled copy is used offline or with `VCA_PRICING_OFFLINE=1`); `paceflow pricing show <model>` shows how a model resolves and its rates, and `paceflow pricing update` refreshes now. Long-context tiers are applied per API call, as LiteLLM does.
- **Quota** is only shown as a percentage where the tool logs it (Codex `rate_limits`). Claude Code, Cursor and OpenCode show token burn inside assumed windows (Claude 5 h and weekly by default) plus any limit hits you record with `quota mark`.

Every usage event belongs to exactly one session and one task, so grouped totals add up to the overall total.

### Sessions, subagents and tasks

A session is the container; subagent transcripts (Claude `subagents/`, OpenCode child sessions, Cursor sub-composers) are child sessions whose spend rolls up into their root. A **task** is one goal inside a session. Without classification every session is one task. With classification (below) long sessions are split into tasks, and turns that return to an earlier goal rejoin that task.

For each task you get the start time, the completion time and how it was decided (`override` > `jev` > `commit` > `validation_pass`; otherwise not observed complete), wall and active time (idle gaps over 30 minutes removed), cost to completion and cost after completion. Attempts are edit batches closed by a validation run (test, build, lint, typecheck); three or more consecutive failures with the same error signature and no new information from you mark an **unproductive loop**.

`paceflow tasks` compares cost and time per task by model and harness. Compare like with like: filter by `--intent` and look at tasks of similar size.

### Context ledger

`paceflow context` and the session Context tab show what entered each conversation's context and what it cost: the one-time write and the cost of carrying it on every later call until compaction or `/clear`. Item sizes are estimates calibrated to the observed per-call context growth; the remainder is shown as baseline (system prompt and tool schemas the transcript does not log) and residual. Reads are tagged startup/used/unused, and reads whose path matches a line in CLAUDE.md or AGENTS.md show that reference. Cache rebuilds after idle gaps or model switches are listed with their extra cost. Cursor has no composition data.

### Rework and defect evidence

`paceflow rework` lists relations where a later task removed or rewrote lines an earlier AI task added (same file, within 30 days, at least 3 lines), with a rework kind (heuristic, classifier or your label). Origin tasks get an evidence level:

- `L0` AI lines later changed
- `L1` the later request or a later commit reads like a fix
- `L2` the later fix is linked to a bug issue
- `L3` the classifier labels the change a defect correction (confidence ≥ 0.7)
- `L4` you confirmed it

Requirement changes, refinements and continuations are never counted as defects. Spend is split into disjoint categories with precedence loop > correction > requirement change > other, so the parts add up to the total. All of this is correlation, not proof that the AI caused a defect.

### Optional classification with Jev

Labels (intent, outcome, progress, framing, problems, user feedback, rework kind) come from local heuristics, marked `h`. You can opt in to TypeSafe's Jev decision model through OpenRouter, which also splits sessions into tasks and picks completion turns:

```bash
paceflow classify plan --days 7                    # units, tokens, estimated cost; sends nothing
paceflow classify preview <session> --pass segment # the exact redacted request
paceflow classify key set                          # or export OPENROUTER_API_KEY
paceflow classify run --days 7 --max-usd 0.05      # asks for confirmation (or --yes)
paceflow classify status
paceflow classify set <session> --task 0 intent bug_fix     # your labels always win
paceflow classify set-turn <session> 5 1                    # turn 5 starts task 1
paceflow classify set-completion <session> 0 7              # task 0 completed at turn 7 (`none` = not completed)
paceflow classify eval                                      # accuracy against your labels
```

Content levels are `evidence_only`, `user_text`, `conversation` (default; code blocks stripped) and `full`. Secrets, emails and home paths are always redacted. Requests ask for zero data retention and do not fall back to other providers. Answers are cached by content, so re-running only sends what changed; runs stop at `--max-usd` and the monthly budget in `~/.vca/classifier.json`. In the TUI, `K` shows the same estimate and consent screen for the selected sessions and `e` edits labels, task starts and completion turns.

### Work stages and the 12 habits

Every user turn gets a **stage** (what kind of work happened: explore, research, plan, clarify, implement, tests, verify, debug, review, refine, docs, ship) and a **request kind** (what you asked for: new request, your own plan, acceptance criteria, clarification, refinement, correction, requirement change, "how does it work", review, approve, ship). Without classification these come from tool evidence and keywords (marked `h`). `paceflow classify run` asks Jev for them on every turn, in the same request as task splitting. Jev also answers per-turn flags: you asked the agent to ask, minimal change demanded, sources cited, the agent asked questions, done claimed without evidence, unrequested changes, and tests changed to pass.

```bash
paceflow stages                      # cost and turns per stage, common task paths, session kinds
paceflow habits                      # adoption and outcomes for the 12 habits
paceflow habits --habit 7 --list     # the tasks behind one habit, with the evidence for each
```

Habits are judged per task as followed, not followed, n/a (e.g. nothing was built) or unknown:
- 01 know done
- 02 plan first
- 03 make it ask
- 04 ground it
- 05 small slices
- 06 minimal diff
- 07 tests that can fail
- 08 evidence, not reports
- 09 independent review
- 10 defend without AI (signal only)
- 11 self-review (signal only)
- 12 corrections into rules (instruction files changed after repeated corrections)

`paceflow habits` compares tasks that followed a habit with those that did not on completion, corrections, loops and median cost, with sample sizes. The comparison is correlation, not causation. In the TUI, the Conversation tab shows the stage and request of each turn, the Tasks tab shows each task's stage path, Practices lists the habits first, and `e` on a turn corrects its stage or request kind. Your corrections become gold labels for `paceflow classify eval`.

### Interactive explorer

`paceflow tui` opens the explorer (`--classic` opens the previous dashboard). `1`–`7` switch screens, `Enter` drills into the selected row, `Esc` returns exactly where you were, `[`/`]` step through siblings, `u` toggles sessions/tasks, `f` filters, `/` searches, `x` explains a number, `m`/`c` mark and compare, `M` logs a quota limit hit, `R` reloads and `?` lists everything.

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
- `Tokens`: provider-reported tokens, or text-estimated tokens for providers without token counters
- `Cost`: actual provider cost when present; otherwise API-equivalent estimated model cost when pricing is known
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

### Cost Metrics

- `Cost`: actual provider cost when present; otherwise API-equivalent estimated model cost from token buckets and known pricing
- `Tokens`: total input, cache, output, and reasoning tokens captured or estimated for included sessions
- `$/Session`: total priced cost divided by included sessions
- `$/Acc Sess`: total priced cost divided by sessions with accepted changed lines
- `$/LOC`: total priced cost divided by accepted changed lines
- `$/Mainline`: total priced cost divided by heavy AI changes that reached mainline
- `Coverage`: sessions with priced cost divided by sessions with token usage

### Grouped Report Weighting

- Repo and weekly rollups use ordinary counts and averages over included sessions or commits
- Provider/model grouped delivery and quality reports work from commit-session attribution rows so unmatched commits can appear as `human`
- Cost reports work from session-cost events and can be grouped by model, provider, task, or branch
- Task-grouped session reports use attribution-weighted averages and rates
- Branch-grouped session reports use attribution-weighted averages and rates
- Task-grouped delivery and quality reports exclude non-ticket task keys plus integration branches such as `main`, `staging`, `master`, and `develop`

## Options And Grouping Notes

- `session`, `delivery`, `quality`, and `cost` share the same filter interface: `--weekly`, `--group-by`, `--from`, `--to`, `--repo`, `--provider`, `--task`, `--branch`, `--model`, and `--limit`
- `session`, `delivery`, `quality`, and `cost` default to `group-by model` when you do not pass `--group-by` or `--overall`
- `--model` filters the current report to one model without changing the active view
- Use `--group-by branch` or `--branch <name>` when the work lives on literal branches like `fix/...`, `perf/...`, `codex/...`, or `main`
- `--overall` swaps the default model comparison for the rolled-up one-row summary and conflicts with `--group-by`
- Provider `human` means a commit had no matched AI session attribution at all
- Task-grouped rows only show ticket-style task keys such as `ABC-123` and exclude integration branches such as `main`, `staging`, `master`, and `develop`
- `branch` grouping keeps literal branch names and is the right view when task-grouped reports are empty but you know work happened on a non-ticket branch
