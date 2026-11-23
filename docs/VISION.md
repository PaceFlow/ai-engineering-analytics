# Vision, Goals and Feature Set

Status: draft for discussion (November 2026). Based on the team huddle of 2026-11-23 and the current state of the codebase.

## Vision

> **VCA shows whether changes to your AI coding setup are making delivery faster, cheaper and better, and which change caused it.**

Today VCA measures one person's coding sessions: what they did and what it cost, by session and by commit. The new direction is to measure the effect of changes to an AI engineering setup, such as a new skill, edited instructions, or a rule like "run integration tests before every merge", on throughput, cycle time, defects and cost.

## Use cases

| # | Who | Question | Constraints |
|---|---|---|---|
| 1 | **Our autonomous engineering loop** (Multica agents that find, implement and test tasks) | Did adding this skill or tweaking these instructions improve the loop? What does a task cost in tokens, time and money? Are we mostly fixing defects or building features? | Multica starts a new session for each conversation, so session-level numbers are misleading. We control the loop, so we can run real A/B experiments, not just before/after comparisons. |
| 2 | **Product factory clients**: people who build their products in a repo configured with our skills | Is it getting better? Is cycle time going down, features per week going up, defects going down? | Runs on the client's repo and machines, not ours. A terminal UI isn't acceptable; the output must be visual. |
| 3 | **Our team, supporting those clients** | Which skill or instruction change caused cycle time to go up, or defects to go down? | Compares many repos before and after each change. Observational only; we can't randomize clients. |
| 4 | Existing individual developer and Paceflow users | Personal session, cost and quality numbers | Keep working, but no longer the main focus. |

Use cases 2 and 3 are one tool with two audiences (client view and agency view). Use case 1 uses the same engine with experiment support on top.

## Goals

1. Make the **task** the unit of analysis, not the session.
2. Make every **setup change** visible and comparable against outcome metrics.
3. Separate **feature, defect, chore and rework** work so mix and quality trends are meaningful.
4. Give non-technical clients a **visual report** they can understand without a terminal.
5. Attribute changes **honestly**: show uncertainty, flag confounders, and report unknowns instead of guessing.

## Core ideas

### Tasks, not sessions
One task can span many sessions, several branches and several PRs. Session metrics become an input to task metrics, not the headline.

### Setup changes as interventions
Skills, `CLAUDE.md` and `AGENTS.md`, `.claude/`, hooks and MCP configuration mostly live in the repository. Their git history is a free, dated timeline of interventions. Every metric chart can mark when each setup change landed.

### Work classification
"Defects dropped" and "we're mostly fixing bugs" both depend on classifying work. Where evidence exists, a defect links back to the work that introduced it. Where it doesn't, the defect is reported as unlinked; the link is never inferred without evidence.

### Careful attribution
Before/after comparisons on small samples mislead. Reports show ranges, split by task type, and flag periods where several setup changes landed together. For our own loop, we support running two configurations side by side on comparable tasks.

## Feature set

### Data layer (CLI)
- **Keep:** providers (Claude Code, Codex, Cursor, OpenCode), ingestion, commit association, cost.
- **New: task grouping.** Group sessions into tasks by branch, issue ID in the branch or commit message, worktree, or Multica task ID. Optionally connect a task tracker: GitHub Issues first, then Linear or Jira, plus Multica if it exposes an API.
- **New: config-change events** from the git history of skill and instruction files. Each event records the files, the date, and a short diff summary.
- **New: work classification** from conventional commits, branch prefixes, issue labels, and fix/revert patterns.
- **New: a stable JSON/SQL output format** that all views read. This extends the existing `event_*` / `view_*` layering. The CLI provides the data; separate views turn it into conclusions.

### Metrics
- **Delivery:** cycle time per task (first session to merge), throughput (features per week), lead time, PR review cycles.
- **Quality:** defect rate, escaped defects, rework and revert rate, test-gate failures.
- **Efficiency:** tokens, cost, wall-clock time and agent-hours per task, human interventions per task.
- **Mix:** feature vs defect vs chore share over time.

### Views
- **Client report:** a visual page that can be shared (static HTML first, a local web view later), showing trends with markers for each setup change.
- **Agency view:** compares repos and setup changes across clients (for example, "skill X added in 4 repos: median cycle time +18%").
- **Experiment view:** compares loop configuration A vs B on similar tasks, showing cost, success rate and defects.

### Integrations
- **Paceflow custom metrics:** push task-level metrics to Paceflow's custom-metrics API. Those metrics are per person by default and configured by admins. Paceflow gets real data, and VCA stays focused on collecting and analyzing it.

## Phasing

1. **December: our own loop.** We are the customer, so feedback is fastest. Deliver task grouping (fixing Multica session fragmentation), config-change events, work classification, and a cost/time/outcome-per-task report.
2. **Client report:** a static HTML report from the same data, installed in one client repo.
3. **Agency comparison across repos and A/B experiments for the loop.**
4. **Then decide** whether VCA stays one CLI with several views or splits into separate tools.

## Open questions

- **Third use case:** is it the agency diagnosis view (use case 3), or something else?
- **Task source of truth:** Multica, an issue tracker, or inferred from git only?
- **Client data:** does anything leave the client's machine? This decides between a local static report and an upload to a hosted dashboard.
- **Defect definition:** issue labels, fix commits, or reported bugs?
- **Product identity:** does this stay `vibe-coding-analytics` with Paceflow as a sync target, or become part of Paceflow's offering?
