# Metric Engine Migration Proposal

## Goal
Migrate the current ad hoc metric code to a reusable counter-based metric engine similar to the attached poker design, while preserving the existing analytics outputs:

- Global metrics: `L4`, `C2`, `L1`, `S4`
- Task metrics: `S2`, `S4`, `S6`, `S9`

The short answer is: **yes, mostly possible**, but not as a direct one-to-one port. The current system has two distinct concerns:

1. deriving analytic facts from raw data and git state
2. aggregating those facts into metrics

The poker model is strong for step 2. In this repo, step 1 is large and cannot be replaced by triggers alone.

## Current Problem
Today, metric logic is split across:

- [`src/metrics/quality.rs`](/home/tadas/Work/paceflow/ai-engineering-analytics/src/metrics/quality.rs)
- [`src/commands/task_stats.rs`](/home/tadas/Work/paceflow/ai-engineering-analytics/src/commands/task_stats.rs)

Each metric mixes:

- SQL loading
- git inspection
- windowing logic
- threshold/status logic
- rendering concerns

That makes metrics harder to add, test, reuse across scopes, or aggregate incrementally.

## Proposed Design
Adopt a **two-stage metric architecture**.

### Stage 1: Fact builders
Keep the existing ingest and commit-association pipeline. Add a new layer that produces canonical metric facts, for example:

- `SessionFact { provider, session_id, user_turns, debug_loop, mid_session_error_paste }`
- `CommitFact { repo_root, commit_id, heavy_ai, reverted, merged_to_mainline }`
- `CommitBudgetFact { commit_id, ai_added_lines, churn_14d_lines }`
- `TaskSessionFact { task_key, provider, session_id, weight, has_commit_within_4h }`

These are the equivalent of the poker hand/action state: stable, derived observations that metric definitions can consume.

### Stage 2: Metric definitions + counters
Introduce Rust equivalents of the poker concepts:

- `MetricType`: `Count`, `Sum`, `Mean`, `Ratio`, `Binomial`
- `MetricCounter`: `metric_id`, `scope`, `numerator`, `denominator`, `squared_numerator`, `samples`
- `MetricTrigger`: optional numerator/denominator increment
- `FactMetric<F>`: metric definition that receives one fact and emits zero or more triggers

The important addition is **scope**. In this repo, metrics are computed at different levels:

- global
- per repo
- per task
- per session

So the counter key should be `(metric_id, scope_type, scope_key)`, not only a player UID.

## Mapping Existing Metrics
### Clean fits
These map well into the new counter model once facts exist.

- `S4`: from `SessionFact`; denominator = sessions, numerator = debug-loop sessions
- `S6`: from `SessionFact`; denominator = sessions, numerator = mid-session-error sessions
- `S9`: from `TaskSessionFact`; denominator = task-linked sessions with known windows, numerator = sessions with commit within 4h
- `L4`: from `CommitFact`; denominator = heavy AI commits, numerator = reverted heavy AI commits
- `C2`: from `CommitFact`; denominator = heavy AI commits eligible for mainline evaluation, numerator = merged commits

### Requires weighted mean support
These do not fit a plain binomial/counter model unless the engine supports weighted sums.

- `S2` task average reprompt count

For `S2`, the fact builder should emit `WeightedValueFact { value=user_turns, weight=session_weight }`, and the metric definition should aggregate:

- numerator += `value * weight`
- denominator += `weight`

This is a weighted mean, not just a ratio.

### Requires precomputed denominators from git analysis
These should not compute directly from raw events inside metric definitions.

- `L1`: churn numerator and denominator come from line-budget analysis over mainline history

`L1` still fits the counter model if Stage 1 emits:

- numerator = churned AI-added lines within 14 days
- denominator = AI-added lines that reached mainline

So the metric definition becomes simple, but the expensive git diff logic stays in fact building.

## Metrics That Should Not Be Trigger-Only
The poker design assumes the statistic can be derived while iterating a hand. That is not true here for:

- line-hash commit attribution
- mainline merge detection
- squash-aware content merge detection
- 14-day churn analysis
- dominant task branch/repo selection

These should remain in specialized builders or materialized facts. The engine should start after those facts exist.

## Recommended Rust Shape
Add a new module, for example `src/metrics/engine.rs`, with:

- `MetricType`
- `MetricScope`
- `MetricCounter`
- `MetricTrigger`
- `MetricDefinition<F>`
- `MetricRegistry<F>`
- `CounterStore`

Then create fact modules:

- `src/metrics/facts/session.rs`
- `src/metrics/facts/task.rs`
- `src/metrics/facts/commit.rs`

And migrate rendering/report assembly to consume counters instead of bespoke structs.

## Migration Plan
### Phase 1: Introduce generic counters
Implement `MetricCounter` and reducers without changing outputs. Re-express current `RatioMetric` as a thin compatibility wrapper over generic counters.

### Phase 2: Extract fact builders
Move existing data loading and git analysis into dedicated fact builders:

- session facts from `events`
- commit facts from `git_commits` + attribution tables
- task-session facts from `commit_task_attributions` + `commit_ai_session_attributions`

### Phase 3: Port simple metrics first
Migrate `S4`, `S6`, `S9`, `L4`, and `C2`. These have clear numerator/denominator semantics and will validate the model quickly.

### Phase 4: Add weighted mean support
Extend the engine for `S2` with weighted means and optional variance fields, similar to `PokerStatisticCounter.squared_numerator`.

### Phase 5: Port `L1`
Keep the current git-heavy analysis, but make it emit `CommitBudgetFact` records that the metric engine reduces into `L1`.

### Phase 6: Replace report assembly
Update `stats.rs` and `task_stats.rs` to query counters and derived facts instead of calling custom metric builders directly.

## Benefits
- One aggregation model for repo, task, and session metrics
- Easier testing: fact builder tests separate from metric definition tests
- Easier incremental updates and caching
- Cleaner path to adding new metrics without editing report-specific code

## Risks
- A naive port will overfit to event triggers and break git-derived metrics
- Weighted task metrics need first-class support
- Some “metrics” are actually report annotations or selection rules, not counters

Examples:

- task grade thresholds
- dominant branch selection
- `vs staging` diff output

Those should remain report logic, not metric definitions.

## Recommendation
Adopt the new design, but **only as a counter engine sitting on top of a fact-building layer**. Do not try to force the entire analytics pipeline into a pure trigger model. The right split is:

`raw data -> attribution/fact builders -> generic counters -> reports`

That preserves the strengths of the poker design while fitting the much more stateful git and attribution logic in this repository.
