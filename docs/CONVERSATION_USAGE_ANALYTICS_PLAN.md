# Plan: Conversation, usage, quota and rework analytics for AI Engineering Analytics

## Context

The workspace (`crates/vca` library + `vca` binary, `crates/paceflow` binary + backend sync/schedule/hooks) ingests local sessions from Codex, Claude Code, Cursor and OpenCode, associates them with git commits by line hashes, and reports session/delivery/quality/cost KPIs through CLI reports and a 4-tab ratatui dashboard. The user wants to understand their *own* AI coding conversations: what each conversation is trying to do and whether it progresses, where tokens/money/subscription quota go (by model, tool, session, subagent), where work is repeated, and how to tighten their agentic coding loop. They want one integrated release: correct usage accounting, deterministic session structure (tasks, attempts, validation, subagents), an **optional** Jev classifier through OpenRouter, a much deeper terminal UI with drill-down, and a rethink of the delivery/quality metrics. Team/backend features must keep working but are not expanded.

This document is the plan only. Nothing has been implemented.

---

## 1. Findings from the current code

Verified in the repo at `2b6a9ad`. Paths are relative to `crates/vca/src/` unless noted.

### 1.1 Accounting is wrong today, before any new feature

| # | Finding | Evidence | Effect |
|---|---|---|---|
| A1 | **Claude usage double-counted.** Claude Code writes one JSONL line per content block, each repeating the same `message.id`/`requestId` and `usage`. `extract_claude_usage` creates a usage row per line, with no dedupe. | `providers/claude/shared.rs:273`, `providers/claude/mod.rs:117-134` | Inflated Claude tokens/cost, probably by 1.5–3× on tool-heavy sessions (a local transcript showed 36 of 50 message ids repeated). |
| A2 | **Codex reasoning billed twice.** `cost_from_pricing` bills `output + reasoning` at the output rate. Codex `output_tokens` already includes `reasoning_output_tokens`. | `cost/pricing.rs:24`; fixture: total 11599 = 11042 + 557, reasoning 404 ⊂ 557 | Codex cost over-estimated. OpenCode reports reasoning separately, so its rule must differ. |
| A3 | **Sessions freeze after their first ingest.** `session_exists` short-circuits re-parsing. It also ignores provider. Usage is only backfilled when absent. | `db/mod.rs:720` (no provider filter), `providers/codex/mod.rs:137`, `claude/mod.rs:67-101`, `opencode` :159-192 | A session ingested while it was still running never gets later messages or usage. Codex keeps the cumulative total it had at first ingest. Quota investigations would miss exactly the long sessions that burn quota. |
| A4 | **Usage has no real timestamp.** Codex stores one row at session *start*. Claude and OpenCode store rows at session *end*. | `codex/mod.rs:338`, `claude/mod.rs:121`, opencode `usage_ts` | Usage can't be placed on a timeline. Spikes and 5h/weekly windows can't be analysed. |
| A5 | **One model per session.** Codex uses the first `turn_context.model`. Claude uses the last `message.model`. Cost uses `MAX(u.model_name)`. | `codex/mod.rs:179`, `claude/shared.rs:394`, `analytics.rs:891-1024` | Mixed-model sessions are mispriced, and model switches are invisible. |
| A6 | **Cursor tokens are guesses.** Tokens are `chars/4` of visible text and the model is usually `cursor/default`, which is unpriced. | `providers/cursor/mod.rs:708,750`, `cursor/shared.rs:1613` | Cursor spend is unknowable from current evidence. Bubble `tokenCount` may exist upstream but isn't verified in our fixtures. |
| A7 | **Pricing matches by substring in a hardcoded table.** Unknown models are unpriced. Cursor-style names (`claude-4-sonnet`) miss. There is no pricing version or date. | `cost/pricing.rs` | Estimates can't be reproduced or audited later. |
| A8 | **Cost report drops sessions without commits** (`HAVING commit_count > 0`). | `analytics.rs:1870` | Planning, research and review spend disappears from grouped cost output. |

### 1.2 Structure that is missing for the user's questions

- **No subagent or parent/child data.** Claude `<session>/subagents/agent-*.jsonl` is deliberately skipped (`claude/shared.rs:195`, test at :831). `isSidechain`, `parentUuid`, `agentId` and Task/Agent tool inputs are ignored. OpenCode `session.parent_id` is not read. Cursor `subagentComposerIds`/`subComposerIds`/`isBestOfN*` are not read. Codex has no fork/sub-session link (newer `session_meta.source` shapes are unverified).
- **No tool-call table.** Tool calls are only mined for file writes, so validation runs (tests/builds), their exit codes, reads/searches, web calls and agent delegations are not stored. Codex `custom_tool_call apply_patch` (the current format) produces no `accepted_change` rows (`codex/mod.rs`; only change-intel catches it).
- **No turns, tasks or attempts.** `fact_session_message` holds flat text. The only "loop" signal is the keyword-based `debug_loop_flag` (same error signature in ≥5 user turns, `analytics.rs:3433`) and `mid_session_error_paste_flag`.
- **Quota evidence exists but is thrown away.** Every Codex `token_count` event carries `rate_limits.primary{used_percent, window_minutes:300, resets_at}`, `secondary{… 10080 …}`, `credits`, `plan_type` (present in `tests/fixtures/regression/home_template/.codex/...`). Claude Code and Cursor have **no** local quota telemetry. OpenCode's `cost` is a provider-reported figure that may be 0 on subscription-backed providers.
- **Raw fields that are available but unused:** Claude `gitBranch`, `requestId`, `message.id`, `stop_reason`, `isApiErrorMessage`, `service_tier`, `speed`, `requestedModel`. Codex `git.branch`, `turn_id`, `effort`, `task_started/complete`, `turn_aborted`, `context_compacted`, `last_token_usage`, `model_context_window`. OpenCode `title`, message `parentID`, `time.completed`, `mode/agent`, `finish`, `error`, `providerID`, `tool` parts with status, `step-finish` tokens/cost. Cursor `createdOnBranch`, `name`, `unifiedMode`, `contextUsagePercent`, `todos`.

### 1.3 Metrics and reports

- Session KPIs (`analytics.rs:607-889`): avg prompts, time to first accepted change (idle gaps > 30 min removed), debug-loop rate, error-paste rate, commit within 4h, no-output rate. All of them treat a conversation as "should produce code".
- Delivery/quality KPIs only count **heavy-AI commits** (AI share ≥ 0.5): mainline reach (incl. squash content match), PR reach/merge, lead time, 14-day churn, 60-day "bug-after-merge" (fix-keyword commit or bug-labelled issue whose removed hashes overlap AI-added hashes), revert rate.
- Session ↔ commit association has **no time constraint**: a session can match an earlier commit by hash (`matcher.rs`). The existing "task" = ticket key from branch name, one per commit. **In this plan that concept is called *ticket*.** The CLI flag `--group-by task` keeps working and is documented as ticket, and the new UI labels it "Ticket". In this plan, **task** means a unit of work inside a session (§3.5a).
- Thresholds (good/watch/risk) live in `commands/{session,delivery,quality}.rs`.

### 1.4 TUI and CLI

- `commands/tui.rs` (1827 lines): tabs Verdict/Sessions/Delivery/Quality, 7/30/90-day window, group-by Model/Provider/Task/Branch. Data is loaded **synchronously** (13 queries per reload on the UI thread). There is **no Enter/drill-down, no detail view, no scrolling offset** (rows past the viewport are clipped), no filters, no sort, no search. Tests are 9 `TestBackend` render tests. paceflow reuses it via `AppContext`.
- CLI `ReportArgs`: `--weekly --group-by --from --to --repo --all-projects --provider --task --branch --model --limit`. Repo is auto-scoped to the cwd git root (`report_scope.rs`). SQL is built with `sql_literal` string interpolation, not parameters.

### 1.5 Storage and compatibility constraints

- One SQLite DB `~/.vca/vca.db`, `SCHEMA_VERSION = 1` in `PRAGMA user_version`. `validate()` rejects versions outside `0..=1` (`data_home.rs:145`). Evolution is `CREATE IF NOT EXISTS` + `add_column_if_missing` (`db/mod.rs:661-718`). There are no stepwise migrations.
- `event_*` tables are fully rebuilt (DELETE+INSERT) on ingest. **They are paceflow's backend contract**: `crates/paceflow/src/sync.rs:398+` selects named columns and builds `json!` payloads. Change detection is `md5(serialized event)`, so changing a value re-uploads that event, and renaming or dropping a column is a runtime SQL error.
- Claude Code deletes old transcripts after `cleanupPeriodDays` (default 30). **The DB is the only long-term copy of older sessions**, so migrations must never delete data they cannot re-derive from sources.

---

## 2. Product behavior and scope

### 2.1 What the release delivers

**Core unit: the task.** A session is a container. Users often run one long session for several things, so a session is split into **tasks**: one goal each, with a start, a completion time, cost to complete and time to complete. Jev does the split and the completion judgment. Without Jev, each session is one task. Cost per task and time per task, broken down by model, harness, intent and project, are headline measures.

1. **Correct, timestamped, per-event usage accounting** with explicit evidence grades, across all four tools and including subagents.
2. **Three separate ledgers that are never blended:**
   - **Tokens** (observed or estimated).
   - **Money**: reported cost, or list-price API-equivalent estimate.
   - **Quota**: observed provider percentages where they exist; otherwise only token burn inside windows, clearly labelled as such.
3. **Deterministic conversation structure:**
   - **Session tree**: root plus subagents/forks.
   - **Turns/exchanges**: one user message and everything until the next.
   - **Work tasks**: a contiguous span pursuing one goal.
   - **Attempts**: an edit batch plus its validation or feedback.
   - **Validation runs**: test, build, lint or typecheck commands with results.
   - **Model changes** and **compactions**.
   - **Cross-session relations**: later work touching earlier AI output.
4. **A context ledger** (§3.6a) showing what fills each conversation's context and what it costs to carry. It covers the baseline (instructions, tool/MCP definitions, skills) and every tool result, file or directory read, and subagent return, each with a one-time write cost and a per-call carry cost. It also covers cache rebuilds, and links reads to the instruction-file lines that reference them.
5. **Optional Jev classification**: intent, outcome/progress, problems, user-feedback stance and rework relation. It is opt-in, previewable, budget-capped, cached and overridable. A local heuristic baseline fills the same label slots (clearly marked `heuristic`), so every screen works without Jev.
6. **A navigable TUI**: Overview, Sessions, Groups, Session detail (Conversation, Tasks, Agents, Usage, Context, Code & Delivery), Usage & Quota (incl. Context & Tools), Delivery & Rework, Compare and Practices. It keeps a navigation stack that preserves filters, sort and cursor.
7. **Revised delivery/quality/rework metrics** segmented by intent, with an evidence ladder instead of "no-output = bad".
8. **Matching CLI subcommands** (`usage`, `quota`, `context`, `sessions`, `tasks`, `show`, `classify`, `rework`) for scripting and tests. Existing commands keep working.

### 2.2 Out of scope (this release)

- Team or cross-member analytics, cross-device reconciliation, and new paceflow sync event types. The paceflow payloads may change only where accounting fixes change values (§7.4).
- Fetching billing or quota data from provider web APIs (Anthropic console, OpenAI usage API, Cursor dashboard). This is listed as a follow-up, and the UI shows where it would close gaps.
- A browser dashboard.
- Uploading transcripts anywhere except the opt-in Jev path.

### 2.3 Product principles (enforced in code and UI)

- **Evidence grade on every number.** Each one is `observed`, `reported`, `estimated`, `allocated`, `inferred` (AI or heuristic), or `missing`. The UI renders the grade (§5.2 glyphs).
- **Progress ≠ delivery.** Conversation progress (inferred) and delivery evidence (observed git/PR facts) are separate columns, never merged into one score.
- **Correlation language only.** "Later fix touched AI-written lines" is never shown as "AI caused bug".
- **Additive totals stay additive.** Every usage event belongs to exactly one session and exactly one task. Overlays such as "downstream correction cost" are marked non-additive and are never summed into totals.

---

## 3. Ingestion, data model, accounting and migration

### 3.1 Ingestion lifecycle changes (all providers)

**Replace "skip if session exists" with fingerprint-based re-ingest.**
- Add `metadata_sessions.source_fingerprint TEXT` (mtime+size, plus an md5 of the last 4 KB for JSONL; `lastUpdatedAt` for Cursor composers; `time_updated` + message count for OpenCode) and `ingest_version INTEGER`.
- On ingest, if the fingerprint or `ingest_version` differs, run `replace_session_facts(provider, session_id)` in one IMMEDIATE transaction. It deletes and re-inserts that session's messages, usage events, tool calls, turns and rate-limit snapshots, all keyed by **natural keys** (below). Code-change rows keep using the existing `reconcile_source_tool_writes` path so dirty-hash tracking still works.
- Make `session_exists`/`sessions_present` provider-scoped (`db/mod.rs:720,748`). Fix every call site.
- **Never delete facts for a session whose source file has disappeared.** Mark it `metadata_sessions.source_status = 'source_missing'` and keep the rows.

**Natural keys** (new `UNIQUE` indexes; legacy MAX+1 indices remain for ordering):

| Table | Natural key per provider |
|---|---|
| usage | Claude: `message.id` (fallback `requestId`). Codex: `turn_id:seq` of each `token_count` delta. OpenCode: message id (+ step part id when step-level). Cursor: bubble id, or `estimate:<composerId>` |
| message | Claude `uuid`; Codex line ordinal; OpenCode message id + part id; Cursor bubble id |
| tool call | Claude `tool_use.id`; Codex `call_id`; OpenCode part id; Cursor bubble id + tool index |

### 3.2 Provider-specific ingestion work

**Claude Code** (`providers/claude/*`)
- Dedupe usage by `message.id`. Keep the **last** line's usage for that id, because streaming lines can update `output_tokens`.
- Per-event `ts` (the line timestamp), `model` (that message's `message.model`), `service_tier`, `speed`, `stop_reason`.
- Read `<session>/subagents/agent-*.jsonl` as **child sessions**:
  - `session_id = <parent>/agent-<agentId>`, `parent_session_id = <parent>`, `agent_kind='subagent'`.
  - `agent_type` comes from the parent's Task/Agent `tool_use.input.subagent_type`, joined via `agentId`/`sourceToolAssistantUUID`.
  - Delete the skip test and replace it with `ingests_subagent_files_as_child_sessions`.
- Lines with `isSidechain=true` in the main file (older format) become a synthetic child session `<sid>/sidechain-<n>`, grouped by sidechain root `parentUuid` chain.
- `agent_progress` lines inside the parent duplicate subagent content. Use them only when the subagent file is absent, and always dedupe usage by `message.id` **across the parent and child within one session tree** (a tree-level `seen` set).
- Capture `gitBranch` (first/last), `version`, `entrypoint`, `permissionMode`, `isApiErrorMessage` (→ tool/error events), `summary` records (→ title), and compaction boundaries (`compact_boundary` / `isCompactSummary`).
- Tool calls: every `tool_use`/`tool_result` pair becomes a `fact_session_tool_call` (name, input summary, `is_error`, Bash exit code parsed from result text, duration from timestamps).

**Codex** (`providers/codex/mod.rs`)
- Emit one usage event per `token_count` from `info.last_token_usage`. The fallback is the delta of `total_token_usage` from the previous event; also reconcile that the sum equals the final total. Each event gets its own `ts` and the model from the latest preceding `turn_context`.
- Store `reasoning_in_output = 1` for Codex (fixes A2; §3.4).
- Store `rate_limits` from every `token_count` into `fact_rate_limit_snapshot` (§3.3).
- Read `git.branch`, `originator`, `source`, `cli_version`, per-turn `model`/`effort`/`approval_policy`/`sandbox_policy`, `task_started/task_complete/turn_aborted` (turn boundaries and aborts), and `context_compacted`.
- Parse `custom_tool_call` (`apply_patch`) into accepted changes (unify with `change_intel` parsers `apply_patch.rs`) and tool calls. `function_call exec_command` + output becomes a tool call with `exit_code` from "Process exited with code N".
- Parent link: if `session_meta.payload.source` is an object naming a parent/subagent (newer Codex), set `parent_session_id`. **Stage-1 task:** capture a real current Codex subagent rollout into a fixture. Until that's done, Codex child linkage shows as `unknown` (not "none").

**OpenCode** (`providers/opencode/mod.rs`)
- `session.parent_id` → parent link (`agent_kind='subagent'`); `session.title` → title.
- Per-message usage at the message `time.created`/`time.completed`, with `providerID` stored. `cost` → `reported_cost_usd`.
  - **Do not label this "actual charge"** when `providerID` maps to a subscription-backed provider (configurable list, default: `github-copilot`, `anthropic` when auth is OAuth unknown → `billing_mode='unknown'`).
- Tool parts (`type='tool'`, `state.status`, `state.input`, `state.output` truncated) → tool calls. `step-finish` tokens are used only if message-level tokens are absent (avoid double counting: message tokens are the sum of steps).
- Schema probing: check `PRAGMA table_info` for optional columns and degrade gracefully, since the test schema lacks them.

**Cursor** (`providers/cursor/*`)
- Add a probe for bubble `tokenCount{inputTokens,outputTokens}` and composer `usageData`. When present, use `token_source='reported'`; otherwise keep the `chars/4` estimate with `token_source='estimated_text'`.
- Per-bubble model: `modelInfo.modelName`, falling back to composer `modelConfig.modelName`. `"default"`/`"auto"` is stored as model `cursor/auto` with `pricing='unpriced'` and an explicit "Cursor auto model — cost unknown" gap.
- Parent links: `subagentComposerIds`, `subComposerIds`, `isBestOfNParent/Subcomposer` → `agent_kind` `subagent` / `best_of_n`. Also read `createdOnBranch`, `name` (title), `unifiedMode`/`isAgentic`, and `contextUsagePercent` (stored as a context-pressure snapshot).
- WAL caveat: compute the fingerprint from `state.vscdb` **and** `state.vscdb-wal` mtime/size.
- Tool calls from `toolFormerData` (name, status). Terminal commands give the exit code when present.

### 3.3 Schema v2 (new and changed tables)

Introduce a real migration runner (`db/migrations.rs`):
- A `schema_migrations(version, applied_at)` table and ordered `fn migrate_vN(conn)` steps, run under the existing `analytics.lock`.
- `SCHEMA_VERSION = 2`, and `validate()` accepts `0..=2`.
- Old binaries reject v2 databases. vca and paceflow ship from one workspace version, so the release notes say "upgrade both".

**Changed tables (additive columns only):**
- `metadata_sessions`: `parent_session_id`, `root_session_id`, `agent_kind` (`main|subagent|sidechain|best_of_n|fork|unknown`), `agent_type`, `title`, `git_branch`, `client_version`, `entrypoint`, `source_fingerprint`, `source_status`, `ingest_version`.
- `fact_session_usage` (kept as the usage-event table, so nothing in paceflow touches it): `usage_key`, `turn_index`, `token_source` (`reported|estimated_text|legacy`), `reasoning_in_output`, `service_tier`, `provider_route` (OpenCode providerID / API host), `reported_cost_usd` (renamed semantics of `actual_cost_usd`; keep the old column populated for compatibility), `ts_quality` (`event|session_start|session_end`).
- `fact_session_message`: `turn_index`, `message_key`, `is_compaction_summary`, `chars`.

**New fact tables:**
```sql
fact_session_turn(provider, session_id, turn_index, user_message_index, started_at, ended_at,
  model_name, user_chars, assistant_chars, tool_calls, edit_calls, validation_runs,
  validation_failures, aborted_flag, compaction_flag, PRIMARY KEY(provider, session_id, turn_index))

fact_session_tool_call(provider, session_id, call_key, turn_index, ts, ended_ts, tool_name,
  tool_category,      -- read|search|edit|exec|validate|web|agent|plan|mcp|other
  validation_kind,    -- test|build|lint|typecheck|run|NULL (regex classifier, §3.5)
  command_head,       -- first 200 chars of the command (local only)
  status,             -- ok|error|rejected|interrupted|unknown
  exit_code, error_signature, child_session_id, paths_touched,
  UNIQUE(provider, session_id, call_key))

fact_rate_limit_snapshot(provider, session_id, ts, limit_id, window_kind,   -- primary|secondary|credits
  window_minutes, used_percent, resets_at, plan_type, credits_balance, source, -- codex_token_count|manual
  UNIQUE(provider, limit_id, window_kind, ts, session_id))

fact_quota_observation(id, provider, observed_at, kind,  -- limit_hit|reset|plan_change|note
  plan_label, note)                                      -- entered by the user (CLI/TUI)
```

**New derived tables** (rebuilt by `refresh_session_structure`, deterministic):
```sql
derived_task(provider, session_id, task_index, root_session_id,
  segmentation_source,          -- jev|override|whole_session (no Jev)
  first_turn, completion_turn, last_turn,          -- completion_turn NULL = not completed / unknown
  started_at, completed_at, last_activity_at,
  completion_source,            -- jev|override|commit|validation_pass|last_activity (upper bound)|none
  wall_minutes_to_complete, active_minutes_to_complete,   -- active = minus idle gaps > 30 min
  agent_minutes, user_minutes,  -- time in assistant/tool work vs waiting on the user
  turns, attempts, validations, validation_failures, edits_lines, files_touched,
  primary_model, models_json, harness,             -- harness = tool (claude_code|codex|cursor|opencode)
  tokens_total, cost_to_complete_usd, post_completion_cost_usd, subagent_cost_usd,
  content_digest)               -- cache key for classification

derived_task_turn(provider, session_id, turn_index, task_index, assignment_source, confidence)
  -- tasks can interleave ("back to the earlier bug…"), so a task is a set of turns, not a range

derived_attempt(provider, session_id, task_index, attempt_index, start_ts, end_ts,
  edit_calls, lines_changed, validation_kind, validation_result,  -- pass|fail|mixed|none
  ended_by)   -- validation|user_feedback|session_end|next_edit

derived_session_relation(from_provider, from_session, from_task, to_provider, to_session, to_task,
  relation_kind,   -- touches_earlier_ai_lines|same_ticket|same_branch_followup|resumed|subagent_of
  overlap_lines, overlap_files, gap_hours, evidence_json)

derived_usage_allocation(provider, session_id, usage_key, task_index, turn_index, root_session_id)
  -- 1:1 assignment of each usage event: the additivity guarantee
```

**Classifier tables** (§4.8): `classifier_run`, `classifier_request`, `classification_result`, `classification_override`, `classification_gold`.

**Views**
- `view_usage_event`: usage ⋈ allocation ⋈ pricing ⋈ session tree.
- `view_session_rollup`: self vs tree totals, cost by grade, task count, label summary.
- `view_quota_window`.
- `view_label_effective`: override > confident Jev > tentative Jev > heuristic > unclassified.

`event_*` tables are untouched in shape. Their values are fed from corrected usage (§7.4).

### 3.4 Accounting rules

**Token normalization (one place: `cost::normalize_usage`).** Store raw provider numbers and compute normalized fields:

| Field | Codex | Claude | OpenCode | Cursor |
|---|---|---|---|---|
| fresh_input | input − cached | input_tokens | tokens.input | est/`inputTokens` |
| cache_read | cached_input | cache_read_input | cache.read | 0 |
| cache_write | 0 | cache_creation_input | cache.write | 0 |
| output_billable | output (reasoning **inside**) | output | output + reasoning | est/`outputTokens` |
| reasoning (info only) | reasoning_output | 0 / unknown | reasoning | — |

`pricing.rs::cost_from_pricing` bills `output_billable` once; `reasoning` is never added again. A regression test uses the Codex fixture numbers.

**Pricing table as data.**
- Move prices into `crates/vca/pricing/pricing.toml`, embedded with `include_str!`. Each entry carries `pricing_version`, `effective_from` and model aliases: exact model ids plus normalized aliases such as Cursor's `claude-4-sonnet` → `claude-sonnet-4`. Use exact/alias match before any substring fallback, and record `price_match = exact|alias|family|none`.
- An optional user override at `~/.vca/pricing.toml`.
- Every estimate stores `pricing_version`, so old numbers can be explained.

**Three ledgers:**
1. **Token ledger.** Σ normalized tokens per usage event. Grade: `reported` / `estimated_text` / `legacy` (pre-v2 rows that couldn't be re-derived).
2. **Money ledger**, two columns that are never summed together:
   - `reported_cost_usd`: OpenCode message cost, plus Jev `usage.cost` for classifier spend.
   - `api_equiv_usd`: tokens × list price, labelled "API list-price equivalent (estimate)".
   - `billing_mode` per provider/route from `~/.vca/billing.toml` (`subscription|api|mixed|unknown`, default `unknown`, with prompts in the UI to set it). With `subscription`, API-equivalent is shown as "value consumed", not "spent". Optional `monthly_fee` per plan gives an amortized-fee line, kept separate from token-based numbers.
3. **Quota ledger** (§3.6).

**Attribution to sessions and agents (no double counting):**
- Usage is owned by the session whose transcript produced it. A subagent's tokens belong to the subagent session.
- `self` = own events. `tree` = self + descendants. List screens default to **root sessions with tree totals** and an `agents` column; the Agents view shows the self split. Global totals sum `self` over all sessions, so each event is counted exactly once.
- Each usage event is assigned to exactly one turn and one task (`derived_usage_allocation`): the turn whose time span contains the event; events before the first user message go to turn 0 ("setup/system").

### 3.5 Deterministic structure derivation (`analytics/structure.rs`, new)

**Turns.** A turn starts with each non-meta user message (tool results aren't user turns). Assistant text, tool calls and usage events until the next user message belong to it. Codex `task_started`/`task_complete` and OpenCode `parentID` refine the boundaries.

**Validation classifier** (`classify_command(cmd) -> Option<ValidationKind>`), using regex tables in code with unit tests:
- test: `cargo test|nextest`, `pytest`, `go test`, `npm|pnpm|yarn (run )?test`, `jest`, `vitest`, `mvn test`, `gradle test`, `rspec`, `dotnet test`
- build: `cargo build|check`, `npm run build`, `tsc` (without `--noEmit`), `make`, `go build`
- typecheck: `tsc --noEmit`, `mypy`, `pyright`
- lint: `cargo clippy`, `eslint`, `ruff`, `golangci-lint`, `cargo fmt --check`, `prettier --check`
- run: `cargo run`, `node`, `python <file>`, `curl localhost`

Result = exit code (0 = pass) when known, else `unknown`. `error_signature` = normalized first error line (reuses the `analytics.rs:3481` normalization).

**Attempts.** Inside a task, an attempt is a maximal run of edit tool calls, closed by the next validation run (result recorded), the next user message (`ended_by=user_feedback`), or the task end. Validation without a preceding edit is a "check", not an attempt.

**Tasks: the core unit of work** (see §3.5a). A session is the container; a task is one goal pursued inside it.

- **Without Jev:** each session is exactly one task (`segmentation_source = whole_session`). No guessing is involved.
  - An opt-in config `segmentation.fallback = "hard_boundaries"` also splits at `/clear` and commits made between turns. It is off by default, because it splits only by convention and is not a judgment about tasks.
- **With Jev:** Jev assigns every user turn to a task (new task / continue / resume an earlier open task / not task work) and determines the turn where each task was completed (§4.3 Q14, Q16).
- **Manual overrides** (`split here`, `merge with previous`, `move turn to task N`, `completed at turn N`) always win.

### 3.5a Task timing and cost

**Start:** the timestamp of the task's first user turn.

**Completion**, in order of preference; `completion_source` is stored and shown:
1. A manual override.
2. Jev `completion_turn` with confidence ≥ the band threshold. `completed_at` = the end of the last assistant/tool activity in that turn.
3. Commit evidence: the first commit (made after the task started) containing ≥ 50% of the task's AI-added line hashes, if a passing validation precedes it. `completed_at` = commit time.
4. The last passing validation after the last edit of the task.
5. Otherwise `completed_at` = NULL. The task is shown as *not observed complete*, with `last_activity_at` offered only as an **upper bound** and labelled as such.

**Durations:**
- `wall_minutes_to_complete` = completed_at − started_at.
- `active_minutes_to_complete` = the same, minus idle gaps > 30 min (reuses the existing idle-gap logic).
- `agent_minutes` = Σ (assistant/tool activity spans), and `user_minutes` = Σ (gaps between the last assistant output and the next user message, capped at 30 min). Together they answer "was I waiting on the agent, or was the agent waiting on me".
- For interleaved tasks, durations are computed over the task's own turns only.

**Cost:**
- `cost_to_complete_usd` = Σ allocated usage events of the task's turns up to and including `completion_turn`, including subagents spawned in those turns (tree).
- `post_completion_cost_usd` covers the task's turns after completion (polish, follow-ups). Kept separate so "done, then kept going" is visible.
- Both are exact sums of `derived_usage_allocation`, so tasks remain additive with session and global totals.

**Breakdowns:**
- Cost, time and completion rate per task, by model (primary, or any-model split by events), harness (tool), intent, project, framing score, subagent use, size (lines changed band), and week.
- Comparisons across models or harnesses are **stratified** (same intent and size band), with n per cell and a "low n" guard. A raw median across mixed work would otherwise mislead.

**Loops vs iterative validation (deterministic):**
- **Unproductive loop**: ≥ 3 consecutive failing validations in one task with the **same** `error_signature` and no new user information in between (no user message > 20 words, no new files touched).
- **Converging iteration**: consecutive failures whose signature changes or whose failing-test count decreases, ending in a pass.
- **Thrash**: the same lines added then removed within one task (line-hash +/− cancel ≥ 20 lines).

These replace the keyword `debug_loop_flag` as the primary signal. The old flag stays in `event_session_quality` for sync compatibility.

**Cross-session relations.** For each later session/task, `-` line hashes (from `fact_session_code_change_line_hashes`, side `-`) are matched against `+` hashes of **earlier** sessions in the same repo and path within 30 days, giving `touches_earlier_ai_lines` with `overlap_lines`. Also add `same_ticket` (same ticket key via commits), `same_branch_followup` (same `git_branch`, gap < 48h) and `resumed` (Claude `--resume` / same slug / OpenCode parent). Commit-level overlap is already computed by the matcher; the new part is the **session→session** edge, which exists before any commit.

### 3.6 Quota windows (`analytics/quota.rs`, new)

- **Codex (observed):**
  - Merge `fact_rate_limit_snapshot` from all sessions into one global timeline per `(limit_id, window_kind)`, ordered by `ts`.
  - A window instance is identified by `resets_at` (with ±60 s tolerance); a `resets_at` change marks a new window.
  - For consecutive snapshots inside a window, Δ`used_percent` over (t0, t1] is **allocated** to the usage events with `ts ∈ (t0, t1]` in proportion to `api_equiv_usd` (fallback: total tokens). Allocated rows are graded `allocated`.
  - When concurrent sessions overlap, the UI shows "shared window — allocation by token share". Snapshot gaps > 30 min are shown as "unobserved interval".
- **Claude Code, Cursor, OpenCode (no telemetry):**
  - Show **token burn** per rolling window shape from `billing.toml` (default Claude: 5h sessions anchored at the first message after a reset, plus a weekly window; default Cursor: monthly).
  - Graded `estimated`, with the persistent label "No local quota telemetry; % of limit unknown".
  - If the user logs a `limit_hit` observation (`vca quota mark claude --at 14:32 --kind limit_hit`), the window shows the tokens consumed up to that point as "observed limit at ≈ N tokens (one observation)". After ≥ 3 observations it shows the min/median, never a precise percentage.
- **Spike detection:**
  - Hourly API-equivalent burn per provider. A spike is an hour > max(3 × trailing-7-day median for that hour-of-week, p95), or a Codex window Δ ≥ 15 pp within 60 min.
  - Spikes are stored in `view_usage_spike` with top contributors (sessions, subagents, models), and contributors are tagged `background` when there was no user message in that session in the 10 min before the event (subagents, long autonomous runs).

### 3.6a Context ledger: what fills the context and what it costs

**Question it answers:** "Which tool call, file, directory, MCP server or instruction put how many tokens into context, and how much did carrying them cost on every later call?"

The key point is that a tool result is paid for many times:
- once when it is written into context (input or cache-write price);
- again on **every subsequent API call** until compaction, `/clear` or session end (cache-read price, or full input price after a cache miss).

A 40k-token directory dump at call 5 of a 120-call session costs 1 write plus 115 cache reads.

**Local evidence (verified on a local Claude Code transcript):**
- **Per-API-call context size** (observed): Claude `usage.input_tokens + cache_creation_input_tokens + cache_read_input_tokens` per unique `message.id`. Codex `last_token_usage.input_tokens` (incl. cached) per `token_count`, plus `model_context_window` for fill %. OpenCode per-message tokens. Cursor only `contextUsagePercent` snapshots.
- **Injected context items** (Claude `attachment` records):
  - `instructions` (CLAUDE.md/AGENTS.md content)
  - `skill_listing`, `mcp_instructions_delta`, `deferred_tools_record/delta` (tool definitions), `agent_listing_delta`
  - `environment`, `date`, `plan_mode`, `task_reminder`, `total_tokens_reminder`, …

  Codex: `session_meta.base_instructions` (text present → measurable), `turn_context`. OpenCode: system parts where present.
- **Tool inputs and outputs:**
  - Claude `tool_use.input` (path/glob/command/url/MCP tool name) and `tool_result` content. `toolUseResult.contentNotInModelContext` marks output **not** sent to the model, so it is excluded from the ledger. Subagent results carry `totalTokens`/`usage` (child spend), and the parent only pays for the returned text.
  - Codex `function_call(_output)`/`custom_tool_call(_output)`. OpenCode `tool` parts `state.input/output`. Cursor `toolFormerData` (partial).
- **What is not in transcripts:** the hidden harness system prompt and tool JSON schemas when they aren't logged as attachments. These show up as **unexplained baseline** (below), never guessed per item.

**Tables:**
```sql
fact_context_item(provider, session_id, item_key, turn_index, ts, call_index_entered,
  kind,         -- baseline_unexplained|instructions|tool_definitions|skill_listing|mcp_instructions|
                -- agent_listing|environment|reminder|user_message|assistant_text|thinking|
                -- tool_input|tool_result|subagent_result|compaction_summary|attachment_other
  tool_name, mcp_server,                    -- e.g. Read / Bash / mcp__github__get_file_contents → server "github"
  target, target_dir, target_kind,          -- path|dir|glob|command|url|query; target_dir = parent dir or glob root
  instruction_source,                       -- path of CLAUDE.md/AGENTS.md when kind=instructions
  chars, est_tokens, calib_tokens,          -- calib = est × session calibration factor (below)
  in_model_context INTEGER,                 -- 0 when contentNotInModelContext / truncated away
  call_index_evicted,                       -- first call after compaction/clear, NULL = until end
  UNIQUE(provider, session_id, item_key))

fact_api_call(provider, session_id, call_index, usage_key, ts, model_name,
  context_tokens, fresh_input, cache_write, cache_read, output, context_window,
  cache_event)  -- normal|cold_start|rebuild_after_idle|rebuild_after_model_switch|rebuild_after_compaction|unknown
  -- 1:1 with deduped usage events; ordered calls within a session

derived_context_cost(provider, session_id, item_key, write_tokens, write_usd, carried_calls,
  carry_tokens, carry_usd, total_usd, peak_share_of_context, grade)   -- grade: estimated|allocated
```

**Attribution algorithm** (deterministic, `analytics/context.rs`):
1. **Observed growth.** For consecutive calls k−1→k: `Δctx_k = context_tokens_k − context_tokens_{k−1}`. This is observed.
2. **Items entering between the calls** (user message, previous assistant output, tool results, attachments) get `est_tokens` = chars / 3.6 (code-heavy) or / 4.2 (prose), picked by a simple code-density check.
3. **Calibration.** The per-session factor = median over calls of `Δctx_k / Σ est_tokens(items entering at k)`, over calls with no compaction or cache event, clamped to [0.6, 1.6]. Each item's `calib_tokens` = est × factor, rescaled so that the items entering at k sum exactly to `Δctx_k` when 0 < ratio < 2. Otherwise the call is flagged `unreconciled` and keeps its estimates. The per-call residual is stored and shown, never hidden.
4. **Baseline.** `context_tokens` at call 1 minus the visible items before call 1 is the **fixed overhead**: system prompt, tool schemas and anything not logged. Attachment-logged instructions, skills, MCP and agent listings are carved out of the baseline as named items. The remainder is `baseline_unexplained`.
5. **Cost.**
   - Write cost: the item's tokens × (cache-write price if the call's `cache_write` > 0, else the input price) at the model of the entering call.
   - Carry cost: Σ over calls k' in (entered, evicted) of tokens × cache-read price of call k's model.
   - If a call is a `rebuild_*` cache event, every live item is re-priced at the cache-write price for that call. This is the **cache-miss penalty**, attributed to the cause (idle > TTL, model switch, compaction).
   - Item costs are an **allocation** of the call's observed cost. For each call, Σ item shares + baseline + residual = that call's input-side API-equiv cost, so this is additive with the money ledger. Output tokens are attributed to the call itself, not to items.
6. **Cache events.**
   - `rebuild_after_idle`: cache_read drops > 50% and cache_write ≈ previous context after a gap > 5 min.
   - `rebuild_after_model_switch`: the same pattern on a model change.
   - `cold_start`: call 1.
   - Each one is stored with its extra cost vs. what a cache hit would have cost.
7. **Grouping keys** for aggregation:
   - `tool_name`, `mcp_server`, `target_dir` (normalized to repo-relative and truncated to depth 3), `target_kind`, `kind`, and `instruction_source`.
   - **Read purpose** (deterministic): *startup read* (before the first edit or first user follow-up in the task); *used* (path later edited, or quoted/mentioned in assistant text); *unused* (neither).

**Linking reads to instructions (correlation, labelled as such):**
- On ingest, read the instruction files the session actually loaded (`instructions` attachments; else CLAUDE.md/AGENTS.md at `repo_root` at session time if present). Extract referenced paths, globs and commands (regex for backticked paths and `docs/…`, `*.md`, shell commands).
- A context item whose target matches a reference gets `instruction_ref = <file>:<line>`.
- Aggregation shows e.g. "AGENTS.md:12 references `docs/` → read in 34/38 sessions in this repo, median 41k tokens, carry ~$0.62/session".
- Recurring startup reads **without** an instruction reference are shown as "habitual startup reads (agent or skill behaviour)".
- Optional Jev noul (`instruction_caused_read`, sent at level ≥ `user_text` with the instruction excerpt + read target) is available. It is off by default, since the deterministic path match covers most cases.

**Per-provider fidelity:**
- **Claude:** high (per-call usage, attachments, full tool results).
- **Codex:** high for growth, base instructions and tool outputs. There is no attachment breakdown, so AGENTS.md is part of the baseline unless it appears in `base_instructions`/user instructions items.
- **OpenCode:** medium (per message, not per API call; steps help when present).
- **Cursor:** low (context % snapshots only). Shown as "context composition unavailable".

### 3.7 Migration from v1

`migrate_v2`:
1. ALTERs and new tables.
2. Set `ingest_version = 0` on all sessions, forcing re-parse on the next `ingest` wherever the source still exists.
3. Pre-existing usage rows get `token_source='legacy'`, `ts_quality='session_start|session_end'`.
4. On re-parse of a session whose source exists, legacy rows are **replaced**. If the source is gone, they are kept and flagged. Legacy Claude rows are flagged `legacy_possible_duplication`; the UI shows `!` and the Usage screen shows "N legacy Claude sessions may be over-counted (source deleted by Claude Code cleanup)".

First `ingest` after upgrade prints a one-time "accounting corrected" summary (tokens and $ before vs after per provider), which is also written to `~/.vca/logs/migration-v2.txt`.

`ingest --fresh` keeps working, but it now **warns** if any session's source is missing, because a fresh rebuild loses those sessions. The prompt can be bypassed with `--yes`.

---

## 4. Optional Jev classification

### 4.0 How Jev is used, end to end (summary)

1. **Off by default.** All analytics (tokens, cost, quota, tasks, validation, commits) are deterministic. A local heuristic fills label slots marked `h`.
2. **Local preparation.** `ingest` builds turns, attempts, validations and cross-session relations. Without Jev, each session is one task.
3. **The user picks what to send.** `vca classify plan` (or `K` in the TUI) selects units by filters, a content level (`evidence_only` / `user_text` / `conversation` / `full`) and project allow/deny lists. It shows the counts and estimated cost. `preview` shows the exact redacted JSON, and nothing is sent without confirmation.
4. **One HTTP call per unit** to `POST openrouter.ai/api/alpha/decisions`, with `model: typesafe/jev-1.13`, `state` = compact JSON (deterministic evidence + truncated transcript) and `questions` = a fixed set of choice/score/noul questions:
   - **Segmentation pass** (sequential per session, one call per user turn): which task does this turn belong to? The options are continue the current task, start a new task, resume one of the listed earlier open tasks, or not task work. This splits one long session into tasks.
   - **Completion pass** (one call per task): at which turn was the task actually completed, if at all? The options are the task's turns plus `not_completed`. This gives the task's completion time, time per task and cost to complete.
   - **Task** call: intent, outcome, progress, framing, and 7 problem flags.
   - **Exchange** call: the user's feedback type and frustration.
   - **Relation** call: why later work changed earlier AI lines.
5. **Jev returns** a label, probabilities and confidence per question, and nothing else (no text). We store them, band them (confident / tentative / uncertain), cache them by content hash, and let manual overrides win.
6. **Labels segment and annotate; they never change measured quantities.** Task boundaries and completion turns decide *which* usage events and timestamps belong to a task, but the tokens, costs and times themselves are measured. Labels also feed intent-based denominators, correction/loop spend categories, rework kinds and defect evidence level L3. They never change token, cost, quota or git figures.
7. **Labels are evaluated against a user-labelled gold set.** Each question is trusted by default only if it beats the heuristic baseline and is calibrated.

Classifier cost is tiny (input-only billing): roughly $0.0001 per user turn for segmentation and $0.0003 per task for the labels. It is budget-capped and shown as its own line in Usage.

### 4.1 What Jev actually is (verified 2026-10-12)

Sources: the OpenRouter model endpoint API (`/api/v1/models/typesafe/jev-1.13/endpoints`), the OpenRouter Jev guide and Decisions API reference, and docs.typesafe.ai (System One, Choice, Score, Confidence, Patterns).

- **It is not a text generator.** Jev is TypeSafe's "System One" structured decision model. Modality is `text->decisions`. It "does not produce reasoning traces, explanations, or free-form text" and is "not a drop-in replacement for a chat model". **So Jev cannot write summaries, titles, rationales or "why" text.** Any prose in the UI must come from deterministic templates over evidence.
- **Endpoint:** `POST https://openrouter.ai/api/alpha/decisions`, with a bearer OpenRouter key. The request is `{model:"typesafe/jev-1.13", state, questions, provider?, session_id?, trace?, user?}`.
  - `state` is a string, object or array.
  - `questions` is a map of `name → {type, instructions, criteria}`.
  - OpenRouter also documents `/api/v1/systemone`; we target the documented Decisions schema.
  - The `alpha` path is a stability risk, so isolate it behind a trait (§4.8).
- **Question types:**
  - `noul`: criteria has `"true"`/`"false"` descriptions; it returns a probability (no confidence field).
  - `choice`: criteria maps up to 255 option names to descriptions; it returns `choice`, `probabilities` (sum 1) and `confidence`.
  - `score`: criteria is an ordered array of 2–10 levels; it returns a probability-weighted `score`, `probabilities`, `legend` and `confidence`.
- **All questions in one request are answered in parallel and cannot see each other.** Questions can't depend on earlier answers inside one call, so dependent logic is done in our code or in a second pass.
- **Limits and pricing:**
  - Endpoint `max_prompt_tokens` 32,000 (context 64k).
  - $0.042 per 1M input tokens; output free.
  - Each response has `usage.input_tokens/output_tokens/cost`.
  - Provider preferences include `zdr`, `allow_fallbacks` and `max_price`.
  - The TypeSafe provider's data-retention policy is **not stated** on the pages fetched. That is an open item, and we default to requesting `zdr: true` (§4.5).
- **Confidence semantics:** confidence summarizes how concentrated the distribution is. It is calibrated across groups, not a guarantee per answer. TypeSafe recommends per-question thresholds tuned on labelled data and routing low-confidence cases to a human. That matches the override/eval design below.
- **Design consequence:** Jev is a good fit for a set of independent, well-described categorical questions over a compact structured state. "Speculative fan-out" (many questions per call) is the cheapest pattern.

### 4.2 Classification units

| Unit | Why | Questions | Volume |
|---|---|---|---|
| **User turn** (segmentation) | Splits long multi-purpose sessions into tasks | `task_assignment` (choice, dynamic options) | Every user turn after the first, sequential per session |
| **Task** (primary unit) | The goal-sized unit for cost, time and outcome | `completion_turn` (choice, dynamic options), `intent`, `outcome`, `progress`, `framing`, the problem nouls | 1 completion call + 1 label call per task |
| **Exchange** = assistant output + following user message | User reaction is the most direct evidence of correction, requirement change, or approval | `feedback` (choice), `frustration` (score) | Every user turn after the first in a task |
| **Relation** (earlier task → later task) | Rework judgment needs both sides | `rework_kind` (choice) | Only `touches_earlier_ai_lines` edges with overlap ≥ 5 lines, plus `same_ticket` edges within 7 days |
| **Session** | **Not classified directly.** Derived from tasks: primary intent = intent of the task with most tokens; session outcome = worst unresolved / best completed rule (below) | — | — |

Session rollup rule:
- `mixed` if tasks have ≥ 2 intents each with ≥ 25% of tokens.
- Outcome = `completed` if the last code task completed and none stalled; `stalled` if any stalled task holds ≥ 40% of tokens; otherwise the outcome of the token-heaviest task.

### 4.3 Questions, labels and criteria

All instructions and criteria live in `crates/vca/src/classify/questions.toml`, with `question_set_version`. Abbreviated text follows; the implementer copies it verbatim into the TOML.

**Q1 `intent`** (choice, task). *Instructions:* "What kind of software work is the user pursuing in this task? Judge by the user's goal, not by what the assistant happened to do."

| Label | Criteria | Example (illustrative) |
|---|---|---|
| `feature` | Adding new user-visible behavior or capability | "Add a --weekly flag to the cost report" |
| `bug_fix` | Making existing behavior correct after a reported or observed defect | "Ingest crashes on empty Cursor DB, fix it" |
| `debug_investigate` | Finding the cause of a problem without (yet) a known fix | "Why are Claude tokens 3x higher than the console?" |
| `refactor` | Restructuring code without intended behavior change | "Split analytics.rs into modules" |
| `tests` | Writing or fixing tests as the main goal | "Add regression tests for Codex token parsing" |
| `research_explain` | Understanding code, a library or an API; answers, not edits | "How does the matcher handle renames?" |
| `planning_design` | Producing a plan, design or spec; deciding an approach | "Plan the schema v2 migration" |
| `review` | Reviewing a diff, PR or code for problems | "Review this PR for correctness" |
| `ops_config` | Build, CI, dependencies, environment, release, deploy | "Fix the clippy job in CI" |
| `docs` | Writing docs, README or comments as the goal | "Update USER_GUIDE for the new metrics" |
| `data_scripting` | One-off data analysis, queries or scripts not part of the product | "Write SQL to find sessions over $5" |
| `other` | None of the above clearly fits | — |

**Q2 `outcome`** (choice, task). *Instructions:* "By the end of the evidence shown, what is the state of the user's goal in this task? Use validation results and the user's last messages; do not assume success from the assistant's claims."

| Label | Criteria |
|---|---|
| `completed_verified` | The goal appears met **and** a validation passed or the user explicitly confirmed (e.g. "works", "lgtm", then commits) |
| `completed_unverified` | The assistant reported completion; no validation or confirmation is visible |
| `answered` | An information, plan or review goal was answered or decided (non-code goals) |
| `partial` | Clear progress, but the user's stated goal is not fully met at the end |
| `stalled` | Repeated attempts without measurable progress; the same failure persists |
| `abandoned_redirected` | The user dropped the goal or switched to a different one without resolution |
| `blocked_external` | Progress depends on something outside the conversation (access, missing info, broken environment) |
| `unclear` | The evidence is insufficient (e.g. a truncated transcript) |

**Q3 `progress`** (score, task, 5 levels): `0` no movement toward the goal · `1` exploration only · `2` partial implementation, unvalidated · `3` mostly done, minor issues remaining · `4` goal reached. *Used for sorting and trends, while `outcome` is used for categories.*

**Q4 `framing`** (score, task, 4 levels; evaluated on the **first user message of the task only**). *Instructions:* "How well-specified was the initial request?"
- `0` vague goal, no constraints ("make it better")
- `1` goal stated, no acceptance criteria
- `2` goal plus constraints or relevant files
- `3` goal plus acceptance criteria / how to verify (e.g. "add X; `cargo test parses_*` must pass; don't touch the sync payload")

**Q5–Q11 problem nouls** (task; each independent; `true`/`false` criteria):

| id | true when… | false when… |
|---|---|---|
| `p_repeated_failure` | The same error or failing check recurs across ≥ 2 attempts without being resolved | Failures change, get fixed, or none occur |
| `p_misunderstood` | The assistant pursued something other than what the user asked, and the user had to restate the request | Assistant actions match the request |
| `p_user_corrected_output` | The user pointed out a defect or error in output the assistant produced earlier in this task | No user report of assistant mistakes |
| `p_requirements_changed` | The user changed or added requirements after seeing output (not because the output was wrong) | Requirements stayed stable |
| `p_ignored_constraint` | The assistant violated an explicit user instruction or constraint (files not to touch, style, scope) | — |
| `p_env_tooling` | Time was lost to environment, tooling, permissions, dependency or network problems rather than the code itself | — |
| `p_hallucination` | The assistant referenced APIs, files, flags or facts that turned out not to exist | — |

**Q12 `feedback`** (choice, exchange). *Instructions:* "How does the user's message respond to the assistant's previous output?"

| Label | Criteria | Example (illustrative) |
|---|---|---|
| `accept_continue` | Approves, or asks for the next step building on the output | "Great, now add tests" |
| `correct_defect` | Says the output is wrong or broken, or reports an error it caused | "That broke the build: E0308 in sync.rs" |
| `correct_misunderstanding` | Says the assistant misunderstood the request and restates it | "No, I meant per-session, not per-model" |
| `change_requirement` | Changes or adds requirements; the output wasn't wrong for the original ask | "Actually make it weekly instead" |
| `redirect_new_goal` | Starts a different goal | "OK, separate thing: the README…" |
| `clarify_answer` | Answers the assistant's question or supplies requested info | "The DB is at ~/.vca" |
| `question` | Asks a question about the output | "Why did you use a HashMap here?" |
| `other` | — | — |

**Q13 `frustration`** (score, exchange, 3 levels): `0` neutral · `1` mild impatience · `2` clear frustration. *This is used only in aggregate (Practices screen), never shown as a per-person judgment beyond the session view.*

**Q14 `task_assignment`** (choice with **dynamic options**, one call per user turn; the segmentation pass).

*Instructions:* "Which task does the user's new message belong to? A task is one goal the user wants done. Refinements, corrections, follow-up fixes and 'also do X for the same feature' continue the current task. A different goal is a new task."

The options are built per call:

| Option name | Criteria text (generated from local data) |
|---|---|
| `continue` | "Continues the current task T3: '<first user request of T3, ≤200 chars>' (last activity: <tools/edits summary>)" |
| `resume_T1`, `resume_T2`, … | "Returns to earlier open task T1: '<first request of T1>'". Only tasks not yet completed are listed, max 20 most recent. |
| `new_task` | "Starts a goal not covered by any task above" |
| `not_task_work` | "Small talk, thanks, meta questions about the assistant, or a slash command with no goal" (assigned to the previous task for cost purposes, flagged) |

- *State:* the new user message (≤ 1,500 chars); the previous 3 turns condensed (user text, assistant text ≤ 400 chars, tool/edit summary, files touched); the idle gap before the message; and whether a commit, `/clear` or compaction happened in the gap.
- Choice supports up to 255 options; we cap at 23 (20 resumes + 3).

**Processing:**
- Sequential within a session, because each call's options depend on previous assignments. Parallel across sessions (default concurrency 4).
- Turn 1 is always the start of T1. A turn after `/clear` lists `continue` with the note "context was cleared".
- Low-confidence assignments (band `uncertain`) fall back to `continue`, flagged `?`. Errors are thereby biased toward fewer, larger tasks, and over-splitting is avoided.
- **Post-processing:** a new task of ≤ 1 turn that is followed by a `resume` of the previous task within 2 turns is merged back as an aside. Merges are recorded so the UI can show them.
- **Long sessions:** the state is per turn and small, so session length doesn't hit the 32k limit. A 300-turn session costs about 300 × ~1.5k tokens ≈ 0.45M tokens ≈ $0.02.
- **Reclassification:** an appended session only classifies its new turns. Earlier assignments are cached by turn key and options digest.
- Overrides (`move turn to task N`, `split here`, `merge`) re-run assignment only for later turns whose options changed.

**Q16 `completion_turn`** (choice with dynamic options, one call per task; the completion pass).

*Instructions:* "At which turn was this task's goal actually achieved? Achieved means the requested change works or the question was answered. Prefer turns where a validation passed or the user confirmed. Choose `not_completed` if the goal was never reached."

- **Options:** `turn_<k>` for each turn of the task, with criteria text = "Turn k (<time>): user: '<≤150 chars>'; assistant: '<≤150 chars>'; tools: edit×2, test ✓", plus `not_completed` and `unclear`.
- **State:** the task's opening request, the per-turn evidence list (validations with results, commits made after the turn, user reactions), and the last 2 turns verbatim (truncated).
- **Tasks with more than 250 turns or a state over budget** go through a two-step chunked query: first a noul per chunk of 40 turns, "was the goal achieved within these turns?", then a choice within the first chunk that has p ≥ 0.6.
- **Consistency checks** (deterministic, in code):
  - A chosen turn **after** which the user reports a defect in the same task (`feedback=correct_defect`) is marked `reopened`. Completion moves to Jev's next-highest-probability turn after the reopen point, or to `not_completed`. Both the first and the final completion times are kept: "first claimed done" vs "actually done" is itself a useful metric.
  - If a commit evidence time and the Jev completion differ by > 2h, both are shown, and the Jev value is used unless its band is `uncertain`.

**Q15 `rework_kind`** (choice, relation; state contains both tasks, §4.4). *Instructions:* "The later work modifies lines produced by the earlier AI work. Why?"

| Label | Criteria | Example (illustrative) |
|---|---|---|
| `defect_correction` | The earlier output was wrong: a bug, a failing behavior, a broken build | Earlier task added a token parser; the later one fixes a double-count it introduced |
| `requirement_change` | The earlier output met the earlier ask; the ask changed | Earlier: daily buckets; later: "switch to weekly" |
| `refinement` | Polish, refactor, naming, performance of working output | Extracting a helper from earlier code |
| `continuation` | Extends the earlier work as planned (next step) | Earlier: schema; later: queries using it |
| `revert_abandon` | Removes the earlier output because the approach was dropped | — |
| `incidental` | The overlap is incidental (formatting, moved code, generated files) | rustfmt reflow |
| `unclear` | — | — |

**Speculative fan-out.**
- Q1–Q11 are sent in **one** request per task.
- Q12–Q13 are sent in one request per exchange. A request has a single `state` and every question applies to the whole of it, so several exchanges cannot be packed into one call and labelled separately. Exchanges are small (~600 tokens), so the cost is negligible (§4.7).
- Q14 (segmentation) runs first and is sequential per session. Q16 (completion) runs after segmentation, then Q1–Q11 per task, then Q12–Q13 per exchange, then Q15 per relation. Each has its own state, so each goes in its own request.

### 4.4 Context sent (state construction)

State is a JSON object built by `classify::state::build_*`. Task example (illustrative):

```json
{
  "unit": "task",
  "tool": "claude_code", "agent_kind": "main",
  "project": "repo-7f3a",                      // hashed unless privacy.project_names = true
  "duration_minutes": 48, "turns": 9, "model_changes": ["sonnet-4.5→opus-4.6 at turn 6"],
  "evidence": {                                 // deterministic facts, always included
    "files_edited": 6, "lines_changed": 214,
    "validations": [{"kind":"test","result":"fail","sig":"E0308 mismatched types"},
                    {"kind":"test","result":"fail","sig":"E0308 mismatched types"},
                    {"kind":"test","result":"pass"}],
    "commits_after_task": 1, "subagents": 1, "compactions": 0
  },
  "transcript": [
    {"turn":1,"user":"Add per-event timestamps to Codex usage… must keep event_session_cost columns…"},
    {"turn":1,"assistant":"I'll parse last_token_usage per token_count…","tools":"read×4 edit×2 bash:cargo test→fail(E0308)"},
    {"turn":2,"user":"still failing, same error in sync.rs"},
    {"omitted":"turns 3-6: 7 edits, 3 test runs (2 fail, 1 pass)"},
    {"turn":9,"user":"looks good, committing"}
  ]
}
```

**Content levels** (`classifier.toml: privacy.level`, overridable per run):

| Level | Sends | Questions allowed |
|---|---|---|
| `evidence_only` | Deterministic `evidence` block, no text | outcome, progress, p_repeated_failure, p_env_tooling (others skipped → unclassified) |
| `user_text` | + user messages | all except p_hallucination |
| `conversation` **(default)** | + assistant prose, **code blocks stripped** (replaced by `[code: 34 lines rust]`) | all |
| `full` | + tool output excerpts (first 3 error lines, command heads) | all |

**Redaction, applied at every level before sending:**
- Secret patterns (AWS/GCP/GitHub/OpenAI/Anthropic/OpenRouter key shapes, `-----BEGIN … KEY-----`, `password=`, JWTs, `.env` lines), replaced by `[secret]`.
- Email addresses, replaced by `[email]`.
- Absolute home paths, replaced by `~`.
- An optional user regex list.
- The redaction count is recorded per request.

**Length budget:**
- Target ≤ 6,000 tokens; hard cap 28,000 (under the 32k prompt limit, with headroom for question text, roughly 1.5k for Q1–Q11), counted with a conservative chars/3.5 estimate.
- Each user message is truncated to 1,500 chars (head 1,000 + tail 500); assistant prose to 600 chars; tool activity collapses to one line per turn.
- If over budget: keep the first 3 and last 5 turns verbatim and collapse the middle into deterministic `omitted` summaries.
- If still over (pathological pasted logs): drop to `user_text`, then `evidence_only`, and record `degraded_level`.

**Mixed-purpose and long conversations** are handled by task segmentation before classification. A 6-hour session becomes e.g. 5 tasks, each classified on its own. Session-level views show the task mix (§5) rather than forcing one label.

**Relation state:**
- `{earlier: {intent_text: first user message (≤600 chars), summary evidence, age_hours}, later: {first user message, feedback labels of its exchanges if already classified, evidence}, overlap: {files, lines, sample_removed_lines? only at level full}}`.
- Prior Jev labels are passed only as `"prior_labels"` metadata, never as text claims. Relations are classified in a second pass after tasks and exchanges.

**Selection and consent** (nothing is sent without explicit action):
- `vca classify plan [--from --to --repo --provider --min-cost-usd --min-tokens --sessions <ids> --level <level>]` prints the unit counts, estimated input tokens, estimated $ (with the current price from the endpoint API), and the excluded counts by reason (denylisted project, already cached, under threshold).
- `vca classify preview <unit-id>` prints the exact JSON that would be sent.
- `vca classify run … --max-usd 0.50` requires `--yes` or an interactive confirm. The TUI equivalent is the consent screen (§5.13).
- `~/.vca/classifier.toml` holds: `enabled=false` by default; `api_key_env="OPENROUTER_API_KEY"` (or `vca classify key set`, stored like `github_token` with 0600 perms); `allow_projects`/`deny_projects` globs; `default_level`; `monthly_budget_usd=2.0`; `model="typesafe/jev-1.13"` (pinned, not `~latest`); `provider = {zdr = true, allow_fallbacks = false}`.
- Auto-classification after `ingest` is possible only with `auto=true` in config, still bounded by the allowlist and budget. It runs as a separate step **after** ingest commits and never blocks ingest.

### 4.5 Privacy defaults

- Off by default. The first run shows a one-time notice: what is sent, to whom (OpenRouter → TypeSafe), the default level, and that the provider data policy is "per OpenRouter/TypeSafe terms; request sent with zdr=true".
- If OpenRouter returns an error for `zdr` (no ZDR endpoint), the run stops with a message. It never silently falls back.
- Payloads are not stored locally by default; only `payload_sha256`, the token estimate and the redaction count are. `--keep-payloads` stores them under `~/.vca/classifier/payloads/` for debugging.

### 4.6 Linking results to evidence

`classification_result` rows are keyed by `(unit_kind, unit_key, question_id)`, where `unit_key` = `provider|session_id|task_index` (task), `…|turn_index` (exchange), or `from_key→to_key` (relation). Joins:
- **Messages/turns:** the task → `start_turn..end_turn` range of `fact_session_turn`/`fact_session_message`. Exchange labels attach to a specific user message.
- **Tool activity and validation:** `derived_attempt` and `fact_session_tool_call` within the task range.
- **Usage/cost:** `derived_usage_allocation.task_index`, so "spend by intent", "spend in stalled tasks" and "spend in correction exchanges" are exact sums of allocated events.
- **Code changes:** `fact_session_code_change` by `change_ts` within the task → line hashes.
- **Commits:** `fact_commit_session_match` + hash overlap gives the task-level share. A commit is linked to tasks whose `+` hashes it contains, with `share = matched_lines_from_task / matched_lines_total`.
- **Later fixes:** `derived_session_relation` + `rework_kind`, and commit-level `event_commit_bug_signal`. The Delivery & Rework screen joins all three.

### 4.7 Uncertainty, overrides, caching, failures, cost

**Uncertainty bands** (defaults, recalibrated after evaluation in §8.2; per question in `questions.toml`):
- choice/score: `confidence ≥ 0.60` → **confident**; `0.35–0.60` → **tentative** (rendered with `?`); `< 0.35` → **uncertain** (rendered `~unclear`, excluded from rate numerators and counted in an "uncertain" bucket).
- noul: `p ≥ 0.70` → yes; `p ≤ 0.30` → no; otherwise uncertain.
- Aggregates show `n confident / n tentative / n uncertain / n unclassified`, and percentages use **confident + tentative** with a coverage figure. A toggle `C` switches to confident-only.

**Incomplete evidence:**
- Tasks from sessions with `source_missing`, truncated transcripts, or `degraded_level` get a `limited_evidence` flag. Their `outcome` is forced to `unclear` when there are no messages after the last edit.
- Cursor tasks without tool data are classified at `user_text` level and flagged.

**Manual corrections:**
- In the TUI (`e` on any label) or `vca classify set <unit> <question> <label> [--note]`, a correction writes `classification_override`.
- Overrides always win in `view_label_effective`, survive reclassification, and are exported to `classification_gold` (they double as evaluation labels).
- An override on a task boundary (`split here`/`merge with previous`) is stored in `task_override` and applied by `refresh_session_structure`, which invalidates the affected cache keys.

**Caching and reclassification:**
- Cache key = `sha256(question_set_version, question_id_set, model_id, privacy_level, unit content_digest)`. `content_digest` covers the turn range's message keys + tool-call keys + evidence.
- A unit is re-sent only when its digest changes (the session grew, boundaries moved) or the question set version or model changes. A model or version bump is **never** automatic: `vca classify run --reclassify model-change` is explicit, and the previous results are kept with a `superseded_by` link for comparison.
- Exchange results are reused across task re-segmentation because the exchange key is turn-based.

**Failure handling** (`classify::client`):

| Response | Action |
|---|---|
| 429 / 529 / 503 | Exponential backoff with jitter (1s→32s, 5 tries); honor `Retry-After` |
| 500 / 502 / 524 / network | Retry 3×, then mark the request `failed_transient` (retried on the next run) |
| 413 | Rebuild the state one privacy level lower, or with tighter truncation; retry once |
| 400 | Mark `invalid_request` with the error message; no retry; counted in the run summary (usually a schema change in the alpha API) |
| 401 / 403 | Abort the run: "check OpenRouter key" |
| 402 | Abort the run: "insufficient credits". Remaining units stay `pending` |
| Malformed answer (missing question, label not in criteria) | Store `invalid_answer`; that question becomes unclassified |

- Runs are resumable (`classifier_request.status`). Concurrency is 4 (configurable). No classification failure ever affects ingest or deterministic reports.

**Classifier cost:**
- Estimate before the run (tokens × endpoint price fetched from `/api/v1/models/…/endpoints`, cached 24h).
- Stop when `Σ usage.cost` would exceed `--max-usd` or the remaining monthly budget.
- Record actual `usage.cost` per request. Classifier spend appears in the Usage screen as tool `jev (classifier)`, so the analytics' own cost is visible.
- Order of magnitude (illustrative): 6k-token task ≈ $0.00025; 600-token exchange ≈ $0.000025. A heavy month of 400 tasks + 4,000 exchanges + 300 relations ≈ $0.10 + $0.10 + $0.04.

### 4.8 Code structure and storage

`crates/vca/src/classify/`:
- `mod.rs`: orchestration.
- `questions.rs` + `questions.toml`.
- `state.rs`: state builders, truncation, redaction.
- `client.rs`: `trait DecisionClient { fn decide(&self, req) -> Result<DecisionResponse> }`; `OpenRouterDecisions` (reqwest, already a workspace dependency); `RecordedClient` for tests.
- `heuristic.rs`: the local baseline.
- `storage.rs`, `eval.rs`.

Tables:
```sql
classifier_run(run_id, started_at, finished_at, model, endpoint, question_set_version, level,
  units_planned, units_sent, units_cached, input_tokens, cost_usd, status, error)
classifier_request(request_id, run_id, unit_kind, unit_key, cache_key, payload_sha256, est_tokens,
  input_tokens, cost_usd, http_status, status, attempts, degraded_level, redactions, error, created_at)
classification_result(unit_kind, unit_key, question_id, question_version, source, -- jev|heuristic
  label, score, probability, probabilities_json, confidence, band, model_version, cache_key,
  run_id, created_at, superseded_by, UNIQUE(unit_kind, unit_key, question_id, source, cache_key))
classification_override(unit_kind, unit_key, question_id, label, note, created_at)
classification_gold(unit_kind, unit_key, question_id, label, labeled_at, origin)  -- override|eval_session
task_override(provider, session_id, turn_index, action)  -- split|merge
```

### 4.9 Heuristic baseline (no Jev)

- **Segmentation (Q14):** none. The whole session is one task (optionally `/clear` and commit boundaries, §3.5).
- **Completion (Q16):** commit or validation evidence (§3.5a, steps 3–4), otherwise "not observed complete".

The other questions are filled with `source='heuristic'`, rendered with a distinct marker (`h`):
- `intent`:
  - No edits + read/search tools → `research_explain`.
  - First message contains plan/design keywords → `planning_design`.
  - Contains review/PR → `review`.
  - Mainly test files edited → `tests`.
  - Fix/bug/error keywords + edits → `bug_fix`.
  - CI/deps/config files → `ops_config`.
  - Otherwise edits → `feature`.
- `outcome`: last validation pass + later commit → `completed_verified`; ≥ 3 same-signature failures at the end → `stalled`; no edits and ≥ 1 assistant answer → `answered`; else `unclear`.
- `feedback`: keyword rules ("no,", "wrong", "that broke", "still failing" → `correct_defect`; "actually", "instead" → `change_requirement`).
- `rework_kind`: the fix-keyword regex on the later first message → `defect_correction`; else `unclear`.

The heuristic is also the **baseline that Jev must beat** in evaluation.

### 4.10 What stays deterministic (never AI)

- **Accounting:** all token, cost, quota and percentage figures, and their allocation.
- **Session structure:** session trees, turns, tool calls, validation results, exit codes, model changes, compactions.
- **Code and delivery facts:** line-hash matching, commit association, mainline/PR/merge facts, churn and revert facts, time and duration.
- **Segmentation without Jev:** whole session = one task (optionally `/clear` and commit boundaries). With Jev, the task *assignment* is a judgment, but each task's timestamps, durations and costs are computed from measured events.
- **Relation candidates:** relation *existence* and overlap size. Jev only labels the *kind*.
- **Prose:** all text in the UI (templates).

AI and heuristic judgments: task assignment of turns, completion turn, intent, outcome, progress, framing, problems, feedback, frustration, rework kind.

---

## 5. Terminal UI

**All numbers, names and labels in the mockups are ILLUSTRATIVE DATA.** Mockups are drawn at 118 columns. Every screen degrades at narrower widths by dropping the lowest-priority columns (each column has a priority value in its definition).

### 5.1 Architecture changes to the TUI

- Split `commands/tui.rs` into `crates/vca/src/tui/`:
  - `app.rs`: App, navigation stack, key dispatch.
  - `loader.rs`: background query thread.
  - `filters.rs`: `FilterSet`, persistence.
  - `screens/{overview,sessions,groups,session_detail,usage,delivery,practices,compare,classify}.rs`
  - `widgets/{data_table,bar,sparkline,breadcrumb,popup,legend}.rs`
  - `commands/tui.rs` stays a thin `run_for` wrapper, so paceflow's `AppContext` reuse keeps working.
- **Query layer** `crates/vca/src/explore/`:
  - Typed functions such as `sessions(&FilterSet, Sort, Page) -> Vec<SessionRow>`, `groups(&FilterSet, GroupDim) -> Vec<GroupRow>`, `session_detail(key)`, `usage_timeline(&FilterSet, Bucket)`, `quota_windows(...)`, `rework_chain(...)`.
  - Parameterized SQL (`rusqlite::params!`), not `sql_literal`.
  - CLI subcommands call the same functions, which makes them unit-testable without the TUI.
- **Async loading:**
  - A worker thread owns its own `Connection` and receives `(request_id, Query)` over `std::sync::mpsc`. Results come back tagged, and stale ids are dropped.
  - The UI shows `⟳ loading` in the affected panel and stays responsive.
  - This replaces the 13 synchronous queries in `reload()`.
- **Data table widget** with `TableState`, scroll offset, sticky header, column priority, a right-aligned numeric format, and per-cell grade glyphs. It fixes the current clipping.
- **Navigation stack:**
  - `Vec<Frame>`, where `Frame { screen: ScreenKind, filters: FilterSet, group: Option<GroupDim>, sort: (Col, Dir), selected: usize, scroll: usize, subtab: usize }`.
  - `Enter` pushes a new frame whose filters = parent filters + the selected constraint.
  - `Esc`/`Backspace` pops and restores the parent frame **exactly** (filters, sort, cursor, scroll).
  - `[` / `]` move to the previous/next sibling of the parent list without popping, for example stepping through the sessions of a group while staying in Session detail.
  - `H` returns to Overview with the stack cleared but global filters kept.
  - The breadcrumb line always shows the stack.
- **Filter persistence:** the last top-level `FilterSet` and per-screen sort are saved to `~/.vca/tui_state.json` (best effort).
- **Repo scope:** the TUI defaults to `--all-projects`. The current cwd-repo auto-scoping is shown as a removable filter chip `project: <cwd repo>` when launched inside a repo.

### 5.2 Global conventions

**Header (every screen):**
```
 AI Engineering Analytics · Sessions                          30d · all tools · all projects · $ api-equiv   14:32
 Overview › Groups(model) › claude-opus-4-6 › Sessions                                    [f]ilters: 2 active
```

**Glyphs:**

| Glyph | Meaning |
|---|---|
| `~` before a number | estimated (text-estimated tokens, or list-price API-equivalent $) |
| `≈` | allocated share (quota Δ split by token share) |
| `!` | data-quality warning (legacy or possibly duplicated usage, source missing); `x` explains it |
| `—` | missing / not available from this tool |
| `·` | not applicable (e.g. mainline for a research task) |
| `?` after a label | tentative classification |
| `h` after a label | heuristic label (no Jev) |
| `✎` | manual override |
| `◌` | unclassified |
| `⊘` | excluded from classification by privacy rules |

**Global keys:**

| Key | Action |
|---|---|
| `1`–`7` | Overview · Sessions · Groups · Usage & Quota · Delivery & Rework · Practices · Compare |
| `Enter` | Drill into the selected row or metric |
| `Esc` / `Backspace` | Back (pop frame) |
| `[` / `]` | Previous/next sibling |
| `H` | Home |
| `↑↓ j k` · `PgUp/PgDn` · `Home/End` | Move · page · top/bottom |
| `Tab` / `Shift-Tab` | Next/previous sub-tab |
| `f` | Filter editor · `t` time range · `/` search (title, first message, id, path) |
| `s` / `S` | Cycle sort column / sort menu (`r` reverses) |
| `G` then a key (`b` band, `m` model, `p` tool, `j` project, `i` intent, `o` outcome; Groups screen) | Regroup (a `g` keypress cycles dimensions, as today) |
| `$` | Money basis: api-equiv ↔ reported ↔ both |
| `a` | Session totals: tree (incl. subagents) ↔ self |
| `C` | Classification mode: confident+tentative ↔ confident only ↔ heuristic only |
| `x` | Explain: evidence panel for the selected number or label |
| `m` / `c` | Mark row for compare / open Compare with marked rows |
| `e` | Edit label (override) · `K` classify the selection (opens consent) |
| `y` | Copy the session id / commit sha to the clipboard (OSC 52) |
| `?` / `L` | Key help / metric legend |
| `q` | Quit |

**Explain panel (`x`):** a right-side popup that states, for the selected cell:
- the formula;
- the evidence grade;
- the source rows (e.g. "42 usage events, 40 reported, 2 estimated; pricing v2026-09 exact match");
- for labels, the probability distribution and confidence band.

This is the universal answer to "where does this number come from".

### 5.3 Overview (`1`)

```
 AI Engineering Analytics · Overview                       30d · all tools · all projects · $ api-equiv   14:32
 Overview                                                                                 ILLUSTRATIVE DATA
┌ Spend & tokens ─────────────────────────────────────┐┌ Quota (observed / estimated) ─────────────────────┐
│ API-equiv  ~$412.80   reported $18.40 (OpenCode)    ││ Codex  5h  ███████▌ 76%   resets 16:10  observed  │
│ Tokens     fresh 38.2M · cache-read 611M            ││ Codex  7d  ████▉    49%   resets Mon    observed  │
│ Sessions   214 roots · 63 subagents · 9 ! legacy    ││ Claude 5h  ~1.9M fresh tok this window  no % data │
│ ▁▂▂▃▁▁▅█▂▁▂▃▂▁▁▂▆▇▃▂▁▁▂▃▂▁▂▁▂▃  daily api-equiv      ││ Claude 7d  ~9.4M fresh tok · 1 limit hit logged   │
│ Spikes: Oct 03 14:00 (Claude, 3 sessions) · Oct 09 ││ Cursor     — no token or quota telemetry          │
└─────────────────────────────────────────────────────┘└───────────────────────────────────────────────────┘
┌ Where it went (by intent, tasks) ─────────────────┐┌ Conversation health ───────────────────────────────┐
│ feature        ~$151  37%  ██████████               ││ completed✓ 41%  answered 14%  partial 18%          │
│ bug_fix        ~$ 88  21%  ██████                   ││ stalled 9% ($61)  abandoned 6%  unclear 4%  ◌ 8%   │
│ debug_invest.  ~$ 64  16%  ████                     ││ correction exchanges 11% of spend (~$45)           │
│ planning       ~$ 22   5%  █▌                       ││ unproductive loops 23 tasks (~$38)              │
│ other/◌        ~$ 87  21%  ██████  (◌ = unclassified)││ rework of earlier AI lines: 31 edges (~$29)       │
└─────────────────────────────────────────────────────┘└───────────────────────────────────────────────────┘
 Top sessions by api-equiv
   ~$38.20  claude  opus-4-6   paceflow     "Migrate sync payload to v2…"   stalled?   3 agents   ! partial src
   ~$21.75  codex   gpt-5.5    vca          "Add per-event usage parsing"   completed✓ 0 agents   ✓ merged
   ~$17.02  claude  sonnet-4-5 web-app      "Investigate flaky e2e"         partial h  1 agent    · no code
 Classification: 312/420 tasks (Jev 288, heuristic 24) · 108 ◌ · classifier spend $0.07 this month
 1 Overview 2 Sessions 3 Groups 4 Usage 5 Delivery 6 Practices 7 Compare · Enter open · f filter · x explain · ? help
```

- **Shows:** the four questions at a glance: spend, quota state, where spend went (intent), and whether conversations are healthy. The bottom shows classification coverage and classifier spend.
- **Filters:** global (time, tool, project, model, billing mode). There is no grouping here; each panel is a fixed summary.
- **Selection:** `Tab` moves focus between panels, and `Enter` on any line drills:
  - Spend sparkline → Usage & Quota with the day selected.
  - Quota row → that quota window (§5.8).
  - Intent row → Sessions filtered to tasks of that intent.
  - "stalled" → Sessions filtered `outcome=stalled`, sorted by spend.
  - "correction exchanges" → Delivery & Rework › Corrections.
  - Top-session row → Session detail.
- **Missing/uncertain:** Cursor shows "— no token or quota telemetry". Claude quota shows tokens, not %. The `◌` share is always visible, and `!` legacy rows are counted.
- **Improves workflow:** it immediately shows whether money is going into stalled or correction work rather than completed work.

### 5.4 Sessions (`2`): rank and explore

```
 AI Engineering Analytics · Sessions                       30d · all tools · all projects · $ api-equiv   14:32
 Overview › Sessions                              sort: api-equiv ↓ · totals: tree · filters: none   ILLUSTRATIVE DATA
  #  Started      Tool    Model(s)        Project   Title / first ask                 API-eq   Fresh tok  Agt Ep Outcome     Delivery
  1  Oct 09 13:58 claude  opus-4-6+1      paceflow  Migrate sync payload to v2…      ~$38.20     4.1M     3  4  stalled?    ✎ fix later
  2  Oct 07 09:12 codex   gpt-5.5         vca       Add per-event usage parsing      ~$21.75     2.6M     0  2  completed✓  merged
  3  Oct 03 14:05 claude  sonnet-4-5      web-app   Investigate flaky e2e            ~$17.02     1.9M     1  3  partial h   · no code
  4  Oct 03 14:11 claude  sonnet-4-5      web-app   (subagent of #3 shown in tree)   …
  5  Oct 05 20:40 opencode claude-sonnet  infra     Terraform module refactor        $9.10 r     1.1M     2  1  completed✓  committed
  6  Oct 06 10:02 cursor  auto            web-app   Fix navbar overflow              —          ~210k     0  1  answered?   —
  7  Sep 28 08:30 claude  opus-4-6        vca      ! (source deleted) Cost report…   ~$12.40!    1.4M     —  1  ◌           merged
 ────────────────────────────────────────────────────────────────────────────────────────────────────────────
 214 sessions · Σ ~$412.80 api-equiv · $18.40 reported · 38.2M fresh tok   cols: [v] toggle  [a] tree/self
 Enter open · m mark · c compare · s sort · f filter · / search · K classify selection · Esc back
```

- **Shows:** one row per **root** session with tree totals by default. Subagents fold into `Agt`; `a` switches to self totals and lists subagents as their own rows, indented under the parent. `r` after a $ amount means reported cost.
- **Sort:** api-equiv $, reported $, fresh tokens, total tokens incl. cache read, cache-read ratio, duration, turns, tasks, corrections, failing validations, started time, quota Δ (Codex).
- **Filters** (`f`):
  - time, tool, model (any/primary), project, branch, billing mode
  - agent kind; has subagents; cost/token band
  - intent / outcome / problem flags (with confidence mode)
  - delivery state (none / edited / validated / committed / mainline / PR merged / later-fixed)
  - evidence grade (exclude estimated); classified/unclassified; text search
- **Selection:**
  - `Enter` → Session detail.
  - `Enter` on the `Delivery` cell → Session detail › Code & Delivery tab directly.
  - `Enter` on `Outcome` → Session detail › Tasks, with the deciding task selected.
- **Missing:** Cursor $ is `—` with `~` tokens. A legacy row shows `!`. A row with a subagent count of `—` means the tool cannot report subagents for that source.
- **Improves workflow:** ranking by cost or tokens next to outcome shows "expensive and stalled" directly. Sorting by cache-read ratio exposes context-bloated sessions that should have been restarted or compacted.

### 5.5 Groups (`3`): group by any dimension

```
 AI Engineering Analytics · Groups                         30d · all tools · all projects · $ api-equiv   14:32
 Overview › Groups                  group: cost band (G b) · unit: sessions · sort: Σ api-eq ↓        ILLUSTRATIVE DATA
  Group             Sessions  Σ API-eq   Share  Fresh tok  Med $/sess  Completed✓  Stalled  Corr.exch  Delivered  ◌
  > $20                    4   ~$118.40   29% ██████        11.9M    ~$27.10        1        2      18%        1/4  0
  $5–20                   19   ~$162.30   39% ████████      14.6M     ~$8.20       41%      16%      12%       47%  2
  $1–5                    61   ~$104.70   25% █████          9.1M     ~$1.60       46%       7%       9%       38% 11
  $0.10–1                 98   ~$ 27.40    7% █              2.5M     ~$0.31       38%       3%       4%       21% 30
  < $0.10                 28   ~$  0.00    0%                0.1M     ~$0.02       ·         ·        ·         ·   9
  unpriced (Cursor auto)  14       —        —               ~3.0M       —          36%h      0%       —         14%  6
 ────────────────────────────────────────────────────────────────────────────────────────────────────────────
 Dimensions: b cost band · k token band · m model · p tool · j project · r branch/task · i intent · o outcome
             a agent kind · w weekday/hour · e evidence grade      unit: u sessions ↔ tasks
 Enter open group · s sort · Esc back
```

- **Bands:**
  - Cost bands (API-equiv per root session, tree): `<$0.10`, `$0.10–1`, `$1–5`, `$5–20`, `>$20`.
  - Token bands use **fresh tokens** (fresh input + cache write + output): `<100k`, `100k–1M`, `1–5M`, `5–20M`, `>20M`. Cache reads dominate raw totals and would make every band look huge; `k` again toggles "incl. cache read".
  - Bands are defined in `explore::bands`, with tests.
  - Unpriced sessions always get their own group, never band 0.
- **Unit toggle `u`:** sessions vs **tasks**. Grouping by intent/outcome is most meaningful per task, because a session can be mixed. Task spend comes from `derived_usage_allocation`, so groups sum exactly to the total.
- **Model grouping:** by *primary* model (most fresh tokens) by default. Alternatively `model (any)`, where sessions appear in each model group with **only that model's events** counted, so the column total still equals the global total.
- **Selection:** `Enter` pushes **Sessions** (or Tasks list when the unit is tasks) with the group constraint added, e.g. breadcrumb `Groups(cost band) › $5–20 › Sessions`. `Esc` returns to the same group row with the same sort.
- **Missing:** the unpriced group, `◌` count per group, and `h` on rates computed mostly from heuristic labels (> 50%).
- **Improves workflow:** "which kind of session eats my budget" (bands), "is opus worth it vs sonnet for bug_fix" (model × intent via filter + group), "which project leaks money into stalled work".

### 5.5a Task cost & time (`2`, then `u` to switch Sessions → Tasks; or `3` Groups with unit = tasks)

The Sessions list has a unit toggle `u` (sessions ↔ tasks). In task mode, each row is one task with its session as context:

```
 Overview › Tasks                    30d · all tools · sort: cost→done ↓ · completed only ☐   ILLUSTRATIVE DATA
  Task (first request)                  Session            Harness   Model(s)     Intent      Started      Wall    Active  Cost→done  Done by
  "Migrate sync payload to v2"          Oct 09 ·T3         claude    opus→sonnet  feature     Oct 09 15:31 —       ≤1h40   ~$15.40⌛  not completed
  "Add per-event Codex usage parsing"   Oct 07 ·T1         codex     gpt-5.5      feature     Oct 07 09:12 2h05    1h31    ~$14.20    jev 0.91+commit
  "Token mismatch vs console"           Oct 09 ·T2         claude    opus-4-6     debug_inv?  Oct 09 14:52 1h10    0h58    ~$11.30    jev 0.71
  "Navbar overflow"                     Oct 06 (session)   cursor    auto         bug_fix h   Oct 06 10:02 0h22~   0h22~   —          commit
 412 tasks (171 sessions; 63 sessions split into ≥2 tasks) · completed 64% · ⌛ open 18% · unclear 18%
```

A breakdown matrix (`G` then `x` on the Groups screen with unit = tasks) shows cost and time per **completed** task by model × harness, stratified:

```
 Overview › Groups(tasks: model × harness) · intent = feature · size band 50–300 lines    ILLUSTRATIVE DATA
                      Completed  Completion   Median cost→done   Median active time   P75 cost    First-claimed→
  Harness · model     tasks      rate         (api-equiv)        to done              →done       actually done
  claude · opus-4-6        23      71%         ~$6.80             0h48                ~$11.90      +0h12
  claude · sonnet-4-5      41      68%         ~$2.90             0h55                ~$5.10       +0h20
  codex  · gpt-5.5         19      74%         ~$4.10             0h41                ~$7.30       +0h06
  opencode · sonnet        11      64%         $2.40 r            1h02                $4.00 r      low n
  cursor · auto             9      —           —                  0h39~               —            low n, no tokens
 Strata: i intent · z size band · f framing score · a with/without subagents      n<15 = low n · ~ = estimated
 Enter on a cell → the tasks behind it (Compare-ready) · x explain (definitions, completion sources mix)
```

- **What and why:** cost per task and time per task are the user's headline efficiency measures. Comparing harnesses and models only means something like-for-like, so the matrix is always filtered or stratified by intent and size band. It also shows the completion-source mix, because Jev-determined completions and commit-determined completions measure slightly different things.
- **Columns:**
  - "Completion rate" = completed ÷ (completed + not completed); unclear tasks are excluded and shown in `x`.
  - "First-claimed → actually done" = the time between the first completion and the final completion when a task was reopened after a reported defect. It is a direct measure of premature "done".
- **Missing:** Cursor has no tokens, so it shows `—` cost and `~` times. Without Jev, every task is a whole session; the header then says "tasks = sessions (classification off)", and the completion rate uses commit/validation evidence only.
- **Improves workflow:** it shows which harness and model gets comparable work done fastest and cheapest, how often "done" is premature, and how much is spent after completion (polish loops).

### 5.6 Opened group → contributing sessions

```
 Overview › Groups(intent, tasks) › debug_investigate › Tasks      sort: api-eq ↓      ILLUSTRATIVE DATA
  Task                         Session (tool·model)          API-eq   Turns Attempts Val(f/p)  Outcome      Problems
  "Why are tokens 3x console?"    claude·opus-4-6  Oct 08 #2/3  ~$9.80     14      6     5/1      completed✓   repeated_failure
  "Flaky e2e on CI"               claude·sonnet    Oct 03 #1/3  ~$7.40     11      4     4/0      stalled      env_tooling, rep_fail
  "OpenCode DB locked"            opencode·sonnet  Oct 01 #1/1  $2.10 r     6      2     1/1      completed✓   —
  "Cursor WAL not picked up"      cursor·auto      Sep 30 #1/2  —           5      —     —        partial?     ◌ (user_text only)
 ────────────────────────────────────────────────────────────────────────────────────────────────────────────
 23 tasks · Σ ~$64.10 (16% of period) · stalled 4 (~$19.20) · median attempts 3 · val. pass after ≤2 fails: 61%
 Enter open task in session · [ ] next/prev · m mark · c compare · Esc back to Groups
```

`Enter` opens Session detail with the **Tasks** tab focused on that task. `[`/`]` step to neighbouring tasks in this list without going back.

### 5.7 Session detail

A header plus sub-tabs (`Tab`): **Conversation · Tasks · Agents · Usage · Context · Code & Delivery · Labels**.

```
 Overview › Sessions › claude·opus-4-6 "Migrate sync payload to v2…"                         ILLUSTRATIVE DATA
 claude · paceflow · branch PAC-812-sync-v2 · Oct 09 13:58–17:41 (3h43, active 2h10) · 41 turns · 4 tasks
 API-eq ~$38.20 tree (self ~$24.90, 3 subagents ~$13.30) · fresh 4.1M · cache-read 71M · models opus-4-6 → sonnet-4-5 @T29
 Outcome: stalled? (conf 0.52) · Delivery: 2 commits, 1 on mainline, later fix-signal ✎ · quota: Claude 5h window #Oct09-B
 [Conversation]  Tasks   Agents   Usage   Code & Delivery   Labels
 T  Time   Who        Content (truncated · Enter expands)                         Tools          Tok(fresh) API-eq  Fb
 1  13:58  user       Move sync payloads to v2 schema; keep event_* columns…      ·                 ·        ·
 1  13:59  assistant  I'll start by reading sync.rs and the event tables…         read×6 grep×3   212k   ~$1.90
 2  14:06  user       ok go ahead                                                  ·                 ·        ·    accept
 2  14:07  assistant  Edited sync.rs, db/mod.rs                                   edit×4 test✗      301k   ~$2.60
 3  14:15  user       still failing: E0308 in sync.rs:612                         ·                 ·        ·    correct_defect
 …  ───── task T2 starts (Jev task_assignment: new_task 0.81) · T1 completed at T8 ─────
 17 15:02  assistant  Delegating to subagent "explore-schema"                     agent→ #a1      ·        ·
 …  ───── compaction ─────
 29 16:20  ⚙ model    opus-4-6 → sonnet-4-5 (user /model)
 33 16:41  assistant  cargo test → 3 failed (same as T27)                         test✗ ×3 loop!  644k   ~$5.80
 Enter expand · v toggle tool rows · F jump to next failure/correction · n/p next/prev task · x explain · e edit label
```

- **Conversation:**
  - The turn timeline with per-turn usage and an exchange feedback label (`Fb`).
  - Inline markers: task boundaries (with reason), compactions, model changes, subagent spawns (`Enter` jumps to the Agents tab), and validation results (✓/✗ with the signature on `x`).
  - `F` jumps to the next failure or correction.
  - `Enter` expands a message in a scrollable popup (local text only).
  - Text comes from `fact_session_message`. For `source_missing` sessions the stored text is still shown.

```
 [Tasks]   segmentation: Jev (12 turns assigned, 1 override ✎) · completion source shown per task
  #  Turns        Started  Done      Wall   Active  Cost→done  After  Intent              Outcome     Attempts Validation       Done by
  T1 1–8          13:58    14:44     0h46   0h41    ~$8.20     ~$0.90 feature             completed✓  3        test ✗✗✓         jev 0.88
  T2 9–21,36–38   14:52    16:02     1h10   0h58    ~$11.30    —      debug_investigate?  completed✓  4        test ✗✗✗✓ conv. jev 0.71 + commit
  T3 22–35        15:31    —         ≤2h10  ≤1h40   ~$15.40 ⌛  —      feature             stalled     5        test ✗✗✗✗✗ LOOP  not completed
  T4 39–41        17:30    17:41     0h11   0h11    ~$2.40     —      docs h              answered    0        —                jev 0.64?
 T2 was resumed at T36 ("back to the token mismatch") · T3 ⌛ = spend so far; no completion observed (upper bound shown with ≤)
 Task 3 · attempts:  A1 edit sync.rs(+42/-10) → test ✗ E0599   A2 edit(+8/-3) → test ✗ E0599 (same sig)
                     A3 edit(+15/-15) → test ✗ E0599           A4 thrash: re-added lines removed in A2
 Enter → task transcript slice · e edit label / split / merge / move turn / set completion turn · x why "LOOP" (3 same-signature failures, no new user info)
```

- **Tasks** answers "what did this session actually do, how long did each piece take, and what did it cost to get it done".
  - Each task has start and done times, wall and active time, cost to completion vs. cost after completion, and how the completion was determined (`Done by`).
  - Interleaved tasks show their turn sets.
  - Each attempt shows edits → validation, with converging vs looping marked deterministically, and labels show their band glyphs.
  - `e` allows relabel, split, merge, moving a turn to another task, or setting the completion turn (writes overrides).
- **Without Jev** the tab shows one row, "whole session (classification off)", with completion from commit or validation evidence. `K` offers to segment this session.

```
 [Agents]   totals: self | tree
  Agent                          Kind       Type            Model       Turns  Fresh tok   API-eq   Spawned by   Result
  ● main                         main       —               opus→sonnet   41     2.6M     ~$24.90   —            stalled?
  ├─ a1 explore-schema           subagent   Explore         sonnet-4-5    12     0.9M     ~$ 6.10   T17 Task     returned 1.2k chars
  ├─ a2 run-tests                subagent   general         sonnet-4-5    20     0.5M     ~$ 5.20   T30 Task     ✗ tests still failing
  └─ a3 review-diff              subagent   code-reviewer   haiku-4-5      4     0.1M     ~$ 2.00   T38 Task     returned
  Subagent share of tree spend: 35% · background (no user msg within 10m): a2 18 min
```

- **Agents** shows the parent/child tree with spend. `Enter` on an agent opens it as its own Session detail (pushes a frame); `Esc` returns. For Codex it shows "subagent links unavailable for this rollout format"; for Cursor, best-of-N siblings appear as `best_of_n`.

```
 [Usage]
  By model           Events  Fresh in  Cache wr  Cache rd   Output   API-eq    Grade
  opus-4-6              84    0.41M     0.62M     48.1M     0.21M   ~$19.70   reported, pricing exact
  sonnet-4-5           131    0.38M     0.40M     22.9M     0.30M   ~$18.50   reported, pricing exact
  Per-turn burn ▁▁▂▂▃▂▁▁▂▅▂▁▁▁▂▃▂▂▂▂▃▅▇█▅▃▂▂▁▂▃▅▆▇▅▂▁▁▁▁   peak T33 (loop)
  Quota: Claude 5h window Oct09 13:58–18:58: this session = 61% of fresh tokens in window (~ no % telemetry)
```

- **Usage:** a per-model split (fixes the single-model pricing), per-turn burn, cache-read ratio, and quota-window membership. `Enter` on the quota line → Usage & Quota at that window.

```
 [Context]   (Session detail sub-tab, between Usage and Code & Delivery)                       ILLUSTRATIVE DATA
 Context size per API call (tokens)          window 200k     ▲ compaction   ✕ cache rebuild
 180k ┤                                  ▄▆██▲
 120k ┤                     ▂▄▅▆▆▇▇██████████   ▂▃▄▅▆▇
  60k ┤▅▆▆▇▇▇██✕▇▇▇▇▇▇▇▇▇▇▇██
   0k ┼──────────────────────────────────────────────────── calls 1…214
       ■ baseline 72k (instructions 6k · tools/MCP 31k · skills 4k · unexplained 31k)
       ■ tool results 96k at peak   ■ conversation 21k   ■ subagent returns 3k
 Top context items by total cost (write + carry)      Tokens  Entered  Carried  Write   Carry    Total  Purpose
  Bash  find docs -name '*.md' | xargs cat              38.4k   T3 c6    118     ~$0.48  ~$2.27   ~$2.75  startup · AGENTS.md:12
  Read  crates/vca/src/analytics.rs (whole file)        61.2k   T9 c31    93     ~$0.77  ~$2.85   ~$3.62  used (edited T14)
  mcp github.get_file_contents ×6                       22.0k   T11…      70     ~$0.28  ~$0.77   ~$1.05  unused
  baseline_unexplained (system prompt + schemas)        31.0k   c1       214     ~$0.39  ~$3.32   ~$3.71  ·
  Cache rebuild after 14 min idle (c87)                 104k    —         —      ~$1.30 extra vs hit         ✕
 Σ input-side cost ~$19.80 = items $13.10 + baseline $4.90 + residual $1.80 (unreconciled calls: 3)
 Enter → item detail (raw tool call, every carry step) · g group by tool/dir/MCP/kind · x explain allocation
```

- **Context** shows how the context was built: composition over the session, the most expensive items (a large read early in a long session dominates), cache rebuilds and their extra cost, and the baseline overhead.
- `Enter` on an item opens the turn in Conversation with the tool call expanded. `Enter` on the baseline opens the attachment list (which instructions, skills and MCP listings were injected).
- **Missing:** Cursor shows "context composition unavailable (only context % snapshots)". Residual and unreconciled calls are always displayed.
- **Code & Delivery:** see §5.9 (the same component scoped to this session).
- **Labels:** every label with source, band, probabilities, model version and cache date; override history; "send to classifier" (`K`) and "preview payload" (`P`).

### 5.8 Usage & Quota (`4`)

```
 AI Engineering Analytics · Usage & Quota                  7d · all tools · bucket: hour · $ api-equiv    14:32
 Overview › Usage & Quota                                                                     ILLUSTRATIVE DATA
 API-eq/h  Oct 03                                                      Oct 04
  $9 ┤              █
  $6 ┤             ██▅                                   ▃
  $3 ┤    ▂▃▂    ▂▃███▆▂                    ▂▃▂       ▂▅██▃
   0 ┼────────────────────────────────────────────────────────────────────────────  ▲ spike  ◆ limit hit logged
      08  10  12  14  16  18  20  22  00  02  04  06  08  10  12  14  16  18  20 ◆
 Quota windows                         Start → reset        Observed      Fresh tok   API-eq   Top contributor
 > Codex 5h   (primary, plus)          Oct 03 12:10→17:10   2% → 100% ◆  3.9M        ~$31.20  ≈62% "Add per-event…"
   Codex 7d   (secondary)              Sep 29 → Oct 06      38% → 71%     21.4M       ~$118     ≈24% "Add per-event…"
   Claude 5h  (estimated shape)        Oct 03 13:40→18:40   no % data ◆   5.1M        ~$44.90  41% "Investigate flaky e2e"
   Cursor month                        Oct 01 → Nov 01      —             ~0.9M est   —        —
 Enter open window · ←/→ move cursor in chart · Enter on bar = that hour · b bucket (15m/h/day) · M log limit hit
```

- **Shows:** a burn timeline (API-eq, fresh tokens, or quota %, toggled with `$`) with spikes (▲) and logged limit hits (◆), plus a table of quota windows: observed % where Codex provides it, token burn otherwise.
- **Filters:** time, tool, model, project, include/exclude subagents, include/exclude classifier spend.
- **Selection:**
  - `Enter` on a bar → **Spike/Interval detail**.
  - `Enter` on a window → **Window detail**.
  - `M` logs a limit hit or reset (`fact_quota_observation`) at the cursor time.

```
 Overview › Usage & Quota › Codex 5h Oct 03 12:10→17:10                                       ILLUSTRATIVE DATA
 used%  ▁▁▂▂▃▃▃▄▅▆▇▇██  2%→100% at 15:47 (limit reached, logged by token_count)   snapshots: 214 · gaps>30m: 1
 Contributors (Δ% allocated by token share within each snapshot interval)
  Session                              Agent kind   Δ quota   Fresh tok  API-eq   Activity         Task outcome
  "Add per-event usage parsing"        main          ≈62 pp     2.4M    ~$19.40  interactive      completed✓
  "Refactor apply_patch parser"        main          ≈21 pp     0.8M    ~$ 6.90  background 40m   partial?
  "Codex review of PR #31"             main          ≈14 pp     0.6M    ~$ 4.10  interactive      answered
  unobserved interval 14:02–14:40                     3 pp       —        —      no snapshots
 Concurrency: 2 sessions overlapped for 1h12 (allocation is proportional; not causal)
 Enter → session detail at the turns inside this window · Esc back
```

- **Missing/estimated:** allocations show `≈`, and unobserved intervals are explicit rows. Claude windows say "estimated shape, no % data". If no limit hits are logged, there's a prompt "M to log when you hit a limit".
- **Improves workflow:** it answers "why did my quota vanish at 15:47". Typical findings are a background session or subagent running in parallel, an opus-heavy loop, or a giant cache-read context. The Practices screen turns repeated patterns into advice.

**Usage & Quota › Context & Tools sub-tab** (`Tab` from the timeline). This is the cross-session view of the context ledger.

```
 Overview › Usage & Quota › Context & Tools      30d · claude+codex · group: tool → target dir       ILLUSTRATIVE DATA
  Group                               Calls  Sessions  Tokens in ctx  Write    Carry     Total   /session  Unused  Instr.ref
  Read                                 2,814     188      41.2M       ~$58     ~$212    ~$270    ~$1.44    31%     —
   ├ crates/vca/src/                   1,120     121      22.0M       ~$31     ~$118    ~$149    ~$1.23    18%     —
   ├ docs/                               402      96       9.8M       ~$13     ~$ 61    ~$ 74    ~$0.77    64%     AGENTS.md:12
   └ tests/fixtures/regression/           38      21       4.1M       ~$ 6     ~$ 27    ~$ 33    ~$1.57    81%     —  ← fixture dumps
  Bash (cat/find/rg output)            1,930     170      18.7M       ~$25     ~$ 96    ~$121    ~$0.71    22%     —
  mcp: github                            311      40       6.2M       ~$ 8     ~$ 30    ~$ 38    ~$0.95    47%     —
  subagent returns                       126      58       0.9M       ~$ 1     ~$  4    ~$  5    ~$0.09     —      —
  baseline (instructions/tools/skills)    —      214      15.4M       ~$19     ~$141    ~$160    ~$0.75     ·      CLAUDE.md, 3 MCP servers
  cache rebuilds (idle/model switch)      77      52        —         ~$41 extra                   ~$0.79     ·      —
 Σ allocated ~$735 of ~$790 input-side api-equiv (residual ~$55 shown in x) · Cursor excluded: no composition data
 g group by: tool · target dir · MCP server · kind · instruction ref · read purpose    Enter → contributing items/sessions
```

- **Groups:** tool, target directory (tree, expandable with `→`), MCP server, item kind, instruction reference, or read purpose (startup/used/unused).
- **Sorts:** total, carry, per-session cost, unused %.
- **Selection:**
  - `Enter` on a group → the list of individual context items across sessions (sortable by cost).
  - `Enter` on an item → Session detail › Context with that item selected.
  - `Enter` on an `Instr.ref` cell → the instruction excerpt (local file and line) and the sessions where the referenced read happened vs. didn't.
- **Missing/estimated:** all token sizes are `calib_tokens` (estimated, calibrated to observed growth), carry is `≈` allocated, and residual and excluded tools are stated in the footer.
- **Improves workflow:** it exposes instruction lines or habits that cause expensive reads ("read all of docs/" → 64% unused), MCP servers whose outputs bloat context, fixtures or generated files being dumped, and idle gaps that trigger cache rebuilds. It directly suggests edits to AGENTS.md/CLAUDE.md, `.claudeignore`/permissions, MCP selection, or session habits (new session per task, avoid long idle mid-session).

### 5.9 Delivery & Rework (`5`)

Sub-tabs: **Ladder · Commits · Corrections · Suspected defects**.

```
 AI Engineering Analytics · Delivery & Rework › Ladder      30d · project: vca · unit: tasks   ILLUSTRATIVE DATA
  Intent (code-producing)  Tasks  Edited  Validated✓  Committed  Mainline  PR merged  Survived 14d  Later-fix sig.
  feature                        48     46       35          31        27         22         24/27         3 (L1:2 L2:1)
  bug_fix                        22     22       19          17        16         14         15/16         1
  refactor                        9      9        8           6         6          5          6/6          0
  tests                           7      7        7           5         5          4          5/5          0
  Non-code intents      Tasks  Outcome: answered/decided   Followed by code ≤48h   API-eq
  research_explain              31   24 (77%)                     12                  ~$41
  planning_design               12   10                            9 (plan→feature)   ~$22
  review                         8    8                            3 fixes applied    ~$9
 Enter on any cell → the tasks/commits behind it · x explain stage definitions
```

- **Ladder** (replaces "no-output rate" and session-to-commit as headline metrics): a funnel per intent from edited to survived. Non-code intents are judged by outcome and by downstream use ("followed by code ≤ 48h" via `same_branch_followup`/`same_ticket` relations), never by commits.

```
 Overview › Delivery & Rework › Commits                                                        ILLUSTRATIVE DATA
  SHA      Date    Subject                         AI share  Sessions (tasks)          Mainline  PR     Later
  a1b2c3d  Oct 07  parse codex usage per event       82%     codex "Add per-event…" (#1)    ✓ Oct 07  #31 ✓  —
  9f8e7d6  Oct 09  sync payload v2                   64%     claude "Migrate sync…" (#1,#3) ✓ Oct 10  #33 ✓  fix-sig L1 Oct 12
  44aa01b  Oct 10  fix: payload null branch_name      12%     claude "Fix sync null" (#1)    ✓ Oct 12  #35 ✓  —
  Enter → commit detail: files, matched lines per session/task, PR, mainline, later commits touching AI lines
```

```
 Overview › Delivery & Rework › Commits › 9f8e7d6 › Correction chain                           ILLUSTRATIVE DATA
  origin  claude "Migrate sync payload to v2…" ep#3 (stalled?)  Oct 09  +142 AI lines → commit 9f8e7d6 ✓ mainline
    └─ Oct 10  claude "Fix sync null branch_name" ep#1  intent bug_fix  rework_kind defect_correction (0.83)
         overlap 9 lines sync.rs · commit 44aa01b "fix: …" · issue —     cost of correction ~$3.40 (own task)
    └─ Oct 11  codex "Rename payload fields" ep#2  rework_kind requirement_change? (0.55)  overlap 21 lines
  Evidence level for "AI-related defect": L3 = fix-keyword commit (L1) + Jev defect_correction ≥0.7, no bug issue
  Correction overlay (not additive): origin ~$15.40 + corrections ~$3.40 (defect) / ~$2.10 (req. change)
```

- **Corrections** lists all `touches_earlier_ai_lines` relations, grouped by `rework_kind`, with the correcting task's own cost.
- **Suspected defects** lists commits with evidence levels L1–L4 (§6.4). Each row names its signals, and no row says "caused".
- **Filters:** project, branch/task, tool, model, intent, evidence level, rework kind, time.
- `Enter` from any session/task reference → Session detail; from a commit → commit detail. `Esc` restores the tab and cursor.
- **Improves workflow:** it shows which origin tasks needed defect corrections and what they had in common, for example `framing ≤ 1`, no validation before commit, or a long stalled task. That leads to concrete practices (§6.6).

### 5.10 Compare (`7`)

Mark 2–4 sessions or tasks (or two groups) with `m`, then press `c`.

```
 Overview › Sessions › Compare (2 sessions)                                                    ILLUSTRATIVE DATA
                               A: codex "Add per-event usage"      B: claude "Migrate sync payload"
  Intent / outcome             feature / completed✓                feature / stalled?
  Framing (first ask)          3 (acceptance criteria + test cmd)  1 (goal only)
  API-eq · fresh tok           ~$21.75 · 2.6M                      ~$38.20 · 4.1M
  Cache-read ratio             9×                                  17×  (no compaction until T25)
  Turns · tasks · attempts  18 · 2 · 4                          41 · 4 · 12
  Validation                   test ✗✓ ✗✓ (fail→pass median 1)     test ✗✗✗✗✗ loop (same E0599)
  User corrections             1 (requirement change)              5 (3 defect, 2 misunderstanding)
  Subagents                    0                                   3 (35% of spend)
  Model changes                none                                opus→sonnet mid-loop
  Delivery                     committed → mainline → PR merged    2 commits, later defect-correction L3
  First 300 chars of first ask (Enter for full):
   A: "Parse last_token_usage per token_count… keep event_session_cost columns… run cargo test -p vca codex"
   B: "Move sync payloads to v2 schema"
```

- The rows are fixed "practice dimensions" (framing, validation cadence, context management, delegation, corrections, model use, delivery).
- Group compare shows the same rows as medians and rates with n.
- `Enter` on a row opens the relevant tab for both items side by side (Conversation splits vertically).

### 5.11 Practices (`6`)

```
 AI Engineering Analytics · Practices                       90d · all tools · all projects         ILLUSTRATIVE DATA
  Pattern (deterministic rule + labels)               Tasks  With       Without    Difference              Conf.
  Acceptance criteria in first ask (framing ≥2)         212     68%✓       41%✓       corrections −0.9/ep     n ok
  Validation within first 2 attempts                    188     $2.10/ep   $4.70/ep   stalled 6% vs 19%       n ok
  Loop ≥3 same-signature failures                        23     —          —          ~$38 spent in loops     obs.
  Restart/compact when cache-read >15× fresh             31     —          —          ~$27 in bloated ctx     obs.
  Subagent delegation for exploration                    40     $1.20 main $3.10 main  similar outcomes       low n
  Opus for research_explain tasks                     17     ~$4.90/ep  ~$1.30/ep  same outcome mix        n=17
  Large reads (>20k tok) in first 5 calls               96     ~$3.40/ep  ~$1.90/ep  64% of those unused     obs.
  Cache rebuild after idle >5 min mid-task            77     —          —          ~$41 extra input cost   obs.
  Instruction-referenced startup reads (AGENTS.md:12)    34     ~$0.62 carry/session   used later in 36%      corr.
 Each row: Enter → contributing tasks (with/without) · x → rule definition · correlation, not causation
```

- **Shows:** a fixed library of practice rules (in `explore::practices`), each with `with`/`without` comparisons over tasks. The rules are deterministic over evidence plus labels, and minimum-n guards are enforced (`n < 15` shows "low n").
- `Enter` → the two task lists (pushes a Compare-ready task list).
- **Improves workflow:** this is where the findings become habits (§6.6).

### 5.12 Filter editor (`f`)

```
┌ Filters (applies to this view; Enter on a field to edit, d delete, D clear all) ──────────────┐
│ Time        last 30d            (t: 7d/30d/90d/custom YYYY-MM-DD..YYYY-MM-DD)                  │
│ Tool        claude, codex                                                                      │
│ Project     vca                 (tab-complete from metadata_repositories)                      │
│ Model       any                 primary ▸ any                                                  │
│ Intent      bug_fix, debug_inv. labels: confident+tentative (C to change)                     │
│ Outcome     ≠ completed✓                                                                       │
│ Cost band   ≥ $1                                                                               │
│ Evidence    exclude estimated tokens ☐  exclude legacy ☑                                       │
│ Agent kind  main + subagents (tree)                                                            │
└───────────────────────────────────────────────────────────────────────────────────────────────┘
```

Filters show as chips in the header and are inherited by pushed frames.

### 5.13 Classification consent (`K`)

```
┌ Classify with Jev (OpenRouter · typesafe/jev-1.13) ───────────────────────────────────────────┐
│ Selection: 17 sessions (filter: 30d, project vca) → 41 tasks, 233 exchanges, 12 relations   │
│ Already cached: 29 units · excluded: 3 (project denylist), 2 (source text missing)             │
│ Content level: [conversation] user_text  evidence_only  full     code blocks stripped ✓        │
│ Redactions so far: 4 secrets, 2 emails · zdr=true · fallbacks off                              │
│ Estimated input: 412k tokens · est. cost $0.017 · run cap $0.50 · month left $1.93             │
│ p preview payload of first unit · Enter run in background · Esc cancel                         │
└───────────────────────────────────────────────────────────────────────────────────────────────┘
```

The run shows progress in the footer. Results stream in, and labels change from `◌`/`h` to Jev labels as they arrive.

### 5.14 End-to-end flows

1. **"My Claude quota burned at 3pm"**
   - `4` Usage & Quota → the cursor is on Oct 03 14:00 ▲ → `Enter` gives the interval detail: 3 sessions; one subagent `run-tests` ran 18 min in the background; the main session was in a loop at T27–T33.
   - `Enter` on that session opens Session detail › Usage (peak T33) → `Tab` to Tasks, where task 3 shows LOOP.
   - `Esc`×2 returns to the chart at the same cursor. `M` logs "limit hit 15:47" so the next windows are comparable.
2. **"Which expensive sessions went nowhere"**
   - `3` Groups → `G b` (cost band) → `Enter` on `$5–20` → the Sessions list → `f` Outcome = stalled → `s` sort api-eq.
   - `m` marks two of them, plus one completed session from the same band (use `Esc` back to the band list and `f` to remove the filter) → `c` Compare: framing 1 vs 3, no validation command in the ask.
3. **"Is this AI code causing bugs?"**
   - `5` Delivery & Rework › Suspected defects → filter level ≥ L2 → `Enter` on a commit → Correction chain → `Enter` on the origin task → Session detail › Conversation at the turns that wrote those lines.
   - Shows validation history ("committed after test ✗, never re-run"). `e` marks the relation `requirement_change` if Jev got it wrong.
4. **"Something is reading too much context"**
   - `4` Usage & Quota → `Tab` Context & Tools → group by tool → `→` expand Read → `docs/` row: ~$74 in 30d, 64% unused, `Instr.ref AGENTS.md:12`.
   - `Enter` on the instruction ref shows the excerpt "Before any change, read everything under docs/". Sessions with the read have a median $0.77 carry vs. none without.
   - `Enter` on the top item → Session detail › Context: a 38k-token `find docs | xargs cat` at call 6, carried 118 calls.
   - Action: narrow the instruction to the specific doc. Re-check the same row a week later with `t` set to the new range.
5. **"Planning sessions look like waste in the old report"**
   - `5` Ladder → the non-code section shows planning 10/12 answered, 9 followed by a feature within 48h → `Enter` shows those pairs.

Key-map note: today's `r` (reload) becomes `R`, because `r` now reverses the sort. The 7/30/90 `w` window cycle is kept as a shortcut on top of `t`.

---

## 6. Revisions to delivery, quality and rework metrics

### 6.1 Disposition of existing metrics

| Existing metric | Decision | Why / new form |
|---|---|---|
| Avg prompts per session (S2) | **Secondary** | Turn count isn't good or bad. Replaced as a headline by *user corrections per task* (feedback = correct_*), which counts the turns that signal a problem. |
| Time to first accepted change | **Change** | It penalizes planning and research. It becomes *time to first **validated** change*, computed only for code-producing intents, with the old metric kept in CLI output as secondary. |
| Debug-loop rate (keyword, ≥ 5 turns) | **Replace** | Use the deterministic *unproductive-loop* rate (§3.5) plus Jev `p_repeated_failure`. Keep the old column in `event_session_quality` for sync. |
| Error-paste rate (S6) | **Secondary** | An error paste is often good practice (feeding back evidence). It folds into exchange feedback (`correct_defect`) and validation failures. |
| Session-to-commit within 4h (S9) | **Replace** | It is meaningless for non-code intents and arbitrary for long tasks. Replaced by the **delivery ladder** per task (edited → validated → committed → mainline → PR merged → survived), with a commit counted when its hashes match the task, regardless of the 4h window. |
| No-output session rate | **Replace** | It counts planning/research/review as failures. Replaced by the outcome mix per intent; "no output" survives only as "code-intent tasks without edits". |
| Heavy-AI commit (share ≥ 0.5) | **Keep (as a filter)** | Useful evidence threshold. In the UI the AI share is shown continuously, and delivery views no longer restrict to heavy by default. A `heavy only` toggle exists, so light-AI commits (where many corrections land) become visible. |
| Mainline reach (C2) | **Keep** | Strong observed evidence. Reported per intent and per task; denominators exclude non-code intents. |
| PR reach / PR merge (C1/C3) | **Secondary** | Workflow-dependent (solo repos rarely use PRs). Shown only when PR data exists for the repo. |
| Lead time to mainline | **Keep, secondary** | Observed, but driven by review process more than AI practice. |
| Churn 14d (L1) | **Change** | Renamed *AI-line survival (14d)*. Removed lines are split by the relation/rework kind of the removing work (defect correction vs requirement change vs refinement vs unknown), so a refactor no longer reads as a quality problem. |
| Bug-after-merge (L3) | **Replace with an evidence ladder** | §6.4. |
| Revert rate (L4) | **Keep, secondary** | Rare but unambiguous. |
| Cost report $/LOC, $/mainline commit | **Change** | $/LOC rewards verbosity. Replace with $ per *completed task* by intent and $ per *mainline-reached task*. Remove `HAVING commit_count > 0` so zero-commit spend is visible. |
| Verdict tab "leader/laggard by mainline %" | **Replace** | Replaced by the Overview. Ranking models by mainline % without intent segmentation is misleading. |

The old `session`/`delivery`/`quality`/`cost` CLI commands keep working, with corrected cost numbers. They gain an `--intent` filter and a footer note pointing to the new commands. Their thresholds remain, and the new metrics get their own thresholds only after a baseline of real data (initially displayed without good/watch/risk colouring).

### 6.2 How classification changes segmentation

- **Every rate gets an intent-appropriate denominator:**
  - Delivery rates use code-producing intents (feature, bug_fix, refactor, tests, ops_config, docs when files change).
  - Outcome rates use all intents.
  - Non-code intents use `answered` + downstream use.
- **Unclassified handling:** without Jev, heuristic intents segment the same way, marked `h`. With neither, a "code-intent" proxy is used: a task with ≥ 1 edit call.
- **Mixed sessions** contribute per task, so one session can count in both the planning and feature rows without double-counting spend (allocation is per task).

### 6.3 Progress vs delivery

- **Progress** (inferred): Jev/heuristic `outcome` and `progress` score, per task. It answers "did the conversation get where it wanted".
- **Delivery evidence** (observed): ladder stages from git/PR facts. It answers "did code ship and stay".
- These are always separate columns. The key diagnostic is the 2×2:
  - completed + delivered = healthy
  - completed + not delivered = unverified claims, or work still pending
  - stalled + delivered = shipped despite struggling, a later-fix risk
  - stalled + not delivered = waste candidate

  The Ladder screen offers this 2×2 for code intents (`Tab`).

### 6.4 Suspected AI-related defects (evidence ladder, never causation)

For a commit with AI-matched lines (any share; heavy is a filter):

| Level | Evidence required | Wording in UI |
|---|---|---|
| L0 | AI-added lines later removed/modified within 60d by other work, kind unknown | "AI lines later changed" |
| L1 | L0 + the changing commit/task matches fix keywords (existing regex `analytics.rs:3795`) or the heuristic `defect_correction` | "Later fix signal" |
| L2 | L1 + a bug-labelled issue / fix PR overlap (existing GitHub signal) | "Later fix linked to bug issue" |
| L3 | L1 or L2 + Jev `rework_kind=defect_correction` with confidence ≥ 0.7, or a user override | "Suspected defect correction" |
| L4 | User-confirmed via override `confirmed_defect` | "Confirmed by you" |

- Requirement-change and refinement relations are **excluded** from defect counts and counted separately.
- The UI copy is "touched AI-written lines", never "caused by".
- The existing `event_commit_bug_signal` remains and maps to L1/L2.

### 6.5 Cost of corrections and repeated work (no double counting)

- **Primary measure (additive):** each usage event is allocated to one task, and each exchange is labelled by feedback. Definitions:
  - **Correction spend** = Σ API-eq of turns whose *triggering* user message is `correct_defect`/`correct_misunderstanding`, plus all spend in tasks whose own intent is `bug_fix` with a `defect_correction` relation to earlier AI work.
  - **Loop spend** = Σ spend of attempts inside unproductive loops.
  - **Requirement-change spend** = turns triggered by `change_requirement`.
- These are disjoint by a precedence rule: loop > correction > requirement change > normal. So they sum to ≤ the total, and a stacked bar of the total is valid.
- **Attribution to origins (non-additive overlay):** for each origin task, "downstream correction cost" = Σ cost of later correcting tasks linked by relation. When one correcting task links to several origins, its cost is split by `overlap_lines` share. The overlay is labelled "not additive with totals" and is never included in group sums of the origin's own spend.
- **Repeated work across sessions:** count `touches_earlier_ai_lines` edges by kind, with the later task's cost. The cost is counted once, at the correcting task.

### 6.6 From findings to practice

The Practices rules map each finding to a concrete loop improvement (shown in `x` on each Practices row):
- **Framing:** low framing score correlates with corrections. Include acceptance criteria and the verification command in the first ask.
- **Validation cadence:** commits without a passing validation in the task correlate with later fix signals. Ask the agent to run the specific test after each attempt, and add it to project instructions (AGENTS.md / CLAUDE.md).
- **Loop breaking:** after 2 identical failure signatures, intervene with new information, narrow the scope, or switch model. The UI shows spend after the 2nd identical failure as "avoidable-loop spend (upper bound)".
- **Context hygiene:** a high cache-read ratio and long sessions spanning many tasks call for a new session per task or compaction. The cost delta is shown.
- **Delegation:** subagent share vs outcome. Delegate exploration (cheap model) and keep the implementation in the main thread. Shown only with sufficient n.
- **Model choice per intent:** $/completed task by model × intent.
- **Quota hygiene:** background/subagent share during windows that hit limits.
- **Context hygiene, at the source level:**
  - Instruction lines that trigger broad startup reads (with the unused %): narrow them to specific files.
  - Tools or MCP servers with large outputs: limit them, or use search over full reads.
  - Generated or fixture files read into context: add them to ignore/deny rules.
  - Idle-triggered cache rebuilds: resume promptly, or start fresh.
  - Baseline overhead per project (skills, MCP servers, instruction size): prune unused MCP servers and skills.

---

## 7. Implementation stages

Each stage is mergeable on its own, keeps `cargo test --workspace --all-targets` green, and keeps paceflow sync working.

### Stage 0: Accounting correctness (no new UI)
1. Provider-scoped `session_exists`/`sessions_present` (`db/mod.rs:720,748`); update callers.
2. Claude usage dedupe by `message.id` (`claude/shared.rs:273`); per-event ts and model.
3. Usage normalization `cost::normalize_usage`, with Codex `reasoning_in_output` (`cost/pricing.rs:24`).
4. Per-event Codex usage from `last_token_usage` with timestamps.
5. Per-model costing in `refresh_session_cost_events` (sum over events instead of `MAX(model_name)`). `event_session_cost.model_name` keeps the primary model for sync.
6. Remove `HAVING commit_count > 0` from the grouped cost report.

**Depends on:** nothing. **Risk:** changes cost/token values, which re-uploads `event_session_cost` (§7.4).

### Stage 1: Schema v2 + ingestion enrichment
- Migration runner, `SCHEMA_VERSION=2`, `validate()` update, new columns/tables (§3.3).
- Fingerprint-based re-ingest, `replace_session_facts`, `source_status`.
- Per-provider enrichment (§3.2): Claude subagent files and sidechains; Codex rate limits, tool calls and `custom_tool_call`; OpenCode parent/title/tools/per-message ts; Cursor parent links, branch, tokenCount probe, WAL fingerprint.
- Pricing to TOML with versions and aliases; `billing.toml`.
- New fixtures (§8.1).

**Depends on:** Stage 0.

### Stage 2: Deterministic structure, quota and explore queries
- `analytics/structure.rs`: turns, validation classifier, attempts, tasks, loops, relations, usage allocation. `refresh_session_structure` runs in `ingest` after `refresh_session_events`.
- `analytics/quota.rs`: windows, allocation, spikes; `vca quota mark`.
- `analytics/context.rs`: `fact_api_call`, `fact_context_item` (captured during Stage 1 parsing: attachments, tool inputs/outputs with `contentNotInModelContext` respected), calibration, baseline, carry/write cost, cache events, read purpose, instruction-reference matching.
  - CLI `vca context [--group-by tool|dir|mcp|kind|instruction|purpose] [--session <id>]`.
- `explore/` query module.
- CLI:
  - `vca usage [--group-by tool|model|project|session|agent|day|hour] [--basis api|reported|tokens]`
  - `vca quota [--provider] [--window]`
  - `vca sessions [--sort cost|tokens|…] [--intent --outcome …]`
  - `vca tasks [--group-by model,harness,intent,project] [--intent] [--size-band] [--completed-only] [--sort cost|time]` (whole-session tasks without Jev)
  - `vca show <session-id> [--tasks|--agents|--usage|--delivery]`
  - `vca rework [--kind]`
  - All take `--json`, and output stays tabular like the existing reports.
- Heuristic labels (`classify/heuristic.rs`) are written as `classification_result source='heuristic'`.

**Depends on:** Stage 1.

### Stage 3: TUI framework + exploration screens
- Module split, async loader, nav stack, data table, filters, persistence (§5.1).
- Screens: Overview, Sessions, Groups, Session detail (all tabs except Labels editing), Usage & Quota.
- Remove the Verdict tab. Sessions/Delivery/Quality legacy tab content moves to the Delivery & Rework screen.

**Depends on:** Stage 2.

### Stage 4: Optional Jev classifier
- `classify/` module, config, key storage (reuse the `github/auth.rs` pattern), consent/plan/preview/run/set commands, recorded-response client.
- **Segmentation pass (Q14) and completion pass (Q16) first**, because they unlock task cost and time per task. Then the label questions.
- TUI consent screen, Labels tab, overrides (split/merge/move turn/set completion).

**Depends on:** Stage 2 (units) and Stage 3 (UI parts). The CLI part can land before Stage 3.

### Stage 5: Revised delivery/rework metrics + remaining screens
- Delivery ladder, relation kinds in churn, evidence ladder L0–L4, correction/loop spend, Delivery & Rework screen, Compare, Practices.
- Update `docs/USER_GUIDE.md`, `README.md` (provider assumptions: Claude subagent files, Codex rate_limits, OpenCode cost semantics, Cursor gaps), and `docs/ARCHITECTURE.md`.

**Depends on:** Stages 2 and 4 (it works with heuristic labels if Stage 4 slips).

### Stage 6: Evaluation tooling and calibration
- `vca classify eval` (§8.2), a TUI gold-labelling mode, and threshold tuning written back to `questions.toml` defaults.

**Depends on:** Stage 4.

### 7.4 Compatibility

- **paceflow sync:** `event_*` shapes, keys and column names are unchanged. Values in `event_session_cost` (tokens, `estimated_cost_usd`) change after Stage 0/1, so their content hashes change and those events re-upload once. That's expected and is mentioned in release notes and the PR description.
  - No new event types are synced.
  - Add a paceflow test asserting the `collect_sync_events` SQL still runs against a v2 schema (`crates/paceflow/src/sync.rs` tests, in-memory schema built via `vca::db::init_app_schema`).
- **DB version:** v2 is rejected by older binaries. Both binaries ship together. Legacy import (`data_home.rs`) runs `migrate_v2` after import.
- **CLI:** all existing commands and flags are preserved. The TUI keymap change (`r` → `R`) is documented in `?` help.
- **Data preservation:** no migration deletes rows that can't be re-derived. `--fresh` warns about source-missing sessions.
- **Performance:** structure derivation is incremental per session (only sessions whose fingerprint or ingest version changed, plus relation recompute for affected repos). Relation matching uses the existing `(side, line_hash)` indexes.

### 7.5 Failure handling summary

- **Provider parse error for one session:** record it in `change_parse_errors` (extended with `stage='session'`), keep the previous facts for that session, and continue.
- **Unknown model:** `price_match='none'`, unpriced. It shows in the "unpriced" group with the model name, so the user can add it to `~/.vca/pricing.toml`.
- **Missing rate-limit data:** the quota UI shows estimated shapes only.
- **Classifier unavailable:** heuristic labels and `◌` remain, and all screens work.
- **TUI query error:** shown in the panel with `R` retry. Other panels keep working (per-panel results).

---

## 8. Tests, classifier evaluation and acceptance criteria

### 8.1 Tests (following AGENTS.md: inline `#[cfg(test)]` + `tests/` regression with insta)

**Fixtures** (sanitized via `crates/vca/examples/sanitize_regression_fixtures.rs`, placed in `tests/fixtures/regression/home_template/`):
- A Claude main transcript with repeated `message.id` usage lines, a model switch, a compaction, and Task tool_use + `subagents/agent-*.jsonl` (2 agents).
- A Claude legacy sidechain transcript.
- A Codex rollout with ≥ 3 `token_count` events and changing `rate_limits` (window reset mid-file), `custom_tool_call apply_patch`, an `exec_command` running `cargo test` with exit codes 101 then 0, and `turn_aborted`.
- An OpenCode DB built in `install_opencode_fixture` extended with `parent_id`, `title`, tool parts, and `providerID`.
- A Cursor composer with `subagentComposerIds`, `createdOnBranch`, and bubbles with and without `tokenCount`.

**Unit tests (examples of names):**
- `dedupes_claude_usage_by_message_id`
- `codex_reasoning_not_double_billed`
- `codex_usage_events_sum_to_final_total`
- `session_exists_is_provider_scoped`
- `reingests_session_when_fingerprint_changes`
- `keeps_facts_when_source_missing`
- `ingests_claude_subagents_as_child_sessions`
- `parses_codex_rate_limit_snapshots`
- `classifies_validation_commands` (table-driven)
- `segments_session_into_tasks_with_recorded_jev_answers`
- `whole_session_is_one_task_without_jev`
- `resumed_task_collects_non_contiguous_turns`
- `task_assignment_options_list_only_open_tasks`
- `uncertain_assignment_falls_back_to_continue`
- `completion_reopened_after_defect_report`
- `cost_to_complete_stops_at_completion_turn`
- `task_costs_sum_to_session_cost`
- `active_time_excludes_idle_gaps`
- `detects_unproductive_loop_vs_converging_iteration`
- `usage_allocation_is_one_to_one`
- `task_spend_sums_to_session_spend`
- `quota_delta_allocation_sums_to_window_delta`
- `pricing_alias_matches_cursor_model_names`
- `cost_bands_boundaries`
- `relation_detects_later_removal_of_ai_lines`
- `correction_spend_categories_are_disjoint`
- `context_items_respect_content_not_in_model_context`
- `context_calibration_reconciles_to_observed_growth`
- `context_allocation_sums_to_call_input_cost`
- `detects_cache_rebuild_after_idle`
- `carry_cost_stops_at_compaction`
- `matches_read_targets_to_instruction_references`
- `attachments_classified_into_baseline_kinds`

The Claude fixture gains `attachment` records (instructions, skill_listing, mcp_instructions_delta, deferred_tools) and a large Read result.

**Migration tests:**
- Open a v1 DB snapshot (created by the current code in-test), migrate, and assert the legacy rows are flagged and preserved.
- `ingest` re-derives sessions with sources and keeps the source-missing ones.

**Regression snapshots (insta)** for `vca usage`, `vca quota`, `vca sessions --sort cost`, `vca show <id> --tasks --agents`, and `vca rework` on the fixture corpus. Existing snapshots are updated in Stage 0 with an explanation of each delta (cost fixes).

**Ingest idempotency:** extend `ingest_is_idempotent_for_fixture_corpus`. A second ingest yields identical row counts and hashes for all new tables. Appending lines to a fixture transcript then re-ingesting updates only that session.

**Classifier:**
- `RecordedClient` replays stored JSON responses. Tests cover the state builder (truncation, redaction, budget, level degradation), request schema serialization (compared against the documented Decisions schema), caching (no re-send for unchanged digest), override precedence, failure mapping per HTTP status (local mock server as in paceflow `sync.rs` tests), and budget stop.
- A live test is `#[ignore]` with `OPENROUTER_API_KEY` (like `github_live.rs`).

**TUI:** `TestBackend` render tests per screen with seeded `explore` rows. Navigation tests (push, pop restores filters/sort/selection/scroll; sibling navigation), and a loader test with a fake query executor (no DB on the UI thread). Insta snapshots of rendered buffers at 118×40 and 80×24.

**paceflow:** the `collect_sync_events` compatibility test on the v2 schema; the existing metrics regression copy updated.

### 8.2 Classifier evaluation

- **Gold set:** `vca classify label --sample 150 --stratify tool,heuristic_intent` opens a TUI labelling mode. For each unit it shows the same content the classifier would see (at `conversation` level) and asks the user to choose labels, *without* showing Jev's answer. It writes `classification_gold(origin='eval_session')`. Overrides are also added as gold with `origin='override'` (biased toward errors, so they are reported separately).
- **Metrics** (`vca classify eval [--question] [--source jev|heuristic]`):
  - Per question: accuracy, macro-F1, and a confusion matrix.
  - Noul: AUROC + Brier.
  - Calibration: a reliability table over 5 confidence bins, plus ECE.
  - **Coverage-accuracy curve:** accuracy of answers above each confidence threshold vs the fraction covered, used to pick per-question bands.
  - Jev vs heuristic baseline on the same units. A question's Jev labels are shown as "confident" by default only if Jev beats the heuristic macro-F1 by ≥ 0.10 and calibration ECE ≤ 0.10. Otherwise that question shows as tentative everywhere until recalibrated.
- **Segmentation and completion quality:**
  - In labelling mode the user marks task boundaries and the completion turn for 40 sampled multi-task sessions.
  - **Boundary F1** (±1 turn tolerance) and **turn-assignment accuracy** (incl. resumes) are reported for Jev vs. the whole-session baseline.
  - **Completion-time error** = median |Jev completed_at − gold| in minutes, plus the % within 10 min, compared with commit-evidence completion.
  - **Impact check:** cost-to-complete per task computed with gold vs. Jev segmentation (median absolute % difference). This tells the user how far task-level cost numbers can be trusted.
  - Jev segmentation is enabled by default only if boundary F1 ≥ 0.75. Otherwise the UI marks task splits as tentative.
- **Self-consistency:** re-label 30 units after ≥ 7 days and report the user's own agreement (κ). This is an upper bound on achievable accuracy for subjective labels such as `refinement` vs `requirement_change`.
- **Stability:** re-send 30 unchanged units and measure label flip rate. This detects nondeterminism and is reported, not assumed zero.
- **Segment checks:** accuracy by tool, by privacy level (does `user_text` lose much vs `conversation`?), and by task length bucket (truncation impact).
- **Model change protocol:** a new Jev version runs only on the gold set first (`--model typesafe/jev-x --gold-only`), with a side-by-side report before the user switches the pinned model.

### 8.3 User-facing acceptance criteria

1. After upgrading, `vca ingest` prints the corrected-accounting summary. Claude token totals for a session match the deduped sum of unique `message.id` usage (verified against fixture expectations), and Codex cost equals tokens × price without reasoning double-counting.
2. A session still running during one ingest shows its additional turns and tokens after the next ingest.
3. `vca usage --group-by model` totals equal `--group-by tool` totals equal the global total (additivity), with estimated and unpriced amounts shown separately.
4. Claude subagent sessions appear under their parent in `vca show --agents` and the TUI Agents tab, with self and tree totals. Global totals don't change when toggling tree/self.
5. Usage & Quota shows Codex 5h/7d windows with observed % from local `rate_limits`. Selecting a window lists contributing sessions with `≈` allocated deltas summing to the window's observed delta (± unobserved intervals, shown explicitly). Claude/Cursor windows show token burn and the "no % data" label.
6. In the TUI, from Groups → group → session → task → commit → correction chain → origin session and back with `Esc` at each step, the filters, sort, selected row and scroll position are restored at every level.
7. With classification disabled, every screen renders. Labels show `h` (heuristic) or `◌`, and no network call is made (asserted by a test with no client configured).
8. `vca classify plan` shows counts, estimated tokens and cost, and exclusions. `preview` shows the exact redacted payload. Nothing is sent without `--yes` or the TUI confirmation. The run stops at `--max-usd`.
9. A long session with several goals is split into tasks after `vca classify run`. Each task shows start, completion time (with its source), wall and active time, cost to completion and cost after completion. Task costs sum to the session cost. Without Jev, the same views show one task per session with commit/validation-based completion. `vca tasks --group-by model,harness --intent feature` prints the stratified cost/time-per-task table.
10. Manual label overrides persist across `ingest`, `classify run` and `ingest --fresh` (overrides are keyed by stable natural keys; documented exception: units whose source text was lost).
11. Delivery & Rework never uses causal wording. Every suspected-defect row shows its evidence level and signals.
12. For a Claude or Codex session, the Context tab shows per-call context size (observed), the baseline split into named instruction/tool/skill items plus `unexplained`, and the top items by write + carry cost. Item + baseline + residual equals the session's input-side cost. `vca context --group-by dir` lists directories by total context cost with the unused % and instruction references.
13. `paceflow sync push` works against a v2 DB, and the only payload changes are corrected cost/token values.

---

## 9. Assumptions and limitations

- **Quota:**
  - Only Codex records quota percentages locally (`rate_limits` in `token_count`). Claude Code, Cursor and (generally) OpenCode don't, so their quota views are token burn in assumed window shapes plus user-logged observations. Percentages are never inferred from tokens.
  - Window shapes (Claude 5h + weekly; Cursor monthly) are configurable defaults, not facts.
- **Money:**
  - API-equivalent cost uses list prices from a versioned table and may differ from real charges (discounts, batch, priority tiers, long-context premiums, regional pricing, Cursor's own pricing).
  - OpenCode `cost` is provider-reported and may be 0 or meaningless on subscription routes.
  - No provider billing APIs are queried in this release.
- **Cursor:** token counts are estimates unless bubble `tokenCount` exists (unverified until the Stage 1 probe on a real DB). The model is often `auto` and unpriced. Cursor will remain the weakest-evidence tool, and the UI says so.
- **Codex subagents:** parent links depend on the current rollout format, which needs a real sample in Stage 1. Until then Codex child linkage is shown as "unknown".
- **Claude transcript retention:** sessions older than Claude Code's cleanup period exist only in our DB. Legacy rows for those can't be corrected for the duplicate-usage bug, so they stay flagged `!`.
- **Jev:**
  - Verified as a decision-only model (noul/choice/score) via OpenRouter's **alpha** Decisions endpoint, with a 32k prompt cap and input-only billing ($0.042/M at time of writing).
  - The alpha API may change; it is isolated behind `DecisionClient`, with 400s surfaced clearly.
  - The TypeSafe data-retention policy wasn't found in the public docs. The plan sends `zdr: true` and fails closed. **The user should confirm the policy before enabling hosted classification.**
  - Labels are probabilistic and are presented with confidence bands. They are never used for accounting.
- **Context ledger:**
  - Per-item token sizes are estimates calibrated to observed per-call growth. They are not tokenizer-exact.
  - Hidden system prompts and tool schemas that aren't logged appear only as `baseline_unexplained`.
  - Harness-side truncation of tool output is handled only where the transcript marks it (`contentNotInModelContext`).
  - Cache TTL behaviour is inferred from usage patterns.
  - "Instruction caused this read" is a path-reference correlation, not proof.
  - Cursor has no composition data.
- **Causality:** relations between AI-written lines and later fixes are correlational evidence. Line-hash matching misses semantically identical rewrites and can match common lines (mitigated by the `incidental` label and minimum-overlap thresholds).
- **Scope:** single user, single device, local data. Team analytics, cross-device merge and new backend events are explicitly out of scope. Existing paceflow features continue unchanged apart from corrected values.
- **Task segmentation and completion** are Jev judgments, or whole-session plus commit evidence without Jev. Errors are correctable via split/merge/move/completion overrides, and quality is measured in evaluation (§8.2). Without Jev, long multi-purpose sessions show as a single task, and that is stated in the UI.
- **"Completed"** means the goal appeared achieved in the conversation (or code was committed). Whether it shipped and stayed is the separate delivery ladder.

---

## Critical files

- **Ingestion:** `crates/vca/src/providers/{claude/shared.rs,claude/mod.rs,codex/mod.rs,opencode/mod.rs,cursor/mod.rs,cursor/shared.rs}`, `crates/vca/src/commands/ingest.rs`, `crates/vca/src/change_intel/pipeline.rs`
- **Storage:** `crates/vca/src/db/mod.rs` (+ new `db/migrations.rs`), `crates/vca/src/data_home.rs`
- **Accounting:** `crates/vca/src/cost/pricing.rs` (+ `pricing.toml`), `crates/vca/src/analytics.rs` (`refresh_session_cost_events`, `query_cost_report`, bug-signal derivation), new `analytics/structure.rs`, `analytics/quota.rs`
- **New modules:** `crates/vca/src/explore/`, `crates/vca/src/classify/`, `crates/vca/src/tui/`
- **CLI:** `crates/vca/src/cli.rs`, `crates/vca/src/app.rs`, `crates/paceflow/src/cli.rs` (inherits automatically via flatten)
- **Compatibility tests:** `crates/paceflow/src/sync.rs` (tests), `crates/{vca,paceflow}/tests/metrics_regression.rs`, `tests/startup_shared.rs`
- **Reuse:** `reconcile_source_tool_writes` (change_intel/storage.rs:73), the line-hash tables and `(side,line_hash)` indexes, `FIRST_CHANGE_IDLE_GAP_MINUTES`, error-signature normalization (analytics.rs:3481), the fix-keyword regex (analytics.rs:3795), the GitHub bug-issue signal, the `github/auth.rs` token storage pattern, the paceflow mock HTTP server test helpers, and `test_support::lock_env`.

## Verification (for the implementer, per stage)

- `cargo fmt --check`, `cargo clippy --workspace --all-targets --all-features`, `cargo test --workspace --all-targets`.
- Run `cargo run -p vca -- ingest` on the fixture HOME and on a real HOME, then `vca usage --group-by tool`, `vca quota`, `vca sessions --sort cost`, `vca show <id> --agents`, and compare totals for additivity.
- `cargo run -p vca -- tui`: walk the flows in §5.14 and confirm back-navigation state restoration.
- With `OPENROUTER_API_KEY`: `vca classify plan`, `preview`, `run --max-usd 0.05` on 5 tasks; check `classifier_request.cost_usd` against the OpenRouter activity page; run `vca classify eval` after labelling.
