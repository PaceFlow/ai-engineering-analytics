# Observe engineering work and setup changes

The observational workflow is: ingest everyday work, record a setup change, compare periods, inspect the contributing goals, and decide what to keep. It does not run agent benchmarks or compute a universal skill score. A change after installing a skill is an association, not proof that the skill caused it.

## Start here

```sh
vca ingest
vca changes capture --timezone Europe/Berlin
vca changes add "Installed Kubernetes skills" --at 2026-10-01 --kind installation
vca trends
vca trends --compare
vca trends --around 1 --json
vca work list
vca work show WORK_ID
```

These commands also work with `paceflow` in place of `vca`. New reports default to the current Git repository. Use `--repo /path/to/project` or `--all-projects` to choose explicitly. Outside a repository the text report identifies its all-project scope. Reports are read-only and never send conversations for classification.

`trends` shows eight completed Monday–Sunday weeks and the current partial week. `--daily`, `--from YYYY-MM-DD`, and `--to YYYY-MM-DD` select other buckets or inclusive calendar dates. `--compare` defaults to four completed weeks versus the preceding four; an explicit range compares to the previous equal-length period. `--baseline-from` and `--baseline-to` select a different baseline. `--around ID` compares 28 calendar days before and after a milestone, excluding its day. An unfinished after period is provisional; the baseline is not shortened.

`--provider`, `--model`, `--intent`, `--verified-only`, and `--include-estimated` apply consistently to both periods. Repository/provider/model selection includes a whole work item when one of its member tasks matches; mixed work is retained as one goal. Breakdown keys list its combined repositories or provider/models rather than counting it twice.

Set the persisted reporting timezone with `changes capture --timezone IANA_ZONE`. Ingest otherwise records the detected local IANA zone, with a visible UTC fallback if detection fails. `trends --timezone` overrides the zone for that report. Usage dates and activity boundaries use this zone, including daylight-saving transitions.

## Read the numbers

A work item represents one goal and begins as a standalone session task. Accepted continuation and reopening relationships join member tasks; a bug fix that corrects an earlier feature remains a separate goal. Root conversations count once and subagents roll up to their parent. Concurrent activity intervals are unioned within each goal. Summed activity across independent goals is **work activity**, not measured human working hours.

- Cycle time runs from the first observed request to the latest supported completion after reopening. Initial completion remains visible in work detail. Open work has an age, not a fabricated cycle time.
- Completion requires an explicit manual mark or a classified goal completion with a compatible outcome. A session ending, commit, or passing command alone does not close the goal. Research and planning may complete through an answered question or decision. `--verified-only` restricts completed-work distributions; expenditure and overall state counts remain visible.
- Activity uses the existing 30-minute idle-gap threshold. Intervals split at calendar boundaries; tokens belong to the usage event's date. Work spanning weeks does not assign all expenditure to its starting week.
- Completed-work distributions include median, p90, sample count, missing/excluded count, and contributor IDs. Fewer than ten samples is flagged, without a significance claim. Completion-date cohorts include lifecycle usage through completion. Work spanning a milestone is excluded from lifecycle attribution, while its dated expenditure remains in totals.
- Goal mix and activity mix are separate. A feature can contain debugging. Count, time, and token shares include unclassified work and show absolute totals. A falling bug-fix share can therefore be distinguished from an actual drop in bug-fix workload.
- Native tokens have explicit uncached-input, cache-write, cache-read, output, and additional-reasoning components. Reasoning already included in output is not added again. Legacy `fresh_tokens` keeps its existing meaning, including output.
- Native, estimated, mixed, missing, and undated usage remain identifiable. Native-only is the default efficiency cohort. Missing usage excludes a work item from token-efficiency distributions instead of becoming zero. Estimated results are available with `--include-estimated`.
- Request-context distributions and peak context are separate from cumulative token expenditure. Reported cost and API-equivalent cost remain separate; unavailable costs are null.
- Reopening and later-correction rates use a seven-day follow-up window by default (`--follow-up-days`). Recent completions without a complete observation window remain pending. A successful provider ingestion must cover the window; merely waiting without ingesting does not mature the rate. These rates, bug-fix workload, and debugging activity are distinct signals, not interchangeable counts of AI-caused bugs.

JSON includes schema and metric versions, effective scope, reporting timezone, cutoff, period boundaries, label versions, coverage, and contributing IDs. The shared query service supplies CLI and TUI. Classifier spending is kept separate from engineering-session usage.

## Correct work membership and labels

```sh
vca work suggestions --json
vca work link codex:SESSION_A#0 codex:SESSION_B#0 --note "Same requested goal"
vca work link WORK_MEMBER_A WORK_MEMBER_B --kind corrects_previous_work
vca work accept RELATION_ID
vca work reject RELATION_ID
vca work unlink WORK_MEMBER_ID
```

Supported relationship kinds are `continuation`, `reopens`, `corrects_previous_work`, and `unrelated`. An explicit reference to one prior conversation with a continuation/reopening request can establish automatic membership within the same repository. Branch, shared references, similar request wording, and changed-line overlap supply suggestions, not membership. Classifier relationship answers remain suggestions for review. Explicit manual links can cross repositories. Manual decisions override automatic discovery and persist across ingestion.

Member identities use provider, conversation, and user-message anchors. A changed or missing anchor suspends relationships and saved task-label/completion overrides for review; a numeric task index cannot silently move the decision to another request. Existing classification commands and TUI task-edit actions can set corrected labels and completion marks. Saved ambiguous decisions remain in persistent storage, and report coverage identifies pending review.

The TUI adds **9: Trends**. Tab switches timeline, work mix, comparison, setup changes, and daily work. Enter opens contributing work items, then member tasks and their existing conversations/events. On the list screen, `u` cycles sessions, tasks, and work items. Use `L` to review links, `a`/`d` to accept/reject a selected suggestion, `l` to enter member IDs, and `n` to record a milestone. Existing task-label editing and Skills/Context detail navigation remain available. Queries and actions use the background loader. Skills obey active project/provider/date filters; the matched cohort is descriptive and retains the existing intent, agent-role, stage, and recorded-listing selection rules.

## Setup history

`changes list` shows both manual milestones and automatic observations. `changes add/edit/remove` manages milestones; `--all-projects` makes a milestone global. An optional `--observation ID` attaches source evidence. `changes capture --root /extra/skill/root` saves an additional inventory root for future ingestion.

Capture hashes relevant `SKILL.md` and known project instruction files; it does not store their contents or copy provider credentials/configuration. It records skill listings, file loads, injections, explicit calls, provider version, model, and tools when available in logs. File locations distinguish filesystem identities. Session observations retain their provider, repository, source and session evidence; unresolved names do not prove an installed version.

Today's scan establishes today's file state. It cannot reconstruct an old session's skill version or installation date. A change is bounded by observations. Removal requires a complete inventory; partial listings cannot establish absence. Manually dated milestones supply dates that the automatic evidence cannot establish. Comparisons list other recorded changes in the same interval.

## Optional incremental classification

```sh
vca classify auto enable
vca classify auto status --json
vca ingest
vca classify auto disable
# Extend the next ingest's historical coverage explicitly:
vca classify auto enable --backfill-days 180
# Optional budget override for one project; no separate activation is needed:
vca classify auto limits --repo /path/to/project --monthly-budget 1
```

There is one global opt-in: enabling the external classifier automatically covers every current and future ingested project. `classifier.json`'s `enabled` setting controls activation; saving a key alone does not turn it on. The next ingestion processes eligible conversations, and newly discovered projects inherit activation without another enable command. Work without repository attribution is included in an explicitly identified unassigned bucket. Existing `allow_projects`/`deny_projects`, redaction and content-level settings still apply. No daemon is added. Reports never trigger classification.

Default initial coverage is 56 days when a project first becomes eligible. Sessions active within 30 minutes are deferred. New or changed inputs and relationship dependencies are processed; unchanged inputs reuse the cache. Both periods should be classified under the same question policy before interpreting changes; old/mixed label coverage is flagged. Status shows the global switch plus each project's deferred/pending work, last errors, coverage start, exclusions and spending. A missing key leaves processing pending for all included projects.

Default caps are $2 per project/month, $2 globally/month, and $0.50 for an automatic ingestion run across all projects. `auto enable --monthly-budget` changes the global monthly cap, `--project-monthly-budget` sets the default for newly discovered projects, and `--max-run-usd` sets the automatic run cap. Existing project limits are preserved; `auto limits --repo` changes an individual budget or historical coverage without changing activation. Unattributed work has its own budget bucket and also consumes the global cap. The most restrictive remaining limit governs dispatch. Manual and automatic classifier entry points share a process/file lock. Responses update spending immediately, estimates are reconciled with reported cost, and an overrun is visible. Exhaustion, missing credentials, and request failures leave ingestion successful with classification pending.

Legacy `classifier_auto_project` rows retain their budgets, history horizon and spending references. Their former activation flag no longer controls sending: activation is global, with exclusions expressed through the classifier's allow/deny configuration. Global disable stops future automatic processing for all projects while preserving labels, cached results and spending history. Explicit manual runs retain their existing confirmation flow.

## Compatibility and reproducibility

The additive schema upgrade is version 3 → 4 (including the preceding 2 → 3 additions). Existing provider facts, API fields, and Paceflow backend export contracts are retained. Work identities/decisions, setup observations, milestones, and opt-in state are persistent; derived activity and membership tables are rebuildable. Insight refreshes publish under a SQLite savepoint. Source facts remain usable even when original transcripts have disappeared; unavailable historical evidence remains unknown.

The session parser version advances to 5 for OpenCode V2 and project-identity extraction. Available sources are reread; missing transcripts are not fabricated. Corpus snapshots include `analytics-inputs.json` containing allowlisted persistent labels, assignments, work decisions, observations, milestones, and reporting timezone. They exclude keys, automatic-classification opt-in and classifier spending. Replay restores these inputs before rebuilding analytics. Old snapshots without this file still replay. This reproduces analytics, not agent performance.

## OpenCode V2 and coverage corrections

OpenCode ingestion supports both `session`/`message`/`part` and `session_v2`/`session_message`. V2 typed messages are authoritative when an ID exists in both stores. Migrated copies are never added together. While a migrated V2 session has no messages, available V1 messages remain authoritative. Existing user-message part identities are preserved when their text matches the migrated message; ambiguous changes retain the normal review safeguards.

V2 extraction includes embedded text and tools, native model/provider identities, instruction sources, errors, model switches, compactions and their usage, and idle execution outcomes. Reasoning is counted separately from output for OpenCode. The new `state.metadata.files` diffs and `input.path` write format are supported alongside legacy tool metadata. Source snapshots remain read-only and exclude credential/configuration tables in corpus capture.

The provider's `project.worktree` and project ID supply recorded project identity even after an individual task directory disappears. Original working directories and the source of this attribution are retained in `fact_session_project`; a current Git checkout is not required to retain historical identity. OpenCode's global `/` project is excluded from repository attribution. This is provider-recorded evidence, not a guess from a similar directory name.

Incremental fingerprints include storage layout, session metadata, message/part counts, and update timestamps. Updating an earlier message triggers re-ingestion even if the session header's timestamp does not change. Unknown session stores, unsupported message types and parsing failures surface as source-coverage errors. Reports preserve known history and explicitly warn that absent activity is not zero. Automatic classification defers sessions from failed sources.

`work-trends-2` excludes conversations with no message, tool or positive-usage activity from work-start, open-work, mix and efficiency denominators. Their session facts and work identities remain inspectable as `unobserved`, and reports show how many empty conversations were excluded. Existing session/export fields retain their meanings.

Runtime `succeeded`/`failed` outcomes are exposed as execution evidence in work detail, classifier inputs and period reports. They never create a goal-completion mark by themselves. Completion-unknown and heuristic-only reports show explicit warnings; unequal native-usage coverage is flagged in comparisons. `classify auto status` includes known projects before global activation, with their pending/deferred backlog and blockers such as a missing classifier key. Recording or displaying a project does not enable a globally disabled classifier.

Reports expose the earliest observed activity and flag periods that begin before recorded history. Those earlier dates have unverified coverage, even when their aggregates are empty. This matters when the default four-week baseline predates the available session logs. Work membership and link queries use indexes and cached candidate descriptions so growing histories do not repeatedly scan every relationship for every goal.


## Daily work, PRs and deployments

```sh
vca daily
vca daily --all-projects --from 2026-10-01 --to 2026-10-27 --timezone Europe/Berlin --json
vca daily --repo /work/project --csv
vca daily --cohorts --follow-up-days 7
vca daily --details 2026-10-01 --metric completed --offset 0 --limit 50
vca status --json
```

The default daily view includes 28 completed local calendar days and the current partial day. Daily usage belongs to its event date; activity is split at local midnight. JSON contains explicit token components, model/provider usage, goal and stage mix, work state, completion evidence coverage, attempts, corrections, source watermarks and publication revision. CSV uses the same query results. Summary JSON omits the potentially large contributor lists: use `--details` with `started`, `active`, `completed`, `bug_work` or `deployed` for paginated identities. Work IDs open with `work show`; PR IDs open with `sources show`.

Work-item completion remains independent of delivery. Work-item cycle time ends at supported goal completion; engineering delivery lead time ends at the first supported live deployment of linked work. PR lead time runs from creation to merge, and deployment lag from merge to live deployment. Reviews can complete before the implementation they review. Research, planning and operations require no PR to complete. Model breakdowns describe usage; delivery outcomes are not assigned to a single model.

Bug-fix work counts goals. Bug-fix PR counts pull requests, with intent provenance from manual overrides, configured labels/template fields, classifier inference, or unknown. Neither is a count of distinct bugs. PR-creation cohorts measure deployment within a fixed follow-up window; younger cohorts and those with incomplete source history remain pending. A deployment today is never divided by today's starts. Shared effort remains attached to work; incomplete or shared attribution is excluded from per-PR efficiency distributions.

### Read-only repository sources

Create a JSON profile containing metadata and environment-variable **names**, never credential values:

```json
{
  "name": "project-github",
  "provider": "github",
  "repository": "organization/project",
  "repo_root": "/work/project",
  "token_env": "GITHUB_TOKEN",
  "live_environments": ["production"],
  "required_components": ["default"],
  "intent_labels": {"bug": "bug_fix", "feature": "feature"}
}
```

```sh
vca sources add /private/project-source.json
vca sources sync
vca sources status
vca sources list
vca sources show 'github:git:github.com/organization/project#123'
vca sources link MEMBER_ID PR_ID --role implementation --decision accepted --note 'Explicit artifact reference'
vca sources intent PR_ID bug_fix
vca sources alias /old/worktree git:github.com/organization/project
```

Bitbucket Cloud uses `"provider":"bitbucket"`, `"repository":"workspace/repository"` and a token environment variable. An optional `email_env` names the email variable for API-token Basic authentication. GitHub uses the saved analytics token when `token_env` is omitted, respecting the invoking binary's credential context. Profiles are explicitly configured; no production service configuration is read. Configured sources also refresh during ingestion. Every request is read-only; pagination stays on the configured API origin. Rate limits retry with a bounded delay; cursors and collected records survive interruption. Ancestry checks retain both positive and negative results, with at most 256 new checks per sync; a paused scan remains incomplete until resumed.

Discovery includes all open PRs, PRs created/updated within 56 days, and older explicitly referenced PRs. It runs independently of session/commit matching. Changed PRs fetch commits, filenames and review/lifecycle summaries, without fetching extensive comments or patches. Explicit PR references and recorded commit attribution establish contributions. Contribution roles are implementation, review, verification, coordination, investigation and recovery; a contribution link does not merge goals. Manual acceptance/rejection takes precedence across refreshes. Ambiguous work identities suspend attribution pending review.

A successful live deployment needs an artifact, configured live environment, timestamp and source evidence. Merge, unrelated successful CI and an assistant's claim do not qualify. Provider ancestry comparisons can associate squash/rebase merges with later deployed artifacts. Native GitHub deployment status events and Bitbucket release/deployment state are supported. Unknown Bitbucket merge timestamps remain unknown rather than using the PR's last-update time.

An optional GitHub `deployment_jobs` profile field can explicitly map a deployment job: each entry needs `workflow_id`, `job_name`, `environment`, `component` and `artifact_from: "head_sha"`. Configure this only when the pipeline guarantees that job deploys the run's head SHA. Generic CI jobs are not discovered as deployments. Multiple components must all succeed for the same environment/artifact; imported components with different artifacts can share a stable `release_id`. `rollback_of` references a prior deployment record and retains its history without erasing the earlier delivery.

### Deployment-system imports and offline replay

`vca sources import FILE` accepts an atomic versioned JSON envelope:

```json
{
  "schema_version": 1,
  "records": [
    {"kind":"deployment","record":{
      "id":"release-42-web-success","provider":"custom","repo_key":"git:github.com/organization/project",
      "artifact_sha":"EXACT_COMMIT_SHA","environment":"production","component":"default",
      "status":"success","live":true,"occurred_at":"2026-10-01T12:00:00Z",
      "evidence_url":"https://deploy.example/releases/42","evidence_source":"pipeline_export",
      "required_components":["default"],"included_commits":[]
    }}
  ]
}
```

NDJSON accepts one tagged record per line. Supported kinds are `pull_request`, `deployment` and `coverage`; fields use the same versioned record types. Coverage records explicitly identify source/provider/repository, observed interval, ingestion time, completeness, kind and optional error. Importing deployment events alone does not prove a complete observation window. Use reliable pipeline exports; assistant assertions are not accepted as delivery proof. Corpus snapshots retain allowlisted PR, deployment, link, intent and coverage inputs; replay requires no network or credentials. Source credential profiles and classifier spending are excluded.

### Freshness, missing measurements and resumable classification

Event time, source observation intervals, ingestion time and report generation time are separate. Snapshot ingestion does not advance its event watermark to today. Optional adjacent `SOURCE.coverage.json` metadata can establish a snapshot's actual coverage. Missing watermarks, truncated history, stale snapshots, partial days and failed collection remain explicit. Source observations retain a history. An empty day without complete coverage is not proven inactivity.

Usage availability distinguishes native measurements, estimates, explicit measured zero, missing data and suspicious zero-filled legacy records. A zero in an older transcript is conservatively uncertain unless the source explicitly establishes measurement availability. Legacy token fields retain their meaning; unavailable cost stays null.

Classification jobs persist selected passes, per-request progress, content fingerprints and spending. Manual `classify run --resume ID` reuses completed requests and the remaining job/monthly/project budgets. A changed classifier policy requires a new run. Planning respects selected passes. Core labels, completion and stages run before relationship/skill analysis; later ingestions process the latter after core coverage is current. Four concurrent requests share one coordinator that reserves budgets, persists responses as they arrive and owns database writes. Routine ingestion stops dispatching after 60 seconds, finishes in-flight requests, and retains pending work. Final derived refreshes publish atomically. Status distinguishes completion decisions processed from newly established completions; classifier expenditure remains separate from engineering usage.

### Persistent analytics home and rollback

`VCA_HOME` names a parent directory; the database is `$VCA_HOME/.vca/vca.db`. Use a private persistent directory such as `/home/user/.local/share/vca`. Before moving existing data, stop analytics writers and take a consistent SQLite backup (including committed WAL data). Copy only the analytics database and allowlisted private classifier/token files, preserve mode 0600, and retain the previous binary with its compatible pre-upgrade database. Do not copy production application configuration. Validate the migrated database and reconcile work IDs, token totals and labels before switching the analytics launcher. Roll back using the paired binary/database backup; an old binary may reject schema 4. This migration requires no changes to application services.
