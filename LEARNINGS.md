# Learnings

Lessons from incidents and investigations that the code alone doesn't make obvious.
Add an entry when something cost real time to find. Keep each entry short: what happened,
why, and the rule to follow now. Newest first.

## Queues keyed by repository path must not fan out to every alias (2026-11)

**What happened:** an incremental `vca ingest` after a parser-version bump wrote ~800 GB in
3 hours (while reading 4.6 GB) and never finished. The database grew from 8.4 GB to 21 GB.

**Why:** `mark_hashes_dirty` queued each edited line hash in `commit_assoc_dirty_hash` under
*every* path of the repository (all `repository_alias` rows). Multica creates one workdir per
issue, so one repository had ~850 aliases. Only paths that store commits (`fact_commit.repo_root`)
ever read or clear that queue, so the rest grew forever (55M rows, ~19 GB, 0.15% useful). Each
re-read session added its hashes ×850 as random-key inserts into B-trees far bigger than the page
cache, so the same pages were rewritten over and over (WAL plus checkpoint).

**Rules:**
- A per-path queue or state row must be written only for paths that something reads and clears.
  For commit association, that means paths with rows in `fact_commit`.
- Expect alias counts in the hundreds or thousands (one per Multica issue workdir). Anything that
  costs O(aliases × edits) per session is a bug.
- Every queue needs a consumer that drains it. A table that only ever grows is a red flag.

## Investigating slow or I/O-heavy ingests

- The `Stage: <name>` label stays until the next stage starts, so work between stages is shown
  under the previous label. For example, `record_repository_aliases` runs under "Session Events",
  before "Commit Association".
- Measure, don't guess: `/proc/<pid>/io` (`write_bytes` vs `read_bytes`) sampled every few
  seconds, and `SELECT name, SUM(pgsize) FROM dbstat GROUP BY name ORDER BY 2 DESC` on a read-only
  copy (`file:...?mode=ro`) to see which table or index holds the space.
- A killed ingest still keeps everything it committed. A copy of a database from a killed run is
  not the pre-run state; check `session_source_state.ingest_version` before reusing it.
- To reproduce a parser-version re-read on a copy, lower `session_source_state.ingest_version`
  for that provider.
- Validate on a copy (`sqlite3 "file:src?mode=ro" ".backup dst"`), never on production, and run
  the classifier with `env -u OPENROUTER_API_KEY` so nothing is spent.

## SQLite migrations on large tables

- To remove most rows of a big table, copy the rows you keep into a new table, `DROP` the old one
  and rename. `DROP` frees pages with reads only, while `DELETE` rewrites the whole table.
- The file doesn't shrink after that: freed pages are reused, and only `VACUUM` gives the space back.
- One-time data migrations are recorded in `schema_migrations`. Numbers in use: 2 and 3
  (`db/session_store.rs`), 4 (`change_intel/schema.rs`), 102 (accounting summary marker in
  `commands/ingest.rs`). Check other branches before you claim a number.
- A primary key that starts with column X already serves lookups on X. A separate index on X only
  adds write cost.
