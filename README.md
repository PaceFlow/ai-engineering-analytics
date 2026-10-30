# AI Engineering Analytics

Two installable products share one analytics implementation:

| Package | Command | What it includes |
| --- | --- | --- |
| [vibe-coding-analytics](crates/vca/README.md) | `vca` | Local ingestion, session/delivery/quality/cost reports, interactive dashboard, event export, optional GitHub PR metrics |
| [Paceflow](crates/paceflow/README.md) | `paceflow` | All VCA analytics plus optional Paceflow backend synchronization, schedules, and setup hooks |

```bash
cargo install --locked vibe-coding-analytics
cargo install --locked paceflow
```

Install either or both. Each package installs only its own executable: `vibe-coding-analytics` installs `vca`; `paceflow` installs `paceflow`. Prebuilt archives are available in [GitHub Releases](https://github.com/PaceFlow/ai-engineering-analytics/releases), with `cargo binstall vibe-coding-analytics` and `cargo binstall paceflow` support.

```bash
vca ingest
vca session
vca delivery
vca quality
vca cost
vca tui
```

Conversation analytics: `vca usage`, `vca quota`, `vca sessions`, `vca tasks`, `vca show`, `vca context` and `vca rework`. Optional, opt-in task splitting and labels: `vca classify`. See the [VCA guide](crates/vca/README.md#conversations-usage-quota-and-tasks).

In `vca tui`, open a session for its **Overview**: request count and peak context, context sources and estimated repeated footprint, tool calls by type, and skills observed. Use Tab for the Conversation, Context, Skills, and Turn Context details; select a Context request and press `x` to inspect its origins. `vca show SESSION --turn-context` gives the same source-only turn progression without prompt text. User turns are coding-agent boundaries and are not asserted to be external runs. Press `8` for skill frequency across sessions. Explicit `skill` tool calls count as invocations; listings, direct injections, and file loads appear separately. When a native listing identifies the skill, the frequency view can compare tasks with the same intent, role, and stage; otherwise relevant opportunities remain unknown. Press `w` in the session Skills tab to compare downstream usage through task completion, session end, or the next skill call. These windows can overlap and include other work, so their tokens and costs are not a causal price for a skill. Current repository skill files can supply a fingerprint and optional classifier context, but the file's historical version is unverified unless the harness captured it. OpenCode's native usage and cost fields may be empty even when an external gateway charged for the request; gateway-only spend is unavailable here. When native usage is absent, session detail shows token and cost figures as unavailable and still reports captured context characters, tools, and skills.

## Automatic history discovery in 0.3.0

Both commands use the same analytics database at `~/.vca/vca.db`. On the first data-using command, either executable automatically discovers and copies previous history from these locations:

- `~/.paceflow/paceflow.db`
- `~/.aieng/aieng.db`
- `~/.aea/aea.db`
- `~/.vibe/vca.db` (original VCA)

Switching directly from an older Paceflow installation to VCA preserves your history without requiring you to run the new Paceflow first. Help and version commands do not migrate or create data.

An existing valid `~/.vca/vca.db` always wins. Otherwise, the most recent usable historical database is selected by recorded session, code-change, or commit activity. If no activity timestamp exists, file modification time is used. Ties follow the order above. The application prints its selection and retains every original; databases are not merged.

Migration includes committed SQLite WAL data, upgrades and validates a temporary copy, and installs it atomically under a cross-process lock. Interrupted migration is retryable. If historical files exist but none can be read or upgraded, the command explains the failure rather than starting with empty history. A saved GitHub token is copied from the selected installation only if no shared token exists. Backend credentials stay separate under `~/.paceflow`.

Analytics-home precedence is `VCA_HOME`, `PACEFLOW_HOME`, `AIENG_HOME`, `AEA_HOME`, then your normal home. Each override names a **parent home directory**, not a database file; discovery is limited to that home. For example, `VCA_HOME=/tmp/analytics` puts shared data in `/tmp/analytics/.vca/vca.db`.

Upgrade older executables before using them again: they continue writing to their old databases, and changes made there after migration are not automatically merged. Both 0.3.0 commands share ingestion, reports, caches, and GitHub credentials. `ingest --fresh` rebuilds this shared data and resets upload cursors; it preserves credentials and original source history.

### Recovery

Keep the original historical databases until you have verified the upgrade. If migration fails, fix permissions or restore a valid backup at the reported source path and retry. If a canonical database already exists, it is never overwritten automatically, including when it is corrupt. Stop running analytics commands and move the canonical database and any `-wal`/`-shm` sidecars aside to a backup location before restoring a complete backup or retrying discovery. Do not delete the historical originals to troubleshoot migration.

## Development

The Cargo workspace has exactly two members: `crates/vca` supplies the `vibe-coding-analytics` package, library, and VCA command; `crates/paceflow` supplies backend integration and the Paceflow command. The shared implementation supports Claude Code, Codex, Cursor, and OpenCode history plus git metadata; GitHub PR metrics require an optional saved token or environment override.

```bash
cargo build --workspace
cargo run -p vibe-coding-analytics -- --help
cargo run -p paceflow -- --help
cargo test --workspace --all-targets
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo install --path crates/vca --locked
cargo install --path crates/paceflow --locked
```

The default workspace member is Paceflow, so existing `cargo run -- ingest` workflows continue to work. The fixture sanitizer is a developer example: `cargo run -p vibe-coding-analytics --example sanitize_regression_fixtures`.

Provider assumptions and override names are described in the [VCA guide](crates/vca/README.md) and [Paceflow guide](crates/paceflow/README.md). Do not commit local databases, generated reports, or build artifacts.

## Releasing

Both crates use the workspace version. A `vX.Y.Z` tag must match both manifests. The release workflow runs checks on Linux, Windows, and macOS, verifies extracted source packages, builds both products' archives, publishes VCA before Paceflow, and creates the GitHub release after both succeed. Publication no longer runs on pushes to `main`.

See [release instructions](packaging/PUBLISHING.md) and [0.3.0 release notes](packaging/RELEASE_NOTES.md).

## Observe work over time

Use `vca daily` for day-by-day model use, bug-fix work, supported completion, effort and rework, with supplemental PR/deployment evidence. `vca status` shows freshness, uncertainty and classification jobs. Use `vca trends` for eight completed weeks plus the current partial week, `vca trends --compare` for four-week comparisons, and `vca trends --around ID` to inspect changes around a setup milestone. `work` and `changes` manage cross-conversation goals and setup history. Enable the external classifier once with `vca classify auto enable`: every current and future ingested project is then processed automatically within the configured exclusions and spending caps. The same commands are available in Paceflow; the TUI adds `9: Trends`. See [Observing work](docs/OBSERVING_WORK.md) for definitions, evidence limits, examples, migration, and reproducible corpus replay.

OpenCode ingestion supports both V1 and V2 stores, deduplicates migrated sessions, and retains provider-recorded project identity after temporary work directories disappear. V2 tool calls, instruction sources, compaction usage and execution outcomes are captured. Runtime success does not establish goal completion. Empty startup conversations are excluded from trend work counts, while missing usage, classification and source coverage remain visible. Parser version 5 re-reads available history; existing facts are retained when sources are unavailable. See [OpenCode V2 and coverage corrections](docs/OBSERVING_WORK.md#opencode-v2-and-coverage-corrections) for provider assumptions and migration behavior.

For long-conversation diagnosis, use `vca sessions --sort active`, `--sort elapsed`,
or `--sort requests`, followed by `vca work show <id> --explain --evidence`.
[Investigation, lifecycle import and schema 5 notes](docs/OBSERVING_WORK.md#investigating-long-conversations-schema-5)
explain supported completion, corrective-purpose coverage and source drill-down.
