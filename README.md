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
