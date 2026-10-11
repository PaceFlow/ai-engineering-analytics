# VCA and Paceflow 0.3.0

This release separates the local analytics product into `vibe-coding-analytics` (Vibe Coding Analytics) and `paceflow`. Each crate installs only its own command. Paceflow includes the same analytics and adds backend synchronization, schedules, and setup hooks.

Install either or both with `cargo install --locked vibe-coding-analytics` and `cargo install --locked paceflow`, or choose the matching prebuilt archive.

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

## Configuration and compatibility

VCA uses `VCA_GITHUB_TOKEN`, `VCA_CURSOR_STATE_PATH`, `VCA_CURSOR_HISTORY_PATH`, and `VCA_OPENCODE_DB_PATH`. Paceflow retains the corresponding `PACEFLOW_*` overrides with shared `VCA_*` fallbacks. Backend credentials and scheduling remain in `~/.paceflow`; VCA does not authenticate with or contact the Paceflow backend.

Existing Paceflow hooks and schedules continue invoking `paceflow`. Upgrade that executable before continuing scheduled use. Normalized event identities and backend payloads are preserved.

## Release assets

Both products have Windows x86_64 ZIPs, Linux x86_64 tarballs, macOS ARM64 tarballs, and SHA-256 checksums. ZIP and tarball layouts both include a product-and-target directory to match `cargo-binstall` metadata.
