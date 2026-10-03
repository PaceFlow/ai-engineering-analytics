//! Shared analytics storage and migration from historical application names.
use anyhow::{Context, Result, bail, ensure};
use fs2::FileExt;
use rusqlite::{Connection, OpenFlags};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, UNIX_EPOCH};

const LEGACY_DATABASES: &[&str] = &[
    ".paceflow/paceflow.db",
    ".aieng/aieng.db",
    ".aea/aea.db",
    ".vibe/vca.db",
];
pub const SCHEMA_VERSION: i64 = 1;

pub fn home() -> Result<PathBuf> {
    ["VCA_HOME", "PACEFLOW_HOME", "AIENG_HOME", "AEA_HOME"]
        .into_iter()
        .find_map(|key| {
            std::env::var_os(key)
                .filter(|v| !v.is_empty())
                .map(PathBuf::from)
        })
        .or_else(dirs::home_dir)
        .ok_or_else(|| anyhow::anyhow!("Home directory not found"))
}

pub fn directory() -> Result<PathBuf> {
    Ok(home()?.join(".vca"))
}

pub fn database_path() -> Result<PathBuf> {
    Ok(directory()?.join("vca.db"))
}

/// The operating system releases this lock on drop or process termination.
pub struct DataLock {
    _file: File,
}

pub fn lock_at(home: &Path) -> Result<DataLock> {
    let directory = home.join(".vca");
    fs::create_dir_all(&directory)?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(directory.join("analytics.lock"))?;
    let started = Instant::now();
    loop {
        match file.try_lock_exclusive() {
            Ok(()) => return Ok(DataLock { _file: file }),
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                ensure!(
                    started.elapsed() < Duration::from_secs(30),
                    "Analytics database is busy. Wait for the other analytics command to finish and retry."
                );
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(err) => return Err(err.into()),
        }
    }
}

pub fn ensure_ready() -> Result<()> {
    let home = home()?;
    let _lock = lock_at(&home)?;
    prepare_at(&home)
}

/// Called while the caller owns the data lock, before opening the database.
pub(crate) fn prepare_at(home: &Path) -> Result<()> {
    let target = home.join(".vca/vca.db");
    if target.exists() {
        inspect(&target).with_context(|| format!(
            "Cannot use analytics database {}. Preserve this file and restore a valid backup before retrying.", target.display()))?;
        return Ok(());
    }
    let mut candidates = Vec::new();
    let mut failures = Vec::new();
    for relative in LEGACY_DATABASES {
        let path = home.join(relative);
        if !path.exists() {
            continue;
        }
        match inspect(&path) {
            Ok(activity) => candidates.push((path, activity)),
            Err(err) => failures.push(format!("{}: {err:#}", path.display())),
        }
    }
    if candidates.is_empty() {
        ensure!(
            failures.is_empty(),
            "Found previous analytics databases, but none can be migrated. Originals were preserved. Restore a valid backup and retry:\n{}",
            failures.join("\n")
        );
        return Ok(());
    }
    // Stable sort preserves the historical-name priority for timestamp ties.
    candidates.sort_by(|a, b| b.1.total_cmp(&a.1));
    let mut migration_errors = failures;
    for (source, _) in &candidates {
        match migrate(source, &target) {
            Ok(()) => {
                eprintln!(
                    "Imported existing analytics from {} into {}. The original was preserved.",
                    source.display(),
                    target.display()
                );
                for (other, _) in &candidates {
                    if other != source {
                        eprintln!(
                            "Retained alternative database: {} (not merged).",
                            other.display()
                        );
                    }
                }
                for failure in &migration_errors {
                    eprintln!("Retained database that could not be migrated: {failure}");
                }
                return Ok(());
            }
            Err(err) => migration_errors.push(format!("{}: {err:#}", source.display())),
        }
    }
    bail!(
        "Found previous analytics databases, but none can be migrated. Originals were preserved. Restore a valid backup and retry:\n{}",
        migration_errors.join("\n")
    )
}

fn read_only(path: &Path) -> Result<Connection> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    conn.busy_timeout(Duration::from_secs(5))?;
    Ok(conn)
}

/// Validate application identity, integrity, and supported schema without writes.
fn inspect(path: &Path) -> Result<f64> {
    let conn = read_only(path)?;
    let integrity: String = conn.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
    ensure!(
        integrity == "ok",
        "SQLite integrity check failed: {integrity}"
    );
    let version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    ensure!(
        (0..=SCHEMA_VERSION).contains(&version),
        "Unsupported analytics schema version {version}"
    );
    for (table, required) in [
        ("metadata_sessions", &["id", "provider", "session_id"][..]),
        ("metadata_repositories", &["id", "repo_root"][..]),
    ] {
        let columns = columns(&conn, table)?;
        ensure!(
            required
                .iter()
                .all(|name| columns.iter().any(|col| col == name)),
            "Not a supported analytics database: {table} is missing required columns"
        );
    }
    let mut latest: Option<f64> = None;
    for (table, names) in [
        ("metadata_sessions", &["started_at", "ended_at"][..]),
        ("fact_session_code_change", &["change_ts"][..]),
        ("fact_commit", &["commit_time"][..]),
    ] {
        let available = columns(&conn, table)?;
        for name in names {
            if !available.iter().any(|column| column == name) {
                continue;
            }
            let sql = format!(
                "SELECT MAX((julianday(\"{name}\") - 2440587.5) * 86400.0) FROM \"{table}\""
            );
            let value: Option<f64> = conn.query_row(&sql, [], |row| row.get(0))?;
            if let Some(value) = value.filter(|v| v.is_finite()) {
                latest = Some(latest.map_or(value, |previous| previous.max(value)));
            }
        }
    }
    Ok(latest.unwrap_or(
        fs::metadata(path)?
            .modified()?
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64(),
    ))
}

fn columns(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let mut statement = conn.prepare(&format!("PRAGMA table_info(\"{table}\")"))?;
    Ok(statement
        .query_map([], |row| row.get(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

fn migrate(source: &Path, target: &Path) -> Result<()> {
    let original = read_only(source)?;
    // A fresh unique staging directory also isolates abandoned copies after a crash.
    let stage = tempfile::Builder::new()
        .prefix("migration-")
        .tempdir_in(target.parent().unwrap())?;
    let staged = stage.path().join("vca.db");
    original.backup("main", &staged, None)?;
    {
        let copy = Connection::open(&staged)?;
        crate::db::init_app_schema(&copy).context("Cannot upgrade historical analytics schema")?;
        crate::analytics::create_reporting_views(&copy)
            .context("Cannot validate reporting schema")?;
        for view in [
            "view_session_metrics_base",
            "view_change_metrics_base",
            "view_session_cost",
        ] {
            copy.prepare(&format!("SELECT * FROM {view} LIMIT 0"))
                .context("Historical reporting schema is incompatible")?;
        }
        copy.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    }
    inspect(&staged)?;
    let token_source = source.parent().unwrap().join("github_token");
    let token_target = target.parent().unwrap().join("github_token");
    // Import before the database rename so a retry still discovers the token.
    if token_source.exists() && !token_target.exists() {
        let token =
            fs::read_to_string(&token_source).context("Cannot read previous GitHub token")?;
        if !token.trim().is_empty() {
            use std::io::Write;
            let mut token_copy = tempfile::NamedTempFile::new_in(target.parent().unwrap())?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                token_copy
                    .as_file()
                    .set_permissions(fs::Permissions::from_mode(0o600))?;
            }
            token_copy.write_all(token.trim().as_bytes())?;
            token_copy.as_file().sync_all()?;
            token_copy.persist_noclobber(&token_target)?;
        }
    }
    // Windows requires a writable handle to flush the completed database copy.
    OpenOptions::new().write(true).open(&staged)?.sync_all()?;
    fs::rename(&staged, target)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;

    fn legacy(home: &Path, relative: &str, session: &str, date: Option<&str>) -> Result<PathBuf> {
        let path = home.join(relative);
        fs::create_dir_all(path.parent().unwrap())?;
        let conn = Connection::open(&path)?;
        crate::db::init_app_schema(&conn)?;
        conn.execute("INSERT INTO metadata_sessions(provider, session_id, started_at) VALUES ('codex', ?1, ?2)", params![session, date])?;
        Ok(path)
    }

    fn prepare(home: &Path) -> Result<()> {
        let _lock = lock_at(home)?;
        prepare_at(home)
    }

    fn sessions(home: &Path) -> Result<Vec<String>> {
        let conn = read_only(&home.join(".vca/vca.db"))?;
        let mut statement =
            conn.prepare("SELECT session_id FROM metadata_sessions ORDER BY session_id")?;
        Ok(statement
            .query_map([], |row| row.get(0))?
            .collect::<rusqlite::Result<_>>()?)
    }

    #[test]
    fn migrates_each_historical_name_and_preserves_original_and_sync_cursors() -> Result<()> {
        for relative in LEGACY_DATABASES {
            let temp = tempfile::tempdir()?;
            let source = legacy(
                temp.path(),
                relative,
                "preserved-session",
                Some("2026-09-30T12:00:00Z"),
            )?;
            let original = Connection::open(&source)?;
            original.execute("INSERT INTO fact_sync_event_state VALUES ('org', 'session', 'event', 'hash', '2026-10-01', 'checkpoint')", [])?;
            drop(original);
            let before = fs::read(&source)?;
            prepare(temp.path())?;
            assert_eq!(sessions(temp.path())?, ["preserved-session"]);
            assert_eq!(fs::read(&source)?, before);
            let copy = read_only(&temp.path().join(".vca/vca.db"))?;
            let checkpoint: String = copy.query_row(
                "SELECT last_server_checkpoint FROM fact_sync_event_state",
                [],
                |row| row.get(0),
            )?;
            assert_eq!(checkpoint, "checkpoint");
        }
        Ok(())
    }

    #[test]
    fn upgrades_actual_original_vca_schema_without_reingestion() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let source = temp.path().join(".vibe/vca.db");
        fs::create_dir_all(source.parent().unwrap())?;
        let conn = Connection::open(&source)?;
        conn.execute_batch(include_str!("db/fixtures/vca_0_1_0.sql"))?;
        conn.execute("INSERT INTO metadata_sessions(provider, session_id, started_at) VALUES ('codex', 'original-vca', '2026-01-01')", [])?;
        drop(conn);
        prepare(temp.path())?;
        assert_eq!(sessions(temp.path())?, ["original-vca"]);
        Ok(())
    }

    #[test]
    fn selects_recorded_activity_and_uses_stable_priority_for_ties() -> Result<()> {
        let temp = tempfile::tempdir()?;
        legacy(
            temp.path(),
            LEGACY_DATABASES[0],
            "paceflow",
            Some("2026-01-01"),
        )?;
        legacy(
            temp.path(),
            LEGACY_DATABASES[1],
            "aieng",
            Some("2026-09-01"),
        )?;
        legacy(temp.path(), LEGACY_DATABASES[2], "aea", Some("2026-09-01"))?;
        prepare(temp.path())?;
        assert_eq!(sessions(temp.path())?, ["aieng"]);
        // Once selected, a later historical update never replaces the canonical DB.
        legacy(
            temp.path(),
            LEGACY_DATABASES[0],
            "later",
            Some("2026-10-03"),
        )?;
        prepare(temp.path())?;
        assert_eq!(sessions(temp.path())?, ["aieng"]);
        Ok(())
    }

    #[test]
    fn compares_normalized_timezones_and_commit_and_change_activity() -> Result<()> {
        let temp = tempfile::tempdir()?;
        legacy(
            temp.path(),
            LEGACY_DATABASES[0],
            "paceflow",
            Some("2026-09-01T12:00:00+02:00"),
        )?;
        let source = legacy(
            temp.path(),
            LEGACY_DATABASES[1],
            "aieng",
            Some("2026-09-01T09:00:00Z"),
        )?;
        let conn = Connection::open(&source)?;
        conn.execute("INSERT INTO fact_session_code_change(provider, session_id, change_ts, source_kind) VALUES ('codex', 'aieng', '2026-09-02', 'test')", [])?;
        drop(conn);
        prepare(temp.path())?;
        assert_eq!(sessions(temp.path())?, ["aieng"]);
        Ok(())
    }

    #[test]
    fn migrates_committed_wal_data_with_open_source_connection() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let source = legacy(temp.path(), LEGACY_DATABASES[0], "initial", None)?;
        let conn = Connection::open(&source)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "wal_autocheckpoint", 0)?;
        conn.execute(
            "INSERT INTO metadata_sessions(provider, session_id) VALUES ('codex', 'wal-session')",
            [],
        )?;
        assert!(PathBuf::from(format!("{}-wal", source.display())).exists());
        prepare(temp.path())?;
        assert_eq!(sessions(temp.path())?, ["initial", "wal-session"]);
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM metadata_sessions", [], |r| r
                .get::<_, i64>(0))?,
            2
        );
        Ok(())
    }

    #[test]
    fn imports_only_github_token_and_never_backend_credentials() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let source = legacy(temp.path(), LEGACY_DATABASES[0], "session", None)?;
        fs::write(
            source.parent().unwrap().join("github_token"),
            "ghp_previous\n",
        )?;
        fs::write(
            source.parent().unwrap().join("sync_config.json"),
            "backend-secret",
        )?;
        prepare(temp.path())?;
        assert_eq!(
            fs::read_to_string(temp.path().join(".vca/github_token"))?,
            "ghp_previous"
        );
        assert!(!temp.path().join(".vca/sync_config.json").exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(temp.path().join(".vca/github_token"))?
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        Ok(())
    }

    #[test]
    fn preserves_an_existing_shared_token() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let source = legacy(temp.path(), LEGACY_DATABASES[0], "session", None)?;
        fs::write(source.parent().unwrap().join("github_token"), "old-token")?;
        fs::create_dir_all(temp.path().join(".vca"))?;
        fs::write(temp.path().join(".vca/github_token"), "new-token")?;
        prepare(temp.path())?;
        assert_eq!(
            fs::read_to_string(temp.path().join(".vca/github_token"))?,
            "new-token"
        );
        Ok(())
    }

    #[test]
    fn errors_on_invalid_or_future_databases_without_creating_empty_history() -> Result<()> {
        for future in [false, true] {
            let temp = tempfile::tempdir()?;
            if future {
                let source = legacy(temp.path(), LEGACY_DATABASES[0], "future", None)?;
                Connection::open(source)?.pragma_update(None, "user_version", 99)?;
            } else {
                fs::create_dir_all(temp.path().join(".paceflow"))?;
                fs::write(temp.path().join(LEGACY_DATABASES[0]), b"not sqlite")?;
            }
            let err = prepare(temp.path()).unwrap_err();
            assert!(err.to_string().contains("none can be migrated"));
            assert!(!temp.path().join(".vca/vca.db").exists());
        }
        Ok(())
    }

    #[test]
    fn corrupt_canonical_database_is_never_replaced_by_legacy_data() -> Result<()> {
        let temp = tempfile::tempdir()?;
        legacy(temp.path(), LEGACY_DATABASES[0], "old", None)?;
        fs::create_dir_all(temp.path().join(".vca"))?;
        fs::write(temp.path().join(".vca/vca.db"), b"corrupt")?;
        assert!(prepare(temp.path()).is_err());
        assert_eq!(fs::read(temp.path().join(".vca/vca.db"))?, b"corrupt");
        Ok(())
    }

    #[test]
    fn accepts_empty_valid_history_and_retries_after_abandoned_staging_copy() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let source = legacy(temp.path(), LEGACY_DATABASES[0], "remove", None)?;
        Connection::open(source)?.execute("DELETE FROM metadata_sessions", [])?;
        fs::create_dir_all(temp.path().join(".vca/migration-interrupted"))?;
        fs::write(
            temp.path().join(".vca/migration-interrupted/vca.db"),
            b"partial",
        )?;
        prepare(temp.path())?;
        assert!(sessions(temp.path())?.is_empty());
        prepare(temp.path())?;
        Ok(())
    }

    #[test]
    fn uses_commit_activity_and_modification_time_fallback() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let first = legacy(temp.path(), LEGACY_DATABASES[0], "paceflow", None)?;
        let second = legacy(temp.path(), LEGACY_DATABASES[1], "aieng", None)?;
        for (path, seconds) in [(&first, 1_000_000_000), (&second, 1_000_000_001)] {
            File::options().write(true).open(path)?.set_times(
                std::fs::FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_secs(seconds)),
            )?;
        }
        assert!(inspect(&second)? > inspect(&first)?);
        let conn = Connection::open(&first)?;
        conn.execute("INSERT INTO fact_commit(repo_root, commit_sha, commit_time, subject) VALUES ('repo', 'sha', '2035-01-01', 'subject')", [])?;
        drop(conn);
        prepare(temp.path())?;
        assert_eq!(sessions(temp.path())?, ["paceflow"]);
        Ok(())
    }

    #[test]
    fn simultaneous_startup_serializes_migration() -> Result<()> {
        let temp = tempfile::tempdir()?;
        legacy(temp.path(), LEGACY_DATABASES[0], "session", None)?;
        std::thread::scope(|scope| {
            let first = scope.spawn(|| prepare(temp.path()));
            let second = scope.spawn(|| prepare(temp.path()));
            first.join().unwrap().unwrap();
            second.join().unwrap().unwrap();
        });
        assert_eq!(sessions(temp.path())?, ["session"]);
        Ok(())
    }
}
