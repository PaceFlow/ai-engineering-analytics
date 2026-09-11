//! Read Windows Cursor databases through a checked, local SQLite snapshot.
//! WSL cannot reliably participate in a Windows process's SQLite WAL locks.
use anyhow::{Context, Result, bail, ensure};
use rusqlite::Connection;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

pub(crate) fn snapshot_database(source: &Path, cache_root: &Path) -> Result<PathBuf> {
    let cache = cache_root.join(format!(
        "{:x}",
        md5::compute(source.to_string_lossy().as_bytes())
    ));
    fs::create_dir_all(&cache)?;
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(cache.join("snapshot.lock"))?;
    lock.lock().context("locking Cursor snapshot cache")?;
    let target = cache.join("state.vscdb");
    let stamp = cache.join("source.signature");
    let signature = source_signature(source)?;
    if target.is_file() && fs::read_to_string(&stamp).ok().as_deref() == Some(&signature) {
        return Ok(target);
    }

    for _ in 0..3 {
        let before = source_signature(source)?;
        let staging = tempfile::tempdir_in(&cache)?;
        let snapshot = staging.path().join("state.vscdb");
        fs::copy(source, &snapshot).context("copying Cursor database")?;
        let wal = sidecar(source, "-wal");
        match fs::copy(&wal, sidecar(&snapshot, "-wal")) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("copying Cursor WAL"),
        }
        if before != source_signature(source)? {
            continue;
        }
        // Never copy SHM: SQLite must build its own WAL index on this filesystem.
        let db = Connection::open(&snapshot)?;
        let check: String = db.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
        ensure!(
            check == "ok",
            "Cursor snapshot failed SQLite integrity check: {check}"
        );
        let (busy, _, _): (i64, i64, i64) =
            db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?;
        ensure!(busy == 0, "Cursor snapshot WAL checkpoint was busy");
        // Publish a standalone main file; readers must not reuse WAL/SHM from
        // an older cached snapshot when the main file is atomically replaced.
        let _: String = db.query_row("PRAGMA journal_mode=DELETE", [], |row| row.get(0))?;
        drop(db);
        // Atomic replacement keeps existing readers on their previous snapshot.
        let mut output = tempfile::NamedTempFile::new_in(&cache)?;
        std::io::copy(&mut fs::File::open(&snapshot)?, output.as_file_mut())?;
        output
            .persist(&target)
            .context("publishing Cursor snapshot")?;
        fs::write(&stamp, before)?;
        return Ok(target);
    }
    bail!(
        "Cursor database changed during all snapshot attempts; retry after Cursor finishes writing"
    )
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

fn source_signature(source: &Path) -> Result<String> {
    let mut signature = String::from("snapshot-v2;");
    for path in [source.to_path_buf(), sidecar(source, "-wal")] {
        match fs::metadata(&path) {
            Ok(meta) => signature.push_str(&format!(
                "{}:{};",
                meta.len(),
                meta.modified()?.duration_since(UNIX_EPOCH)?.as_nanos()
            )),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && path != source => {
                signature.push_str("absent;")
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(signature)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_includes_wal_and_refreshes_when_only_wal_changes() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let source = dir.path().join("source.db");
        let db = Connection::open(&source)?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE events(value); INSERT INTO events VALUES (1);")?;
        let cache = dir.path().join("cache");
        let snapshot = snapshot_database(&source, &cache)?;
        let read = || -> Result<i64> {
            Ok(Connection::open(&snapshot)?.query_row(
                "SELECT sum(value) FROM events",
                [],
                |row| row.get(0),
            )?)
        };
        assert_eq!(read()?, 1);
        let unchanged = fs::metadata(&snapshot)?.modified()?;
        snapshot_database(&source, &cache)?;
        assert_eq!(fs::metadata(&snapshot)?.modified()?, unchanged);
        db.execute("INSERT INTO events VALUES (2)", [])?;
        snapshot_database(&source, &cache)?;
        assert_eq!(read()?, 3);
        assert_eq!(
            db.query_row("SELECT count(*) FROM events", [], |row| row
                .get::<_, i64>(0))?,
            2
        );
        Ok(())
    }
}
