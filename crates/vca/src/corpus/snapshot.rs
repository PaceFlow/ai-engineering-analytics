//! Copy one root session's sources into a self-contained snapshot folder.
//!
//! Layout (mirrors where ingest looks, so replay only has to point a home at it):
//! - `home/.claude/projects/<project>/<session>.jsonl` (+ `<session>/subagents/`)
//! - `home/.codex/sessions/.../rollout-*.jsonl`
//! - `home/.local/share/opencode/opencode.db` (only this session's rows)
//!   and `storage/session_diff/<id>.json`
//! - `cursor/state.vscdb` (only rows that mention this composer)
//! - `repo.bundle`, `litellm_prices.json`, `expectations.json`, `manifest.json`

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OpenFlags, params};
use std::path::{Path, PathBuf};
use std::process::Command;

use super::{BUNDLE, EXPECTATIONS, MANIFEST, Manifest, ManifestFile, PRICES};
use crate::explore::review::Expectations;

#[derive(Debug, Clone, serde::Serialize)]
pub struct Saved {
    pub dir: PathBuf,
    pub files: usize,
    pub bytes: u64,
    pub warnings: Vec<String>,
}

struct SessionSource {
    session_id: String,
    source_path: Option<String>,
}

/// Write (or replace) the snapshot for `provider:session_id` under `root`.
pub fn save(
    conn: &Connection,
    root: &Path,
    provider: &str,
    session_id: &str,
    expectations: &Expectations,
) -> Result<Saved> {
    let sessions = tree(conn, provider, session_id)?;
    if sessions.is_empty() {
        bail!("session {provider}:{session_id} not found");
    }
    let dir = root.join(super::snapshot_name(provider, session_id));
    // Write into a sibling and swap, so a failed save keeps the previous snapshot.
    let staging = root.join(format!(
        ".{}.tmp",
        super::snapshot_name(provider, session_id)
    ));
    if staging.exists() {
        std::fs::remove_dir_all(&staging)?;
    }
    std::fs::create_dir_all(&staging)?;

    let mut files: Vec<ManifestFile> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();
    match provider {
        "claude" => copy_claude(&sessions, &staging, &mut files, &mut warnings)?,
        "codex" => copy_codex(&sessions, &staging, &mut files, &mut warnings)?,
        "opencode" => copy_opencode(&sessions, &staging, &mut files, &mut warnings)?,
        "cursor" => copy_cursor(&sessions, &staging, &mut files, &mut warnings)?,
        other => bail!("snapshots are not supported for provider {other}"),
    }
    if files.is_empty() {
        bail!("no source files found for {provider}:{session_id}");
    }

    let (repo_root, title) = conn
        .query_row(
            "SELECT r.repo_root, s.title FROM metadata_sessions s
             LEFT JOIN metadata_repositories r ON r.id = s.repository_id
             WHERE s.provider = ?1 AND s.session_id = ?2",
            params![provider, session_id],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                ))
            },
        )
        .unwrap_or((None, None));
    let repo = match repo_root.as_deref() {
        Some(repo) => bundle_repo(Path::new(repo), &staging, &mut warnings),
        None => {
            warnings.push("no git repository: delivery numbers cannot be replayed".into());
            None
        }
    };
    if repo.is_some() {
        record(&staging, BUNDLE, "git bundle", &mut files)?;
    }
    let repo = repo.unwrap_or_default();

    match crate::cost::litellm::cache_path() {
        Ok(prices) if prices.exists() => {
            std::fs::copy(&prices, staging.join(PRICES))?;
            record(&staging, PRICES, &prices.to_string_lossy(), &mut files)?;
        }
        _ => warnings.push("no cached price map: replay uses the bundled one".into()),
    }

    std::fs::write(
        staging.join(EXPECTATIONS),
        serde_json::to_string_pretty(expectations)? + "\n",
    )?;
    super::observations::save(
        conn,
        &staging.join(super::observations::FILE),
        provider,
        session_id,
        repo_root.as_deref(),
    )?;
    record(
        &staging,
        super::observations::FILE,
        "persistent analytics inputs",
        &mut files,
    )?;
    let manifest = Manifest {
        format: 1,
        key: format!("{provider}:{session_id}"),
        provider: provider.to_string(),
        session_id: session_id.to_string(),
        sessions: sessions.iter().map(|s| s.session_id.clone()).collect(),
        title,
        created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        vca_version: crate::cli::VERSION.to_string(),
        pricing: crate::cost::pricing::pricing_version(),
        repo_root,
        repo_head: repo.head,
        repo_head_ref: repo.head_ref,
        repo_remote: repo.remote,
        repo_origin_head: repo.origin_head,
        files: files.clone(),
        warnings: warnings.clone(),
    };
    std::fs::write(
        staging.join(MANIFEST),
        serde_json::to_string_pretty(&manifest)? + "\n",
    )?;

    if dir.exists() {
        std::fs::remove_dir_all(&dir)
            .with_context(|| format!("cannot replace {}", dir.display()))?;
    }
    std::fs::rename(&staging, &dir)?;
    Ok(Saved {
        dir,
        files: files.len(),
        bytes: files.iter().map(|f| f.bytes).sum(),
        warnings,
    })
}

/// The root session and everything that rolls up into it.
fn tree(conn: &Connection, provider: &str, session_id: &str) -> Result<Vec<SessionSource>> {
    let mut stmt = conn.prepare(
        "SELECT session_id, source_path FROM metadata_sessions
         WHERE provider = ?1 AND COALESCE(NULLIF(root_session_id, ''), session_id) = ?2
         ORDER BY CASE WHEN session_id = ?2 THEN 0 ELSE 1 END, started_at",
    )?;
    Ok(stmt
        .query_map(params![provider, session_id], |row| {
            Ok(SessionSource {
                session_id: row.get(0)?,
                source_path: row.get(1)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?)
}

fn record(staging: &Path, rel: &str, source: &str, files: &mut Vec<ManifestFile>) -> Result<()> {
    let path = staging.join(rel);
    let bytes = std::fs::read(&path)?;
    files.push(ManifestFile {
        path: rel.replace('\\', "/"),
        source: source.to_string(),
        bytes: bytes.len() as u64,
        md5: format!("{:x}", md5::compute(&bytes)),
    });
    Ok(())
}

fn copy_file(
    source: &Path,
    staging: &Path,
    rel: &Path,
    files: &mut Vec<ManifestFile>,
) -> Result<()> {
    let dest = staging.join(rel);
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Agents keep live sessions open; read through a shared handle.
    let bytes =
        std::fs::read(source).with_context(|| format!("cannot read {}", source.display()))?;
    std::fs::write(&dest, &bytes)?;
    record(
        staging,
        &rel.to_string_lossy(),
        &source.to_string_lossy(),
        files,
    )
}

fn copy_tree(
    source: &Path,
    staging: &Path,
    rel: &Path,
    files: &mut Vec<ManifestFile>,
) -> Result<()> {
    for entry in std::fs::read_dir(source)?.flatten() {
        let path = entry.path();
        let child = rel.join(entry.file_name());
        if path.is_dir() {
            copy_tree(&path, staging, &child, files)?;
        } else {
            copy_file(&path, staging, &child, files)?;
        }
    }
    Ok(())
}

/// Path of `path` below `base`, or `fallback/<file name>` when it is elsewhere.
fn below(path: &Path, base: &Path, fallback: &str) -> PathBuf {
    match path.strip_prefix(base) {
        Ok(rel) => rel.to_path_buf(),
        Err(_) => Path::new(fallback).join(path.file_name().unwrap_or_default()),
    }
}

fn copy_claude(
    sessions: &[SessionSource],
    staging: &Path,
    files: &mut Vec<ManifestFile>,
    warnings: &mut Vec<String>,
) -> Result<()> {
    let projects = crate::path_utils::require_agent_home()?
        .join(".claude")
        .join("projects");
    let dest_root = Path::new("home").join(".claude").join("projects");
    for session in sessions {
        let Some(source) = session.source_path.as_deref().map(PathBuf::from) else {
            continue;
        };
        // Subagent transcripts are copied with their main file's folder.
        if source
            .parent()
            .and_then(Path::file_name)
            .and_then(|n| n.to_str())
            == Some("subagents")
        {
            continue;
        }
        if !source.exists() {
            warnings.push(format!("missing source {}", source.display()));
            continue;
        }
        let rel = dest_root.join(below(&source, &projects, "_external"));
        copy_file(&source, staging, &rel, files)?;
        let companion = source.with_extension("");
        if companion.is_dir() {
            copy_tree(&companion, staging, &rel.with_extension(""), files)?;
        }
    }
    Ok(())
}

fn copy_codex(
    sessions: &[SessionSource],
    staging: &Path,
    files: &mut Vec<ManifestFile>,
    warnings: &mut Vec<String>,
) -> Result<()> {
    let codex = crate::path_utils::codex_home()?;
    let dest_root = Path::new("home").join(".codex");
    for session in sessions {
        let Some(source) = session.source_path.as_deref().map(PathBuf::from) else {
            continue;
        };
        if !source.exists() {
            warnings.push(format!("missing source {}", source.display()));
            continue;
        }
        let rel = dest_root.join(below(&source, &codex, "sessions/_external"));
        copy_file(&source, staging, &rel, files)?;
    }
    Ok(())
}

fn open_source(path: &Path) -> Result<Connection> {
    Ok(Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?)
}

/// New database with the same schema as `tables` in the source.
fn create_like(source: &Connection, dest: &Path, tables: &[&str]) -> Result<Connection> {
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let out = Connection::open(dest)?;
    // Rows are copied table by table; the source schema's foreign keys
    // (OpenCode: session -> project, part -> message) would reject that order.
    out.execute_batch("PRAGMA foreign_keys = OFF;")?;
    for table in tables {
        let sql: Option<String> = source
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
                [table],
                |row| row.get(0),
            )
            .ok();
        if let Some(sql) = sql {
            out.execute_batch(&sql)?;
        }
    }
    Ok(out)
}

fn columns(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    Ok(stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<_>>()?)
}

/// Copy rows of `table` matching `filter` (SQL with `?1` bound to each id).
fn copy_rows(
    source: &Connection,
    dest: &Connection,
    table: &str,
    filter: &str,
    ids: &[String],
) -> Result<usize> {
    let cols = columns(source, table)?;
    if cols.is_empty() {
        return Ok(0);
    }
    let list = cols.join(", ");
    let marks = (1..=cols.len())
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut read = source.prepare(&format!("SELECT {list} FROM {table} WHERE {filter}"))?;
    let mut write = dest.prepare(&format!(
        "INSERT OR IGNORE INTO {table} ({list}) VALUES ({marks})"
    ))?;
    let mut copied = 0;
    for id in ids {
        let mut rows = read.query([id])?;
        while let Some(row) = rows.next()? {
            let values: Vec<rusqlite::types::Value> = (0..cols.len())
                .map(|i| row.get(i))
                .collect::<rusqlite::Result<_>>()?;
            copied += write.execute(rusqlite::params_from_iter(values))?;
        }
    }
    Ok(copied)
}

fn copy_opencode(
    sessions: &[SessionSource],
    staging: &Path,
    files: &mut Vec<ManifestFile>,
    warnings: &mut Vec<String>,
) -> Result<()> {
    let db_path = crate::providers::opencode::opencode_db_path()?;
    copy_opencode_from(&db_path, sessions, staging, files, warnings)
}

fn copy_opencode_from(
    db_path: &Path,
    sessions: &[SessionSource],
    staging: &Path,
    files: &mut Vec<ManifestFile>,
    warnings: &mut Vec<String>,
) -> Result<()> {
    let source = open_source(db_path)?;
    let rel = Path::new("home/.local/share/opencode/opencode.db");
    let tables = [
        "project",
        "session",
        "message",
        "part",
        "session_v2",
        "session_message",
    ];
    let dest = create_like(&source, &staging.join(rel), &tables)?;
    let ids: Vec<String> = sessions.iter().map(|s| s.session_id.clone()).collect();
    let copied = copy_rows(&source, &dest, "session", "id = ?1", &ids)?
        + copy_rows(&source, &dest, "session_v2", "id = ?1", &ids)?;
    if copied == 0 {
        bail!("session rows not found in {}", db_path.display());
    }
    copy_rows(&source, &dest, "message", "session_id = ?1", &ids)?;
    copy_rows(&source, &dest, "part", "session_id = ?1", &ids)?;
    copy_rows(&source, &dest, "session_message", "session_id = ?1", &ids)?;
    let mut projects = Vec::new();
    for table in ["session", "session_v2"] {
        if columns(&dest, table)?.is_empty() {
            continue;
        }
        let mut stmt = dest.prepare(&format!("SELECT DISTINCT project_id FROM {table}"))?;
        projects.extend(
            stmt.query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?,
        );
    }
    copy_rows(&source, &dest, "project", "id = ?1", &projects)?;
    drop(dest);
    record(
        staging,
        &rel.to_string_lossy(),
        &db_path.to_string_lossy(),
        files,
    )?;

    let diffs = db_path
        .parent()
        .map(|dir| dir.join("storage").join("session_diff"));
    for id in &ids {
        let Some(diff) = diffs.as_ref().map(|dir| dir.join(format!("{id}.json"))) else {
            continue;
        };
        if diff.exists() {
            let rel = Path::new("home/.local/share/opencode/storage/session_diff")
                .join(format!("{id}.json"));
            copy_file(&diff, staging, &rel, files)?;
        }
    }
    if sessions.len() > 1 {
        warnings.push(format!(
            "{} OpenCode sessions (root + subagents)",
            sessions.len()
        ));
    }
    Ok(())
}

fn copy_cursor(
    sessions: &[SessionSource],
    staging: &Path,
    files: &mut Vec<ManifestFile>,
    warnings: &mut Vec<String>,
) -> Result<()> {
    let Some(state) = crate::cursor_paths::cursor_state_path()? else {
        bail!("Cursor state.vscdb not found");
    };
    let source = open_source(&state)?;
    let rel = Path::new("cursor/state.vscdb");
    let dest = create_like(&source, &staging.join(rel), &["ItemTable", "cursorDiskKV"])?;
    let ids: Vec<String> = sessions.iter().map(|s| s.session_id.clone()).collect();
    // Composer data, bubbles, diffs and fates carry the id in their key; inline
    // diff rows point back to it through composerMetadata in their value.
    let copied = copy_rows(
        &source,
        &dest,
        "cursorDiskKV",
        "instr(key, ?1) > 0 OR instr(CAST(value AS TEXT), ?1) > 0",
        &ids,
    )?;
    drop(dest);
    if copied == 0 {
        bail!("no rows for this composer in {}", state.display());
    }
    record(
        staging,
        &rel.to_string_lossy(),
        &state.to_string_lossy(),
        files,
    )?;
    warnings.push(
        "Cursor History (local file versions) is not copied; numbers that need it may differ"
            .into(),
    );
    Ok(())
}

#[derive(Debug, Default)]
struct RepoInfo {
    head: Option<String>,
    head_ref: Option<String>,
    remote: Option<String>,
    origin_head: Option<String>,
}

/// `git bundle` of the whole repository, plus what the replay needs to put
/// HEAD and origin back as they were.
fn bundle_repo(repo: &Path, staging: &Path, warnings: &mut Vec<String>) -> Option<RepoInfo> {
    if !repo.join(".git").exists() {
        warnings.push(format!(
            "{} is not a git checkout; no bundle",
            repo.display()
        ));
        return None;
    }
    let read = |args: &[&str]| {
        Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .filter(|s| !s.is_empty())
    };
    let info = RepoInfo {
        head: read(&["rev-parse", "HEAD"]),
        head_ref: read(&["symbolic-ref", "--quiet", "HEAD"]),
        remote: read(&["remote", "get-url", "origin"]),
        origin_head: read(&["symbolic-ref", "--quiet", "refs/remotes/origin/HEAD"]),
    };
    info.head.as_ref()?;
    let bundle = staging.join(BUNDLE);
    match Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["bundle", "create"])
        .arg(&bundle)
        .args(["--all"])
        .output()
    {
        Ok(out) if out.status.success() => Some(info),
        Ok(out) => {
            warnings.push(format!(
                "git bundle failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            ));
            None
        }
        Err(error) => {
            warnings.push(format!("git not runnable: {error}"));
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v2_only_opencode_snapshot_replays_selected_sessions_without_private_tables() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let src_path = dir.path().join("opencode.db");
        let src = Connection::open(&src_path)?;
        src.execute_batch("CREATE TABLE project(id TEXT PRIMARY KEY,worktree TEXT);
            CREATE TABLE session_v2(id TEXT PRIMARY KEY,project_id TEXT,directory TEXT,time_created INTEGER,time_updated INTEGER);
            CREATE TABLE session_message(id TEXT PRIMARY KEY,session_id TEXT,type TEXT,seq INTEGER,time_created INTEGER,time_updated INTEGER,data TEXT);
            CREATE TABLE credential(secret TEXT);
            INSERT INTO credential VALUES('must never be copied');
            INSERT INTO project VALUES('p','/recorded/repository');
            INSERT INTO session_v2 VALUES('s','p','/removed/worktree',1790848800000,1790848860000);
            INSERT INTO session_v2 VALUES('other','p','/removed/other',1790848800000,1790848860000);")?;
        src.execute(
            "INSERT INTO session_message VALUES('u','s','user',0,1790848800000,1790848800000,?1)",
            [serde_json::json!({"text":"Explain this behavior"}).to_string()],
        )?;
        src.execute("INSERT INTO session_message VALUES('a','s','assistant',1,1790848860000,1790848860000,?1)",[serde_json::json!({"model":{"id":"model"},"cost":0.01,"tokens":{"input":5,"output":2,"reasoning":1,"cache":{"read":3,"write":0}},"content":[{"type":"text","text":"Explanation"}]}).to_string()])?;
        src.execute("INSERT INTO session_message VALUES('private','other','user',0,1790848800000,1790848800000,?1)",[serde_json::json!({"text":"unselected conversation"}).to_string()])?;
        let staging = dir.path().join("snapshot");
        copy_opencode_from(
            &src_path,
            &[SessionSource {
                session_id: "s".into(),
                source_path: Some(src_path.to_string_lossy().into_owned()),
            }],
            &staging,
            &mut vec![],
            &mut vec![],
        )?;
        let copied = staging.join("home/.local/share/opencode/opencode.db");
        let snapshot = Connection::open(&copied)?;
        assert_eq!(
            snapshot.query_row("SELECT COUNT(*) FROM session_v2", [], |r| r
                .get::<_, i64>(0))?,
            1
        );
        assert_eq!(
            snapshot.query_row("SELECT COUNT(*) FROM session_message", [], |r| r
                .get::<_, i64>(0))?,
            2
        );
        assert_eq!(
            snapshot.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name='credential'",
                [],
                |r| r.get::<_, i64>(0)
            )?,
            0
        );
        let analytics = Connection::open_in_memory()?;
        crate::db::init_app_schema(&analytics)?;
        crate::providers::opencode::ingest_planned_sessions(&analytics, &[copied], false, None)?;
        assert_eq!(
            analytics.query_row(
                "SELECT SUM(total_tokens) FROM fact_session_usage",
                [],
                |r| r.get::<_, i64>(0)
            )?,
            11
        );
        Ok(())
    }

    #[test]
    fn below_keeps_relative_layout_or_falls_back() {
        let base = Path::new("/home/me/.claude/projects");
        assert_eq!(
            below(
                Path::new("/home/me/.claude/projects/p/s.jsonl"),
                base,
                "_external"
            ),
            PathBuf::from("p/s.jsonl")
        );
        assert_eq!(
            below(Path::new("/elsewhere/s.jsonl"), base, "_external"),
            PathBuf::from("_external/s.jsonl")
        );
    }

    #[test]
    fn copies_only_one_composers_cursor_rows() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let source_path = dir.path().join("state.vscdb");
        let source = Connection::open(&source_path)?;
        source.execute_batch(
            "CREATE TABLE ItemTable (key TEXT UNIQUE ON CONFLICT REPLACE, value BLOB);
             CREATE TABLE cursorDiskKV (key TEXT UNIQUE ON CONFLICT REPLACE, value BLOB);
             INSERT INTO cursorDiskKV VALUES ('composerData:c1', '{\"composerId\":\"c1\"}');
             INSERT INTO cursorDiskKV VALUES ('bubbleId:c1:b1', '{\"type\":1}');
             INSERT INTO cursorDiskKV VALUES ('inlineDiff:x', '{\"composerMetadata\":{\"composerId\":\"c1\"}}');
             INSERT INTO cursorDiskKV VALUES ('composerData:other', '{\"composerId\":\"other\"}');
             INSERT INTO cursorDiskKV VALUES ('bubbleId:other:b9', '{\"type\":1}');",
        )?;
        let dest_path = dir.path().join("out.vscdb");
        let dest = create_like(&source, &dest_path, &["ItemTable", "cursorDiskKV"])?;
        let copied = copy_rows(
            &source,
            &dest,
            "cursorDiskKV",
            "instr(key, ?1) > 0 OR instr(CAST(value AS TEXT), ?1) > 0",
            &["c1".to_string()],
        )?;
        assert_eq!(copied, 3);
        let keys: Vec<String> = dest
            .prepare("SELECT key FROM cursorDiskKV ORDER BY key")?
            .query_map([], |row| row.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        assert_eq!(keys, ["bubbleId:c1:b1", "composerData:c1", "inlineDiff:x"]);
        Ok(())
    }
}
