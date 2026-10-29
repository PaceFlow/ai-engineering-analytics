//! Setup observations are dated evidence; filesystem scans never backdate installation.
use anyhow::{Result, bail};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

pub const SCHEMA:&str="
CREATE TABLE IF NOT EXISTS setup_observation (
 id TEXT PRIMARY KEY, repo_root TEXT, identity TEXT NOT NULL, kind TEXT NOT NULL,
 fingerprint TEXT, observed_at TEXT NOT NULL, effective_at TEXT,
 previous_observed_at TEXT, source TEXT NOT NULL, state TEXT NOT NULL,
 provider TEXT, session_id TEXT, detail TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_setup_observed ON setup_observation(observed_at);
CREATE TABLE IF NOT EXISTS setup_scan (
 root TEXT PRIMARY KEY, repo_root TEXT, observed_at TEXT NOT NULL,
 complete INTEGER NOT NULL, identities_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS change_milestone (
 id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL, at TEXT NOT NULL,
 repo_root TEXT, kind TEXT NOT NULL, note TEXT NOT NULL DEFAULT '', observation_id TEXT,
 created_at TEXT NOT NULL DEFAULT (datetime('now')));
CREATE TABLE IF NOT EXISTS classifier_auto_project (
 repo_root TEXT PRIMARY KEY, enabled INTEGER NOT NULL, since TEXT NOT NULL,
 monthly_budget REAL NOT NULL, max_run_usd REAL NOT NULL, updated_at TEXT NOT NULL DEFAULT (datetime('now')));
CREATE TABLE IF NOT EXISTS classifier_project_spend (
 request_id INTEGER PRIMARY KEY, repo_root TEXT NOT NULL, at TEXT NOT NULL, usd REAL NOT NULL);
CREATE TABLE IF NOT EXISTS classifier_auto_state (
 provider TEXT NOT NULL,session_id TEXT NOT NULL,fingerprint TEXT NOT NULL,
 question_version TEXT NOT NULL,status TEXT NOT NULL,error TEXT,updated_at TEXT NOT NULL,
 PRIMARY KEY(provider,session_id));
";
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Milestone {
    pub id: i64,
    pub title: String,
    pub at: String,
    pub repo_root: Option<String>,
    pub kind: String,
    pub note: String,
    pub observation_id: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Observation {
    pub id: String,
    pub repo_root: Option<String>,
    pub identity: String,
    pub kind: String,
    pub fingerprint: Option<String>,
    pub observed_at: String,
    pub effective_at: Option<String>,
    pub previous_observed_at: Option<String>,
    pub source: String,
    pub state: String,
    pub provider: Option<String>,
    pub session_id: Option<String>,
    pub detail: String,
}
pub fn milestones(conn: &Connection, repo: Option<&str>) -> Result<Vec<Milestone>> {
    let mut stmt=conn.prepare("SELECT id,title,at,repo_root,kind,note,observation_id FROM change_milestone WHERE ?1 IS NULL OR repo_root IS NULL OR repo_root=?1 ORDER BY at,id")?;
    Ok(stmt
        .query_map([repo], |r| {
            Ok(Milestone {
                id: r.get(0)?,
                title: r.get(1)?,
                at: r.get(2)?,
                repo_root: r.get(3)?,
                kind: r.get(4)?,
                note: r.get(5)?,
                observation_id: r.get(6)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?)
}
pub fn observations(conn: &Connection, repo: Option<&str>) -> Result<Vec<Observation>> {
    let mut stmt=conn.prepare("SELECT id,repo_root,identity,kind,fingerprint,observed_at,effective_at,previous_observed_at,source,state,provider,session_id,detail FROM setup_observation WHERE ?1 IS NULL OR repo_root IS NULL OR repo_root=?1 ORDER BY COALESCE(effective_at,observed_at),id")?;
    Ok(stmt
        .query_map([repo], |r| {
            Ok(Observation {
                id: r.get(0)?,
                repo_root: r.get(1)?,
                identity: r.get(2)?,
                kind: r.get(3)?,
                fingerprint: r.get(4)?,
                observed_at: r.get(5)?,
                effective_at: r.get(6)?,
                previous_observed_at: r.get(7)?,
                source: r.get(8)?,
                state: r.get(9)?,
                provider: r.get(10)?,
                session_id: r.get(11)?,
                detail: r.get(12)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?)
}
#[allow(clippy::too_many_arguments)]
pub fn save_milestone(
    conn: &Connection,
    id: Option<i64>,
    title: &str,
    at: &str,
    repo: Option<&str>,
    kind: &str,
    note: &str,
    observation: Option<&str>,
) -> Result<i64> {
    if title.trim().is_empty() {
        bail!("A milestone needs a title");
    }
    let at = chrono::DateTime::parse_from_rfc3339(at)?
        .with_timezone(&chrono::Utc)
        .to_rfc3339();
    if let Some(observation) = observation {
        let found: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM setup_observation WHERE id=?1)",
            [observation],
            |r| r.get(0),
        )?;
        if !found {
            bail!("Observation not found");
        }
    }
    if let Some(id) = id {
        if conn.execute("UPDATE change_milestone SET title=?2,at=?3,repo_root=?4,kind=?5,note=?6,observation_id=?7 WHERE id=?1",params![id,title,at,repo,kind,note,observation])?==0 {bail!("Milestone not found");}
        Ok(id)
    } else {
        conn.execute("INSERT INTO change_milestone(title,at,repo_root,kind,note,observation_id) VALUES(?1,?2,?3,?4,?5,?6)",params![title,at,repo,kind,note,observation])?;
        Ok(conn.last_insert_rowid())
    }
}
fn store(conn: &Connection, o: &Observation) -> Result<()> {
    conn.execute("INSERT OR IGNORE INTO setup_observation VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",params![o.id,o.repo_root,o.identity,o.kind,o.fingerprint,o.observed_at,o.effective_at,o.previous_observed_at,o.source,o.state,o.provider,o.session_id,o.detail])?;
    Ok(())
}
fn hash(s: &str) -> String {
    format!("{:x}", md5::compute(s))
}

pub fn capture(conn: &Connection, repo: Option<&str>, additional: &[PathBuf]) -> Result<usize> {
    let before: i64 = conn.query_row("SELECT COUNT(*) FROM setup_observation", [], |r| r.get(0))?;
    let now = chrono::Utc::now().to_rfc3339();
    let (tz, _) = super::trends::timezone(conn, None)?;
    conn.execute(
        "INSERT OR IGNORE INTO analytics_setting VALUES('timezone',?1)",
        [tz.name()],
    )?;
    let repos = if let Some(repo) = repo {
        vec![repo.to_string()]
    } else {
        let mut stmt = conn.prepare("SELECT DISTINCT repo_root FROM metadata_repositories")?;
        stmt.query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<Vec<String>>>()?
    };
    let mut roots: Vec<(PathBuf, Option<String>, bool)> = Vec::new();
    for repo in &repos {
        for dir in [
            ".agents/skills",
            ".claude/skills",
            ".codex/skills",
            ".cursor/skills",
            ".opencode/skills",
            "skills",
        ] {
            roots.push((Path::new(repo).join(dir), Some(repo.clone()), false));
        }
        // An instruction inventory is only complete for these allowlisted root files.
        roots.push((PathBuf::from(repo), Some(repo.clone()), true));
    }
    if let Some(home) = crate::path_utils::agent_home() {
        for dir in [
            ".agents/skills",
            ".claude/skills",
            ".cursor/skills",
            ".config/opencode/skills",
        ] {
            roots.push((home.join(dir), None, false));
        }
    }
    if let Ok(home) = crate::path_utils::codex_home() {
        roots.push((home.join("skills"), None, false));
    }
    for path in additional {
        roots.push((path.clone(), repo.map(str::to_owned), false));
    }
    if let Some(json) = conn
        .query_row(
            "SELECT value FROM analytics_setting WHERE key='skill_roots'",
            [],
            |r| r.get::<_, String>(0),
        )
        .optional()?
    {
        for path in serde_json::from_str::<Vec<PathBuf>>(&json)? {
            roots.push((path, None, false));
        }
    }
    roots.sort();
    roots.dedup();
    for (root, scope, instructions) in roots {
        scan(conn, &root, scope.as_deref(), instructions, &now)?;
    }
    capture_sessions(conn, repo, &now)?;
    let after: i64 = conn.query_row("SELECT COUNT(*) FROM setup_observation", [], |r| r.get(0))?;
    Ok((after - before) as usize)
}
fn scan(
    conn: &Connection,
    root: &Path,
    repo: Option<&str>,
    instructions: bool,
    now: &str,
) -> Result<()> {
    let root_id = format!(
        "{}:{}",
        if instructions {
            "instructions"
        } else {
            "skills"
        },
        root.display()
    );
    let previous: Option<(String, bool, String)> = conn
        .query_row(
            "SELECT observed_at,complete,identities_json FROM setup_scan WHERE root=?1",
            [&root_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    let mut files = Vec::new();
    let mut complete = true;
    if instructions {
        for name in ["AGENTS.md", "CLAUDE.md", ".cursorrules"] {
            files.push(root.join(name));
        }
        if !root.is_dir() {
            complete = false;
        }
    } else {
        match std::fs::read_dir(root) {
            Ok(entries) => {
                for entry in entries {
                    match entry {
                        Ok(e) => files.push(e.path().join("SKILL.md")),
                        Err(_) => complete = false,
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => complete = false,
        }
    }
    let mut identities = BTreeSet::new();
    for file in files {
        let identity = file.to_string_lossy().replace('\\', "/");
        match std::fs::metadata(&file) {
            Ok(meta) if meta.is_file() && meta.len() <= 128_000 => {}
            Ok(_) => {
                complete = false;
                continue;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => {
                complete = false;
                continue;
            }
        }
        identities.insert(identity.clone());
        let bytes = match std::fs::read(&file) {
            Ok(v) => v,
            Err(_) => {
                complete = false;
                continue;
            }
        };
        let fingerprint = format!("{:x}", md5::compute(bytes));
        let last:Option<(Option<String>,String)>=conn.query_row("SELECT fingerprint,state FROM setup_observation WHERE identity=?1 AND source='filesystem' ORDER BY observed_at DESC,rowid DESC LIMIT 1",[&identity],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
        if last
            .as_ref()
            .is_some_and(|(fp, state)| fp.as_deref() == Some(&fingerprint) && state != "removed")
        {
            continue;
        }
        store(
            conn,
            &Observation {
                id: hash(&format!("{identity}|{fingerprint}|{now}")),
                repo_root: repo.map(str::to_owned),
                identity: identity.clone(),
                kind: if instructions {
                    "instructions"
                } else {
                    "skill"
                }
                .into(),
                fingerprint: Some(fingerprint),
                observed_at: now.into(),
                effective_at: None,
                previous_observed_at: previous.as_ref().map(|p| p.0.clone()),
                source: "filesystem".into(),
                state: if last.is_some() {
                    "changed"
                } else {
                    "first_observed"
                }
                .into(),
                provider: None,
                session_id: None,
                detail:
                    "Current file fingerprint; historical version and installation date unverified"
                        .into(),
            },
        )?;
    }
    if complete && let Some((at, true, old)) = &previous {
        let old: BTreeSet<String> = serde_json::from_str(old)?;
        for identity in old.difference(&identities) {
            store(
                conn,
                &Observation {
                    id: hash(&format!("{identity}|removed|{now}")),
                    repo_root: repo.map(str::to_owned),
                    identity: identity.clone(),
                    kind: if instructions {
                        "instructions"
                    } else {
                        "skill"
                    }
                    .into(),
                    fingerprint: None,
                    observed_at: now.into(),
                    effective_at: None,
                    previous_observed_at: Some(at.clone()),
                    source: "filesystem".into(),
                    state: "removed".into(),
                    provider: None,
                    session_id: None,
                    detail:
                        "Absent from a complete inventory; removed sometime since previous scan"
                            .into(),
                },
            )?;
        }
    }
    // Preserve the last complete inventory through a failed scan.
    if complete || previous.is_none() {
        conn.execute("INSERT INTO setup_scan VALUES(?1,?2,?3,?4,?5) ON CONFLICT(root) DO UPDATE SET observed_at=excluded.observed_at,complete=excluded.complete,identities_json=excluded.identities_json",params![root_id,repo,now,complete,serde_json::to_string(&identities)?])?;
    }
    Ok(())
}
fn capture_sessions(conn: &Connection, repo: Option<&str>, now: &str) -> Result<()> {
    let mut stmt=conn.prepare("SELECT s.provider,s.session_id,r.repo_root,s.started_at,s.client_version FROM metadata_sessions s LEFT JOIN metadata_repositories r ON r.id=s.repository_id WHERE ?1 IS NULL OR r.repo_root=?1")?;
    let sessions = stmt
        .query_map([repo], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, Option<String>>(4)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (provider, session, repo, started, version) in sessions {
        let mut evidence: Vec<(String, String, Option<String>, String)> = Vec::new();
        if let Some(version) = version {
            evidence.push((
                "agent_version".into(),
                format!("{provider}:{version}"),
                started.clone(),
                "observed".into(),
            ));
        }
        let mut q=conn.prepare("SELECT DISTINCT model_name,usage_ts FROM derived_usage_event WHERE provider=?1 AND session_id=?2 AND model_name IS NOT NULL")?;
        for row in q.query_map(params![provider, session], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
        })? {
            let (model, at) = row?;
            evidence.push(("model".into(), model, at, "used".into()));
        }
        let mut q=conn.prepare("SELECT tool_name,target,ts FROM fact_session_tool_call WHERE provider=?1 AND session_id=?2")?;
        for row in q.query_map(params![provider, session], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, Option<String>>(2)?,
            ))
        })? {
            let (tool, target, at) = row?;
            evidence.push(("tool".into(), tool.clone(), at.clone(), "used".into()));
            let lower = tool.to_lowercase();
            if lower == "skill"
                || lower.ends_with(".skill")
                || lower.ends_with("_skill")
                || lower.ends_with(":skill")
            {
                if let Some(target) = target {
                    evidence.push((
                        "skill".into(),
                        format!("{provider}:{target}"),
                        at,
                        "call".into(),
                    ));
                }
            } else if let Some(target) =
                target.filter(|t| t.ends_with("/SKILL.md") && !t.contains('\n') && !t.contains(' '))
            {
                evidence.push(("skill".into(), target, at, "loaded".into()));
            }
        }
        let mut q=conn.prepare("SELECT kind,source,ts FROM fact_session_context_injection WHERE provider=?1 AND session_id=?2 AND kind IN ('skill_listing','skill_body','skill_injection','instructions','tool_definitions','mcp_instructions')")?;
        for row in q.query_map(params![provider, session], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, Option<String>>(2)?,
            ))
        })? {
            let (kind, source, at) = row?;
            if let Some(source) = source {
                evidence.push((kind, format!("{provider}:{source}"), at, "visible".into()));
            }
        }
        let mut seen = BTreeSet::new();
        for (kind, identity, at, state) in evidence {
            if !seen.insert((kind.clone(), identity.clone(), state.clone())) {
                continue;
            }

            store(conn,&Observation{id:hash(&format!("{provider}|{session}|{kind}|{identity}|{at:?}|{state}")),repo_root:repo.clone(),identity,kind,fingerprint:None,observed_at:now.into(),effective_at:at,previous_observed_at:None,source:"session".into(),state,provider:Some(provider.clone()),session_id:Some(session.clone()),detail:"Recorded session evidence; inventory completeness and historical file version unknown".into()})?;
        }
    }
    Ok(())
}
