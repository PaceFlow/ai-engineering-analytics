//! Persistent source coverage and a compact, atomically published work index.
use anyhow::{Result, bail};
use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

pub const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS source_coverage (
 source_id TEXT PRIMARY KEY, provider TEXT NOT NULL, repo_root TEXT,
 observed_from TEXT, observed_through TEXT, ingested_at TEXT NOT NULL,
 complete INTEGER NOT NULL, kind TEXT NOT NULL, error TEXT);
CREATE TABLE IF NOT EXISTS source_coverage_history (source_id TEXT NOT NULL,ingested_at TEXT NOT NULL,record_json TEXT NOT NULL,PRIMARY KEY(source_id,ingested_at));
CREATE TABLE IF NOT EXISTS repository_alias (
 alias TEXT PRIMARY KEY, canonical TEXT NOT NULL, source TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS usage_measurement (
 provider TEXT NOT NULL, session_id TEXT NOT NULL, usage_key TEXT NOT NULL,
 availability TEXT NOT NULL, source TEXT NOT NULL,
 PRIMARY KEY(provider,session_id,usage_key));
CREATE TABLE IF NOT EXISTS derived_work_index (
 work_id TEXT PRIMARY KEY, intent TEXT NOT NULL, intent_source TEXT NOT NULL,
 started_ms INTEGER, completed_ms INTEGER, last_ms INTEGER,
 state TEXT NOT NULL, verified INTEGER NOT NULL, conversations INTEGER NOT NULL,
 attempts INTEGER NOT NULL, corrections INTEGER NOT NULL, validation_failures INTEGER NOT NULL,
 observed INTEGER NOT NULL, missing_usage INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS derived_work_index_member (
 identity_id TEXT PRIMARY KEY, work_id TEXT NOT NULL, provider TEXT NOT NULL,
 session_id TEXT NOT NULL, task_index INTEGER NOT NULL, repo_root TEXT,
 UNIQUE(provider,session_id,task_index));
CREATE INDEX IF NOT EXISTS idx_work_index_member_work ON derived_work_index_member(work_id);
CREATE INDEX IF NOT EXISTS idx_work_index_member_repo ON derived_work_index_member(repo_root);
CREATE TABLE IF NOT EXISTS published_work_lifecycle (work_id TEXT,at_ms INTEGER,state TEXT,verified INTEGER);
CREATE INDEX IF NOT EXISTS idx_published_lifecycle ON published_work_lifecycle(work_id,at_ms);
CREATE TABLE IF NOT EXISTS published_work_conversation (work_id TEXT,provider TEXT,session_id TEXT,started_ms INTEGER);
CREATE TABLE IF NOT EXISTS published_work_usage (usage_id INTEGER PRIMARY KEY,work_id TEXT,usage_ts TEXT,provider TEXT,model_name TEXT,token_source TEXT,availability TEXT,input_tokens INTEGER,cache_write INTEGER,cache_read INTEGER,output_tokens INTEGER,reasoning_extra INTEGER,total_tokens INTEGER,reported_cost_usd REAL,api_equiv_usd REAL,stage TEXT,context_tokens INTEGER);
CREATE TABLE IF NOT EXISTS published_work_activity (work_id TEXT,start_ms INTEGER,end_ms INTEGER,stage TEXT);
CREATE TABLE IF NOT EXISTS published_work_models (provider TEXT,session_id TEXT,task_index INTEGER,models TEXT,PRIMARY KEY(provider,session_id,task_index));
CREATE TABLE IF NOT EXISTS published_work_event (work_id TEXT,ts TEXT,kind TEXT);
CREATE TABLE IF NOT EXISTS classification_job (
 id INTEGER PRIMARY KEY, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 state TEXT NOT NULL, passes TEXT NOT NULL, level TEXT NOT NULL, policy TEXT NOT NULL,
 max_usd REAL NOT NULL, spent_usd REAL NOT NULL DEFAULT 0, error TEXT);
CREATE TABLE IF NOT EXISTS classification_job_unit (
 job_id INTEGER NOT NULL, provider TEXT NOT NULL, session_id TEXT NOT NULL,
 repo_root TEXT, fingerprint TEXT NOT NULL, state TEXT NOT NULL, pass TEXT NOT NULL,
 error TEXT, PRIMARY KEY(job_id,provider,session_id,pass));
";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceCoverage {
    pub source_id: String,
    pub provider: String,
    pub repo_root: Option<String>,
    pub observed_from: Option<String>,
    pub observed_through: Option<String>,
    pub ingested_at: String,
    pub complete: bool,
    pub kind: String,
    pub error: Option<String>,
}

pub fn record(conn: &Connection, c: &SourceCoverage) -> Result<()> {
    if let Some(at) = &c.observed_from {
        chrono::DateTime::parse_from_rfc3339(at)?;
    }
    if let Some(at) = &c.observed_through {
        chrono::DateTime::parse_from_rfc3339(at)?;
    }
    if let (Some(from), Some(through)) = (&c.observed_from, &c.observed_through) {
        anyhow::ensure!(
            chrono::DateTime::parse_from_rfc3339(from)?
                <= chrono::DateTime::parse_from_rfc3339(through)?,
            "Source interval ends before it starts"
        );
    }
    conn.execute(
        "INSERT OR REPLACE INTO source_coverage_history VALUES(?1,?2,?3)",
        params![c.source_id, c.ingested_at, serde_json::to_string(c)?],
    )?;
    conn.execute("INSERT INTO source_coverage VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)
      ON CONFLICT(source_id) DO UPDATE SET provider=excluded.provider,repo_root=excluded.repo_root,
      observed_from=excluded.observed_from,observed_through=excluded.observed_through,
      ingested_at=excluded.ingested_at,complete=excluded.complete,kind=excluded.kind,error=excluded.error",
      params![c.source_id,c.provider,c.repo_root,c.observed_from,c.observed_through,c.ingested_at,c.complete,c.kind,c.error])?;
    Ok(())
}

pub fn coverage(conn: &Connection) -> Result<Vec<SourceCoverage>> {
    let mut q=conn.prepare("SELECT source_id,provider,repo_root,observed_from,observed_through,ingested_at,complete,kind,error FROM source_coverage ORDER BY source_id")?;
    Ok(q.query_map([], |r| {
        Ok(SourceCoverage {
            source_id: r.get(0)?,
            provider: r.get(1)?,
            repo_root: r.get(2)?,
            observed_from: r.get(3)?,
            observed_through: r.get(4)?,
            ingested_at: r.get(5)?,
            complete: r.get(6)?,
            kind: r.get(7)?,
            error: r.get(8)?,
        })
    })?
    .collect::<rusqlite::Result<_>>()?)
}

pub fn canonical_repo(conn: &Connection, path: &str) -> Result<String> {
    let mut current = path.to_string();
    let mut seen = std::collections::BTreeSet::new();
    loop {
        if !seen.insert(current.clone()) {
            bail!("Repository alias cycle at {current}");
        }
        let next = conn
            .query_row(
                "SELECT canonical FROM repository_alias WHERE alias=?1",
                [&current],
                |r| r.get::<_, String>(0),
            )
            .optional()?;
        match next {
            Some(next) => current = next,
            None => return Ok(current),
        }
    }
}

pub fn set_alias(conn: &Connection, alias: &str, canonical: &str, source: &str) -> Result<()> {
    let target = canonical_repo(conn, canonical)?;
    if alias == target {
        bail!("Repository alias would create a cycle");
    }
    conn.execute("INSERT INTO repository_alias VALUES(?1,?2,?3) ON CONFLICT(alias) DO UPDATE SET canonical=excluded.canonical,source=excluded.source WHERE excluded.source='manual' OR (repository_alias.source!='manual' AND (CASE excluded.source WHEN 'source' THEN 3 WHEN 'git' THEN 2 ELSE 1 END)>=(CASE repository_alias.source WHEN 'source' THEN 3 WHEN 'git' THEN 2 ELSE 1 END))",params![alias,target,source])?;
    Ok(())
}

/// Called inside the insight refresh savepoint. Readers see one complete generation.
pub fn publish(conn: &Connection) -> Result<()> {
    let roots = conn
        .prepare("SELECT repo_root FROM metadata_repositories")?
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for root in roots {
        if let Some(key) = crate::event_identity::repo_key_for_repo_root(Some(&root))
            && root != key
        {
            set_alias(conn, &root, &key, "git")?;
        }
    }
    let recorded=conn.prepare("SELECT DISTINCT r.repo_root,p.project_root FROM fact_session_project p JOIN metadata_sessions s USING(provider,session_id) JOIN metadata_repositories r ON r.id=s.repository_id WHERE p.project_root!='/' AND p.project_root!=''")?.query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
    for (alias, root) in recorded {
        let target = canonical_repo(conn, &root)?;
        if alias != target {
            set_alias(conn, &alias, &target, "provider")?;
        }
    }
    conn.execute("INSERT OR IGNORE INTO usage_measurement SELECT provider,session_id,usage_key,CASE WHEN total_tokens>0 THEN CASE WHEN token_source='reported' THEN 'native' ELSE 'estimated' END ELSE 'suspect_zero' END,'retained_fact' FROM fact_session_usage WHERE usage_key IS NOT NULL",[])?;
    let items = super::work::items(conn)?;
    conn.execute("DELETE FROM derived_work_index_member", [])?;
    conn.execute("DELETE FROM derived_work_index", [])?;
    conn.execute_batch(
        "DELETE FROM published_work_lifecycle; DELETE FROM published_work_conversation;",
    )?;
    let mut index = conn.prepare_cached(
        "INSERT INTO derived_work_index VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
    )?;
    let mut member =
        conn.prepare_cached("INSERT INTO derived_work_index_member VALUES(?1,?2,?3,?4,?5,?6)")?;
    for w in items {
        for m in &w.members {
            if let Some(at) = m
                .task
                .started_at
                .as_deref()
                .and_then(super::structure::parse_ts)
            {
                conn.execute(
                    "INSERT INTO published_work_lifecycle VALUES(?1,?2,'open',0)",
                    params![w.id, at],
                )?;
                conn.execute(
                    "INSERT INTO published_work_conversation VALUES(?1,?2,?3,?4)",
                    params![w.id, m.task.provider, m.task.session_id, at],
                )?;
            }
        }
        for c in &w.completions {
            let current = w
                .members
                .iter()
                .filter(|m| {
                    m.task
                        .started_at
                        .as_deref()
                        .and_then(super::structure::parse_ts)
                        .is_some_and(|s| s <= c.at)
                })
                .max_by(|a, b| a.task.started_at.cmp(&b.task.started_at));
            if current.is_some_and(|m| m.id == c.member_id) {
                conn.execute(
                    "INSERT INTO published_work_lifecycle VALUES(?1,?2,'completed',?3)",
                    params![w.id, c.at, c.verified],
                )?;
            }
        }
        for at in &w.reopens {
            conn.execute(
                "INSERT INTO published_work_lifecycle VALUES(?1,?2,'reopened',0)",
                params![w.id, at],
            )?;
        }
        index.execute(params![
            w.id,
            w.intent,
            w.intent_source,
            w.started_at,
            w.completed_at,
            w.last_activity_at,
            w.state,
            w.verified,
            w.conversations as i64,
            w.attempts,
            w.corrections,
            w.validation_failures,
            w.observed_activity,
            w.members_missing_usage as i64,
            {let first=&w.members[0];let (purpose,subtype,source)=super::investigation::purpose(conn,&first.task.provider,&first.task.session_id,first.task.task_index)?;let history:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM audit_work_event a JOIN derived_work_member m ON m.identity_id=a.subject_key WHERE a.subject_kind='task' AND a.source IN ('manual','import') AND m.identity_id IN (SELECT value FROM json_each(?1)))",[serde_json::to_string(&w.members.iter().map(|m|&m.id).collect::<Vec<_>>())?],|r|r.get(0))?;serde_json::json!({"purpose":purpose,"subtype":subtype,"source":source,"history":history}).to_string()}
        ])?;
        for m in w.members {
            let repo = m
                .task
                .repo_root
                .as_deref()
                .map(|r| canonical_repo(conn, r))
                .transpose()?;
            member.execute(params![
                m.id,
                w.id,
                m.task.provider,
                m.task.session_id,
                m.task.task_index,
                repo
            ])?;
        }
    }
    conn.execute_batch("DELETE FROM published_work_usage; DELETE FROM published_work_activity; DELETE FROM published_work_models; DELETE FROM published_work_event;
      INSERT INTO published_work_models SELECT m.provider,m.session_id,m.task_index,t.models FROM derived_work_index_member m JOIN derived_task t USING(provider,session_id,task_index);
      INSERT INTO published_work_usage SELECT d.usage_id,m.work_id,d.usage_ts,d.provider,COALESCE(d.model_name,'unknown'),d.token_source,COALESCE(a.availability,CASE WHEN d.total_tokens>0 THEN CASE WHEN d.token_source='reported' THEN 'native' ELSE 'estimated' END ELSE 'suspect_zero' END),u.input_tokens,u.cache_creation_tokens,u.cached_input_tokens,u.output_tokens,CASE WHEN COALESCE(u.reasoning_in_output,0)=0 THEN u.reasoning_tokens ELSE 0 END,d.total_tokens,d.reported_cost_usd,d.api_equiv_usd,COALESCE(s.stage,'unclassified'),d.context_tokens,COALESCE(p.label,'unknown') FROM derived_usage_event d JOIN fact_session_usage u ON u.id=d.usage_id JOIN derived_work_index_member m ON m.provider=d.provider AND m.session_id=d.root_session_id AND m.task_index=d.task_index LEFT JOIN usage_measurement a ON a.provider=u.provider AND a.session_id=u.session_id AND a.usage_key=u.usage_key LEFT JOIN derived_turn_stage s ON s.provider=d.provider AND s.session_id=d.root_session_id AND s.turn_index=d.root_turn_index LEFT JOIN view_label_effective p ON p.unit_kind='turn' AND p.question_id='activity_purpose' AND p.unit_key=d.provider || '|' || d.root_session_id || '|' || d.root_turn_index;
      INSERT INTO published_work_activity SELECT m.work_id,a.start_ms,a.end_ms,COALESCE(s.stage,'unclassified'),COALESCE(p.label,'unknown') FROM derived_work_activity a JOIN derived_work_index_member m USING(provider,session_id,task_index) LEFT JOIN derived_turn_stage s ON s.provider=a.provider AND s.session_id=a.session_id AND s.turn_index=a.turn_index LEFT JOIN view_label_effective p ON p.unit_kind='turn' AND p.question_id='activity_purpose' AND p.unit_key=a.provider || '|' || a.session_id || '|' || a.turn_index;
      INSERT INTO published_work_event SELECT m.work_id,a.started_at,a.validation_result FROM derived_attempt a JOIN derived_work_index_member m USING(provider,session_id,task_index);
      INSERT INTO published_work_event SELECT m.work_id,t.started_at,'correction' FROM view_label_effective l JOIN derived_turn t ON l.unit_key=t.provider||'|'||t.session_id||'|'||t.turn_index JOIN derived_task k ON k.provider=t.provider AND k.session_id=t.session_id AND (','||k.turn_list||',') LIKE '%,'||t.turn_index||',%' JOIN derived_work_index_member m ON m.provider=k.provider AND m.session_id=k.session_id AND m.task_index=k.task_index WHERE l.unit_kind='exchange' AND l.question_id='feedback' AND l.label='correct_defect';")?;
    conn.execute(
        "INSERT OR REPLACE INTO analytics_setting VALUES('published_revision',?1)",
        [Utc::now().to_rfc3339()],
    )?;
    Ok(())
}

/// An explicit source override is conservatively treated as a snapshot. A small
/// adjacent .coverage.json manifest may establish its actual capture interval.
pub fn scanned(conn: &Connection, provider: &str, snapshot: bool) -> Result<()> {
    let paths=conn.prepare("SELECT DISTINCT source_path FROM metadata_sessions WHERE provider=?1 AND source_path IS NOT NULL")?.query_map([provider],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let (first,last):(Option<String>,Option<String>)=conn.query_row("SELECT MIN(started_at),MAX(COALESCE(ended_at,started_at)) FROM metadata_sessions WHERE provider=?1",[provider],|r|Ok((r.get(0)?,r.get(1)?)))?;
    let live_complete =
        !snapshot && !paths.is_empty() && paths.iter().all(|p| std::path::Path::new(p).exists());
    let mut c = SourceCoverage {
        source_id: format!("sessions:{provider}"),
        provider: provider.into(),
        repo_root: None,
        observed_from: first,
        observed_through: if live_complete {
            Some(Utc::now().to_rfc3339())
        } else {
            last
        },
        ingested_at: Utc::now().to_rfc3339(),
        complete: live_complete,
        kind: if snapshot { "snapshot" } else { "session_scan" }.into(),
        error: None,
    };
    if snapshot && paths.len() == 1 {
        let manifest = std::path::PathBuf::from(format!("{}.coverage.json", paths[0]));
        if manifest.is_file() && manifest.metadata()?.len() <= 64 * 1024 {
            let recorded: SourceCoverage = serde_json::from_slice(&std::fs::read(manifest)?)?;
            c.observed_from = recorded.observed_from;
            c.observed_through = recorded.observed_through;
            c.complete = recorded.complete;
        }
    }
    record(conn, &c)?;
    if let Some(through) = c.observed_through {
        conn.execute(
            "INSERT OR REPLACE INTO analytics_setting VALUES(?1,?2)",
            params![format!("observed_provider:{provider}"), through],
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn aliases_preserve_manual_decisions_and_reject_cycles() -> Result<()> {
        let c = Connection::open_in_memory()?;
        c.execute_batch(SCHEMA)?;
        set_alias(&c, "worktree", "repo", "manual")?;
        set_alias(&c, "worktree", "other", "git")?;
        assert_eq!(canonical_repo(&c, "worktree")?, "repo");
        assert!(set_alias(&c, "repo", "worktree", "manual").is_err());
        Ok(())
    }
    #[test]
    fn snapshot_observation_is_not_ingestion_time() -> Result<()> {
        let c = Connection::open_in_memory()?;
        c.execute_batch(SCHEMA)?;
        record(
            &c,
            &SourceCoverage {
                source_id: "snapshot".into(),
                provider: "opencode".into(),
                repo_root: None,
                observed_from: None,
                observed_through: Some("2026-10-27T18:15:00Z".into()),
                ingested_at: "2026-10-28T12:00:00Z".into(),
                complete: false,
                kind: "snapshot".into(),
                error: None,
            },
        )?;
        let rows = coverage(&c)?;
        assert!(!rows[0].complete);
        assert_ne!(
            rows[0].observed_through.as_deref(),
            Some(rows[0].ingested_at.as_str())
        );
        Ok(())
    }
}
