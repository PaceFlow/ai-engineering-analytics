//! One global opt-in covers every ingested project, subject to policy and budgets.
use super::{
    client::OpenRouterClient,
    config::{self, ClassifierConfig},
    questions::QUESTION_SET_VERSION,
    run::{self, Passes, RunOptions, SessionRef},
};
use anyhow::{Result, bail};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
pub const UNATTRIBUTED_PROJECT: &str = "__unattributed__";
#[derive(Debug, Clone, Serialize)]
pub struct ProjectStatus {
    pub repo_root: String,
    pub repository_attributed: bool,
    pub enabled: bool,
    pub since: String,
    pub monthly_budget: f64,
    pub max_run_usd: f64,
    pub spent_usd: f64,
    pub pending: usize,
    pub pending_extended: usize,
    pub deferred: usize,
    pub last_error: Option<String>,
    pub blocked_reason: Option<String>,
}
#[derive(Debug, Clone)]
struct Candidate {
    session: SessionRef,
    fingerprint: String,
    last_ms: Option<i64>,
    deferred: bool,
    current: bool,
}
pub fn configure_limits(
    conn: &Connection,
    repo: &str,
    budget: f64,
    run_cap: f64,
    days: u32,
) -> Result<()> {
    if !budget.is_finite() || budget <= 0.0 || !run_cap.is_finite() || run_cap <= 0.0 {
        bail!("Budgets must be positive finite amounts");
    }
    let since = (chrono::Utc::now() - chrono::Duration::days(days as i64)).to_rfc3339();
    conn.execute("INSERT INTO classifier_auto_project(repo_root,enabled,since,monthly_budget,max_run_usd) VALUES(?1,1,?2,?3,?4)
      ON CONFLICT(repo_root) DO UPDATE SET since=excluded.since,monthly_budget=excluded.monthly_budget,max_run_usd=excluded.max_run_usd,updated_at=datetime('now')",params![repo,since,budget,run_cap])?;
    Ok(())
}
pub fn project_spend(conn: &Connection, repo: &str) -> Result<f64> {
    let root = crate::insights::observation::canonical_repo(conn, repo)?;
    Ok(conn.query_row("SELECT COALESCE(SUM(usd),0) FROM classifier_project_spend WHERE at>=datetime('now','start of month') AND (repo_root=?1 OR repo_root=?2 OR repo_root IN(SELECT alias FROM repository_alias WHERE canonical=?2))",params![repo,root],|r|r.get(0))?)
}
pub fn project_cap(conn: &Connection, repo: &str, default: f64) -> Result<f64> {
    let root = crate::insights::observation::canonical_repo(conn, repo)?;
    Ok(conn.query_row("SELECT COALESCE(MIN(monthly_budget),?3) FROM classifier_auto_project WHERE repo_root=?1 OR repo_root=?2 OR repo_root IN(SELECT alias FROM repository_alias WHERE canonical=?2)",params![repo,root,default],|r|r.get(0))?)
}

fn actual_repo(repo: &str) -> Option<&str> {
    (repo != UNATTRIBUTED_PROJECT).then_some(repo)
}

fn candidates(conn: &Connection, repo: &str, since: &str, policy: &str) -> Result<Vec<Candidate>> {
    let mut stmt=conn.prepare("SELECT s.provider,s.session_id,'',COALESCE(MAX(t.last_activity_at),s.ended_at,s.started_at),s.ingest_version
       FROM metadata_sessions s LEFT JOIN metadata_repositories r ON r.id=s.repository_id
       LEFT JOIN derived_task t ON t.provider=s.provider AND t.session_id=s.session_id
       WHERE ((?1 IS NULL AND s.repository_id IS NULL) OR r.repo_root=?1) AND COALESCE(NULLIF(s.root_session_id,''),s.session_id)=s.session_id
       GROUP BY s.provider,s.session_id HAVING julianday(COALESCE(MAX(t.last_activity_at),s.ended_at,s.started_at)) >= julianday(?2) AND EXISTS(SELECT 1 FROM derived_turn d WHERE d.provider=s.provider AND d.session_id=s.session_id AND (d.user_chars>0 OR d.assistant_chars>0 OR d.tool_calls>0 OR d.tokens_total>0)) ORDER BY s.started_at DESC")?;
    let rows = stmt
        .query_map(params![actual_repo(repo), since], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, Option<i64>>(4)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut result = Vec::new();
    for (provider, session_id, fp, last, version) in rows {
        let dependencies:String=conn.query_row("SELECT COALESCE(group_concat(id || ':' || decision || ':' || kind),'') FROM work_relation WHERE to_id IN (SELECT identity_id FROM derived_work_member WHERE provider=?1 AND session_id=?2)",params![provider,session_id],|r|r.get(0))?;
        let reference = SessionRef {
            provider: provider.clone(),
            session_id: session_id.clone(),
            repo_root: actual_repo(repo).map(str::to_owned),
        };
        let _ = (&fp, &version);
        let content = run::session_fingerprint(conn, &reference)?;
        let fingerprint = format!(
            "{:x}",
            md5::compute(format!("{content}|{policy}|{dependencies}"))
        );
        let current:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM classifier_auto_state WHERE provider=?1 AND session_id=?2 AND fingerprint=?3 AND question_version=?4 AND status='completed')",params![provider,session_id,fingerprint,QUESTION_SET_VERSION],|r|r.get(0))?;
        let source_failed: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM analytics_setting WHERE key=?1)",
            [format!("source_error:{provider}")],
            |r| r.get(0),
        )?;
        let deferred = source_failed
            || last
                .as_deref()
                .and_then(crate::insights::structure::parse_ts)
                .is_none_or(|ts| chrono::Utc::now().timestamp_millis() - ts < 30 * 60_000);
        result.push(Candidate {
            session: SessionRef {
                provider,
                session_id,
                repo_root: actual_repo(repo).map(str::to_owned),
            },
            fingerprint,
            last_ms: last
                .as_deref()
                .and_then(crate::insights::structure::parse_ts),
            deferred,
            current,
        });
    }
    Ok(result)
}
fn extended_passes() -> Passes {
    Passes {
        rework: true,
        skills: true,
        prs: true,
        segment: false,
        feedback: false,
        labels: false,
        completion: false,
        stages: false,
    }
}
fn extended_current(conn: &Connection, c: &Candidate, config: &ClassifierConfig) -> Result<bool> {
    let fp = run::session_fingerprint(conn, &c.session)?;
    Ok(conn.query_row("SELECT EXISTS(SELECT 1 FROM classification_job j JOIN classification_job_unit u ON u.job_id=j.id WHERE j.state='completed' AND j.passes=?1 AND j.policy=?2 AND u.provider=?3 AND u.session_id=?4 AND u.fingerprint=?5 AND u.pass='session')",params![extended_passes().names(),run::job_policy(config),c.session.provider,c.session.session_id,fp],|r|r.get(0))?)
}
pub fn status(
    conn: &Connection,
    repo: Option<&str>,
    config: &ClassifierConfig,
) -> Result<Vec<ProjectStatus>> {
    let since =
        (chrono::Utc::now() - chrono::Duration::days(config.backfill_days as i64)).to_rfc3339();
    let mut stmt=conn.prepare("WITH roots AS (SELECT repo_root FROM classifier_auto_project UNION SELECT r.repo_root FROM metadata_repositories r JOIN metadata_sessions s ON s.repository_id=r.id UNION SELECT ?3 WHERE EXISTS(SELECT 1 FROM metadata_sessions WHERE repository_id IS NULL)) SELECT roots.repo_root,COALESCE(p.since,?2),COALESCE(p.monthly_budget,?4),COALESCE(p.max_run_usd,?5) FROM roots LEFT JOIN classifier_auto_project p ON p.repo_root=roots.repo_root WHERE ?1 IS NULL OR roots.repo_root=?1 ORDER BY roots.repo_root")?;
    let rows = stmt
        .query_map(
            params![
                repo,
                since,
                UNATTRIBUTED_PROJECT,
                config.project_monthly_budget_usd,
                config.max_run_usd
            ],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, f64>(2)?,
                    r.get::<_, f64>(3)?,
                ))
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut result = Vec::new();
    let key_available = if config.enabled {
        config::api_key()?.is_some()
    } else {
        false
    };
    for (repo, since, monthly_budget, max_run_usd) in rows {
        let allowed = config.project_allowed(actual_repo(&repo));
        let enabled = config.enabled && allowed;
        let candidates = candidates(
            conn,
            &repo,
            &since,
            &format!(
                "{}|{}|{}",
                config.model,
                config.level().as_str(),
                config.endpoint
            ),
        )?;
        let last_error=conn.query_row("SELECT a.error FROM classifier_auto_state a JOIN metadata_sessions s ON s.provider=a.provider AND s.session_id=a.session_id LEFT JOIN metadata_repositories r ON r.id=s.repository_id WHERE COALESCE(r.repo_root,?2)=?1 AND a.error IS NOT NULL ORDER BY a.updated_at DESC LIMIT 1",params![repo,UNATTRIBUTED_PROJECT],|r|r.get::<_,String>(0)).optional()?;
        let pending_extended = candidates
            .iter()
            .filter(|c| c.current && !c.deferred)
            .map(|c| extended_current(conn, c, config))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .filter(|done| !*done)
            .count();
        result.push(ProjectStatus {
            pending_extended,
            blocked_reason: if !config.enabled {
                Some("External classifier disabled".into())
            } else if !allowed {
                Some("Excluded by classifier project policy".into())
            } else if !key_available {
                Some("No classifier API key configured".into())
            } else {
                None
            },
            spent_usd: project_spend(conn, &repo)?,
            repository_attributed: actual_repo(&repo).is_some(),
            repo_root: repo,
            enabled,
            since,
            monthly_budget,
            max_run_usd,
            pending: candidates
                .iter()
                .filter(|c| !c.current && !c.deferred)
                .count(),
            deferred: candidates
                .iter()
                .filter(|c| !c.current && c.deferred)
                .count(),
            last_error,
        });
    }
    Ok(result)
}
pub fn after_ingest(conn: &Connection) -> Result<Vec<String>> {
    let config = ClassifierConfig::load()?;
    if !config.enabled {
        return Ok(vec![]);
    }
    ensure_projects(conn, &config)?;
    let Some(key) = config::api_key()? else {
        return Ok(vec![
            "Automatic classification pending: no OpenRouter key configured.".into(),
        ]);
    };
    let client = OpenRouterClient::new(&config.endpoint, &key)?;
    run_automatic(conn, &config, &client)
}

/// Register coverage horizons and spending limits, never a separate project activation gate.
pub fn ensure_projects(conn: &Connection, config: &ClassifierConfig) -> Result<()> {
    if !config.enabled {
        return Ok(());
    }
    let mut q=conn.prepare("SELECT DISTINCT r.repo_root FROM metadata_repositories r JOIN metadata_sessions s ON s.repository_id=r.id UNION SELECT ?1 WHERE EXISTS(SELECT 1 FROM metadata_sessions WHERE repository_id IS NULL)")?;
    let roots = q
        .query_map([UNATTRIBUTED_PROJECT], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let since =
        (chrono::Utc::now() - chrono::Duration::days(config.backfill_days as i64)).to_rfc3339();
    for root in roots
        .into_iter()
        .filter(|root| config.project_allowed(actual_repo(root)))
    {
        conn.execute("INSERT OR IGNORE INTO classifier_auto_project(repo_root,enabled,since,monthly_budget,max_run_usd) VALUES(?1,1,?2,?3,?4)",params![root,since,config.project_monthly_budget_usd,config.max_run_usd])?;
    }
    Ok(())
}

fn fairly_scheduled(conn: &Connection, mut rows: Vec<Candidate>) -> Result<Vec<Candidate>> {
    use std::collections::{BTreeMap, VecDeque};
    rows.sort_by_key(|c| std::cmp::Reverse(c.last_ms));
    let mut projects: BTreeMap<String, BTreeMap<i64, VecDeque<Candidate>>> = BTreeMap::new();
    for row in rows {
        let key = crate::insights::observation::canonical_repo(
            conn,
            row.session
                .repo_root
                .as_deref()
                .unwrap_or(UNATTRIBUTED_PROJECT),
        )?;
        projects
            .entry(key)
            .or_default()
            .entry(row.last_ms.unwrap_or(0).div_euclid(86_400_000))
            .or_default()
            .push_back(row);
    }
    let mut queues = Vec::new();
    for (_, mut dates) in projects {
        let mut queue = VecDeque::new();
        loop {
            let mut any = false;
            for date in dates.values_mut().rev() {
                if let Some(c) = date.pop_front() {
                    queue.push_back(c);
                    any = true;
                }
            }
            if !any {
                break;
            }
        }
        queues.push(queue);
    }
    let mut result = Vec::new();
    loop {
        let mut any = false;
        for queue in &mut queues {
            if let Some(c) = queue.pop_front() {
                result.push(c);
                any = true;
            }
        }
        if !any {
            break;
        }
    }
    Ok(result)
}
pub(crate) fn run_automatic(
    conn: &Connection,
    config: &ClassifierConfig,
    client: &dyn super::client::DecisionClient,
) -> Result<Vec<String>> {
    if !config.enabled {
        return Ok(vec![]);
    }
    ensure_projects(conn, config)?;
    let policy = format!(
        "{}|{}|{}",
        config.model,
        config.level().as_str(),
        config.endpoint
    );
    let mut queues = Vec::new();
    let mut messages = Vec::new();
    for project in status(conn, None, config)?
        .into_iter()
        .filter(|p| p.enabled && p.pending > 0)
    {
        if project_spend(conn, &project.repo_root)?
            >= project_cap(conn, &project.repo_root, config.project_monthly_budget_usd)?
        {
            messages.push(format!(
                "Automatic classification pending: budget reached for {}",
                project.repo_root
            ));
            continue;
        }
        queues.push(
            candidates(conn, &project.repo_root, &project.since, &policy)?
                .into_iter()
                .filter(|c| !c.deferred && !c.current)
                .collect::<std::collections::VecDeque<_>>(),
        );
    }
    // Round robin by project, newest work first. No project monopolizes the first minute.
    let mut chosen = Vec::new();
    loop {
        let mut any = false;
        for queue in &mut queues {
            if let Some(c) = queue.pop_front() {
                chosen.push(c);
                any = true;
            }
        }
        if !any {
            break;
        }
    }
    let extended = chosen.is_empty();
    let mut passes = if extended {
        extended_passes()
    } else {
        Passes::CORE
    };
    if extended {
        let mut queues = Vec::new();
        for project in status(conn, None, config)?
            .into_iter()
            .filter(|p| p.enabled && p.pending_extended > 0)
        {
            if project.spent_usd >= project.monthly_budget {
                continue;
            }
            let mut queue = std::collections::VecDeque::new();
            for c in candidates(conn, &project.repo_root, &project.since, &policy)?
                .into_iter()
                .filter(|c| c.current && !c.deferred)
            {
                if !extended_current(conn, &c, config)? {
                    queue.push_back(c);
                }
            }
            queues.push(queue);
        }
        loop {
            let mut any = false;
            for queue in &mut queues {
                if let Some(c) = queue.pop_front() {
                    chosen.push(c);
                    any = true;
                }
            }
            if !any {
                break;
            }
        }
    }
    if chosen.is_empty() {
        for pr in crate::insights::delivery::prs(conn)? {
            let (_, source) = crate::insights::delivery::intent(conn, &pr)?;
            if source != "missing" {
                continue;
            }
            let root = crate::delivery_sources::sources(conn)?
                .into_iter()
                .find(|s| s.repo_key() == pr.repo_key)
                .map(|s| s.repo_root)
                .unwrap_or_else(|| pr.repo_key.clone());
            if config.project_allowed(Some(&root)) {
                chosen.push(Candidate {
                    session: SessionRef {
                        provider: "pr".into(),
                        session_id: pr.id.clone(),
                        repo_root: Some(root),
                    },
                    fingerprint: String::new(),
                    last_ms: None,
                    deferred: false,
                    current: true,
                });
            }
        }
        if !chosen.is_empty() {
            passes = Passes {
                prs: true,
                rework: false,
                skills: false,
                segment: false,
                feedback: false,
                labels: false,
                completion: false,
                stages: false,
            };
        }
    }
    if chosen.is_empty() {
        return Ok(messages);
    }
    let chosen = fairly_scheduled(conn, chosen)?;
    let sessions: Vec<_> = chosen.iter().map(|c| c.session.clone()).collect();
    let resumable:Option<(i64,f64,f64)>=conn.query_row("SELECT id,max_usd,spent_usd FROM classification_job WHERE state='pending_auto' AND passes=?1 AND policy=?2 AND spent_usd<max_usd ORDER BY id DESC LIMIT 1",params![passes.names(),QUESTION_SET_VERSION],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    let options = RunOptions {
        level: config.level(),
        max_usd: resumable
            .map(|(_, max, spent)| (max - spent).min(config.max_run_usd))
            .unwrap_or(config.max_run_usd),
        passes,
        dry_run: false,
        dispatch_seconds: Some(config.automatic_dispatch_seconds),
        job_id: resumable.map(|r| r.0),
    };
    match run::run(conn, client, config, &sessions, &options) {
        Ok(summary) => {
            let complete =
                summary.stopped.is_none() && summary.failed == 0 && summary.invalid_answers == 0;
            let error = summary.stopped.clone().or_else(|| {
                (!complete).then(|| {
                    format!(
                        "{} failed, {} invalid",
                        summary.failed, summary.invalid_answers
                    )
                })
            });
            if !complete && let Some(id) = summary.job_id {
                conn.execute(
                    "UPDATE classification_job SET state='pending_auto' WHERE id=?1",
                    [id],
                )?;
            }
            for candidate in chosen.iter().filter(|_| !extended) {
                conn.execute("INSERT INTO classifier_auto_state VALUES(?1,?2,?3,?4,?5,?6,datetime('now')) ON CONFLICT(provider,session_id) DO UPDATE SET fingerprint=excluded.fingerprint,question_version=excluded.question_version,status=excluded.status,error=excluded.error,updated_at=excluded.updated_at",params![candidate.session.provider,candidate.session.session_id,candidate.fingerprint,QUESTION_SET_VERSION,if complete{"completed"}else{"pending"},error])?;
            }
            messages.push(format!(
                "Automatic classification job {}: {} requests, {} cached, ${:.4}; {}",
                summary.job_id.unwrap_or_default(),
                summary.sent,
                summary.cached,
                summary.cost_usd,
                if complete {
                    if extended {
                        "relationship and skill passes complete".to_string()
                    } else {
                        "core passes complete".to_string()
                    }
                } else {
                    format!(
                        "pending: {}; resume with classify run --resume JOB_ID or next ingest",
                        error.as_deref().unwrap_or("classification incomplete")
                    )
                }
            ));
        }
        Err(error) => messages.push(format!("Automatic classification pending: {error}")),
    }
    Ok(messages)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::classify::client::{ScriptedClient, canned_response};
    use crate::db::session_store::{
        MessageRecord, SessionRecord, UsageRecord, refresh_session_roots, store_session_record,
    };

    fn add_session(conn: &Connection, id: &str, repo: Option<&str>) -> Result<()> {
        let start = (chrono::Utc::now() - chrono::Duration::hours(3)).to_rfc3339();
        let end = (chrono::Utc::now() - chrono::Duration::hours(2)).to_rfc3339();
        let mut s = SessionRecord::new("codex", id);
        s.started_at = Some(start.clone());
        s.ended_at = Some(end.clone());
        s.messages = vec![
            MessageRecord {
                key: format!("{id}-request"),
                ts: Some(start),
                role: "user".into(),
                text: format!("Explain behavior for {id}"),
                is_compaction_summary: false,
            },
            MessageRecord {
                key: format!("{id}-answer"),
                ts: Some(end.clone()),
                role: "assistant".into(),
                text: "Here is the explanation".into(),
                is_compaction_summary: false,
            },
        ];
        let mut usage = UsageRecord::new("usage");
        usage.ts = Some(end);
        usage.input_tokens = 100;
        s.usage.push(usage);
        store_session_record(conn, &s)?;
        if let Some(repo) = repo {
            let rid = crate::db::upsert_metadata_repository(conn, repo)?;
            conn.execute(
                "UPDATE metadata_sessions SET repository_id=?1 WHERE session_id=?2",
                params![rid, id],
            )?;
        }
        refresh_session_roots(conn)?;
        crate::insights::refresh(conn)
    }

    fn client() -> ScriptedClient {
        ScriptedClient::new(|request| {
            Ok(canned_response(request, |_, q| {
                let label = match q["type"].as_str() {
                    Some("noul") => "false".into(),
                    Some("score") => "1".into(),
                    _ => {
                        let choices = q["criteria"].as_object().unwrap();
                        ["unclear", "other", "not_completed", "continue"]
                            .into_iter()
                            .find(|k| choices.contains_key(*k))
                            .map(str::to_owned)
                            .unwrap_or_else(|| choices.keys().next().unwrap().clone())
                    }
                };
                (label, 0.9)
            }))
        })
    }

    #[test]
    fn one_global_enable_covers_current_future_and_unattributed_work_and_reuses_cache() -> Result<()>
    {
        let conn = Connection::open_in_memory()?;
        crate::db::init_app_schema(&conn)?;
        add_session(&conn, "first", Some("/work/first"))?;
        add_session(&conn, "second", Some("/work/second"))?;
        add_session(&conn, "private", Some("/work/private"))?;
        add_session(&conn, "unattributed", None)?;
        let client = client();
        let mut config = ClassifierConfig {
            deny_projects: vec!["/work/private".into()],
            ..Default::default()
        };
        // Legacy per-project activation cannot turn on a globally disabled classifier.
        configure_limits(&conn, "/work/first", 2.0, 0.5, 56)?;
        run_automatic(&conn, &config, &client)?;
        assert_eq!(client.calls(), 0);
        config.enabled = true;
        conn.execute("UPDATE classifier_auto_project SET enabled=0", [])?;
        run_automatic(&conn, &config, &client)?;
        assert!(client.calls() > 0);
        let completed:Vec<String>=conn.prepare("SELECT session_id FROM classifier_auto_state WHERE status='completed' ORDER BY session_id")?.query_map([],|r|r.get(0))?.collect::<rusqlite::Result<_>>()?;
        assert_eq!(completed, vec!["first", "second", "unattributed"]);
        assert!(project_spend(&conn, UNATTRIBUTED_PROJECT)? > 0.0);
        let calls = client.calls();
        run_automatic(&conn, &config, &client)?;
        assert_eq!(client.calls(), calls);
        add_session(&conn, "future", Some("/work/future"))?;
        run_automatic(&conn, &config, &client)?;
        assert!(client.calls() > calls);
        let enrolled: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM classifier_auto_project WHERE repo_root='/work/future')",
            [],
            |r| r.get(0),
        )?;
        assert!(enrolled);
        let rows = status(&conn, None, &config)?;
        assert!(
            rows.iter()
                .find(|p| p.repo_root == "/work/future")
                .unwrap()
                .enabled
        );
        assert!(
            !rows
                .iter()
                .find(|p| p.repo_root == "/work/private")
                .unwrap()
                .enabled
        );
        assert!(
            !rows
                .iter()
                .find(|p| p.repo_root == UNATTRIBUTED_PROJECT)
                .unwrap()
                .repository_attributed
        );
        let calls = client.calls();
        config.enabled = false;
        add_session(&conn, "after-disable", Some("/work/later"))?;
        run_automatic(&conn, &config, &client)?;
        assert_eq!(client.calls(), calls);
        Ok(())
    }

    #[test]
    fn global_automatic_run_cap_stops_dispatch_without_project_activation() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        crate::db::init_app_schema(&conn)?;
        add_session(&conn, "first", Some("/work/first"))?;
        add_session(&conn, "second", Some("/work/second"))?;
        let client = client();
        let config = ClassifierConfig {
            enabled: true,
            max_run_usd: 0.000000001,
            ..Default::default()
        };
        let messages = run_automatic(&conn, &config, &client)?;
        assert_eq!(client.calls(), 0);
        assert!(messages.iter().any(|m| m.contains("budget")));
        assert_eq!(run::monthly_spend(&conn)?, 0.0);
        Ok(())
    }
}
