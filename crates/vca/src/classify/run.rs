//! Classification runs: plan, preview and send.
//!
//! Order per run: segmentation (which task each user turn belongs to,
//! sequential within a session) → structure refresh → exchange feedback →
//! task labels → completion turn → full insight refresh. Every answer is
//! cached by content, so re-running only sends what changed.

use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::classify::client::{
    Answer, DecisionClient, DecisionRequest, ErrorKind, answer_valid, band,
};
use crate::classify::config::ClassifierConfig;
use crate::classify::heuristic::{task_unit_key, turn_unit_key};
use crate::classify::questions::{
    QUESTION_SET_VERSION, SKILL_QUESTION_VERSION, completion_question, exchange_questions,
    rework_question, skill_follow_through_question, task_assignment_question, task_label_questions,
    turn_questions,
};
use crate::classify::state::{
    BuiltState, Level, TurnFacts, assignment_state, estimate_tokens, exchange_state, head_tail,
    load_turns, redact, task_state, turn_option,
};

const MAX_COMPLETION_OPTIONS: usize = 250;

#[derive(Debug, Clone)]
pub struct SessionRef {
    pub provider: String,
    pub session_id: String,
    pub repo_root: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct Passes {
    pub segment: bool,
    pub feedback: bool,
    pub labels: bool,
    pub completion: bool,
    pub rework: bool,
    pub stages: bool,
    pub skills: bool,
    pub prs: bool,
}

impl Passes {
    pub const ALL: Self = Self {
        segment: true,
        feedback: true,
        labels: true,
        completion: true,
        rework: true,
        stages: true,
        skills: true,
        prs: true,
    };

    pub fn parse(value: &str) -> Option<Self> {
        let mut passes = Self {
            segment: false,
            feedback: false,
            labels: false,
            completion: false,
            rework: false,
            stages: false,
            skills: false,
            prs: false,
        };
        for part in value.split(',').map(str::trim) {
            match part {
                "all" => return Some(Self::ALL),
                "segment" | "segmentation" => passes.segment = true,
                "feedback" | "exchanges" => passes.feedback = true,
                "labels" | "tasks" => passes.labels = true,
                "completion" => passes.completion = true,
                "rework" | "relations" => passes.rework = true,
                "stages" | "stage" => passes.stages = true,
                "skills" => passes.skills = true,
                "prs" => passes.prs = true,
                _ => return None,
            }
        }
        Some(passes)
    }
}

#[derive(Debug, Clone)]
pub struct RunOptions {
    pub level: Level,
    pub max_usd: f64,
    pub passes: Passes,
    /// Count units and tokens without sending anything.
    pub dry_run: bool,
    pub dispatch_seconds: Option<u64>,
    pub job_id: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct RunSummary {
    pub run_id: Option<i64>,
    pub job_id: Option<i64>,
    pub sessions: usize,
    pub excluded_projects: usize,
    pub units: BTreeMap<String, usize>,
    pub sent: usize,
    pub cached: usize,
    pub failed: usize,
    pub invalid_answers: usize,
    pub degraded: usize,
    pub redactions: usize,
    pub est_tokens: usize,
    pub est_usd: f64,
    pub input_tokens: i64,
    pub cost_usd: f64,
    pub budget_usd: f64,
    pub tasks_split: usize,
    pub completions: usize,
    pub completion_decisions: usize,
    pub new_completions: usize,
    pub stopped: Option<String>,
}

struct Asked {
    answers: BTreeMap<String, (Answer, &'static str)>,
}

type Builder<'a> = dyn Fn(Level) -> (BuiltState, Map<String, Value>) + 'a;

struct Runner<'a> {
    conn: &'a Connection,
    client: Option<&'a dyn DecisionClient>,
    config: &'a ClassifierConfig,
    level: Level,
    run_id: Option<i64>,
    budget: f64,
    summary: RunSummary,
    deadline: Option<Instant>,
    prepaid: bool,
    prefetched: BTreeSet<String>,
    job_id: Option<i64>,
    prefetched_tasks: BTreeSet<String>,
}

pub fn monthly_spend(conn: &Connection) -> Result<f64> {
    Ok(conn.query_row(
        "SELECT COALESCE(SUM(cost_usd), 0) FROM classifier_run
         WHERE started_at >= datetime('now', 'start of month')",
        [],
        |row| row.get(0),
    )?)
}

/// Count what a run would send (nothing is sent).
pub fn plan(
    conn: &Connection,
    config: &ClassifierConfig,
    sessions: &[SessionRef],
    level: Level,
) -> Result<RunSummary> {
    plan_passes(conn, config, sessions, level, Passes::ALL)
}
pub fn plan_passes(
    conn: &Connection,
    config: &ClassifierConfig,
    sessions: &[SessionRef],
    level: Level,
    passes: Passes,
) -> Result<RunSummary> {
    execute(
        conn,
        None,
        config,
        sessions,
        &RunOptions {
            level,
            max_usd: f64::INFINITY,
            passes,
            dry_run: true,
            dispatch_seconds: None,
            job_id: None,
        },
    )
}

pub fn run(
    conn: &Connection,
    client: &dyn DecisionClient,
    config: &ClassifierConfig,
    sessions: &[SessionRef],
    options: &RunOptions,
) -> Result<RunSummary> {
    // Serialize all entry points, including TUI and automatic runs. SQLite is not
    // held in a write transaction during network calls.
    static RUN_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _guard = RUN_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let database: String = conn.query_row(
        "SELECT file FROM pragma_database_list WHERE name='main'",
        [],
        |r| r.get(0),
    )?;
    let _file_lock = if database.is_empty() {
        None
    } else {
        use fs2::FileExt;
        let path = std::path::Path::new(&database).with_extension("classifier.lock");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path)?;
        file.try_lock_exclusive().map_err(|_| {
            anyhow::anyhow!("Another classification run is active; retry after it finishes")
        })?;
        Some(file)
    };
    let before: i64 = conn.query_row(
        "SELECT COALESCE(MAX(id),0) FROM classification_job",
        [],
        |r| r.get(0),
    )?;
    let result = execute(conn, Some(client), config, sessions, options);
    if let Err(error) = &result {
        if let Some(id) = options.job_id {
            conn.execute("UPDATE classification_job SET state='pending',error=?2,updated_at=datetime('now') WHERE id=?1",params![id,error.to_string()])?;
        } else {
            conn.execute("UPDATE classification_job SET state='pending',error=?2,updated_at=datetime('now') WHERE id>?1 AND state='running'",params![before,error.to_string()])?;
        }
    }
    result
}

fn execute(
    conn: &Connection,
    client: Option<&dyn DecisionClient>,
    config: &ClassifierConfig,
    sessions: &[SessionRef],
    options: &RunOptions,
) -> Result<RunSummary> {
    let remaining_job = options
        .job_id
        .map(|id| {
            conn.query_row(
                "SELECT max_usd-spent_usd FROM classification_job WHERE id=?1",
                [id],
                |r| r.get::<_, f64>(0),
            )
        })
        .transpose()?
        .unwrap_or(f64::INFINITY)
        .max(0.0);
    let budget = if options.dry_run {
        f64::INFINITY
    } else {
        options
            .max_usd
            .min(remaining_job)
            .min((config.monthly_budget_usd - monthly_spend(conn)?).max(0.0))
    };
    let job_id = if options.dry_run {
        None
    } else if let Some(id) = options.job_id {
        Some(id)
    } else {
        conn.execute("INSERT INTO classification_job(created_at,updated_at,state,passes,level,policy,max_usd) VALUES(datetime('now'),datetime('now'),'running',?1,?2,?3,?4)",params![options.passes.names(),options.level.as_str(),job_policy(config),options.max_usd])?;
        Some(conn.last_insert_rowid())
    };
    if let Some(id) = job_id {
        conn.execute("UPDATE classification_job SET state='running',updated_at=datetime('now'),error=NULL WHERE id=?1",[id])?;
        for session in sessions {
            conn.execute("INSERT OR IGNORE INTO classification_job_unit(job_id,provider,session_id,repo_root,fingerprint,state,pass) VALUES(?1,?2,?3,?4,?5,'pending','session')",params![id,session.provider,session.session_id,session.repo_root,session_fingerprint(conn,session)?])?;
        }
    }
    let mut runner = Runner {
        conn,
        client: if options.dry_run { None } else { client },
        config,
        level: options.level,
        run_id: None,
        budget,
        summary: RunSummary {
            budget_usd: budget,
            job_id,
            ..RunSummary::default()
        },
        deadline: options
            .dispatch_seconds
            .map(|s| Instant::now() + Duration::from_secs(s)),
        prepaid: false,
        prefetched: BTreeSet::new(),
        prefetched_tasks: BTreeSet::new(),
        job_id,
    };
    if !options.dry_run {
        conn.execute(
            "INSERT INTO classifier_run(model, endpoint, question_set_version, level, status)
             VALUES (?1, ?2, ?3, ?4, 'running')",
            params![
                config.model,
                config.endpoint,
                QUESTION_SET_VERSION,
                options.level.as_str()
            ],
        )?;
        runner.run_id = Some(conn.last_insert_rowid());
        runner.summary.run_id = runner.run_id;
    }

    let mut selected: Vec<(&SessionRef, Vec<TurnFacts>)> = Vec::new();
    for session in sessions {
        if !config.project_allowed(session.repo_root.as_deref()) {
            runner.summary.excluded_projects += 1;
            continue;
        }
        let turns = load_turns(conn, &session.provider, &session.session_id)?;
        if turns.is_empty() {
            continue;
        }
        selected.push((session, turns));
    }
    runner.summary.sessions = selected.len();

    let segment = options.passes.segment && options.level > Level::EvidenceOnly;
    if segment || options.passes.stages {
        let mut changed = false;
        for (session, turns) in &selected {
            if runner.stopped() {
                break;
            }
            changed |= runner.segment_session(session, turns, segment, options.passes.stages)?;
        }
        if changed && !options.dry_run {
            crate::insights::refresh_structure(conn)?;
        }
    }
    if options.passes.feedback && options.level > Level::EvidenceOnly {
        for (session, turns) in &selected {
            if runner.stopped() {
                break;
            }
            runner.feedback_session(session, turns)?;
        }
    }
    if options.passes.labels || options.passes.completion {
        if !options.dry_run && config.concurrency > 1 {
            let mut units = Vec::new();
            for (session, turns) in &selected {
                let shared = Arc::new(turns.clone());
                for task in load_tasks(conn, session)? {
                    for (enabled, kind) in [
                        (options.passes.labels, "task"),
                        (options.passes.completion, "task_completion"),
                    ] {
                        if enabled {
                            units.push(TaskUnit {
                                kind,
                                key: task_unit_key(
                                    &session.provider,
                                    &session.session_id,
                                    task.index,
                                ),
                                task: task.clone(),
                                turns: shared.clone(),
                                repo: session.repo_root.clone(),
                            });
                        }
                    }
                }
            }
            runner.prefetch_tasks(&units)?;
        }
        for (session, turns) in &selected {
            for task in load_tasks(conn, session)? {
                if runner.stopped()
                    && !runner.prefetched_tasks.contains(&task_unit_key(
                        &session.provider,
                        &session.session_id,
                        task.index,
                    ))
                {
                    continue;
                }
                if options.passes.labels {
                    runner.label_task(session, turns, &task)?;
                }
                if options.passes.completion {
                    runner.complete_task(session, turns, &task)?;
                }
            }
        }
    }

    if options.passes.prs {
        runner.classify_prs(sessions)?;
    }
    if options.passes.skills {
        for (session, turns) in &selected {
            for task in load_tasks(conn, session)? {
                runner.classify_skills(session, turns, &task)?;
            }
        }
    }
    if options.passes.rework {
        let selected_keys: Vec<(String, String)> = selected
            .iter()
            .map(|(session, _)| (session.provider.clone(), session.session_id.clone()))
            .collect();
        for relation in load_relations(conn)? {
            if selected_keys.contains(&(relation.to.0.clone(), relation.to.1.clone())) {
                runner.classify_relation(&relation)?;
            }
        }
    }

    if options.passes.rework && !options.dry_run {
        crate::insights::refresh(conn)?;
        runner.classify_work_links(sessions)?;
    }
    runner.summary.est_usd = config.estimate_usd(runner.summary.est_tokens);
    if let Some(run_id) = runner.run_id {
        let summary = &runner.summary;
        let status = match &summary.stopped {
            Some(reason) if reason.starts_with("aborted") => "aborted",
            Some(_) => "budget_reached",
            None => "completed",
        };
        conn.execute(
            "UPDATE classifier_run SET finished_at = datetime('now'), units_planned = ?2,
                 units_sent = ?3, units_cached = ?4, input_tokens = ?5, cost_usd = ?6,
                 status = ?7, error = ?8
             WHERE run_id = ?1",
            params![
                run_id,
                summary.units.values().sum::<usize>() as i64,
                summary.sent as i64,
                summary.cached as i64,
                summary.input_tokens,
                summary.cost_usd,
                status,
                summary.stopped
            ],
        )?;
        if summary.sent > 0 || summary.cached > 0 {
            crate::insights::refresh(conn)?;
        }
    }
    if let Some(id) = job_id {
        let state = if runner.summary.stopped.is_none()
            && runner.summary.failed == 0
            && runner.summary.invalid_answers == 0
        {
            "completed"
        } else {
            "pending"
        };
        conn.execute("UPDATE classification_job SET state=?2,updated_at=datetime('now'),error=?3 WHERE id=?1",params![id,state,runner.summary.stopped])?;
        if state == "completed" {
            conn.execute("UPDATE classification_job_unit SET state='completed',error=NULL WHERE job_id=?1 AND pass='session'",[id])?;
        }
    }
    Ok(runner.summary)
}

impl Passes {
    pub const CORE: Self = Self {
        segment: true,
        feedback: true,
        labels: true,
        completion: true,
        rework: false,
        stages: true,
        skills: false,
        prs: true,
    };
    pub fn names(self) -> String {
        [
            (self.segment, "segment"),
            (self.feedback, "feedback"),
            (self.labels, "labels"),
            (self.completion, "completion"),
            (self.stages, "stages"),
            (self.rework, "rework"),
            (self.skills, "skills"),
            (self.prs, "prs"),
        ]
        .into_iter()
        .filter_map(|(enabled, name)| enabled.then_some(name))
        .collect::<Vec<_>>()
        .join(",")
    }
}

// SQLite otherwise favors scanning the provider's ordered fact index for each
// conversation. Keep session selection first, then perform indexed fact lookups.
const SESSION_FINGERPRINT_QUERIES: [&str; 2] = [
    "SELECT COALESCE(m.message_key,'')||':'||COALESCE(m.message_ts,'')||':'||m.role||':'||m.content FROM metadata_sessions s CROSS JOIN fact_session_message m ON m.provider=s.provider AND m.session_id=s.session_id WHERE s.provider=?1 AND (s.root_session_id=?2 OR (s.root_session_id IS NULL AND s.session_id=?2)) ORDER BY m.session_id,m.message_index",
    "SELECT t.call_key||':'||COALESCE(t.ts,'')||':'||t.tool_name||':'||COALESCE(t.target,'')||':'||t.status FROM metadata_sessions s CROSS JOIN fact_session_tool_call t ON t.provider=s.provider AND t.session_id=s.session_id WHERE s.provider=?1 AND (s.root_session_id=?2 OR (s.root_session_id IS NULL AND s.session_id=?2)) ORDER BY t.session_id,t.call_index",
];

pub fn session_fingerprint(conn: &Connection, session: &SessionRef) -> Result<String> {
    let mut material = super::request::VERSION.to_owned();
    for sql in SESSION_FINGERPRINT_QUERIES {
        for value in conn
            .prepare(sql)?
            .query_map(params![session.provider, session.session_id], |r| {
                r.get::<_, String>(0)
            })?
        {
            material.push_str(&value?);
            material.push('\0');
        }
    }
    Ok(format!("{:x}", md5::compute(material)))
}

pub fn job_policy(config: &ClassifierConfig) -> String {
    format!(
        "{}|{}|{}|{}|{}",
        QUESTION_SET_VERSION,
        SKILL_QUESTION_VERSION,
        config.model,
        config.endpoint,
        config.level().as_str()
    )
}
pub fn resume_job(
    conn: &Connection,
    id: i64,
    config: &ClassifierConfig,
) -> Result<(Vec<SessionRef>, RunOptions)> {
    let (state,passes,level,policy,max,spent):(String,String,String,String,f64,f64)=conn.query_row("SELECT state,passes,level,policy,max_usd,spent_usd FROM classification_job WHERE id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)))?;
    anyhow::ensure!(
        state != "completed",
        "Classification job is already complete"
    );
    anyhow::ensure!(
        policy == job_policy(config),
        "Classifier policy changed; start a new run"
    );
    anyhow::ensure!(
        spent < max,
        "This job reached its spending cap; start an explicit new run to extend work"
    );
    let sessions=conn.prepare("SELECT provider,session_id,repo_root FROM classification_job_unit WHERE job_id=?1 AND pass='session' ORDER BY rowid")?.query_map([id],|r|Ok(SessionRef{provider:r.get(0)?,session_id:r.get(1)?,repo_root:r.get(2)?}))?.collect::<rusqlite::Result<Vec<_>>>()?;
    Ok((
        sessions,
        RunOptions {
            level: Level::parse(&level)
                .ok_or_else(|| anyhow::anyhow!("Invalid saved content level"))?,
            max_usd: max - spent,
            passes: Passes::parse(&passes)
                .ok_or_else(|| anyhow::anyhow!("Invalid saved passes"))?,
            dry_run: false,
            dispatch_seconds: None,
            job_id: Some(id),
        },
    ))
}

struct ResponseTransaction<'a> {
    conn: &'a Connection,
    done: bool,
}
impl<'a> ResponseTransaction<'a> {
    fn new(conn: &'a Connection) -> Result<Self> {
        conn.execute_batch("SAVEPOINT classifier_response")?;
        Ok(Self { conn, done: false })
    }
    fn commit(mut self) -> Result<()> {
        self.conn.execute_batch("RELEASE classifier_response")?;
        self.done = true;
        Ok(())
    }
}
impl Drop for ResponseTransaction<'_> {
    fn drop(&mut self) {
        if !self.done {
            let _ = self
                .conn
                .execute_batch("ROLLBACK TO classifier_response; RELEASE classifier_response");
        }
    }
}

struct TaskUnit {
    kind: &'static str,
    key: String,
    task: TaskRef,
    turns: Arc<Vec<TurnFacts>>,
    repo: Option<String>,
}
impl TaskUnit {
    fn build(&self, level: Level) -> (BuiltState, Map<String, Value>) {
        let mut built = task_state(
            &self.task.harness,
            &self.turns,
            &self.task.turns,
            self.task.commits,
            self.task.subagents,
            level,
        );
        if self.kind == "task" {
            let questions = task_label_questions(built.level);
            return (built, questions);
        }
        let task_turns: Vec<_> = self
            .turns
            .iter()
            .filter(|t| self.task.turns.contains(&t.index))
            .collect();
        let skip = task_turns.len().saturating_sub(MAX_COMPLETION_OPTIONS);
        let mut redactions = 0;
        let options: Vec<_> = task_turns[skip..]
            .iter()
            .map(|t| (t.index, turn_option(t, built.level, &mut redactions)))
            .collect();
        built.redactions += redactions;
        (built, completion_question(&options))
    }
}
struct ReplyClient<'a> {
    response: Mutex<
        Option<
            std::result::Result<
                crate::classify::client::DecisionResponse,
                crate::classify::client::ClientError,
            >,
        >,
    >,
    fallback: &'a dyn DecisionClient,
}
impl DecisionClient for ReplyClient<'_> {
    fn decide(
        &self,
        request: &DecisionRequest,
    ) -> std::result::Result<
        crate::classify::client::DecisionResponse,
        crate::classify::client::ClientError,
    > {
        if let Some(response) = self
            .response
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            response
        } else {
            self.fallback.decide(request)
        }
    }
}
impl Runner<'_> {
    fn record_job_unit(
        &self,
        kind: &str,
        unit_key: &str,
        fingerprint: &str,
        state: &str,
        error: Option<&str>,
    ) -> Result<()> {
        if let Some(job) = self.job_id {
            let parts: Vec<_> = unit_key.split('|').collect();
            let provider = parts.first().copied().unwrap_or(kind);
            let session = parts.get(1).copied().unwrap_or(unit_key);
            self.conn.execute("INSERT INTO classification_job_unit(job_id,provider,session_id,repo_root,fingerprint,state,pass,error) VALUES(?1,?2,?3,NULL,?4,?5,?6,?7) ON CONFLICT(job_id,provider,session_id,pass) DO UPDATE SET fingerprint=excluded.fingerprint,state=excluded.state,error=excluded.error",params![job,provider,session,fingerprint,state,format!("{kind}:{}",parts.get(2).copied().unwrap_or(unit_key)),error])?;
        }
        Ok(())
    }
    fn prefetch_tasks(&mut self, units: &[TaskUnit]) -> Result<()> {
        let Some(client) = self.client else {
            return Ok(());
        };
        let concurrency = self.config.concurrency.clamp(1, 16);
        let mut at = 0;
        while at < units.len() && !self.stopped() {
            if self.deadline.is_some_and(|d| Instant::now() >= d) {
                self.summary.stopped = Some(
                    "dispatch time budget reached; resume the saved classification job".into(),
                );
                break;
            }
            let mut indices = vec![];
            let mut requests = vec![];
            let mut reserved = 0.0;
            let mut project_reserved: BTreeMap<String, f64> = BTreeMap::new();
            while at < units.len() && requests.len() < concurrency {
                let index = at;
                at += 1;
                let unit = &units[index];
                let (built, questions) = unit.build(self.level);
                let key = cache_key(&self.config.model, built.level, &questions, &built.state);
                if self
                    .cached(unit.kind, &unit.key, &key, &questions)?
                    .is_some()
                {
                    continue;
                }
                let request = DecisionRequest {
                    model: self.config.model.clone(),
                    state: built.state,
                    questions,
                    provider: self.config.provider.clone(),
                };
                let estimate = self.config.estimate_usd(estimate_tokens(&request.body()));
                if self.summary.cost_usd + reserved + estimate > self.budget {
                    self.summary.stopped = Some(
                        "run budget reached; resume pending work within remaining limits".into(),
                    );
                    break;
                }
                let repo = crate::insights::observation::canonical_repo(
                    self.conn,
                    unit.repo
                        .as_deref()
                        .unwrap_or(super::automatic::UNATTRIBUTED_PROJECT),
                )?;
                let cap = super::automatic::project_cap(
                    self.conn,
                    &repo,
                    self.config.project_monthly_budget_usd,
                )?;
                if super::automatic::project_spend(self.conn, &repo)?
                    + project_reserved.get(&repo).copied().unwrap_or(0.0)
                    + estimate
                    > cap
                {
                    self.summary.stopped = Some(format!("project budget reached: {repo}"));
                    break;
                }
                reserved += estimate;
                *project_reserved.entry(repo).or_default() += estimate;
                indices.push(index);
                requests.push(request);
            }
            if requests.is_empty() {
                continue;
            }
            let mut error = None;
            client.decide_many(&requests, &mut |index, response| {
                let unit = &units[indices[index]];
                let reply = ReplyClient {
                    response: Mutex::new(Some(response)),
                    fallback: client,
                };
                let mut child = Runner {
                    conn: self.conn,
                    client: Some(&reply),
                    config: self.config,
                    level: self.level,
                    run_id: self.run_id,
                    budget: self.budget,
                    summary: self.summary.clone(),
                    deadline: self.deadline,
                    prepaid: true,
                    prefetched: BTreeSet::new(),
                    prefetched_tasks: BTreeSet::new(),
                    job_id: self.job_id,
                };
                let result = child.ask(unit.kind, &unit.key, &|level| unit.build(level));
                self.summary = child.summary;
                self.prefetched_tasks.insert(unit.key.clone());
                let (built, questions) = unit.build(self.level);
                self.prefetched.insert(cache_key(
                    &self.config.model,
                    built.level,
                    &questions,
                    &built.state,
                ));
                if let Err(e) = result {
                    error = Some(e);
                }
            });
            if let Some(e) = error {
                return Err(e);
            }
        }
        Ok(())
    }
    fn classify_prs(&mut self, sessions: &[SessionRef]) -> Result<()> {
        use crate::insights::{delivery, observation};
        let roots: BTreeSet<_> = sessions
            .iter()
            .filter_map(|s| s.repo_root.as_deref())
            .map(|r| observation::canonical_repo(self.conn, r))
            .collect::<Result<_>>()?;
        for pr in delivery::prs(self.conn)? {
            if !roots.contains(&observation::canonical_repo(self.conn, &pr.repo_key)?) {
                continue;
            }
            let (source_intent, source) = delivery::intent(self.conn, &pr)?;
            if source == "override" || source == "mapping" {
                let _ = source_intent;
                continue;
            }
            let build = |level: Level| {
                let text = format!("{}\n{}", pr.title, pr.description);
                let text = if level == Level::EvidenceOnly {
                    String::new()
                } else if level == Level::Full {
                    text
                } else {
                    crate::classify::state::strip_code_blocks(&text)
                };
                let (text, redactions) = redact(&head_tail(&text, 12000));
                (
                    BuiltState {
                        state: json!({"pull_request":{"title_and_description":text,"labels":pr.labels,"state":pr.state,"files":pr.files}}),
                        level,
                        redactions,
                        est_tokens: 0,
                    },
                    crate::classify::questions::pr_questions(),
                )
            };
            if let Some(asked) = self.ask("pr", &pr.id, &build)?
                && let Some((answer, band)) = asked.answers.get("intent")
                && matches!(*band, "confident" | "tentative")
                && let Some(label) = answer.label.as_deref()
            {
                delivery::set_intent(self.conn, &pr.id, label, "classifier")?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct TaskRef {
    index: i64,
    turns: Vec<i64>,
    harness: String,
    commits: i64,
    subagents: i64,
}

fn load_tasks(conn: &Connection, session: &SessionRef) -> Result<Vec<TaskRef>> {
    let mut stmt = conn.prepare(
        "SELECT task_index, COALESCE(turn_list, ''), harness, commits, subagents FROM derived_task
         WHERE provider = ?1 AND session_id = ?2 ORDER BY task_index",
    )?;
    let rows = stmt.query_map(params![session.provider, session.session_id], |row| {
        Ok(TaskRef {
            index: row.get(0)?,
            turns: row
                .get::<_, String>(1)?
                .split(',')
                .filter_map(|value| value.trim().parse().ok())
                .collect(),
            harness: row.get(2)?,
            commits: row.get(3)?,
            subagents: row.get(4)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

/// Smallest overlap worth asking about; smaller overlaps keep the heuristic.
const MIN_RELATION_LINES: i64 = 5;

#[derive(Debug, Clone)]
struct RelationRef {
    from: (String, String, i64),
    to: (String, String, i64),
    lines: i64,
    files: i64,
    gap_hours: Option<f64>,
}

fn load_relations(conn: &Connection) -> Result<Vec<RelationRef>> {
    let mut stmt = conn.prepare(
        "SELECT from_provider, from_session, from_task, to_provider, to_session, to_task,
                overlap_lines, overlap_files, gap_hours
         FROM derived_task_relation
         WHERE relation_kind = 'touches_earlier_ai_lines' AND overlap_lines >= ?1",
    )?;
    let rows = stmt.query_map(params![MIN_RELATION_LINES], |row| {
        Ok(RelationRef {
            from: (row.get(0)?, row.get(1)?, row.get(2)?),
            to: (row.get(3)?, row.get(4)?, row.get(5)?),
            lines: row.get(6)?,
            files: row.get(7)?,
            gap_hours: row.get(8)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

/// First request and evidence summary of one task, at a content level.
fn task_side(
    conn: &Connection,
    task: &(String, String, i64),
    level: Level,
    redactions: &mut usize,
) -> Result<Value> {
    let session = SessionRef {
        provider: task.0.clone(),
        session_id: task.1.clone(),
        repo_root: None,
    };
    let turns = load_turns(conn, &task.0, &task.1)?;
    let info = load_tasks(conn, &session)?
        .into_iter()
        .find(|t| t.index == task.2);
    let Some(info) = info else {
        return Ok(json!({"missing": true}));
    };
    let built = task_state(
        &info.harness,
        &turns,
        &info.turns,
        info.commits,
        info.subagents,
        Level::EvidenceOnly,
    );
    let mut side = json!({"turns": info.turns.len(), "evidence": built.state["evidence"]});
    if level > Level::EvidenceOnly
        && let Some(first) = turns.iter().find(|turn| info.turns.contains(&turn.index))
    {
        let (text, count) = redact(&head_tail(&first.user, 600));
        *redactions += count;
        side["first_request"] = Value::String(text);
    }
    Ok(side)
}

fn cache_key(model: &str, level: Level, questions: &Map<String, Value>, state: &Value) -> String {
    format!(
        "{:x}",
        md5::compute(format!(
            "{QUESTION_SET_VERSION}|{model}|{}|{}|{}",
            level.as_str(),
            Value::Object(questions.clone()),
            state
        ))
    )
}

fn task_request_text(turn: &TurnFacts) -> String {
    redact(&head_tail(&turn.user, 200)).0.replace('\n', " ")
}

fn post_skill_actions(
    conn: &Connection,
    provider: &str,
    session_id: &str,
    task_index: i64,
    after: Option<&str>,
) -> Result<Vec<Value>> {
    let Some(after) = after else {
        return Ok(Vec::new());
    };
    let end: Option<String> = conn
        .query_row(
            "SELECT COALESCE(completed_at, last_activity_at) FROM derived_task
         WHERE provider = ?1 AND session_id = ?2 AND task_index = ?3",
            params![provider, session_id, task_index],
            |row| row.get(0),
        )
        .unwrap_or(None);
    let mut stmt = conn.prepare(
        "SELECT tool_name, tool_category, status, validation_kind FROM fact_session_tool_call
         WHERE provider = ?1 AND session_id = ?2 AND ts >= ?3
           AND (?4 IS NULL OR ts <= ?4) ORDER BY ts, call_index LIMIT 100",
    )?;
    let rows = stmt.query_map(params![provider, session_id, after, end], |row| {
        Ok(json!({
            "tool": row.get::<_, String>(0)?,
            "category": row.get::<_, String>(1)?,
            "status": row.get::<_, String>(2)?,
            "validation": row.get::<_, Option<String>>(3)?,
        }))
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

impl Runner<'_> {
    fn stopped(&self) -> bool {
        self.summary.stopped.is_some()
    }

    /// Ask one unit's questions, from cache when the content is unchanged.
    fn ask(&mut self, unit_kind: &str, unit_key: &str, build: &Builder) -> Result<Option<Asked>> {
        *self.summary.units.entry(unit_kind.to_string()).or_default() += 1;
        let mut level = self.level;
        let mut degraded = false;
        loop {
            let (built, questions) = build(level);
            let key = cache_key(&self.config.model, built.level, &questions, &built.state);
            if let Some(cached) = self.cached(unit_kind, unit_key, &key, &questions)? {
                if self.prefetched.remove(&key) {
                    *self.summary.units.entry(unit_kind.into()).or_default() -= 1;
                } else {
                    self.summary.cached += 1;
                }
                self.record_job_unit(unit_kind, unit_key, &key, "completed", None)?;
                return Ok(Some(cached));
            }
            if self.prefetched.remove(&key) {
                *self.summary.units.entry(unit_kind.into()).or_default() -= 1;
                return Ok(None);
            }
            if !self.prepaid {
                if self.deadline.is_some_and(|d| Instant::now() >= d)
                    && self.summary.stopped.is_none()
                {
                    self.summary.stopped = Some(
                        "dispatch time budget reached; resume the saved classification job".into(),
                    );
                }
                if self.stopped() {
                    return Ok(None);
                }
            }
            let request = DecisionRequest {
                model: self.config.model.clone(),
                state: built.state.clone(),
                questions: questions.clone(),
                provider: self.config.provider.clone(),
            };
            let tokens = estimate_tokens(&request.body());
            self.summary.redactions += built.redactions;
            let Some(client) = self.client else {
                self.summary.est_tokens += tokens;
                return Ok(None);
            };
            let est_usd = self.config.estimate_usd(tokens);
            let parts: Vec<_> = unit_key.split('|').collect();
            let project: Option<String> = if unit_kind == "pr" {
                let repo: Option<String> = self
                    .conn
                    .query_row(
                        "SELECT repo_key FROM pull_request_record WHERE id=?1",
                        [unit_key],
                        |r| r.get(0),
                    )
                    .optional()?;
                if let Some(repo) = repo {
                    Some(
                        crate::delivery_sources::sources(self.conn)?
                            .into_iter()
                            .find(|s| s.repo_key() == repo)
                            .map(|s| s.repo_root)
                            .unwrap_or(repo),
                    )
                } else {
                    None
                }
            } else if parts.len() >= 2 {
                self.conn.query_row("SELECT COALESCE(r.repo_root,?3) FROM metadata_sessions s LEFT JOIN metadata_repositories r ON r.id=s.repository_id WHERE s.provider=?1 AND s.session_id=?2", params![parts[0],parts[1],super::automatic::UNATTRIBUTED_PROJECT], |r|r.get(0)).ok()
            } else {
                None
            };
            if !self.prepaid
                && let Some(repo) = &project
            {
                let cap = super::automatic::project_cap(
                    self.conn,
                    repo,
                    self.config.project_monthly_budget_usd,
                )?;
                if crate::classify::automatic::project_spend(self.conn, repo)? + est_usd > cap {
                    self.summary.stopped = Some(format!("project budget reached: {repo}"));
                    return Ok(None);
                }
            }
            if !self.prepaid && self.summary.cost_usd + est_usd > self.budget {
                self.summary.stopped = Some(format!(
                    "budget reached (${:.4} spent of ${:.4})",
                    self.summary.cost_usd, self.budget
                ));
                return Ok(None);
            }
            let payload_digest = format!("{:x}", md5::compute(request.body().to_string()));
            let degraded_level =
                (degraded || built.level != self.level).then(|| built.level.as_str());
            self.prepaid = false;
            match client.decide(&request) {
                Ok(response) => {
                    let response_transaction = ResponseTransaction::new(self.conn)?;
                    let cost = response
                        .cost_usd
                        .filter(|c| c.is_finite() && *c >= 0.0)
                        .unwrap_or(est_usd);
                    self.summary.sent += 1;
                    self.summary.cost_usd += cost;
                    if let Some(job) = self.job_id {
                        self.conn.execute("UPDATE classification_job SET spent_usd=spent_usd+?2,updated_at=datetime('now') WHERE id=?1",params![job,cost])?;
                    }
                    if self.summary.cost_usd > self.budget {
                        self.summary.stopped = Some(format!(
                            "Reported cost exceeded dispatch estimate: ${:.4} spent, ${:.4} run limit",
                            self.summary.cost_usd, self.budget
                        ));
                    }
                    if let Some(run_id) = self.run_id {
                        self.conn.execute(
                            "UPDATE classifier_run SET cost_usd=?2,units_sent=?3 WHERE run_id=?1",
                            params![run_id, self.summary.cost_usd, self.summary.sent as i64],
                        )?;
                    }
                    self.summary.input_tokens += response.input_tokens.unwrap_or(tokens as i64);
                    if degraded_level.is_some() {
                        self.summary.degraded += 1;
                    }
                    let mut answers = BTreeMap::new();
                    let mut invalid = 0;
                    for (name, question) in &questions {
                        match response.answers.get(name) {
                            Some(answer) if answer_valid(question, answer) => {
                                let answer_band = band(question, answer);
                                self.store_result(
                                    unit_kind,
                                    unit_key,
                                    name,
                                    answer,
                                    answer_band,
                                    &key,
                                )?;
                                answers.insert(name.clone(), (answer.clone(), answer_band));
                            }
                            _ => invalid += 1,
                        }
                    }
                    self.summary.invalid_answers += invalid;
                    self.record_request(
                        unit_kind,
                        unit_key,
                        &key,
                        &payload_digest,
                        tokens,
                        response.input_tokens,
                        Some(cost),
                        Some(response.http_status),
                        if invalid > 0 { "invalid_answer" } else { "ok" },
                        response.attempts,
                        degraded_level,
                        built.redactions,
                        None,
                    )?;
                    let manifest = input_manifest(&built.state, built.level);
                    self.conn.execute("UPDATE classifier_request SET input_manifest_json=?1 WHERE request_id=last_insert_rowid()",[manifest.to_string()])?;
                    self.conn.execute("UPDATE classification_result SET evidence_json=?1 WHERE unit_kind=?2 AND unit_key=?3 AND cache_key=?4 AND source='jev'", params![json!({"input_refs":manifest["source_refs"],"supports":[],"contradicts":[],"citation_status":"uncited"}).to_string(),unit_kind,unit_key,key])?;
                    if let Some(repo) = &project {
                        self.conn.execute("INSERT OR IGNORE INTO classifier_project_spend(request_id,repo_root,at,usd) VALUES(?1,?2,datetime('now'),?3)",params![self.conn.last_insert_rowid(),repo,cost])?;
                    }
                    response_transaction.commit()?;
                    return Ok(Some(Asked { answers }));
                }
                Err(error) => {
                    let status = match error.kind {
                        ErrorKind::Transient => "failed_transient",
                        ErrorKind::TooLarge => "too_large",
                        ErrorKind::InvalidRequest => "invalid_request",
                        ErrorKind::Auth | ErrorKind::Credits => "aborted",
                    };
                    self.record_request(
                        unit_kind,
                        unit_key,
                        &key,
                        &payload_digest,
                        tokens,
                        None,
                        None,
                        error.status,
                        status,
                        error.attempts,
                        degraded_level,
                        built.redactions,
                        Some(&error.message),
                    )?;
                    match error.kind {
                        ErrorKind::TooLarge if !degraded => {
                            if let Some(lower) = built.level.lower() {
                                level = lower;
                                degraded = true;
                                continue;
                            }
                            self.summary.failed += 1;
                        }
                        ErrorKind::Auth => {
                            self.summary.stopped =
                                Some(format!("aborted: check the OpenRouter key ({error})"));
                        }
                        ErrorKind::Credits => {
                            self.summary.stopped =
                                Some(format!("aborted: insufficient credits ({error})"));
                        }
                        _ => self.summary.failed += 1,
                    }
                    return Ok(None);
                }
            }
        }
    }

    fn cached(
        &self,
        unit_kind: &str,
        unit_key: &str,
        key: &str,
        questions: &Map<String, Value>,
    ) -> Result<Option<Asked>> {
        let mut stmt = self.conn.prepare(
            "SELECT question_id, label, score, probability, probabilities_json, confidence, band
             FROM classification_result
             WHERE source = 'jev' AND unit_kind = ?1 AND unit_key = ?2 AND cache_key = ?3",
        )?;
        let rows = stmt
            .query_map(params![unit_kind, unit_key, key], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    Answer {
                        label: row.get(1)?,
                        score: row.get(2)?,
                        probability: row.get(3)?,
                        probabilities: row
                            .get::<_, Option<String>>(4)?
                            .and_then(|text| serde_json::from_str(&text).ok()),
                        confidence: row.get(5)?,
                    },
                    row.get::<_, String>(6)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if rows.is_empty() {
            return Ok(None);
        }
        let mut answers = BTreeMap::new();
        for (question, answer, stored_band) in rows {
            if questions.contains_key(&question) {
                let fixed: &'static str = match stored_band.as_str() {
                    "confident" => "confident",
                    "tentative" => "tentative",
                    _ => "uncertain",
                };
                answers.insert(question, (answer, fixed));
            }
        }
        // Make the newest cached result current again (it may have been
        // superseded by a later, since-reverted content change).
        self.conn.execute(
            "UPDATE classification_result SET superseded_by = NULL
             WHERE source = 'jev' AND unit_kind = ?1 AND unit_key = ?2 AND cache_key = ?3",
            params![unit_kind, unit_key, key],
        )?;
        self.conn.execute(
            "UPDATE classification_result SET superseded_by = -1
             WHERE source = 'jev' AND unit_kind = ?1 AND unit_key = ?2 AND cache_key <> ?3
               AND superseded_by IS NULL",
            params![unit_kind, unit_key, key],
        )?;
        Ok(Some(Asked { answers }))
    }

    fn store_result(
        &self,
        unit_kind: &str,
        unit_key: &str,
        question: &str,
        answer: &Answer,
        answer_band: &str,
        key: &str,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT OR IGNORE INTO classification_result (
                unit_kind, unit_key, question_id, question_version, source, label, score,
                probability, probabilities_json, confidence, band, model_version, cache_key, run_id
             ) VALUES (?1, ?2, ?3, ?4, 'jev', ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                unit_kind,
                unit_key,
                question,
                if question == "skill_follow_through" {
                    SKILL_QUESTION_VERSION
                } else {
                    QUESTION_SET_VERSION
                },
                answer.label,
                answer.score,
                answer.probability,
                answer.probabilities.as_ref().map(Value::to_string),
                answer.confidence,
                answer_band,
                self.config.model,
                key,
                self.run_id
            ],
        )?;
        let id: i64 = self.conn.query_row(
            "SELECT id FROM classification_result
             WHERE source = 'jev' AND unit_kind = ?1 AND unit_key = ?2 AND question_id = ?3 AND cache_key = ?4",
            params![unit_kind, unit_key, question, key],
            |row| row.get(0),
        )?;
        self.conn.execute(
            "UPDATE classification_result SET superseded_by = ?5
             WHERE source = 'jev' AND unit_kind = ?1 AND unit_key = ?2 AND question_id = ?3
               AND cache_key <> ?4",
            params![unit_kind, unit_key, question, key, id],
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn record_request(
        &self,
        unit_kind: &str,
        unit_key: &str,
        key: &str,
        payload_digest: &str,
        est_tokens: usize,
        input_tokens: Option<i64>,
        cost: Option<f64>,
        http_status: Option<u16>,
        status: &str,
        attempts: u32,
        degraded_level: Option<&str>,
        redactions: usize,
        error: Option<&str>,
    ) -> Result<()> {
        let Some(run_id) = self.run_id else {
            return Ok(());
        };
        self.conn.execute(
            "INSERT INTO classifier_request (
                run_id, unit_kind, unit_key, cache_key, payload_sha256, est_tokens, input_tokens,
                cost_usd, http_status, status, attempts, degraded_level, redactions, error
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                run_id,
                unit_kind,
                unit_key,
                key,
                payload_digest,
                est_tokens as i64,
                input_tokens,
                cost,
                http_status.map(i64::from),
                status,
                attempts as i64,
                degraded_level,
                redactions as i64,
                error
            ],
        )?;
        self.record_job_unit(
            unit_kind,
            unit_key,
            key,
            if status == "ok" {
                "completed"
            } else {
                "pending"
            },
            error,
        )?;
        Ok(())
    }

    // ── Passes ───────────────────────────────────────────────────────────

    /// Assign every user turn to a task. Returns whether stored assignments changed.
    /// Per user turn, one request with the task-assignment question (when
    /// `segment`) and the stage, request-kind and flag questions (when
    /// `stages`). Returns whether stored task assignments changed.
    fn segment_session(
        &mut self,
        session: &SessionRef,
        turns: &[TurnFacts],
        segment: bool,
        stages: bool,
    ) -> Result<bool> {
        let overrides: BTreeMap<i64, i64> = {
            let mut stmt = self.conn.prepare(
                "SELECT turn_index, task_index FROM task_turn_assignment
                 WHERE provider = ?1 AND session_id = ?2 AND source = 'override'",
            )?;
            stmt.query_map(params![session.provider, session.session_id], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<rusqlite::Result<_>>()?
        };
        let mut tasks: Vec<(i64, String)> = Vec::new();
        let mut assigned: Vec<(i64, Option<f64>)> = Vec::with_capacity(turns.len());
        let mut answered = 0usize;
        for (position, turn) in turns.iter().enumerate() {
            let key = turn_unit_key(&session.provider, &session.session_id, turn.index);
            let fixed = if let Some(task) = overrides.get(&turn.index) {
                if !tasks.iter().any(|(index, _)| index == task) {
                    tasks.push((*task, task_request_text(turn)));
                }
                Some((*task, Some(1.0)))
            } else if position == 0 || !segment {
                if position == 0 {
                    tasks.push((0, task_request_text(turn)));
                }
                let previous = if position == 0 {
                    0
                } else {
                    assigned[position - 1].0
                };
                Some((previous, None))
            } else {
                None
            };
            if let Some(entry) = fixed {
                if stages {
                    let build = |level: Level| {
                        (
                            assignment_state(turns, position, level),
                            turn_questions(level),
                        )
                    };
                    self.ask("turn", &key, &build)?;
                }
                assigned.push(entry);
                continue;
            }
            let current = assigned[position - 1].0;
            let current_request = tasks
                .iter()
                .find(|(index, _)| *index == current)
                .map(|(_, request)| request.clone())
                .unwrap_or_default();
            let open: Vec<(i64, String)> = tasks
                .iter()
                .filter(|(index, _)| *index != current)
                .map(|(index, request)| (index + 1, request.clone()))
                .collect();
            let current_label = format!("T{}", current + 1);
            let build = |level: Level| {
                let mut built = assignment_state(turns, position, level);
                if turn.events_before.iter().any(|event| event == "clear") {
                    built.state["note"] =
                        Value::String("The user cleared the context before this message.".into());
                }
                let mut questions =
                    task_assignment_question(Some((&current_label, &current_request)), &open);
                if stages {
                    questions.extend(turn_questions(level));
                }
                (built, questions)
            };
            let asked = self.ask("turn", &key, &build)?;
            let mut choice = (current, None);
            if let Some(asked) = asked
                && let Some((answer, answer_band)) = asked.answers.get("task_assignment")
            {
                answered += 1;
                if matches!(*answer_band, "confident" | "tentative") {
                    let label = answer.label.as_deref().unwrap_or("continue");
                    let task = if label == "new_task" {
                        let next = tasks.iter().map(|(index, _)| *index).max().unwrap_or(-1) + 1;
                        tasks.push((next, task_request_text(turn)));
                        next
                    } else if let Some(number) = label
                        .strip_prefix("resume_T")
                        .and_then(|n| n.parse::<i64>().ok())
                        && tasks.iter().any(|(index, _)| *index == number - 1)
                    {
                        number - 1
                    } else {
                        current
                    };
                    choice = (task, answer.confidence);
                }
            }
            assigned.push(choice);
        }
        if self.client.is_none() || answered == 0 {
            return Ok(false);
        }

        // A one-turn task between two turns of the same task is an aside.
        for position in 1..assigned.len().saturating_sub(1) {
            let task = assigned[position].0;
            let single = assigned.iter().filter(|(other, _)| *other == task).count() == 1;
            if single
                && assigned[position - 1].0 == assigned[position + 1].0
                && !overrides.contains_key(&turns[position].index)
            {
                assigned[position].0 = assigned[position - 1].0;
            }
        }
        // Number tasks by first appearance unless overrides pin indices.
        if overrides.is_empty() {
            let mut order: Vec<i64> = Vec::new();
            for (task, _) in &assigned {
                if !order.contains(task) {
                    order.push(*task);
                }
            }
            for entry in &mut assigned {
                entry.0 = order.iter().position(|task| *task == entry.0).unwrap_or(0) as i64;
            }
        }
        let task_count = {
            let mut distinct: Vec<i64> = assigned.iter().map(|(task, _)| *task).collect();
            distinct.sort_unstable();
            distinct.dedup();
            distinct.len()
        };
        if task_count > 1 {
            self.summary.tasks_split += 1;
        }

        let previous: Vec<(i64, i64)> = {
            let mut stmt = self.conn.prepare(
                "SELECT turn_index, task_index FROM task_turn_assignment
                 WHERE provider = ?1 AND session_id = ?2 AND source = 'jev' ORDER BY turn_index",
            )?;
            stmt.query_map(params![session.provider, session.session_id], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<rusqlite::Result<_>>()?
        };
        let next: Vec<(i64, i64)> = turns
            .iter()
            .zip(&assigned)
            .map(|(turn, (task, _))| (turn.index, *task))
            .collect();
        if previous == next {
            return Ok(false);
        }
        self.conn.execute(
            "DELETE FROM task_turn_assignment WHERE provider = ?1 AND session_id = ?2 AND source = 'jev'",
            params![session.provider, session.session_id],
        )?;
        // Task indices moved, so completion marks for the old numbering are void.
        self.conn.execute(
            "DELETE FROM task_completion_mark WHERE provider = ?1 AND session_id = ?2 AND source = 'jev'",
            params![session.provider, session.session_id],
        )?;
        for (turn, (task, confidence)) in turns.iter().zip(&assigned) {
            self.conn.execute(
                "INSERT INTO task_turn_assignment(provider, session_id, turn_index, task_index, source, confidence)
                 VALUES (?1, ?2, ?3, ?4, 'jev', ?5)",
                params![session.provider, session.session_id, turn.index, task, confidence],
            )?;
        }
        Ok(true)
    }

    fn feedback_session(&mut self, session: &SessionRef, turns: &[TurnFacts]) -> Result<()> {
        for position in 1..turns.len() {
            let build =
                |level: Level| (exchange_state(turns, position, level), exchange_questions());
            let key = turn_unit_key(
                &session.provider,
                &session.session_id,
                turns[position].index,
            );
            self.ask("exchange", &key, &build)?;
        }
        Ok(())
    }

    fn label_task(
        &mut self,
        session: &SessionRef,
        turns: &[TurnFacts],
        task: &TaskRef,
    ) -> Result<()> {
        let build = |level: Level| {
            let built = task_state(
                &task.harness,
                turns,
                &task.turns,
                task.commits,
                task.subagents,
                level,
            );
            let questions = task_label_questions(built.level);
            (built, questions)
        };
        let key = task_unit_key(&session.provider, &session.session_id, task.index);
        self.ask("task", &key, &build)?;
        Ok(())
    }

    fn classify_skills(
        &mut self,
        session: &SessionRef,
        turns: &[TurnFacts],
        task: &TaskRef,
    ) -> Result<()> {
        let Some(repo_root) = session.repo_root.as_deref() else {
            return Ok(());
        };
        for event in crate::explore::skills::session_skill_events(
            self.conn,
            &session.provider,
            &session.session_id,
        )? {
            if event.task_index != Some(task.index)
                || event.kind == "listed"
                || (event.kind == "call" && event.status != "ok")
            {
                continue;
            }
            let Some(instructions) =
                crate::explore::skills::local_skill_instructions(repo_root, &event.skill_id)
            else {
                continue;
            };
            let start = event
                .ts
                .as_deref()
                .and_then(crate::insights::structure::parse_ts);
            let first_turn = turns
                .iter()
                .filter(|turn| task.turns.contains(&turn.index))
                .filter(|turn| start.is_none_or(|start| turn.ts.is_some_and(|ts| ts <= start)))
                .map(|turn| turn.index)
                .max();
            let after: Vec<TurnFacts> = turns
                .iter()
                .filter(|turn| {
                    task.turns.contains(&turn.index)
                        && first_turn.is_none_or(|first| turn.index >= first)
                })
                .cloned()
                .collect();
            if after.is_empty() {
                continue;
            }
            let turn_ids: Vec<i64> = after.iter().map(|turn| turn.index).collect();
            let post_actions = post_skill_actions(
                self.conn,
                &session.provider,
                &session.session_id,
                task.index,
                event.ts.as_deref(),
            )?;
            let skill_id = event.skill_id.clone();
            let build = |level: Level| {
                let mut built = task_state(
                    &task.harness,
                    &after,
                    &turn_ids,
                    task.commits,
                    task.subagents,
                    level,
                );
                let (redacted, count) = redact(&head_tail(&instructions, 12_000));
                built.redactions += count;
                built.state["skill"] = json!({
                    "id": skill_id,
                    "instructions": redacted,
                    "current_file_fingerprint": format!("{:x}", md5::compute(instructions.as_bytes())),
                    "historical_version_verified": false,
                });
                built.state["post_invocation_tools"] = Value::Array(post_actions.clone());
                (built, skill_follow_through_question())
            };
            let key = format!("{}|{}|{}", session.provider, session.session_id, event.key);
            self.ask("skill", &key, &build)?;
        }
        Ok(())
    }

    fn complete_task(
        &mut self,
        session: &SessionRef,
        turns: &[TurnFacts],
        task: &TaskRef,
    ) -> Result<()> {
        let task_turns: Vec<&TurnFacts> = turns
            .iter()
            .filter(|turn| task.turns.contains(&turn.index))
            .collect();
        if task_turns.is_empty() {
            return Ok(());
        }
        let skip = task_turns.len().saturating_sub(MAX_COMPLETION_OPTIONS);
        let build = |level: Level| {
            let mut built = task_state(
                &task.harness,
                turns,
                &task.turns,
                task.commits,
                task.subagents,
                level,
            );
            let mut redactions = 0;
            let options: Vec<(i64, String)> = task_turns[skip..]
                .iter()
                .map(|turn| (turn.index, turn_option(turn, built.level, &mut redactions)))
                .collect();
            built.redactions += redactions;
            (built, completion_question(&options))
        };
        let key = task_unit_key(&session.provider, &session.session_id, task.index);
        let Some(asked) = self.ask("task_completion", &key, &build)? else {
            return Ok(());
        };
        let Some((answer, answer_band)) = asked.answers.get("completion_turn") else {
            return Ok(());
        };
        if !matches!(*answer_band, "confident" | "tentative") {
            return Ok(());
        }
        let mut mark = match answer.label.as_deref() {
            Some("not_completed") => None,
            Some(label) => match label
                .strip_prefix("turn_")
                .and_then(|n| n.parse::<i64>().ok())
            {
                Some(turn) => Some(turn),
                None => return Ok(()),
            },
            None => return Ok(()),
        };
        // Reopen: a defect report after the chosen turn means it was not done.
        if let Some(turn) = mark
            && let Some(reopened_at) = self.defect_report_after(session, &task.turns, turn)?
        {
            let _ = reopened_at;
            mark = None; // A new supported decision is required after a correction.
        }
        if self.client.is_none() {
            return Ok(());
        }
        let prior:bool=self.conn.query_row("SELECT EXISTS(SELECT 1 FROM task_completion_mark WHERE provider=?1 AND session_id=?2 AND task_index=?3 AND completion_turn IS NOT NULL)",params![session.provider,session.session_id,task.index],|r|r.get(0))?;
        self.conn.execute(
            "INSERT OR REPLACE INTO task_completion_mark(provider, session_id, task_index, completion_turn, source, confidence)
             VALUES (?1, ?2, ?3, ?4, 'jev', ?5)",
            params![session.provider, session.session_id, task.index, mark, answer.confidence],
        )?;
        let member: Option<String> = self.conn.query_row("SELECT identity_id FROM derived_work_member WHERE provider=?1 AND session_id=?2 AND task_index=?3",params![session.provider,session.session_id,task.index],|r|r.get(0)).optional()?;
        if let Some(member) = member {
            let result_id:Option<i64>=self.conn.query_row("SELECT id FROM classification_result WHERE unit_kind='task_completion' AND unit_key=?1 AND question_id='completion_turn' AND source='jev' AND superseded_by IS NULL ORDER BY id DESC LIMIT 1",[&key],|r|r.get(0)).optional()?;
            let evidence = result_id
                .into_iter()
                .map(|id| crate::insights::audit::Reference::Classification { id })
                .collect();
            let outcome:Option<String>=self.conn.query_row("SELECT label FROM view_label_effective WHERE unit_kind='task' AND unit_key=?1 AND question_id='outcome'",[&key],|r|r.get(0)).optional()?;
            let supported = matches!(
                outcome.as_deref(),
                Some("completed_verified" | "completed_unverified" | "answered")
            );
            // Completion is observed at the end of the selected turn, not at
            // the request that started it. Unknown timestamps stay unknown.
            let at = if let Some(turn) = mark.filter(|_| supported) {
                self.conn.query_row("SELECT ended_at FROM derived_turn WHERE provider=?1 AND session_id=?2 AND turn_index=?3",params![session.provider,session.session_id,turn],|r|r.get::<_,Option<String>>(0)).optional()?.flatten().as_deref().and_then(crate::insights::structure::parse_ts)
            } else {
                None
            };
            crate::insights::audit::record_completion(
                self.conn,
                &member,
                "classifier",
                at,
                outcome.as_deref() == Some("completed_verified"),
                evidence,
                "classifier reassessment: no supported completion",
            )?;
        }
        self.summary.completions += 1;
        self.summary.completion_decisions += 1;
        self.summary.new_completions += usize::from(mark.is_some() && !prior);
        Ok(())
    }

    fn classify_work_links(&mut self, selected: &[SessionRef]) -> Result<()> {
        use crate::insights::work;
        let items = work::items(self.conn)?;
        let members: BTreeMap<_, _> = items
            .iter()
            .flat_map(|w| w.members.iter())
            .map(|m| (m.id.clone(), m))
            .collect();
        for relation in work::relations(self.conn)?.into_iter().filter(|r| {
            r.decision == "suggested" && r.source != "manual" && r.review_reason.is_none()
        }) {
            let (Some(from), Some(to)) =
                (members.get(&relation.from_id), members.get(&relation.to_id))
            else {
                continue;
            };
            if !selected
                .iter()
                .any(|s| s.provider == to.task.provider && s.session_id == to.task.session_id)
                || !self.config.project_allowed(from.task.repo_root.as_deref())
            {
                continue;
            }
            let a = (
                from.task.provider.clone(),
                from.task.session_id.clone(),
                from.task.task_index,
            );
            let b = (
                to.task.provider.clone(),
                to.task.session_id.clone(),
                to.task.task_index,
            );
            let conn = self.conn;
            let build = |level: Level| {
                let mut redactions = 0;
                let earlier = task_side(conn, &a, level, &mut redactions).unwrap_or(Value::Null);
                let later = task_side(conn, &b, level, &mut redactions).unwrap_or(Value::Null);
                let state = json!({"unit":"work_relation","earlier_task":earlier,"later_task":later,"candidate_evidence":relation.evidence});
                let est_tokens = estimate_tokens(&state);
                (
                    BuiltState {
                        state,
                        level,
                        redactions,
                        est_tokens,
                    },
                    crate::classify::questions::work_relationship_question(),
                )
            };
            let key = format!(
                "{}|{}|work:{}",
                to.task.provider, to.task.session_id, relation.id
            );
            if let Some(answer) = self
                .ask("work_relation", &key, &build)?
                .and_then(|a| a.answers.get("work_relationship").cloned())
                && let Some(kind) = answer.0.label.as_deref().filter(|s| *s != "unknown")
            {
                work::insert_relation(
                    self.conn,
                    &relation.from_id,
                    &relation.to_id,
                    kind,
                    if kind == "unrelated" && answer.1 == "confident" {
                        "rejected"
                    } else {
                        "suggested"
                    },
                    "classifier",
                    &relation.evidence,
                    answer.0.confidence,
                )?;
            }
        }
        Ok(())
    }

    fn classify_relation(&mut self, relation: &RelationRef) -> Result<()> {
        let conn = self.conn;
        let feedback: Vec<String> = {
            let prefix = format!("{}|{}|", relation.to.0, relation.to.1);
            let mut stmt = conn.prepare(
                "SELECT label FROM view_label_effective
                 WHERE unit_kind = 'exchange' AND question_id = 'feedback' AND source <> 'heuristic'
                   AND substr(unit_key, 1, length(?1)) = ?1",
            )?;
            stmt.query_map(params![prefix], |row| row.get(0))?
                .collect::<rusqlite::Result<_>>()?
        };
        let build = |level: Level| {
            let mut redactions = 0;
            let earlier =
                task_side(conn, &relation.from, level, &mut redactions).unwrap_or(Value::Null);
            let mut later =
                task_side(conn, &relation.to, level, &mut redactions).unwrap_or(Value::Null);
            if !feedback.is_empty() {
                later["prior_feedback_labels"] = json!(feedback);
            }
            let state = json!({
                "unit": "relation",
                "earlier_task": earlier,
                "later_task": later,
                "hours_between": relation.gap_hours.map(|h| h.round()),
                "overlap": {"files": relation.files, "lines_changed": relation.lines},
            });
            let est_tokens = estimate_tokens(&state);
            (
                BuiltState {
                    state,
                    level,
                    redactions,
                    est_tokens,
                },
                rework_question(),
            )
        };
        let key = crate::insights::relations::relation_unit_key(&relation.from, &relation.to);
        self.ask("relation", &key, &build)?;
        Ok(())
    }

    fn defect_report_after(
        &self,
        session: &SessionRef,
        task_turns: &[i64],
        after: i64,
    ) -> Result<Option<i64>> {
        for turn in task_turns.iter().filter(|turn| **turn > after) {
            let key = turn_unit_key(&session.provider, &session.session_id, *turn);
            let label: Option<String> = self
                .conn
                .query_row(
                    "SELECT label FROM view_label_effective
                     WHERE unit_kind = 'exchange' AND unit_key = ?1 AND question_id = 'feedback'
                       AND source IN ('jev', 'override')",
                    params![key],
                    |row| row.get(0),
                )
                .optional()?
                .flatten();
            if label.as_deref() == Some("correct_defect") {
                return Ok(Some(*turn));
            }
        }
        Ok(None)
    }
}

/// The exact request a pass would send for one unit.
pub fn preview(
    conn: &Connection,
    config: &ClassifierConfig,
    session: &SessionRef,
    pass: &str,
    unit: Option<i64>,
    level: Level,
) -> Result<Value> {
    let turns = load_turns(conn, &session.provider, &session.session_id)?;
    if turns.is_empty() {
        anyhow::bail!("session has no user turns");
    }
    let (built, questions, unit_key) = match pass {
        "segment" | "feedback" => {
            let position = unit.unwrap_or(1).clamp(1, turns.len().max(2) as i64 - 1) as usize;
            if position >= turns.len() {
                anyhow::bail!("session has a single turn; nothing to segment");
            }
            let key = turn_unit_key(&session.provider, &session.session_id, position as i64);
            if pass == "segment" {
                let first = (0, task_request_text(&turns[0]));
                let built = assignment_state(&turns, position, level);
                (
                    built,
                    task_assignment_question(Some(("T1", &first.1)), &[]),
                    key,
                )
            } else {
                (
                    exchange_state(&turns, position, level),
                    exchange_questions(),
                    key,
                )
            }
        }
        "labels" | "completion" => {
            let tasks = load_tasks(conn, session)?;
            let index = unit.unwrap_or(0);
            let task = tasks
                .iter()
                .find(|task| task.index == index)
                .ok_or_else(|| anyhow::anyhow!("task {index} not found"))?;
            let built = task_state(
                &task.harness,
                &turns,
                &task.turns,
                task.commits,
                task.subagents,
                level,
            );
            let key = task_unit_key(&session.provider, &session.session_id, index);
            let questions = if pass == "labels" {
                task_label_questions(built.level)
            } else {
                let mut redactions = 0;
                let options: Vec<(i64, String)> = turns
                    .iter()
                    .filter(|turn| task.turns.contains(&turn.index))
                    .map(|turn| (turn.index, turn_option(turn, built.level, &mut redactions)))
                    .collect();
                completion_question(&options)
            };
            (built, questions, key)
        }
        other => anyhow::bail!("unknown pass '{other}' (segment, feedback, labels, completion)"),
    };
    let request = DecisionRequest {
        model: config.model.clone(),
        state: built.state.clone(),
        questions,
        provider: config.provider.clone(),
    };
    let body = request.body();
    Ok(json!({
        "unit_key": unit_key,
        "level": built.level.as_str(),
        "redactions": built.redactions,
        "est_tokens": estimate_tokens(&body),
        "endpoint": config.endpoint,
        "request": body,
    }))
}

fn input_manifest(state: &Value, level: Level) -> Value {
    fn walk(value: &Value, refs: &mut Vec<Value>, omitted: &mut Vec<Value>) {
        match value {
            Value::Object(map) => {
                for (key, value) in map {
                    if key == "source_refs" {
                        if let Some(items) = value.as_array() {
                            for item in items {
                                if !refs.contains(item) {
                                    refs.push(item.clone());
                                }
                            }
                        }
                    } else if key == "omitted" {
                        omitted.push(value.clone());
                    } else {
                        walk(value, refs, omitted);
                    }
                }
            }
            Value::Array(items) => {
                for item in items {
                    walk(item, refs, omitted);
                }
            }
            _ => {}
        }
    }
    let (mut refs, mut omitted) = (vec![], vec![]);
    walk(state, &mut refs, &mut omitted);
    json!({"normalizer_version":super::request::VERSION,"question_version":QUESTION_SET_VERSION,"content_level":level.as_str(),"source_refs":refs,"omitted_ranges":omitted})
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::classify::client::{ClientError, ScriptedClient, canned_response};
    use crate::db::session_store::{MessageRecord, SessionRecord, store_session_record};

    #[test]
    fn skill_follow_through_state_uses_bounded_structural_actions() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        crate::db::init_app_schema(&conn)?;
        conn.execute(
            "INSERT INTO fact_session_tool_call
             (provider, session_id, call_key, call_index, ts, tool_name, tool_category,
              status, target) VALUES ('claude', 's1', 't1', 0, '2026-10-01T10:00:00Z',
              'Bash', 'exec', 'ok', 'secret=do-not-send')",
            [],
        )?;
        let actions = post_skill_actions(&conn, "claude", "s1", 0, Some("2026-10-01T10:00:00Z"))?;
        assert_eq!(actions.len(), 1);
        assert!(!actions[0].to_string().contains("do-not-send"));
        Ok(())
    }

    fn message(key: &str, ts: &str, role: &str, text: &str) -> MessageRecord {
        MessageRecord {
            key: key.into(),
            ts: Some(ts.into()),
            role: role.into(),
            text: text.into(),
            is_compaction_summary: false,
        }
    }

    /// Session with three user turns: a feature ask, a different goal, and a
    /// return to the first goal.
    fn seeded() -> Result<(Connection, SessionRef)> {
        let conn = Connection::open_in_memory()?;
        crate::db::init_app_schema(&conn)?;
        let mut record = SessionRecord::new("claude", "s1");
        record.started_at = Some("2026-10-01T10:00:00Z".into());
        record.ended_at = Some("2026-10-01T11:00:00Z".into());
        record.messages = vec![
            message(
                "m1",
                "2026-10-01T10:00:00Z",
                "user",
                "Add a --weekly flag to the cost report",
            ),
            message("m2", "2026-10-01T10:01:00Z", "assistant", "Added the flag."),
            message(
                "m3",
                "2026-10-01T10:10:00Z",
                "user",
                "Separate thing: fix the README typo",
            ),
            message("m4", "2026-10-01T10:11:00Z", "assistant", "Fixed."),
            message(
                "m5",
                "2026-10-01T10:20:00Z",
                "user",
                "Back to the weekly flag: it breaks on empty weeks",
            ),
            message(
                "m6",
                "2026-10-01T10:21:00Z",
                "assistant",
                "Handled empty weeks.",
            ),
            message(
                "m7",
                "2026-10-01T10:30:00Z",
                "user",
                "Docs: also explain the flag in USER_GUIDE",
            ),
            message("m8", "2026-10-01T10:31:00Z", "assistant", "Documented."),
        ];
        store_session_record(&conn, &record)?;
        crate::db::session_store::refresh_session_roots(&conn)?;
        crate::insights::refresh(&conn)?;
        Ok((
            conn,
            SessionRef {
                provider: "claude".into(),
                session_id: "s1".into(),
                repo_root: None,
            },
        ))
    }

    fn scripted() -> ScriptedClient {
        ScriptedClient::new(|request| {
            let user = request
                .state
                .get("new_message")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            Ok(canned_response(request, |name, question| match name {
                "task_assignment" if user.starts_with("Separate") => ("new_task".into(), 0.9),
                "task_assignment" if user.starts_with("Back to") => ("resume_T1".into(), 0.8),
                "task_assignment" if user.starts_with("Docs") => ("new_task".into(), 0.7),
                "task_assignment" => ("continue".into(), 0.9),
                "completion_turn" => {
                    let criteria = question["criteria"].as_object().unwrap();
                    let last = criteria
                        .keys()
                        .filter(|k| k.starts_with("turn_"))
                        .max()
                        .unwrap()
                        .clone();
                    (last, 0.8)
                }
                "feedback" if user.is_empty() => ("other".into(), 0.5),
                "feedback" => ("accept_continue".into(), 0.7),
                "frustration" | "framing" | "progress" => ("1".into(), 0.6),
                "intent" => ("feature".into(), 0.8),
                "outcome" => ("completed_unverified".into(), 0.65),
                _ if question["type"] == "noul" => ("false".into(), 0.9),
                _ => {
                    let first = question["criteria"]
                        .as_object()
                        .unwrap()
                        .keys()
                        .next()
                        .unwrap()
                        .clone();
                    (first, 0.5)
                }
            }))
        })
    }

    fn options(max_usd: f64) -> RunOptions {
        RunOptions {
            level: Level::Conversation,
            max_usd,
            passes: Passes::ALL,
            dry_run: false,
            dispatch_seconds: None,
            job_id: None,
        }
    }

    fn config() -> ClassifierConfig {
        ClassifierConfig {
            monthly_budget_usd: 10.0,
            ..ClassifierConfig::default()
        }
    }

    #[test]
    fn fingerprint_scopes_indexed_facts_to_the_conversation_and_subagents() -> Result<()> {
        use crate::db::session_store::{AgentKind, ToolCallRecord, refresh_session_roots};
        let (conn, root) = seeded()?;
        let original = session_fingerprint(&conn, &root)?;
        let mut other = SessionRecord::new("claude", "other");
        other.messages.push(message(
            "other-request",
            "2026-10-01T12:00:00Z",
            "user",
            "Unrelated work",
        ));
        store_session_record(&conn, &other)?;
        refresh_session_roots(&conn)?;
        assert_eq!(session_fingerprint(&conn, &root)?, original);
        let mut child = SessionRecord::new("claude", "child");
        child.parent_session_id = Some("s1".into());
        child.agent_kind = AgentKind::Subagent;
        child.messages.push(message(
            "child-request",
            "2026-10-01T10:01:00Z",
            "user",
            "Check this goal",
        ));
        child.tool_calls.push(ToolCallRecord {
            key: "child-tool".into(),
            tool_name: "Bash".into(),
            target: Some("cargo test".into()),
            status: "ok".into(),
            ..Default::default()
        });
        store_session_record(&conn, &child)?;
        refresh_session_roots(&conn)?;
        let with_child = session_fingerprint(&conn, &root)?;
        assert_ne!(with_child, original);
        conn.execute(
            "UPDATE fact_session_tool_call SET target='cargo build' WHERE session_id='child'",
            [],
        )?;
        assert_ne!(session_fingerprint(&conn, &root)?, with_child);
        for (sql, alias) in SESSION_FINGERPRINT_QUERIES.into_iter().zip(["m", "t"]) {
            let plan = conn
                .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?
                .query_map(params![root.provider, root.session_id], |r| {
                    r.get::<_, String>(3)
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            assert!(
                plan.iter()
                    .any(|detail| detail.starts_with(&format!("SEARCH {alias} "))
                        && detail.contains("provider=? AND session_id=?")),
                "Fact lookups must constrain both conversation keys: {plan:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn segments_session_into_tasks_with_recorded_answers() -> Result<()> {
        let (conn, session) = seeded()?;
        let client = scripted();
        let summary = run(
            &conn,
            &client,
            &config(),
            std::slice::from_ref(&session),
            &options(1.0),
        )?;
        assert!(summary.sent > 0 && summary.stopped.is_none(), "{summary:?}");
        let rows: Vec<(i64, String, String)> = conn
            .prepare(
                "SELECT task_index, turn_list, segmentation_source FROM derived_task
                 WHERE session_id = 's1' ORDER BY task_index",
            )?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
            .collect::<rusqlite::Result<_>>()?;
        assert_eq!(
            rows,
            // The one-turn README detour between two turns of T1 is an aside.
            vec![
                (0, "0,1,2".to_string(), "classified".to_string()),
                (1, "3".to_string(), "classified".to_string()),
            ]
        );
        let completion: (Option<i64>, String) = conn.query_row(
            "SELECT completion_turn, completion_source FROM derived_task WHERE session_id = 's1' AND task_index = 0",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(completion, (Some(2), "jev".to_string()));
        let intent: String = conn.query_row(
            "SELECT label FROM view_label_effective WHERE unit_kind = 'task' AND unit_key = 'claude|s1|0'
               AND question_id = 'intent'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(intent, "feature");
        Ok(())
    }

    #[test]
    fn second_run_is_served_from_cache() -> Result<()> {
        let (conn, session) = seeded()?;
        let client = scripted();
        run(
            &conn,
            &client,
            &config(),
            std::slice::from_ref(&session),
            &options(1.0),
        )?;
        let first = client.calls();
        let summary = run(
            &conn,
            &client,
            &config(),
            std::slice::from_ref(&session),
            &options(1.0),
        )?;
        assert_eq!(client.calls(), first, "no request should be re-sent");
        assert_eq!(summary.sent, 0);
        assert!(summary.cached > 0);
        Ok(())
    }

    #[test]
    fn uncertain_assignment_falls_back_to_continue() -> Result<()> {
        let (conn, session) = seeded()?;
        let client = ScriptedClient::new(|request| {
            Ok(canned_response(request, |name, _| match name {
                "task_assignment" => ("new_task".into(), 0.2),
                _ => ("other".into(), 0.2),
            }))
        });
        let passes = Passes {
            segment: true,
            feedback: false,
            labels: false,
            completion: false,
            rework: false,
            stages: false,
            skills: false,
            prs: false,
        };
        run(
            &conn,
            &client,
            &config(),
            &[session],
            &RunOptions {
                passes,
                ..options(1.0)
            },
        )?;
        let tasks: i64 = conn.query_row(
            "SELECT COUNT(*) FROM derived_task WHERE session_id = 's1'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(tasks, 1);
        Ok(())
    }

    #[test]
    fn stopped_jobs_resume_without_resetting_spend_or_mutating_during_planning() -> Result<()> {
        let (conn, session) = seeded()?;
        let client = scripted();
        let cfg = config();
        let mut opts = options(1.0);
        opts.dispatch_seconds = Some(0);
        let first = run(&conn, &client, &cfg, std::slice::from_ref(&session), &opts)?;
        assert_eq!(first.sent, 0);
        assert!(first.stopped.is_some());
        let id = first.job_id.unwrap();
        conn.execute(
            "UPDATE classification_job SET spent_usd=0.1 WHERE id=?1",
            [id],
        )?;
        let (sessions, resume) = resume_job(&conn, id, &cfg)?;
        assert!((resume.max_usd - 0.9).abs() < 1e-9);
        assert_eq!(
            conn.query_row(
                "SELECT state FROM classification_job WHERE id=?1",
                [id],
                |r| r.get::<_, String>(0)
            )?,
            "pending"
        );
        let result = run(&conn, &client, &cfg, &sessions, &resume)?;
        assert!(result.sent > 0);
        let spent: f64 = conn.query_row(
            "SELECT spent_usd FROM classification_job WHERE id=?1",
            [id],
            |r| r.get(0),
        )?;
        assert!((spent - 0.1 - result.cost_usd).abs() < 1e-9);
        let mut changed = cfg;
        changed.model = "different-policy".into();
        assert!(resume_job(&conn, id, &changed).is_err());
        Ok(())
    }

    #[test]
    fn budget_cap_stops_the_run() -> Result<()> {
        let (conn, session) = seeded()?;
        let client = scripted();
        let summary = run(&conn, &client, &config(), &[session], &options(0.000_000_1))?;
        assert_eq!(client.calls(), 0);
        assert!(
            summary
                .stopped
                .as_deref()
                .unwrap_or("")
                .starts_with("budget")
        );
        Ok(())
    }

    #[test]
    fn auth_failure_aborts_and_keeps_heuristics() -> Result<()> {
        let (conn, session) = seeded()?;
        let client = ScriptedClient::new(|_| {
            Err(ClientError {
                kind: ErrorKind::Auth,
                status: Some(401),
                message: "bad key".into(),
                attempts: 1,
            })
        });
        let summary = run(&conn, &client, &config(), &[session], &options(1.0))?;
        assert_eq!(client.calls(), 1);
        assert!(
            summary
                .stopped
                .as_deref()
                .unwrap_or("")
                .starts_with("aborted")
        );
        let source: String = conn.query_row(
            "SELECT segmentation_source FROM derived_task WHERE session_id = 's1'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(source, "whole_session");
        Ok(())
    }

    #[test]
    fn plan_sends_nothing_and_estimates_tokens() -> Result<()> {
        let (conn, session) = seeded()?;
        let summary = plan(&conn, &config(), &[session], Level::Conversation)?;
        assert!(summary.est_tokens > 0);
        // Every turn gets stage questions; turns after the first also get task assignment.
        assert_eq!(summary.units.get("turn"), Some(&4));
        assert_eq!(summary.units.get("exchange"), Some(&3));
        let runs: i64 =
            conn.query_row("SELECT COUNT(*) FROM classifier_run", [], |row| row.get(0))?;
        assert_eq!(runs, 0);
        Ok(())
    }

    #[test]
    fn completion_reopened_after_defect_report() -> Result<()> {
        let (conn, session) = seeded()?;
        // Single task; Jev says done at turn 1, but turn 2 reports a defect.
        let client = ScriptedClient::new(|request| {
            let user = request
                .state
                .get("user_message")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            Ok(canned_response(request, |name, question| match name {
                "task_assignment" => ("continue".into(), 0.9),
                "feedback" if user.starts_with("Back to") => ("correct_defect".into(), 0.8),
                "feedback" => ("accept_continue".into(), 0.8),
                "completion_turn" => ("turn_1".into(), 0.7),
                _ if question["type"] == "noul" => ("false".into(), 0.9),
                _ if question["type"] == "score" => ("1".into(), 0.6),
                _ => ("other".into(), 0.6),
            }))
        });
        run(&conn, &client, &config(), &[session], &options(1.0))?;
        let mark: Option<i64> = conn.query_row(
            "SELECT completion_turn FROM task_completion_mark WHERE session_id = 's1' AND source = 'jev'",
            [],
            |row| row.get(0),
        )?;
        // canned answers carry no probabilities, so no later turn qualifies.
        assert_eq!(mark, None);
        Ok(())
    }

    #[test]
    fn rework_pass_labels_relations_between_tasks() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        crate::db::init_app_schema(&conn)?;
        for (id, day, ask) in [
            ("early", "01", "Add the parser"),
            ("late", "03", "Fix the parser crash"),
        ] {
            let mut record = SessionRecord::new("claude", id);
            record.started_at = Some(format!("2026-10-{day}T10:00:00Z"));
            record.ended_at = Some(format!("2026-10-{day}T11:00:00Z"));
            record.messages = vec![message(
                "u1",
                &format!("2026-10-{day}T10:00:00Z"),
                "user",
                ask,
            )];
            store_session_record(&conn, &record)?;
        }
        crate::db::session_store::refresh_session_roots(&conn)?;
        for (session, ts, side) in [
            ("early", "2026-10-01T10:05:00Z", "+"),
            ("late", "2026-10-03T10:05:00Z", "-"),
        ] {
            conn.execute(
                "INSERT INTO fact_session_code_change(provider, session_id, change_ts, repo_root, rel_path, source_kind)
                 VALUES ('claude', ?1, ?2, '/repo', 'src/p.rs', 'tool_write')",
                params![session, ts],
            )?;
            let id = conn.last_insert_rowid();
            for hash in ["a", "b", "c", "d", "e", "f"] {
                conn.execute(
                    "INSERT INTO fact_session_code_change_line_hashes(code_change_id, side, line_hash, count)
                     VALUES (?1, ?2, ?3, 1)",
                    params![id, side, hash],
                )?;
            }
        }
        crate::insights::refresh(&conn)?;
        let heuristic: String = conn.query_row(
            "SELECT label FROM view_label_effective WHERE unit_kind = 'relation'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(heuristic, "defect_correction");
        let client = ScriptedClient::new(|request| {
            assert_eq!(request.state["unit"], "relation");
            assert_eq!(
                request.state["earlier_task"]["first_request"],
                "Add the parser"
            );
            Ok(canned_response(request, |_, _| {
                ("requirement_change".into(), 0.8)
            }))
        });
        let passes = Passes {
            segment: false,
            feedback: false,
            labels: false,
            completion: false,
            rework: true,
            stages: false,
            skills: false,
            prs: false,
        };
        let late = SessionRef {
            provider: "claude".into(),
            session_id: "late".into(),
            repo_root: Some("/repo".into()),
        };
        run(
            &conn,
            &client,
            &config(),
            &[late],
            &RunOptions {
                passes,
                ..options(1.0)
            },
        )?;
        assert_eq!(client.calls(), 1);
        let (label, source): (String, String) = conn.query_row(
            "SELECT label, source FROM view_label_effective WHERE unit_kind = 'relation'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(
            (label.as_str(), source.as_str()),
            ("requirement_change", "jev")
        );
        Ok(())
    }

    #[test]
    fn stage_questions_ride_with_task_assignment_and_cover_turn_zero() -> Result<()> {
        let (conn, session) = seeded()?;
        let client = scripted();
        run(
            &conn,
            &client,
            &config(),
            std::slice::from_ref(&session),
            &options(1.0),
        )?;
        let requests = client.requests.lock().unwrap().clone();
        let turn_requests: Vec<_> = requests
            .iter()
            .filter(|r| r.state["unit"] == "user_turn")
            .collect();
        assert_eq!(turn_requests.len(), 4, "one request per user turn");
        let first = turn_requests[0];
        assert!(first.questions.contains_key("turn_stage"));
        assert!(!first.questions.contains_key("task_assignment"));
        assert!(turn_requests[1..].iter().all(|r| {
            r.questions.contains_key("task_assignment")
                && r.questions.contains_key("turn_stage")
                && r.questions.contains_key("request_kind")
                && r.questions.contains_key("unrequested_changes")
        }));
        assert!(first.state.get("current_turn").is_some());
        let stages: i64 = conn.query_row(
            "SELECT COUNT(*) FROM classification_result
             WHERE source = 'jev' AND unit_kind = 'turn' AND question_id = 'turn_stage' AND superseded_by IS NULL",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(stages, 4);
        Ok(())
    }
    #[test]
    fn project_limit_and_reported_overrun_are_persisted() -> Result<()> {
        let (conn, mut session) = seeded()?;
        conn.execute("INSERT INTO metadata_repositories(id,repo_root,repo_name) VALUES(1,'/project','project')",[])?;
        conn.execute("UPDATE metadata_sessions SET repository_id=1", [])?;
        session.repo_root = Some("/project".into());
        crate::classify::automatic::configure_limits(&conn, "/project", 0.00000001, 0.5, 56)?;
        let client = scripted();
        let summary = run(
            &conn,
            &client,
            &config(),
            std::slice::from_ref(&session),
            &options(1.0),
        )?;
        assert_eq!(client.calls(), 0);
        assert!(summary.stopped.unwrap().contains("project budget"));
        crate::classify::automatic::configure_limits(&conn, "/project", 2.0, 0.5, 56)?;
        let client = ScriptedClient::new(|r| {
            let mut response = canned_response(r, |_, q| {
                (
                    q["criteria"]
                        .as_object()
                        .unwrap()
                        .keys()
                        .next()
                        .unwrap()
                        .clone(),
                    0.8,
                )
            });
            response.cost_usd = Some(0.7);
            Ok(response)
        });
        let summary = run(&conn, &client, &config(), &[session], &options(0.5))?;
        assert_eq!(client.calls(), 1);
        assert_eq!(summary.cost_usd, 0.7);
        assert!(summary.stopped.is_some());
        assert_eq!(
            crate::classify::automatic::project_spend(&conn, "/project")?,
            0.7
        );
        assert_eq!(monthly_spend(&conn)?, 0.7);
        Ok(())
    }

    #[test]
    fn simultaneous_classifier_entry_point_cannot_dispatch() -> Result<()> {
        use fs2::FileExt;
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("vca.db");
        let conn = Connection::open(&path)?;
        crate::db::init_app_schema(&conn)?;
        let lock = std::fs::File::create(path.with_extension("classifier.lock"))?;
        lock.try_lock_exclusive()?;
        let client = scripted();
        let error = run(&conn, &client, &config(), &[], &options(0.5)).unwrap_err();
        assert!(error.to_string().contains("Another classification run"));
        assert_eq!(client.calls(), 0);
        drop(lock);
        assert!(run(&conn, &client, &config(), &[], &options(0.5)).is_ok());
        Ok(())
    }
}
