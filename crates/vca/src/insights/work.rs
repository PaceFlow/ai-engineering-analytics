//! Persistent identities and decisions for goals that span conversations.
use super::structure::parse_ts;
use crate::explore::{Filters, TaskRow, TaskSort};
use anyhow::{Result, bail};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS work_identity (
 id TEXT PRIMARY KEY, provider TEXT NOT NULL, session_id TEXT NOT NULL,
 anchor TEXT NOT NULL, messages_json TEXT NOT NULL, review_reason TEXT,
 UNIQUE(provider,session_id,anchor));
CREATE TABLE IF NOT EXISTS work_saved_decision (identity_id TEXT NOT NULL, kind TEXT NOT NULL, payload TEXT NOT NULL, review_reason TEXT, PRIMARY KEY(identity_id,kind));
CREATE TABLE IF NOT EXISTS derived_work_member (
 identity_id TEXT PRIMARY KEY, provider TEXT NOT NULL, session_id TEXT NOT NULL,
 task_index INTEGER NOT NULL, UNIQUE(provider,session_id,task_index));
CREATE TABLE IF NOT EXISTS work_relation (
 id TEXT PRIMARY KEY, from_id TEXT NOT NULL, to_id TEXT NOT NULL,
 kind TEXT NOT NULL, decision TEXT NOT NULL, source TEXT NOT NULL,
 evidence TEXT NOT NULL, confidence REAL, created_at TEXT NOT NULL DEFAULT (datetime('now')),
 UNIQUE(from_id,to_id));
CREATE INDEX IF NOT EXISTS idx_work_relation_to ON work_relation(to_id);
CREATE TABLE IF NOT EXISTS derived_work_activity (
 provider TEXT NOT NULL, session_id TEXT NOT NULL, task_index INTEGER NOT NULL,
 turn_index INTEGER NOT NULL, start_ms INTEGER NOT NULL, end_ms INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS idx_work_activity_time ON derived_work_activity(start_ms,end_ms);
CREATE INDEX IF NOT EXISTS idx_work_activity_member ON derived_work_activity(provider,session_id,task_index);
CREATE INDEX IF NOT EXISTS idx_usage_work_member ON derived_usage_event(provider,root_session_id,task_index);
CREATE TABLE IF NOT EXISTS analytics_setting (key TEXT PRIMARY KEY, value TEXT NOT NULL);
";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Relation {
    pub id: String,
    pub from_id: String,
    pub to_id: String,
    pub kind: String,
    pub decision: String,
    pub source: String,
    pub evidence: String,
    pub confidence: Option<f64>,
    pub review_reason: Option<String>,
}
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Member {
    pub id: String,
    pub task: TaskRow,
    pub review_reason: Option<String>,
}
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Activity {
    pub start_ms: i64,
    pub end_ms: i64,
    pub stage: String,
    pub source: String,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Tokens {
    pub uncached_input: i64,
    pub cache_write: i64,
    pub cache_read: i64,
    pub output: i64,
    pub reasoning_extra: i64,
    pub total: i64,
}
impl Tokens {
    pub fn add(&mut self, other: &Self) {
        self.uncached_input += other.uncached_input;
        self.cache_write += other.cache_write;
        self.cache_read += other.cache_read;
        self.output += other.output;
        self.reasoning_extra += other.reasoning_extra;
        self.total += other.total;
    }
}
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Usage {
    pub ts: Option<i64>,
    pub provider: String,
    pub model: Option<String>,
    pub tokens: Tokens,
    pub native: bool,
    pub available: bool,
    pub availability: String,
    pub context: Option<i64>,
    pub reported_cost: Option<f64>,
    pub api_equivalent_cost: Option<f64>,
    pub stage: String,
    pub stage_source: String,
}
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Completion {
    pub at: i64,
    pub verified: bool,
    pub source: String,
    pub member_id: String,
}
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ExecutionOutcome {
    pub at: Option<i64>,
    pub outcome: String,
    pub member_id: String,
    pub source: String,
}
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct WorkItem {
    pub id: String,
    pub title: String,
    pub intent: String,
    pub intent_source: String,
    pub started_at: Option<i64>,
    pub last_activity_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub initial_completed_at: Option<i64>,
    pub verified: bool,
    pub state: String,
    pub conversations: usize,
    pub attempts: i64,
    pub validation_failures: i64,
    pub corrections: i64,
    pub members: Vec<Member>,
    pub completions: Vec<Completion>,
    pub reopens: Vec<i64>,
    pub relations: Vec<Relation>,
    pub activity: Vec<Activity>,
    pub usage: Vec<Usage>,
    pub members_missing_usage: usize,
    pub observed_activity: bool,
    /// Runtime success/failure is distinct from completion of the user's goal.
    pub execution_outcomes: Vec<ExecutionOutcome>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub pull_requests: Vec<super::delivery::PrEvidence>,
}
impl WorkItem {
    pub fn cycle_minutes(&self) -> Option<f64> {
        Some((self.completed_at? - self.started_at?).max(0) as f64 / 60_000.0)
    }
    pub fn active_minutes(&self) -> f64 {
        union_minutes(
            self.activity
                .iter()
                .map(|a| (a.start_ms, a.end_ms))
                .collect(),
        )
    }
    pub fn native_tokens(&self) -> Option<i64> {
        (self.members_missing_usage == 0
            && !self.usage.is_empty()
            && self.usage.iter().all(|u| u.native && u.available))
        .then(|| self.usage.iter().map(|u| u.tokens.total).sum())
    }
}

pub fn union_minutes(mut intervals: Vec<(i64, i64)>) -> f64 {
    intervals.sort_unstable();
    let mut total = 0;
    let mut current: Option<(i64, i64)> = None;
    for (start, end) in intervals.into_iter().filter(|(s, e)| e > s) {
        match current {
            Some((s, e)) if start <= e => current = Some((s, e.max(end))),
            Some((s, e)) => {
                total += e - s;
                current = Some((start, end));
            }
            None => current = Some((start, end)),
        }
    }
    if let Some((s, e)) = current {
        total += e - s;
    }
    total as f64 / 60_000.0
}
fn digest(value: &str) -> String {
    format!("{:x}", md5::compute(value))
}

/// Reconcile using message anchors. Existing decisions survive and are suspended on ambiguity.
pub fn refresh(conn: &Connection) -> Result<()> {
    conn.execute("DELETE FROM derived_work_member", [])?;
    let tasks =
        crate::explore::tasks(conn, &Filters::default(), TaskSort::Started, usize::MAX / 2)?;
    for task in &tasks {
        let keys: Vec<String> = {
            let mut stmt = conn.prepare("SELECT COALESCE(m.message_key, 'legacy:' || m.message_index)
                FROM derived_turn d JOIN fact_session_message m ON m.provider=d.provider AND m.session_id=d.session_id
                AND m.role='user' AND COALESCE(m.is_compaction_summary,0)=0
                AND ((m.message_ts IS NOT NULL AND m.message_ts=d.started_at) OR (m.message_ts IS NULL AND substr(m.content,1,length(d.user_text_head))=d.user_text_head))
                JOIN derived_task t ON t.provider=d.provider AND t.session_id=d.session_id
                WHERE t.provider=?1 AND t.session_id=?2 AND t.task_index=?3
                AND (',' || t.turn_list || ',') LIKE ('%,' || d.turn_index || ',%') ORDER BY d.turn_index,m.message_index")?;
            stmt.query_map(
                params![task.provider, task.session_id, task.task_index],
                |r| r.get(0),
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let anchor = keys
            .first()
            .cloned()
            .unwrap_or_else(|| format!("unresolved:{}", task.task_index));
        let id = digest(&format!("{}|{}|{anchor}", task.provider, task.session_id));
        let old: Option<String> = conn
            .query_row(
                "SELECT messages_json FROM work_identity WHERE id=?1",
                [&id],
                |r| r.get(0),
            )
            .optional()?;
        let old_keys: Vec<String> = old
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();
        let changed = old_keys.iter().any(|k| !keys.contains(k));
        let absorbs_prior_anchor: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM work_identity WHERE provider=?1 AND session_id=?2 AND anchor!=?3 AND anchor IN (SELECT value FROM json_each(?4)))",params![task.provider,task.session_id,anchor,serde_json::to_string(&keys)?],|r|r.get(0))?;
        let review = if keys.is_empty() || keys.iter().any(|k| k.starts_with("legacy:")) {
            Some("Stable message identity unavailable")
        } else if changed || absorbs_prior_anchor {
            Some("Task boundaries changed; review saved relationships")
        } else {
            None
        };
        conn.execute("INSERT INTO work_identity(id,provider,session_id,anchor,messages_json,review_reason) VALUES(?1,?2,?3,?4,?5,?6)
          ON CONFLICT(id) DO UPDATE SET messages_json=excluded.messages_json,
          review_reason=COALESCE(excluded.review_reason,work_identity.review_reason)",
          params![id,task.provider,task.session_id,anchor,serde_json::to_string(&keys)?,review])?;
        conn.execute(
            "INSERT OR IGNORE INTO derived_work_member VALUES(?1,?2,?3,?4)",
            params![id, task.provider, task.session_id, task.task_index],
        )?;
    }
    // Old endpoints remain auditable when segmentation moves their anchor out of a task boundary.
    conn.execute("UPDATE work_identity SET review_reason='Task anchor no longer starts a task; review membership'
        WHERE NOT EXISTS(SELECT 1 FROM derived_work_member m WHERE m.identity_id=work_identity.id)",[])?;
    discover(conn, &tasks)?;
    Ok(())
}

fn discover(conn: &Connection, tasks: &[TaskRow]) -> Result<()> {
    let repositories: BTreeMap<_, _> = tasks
        .iter()
        .map(|t| {
            (
                (t.provider.as_str(), t.session_id.as_str(), t.task_index),
                t.repo_root.as_deref(),
            )
        })
        .collect();
    let mut ids = BTreeMap::new();
    let mut stmt =
        conn.prepare("SELECT provider,session_id,task_index,identity_id FROM derived_work_member")?;
    for row in stmt.query_map([], |r| {
        Ok((
            (
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
            ),
            r.get::<_, String>(3)?,
        ))
    })? {
        let (key, id) = row?;
        ids.insert(key, id);
    }
    // Existing overlap/branch relations are candidate evidence, never automatic membership.
    let mut stmt=conn.prepare("SELECT from_provider,from_session,from_task,to_provider,to_session,to_task,relation_kind FROM derived_task_relation")?;
    for row in stmt.query_map([], |r| {
        Ok((
            (
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
            ),
            (
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, i64>(5)?,
            ),
            r.get::<_, String>(6)?,
        ))
    })? {
        let (from, to, evidence) = row?;
        let repo = |key: &(String, String, i64)| {
            repositories
                .get(&(key.0.as_str(), key.1.as_str(), key.2))
                .copied()
                .flatten()
        };
        if repo(&from).is_none() || repo(&from) != repo(&to) {
            continue;
        }
        if let (Some(a), Some(b)) = (ids.get(&from), ids.get(&to)) {
            insert_relation(
                conn,
                a,
                b,
                "continuation",
                "suggested",
                "heuristic",
                &evidence,
                None,
            )?;
        }
    }
    // Weak context is useful for discovery, but never establishes shared membership.
    let words = |text: &str| {
        text.split_whitespace()
            .map(|s| {
                s.trim_matches(|c: char| !c.is_alphanumeric() && c != '-' && c != '#')
                    .to_lowercase()
            })
            .filter(|s| s.len() > 3)
            .collect::<BTreeSet<_>>()
    };
    let normalized: Vec<_> = tasks
        .iter()
        .map(|t| (words(&t.title), t.started_at.as_deref().and_then(parse_ts)))
        .collect();
    for (later_index, later) in tasks.iter().enumerate() {
        let (later_words, later_start) = &normalized[later_index];
        let mut candidates = 0;
        for (earlier_index, earlier) in tasks.iter().enumerate().rev() {
            if candidates >= 20 {
                break;
            }
            if earlier.repo_root.is_none()
                || earlier.repo_root != later.repo_root
                || earlier.session_id == later.session_id
            {
                continue;
            }
            let Some(gap) = normalized[earlier_index]
                .1
                .zip(*later_start)
                .map(|(a, b)| b - a)
            else {
                continue;
            };
            if !(1..=56 * 86_400_000).contains(&gap) {
                continue;
            }
            let earlier_words = &normalized[earlier_index].0;
            let shared = earlier_words.intersection(later_words).count();
            let similar = shared >= 3 && shared * 2 >= earlier_words.union(later_words).count();
            let ticket = earlier_words.intersection(later_words).any(|s| {
                s.chars().any(|c| c.is_ascii_digit())
                    && (s.contains('-') || s.starts_with('#') || s.contains("/pull/"))
            });
            let branch = earlier.git_branch.as_ref().is_some_and(|b| {
                !["main", "master", "HEAD"].contains(&b.as_str())
                    && Some(b) == later.git_branch.as_ref()
            });
            if !similar && !ticket && !branch {
                continue;
            }
            if let (Some(a), Some(b)) = (
                ids.get(&(
                    earlier.provider.clone(),
                    earlier.session_id.clone(),
                    earlier.task_index,
                )),
                ids.get(&(
                    later.provider.clone(),
                    later.session_id.clone(),
                    later.task_index,
                )),
            ) {
                let evidence = format!(
                    "Candidate only: shared topic={similar}, shared reference={ticket}, shared branch={branch}"
                );
                insert_relation(
                    conn,
                    a,
                    b,
                    "continuation",
                    "suggested",
                    "heuristic",
                    &evidence,
                    None,
                )?;
                candidates += 1;
            }
        }
    }
    // A named prior conversation plus an explicit continuation/reopening instruction is unambiguous.
    for later in tasks {
        let text = later.title.to_lowercase();
        let kind = if text.contains("reopen") {
            "reopens"
        } else if text.contains("continue") || text.contains("resume") {
            "continuation"
        } else {
            continue;
        };
        let candidates: Vec<_> = tasks
            .iter()
            .filter(|earlier| {
                earlier.repo_root.is_some()
                    && earlier.repo_root == later.repo_root
                    && earlier.session_id != later.session_id
                    && earlier.started_at < later.started_at
                    && text.contains(&earlier.session_id.to_lowercase())
            })
            .collect();
        if candidates.len() == 1 {
            let earlier = candidates[0];
            if let (Some(a), Some(b)) = (
                ids.get(&(
                    earlier.provider.clone(),
                    earlier.session_id.clone(),
                    earlier.task_index,
                )),
                ids.get(&(
                    later.provider.clone(),
                    later.session_id.clone(),
                    later.task_index,
                )),
            ) {
                insert_relation(
                    conn,
                    a,
                    b,
                    kind,
                    "accepted",
                    "explicit_reference",
                    &later.title,
                    Some(1.0),
                )?;
            }
        }
    }
    Ok(())
}
#[allow(clippy::too_many_arguments)]
pub fn insert_relation(
    conn: &Connection,
    from: &str,
    to: &str,
    kind: &str,
    decision: &str,
    source: &str,
    evidence: &str,
    confidence: Option<f64>,
) -> Result<String> {
    if from == to {
        bail!("A task cannot link to itself");
    }
    if ![
        "continuation",
        "reopens",
        "corrects_previous_work",
        "unrelated",
    ]
    .contains(&kind)
    {
        bail!("Unknown relationship kind: {kind}");
    }
    let id = digest(&format!("{from}>{to}"));
    if source == "manual" {
        conn.execute("INSERT INTO work_relation(id,from_id,to_id,kind,decision,source,evidence,confidence) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)
          ON CONFLICT(from_id,to_id) DO UPDATE SET kind=excluded.kind,decision=excluded.decision,source=excluded.source,evidence=excluded.evidence,confidence=excluded.confidence",
          params![id,from,to,kind,decision,source,evidence,confidence])?;
    } else {
        conn.execute("INSERT INTO work_relation(id,from_id,to_id,kind,decision,source,evidence,confidence) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)
          ON CONFLICT(from_id,to_id) DO UPDATE SET kind=excluded.kind,decision=excluded.decision,source=excluded.source,evidence=excluded.evidence,confidence=excluded.confidence
          WHERE work_relation.source != 'manual' AND (excluded.source='explicit_reference' OR work_relation.source='heuristic')",
          params![id,from,to,kind,decision,source,evidence,confidence])?;
    }
    Ok(id)
}
pub fn relations(conn: &Connection) -> Result<Vec<Relation>> {
    let mut stmt=conn.prepare("SELECT r.id,r.from_id,r.to_id,r.kind,r.decision,r.source,r.evidence,r.confidence,
      COALESCE(a.review_reason,b.review_reason) FROM work_relation r
      JOIN work_identity a ON a.id=r.from_id JOIN work_identity b ON b.id=r.to_id ORDER BY r.created_at,r.id")?;
    Ok(stmt
        .query_map([], |r| {
            Ok(Relation {
                id: r.get(0)?,
                from_id: r.get(1)?,
                to_id: r.get(2)?,
                kind: r.get(3)?,
                decision: r.get(4)?,
                source: r.get(5)?,
                evidence: r.get(6)?,
                confidence: r.get(7)?,
                review_reason: r.get(8)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?)
}
pub fn resolve_member(conn: &Connection, prefix: &str) -> Result<String> {
    let mut stmt=conn.prepare("SELECT id FROM work_identity WHERE id LIKE ?1 OR provider || ':' || session_id || '#' || (SELECT task_index FROM derived_work_member WHERE identity_id=id)=?2")?;
    let ids = stmt
        .query_map(params![format!("{prefix}%"), prefix], |r| {
            r.get::<_, String>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if ids.len() != 1 {
        bail!(
            "Work/member reference must identify exactly one item (found {})",
            ids.len()
        );
    }
    Ok(ids[0].clone())
}
pub fn decide(conn: &Connection, prefix: &str, accepted: bool) -> Result<()> {
    let found: Vec<_> = relations(conn)?
        .into_iter()
        .filter(|r| r.id.starts_with(prefix))
        .collect();
    if found.len() != 1 {
        bail!("Relationship reference must identify exactly one link");
    }
    let r = &found[0];
    if accepted && r.review_reason.is_some() {
        bail!(
            "Task boundaries changed. Detach the stale link and link the current member IDs explicitly."
        );
    }
    conn.execute(
        "UPDATE work_relation SET decision=?2,source='manual' WHERE id=?1",
        params![r.id, if accepted { "accepted" } else { "rejected" }],
    )?;
    Ok(())
}

pub fn items(conn: &Connection) -> Result<Vec<WorkItem>> {
    let tasks =
        crate::explore::tasks(conn, &Filters::default(), TaskSort::Started, usize::MAX / 2)?;
    let mut members = BTreeMap::new();
    for task in tasks {
        let identity: Option<(String,Option<String>)>=conn.query_row("SELECT m.identity_id,i.review_reason FROM derived_work_member m JOIN work_identity i ON i.id=m.identity_id WHERE m.provider=?1 AND m.session_id=?2 AND m.task_index=?3",params![task.provider,task.session_id,task.task_index],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
        if let Some((id, review_reason)) = identity {
            members.insert(
                id.clone(),
                Member {
                    id,
                    task,
                    review_reason,
                },
            );
        }
    }
    let relations = relations(conn)?;
    let mut relations_by_member: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (index, r) in relations.iter().enumerate() {
        relations_by_member
            .entry(&r.from_id)
            .or_default()
            .push(index);
        relations_by_member.entry(&r.to_id).or_default().push(index);
    }
    let mut groups: BTreeMap<String, String> =
        members.keys().map(|id| (id.clone(), id.clone())).collect();
    for r in relations.iter().filter(|r| {
        r.decision == "accepted"
            && r.review_reason.is_none()
            && ["continuation", "reopens"].contains(&r.kind.as_str())
    }) {
        if let (Some(a), Some(b)) = (
            groups.get(&r.from_id).cloned(),
            groups.get(&r.to_id).cloned(),
        ) {
            let canonical = a.min(b.clone());
            let other = if canonical == b {
                groups[&r.from_id].clone()
            } else {
                b
            };
            for group in groups.values_mut() {
                if *group == other {
                    *group = canonical.clone();
                }
            }
        }
    }
    let mut grouped: BTreeMap<String, Vec<Member>> = BTreeMap::new();
    for (id, member) in members {
        grouped.entry(groups[&id].clone()).or_default().push(member);
    }
    let mut result = Vec::new();
    for (id, mut members) in grouped {
        members.sort_by(|a, b| {
            a.task
                .started_at
                .cmp(&b.task.started_at)
                .then(a.id.cmp(&b.id))
        });
        let first = &members[0];
        let mut work = WorkItem {
            id,
            title: first.task.title.clone(),
            intent: first.task.intent.clone().unwrap_or("unclassified".into()),
            intent_source: first.task.intent_source.clone().unwrap_or("missing".into()),
            started_at: first.task.started_at.as_deref().and_then(parse_ts),
            last_activity_at: None,
            completed_at: None,
            initial_completed_at: None,
            verified: false,
            state: "open".into(),
            conversations: members
                .iter()
                .map(|m| (&m.task.provider, &m.task.session_id))
                .collect::<BTreeSet<_>>()
                .len(),
            attempts: 0,
            validation_failures: 0,
            corrections: 0,
            members: vec![],
            completions: vec![],
            reopens: vec![],
            relations: vec![],
            activity: vec![],
            usage: vec![],
            members_missing_usage: 0,
            observed_activity: false,
            execution_outcomes: vec![],
            pull_requests: vec![],
        };
        for member in &members {
            let t = &member.task;
            let observed:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM derived_turn d JOIN derived_task t ON t.provider=d.provider AND t.session_id=d.session_id WHERE t.provider=?1 AND t.session_id=?2 AND t.task_index=?3 AND (',' || t.turn_list || ',') LIKE ('%,' || d.turn_index || ',%') AND (d.user_chars>0 OR d.assistant_chars>0 OR d.tool_calls>0 OR d.tokens_total>0))",params![t.provider,t.session_id,t.task_index],|r|r.get(0))?;
            work.observed_activity |= observed;
            let mut q=conn.prepare("SELECT e.ts,e.detail FROM fact_session_event e JOIN derived_task t ON t.provider=e.provider AND t.session_id=e.session_id WHERE e.provider=?1 AND e.session_id=?2 AND t.task_index=?3 AND e.kind='runtime_outcome' AND (',' || t.turn_list || ',') LIKE ('%,' || COALESCE((SELECT d.turn_index FROM derived_turn d WHERE d.provider=e.provider AND d.session_id=e.session_id AND julianday(d.started_at)<=julianday(e.ts) ORDER BY d.turn_index DESC LIMIT 1),0) || ',%') ORDER BY e.event_index")?;
            work.execution_outcomes.extend(
                q.query_map(params![t.provider, t.session_id, t.task_index], |r| {
                    let ts: Option<String> = r.get(0)?;
                    Ok(ExecutionOutcome {
                        at: ts.as_deref().and_then(parse_ts),
                        outcome: r
                            .get::<_, Option<String>>(1)?
                            .unwrap_or_else(|| "unknown".into()),
                        member_id: member.id.clone(),
                        source: "provider_runtime".into(),
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?,
            );
            work.attempts += t.attempts;
            work.validation_failures += t.validation_failures;
            let last:Option<String>=conn.query_row("SELECT last_activity_at FROM derived_task WHERE provider=?1 AND session_id=?2 AND task_index=?3",params![t.provider,t.session_id,t.task_index],|r|r.get(0))?;
            work.last_activity_at = work
                .last_activity_at
                .max(last.as_deref().and_then(parse_ts));
            let supported = t.completion_source == "override"
                || (t.completion_source == "jev"
                    && matches!(
                        t.outcome.as_deref(),
                        Some("completed_verified" | "completed_unverified" | "answered")
                    ));
            if supported && let Some(at) = t.completed_at.as_deref().and_then(parse_ts) {
                work.completions.push(Completion {
                    at,
                    verified: t.outcome.as_deref() == Some("completed_verified"),
                    source: t.completion_source.clone(),
                    member_id: member.id.clone(),
                });
            }
            let usage = load_usage(conn, t)?;
            work.members_missing_usage +=
                usize::from(usage.is_empty() || usage.iter().any(|u| !u.available));
            work.usage.extend(usage);
            let mut stmt=conn.prepare("SELECT a.start_ms,a.end_ms,COALESCE(s.stage,'unclassified'),COALESCE(s.stage_source,'missing') FROM derived_work_activity a LEFT JOIN derived_turn_stage s ON s.provider=a.provider AND s.session_id=a.session_id AND s.turn_index=a.turn_index WHERE a.provider=?1 AND a.session_id=?2 AND a.task_index=?3")?;
            work.activity.extend(
                stmt.query_map(params![t.provider, t.session_id, t.task_index], |r| {
                    Ok(Activity {
                        start_ms: r.get(0)?,
                        end_ms: r.get(1)?,
                        stage: r.get(2)?,
                        source: r.get(3)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?,
            );
            work.corrections+=conn.query_row("SELECT COUNT(*) FROM view_label_effective l JOIN derived_task t ON t.provider=?1 AND t.session_id=?2 AND t.task_index=?3 WHERE l.unit_kind='exchange' AND l.question_id='feedback' AND l.label='correct_defect' AND l.unit_key LIKE ?1 || '|' || ?2 || '|%' AND (',' || t.turn_list || ',') LIKE ('%,' || substr(l.unit_key,length(?1)+length(?2)+3) || ',%')",params![t.provider,t.session_id,t.task_index],|r|r.get::<_,i64>(0))?;
        }
        work.completions.sort_by_key(|c| c.at);
        work.initial_completed_at = work.completions.first().map(|c| c.at);
        let related: BTreeSet<usize> = members
            .iter()
            .flat_map(|m| {
                relations_by_member
                    .get(m.id.as_str())
                    .into_iter()
                    .flatten()
                    .copied()
            })
            .collect();
        for index in related {
            let r = &relations[index];
            if members.iter().any(|m| m.id == r.from_id || m.id == r.to_id) {
                if r.kind == "reopens"
                    && r.decision == "accepted"
                    && r.review_reason.is_none()
                    && let Some(at) = members
                        .iter()
                        .find(|m| m.id == r.to_id)
                        .and_then(|m| m.task.started_at.as_deref().and_then(parse_ts))
                {
                    work.reopens.push(at);
                }
                work.relations.push(r.clone());
            }
        }
        // The latest continuation must finish; a prior completion cannot close new unresolved work.
        let last = &members[members.len() - 1];
        if let Some(done) = work
            .completions
            .iter()
            .rev()
            .find(|c| c.member_id == last.id && work.reopens.iter().all(|r| *r <= c.at))
        {
            work.completed_at = Some(done.at);
            work.verified = done.verified;
            work.state = "completed".into();
        } else if last.task.outcome.as_deref() == Some("abandoned_redirected") {
            work.state = "abandoned".into();
        }
        if !work.observed_activity {
            work.state = "unobserved".into();
        }
        work.members = members;
        result.push(work);
    }
    let prs = super::delivery::evidence(conn)?;
    for work in &mut result {
        work.pull_requests = prs
            .iter()
            .filter(|e| {
                e.member_ids
                    .iter()
                    .any(|id| work.members.iter().any(|m| &m.id == id))
            })
            .cloned()
            .collect();
    }
    result.sort_by_key(|a| std::cmp::Reverse(a.started_at));
    Ok(result)
}
fn load_usage(conn: &Connection, t: &TaskRow) -> Result<Vec<Usage>> {
    let mut stmt=conn.prepare("SELECT d.usage_ts,d.provider,d.model_name,u.input_tokens,u.cache_creation_tokens,u.cached_input_tokens,u.output_tokens,
       CASE WHEN COALESCE(u.reasoning_in_output,0)=0 THEN u.reasoning_tokens ELSE 0 END,d.total_tokens,d.token_source,d.context_tokens,d.reported_cost_usd,d.api_equiv_usd,
       COALESCE(s.stage,'unclassified'),COALESCE(s.stage_source,'missing'),
       COALESCE(a.availability,CASE WHEN d.total_tokens>0 THEN CASE WHEN d.token_source='reported' THEN 'native' ELSE 'estimated' END ELSE 'suspect_zero' END)
       FROM derived_usage_event d JOIN fact_session_usage u ON u.id=d.usage_id
       LEFT JOIN usage_measurement a ON a.provider=u.provider AND a.session_id=u.session_id AND a.usage_key=u.usage_key
       LEFT JOIN derived_turn_stage s ON s.provider=d.provider AND s.session_id=d.root_session_id AND s.turn_index=d.root_turn_index
       WHERE d.provider=?1 AND d.root_session_id=?2 AND d.task_index=?3")?;
    Ok(stmt
        .query_map(params![t.provider, t.session_id, t.task_index], |r| {
            let ts: Option<String> = r.get(0)?;
            let source: String = r.get(9)?;
            let total: i64 = r.get(8)?;
            let context: i64 = r.get(10)?;
            Ok(Usage {
                ts: ts.as_deref().and_then(parse_ts),
                provider: r.get(1)?,
                model: r.get(2)?,
                tokens: Tokens {
                    uncached_input: r.get(3)?,
                    cache_write: r.get(4)?,
                    cache_read: r.get(5)?,
                    output: r.get(6)?,
                    reasoning_extra: r.get(7)?,
                    total,
                },
                native: source == "reported",
                available: matches!(
                    r.get::<_, String>(15)?.as_str(),
                    "native" | "estimated" | "explicit_zero"
                ),
                availability: r.get(15)?,
                context: (context > 0).then_some(context),
                reported_cost: r.get(11)?,
                api_equivalent_cost: r.get(12)?,
                stage: r.get(13)?,
                stage_source: r.get(14)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?)
}

/// Capture numeric overrides against the previous message identity before segmentation changes.
pub fn save_decisions(conn: &Connection) -> Result<()> {
    conn.execute(
        "DELETE FROM work_saved_decision WHERE review_reason IS NULL",
        [],
    )?;
    conn.execute("INSERT OR REPLACE INTO work_saved_decision(identity_id,kind,payload,review_reason)
        SELECT m.identity_id,'label:' || o.question_id,json_object('key',o.unit_key,'question',o.question_id,'label',o.label,'note',o.note),NULL
        FROM classification_override o JOIN derived_work_member m ON o.unit_kind='task' AND o.unit_key=m.provider || '|' || m.session_id || '|' || m.task_index",[])?;
    conn.execute("INSERT OR REPLACE INTO work_saved_decision(identity_id,kind,payload,review_reason)
        SELECT m.identity_id,'completion',json_object('provider',c.provider,'session',c.session_id,'task',c.task_index,'turn',c.completion_turn,'confidence',c.confidence),NULL
        FROM task_completion_mark c JOIN derived_work_member m USING(provider,session_id,task_index) WHERE c.source='override'",[])?;
    Ok(())
}

/// Suspend ambiguous overrides instead of applying them to whatever now occupies task N.
pub fn reconcile_decisions(conn: &Connection) -> Result<bool> {
    let mut stmt = conn.prepare("SELECT d.identity_id,d.kind,d.payload,i.review_reason,m.provider,m.session_id,m.task_index FROM work_saved_decision d JOIN work_identity i ON i.id=d.identity_id LEFT JOIN derived_work_member m ON m.identity_id=d.identity_id WHERE d.review_reason IS NULL")?;
    let rows = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, Option<String>>(4)?,
                r.get::<_, Option<String>>(5)?,
                r.get::<_, Option<i64>>(6)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut changed = false;
    // Clear all old addresses before moving any, so swaps cannot overwrite one another.
    let mut moves = Vec::new();
    for (id, kind, json, review, provider, session, task) in rows {
        let payload: serde_json::Value = serde_json::from_str(&json)?;
        let new_key = provider
            .as_ref()
            .zip(session.as_ref())
            .zip(task)
            .map(|((p, s), t)| format!("{p}|{s}|{t}"));
        let old_key = if kind == "completion" {
            format!(
                "{}|{}|{}",
                payload["provider"].as_str().unwrap_or_default(),
                payload["session"].as_str().unwrap_or_default(),
                payload["task"]
            )
        } else {
            payload["key"].as_str().unwrap_or_default().to_string()
        };
        if review.is_none() && new_key.as_ref() == Some(&old_key) {
            continue;
        }
        changed = true;
        if kind == "completion" {
            conn.execute("DELETE FROM task_completion_mark WHERE provider=?1 AND session_id=?2 AND task_index=?3 AND source='override'",params![payload["provider"].as_str(),payload["session"].as_str(),payload["task"].as_i64()])?;
        } else {
            conn.execute("DELETE FROM classification_override WHERE unit_kind='task' AND unit_key=?1 AND question_id=?2",params![old_key,payload["question"].as_str()])?;
        }
        if review.is_some() || new_key.is_none() {
            conn.execute(
                "UPDATE work_saved_decision SET review_reason=?2 WHERE identity_id=?1 AND kind=?3",
                params![
                    id,
                    review.unwrap_or("Task identity unavailable".into()),
                    kind
                ],
            )?;
        } else if let Some(new_key) = new_key {
            moves.push((
                id,
                kind,
                payload,
                provider.unwrap(),
                session.unwrap(),
                task.unwrap(),
                new_key,
            ));
        }
    }
    for (id, kind, mut payload, provider, session, task, key) in moves {
        if kind == "completion" {
            conn.execute(
                "INSERT OR REPLACE INTO task_completion_mark VALUES(?1,?2,?3,?4,'override',?5)",
                params![
                    provider,
                    session,
                    task,
                    payload["turn"].as_i64(),
                    payload["confidence"].as_f64()
                ],
            )?;
            payload["task"] = task.into();
        } else {
            conn.execute("INSERT OR REPLACE INTO classification_override(unit_kind,unit_key,question_id,label,note) VALUES('task',?1,?2,?3,?4)",params![key,payload["question"].as_str(),payload["label"].as_str(),payload["note"].as_str()])?;
            payload["key"] = key.into();
        }
        conn.execute(
            "UPDATE work_saved_decision SET payload=?3 WHERE identity_id=?1 AND kind=?2",
            params![id, kind, payload.to_string()],
        )?;
    }
    Ok(changed)
}

pub fn pending_decisions(conn: &Connection, repo: Option<&str>) -> Result<Vec<serde_json::Value>> {
    let mut q=conn.prepare("SELECT d.identity_id,d.kind,d.payload,d.review_reason,i.provider,i.session_id FROM work_saved_decision d JOIN work_identity i ON i.id=d.identity_id LEFT JOIN metadata_sessions s ON s.provider=i.provider AND s.session_id=i.session_id LEFT JOIN metadata_repositories r ON r.id=s.repository_id WHERE d.review_reason IS NOT NULL AND (?1 IS NULL OR r.repo_root=?1)")?;
    Ok(q.query_map([repo], |r| Ok(serde_json::json!({"identity":r.get::<_,String>(0)?,"kind":r.get::<_,String>(1)?,"saved":serde_json::from_str::<serde_json::Value>(&r.get::<_,String>(2)?).unwrap_or_default(),"review_reason":r.get::<_,String>(3)?,"provider":r.get::<_,String>(4)?,"session":r.get::<_,String>(5)?})))?.collect::<rusqlite::Result<_>>()?)
}
