//! PR and release evidence enriches work items; it never changes their completion.
use anyhow::{Result, bail, ensure};
use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS pull_request_record (
 id TEXT PRIMARY KEY, provider TEXT NOT NULL, repo_key TEXT NOT NULL,
 number INTEGER NOT NULL, url TEXT NOT NULL, record_json TEXT NOT NULL,
 updated_at TEXT, observed_at TEXT NOT NULL, UNIQUE(provider,repo_key,number));
CREATE INDEX IF NOT EXISTS idx_pr_repo ON pull_request_record(repo_key);
CREATE TABLE IF NOT EXISTS pull_request_revision (
 pr_id TEXT NOT NULL, fingerprint TEXT NOT NULL, observed_at TEXT NOT NULL, record_json TEXT NOT NULL,
 PRIMARY KEY(pr_id,fingerprint));
CREATE TABLE IF NOT EXISTS work_pr_link (
 member_id TEXT NOT NULL, pr_id TEXT NOT NULL, role TEXT NOT NULL,
 decision TEXT NOT NULL, source TEXT NOT NULL, evidence TEXT NOT NULL,
 PRIMARY KEY(member_id,pr_id));
CREATE INDEX IF NOT EXISTS idx_work_pr_pr ON work_pr_link(pr_id);
CREATE TABLE IF NOT EXISTS pull_request_intent (
 pr_id TEXT NOT NULL, source TEXT NOT NULL, intent TEXT NOT NULL, confidence REAL,
 input_digest TEXT, PRIMARY KEY(pr_id,source));
CREATE TABLE IF NOT EXISTS deployment_record (
 id TEXT PRIMARY KEY, repo_key TEXT NOT NULL, record_json TEXT NOT NULL,
 occurred_at TEXT NOT NULL, observed_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_deployment_repo ON deployment_record(repo_key,occurred_at);
CREATE TABLE IF NOT EXISTS deployment_inclusion (
 deployment_id TEXT NOT NULL, pr_id TEXT NOT NULL, source TEXT NOT NULL, evidence TEXT NOT NULL,
 PRIMARY KEY(deployment_id,pr_id));
CREATE TABLE IF NOT EXISTS deployment_ancestry_check (repo_key TEXT NOT NULL,merge_sha TEXT NOT NULL,artifact_sha TEXT NOT NULL,included INTEGER NOT NULL,evidence TEXT NOT NULL,PRIMARY KEY(repo_key,merge_sha,artifact_sha));
CREATE TABLE IF NOT EXISTS delivery_source (
 name TEXT PRIMARY KEY, config_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS delivery_checkpoint (
 source_id TEXT PRIMARY KEY, cursor TEXT, completed_at TEXT, error TEXT);
";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct PullRequest {
    pub id: String,
    pub provider: String,
    pub repo_key: String,
    pub number: i64,
    pub url: String,
    pub title: String,
    pub description: String,
    pub labels: Vec<String>,
    /// open, draft, merged, closed, declined or superseded.
    pub state: String,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub merged_at: Option<String>,
    pub closed_at: Option<String>,
    pub head_sha: Option<String>,
    pub merge_sha: Option<String>,
    pub head_ref: Option<String>,
    pub base_ref: Option<String>,
    pub commits: Vec<String>,
    pub files: Vec<String>,
    pub review_events: Vec<serde_json::Value>,
    pub observed_at: String,
}
impl Default for PullRequest {
    fn default() -> Self {
        Self {
            id: String::new(),
            provider: String::new(),
            repo_key: String::new(),
            number: 0,
            url: String::new(),
            title: String::new(),
            description: String::new(),
            labels: vec![],
            state: "open".into(),
            created_at: None,
            updated_at: None,
            merged_at: None,
            closed_at: None,
            head_sha: None,
            merge_sha: None,
            head_ref: None,
            base_ref: None,
            commits: vec![],
            files: vec![],
            review_events: vec![],
            observed_at: Utc::now().to_rfc3339(),
        }
    }
}
impl PullRequest {
    pub fn stable_id(&self) -> String {
        format!("{}:{}#{}", self.provider, self.repo_key, self.number)
    }
    pub fn artifact_shas(&self) -> BTreeSet<String> {
        self.commits
            .iter()
            .chain(self.head_sha.iter())
            .chain(self.merge_sha.iter())
            .cloned()
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct Deployment {
    pub id: String,
    pub provider: String,
    pub repo_key: String,
    pub artifact_sha: String,
    pub environment: String,
    pub component: String,
    pub status: String,
    pub live: bool,
    pub occurred_at: String,
    pub observed_at: String,
    pub evidence_url: String,
    /// Explicit provider/pipeline ancestry evidence, never an assistant assertion.
    pub included_commits: Vec<String>,
    pub required_components: Vec<String>,
    pub rollback_of: Option<String>,
    /// Stable release identity for components with different artifact hashes.
    pub release_id: Option<String>,
    pub evidence_source: String,
}
impl Default for Deployment {
    fn default() -> Self {
        Self {
            id: String::new(),
            provider: String::new(),
            repo_key: String::new(),
            artifact_sha: String::new(),
            environment: String::new(),
            component: "default".into(),
            status: "pending".into(),
            live: false,
            occurred_at: String::new(),
            observed_at: Utc::now().to_rfc3339(),
            evidence_url: String::new(),
            included_commits: vec![],
            required_components: vec!["default".into()],
            rollback_of: None,
            release_id: None,
            evidence_source: "pipeline_export".into(),
        }
    }
}

pub fn save_pr(conn: &Connection, pr: &PullRequest) -> Result<String> {
    ensure!(
        !pr.provider.is_empty() && !pr.repo_key.is_empty() && pr.number > 0,
        "PR needs provider, repository identity and positive number"
    );
    ensure!(
        matches!(
            pr.state.as_str(),
            "open" | "draft" | "merged" | "closed" | "declined" | "superseded"
        ),
        "Unknown PR state"
    );
    validate_times([
        pr.created_at.as_deref(),
        pr.updated_at.as_deref(),
        pr.merged_at.as_deref(),
        pr.closed_at.as_deref(),
        Some(&pr.observed_at),
    ])?;
    let mut pr = pr.clone();
    pr.id = pr.stable_id();
    let json = serde_json::to_string(&pr)?;
    conn.execute("INSERT INTO pull_request_record VALUES(?1,?2,?3,?4,?5,?6,?7,?8) ON CONFLICT(id) DO UPDATE SET url=excluded.url,record_json=excluded.record_json,updated_at=excluded.updated_at,observed_at=excluded.observed_at",params![pr.id,pr.provider,pr.repo_key,pr.number,pr.url,json,pr.updated_at,pr.observed_at])?;
    let mut revision = pr.clone();
    revision.observed_at.clear();
    conn.execute(
        "INSERT OR IGNORE INTO pull_request_revision VALUES(?1,?2,?3,?4)",
        params![
            pr.id,
            format!("{:x}", md5::compute(serde_json::to_vec(&revision)?)),
            pr.observed_at,
            json
        ],
    )?;
    Ok(pr.id)
}
fn validate_times<'a>(values: impl IntoIterator<Item = Option<&'a str>>) -> Result<()> {
    for s in values.into_iter().flatten() {
        chrono::DateTime::parse_from_rfc3339(s)?;
    }
    Ok(())
}

pub fn save_deployment(conn: &Connection, d: &Deployment) -> Result<()> {
    ensure!(
        !d.id.is_empty() && !d.repo_key.is_empty() && !d.environment.is_empty(),
        "Deployment needs identity, repository and environment"
    );
    ensure!(
        matches!(
            d.status.as_str(),
            "success"
                | "failed"
                | "failure"
                | "error"
                | "pending"
                | "queued"
                | "in_progress"
                | "inactive"
                | "rollback"
                | "cancelled"
        ),
        "Unknown deployment status"
    );
    ensure!(
        d.status != "success" || (!d.artifact_sha.is_empty() && !d.evidence_url.is_empty()),
        "Successful deployment needs artifact and source evidence"
    );
    validate_times([Some(d.occurred_at.as_str()), Some(d.observed_at.as_str())])?;
    conn.execute("INSERT INTO deployment_record VALUES(?1,?2,?3,?4,?5) ON CONFLICT(id) DO UPDATE SET record_json=excluded.record_json,occurred_at=excluded.occurred_at,observed_at=excluded.observed_at",params![d.id,d.repo_key,serde_json::to_string(d)?,d.occurred_at,d.observed_at])?;
    Ok(())
}

pub fn prs(conn: &Connection) -> Result<Vec<PullRequest>> {
    json_records(
        conn,
        "SELECT record_json FROM pull_request_record ORDER BY id",
    )
}
pub fn deployments(conn: &Connection) -> Result<Vec<Deployment>> {
    json_records(
        conn,
        "SELECT record_json FROM deployment_record ORDER BY occurred_at,id",
    )
}
fn json_records<T: serde::de::DeserializeOwned>(conn: &Connection, sql: &str) -> Result<Vec<T>> {
    let strings = conn
        .prepare(sql)?
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    strings
        .iter()
        .map(|s| Ok(serde_json::from_str(s)?))
        .collect()
}

pub fn set_intent(conn: &Connection, pr: &str, intent: &str, source: &str) -> Result<()> {
    ensure!(!intent.is_empty(), "Intent cannot be empty");
    conn.execute("INSERT INTO pull_request_intent(pr_id,source,intent) VALUES(?1,?2,?3) ON CONFLICT(pr_id,source) DO UPDATE SET intent=excluded.intent",params![pr,source,intent])?;
    Ok(())
}
pub fn intent(conn: &Connection, pr: &PullRequest) -> Result<(String, String)> {
    let saved=conn.query_row("SELECT intent,source FROM pull_request_intent WHERE pr_id=?1 ORDER BY CASE source WHEN 'override' THEN 0 WHEN 'mapping' THEN 1 WHEN 'classifier' THEN 2 ELSE 3 END LIMIT 1",[&pr.id],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    Ok(saved.unwrap_or_else(|| ("unknown".into(), "missing".into())))
}

pub fn link(
    conn: &Connection,
    member: &str,
    pr: &str,
    role: &str,
    decision: &str,
    source: &str,
    evidence: &str,
) -> Result<()> {
    ensure!(
        [
            "implementation",
            "review",
            "verification",
            "coordination",
            "investigation",
            "recovery"
        ]
        .contains(&role),
        "Unknown contribution role"
    );
    ensure!(
        ["accepted", "suggested", "rejected"].contains(&decision),
        "Unknown link decision"
    );
    ensure!(
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM work_identity WHERE id=?1)",
            [member],
            |r| r.get::<_, bool>(0)
        )?,
        "Unknown work member"
    );
    ensure!(
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM pull_request_record WHERE id=?1)",
            [pr],
            |r| r.get::<_, bool>(0)
        )?,
        "Unknown PR"
    );
    conn.execute("INSERT INTO work_pr_link VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(member_id,pr_id) DO UPDATE SET role=excluded.role,decision=excluded.decision,source=excluded.source,evidence=excluded.evidence WHERE work_pr_link.source!='manual' OR excluded.source='manual'",params![member,pr,role,decision,source,evidence])?;
    Ok(())
}

/// References associate contributions, not whole goals or token ownership.
pub fn refresh_links(conn: &Connection) -> Result<()> {
    let prs = prs(conn)?;
    if prs.is_empty() {
        return Ok(());
    }
    let mut q=conn.prepare("SELECT m.identity_id,COALESCE((SELECT group_concat(f.content,char(10)) FROM fact_session_message f WHERE f.provider=i.provider AND f.session_id=i.session_id AND f.role='user' AND COALESCE(f.message_key,'legacy:'||f.message_index) IN (SELECT value FROM json_each(i.messages_json))),t.title) FROM derived_work_member m JOIN derived_task t USING(provider,session_id,task_index) JOIN work_identity i ON i.id=m.identity_id WHERE i.review_reason IS NULL")?;
    let rows = q
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (member, text) in rows {
        for pr in &prs {
            if !pr.url.is_empty()
                && regex::Regex::new(&format!(r"{}(?:$|[^0-9])", regex::escape(&pr.url)))?
                    .is_match(&text)
            {
                let lower = text.to_lowercase();
                let role = if lower.contains("review") {
                    "review"
                } else if lower.contains("verify") || lower.contains("acceptance") {
                    "verification"
                } else if lower.contains("recover") {
                    "recovery"
                } else {
                    "coordination"
                };
                link(
                    conn,
                    &member,
                    &pr.id,
                    role,
                    "accepted",
                    "explicit_reference",
                    &pr.url,
                )?;
            }
        }
    }
    // Existing matched commit attribution is stronger than a shared branch name.
    let mut q=conn.prepare("SELECT MIN(m.identity_id),a.commit_sha FROM fact_commit_session_match a JOIN metadata_sessions s ON s.provider=a.provider AND s.session_id=a.session_id JOIN derived_work_member m ON m.provider=s.provider AND m.session_id=COALESCE(NULLIF(s.root_session_id,''),s.session_id) WHERE a.matched_lines>0 GROUP BY a.provider,a.session_id,a.commit_sha HAVING COUNT(DISTINCT m.identity_id)=1")?;
    let rows = q
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (member, sha) in rows {
        for pr in &prs {
            if pr.artifact_shas().contains(&sha) {
                link(
                    conn,
                    &member,
                    &pr.id,
                    "implementation",
                    "accepted",
                    "commit_attribution",
                    &sha,
                )?;
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContributionLink {
    pub member_id: String,
    pub role: String,
    pub decision: String,
    pub source: String,
    pub evidence: String,
    pub review_reason: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PrEvidence {
    pub pr: PullRequest,
    pub intent: String,
    pub intent_source: String,
    pub work_ids: Vec<String>,
    pub member_ids: Vec<String>,
    pub roles: Vec<String>,
    pub deployments: Vec<Deployment>,
    pub deployed_at: Option<String>,
    pub rolled_back: bool,
    pub shared_work: bool,
    pub contributions: Vec<ContributionLink>,
}

pub fn evidence(conn: &Connection) -> Result<Vec<PrEvidence>> {
    let all = deployments(conn)?;
    let mut result = vec![];
    for pr in prs(conn)? {
        let (intent, intent_source) = intent(conn, &pr)?;
        let mut q=conn.prepare("SELECT DISTINCT m.work_id,l.member_id,l.role FROM work_pr_link l JOIN derived_work_index_member m ON m.identity_id=l.member_id JOIN work_identity i ON i.id=l.member_id WHERE l.pr_id=?1 AND l.decision='accepted' AND i.review_reason IS NULL")?;
        let links = q
            .query_map([&pr.id], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let shas = pr.artifact_shas();
        let stored = conn
            .prepare("SELECT deployment_id FROM deployment_inclusion WHERE pr_id=?1")?
            .query_map([&pr.id], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<BTreeSet<_>>>()?;
        let mut related: Vec<_> = all
            .iter()
            .filter(|d| {
                d.repo_key == pr.repo_key
                    && (shas.contains(&d.artifact_sha)
                        || d.included_commits.iter().any(|s| shas.contains(s))
                        || stored.contains(&d.id))
            })
            .cloned()
            .collect();
        let included_ids: BTreeSet<_> = related.iter().map(|d| d.id.clone()).collect();
        for d in &all {
            if d.repo_key == pr.repo_key
                && d.rollback_of
                    .as_ref()
                    .is_some_and(|id| included_ids.contains(id))
                && !related.iter().any(|r| r.id == d.id)
            {
                related.push(d.clone());
            }
        }
        related.sort_by(|a, b| {
            super::structure::parse_ts(&a.occurred_at)
                .cmp(&super::structure::parse_ts(&b.occurred_at))
                .then(a.id.cmp(&b.id))
        });
        let mut components: BTreeMap<(String, String), BTreeMap<String, String>> = BTreeMap::new();
        let mut deployed_at = None;
        for d in &related {
            if d.live
                && d.status == "success"
                && !d.artifact_sha.is_empty()
                && !d.evidence_url.is_empty()
                && !d.required_components.is_empty()
                && ["provider_api", "pipeline_export"].contains(&d.evidence_source.as_str())
            {
                let done = components
                    .entry((
                        d.environment.clone(),
                        d.release_id
                            .clone()
                            .unwrap_or_else(|| d.artifact_sha.clone()),
                    ))
                    .or_default();
                done.insert(d.component.clone(), d.occurred_at.clone());
                if d.required_components.iter().all(|c| done.contains_key(c)) {
                    let at = done
                        .values()
                        .max_by_key(|v| super::structure::parse_ts(v))
                        .cloned();
                    if deployed_at.is_none()
                        || at.as_deref().and_then(super::structure::parse_ts)
                            < deployed_at.as_deref().and_then(super::structure::parse_ts)
                    {
                        deployed_at = at;
                    }
                }
            }
        }
        let rolled_back = related
            .iter()
            .any(|d| d.status == "rollback" || d.rollback_of.is_some());
        let work_ids: Vec<_> = links
            .iter()
            .map(|r| r.0.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let mut shared = false;
        for id in &work_ids {
            let n:i64=conn.query_row("SELECT count(DISTINCT l.pr_id) FROM work_pr_link l JOIN derived_work_index_member m ON m.identity_id=l.member_id WHERE m.work_id=?1 AND l.decision='accepted'",[id],|r|r.get(0))?;
            shared |= n > 1;
        }
        let contributions=conn.prepare("SELECT l.member_id,l.role,l.decision,l.source,l.evidence,i.review_reason FROM work_pr_link l LEFT JOIN work_identity i ON i.id=l.member_id WHERE l.pr_id=?1 ORDER BY l.member_id")?.query_map([&pr.id],|r|Ok(ContributionLink{member_id:r.get(0)?,role:r.get(1)?,decision:r.get(2)?,source:r.get(3)?,evidence:r.get(4)?,review_reason:r.get(5)?}))?.collect::<rusqlite::Result<Vec<_>>>()?;
        result.push(PrEvidence {
            pr,
            intent,
            intent_source,
            work_ids,
            member_ids: links.iter().map(|r| r.1.clone()).collect(),
            roles: links
                .iter()
                .map(|r| r.2.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            deployments: related,
            deployed_at,
            rolled_back,
            shared_work: shared,
            contributions,
        });
    }
    Ok(result)
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", content = "record", rename_all = "snake_case")]
pub enum ImportRecord {
    PullRequest(PullRequest),
    Deployment(Deployment),
    Coverage(super::observation::SourceCoverage),
    WorkEvent(super::audit::Event),
}
#[derive(Debug, Deserialize, Serialize)]
pub struct Import {
    pub schema_version: u32,
    pub records: Vec<ImportRecord>,
}
pub fn import(conn: &Connection, bytes: &[u8]) -> Result<usize> {
    let records = if bytes.iter().find(|b| !b.is_ascii_whitespace()) == Some(&b'{') {
        if let Ok(envelope) = serde_json::from_slice::<Import>(bytes) {
            ensure!(
                matches!(envelope.schema_version, 1 | 2),
                "Unsupported delivery import schema"
            );
            envelope.records
        } else {
            std::str::from_utf8(bytes)?
                .lines()
                .filter(|s| !s.trim().is_empty())
                .map(serde_json::from_str)
                .collect::<std::result::Result<Vec<ImportRecord>, _>>()?
        }
    } else {
        bail!("Expected versioned JSON or NDJSON records");
    };
    conn.execute_batch("SAVEPOINT delivery_import")?;
    let result = (|| {
        for r in &records {
            match r {
                ImportRecord::PullRequest(pr) => {
                    save_pr(conn, pr)?;
                }
                ImportRecord::Deployment(d) => save_deployment(conn, d)?,
                ImportRecord::Coverage(c) => super::observation::record(conn, c)?,
                ImportRecord::WorkEvent(e) => {
                    super::audit::save(conn, e)?;
                }
            }
        }
        refresh_links(conn)?;
        super::observation::publish(conn)?;
        Ok(records.len())
    })();
    match result {
        Ok(n) => {
            conn.execute_batch("RELEASE delivery_import")?;
            Ok(n)
        }
        Err(e) => {
            conn.execute_batch("ROLLBACK TO delivery_import; RELEASE delivery_import")?;
            Err(e)
        }
    }
}
