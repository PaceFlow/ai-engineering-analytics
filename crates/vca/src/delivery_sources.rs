//! Read-only repository-driven PR and deployment collection.
use crate::insights::{
    delivery::{self, Deployment, PullRequest},
    observation::{self, SourceCoverage},
};
use anyhow::{Context, Result, bail, ensure};
use chrono::{Duration, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Source {
    pub name: String,
    pub provider: String,
    pub repository: String,
    pub repo_root: String,
    pub api_base: Option<String>,
    pub token_env: Option<String>,
    pub email_env: Option<String>,
    pub live_environments: Vec<String>,
    pub required_components: Vec<String>,
    pub intent_labels: BTreeMap<String, String>,
    pub enabled: bool,
    pub deployment_jobs: Vec<DeploymentJob>,
}
impl Default for Source {
    fn default() -> Self {
        Self {
            name: String::new(),
            provider: "github".into(),
            repository: String::new(),
            repo_root: String::new(),
            api_base: None,
            token_env: None,
            email_env: None,
            live_environments: vec!["production".into()],
            required_components: vec!["default".into()],
            intent_labels: BTreeMap::from([
                ("bug".into(), "bug_fix".into()),
                ("bugfix".into(), "bug_fix".into()),
                ("feature".into(), "feature".into()),
            ]),
            enabled: true,
            deployment_jobs: vec![],
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentJob {
    pub workflow_id: String,
    pub job_name: String,
    pub environment: String,
    pub component: String,
    /// Explicit pipeline contract: this job deploys precisely the run's head SHA.
    pub artifact_from: String,
}
impl Source {
    pub fn repo_key(&self) -> String {
        format!(
            "git:{}/{}",
            if self.provider == "github" {
                "github.com"
            } else {
                "bitbucket.org"
            },
            self.repository.trim_end_matches(".git")
        )
    }
    pub fn base(&self) -> String {
        self.api_base
            .clone()
            .unwrap_or_else(|| {
                if self.provider == "github" {
                    "https://api.github.com".into()
                } else {
                    "https://api.bitbucket.org/2.0".into()
                }
            })
            .trim_end_matches('/')
            .into()
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.name.is_empty() && !self.repo_root.is_empty(),
            "Source needs name and repo_root"
        );
        ensure!(
            ["github", "bitbucket"].contains(&self.provider.as_str()),
            "Provider must be github or bitbucket"
        );
        let parts: Vec<_> = self.repository.split('/').collect();
        ensure!(
            parts.len() == 2
                && parts.iter().all(|p| !p.is_empty()
                    && p.chars()
                        .all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c))),
            "Repository must be owner/name or workspace/name"
        );
        let url = reqwest::Url::parse(&self.base())?;
        ensure!(
            url.username().is_empty() && url.password().is_none() && url.query().is_none(),
            "API base must not contain credentials or query parameters"
        );
        ensure!(
            url.scheme() == "https"
                || [Some("localhost"), Some("127.0.0.1")].contains(&url.host_str()),
            "API base requires HTTPS"
        );
        ensure!(
            !self.required_components.is_empty(),
            "At least one deployment component is required"
        );
        for j in &self.deployment_jobs {
            ensure!(
                self.provider == "github"
                    && j.artifact_from == "head_sha"
                    && !j.job_name.is_empty(),
                "Deployment jobs require GitHub and an explicit head_sha artifact contract"
            );
        }
        Ok(())
    }
}
pub fn sources(conn: &Connection) -> Result<Vec<Source>> {
    let rows = conn
        .prepare("SELECT config_json FROM delivery_source ORDER BY name")?
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    rows.iter().map(|s| Ok(serde_json::from_str(s)?)).collect()
}
pub fn save(conn: &Connection, s: &Source) -> Result<()> {
    s.validate()?;
    conn.execute("INSERT INTO delivery_source VALUES(?1,?2) ON CONFLICT(name) DO UPDATE SET config_json=excluded.config_json",params![s.name,serde_json::to_string(s)?])?;
    if s.repo_root != s.repo_key() {
        observation::set_alias(conn, &s.repo_root, &s.repo_key(), "source")?;
    }
    Ok(())
}

struct Http {
    client: reqwest::Client,
    base: reqwest::Url,
    token: String,
    email: Option<String>,
}
impl Http {
    fn new(s: &Source, context: crate::app::AppContext) -> Result<Self> {
        let token = match s.token_env.as_deref() {
            Some(key) => std::env::var(key).ok(),
            None if s.provider == "github" => crate::github::auth::github_token_for(context)?,
            None => std::env::var("BITBUCKET_TOKEN").ok(),
        }
        .filter(|s| !s.trim().is_empty())
        .context("Source credential unavailable; configure its token environment variable")?;
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .user_agent("VCA analytics")
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            base: reqwest::Url::parse(&s.base())?,
            token,
            email: s.email_env.as_ref().and_then(|e| std::env::var(e).ok()),
        })
    }
    async fn get(&self, url: &str) -> Result<(Value, Option<String>)> {
        let parsed = reqwest::Url::parse(url)?;
        ensure!(
            parsed.scheme() == self.base.scheme()
                && parsed.host_str() == self.base.host_str()
                && parsed.port_or_known_default() == self.base.port_or_known_default(),
            "Refusing cross-origin source pagination"
        );
        for attempt in 0..4 {
            let request = self
                .client
                .get(parsed.clone())
                .header("Accept", "application/json");
            let request = if let Some(email) = &self.email {
                request.basic_auth(email, Some(&self.token))
            } else {
                request.bearer_auth(&self.token)
            };
            let response = request
                .send()
                .await
                .context("Read-only source request failed")?;
            let status = response.status();
            if (status.as_u16() == 429 || status.is_server_error()) && attempt < 3 {
                let delay = response
                    .headers()
                    .get("retry-after")
                    .and_then(|h| h.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(1 << attempt)
                    .min(10);
                tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                continue;
            }
            ensure!(
                status.is_success(),
                "Source returned HTTP {status}; history remains incomplete"
            );
            let next = response
                .headers()
                .get("link")
                .and_then(|h| h.to_str().ok())
                .and_then(|h| h.split(',').find(|v| v.contains("rel=\"next\"")))
                .and_then(|s| s.split_once('<'))
                .and_then(|(_, s)| s.split_once('>'))
                .map(|(s, _)| s.to_string());
            let value: Value = response.json().await.context("Invalid source JSON")?;
            let next = value
                .get("next")
                .and_then(Value::as_str)
                .map(str::to_string)
                .or(next);
            return Ok((value, next));
        }
        bail!("Source retries exhausted")
    }
    async fn list(&self, url: &str, field: Option<&str>) -> Result<Vec<Value>> {
        let mut next = Some(url.to_string());
        let mut seen = BTreeSet::new();
        let mut rows = vec![];
        while let Some(url) = next.take() {
            ensure!(
                seen.insert(url.clone()) && seen.len() <= 10000,
                "Source pagination repeated or exceeded limit"
            );
            let (v, n) = self.get(&url).await?;
            let values = field
                .and_then(|f| v.get(f))
                .or_else(|| v.get("values"))
                .unwrap_or(&v)
                .as_array()
                .context("Expected a paginated source array")?;
            rows.extend(values.iter().cloned());
            next = n;
        }
        Ok(rows)
    }
}
fn text(v: &Value, path: &str) -> Option<String> {
    v.pointer(path).and_then(Value::as_str).map(str::to_string)
}
fn now() -> String {
    Utc::now().to_rfc3339()
}
fn normalized_github(s: &Source, v: &Value) -> PullRequest {
    PullRequest {
        provider: "github".into(),
        repo_key: s.repo_key(),
        number: v["number"].as_i64().unwrap_or(0),
        url: text(v, "/html_url").unwrap_or_default(),
        title: text(v, "/title").unwrap_or_default(),
        description: text(v, "/body").unwrap_or_default(),
        labels: v["labels"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|l| text(l, "/name"))
            .collect(),
        state: if !v["merged_at"].is_null() {
            "merged"
        } else if v["state"] == "closed" {
            "closed"
        } else if v["draft"] == true {
            "draft"
        } else {
            "open"
        }
        .into(),
        created_at: text(v, "/created_at"),
        updated_at: text(v, "/updated_at"),
        merged_at: text(v, "/merged_at"),
        closed_at: text(v, "/closed_at"),
        head_sha: text(v, "/head/sha"),
        merge_sha: (!v["merged_at"].is_null())
            .then(|| text(v, "/merge_commit_sha"))
            .flatten(),
        head_ref: text(v, "/head/ref"),
        base_ref: text(v, "/base/ref"),
        ..PullRequest::default()
    }
}
fn normalized_bitbucket(s: &Source, v: &Value) -> PullRequest {
    let state = text(v, "/state")
        .unwrap_or_else(|| "OPEN".into())
        .to_lowercase();
    PullRequest {
        provider: "bitbucket".into(),
        repo_key: s.repo_key(),
        number: v["id"].as_i64().unwrap_or(0),
        url: text(v, "/links/html/href").unwrap_or_default(),
        title: text(v, "/title").unwrap_or_default(),
        description: text(v, "/description").unwrap_or_default(),
        state: state.clone(),
        created_at: text(v, "/created_on"),
        updated_at: text(v, "/updated_on"),
        merged_at: None,
        closed_at: None,
        head_sha: text(v, "/source/commit/hash"),
        merge_sha: text(v, "/merge_commit/hash"),
        head_ref: text(v, "/source/branch/name"),
        base_ref: text(v, "/destination/branch/name"),
        ..PullRequest::default()
    }
}
fn mapped_intent(s: &Source, pr: &PullRequest) -> Option<String> {
    for label in &pr.labels {
        if let Some(intent) = s.intent_labels.get(&label.to_lowercase()) {
            return Some(intent.clone());
        }
    }
    // A structured template field is explicit metadata; free-form title keywords are not.
    for line in pr.description.lines() {
        if let Some((key, value)) = line.split_once(':')
            && ["change type", "type"].contains(&key.trim().to_lowercase().as_str())
        {
            let value = value.trim().to_lowercase();
            if let Some(intent) = s.intent_labels.get(&value) {
                return Some(intent.clone());
            }
        }
    }
    None
}

async fn collect_pr(conn: &Connection, s: &Source, h: &Http, v: &Value) -> Result<()> {
    let mut pr = if s.provider == "github" {
        normalized_github(s, v)
    } else {
        normalized_bitbucket(s, v)
    };
    pr.id = pr.stable_id();
    let old = conn
        .query_row(
            "SELECT record_json FROM pull_request_record WHERE id=?1",
            [&pr.id],
            |r| r.get::<_, String>(0),
        )
        .optional()?
        .map(|j| serde_json::from_str::<PullRequest>(&j))
        .transpose()?;
    if old.as_ref().is_some_and(|o| {
        o.updated_at == pr.updated_at && o.state == pr.state && o.head_sha == pr.head_sha
    }) {
        return Ok(());
    }
    let prefix = if s.provider == "github" {
        format!("{}/repos/{}/pulls/{}", s.base(), s.repository, pr.number)
    } else {
        format!(
            "{}/repositories/{}/pullrequests/{}",
            s.base(),
            s.repository,
            pr.number
        )
    };
    if s.provider == "github" {
        pr.commits = h
            .list(&format!("{prefix}/commits?per_page=100"), None)
            .await?
            .iter()
            .filter_map(|v| text(v, "/sha"))
            .collect();
        pr.files = h
            .list(&format!("{prefix}/files?per_page=100"), None)
            .await?
            .iter()
            .filter_map(|v| text(v, "/filename"))
            .collect();
        pr.review_events=h.list(&format!("{prefix}/reviews?per_page=100"),None).await?.iter().map(|v|serde_json::json!({"state":v["state"],"at":v["submitted_at"],"sha":v["commit_id"]})).collect();
    } else {
        pr.commits = h
            .list(&format!("{prefix}/commits?pagelen=100"), None)
            .await?
            .iter()
            .filter_map(|v| text(v, "/hash"))
            .collect();
        pr.files = h
            .list(&format!("{prefix}/diffstat?pagelen=100"), None)
            .await?
            .iter()
            .filter_map(|v| text(v, "/new/path").or_else(|| text(v, "/old/path")))
            .collect();
        let events = h
            .list(&format!("{prefix}/activity?pagelen=100"), None)
            .await?;
        for event in events {
            if let Some(update) = event.get("update") {
                let state = text(update, "/state").unwrap_or_default();
                let at = text(update, "/date");
                if state == "MERGED" {
                    pr.merged_at = at.clone();
                }
                if ["MERGED", "DECLINED", "SUPERSEDED"].contains(&state.as_str()) {
                    pr.closed_at = at.clone();
                }
                pr.review_events
                    .push(serde_json::json!({"state":state,"at":at}));
            } else if let Some(a) = event.get("approval") {
                pr.review_events
                    .push(serde_json::json!({"state":"approved","at":a["date"]}));
            }
        }
    }
    let id = delivery::save_pr(conn, &pr)?;
    conn.execute(
        "DELETE FROM pull_request_intent WHERE pr_id=?1 AND source IN ('mapping','classifier')",
        [&id],
    )?;
    if let Some(intent) = mapped_intent(s, &pr) {
        delivery::set_intent(conn, &id, &intent, "mapping")?;
    }
    Ok(())
}

async fn collect_prs(conn: &Connection, s: &Source, h: &Http, since: &str) -> Result<usize> {
    let mut count = 0;
    for phase in ["open", "recent"] {
        let checkpoint = format!("{}:prs:{phase}", s.name);
        let fresh = if s.provider == "github" {
            format!(
                "{}/repos/{}/pulls?state={}&sort=updated&direction=desc&per_page=100",
                s.base(),
                s.repository,
                if phase == "open" { "open" } else { "all" }
            )
        } else {
            format!(
                "{}/repositories/{}/pullrequests?state={}&sort=-updated_on&pagelen=100",
                s.base(),
                s.repository,
                if phase == "open" {
                    "OPEN"
                } else {
                    "OPEN&state=MERGED&state=DECLINED&state=SUPERSEDED"
                }
            )
        };
        let saved = conn
            .query_row(
                "SELECT cursor FROM delivery_checkpoint WHERE source_id=?1",
                [&checkpoint],
                |r| r.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten();
        let mut next = Some(saved.unwrap_or(fresh));
        let mut seen = BTreeSet::new();
        while let Some(url) = next.take() {
            ensure!(
                seen.insert(url.clone()) && seen.len() <= 10000,
                "Repeated PR pagination"
            );
            conn.execute("INSERT INTO delivery_checkpoint(source_id,cursor) VALUES(?1,?2) ON CONFLICT(source_id) DO UPDATE SET cursor=excluded.cursor,error=NULL",params![checkpoint,url])?;
            let (page, n) = h.get(&url).await?;
            let values = page
                .get("values")
                .unwrap_or(&page)
                .as_array()
                .context("Expected PR page")?;
            let mut recent = false;
            for value in values {
                let at = if s.provider == "github" {
                    text(value, "/updated_at")
                } else {
                    text(value, "/updated_on")
                };
                let in_window = at
                    .as_deref()
                    .and_then(crate::insights::structure::parse_ts)
                    .zip(crate::insights::structure::parse_ts(since))
                    .is_none_or(|(a, b)| a >= b);
                recent |= in_window;
                if phase == "open" || in_window {
                    collect_pr(conn, s, h, value).await?;
                    count += 1;
                }
            }
            next = if phase == "recent" && !recent {
                None
            } else {
                n
            };
            conn.execute(
                "UPDATE delivery_checkpoint SET cursor=?2 WHERE source_id=?1",
                params![checkpoint, next],
            )?;
        }
        conn.execute(
            "UPDATE delivery_checkpoint SET completed_at=?2,error=NULL WHERE source_id=?1",
            params![checkpoint, now()],
        )?;
    }
    // Explicit old PR references are included even outside the default backfill.
    let prefix = if s.provider == "github" {
        format!("https://github.com/{}/pull/", s.repository)
    } else {
        format!("https://bitbucket.org/{}/pull-requests/", s.repository)
    };
    let re = regex::Regex::new(&format!(r"{}(\d+)", regex::escape(&prefix)))?;
    let rows = conn
        .prepare("SELECT content FROM fact_session_message WHERE instr(content,?1)>0")?
        .query_map([&prefix], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let refs: BTreeSet<_> = rows
        .iter()
        .flat_map(|body| re.captures_iter(body).map(|c| c[1].to_string()))
        .collect();
    for number in refs {
        let id = format!("{}:{}#{}", s.provider, s.repo_key(), number);
        let known: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM pull_request_record WHERE id=?1)",
            [&id],
            |r| r.get(0),
        )?;
        if !known {
            let url = if s.provider == "github" {
                format!("{}/repos/{}/pulls/{number}", s.base(), s.repository)
            } else {
                format!(
                    "{}/repositories/{}/pullrequests/{number}",
                    s.base(),
                    s.repository
                )
            };
            let (v, _) = h.get(&url).await?;
            collect_pr(conn, s, h, &v).await?;
            count += 1;
        }
    }
    Ok(count)
}

async fn collect_deployments(
    conn: &Connection,
    s: &Source,
    h: &Http,
    since: &str,
) -> Result<usize> {
    let url = if s.provider == "github" {
        format!(
            "{}/repos/{}/deployments?per_page=100",
            s.base(),
            s.repository
        )
    } else {
        format!(
            "{}/repositories/{}/deployments?pagelen=100",
            s.base(),
            s.repository
        )
    };
    let raw = h.list(&url, None).await?;
    let mut count = 0;
    for v in raw {
        if s.provider == "github" {
            let id = v["id"].as_i64().context("Deployment has no ID")?;
            let statuses = h
                .list(
                    &format!(
                        "{}/repos/{}/deployments/{id}/statuses?per_page=100",
                        s.base(),
                        s.repository
                    ),
                    None,
                )
                .await?;
            for status in statuses {
                let at = text(&status, "/created_at").context("Deployment status has no time")?;
                if crate::insights::structure::parse_ts(&at)
                    < crate::insights::structure::parse_ts(since)
                {
                    continue;
                }
                let d = Deployment {
                    id: format!("{}:deployment:{id}:{}", s.repo_key(), status["id"]),
                    provider: s.provider.clone(),
                    repo_key: s.repo_key(),
                    artifact_sha: text(&v, "/sha").unwrap_or_default(),
                    environment: text(&v, "/environment").unwrap_or_default(),
                    status: text(&status, "/state").unwrap_or_else(|| "pending".into()),
                    live: s
                        .live_environments
                        .iter()
                        .any(|e| Some(e) == text(&v, "/environment").as_ref()),
                    occurred_at: at,
                    evidence_url: text(&status, "/log_url")
                        .filter(|s| !s.is_empty())
                        .or_else(|| text(&status, "/url"))
                        .unwrap_or_default(),
                    required_components: s.required_components.clone(),
                    evidence_source: "provider_api".into(),
                    ..Deployment::default()
                };
                delivery::save_deployment(conn, &d)?;
                count += 1;
            }
        } else {
            let at = text(&v, "/state/completion_date").or_else(|| text(&v, "/state/start_date"));
            let Some(at) = at else {
                continue;
            };
            if crate::insights::structure::parse_ts(&at)
                < crate::insights::structure::parse_ts(since)
            {
                continue;
            }
            let native = text(&v, "/state/status/name")
                .or_else(|| text(&v, "/state/name"))
                .unwrap_or_default();
            let status = match native.as_str() {
                "SUCCESSFUL" => "success",
                "FAILED" => "failed",
                "STOPPED" => "cancelled",
                "IN_PROGRESS" => "in_progress",
                _ => "pending",
            };
            let env = text(&v, "/environment/name").unwrap_or_default();
            let d = Deployment {
                id: format!(
                    "{}:deployment:{}:{native}",
                    s.repo_key(),
                    text(&v, "/uuid").unwrap_or_default()
                ),
                provider: s.provider.clone(),
                repo_key: s.repo_key(),
                artifact_sha: text(&v, "/release/commit/hash").unwrap_or_default(),
                environment: env.clone(),
                status: status.into(),
                live: s.live_environments.contains(&env),
                occurred_at: at,
                evidence_url: text(&v, "/state/url")
                    .or_else(|| text(&v, "/release/url"))
                    .unwrap_or_default(),
                required_components: s.required_components.clone(),
                evidence_source: "provider_api".into(),
                ..Deployment::default()
            };
            delivery::save_deployment(conn, &d)?;
            count += 1;
        }
    }
    for job in &s.deployment_jobs {
        let runs=h.list(&format!("{}/repos/{}/actions/workflows/{}/runs?status=completed&created=%3E%3D{}&per_page=100",s.base(),s.repository,job.workflow_id,&since[..10]),Some("workflow_runs")).await?;
        for run in runs {
            let id = run["id"].as_i64().context("Run has no ID")?;
            let jobs = h
                .list(
                    &format!(
                        "{}/repos/{}/actions/runs/{id}/jobs?per_page=100",
                        s.base(),
                        s.repository
                    ),
                    Some("jobs"),
                )
                .await?;
            for j in jobs
                .iter()
                .filter(|j| j["name"].as_str() == Some(job.job_name.as_str()))
            {
                let status = match j["conclusion"].as_str() {
                    Some("success") => "success",
                    Some("failure" | "timed_out") => "failed",
                    Some("cancelled") => "cancelled",
                    _ => "pending",
                };
                if let Some(at) = text(j, "/completed_at") {
                    let d = Deployment {
                        id: format!("{}:job:{id}:{}", s.repo_key(), j["id"]),
                        provider: s.provider.clone(),
                        repo_key: s.repo_key(),
                        artifact_sha: text(&run, "/head_sha").unwrap_or_default(),
                        environment: job.environment.clone(),
                        component: job.component.clone(),
                        status: status.into(),
                        live: s.live_environments.contains(&job.environment),
                        occurred_at: at,
                        evidence_url: text(j, "/html_url").unwrap_or_default(),
                        required_components: s.required_components.clone(),
                        evidence_source: "provider_api".into(),
                        ..Deployment::default()
                    };
                    delivery::save_deployment(conn, &d)?;
                    count += 1;
                }
            }
        }
    }
    // Persist negative checks as well; an interrupted scan resumes without repeating requests.
    let prs = delivery::prs(conn)?;
    let deployments = delivery::deployments(conn)?;
    let mut checks = 0;
    for d in deployments
        .iter()
        .filter(|d| d.repo_key == s.repo_key() && d.status == "success")
    {
        for pr in prs.iter().filter(|p| p.repo_key == s.repo_key()) {
            let Some(merge) = &pr.merge_sha else {
                continue;
            };
            if d.artifact_sha == *merge {
                continue;
            }
            let after = pr
                .merged_at
                .as_deref()
                .and_then(crate::insights::structure::parse_ts)
                .zip(crate::insights::structure::parse_ts(&d.occurred_at))
                .is_some_and(|(a, b)| b >= a);
            if !after {
                continue;
            }
            let cached:Option<(bool,String)>=conn.query_row("SELECT included,evidence FROM deployment_ancestry_check WHERE repo_key=?1 AND merge_sha=?2 AND artifact_sha=?3",params![s.repo_key(),merge,d.artifact_sha],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
            let (includes, url) = if let Some(cached) = cached {
                cached
            } else {
                ensure!(
                    checks < 256,
                    "Ancestry collection paused after 256 checks; sync again to resume retained progress"
                );
                checks += 1;
                let url = if s.provider == "github" {
                    format!(
                        "{}/repos/{}/compare/{merge}...{}",
                        s.base(),
                        s.repository,
                        d.artifact_sha
                    )
                } else {
                    format!(
                        "{}/repositories/{}/merge-base/{}..{merge}",
                        s.base(),
                        s.repository,
                        d.artifact_sha
                    )
                };
                let (v, _) = h.get(&url).await?;
                let includes = if s.provider == "github" {
                    [Some("ahead"), Some("identical")].contains(&v["status"].as_str())
                } else {
                    v["hash"].as_str() == Some(merge.as_str())
                };
                conn.execute(
                    "INSERT OR REPLACE INTO deployment_ancestry_check VALUES(?1,?2,?3,?4,?5)",
                    params![s.repo_key(), merge, d.artifact_sha, includes, url],
                )?;
                (includes, url)
            };
            if includes {
                conn.execute("INSERT OR IGNORE INTO deployment_inclusion VALUES(?1,?2,'provider_ancestry',?3)",params![d.id,pr.id,url])?;
            }
        }
    }
    Ok(count)
}

pub fn sync(conn: &Connection, name: Option<&str>) -> Result<Vec<serde_json::Value>> {
    sync_for(conn, name, crate::app::AppContext::VCA)
}
pub fn sync_for(
    conn: &Connection,
    name: Option<&str>,
    context: crate::app::AppContext,
) -> Result<Vec<serde_json::Value>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut reports = vec![];
    for s in sources(conn)?
        .into_iter()
        .filter(|s| s.enabled && name.is_none_or(|n| n == s.name))
    {
        s.validate()?;
        let scan_started = now();
        let since = (Utc::now() - Duration::days(56)).to_rfc3339();
        let result = runtime.block_on(async {
            let h = Http::new(&s, context)?;
            let prs = collect_prs(conn, &s, &h, &since).await?;
            let releases = collect_deployments(conn, &s, &h, &since).await?;
            Ok::<_, anyhow::Error>((prs, releases))
        });
        let (error, prs, releases) = match result {
            Ok((p, d)) => (None, p, d),
            Err(e) => (Some(e.to_string()), 0, 0),
        };
        observation::record(
            conn,
            &SourceCoverage {
                source_id: format!("delivery:{}", s.name),
                provider: s.provider.clone(),
                repo_root: Some(s.repo_key()),
                observed_from: Some(since),
                observed_through: error.is_none().then_some(scan_started),
                ingested_at: now(),
                complete: error.is_none(),
                kind: "pull_requests_and_deployments".into(),
                error: error.clone(),
            },
        )?;
        reports.push(serde_json::json!({"source":s.name,"prs_collected":prs,"deployment_events_collected":releases,"error":error}));
    }
    delivery::refresh_links(conn)?;
    Ok(reports)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{BufRead, BufReader, Write},
        net::TcpListener,
    };
    fn server(
        responses: Vec<(String, u16, Value)>,
    ) -> (Http, std::thread::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let handle = std::thread::spawn(move || {
            let mut paths = vec![];
            for (expected, status, body) in responses {
                let (stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream);
                let mut first = String::new();
                reader.read_line(&mut first).unwrap();
                let path = first.split_whitespace().nth(1).unwrap().to_string();
                assert!(path.starts_with(&expected), "{path} != {expected}");
                paths.push(path);
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                }
                let body = body.to_string();
                write!(reader.into_inner(),"HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).unwrap();
            }
            paths
        });
        (
            Http {
                client: reqwest::Client::builder().no_proxy().build().unwrap(),
                base: reqwest::Url::parse(&base).unwrap(),
                token: "test-only".into(),
                email: None,
            },
            handle,
        )
    }
    #[test]
    fn provider_pr_shapes_keep_merge_times_and_intent_provenance() {
        let source = Source {
            repository: "org/repo".into(),
            ..Default::default()
        };
        let p = normalized_github(
            &source,
            &serde_json::json!({"number":3,"state":"closed","merged_at":"2026-10-01T12:00:00Z","merge_commit_sha":"squash","labels":[{"name":"bug"}]}),
        );
        assert_eq!(p.state, "merged");
        assert_eq!(p.merge_sha.as_deref(), Some("squash"));
        assert_eq!(mapped_intent(&source, &p).as_deref(), Some("bug_fix"));
        let bb = normalized_bitbucket(
            &source,
            &serde_json::json!({"id":4,"state":"MERGED","updated_on":"2026-10-02T12:00:00Z","merge_commit":{"hash":"rebase"}}),
        );
        assert!(bb.merged_at.is_none(), "updated_on cannot prove merge time");
    }
    #[test]
    fn paginated_reads_retry_rate_limits_and_refuse_cross_origin() -> Result<()> {
        let (h, server) = server(vec![
            ("/list".into(), 429, serde_json::json!({})),
            (
                "/list".into(),
                200,
                serde_json::json!({"values":[{"id":1}]}),
            ),
        ]);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let rows = rt.block_on(h.list(
            &format!("{}/list", h.base.as_str().trim_end_matches('/')),
            None,
        ))?;
        assert_eq!(rows.len(), 1);
        assert!(
            rt.block_on(h.get("https://untrusted.example/steal"))
                .is_err()
        );
        assert_eq!(server.join().unwrap().len(), 2);
        Ok(())
    }
    #[test]
    fn interrupted_pr_collection_retains_records_and_cursor_without_commit_matches() -> Result<()> {
        let c = Connection::open_in_memory()?;
        crate::db::init_app_schema(&c)?;
        let p = serde_json::json!({"number":1,"html_url":"https://github.com/org/repo/pull/1","state":"open","updated_at":"2026-10-01T00:00:00Z"});
        let (h, server) = server(vec![
            (
                "/repos/org/repo/pulls?state=open".into(),
                200,
                serde_json::json!([p.clone()]),
            ),
            (
                "/repos/org/repo/pulls/1/commits".into(),
                200,
                serde_json::json!([{"sha":"commit"}]),
            ),
            (
                "/repos/org/repo/pulls/1/files".into(),
                200,
                serde_json::json!([{"filename":"src/lib.rs"}]),
            ),
            (
                "/repos/org/repo/pulls/1/reviews".into(),
                200,
                serde_json::json!([]),
            ),
            (
                "/repos/org/repo/pulls?state=all".into(),
                403,
                serde_json::json!({}),
            ),
            (
                "/repos/org/repo/pulls?state=open".into(),
                200,
                serde_json::json!([p.clone()]),
            ),
            (
                "/repos/org/repo/pulls?state=all".into(),
                200,
                serde_json::json!([p]),
            ),
        ]);
        let s = Source {
            name: "fixture".into(),
            repository: "org/repo".into(),
            repo_root: "/repo".into(),
            api_base: Some(h.base.as_str().trim_end_matches('/').into()),
            ..Default::default()
        };
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        assert!(
            rt.block_on(collect_prs(&c, &s, &h, "2026-09-01T00:00:00Z"))
                .is_err()
        );
        assert_eq!(delivery::prs(&c)?.len(), 1);
        rt.block_on(collect_prs(&c, &s, &h, "2026-09-01T00:00:00Z"))?;
        let rows = delivery::evidence(&c)?;
        assert_eq!(rows[0].pr.files, vec!["src/lib.rs"]);
        assert!(rows[0].work_ids.is_empty());
        assert_eq!(server.join().unwrap().len(), 7);
        Ok(())
    }
}
