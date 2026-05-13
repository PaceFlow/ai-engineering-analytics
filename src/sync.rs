use anyhow::{Result, anyhow, bail};
use reqwest::{Client, RequestBuilder};
use rusqlite::{Connection, OptionalExtension, params};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::path_utils::detect_repo_root;

pub const SYNC_BASE_URL_ENV_VAR: &str = "PACEFLOW_SYNC_BASE_URL";
pub const SYNC_ORGANIZATION_ID_ENV_VAR: &str = "PACEFLOW_SYNC_ORGANIZATION_ID";
pub const SYNC_TOKEN_ENV_VAR: &str = "PACEFLOW_SYNC_TOKEN";
const SYNC_CONFIG_FILE_NAME: &str = "sync_config.json";
const DEFAULT_BATCH_SIZE: usize = 500;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SavedSyncConfig {
    pub base_url: String,
    pub organization_id: String,
    pub organization_name: Option<String>,
    #[serde(default)]
    pub member_email: Option<String>,
    pub token: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncConfigSource {
    Environment,
    Saved,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSyncConfig {
    pub base_url: String,
    pub base_url_source: SyncConfigSource,
    pub organization_id: String,
    pub organization_id_source: SyncConfigSource,
    pub organization_name: Option<String>,
    pub member_email: Option<String>,
    pub token: String,
    pub token_source: SyncConfigSource,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncOrganization {
    pub id: String,
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountMeResponse {
    #[serde(default)]
    pub organizations: Vec<SyncOrganization>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PersonSyncLinkVerifyResponse {
    pub token: String,
    pub organization_id: String,
    #[serde(default)]
    pub organization_name: Option<String>,
    pub member_email: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RemoteSyncEvent {
    pub event_type: String,
    pub event_key: String,
    pub repo_key: Option<String>,
    pub member_email: Option<String>,
    pub device_id: Option<String>,
    pub occurred_at: Option<String>,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct SyncClientMetadata {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SyncPushRequest {
    pub client: SyncClientMetadata,
    pub batch: Vec<RemoteSyncEvent>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SyncPushResponse {
    pub accepted: usize,
    pub rejected: usize,
    pub checkpoint: String,
    #[serde(default)]
    pub errors: Vec<SyncPushError>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SyncPushError {
    pub index: Option<usize>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RemoteSyncStatus {
    pub organization_id: String,
    #[serde(default)]
    pub organization_name: Option<String>,
    pub total_events: usize,
    #[serde(default)]
    pub last_event_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct SyncRepositoryListResponse {
    pub organization_id: String,
    #[serde(default)]
    pub repositories: Vec<SyncRepositoryRef>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct SyncRepositoryRef {
    pub repo_key: String,
    pub provider: String,
    pub repo_name: String,
    #[serde(default)]
    pub last_seen_at: Option<String>,
    #[serde(default)]
    pub sources: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LocalSyncEvent {
    pub event_type: String,
    pub event_key: String,
    pub repo_key: Option<String>,
    pub member_email: Option<String>,
    pub device_id: Option<String>,
    pub occurred_at: Option<String>,
    pub payload: Value,
    pub content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncScope {
    pub repo_root: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncRunState {
    pub last_successful_push_at: String,
    pub last_server_checkpoint: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EligibleSyncEvents {
    pub eligible: Vec<LocalSyncEvent>,
    pub skipped: Vec<LocalSyncEvent>,
    pub skipped_repo_keys: BTreeSet<String>,
}

pub struct SyncApiClient {
    client: Client,
    base_url: String,
    token: Option<String>,
}

impl SyncApiClient {
    pub fn new(base_url: impl Into<String>, token: Option<String>) -> Result<Self> {
        // Disable idle connection pooling to avoid reqwest/hyper "connection closed before
        // message completed" errors when an HTTP keep-alive connection goes stale during
        // interactive pauses (for example, while the user types a verification code between
        // person-link request and verify calls).
        let client = Client::builder()
            .pool_max_idle_per_host(0)
            .connect_timeout(Duration::from_secs(15))
            .build()?;
        Ok(Self {
            client,
            base_url: normalize_base_url(&base_url.into())?,
            token: token.map(normalized_non_empty).transpose()?,
        })
    }

    pub async fn request_person_link(&self, organization_id: &str, email: &str) -> Result<()> {
        let _: serde_json::Value = self
            .send_json(
                self.client
                    .post(self.url(&format!(
                        "/api/v1/organizations/{organization_id}/sync/person-link/request"
                    )))
                    .json(&json!({ "email": email })),
            )
            .await?;
        Ok(())
    }

    pub async fn verify_person_link(
        &self,
        organization_id: &str,
        email: &str,
        code: &str,
        device_id: &str,
    ) -> Result<PersonSyncLinkVerifyResponse> {
        self.send_json(
            self.client
                .post(self.url(&format!(
                    "/api/v1/organizations/{organization_id}/sync/person-link/verify"
                )))
                .json(&json!({ "email": email, "code": code, "device_id": device_id })),
        )
        .await
    }

    pub async fn push_events(
        &self,
        organization_id: &str,
        request: &SyncPushRequest,
    ) -> Result<SyncPushResponse> {
        self.send_json(
            self.with_auth(self.client.post(self.url(&format!(
                "/api/v1/organizations/{organization_id}/sync/events"
            ))))
            .json(request),
        )
        .await
    }

    pub async fn status(&self, organization_id: &str) -> Result<RemoteSyncStatus> {
        self.send_json(self.with_auth(self.client.get(self.url(&format!(
            "/api/v1/organizations/{organization_id}/sync/status"
        )))))
        .await
    }

    pub async fn repositories(&self, organization_id: &str) -> Result<SyncRepositoryListResponse> {
        self.send_json(self.with_auth(self.client.get(self.url(&format!(
            "/api/v1/organizations/{organization_id}/sync/repositories"
        )))))
        .await
    }

    fn with_auth(&self, request: RequestBuilder) -> RequestBuilder {
        match &self.token {
            Some(token) => request.bearer_auth(token),
            None => request,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    async fn send_json<T: DeserializeOwned>(&self, request: RequestBuilder) -> Result<T> {
        let response = request.send().await?;
        let status = response.status();
        if !status.is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| String::from("(failed to read response body)"));
            bail!("sync request failed: {} {}", status, body.trim());
        }
        Ok(response.json().await?)
    }
}

impl LocalSyncEvent {
    pub fn to_remote_event(&self) -> RemoteSyncEvent {
        RemoteSyncEvent {
            event_type: self.event_type.clone(),
            event_key: self.event_key.clone(),
            repo_key: self.repo_key.clone(),
            member_email: self.member_email.clone(),
            device_id: self.device_id.clone(),
            occurred_at: self.occurred_at.clone(),
            payload: self.payload.clone(),
        }
    }
}

pub fn sync_config_path() -> Result<PathBuf> {
    let home = paceflow_home_dir()?;
    let app_dir = home.join(".paceflow");
    fs::create_dir_all(&app_dir)?;
    Ok(app_dir.join(SYNC_CONFIG_FILE_NAME))
}

pub fn normalized_base_url(raw: &str) -> Result<String> {
    normalize_base_url(raw)
}

pub fn load_saved_sync_config() -> Result<Option<SavedSyncConfig>> {
    let path = sync_config_path()?;
    match fs::read_to_string(path) {
        Ok(contents) => Ok(Some(serde_json::from_str(&contents)?)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

pub fn save_sync_config(config: &SavedSyncConfig) -> Result<PathBuf> {
    let path = sync_config_path()?;
    let data = serde_json::to_vec_pretty(config)?;
    write_secret_file(&path, &data)?;
    Ok(path)
}

pub fn delete_saved_sync_config() -> Result<bool> {
    let path = sync_config_path()?;
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err.into()),
    }
}

pub fn resolved_sync_config() -> Result<Option<ResolvedSyncConfig>> {
    let saved = load_saved_sync_config()?;

    let (base_url, base_url_source) = resolve_sync_field(
        SYNC_BASE_URL_ENV_VAR,
        saved.as_ref().map(|cfg| cfg.base_url.as_str()),
    )?;
    let (organization_id, organization_id_source) = resolve_sync_field(
        SYNC_ORGANIZATION_ID_ENV_VAR,
        saved.as_ref().map(|cfg| cfg.organization_id.as_str()),
    )?;
    let (token, token_source) = resolve_sync_field(
        SYNC_TOKEN_ENV_VAR,
        saved.as_ref().map(|cfg| cfg.token.as_str()),
    )?;

    let Some(base_url) = base_url else {
        return Ok(None);
    };
    let Some(organization_id) = organization_id else {
        return Ok(None);
    };
    let Some(token) = token else {
        return Ok(None);
    };

    let organization_name = match (organization_id_source, saved.as_ref()) {
        (SyncConfigSource::Saved, Some(saved)) => saved.organization_name.clone(),
        (_, Some(saved)) if saved.organization_id == organization_id => {
            saved.organization_name.clone()
        }
        _ => None,
    };

    Ok(Some(ResolvedSyncConfig {
        base_url: normalize_base_url(&base_url)?,
        base_url_source,
        organization_id,
        organization_id_source,
        organization_name,
        member_email: saved.and_then(|cfg| cfg.member_email.clone()),
        token,
        token_source,
    }))
}

pub fn env_override_keys() -> &'static [&'static str] {
    &[
        SYNC_BASE_URL_ENV_VAR,
        SYNC_ORGANIZATION_ID_ENV_VAR,
        SYNC_TOKEN_ENV_VAR,
    ]
}

pub fn resolve_sync_scope(repo: Option<&str>, all_projects: bool) -> Result<SyncScope> {
    if all_projects {
        return Ok(SyncScope { repo_root: None });
    }

    let root = if let Some(repo) = repo {
        let candidate = PathBuf::from(repo);
        resolve_repo_root_from_path(&candidate)?
    } else {
        let cwd = env::current_dir()?;
        resolve_repo_root_from_path(&cwd)?
    };

    Ok(SyncScope {
        repo_root: Some(root.to_string_lossy().to_string()),
    })
}

pub fn collect_sync_events(conn: &Connection, scope: &SyncScope) -> Result<Vec<LocalSyncEvent>> {
    let mut events = Vec::new();
    collect_session_quality_events(conn, scope, &mut events)?;
    collect_session_productivity_events(conn, scope, &mut events)?;
    collect_session_cost_events(conn, scope, &mut events)?;
    collect_commit_outcome_events(conn, scope, &mut events)?;
    collect_commit_churn_events(conn, scope, &mut events)?;
    collect_commit_bug_signal_events(conn, scope, &mut events)?;
    collect_commit_session_events(conn, scope, &mut events)?;
    collect_task_commit_events(conn, scope, &mut events)?;
    collect_task_session_events(conn, scope, &mut events)?;
    collect_commit_pr_outcome_events(conn, scope, &mut events)?;
    Ok(events)
}

pub fn pending_sync_events(
    conn: &Connection,
    organization_id: &str,
    scope: &SyncScope,
) -> Result<Vec<LocalSyncEvent>> {
    collect_sync_events(conn, scope)?
        .into_iter()
        .filter(|event| {
            match synced_content_hash(conn, organization_id, &event.event_type, &event.event_key) {
                Ok(Some(hash)) => hash != event.content_hash,
                Ok(None) => true,
                Err(_) => true,
            }
        })
        .collect::<Vec<_>>()
        .pipe(Ok)
}

pub fn pending_sync_counts(
    conn: &Connection,
    organization_id: &str,
    scope: &SyncScope,
) -> Result<BTreeMap<String, usize>> {
    let mut counts = BTreeMap::new();
    for event in pending_sync_events(conn, organization_id, scope)? {
        *counts.entry(event.event_type.clone()).or_default() += 1;
    }
    Ok(counts)
}

pub fn grouped_event_counts(events: &[LocalSyncEvent]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        *counts.entry(event.event_type.clone()).or_default() += 1;
    }
    counts
}

pub fn partition_eligible_sync_events(
    events: Vec<LocalSyncEvent>,
    allowlist: &SyncRepositoryListResponse,
) -> EligibleSyncEvents {
    let allowed_repo_keys = allowlist
        .repositories
        .iter()
        .filter_map(|repository| repo_match_key(&repository.repo_key))
        .collect::<BTreeSet<_>>();
    let mut eligible = Vec::new();
    let mut skipped = Vec::new();
    let mut skipped_repo_keys = BTreeSet::new();

    for event in events {
        let is_allowed = event
            .repo_key
            .as_deref()
            .and_then(repo_match_key)
            .is_some_and(|repo_key| allowed_repo_keys.contains(&repo_key));
        if is_allowed {
            eligible.push(event);
        } else {
            if let Some(repo_key) = event
                .repo_key
                .as_ref()
                .filter(|repo_key| !repo_key.is_empty())
            {
                skipped_repo_keys.insert(repo_key.clone());
            }
            skipped.push(event);
        }
    }

    EligibleSyncEvents {
        eligible,
        skipped,
        skipped_repo_keys,
    }
}

pub fn mark_synced_events(
    conn: &mut Connection,
    organization_id: &str,
    events: &[LocalSyncEvent],
    checkpoint: &str,
) -> Result<()> {
    let tx = conn.transaction()?;
    let now = chrono::Utc::now().to_rfc3339();
    for event in events {
        tx.execute(
            "INSERT INTO fact_sync_event_state (
                organization_id,
                event_type,
                event_key,
                content_hash,
                last_synced_at,
                last_server_checkpoint
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(organization_id, event_type, event_key) DO UPDATE SET
                content_hash = excluded.content_hash,
                last_synced_at = excluded.last_synced_at,
                last_server_checkpoint = excluded.last_server_checkpoint",
            params![
                organization_id,
                event.event_type,
                event.event_key,
                event.content_hash,
                now,
                checkpoint,
            ],
        )?;
    }

    tx.execute(
        "INSERT INTO fact_sync_run_state (
            organization_id,
            last_successful_push_at,
            last_server_checkpoint
         ) VALUES (?1, ?2, ?3)
         ON CONFLICT(organization_id) DO UPDATE SET
            last_successful_push_at = excluded.last_successful_push_at,
            last_server_checkpoint = excluded.last_server_checkpoint",
        params![organization_id, now, checkpoint],
    )?;
    tx.commit()?;
    Ok(())
}

pub fn reset_local_sync_state(conn: &Connection) -> Result<()> {
    conn.execute("DELETE FROM fact_sync_event_state", [])?;
    conn.execute("DELETE FROM fact_sync_run_state", [])?;
    Ok(())
}

pub fn last_sync_run_state(
    conn: &Connection,
    organization_id: &str,
) -> Result<Option<SyncRunState>> {
    conn.query_row(
        "SELECT last_successful_push_at, last_server_checkpoint
         FROM fact_sync_run_state
         WHERE organization_id = ?1",
        params![organization_id],
        |row| {
            Ok(SyncRunState {
                last_successful_push_at: row.get(0)?,
                last_server_checkpoint: row.get(1)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

pub fn make_push_request(events: &[LocalSyncEvent]) -> SyncPushRequest {
    SyncPushRequest {
        client: SyncClientMetadata {
            name: "paceflow".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        },
        batch: events.iter().map(LocalSyncEvent::to_remote_event).collect(),
    }
}

pub fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}

impl<T> Pipe for T {}

fn collect_session_quality_events(
    conn: &Connection,
    scope: &SyncScope,
    output: &mut Vec<LocalSyncEvent>,
) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT repo_key, member_email, MAX(device_id), provider, session_id, MAX(model_name), MIN(started_at), MAX(ended_at),
                MAX(user_turn_count), MAX(debug_loop_flag), MAX(mid_session_error_paste_flag), MAX(accepted_output_flag),
                MIN(first_accepted_change_at), MIN(minutes_to_first_accepted_change), MAX(session_commit_within_4h_flag)
         FROM event_session_quality
         WHERE repo_key IS NOT NULL AND repo_key != ''
           AND (?1 IS NULL OR repo_root = ?1)
         GROUP BY repo_key, member_email, provider, session_id",
    )?;
    let rows = stmt.query_map(params![scope.repo_root.as_deref()], |row| {
        let repo_key: String = row.get(0)?;
        let member_email: String = row.get(1)?;
        let provider: String = row.get(3)?;
        let session_id: String = row.get(4)?;
        Ok(build_local_sync_event(
            "event_session_quality",
            format!(
                "{}|{}|{}|{}",
                repo_key,
                stable_key_part(Some(member_email.clone())),
                provider,
                session_id
            ),
            Some(repo_key),
            Some(member_email),
            row.get(2)?,
            row.get(6)?,
            json!({
                "provider": provider,
                "session_id": session_id,
                "model_name": row.get::<_, Option<String>>(5)?,
                "started_at": row.get::<_, Option<String>>(6)?,
                "ended_at": row.get::<_, Option<String>>(7)?,
                "user_turn_count": row.get::<_, i64>(8)?,
                "debug_loop_flag": row.get::<_, i64>(9)?,
                "mid_session_error_paste_flag": row.get::<_, i64>(10)?,
                "accepted_output_flag": row.get::<_, i64>(11)?,
                "first_accepted_change_at": row.get::<_, Option<String>>(12)?,
                "minutes_to_first_accepted_change": row.get::<_, Option<f64>>(13)?,
                "session_commit_within_4h_flag": row.get::<_, i64>(14)?,
            }),
        ))
    })?;
    append_query_rows(rows, output)
}

fn collect_session_productivity_events(
    conn: &Connection,
    scope: &SyncScope,
    output: &mut Vec<LocalSyncEvent>,
) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT repo_key, member_email, MAX(device_id), provider, session_id, MAX(model_name), MIN(started_at), MAX(ended_at),
                MAX(accepted_lines_added), MAX(accepted_lines_removed), MAX(accepted_total_changed_lines), MAX(user_word_count)
         FROM event_session_productivity
         WHERE repo_key IS NOT NULL AND repo_key != ''
           AND (?1 IS NULL OR repo_root = ?1)
         GROUP BY repo_key, member_email, provider, session_id",
    )?;
    let rows = stmt.query_map(params![scope.repo_root.as_deref()], |row| {
        let repo_key: String = row.get(0)?;
        let member_email: String = row.get(1)?;
        let provider: String = row.get(3)?;
        let session_id: String = row.get(4)?;
        Ok(build_local_sync_event(
            "event_session_productivity",
            format!(
                "{}|{}|{}|{}",
                repo_key,
                stable_key_part(Some(member_email.clone())),
                provider,
                session_id
            ),
            Some(repo_key),
            Some(member_email),
            row.get(2)?,
            row.get(6)?,
            json!({
                "provider": provider,
                "session_id": session_id,
                "model_name": row.get::<_, Option<String>>(5)?,
                "started_at": row.get::<_, Option<String>>(6)?,
                "ended_at": row.get::<_, Option<String>>(7)?,
                "accepted_lines_added": row.get::<_, i64>(8)?,
                "accepted_lines_removed": row.get::<_, i64>(9)?,
                "accepted_total_changed_lines": row.get::<_, i64>(10)?,
                "user_word_count": row.get::<_, i64>(11)?,
            }),
        ))
    })?;
    append_query_rows(rows, output)
}

fn collect_session_cost_events(
    conn: &Connection,
    scope: &SyncScope,
    output: &mut Vec<LocalSyncEvent>,
) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT repo_key, member_email, MAX(device_id), provider, session_id, MAX(model_name), MIN(started_at), MAX(ended_at),
                MAX(accepted_total_changed_lines), MAX(accepted_output_flag), MAX(input_tokens), MAX(cached_input_tokens),
                MAX(cache_creation_tokens), MAX(output_tokens), MAX(reasoning_tokens), MAX(total_tokens), MAX(estimated_cost_usd),
                MAX(actual_cost_usd), MAX(cost_source)
         FROM event_session_cost
         WHERE repo_key IS NOT NULL AND repo_key != ''
           AND (?1 IS NULL OR repo_root = ?1)
         GROUP BY repo_key, member_email, provider, session_id",
    )?;
    let rows = stmt.query_map(params![scope.repo_root.as_deref()], |row| {
        let repo_key: String = row.get(0)?;
        let member_email: String = row.get(1)?;
        let provider: String = row.get(3)?;
        let session_id: String = row.get(4)?;
        Ok(build_local_sync_event(
            "event_session_cost",
            format!(
                "{}|{}|{}|{}",
                repo_key,
                stable_key_part(Some(member_email.clone())),
                provider,
                session_id
            ),
            Some(repo_key),
            Some(member_email),
            row.get(2)?,
            row.get(6)?,
            json!({
                "provider": provider,
                "session_id": session_id,
                "model_name": row.get::<_, Option<String>>(5)?,
                "started_at": row.get::<_, Option<String>>(6)?,
                "ended_at": row.get::<_, Option<String>>(7)?,
                "accepted_total_changed_lines": row.get::<_, i64>(8)?,
                "accepted_output_flag": row.get::<_, i64>(9)?,
                "input_tokens": row.get::<_, i64>(10)?,
                "cached_input_tokens": row.get::<_, i64>(11)?,
                "cache_creation_tokens": row.get::<_, i64>(12)?,
                "output_tokens": row.get::<_, i64>(13)?,
                "reasoning_tokens": row.get::<_, i64>(14)?,
                "total_tokens": row.get::<_, i64>(15)?,
                "estimated_cost_usd": row.get::<_, Option<f64>>(16)?,
                "actual_cost_usd": row.get::<_, Option<f64>>(17)?,
                "cost_source": row.get::<_, String>(18)?,
            }),
        ))
    })?;
    append_query_rows(rows, output)
}

fn collect_commit_outcome_events(
    conn: &Connection,
    scope: &SyncScope,
    output: &mut Vec<LocalSyncEvent>,
) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT o.repo_key, o.commit_sha, MIN(o.commit_time), MAX(o.mainline_reached_at), MAX(o.heavy_ai_flag),
                MAX(o.merged_to_mainline_flag), MAX(o.reverted_later_flag), MAX(o.total_matched_ai_lines),
                MAX(o.commit_total_changed_lines), MAX(COALESCE(c.total_added, 0)), MAX(COALESCE(c.total_removed, 0))
         FROM event_commit_outcome o
         LEFT JOIN fact_commit c
           ON c.repo_root = o.repo_root
          AND c.commit_sha = o.commit_sha
         WHERE o.repo_key IS NOT NULL AND o.repo_key != ''
           AND (?1 IS NULL OR o.repo_root = ?1)
         GROUP BY o.repo_key, o.commit_sha",
    )?;
    let rows = stmt.query_map(params![scope.repo_root.as_deref()], |row| {
        let repo_key: String = row.get(0)?;
        let commit_sha: String = row.get(1)?;
        Ok(build_local_sync_event(
            "event_commit_outcome",
            format!("{repo_key}|{commit_sha}"),
            Some(repo_key),
            None,
            None,
            row.get(2)?,
            json!({
                "commit_sha": commit_sha,
                "commit_time": row.get::<_, String>(2)?,
                "mainline_reached_at": row.get::<_, Option<String>>(3)?,
                "heavy_ai_flag": row.get::<_, i64>(4)?,
                "merged_to_mainline_flag": row.get::<_, i64>(5)?,
                "reverted_later_flag": row.get::<_, i64>(6)?,
                "total_matched_ai_lines": row.get::<_, i64>(7)?,
                "commit_total_changed_lines": row.get::<_, i64>(8)?,
                "lines_added": row.get::<_, i64>(9)?,
                "lines_removed": row.get::<_, i64>(10)?,
            }),
        ))
    })?;
    append_query_rows(rows, output)
}

fn collect_commit_churn_events(
    conn: &Connection,
    scope: &SyncScope,
    output: &mut Vec<LocalSyncEvent>,
) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT repo_key, commit_sha, MAX(ai_added_lines_reaching_mainline),
                MAX(ai_added_lines_removed_within_window), MAX(churn_window_days)
         FROM event_commit_churn
         WHERE repo_key IS NOT NULL AND repo_key != ''
           AND (?1 IS NULL OR repo_root = ?1)
         GROUP BY repo_key, commit_sha",
    )?;
    let rows = stmt.query_map(params![scope.repo_root.as_deref()], |row| {
        let repo_key: String = row.get(0)?;
        let commit_sha: String = row.get(1)?;
        Ok(build_local_sync_event(
            "event_commit_churn",
            format!("{repo_key}|{commit_sha}"),
            Some(repo_key),
            None,
            None,
            None,
            json!({
                "commit_sha": commit_sha,
                "ai_added_lines_reaching_mainline": row.get::<_, i64>(2)?,
                "ai_added_lines_removed_within_window": row.get::<_, i64>(3)?,
                "churn_window_days": row.get::<_, i64>(4)?,
            }),
        ))
    })?;
    append_query_rows(rows, output)
}

fn collect_commit_bug_signal_events(
    conn: &Connection,
    scope: &SyncScope,
    output: &mut Vec<LocalSyncEvent>,
) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT repo_key, commit_sha, MAX(bug_after_merge_flag), MAX(first_bug_signal_commit_sha),
                MIN(first_bug_signal_commit_time), MAX(bug_signal_count), MAX(window_days), MAX(signal_source)
         FROM event_commit_bug_signal
         WHERE repo_key IS NOT NULL AND repo_key != ''
           AND (?1 IS NULL OR repo_root = ?1)
         GROUP BY repo_key, commit_sha",
    )?;
    let rows = stmt.query_map(params![scope.repo_root.as_deref()], |row| {
        let repo_key: String = row.get(0)?;
        let commit_sha: String = row.get(1)?;
        Ok(build_local_sync_event(
            "event_commit_bug_signal",
            format!("{repo_key}|{commit_sha}"),
            Some(repo_key),
            None,
            None,
            row.get(4)?,
            json!({
                "commit_sha": commit_sha,
                "bug_after_merge_flag": row.get::<_, i64>(2)?,
                "first_bug_signal_commit_sha": row.get::<_, Option<String>>(3)?,
                "first_bug_signal_commit_time": row.get::<_, Option<String>>(4)?,
                "bug_signal_count": row.get::<_, i64>(5)?,
                "window_days": row.get::<_, i64>(6)?,
                "signal_source": row.get::<_, String>(7)?,
            }),
        ))
    })?;
    append_query_rows(rows, output)
}

fn collect_commit_session_events(
    conn: &Connection,
    scope: &SyncScope,
    output: &mut Vec<LocalSyncEvent>,
) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT repo_key, member_email, MAX(device_id), provider, session_id, commit_sha, MIN(commit_time),
                MAX(model_name), MAX(matched_lines), MAX(share_of_commit), MAX(share_of_ai)
         FROM event_commit_session
         WHERE repo_key IS NOT NULL AND repo_key != ''
           AND (?1 IS NULL OR repo_root = ?1)
         GROUP BY repo_key, member_email, provider, session_id, commit_sha",
    )?;
    let rows = stmt.query_map(params![scope.repo_root.as_deref()], |row| {
        let repo_key: String = row.get(0)?;
        let member_email: String = row.get(1)?;
        let provider: String = row.get(3)?;
        let session_id: String = row.get(4)?;
        let commit_sha: String = row.get(5)?;
        Ok(build_local_sync_event(
            "event_commit_session",
            format!(
                "{}|{}|{}|{}|{}",
                repo_key,
                stable_key_part(Some(member_email.clone())),
                provider,
                session_id,
                commit_sha
            ),
            Some(repo_key),
            Some(member_email),
            row.get(2)?,
            row.get(6)?,
            json!({
                "provider": provider,
                "session_id": session_id,
                "commit_sha": commit_sha,
                "commit_time": row.get::<_, Option<String>>(6)?,
                "model_name": row.get::<_, Option<String>>(7)?,
                "matched_lines": row.get::<_, f64>(8)?,
                "share_of_commit": row.get::<_, f64>(9)?,
                "share_of_ai": row.get::<_, f64>(10)?,
            }),
        ))
    })?;
    append_query_rows(rows, output)
}

fn collect_task_commit_events(
    conn: &Connection,
    scope: &SyncScope,
    output: &mut Vec<LocalSyncEvent>,
) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT repo_key, task_key, MAX(branch_name), commit_sha, MIN(fallback_flag), MAX(confidence), MIN(commit_time)
         FROM event_task_commit
         WHERE repo_key IS NOT NULL AND repo_key != ''
           AND (?1 IS NULL OR repo_root = ?1)
         GROUP BY repo_key, task_key, commit_sha",
    )?;
    let rows = stmt.query_map(params![scope.repo_root.as_deref()], |row| {
        let repo_key: String = row.get(0)?;
        let task_key: String = row.get(1)?;
        let commit_sha: String = row.get(3)?;
        Ok(build_local_sync_event(
            "event_task_commit",
            format!("{repo_key}|{task_key}|{commit_sha}"),
            Some(repo_key),
            None,
            None,
            row.get(6)?,
            json!({
                "task_key": task_key,
                "branch_name": row.get::<_, String>(2)?,
                "commit_sha": commit_sha,
                "fallback_flag": row.get::<_, i64>(4)?,
                "confidence": row.get::<_, f64>(5)?,
                "commit_time": row.get::<_, Option<String>>(6)?,
            }),
        ))
    })?;
    append_query_rows(rows, output)
}

fn collect_task_session_events(
    conn: &Connection,
    scope: &SyncScope,
    output: &mut Vec<LocalSyncEvent>,
) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT repo_key, task_key, MAX(branch_name), provider, session_id, member_email, MAX(device_id),
                MAX(model_name), MIN(started_at), MAX(attribution_weight), MAX(commit_within_window_flag),
                MAX(user_turn_count), MAX(debug_loop_flag), MAX(mid_session_error_paste_flag), MAX(accepted_output_flag),
                MIN(first_accepted_change_at), MIN(minutes_to_first_accepted_change)
         FROM event_task_session
         WHERE repo_key IS NOT NULL AND repo_key != ''
           AND (?1 IS NULL OR repo_root = ?1)
         GROUP BY repo_key, task_key, member_email, provider, session_id",
    )?;
    let rows = stmt.query_map(params![scope.repo_root.as_deref()], |row| {
        let repo_key: String = row.get(0)?;
        let task_key: String = row.get(1)?;
        let provider: String = row.get(3)?;
        let session_id: String = row.get(4)?;
        let member_email: String = row.get(5)?;
        Ok(build_local_sync_event(
            "event_task_session",
            format!(
                "{}|{}|{}|{}|{}",
                repo_key,
                task_key,
                stable_key_part(Some(member_email.clone())),
                provider,
                session_id
            ),
            Some(repo_key),
            Some(member_email),
            row.get(6)?,
            row.get(8)?,
            json!({
                "task_key": task_key,
                "branch_name": row.get::<_, String>(2)?,
                "provider": provider,
                "session_id": session_id,
                "model_name": row.get::<_, Option<String>>(7)?,
                "started_at": row.get::<_, Option<String>>(8)?,
                "attribution_weight": row.get::<_, f64>(9)?,
                "commit_within_window_flag": row.get::<_, i64>(10)?,
                "user_turn_count": row.get::<_, Option<i64>>(11)?,
                "debug_loop_flag": row.get::<_, Option<i64>>(12)?,
                "mid_session_error_paste_flag": row.get::<_, Option<i64>>(13)?,
                "accepted_output_flag": row.get::<_, Option<i64>>(14)?,
                "first_accepted_change_at": row.get::<_, Option<String>>(15)?,
                "minutes_to_first_accepted_change": row.get::<_, Option<f64>>(16)?,
            }),
        ))
    })?;
    append_query_rows(rows, output)
}

fn collect_commit_pr_outcome_events(
    conn: &Connection,
    scope: &SyncScope,
    output: &mut Vec<LocalSyncEvent>,
) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT repo_key, commit_sha, MAX(lookup_status), MAX(pr_number), MAX(pr_opened_flag), MAX(pr_merged_flag),
                MIN(pr_created_at), MAX(pr_merged_at)
         FROM event_commit_pr_outcome
         WHERE repo_key IS NOT NULL AND repo_key != ''
           AND (?1 IS NULL OR repo_root = ?1)
         GROUP BY repo_key, commit_sha",
    )?;
    let rows = stmt.query_map(params![scope.repo_root.as_deref()], |row| {
        let repo_key: String = row.get(0)?;
        let commit_sha: String = row.get(1)?;
        let pr_created_at: Option<String> = row.get(6)?;
        let pr_merged_at: Option<String> = row.get(7)?;
        Ok(build_local_sync_event(
            "event_commit_pr_outcome",
            format!("{repo_key}|{commit_sha}"),
            Some(repo_key),
            None,
            None,
            pr_merged_at.clone().or(pr_created_at.clone()),
            json!({
                "commit_sha": commit_sha,
                "lookup_status": row.get::<_, String>(2)?,
                "pr_number": row.get::<_, Option<i64>>(3)?,
                "pr_opened_flag": row.get::<_, i64>(4)?,
                "pr_merged_flag": row.get::<_, i64>(5)?,
                "pr_created_at": pr_created_at,
                "pr_merged_at": pr_merged_at,
            }),
        ))
    })?;
    append_query_rows(rows, output)
}

fn append_query_rows<T>(
    rows: impl Iterator<Item = rusqlite::Result<T>>,
    output: &mut Vec<T>,
) -> Result<()> {
    for row in rows {
        output.push(row?);
    }
    Ok(())
}

fn build_local_sync_event(
    event_type: &str,
    event_key: String,
    repo_key: Option<String>,
    member_email: Option<String>,
    device_id: Option<String>,
    occurred_at: Option<String>,
    payload: Value,
) -> LocalSyncEvent {
    let remote = RemoteSyncEvent {
        event_type: event_type.to_string(),
        event_key: event_key.clone(),
        repo_key: repo_key.clone(),
        member_email: member_email.clone(),
        device_id: device_id.clone(),
        occurred_at: occurred_at.clone(),
        payload: payload.clone(),
    };
    let content_hash = format!(
        "{:x}",
        md5::compute(
            serde_json::to_vec(&remote).expect("remote sync event should serialize for hashing")
        )
    );
    LocalSyncEvent {
        event_type: event_type.to_string(),
        event_key,
        repo_key,
        member_email,
        device_id,
        occurred_at,
        payload,
        content_hash,
    }
}

fn synced_content_hash(
    conn: &Connection,
    organization_id: &str,
    event_type: &str,
    event_key: &str,
) -> Result<Option<String>> {
    conn.query_row(
        "SELECT content_hash
         FROM fact_sync_event_state
         WHERE organization_id = ?1 AND event_type = ?2 AND event_key = ?3",
        params![organization_id, event_type, event_key],
        |row| row.get(0),
    )
    .optional()
    .map_err(Into::into)
}

fn resolve_sync_field(
    env_key: &str,
    saved: Option<&str>,
) -> Result<(Option<String>, SyncConfigSource)> {
    if let Some(value) = env::var(env_key)
        .ok()
        .map(normalized_non_empty)
        .transpose()?
    {
        return Ok((Some(value), SyncConfigSource::Environment));
    }

    if let Some(saved) = saved {
        return Ok((
            Some(normalized_non_empty(saved.to_string())?),
            SyncConfigSource::Saved,
        ));
    }

    Ok((None, SyncConfigSource::Saved))
}

fn resolve_repo_root_from_path(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()?.join(path)
    };
    detect_repo_root(&absolute).ok_or_else(|| {
        anyhow!(
            "could not determine a git repository root from {}",
            absolute.display()
        )
    })
}

fn paceflow_home_dir() -> Result<PathBuf> {
    env::var_os("PACEFLOW_HOME")
        .map(PathBuf::from)
        .or_else(dirs::home_dir)
        .ok_or_else(|| anyhow!("Home directory not found"))
}

fn normalize_base_url(raw: &str) -> Result<String> {
    let value = normalized_non_empty(raw.to_string())?;
    if !(value.starts_with("http://") || value.starts_with("https://")) {
        bail!("sync base URL must start with http:// or https://");
    }
    Ok(value.trim_end_matches('/').to_string())
}

fn normalized_non_empty(raw: String) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("sync configuration value cannot be empty");
    }
    Ok(trimmed.to_string())
}

fn stable_key_part(value: Option<String>) -> String {
    value
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "(unknown)".to_string())
}

fn repo_match_key(repo_key: &str) -> Option<String> {
    let repo_key = repo_key
        .trim()
        .trim_end_matches('/')
        .trim_end_matches(".git");
    if repo_key.is_empty() {
        return None;
    }
    repo_key
        .strip_prefix("git:")
        .filter(|value| value.split('/').count() >= 3)
        .map(|value| format!("git:{}", value.to_ascii_lowercase()))
}

fn write_secret_file(path: &Path, contents: &[u8]) -> Result<()> {
    let mut options = OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(contents)?;
    file.write_all(b"\n")?;
    file.flush()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_app_schema;
    use crate::test_support::{ScopedEnvVar, lock_env};
    use anyhow::Result;
    use std::net::TcpListener;
    use std::thread;
    use tempfile::tempdir;

    fn open_sync_test_db() -> Result<Connection> {
        let conn = Connection::open_in_memory()?;
        init_app_schema(&conn)?;
        Ok(conn)
    }

    #[test]
    fn saved_sync_config_round_trips() -> Result<()> {
        let _guard = lock_env();
        let tempdir = tempdir()?;
        let _paceflow_home = ScopedEnvVar::set("PACEFLOW_HOME", tempdir.path());
        let _env_base = ScopedEnvVar::unset(SYNC_BASE_URL_ENV_VAR);
        let _env_org = ScopedEnvVar::unset(SYNC_ORGANIZATION_ID_ENV_VAR);
        let _env_token = ScopedEnvVar::unset(SYNC_TOKEN_ENV_VAR);

        let config = SavedSyncConfig {
            base_url: "https://api.example.com".to_string(),
            organization_id: "01TESTORG".to_string(),
            organization_name: Some("Example Org".to_string()),
            member_email: Some("dev@example.com".to_string()),
            token: "token-123".to_string(),
        };
        save_sync_config(&config)?;

        assert_eq!(load_saved_sync_config()?, Some(config));
        Ok(())
    }

    #[test]
    fn resolved_sync_config_prefers_env_over_saved_values() -> Result<()> {
        let _guard = lock_env();
        let tempdir = tempdir()?;
        let _paceflow_home = ScopedEnvVar::set("PACEFLOW_HOME", tempdir.path());
        let _env_base = ScopedEnvVar::set(SYNC_BASE_URL_ENV_VAR, "https://env.example.com");
        let _env_org = ScopedEnvVar::set(SYNC_ORGANIZATION_ID_ENV_VAR, "01ENVORG");
        let _env_token = ScopedEnvVar::set(SYNC_TOKEN_ENV_VAR, "env-token");
        save_sync_config(&SavedSyncConfig {
            base_url: "https://saved.example.com".to_string(),
            organization_id: "01SAVEDORG".to_string(),
            organization_name: Some("Saved Org".to_string()),
            member_email: Some("saved@example.com".to_string()),
            token: "saved-token".to_string(),
        })?;

        let resolved = resolved_sync_config()?.expect("resolved config");
        assert_eq!(resolved.base_url, "https://env.example.com");
        assert_eq!(resolved.organization_id, "01ENVORG");
        assert_eq!(resolved.token, "env-token");
        assert_eq!(resolved.base_url_source, SyncConfigSource::Environment);
        assert_eq!(
            resolved.organization_id_source,
            SyncConfigSource::Environment
        );
        assert_eq!(resolved.token_source, SyncConfigSource::Environment);
        assert_eq!(resolved.organization_name, None);
        Ok(())
    }

    #[test]
    fn collect_sync_events_builds_expected_keys_for_all_event_tables() -> Result<()> {
        let conn = open_sync_test_db()?;
        let repo_root = "/tmp/repo";
        let repo_key = "git:github.com/PaceFlow/ai-engineering-analytics";

        conn.execute(
            "INSERT INTO event_session_quality (
                provider, session_id, repo_root, repo_key, member_email, device_id, model_name,
                started_at, ended_at, user_turn_count, debug_loop_flag, mid_session_error_paste_flag,
                accepted_output_flag, first_accepted_change_at, minutes_to_first_accepted_change,
                session_commit_within_4h_flag
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1, 0, 0, 1, ?10, 2.5, 1)",
            params![
                "cursor",
                "sess-1",
                repo_root,
                repo_key,
                "dev@paceflow.io",
                "device:test",
                "gpt-5",
                "2026-01-15T00:00:00Z",
                "2026-01-15T00:10:00Z",
                "2026-01-15T00:01:00Z"
            ],
        )?;
        conn.execute(
            "INSERT INTO event_session_productivity (
                provider, session_id, repo_root, repo_key, member_email, device_id, model_name,
                project_path, started_at, ended_at, accepted_lines_added, accepted_lines_removed,
                accepted_total_changed_lines, user_word_count
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 10, 2, 12, 50)",
            params![
                "cursor",
                "sess-1",
                repo_root,
                repo_key,
                "dev@paceflow.io",
                "device:test",
                "gpt-5",
                "/tmp/repo",
                "2026-01-15T00:00:00Z",
                "2026-01-15T00:10:00Z",
            ],
        )?;
        conn.execute(
            "INSERT INTO event_session_cost (
                provider, session_id, repo_root, repo_key, member_email, device_id, model_name,
                started_at, ended_at, accepted_total_changed_lines, accepted_output_flag,
                input_tokens, cached_input_tokens, cache_creation_tokens, output_tokens,
                reasoning_tokens, total_tokens, estimated_cost_usd, actual_cost_usd, cost_source
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 12, 1, 1000, 200, 300, 400, 50, 1950, 0.0123, NULL, 'estimated')",
            params![
                "cursor",
                "sess-1",
                repo_root,
                repo_key,
                "dev@paceflow.io",
                "device:test",
                "gpt-5",
                "2026-01-15T00:00:00Z",
                "2026-01-15T00:10:00Z",
            ],
        )?;
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, repo_key, commit_sha, commit_time, mainline_reached_at, heavy_ai_flag, merged_to_mainline_flag,
                reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
            ) VALUES (?1, ?2, 'abc123', '2026-01-16T00:00:00Z', '2026-01-16T03:00:00Z', 1, 1, 0, 42, 60)",
            params![repo_root, repo_key],
        )?;
        conn.execute(
            "INSERT INTO fact_commit (
                repo_root, commit_sha, parent_sha, commit_time, subject, total_added, total_removed,
                matched_total_lines, matched_added_lines, matched_removed_lines, ai_share, heavy_ai
            ) VALUES (?1, 'abc123', NULL, '2026-01-16T00:00:00Z', 'sync test commit', 17, 5, 42, 38, 4, 0.7, 1)",
            params![repo_root],
        )?;
        conn.execute(
            "INSERT INTO event_commit_churn (
                repo_root, repo_key, commit_sha, ai_added_lines_reaching_mainline,
                ai_added_lines_removed_within_window, churn_window_days
            ) VALUES (?1, ?2, 'abc123', 20, 4, 14)",
            params![repo_root, repo_key],
        )?;
        conn.execute(
            "INSERT INTO event_commit_bug_signal (
                repo_root, repo_key, commit_sha, bug_after_merge_flag, first_bug_signal_commit_sha,
                first_bug_signal_commit_time, bug_signal_count, window_days, signal_source
            ) VALUES (?1, ?2, 'abc123', 1, 'fix123', '2026-01-17T00:00:00Z', 1, 60, 'git_fix_commit')",
            params![repo_root, repo_key],
        )?;
        conn.execute(
            "INSERT INTO event_commit_session (
                repo_root, repo_key, commit_sha, provider, session_id, member_email, device_id,
                commit_time, model_name, matched_lines, share_of_commit, share_of_ai
            ) VALUES (?1, ?2, 'abc123', 'cursor', 'sess-1', 'dev@paceflow.io', 'device:test',
                '2026-01-16T00:00:00Z', 'gpt-5', 24.0, 0.6, 0.8)",
            params![repo_root, repo_key],
        )?;
        conn.execute(
            "INSERT INTO event_task_commit (
                repo_root, repo_key, task_key, branch_name, commit_sha, fallback_flag, confidence, commit_time
            ) VALUES (?1, ?2, 'PAC-101', 'feature/pac-101', 'abc123', 0, 0.9, '2026-01-16T00:00:00Z')",
            params![repo_root, repo_key],
        )?;
        conn.execute(
            "INSERT INTO event_task_session (
                repo_root, repo_key, task_key, branch_name, provider, session_id, member_email, device_id,
                model_name, started_at, attribution_weight, commit_within_window_flag, user_turn_count,
                debug_loop_flag, mid_session_error_paste_flag, accepted_output_flag,
                first_accepted_change_at, minutes_to_first_accepted_change
            ) VALUES (?1, ?2, 'PAC-101', 'feature/pac-101', 'cursor', 'sess-1', 'dev@paceflow.io',
                'device:test', 'gpt-5', '2026-01-15T00:00:00Z', 0.75, 1, 3, 0, 0, 1,
                '2026-01-15T00:01:00Z', 1.0)",
            params![repo_root, repo_key],
        )?;
        conn.execute(
            "INSERT INTO event_commit_pr_outcome (
                repo_root, repo_key, commit_sha, lookup_status, pr_number, pr_opened_flag,
                pr_merged_flag, pr_created_at, pr_merged_at
            ) VALUES (?1, ?2, 'abc123', 'found', 9, 1, 1, '2026-01-16T01:00:00Z', '2026-01-16T02:00:00Z')",
            params![repo_root, repo_key],
        )?;

        let events = collect_sync_events(
            &conn,
            &SyncScope {
                repo_root: Some(repo_root.to_string()),
            },
        )?;

        assert_eq!(events.len(), 10);
        let keys = events
            .iter()
            .map(|event| (event.event_type.as_str(), event.event_key.as_str()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            keys.get("event_session_quality"),
            Some(&"git:github.com/PaceFlow/ai-engineering-analytics|dev@paceflow.io|cursor|sess-1")
        );
        assert_eq!(
            keys.get("event_session_productivity"),
            Some(&"git:github.com/PaceFlow/ai-engineering-analytics|dev@paceflow.io|cursor|sess-1")
        );
        assert_eq!(
            keys.get("event_session_cost"),
            Some(&"git:github.com/PaceFlow/ai-engineering-analytics|dev@paceflow.io|cursor|sess-1")
        );
        assert_eq!(
            keys.get("event_commit_outcome"),
            Some(&"git:github.com/PaceFlow/ai-engineering-analytics|abc123")
        );
        let cost_event = events
            .iter()
            .find(|event| event.event_type == "event_session_cost")
            .expect("session cost sync event");
        assert_eq!(cost_event.payload["total_tokens"], json!(1950));
        assert_eq!(cost_event.payload["estimated_cost_usd"], json!(0.0123));
        assert_eq!(cost_event.payload["cost_source"], json!("estimated"));
        let outcome_event = events
            .iter()
            .find(|event| event.event_type == "event_commit_outcome")
            .expect("commit outcome sync event");
        assert_eq!(
            outcome_event.payload["mainline_reached_at"],
            json!("2026-01-16T03:00:00Z")
        );
        assert_eq!(outcome_event.payload["lines_added"], json!(17));
        assert_eq!(outcome_event.payload["lines_removed"], json!(5));
        assert_eq!(
            keys.get("event_commit_churn"),
            Some(&"git:github.com/PaceFlow/ai-engineering-analytics|abc123")
        );
        assert_eq!(
            keys.get("event_commit_bug_signal"),
            Some(&"git:github.com/PaceFlow/ai-engineering-analytics|abc123")
        );
        assert_eq!(
            keys.get("event_commit_session"),
            Some(
                &"git:github.com/PaceFlow/ai-engineering-analytics|dev@paceflow.io|cursor|sess-1|abc123"
            )
        );
        assert_eq!(
            keys.get("event_task_commit"),
            Some(&"git:github.com/PaceFlow/ai-engineering-analytics|PAC-101|abc123")
        );
        assert_eq!(
            keys.get("event_task_session"),
            Some(
                &"git:github.com/PaceFlow/ai-engineering-analytics|PAC-101|dev@paceflow.io|cursor|sess-1"
            )
        );
        assert_eq!(
            keys.get("event_commit_pr_outcome"),
            Some(&"git:github.com/PaceFlow/ai-engineering-analytics|abc123")
        );
        Ok(())
    }

    #[test]
    fn pending_sync_events_are_scoped_by_organization() -> Result<()> {
        let mut conn = open_sync_test_db()?;
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, repo_key, commit_sha, commit_time, heavy_ai_flag, merged_to_mainline_flag,
                reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
            ) VALUES ('/tmp/repo', 'git:github.com/PaceFlow/ai-engineering-analytics', 'abc123',
                '2026-01-16T00:00:00Z', 1, 1, 0, 42, 60)",
            [],
        )?;
        let scope = SyncScope {
            repo_root: Some("/tmp/repo".to_string()),
        };

        let pending_org_a = pending_sync_events(&conn, "org-A", &scope)?;
        assert_eq!(pending_org_a.len(), 1);

        mark_synced_events(&mut conn, "org-A", &pending_org_a, "checkpoint-1")?;

        assert!(pending_sync_events(&conn, "org-A", &scope)?.is_empty());
        assert_eq!(pending_sync_events(&conn, "org-B", &scope)?.len(), 1);
        assert!(last_sync_run_state(&conn, "org-A")?.is_some());
        Ok(())
    }

    #[test]
    fn sync_repository_filter_matches_repo_keys_case_insensitively() {
        let events = vec![
            test_sync_event("git:github.com/PaceFlow/webapp", "abc123"),
            test_sync_event("git:github.com/personal/weekend", "def456"),
        ];
        let allowlist = SyncRepositoryListResponse {
            organization_id: "01ORG".to_string(),
            repositories: vec![SyncRepositoryRef {
                repo_key: "git:github.com/paceflow/WEBAPP".to_string(),
                provider: "github".to_string(),
                repo_name: "WEBAPP".to_string(),
                last_seen_at: None,
                sources: vec!["pull_request".to_string()],
            }],
        };

        let partitioned = partition_eligible_sync_events(events, &allowlist);

        assert_eq!(partitioned.eligible.len(), 1);
        assert_eq!(partitioned.skipped.len(), 1);
        assert_eq!(
            partitioned
                .skipped_repo_keys
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            vec!["git:github.com/personal/weekend".to_string()]
        );
    }

    #[test]
    fn skipped_sync_events_are_not_marked_as_synced() -> Result<()> {
        let mut conn = open_sync_test_db()?;
        let work = test_sync_event("git:github.com/PaceFlow/webapp", "abc123");
        let personal = test_sync_event("git:github.com/personal/weekend", "def456");
        let allowlist = SyncRepositoryListResponse {
            organization_id: "01ORG".to_string(),
            repositories: vec![SyncRepositoryRef {
                repo_key: "git:github.com/paceflow/webapp".to_string(),
                provider: "github".to_string(),
                repo_name: "webapp".to_string(),
                last_seen_at: None,
                sources: vec!["pull_request".to_string()],
            }],
        };
        let partitioned =
            partition_eligible_sync_events(vec![work.clone(), personal.clone()], &allowlist);

        mark_synced_events(&mut conn, "org-A", &partitioned.eligible, "checkpoint-1")?;

        assert_eq!(
            synced_content_hash(&conn, "org-A", &work.event_type, &work.event_key)?,
            Some(work.content_hash)
        );
        assert_eq!(
            synced_content_hash(&conn, "org-A", &personal.event_type, &personal.event_key)?,
            None
        );
        Ok(())
    }

    #[test]
    fn pending_sync_events_converge_with_duplicate_remote_event_keys() -> Result<()> {
        let mut conn = open_sync_test_db()?;
        let repo_key = "git:github.com/PaceFlow/ai-engineering-analytics";
        conn.execute(
            "INSERT INTO event_commit_outcome (
                repo_root, repo_key, commit_sha, commit_time, mainline_reached_at, heavy_ai_flag, merged_to_mainline_flag,
                reverted_later_flag, total_matched_ai_lines, commit_total_changed_lines
            ) VALUES
                ('/tmp/repo-a', ?1, 'abc123', '2026-01-16T00:00:00Z', '2026-01-16T02:00:00Z', 1, 1, 0, 42, 60),
                ('/tmp/repo-b', ?1, 'abc123', '2026-01-15T23:00:00Z', '2026-01-16T03:00:00Z', 0, 1, 1, 43, 61)",
            params![repo_key],
        )?;
        conn.execute(
            "INSERT INTO fact_commit (
                repo_root, commit_sha, parent_sha, commit_time, subject, total_added, total_removed,
                matched_total_lines, matched_added_lines, matched_removed_lines, ai_share, heavy_ai
            ) VALUES
                ('/tmp/repo-a', 'abc123', NULL, '2026-01-16T00:00:00Z', 'a', 10, 2, 42, 40, 2, 0.7, 1),
                ('/tmp/repo-b', 'abc123', NULL, '2026-01-15T23:00:00Z', 'b', 12, 3, 43, 41, 2, 0.7, 1)",
            [],
        )?;
        conn.execute(
            "INSERT INTO event_commit_churn (
                repo_root, repo_key, commit_sha, ai_added_lines_reaching_mainline,
                ai_added_lines_removed_within_window, churn_window_days
            ) VALUES
                ('/tmp/repo-a', ?1, 'abc123', 8, 1, 14),
                ('/tmp/repo-b', ?1, 'abc123', 9, 2, 30)",
            params![repo_key],
        )?;
        conn.execute(
            "INSERT INTO event_commit_bug_signal (
                repo_root, repo_key, commit_sha, bug_after_merge_flag, first_bug_signal_commit_sha,
                first_bug_signal_commit_time, bug_signal_count, window_days, signal_source
            ) VALUES
                ('/tmp/repo-a', ?1, 'abc123', 0, NULL, NULL, 0, 60, 'none'),
                ('/tmp/repo-b', ?1, 'abc123', 1, 'fix123', '2026-01-17T00:00:00Z', 2, 60, 'git_fix_commit')",
            params![repo_key],
        )?;
        conn.execute(
            "INSERT INTO event_commit_pr_outcome (
                repo_root, repo_key, commit_sha, lookup_status, pr_number, pr_opened_flag,
                pr_merged_flag, pr_created_at, pr_merged_at
            ) VALUES
                ('/tmp/repo-a', ?1, 'abc123', 'resolved', 7, 1, 0, '2026-01-16T01:00:00Z', NULL),
                ('/tmp/repo-b', ?1, 'abc123', 'resolved', 7, 1, 1, '2026-01-16T01:00:00Z', '2026-01-16T04:00:00Z')",
            params![repo_key],
        )?;
        conn.execute(
            "INSERT INTO event_commit_session (
                repo_root, repo_key, commit_sha, provider, session_id, member_email, device_id,
                commit_time, model_name, matched_lines, share_of_commit, share_of_ai
            ) VALUES
                ('/tmp/repo-a', ?1, 'abc123', 'cursor', 'sess-1', 'dev@paceflow.io', 'device:a',
                 '2026-01-16T00:00:00Z', 'gpt-4', 20.0, 0.5, 0.6),
                ('/tmp/repo-b', ?1, 'abc123', 'cursor', 'sess-1', 'dev@paceflow.io', 'device:b',
                 '2026-01-15T23:00:00Z', 'gpt-5', 24.0, 0.6, 0.8)",
            params![repo_key],
        )?;
        conn.execute(
            "INSERT INTO event_task_commit (
                repo_root, repo_key, task_key, branch_name, commit_sha, fallback_flag, confidence, commit_time
            ) VALUES
                ('/tmp/repo-a', ?1, 'PAC-101', 'feature/a', 'abc123', 1, 0.7, '2026-01-16T00:00:00Z'),
                ('/tmp/repo-b', ?1, 'PAC-101', 'feature/b', 'abc123', 0, 0.9, '2026-01-15T23:00:00Z')",
            params![repo_key],
        )?;
        conn.execute(
            "INSERT INTO event_task_session (
                repo_root, repo_key, task_key, branch_name, provider, session_id, member_email, device_id,
                model_name, started_at, attribution_weight, commit_within_window_flag, user_turn_count,
                debug_loop_flag, mid_session_error_paste_flag, accepted_output_flag,
                first_accepted_change_at, minutes_to_first_accepted_change
            ) VALUES
                ('/tmp/repo-a', ?1, 'PAC-101', 'feature/a', 'cursor', 'sess-1', 'dev@paceflow.io',
                 'device:a', 'gpt-4', '2026-01-15T00:00:00Z', 0.5, 0, 3, 0, 0, 0,
                 '2026-01-15T00:05:00Z', 5.0),
                ('/tmp/repo-b', ?1, 'PAC-101', 'feature/b', 'cursor', 'sess-1', 'dev@paceflow.io',
                 'device:b', 'gpt-5', '2026-01-14T23:00:00Z', 0.8, 1, 4, 1, 1, 1,
                 '2026-01-15T00:01:00Z', 1.0)",
            params![repo_key],
        )?;
        let scope = SyncScope { repo_root: None };

        let pending = pending_sync_events(&conn, "org-A", &scope)?;
        let counts = grouped_event_counts(&pending);
        assert_eq!(counts.get("event_commit_outcome"), Some(&1));
        assert_eq!(counts.get("event_commit_churn"), Some(&1));
        assert_eq!(counts.get("event_commit_bug_signal"), Some(&1));
        assert_eq!(counts.get("event_commit_pr_outcome"), Some(&1));
        assert_eq!(counts.get("event_commit_session"), Some(&1));
        assert_eq!(counts.get("event_task_commit"), Some(&1));
        assert_eq!(counts.get("event_task_session"), Some(&1));

        let outcome_event = pending
            .iter()
            .find(|event| event.event_type == "event_commit_outcome")
            .expect("outcome event");
        assert_eq!(outcome_event.payload["heavy_ai_flag"], json!(1));
        assert_eq!(outcome_event.payload["merged_to_mainline_flag"], json!(1));
        assert_eq!(outcome_event.payload["reverted_later_flag"], json!(1));
        assert_eq!(
            outcome_event.payload["commit_time"],
            json!("2026-01-15T23:00:00Z")
        );
        assert_eq!(
            outcome_event.payload["mainline_reached_at"],
            json!("2026-01-16T03:00:00Z")
        );
        assert_eq!(outcome_event.payload["lines_added"], json!(12));
        assert_eq!(outcome_event.payload["lines_removed"], json!(3));

        mark_synced_events(&mut conn, "org-A", &pending, "checkpoint-1")?;
        assert!(
            pending_sync_events(&conn, "org-A", &scope)?.is_empty(),
            "pending sync should converge after marking the canonical event key synced"
        );
        Ok(())
    }

    #[test]
    fn api_client_handles_person_link_org_repositories_and_org_status() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        thread::spawn(move || {
            for (index, body) in [
                json_response("{\"message\":\"If this email belongs to an enabled person, a verification code has been sent.\"}"),
                json_response(
                    "{\"token\":\"abc123\",\"organization_id\":\"01ORG\",\"organization_name\":\"Example Org\",\"member_email\":\"dev@example.com\"}"
                ),
                json_response(
                    "{\"organization_id\":\"01ORG\",\"repositories\":[{\"repo_key\":\"git:github.com/PaceFlow/webapp\",\"provider\":\"github\",\"repo_name\":\"webapp\",\"last_seen_at\":\"2026-05-08T10:15:00\",\"sources\":[\"pull_request\"]}]}"
                ),
                json_response(
                    "{\"organization_id\":\"01ORG\",\"organization_name\":\"Example Org\",\"total_events\":3,\"last_event_at\":null}"
                ),
            ]
            .into_iter()
            .enumerate()
            {
                let (mut stream, _) = listener.accept().expect("accept");
                let mut buf = [0_u8; 4096];
                let _ = std::io::Read::read(&mut stream, &mut buf);
                std::io::Write::write_all(&mut stream, body.as_bytes()).expect("write");
                std::io::Write::flush(&mut stream).expect("flush");
                if index == 3 {
                    break;
                }
            }
        });

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let base_url = format!("http://{}", addr);
        let unauthenticated = SyncApiClient::new(base_url.clone(), None)?;
        runtime.block_on(unauthenticated.request_person_link("01ORG", "dev@example.com"))?;
        let linked = runtime.block_on(unauthenticated.verify_person_link(
            "01ORG",
            "dev@example.com",
            "123456",
            "device:test",
        ))?;
        assert_eq!(linked.token, "abc123");
        assert_eq!(linked.member_email, "dev@example.com");

        let authenticated = SyncApiClient::new(base_url, Some(linked.token))?;

        let repositories = runtime.block_on(authenticated.repositories("01ORG"))?;
        assert_eq!(repositories.repositories.len(), 1);
        assert_eq!(
            repositories.repositories[0].repo_key,
            "git:github.com/PaceFlow/webapp"
        );

        let status = runtime.block_on(authenticated.status("01ORG"))?;
        assert_eq!(status.total_events, 3);
        assert_eq!(status.organization_name.as_deref(), Some("Example Org"));
        Ok(())
    }

    fn json_response(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
    }

    fn test_sync_event(repo_key: &str, commit_sha: &str) -> LocalSyncEvent {
        build_local_sync_event(
            "event_commit_outcome",
            format!("{repo_key}|{commit_sha}"),
            Some(repo_key.to_string()),
            Some("dev@paceflow.io".to_string()),
            Some("device:test".to_string()),
            Some("2026-01-16T00:00:00Z".to_string()),
            json!({"commit_sha": commit_sha}),
        )
    }
}
