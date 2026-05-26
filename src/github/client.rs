use anyhow::{Context, Result};
use reqwest::Url;
use reqwest::header::{ACCEPT, AUTHORIZATION, HeaderMap, HeaderValue, USER_AGENT};
use serde::{Deserialize, de::DeserializeOwned};

use crate::github::types::{
    GitHubRepo, IssueRecord, IssueTimelineEvent, PullRequestFileRecord, PullRequestRecord,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GitHubApiErrorKind {
    RateLimited,
    Other,
}

#[derive(Debug, Clone)]
pub struct GitHubApiError {
    pub kind: GitHubApiErrorKind,
    pub message: String,
}

impl std::fmt::Display for GitHubApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for GitHubApiError {}

pub trait GitHubApi {
    fn fetch_commit_pulls(
        &self,
        repo: &GitHubRepo,
        commit_sha: &str,
    ) -> impl std::future::Future<
        Output = std::result::Result<Vec<PullRequestRecord>, GitHubApiError>,
    > + Send;

    fn fetch_pull_request(
        &self,
        repo: &GitHubRepo,
        pr_number: i64,
    ) -> impl std::future::Future<Output = std::result::Result<PullRequestRecord, GitHubApiError>> + Send;

    fn fetch_closed_issues(
        &self,
        repo: &GitHubRepo,
        since: Option<&str>,
    ) -> impl std::future::Future<Output = std::result::Result<Vec<IssueRecord>, GitHubApiError>> + Send;

    fn fetch_issue_timeline(
        &self,
        repo: &GitHubRepo,
        issue_number: i64,
    ) -> impl std::future::Future<
        Output = std::result::Result<Vec<IssueTimelineEvent>, GitHubApiError>,
    > + Send;

    fn fetch_pull_request_files(
        &self,
        repo: &GitHubRepo,
        pr_number: i64,
    ) -> impl std::future::Future<
        Output = std::result::Result<Vec<PullRequestFileRecord>, GitHubApiError>,
    > + Send;
}

#[derive(Clone)]
pub struct ReqwestGitHubApi {
    client: reqwest::Client,
    base_url: String,
}

impl ReqwestGitHubApi {
    const PAGE_SIZE: usize = 100;

    pub fn new(token: &str) -> Result<Self> {
        Self::with_base_url(token, "https://api.github.com")
    }

    pub fn with_base_url(token: &str, base_url: &str) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/vnd.github+json"),
        );
        headers.insert(USER_AGENT, HeaderValue::from_static("paceflow/0.1.0"));
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", token.trim()))
                .context("invalid GitHub token header")?,
        );
        headers.insert(
            "X-GitHub-Api-Version",
            HeaderValue::from_static("2022-11-28"),
        );

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .context("build GitHub HTTP client")?;

        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
        })
    }

    fn build_url(&self, path: &str) -> std::result::Result<Url, GitHubApiError> {
        Url::parse(&format!(
            "{}/{}",
            self.base_url,
            path.trim_start_matches('/')
        ))
        .map_err(|err| GitHubApiError {
            kind: GitHubApiErrorKind::Other,
            message: err.to_string(),
        })
    }

    async fn fetch_json<T>(&self, url: Url) -> std::result::Result<T, GitHubApiError>
    where
        T: DeserializeOwned,
    {
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|err| GitHubApiError {
                kind: GitHubApiErrorKind::Other,
                message: err.to_string(),
            })?;

        if response.status().is_success() {
            return response.json::<T>().await.map_err(|err| GitHubApiError {
                kind: GitHubApiErrorKind::Other,
                message: err.to_string(),
            });
        }

        let status = response.status();
        let remaining = response
            .headers()
            .get("x-ratelimit-remaining")
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned);
        let body = response.text().await.unwrap_or_else(|_| String::new());
        let message = if body.trim().is_empty() {
            format!("GitHub API request failed with {}", status)
        } else {
            format!("GitHub API request failed with {}: {}", status, body.trim())
        };

        let kind = if status.as_u16() == 429
            || (status.as_u16() == 403 && remaining.as_deref() == Some("0"))
        {
            GitHubApiErrorKind::RateLimited
        } else {
            GitHubApiErrorKind::Other
        };

        Err(GitHubApiError { kind, message })
    }

    async fn fetch_paginated_json<T, F>(
        &self,
        mut build_page_url: F,
    ) -> std::result::Result<Vec<T>, GitHubApiError>
    where
        T: DeserializeOwned,
        F: FnMut(usize) -> std::result::Result<Url, GitHubApiError>,
    {
        let mut out = Vec::new();
        let mut page = 1usize;
        loop {
            let batch = self.fetch_json::<Vec<T>>(build_page_url(page)?).await?;
            let batch_len = batch.len();
            out.extend(batch);
            if batch_len < Self::PAGE_SIZE {
                break;
            }
            page += 1;
        }
        Ok(out)
    }
}

impl GitHubApi for ReqwestGitHubApi {
    async fn fetch_commit_pulls(
        &self,
        repo: &GitHubRepo,
        commit_sha: &str,
    ) -> std::result::Result<Vec<PullRequestRecord>, GitHubApiError> {
        let url = self.build_url(&format!(
            "/repos/{}/{}/commits/{}/pulls",
            repo.owner, repo.name, commit_sha
        ))?;
        let pulls = self.fetch_json::<Vec<PullRequestResponse>>(url).await?;
        Ok(pulls.into_iter().map(Into::into).collect())
    }

    async fn fetch_pull_request(
        &self,
        repo: &GitHubRepo,
        pr_number: i64,
    ) -> std::result::Result<PullRequestRecord, GitHubApiError> {
        let url = self.build_url(&format!(
            "/repos/{}/{}/pulls/{}",
            repo.owner, repo.name, pr_number
        ))?;
        let pull = self.fetch_json::<PullRequestResponse>(url).await?;
        Ok(pull.into())
    }

    async fn fetch_closed_issues(
        &self,
        repo: &GitHubRepo,
        since: Option<&str>,
    ) -> std::result::Result<Vec<IssueRecord>, GitHubApiError> {
        let issues = self
            .fetch_paginated_json::<IssueResponse, _>(|page| {
                let mut url =
                    self.build_url(&format!("/repos/{}/{}/issues", repo.owner, repo.name))?;
                {
                    let mut query = url.query_pairs_mut();
                    query.append_pair("state", "closed");
                    query.append_pair("per_page", &Self::PAGE_SIZE.to_string());
                    query.append_pair("page", &page.to_string());
                    if let Some(since) = since {
                        query.append_pair("since", since);
                    }
                }
                Ok(url)
            })
            .await?;
        Ok(issues.into_iter().map(Into::into).collect())
    }

    async fn fetch_issue_timeline(
        &self,
        repo: &GitHubRepo,
        issue_number: i64,
    ) -> std::result::Result<Vec<IssueTimelineEvent>, GitHubApiError> {
        let events = self
            .fetch_paginated_json::<IssueTimelineEventResponse, _>(|page| {
                let mut url = self.build_url(&format!(
                    "/repos/{}/{}/issues/{}/timeline",
                    repo.owner, repo.name, issue_number
                ))?;
                {
                    let mut query = url.query_pairs_mut();
                    query.append_pair("per_page", &Self::PAGE_SIZE.to_string());
                    query.append_pair("page", &page.to_string());
                }
                Ok(url)
            })
            .await?;
        Ok(events.into_iter().map(Into::into).collect())
    }

    async fn fetch_pull_request_files(
        &self,
        repo: &GitHubRepo,
        pr_number: i64,
    ) -> std::result::Result<Vec<PullRequestFileRecord>, GitHubApiError> {
        let files = self
            .fetch_paginated_json::<PullRequestFileResponse, _>(|page| {
                let mut url = self.build_url(&format!(
                    "/repos/{}/{}/pulls/{}/files",
                    repo.owner, repo.name, pr_number
                ))?;
                {
                    let mut query = url.query_pairs_mut();
                    query.append_pair("per_page", &Self::PAGE_SIZE.to_string());
                    query.append_pair("page", &page.to_string());
                }
                Ok(url)
            })
            .await?;
        Ok(files.into_iter().map(Into::into).collect())
    }
}

#[derive(Debug, Deserialize)]
struct PullRequestResponse {
    number: i64,
    state: String,
    draft: Option<bool>,
    created_at: Option<String>,
    updated_at: Option<String>,
    closed_at: Option<String>,
    merged_at: Option<String>,
    html_url: Option<String>,
    base: PullRequestBranchRef,
    head: PullRequestBranchRef,
}

#[derive(Debug, Deserialize)]
struct PullRequestBranchRef {
    #[serde(rename = "ref")]
    git_ref: Option<String>,
}

impl From<PullRequestResponse> for PullRequestRecord {
    fn from(value: PullRequestResponse) -> Self {
        Self {
            number: value.number,
            state: value.state,
            draft: value.draft.unwrap_or(false),
            created_at: value.created_at,
            updated_at: value.updated_at,
            closed_at: value.closed_at,
            merged_at: value.merged_at,
            base_ref: value.base.git_ref,
            head_ref: value.head.git_ref,
            html_url: value.html_url,
        }
    }
}

#[derive(Debug, Deserialize)]
struct IssueResponse {
    number: i64,
    state: String,
    created_at: Option<String>,
    updated_at: Option<String>,
    closed_at: Option<String>,
    pull_request: Option<serde_json::Value>,
    labels: Vec<IssueLabelResponse>,
}

#[derive(Debug, Deserialize)]
struct IssueLabelResponse {
    name: Option<String>,
}

impl From<IssueResponse> for IssueRecord {
    fn from(value: IssueResponse) -> Self {
        Self {
            number: value.number,
            state: value.state,
            created_at: value.created_at,
            updated_at: value.updated_at,
            closed_at: value.closed_at,
            is_pull_request: value.pull_request.is_some(),
            label_names: value
                .labels
                .into_iter()
                .filter_map(|label| label.name)
                .collect(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct IssueTimelineEventResponse {
    event: String,
    created_at: Option<String>,
    source: Option<IssueTimelineSourceResponse>,
}

#[derive(Debug, Deserialize)]
struct IssueTimelineSourceResponse {
    issue: Option<IssueTimelineSourceIssueResponse>,
}

#[derive(Debug, Deserialize)]
struct IssueTimelineSourceIssueResponse {
    number: i64,
    pull_request: Option<serde_json::Value>,
}

impl From<IssueTimelineEventResponse> for IssueTimelineEvent {
    fn from(value: IssueTimelineEventResponse) -> Self {
        let source_pr_number = value
            .source
            .and_then(|source| source.issue)
            .and_then(|issue| issue.pull_request.map(|_| issue.number));
        Self {
            event: value.event,
            created_at: value.created_at,
            source_pr_number,
        }
    }
}

#[derive(Debug, Deserialize)]
struct PullRequestFileResponse {
    filename: String,
    previous_filename: Option<String>,
    status: String,
    patch: Option<String>,
}

impl From<PullRequestFileResponse> for PullRequestFileRecord {
    fn from(value: PullRequestFileResponse) -> Self {
        Self {
            filename: value.filename,
            previous_filename: value.previous_filename,
            status: value.status,
            patch: value.patch,
        }
    }
}

pub fn github_token_from_env() -> Option<String> {
    std::env::var("PACEFLOW_GITHUB_TOKEN")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}
