use anyhow::{Context, Result};
use reqwest::header::{ACCEPT, AUTHORIZATION, HeaderMap, HeaderValue, USER_AGENT};
use serde::Deserialize;

use crate::github::types::{GitHubRepo, PullRequestRecord};

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
}

#[derive(Clone)]
pub struct ReqwestGitHubApi {
    client: reqwest::Client,
    base_url: String,
}

impl ReqwestGitHubApi {
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

    async fn fetch_json<T>(&self, url: String) -> std::result::Result<T, GitHubApiError>
    where
        T: for<'de> Deserialize<'de>,
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
}

impl GitHubApi for ReqwestGitHubApi {
    async fn fetch_commit_pulls(
        &self,
        repo: &GitHubRepo,
        commit_sha: &str,
    ) -> std::result::Result<Vec<PullRequestRecord>, GitHubApiError> {
        let url = format!(
            "{}/repos/{}/{}/commits/{}/pulls",
            self.base_url, repo.owner, repo.name, commit_sha
        );
        let pulls = self.fetch_json::<Vec<PullRequestResponse>>(url).await?;
        Ok(pulls.into_iter().map(Into::into).collect())
    }

    async fn fetch_pull_request(
        &self,
        repo: &GitHubRepo,
        pr_number: i64,
    ) -> std::result::Result<PullRequestRecord, GitHubApiError> {
        let url = format!(
            "{}/repos/{}/{}/pulls/{}",
            self.base_url, repo.owner, repo.name, pr_number
        );
        let pull = self.fetch_json::<PullRequestResponse>(url).await?;
        Ok(pull.into())
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

pub fn github_token_from_env() -> Option<String> {
    std::env::var("PACEFLOW_GITHUB_TOKEN")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}
