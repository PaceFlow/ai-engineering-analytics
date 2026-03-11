use anyhow::{Result, anyhow};
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use rusqlite::{Connection, params};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::process::Command;

use crate::change_intel::commit_assoc::git_scan::{is_ancestor, load_commit_diff};
use crate::change_intel::types::LineSide;

const SAMPLE_FLOOR: i64 = 20;
const C2_STRICT_WEAK_RATIO: f64 = 0.20;
const C2_MIN_RATIO: f64 = 0.80;
const C2_MIN_MATCHED_LINES: i64 = 30;
const C2_WINNER_MARGIN: f64 = 0.20;

#[derive(Debug, Clone)]
pub struct RatioMetric {
    pub numerator: i64,
    pub denominator: i64,
    pub status: MetricStatus,
}

impl RatioMetric {
    pub fn percent(&self) -> Option<f64> {
        if self.denominator > 0 {
            Some(self.numerator as f64 / self.denominator as f64 * 100.0)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricStatus {
    Healthy,
    Warning,
    Watch,
    InsufficientData,
    NotApplicable,
}

impl MetricStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            MetricStatus::Healthy => "Healthy",
            MetricStatus::Warning => "Warning",
            MetricStatus::Watch => "Watch",
            MetricStatus::InsufficientData => "Insufficient data",
            MetricStatus::NotApplicable => "N/A",
        }
    }
}

#[derive(Debug, Clone)]
pub struct RepoQualityMetrics {
    pub repo_root: String,
    pub heavy_commits: i64,
    pub l4: RatioMetric,
    pub c2: RatioMetric,
    pub l1: RatioMetric,
    pub note: Option<String>,
}

#[derive(Debug, Clone)]
pub struct QualityMetricsReport {
    pub l4: RatioMetric,
    pub c2: RatioMetric,
    pub l1: RatioMetric,
    pub s4: RatioMetric,
    pub repos: Vec<RepoQualityMetrics>,
    pub sample_floor: i64,
}

#[derive(Debug, Clone)]
struct SessionMessage {
    role: String,
    content: String,
}

#[derive(Debug, Clone)]
struct SessionTurn {
    user_text: String,
    assistant_text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct SessionKey {
    provider: String,
    session_id: String,
}

#[derive(Debug, Clone)]
pub struct SessionQualitySignals {
    pub provider: String,
    pub session_id: String,
    pub user_turns: i64,
    pub debug_loop: bool,
    pub mid_session_error_paste: bool,
}

#[derive(Debug, Clone)]
struct CandidateCommit {
    commit_id: i64,
    repo_root: String,
    commit_sha: String,
    commit_time: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct MainlineIndex {
    by_path_hash: HashMap<(String, String), i64>,
    by_hash_paths: HashMap<String, HashMap<String, i64>>,
}

impl MainlineIndex {
    fn new() -> Self {
        Self {
            by_path_hash: HashMap::new(),
            by_hash_paths: HashMap::new(),
        }
    }

    fn add_max(&mut self, path: &str, line_hash: &str, count: i64) {
        let path_key = path.to_string();
        let hash_key = line_hash.to_string();

        let entry = self
            .by_path_hash
            .entry((path_key.clone(), hash_key.clone()))
            .or_insert(0);
        if count > *entry {
            *entry = count;
        }

        let path_map = self.by_hash_paths.entry(hash_key).or_default();
        let path_count = path_map.entry(path_key).or_insert(0);
        if count > *path_count {
            *path_count = count;
        }
    }

    fn strict_match(&self, path: &str, line_hash: &str, budget: i64) -> i64 {
        let avail = self
            .by_path_hash
            .get(&(path.to_string(), line_hash.to_string()))
            .copied()
            .unwrap_or(0);
        budget.min(avail)
    }
}

pub fn compute_quality_metrics(conn: &Connection) -> Result<QualityMetricsReport> {
    let s4 = compute_s4_debug_loop_metric(conn)?;
    let candidates = load_heavy_candidates(conn)?;
    let mut by_repo: BTreeMap<String, Vec<CandidateCommit>> = BTreeMap::new();
    for c in candidates {
        by_repo.entry(c.repo_root.clone()).or_default().push(c);
    }

    let mut overall_l4_num = 0i64;
    let mut overall_l4_den = 0i64;
    let mut overall_c2_num = 0i64;
    let mut overall_c2_den = 0i64;
    let mut overall_l1_num = 0i64;
    let mut overall_l1_den = 0i64;
    let mut repo_rows = Vec::new();

    for (repo_root, commits) in by_repo {
        let mut repo_l4_num = 0i64;
        let mut repo_l4_den = 0i64;
        let mut repo_c2_num = 0i64;
        let mut repo_c2_den = 0i64;
        let mut repo_l1_num = 0i64;
        let mut repo_l1_den = 0i64;
        let mut heavy_commits = 0i64;
        let mut note: Option<String> = None;

        let repo_ok = Path::new(&repo_root).join(".git").exists();
        if !repo_ok {
            note = Some("repo not found locally".to_string());
            repo_rows.push(RepoQualityMetrics {
                repo_root,
                heavy_commits,
                l4: make_lower_better(repo_l4_num, repo_l4_den, 2.0, 5.0),
                c2: make_higher_better(repo_c2_num, repo_c2_den, 80.0, 60.0),
                l1: make_lower_better(repo_l1_num, repo_l1_den, 15.0, 30.0),
                note,
            });
            continue;
        }

        let main_ref = resolve_mainline_ref(&repo_root)?;
        if main_ref.is_none() {
            note = Some("main/master ref not found".to_string());
        }

        let revert_map = build_revert_map(&repo_root)?;
        let mut budget_cache: HashMap<i64, HashMap<String, HashMap<String, i64>>> = HashMap::new();

        for commit in commits {
            if is_merge_commit(&repo_root, &commit.commit_sha)? {
                continue;
            }
            heavy_commits += 1;

            repo_l4_den += 1;
            if let Some(revert_time) = revert_map.get(&commit.commit_sha) {
                if *revert_time > commit.commit_time {
                    repo_l4_num += 1;
                }
            }

            let Some(ref_name) = main_ref.as_ref() else {
                continue;
            };

            repo_c2_den += 1;
            let mut merged = is_ancestor(&repo_root, &commit.commit_sha, ref_name)?;
            let budget = budget_cache
                .entry(commit.commit_id)
                .or_insert_with(|| build_ai_added_budget(conn, &commit).unwrap_or_default())
                .clone();

            if !merged {
                merged = is_content_merged_on_mainline(
                    &repo_root,
                    ref_name,
                    &commit.commit_time,
                    &budget,
                )?;
            }

            if merged {
                repo_c2_num += 1;
            }

            if !merged {
                continue;
            }

            let budget_total = budget_total(&budget);
            if budget_total <= 0 {
                continue;
            }
            repo_l1_den += budget_total;

            let end = commit.commit_time + Duration::days(14);
            let removed_index =
                build_mainline_removed_index(&repo_root, ref_name, &commit.commit_time, &end)?;
            repo_l1_num += compute_churn(&budget, &removed_index);
        }

        overall_l4_num += repo_l4_num;
        overall_l4_den += repo_l4_den;
        overall_c2_num += repo_c2_num;
        overall_c2_den += repo_c2_den;
        overall_l1_num += repo_l1_num;
        overall_l1_den += repo_l1_den;

        repo_rows.push(RepoQualityMetrics {
            repo_root,
            heavy_commits,
            l4: make_lower_better(repo_l4_num, repo_l4_den, 2.0, 5.0),
            c2: make_higher_better(repo_c2_num, repo_c2_den, 80.0, 60.0),
            l1: make_lower_better(repo_l1_num, repo_l1_den, 15.0, 30.0),
            note,
        });
    }

    Ok(QualityMetricsReport {
        l4: make_lower_better(overall_l4_num, overall_l4_den, 2.0, 5.0),
        c2: make_higher_better(overall_c2_num, overall_c2_den, 80.0, 60.0),
        l1: make_lower_better(overall_l1_num, overall_l1_den, 15.0, 30.0),
        s4,
        repos: repo_rows,
        sample_floor: SAMPLE_FLOOR,
    })
}

pub fn compute_session_quality_signals(conn: &Connection) -> Result<Vec<SessionQualitySignals>> {
    let sessions = load_session_messages(conn)?;
    let mut out = Vec::new();

    for (session, messages) in sessions {
        let turns = build_session_turns(&messages);
        if turns.is_empty() {
            continue;
        }

        let user_turns = turns
            .iter()
            .filter(|turn| !turn.user_text.trim().is_empty())
            .count() as i64;
        if user_turns <= 0 {
            continue;
        }

        out.push(SessionQualitySignals {
            provider: session.provider,
            session_id: session.session_id,
            user_turns,
            debug_loop: is_debug_loop_session(&turns),
            mid_session_error_paste: has_mid_session_error_paste(&messages),
        });
    }

    out.sort_by(|a, b| {
        a.provider
            .cmp(&b.provider)
            .then_with(|| a.session_id.cmp(&b.session_id))
    });
    Ok(out)
}

pub fn render_quality_metrics(report: &QualityMetricsReport) -> String {
    let mut out = String::new();
    out.push_str("AI Quality Metrics (heavy commits only)\n");
    out.push_str(&format!("Sample floor for status: {}\n\n", report.sample_floor));

    out.push_str(&render_metric_line(
        "L4 Revert Rate",
        &report.l4,
        Some("Healthy <2% | Warning >5%"),
    ));
    out.push('\n');
    out.push_str(&render_metric_line(
        "C2 Merge Rate (git proxy, squash-aware)",
        &report.c2,
        Some("Healthy >80% | Warning <60%"),
    ));
    out.push('\n');
    out.push_str(&render_metric_line(
        "L1 Code Churn Rate (14d, mainline-only)",
        &report.l1,
        Some("Healthy <15% | Warning >30%"),
    ));
    out.push('\n');
    out.push_str(&render_metric_line(
        "S4 Debug Loop Rate",
        &report.s4,
        Some("Healthy <20% | Warning >40%"),
    ));
    out.push_str("\n\nPer-repo breakdown\n");
    out.push_str(
        "Repo                                      Heavy   L4(rev/den)           C2(merged/den)        L1(churn/den)\n",
    );
    out.push_str(
        "────────────────────────────────────────  ─────  ────────────────────  ────────────────────  ────────────────────\n",
    );
    for r in &report.repos {
        out.push_str(&format!(
            "{:<40}  {:>5}  {:<20}  {:<20}  {:<20}\n",
            shorten_repo(&r.repo_root),
            r.heavy_commits,
            metric_compact(&r.l4),
            metric_compact(&r.c2),
            metric_compact(&r.l1),
        ));
        if let Some(note) = &r.note {
            out.push_str(&format!("  note: {}\n", note));
        }
    }

    out
}

fn render_metric_line(name: &str, metric: &RatioMetric, guidance: Option<&str>) -> String {
    let pct = metric
        .percent()
        .map(|v| format!("{:>6.2}%", v))
        .unwrap_or_else(|| "   N/A ".to_string());
    let base = format!(
        "{:<36} {} ({}/{})  [{}]",
        name,
        pct,
        metric.numerator,
        metric.denominator,
        metric.status.as_str()
    );
    if let Some(g) = guidance {
        format!("{}  {}", base, g)
    } else {
        base
    }
}

fn metric_compact(metric: &RatioMetric) -> String {
    if let Some(p) = metric.percent() {
        format!(
            "{:.1}% ({}/{}) {}",
            p,
            metric.numerator,
            metric.denominator,
            metric.status.as_str()
        )
    } else {
        format!("N/A ({}/{}) {}", metric.numerator, metric.denominator, metric.status.as_str())
    }
}

fn shorten_repo(path: &str) -> String {
    if let Some(home) = dirs::home_dir() {
        let home = home.to_string_lossy();
        if path.starts_with(home.as_ref()) {
            return format!("~{}", &path[home.len()..]);
        }
    }
    path.to_string()
}

fn compute_s4_debug_loop_metric(conn: &Connection) -> Result<RatioMetric> {
    let sessions = compute_session_quality_signals(conn)?;
    let total_sessions = sessions.len() as i64;
    let debug_loop_sessions = sessions.iter().filter(|s| s.debug_loop).count() as i64;

    Ok(make_lower_better(debug_loop_sessions, total_sessions, 20.0, 40.0))
}

fn load_session_messages(conn: &Connection) -> Result<BTreeMap<SessionKey, Vec<SessionMessage>>> {
    let mut stmt = conn.prepare(
        "SELECT provider, session_id, role, content
         FROM events
         WHERE event_type = 'message'
           AND role IN ('user', 'assistant')
         ORDER BY provider, session_id, COALESCE(event_ts, session_started_at), id",
    )?;

    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
        ))
    })?;

    let mut sessions: BTreeMap<SessionKey, Vec<SessionMessage>> = BTreeMap::new();
    for row in rows {
        let (provider, session_id, role, content) = row?;
        sessions
            .entry(SessionKey {
                provider,
                session_id,
            })
            .or_default()
            .push(SessionMessage { role, content });
    }
    Ok(sessions)
}

fn build_session_turns(messages: &[SessionMessage]) -> Vec<SessionTurn> {
    let mut turns = Vec::new();
    let mut current_user: Option<String> = None;
    let mut assistant_parts: Vec<&str> = Vec::new();

    for message in messages {
        match message.role.as_str() {
            "user" => {
                if let Some(user_text) = current_user.take() {
                    turns.push(SessionTurn {
                        user_text,
                        assistant_text: assistant_parts.join("\n"),
                    });
                    assistant_parts.clear();
                }
                current_user = Some(message.content.clone());
            }
            "assistant" => {
                if current_user.is_some() {
                    assistant_parts.push(message.content.as_str());
                }
            }
            _ => {}
        }
    }

    if let Some(user_text) = current_user {
        turns.push(SessionTurn {
            user_text,
            assistant_text: assistant_parts.join("\n"),
        });
    }

    turns
}

fn has_mid_session_error_paste(messages: &[SessionMessage]) -> bool {
    let mut user_message_index = 0usize;

    for message in messages {
        if message.role != "user" {
            continue;
        }

        user_message_index += 1;
        if user_message_index <= 1 {
            continue;
        }

        if contains_error_paste_signal(&message.content) {
            return true;
        }
    }

    false
}

fn contains_error_paste_signal(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();

    const STRONG_MARKERS: [&str; 17] = [
        "traceback (most recent call last):",
        "error ts",
        "typeerror:",
        "referenceerror:",
        "syntaxerror:",
        "runtimeerror:",
        "cannot find module",
        "module not found",
        "build failed",
        "test failed",
        "tests failed",
        "compilation failed",
        "failed with exit code",
        "panic:",
        "assertionerror",
        "exception:",
        "stack trace",
    ];
    if STRONG_MARKERS.iter().any(|marker| lower.contains(marker)) {
        return true;
    }

    if lower.contains("error[") || lower.contains("error:") || lower.contains("traceback") {
        return true;
    }

    if lower.contains(" failed")
        && (lower.contains("test")
            || lower.contains("build")
            || lower.contains("compile")
            || lower.contains("lint"))
    {
        return true;
    }

    contains_numbered_errors(&lower)
}

fn contains_numbered_errors(lower: &str) -> bool {
    let mut tokens = lower.split_whitespace().peekable();
    while let Some(token) = tokens.next() {
        let numeric = token
            .trim_matches(|c: char| !c.is_ascii_digit())
            .parse::<usize>()
            .ok();
        if numeric.is_none() {
            continue;
        }

        let Some(next) = tokens.peek() else {
            break;
        };
        let next_clean = next.trim_matches(|c: char| !c.is_ascii_alphabetic());
        if next_clean == "error" || next_clean == "errors" {
            return true;
        }
    }
    false
}

fn is_debug_loop_session(turns: &[SessionTurn]) -> bool {
    const LOOP_THRESHOLD: i64 = 5;

    let mut signature_counts: HashMap<String, i64> = HashMap::new();
    let mut previous_signature: Option<String> = None;

    for turn in turns {
        if turn.user_text.trim().is_empty() || turn.assistant_text.trim().is_empty() {
            continue;
        }

        if let Some(signature) = extract_error_signature(&turn.user_text, previous_signature.as_deref()) {
            let count = signature_counts.entry(signature.clone()).or_insert(0);
            *count += 1;
            if *count >= LOOP_THRESHOLD {
                return true;
            }
            previous_signature = Some(signature);
        } else {
            previous_signature = None;
        }
    }

    false
}

fn extract_error_signature(user_text: &str, previous_signature: Option<&str>) -> Option<String> {
    let lower = user_text.to_ascii_lowercase();

    if is_error_continuation(&lower) {
        if let Some(prev) = previous_signature {
            return Some(prev.to_string());
        }
    }

    if !contains_debug_keyword(&lower) {
        return None;
    }

    if let Some(line) = first_error_line(user_text) {
        let signature = normalize_signature(line);
        if !signature.is_empty() {
            return Some(signature);
        }
    }

    let signature = normalize_signature(user_text);
    if signature.is_empty() {
        None
    } else {
        Some(signature)
    }
}

fn contains_debug_keyword(text_lower: &str) -> bool {
    const KEYWORDS: [&str; 20] = [
        " error ",
        "error:",
        "exception",
        "traceback",
        "stack trace",
        "undefined",
        "not found",
        "failed",
        "failure",
        "cannot",
        "can't",
        "panic",
        "crash",
        "typeerror",
        "referenceerror",
        "syntaxerror",
        "runtimeerror",
        "build failed",
        "test failed",
        "compile error",
    ];

    let padded = format!(" {} ", text_lower);
    KEYWORDS.iter().any(|kw| padded.contains(kw))
}

fn is_error_continuation(text_lower: &str) -> bool {
    const PHRASES: [&str; 11] = [
        "same error",
        "same issue",
        "still failing",
        "still fails",
        "still broken",
        "not fixed",
        "didn't work",
        "didnt work",
        "doesn't work",
        "doesnt work",
        "still not working",
    ];
    PHRASES.iter().any(|phrase| text_lower.contains(phrase))
}

fn first_error_line(text: &str) -> Option<&str> {
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if contains_debug_keyword(&trimmed.to_ascii_lowercase()) {
            return Some(trimmed);
        }
    }
    None
}

fn normalize_signature(text: &str) -> String {
    let mut tokens = Vec::new();
    for raw in text.split_whitespace() {
        let trimmed = raw.trim_matches(|c: char| {
            !c.is_ascii_alphanumeric() && c != '/' && c != '\\' && c != '_' && c != '.' && c != ':' && c != '-'
        });
        if trimmed.is_empty() {
            continue;
        }

        let lower = trimmed.to_ascii_lowercase();
        let normalized = if lower.starts_with("http://") || lower.starts_with("https://") {
            "<url>".to_string()
        } else if lower.contains('/') || lower.contains('\\') {
            "<path>".to_string()
        } else if looks_like_hex_hash(&lower) {
            "<sha>".to_string()
        } else {
            collapse_digits(&lower)
        };
        tokens.push(normalized);
        if tokens.len() >= 18 {
            break;
        }
    }
    tokens.join(" ")
}

fn collapse_digits(token: &str) -> String {
    let mut out = String::with_capacity(token.len());
    let mut in_digits = false;
    for ch in token.chars() {
        if ch.is_ascii_digit() {
            if !in_digits {
                out.push('#');
                in_digits = true;
            }
        } else {
            in_digits = false;
            out.push(ch);
        }
    }
    out
}

fn looks_like_hex_hash(token: &str) -> bool {
    let len = token.len();
    len >= 8 && len <= 64 && token.chars().all(|ch| ch.is_ascii_hexdigit())
}

fn make_lower_better(numerator: i64, denominator: i64, healthy_lt: f64, warning_gt: f64) -> RatioMetric {
    RatioMetric {
        numerator,
        denominator,
        status: classify_lower_better(numerator, denominator, healthy_lt, warning_gt),
    }
}

fn make_higher_better(numerator: i64, denominator: i64, healthy_gt: f64, warning_lt: f64) -> RatioMetric {
    RatioMetric {
        numerator,
        denominator,
        status: classify_higher_better(numerator, denominator, healthy_gt, warning_lt),
    }
}

fn classify_lower_better(numerator: i64, denominator: i64, healthy_lt: f64, warning_gt: f64) -> MetricStatus {
    if denominator <= 0 {
        return MetricStatus::NotApplicable;
    }
    if denominator < SAMPLE_FLOOR {
        return MetricStatus::InsufficientData;
    }
    let pct = numerator as f64 / denominator as f64 * 100.0;
    if pct < healthy_lt {
        MetricStatus::Healthy
    } else if pct > warning_gt {
        MetricStatus::Warning
    } else {
        MetricStatus::Watch
    }
}

fn classify_higher_better(numerator: i64, denominator: i64, healthy_gt: f64, warning_lt: f64) -> MetricStatus {
    if denominator <= 0 {
        return MetricStatus::NotApplicable;
    }
    if denominator < SAMPLE_FLOOR {
        return MetricStatus::InsufficientData;
    }
    let pct = numerator as f64 / denominator as f64 * 100.0;
    if pct > healthy_gt {
        MetricStatus::Healthy
    } else if pct < warning_lt {
        MetricStatus::Warning
    } else {
        MetricStatus::Watch
    }
}

fn load_heavy_candidates(conn: &Connection) -> Result<Vec<CandidateCommit>> {
    let mut stmt = conn.prepare(
        "SELECT g.id, g.repo_root, g.commit_sha, g.commit_time
         FROM commit_ai_attributions a
         JOIN git_commits g ON g.id = a.commit_id
         WHERE a.heavy_ai = 1
         ORDER BY g.repo_root, g.commit_time",
    )?;

    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
        ))
    })?;

    let mut out = Vec::new();
    for row in rows {
        let (commit_id, repo_root, commit_sha, commit_time_raw) = row?;
        let commit_time = DateTime::parse_from_rfc3339(&commit_time_raw)
            .map_err(|e| anyhow!("invalid commit_time '{}': {}", commit_time_raw, e))?
            .with_timezone(&Utc);
        out.push(CandidateCommit {
            commit_id,
            repo_root,
            commit_sha,
            commit_time,
        });
    }
    Ok(out)
}

fn resolve_mainline_ref(repo_root: &str) -> Result<Option<String>> {
    let candidates = [
        "refs/heads/main",
        "refs/heads/master",
        "refs/remotes/origin/main",
        "refs/remotes/origin/master",
    ];
    for r in candidates {
        if git_ref_exists(repo_root, r)? {
            return Ok(Some(r.to_string()));
        }
    }
    Ok(None)
}

fn git_ref_exists(repo_root: &str, reference: &str) -> Result<bool> {
    let status = Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .arg("show-ref")
        .arg("--verify")
        .arg("--quiet")
        .arg(reference)
        .status()?;
    Ok(status.success())
}

fn is_merge_commit(repo_root: &str, commit_sha: &str) -> Result<bool> {
    let out = run_git_capture(
        repo_root,
        &[
            "rev-list".to_string(),
            "--parents".to_string(),
            "-n".to_string(),
            "1".to_string(),
            commit_sha.to_string(),
        ],
    )?;
    let line = out.trim();
    if line.is_empty() {
        return Ok(false);
    }
    let tokens: Vec<&str> = line.split_whitespace().collect();
    Ok(tokens.len() > 2)
}

fn build_revert_map(repo_root: &str) -> Result<HashMap<String, DateTime<Utc>>> {
    let out = run_git_capture(
        repo_root,
        &[
            "log".to_string(),
            "--all".to_string(),
            "--pretty=format:%H%x1f%cI%x1f%B%x1e".to_string(),
        ],
    )?;

    let mut map: HashMap<String, DateTime<Utc>> = HashMap::new();
    for record in out.split('\u{1e}') {
        let rec = record.trim();
        if rec.is_empty() {
            continue;
        }

        let mut parts = rec.splitn(3, '\u{1f}');
        let _sha = parts.next();
        let commit_time_raw = parts.next();
        let body = parts.next();
        let (Some(commit_time_raw), Some(body)) = (commit_time_raw, body) else {
            continue;
        };

        let commit_time = match DateTime::parse_from_rfc3339(commit_time_raw) {
            Ok(v) => v.with_timezone(&Utc),
            Err(_) => continue,
        };

        for target in extract_reverted_shas(body) {
            map.entry(target)
                .and_modify(|existing| {
                    if commit_time < *existing {
                        *existing = commit_time;
                    }
                })
                .or_insert(commit_time);
        }
    }
    Ok(map)
}

fn extract_reverted_shas(body: &str) -> Vec<String> {
    let mut out = Vec::new();
    let needle = "This reverts commit ";
    for line in body.lines() {
        let Some(start) = line.find(needle) else {
            continue;
        };
        let suffix = &line[start + needle.len()..];
        let sha: String = suffix.chars().take_while(|c| c.is_ascii_hexdigit()).collect();
        if sha.len() == 40 {
            out.push(sha.to_lowercase());
        }
    }
    out
}

fn build_ai_added_budget(
    conn: &Connection,
    commit: &CandidateCommit,
) -> Result<HashMap<String, HashMap<String, i64>>> {
    let mut commit_stmt = conn.prepare(
        "SELECT f.rel_path, h.line_hash, h.count
         FROM git_commit_file_diffs f
         JOIN git_commit_line_hashes h ON h.file_diff_id = f.id
         WHERE f.commit_id = ?1 AND h.side = '+'",
    )?;
    let mut avail_stmt = conn.prepare(
        "SELECT MAX(hol.count) AS avail
         FROM change_ops co
         JOIN change_op_line_hashes hol ON hol.op_id = co.id
         WHERE co.repo_root = ?1
           AND co.rel_path = ?2
           AND hol.side = '+'
           AND hol.line_hash = ?3
         GROUP BY co.provider",
    )?;

    let rows = commit_stmt.query_map(params![commit.commit_id], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, i64>(2)?,
        ))
    })?;

    let mut out: HashMap<String, HashMap<String, i64>> = HashMap::new();
    for row in rows {
        let (rel_path, line_hash, commit_count) = row?;
        let avail_rows = avail_stmt.query_map(
            params![commit.repo_root, rel_path, line_hash],
            |r| r.get::<_, i64>(0),
        )?;
        let mut avail_total = 0i64;
        for a in avail_rows {
            avail_total += a?;
        }
        let budget = commit_count.min(avail_total);
        if budget > 0 {
            out.entry(rel_path)
                .or_default()
                .entry(line_hash)
                .and_modify(|v| *v += budget)
                .or_insert(budget);
        }
    }
    Ok(out)
}

fn budget_total(budget: &HashMap<String, HashMap<String, i64>>) -> i64 {
    budget
        .values()
        .flat_map(|hashes| hashes.values())
        .copied()
        .sum()
}

fn is_content_merged_on_mainline(
    repo_root: &str,
    main_ref: &str,
    commit_time: &DateTime<Utc>,
    budget: &HashMap<String, HashMap<String, i64>>,
) -> Result<bool> {
    let total_budget = budget_total(budget);
    if total_budget <= 0 {
        return Ok(false);
    }

    let index = build_mainline_added_index(repo_root, main_ref, commit_time)?;
    let matched = match_budget_to_mainline(budget, &index);
    let ratio = matched as f64 / total_budget as f64;
    Ok(matched >= C2_MIN_MATCHED_LINES && ratio >= C2_MIN_RATIO)
}

fn build_mainline_added_index(
    repo_root: &str,
    main_ref: &str,
    since: &DateTime<Utc>,
) -> Result<MainlineIndex> {
    let since_iso = since.to_rfc3339_opts(SecondsFormat::Millis, true);
    let commits = list_commits_on_ref(repo_root, main_ref, Some(&since_iso), None)?;
    let mut index = MainlineIndex::new();
    for sha in commits {
        let diff = load_commit_diff(repo_root, &sha)?;
        for file in diff.file_diffs {
            for lh in file.line_hashes {
                if lh.side == LineSide::Added {
                    index.add_max(&file.rel_path, &lh.line_hash, lh.count);
                }
            }
        }
    }
    Ok(index)
}

fn build_mainline_removed_index(
    repo_root: &str,
    main_ref: &str,
    since: &DateTime<Utc>,
    until: &DateTime<Utc>,
) -> Result<HashMap<(String, String), i64>> {
    let since_iso = since.to_rfc3339_opts(SecondsFormat::Millis, true);
    let until_iso = until.to_rfc3339_opts(SecondsFormat::Millis, true);
    let commits = list_commits_on_ref(repo_root, main_ref, Some(&since_iso), Some(&until_iso))?;

    let mut out: HashMap<(String, String), i64> = HashMap::new();
    for sha in commits {
        let diff = load_commit_diff(repo_root, &sha)?;
        for file in diff.file_diffs {
            for lh in file.line_hashes {
                if lh.side == LineSide::Removed {
                    *out.entry((file.rel_path.clone(), lh.line_hash.clone()))
                        .or_insert(0) += lh.count;
                }
            }
        }
    }
    Ok(out)
}

fn compute_churn(
    budget: &HashMap<String, HashMap<String, i64>>,
    removed_index: &HashMap<(String, String), i64>,
) -> i64 {
    let mut churn = 0i64;
    for (path, hashes) in budget {
        for (line_hash, bcount) in hashes {
            let removed = removed_index
                .get(&(path.clone(), line_hash.clone()))
                .copied()
                .unwrap_or(0);
            churn += (*bcount).min(removed);
        }
    }
    churn
}

fn match_budget_to_mainline(
    budget: &HashMap<String, HashMap<String, i64>>,
    index: &MainlineIndex,
) -> i64 {
    let mut matched_total = 0i64;

    for (path, hashes) in budget {
        let file_total: i64 = hashes.values().copied().sum();
        if file_total <= 0 {
            continue;
        }

        let mut strict = 0i64;
        for (line_hash, bcount) in hashes {
            strict += index.strict_match(path, line_hash, *bcount);
        }
        let strict_ratio = strict as f64 / file_total as f64;
        let mut selected = strict;

        if strict_ratio < C2_STRICT_WEAK_RATIO {
            if let Some(alias_path) = choose_alias_path(path, hashes, file_total, index) {
                let mut alias_matched = 0i64;
                for (line_hash, bcount) in hashes {
                    alias_matched += index.strict_match(&alias_path, line_hash, *bcount);
                }
                if alias_matched > selected {
                    selected = alias_matched;
                }
            }
        }

        matched_total += selected;
    }

    matched_total
}

fn choose_alias_path(
    strict_path: &str,
    hashes: &HashMap<String, i64>,
    file_total: i64,
    index: &MainlineIndex,
) -> Option<String> {
    let mut scores: HashMap<String, i64> = HashMap::new();
    for (line_hash, bcount) in hashes {
        if let Some(path_counts) = index.by_hash_paths.get(line_hash) {
            for (candidate_path, candidate_count) in path_counts {
                if candidate_path == strict_path {
                    continue;
                }
                let matched = (*bcount).min(*candidate_count);
                if matched > 0 {
                    *scores.entry(candidate_path.clone()).or_insert(0) += matched;
                }
            }
        }
    }

    if scores.is_empty() {
        return None;
    }

    let mut all_candidates: Vec<(String, i64)> = scores.into_iter().collect();
    all_candidates.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    let strict_base = basename(strict_path).to_string();
    let mut filename_candidates: Vec<(String, i64)> = all_candidates
        .iter()
        .filter(|(path, _)| basename(path) == strict_base)
        .cloned()
        .collect();
    filename_candidates.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    if let Some(path) = pick_confident_alias(&filename_candidates, file_total) {
        return Some(path);
    }
    pick_confident_alias(&all_candidates, file_total)
}

fn pick_confident_alias(candidates: &[(String, i64)], file_total: i64) -> Option<String> {
    if candidates.is_empty() || file_total <= 0 {
        return None;
    }
    let (winner_path, winner_matched) = &candidates[0];
    let runner_matched = candidates.get(1).map(|(_, m)| *m).unwrap_or(0);

    let winner_ratio = *winner_matched as f64 / file_total as f64;
    let runner_ratio = runner_matched as f64 / file_total as f64;
    if winner_ratio >= C2_MIN_RATIO
        && *winner_matched >= C2_MIN_MATCHED_LINES
        && (winner_ratio - runner_ratio) >= C2_WINNER_MARGIN
    {
        Some(winner_path.clone())
    } else {
        None
    }
}

fn basename(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn list_commits_on_ref(
    repo_root: &str,
    reference: &str,
    since: Option<&str>,
    until: Option<&str>,
) -> Result<Vec<String>> {
    let mut args = vec!["rev-list".to_string(), "--reverse".to_string()];
    if let Some(s) = since {
        args.push(format!("--since={}", s));
    }
    if let Some(u) = until {
        args.push(format!("--until={}", u));
    }
    args.push(reference.to_string());
    let out = run_git_capture(repo_root, &args)?;
    Ok(out
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}

fn run_git_capture(repo_root: &str, args: &[String]) -> Result<String> {
    let output = Command::new("git")
        .arg("-c")
        .arg("core.quotepath=false")
        .arg("-C")
        .arg(repo_root)
        .args(args)
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let rendered = args.join(" ");
        return Err(anyhow!("git {} failed: {}", rendered, stderr));
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}
