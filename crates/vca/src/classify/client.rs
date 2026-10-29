//! Decision client: one request = one state + independent typed questions.
//! The OpenRouter Decisions endpoint is an alpha API, so everything above
//! this module talks to the `DecisionClient` trait only.

use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use crate::classify::config::ProviderPrefs;

#[derive(Debug, Clone)]
pub struct DecisionRequest {
    pub model: String,
    pub state: Value,
    pub questions: Map<String, Value>,
    pub provider: ProviderPrefs,
}

impl DecisionRequest {
    pub fn body(&self) -> Value {
        json!({
            "model": self.model,
            "state": self.state,
            "questions": self.questions,
            "provider": {"zdr": self.provider.zdr, "allow_fallbacks": self.provider.allow_fallbacks},
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Answer {
    /// `choice` label; for `noul` "true"/"false"; for `score` the nearest level.
    pub label: Option<String>,
    pub score: Option<f64>,
    /// `noul` probability of true.
    pub probability: Option<f64>,
    pub probabilities: Option<Value>,
    pub confidence: Option<f64>,
}

#[derive(Debug, Clone, Default)]
pub struct DecisionResponse {
    pub answers: BTreeMap<String, Answer>,
    pub input_tokens: Option<i64>,
    pub cost_usd: Option<f64>,
    pub http_status: u16,
    pub attempts: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Retries exhausted on 429/5xx/network; retried on the next run.
    Transient,
    /// 413: the caller may rebuild a smaller state.
    TooLarge,
    /// 400 and other 4xx: not retried.
    InvalidRequest,
    /// 401/403: abort the run.
    Auth,
    /// 402: abort the run.
    Credits,
}

#[derive(Debug, Clone)]
pub struct ClientError {
    pub kind: ErrorKind,
    pub status: Option<u16>,
    pub message: String,
    pub attempts: u32,
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.status {
            Some(status) => write!(f, "{:?} (HTTP {status}): {}", self.kind, self.message),
            None => write!(f, "{:?}: {}", self.kind, self.message),
        }
    }
}

pub trait DecisionClient {
    fn decide(&self, request: &DecisionRequest) -> Result<DecisionResponse, ClientError>;
    /// The coordinator invokes the callback as each response arrives; it owns SQLite and budgets.
    fn decide_many(
        &self,
        requests: &[DecisionRequest],
        callback: &mut dyn FnMut(usize, Result<DecisionResponse, ClientError>),
    ) {
        for (index, request) in requests.iter().enumerate() {
            callback(index, self.decide(request));
        }
    }
}

/// Parse the `answers` and `usage` of a Decisions response.
pub fn parse_response(body: &Value, status: u16, attempts: u32) -> DecisionResponse {
    let mut answers = BTreeMap::new();
    if let Some(map) = body.get("answers").and_then(Value::as_object) {
        for (name, raw) in map {
            answers.insert(name.clone(), parse_answer(raw));
        }
    }
    let usage = body.get("usage");
    DecisionResponse {
        answers,
        input_tokens: usage
            .and_then(|u| u.get("input_tokens"))
            .and_then(Value::as_i64),
        cost_usd: usage.and_then(|u| u.get("cost")).and_then(Value::as_f64),
        http_status: status,
        attempts,
    }
}

fn parse_answer(raw: &Value) -> Answer {
    let kind = raw.get("type").and_then(Value::as_str).unwrap_or("");
    let probabilities = raw.get("probabilities").cloned();
    let confidence = raw.get("confidence").and_then(Value::as_f64);
    match kind {
        "noul" => {
            let probability = probabilities
                .as_ref()
                .and_then(|p| p.get("true"))
                .and_then(Value::as_f64)
                .or_else(|| raw.get("probability").and_then(Value::as_f64))
                .or_else(|| match raw.get("noul") {
                    Some(Value::Bool(value)) => Some(if *value { 1.0 } else { 0.0 }),
                    Some(Value::Number(value)) => value.as_f64(),
                    _ => None,
                });
            Answer {
                label: probability.map(|p| if p >= 0.5 { "true" } else { "false" }.to_string()),
                probability,
                probabilities,
                confidence,
                ..Answer::default()
            }
        }
        "score" => {
            let score = raw.get("score").and_then(Value::as_f64);
            Answer {
                label: score.map(|value| format!("{}", value.round() as i64)),
                score,
                probabilities,
                confidence,
                ..Answer::default()
            }
        }
        _ => Answer {
            label: raw
                .get("choice")
                .and_then(Value::as_str)
                .map(str::to_string),
            probabilities,
            confidence,
            ..Answer::default()
        },
    }
}

/// Confidence band: choice/score by `confidence`, noul by distance from 0.5.
pub fn band(question: &Value, answer: &Answer) -> &'static str {
    if question.get("type").and_then(Value::as_str) == Some("noul") {
        return match answer.probability {
            Some(p) if !(0.3..0.7).contains(&p) => "confident",
            Some(_) => "uncertain",
            None => "invalid",
        };
    }
    match answer.confidence {
        Some(c) if c >= 0.6 => "confident",
        Some(c) if c >= 0.35 => "tentative",
        Some(_) => "uncertain",
        // A label without confidence is usable but not trusted.
        None if answer.label.is_some() => "tentative",
        None => "invalid",
    }
}

/// Whether an answer fits the question (label in criteria, score in range).
pub fn answer_valid(question: &Value, answer: &Answer) -> bool {
    match question.get("type").and_then(Value::as_str) {
        Some("choice") => answer.label.as_deref().is_some_and(|label| {
            question
                .get("criteria")
                .and_then(Value::as_object)
                .is_some_and(|criteria| criteria.contains_key(label))
        }),
        Some("noul") => answer.probability.is_some_and(|p| (0.0..=1.0).contains(&p)),
        Some("score") => {
            let levels = question
                .get("criteria")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or(0);
            answer
                .score
                .is_some_and(|s| s >= 0.0 && s <= levels.saturating_sub(1) as f64 + 1e-6)
        }
        _ => false,
    }
}

// ── OpenRouter ───────────────────────────────────────────────────────────

pub struct OpenRouterClient {
    endpoint: String,
    key: String,
    http: reqwest::Client,
    runtime: tokio::runtime::Runtime,
    /// Base delay for backoff; tests set it near zero.
    pub base_delay: Duration,
}

impl OpenRouterClient {
    pub fn new(endpoint: &str, key: &str) -> anyhow::Result<Self> {
        let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(60));
        if endpoint.starts_with("http://127.0.0.1") || endpoint.starts_with("http://localhost") {
            builder = builder.no_proxy();
        }
        Ok(Self {
            endpoint: endpoint.to_string(),
            key: key.to_string(),
            http: builder.build()?,
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?,
            base_delay: Duration::from_secs(1),
        })
    }

    fn transport(&self) -> Transport {
        Transport {
            endpoint: self.endpoint.clone(),
            key: self.key.clone(),
            http: self.http.clone(),
            base_delay: self.base_delay,
        }
    }
}
#[derive(Clone)]
struct Transport {
    endpoint: String,
    key: String,
    http: reqwest::Client,
    base_delay: Duration,
}
impl Transport {
    async fn post(&self, body: &Value) -> Result<(u16, Option<u64>, String), String> {
        let response = self
            .http
            .post(&self.endpoint)
            .bearer_auth(&self.key)
            .header("X-Title", "AI Engineering Analytics")
            .json(body)
            .send()
            .await
            .map_err(|err| err.to_string())?;
        let status = response.status().as_u16();
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<u64>().ok());
        let text = response.text().await.map_err(|err| err.to_string())?;
        Ok((status, retry_after, text))
    }
    async fn decide(&self, request: &DecisionRequest) -> Result<DecisionResponse, ClientError> {
        let body = request.body();
        let mut attempts = 0u32;
        let mut rate_limited = 0u32;
        let mut transient = 0u32;
        loop {
            attempts += 1;
            let (status, retry_after, message) = match self.post(&body).await {
                Ok((status, retry_after, text)) => {
                    if status == 200 {
                        return match serde_json::from_str::<Value>(&text) {
                            Ok(value) => Ok(parse_response(&value, status, attempts)),
                            Err(err) => Err(ClientError {
                                kind: ErrorKind::InvalidRequest,
                                status: Some(status),
                                message: format!("unparseable response: {err}"),
                                attempts,
                            }),
                        };
                    }
                    (
                        Some(status),
                        retry_after,
                        text.chars().take(300).collect::<String>(),
                    )
                }
                Err(err) => (None, None, err),
            };
            let fail = |kind| ClientError {
                kind,
                status,
                message: message.clone(),
                attempts,
            };
            match status {
                Some(429 | 503 | 529) => {
                    rate_limited += 1;
                    if rate_limited >= 5 {
                        return Err(fail(ErrorKind::Transient));
                    }
                    let backoff = self.base_delay * 2u32.pow(rate_limited - 1);
                    let wait = retry_after
                        .map(|seconds| Duration::from_secs(seconds).min(Duration::from_secs(60)))
                        .unwrap_or(backoff)
                        .min(self.base_delay * 32);
                    tokio::time::sleep(wait).await;
                }
                None | Some(500..=599) => {
                    transient += 1;
                    if transient > 3 {
                        return Err(fail(ErrorKind::Transient));
                    }
                    tokio::time::sleep(self.base_delay * transient).await;
                }
                Some(413) => return Err(fail(ErrorKind::TooLarge)),
                Some(401 | 403) => return Err(fail(ErrorKind::Auth)),
                Some(402) => return Err(fail(ErrorKind::Credits)),
                Some(_) => return Err(fail(ErrorKind::InvalidRequest)),
            }
        }
    }
}
impl DecisionClient for OpenRouterClient {
    fn decide(&self, request: &DecisionRequest) -> Result<DecisionResponse, ClientError> {
        self.runtime.block_on(self.transport().decide(request))
    }
    fn decide_many(
        &self,
        requests: &[DecisionRequest],
        callback: &mut dyn FnMut(usize, Result<DecisionResponse, ClientError>),
    ) {
        let mut tasks = tokio::task::JoinSet::new();
        let mut completed = std::collections::BTreeSet::new();
        {
            let _runtime = self.runtime.enter();
            for (index, request) in requests.iter().cloned().enumerate() {
                let transport = self.transport();
                tasks.spawn(async move { (index, transport.decide(&request).await) });
            }
        }
        while let Some(result) = self.runtime.block_on(tasks.join_next()) {
            match result {
                Ok((index, response)) => {
                    completed.insert(index);
                    callback(index, response);
                }
                Err(_) => {
                    if let Some(index) = (0..requests.len()).find(|i| !completed.contains(i)) {
                        completed.insert(index);
                        callback(
                            index,
                            Err(ClientError {
                                kind: ErrorKind::Transient,
                                status: None,
                                message: "Classifier request worker failed".into(),
                                attempts: 1,
                            }),
                        );
                    }
                }
            }
        }
    }
}

// ── Test double ──────────────────────────────────────────────────────────

type Responder = dyn Fn(&DecisionRequest) -> Result<DecisionResponse, ClientError> + Send + Sync;

/// A client that answers from a closure and records every request.
pub struct ScriptedClient {
    responder: Box<Responder>,
    pub requests: Mutex<Vec<DecisionRequest>>,
}

impl ScriptedClient {
    pub fn new(
        responder: impl Fn(&DecisionRequest) -> Result<DecisionResponse, ClientError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            responder: Box::new(responder),
            requests: Mutex::new(Vec::new()),
        }
    }

    pub fn calls(&self) -> usize {
        self.requests.lock().map(|r| r.len()).unwrap_or(0)
    }
}

impl DecisionClient for ScriptedClient {
    fn decide(&self, request: &DecisionRequest) -> Result<DecisionResponse, ClientError> {
        if let Ok(mut requests) = self.requests.lock() {
            requests.push(request.clone());
        }
        (self.responder)(request)
    }
}

/// Build a response answering each question with a fixed label (choice),
/// probability (noul, from "true"/"false") or score.
pub fn canned_response(
    request: &DecisionRequest,
    pick: impl Fn(&str, &Value) -> (String, f64),
) -> DecisionResponse {
    let mut answers = BTreeMap::new();
    for (name, question) in &request.questions {
        let (label, confidence) = pick(name, question);
        let answer = match question.get("type").and_then(Value::as_str) {
            Some("noul") => {
                let p = if label == "true" {
                    confidence
                } else {
                    1.0 - confidence
                };
                Answer {
                    label: Some(label),
                    probability: Some(p),
                    ..Answer::default()
                }
            }
            Some("score") => {
                let score = label.parse::<f64>().unwrap_or(0.0);
                Answer {
                    label: Some(label),
                    score: Some(score),
                    confidence: Some(confidence),
                    ..Answer::default()
                }
            }
            _ => Answer {
                label: Some(label),
                confidence: Some(confidence),
                ..Answer::default()
            },
        };
        answers.insert(name.clone(), answer);
    }
    let tokens = (request.body().to_string().len() / 4) as i64;
    DecisionResponse {
        answers,
        input_tokens: Some(tokens),
        cost_usd: Some(tokens as f64 * 0.042 / 1_000_000.0),
        http_status: 200,
        attempts: 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;

    fn serve(
        responses: Vec<(u16, &'static str)>,
    ) -> (String, std::thread::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = format!("http://{}/decisions", listener.local_addr().unwrap());
        let handle = std::thread::spawn(move || {
            let mut bodies = Vec::new();
            for (status, body) in responses {
                let (stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream);
                let mut length = 0usize;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap();
                    }
                }
                let mut request_body = vec![0; length];
                reader.read_exact(&mut request_body).unwrap();
                bodies.push(String::from_utf8_lossy(&request_body).to_string());
                let mut stream = reader.into_inner();
                write!(
                    stream,
                    "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
            bodies
        });
        (address, handle)
    }

    fn request() -> DecisionRequest {
        let mut questions = Map::new();
        questions.insert(
            "intent".into(),
            json!({"type": "choice", "instructions": "x", "criteria": {"feature": "a", "bug_fix": "b"}}),
        );
        DecisionRequest {
            model: "typesafe/jev-1.13".into(),
            state: json!({"unit": "task"}),
            questions,
            provider: ProviderPrefs::default(),
        }
    }

    #[test]
    fn concurrent_batch_dispatches_four_before_any_response() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}/decisions", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            let mut streams = Vec::new();
            while streams.len() < 4 && std::time::Instant::now() < deadline {
                match listener.accept() {
                    Ok((stream, _)) => streams.push(stream),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5))
                    }
                    Err(e) => panic!("{e}"),
                }
            }
            let n = streams.len();
            for stream in streams {
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut reader = BufReader::new(stream);
                let mut length = 0;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                    if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = v.trim().parse().unwrap();
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                let response = r#"{"answers":{},"usage":{"cost":0.01}}"#;
                write!(reader.into_inner(),"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",response.len()).unwrap();
            }
            n
        });
        let client = OpenRouterClient::new(&endpoint, "test-only").unwrap();
        let mut received = Vec::new();
        client.decide_many(&vec![request(); 4], &mut |index, result| {
            received.push((index, result.unwrap().cost_usd))
        });
        assert_eq!(server.join().unwrap(), 4);
        received.sort_by_key(|v| v.0);
        assert_eq!(
            received,
            (0..4).map(|i| (i, Some(0.01))).collect::<Vec<_>>()
        );
    }

    #[test]
    fn retries_overload_then_parses_answers_and_usage() {
        let ok = r#"{"answers":{"intent":{"type":"choice","choice":"bug_fix","probabilities":{"feature":0.2,"bug_fix":0.8},"confidence":0.71}},"usage":{"input_tokens":120,"output_tokens":0,"cost":0.000005}}"#;
        let (endpoint, server) = serve(vec![(503, "{}"), (200, ok)]);
        let mut client = OpenRouterClient::new(&endpoint, "k").unwrap();
        client.base_delay = Duration::from_millis(1);
        let response = client.decide(&request()).unwrap();
        assert_eq!(response.attempts, 2);
        assert_eq!(response.input_tokens, Some(120));
        assert_eq!(response.answers["intent"].label.as_deref(), Some("bug_fix"));
        let bodies = server.join().unwrap();
        let sent: Value = serde_json::from_str(&bodies[1]).unwrap();
        assert_eq!(sent["model"], "typesafe/jev-1.13");
        assert_eq!(sent["provider"]["zdr"], true);
        assert_eq!(sent["questions"]["intent"]["type"], "choice");
    }

    #[test]
    fn maps_http_statuses_to_error_kinds() {
        for (status, kind) in [
            (401, ErrorKind::Auth),
            (402, ErrorKind::Credits),
            (413, ErrorKind::TooLarge),
            (400, ErrorKind::InvalidRequest),
        ] {
            let (endpoint, server) = serve(vec![(status, r#"{"error":"x"}"#)]);
            let client = OpenRouterClient::new(&endpoint, "k").unwrap();
            let error = client.decide(&request()).unwrap_err();
            assert_eq!(error.kind, kind, "status {status}");
            server.join().unwrap();
        }
    }

    #[test]
    fn bands_and_validation() {
        let choice = json!({"type": "choice", "criteria": {"a": "", "b": ""}});
        let answer = Answer {
            label: Some("a".into()),
            confidence: Some(0.4),
            ..Answer::default()
        };
        assert_eq!(band(&choice, &answer), "tentative");
        assert!(answer_valid(&choice, &answer));
        let wrong = Answer {
            label: Some("z".into()),
            confidence: Some(0.9),
            ..Answer::default()
        };
        assert!(!answer_valid(&choice, &wrong));
        let noul = json!({"type": "noul"});
        let yes =
            parse_answer(&json!({"type": "noul", "probabilities": {"true": 0.82, "false": 0.18}}));
        assert_eq!(yes.label.as_deref(), Some("true"));
        assert_eq!(band(&noul, &yes), "confident");
        let unsure = Answer {
            probability: Some(0.55),
            ..Answer::default()
        };
        assert_eq!(band(&noul, &unsure), "uncertain");
        let score = json!({"type": "score", "criteria": ["a", "b", "c"]});
        let parsed = parse_answer(&json!({"type": "score", "score": 1.6, "confidence": 0.7}));
        assert_eq!(parsed.label.as_deref(), Some("2"));
        assert!(answer_valid(&score, &parsed));
    }
}
