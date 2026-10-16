//! `vca corpus review`: a localhost-only page to sample sessions, review them
//! and save approved ones into the reference corpus.
//!
//! Plain HTTP/1.1 on 127.0.0.1, one request at a time. Saving requires a JSON
//! body and, when the browser sends one, an Origin of this server, so another
//! web page cannot write snapshots through it.

use anyhow::{Context, Result};
use percent_encoding::percent_decode_str;
use rusqlite::Connection;
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;

use crate::explore::{self, Filters, SessionSort, short_repo};

const APP: &str = include_str!("app.html");
/// Largest request body accepted (expectations JSON).
const MAX_BODY: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct SampleOptions {
    /// Case-insensitive substrings of the repository path or name.
    pub projects: Vec<String>,
    pub providers: Vec<String>,
    /// Sessions per (project, tool).
    pub per_group: usize,
    pub seed: u64,
    pub min_turns: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Candidate {
    pub provider: String,
    pub session_id: String,
    pub title: String,
    pub project: String,
    pub repo_root: Option<String>,
    pub started_at: Option<String>,
    pub turns: i64,
    pub agents: i64,
    pub cost: Option<f64>,
    pub saved_at: Option<String>,
    pub approved: Option<usize>,
}

/// Up to `per_group` sessions per (project, tool), in seeded random order.
/// Sessions already in the corpus are always listed.
pub fn sample(
    conn: &Connection,
    root: &std::path::Path,
    opts: &SampleOptions,
) -> Result<Vec<Candidate>> {
    let saved: HashMap<String, super::SavedSnapshot> = super::list(root)?
        .into_iter()
        .map(|s| (s.manifest.key.clone(), s))
        .collect();
    let filters = Filters {
        providers: opts.providers.clone(),
        ..Filters::default()
    };
    let rows = explore::sessions(conn, &filters, SessionSort::Started, 1_000_000)?;
    let wanted = |repo: &str| {
        let repo = repo.to_ascii_lowercase();
        opts.projects.is_empty()
            || opts
                .projects
                .iter()
                .any(|p| repo.contains(&p.to_ascii_lowercase()))
    };
    let mut groups: BTreeMap<(String, String), Vec<Candidate>> = BTreeMap::new();
    for row in rows {
        let key = format!("{}:{}", row.provider, row.session_id);
        let repo = row.repo_root.clone().unwrap_or_default();
        let is_saved = saved.get(&key);
        if is_saved.is_none() && (repo.is_empty() || !wanted(&repo) || row.turns < opts.min_turns) {
            continue;
        }
        let project = if repo.is_empty() {
            "(no repo)".into()
        } else {
            short_repo(&repo)
        };
        groups
            .entry((project.clone(), row.provider.clone()))
            .or_default()
            .push(Candidate {
                provider: row.provider.clone(),
                session_id: row.session_id.clone(),
                title: row.title.clone(),
                project,
                repo_root: row.repo_root.clone(),
                started_at: row.started_at.clone(),
                turns: row.turns,
                agents: row.agents,
                cost: row.cost,
                saved_at: is_saved.map(|s| s.manifest.created_at.clone()),
                approved: is_saved.map(|s| s.approved),
            });
    }
    let mut out = Vec::new();
    for (_, mut list) in groups {
        list.sort_by_key(|c| {
            super::sample_rank(opts.seed, &format!("{}:{}", c.provider, c.session_id))
        });
        let (kept, rest): (Vec<_>, Vec<_>) = list.into_iter().partition(|c| c.saved_at.is_some());
        let room = opts.per_group.saturating_sub(kept.len());
        out.extend(kept);
        out.extend(rest.into_iter().take(room));
    }
    Ok(out)
}

pub fn serve(port: u16, opts: SampleOptions, open_browser: bool) -> Result<()> {
    let root = super::corpus_dir()?;
    std::fs::create_dir_all(&root)?;
    let listener = TcpListener::bind(("127.0.0.1", port))
        .with_context(|| format!("cannot listen on 127.0.0.1:{port}"))?;
    let addr = listener.local_addr()?;
    let origin = format!("http://{addr}");
    println!("Corpus review at {origin}/  (corpus: {})", root.display());
    println!("Only this machine can reach it. Ctrl+C to stop.");
    if open_browser {
        open_url(&origin);
    }
    let mut state = State { root, opts, origin };
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        if let Err(error) = state.handle(stream) {
            eprintln!("request failed: {error:#}");
        }
    }
    Ok(())
}

struct State {
    root: PathBuf,
    opts: SampleOptions,
    origin: String,
}

struct Request {
    method: String,
    path: String,
    query: HashMap<String, String>,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

impl State {
    fn handle(&mut self, mut stream: TcpStream) -> Result<()> {
        let request = read_request(&stream)?;
        let response = match self.route(&request) {
            Ok(response) => response,
            Err(error) => Response::json(500, &json!({ "error": format!("{error:#}") })),
        };
        response.write(&mut stream)
    }

    fn route(&mut self, req: &Request) -> Result<Response> {
        let parts: Vec<String> = req
            .path
            .trim_start_matches('/')
            .split('/')
            .map(|p| percent_decode_str(p).decode_utf8_lossy().to_string())
            .collect();
        match (
            req.method.as_str(),
            parts
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .as_slice(),
        ) {
            ("GET", [""]) => {
                if let Some(seed) = req.query.get("seed").and_then(|s| s.parse().ok()) {
                    self.opts.seed = seed;
                }
                if let Some(per) = req.query.get("per").and_then(|s| s.parse().ok()) {
                    self.opts.per_group = per;
                }
                let conn = crate::db::open()?;
                let candidates = sample(&conn, &self.root, &self.opts)?;
                let data = json!({
                    "corpus": self.root,
                    "seed": self.opts.seed,
                    "per": self.opts.per_group,
                    "projects": self.opts.projects,
                    "candidates": candidates,
                });
                Ok(Response::html(APP.replace(
                    "/*__CORPUS_APP__*/null",
                    &serde_json::to_string(&data)?.replace('<', "\\u003c"),
                )))
            }
            ("GET", ["open"]) => {
                let input = req.query.get("session").cloned().unwrap_or_default();
                let conn = crate::db::open()?;
                let (provider, session) =
                    crate::commands::insights::resolve_session(&conn, input.trim())?;
                Ok(Response::redirect(&format!(
                    "/review/{provider}/{}",
                    percent_encoding::utf8_percent_encode(
                        &session,
                        percent_encoding::NON_ALPHANUMERIC
                    )
                )))
            }
            ("GET", ["review", provider, session]) => {
                let conn = crate::db::open()?;
                let Some(data) = explore::review::review(&conn, provider, session)? else {
                    return Ok(Response::text(404, "session not found"));
                };
                let dir = self.root.join(super::snapshot_name(provider, session));
                let saved_at = super::read_manifest(&dir).ok().map(|m| m.created_at);
                let corpus = json!({
                    "save": format!("/api/save/{provider}/{}",
                        percent_encoding::utf8_percent_encode(session, percent_encoding::NON_ALPHANUMERIC)),
                    "saved_at": saved_at,
                });
                Ok(Response::html(explore::review::render_html_for_corpus(
                    &data, &corpus,
                )?))
            }
            ("POST", ["api", "save", provider, session]) => {
                if let Some(origin) = req.headers.get("origin")
                    && origin != &self.origin
                {
                    return Ok(Response::json(
                        403,
                        &json!({ "error": "cross-origin request refused" }),
                    ));
                }
                if !req
                    .headers
                    .get("content-type")
                    .is_some_and(|t| t.starts_with("application/json"))
                {
                    return Ok(Response::json(
                        415,
                        &json!({ "error": "expected application/json" }),
                    ));
                }
                let expectations: explore::review::Expectations =
                    serde_json::from_slice(&req.body)?;
                if expectations.session != format!("{provider}:{session}") {
                    return Ok(Response::json(
                        400,
                        &json!({ "error": "expectations are for another session" }),
                    ));
                }
                let conn = crate::db::open()?;
                println!("saving {provider}:{session} and replaying its project…");
                let exe = std::env::current_exe()?;
                let saved = super::save_reviewed(
                    &conn,
                    &self.root,
                    &exe,
                    provider,
                    session,
                    &expectations,
                )?;
                println!(
                    "saved {provider}:{session} → {} ({} files, {} approved)",
                    saved.dir.display(),
                    saved.files,
                    saved.approved
                );
                for warning in &saved.warnings {
                    println!("  note: {warning}");
                }
                Ok(Response::json(
                    200,
                    &json!({
                        "dir": saved.dir,
                        "files": saved.files,
                        "bytes": saved.bytes,
                        "approved": saved.approved,
                        "warnings": saved.warnings,
                    }),
                ))
            }
            _ => Ok(Response::text(404, "not found")),
        }
    }
}

fn read_request(stream: &TcpStream) -> Result<Request> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let mut words = line.split_whitespace();
    let method = words.next().unwrap_or_default().to_string();
    let target = words.next().unwrap_or("/").to_string();
    let mut headers = HashMap::new();
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header)? == 0 || header.trim().is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }
    let length: usize = headers
        .get("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    anyhow::ensure!(length <= MAX_BODY, "request body too large");
    let mut body = vec![0; length];
    reader.read_exact(&mut body)?;
    let (path, query) = match target.split_once('?') {
        Some((path, query)) => (path.to_string(), parse_query(query)),
        None => (target, HashMap::new()),
    };
    Ok(Request {
        method,
        path,
        query,
        headers,
        body,
    })
}

fn parse_query(query: &str) -> HashMap<String, String> {
    query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .map(|(k, v)| {
            let decode = |s: &str| {
                percent_decode_str(&s.replace('+', " "))
                    .decode_utf8_lossy()
                    .to_string()
            };
            (decode(k), decode(v))
        })
        .collect()
}

struct Response {
    status: u16,
    content_type: &'static str,
    location: Option<String>,
    body: Vec<u8>,
}

impl Response {
    fn html(body: String) -> Self {
        Self {
            status: 200,
            content_type: "text/html; charset=utf-8",
            location: None,
            body: body.into_bytes(),
        }
    }
    fn text(status: u16, body: &str) -> Self {
        Self {
            status,
            content_type: "text/plain; charset=utf-8",
            location: None,
            body: body.as_bytes().to_vec(),
        }
    }
    fn json(status: u16, value: &Value) -> Self {
        Self {
            status,
            content_type: "application/json",
            location: None,
            body: value.to_string().into_bytes(),
        }
    }
    fn redirect(to: &str) -> Self {
        Self {
            status: 303,
            content_type: "text/plain",
            location: Some(to.to_string()),
            body: Vec::new(),
        }
    }
    fn write(&self, stream: &mut TcpStream) -> Result<()> {
        let reason = match self.status {
            200 => "OK",
            303 => "See Other",
            400 => "Bad Request",
            403 => "Forbidden",
            404 => "Not Found",
            415 => "Unsupported Media Type",
            _ => "Error",
        };
        let mut head = format!(
            "HTTP/1.1 {} {reason}\r\nContent-Type: {}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n",
            self.status,
            self.content_type,
            self.body.len()
        );
        if let Some(location) = &self.location {
            head.push_str(&format!("Location: {location}\r\n"));
        }
        head.push_str("\r\n");
        stream.write_all(head.as_bytes())?;
        stream.write_all(&self.body)?;
        Ok(())
    }
}

fn open_url(url: &str) {
    let result = if cfg!(windows) {
        std::process::Command::new("cmd")
            .args(["/C", "start", "", url])
            .spawn()
    } else if cfg!(target_os = "macos") {
        std::process::Command::new("open").arg(url).spawn()
    } else {
        std::process::Command::new("xdg-open").arg(url).spawn()
    };
    if result.is_err() {
        println!("Open {url} in a browser.");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_query_strings() {
        let q = parse_query("seed=7&session=claude%3Aabc&x=a+b");
        assert_eq!(q["seed"], "7");
        assert_eq!(q["session"], "claude:abc");
        assert_eq!(q["x"], "a b");
    }
}
