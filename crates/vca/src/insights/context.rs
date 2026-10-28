//! Context ledger: what filled each conversation's context and what carrying
//! it cost.
//!
//! For every API call we know the observed context size (input + cache read +
//! cache write). Items that entered between two calls (tool results, user
//! messages, injected instructions) get estimated token sizes calibrated to
//! the observed growth. Each call's input-side cost is then split exactly:
//! newly written tokens pay the fresh/write cost, carried tokens pay the cache
//! read cost, and on a cache rebuild carried tokens pay the rewrite. Item
//! cost therefore sums to the session's input-side cost, with the baseline
//! (system prompt and tool schemas that are not logged) and any unexplained
//! residual shown as their own items.

use anyhow::Result;
use rusqlite::{Connection, params};
use std::collections::{BTreeMap, HashMap};

use super::structure::parse_ts;
use super::tools::{mcp_server, target_dir};
use crate::cost::pricing;

const CHARS_PER_TOKEN_CODE: f64 = 3.6;
const CHARS_PER_TOKEN_PROSE: f64 = 4.2;
const CALIBRATION_MIN: f64 = 0.6;
const CALIBRATION_MAX: f64 = 1.6;
/// Context shrinking below this share of the previous call means compaction.
const COMPACTION_SHRINK: f64 = 0.8;
/// Cache reads below this share of carried tokens mean the cache was rebuilt.
const REBUILD_READ_SHARE: f64 = 0.5;

pub(crate) const CONTEXT_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS derived_api_call (
    provider          TEXT NOT NULL,
    session_id        TEXT NOT NULL,
    call_index        INTEGER NOT NULL,
    usage_id          INTEGER NOT NULL,
    ts                TEXT,
    model_name        TEXT,
    context_tokens    INTEGER NOT NULL,
    new_tokens        INTEGER NOT NULL,
    carried_tokens    INTEGER NOT NULL,
    cache_event       TEXT NOT NULL,
    input_cost_usd    REAL,
    rebuild_extra_usd REAL,
    residual_tokens   INTEGER NOT NULL DEFAULT 0,
    context_window    INTEGER,
    PRIMARY KEY(provider, session_id, call_index)
);

CREATE TABLE IF NOT EXISTS derived_context_item (
    provider        TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    item_key        TEXT NOT NULL,
    ts              TEXT,
    kind            TEXT NOT NULL,
    tool_name       TEXT,
    tool_category   TEXT,
    mcp_server      TEXT,
    target          TEXT,
    target_dir      TEXT,
    source          TEXT,
    entered_call    INTEGER NOT NULL,
    evicted_call    INTEGER,
    carried_calls   INTEGER NOT NULL DEFAULT 0,
    chars           INTEGER NOT NULL DEFAULT 0,
    est_tokens      INTEGER NOT NULL DEFAULT 0,
    tokens          INTEGER NOT NULL DEFAULT 0,
    write_usd       REAL,
    carry_usd       REAL,
    rebuild_usd     REAL,
    total_usd       REAL,
    peak_share      REAL,
    purpose         TEXT,
    instruction_ref TEXT,
    PRIMARY KEY(provider, session_id, item_key)
);

CREATE INDEX IF NOT EXISTS idx_derived_context_item_kind
    ON derived_context_item(kind, tool_category);

CREATE TABLE IF NOT EXISTS derived_context_origin (
    provider TEXT NOT NULL,
    session_id TEXT NOT NULL,
    item_key TEXT NOT NULL,
    origin_event TEXT NOT NULL,
    fingerprint TEXT NOT NULL, -- content when captured, otherwise structural origin
    fingerprint_kind TEXT NOT NULL,
    content_source TEXT NOT NULL,
    delivery_path TEXT NOT NULL,
    first_request INTEGER NOT NULL,
    last_request INTEGER,
    repeated_chars INTEGER NOT NULL,
    completeness TEXT NOT NULL,
    PRIMARY KEY(provider, session_id, item_key)
);
";

#[derive(Debug, Clone)]
struct Call {
    usage_id: i64,
    ts: Option<i64>,
    ts_text: Option<String>,
    model: Option<String>,
    fresh_input: i64,
    cache_read: i64,
    cache_write: i64,
    cache_write_1h: i64,
    output: i64,
    context_window: Option<i64>,
}

impl Call {
    fn context(&self) -> i64 {
        self.fresh_input + self.cache_read + self.cache_write
    }
}

#[derive(Debug, Clone)]
struct Item {
    key: String,
    ts: Option<i64>,
    ts_text: Option<String>,
    kind: String,
    tool_name: Option<String>,
    tool_category: Option<String>,
    target: Option<String>,
    source: Option<String>,
    content_fingerprint: Option<String>,
    chars: i64,
    est_tokens: f64,
    tokens: f64,
    entered_call: usize,
    evicted_call: Option<usize>,
    write_usd: f64,
    carry_usd: f64,
    rebuild_usd: f64,
    priced: bool,
    peak_share: f64,
}

impl Item {
    fn new(key: String, kind: &str, ts_text: Option<String>, chars: i64) -> Self {
        let prose = matches!(
            kind,
            "user_message"
                | "assistant_text"
                | "instructions"
                | "skill_injection"
                | "skill_body"
                | "skill_listing"
        );
        let ratio = if prose {
            CHARS_PER_TOKEN_PROSE
        } else {
            CHARS_PER_TOKEN_CODE
        };
        Self {
            key,
            ts: ts_text.as_deref().and_then(parse_ts),
            ts_text,
            kind: kind.to_string(),
            tool_name: None,
            tool_category: None,
            target: None,
            source: None,
            content_fingerprint: None,
            chars,
            est_tokens: chars.max(0) as f64 / ratio,
            tokens: 0.0,
            entered_call: 0,
            evicted_call: None,
            write_usd: 0.0,
            carry_usd: 0.0,
            rebuild_usd: 0.0,
            priced: false,
            peak_share: 0.0,
        }
    }
}

struct SessionInfo {
    repo_root: Option<String>,
}

pub fn refresh_context_ledger(conn: &Connection) -> Result<()> {
    conn.execute_batch("DELETE FROM derived_api_call; DELETE FROM derived_context_item; DELETE FROM derived_context_origin;")?;

    let sessions = load_sessions(conn)?;
    let calls = load_calls(conn)?;
    let mut items = load_items(conn)?;
    let edits = load_edit_paths(conn)?;
    let assistant_text = load_assistant_text(conn)?;
    let mut instruction_cache: HashMap<String, Vec<(usize, String)>> = HashMap::new();
    let table = pricing::table();

    let mut call_stmt = conn.prepare(
        "INSERT INTO derived_api_call (
            provider, session_id, call_index, usage_id, ts, model_name, context_tokens,
            new_tokens, carried_tokens, cache_event, input_cost_usd, rebuild_extra_usd,
            residual_tokens, context_window
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
    )?;
    let mut item_stmt = conn.prepare(
        "INSERT OR REPLACE INTO derived_context_item (
            provider, session_id, item_key, ts, kind, tool_name, tool_category, mcp_server,
            target, target_dir, source, entered_call, evicted_call, carried_calls, chars,
            est_tokens, tokens, write_usd, carry_usd, rebuild_usd, total_usd, peak_share,
            purpose, instruction_ref
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17,
                   ?18, ?19, ?20, ?21, ?22, ?23, ?24)",
    )?;
    let mut origin_stmt = conn.prepare(
        "INSERT OR REPLACE INTO derived_context_origin
         (provider, session_id, item_key, origin_event, fingerprint, fingerprint_kind,
          content_source, delivery_path, first_request, last_request, repeated_chars, completeness)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
    )?;

    for (key, session_calls) in calls {
        if session_calls.is_empty() {
            continue;
        }
        let mut session_items = items.remove(&key).unwrap_or_default();
        let info = sessions.get(&key);
        let ledger = build_ledger(&session_calls, &mut session_items, &table);

        for (index, call) in session_calls.iter().enumerate() {
            let row = &ledger.calls[index];
            call_stmt.execute(params![
                key.0,
                key.1,
                index as i64,
                call.usage_id,
                call.ts_text,
                call.model,
                call.context(),
                row.new_tokens,
                row.carried_tokens,
                row.cache_event,
                row.input_cost,
                row.rebuild_extra,
                row.residual_tokens,
                call.context_window
            ])?;
        }

        let repo_root = info.and_then(|info| info.repo_root.clone());
        let session_edits = edits.get(&key);
        let session_text = assistant_text.get(&key);
        let first_edit_ts =
            session_edits.and_then(|edits| edits.iter().filter_map(|(ts, _)| *ts).min());
        let instruction_refs = instruction_references(&session_items, &mut instruction_cache);

        for item in session_items.iter().chain(ledger.synthetic.iter()) {
            let carried = item
                .evicted_call
                .unwrap_or(session_calls.len())
                .saturating_sub(item.entered_call + 1) as i64;
            let purpose = read_purpose(item, first_edit_ts, session_edits, session_text);
            let dir = item
                .target
                .as_deref()
                .filter(|_| {
                    matches!(
                        item.tool_category.as_deref(),
                        Some("read" | "search" | "edit")
                    )
                })
                .map(|target| target_dir(target, repo_root.as_deref(), 3));
            let instruction_ref = item.target.as_deref().and_then(|target| {
                match_instruction(target, repo_root.as_deref(), &instruction_refs)
            });
            let total = item
                .priced
                .then_some(item.write_usd + item.carry_usd + item.rebuild_usd);
            item_stmt.execute(params![
                key.0,
                key.1,
                item.key,
                item.ts_text,
                item.kind,
                item.tool_name,
                item.tool_category,
                item.tool_name.as_deref().and_then(mcp_server),
                item.target,
                dir,
                item.source,
                item.entered_call as i64,
                item.evicted_call.map(|call| call as i64),
                carried,
                item.chars,
                item.est_tokens.round() as i64,
                item.tokens.round() as i64,
                item.priced.then_some(item.write_usd),
                item.priced.then_some(item.carry_usd),
                item.priced.then_some(item.rebuild_usd),
                total,
                item.peak_share,
                purpose,
                instruction_ref
            ])?;
            let (content_source, delivery_path, completeness) = origin_labels(item);
            let structural_fingerprint = format!(
                "{:x}",
                md5::compute(format!(
                    "{}\0{}\0{}\0{}\0{}",
                    key.0,
                    key.1,
                    item.key,
                    item.kind,
                    item.target.as_deref().unwrap_or("")
                ))
            );
            let fingerprint = item
                .content_fingerprint
                .as_deref()
                .unwrap_or(&structural_fingerprint);
            origin_stmt.execute(params![
                key.0,
                key.1,
                item.key,
                item.key,
                fingerprint,
                if item.content_fingerprint.is_some() {
                    "content_md5"
                } else {
                    "origin_md5"
                },
                content_source,
                delivery_path,
                item.entered_call as i64,
                (item.entered_call < session_calls.len()).then(|| item
                    .evicted_call
                    .unwrap_or(session_calls.len())
                    .saturating_sub(1)
                    as i64),
                if item.entered_call < session_calls.len() {
                    item.chars.saturating_mul(carried.saturating_add(1))
                } else {
                    0
                },
                completeness
            ])?;
        }
    }
    Ok(())
}

fn origin_labels(item: &Item) -> (&'static str, &'static str, &'static str) {
    let delivery = if item
        .tool_name
        .as_deref()
        .is_some_and(|name| name.starts_with("mcp__") || name.starts_with("mcp_"))
    {
        "mcp_tool"
    } else if item.tool_name.as_deref().is_some_and(|name| {
        matches!(
            name.to_ascii_lowercase().as_str(),
            "bash"
                | "exec_command"
                | "shell"
                | "run_terminal_cmd"
                | "run_terminal_command"
                | "local_shell"
        )
    }) {
        "shell"
    } else if item.tool_name.is_some() {
        "tool"
    } else {
        "direct"
    };
    let source = match item.kind.as_str() {
        "skill_listing" | "skill_body" | "skill_injection" => "skill",
        "mcp_instructions" => "mcp_server",
        "tool_definitions" => "tool_schema",
        "instructions" | "system_prompt" => "instructions",
        "user_message" | "assistant_text" | "assistant_output" => "conversation",
        "baseline" | "unexplained" => "unknown",
        "tool_result"
            if item.tool_name.as_deref().is_some_and(|name| {
                let name = name.to_ascii_lowercase();
                name == "skill"
                    || name.ends_with(".skill")
                    || name.ends_with("_skill")
                    || name.ends_with(":skill")
            }) =>
        {
            "skill"
        }
        "tool_result"
            if item.target.as_deref().is_some_and(|target| {
                let normalized = target.replace('\\', "/");
                normalized.contains("/skills/") && normalized.contains("/SKILL.md")
            }) =>
        {
            "skill"
        }
        "tool_result" if item.tool_category.as_deref() == Some("read") => "file",
        "tool_result"
            if delivery == "shell"
                && item.target.as_deref().is_some_and(|target| {
                    ["cat ", "sed ", "head ", "tail ", "rg "]
                        .iter()
                        .any(|prefix| target.trim_start().starts_with(prefix))
                }) =>
        {
            "file"
        }
        "tool_result" if delivery == "mcp_tool" => "mcp_server",
        "tool_result" | "subagent_result" => "tool_result",
        _ => "other",
    };
    let completeness = if source == "unknown" {
        "unknown"
    } else if item.kind == "assistant_output" {
        "estimated"
    } else {
        "observed_size"
    };
    (source, delivery, completeness)
}

#[derive(Debug, Default, Clone)]
struct CallRow {
    new_tokens: i64,
    carried_tokens: i64,
    cache_event: &'static str,
    input_cost: Option<f64>,
    rebuild_extra: Option<f64>,
    residual_tokens: i64,
}

struct Ledger {
    calls: Vec<CallRow>,
    synthetic: Vec<Item>,
}

fn build_ledger(calls: &[Call], items: &mut [Item], table: &pricing::PricingTable) -> Ledger {
    // Assign each item to the first call at or after its timestamp.
    for item in items.iter_mut() {
        item.entered_call = match item.ts {
            Some(ts) => calls
                .iter()
                .position(|call| call.ts.is_some_and(|call_ts| call_ts >= ts))
                .unwrap_or(calls.len()),
            None => 0,
        };
    }
    items.sort_by_key(|item| (item.entered_call, item.ts.unwrap_or(0)));

    // Calibration factor: observed growth vs estimated entering items.
    let mut ratios: Vec<f64> = Vec::new();
    for index in 1..calls.len() {
        let growth = calls[index].context() - calls[index - 1].context() - calls[index - 1].output;
        let estimated: f64 = items
            .iter()
            .filter(|item| item.entered_call == index)
            .map(|item| item.est_tokens)
            .sum();
        if growth > 0 && estimated > 50.0 {
            ratios.push(growth as f64 / estimated);
        }
    }
    ratios.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let factor = ratios
        .get(ratios.len() / 2)
        .copied()
        .unwrap_or(1.0)
        .clamp(CALIBRATION_MIN, CALIBRATION_MAX);

    let mut synthetic: Vec<Item> = Vec::new();
    let mut rows: Vec<CallRow> = Vec::with_capacity(calls.len());
    let mut live: Vec<(bool, usize)> = Vec::new(); // (is_synthetic, index)
    let mut previous_context = 0i64;
    let mut previous_output = 0i64;

    for (call_index, call) in calls.iter().enumerate() {
        let context = call.context();
        let mut cache_event = if call_index == 0 {
            "cold_start"
        } else {
            "normal"
        };

        // Compaction: context shrank. Evict everything except the baseline.
        if call_index > 0 && (context as f64) < previous_context as f64 * COMPACTION_SHRINK {
            cache_event = "compaction";
            live.retain(|(is_synthetic, index)| {
                let keep = *is_synthetic && synthetic[*index].kind == "baseline";
                if !keep {
                    if *is_synthetic {
                        synthetic[*index].evicted_call = Some(call_index);
                    } else {
                        items[*index].evicted_call = Some(call_index);
                    }
                }
                keep
            });
            previous_output = 0;
        }

        let carried_before: f64 = live
            .iter()
            .map(|(is_synthetic, index)| {
                if *is_synthetic {
                    synthetic[*index].tokens
                } else {
                    items[*index].tokens
                }
            })
            .sum();
        let new_tokens = (context as f64 - carried_before).max(0.0);

        // Entering items: previous output first, then calibrated items.
        let entering: Vec<usize> = (0..items.len())
            .filter(|index| items[*index].entered_call == call_index)
            .collect();
        let mut budget = new_tokens;
        if call_index == 0 {
            // Everything logged before the first call; the rest is baseline.
        } else if previous_output > 0 {
            let output_tokens = (previous_output as f64).min(budget);
            if output_tokens > 0.0 {
                let mut item = Item::new(
                    format!("output:{call_index}"),
                    "assistant_output",
                    calls[call_index - 1].ts_text.clone(),
                    0,
                );
                item.tokens = output_tokens;
                item.est_tokens = output_tokens;
                item.entered_call = call_index;
                synthetic.push(item);
                live.push((true, synthetic.len() - 1));
                budget -= output_tokens;
            }
        }
        let estimated: f64 = entering
            .iter()
            .map(|index| items[*index].est_tokens * factor)
            .sum();
        let scale = if estimated > budget && estimated > 0.0 {
            budget / estimated
        } else {
            1.0
        };
        for index in &entering {
            items[*index].tokens = items[*index].est_tokens * factor * scale;
            live.push((false, *index));
        }
        let residual = (budget - estimated * scale).max(0.0);
        if residual >= 1.0 {
            let kind = if call_index == 0 {
                "baseline"
            } else {
                "unexplained"
            };
            let mut item = Item::new(
                format!("{kind}:{call_index}"),
                kind,
                call.ts_text.clone(),
                0,
            );
            item.tokens = residual;
            item.entered_call = call_index;
            synthetic.push(item);
            live.push((true, synthetic.len() - 1));
        }

        // Cost split for this call.
        let entry = call.model.as_deref().and_then(|model| table.lookup(model));
        let mut row = CallRow {
            new_tokens: new_tokens.round() as i64,
            carried_tokens: carried_before.round() as i64,
            cache_event,
            residual_tokens: if call_index == 0 {
                0
            } else {
                residual.round() as i64
            },
            ..CallRow::default()
        };
        let carried_reading_cache = call.cache_read as f64 >= carried_before * REBUILD_READ_SHARE;
        if call_index > 0
            && carried_before > 0.0
            && !carried_reading_cache
            && cache_event == "normal"
        {
            row.cache_event = "rebuild";
        }
        if let Some((entry, _)) = entry {
            let rates = entry.rates(call.context());
            let write_5m = (call.cache_write - call.cache_write_1h).max(0) as f64;
            let read_cost = call.cache_read as f64 * rates.cache_read / 1e6;
            let write_cost = (call.fresh_input as f64 * rates.input
                + write_5m * rates.cache_write_5m
                + call.cache_write_1h as f64 * rates.cache_write_1h)
                / 1e6;
            let input_cost = read_cost + write_cost;
            row.input_cost = Some(input_cost);
            let total_tokens = context.max(1) as f64;
            let (carried_cost, new_cost, rebuild) = if row.cache_event == "rebuild" {
                let carried_share = write_cost * (carried_before / total_tokens).min(1.0);
                let would_have = carried_before * rates.cache_read / 1e6;
                (
                    carried_share + read_cost,
                    write_cost - carried_share,
                    (carried_share + read_cost - would_have).max(0.0),
                )
            } else {
                (read_cost, write_cost, 0.0)
            };
            if rebuild > 0.0 {
                row.rebuild_extra = Some(rebuild);
            }
            let new_total: f64 = live
                .iter()
                .filter(|(s, i)| entered_at(*s, *i, items, &synthetic) == call_index)
                .map(|(s, i)| tokens_of(*s, *i, items, &synthetic))
                .sum();
            for (is_synthetic, index) in live.clone() {
                let tokens = tokens_of(is_synthetic, index, items, &synthetic);
                let entered = entered_at(is_synthetic, index, items, &synthetic);
                let item = if is_synthetic {
                    &mut synthetic[index]
                } else {
                    &mut items[index]
                };
                item.priced = true;
                if entered == call_index {
                    if new_total > 0.0 {
                        item.write_usd += new_cost * tokens / new_total;
                    }
                } else if carried_before > 0.0 {
                    let share = tokens / carried_before;
                    item.carry_usd += (carried_cost - rebuild) * share;
                    item.rebuild_usd += rebuild * share;
                    // Fresh tokens with no entering item (rounding, tiny
                    // prompts) are carried by what is already in context.
                    if new_total <= 0.0 {
                        item.write_usd += new_cost * share;
                    }
                }
            }
        }
        for (is_synthetic, index) in &live {
            let tokens = tokens_of(*is_synthetic, *index, items, &synthetic);
            let item = if *is_synthetic {
                &mut synthetic[*index]
            } else {
                &mut items[*index]
            };
            let share = tokens / context.max(1) as f64;
            if share > item.peak_share {
                item.peak_share = share;
            }
        }
        rows.push(row);
        previous_context = context;
        previous_output = call.output;
    }

    // Items logged after the last call never entered a billed context.
    for item in items.iter_mut() {
        if item.entered_call >= calls.len() {
            item.evicted_call = Some(item.entered_call);
        }
    }

    Ledger {
        calls: rows,
        synthetic,
    }
}

fn tokens_of(is_synthetic: bool, index: usize, items: &[Item], synthetic: &[Item]) -> f64 {
    if is_synthetic {
        synthetic[index].tokens
    } else {
        items[index].tokens
    }
}

fn entered_at(is_synthetic: bool, index: usize, items: &[Item], synthetic: &[Item]) -> usize {
    if is_synthetic {
        synthetic[index].entered_call
    } else {
        items[index].entered_call
    }
}

/// `startup` reads happen before the first edit; `used` reads target a path
/// that is later edited or named in assistant text; otherwise `unused`.
fn read_purpose(
    item: &Item,
    first_edit_ts: Option<i64>,
    edits: Option<&Vec<(Option<i64>, String)>>,
    assistant_text: Option<&Vec<(Option<i64>, String)>>,
) -> Option<&'static str> {
    if !matches!(
        item.tool_category.as_deref(),
        Some("read" | "search" | "mcp" | "exec")
    ) {
        return None;
    }
    let target = item.target.as_deref()?;
    let path = target.split(" :: ").next().unwrap_or(target).trim();
    let basename = path.rsplit(['/', '\\']).next().unwrap_or(path);
    let later = |ts: &Option<i64>| match (item.ts, ts) {
        (Some(item_ts), Some(ts)) => *ts >= item_ts,
        _ => true,
    };
    let edited = edits.is_some_and(|edits| {
        edits.iter().any(|(ts, edited)| {
            later(ts)
                && !path.is_empty()
                && (edited == path || edited.ends_with(path) || path.ends_with(edited.as_str()))
        })
    });
    let mentioned = basename.len() >= 4
        && assistant_text.is_some_and(|texts| {
            texts
                .iter()
                .any(|(ts, text)| later(ts) && text.contains(basename))
        });
    let startup = match (item.ts, first_edit_ts) {
        (Some(ts), Some(first)) => ts < first,
        (Some(_), None) => false,
        _ => false,
    };
    Some(if edited || mentioned {
        if startup { "startup_used" } else { "used" }
    } else if startup {
        "startup_unused"
    } else {
        "unused"
    })
}

/// Paths and directories referenced by loaded instruction files, with line
/// numbers, e.g. `("docs/", "AGENTS.md:12")`.
fn instruction_references(
    items: &[Item],
    cache: &mut HashMap<String, Vec<(usize, String)>>,
) -> Vec<(String, String)> {
    let mut refs = Vec::new();
    for item in items.iter().filter(|item| item.kind == "instructions") {
        let Some(sources) = item.source.as_deref() else {
            continue;
        };
        for path in sources
            .lines()
            .filter(|line| std::path::Path::new(line).is_absolute() || line.starts_with('/'))
        {
            let lines = cache
                .entry(path.to_string())
                .or_insert_with(|| extract_path_references(path));
            let file_name = path.rsplit(['/', '\\']).next().unwrap_or(path);
            for (line, reference) in lines.iter() {
                refs.push((reference.clone(), format!("{file_name}:{line}")));
            }
        }
    }
    refs
}

fn extract_path_references(path: &str) -> Vec<(usize, String)> {
    let Ok(text) = std::fs::read_to_string(crate::path_utils::on_disk_str(path)) else {
        return Vec::new();
    };
    let mut refs = Vec::new();
    for (index, line) in text.lines().enumerate() {
        for token in
            line.split(|c: char| c.is_whitespace() || c == '`' || c == '(' || c == ')' || c == ',')
        {
            let token = token.trim_matches(|c: char| c == '.' || c == ':' || c == '"' || c == '\'');
            let looks_like_path = token.contains('/')
                && !token.starts_with("http")
                && !token.contains("://")
                && token.len() > 2
                && token
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || "/._-*".contains(c));
            let looks_like_doc = token.ends_with(".md") && token.len() > 3;
            if looks_like_path || looks_like_doc {
                refs.push((
                    index + 1,
                    token
                        .trim_start_matches("./")
                        .trim_end_matches('*')
                        .to_string(),
                ));
            }
        }
    }
    refs
}

fn match_instruction(
    target: &str,
    repo_root: Option<&str>,
    refs: &[(String, String)],
) -> Option<String> {
    if refs.is_empty() {
        return None;
    }
    let mut relative = target.trim().to_string();
    if let Some(root) = repo_root
        && let Some(stripped) = relative.strip_prefix(root.trim_end_matches('/'))
    {
        relative = stripped.trim_start_matches('/').to_string();
    }
    refs.iter()
        .filter(|(reference, _)| !reference.is_empty())
        .find(|(reference, _)| {
            relative.starts_with(reference.as_str())
                || relative.contains(&format!(" {reference}"))
                || relative.ends_with(reference.as_str())
        })
        .map(|(_, location)| location.clone())
}

type Key = (String, String);
type TimedTexts = HashMap<Key, Vec<(Option<i64>, String)>>;

fn load_sessions(conn: &Connection) -> Result<HashMap<Key, SessionInfo>> {
    let mut stmt = conn.prepare(
        "SELECT s.provider, s.session_id, r.repo_root
         FROM metadata_sessions s LEFT JOIN metadata_repositories r ON r.id = s.repository_id",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            (row.get::<_, String>(0)?, row.get::<_, String>(1)?),
            SessionInfo {
                repo_root: row.get(2)?,
            },
        ))
    })?;
    Ok(rows.collect::<rusqlite::Result<HashMap<_, _>>>()?)
}

fn load_calls(conn: &Connection) -> Result<BTreeMap<Key, Vec<Call>>> {
    let mut stmt = conn.prepare(
        "SELECT id, provider, session_id, usage_ts, model_name, input_tokens, cached_input_tokens,
                cache_creation_tokens, cache_creation_1h_tokens, output_tokens, context_window
         FROM fact_session_usage
         WHERE token_source = 'reported' AND usage_ts IS NOT NULL
         ORDER BY provider, session_id, usage_ts, usage_index",
    )?;
    let rows = stmt.query_map([], |row| {
        let ts_text: Option<String> = row.get(3)?;
        Ok((
            (row.get::<_, String>(1)?, row.get::<_, String>(2)?),
            Call {
                usage_id: row.get(0)?,
                ts: ts_text.as_deref().and_then(parse_ts),
                ts_text,
                model: row.get(4)?,
                fresh_input: row.get(5)?,
                cache_read: row.get(6)?,
                cache_write: row.get(7)?,
                cache_write_1h: row.get(8)?,
                output: row.get(9)?,
                context_window: row.get(10)?,
            },
        ))
    })?;
    let mut out: BTreeMap<Key, Vec<Call>> = BTreeMap::new();
    for row in rows {
        let (key, call) = row?;
        out.entry(key).or_default().push(call);
    }
    Ok(out)
}

fn load_items(conn: &Connection) -> Result<HashMap<Key, Vec<Item>>> {
    let mut out: HashMap<Key, Vec<Item>> = HashMap::new();

    let mut stmt = conn.prepare(
        "SELECT provider, session_id, item_key, ts, kind, source, chars
         FROM fact_session_context_injection",
    )?;
    let rows = stmt.query_map([], |row| {
        let kind: String = row.get(4)?;
        let mut item = Item::new(
            format!("inj:{}", row.get::<_, String>(2)?),
            &kind,
            row.get(3)?,
            row.get(6)?,
        );
        item.source = row.get(5)?;
        Ok(((row.get::<_, String>(0)?, row.get::<_, String>(1)?), item))
    })?;
    for row in rows {
        let (key, item) = row?;
        out.entry(key).or_default().push(item);
    }

    let mut stmt = conn.prepare(
        "SELECT provider, session_id, message_index, message_ts, COALESCE(chars, LENGTH(content)), content
         FROM fact_session_message WHERE role = 'user'",
    )?;
    let rows = stmt.query_map([], |row| {
        let provider: String = row.get(0)?;
        let content: String = row.get(5)?;
        let kind = if provider == "opencode"
            && (content.trim_start().starts_with("# Skill: ")
                || content.trim_start().starts_with("<skill name=\""))
        {
            "skill_injection"
        } else {
            "user_message"
        };
        let mut item = Item::new(
            format!("user:{}", row.get::<_, i64>(2)?),
            kind,
            row.get(3)?,
            row.get(4)?,
        );
        item.content_fingerprint = Some(format!("{:x}", md5::compute(content.as_bytes())));
        Ok(((provider, row.get::<_, String>(1)?), item))
    })?;
    for row in rows {
        let (key, item) = row?;
        out.entry(key).or_default().push(item);
    }

    let mut stmt = conn.prepare(
        "SELECT provider, session_id, call_key, COALESCE(ended_ts, ts), tool_name, tool_category,
                target, output_chars, output_in_context, child_session_id
         FROM fact_session_tool_call",
    )?;
    let rows = stmt.query_map([], |row| {
        let in_context: i64 = row.get(8)?;
        let child: Option<String> = row.get(9)?;
        let kind = if child.is_some() {
            "subagent_result"
        } else {
            "tool_result"
        };
        let mut item = Item::new(
            format!("tool:{}", row.get::<_, String>(2)?),
            kind,
            row.get(3)?,
            if in_context != 0 { row.get(7)? } else { 0 },
        );
        item.tool_name = row.get(4)?;
        item.tool_category = row.get(5)?;
        item.target = row.get(6)?;
        item.source = child;
        Ok(((row.get::<_, String>(0)?, row.get::<_, String>(1)?), item))
    })?;
    for row in rows {
        let (key, item) = row?;
        if item.chars > 0 {
            out.entry(key).or_default().push(item);
        }
    }
    Ok(out)
}

fn load_edit_paths(conn: &Connection) -> Result<TimedTexts> {
    let mut stmt = conn.prepare(
        "SELECT provider, session_id, ts, target FROM fact_session_tool_call
         WHERE tool_category = 'edit' AND target IS NOT NULL",
    )?;
    let rows = stmt.query_map([], |row| {
        let ts: Option<String> = row.get(2)?;
        Ok((
            (row.get::<_, String>(0)?, row.get::<_, String>(1)?),
            (ts.as_deref().and_then(parse_ts), row.get::<_, String>(3)?),
        ))
    })?;
    let mut out: TimedTexts = HashMap::new();
    for row in rows {
        let (key, value) = row?;
        out.entry(key).or_default().push(value);
    }
    Ok(out)
}

fn load_assistant_text(conn: &Connection) -> Result<TimedTexts> {
    let mut stmt = conn.prepare(
        "SELECT provider, session_id, message_ts, content FROM fact_session_message
         WHERE role = 'assistant'",
    )?;
    let rows = stmt.query_map([], |row| {
        let ts: Option<String> = row.get(2)?;
        Ok((
            (row.get::<_, String>(0)?, row.get::<_, String>(1)?),
            (ts.as_deref().and_then(parse_ts), row.get::<_, String>(3)?),
        ))
    })?;
    let mut out: TimedTexts = HashMap::new();
    for row in rows {
        let (key, value) = row?;
        out.entry(key).or_default().push(value);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_app_schema;
    use crate::db::session_store::{
        ContextInjectionRecord, MessageRecord, SessionRecord, ToolCallRecord, UsageRecord,
        store_session_record,
    };

    fn call(key: &str, ts: &str, fresh: i64, read: i64, write: i64, output: i64) -> UsageRecord {
        let mut usage = UsageRecord::new(key);
        usage.ts = Some(ts.into());
        usage.model = Some("claude-opus-5-5".into());
        usage.input_tokens = fresh;
        usage.cache_read_tokens = read;
        usage.cache_write_tokens = write;
        usage.output_tokens = output;
        usage
    }

    fn seed(conn: &Connection) -> Result<()> {
        let mut record = SessionRecord::new("claude", "s1");
        record.started_at = Some("2026-10-01T10:00:00Z".into());
        record.context_injections.push(ContextInjectionRecord {
            key: "i1".into(),
            ts: Some("2026-10-01T10:00:00Z".into()),
            kind: "instructions".into(),
            source: None,
            chars: 4_200,
        });
        record.messages.push(MessageRecord {
            key: "u1".into(),
            ts: Some("2026-10-01T10:00:01Z".into()),
            role: "user".into(),
            text: "read the docs".into(),
            is_compaction_summary: false,
        });
        record.tool_calls.push(ToolCallRecord {
            key: "t1".into(),
            ts: Some("2026-10-01T10:00:05Z".into()),
            ended_ts: Some("2026-10-01T10:00:06Z".into()),
            tool_name: "Read".into(),
            target: Some("docs/big.md".into()),
            output_chars: 36_000,
            output_in_context: true,
            status: "ok".into(),
            ..ToolCallRecord::default()
        });
        record.usage = vec![
            call("m1", "2026-10-01T10:00:02Z", 2, 0, 20_000, 100),
            call("m2", "2026-10-01T10:00:07Z", 2, 20_000, 10_100, 100),
            call("m3", "2026-10-01T10:00:09Z", 2, 30_100, 100, 100),
            // Idle for 20 minutes: cache expired and is rebuilt.
            call("m4", "2026-10-01T10:20:09Z", 2, 0, 30_300, 100),
        ];
        store_session_record(conn, &record)?;
        Ok(())
    }

    #[test]
    fn context_allocation_sums_to_call_input_cost() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        init_app_schema(&conn)?;
        seed(&conn)?;
        refresh_context_ledger(&conn)?;
        let calls: f64 = conn.query_row(
            "SELECT SUM(input_cost_usd) FROM derived_api_call",
            [],
            |r| r.get(0),
        )?;
        let items: f64 =
            conn.query_row("SELECT SUM(total_usd) FROM derived_context_item", [], |r| {
                r.get(0)
            })?;
        assert!((calls - items).abs() < 1e-9, "calls {calls} items {items}");
        Ok(())
    }

    #[test]
    fn large_read_is_carried_and_rebuild_is_detected() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        init_app_schema(&conn)?;
        seed(&conn)?;
        refresh_context_ledger(&conn)?;
        let (tokens, carried, rebuild): (i64, i64, f64) = conn.query_row(
            "SELECT tokens, carried_calls, rebuild_usd FROM derived_context_item WHERE item_key = 'tool:t1'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        assert!(
            tokens > 8_000 && tokens <= 10_100,
            "calibrated to observed growth: {tokens}"
        );
        assert_eq!(carried, 2);
        assert!(rebuild > 0.0);
        let event: String = conn.query_row(
            "SELECT cache_event FROM derived_api_call WHERE call_index = 3",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(event, "rebuild");
        let baseline: i64 = conn.query_row(
            "SELECT tokens FROM derived_context_item WHERE kind = 'baseline'",
            [],
            |r| r.get(0),
        )?;
        assert!(
            baseline > 18_000,
            "unlogged system prompt shows as baseline: {baseline}"
        );
        Ok(())
    }

    #[test]
    fn provenance_counts_repeated_file_content_once_per_request() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        init_app_schema(&conn)?;
        seed(&conn)?;
        refresh_context_ledger(&conn)?;
        let (source, path, first, last, repeated, fingerprint): (String, String, i64, i64, i64, String) = conn.query_row(
            "SELECT content_source, delivery_path, first_request, last_request, repeated_chars, fingerprint
             FROM derived_context_origin WHERE item_key = 'tool:t1'", [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
        )?;
        assert_eq!(source, "file");
        assert_eq!(path, "tool");
        assert_eq!(first, 1);
        assert_eq!(last, 3);
        assert_eq!(repeated, 36_000 * 3);
        assert_eq!(fingerprint.len(), 32);
        refresh_context_ledger(&conn)?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM derived_context_origin WHERE item_key = 'tool:t1'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(count, 1);
        Ok(())
    }

    #[test]
    fn skill_tool_output_is_attributed_to_skill_content() {
        let mut item = Item::new("tool:skill-1".into(), "tool_result", None, 6_492);
        item.tool_name = Some("skill".into());
        item.target = Some("ai-crm-context".into());
        assert_eq!(origin_labels(&item), ("skill", "tool", "observed_size"));
    }

    #[test]
    fn extracts_instruction_path_references() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("AGENTS.md");
        std::fs::write(&path, "# Rules\nBefore changes read everything under `docs/`.\nSee crates/vca/src/db/mod.rs\n").unwrap();
        let refs = extract_path_references(path.to_str().unwrap());
        assert!(refs.contains(&(2, "docs/".to_string())));
        assert!(refs.contains(&(3, "crates/vca/src/db/mod.rs".to_string())));
        let located = match_instruction(
            "/repo/docs/guide.md",
            Some("/repo"),
            &[("docs/".to_string(), "AGENTS.md:2".to_string())],
        );
        assert_eq!(located.as_deref(), Some("AGENTS.md:2"));
    }
}
