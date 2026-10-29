//! Side-by-side comparison of tasks along fixed practice dimensions.

use anyhow::Result;
use rusqlite::Connection;
use serde::Serialize;

use super::delivery::session_delivery;
use super::format as fmt;
use super::practices::names_verification;
use super::{Filters, TaskSort, short_model, tasks};

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CompareColumn {
    pub label: String,
    pub values: Vec<(String, String)>,
    pub first_request: String,
}

pub const DIMENSIONS: [&str; 13] = [
    "Tool · model",
    "Intent / outcome",
    "Verification in first ask",
    "API-equiv · fresh tokens",
    "Cache-read ratio",
    "Turns · attempts",
    "Validation runs (failed)",
    "Loops",
    "User corrections",
    "Subagents (share of cost)",
    "Model changes",
    "Active time · to done",
    "Delivery",
];

pub fn compare_tasks(
    conn: &Connection,
    keys: &[(String, String, i64)],
) -> Result<Vec<CompareColumn>> {
    let filters = Filters {
        task_keys: keys.to_vec(),
        ..Filters::default()
    };
    let rows = tasks(conn, &filters, TaskSort::Started, keys.len().max(1))?;
    let delivery = session_delivery(conn)?;
    let mut columns = Vec::new();
    for key in keys {
        let Some(task) = rows
            .iter()
            .find(|t| t.provider == key.0 && t.session_id == key.1 && t.task_index == key.2)
        else {
            continue;
        };
        let (cache_read, fresh): (i64, i64) = conn
            .query_row(
                "SELECT COALESCE(SUM(cache_read_tokens), 0), COALESCE(SUM(fresh_tokens), 0)
                 FROM derived_usage_event
                 WHERE provider = ?1 AND root_session_id = ?2 AND task_index = ?3",
                rusqlite::params![task.provider, task.session_id, task.task_index],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap_or((0, 0));
        let corrections: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM view_label_effective l
                 JOIN derived_task t ON t.provider = ?1 AND t.session_id = ?2 AND t.task_index = ?3
                 WHERE l.unit_kind = 'exchange' AND l.question_id = 'feedback'
                   AND l.label LIKE 'correct_%'
                   AND l.unit_key LIKE ?1 || '|' || ?2 || '|%'
                   AND (',' || t.turn_list || ',') LIKE
                       ('%,' || SUBSTR(l.unit_key, LENGTH(?1) + LENGTH(?2) + 3) || ',%')",
                rusqlite::params![task.provider, task.session_id, task.task_index],
                |row| row.get(0),
            )
            .unwrap_or(0);
        let evidence = delivery
            .get(&(task.provider.clone(), task.session_id.clone()))
            .cloned()
            .unwrap_or_default();
        let delivery_text = if evidence.commits == 0 {
            "no commits".to_string()
        } else {
            let mut parts = vec![format!("{} commit(s)", evidence.commits)];
            if evidence.mainline {
                parts.push("mainline".into());
            }
            if evidence.pr_merged {
                parts.push("PR merged".into());
            }
            if evidence.later_fix {
                parts.push("later fix signal".into());
            }
            if evidence.reverted {
                parts.push("reverted".into());
            }
            parts.join(" → ")
        };
        let subagent_share = match (task.subagent_cost, task.cost_total) {
            (Some(sub), Some(total)) if total > 0.0 => {
                format!("{} ({:.0}%)", task.subagents, sub / total * 100.0)
            }
            _ => task.subagents.to_string(),
        };
        let values = vec![
            (
                DIMENSIONS[0].to_string(),
                format!(
                    "{} · {}",
                    task.provider,
                    task.primary_model
                        .as_deref()
                        .map(short_model)
                        .unwrap_or_default()
                ),
            ),
            (
                DIMENSIONS[1].to_string(),
                format!(
                    "{} / {}",
                    fmt::label(
                        task.intent.as_deref(),
                        task.intent_source.as_deref(),
                        task.intent_band.as_deref()
                    ),
                    fmt::label(
                        task.outcome.as_deref(),
                        task.outcome_source.as_deref(),
                        task.outcome_band.as_deref()
                    )
                ),
            ),
            (
                DIMENSIONS[2].to_string(),
                if names_verification(&task.title) {
                    "yes"
                } else {
                    "no"
                }
                .to_string(),
            ),
            (
                DIMENSIONS[3].to_string(),
                format!(
                    "{} · {}",
                    fmt::usd(task.cost_total),
                    fmt::tokens(task.fresh_tokens)
                ),
            ),
            (
                DIMENSIONS[4].to_string(),
                if fresh > 0 {
                    format!("{:.0}×", cache_read as f64 / fresh as f64)
                } else {
                    "—".into()
                },
            ),
            (
                DIMENSIONS[5].to_string(),
                format!("{} · {}", task.turns, task.attempts),
            ),
            (
                DIMENSIONS[6].to_string(),
                format!("{} ({}✗)", task.validations, task.validation_failures),
            ),
            (DIMENSIONS[7].to_string(), task.loop_attempts.to_string()),
            (DIMENSIONS[8].to_string(), corrections.to_string()),
            (DIMENSIONS[9].to_string(), subagent_share),
            (DIMENSIONS[10].to_string(), task.model_changes.to_string()),
            (
                DIMENSIONS[11].to_string(),
                format!(
                    "{} · {}",
                    fmt::minutes(task.active_minutes_total),
                    fmt::minutes(task.active_minutes_to_complete)
                ),
            ),
            (DIMENSIONS[12].to_string(), delivery_text),
        ];
        columns.push(CompareColumn {
            label: format!(
                "{}:{}#{}",
                task.provider,
                super::short_id(&task.session_id),
                task.task_index
            ),
            values,
            first_request: task.title.clone(),
        });
    }
    Ok(columns)
}
