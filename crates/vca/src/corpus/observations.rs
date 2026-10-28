//! Allowlisted persistent analytics inputs; never exports credentials or automatic opt-in.
use anyhow::{Result, bail};
use rusqlite::{
    Connection,
    types::{Value, ValueRef},
};
use serde::{Deserialize, Serialize};
use std::path::Path;

pub const FILE: &str = "analytics-inputs.json";
const TABLES: &[&str] = &[
    "work_identity",
    "work_relation",
    "work_saved_decision",
    "change_milestone",
    "setup_observation",
    "setup_scan",
    "analytics_setting",
    "task_turn_assignment",
    "task_completion_mark",
    "classification_override",
    "classification_result",
    "source_coverage",
    "source_coverage_history",
    "repository_alias",
    "usage_measurement",
    "pull_request_record",
    "pull_request_revision",
    "pull_request_intent",
    "work_pr_link",
    "deployment_record",
    "deployment_inclusion",
];
#[derive(Serialize, Deserialize)]
struct Table {
    name: String,
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
}
#[derive(Serialize, Deserialize)]
struct Snapshot {
    schema_version: u32,
    tables: Vec<Table>,
}

pub fn save(
    conn: &Connection,
    path: &Path,
    provider: &str,
    session: &str,
    repo: Option<&str>,
) -> Result<()> {
    let identities: Vec<String> = conn
        .prepare("SELECT id FROM work_identity WHERE provider=?1 AND session_id=?2")?
        .query_map([provider, session], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    let pr_ids:Vec<String>=conn.prepare("SELECT DISTINCT pr_id FROM work_pr_link WHERE member_id IN (SELECT id FROM work_identity WHERE provider=?1 AND session_id=?2)")?.query_map([provider,session],|r|r.get(0))?.collect::<rusqlite::Result<_>>()?;
    let canonical = repo
        .map(|r| crate::insights::observation::canonical_repo(conn, r))
        .transpose()?;
    let mut pr_ids = pr_ids;
    if let Some(repo) = &canonical {
        pr_ids.extend(
            conn.prepare("SELECT id FROM pull_request_record WHERE repo_key=?1")?
                .query_map([repo], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?,
        );
    }
    let mut tables = Vec::new();
    for name in TABLES {
        let mut statement = conn.prepare(&format!("SELECT * FROM {name}"))?;
        let columns = statement
            .column_names()
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let mut rows = statement.query([])?;
        let mut kept = Vec::new();
        while let Some(row) = rows.next()? {
            let values = (0..columns.len())
                .map(|i| {
                    Ok(match row.get_ref(i)? {
                        ValueRef::Null => serde_json::Value::Null,
                        ValueRef::Integer(n) => n.into(),
                        ValueRef::Real(n) => n.into(),
                        ValueRef::Text(s) => String::from_utf8_lossy(s).to_string().into(),
                        ValueRef::Blob(_) => bail!("Blob in analytics inputs"),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let field = |key: &str| {
                columns
                    .iter()
                    .position(|c| c == key)
                    .and_then(|i| values[i].as_str())
            };
            let include = match *name {
                "source_coverage" => {
                    field("provider") == Some(provider)
                        || field("repo_root") == canonical.as_deref() && canonical.is_some()
                }
                "source_coverage_history" => field("record_json")
                    .and_then(|s| {
                        serde_json::from_str::<crate::insights::observation::SourceCoverage>(s).ok()
                    })
                    .is_some_and(|c| {
                        c.provider == provider || (canonical.is_some() && c.repo_root == canonical)
                    }),
                "repository_alias" => {
                    field("alias") == repo
                        || field("canonical") == canonical.as_deref() && canonical.is_some()
                }
                "work_pr_link" => {
                    field("member_id").is_some_and(|id| identities.iter().any(|i| i == id))
                }
                "pull_request_record" => {
                    field("id").is_some_and(|id| pr_ids.iter().any(|p| p == id))
                        || field("repo_key") == canonical.as_deref() && canonical.is_some()
                }
                "pull_request_revision" | "pull_request_intent" | "deployment_inclusion" => {
                    field("pr_id").is_some_and(|id| pr_ids.iter().any(|p| p == id))
                }
                "deployment_record" => {
                    field("repo_key") == canonical.as_deref() && canonical.is_some()
                }
                "work_relation" => ["from_id", "to_id"]
                    .iter()
                    .any(|k| field(k).is_some_and(|v| identities.iter().any(|id| id == v))),
                "work_saved_decision" => {
                    field("identity_id").is_some_and(|v| identities.iter().any(|id| id == v))
                }
                "analytics_setting" => {
                    field("key") == Some("timezone")
                        || field("key") == Some(format!("observed_provider:{provider}").as_str())
                }
                "change_milestone" | "setup_scan" | "setup_observation" => {
                    field("repo_root").is_none() || field("repo_root") == repo
                }
                "classification_result" | "classification_override" => field("unit_key")
                    .is_some_and(|k| {
                        k == format!("{provider}|{session}")
                            || k.starts_with(&format!("{provider}|{session}|"))
                    }),
                _ => field("provider") == Some(provider) && field("session_id") == Some(session),
            };
            if include {
                kept.push(values);
            }
        }
        tables.push(Table {
            name: name.to_string(),
            columns,
            rows: kept,
        });
    }
    std::fs::write(
        path,
        serde_json::to_vec_pretty(&Snapshot {
            schema_version: 1,
            tables,
        })?,
    )?;
    Ok(())
}

pub fn restore(
    conn: &Connection,
    path: &Path,
    repos: &serde_json::Map<String, serde_json::Value>,
) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let snapshot: Snapshot = serde_json::from_slice(&std::fs::read(path)?)?;
    if snapshot.schema_version != 1 {
        bail!("Unsupported analytics input version");
    }
    for table in snapshot.tables {
        if !TABLES.contains(&table.name.as_str()) {
            bail!("Unexpected analytics input table");
        }
        let known: Vec<String> = conn
            .prepare(&format!("PRAGMA table_info({})", table.name))?
            .query_map([], |r| r.get(1))?
            .collect::<rusqlite::Result<_>>()?;
        if table.columns != known {
            bail!("Analytics input columns differ for {}", table.name);
        }
        let placeholders = (0..known.len()).map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!(
            "INSERT OR REPLACE INTO {} ({}) VALUES({placeholders})",
            table.name,
            known.join(",")
        );
        for row in table.rows {
            let values = row
                .into_iter()
                .map(|v| match v {
                    serde_json::Value::Null => Ok(Value::Null),
                    serde_json::Value::String(s) => Ok(Value::Text(
                        repos.get(&s).and_then(|v| v.as_str()).unwrap_or(&s).into(),
                    )),
                    serde_json::Value::Number(n) => Ok(n
                        .as_i64()
                        .map(Value::Integer)
                        .unwrap_or_else(|| Value::Real(n.as_f64().unwrap()))),
                    _ => bail!("Invalid analytics input value"),
                })
                .collect::<Result<Vec<_>>>()?;
            conn.execute(&sql, rusqlite::params_from_iter(values))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            self,
            session_store::{self, MessageRecord, SessionRecord},
        },
        insights::{
            self,
            delivery::{self, Deployment, PullRequest},
            observation, work,
        },
    };
    #[test]
    fn offline_inputs_preserve_linked_and_unmatched_prs_deployments_and_provenance() -> Result<()> {
        fn seed() -> Result<Connection> {
            let c = Connection::open_in_memory()?;
            db::init_app_schema(&c)?;
            let mut s = SessionRecord::new("codex", "corpus");
            s.started_at = Some("2026-10-01T10:00:00Z".into());
            s.messages.push(MessageRecord {
                key: "request".into(),
                ts: s.started_at.clone(),
                role: "user".into(),
                text: "Implement a change".into(),
                is_compaction_summary: false,
            });
            session_store::store_session_record(&c, &s)?;
            session_store::refresh_session_roots(&c)?;
            insights::refresh(&c)?;
            Ok(c)
        }
        let source = seed()?;
        let repo = "git:github.com/org/project";
        observation::set_alias(&source, "/repo", repo, "manual")?;
        let member = work::resolve_member(&source, "codex:corpus#0")?;
        for number in [1, 2] {
            let p = PullRequest {
                provider: "github".into(),
                repo_key: repo.into(),
                number,
                merge_sha: Some("artifact".into()),
                ..Default::default()
            };
            let id = delivery::save_pr(&source, &p)?;
            delivery::set_intent(&source, &id, "bug_fix", "mapping")?;
            if number == 1 {
                delivery::link(
                    &source,
                    &member,
                    &id,
                    "implementation",
                    "accepted",
                    "manual",
                    "explicit artifact",
                )?;
            }
        }
        delivery::save_deployment(
            &source,
            &Deployment {
                id: "release".into(),
                provider: "custom".into(),
                repo_key: repo.into(),
                artifact_sha: "artifact".into(),
                environment: "production".into(),
                status: "success".into(),
                live: true,
                occurred_at: "2026-10-02T12:00:00Z".into(),
                evidence_url: "https://deploy.example/1".into(),
                ..Default::default()
            },
        )?;
        let folder = tempfile::tempdir()?;
        let path = folder.path().join(FILE);
        save(&source, &path, "codex", "corpus", Some("/repo"))?;
        let serialized = std::fs::read_to_string(&path)?;
        assert!(!serialized.contains("delivery_source"));
        assert!(!serialized.contains("classification_job"));
        let target = seed()?;
        restore(&target, &path, &Default::default())?;
        insights::refresh(&target)?;
        let replayed = delivery::evidence(&target)?;
        assert_eq!(replayed.len(), 2);
        assert!(replayed.iter().all(|p| p.intent == "bug_fix"
            && p.intent_source == "mapping"
            && p.deployed_at.is_some()));
        assert_eq!(replayed.iter().filter(|p| p.work_ids.is_empty()).count(), 1);
        assert_eq!(
            replayed
                .iter()
                .find(|p| p.pr.number == 1)
                .unwrap()
                .contributions[0]
                .source,
            "manual"
        );
        Ok(())
    }
}
