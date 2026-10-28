use super::*;
use serde_json::json;

fn source(path: &Path, mixed: bool) -> Result<Connection> {
    let c = Connection::open(path)?;
    c.execute_batch("CREATE TABLE project(id TEXT PRIMARY KEY,worktree TEXT);
        CREATE TABLE session_v2(id TEXT PRIMARY KEY,project_id TEXT,directory TEXT,parent_id TEXT,fork_session_id TEXT,title TEXT,version TEXT,time_created INTEGER,time_updated INTEGER);
        CREATE TABLE session_message(id TEXT PRIMARY KEY,session_id TEXT,type TEXT,seq INTEGER,time_created INTEGER,time_updated INTEGER,data TEXT);
        INSERT INTO project VALUES('project','/removed/repository');
        INSERT INTO session_v2 VALUES('s','project','/removed/worktree',NULL,NULL,'Implement requested behavior','2.0.32',1790848800000,1790849400000);")?;
    if mixed {
        c.execute_batch("CREATE TABLE session(id TEXT PRIMARY KEY,project_id TEXT,directory TEXT,time_created INTEGER,time_updated INTEGER);
            CREATE TABLE message(id TEXT PRIMARY KEY,session_id TEXT,time_created INTEGER,time_updated INTEGER,data TEXT);
            CREATE TABLE part(id TEXT PRIMARY KEY,message_id TEXT,session_id TEXT,time_created INTEGER,time_updated INTEGER,data TEXT);
            INSERT INTO session VALUES('s','project','/removed/worktree',1790848800000,1790849400000);")?;
        c.execute(
            "INSERT INTO message VALUES('u','s',1790848800000,1790848800000,?1)",
            [json!({"role":"user"}).to_string()],
        )?;
        c.execute("INSERT INTO part VALUES('original-request-part','u','s',1790848800000,1790848800000,?1)",[json!({"type":"text","text":"Implement requested behavior"}).to_string()])?;
    }
    event(
        &c,
        "u",
        "user",
        0,
        json!({"text":"Implement requested behavior","time":{"created":1790848800000_i64}}),
    )?;
    event(
        &c,
        "system",
        "system",
        1,
        json!({"text":"Repository instructions","metadata":{"instructionSources":["/removed/repository/AGENTS.md"]}}),
    )?;
    event(
        &c,
        "a",
        "assistant",
        2,
        json!({"time":{"created":1790848860000_i64,"completed":1790848920000_i64},"model":{"id":"test-model","providerID":"test-provider"},"cost":0.03,"tokens":{"input":100,"output":20,"reasoning":5,"cache":{"read":50,"write":10}},"finish":"tool-calls","content":[{"type":"reasoning","text":"Hidden reasoning"},{"type":"text","text":"Implemented the requested behavior"},{"type":"tool","id":"skill-call","name":"skill","state":{"status":"completed","input":{"name":"pr-ready"},"content":[{"type":"text","text":"Skill instructions"}]},"time":{"created":1790848860000_i64,"ran":1790848860000_i64,"completed":1790848870000_i64}},{"type":"tool","id":"test-call","name":"bash","state":{"status":"completed","input":{"command":"cargo test"},"metadata":{"exit":1},"content":[{"type":"text","text":"Test failed"}]},"time":{"created":1790848880000_i64,"completed":1790848900000_i64}},{"type":"tool","id":"edit-call","name":"edit","state":{"status":"completed","input":{"filePath":"/removed/worktree/src/lib.rs"},"metadata":{"filediff":{"file":"src/lib.rs","additions":1,"deletions":0}},"content":[]},"time":{"created":1790848860000_i64,"completed":1790848870000_i64}}]}),
    )?;
    event(
        &c,
        "compact",
        "compaction",
        3,
        json!({"status":"completed","summary":"Conversation summary","model":{"id":"test-model","providerID":"test-provider"},"cost":0.01,"tokens":{"input":4,"output":2,"reasoning":1,"cache":{"read":0,"write":0}}}),
    )?;
    event(&c, "idle", "idle", 4, json!({"outcome":"succeeded"}))?;
    Ok(c)
}
fn event(c: &Connection, id: &str, kind: &str, seq: i64, data: Value) -> Result<()> {
    c.execute(
        "INSERT INTO session_message VALUES(?1,'s',?2,?3,?4,?4,?5)",
        params![
            id,
            kind,
            seq,
            1790848800000_i64 + seq * 60000,
            data.to_string()
        ],
    )?;
    Ok(())
}

#[test]
fn reads_v2_only_usage_tools_context_and_persistent_project_identity() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("opencode.db");
    let src = source(&path, false)?;
    let c = Connection::open_in_memory()?;
    crate::db::init_app_schema(&c)?;
    assert!(ingest_db(&path, &c)? > 0);
    assert_eq!(ingest_db(&path, &c)?, 0);
    let totals: (i64, i64, f64) = c.query_row(
        "SELECT COUNT(*),SUM(total_tokens),SUM(actual_cost_usd) FROM fact_session_usage",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    assert_eq!(totals, (2, 192, 0.04));
    let repo:(String,String,String)=c.query_row("SELECT r.repo_root,s.project_path,s.client_version FROM metadata_sessions s JOIN metadata_repositories r ON r.id=s.repository_id",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    assert_eq!(
        repo,
        (
            "/removed/repository".into(),
            "/removed/worktree".into(),
            "2.0.32".into()
        )
    );
    let calls: Vec<(String, String, String)> = c
        .prepare(
            "SELECT call_key,tool_name,status FROM fact_session_tool_call ORDER BY call_index",
        )?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .collect::<rusqlite::Result<_>>()?;
    assert_eq!(
        calls,
        vec![
            ("skill-call".into(), "skill".into(), "ok".into()),
            ("test-call".into(), "bash".into(), "error".into()),
            ("edit-call".into(), "edit".into(), "ok".into())
        ]
    );
    assert_eq!(
        c.query_row(
            "SELECT COUNT(*) FROM fact_session_context_injection WHERE kind='instructions'",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        1
    );
    assert_eq!(
        c.query_row(
            "SELECT COUNT(*) FROM fact_session_message WHERE is_compaction_summary=1",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        1
    );
    assert_eq!(c.query_row("SELECT COUNT(*) FROM fact_session_event WHERE kind='runtime_outcome' AND detail='succeeded'",[],|r|r.get::<_,i64>(0))?,1);
    crate::db::session_store::refresh_session_roots(&c)?;
    crate::insights::refresh(&c)?;
    assert!(crate::insights::work::items(&c)?[0].completed_at.is_none());
    let turns = crate::classify::state::load_turns(&c, "opencode", "s")?;
    assert_eq!(turns[0].runtime_outcomes, vec!["succeeded"]);
    assert!(!turns[0].assistant.contains("Conversation summary"));
    // A message can change while the session header timestamp stays unchanged.
    src.execute("UPDATE session_message SET time_updated=time_updated+1,data=json_set(data,'$.text','An updated request') WHERE id='u'",[])?;
    assert!(ingest_db(&path, &c)? > 0);
    Ok(())
}

#[test]
fn migrated_v2_replaces_v1_once_and_preserves_request_anchor_and_manual_links() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("opencode.db");
    let src = source(&path, true)?;
    src.execute("INSERT INTO message VALUES('old-assistant','s',1790848860000,1790848860000,?1)",[json!({"role":"assistant","modelID":"old-model","tokens":{"input":999,"output":1,"reasoning":0,"cache":{"read":0,"write":0}},"cost":0.5}).to_string()])?;
    src.execute_batch("ALTER TABLE session_v2 RENAME TO pending_v2; ALTER TABLE session_message RENAME TO pending_message")?;
    let c = Connection::open_in_memory()?;
    crate::db::init_app_schema(&c)?;
    ingest_db(&path, &c)?;
    let mut other = SessionRecord::new("codex", "other");
    other.started_at = Some("2026-10-02T10:00:00Z".into());
    other.messages.push(MessageRecord {
        key: "other-request".into(),
        ts: other.started_at.clone(),
        role: "user".into(),
        text: "Continue the same work".into(),
        is_compaction_summary: false,
    });
    session_store::store_session_record(&c, &other)?;
    session_store::refresh_session_roots(&c)?;
    crate::insights::refresh(&c)?;
    let original = crate::insights::work::resolve_member(&c, "opencode:s#0")?;
    let next = crate::insights::work::resolve_member(&c, "codex:other#0")?;
    crate::insights::work::insert_relation(
        &c,
        &original,
        &next,
        "continuation",
        "accepted",
        "manual",
        "same goal",
        Some(1.0),
    )?;
    crate::classify::overrides::set_label(
        &c,
        "task",
        "opencode|s|0",
        "intent",
        Some("feature"),
        None,
    )?;
    src.execute_batch("ALTER TABLE pending_v2 RENAME TO session_v2; ALTER TABLE pending_message RENAME TO session_message")?;
    ingest_db(&path, &c)?;
    session_store::refresh_session_roots(&c)?;
    crate::insights::refresh(&c)?;
    assert_eq!(
        crate::insights::work::resolve_member(&c, "opencode:s#0")?,
        original
    );
    assert_eq!(crate::insights::work::items(&c)?.len(), 1);
    assert_eq!(c.query_row("SELECT label FROM classification_override WHERE unit_key='opencode|s|0' AND question_id='intent'",[],|r|r.get::<_,String>(0))?,"feature");
    assert_eq!(
        c.query_row(
            "SELECT COUNT(*) FROM metadata_sessions WHERE provider='opencode'",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        1
    );
    assert_eq!(
        c.query_row(
            "SELECT SUM(total_tokens) FROM fact_session_usage",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        192
    );
    assert_eq!(
        c.query_row(
            "SELECT message_key FROM fact_session_message WHERE role='user' AND provider='opencode'",
            [],
            |r| r.get::<_, String>(0)
        )?,
        "original-request-part"
    );
    Ok(())
}

#[test]
fn unknown_v2_events_fail_visibly_instead_of_reporting_complete_coverage() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("opencode.db");
    let src = source(&path, false)?;
    event(&src, "future", "new-usage-format", 9, json!({}))?;
    let c = Connection::open_in_memory()?;
    crate::db::init_app_schema(&c)?;
    let error = ingest_planned_sessions(&c, &[path], false, None).unwrap_err();
    assert!(format!("{error:#}").contains("Unsupported OpenCode V2 message type"));
    assert_eq!(
        c.query_row("SELECT COUNT(*) FROM session_source_state", [], |r| r
            .get::<_, i64>(0))?,
        0
    );
    Ok(())
}

#[test]
fn global_opencode_project_is_not_assigned_as_root_repository() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("opencode.db");
    let src = source(&path, false)?;
    src.execute("UPDATE project SET worktree='/'", [])?;
    assert!(session_headers(&src)?[0].repository_hint().is_none());
    Ok(())
}

#[test]
fn incomplete_migration_keeps_legacy_messages_and_future_stores_fail_visibly() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("opencode.db");
    let src = source(&path, true)?;
    src.execute("DELETE FROM session_message", [])?;
    let headers = session_headers(&src)?;
    assert_eq!(headers.len(), 1);
    assert!(!headers[0].v2);
    let c = Connection::open_in_memory()?;
    crate::db::init_app_schema(&c)?;
    ingest_db(&path, &c)?;
    assert_eq!(
        c.query_row(
            "SELECT COUNT(*) FROM fact_session_message WHERE message_key='original-request-part'",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        1
    );
    src.execute_batch("CREATE TABLE session_v3(id TEXT)")?;
    assert!(
        session_headers(&src)
            .unwrap_err()
            .to_string()
            .contains("Unsupported OpenCode session store session_v3")
    );
    Ok(())
}

#[test]
fn v2_file_metadata_and_write_paths_preserve_code_changes() {
    let edit = json!({"type":"tool","tool":"edit","state":{"metadata":{"files":[{"file":"src/lib.rs","patch":"@@ -1 +1,2 @@\n-old\n+new\n+line","additions":2,"deletions":1}]}}});
    let edits = edits_from_tool_part(&edit, Some("/removed/worktree"), &None);
    assert_eq!(edits.len(), 1);
    assert_eq!((edits[0].added_lines, edits[0].removed_lines), (2, 1));
    assert!(edits[0].file_path.ends_with("src/lib.rs"));
    let write = json!({"type":"tool","tool":"write","state":{"input":{"path":"src/new.rs","content":"first\nsecond\n"}}});
    let writes = edits_from_tool_part(&write, Some("/removed/worktree"), &None);
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].added_lines, 2);
}
