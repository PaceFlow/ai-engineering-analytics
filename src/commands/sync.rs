use anyhow::{Result, anyhow, bail};
use std::io::{self, Write};

use crate::cli::{SyncArgs, SyncCommands, SyncPushArgs, SyncStatusArgs};
use crate::db;
use crate::sync::{
    SavedSyncConfig, SyncApiClient, SyncConfigSource, delete_saved_sync_config, env_override_keys,
    grouped_event_counts, last_sync_run_state, load_saved_sync_config, make_push_request,
    mark_synced_events, normalized_base_url, partition_eligible_sync_events, pending_sync_events,
    reset_local_sync_state, resolve_sync_scope, resolved_sync_config, save_sync_config,
};
use crate::sync_identity;

const DEFAULT_SYNC_API_BASE_URL: &str = "http://localhost:8443";

pub fn run(args: SyncArgs) -> Result<()> {
    match args.command {
        SyncCommands::Config => run_config(),
        SyncCommands::Push(args) => run_push(args),
        SyncCommands::Status(args) => run_status(args),
        SyncCommands::Reset => run_reset(),
    }
}

fn run_config() -> Result<()> {
    match load_saved_sync_config()? {
        Some(_) => prompt_existing_config_flow(),
        None => prompt_initial_config_flow(),
    }
}

fn run_push(args: SyncPushArgs) -> Result<()> {
    let config = resolved_sync_config()?
        .ok_or_else(|| anyhow!("Sync is not configured. Run `paceflow sync config` first."))?;
    let scope = resolve_sync_scope(args.repo.as_deref(), args.all_projects)?;
    let mut conn = db::open()?;
    let pending = pending_sync_events(&conn, &config.organization_id, &scope)?;

    if pending.is_empty() {
        println!(
            "No pending sync events for {} ({})",
            config
                .organization_name
                .as_deref()
                .unwrap_or("(unnamed organization)"),
            config.organization_id
        );
        return Ok(());
    }

    println!(
        "Uploading {} pending sync events to {} ({})",
        pending.len(),
        config
            .organization_name
            .as_deref()
            .unwrap_or("(unnamed organization)"),
        config.organization_id
    );
    print_event_counts("Pending", &grouped_event_counts(&pending));

    let runtime = new_runtime()?;
    let client = SyncApiClient::new(config.base_url.clone(), Some(config.token.clone()))?;
    let allowlist = runtime.block_on(client.repositories(&config.organization_id))?;
    let partitioned = partition_eligible_sync_events(pending, &allowlist);

    if !partitioned.skipped.is_empty() {
        println!(
            "Skipping {} pending sync events from {} repos not recognized by PaceFlow.",
            partitioned.skipped.len(),
            partitioned.skipped_repo_keys.len()
        );
    }

    if partitioned.eligible.is_empty() {
        println!(
            "No eligible org project sync events found. Skipped {} pending sync events from {} repos not recognized by PaceFlow.",
            partitioned.skipped.len(),
            partitioned.skipped_repo_keys.len()
        );
        return Ok(());
    }

    println!(
        "Uploading {} eligible org project sync events.",
        partitioned.eligible.len()
    );
    print_event_counts("Eligible", &grouped_event_counts(&partitioned.eligible));

    let batch_size = args.batch_size.max(1);
    let mut uploaded = 0usize;

    for chunk in partitioned.eligible.chunks(batch_size) {
        let request = make_push_request(chunk);
        let response = runtime.block_on(client.push_events(&config.organization_id, &request))?;
        if response.rejected > 0 || response.accepted != chunk.len() {
            bail!(
                "sync push was only partially accepted: accepted={} rejected={}",
                response.accepted,
                response.rejected
            );
        }
        mark_synced_events(
            &mut conn,
            &config.organization_id,
            chunk,
            &response.checkpoint,
        )?;
        uploaded += response.accepted;
    }

    println!("Uploaded {uploaded} sync events.");
    Ok(())
}

fn run_status(args: SyncStatusArgs) -> Result<()> {
    let Some(config) = resolved_sync_config()? else {
        println!("Sync is not configured. Run `paceflow sync config` first.");
        return Ok(());
    };

    let scope = resolve_sync_scope(args.repo.as_deref(), args.all_projects)?;
    let conn = db::open()?;
    let pending = pending_sync_events(&conn, &config.organization_id, &scope)?;
    let counts = grouped_event_counts(&pending);

    println!("Sync Configuration");
    println!(
        "Base URL: {} ({})",
        config.base_url,
        format_source(config.base_url_source)
    );
    println!(
        "Organization: {} ({}) ({})",
        config
            .organization_name
            .as_deref()
            .unwrap_or("(unnamed organization)"),
        config.organization_id,
        format_source(config.organization_id_source)
    );
    println!("Token: configured ({})", format_source(config.token_source));

    if let Some(run_state) = last_sync_run_state(&conn, &config.organization_id)? {
        println!(
            "Last Successful Push: {}",
            run_state.last_successful_push_at
        );
        println!(
            "Last Server Checkpoint: {}",
            run_state.last_server_checkpoint.as_deref().unwrap_or("-")
        );
    }

    print_event_counts("Local Pending", &counts);

    let runtime = new_runtime()?;
    let client = SyncApiClient::new(config.base_url.clone(), Some(config.token.clone()))?;
    match runtime.block_on(client.repositories(&config.organization_id)) {
        Ok(allowlist) => {
            let partitioned = partition_eligible_sync_events(pending.clone(), &allowlist);
            println!("\nOrg Project Eligibility");
            println!("Recognized Repositories: {}", allowlist.repositories.len());
            println!("Eligible Pending Events: {}", partitioned.eligible.len());
            println!("Skipped Pending Events: {}", partitioned.skipped.len());
            println!(
                "Skipped Repositories: {}",
                partitioned.skipped_repo_keys.len()
            );
        }
        Err(err) => {
            println!("\nOrg Project Eligibility");
            println!("Warning: could not load recognized repositories: {err}");
        }
    }

    match runtime.block_on(client.status(&config.organization_id)) {
        Ok(remote) => {
            println!("\nRemote Status");
            println!(
                "Organization: {} ({})",
                remote
                    .organization_name
                    .as_deref()
                    .unwrap_or("(unnamed organization)"),
                remote.organization_id
            );
            println!("Stored Events: {}", remote.total_events);
            println!(
                "Last Event At: {}",
                remote.last_event_at.as_deref().unwrap_or("-")
            );
        }
        Err(err) => {
            let message = err.to_string();
            if message.contains("401") || message.contains("403") {
                return Err(err);
            }
            println!("\nRemote Status");
            println!("Unavailable: {message}");
        }
    }

    Ok(())
}

fn run_reset() -> Result<()> {
    let deleted = delete_saved_sync_config()?;
    let conn = db::open()?;
    reset_local_sync_state(&conn)?;
    if deleted {
        println!("Deleted saved sync configuration and cleared local sync state.");
    } else {
        println!("Cleared local sync state. No saved sync configuration was present.");
    }
    Ok(())
}

fn prompt_initial_config_flow() -> Result<()> {
    let config = prompt_sync_configuration()?;
    let path = save_sync_config(&config)?;
    println!("Saved sync configuration to {}", path.display());
    print_env_override_notice();
    Ok(())
}

fn prompt_existing_config_flow() -> Result<()> {
    println!("A saved sync configuration already exists.");
    print_env_override_notice();
    print!("Type `update` to replace it or `delete` to remove it: ");
    io::stdout().flush()?;
    let choice = read_line()?;
    match choice.as_str() {
        "update" | "u" => prompt_initial_config_flow(),
        "delete" | "d" => {
            delete_saved_sync_config()?;
            println!("Deleted saved sync configuration.");
            Ok(())
        }
        _ => bail!("Expected `update` or `delete`"),
    }
}

fn prompt_sync_configuration() -> Result<SavedSyncConfig> {
    let base_url = prompt_sync_api_base_url()?;
    let organization_id =
        parse_organization_setup_input(&prompt_line("PaceFlow organization ID or setup URL: ")?)?;
    let email = prompt_line("PaceFlow person email: ")?;

    let runtime = new_runtime()?;
    let unauthenticated = SyncApiClient::new(base_url.clone(), None)?;
    runtime.block_on(unauthenticated.request_person_link(&organization_id, &email))?;
    println!("Sent a PaceFlow CLI sync verification code to {email}.");
    let code = prompt_line("Verification code: ")?;
    let linked = runtime.block_on(unauthenticated.verify_person_link(
        &organization_id,
        &email,
        &code,
        &sync_identity::device_id(),
    ))?;

    Ok(SavedSyncConfig {
        base_url,
        organization_id: linked.organization_id,
        organization_name: linked.organization_name,
        member_email: Some(linked.member_email),
        token: linked.token,
    })
}

fn prompt_sync_api_base_url() -> Result<String> {
    print!("PaceFlow API base URL [{DEFAULT_SYNC_API_BASE_URL}]: ");
    io::stdout().flush()?;
    normalize_prompted_sync_api_base_url(&read_line()?)
}

fn normalize_prompted_sync_api_base_url(raw: &str) -> Result<String> {
    let value = raw.trim();
    if value.is_empty() {
        return normalized_base_url(DEFAULT_SYNC_API_BASE_URL);
    }
    normalized_base_url(value)
}

fn parse_organization_setup_input(raw: &str) -> Result<String> {
    let value = raw.trim();
    if value.is_empty() {
        bail!("PaceFlow organization ID cannot be empty");
    }
    if let Some((_, tail)) = value.split_once("/organizations/") {
        return tail
            .split(['/', '?', '#'])
            .next()
            .filter(|part| !part.trim().is_empty())
            .map(|part| part.trim().to_string())
            .ok_or_else(|| anyhow!("Could not read organization ID from setup URL"));
    }
    Ok(value.to_string())
}

fn print_env_override_notice() {
    let active = env_override_keys()
        .iter()
        .copied()
        .filter(|key| std::env::var(key).ok().is_some())
        .collect::<Vec<_>>();
    if active.is_empty() {
        return;
    }

    println!(
        "{} will continue to override the saved sync configuration.",
        active.join(", ")
    );
}

fn print_event_counts(label: &str, counts: &std::collections::BTreeMap<String, usize>) {
    println!("{label} Events");
    if counts.is_empty() {
        println!("  none");
        return;
    }
    for (event_type, count) in counts {
        println!("  {event_type}: {count}");
    }
}

fn format_source(source: SyncConfigSource) -> &'static str {
    match source {
        SyncConfigSource::Environment => "env",
        SyncConfigSource::Saved => "saved",
    }
}

fn prompt_line(prompt: &str) -> Result<String> {
    print!("{prompt}");
    io::stdout().flush()?;
    let value = read_line()?;
    if value.is_empty() {
        bail!("Input cannot be empty");
    }
    Ok(value)
}

fn read_line() -> Result<String> {
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input.trim().to_string())
}

fn new_runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_sync_api_base_url_uses_local_backend_default() -> Result<()> {
        assert_eq!(
            normalize_prompted_sync_api_base_url("")?,
            DEFAULT_SYNC_API_BASE_URL
        );
        assert_eq!(
            normalize_prompted_sync_api_base_url("   ")?,
            DEFAULT_SYNC_API_BASE_URL
        );
        Ok(())
    }

    #[test]
    fn sync_api_base_url_still_accepts_explicit_url() -> Result<()> {
        assert_eq!(
            normalize_prompted_sync_api_base_url("http://localhost:3000/")?,
            "http://localhost:3000"
        );
        Ok(())
    }

    #[test]
    fn sync_api_base_url_rejects_non_url_values() {
        let err = normalize_prompted_sync_api_base_url("localhost:8443")
            .expect_err("missing scheme should fail");
        assert!(err.to_string().contains("http:// or https://"));
    }
}
