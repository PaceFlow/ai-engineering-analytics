use anyhow::{Result, anyhow, bail};
use std::io::{self, Write};

use crate::cli::{SyncArgs, SyncCommands, SyncPushArgs, SyncStatusArgs};
use crate::db;
use crate::sync::{
    SavedSyncConfig, SyncApiClient, SyncConfigSource, SyncOrganization, delete_saved_sync_config,
    env_override_keys, grouped_event_counts, last_sync_run_state, load_saved_sync_config,
    make_push_request, mark_synced_events, normalized_base_url, pending_sync_counts,
    pending_sync_events, reset_local_sync_state, resolve_sync_scope, resolved_sync_config,
    save_sync_config,
};

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
    let batch_size = args.batch_size.max(1);
    let mut uploaded = 0usize;

    for chunk in pending.chunks(batch_size) {
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
    let counts = pending_sync_counts(&conn, &config.organization_id, &scope)?;

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
    let base_url = normalized_base_url(&prompt_line("PaceFlow base URL: ")?)?;
    let email = prompt_line("PaceFlow email: ")?;
    let password = prompt_line("PaceFlow password: ")?;

    let runtime = new_runtime()?;
    let unauthenticated = SyncApiClient::new(base_url.clone(), None)?;
    let token = runtime.block_on(unauthenticated.login(&email, &password))?;

    let authenticated = SyncApiClient::new(base_url.clone(), Some(token.clone()))?;
    let account = runtime.block_on(authenticated.account_me())?;
    let organization = choose_default_organization(&account.organizations)?;

    Ok(SavedSyncConfig {
        base_url,
        organization_id: organization.id,
        organization_name: organization.name,
        token,
    })
}

fn choose_default_organization(organizations: &[SyncOrganization]) -> Result<SyncOrganization> {
    if organizations.is_empty() {
        bail!("the authenticated account has no accessible organizations");
    }
    if organizations.len() == 1 {
        return Ok(organizations[0].clone());
    }

    println!("Available organizations:");
    for (index, organization) in organizations.iter().enumerate() {
        println!(
            "  {}. {} ({})",
            index + 1,
            organization
                .name
                .as_deref()
                .unwrap_or("(unnamed organization)"),
            organization.id
        );
    }

    let choice = prompt_line("Choose default organization number: ")?;
    choice
        .parse::<usize>()
        .ok()
        .and_then(|index| organizations.get(index.saturating_sub(1)).cloned())
        .ok_or_else(|| anyhow!("Invalid organization selection"))
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
