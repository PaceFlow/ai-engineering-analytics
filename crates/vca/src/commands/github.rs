use anyhow::{Result, bail};
use std::io::{self, Write};

use crate::cli::{GitHubArgs, GitHubCommands};
use crate::github::auth::{
    delete_saved_github_token, github_token_from_env_for, load_saved_github_token,
    save_github_token,
};

pub fn run(args: GitHubArgs) -> Result<()> {
    run_for(args, crate::app::AppContext::VCA)
}

pub fn run_for(args: GitHubArgs, context: crate::app::AppContext) -> Result<()> {
    crate::data_home::ensure_ready()?;
    match args.command {
        GitHubCommands::Token => run_token(context),
    }
}

fn run_token(context: crate::app::AppContext) -> Result<()> {
    match load_saved_github_token()? {
        Some(_) => prompt_existing_token_flow(context),
        None => prompt_initial_token_flow(context),
    }
}

fn prompt_initial_token_flow(context: crate::app::AppContext) -> Result<()> {
    let token = prompt_line("Paste GitHub token and press Enter: ")?;
    let path = save_github_token(&token)?;
    println!("Saved GitHub token to {}", path.display());
    if github_token_from_env_for(context).is_some() {
        println!(
            "{}_GITHUB_TOKEN is currently set and will continue to override the saved token.",
            context.env_prefix
        );
    }
    Ok(())
}

fn prompt_existing_token_flow(context: crate::app::AppContext) -> Result<()> {
    println!("A saved GitHub token already exists.");
    if github_token_from_env_for(context).is_some() {
        println!(
            "{}_GITHUB_TOKEN is currently set and will continue to override the saved token.",
            context.env_prefix
        );
    }
    print!("Type `update` to replace it or `delete` to remove it: ");
    io::stdout().flush()?;
    let choice = read_line()?;
    match choice.as_str() {
        "update" | "u" => {
            let token = prompt_line("Paste new GitHub token and press Enter: ")?;
            let path = save_github_token(&token)?;
            println!("Updated saved GitHub token at {}", path.display());
            Ok(())
        }
        "delete" | "d" => {
            delete_saved_github_token()?;
            println!("Deleted saved GitHub token.");
            Ok(())
        }
        _ => bail!("Expected `update` or `delete`"),
    }
}

fn prompt_line(prompt: &str) -> Result<String> {
    print!("{prompt}");
    io::stdout().flush()?;
    let value = read_line()?;
    if value.is_empty() {
        bail!("GitHub token cannot be empty");
    }
    Ok(value)
}

fn read_line() -> Result<String> {
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input.trim().to_string())
}
