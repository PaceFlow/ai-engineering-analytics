use anyhow::{Result, bail};
use std::io::{self, Write};

use crate::cli::{GitHubArgs, GitHubCommands};
use crate::github::auth::{
    GITHUB_TOKEN_ENV_VAR, delete_saved_github_token, github_token_from_env,
    load_saved_github_token, save_github_token,
};

pub fn run(args: GitHubArgs) -> Result<()> {
    match args.command {
        GitHubCommands::Token => run_token(),
    }
}

fn run_token() -> Result<()> {
    match load_saved_github_token()? {
        Some(_) => prompt_existing_token_flow(),
        None => prompt_initial_token_flow(),
    }
}

fn prompt_initial_token_flow() -> Result<()> {
    let token = prompt_line("Paste GitHub token and press Enter: ")?;
    let path = save_github_token(&token)?;
    println!("Saved GitHub token to {}", path.display());
    if github_token_from_env().is_some() {
        println!(
            "{} is currently set and will continue to override the saved token.",
            GITHUB_TOKEN_ENV_VAR
        );
    }
    Ok(())
}

fn prompt_existing_token_flow() -> Result<()> {
    println!("A saved GitHub token already exists.");
    if github_token_from_env().is_some() {
        println!(
            "{} is currently set and will continue to override the saved token.",
            GITHUB_TOKEN_ENV_VAR
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
