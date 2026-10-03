//! Explicit product identity used by the shared command implementation.
use clap::{Command, CommandFactory, FromArgMatches};

#[derive(Clone, Copy, Debug)]
pub struct AppContext {
    pub command: &'static str,
    pub title: &'static str,
    pub version: &'static str,
    pub env_prefix: &'static str,
}

impl AppContext {
    pub const VCA: Self = Self {
        command: "vca",
        title: "Vibe Coding Analytics",
        version: crate::cli::VERSION,
        env_prefix: "VCA",
    };

    /// Product-specific overrides take precedence over the shared VCA variables.
    pub fn env(self, suffix: &str) -> Option<std::ffi::OsString> {
        std::env::var_os(format!("{}_{}", self.env_prefix, suffix))
            .filter(|value| !value.is_empty())
            .or_else(|| std::env::var_os(format!("VCA_{suffix}")).filter(|value| !value.is_empty()))
    }

    pub fn configure_command(self, command: Command) -> Command {
        fn brand(mut command: Command, name: &str) -> Command {
            if let Some(help) = command.get_after_help() {
                let text = help.to_string().replace("vca ", &format!("{name} "));
                command = command.after_help(text);
            }
            let children: Vec<_> = command
                .get_subcommands()
                .map(|c| c.get_name().to_string())
                .collect();
            for child in children {
                command = command.mut_subcommand(child, |c| brand(c, name));
            }
            command
        }
        brand(command, self.command)
            .name(self.command)
            .version(self.version)
    }

    pub fn parse<T: CommandFactory + FromArgMatches>(self) -> T {
        let mut command = self.configure_command(T::command());
        let matches = command.clone().get_matches();
        T::from_arg_matches(&matches).unwrap_or_else(|err| err.format(&mut command).exit())
    }
}

pub fn run(
    command: crate::cli::Commands,
    verbose: bool,
    context: AppContext,
) -> anyhow::Result<()> {
    use crate::cli::Commands;
    use crate::commands;
    match command {
        Commands::Ingest(args) => commands::ingest::run_for(verbose, args, context),
        Commands::Session(args) => commands::session::run_for(args, context),
        Commands::Delivery(args) => commands::delivery::run_for(args, context),
        Commands::Quality(args) => commands::quality::run_for(args, context),
        Commands::Cost(args) => commands::cost::run_for(args, context),
        Commands::EventStream(args) => commands::event_stream::run(args),
        Commands::GitHub(args) => commands::github::run_for(args, context),
        Commands::Tui(args) => commands::tui::run_for(args, context),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{ScopedEnvVar, lock_env};

    #[test]
    fn product_overrides_take_precedence_and_vca_ignores_backend_product_overrides() {
        let _guard = lock_env();
        let _shared = ScopedEnvVar::set("VCA_GITHUB_TOKEN", "shared");
        let _paceflow = ScopedEnvVar::set("PACEFLOW_GITHUB_TOKEN", "paceflow");
        let context = AppContext {
            env_prefix: "PACEFLOW",
            ..AppContext::VCA
        };
        assert_eq!(AppContext::VCA.env("GITHUB_TOKEN").unwrap(), "shared");
        assert_eq!(context.env("GITHUB_TOKEN").unwrap(), "paceflow");
        let _empty = ScopedEnvVar::set("PACEFLOW_GITHUB_TOKEN", "");
        assert_eq!(context.env("GITHUB_TOKEN").unwrap(), "shared");
    }
}
