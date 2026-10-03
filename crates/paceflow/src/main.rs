use paceflow::cli::{Cli, Commands};
use paceflow::commands;

fn main() -> anyhow::Result<()> {
    let context = vca::app::AppContext {
        command: "paceflow",
        title: "Paceflow",
        version: paceflow::cli::VERSION,
        env_prefix: "PACEFLOW",
    };
    let cli: Cli = context.parse();
    match cli.command {
        Commands::Analytics(command) => vca::app::run(command, cli.verbose, context),
        Commands::Sync(args) => commands::sync::run(args),
        Commands::Hooks(args) => commands::hooks::run(args),
    }
}
