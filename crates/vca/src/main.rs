fn main() -> anyhow::Result<()> {
    let context = vca::app::AppContext::VCA;
    let cli: vca::cli::Cli = context.parse();
    vca::app::run(cli.command, cli.verbose, context)
}
