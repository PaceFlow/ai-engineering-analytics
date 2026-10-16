//! Interactive exploration TUI: navigation stack, background loading,
//! drill-down from totals to sessions, tasks, tool calls and commits.

pub mod app;
pub mod data;
pub mod explain;
pub mod ui;

#[cfg(test)]
mod tests;

use std::io;
use std::time::Duration;

use anyhow::Result;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::crossterm::event::{self, Event};
use ratatui::crossterm::execute;
use ratatui::crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};

use crate::cli::TuiArgs;
use crate::explore::Filters;

pub fn run_for(args: TuiArgs, context: crate::app::AppContext) -> Result<()> {
    // Open once up front so schema problems surface before raw mode.
    drop(crate::db::open()?);
    let mut filters = Filters::default();
    if args.here && !args.all_projects {
        filters.repo = std::env::current_dir()
            .ok()
            .and_then(|dir| crate::path_utils::detect_repo_root(&dir))
            .map(|root| crate::path_utils::path_to_string(&root));
    }
    let loader = data::ThreadLoader::spawn(crate::db::open);
    let mut app = app::App::new(Box::new(loader), filters, context.title);

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    let result = run_loop(&mut terminal, &mut app);
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    result
}

fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut app::App,
) -> Result<()> {
    while !app.should_quit {
        app.drain();
        terminal.draw(|frame| ui::draw(frame, app))?;
        if event::poll(Duration::from_millis(120))?
            && let Event::Key(key) = event::read()?
        {
            app.on_key(key);
        }
    }
    Ok(())
}
