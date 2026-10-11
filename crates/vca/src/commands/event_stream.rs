use anyhow::Result;
use std::io::{self, Write};

use crate::analytics;
use crate::cli::EventStreamArgs;
use crate::db;

pub fn run(args: EventStreamArgs) -> Result<()> {
    let db = db::open()?;
    analytics::create_reporting_views(&db)?;
    let rows = analytics::query_event_stream(&db, &args)?;
    let total_rows = rows.len();

    let stdout = io::stdout();
    let mut handle = stdout.lock();
    for (index, row) in rows.into_iter().enumerate() {
        if args.pretty {
            serde_json::to_writer_pretty(&mut handle, &row)?;
            handle.write_all(b"\n")?;
            if index + 1 < total_rows {
                handle.write_all(b"\n")?;
            }
        } else {
            serde_json::to_writer(&mut handle, &row)?;
            handle.write_all(b"\n")?;
        }
    }
    Ok(())
}
