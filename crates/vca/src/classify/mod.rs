//! Optional conversation classification: local heuristics always, and the
//! hosted Jev classifier when the user opts in.
pub mod automatic;
pub mod client;
pub mod config;
pub mod heuristic;
pub mod overrides;
pub mod questions;
pub mod run;
pub mod state;
