//! Command-line surface.
//!
//! This module only parses and validates input. No protocol decision is made here - that
//! belongs to `spd-core`, so the same behaviour is reachable from a test without going
//! through `clap`.

use clap::{Parser, Subcommand, ValueEnum};

/// Peer-to-peer file and folder transfer over QUIC.
#[derive(Debug, Parser)]
#[command(name = "spd", version, about, long_about = None)]
pub(crate) struct Cli {
    /// Log verbosity: error, warn, info, debug or trace.
    #[arg(long, global = true, default_value = "info", value_name = "LEVEL")]
    pub(crate) log_level: String,

    /// Log output shape: human-readable text, or JSON for machines.
    #[arg(long, global = true, value_enum, default_value_t = LogFormat::Text)]
    pub(crate) log_format: LogFormat,

    /// What to run.
    #[command(subcommand)]
    pub(crate) command: Command,
}

/// Log output shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum LogFormat {
    /// One line per event, for a human at a terminal.
    Text,
    /// One JSON object per event, for a log collector.
    Json,
}

/// Available subcommands.
#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Show the effective limits and the concurrency they produce, without touching the
    /// network.
    Doctor(DoctorArgs),
}

/// Options for `spd doctor`.
#[derive(Debug, clap::Args)]
pub(crate) struct DoctorArgs {
    /// Memory budget for the transfer, in MiB. Concurrency is derived from this, never
    /// the other way around.
    #[arg(long, default_value_t = 256, value_name = "MIB")]
    pub(crate) mem_budget_mb: u64,

    /// Upper bound on files transferred at once, one QUIC stream each.
    #[arg(long, default_value_t = 16, value_name = "N")]
    pub(crate) streams: u32,
}
