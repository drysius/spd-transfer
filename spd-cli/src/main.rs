//! `spd` - command-line front end for peer-to-peer file transfer.
//!
//! This binary owns argument parsing, logging setup and human-facing wording. Every
//! protocol decision lives in `spd-core`, so nothing here can change how a transfer
//! behaves.

mod args;
mod doctor;
mod recv;
mod send;
mod trust;
mod ui;

use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

use crate::args::{Cli, Command, LogFormat};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_logging(&cli.log_level, cli.log_format)?;

    match &cli.command {
        Command::Send(args) => send::run(args).await,
        Command::Recv(args) => recv::run(args).await,
        Command::Doctor(args) => doctor::run(args),
    }
}

/// Installs the global tracing subscriber.
///
/// Logs go to stderr so `--log-format json` on stdout never gets mixed with data another
/// program is meant to read.
///
/// # Errors
/// Fails if `level` is not a valid filter directive.
fn init_logging(level: &str, format: LogFormat) -> Result<()> {
    let filter = EnvFilter::try_new(level)
        .with_context(|| format!("--log-level {level} is not a valid filter"))?;

    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);

    match format {
        LogFormat::Text => builder.init(),
        LogFormat::Json => builder.json().init(),
    }

    Ok(())
}
