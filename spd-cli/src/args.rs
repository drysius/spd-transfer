//! Command-line surface.
//!
//! This module only parses and validates input. No protocol decision is made here - that
//! belongs to `spd-core`, so the same behaviour is reachable from a test without going
//! through `clap`.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;

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
    /// Send a file to a waiting peer.
    Send(SendArgs),

    /// Wait for a peer and receive into a directory.
    Recv(RecvArgs),

    /// Show the effective limits and the concurrency they produce, without touching the
    /// network.
    Doctor(DoctorArgs),
}

/// Port used when none is given. Unassigned by IANA, and easy to remember.
pub(crate) const DEFAULT_PORT: u16 = 9432;

/// Options for `spd send`.
#[derive(Debug, clap::Args)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "command-line switches are booleans; grouping them would only hide the surface"
)]
pub(crate) struct SendArgs {
    /// File or directory to send.
    #[arg(value_name = "PATH")]
    pub(crate) file: PathBuf,

    /// Where the receiver is listening, as `host:port`.
    #[arg(value_name = "ADDRESS")]
    pub(crate) address: SocketAddr,

    /// Accept any peer without verifying who it is. Traffic stays encrypted; nothing
    /// proves the receiver is the machine you meant. Required until pairing exists.
    #[arg(long)]
    pub(crate) insecure: bool,

    /// Negotiate as usual and report what would move, without sending any file.
    #[arg(long)]
    pub(crate) dry_run: bool,

    /// Hash every file instead of trusting size and timestamp. Slower, and certain.
    #[arg(long)]
    pub(crate) checksum: bool,

    /// Follow symlinks while scanning. Off by default: a link can point outside the tree
    /// you meant to send.
    #[arg(long)]
    pub(crate) follow_links: bool,
}

/// Options for `spd recv`.
#[derive(Debug, clap::Args)]
pub(crate) struct RecvArgs {
    /// Directory to write into. It must already exist.
    #[arg(long, short, default_value = ".", value_name = "DIR")]
    pub(crate) out: PathBuf,

    /// Address to listen on.
    #[arg(long, default_value_t = default_listen_address(), value_name = "ADDRESS")]
    pub(crate) listen: SocketAddr,

    /// Accept any peer without verifying who it is. Traffic stays encrypted; nothing
    /// proves the sender is who you expect. Required until pairing exists.
    #[arg(long)]
    pub(crate) insecure: bool,

    /// Hash local files before deciding, instead of trusting size and timestamp.
    #[arg(long)]
    pub(crate) checksum: bool,
}

fn default_listen_address() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::UNSPECIFIED, DEFAULT_PORT))
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
