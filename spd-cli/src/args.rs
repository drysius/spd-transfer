//! Command-line surface.
//!
//! This module only parses and validates input. No protocol decision is made here - that
//! belongs to `spd-core`, so the same behaviour is reachable from a test without going
//! through `clap`.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};
use spd_core::pipeline::budget::JobLimits;
use spd_core::pipeline::retry::RetryPolicy;

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

    /// How much memory the transfer may hold, in MiB. Concurrency follows from this.
    #[arg(long, default_value_t = 256, value_name = "MIB")]
    pub(crate) mem_budget_mb: u64,

    /// Upper bound on files in flight, one stream each. The memory budget can lower it.
    #[arg(long, default_value_t = 16, value_name = "N")]
    pub(crate) streams: u32,

    /// Send every file as it is, without compressing anything. Worth it on a link fast
    /// enough that the processor, not the network, is what you are short of.
    #[arg(long)]
    pub(crate) no_compress: bool,

    /// Files read from disk at once. Raise it on an SSD, leave it low on a spinning disk.
    #[arg(long, default_value_t = 4, value_name = "N")]
    pub(crate) disk_read_jobs: u32,

    /// Buffers being compressed at once. Defaults to the number of cores; lower it to
    /// leave the machine room for something else.
    #[arg(long, default_value_t = default_cpu_jobs(), value_name = "N")]
    pub(crate) cpu_jobs: u32,

    /// How many times to try, counting the first attempt. A dropped connection is picked
    /// up where it stopped; anything else fails immediately.
    #[arg(long, default_value_t = RetryPolicy::DEFAULT.attempts.get(), value_name = "N")]
    pub(crate) attempts: u32,
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

    /// How much memory the transfer may hold, in MiB. Concurrency follows from this.
    #[arg(long, default_value_t = 256, value_name = "MIB")]
    pub(crate) mem_budget_mb: u64,

    /// Upper bound on files in flight, one stream each. The memory budget can lower it.
    #[arg(long, default_value_t = 16, value_name = "N")]
    pub(crate) streams: u32,

    /// Files written to disk at once. Raise it on an SSD, leave it low on a spinning disk.
    #[arg(long, default_value_t = 4, value_name = "N")]
    pub(crate) disk_write_jobs: u32,

    /// Buffers being decompressed at once. Defaults to the number of cores; lower it to
    /// leave the machine room for something else.
    #[arg(long, default_value_t = default_cpu_jobs(), value_name = "N")]
    pub(crate) cpu_jobs: u32,

    /// How many sessions to accept before giving up, counting the first. A sender that
    /// reconnects finds the listener still waiting and continues where it stopped.
    #[arg(long, default_value_t = RetryPolicy::DEFAULT.attempts.get(), value_name = "N")]
    pub(crate) attempts: u32,
}

fn default_listen_address() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::UNSPECIFIED, DEFAULT_PORT))
}

/// One codec job per core.
///
/// The core count is not a constant, so it cannot live in `JobLimits::DEFAULT`; this is
/// where the program finds out what machine it is on.
fn default_cpu_jobs() -> u32 {
    std::thread::available_parallelism()
        .map(|cores| u32::try_from(cores.get()).unwrap_or(u32::MAX))
        .unwrap_or(JobLimits::DEFAULT.cpu_jobs.get())
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
