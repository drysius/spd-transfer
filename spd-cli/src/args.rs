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
use spd_core::safety::path::NamePolicy;

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

/// Which file names a transfer is allowed to carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum Names {
    /// Only names every platform can write. A folder received here still opens on Windows.
    Portable,
    /// Also names that are ordinary on Unix and impossible on Windows, such as `?` or a
    /// trailing dot. Both sides have to allow it, and neither side may be on Windows.
    Posix,
}

impl From<Names> for NamePolicy {
    fn from(names: Names) -> Self {
        match names {
            Names::Portable => Self::Portable,
            Names::Posix => Self::Posix,
        }
    }
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
#[command(after_help = SEND_EXAMPLES)]
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

    /// The pairing code the receiver is showing. Proves you reached that machine and not
    /// something answering in its place.
    #[arg(long, value_name = "CODE", conflicts_with = "insecure")]
    pub(crate) code: Option<String>,

    /// Accept any peer without proving who it is. Traffic stays encrypted; nothing proves
    /// the receiver is the machine you meant.
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

    /// Hold the transfer to this many MiB per second, leaving the link usable for
    /// everything else on it. Unlimited when not given.
    #[arg(long, value_name = "MIB")]
    pub(crate) limit_rate_mb: Option<u64>,

    /// Which names may be sent. `posix` also offers names Windows forbids, such as `?`,
    /// and only if the receiver says it can write them too.
    #[arg(long, value_enum, default_value_t = Names::Portable, value_name = "POLICY")]
    pub(crate) names: Names,

    /// Print what the transfer cost when it ends: bytes, compression, throughput, time.
    #[arg(long)]
    pub(crate) stats: bool,

    /// Do not draw a progress bar.
    #[arg(long)]
    pub(crate) no_progress: bool,
}

/// Worked examples, shown under `spd send --help`.
///
/// The flags are documented one by one above; what a reader usually wants is the shape of
/// a whole command, and that is what is missing from a list of switches.
const SEND_EXAMPLES: &str = "\
Examples:
  # The receiver shows a code; type it here.
  spd send ./photos 192.168.1.20:9432 --code A1B2C-D3E4F

  # See what would move, without moving it.
  spd send ./photos 192.168.1.20:9432 --code A1B2C-D3E4F --dry-run

  # Leave the link usable for everything else, and say what it cost.
  spd send ./backup 192.168.1.20:9432 --code A1B2C-D3E4F --limit-rate-mb 20 --stats

  # A trusted network, no code, and content compared by hash rather than timestamp.
  spd send ./photos 192.168.1.20:9432 --insecure --checksum";

/// Worked examples, shown under `spd recv --help`.
const RECV_EXAMPLES: &str = "\
Examples:
  # Show a pairing code and wait for one transfer.
  spd recv --out ./inbox

  # A code agreed in advance, on a specific address.
  spd recv --out ./inbox --listen 0.0.0.0:9432 --code A1B2C-D3E4F

  # A trusted network, with a summary at the end.
  spd recv --out ./inbox --insecure --stats";

/// Options for `spd recv`.
#[derive(Debug, clap::Args)]
#[command(after_help = RECV_EXAMPLES)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "command-line switches are booleans; grouping them would only hide the surface"
)]
pub(crate) struct RecvArgs {
    /// Directory to write into. It must already exist.
    #[arg(long, short, default_value = ".", value_name = "DIR")]
    pub(crate) out: PathBuf,

    /// Address to listen on.
    #[arg(long, default_value_t = default_listen_address(), value_name = "ADDRESS")]
    pub(crate) listen: SocketAddr,

    /// Pairing code to expect, instead of showing a fresh one. Useful when the code has
    /// to be arranged in advance.
    #[arg(long, value_name = "CODE", conflicts_with = "insecure")]
    pub(crate) code: Option<String>,

    /// Accept any peer without proving who it is. Traffic stays encrypted; nothing proves
    /// the sender is who you expect.
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

    /// Which names may be written here. `posix` also accepts names Windows forbids, such
    /// as `?`; the sender is told, and holds back those files if it is not set on both
    /// sides.
    #[arg(long, value_enum, default_value_t = Names::Portable, value_name = "POLICY")]
    pub(crate) names: Names,

    /// How many sessions to accept before giving up, counting the first. A sender that
    /// reconnects finds the listener still waiting and continues where it stopped.
    #[arg(long, default_value_t = RetryPolicy::DEFAULT.attempts.get(), value_name = "N")]
    pub(crate) attempts: u32,

    /// Print what the transfer cost when it ends: bytes, compression, throughput, time.
    #[arg(long)]
    pub(crate) stats: bool,

    /// Do not draw a progress bar.
    #[arg(long)]
    pub(crate) no_progress: bool,
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
