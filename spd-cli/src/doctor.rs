//! `spd doctor` - show what this machine would do, before it does anything.
//!
//! Runs the same limit validation and budget arithmetic a real transfer runs, then stops.
//! If `doctor` disagrees with a transfer, one of them is a bug.

use std::num::{NonZeroU32, NonZeroUsize};
use std::thread;

use anyhow::{Context, Result};
use spd_core::pipeline::budget::{self, DEFAULT_BUF_SIZE_BYTES, DEFAULT_PIPELINE_DEPTH};
use spd_core::proto::version::{Features, PROTOCOL_VERSION};
use spd_core::safety::limits::Limits;

use crate::args::DoctorArgs;
use crate::ui;

/// Runs the diagnostic and prints the report.
///
/// # Errors
/// Fails if the requested limits are inconsistent - for example `--streams 0`.
pub(crate) fn run(args: &DoctorArgs) -> Result<()> {
    let streams = NonZeroU32::new(args.streams)
        .context("--streams must be at least 1; use --mem-budget-mb to shrink memory instead")?;

    let limits = Limits {
        max_concurrent_streams: streams.get(),
        ..Limits::DEFAULT
    };
    limits
        .validate()
        .context("the configured limits are not usable")?;

    let mem_budget_bytes = args.mem_budget_mb.saturating_mul(1024 * 1024);
    let concurrency = budget::derive_concurrency_with_defaults(mem_budget_bytes, streams);

    // A machine that cannot report its parallelism is still usable; report 0 rather than
    // failing a diagnostic over it.
    let cores = thread::available_parallelism().map_or(0, NonZeroUsize::get);

    ui::section("protocol");
    ui::field("version", &PROTOCOL_VERSION.to_string());
    ui::field("features", &format!("{:#06b}", Features::SUPPORTED.bits()));

    ui::section("limits");
    ui::field(
        "max frame",
        &ui::format_bytes(limits.max_frame_len_bytes as u64),
    );
    ui::field("manifest batch", &limits.max_manifest_entries.to_string());
    ui::field("max path depth", &limits.max_path_depth.to_string());
    ui::field(
        "max path length",
        &ui::format_bytes(limits.max_path_len_bytes as u64),
    );
    ui::field(
        "handshake timeout",
        &format!("{:?}", limits.handshake_timeout),
    );
    ui::field("idle timeout", &format!("{:?}", limits.idle_timeout));

    ui::section("memory budget");
    ui::field("requested", &ui::format_bytes(mem_budget_bytes));
    ui::field("buffer size", &ui::format_bytes(DEFAULT_BUF_SIZE_BYTES));
    ui::field("pipeline depth", &DEFAULT_PIPELINE_DEPTH.to_string());
    ui::field("streams", &concurrency.streams.to_string());
    ui::field("reserved", &ui::format_bytes(concurrency.reserved_bytes));

    if concurrency.reserved_bytes > mem_budget_bytes {
        println!(
            "\nnote: the budget is smaller than one pipeline ({}); running with a single \
             stream, which needs that much",
            ui::format_bytes(concurrency.reserved_bytes)
        );
    }

    ui::section("machine");
    ui::field("logical cores", &cores.to_string());

    Ok(())
}
