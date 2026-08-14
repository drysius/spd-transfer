//! `spd recv` - wait for one peer and write what it sends.

use core::num::NonZeroU32;
use std::time::Instant;

use anyhow::{Context, Result};
use spd_core::metrics::Progress;
use spd_core::pipeline::budget::JobLimits;
use spd_core::pipeline::recv::ReceiveOptions;
use spd_core::pipeline::retry::{RetryPolicy, receive_tree_resuming};
use spd_core::proto::messages::DeviceId;
use spd_core::safety::limits::Limits;
use spd_core::transport::listen;

use crate::args::RecvArgs;
use crate::progress::Bar;
use crate::{auth, stats, ui};

/// Listens, accepts one peer, receives what it offers.
///
/// # Errors
/// Fails if the address cannot be bound, the destination is not writable, or arrived bytes
/// do not match the sender's hash.
pub(crate) async fn run(args: &RecvArgs) -> Result<()> {
    // Resolved here rather than in the transport: the code is shown to the person sitting
    // at this machine, and a warning belongs where they can still stop.
    let authentication = auth::for_receiver(args.code.as_deref(), args.insecure)?;

    let device = DeviceId::random()?;
    let streams = NonZeroU32::new(args.streams).context("--streams must be at least 1")?;
    let disk_write_jobs =
        NonZeroU32::new(args.disk_write_jobs).context("--disk-write-jobs must be at least 1")?;
    let cpu_jobs = NonZeroU32::new(args.cpu_jobs).context("--cpu-jobs must be at least 1")?;

    let limits = Limits {
        max_concurrent_streams: streams.get(),
        ..Limits::DEFAULT
    };

    let progress = Progress::new();
    let options = ReceiveOptions {
        checksum: args.checksum,
        mem_budget_bytes: args.mem_budget_mb.saturating_mul(1024 * 1024),
        jobs: JobLimits {
            streams,
            disk_write_jobs,
            cpu_jobs,
            ..JobLimits::DEFAULT
        },
        progress: progress.clone(),
    };

    let destination = args
        .out
        .canonicalize()
        .with_context(|| format!("destination {} is unavailable", args.out.display()))?;

    let listener = listen(args.listen, device, authentication, limits)
        .with_context(|| format!("could not listen on {}", args.listen))?;

    ui::section("listening");
    ui::field("address", &listener.local_addr()?.to_string());
    ui::field("device", &device.to_string());
    ui::field("destination", &destination.display().to_string());

    let retry = RetryPolicy {
        attempts: NonZeroU32::new(args.attempts).context("--attempts must be at least 1")?,
        ..RetryPolicy::DEFAULT
    };

    // Started before the wait for a peer, so the bar is already there when bytes are.
    let bar = Bar::start(&progress, !args.no_progress);
    let started = Instant::now();

    let summary = receive_tree_resuming(&listener, &destination, options, &limits, retry).await;

    bar.stop();
    let elapsed = started.elapsed();
    let summary = summary.context("the transfer failed")?;

    ui::section("received");
    ui::field("files", &summary.files.to_string());
    ui::field("bytes", &ui::format_bytes(summary.bytes));
    if summary.wire_bytes != summary.bytes {
        ui::field("on the wire", &ui::format_bytes(summary.wire_bytes));
    }

    if args.stats {
        stats::print(&progress.snapshot(), elapsed);
    }

    Ok(())
}
