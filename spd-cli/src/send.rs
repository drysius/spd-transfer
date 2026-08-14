//! `spd send` - offer a file or a folder to a waiting peer.

use core::num::{NonZeroU32, NonZeroU64};
use std::time::Instant;

use anyhow::{Context, Result};
use spd_core::metrics::Progress;
use spd_core::pipeline::budget::JobLimits;
use spd_core::pipeline::rate::RateLimit;
use spd_core::pipeline::retry::{RetryPolicy, send_tree_reconnecting};
use spd_core::pipeline::send::SendOptions;
use spd_core::proto::messages::DeviceId;
use spd_core::safety::limits::Limits;

use crate::args::SendArgs;
use crate::progress::Bar;
use crate::{auth, stats, ui};

/// Connects, offers the tree, sends whatever the receiver asks for.
///
/// # Errors
/// Fails if the peer is unreachable, the tree cannot be read, the pairing code does not
/// match, or the receiver reports that a hash did not match.
pub(crate) async fn run(args: &SendArgs) -> Result<()> {
    let authentication = auth::for_sender(args.code.as_deref(), args.insecure)?;
    let device = DeviceId::random()?;

    let streams = NonZeroU32::new(args.streams).context("--streams must be at least 1")?;
    let disk_read_jobs =
        NonZeroU32::new(args.disk_read_jobs).context("--disk-read-jobs must be at least 1")?;
    let cpu_jobs = NonZeroU32::new(args.cpu_jobs).context("--cpu-jobs must be at least 1")?;

    let limits = Limits {
        max_concurrent_streams: streams.get(),
        ..Limits::DEFAULT
    };

    let progress = Progress::new();
    let options = SendOptions {
        follow_links: args.follow_links,
        checksum: args.checksum,
        dry_run: args.dry_run,
        compress: !args.no_compress,
        mem_budget_bytes: args.mem_budget_mb.saturating_mul(1024 * 1024),
        jobs: JobLimits {
            streams,
            disk_read_jobs,
            cpu_jobs,
            ..JobLimits::DEFAULT
        },
        rate: rate_limit(args.limit_rate_mb)?,
        progress: progress.clone(),
    };

    let retry = RetryPolicy {
        attempts: NonZeroU32::new(args.attempts).context("--attempts must be at least 1")?,
        ..RetryPolicy::DEFAULT
    };

    ui::section("sending");
    ui::field("to", &args.address.to_string());
    ui::field("device", &device.to_string());
    ui::field("path", &args.file.display().to_string());

    // A dry run negotiates and stops; a bar for it would flash once and vanish.
    let bar = Bar::start(&progress, !args.no_progress && !args.dry_run);
    let started = Instant::now();

    let report = send_tree_reconnecting(
        args.address,
        device,
        &authentication,
        &args.file,
        options,
        &limits,
        retry,
    )
    .await;

    bar.stop();
    let elapsed = started.elapsed();
    let report = report.with_context(|| format!("could not send {}", args.file.display()))?;

    if args.dry_run {
        ui::section("dry run");
        ui::field("would send", &report.planned.files.to_string());
        ui::field("would move", &ui::format_bytes(report.planned.bytes));
        ui::field("already there", &report.skipped.to_string());
        return Ok(());
    }

    ui::section("sent");
    ui::field("files", &report.transferred.files.to_string());
    ui::field("bytes", &ui::format_bytes(report.transferred.bytes));
    if report.transferred.wire_bytes != report.transferred.bytes {
        ui::field(
            "on the wire",
            &ui::format_bytes(report.transferred.wire_bytes),
        );
    }
    ui::field("skipped", &report.skipped.to_string());

    if args.stats {
        stats::print(&progress.snapshot(), elapsed);
    }

    report_unportable(&report.unportable);

    Ok(())
}

/// How many unsendable names are listed before the rest become a count.
///
/// Enough to recognise a pattern - one plugin's temporary directory, one bad export - and
/// few enough not to bury the summary above them.
const NAMES_SHOWN: usize = 10;

/// Says which files were never offered, and why.
///
/// Printed after the summary rather than logged during the scan: a warning that scrolled
/// past twenty thousand files ago is a warning nobody saw.
fn report_unportable(unportable: &[spd_core::scan::walk::Unportable]) {
    if unportable.is_empty() {
        return;
    }

    ui::section("not sent");
    for skipped in unportable.iter().take(NAMES_SHOWN) {
        ui::field(
            &skipped.path.display().to_string(),
            &skipped.reason.to_string(),
        );
    }

    if let Some(rest) = unportable.len().checked_sub(NAMES_SHOWN).filter(|n| *n > 0) {
        ui::field("and", &format!("{rest} more"));
    }

    eprintln!();
    eprintln!(
        "warning: {} file(s) were not sent. Their names are usable here and cannot be",
        unportable.len()
    );
    eprintln!("         written on Windows, so the receiver was never offered them.");
}

/// Turns `--limit-rate-mb` into a rate, refusing a limit of zero.
///
/// Zero would mean "never send anything", which nobody means and which would look like a
/// hang rather than a mistake.
fn rate_limit(mib_per_second: Option<u64>) -> Result<RateLimit> {
    let Some(mib) = mib_per_second else {
        return Ok(RateLimit::UNLIMITED);
    };

    let bytes = NonZeroU64::new(mib.saturating_mul(1024 * 1024))
        .context("--limit-rate-mb must be at least 1")?;

    Ok(RateLimit::bytes_per_second(bytes))
}
