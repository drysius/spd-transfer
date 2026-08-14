//! `spd send` - offer a file or a folder to a waiting peer.

use std::num::NonZeroU32;

use anyhow::{Context, Result};
use spd_core::pipeline::budget::JobLimits;
use spd_core::pipeline::retry::{RetryPolicy, send_tree_reconnecting};
use spd_core::pipeline::send::SendOptions;
use spd_core::proto::messages::DeviceId;
use spd_core::safety::limits::Limits;

use crate::args::SendArgs;
use crate::{trust, ui};

/// Connects, offers the tree, sends whatever the receiver asks for.
///
/// # Errors
/// Fails if the peer is unreachable, the tree cannot be read, or the receiver reports that
/// a hash did not match.
pub(crate) async fn run(args: &SendArgs) -> Result<()> {
    let policy = trust::policy(args.insecure)?;
    let device = DeviceId::random()?;

    let streams = NonZeroU32::new(args.streams).context("--streams must be at least 1")?;
    let disk_read_jobs =
        NonZeroU32::new(args.disk_read_jobs).context("--disk-read-jobs must be at least 1")?;

    let limits = Limits {
        max_concurrent_streams: streams.get(),
        ..Limits::DEFAULT
    };

    let options = SendOptions {
        follow_links: args.follow_links,
        checksum: args.checksum,
        dry_run: args.dry_run,
        mem_budget_bytes: args.mem_budget_mb.saturating_mul(1024 * 1024),
        jobs: JobLimits {
            streams,
            disk_read_jobs,
            ..JobLimits::DEFAULT
        },
    };

    let retry = RetryPolicy {
        attempts: NonZeroU32::new(args.attempts).context("--attempts must be at least 1")?,
        ..RetryPolicy::DEFAULT
    };

    ui::section("sending");
    ui::field("to", &args.address.to_string());
    ui::field("device", &device.to_string());
    ui::field("path", &args.file.display().to_string());

    let report = send_tree_reconnecting(
        args.address,
        device,
        policy,
        &args.file,
        options,
        &limits,
        retry,
    )
    .await
    .with_context(|| format!("could not send {}", args.file.display()))?;

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
    ui::field("skipped", &report.skipped.to_string());

    Ok(())
}
