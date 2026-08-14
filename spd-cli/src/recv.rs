//! `spd recv` - wait for one peer and write what it sends.

use std::num::NonZeroU32;

use anyhow::{Context, Result};
use spd_core::pipeline::budget::JobLimits;
use spd_core::pipeline::recv::{ReceiveOptions, receive_tree};
use spd_core::proto::messages::DeviceId;
use spd_core::safety::limits::Limits;
use spd_core::transport::listen;

use crate::args::RecvArgs;
use crate::{trust, ui};

/// Listens, accepts one peer, receives what it offers.
///
/// # Errors
/// Fails if the address cannot be bound, the destination is not writable, or arrived bytes
/// do not match the sender's hash.
pub(crate) async fn run(args: &RecvArgs) -> Result<()> {
    // Announced here rather than in the transport: the policy applies to the peer this
    // process is about to accept, and the warning belongs where the user can still stop.
    let _policy = trust::policy(args.insecure)?;

    let device = DeviceId::random()?;
    let streams = NonZeroU32::new(args.streams).context("--streams must be at least 1")?;
    let disk_write_jobs =
        NonZeroU32::new(args.disk_write_jobs).context("--disk-write-jobs must be at least 1")?;

    let limits = Limits {
        max_concurrent_streams: streams.get(),
        ..Limits::DEFAULT
    };

    let options = ReceiveOptions {
        checksum: args.checksum,
        mem_budget_bytes: args.mem_budget_mb.saturating_mul(1024 * 1024),
        jobs: JobLimits {
            streams,
            disk_write_jobs,
            ..JobLimits::DEFAULT
        },
    };

    let destination = args
        .out
        .canonicalize()
        .with_context(|| format!("destination {} is unavailable", args.out.display()))?;

    let listener = listen(args.listen, device, limits)
        .with_context(|| format!("could not listen on {}", args.listen))?;

    ui::section("listening");
    ui::field("address", &listener.local_addr()?.to_string());
    ui::field("device", &device.to_string());
    ui::field("destination", &destination.display().to_string());

    let session = listener
        .accept()
        .await
        .context("no peer completed a session")?;

    ui::section("connected");
    ui::field("peer", &session.peer().device.to_string());
    ui::field("address", &session.peer().address.to_string());

    let summary = receive_tree(session, &destination, options, &limits)
        .await
        .context("the transfer failed")?;

    ui::section("received");
    ui::field("files", &summary.files.to_string());
    ui::field("bytes", &ui::format_bytes(summary.bytes));

    Ok(())
}
