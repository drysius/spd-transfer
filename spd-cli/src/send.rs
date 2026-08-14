//! `spd send` - offer a file or a folder to a waiting peer.

use anyhow::{Context, Result};
use spd_core::pipeline::send::{SendOptions, send_tree};
use spd_core::proto::messages::DeviceId;
use spd_core::safety::limits::Limits;
use spd_core::transport::connect;

use crate::args::SendArgs;
use crate::{trust, ui};

/// Connects, offers the tree, sends whatever the receiver asks for.
///
/// # Errors
/// Fails if the peer is unreachable, the tree cannot be read, or the receiver reports that
/// a hash did not match.
pub(crate) async fn run(args: &SendArgs) -> Result<()> {
    let policy = trust::policy(args.insecure)?;
    let limits = Limits::DEFAULT;
    let device = DeviceId::random()?;

    let options = SendOptions {
        follow_links: args.follow_links,
        checksum: args.checksum,
        dry_run: args.dry_run,
    };

    let mut session = connect(args.address, device, policy, limits)
        .await
        .with_context(|| format!("could not reach a receiver at {}", args.address))?;

    ui::section("connected");
    ui::field("peer", &session.peer().device.to_string());
    ui::field("address", &session.peer().address.to_string());

    let report = send_tree(&mut session, &args.file, options, &limits)
        .await
        .with_context(|| format!("could not send {}", args.file.display()))?;

    session.close_gracefully("transfer complete").await;

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
