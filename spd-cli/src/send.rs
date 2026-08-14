//! `spd send` - offer a file to a waiting peer.

use anyhow::{Context, Result};
use spd_core::pipeline::send::send_file;
use spd_core::proto::messages::DeviceId;
use spd_core::safety::limits::Limits;
use spd_core::transport::connect;

use crate::args::SendArgs;
use crate::{trust, ui};

/// Connects, sends one file, and reports what crossed.
///
/// # Errors
/// Fails if the peer is unreachable, the file cannot be read, or the receiver reports that
/// the hash did not match.
pub(crate) async fn run(args: &SendArgs) -> Result<()> {
    let policy = trust::policy(args.insecure)?;
    let limits = Limits::DEFAULT;
    let device = DeviceId::random()?;

    let mut session = connect(args.address, device, policy, limits)
        .await
        .with_context(|| format!("could not reach a receiver at {}", args.address))?;

    ui::section("connected");
    ui::field("peer", &session.peer().device.to_string());
    ui::field("address", &session.peer().address.to_string());

    let summary = send_file(&mut session, &args.file, &limits)
        .await
        .with_context(|| format!("could not send {}", args.file.display()))?;

    session.close_gracefully("transfer complete").await;

    ui::section("sent");
    if summary.files == 0 {
        ui::field("result", "the receiver already had this file");
    } else {
        ui::field("files", &summary.files.to_string());
        ui::field("bytes", &ui::format_bytes(summary.bytes));
    }

    Ok(())
}
