//! `spd recv` - wait for one peer and write what it sends.

use anyhow::{Context, Result};
use spd_core::pipeline::recv::{ReceiveOptions, receive_tree};
use spd_core::proto::messages::DeviceId;
use spd_core::safety::limits::Limits;
use spd_core::transport::listen;

use crate::args::RecvArgs;
use crate::{trust, ui};

/// Listens, accepts one peer, receives one file.
///
/// # Errors
/// Fails if the address cannot be bound, the destination is not writable, or the arrived
/// bytes do not match the sender's hash.
pub(crate) async fn run(args: &RecvArgs) -> Result<()> {
    // Announced here rather than in the transport: the policy applies to the peer this
    // process is about to accept, and the warning belongs where the user can still stop.
    let _policy = trust::policy(args.insecure)?;

    let limits = Limits::DEFAULT;
    let device = DeviceId::random()?;

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

    let mut session = listener
        .accept()
        .await
        .context("no peer completed a session")?;

    ui::section("connected");
    ui::field("peer", &session.peer().device.to_string());
    ui::field("address", &session.peer().address.to_string());

    let options = ReceiveOptions {
        checksum: args.checksum,
    };

    let summary = receive_tree(&mut session, &destination, options, &limits)
        .await
        .context("the transfer failed")?;

    session.close("transfer complete");

    ui::section("received");
    ui::field("files", &summary.files.to_string());
    ui::field("bytes", &ui::format_bytes(summary.bytes));

    Ok(())
}
