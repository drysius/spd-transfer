//! Who is allowed to send anything at all.
//!
//! Covers the F7 acceptance condition that a peer which cannot prove it knows the code
//! writes nothing to disk - not a partial file, not an empty directory, not a state file.
//! The other half of that phase, that every traversal vector is rejected, lives in
//! `tests/path_safety.rs`.

mod common;

use std::net::{Ipv4Addr, SocketAddr};

use common::{Scratch, pattern};
use spd_core::pipeline::recv::{ReceiveOptions, receive_tree};
use spd_core::pipeline::send::{SendOptions, send_tree};
use spd_core::proto::messages::DeviceId;
use spd_core::safety::limits::Limits;
use spd_core::transport::{
    Authentication, Listener, PairingCode, PairingError, TransportError, connect, listen,
};

/// Binds a listener that will only accept peers proving `auth`.
fn bound(auth: Authentication) -> (Listener, SocketAddr) {
    let listener = listen(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        DeviceId::random().unwrap(),
        auth,
        Limits::DEFAULT,
    )
    .unwrap();

    let address = listener.local_addr().unwrap();
    (listener, address)
}

fn code(text: &str) -> Authentication {
    Authentication::Code(PairingCode::parse(text).unwrap())
}

/// A directory holding one file, to have something worth refusing.
fn source(label: &str) -> Scratch {
    let scratch = Scratch::new(label);
    scratch.write("secret.bin", &pattern(4096));
    scratch
}

#[tokio::test]
async fn the_right_code_lets_the_transfer_through() {
    let source = source("pair-ok-source");
    let destination = Scratch::new("pair-ok-dest");
    let (listener, address) = bound(code("A1B2C-D3E4F"));

    let into = destination.path().to_path_buf();
    let receiving = tokio::spawn(async move {
        let session = listener.accept().await.unwrap();
        receive_tree(session, &into, ReceiveOptions::default(), &Limits::DEFAULT)
            .await
            .unwrap()
    });

    let session = connect(
        address,
        DeviceId::random().unwrap(),
        // Typed the way a user would, rather than the way it was generated.
        &code("a1b2c d3e4f"),
        Limits::DEFAULT,
    )
    .await
    .unwrap();

    let report = send_tree(
        session,
        source.path(),
        SendOptions::default(),
        &Limits::DEFAULT,
    )
    .await
    .unwrap();

    assert_eq!(report.transferred.files, 1);
    assert_eq!(receiving.await.unwrap().files, 1);
    assert_eq!(
        std::fs::read(destination.path().join("secret.bin")).unwrap(),
        pattern(4096)
    );
}

#[tokio::test]
async fn the_wrong_code_writes_nothing_at_all() {
    let source = source("pair-wrong-source");
    let destination = Scratch::new("pair-wrong-dest");
    let (listener, address) = bound(code("A1B2C-D3E4F"));

    let accepting = tokio::spawn(async move { listener.accept().await.map(|_| ()) });

    let refused = connect(
        address,
        DeviceId::random().unwrap(),
        &code("Z9Y8X-W7V6T"),
        Limits::DEFAULT,
    )
    .await
    .unwrap_err();

    assert!(
        matches!(
            refused,
            TransportError::PeerRefused { .. } | TransportError::Pairing(PairingError::Mismatch)
        ),
        "the sender should learn the code did not match, got {refused:?}"
    );

    let listener_side = accepting.await.unwrap().unwrap_err();
    assert!(
        matches!(
            listener_side,
            TransportError::Pairing(PairingError::Mismatch)
        ),
        "the receiver should refuse by pairing, got {listener_side:?}"
    );

    assert!(
        std::fs::read_dir(destination.path())
            .unwrap()
            .next()
            .is_none(),
        "a peer that could not pair must not have created anything"
    );
    assert!(source.path().join("secret.bin").exists());
}

#[tokio::test]
async fn a_sender_with_a_code_refuses_a_receiver_that_asks_for_none() {
    let (listener, address) = bound(Authentication::Insecure);
    let accepting = tokio::spawn(async move { listener.accept().await.map(|_| ()) });

    let refused = connect(
        address,
        DeviceId::random().unwrap(),
        &code("A1B2C-D3E4F"),
        Limits::DEFAULT,
    )
    .await
    .unwrap_err();

    // Whichever side notices first: the receiver refuses before answering the hello, and
    // the sender refuses on the answer if it ever arrives. Either way nothing is agreed.
    assert!(
        matches!(
            refused,
            TransportError::PeerRefused { .. }
                | TransportError::Pairing(PairingError::Disagreement { .. })
        ),
        "being asked for no proof at all is a downgrade, not a convenience: {refused:?}"
    );

    // The listener's own outcome does not matter here - it may be waiting on a caller that
    // walked away - but it must not have produced a usable session.
    let listener_side = accepting.await.unwrap();
    assert!(listener_side.is_err());
}

#[tokio::test]
async fn a_receiver_with_a_code_refuses_a_sender_that_offers_none() {
    let (listener, address) = bound(code("A1B2C-D3E4F"));
    let accepting = tokio::spawn(async move { listener.accept().await.map(|_| ()) });

    let refused = connect(
        address,
        DeviceId::random().unwrap(),
        &Authentication::Insecure,
        Limits::DEFAULT,
    )
    .await
    .unwrap_err();

    assert!(
        matches!(
            refused,
            TransportError::PeerRefused { .. }
                | TransportError::Pairing(PairingError::Disagreement { .. })
        ),
        "got {refused:?}"
    );

    let listener_side = accepting.await.unwrap().unwrap_err();
    assert!(
        matches!(
            listener_side,
            TransportError::Pairing(PairingError::Disagreement { .. })
        ),
        "got {listener_side:?}"
    );
}

#[tokio::test]
async fn two_peers_with_no_code_still_talk() {
    let source = source("pair-insecure-source");
    let destination = Scratch::new("pair-insecure-dest");
    let (listener, address) = bound(Authentication::Insecure);

    let into = destination.path().to_path_buf();
    let receiving = tokio::spawn(async move {
        let session = listener.accept().await.unwrap();
        receive_tree(session, &into, ReceiveOptions::default(), &Limits::DEFAULT)
            .await
            .unwrap()
    });

    let session = connect(
        address,
        DeviceId::random().unwrap(),
        &Authentication::Insecure,
        Limits::DEFAULT,
    )
    .await
    .unwrap();

    send_tree(
        session,
        source.path(),
        SendOptions::default(),
        &Limits::DEFAULT,
    )
    .await
    .unwrap();

    assert_eq!(receiving.await.unwrap().files, 1);
}
