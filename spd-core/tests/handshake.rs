//! Two peers meeting over loopback QUIC.
//!
//! Exercises the F1 acceptance condition: a real connection, a real handshake, and
//! control messages crossing in both directions afterwards.

use std::net::{Ipv4Addr, SocketAddr};

use spd_core::proto::codec::ProtoError;
use spd_core::proto::messages::{Control, DeviceId, ErrorCode};
use spd_core::proto::version::{Features, PROTOCOL_VERSION};
use spd_core::safety::limits::Limits;
use spd_core::transport::{Authentication, connect, listen};

fn loopback() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}

#[tokio::test]
async fn peers_negotiate_and_then_talk_over_the_control_stream() {
    let listener_device = DeviceId::random().unwrap();
    let caller_device = DeviceId::random().unwrap();

    let listener = listen(
        loopback(),
        listener_device,
        Authentication::Insecure,
        Limits::DEFAULT,
    )
    .unwrap();
    let address = listener.local_addr().unwrap();

    let accepting = tokio::spawn(async move {
        let mut session = listener.accept().await.unwrap();

        let first = session.control().recv().await.unwrap();
        session
            .control()
            .send(&Control::Done {
                files: 1,
                bytes: 42,
            })
            .await
            .unwrap();

        // Stay on the line until the caller hangs up. Dropping the session here would
        // close the connection and discard the answer before it is read - and waiting
        // for the hangup is also how the listener learns the session is over.
        let hangup = session.control().recv().await.unwrap_err();

        (
            session.peer().device,
            session.peer().negotiated,
            first,
            hangup,
        )
    });

    let mut caller = connect(
        address,
        caller_device,
        &Authentication::Insecure,
        Limits::DEFAULT,
    )
    .await
    .unwrap();

    // Each side learns who the other is, and both agree on version and features.
    assert_eq!(caller.peer().device, listener_device);
    assert_eq!(caller.peer().negotiated.version, PROTOCOL_VERSION);
    assert_eq!(
        caller.peer().negotiated.features,
        Features::announced(false),
        "unpaired peers agree on everything except pairing"
    );

    caller
        .control()
        .send(&Control::Error {
            code: ErrorCode::Internal,
            msg: "ping".to_owned(),
        })
        .await
        .unwrap();

    let answer = caller.control().recv().await.unwrap();
    assert_eq!(
        answer,
        Control::Done {
            files: 1,
            bytes: 42
        }
    );

    caller.close("done");

    let (seen_device, negotiated, first, hangup) = accepting.await.unwrap();
    assert_eq!(seen_device, caller_device);
    assert_eq!(negotiated.features, Features::announced(false));
    assert_eq!(
        first,
        Control::Error {
            code: ErrorCode::Internal,
            msg: "ping".to_owned()
        }
    );
    assert!(
        matches!(hangup, ProtoError::PeerClosed | ProtoError::Io(_)),
        "a hangup should surface as an end of stream, got {hangup:?}"
    );
}

#[tokio::test]
async fn a_data_stream_crosses_and_ends_where_the_sender_ended_it() {
    let listener = listen(
        loopback(),
        DeviceId::random().unwrap(),
        Authentication::Insecure,
        Limits::DEFAULT,
    )
    .unwrap();
    let address = listener.local_addr().unwrap();

    let accepting = tokio::spawn(async move {
        let session = listener.accept().await.unwrap();
        let mut stream = session.accept_data_stream().await.unwrap();
        stream.read_to_end(64).await.unwrap()
    });

    let caller = connect(
        address,
        DeviceId::random().unwrap(),
        &Authentication::Insecure,
        Limits::DEFAULT,
    )
    .await
    .unwrap();

    let mut stream = caller.open_data_stream().await.unwrap();
    stream.write_all(b"raw file bytes").await.unwrap();
    stream.finish().unwrap();

    assert_eq!(accepting.await.unwrap(), b"raw file bytes");
    caller.close("done");
}
