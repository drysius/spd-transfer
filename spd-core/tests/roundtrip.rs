//! One file crossing the wire, verified end to end.
//!
//! Covers the F2 acceptance conditions: bytes arrive identical, a corrupted body is
//! detected rather than committed, and a hostile path is refused before anything is
//! written.

mod common;

use common::{Scratch, bound_listener, dial, pattern};
use spd_core::pipeline::PipelineError;
use spd_core::pipeline::recv::receive_file;
use spd_core::pipeline::send::send_file;
use spd_core::proto::codec::write_data_header;
use spd_core::proto::messages::{Control, DataHeader, Decision, Entry, FileId};
use spd_core::safety::limits::Limits;

#[tokio::test]
async fn a_file_arrives_byte_identical() {
    let source = Scratch::new("send");
    let destination = Scratch::new("recv");
    let contents = pattern(3 * 1024 * 1024 + 17);
    let file = source.write("payload.bin", &contents);

    let limits = Limits::DEFAULT;
    let (listener, address) = bound_listener(limits);
    let destination_path = destination.path().to_path_buf();

    let receiving = tokio::spawn(async move {
        let mut session = listener.accept().await.unwrap();
        receive_file(&mut session, &destination_path, &limits)
            .await
            .unwrap()
    });

    let mut sender = dial(address, limits).await;
    let sent = send_file(&mut sender, &file, &limits).await.unwrap();
    let received = receiving.await.unwrap();

    assert_eq!(sent.files, 1);
    assert_eq!(sent.bytes, contents.len() as u64);
    assert_eq!(received.bytes, sent.bytes);

    let arrived = std::fs::read(destination.path().join("payload.bin")).unwrap();
    assert_eq!(arrived, contents, "the file should arrive unchanged");

    assert!(
        !destination.path().join("payload.bin.part").exists(),
        "the partial file should be renamed, not left behind"
    );
}

/// A sender whose bytes do not match the hash it announces stands in for a corrupted
/// transfer: the receiver must refuse it and leave nothing behind.
#[tokio::test]
async fn corrupted_bytes_are_detected_and_nothing_is_committed() {
    let destination = Scratch::new("corrupt");
    let limits = Limits::DEFAULT;
    let (listener, address) = bound_listener(limits);
    let destination_path = destination.path().to_path_buf();

    let receiving = tokio::spawn(async move {
        let mut session = listener.accept().await.unwrap();
        receive_file(&mut session, &destination_path, &limits)
            .await
            .unwrap_err()
    });

    let mut sender = dial(address, limits).await;
    let file_id = FileId(0);

    sender
        .control()
        .send(&Control::Manifest {
            batch_seq: 0,
            last: true,
            entries: vec![Entry {
                file_id,
                path: vec!["tampered.bin".to_owned()],
                size: 4,
                mtime: 0,
                mode: 0,
                hash: None,
            }],
        })
        .await
        .unwrap();

    let reply = sender.control().recv().await.unwrap();
    assert!(matches!(
        reply,
        Control::SyncReply { ref decisions, .. } if matches!(decisions.first(), Some(Decision::Need { .. }))
    ));

    let mut stream = sender.open_data_stream().await.unwrap();
    write_data_header(
        &mut stream,
        &DataHeader {
            file_id,
            offset: 0,
            compressed: false,
        },
    )
    .await
    .unwrap();
    stream.write_all(b"real").await.unwrap();
    stream.finish().unwrap();

    // Announce a hash of different bytes: exactly what a flipped bit on the wire or a bad
    // disk would produce.
    let lie = *blake3::hash(b"fake").as_bytes();
    sender
        .control()
        .send(&Control::FileDone { file_id, hash: lie })
        .await
        .unwrap();

    let verdict = sender.control().recv().await.unwrap();
    assert_eq!(verdict, Control::FileVerdict { file_id, ok: false });

    let failure = receiving.await.unwrap();
    assert!(
        matches!(failure, PipelineError::HashMismatch { .. }),
        "expected a hash mismatch, got {failure:?}"
    );

    assert!(
        !destination.path().join("tampered.bin").exists(),
        "a failed transfer must not publish a file"
    );
    assert!(
        !destination.path().join("tampered.bin.part").exists(),
        "the partial file should be removed once it is known to be wrong"
    );
}

#[tokio::test]
async fn a_traversing_path_is_refused_before_anything_is_written() {
    let destination = Scratch::new("traversal");
    let limits = Limits::DEFAULT;
    let (listener, address) = bound_listener(limits);
    let destination_path = destination.path().to_path_buf();

    let receiving = tokio::spawn(async move {
        let mut session = listener.accept().await.unwrap();
        receive_file(&mut session, &destination_path, &limits)
            .await
            .unwrap_err()
    });

    let mut sender = dial(address, limits).await;
    sender
        .control()
        .send(&Control::Manifest {
            batch_seq: 0,
            last: true,
            entries: vec![Entry {
                file_id: FileId(0),
                path: vec!["..".to_owned(), "escaped.txt".to_owned()],
                size: 1,
                mtime: 0,
                mode: 0,
                hash: None,
            }],
        })
        .await
        .unwrap();

    let failure = receiving.await.unwrap();
    assert!(
        matches!(failure, PipelineError::Path(_)),
        "expected the path to be refused, got {failure:?}"
    );

    let parent = destination.path().parent().unwrap().join("escaped.txt");
    assert!(
        !parent.exists(),
        "nothing may be written outside the destination"
    );
}
