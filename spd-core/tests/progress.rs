//! What the counters say while a transfer runs, and after it.
//!
//! A progress bar is only worth drawing if what it draws is true. These are the two claims
//! it rests on: the counters end up agreeing with the summary, and both sides count the
//! same transfer.

mod common;

use common::{Scratch, bound_listener, dial, pattern};
use spd_core::metrics::Progress;
use spd_core::pipeline::recv::{ReceiveOptions, receive_tree};
use spd_core::pipeline::send::{SendOptions, send_tree};
use spd_core::safety::limits::Limits;

const FILE_BYTES: usize = 64 * 1024;
const FILES: usize = 5;

#[tokio::test]
async fn the_counters_end_up_agreeing_with_the_summary() {
    let source = Scratch::new("progress-source");
    let destination = Scratch::new("progress-dest");
    for index in 0..FILES {
        source.write(&format!("file-{index}.bin"), &pattern(FILE_BYTES));
    }

    let limits = Limits::DEFAULT;
    let (listener, address) = bound_listener(limits);

    let receiving_progress = Progress::new();
    let seen_by_receiver = receiving_progress.clone();
    let into = destination.path().to_path_buf();

    let receiving = tokio::spawn(async move {
        let session = listener.accept().await.unwrap();
        receive_tree(
            session,
            &into,
            ReceiveOptions {
                progress: seen_by_receiver,
                ..ReceiveOptions::default()
            },
            &limits,
        )
        .await
        .unwrap()
    });

    let sending_progress = Progress::new();
    let sender = dial(address, limits).await;
    let report = send_tree(
        sender,
        source.path(),
        SendOptions {
            progress: sending_progress.clone(),
            ..SendOptions::default()
        },
        &limits,
    )
    .await
    .unwrap();

    let received = receiving.await.unwrap();

    let sent = sending_progress.snapshot();
    assert_eq!(sent.files_total, FILES as u64);
    assert_eq!(sent.bytes_total, (FILES * FILE_BYTES) as u64);
    assert_eq!(sent.files_done, report.transferred.files);
    assert_eq!(sent.bytes_done, report.transferred.bytes);
    assert_eq!(sent.wire_bytes, report.transferred.wire_bytes);

    let got = receiving_progress.snapshot();
    assert_eq!(got.files_total, FILES as u64, "the sender announced these");
    assert_eq!(got.files_done, received.files);
    assert_eq!(got.bytes_done, received.bytes);
    assert_eq!(got.wire_bytes, received.wire_bytes);
    assert_eq!(
        got.scan_millis, 0,
        "only the sending side scans a tree, so only it has a scan to time"
    );
}

#[tokio::test]
async fn a_transfer_nobody_is_watching_still_counts() {
    let source = Scratch::new("progress-ignored-source");
    let destination = Scratch::new("progress-ignored-dest");
    source.write("only.bin", &pattern(FILE_BYTES));

    let limits = Limits::DEFAULT;
    let (listener, address) = bound_listener(limits);
    let into = destination.path().to_path_buf();

    let receiving = tokio::spawn(async move {
        let session = listener.accept().await.unwrap();
        receive_tree(session, &into, ReceiveOptions::default(), &limits)
            .await
            .unwrap()
    });

    let sender = dial(address, limits).await;
    let report = send_tree(sender, source.path(), SendOptions::default(), &limits)
        .await
        .unwrap();

    assert_eq!(report.transferred.files, 1);
    assert_eq!(receiving.await.unwrap().files, 1);
}
