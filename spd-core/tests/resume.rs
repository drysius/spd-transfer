//! Interrupting a transfer and picking it up where it stopped.
//!
//! Covers the F5 acceptance condition: a connection cut part way through leaves a tree that
//! the next run completes, byte for byte. It is cut for real once, and then resumed from a
//! hundred different offsets in turn, because a resume that works somewhere comfortable in
//! the middle says nothing about the first block or the last one.

mod common;

use std::path::Path;
use std::time::{Duration, UNIX_EPOCH};

use common::{Scratch, bound_listener, dial, pattern};
use spd_core::pipeline::recv::{ReceiveOptions, receive_tree};
use spd_core::pipeline::send::{SendOptions, SendReport, send_tree};
use spd_core::safety::limits::Limits;
use spd_core::safety::path::SafeRelPath;
use spd_core::state::model::Expected;
use spd_core::state::spawn_state;

/// The one file every test here moves.
const PAYLOAD: &str = "payload.bin";

/// Where the receiver puts a file that has not been verified yet.
const PARTIAL: &str = "payload.bin.part";

/// Long enough that a cut lands in the middle of it rather than after the last byte.
const BIG_ENOUGH_TO_INTERRUPT: usize = 32 * 1024 * 1024;

/// Small enough to run a session per offset without slowing the suite down.
const SMALL: usize = 128 * 1024;

/// How often the harness looks at the growing `.part` file while waiting to cut.
const POLL_INTERVAL: Duration = Duration::from_millis(1);

/// How long it waits before cutting anyway, so a broken transfer fails the assertion rather
/// than hanging the suite.
const POLL_TIMEOUT: Duration = Duration::from_secs(30);

/// Runs one full session and returns what the sender reported.
async fn transfer(source: &Path, destination: &Path) -> SendReport {
    let limits = Limits::DEFAULT;
    let (listener, address) = bound_listener(limits);
    let destination = destination.to_path_buf();

    let receiving = tokio::spawn(async move {
        let session = listener.accept().await.unwrap();
        receive_tree(session, &destination, ReceiveOptions::default(), &limits)
            .await
            .unwrap()
    });

    let sender = dial(address, limits).await;
    let report = send_tree(sender, source, SendOptions::default(), &limits)
        .await
        .unwrap();

    receiving.await.unwrap();
    report
}

/// Starts a session and drops the receiver once `cut_at` bytes have reached the disk.
///
/// Dropping it takes the connection with it, which is what a pulled cable looks like from
/// the sender's side: neither peer gets to finish tidily.
async fn transfer_until_cut(source: &Path, destination: &Path, cut_at: u64) {
    let limits = Limits::DEFAULT;
    let (listener, address) = bound_listener(limits);
    let into = destination.to_path_buf();

    let receiving = tokio::spawn(async move {
        let session = listener.accept().await.unwrap();
        // The error this returns is the cut itself, and the test is about what it leaves
        // on disk rather than about how it was reported.
        let _ = receive_tree(session, &into, ReceiveOptions::default(), &limits).await;
    });

    let source = source.to_path_buf();
    let sending = tokio::spawn(async move {
        let sender = dial(address, limits).await;
        let _ = send_tree(sender, &source, SendOptions::default(), &limits).await;
    });

    wait_for_partial(&destination.join(PARTIAL), cut_at).await;
    receiving.abort();
    let _ = sending.await;
}

/// Waits until the partial file holds at least `bytes`, or until the timeout runs out.
async fn wait_for_partial(partial: &Path, bytes: u64) {
    let deadline = tokio::time::Instant::now() + POLL_TIMEOUT;

    while tokio::time::Instant::now() < deadline {
        if std::fs::metadata(partial).is_ok_and(|found| found.len() >= bytes) {
            return;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Leaves the destination looking like an interrupted run: a `.part` file holding the first
/// `bytes` of the source, and the record saying what it was started for.
///
/// The record is written through the same state actor the receiver uses, so the test cannot
/// pass by writing a state file the real code would not accept.
async fn seed_interrupted(source_file: &Path, destination: &Path, bytes: usize) {
    let limits = Limits::DEFAULT;
    let contents = std::fs::read(source_file).unwrap();
    std::fs::write(destination.join(PARTIAL), &contents[..bytes]).unwrap();

    let (state, task) = spawn_state(destination, &limits).await.unwrap();
    state
        .started(
            SafeRelPath::from_components(&[PAYLOAD.to_owned()], &limits).unwrap(),
            expected_of(source_file),
        )
        .await
        .unwrap();
    drop(state);
    task.await.unwrap().unwrap();
}

/// What the sender will offer for this file: its size, its timestamp and its hash.
fn expected_of(source_file: &Path) -> Expected {
    let metadata = std::fs::metadata(source_file).unwrap();
    let mtime = metadata
        .modified()
        .unwrap()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    Expected {
        size: metadata.len(),
        mtime,
        hash: Some(*blake3::hash(&std::fs::read(source_file).unwrap()).as_bytes()),
    }
}

/// A source directory holding one file of `size` bytes.
fn source_tree(label: &str, size: usize) -> (Scratch, Vec<u8>) {
    let scratch = Scratch::new(label);
    let contents = pattern(size);
    scratch.write(PAYLOAD, &contents);
    (scratch, contents)
}

#[tokio::test]
async fn a_transfer_cut_in_the_middle_finishes_on_the_next_run() {
    let (source, contents) = source_tree("cut-source", BIG_ENOUGH_TO_INTERRUPT);
    let destination = Scratch::new("cut-dest");
    let cut_at = (BIG_ENOUGH_TO_INTERRUPT / 8) as u64;

    transfer_until_cut(source.path(), destination.path(), cut_at).await;

    let partial = destination.path().join(PARTIAL);
    let reached = std::fs::metadata(&partial).unwrap().len();
    assert!(
        reached >= cut_at,
        "the harness should have cut the connection mid-file, not before it started"
    );

    let second = transfer(source.path(), destination.path()).await;

    assert_eq!(
        std::fs::read(destination.path().join(PAYLOAD)).unwrap(),
        contents,
        "the finished file must be byte-identical to the source"
    );
    assert!(
        second.transferred.bytes < contents.len() as u64,
        "the second run should have sent only what was missing, not the whole file again"
    );
    assert!(
        !partial.exists(),
        "the partial file should be gone once it has been committed"
    );
}

#[tokio::test]
async fn resuming_moves_only_the_bytes_that_were_missing() {
    let (source, contents) = source_tree("offsets-source", SMALL);
    let size = contents.len();

    // Every hundredth of the file, so the first and last blocks are covered as well as the
    // comfortable middle.
    for step in 1..=100 {
        let already_there = size * step / 101;
        let destination = Scratch::new(&format!("offsets-dest-{step}"));

        seed_interrupted(
            &source.path().join(PAYLOAD),
            destination.path(),
            already_there,
        )
        .await;
        let report = transfer(source.path(), destination.path()).await;

        assert_eq!(
            report.transferred.bytes,
            (size - already_there) as u64,
            "a transfer resuming at byte {already_there} should move exactly what is missing"
        );
        assert_eq!(
            std::fs::read(destination.path().join(PAYLOAD)).unwrap(),
            contents,
            "the tree must be byte-identical after resuming at byte {already_there}"
        );
    }
}

#[tokio::test]
async fn a_partial_file_nobody_can_identify_is_not_resumed() {
    let (source, contents) = source_tree("orphan-source", SMALL);
    let destination = Scratch::new("orphan-dest");

    // A `.part` file with no record behind it: left by another tool, an older version, or a
    // run whose journal was deleted. Its bytes mean nothing here.
    std::fs::write(destination.path().join(PARTIAL), vec![0_u8; SMALL / 2]).unwrap();

    let report = transfer(source.path(), destination.path()).await;

    assert_eq!(
        report.transferred.bytes,
        contents.len() as u64,
        "an unidentifiable partial file must be overwritten from the first byte"
    );
    assert_eq!(
        std::fs::read(destination.path().join(PAYLOAD)).unwrap(),
        contents
    );
}
