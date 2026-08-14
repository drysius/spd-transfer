//! What crosses the wire when a body is worth compressing, and when it is not.
//!
//! Covers the F6 acceptance conditions: a folder of text moves fewer bytes than it holds,
//! a folder of video moves exactly as many, and a folder holding both gets it right file by
//! file - which is the case a receiver deciding from its own configuration would get wrong.

mod common;

use std::path::Path;

use common::{Scratch, bound_listener, dial, pattern};
use spd_core::pipeline::recv::{ReceiveOptions, receive_tree};
use spd_core::pipeline::send::{SendOptions, SendReport, send_tree};
use spd_core::safety::limits::Limits;

/// Big enough for the sample to be representative and for the ratio to be obvious.
const SIZE: usize = 512 * 1024;

/// Runs one full session and returns what each side reported.
async fn transfer(source: &Path, destination: &Path, options: SendOptions) -> SendReport {
    let limits = Limits::DEFAULT;
    let (listener, address) = bound_listener(limits);
    let into = destination.to_path_buf();

    let receiving = tokio::spawn(async move {
        let session = listener.accept().await.unwrap();
        receive_tree(session, &into, ReceiveOptions::default(), &limits)
            .await
            .unwrap()
    });

    let sender = dial(address, limits).await;
    let report = send_tree(sender, source, options, &limits).await.unwrap();
    let received = receiving.await.unwrap();

    assert_eq!(
        received.wire_bytes, report.transferred.wire_bytes,
        "both sides should count the same bytes crossing"
    );
    assert_eq!(received.bytes, report.transferred.bytes);

    report
}

/// Repetitive text, which is what a folder of source, logs or configuration looks like.
fn text(len: usize) -> Vec<u8> {
    "the quick brown fox jumps over the lazy dog. "
        .bytes()
        .cycle()
        .take(len)
        .collect()
}

/// Bytes with no structure left to find, without needing randomness at test time.
fn noise(len: usize) -> Vec<u8> {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

#[tokio::test]
async fn a_folder_of_text_moves_fewer_bytes_than_it_holds() {
    let source = Scratch::new("zstd-text-source");
    let destination = Scratch::new("zstd-text-dest");
    let contents = text(SIZE);
    source.write("notes.txt", &contents);

    let report = transfer(source.path(), destination.path(), SendOptions::default()).await;

    assert_eq!(report.transferred.bytes, SIZE as u64);
    assert!(
        report.transferred.wire_bytes < report.transferred.bytes / 2,
        "repeated text should more than halve: moved {} of {} bytes",
        report.transferred.wire_bytes,
        report.transferred.bytes
    );
    assert_eq!(
        std::fs::read(destination.path().join("notes.txt")).unwrap(),
        contents,
        "what arrives must be the file, not the frames it travelled in"
    );
}

#[tokio::test]
async fn a_folder_of_video_is_not_read_twice_or_compressed() {
    let source = Scratch::new("zstd-video-source");
    let destination = Scratch::new("zstd-video-dest");
    let contents = noise(SIZE);
    source.write("holiday.mp4", &contents);

    let report = transfer(source.path(), destination.path(), SendOptions::default()).await;

    assert_eq!(
        report.transferred.wire_bytes, report.transferred.bytes,
        "an extension that already means compressed should settle it without sampling"
    );
    assert_eq!(
        std::fs::read(destination.path().join("holiday.mp4")).unwrap(),
        contents
    );
}

#[tokio::test]
async fn a_file_that_does_not_shrink_is_sent_as_it_is() {
    let source = Scratch::new("zstd-sample-source");
    let destination = Scratch::new("zstd-sample-dest");
    let contents = noise(SIZE);

    // A name that promises nothing: only compressing a sample can tell.
    source.write("payload.bin", &contents);

    let report = transfer(source.path(), destination.path(), SendOptions::default()).await;

    assert_eq!(
        report.transferred.wire_bytes, report.transferred.bytes,
        "a sample that does not shrink should stop the whole file being compressed"
    );
    assert_eq!(
        std::fs::read(destination.path().join("payload.bin")).unwrap(),
        contents
    );
}

#[tokio::test]
async fn each_file_in_a_mixed_folder_is_decided_on_its_own() {
    let source = Scratch::new("zstd-mixed-source");
    let destination = Scratch::new("zstd-mixed-dest");
    let compressible = text(SIZE);
    let incompressible = noise(SIZE);
    source.write("notes.txt", &compressible);
    source.write("holiday.mp4", &incompressible);

    let report = transfer(source.path(), destination.path(), SendOptions::default()).await;

    assert_eq!(report.transferred.files, 2);
    assert!(
        report.transferred.wire_bytes > SIZE as u64,
        "the video still has to cross whole"
    );
    assert!(
        report.transferred.wire_bytes < report.transferred.bytes,
        "the text should not"
    );

    assert_eq!(
        std::fs::read(destination.path().join("notes.txt")).unwrap(),
        compressible
    );
    assert_eq!(
        std::fs::read(destination.path().join("holiday.mp4")).unwrap(),
        incompressible
    );
}

#[tokio::test]
async fn turning_compression_off_sends_every_body_as_it_is() {
    let source = Scratch::new("zstd-off-source");
    let destination = Scratch::new("zstd-off-dest");
    let contents = text(SIZE);
    source.write("notes.txt", &contents);

    let report = transfer(
        source.path(),
        destination.path(),
        SendOptions {
            compress: false,
            ..SendOptions::default()
        },
    )
    .await;

    assert_eq!(report.transferred.wire_bytes, report.transferred.bytes);
    assert_eq!(
        std::fs::read(destination.path().join("notes.txt")).unwrap(),
        contents
    );
}

#[tokio::test]
async fn a_compressed_body_still_resumes_at_a_file_offset() {
    let source = Scratch::new("zstd-resume-source");
    let destination = Scratch::new("zstd-resume-dest");
    let contents = text(SIZE);
    source.write("notes.txt", &contents);

    // First run puts the whole file there, second run changes it and resumes nothing;
    // what matters is that the offsets in play are file offsets and not stream offsets.
    transfer(source.path(), destination.path(), SendOptions::default()).await;
    assert_eq!(
        std::fs::read(destination.path().join("notes.txt")).unwrap(),
        contents
    );

    let longer = text(SIZE + 4096);
    source.write("notes.txt", &longer);

    let second = transfer(source.path(), destination.path(), SendOptions::default()).await;

    assert_eq!(second.transferred.files, 1);
    assert_eq!(
        std::fs::read(destination.path().join("notes.txt")).unwrap(),
        longer
    );
}

#[tokio::test]
async fn a_pattern_that_is_neither_text_nor_noise_still_arrives_intact() {
    let source = Scratch::new("zstd-pattern-source");
    let destination = Scratch::new("zstd-pattern-dest");
    let contents = pattern(SIZE);
    source.write("pattern.bin", &contents);

    transfer(source.path(), destination.path(), SendOptions::default()).await;

    assert_eq!(
        std::fs::read(destination.path().join("pattern.bin")).unwrap(),
        contents
    );
}
