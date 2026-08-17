//! Sending a folder twice, and what the second run is allowed to move.
//!
//! Covers the F3 acceptance conditions: an unchanged tree transfers nothing, a single
//! edited file transfers alone, and `--dry-run` reports exactly what the real run does.

mod common;

use std::path::Path;

use common::{Scratch, bound_listener, dial, pattern};
use spd_core::pipeline::recv::{ReceiveOptions, receive_tree};
use spd_core::pipeline::send::{SendOptions, SendReport, send_tree};
use spd_core::safety::limits::Limits;

/// Runs one full session and returns what each side reported.
async fn transfer(source: &Path, destination: &Path, options: SendOptions) -> SendReport {
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
    let report = send_tree(sender, source, options, &limits).await.unwrap();

    let received = receiving.await.unwrap();
    assert_eq!(
        received.files, report.transferred.files,
        "both sides should agree on how many files moved"
    );

    report
}

fn tree(label: &str) -> Scratch {
    let scratch = Scratch::new(label);
    std::fs::create_dir_all(scratch.path().join("nested")).unwrap();
    scratch.write("notes.txt", b"first version");
    scratch.write("nested/photo.bin", &pattern(64 * 1024));
    scratch.write("nested/data.bin", &pattern(4096));
    scratch
}

#[tokio::test]
async fn a_second_run_over_an_unchanged_tree_moves_nothing() {
    let source = tree("sync-source");
    let destination = Scratch::new("sync-dest");

    let first = transfer(source.path(), destination.path(), SendOptions::default()).await;
    assert_eq!(first.transferred.files, 3);
    assert_eq!(first.skipped, 0);

    let second = transfer(source.path(), destination.path(), SendOptions::default()).await;
    assert_eq!(
        second.transferred.files, 0,
        "nothing changed, nothing moves"
    );
    assert_eq!(second.transferred.bytes, 0);
    assert_eq!(second.skipped, 3);
}

#[tokio::test]
async fn changing_one_file_transfers_only_that_file() {
    let source = tree("delta-source");
    let destination = Scratch::new("delta-dest");

    transfer(source.path(), destination.path(), SendOptions::default()).await;

    // A different length, so the diff settles on size alone and the test does not depend
    // on timestamp resolution.
    source.write("notes.txt", b"second version, longer than the first");

    let second = transfer(source.path(), destination.path(), SendOptions::default()).await;

    assert_eq!(second.transferred.files, 1);
    assert_eq!(second.skipped, 2);
    assert_eq!(
        std::fs::read(destination.path().join("notes.txt")).unwrap(),
        b"second version, longer than the first"
    );
}

#[tokio::test]
async fn a_dry_run_reports_what_the_real_run_then_does() {
    let source = tree("dry-source");
    let destination = Scratch::new("dry-dest");

    let planned = transfer(
        source.path(),
        destination.path(),
        SendOptions {
            dry_run: true,
            ..SendOptions::default()
        },
    )
    .await;

    assert_eq!(planned.transferred.files, 0, "a dry run sends nothing");
    assert_eq!(planned.planned.files, 3);
    assert!(
        std::fs::read_dir(destination.path())
            .unwrap()
            .next()
            .is_none(),
        "a dry run must not create anything"
    );

    let real = transfer(source.path(), destination.path(), SendOptions::default()).await;

    assert_eq!(real.transferred.files, planned.planned.files);
    assert_eq!(real.transferred.bytes, planned.planned.bytes);
}

/// Without pre-hashing, the first copy still arrives byte for byte and no hash cache is
/// written - the hashes it would hold were never computed. What the receiver checks after
/// each file is unaffected: that hash is taken from the bytes as they are read.
#[tokio::test]
async fn skipping_the_prehash_still_delivers_the_tree_and_leaves_no_cache() {
    let source = tree("no-prehash-source");
    let destination = Scratch::new("no-prehash-dest");

    let report = transfer(
        source.path(),
        destination.path(),
        SendOptions {
            prehash: false,
            ..SendOptions::default()
        },
    )
    .await;

    assert_eq!(report.transferred.files, 3);
    assert_eq!(
        std::fs::read(destination.path().join("notes.txt")).unwrap(),
        b"first version"
    );
    assert!(
        !source.path().join(".spd").join("hashcache").exists(),
        "nothing was hashed, so there is nothing to remember"
    );

    // And a second run still skips what is already there: size and mtime settle it without
    // a single hash on either side.
    let second = transfer(
        source.path(),
        destination.path(),
        SendOptions {
            prehash: false,
            ..SendOptions::default()
        },
    )
    .await;

    assert_eq!(second.transferred.files, 0);
    assert_eq!(second.skipped, 3);
}

/// A received file keeps the timestamp it had on the sender.
///
/// This is what makes the second run cheap when there are no hashes to compare: stamped
/// with its arrival time instead, every file would look changed and the whole tree would
/// move again, every single run.
#[tokio::test]
async fn a_received_file_keeps_the_senders_timestamp() {
    let source = tree("mtime-source");
    let destination = Scratch::new("mtime-dest");

    transfer(source.path(), destination.path(), SendOptions::default()).await;

    for name in ["notes.txt", "nested/photo.bin"] {
        let sent = std::fs::metadata(source.path().join(name))
            .unwrap()
            .modified()
            .unwrap();
        let arrived = std::fs::metadata(destination.path().join(name))
            .unwrap()
            .modified()
            .unwrap();

        let apart = sent.duration_since(arrived).unwrap_or_default()
            + arrived.duration_since(sent).unwrap_or_default();
        assert!(
            apart < std::time::Duration::from_secs(2),
            "{name} arrived stamped {apart:?} away from the file it was copied from"
        );
    }
}

#[tokio::test]
async fn checksum_mode_notices_a_file_that_kept_its_size_and_timestamp() {
    let source = Scratch::new("checksum-source");
    let destination = Scratch::new("checksum-dest");
    source.write("same-size.bin", b"aaaa");

    transfer(source.path(), destination.path(), SendOptions::default()).await;

    // Overwrite the destination with different content of the same length, then restore
    // its timestamp: only hashing can tell the two apart.
    let target = destination.path().join("same-size.bin");
    let before = std::fs::metadata(&target).unwrap().modified().unwrap();
    std::fs::write(&target, b"bbbb").unwrap();
    filetime_restore(&target, before);

    let with_checksum = transfer(
        source.path(),
        destination.path(),
        SendOptions {
            checksum: true,
            ..SendOptions::default()
        },
    )
    .await;

    assert_eq!(
        with_checksum.transferred.files, 1,
        "hashing should catch content that size and mtime call identical"
    );
    assert_eq!(std::fs::read(&target).unwrap(), b"aaaa");
}

/// Puts a file's modification time back, so a test can change content without changing the
/// metadata the diff would otherwise notice.
fn filetime_restore(path: &Path, when: std::time::SystemTime) {
    let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    file.set_modified(when).unwrap();
}
