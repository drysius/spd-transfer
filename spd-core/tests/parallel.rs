//! Many files at once.
//!
//! Covers the F4 acceptance conditions: files move in parallel and all of them arrive
//! intact, and a memory budget too small for the requested stream count narrows the
//! transfer instead of ignoring the budget.

mod common;

use std::num::NonZeroU32;
use std::path::Path;

use common::{Scratch, bound_listener, dial, pattern};
use spd_core::pipeline::budget::{JobLimits, TransferPlan};
use spd_core::pipeline::recv::{ReceiveOptions, receive_tree};
use spd_core::pipeline::send::{SendOptions, send_tree};
use spd_core::safety::limits::Limits;

const FILES: usize = 40;

fn jobs(streams: u32) -> JobLimits {
    JobLimits {
        streams: NonZeroU32::new(streams).unwrap(),
        ..JobLimits::DEFAULT
    }
}

/// Sends `source` into `destination` with a given budget and stream ceiling.
async fn transfer(source: &Path, destination: &Path, mem_budget_bytes: u64, streams: u32) -> u64 {
    let limits = Limits::DEFAULT;
    let (listener, address) = bound_listener(limits);
    let destination = destination.to_path_buf();

    let receiving = tokio::spawn(async move {
        let session = listener.accept().await.unwrap();
        receive_tree(
            session,
            &destination,
            ReceiveOptions {
                mem_budget_bytes,
                jobs: jobs(streams),
                ..ReceiveOptions::default()
            },
            &limits,
        )
        .await
        .unwrap()
    });

    let sender = dial(address, limits).await;
    let report = send_tree(
        sender,
        source,
        SendOptions {
            mem_budget_bytes,
            jobs: jobs(streams),
            ..SendOptions::default()
        },
        &limits,
    )
    .await
    .unwrap();

    let received = receiving.await.unwrap();
    assert_eq!(
        received.files, report.transferred.files,
        "both sides should agree on how many files moved"
    );

    report.transferred.files
}

fn many_files(label: &str) -> Scratch {
    let scratch = Scratch::new(label);
    for index in 0..FILES {
        // Sizes vary so the workers do not all finish together, which is where an
        // interleaved reply would show up.
        scratch.write(
            &format!("file-{index:02}.bin"),
            &pattern(1024 * (index + 1)),
        );
    }
    scratch
}

#[tokio::test]
async fn every_file_arrives_when_many_are_in_flight() {
    let source = many_files("parallel-source");
    let destination = Scratch::new("parallel-dest");

    let moved = transfer(source.path(), destination.path(), 64 * 1024 * 1024, 8).await;
    assert_eq!(moved, FILES as u64);

    for index in 0..FILES {
        let name = format!("file-{index:02}.bin");
        assert_eq!(
            std::fs::read(destination.path().join(&name)).unwrap(),
            pattern(1024 * (index + 1)),
            "{name} should arrive unchanged"
        );
    }

    assert!(
        std::fs::read_dir(destination.path())
            .unwrap()
            .filter_map(Result::ok)
            .all(|entry| !entry.file_name().to_string_lossy().ends_with(".part")),
        "no partial file should be left behind"
    );
}

#[tokio::test]
async fn a_budget_too_small_for_the_stream_count_still_transfers_everything() {
    let source = many_files("narrow-source");
    let destination = Scratch::new("narrow-dest");

    // 4 MiB cannot feed 16 streams of three 1 MiB buffers each, so the plan narrows.
    let budget = 4 * 1024 * 1024;
    let plan = TransferPlan::derive(budget, jobs(16));
    assert!(
        plan.workers.get() < 16,
        "the budget should cap the workers, got {}",
        plan.workers.get()
    );
    assert!(plan.reserved_bytes() <= budget);

    let moved = transfer(source.path(), destination.path(), budget, 16).await;
    assert_eq!(
        moved, FILES as u64,
        "a narrow budget still moves every file"
    );
}

#[tokio::test]
async fn a_single_stream_moves_the_same_tree() {
    let source = many_files("serial-source");
    let destination = Scratch::new("serial-dest");

    let moved = transfer(source.path(), destination.path(), 64 * 1024 * 1024, 1).await;

    assert_eq!(moved, FILES as u64);
    assert_eq!(
        std::fs::read(destination.path().join("file-00.bin")).unwrap(),
        pattern(1024)
    );
}
