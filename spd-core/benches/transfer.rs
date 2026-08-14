//! What the transfer path costs, per byte and per file.
//!
//! These are the steps between a file on one disk and a file on another that are not the
//! network: hashing it, deciding whether to compress it, compressing it, and listing the
//! tree it lives in. Each one is measured on its own, because "the transfer got slower" is
//! not a sentence anybody can act on.
//!
//! End-to-end throughput is deliberately absent. It is a property of the disk and the link
//! rather than of this code, and a number that moves with whatever else the machine is
//! doing teaches nothing. `PLAN.md §10` records the measured figures for that instead.

// `criterion_group!` expands to a public item this crate cannot document. Scoped to this
// one bench harness rather than loosened for the workspace.
#![allow(missing_docs)]

use core::time::Duration;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use spd_core::compress::{self, Encoder};
use spd_core::safety::limits::Limits;
use spd_core::safety::path::SafeRelPath;
use spd_core::scan::hash_cache::hash_file;
use spd_core::scan::walk::{WalkOptions, walk};

/// One mebibyte: large enough to measure per-byte cost, small enough to run quickly.
const BLOCK: usize = 1024 * 1024;

/// Files in the scanned tree. A few hundred is a realistic project folder.
const TREE_FILES: usize = 200;

/// Repetitive text, which is what a folder of source or logs looks like to zstd.
fn text(len: usize) -> Vec<u8> {
    "the quick brown fox jumps over the lazy dog. "
        .bytes()
        .cycle()
        .take(len)
        .collect()
}

/// Bytes with no structure left to find.
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

/// A directory that deletes itself when the benchmark ends.
struct Scratch(std::path::PathBuf);

impl Scratch {
    fn new(label: &str) -> Self {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.as_nanos())
            .unwrap_or_default();
        let path = std::env::temp_dir().join(format!("spd-bench-{label}-{unique}"));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Hashing, which every byte of every transfer goes through twice - once on each side.
fn hashing(criterion: &mut Criterion) {
    let scratch = Scratch::new("hash");
    let path = scratch.0.join("payload.bin");
    std::fs::write(&path, noise(BLOCK)).unwrap();

    let mut group = criterion.benchmark_group("hash");
    group.throughput(Throughput::Bytes(BLOCK as u64));
    group.bench_function("blake3 1 MiB from disk", |bencher| {
        bencher.iter(|| hash_file(&path).unwrap());
    });
    group.finish();
}

/// The compression decision, which every file pays and most files pay in full.
fn deciding(criterion: &mut Criterion) {
    let compressible = text(compress::SAMPLE_BYTES);
    let incompressible = noise(compress::SAMPLE_BYTES);
    let mut scratch = vec![0_u8; compress::SAMPLE_BYTES];

    let mut group = criterion.benchmark_group("compression decision");
    group.throughput(Throughput::Bytes(compress::SAMPLE_BYTES as u64));

    group.bench_function("sample of text", |bencher| {
        bencher.iter(|| compress::worth_compressing(&compressible, &mut scratch).unwrap());
    });
    group.bench_function("sample of noise", |bencher| {
        bencher.iter(|| compress::worth_compressing(&incompressible, &mut scratch).unwrap());
    });
    group.bench_function("extension only", |bencher| {
        bencher.iter(|| compress::is_already_compressed(std::path::Path::new("holiday.mp4")));
    });

    group.finish();
}

/// Compressing a body, which is the CPU cost a transfer trades bandwidth for.
fn compressing(criterion: &mut Criterion) {
    let block = text(BLOCK);

    let mut group = criterion.benchmark_group("compress");
    group.throughput(Throughput::Bytes(BLOCK as u64));
    group.bench_function("zstd 1 MiB of text", |bencher| {
        bencher.iter_batched(
            || (Encoder::new().unwrap(), vec![0_u8; BLOCK]),
            |(mut encoder, mut output)| {
                let mut taken = 0;
                while taken < block.len() {
                    let step = encoder.push(&block[taken..], &mut output).unwrap();
                    taken += step.taken;
                }
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// Validating a path, which happens once per file per transfer and must not be the cost.
fn validating(criterion: &mut Criterion) {
    let components = vec![
        "nested".to_owned(),
        "deeper".to_owned(),
        "file.bin".to_owned(),
    ];

    criterion.bench_function("validate a path", |bencher| {
        bencher.iter(|| SafeRelPath::from_components(&components, &Limits::DEFAULT).unwrap());
    });
}

/// Listing a tree, which is what every send pays before it can offer anything.
///
/// Only the walk: hashing is measured on its own above, and what the hash cache saves is
/// the difference between the two, per file, on a second run.
fn scanning(criterion: &mut Criterion) {
    let scratch = Scratch::new("scan");
    for index in 0..TREE_FILES {
        std::fs::write(scratch.0.join(format!("file-{index}.bin")), text(4_096)).unwrap();
    }

    let mut group = criterion.benchmark_group("scan");
    group.throughput(Throughput::Elements(TREE_FILES as u64));
    group.bench_function("walk 200 files", |bencher| {
        bencher.iter(|| walk(&scratch.0, WalkOptions::default(), &Limits::DEFAULT).unwrap());
    });
    group.finish();
}

criterion_group! {
    name = benches;
    // Enough samples to be stable, short enough that running the suite is not an event.
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2));
    targets = hashing, deciding, compressing, validating, scanning
}
criterion_main!(benches);
