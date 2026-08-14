//! Counters a transfer keeps while it runs, and the snapshots a user interface reads.
//!
//! The previous project threaded `&mut ProgressState` through six transfer functions, so
//! every signature on the path carried a user-interface concern and no function could be
//! called without one. Here the counters are atomics behind a cheap handle: a worker adds
//! to them, whoever is drawing reads a snapshot, and neither waits for the other.
//!
//! Atomics rather than an actor because there is no invariant between the counters. Nothing
//! goes wrong if `bytes` is read a microsecond after `files`; a progress bar that is one
//! buffer out of date is a progress bar.
//!
//! A [`Progress`] nobody reads costs a handful of atomic adds per buffer, which is why
//! there is no "off" switch: a switch would be a branch on the same hot path.

use core::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Live counters for one transfer.
///
/// Cheap to clone - it is a handle - so every worker holds its own without coordinating.
#[derive(Debug, Clone, Default)]
pub struct Progress {
    inner: Arc<Counters>,
}

#[derive(Debug, Default)]
struct Counters {
    files_total: AtomicU64,
    bytes_total: AtomicU64,
    files_done: AtomicU64,
    bytes_done: AtomicU64,
    wire_bytes: AtomicU64,
    scan_millis: AtomicU64,
}

impl Progress {
    /// A fresh set of counters.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records what the transfer is expected to move, once negotiation settles it.
    pub fn expect(&self, files: u64, bytes: u64) {
        self.inner.files_total.store(files, Ordering::Relaxed);
        self.inner.bytes_total.store(bytes, Ordering::Relaxed);
    }

    /// Records how long scanning and hashing the tree took.
    pub fn scanned_in(&self, millis: u64) {
        self.inner.scan_millis.store(millis, Ordering::Relaxed);
    }

    /// Adds file bytes that have moved, and the bytes they actually cost on the wire.
    ///
    /// The two differ whenever a body is compressed, and the difference is the only honest
    /// way to answer "is this transfer worth compressing".
    pub fn advance(&self, bytes: u64, wire_bytes: u64) {
        self.inner.bytes_done.fetch_add(bytes, Ordering::Relaxed);
        self.inner
            .wire_bytes
            .fetch_add(wire_bytes, Ordering::Relaxed);
    }

    /// Records one finished file.
    pub fn finished_file(&self) {
        self.inner.files_done.fetch_add(1, Ordering::Relaxed);
    }

    /// Everything at once, for whoever is drawing.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            files_total: self.inner.files_total.load(Ordering::Relaxed),
            bytes_total: self.inner.bytes_total.load(Ordering::Relaxed),
            files_done: self.inner.files_done.load(Ordering::Relaxed),
            bytes_done: self.inner.bytes_done.load(Ordering::Relaxed),
            wire_bytes: self.inner.wire_bytes.load(Ordering::Relaxed),
            scan_millis: self.inner.scan_millis.load(Ordering::Relaxed),
        }
    }
}

/// What the counters said at one moment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Snapshot {
    /// Files the transfer expects to move.
    pub files_total: u64,
    /// File bytes it expects to move.
    pub bytes_total: u64,
    /// Files finished so far.
    pub files_done: u64,
    /// File bytes moved so far.
    pub bytes_done: u64,
    /// Bytes that actually crossed so far, after compression.
    pub wire_bytes: u64,
    /// Milliseconds spent scanning and hashing before anything moved.
    pub scan_millis: u64,
}

impl Snapshot {
    /// Bytes saved by compression, as a percentage of the file bytes moved.
    ///
    /// Zero when nothing has moved yet, or when nothing compressed. Never negative: a body
    /// that grew was sent uncompressed instead, so `wire_bytes` cannot exceed `bytes_done`
    /// by more than the odd frame header.
    #[must_use]
    pub const fn saved_percent(&self) -> u64 {
        if self.bytes_done == 0 || self.wire_bytes >= self.bytes_done {
            return 0;
        }

        (self.bytes_done - self.wire_bytes) * 100 / self.bytes_done
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_add_up_across_handles() {
        let progress = Progress::new();
        let worker = progress.clone();

        progress.expect(2, 1_000);
        worker.advance(400, 100);
        worker.finished_file();
        progress.advance(600, 500);
        progress.finished_file();

        let snapshot = progress.snapshot();
        assert_eq!(snapshot.files_total, 2);
        assert_eq!(snapshot.bytes_total, 1_000);
        assert_eq!(snapshot.files_done, 2);
        assert_eq!(snapshot.bytes_done, 1_000);
        assert_eq!(snapshot.wire_bytes, 600);
    }

    #[test]
    fn the_saving_is_a_percentage_of_what_moved() {
        let progress = Progress::new();
        progress.advance(1_000, 250);

        assert_eq!(progress.snapshot().saved_percent(), 75);
    }

    #[test]
    fn nothing_moved_and_nothing_saved_are_both_zero_rather_than_a_division() {
        assert_eq!(Snapshot::default().saved_percent(), 0);

        let grew = Snapshot {
            bytes_done: 100,
            wire_bytes: 101,
            ..Snapshot::default()
        };
        assert_eq!(grew.saved_percent(), 0);
    }
}
