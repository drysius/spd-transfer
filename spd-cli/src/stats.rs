//! The `--stats` block: what the transfer actually cost.
//!
//! Everything here comes from counters the core kept anyway. Nothing is measured twice and
//! nothing is estimated: a number that could not be known is left out rather than guessed.

use core::time::Duration;

use spd_core::metrics::Snapshot;

use crate::ui;

/// Prints the closing summary.
pub(crate) fn print(snapshot: &Snapshot, elapsed: Duration) {
    ui::section("stats");
    ui::field("files", &snapshot.files_done.to_string());
    ui::field("file bytes", &ui::format_bytes(snapshot.bytes_done));
    ui::field("on the wire", &ui::format_bytes(snapshot.wire_bytes));

    if snapshot.bytes_done > 0 {
        ui::field(
            "compression saved",
            &format!("{}%", snapshot.saved_percent()),
        );
    }

    ui::field(
        "scan",
        &format_duration(Duration::from_millis(snapshot.scan_millis)),
    );
    ui::field("total", &format_duration(elapsed));

    if let Some(rate) = per_second(snapshot.wire_bytes, elapsed) {
        ui::field("throughput", &format!("{}/s", ui::format_bytes(rate)));
    }
}

/// Bytes per second, or nothing when the transfer was too short to divide by.
///
/// A run that finished in under a millisecond has no meaningful rate, and inventing one
/// from a rounded-down duration produces numbers nobody should quote.
fn per_second(bytes: u64, elapsed: Duration) -> Option<u64> {
    let millis = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
    if millis == 0 {
        return None;
    }

    Some(bytes.saturating_mul(1_000) / millis)
}

/// Seconds with one decimal, or milliseconds when that would read as `0.0`.
fn format_duration(elapsed: Duration) -> String {
    let millis = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);

    if millis < 1_000 {
        return format!("{millis} ms");
    }

    format!("{}.{} s", millis / 1_000, (millis % 1_000) / 100)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_rate_needs_a_measurable_duration() {
        assert_eq!(per_second(1_000, Duration::from_millis(500)), Some(2_000));
        assert_eq!(per_second(1_000, Duration::from_micros(10)), None);
    }

    #[test]
    fn durations_read_in_the_unit_that_says_something() {
        assert_eq!(format_duration(Duration::from_millis(42)), "42 ms");
        assert_eq!(format_duration(Duration::from_millis(1_500)), "1.5 s");
        assert_eq!(format_duration(Duration::from_secs(90)), "90.0 s");
    }
}
