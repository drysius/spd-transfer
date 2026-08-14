//! Formatting for humans.
//!
//! Human-readable units live here and nowhere else: the core keeps raw numbers, so JSON
//! logs and tests never have to parse "4.0 MiB" back into bytes.

/// Formats a byte count with a binary unit and one decimal.
///
/// Integer arithmetic on purpose - a float cast would lose precision on large values for
/// no benefit, since only one decimal is ever shown.
pub(crate) fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];

    let mut value = bytes;
    let mut remainder = 0;
    let mut unit = 0;

    while value >= 1024 && unit + 1 < UNITS.len() {
        remainder = value % 1024;
        value /= 1024;
        unit += 1;
    }

    if unit == 0 {
        return format!("{value} B");
    }

    let tenths = remainder * 10 / 1024;
    format!("{value}.{tenths} {}", UNITS[unit])
}

/// Prints a `label: value` line, aligned so a block of them reads as a table.
pub(crate) fn field(label: &str, value: &str) {
    println!("  {label:<24} {value}");
}

/// Prints a section heading.
pub(crate) fn section(title: &str) {
    println!("\n{title}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_below_a_kib_keep_their_unit() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1023), "1023 B");
    }

    #[test]
    fn larger_values_step_up_the_unit() {
        assert_eq!(format_bytes(1024), "1.0 KiB");
        assert_eq!(format_bytes(4 * 1024 * 1024), "4.0 MiB");
        assert_eq!(format_bytes(1536 * 1024), "1.5 MiB");
    }

    #[test]
    fn the_largest_unit_does_not_overflow_the_table() {
        assert!(format_bytes(u64::MAX).ends_with("TiB"));
    }
}
