//! Feeding arbitrary bytes to the two places a peer's input reaches this build.
//!
//! Everything else a peer sends is already shaped by the time it is looked at: sizes are
//! bounded by the framing, paths arrive as components. These two are where raw bytes turn
//! into structure, so these two are where a malformed input either becomes a typed error or
//! becomes a panic.
//!
//! The harnesses are plain functions rather than `libfuzzer` targets so they build on
//! stable and stay in CI. `spd-fuzz/fuzz` wires the same functions to cargo-fuzz for the
//! long runs; `cargo xtask fuzz` starts one.
//!
//! Neither harness asserts anything about the *result*. Refusing malformed input is the
//! expected outcome; the property under test is that nothing panics, allocates without a
//! bound, or loops forever on it.

use spd_core::proto::codec::decode;
use spd_core::safety::limits::Limits;
use spd_core::safety::path::SafeRelPath;

/// Decodes one control frame.
pub fn decode_control(data: &[u8]) {
    let _ = decode(data);
}

/// Builds a path from components carved out of `data`.
///
/// The bytes are split on a separator that cannot appear inside a component, so a fuzzer
/// exploring the input space explores component *boundaries* as well as their contents -
/// which is where the interesting cases are: `..` alone, an empty component, a name that is
/// fine until the one after it.
///
/// # Panics
/// On purpose, if a path that was accepted stops being acceptable when rebuilt from its own
/// components. That is the property being fuzzed, and a fuzz harness reports a broken
/// invariant by crashing - that is how the fuzzer notices.
pub fn safe_path(data: &[u8]) {
    let Ok(text) = core::str::from_utf8(data) else {
        return;
    };

    let components: Vec<String> = text.split('\u{1f}').map(str::to_owned).collect();

    if let Ok(path) = SafeRelPath::from_components(&components, &Limits::DEFAULT) {
        // A path that was accepted must survive being written down and read back: the
        // components on the wire are the only thing the other side ever sees.
        let round_trip = SafeRelPath::from_components(path.components(), &Limits::DEFAULT);
        assert!(
            round_trip.is_ok(),
            "an accepted path stopped being acceptable: {components:?}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic bytes, so a failure here is reproducible without a corpus file.
    fn pseudorandom(seed: u64, len: usize) -> Vec<u8> {
        let mut state = seed | 1;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state.to_le_bytes()[0]
            })
            .collect()
    }

    /// A short, deterministic sweep of both harnesses.
    ///
    /// Not a substitute for a real fuzz run - it is a few thousand inputs, not a few
    /// billion - but it is the part that runs on every commit, so a harness that stops
    /// compiling or starts panicking is caught immediately rather than the next time
    /// somebody remembers to fuzz.
    #[test]
    fn neither_harness_panics_on_arbitrary_bytes() {
        for seed in 1..500_u64 {
            for len in [0, 1, 7, 64, 4096] {
                let data = pseudorandom(seed, len);
                decode_control(&data);
                safe_path(&data);
            }
        }
    }

    #[test]
    fn the_path_harness_reaches_real_components() {
        // Anything a fuzzer might stumble on: a traversal, an empty component, a plain
        // name. None of them may panic, and only the last may be accepted.
        safe_path(b"..\x1fescaped.bin");
        safe_path(b"\x1f\x1f");
        safe_path("docs\u{1f}notes.txt".as_bytes());
    }
}
