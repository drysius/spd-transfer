//! Arbitrary bytes where a peer-supplied path is expected.

#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    spd_fuzz::safe_path(data);
});
