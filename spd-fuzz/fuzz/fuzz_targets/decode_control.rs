//! Arbitrary bytes where a control frame is expected.

#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    spd_fuzz::decode_control(data);
});
