#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    animus_fuzz::targets::net_frames(data);
});
