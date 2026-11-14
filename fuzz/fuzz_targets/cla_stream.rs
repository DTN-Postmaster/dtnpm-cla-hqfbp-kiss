#![no_main]

use libfuzzer_sys::fuzz_target;

use dtnpm_cla_hqfbp_kiss_fuzz::fuzz_kiss_and_hqfbp;

fuzz_target!(|data: &[u8]| {
    fuzz_kiss_and_hqfbp(data);
});
