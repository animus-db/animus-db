//! Single merged integration-test binary for this crate (one link instead of 11).
//! Real-thread / `ProdEnv` binaries stay separate `tests/*.rs` targets — see the crate's CLAUDE.md.

mod clock_skew;
mod determinism;
mod disk_faults;
mod encrypted_disk;
mod executor_leak;
mod handshake_ext;
mod inbox_cap;
mod net_faults;
mod pause;
mod sleep_drop;
mod stop_semantics;
mod stream_close;
