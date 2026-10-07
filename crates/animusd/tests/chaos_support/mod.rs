//! Real-process chaos harness (R-01 sub-track b, `docs/chaos.md`).
//!
//! Not part of the per-push gates: the `chaos` cargo feature gates the
//! `chaos` test target. See `docs/chaos.md` for what each piece proves.

pub mod client;
pub mod cluster;
pub mod diskfull;
pub mod nemesis;
pub mod proxy;
pub mod rng;
pub mod workload;
