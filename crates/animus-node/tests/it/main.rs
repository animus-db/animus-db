//! Single merged integration-test binary for this crate (one link instead of 2).
//! Real-thread / `ProdEnv` binaries stay separate `tests/*.rs` targets — see the crate's CLAUDE.md.

mod index_backfill_sim;
mod ttl_reaper_sim;
