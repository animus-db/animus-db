//! Single merged integration-test binary for this crate (one link instead of 3).
//! Real-thread / `ProdEnv` binaries stay separate `tests/*.rs` targets — see the crate's CLAUDE.md.

mod placement;
mod placement_props;
mod rebalance;
