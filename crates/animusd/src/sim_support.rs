//! Helpers shared by the `sim_*` test modules.
//!
//! A helper lands here once two or more `sim_*` files carried a
//! byte-identical copy of it; the `sim_*` files import it from here instead of
//! re-declaring it. A scenario-specific variant (a different table shape, key
//! name, assertion or panic message) stays local to its file: only move a
//! helper whose body is identical, because every body here is part of the
//! seed-reproducible virtual-time schedule (step sizes, timeouts, panic
//! messages) the scenarios rely on.

/// The `ANIMUS_SEED` replay override: the env var's value when it parses as a
/// `u64`, else `default`. Single-sourced for every `sim_*` scenario.
pub(crate) fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Parse a response body as JSON, panicking with the body on failure.
pub(crate) fn json(body: &str) -> serde_json::Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("invalid JSON ({e}): {body}"))
}
