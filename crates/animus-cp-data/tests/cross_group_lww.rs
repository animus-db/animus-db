//! TOMBSTONE (ADR 0050 Train B rung 2) — this binary's four scenarios all
//! modeled the **zero-copy split's shared-engine LWW handoff**: a source
//! group live-narrowing its `StorageScope` (`narrow_scope`), sealing the
//! handed-off range (`seal.rs`), and a sibling group serving the same
//! physical rows on the same engine. Under per-tablet engines with immutable
//! ranges (rung 1 + 2), every one of those ingredients is gone or inert:
//! `narrow_scope` no longer exists, sibling tablets have disjoint private
//! engines (the cross-group shared-row LWW hazard class is structurally
//! unrepresentable), and the range seal has no production proposer while the
//! old split is disabled.
//!
//! What this file used to prove, and where the surviving property lives now:
//! - HLC witnessing / apply-time ts monotonicity → `tests/witnessing.rs`
//!   (leader-change + restart monotonicity, unchanged).
//! - Seal-rejection mechanics → `seal.rs`'s own unit tests (the module
//!   itself survives, inert, until the Train B deletion sweep removes it
//!   together with this tombstone; the copy-based split's `Freeze` replaces
//!   it with its own tests in that rung).
//! - Fence-gates-apply → `tests/fenced_commands.rs` (rewritten born-narrow).
//!
//! The pre-pivot file is retrievable from git history
//! (`train-b/1-per-tablet-storage` and earlier).
