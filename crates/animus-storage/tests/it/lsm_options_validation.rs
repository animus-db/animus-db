//! `LsmOptions::level_fanout` validation (issue #441).
//!
//! The per-level compaction table budget is
//! `L1_TABLE_BUDGET * level_fanout^(level - 1)` (`lsm.rs::level_table_budget`).
//! At `level_fanout <= 1` that budget never grows with depth (`pow(0) == 1`
//! for every level at `level_fanout == 1`; `pow(n) == 0` for every level
//! `>= 2` at `level_fanout == 0`), so a table set whose fully-merged size
//! exceeds the fixed L1 budget cascades down through every level forever —
//! each cascade at level `n` re-lands over budget at level `n + 1` — without
//! ever reaching the settled (`None`) state `next_compaction_plan`'s own
//! termination argument assumes. `LsmOptions::validate()`, wired into
//! `LsmEngine::open_with_metrics` (so both `open` and `open_with` inherit
//! it), rejects this before any I/O.

use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, StorageError};
use futures::executor::block_on;

const PREFIX: &str = "db/";

fn opts_with_fanout(level_fanout: usize) -> LsmOptions {
    LsmOptions {
        level_fanout,
        ..LsmOptions::default()
    }
}

/// `validate()` alone (no engine, no `Env`) rejects `level_fanout <= 1`.
///
/// This is the exact case the issue describes: before this fix,
/// `LsmOptions { level_fanout: 1, ..Default::default() }` was accepted
/// unconditionally — `LsmOptions` derives no `Default`-only invariant check,
/// and nothing on the (then nonexistent) `validate` path stopped it. Every
/// `level_table_budget(level, opts)` call for that config now evaluates to
/// `L1_TABLE_BUDGET * 1 == 4` regardless of `level`, so a compaction cascade
/// that pushes more than 4 tables down to any level never settles.
#[test]
fn validate_rejects_fanout_of_one() {
    let err = opts_with_fanout(1)
        .validate()
        .expect_err("fanout=1 must be rejected");
    match err {
        StorageError::InvalidLevelFanout { level_fanout } => assert_eq!(level_fanout, 1),
        other => panic!("expected InvalidLevelFanout, got {other:?}"),
    }
}

/// `level_fanout == 0` is worse than `1` (level >= 2 gets budget `0`, over
/// budget from the very first table merged into it) and must also be
/// rejected, along with the `pow` edge case: `usize::pow` panics on overflow
/// in debug builds but `0` is well inside range — this is a policy rejection,
/// not a panic-avoidance patch.
#[test]
fn validate_rejects_fanout_of_zero() {
    let err = opts_with_fanout(0)
        .validate()
        .expect_err("fanout=0 must be rejected");
    assert!(matches!(
        err,
        StorageError::InvalidLevelFanout { level_fanout: 0 }
    ));
}

/// Every fanout `>= 2` — including the crate default and the smallest legal
/// value — passes validation unchanged (no behavior change for valid config).
#[test]
fn validate_accepts_fanout_of_two_and_the_default() {
    opts_with_fanout(2).validate().expect("fanout=2 is valid");
    LsmOptions::default()
        .validate()
        .expect("the crate default must stay valid");
}

/// End-to-end: `LsmEngine::open_with` propagates the same rejection, and does
/// so before touching disk (opening at a fresh prefix would otherwise
/// succeed, since there is nothing to recover).
#[test]
fn open_with_rejects_invalid_fanout() {
    let sim = Simulator::new(0x441_u64);
    let env: SimEnv = sim.env(nid(0));
    let err = match block_on(LsmEngine::open_with(env, PREFIX, opts_with_fanout(1))) {
        Ok(_) => panic!("open_with must reject level_fanout <= 1"),
        Err(e) => e,
    };
    assert!(matches!(
        err,
        StorageError::InvalidLevelFanout { level_fanout: 1 }
    ));
}

/// A valid fanout still opens normally (no regression at the validated
/// boundary).
#[test]
fn open_with_accepts_valid_fanout() {
    let sim = Simulator::new(0x442_u64);
    let env: SimEnv = sim.env(nid(0));
    block_on(LsmEngine::open_with(env, PREFIX, opts_with_fanout(4)))
        .expect("open_with must accept a valid level_fanout");
}
