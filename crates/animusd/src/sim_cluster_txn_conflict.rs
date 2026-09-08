//! Issue #734's deterministic sibling of `lib.rs`'s own
//! `issue_298_conflict_tests::a_fresh_stage_pushes_a_decided_blockers_
//! resolution_instead_of_conflicting` — the identical scenario (a fresh
//! transaction's stage on a key blocked by an ALREADY-DECIDED-but-
//! unresolved intent converges to success, never a spurious
//! `TransactionConflict`), driven against a real [`SimCluster`] instead of
//! a real-thread `ProdEnv` node.
//!
//! **Why this file exists, given the real-thread test already covers the
//! mechanism.** The real-thread test's own isolation claim — "B's single,
//! direct `txn_prepare` attempt observes `IntentBlocked` on A's still-live
//! intent, deterministically" — used to be aspirational, not real: this
//! node's own `txn_resolver_loop` sweeps every decided-but-unresolved
//! anchor unconditionally, once a second, for as long as it runs, and
//! under real CI/sandbox contention that sweep occasionally won the race
//! against the test's own probe before `Node::abort_background_tasks_
//! for_test` closed the gap (see that method's own doc, and this crate's
//! `issue_298_conflict_tests` module, for the full incident). This module
//! proves the identical property with NO real-time dependency at all:
//! [`SimCluster`] spawns no background loops whatsoever (`sim_cluster`'s
//! own module doc — `txn_resolver_loop` included), so there is no sweep
//! to race in the first place, and every step below happens in the exact
//! order the scenario itself calls it, virtual-time-driven and
//! seed-reproducible.
//!
//! Declared `#[cfg(test)] mod sim_cluster_txn_conflict;` from `lib.rs`, a
//! sibling of `sim_cluster_corpus`/`sim_cluster_dynamo`/`sim_cluster_
//! dynamo_corpus`, for the identical reason those already document: needs
//! `SimCluster`'s own `pub(crate)` surface (here, the `txn_prepare_pushing`/
//! `txn_prepare_once`/`txn_decide_anchor`/`push_resolution_if_decided`/
//! `txn_resolve_participant` wrapper methods added to `sim_cluster.rs`
//! alongside `put`/`get`/`delete`/`scan` for this module's own sake), no
//! further visibility widened. **Named deliberately distinct from `sim_
//! cluster_dynamo_transact`** — that name is already spoken for by the C-06
//! stack's own `TransactWriteItems`/`TransactGetItems` wire-level scenarios
//! (a different, unrelated file); this module is about the raw 2PC
//! coordinator primitives (`txn_prepare`/`txn_decide_anchor`/
//! `push_resolution_if_decided`), never the DynamoDB wire at all.

use animus_cp_data::{StageOutcome, TxnOutcome};
use animus_dynamo::AttributeValue;

use super::sim_cluster::SimCluster;
use crate::dynamo;

const TABLE: &str = "issue298_conflict";
const PK: &str = "k1";
const SK: &str = "v1";

#[test]
fn a_fresh_stage_pushes_a_decided_blockers_resolution_instead_of_conflicting() {
    run(0xD2C4_0001);
}

#[test]
fn a_fresh_stage_pushes_a_decided_blockers_resolution_instead_of_conflicting_seed2() {
    run(0xD2C4_0002);
}

/// Replay proof (repo convention): `ANIMUS_SEED=<seed> cargo test -p
/// animusd --lib replays_issue_734_txn_intent_block_push_from_an_explicit_env_seed`.
#[test]
fn replays_issue_734_txn_intent_block_push_from_an_explicit_env_seed() {
    let seed = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0xD2C4_0003);
    run(seed);
}

/// One node, one tablet, RF1 — the exact shape `issue_298_conflict_tests`'s
/// own `single_node`/`single_node_config` build over `ProdEnv`, so this
/// scenario is a faithful sibling rather than a differently-shaped proof.
fn run(seed: u64) {
    let mut cluster = SimCluster::new(seed, 1, 1);
    cluster.create_table(TABLE);

    // The same raw key `SimClusterHandle::get`/`put`'s own private
    // `item_key` helper would build for `(PK, SK)` — built directly via
    // `dynamo::item_key` (this crate's own pub(crate) primitive) since
    // `txn_prepare_pushing`/`txn_prepare_once` take a raw key, not a
    // `(pk, sk)` pair, but the final read below goes through `SimCluster::
    // get(_, _, PK, SK, _)`, so the two encodings must (and do) agree.
    let key = dynamo::item_key(
        &AttributeValue::S(PK.to_string()),
        Some(&AttributeValue::S(SK.to_string())),
    );

    // Transaction A stages cleanly, then is decided `Aborted` (standing in
    // for a participant elsewhere having failed) — but its own resolve is
    // deliberately never called, leaving `key` an unresolved `Intent(txn_a)`
    // even though A's record is already a final, durable decision.
    let (txn_a, record_a, table_a, ts_a) = cluster
        .txn_prepare_pushing(0, TABLE, key.clone(), Some(b"from-a".to_vec()))
        .unwrap_or_else(|e| panic!("transaction A stages cleanly (seed={seed}): {e:?}"));
    let decided_a = cluster
        .txn_decide_anchor(0, &table_a, txn_a.clone(), record_a.clone(), false, ts_a)
        .unwrap_or_else(|e| {
            panic!("deciding A never fails outright in this single-node setup (seed={seed}): {e}")
        });
    assert!(
        matches!(decided_a, TxnOutcome::Aborted),
        "A must decide Aborted (seed={seed}): {decided_a:?}"
    );
    // No `txn_resolve_participant` call here — the intentional gap.

    // A single, direct `txn_prepare_once` attempt for B (never
    // `txn_prepare_pushing`'s own retry loop — this scenario asserts what
    // ONE push accomplishes). Deterministic by construction: this fixture
    // never spawns `txn_resolver_loop`, so there is no background sweep
    // that could have cleared A's intent before this call — `key` is
    // guaranteed to still hold `Intent(txn_a)`.
    let (_, _, _, _, outcome) = cluster
        .txn_prepare_once(0, TABLE, key.clone(), Some(b"from-b".to_vec()))
        .unwrap_or_else(|e| panic!("B's stage entry itself applies (seed={seed}): {e:?}"));
    let (blocker, blocker_record_table, blocker_record_key) = match outcome {
        StageOutcome::IntentBlocked {
            txn_id,
            record_table,
            record_key,
            ..
        } => (txn_id, record_table, record_key),
        other => {
            panic!("expected IntentBlocked on A's still-live intent, got {other:?} (seed={seed})")
        }
    };
    assert_eq!(
        blocker, txn_a,
        "the blocker must be A's own txn_id (seed={seed})"
    );

    // The fix under test, called directly and in isolation.
    cluster.push_resolution_if_decided(
        0,
        TABLE,
        key.clone(),
        blocker,
        blocker_record_table,
        blocker_record_key,
    );

    // Pre-fix (`push_resolution_if_decided` a no-op stub), this second
    // attempt hits the identical still-live `Intent(txn_a)` and reports
    // `IntentBlocked` again — never `Staged`. Post-fix, the push above
    // already resolved A's intent, so this attempt stages cleanly.
    let (txn_b, record_b, table_b, ts_b, outcome_b) = cluster
        .txn_prepare_once(0, TABLE, key.clone(), Some(b"from-b".to_vec()))
        .unwrap_or_else(|e| panic!("B's second stage attempt itself applies (seed={seed}): {e:?}"));
    assert!(
        matches!(outcome_b, StageOutcome::Staged),
        "B must stage cleanly once A's already-decided blocker has been pushed — never a \
         repeat IntentBlocked (which is what a spurious TransactionConflict is built from): \
         {outcome_b:?} (seed={seed})"
    );

    // Decide and resolve B, then confirm the key holds EXACTLY B's value —
    // never A's (which must stay discarded, since A aborted) and never a
    // torn/duplicated state.
    let decided_b = cluster
        .txn_decide_anchor(0, &table_b, txn_b.clone(), record_b.clone(), true, ts_b)
        .unwrap_or_else(|e| {
            panic!("deciding B never fails outright in this single-node setup (seed={seed}): {e}")
        });
    assert!(
        matches!(decided_b, TxnOutcome::Committed { .. }),
        "B must decide Committed (seed={seed}): {decided_b:?}"
    );
    cluster
        .txn_resolve_participant(0, &table_b, txn_b, record_b, vec![key.clone()], decided_b)
        .unwrap_or_else(|e| panic!("resolving B succeeds (seed={seed}): {e}"));

    let value = cluster
        .get(0, TABLE, PK, SK, true)
        .unwrap_or_else(|e| panic!("linearizable read succeeds (seed={seed}): {e}"))
        .unwrap_or_else(|| {
            panic!("B's committed write must be readable, never lost (seed={seed})")
        });
    assert_eq!(
        value,
        b"from-b".to_vec(),
        "the key must hold exactly B's value — A's aborted write must never resurface \
         (seed={seed})"
    );
}
