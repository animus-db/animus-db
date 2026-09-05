//! ADR 0065's own `SimEnv`-driven, virtual-time-only throttle-enforcement
//! coverage (roadmap W-08 step 3) — proving the bucket lifecycle end to end
//! through the real `SimCluster` fixture's route/propose/confirm (write) and
//! route/local-resolve (read) loops, not just the bucket primitive in
//! isolation (`throttle_bucket_tests`, `lib.rs`). In-crate `#[cfg(test)]
//! mod`, sibling to `sim_cluster_corpus` and for the identical reason: needs
//! `SimCluster`'s own `pub(crate)` surface (`set_throttle_defaults`,
//! `put`/`get`), no further visibility widened.
//!
//! `SimCluster`'s every node runs with `data: None` (see its own module
//! doc's "What is still `ProdEnv`-only" bullet) — `ThrottledWrites`/
//! `ThrottledReads` therefore never increment here (the metric-recording
//! sites all gate on `self.data.as_ref()`), which is a deliberate, harmless
//! gap for this fixture: the metric itself has no `SimEnv` coverage need
//! beyond what this file proves about the bucket it counts refusals from.
//! The real-thread `tests/dynamo_throttling.rs` asserts the metric's own
//! value.

use std::time::Duration;

use super::sim_cluster::SimCluster;

/// The substring [`crate::dynamo::THROTTLE_WRITE_REFUSAL`]/
/// `THROTTLE_READ_REFUSAL` share — asserted by substring rather than an
/// exact-string comparison so this test doesn't have to re-import the
/// `pub(crate)` constants from a different module for a single string
/// check.
const REFUSAL_MARKER: &str = "provisioned throughput exceeded";

#[test]
fn write_admits_a_burst_then_refuses_then_recovers_after_a_full_refill() {
    let mut cluster = SimCluster::new(0x5448_524f_5731, 1, 1);
    cluster.create_table("t");
    // 1 WCU/s on this table's one tablet ⇒ a 300-unit (300s) burst
    // (ADR 0065 Decision 4). A ~100 KiB value costs ~100 WCU/write, so the
    // burst admits a small, fast-to-loop number of writes before refusing.
    cluster.set_throttle_defaults_all(None, Some(1));
    let value = vec![0u8; 100 * 1024];
    let mut admitted = 0u32;
    let mut refused = false;
    for i in 0..10 {
        match cluster.put(0, "t", &format!("k{i}"), "", &value) {
            Ok(()) => admitted += 1,
            Err(e) => {
                assert!(
                    e.contains(REFUSAL_MARKER),
                    "expected a throttle refusal, got: {e}"
                );
                refused = true;
                break;
            }
        }
    }
    assert!(
        refused,
        "expected the burst to eventually refuse a write (admitted {admitted} first)"
    );
    assert!(
        (1..=3).contains(&admitted),
        "expected ~3 ~100-WCU writes to exhaust a 300-unit burst, admitted {admitted}"
    );

    // A refused write must be a genuine refusal, not a retryable one — the
    // very next call (no time advance) still refuses.
    let still_refused = cluster.put(0, "t", "immediate-retry", "", &value);
    assert!(
        still_refused.is_err_and(|e| e.contains(REFUSAL_MARKER)),
        "a throttled write must not silently succeed on an immediate retry"
    );

    // Advance virtual time a full burst window — the bucket refills fully.
    cluster.run_for(Duration::from_secs(300));
    cluster
        .put(0, "t", "after-refill", "", &value)
        .expect("a write after a full refill window should succeed");
}

#[test]
fn a_table_with_no_configured_limit_is_never_refused() {
    let mut cluster = SimCluster::new(0x5448_524f_5732, 1, 1);
    cluster.create_table("t");
    // No `set_throttle_defaults*` call at all — PAY_PER_REQUEST, the
    // default (ADR 0065 Decision 5(a)): byte-for-byte unchanged from
    // before this ADR.
    let value = vec![0u8; 256 * 1024]; // deliberately large — would exhaust
    // even a generous configured burst many times over if throttling were
    // (incorrectly) active.
    for i in 0..20 {
        cluster
            .put(0, "t", &format!("k{i}"), "", &value)
            .unwrap_or_else(|e| {
                panic!("put {i} unexpectedly refused on an unthrottled table: {e}")
            });
    }
}

#[test]
fn read_admits_a_burst_then_refuses_then_recovers_after_a_full_refill() {
    let mut cluster = SimCluster::new(0x5448_524f_5733, 1, 1);
    cluster.create_table("t");
    // Seed one large item BEFORE configuring the read limit (the seed
    // write itself must not be throttled). **Deliberately large** (512
    // KiB ⇒ 128 RCU/consistent-read): `SimCluster::get`'s own
    // `spawn_and_capture` advances the virtual clock by up to `OP_BUDGET`
    // (12s) per call, which at this test's 1 RCU/s rate refills up to 12
    // RCU between reads — the per-read cost must clear that per-call
    // refill by a wide margin for the loop to net-drain at all (the write
    // test's own ~100 WCU/write already clears its own 12 WCU/call refill
    // the same way).
    let value = vec![0u8; 512 * 1024];
    cluster
        .put(0, "t", "k", "", &value)
        .expect("unthrottled seed write");
    // 1 RCU/s ⇒ a 300-unit burst; each consistent read costs ~128 RCU
    // (minus whatever refilled since the last call), so a handful of reads
    // exhausts it.
    cluster.set_throttle_defaults_all(Some(1), None);
    let mut admitted = 0u32;
    let mut refused = false;
    for _ in 0..10 {
        match cluster.get(0, "t", "k", "", true) {
            Ok(_) => admitted += 1,
            Err(e) => {
                assert!(
                    e.contains(REFUSAL_MARKER),
                    "expected a throttle refusal, got: {e}"
                );
                refused = true;
                break;
            }
        }
    }
    assert!(
        refused,
        "expected the burst to eventually refuse a read (admitted {admitted} first)"
    );
    assert!(
        (1..=5).contains(&admitted),
        "unexpected admitted read count: {admitted}"
    );

    // ADR 0065 §2: a refused eventual read must not silently fall back to
    // the linearizable path either — same tablet, same bucket, still
    // refused (no time advance).
    let eventual = cluster.get(0, "t", "k", "", false);
    assert!(
        eventual.is_err_and(|e| e.contains(REFUSAL_MARKER)),
        "a throttled eventual read must return the refusal, not silently succeed via fallback"
    );

    cluster.run_for(Duration::from_secs(300));
    cluster
        .get(0, "t", "k", "", true)
        .expect("a read after a full refill window should succeed");
}

/// ADR 0065 §5(b), W-08 step 4: a table's own per-table `throughput`
/// overrides the cluster-wide default entirely — a restrictive cluster
/// default would throttle this write, but the table's own generous
/// override wins, so it never does.
/// The `ClientCtx::any_table_throughput` fast-path flag (ADR 0065 §5(b))
/// flips `true` the moment a table's per-table `throughput` is set through
/// the real schema path, and back to `false` once every throughput-carrying
/// table reverts to `PAY_PER_REQUEST` — proving `SimCluster::
/// set_table_throughput`'s tail recompute (`SimClusterHandle::
/// recompute_any_table_throughput_all`) actually keeps the flag correct in
/// both directions, the exact behavior `ClientCtx::any_table_throughput`'s
/// own field doc documents as its real-cluster recompute points'
/// (`dynamo::create_table`/`update_table_throughput`, `index_drain::
/// change_consumer_loop`) job. A fresh cluster with no throughput anywhere
/// starts `false` — the same default state [`a_table_with_no_configured_limit_is_never_refused`]
/// exercises functionally; this test asserts the flag's own value directly.
#[test]
fn any_table_throughput_flag_tracks_the_catalog_through_set_and_revert() {
    let mut cluster = SimCluster::new(0x5448_524f_5735, 1, 1);
    cluster.create_table("t");
    assert!(
        !cluster.any_table_throughput(0),
        "a fresh table with no throughput configured must leave the flag false"
    );

    cluster.set_table_throughput(
        "t",
        Some(animus_control::ProvisionedThroughput {
            read_units: 100,
            write_units: 100,
        }),
    );
    assert!(
        cluster.any_table_throughput(0),
        "setting a table's own throughput must flip the flag true"
    );

    // `UpdateTable` reverting to `PAY_PER_REQUEST` (`spec: None`) is the
    // real wire shape `update_table_throughput` proposes for a disable —
    // mirrored here via the identical `MetaCommand::SetTableThroughput`
    // the real DDL handler sends.
    cluster.set_table_throughput("t", None);
    assert!(
        !cluster.any_table_throughput(0),
        "reverting the last table's own throughput to PAY_PER_REQUEST must flip the flag back false"
    );
}

#[test]
fn per_table_throughput_override_wins_over_a_restrictive_cluster_default() {
    let mut cluster = SimCluster::new(0x5448_524f_5734, 1, 1);
    cluster.create_table("t");
    // A tiny cluster-wide default that would refuse almost any write...
    cluster.set_throttle_defaults_all(None, Some(1));
    // ...but this table's own override is generous enough to admit every
    // write in this test, proving the per-table spec takes priority rather
    // than merging with (or falling back to) the cluster default.
    cluster.set_table_throughput(
        "t",
        Some(animus_control::ProvisionedThroughput {
            read_units: 1_000_000,
            write_units: 1_000_000,
        }),
    );
    let value = vec![0u8; 100 * 1024];
    for i in 0..10 {
        cluster
            .put(0, "t", &format!("k{i}"), "", &value)
            .unwrap_or_else(|e| {
                panic!("put {i} unexpectedly refused despite a generous per-table override: {e}")
            });
    }
}
