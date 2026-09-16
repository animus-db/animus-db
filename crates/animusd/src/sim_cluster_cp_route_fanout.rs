//! Issue #950 regression: `ClientCtx::cp_route`'s cross-replica leader-hint
//! fan-out (`forwarding.rs`'s `cp_route`/`probe_replica_leader_hints`).
//!
//! This drives `ClientCtx::cp_route` directly ([`SimClusterHandle::
//! cp_route`]/[`SimCluster::cp_route_timed`]) rather than a full
//! [`SimCluster::put`], so it measures the routing DECISION in isolation —
//! see those methods' own docs for why: the actual write can only complete
//! once `victim` can physically reach the leader again, a separate,
//! already-correct concern this scenario's own permanent partition
//! deliberately leaves unresolved (the fix under test is about how fast
//! `cp_route` stops trusting a stale purely-local view, not about routing
//! around a genuine, ongoing network partition).

use std::time::Duration;

use animus_dynamo::AttributeValue;

use super::sim_cluster::{CpRouteOutcome, SimCluster};

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// A tablet's leader partitioned away from a node that also hosts a replica
/// (`victim`) must not strand a routed write on `victim`'s own purely local
/// view for the whole `CLIENT_TIMEOUT` — issue #950's own reproduction: an
/// independent poll of the SAME group saw a continuously known, stable
/// leader the entire time, while a routed write on the client-facing node
/// burned the entire budget reporting "no CP group leader reachable"
/// because that ONE node's own local view had simply gone stale.
///
/// `victim`'s local `leader()` hint genuinely clears here (the leader is
/// unreachable to it specifically — a real, not merely slow, partition) and
/// stays cleared for the rest of the test: the cross-replica fan-out
/// (`CP_ROUTE_LOCAL_SUB_BUDGET`) is the only way `cp_route` can recover a
/// route at all in this scenario, since `victim`'s own local Raft state can
/// never resolve it on its own.
///
/// **Before the fix** (`CP_ROUTE_LOCAL_SUB_BUDGET`/the cross-replica
/// fan-out, `ClientCtx::probe_replica_leader_hints`): this call would poll
/// only `victim`'s own local state every `SCHEMA_POLL_INTERVAL` for the
/// full `CLIENT_TIMEOUT` (10s) and return `CpRouteOutcome::None` — verified
/// by hand (temporarily reverting `cp_route` to its pre-fix shape) before
/// landing this test; not re-asserted here as a second code path, per this
/// repo's own "read what actually failed" convention for a fix this
/// specific.
#[test]
fn victim_recovers_a_route_via_cross_replica_fanout_despite_a_permanently_stale_local_view() {
    let seed = env_seed(0x950_0001);
    let mut cluster = SimCluster::new(seed, 3, 3);
    let tablet = cluster.create_table("t");

    let leader = cluster
        .leader_index_of(tablet)
        .unwrap_or_else(|| panic!("seed={seed}: table `t`'s tablet has no leader after create"));
    let others: Vec<u64> = (0..3u64).filter(|&n| n != leader).collect();
    let victim = others[0];
    // `others[1]` (the un-partitioned third replica) is deliberately never
    // referenced by name below — it's simply reachable from `victim`
    // throughout, which is the whole point: it's the informant the
    // cross-replica fan-out reaches to learn the real leader.

    // Partition `victim` from the leader only (`SimCluster::partition` is
    // symmetric) — `victim`'s own local replica genuinely cannot hear from
    // the real leader (not merely slow), so its own `leader()` hint clears
    // via its own election timer and never recovers on its own for the
    // rest of this test. The third replica is untouched, so the leader
    // keeps its majority (itself + the third replica) and never steps
    // down — the group has a continuously known, stable leader throughout,
    // exactly the issue's own reproduction shape.
    cluster.partition(victim, leader);
    // Past several CP-data election-timeout windows (`election_base`'s
    // `[150ms, 300ms)` range, `animus-control`) so `victim`'s own belief
    // has genuinely cleared before the timed call below starts, not
    // mid-flight (a call issued the instant the partition begins would
    // still see `victim`'s own last-known-good hint and resolve instantly,
    // proving nothing about this mechanism).
    cluster.run_for(Duration::from_millis(600));

    let key = crate::dynamo::item_key(
        &AttributeValue::S("pk1".into()),
        Some(&AttributeValue::S("sk1".into())),
    );
    let (elapsed, outcome) =
        cluster.cp_route_timed(victim, "t", &key, Duration::from_millis(50), 100);

    assert!(
        matches!(outcome, CpRouteOutcome::Forward(_, true)),
        "seed={seed}: expected a hinted forward once the cross-replica fan-out found the \
         leader via the un-partitioned third replica, got {outcome:?} after {elapsed:?}"
    );
    // Far below `CLIENT_TIMEOUT` (10s) — issue #950's own bug is exactly
    // this call burning the whole budget on `victim`'s own stale local
    // view alone; the fix (`CP_ROUTE_LOCAL_SUB_BUDGET` + the cross-replica
    // fan-out) resolves it in roughly one sub-budget plus one fan-out
    // round instead (well under a second in practice).
    assert!(
        elapsed < Duration::from_secs(3),
        "seed={seed}: cp_route took {elapsed:?} to resolve via the fan-out — expected well \
         under CLIENT_TIMEOUT (10s)"
    );
}
