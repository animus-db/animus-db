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

/// Issue #961 regression: a single logical write/read attempt must be
/// bounded by `CLIENT_TIMEOUT` end to end, even when `cp_route`'s own
/// cross-replica fan-out burns most of that budget before the forward chase
/// that follows it stalls too.
///
/// **The scenario, precisely** (see [`SimCluster::deadline_regression_write`]/
/// [`SimCluster::deadline_regression_read`]'s own doc for the driving
/// mechanism): `victim` (a non-leader replica) is partitioned from BOTH the
/// tablet's real leader and the third replica (`informant`) — its own local
/// leader belief clears (the same 600ms settle this file's sibling test
/// above uses) and every cross-replica fan-out round finds nothing, since
/// `informant` is unreachable too. Partway through the call (`HEAL_AT`,
/// comfortably past several fan-out rounds — most of `CLIENT_TIMEOUT`),
/// `victim`↔`informant` heals — the NEXT fan-out round succeeds, resolving a
/// real, live-vouched-for **hinted** forward to the leader's address. But
/// `victim`↔`leader` is never healed: the forward chase that follows spends
/// the REST of the call's budget stalled on that one hinted hop, exactly the
/// "the following hop stalls on a slow-to-answer replica" shape issue #961
/// names — under `SimEnv` a partitioned peer and a merely slow one are
/// indistinguishable to the relay layer (`RELAY_HOP_TIMEOUT`'s own doc), so
/// this is the deterministic stand-in for a genuinely slow (not dead) stub
/// replica.
///
/// **Before the fix**, `cp_route` and `forward_to_tablet_leader` each minted
/// their own fresh `now + CLIENT_TIMEOUT` — so this one attempt cost roughly
/// (time for `cp_route` to resolve via the fan-out) **plus** a full second
/// `CLIENT_TIMEOUT` for the stalled forward chase, on the order of 1.5–2x
/// the nominal budget. **After the fix**, `cp_route`/`cp_forward`/
/// `forward_to_tablet_leader` all spend from the ONE deadline the caller
/// (`cp_kind_write_raw`/`cp_read`) minted once at the top of its own loop,
/// so the whole attempt is bounded by `CLIENT_TIMEOUT` plus only a small
/// amount of scheduling slack (this fixture's own `STEP` granularity plus
/// `SCHEMA_POLL_INTERVAL`) — never a second hop cap's worth, and never a
/// second full timeout.
mod deadline_budget_tests {
    use std::time::Duration;

    use animus_dynamo::AttributeValue;

    use crate::sim_cluster::SimCluster;

    fn env_seed(default: u64) -> u64 {
        std::env::var("ANIMUS_SEED")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(default)
    }

    /// Every faked-network parameter shared by both scenarios below —
    /// pulled up here once so the write/read variants can't drift apart.
    const HEAL_AT: Duration = Duration::from_secs(5);
    const STEP: Duration = Duration::from_millis(20);
    const MAX_TOTAL: Duration = Duration::from_secs(25);
    /// `CLIENT_TIMEOUT` itself (`crate::CLIENT_TIMEOUT` is private to
    /// `lib.rs`; this crate's own doc convention states it as 10s in every
    /// constant's doc comment — restated here as a plain literal so this
    /// test has no dependency on that visibility).
    const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);
    /// The bound this test actually asserts: `CLIENT_TIMEOUT` plus a few
    /// hundred ms of scheduling slack (this fixture's own `STEP`
    /// granularity, `SCHEMA_POLL_INTERVAL`, and the terminal hop's own
    /// timeout rounding) — explicitly NOT `CLIENT_TIMEOUT` plus a whole
    /// extra hop cap, which is the exact bound issue #961 makes unacceptable.
    const ACCEPTABLE_BOUND: Duration = Duration::from_millis(10_300);

    fn setup(seed: u64) -> (SimCluster, animus_tablet::TabletId, u64, u64, u64) {
        let mut cluster = SimCluster::new(seed, 3, 3);
        let tablet = cluster.create_table("t");
        let leader = cluster.leader_index_of(tablet).unwrap_or_else(|| {
            panic!("seed={seed}: table `t`'s tablet has no leader after create")
        });
        let others: Vec<u64> = (0..3u64).filter(|&n| n != leader).collect();
        let victim = others[0];
        let informant = others[1];
        // Fully isolate `victim` from both other replicas — its own local
        // view clears, and every fan-out round finds nothing until
        // `victim`<->`informant` heals partway through the timed call
        // itself (never `victim`<->`leader`, which stays partitioned for
        // the whole test — that is the stalled hop the forward chase hits).
        cluster.partition(victim, leader);
        cluster.partition(victim, informant);
        cluster.run_for(Duration::from_millis(600));
        (cluster, tablet, leader, victim, informant)
    }

    #[test]
    fn a_stalled_route_then_a_stalled_hop_still_bounds_a_kind_write_raw_attempt_by_client_timeout()
    {
        let seed = env_seed(0x961_0001);
        let (mut cluster, _tablet, leader, victim, informant) = setup(seed);

        let key = crate::dynamo::item_key(
            &AttributeValue::S("pk1".into()),
            Some(&AttributeValue::S("sk1".into())),
        );
        let (elapsed, result) = cluster.deadline_regression_write(
            victim,
            "t",
            key,
            b"v1".to_vec(),
            (victim, informant),
            HEAL_AT,
            STEP,
            MAX_TOTAL,
        );

        // The attempt is expected to fail (the leader is never reachable
        // from `victim` at all) — what this test proves is HOW LONG that
        // failure is allowed to take, not that it succeeds.
        assert!(
            result.is_err(),
            "seed={seed}: expected the write to fail (leader={leader} stays partitioned from \
             victim={victim} for the whole test), got {result:?} after {elapsed:?}"
        );
        assert!(
            elapsed <= ACCEPTABLE_BOUND,
            "seed={seed}: cp_kind_write_raw took {elapsed:?} — expected at most \
             {ACCEPTABLE_BOUND:?} ({CLIENT_TIMEOUT:?} CLIENT_TIMEOUT plus scheduling slack, \
             never a second independent timeout on top of it, issue #961)"
        );
    }

    #[test]
    fn a_stalled_route_then_a_stalled_hop_still_bounds_a_cp_read_attempt_by_client_timeout() {
        let seed = env_seed(0x961_0002);
        let (mut cluster, _tablet, leader, victim, informant) = setup(seed);

        let key = crate::dynamo::item_key(
            &AttributeValue::S("pk1".into()),
            Some(&AttributeValue::S("sk1".into())),
        );
        let (elapsed, result) = cluster.deadline_regression_read(
            victim,
            "t",
            key,
            (victim, informant),
            HEAL_AT,
            STEP,
            MAX_TOTAL,
        );

        assert!(
            result.is_err(),
            "seed={seed}: expected the read to fail (leader={leader} stays partitioned from \
             victim={victim} for the whole test), got {result:?} after {elapsed:?}"
        );
        assert!(
            elapsed <= ACCEPTABLE_BOUND,
            "seed={seed}: cp_read took {elapsed:?} — expected at most {ACCEPTABLE_BOUND:?} \
             ({CLIENT_TIMEOUT:?} CLIENT_TIMEOUT plus scheduling slack, never a second \
             independent timeout on top of it, issue #961)"
        );
    }
}
