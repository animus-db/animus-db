//! Deterministic `SimCluster` regression for issue #610:
//! `ClientCtx::propose_schema`'s "no locally-known leader" broadcast
//! fallback (`schema.rs`) must race every known intra candidate
//! concurrently, never serially — a serial loop pays up to
//! `(N-1) * FORWARD_HOP_TIMEOUT` for a *single* call whenever an unlucky
//! candidate ordering tries an unreachable member before a reachable one,
//! which a real `CreateTable`'s own outer `SCHEMA_COMMIT_TIMEOUT` (5s,
//! `dynamo.rs`) has little headroom to absorb.
//!
//! Pins the exact real-world race the issue names: `await_bootstrap` (every
//! `ProdEnv` cluster fixture's own barrier) only guarantees *some* node is
//! control leader and every node has non-empty membership — never that the
//! node a client's first `CreateTable` happens to reach already knows
//! *who* leads. Here that is reproduced without any timing luck: partition
//! a follower from the (pinned) leader for just past one election window,
//! exactly `leader_within_hysteresis.rs`'s own repro shape for issue #595
//! (`start_pre_vote` clears `leader_id` unconditionally on a bare
//! election-timer expiry, before any pre-vote is even answered) — the
//! follower's `leader()` clears while the leader keeps its majority via the
//! third (witness) node and never steps down. The partition is never
//! healed: it stays permanent for the rest of the test, which is why this
//! measures [`SimCluster::propose_schema_fast`] directly (bypassing a full
//! wire `CreateTable`'s own confirmation-poll loop) rather than a full
//! `CreateTable` round trip — that loop needs THIS node's own Raft log to
//! observe the commit, which a permanently-partitioned node structurally
//! never can, no matter how quickly `propose_schema` itself reaches a
//! leader through a relay. That is a separate, correct limitation, not
//! part of what issue #610 is about.
//!
//! With the leader pinned to node 0 (`SimCluster::transfer_control_
//! leadership_to`), the partitioned follower's own **intra** candidate
//! order (`BTreeMap<NodeId, _>`, i.e. ascending node id) always tries the
//! leader FIRST, `node 0` sorting below every other id — so a pre-fix
//! serial loop hits the one candidate this follower cannot reach before
//! ever trying the witness that could actually relay onward. The witness
//! (node 2) stays fully connected to both, so `propose_schema` from the
//! follower succeeds either way; only the elapsed **virtual** time tells
//! the two implementations apart.
//!
//! Seed replay (repo convention): `ANIMUS_SEED=<seed> cargo test -p animusd
//! --lib <test name>`.

use std::time::Duration;

use animus_control::{ColumnType, MetaCommand, TableSchema};

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

#[test]
fn propose_schema_from_a_leaderless_follower_does_not_wait_out_an_unreachable_broadcast_candidate()
{
    let seed = env_seed(0x6100_0001);
    let mut cluster = SimCluster::new(seed, 3, 3);

    // Pin leadership to node 0: this fixture's own intra broadcast order is
    // ascending node id, so node 0 always sorts first among any follower's
    // candidates — the worst possible ordering for a serial loop once node
    // 0 is the one candidate a given follower cannot reach.
    cluster.transfer_control_leadership_to(0);
    let leader = 0u64;
    let follower = 1u64;
    let witness = 2u64;

    // Partition the follower from the leader ONLY, in both directions — it
    // hears no more heartbeats and its own pre-vote traffic can't reach the
    // leader either. The witness stays fully connected to both, so the
    // leader keeps its majority (leader + witness) and never steps down —
    // `leader_within_hysteresis.rs`'s own repro shape for issue #595. Never
    // healed: `propose_schema_fast` below is measured while this partition
    // is still fully in effect.
    cluster.partition(follower, leader);

    // Run just past one election window so the follower's own election
    // timer has fired and cleared its `leader()` belief (`start_pre_vote`),
    // comfortably short of any health-readiness grace (not under test
    // here — this is `RaftCore::leader()`, the same raw belief
    // `ClientCtx::propose_schema` reads).
    let window = cluster.control_election_timeout(follower) * 2 + Duration::from_millis(50);
    cluster.run_for(window);
    assert!(
        !cluster.control_knows_leader(follower),
        "seed={seed}: node {follower} should have cleared its own leader() belief \
         after {window:?} partitioned from the leader"
    );
    assert!(
        cluster.control_knows_leader(witness),
        "seed={seed}: the fully-connected witness should still see the healthy leader"
    );

    // The actual regression: a schema-catalog DDL proposed from the
    // now-leaderless follower — the same command shape (`CreateTableSchema`)
    // and relay path `dynamo.rs`'s real `CreateTable` handler uses. The
    // follower has no local leadership, no intra-leader-address hint (a
    // `Local` control handle never has one), and its own raw `leader()` is
    // `None` — so `propose_schema` falls straight to the broadcast
    // fallback, whose only two candidates are {leader, witness}.
    let command = MetaCommand::CreateTableSchema {
        table: "first_after_bootstrap".to_owned(),
        schema: TableSchema::composite("pk", ColumnType::String, "sk", ColumnType::String),
    };
    let t0 = cluster.sim_now();
    let accepted = cluster.propose_schema_fast(follower, command);
    let elapsed = cluster.sim_now().duration_since(t0);
    assert!(
        accepted,
        "seed={seed}: propose_schema from the leaderless follower never reached a leader \
         (relayed through the witness, node {witness})"
    );

    // The decisive assertion: a serial broadcast that tries the
    // unreachable leader FIRST (this fixture's own ascending-id order)
    // would burn a full `FORWARD_HOP_TIMEOUT` (2s) hop on that dead end
    // before ever trying the witness — issue #610's exact "one or two slow
    // hops... exhaust it" shape, against `dynamo.rs`'s 5s
    // `SCHEMA_COMMIT_TIMEOUT`. Racing every candidate concurrently
    // resolves via the witness alone, well under one second of virtual
    // time. `propose_schema_fast`'s own polling granularity is 100ms
    // (`SPAWN_CAPTURE_FAST_STEP`), so this bound is generous, not
    // hair-trigger.
    assert!(
        elapsed < Duration::from_secs(1),
        "seed={seed}: propose_schema took {elapsed:?} from a leaderless follower whose first \
         broadcast candidate (the leader) is unreachable — as long as waiting out that dead \
         candidate's own FORWARD_HOP_TIMEOUT, meaning the broadcast fallback is still \
         serialized instead of racing every candidate concurrently (issue #610)"
    );
}
