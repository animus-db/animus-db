//! Root-cause repro for PR #661's kind e2e failure (S-07d `spec.controlNodes`
//! growth on `storage.ephemeral: true`): a StatefulSet pod whose ordinal is
//! already an established control voter is deleted and recreated on an
//! **empty** volume (a real `EmptyDir` wipes the WAL *and* the engine, not
//! just the engine the way `animus-cp-data`'s own
//! `engine_wipe_needs_snapshot.rs` covers). `RaftNode::start_with_orphan_
//! sweep_after` treats any node whose persisted control storage is empty as
//! a **genesis bootstrap** participant (`node.rs`'s `drive`: `if
//! !state.is_empty() { RaftCore::recovered(..) }`, else the fresh
//! `RaftCore::new` built at `start` time is kept) — safe by construction for
//! a *never-before-voter* ordinal being promoted (ADR 0060's "Why growth
//! doesn't need genesis's own sequential join answer"), since pre-vote can
//! never let it win a real election against the established group's
//! far-ahead log. This file asks the same question for the *other* case ADR
//! 0060 didn't need to answer: an ordinal that is **already** a live voter
//! in the group's own committed config, wiped and restarted mid-rollout.
//!
//! Uses [`animus_sim::Simulator::wipe_disk`] (added for this repro) to model
//! an `EmptyDir` wipe precisely: unlike `crash`/`stop`, which both
//! deliberately preserve durable disk (a real process restart on persistent
//! storage), `wipe_disk` drops every byte, so the freshly-started
//! `RaftNode` reads back a genuinely empty WAL — exactly `drive`'s
//! `state.is_empty()` branch a recreated ephemeral pod hits.

use std::collections::BTreeSet;
use std::time::Duration;

use animus_control::{MetaCommand, NodeStatus, ProposeResult, RaftNode};
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

fn upsert(node: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node: nid(node),
        labels: Default::default(),
        status: NodeStatus::Active,
    }
}

fn set(ids: &[u64]) -> BTreeSet<animus_env::NodeId> {
    ids.iter().copied().map(nid).collect()
}

fn unique_leader(nodes: &[RaftNode<SimEnv>], live: &[usize], seed: u64, when: &str) -> usize {
    let leaders: Vec<usize> = live
        .iter()
        .copied()
        .filter(|&i| nodes[i].is_leader())
        .collect();
    assert_eq!(
        leaders.len(),
        1,
        "{when}: expected exactly one leader among {live:?}, found {leaders:?} (seed={seed})"
    );
    leaders[0]
}

/// Mirrors `animusd::admin::health`'s own grace window
/// (`HEALTH_LEADER_GRACE_ELECTION_TIMEOUTS = 3`) — the e2e's actual liveness
/// probe gate.
fn health_ok(nodes: &[RaftNode<SimEnv>], live: &[usize]) -> bool {
    live.iter().all(|&i| {
        let grace = nodes[i].election_timeout() * 3;
        nodes[i].leader_within(grace).is_some()
    })
}

/// The scenario in the e2e trace: a 3-voter control group grows to 4 via
/// `change_membership` (the same primitive `POST /admin/control/member/add`
/// drives), converges, and *then* the group's existing voter 2 — forced to
/// be the current leader, the worst case the incident log calls out — is
/// wiped (WAL + engine both gone, as `storage.ephemeral: true` produces) and
/// restarted fresh on the same id with the grown 4-node bootstrap config.
/// The rest of the group (0, 1, 3) is never touched. Expect: a leader
/// re-establishes and every live node's own `leader_within` (the e2e's
/// `/admin/health` gate) recovers within a bounded number of election
/// timeouts, and a further write commits.
#[test]
fn growth_then_wiped_leader_rejoin_reestablishes_leader() {
    let seed = 0x6154_0001;
    let mut sim = Simulator::new(seed);
    let base_ids = [0u64, 1, 2];
    let all_ids = [0u64, 1, 2, 3];

    let mut nodes: Vec<RaftNode<SimEnv>> = base_ids
        .iter()
        .map(|&id| {
            RaftNode::start(
                sim.env(nid(id)),
                set(&base_ids).into_iter().collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    sim.run_for(Duration::from_secs(2));
    let l = unique_leader(&nodes, &[0, 1, 2], seed, "pre-growth");
    assert!(
        matches!(
            nodes[l].propose(upsert(100)),
            ProposeResult::Accepted { .. }
        ),
        "seed={seed}"
    );
    sim.run_for(Duration::from_secs(1));

    // Grow to 4, exactly like the operator's `add_control_voter`.
    let node3 = RaftNode::start(
        sim.env(nid(3)),
        set(&all_ids).into_iter().collect(),
        MemoryEngine::new(),
    );
    assert!(
        matches!(
            nodes[l].change_membership(set(&all_ids)),
            ProposeResult::Accepted { .. }
        ),
        "seed={seed}: growth to 4 voters should be accepted"
    );
    nodes.push(node3);
    sim.run_for(Duration::from_secs(3));
    assert_eq!(
        nodes[3].config(),
        set(&all_ids),
        "seed={seed}: node 3 should have adopted the grown config"
    );

    // Force node 2 to be the leader — the worst case the incident calls out
    // ("possibly the control leader").
    let cur = unique_leader(&nodes, &[0, 1, 2, 3], seed, "post-growth");
    if cur != 2 {
        assert!(
            nodes[cur].transfer_leadership(nid(2)),
            "seed={seed}: transfer to 2 should arm"
        );
        sim.run_for(Duration::from_secs(2));
    }
    assert!(
        nodes[2].is_leader(),
        "seed={seed}: node 2 must be leader before the wipe"
    );

    // The StatefulSet roll: delete node 2's process, wipe its ephemeral
    // volume, recreate it fresh with the *grown* 4-node bootstrap config —
    // exactly `animusd --config cluster.json --node 2` reading an empty
    // EmptyDir mount. Nodes 0, 1, 3 are never touched.
    sim.stop(nid(2));
    sim.wipe_disk(nid(2));
    nodes[2] = RaftNode::start(
        sim.env(nid(2)),
        set(&all_ids).into_iter().collect(),
        MemoryEngine::new(),
    );

    // Bounded budget: the e2e observed a 60s+ stall against a 150ms election
    // base: a healthy rejoin should resolve in a handful of election
    // timeouts, generously 15s of simulated time.
    sim.run_for(Duration::from_secs(15));

    let live = [0, 1, 2, 3];
    let leaders: Vec<usize> = live
        .iter()
        .copied()
        .filter(|&i| nodes[i].is_leader())
        .collect();
    assert_eq!(
        leaders.len(),
        1,
        "seed={seed}: expected exactly one leader after the wiped leader's rejoin, found {leaders:?} \
         (replay: ANIMUS_SEED={seed} cargo test -p animus-control --test wiped_voter_rejoin \
         growth_then_wiped_leader_rejoin_reestablishes_leader)"
    );
    assert!(
        health_ok(&nodes, &live),
        "seed={seed}: every live node's leader_within (the e2e /admin/health gate) must hold \
         within 3 election timeouts after the wiped leader's rejoin"
    );

    let new_leader = leaders[0];
    assert!(
        matches!(
            nodes[new_leader].propose(upsert(200)),
            ProposeResult::Accepted { .. }
        ),
        "seed={seed}: a write must still commit after the wiped leader's rejoin"
    );
    sim.run_for(Duration::from_secs(2));
    assert!(
        nodes[new_leader].metadata().members.contains_key(&nid(200)),
        "seed={seed}: the post-rejoin write must actually commit"
    );
}

/// Same shape, but node 2 rejoins as a **follower**, not the leader — the
/// other case the incident's own evidence names as plausible.
#[test]
fn growth_then_wiped_follower_rejoin_reestablishes_leader() {
    let seed = 0x6154_0002;
    let mut sim = Simulator::new(seed);
    let base_ids = [0u64, 1, 2];
    let all_ids = [0u64, 1, 2, 3];

    let mut nodes: Vec<RaftNode<SimEnv>> = base_ids
        .iter()
        .map(|&id| {
            RaftNode::start(
                sim.env(nid(id)),
                set(&base_ids).into_iter().collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    sim.run_for(Duration::from_secs(2));
    let l = unique_leader(&nodes, &[0, 1, 2], seed, "pre-growth");
    sim.run_for(Duration::from_secs(1));

    let node3 = RaftNode::start(
        sim.env(nid(3)),
        set(&all_ids).into_iter().collect(),
        MemoryEngine::new(),
    );
    assert!(
        matches!(
            nodes[l].change_membership(set(&all_ids)),
            ProposeResult::Accepted { .. }
        ),
        "seed={seed}"
    );
    nodes.push(node3);
    sim.run_for(Duration::from_secs(3));

    let cur = unique_leader(&nodes, &[0, 1, 2, 3], seed, "post-growth");
    // Pick any voter that is *not* the current leader — the "wiped leader"
    // case is already covered by
    // `growth_then_wiped_leader_rejoin_reestablishes_leader`, so this test's
    // whole point is exercising the other one.
    let victim = [0usize, 1, 2, 3]
        .into_iter()
        .find(|&i| i != cur)
        .expect("4 voters, only one of which is the leader");
    assert_ne!(
        cur, victim,
        "seed={seed}: victim must not be the current leader"
    );

    sim.stop(nid(victim as u64));
    sim.wipe_disk(nid(victim as u64));
    nodes[victim] = RaftNode::start(
        sim.env(nid(victim as u64)),
        set(&all_ids).into_iter().collect(),
        MemoryEngine::new(),
    );

    sim.run_for(Duration::from_secs(15));

    let live = [0, 1, 2, 3];
    let leaders: Vec<usize> = live
        .iter()
        .copied()
        .filter(|&i| nodes[i].is_leader())
        .collect();
    assert_eq!(
        leaders.len(),
        1,
        "seed={seed}: expected exactly one leader after wiped follower {victim}'s rejoin, found {leaders:?} \
         (replay: ANIMUS_SEED={seed} cargo test -p animus-control --test wiped_voter_rejoin \
         growth_then_wiped_follower_rejoin_reestablishes_leader)"
    );
    assert!(
        health_ok(&nodes, &live),
        "seed={seed}: every live node's leader_within must hold after the wiped follower's rejoin"
    );
}

/// Control: is the wiped-voter rejoin itself broken, independent of growth?
/// A plain 3-node group, no `change_membership` ever proposed, one voter
/// wiped and restarted fresh on the same id/bootstrap config.
#[test]
fn wiped_voter_rejoin_without_growth_reestablishes_leader() {
    let seed = 0x6154_0003;
    let mut sim = Simulator::new(seed);
    let ids = [0u64, 1, 2];

    let mut nodes: Vec<RaftNode<SimEnv>> = ids
        .iter()
        .map(|&id| {
            RaftNode::start(
                sim.env(nid(id)),
                set(&ids).into_iter().collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    sim.run_for(Duration::from_secs(2));
    let l = unique_leader(&nodes, &[0, 1, 2], seed, "initial");
    assert!(
        matches!(nodes[l].propose(upsert(1)), ProposeResult::Accepted { .. }),
        "seed={seed}"
    );
    sim.run_for(Duration::from_secs(1));

    let victim = (0..3).find(|&i| i != l).expect("a non-leader voter");
    sim.stop(nid(victim as u64));
    sim.wipe_disk(nid(victim as u64));
    nodes[victim] = RaftNode::start(
        sim.env(nid(victim as u64)),
        set(&ids).into_iter().collect(),
        MemoryEngine::new(),
    );

    sim.run_for(Duration::from_secs(10));

    let live = [0, 1, 2];
    let leaders: Vec<usize> = live
        .iter()
        .copied()
        .filter(|&i| nodes[i].is_leader())
        .collect();
    assert_eq!(
        leaders.len(),
        1,
        "seed={seed}: expected exactly one leader after wiped voter {victim}'s rejoin (no growth involved), \
         found {leaders:?} (replay: ANIMUS_SEED={seed} cargo test -p animus-control --test wiped_voter_rejoin \
         wiped_voter_rejoin_without_growth_reestablishes_leader)"
    );
    assert!(
        health_ok(&nodes, &live),
        "seed={seed}: every live node's leader_within must hold after the wiped voter's rejoin"
    );
}
