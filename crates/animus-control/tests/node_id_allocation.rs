//! Fault-injecting acceptance tests for `MetaCommand::AllocateNodeId` (ADR
//! 0036 — cluster-allocated member ids), mirroring `control_raft.rs`'s style:
//! a 3-node control group under `SimEnv`, seed-reproducible.
//!
//! Covers exactly the three fault scenarios the plan calls for: two proposers
//! racing through the same leader, a leader killed mid-allocation with a
//! same-nonce retry, and a follower-connected proposer's relay-shaped retry
//! (the direct regression for `animusd`'s `is_relayable_command` allowlist,
//! which must accept this command — see `crates/animusd/src/lib.rs`).

use std::time::Duration;

use animus_control::raft::ProposeResult;
use animus_control::{MetaCommand, NodeStatus, RaftNode};
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

const NODES: [u64; 3] = [0, 1, 2];

fn cluster(seed: u64) -> (Simulator, Vec<RaftNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| {
            RaftNode::start(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    (sim, nodes)
}

/// Index of the unique leader among `live` nodes, asserting there is exactly one.
fn unique_leader(nodes: &[RaftNode<SimEnv>], live: &[usize], seed: u64) -> usize {
    let leaders: Vec<usize> = live
        .iter()
        .copied()
        .filter(|&i| nodes[i].is_leader())
        .collect();
    assert_eq!(
        leaders.len(),
        1,
        "expected exactly one leader among {live:?}, found {leaders:?} (seed={seed})"
    );
    leaders[0]
}

fn allocate(nonce: &str) -> MetaCommand {
    MetaCommand::AllocateNodeId {
        nonce: nonce.to_owned(),
        labels: [("region".to_string(), "eu-west".to_string())]
            .into_iter()
            .collect(),
    }
}

/// Two proposers, different nonces, racing through the same leader: both
/// commands land in some order, minting two **distinct** ids, and every
/// replica agrees on both — the ordinary total-order-replication property
/// applied to this specific command, which additionally proves no epoch-CAS
/// or pre-check is needed for uniqueness (the allocator's own monotonic
/// floor is enough).
#[test]
fn racing_allocations_through_the_same_leader_mint_distinct_ids() {
    let seed = 0xA110_C1D0;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&nodes, &[0, 1, 2], seed);

    assert!(matches!(
        nodes[leader].propose(allocate("racer-a")),
        ProposeResult::Accepted { .. }
    ));
    assert!(matches!(
        nodes[leader].propose(allocate("racer-b")),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));

    for &i in &[0usize, 1, 2] {
        let meta = nodes[i].metadata();
        let a = meta
            .node_id_allocations
            .get("racer-a")
            .cloned()
            .unwrap_or_else(|| panic!("racer-a not allocated on replica {i} (seed={seed})"));
        let b = meta
            .node_id_allocations
            .get("racer-b")
            .cloned()
            .unwrap_or_else(|| panic!("racer-b not allocated on replica {i} (seed={seed})"));
        assert_ne!(a, b, "two distinct join attempts must get distinct ids");
        assert_eq!(
            meta.members.get(&a).map(|m| m.status),
            Some(NodeStatus::Down)
        );
        assert_eq!(
            meta.members.get(&b).map(|m| m.status),
            Some(NodeStatus::Down)
        );
    }

    // Every replica's ledger and allocated-id pair agree byte-for-byte.
    let m0 = nodes[0].metadata();
    let m1 = nodes[1].metadata();
    let m2 = nodes[2].metadata();
    assert_eq!(m0.node_id_allocations, m1.node_id_allocations);
    assert_eq!(m1.node_id_allocations, m2.node_id_allocations);
}

/// Leader killed mid-allocation: propose, let it commit to a quorum, then
/// kill the leader before confirming — the exact "accepted but unconfirmed"
/// window every proposer here must tolerate (root `CLAUDE.md`'s
/// durable-before-visible discipline). A retry with the **same nonce**
/// against the new leader converges to exactly the one id already minted,
/// never a second one.
#[test]
fn leader_killed_mid_allocation_same_nonce_retry_converges_to_one_id() {
    let seed = 0x1EAD_C111;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let old_leader = unique_leader(&nodes, &[0, 1, 2], seed);

    assert!(matches!(
        nodes[old_leader].propose(allocate("joiner-1")),
        ProposeResult::Accepted { .. }
    ));
    // Let it replicate to the survivors before the kill, but the proposer
    // itself never observes a confirmation (this call site stands in for
    // the caller crashing/timing out before reading the outcome).
    sim.run_for(Duration::from_secs(1));

    let survivors: Vec<usize> = (0..3).filter(|&i| i != old_leader).collect();
    let pre_kill_id = nodes[survivors[0]]
        .metadata()
        .node_id_allocations
        .get("joiner-1")
        .cloned()
        .expect("committed allocation not replicated pre-kill");

    sim.crash(nid(old_leader as u64));
    sim.run_for(Duration::from_secs(3));

    let new_leader = unique_leader(&nodes, &survivors, seed);

    // The retry: same nonce, against the newly elected leader.
    assert!(matches!(
        nodes[new_leader].propose(allocate("joiner-1")),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));

    for &i in &survivors {
        let meta = nodes[i].metadata();
        let id = meta
            .node_id_allocations
            .get("joiner-1")
            .cloned()
            .expect("joiner-1 must still be allocated after the retry");
        assert_eq!(
            id, pre_kill_id,
            "a same-nonce retry must converge to the original id, not mint a second one \
             (seed={seed})"
        );
    }
    // Exactly one member was ever registered for this nonce's id — the retry
    // did not also insert a second `Down` member under a different id.
    let meta = nodes[survivors[0]].metadata();
    assert_eq!(
        meta.members
            .values()
            .filter(|m| m.status == NodeStatus::Down)
            .count(),
        1,
        "the retry must not have minted a second Down member (seed={seed})"
    );
}

/// A follower-connected proposer: proposing directly on a follower is
/// refused with a leader hint (ordinary `RaftCore` behavior, unrelated to
/// this command specifically) — but retrying the **same nonce** at the
/// hinted leader succeeds, exactly the shape `animusd`'s
/// `ClientCtx::propose_schema` relay takes for every command in
/// `is_relayable_command`'s allowlist (which `AllocateNodeId` must belong
/// to — this is the `animus-control`-level half of that regression; the
/// `animusd` wire-level half is `tests/seed_join_allocated.rs`'s
/// follower-connected-seed case).
#[test]
fn follower_connected_proposer_relays_via_the_leader_hint() {
    let seed = 0xF011_0EED;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&nodes, &[0, 1, 2], seed);
    let follower = (0..3).find(|&i| i != leader).expect("a follower exists");

    // Proposing on the follower is refused, with a hint pointing at the
    // real leader — the exact signal a relay chases.
    match nodes[follower].propose(allocate("via-follower")) {
        ProposeResult::NotLeader { leader: hint } => {
            assert_eq!(
                hint,
                Some(nid(leader as u64)),
                "the follower's hint must name the real leader (seed={seed})"
            );
        }
        other => panic!("expected NotLeader from a follower, got {other:?} (seed={seed})"),
    }

    // The relay retries the *same* nonce at the hinted leader.
    assert!(matches!(
        nodes[leader].propose(allocate("via-follower")),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));

    for &i in &[0usize, 1, 2] {
        assert!(
            nodes[i]
                .metadata()
                .node_id_allocations
                .contains_key("via-follower"),
            "replica {i} must observe the relayed allocation (seed={seed})"
        );
    }
}
