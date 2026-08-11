//! Fault-injecting acceptance tests for `MetaCommand::RegisterNode` (ADR 0040
//! Decision C — the registration compare-and-swap that retires ADR 0036's
//! `AllocateNodeId` allocator), mirroring `control_raft.rs`'s style: a 3-node
//! control group under `SimEnv`, seed-reproducible.
//!
//! Covers the fault scenarios `node_id_allocation.rs` (this file's ADR 0036
//! predecessor, now deleted) proved for the allocator, adapted to the CAS
//! shape: two proposers racing through the same leader with distinct
//! registrations, a leader killed mid-registration with a same-registration
//! retry, a follower-connected proposer's relay-shaped retry (the direct
//! regression for `animusd`'s `is_relayable_command` allowlist, which must
//! accept `RegisterNode`), and — new to the CAS design — a genuine
//! collision: a second, *different* registration for an already-claimed id
//! is rejected outright, never silently overwriting the first.

use std::time::Duration;

use animus_control::meta::NodeAddrs;
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

/// A `RegisterNode` command claiming `node` with a deterministic address book
/// (keyed off `node`'s own numeric suffix so two distinct test ids never
/// collide by accident) and the same `region=eu-west` label the old allocator
/// test used.
fn register(node: animus_env::NodeId, suffix: u16) -> MetaCommand {
    MetaCommand::RegisterNode {
        node,
        addrs: NodeAddrs {
            internal: format!("127.0.0.1:{}", 9300 + suffix),
            client: format!("127.0.0.1:{}", 9000 + suffix),
            admin: format!("127.0.0.1:{}", 9500 + suffix),
            role: "combined".to_string(),
        },
        labels: [("region".to_string(), "eu-west".to_string())]
            .into_iter()
            .collect(),
    }
}

/// Two proposers, distinct ids, racing through the same leader: both
/// registrations land, and every replica agrees on both — the ordinary
/// total-order-replication property, applied to the command that replaces
/// `AllocateNodeId`'s monotonic-allocator uniqueness with per-id CAS
/// uniqueness instead (there is no shared counter left to race over).
#[test]
fn two_concurrent_registrations_with_distinct_ids_both_succeed() {
    let seed = 0xA110_C1D0;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&nodes, &[0, 1, 2], seed);

    assert!(matches!(
        nodes[leader].propose(register(nid(900), 0)),
        ProposeResult::Accepted { .. }
    ));
    assert!(matches!(
        nodes[leader].propose(register(nid(901), 1)),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));

    for &i in &[0usize, 1, 2] {
        let meta = nodes[i].metadata();
        assert_eq!(
            meta.members.get(&nid(900)).map(|m| m.status),
            Some(NodeStatus::Down),
            "replica {i} (seed={seed})"
        );
        assert_eq!(
            meta.members.get(&nid(901)).map(|m| m.status),
            Some(NodeStatus::Down),
            "replica {i} (seed={seed})"
        );
        assert!(meta.node_addrs.contains_key(&nid(900)));
        assert!(meta.node_addrs.contains_key(&nid(901)));
    }

    // Every replica agrees byte-for-byte.
    let m0 = nodes[0].metadata();
    let m1 = nodes[1].metadata();
    let m2 = nodes[2].metadata();
    assert_eq!(m0.members, m1.members);
    assert_eq!(m1.members, m2.members);
    assert_eq!(m0.node_addrs, m1.node_addrs);
    assert_eq!(m1.node_addrs, m2.node_addrs);
}

/// Leader killed mid-registration: propose, let it commit to a quorum, then
/// kill the leader before confirming — the exact "accepted but unconfirmed"
/// window every proposer here must tolerate (root `CLAUDE.md`'s
/// durable-before-visible discipline). A retry with the **identical**
/// registration against the new leader is a no-op that converges to exactly
/// the one claim already made, never a rejection and never a second entry —
/// this is what makes a proposer's blind retry-after-timeout safe.
#[test]
fn leader_killed_mid_registration_identical_retry_converges() {
    let seed = 0x1EAD_C111;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let old_leader = unique_leader(&nodes, &[0, 1, 2], seed);

    let cmd = register(nid(910), 10);
    assert!(matches!(
        nodes[old_leader].propose(cmd.clone()),
        ProposeResult::Accepted { .. }
    ));
    // Let it replicate to the survivors before the kill, but the proposer
    // itself never observes a confirmation (this call site stands in for
    // the caller crashing/timing out before reading the outcome).
    sim.run_for(Duration::from_secs(1));

    let survivors: Vec<usize> = (0..3).filter(|&i| i != old_leader).collect();
    assert!(
        nodes[survivors[0]]
            .metadata()
            .members
            .contains_key(&nid(910)),
        "committed registration not replicated pre-kill"
    );

    sim.crash(nid(old_leader as u64));
    sim.run_for(Duration::from_secs(3));

    let new_leader = unique_leader(&nodes, &survivors, seed);

    // The retry: the identical registration, against the newly elected leader.
    assert!(matches!(
        nodes[new_leader].propose(cmd),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));

    for &i in &survivors {
        let meta = nodes[i].metadata();
        assert_eq!(
            meta.members.get(&nid(910)).map(|m| m.status),
            Some(NodeStatus::Down),
            "seed={seed}"
        );
    }
    // Exactly one member was ever registered — the retry did not also insert
    // a second, differently-shaped entry under the same id.
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
/// this command specifically) — but retrying the **identical** registration
/// at the hinted leader succeeds, exactly the shape `animusd`'s
/// `ClientCtx::propose_schema` relay takes for every command in
/// `is_relayable_command`'s allowlist (which `RegisterNode` must belong to
/// — this is the `animus-control`-level half of that regression; the
/// `animusd` wire-level half is `tests/seed_join_allocated.rs`'s
/// follower-connected-seed case).
#[test]
fn follower_connected_proposer_relays_via_the_leader_hint() {
    let seed = 0xF011_0EED;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&nodes, &[0, 1, 2], seed);
    let follower = (0..3).find(|&i| i != leader).expect("a follower exists");

    let cmd = register(nid(920), 20);

    // Proposing on the follower is refused, with a hint pointing at the
    // real leader — the exact signal a relay chases.
    match nodes[follower].propose(cmd.clone()) {
        ProposeResult::NotLeader { leader: hint } => {
            assert_eq!(
                hint,
                Some(nid(leader as u64)),
                "the follower's hint must name the real leader (seed={seed})"
            );
        }
        other => panic!("expected NotLeader from a follower, got {other:?} (seed={seed})"),
    }

    // The relay retries the *same* registration at the hinted leader.
    assert!(matches!(
        nodes[leader].propose(cmd),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));

    for &i in &[0usize, 1, 2] {
        assert!(
            nodes[i].metadata().members.contains_key(&nid(920)),
            "replica {i} must observe the relayed registration (seed={seed})"
        );
    }
}

/// **The collision case** (ADR 0040 Decision C, new relative to the ADR 0036
/// allocator this replaces): a second, *different* registration for an
/// already-claimed id is rejected outright — never silently overwritten,
/// and never merged. This is the structural mechanism `animusd`'s
/// re-mint-and-retry (minted id) / fail-loudly (proposed id) caller-side
/// logic both depend on: a minted-id collision retries with a *different*
/// id (proven distinct-id-registration above); a proposed-id collision must
/// see this exact rejection to fail loudly instead of clobbering someone
/// else's claim.
#[test]
fn a_different_registration_for_an_already_claimed_id_is_rejected() {
    let seed = 0xC011_1DE0;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&nodes, &[0, 1, 2], seed);

    assert!(matches!(
        nodes[leader].propose(register(nid(930), 30)),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));
    for &i in &[0usize, 1, 2] {
        assert!(nodes[i].metadata().members.contains_key(&nid(930)));
    }
    let before = nodes[leader].metadata();

    // A different registration for the SAME id (different address book) —
    // accepted into the log (Raft doesn't know it will be rejected at apply
    // time), but its apply must be a no-op on every replica's observable
    // state.
    assert!(matches!(
        nodes[leader].propose(register(nid(930), 31)),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));

    for &i in &[0usize, 1, 2] {
        let meta = nodes[i].metadata();
        assert_eq!(
            meta.node_addrs.get(&nid(930)),
            before.node_addrs.get(&nid(930)),
            "a colliding registration must never overwrite the original claim \
             (replica {i}, seed={seed})"
        );
    }
}
