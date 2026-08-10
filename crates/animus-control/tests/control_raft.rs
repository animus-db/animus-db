//! M3 acceptance tests for the control-plane Raft RSM.
//!
//! Under `SimEnv`, a 3-node control group elects a leader, applies metadata
//! transitions in total order, and survives a leader kill (re-electing with no
//! metadata divergence). Compare-and-swap epoch transactions are enforced
//! deterministically, and the whole run is byte-reproducible from its seed.

use std::time::Duration;

use animus_control::raft::ProposeResult;
use animus_control::{MetaCommand, Metadata, NodeStatus, RaftNode};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{Epoch, KeyRange, TabletId};

const NODES: [u64; 3] = [0, 1, 2];

fn cluster(seed: u64) -> (Simulator, Vec<RaftNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| RaftNode::start(sim.env(id), NODES.to_vec(), MemoryEngine::new()))
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

fn upsert(node: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node,
        labels: [("region".to_string(), "eu-west".to_string())]
            .into_iter()
            .collect(),
        status: NodeStatus::Active,
    }
}

#[test]
fn elects_a_single_stable_leader() {
    let seed = 0xC0_FFEE;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));

    let leader = unique_leader(&nodes, &[0, 1, 2], seed);
    let term = nodes[leader].term();
    // All nodes agree on the term and recognize the same leader.
    for n in &nodes {
        assert_eq!(n.term(), term, "term disagreement (seed={seed})");
        assert_eq!(
            n.leader(),
            Some(leader as u64),
            "leader disagreement (seed={seed})"
        );
    }
}

#[test]
fn replicates_metadata_in_total_order() {
    let seed = 0xABCD_1234;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&nodes, &[0, 1, 2], seed);

    // Propose a sequence of commands to the leader.
    for &id in &NODES {
        assert!(matches!(
            nodes[leader].propose(upsert(id)),
            ProposeResult::Accepted { .. }
        ));
    }
    assert!(matches!(
        nodes[leader].propose(MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: None,
            range: KeyRange::whole(),
            replicas: NODES.to_vec(),
        }),
        ProposeResult::Accepted { .. }
    ));

    // A non-leader must refuse and hint at the leader.
    let follower = (0..3).find(|&i| i != leader).unwrap();
    assert_eq!(
        nodes[follower].propose(upsert(99)),
        ProposeResult::NotLeader {
            leader: Some(leader as u64)
        }
    );

    sim.run_for(Duration::from_secs(2));

    // Every node applied the same commands in the same order — proven by
    // identical resulting metadata (ADR 0038 PR3: `Metadata` is
    // `DRIVER_APPLIED`, so there is no per-core `applied()` command list to
    // compare directly anymore; the apply task's published cache is the
    // observable convergence point).
    let reference_meta = nodes[leader].metadata();
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            n.metadata(),
            reference_meta,
            "node {i} metadata diverged (seed={seed})"
        );
    }
    // The membership actually landed.
    assert_eq!(reference_meta.members.len(), 3);
    assert!(reference_meta.tablets.contains_key(&TabletId(1)));
}

#[test]
fn survives_leader_kill_without_divergence() {
    let seed = 0x5EED_5EED;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let old_leader = unique_leader(&nodes, &[0, 1, 2], seed);
    let old_term = nodes[old_leader].term();

    // Commit a command, then let it replicate to the followers before the kill.
    assert!(matches!(
        nodes[old_leader].propose(upsert(7)),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(1));

    let survivors: Vec<usize> = (0..3).filter(|&i| i != old_leader).collect();
    let pre_kill_meta = nodes[survivors[0]].metadata();
    assert!(
        pre_kill_meta.members.contains_key(&7),
        "committed write not replicated pre-kill"
    );

    // Kill the leader; the survivors must re-elect among themselves.
    sim.crash(old_leader as u64);
    sim.run_for(Duration::from_secs(3));

    let new_leader = unique_leader(&nodes, &survivors, seed);
    assert!(
        survivors.contains(&new_leader),
        "new leader is not a survivor (seed={seed})"
    );
    assert!(
        nodes[new_leader].term() > old_term,
        "new term should exceed the old one (seed={seed})"
    );

    // Survivors agree, and the pre-kill committed write is preserved.
    let a = nodes[survivors[0]].metadata();
    let b = nodes[survivors[1]].metadata();
    assert_eq!(
        a, b,
        "survivor metadata diverged after leader kill (seed={seed})"
    );
    assert!(
        a.members.contains_key(&7),
        "acknowledged write lost across leader kill (seed={seed})"
    );

    // The new leader can still make progress.
    assert!(matches!(
        nodes[new_leader].propose(upsert(8)),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));
    assert!(nodes[survivors[0]].metadata().members.contains_key(&8));
    assert!(nodes[survivors[1]].metadata().members.contains_key(&8));
}

#[test]
fn cas_epoch_transactions_are_enforced() {
    let seed = 0xCA5_CA5;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&nodes, &[0, 1, 2], seed);

    nodes[leader].propose(MetaCommand::CreateTablet {
        tablet: TabletId(1),
        table: None,
        range: KeyRange::whole(),
        replicas: vec![0, 1, 2],
    });
    // A correct-epoch CAS, then a stale-epoch CAS (must be rejected on apply).
    nodes[leader].propose(MetaCommand::CasTabletReplicas {
        tablet: TabletId(1),
        expected_epoch: Epoch::INITIAL,
        replicas: vec![0, 1],
    });
    nodes[leader].propose(MetaCommand::CasTabletReplicas {
        tablet: TabletId(1),
        expected_epoch: Epoch::INITIAL, // stale: epoch is now 2
        replicas: vec![2],
    });
    sim.run_for(Duration::from_secs(2));

    let tablet = &nodes[leader].metadata().tablets[&TabletId(1)];
    assert_eq!(
        tablet.epoch,
        Epoch(2),
        "successful CAS should bump the epoch once"
    );
    assert_eq!(
        tablet.replicas,
        vec![0, 1],
        "stale CAS must not have taken effect"
    );

    // Deterministic across replicas.
    for n in &nodes {
        assert_eq!(&n.metadata().tablets[&TabletId(1)], tablet);
    }
}

#[test]
fn converges_across_many_seeds() {
    // Guard against passing only on hand-picked seeds: every seed must elect a
    // single leader within a bounded time and converge metadata after writes.
    for seed in 0..64u64 {
        let (mut sim, nodes) = cluster(seed);
        sim.run_for(Duration::from_secs(3));
        let leader = unique_leader(&nodes, &[0, 1, 2], seed);

        for &id in &NODES {
            nodes[leader].propose(upsert(id));
        }
        sim.run_for(Duration::from_secs(2));

        let reference = nodes[leader].metadata();
        for (i, n) in nodes.iter().enumerate() {
            assert_eq!(n.metadata(), reference, "seed {seed}: node {i} diverged");
        }
        assert_eq!(
            reference.members.len(),
            3,
            "seed {seed}: writes did not all commit"
        );
    }
}

#[test]
fn run_is_byte_reproducible_from_seed() {
    let seed = 0xD37E_8E5D;

    fn scenario(seed: u64) -> (Vec<String>, Metadata) {
        let (mut sim, nodes) = cluster(seed);
        sim.run_for(Duration::from_secs(2));
        let leader = unique_leader(&nodes, &[0, 1, 2], seed);
        for &id in &NODES {
            nodes[leader].propose(upsert(id));
        }
        sim.run_for(Duration::from_secs(2));
        (sim.trace_lines(), nodes[leader].metadata())
    }

    let (trace_a, meta_a) = scenario(seed);
    let (trace_b, meta_b) = scenario(seed);
    assert_eq!(
        trace_a, trace_b,
        "control-plane run was not byte-reproducible (seed={seed})"
    );
    assert_eq!(meta_a, meta_b);
    assert!(!trace_a.is_empty());
}
