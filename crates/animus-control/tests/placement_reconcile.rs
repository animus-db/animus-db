//! Placement driving real control-plane consensus (ADR 0005 + 0009).
//!
//! `animus-placement` is a pure policy engine; this test wires it to the
//! Raft-replicated metadata under simulation: a tablet is placed across EU
//! failure domains, a replica node dies, and the surviving cluster commits a
//! `CasTabletReplicas` — computed by `replan` — that replaces *only* the dead
//! replica while preserving residency and spread. A control follower is crashed
//! mid-reconcile to prove the placement transaction still commits on a quorum,
//! and the whole run is reproducible from its seed.

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::raft::ProposeResult;
use animus_control::{MetaCommand, Metadata, NodeStatus, RaftNode};
use animus_placement::{Candidate, PlacementPolicy, replan, select_replicas};
use animus_sim::{SimEnv, Simulator};
use animus_tablet::{KeyRange, TabletId};

const CONTROL: [u64; 3] = [0, 1, 2];
const TABLET: TabletId = TabletId(1);

/// Data nodes available for placement: `(id, region, zone)`. Six EU nodes
/// across three zones, plus one US node that residency must always exclude.
const DATA_NODES: [(u64, &str, &str); 7] = [
    (10, "eu", "a"),
    (11, "eu", "a"),
    (12, "eu", "b"),
    (13, "eu", "b"),
    (14, "eu", "c"),
    (15, "eu", "c"),
    (20, "us", "a"),
];

fn cluster(seed: u64) -> (Simulator, Vec<RaftNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes = CONTROL
        .iter()
        .map(|&id| RaftNode::start(sim.env(id), CONTROL.to_vec()))
        .collect();
    (sim, nodes)
}

fn leader_among(nodes: &[RaftNode<SimEnv>], live: &[usize]) -> usize {
    let leaders: Vec<usize> = live
        .iter()
        .copied()
        .filter(|&i| nodes[i].is_leader())
        .collect();
    assert_eq!(
        leaders.len(),
        1,
        "expected one leader among {live:?}, got {leaders:?}"
    );
    leaders[0]
}

fn labels(region: &str, zone: &str) -> BTreeMap<String, String> {
    [("region", region), ("zone", zone)]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// Candidates for placement: the `Active` members and their labels.
fn active_candidates(meta: &Metadata) -> Vec<Candidate> {
    meta.members
        .iter()
        .filter(|(_, m)| m.status == NodeStatus::Active)
        .map(|(id, m)| Candidate::new(*id, m.labels.clone()))
        .collect()
}

fn zone_of(meta: &Metadata, node: u64) -> String {
    meta.members[&node].labels["zone"].clone()
}

#[test]
fn placement_survives_a_replica_death_and_reconciles_through_raft() {
    run(0x0EE5_0005);
}

fn run(seed: u64) {
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = leader_among(&nodes, &[0, 1, 2]);

    // Register every data node as an Active member with its topology labels.
    for (id, region, zone) in DATA_NODES {
        assert!(matches!(
            nodes[leader].propose(MetaCommand::UpsertMember {
                node: id,
                labels: labels(region, zone),
                status: NodeStatus::Active,
            }),
            ProposeResult::Accepted { .. }
        ));
    }
    sim.run_for(Duration::from_secs(1));

    // Place a 3-replica tablet pinned to EU, one replica per zone.
    let policy = PlacementPolicy::simple("eu-rf3", 3)
        .require_label("region", "eu")
        .spread_across("zone", true);
    let meta = nodes[leader].metadata();
    let replicas = select_replicas(&active_candidates(&meta), &policy).unwrap();
    assert_eq!(replicas.len(), 3);
    assert!(
        replicas.iter().all(|n| (10..20).contains(n)),
        "placed outside EU: {replicas:?} (seed={seed})"
    );
    let mut zones: Vec<String> = replicas.iter().map(|n| zone_of(&meta, *n)).collect();
    zones.sort();
    assert_eq!(
        zones,
        vec!["a", "b", "c"],
        "tablet not spread across zones (seed={seed})"
    );

    assert!(matches!(
        nodes[leader].propose(MetaCommand::CreateTablet {
            tablet: TABLET,
            range: KeyRange::whole(),
            replicas: replicas.clone(),
        }),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(1));

    // --- Fault: a placed replica dies, and a control follower crashes. ---
    let current = nodes[leader].metadata().tablets[&TABLET].replicas.clone();
    let dead = current[0];
    let dead_zone = zone_of(&nodes[leader].metadata(), dead);

    assert!(matches!(
        nodes[leader].propose(MetaCommand::UpsertMember {
            node: dead,
            labels: nodes[leader].metadata().members[&dead].labels.clone(),
            status: NodeStatus::Down,
        }),
        ProposeResult::Accepted { .. }
    ));
    // Crash a control follower; the leader + the other follower remain a quorum.
    let crashed = (0..3).find(|&i| i != leader).unwrap();
    sim.crash(crashed as u64);
    sim.run_for(Duration::from_secs(1));

    // Reconcile: keep the survivors, replace only the dead replica.
    let meta = nodes[leader].metadata();
    let new = replan(&current, &active_candidates(&meta), &policy).unwrap();
    assert!(
        !new.contains(&dead),
        "dead replica not replaced (seed={seed})"
    );
    for kept in current.iter().filter(|n| **n != dead) {
        assert!(
            new.contains(kept),
            "survivor {kept} needlessly moved (seed={seed})"
        );
    }

    let epoch = meta.tablets[&TABLET].epoch;
    let leader2 = leader_among(
        &nodes,
        &[
            leader,
            (0..3).find(|&i| i != leader && i != crashed).unwrap(),
        ],
    );
    assert!(matches!(
        nodes[leader2].propose(MetaCommand::CasTabletReplicas {
            tablet: TABLET,
            expected_epoch: epoch,
            replicas: new.clone(),
        }),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));

    // The reconcile committed: the new set is in place, the dead node is gone,
    // and residency + spread still hold — on every surviving control node.
    let mut expected = new.clone();
    expected.sort_unstable();
    for i in (0..3).filter(|&i| i != crashed) {
        let m = nodes[i].metadata();
        let placed = &m.tablets[&TABLET].replicas;
        assert_eq!(
            placed, &expected,
            "node {i} has wrong replica set (seed={seed})"
        );
        assert!(
            m.tablets[&TABLET].epoch > epoch,
            "epoch not bumped (seed={seed})"
        );
        assert!(!placed.contains(&dead));
        let mut zs: Vec<String> = placed.iter().map(|n| zone_of(&m, *n)).collect();
        zs.sort();
        zs.dedup();
        assert_eq!(zs.len(), 3, "spread lost after reconcile (seed={seed})");
        assert!(
            placed.iter().all(|n| (10..20).contains(n)),
            "residency lost (seed={seed})"
        );
    }
    // The replacement came from the dead replica's zone (minimal, like-for-like).
    let replacement = *new.iter().find(|n| !current.contains(n)).unwrap();
    assert_eq!(
        zone_of(&nodes[leader].metadata(), replacement),
        dead_zone,
        "replacement should reuse the dead replica's zone (seed={seed})"
    );
}

#[test]
fn reconcile_is_reproducible_from_seed() {
    // Two runs of the same scenario commit identical traces.
    fn trace(seed: u64) -> Vec<String> {
        let (mut sim, nodes) = cluster(seed);
        sim.run_for(Duration::from_secs(2));
        let leader = leader_among(&nodes, &[0, 1, 2]);
        for (id, region, zone) in DATA_NODES {
            nodes[leader].propose(MetaCommand::UpsertMember {
                node: id,
                labels: labels(region, zone),
                status: NodeStatus::Active,
            });
        }
        sim.run_for(Duration::from_secs(1));
        sim.trace_lines()
    }
    assert_eq!(trace(0x5EED_0005), trace(0x5EED_0005));
}
