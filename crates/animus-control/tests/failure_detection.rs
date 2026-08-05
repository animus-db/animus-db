//! Heartbeat-based failure detection, end to end (ADR 0012 + 0005 + 0009).
//!
//! Member nodes heartbeat the control group on an `Env` timer; the leader's pure
//! [`FailureDetector`](animus_control::FailureDetector) classifies them and, when
//! leader, proposes `UpsertMember{status}` transitions through real Raft. This
//! test stands up a 3-node control cluster plus seven data members that
//! heartbeat, then:
//!
//! 1. crashes one member (its heartbeats stop) and asserts the leader detects the
//!    silence and **commits `Down`** for it on every control node;
//! 2. ties it to placement: a tablet pinned to the member's failure domain is
//!    **auto-reconciled off the dead member** (residency + spread preserved,
//!    survivors kept) — the cascade is driven entirely by the detected failure,
//!    with no test-side `replan`/CAS and no manual `Down`;
//! 3. restarts the member (heartbeats resume) and asserts it returns to `Active`.
//!
//! The whole run is a pure function of its seed.

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::node::heartbeat_loop;
use animus_control::raft::ProposeResult;
use animus_control::{MetaCommand, Metadata, NodeStatus, RaftNode};
use animus_env::EnvExt;
use animus_placement::PlacementPolicy;
use animus_sim::{SimEnv, Simulator};
use animus_tablet::{KeyRange, TabletId};

const CONTROL: [u64; 3] = [0, 1, 2];
const TABLET: TabletId = TabletId(1);

/// Data members: `(id, region, zone)`. Six EU nodes across three zones, plus one
/// US node residency must exclude.
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
    let nodes: Vec<RaftNode<SimEnv>> = CONTROL
        .iter()
        .map(|&id| RaftNode::start(sim.env(id), CONTROL.to_vec()))
        .collect();
    // Every data member heartbeats the whole control group on a timer.
    for (id, _, _) in DATA_NODES {
        let env = sim.env(id);
        env.spawn_task(heartbeat_loop(env.clone(), CONTROL.to_vec()));
    }
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

fn zone_of(meta: &Metadata, node: u64) -> String {
    meta.members[&node].labels["zone"].clone()
}

fn policy() -> PlacementPolicy {
    PlacementPolicy::simple("eu-rf3", 3)
        .require_label("region", "eu")
        .spread_across("zone", true)
}

fn status_of(meta: &Metadata, node: u64) -> NodeStatus {
    meta.members[&node].status
}

#[test]
fn leader_detects_failure_and_recovery_and_cascades_to_placement() {
    run(0xFA11_DE7E);
}

fn run(seed: u64) {
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = leader_among(&nodes, &[0, 1, 2]);

    // Register every data member as Active with its topology labels.
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

    // Place a 3-replica tablet one-per-zone and pin it to EU/one-per-zone.
    let initial = vec![10, 12, 14];
    assert!(matches!(
        nodes[leader].propose(MetaCommand::CreateTablet {
            tablet: TABLET,
            table: None,
            range: KeyRange::whole(),
            replicas: initial.clone(),
        }),
        ProposeResult::Accepted { .. }
    ));
    assert!(matches!(
        nodes[leader].propose(MetaCommand::SetTabletPolicy {
            tablet: TABLET,
            policy: Some(policy()),
        }),
        ProposeResult::Accepted { .. }
    ));

    // Let heartbeats flow and the cluster settle. With everyone heartbeating,
    // the detector must not flap anyone Down: all members stay Active and the
    // compliant tablet is untouched.
    sim.run_for(Duration::from_secs(2));
    let meta = nodes[leader].metadata();
    for (id, _, _) in DATA_NODES {
        assert_eq!(
            status_of(&meta, id),
            NodeStatus::Active,
            "member {id} flapped Down while heartbeating (seed={seed})"
        );
    }
    let before = meta.tablets[&TABLET].clone();
    assert_eq!(before.replicas, initial, "initial placement drifted");

    // --- Fault: one placed member crashes; its heartbeats stop. ---
    let dead = initial[0];
    let dead_zone = zone_of(&meta, dead);
    let epoch_before = before.epoch;
    sim.crash(dead);

    // No manual `Down`, no test-driven reconcile: the leader's detector must
    // notice the silence (> DETECT_TIMEOUT) and commit `Down`, which the
    // placement reconciler then reacts to.
    sim.run_for(Duration::from_secs(2));

    for (i, node) in nodes.iter().enumerate() {
        let m = node.metadata();
        // 1. Detected and committed Down on every control node.
        assert_eq!(
            status_of(&m, dead),
            NodeStatus::Down,
            "node {i}: dead member not marked Down (seed={seed})"
        );
        // 2. Cascade: placement moved off the dead member, residency + spread
        //    preserved, survivors kept, replacement reused the dead zone.
        let placed = &m.tablets[&TABLET].replicas;
        assert!(
            !placed.contains(&dead),
            "node {i}: dead replica still placed (seed={seed})"
        );
        assert!(
            m.tablets[&TABLET].epoch > epoch_before,
            "node {i}: tablet epoch not bumped (seed={seed})"
        );
        assert_residency_and_spread(&m, placed, seed);
        for kept in initial.iter().filter(|n| **n != dead) {
            assert!(
                placed.contains(kept),
                "node {i}: survivor {kept} needlessly moved (seed={seed})"
            );
        }
        let replacement = *placed.iter().find(|n| !initial.contains(n)).unwrap();
        assert_eq!(
            zone_of(&m, replacement),
            dead_zone,
            "node {i}: replacement should reuse the dead zone (seed={seed})"
        );
    }

    // --- Recovery: the member comes back and resumes heartbeating. ---
    sim.restart(dead);
    sim.run_for(Duration::from_secs(2));

    // 3. Detected recovery: the member returns to Active on every control node.
    for (i, node) in nodes.iter().enumerate() {
        assert_eq!(
            status_of(&node.metadata(), dead),
            NodeStatus::Active,
            "node {i}: recovered member not marked Active (seed={seed})"
        );
    }

    // Idempotence at steady state: no further status churn once everyone is back.
    let stable: BTreeMap<u64, NodeStatus> = DATA_NODES
        .iter()
        .map(|&(id, _, _)| (id, status_of(&nodes[leader].metadata(), id)))
        .collect();
    sim.run_for(Duration::from_secs(2));
    for (&id, &st) in &stable {
        assert_eq!(
            status_of(&nodes[leader].metadata(), id),
            st,
            "member {id} status churned at steady state (seed={seed})"
        );
    }
}

fn assert_residency_and_spread(meta: &Metadata, placed: &[u64], seed: u64) {
    assert_eq!(placed.len(), 3, "wrong replica count (seed={seed})");
    assert!(
        placed.iter().all(|n| (10..20).contains(n)),
        "residency lost: {placed:?} (seed={seed})"
    );
    let mut zones: Vec<String> = placed.iter().map(|n| zone_of(meta, *n)).collect();
    zones.sort();
    zones.dedup();
    assert_eq!(zones.len(), 3, "spread lost: {placed:?} (seed={seed})");
}

#[test]
fn detection_is_reproducible_from_seed() {
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
        nodes[leader].propose(MetaCommand::CreateTablet {
            tablet: TABLET,
            table: None,
            range: KeyRange::whole(),
            replicas: vec![10, 12, 14],
        });
        nodes[leader].propose(MetaCommand::SetTabletPolicy {
            tablet: TABLET,
            policy: Some(policy()),
        });
        sim.run_for(Duration::from_secs(2));
        sim.crash(10);
        sim.run_for(Duration::from_secs(2));
        sim.restart(10);
        sim.run_for(Duration::from_secs(2));
        sim.trace_lines()
    }
    assert_eq!(trace(0x5EED_FA11), trace(0x5EED_FA11));
}
