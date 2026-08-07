//! Automatic, leader-driven placement reconciliation (ADR 0005 + 0009).
//!
//! Unlike `placement_reconcile.rs` (which drives `replan` + the CAS from the
//! test), here the test does **no** placement math: it persists a
//! [`PlacementPolicy`] in the replicated `Metadata` via `SetTabletPolicy`, marks
//! a placed replica's member `Down`, and then merely advances virtual time. The
//! leader's in-node reconciler is expected to notice the violation, recompute
//! the desired set with `animus_placement::replan`, and commit a
//! `CasTabletReplicas` — converging the cluster to a policy-satisfying set
//! (residency + spread preserved, only the dead replica moved). The whole run is
//! a pure function of its seed.
//!
//! **Every data node heartbeats** (ADR 0030 phantom-member hardening): the
//! detector now also demotes an `Active` member it has never heard a heartbeat
//! from (see `animus_control::node::detect_loop`'s doc) — a real deployment's
//! members always heartbeat, but a *test's* `Active` member only stays `Active`
//! under this stricter rule if it does too, same as `failure_detection.rs`
//! already does. The one member the test itself marks `Down` has its heartbeat
//! task stopped (`sim.crash`) at that exact moment — the manual `Down` would
//! otherwise be immediately reverted by the (pre-existing, unchanged) `Down` →
//! `Active` recovery rule, since a still-heartbeating member is live evidence
//! the detector never suppresses.

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

/// Data nodes available for placement: `(id, region, zone)`. Six EU nodes across
/// three zones, plus one US node that residency must always exclude.
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
    // Every data node heartbeats the control group — see this file's doc.
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

#[test]
fn leader_automatically_reconciles_a_dead_replica() {
    run(0x0EE5_A070);
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

    // Create a 3-replica tablet placed one-per-zone (policy-satisfying) and
    // attach the policy pinning it to EU, one replica per zone. With placement
    // already compliant the reconciler must do nothing until something drifts.
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
    sim.run_for(Duration::from_secs(1));
    let before = nodes[leader].metadata().tablets[&TABLET].clone();

    // No test-driven replan/CAS. Placement is already compliant, so reconciling
    // must be a no-op: the set and epoch stay put.
    sim.run_for(Duration::from_secs(3));
    let meta = nodes[leader].metadata();
    assert_eq!(
        meta.tablets[&TABLET], before,
        "reconciler churned a compliant tablet (seed={seed})"
    );
    let after_fresh = meta.tablets[&TABLET].replicas.clone();
    assert_residency_and_spread(&meta, &after_fresh, seed);

    // --- Fault: a placed replica's member dies. ---
    let dead = after_fresh[0];
    let dead_zone = zone_of(&meta, dead);
    // Stop its heartbeat too: otherwise the (pre-existing, unchanged) `Down` →
    // `Active` recovery rule would immediately revert this manual `Down` the
    // moment `detect_loop` next sees a heartbeat still arriving from it.
    sim.crash(dead);
    assert!(matches!(
        nodes[leader].propose(MetaCommand::UpsertMember {
            node: dead,
            labels: meta.members[&dead].labels.clone(),
            status: NodeStatus::Down,
        }),
        ProposeResult::Accepted { .. }
    ));

    // Again: no test-driven reconcile. Let the leader notice and fix it.
    let epoch_before = nodes[leader].metadata().tablets[&TABLET].epoch;
    sim.run_for(Duration::from_secs(3));

    // The reconcile committed on every control node: the dead replica is gone,
    // residency + spread still hold, survivors stayed put, and the replacement
    // reused the dead replica's zone (minimal, like-for-like churn).
    for (i, node) in nodes.iter().enumerate() {
        let m = node.metadata();
        let placed = &m.tablets[&TABLET].replicas;
        assert!(
            !placed.contains(&dead),
            "node {i}: dead replica still placed (seed={seed})"
        );
        assert!(
            m.tablets[&TABLET].epoch > epoch_before,
            "node {i}: epoch not bumped (seed={seed})"
        );
        assert_residency_and_spread(&m, placed, seed);
        for kept in after_fresh.iter().filter(|n| **n != dead) {
            assert!(
                placed.contains(kept),
                "node {i}: survivor {kept} needlessly moved (seed={seed})"
            );
        }
        let replacement = *placed.iter().find(|n| !after_fresh.contains(n)).unwrap();
        assert_eq!(
            zone_of(&m, replacement),
            dead_zone,
            "node {i}: replacement should reuse the dead zone (seed={seed})"
        );
    }

    // Idempotence: with the cluster already compliant, more time produces no
    // further placement change (the epoch is stable).
    let stable = nodes[leader].metadata().tablets[&TABLET].clone();
    sim.run_for(Duration::from_secs(3));
    assert_eq!(
        nodes[leader].metadata().tablets[&TABLET],
        stable,
        "reconciler churned a compliant tablet (seed={seed})"
    );
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
fn reconcile_is_reproducible_from_seed() {
    // Two runs of the same scenario commit byte-identical traces.
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
        sim.run_for(Duration::from_secs(3));
        let dead = nodes[leader].metadata().tablets[&TABLET].replicas[0];
        let dead_labels = nodes[leader].metadata().members[&dead].labels.clone();
        sim.crash(dead);
        nodes[leader].propose(MetaCommand::UpsertMember {
            node: dead,
            labels: dead_labels,
            status: NodeStatus::Down,
        });
        sim.run_for(Duration::from_secs(3));
        sim.trace_lines()
    }
    assert_eq!(trace(0x5EED_A070), trace(0x5EED_A070));
}
