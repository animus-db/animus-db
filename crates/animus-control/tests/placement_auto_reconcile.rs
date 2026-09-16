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
use animus_env::{EnvExt, NodeId, nid};
use animus_placement::PlacementPolicy;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
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
        .map(|&id| {
            RaftNode::start(
                sim.env(nid(id)),
                CONTROL.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    // Every data node heartbeats the control group — see this file's doc.
    for (id, _, _) in DATA_NODES {
        let env = sim.env(nid(id));
        env.spawn_task(heartbeat_loop(
            env.clone(),
            CONTROL.iter().copied().map(nid).collect(),
        ));
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

fn zone_of(meta: &Metadata, node: &NodeId) -> String {
    meta.members[node].labels["zone"].clone()
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
                node: nid(id),
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
            replicas: initial.clone().into_iter().map(nid).collect(),
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
    let dead = after_fresh[0].clone();
    let dead_zone = zone_of(&meta, &dead);
    // Stop its heartbeat too: otherwise the (pre-existing, unchanged) `Down` →
    // `Active` recovery rule would immediately revert this manual `Down` the
    // moment `detect_loop` next sees a heartbeat still arriving from it.
    sim.crash(dead.clone());
    assert!(matches!(
        nodes[leader].propose(MetaCommand::UpsertMember {
            node: dead.clone(),
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
        let replacement = placed
            .iter()
            .find(|n| !after_fresh.contains(n))
            .unwrap()
            .clone();
        assert_eq!(
            zone_of(&m, &replacement),
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

fn assert_residency_and_spread(meta: &Metadata, placed: &[NodeId], seed: u64) {
    assert_eq!(placed.len(), 3, "wrong replica count (seed={seed})");
    let eu_ids: Vec<NodeId> = (10..=15).map(nid).collect();
    assert!(
        placed.iter().all(|n| eu_ids.contains(n)),
        "residency lost: {placed:?} (seed={seed})"
    );
    let mut zones: Vec<String> = placed.iter().map(|n| zone_of(meta, n)).collect();
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
                node: nid(id),
                labels: labels(region, zone),
                status: NodeStatus::Active,
            });
        }
        sim.run_for(Duration::from_secs(1));
        nodes[leader].propose(MetaCommand::CreateTablet {
            tablet: TABLET,
            table: None,
            range: KeyRange::whole(),
            replicas: vec![nid(10), nid(12), nid(14)],
        });
        nodes[leader].propose(MetaCommand::SetTabletPolicy {
            tablet: TABLET,
            policy: Some(policy()),
        });
        sim.run_for(Duration::from_secs(3));
        let dead = nodes[leader].metadata().tablets[&TABLET].replicas[0].clone();
        let dead_labels = nodes[leader].metadata().members[&dead].labels.clone();
        sim.crash(dead.clone());
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

/// Issue #957: a tablet whose policy RF exceeds the number of currently
/// `Active` candidates must still be grown toward that RF as far as the
/// cluster genuinely allows — not refused outright the instant the full RF
/// can't be met. Root-caused chasing the flaky
/// `tablet_provisioned_undersized_on_a_small_cluster_self_heals_after_
/// growth` (`crates/animusd/tests/tablet_rf_self_heals.rs`): the boot-time
/// cluster check (issue #667/#900) was the prime suspect but was ruled out
/// live (`cluster_check_pending`/`refused_as_voter` both `false` on every
/// replica throughout a captured stall, control-plane consensus fully
/// healthy and idle) — the real mechanism was `reconcile_placement` calling
/// plain `animus_placement::replan`, whose `choose` refuses outright
/// ("not enough eligible candidates") the instant the full policy RF can't
/// be met, even when strictly-improving partial progress is possible. Fixed
/// by `replan_repair` (`animus-placement`).
///
/// Uses a 2-node data pool against an RF-3 policy (mirroring
/// `tablet_rf_self_heals.rs`'s own 2-node-cluster/RF-3-policy shape) so the
/// leader's real, timer-driven `reconcile_loop` (never test-driven here,
/// same discipline as `leader_automatically_reconciles_a_dead_replica`)
/// must commit a `CasTabletReplicas` that grows 1 -> 2 replicas, then
/// converge idempotently until a 3rd candidate makes the full RF reachable.
#[test]
fn leader_automatically_grows_an_undersized_tablet_when_rf_exceeds_the_candidate_pool() {
    run_undersized(0x0957_0001);
}

fn run_undersized(seed: u64) {
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = leader_among(&nodes, &[0, 1, 2]);

    // Only node 10 registered/Active at first — mirroring the real-world
    // race `docs/lessons/testing/2026-09-16-a-faster-bootstrap-time-schema-
    // proposal-makes-initial-tablet-placement-an-eventual-property.md`
    // documents: a fresh cluster's very first tablet can be minted before
    // every founding member's own registration has committed and applied.
    assert!(matches!(
        nodes[leader].propose(MetaCommand::UpsertMember {
            node: nid(10),
            labels: labels("eu", "a"),
            status: NodeStatus::Active,
        }),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(1));

    // RF 3 — deliberately more than either candidate count this scenario
    // ever exercises before its final phase, mirroring `provision_tablet`'s
    // own always-record-the-target-RF discipline.
    let rf3 = PlacementPolicy::simple("cp-rf", 3);
    assert!(matches!(
        nodes[leader].propose(MetaCommand::CreateTablet {
            tablet: TABLET,
            table: None,
            range: KeyRange::whole(),
            replicas: vec![nid(10)],
        }),
        ProposeResult::Accepted { .. }
    ));
    assert!(matches!(
        nodes[leader].propose(MetaCommand::SetTabletPolicy {
            tablet: TABLET,
            policy: Some(rf3),
        }),
        ProposeResult::Accepted { .. }
    ));

    // The 2nd node joins only now — this is what the leader's own
    // reconciler must react to, entirely on its own timer.
    assert!(matches!(
        nodes[leader].propose(MetaCommand::UpsertMember {
            node: nid(11),
            labels: labels("eu", "b"),
            status: NodeStatus::Active,
        }),
        ProposeResult::Accepted { .. }
    ));

    // No test-driven replan/CAS: let the leader's own reconcile_loop notice
    // and grow the tablet as far as it genuinely can (2 of the RF-3
    // target) — this is the exact self-heal the flaky test's own first
    // `poll_until_or_stalled` call was waiting on, and which the unfixed
    // code never proposed at all.
    sim.run_for(Duration::from_secs(3));
    for (i, node) in nodes.iter().enumerate() {
        let m = node.metadata();
        let placed = &m.tablets[&TABLET].replicas;
        assert_eq!(
            placed,
            &vec![nid(10), nid(11)],
            "node {i}: tablet did not grow to both available candidates (seed={seed})"
        );
    }

    // Idempotent: further time produces no more churn on this 2-node pool
    // — the exact steady state a wider stall-timeout would have papered
    // over, never the actual fix.
    let stable = nodes[leader].metadata().tablets[&TABLET].clone();
    sim.run_for(Duration::from_secs(3));
    assert_eq!(
        nodes[leader].metadata().tablets[&TABLET],
        stable,
        "reconciler churned a tablet already at this cluster's own capacity (seed={seed})"
    );

    // A 3rd candidate makes the full RF reachable — the ordinary case,
    // unaffected by this fix (and the shape `tablet_rf_self_heals.rs`'s own
    // second phase, growth to 3 nodes, already exercised without flaking),
    // converges the rest of the way.
    assert!(matches!(
        nodes[leader].propose(MetaCommand::UpsertMember {
            node: nid(12),
            labels: labels("eu", "c"),
            status: NodeStatus::Active,
        }),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(3));
    for (i, node) in nodes.iter().enumerate() {
        let m = node.metadata();
        let placed = &m.tablets[&TABLET].replicas;
        assert_eq!(
            placed,
            &vec![nid(10), nid(11), nid(12)],
            "node {i}: tablet did not reach the full RF once 3 candidates existed (seed={seed})"
        );
    }
}

#[test]
fn undersized_growth_is_reproducible_from_seed() {
    fn trace(seed: u64) -> Vec<String> {
        let (mut sim, nodes) = cluster(seed);
        sim.run_for(Duration::from_secs(2));
        let leader = leader_among(&nodes, &[0, 1, 2]);
        nodes[leader].propose(MetaCommand::UpsertMember {
            node: nid(10),
            labels: labels("eu", "a"),
            status: NodeStatus::Active,
        });
        sim.run_for(Duration::from_secs(1));
        nodes[leader].propose(MetaCommand::CreateTablet {
            tablet: TABLET,
            table: None,
            range: KeyRange::whole(),
            replicas: vec![nid(10)],
        });
        nodes[leader].propose(MetaCommand::SetTabletPolicy {
            tablet: TABLET,
            policy: Some(PlacementPolicy::simple("cp-rf", 3)),
        });
        nodes[leader].propose(MetaCommand::UpsertMember {
            node: nid(11),
            labels: labels("eu", "b"),
            status: NodeStatus::Active,
        });
        sim.run_for(Duration::from_secs(3));
        sim.trace_lines()
    }
    assert_eq!(trace(0x5EED_0957), trace(0x5EED_0957));
}
