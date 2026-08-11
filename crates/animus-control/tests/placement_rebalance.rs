//! Automatic, leader-driven load **rebalancing** (ADR 0029), the balance-driven
//! counterpart of `placement_auto_reconcile.rs`'s violation-driven repair.
//!
//! The test does **no** placement math: it persists RF-3 [`PlacementPolicy`]s in
//! the replicated `Metadata`, places every tablet on the original members, then
//! grows the cluster by registering new members and merely advances virtual time.
//! The leader's `reconcile_loop` is expected to notice the imbalance (repair has
//! nothing to do — every set is compliant), call `animus_placement::rebalance_step`
//! once per rebalance tick, and commit a `CasTabletReplicas` per move — converging
//! per-node replica counts to max−min ≤ 1 while preserving residency + spread, and
//! then going quiet (no churn once balanced). Every run is a pure function of its
//! seed.
//!
//! Every registered data member heartbeats (ADR 0030 phantom-member hardening):
//! the detector now also demotes an `Active` member it has never heard a
//! heartbeat from (see `animus_control::node::detect_loop`'s doc) -- see
//! `placement_auto_reconcile.rs`'s identical note for why `register` spawns one
//! and why a member the test itself marks `Down` has its heartbeat stopped
//! (`sim.crash`) at that exact moment.

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::node::heartbeat_loop;
use animus_control::raft::ProposeResult;
use animus_control::{MetaCommand, Metadata, NodeStatus, RaftNode};
use animus_env::{EnvExt, nid};
use animus_placement::PlacementPolicy;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, TabletId};

const CONTROL: [u64; 3] = [0, 1, 2];
/// Number of policied tablets provisioned, all initially on the first three data
/// members (a "provisioned before the cluster grew" scenario).
const TABLETS: u64 = 6;

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

fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// Register a data member as `Active` with the given labels, and start it
/// heartbeating (ADR 0030 phantom-member hardening — see this file's doc).
fn register(sim: &Simulator, node: &RaftNode<SimEnv>, id: u64, lbls: BTreeMap<String, String>) {
    assert!(matches!(
        node.propose(MetaCommand::UpsertMember {
            node: nid(id),
            labels: lbls,
            status: NodeStatus::Active,
        }),
        ProposeResult::Accepted { .. }
    ));
    let env = sim.env(nid(id));
    env.spawn_task(heartbeat_loop(
        env.clone(),
        CONTROL.iter().copied().map(nid).collect(),
    ));
}

/// The numeric id backing a `nid(n)`-formatted `"n{n}"` string id.
fn id_num(node: &animus_env::NodeId) -> u64 {
    node.as_str()
        .trim_start_matches('n')
        .parse()
        .expect("test node ids are nid(n)-formatted")
}

/// Per-node replica counts over `ids`, seeded 0.
fn counts(meta: &Metadata, ids: &[u64]) -> BTreeMap<u64, usize> {
    let mut c: BTreeMap<u64, usize> = ids.iter().map(|&id| (id, 0)).collect();
    for t in meta.tablets.values() {
        for r in &t.replicas {
            if let Some(n) = c.get_mut(&id_num(r)) {
                *n += 1;
            }
        }
    }
    c
}

/// Whether `counts(meta, ids)` is balanced (max − min ≤ 1).
fn is_balanced(meta: &Metadata, ids: &[u64]) -> bool {
    let c = counts(meta, ids);
    let (min, max) = (
        *c.values().min().unwrap_or(&0),
        *c.values().max().unwrap_or(&0),
    );
    max - min <= 1
}

/// Provision `TABLETS` RF-3 tablets, all placed on `initial`, each with `policy`.
fn provision(node: &RaftNode<SimEnv>, initial: &[u64], policy: &PlacementPolicy) {
    for i in 1..=TABLETS {
        assert!(matches!(
            node.propose(MetaCommand::CreateTablet {
                tablet: TabletId(i),
                table: None,
                range: KeyRange::whole(),
                replicas: initial.iter().copied().map(nid).collect(),
            }),
            ProposeResult::Accepted { .. }
        ));
        assert!(matches!(
            node.propose(MetaCommand::SetTabletPolicy {
                tablet: TabletId(i),
                policy: Some(policy.clone()),
            }),
            ProposeResult::Accepted { .. }
        ));
    }
}

#[test]
fn leader_automatically_rebalances_onto_new_members() {
    for seed in [0x0EBA_1A0Cu64, 0x5EED_0002, 0xABCD_1234] {
        rebalances_onto_new_members(seed);
    }
}

fn rebalances_onto_new_members(seed: u64) {
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = leader_among(&nodes, &[0, 1, 2]);
    let policy = PlacementPolicy::simple("rf3", 3);

    // Three data members, all six tablets placed on them.
    for id in [10, 11, 12] {
        register(&sim, &nodes[leader], id, labels(&[("region", "eu")]));
    }
    sim.run_for(Duration::from_secs(1));
    provision(&nodes[leader], &[10, 11, 12], &policy);
    sim.run_for(Duration::from_secs(2));

    // The cluster grows: two more members. Nothing else — the leader must spread
    // existing tablets onto them on its own.
    for id in [13, 14] {
        register(&sim, &nodes[leader], id, labels(&[("region", "eu")]));
    }

    let all = [10, 11, 12, 13, 14];
    // Poll (converged-or-timeout) until every node's replica count is within 1.
    let mut balanced = false;
    for _ in 0..120 {
        sim.run_for(Duration::from_secs(1));
        if is_balanced(&nodes[leader].metadata(), &all) {
            balanced = true;
            break;
        }
    }
    let c = counts(&nodes[leader].metadata(), &all);
    assert!(balanced, "did not balance (seed={seed}): {c:?}");
    // Every node — including the two new ones — carries load.
    assert!(
        all.iter().all(|id| c[id] > 0),
        "a member got no replicas (seed={seed}): {c:?}"
    );

    // Steady state: once balanced, no further placement churn. Record every
    // policied tablet's epoch, let several rebalance ticks pass, and assert none
    // moved (rebalance_step returns None at max−min ≤ 1).
    let epochs: BTreeMap<TabletId, _> = nodes[leader]
        .metadata()
        .tablets
        .iter()
        .map(|(id, t)| (*id, t.epoch))
        .collect();
    sim.run_for(Duration::from_secs(20));
    for (id, t) in &nodes[leader].metadata().tablets {
        assert_eq!(
            t.epoch, epochs[id],
            "tablet {id:?} churned after balancing (seed={seed})"
        );
    }
}

#[test]
fn rebalance_defers_to_violation_repair() {
    let seed = 0x0DEF_2200u64;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = leader_among(&nodes, &[0, 1, 2]);
    let policy = PlacementPolicy::simple("rf3", 3);

    for id in [10, 11, 12] {
        register(&sim, &nodes[leader], id, labels(&[("region", "eu")]));
    }
    sim.run_for(Duration::from_secs(1));
    provision(&nodes[leader], &[10, 11, 12], &policy);
    sim.run_for(Duration::from_secs(1));

    // Grow the cluster, then — mid-imbalance — kill one of the ORIGINAL members.
    for id in [13, 14] {
        register(&sim, &nodes[leader], id, labels(&[("region", "eu")]));
    }
    sim.run_for(Duration::from_secs(5));
    let dead = 10;
    // Stop its heartbeat too — otherwise the (pre-existing, unchanged) `Down` ->
    // `Active` recovery rule would immediately revert this manual `Down`.
    sim.crash(nid(dead));
    assert!(matches!(
        nodes[leader].propose(MetaCommand::UpsertMember {
            node: nid(dead),
            labels: labels(&[("region", "eu")]),
            status: NodeStatus::Down,
        }),
        ProposeResult::Accepted { .. }
    ));

    // Repair (violation-driven) always wins a tick over rebalance (balance-driven)
    // by construction — the loop only rebalances when repair proposed nothing —
    // so we simply assert the end state: the dead replica is fully evicted and the
    // survivors {11,12,13,14} converge to a balanced, RF-3, compliant placement.
    let live = [11, 12, 13, 14];
    let mut done = false;
    for _ in 0..120 {
        sim.run_for(Duration::from_secs(1));
        let meta = nodes[leader].metadata();
        let no_dead = meta
            .tablets
            .values()
            .all(|t| !t.replicas.contains(&nid(dead)));
        if no_dead && is_balanced(&meta, &live) {
            done = true;
            break;
        }
    }
    let meta = nodes[leader].metadata();
    assert!(done, "did not repair+balance: {:?}", counts(&meta, &live));
    for t in meta.tablets.values() {
        assert_eq!(t.replicas.len(), 3, "a tablet lost its replication factor");
        assert!(
            !t.replicas.contains(&nid(dead)),
            "dead replica still placed"
        );
    }
}

#[test]
fn rebalance_preserves_residency_and_spread() {
    let seed = 0x5A1E_7700u64;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = leader_among(&nodes, &[0, 1, 2]);
    // RF-3, EU-only, one replica per zone (strict spread).
    let policy = PlacementPolicy::simple("eu-rf3", 3)
        .require_label("region", "eu")
        .spread_across("zone", true);

    // Original three: one per zone.
    register(
        &sim,
        &nodes[leader],
        10,
        labels(&[("region", "eu"), ("zone", "a")]),
    );
    register(
        &sim,
        &nodes[leader],
        11,
        labels(&[("region", "eu"), ("zone", "b")]),
    );
    register(
        &sim,
        &nodes[leader],
        12,
        labels(&[("region", "eu"), ("zone", "c")]),
    );
    sim.run_for(Duration::from_secs(1));
    provision(&nodes[leader], &[10, 11, 12], &policy);
    sim.run_for(Duration::from_secs(2));

    // Grow 3 → 7: two new EU nodes (giving zones a and b a second host so those
    // zones can rebalance) and two new US nodes that residency must always exclude.
    register(
        &sim,
        &nodes[leader],
        13,
        labels(&[("region", "eu"), ("zone", "a")]),
    );
    register(
        &sim,
        &nodes[leader],
        14,
        labels(&[("region", "eu"), ("zone", "b")]),
    );
    register(
        &sim,
        &nodes[leader],
        15,
        labels(&[("region", "us"), ("zone", "a")]),
    );
    register(
        &sim,
        &nodes[leader],
        16,
        labels(&[("region", "us"), ("zone", "b")]),
    );

    // Strict spread must hold at every observed state, US nodes must never gain a
    // replica, and rebalancing must eventually place load on both new EU nodes.
    let check_invariants = |meta: &Metadata, seed: u64| {
        for (id, t) in &meta.tablets {
            // Residency: no US node ever hosts a replica.
            assert!(
                t.replicas.iter().all(|r| *r != nid(15) && *r != nid(16)),
                "tablet {id:?} placed on a US node (seed={seed}): {:?}",
                t.replicas
            );
            // Strict spread: three replicas, three distinct zones.
            let mut zones: Vec<&String> = t
                .replicas
                .iter()
                .map(|r| &meta.members[r].labels["zone"])
                .collect();
            zones.sort();
            zones.dedup();
            assert_eq!(
                zones.len(),
                3,
                "tablet {id:?} lost strict spread (seed={seed}): {:?}",
                t.replicas
            );
        }
    };

    let mut spread_onto_new = false;
    for _ in 0..120 {
        sim.run_for(Duration::from_secs(1));
        let meta = nodes[leader].metadata();
        check_invariants(&meta, seed);
        let c = counts(&meta, &[13, 14]);
        if c[&13] > 0 && c[&14] > 0 {
            spread_onto_new = true;
            break;
        }
    }
    let meta = nodes[leader].metadata();
    check_invariants(&meta, seed);
    assert!(
        spread_onto_new,
        "rebalancing never used the new EU nodes: {:?}",
        counts(&meta, &[13, 14])
    );
    // The US nodes carry nothing, confirmed at the end too.
    let us = counts(&meta, &[15, 16]);
    assert_eq!(us[&15] + us[&16], 0, "US nodes gained replicas: {us:?}");
}

#[test]
fn rebalance_survives_leader_kill() {
    for seed in [0xC0FF_EE01u64, 0x1DEA_D000] {
        survives_leader_kill(seed);
    }
}

fn survives_leader_kill(seed: u64) {
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = leader_among(&nodes, &[0, 1, 2]);
    let policy = PlacementPolicy::simple("rf3", 3);

    for id in [10, 11, 12] {
        register(&sim, &nodes[leader], id, labels(&[("region", "eu")]));
    }
    sim.run_for(Duration::from_secs(1));
    provision(&nodes[leader], &[10, 11, 12], &policy);
    sim.run_for(Duration::from_secs(1));
    for id in [13, 14] {
        register(&sim, &nodes[leader], id, labels(&[("region", "eu")]));
    }
    // Let rebalancing get underway, then kill the control leader mid-flight.
    sim.run_for(Duration::from_secs(5));
    sim.stop(nid(leader as u64));
    let live: Vec<usize> = (0..3).filter(|&i| i != leader).collect();

    // A new leader must take over and finish rebalancing. Read committed state
    // from a surviving node.
    sim.run_for(Duration::from_secs(2));
    let survivor = live[0];
    let all = [10, 11, 12, 13, 14];
    let mut balanced = false;
    for _ in 0..120 {
        sim.run_for(Duration::from_secs(1));
        if is_balanced(&nodes[survivor].metadata(), &all) {
            balanced = true;
            break;
        }
    }
    let c = counts(&nodes[survivor].metadata(), &all);
    assert!(
        balanced,
        "did not balance after leader kill (seed={seed}): {c:?}"
    );
    // Exactly one leader took over among the survivors (asserted inside).
    let _ = leader_among(&nodes, &live);
}
