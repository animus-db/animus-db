//! Reproduction attempt for issue #864/#902-stack e2e stall: does the S-07d
//! Kubernetes-operator control-voter-growth ordering (the NEW pod boots with
//! its OWN static config already listing itself as a founder of the target
//! N-voter group, i.e. `RaftNode::start(env, control_ids_INCLUDING_SELF)`,
//! *before* the operator ever calls `change_membership`/`POST
//! /admin/control/member/add`) ever leaves the new node unable to reach a
//! healthy `leader_within` state within a bounded time, under realistic
//! (delayed/lossy) network conditions on the new node's own links?
//!
//! This deliberately differs from `genesis_and_growth_shapes.rs`'s own
//! growth scenario (which calls `change_membership` BEFORE constructing the
//! new node's `RaftNode`) and from `SimCluster::grow_control` (which
//! constructs the new node's own `RaftCore` EXCLUDING itself from its own
//! config, a "lone standalone core") -- neither of those matches
//! `animus-operator`'s actual S-07d sequence, where the promoted pod's own
//! generated `cluster.json` already lists it as one of `spec.controlNodes`
//! nodes (role "both") the moment it restarts, and the operator only calls
//! the admin add-voter route *afterward*, once it observes the pod's own
//! `/admin/config` reporting role "combined".

use std::collections::BTreeSet;
use std::time::Duration;

use animus_control::{ProposeResult, RaftNode};
use animus_env::{NodeId, nid};
use animus_sim::{NetConfig, Simulator};
use animus_storage::MemoryEngine;

fn set(ids: &[u64]) -> BTreeSet<NodeId> {
    ids.iter().copied().map(nid).collect()
}

fn health_ok(nodes: &[RaftNode<animus_sim::SimEnv>], live: &[usize]) -> bool {
    live.iter().all(|&i| {
        let grace = nodes[i].election_timeout() * 3;
        nodes[i].leader_within(grace).is_some()
    })
}

/// Models the real S-07d ordering under a moderately lossy/delayed link
/// between the new node and each of its three peers -- close to what a real
/// `kind` cluster's CNI overlay can look like under load, but well short of
/// a partition.
fn run_scenario(seed: u64, drop_prob: f64, base_delay_ms: u64, jitter_ms: u64) {
    let mut sim = Simulator::new(seed);

    // Established 3-voter genesis group.
    let mut nodes: Vec<RaftNode<animus_sim::SimEnv>> = (0..3)
        .map(|i| {
            RaftNode::start(
                sim.env(nid(i)),
                set(&[0, 1, 2]).into_iter().collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    sim.run_for(Duration::from_secs(3));
    assert!(
        nodes.iter().any(RaftNode::is_leader),
        "seed={seed}: the 3-voter genesis group must elect a leader before growth starts"
    );
    assert!(
        health_ok(&nodes, &[0, 1, 2]),
        "seed={seed}: the pre-growth group must be healthy"
    );

    // Degrade every link between the not-yet-added node 3 and each
    // established peer, in both directions -- the new pod's own boot-time
    // `ClusterProbe` round and any later real Raft traffic to/from it.
    let mut degraded = NetConfig::default();
    degraded.set_drop_prob(drop_prob);
    degraded.base_delay = Duration::from_millis(base_delay_ms);
    degraded.max_jitter = Duration::from_millis(jitter_ms);
    for peer in 0..3u64 {
        sim.set_link_net_config(nid(3), nid(peer), degraded.clone());
        sim.set_link_net_config(nid(peer), nid(3), degraded.clone());
    }

    // The new pod boots with its OWN generated `cluster.json` already
    // listing all 4 nodes as voters (self-inclusive) -- the real
    // `animusd::run_node_with_cluster_settings` -> `Node::start` shape via
    // `ClusterConfig::control_ids()`, NOT `grow_control`'s
    // excluding-self "lone standalone core" shape.
    let node3 = RaftNode::start(
        sim.env(nid(3)),
        set(&[0, 1, 2, 3]).into_iter().collect(),
        MemoryEngine::new(),
    );
    nodes.push(node3);

    // Let node 3 run for a while on its own, exactly like the real operator
    // does: it waits ~30s of reconcile cadence (modeled here as a shorter
    // but still "the node has fully booted and been observed" window)
    // before ever calling the admin add-voter route.
    sim.run_for(Duration::from_secs(5));

    // The operator's `add_control_voter`: a real `change_membership` on the
    // CURRENT real leader of {0,1,2}, adding node 3.
    let leader_idx = (0..3)
        .find(|&i| nodes[i].is_leader())
        .expect("a leader must still exist");
    assert!(
        matches!(
            nodes[leader_idx].change_membership(set(&[0, 1, 2, 3])),
            ProposeResult::Accepted { .. }
        ),
        "seed={seed}: change_membership adding node 3 must be accepted by the real leader"
    );

    // Converged-or-timeout poll (never a fixed-deadline one-shot assert,
    // root CLAUDE.md's Testing rule): node 3 must become a healthy voter
    // (a leader within its own grace window) well within a generous real
    // budget, and must never be falsely, permanently refused.
    const STEP: Duration = Duration::from_millis(200);
    const BUDGET: Duration = Duration::from_secs(60);
    let mut elapsed = Duration::ZERO;
    loop {
        assert!(
            !nodes[3].refused_as_voter(),
            "seed={seed}: node 3 was falsely refused as a voter -- issue #667 regression"
        );
        if nodes[3].config().contains(&nid(3)) && health_ok(&nodes, &[0, 1, 2, 3]) {
            break;
        }
        assert!(
            elapsed < BUDGET,
            "seed={seed}: node 3 never became a healthy voter within {BUDGET:?} \
             (cluster_check_pending={}, refused={}, config={:?}, leader={:?}) -- \
             this is the issue #864/S-07d e2e stall shape",
            nodes[3].cluster_check_pending(),
            nodes[3].refused_as_voter(),
            nodes[3].config(),
            nodes[3].is_leader(),
        );
        sim.run_for(STEP);
        elapsed += STEP;
    }
}

#[test]
fn s07d_style_growth_with_clean_network_converges() {
    run_scenario(0x8640_0001, 0.0, 1, 4);
}

#[test]
fn s07d_style_growth_with_lossy_new_node_links_converges() {
    // 20% drop probability on every message to/from the not-yet-added node,
    // plus a heavier base delay -- models a flaky kind-cluster CNI link
    // without an outright partition.
    run_scenario(0x8640_0002, 0.20, 20, 60);
}

#[test]
fn s07d_style_growth_with_lossy_new_node_links_converges_seeds() {
    for seed in 0x8640_1000u64..0x8640_1010u64 {
        run_scenario(seed, 0.20, 20, 60);
    }
}
