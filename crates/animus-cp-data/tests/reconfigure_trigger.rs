//! Stage C **automatic trigger**, end to end (ADR 0017 C + 0012 + 0005): a node
//! failure cascades all the way into a per-tablet Raft KV group reconfiguring
//! itself off the dead node and onto a spare — with **no test-side membership
//! call**.
//!
//! The full flow under one `SimEnv`:
//!
//! 1. A 3-node **control plane** owns a tablet placed `{10,12,14}` (one per zone)
//!    under an RF-3 / one-per-zone policy. The data members heartbeat it.
//! 2. A per-tablet **Raft KV group** runs on the same data ids, voters
//!    `{10,12,14}`, with a pre-started zone-`a` **spare** (`11`) ready to join.
//! 3. Each group node runs the **epoch-driven-pull reconfigure loop**
//!    (`spawn_reconfigure_loop`): it polls the control plane's replicated
//!    `Metadata.tablets[t].replicas` and steps its own Raft config toward it.
//! 4. Node `10` crashes. The control plane's failure detector marks it `Down`
//!    (ADR 0012); the placement reconciler swaps in the same-zone spare `11`
//!    (ADR 0005), committing a `CasTabletReplicas` → desired `{11,12,14}`.
//! 5. The surviving group leader pulls that desired set and drives the group's
//!    Raft config to it (remove `10`, add `11`) — single-server steps via the
//!    shared `RaftCore::change_membership`. The spare catches up and the group
//!    keeps serving linearizable reads/writes.
//!
//! This is the "remaining integration plumbing" ADR 0017 flagged for Stage C; the
//! membership *mechanism* is proven separately in `membership.rs`. Decided seam
//! (per maintainer): **SimEnv first** (the `animusd`/`ProdEnv` assembly is later
//! integration work) and **epoch-driven pull** (each group leader reconfigures
//! itself from replicated metadata — no new control→data command).
//!
//! Deterministic + seed-reproducible (drive with `run_for`, never `run()`).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::node::heartbeat_loop;
use animus_control::{MetaCommand, NodeStatus, ProposeResult, RaftNode};
use animus_cp_data::RaftKvNode;
use animus_env::{EnvExt, NodeId};
use animus_placement::PlacementPolicy;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, TabletId};
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

const CONTROL: [u64; 3] = [0, 1, 2];
const TABLET: TabletId = TabletId(1);

/// Data members `(id, region, zone)`: the initial group is one-per-zone
/// `{10,12,14}`; `11` is a zone-`a` spare the reconciler picks when `10` dies.
const DATA_NODES: [(u64, &str, &str); 4] = [
    (10, "eu", "a"),
    (11, "eu", "a"),
    (12, "eu", "b"),
    (14, "eu", "c"),
];
/// The data ids the Raft KV group spans over its lifetime (initial voters + the
/// spare). Each is pre-started so the spare can catch up once added.
const GROUP_IDS: [u64; 4] = [10, 11, 12, 14];
const INITIAL_VOTERS: [u64; 3] = [10, 12, 14];

/// How often each group node polls control metadata and steps its config.
const RECONFIGURE_INTERVAL: Duration = Duration::from_millis(200);

fn labels(region: &str, zone: &str) -> BTreeMap<String, String> {
    [("region", region), ("zone", zone)]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn policy() -> PlacementPolicy {
    PlacementPolicy::simple("eu-rf3", 3)
        .require_label("region", "eu")
        .spread_across("zone", true)
}

fn set(ids: &[u64]) -> BTreeSet<NodeId> {
    ids.iter().copied().collect()
}

/// The control leader index, asserting exactly one among `0..3`.
fn control_leader(nodes: &[RaftNode<SimEnv>]) -> usize {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(ls.len(), 1, "expected one control leader, got {ls:?}");
    ls[0]
}

/// The current group leader's id among the live group nodes, if exactly one.
fn group_leader(group: &BTreeMap<u64, KvNode>, live: &[u64]) -> Option<u64> {
    let ls: Vec<u64> = live
        .iter()
        .copied()
        .filter(|id| group.get(id).is_some_and(|n| n.is_leader()))
        .collect();
    if ls.len() == 1 { Some(ls[0]) } else { None }
}

/// Run a linearizable read on `node` to completion (spawned, since it awaits a
/// read-barrier round), driving the sim up to `budget`.
fn lin_read(sim: &mut Simulator, node: &KvNode, key: &[u8], budget: Duration) -> Option<Vec<u8>> {
    let slot: Arc<Mutex<Option<Option<Vec<u8>>>>> = Arc::new(Mutex::new(None));
    let n = node.clone();
    let s = Arc::clone(&slot);
    let k = key.to_vec();
    node.env().clone().spawn_task(async move {
        *s.lock().unwrap() = Some(n.linearizable_get(&k).await);
    });
    sim.run_for(budget);
    let v = slot.lock().unwrap().clone();
    v.expect("linearizable read did not complete")
}

#[test]
fn node_failure_auto_reconfigures_the_tablet_group() {
    // A small seed sweep: the cascade (detect → reconcile → reconfigure) is timing
    // sensitive, so exercise several interleavings, not one lucky schedule.
    for seed in [0xC0FFEE, 0x1234_5678, 0xBEEF, 0xA11CE, 0x5EED_5EED] {
        run(seed);
    }
}

fn run(seed: u64) {
    let mut sim = Simulator::new(seed);

    // --- Control plane (ids 0..3). ---
    let control: Vec<RaftNode<SimEnv>> = CONTROL
        .iter()
        .map(|&id| RaftNode::start(sim.env(id), CONTROL.to_vec()))
        .collect();
    // Every data member heartbeats the control group (so the detector sees them).
    for (id, _, _) in DATA_NODES {
        let env = sim.env(id);
        env.spawn_task(heartbeat_loop(env.clone(), CONTROL.to_vec()));
    }

    // --- Per-tablet Raft KV group. Voters start {10,12,14}; the spare 11 is
    // pre-started knowing those voters but NOT itself, so it is a quiet non-voter
    // (it can't campaign while outside its own config — `RaftCore::start_election`
    // gates on `is_voter`) until the leader adds it, then it catches up. ---
    let mut group: BTreeMap<u64, KvNode> = BTreeMap::new();
    for &id in &INITIAL_VOTERS {
        group.insert(
            id,
            RaftKvNode::start(sim.env(id), INITIAL_VOTERS.to_vec(), MemoryEngine::new()),
        );
    }
    group.insert(
        11,
        RaftKvNode::start(sim.env(11), INITIAL_VOTERS.to_vec(), MemoryEngine::new()),
    );

    // --- The epoch-driven-pull reconfigure loop on every group node: poll the
    // control plane's replicated desired replica set for this tablet and step the
    // local group config toward it. Read committed metadata off a control replica
    // (a follower's `metadata()` is committed state). ---
    for (&_id, node) in &group {
        let ctrl = control[0].clone();
        node.spawn_reconfigure_loop(RECONFIGURE_INTERVAL, move || {
            ctrl.metadata()
                .tablets
                .get(&TABLET)
                .map(|t| t.replicas.iter().copied().collect())
        });
    }

    sim.run_for(Duration::from_secs(2));
    let cl = control_leader(&control);

    // Register members + place the tablet under the policy.
    for (id, region, zone) in DATA_NODES {
        assert!(matches!(
            control[cl].propose(MetaCommand::UpsertMember {
                node: id,
                labels: labels(region, zone),
                status: NodeStatus::Active,
            }),
            ProposeResult::Accepted { .. }
        ));
    }
    assert!(matches!(
        control[cl].propose(MetaCommand::CreateTablet {
            tablet: TABLET,
            table: None,
            range: KeyRange::whole(),
            replicas: INITIAL_VOTERS.to_vec(),
        }),
        ProposeResult::Accepted { .. }
    ));
    assert!(matches!(
        control[cl].propose(MetaCommand::SetTabletPolicy {
            tablet: TABLET,
            policy: Some(policy()),
        }),
        ProposeResult::Accepted { .. }
    ));

    // Settle: members Active, placement stable at {10,12,14}, group config matches.
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        control[cl].metadata().tablets[&TABLET].replicas,
        INITIAL_VOTERS.to_vec(),
        "initial placement drifted (seed={seed})"
    );
    let l0 = group_leader(&group, &GROUP_IDS).expect("a group leader after settling");
    assert_eq!(
        group[&l0].config(),
        set(&INITIAL_VOTERS),
        "group config should match initial placement (seed={seed})"
    );

    // A committed write the new replica must inherit on reconfigure.
    assert!(matches!(
        group[&l0].put(b"k".to_vec(), b"v1".to_vec()),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(1));

    // --- Fault: the placed node 10 crashes (heartbeats stop). ---
    let dead = 10u64;
    let live: Vec<u64> = GROUP_IDS.iter().copied().filter(|&id| id != dead).collect();
    sim.crash(dead);

    // No manual Down, no manual change_membership: detector → reconciler →
    // group-leader reconfigure. Bounded poll until the control plane re-places the
    // tablet AND the group config converges to it (or a generous budget elapses).
    let desired = set(&[11, 12, 14]);
    let mut converged = false;
    for _ in 0..60 {
        sim.run_for(Duration::from_secs(1));
        let placed: BTreeSet<NodeId> = control[1]
            .metadata()
            .tablets
            .get(&TABLET)
            .map(|t| t.replicas.iter().copied().collect())
            .unwrap_or_default();
        let cfg_ok = live
            .iter()
            .filter_map(|id| group.get(id))
            .all(|n| n.config() == desired);
        if placed == desired && cfg_ok {
            converged = true;
            break;
        }
    }
    assert!(
        converged,
        "tablet group did not auto-reconfigure to {desired:?} (placed={:?}, configs={:?}, seed={seed})",
        control[1].metadata().tablets[&TABLET].replicas,
        live.iter()
            .filter_map(|id| group.get(id).map(|n| (*id, n.config())))
            .collect::<Vec<_>>(),
    );

    // The dead node was dropped, the spare was added, on every live group node.
    for &id in &live {
        assert_eq!(
            group[&id].config(),
            desired,
            "node {id} did not adopt the reconfigured group (seed={seed})"
        );
    }

    // The group still serves: the spare inherited the pre-fault write, and a fresh
    // write replicates across the new configuration.
    let l1 = group_leader(&group, &live).expect("a group leader after reconfigure");
    assert_eq!(
        lin_read(&mut sim, &group[&l1], b"k", Duration::from_secs(2)),
        Some(b"v1".to_vec()),
        "linearizable read lost the pre-fault write after reconfigure (seed={seed})"
    );
    assert!(matches!(
        group[&l1].put(b"k2".to_vec(), b"v2".to_vec()),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));
    for &id in &desired {
        assert_eq!(
            block_on(group[&id].local_get(b"k2")),
            Some(b"v2".to_vec()),
            "node {id} (incl. the added spare) missing the post-reconfigure write (seed={seed})"
        );
    }
}

#[test]
fn auto_reconfigure_is_reproducible_from_seed() {
    fn trace(seed: u64) -> Vec<String> {
        let sim = Simulator::new(seed);
        let control: Vec<RaftNode<SimEnv>> = CONTROL
            .iter()
            .map(|&id| RaftNode::start(sim.env(id), CONTROL.to_vec()))
            .collect();
        for (id, _, _) in DATA_NODES {
            let env = sim.env(id);
            env.spawn_task(heartbeat_loop(env.clone(), CONTROL.to_vec()));
        }
        let mut group: BTreeMap<u64, KvNode> = BTreeMap::new();
        for &id in &INITIAL_VOTERS {
            group.insert(
                id,
                RaftKvNode::start(sim.env(id), INITIAL_VOTERS.to_vec(), MemoryEngine::new()),
            );
        }
        group.insert(
            11,
            RaftKvNode::start(sim.env(11), INITIAL_VOTERS.to_vec(), MemoryEngine::new()),
        );
        for (&_id, node) in &group {
            let ctrl = control[0].clone();
            node.spawn_reconfigure_loop(RECONFIGURE_INTERVAL, move || {
                ctrl.metadata()
                    .tablets
                    .get(&TABLET)
                    .map(|t| t.replicas.iter().copied().collect())
            });
        }
        let mut sim = sim;
        sim.run_for(Duration::from_secs(2));
        let cl = control_leader(&control);
        for (id, region, zone) in DATA_NODES {
            control[cl].propose(MetaCommand::UpsertMember {
                node: id,
                labels: labels(region, zone),
                status: NodeStatus::Active,
            });
        }
        control[cl].propose(MetaCommand::CreateTablet {
            tablet: TABLET,
            table: None,
            range: KeyRange::whole(),
            replicas: INITIAL_VOTERS.to_vec(),
        });
        control[cl].propose(MetaCommand::SetTabletPolicy {
            tablet: TABLET,
            policy: Some(policy()),
        });
        sim.run_for(Duration::from_secs(2));
        sim.crash(10);
        sim.run_for(Duration::from_secs(10));
        sim.trace_lines()
    }
    assert_eq!(trace(0xD00D), trace(0xD00D));
}
