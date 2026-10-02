//! Deterministic regression for issue #781: dropping one **healthy** voter
//! (never a `Down` one) from an otherwise-idle tablet group's replica set,
//! through the real per-node reconfigure loop (`RaftKvNode::
//! spawn_reconfigure_loop` / `reconfigure_step`) and a simulated heartbeat
//! cadence, converges within a bounded budget and the removed replica does
//! not go on to disrupt the surviving group.
//!
//! `reconfigure_trigger.rs` in this same directory covers the **failure**
//! path end to end (a node crashes, gets marked `Down`, and the reconciler
//! auto-replaces it onto a spare). This file is its "drop a healthy voter"
//! sibling — the coverage gap issue #781's investigation found: no
//! deterministic test drove "drop one healthy voter from an idle group
//! through the real reconciler + simulated heartbeat cadence" end to end,
//! nor checked that the removed node does not disrupt the group afterwards.
//! `animusd/tests/cp_reconfigure.rs::cp_group_follows_tablet_replica_set` is
//! this file's real-`ProdEnv`/real-TCP sibling — it proves the same shape is
//! wired correctly in production, but (being real-time) can't pin a seed or
//! assert the removed node's long-run silence the way a `SimEnv` corpus can.
//!
//! Two scenarios, both driven by a direct `MetaCommand::CasTabletReplicas`
//! (no failure detector involved anywhere — this is a plain drop of a live,
//! healthy voter, never a `Down` one):
//! - drop a **follower**: `reconfigure_step`'s ordinary "remove a healthy
//!   extra voter" path (`lib.rs` ~4246-4401).
//! - drop the **leader**: `reconfigure_step`'s "must remove self" path, which
//!   arms a leadership transfer to a caught-up desired member before the new
//!   leader removes the old one (`lib.rs` ~4353-4401).
//!
//! Deterministic + seed-reproducible (ADR 0003): drive with `run_for`, never
//! `run()` (the driver has perpetual heartbeat/election timers). Replay a
//! specific pinned-test run with `ANIMUS_SEED=<seed> cargo test -p
//! animus-cp-data --test reconfigure_healthy_drop`. Corpus depth knob:
//! `ANIMUS_RECONFIGURE_DROP_SEEDS` (default 1).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::{MetaCommand, NodeStatus, ProposeResult, RaftNode};
use animus_cp_data::RaftKvNode;
use animus_env::{EnvExt, NodeId, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, TabletId};
use animus_test::corpus;
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

const CONTROL: [u64; 3] = [0, 1, 2];
const TABLET: TabletId = TabletId(1);
/// The tablet group's own three data-plane ids (distinct from `CONTROL`'s,
/// mirroring `reconfigure_trigger.rs`'s id-space split).
const DATA_NODES: [u64; 3] = [10, 11, 12];
/// How often each group node polls control metadata and steps its config —
/// same value `reconfigure_trigger.rs` uses.
const RECONFIGURE_INTERVAL: Duration = Duration::from_millis(200);
/// Bounded convergence budget: iterations of 1s virtual time each.
const CONVERGE_ITERS: u32 = 30;

fn set(ids: &[u64]) -> BTreeSet<NodeId> {
    ids.iter().copied().map(nid).collect()
}

/// The control leader index, asserting exactly one among `0..3`.
fn control_leader(nodes: &[RaftNode<SimEnv>]) -> usize {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(ls.len(), 1, "expected one control leader, got {ls:?}");
    ls[0]
}

/// The current group leader's id among `live`, if exactly one.
fn group_leader(group: &BTreeMap<u64, KvNode>, live: &[u64]) -> Option<u64> {
    let ls: Vec<u64> = live
        .iter()
        .copied()
        .filter(|id| group.get(id).is_some_and(|n| n.is_leader()))
        .collect();
    if ls.len() == 1 { Some(ls[0]) } else { None }
}

/// Run a linearizable read on `node` to completion (spawned, since it awaits
/// a read-barrier round), driving the sim up to `budget`.
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

/// Bring up the 3-node control plane + 3-voter tablet group harness (mirrors
/// `reconfigure_trigger.rs`'s harness, minus the spare/failure-cascade
/// machinery this file doesn't need — the mutation here is a direct
/// `CasTabletReplicas`, never a `Down` cascade), write one key, confirm the
/// group forms with 3 voters and a leader, then let it go idle for a few
/// seconds of virtual time before returning.
fn setup(seed: u64) -> (Simulator, Vec<RaftNode<SimEnv>>, BTreeMap<u64, KvNode>) {
    let mut sim = Simulator::new(seed);

    let control: Vec<RaftNode<SimEnv>> = CONTROL
        .iter()
        .map(|&id| {
            RaftNode::start(
                sim.env(nid(id)),
                CONTROL.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();

    let mut group: BTreeMap<u64, KvNode> = BTreeMap::new();
    for &id in &DATA_NODES {
        group.insert(
            id,
            RaftKvNode::start(
                sim.env(nid(id)),
                DATA_NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            ),
        );
    }

    // The real per-node reconfigure loop on every group node: poll the
    // control plane's replicated desired replica set for this tablet and
    // step the local group config toward it. `down` always reports empty
    // here — nothing in this file's two scenarios ever marks a member
    // `Down`; the mutation is always a direct, healthy-voter
    // `CasTabletReplicas`.
    for node in group.values() {
        let ctrl = control[0].clone();
        let ctrl_down = control[0].clone();
        node.spawn_reconfigure_loop(
            RECONFIGURE_INTERVAL,
            move || {
                ctrl.metadata()
                    .tablets
                    .get(&TABLET)
                    .map(|t| t.replicas.iter().cloned().collect())
            },
            move || {
                ctrl_down
                    .metadata()
                    .members
                    .iter()
                    .filter(|(_, m)| m.status == NodeStatus::Down)
                    .map(|(id, _)| id.clone())
                    .collect()
            },
        );
    }

    sim.run_for(Duration::from_secs(2));
    let cl = control_leader(&control);

    for &id in &DATA_NODES {
        assert!(matches!(
            control[cl].propose(MetaCommand::UpsertMember {
                node: nid(id),
                labels: BTreeMap::new(),
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
            replicas: DATA_NODES.iter().copied().map(nid).collect(),
        }),
        ProposeResult::Accepted { .. }
    ));

    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        control[cl].metadata().tablets[&TABLET].replicas,
        DATA_NODES.into_iter().map(nid).collect::<Vec<_>>(),
        "initial placement drifted (seed={seed})"
    );
    let l0 = group_leader(&group, &DATA_NODES).expect("a group leader after settling");
    assert_eq!(
        group[&l0].config(),
        set(&DATA_NODES),
        "group config should match initial placement (seed={seed})"
    );

    assert!(matches!(
        group[&l0].put(b"k".to_vec(), b"v1".to_vec()),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(1));

    // Idle for a few virtual seconds before either scenario's drop — #781's
    // gap was specifically an idle group's healthy-voter drop, not one racing
    // an in-flight write.
    sim.run_for(Duration::from_secs(3));

    (sim, control, group)
}

/// Drop `victim` (a currently-live voter, healthy — never `Down`) from the
/// tablet's replicas via a direct `CasTabletReplicas`, then assert (a) some
/// live node converges to leadership over exactly the two kept voters within
/// `CONVERGE_ITERS` seconds, (b) after convergence, running the sim further
/// leaves that leader's term and identity unchanged (the removed node does
/// not disrupt the group), and (c) a linearizable read of the pre-drop write
/// still succeeds via the new leader, plus a fresh post-convergence write
/// replicates to both kept nodes.
fn run_drop_scenario(
    mut sim: Simulator,
    control: Vec<RaftNode<SimEnv>>,
    group: BTreeMap<u64, KvNode>,
    victim: u64,
    victim_label: &str,
    seed: u64,
) {
    let cl = control_leader(&control);

    let kept: Vec<u64> = DATA_NODES
        .iter()
        .copied()
        .filter(|&id| id != victim)
        .collect();
    assert_eq!(kept.len(), 2, "seed={seed}");
    let desired = set(&kept);

    let epoch = control[cl].metadata().tablets[&TABLET].epoch;
    assert!(matches!(
        control[cl].propose(MetaCommand::CasTabletReplicas {
            tablet: TABLET,
            expected_epoch: epoch,
            replicas: kept.iter().copied().map(nid).collect(),
        }),
        ProposeResult::Accepted { .. }
    ));

    // (a) bounded convergence: some live (kept) node leads with exactly the
    // two kept voters.
    let mut converged: Option<u64> = None;
    for _ in 0..CONVERGE_ITERS {
        sim.run_for(Duration::from_secs(1));
        if let Some(l) = kept.iter().copied().find(|id| {
            group
                .get(id)
                .is_some_and(|n| n.is_leader() && n.config() == desired)
        }) {
            converged = Some(l);
            break;
        }
    }
    let leader = converged.unwrap_or_else(|| {
        panic!(
            "group did not converge to {desired:?} within {CONVERGE_ITERS}s after dropping the \
             HEALTHY {victim_label} {victim} (seed={seed}); last observed configs={:?}",
            kept.iter()
                .filter_map(|id| group.get(id).map(|n| (*id, n.is_leader(), n.config())))
                .collect::<Vec<_>>(),
        )
    });
    if victim_label == "leader" {
        assert_ne!(
            leader, victim,
            "the removed leader {victim} is still reporting itself as this group's leader \
             after convergence (seed={seed})"
        );
    }

    // (b) after convergence, the removed node must not disrupt the group:
    // run further virtual time and confirm the leader's term and identity
    // hold. If this fails, it is a real production finding — a removed
    // voter deposing the leader — not a test artifact.
    let term0 = group[&leader].term();
    sim.run_for(Duration::from_secs(5));
    let term1 = group[&leader].term();
    assert_eq!(
        term0, term1,
        "REAL PRODUCTION FINDING (not a test artifact): removing the healthy {victim_label} \
         {victim} disrupted the group — leader {leader}'s Raft term changed after convergence \
         with no further fault injected ({term0} -> {term1}), i.e. a removed voter deposed the \
         leader (seed={seed})"
    );
    assert!(
        group[&leader].is_leader(),
        "REAL PRODUCTION FINDING (not a test artifact): leader {leader} lost leadership after \
         convergence with no further fault injected, following the removal of the healthy \
         {victim_label} {victim} (seed={seed})"
    );
    assert!(
        !group[&victim].is_leader(),
        "REAL PRODUCTION FINDING (not a test artifact): the removed {victim_label} {victim} is \
         reporting itself as this group's leader after convergence (seed={seed})"
    );

    // (c) a linearizable read of the pre-drop write still succeeds via the
    // new leader.
    assert_eq!(
        lin_read(&mut sim, &group[&leader], b"k", Duration::from_secs(2)),
        Some(b"v1".to_vec()),
        "linearizable read of the pre-drop write failed via {leader} after removing the \
         healthy {victim_label} {victim} (seed={seed})"
    );

    // Every kept node adopted the reconfigured group.
    for &id in &kept {
        assert_eq!(
            group[&id].config(),
            desired,
            "node {id} did not adopt the reconfigured group after dropping the healthy \
             {victim_label} {victim} (seed={seed})"
        );
    }

    // A fresh post-convergence write replicates to both kept nodes — the
    // group genuinely keeps serving, not just electing.
    assert!(matches!(
        group[&leader].put(b"k2".to_vec(), b"v2".to_vec()),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));
    for &id in &kept {
        assert_eq!(
            block_on(group[&id].local_get(b"k2")),
            Some(b"v2".to_vec()),
            "node {id} missing the post-reconfigure write after dropping the healthy \
             {victim_label} {victim} (seed={seed})"
        );
    }
}

fn run_drop_follower(seed: u64) {
    let (sim, control, group) = setup(seed);
    let l0 = group_leader(&group, &DATA_NODES).expect("a group leader before the drop");
    let follower = DATA_NODES
        .iter()
        .copied()
        .find(|&id| id != l0)
        .expect("a follower exists");
    run_drop_scenario(sim, control, group, follower, "follower", seed);
}

fn run_drop_leader(seed: u64) {
    let (sim, control, group) = setup(seed);
    let l0 = group_leader(&group, &DATA_NODES).expect("a group leader before the drop");
    run_drop_scenario(sim, control, group, l0, "leader", seed);
}

#[test]
fn healthy_follower_drop_converges_and_does_not_disrupt() {
    let seed = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0x7810_0001);
    run_drop_follower(seed);
}

#[test]
fn healthy_leader_drop_converges_via_transfer_and_does_not_disrupt() {
    let seed = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0x7810_0002);
    run_drop_leader(seed);
}

/// Depth knob (`ANIMUS_RECONFIGURE_DROP_SEEDS`, default 1) — mirrors
/// `ANIMUS_RECONCILER_SEEDS`/`ANIMUS_RAFTKV_SEEDS`/`ANIMUS_SPLIT_SEEDS`.
fn seeds_per_cell() -> u64 {
    corpus::seeds_from_env("ANIMUS_RECONFIGURE_DROP_SEEDS") as u64
}

#[test]
fn healthy_drop_over_seeds() {
    for round in 0..seeds_per_cell() {
        let seed = 0x7810_1000 + round;
        run_drop_follower(seed);
        run_drop_leader(seed);
    }
}
