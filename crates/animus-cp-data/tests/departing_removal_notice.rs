//! Issue #1061 end to end on the CP data plane: a replica removed from a
//! tablet's Raft group must learn it was removed — through an explicit
//! `RaftMsg::Removed` notice, never a snapshot — and its host reconciler must
//! then release it, once (and only once) replicated `Metadata` also excludes
//! the node.
//!
//! Before the fix a departing peer that had fallen behind the leader's
//! compacted log was shipped a full chunked `InstallSnapshot`, restarted from
//! chunk 0 by every later compaction for as long as the writer ran (even
//! when the peer was dead), and a peer that merely sat out the removal
//! behind a partition never learned of it at all — the host reconciler
//! releases a replica only when its own log-derived config excludes it, so
//! the group stayed hosted forever as a zombie.
//!
//! Two cells, both seed-reproducible (`ANIMUS_SEED=<seed>`):
//!
//! 1. `a_removed_replica_behind_the_compacted_log_is_told_not_snapshotted_and_released`
//!    — a real reconciler-hosted replica, partitioned, removed, left behind
//!    the compacted log by a continuous writer, then healed: no snapshot is
//!    ever shipped (`CpSnapshotShips`/`CpSnapshotTransferRestarts` stay at
//!    0), the replica records its removal and stops campaigning, the
//!    reconciler does NOT release it while `Metadata` still lists it, and
//!    does once `Metadata` excludes it.
//! 2. `a_dead_departing_replica_costs_no_snapshots_and_is_eventually_dropped`
//!    — the peer never comes back: the leader's traffic to it stays a
//!    capped-backoff trickle of tiny notices (never a snapshot) under a
//!    continuous writer, and it is dropped from the leader's bookkeeping
//!    after the give-up bound.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::{DEPARTING_NOTICE_GIVE_UP, ProposeResult};
use animus_cp_data::host::{MemoryTabletEngines, MetadataView, Reconciler};
use animus_cp_data::{RaftKvNode, StorageScope};
use animus_env::{Clock, Env, EnvExt, Metric, MetricsHandle, NodeId, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, Tablet, TabletId};

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

const BASE: u64 = 300;
const OTHER: u64 = 301;
const THIRD: u64 = 302;

fn tablet(replicas: Vec<u64>) -> Tablet {
    let mut t = Tablet::new(
        TabletId(1),
        KeyRange::whole(),
        replicas.into_iter().map(nid).collect(),
    );
    t.table = None;
    t
}

fn view(replicas: Vec<u64>) -> MetadataView {
    let mut v = MetadataView::default();
    let t = tablet(replicas);
    v.tablets.insert(t.id, t);
    v
}

fn seed_from_env(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn sum(nodes: &[&SimEnv], m: Metric) -> u64 {
    nodes.iter().map(|e| e.metrics().get(m)).sum()
}

/// One burst of writes, synchronously (no yield) — the batching idiom
/// `learner_snapshot_livelock_under_continuous_writer.rs` documents.
fn burst(node: &KvNode, tag: &str, round: u64, n: u64) {
    for i in 0..n {
        let res = node.put(format!("{tag}-{round}-{i}").into_bytes(), vec![b'v'; 200]);
        assert!(
            matches!(res, ProposeResult::Accepted { .. }),
            "write {tag}-{round}-{i} must be locally accepted, got {res:?}"
        );
    }
}

fn scenario_release(seed: u64) {
    let mut sim = Simulator::new(seed);
    let base_env = sim.env(nid(BASE));
    let other_env = sim.env(nid(OTHER));
    let third_env = sim.env(nid(THIRD));
    let voters: Vec<NodeId> = [BASE, OTHER, THIRD].into_iter().map(nid).collect();

    // BASE is hosted by a real reconciler (so `TabletFacts` is gathered the
    // production way); it publishes its hosted node and the reconciler's own
    // hosted set for the test thread to observe.
    let view_slot: Arc<Mutex<MetadataView>> = Arc::new(Mutex::new(view(vec![BASE, OTHER, THIRD])));
    let base_node: Arc<Mutex<Option<KvNode>>> = Arc::new(Mutex::new(None));
    let hosted_now: Arc<Mutex<Vec<TabletId>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let (view_slot, base_node, hosted_now) = (
            Arc::clone(&view_slot),
            Arc::clone(&base_node),
            Arc::clone(&hosted_now),
        );
        let env = base_env.clone();
        base_env.clone().spawn_task(async move {
            let mut reconciler: Reconciler<SimEnv, MemoryEngine> = Reconciler::new(
                env.clone(),
                MemoryTabletEngines::new(),
                nid(BASE),
                |_t, _n| {},
                |_t| {},
            );
            loop {
                let v = view_slot.lock().unwrap().clone();
                reconciler.tick(&v).await;
                *base_node.lock().unwrap() = reconciler.hosted_node(TabletId(1)).cloned();
                *hosted_now.lock().unwrap() =
                    reconciler.local_state().hosted.iter().copied().collect();
                env.sleep(Duration::from_millis(50)).await;
            }
        });
    }
    let other = KvNode::start_hosted(
        other_env.clone(),
        voters.clone(),
        MemoryEngine::new(),
        StorageScope::new(KeyRange::whole()),
        1,
    );
    let third = KvNode::start_hosted(
        third_env.clone(),
        voters.clone(),
        MemoryEngine::new(),
        StorageScope::new(KeyRange::whole()),
        1,
    );
    sim.run_for(Duration::from_secs(3));
    let base = base_node
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_else(|| panic!("seed={seed:#x}: the reconciler must have hosted tablet 1"));

    // Leadership must sit on one of the two nodes that stay: hand it off if
    // the victim won.
    if base.is_leader() {
        let mut armed = false;
        for _ in 0..50 {
            if base.transfer_leadership(nid(OTHER)) {
                armed = true;
                break;
            }
            sim.run_for(Duration::from_millis(100));
        }
        assert!(armed, "seed={seed:#x}: leadership transfer never armed");
        for _ in 0..50 {
            sim.run_for(Duration::from_millis(100));
            if other.is_leader() {
                break;
            }
        }
    }
    let leader = if other.is_leader() { &other } else { &third };
    assert!(leader.is_leader(), "seed={seed:#x}: a surviving leader");
    burst(leader, "warm", 0, 20);
    sim.run_for(Duration::from_millis(500));

    // The victim drops off the network and is removed.
    for peer in [OTHER, THIRD] {
        sim.partition_pair(nid(BASE), nid(peer));
    }
    let removed = leader.change_membership([OTHER, THIRD].into_iter().map(nid).collect());
    assert!(
        matches!(removed, ProposeResult::Accepted { .. }),
        "seed={seed:#x}: removing the victim must be accepted, got {removed:?}"
    );
    sim.run_for(Duration::from_secs(1));

    // A continuous writer drives several compactions past the victim.
    for round in 0..40u64 {
        burst(leader, "k", round, 100);
        sim.run_for(Duration::from_millis(100));
    }
    assert!(
        leader.snapshot_index() > base.commit_index(),
        "seed={seed:#x}: precondition — the victim must be behind the leader's compacted log \
         (leader snapshot_index {}, victim commit {})",
        leader.snapshot_index(),
        base.commit_index()
    );
    assert!(
        base.config().contains(&nid(BASE)) && !base.removed_by_leader(),
        "seed={seed:#x}: precondition — the partitioned victim still believes it is a member"
    );

    // Heal; the writer never stops.
    let survivors = [&other_env, &third_env];
    let ships_before = sum(&survivors, Metric::CpSnapshotShips);
    let restarts_before = sum(&survivors, Metric::CpSnapshotTransferRestarts);
    let term_at_heal = base.term();
    for peer in [OTHER, THIRD] {
        sim.heal(nid(BASE), nid(peer));
        sim.heal(nid(peer), nid(BASE));
    }
    let mut told_at = None;
    for round in 0..80u64 {
        burst(leader, "h", round, 100);
        sim.run_for(Duration::from_millis(100));
        if told_at.is_none() && base.removed_by_leader() {
            told_at = Some(round);
        }
    }
    let ships = sum(&survivors, Metric::CpSnapshotShips) - ships_before;
    let restarts = sum(&survivors, Metric::CpSnapshotTransferRestarts) - restarts_before;
    assert_eq!(
        ships, 0,
        "seed={seed:#x}: a departing replica must never be shipped a snapshot chunk (issue #1061)"
    );
    assert_eq!(
        restarts, 0,
        "seed={seed:#x}: no snapshot transfer may be (re)started for a departing replica"
    );
    assert!(
        told_at.is_some(),
        "seed={seed:#x}: the replica never recorded its removal — a zombie (issue #1061)"
    );
    assert!(
        !base.is_leader() && base.term() <= leader.term() && base.term() <= term_at_heal + 1,
        "seed={seed:#x}: a removed replica must not campaign (term at heal {term_at_heal}, now {})",
        base.term()
    );
    assert!(
        leader.departing_peers().is_empty(),
        "seed={seed:#x}: the ack must have dropped the peer from the leader's departing set"
    );

    // The reconciler must NOT release it while `Metadata` still lists it, …
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        hosted_now.lock().unwrap().as_slice(),
        &[TabletId(1)],
        "seed={seed:#x}: the removal flag alone must never release a replica still in Metadata"
    );
    // … and must once `Metadata` also excludes it (RELEASE_CONFIRM_TICKS,
    // ADR 0029, still applies).
    *view_slot.lock().unwrap() = view(vec![OTHER, THIRD]);
    let mut released = false;
    for _ in 0..100 {
        sim.run_for(Duration::from_millis(100));
        if hosted_now.lock().unwrap().is_empty() {
            released = true;
            break;
        }
    }
    assert!(
        released,
        "seed={seed:#x}: the reconciler never released the removed replica although Metadata \
         excludes it and the replica knows it was removed (issue #1061)"
    );
}

#[test]
fn a_removed_replica_behind_the_compacted_log_is_told_not_snapshotted_and_released() {
    scenario_release(seed_from_env(0x1061_5001));
}

#[test]
fn a_removed_replica_behind_the_compacted_log_corpus() {
    let k = animus_test::corpus::seeds_from_env("ANIMUS_LEARNER_SEEDS").max(3);
    for i in 0..k {
        scenario_release(animus_test::corpus::name_seed(&format!(
            "removal_notice_release_s{i:03}"
        )));
    }
}

#[test]
fn a_dead_departing_replica_costs_no_snapshots_and_is_eventually_dropped() {
    let seed = seed_from_env(0x1061_5002);
    let mut sim = Simulator::new(seed);
    let ids = [0u64, 1, 2];
    let handles: Vec<MetricsHandle> = ids.iter().map(|_| MetricsHandle::recording()).collect();
    let nodes: Vec<KvNode> = ids
        .iter()
        .enumerate()
        .map(|(i, &id)| {
            RaftKvNode::start_with_metrics(
                sim.env(nid(id)),
                ids.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
                handles[i].clone(),
            )
        })
        .collect();
    sim.run_for(Duration::from_secs(2));
    let l = (0..3)
        .find(|&i| nodes[i].is_leader())
        .expect("an initial leader");
    burst(&nodes[l], "warm", 0, 20);
    sim.run_for(Duration::from_millis(500));

    let victim = (0..3).find(|&i| i != l).unwrap();
    sim.crash(nid(ids[victim]));
    let remaining: std::collections::BTreeSet<NodeId> = ids
        .iter()
        .enumerate()
        .filter(|&(i, _)| i != victim)
        .map(|(_, &id)| nid(id))
        .collect();
    assert!(matches!(
        nodes[l].change_membership(remaining),
        ProposeResult::Accepted { .. }
    ));
    assert!(nodes[l].departing_peers().contains(&nid(ids[victim])));

    // A minute of continuous writing (many compactions).
    let ships_of = |handles: &[MetricsHandle]| -> u64 {
        handles.iter().map(|h| h.get(Metric::CpSnapshotShips)).sum()
    };
    let ships_before = ships_of(&handles);
    for round in 0..600u64 {
        burst(&nodes[l], "k", round, 50);
        sim.run_for(Duration::from_millis(100));
    }
    assert_eq!(
        ships_of(&handles) - ships_before,
        0,
        "seed={seed:#x}: a dead departing peer must never be shipped a snapshot chunk"
    );
    assert!(
        nodes[l].departing_peers().contains(&nid(ids[victim])),
        "seed={seed:#x}: still owed within the give-up bound"
    );

    // Past the give-up bound it is dropped from the leader's bookkeeping.
    sim.run_for(DEPARTING_NOTICE_GIVE_UP + Duration::from_secs(10));
    assert!(
        nodes[l].departing_peers().is_empty(),
        "seed={seed:#x}: a peer silent past the give-up bound must be dropped"
    );
}
