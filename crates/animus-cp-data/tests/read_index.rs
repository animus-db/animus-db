//! Stage B.2 (ADR 0017): **linearizable ReadIndex reads** for the per-tablet Raft
//! KV group. A read on the leader reflects every write committed before it; a
//! deposed (partitioned) leader cannot confirm a read quorum, so it returns
//! `None` rather than a stale value — no wall clock involved.
//!
//! Linearizable reads are async (a read-barrier probe round + applied wait), so
//! we drive them as spawned tasks and `run_for` to let the sim advance.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{EnvExt, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

const NODES: [u64; 3] = [0, 1, 2];

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn group(seed: u64) -> (Simulator, Vec<KvNode>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| {
            RaftKvNode::start(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    (sim, nodes)
}

fn leader(nodes: &[KvNode], live: &[usize], seed: u64) -> usize {
    let ls: Vec<usize> = nodes
        .iter()
        .enumerate()
        .filter(|(i, n)| live.contains(i) && n.is_leader())
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        ls.len(),
        1,
        "expected one leader among {live:?}, got {ls:?} (seed={seed})"
    );
    ls[0]
}

/// Run a linearizable read on `node` to completion (spawned as a task, since it
/// awaits a quorum probe round), driving the sim up to `budget`.
fn lin_read(sim: &mut Simulator, node: &KvNode, key: &[u8], budget: Duration) -> Option<Vec<u8>> {
    let slot: Arc<Mutex<Option<Option<Vec<u8>>>>> = Arc::new(Mutex::new(None));
    let n = node.clone();
    let s = Arc::clone(&slot);
    let k = key.to_vec();
    node.env().clone().spawn_task(async move {
        let v = n.linearizable_get(&k).await;
        *s.lock().unwrap() = Some(v);
    });
    sim.run_for(budget);
    slot.lock()
        .unwrap()
        .clone()
        .expect("linearizable read did not complete")
}

fn put(nodes: &[KvNode], live: &[usize], seed: u64, key: &[u8], value: &[u8]) {
    let l = leader(nodes, live, seed);
    assert!(matches!(
        nodes[l].put(key.to_vec(), value.to_vec()),
        ProposeResult::Accepted { .. }
    ));
}

/// Run a linearizable range scan to completion (spawned, since it awaits a read
/// barrier), driving the sim up to `budget`.
#[allow(clippy::type_complexity)]
fn lin_scan(
    sim: &mut Simulator,
    node: &KvNode,
    start: &[u8],
    end: &[u8],
    budget: Duration,
) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
    let slot: Arc<Mutex<Option<Option<Vec<(Vec<u8>, Vec<u8>)>>>>> = Arc::new(Mutex::new(None));
    let n = node.clone();
    let s = Arc::clone(&slot);
    let (lo, hi) = (start.to_vec(), end.to_vec());
    node.env().clone().spawn_task(async move {
        *s.lock().unwrap() = Some(n.linearizable_scan(&lo, Some(&hi), None).await);
    });
    sim.run_for(budget);
    slot.lock().unwrap().clone().expect("scan did not complete")
}

#[test]
fn linearizable_scan_returns_sorted_live_range() {
    let seed = 0x5CA4;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2], seed);

    // Write keys out of order; the scan must return them key-sorted.
    for (k, v) in [(b"k3", b"v3"), (b"k1", b"v1"), (b"k2", b"v2")] {
        put(&nodes, &[0, 1, 2], seed, k, v);
    }
    sim.run_for(Duration::from_secs(1));

    // Half-open [k1, k3): k1, k2 — not k3.
    assert_eq!(
        lin_scan(&mut sim, &nodes[l], b"k1", b"k3", Duration::from_secs(2)),
        Some(vec![
            (b"k1".to_vec(), b"v1".to_vec()),
            (b"k2".to_vec(), b"v2".to_vec()),
        ]),
    );
    // Whole range covers all three, sorted.
    assert_eq!(
        lin_scan(&mut sim, &nodes[l], b"k0", b"k9", Duration::from_secs(2)),
        Some(vec![
            (b"k1".to_vec(), b"v1".to_vec()),
            (b"k2".to_vec(), b"v2".to_vec()),
            (b"k3".to_vec(), b"v3".to_vec()),
        ]),
    );

    // A non-leader cannot serve a linearizable scan (no read barrier) → None.
    let follower = (0..3).find(|&i| i != l).unwrap();
    assert_eq!(
        lin_scan(
            &mut sim,
            &nodes[follower],
            b"k0",
            b"k9",
            Duration::from_secs(2)
        ),
        None,
    );
}

#[test]
fn linearizable_read_reflects_committed_writes() {
    let seed = 0x1EAD;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2], seed);

    put(&nodes, &[0, 1, 2], seed, b"x", b"1");
    sim.run_for(Duration::from_secs(1));
    assert_eq!(
        lin_read(&mut sim, &nodes[l], b"x", Duration::from_secs(1)),
        Some(b"1".to_vec()),
        "linearizable read must see the committed write (seed={seed})"
    );

    // Read-your-writes across an update.
    put(&nodes, &[0, 1, 2], seed, b"x", b"2");
    sim.run_for(Duration::from_secs(1));
    assert_eq!(
        lin_read(&mut sim, &nodes[l], b"x", Duration::from_secs(1)),
        Some(b"2".to_vec()),
    );

    // An absent key reads as None (not a phantom).
    assert_eq!(
        lin_read(&mut sim, &nodes[l], b"absent", Duration::from_secs(1)),
        None
    );
}

#[test]
fn deposed_leader_does_not_serve_a_stale_read() {
    let seed = 0xDEAD;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));

    put(&nodes, &[0, 1, 2], seed, b"x", b"old");
    sim.run_for(Duration::from_secs(1));

    // Partition the leader away; the survivors elect a new leader and accept a
    // newer write the old leader never sees.
    let old = leader(&nodes, &[0, 1, 2], seed);
    let survivors: Vec<usize> = (0..3).filter(|&i| i != old).collect();
    for &s in &survivors {
        sim.partition_pair(nid(old as u64), nid(s as u64));
    }
    sim.run_for(Duration::from_secs(3));
    put(&nodes, &survivors, seed, b"x", b"new");
    sim.run_for(Duration::from_secs(2));

    // The isolated old leader still *believes* it leads its (stale) term, but a
    // linearizable read cannot collect a quorum ack — so it returns None, never
    // the stale "old". (Read timeout is 5s; give it room to expire.)
    let stale = lin_read(&mut sim, &nodes[old], b"x", Duration::from_secs(7));
    assert_eq!(
        stale, None,
        "a deposed leader must not serve a stale linearizable read (seed={seed})"
    );

    // The new leader serves the up-to-date value.
    let new = leader(&nodes, &survivors, seed);
    assert_eq!(
        lin_read(&mut sim, &nodes[new], b"x", Duration::from_secs(1)),
        Some(b"new".to_vec()),
    );
}

fn set(ids: &[u64]) -> BTreeSet<u64> {
    ids.iter().copied().collect()
}

/// Regression (ADR 0029): the read-barrier's quorum-of-acks bookkeeping
/// (`majority`, and which peers get probed) must track the group's **current**
/// Raft voter config, not `all_nodes` — the peer set a node happened to be
/// *hosted/started with*. `membership.rs`'s `add_a_node_grows_the_group_and_
/// catches_it_up` already exercises adding a node, but that test's joiner is
/// started already knowing the whole eventual `all_nodes` set, and it only
/// checks `local_get`, never a linearizable read — so it cannot see this bug.
///
/// Here nodes 0/1/2 each started knowing only `{0, 1, 2}` and never learn
/// otherwise (their `all_nodes` is fixed at hosting time). The group is then
/// rebalanced from `{0, 1, 2}` to `{2, 3, 4}` — **two** of three original
/// members replaced, leaving only node 2 in common, exactly the shape that
/// actually broke in production (a tablet fully rotated off its original
/// hosts) — and nodes 0/1 are stopped outright, as the removed-replica GC
/// (ADR 0029 §3) eventually does. Node 2's stale `all_nodes = {0, 1, 2}`
/// intersects the *current* config `{2, 3, 4}` in only itself: before the fix,
/// its read barrier can only ever self-ack, never reaching the majority of 2
/// a 3-voter group needs, so a linearizable read on it times out and
/// incorrectly reports the key absent forever after such a move.
#[test]
fn linearizable_read_succeeds_after_a_full_membership_rotation() {
    let seed = 0xBEAD;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    put(&nodes, &[0, 1, 2], seed, b"k", b"v");
    sim.run_for(Duration::from_secs(1));

    // Nodes 3 and 4 join as quiet **non-voters** the way a real
    // reconciler-placed spare does (`cp_join_host`'s "others" shape):
    // `all_nodes` excludes self, so neither can campaign before a leader's
    // config entry actually adds it.
    let node3 = RaftKvNode::start(sim.env(nid(3)), vec![nid(1), nid(2)], MemoryEngine::new());
    let node4 = RaftKvNode::start(sim.env(nid(4)), vec![nid(1), nid(2)], MemoryEngine::new());
    sim.run_for(Duration::from_secs(1));

    // Four single-server steps rotate {0,1,2} -> {2,3,4}: add 3, add 4, remove
    // 0, remove 1 — the sequence a real `reconfigure_step`-driven series of
    // healthy moves takes (never dropping below the original 3-voter margin
    // until each newcomer has caught up).
    for step in [
        set(&[0, 1, 2, 3]),
        set(&[0, 1, 2, 3, 4]),
        set(&[1, 2, 3, 4]),
        set(&[2, 3, 4]),
    ] {
        let l = leader(&nodes, &[0, 1, 2], seed);
        assert!(
            matches!(
                nodes[l].change_membership(step.clone().into_iter().map(nid).collect()),
                ProposeResult::Accepted { .. }
            ),
            "leader {l} rejected the step to {step:?} (seed={seed})"
        );
        sim.run_for(Duration::from_secs(2));
    }

    // Stop the fully-departed nodes outright — mirroring the removed-replica
    // GC. Without this, a still-live-but-no-longer-voting node would still
    // ack a `ReadProbe` on term match alone, which could mask this bug.
    nodes[0].shutdown();
    nodes[1].shutdown();
    sim.run_for(Duration::from_secs(1));

    // Find whichever of the surviving nodes {2, 3, 4} currently leads and read
    // through it.
    let candidates = [(2usize, &nodes[2]), (3, &node3), (4, &node4)];
    let (leader_id, leader_node) = candidates
        .into_iter()
        .find(|(_, n)| n.is_leader())
        .expect("exactly one of {2,3,4} should lead after the rotation");
    assert_eq!(
        leader_node.config(),
        set(&[2, 3, 4]).into_iter().map(nid).collect(),
        "expected the rotation to have fully converged (leader={leader_id}, seed={seed})"
    );
    assert_eq!(
        lin_read(&mut sim, leader_node, b"k", Duration::from_secs(7)),
        Some(b"v".to_vec()),
        "a linearizable read must succeed once the group's read barrier reaches the current quorum, not a stale hosting-time peer set (leader={leader_id}, seed={seed})"
    );
}
