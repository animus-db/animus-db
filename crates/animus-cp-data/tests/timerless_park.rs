//! ADR 0044 phase-1 PR2: the mechanism (not yet the policy) for a quiesced
//! group's **timerless park**. `RaftCore::next_deadline` now returns
//! `Option<Nanos>`, and both drivers drop the timer arm from their `select`
//! entirely on `None` — but nothing in the core can produce `None` yet (that's
//! phase-1 PR3's `quiesce_after` state machine), so this PR's own gate is that
//! behavior stays byte-identical (proven by the rest of this crate's suite
//! passing unchanged).
//!
//! What *is* new and testable here is finding 4's hazard 1, fixed **ahead of
//! need**: `shutdown()` now also notifies a `WakeSignal` the consensus loop's
//! own `select` races (mirroring the apply task's `ApplySignal` from PR1) —
//! today this is a no-op in effect, since the consensus loop already re-wakes
//! within one heartbeat interval on its own, but it is exactly the plumbing a
//! genuinely timerless-parked (quiesced) group will depend on in PR3 to ever
//! notice a shutdown request at all.
//!
//! This test proves the wake fires **on the signal, not the natural Raft
//! timer**: it shuts down the group's leader and checks `is_stopped()` within
//! a window far shorter than one heartbeat interval (50ms) — short enough that
//! only an explicit wake, not the leader's own heartbeat_deadline, could
//! explain a prompt exit.

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use futures::executor::block_on;

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

fn leader(nodes: &[KvNode], seed: u64) -> usize {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(ls.len(), 1, "expected one leader, got {ls:?} (seed={seed})");
    ls[0]
}

/// `shutdown()` on the group's leader is observed **well inside one heartbeat
/// interval** (50ms) — a bound only reachable via the new driver-level
/// `WakeSignal` (finding 4's hazard 1 fix, landed ahead of need in this PR),
/// never the leader's own `next_deadline` (`heartbeat_deadline`, up to 50ms
/// out at the moment `shutdown` is called).
#[test]
fn shutdown_of_the_leader_is_observed_far_faster_than_one_heartbeat_interval() {
    let seed = 0xF00D1;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect + settle onto a steady heartbeat cadence

    let l = leader(&nodes, seed);
    assert!(!nodes[l].is_stopped());
    nodes[l].shutdown();

    // 3ms: a small fraction of the 50ms heartbeat interval and the 150ms+
    // election timeout alike, so this is only reachable via the wake signal.
    sim.run_for(Duration::from_millis(3));

    assert!(
        nodes[l].is_stopped(),
        "shutdown of the leader must be observed well within one heartbeat \
         interval, not left to the natural heartbeat timer (seed={seed})"
    );
}

/// The same property for a **follower**: its own timer is the (longer, more
/// randomized) election timeout, so the margin is even wider, but the wake
/// must still fire immediately rather than waiting on it.
#[test]
fn shutdown_of_a_follower_is_observed_far_faster_than_one_election_timeout() {
    let seed = 0xF00D2;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));

    let l = leader(&nodes, seed);
    let follower = (0..3).find(|&i| i != l).expect("a follower exists");
    assert!(!nodes[follower].is_stopped());
    nodes[follower].shutdown();

    sim.run_for(Duration::from_millis(3));

    assert!(
        nodes[follower].is_stopped(),
        "shutdown of a follower must be observed well within one election \
         timeout, not left to its own election timer (seed={seed})"
    );
}

/// `RaftKvNode::wake()` (the PR4 hook, unused for now beyond this) is a safe
/// no-op on a live group: it never itself changes committed state, and a
/// write proposed right after it still commits normally.
#[test]
fn wake_on_a_live_group_is_an_inert_no_op() {
    let seed = 0xF00D3;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));

    let l = leader(&nodes, seed);
    nodes[l].wake();
    nodes[l].wake(); // idempotent under repeated calls too

    match nodes[l].put(b"k".to_vec(), b"v".to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("leader rejected put after wake(): {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(1));
    assert_eq!(
        block_on(nodes[l].local_get(b"k")),
        Some(b"v".to_vec()),
        "a write proposed right after wake() must still commit normally (seed={seed})"
    );
}
