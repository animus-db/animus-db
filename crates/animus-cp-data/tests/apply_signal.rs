//! ADR 0044 phase-1 PR1: the apply task's idle back-off used to be an
//! unconditional `env.sleep(APPLY_IDLE_POLL)` (5ms) every time there was
//! nothing to merge — ~200 wakeups/s per hosted group at complete idle. It now
//! races a new `ApplySignal` (raised by the consensus loop at every point that
//! can create apply work, and by `shutdown()`) against a much longer
//! `APPLY_SAFETY_POLL` (250ms).
//!
//! Three properties this corpus proves, deterministically (ADR 0003):
//!
//! - **The signal path is prompt.** A fresh commit on a group that has been
//!   idle far longer than `APPLY_SAFETY_POLL` is still applied within a small
//!   fraction of that safety poll — proving the apply task actually woke on
//!   the signal, not on the fallback timer.
//! - **The parked-task shutdown hazard is fixed.** `shutdown()` on an idle
//!   group's node completes (`is_stopped()`) within that same small window,
//!   not after waiting out the (now much longer) safety poll — the exact
//!   hazard the plan's finding 4 named.
//! - **A signal-less transition still converges off the safety poll alone.**
//!   `RaftCore::take_snapshot_needed` (the lazy on-demand snapshot-image-build
//!   request) is set purely by the leader's own heartbeat/replicate cycle when
//!   it discovers a follower's `next_index` behind the truncated log prefix —
//!   with **no** commit advance and no `mark_durable_through` call anywhere in
//!   that transition, so none of `ApplySignal`'s raise points fire for it. The
//!   apply task can only ever learn of it by re-checking
//!   `take_snapshot_needed()` — which happens only when `apply_and_compact`
//!   runs again, i.e. only via the safety poll once the task has gone idle.

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

fn leader(nodes: &[KvNode], live: &[usize], seed: u64) -> usize {
    let ls: Vec<usize> = live
        .iter()
        .copied()
        .filter(|&i| nodes[i].is_leader())
        .collect();
    assert_eq!(
        ls.len(),
        1,
        "expected one leader among {live:?}, got {ls:?} (seed={seed})"
    );
    ls[0]
}

/// A commit proposed after the group has been idle for well over
/// `APPLY_SAFETY_POLL` (250ms) is applied within a small fraction of that
/// interval — the wake-on-commit signal path, not the fallback timer.
#[test]
fn idle_group_applies_a_fresh_commit_promptly() {
    let seed = 0xA991;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect + settle
    // Let the apply task go fully idle and park on the signal/safety-poll race —
    // several safety-poll intervals with nothing happening.
    sim.run_for(Duration::from_secs(2));

    let l = leader(&nodes, &[0, 1, 2], seed);
    match nodes[l].put(b"k".to_vec(), b"v".to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("leader rejected put: {other:?} (seed={seed})"),
    }

    // Far shorter than APPLY_SAFETY_POLL (250ms): only reachable if the apply
    // task woke on the signal, not the safety-poll fallback.
    sim.run_for(Duration::from_millis(60));

    assert_eq!(
        block_on(nodes[l].local_get(b"k")),
        Some(b"v".to_vec()),
        "a fresh commit on an idle group must be applied well within one \
         APPLY_SAFETY_POLL interval (seed={seed})"
    );
}

/// `shutdown()` on an idle group's node completes promptly — the parked-task
/// hazard the plan's finding 4 named: `apply_loop`'s idle back-off now waits
/// up to `APPLY_SAFETY_POLL` (250ms, was 5ms) unless `shutdown` itself raises
/// `ApplySignal` to wake a parked task.
#[test]
fn shutdown_of_an_idle_group_stops_promptly() {
    let seed = 0xB110;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect + settle
    sim.run_for(Duration::from_secs(2)); // idle well past one safety poll

    let l = leader(&nodes, &[0, 1, 2], seed);
    let follower = (0..3).find(|&i| i != l).expect("a follower exists");
    assert!(!nodes[follower].is_stopped());
    nodes[follower].shutdown();

    // Far shorter than APPLY_SAFETY_POLL: only reachable if `shutdown` woke the
    // parked apply task directly instead of leaving it to the safety poll.
    sim.run_for(Duration::from_millis(60));

    assert!(
        nodes[follower].is_stopped(),
        "shutdown of an idle group must be observed by the apply task well \
         within one APPLY_SAFETY_POLL interval (seed={seed})"
    );
}

/// The signal-less path: an on-demand snapshot-image build request
/// (`take_snapshot_needed`) is set purely by the leader's heartbeat/replicate
/// cycle discovering a reconnected follower's log has been compacted away —
/// no commit advance, no `mark_durable_through` call, so none of
/// `ApplySignal`'s raise points fire. The apply task must still notice and
/// build the image via `APPLY_SAFETY_POLL` alone, and the follower must still
/// converge (bounded, not stalled).
#[test]
fn apply_converges_via_safety_poll_on_a_signal_less_snapshot_build() {
    let seed = 0xC0DE1;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect
    let l = leader(&nodes, &[0, 1, 2], seed);
    let lagging = (0..3).find(|&i| i != l).expect("a follower exists");

    // Crash the lagging follower so it stays behind while the leader compacts.
    sim.crash(nid(lagging as u64));

    // Write well past the compaction threshold (64) so the leader truncates the
    // log prefix the crashed follower would have needed, and let it settle +
    // go fully idle (several safety-poll intervals of nothing happening) before
    // the follower ever comes back — so when the leader's heartbeat cycle later
    // discovers the follower needs a snapshot, the apply task is genuinely
    // parked and has no signal telling it so.
    const N: u64 = 150;
    for i in 0..N {
        match nodes[l].put(
            format!("k{i:03}").into_bytes(),
            format!("v{i}").into_bytes(),
        ) {
            ProposeResult::Accepted { .. } => {}
            other => panic!("leader rejected put {i}: {other:?} (seed={seed})"),
        }
    }
    sim.run_for(Duration::from_secs(2)); // replicate + apply + compact on {l, third}
    sim.run_for(Duration::from_secs(1)); // settle fully idle before the restart

    // Restart the lagging follower. Its log is far behind the leader's
    // compacted base, so the leader's next heartbeat to it raises
    // `take_snapshot_needed` with no commit advance anywhere in that step.
    sim.restart(nid(lagging as u64));

    // Bounded convergence: a handful of safety-poll intervals plus heartbeats
    // and chunk transfer, comfortably less than "never" but well over one
    // interval — proving the safety poll alone (not a signal) drives this.
    sim.run_for(Duration::from_secs(3));

    for i in [0u64, 1, 64, 100, N - 1] {
        let key = format!("k{i:03}").into_bytes();
        assert_eq!(
            block_on(nodes[lagging].local_get(&key)),
            Some(format!("v{i}").into_bytes()),
            "follower {lagging} missing k{i:03} after a signal-less \
             snapshot-image build (seed={seed})"
        );
    }
}
