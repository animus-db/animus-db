//! Issue #1180 / ADR 0048: a quiesced CP-data group's apply task must not
//! wake on `APPLY_SAFETY_POLL` (250ms) -- it parks on `ApplySignal` alone, so a
//! fully quiesced group posts **zero** `SimEnv` timeline events. Before the
//! fix every hosted replica, quiesced or not, kept one 250ms timer alive
//! forever (measured: ~20 ms/s of CPU at 1,000 quiesced RF1 groups).
//!
//! Two properties, deterministically (ADR 0003):
//!
//! - **Zero wakeups**: after the group quiesces, `Simulator::run_until_quiescent`
//!   returns `true` (an empty timeline) and a long idle window advances no
//!   timer at all -- the previous behavior could never satisfy this.
//! - **Still converges once woken**: a write after a long quiescence
//!   un-quiesces the group and is applied and readable on every replica
//!   (the consensus loop raises `ApplySignal` on the un-quiesce), and the group
//!   re-quiesces afterward.

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];
const QUIESCE_AFTER: Duration = Duration::from_millis(200);

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn group(seed: u64) -> (Simulator, Vec<KvNode>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| {
            let n = RaftKvNode::start(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            );
            n.enable_quiescence(QUIESCE_AFTER);
            n
        })
        .collect();
    (sim, nodes)
}

fn leader(nodes: &[KvNode], seed: u64) -> usize {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(ls.len(), 1, "expected one leader, got {ls:?} (seed={seed})");
    ls[0]
}

fn timer_count(sim: &Simulator) -> usize {
    sim.trace_lines()
        .iter()
        .filter(|l| l.contains("Timer") || l.to_lowercase().contains("timer"))
        .count()
}

#[test]
fn quiesced_group_schedules_no_apply_poll_and_applies_a_late_write() {
    let seed = 0x1180_0001;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect
    let l = leader(&nodes, seed);
    match nodes[l].put(b"early".to_vec(), b"1".to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("leader rejected put: {other:?} (seed={seed})"),
    }
    // Idle well past QUIESCE_AFTER and several APPLY_SAFETY_POLL intervals so
    // every replica quiesces and any already-armed poll has expired.
    sim.run_for(Duration::from_secs(3));
    for (i, n) in nodes.iter().enumerate() {
        assert!(n.is_quiesced(), "node {i} must be quiesced (seed={seed})");
    }

    // Zero wakeups: the timeline is empty -- no consensus timer and, since
    // #1180, no apply safety poll either.
    assert!(
        sim.run_until_quiescent(10_000),
        "a quiesced group must leave an empty SimEnv timeline: the apply task \
         must not be on a safety-poll timer (seed={seed})"
    );
    let timers_before = timer_count(&sim);
    sim.run_for(Duration::from_secs(60));
    assert_eq!(
        timer_count(&sim),
        timers_before,
        "no timer may fire over a long idle window on a quiesced group (seed={seed})"
    );
    for n in &nodes {
        assert!(n.is_quiesced(), "must stay quiesced (seed={seed})");
    }

    // A write after the long quiescence wakes the group and is applied
    // everywhere.
    match nodes[l].put(b"late".to_vec(), b"2".to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("leader rejected late put: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(1));
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(b"late")),
            Some(b"2".to_vec()),
            "node {i} must apply a write issued after long quiescence (seed={seed})"
        );
        assert_eq!(block_on(n.local_get(b"early")), Some(b"1".to_vec()));
    }

    // And it settles back to a timer-free park.
    sim.run_for(Duration::from_secs(3));
    assert!(
        sim.run_until_quiescent(10_000),
        "the group must re-quiesce with an empty timeline after the write (seed={seed})"
    );
}
