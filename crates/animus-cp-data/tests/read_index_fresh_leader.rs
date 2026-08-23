//! ReadIndex linearizability at a **leader change** (Raft §6.4's second, mandatory
//! requirement): a freshly elected leader must not serve a read until it has
//! committed an entry of its **own term** (its election no-op).
//!
//! The hazard this drives into: the old leader commits + acks a write that
//! reached a majority's *logs*, but the `leader_commit` follow-up only reached
//! the old leader itself. The follower holding the entry then wins the election
//! — leader completeness guarantees its log *contains* the acked write, but its
//! `commit_index` does not yet cover it (the commit rule refuses to count
//! old-term entries toward a majority). In that window the term-only `ReadProbe`
//! quorum still passes (it involves no log state), so a barrier that captures
//! `read_index = commit_index` without the current-term-commit gate serves the
//! **stale** pre-write value for a key whose newer value was already acked.
//!
//! The existing corpus polls at coarse (100ms) granularity and structurally
//! cannot land inside this window, so this test drives the sim in 1ms steps to
//! stop exactly at commit / election edges, and keeps a third replica far behind
//! (~40 missing entries) so the new leader's no-op commit needs ~40 next_index
//! backtrack round-trips while a read probe needs one — holding the window open
//! far longer than the read's poll interval. The linearizable read issued inside
//! the window must wait for the no-op to commit and return the acked value,
//! never the stale one.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{EnvExt, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

const NODES: [u64; 3] = [0, 1, 2];

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

/// Step the sim in `step`-sized slices until `cond` holds (checked between
/// slices), up to `budget`. Returns whether the condition held — the caller
/// asserts, with its seed, so a drift is loud. Pure virtual time.
fn run_until_cond(
    sim: &mut Simulator,
    budget: Duration,
    step: Duration,
    cond: impl Fn() -> bool,
) -> bool {
    let deadline = sim.now().0 + budget.as_nanos() as u64;
    while sim.now().0 < deadline {
        if cond() {
            return true;
        }
        sim.run_for(step);
    }
    cond()
}

/// Run a linearizable read on `node` to completion (spawned as a task, since it
/// awaits the read barrier), driving the sim up to `budget`.
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

#[test]
fn fresh_leader_waits_for_its_no_op_before_serving_a_read() {
    // A few seeds so the window isn't an artifact of one jitter draw. Each run is
    // a pure function of its seed; the in-window preconditions are asserted, so a
    // seed that stops driving the window fails loudly rather than passing vacuously.
    for seed in [0xF5E5, 0xBEEF_0001, 0xA11CE] {
        fresh_leader_case(seed);
    }
}

fn fresh_leader_case(seed: u64) {
    let mut sim = Simulator::new(seed);
    let nodes: Vec<KvNode> = NODES
        .iter()
        .map(|&id| {
            RaftKvNode::start(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    sim.run_for(Duration::from_secs(2));

    let old: usize = {
        let ls: Vec<usize> = (0..3).filter(|&i| nodes[i].is_leader()).collect();
        assert_eq!(ls.len(), 1, "expected one leader, got {ls:?} (seed={seed})");
        ls[0]
    };
    let followers: Vec<usize> = (0..3).filter(|&i| i != old).collect();
    let (heir, laggard) = (followers[0], followers[1]);

    // A committed baseline value on every replica.
    assert!(matches!(
        nodes[old].put(b"x".to_vec(), b"old".to_vec()),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(1));

    // Cut the laggard off; everything from here on lands only on {old, heir}, so
    // the laggard falls ~40 entries behind — the new leader's no-op commit will
    // need that many next_index backtrack round-trips against it.
    sim.partition_pair(nid(old as u64), nid(laggard as u64));
    sim.partition_pair(nid(heir as u64), nid(laggard as u64));
    for i in 0..40u32 {
        assert!(matches!(
            nodes[old].put(format!("filler-{i:02}").into_bytes(), b"f".to_vec()),
            ProposeResult::Accepted { .. }
        ));
    }
    sim.run_for(Duration::from_secs(1));

    // The write under test: acked by the old leader once the heir's ack lands.
    let v2_index = match nodes[old].put(b"x".to_vec(), b"new".to_vec()) {
        ProposeResult::Accepted { index, .. } => index,
        other => panic!("put not accepted: {other:?} (seed={seed})"),
    };
    // Step at 1ms granularity to the exact commit edge: the old leader has
    // counted the heir's ack (the write is now committed + ack-able)...
    assert!(
        run_until_cond(
            &mut sim,
            Duration::from_secs(1),
            Duration::from_millis(1),
            || nodes[old].commit_index() >= v2_index
        ),
        "old leader never committed the write (seed={seed})"
    );
    // ...but the leader_commit follow-up (next heartbeat, ~50ms away) has NOT
    // reached the heir: it holds the entry in its log, uncommitted. This is the
    // §6.4 window. Both asserted so the scenario can't silently degrade.
    assert!(
        nodes[heir].snapshot_index() + nodes[heir].log_len() as u64 >= v2_index,
        "the heir must hold the acked entry in its log (seed={seed})"
    );
    assert!(
        nodes[heir].commit_index() < v2_index,
        "the heir's commit_index must not cover the acked write yet (seed={seed})"
    );

    // Kill the old leader inside the window; heal the laggard so an election can
    // reach a majority. The heir's log is longest, so only it can win.
    sim.crash(nid(old as u64));
    for &(a, b) in &[(heir, laggard), (old, laggard)] {
        sim.heal(nid(a as u64), nid(b as u64));
        sim.heal(nid(b as u64), nid(a as u64));
    }
    assert!(
        run_until_cond(
            &mut sim,
            Duration::from_secs(5),
            Duration::from_millis(1),
            || nodes[heir].is_leader()
        ),
        "the heir never won the election (seed={seed})"
    );
    // Still inside the window: freshly leader, its no-op (and thus the acked
    // write beneath it) uncommitted — committing needs ~40 backtrack round-trips
    // against the laggard, while a read probe needs one.
    assert!(
        nodes[heir].commit_index() < v2_index,
        "window closed before the read was issued (seed={seed})"
    );

    // A linearizable read issued INSIDE the window. Without the current-term-
    // commit gate the barrier captures read_index = commit_index (below the acked
    // write), passes the term-only probe quorum, and serves the stale "old". With
    // the gate it waits for the no-op to commit — which commits the acked write —
    // and serves "new".
    let read = lin_read(&mut sim, &nodes[heir], b"x", Duration::from_secs(3));
    assert_eq!(
        read,
        Some(b"new".to_vec()),
        "a fresh leader served a read below an already-acked write (seed={seed})"
    );
}
