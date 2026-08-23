//! Regression for the ADR 0048/0055 claim that an eventual
//! (`ConsistentRead: false`) read never wakes a quiesced group —
//! `stale_read_ready`'s own doc comment states it outright ("Never wakes a
//! quiesced group ... an eventual read needs no Raft activity at all, and a
//! quiesced group is idle by construction"), but until this file the claim
//! was verified only **structurally** (grepping the stale-read path for a
//! `.wake()`/`WakeSignal` call), never by a test that actually quiesces a
//! group and drives a real read through it.
//!
//! One property, checked as strongly as `SimEnv` allows: bring a real 3-node
//! group to genuine quiescence (`quiescence.rs`'s own idiom), serve an
//! eventual **point read and scan** through it, and prove
//!
//! - the read returns the applied data (it would be a vacuous pass otherwise
//!   — a read that is refused or wrong would trivially "not wake" anything);
//! - **zero new `SimEnv` timeline events** occurred across the read — the
//!   strongest, unfakeable proof available, in the same spirit as
//!   `quiescence.rs`'s module doc on why `next_deadline() == None` beats a
//!   trace tally for *proving* timerlessness: here there is no timer/sleep
//!   race to worry about at all, since `stale_get_served`/`stale_scan` never
//!   `await` a sleep (`stale_read.rs`'s own doc), so driving them with
//!   `block_on` never touches the `Simulator`'s executor or timeline in the
//!   first place — a read that grew a network round trip or a spawned task
//!   would show up directly as a nonzero delta;
//! - `Metric::CpAppendEntriesSent`/`CpUnquiesces` stay flat (the same
//!   corroborating quantitative check `quiescence.rs` uses for its own
//!   "no wakeup cost" property);
//! - every replica's `is_quiesced()` still reads `true` afterward.
//!
//! **Negative control** (per `docs/engineering-lessons.md`'s Testing
//! section: a test must be shown capable of failing): a write on the same,
//! still-quiesced leader immediately un-quiesces it and generates real
//! timeline traffic. This repeats the minimal shape of `quiescence.rs`'s
//! `a_write_after_quiescence_un_quiesces_and_commits` on this file's own
//! group rather than duplicating that test's full coverage — its purpose
//! here is narrow: proving the detection technique above (timeline-event
//! delta + `is_quiesced()`) actually goes red on a genuine wake, not just
//! asserting one didn't happen.

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{Metric, MetricsHandle, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];
/// Short relative to this test's own settle window, long relative to one
/// heartbeat interval — mirrors `quiescence.rs`'s identical constant and
/// reasoning.
const QUIESCE_AFTER: Duration = Duration::from_millis(200);

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

/// A 3-node group, every replica opted into quiescence — `quiescence.rs`'s
/// own `group()` helper, duplicated rather than shared across test binaries
/// (the house convention: each `tests/*.rs` is its own crate).
fn group(seed: u64) -> (Simulator, Vec<KvNode>, Vec<MetricsHandle>) {
    let sim = Simulator::new(seed);
    let handles: Vec<MetricsHandle> = NODES.iter().map(|_| MetricsHandle::recording()).collect();
    let nodes: Vec<KvNode> = NODES
        .iter()
        .enumerate()
        .map(|(i, &id)| {
            let n = RaftKvNode::start_with_metrics(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
                handles[i].clone(),
            );
            n.enable_quiescence(QUIESCE_AFTER);
            n
        })
        .collect();
    (sim, nodes, handles)
}

fn leader_index(nodes: &[KvNode], seed: u64) -> usize {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(ls.len(), 1, "expected one leader, got {ls:?} (seed={seed})");
    ls[0]
}

fn put(nodes: &[KvNode], l: usize, key: &[u8], value: &[u8]) {
    assert!(matches!(
        nodes[l].put(key.to_vec(), value.to_vec()),
        ProposeResult::Accepted { .. }
    ));
}

fn append_entries_sent(handles: &[MetricsHandle]) -> u64 {
    handles
        .iter()
        .map(|h| h.get(Metric::CpAppendEntriesSent))
        .sum()
}

fn unquiesces(handles: &[MetricsHandle]) -> u64 {
    handles.iter().map(|h| h.get(Metric::CpUnquiesces)).sum()
}

#[test]
fn an_eventual_read_on_a_quiesced_group_never_wakes_it() {
    let seed = 0xEE7E_A0D1;
    let (mut sim, nodes, handles) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect + replicate the no-op
    let leader = leader_index(&nodes, seed);

    for (k, v) in [
        (b"k1".as_slice(), b"v1".as_slice()),
        (b"k2", b"v2"),
        (b"k3", b"v3"),
    ] {
        put(&nodes, leader, k, v);
    }
    sim.run_for(Duration::from_secs(2)); // replicate + apply
    sim.run_for(Duration::from_secs(2)); // idle well past QUIESCE_AFTER

    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.is_quiesced(),
            "node {i} must have quiesced before the eventual read is even \
             attempted (seed={seed})"
        );
    }

    let follower = (0..nodes.len())
        .find(|&i| i != leader)
        .expect("a follower exists");

    let trace_before = sim.trace().len();
    let ae_before = append_entries_sent(&handles);
    let unq_before = unquiesces(&handles);

    // The point read.
    assert_eq!(
        block_on(nodes[follower].stale_get_served(b"k2")),
        Some(Some(b"v2".to_vec())),
        "the eventual point read must return the applied data (seed={seed})"
    );
    // The scan form, not just the point read.
    assert_eq!(
        block_on(nodes[follower].stale_scan(b"k1", Some(b"k3"), None)),
        vec![
            (b"k1".to_vec(), b"v1".to_vec()),
            (b"k2".to_vec(), b"v2".to_vec()),
        ],
        "the eventual scan must return the applied range (seed={seed})"
    );

    // The strongest available proof of "no wake": zero new SimEnv timeline
    // events across both reads. Neither `stale_get_served` nor `stale_scan`
    // ever `await`s a sleep (see `stale_read.rs`'s own module doc), so
    // driving them with `block_on` never touches the Simulator's executor at
    // all — any Raft message, spawned task, or timer this read caused would
    // show up here directly, not merely via `is_quiesced()` staying true.
    assert_eq!(
        sim.trace().len(),
        trace_before,
        "an eventual read/scan must add zero SimEnv timeline events on a \
         quiesced group (seed={seed})"
    );
    assert_eq!(
        append_entries_sent(&handles),
        ae_before,
        "an eventual read must not generate AppendEntries traffic (seed={seed})"
    );
    assert_eq!(
        unquiesces(&handles),
        unq_before,
        "an eventual read must not un-quiesce any replica (seed={seed})"
    );
    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.is_quiesced(),
            "node {i} must still be quiesced after the eventual read (seed={seed})"
        );
    }

    // `block_on` never drives the Simulator's own executor (see above), so
    // the zero-timeline-events check only catches activity the read causes
    // *synchronously*. A bug that merely queues a wake (e.g. a stray
    // `RaftKvNode::wake()` call, which just notifies an `AtomicWaker` — see
    // its own doc) would stay invisible until something later advances
    // virtual time. Close that gap by advancing a window past `QUIESCE_AFTER`
    // with no further reads and re-checking the same, apply-task-noise-immune
    // signals `quiescence.rs`'s own property (ii) uses (`CpAppendEntriesSent`
    // stays flat regardless of the apply task's own harmless 250ms safety-poll
    // timer, unlike a raw trace-event tally would).
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        append_entries_sent(&handles),
        ae_before,
        "a queued-but-not-yet-fired wake from the eventual read must not \
         surface as AppendEntries traffic once virtual time advances \
         (seed={seed})"
    );
    assert_eq!(
        unquiesces(&handles),
        unq_before,
        "a queued-but-not-yet-fired wake from the eventual read must not \
         surface as an un-quiesce transition once virtual time advances \
         (seed={seed})"
    );
    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.is_quiesced(),
            "node {i} must still be quiesced after settling past the \
             eventual read (seed={seed})"
        );
    }

    // ---- Negative control: prove the technique above can actually fail ----
    // A write on the still-quiesced leader must un-quiesce it immediately and
    // generate real timeline traffic — mirrors `quiescence.rs`'s
    // `a_write_after_quiescence_un_quiesces_and_commits`; see this file's own
    // doc for why it is repeated here rather than merely cited.
    let trace_before_write = sim.trace().len();
    match nodes[leader].put(b"k4".to_vec(), b"v4".to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("leader rejected a put after quiescing: {other:?} (seed={seed})"),
    }
    assert!(
        !nodes[leader].is_quiesced(),
        "a local propose must un-quiesce the leader immediately, unlike the \
         eventual read above (seed={seed})"
    );
    sim.run_for(Duration::from_secs(2)); // replicate + apply
    assert!(
        sim.trace().len() > trace_before_write,
        "a write must add real timeline events, unlike the eventual read \
         above — proof this test's wake-detection technique is not vacuous \
         (seed={seed})"
    );
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(b"k4")),
            Some(b"v4".to_vec()),
            "node {i} missing the post-quiescence write (seed={seed})"
        );
    }
}
