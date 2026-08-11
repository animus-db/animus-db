//! Linearizable **compare-and-swap** (ADR 0017): `KvCommand::Cas` sets `key` to
//! `value` iff the key's current committed value equals `expected`. The decision
//! is made at *apply* time in commit order against the engine's committed state,
//! so every replica makes the identical accept/reject choice (deterministic, no
//! clock/RNG) and two CAS racing from the same `expected` resolve to exactly one
//! winner. The proposer learns the outcome by the entry's Raft log index
//! (`compare_and_swap` proposes + waits + returns the recorded outcome).
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`, never
//! `run()` (the driver has perpetual heartbeat/election timers).

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

/// Propose a put on whoever is leader, asserting it was accepted.
fn put(nodes: &[KvNode], live: &[usize], seed: u64, key: &[u8], value: &[u8]) {
    let l = leader(nodes, live, seed);
    match nodes[l].put(key.to_vec(), value.to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("leader rejected a put: {other:?} (seed={seed})"),
    }
}

/// Two CAS on the same key from the same `expected`, proposed back-to-back on the
/// leader before either commits, race: exactly one swaps, the other fails, and the
/// stored value (on every replica) is the winner's.
#[test]
fn concurrent_cas_has_exactly_one_winner() {
    let seed = 0xCA5;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect

    // Seed a baseline value so both CAS share the same `expected = "v0"`.
    put(&nodes, &[0, 1, 2], seed, b"k", b"v0");
    sim.run_for(Duration::from_secs(2));

    let l = leader(&nodes, &[0, 1, 2], seed);
    // Propose both CAS *before* either applies — they are concurrent: each is
    // ordered by Raft; the first to apply moves the committed value, so the
    // second's compare against the (now-changed) value fails.
    let a = match nodes[l].cas(b"k".to_vec(), Some(b"v0".to_vec()), b"A".to_vec()) {
        ProposeResult::Accepted { index } => index,
        other => panic!("CAS A rejected: {other:?} (seed={seed})"),
    };
    let b = match nodes[l].cas(b"k".to_vec(), Some(b"v0".to_vec()), b"B".to_vec()) {
        ProposeResult::Accepted { index } => index,
        other => panic!("CAS B rejected: {other:?} (seed={seed})"),
    };
    sim.run_for(Duration::from_secs(3)); // commit + apply both

    let oa = nodes[l].cas_result(a).expect("CAS A applied (seed)");
    let ob = nodes[l].cas_result(b).expect("CAS B applied (seed)");
    assert!(
        oa ^ ob,
        "exactly one CAS must win: A={oa}, B={ob} (seed={seed})"
    );

    // The winner is whichever Raft ordered first (lower index), since the first
    // swap changes the committed value out from under the second.
    let winner_value = if a < b { b"A".to_vec() } else { b"B".to_vec() };
    assert!(
        if a < b { oa } else { ob },
        "the earlier-indexed CAS wins (seed={seed})"
    );

    // Every replica's committed value is the winner's, and every replica recorded
    // the identical pair of outcomes (deterministic apply).
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(b"k")),
            Some(winner_value.clone()),
            "node {i} disagrees on the CAS winner (seed={seed})"
        );
        assert_eq!(
            n.cas_result(a),
            Some(oa),
            "node {i} disagrees on CAS A outcome (seed={seed})"
        );
        assert_eq!(
            n.cas_result(b),
            Some(ob),
            "node {i} disagrees on CAS B outcome (seed={seed})"
        );
    }
}

/// `compare_and_swap` (propose + wait for the committed outcome) returns `true`
/// for the winner and `false` for the loser of a same-`expected` race.
#[test]
fn compare_and_swap_reports_committed_outcome() {
    let seed = 0xCA5E;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    put(&nodes, &[0, 1, 2], seed, b"k", b"v0");
    sim.run_for(Duration::from_secs(2));

    let l = leader(&nodes, &[0, 1, 2], seed);
    // `compare_and_swap` proposes on first poll, then `env.sleep`s until its entry
    // applies — and virtual time only advances under `sim.run_for`, so we can't
    // `block_on` it (its sleep would never wake). Instead: poll once to issue both
    // proposals, drive the simulator to commit + apply them, then poll again — by
    // then each outcome is recorded, so the next poll returns `Ready`.
    let mut fa =
        Box::pin(nodes[l].compare_and_swap(b"k".to_vec(), Some(b"v0".to_vec()), b"A".to_vec()));
    let mut fb =
        Box::pin(nodes[l].compare_and_swap(b"k".to_vec(), Some(b"v0".to_vec()), b"B".to_vec()));
    poll_once(&mut fa);
    poll_once(&mut fb);
    sim.run_for(Duration::from_secs(3));
    let ra = poll_result(&mut fa)
        .expect("CAS A future ready")
        .expect("CAS A on the leader (Some)");
    let rb = poll_result(&mut fb)
        .expect("CAS B future ready")
        .expect("CAS B on the leader (Some)");
    assert!(
        ra ^ rb,
        "compare_and_swap: exactly one true, got A={ra}, B={rb} (seed={seed})"
    );
}

/// Poll a future once on a no-op waker, discarding the result; used to issue a
/// `compare_and_swap`'s proposal eagerly before driving virtual time.
fn poll_once<F: std::future::Future + Unpin>(f: &mut F) {
    use std::task::Context;
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    let _ = std::pin::Pin::new(f).poll(&mut cx);
}

/// Poll a future once and return its output if it is `Ready` (else `None`). After
/// the simulator has driven the proposed CAS to apply, the recorded outcome is
/// available, so the next poll returns `Ready`.
fn poll_result<F: std::future::Future + Unpin>(f: &mut F) -> Option<F::Output> {
    use std::task::{Context, Poll};
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    match std::pin::Pin::new(f).poll(&mut cx) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

/// CAS-if-absent (`expected: None`) succeeds on an empty key and fails once a
/// value exists.
#[test]
fn cas_if_absent() {
    let seed = 0xAB5E;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2], seed);

    // Empty key: CAS-if-absent succeeds.
    let first = match nodes[l].cas(b"k".to_vec(), None, b"v1".to_vec()) {
        ProposeResult::Accepted { index } => index,
        other => panic!("CAS-if-absent rejected: {other:?} (seed={seed})"),
    };
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        nodes[l].cas_result(first),
        Some(true),
        "CAS-if-absent must succeed on an empty key (seed={seed})"
    );
    assert_eq!(block_on(nodes[l].local_get(b"k")), Some(b"v1".to_vec()));

    // Now a value exists: a second CAS-if-absent fails (no overwrite).
    let second = match nodes[l].cas(b"k".to_vec(), None, b"v2".to_vec()) {
        ProposeResult::Accepted { index } => index,
        other => panic!("CAS-if-absent rejected: {other:?} (seed={seed})"),
    };
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        nodes[l].cas_result(second),
        Some(false),
        "CAS-if-absent must fail once the key has a value (seed={seed})"
    );
    assert_eq!(
        block_on(nodes[l].local_get(b"k")),
        Some(b"v1".to_vec()),
        "a failed CAS must not overwrite (seed={seed})"
    );
}

/// A successful CAS survives a `stop` + restart: the committed entry is durable in
/// the WAL, so a fresh node on the same id replays it and re-applies the swap.
#[test]
fn successful_cas_survives_restart() {
    let seed = 0x5A4E;
    let sim = Simulator::new(seed);
    // We restart node 0, so build the group keeping ids to rebuild it.
    let mut nodes: Vec<KvNode> = NODES
        .iter()
        .map(|&id| {
            RaftKvNode::start(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    let mut sim = sim;
    sim.run_for(Duration::from_secs(2));

    put(&nodes, &[0, 1, 2], seed, b"k", b"v0");
    sim.run_for(Duration::from_secs(2));

    let l = leader(&nodes, &[0, 1, 2], seed);
    let idx = match nodes[l].cas(b"k".to_vec(), Some(b"v0".to_vec()), b"swapped".to_vec()) {
        ProposeResult::Accepted { index } => index,
        other => panic!("CAS rejected: {other:?} (seed={seed})"),
    };
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        nodes[l].cas_result(idx),
        Some(true),
        "CAS must succeed before restart (seed={seed})"
    );

    // Stop node 0 (volatile state dies, synced WAL survives), then restart it with
    // a fresh engine: the driver replays the WAL and re-applies committed commands,
    // so the swapped value is recovered.
    sim.stop(nid(0));
    nodes[0] = RaftKvNode::start(
        sim.env(nid(0)),
        NODES.iter().copied().map(nid).collect(),
        MemoryEngine::new(),
    );
    sim.run_for(Duration::from_secs(4)); // recover + catch up

    assert_eq!(
        block_on(nodes[0].local_get(b"k")),
        Some(b"swapped".to_vec()),
        "node 0 lost the committed CAS across restart (seed={seed})"
    );
    // And the survivors still hold it too.
    for s in [1usize, 2] {
        assert_eq!(
            block_on(nodes[s].local_get(b"k")),
            Some(b"swapped".to_vec()),
            "survivor {s} lost the CAS (seed={seed})"
        );
    }
}

/// A seed sweep: across many seeds, a same-`expected` race always has exactly one
/// winner and all replicas agree on the stored value.
#[test]
fn cas_winner_consistent_across_seeds() {
    for seed in 0u64..24 {
        let (mut sim, nodes) = group(seed);
        sim.run_for(Duration::from_secs(2));
        put(&nodes, &[0, 1, 2], seed, b"k", b"v0");
        sim.run_for(Duration::from_secs(2));

        let l = leader(&nodes, &[0, 1, 2], seed);
        let a = match nodes[l].cas(b"k".to_vec(), Some(b"v0".to_vec()), b"A".to_vec()) {
            ProposeResult::Accepted { index } => index,
            other => panic!("CAS A rejected: {other:?} (seed={seed})"),
        };
        let b = match nodes[l].cas(b"k".to_vec(), Some(b"v0".to_vec()), b"B".to_vec()) {
            ProposeResult::Accepted { index } => index,
            other => panic!("CAS B rejected: {other:?} (seed={seed})"),
        };
        sim.run_for(Duration::from_secs(3));

        let oa = nodes[l].cas_result(a).expect("CAS A applied");
        let ob = nodes[l].cas_result(b).expect("CAS B applied");
        assert!(oa ^ ob, "exactly one winner (seed={seed})");
        let winner = if a < b { b"A".to_vec() } else { b"B".to_vec() };
        for (i, n) in nodes.iter().enumerate() {
            assert_eq!(
                block_on(n.local_get(b"k")),
                Some(winner.clone()),
                "node {i} disagrees on winner (seed={seed})"
            );
        }
    }
}

/// Same seed reproduces an identical trace (ADR 0003).
#[test]
fn run_is_deterministic_from_seed() {
    let observe = |seed: u64| {
        let (mut sim, nodes) = group(seed);
        sim.run_for(Duration::from_secs(2));
        put(&nodes, &[0, 1, 2], seed, b"k", b"v0");
        sim.run_for(Duration::from_secs(2));
        let l = leader(&nodes, &[0, 1, 2], seed);
        let _ = nodes[l].cas(b"k".to_vec(), Some(b"v0".to_vec()), b"A".to_vec());
        let _ = nodes[l].cas(b"k".to_vec(), Some(b"v0".to_vec()), b"B".to_vec());
        sim.run_for(Duration::from_secs(3));
        sim.trace_lines()
    };
    assert_eq!(
        observe(0x7),
        observe(0x7),
        "same seed must reproduce the trace"
    );
}
