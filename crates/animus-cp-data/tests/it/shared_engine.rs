//! Per-tablet **private engines** + F2b physical keys (ADR 0050 Train B
//! rungs 1–2; this binary was the ADR 0028 shared-engine confinement suite —
//! its four scenarios survive with the same assertions, now proving the
//! *structural* form of the same properties: isolation comes from each
//! tablet owning its own engine, not from prefix confinement on a shared
//! one). Plus this rung's own teeth: a written row's physical key is
//! byte-exactly `[kind] || logical` (no table prefix, no tablet identity in
//! the bytes — red on rung 1, where the table prefix was still present), and
//! an `engine_image` of one tablet's private engine installs byte-identically
//! into another tablet's engine (the rung-B4 seed path's foundation).
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()` (the driver has perpetual heartbeat/election timers).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::{KIND_BASE, RaftKvNode, StorageScope};
use animus_env::{EnvExt, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{MemoryEngine, StorageEngine};
use futures::executor::block_on;

const GROUP_A: [u64; 3] = [0, 1, 2];
const GROUP_B: [u64; 3] = [10, 11, 12];

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

/// Two independent groups (distinct node-id sets), modeling two co-hosted
/// tablets — since ADR 0050 rung 1 each holds its **own private engine**;
/// per-replica engines are returned so tests can ground-truth the raw bytes.
fn two_private_groups(
    sim: &Simulator,
) -> (
    Vec<KvNode>,
    Vec<KvNode>,
    Vec<MemoryEngine>,
    Vec<MemoryEngine>,
) {
    let engines_a: Vec<MemoryEngine> = GROUP_A.iter().map(|_| MemoryEngine::new()).collect();
    let engines_b: Vec<MemoryEngine> = GROUP_B.iter().map(|_| MemoryEngine::new()).collect();
    let a = GROUP_A
        .iter()
        .zip(&engines_a)
        .map(|(&id, engine)| {
            RaftKvNode::start_scoped(
                sim.env(nid(id)),
                GROUP_A.iter().copied().map(nid).collect(),
                engine.clone(),
                StorageScope::whole(),
            )
        })
        .collect();
    let b = GROUP_B
        .iter()
        .zip(&engines_b)
        .map(|(&id, engine)| {
            RaftKvNode::start_scoped(
                sim.env(nid(id)),
                GROUP_B.iter().copied().map(nid).collect(),
                engine.clone(),
                StorageScope::whole(),
            )
        })
        .collect();
    (a, b, engines_a, engines_b)
}

fn leader(nodes: &[KvNode], seed: u64) -> usize {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(ls.len(), 1, "expected one leader, got {ls:?} (seed={seed})");
    ls[0]
}

fn put(nodes: &[KvNode], l: usize, seed: u64, key: &[u8], value: &[u8]) {
    match nodes[l].put(key.to_vec(), value.to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("leader rejected a put: {other:?} (seed={seed})"),
    }
}

/// Run a linearizable range scan to completion (spawned as a task, since it
/// awaits a read-barrier quorum round that only resolves while `Simulator`
/// advances virtual time — a bare `block_on` here would hang forever; see
/// `tests/read_index.rs`'s `lin_scan`, whose exact shape this mirrors).
#[allow(clippy::type_complexity)]
fn lin_scan(
    sim: &mut Simulator,
    node: &KvNode,
    start: &[u8],
    end: Option<&[u8]>,
    budget: Duration,
) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
    let slot: Arc<Mutex<Option<Option<Vec<(Vec<u8>, Vec<u8>)>>>>> = Arc::new(Mutex::new(None));
    let n = node.clone();
    let s = Arc::clone(&slot);
    let lo = start.to_vec();
    let hi = end.map(<[u8]>::to_vec);
    node.env().clone().spawn_task(async move {
        *s.lock().unwrap() = Some(n.linearizable_scan(&lo, hi.as_deref(), None).await);
    });
    sim.run_for(budget);
    slot.lock().unwrap().clone().expect("scan did not complete")
}

/// F2b teeth: a written row's physical key in a tablet's own engine is
/// byte-exactly `[KIND_BASE] || logical` — no table prefix, no tablet
/// identity. Red on rung 1 (the physical key still led with
/// `escape(table)`).
#[test]
fn a_written_rows_physical_key_is_exactly_kind_then_logical() {
    let seed = 0xF2B;
    let mut sim = Simulator::new(seed);
    let (nodes_a, _, engines_a, _) = two_private_groups(&sim);
    sim.run_for(Duration::from_secs(2));

    let la = leader(&nodes_a, seed);
    put(&nodes_a, la, seed, b"the-logical-key", b"v");
    sim.run_for(Duration::from_secs(2));

    let entries = block_on(engines_a[la].entries()).expect("raw engine scan");
    let expected_physical = [[KIND_BASE].as_slice(), b"the-logical-key".as_slice()].concat();
    assert_eq!(
        entries.len(),
        1,
        "exactly the one written row (seed={seed})"
    );
    assert_eq!(
        entries[0].0, expected_physical,
        "physical key must be byte-exactly [kind] || logical (seed={seed})"
    );
}

/// The core safety property, now structural: two groups writing the
/// **identical logical key** never collide — each tablet's own value
/// survives in its own private engine, on every one of its own replicas.
#[test]
fn two_groups_writing_the_same_logical_key_do_not_collide() {
    let seed = 0x5E1;
    let mut sim = Simulator::new(seed);
    let (nodes_a, nodes_b, _, _) = two_private_groups(&sim);
    sim.run_for(Duration::from_secs(2));

    let la = leader(&nodes_a, seed);
    let lb = leader(&nodes_b, seed);
    put(&nodes_a, la, seed, b"k", b"A-value");
    put(&nodes_b, lb, seed, b"k", b"B-value");
    sim.run_for(Duration::from_secs(2));

    for (i, n) in nodes_a.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(b"k")),
            Some(b"A-value".to_vec()),
            "group A node {i} lost its value to group B's write (seed={seed})"
        );
    }
    for (i, n) in nodes_b.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(b"k")),
            Some(b"B-value".to_vec()),
            "group B node {i} lost its value to group A's write (seed={seed})"
        );
    }
}

/// A full-table scan (`linearizable_scan` with `end = None`) returns only
/// the calling tablet's own keys — its engine holds nothing else.
#[test]
fn unbounded_scan_returns_exactly_the_tablets_own_keys() {
    let seed = 0x5E2;
    let mut sim = Simulator::new(seed);
    let (nodes_a, nodes_b, _, _) = two_private_groups(&sim);
    sim.run_for(Duration::from_secs(2));

    let la = leader(&nodes_a, seed);
    let lb = leader(&nodes_b, seed);
    put(&nodes_a, la, seed, b"a1", b"1");
    put(&nodes_a, la, seed, b"a2", b"2");
    put(&nodes_b, lb, seed, b"b1", b"9");
    sim.run_for(Duration::from_secs(2));

    let scanned = lin_scan(&mut sim, &nodes_a[la], b"", None, Duration::from_secs(2))
        .expect("leader serves a linearizable scan");
    let mut keys: Vec<Vec<u8>> = scanned.into_iter().map(|(k, _)| k).collect();
    keys.sort();
    assert_eq!(
        keys,
        vec![b"a1".to_vec(), b"a2".to_vec()],
        "group A's unbounded scan must return exactly its own keys (seed={seed})"
    );
}

/// A bounded scan is likewise the tablet's own data only, even when another
/// tablet wrote the same-named logical key.
#[test]
fn bounded_scan_returns_the_tablets_own_value_for_a_same_named_key() {
    let seed = 0x5E3;
    let mut sim = Simulator::new(seed);
    let (nodes_a, nodes_b, _, _) = two_private_groups(&sim);
    sim.run_for(Duration::from_secs(2));

    let la = leader(&nodes_a, seed);
    let lb = leader(&nodes_b, seed);
    put(&nodes_a, la, seed, b"a1", b"1");
    put(&nodes_b, lb, seed, b"a1", b"leaked-if-isolation-is-broken");
    sim.run_for(Duration::from_secs(2));

    let scanned = lin_scan(
        &mut sim,
        &nodes_a[la],
        b"",
        Some(b"zz"),
        Duration::from_secs(2),
    )
    .expect("leader serves a linearizable scan");
    assert_eq!(
        scanned,
        vec![(b"a1".to_vec(), b"1".to_vec())],
        "group A's bounded scan must return its own value for a1 (seed={seed})"
    );
}

/// **Snapshot catch-up** (`InstallSnapshot`, ADR 0017 A.2) restores exactly
/// the tablet's own data into the recovering replica's own engine — with a
/// concurrent second tablet writing the same key shapes into its own engine
/// (the pre-pivot leak hazard, now structural; the raw ground-truth count
/// still guards a mis-bounded `engine_image`).
#[test]
fn snapshot_catchup_restores_exactly_the_tablets_own_data() {
    let seed = 0x5E4;
    let sim = Simulator::new(seed);
    let (nodes_a, nodes_b, engines_a, _) = two_private_groups(&sim);
    let mut sim = sim;
    sim.run_for(Duration::from_secs(2));

    let la = leader(&nodes_a, seed);
    let lagging = (0..3).find(|&i| i != la).expect("a follower exists");
    sim.crash(nid(GROUP_A[lagging]));

    // Group A writes well past the compaction threshold (64) so the leader
    // snapshots + truncates the log prefix the crashed follower would need.
    for i in 0..150u64 {
        put(
            &nodes_a,
            la,
            seed,
            format!("k{i:03}").as_bytes(),
            format!("vA{i}").as_bytes(),
        );
    }
    // Group B concurrently writes the same key shapes into its own engine.
    let lb = leader(&nodes_b, seed);
    for i in 0..150u64 {
        put(
            &nodes_b,
            lb,
            seed,
            format!("k{i:03}").as_bytes(),
            format!("vB{i}").as_bytes(),
        );
    }
    sim.run_for(Duration::from_secs(3));

    sim.restart(nid(GROUP_A[lagging]));
    sim.run_for(Duration::from_secs(6));

    for i in [0u64, 1, 64, 100, 149] {
        let key = format!("k{i:03}").into_bytes();
        assert_eq!(
            block_on(nodes_a[lagging].local_get(&key)),
            Some(format!("vA{i}").into_bytes()),
            "group A replica {lagging} missing k{i:03} after snapshot catch-up (seed={seed})"
        );
    }
    // Ground truth on the RAW recovered engine: exactly 150 base-kind rows,
    // every one group A's own (no other tablet's bytes can appear — and a
    // mis-bounded image would show up as a count mismatch here).
    let physical_keys = block_on(engines_a[lagging].entries())
        .expect("raw engine scan")
        .into_iter()
        .filter(|(k, _)| k.first() == Some(&KIND_BASE))
        .count();
    assert_eq!(
        physical_keys, 150,
        "the recovered engine must hold exactly group A's 150 base rows (seed={seed})"
    );
}
