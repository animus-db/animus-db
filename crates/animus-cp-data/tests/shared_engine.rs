//! `StorageScope` (PR3 of the single-command-split redesign, ADR 0028): two
//! independent tablet groups, each confined to its own scope, can safely
//! share **one physical `StorageEngine` instance** — writes, reads, scans,
//! and snapshot catch-up never leak or collide across scopes. Not yet wired
//! into any real caller (`animusd` still opens one dedicated engine per
//! tablet); this proves the mechanism `RaftKvNode` itself needs before that
//! wiring can happen safely.
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()` (the driver has perpetual heartbeat/election timers).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::{RaftKvNode, StorageScope};
use animus_env::{EnvExt, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{MemoryEngine, StorageEngine};
use animus_tablet::KeyRange;
use futures::executor::block_on;

const GROUP_A: [u64; 3] = [0, 1, 2];
const GROUP_B: [u64; 3] = [10, 11, 12];

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn scope(prefix: &[u8]) -> StorageScope {
    StorageScope::new(prefix.to_vec(), KeyRange::whole())
}

/// Two independent groups (distinct node-id sets — a shared physical *engine*
/// does not imply a shared physical *network inbox*, that's ADR 0026 Stage B,
/// a later PR), each confined to its own scope, but both handed **the same**
/// cloned `MemoryEngine` handle — modeling two tablets co-resident on one node
/// sharing that node's single engine.
fn two_scoped_groups(sim: &Simulator, engine: MemoryEngine) -> (Vec<KvNode>, Vec<KvNode>) {
    let a = GROUP_A
        .iter()
        .map(|&id| {
            RaftKvNode::start_scoped(
                sim.env(nid(id)),
                GROUP_A.iter().copied().map(nid).collect(),
                engine.clone(),
                scope(b"A:"),
            )
        })
        .collect();
    let b = GROUP_B
        .iter()
        .map(|&id| {
            RaftKvNode::start_scoped(
                sim.env(nid(id)),
                GROUP_B.iter().copied().map(nid).collect(),
                engine.clone(),
                scope(b"B:"),
            )
        })
        .collect();
    (a, b)
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

/// The core safety property: two groups writing the **identical logical key**
/// under different scopes must never collide physically — each group's own
/// value survives, on every one of its own replicas.
#[test]
fn two_scoped_groups_writing_the_same_logical_key_do_not_collide() {
    let seed = 0x5E1;
    let mut sim = Simulator::new(seed);
    let (nodes_a, nodes_b) = two_scoped_groups(&sim, MemoryEngine::new());
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
            "group A node {i} lost its value to group B's write on the shared engine (seed={seed})"
        );
    }
    for (i, n) in nodes_b.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(b"k")),
            Some(b"B-value".to_vec()),
            "group B node {i} lost its value to group A's write on the shared engine (seed={seed})"
        );
    }
}

/// A full-table scan (`linearizable_scan` with `end = None`, the `entries()`
/// unbounded branch — the one that genuinely reads the *whole* shared engine)
/// must return **only** the calling group's own keys, never the sibling
/// scope's, even though both are physically present in the one engine.
#[test]
fn unbounded_scan_never_returns_a_sibling_scopes_keys() {
    let seed = 0x5E2;
    let mut sim = Simulator::new(seed);
    let (nodes_a, nodes_b) = two_scoped_groups(&sim, MemoryEngine::new());
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
        "group A's unbounded scan must return exactly its own keys, \
         never group B's (seed={seed})"
    );
}

/// A bounded scan (`linearizable_scan` with `Some(end)`) is likewise confined
/// to the calling group's scope.
#[test]
fn bounded_scan_never_returns_a_sibling_scopes_keys() {
    let seed = 0x5E3;
    let mut sim = Simulator::new(seed);
    let (nodes_a, nodes_b) = two_scoped_groups(&sim, MemoryEngine::new());
    sim.run_for(Duration::from_secs(2));

    let la = leader(&nodes_a, seed);
    let lb = leader(&nodes_b, seed);
    put(&nodes_a, la, seed, b"a1", b"1");
    put(&nodes_b, lb, seed, b"a1", b"leaked-if-scoping-is-broken");
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
        "group A's bounded scan must return its own value for a1, \
         never group B's same-named key (seed={seed})"
    );
}

/// **Snapshot catch-up** (`InstallSnapshot`, ADR 0017 A.2) must not leak a
/// sibling scope's data into the receiving replica's engine, and must
/// correctly restore only the calling group's own data. This is the sharpest
/// edge in `StorageScope`'s design: `engine_image`/`install_engine_image`
/// (unlike normal put/get) touch the *whole* underlying engine unless
/// properly bounded (see their doc comments).
#[test]
fn snapshot_catchup_does_not_leak_the_sibling_scopes_data() {
    let seed = 0x5E4;
    let engine = MemoryEngine::new();
    let sim = Simulator::new(seed);
    let (nodes_a, nodes_b) = two_scoped_groups(&sim, engine.clone());
    let mut sim = sim;
    sim.run_for(Duration::from_secs(2));

    let la = leader(&nodes_a, seed);
    let lagging = (0..3).find(|&i| i != la).expect("a follower exists");
    // Crash (mute) the lagging follower so it misses everything below.
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
    // Group B also writes into the SAME shared engine, under its own scope —
    // this is what an unbounded `engine_image` would incorrectly ship.
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

    // Bring the lagging replica back: its log is far behind the leader's
    // compacted base, so it must catch up via InstallSnapshot.
    sim.restart(nid(GROUP_A[lagging]));
    sim.run_for(Duration::from_secs(6));

    // The recovered replica has every one of group A's writes...
    for i in [0u64, 1, 64, 100, 149] {
        let key = format!("k{i:03}").into_bytes();
        assert_eq!(
            block_on(nodes_a[lagging].local_get(&key)),
            Some(format!("vA{i}").into_bytes()),
            "group A replica {lagging} missing k{i:03} after snapshot catch-up (seed={seed})"
        );
    }
    // Ground-truth check on the RAW shared engine (bypassing `RaftKvNode`'s own
    // scoping, which would trivially hide a leak): after catch-up, the physical
    // engine must hold exactly 150 keys under the `A:` prefix — not more (a
    // leaked/mis-prefixed group-B key), not fewer (a dropped write).
    let physical_a_keys = block_on(engine.entries())
        .expect("raw engine scan")
        .into_iter()
        .filter(|(k, _)| k.starts_with(b"A:"))
        .count();
    assert_eq!(
        physical_a_keys, 150,
        "the shared engine must hold exactly group A's 150 physical keys after \
         catch-up — a mismatch means group B's data leaked in (or out) during \
         InstallSnapshot (seed={seed})"
    );
}
