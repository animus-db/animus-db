//! ADR 0018 §2/PR2b: MVCC snapshot reads at an HLC timestamp (`read_at`/
//! `scan_at`) — the linearizable-anchored building block cross-tablet
//! transactions will read through in a later PR, not a transaction's read
//! itself. Mirrors `read_index.rs`'s harness shape (spawned-task + `run_for`,
//! since these are async barrier-gated reads) and its "deposed leader never
//! serves stale" regression.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_cp_data::hlc::{self, HlcTimestamp};
use animus_env::{EnvExt, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{MemoryEngine, StorageEngine};
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

fn put(nodes: &[KvNode], live: &[usize], seed: u64, key: &[u8], value: &[u8]) {
    let l = leader(nodes, live, seed);
    assert!(
        matches!(
            nodes[l].put(key.to_vec(), value.to_vec()),
            ProposeResult::Accepted { .. }
        ),
        "leader {l} rejected a put (seed={seed})"
    );
}

/// The packed MVCC version `storage.get(key)` records for `key` on `node`
/// (every node in this file uses `StorageScope::whole()`, so the physical
/// key is the identity), unpacked back to the `HlcTimestamp` that committed
/// it.
fn ts_of(node: &KvNode, key: &[u8]) -> HlcTimestamp {
    let version = block_on(node.storage().get(key))
        .expect("engine read ok")
        .unwrap_or_else(|| panic!("key {key:?} missing"))
        .version;
    hlc::unpack(version)
}

/// Run a `linearizable_get` to completion (spawned, since it awaits a read
/// barrier + possible ceiling proposal) — used here purely to *drive the
/// committed read ceiling forward*, mirroring what a real caller would do
/// before relying on `read_at` at a given ts.
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

/// Run `node.read_at(key, ts)` to completion.
#[allow(clippy::type_complexity)]
fn read_at(
    sim: &mut Simulator,
    node: &KvNode,
    key: &[u8],
    ts: HlcTimestamp,
    budget: Duration,
) -> Option<Option<Vec<u8>>> {
    let slot: Arc<Mutex<Option<Option<Option<Vec<u8>>>>>> = Arc::new(Mutex::new(None));
    let n = node.clone();
    let s = Arc::clone(&slot);
    let k = key.to_vec();
    node.env().clone().spawn_task(async move {
        let v = n.read_at(&k, ts).await;
        *s.lock().unwrap() = Some(v);
    });
    sim.run_for(budget);
    slot.lock()
        .unwrap()
        .clone()
        .expect("read_at did not complete")
}

/// Run `node.scan_at(start, end, ts)` to completion.
#[allow(clippy::type_complexity)]
fn scan_at(
    sim: &mut Simulator,
    node: &KvNode,
    start: &[u8],
    end: &[u8],
    ts: HlcTimestamp,
    budget: Duration,
) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
    let slot: Arc<Mutex<Option<Option<Vec<(Vec<u8>, Vec<u8>)>>>>> = Arc::new(Mutex::new(None));
    let n = node.clone();
    let s = Arc::clone(&slot);
    let (lo, hi) = (start.to_vec(), end.to_vec());
    node.env().clone().spawn_task(async move {
        *s.lock().unwrap() = Some(n.scan_at(&lo, Some(&hi), ts).await);
    });
    sim.run_for(budget);
    slot.lock()
        .unwrap()
        .clone()
        .expect("scan_at did not complete")
}

#[test]
fn read_at_sees_exactly_the_version_at_or_before_ts() {
    let seed = 0xA5_1CE1;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2], seed);

    put(&nodes, &[0, 1, 2], seed, b"k", b"v1");
    sim.run_for(Duration::from_secs(1));
    let t1 = ts_of(&nodes[l], b"k");

    put(&nodes, &[0, 1, 2], seed, b"k", b"v2");
    sim.run_for(Duration::from_secs(1));
    let t2 = ts_of(&nodes[l], b"k");
    assert!(
        t2 > t1,
        "the second write's ts must strictly exceed the first (seed={seed})"
    );

    // Drive the ceiling forward past t2 first — read_at refuses a ts not
    // yet strictly below the committed ceiling (Deliverable 2's contract).
    lin_read(&mut sim, &nodes[l], b"k", Duration::from_secs(2));

    assert_eq!(
        read_at(&mut sim, &nodes[l], b"k", t1, Duration::from_secs(2)),
        Some(Some(b"v1".to_vec())),
        "read_at(t1) must see v1 (seed={seed})"
    );
    assert_eq!(
        read_at(&mut sim, &nodes[l], b"k", t2, Duration::from_secs(2)),
        Some(Some(b"v2".to_vec())),
        "read_at(t2) must see v2 (seed={seed})"
    );

    // A genuine gap exists between the two writes (driven by the 1s
    // `run_for` between them, so wall_ms differs by roughly that much) —
    // construct a ts strictly between and confirm it still reads v1 (the
    // version in effect at that point, before v2 landed).
    let mid_wall = t1.wall_ms + (t2.wall_ms - t1.wall_ms) / 2;
    let between = HlcTimestamp {
        wall_ms: mid_wall,
        logical: 0,
    };
    assert!(
        t1 < between && between < t2,
        "test fixture must genuinely land strictly between t1 and t2 \
         (t1={t1:?} between={between:?} t2={t2:?}, seed={seed})"
    );
    assert_eq!(
        read_at(&mut sim, &nodes[l], b"k", between, Duration::from_secs(2)),
        Some(Some(b"v1".to_vec())),
        "read_at at a ts strictly between the two writes must still see v1 (seed={seed})"
    );
}

#[test]
fn scan_at_sees_exactly_the_versions_at_or_before_ts_across_multiple_keys() {
    let seed = 0x5CA_2A7;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2], seed);

    put(&nodes, &[0, 1, 2], seed, b"k1", b"a1");
    put(&nodes, &[0, 1, 2], seed, b"k2", b"b1");
    sim.run_for(Duration::from_secs(1));
    let t1 = ts_of(&nodes[l], b"k2"); // the later of the two writes at this point

    put(&nodes, &[0, 1, 2], seed, b"k1", b"a2");
    put(&nodes, &[0, 1, 2], seed, b"k3", b"c1");
    sim.run_for(Duration::from_secs(1));
    let t2 = ts_of(&nodes[l], b"k3");

    lin_read(&mut sim, &nodes[l], b"k1", Duration::from_secs(2)); // drive the ceiling past t2

    // As of t1: k1=a1, k2=b1, k3 doesn't exist yet.
    assert_eq!(
        scan_at(
            &mut sim,
            &nodes[l],
            b"k0",
            b"k9",
            t1,
            Duration::from_secs(2)
        ),
        Some(vec![
            (b"k1".to_vec(), b"a1".to_vec()),
            (b"k2".to_vec(), b"b1".to_vec()),
        ]),
        "scan_at(t1) must reflect only what was committed by t1 (seed={seed})"
    );
    // As of t2: k1 moved to a2, k2 unchanged, k3 now present.
    assert_eq!(
        scan_at(
            &mut sim,
            &nodes[l],
            b"k0",
            b"k9",
            t2,
            Duration::from_secs(2)
        ),
        Some(vec![
            (b"k1".to_vec(), b"a2".to_vec()),
            (b"k2".to_vec(), b"b1".to_vec()),
            (b"k3".to_vec(), b"c1".to_vec()),
        ]),
        "scan_at(t2) must reflect every write committed by t2 (seed={seed})"
    );
}

#[test]
fn read_at_above_the_ceiling_is_refused_then_succeeds_once_the_ceiling_advances() {
    let seed = 0xCE_111;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2], seed);

    put(&nodes, &[0, 1, 2], seed, b"k", b"v");
    sim.run_for(Duration::from_secs(1));
    let t = ts_of(&nodes[l], b"k");

    // No read has ever been served on this fresh group, so the committed
    // ceiling is still zero: `t` (> zero) is not strictly below it, so
    // read_at must refuse — outer `None`, never a served value.
    assert_eq!(
        read_at(&mut sim, &nodes[l], b"k", t, Duration::from_secs(2)),
        None,
        "read_at above the (as-yet-unraised) ceiling must be refused (seed={seed})"
    );

    // Drive the ceiling forward: a linearizable_get proposes+commits+applies
    // a `ReadCeiling` covering its own (necessarily >= t) serve ts.
    lin_read(&mut sim, &nodes[l], b"k", Duration::from_secs(2));

    assert_eq!(
        read_at(&mut sim, &nodes[l], b"k", t, Duration::from_secs(2)),
        Some(Some(b"v".to_vec())),
        "read_at(t) must succeed once the ceiling has advanced past t (seed={seed})"
    );
}

#[test]
fn deposed_leader_read_at_never_returns_a_stale_value() {
    let seed = 0xDE_AD02;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));

    put(&nodes, &[0, 1, 2], seed, b"x", b"old");
    sim.run_for(Duration::from_secs(1));
    let old = leader(&nodes, &[0, 1, 2], seed);
    let t_old = ts_of(&nodes[old], b"x");
    // Drive the ceiling forward *before* the partition, so read_at(t_old)
    // would have succeeded had this leader survived.
    lin_read(&mut sim, &nodes[old], b"x", Duration::from_secs(2));

    let survivors: Vec<usize> = (0..3).filter(|&i| i != old).collect();
    for &s in &survivors {
        sim.partition_pair(nid(old as u64), nid(s as u64));
    }
    sim.run_for(Duration::from_secs(3));
    put(&nodes, &survivors, seed, b"x", b"new");
    sim.run_for(Duration::from_secs(2));

    // The isolated old leader still *believes* it leads its (stale) term,
    // but read_at's read barrier cannot collect a quorum ack — so it must
    // return "not served" (outer `None`), never the stale "old" value.
    let stale = read_at(&mut sim, &nodes[old], b"x", t_old, Duration::from_secs(7));
    assert_eq!(
        stale, None,
        "a deposed leader must not serve read_at, stale or otherwise (seed={seed})"
    );

    // The new leader serves the up-to-date value through the ordinary
    // linearizable path (read_at through it is exercised in the ceiling
    // test above; re-proving it here would be redundant).
    let new = leader(&nodes, &survivors, seed);
    assert_eq!(
        lin_read(&mut sim, &nodes[new], b"x", Duration::from_secs(1)),
        Some(b"new".to_vec()),
    );
}
