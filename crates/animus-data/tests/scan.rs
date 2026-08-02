//! Native quorum range scan: [`DataClient::scan`] broadcasts a `ScanRange` to a
//! tablet's replicas and merges their per-replica results by per-key newest MVCC
//! version (LWW) across an R-quorum, returning the merged, sorted `(key, value)`
//! set. Divergent replicas (one missed a write, one holds a stale value behind a
//! peer's newer tombstone) must not corrupt the merge; tombstoned keys are
//! excluded. This is the primitive the wire adapters use instead of tracking
//! keys in-memory.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_data::{DataClient, DataMsg, TabletView, serve_replica};
use animus_env::{EnvExt, NodeId};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{MemoryEngine, StorageEngine};
use animus_tablet::{Epoch, TabletId};

const REPLICAS: [u64; 3] = [0, 1, 2];
const CLIENT: u64 = 10;
const TABLET: TabletId = TabletId(1);
const TIMEOUT: Duration = Duration::from_secs(2);

fn view(epoch: Epoch) -> TabletView {
    // R + W = 4 > N = 3.
    TabletView {
        tablet: TABLET,
        replicas: REPLICAS.to_vec(),
        epoch,
        r: 2,
        w: 2,
    }
}

fn cluster(seed: u64) -> (Simulator, Vec<animus_data::ReplicaHandle<MemoryEngine>>) {
    let sim = Simulator::new(seed);
    let handles = REPLICAS
        .iter()
        .map(|&id| serve_replica(sim.env(id), MemoryEngine::new(), Epoch::INITIAL))
        .collect();
    (sim, handles)
}

/// Drive one coordinator op to completion on the `CLIENT` node.
fn run_op<T: Clone + Send + 'static>(
    sim: &mut Simulator,
    op: impl FnOnce(DataClient<SimEnv>) -> futures::future::BoxFuture<'static, T> + Send + 'static,
) -> T {
    let result: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
    let client_env = sim.env(CLIENT);
    let out = Arc::clone(&result);
    client_env.clone().spawn_task(async move {
        let client = DataClient::new(client_env);
        *out.lock().unwrap() = Some(op(client).await);
    });
    sim.run();
    result.lock().unwrap().clone().expect("op did not complete")
}

/// Seed a replica's storage directly via per-key LWW merge (bypassing quorum), so
/// we can construct a deliberately divergent replica set.
fn seed(handle: &animus_data::ReplicaHandle<MemoryEngine>, key: &[u8], value: &[u8], version: u64) {
    futures::executor::block_on(handle.storage().merge(key, value, version)).unwrap();
}

fn seed_tombstone(handle: &animus_data::ReplicaHandle<MemoryEngine>, key: &[u8], version: u64) {
    futures::executor::block_on(handle.storage().merge_tombstone(key, version)).unwrap();
}

#[test]
fn scan_merges_divergent_replicas_newest_per_key_sorted() {
    let seed_v = 0x5CA4_0000u64;
    let (mut sim, handles) = cluster(seed_v);

    // Build a divergent range across the three replicas:
    //  - "a": replica 0 missed it; replicas 1,2 have v1.
    //  - "b": replica 0 has stale v1, replica 1 has newest v3, replica 2 has v2.
    //  - "c": present on all at v1.
    //  - "d": OUTSIDE the scan range [a, d) — must not appear.
    seed(&handles[1], b"a", b"a-v1", 1);
    seed(&handles[2], b"a", b"a-v1", 1);

    seed(&handles[0], b"b", b"b-v1", 1);
    seed(&handles[2], b"b", b"b-v2", 2);
    seed(&handles[1], b"b", b"b-v3", 3);

    seed(&handles[0], b"c", b"c-v1", 1);
    seed(&handles[1], b"c", b"c-v1", 1);
    seed(&handles[2], b"c", b"c-v1", 1);

    // "d" is outside [a, d) — present everywhere but must be excluded by range.
    seed(&handles[0], b"d", b"d-v1", 1);
    seed(&handles[1], b"d", b"d-v1", 1);
    seed(&handles[2], b"d", b"d-v1", 1);

    let v = view(Epoch::INITIAL);
    let got = run_op(&mut sim, move |c| {
        Box::pin(async move { c.scan(&v, b"a", b"d", None, TIMEOUT).await })
    });

    assert_eq!(
        got,
        Some(vec![
            (b"a".to_vec(), b"a-v1".to_vec()),
            (b"b".to_vec(), b"b-v3".to_vec()), // newest version wins across replicas
            (b"c".to_vec(), b"c-v1".to_vec()),
        ]),
        "scan must merge newest-per-key, sorted, within [a,d) (seed={seed_v:#x})"
    );
}

#[test]
fn scan_excludes_tombstoned_keys_even_when_a_replica_holds_a_stale_value() {
    let seed_v = 0x5CA4_0001u64;
    let (mut sim, handles) = cluster(seed_v);

    // "a": live everywhere at v1.
    seed(&handles[0], b"a", b"a-v1", 1);
    seed(&handles[1], b"a", b"a-v1", 1);
    seed(&handles[2], b"a", b"a-v1", 1);

    // "b": replicas 0,1 deleted it at v2 (newer), but replica 2 still holds the
    // stale value at v1. The merge must let the newer tombstone win, so "b" is
    // excluded from the result — a stale value must not mask a peer's delete.
    seed(&handles[2], b"b", b"b-v1", 1);
    seed_tombstone(&handles[0], b"b", 2);
    seed_tombstone(&handles[1], b"b", 2);

    let v = view(Epoch::INITIAL);
    let got = run_op(&mut sim, move |c| {
        Box::pin(async move { c.scan(&v, b"a", b"z", None, TIMEOUT).await })
    });

    assert_eq!(
        got,
        Some(vec![(b"a".to_vec(), b"a-v1".to_vec())]),
        "a newer tombstone must shadow a stale value in the merge (seed={seed_v:#x})"
    );
}

#[test]
fn scan_honors_limit_in_key_order() {
    let seed_v = 0x5CA4_0002u64;
    let (mut sim, handles) = cluster(seed_v);
    for (i, k) in [b"a", b"b", b"c", b"d", b"e"].iter().enumerate() {
        let ver = (i as u64) + 1;
        seed(&handles[0], *k, b"v", ver);
        seed(&handles[1], *k, b"v", ver);
    }

    let v = view(Epoch::INITIAL);
    let got = run_op(&mut sim, move |c| {
        Box::pin(async move { c.scan(&v, b"a", b"z", Some(2), TIMEOUT).await })
    });
    assert_eq!(
        got,
        Some(vec![
            (b"a".to_vec(), b"v".to_vec()),
            (b"b".to_vec(), b"v".to_vec()),
        ]),
        "limit caps the first N keys in key order (seed={seed_v:#x})"
    );
}

#[test]
fn scan_fails_when_a_read_quorum_is_unreachable() {
    let seed_v = 0x5CA4_0003u64;
    let (mut sim, handles) = cluster(seed_v);
    seed(&handles[0], b"a", b"a-v1", 1);
    seed(&handles[1], b"a", b"a-v1", 1);
    seed(&handles[2], b"a", b"a-v1", 1);

    // Crash two of three replicas: only one can respond, below R=2.
    sim.crash(1);
    sim.crash(2);

    let v = view(Epoch::INITIAL);
    let got = run_op(&mut sim, move |c| {
        Box::pin(async move { c.scan(&v, b"a", b"z", None, TIMEOUT).await })
    });
    assert_eq!(
        got, None,
        "scan must fail (None) when fewer than R replicas respond (seed={seed_v:#x})"
    );
}

#[test]
fn scan_is_fenced_on_a_stale_epoch() {
    let seed_v = 0x5CA4_0004u64;
    let sim = Simulator::new(seed_v);
    // Replicas know epoch 2.
    let handles: Vec<animus_data::ReplicaHandle<MemoryEngine>> = REPLICAS
        .iter()
        .map(|&id| serve_replica(sim.env(id), MemoryEngine::new(), Epoch(2)))
        .collect();
    let mut sim = sim;
    seed(&handles[0], b"a", b"a-v1", 1);
    seed(&handles[1], b"a", b"a-v1", 1);

    // A coordinator on the old epoch 1 is fenced ⇒ no quorum ⇒ None.
    let stale = view(Epoch(1));
    let got = run_op(&mut sim, move |c| {
        Box::pin(async move { c.scan(&stale, b"a", b"z", None, TIMEOUT).await })
    });
    assert_eq!(
        got, None,
        "a stale-epoch scan must be fenced (seed={seed_v:#x})"
    );
}

#[test]
fn scan_run_is_byte_reproducible_from_seed() {
    type ScanRows = Vec<(Vec<u8>, Vec<u8>)>;
    fn scenario(seed_v: u64) -> (Vec<String>, Option<ScanRows>) {
        let (mut sim, handles) = cluster(seed_v);
        seed(&handles[0], b"a", b"a-v1", 1);
        seed(&handles[1], b"a", b"a-v1", 1);
        seed(&handles[2], b"b", b"b-v1", 1);
        seed(&handles[1], b"b", b"b-v1", 1);
        let v = view(Epoch::INITIAL);
        let got = run_op(&mut sim, move |c| {
            Box::pin(async move { c.scan(&v, b"a", b"z", None, TIMEOUT).await })
        });
        (sim.trace_lines(), got)
    }
    let a = scenario(0x1357_2468);
    let b = scenario(0x1357_2468);
    assert_eq!(a.0, b.0, "scan run was not byte-reproducible");
    assert_eq!(a.1, b.1);
    assert!(!a.0.is_empty());
}

/// A residency-irrelevant sanity check that the wire enum round-trips the new
/// variants (guards against an accidental serde break for the adapters).
#[test]
fn scan_wire_variants_round_trip() {
    let msg = DataMsg::ScanRange {
        req: 7,
        tablet: TABLET,
        epoch: Epoch::INITIAL,
        start: b"a".to_vec(),
        end: b"z".to_vec(),
    };
    let bytes = serde_json::to_vec(&msg).unwrap();
    let back: DataMsg = serde_json::from_slice(&bytes).unwrap();
    assert!(matches!(back, DataMsg::ScanRange { req: 7, .. }));

    let resp = DataMsg::ScanResp {
        req: 7,
        ok: true,
        entries: vec![(b"a".to_vec(), Some(b"v".to_vec()), 1)],
    };
    let bytes = serde_json::to_vec(&resp).unwrap();
    let back: DataMsg = serde_json::from_slice(&bytes).unwrap();
    assert!(matches!(
        back,
        DataMsg::ScanResp {
            req: 7,
            ok: true,
            ..
        }
    ));

    // Touch NodeId/BTreeSet imports so the test file stays warning-clean if the
    // residency helpers are later folded in.
    let _used: BTreeSet<NodeId> = BTreeSet::new();
}
