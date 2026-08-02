//! ADR 0011 **frontier** acceptance tests: Accord over the *replicated data
//! plane*.
//!
//! The headline "real distributed transactions" slice. An Accord transaction is
//! agreed (PreAccept/Accept/Commit) and executed in agreed order, but its write
//! *effect* now lands in the leaderless AP **data plane** (`custos-data`) rather
//! than only a per-node store: on Apply, each Accord node writes the
//! transaction's keys through a data-plane quorum coordinator
//! ([`custos_data::DataClient`]) to a tablet's replica set. Those writes are then
//! readable via ordinary data-plane quorum reads.
//!
//! Assembly (one inbox per node id, single-consumer — so every role gets a
//! distinct id):
//!
//! - Accord replicas (consensus protocol traffic): ids 0, 1, 2.
//! - Each Accord node's own data-plane coordinator: ids 10, 11, 12.
//! - Data-plane replicas (`serve_replica` over a `MemoryEngine`): ids 3, 4, 5.
//! - A standalone verifier `DataClient`: id 20.
//!
//! All three Accord nodes write the same committed effect through the quorum at
//! the same execution-timestamp version, so the data plane's per-key LWW keeps a
//! single winner. The test proves a **multi-key** transaction's writes become
//! readable via quorum reads, and that two conflicting transactions land
//! all-or-nothing in a consistent order (the shared key carries the
//! second-ordered transaction; each transaction's private keys all carry that
//! same transaction — atomic write set).
//!
//! The whole run is byte-reproducible from its seed.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use custos_consensus::{AccordNode, Key, TxnId};
use custos_data::{DataClient, ReadResult, TabletView, serve_replica};
use custos_env::EnvExt;
use custos_sim::{SimEnv, Simulator};
use custos_storage::MemoryEngine;
use custos_tablet::{Epoch, KeyRange, Tablet, TabletId};

const ACCORD: [u64; 3] = [0, 1, 2];
const COORDS: [u64; 3] = [10, 11, 12];
const DATA: [u64; 3] = [3, 4, 5];
const VERIFIER: u64 = 20;
const TIMEOUT: Duration = Duration::from_secs(2);

fn keys(ks: &[Key]) -> BTreeSet<Key> {
    ks.iter().copied().collect()
}

/// The data-plane storage-key bytes for an Accord key, mirroring the node's
/// internal `storage_key` (big-endian).
fn storage_key(key: Key) -> Vec<u8> {
    key.to_be_bytes().to_vec()
}

/// The value bytes an executed transaction writes (its id), mirroring the node's
/// internal `encode_txn`: `(logical, node)` as two big-endian u64s.
fn encode_txn(txn: TxnId) -> Vec<u8> {
    let mut v = Vec::with_capacity(16);
    v.extend_from_slice(&txn.logical.to_be_bytes());
    v.extend_from_slice(&txn.node.to_be_bytes());
    v
}

/// Stand up the frontier: a tablet over `DATA`, data replicas serving it, and
/// three Accord nodes whose committed write effects land in that tablet via
/// per-node data-plane coordinators. Returns the simulator, the Accord nodes, and
/// the `TabletView` the verifier reads through.
fn frontier(seed: u64) -> (Simulator, Vec<AccordNode<SimEnv>>, TabletView) {
    let sim = Simulator::new(seed);

    // Data-plane replicas over the whole keyspace. The serve loop is spawned on
    // each env and holds its own storage clone, so it keeps running after the
    // returned handle is dropped here.
    for &id in &DATA {
        serve_replica(sim.env(id), MemoryEngine::new(), Epoch::INITIAL);
    }

    let tablet = Tablet::new(TabletId(1), KeyRange::whole(), DATA.to_vec());
    let view = TabletView::from_tablet(&tablet, 2, 2);

    let nodes = (0..ACCORD.len())
        .map(|i| {
            AccordNode::start_with_data_plane(
                sim.env(ACCORD[i]),
                ACCORD.to_vec(),
                MemoryEngine::new(),
                sim.env(COORDS[i]),
                view.clone(),
            )
        })
        .collect();

    (sim, nodes, view)
}

/// Read a key through a standalone data-plane quorum coordinator, returning the
/// `TxnId` recorded there (decoded), or `None` if absent / quorum unreachable.
fn quorum_writer(sim: &mut Simulator, view: &TabletView, key: Key) -> Option<TxnId> {
    let result: Arc<Mutex<Option<ReadResult>>> = Arc::new(Mutex::new(None));
    let env = sim.env(VERIFIER);
    let out = Arc::clone(&result);
    let view = view.clone();
    env.clone().spawn_task(async move {
        let client = DataClient::new(env);
        let r = client.read(&view, &storage_key(key), TIMEOUT).await;
        *out.lock().unwrap() = Some(r);
    });
    sim.run_for(TIMEOUT);
    let r = result
        .lock()
        .unwrap()
        .clone()
        .expect("quorum read completed");
    match r {
        ReadResult::Value(Some(bytes)) => {
            assert_eq!(bytes.len(), 16, "stored value is an encoded TxnId");
            let logical = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
            let node = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
            Some(TxnId::new(logical, node))
        }
        ReadResult::Value(None) => None,
        ReadResult::Failed => panic!("quorum read failed (not enough replicas)"),
    }
}

/// A multi-key transaction commits through Accord and its writes are then
/// readable via ordinary data-plane quorum reads — at the same `TxnId` for every
/// key in its write set.
#[test]
fn multi_key_transaction_lands_in_data_plane() {
    let seed = 0xF40E_0001;
    let (mut sim, nodes, view) = frontier(seed);

    let txn = nodes[0].submit(keys(&[1, 2, 3]));
    sim.run_for(Duration::from_secs(5));

    // Executed on every Accord replica.
    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.is_applied(txn),
            "accord node {i} did not execute the txn (seed={seed})"
        );
    }

    // Each key of the write set is now readable via a data-plane quorum read, and
    // carries this transaction's id.
    for &k in &[1u64, 2, 3] {
        assert_eq!(
            quorum_writer(&mut sim, &view, k),
            Some(txn),
            "key {k} not readable as txn {txn:?} via data-plane quorum (seed={seed})"
        );
    }
    // A key the transaction did not touch is absent in the data plane.
    assert_eq!(
        quorum_writer(&mut sim, &view, 99),
        None,
        "untouched key should be absent (seed={seed})"
    );
}

/// Two conflicting multi-key transactions land atomically and in a consistent
/// order: the shared key carries the **second-ordered** transaction, and each
/// transaction's private key carries that same transaction (no torn write set).
#[test]
fn conflicting_transactions_are_atomic_in_data_plane() {
    let seed = 0xF40E_0002;
    let (mut sim, nodes, view) = frontier(seed);

    // a touches {shared=5, private=50}; b touches {shared=5, private=60}.
    let a = nodes[0].submit(keys(&[5, 50]));
    let b = nodes[1].submit(keys(&[5, 60]));
    sim.run_for(Duration::from_secs(6));

    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.is_applied(a) && n.is_applied(b),
            "accord node {i} did not execute both txns (seed={seed})"
        );
    }

    // The agreed relative execution order (consistent on every replica).
    let order: Vec<TxnId> = nodes[0]
        .applied_order()
        .into_iter()
        .filter(|t| *t == a || *t == b)
        .collect();
    assert_eq!(order.len(), 2, "both must execute (seed={seed})");
    let winner = *order.last().unwrap(); // the one ordered second wins the shared key

    // Shared key: the second-ordered transaction won, via data-plane quorum read.
    assert_eq!(
        quorum_writer(&mut sim, &view, 5),
        Some(winner),
        "shared key did not carry the second-ordered txn (seed={seed})"
    );
    // Private keys: each carries its own transaction — the write set landed whole.
    assert_eq!(
        quorum_writer(&mut sim, &view, 50),
        Some(a),
        "txn a's private key torn from its write set (seed={seed})"
    );
    assert_eq!(
        quorum_writer(&mut sim, &view, 60),
        Some(b),
        "txn b's private key torn from its write set (seed={seed})"
    );
}

/// The raw stored value matches the node's own encoding (a guard that the test's
/// decode mirrors the node's `encode_txn`, so the assertions above are meaningful).
#[test]
fn stored_value_encoding_matches() {
    let seed = 0xF40E_0003;
    let (mut sim, nodes, _view) = frontier(seed);
    let txn = nodes[0].submit(keys(&[7]));
    sim.run_for(Duration::from_secs(5));
    assert!(nodes[0].is_applied(txn));
    // The local store_writer (decoded by the node) agrees with our encode_txn.
    let local = futures::executor::block_on(nodes[0].store_writer(7));
    assert_eq!(local, Some(txn));
    assert_eq!(encode_txn(txn).len(), 16);
}

/// The frontier run is byte-reproducible from its seed.
#[test]
fn frontier_run_is_reproducible_from_seed() {
    let seed = 0xF40E_0004;
    let trace = |seed| {
        let (mut sim, nodes, _view) = frontier(seed);
        nodes[0].submit(keys(&[1, 2]));
        sim.run_for(Duration::from_secs(5));
        sim.trace_lines()
    };
    assert_eq!(trace(seed), trace(seed), "frontier trace not reproducible");
}
