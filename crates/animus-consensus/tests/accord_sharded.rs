//! ADR 0011 **sharded (multi-tablet) transactions** acceptance tests.
//!
//! Accord is naturally multi-shard: the consensus round replicates every
//! transaction to the whole Accord replica set, agreeing **one global execution
//! timestamp** and one dependency set regardless of which tablets the keys live
//! in. The only place sharding shows up is the *execution effect* — each key must
//! be written to (and read from) **its own** tablet's replica set. An
//! [`AccordNode`] started via [`AccordNode::start_with_router`] carries a
//! [`Router`] over a multi-tablet map and routes the per-key data-plane
//! write/read accordingly.
//!
//! Assembly (one inbox per node id, single-consumer):
//!
//! - Accord replicas (consensus protocol traffic): ids 0, 1, 2.
//! - Each Accord node's own data-plane coordinator: ids 10, 11, 12.
//! - Data-plane replicas (`serve_replica` over a `MemoryEngine`): ids 3, 4, 5.
//! - A standalone verifier `DataClient`: id 20.
//!
//! Two tablets partition the (big-endian `u64`) key space at `1000`: tablet 1
//! owns `[_, BE(1000))` on replicas {3, 4}; tablet 2 owns `[BE(1000), _)` on
//! replicas {4, 5}. So a transaction over `{5, 5000}` spans both tablets.
//!
//! The whole run is byte-reproducible from its seed.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_consensus::{AccordNode, Key, TxnId};
use animus_data::{DataClient, ReadResult, Router, serve_replica};
use animus_env::EnvExt;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{Epoch, KeyRange, Tablet, TabletId};

const ACCORD: [u64; 3] = [0, 1, 2];
const COORDS: [u64; 3] = [10, 11, 12];
const DATA: [u64; 3] = [3, 4, 5];
const VERIFIER: u64 = 20;
const TIMEOUT: Duration = Duration::from_secs(2);

/// The tablet-split boundary (a `u64` key): keys `< 1000` live in tablet 1, keys
/// `>= 1000` in tablet 2.
const SPLIT: Key = 1000;

fn keys(ks: &[Key]) -> BTreeSet<Key> {
    ks.iter().copied().collect()
}

/// The data-plane storage-key bytes for an Accord key (big-endian, mirroring the
/// node's internal `storage_key`).
fn storage_key(key: Key) -> Vec<u8> {
    key.to_be_bytes().to_vec()
}

/// Two tablets partitioning the key space at [`SPLIT`]: tablet 1 owns
/// `[_, BE(SPLIT))` on {3, 4}; tablet 2 owns `[BE(SPLIT), _)` on {4, 5}.
fn tablets() -> Vec<Tablet> {
    let boundary = storage_key(SPLIT);
    vec![
        Tablet::new(
            TabletId(1),
            KeyRange::new(Vec::new(), Some(boundary.clone())),
            vec![3, 4],
        ),
        Tablet::new(TabletId(2), KeyRange::new(boundary, None), vec![4, 5]),
    ]
}

/// Stand up the sharded frontier: data replicas {3,4,5} serving two tablets, and
/// three Accord nodes routing their committed effects per key via a `Router`.
fn sharded(seed: u64) -> (Simulator, Vec<AccordNode<SimEnv>>, Router) {
    let sim = Simulator::new(seed);

    for &id in &DATA {
        serve_replica(sim.env(id), MemoryEngine::new(), Epoch::INITIAL);
    }

    let router = Router::new(tablets(), 2, 2);

    let nodes = (0..ACCORD.len())
        .map(|i| {
            AccordNode::start_with_router(
                sim.env(ACCORD[i]),
                ACCORD.to_vec(),
                MemoryEngine::new(),
                sim.env(COORDS[i]),
                router.clone(),
            )
        })
        .collect();

    (sim, nodes, router)
}

/// Read a key through a standalone data-plane quorum coordinator, routing to the
/// key's owning tablet, returning the recorded `TxnId` (decoded) or `None`.
fn quorum_writer(sim: &mut Simulator, router: &Router, key: Key) -> Option<TxnId> {
    let result: Arc<Mutex<Option<ReadResult>>> = Arc::new(Mutex::new(None));
    let env = sim.env(VERIFIER);
    let out = Arc::clone(&result);
    let view = router
        .view_for(&storage_key(key))
        .expect("key must route to a tablet");
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

/// A transaction whose key set spans **two tablets** commits atomically through
/// Accord, and **both** keys become readable via data-plane quorum reads — each
/// from its own tablet — at the same `TxnId`.
#[test]
fn cross_tablet_transaction_lands_atomically() {
    let seed = 0x5A0E_0001;
    let (mut sim, nodes, router) = sharded(seed);

    // 5 → tablet 1 ({3,4}); 5000 → tablet 2 ({4,5}). One transaction, two shards.
    assert_eq!(
        router.view_for(&storage_key(5)).unwrap().tablet,
        TabletId(1)
    );
    assert_eq!(
        router.view_for(&storage_key(5000)).unwrap().tablet,
        TabletId(2)
    );

    let txn = nodes[0].submit(keys(&[5, 5000]));
    sim.run_for(Duration::from_secs(5));

    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.is_applied(txn),
            "accord node {i} did not execute the cross-tablet txn (seed={seed})"
        );
    }

    // Both keys readable via their own tablet's quorum, at this transaction's id.
    for &k in &[5u64, 5000] {
        assert_eq!(
            quorum_writer(&mut sim, &router, k),
            Some(txn),
            "key {k} not readable as txn {txn:?} via its tablet's quorum (seed={seed})"
        );
    }
}

/// Two conflicting cross-tablet transactions order **consistently on every
/// shard**: the shared key (in tablet 1) carries the second-ordered transaction,
/// and each transaction's private key (in the *other* tablet) carries that same
/// transaction — no torn write set across tablets.
#[test]
fn conflicting_cross_tablet_transactions_order_consistently() {
    let seed = 0x5A0E_0002;
    let (mut sim, nodes, router) = sharded(seed);

    // a touches {shared=7 (tablet 1), private=7000 (tablet 2)};
    // b touches {shared=7 (tablet 1), private=8000 (tablet 2)}.
    let a = nodes[0].submit(keys(&[7, 7000]));
    let b = nodes[1].submit(keys(&[7, 8000]));
    sim.run_for(Duration::from_secs(6));

    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.is_applied(a) && n.is_applied(b),
            "accord node {i} did not execute both cross-tablet txns (seed={seed})"
        );
    }

    // The agreed relative execution order, consistent on every Accord replica.
    let order_on = |n: &AccordNode<SimEnv>| -> Vec<TxnId> {
        n.applied_order()
            .into_iter()
            .filter(|t| *t == a || *t == b)
            .collect()
    };
    let reference = order_on(&nodes[0]);
    assert_eq!(reference.len(), 2, "both must execute (seed={seed})");
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            order_on(n),
            reference,
            "accord node {i} diverged on cross-tablet order (seed={seed})"
        );
    }
    let winner = *reference.last().unwrap();

    // Shared key (tablet 1): the second-ordered transaction won.
    assert_eq!(
        quorum_writer(&mut sim, &router, 7),
        Some(winner),
        "shared key did not carry the second-ordered txn (seed={seed})"
    );
    // Private keys (tablet 2): each carries its own transaction.
    assert_eq!(
        quorum_writer(&mut sim, &router, 7000),
        Some(a),
        "txn a's private (other-tablet) key torn from its write set (seed={seed})"
    );
    assert_eq!(
        quorum_writer(&mut sim, &router, 8000),
        Some(b),
        "txn b's private (other-tablet) key torn from its write set (seed={seed})"
    );
}

/// The sharded frontier run is byte-reproducible from its seed.
#[test]
fn sharded_run_is_reproducible_from_seed() {
    let seed = 0x5A0E_0003;
    let trace = |seed| {
        let (mut sim, nodes, _router) = sharded(seed);
        nodes[0].submit(keys(&[5, 5000]));
        sim.run_for(Duration::from_secs(5));
        sim.trace_lines()
    };
    assert_eq!(trace(seed), trace(seed), "sharded trace not reproducible");
}
