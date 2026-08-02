//! ADR 0011 acceptance tests for **data-plane reads** and the **interactive
//! transaction API**.
//!
//! Two gaps the earlier frontier slice named as deferred:
//!
//! 1. **Read through the data plane.** A read-only Accord transaction wired to
//!    the replicated data plane (`start_with_data_plane`) observes the committed
//!    values from the **data-plane quorum** at its execution time — the same
//!    replicated state a prior write transaction's effect landed in — rather than
//!    a private local snapshot. Because the read is ordered like a write (it only
//!    executes once every earlier-ordered conflicting write has applied, and an
//!    applied write was already pushed through the same quorum), a current quorum
//!    read at execution time observes exactly the writes ordered before it.
//!
//! 2. **Interactive transactions.** A `begin → read → decide → write → commit`
//!    handle ([`AccordNode::begin`]) runs a multi-step read-modify-write under one
//!    Accord transaction; conflicting interactive transactions are ordered
//!    consistently on every replica and land atomically.
//!
//! Assembly mirrors `accord_data_plane.rs` (one inbox per node id):
//! - Accord replicas (consensus traffic): ids 0, 1, 2.
//! - Each Accord node's own data-plane coordinator: ids 10, 11, 12.
//! - Data-plane replicas (`serve_replica` over a `MemoryEngine`): ids 3, 4, 5.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_consensus::{AccordNode, Key, TxnId};
use animus_data::{TabletView, serve_replica};
use animus_env::EnvExt;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{Epoch, KeyRange, Tablet, TabletId};

const ACCORD: [u64; 3] = [0, 1, 2];
const COORDS: [u64; 3] = [10, 11, 12];
const DATA: [u64; 3] = [3, 4, 5];

fn keys(ks: &[Key]) -> BTreeSet<Key> {
    ks.iter().copied().collect()
}

/// Stand up the frontier: a tablet over `DATA`, data replicas serving it, and
/// three Accord nodes whose committed write effects land in (and whose read
/// effects observe) that tablet via per-node data-plane coordinators.
fn frontier(seed: u64) -> (Simulator, Vec<AccordNode<SimEnv>>, TabletView) {
    let sim = Simulator::new(seed);

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

/// A read transaction in an assembled (data + Accord) cluster observes a prior
/// write transaction's data **via the replicated data plane** — i.e. it sees the
/// same writer id the write landed in the data-plane quorum, not a private
/// snapshot.
#[test]
fn read_observes_prior_write_through_data_plane() {
    let seed = 0x4EAD_DA01;
    let (mut sim, nodes, _view) = frontier(seed);

    // Write transaction lands key 7 in the data-plane quorum.
    let w = nodes[0].submit(keys(&[7]));
    sim.run_for(Duration::from_secs(5));
    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.is_applied(w),
            "accord node {i} did not execute the write (seed={seed})"
        );
    }

    // Read transaction ordered after the write — must observe the write's id,
    // read through the data plane.
    let r = nodes[1].submit_read(keys(&[7]));
    sim.run_for(Duration::from_secs(5));

    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.is_applied(r),
            "accord node {i} did not execute the read (seed={seed})"
        );
        let observed = n
            .read_result(r)
            .expect("read executed here")
            .get(&7)
            .copied()
            .flatten();
        assert_eq!(
            observed,
            Some(w),
            "node {i}: read must observe the prior write via the data plane (seed={seed})"
        );
    }
}

/// A read of a key that was never written observes nothing (the data-plane
/// quorum holds no value for it), consistently on every replica.
#[test]
fn read_of_unwritten_key_through_data_plane_is_none() {
    let seed = 0x4EAD_DA02;
    let (mut sim, nodes, _view) = frontier(seed);

    let r = nodes[0].submit_read(keys(&[42]));
    sim.run_for(Duration::from_secs(5));

    for (i, n) in nodes.iter().enumerate() {
        assert!(n.is_applied(r), "node {i} did not execute the read");
        let observed = n.read_result(r).expect("read executed").get(&42).copied();
        assert_eq!(
            observed,
            Some(None),
            "node {i}: unwritten key must read as absent via data plane (seed={seed})"
        );
    }
}

/// Run an interactive read-modify-write **inside a spawned task** (so the
/// data-plane quorum read is driven by the simulator), then drive the simulator
/// and return the committed `TxnId` (and the value the read observed).
///
/// `decide` receives the observed writer of `read_key` and returns the keys to
/// write (empty = no-op commit). The whole `begin → read → decide → write* →
/// commit` runs under one Accord transaction.
/// The outcome of an interactive session: `(value the read observed, committed
/// txn id — `None` if the write set was empty)`.
type Outcome = (Option<TxnId>, Option<TxnId>);

fn run_interactive(
    sim: &mut Simulator,
    node: &AccordNode<SimEnv>,
    coord_id: u64,
    read_key: Key,
    decide: impl FnOnce(Option<TxnId>) -> Vec<Key> + Send + 'static,
) -> Outcome {
    let out: Arc<Mutex<Option<Outcome>>> = Arc::new(Mutex::new(None));
    let node = node.clone();
    let sink = Arc::clone(&out);
    sim.env(coord_id).spawn_task(async move {
        let mut tx = node.begin();
        let observed = tx.read(read_key).await;
        for k in decide(observed) {
            tx.write(k);
        }
        let committed = tx.commit();
        *sink.lock().unwrap() = Some((observed, committed));
    });
    sim.run_for(Duration::from_secs(6));
    let r = out.lock().unwrap().take().expect("interactive session ran");
    // Drive any work the commit kicked off (the PreAccept/Commit rounds + data
    // plane writes) to completion.
    sim.run_for(Duration::from_secs(6));
    r
}

/// An interactive read-modify-write transaction commits atomically: the caller
/// reads the current value, decides, then writes — all under one Accord
/// transaction whose write set lands in the data plane.
#[test]
fn interactive_read_modify_write_commits_atomically() {
    let seed = 0x4EAD_DA03;
    let (mut sim, nodes, _view) = frontier(seed);

    // Seed an initial value at the keys with a plain write transaction.
    let seed_txn = nodes[0].submit(keys(&[1, 2]));
    sim.run_for(Duration::from_secs(5));
    assert!(nodes[0].is_applied(seed_txn));

    // Interactive transaction on node 1 (its coordinator id is 11): read key 1,
    // decide (it was written → also write key 2), commit both atomically.
    let (observed, committed) = run_interactive(&mut sim, &nodes[1], COORDS[1], 1, |obs| {
        if obs.is_some() { vec![1, 2] } else { vec![] }
    });
    assert_eq!(
        observed,
        Some(seed_txn),
        "interactive read must observe the seeded write (seed={seed})"
    );
    let committed = committed.expect("non-empty write set commits");

    // Both keys carry the interactive transaction's id on every replica (atomic
    // write set), and the transaction executed everywhere.
    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.is_applied(committed),
            "node {i} did not execute the interactive txn (seed={seed})"
        );
        assert_eq!(
            futures::executor::block_on(n.store_writer(1)),
            Some(committed),
            "node {i}: key 1 not written by the interactive txn (seed={seed})"
        );
        assert_eq!(
            futures::executor::block_on(n.store_writer(2)),
            Some(committed),
            "node {i}: key 2 torn from the interactive txn's write set (seed={seed})"
        );
    }
}

/// Two conflicting interactive transactions are ordered consistently on every
/// replica: the shared key carries the second-ordered transaction, and each
/// transaction's private key carries that same transaction.
#[test]
fn conflicting_interactive_transactions_are_ordered_consistently() {
    let seed = 0x4EAD_DA04;
    let (mut sim, nodes, _view) = frontier(seed);

    // Two interactive transactions: a touches {shared=5, private=50}; b touches
    // {shared=5, private=60}. Each reads the shared key, then writes its set.
    let (_, a) = run_interactive(&mut sim, &nodes[0], COORDS[0], 5, |_| vec![5, 50]);
    let (_, b) = run_interactive(&mut sim, &nodes[1], COORDS[1], 5, |_| vec![5, 60]);
    let a = a.expect("a commits");
    let b = b.expect("b commits");
    sim.run_for(Duration::from_secs(6));

    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.is_applied(a) && n.is_applied(b),
            "node {i} did not execute both interactive txns (seed={seed})"
        );
    }

    // Consistent relative order on every replica.
    let order = |n: &AccordNode<SimEnv>| -> Vec<TxnId> {
        n.applied_order()
            .into_iter()
            .filter(|t| *t == a || *t == b)
            .collect()
    };
    let reference = order(&nodes[0]);
    assert_eq!(reference.len(), 2, "both must execute (seed={seed})");
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            order(n),
            reference,
            "node {i}: interactive order diverged from replica 0 (seed={seed})"
        );
    }

    let winner = *reference.last().unwrap();
    // Shared key → second-ordered txn; private keys → their own txn.
    assert_eq!(
        futures::executor::block_on(nodes[0].store_writer(5)),
        Some(winner),
        "shared key did not carry the second-ordered txn (seed={seed})"
    );
    assert_eq!(
        futures::executor::block_on(nodes[0].store_writer(50)),
        Some(a),
        "txn a's private key torn from its write set (seed={seed})"
    );
    assert_eq!(
        futures::executor::block_on(nodes[0].store_writer(60)),
        Some(b),
        "txn b's private key torn from its write set (seed={seed})"
    );
}

/// An empty interactive transaction (no writes buffered) is a no-op.
#[test]
fn empty_interactive_transaction_is_a_noop() {
    let seed = 0x4EAD_DA05;
    let (mut sim, nodes, _view) = frontier(seed);
    let (_, committed) = run_interactive(&mut sim, &nodes[0], COORDS[0], 1, |_| vec![]);
    assert_eq!(committed, None, "empty interactive txn must be a no-op");
}

/// The data-plane read + interactive path is byte-reproducible from its seed.
#[test]
fn data_plane_read_run_is_reproducible_from_seed() {
    let seed = 0x4EAD_DA06;
    let trace = |seed| {
        let (mut sim, nodes, _view) = frontier(seed);
        nodes[0].submit(keys(&[1]));
        sim.run_for(Duration::from_secs(4));
        nodes[1].submit_read(keys(&[1]));
        sim.run_for(Duration::from_secs(4));
        sim.trace_lines()
    };
    assert_eq!(
        trace(seed),
        trace(seed),
        "data-plane read trace not reproducible"
    );
}
