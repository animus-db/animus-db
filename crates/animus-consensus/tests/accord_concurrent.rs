//! Real-multithreading liveness regression for the Accord node driver.
//!
//! The deterministic single-threaded `SimEnv` proves the *logic* and *ordering*
//! of the protocol, but it runs every task cooperatively on one thread, so it
//! cannot surface a real-thread liveness bug — e.g. a `std::sync::Mutex` guard
//! held across an `.await`, or a waker handoff that strands a task. (That exact
//! class bit the storage WAL group-commit; see
//! `animus-storage/tests/lsm_concurrent.rs`.)
//!
//! This drives several Accord replicas over the real multi-threaded `ProdEnv`
//! (real tokio runtime, real TCP, real disk) with multiple coordinators
//! concurrently submitting *conflicting* transactions, and is guarded by a
//! `tokio::time::timeout` so a deadlock/strand fails loudly instead of hanging
//! the suite. It also asserts the safety property still holds under genuine
//! parallelism: every replica commits + executes every transaction in the same
//! agreed order, and their executed stores converge.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use animus_consensus::{AccordNode, Key, TxnId};
use animus_env::{NodeId, ProdEnv, nid};
const NODES: [u64; 3] = [0, 1, 2];

fn keys(ks: &[Key]) -> BTreeSet<Key> {
    ks.iter().copied().collect()
}

/// Bind a 3-node cluster over `ProdEnv` on ephemeral loopback ports, exchange
/// addresses, and start an `AccordNode` per node. Returns the nodes and the
/// temp dir guard (kept alive for the test's lifetime).
async fn cluster(tag: &str) -> (Vec<AccordNode<ProdEnv>>, std::path::PathBuf) {
    let base = std::env::temp_dir().join(format!("animus-accord-mt-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);

    let loop_addr = "127.0.0.1:0".parse().unwrap();
    let mut envs = Vec::new();
    let mut addrs: BTreeMap<NodeId, std::net::SocketAddr> = BTreeMap::new();
    for &id in &NODES {
        let dir = base.join(format!("node-{id}"));
        let (env, bound) = ProdEnv::bind(nid(id), loop_addr, &dir)
            .await
            .expect("bind ProdEnv");
        addrs.insert(nid(id), bound);
        envs.push(env);
    }
    // Install the peer address book on every node before any send.
    for env in &envs {
        env.set_peers(addrs.clone());
    }
    let nodes = envs
        .into_iter()
        .map(|env| AccordNode::start(env, NODES.iter().copied().map(nid).collect()))
        .collect();
    (nodes, base)
}

/// Poll until `cond` holds or the deadline passes; returns whether it held.
async fn poll_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let work = async {
        loop {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(timeout, work).await.is_ok()
}

/// Wait until every replica's executed store reports the *same* (non-`None`)
/// writer for `key`, or the deadline passes. The execution effect is applied
/// asynchronously (the core flips `is_applied` before the spawned task `merge`s
/// the write, and the highest-versioned `merge` may still be in flight on some
/// replica), so a store-convergence assertion must poll.
async fn stores_converge(nodes: &[AccordNode<ProdEnv>], key: Key, timeout: Duration) -> bool {
    let work = async {
        loop {
            let mut writers = Vec::with_capacity(nodes.len());
            for n in nodes {
                writers.push(n.store_writer(key).await);
            }
            let converged = writers[0].is_some() && writers.iter().all(|w| *w == writers[0]);
            if converged {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(timeout, work).await.is_ok()
}

/// Several coordinators concurrently submit conflicting transactions over a real
/// multi-threaded runtime. The driver must not deadlock (no mutex guard held
/// across an `.await`); every replica must commit + execute every transaction in
/// a single consistent order and converge its store. The whole thing is
/// timeout-guarded so a strand fails loudly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_conflicting_coordinators_do_not_deadlock() {
    let (nodes, dir) = cluster("conflict").await;

    // Three coordinators (one per node) each submit a transaction touching the
    // shared key 1 — so all three conflict and must be totally ordered — plus a
    // private key, submitted from three threads at once to maximise contention.
    let n0 = nodes[0].clone();
    let n1 = nodes[1].clone();
    let n2 = nodes[2].clone();
    let h0 = tokio::spawn(async move { n0.submit(keys(&[1, 10])) });
    let h1 = tokio::spawn(async move { n1.submit(keys(&[1, 11])) });
    let h2 = tokio::spawn(async move { n2.submit(keys(&[1, 12])) });
    let a = h0.await.unwrap();
    let b = h1.await.unwrap();
    let c = h2.await.unwrap();
    let txns = [a, b, c];

    // Liveness: every replica executes all three transactions, within a bound.
    let live = {
        let nodes = nodes.clone();
        poll_until(Duration::from_secs(30), move || {
            nodes.iter().all(|n| txns.iter().all(|&t| n.is_applied(t)))
        })
        .await
    };
    assert!(
        live,
        "Accord node driver deadlocked/stranded: not every replica executed \
         every concurrently-submitted conflicting transaction within the timeout"
    );

    // The execution *effect* lands on storage in a task spawned after the core
    // flips `is_applied`, so wait for the shared key's write to converge to one
    // writer on every replica before asserting on the store (still inside the
    // liveness budget — a permanently-stranded apply task would fail here too).
    assert!(
        stores_converge(&nodes, 1, Duration::from_secs(10)).await,
        "execution effect never converged on storage across replicas (apply task stranded)"
    );

    // Safety still holds under real parallelism: every replica agreed on the
    // same execution order for the conflicting transactions.
    let order = |n: &AccordNode<ProdEnv>| -> Vec<TxnId> {
        n.applied_order()
            .into_iter()
            .filter(|t| txns.contains(t))
            .collect()
    };
    let reference = order(&nodes[0]);
    assert_eq!(reference.len(), 3, "all three must be in the applied order");
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            order(n),
            reference,
            "node {i} executed the conflicting txns in a different order"
        );
    }

    // The shared key 1 has a single final writer, identical on every replica.
    let writer0 = nodes[0].store_writer(1).await;
    assert!(writer0.is_some(), "shared key never written");
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            n.store_writer(1).await,
            writer0,
            "node {i} executed store diverged on the shared key"
        );
    }

    for n in &nodes {
        n.env().shutdown_and_wait().await;
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// Many rounds of concurrent submissions from all coordinators, hammering the
/// shared lock + the spawned-task fsync/apply/ship pipeline repeatedly, to shake
/// out any interleaving where a task strands under real-thread contention.
///
/// Each round is an *independent* three-way conflict on its own per-round key,
/// and we drive every round to full commit + execution + store-convergence
/// before starting the next (a completion barrier). That keeps the conflict
/// dependency depth bounded (so we are testing the driver's liveness, not the
/// protocol's behaviour on a pathologically long single-key chain) while still
/// exercising the concurrent lock + spawned-task pipeline on every round. The
/// barrier also means a genuinely stranded task fails the round's timeout
/// loudly. (This slice has no message retry — `Network::send` is fire-and-forget
/// — so we deliberately do not pile unbounded in-flight work on a lossy
/// transport, which would be a transport limitation, not the mutex/waker
/// liveness bug this test targets; see ADR 0011.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_concurrent_rounds_make_progress() {
    let (nodes, dir) = cluster("rounds").await;

    for round in 0..16u64 {
        let shared = 1000 + round; // a fresh contended key per round
        let n0 = nodes[0].clone();
        let n1 = nodes[1].clone();
        let n2 = nodes[2].clone();
        // All three coordinators submit the conflicting transaction at once.
        let h0 = tokio::spawn(async move { n0.submit(keys(&[shared, 10_000 + round])) });
        let h1 = tokio::spawn(async move { n1.submit(keys(&[shared, 20_000 + round])) });
        let h2 = tokio::spawn(async move { n2.submit(keys(&[shared, 30_000 + round])) });
        let a = h0.await.unwrap();
        let b = h1.await.unwrap();
        let c = h2.await.unwrap();
        let txns = [a, b, c];

        // Drive the round to completion on every replica before the next.
        let live = {
            let nodes = nodes.clone();
            poll_until(Duration::from_secs(30), move || {
                nodes.iter().all(|n| txns.iter().all(|&t| n.is_applied(t)))
            })
            .await
        };
        assert!(
            live,
            "round {round}: a task stranded — not every replica executed the \
             round's conflicting transactions within the timeout"
        );

        // Every replica agrees on the round's execution order and store value.
        let order = |n: &AccordNode<ProdEnv>| -> Vec<TxnId> {
            n.applied_order()
                .into_iter()
                .filter(|t| txns.contains(t))
                .collect()
        };
        let reference = order(&nodes[0]);
        assert_eq!(reference.len(), 3, "round {round}: all three must execute");
        for (i, n) in nodes.iter().enumerate() {
            assert_eq!(
                order(n),
                reference,
                "round {round}: node {i} diverged on execution order"
            );
        }
        assert!(
            stores_converge(&nodes, shared, Duration::from_secs(10)).await,
            "round {round}: replica stores never converged on the shared key"
        );
    }

    for n in &nodes {
        n.env().shutdown_and_wait().await;
    }
    let _ = std::fs::remove_dir_all(&dir);
}
