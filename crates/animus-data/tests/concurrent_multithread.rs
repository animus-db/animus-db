//! Real-multithreading liveness regression for the data plane.
//!
//! The deterministic single-threaded `SimEnv` proves logic and message ordering
//! but **cannot** prove real-thread liveness: a `std::sync::Mutex` guard held
//! across an `.await`, or a serve loop / background loop / coordinator wedged on
//! a lock or a waker handoff, all run fine under the cooperative single-threaded
//! executor and only deadlock under a genuinely preemptive multi-threaded
//! runtime. (This is exactly the class of bug that stranded the storage WAL
//! group-commit — see `animus-storage/tests/lsm_concurrent.rs`.)
//!
//! This stands up a 3-replica tablet over real `ProdEnv` (real tokio
//! multi-thread runtime, real TCP, real OS clock/RNG), with each replica running
//! both `serve_replica` and the background `serve_anti_entropy` loop, then drives
//! several concurrent `DataClient` coordinators hammering the **same keys** in
//! the **same tablet** with interleaved writes/reads/deletes. It is guarded by a
//! `tokio::time::timeout` so a liveness regression (the replica's epoch `Mutex`,
//! the per-replica storage `Mutex`, or the shared single-consumer inbox wedging
//! under contention) fails loudly instead of hanging CI forever.
//!
//! Audit conclusion (2026-06): the data plane holds **no** `std::sync::Mutex`
//! guard across an `.await` — the replica's epoch `Mutex` is taken and released
//! inside the synchronous `fenced()` helper *before* any storage/network await,
//! and `MemoryEngine`'s internal `Mutex` is likewise never held across an await.
//! This test is the liveness *confidence* that the audit is right under real
//! threads, and a regression guard against a future change reintroducing the
//! cross-await-lock pattern.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use animus_data::{DataClient, ReadResult, TabletView, serve_anti_entropy, serve_replica};
use animus_env::{NodeId, ProdEnv};
use animus_storage::MemoryEngine;
use animus_tablet::{Epoch, TabletId};

const TABLET: TabletId = TabletId(1);
const REPLICAS: [NodeId; 3] = [103, 104, 105];
const COORDS: [NodeId; 4] = [201, 202, 203, 204];
const R: usize = 2;
const W: usize = 2;
const OP_TIMEOUT: Duration = Duration::from_secs(5);
const TEST_TIMEOUT: Duration = Duration::from_secs(60);
/// The keyspace the coordinators contend on — small, so writes/reads/deletes
/// from different coordinators collide on the same keys.
const KEYS: usize = 8;
const ROUNDS: u64 = 40;

fn view() -> TabletView {
    TabletView {
        tablet: TABLET,
        replicas: REPLICAS.to_vec(),
        epoch: Epoch::INITIAL,
        r: R,
        w: W,
    }
}

fn key(i: usize) -> Vec<u8> {
    format!("k{i}").into_bytes()
}

/// Bind a `ProdEnv` for `node_id` on an ephemeral loopback port under `dir`.
async fn bind(node_id: NodeId, dir: &std::path::Path) -> (ProdEnv, SocketAddr) {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    ProdEnv::bind(node_id, addr, dir.join(format!("n{node_id}")))
        .await
        .expect("bind ProdEnv")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_clients_do_not_deadlock_and_converge() {
    let dir = std::env::temp_dir().join(format!("animus-data-mt-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    // Bind every node (replicas + coordinators) and collect the address book.
    let mut peers: BTreeMap<NodeId, SocketAddr> = BTreeMap::new();
    let mut replica_envs = Vec::new();
    for &id in &REPLICAS {
        let (env, addr) = bind(id, &dir).await;
        peers.insert(id, addr);
        replica_envs.push((id, env));
    }
    let mut coord_envs = Vec::new();
    for &id in &COORDS {
        let (env, addr) = bind(id, &dir).await;
        peers.insert(id, addr);
        coord_envs.push(env);
    }

    // Every env can reach every other node.
    for (_, env) in &replica_envs {
        env.set_peers(peers.clone());
    }
    for env in &coord_envs {
        env.set_peers(peers.clone());
    }

    // Start each replica: a serve loop over its own MemoryEngine plus the
    // background anti-entropy loop pushing segment digests to its peers. The
    // engines start independent, so anti-entropy + read-repair are what make raw
    // replica state converge (R + W > N only makes quorum reads intersect).
    for (id, env) in &replica_envs {
        let storage = MemoryEngine::new();
        let handle = serve_replica(env.clone(), storage, Epoch::INITIAL);
        let peers_for: Vec<NodeId> = REPLICAS.iter().copied().filter(|&p| p != *id).collect();
        serve_anti_entropy(
            env.clone(),
            handle,
            TABLET,
            peers_for,
            // Tight interval to maximize contention between the background loop
            // and the foreground serve loop on the shared inbox/epoch lock.
            Duration::from_millis(5),
        );
    }

    let work = async {
        // One concurrent task per coordinator, each running many rounds of
        // write/read/delete against the shared keys. Versions are namespaced by
        // (round, coordinator) so concurrent writers never collide on a version
        // (per-key LWW just keeps the highest), and every op is fire-and-collect
        // through the quorum coordinator over real TCP. We use `tokio::spawn`
        // (test-only — the determinism rule binds library code, not tests) so we
        // can join the coordinators and observe the storm completing rather than
        // hanging.
        let mut tasks = Vec::new();
        for (ci, env) in coord_envs.iter().cloned().enumerate() {
            tasks.push(tokio::spawn(async move {
                let client = DataClient::new(env);
                let v = view();
                for round in 0..ROUNDS {
                    let base = (round * (COORDS.len() as u64) + ci as u64) * 1000 + 1;
                    for i in 0..KEYS {
                        let k = key(i);
                        let version = base + i as u64;
                        // Interleave the three op kinds across keys so different
                        // coordinators hit a given key with different ops
                        // concurrently.
                        match (ci + i + round as usize) % 3 {
                            0 => {
                                let _ = client.write(&v, &k, b"value", version, OP_TIMEOUT).await;
                            }
                            1 => {
                                let _ = client.read(&v, &k, OP_TIMEOUT).await;
                            }
                            _ => {
                                let _ = client.delete(&v, &k, version, OP_TIMEOUT).await;
                            }
                        }
                    }
                }
            }));
        }
        for t in tasks {
            t.await.expect("coordinator task panicked");
        }
    };

    tokio::time::timeout(TEST_TIMEOUT, work).await.expect(
        "data plane deadlocked: a coordinator, the replica serve loop, or the \
         anti-entropy loop wedged under real multi-threaded contention",
    );

    // Liveness is the headline assertion. As a state-sanity check, drive a fresh
    // coordinator and confirm the cluster still serves quorum reads for every
    // contended key (it neither deadlocked nor poisoned a replica's storage).
    let probe_env = {
        let (env, addr) = bind(299, &dir).await;
        peers.insert(299, addr);
        // Refresh every env's book so they can reply to the probe.
        for (_, e) in &replica_envs {
            e.set_peers(peers.clone());
        }
        env.set_peers(peers.clone());
        env
    };
    let probe = DataClient::new(probe_env);
    let v = view();
    // Give anti-entropy a few rounds to settle, then assert each key is readable
    // (Value, not Failed) — i.e. a read quorum still responds after the storm.
    tokio::time::sleep(Duration::from_millis(200)).await;
    for i in 0..KEYS {
        let res = tokio::time::timeout(OP_TIMEOUT, probe.read(&v, &key(i), OP_TIMEOUT))
            .await
            .expect("probe read timed out — quorum unavailable after the storm");
        assert!(
            matches!(res, ReadResult::Value(_)),
            "key {i} could not reach a read quorum after concurrent storm: {res:?}"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}
