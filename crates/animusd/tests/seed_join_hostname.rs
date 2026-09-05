//! `--seed` accepts a hostname, not just a literal socket address (pod-
//! friendliness on Kubernetes, where a seed Service's DNS name is the honest
//! address to hand a joining pod — see the crate guide's Kubernetes operator
//! entry): seed entries flow through the join chain as `host:port` strings
//! and resolve at dial time via `TcpStream::connect`'s own `ToSocketAddrs`
//! handling, so no pre-resolution step exists to get stale.
//!
//! This proves a `localhost:<port>` seed — a real hostname resolving to
//! 127.0.0.1, not a literal address — actually joins a real cluster end to
//! end, through the same `run_node_join` public join entry point every other
//! join test in this directory uses (`tests/seed_join.rs`).
//!
//! Real TCP/time — a converged-or-timeout poll, never a fixed-deadline
//! one-shot assert (see `docs/engineering-lessons.md`'s Testing section).

use std::collections::BTreeMap;
use std::path::Path;
use std::slice;

use animusd::config::NodeRole;
use animusd::{ClusterConfig, Node, RoleAddrs, StorageBackend};

mod support;

async fn bring_up(n: usize, dir: &Path) -> (Vec<Node>, ClusterConfig) {
    support::bring_up_deadline(n, dir, support::JOIN_DEADLINE).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn node_joins_via_a_hostname_seed() {
    let dir = support::panic_safe_tempdir();

    // 1. A single-node core to seed against.
    let (core_nodes, core_config) = bring_up(1, dir.path()).await;
    let seed_intra_port = core_config.nodes[0].intra.port();

    // 2. The seed is `localhost:<port>` — a real hostname, never resolved by
    // the test — handed to the join chain exactly as `main.rs`'s
    // `parse_seed_arg` would pass a `--seed` entry through.
    let seeds = vec![format!("localhost:{seed_intra_port}")];

    // 3. Join a second node against the hostname seed through the public
    // `run_node_join` entry point, with the same bounded port-TOCTOU retry
    // the shared harness uses (see `support::join_fresh_deadline`).
    let join_index = 1;
    let hard_deadline = tokio::time::Instant::now() + support::JOIN_DEADLINE;
    let mut attempt: u64 = 0;
    let joined = loop {
        let raw = support::free_addrs(6);
        let id = animusd::config::node_id(join_index);
        let addrs = RoleAddrs {
            id: id.clone(),
            role: NodeRole::Both,
            internal: raw[0],
            client: raw[1],
            dynamo: raw[2],
            admin: raw[3],
            intra: raw[4],
            console: raw[5],
            advertise_host: None,
            tls: None,
        };
        let node_dir = dir.path().join(format!("join-{join_index}-{attempt}"));
        match animusd::run_node_join(
            seeds.clone(),
            Some(id),
            addrs,
            &node_dir,
            StorageBackend::default(),
            BTreeMap::new(),
        )
        .await
        {
            Ok(node) => break node,
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < hard_deadline,
                    "hostname-seed join did not succeed before the deadline; last error: {e}"
                );
                attempt += 1;
            }
        }
    };

    // 4. Converged-or-timeout: the joined node becomes an Active member of
    // the core's own replicated metadata (the unmodified ADR 0012 heartbeat/
    // failure-detector promotion chain — no test-side force).
    let join_raftkv_id = animusd::config::node_id(join_index);
    support::await_data_nodes_active(&core_nodes, slice::from_ref(&join_raftkv_id)).await;

    joined.shutdown_graceful().await;
    for node in core_nodes {
        node.shutdown_graceful().await;
    }
}
