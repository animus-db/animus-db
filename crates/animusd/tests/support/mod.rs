//! Shared support for the `animusd` integration tests.
//!
//! Each `tests/*.rs` file is its own crate that pulls this module in via `mod
//! support;`, and no single test file uses every helper here — so per-binary
//! dead-code analysis flags whichever ones a given consumer doesn't call.
//! `#![allow(dead_code)]` is the standard fix for a shared multi-consumer test
//! support module (same shape as `tests/common/mod.rs` elsewhere).
#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use animusd::{ClusterConfig, Node, RoleAddrs, StorageBackend};

/// Reserve `count` free loopback ports (bind :0, read addr, release the
/// listener). This is itself the source of the documented port-TOCTOU: the
/// port is free the instant this returns, so another test binary's own probe
/// can steal it before the real bind. Callers that build a **fresh** config
/// per attempt (e.g. [`start_single_node`], or a per-process cluster
/// bring-up helper) ride this out by retrying the whole
/// allocate-fresh-ports-and-start unit; a same-address restart that must
/// reuse a captured config instead retries the rebind itself (see
/// [`restart_same_addrs`]).
pub fn free_addrs(count: usize) -> Vec<SocketAddr> {
    let listeners: Vec<std::net::TcpListener> = (0..count)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    listeners.iter().map(|l| l.local_addr().unwrap()).collect()
    // listeners dropped here, freeing the ports for the caller to bind.
}

/// A single-node config pinned to fresh ephemeral addresses.
fn single_node_config() -> ClusterConfig {
    let a = free_addrs(6);
    ClusterConfig {
        nodes: vec![RoleAddrs {
            role: animusd::config::NodeRole::Both,
            control: Some(a[0]),
            client: a[1],
            dynamo: a[2],
            cql: a[3],
            raftkv: Some(a[4]),
            admin: a[5],
        }],
    }
}

/// Start a single-node cluster, retrying bring-up against the port-TOCTOU
/// race documented on [`free_addrs`] — each attempt allocates a **fresh**
/// config (new ports), since unlike [`restart_same_addrs`] there is no
/// existing config this helper is bound to reuse.
pub async fn start_single_node(dir: &Path, backend: StorageBackend) -> (Node, ClusterConfig) {
    let mut last_err = None;
    for attempt in 0..10 {
        let config = single_node_config();
        match animusd::run_node_with(&config, 0, dir, backend).await {
            Ok(node) => return (node, config),
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(50 * (attempt + 1))).await;
            }
        }
    }
    panic!("single node failed to start after 10 attempts: {last_err:?}");
}

/// Restart a node on the **same addresses + data dir** (the durability tests'
/// same-address recovery), retrying the rebind briefly. A clean shutdown frees
/// the ports, but another test binary's `free_addrs` probe can bind a just-freed
/// port for a moment (the documented port-TOCTOU) — and unlike a *first*
/// bring-up, a same-address restart cannot re-allocate around the thief (reusing
/// the captured config *is the test*). The probe holds the port only
/// microseconds, so a bounded retry rides it out; a genuinely occupied port
/// still fails when the deadline exhausts.
pub async fn restart_same_addrs(
    config: &ClusterConfig,
    index: usize,
    dir: &Path,
    backend: StorageBackend,
) -> Node {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match animusd::run_node_with(config, index, dir, backend).await {
            Ok(node) => return node,
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "restart on the same dir/addresses did not rebind: {e}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}
