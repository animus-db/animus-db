//! Shared support for the `animusd` integration tests.

use std::path::Path;
use std::time::Duration;

use animusd::{ClusterConfig, Node, StorageBackend};

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
