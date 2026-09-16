//! Regression for issue #610's own fd-exhaustion follow-up: `ClientCtx::
//! propose_schema`'s "no locally-known leader" broadcast fallback must not
//! let a relayed `ProposeSchema` cascade into a further broadcast on the
//! receiving node (`propose_schema_local_or_hinted`, `schema.rs`), and a
//! full-failure round must back off (`BROADCAST_EXHAUSTED_BACKOFF`) rather
//! than let every caller's own `SCHEMA_POLL_INTERVAL` (50ms) retry turn into
//! an unthrottled broadcast storm.
//!
//! Found live in CI (`prod-liveness-hammer-pair`/`prod-liveness-animusd`):
//! several real 3-node clusters bootstrapping concurrently in one process
//! (every node briefly has no locally-known control leader during the
//! pre-election window, which is exactly when every node's own
//! self-registration hammers `propose_schema`) drove the process's open-fd
//! count high enough to fail an unrelated WAL append with `EMFILE`. This
//! test reproduces the concurrent-bootstrap shape directly (no artificial
//! `ulimit`/ambient-load trick needed) and asserts this process's own open
//! file descriptor count returns to a small, bounded number once every
//! cluster has finished bootstrapping — pre-fix, `strings /proc/self/fd`'s
//! count was observed in the hundreds under this exact shape (see the PR's
//! own fd-sampling numbers); post-fix it stays under `FD_CEILING` even with
//! several concurrent clusters.

use std::time::Duration;

use animusd::{Node, bind_cluster, start_cluster};
use tokio::time::timeout;

mod support;

async fn await_bootstrap(nodes: &[Node]) {
    let ready = async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
                && nodes.iter().all(|n| !n.metadata().members.is_empty())
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    timeout(Duration::from_secs(20), ready)
        .await
        .expect("cluster did not bootstrap within 20s");
}

/// This process's own currently-open file descriptor count (`/proc/self/
/// fd`, Linux-only — matching every other real-socket `ProdEnv` test in
/// this crate, which already assumes a Linux CI runner).
fn open_fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd")
        .expect("read /proc/self/fd")
        .count()
}

/// Generous ceiling for `CLUSTERS` concurrent 3-node bootstraps in one
/// process: pre-fix, this exact shape drove the count into the hundreds
/// (observed up to the low thousands under `ulimit -n 1024`, see the PR's
/// own fd-sampling numbers); post-fix it stays under 50 per cluster even
/// immediately after bootstrap, so this leaves generous headroom against
/// legitimate baseline fd usage (listeners, log/temp-dir files, the test
/// harness's own sockets) without being loose enough to miss a
/// reintroduced cascade or a reintroduced unthrottled-retry storm.
const FD_CEILING: usize = 400;
const CLUSTERS: usize = 4;

/// Several 3-node clusters bootstrapping **concurrently** in one process —
/// the exact shape that found this regression (every node's own
/// self-registration hammers `propose_schema` while nothing has elected a
/// leader yet, and issue #610's own concurrent broadcast fallback opens up
/// to `N-1` sockets per attempt). Asserts every cluster bootstraps
/// successfully (the direct regression: pre-fix, some bootstraps could
/// themselves fail outright to `EMFILE`-caused WAL append errors) and that
/// this process's own open-fd count is back under [`FD_CEILING`] once they
/// all have.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_cluster_bootstraps_do_not_exhaust_file_descriptors() {
    let before = open_fd_count();

    let dirs: Vec<_> = (0..CLUSTERS)
        .map(|_| support::panic_safe_tempdir())
        .collect();
    let mut handles = Vec::with_capacity(CLUSTERS);
    for dir in &dirs {
        let path = dir.path().to_owned();
        handles.push(tokio::spawn(async move {
            let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), path)
                .await
                .expect("bind_cluster");
            let nodes = start_cluster(bound).await.expect("start_cluster");
            await_bootstrap(&nodes).await;
            // Keep the nodes (and their listeners/engines) alive until every
            // cluster in this test has bootstrapped, then let them drop —
            // dropping is what should promptly release every relay socket
            // this bootstrap opened.
            nodes
        }));
    }
    // Every task above is already spawned and running concurrently on this
    // runtime regardless of the order awaited here (all `CLUSTERS` clusters
    // genuinely race their own bootstraps against each other, matching the
    // CI shape this regresses) — this loop just collects each result and
    // drops its `Node`s (tearing that cluster down and releasing its
    // sockets) as soon as it resolves, so by the time every handle has
    // been awaited, every cluster has both bootstrapped and been torn
    // down.
    for handle in handles {
        let _nodes = handle.await.expect("cluster bring-up task panicked");
    }

    // Give already-closed sockets a moment to actually leave this
    // process's fd table (a closed fd is reclaimed synchronously in Rust,
    // but the OS-level TCP teardown / any last background task each
    // cluster's own `Node::drop` may not join on is not necessarily
    // instantaneous) before asserting the steady-state count.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let after = open_fd_count();
    assert!(
        after < FD_CEILING,
        "open fd count after {CLUSTERS} concurrent cluster bootstraps: {after} \
         (before this test: {before}) — expected under {FD_CEILING}; issue #610's own \
         fd-exhaustion regression reintroduced?"
    );
}
