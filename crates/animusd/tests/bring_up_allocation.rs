//! Deterministic coverage for [`support::bring_up_deadline`]'s own
//! allocation mechanism (issue #627): every address it hands out is bound
//! by [`Node::bind`] itself (`:0`, OS-assigned at bind time) and held open
//! by the live cluster for as long as it runs — never probed-and-released
//! first — so two concurrent bring-ups can never be handed the same
//! address, and no address in a live config is ever bindable by anyone
//! else while the cluster that owns it is still up.
//!
//! Real sockets/time (the `ProdEnv` edge), same posture as every other
//! `animusd` integration test — but each test here stays small and bounded
//! (a 2-3 node cluster, a short deadline), so it runs in a few seconds.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::time::Duration;

use animusd::RoleAddrs;

mod support;

/// Two independent 3-node bring-ups, driven concurrently in one runtime via
/// `tokio::join!`, never overlap on a single address — 36 distinct
/// addresses across both clusters. If `bring_up_deadline` still probed and
/// released ports the old way, two concurrent callers racing `free_addrs`
/// could observe (and then rebind) the same freshly-released port; the
/// bind-and-hold fixture makes that structurally impossible, since each
/// `Node::bind` call resolves and holds its own address atomically.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_bring_ups_never_overlap() {
    let dir = support::panic_safe_tempdir();
    let dir_a = dir.path().join("a");
    let dir_b = dir.path().join("b");
    let deadline = Duration::from_secs(30);

    let ((nodes_a, config_a), (nodes_b, config_b)) = tokio::join!(
        support::bring_up_deadline(3, &dir_a, deadline),
        support::bring_up_deadline(3, &dir_b, deadline),
    );

    let mut addrs: BTreeSet<SocketAddr> = BTreeSet::new();
    for config in [&config_a, &config_b] {
        for node in &config.nodes {
            for addr in every_addr(node) {
                assert!(
                    addrs.insert(addr),
                    "address {addr} was allocated by both concurrent bring-ups"
                );
            }
        }
    }
    assert_eq!(
        addrs.len(),
        36,
        "expected 36 distinct addresses (2 clusters x 3 nodes x 6 ports), got {}",
        addrs.len()
    );

    for node in nodes_a.iter().chain(nodes_b.iter()) {
        node.shutdown_graceful().await;
    }
}

/// Every node in a live cluster is bound on exactly the addresses its own
/// `ClusterConfig` entry names (proof `bring_up_deadline`'s reconstructed
/// config matches what actually got bound), and none of those addresses is
/// bindable by anyone else while the cluster lives — no release window at
/// any point, unlike the old probe-then-release-then-rebind fixture. After a
/// graceful shutdown, every address becomes bindable again.
#[tokio::test(flavor = "multi_thread")]
async fn node_is_bound_on_exactly_its_allocated_addresses() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = support::bring_up_deadline(2, dir.path(), Duration::from_secs(30)).await;

    for (i, node) in nodes.iter().enumerate() {
        let cfg = &config.nodes[i];
        assert_eq!(
            node.client_addr(),
            cfg.client,
            "node {i}: client_addr mismatch"
        );
        assert_eq!(
            node.dynamo_addr(),
            cfg.dynamo,
            "node {i}: dynamo_addr mismatch"
        );
        assert_eq!(
            node.admin_addr(),
            cfg.admin,
            "node {i}: admin_addr mismatch"
        );
        assert_eq!(
            node.intra_addr(),
            cfg.intra,
            "node {i}: intra_addr mismatch"
        );
        assert_eq!(
            node.console_addr(),
            cfg.console,
            "node {i}: console_addr mismatch"
        );
    }

    // While the cluster lives, every one of its 6 addresses per node is
    // genuinely held — a fresh bind attempt always fails `AddrInUse`, never
    // "momentarily free" the way a probe-and-release allocator would leave
    // it.
    for cfg in &config.nodes {
        for addr in every_addr(cfg) {
            let err = std::net::TcpListener::bind(addr).expect_err(&format!(
                "address {addr} should still be held by the live cluster"
            ));
            assert_eq!(
                err.kind(),
                std::io::ErrorKind::AddrInUse,
                "address {addr}: unexpected bind error kind {err:?}"
            );
        }
    }

    for node in &nodes {
        node.shutdown_graceful().await;
    }

    // `shutdown_graceful` already waits for every task to finish
    // (`Node::shutdown_and_wait`), but the OS's own socket teardown can lag
    // that by a beat — poll to convergence rather than asserting once.
    let poll_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    for cfg in &config.nodes {
        for addr in every_addr(cfg) {
            loop {
                if std::net::TcpListener::bind(addr).is_ok() {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < poll_deadline,
                    "address {addr} never became bindable again after shutdown_graceful"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// The 6 ports a single [`RoleAddrs`] entry names.
fn every_addr(addrs: &RoleAddrs) -> [SocketAddr; 6] {
    [
        addrs.internal,
        addrs.client,
        addrs.dynamo,
        addrs.admin,
        addrs.intra,
        addrs.console,
    ]
}
