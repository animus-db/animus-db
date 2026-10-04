//! Regression for issue #1094: a same-address restart must not be able to
//! lose a port in the gap between the old node's shutdown and the new node's
//! rebind. Pre-fix, `support::free_addrs` / `Node::bind(:0)` left every port
//! entirely unclaimed for that gap, so any other process's bind could take it
//! (the `Address already in use` flakes of `control_mirror_restart` and
//! `control_metadata_restart`).
//!
//! The thief here is deliberately a socket **without `SO_REUSEADDR`**: it
//! models any process that allocates a port without the reuse flag, and its
//! bind is refused iff the port is still claimed in the kernel's bind table.
//! (`SO_REUSEADDR` thieves that *name the exact port* are not defended
//! against — see `support::reserve_addrs`. The other real thief class,
//! `bind(:0)` and outgoing-connect source ports, is covered by the narrowed
//! `ip_local_port_range` experiment recorded in the lesson.)

use std::net::SocketAddr;

use animus_env::nid;
use animusd::{ClusterConfig, RoleAddrs, StorageBackend};

mod support;

fn assert_port_still_claimed(what: &str, addr: SocketAddr) {
    let thief = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )
    .unwrap();
    // No SO_REUSEADDR: only a port nobody has claimed can be taken.
    let res = thief.bind(&addr.into());
    assert!(
        res.is_err(),
        "{what} {addr} was stealable in the shutdown->rebind gap (no bind-table claim held)"
    );
}

fn all_addrs(a: &RoleAddrs) -> [(&'static str, SocketAddr); 4] {
    [
        ("internal", a.internal),
        ("client", a.client),
        ("admin", a.admin),
        ("intra", a.intra),
    ]
}

/// Combined node: after shutdown, every port stays claimed, and the
/// same-address restart binds them again.
#[tokio::test(flavor = "multi_thread")]
async fn combined_node_ports_stay_claimed_across_a_restart_gap() {
    let dir = support::panic_safe_tempdir();
    let node_dir = dir.path().join("node-0");
    let (node, config) = support::start_single_node(&node_dir, StorageBackend::default()).await;
    node.shutdown_and_wait().await;

    for (what, addr) in all_addrs(&config.nodes[0]) {
        assert_port_still_claimed(what, addr);
    }
    let node = support::restart_same_addrs(&config, 0, &node_dir, StorageBackend::default()).await;
    node.shutdown_and_wait().await;
}

/// Control-only node built from `free_addrs`-style allocation (what the two
/// restart tests in the issue use): same property.
#[tokio::test(flavor = "multi_thread")]
async fn control_only_node_ports_stay_claimed_across_a_restart_gap() {
    let dir = support::panic_safe_tempdir();
    let node_dir = dir.path().join("node-0");
    let a = support::free_addrs(6);
    let config = ClusterConfig {
        version: animusd::config::CLUSTER_CONFIG_VERSION,
        nodes: vec![RoleAddrs {
            id: nid(0),
            role: animusd::config::NodeRole::Control,
            internal: a[0],
            client: a[1],
            dynamo: a[2],
            admin: a[3],
            intra: a[4],
            console: a[5],
            advertise_host: None,
            tls: None,
            encryption_key_path: None,
            overload: None,
        }],
        dynamo_auth: None,
        cluster_settings: None,
    };
    let node = animusd::run_node_control(&config, 0, &node_dir, StorageBackend::Memory)
        .await
        .expect("first start");
    node.shutdown_and_wait().await;

    for (what, addr) in all_addrs(&config.nodes[0]) {
        assert_port_still_claimed(what, addr);
    }
    let node = animusd::run_node_control(&config, 0, &node_dir, StorageBackend::Memory)
        .await
        .expect("restart on the same addresses");
    node.shutdown_and_wait().await;
}
