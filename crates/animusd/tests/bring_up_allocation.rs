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

/// Regression test for issue #1010 (layer 2): a node-assembly function
/// (`BoundNode::start_with_growth` and its siblings) that has already
/// spawned the control-plane Raft driver and `spawn_common_tail`'s own
/// accept loops/background loops, and THEN hits a later fallible `?` step
/// (`animus_cp_data::host::check_wal_layout` refusing a data directory
/// whose on-disk WAL layout disagrees with the `shared_wal` flag), used to
/// leak every task already spawned: the function's own `Vec<JoinHandle<_>>`/
/// env-clone locals were simply dropped along with its early `return
/// Err(..)`, with nothing left to abort the Raft driver or free the six
/// listeners it had already bound and handed off to `spawn_common_tail`.
/// `StartupTasks` (a private RAII guard in `lib.rs`) now aborts every task
/// spawned so far and requests every env's shutdown on exactly this
/// early-return path.
///
/// This drives that real failure end to end: bind a single node, write a
/// bogus per-group WAL file (`raftkv.wal.1`) directly into its internal
/// `ProdEnv`'s own data directory (`<dir>/internal/` — `Node::bind`'s own
/// `dir.join("internal")`, the same directory `check_wal_layout` lists),
/// then start it with `shared_wal: true` (the production default,
/// `main::DEFAULT_SHARED_WAL`) — `check_wal_layout` refuses, since a
/// per-group file alongside `shared_wal: true` is exactly the mismatch it
/// exists to catch. Assert the returned error names the WAL-layout
/// mismatch, then assert — a bounded converged-or-timeout poll, never a
/// single check immediately after the failing call returns
/// (`StartupTasks::Drop` can only *request* the abort/shutdown, never wait
/// for it — `Drop` cannot `.await`) — that every one of this node's six
/// addresses becomes bindable again.
///
/// **Verified RED on unfixed code**: temporarily reverting the
/// `StartupTasks` guard in `lib.rs` (back to a bare `let envs =
/// vec![self.env.clone()];` and `let (ctx, mut tasks) =
/// spawn_common_tail(..);`, with nothing aborting/shutting down on the
/// early return) makes the address-rebind poll below time out instead of
/// ever converging — the leaked accept loops (holding the client/admin/
/// intra/console/dynamo listeners `spawn_common_tail` already started, plus
/// the internal `ProdEnv`'s own still-running Raft driver and accept loop)
/// keep every one of the six addresses held. See this commit's own message
/// for the captured red-run output.
#[tokio::test(flavor = "multi_thread")]
async fn a_late_start_failure_leaks_no_task_or_port() {
    let dir = support::panic_safe_tempdir();
    let unbound = RoleAddrs {
        id: animusd::config::node_id(0),
        role: animusd::config::NodeRole::Both,
        internal: SocketAddr::from(([127, 0, 0, 1], 0)),
        client: SocketAddr::from(([127, 0, 0, 1], 0)),
        dynamo: SocketAddr::from(([127, 0, 0, 1], 0)),
        admin: SocketAddr::from(([127, 0, 0, 1], 0)),
        intra: SocketAddr::from(([127, 0, 0, 1], 0)),
        console: SocketAddr::from(([127, 0, 0, 1], 0)),
        advertise_host: None,
        tls: None,
        encryption_key_path: None,
    };
    let bound = animusd::Node::bind(unbound.id.clone(), unbound, dir.path())
        .await
        .unwrap_or_else(|e| panic!("bind failed: {e}"));

    // The `RoleAddrs` this node actually resolved to (`:0` becomes a real,
    // OS-assigned port at bind time) — used both for `ClusterConfig` and,
    // after the failed start below, for the address-rebind poll.
    let resolved = RoleAddrs {
        id: bound.id().clone(),
        role: animusd::config::NodeRole::Both,
        internal: bound.internal_addr(),
        client: bound.client_addr(),
        dynamo: bound.dynamo_addr(),
        admin: bound.admin_addr(),
        intra: bound.intra_addr(),
        console: bound.console_addr(),
        advertise_host: None,
        tls: None,
        encryption_key_path: None,
    };
    let config = animusd::ClusterConfig {
        nodes: vec![resolved.clone()],
        dynamo_auth: None,
        cluster_settings: None,
    };

    // Write a bogus per-group WAL file straight into this node's internal
    // `ProdEnv` data directory — before this node is ever started, so
    // `check_wal_layout`'s directory listing (`Env::list`, non-recursive
    // over exactly this directory) sees it on its one and only call.
    let internal_dir = dir.path().join("internal");
    std::fs::create_dir_all(&internal_dir).unwrap_or_else(|e| panic!("create internal dir: {e}"));
    std::fs::write(
        internal_dir.join(animus_cp_data::wal_file(1)),
        b"bogus per-group wal content",
    )
    .unwrap_or_else(|e| panic!("write bogus per-group WAL file: {e}"));

    // `Node` implements no `Debug`, so `Result::expect_err` (which requires
    // the `Ok` side to) can't be used here — a plain `match` instead.
    let err = match animusd::start_bound_node_with_streams_quiesce_and_ttl_sweep_interval(
        bound,
        &config,
        0,
        animusd::StorageBackend::Memory,
        animus_control::node::DEFAULT_ORPHAN_SWEEP_AFTER,
        animusd::StreamSealKnobs::default(),
        animusd::SegmentStoreConfig::default(),
        animusd::DEFAULT_STREAM_RETENTION,
        Duration::ZERO, // quiesce_after: off
        false,          // heartbeat_batch: off
        None,           // auto_split_bytes
        None,           // auto_split_change_rate
        None,           // auto_split_ops_rate
        animus_node::ttl_reaper::DEFAULT_TTL_SWEEP_INTERVAL,
        animusd::BackupStoreConfig::default(),
        animus_node::pitr_janitor::DEFAULT_PITR_SNAPSHOT_CADENCE,
        None, // throttle_read_units
        None, // throttle_write_units
        None, // tablet_max_read_units
        None, // tablet_max_write_units
        None, // export_s3
        true, // shared_wal: the production default (main::DEFAULT_SHARED_WAL)
    )
    .await
    {
        Ok(_) => panic!("a per-group WAL file alongside shared_wal: true must refuse to start"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("per-group WAL file"),
        "error must name the WAL-layout mismatch, got: {msg}"
    );
    assert!(
        msg.contains("Refusing to start"),
        "error must say it refused to start, got: {msg}"
    );

    // `StartupTasks::Drop` (issue #1010, layer 2) only ever *requests* the
    // abort/shutdown — `Drop` cannot `.await`, so there is no synchronous
    // guarantee here — hence a bounded poll rather than a single check
    // right after the failing call returns.
    let poll_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    for addr in every_addr(&resolved) {
        loop {
            if std::net::TcpListener::bind(addr).is_ok() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < poll_deadline,
                "address {addr} never became bindable again after the failed start \
                 (issue #1010: a leaked accept loop or Raft driver is still holding it)"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

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
