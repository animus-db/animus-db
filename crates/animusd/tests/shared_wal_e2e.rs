//! C-05 PR 2 (ADR 0028) — the real-`ProdEnv` end-to-end proof that
//! `--shared-wal`/`--no-shared-wal`/`cluster_settings.shared_wal` actually
//! works wired into a
//! genuine running node: two tables (so two distinct CP-data tablets, each
//! its own `RaftKvNode`, share the one node's per-node `SharedWal` over a
//! real `LsmEngine`/disk), `PutItem`/`GetItem` on both, then a REAL process
//! restart (same data dir, same addresses, `run_node_with_cluster_settings`
//! called fresh) with the flag still on — proving the shared WAL file
//! genuinely round-trips through `SharedWal::open`'s recovery path on real
//! disk, not just under `SimEnv` (`animus-cp-data`'s own
//! `sharedwal_fault_corpus.rs` and `animus-control`'s `shared_wal.rs` unit
//! tests already cover the deterministic/fault-injection side; this is the
//! "does it actually work" real-disk complement, mirroring
//! `heartbeat_batch_liveness.rs`'s own role for that mechanism's cutover).

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

mod support;

async fn dynamo(addr: SocketAddr, target: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to dynamo");
    let request = format!(
        "POST / HTTP/1.1\r\n\
         Host: animus\r\n\
         X-Amz-Target: {target}\r\n\
         Content-Type: application/x-amz-json-1.0\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len(),
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("send request");
    stream.flush().await.expect("flush");
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .await
        .expect("read full response");
    let text = String::from_utf8(raw).expect("utf8 response");
    let (head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status line");
    (status, payload.to_string())
}

async fn create_table(addr: SocketAddr, name: &str) {
    let body = format!(
        r#"{{"TableName":"{name}","AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}],"KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],"BillingMode":"PAY_PER_REQUEST"}}"#
    );
    let (status, resp) = dynamo(addr, "DynamoDB_20120810.CreateTable", &body).await;
    assert_eq!(status, 200, "CreateTable {name} failed: {resp}");
}

async fn put_item(addr: SocketAddr, table: &str, pk: &str, val: &str) {
    let body = format!(
        r#"{{"TableName":"{table}","Item":{{"pk":{{"S":"{pk}"}},"val":{{"S":"{val}"}}}}}}"#
    );
    let (status, resp) = dynamo(addr, "DynamoDB_20120810.PutItem", &body).await;
    assert_eq!(status, 200, "PutItem {table}/{pk} failed: {resp}");
}

async fn get_item_consistent(addr: SocketAddr, table: &str, pk: &str) -> Option<String> {
    let body =
        format!(r#"{{"TableName":"{table}","Key":{{"pk":{{"S":"{pk}"}}}},"ConsistentRead":true}}"#);
    let (status, resp) = dynamo(addr, "DynamoDB_20120810.GetItem", &body).await;
    assert_eq!(status, 200, "GetItem {table}/{pk} failed: {resp}");
    let v: serde_json::Value = serde_json::from_str(&resp).expect("valid JSON response");
    v.get("Item")
        .and_then(|item| item.get("val"))
        .and_then(|val| val.get("S"))
        .and_then(|s| s.as_str())
        .map(str::to_string)
}

/// Poll [`get_item_consistent`] to convergence rather than a single-shot
/// read — the house discipline for any eventual property (root `CLAUDE.md`:
/// "Eventual properties get a converged-or-timeout poll, never a
/// fixed-deadline one-shot assert"). Right after a fresh
/// `run_node_with_cluster_settings` returns, the tablet-host reconciler has
/// not necessarily re-hosted every tablet yet (it is event-driven, on its
/// own `RECONCILE_FALLBACK_INTERVAL`) — a `ConsistentRead` read reaching a
/// not-yet-rehosted group waits out its own route/election budget
/// internally, but a single HTTP round trip can still race a freshly
/// rebound listener's very first requests before that settles.
async fn poll_get_item_consistent(
    addr: SocketAddr,
    table: &str,
    pk: &str,
    expected: &str,
    budget: Duration,
) {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let got = get_item_consistent(addr, table, pk).await;
        if got.as_deref() == Some(expected) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{table}/{pk} never converged to {expected:?} within {budget:?} (last read: {got:?})"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Start (or restart, on a later call with the same `config`/`dir`) a
/// single combined-mode node with `--shared-wal` set to `shared_wal`.
async fn start_node_with(
    config: &animusd::ClusterConfig,
    dir: &Path,
    shared_wal: bool,
) -> Result<animusd::Node, String> {
    animusd::run_node_with_cluster_settings(
        config,
        0,
        dir,
        animusd::StorageBackend::default(),
        animus_control::node::DEFAULT_ORPHAN_SWEEP_AFTER,
        animusd::StreamSealKnobs::default(),
        animusd::SegmentStoreConfig::default(),
        animusd::DEFAULT_STREAM_RETENTION,
        Duration::ZERO,
        false,
        None,
        None,
        None,
        animusd::BackupStoreConfig::default(),
        None,
        None,
        None,
        None,
        None,
        shared_wal,
    )
    .await
    .map_err(|e| e.to_string())
}

/// Start (or restart, on a later call with the same `config`/`dir`) a
/// single combined-mode node with `--shared-wal` on — the whole point of
/// this test.
async fn start_node(config: &animusd::ClusterConfig, dir: &Path) -> Result<animusd::Node, String> {
    start_node_with(config, dir, true).await
}

/// The initial bring-up retries the WHOLE fresh-port-allocation-plus-start
/// as a unit, on a wall-clock deadline — the same port-TOCTOU-resilient
/// shape `tests/support::bring_up_deadline` and every other multi-node
/// fixture in this crate use (a freshly `free_addrs`-allocated port can
/// still lose a bind race under `cargo test --workspace`-level contention
/// before this process claims it; a single-attempt bring-up is a
/// documented flake class in this codebase, not a "this test is flaky"
/// one — see the root `CLAUDE.md` engineering-lessons log).
async fn bring_up_with(
    base_dir: &Path,
    shared_wal: bool,
) -> (animusd::Node, animusd::ClusterConfig, std::path::PathBuf) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut attempt: u64 = 0;
    loop {
        let addrs = support::free_addrs(6);
        let node_cfg = animusd::RoleAddrs {
            id: animusd::config::node_id(0),
            role: animusd::config::NodeRole::Both,
            internal: addrs[0],
            client: addrs[1],
            dynamo: addrs[2],
            admin: addrs[3],
            intra: addrs[4],
            console: addrs[5],
            advertise_host: None,
            tls: None,
            encryption_key_path: None,
        };
        let config = animusd::ClusterConfig {
            nodes: vec![node_cfg],
            dynamo_auth: None,
            cluster_settings: None,
        };
        let dir = base_dir.join(format!("attempt-{attempt}"));
        match start_node_with(&config, &dir, shared_wal).await {
            Ok(node) => return (node, config, dir),
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "could not bring up the node within the deadline: {e}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
                attempt += 1;
            }
        }
    }
}

/// [`bring_up_with`] with `--shared-wal` on — the whole point of this test.
async fn bring_up(base_dir: &Path) -> (animusd::Node, animusd::ClusterConfig, std::path::PathBuf) {
    bring_up_with(base_dir, true).await
}

#[tokio::test(flavor = "multi_thread")]
async fn shared_wal_two_tables_survive_a_real_process_restart() {
    let tmp = support::panic_safe_tempdir();
    let (node, config, dir) = bring_up(tmp.path()).await;
    let dynamo_addr = node.dynamo_addr();

    // Two tables ⇒ two distinct CP-data tablets sharing the one node's
    // per-node SharedWal — the shape this whole PR is about.
    create_table(dynamo_addr, "orders").await;
    create_table(dynamo_addr, "users").await;
    put_item(dynamo_addr, "orders", "o1", "widget").await;
    put_item(dynamo_addr, "users", "u1", "alice").await;
    assert_eq!(
        get_item_consistent(dynamo_addr, "orders", "o1").await,
        Some("widget".to_string())
    );
    assert_eq!(
        get_item_consistent(dynamo_addr, "users", "u1").await,
        Some("alice".to_string())
    );

    // A real process restart: shut this node all the way down (freeing its
    // ports), then bring a FRESH node up on the SAME dir/addresses, still
    // with the flag on — proving `SharedWal::open`'s recovery path round
    // trips on real disk (`LsmEngine`/`ProdEnv`), not just under `SimEnv`.
    node.shutdown_graceful().await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let node2 = loop {
        match start_node(&config, &dir).await {
            Ok(n) => break n,
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "restart on the same dir/addresses did not rebind: {e}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    };
    let dynamo_addr2 = node2.dynamo_addr();

    // Both tables' data — recovered purely off the shared WAL file plus
    // each tablet's own durable engine — must be intact, and a fresh write
    // after the restart must also land. Polled to convergence: the
    // tablet-host reconciler re-hosts each table's tablet asynchronously
    // (event-driven, its own fallback cadence), so a `ConsistentRead` read
    // landing before that settles waits out its own internal route/election
    // budget — this loop is about tolerating THAT, not about tolerating any
    // real data loss (a `None` that never converges within the budget still
    // fails the test).
    let converge_budget = Duration::from_secs(10);
    poll_get_item_consistent(dynamo_addr2, "orders", "o1", "widget", converge_budget).await;
    poll_get_item_consistent(dynamo_addr2, "users", "u1", "alice", converge_budget).await;
    put_item(dynamo_addr2, "orders", "o2", "gadget").await;
    poll_get_item_consistent(dynamo_addr2, "orders", "o2", "gadget", converge_budget).await;

    node2.shutdown_graceful().await;
}

/// ADR 0028's layout-mismatch amendment (C-05 PR 2 follow-up): a restart
/// against an existing data directory with `--shared-wal` flipped from
/// whatever wrote that directory must refuse to start — loudly, with a
/// message naming both layouts and the flag — rather than silently
/// discarding that node's persisted Raft state. Proven both directions
/// through the REAL animusd startup surface
/// (`run_node_with_cluster_settings`'s `Err`, the same path `main.rs`
/// turns into a non-zero exit code), not just the underlying
/// `animus_cp_data::host::check_wal_layout` unit tests (`animus-cp-data`'s
/// own `host::wal_layout_tests`).
#[tokio::test(flavor = "multi_thread")]
async fn a_restart_with_shared_wal_flipped_refuses_to_start() {
    let tmp = support::panic_safe_tempdir();

    // Direction 1: written with --shared-wal ON, restarted with it OFF.
    let (node, config, dir) = bring_up(tmp.path()).await;
    create_table(node.dynamo_addr(), "orders").await;
    put_item(node.dynamo_addr(), "orders", "o1", "widget").await;
    node.shutdown_graceful().await;
    let err = match start_node_with(&config, &dir, false).await {
        Err(e) => e,
        Ok(_) => panic!("a restart with --shared-wal flipped off must fail"),
    };
    assert!(err.contains("--shared-wal"), "error text: {err}");
    assert!(err.contains("shared WAL file"), "error text: {err}");
    assert!(err.contains("Refusing to start"), "error text: {err}");
    assert!(err.contains("persisted Raft state"), "error text: {err}");

    // Direction 2: a FRESH node written with --shared-wal OFF, restarted
    // with it ON.
    let (node2, config2, dir2) = bring_up_with(tmp.path(), false).await;
    create_table(node2.dynamo_addr(), "orders").await;
    put_item(node2.dynamo_addr(), "orders", "o1", "widget").await;
    node2.shutdown_graceful().await;
    let err2 = match start_node_with(&config2, &dir2, true).await {
        Err(e) => e,
        Ok(_) => panic!("a restart with --shared-wal flipped on must fail"),
    };
    assert!(err2.contains("--shared-wal"), "error text: {err2}");
    assert!(err2.contains("per-group WAL file"), "error text: {err2}");
    assert!(err2.contains("Refusing to start"), "error text: {err2}");
    assert!(err2.contains("persisted Raft state"), "error text: {err2}");
}
