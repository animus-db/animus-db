//! Drop-table **data GC** end-to-end over `ProdEnv` (ADR 0024, updated for the
//! shared-storage/single-command-split redesign): dropping a table removes its
//! tablets from the replicated map, and every hosting node's GC loop stops the
//! tablet's Raft group, tombstones its range out of the node's one shared
//! storage engine (`RaftKvNode::erase_scope`), and deletes its own per-tablet
//! WAL file (`raftkv.wal.<tablet>`) — there is no more per-tablet LSM engine or
//! sibling env to delete (every tablet on a node shares one `LsmEngine`,
//! confined by its `StorageScope`), and no more durable `cp-hosted` marker (a
//! restart just re-discovers every tablet to host from replicated `Metadata`).
//!
//! Real time + sockets, so it polls with generous timeouts. The single-node
//! test drives a **split** first (the split trigger needs the control leader
//! and the CP leader on the same node — the documented `--cluster`/per-process
//! routing gotcha), then asserts reclamation survives a restart (no
//! resurrection) and that the node keeps serving fresh tables. The 3-node
//! per-process test asserts **every replica** reclaims its own WAL file.
//!
//! A third real-disk test (issue #722, ADR 0061 rung D4 PR 3's finding) adds
//! the missed case: a replica stopped BEFORE the drop, restarted only AFTER
//! the drop has already converged on the others — proving `host::
//! Reconciler`'s second fact source (`EngineFactory::local_tablets`, backed
//! here by the real `LsmTabletFactory`'s own directory listing) reclaims the
//! restarted node's leftover per-tablet **engine** files
//! (`tablet_engine_present`, distinct from `tablet_wal_present`'s Raft-log
//! check above) within a bounded converge-or-timeout poll.

mod support;

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, Node, read_frame, write_frame};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

/// Bring up an `n`-node cluster, one process per node, retrying the
/// (allocate-fresh-ports + start-all) as a unit (the documented port-TOCTOU
/// mitigation). Returns the per-node data dirs so the test can assert on-disk
/// state and restart nodes on the same dirs.
async fn bring_up(
    n: usize,
    dir: &Path,
) -> (Vec<Node>, animusd::ClusterConfig, Vec<std::path::PathBuf>) {
    for attempt in 0..16 {
        let addrs = support::free_addrs(n * 6);
        let nodes_cfg: Vec<animusd::RoleAddrs> = (0..n)
            .map(|i| animusd::RoleAddrs {
                id: animusd::config::node_id(i),
                role: animusd::config::NodeRole::Both,
                internal: addrs[6 * i],
                client: addrs[6 * i + 1],
                dynamo: addrs[6 * i + 2],
                admin: addrs[6 * i + 3],
                intra: addrs[6 * i + 4],
                console: addrs[6 * i + 5],
                advertise_host: None,
                tls: None,
                encryption_key_path: None,
            })
            .collect();
        let config = animusd::ClusterConfig {
            nodes: nodes_cfg,
            dynamo_auth: None,
            cluster_settings: None,
        };
        let dirs: Vec<std::path::PathBuf> = (0..n)
            .map(|i| dir.join(format!("node-{attempt}-{i}")))
            .collect();
        let mut nodes = Vec::new();
        let mut failed = false;
        for (i, node_dir) in dirs.iter().enumerate() {
            match animusd::run_node(&config, i, node_dir).await {
                Ok(node) => nodes.push(node),
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        if !failed {
            return (nodes, config, dirs);
        }
        for node in &nodes {
            node.shutdown_graceful().await;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("could not bring up cluster after retries (ports kept getting stolen)");
}

async fn await_bootstrap(nodes: &[Node]) {
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
                && nodes.iter().all(|n| !n.metadata().members.is_empty())
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("cluster did not bootstrap in 20s");
}

/// One HTTP/1.0 request to the admin endpoint; returns `(status, parsed JSON)`.
async fn admin(addr: SocketAddr, method: &str, path: &str, body: Option<&str>) -> (u16, Value) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to admin");
    let body = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.0\r\nHost: animus\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len(),
    );
    stream.write_all(request.as_bytes()).await.expect("send");
    stream.flush().await.expect("flush");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read response");
    let text = String::from_utf8(raw).expect("utf8 response");
    let (head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status line");
    let value: Value = serde_json::from_str(payload).unwrap_or(Value::Null);
    (status, value)
}

/// Put `key = value` into `table` through a node's client port, with a
/// bounded retry on ANY error reply (a put is idempotent, and both
/// early-cluster/first-provision transients and split/leadership churn
/// surface as a clean, retryable `ClientResponse::Error` — see
/// `docs/engineering-lessons.md`'s "CP write-forward path has no
/// retry-on-not-the-leader-here" entry).
async fn client_put(addr: SocketAddr, table: &str, key: &[u8], value: &[u8]) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let mut stream = TcpStream::connect(addr).await.expect("connect client");
        write_frame(
            &mut stream,
            &ClientRequest::Put {
                key: key.to_vec(),
                value: value.to_vec(),
                table: table.to_string(),
            },
        )
        .await
        .expect("send put");
        let reply: ClientResponse = read_frame(&mut stream)
            .await
            .expect("read reply")
            .expect("a reply");
        match reply {
            ClientResponse::PutOk => return,
            ClientResponse::Error(_) if tokio::time::Instant::now() < deadline => {
                sleep(Duration::from_millis(150)).await;
            }
            other => panic!("put failed: {other:?}"),
        }
    }
}

/// Linearizable read of `key` from `table` through a node's client port.
async fn client_get(addr: SocketAddr, table: &str, key: &[u8]) -> Option<Vec<u8>> {
    let mut stream = TcpStream::connect(addr).await.expect("connect client");
    write_frame(
        &mut stream,
        &ClientRequest::Get {
            key: key.to_vec(),
            table: table.to_string(),
            stale: false,
        },
    )
    .await
    .expect("send get");
    match read_frame(&mut stream)
        .await
        .expect("read reply")
        .expect("a reply")
    {
        ClientResponse::Value(v) => v,
        other => panic!("get failed: {other:?}"),
    }
}

/// The file names directly inside `dir` (empty if the dir does not exist).
fn files_in(dir: &Path) -> Vec<String> {
    match std::fs::read_dir(dir) {
        Ok(entries) => entries
            .filter_map(Result::ok)
            .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
            .filter_map(|e| e.file_name().into_string().ok())
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// Whether `tablet`'s own per-tablet Raft WAL file (`raftkv.wal.<tablet>`)
/// exists in `dir` — the node-local artifact the GC loop must delete for a
/// dropped tablet. The LSM engine's own files are now **shared** across every
/// tablet a node hosts (ADR 0026/0028), so their presence/absence is no
/// longer a per-tablet signal; the WAL file is.
fn tablet_wal_present(dir: &Path, tablet: u64) -> bool {
    files_in(dir).contains(&animus_cp_data::wal_file(tablet))
}

/// Whether `tablet`'s own private LSM **engine** files (ADR 0050 rung 1 —
/// distinct from the per-group Raft WAL file [`tablet_wal_present`] checks)
/// exist in `dir`: any file carrying the `{LSM_PREFIX}t{tablet}-` prefix
/// `animusd`'s own `LsmTabletFactory` opens/probes/destroys by (see that
/// impl's doc, `crates/animusd/src/lib.rs`) — the physical artifact issue
/// #722's fix reclaims for a node that missed the whole drop window while
/// offline.
fn tablet_engine_present(dir: &Path, tablet: u64) -> bool {
    let prefix = format!("{}t{tablet}-", animusd::LSM_PREFIX);
    files_in(dir).iter().any(|f| f.starts_with(&prefix))
}

/// Whether the replicated metadata (as node `n` sees it) has any tablet scoped
/// to `table`.
fn has_table_tablet(node: &Node, table: &str) -> bool {
    node.metadata()
        .tablets
        .values()
        .any(|t| t.table.as_deref() == Some(table))
}

/// Poll until `cond` holds, panicking with `what` after `secs` seconds.
async fn await_true<F: Fn() -> bool>(secs: u64, what: &str, cond: F) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while !cond() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        sleep(Duration::from_millis(100)).await;
    }
}

/// Single node, LSM backend: write a table (auto-provisions its tablet on the
/// node's one shared engine), split it (the child shares the *same* engine,
/// scoped to its own narrower range, but gets its own WAL file), then DROP the
/// table and watch it get reclaimed: both tablets out of the map, both WAL
/// files deleted, no hosted CP groups left. A restart must not resurrect
/// anything, and the node must keep serving a freshly created table afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn dropped_table_data_is_reclaimed_including_split_child() {
    timeout(Duration::from_secs(120), async {
        let tmp = support::panic_safe_tempdir();
        let (nodes, config, dirs) = bring_up(1, tmp.path()).await;
        await_bootstrap(&nodes).await;
        let client = nodes[0].client_addr();
        let admin_addr = nodes[0].admin_addr();
        let raftkv_dir = dirs[0].join("internal");

        // Write keys across the ring; the first write auto-provisions tablet 1
        // for table `kv`.
        for k in [b"a".as_slice(), b"g", b"m", b"s", b"z"] {
            client_put(client, "kv", k, b"v").await;
        }
        await_true(10, "tablet for `kv` provisioned", || {
            has_table_tablet(&nodes[0], "kv")
        })
        .await;
        await_true(10, "tablet 1's WAL file exists", || {
            tablet_wal_present(&raftkv_dir, 1)
        })
        .await;

        // Split tablet 1 at "m": a single atomic control-plane command mints
        // tablet 2 covering the upper range, served by the *same* shared
        // engine, with its own WAL file.
        let (s, body) = admin(
            admin_addr,
            "POST",
            "/admin/tablet/split",
            Some(r#"{"tablet":1,"split_key":"m"}"#),
        )
        .await;
        assert_eq!(s, 200, "split trigger: {body}");
        await_true(20, "split child hosted with its own WAL file", || {
            nodes[0].metadata().tablets.len() >= 2 && tablet_wal_present(&raftkv_dir, 2)
        })
        .await;

        // DROP the table via the admin sink.
        let (s, body) = admin(
            admin_addr,
            "POST",
            "/admin/data/drop-table",
            Some(r#"{"table":"kv"}"#),
        )
        .await;
        assert_eq!(s, 200, "drop-table: {body}");

        // Tablets leave the replicated map, and the GC loop reclaims both
        // groups' WAL files.
        await_true(30, "tablets dropped from the map", || {
            !has_table_tablet(&nodes[0], "kv") && nodes[0].metadata().tablets.is_empty()
        })
        .await;
        await_true(30, "tablet 1's WAL file reclaimed", || {
            !tablet_wal_present(&raftkv_dir, 1)
        })
        .await;
        await_true(30, "split child's WAL file reclaimed", || {
            !tablet_wal_present(&raftkv_dir, 2)
        })
        .await;
        // The admin view lists no hosted groups anymore (poll: the per-tablet
        // teardowns finish on their own GC ticks).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (s, view) = admin(admin_addr, "GET", "/admin/raftkv", None).await;
            assert_eq!(s, 200);
            if view["groups"].as_array().is_some_and(Vec::is_empty) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "hosted CP groups remain after GC: {view}"
            );
            sleep(Duration::from_millis(100)).await;
        }

        // Restart on the same dir + addresses: nothing resurrects…
        nodes[0].shutdown_graceful().await;
        let node =
            support::restart_same_addrs(&config, 0, &dirs[0], animusd::StorageBackend::default())
                .await;
        await_bootstrap(std::slice::from_ref(&node)).await;
        // The restarted control replica re-applies its recovered log from the
        // start, so the tablet map transiently passes through **historical**
        // states in which the dropped tablet still exists — the join-host loop
        // may briefly re-host an empty group for it, which the GC loop then
        // reclaims once replay reaches the committed drop (convergent by
        // design, ADR 0024). So: wait for replay to complete (everything
        // committed is applied), then poll disk + map to their converged
        // state — never one-shot-assert an eventual property.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let (s, raft) = admin(admin_addr, "GET", "/admin/raft", None).await;
            assert_eq!(s, 200);
            let applied = raft["last_applied"].as_u64().unwrap_or(0);
            let commit = raft["commit_index"].as_u64().unwrap_or(u64::MAX);
            let full_log = raft["snapshot_index"].as_u64().unwrap_or(0)
                + raft["log_len"].as_u64().unwrap_or(0);
            if raft["is_leader"] == Value::Bool(true) && applied == commit && commit >= full_log {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "control replay did not complete: {raft}"
            );
            sleep(Duration::from_millis(100)).await;
        }
        await_true(20, "no tablet resurrects after restart", || {
            !has_table_tablet(&node, "kv")
        })
        .await;
        await_true(20, "WAL files stay reclaimed after restart", || {
            !tablet_wal_present(&raftkv_dir, 1) && !tablet_wal_present(&raftkv_dir, 2)
        })
        .await;

        // …and the node keeps serving: a fresh table provisions a fresh tablet
        // (ids are never reused) and reads back.
        let client = node.client_addr();
        client_put(client, "kv2", b"new-key", b"new-val").await;
        assert_eq!(
            client_get(client, "kv2", b"new-key").await,
            Some(b"new-val".to_vec()),
            "a table created after the drop serves reads"
        );

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// Three nodes, one process each (separate edge states — the real deployment
/// shape): the dropped table's tablet is replicated on all three, and **every
/// replica's** GC loop must delete its own WAL file, driven purely off the
/// replicated map (no cross-node teardown message exists).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn every_replica_reclaims_a_dropped_tables_files() {
    timeout(Duration::from_secs(120), async {
        let tmp = support::panic_safe_tempdir();
        let (nodes, _config, dirs) = bring_up(3, tmp.path()).await;
        await_bootstrap(&nodes).await;

        client_put(nodes[0].client_addr(), "orders", b"k1", b"v1").await;
        await_true(10, "tablet for `orders` provisioned", || {
            has_table_tablet(&nodes[0], "orders")
        })
        .await;
        let tablet = nodes[0]
            .metadata()
            .tablets
            .iter()
            .find(|(_, t)| t.table.as_deref() == Some("orders"))
            .map(|(id, _)| id.0)
            .expect("orders tablet exists");
        // Every replica hosts the tablet's group, with its own WAL file.
        for dir in &dirs {
            await_true(20, "replica hosts the tablet's WAL file", || {
                tablet_wal_present(&dir.join("internal"), tablet)
            })
            .await;
        }

        let (s, body) = admin(
            nodes[0].admin_addr(),
            "POST",
            "/admin/data/drop-table",
            Some(r#"{"table":"orders"}"#),
        )
        .await;
        assert_eq!(s, 200, "drop-table: {body}");

        // The drop replicates; each node's own GC loop deletes its local WAL file.
        for node in &nodes {
            await_true(30, "drop visible on every replica", || {
                !has_table_tablet(node, "orders")
            })
            .await;
        }
        for dir in &dirs {
            await_true(30, "every replica reclaims its WAL file", || {
                !tablet_wal_present(&dir.join("internal"), tablet)
            })
            .await;
        }

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}

/// Real-disk regression for issue #722 (ADR 0061 rung D4 PR 3's own
/// finding, closed by the sibling `animus-cp-data`/`animusd` PR this file's
/// change lands alongside): a node stopped BEFORE a table drop is issued,
/// and restarted only AFTER the drop has already fully converged on every
/// other replica, must still reclaim its own leftover per-tablet **engine**
/// files — not merely the metadata/hosted-set facts, which purge/re-derive
/// on their own regardless (see `crates/animus-cp-data/src/host.rs`'s own
/// `EngineFactory::local_tablets`/`Reconciler::tick` docs for the full
/// mechanism, and `crates/animusd/src/sim_cluster_dynamo_drop_table.rs`'s
/// `scenario_4_a_node_crashed_during_the_drop_and_restarted_reclaims_its_
/// engine` for the identical scenario over the in-memory `SimCluster`
/// tier). This is the real `LsmTabletFactory` — its `local_tablets`
/// enumerates this exact on-disk directory by listing it and parsing each
/// file's own `db-t{tablet}-` prefix, the identical mechanism `probe`/
/// `destroy` already use.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_node_stopped_before_the_drop_and_restarted_after_reclaims_its_leftover_engine() {
    timeout(Duration::from_secs(120), async {
        let tmp = support::panic_safe_tempdir();
        let (mut nodes, config, dirs) = bring_up(3, tmp.path()).await;
        await_bootstrap(&nodes).await;

        client_put(nodes[0].client_addr(), "ledger", b"k1", b"v1").await;
        await_true(10, "tablet for `ledger` provisioned", || {
            has_table_tablet(&nodes[0], "ledger")
        })
        .await;
        let tablet = nodes[0]
            .metadata()
            .tablets
            .iter()
            .find(|(_, t)| t.table.as_deref() == Some("ledger"))
            .map(|(id, _)| id.0)
            .expect("ledger tablet exists");

        // Every replica genuinely hosts real, on-disk engine content before
        // anything stops.
        for dir in &dirs {
            await_true(20, "replica hosts the tablet's own engine files", || {
                tablet_engine_present(&dir.join("internal"), tablet)
            })
            .await;
        }

        // Stop node 2's whole process (not a restart yet — see below) BEFORE
        // the drop. Nothing touches its on-disk engine files while it's down.
        nodes[2].shutdown_graceful().await;

        // Drop the table from a still-live node, and let it fully converge
        // on the two still-live replicas before node 2 ever comes back — the
        // realistic "restart once the dust has settled" shape, not an
        // artificially narrow race window.
        let (s, body) = admin(
            nodes[0].admin_addr(),
            "POST",
            "/admin/data/drop-table",
            Some(r#"{"table":"ledger"}"#),
        )
        .await;
        assert_eq!(s, 200, "drop-table: {body}");

        for (i, node) in nodes.iter().enumerate() {
            if i == 2 {
                continue;
            }
            await_true(30, "drop visible on this live replica", || {
                !has_table_tablet(node, "ledger")
            })
            .await;
        }
        for (i, dir) in dirs.iter().enumerate() {
            if i == 2 {
                continue;
            }
            await_true(30, "live replica reclaims its own engine files", || {
                !tablet_engine_present(&dir.join("internal"), tablet)
            })
            .await;
        }

        // Node 2's own leftover engine files are still genuinely present —
        // real data, not an already-empty engine that would trivially
        // "converge" either way — it never saw the drop, and nothing
        // touched its disk while it was down.
        assert!(
            tablet_engine_present(&dirs[2].join("internal"), tablet),
            "node 2's own engine files must still be present before it restarts"
        );

        // Restart node 2 on the SAME dir/addresses — a real process restart.
        // This is the exact window issue #722 is about: node 2's own first
        // reconciler tick since restart already sees the fully-converged,
        // table-absent replicated metadata.
        let node2 =
            support::restart_same_addrs(&config, 2, &dirs[2], animusd::StorageBackend::default())
                .await;
        nodes[2] = node2;
        await_bootstrap(&nodes).await;

        await_true(
            30,
            "the restarted node reclaims its own leftover engine files",
            || !tablet_engine_present(&dirs[2].join("internal"), tablet),
        )
        .await;

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}
