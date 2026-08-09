//! Tablet **merge** end-to-end over `ProdEnv` (ADR 0033): split a table's
//! tablet, write data on both sides, merge the two halves back together, and
//! confirm the merged tablet serves ALL the data while the absorbed
//! (merged-away) tablet's group is torn down on every replica **without its
//! data being erased** — the dual of `drop_table_gc.rs`'s split+drop test.
//!
//! Real time + sockets, so it polls with generous timeouts (the documented
//! `ProdEnv` discipline: converged-or-timeout, never a fixed sleep).

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
                role: animusd::config::NodeRole::Both,
                control: Some(addrs[6 * i]),
                client: addrs[6 * i + 1],
                dynamo: addrs[6 * i + 2],
                cql: addrs[6 * i + 3],
                raftkv: Some(addrs[6 * i + 4]),
                admin: addrs[6 * i + 5],
            })
            .collect();
        let config = animusd::ClusterConfig { nodes: nodes_cfg };
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

/// Put `key = value` into `table` through a node's client port, asserting ok.
async fn client_put(addr: SocketAddr, table: &str, key: &[u8], value: &[u8]) {
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
    assert!(
        matches!(reply, ClientResponse::PutOk),
        "put failed: {reply:?}"
    );
}

/// Linearizable read of `key` from `table` through a node's client port.
async fn client_get(addr: SocketAddr, table: &str, key: &[u8]) -> Option<Vec<u8>> {
    let mut stream = TcpStream::connect(addr).await.expect("connect client");
    write_frame(
        &mut stream,
        &ClientRequest::Get {
            key: key.to_vec(),
            table: table.to_string(),
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
/// exists in `dir` (the node-local artifact torn down whether by `Reclaim`
/// (drop) or `Absorb` (merge, ADR 0033) — only the *data* differs).
fn tablet_wal_present(dir: &Path, tablet: u64) -> bool {
    files_in(dir).contains(&animus_cp_data::wal_file(tablet))
}

/// Whether the replicated metadata (as node `n` sees it) has any tablet scoped
/// to `table`.
fn table_tablets(node: &Node, table: &str) -> Vec<u64> {
    node.metadata()
        .tablets
        .iter()
        .filter(|(_, t)| t.table.as_deref() == Some(table))
        .map(|(id, _)| id.0)
        .collect()
}

/// Poll until `cond` holds, panicking with `what` after `secs` seconds.
async fn await_true<F: Fn() -> bool>(secs: u64, what: &str, cond: F) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while !cond() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        sleep(Duration::from_millis(100)).await;
    }
}

const KEYS: [(&str, &str); 7] = [
    ("a", "v-a"),
    ("b", "v-b"),
    ("g", "v-g"),
    ("m", "v-m"),
    ("s", "v-s"),
    ("y", "v-y"),
    ("z", "v-z"),
];

/// Single node, LSM backend: write a table (auto-provisions its tablet), split
/// it at `m`, write MORE keys into both the lower and upper halves (proving
/// both sides are live and independently writable pre-merge), then merge them
/// back together. The merged tablet must serve **every** key — both halves'
/// pre- and post-split writes — and the absorbed (`right`) tablet's group must
/// be torn down (its WAL file reclaimed, no `/admin/raftkv` entry) while its
/// data is never erased (proven precisely by those reads still succeeding
/// through the *survivor*'s widened scope). A restart must not resurrect the
/// absorbed tablet, and the merged data must still be there afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn merge_serves_all_data_and_reclaims_the_absorbed_wal() {
    timeout(Duration::from_secs(120), async {
        let tmp = tempfile::tempdir().unwrap();
        let (nodes, config, dirs) = bring_up(1, tmp.path()).await;
        await_bootstrap(&nodes).await;
        let client = nodes[0].client_addr();
        let admin_addr = nodes[0].admin_addr();
        let raftkv_dir = dirs[0].join("raftkv");

        // Write across the ring; the first write auto-provisions tablet 1 for
        // table `kv`.
        for (k, v) in [
            ("a", "v-a"),
            ("g", "v-g"),
            ("m", "v-m"),
            ("s", "v-s"),
            ("z", "v-z"),
        ] {
            client_put(client, "kv", k.as_bytes(), v.as_bytes()).await;
        }
        await_true(10, "tablet for `kv` provisioned", || {
            !table_tablets(&nodes[0], "kv").is_empty()
        })
        .await;

        // Split tablet 1 at "m": a single atomic control-plane command mints
        // tablet 2 covering the upper range `[m, ∞)`.
        let (s, body) = admin(
            admin_addr,
            "POST",
            "/admin/tablet/split",
            Some(r#"{"tablet":1,"split_key":"m"}"#),
        )
        .await;
        assert_eq!(s, 200, "split trigger: {body}");
        await_true(20, "split produced two tablets", || {
            table_tablets(&nodes[0], "kv").len() == 2
        })
        .await;
        await_true(20, "split child's WAL file exists", || {
            tablet_wal_present(&raftkv_dir, 2)
        })
        .await;

        // Write MORE keys on both sides post-split, proving both halves are
        // live and independently writable before the merge.
        client_put(client, "kv", b"b", b"v-b").await;
        client_put(client, "kv", b"y", b"v-y").await;
        assert_eq!(client_get(client, "kv", b"b").await, Some(b"v-b".to_vec()));
        assert_eq!(client_get(client, "kv", b"y").await, Some(b"v-y".to_vec()));

        // Merge tablets 1 (left, survives) and 2 (right, absorbed) back
        // together — the dual of split, one atomic control-plane command.
        let (s, body) = admin(
            admin_addr,
            "POST",
            "/admin/tablet/merge",
            Some(r#"{"left":1,"right":2}"#),
        )
        .await;
        assert_eq!(s, 200, "merge trigger: {body}");
        await_true(20, "merge collapsed back to one tablet", || {
            table_tablets(&nodes[0], "kv") == vec![1]
        })
        .await;

        // ALL data — both pre-split values and both post-split additions —
        // must be readable through the merged (single) tablet.
        for (k, v) in KEYS {
            assert_eq!(
                client_get(client, "kv", k.as_bytes()).await,
                Some(v.as_bytes().to_vec()),
                "key {k:?} must survive the merge"
            );
        }

        // The absorbed tablet's group is torn down: its WAL file is reclaimed
        // and it no longer appears in the admin Raft-group view.
        await_true(20, "absorbed tablet's WAL file reclaimed", || {
            !tablet_wal_present(&raftkv_dir, 2)
        })
        .await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (s, view) = admin(admin_addr, "GET", "/admin/raftkv", None).await;
            assert_eq!(s, 200);
            let groups = view["groups"].as_array().cloned().unwrap_or_default();
            let tablets: Vec<u64> = groups.iter().filter_map(|g| g["tablet"].as_u64()).collect();
            if tablets == vec![1] {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "absorbed tablet's group still hosted: {view}"
            );
            sleep(Duration::from_millis(100)).await;
        }

        // Restart on the same dir + addresses: nothing resurrects, and every
        // key merged into tablet 1 is still there. The restarted control
        // replica re-applies its recovered log from the start (transient
        // historical states, ADR 0024/0031 discipline) — wait for replay to
        // complete before asserting the converged state, never a fixed sleep.
        nodes[0].shutdown_graceful().await;
        let node =
            support::restart_same_addrs(&config, 0, &dirs[0], animusd::StorageBackend::default())
                .await;
        await_bootstrap(std::slice::from_ref(&node)).await;
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
        await_true(20, "the absorbed tablet does not resurrect", || {
            table_tablets(&node, "kv") == vec![1]
        })
        .await;
        await_true(
            20,
            "absorbed WAL file stays reclaimed after restart",
            || !tablet_wal_present(&raftkv_dir, 2),
        )
        .await;
        let client = node.client_addr();
        for (k, v) in KEYS {
            assert_eq!(
                client_get(client, "kv", k.as_bytes()).await,
                Some(v.as_bytes().to_vec()),
                "key {k:?} must survive the merge AND the restart"
            );
        }

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// Three nodes, one process each (separate edge states — the real deployment
/// shape): the merge is proposed via node 0 but every replica hosts both
/// tablets' groups, so every replica's own reconciler must widen the
/// survivor's scope and absorb the merged-away sibling — proven by checking
/// EVERY node's `/admin/raftkv` view and reading the merged data back through
/// a **different** node than the one the merge was triggered on.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn every_replica_absorbs_the_merged_away_tablet() {
    timeout(Duration::from_secs(120), async {
        let tmp = tempfile::tempdir().unwrap();
        let (nodes, _config, dirs) = bring_up(3, tmp.path()).await;
        await_bootstrap(&nodes).await;

        client_put(nodes[0].client_addr(), "orders", b"a", b"v-a").await;
        client_put(nodes[0].client_addr(), "orders", b"z", b"v-z").await;
        await_true(10, "tablet for `orders` provisioned", || {
            !table_tablets(&nodes[0], "orders").is_empty()
        })
        .await;
        let left = table_tablets(&nodes[0], "orders")[0];

        let (s, body) = admin(
            nodes[0].admin_addr(),
            "POST",
            "/admin/tablet/split",
            Some(&format!(r#"{{"tablet":{left},"split_key":"m"}}"#)),
        )
        .await;
        assert_eq!(s, 200, "split trigger: {body}");
        await_true(20, "split produced two tablets on every replica", || {
            nodes.iter().all(|n| table_tablets(n, "orders").len() == 2)
        })
        .await;
        let right = *table_tablets(&nodes[0], "orders")
            .iter()
            .find(|&&id| id != left)
            .expect("a second tablet id");
        for dir in &dirs {
            await_true(20, "replica hosts the split child's WAL file", || {
                tablet_wal_present(&dir.join("raftkv"), right)
            })
            .await;
        }
        client_put(nodes[0].client_addr(), "orders", b"y", b"v-y").await;

        let (s, body) = admin(
            nodes[0].admin_addr(),
            "POST",
            "/admin/tablet/merge",
            Some(&format!(r#"{{"left":{left},"right":{right}}}"#)),
        )
        .await;
        assert_eq!(s, 200, "merge trigger: {body}");
        await_true(
            20,
            "merge collapses back to one tablet on every replica",
            || {
                nodes
                    .iter()
                    .all(|n| table_tablets(n, "orders") == vec![left])
            },
        )
        .await;

        // Read the merged data back through node 1 — NOT the node the merge
        // was triggered on — proving every replica actually serves it.
        let other = nodes[1].client_addr();
        for (k, v) in [("a", "v-a"), ("y", "v-y"), ("z", "v-z")] {
            assert_eq!(
                client_get(other, "orders", k.as_bytes()).await,
                Some(v.as_bytes().to_vec()),
                "key {k:?} must be readable through a different replica after merge"
            );
        }

        // Every replica reclaims the absorbed tablet's WAL file and drops it
        // from its own `/admin/raftkv` view.
        for dir in &dirs {
            await_true(30, "every replica reclaims the absorbed WAL file", || {
                !tablet_wal_present(&dir.join("raftkv"), right)
            })
            .await;
        }
        for node in &nodes {
            let admin_addr = node.admin_addr();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
            loop {
                let (s, view) = admin(admin_addr, "GET", "/admin/raftkv", None).await;
                assert_eq!(s, 200);
                let groups = view["groups"].as_array().cloned().unwrap_or_default();
                let tablets: Vec<u64> =
                    groups.iter().filter_map(|g| g["tablet"].as_u64()).collect();
                if tablets == vec![left] {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "absorbed tablet's group still hosted on {admin_addr}: {view}"
                );
                sleep(Duration::from_millis(100)).await;
            }
        }

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}
