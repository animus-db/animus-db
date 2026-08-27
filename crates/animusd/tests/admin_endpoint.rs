//! End-to-end test of the admin / debug HTTP-JSON interface over real TCP
//! (`ProdEnv`, ADR 0020), as an operator or the `animus admin` CLI would hit it.
//!
//! Brings up a 3-node cluster **one process per node** (each its own
//! `ClusterEdgeState`, as a real deployment — so each node's admin views are
//! node-local, not the shared in-process `--cluster` registry). Lets it elect a
//! control leader + bootstrap, writes a key through the client API (forwarded to
//! the CP leader), then exercises the admin routes on the dedicated admin port:
//! the read-only views and an operator action (`storage/flush`, forcing the
//! written key out to an SSTable observed via `storage/lsm`). Real time + sockets,
//! so it polls with generous timeouts.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, Node, read_frame};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// Bring up an `n`-node cluster, one process per node (each its own edge state),
/// retrying the (allocate-fresh-ports + start-all) as a unit. `free_addrs` frees
/// each port before `run_node` rebinds it, so another test binary can steal one in
/// the window (`AddrInUse`); a fresh attempt re-allocates and the started nodes are
/// torn down first (the documented port-TOCTOU mitigation, see the crate guide).
async fn bring_up(n: usize, dir: &std::path::Path) -> (Vec<Node>, animusd::ClusterConfig) {
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
            })
            .collect();
        let config = animusd::ClusterConfig {
            nodes: nodes_cfg,
            dynamo_auth: None,
        };
        let mut nodes = Vec::new();
        let mut failed = false;
        for i in 0..n {
            match animusd::run_node(&config, i, dir.join(format!("node-{attempt}-{i}"))).await {
                Ok(node) => nodes.push(node),
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        if !failed {
            return (nodes, config);
        }
        for node in &nodes {
            node.shutdown_graceful().await;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("could not bring up cluster after retries (ports kept getting stolen)");
}

/// Like [`bring_up`], but pins every node's [`animusd::SplitMode`] explicitly
/// instead of taking whatever `SplitMode::default()` currently is — for
/// `admin_split_kicks_off_the_copy_based_workflow` below, which asserts the
/// ADR 0050 copy-based workflow's own intermediate states (`Splitting` +
/// two `Building` children) and must keep exercising that workflow
/// regardless of which mode is the cluster-wide default (ADR 0058 rung 4
/// layer 2 flipped it to `InPlace`).
async fn bring_up_with_split_mode(
    n: usize,
    dir: &std::path::Path,
    split_mode: animusd::SplitMode,
) -> (Vec<Node>, animusd::ClusterConfig) {
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
            })
            .collect();
        let config = animusd::ClusterConfig {
            nodes: nodes_cfg,
            dynamo_auth: None,
        };
        let mut nodes = Vec::new();
        let mut failed = false;
        for i in 0..n {
            match animusd::run_node_with_streams_quiesce_and_split_mode(
                &config,
                i,
                dir.join(format!("node-{attempt}-{i}")),
                animusd::StorageBackend::default(),
                animus_control::node::DEFAULT_ORPHAN_SWEEP_AFTER,
                animusd::StreamSealKnobs::default(),
                animusd::SegmentStoreConfig::default(),
                animusd::DEFAULT_STREAM_RETENTION,
                Duration::ZERO,
                split_mode,
                animusd::BackupStoreConfig::default(),
            )
            .await
            {
                Ok(node) => nodes.push(node),
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        if !failed {
            return (nodes, config);
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
        "{method} {path} HTTP/1.0\r\n\
         Host: animus\r\n\
         Content-Type: application/json\r\n\
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
    stream.read_to_end(&mut raw).await.expect("read response");
    let text = String::from_utf8(raw).expect("utf8 response");
    let (head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status line");
    assert!(
        head.to_ascii_lowercase()
            .contains("content-type: application/json"),
        "admin response should be application/json, headers:\n{head}"
    );
    let value: Value = serde_json::from_str(payload)
        .unwrap_or_else(|e| panic!("admin body is not JSON ({e}): {payload}"));
    (status, value)
}

async fn admin_get(addr: SocketAddr, path: &str) -> (u16, Value) {
    admin(addr, "GET", path, None).await
}

/// A put with a bounded retry on ANY error reply (mirrors
/// `split_build.rs::put`): a put is idempotent, and early-cluster-formation/
/// split/election transients all surface as a clean, retryable
/// `ClientResponse::Error` — see `docs/engineering-lessons.md`'s "CP
/// write-forward path has no retry-on-not-the-leader-here" and issue #268's
/// fast-futility entries. A bare one-shot assert on the first write right
/// after bootstrap (or racing a split) is a documented latent flake, not a
/// bug this test should paper over.
async fn put(stream: &mut TcpStream, table: &str, key: Vec<u8>, value: Vec<u8>) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        animusd::write_frame(
            stream,
            &ClientRequest::Put {
                key: key.clone(),
                value: value.clone(),
                table: table.to_string(),
            },
        )
        .await
        .expect("send put");
        let reply: ClientResponse = read_frame(stream)
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

/// Encode a query-param value the way the browser's `encodeURIComponent` does
/// (everything but unreserved characters becomes `%NN`), so a test drives the
/// same bytes the dashboard would.
fn percent_encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn admin_interface_surfaces_state_and_actions() {
    timeout(Duration::from_secs(60), async {
        let dir = tempfile::tempdir().unwrap();
        let (nodes, _config) = bring_up(3, dir.path()).await;
        await_bootstrap(&nodes).await;

        // Write a key through the client API (forwarded to the CP leader).
        let mut stream = TcpStream::connect(nodes[0].client_addr())
            .await
            .expect("connect");
        put(
            &mut stream,
            "kv",
            b"admin-key".to_vec(),
            b"admin-val".to_vec(),
        )
        .await;

        let admin_addr = nodes[0].admin_addr();

        // ---- /admin/config -------------------------------------------------
        let (s, config_view) = admin_get(admin_addr, "/admin/config").await;
        assert_eq!(s, 200);
        assert_eq!(
            config_view["node_id"], "n0",
            "node 0's one id (ADR 0040 PR1/PR3)"
        );
        assert_eq!(
            config_view["addrs"]["admin"].as_str(),
            Some(admin_addr.to_string().as_str()),
            "config echoes the admin address: {config_view}"
        );

        // ---- /admin/status -------------------------------------------------
        let (s, status) = admin_get(admin_addr, "/admin/status").await;
        assert_eq!(s, 200);
        assert_eq!(
            status["members"].as_object().map(|m| m.len()),
            Some(3),
            "status reports 3 members: {status}"
        );

        // ---- /admin/raft ---------------------------------------------------
        let (s, raft) = admin_get(admin_addr, "/admin/raft").await;
        assert_eq!(s, 200);
        assert!(raft["term"].as_u64().unwrap() >= 1, "raft term: {raft}");
        assert!(raft["role"].is_string(), "raft role present: {raft}");
        assert_eq!(
            raft["members"].as_array().map(Vec::len),
            Some(3),
            "raft view lists members: {raft}"
        );

        // ---- /admin/raftkv (one group per node in per-process mode) --------
        let (s, raftkv) = admin_get(admin_addr, "/admin/raftkv").await;
        assert_eq!(s, 200);
        assert_eq!(raftkv["hosts_cp"], true, "node 0 hosts the CP group");
        let groups = raftkv["groups"].as_array().expect("groups array");
        assert_eq!(groups.len(), 1, "node-local view: one group: {raftkv}");
        assert_eq!(groups[0]["tablet"], 1, "the bootstrap tablet id");
        assert_eq!(groups[0]["backend"], "lsm", "durable backend");

        // ---- /admin/storage/wal --------------------------------------------
        let (s, wal) = admin_get(admin_addr, "/admin/storage/wal?tablet=1").await;
        assert_eq!(s, 200);
        assert_eq!(wal["backend"], "lsm");
        assert!(
            wal["segments"].as_array().is_some_and(|a| !a.is_empty()),
            "WAL has at least one live segment: {wal}"
        );

        // ---- /admin/storage/control (ADR 0038 PR4) -------------------------
        // A combined node has a `ControlHandle::Local` control role, so its
        // own control-plane system-keyspace engine is available — the exact
        // same physical shared engine `/admin/raftkv`'s hosted tablet above
        // also reports on (`Metadata` just lives at a reserved key prefix
        // within it, ADR 0038), so the backend agrees.
        let (s, ctl_storage) = admin_get(admin_addr, "/admin/storage/control").await;
        assert_eq!(s, 200);
        assert_eq!(
            ctl_storage["available"], true,
            "a combined node has a local control-plane system-keyspace engine: {ctl_storage}"
        );
        assert_eq!(ctl_storage["backend"], "lsm");

        // ---- /admin/metrics ------------------------------------------------
        // Metrics are per-node sinks (a follower's leader-only counters are 0), so
        // every node exposes the full counter set as JSON, and the *control
        // leader's* node reports the election counters non-zero + is_leader 1.
        let (s, metrics) = admin_get(admin_addr, "/admin/metrics").await;
        assert_eq!(s, 200);
        assert!(
            metrics["counters"]["control_elections_started"].is_u64(),
            "metrics JSON exposes the full counter set: {metrics}"
        );
        let leader_idx = nodes
            .iter()
            .position(Node::is_control_leader)
            .expect("a control leader exists after bootstrap");
        let (s, lmetrics) = admin_get(nodes[leader_idx].admin_addr(), "/admin/metrics").await;
        assert_eq!(s, 200);
        assert_eq!(lmetrics["is_leader"], 1, "leader node reports is_leader 1");
        assert!(
            lmetrics["counters"]["control_elections_won"]
                .as_u64()
                .unwrap()
                >= 1,
            "control leader won an election: {lmetrics}"
        );

        // ---- /admin/metrics/history -----------------------------------------
        // The sampler ticks every 10s (METRICS_SAMPLE_INTERVAL); poll rather than
        // sleep a fixed amount, matching this suite's real-time convention.
        let (s, history) = admin_get(admin_addr, "/admin/metrics/history").await;
        assert_eq!(s, 200);
        assert!(
            history["samples"].as_array().is_some(),
            "history is a samples array even before the first tick: {history}"
        );
        let sample = timeout(Duration::from_secs(15), async {
            loop {
                let (_, h) = admin_get(admin_addr, "/admin/metrics/history").await;
                if let Some(first) = h["samples"].as_array().and_then(|a| a.first()) {
                    return first.clone();
                }
                sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .expect("a metrics-history sample appears within one sample interval");
        assert!(sample["ts_ms"].as_u64().is_some_and(|t| t > 0), "{sample}");
        assert!(
            sample["counters"]["control_elections_started"].is_u64(),
            "a sample carries the same counter set as /admin/metrics: {sample}"
        );

        // ---- /admin/health -------------------------------------------------
        let (s, health) = admin_get(admin_addr, "/admin/health").await;
        assert_eq!(s, 200, "health 200 once a leader is known");
        assert_eq!(health["ok"], true);

        // ---- unknown route + malformed body --------------------------------
        let (s, _) = admin_get(admin_addr, "/admin/nope").await;
        assert_eq!(s, 404, "unknown admin route is 404");
        let (s, _) = admin(admin_addr, "POST", "/admin/storage/flush", Some("not json")).await;
        assert_eq!(s, 400, "malformed JSON body is 400");

        // ---- action: flush on the CP leader's node, observe the SSTable.
        // The leader has the put applied for certain, so the forced flush has data.
        let mut leader_admin = None;
        for node in &nodes {
            let (_, rk) = admin_get(node.admin_addr(), "/admin/raftkv").await;
            if rk["groups"][0]["is_leader"].as_bool() == Some(true) {
                leader_admin = Some(node.admin_addr());
                break;
            }
        }
        let leader_admin = leader_admin.expect("a CP group leader exists");

        let (s, flushed) = admin(
            leader_admin,
            "POST",
            "/admin/storage/flush",
            Some("{\"tablet\":1}"),
        )
        .await;
        assert_eq!(s, 200, "flush action returns 200: {flushed}");
        assert_eq!(flushed["flushed"], true, "flush ran: {flushed}");

        let (s, lsm) = admin_get(leader_admin, "/admin/storage/lsm?tablet=1").await;
        assert_eq!(s, 200);
        assert_eq!(lsm["backend"], "lsm");
        assert!(
            lsm["sstables"].as_array().is_some_and(|a| !a.is_empty()),
            "the forced flush produced at least one SSTable: {lsm}"
        );

        // ---- /admin/storage/scan (browse keys) — the written key is listed -----
        let (s, scan) = admin_get(leader_admin, "/admin/storage/scan?tablet=1&limit=10").await;
        assert_eq!(s, 200, "scan returns 200: {scan}");
        let items = scan["items"].as_array().expect("scan items array");
        assert!(
            items
                .iter()
                .any(|it| { it["key"] == "admin-key" && it["value"] == "admin-val" }),
            "browse-keys lists the written pair: {scan}"
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}

/// The dashboard's write proxy (ADR 0021): run a DynamoDB CRUD round-trip
/// through the admin port, asserting data flows back.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn admin_data_write_dynamo() {
    timeout(Duration::from_secs(60), async {
        let dir = tempfile::tempdir().unwrap();
        let (nodes, _config) = bring_up(3, dir.path()).await;
        await_bootstrap(&nodes).await;
        let a = nodes[0].admin_addr();

        // ---- DynamoDB: PutItem then GetItem via /admin/data/dynamo ----------
        let (s, put) = admin(
            a,
            "POST",
            "/admin/data/dynamo",
            Some(r#"{"op":"PutItem","payload":{"TableName":"t","Item":{"pk":{"S":"alice"},"v":{"N":"7"}}}}"#),
        )
        .await;
        assert_eq!(s, 200, "PutItem via admin proxy: {put}");

        let (s, got) = admin(
            a,
            "POST",
            "/admin/data/dynamo",
            // `ConsistentRead: true` (ADR 0055): this reads back a write it
            // just made, and the wire default is now a genuinely
            // eventually-consistent read that may not reflect it yet.
            Some(
                r#"{"op":"GetItem","payload":{"TableName":"t",
                    "Key":{"pk":{"S":"alice"}},"ConsistentRead":true}}"#,
            ),
        )
        .await;
        assert_eq!(s, 200, "GetItem via admin proxy: {got}");
        assert_eq!(
            got["Item"]["v"]["N"].as_str(),
            Some("7"),
            "GetItem reads back the written value: {got}"
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}

/// Table management (ADR 0021): a DynamoDB `CreateTable` via the write proxy shows
/// up in the replicated catalog (`/admin/status`), then `/admin/data/drop-table`
/// removes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn admin_table_management_create_and_drop() {
    timeout(Duration::from_secs(60), async {
        let dir = tempfile::tempdir().unwrap();
        let (nodes, _config) = bring_up(3, dir.path()).await;
        await_bootstrap(&nodes).await;
        let a = nodes[0].admin_addr();

        let has_widgets = || async {
            let (_, status) = admin_get(a, "/admin/status").await;
            status["schemas"]["tables"]
                .get("widgets")
                .is_some_and(|v| !v.is_null())
        };

        // Create a composite table (string partition key + **numeric** sort key).
        let (s, body) = admin(
            a,
            "POST",
            "/admin/data/dynamo",
            Some(
                r#"{"op":"CreateTable","payload":{"TableName":"widgets","KeySchema":[{"AttributeName":"id","KeyType":"HASH"},{"AttributeName":"seq","KeyType":"RANGE"}],"AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"},{"AttributeName":"seq","AttributeType":"N"}]}}"#,
            ),
        )
        .await;
        assert_eq!(s, 200, "CreateTable via admin proxy: {body}");
        timeout(Duration::from_secs(10), async {
            while !has_widgets().await {
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("created table did not appear in the catalog");

        // The catalog records the key columns with their declared types — this is the
        // contract the dashboard's key prefill reads (partition key + sort key, typed).
        let (_, status) = admin_get(a, "/admin/status").await;
        let schema = &status["schemas"]["tables"]["widgets"];
        assert_eq!(schema["partition_key"], "id", "partition key recorded: {schema}");
        assert_eq!(schema["clustering_keys"][0], "seq", "sort key recorded: {schema}");
        let seq_ty = schema["columns"]
            .as_array()
            .and_then(|cols| cols.iter().find(|c| c["name"] == "seq"))
            .map(|c| c["ty"].clone());
        assert_eq!(
            seq_ty,
            Some(serde_json::json!("Number")),
            "the numeric sort key's type reaches the catalog (not defaulted to String): {schema}"
        );

        // Drop.
        let (s, body) =
            admin(a, "POST", "/admin/data/drop-table", Some(r#"{"table":"widgets"}"#)).await;
        assert_eq!(s, 200, "drop-table: {body}");
        timeout(Duration::from_secs(10), async {
            while has_widgets().await {
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("dropped table still in the catalog");

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}

/// `GET /admin/backups` (ADR 0059 §3): a pure observer over the replicated
/// backup catalog. No wire API/capture driver exists yet in this train, so
/// this drives the catalog directly via `Node::propose_meta` (mirroring
/// `system_table.rs`'s own harness-level `MetaCommand` proposals) — a
/// real-cluster proof that the admin view reflects `Metadata::backups`/
/// `backup_tablet_progress` end to end, including the status transition to
/// `AVAILABLE`.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn admin_backups_view_reflects_the_catalog() {
    timeout(Duration::from_secs(60), async {
        let dir = tempfile::tempdir().unwrap();
        let (nodes, _config) = bring_up(3, dir.path()).await;
        await_bootstrap(&nodes).await;
        let a = nodes[0].admin_addr();

        // A real table (also provisions its first tablet, via the ordinary
        // DynamoDB `CreateTable` path).
        let (s, body) = admin(
            a,
            "POST",
            "/admin/data/dynamo",
            Some(
                r#"{"op":"CreateTable","payload":{"TableName":"widgets","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],"AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]}}"#,
            ),
        )
        .await;
        assert_eq!(s, 200, "CreateTable via admin proxy: {body}");
        timeout(Duration::from_secs(10), async {
            loop {
                let (_, status) = admin_get(a, "/admin/status").await;
                if status["schemas"]["tables"]
                    .get("widgets")
                    .is_some_and(|v| !v.is_null())
                {
                    return;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("table did not appear in the catalog");

        // Starts empty.
        let (s, empty) = admin_get(a, "/admin/backups").await;
        assert_eq!(s, 200);
        assert_eq!(
            empty["backups"].as_array().unwrap().len(),
            0,
            "no backups yet: {empty}"
        );

        // `BeginBackup`, proposed directly (no wire API in this train yet) —
        // retried across nodes since only the control leader accepts it.
        let begin = animus_control::MetaCommand::BeginBackup {
            backup_id: "backup-1".to_string(),
            table: "widgets".to_string(),
            created_wall_ms: 1_000,
        };
        timeout(Duration::from_secs(10), async {
            loop {
                if nodes.iter().any(|n| n.propose_meta(begin.clone())) {
                    return;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("BeginBackup never accepted by a leader");

        let backups = timeout(Duration::from_secs(10), async {
            loop {
                let (s, body) = admin_get(a, "/admin/backups").await;
                assert_eq!(s, 200);
                let arr = body["backups"].as_array().cloned().unwrap_or_default();
                if arr.iter().any(|b| b["backup_id"] == "backup-1") {
                    return body;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("backup row did not appear in /admin/backups");

        let tablet_ids: Vec<u64> = {
            let row = backups["backups"]
                .as_array()
                .unwrap()
                .iter()
                .find(|b| b["backup_id"] == "backup-1")
                .expect("backup-1 present");
            assert_eq!(row["table"], "widgets");
            assert_eq!(row["status"]["state"], "CREATING");
            assert_eq!(row["created_wall_ms"], 1000);
            let tablets = row["tablets"].as_array().expect("tablets array");
            assert!(!tablets.is_empty(), "at least one pinned tablet: {row}");
            assert!(
                tablets.iter().all(|t| t["reported"] == false),
                "nothing reported yet: {row}"
            );
            tablets
                .iter()
                .map(|t| t["tablet"].as_str().unwrap().parse().unwrap())
                .collect()
        };

        // Report every pinned tablet complete, then complete the backup —
        // proposed on the same leader in order, so Raft's log order alone
        // guarantees `CompleteBackup` applies after every report.
        for tablet in &tablet_ids {
            let record = animus_control::MetaCommand::RecordBackupTabletComplete {
                backup_id: "backup-1".to_string(),
                tablet: animus_tablet::TabletId(*tablet),
                cut_version: 42,
                bytes: 4_096,
            };
            assert!(
                nodes.iter().any(|n| n.propose_meta(record.clone())),
                "RecordBackupTabletComplete never accepted by a leader"
            );
        }
        let complete = animus_control::MetaCommand::CompleteBackup {
            backup_id: "backup-1".to_string(),
        };
        assert!(
            nodes.iter().any(|n| n.propose_meta(complete.clone())),
            "CompleteBackup never accepted by a leader"
        );

        let expected_total = 4_096 * tablet_ids.len() as u64;
        timeout(Duration::from_secs(10), async {
            loop {
                let (_, body) = admin_get(a, "/admin/backups").await;
                let row = body["backups"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|b| b["backup_id"] == "backup-1")
                    .cloned();
                if let Some(row) = row
                    && row["status"]["state"] == "AVAILABLE"
                {
                    assert_eq!(row["total_bytes"].as_u64().unwrap(), expected_total);
                    assert!(row["tablets"].as_array().unwrap().iter().all(|t| t["reported"] == true));
                    return;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("backup never reached AVAILABLE in /admin/backups");

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}

/// The CP data plane's per-tablet Raft group must hold **stable leadership under
/// sustained write load** — the driver-liveness guarantee (ADR 0017). Before engine
/// apply + compaction were moved off the consensus loop, a bulk seed blocked the
/// single driver task for ~180-300ms per batch (LSM merges + compaction), past the
/// 150ms election timeout, so followers repeatedly timed out and campaigned: the CP
/// term climbed ~8-18 per 2000-key seed (a leader-election storm that truncated
/// in-flight writes and collapsed throughput to ~15/s). With apply/compaction on a
/// separate task, the leader keeps heartbeating and the term stays flat.
///
/// This is a **real-time `ProdEnv` liveness assertion** — the class `SimEnv` cannot
/// catch (virtual time never trips the wall-clock election timeout). We seed 2000
/// keys through the CP leader and require the group's term to barely move.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn seed_load_does_not_storm_cp_elections() {
    // The CP term may legitimately advance a little (an initial election retry, a
    // stray heartbeat miss under CI load); the storm this guards against moved it by
    // 8-18 in a single seed. A generous bound stays non-flaky while still failing
    // loudly if the storm returns.
    const MAX_TERM_DELTA: u64 = 3;

    timeout(Duration::from_secs(90), async {
        let dir = tempfile::tempdir().unwrap();
        let (nodes, _config) = bring_up(3, dir.path()).await;
        await_bootstrap(&nodes).await;
        let (s, _ct) = admin(
            nodes[0].admin_addr(),
            "POST",
            "/admin/data/dynamo",
            Some(
                r#"{"op":"CreateTable","payload":{"TableName":"seedt",
                    "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
                    "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]}}"#,
            ),
        )
        .await;
        assert_eq!(s, 200, "CreateTable seedt");

        // Poll until the bootstrap CP group has a leader; return (node index, term).
        async fn cp_leader(nodes: &[Node]) -> (usize, u64) {
            for _ in 0..100 {
                for (i, node) in nodes.iter().enumerate() {
                    let (_, rk) = admin_get(node.admin_addr(), "/admin/raftkv").await;
                    if let Some(groups) = rk["groups"].as_array() {
                        for g in groups {
                            if g["is_leader"].as_bool() == Some(true) {
                                return (i, g["term"].as_u64().unwrap_or(0));
                            }
                        }
                    }
                }
                sleep(Duration::from_millis(100)).await;
            }
            panic!("CP group never elected a leader");
        }

        let (leader_idx, term_before) = cp_leader(&nodes).await;
        let started = std::time::Instant::now();
        let (s, body) = admin(
            nodes[leader_idx].admin_addr(),
            "POST",
            "/admin/data/seed",
            Some(r#"{"table":"seedt","count":2000,"key_prefix":"seed:","value_bytes":64}"#),
        )
        .await;
        assert_eq!(s, 200, "seed returns 200: {body}");
        assert_eq!(
            body["written"], 2000,
            "seed wrote all requested keys: {body}"
        );

        let (_, term_after) = cp_leader(&nodes).await;
        let delta = term_after.saturating_sub(term_before);
        let rate = 2000.0 / started.elapsed().as_secs_f64();
        eprintln!("seed 2000 keys: {rate:.0}/s, CP term {term_before} -> {term_after} (Δ{delta})");
        assert!(
            delta <= MAX_TERM_DELTA,
            "CP leadership stormed under seed load: term moved {term_before} -> {term_after} \
             (Δ{delta} > {MAX_TERM_DELTA}) — apply/compaction is likely blocking the driver \
             loop past the election timeout again"
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("seed-load election-stability test timed out");
}

/// The bulk-seed endpoint (ADR 0021) writes the requested number of synthetic
/// **DynamoDB items** to the CP plane: they land durably (visible in the CP
/// leader's storage) and read back through the DynamoDB edge by their catalog
/// key attributes (`GetItem`), for simple and composite tables alike.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn admin_seed_writes_synthetic_keys() {
    timeout(Duration::from_secs(60), async {
        let dir = tempfile::tempdir().unwrap();
        let (nodes, _config) = bring_up(3, dir.path()).await;
        await_bootstrap(&nodes).await;
        let a = nodes[0].admin_addr();

        // Seeding writes into an **existing** table (ADR 0023) — create it first.
        let (s, ct) = admin(
            a,
            "POST",
            "/admin/data/dynamo",
            Some(
                r#"{"op":"CreateTable","payload":{"TableName":"seedt",
                    "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
                    "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]}}"#,
            ),
        )
        .await;
        assert_eq!(s, 200, "CreateTable seedt: {ct}");

        // Seeding a table that does not exist is a 404 (no implicit create).
        let (s, missing) = admin(
            a,
            "POST",
            "/admin/data/seed",
            Some(r#"{"table":"nope","count":1}"#),
        )
        .await;
        assert_eq!(
            s, 404,
            "seeding a non-existent table is rejected: {missing}"
        );

        let (s, body) = admin(
            a,
            "POST",
            "/admin/data/seed",
            Some(r#"{"table":"seedt","count":60,"key_prefix":"seed:","value_bytes":8}"#),
        )
        .await;
        assert_eq!(s, 200, "seed returns 200: {body}");
        assert_eq!(body["written"], 60, "seed wrote all requested keys: {body}");

        // The seeded keys are durably in the CP leader's local storage.
        let mut leader_admin = None;
        for node in &nodes {
            let (_, rk) = admin_get(node.admin_addr(), "/admin/raftkv").await;
            if rk["groups"][0]["is_leader"].as_bool() == Some(true) {
                leader_admin = Some(node.admin_addr());
                break;
            }
        }
        let leader_admin = leader_admin.expect("a CP group leader exists");
        let (s, scan) = admin_get(leader_admin, "/admin/storage/scan?tablet=1&limit=200").await;
        assert_eq!(s, 200);
        // A seeded key is token-prefixed (ADR 0022: `partition_token || escape(pk)`),
        // so the readable pk follows 8 hash bytes — `contains`, not `starts_with`.
        let seeded = scan["items"]
            .as_array()
            .map(|items| {
                items
                    .iter()
                    .filter(|it| it["key"].as_str().is_some_and(|k| k.contains("seed:")))
                    .count()
            })
            .unwrap_or(0);
        assert!(
            seeded >= 60,
            "all seeded keys are in the leader's storage: {scan}"
        );

        // A seeded row is a real DynamoDB item — the exact key/value bytes
        // `PutItem` would store — so it reads back through the DynamoDB edge by
        // its catalog key attribute (`id`, not the legacy `pk`), filler
        // `payload` included.
        let (s, got) = admin(
            a,
            "POST",
            "/admin/data/dynamo",
            Some(
                // `ConsistentRead: true` (ADR 0055), like the composite-table
                // read below: this reads back the seeder's own writes.
                r#"{"op":"GetItem","payload":{"TableName":"seedt",
                    "Key":{"id":{"S":"seed:000000000007"}},"ConsistentRead":true}}"#,
            ),
        )
        .await;
        assert_eq!(s, 200, "GetItem on a seeded row: {got}");
        assert_eq!(
            got["Item"]["id"]["S"], "seed:000000000007",
            "seeded item carries its schema partition key: {got}"
        );
        assert!(
            got["Item"]["payload"]["S"].is_string(),
            "seeded item carries the filler payload attribute: {got}"
        );

        // A composite table seeds items with **both** key attributes (the sort
        // key gets the same zero-padded index), addressable by the full key.
        let (s, ct) = admin(
            a,
            "POST",
            "/admin/data/dynamo",
            Some(
                r#"{"op":"CreateTable","payload":{"TableName":"seedc",
                    "KeySchema":[{"AttributeName":"id","KeyType":"HASH"},
                                 {"AttributeName":"rk","KeyType":"RANGE"}],
                    "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"},
                                            {"AttributeName":"rk","AttributeType":"S"}]}}"#,
            ),
        )
        .await;
        assert_eq!(s, 200, "CreateTable seedc: {ct}");
        let (s, body) = admin(
            a,
            "POST",
            "/admin/data/seed",
            Some(r#"{"table":"seedc","count":5,"key_prefix":"seed:","value_bytes":32}"#),
        )
        .await;
        assert_eq!(s, 200, "seed seedc returns 200: {body}");
        assert_eq!(body["written"], 5, "seed wrote all requested keys: {body}");
        let (s, got) = admin(
            a,
            "POST",
            "/admin/data/dynamo",
            Some(
                // `ConsistentRead: true` (ADR 0055): reads back the seeder's
                // own writes.
                r#"{"op":"GetItem","payload":{"TableName":"seedc",
                    "Key":{"id":{"S":"seed:000000000003"},"rk":{"S":"000000000003"}},
                    "ConsistentRead":true}}"#,
            ),
        )
        .await;
        assert_eq!(s, 200, "GetItem on a seeded composite row: {got}");
        assert_eq!(
            got["Item"]["rk"]["S"], "000000000003",
            "seeded composite item carries its sort key: {got}"
        );

        // A displayed key (`<token-base64>:<pk>`) round-trips through the
        // inspector URL exactly as the dashboard sends it (percent-encoded):
        // the server must reverse the display back to the raw token-prefixed
        // key, or `live` comes back null.
        let shown = scan["items"]
            .as_array()
            .and_then(|items| {
                items
                    .iter()
                    .find_map(|it| it["key"].as_str().filter(|k| k.contains("seed:")))
            })
            .expect("a seeded key is listed")
            .to_owned();
        let (s, inspect) = admin_get(
            leader_admin,
            &format!("/admin/storage/key?tablet=1&key={}", percent_encode(&shown)),
        )
        .await;
        assert_eq!(s, 200);
        assert!(
            inspect["live"].is_string(),
            "displayed key `{shown}` resolves to its live value: {inspect}"
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}

/// Regression (2026-08-19): `/admin/raftkv` is **polled** — the Console
/// fetches it from every node on its auto-refresh interval (5s by default) —
/// so its default response must not materialize every hosted tablet's rows.
/// It used to: `key_count`/`byte_size` came from `local_pairs()`, an
/// O(dataset) scan per hosted group per request. Measured on a live
/// 20,000-row cluster, polling this route every 3s inflated a split's own
/// build from 4.5s to 41.8s (~9x) — an observer that materially perturbs
/// what it observes.
///
/// The teeth use the LSM's own `storage_sstable_block_reads` counter as the
/// cost meter rather than wall-clock, which would be flaky under CI
/// contention: a materializing read of flushed data must read SSTable
/// blocks, and a metadata-only estimate must not. Both windows are the same
/// shape and duration, so whatever background work the node's own loops do
/// lands in both and cancels out of the comparison; only the route's own
/// cost differs.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn admin_raftkv_default_does_not_materialize_the_dataset() {
    /// Enough rows that one materializing scan is unmistakable in block
    /// reads, few enough that seeding stays quick.
    const N: usize = 2_000;
    /// Polls per window — mirrors an operator leaving the Tablets tab open.
    const POLLS: usize = 10;

    timeout(Duration::from_secs(120), async {
        let dir = tempfile::tempdir().unwrap();
        let (nodes, _config) = bring_up(3, dir.path()).await;
        await_bootstrap(&nodes).await;
        let a = nodes[0].admin_addr();

        let (s, ct) = admin(
            a,
            "POST",
            "/admin/data/dynamo",
            Some(
                r#"{"op":"CreateTable","payload":{"TableName":"big",
                    "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
                    "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]}}"#,
            ),
        )
        .await;
        assert_eq!(s, 200, "CreateTable big: {ct}");
        let (s, seeded) = admin(
            a,
            "POST",
            "/admin/data/seed",
            Some(&format!(
                r#"{{"table":"big","count":{N},"key_prefix":"seed:","value_bytes":64}}"#
            )),
        )
        .await;
        assert_eq!(s, 200, "seed: {seeded}");

        // Flush every hosted group so the rows live in SSTables, not the
        // memtable — otherwise even the exact scan reads no blocks and the
        // meter below cannot tell the two paths apart.
        let (_, rk) = admin_get(a, "/admin/raftkv").await;
        for g in rk["groups"].as_array().cloned().unwrap_or_default() {
            let tablet = g["tablet"].as_u64().expect("group has a tablet id");
            let (s, f) = admin(
                a,
                "POST",
                "/admin/storage/flush",
                Some(&format!(r#"{{"tablet":{tablet}}}"#)),
            )
            .await;
            assert_eq!(s, 200, "flush tablet {tablet}: {f}");
        }

        async fn block_reads(a: SocketAddr) -> u64 {
            let (_, m) = admin_get(a, "/admin/metrics").await;
            m["counters"]["storage_sstable_block_reads"]
                .as_u64()
                .expect("the LSM block-read counter is exported")
        }

        let base = block_reads(a).await;
        for _ in 0..POLLS {
            let (s, _) = admin_get(a, "/admin/raftkv").await;
            assert_eq!(s, 200, "default raftkv poll");
        }
        let after_cheap = block_reads(a).await;
        for _ in 0..POLLS {
            let (s, _) = admin_get(a, "/admin/raftkv?exact=1").await;
            assert_eq!(s, 200, "exact raftkv poll");
        }
        let after_exact = block_reads(a).await;

        let cheap = after_cheap - base;
        let exact = after_exact - after_cheap;
        eprintln!(
            "raftkv cost over {N} flushed rows: {POLLS} default polls = {cheap} SSTable block \
             reads, {POLLS} `?exact=1` polls = {exact}"
        );
        assert!(
            exact > cheap * 4 + 50,
            "the default `/admin/raftkv` must not materialize the dataset: {POLLS} default \
             polls read {cheap} SSTable blocks vs {exact} for the same number of `?exact=1` \
             polls over {N} flushed rows — the default is scanning again"
        );

        // The cheap path still answers, and `?exact=1` still answers exactly.
        let (_, cheap_view) = admin_get(a, "/admin/raftkv").await;
        let (_, exact_view) = admin_get(a, "/admin/raftkv?exact=1").await;
        let sum = |v: &Value| -> u64 {
            v["groups"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .iter()
                .filter_map(|g| g["key_count"].as_u64())
                .sum()
        };
        assert!(
            sum(&cheap_view) > 0,
            "the LSM backend has a cheap key-count estimate: {cheap_view}"
        );
        assert!(
            sum(&exact_view) >= N as u64,
            "`?exact=1` counts every seeded row (plus bookkeeping kinds): {exact_view}"
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}

/// Regression: `/admin/raftkv?exact=1`'s `key_count` must be scoped to each
/// tablet's own range, not the node's combined total. A node hosts more than
/// one tablet as soon as it hosts a split's parent + child (ADR 0028); before
/// the fix, `key_count` read the whole shared engine
/// (`CpGroup::approx_key_count`), so both halves' rows showed the *node's*
/// combined total rather than their own subset.
///
/// Asks for `?exact=1` explicitly: the polled default is now the cheap
/// estimate (see `admin_raftkv_default_does_not_materialize_the_dataset`),
/// and this test's exact-total assertion is precisely what the exact path
/// exists to answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn admin_raftkv_key_count_is_scoped_per_tablet_after_split() {
    timeout(Duration::from_secs(60), async {
        let dir = tempfile::tempdir().unwrap();
        let (nodes, _config) = bring_up(3, dir.path()).await;
        await_bootstrap(&nodes).await;

        // Write 10 distinct keys to the single bootstrap tablet through the
        // client API (forwarded to the CP leader as needed).
        let mut stream = TcpStream::connect(nodes[0].client_addr())
            .await
            .expect("connect");
        for i in 0..10u32 {
            let key = format!("key{i:02}").into_bytes();
            let value = format!("v{i}").into_bytes();
            put(&mut stream, "kv", key, value).await;
        }

        // Manually split the bootstrap tablet at the midpoint key (ADR 0028: a
        // single atomic control-plane command, no separate data-plane step).
        let admin_addr = nodes[0].admin_addr();
        let (s, split) = admin(
            admin_addr,
            "POST",
            "/admin/tablet/split",
            Some(r#"{"tablet":1,"split_key":"key05"}"#),
        )
        .await;
        assert_eq!(s, 200, "split committed: {split}");

        // Wait for both halves to appear in the replicated tablet map.
        timeout(Duration::from_secs(15), async {
            loop {
                if nodes.iter().all(|n| n.metadata().tablets.len() >= 2) {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("split did not produce two tablets");

        // Node 0 hosts both halves (RF == cluster size here). Poll
        // `/admin/raftkv` until both groups report a `key_count`.
        let counts: std::collections::BTreeMap<u64, u64> =
            timeout(Duration::from_secs(15), async {
                loop {
                    let (_, raftkv) = admin_get(admin_addr, "/admin/raftkv?exact=1").await;
                    let groups = raftkv["groups"].as_array().cloned().unwrap_or_default();
                    // Post-cutover + reclaim (ADR 0050 rung 5): exactly the
                    // two children remain — the parent's group (tablet 1)
                    // must be GONE, not merely outnumbered (a mid-workflow
                    // poll sees parent + two seeding children = 3 groups).
                    if groups.len() == 2
                        && groups.iter().all(|g| {
                            g["tablet"].as_u64() != Some(1) && g["key_count"].as_u64().is_some()
                        })
                    {
                        return groups
                            .into_iter()
                            .map(|g| {
                                (
                                    g["tablet"].as_u64().unwrap(),
                                    g["key_count"].as_u64().unwrap(),
                                )
                            })
                            .collect();
                    }
                    sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .expect("node 0 hosts both split halves with a key_count");

        assert_eq!(
            counts.len(),
            2,
            "node 0 hosts two distinct tablets after the split: {counts:?}"
        );
        let total: u64 = counts.values().sum();
        assert_eq!(
            total, 10,
            "combined key_count across both tablets equals the 10 written keys: {counts:?}"
        );
        for (tablet, count) in &counts {
            assert!(
                *count < 10,
                "tablet {tablet}'s key_count ({count}) must be its own scoped subset, \
                 not the node's combined total of 10 keys across both co-resident tablets \
                 (the regression this test guards against)"
            );
        }

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}

/// ADR 0050 (Train B rung 3) teeth: a split is now an **asynchronous
/// workflow kickoff** — `POST /admin/tablet/split` starts it (parent
/// observed `Splitting`, two `Building` children minted, all visible in
/// `/admin/status`'s serialized `Metadata`), a second call is an idempotent
/// "already in flight" success, and a `Building` child is not splittable.
/// The workflow deliberately STOPS there in this rung (no driver/cutover
/// yet), so the end state this asserts — parent `Splitting` + children
/// `Building`, indefinitely — is the rung's contract, not an artifact.
///
/// **Pinned to `SplitMode::Copy`** (ADR 0058 rung 4 layer 2 flipped the
/// cluster-wide default to `InPlace`): this test's whole point is the
/// copy-based workflow's own intermediate metadata shape
/// (`Splitting`/`Building`), which the in-place fork doesn't produce at
/// all — an in-place split forms both children atomically with no
/// standing `Building` window. See `bring_up_with_split_mode`'s doc.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_split_kicks_off_the_copy_based_workflow() {
    timeout(Duration::from_secs(60), async {
        let dir = tempfile::tempdir().unwrap();
        let (nodes, _config) =
            bring_up_with_split_mode(1, dir.path(), animusd::SplitMode::Copy).await;
        await_bootstrap(&nodes).await;

        // Provision the table's bootstrap tablet through the ordinary client
        // write path, so the split has a real target.
        let mut stream = TcpStream::connect(nodes[0].client_addr())
            .await
            .expect("connect client port");
        put(&mut stream, "t", b"k".to_vec(), b"v".to_vec()).await;

        // Kick off the split via the admin action.
        let (status, body) = admin(
            nodes[0].admin_addr(),
            "POST",
            "/admin/tablet/split",
            Some(r#"{"tablet":1,"split_key":"k"}"#),
        )
        .await;
        assert_eq!(status, 200, "kickoff must succeed, got: {body}");

        // The lifecycle states ride `/admin/status` (the serialized
        // `Metadata` — the rung's split-status surface): poll until the
        // parent reads `Splitting` with exactly two `Building` children.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let states = loop {
            let (_, status_body) = admin(nodes[0].admin_addr(), "GET", "/admin/status", None).await;
            let tablets = status_body["tablets"]
                .as_object()
                .cloned()
                .unwrap_or_default();
            let state_of = |id: &str| {
                tablets
                    .get(id)
                    .map(|t| t["state"].as_str().unwrap_or("").to_owned())
            };
            let building: Vec<String> = tablets
                .iter()
                .filter(|(_, t)| t["state"].as_str() == Some("Building"))
                .map(|(id, _)| id.clone())
                .collect();
            if state_of("1").as_deref() == Some("Splitting") && building.len() == 2 {
                break building;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "parent never read Splitting with two Building children; tablets: {tablets:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        };

        // Idempotent kickoff: a second call reports success (already in
        // flight), and does NOT mint further children.
        let (status2, body2) = admin(
            nodes[0].admin_addr(),
            "POST",
            "/admin/tablet/split",
            Some(r#"{"tablet":1,"split_key":"k"}"#),
        )
        .await;
        assert_eq!(status2, 200, "re-kickoff must be idempotent, got: {body2}");
        let (_, status_body) = admin(nodes[0].admin_addr(), "GET", "/admin/status", None).await;
        let n_building = status_body["tablets"]
            .as_object()
            .unwrap()
            .values()
            .filter(|t| t["state"].as_str() == Some("Building"))
            .count();
        assert_eq!(n_building, 2, "no further children minted");

        // A `Building` child is not splittable.
        let child = &states[0];
        let (status3, body3) = admin(
            nodes[0].admin_addr(),
            "POST",
            "/admin/tablet/split",
            Some(&format!(r#"{{"tablet":{child},"split_key":"k"}}"#)),
        )
        .await;
        assert_ne!(status3, 200, "splitting a Building child must refuse");
        assert!(
            body3.to_string().contains("not splittable"),
            "refusal must say why, got: {body3}"
        );

        // The parent still serves BOTH reads and writes while `Splitting`.
        put(&mut stream, "t", b"k2".to_vec(), b"v2".to_vec()).await;
        animusd::write_frame(
            &mut stream,
            &ClientRequest::Get {
                key: b"k".to_vec(),
                table: "t".to_string(),
                stale: false,
            },
        )
        .await
        .expect("send post-split get");
        let got: ClientResponse = read_frame(&mut stream)
            .await
            .expect("read post-split get reply")
            .expect("a get reply");
        assert!(
            matches!(got, ClientResponse::Value(Some(ref v)) if v == b"v"),
            "a Splitting parent must keep serving reads: {got:?}"
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}
