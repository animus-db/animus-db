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

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, DynamoAuthConfig, Node, read_frame};
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
                advertise_host: None,
            })
            .collect();
        let config = animusd::ClusterConfig {
            nodes: nodes_cfg,
            dynamo_auth: None,
            cluster_settings: None,
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

/// Like [`bring_up`], but goes through `run_node_with_streams_quiesce_and_
/// backup_store` directly (production streams/segment-store/backup-store
/// defaults, quiescence explicitly disabled) instead of [`bring_up`]'s
/// plain [`animusd::run_node`] — used by
/// `admin_split_in_place_children_inherit_the_parents_own_replicas` below.
/// (Originally also pinned an explicit `SplitMode`, back when the crate had
/// two split workflows to choose between; the copy-based one and the
/// `SplitMode` selector were deleted in the copy-split-deletion endgame's
/// Layer B1 — every split is in-place unconditionally now.)
async fn bring_up_with_streams_quiesce(
    n: usize,
    dir: &std::path::Path,
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
                advertise_host: None,
            })
            .collect();
        let config = animusd::ClusterConfig {
            nodes: nodes_cfg,
            dynamo_auth: None,
            cluster_settings: None,
        };
        let mut nodes = Vec::new();
        let mut failed = false;
        for i in 0..n {
            match animusd::run_node_with_streams_quiesce_and_backup_store(
                &config,
                i,
                dir.join(format!("node-{attempt}-{i}")),
                animusd::StorageBackend::default(),
                animus_control::node::DEFAULT_ORPHAN_SWEEP_AFTER,
                animusd::StreamSealKnobs::default(),
                animusd::SegmentStoreConfig::default(),
                animusd::DEFAULT_STREAM_RETENTION,
                Duration::ZERO,
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
        let dir = support::panic_safe_tempdir();
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
        // U-06 (docs/roadmap.md): a combined-mode node started through plain
        // `run_node` gets the production-default `Cluster` stores, no
        // quiescence (disabled by default), and no `dynamo_auth` section —
        // `auth_enabled` is `Some(false)`, never `null` (combined nodes bind
        // the dynamo listener, so the field applies), and no key ids.
        assert_eq!(
            config_view["backup_store"]["kind"].as_str(),
            Some("cluster"),
            "default backup store: {config_view}"
        );
        assert_eq!(
            config_view["segment_store"]["kind"].as_str(),
            Some("cluster"),
            "default segment store: {config_view}"
        );
        assert!(
            config_view["quiesce_after_ms"].is_null(),
            "quiescence is off by default: {config_view}"
        );
        assert_eq!(
            config_view["auth_enabled"].as_bool(),
            Some(false),
            "no dynamo_auth section: {config_view}"
        );
        assert!(
            config_view["auth_access_key_ids"].is_null(),
            "auth is off: {config_view}"
        );
        // The resolved OTLP endpoint mirrors `animusd::otel::resolved_endpoint`'s
        // own env-var read exactly — asserted against the live process
        // environment rather than hardcoded to `null`, so this doesn't go
        // flaky under a CI runner that happens to export the var.
        let expected_otlp = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .ok()
            .filter(|e| !e.is_empty());
        assert_eq!(
            config_view["otlp_endpoint"].as_str().map(str::to_owned),
            expected_otlp,
            "resolved OTLP endpoint mirrors the process env: {config_view}"
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
        let dir = support::panic_safe_tempdir();
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
        let dir = support::panic_safe_tempdir();
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
        let dir = support::panic_safe_tempdir();
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

        // `BeginBackup`, proposed directly (this test predates the Train 1
        // PR④ wire surface and stays scoped to the admin observer alone —
        // `dynamo_backup.rs` covers the real `CreateBackup` wire path) —
        // retried across nodes since only the control leader accepts it.
        let begin = animus_control::MetaCommand::BeginBackup {
            backup_id: "backup-1".to_string(),
            table: "widgets".to_string(),
            created_wall_ms: 1_000,
            backup_name: "backup".to_string(),
            pitr_base: false,
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
        let dir = support::panic_safe_tempdir();
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
        let dir = support::panic_safe_tempdir();
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
        let dir = support::panic_safe_tempdir();
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
        let mut tablets: Vec<u64> = Vec::new();
        for g in rk["groups"].as_array().cloned().unwrap_or_default() {
            let tablet = g["tablet"].as_u64().expect("group has a tablet id");
            tablets.push(tablet);
            let (s, f) = admin(
                a,
                "POST",
                "/admin/storage/flush",
                Some(&format!(r#"{{"tablet":{tablet}}}"#)),
            )
            .await;
            assert_eq!(s, 200, "flush tablet {tablet}: {f}");
        }

        // Issue #587: wait for `big`'s own change-log housekeeping to drain
        // on THIS node before metering. `bring_up`'s nodes run with
        // quiescence disabled (`run_node`'s `quiesce_after: Duration::ZERO`
        // default), so `change_consumer_loop` (`index_drain.rs`) never gets
        // to skip a led group via `is_quiesced()` — it ticks every
        // `INDEX_DRAIN_INTERVAL` regardless. Every one of the `N` seeded
        // writes leaves an ADR 0049 change-log marker record with no
        // stream/GSI/PITR to consume it, so this table's tablet takes the
        // loop's mandatory idle fast path: as long as `KIND_CHANGE` bytes
        // remain, each tick does a real `pending_changes` scan (real
        // SSTable block reads once flushed) and trims a `TRIM_BATCH`-sized
        // slice, in batches, until the backlog is fully drained — only then
        // does the fast path's `bytes == 0` branch stop scanning for good.
        // That scan has nothing to do with the routes under test, but it
        // lands on the exact same node/counter this test meters, and under
        // scheduling pressure a tick can fall inside one metered window and
        // not the other, inflating `cheap` (or `exact`) independent of
        // `/admin/raftkv`'s own cost — the actual cause of this test's
        // one-off failure (issue #587), not a cross-node/process-wide
        // counter (the ADR 0015 metrics sink is per-node, see
        // `docs/engineering-lessons.md`).
        //
        // There is no single already-exposed "drain done" boolean here (the
        // RaftCore-level `quiesced` diagnostic doesn't apply — it never
        // fires at all with quiescence disabled), so the existing knob to
        // poll instead is the exact view's own `key_count`/`byte_size`
        // (`?exact=1`, ADR 0020): once the trim loop's batches stop landing,
        // these stop changing. Require several consecutive identical reads
        // (spaced past one `INDEX_DRAIN_INTERVAL`, so a batch's own
        // busy/idle-throttled cadence can't look "stable" mid-drain) before
        // treating the tablet as settled — a converged-or-timeout poll, not
        // a fixed sleep, per this repo's own testing discipline.
        const STABLE_READS_REQUIRED: usize = 5;
        let settle_deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let mut stable = 0usize;
        let mut last: Option<(u64, u64)> = None;
        loop {
            let (_, rk) = admin_get(a, "/admin/raftkv?exact=1").await;
            let groups = rk["groups"].as_array().cloned().unwrap_or_default();
            let totals = tablets.iter().try_fold((0u64, 0u64), |(kc, bs), t| {
                let g = groups.iter().find(|g| g["tablet"].as_u64() == Some(*t))?;
                Some((kc + g["key_count"].as_u64()?, bs + g["byte_size"].as_u64()?))
            });
            match totals {
                Some(cur) if last == Some(cur) => {
                    stable += 1;
                    if stable >= STABLE_READS_REQUIRED {
                        break;
                    }
                }
                Some(cur) => {
                    last = Some(cur);
                    stable = 1;
                }
                None => {
                    stable = 0;
                    last = None;
                }
            }
            assert!(
                tokio::time::Instant::now() < settle_deadline,
                "big's change-log housekeeping never settled before metering: {groups:?}"
            );
            sleep(Duration::from_millis(250)).await;
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
        // `raft_view`'s exact path (`CpGroup::local_pairs`) is a pure LOCAL engine
        // read with no consensus barrier or leadership check (`admin.rs`'s own
        // doc: "/admin/raftkv is node-local"), and every poll in this test targets
        // `nodes[0]` specifically regardless of which node leads the "big" table's
        // tablet. If node 0 is a follower here, its exact count is an EVENTUAL
        // property of its own apply loop, not a fact the leader-side seed/flush
        // acks already guarantee — so assert it one-shot only. Converge-poll
        // instead (the repo's own idiom, `docs/engineering-lessons.md`'s Testing
        // section: "Eventual properties get a converged-or-timeout poll, never a
        // fixed-deadline one-shot assert"). Bounded generously (20s) relative to
        // the apply/flush cadence exercised above, not guessed.
        let exact_view = timeout(Duration::from_secs(20), async {
            loop {
                let (_, v) = admin_get(a, "/admin/raftkv?exact=1").await;
                if sum(&v) >= N as u64 {
                    return v;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "`?exact=1` on node {a} never converged to counting every seeded row \
                 within 20s — a lagging follower's own applied state, not a genuine \
                 undercount (the seeding/flush above only guarantees the LEADER's \
                 view, not every follower's)"
            )
        });
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
        let dir = support::panic_safe_tempdir();
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

/// ADR 0062 rung 4 ("fork first, always local") teeth: `trigger_split`'s
/// `InPlace` arm no longer calls `split_child_placement` — both children's
/// recorded replicas must be exactly the parent's own current replicas,
/// never a placement-recomputed set. A 4-node cluster with `RF = 3`
/// (`MAX_REPLICATION_FACTOR`) is the deliberate setup: node `n3` is never
/// one of the table's tablet's replicas, so it is exactly the kind of
/// currently-idle, would-balance-the-load candidate the OLD `split_
/// child_placement`/fork F5 path would have been drawn to recruit for at
/// least one child (a genuine differentiator, not just "the only replica
/// set available"). This asserts the pre-fork `MetaCommand::
/// BeginSplitInPlace` intent recorded on the STILL-`Splitting` parent
/// (`Tablet::inplace_split`, visible on `/admin/status` before Stage 3's
/// fork ever runs) — the proposer-side computation this rung changed —
/// without needing the fork/cutover to actually complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_split_in_place_children_inherit_the_parents_own_replicas() {
    timeout(Duration::from_secs(60), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, _config) = bring_up_with_streams_quiesce(4, dir.path()).await;
        await_bootstrap(&nodes).await;

        // Provision the table's bootstrap tablet through the ordinary
        // client write path — `provision_tablet` picks the first
        // `min(N, MAX_REPLICATION_FACTOR)` = 3 of the 4 `Active` members in
        // `NodeId` order (n0, n1, n2), leaving n3 unhosted and idle.
        let mut stream = TcpStream::connect(nodes[0].client_addr())
            .await
            .expect("connect client port");
        put(&mut stream, "t", b"k".to_vec(), b"v".to_vec()).await;

        let (_, before) = admin(nodes[0].admin_addr(), "GET", "/admin/status", None).await;
        let parent_replicas: Vec<String> = before["tablets"]["1"]["replicas"]
            .as_array()
            .expect("parent has a replica list")
            .iter()
            .map(|v| v.as_str().unwrap().to_owned())
            .collect();
        assert_eq!(
            parent_replicas.len(),
            3,
            "the bootstrap tablet's RF must be MAX_REPLICATION_FACTOR (3): {before}"
        );
        assert!(
            !parent_replicas.iter().any(|n| n == "n3"),
            "n3 must be idle (not one of the parent's replicas) for this test to \
             distinguish fork-first from placement-chosen homes: {parent_replicas:?}"
        );

        let (status, body) = admin(
            nodes[0].admin_addr(),
            "POST",
            "/admin/tablet/split",
            Some(r#"{"tablet":1,"split_key":"k"}"#),
        )
        .await;
        assert_eq!(status, 200, "kickoff must succeed, got: {body}");

        // Poll for the parent reading `Splitting` with its `inplace_split`
        // intent recorded — the in-place workflow mints no `Building` rows,
        // so (unlike the copy-based test above) the intent's own `children`
        // array, not the tablet map, is what carries each child's replicas.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let intent = loop {
            let (_, status_body) = admin(nodes[0].admin_addr(), "GET", "/admin/status", None).await;
            let parent = &status_body["tablets"]["1"];
            if parent["state"].as_str() == Some("Splitting") && !parent["inplace_split"].is_null() {
                break parent["inplace_split"].clone();
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "parent never recorded an in-place split intent; status: {status_body:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        };

        let children = intent["children"]
            .as_array()
            .expect("intent carries exactly two children");
        assert_eq!(children.len(), 2, "intent must carry exactly two children");
        for (i, child) in children.iter().enumerate() {
            let child_replicas: Vec<String> = child["replicas"]
                .as_array()
                .unwrap_or_else(|| panic!("child {i} has a replica list: {child}"))
                .iter()
                .map(|v| v.as_str().unwrap().to_owned())
                .collect();
            assert_eq!(
                child_replicas, parent_replicas,
                "child {i}'s replicas must be exactly the parent's own current \
                 replicas (ADR 0062 rung 4), got {child_replicas:?} vs parent \
                 {parent_replicas:?}"
            );
        }

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}

/// Bring up a single node with a `dynamo_auth` section (ADR 0057), retrying
/// the port-TOCTOU race exactly like [`bring_up`] does — this file's own
/// copy since `bring_up` always builds a config with `dynamo_auth: None`,
/// the same "sibling test modules keep their own fixtures independent"
/// convention `dynamo_sigv4.rs::start_single_node_with_auth` already uses.
async fn bring_up_with_auth(
    dir: &std::path::Path,
    credentials: BTreeMap<String, String>,
) -> (Node, animusd::ClusterConfig) {
    for attempt in 0..16 {
        let addrs = support::free_addrs(6);
        let config = animusd::ClusterConfig {
            nodes: vec![animusd::RoleAddrs {
                id: animusd::config::node_id(0),
                role: animusd::config::NodeRole::Both,
                internal: addrs[0],
                client: addrs[1],
                dynamo: addrs[2],
                admin: addrs[3],
                intra: addrs[4],
                console: addrs[5],
                advertise_host: None,
            }],
            dynamo_auth: Some(DynamoAuthConfig {
                credentials: credentials.clone(),
            }),
            cluster_settings: None,
        };
        match animusd::run_node(&config, 0, dir.join(format!("node-{attempt}"))).await {
            Ok(node) => return (node, config),
            Err(_) => sleep(Duration::from_millis(50)).await,
        }
    }
    panic!("single node (dynamo_auth) failed to start after retries (ports kept getting stolen)");
}

/// U-06 (docs/roadmap.md): `/admin/config` reports `auth_enabled: true` and
/// the configured access key **ids** once a `dynamo_auth` section is
/// present — and, load-bearing, the configured *secret* never appears
/// anywhere in the served JSON, however it's rendered. Asserted against the
/// raw response body text, not just the parsed `auth_access_key_ids` field,
/// so this would catch the secret leaking through some other field too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_config_reports_auth_state_and_never_serves_the_secret() {
    timeout(Duration::from_secs(30), async {
        const ACCESS_KEY_ID: &str = "AKIDEXAMPLE";
        const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
        let mut credentials = BTreeMap::new();
        credentials.insert(ACCESS_KEY_ID.to_string(), SECRET.to_string());

        let dir = support::panic_safe_tempdir();
        let (node, _config) = bring_up_with_auth(dir.path(), credentials).await;
        let admin_addr = node.admin_addr();

        let (status, config_view) = admin_get(admin_addr, "/admin/config").await;
        assert_eq!(status, 200, "config_view: {config_view}");

        assert_eq!(
            config_view["auth_enabled"].as_bool(),
            Some(true),
            "a dynamo_auth section is configured: {config_view}"
        );
        assert_eq!(
            config_view["auth_access_key_ids"].as_array(),
            Some(&vec![Value::String(ACCESS_KEY_ID.to_string())]),
            "the access key id (never the secret) is reported: {config_view}"
        );

        // The load-bearing assertion: the secret never leaves this node's
        // admin surface, in this field or any other.
        let raw = serde_json::to_string(&config_view).expect("config_view serializes");
        assert!(
            !raw.contains(SECRET),
            "the SigV4 secret access key must never appear in /admin/config: {raw}"
        );

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}
