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

/// Reserve `count` free loopback ports (bind :0, read addr, release).
fn free_addrs(count: usize) -> Vec<SocketAddr> {
    let ls: Vec<std::net::TcpListener> = (0..count)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    ls.iter().map(|l| l.local_addr().unwrap()).collect()
}

/// Bring up an `n`-node cluster, one process per node (each its own edge state),
/// retrying the (allocate-fresh-ports + start-all) as a unit. `free_addrs` frees
/// each port before `run_node` rebinds it, so another test binary can steal one in
/// the window (`AddrInUse`); a fresh attempt re-allocates and the started nodes are
/// torn down first (the documented port-TOCTOU mitigation, see the crate guide).
async fn bring_up(n: usize, dir: &std::path::Path) -> (Vec<Node>, animusd::ClusterConfig) {
    for attempt in 0..16 {
        let addrs = free_addrs(n * 6);
        let nodes_cfg: Vec<animusd::RoleAddrs> = (0..n)
            .map(|i| animusd::RoleAddrs {
                control: addrs[6 * i],
                client: addrs[6 * i + 1],
                dynamo: addrs[6 * i + 2],
                cql: addrs[6 * i + 3],
                raftkv: addrs[6 * i + 4],
                admin: addrs[6 * i + 5],
            })
            .collect();
        let config = animusd::ClusterConfig { nodes: nodes_cfg };
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
        animusd::write_frame(
            &mut stream,
            &ClientRequest::Put {
                key: b"admin-key".to_vec(),
                value: b"admin-val".to_vec(),
                table: "kv".to_string(),
            },
        )
        .await
        .expect("send put");
        let put: ClientResponse = read_frame(&mut stream)
            .await
            .expect("read reply")
            .expect("a reply");
        assert!(matches!(put, ClientResponse::PutOk), "put failed: {put:?}");

        let admin_addr = nodes[0].admin_addr();

        // ---- /admin/config -------------------------------------------------
        let (s, config_view) = admin_get(admin_addr, "/admin/config").await;
        assert_eq!(s, 200);
        assert_eq!(config_view["control_id"], 0, "node 0's control id");
        assert_eq!(
            config_view["raftkv_id"], 300,
            "node 0's raftkv id (300 + 0)"
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

/// The dashboard's write proxies (ADR 0021): run a DynamoDB CRUD round-trip and a
/// multi-statement CQL script through the admin port, asserting data flows back.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn admin_data_write_dynamo_and_cql() {
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
            Some(r#"{"op":"GetItem","payload":{"TableName":"t","Key":{"pk":{"S":"alice"}}}}"#),
        )
        .await;
        assert_eq!(s, 200, "GetItem via admin proxy: {got}");
        assert_eq!(
            got["Item"]["v"]["N"].as_str(),
            Some("7"),
            "GetItem reads back the written value: {got}"
        );

        // ---- CQL: CREATE/INSERT/SELECT script via /admin/data/cql -----------
        let (s, cql) = admin(
            a,
            "POST",
            "/admin/data/cql",
            Some(
                r#"{"query":"CREATE KEYSPACE ks; USE ks; CREATE TABLE t2 (id int PRIMARY KEY, v text); INSERT INTO t2 (id, v) VALUES (1, 'hi'); SELECT * FROM t2 WHERE id = 1;"}"#,
            ),
        )
        .await;
        assert_eq!(s, 200, "CQL script ran: {cql}");
        let results = cql["results"].as_array().expect("results array");
        let select = results.last().expect("a final SELECT result");
        assert_eq!(select["kind"], "rows", "last statement is a row result: {cql}");
        assert_eq!(select["row_count"], 1, "one row inserted: {cql}");
        let row = &select["rows"][0];
        assert!(
            row.as_array().is_some_and(|cells| cells.iter().any(|c| c == "hi")),
            "SELECT returns the inserted value: {cql}"
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

/// The bulk-seed endpoint (ADR 0021) writes the requested number of synthetic keys
/// to the CP plane, and they land durably (visible in the CP leader's storage).
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

/// Regression: `/admin/raftkv`'s `key_count` must be scoped to each tablet's own
/// `StorageScope` range, not the shared engine's combined total. A node hosts
/// more than one tablet on the same engine as soon as it hosts a split's parent
/// + child (ADR 0028); before the fix, `key_count` read the whole shared engine
/// (`CpGroup::approx_key_count`), so both halves' rows showed the *node's*
/// combined total rather than their own subset.
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
            animusd::write_frame(
                &mut stream,
                &ClientRequest::Put {
                    key,
                    value,
                    table: "kv".to_string(),
                },
            )
            .await
            .expect("send put");
            let put: ClientResponse = read_frame(&mut stream)
                .await
                .expect("read reply")
                .expect("a reply");
            assert!(matches!(put, ClientResponse::PutOk), "put failed: {put:?}");
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
                    let (_, raftkv) = admin_get(admin_addr, "/admin/raftkv").await;
                    let groups = raftkv["groups"].as_array().cloned().unwrap_or_default();
                    if groups.len() >= 2 && groups.iter().all(|g| g["key_count"].as_u64().is_some())
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
