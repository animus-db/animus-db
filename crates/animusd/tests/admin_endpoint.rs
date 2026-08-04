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
                && nodes.iter().all(|n| !n.metadata().tablets.is_empty())
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
                table: None,
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
