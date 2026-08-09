//! End-to-end test of the web dashboard surface (ADR 0021) over real TCP
//! (`ProdEnv`): the static SPA is served from the admin port, `/admin/*` responses
//! carry the CORS header the browser's cross-node fan-out needs, `OPTIONS`
//! preflight is answered, and `GET /admin/peers` returns every node's admin
//! address (the fan-out seed). The panels themselves are pure clients of the
//! ADR 0020 JSON already covered by `admin_endpoint.rs`; here we prove the
//! plumbing the dashboard relies on.
//!
//! Brings the cluster up one process per node (each its own `ClusterEdgeState`),
//! so `/admin/peers` is exercised against a real per-process config. Real time +
//! sockets, so it polls with generous timeouts and uses the documented
//! port-TOCTOU bring-up retry.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::Node;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// Bring up an `n`-node cluster, one process per node, retrying the
/// (allocate-fresh-ports + start-all) as a unit (the documented port-TOCTOU
/// mitigation, see the crate guide).
async fn bring_up(n: usize, dir: &std::path::Path) -> (Vec<Node>, animusd::ClusterConfig) {
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

/// One HTTP/1.0 request; returns `(status, raw header block, body)`.
async fn raw(addr: SocketAddr, method: &str, path: &str) -> (u16, String, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to admin");
    let request = format!("{method} {path} HTTP/1.0\r\nHost: animus\r\nConnection: close\r\n\r\n",);
    stream
        .write_all(request.as_bytes())
        .await
        .expect("send request");
    stream.flush().await.expect("flush");
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await.expect("read response");
    let text = String::from_utf8(bytes).expect("utf8 response");
    let (head, body) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status line");
    (status, head.to_string(), body.to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn dashboard_serves_spa_with_cors_and_peers() {
    timeout(Duration::from_secs(60), async {
        let dir = tempfile::tempdir().unwrap();
        let (nodes, config) = bring_up(3, dir.path()).await;
        await_bootstrap(&nodes).await;

        let admin_addr = nodes[0].admin_addr();

        // ---- GET / serves the SPA as text/html, with CORS ------------------
        let (s, head, body) = raw(admin_addr, "GET", "/").await;
        let head_lc = head.to_ascii_lowercase();
        assert_eq!(s, 200, "root serves the dashboard");
        assert!(
            head_lc.contains("content-type: text/html"),
            "dashboard is text/html, headers:\n{head}"
        );
        assert!(
            head_lc.contains("access-control-allow-origin: *"),
            "dashboard response carries CORS, headers:\n{head}"
        );
        assert!(
            body.contains("AnimusDB Console") && body.contains("dashboard_core.js"),
            "served the console shell, referencing its script assets"
        );
        // The/admin/data/dynamo item form locks key attribute rows (no delete
        // on pk/sk) — that logic lives in dashboard_browser.js now (the shell
        // only references it by `<script src>`, ADR 0021 §1's split-file
        // architecture), so check the asset that actually carries it rather
        // than the shell's own body.
        let (s, _, browser_js) = raw(admin_addr, "GET", "/admin/ui/dashboard_browser.js").await;
        assert_eq!(s, 200, "dashboard_browser.js is served");
        assert!(
            browser_js.contains("key-badge"),
            "the item form locks key attribute rows (no delete on pk/sk)"
        );
        // The /admin/ui alias serves the same asset.
        let (s, _, body2) = raw(admin_addr, "GET", "/admin/ui").await;
        assert_eq!(s, 200);
        assert_eq!(body2.len(), body.len(), "/admin/ui is the same asset");

        // ---- per-tab deep links (ADR 0021 follow-up 7) also serve the SPA --
        // The client reads location.pathname to pick the active tab, so the
        // server just needs to serve the same asset for any /admin/ui/<tab>
        // path — including one it doesn't recognize (the client falls back to
        // the default tab rather than 404ing).
        for path in [
            "/admin/ui/overview",
            "/admin/ui/placement",
            "/admin/ui/tablets",
            "/admin/ui/browser",
            "/admin/ui/storage",
            "/admin/ui/not-a-real-tab",
        ] {
            let (s, head, body3) = raw(admin_addr, "GET", path).await;
            assert_eq!(s, 200, "{path} serves the dashboard");
            assert!(
                head.to_ascii_lowercase()
                    .contains("content-type: text/html"),
                "{path} is text/html, headers:\n{head}"
            );
            assert_eq!(body3.len(), body.len(), "{path} is the same asset");
        }

        // ---- CORS on the JSON surface (the fan-out prerequisite) -----------
        let (s, head, _) = raw(admin_addr, "GET", "/admin/status").await;
        assert_eq!(s, 200);
        assert!(
            head.to_ascii_lowercase()
                .contains("access-control-allow-origin: *"),
            "JSON responses carry CORS for cross-node fan-out, headers:\n{head}"
        );

        // ---- OPTIONS preflight is answered with CORS + 204 -----------------
        let (s, head, _) = raw(admin_addr, "OPTIONS", "/admin/status").await;
        assert_eq!(s, 204, "preflight is 204 No Content");
        assert!(
            head.to_ascii_lowercase()
                .contains("access-control-allow-methods"),
            "preflight advertises allowed methods, headers:\n{head}"
        );

        // ---- /admin/peers lists every node's admin address -----------------
        let (s, head, body) = raw(admin_addr, "GET", "/admin/peers").await;
        assert_eq!(s, 200);
        assert!(
            head.to_ascii_lowercase()
                .contains("content-type: application/json"),
            "peers is JSON, headers:\n{head}"
        );
        let peers: Value = serde_json::from_str(&body).expect("peers is JSON");
        let addrs = peers["admin_addrs"].as_array().expect("admin_addrs array");
        assert_eq!(
            addrs.len(),
            3,
            "the fan-out seed lists all 3 nodes: {peers}"
        );
        for node_cfg in &config.nodes {
            let want = node_cfg.admin.to_string();
            assert!(
                addrs.iter().any(|a| a.as_str() == Some(want.as_str())),
                "peers includes {want}: {peers}"
            );
        }
        assert_eq!(
            peers["this"].as_str(),
            Some(admin_addr.to_string().as_str()),
            "peers marks the serving node: {peers}"
        );

        // ---- /admin/config's derived `role` (ADR 0035 PR6) -----------------
        // This cluster is combined-mode (every node `Both`); the dashboard
        // renders per-node role from this same field for a split deployment
        // (see the JS asset checks below), so a combined node reporting
        // "combined" here is what makes that rendering honest rather than
        // coincidentally correct.
        let (s, _, body) = raw(admin_addr, "GET", "/admin/config").await;
        assert_eq!(s, 200);
        let config_view: Value = serde_json::from_str(&body).expect("config is JSON");
        assert_eq!(
            config_view["role"].as_str(),
            Some("combined"),
            "a combined-mode node's /admin/config reports role=combined: {config_view}"
        );

        // ---- the Overview view renders a node's role, including a
        // control-only node's leader label (ADR 0035 PR6) -------------------
        // Asserted against the JS assets that actually carry this behavior
        // (`dashboard_core.js`'s `nodeDisplayId` helper, `dashboard_overview.js`'s
        // use of it for the control-leader label, and its per-node role tag) —
        // not the shell's own body, which never contained this logic even
        // before the split-file rewrite (see this file's own note above on
        // why that distinction matters).
        let (s, _, core_js) = raw(admin_addr, "GET", "/admin/ui/dashboard_core.js").await;
        assert_eq!(s, 200, "dashboard_core.js is served");
        assert!(
            core_js.contains("function nodeDisplayId"),
            "dashboard_core.js defines nodeDisplayId (falls back to control_id \
             for a control-only leader, ADR 0035)"
        );
        let (s, _, overview_js) = raw(admin_addr, "GET", "/admin/ui/dashboard_overview.js").await;
        assert_eq!(s, 200, "dashboard_overview.js is served");
        assert!(
            overview_js.contains("nodeDisplayId(h.controlLeader)"),
            "the control-plane tile/banner label the leader via nodeDisplayId, \
             not nodeRaftkvId (which is null for a control-only leader)"
        );
        assert!(
            overview_js.contains("config.role"),
            "the nodes list renders each row's role (control/data/combined)"
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}
