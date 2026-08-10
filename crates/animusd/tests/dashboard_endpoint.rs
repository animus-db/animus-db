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

/// ADR 0035 PR7: role-gated dashboards. A genuine split deployment (a
/// control-only node, no data role at all, alongside a data-only node, no
/// local control `RaftCore` at all — `support::bring_up_split`) still serves
/// the identical SPA shell + JS assets from both admin ports (no forked
/// shell, no second HTML file — `admin.rs::is_ui_path`/`static_asset` are
/// unchanged by this feature); what differs is which tabs the *client-side*
/// role gating shows, which we assert against the JS assets that actually
/// carry that behavior (`dashboard_core.js`'s `ROLE_TABS`, `dashboard_node.js`
/// itself), per this file's own documented lesson on asserting against the
/// asset that carries the behavior rather than the shell. Also proves the
/// backend addition this PR needed: `/admin/raft`'s `control_mirror` (the ADR
/// 0035 §1/§5 watermark + leader hint) actually syncs on the data-only node.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn dashboard_role_gating_split_deployment() {
    timeout(Duration::from_secs(60), async {
        let dir = tempfile::tempdir().unwrap();
        let (control_nodes, data_nodes, _config) = support::bring_up_split(1, 1, dir.path()).await;
        support::await_leader(&control_nodes).await;
        let data_raftkv_ids: Vec<animus_env::NodeId> =
            (1..2).map(animusd::config::raftkv_id).collect();
        support::await_data_nodes_active(&control_nodes, &data_raftkv_ids).await;

        let control_admin = control_nodes[0].admin_addr();
        let data_admin = data_nodes[0].admin_addr();

        // ---- both roles serve the same shell + JS assets ------------------
        for addr in [control_admin, data_admin] {
            let (s, head, body) = raw(addr, "GET", "/").await;
            assert_eq!(s, 200, "the shell 200s on both roles");
            assert!(
                head.to_ascii_lowercase()
                    .contains("content-type: text/html"),
                "shell is html: {head}"
            );
            assert!(
                body.contains("AnimusDB Console") && body.contains("dashboard_node.js"),
                "shell references the new Node view asset: {body}"
            );
            let (s, _, node_js) = raw(addr, "GET", "/admin/ui/dashboard_node.js").await;
            assert_eq!(s, 200, "dashboard_node.js is served on both roles");
            assert!(
                node_js.contains("function renderNode")
                    && node_js.contains("control_mirror")
                    && node_js.contains("nd-tablet-sel"),
                "dashboard_node.js carries the Node view's rendering + storage-debug markers"
            );
        }

        // ---- the gating logic itself lives in dashboard_core.js -----------
        let (s, _, core_js) = raw(control_admin, "GET", "/admin/ui/dashboard_core.js").await;
        assert_eq!(s, 200);
        assert!(
            core_js.contains("ROLE_TABS")
                && core_js.contains("applyRoleGating")
                && core_js.contains(r#"data: ["node", "browser"]"#),
            "dashboard_core.js defines the per-role tab gating, including the \
             data role's node-first tab list: {core_js}"
        );

        // ---- /admin/config's role differs across the split -----------------
        let (s, _, body) = raw(control_admin, "GET", "/admin/config").await;
        assert_eq!(s, 200);
        let cfg: Value = serde_json::from_str(&body).expect("config is JSON");
        assert_eq!(cfg["role"].as_str(), Some("control"));

        let (s, _, body) = raw(data_admin, "GET", "/admin/config").await;
        assert_eq!(s, 200);
        let cfg: Value = serde_json::from_str(&body).expect("config is JSON");
        assert_eq!(cfg["role"].as_str(), Some("data"));

        // ---- the data-only node's control-plane mirror actually syncs -----
        // (ADR 0035 PR7's one backend addition: `/admin/raft`'s
        // `control_mirror`.) Bounded poll — the mirror needs at least one
        // sync/long-poll round trip against the control deployment.
        timeout(Duration::from_secs(20), async {
            loop {
                let (s, _, body) = raw(data_admin, "GET", "/admin/raft").await;
                assert_eq!(s, 200);
                let view: Value = serde_json::from_str(&body).expect("raft view is JSON");
                let cm = &view["control_mirror"];
                assert!(cm.is_object(), "control_mirror is present: {view}");
                assert!(cm["watermark"].is_u64(), "watermark is a number: {cm}");
                assert!(
                    cm["leader_hint"].is_null() || cm["leader_hint"].is_string(),
                    "leader_hint is null or a string: {cm}"
                );
                if cm["has_synced"] == Value::Bool(true) {
                    return;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("data-only node's control_mirror did not sync in 20s");

        // A control-only node IS a control-plane voter, so its own `/admin/raft`
        // reports the honest degenerate mirror (never synced via a mirror —
        // its own Raft state above already is the ground truth).
        let (s, _, body) = raw(control_admin, "GET", "/admin/raft").await;
        assert_eq!(s, 200);
        let view: Value = serde_json::from_str(&body).expect("raft view is JSON");
        assert_eq!(
            view["control_mirror"]["has_synced"],
            Value::Bool(false),
            "a control-plane voter's own mirror is never 'synced' (no mirror involved): {view}"
        );

        // ---- /admin/peers carries per-node ROLE (ADR 0035 residual
        // follow-up) for a genuine mixed control-only + data-only cluster —
        // previously role was only ever knowable by fetching that specific
        // node's own `/admin/config`, so the dashboard had to fan out to
        // every peer just to label them. Each node's role is read off its
        // own self-registered `NodeAddrs.role` (`RegisterNodeAddrs`), a
        // best-effort proposal — bounded poll for it to have landed, not a
        // one-shot assert, mirroring every other commit-wait in this suite.
        // `admin_addrs` (the pre-existing field) is asserted unchanged too,
        // so this stays a strict addition, not a breaking response shape.
        timeout(Duration::from_secs(20), async {
            loop {
                let (s, _, body) = raw(control_admin, "GET", "/admin/peers").await;
                assert_eq!(s, 200);
                let peers: Value = serde_json::from_str(&body).expect("peers is JSON");
                let admin_addrs = peers["admin_addrs"]
                    .as_array()
                    .expect("admin_addrs is still an array");
                let control_str = control_admin.to_string();
                let data_str = data_admin.to_string();
                assert!(
                    admin_addrs
                        .iter()
                        .any(|a| a.as_str() == Some(control_str.as_str())),
                    "admin_addrs still lists the control node: {peers}"
                );
                assert!(
                    admin_addrs
                        .iter()
                        .any(|a| a.as_str() == Some(data_str.as_str())),
                    "admin_addrs still lists the data node: {peers}"
                );
                let peer_list = peers["peers"].as_array().expect("peers array present");
                let role_of = |addr: &str| {
                    peer_list
                        .iter()
                        .find(|p| p["admin"].as_str() == Some(addr))
                        .and_then(|p| p["role"].as_str())
                        .map(str::to_string)
                };
                if role_of(&control_str).as_deref() == Some("control")
                    && role_of(&data_str).as_deref() == Some("data")
                {
                    return;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("peers did not report both nodes' roles in 20s");

        for node in control_nodes.iter().chain(data_nodes.iter()) {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}
