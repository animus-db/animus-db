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

/// Like [`raw`], but a `POST` carrying a body and one extra header line
/// (e.g. `X-Amz-Target: ...` for the DynamoDB edge, or none for the plain-JSON
/// admin proxy) — needed for the streams-on-control-node regression below,
/// which drives both the DynamoDB item edge (to create a streamed table and
/// force a seal) and the admin `/admin/data/dynamo` Streams proxy.
async fn raw_post(addr: SocketAddr, path: &str, extra_header: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let header = if extra_header.is_empty() {
        String::new()
    } else {
        format!("{extra_header}\r\n")
    };
    let request = format!(
        "POST {path} HTTP/1.0\r\nHost: animus\r\nConnection: close\r\n{header}Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("send request");
    stream.flush().await.expect("flush");
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await.expect("read response");
    let text = String::from_utf8(bytes).expect("utf8 response");
    let (head, resp_body) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status line");
    (status, resp_body.to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn dashboard_serves_spa_with_cors_and_peers() {
    timeout(Duration::from_secs(60), async {
        let dir = support::panic_safe_tempdir();
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
            body.contains("animusd admin") && body.contains("dashboard_core.js"),
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
        // ADR 0038 PR4: the Storage tab's control-plane system-keyspace
        // section — the shell carries the markup (a distinct node selector +
        // card, independent of the per-tablet `st-tablet`/`st-node` ones,
        // since a control-only node hosts no CP tablet at all), and
        // dashboard_storage.js carries the fetch/render logic against
        // `/admin/storage/control`.
        assert!(
            body.contains("ctl-node") && body.contains("ctl-storage-body"),
            "the Storage tab's shell carries the control-plane storage section"
        );
        let (s, _, storage_js) = raw(admin_addr, "GET", "/admin/ui/dashboard_storage.js").await;
        assert_eq!(s, 200, "dashboard_storage.js is served");
        assert!(
            storage_js.contains("function loadControlStorage")
                && storage_js.contains("/admin/storage/control")
                && storage_js.contains("function updateControlStorageNodeOptions"),
            "dashboard_storage.js fetches and renders the control-plane storage section"
        );
        // plan-syskv-ui (an ADR 0038 addendum): the system-keyspace BROWSE
        // section nested inside that same card — a kind filter, an
        // "as of index N" watermark label, and a forward-only pager — and
        // the JS wiring driving it against `/admin/system-table`.
        assert!(
            body.contains("ctl-kind")
                && body.contains("ctl-browse-body")
                && body.contains("ctl-applied-index")
                && body.contains("ctl-next-page"),
            "the Storage tab's shell carries the system-table browse section"
        );
        assert!(
            storage_js.contains("function loadSystemTable")
                && storage_js.contains("/admin/system-table")
                && storage_js.contains("function renderSystemTableKindOptions"),
            "dashboard_storage.js fetches and renders the system-table browse section"
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
            "dashboard_core.js defines nodeDisplayId (renders a node's one id, \
             ADR 0040 PR1 — works for a control-only leader too, since every \
             role now has exactly one id)"
        );
        let (s, _, overview_js) = raw(admin_addr, "GET", "/admin/ui/dashboard_overview.js").await;
        assert_eq!(s, 200, "dashboard_overview.js is served");
        assert!(
            overview_js.contains("nodeDisplayId(h.controlLeader)"),
            "the control-plane tile/banner label the leader via nodeDisplayId"
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
        let dir = support::panic_safe_tempdir();
        let (control_nodes, data_nodes, _config) = support::bring_up_split(1, 1, dir.path()).await;
        support::await_leader(&control_nodes).await;
        let data_raftkv_ids: Vec<animus_env::NodeId> =
            (1..2).map(animusd::config::node_id).collect();
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
                body.contains("animusd admin") && body.contains("dashboard_node.js"),
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
                && core_js.contains(r#"data: ["node", "browser", "streams"]"#),
            "dashboard_core.js defines the per-role tab gating, including the \
             data role's node-first tab list (now with Streams, ADR 0042/0043): {core_js}"
        );
        assert!(
            core_js.contains(
                r#"control: ["overview", "placement", "tablets", "txns", "browser", "streams", "storage", "backups"]"#
            ),
            "the control role's own tab list now includes Streams too (a control-only \
             node holds the full replicated Metadata, so the stream list + shard-chain \
             detail render truthfully there; only the live-tail poller degrades), \
             Transactions too (docs/roadmap.md U-01, gated like tablets), and now \
             Backups too (docs/roadmap.md U-02, gated like placement): {core_js}"
        );

        // ---- /admin/config's role differs across the split -----------------
        let (s, _, body) = raw(control_admin, "GET", "/admin/config").await;
        assert_eq!(s, 200);
        let cfg: Value = serde_json::from_str(&body).expect("config is JSON");
        assert_eq!(cfg["role"].as_str(), Some("control"));
        // U-06 (docs/roadmap.md): a control-only node provisions neither
        // store, never runs the reconciler quiescence knob, and never binds
        // the dynamo listener — every one of these fields is `null`, never
        // omitted, so the shape stays stable across roles.
        assert!(
            cfg["backup_store"].is_null()
                && cfg["segment_store"].is_null()
                && cfg["quiesce_after_ms"].is_null()
                && cfg["auth_enabled"].is_null()
                && cfg["auth_access_key_ids"].is_null(),
            "a control-only node's /admin/config has no store/quiesce/auth fields: {cfg}"
        );

        let (s, _, body) = raw(data_admin, "GET", "/admin/config").await;
        assert_eq!(s, 200);
        let cfg: Value = serde_json::from_str(&body).expect("config is JSON");
        assert_eq!(cfg["role"].as_str(), Some("data"));
        // A data-only node gets its own real, independently-configured
        // backup/segment store (ADR 0059 §1's asymmetry with control-only),
        // and binds the dynamo listener (so `auth_enabled` is `Some(false)`
        // — this deployment has no `dynamo_auth` section — never `null`).
        // `quiesce_after_ms` is `null` here because this fixture's config
        // carries no `cluster_settings.quiesce_after_secs` (S-06's only
        // route to the knob on a data-only node), so quiescence is off.
        assert_eq!(
            cfg["backup_store"]["kind"].as_str(),
            Some("cluster"),
            "a data-only node's own backup store: {cfg}"
        );
        assert_eq!(
            cfg["segment_store"]["kind"].as_str(),
            Some("cluster"),
            "a data-only node's own segment store: {cfg}"
        );
        assert!(
            cfg["quiesce_after_ms"].is_null(),
            "data-only quiescence is off without cluster_settings: {cfg}"
        );
        assert_eq!(
            cfg["auth_enabled"].as_bool(),
            Some(false),
            "no dynamo_auth section in this deployment: {cfg}"
        );
        assert!(cfg["auth_access_key_ids"].is_null(), "auth is off: {cfg}");

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

/// Dashboard follow-up (this PR): the Streams tab now also shows on a
/// control-only node's console (`ROLE_TABS`, `dashboard_core.js`) — verified
/// here against a **real** split deployment that a control-only node's
/// `/admin/status` genuinely carries the replicated stream catalog
/// (`schemas.tables[t].stream` + `stream_shards`), and that the metadata-only
/// half of the Streams read API the view calls through `/admin/data/dynamo`
/// (`ListStreams`/`DescribeStream`) answers truthfully there for both an
/// still-open stream and one force-sealed via `UpdateTable{StreamEnabled:
/// false}` (F12-b's final seal — no small `--stream-seal-bytes` knob is
/// needed to exercise a genuine sealed shard this way).
///
/// **Deliberately does not call `GetShardIterator`/`GetRecords` here.**
/// Verified manually against a live cluster while building the dashboard
/// change: `GetRecords` on a sealed shard from a control-only node panics
/// inside `ClientCtx::data()` (an empty/dropped HTTP reply, not a JSON
/// error — `ClientCtx::data()`'s own doc says exactly this must never be
/// reachable from a client-dispatch path, but `dynamo_streams::
/// get_records_sealed` calls it unconditionally), and the open-shard path
/// (`GetShardIterator{LATEST}`/`GetRecords`) stalls for the full
/// `SCHEMA_COMMIT_TIMEOUT` (~10s) before failing, since a control-only
/// node's blind-forward routing fallback has no leader-hint to chase. Both
/// are pre-existing backend gaps this PR's dashboard change works around
/// (the Streams view never dials either op from a control-only console) but
/// deliberately does not fix — see `docs/engineering-lessons.md` and
/// `dashboard_streams.js`'s own doc. Pinning down the panic's exact HTTP
/// behavior in an automated test is left to whichever follow-up PR fixes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn control_node_streams_read_path_is_ground_truth() {
    timeout(Duration::from_secs(60), async {
        let dir = support::panic_safe_tempdir();
        let (control_nodes, data_nodes, _config) = support::bring_up_split(1, 1, dir.path()).await;
        support::await_leader(&control_nodes).await;
        let data_raftkv_ids: Vec<animus_env::NodeId> =
            (1..2).map(animusd::config::node_id).collect();
        support::await_data_nodes_active(&control_nodes, &data_raftkv_ids).await;

        let control_admin = control_nodes[0].admin_addr();
        let data_dynamo = data_nodes[0].dynamo_addr();

        // An ENABLED stream (stays open — no seal).
        let (s, body) = raw_post(
            data_dynamo,
            "/",
            "X-Amz-Target: DynamoDB_20120810.CreateTable",
            r#"{"TableName":"OpenT","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
                "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
                "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
        )
        .await;
        assert_eq!(s, 200, "{body}");
        let (s, body) = raw_post(
            data_dynamo,
            "/",
            "X-Amz-Target: DynamoDB_20120810.PutItem",
            r#"{"TableName":"OpenT","Item":{"pk":{"S":"k1"}}}"#,
        )
        .await;
        assert_eq!(s, 200, "{body}");

        // A stream forced sealed via disable (F12-b's final seal) — a real
        // sealed catalog row with no small seal-threshold knob needed.
        let (s, body) = raw_post(
            data_dynamo,
            "/",
            "X-Amz-Target: DynamoDB_20120810.CreateTable",
            r#"{"TableName":"SealedT","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
                "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
                "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
        )
        .await;
        assert_eq!(s, 200, "{body}");
        let (s, body) = raw_post(
            data_dynamo,
            "/",
            "X-Amz-Target: DynamoDB_20120810.PutItem",
            r#"{"TableName":"SealedT","Item":{"pk":{"S":"k1"}}}"#,
        )
        .await;
        assert_eq!(s, 200, "{body}");
        let (s, body) = raw_post(
            data_dynamo,
            "/",
            "X-Amz-Target: DynamoDB_20120810.UpdateTable",
            r#"{"TableName":"SealedT","StreamSpecification":{"StreamEnabled":false}}"#,
        )
        .await;
        assert_eq!(s, 200, "{body}");

        // ---- ground truth on the CONTROL-ONLY node's own admin port -------

        // `/admin/status` mirrors the full replicated catalog: both streams'
        // specs/rows, converged-or-timeout (the seal is itself async).
        timeout(Duration::from_secs(20), async {
            loop {
                let (s, _, body) = raw(control_admin, "GET", "/admin/status").await;
                assert_eq!(s, 200);
                let status: Value = serde_json::from_str(&body).expect("status is JSON");
                let tables = &status["schemas"]["tables"];
                let open_ok = tables["OpenT"]["stream"]["label"].is_string();
                let sealed_row_present = status["stream_shards"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .any(|r| r["table"].as_str() == Some("SealedT") && !r["expired"].as_bool().unwrap_or(true));
                if open_ok && sealed_row_present {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("control node's /admin/status never converged to the sealed row in 20s");

        // `ListStreams` through the admin proxy (`/admin/data/dynamo`) —
        // metadata-only, so it must be exact, not eventually-consistent.
        let (s, body) = raw_post(
            control_admin,
            "/admin/data/dynamo",
            "",
            r#"{"op":"ListStreams","payload":{}}"#,
        )
        .await;
        assert_eq!(s, 200, "{body}");
        let list: Value = serde_json::from_str(&body).expect("ListStreams body is JSON");
        let streams = list["Streams"].as_array().expect("Streams array");
        let names: Vec<&str> = streams
            .iter()
            .filter_map(|s| s["TableName"].as_str())
            .collect();
        assert!(
            names.contains(&"OpenT") && names.contains(&"SealedT"),
            "ListStreams from the control-only node's admin proxy lists both \
             streams: {body}"
        );

        // `DescribeStream` on each — open stream has exactly one shard with
        // no `EndingSequenceNumber`; the sealed one has exactly one shard
        // WITH an `EndingSequenceNumber` and `StreamStatus: DISABLED`.
        let open_arn = streams
            .iter()
            .find(|s| s["TableName"].as_str() == Some("OpenT"))
            .and_then(|s| s["StreamArn"].as_str())
            .expect("OpenT's stream ARN")
            .to_string();
        let sealed_arn = streams
            .iter()
            .find(|s| s["TableName"].as_str() == Some("SealedT"))
            .and_then(|s| s["StreamArn"].as_str())
            .expect("SealedT's stream ARN")
            .to_string();

        let (s, body) = raw_post(
            control_admin,
            "/admin/data/dynamo",
            "",
            &format!(r#"{{"op":"DescribeStream","payload":{{"StreamArn":"{open_arn}"}}}}"#),
        )
        .await;
        assert_eq!(s, 200, "{body}");
        let desc: Value = serde_json::from_str(&body).expect("DescribeStream body is JSON");
        let sd = &desc["StreamDescription"];
        assert_eq!(sd["StreamStatus"], "ENABLED", "{body}");
        let shards = sd["Shards"].as_array().expect("Shards array");
        assert_eq!(shards.len(), 1, "{body}");
        assert!(
            shards[0]["SequenceNumberRange"]["EndingSequenceNumber"].is_null(),
            "OpenT's own shard is genuinely open (no EndingSequenceNumber): {body}"
        );

        let (s, body) = raw_post(
            control_admin,
            "/admin/data/dynamo",
            "",
            &format!(r#"{{"op":"DescribeStream","payload":{{"StreamArn":"{sealed_arn}"}}}}"#),
        )
        .await;
        assert_eq!(s, 200, "{body}");
        let desc: Value = serde_json::from_str(&body).expect("DescribeStream body is JSON");
        let sd = &desc["StreamDescription"];
        assert_eq!(sd["StreamStatus"], "DISABLED", "{body}");
        let shards = sd["Shards"].as_array().expect("Shards array");
        assert_eq!(shards.len(), 1, "{body}");
        assert!(
            shards[0]["SequenceNumberRange"]["EndingSequenceNumber"].is_string(),
            "SealedT's own shard is genuinely sealed (has an EndingSequenceNumber): {body}"
        );

        for node in control_nodes.iter().chain(data_nodes.iter()) {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}

/// docs/roadmap.md U-01 (render-only dashboard fixes, no backend change) —
/// one assertion group per bullet, all against the served static assets
/// (this is a render-only change: the JSON they consume is already covered
/// by `admin_endpoint.rs`). A single-node cluster is enough for every one of
/// these; nothing here needs a multi-node fan-out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dashboard_u01_render_only_fixes() {
    timeout(Duration::from_secs(60), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, _config) = bring_up(1, dir.path()).await;
        await_bootstrap(&nodes).await;
        let admin_addr = nodes[0].admin_addr();

        // ---- 1. Transactions tab over /admin/txns (CpTxnView) --------------
        let (s, _, shell) = raw(admin_addr, "GET", "/").await;
        assert_eq!(s, 200);
        assert!(
            shell.contains(r#"data-tab="txns""#) && shell.contains(r#"<section id="txns""#),
            "the shell carries the Transactions nav link and section: {shell}"
        );
        assert!(
            shell.contains("dashboard_txns.js"),
            "the shell references the Transactions view's script asset: {shell}"
        );
        let (s, _, txns_js) = raw(admin_addr, "GET", "/admin/ui/dashboard_txns.js").await;
        assert_eq!(s, 200, "dashboard_txns.js is served");
        assert!(
            txns_js.contains("function renderTxns") && txns_js.contains("txnViewsByTablet"),
            "dashboard_txns.js renders the per-hosted-tablet transaction-tracker view: {txns_js}"
        );
        let (s, _, core_js) = raw(admin_addr, "GET", "/admin/ui/dashboard_core.js").await;
        assert_eq!(s, 200, "dashboard_core.js is served");
        assert!(
            core_js.contains("/admin/txns") && core_js.contains("function txnViewsByTablet"),
            "dashboard_core.js fans out /admin/txns and merges it cluster-wide: {core_js}"
        );
        assert!(
            core_js.contains(
                r#"control: ["overview", "placement", "tablets", "txns", "browser", "streams", "storage", "backups"]"#
            ) && core_js.contains(
                r#"combined: ["overview", "placement", "tablets", "txns", "browser", "streams", "storage", "backups", "node"]"#
            ),
            "the Transactions tab is role-gated exactly like Tablets (ROLE_TABS): {core_js}"
        );
        let (s, _, txns_body) = raw(admin_addr, "GET", "/admin/txns").await;
        assert_eq!(s, 200, "GET /admin/txns: {txns_body}");
        let txns_json: Value = serde_json::from_str(&txns_body).expect("/admin/txns is JSON");
        assert!(
            txns_json.get("groups").is_some(),
            "the CpTxnView list is under \"groups\", the same shape dashboard_txns.js reads: {txns_body}"
        );

        // ---- 2. Full per-group Raft detail in renderTabletDetail -----------
        let (s, _, tablets_js) = raw(admin_addr, "GET", "/admin/ui/dashboard_tablets.js").await;
        assert_eq!(s, 200, "dashboard_tablets.js is served");
        for field in [
            "commit_index",
            "durable_index",
            "snapshot_index",
            "log_len",
            "g.voters",
            "g.learners",
        ] {
            assert!(
                tablets_js.contains(field),
                "renderTabletDetail renders CpRaftView's own {field}: {tablets_js}"
            );
        }

        // ---- 3. believes_alive badge in renderOverview ----------------------
        let (s, _, overview_js) = raw(admin_addr, "GET", "/admin/ui/dashboard_overview.js").await;
        assert_eq!(s, 200, "dashboard_overview.js is served");
        assert!(
            overview_js.contains("believes_alive") && overview_js.contains("believesAlive"),
            "renderOverview surfaces the control leader's own believes_alive verdict per member: {overview_js}"
        );

        // ---- 4. Sparklines from /admin/metrics/history as a shared component,
        //         charting the six CP read-path counters on Overview --------
        let (s, _, core_js) = raw(admin_addr, "GET", "/admin/ui/dashboard_core.js").await;
        assert_eq!(s, 200, "dashboard_core.js is served");
        assert!(
            core_js.contains("function sparkline"),
            "dashboard_core.js defines a shared sparkline() component: {core_js}"
        );
        assert!(
            core_js.contains("/admin/metrics/history"),
            "dashboard_core.js fetches this node's own metrics-history ring: {core_js}"
        );
        assert!(
            overview_js.contains("sparkline("),
            "renderOverview renders sparklines: {overview_js}"
        );
        for counter in [
            "cp_read_barriers_served",
            "cp_read_barriers_timed_out",
            "cp_eventual_reads_local",
            "cp_eventual_reads_forwarded",
            "cp_eventual_reads_fell_back",
            "cp_uncertainty_restarts",
        ] {
            assert!(
                overview_js.contains(counter),
                "the Overview read-path sparklines chart {counter}: {overview_js}"
            );
        }
        // The route itself already serves real samples the sparklines can
        // read (`admin_endpoint.rs` covers `/admin/metrics/history`'s own
        // shape more thoroughly; this just proves the render-only wiring
        // reaches a real 200).
        let (s, _, history_body) = raw(admin_addr, "GET", "/admin/metrics/history").await;
        assert_eq!(s, 200, "GET /admin/metrics/history: {history_body}");
        assert!(
            serde_json::from_str::<Value>(&history_body)
                .expect("metrics history is JSON")
                .get("samples")
                .is_some(),
            "the ring buffer is served under \"samples\": {history_body}"
        );

        // ---- 5. SYSTEM_TABLE_KINDS extended to all 16 EntityKind variants --
        let (s, _, storage_js) = raw(admin_addr, "GET", "/admin/ui/dashboard_storage.js").await;
        assert_eq!(s, 200, "dashboard_storage.js is served");
        // Pin the expected 16 segment strings by constructing every real
        // `EntityKind` variant and reading its own `as_str()` — this crate
        // has no `EntityKind::ALL`/iterator to derive the list from
        // (`syskv.rs`), so a future 17th variant needs both this array and
        // `SYSTEM_TABLE_KINDS` updated by hand; this at least proves the 16
        // that exist today are exactly the 16 the dropdown lists, spelled
        // exactly the way `EntityKind::from_segment` expects them back.
        use animus_control::syskv::EntityKind;
        let expected_kinds: [&str; 16] = [
            EntityKind::Tablet.as_str(),
            EntityKind::Member.as_str(),
            EntityKind::Schema.as_str(),
            EntityKind::Policy.as_str(),
            EntityKind::NodeAddrs.as_str(),
            EntityKind::Counter.as_str(),
            EntityKind::CpMemberAddr.as_str(),
            EntityKind::StreamShard.as_str(),
            EntityKind::IndexBackfill.as_str(),
            EntityKind::SplitLineage.as_str(),
            EntityKind::SplitPlacing.as_str(),
            EntityKind::Backup.as_str(),
            EntityKind::BackupProgress.as_str(),
            EntityKind::Restore.as_str(),
            EntityKind::PitrSegment.as_str(),
            EntityKind::PitrBaseBackup.as_str(),
        ];
        for kind in expected_kinds {
            assert!(
                storage_js.contains(&format!("[\"{kind}\",")),
                "SYSTEM_TABLE_KINDS lists the real EntityKind segment {kind:?}: {storage_js}"
            );
            // `EntityKind::from_segment` recognizes every one of these, so a
            // round trip through it is a live cross-check that the pinned
            // literal actually decodes back to the variant it came from —
            // not just a string that happens to match.
            assert!(
                EntityKind::from_segment(kind.as_bytes()).is_some(),
                "{kind:?} round-trips through EntityKind::from_segment"
            );
        }
        assert!(
            !storage_js.contains("[\"keyspace\","),
            "the stray [\"keyspace\", ...] dropdown entry (never a real EntityKind \
             segment, always returned zero rows) is dropped, not carried forward: {storage_js}"
        );

        nodes[0].shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// docs/roadmap.md U-02: the Backups tab — a render-only assertion group,
/// mirroring `dashboard_u01_render_only_fixes`' own structure (this is a
/// render-only change over already-covered JSON: `/admin/backups`'s and
/// `/admin/restores`'s own shapes are proven end to end elsewhere —
/// `admin_endpoint.rs::admin_backups_view_reflects_the_catalog` and
/// `dynamo_restore.rs`'s own `/admin/restores` assertion — so this file only
/// proves the dashboard surface: the shell/script markers, the four gated
/// actions' real DynamoDB op names/payload shapes (`CreateBackup`/
/// `DeleteBackup`/`RestoreTableFromBackup`/`UpdateContinuousBackups`, each
/// behind a `window.confirm`), role gating (control/combined, exactly like
/// Placement — asserted above in `dashboard_role_gating_split_deployment`,
/// which this PR's own diff to that test's `ROLE_TABS` string already
/// covers the data-only exclusion for), and that both routes actually 200
/// from a live node). No proxy-allowlist change was needed for this PR —
/// `admin.rs::action_data_dynamo` has no op allowlist beyond the bare-name
/// Streams-vs-item disambiguation (`STREAMS_OPS`), and none of these four
/// ops are Streams ops, so each resolves to the ordinary
/// `DynamoDB_20120810.<op>` item-API target and reaches
/// `animus_dynamo::wire::decode_request` unchanged — already exercised by
/// `dynamo_backup.rs`/`dynamo_restore.rs`/`dynamo_pitr.rs` over the real
/// wire edge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dashboard_u02_backups_tab() {
    timeout(Duration::from_secs(60), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, _config) = bring_up(1, dir.path()).await;
        await_bootstrap(&nodes).await;
        let admin_addr = nodes[0].admin_addr();

        // ---- the shell carries the Backups nav link + section --------------
        let (s, _, shell) = raw(admin_addr, "GET", "/").await;
        assert_eq!(s, 200);
        assert!(
            shell.contains(r#"data-tab="backups""#) && shell.contains(r#"<section id="backups""#),
            "the shell carries the Backups nav link and section: {shell}"
        );
        assert!(
            shell.contains("dashboard_backups.js"),
            "the shell references the Backups view's script asset: {shell}"
        );
        assert!(
            shell.contains(r#"id="bk-create-table""#)
                && shell.contains(r#"id="bk-list-body""#)
                && shell.contains(r#"id="bk-pitr-body""#)
                && shell.contains(r#"id="bk-restores-body""#),
            "the shell carries the Create-backup form, the backup list, the \
             per-table PITR toggle list, and the restores list: {shell}"
        );

        // ---- dashboard_backups.js renders the catalog + the four gated
        //      actions with the real DynamoDB op names/payload field names --
        let (s, _, backups_js) = raw(admin_addr, "GET", "/admin/ui/dashboard_backups.js").await;
        assert_eq!(s, 200, "dashboard_backups.js is served");
        assert!(
            backups_js.contains("function renderBackups"),
            "dashboard_backups.js renders the backup/restore/PITR catalogs: {backups_js}"
        );
        for (op, field) in [
            ("CreateBackup", "BackupName"),
            ("DeleteBackup", "BackupArn"),
            ("RestoreTableFromBackup", "TargetTableName"),
            ("UpdateContinuousBackups", "PointInTimeRecoveryEnabled"),
        ] {
            assert!(
                backups_js.contains(op) && backups_js.contains(field),
                "dashboard_backups.js posts {op} with its real payload field \
                 {field}: {backups_js}"
            );
        }
        assert!(
            backups_js.contains("PointInTimeRecoverySpecification"),
            "UpdateContinuousBackups nests PointInTimeRecoveryEnabled under \
             PointInTimeRecoverySpecification, matching the wire decoder: {backups_js}"
        );
        assert_eq!(
            backups_js.matches("if (!window.confirm(").count(),
            4,
            "each of the four actions (create/delete/restore/PITR toggle) is \
             gated behind its own window.confirm, the crate's one mutation \
             idiom (the module doc comment's own mention of window.confirm \
             is deliberately excluded by this narrower pattern): {backups_js}"
        );
        assert!(
            backups_js.contains("/admin/data/dynamo"),
            "every action posts through the existing dashboard dynamo proxy, \
             never a new endpoint: {backups_js}"
        );
        assert!(
            backups_js.contains("dynamoTables()"),
            "the Create-backup table picker and the PITR table list reuse the \
             Data Browser's own table source rather than a second fetch: {backups_js}"
        );

        // ---- dashboard_core.js fetches both catalogs alongside /admin/status,
        //      not a per-node fan-out (unlike /admin/txns) ---------------------
        let (s, _, core_js) = raw(admin_addr, "GET", "/admin/ui/dashboard_core.js").await;
        assert_eq!(s, 200, "dashboard_core.js is served");
        assert!(
            core_js.contains("/admin/backups") && core_js.contains("/admin/restores"),
            "dashboard_core.js fetches both catalogs once against SEED: {core_js}"
        );
        assert!(
            core_js.contains(
                r#"control: ["overview", "placement", "tablets", "txns", "browser", "streams", "storage", "backups"]"#
            ) && !core_js.contains(r#"data: ["node", "browser", "streams", "backups"]"#),
            "Backups is role-gated to control + combined exactly like Placement \
             — absent from the data role's own tab list: {core_js}"
        );

        // ---- both routes actually serve from a live node --------------------
        let (s, _, backups_body) = raw(admin_addr, "GET", "/admin/backups").await;
        assert_eq!(s, 200, "GET /admin/backups: {backups_body}");
        assert!(
            serde_json::from_str::<Value>(&backups_body)
                .expect("backups view is JSON")
                .get("backups")
                .is_some(),
            "the catalog is served under \"backups\", the shape dashboard_backups.js reads: {backups_body}"
        );
        let (s, _, restores_body) = raw(admin_addr, "GET", "/admin/restores").await;
        assert_eq!(s, 200, "GET /admin/restores: {restores_body}");
        assert!(
            serde_json::from_str::<Value>(&restores_body)
                .expect("restores view is JSON")
                .get("restores")
                .is_some(),
            "the catalog is served under \"restores\", the shape dashboard_backups.js reads: {restores_body}"
        );

        nodes[0].shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// docs/roadmap.md U-04 (PR 1): the Data Browser's `#br-dy-ttl` row, beside
/// `#br-dy-stream` — same render-only-markers-plus-live-round-trip structure
/// as `dashboard_u01_render_only_fixes`/`dashboard_u02_backups_tab`. The
/// actual `UpdateTimeToLive`/`DescribeTimeToLive` wire mechanics already have
/// their own full end-to-end coverage (`tests/dynamo_ttl.rs`), so this test
/// only proves: the shell carries the new row beside the Stream row,
/// `dashboard_browser.js` renders it from `schema.ttl` (the same
/// already-fetched `/admin/status` fact `dynamo::describe_time_to_live`
/// itself reads) and posts the real `UpdateTimeToLive` op/payload shape
/// behind `window.confirm`, and that exact shape — enable, then disable
/// with the same `AttributeName` AWS requires on both calls — is accepted
/// by the real wire through the admin proxy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dashboard_u04_ttl_row() {
    timeout(Duration::from_secs(60), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, _config) = bring_up(1, dir.path()).await;
        await_bootstrap(&nodes).await;
        let admin_addr = nodes[0].admin_addr();

        // ---- the shell carries #br-dy-ttl beside #br-dy-stream -------------
        let (s, _, shell) = raw(admin_addr, "GET", "/").await;
        assert_eq!(s, 200);
        let stream_pos = shell
            .find(r#"id="br-dy-stream""#)
            .expect("shell carries #br-dy-stream");
        let ttl_pos = shell
            .find(r#"id="br-dy-ttl""#)
            .expect("shell carries #br-dy-ttl");
        assert!(
            ttl_pos > stream_pos && ttl_pos - stream_pos < 200,
            "#br-dy-ttl sits immediately beside #br-dy-stream: {shell}"
        );

        // ---- dashboard_browser.js renders it and posts the real op ---------
        let (s, _, browser_js) = raw(admin_addr, "GET", "/admin/ui/dashboard_browser.js").await;
        assert_eq!(s, 200, "dashboard_browser.js is served");
        assert!(
            browser_js.contains("function renderTtlRow")
                && browser_js.contains("function enableTtl")
                && browser_js.contains("function disableTtl"),
            "dashboard_browser.js defines the TTL row's render + enable/disable handlers: {browser_js}"
        );
        assert!(
            browser_js.contains("schema.ttl"),
            "renderTtlRow reads the already-fetched schema.ttl, no extra DescribeTimeToLive round trip: {browser_js}"
        );
        assert!(
            browser_js.contains("UpdateTimeToLive")
                && browser_js.contains("TimeToLiveSpecification")
                && browser_js.contains("AttributeName"),
            "the TTL row posts the real UpdateTimeToLive op with its real payload shape: {browser_js}"
        );
        assert!(
            browser_js.contains("Enable TTL on") && browser_js.contains("Disable TTL on"),
            "both enable and disable are gated behind their own window.confirm: {browser_js}"
        );

        // ---- the exact payload shape round-trips through the real wire -----
        let (s, ct_body) = raw_post(
            admin_addr,
            "/admin/data/dynamo",
            "",
            r#"{"op":"CreateTable","payload":{"TableName":"widgets","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],"AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]}}"#,
        )
        .await;
        assert_eq!(s, 200, "CreateTable widgets: {ct_body}");

        let (s, en_body) = raw_post(
            admin_addr,
            "/admin/data/dynamo",
            "",
            r#"{"op":"UpdateTimeToLive","payload":{"TableName":"widgets","TimeToLiveSpecification":{"Enabled":true,"AttributeName":"expiresAt"}}}"#,
        )
        .await;
        assert_eq!(s, 200, "enable TTL: {en_body}");

        let (s, _, status_body) = raw(admin_addr, "GET", "/admin/status").await;
        assert_eq!(s, 200);
        let status: Value = serde_json::from_str(&status_body).expect("status is JSON");
        assert_eq!(
            status["schemas"]["tables"]["widgets"]["ttl"]["attribute_name"].as_str(),
            Some("expiresAt"),
            "TTL is enabled with the declared attribute: {status_body}"
        );

        let (s, dis_body) = raw_post(
            admin_addr,
            "/admin/data/dynamo",
            "",
            r#"{"op":"UpdateTimeToLive","payload":{"TableName":"widgets","TimeToLiveSpecification":{"Enabled":false,"AttributeName":"expiresAt"}}}"#,
        )
        .await;
        assert_eq!(s, 200, "disable TTL: {dis_body}");

        let (s, _, status_body) = raw(admin_addr, "GET", "/admin/status").await;
        assert_eq!(s, 200);
        let status: Value = serde_json::from_str(&status_body).expect("status is JSON");
        assert!(
            status["schemas"]["tables"]["widgets"]["ttl"].is_null(),
            "TTL is disabled: {status_body}"
        );

        nodes[0].shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// docs/roadmap.md U-04 (PR 2): the create-table form declares GSIs/LSIs/a
/// stream/TTL in one place. Same structure as `dashboard_u04_ttl_row` above:
/// shell/script markers proving the new fields and their client-side
/// validation exist, then a live round trip posting the exact
/// `CreateTable` + follow-up `UpdateTimeToLive` request sequence the
/// extended `submitTableForm` builds (mirroring `console::ConsoleBackend::
/// create_table`'s own sequence, `crates/animusd/src/lib.rs`) and checking
/// the real catalog reflects every declared piece.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dashboard_u04_create_table_form() {
    timeout(Duration::from_secs(60), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, _config) = bring_up(1, dir.path()).await;
        await_bootstrap(&nodes).await;
        let admin_addr = nodes[0].admin_addr();

        // ---- the shell carries the new form fields --------------------------
        let (s, _, shell) = raw(admin_addr, "GET", "/").await;
        assert_eq!(s, 200);
        for id in [
            "br-dy-ct-lsi-table",
            "br-dy-ct-lsi-add",
            "br-dy-ct-gsi-table",
            "br-dy-ct-gsi-add",
            "br-dy-ct-stream",
            "br-dy-ct-stream-vt",
            "br-dy-ct-ttl",
            "br-dy-ct-ttl-attr",
        ] {
            assert!(
                shell.contains(&format!(r#"id="{id}""#)),
                "the create-table form carries #{id}: {shell}"
            );
        }

        // ---- dashboard_browser.js builds the real request sequence ----------
        let (s, _, browser_js) = raw(admin_addr, "GET", "/admin/ui/dashboard_browser.js").await;
        assert_eq!(s, 200, "dashboard_browser.js is served");
        assert!(
            browser_js.contains("function addCtLsiRow") && browser_js.contains("function addCtGsiRow"),
            "the form's GSI/LSI row editors exist: {browser_js}"
        );
        assert!(
            browser_js.contains("GlobalSecondaryIndexes")
                && browser_js.contains("LocalSecondaryIndexes")
                && browser_js.contains("StreamSpecification"),
            "submitTableForm sends GSIs, LSIs, and a stream spec on CreateTable: {browser_js}"
        );
        // Client-side validation, mirroring ConsoleBackend::create_table's own
        // checks (lib.rs) so a mistake bounces here, not off the wire.
        for marker in [
            "needs a hash attribute",
            "needs a sort key attribute",
            "requires the table to have its own sort key",
            "INCLUDE projection needs at least one attribute",
        ] {
            assert!(
                browser_js.contains(marker),
                "submitTableForm validates {marker:?} client-side: {browser_js}"
            );
        }
        // Every key attribute (base + every declared index) ends up in
        // AttributeDefinitions before the request is ever sent.
        assert!(
            browser_js.contains("AttributeDefinitions") && browser_js.contains("declareDefault"),
            "the form declares AttributeDefinitions for every base and index key: {browser_js}"
        );

        // ---- the exact request sequence a filled-in form would send --------
        let (s, ct_body) = raw_post(
            admin_addr,
            "/admin/data/dynamo",
            "",
            r#"{"op":"CreateTable","payload":{
                "TableName":"orders",
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"},{"AttributeName":"created_at","KeyType":"RANGE"}],
                "GlobalSecondaryIndexes":[{"IndexName":"by-status","KeySchema":[{"AttributeName":"status","KeyType":"HASH"}]}],
                "LocalSecondaryIndexes":[{"IndexName":"by-score","KeySchema":[{"AttributeName":"id","KeyType":"HASH"},{"AttributeName":"score","KeyType":"RANGE"}]}],
                "AttributeDefinitions":[
                    {"AttributeName":"id","AttributeType":"S"},
                    {"AttributeName":"created_at","AttributeType":"N"},
                    {"AttributeName":"status","AttributeType":"S"},
                    {"AttributeName":"score","AttributeType":"S"}
                ],
                "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}
            }}"#,
        )
        .await;
        assert_eq!(s, 200, "CreateTable orders: {ct_body}");

        let (s, ttl_body) = raw_post(
            admin_addr,
            "/admin/data/dynamo",
            "",
            r#"{"op":"UpdateTimeToLive","payload":{"TableName":"orders","TimeToLiveSpecification":{"Enabled":true,"AttributeName":"expiresAt"}}}"#,
        )
        .await;
        assert_eq!(s, 200, "enable TTL on orders: {ttl_body}");

        let (s, _, status_body) = raw(admin_addr, "GET", "/admin/status").await;
        assert_eq!(s, 200);
        let status: Value = serde_json::from_str(&status_body).expect("status is JSON");
        let schema = &status["schemas"]["tables"]["orders"];
        let indexes = schema["indexes"].as_array().expect("indexes array");
        assert!(
            indexes
                .iter()
                .any(|i| i["name"] == "by-status" && i["kind"] == "Global" && i["hash_attribute"] == "status"),
            "the GSI is declared: {schema}"
        );
        assert!(
            indexes
                .iter()
                .any(|i| i["name"] == "by-score" && i["kind"] == "Local" && i["sort_attribute"] == "score"),
            "the LSI is declared: {schema}"
        );
        assert_eq!(
            schema["stream"]["view_type"].as_str(),
            Some("NewAndOldImages"),
            "the stream is declared: {schema}"
        );
        assert_eq!(
            schema["ttl"]["attribute_name"].as_str(),
            Some("expiresAt"),
            "TTL is declared via the follow-up call: {schema}"
        );

        nodes[0].shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}
