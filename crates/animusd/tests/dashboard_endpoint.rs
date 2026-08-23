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
        let dir = tempfile::tempdir().unwrap();
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
                && core_js.contains(r#"data: ["node", "browser", "streams"]"#),
            "dashboard_core.js defines the per-role tab gating, including the \
             data role's node-first tab list (now with Streams, ADR 0042/0043): {core_js}"
        );
        assert!(
            core_js.contains(
                r#"control: ["overview", "placement", "tablets", "browser", "streams", "storage"]"#
            ),
            "the control role's own tab list now includes Streams too (a control-only \
             node holds the full replicated Metadata, so the stream list + shard-chain \
             detail render truthfully there; only the live-tail poller degrades): {core_js}"
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
        let dir = tempfile::tempdir().unwrap();
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
