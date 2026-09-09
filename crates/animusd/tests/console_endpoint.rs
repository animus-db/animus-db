//! End-to-end test of animusd console's (ADR 0052's "AnimusDB Data Console")
//! listener over
//! real TCP (`ProdEnv`): the shell 200s as `text/html` and names itself, both
//! static assets (CSS + the PR2 tables-list JS) 200 with the right content
//! type, a `/console/ui/*` deep link returns the identical shell (the
//! client-side router in `console.js`, not the server, decides what that
//! path renders), the bound port matches what the config says, and (the
//! deliberate non-goal this ADR calls out) a **control-only** node never
//! binds one at all. The tables-list **JSON endpoint's own projection
//! correctness** (key shapes, GSI/LSI counts, stream/TTL, hidden-table
//! exclusion, the no-cluster-shape property) is a separate concern, now
//! covered by `sim_cluster_console.rs::
//! tables_endpoint_projects_the_schema_catalog_correctly` (ADR 0061 rung H,
//! C-08 PR 3 — the real-socket original, `tests/console_tables.rs`, was
//! deleted whole once that sibling landed) — this file only proves the
//! endpoint exists and serves valid JSON.
//!
//! Real time + sockets, so it brings the cluster up with the documented
//! port-TOCTOU bounded retry (`support::bring_up_split`/`support::
//! start_single_node`) rather than a fixed-port config.
//!
//! **ADR 0061 rung H, C-08 PR 3**: every test in this file stayed `ProdEnv`
//! at the time — each is fundamentally about real HTTP framing (status
//! line, headers, content-type, static-asset bytes) or a genuine process
//! role split, neither of which `SimCluster::console` can reproduce (it
//! builds a bare `HttpRequest` with no framing at all). The first test's
//! own tail (`GET /console/api/tables` on a freshly-booted node returns an
//! empty array; an unrecognized path 404s) is genuinely JSON-routing, not
//! framing — that slice now has its own sibling, `sim_cluster_console.rs::
//! console_error_mapping_and_json_routing_assertions`, extended with the
//! console's error-mapping contract (missing table -> 404, malformed body
//! -> 400) — but the rest of that test (the shell/static-asset/deep-link
//! assertions) keeps it whole on `ProdEnv`.
//!
//! **ADR 0061 rung L, C-12 PR 4c**: `SimCluster` gained per-node `NodeRole`
//! in C-12 PRs 2/3 — the "has no node-role concept" half of PR 3's own
//! reasoning above no longer holds. Both of this file's remaining two
//! tests now have deterministic siblings in `sim_cluster_console.rs`
//! (`console_reachable_on_a_data_only_node`/`console_dispatch_from_a_
//! control_only_node_forwards_to_the_data_node`) proving the underlying,
//! role-driven `ClientCtx`/dispatch behavior — but **both stay whole,
//! real-socket, here too**: each test's own defining assertion is a
//! genuine `Node`-level listener-binding/HTTP-framing fact (the console
//! port is bound at all on a data-only node; `Node::console_addr()`
//! panics with "this node has no data role" on a control-only node) that
//! `SimCluster` cannot reproduce regardless of role support — it has no
//! `Node` struct and binds no real listener for any role. See each test's
//! own doc comment, and the sim module's own "PR 4c" doc section, for the
//! exact classification.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

mod support;

/// One HTTP/1.0 GET; returns `(status, raw header block, body)` — mirrors
/// `dashboard_endpoint.rs::raw`.
async fn raw(addr: SocketAddr, path: &str) -> (u16, String, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to console");
    let request = format!("GET {path} HTTP/1.0\r\nHost: animus\r\nConnection: close\r\n\r\n");
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

/// The shell 200s as `text/html` and names itself, both static assets 200,
/// a deep link under `/console/ui/` returns the identical shell (the same
/// static bytes — routing happens client-side), the tables JSON endpoint
/// 200s with a valid `{"tables": [...]}` body, and the bound port is exactly
/// the one `RoleAddrs::console` named in the config — on a **combined**
/// node.
///
/// **KEPT `ProdEnv`** (ADR 0061 rung H, C-08 PR 3): real HTTP framing —
/// status line, `Content-Type` headers, static-asset bytes, deep-link
/// routing — none of which `SimCluster::console` can reproduce (no framing
/// at all). Its own JSON-routing tail has a sim sibling; see this file's
/// own module doc.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn console_serves_shell_assets_and_deep_links_on_combined_node() {
    timeout(Duration::from_secs(30), async {
        let dir = support::panic_safe_tempdir();
        let (node, config) =
            support::start_single_node(dir.path(), animusd::StorageBackend::Memory).await;

        assert_eq!(
            node.console_addr(),
            config.nodes[0].console,
            "the bound console port matches what the config named"
        );

        // ---- the shell ------------------------------------------------
        let (status, head, body) = raw(node.console_addr(), "/").await;
        assert_eq!(status, 200, "the shell 200s");
        assert!(
            head.to_ascii_lowercase()
                .contains("content-type: text/html"),
            "the shell is text/html, headers:\n{head}"
        );
        assert!(
            body.contains("animusd console"),
            "the shell names itself: {body}"
        );
        assert!(
            body.contains("console.js"),
            "the shell loads the console's client-side app: {body}"
        );

        // ---- its static assets -----------------------------------------
        let (status, head, css) = raw(node.console_addr(), "/console/ui/console.css").await;
        assert_eq!(status, 200, "the stylesheet asset 200s");
        assert!(
            head.to_ascii_lowercase().contains("content-type: text/css"),
            "the asset is text/css, headers:\n{head}"
        );
        assert!(!css.is_empty(), "the stylesheet has content");

        let (status, head, js) = raw(node.console_addr(), "/console/ui/console.js").await;
        assert_eq!(status, 200, "the tables-list JS asset 200s");
        assert!(
            head.to_ascii_lowercase()
                .contains("content-type: text/javascript"),
            "the asset is text/javascript, headers:\n{head}"
        );
        assert!(!js.is_empty(), "the script has content");

        // ---- a deep link returns the identical shell -------------------
        let (status, head, deep_body) = raw(node.console_addr(), "/console/ui/tables").await;
        assert_eq!(status, 200, "a /console/ui/* deep link 200s");
        assert!(
            head.to_ascii_lowercase()
                .contains("content-type: text/html"),
            "the deep link is text/html, headers:\n{head}"
        );
        assert_eq!(
            deep_body, body,
            "the deep link serves the exact same shell as the root"
        );

        // ---- the tables-list JSON endpoint (PR2) ------------------------
        let (status, head, api_body) = raw(node.console_addr(), "/console/api/tables").await;
        assert_eq!(status, 200, "the tables endpoint 200s");
        assert!(
            head.to_ascii_lowercase()
                .contains("content-type: application/json"),
            "the tables endpoint is JSON, headers:\n{head}"
        );
        let parsed: serde_json::Value = serde_json::from_str(&api_body).expect("valid JSON body");
        assert_eq!(
            parsed["tables"],
            serde_json::json!([]),
            "a freshly-booted node has no tables yet: {api_body}"
        );

        // ---- an unknown path still 404s ---------------------------------
        let (status, _, _) = raw(node.console_addr(), "/console/api/nonexistent").await;
        assert_eq!(status, 404, "an unrecognized path still 404s");

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// The console listener is bound on a **data-only** node too (it hosts real
/// CP-data tablets, the console's subject matter) — verified against a
/// genuine split deployment, not assumed from the combined-node case above.
///
/// **KEPT `ProdEnv`** (ADR 0061 rung L, C-12 PR 4c): the literal listener-
/// binding/HTTP-framing proof (a real port is bound, `GET /` 200s as
/// `text/html`) is a `Node`-level fact `SimCluster` cannot reproduce — it
/// binds no real listener for any role. The underlying JSON-dispatch half
/// — the console backend genuinely answers real, converged cluster state
/// once driven from a data-only node's own `ClientCtx` — now has a
/// deterministic sibling: `sim_cluster_console.rs::
/// console_reachable_on_a_data_only_node`.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn console_serves_shell_on_data_only_node() {
    timeout(Duration::from_secs(60), async {
        let dir = support::panic_safe_tempdir();
        let (control_nodes, data_nodes, config) = support::bring_up_split(1, 1, dir.path()).await;
        support::await_leader(&control_nodes).await;
        let data_raftkv_ids: Vec<animus_env::NodeId> =
            (1..2).map(animusd::config::node_id).collect();
        support::await_data_nodes_active(&control_nodes, &data_raftkv_ids).await;

        let data_console_addr = data_nodes[0].console_addr();
        assert_eq!(
            data_console_addr, config.nodes[1].console,
            "the data-only node's bound console port matches the config"
        );

        let (status, head, body) = raw(data_console_addr, "/").await;
        assert_eq!(status, 200, "the shell 200s on a data-only node too");
        assert!(
            head.to_ascii_lowercase()
                .contains("content-type: text/html"),
            "the shell is text/html, headers:\n{head}"
        );
        assert!(
            body.contains("animusd console"),
            "the shell names itself: {body}"
        );

        for node in control_nodes.iter().chain(data_nodes.iter()) {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("test timed out");
}

/// The deliberate non-goal (ADR 0052 "Which deployment shapes serve it"): a
/// **control-only** node hosts no CP-data tablet, so it binds no console
/// listener at all — `Node::console_addr()` panics there, the identical
/// contract `dynamo_addr()` already carries for the same shape.
///
/// **KEPT `ProdEnv`** (ADR 0061 rung L, C-12 PR 4c): this assertion is
/// purely a `Node`-level listener-binding fact (`self.data.as_ref().
/// expect(..)` on a plain `Option<SocketAddr>` field) — it has nothing to
/// do with `ClientCtx`/`DataRole` and no `SimCluster` analog exists for
/// it, since the fixture has no `Node` struct and binds no real listener
/// for any role (unaffected by `SimCluster` gaining per-node `NodeRole` in
/// C-12 PRs 2/3). What DOES carry over — the underlying invariant this
/// panic exists to enforce, that a control-only node structurally has no
/// data role, plus a genuine, stronger-than-expected live consequence (a
/// console item mutation dispatched directly against that node's own
/// `ClientCtx` panics too, deep inside `ClientCtx::data()` — a real
/// finding, not a product bug, see that sibling's own doc comment) — is
/// proven by `sim_cluster_console.rs::
/// console_item_write_from_a_control_only_node_panics`.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[should_panic(expected = "this node has no data role")]
async fn console_addr_panics_on_control_only_node() {
    let dir = support::panic_safe_tempdir();
    let (control_nodes, data_nodes, _config) = support::bring_up_split(1, 1, dir.path()).await;
    support::await_leader(&control_nodes).await;

    // Panics here — the assertion under test.
    let _ = control_nodes[0].console_addr();

    // Unreachable, but keeps the nodes alive (and clippy quiet) if the
    // panic contract above is ever weakened.
    for node in control_nodes.iter().chain(data_nodes.iter()) {
        node.shutdown_graceful().await;
    }
}
