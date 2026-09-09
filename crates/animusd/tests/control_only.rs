//! `animusd control` — the control-only process (ADR 0035 PR3).
//!
//! **Trimmed (ADR 0061 rung L, C-12 PR 4a).** All three of this file's
//! original tests converted whole to `SimCluster` (`sim_cluster_control_
//! data_split.rs`'s own classification table has the per-test mapping):
//! `schema_ddl_via_control_node_commits_and_relays` and
//! `mixed_cluster_put_via_control_node_forwards_to_data_node` are fully
//! converted, needing no real-socket residual. Kept here as the crate's
//! one real-socket "does a bare control-only process actually bind and
//! serve" bring-up smoke — real TCP accept loops, a real HTTP/1.0
//! `/admin/*` response, and a real on-disk system-keyspace engine
//! (`/admin/storage/control`), none of which `SimCluster` (in-process,
//! no sockets, `MemoryEngine`) can stand in for. See `sim_cluster_
//! control_data_split.rs`'s own module doc for the sim siblings covering
//! the DDL-commit/relay and mixed-cluster-forwarding properties this file
//! used to also prove.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::Node;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// Reserve `count` free loopback ports (bind :0, read addr, release).
fn free_addrs(count: usize) -> Vec<SocketAddr> {
    let ls: Vec<std::net::TcpListener> = (0..count)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    ls.iter().map(|l| l.local_addr().unwrap()).collect()
}

/// Bring up an `n`-node **control-only** cluster, one process (in this test
/// binary) per node — each its own `ClusterEdgeState`, matching a real
/// deployment. Retries the (allocate-fresh-ports + start-all) as a unit
/// (the documented port-TOCTOU mitigation: `free_addrs` releases each probed
/// port before `run_node_control` rebinds it, so another test binary can
/// steal one in the window).
async fn bring_up_control(n: usize, dir: &std::path::Path) -> (Vec<Node>, animusd::ClusterConfig) {
    for attempt in 0..16 {
        // Six addresses per index even though a control-only entry only ever
        // binds four of them (internal, client, admin, intra) — `RoleAddrs::
        // dynamo` isn't `Option`, and matching the six-port stride (ADR
        // 0047) keeps this config trivially comparable to a combined-mode one.
        let addrs = free_addrs(n * 6);
        let nodes_cfg: Vec<animusd::RoleAddrs> = (0..n)
            .map(|i| animusd::RoleAddrs {
                id: animusd::config::node_id(i),
                role: animusd::config::NodeRole::Control,
                internal: addrs[6 * i],
                client: addrs[6 * i + 1],
                dynamo: addrs[6 * i + 2],
                admin: addrs[6 * i + 3],
                intra: addrs[6 * i + 4],
                console: addrs[6 * i + 5],
                advertise_host: None,
                tls: None,
                encryption_key_path: None,
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
            match animusd::run_node_control(
                &config,
                i,
                dir.join(format!("node-{attempt}-{i}")),
                animusd::StorageBackend::default(),
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
    panic!("could not bring up control-only cluster after retries (ports kept getting stolen)");
}

async fn await_leader(nodes: &[Node]) {
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes.iter().any(Node::is_control_leader) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("control-only cluster did not elect a leader in 20s");
}

/// One HTTP/1.0 GET to the admin endpoint; returns `(status, parsed JSON)`.
async fn admin_get(addr: SocketAddr, path: &str) -> (u16, serde_json::Value) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = TcpStream::connect(addr).await.expect("connect to admin");
    let request = format!("GET {path} HTTP/1.0\r\nHost: animus\r\nConnection: close\r\n\r\n");
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
    let value: serde_json::Value = serde_json::from_str(payload)
        .unwrap_or_else(|e| panic!("admin body is not JSON ({e}): {payload}"));
    (status, value)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn control_only_cluster_elects_leader_and_serves_status() {
    timeout(Duration::from_secs(60), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, _config) = bring_up_control(3, dir.path()).await;
        await_leader(&nodes).await;

        // Every node's own `/admin/status` serves — including the followers,
        // whose `effective_metadata()` reads their own real (non-mirrored)
        // control Raft, and every node's `/admin/health` reports it hosts no
        // CP group (a control-only node never does).
        for node in &nodes {
            let (s, status) = admin_get(node.admin_addr(), "/admin/status").await;
            assert_eq!(s, 200, "admin/status on {}", node.admin_addr());
            assert!(
                status.get("members").is_some(),
                "status should carry the (empty) members map: {status}"
            );

            let (s, health) = admin_get(node.admin_addr(), "/admin/health").await;
            assert_eq!(s, 200, "admin/health on {}", node.admin_addr());
            assert_eq!(
                health["hosts_cp"], false,
                "a control-only node never hosts a CP group: {health}"
            );

            let (s, config_view) = admin_get(node.admin_addr(), "/admin/config").await;
            assert_eq!(s, 200, "admin/config on {}", node.admin_addr());
            assert!(
                !config_view["node_id"].is_null(),
                "every node has one id (ADR 0040 PR1), control-only included: {config_view}"
            );
            assert!(
                !config_view["addrs"]["internal"].is_null(),
                "every role binds the one internal address (ADR 0040 PR1): {config_view}"
            );
            assert!(
                config_view["addrs"]["dynamo"].is_null(),
                "a control-only node's dynamo listener is never bound: {config_view}"
            );

            // ADR 0038 PR4: a control-only node unconditionally provisions
            // its own dedicated system-keyspace engine (the durable home of
            // the apply task's published `Metadata` cache) — unlike every
            // other `/admin/storage/*` route (all keyed on a hosted CP
            // tablet group, which a control-only node never has), this one
            // is available here.
            let (s, ctl_storage) = admin_get(node.admin_addr(), "/admin/storage/control").await;
            assert_eq!(s, 200, "admin/storage/control on {}", node.admin_addr());
            assert_eq!(
                ctl_storage["available"], true,
                "a control-only node has its own dedicated system-keyspace engine: {ctl_storage}"
            );
            assert_eq!(
                ctl_storage["backend"], "lsm",
                "bring_up_control uses the durable default backend: {ctl_storage}"
            );

            // The system-table browse surface (plan-syskv-ui, ADR 0038
            // addendum) — real rows on this same node, since it has
            // self-proposed its own `RegisterNodeAddrs`. `system_table.rs`
            // covers the full endpoint contract (every kind, filtering,
            // pagination, value shapes); this just proves it's wired up on
            // a genuine control-only node, not just the combined-node
            // fixture that test file uses. `await_leader` only waits for
            // *a* leader to exist, not for *this* node's own
            // self-registration to have committed AND been mirrored by the
            // (ADR 0038 PR3) async apply task — so this is a bounded poll,
            // not a single-shot assert right after `await_leader` returns
            // (that raced and flaked under `cargo test --workspace` load:
            // a freshly-elected leader's own election no-op can be the
            // *only* thing applied so far, giving `count: 0`). A
            // control-only cluster never runs the raftkv-side `bootstrap`
            // loop (only registers raftkv ids as `Member`s), so it never
            // has `member` rows — every control-only node's own
            // `node_addrs` self-registration is what's actually guaranteed
            // present here.
            let syst = timeout(Duration::from_secs(10), async {
                loop {
                    let (s, syst) = admin_get(node.admin_addr(), "/admin/system-table").await;
                    assert_eq!(s, 200, "admin/system-table on {}", node.admin_addr());
                    assert_eq!(
                        syst["available"], true,
                        "a control-only node has a system keyspace to browse: {syst}"
                    );
                    if syst["items"]
                        .as_array()
                        .is_some_and(|items| items.iter().any(|it| it["kind"] == "node_addrs"))
                    {
                        return syst;
                    }
                    sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "{}'s own node_addrs self-registration did not appear in its system \
                     keyspace within 10s",
                    node.admin_addr()
                )
            });
            assert!(
                syst["items"]
                    .as_array()
                    .expect("items array")
                    .iter()
                    .any(|it| it["kind"] == "node_addrs"),
                "at least one row is a node_addrs entity: {syst}"
            );
        }

        // No data members were ever registered — the placement reconciler
        // and failure detector on zero members should just be quiet, not
        // panic or busy-loop, over a bounded window. Poll status repeatedly
        // instead of a single fixed sleep, so a crash surfaces immediately
        // as a connection failure rather than only being caught by luck.
        for _ in 0..10 {
            for node in &nodes {
                let (s, status) = admin_get(node.admin_addr(), "/admin/status").await;
                assert_eq!(s, 200);
                assert!(status["members"].as_object().unwrap().is_empty());
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("control_only_cluster_elects_leader_and_serves_status timed out");
}
