//! `animusd control` — the control-only process (ADR 0035 PR3).
//!
//! Covers, over real TCP/time (so every wait is a bounded poll, never a fixed
//! sleep):
//! - a bare control-only cluster elects a leader, serves `/admin/status`, and
//!   is quiet (no panic) over a bounded window with zero data members;
//! - schema DDL (`ProposeSchema`) issued against a control-only node commits
//!   and replicates to every control node, including via the leader-relay
//!   path when issued against a follower;
//! - a mixed cluster (a control-only trio plus a combined-mode data node,
//!   the data node reaching the trio via the ADR 0030 growth-node mirror —
//!   the closest existing mechanism to a "data-only" node until ADR 0035
//!   PR4's `ControlHandle::Remote`): a `Put` sent to the CONTROL node's
//!   client port is provisioned and forwarded to the data node and
//!   succeeds; a schema command issued against the data node relays to the
//!   control leader.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use animusd::{
    ClientRequest, ClientResponse, ColumnType, MetaCommand, Node, NodeStatus, TableSchema,
    read_frame,
};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

/// Reserve `count` free loopback ports (bind :0, read addr, release).
fn free_addrs(count: usize) -> Vec<SocketAddr> {
    let ls: Vec<std::net::TcpListener> = (0..count)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    ls.iter().map(|l| l.local_addr().unwrap()).collect()
}

async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    animusd::write_frame(&mut stream, &req).await.expect("send");
    read_frame(&mut stream)
        .await
        .expect("read")
        .expect("a reply")
}

/// Bring up an `n`-node **control-only** cluster, one process (in this test
/// binary) per node — each its own `ClusterEdgeState`, matching a real
/// deployment. Retries the (allocate-fresh-ports + start-all) as a unit
/// (the documented port-TOCTOU mitigation: `free_addrs` releases each probed
/// port before `run_node_control` rebinds it, so another test binary can
/// steal one in the window).
async fn bring_up_control(n: usize, dir: &std::path::Path) -> (Vec<Node>, animusd::ClusterConfig) {
    for attempt in 0..16 {
        // Five addresses per index even though a control-only entry only ever
        // binds three of them (internal, client, admin) — `RoleAddrs::dynamo`/
        // `cql` aren't `Option`, and matching the five-port stride (ADR 0040
        // PR1) keeps this config trivially comparable to a combined-mode one.
        let addrs = free_addrs(n * 5);
        let nodes_cfg: Vec<animusd::RoleAddrs> = (0..n)
            .map(|i| animusd::RoleAddrs {
                role: animusd::config::NodeRole::Control,
                internal: addrs[5 * i],
                client: addrs[5 * i + 1],
                dynamo: addrs[5 * i + 2],
                cql: addrs[5 * i + 3],
                admin: addrs[5 * i + 4],
            })
            .collect();
        let config = animusd::ClusterConfig { nodes: nodes_cfg };
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
        let dir = tempfile::tempdir().unwrap();
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
            assert!(
                config_view["addrs"]["cql"].is_null(),
                "a control-only node's cql listener is never bound: {config_view}"
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

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn schema_ddl_via_control_node_commits_and_relays() {
    timeout(Duration::from_secs(60), async {
        let dir = tempfile::tempdir().unwrap();
        let (nodes, config) = bring_up_control(3, dir.path()).await;
        await_leader(&nodes).await;

        // Issue the DDL against the LEADER first — the direct-propose path.
        let leader = nodes.iter().position(Node::is_control_leader).unwrap();
        let create = MetaCommand::CreateTableSchema {
            table: "control_ddl_t".into(),
            schema: TableSchema::simple("id", ColumnType::String),
        };
        let resp = call(
            config.nodes[leader].client,
            ClientRequest::ProposeSchema(create.clone()),
        )
        .await;
        assert!(
            matches!(resp, ClientResponse::PutOk),
            "leader-local ProposeSchema should ack: {resp:?}"
        );
        timeout(Duration::from_secs(20), async {
            loop {
                if nodes
                    .iter()
                    .all(|n| n.metadata().has_table_schema("control_ddl_t"))
                {
                    return;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("leader-issued schema did not replicate to every control node in 20s");

        // Now issue a second DDL against a FOLLOWER — must relay to the
        // leader (the same `is_relayable_command`/`propose_schema` path a
        // follower-connected combined-mode node already exercises), not
        // time out.
        let follower = (0..nodes.len()).find(|&i| i != leader).unwrap();
        let create2 = MetaCommand::CreateTableSchema {
            table: "control_ddl_t2".into(),
            schema: TableSchema::simple("id", ColumnType::String),
        };
        timeout(Duration::from_secs(20), async {
            loop {
                let _ = call(
                    config.nodes[follower].client,
                    ClientRequest::ProposeSchema(create2.clone()),
                )
                .await;
                if nodes
                    .iter()
                    .all(|n| n.metadata().has_table_schema("control_ddl_t2"))
                {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("follower-issued schema did not relay + replicate in 20s");
    })
    .await
    .expect("schema_ddl_via_control_node_commits_and_relays timed out");
}

/// Bring `id` (a raftkv member id) to `Active` by directly proposing
/// `UpsertMember` on whichever control node currently leads — the same
/// primitive `bootstrap`'s own leader-only auto-registration uses, driven
/// from the test because a growth/mixed-deployment data node's own
/// `bootstrap` task never fires (its local control role is a permanent
/// non-voter of the control-only trio's group, so `raft.is_leader()` there
/// is always `false` — see `run_node_growth`'s doc). Retried across every
/// node each tick so it doesn't matter which trio member currently leads.
async fn force_active(nodes: &[Node], id: animus_env::NodeId) {
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes.iter().any(|n| {
                n.metadata().members.get(&id).map(|m| m.status) == Some(NodeStatus::Active)
            }) {
                return;
            }
            for n in nodes {
                let _ = n.propose_meta(MetaCommand::UpsertMember {
                    node: id,
                    labels: BTreeMap::new(),
                    status: NodeStatus::Active,
                });
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("member did not become Active in time");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn mixed_cluster_put_via_control_node_forwards_to_data_node() {
    timeout(Duration::from_secs(90), async {
        let dir = tempfile::tempdir().unwrap();

        // A control-only trio (indices 0..3) plus one combined-mode data
        // node (index 3) reaching it via the ADR 0030 growth-node mirror —
        // the closest existing mechanism to "a data node with no local
        // control-voter slot" until ADR 0035 PR4 provides
        // `ControlHandle::Remote` outright. `run_node_growth` is exactly
        // this shape: the data node's own control role never joins the
        // trio's voter set, and it mirrors the trio's `Metadata` via
        // `remote_metadata_sync_loop`.
        let (control_nodes, config, data_node) = 'bring_up: loop {
            let addrs = free_addrs(4 * 5);
            let mut nodes_cfg: Vec<animusd::RoleAddrs> = (0..3)
                .map(|i| animusd::RoleAddrs {
                    role: animusd::config::NodeRole::Control,
                    internal: addrs[5 * i],
                    client: addrs[5 * i + 1],
                    dynamo: addrs[5 * i + 2],
                    cql: addrs[5 * i + 3],
                    admin: addrs[5 * i + 4],
                })
                .collect();
            nodes_cfg.push(animusd::RoleAddrs {
                role: animusd::config::NodeRole::Both,
                internal: addrs[15],
                client: addrs[16],
                dynamo: addrs[17],
                cql: addrs[18],
                admin: addrs[19],
            });
            let config = animusd::ClusterConfig { nodes: nodes_cfg };

            let mut control_nodes: Vec<Node> = Vec::new();
            let mut ok = true;
            for i in 0..3 {
                match animusd::run_node_control(
                    &config,
                    i,
                    dir.path().join(format!("c-{i}")),
                    animusd::StorageBackend::default(),
                )
                .await
                {
                    Ok(n) => control_nodes.push(n),
                    Err(_) => {
                        ok = false;
                        break;
                    }
                }
            }
            if ok {
                match animusd::run_node_growth(
                    &config,
                    3,
                    vec![0, 1, 2],
                    dir.path().join("data-3"),
                    animusd::StorageBackend::Memory,
                )
                .await
                {
                    Ok(data_node) => break 'bring_up (control_nodes, config, data_node),
                    Err(_) => ok = false,
                }
            }
            if !ok {
                for n in &control_nodes {
                    n.shutdown_graceful().await;
                }
                sleep(Duration::from_millis(50)).await;
            }
        };

        await_leader(&control_nodes).await;

        // The data node's own id (ADR 0040 PR1 — one identity per node, was
        // `300 + 3 = 303`, now just `3`) must become `Active` before a
        // table's first tablet can be provisioned onto it.
        force_active(&control_nodes, 3).await;

        // A `Put` sent to a CONTROL node's client port: `cp_put` provisions
        // the table's first tablet (replicas = the one Active data member),
        // then `resolve_cp_route` — this control node hosts no local CP
        // group at all — forwards it to the data node's client address
        // (`client_route`, seeded from the same `config`). The forward can
        // legitimately land before the data node's own tablet-host
        // reconciler has stood the freshly-provisioned group up and elected
        // (its own `metadata_watch` never fires — a growth node's local
        // control raft never advances — so it only reacts on the 500ms
        // fallback tick), so retry with fresh routing on a clean
        // "not the leader here" error exactly like a real client would.
        timeout(Duration::from_secs(20), async {
            loop {
                let put = call(
                    config.nodes[0].client,
                    ClientRequest::Put {
                        key: b"mixed-key".to_vec(),
                        value: b"mixed-val".to_vec(),
                        table: "mixed_t".to_string(),
                    },
                )
                .await;
                if matches!(put, ClientResponse::PutOk) {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("put via the control node did not forward and succeed in 20s");

        // Read it back the same way, and directly on the data node too.
        let get = call(
            config.nodes[0].client,
            ClientRequest::Get {
                key: b"mixed-key".to_vec(),
                table: "mixed_t".to_string(),
            },
        )
        .await;
        assert_eq!(
            get,
            ClientResponse::Value(Some(b"mixed-val".to_vec())),
            "read-back via the control node"
        );
        let get_direct = call(
            data_node.client_addr(),
            ClientRequest::Get {
                key: b"mixed-key".to_vec(),
                table: "mixed_t".to_string(),
            },
        )
        .await;
        assert_eq!(
            get_direct,
            ClientResponse::Value(Some(b"mixed-val".to_vec())),
            "read-back directly on the data node"
        );

        // A schema command issued against the DATA node relays to the
        // control leader (the data node's own control role can never accept
        // a local propose — it is a permanent non-voter — so this proves
        // the relay path, not a local accept).
        let create = MetaCommand::CreateTableSchema {
            table: "mixed_ddl_t".into(),
            schema: TableSchema::simple("id", ColumnType::String),
        };
        timeout(Duration::from_secs(20), async {
            loop {
                let _ = call(
                    data_node.client_addr(),
                    ClientRequest::ProposeSchema(create.clone()),
                )
                .await;
                if control_nodes
                    .iter()
                    .all(|n| n.metadata().has_table_schema("mixed_ddl_t"))
                {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("data-node-issued schema did not relay to the control leader in 20s");

        for n in &control_nodes {
            n.shutdown_graceful().await;
        }
        data_node.shutdown_graceful().await;
    })
    .await
    .expect("mixed_cluster_put_via_control_node_forwards_to_data_node timed out");
}
