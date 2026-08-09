//! Split-deployment scenarios beyond what `data_only.rs` / `control_only.rs` /
//! `data_join.rs` / `watch_metadata.rs` already cover: control-leader
//! failover under live data traffic, split + merge, failure-driven replica
//! repair onto a spare, decommission of a data node via the control leader,
//! and a full-cluster stop/restart — all against a **genuine** split
//! deployment (real `animusd control` + `animusd data` processes, no
//! combined-mode node anywhere).
//!
//! Real TCP/time throughout — every wait is a bounded, converged-or-timeout
//! poll, never a fixed sleep used as an assertion.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animusd::config::NodeRole;
use animusd::{
    ClientRequest, ClientResponse, ClusterConfig, ColumnType, MetaCommand, Node, NodeStatus,
    RoleAddrs, StorageBackend, TableSchema, read_frame,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;
use support::{await_data_nodes_active, await_leader, bring_up_split, free_addrs};

async fn call(addr: SocketAddr, req: ClientRequest) -> Option<ClientResponse> {
    let mut stream = TcpStream::connect(addr).await.ok()?;
    animusd::write_frame(&mut stream, &req).await.ok()?;
    read_frame(&mut stream).await.ok()?
}

/// Try every client address in `clients` (round-robin) until one accepts the
/// write (mirrors `tests/data_join.rs`/`tests/decommission.rs`'s `put`).
async fn put(clients: &[SocketAddr], table: &str, key: &[u8], value: &[u8], secs: u64) {
    let mut last: Option<ClientResponse> = None;
    let w = async {
        loop {
            for &c in clients {
                let resp = call(
                    c,
                    ClientRequest::Put {
                        key: key.to_vec(),
                        value: value.to_vec(),
                        table: table.to_string(),
                    },
                )
                .await;
                if let Some(ClientResponse::PutOk) = &resp {
                    return;
                }
                last = resp;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(secs), w)
        .await
        .unwrap_or_else(|_| {
            panic!("write of {table}/{key:?} never committed; last reply: {last:?}")
        });
}

async fn await_value(clients: &[SocketAddr], table: &str, key: &[u8], want: &[u8], secs: u64) {
    let p = async {
        loop {
            for &c in clients {
                if let Some(ClientResponse::Value(Some(v))) = call(
                    c,
                    ClientRequest::Get {
                        key: key.to_vec(),
                        table: table.to_string(),
                    },
                )
                .await
                {
                    if v == want {
                        return;
                    }
                }
            }
            sleep(Duration::from_millis(150)).await;
        }
    };
    timeout(Duration::from_secs(secs), p)
        .await
        .unwrap_or_else(|_| panic!("key {table}/{key:?} never read back as {want:?}"));
}

/// One HTTP/1.0 request to the admin endpoint; returns `(status, parsed JSON)`.
async fn admin(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> (u16, serde_json::Value) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to admin");
    let body = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.0\r\nHost: animus\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len(),
    );
    stream.write_all(request.as_bytes()).await.expect("send");
    stream.flush().await.expect("flush");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read response");
    let text = String::from_utf8(raw).expect("utf8 response");
    let (head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse().ok())
        .expect("status line");
    let value: serde_json::Value = serde_json::from_str(payload).unwrap_or(serde_json::Value::Null);
    (status, value)
}

/// Poll `cond` until it holds, panicking with `what` after `secs` seconds.
async fn await_true<F: Fn() -> bool>(secs: u64, what: &str, cond: F) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while !cond() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        sleep(Duration::from_millis(100)).await;
    }
}

/// The ids of `table`'s tablets, from `node`'s own cached metadata —
/// [`Node::metadata`] works uniformly for any node shape (a control node's
/// real applied state; a data node's polled mirror), so this helper needs no
/// admin-JSON round trip.
fn table_tablets(node: &Node, table: &str) -> Vec<u64> {
    node.metadata()
        .tablets
        .iter()
        .filter(|(_, t)| t.table.as_deref() == Some(table))
        .map(|(id, _)| id.0)
        .collect()
}

// ---- 1. Control-leader failover under live data traffic --------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn control_leader_failover_under_live_data_traffic() {
    timeout(Duration::from_secs(120), async {
        let dir = tempfile::tempdir().unwrap();
        let (mut control_nodes, data_nodes, _config) = bring_up_split(3, 2, dir.path()).await;
        await_leader(&control_nodes).await;
        let data_raftkv_ids: Vec<animus_env::NodeId> =
            (3..5).map(animusd::config::raftkv_id).collect();
        await_data_nodes_active(&control_nodes, &data_raftkv_ids).await;

        let data_clients: Vec<SocketAddr> = data_nodes.iter().map(Node::client_addr).collect();

        // A write loop spanning the leader kill: each key gets its own
        // bounded (8s) retry, so an attempt straddling the failover window
        // survives it rather than failing outright. We record exactly which
        // indices actually got a `PutOk` — the later "nothing acked was
        // lost" check only has to hold for what this loop actually claims.
        let acked: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
        let acked_writer = acked.clone();
        let traffic_clients = data_clients.clone();
        let traffic = tokio::spawn(async move {
            for i in 0..30usize {
                let key = format!("traffic-{i}").into_bytes();
                let value = format!("v{i}").into_bytes();
                let ok = timeout(Duration::from_secs(8), async {
                    loop {
                        for &c in &traffic_clients {
                            if let Some(ClientResponse::PutOk) = call(
                                c,
                                ClientRequest::Put {
                                    key: key.clone(),
                                    value: value.clone(),
                                    table: "failover_t".to_string(),
                                },
                            )
                            .await
                            {
                                return;
                            }
                        }
                        sleep(Duration::from_millis(50)).await;
                    }
                })
                .await
                .is_ok();
                if ok {
                    acked_writer.lock().unwrap().push(i);
                }
            }
        });

        // Let a few writes land (proving the table provisioned) before
        // disrupting anything.
        timeout(Duration::from_secs(15), async {
            loop {
                if acked.lock().unwrap().len() >= 3 {
                    return;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("no writes landed before the leader kill");

        // Kill the CURRENT control LEADER specifically (not an arbitrary
        // follower) — the harder failover case: the node every proposal was
        // routing through, including any in-flight schema-relay traffic.
        let leader_idx = control_nodes
            .iter()
            .position(Node::is_control_leader)
            .unwrap();
        let leader_node = control_nodes.remove(leader_idx);
        leader_node.shutdown_graceful().await;

        // Let the write loop finish; it spans the kill and the re-election.
        traffic.await.expect("traffic task panicked");
        let acked_indices = acked.lock().unwrap().clone();
        assert!(
            acked_indices.len() >= 20,
            "too many writes failed outright across the leader failover: {} / 30 acked",
            acked_indices.len()
        );

        // The remaining pair elects a new leader.
        await_leader(&control_nodes).await;

        // No write this test believes was acked was lost.
        for &i in &acked_indices {
            let key = format!("traffic-{i}").into_bytes();
            let value = format!("v{i}").into_bytes();
            await_value(&data_clients, "failover_t", &key, &value, 20).await;
        }

        // A DDL issued only AFTER the kill still commits, relayed from a
        // data node to whichever control node now leads — proving the
        // control plane itself recovered availability for *changes*, not
        // just that pre-routed data-plane traffic was unaffected.
        let create = MetaCommand::CreateTableSchema {
            table: "failover_ddl_t".into(),
            schema: TableSchema::simple("id", ColumnType::String),
        };
        timeout(Duration::from_secs(20), async {
            loop {
                let _ = call(
                    data_nodes[0].client_addr(),
                    ClientRequest::ProposeSchema(create.clone()),
                )
                .await;
                if control_nodes
                    .iter()
                    .all(|n| n.metadata().has_table_schema("failover_ddl_t"))
                {
                    return;
                }
                sleep(Duration::from_millis(150)).await;
            }
        })
        .await
        .expect("post-failover schema DDL never committed to the surviving control pair");

        for n in control_nodes.iter().chain(data_nodes.iter()) {
            n.shutdown_graceful().await;
        }
    })
    .await
    .expect("control_leader_failover_under_live_data_traffic timed out");
}

// ---- 2. Split + merge over a split deployment -------------------------------

const MERGE_KEYS: [(&str, &str); 5] = [
    ("a", "v-a"),
    ("g", "v-g"),
    ("m", "v-m"),
    ("s", "v-s"),
    ("z", "v-z"),
];

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn split_and_merge_over_a_split_deployment() {
    timeout(Duration::from_secs(120), async {
        let dir = tempfile::tempdir().unwrap();
        let (control_nodes, data_nodes, _config) = bring_up_split(3, 2, dir.path()).await;
        await_leader(&control_nodes).await;
        let data_raftkv_ids: Vec<animus_env::NodeId> =
            (3..5).map(animusd::config::raftkv_id).collect();
        await_data_nodes_active(&control_nodes, &data_raftkv_ids).await;

        let data_clients: Vec<SocketAddr> = data_nodes.iter().map(Node::client_addr).collect();
        for (k, v) in MERGE_KEYS {
            put(
                &data_clients,
                "split_merge_t",
                k.as_bytes(),
                v.as_bytes(),
                20,
            )
            .await;
        }
        await_true(20, "table provisioned", || {
            !table_tablets(&data_nodes[0], "split_merge_t").is_empty()
        })
        .await;
        let parent = table_tablets(&data_nodes[0], "split_merge_t")[0];

        // Trigger the split against a DATA-only node's admin port — a
        // control-plane admin action reached through the genuinely-`Remote`
        // fleet, not only against the control deployment.
        let (status, body) = admin(
            data_nodes[0].admin_addr(),
            "POST",
            "/admin/tablet/split",
            Some(&format!(r#"{{"tablet":{parent},"split_key":"m"}}"#)),
        )
        .await;
        assert_eq!(status, 200, "split trigger: {body}");

        await_true(30, "split produced two tablets", || {
            table_tablets(&data_nodes[0], "split_merge_t").len() == 2
        })
        .await;
        let child = table_tablets(&data_nodes[0], "split_merge_t")
            .into_iter()
            .find(|&t| t != parent)
            .expect("a child tablet exists");

        // Both halves independently writable/servable post-split.
        put(&data_clients, "split_merge_t", b"b", b"v-b2", 20).await;
        put(&data_clients, "split_merge_t", b"y", b"v-y2", 20).await;
        await_value(&data_clients, "split_merge_t", b"b", b"v-b2", 20).await;
        await_value(&data_clients, "split_merge_t", b"y", b"v-y2", 20).await;

        // Merge back — trigger against a CONTROL node's admin port this
        // time, exercising the admin surface from the other deployment half.
        let (status, body) = admin(
            control_nodes[0].admin_addr(),
            "POST",
            "/admin/tablet/merge",
            Some(&format!(r#"{{"left":{parent},"right":{child}}}"#)),
        )
        .await;
        assert_eq!(status, 200, "merge trigger: {body}");

        await_true(30, "merge collapsed back to one tablet", || {
            table_tablets(&data_nodes[0], "split_merge_t").len() == 1
        })
        .await;

        // The survivor serves every key, both from before AND after the
        // split — through the control-plane metadata the whole way.
        for (k, v) in MERGE_KEYS {
            await_value(
                &data_clients,
                "split_merge_t",
                k.as_bytes(),
                v.as_bytes(),
                20,
            )
            .await;
        }
        await_value(&data_clients, "split_merge_t", b"b", b"v-b2", 20).await;
        await_value(&data_clients, "split_merge_t", b"y", b"v-y2", 20).await;

        for n in control_nodes.iter().chain(data_nodes.iter()) {
            n.shutdown_graceful().await;
        }
    })
    .await
    .expect("split_and_merge_over_a_split_deployment timed out");
}

// ---- 3. Data-node failure -> detect -> repair onto a spare -----------------

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn data_node_failure_is_detected_and_repaired_onto_a_spare() {
    timeout(Duration::from_secs(120), async {
        let dir = tempfile::tempdir().unwrap();
        // RF = min(N, 3): 4 data nodes leaves exactly one idle spare once the
        // first table's tablet provisions onto the 3 lowest-id Active members.
        let (control_nodes, mut data_nodes, _config) = bring_up_split(3, 4, dir.path()).await;
        await_leader(&control_nodes).await;
        let data_raftkv_ids: Vec<animus_env::NodeId> =
            (3..7).map(animusd::config::raftkv_id).collect();
        await_data_nodes_active(&control_nodes, &data_raftkv_ids).await;

        let data_clients: Vec<SocketAddr> = data_nodes.iter().map(Node::client_addr).collect();
        put(&data_clients, "repair_t", b"k0", b"v0", 20).await;

        let (tablet, replicas_before) = timeout(Duration::from_secs(20), async {
            loop {
                let meta = control_nodes[0].metadata();
                if let Some((&id, t)) = meta.tablets_for_table("repair_t").next() {
                    if t.replicas.len() == 3 {
                        return (id, t.replicas.clone());
                    }
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("tablet was not provisioned with 3 replicas in 20s");

        let spare = data_raftkv_ids
            .iter()
            .copied()
            .find(|id| !replicas_before.contains(id))
            .expect("a spare data node exists");
        let killed_id = replicas_before[0];
        let victim_idx = data_raftkv_ids
            .iter()
            .position(|&id| id == killed_id)
            .unwrap();
        let killed_node = data_nodes.remove(victim_idx);
        killed_node.shutdown_graceful().await;

        // The detector marks it Down in the control deployment's own
        // (real, non-mirrored) metadata.
        timeout(Duration::from_secs(30), async {
            loop {
                if control_nodes[0]
                    .metadata()
                    .members
                    .get(&killed_id)
                    .map(|m| m.status)
                    == Some(NodeStatus::Down)
                {
                    return;
                }
                sleep(Duration::from_millis(150)).await;
            }
        })
        .await
        .expect("the killed data node was never marked Down");

        // Placement repairs the tablet onto the spare.
        timeout(Duration::from_secs(60), async {
            loop {
                if let Some(t) = control_nodes[0].metadata().tablets.get(&tablet) {
                    if t.replicas.contains(&spare) && !t.replicas.contains(&killed_id) {
                        return;
                    }
                }
                sleep(Duration::from_millis(150)).await;
            }
        })
        .await
        .expect("the dead replica was not auto-replaced by the spare");

        // Still readable through the survivors, and a fresh write commits.
        let survivor_clients: Vec<SocketAddr> = data_nodes.iter().map(Node::client_addr).collect();
        await_value(&survivor_clients, "repair_t", b"k0", b"v0", 30).await;
        put(&survivor_clients, "repair_t", b"k1", b"v1", 20).await;
        await_value(&survivor_clients, "repair_t", b"k1", b"v1", 20).await;

        for n in control_nodes.iter().chain(data_nodes.iter()) {
            n.shutdown_graceful().await;
        }
    })
    .await
    .expect("data_node_failure_is_detected_and_repaired_onto_a_spare timed out");
}

// ---- 4. Decommission a data node, only via the control leader --------------

/// Several independent tables (mirroring `tests/decommission.rs`/
/// `tests/data_join.rs`'s `TABLES`): initial RF-based placement always picks
/// the *lowest* `min(N,3)` Active raftkv ids (deterministic `BTreeMap`
/// iteration, no load-awareness), so with one table the highest-id data node
/// would never gain a replica at all. Several tables raise the rebalancer's
/// (ADR 0029) global imbalance enough that it actually moves one onto it.
const DECOMM_TABLES: [&str; 3] = ["dsplit0", "dsplit1", "dsplit2"];

fn table_with_replica(node: &Node, raftkv_id: animus_env::NodeId) -> Option<String> {
    node.metadata()
        .tablets
        .values()
        .find(|t| t.replicas.contains(&raftkv_id))
        .and_then(|t| t.table.clone())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn decommission_a_data_node_over_split_deployment_via_the_control_leader() {
    timeout(Duration::from_secs(150), async {
        let dir = tempfile::tempdir().unwrap();
        let (control_nodes, mut data_nodes, _config) = bring_up_split(3, 4, dir.path()).await;
        await_leader(&control_nodes).await;
        let data_raftkv_ids: Vec<animus_env::NodeId> =
            (3..7).map(animusd::config::raftkv_id).collect();
        await_data_nodes_active(&control_nodes, &data_raftkv_ids).await;

        let data_clients: Vec<SocketAddr> = data_nodes.iter().map(Node::client_addr).collect();
        for table in DECOMM_TABLES {
            put(&data_clients, table, b"k0", b"v0", 20).await;
        }

        // Target the highest-id data node — guaranteed to start as a pure
        // spare (see `DECOMM_TABLES`'s doc) — and wait for the rebalancer to
        // actually give it a real replica before decommissioning it; this
        // proves the flow drains a node that genuinely hosts data, not an
        // already-idle one.
        let target_id = data_raftkv_ids[3];
        let hosted_table = timeout(Duration::from_secs(90), async {
            loop {
                if let Some(t) = table_with_replica(&control_nodes[0], target_id) {
                    return t;
                }
                sleep(Duration::from_millis(300)).await;
            }
        })
        .await
        .expect("the target data node never gained a rebalanced replica");

        // Sanity: it genuinely serves before decommission starts.
        put(
            &[data_nodes[3].client_addr()],
            &hosted_table,
            b"pre",
            b"ok",
            20,
        )
        .await;
        await_value(&data_clients, &hosted_table, b"pre", b"ok", 20).await;

        let leader_idx = control_nodes
            .iter()
            .position(Node::is_control_leader)
            .unwrap();

        // A control-plane admin action against the DATA node's OWN admin
        // port must refuse with a leader-routing hint: a data-only node
        // never registers a local control handle at all
        // (`ClusterEdgeState::leader_handle` is unconditionally empty
        // there), unlike a combined-mode follower, which at least has *a*
        // handle to check `is_leader()` on and already exercised this
        // refusal shape (`tests/decommission.rs`).
        {
            let body = serde_json::json!({"node": target_id}).to_string();
            let (status, resp) = admin(
                data_nodes[3].admin_addr(),
                "POST",
                "/admin/drain",
                Some(&body),
            )
            .await;
            assert_eq!(
                status, 409,
                "drain via the data node's own admin port should be refused: {resp}"
            );
            let msg = resp["error"]
                .as_str()
                .unwrap_or_default()
                .to_ascii_lowercase();
            assert!(
                msg.contains("leader"),
                "expected a leader-routing hint, got: {msg}"
            );
        }

        // The control LEADER's admin port succeeds.
        {
            let body = serde_json::json!({"node": target_id}).to_string();
            let (status, resp) = admin(
                control_nodes[leader_idx].admin_addr(),
                "POST",
                "/admin/drain",
                Some(&body),
            )
            .await;
            assert_eq!(status, 200, "drain failed: {resp}");
        }

        // Drain-status is read-only and serves off ANY node's
        // `effective_metadata()` — poll it via the DATA node's own admin
        // port (its mirror), proving that cross-node-type read path too.
        timeout(Duration::from_secs(60), async {
            loop {
                let (status, body) = admin(
                    data_nodes[3].admin_addr(),
                    "GET",
                    &format!("/admin/member/drain-status?node={target_id}"),
                    None,
                )
                .await;
                if status == 200 {
                    let remaining = body["tablets_remaining"].as_u64().unwrap_or(u64::MAX);
                    let node_status = body["status"].as_str().unwrap_or("");
                    if remaining == 0 && node_status != "Active" {
                        return;
                    }
                }
                sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .expect("the target node never finished draining");

        // Remove: refused via the data node's own admin port, succeeds via
        // the control leader — same refusal/success pairing as drain above.
        {
            let body = serde_json::json!({"node": target_id}).to_string();
            let (status, resp) = admin(
                data_nodes[3].admin_addr(),
                "POST",
                "/admin/member/remove",
                Some(&body),
            )
            .await;
            assert_eq!(
                status, 409,
                "remove via the data node's own admin port should be refused: {resp}"
            );
            let msg = resp["error"]
                .as_str()
                .unwrap_or_default()
                .to_ascii_lowercase();
            assert!(
                msg.contains("leader"),
                "expected a leader-routing hint, got: {msg}"
            );
        }
        {
            let body = serde_json::json!({"node": target_id}).to_string();
            let (status, resp) = admin(
                control_nodes[leader_idx].admin_addr(),
                "POST",
                "/admin/member/remove",
                Some(&body),
            )
            .await;
            assert_eq!(status, 200, "remove failed: {resp}");
        }

        // Membership + address book pruned; cluster still serving.
        timeout(Duration::from_secs(30), async {
            loop {
                let meta = control_nodes[0].metadata();
                if !meta.members.contains_key(&target_id)
                    && !meta.node_addrs.contains_key(&target_id)
                {
                    return;
                }
                sleep(Duration::from_millis(150)).await;
            }
        })
        .await
        .expect("the removed node never disappeared from membership/address book");

        let removed = data_nodes.remove(3);
        removed.shutdown_graceful().await;

        let survivor_clients: Vec<SocketAddr> = data_nodes.iter().map(Node::client_addr).collect();
        put(&survivor_clients, &hosted_table, b"post-remove", b"ok", 30).await;
        await_value(&survivor_clients, &hosted_table, b"post-remove", b"ok", 30).await;

        for n in control_nodes.iter().chain(data_nodes.iter()) {
            n.shutdown_graceful().await;
        }
    })
    .await
    .expect("decommission_a_data_node_over_split_deployment_via_the_control_leader timed out");
}

// ---- 5. Full-cluster stop/restart -------------------------------------------

/// Bring up a split cluster with a DURABLE data backend (`Lsm`, unlike
/// `support::bring_up_split`'s always-`Memory` — every other test in this
/// suite restarts at most one node and relies on the *surviving* replica's
/// live Raft replication instead of on-disk durability) and return each
/// node's own directory, so a full-outage restart can rebind every node on
/// its own same dir/addresses.
async fn bring_up_split_durable(
    control_n: usize,
    data_n: usize,
    dir: &Path,
) -> (
    Vec<Node>,
    Vec<Node>,
    ClusterConfig,
    Vec<PathBuf>,
    Vec<PathBuf>,
) {
    let total = control_n + data_n;
    for attempt in 0..16 {
        let addrs = free_addrs(total * 6);
        let nodes_cfg: Vec<RoleAddrs> = (0..total)
            .map(|i| {
                let role = if i < control_n {
                    NodeRole::Control
                } else {
                    NodeRole::Data
                };
                RoleAddrs {
                    role,
                    control: role.has_control().then_some(addrs[6 * i]),
                    client: addrs[6 * i + 1],
                    dynamo: addrs[6 * i + 2],
                    cql: addrs[6 * i + 3],
                    raftkv: role.has_data().then_some(addrs[6 * i + 4]),
                    admin: addrs[6 * i + 5],
                }
            })
            .collect();
        let config = ClusterConfig { nodes: nodes_cfg };
        let control_dirs: Vec<PathBuf> = (0..control_n)
            .map(|i| dir.join(format!("a{attempt}-c{i}")))
            .collect();
        let data_dirs: Vec<PathBuf> = (0..data_n)
            .map(|i| dir.join(format!("a{attempt}-d{i}")))
            .collect();

        let mut control_nodes = Vec::new();
        let mut data_nodes = Vec::new();
        let mut failed = false;
        for (i, d) in control_dirs.iter().enumerate() {
            match animusd::run_node_control(&config, i, d).await {
                Ok(n) => control_nodes.push(n),
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        if !failed {
            for (idx, d) in data_dirs.iter().enumerate() {
                let i = control_n + idx;
                match animusd::run_node_data(&config, i, d, StorageBackend::Lsm).await {
                    Ok(n) => data_nodes.push(n),
                    Err(_) => {
                        failed = true;
                        break;
                    }
                }
            }
        }
        if !failed {
            return (control_nodes, data_nodes, config, control_dirs, data_dirs);
        }
        for n in control_nodes.iter().chain(data_nodes.iter()) {
            n.shutdown_graceful().await;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("could not bring up a durable split cluster after retries (ports kept getting stolen)");
}

/// How long a same-address restart's rebind retries before giving up
/// (`support::restart_same_addrs` uses 5s for a *single* node). Confirmed
/// this is genuine ephemeral-port contention, not a socket-close ordering
/// bug: `mio::net::TcpListener::bind` already sets `SO_REUSEADDR` (checked
/// in the vendored source), which rules out a lingering-`TIME_WAIT`
/// explanation for "Address already in use" here — every failed rebind
/// attempt really did race another process's live bind on that exact port.
/// A full-cluster restart rebinds *every* node in sequence right after
/// tearing every other node down, so it multiplies a single node's usual
/// sub-second exposure to that race by however many nodes must rebind; under
/// heavy `cargo test --workspace`-style CPU/ephemeral-port contention the
/// tail of that race can occasionally stretch well past what a lone
/// restart test ever needs. A generous bound here turns "rare and slow"
/// into "reliably eventually succeeds" without weakening what the retry
/// actually proves (same-address recovery, not a latency bound) — paired
/// with keeping the fleet small (`bring_up_split_durable(3, 1, ..)` below)
/// to also cut the number of rebinds this test needs, not just how long
/// each may take.
const RESTART_REBIND_TIMEOUT: Duration = Duration::from_secs(60);

/// Rebind `run_node_control` on the same address/dir, retrying to ride out
/// the documented port-TOCTOU (the control-only counterpart of
/// `support::restart_same_addrs`, which is combined-mode/data-backend only).
async fn restart_control(config: &ClusterConfig, index: usize, dir: &Path) -> Node {
    let deadline = tokio::time::Instant::now() + RESTART_REBIND_TIMEOUT;
    loop {
        match animusd::run_node_control(config, index, dir).await {
            Ok(n) => return n,
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!(
                        "control node {index} did not rebind on restart: {e}\n{}",
                        listen_holders(config.nodes[index].control)
                    );
                }
                sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// Diagnostic-only: shell out to `ss` to show which process (if any) is
/// listening on `addr` right now — attached to a rebind-timeout panic so a
/// future flake carries forensic evidence (PID/process name) instead of just
/// "address already in use", distinguishing "another process on this
/// machine is genuinely squatting on this port" from a same-process
/// socket-lifetime bug. Best-effort: `ss` may not exist or may need
/// privileges to show every process, so a failure here never masks the real
/// assertion.
fn listen_holders(addr: Option<SocketAddr>) -> String {
    let Some(addr) = addr else {
        return "listen_holders: no address".into();
    };
    match std::process::Command::new("ss")
        .args(["-ltnp", "-H"])
        .output()
    {
        Ok(out) => {
            let text = String::from_utf8_lossy(&out.stdout);
            let port_suffix = format!(":{}", addr.port());
            let hits: Vec<&str> = text.lines().filter(|l| l.contains(&port_suffix)).collect();
            if hits.is_empty() {
                format!("ss found no listener on {addr} (may lack permission to see it)")
            } else {
                format!("ss listeners on {addr}:\n{}", hits.join("\n"))
            }
        }
        Err(e) => format!("ss unavailable ({e}); no diagnostic for {addr}"),
    }
}

/// The data-only counterpart of [`restart_control`].
async fn restart_data(
    config: &ClusterConfig,
    index: usize,
    dir: &Path,
    backend: StorageBackend,
) -> Node {
    let deadline = tokio::time::Instant::now() + RESTART_REBIND_TIMEOUT;
    loop {
        match animusd::run_node_data(config, index, dir, backend).await {
            Ok(n) => return n,
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "data node {index} did not rebind on restart: {e}"
                );
                sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn full_split_cluster_restart_recovers_metadata_and_data() {
    // Generous outer bound: 4 sequential rebinds can each need up to
    // `RESTART_REBIND_TIMEOUT` under heavy contention (see its doc).
    timeout(Duration::from_secs(300), async {
        let dir = tempfile::tempdir().unwrap();
        // A single data node is enough to prove "data re-hosts and re-serves
        // after a full outage" — replication/HA across multiple data nodes is
        // already covered elsewhere in this file; keeping the fleet small
        // here directly reduces how many same-address rebinds this test
        // needs (see `RESTART_REBIND_TIMEOUT`'s doc).
        let (control_nodes, data_nodes, config, control_dirs, data_dirs) =
            bring_up_split_durable(3, 1, dir.path()).await;
        await_leader(&control_nodes).await;
        let data_raftkv_ids: Vec<animus_env::NodeId> =
            (3..4).map(animusd::config::raftkv_id).collect();
        await_data_nodes_active(&control_nodes, &data_raftkv_ids).await;

        // Schema DDL + data, both meant to survive the full outage.
        let create = MetaCommand::CreateTableSchema {
            table: "restart_t".into(),
            schema: TableSchema::simple("id", ColumnType::String),
        };
        timeout(Duration::from_secs(20), async {
            loop {
                let _ = call(
                    data_nodes[0].client_addr(),
                    ClientRequest::ProposeSchema(create.clone()),
                )
                .await;
                if control_nodes
                    .iter()
                    .all(|n| n.metadata().has_table_schema("restart_t"))
                {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("schema did not commit before the outage");

        let data_clients: Vec<SocketAddr> = data_nodes.iter().map(Node::client_addr).collect();
        put(&data_clients, "restart_t", b"k0", b"v0", 20).await;
        await_value(&data_clients, "restart_t", b"k0", b"v0", 20).await;

        // Stop EVERYTHING — control trio and data fleet alike.
        for n in control_nodes.iter().chain(data_nodes.iter()) {
            n.shutdown_graceful().await;
        }

        // Restart every node on its own same dir/addresses — control first
        // (the discovery root data nodes mirror), then data.
        let mut restarted_control = Vec::new();
        for (i, d) in control_dirs.iter().enumerate() {
            restarted_control.push(restart_control(&config, i, d).await);
        }
        let mut restarted_data = Vec::new();
        for (idx, d) in data_dirs.iter().enumerate() {
            let i = 3 + idx;
            restarted_data.push(restart_data(&config, i, d, StorageBackend::Lsm).await);
        }

        // Control metadata recovered — a catch-up gate (any node electing),
        // not a specific-node leadership gate: every node in this fresh
        // 3-of-3 restart replays/elects the same as any other cold start.
        await_leader(&restarted_control).await;
        timeout(Duration::from_secs(20), async {
            loop {
                if restarted_control
                    .iter()
                    .all(|n| n.metadata().has_table_schema("restart_t"))
                {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("control metadata (schema) did not recover after the full restart");

        // Data re-hosts (from durable on-disk state, `Lsm` backend) and the
        // pre-restart write is readable again.
        await_data_nodes_active(&restarted_control, &data_raftkv_ids).await;
        let restarted_clients: Vec<SocketAddr> =
            restarted_data.iter().map(Node::client_addr).collect();
        await_value(&restarted_clients, "restart_t", b"k0", b"v0", 30).await;

        // A fresh write after the restart also works end to end.
        put(&restarted_clients, "restart_t", b"k1", b"v1", 20).await;
        await_value(&restarted_clients, "restart_t", b"k1", b"v1", 20).await;

        for n in restarted_control.iter().chain(restarted_data.iter()) {
            n.shutdown_graceful().await;
        }
    })
    .await
    .expect("full_split_cluster_restart_recovers_metadata_and_data timed out");
}
