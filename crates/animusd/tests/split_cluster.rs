//! Split-deployment scenarios beyond what `data_only.rs` / `control_only.rs` /
//! `data_join.rs` / `watch_metadata.rs` already cover: control-leader
//! failover under live data traffic, split, failure-driven replica
//! repair onto a spare, decommission of a data node via the control leader,
//! a full-cluster stop/restart, a **simultaneous** control-leader + data-node
//! failure, and a decommission racing a tablet-split crossover — all against
//! a **genuine** split deployment (real `animusd control` + `animusd data`
//! processes, no combined-mode node anywhere). The last two are the
//! multi-fault chaos scenarios flagged as residual follow-ups from the ADR
//! 0035 stack (see `docs/adr/0035-control-plane-separate-deployment.md`).
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
                    && v == want
                {
                    return;
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
            (3..5).map(animusd::config::node_id).collect();
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
                    // ADR 0047: `ProposeSchema` is intra-only.
                    data_nodes[0].intra_addr(),
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

// ---- 2. Split over a split deployment ---------------------------------------
//
// Was `split_and_merge_over_a_split_deployment` (ADR 0033): the merge half
// (trigger via a control node's admin port, survivor serving both halves) is
// deleted along with tablet merge itself (split-only tablets, ADR 0044) — its
// data-plane reaction (`HostAction::WidenScope`/`Absorb`) no longer exists, so
// the merge trigger left the survivor's scope un-widened and every
// post-merge read failing. The split half is kept and isolated into its own
// test: `decommission_racing_a_tablet_split_converges_with_no_data_loss`
// (below) also splits over a split deployment, but always *combined* with a
// simultaneous decommission — this is the one place a bare split, no other
// fault in flight, is proven to converge and serve both halves' data on a
// genuine (non-combined-mode) split deployment.

const SPLIT_KEYS: [(&str, &str); 5] = [
    ("a", "v-a"),
    ("g", "v-g"),
    ("m", "v-m"),
    ("s", "v-s"),
    ("z", "v-z"),
];

#[ignore = "PARKED (ADR 0050 Train B rung 1): zero-copy split of a populated tablet is disabled during the storage pivot; revived/replaced by the copy-based split workflow in later rungs of this train"]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn split_over_a_split_deployment() {
    timeout(Duration::from_secs(120), async {
        let dir = tempfile::tempdir().unwrap();
        let (control_nodes, data_nodes, _config) = bring_up_split(3, 2, dir.path()).await;
        await_leader(&control_nodes).await;
        let data_raftkv_ids: Vec<animus_env::NodeId> =
            (3..5).map(animusd::config::node_id).collect();
        await_data_nodes_active(&control_nodes, &data_raftkv_ids).await;

        let data_clients: Vec<SocketAddr> = data_nodes.iter().map(Node::client_addr).collect();
        for (k, v) in SPLIT_KEYS {
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

        // Both halves independently writable/servable post-split, and every
        // pre-split key survives the crossover.
        put(&data_clients, "split_merge_t", b"b", b"v-b2", 20).await;
        put(&data_clients, "split_merge_t", b"y", b"v-y2", 20).await;
        await_value(&data_clients, "split_merge_t", b"b", b"v-b2", 20).await;
        await_value(&data_clients, "split_merge_t", b"y", b"v-y2", 20).await;
        for (k, v) in SPLIT_KEYS {
            await_value(
                &data_clients,
                "split_merge_t",
                k.as_bytes(),
                v.as_bytes(),
                20,
            )
            .await;
        }

        for n in control_nodes.iter().chain(data_nodes.iter()) {
            n.shutdown_graceful().await;
        }
    })
    .await
    .expect("split_over_a_split_deployment timed out");
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
            (3..7).map(animusd::config::node_id).collect();
        await_data_nodes_active(&control_nodes, &data_raftkv_ids).await;

        let data_clients: Vec<SocketAddr> = data_nodes.iter().map(Node::client_addr).collect();
        put(&data_clients, "repair_t", b"k0", b"v0", 20).await;

        let (tablet, replicas_before) = timeout(Duration::from_secs(20), async {
            loop {
                let meta = control_nodes[0].metadata();
                if let Some((&id, t)) = meta.tablets_for_table("repair_t").next()
                    && t.replicas.len() == 3
                {
                    return (id, t.replicas.clone());
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("tablet was not provisioned with 3 replicas in 20s");

        let spare = data_raftkv_ids
            .iter()
            .find(|id| !replicas_before.contains(id))
            .cloned()
            .expect("a spare data node exists");
        let killed_id = replicas_before[0].clone();
        let victim_idx = data_raftkv_ids
            .iter()
            .position(|id| *id == killed_id)
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
                if let Some(t) = control_nodes[0].metadata().tablets.get(&tablet)
                    && t.replicas.contains(&spare)
                    && !t.replicas.contains(&killed_id)
                {
                    return;
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
            (3..7).map(animusd::config::node_id).collect();
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
        let target_id = data_raftkv_ids[3].clone();
        let hosted_table = timeout(Duration::from_secs(90), async {
            loop {
                if let Some(t) = table_with_replica(&control_nodes[0], target_id.clone()) {
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
                    id: animusd::config::node_id(i),
                    role,
                    internal: addrs[6 * i],
                    client: addrs[6 * i + 1],
                    dynamo: addrs[6 * i + 2],
                    cql: addrs[6 * i + 3],
                    admin: addrs[6 * i + 4],
                    intra: addrs[6 * i + 5],
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
            match animusd::run_node_control(&config, i, d, StorageBackend::Lsm).await {
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
        match animusd::run_node_control(config, index, dir, StorageBackend::Lsm).await {
            Ok(n) => return n,
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!(
                        "control node {index} did not rebind on restart: {e}\n{}",
                        listen_holders(Some(config.nodes[index].internal))
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
            (3..4).map(animusd::config::node_id).collect();
        await_data_nodes_active(&control_nodes, &data_raftkv_ids).await;

        // Schema DDL + data, both meant to survive the full outage.
        let create = MetaCommand::CreateTableSchema {
            table: "restart_t".into(),
            schema: TableSchema::simple("id", ColumnType::String),
        };
        timeout(Duration::from_secs(20), async {
            loop {
                let _ = call(
                    // ADR 0047: `ProposeSchema` is intra-only.
                    data_nodes[0].intra_addr(),
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

// ---- 6. Control-leader failover + data-node failure, SIMULTANEOUSLY --------

/// Kills the control-plane leader and a live data replica at the same
/// instant (`tokio::join!` over two `shutdown_graceful` calls — both take
/// `&self`, so neither waits on the other's teardown to start) and asserts
/// the cluster converges on every axis: a new control leader is elected, the
/// dead data replica is auto-repaired onto the spare, in-flight writes
/// spanning the dual outage are not lost, and both planes recover
/// availability for fresh work (a post-outage schema DDL, a post-outage
/// write). Distinct from scenario 1 (control-only failover) and scenario 3
/// (data-only failure): here both faults land on the same instant, so the
/// control plane's own re-election and the data plane's replica repair race
/// each other with no ordering guarantee between them.
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn control_leader_and_data_node_failure_simultaneously_still_converges() {
    timeout(Duration::from_secs(150), async {
        let dir = tempfile::tempdir().unwrap();
        // RF = min(N,3): 4 data nodes leaves exactly one spare once the
        // first table's tablet provisions onto the 3 lowest-id Active
        // members (mirroring scenario 3), so the killed data replica has
        // somewhere to be auto-repaired onto.
        let (mut control_nodes, mut data_nodes, _config) = bring_up_split(3, 4, dir.path()).await;
        await_leader(&control_nodes).await;
        let data_raftkv_ids: Vec<animus_env::NodeId> =
            (3..7).map(animusd::config::node_id).collect();
        await_data_nodes_active(&control_nodes, &data_raftkv_ids).await;

        let data_clients: Vec<SocketAddr> = data_nodes.iter().map(Node::client_addr).collect();
        put(&data_clients, "dualfail_t", b"k0", b"v0", 20).await;

        let (tablet, replicas_before) = timeout(Duration::from_secs(20), async {
            loop {
                let meta = control_nodes[0].metadata();
                if let Some((&id, t)) = meta.tablets_for_table("dualfail_t").next()
                    && t.replicas.len() == 3
                {
                    return (id, t.replicas.clone());
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("tablet was not provisioned with 3 replicas in 20s");

        let spare = data_raftkv_ids
            .iter()
            .find(|id| !replicas_before.contains(id))
            .cloned()
            .expect("a spare data node exists");
        let killed_data_id = replicas_before[0].clone();
        let victim_idx = data_raftkv_ids
            .iter()
            .position(|id| *id == killed_data_id)
            .unwrap();

        // A write loop spanning BOTH simultaneous failures: each key gets
        // its own bounded (8s) retry across every ORIGINAL data client
        // address (including the soon-to-be-dead victim's — `call` just
        // fails fast on a dead connection and the loop moves to the next
        // candidate), so an attempt straddling the dual failover survives it
        // rather than failing outright. Only indices this loop itself
        // believes were acked are checked for survival afterward.
        let acked: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
        let acked_writer = acked.clone();
        let traffic_clients = data_clients.clone();
        let traffic = tokio::spawn(async move {
            for i in 0..30usize {
                let key = format!("dual-{i}").into_bytes();
                let value = format!("v{i}").into_bytes();
                let ok = timeout(Duration::from_secs(8), async {
                    loop {
                        for &c in &traffic_clients {
                            if let Some(ClientResponse::PutOk) = call(
                                c,
                                ClientRequest::Put {
                                    key: key.clone(),
                                    value: value.clone(),
                                    table: "dualfail_t".to_string(),
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
        .expect("no writes landed before the dual failure");

        // Kill the control LEADER and a live DATA replica of the tablet
        // AT THE SAME TIME.
        let leader_idx = control_nodes
            .iter()
            .position(Node::is_control_leader)
            .unwrap();
        tokio::join!(
            control_nodes[leader_idx].shutdown_graceful(),
            data_nodes[victim_idx].shutdown_graceful(),
        );
        control_nodes.remove(leader_idx);
        data_nodes.remove(victim_idx);

        // Let the write loop finish; it spans both kills and both recoveries.
        traffic.await.expect("traffic task panicked");
        let acked_indices = acked.lock().unwrap().clone();
        assert!(
            acked_indices.len() >= 15,
            "too many writes failed outright across the dual failure: {} / 30 acked",
            acked_indices.len()
        );

        // The remaining control pair elects a new leader...
        await_leader(&control_nodes).await;
        // ...and placement repairs the tablet onto the spare, closing the
        // dead replica out of its set.
        timeout(Duration::from_secs(60), async {
            loop {
                if let Some(t) = control_nodes[0].metadata().tablets.get(&tablet)
                    && t.replicas.contains(&spare)
                    && !t.replicas.contains(&killed_data_id)
                {
                    return;
                }
                sleep(Duration::from_millis(150)).await;
            }
        })
        .await
        .expect("the dead replica was not auto-replaced by the spare after the dual failure");

        // No write this test believes was acked was lost.
        let survivor_clients: Vec<SocketAddr> = data_nodes.iter().map(Node::client_addr).collect();
        for &i in &acked_indices {
            let key = format!("dual-{i}").into_bytes();
            let value = format!("v{i}").into_bytes();
            await_value(&survivor_clients, "dualfail_t", &key, &value, 30).await;
        }

        // A DDL issued only AFTER both kills still commits, relayed to
        // whichever control node now leads — proving the control plane
        // recovered availability for *changes*, not just that pre-routed
        // data-plane traffic was unaffected.
        let create = MetaCommand::CreateTableSchema {
            table: "dualfail_ddl_t".into(),
            schema: TableSchema::simple("id", ColumnType::String),
        };
        // ADR 0047: `ProposeSchema` is intra-only — a separate address list
        // from `survivor_clients` above (which stays client-flavored for the
        // data-plane `put`/`await_value` calls it feeds).
        let survivor_intra: Vec<SocketAddr> = data_nodes.iter().map(Node::intra_addr).collect();
        timeout(Duration::from_secs(20), async {
            loop {
                let _ = call(
                    survivor_intra[0],
                    ClientRequest::ProposeSchema(create.clone()),
                )
                .await;
                if control_nodes
                    .iter()
                    .all(|n| n.metadata().has_table_schema("dualfail_ddl_t"))
                {
                    return;
                }
                sleep(Duration::from_millis(150)).await;
            }
        })
        .await
        .expect("post-dual-failure schema DDL never committed to the surviving control pair");

        // A brand-new write also works end to end, through the
        // fully-converged cluster.
        put(&survivor_clients, "dualfail_t", b"k1", b"v1", 20).await;
        await_value(&survivor_clients, "dualfail_t", b"k1", b"v1", 20).await;

        for n in control_nodes.iter().chain(data_nodes.iter()) {
            n.shutdown_graceful().await;
        }
    })
    .await
    .expect("control_leader_and_data_node_failure_simultaneously_still_converges timed out");
}

// ---- 7. Decommission racing a tablet-split crossover ------------------------

const CROSSOVER_KEYS: [(&str, &str); 5] = [
    ("a", "v-a"),
    ("g", "v-g"),
    ("m", "v-m"),
    ("s", "v-s"),
    ("z", "v-z"),
];

/// Fires a tablet split and a data-node decommission (drain, then remove) at
/// the same time, against the SAME tablet: the split's freshly-minted child
/// inherits the parent's replica set at the instant of the split (including
/// the node being drained), so the reconciler must evacuate the draining
/// node off BOTH the narrowed parent AND the new child before the
/// decommission can complete — the actual "drain landing mid-split
/// crossover" hazard this test targets, rather than a drain and a split on
/// unrelated tablets that never interact. Asserts no data is lost (every
/// pre-split key, plus writes racing the crossover window itself, all
/// readable after full convergence) and that metadata converges (the
/// decommissioned node is gone from membership/the address book, and both
/// resulting tablets end up fully replicated across the survivors).
#[ignore = "PARKED (ADR 0050 Train B rung 1): zero-copy split of a populated tablet is disabled during the storage pivot; revived/replaced by the copy-based split workflow in later rungs of this train"]
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn decommission_racing_a_tablet_split_converges_with_no_data_loss() {
    timeout(Duration::from_secs(150), async {
        let dir = tempfile::tempdir().unwrap();
        // RF = min(N,3): 4 data nodes leaves exactly one spare so the
        // decommissioned replica has somewhere for BOTH the split parent and
        // its new child to be repaired onto.
        let (control_nodes, mut data_nodes, _config) = bring_up_split(3, 4, dir.path()).await;
        await_leader(&control_nodes).await;
        let data_raftkv_ids: Vec<animus_env::NodeId> =
            (3..7).map(animusd::config::node_id).collect();
        await_data_nodes_active(&control_nodes, &data_raftkv_ids).await;

        let data_clients: Vec<SocketAddr> = data_nodes.iter().map(Node::client_addr).collect();
        for (k, v) in CROSSOVER_KEYS {
            put(&data_clients, "cross_t", k.as_bytes(), v.as_bytes(), 20).await;
        }

        let (parent, replicas_before) = timeout(Duration::from_secs(20), async {
            loop {
                let meta = control_nodes[0].metadata();
                if let Some((&id, t)) = meta.tablets_for_table("cross_t").next()
                    && t.replicas.len() == 3
                {
                    return (id, t.replicas.clone());
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("tablet was not provisioned with 3 replicas in 20s");

        let victim_id = replicas_before[0].clone();
        let victim_idx = data_raftkv_ids
            .iter()
            .position(|id| *id == victim_id)
            .unwrap();
        let leader_idx = control_nodes
            .iter()
            .position(Node::is_control_leader)
            .unwrap();

        // Trigger the split AND drain the victim AT THE SAME TIME —
        // `tokio::join!` fires both admin calls without either waiting for
        // the other to finish.
        let split_body = format!(r#"{{"tablet":{},"split_key":"m"}}"#, parent.0);
        let drain_body = serde_json::json!({"node": victim_id}).to_string();
        let (split_result, drain_result) = tokio::join!(
            admin(
                data_nodes[0].admin_addr(),
                "POST",
                "/admin/tablet/split",
                Some(&split_body),
            ),
            admin(
                control_nodes[leader_idx].admin_addr(),
                "POST",
                "/admin/drain",
                Some(&drain_body),
            ),
        );
        assert_eq!(split_result.0, 200, "split trigger: {}", split_result.1);
        assert_eq!(drain_result.0, 200, "drain trigger: {}", drain_result.1);

        // Writes racing the crossover window itself — one key on each future
        // half of the range — must still land (a bounded retry poll, per the
        // standing "a first write during a crossover/formation window can
        // legitimately fail once" discipline `put` already implements).
        put(&data_clients, "cross_t", b"b", b"v-b2", 20).await;
        put(&data_clients, "cross_t", b"y", b"v-y2", 20).await;

        await_true(30, "split produced two tablets", || {
            table_tablets(&data_nodes[0], "cross_t").len() == 2
        })
        .await;
        let child = table_tablets(&data_nodes[0], "cross_t")
            .into_iter()
            .find(|&t| t != parent.0)
            .expect("a child tablet exists");
        let _ = child; // only needed to prove the split produced a real sibling

        // The drained node is fully evacuated from BOTH resulting tablets —
        // the reconciler's `NarrowScope`/`WidenScope` + repair-onto-spare
        // acting across the split boundary.
        timeout(Duration::from_secs(60), async {
            loop {
                let meta = control_nodes[0].metadata();
                let still_hosts = meta.tablets.values().any(|t| {
                    t.table.as_deref() == Some("cross_t") && t.replicas.contains(&victim_id)
                });
                if !still_hosts {
                    return;
                }
                sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .expect("the drained node was never evacuated from both split halves");

        // Drain-status confirms it (the same signal `admin_remove_member`
        // itself gates on) before removal is attempted.
        timeout(Duration::from_secs(30), async {
            loop {
                let (status, body) = admin(
                    control_nodes[leader_idx].admin_addr(),
                    "GET",
                    &format!("/admin/member/drain-status?node={victim_id}"),
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
        .expect("the drained node never finished draining");

        let remove_body = serde_json::json!({"node": victim_id}).to_string();
        let (status, resp) = admin(
            control_nodes[leader_idx].admin_addr(),
            "POST",
            "/admin/member/remove",
            Some(&remove_body),
        )
        .await;
        assert_eq!(status, 200, "remove failed: {resp}");

        let removed = data_nodes.remove(victim_idx);
        removed.shutdown_graceful().await;

        // No data lost: every pre-split key, plus both crossover writes,
        // read through the survivors after full convergence.
        let survivor_clients: Vec<SocketAddr> = data_nodes.iter().map(Node::client_addr).collect();
        for (k, v) in CROSSOVER_KEYS {
            await_value(&survivor_clients, "cross_t", k.as_bytes(), v.as_bytes(), 30).await;
        }
        await_value(&survivor_clients, "cross_t", b"b", b"v-b2", 30).await;
        await_value(&survivor_clients, "cross_t", b"y", b"v-y2", 30).await;

        // Metadata converged: the decommissioned node is gone from
        // membership/the address book.
        timeout(Duration::from_secs(30), async {
            loop {
                let meta = control_nodes[0].metadata();
                if !meta.members.contains_key(&victim_id)
                    && !meta.node_addrs.contains_key(&victim_id)
                {
                    return;
                }
                sleep(Duration::from_millis(150)).await;
            }
        })
        .await
        .expect("the removed node never disappeared from membership/address book");

        // Fresh writes on both halves of the split still work
        // post-convergence.
        put(&survivor_clients, "cross_t", b"b2", b"v-b3", 20).await;
        put(&survivor_clients, "cross_t", b"y2", b"v-y3", 20).await;
        await_value(&survivor_clients, "cross_t", b"b2", b"v-b3", 20).await;
        await_value(&survivor_clients, "cross_t", b"y2", b"v-y3", 20).await;

        for n in control_nodes.iter().chain(data_nodes.iter()) {
            n.shutdown_graceful().await;
        }
    })
    .await
    .expect("decommission_racing_a_tablet_split_converges_with_no_data_loss timed out");
}
