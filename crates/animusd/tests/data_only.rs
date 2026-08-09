//! `animusd data` — the data-only process with `ControlHandle::Remote` (ADR
//! 0035 PR4): no local control `RaftCore` at all, reaching a
//! separately-deployed control plane exclusively over the network.
//!
//! Covers, over real TCP/time (so every wait is a bounded poll, never a
//! fixed sleep):
//! - a genuine split cluster (3 control-only + 2 data-only nodes, no
//!   combined-mode node anywhere) converges: data nodes self-register via
//!   the relayed `admin_add_member`, get promoted `Active` by the unmodified
//!   ADR 0012 heartbeat/failure-detector chain, a table provisions onto
//!   them, and `Put`/`Get` work through a data node — including a read
//!   served by a *different* data node than the one written through;
//! - schema DDL issued against a data node relays to the control leader and
//!   commits, visible from every node (`metadata_fresh` soundness: the data
//!   node's own commit-wait poll must observe its just-proposed command,
//!   never a stale mirror);
//! - one control node down (of 3): a data node's mirror/leader-hint sync
//!   loop falls over to a remaining seed and traffic continues;
//! - a data-node restart: it rejoins, re-hosts its tablets via the
//!   tablet-host reconciler, and serves a pre-restart write again (a
//!   converged-or-timeout poll, no leadership gate — a data-only node is
//!   never a "leader" at all).

use std::net::SocketAddr;
use std::time::Duration;

use animusd::config::NodeRole;
use animusd::{
    ClientRequest, ClientResponse, ClusterConfig, ColumnType, MetaCommand, Node, RoleAddrs,
    StorageBackend, TableSchema, read_frame,
};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;
use support::free_addrs;

async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    animusd::write_frame(&mut stream, &req).await.expect("send");
    read_frame(&mut stream)
        .await
        .expect("read")
        .expect("a reply")
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

/// Bring up a genuine split cluster: `control_n` control-only nodes
/// (`animusd control`'s `run_node_control`) plus `data_n` data-only nodes
/// (`animusd data`'s `run_node_data`, `ControlHandle::Remote`) — **no**
/// combined-mode node anywhere, one process (in this test binary) per node,
/// each its own `ClusterEdgeState`. Retries the (allocate-fresh-ports +
/// start-all) as a unit, mirroring `tests/control_only.rs`'s
/// `bring_up_control` (the documented port-TOCTOU mitigation).
async fn bring_up_split(
    control_n: usize,
    data_n: usize,
    dir: &std::path::Path,
) -> (Vec<Node>, Vec<Node>, ClusterConfig) {
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

        let mut control_nodes = Vec::new();
        let mut data_nodes = Vec::new();
        let mut failed = false;
        for i in 0..control_n {
            match animusd::run_node_control(&config, i, dir.join(format!("a{attempt}-c{i}"))).await
            {
                Ok(n) => control_nodes.push(n),
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        if !failed {
            for i in control_n..total {
                match animusd::run_node_data(
                    &config,
                    i,
                    dir.join(format!("a{attempt}-d{i}")),
                    StorageBackend::Memory,
                )
                .await
                {
                    Ok(n) => data_nodes.push(n),
                    Err(_) => {
                        failed = true;
                        break;
                    }
                }
            }
        }
        if !failed {
            return (control_nodes, data_nodes, config);
        }
        for n in control_nodes.iter().chain(data_nodes.iter()) {
            n.shutdown_graceful().await;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("could not bring up split cluster after retries (ports kept getting stolen)");
}

async fn await_leader(control_nodes: &[Node]) {
    timeout(Duration::from_secs(20), async {
        loop {
            if control_nodes.iter().any(Node::is_control_leader) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("control deployment did not elect a leader in 20s");
}

/// Wait for every data node's raftkv id to become `Active` in the control
/// deployment's own metadata (the unmodified ADR 0012 heartbeat/detector
/// promotion chain — `tests/cluster_growth.rs` is the existing proof this
/// mechanism works unattended; no test-side force here).
async fn await_data_nodes_active(control_nodes: &[Node], data_raftkv_ids: &[animus_env::NodeId]) {
    timeout(Duration::from_secs(20), async {
        loop {
            if data_raftkv_ids.iter().all(|id| {
                control_nodes.iter().any(|n| {
                    n.metadata().members.get(id).map(|m| m.status)
                        == Some(animusd::NodeStatus::Active)
                })
            }) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("data nodes did not become Active in 20s");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn split_cluster_serves_reads_and_writes_across_data_nodes() {
    timeout(Duration::from_secs(90), async {
        let dir = tempfile::tempdir().unwrap();
        let (control_nodes, data_nodes, config) = bring_up_split(3, 2, dir.path()).await;
        await_leader(&control_nodes).await;

        let data_raftkv_ids: Vec<animus_env::NodeId> =
            (3..5).map(animusd::config::raftkv_id).collect();
        await_data_nodes_active(&control_nodes, &data_raftkv_ids).await;

        // A `Put` issued against ONE data node's client port; a `Get`
        // against the OTHER data node — proving both provisioning (the
        // table's tablet is created with the two `Active` data members as
        // replicas) and cross-data-node routing/forwarding work with **no**
        // control node involved in the data path at all.
        timeout(Duration::from_secs(20), async {
            loop {
                let put = call(
                    data_nodes[0].client_addr(),
                    ClientRequest::Put {
                        key: b"split-key".to_vec(),
                        value: b"split-val".to_vec(),
                        table: "split_t".to_string(),
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
        .expect("put via a data node did not succeed in 20s");

        let get = call(
            data_nodes[1].client_addr(),
            ClientRequest::Get {
                key: b"split-key".to_vec(),
                table: "split_t".to_string(),
            },
        )
        .await;
        assert_eq!(
            get,
            ClientResponse::Value(Some(b"split-val".to_vec())),
            "read-back via the other data node"
        );

        // The data-only nodes' own `/admin/health` reports data-plane
        // readiness with `is_control_leader` hardcoded false (no local
        // control RaftCore to ever lead) — falls out of `ControlHandle::
        // Remote::is_leader()` returning `false` unconditionally, no
        // Remote-specific code in `admin.rs` at all. `hosts_cp` is polled,
        // not snapshotted immediately: the Get above can succeed via a
        // one-hop forward before *this* node's own tablet-host reconciler
        // has finished standing its own replica of the group up (an
        // eventual, not immediate, property of a just-provisioned tablet).
        for n in &data_nodes {
            let (status, health) = admin_get(n.admin_addr(), "/admin/health").await;
            assert_eq!(status, 200, "admin/health on {}", n.admin_addr());
            assert_eq!(
                health["is_control_leader"], false,
                "a data-only node never leads the control plane: {health}"
            );
        }
        timeout(Duration::from_secs(20), async {
            loop {
                let mut all_host = true;
                for n in &data_nodes {
                    let (_, health) = admin_get(n.admin_addr(), "/admin/health").await;
                    if health["hosts_cp"] != true {
                        all_host = false;
                    }
                }
                if all_host {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("both data nodes never converged to hosting the tablet's CP group (20s)");

        // A control-only node's own `/admin/config` still has no raftkv id
        // (unchanged from ADR 0035 PR3); a data-only node's has no control
        // id at all (ADR 0035 PR4's new case).
        for n in &data_nodes {
            let (status, cfg) = admin_get(n.admin_addr(), "/admin/config").await;
            assert_eq!(status, 200);
            assert!(
                cfg["control_id"].is_null(),
                "a data-only node has no local control id: {cfg}"
            );
            assert!(
                cfg["addrs"]["control"].is_null(),
                "a data-only node has no control address: {cfg}"
            );
        }

        for n in control_nodes.iter().chain(data_nodes.iter()) {
            n.shutdown_graceful().await;
        }
        let _ = config;
    })
    .await
    .expect("split_cluster_serves_reads_and_writes_across_data_nodes timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn schema_ddl_via_a_data_node_relays_and_commits() {
    timeout(Duration::from_secs(90), async {
        let dir = tempfile::tempdir().unwrap();
        let (control_nodes, data_nodes, _config) = bring_up_split(3, 2, dir.path()).await;
        await_leader(&control_nodes).await;

        // A data-only node can never satisfy `propose_schema`'s local-leader
        // branch (it holds no control Raft role at all) — this proves the
        // relay path (`leader_addr_hint`-then-broadcast, ADR 0035 §1) reaches
        // the real control leader from a node with zero control-plane state
        // of its own at process start.
        let create = MetaCommand::CreateTableSchema {
            table: "data_ddl_t".into(),
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
                    .all(|n| n.metadata().has_table_schema("data_ddl_t"))
                {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("data-node-issued schema did not relay + commit in 20s");

        // Every data node's own mirror converges to the same schema too
        // (`ControlHandle::Remote::metadata_cached()`), not just the control
        // deployment's own replicas.
        timeout(Duration::from_secs(20), async {
            loop {
                let (status, cfg) = admin_get(data_nodes[1].admin_addr(), "/admin/status").await;
                assert_eq!(status, 200);
                if cfg["schemas"]["tables"]
                    .as_object()
                    .is_some_and(|t| t.contains_key("data_ddl_t"))
                {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("the other data node's mirror never observed the schema in 20s");

        for n in control_nodes.iter().chain(data_nodes.iter()) {
            n.shutdown_graceful().await;
        }
    })
    .await
    .expect("schema_ddl_via_a_data_node_relays_and_commits timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn data_node_falls_over_to_a_remaining_control_seed() {
    timeout(Duration::from_secs(90), async {
        let dir = tempfile::tempdir().unwrap();
        let (mut control_nodes, data_nodes, _config) = bring_up_split(3, 2, dir.path()).await;
        await_leader(&control_nodes).await;

        let data_raftkv_ids: Vec<animus_env::NodeId> =
            (3..5).map(animusd::config::raftkv_id).collect();
        await_data_nodes_active(&control_nodes, &data_raftkv_ids).await;

        // Stop a control node that is NOT the current leader (stopping the
        // leader would just force a re-election among the remaining two,
        // which is a different, already-covered scenario) — the data
        // nodes' `remote_metadata_sync_loop`/`RemoteControlClient` must fall
        // over to a remaining seed rather than getting stuck retrying a dead
        // one forever.
        let leader = control_nodes
            .iter()
            .position(Node::is_control_leader)
            .unwrap();
        let victim = (0..control_nodes.len()).find(|&i| i != leader).unwrap();
        let stopped = control_nodes.remove(victim);
        stopped.shutdown_graceful().await;

        // Traffic through a data node must keep working: a *new* write,
        // issued only after the control node is down, still has to reach
        // the (still up) leader through the mirror's seed-scan fallback.
        timeout(Duration::from_secs(20), async {
            loop {
                let put = call(
                    data_nodes[0].client_addr(),
                    ClientRequest::Put {
                        key: b"post-failure-key".to_vec(),
                        value: b"post-failure-val".to_vec(),
                        table: "split_t2".to_string(),
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
        .expect("put via a data node did not succeed after a control node went down (20s)");

        let get = call(
            data_nodes[1].client_addr(),
            ClientRequest::Get {
                key: b"post-failure-key".to_vec(),
                table: "split_t2".to_string(),
            },
        )
        .await;
        assert_eq!(
            get,
            ClientResponse::Value(Some(b"post-failure-val".to_vec()))
        );

        for n in control_nodes.iter().chain(data_nodes.iter()) {
            n.shutdown_graceful().await;
        }
    })
    .await
    .expect("data_node_falls_over_to_a_remaining_control_seed timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn data_node_restart_rejoins_and_serves_reads_again() {
    timeout(Duration::from_secs(90), async {
        let dir = tempfile::tempdir().unwrap();
        let (control_nodes, mut data_nodes, config) = bring_up_split(3, 2, dir.path()).await;
        await_leader(&control_nodes).await;

        let data_raftkv_ids: Vec<animus_env::NodeId> =
            (3..5).map(animusd::config::raftkv_id).collect();
        await_data_nodes_active(&control_nodes, &data_raftkv_ids).await;

        timeout(Duration::from_secs(20), async {
            loop {
                let put = call(
                    data_nodes[0].client_addr(),
                    ClientRequest::Put {
                        key: b"restart-key".to_vec(),
                        value: b"restart-val".to_vec(),
                        table: "split_t3".to_string(),
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
        .expect("initial put did not succeed in 20s");

        // Restart data node 0 on the same addresses + data dir. A clean
        // teardown frees its ports (`shutdown_graceful`); rebind on the same
        // config/dir is `run_node_data` again — the standing "restart-one-
        // node" lesson applies (no leadership gate: a data-only node is
        // never a leader of anything; poll for catch-up instead).
        let stopped = data_nodes.remove(0);
        stopped.shutdown_graceful().await;
        let restarted = timeout(Duration::from_secs(10), async {
            loop {
                match animusd::run_node_data(
                    &config,
                    3,
                    dir.path().join("a0-d3"),
                    StorageBackend::Memory,
                )
                .await
                {
                    Ok(n) => return n,
                    Err(_) => sleep(Duration::from_millis(100)).await,
                }
            }
        })
        .await
        .expect("data node did not rebind on restart in 10s");

        // Poll for the restarted node to re-host the tablet and serve the
        // pre-restart write again — the reconciler re-discovers what to
        // host from the (mirrored) replicated `Metadata`, not local state
        // (a data-only node keeps nothing across a restart except the
        // shared engine's own durable data, which `--ephemeral` here does
        // NOT persist — so this specifically proves catch-up via the OTHER
        // still-hosting replica's data reaching this node through Raft
        // replication onto a freshly re-formed group member, not merely a
        // local reopen).
        timeout(Duration::from_secs(20), async {
            loop {
                let get = call(
                    restarted.client_addr(),
                    ClientRequest::Get {
                        key: b"restart-key".to_vec(),
                        table: "split_t3".to_string(),
                    },
                )
                .await;
                if get == ClientResponse::Value(Some(b"restart-val".to_vec())) {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("restarted data node never caught up + served the pre-restart write (20s)");

        restarted.shutdown_graceful().await;
        for n in control_nodes.iter().chain(data_nodes.iter()) {
            n.shutdown_graceful().await;
        }
    })
    .await
    .expect("data_node_restart_rejoins_and_serves_reads_again timed out");
}
