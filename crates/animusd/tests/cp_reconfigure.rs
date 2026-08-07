//! **Dynamic CP reconfigure + failure-detection over `ProdEnv`** (#3, ADR 0017
//! Stage C / ADR 0012). The membership *mechanism* and the SimEnv failure→reconfigure
//! cascade are proven deterministically in `animus-cp-data`
//! (`tests/reconfigure_trigger.rs`); these tests cover the **production wiring** over
//! real TCP/time, which the deterministic suite cannot — a class of timing/wiring
//! bugs the crate guide calls out as `ProdEnv`-only.
//!
//! 1. `data_node_failure_is_detected` — each node heartbeats the control group *as
//!    its `raftkv` member id*, so the control leader's `detect_loop` marks a crashed
//!    CP node `Down` in replicated `Metadata`.
//! 2. `cp_group_follows_tablet_replica_set` — the per-node CP reconfigure loop steps a
//!    group it leads toward the tablet's replicated replica set: dropping a follower
//!    from `Metadata.tablets[t].replicas` makes the group reconfigure its Raft voters
//!    down to the new set (observed via the admin `/admin/raftkv` view).
//! 3. `failure_auto_replaces_replica_onto_spare` — the full cascade (D1): a 4-node
//!    cluster (RF 3 + a spare) auto-heals a killed replica — detector marks it Down,
//!    the RF policy's reconciler swaps in the spare, the spare join-hosts an empty
//!    group and is added as a voter, and the group keeps serving.
//!
//! Real TCP/time — polls with generous timeouts, not deterministic assertions.

use std::net::SocketAddr;
use std::time::Duration;

use animus_tablet::{Epoch, TabletId};
use animusd::{
    ClientRequest, ClientResponse, ClusterConfig, MetaCommand, Node, NodeStatus, RoleAddrs,
    bind_cluster, read_frame, start_cluster,
};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

const BOOTSTRAP_TABLET: TabletId = TabletId(1);

async fn await_bootstrap(nodes: &[Node]) {
    let ready = async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
                && nodes.iter().all(|node| !node.metadata().members.is_empty())
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(30), ready)
        .await
        .expect("cluster did not bootstrap within 30s");
}

// ---- Test 1: data-node failure detection over ProdEnv -----------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn data_node_failure_is_detected() {
    let dir = tempfile::tempdir().unwrap();
    let ip = "127.0.0.1".parse().unwrap();
    let nodes = start_cluster(bind_cluster(3, ip, dir.path()).await.unwrap())
        .await
        .unwrap();
    await_bootstrap(&nodes).await;

    // All three CP `raftkv` ids start Active (bootstrap registered them as members).
    let active = async {
        loop {
            let m = nodes[0].metadata();
            if m.members.len() == 3 && m.members.values().all(|x| x.status == NodeStatus::Active) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), active)
        .await
        .expect("all members did not become Active within 20s");

    // Crash node 2 (its heartbeat loop is aborted with the node). Use a surviving
    // node that is *not* node 2 to observe — and one likely to host the control
    // leader (node 0/1).
    nodes[2].shutdown();

    let down = async {
        loop {
            // Read from whichever survivor currently leads the control plane (a
            // follower's view tracks the leader's committed `Down`).
            for node in &nodes[..2] {
                if node
                    .metadata()
                    .members
                    .values()
                    .any(|m| m.status == NodeStatus::Down)
                {
                    return;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(30), down)
        .await
        .expect("the crashed CP node was not marked Down within 30s");

    nodes[0].shutdown();
    nodes[1].shutdown();
}

// ---- Test 2: CP group follows the replicated replica set --------------------

async fn admin_get(addr: SocketAddr, path: &str) -> (u16, Value) {
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
    let value: Value = serde_json::from_str(payload).expect("admin body is JSON");
    (status, value)
}

/// The local group's `(is_leader, voters)` for the bootstrap tablet, from this
/// node's node-local admin view (per-process: one group per node).
async fn group_view(admin_addr: SocketAddr) -> Option<(bool, Vec<u64>)> {
    let (_s, v) = admin_get(admin_addr, "/admin/raftkv").await;
    let g = v["groups"].as_array()?.iter().find(|g| g["tablet"] == 1)?;
    let voters = g["voters"]
        .as_array()?
        .iter()
        .filter_map(|x| x.as_u64())
        .collect();
    Some((g["is_leader"].as_bool().unwrap_or(false), voters))
}

/// Bring up `n` nodes one-process-per-node (node-local admin views), retrying the
/// (alloc fresh ports + start) unit on a bind race. Returns the bound config.
async fn bring_up(n: usize, dir: &std::path::Path) -> (Vec<Node>, ClusterConfig) {
    for attempt in 0..16 {
        let a: Vec<SocketAddr> = {
            let ls: Vec<std::net::TcpListener> = (0..n * 6)
                .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
                .collect();
            ls.iter().map(|l| l.local_addr().unwrap()).collect()
        };
        let cfg = ClusterConfig {
            nodes: (0..n)
                .map(|i| RoleAddrs {
                    control: a[6 * i],
                    client: a[6 * i + 1],
                    dynamo: a[6 * i + 2],
                    cql: a[6 * i + 3],
                    raftkv: a[6 * i + 4],
                    admin: a[6 * i + 5],
                })
                .collect(),
        };
        let mut nodes = Vec::new();
        let mut ok = true;
        for i in 0..n {
            match animusd::run_node(&cfg, i, dir.join(format!("node-{attempt}-{i}"))).await {
                Ok(node) => nodes.push(node),
                Err(_) => {
                    ok = false;
                    break;
                }
            }
        }
        if ok {
            return (nodes, cfg);
        }
        for node in &nodes {
            node.shutdown();
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("could not bring up cluster");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cp_group_follows_tablet_replica_set() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(3, dir.path()).await;
    await_bootstrap(&nodes).await;
    let raftkv_ids = config.raftkv_ids(); // [300, 301, 302]

    // ADR 0023: a fresh cluster has no data tablet — provision the `kv` tablet by
    // writing first, so the CP group forms (auto-provisioned on the first write).
    let clients: Vec<SocketAddr> = config.nodes.iter().map(|a| a.client).collect();
    put(&clients, b"k0", b"v0", 30).await;

    // Wait until the CP group has formed with all three voters and elected a leader
    // on some node.
    let leader_idx = {
        let formed = async {
            loop {
                for (i, node) in nodes.iter().enumerate() {
                    if let Some((is_leader, voters)) = group_view(node.admin_addr()).await {
                        if is_leader && voters.len() == 3 {
                            return i;
                        }
                    }
                }
                sleep(Duration::from_millis(100)).await;
            }
        };
        timeout(Duration::from_secs(30), formed)
            .await
            .expect("CP group did not form with 3 voters + a leader within 30s")
    };

    // Drop a *follower* (the leader can't remove itself) from the tablet's replica
    // set: keep the leader + the other follower.
    let drop_idx = (0..3)
        .find(|&i| i != leader_idx)
        .expect("a follower exists");
    let kept: Vec<u64> = raftkv_ids
        .iter()
        .copied()
        .filter(|&id| id != raftkv_ids[drop_idx])
        .collect();
    assert_eq!(kept.len(), 2);

    // Commit the replica-set change on the control leader (epoch-CAS). Retry on a
    // racing epoch bump / leadership move until the tablet map shows two replicas.
    let change = async {
        loop {
            let epoch: Epoch = nodes[0]
                .metadata()
                .tablets
                .get(&BOOTSTRAP_TABLET)
                .expect("bootstrap tablet")
                .epoch;
            let cmd = MetaCommand::CasTabletReplicas {
                tablet: BOOTSTRAP_TABLET,
                expected_epoch: epoch,
                replicas: kept.clone(),
            };
            for node in &nodes {
                if node.is_control_leader() {
                    node.propose_meta(cmd.clone());
                }
            }
            if nodes[0]
                .metadata()
                .tablets
                .get(&BOOTSTRAP_TABLET)
                .is_some_and(|t| t.replicas.len() == 2)
            {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(20), change)
        .await
        .expect("replica-set change did not replicate within 20s");

    // The reconfigure loop on the leader's node steps the group's Raft config to the
    // new set: the leader now reports two voters, and the dropped id is gone.
    //
    // ADR 0029: removing a *healthy* extra voter (this is a plain drop, not a
    // failure repair — nothing here is `Down`) is now gated on the surviving
    // kept replica being caught up to the leader's commit index, so this can
    // take one extra heartbeat round trip versus the old unconditional removal
    // — a wider, 60s timeout (matching this file's spare-replacement test
    // below) absorbs that under real `cargo test --workspace` contention.
    let dropped = raftkv_ids[drop_idx];
    let reconfigured = async {
        loop {
            if let Some((is_leader, voters)) = group_view(nodes[leader_idx].admin_addr()).await {
                if is_leader && voters.len() == 2 && !voters.contains(&dropped) {
                    return;
                }
            }
            sleep(Duration::from_millis(150)).await;
        }
    };
    timeout(Duration::from_secs(60), reconfigured)
        .await
        .expect("CP group did not reconfigure to the new replica set within 60s");

    for node in nodes {
        node.shutdown();
    }
}

// ---- Test 3: full failure->placement->reconfigure cascade with a spare (D1) --

/// `group_view` that returns `None` instead of panicking when the admin endpoint is
/// unreachable (a killed node), so the cascade test can poll only the survivors.
async fn group_view_opt(addr: SocketAddr) -> Option<(bool, Vec<u64>)> {
    if TcpStream::connect(addr).await.is_err() {
        return None;
    }
    group_view(addr).await
}

async fn call(addr: SocketAddr, req: ClientRequest) -> Option<ClientResponse> {
    let mut stream = TcpStream::connect(addr).await.ok()?;
    animusd::write_frame(&mut stream, &req).await.ok()?;
    read_frame(&mut stream).await.ok()?
}

async fn put(clients: &[SocketAddr], key: &[u8], value: &[u8], secs: u64) {
    let w = async {
        loop {
            for &c in clients {
                if let Some(ClientResponse::PutOk) = call(
                    c,
                    ClientRequest::Put {
                        key: key.to_vec(),
                        value: value.to_vec(),
                        table: "kv".to_string(),
                    },
                )
                .await
                {
                    return;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(secs), w)
        .await
        .unwrap_or_else(|_| panic!("write of {key:?} never committed"));
}

async fn await_value(clients: &[SocketAddr], key: &[u8], want: &[u8], secs: u64) {
    let p = async {
        loop {
            for &c in clients {
                if let Some(ClientResponse::Value(Some(v))) = call(
                    c,
                    ClientRequest::Get {
                        key: key.to_vec(),
                        table: "kv".to_string(),
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
        .unwrap_or_else(|_| panic!("key {key:?} never read back as {want:?}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn failure_auto_replaces_replica_onto_spare() {
    // 4 nodes: RF=3, so the bootstrap tablet is placed on nodes 0..3 (raftkv
    // 300..302) and node 3 (raftkv 303) is an idle spare. Killing a replica should
    // cascade: detector marks it Down -> reconciler swaps in the spare -> the group
    // reconfigures onto it and keeps serving.
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(4, dir.path()).await;
    await_bootstrap(&nodes).await;
    let raftkv_ids = config.raftkv_ids(); // [300, 301, 302, 303]
    let spare = raftkv_ids[3];
    let clients: Vec<SocketAddr> = config.nodes.iter().map(|a| a.client).collect();

    // ADR 0023: provision the `kv` tablet by writing first (no bootstrap tablet); it
    // lands on the first RF=3 members (300..302), leaving 303 as the idle spare.
    put(&clients, b"k0", b"v0", 30).await;

    // Wait for the group to form with 3 voters + a leader; write a key.
    let leader_idx = {
        let formed = async {
            loop {
                for (i, node) in nodes.iter().enumerate() {
                    if let Some((true, voters)) = group_view(node.admin_addr()).await {
                        if voters.len() == 3 {
                            return i;
                        }
                    }
                }
                sleep(Duration::from_millis(100)).await;
            }
        };
        timeout(Duration::from_secs(30), formed)
            .await
            .expect("CP group did not form with 3 voters + a leader within 30s")
    };
    put(&clients, b"k1", b"v1", 20).await;

    // Kill a **follower** replica (the leader can't remove itself, and killing a
    // follower keeps the test's leader stable). A replica node hosts tablet 1
    // (group_view Some); the spare (node 3) does not.
    let kill_idx = (0..3)
        .find(|&i| i != leader_idx)
        .expect("a follower replica exists");
    let killed_id = raftkv_ids[kill_idx];
    nodes[kill_idx].shutdown();
    let survivors: Vec<usize> = (0..4).filter(|&i| i != kill_idx).collect();
    let survivor_clients: Vec<SocketAddr> =
        survivors.iter().map(|&i| config.nodes[i].client).collect();

    // The cascade: the tablet map drops the dead replica and adds the spare.
    let replaced = async {
        loop {
            for &i in &survivors {
                if let Some(t) = nodes[i].metadata().tablets.get(&BOOTSTRAP_TABLET) {
                    if t.replicas.contains(&spare) && !t.replicas.contains(&killed_id) {
                        return;
                    }
                }
            }
            sleep(Duration::from_millis(150)).await;
        }
    };
    timeout(Duration::from_secs(60), replaced)
        .await
        .expect("the dead replica was not auto-replaced by the spare within 60s");

    // The group reconfigured onto the spare: some survivor leads with 3 voters that
    // include the spare and exclude the dead node (i.e. the spare join-hosted an
    // empty group and was added as a voter).
    let reconfigured = async {
        loop {
            for &i in &survivors {
                if let Some((true, voters)) = group_view_opt(config.nodes[i].admin).await {
                    if voters.len() == 3 && voters.contains(&spare) && !voters.contains(&killed_id)
                    {
                        return;
                    }
                }
            }
            sleep(Duration::from_millis(150)).await;
        }
    };
    timeout(Duration::from_secs(60), reconfigured)
        .await
        .expect("the CP group did not reconfigure onto the spare within 60s");

    // The healed group still serves: the old key survives and a new write commits.
    await_value(&survivor_clients, b"k1", b"v1", 30).await;
    put(&survivor_clients, b"k2", b"v2", 30).await;
    await_value(&survivor_clients, b"k2", b"v2", 30).await;

    for &i in &survivors {
        nodes[i].shutdown();
    }
}
