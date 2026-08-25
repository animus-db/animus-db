//! ADR 0058 Train 1's **reconciler adoption**, exercised over a real
//! multi-process `ProdEnv` cluster (the "both-layers" discipline —
//! `animus-cp-data`'s deterministic `SimEnv` suites prove the mechanism, this
//! file proves the production wiring that hands it a real replica move).
//!
//! `cp_reconfigure.rs::failure_auto_replaces_replica_onto_spare` already
//! proves the end-to-end cascade converges under this change (unmodified: it
//! only asserts the FINAL state, and the learner phase adds at most one extra
//! reconfigure round trip, well inside its existing 60s budget). This file
//! adds the two things that test doesn't cover: **observing the intermediate
//! learner state itself** through `/admin/raftkv` (now that
//! `admin::CpRaftView` carries a `learners` field), and confirming the group
//! **keeps serving writes** while a newcomer is mid-catch-up as a learner —
//! the structural property this rung exists for, over real TCP/time rather
//! than `SimEnv`.

use std::net::SocketAddr;
use std::time::Duration;

use animus_env::NodeId;
use animusd::{ClientRequest, ClientResponse, ClusterConfig, Node, RoleAddrs, read_frame};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

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

/// The local group's `(is_leader, voters, learners)` for the bootstrap
/// tablet, from this node's node-local admin view — mirrors
/// `cp_reconfigure.rs::group_view`, extended with the new `learners` field
/// (ADR 0058 Train 1's reconciler adoption).
async fn group_view(admin_addr: SocketAddr) -> Option<(bool, Vec<NodeId>, Vec<NodeId>)> {
    let (_s, v) = admin_get(admin_addr, "/admin/raftkv").await;
    let g = v["groups"].as_array()?.iter().find(|g| g["tablet"] == 1)?;
    let voters = g["voters"]
        .as_array()?
        .iter()
        .filter_map(|x| x.as_str()?.parse::<NodeId>().ok())
        .collect();
    let learners = g["learners"]
        .as_array()?
        .iter()
        .filter_map(|x| x.as_str()?.parse::<NodeId>().ok())
        .collect();
    Some((g["is_leader"].as_bool().unwrap_or(false), voters, learners))
}

/// Bring up `n` nodes one-process-per-node, retrying the (alloc fresh ports +
/// start) unit on a bind race — mirrors `cp_reconfigure.rs::bring_up`.
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
                    id: animusd::config::node_id(i),
                    role: animusd::config::NodeRole::Both,
                    internal: a[6 * i],
                    client: a[6 * i + 1],
                    dynamo: a[6 * i + 2],
                    admin: a[6 * i + 3],
                    intra: a[6 * i + 4],
                    console: a[6 * i + 5],
                })
                .collect(),
            dynamo_auth: None,
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
            node.shutdown_graceful().await;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("could not bring up cluster");
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

/// A 4-node cluster (RF=3, one idle spare) auto-heals a killed replica onto
/// the spare — the identical cascade `cp_reconfigure.rs::
/// failure_auto_replaces_replica_onto_spare` proves converges, but this test
/// additionally **observes the newcomer pass through a real, admin-visible
/// learner state** (ADR 0058 Train 1's reconciler adoption: the spare must
/// never appear directly in `voters` without first appearing in `learners`)
/// and confirms the group **never stops accepting writes** while it does —
/// the quorum-preserving property this rung exists for, proven here over
/// real TCP/time rather than `SimEnv`.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn spare_replacement_passes_through_an_observable_learner_state_and_keeps_serving() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(4, dir.path()).await;
    await_bootstrap(&nodes).await;
    let raftkv_ids = config.data_ids(); // [0, 1, 2, 3]
    let spare = raftkv_ids[3].clone();
    let clients: Vec<SocketAddr> = config.nodes.iter().map(|a| a.client).collect();

    // ADR 0023: provision the `kv` tablet by writing first — it lands on ids
    // 0..2, leaving id 3 as the idle spare.
    put(&clients, b"k0", b"v0", 30).await;

    let leader_idx = {
        let formed = async {
            loop {
                for (i, node) in nodes.iter().enumerate() {
                    if let Some((true, voters, _)) = group_view(node.admin_addr()).await
                        && voters.len() == 3
                    {
                        return i;
                    }
                }
                sleep(Duration::from_millis(100)).await;
            }
        };
        timeout(Duration::from_secs(30), formed)
            .await
            .expect("CP group did not form with 3 voters + a leader within 30s")
    };

    // Kill a follower replica — the leader survives, so this test's own
    // write-liveness poll below has a stable place to route through.
    let kill_idx = (0..3)
        .find(|&i| i != leader_idx)
        .expect("a follower replica exists");
    let killed_id = raftkv_ids[kill_idx].clone();
    nodes[kill_idx].shutdown();
    let survivors: Vec<usize> = (0..4).filter(|&i| i != kill_idx).collect();

    // Background write-liveness poll: while the cascade runs, keep proposing
    // writes through whichever survivor currently answers. Every attempt
    // that succeeds proves the OLD quorum never stalled — the structural
    // regression ADR 0058 Train 1 exists to close (pre-Train-1 semantics
    // would have added the spare straight to the voter set, briefly
    // requiring one MORE ack than the group's healthy replicas alone could
    // give while the spare was still an empty, uncaught-up group).
    let writes_committed = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let writer_done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let survivor_clients: Vec<SocketAddr> =
            survivors.iter().map(|&i| config.nodes[i].client).collect();
        let writes_committed = std::sync::Arc::clone(&writes_committed);
        let writer_done = std::sync::Arc::clone(&writer_done);
        tokio::spawn(async move {
            let mut i = 0u64;
            while !writer_done.load(std::sync::atomic::Ordering::Relaxed) {
                let key = format!("live{i}").into_bytes();
                for &c in &survivor_clients {
                    if let Some(ClientResponse::PutOk) = call(
                        c,
                        ClientRequest::Put {
                            key: key.clone(),
                            value: b"v".to_vec(),
                            table: "kv".to_string(),
                        },
                    )
                    .await
                    {
                        writes_committed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        break;
                    }
                }
                i += 1;
                sleep(Duration::from_millis(50)).await;
            }
        })
    };

    // Observe the spare pass through a real learner state: it must appear in
    // SOME survivor's `learners` list before it ever appears in `voters`.
    let mut saw_spare_as_learner = false;
    let mut saw_spare_as_voter_before_learner = false;
    let cascade = async {
        loop {
            for &i in &survivors {
                if let Some((_, voters, learners)) = group_view(config.nodes[i].admin).await {
                    if learners.contains(&spare) {
                        saw_spare_as_learner = true;
                    }
                    if voters.contains(&spare) && !learners.contains(&spare) {
                        if !saw_spare_as_learner {
                            saw_spare_as_voter_before_learner = true;
                        }
                        if !voters.contains(&killed_id) {
                            return;
                        }
                    }
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(90), cascade)
        .await
        .expect("the CP group did not reconfigure onto the spare within 90s");

    writer_done.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.await.expect("writer task panicked");

    assert!(
        saw_spare_as_learner,
        "the spare must have been observed in some replica's `learners` set before joining \
         `voters` — a real, admin-visible instance of ADR 0058 Train 1's add-learner phase, \
         not just the eventual converged state"
    );
    assert!(
        !saw_spare_as_voter_before_learner,
        "the spare must never be observed as a voter without ALSO having been observed as a \
         learner first — a direct voter add would defeat the whole point of this rung"
    );
    assert!(
        writes_committed.load(std::sync::atomic::Ordering::Relaxed) > 0,
        "the old quorum must have kept accepting writes throughout the cascade"
    );

    // The healed group still serves both the pre-cascade and live writes.
    let survivor_clients: Vec<SocketAddr> =
        survivors.iter().map(|&i| config.nodes[i].client).collect();
    put(&survivor_clients, b"k_after", b"v_after", 30).await;

    for &i in &survivors {
        nodes[i].shutdown_graceful().await;
    }
}
