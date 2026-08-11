//! **End-to-end automatic tablet-replica rebalancing** (ADR 0029) over
//! `ProdEnv`: a cluster that is *natively* imbalanced at bring-up (every
//! table's tablet lands on the first `min(N, 3)` `Active` raftkv members,
//! ADR 0023 provisioning) spreads its existing tablets across every node with
//! no operator action, keeps serving throughout, and settles (no further
//! churn once balanced).
//!
//! The planner/trigger mechanism is proven deterministically under `SimEnv` in
//! `animus-control/tests/placement_rebalance.rs`; this is the production-wiring
//! counterpart real TCP/time/disk can catch that the deterministic suite
//! cannot (per this crate's own `ProdEnv`-integration-test doctrine).
//!
//! Real TCP/time — polls with generous timeouts, not deterministic assertions.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, ClusterConfig, Node, RoleAddrs, read_frame};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

const TABLES: [&str; 6] = ["kv0", "kv1", "kv2", "kv3", "kv4", "kv5"];

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

/// The full replicated tablet map, `TabletId -> (replicas, epoch)`, from a
/// node's `/admin/status` (`Metadata`, identical on every node).
async fn tablet_map(admin_addr: SocketAddr) -> BTreeMap<u64, (Vec<u64>, u64)> {
    let (_s, v) = admin_get(admin_addr, "/admin/status").await;
    v["tablets"]
        .as_object()
        .expect("tablets is an object")
        .iter()
        .map(|(id, t)| {
            let replicas = t["replicas"]
                .as_array()
                .expect("replicas is an array")
                .iter()
                .filter_map(Value::as_u64)
                .collect();
            let epoch = t["epoch"].as_u64().expect("epoch is a number");
            (
                id.parse().expect("tablet id key is numeric"),
                (replicas, epoch),
            )
        })
        .collect()
}

/// Per-node replica counts across every tablet, seeded 0 for every id in
/// `raftkv_ids` so an as-yet-untouched node shows up as a genuine minimum.
fn replica_counts(
    map: &BTreeMap<u64, (Vec<u64>, u64)>,
    raftkv_ids: &[u64],
) -> BTreeMap<u64, usize> {
    let mut counts: BTreeMap<u64, usize> = raftkv_ids.iter().map(|&id| (id, 0)).collect();
    for (replicas, _) in map.values() {
        for &r in replicas {
            *counts.entry(r).or_insert(0) += 1;
        }
    }
    counts
}

fn imbalance(counts: &BTreeMap<u64, usize>) -> usize {
    let max = counts.values().copied().max().unwrap_or(0);
    let min = counts.values().copied().min().unwrap_or(0);
    max - min
}

/// This node's own hosted-groups view (`/admin/raftkv`): `tablet -> voters`,
/// only for tablets this node currently hosts (per-process mode, so this is
/// genuinely node-local, not the shared `--cluster N` aggregate).
async fn hosted_voters(admin_addr: SocketAddr) -> BTreeMap<u64, Vec<u64>> {
    let (_s, v) = admin_get(admin_addr, "/admin/raftkv").await;
    v["groups"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|g| {
            let tablet = g["tablet"].as_u64()?;
            let voters: Vec<u64> = g["voters"]
                .as_array()?
                .iter()
                .filter_map(Value::as_u64)
                .collect();
            Some((tablet, voters))
        })
        .collect()
}

async fn bring_up(n: usize, dir: &std::path::Path) -> (Vec<Node>, ClusterConfig) {
    for attempt in 0..16 {
        let a: Vec<SocketAddr> = {
            let ls: Vec<std::net::TcpListener> = (0..n * 5)
                .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
                .collect();
            ls.iter().map(|l| l.local_addr().unwrap()).collect()
        };
        let cfg = ClusterConfig {
            nodes: (0..n)
                .map(|i| RoleAddrs {
                    role: animusd::config::NodeRole::Both,
                    internal: a[5 * i],
                    client: a[5 * i + 1],
                    dynamo: a[5 * i + 2],
                    cql: a[5 * i + 3],
                    admin: a[5 * i + 4],
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

async fn call(addr: SocketAddr, req: ClientRequest) -> Option<ClientResponse> {
    let mut stream = TcpStream::connect(addr).await.ok()?;
    animusd::write_frame(&mut stream, &req).await.ok()?;
    read_frame(&mut stream).await.ok()?
}

async fn put(clients: &[SocketAddr], table: &str, key: &[u8], value: &[u8], secs: u64) {
    let w = async {
        loop {
            for &c in clients {
                if let Some(ClientResponse::PutOk) = call(
                    c,
                    ClientRequest::Put {
                        key: key.to_vec(),
                        value: value.to_vec(),
                        table: table.to_string(),
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
        .unwrap_or_else(|_| panic!("write of {table}/{key:?} never committed"));
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

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn cluster_grown_to_five_nodes_rebalances_existing_tablets() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(5, dir.path()).await;
    await_bootstrap(&nodes).await;
    let raftkv_ids = config.data_ids(); // [0, 1, 2, 3, 4]
    let admin_addrs: Vec<SocketAddr> = config.nodes.iter().map(|a| a.admin).collect();
    let clients: Vec<SocketAddr> = config.nodes.iter().map(|a| a.client).collect();

    // Provision several tables. ADR 0023: each table's tablet forms on first
    // write, on the first min(N, 3) *Active* raftkv members — with all 5 nodes
    // already Active at bootstrap, every table lands on {0,1,2}, leaving
    // 3/4 idle (ADR 0040 PR1: one id per node). This is the natural "grew the cluster before creating this
    // data" imbalance ADR 0029 exists to fix, with no membership-growing
    // machinery needed to set it up.
    for table in TABLES {
        put(&clients, table, b"k0", b"v0", 30).await;
    }

    // Sanity: confirm the natural imbalance actually exists before asserting
    // convergence away from it (else a no-op planner would pass vacuously).
    let initial = tablet_map(admin_addrs[0]).await;
    assert_eq!(initial.len(), TABLES.len(), "expected one tablet per table");
    let initial_counts = replica_counts(&initial, &raftkv_ids);
    assert!(
        imbalance(&initial_counts) >= 2,
        "expected a real starting imbalance, got {initial_counts:?}"
    );
    assert_eq!(
        initial_counts[&raftkv_ids[3]], 0,
        "node 3 should start with no replicas: {initial_counts:?}"
    );
    assert_eq!(
        initial_counts[&raftkv_ids[4]], 0,
        "node 4 should start with no replicas: {initial_counts:?}"
    );

    // Converge: poll the replicated tablet map until every node's replica
    // count is within 1 of every other's. Generous timeout — six tablets each
    // need at least one single-server reconfigure + InstallSnapshot catch-up,
    // paced by `REBALANCE_EVERY_N_TICKS` (ADR 0029) behind the control
    // plane's own reconcile cadence.
    let converged = async {
        loop {
            let map = tablet_map(admin_addrs[0]).await;
            let counts = replica_counts(&map, &raftkv_ids);
            if imbalance(&counts) <= 1 {
                return map;
            }
            sleep(Duration::from_millis(300)).await;
        }
    };
    let converged_map = timeout(Duration::from_secs(120), converged)
        .await
        .unwrap_or_else(|_| panic!("tablet replicas never spread across all 5 nodes within 120s"));
    let converged_counts = replica_counts(&converged_map, &raftkv_ids);
    assert!(
        converged_counts[&raftkv_ids[3]] > 0,
        "node 3 never gained a replica: {converged_counts:?}"
    );
    assert!(
        converged_counts[&raftkv_ids[4]] > 0,
        "node 4 never gained a replica: {converged_counts:?}"
    );

    // The data plane, not just metadata, must reflect the new placement: for
    // every tablet, some node's own hosted-group view reports voters matching
    // the tablet's replicated replica set exactly.
    for (&tablet, (replicas, _epoch)) in &converged_map {
        let mut expected = replicas.clone();
        expected.sort_unstable();
        let matched = async {
            loop {
                for &addr in &admin_addrs {
                    if let Some(mut voters) = hosted_voters(addr).await.get(&tablet).cloned() {
                        voters.sort_unstable();
                        if voters == expected {
                            return;
                        }
                    }
                }
                sleep(Duration::from_millis(200)).await;
            }
        };
        timeout(Duration::from_secs(30), matched)
            .await
            .unwrap_or_else(|_| {
                panic!("tablet {tablet:?}'s CP group never converged to voters {expected:?}")
            });
    }

    // Still serving, linearizably, throughout and after. Generous per-table
    // timeout: a table's group may still be mid-reconfigure (a healthy move's
    // add-then-catch-up-then-remove sequence, or even a leadership transfer)
    // right as this loop reaches it.
    for table in TABLES {
        put(&clients, table, b"k1", b"v1", 30).await;
        await_value(&clients, table, b"k1", b"v1", 30).await;
    }

    // Settled: once balanced, no further churn. Record every tablet's epoch,
    // wait several rebalance-evaluation cadences, and confirm nothing moved.
    let epochs_before: BTreeMap<u64, u64> = converged_map
        .iter()
        .map(|(&t, &(_, epoch))| (t, epoch))
        .collect();
    sleep(Duration::from_secs(10)).await;
    let epochs_after: BTreeMap<u64, u64> = tablet_map(admin_addrs[0])
        .await
        .into_iter()
        .map(|(t, (_, epoch))| (t, epoch))
        .collect();
    assert_eq!(
        epochs_before, epochs_after,
        "tablet epochs kept moving after the cluster reached a balanced state"
    );

    for node in nodes {
        node.shutdown();
    }
}
