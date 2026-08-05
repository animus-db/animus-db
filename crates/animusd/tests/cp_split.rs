//! **Cross-process CP split trigger** (D2, ADR 0017). A tablet split has two halves
//! that target different leaders: the `SplitTablet` metadata must reach the **control
//! leader**, and the data-plane `propose_split` must reach the tablet's **CP-group
//! leader** — and in a one-process-per-node deployment those two leaders can sit on
//! different nodes. This test drives the split from a node that is **neither** leader
//! and asserts it still completes: the metadata relays to the control leader, and the
//! data-plane half forwards a one-hop `CpSplit` to the CP leader's node.
//!
//! (The in-process split path is covered by `cp_rehost.rs`, where the shared edge
//! reaches both leaders from any node; this is the per-process counterpart.)
//!
//! Real TCP/time — polls with generous timeouts, not deterministic assertions.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, ClusterConfig, Node, RoleAddrs, read_frame};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

async fn call(addr: SocketAddr, req: ClientRequest) -> Option<ClientResponse> {
    let mut stream = TcpStream::connect(addr).await.ok()?;
    animusd::write_frame(&mut stream, &req).await.ok()?;
    read_frame(&mut stream).await.ok()?
}

async fn admin_get(addr: SocketAddr, path: &str) -> Value {
    let mut stream = TcpStream::connect(addr).await.expect("connect admin");
    let req = format!("GET {path} HTTP/1.0\r\nHost: a\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.expect("send");
    stream.flush().await.expect("flush");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read");
    let text = String::from_utf8(raw).expect("utf8");
    let (_h, body) = text.split_once("\r\n\r\n").expect("body");
    serde_json::from_str(body).expect("json")
}

/// Whether this node currently leads the bootstrap tablet's CP group.
async fn leads_cp(admin_addr: SocketAddr) -> bool {
    let v = admin_get(admin_addr, "/admin/raftkv").await;
    v["groups"]
        .as_array()
        .map(|gs| {
            gs.iter()
                .any(|g| g["tablet"] == 1 && g["is_leader"].as_bool().unwrap_or(false))
        })
        .unwrap_or(false)
}

async fn await_bootstrap(nodes: &[Node]) {
    let ready = async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
                && nodes.iter().all(|n| !n.metadata().members.is_empty())
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(30), ready)
        .await
        .expect("bootstrap within 30s");
}

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

async fn put(clients: &[SocketAddr], key: &[u8], value: &[u8]) {
    let w = async {
        loop {
            for &c in clients {
                if let Some(ClientResponse::PutOk) = call(
                    c,
                    ClientRequest::Put {
                        key: key.to_vec(),
                        value: value.to_vec(),
                        table: Some("kv".to_string()),
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
    timeout(Duration::from_secs(20), w)
        .await
        .unwrap_or_else(|_| panic!("write {key:?} never committed"));
}

async fn await_value(clients: &[SocketAddr], key: &[u8], want: &[u8], secs: u64) {
    let p = async {
        loop {
            for &c in clients {
                if let Some(ClientResponse::Value(Some(v))) = call(
                    c,
                    ClientRequest::Get {
                        key: key.to_vec(),
                        table: Some("kv".to_string()),
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
async fn split_triggered_from_a_non_leader_node() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(3, dir.path()).await;
    await_bootstrap(&nodes).await;
    let clients: Vec<SocketAddr> = config.nodes.iter().map(|a| a.client).collect();

    // ADR 0023: no bootstrap tablet — write first to provision the `kv` tablet (the
    // writes auto-provision + wait), then confirm the group elected a leader (the
    // split below needs one). A lower + an upper key span the split point.
    put(&clients, b"k1", b"lower").await;
    put(&clients, b"k9", b"upper").await;
    let cp_up = async {
        loop {
            for n in &nodes {
                if leads_cp(n.admin_addr()).await {
                    return;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(30), cp_up)
        .await
        .expect("CP group did not elect a leader within 30s");

    // Pick a node that is **neither** the control leader nor the CP-group leader, so
    // both halves of the split must cross process boundaries. With 3 nodes and ≤2
    // distinct leaders, such a node always exists.
    let driver = {
        let pick = async {
            loop {
                for (i, n) in nodes.iter().enumerate() {
                    if !n.is_control_leader() && !leads_cp(n.admin_addr()).await {
                        return i;
                    }
                }
                sleep(Duration::from_millis(100)).await;
            }
        };
        timeout(Duration::from_secs(20), pick)
            .await
            .expect("no non-leader node found within 20s")
    };

    // Drive the split from that node. It must succeed via cross-process routing
    // (metadata relayed to the control leader; data-plane `CpSplit` forwarded to the
    // CP leader). Retry: leaders may still be settling right after bring-up.
    let split = async {
        loop {
            if let Some(ClientResponse::PutOk) = call(
                config.nodes[driver].client,
                ClientRequest::SplitTablet {
                    tablet: 1,
                    split_key: b"k5".to_vec(),
                },
            )
            .await
            {
                return;
            }
            sleep(Duration::from_millis(200)).await;
        }
    };
    timeout(Duration::from_secs(30), split)
        .await
        .expect("split driven from a non-leader node was not accepted within 30s");

    // The split took effect: a second tablet exists and the upper key is served from
    // the new co-resident group (which formed cross-process — its members published
    // their addresses by relaying to the control leader).
    let two = async {
        loop {
            if nodes.iter().any(|n| n.metadata().tablets.len() >= 2) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(20), two)
        .await
        .expect("split tablet did not appear within 20s");
    await_value(&clients, b"k9", b"upper", 40).await;
    await_value(&clients, b"k1", b"lower", 40).await;

    for n in nodes {
        n.shutdown();
    }
}
