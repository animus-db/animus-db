//! `animusd --cluster-control N --cluster-data M` — a whole split deployment
//! in **one process** (the in-process, single-command counterpart of
//! `animusd control` + `animusd data` real-process split, ADR 0035).
//!
//! One tight test, over real TCP/time (a bounded, converged-or-timeout poll,
//! never a fixed sleep used as an assertion): a genuine split cluster
//! assembled via [`animusd::start_split_cluster_with`] (the entry point the
//! CLI's `--cluster-control`/`--cluster-data` flags call) — control leader
//! elects, data nodes self-register and promote `Active`, a table CRUD
//! round-trip through a **data** node's client address works, and
//! `/admin/config`'s `role` differs across a control vs. data node.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, Node, StorageBackend, read_frame};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

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

/// Try every client address in `clients` (round-robin) until one accepts the
/// write — the documented "a control-only node's forward path needs
/// round-robin, not a single fixed node" lesson (root `CLAUDE.md`).
async fn put(clients: &[SocketAddr], table: &str, key: &[u8], value: &[u8]) {
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
                if matches!(resp, ClientResponse::PutOk) {
                    return;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(20), w)
        .await
        .unwrap_or_else(|_| panic!("write of {table}/{key:?} never committed"));
}

async fn await_value(clients: &[SocketAddr], table: &str, key: &[u8], want: &[u8]) {
    let p = async {
        loop {
            for &c in clients {
                if let ClientResponse::Value(Some(v)) = call(
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
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(20), p)
        .await
        .unwrap_or_else(|_| panic!("key {table}/{key:?} never read back as {want:?}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_process_split_cluster_serves_writes_and_reports_roles() {
    let dir = tempfile::tempdir().unwrap();
    const CONTROL_N: usize = 3;
    const DATA_N: usize = 2;

    let nodes: Vec<Node> = animusd::start_split_cluster_with(
        CONTROL_N,
        DATA_N,
        dir.path(),
        "127.0.0.1".parse().unwrap(),
        StorageBackend::Memory,
        None,
        None,
    )
    .await
    .expect("split cluster starts");
    assert_eq!(nodes.len(), CONTROL_N + DATA_N);

    let control_nodes = &nodes[..CONTROL_N];
    let data_nodes = &nodes[CONTROL_N..];

    // Control leader elects.
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

    // Every data node's raftkv id self-registers and gets promoted `Active`
    // by the unmodified ADR 0012 heartbeat/detector chain (no test-side
    // force — mirrors `support::await_data_nodes_active`).
    let data_raftkv_ids: Vec<animus_env::NodeId> = (0..DATA_N)
        .map(|i| animusd::config::raftkv_id(CONTROL_N + i))
        .collect();
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

    // Table CRUD round-trip through a DATA node's client address —
    // round-robin across every client address (control nodes included: a
    // Put against a control-only node's client port must forward correctly
    // too, the same shape `tests/data_only.rs` exercises).
    let all_clients: Vec<SocketAddr> = nodes.iter().map(Node::client_addr).collect();
    let data_client = data_nodes[0].client_addr();

    put(&[data_client], "kv", b"hello", b"world").await;
    await_value(&all_clients, "kv", b"hello", b"world").await;

    // A write via one data node, read via the *other* data node.
    put(&[data_client], "kv", b"cross", b"replica").await;
    await_value(&[data_nodes[1].client_addr()], "kv", b"cross", b"replica").await;

    // A write issued while *preferring* a control node's client address —
    // round-robin across every address (not a single fixed control node),
    // per the documented "zero-replica blind-forward" lesson: a node with no
    // local replica forwards to *some* known replica of the tablet, not
    // necessarily its leader, so a single fixed no-replica node can retry the
    // same non-leader forever. Putting the control addresses first in the
    // list still exercises their forward path on every attempt that reaches
    // them; the data addresses are the fallback that keeps this assertion
    // sound rather than flaky.
    let control_first: Vec<SocketAddr> = control_nodes
        .iter()
        .chain(data_nodes.iter())
        .map(Node::client_addr)
        .collect();
    put(&control_first, "kv", b"via-control", b"ok").await;
    await_value(&all_clients, "kv", b"via-control", b"ok").await;

    // `/admin/config`'s `role` differs across a control vs. data node.
    let (status, control_cfg) = admin_get(control_nodes[0].admin_addr(), "/admin/config").await;
    assert_eq!(status, 200);
    assert_eq!(control_cfg["role"], "control");
    assert!(control_cfg["addrs"]["raftkv"].is_null());

    let (status, data_cfg) = admin_get(data_nodes[0].admin_addr(), "/admin/config").await;
    assert_eq!(status, 200);
    assert_eq!(data_cfg["role"], "data");
    assert!(data_cfg["addrs"]["control"].is_null());

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}
