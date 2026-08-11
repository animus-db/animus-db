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

/// Try every client address in `clients` (round-robin, retrying until the
/// deadline) until one accepts the write. `clients` is usually a single fixed
/// address in this file — the hinted-retry forwarder (root `CLAUDE.md`'s
/// "zero-replica blind-forward" entry) now resolves a zero-replica node's
/// forward deterministically, so a round-robin across many addresses is no
/// longer needed just to dodge that hazard.
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
                    && v == want
                {
                    return;
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
        .map(|i| animusd::config::node_id(CONTROL_N + i))
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

    // Table CRUD round-trip through a DATA node's client address; `all_clients`
    // (every control and data node) is used below to confirm the write is
    // visible cluster-wide, and separately to exercise a control-only node's
    // forward path with a single fixed address (the same shape
    // `tests/data_only.rs` exercises).
    let all_clients: Vec<SocketAddr> = nodes.iter().map(Node::client_addr).collect();
    let data_client = data_nodes[0].client_addr();

    put(&[data_client], "kv", b"hello", b"world").await;
    await_value(&all_clients, "kv", b"hello", b"world").await;

    // A write via one data node, read via the *other* data node.
    put(&[data_client], "kv", b"cross", b"replica").await;
    await_value(&[data_nodes[1].client_addr()], "kv", b"cross", b"replica").await;

    // A write issued through a single **fixed** control-only node's client
    // address — that node hosts zero local CP replicas of anything, so this
    // exercises `resolve_cp_route`'s no-local-replica forward branch. This
    // used to need a round-robin across every address, per the documented
    // "zero-replica blind-forward" lesson (root `CLAUDE.md`): a node with no
    // local replica forwards to *some* known replica of the tablet, not
    // necessarily its leader, and a wrong guess used to error forever
    // (the receiver never re-forwards). The forwarder now retries a "not
    // the leader here" refusal at the refusing node's own embedded leader
    // hint, then at the tablet's other replicas, so a single fixed control
    // node resolves deterministically — this is the intended regression
    // proof that the hazard is closed.
    let fixed_control = control_nodes[0].client_addr();
    put(&[fixed_control], "kv", b"via-control", b"ok").await;
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

/// Focused regression for the hinted-retry forwarder (root `CLAUDE.md`'s
/// "zero-replica blind-forward" entry): a control-only node hosts zero local
/// CP replicas of anything, so every write/read through it must take
/// `resolve_cp_route`'s no-local-replica forward branch, guessing a first
/// target among the tablet's replicas. Pre-fix, a wrong guess (a non-leader
/// replica) errored forever — the receiver never re-forwards — so a
/// repeated write/read through **one fixed** control node's client address
/// failed on roughly half of runs (whichever replica happened to win the
/// tablet's Raft election). Post-fix, `ClientCtx::cp_forward` retries a "not
/// the leader here" refusal at the refusing node's own embedded leader hint
/// (or another known replica), so this must succeed deterministically for
/// every key, every run — no round-robin across addresses needed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixed_control_node_write_read_is_deterministic() {
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

    let control_nodes = &nodes[..CONTROL_N];

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

    let data_raftkv_ids: Vec<animus_env::NodeId> = (0..DATA_N)
        .map(|i| animusd::config::node_id(CONTROL_N + i))
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

    let fixed_control = control_nodes[0].client_addr();
    for i in 0..20u32 {
        let key = format!("fixed-key-{i}").into_bytes();
        let value = format!("fixed-val-{i}").into_bytes();
        put(&[fixed_control], "fixed_kv", &key, &value).await;
        await_value(&[fixed_control], "fixed_kv", &key, &value).await;
    }

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

/// A **single-shot** first write through a control-only node must succeed —
/// no client-side retry loop at all. The very first `Put` to a fresh table
/// provisions its tablet and races the group's formation/election window on
/// the data nodes; a zero-replica control node forwards into that window and
/// every replica refuses with `leader_hint=none` (no leader exists yet).
/// Pre-fix, `cp_forward` gave up the moment one pass over the replicas
/// exhausted, surfacing "not the leader here; leader_hint=none" to the
/// client (user-hit, live); it now waits out the election
/// (`FORWARD_ELECTION_BACKOFF` passes bounded by `CLIENT_TIMEOUT`), so the
/// one-attempt write must land deterministically.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_shot_first_write_through_control_node_succeeds() {
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

    let control_nodes = &nodes[..CONTROL_N];

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

    let data_raftkv_ids: Vec<animus_env::NodeId> = (0..DATA_N)
        .map(|i| animusd::config::node_id(CONTROL_N + i))
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

    // ONE attempt, fresh table, fixed control-only address. The 15s outer
    // bound only guards a hang; the server's own CLIENT_TIMEOUT (10s) is the
    // real budget the forwarder works within.
    let resp = timeout(
        Duration::from_secs(15),
        call(
            control_nodes[0].client_addr(),
            ClientRequest::Put {
                key: b"first-key".to_vec(),
                value: b"first-val".to_vec(),
                table: "single_shot_kv".to_string(),
            },
        ),
    )
    .await
    .expect("single-shot put hung");
    assert!(
        matches!(resp, ClientResponse::PutOk),
        "single-shot first write through a control node failed: {resp:?}"
    );
    await_value(
        &[control_nodes[0].client_addr()],
        "single_shot_kv",
        b"first-key",
        b"first-val",
    )
    .await;

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}
