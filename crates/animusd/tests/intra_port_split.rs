//! **The intra-cluster port split, end to end (ADR 0047).**
//!
//! Two things must both hold, over a real 3-process `ProdEnv` cluster:
//!
//! 1. A node's **client** port refuses a bare `ClientRequest::Forwarded{..}`
//!    and a representative bare internal-only variant (`KindWrite`) —
//!    regardless of whether that node happens to be a tablet's leader or
//!    not, since the client-port guard (`handle_request`'s one guard clause
//!    before its existing match, `surface_of`) is a pure listener+variant
//!    check, not a leadership check.
//! 2. The **same** node's **intra** port serves the identical `Forwarded`
//!    request end to end, one hop, returning the real committed value —
//!    proving the intra port is not just "not blocked" but genuinely wired
//!    to `cp_serve_forwarded`, the same one-hop path `ClientCtx::cp_forward`
//!    itself uses internally.
//!
//! This is the regression the plan for ADR 0047 calls for (§6): follows the
//! house lesson already on file (`docs/engineering-lessons.md`'s "a
//! forwarded-command test suite needs at least one non-leader-issued call")
//! by driving the wire directly rather than through `ClientCtx::cp_forward`.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, Node, read_frame};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    animusd::write_frame(&mut stream, &req).await.expect("send");
    read_frame(&mut stream)
        .await
        .expect("read")
        .expect("a reply")
}

async fn call_forwarded(addr: SocketAddr, request: ClientRequest) -> ClientResponse {
    call(
        addr,
        ClientRequest::Forwarded {
            request: Box::new(request),
            traceparent: None,
        },
    )
    .await
}

/// Mirrors `cp_txn.rs`'s identical helper: `n` per-process nodes, each with
/// its own edge state (a real multi-process deployment), wrapped in the
/// documented port-TOCTOU retry (`support::free_addrs`'s own doc).
async fn bring_up(n: usize, dir: &std::path::Path) -> (Vec<Node>, animusd::ClusterConfig) {
    for attempt in 0..16 {
        let addrs = support::free_addrs(n * 6);
        let nodes_cfg: Vec<animusd::RoleAddrs> = (0..n)
            .map(|i| animusd::RoleAddrs {
                id: animusd::config::node_id(i),
                role: animusd::config::NodeRole::Both,
                internal: addrs[6 * i],
                client: addrs[6 * i + 1],
                dynamo: addrs[6 * i + 2],
                admin: addrs[6 * i + 3],
                intra: addrs[6 * i + 4],
                console: addrs[6 * i + 5],
            })
            .collect();
        let config = animusd::ClusterConfig {
            nodes: nodes_cfg,
            dynamo_auth: None,
        };
        let mut nodes = Vec::new();
        let mut failed = false;
        for i in 0..n {
            match animusd::run_node(&config, i, dir.join(format!("node-{attempt}-{i}"))).await {
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
    panic!("could not bring up cluster after retries (ports kept getting stolen)");
}

async fn await_bootstrap(nodes: &[Node]) {
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
                && nodes.iter().all(|n| !n.metadata().members.is_empty())
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("cluster did not bootstrap in 20s");
}

async fn put_until_ok(addr: SocketAddr, table: &str, key: &[u8], value: &[u8]) {
    timeout(Duration::from_secs(25), async {
        loop {
            match call(
                addr,
                ClientRequest::Put {
                    key: key.to_vec(),
                    value: value.to_vec(),
                    table: table.to_string(),
                },
            )
            .await
            {
                ClientResponse::PutOk => return,
                ClientResponse::Error(_) => sleep(Duration::from_millis(150)).await,
                other => panic!("unexpected put response: {other:?}"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("put {table}/{key:?} did not succeed in 25s"));
}

/// Find the tablet's current leader by trying `Forwarded { Get }` against
/// every node's own **intra** port until one actually returns the
/// committed value — exactly what `ClientCtx::cp_forward` itself would do
/// (bounded, retried), just driven by hand so the test controls which
/// physical node it then re-uses for the client-port-refusal assertions.
async fn find_leader_index(
    intra_addrs: &[SocketAddr],
    table: &str,
    key: &[u8],
    expected_value: &[u8],
) -> usize {
    timeout(Duration::from_secs(20), async {
        loop {
            for (i, &addr) in intra_addrs.iter().enumerate() {
                if let ClientResponse::Value(Some(v)) = call_forwarded(
                    addr,
                    ClientRequest::Get {
                        key: key.to_vec(),
                        table: table.to_string(),
                        stale: false,
                    },
                )
                .await
                    && v == expected_value
                {
                    return i;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("no node's intra port ever served the leader's Get within 20s")
}

/// The full ADR 0047 regression: a node's client port refuses cluster-
/// internal traffic unconditionally; that same node's intra port serves it
/// end to end.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn client_port_refuses_intra_traffic_intra_port_serves_it() {
    let n = 3;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;

    let table = "intra_port_split_t1";
    let key = b"k1";
    let value = b"leader-served-value";
    let addr0 = config.nodes[0].client;
    put_until_ok(addr0, table, key, value).await;

    let intra_addrs: Vec<SocketAddr> = config.nodes.iter().map(|c| c.intra).collect();
    let leader_idx = find_leader_index(&intra_addrs, table, key, value).await;
    let leader_intra = config.nodes[leader_idx].intra;
    // A genuine follower of this tablet: any other node in the 3-node
    // cluster. The client-port guard fires purely on listener+surface, so
    // this isn't load-bearing for assertion 1 below — but picking a real
    // follower (rather than the leader itself) matches the scenario the ADR
    // describes and rules out any "only refused because it happens to be
    // the leader" doubt.
    let follower_idx = (leader_idx + 1) % n;
    let follower_client = config.nodes[follower_idx].client;

    // 1a. Bare `Forwarded{Get}` on a FOLLOWER's own CLIENT port is refused —
    // a pure listener+surface check, independent of leadership.
    let refusal = call_forwarded(
        follower_client,
        ClientRequest::Get {
            key: key.to_vec(),
            table: table.to_string(),
            stale: false,
        },
    )
    .await;
    match refusal {
        ClientResponse::Error(msg) => {
            assert!(
                msg.contains("cluster-internal request") && msg.contains("intra port"),
                "expected the client-port intra-surface guard's message, got: {msg}"
            );
            assert!(
                msg.contains("forwarded"),
                "expected `request_kind` to name the refused variant, got: {msg}"
            );
        }
        other => panic!("expected the client port to refuse a bare Forwarded, got {other:?}"),
    }

    // 1b. A representative bare internal-only variant, `KindWrite`, gets the
    // identical port-guard refusal on the same FOLLOWER's CLIENT port — the
    // guard clause fires before the existing bare-refusal match arms even
    // run, so this is refused for "wrong port," not (only) "wrong envelope."
    let kind_write_refusal = call(
        follower_client,
        ClientRequest::KindWrite {
            table: table.to_string(),
            writes: Vec::new(),
            change_log: Vec::new(),
        },
    )
    .await;
    match kind_write_refusal {
        ClientResponse::Error(msg) => {
            assert!(
                msg.contains("cluster-internal request") && msg.contains("intra port"),
                "expected the client-port intra-surface guard's message, got: {msg}"
            );
            assert!(
                msg.contains("kind_write"),
                "expected `request_kind` to name the refused variant, got: {msg}"
            );
        }
        other => panic!("expected the client port to refuse a bare KindWrite, got {other:?}"),
    }

    // 2. The tablet LEADER's own INTRA port serves the identical
    // `Forwarded{Get}` request end to end, one hop, returning the real
    // committed value — not merely "not port-refused," but genuinely wired
    // to `cp_serve_forwarded`. (`cp_serve_forwarded` never re-forwards — see
    // `crates/animusd/CLAUDE.md`'s "one-hop invariant" — so this is deliberately
    // dialed straight at the leader, exactly the target `ClientCtx::cp_forward`
    // itself would have chased the hint to.)
    let served = call_forwarded(
        leader_intra,
        ClientRequest::Get {
            key: key.to_vec(),
            table: table.to_string(),
            stale: false,
        },
    )
    .await;
    assert_eq!(
        served,
        ClientResponse::Value(Some(value.to_vec())),
        "expected the leader's own intra port to serve the forwarded Get end-to-end"
    );

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}
