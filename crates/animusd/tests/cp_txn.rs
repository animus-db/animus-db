//! **Multi-participant transactions over `ProdEnv`** (ADR 0018 §2/PR4):
//! `ClientCtx::cp_txn` end to end, through the real client TCP protocol, a
//! real 3-process cluster (each node its own `ClusterEdgeState`, exactly
//! like `cp_cross_process.rs`), and a real pre-split table (forcing a
//! transaction to span two independent tablet Raft groups, each possibly
//! led by a different node).
//!
//! The multi-tablet 2PC mechanics themselves (prepare/commit/resolve,
//! foreign-intent resolution, abort cleanup, participant leader-kill,
//! fence/seal interplay, seed reproducibility) are proven at the primitive
//! level, deterministically, in `animus-cp-data/tests/txn_multi.rs`. This
//! suite proves the **wire-level coordinator** composes those primitives
//! correctly over real forwarding — in particular the mandated regression
//! for a newly-forwarded command enum (`docs/engineering-lessons.md`): a
//! client connected to **every** node, including ones that don't host
//! either participant's leader, must still see the transaction commit
//! atomically (the `TxnPrepare`/`TxnDecide`/`TxnResolve`/`TxnStatus`
//! forwarding arms this PR adds to `cp_serve_forwarded`).
//!
//! Real TCP/time → polls with generous timeouts, never a fixed sleep.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, Node, read_frame};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// A key at least [`TOKEN_BYTES`]-long (ADR 0022's floor `cp_txn` enforces
/// for every transaction write, since `RaftKvNode::txn_stage`'s anchor-key
/// assert is now wire-reachable — see `cp_txn`'s doc), embedding `suffix`
/// for uniqueness and padded with `_` to reach the minimum.
const TOKEN_BYTES: usize = 8;
fn txn_key(prefix: &str, suffix: &str) -> Vec<u8> {
    let mut s = format!("{prefix}{suffix}");
    while s.len() < TOKEN_BYTES {
        s.push('_');
    }
    s.into_bytes()
}

async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    animusd::write_frame(&mut stream, &req).await.expect("send");
    read_frame(&mut stream)
        .await
        .expect("read")
        .expect("a reply")
}

/// Mirrors `cp_cross_process.rs`'s identical helper: `n` per-process nodes,
/// each with its own edge state (a real multi-process deployment), wrapped
/// in the documented port-TOCTOU retry.
async fn bring_up(n: usize, dir: &std::path::Path) -> (Vec<Node>, animusd::ClusterConfig) {
    for attempt in 0..16 {
        let addrs = support::free_addrs(n * 5);
        let nodes_cfg: Vec<animusd::RoleAddrs> = (0..n)
            .map(|i| animusd::RoleAddrs {
                id: animusd::config::node_id(i),
                role: animusd::config::NodeRole::Both,
                internal: addrs[5 * i],
                client: addrs[5 * i + 1],
                dynamo: addrs[5 * i + 2],
                cql: addrs[5 * i + 3],
                admin: addrs[5 * i + 4],
            })
            .collect();
        let config = animusd::ClusterConfig { nodes: nodes_cfg };
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

/// Split the bootstrap tablet (id 1) of `table` at `split_key`, waiting
/// until the control plane's tablet map records two tablets and the new
/// upper half is genuinely servable — mirrors `cp_plane.rs`'s
/// `cp_tablet_splits_and_both_halves_serve`.
async fn split_and_settle(nodes: &[Node], addr: SocketAddr, table: &str, split_key: &[u8]) {
    let resp = call(
        addr,
        ClientRequest::SplitTablet {
            tablet: 1,
            split_key: split_key.to_vec(),
        },
    )
    .await;
    assert!(
        matches!(resp, ClientResponse::PutOk),
        "split trigger rejected: {resp:?}"
    );
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes.iter().all(|n| n.metadata().tablets.len() == 2) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("split was not recorded in the tablet map within 20s");

    // Confirm the new (upper) group actually serves before relying on it —
    // read a key we know landed there.
    let probe_key = [split_key, b"zzz-probe"].concat();
    put_until_ok(addr, table, &probe_key, b"probe").await;
}

/// **Atomicity across a genuinely split table**: a transaction writing one
/// key below the split point and one above commits, and both become
/// visible together via a node that may not host either participant's
/// leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn multi_tablet_txn_commits_atomically_across_a_split_table() {
    let n = 3;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].client;

    // Seed a lower and an upper key so the split has real data on both
    // sides, then split at "k5".
    put_until_ok(addr0, "txn_t", b"k1", b"seed-lower").await;
    put_until_ok(addr0, "txn_t", b"k9", b"seed-upper").await;
    split_and_settle(&nodes, addr0, "txn_t", b"k5").await;

    // The transaction: one key in each half. Every txn write key must be at
    // least 8 bytes (ADR 0022 — `RaftKvNode::txn_stage`'s anchor-token
    // assert; `cp_txn` itself validates this and returns a clean error for
    // a short key rather than ever reaching that assert, but every key here
    // is intentionally already valid).
    let resp = timeout(
        Duration::from_secs(25),
        call(
            addr0,
            ClientRequest::Txn {
                writes: vec![
                    (
                        "txn_t".to_string(),
                        b"k2000000".to_vec(),
                        Some(b"lower-txn".to_vec()),
                    ),
                    (
                        "txn_t".to_string(),
                        b"k8000000".to_vec(),
                        Some(b"upper-txn".to_vec()),
                    ),
                ],
                preconditions: vec![],
            },
        ),
    )
    .await
    .expect("txn call did not return in 25s");
    assert!(
        matches!(resp, ClientResponse::TxnCommitted { .. }),
        "multi-tablet txn should commit: {resp:?}"
    );

    // Read both keys back via a DIFFERENT node than the one the txn was
    // issued through — both must be visible together.
    let reader = config.nodes[1 % n].client;
    for (key, expected) in [
        (b"k2000000".to_vec(), b"lower-txn".to_vec()),
        (b"k8000000".to_vec(), b"upper-txn".to_vec()),
    ] {
        let got = timeout(
            Duration::from_secs(10),
            call(
                reader,
                ClientRequest::Get {
                    key: key.clone(),
                    table: "txn_t".to_string(),
                },
            ),
        )
        .await
        .expect("read did not return in 10s");
        assert_eq!(
            got,
            ClientResponse::Value(Some(expected)),
            "key {key:?} missing after a committed cross-tablet transaction"
        );
    }

    for node in &nodes {
        node.shutdown();
    }
}

/// **Follower-connected regression**: issue the identical multi-tablet
/// transaction from **every** node in turn. With one leader per tablet
/// among three nodes, at least one of these calls originates on a node
/// that hosts neither participant's leader — proving the
/// `TxnPrepare`/`TxnDecide`/`TxnResolve` forwarding arms this PR adds to
/// `cp_serve_forwarded` are wired correctly (a missing arm here is exactly
/// the "bimodal per-process flake" the house lesson on forwarded command
/// enums warns about).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn txn_through_every_node_including_followers_succeeds() {
    let n = 3;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].client;

    put_until_ok(addr0, "txn_t2", b"k1", b"seed-lower").await;
    put_until_ok(addr0, "txn_t2", b"k9", b"seed-upper").await;
    split_and_settle(&nodes, addr0, "txn_t2", b"k5").await;

    for i in 0..n {
        let addr = config.nodes[i].client;
        let lower_key = txn_key("k2", &format!("-{i}"));
        let upper_key = txn_key("k8", &format!("-{i}"));
        let resp = timeout(
            Duration::from_secs(25),
            call(
                addr,
                ClientRequest::Txn {
                    writes: vec![
                        (
                            "txn_t2".to_string(),
                            lower_key.clone(),
                            Some(b"lo".to_vec()),
                        ),
                        (
                            "txn_t2".to_string(),
                            upper_key.clone(),
                            Some(b"hi".to_vec()),
                        ),
                    ],
                    preconditions: vec![],
                },
            ),
        )
        .await
        .unwrap_or_else(|_| panic!("txn via node {i} did not return in 25s"));
        assert!(
            matches!(resp, ClientResponse::TxnCommitted { .. }),
            "txn issued via node {i} should commit: {resp:?}"
        );

        for (key, expected) in [(lower_key, b"lo".to_vec()), (upper_key, b"hi".to_vec())] {
            let got = call(
                addr0,
                ClientRequest::Get {
                    key: key.clone(),
                    table: "txn_t2".to_string(),
                },
            )
            .await;
            assert_eq!(
                got,
                ClientResponse::Value(Some(expected)),
                "key {key:?} from the txn issued via node {i} must be visible"
            );
        }
    }

    for node in &nodes {
        node.shutdown();
    }
}

/// **Concurrent transactions are individually atomic**: several `cp_txn`
/// calls run concurrently, each its own independent two-tablet pair; every
/// one's own pair must be atomically visible (both keys, never one without
/// the other), regardless of how their prepare/commit rounds interleave in
/// real wall-clock time.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn concurrent_transactions_are_individually_atomic() {
    let n = 3;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].client;

    put_until_ok(addr0, "txn_t3", b"k1", b"seed-lower").await;
    put_until_ok(addr0, "txn_t3", b"k9", b"seed-upper").await;
    split_and_settle(&nodes, addr0, "txn_t3", b"k5").await;

    let handles: Vec<_> = (0..8)
        .map(|i| {
            let addr = config.nodes[i % n].client;
            tokio::spawn(async move {
                let lower_key = txn_key("k2", &format!("-c{i}"));
                let upper_key = txn_key("k8", &format!("-c{i}"));
                let resp = timeout(
                    Duration::from_secs(25),
                    call(
                        addr,
                        ClientRequest::Txn {
                            writes: vec![
                                (
                                    "txn_t3".to_string(),
                                    lower_key.clone(),
                                    Some(format!("lo{i}").into_bytes()),
                                ),
                                (
                                    "txn_t3".to_string(),
                                    upper_key.clone(),
                                    Some(format!("hi{i}").into_bytes()),
                                ),
                            ],
                            preconditions: vec![],
                        },
                    ),
                )
                .await
                .unwrap_or_else(|_| panic!("concurrent txn {i} did not return in 25s"));
                assert!(
                    matches!(resp, ClientResponse::TxnCommitted { .. }),
                    "concurrent txn {i} should commit: {resp:?}"
                );
                (i, lower_key, upper_key)
            })
        })
        .collect();

    for handle in handles {
        let (i, lower_key, upper_key) = handle.await.expect("txn task panicked");
        let lower = call(
            addr0,
            ClientRequest::Get {
                key: lower_key,
                table: "txn_t3".to_string(),
            },
        )
        .await;
        let upper = call(
            addr0,
            ClientRequest::Get {
                key: upper_key,
                table: "txn_t3".to_string(),
            },
        )
        .await;
        assert_eq!(
            lower,
            ClientResponse::Value(Some(format!("lo{i}").into_bytes())),
            "concurrent txn {i}'s lower key must be visible"
        );
        assert_eq!(
            upper,
            ClientResponse::Value(Some(format!("hi{i}").into_bytes())),
            "concurrent txn {i}'s upper key must be visible"
        );
    }

    for node in &nodes {
        node.shutdown();
    }
}

/// **Condition-read precondition**: a transaction whose precondition is
/// already false (a stale expected value) aborts with a retryable conflict
/// error, and — critically — commits **nothing**, including the other
/// participant's keys, proving the whole-transaction abort is atomic too.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn violated_precondition_aborts_the_whole_transaction() {
    let n = 3;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].client;

    let lower_key = txn_key("k2", "");
    let upper_key = txn_key("k8", "");

    put_until_ok(addr0, "txn_t4", b"k1", b"seed-lower").await;
    put_until_ok(addr0, "txn_t4", b"k9", b"seed-upper").await;
    split_and_settle(&nodes, addr0, "txn_t4", b"k5").await;
    // The guarded key's real current value.
    put_until_ok(addr0, "txn_t4", &lower_key, b"real-value").await;

    let resp = timeout(
        Duration::from_secs(25),
        call(
            addr0,
            ClientRequest::Txn {
                writes: vec![
                    (
                        "txn_t4".to_string(),
                        lower_key.clone(),
                        Some(b"should-not-land".to_vec()),
                    ),
                    (
                        "txn_t4".to_string(),
                        upper_key.clone(),
                        Some(b"should-not-land-either".to_vec()),
                    ),
                ],
                preconditions: vec![(
                    "txn_t4".to_string(),
                    lower_key.clone(),
                    Some(b"wrong-expected-value".to_vec()),
                )],
            },
        ),
    )
    .await
    .expect("txn call did not return in 25s");
    assert!(
        matches!(resp, ClientResponse::Error(_)),
        "a violated precondition must abort the transaction: {resp:?}"
    );

    // Neither write landed — the guarded key keeps its real value, and the
    // other participant's key was never written at all.
    let k2 = call(
        addr0,
        ClientRequest::Get {
            key: lower_key,
            table: "txn_t4".to_string(),
        },
    )
    .await;
    assert_eq!(k2, ClientResponse::Value(Some(b"real-value".to_vec())));
    let k8 = call(
        addr0,
        ClientRequest::Get {
            key: upper_key,
            table: "txn_t4".to_string(),
        },
    )
    .await;
    assert_eq!(
        k8,
        ClientResponse::Value(None),
        "the other participant's key must never have been committed"
    );

    for node in &nodes {
        node.shutdown();
    }
}
