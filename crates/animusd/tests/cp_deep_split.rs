//! **Deep splits** (D3, ADR 0017): a tablet created by a split can itself be split
//! again — required for auto-sharding to keep working as a shard grows. Before this,
//! split-created groups were started without a split hook, so a second split of a
//! half was a no-op; and the member-id derivation compounded with depth, diverging
//! from the reconfigure loop's flat `base + tablet*STRIDE`. Now every group (bootstrap,
//! split child, re-hosted, joined) carries a hook, and member ids derive flatly from
//! the base id at any depth.
//!
//! In-process (`start_cluster`) so the split trigger reaches both the control + CP
//! leaders via the shared edge.
//!
//! Real TCP/time — polls with generous timeouts.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, Node, bind_cluster, read_frame, start_cluster};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    animusd::write_frame(&mut stream, &req).await.expect("send");
    read_frame(&mut stream).await.expect("read").expect("reply")
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

async fn put(clients: &[SocketAddr], key: &[u8], value: &[u8]) {
    let w = async {
        loop {
            for &c in clients {
                if let ClientResponse::PutOk = call(
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
                if let ClientResponse::Value(Some(v)) = call(
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

/// Trigger a split of `tablet` at `split_key` (in-process: any node reaches both
/// leaders), retrying until accepted, then wait until the tablet count reaches
/// `want_tablets`.
async fn split_at(
    clients: &[SocketAddr],
    nodes: &[Node],
    tablet: u64,
    split_key: &[u8],
    want_tablets: usize,
) {
    let split = async {
        loop {
            if let ClientResponse::PutOk = call(
                clients[0],
                ClientRequest::SplitTablet {
                    tablet,
                    split_key: split_key.to_vec(),
                },
            )
            .await
            {
                return;
            }
            sleep(Duration::from_millis(150)).await;
        }
    };
    timeout(Duration::from_secs(20), split)
        .await
        .unwrap_or_else(|_| panic!("split of tablet {tablet} at {split_key:?} not accepted"));

    let grew = async {
        loop {
            if nodes
                .iter()
                .any(|n| n.metadata().tablets.len() >= want_tablets)
            {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(20), grew)
        .await
        .unwrap_or_else(|_| panic!("tablet count did not reach {want_tablets}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_split_tablet_can_be_split_again() {
    let dir = tempfile::tempdir().unwrap();
    let ip = "127.0.0.1".parse().unwrap();
    let nodes = start_cluster(bind_cluster(3, ip, dir.path()).await.unwrap())
        .await
        .unwrap();
    await_bootstrap(&nodes).await;
    let clients: Vec<SocketAddr> = nodes.iter().map(Node::client_addr).collect();

    // Three keys spanning the eventual three ranges: k1 < k5 <= k6 < k7 <= k9.
    put(&clients, b"k1", b"a").await;
    put(&clients, b"k6", b"b").await;
    put(&clients, b"k9", b"c").await;

    // First split: tablet 1 -> {1: [.., k5), 2: [k5, ..)}. k6 + k9 move to tablet 2.
    split_at(&clients, &nodes, 1, b"k5", 2).await;
    await_value(&clients, b"k9", b"c", 30).await;

    // Deep split: split the split-created tablet 2 at "k7" -> {2: [k5, k7), 3: [k7, ..)}.
    // This only works if tablet 2's group carries a split hook of its own (D3).
    split_at(&clients, &nodes, 2, b"k7", 3).await;

    // All three ranges still serve their keys, now across three tablets:
    //   k1 -> tablet 1, k6 -> tablet 2 (the middle range), k9 -> tablet 3.
    await_value(&clients, b"k9", b"c", 30).await;
    await_value(&clients, b"k6", b"b", 30).await;
    await_value(&clients, b"k1", b"a", 30).await;

    // New writes into each of the three ranges commit + read back.
    put(&clients, b"k0", b"d").await; // tablet 1
    put(&clients, b"k6b", b"e").await; // tablet 2
    put(&clients, b"k8", b"f").await; // tablet 3
    await_value(&clients, b"k0", b"d", 30).await;
    await_value(&clients, b"k6b", b"e", 30).await;
    await_value(&clients, b"k8", b"f", 30).await;

    for n in nodes {
        n.shutdown();
    }
}
