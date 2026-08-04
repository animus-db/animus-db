//! **Tablet-map-driven CP hosting — re-host split tablets on restart** (#2, ADR
//! 0017). A running CP tablet that splits stands up a co-resident group for the new
//! tablet (Phase 2.2). Before this increment that group lived only in process
//! memory, so a restart silently lost the post-split tablet: its `db-t{id}-` engine
//! sat on disk with no group serving it. Now each node durably records the split
//! tablets it hosts (the `cp-hosted` marker) and, on start, re-hosts each one by
//! recovering its engine + Raft WAL — so a key on the upper (split-off) range
//! survives a full cluster restart.
//!
//! In-process (`start_cluster`, the `--cluster N` shape) so the split trigger
//! reaches both the control leader and the CP leader through the shared edge state
//! (cross-process split-trigger routing is a separate follow-on); the **restart**
//! re-binds the same data dir, the part under test. Each incarnation gets fresh
//! ephemeral ports — re-hosting re-publishes the new sibling addresses, so the
//! re-formed groups find each other.
//!
//! Real TCP/time — polls with generous timeouts, not deterministic assertions (the
//! determinism guarantee is `SimEnv`-only; the mechanism is sim-proven in
//! `animus-cp-data`).

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, Node, bind_cluster, read_frame, start_cluster};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect to node");
    animusd::write_frame(&mut stream, &req).await.expect("send");
    read_frame(&mut stream)
        .await
        .expect("read")
        .expect("a reply")
}

async fn await_bootstrap(nodes: &[Node]) {
    let ready = async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
                && nodes.iter().all(|node| !node.metadata().tablets.is_empty())
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

async fn put(clients: &[SocketAddr], key: &[u8], value: &[u8]) {
    let write = async {
        loop {
            for &c in clients {
                if let ClientResponse::PutOk = call(
                    c,
                    ClientRequest::Put {
                        key: key.to_vec(),
                        value: value.to_vec(),
                        table: None,
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
    timeout(Duration::from_secs(20), write)
        .await
        .unwrap_or_else(|_| panic!("write of {key:?} never committed"));
}

async fn get(client: SocketAddr, key: &[u8]) -> Option<Vec<u8>> {
    match call(
        client,
        ClientRequest::Get {
            key: key.to_vec(),
            table: None,
        },
    )
    .await
    {
        ClientResponse::Value(v) => v,
        other => panic!("unexpected get reply: {other:?}"),
    }
}

/// Poll every node's client API for `key == want`, succeeding as soon as any node
/// serves it. Tolerates leader churn / group re-formation after a restart.
async fn await_value(clients: &[SocketAddr], key: &[u8], want: &[u8], secs: u64) {
    let poll = async {
        loop {
            for &c in clients {
                if get(c, key).await.as_deref() == Some(want) {
                    return;
                }
            }
            sleep(Duration::from_millis(150)).await;
        }
    };
    timeout(Duration::from_secs(secs), poll)
        .await
        .unwrap_or_else(|_| panic!("key {key:?} never read back as {want:?}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn split_tablet_survives_cluster_restart() {
    let dir = tempfile::tempdir().unwrap();
    let ip = "127.0.0.1".parse().unwrap();

    let nodes = start_cluster(bind_cluster(3, ip, dir.path()).await.unwrap())
        .await
        .unwrap();
    await_bootstrap(&nodes).await;
    let clients: Vec<SocketAddr> = nodes.iter().map(Node::client_addr).collect();

    // A lower key (stays on the original tablet) and an upper key (handed off to the
    // split-created tablet at split key "k5").
    put(&clients, b"k1", b"lower").await;
    put(&clients, b"k9", b"upper").await;

    // Split the bootstrap tablet (id 1) at "k5". In-process the shared edge reaches
    // both the control + CP leaders, so any node's client API can drive it; retry
    // until accepted (the CP group may still be electing).
    let split = async {
        loop {
            if let ClientResponse::PutOk = call(
                clients[0],
                ClientRequest::SplitTablet {
                    tablet: 1,
                    split_key: b"k5".to_vec(),
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
        .expect("split was not accepted within 20s");

    // The split created a second tablet, and the upper key is served from its new
    // co-resident group (seeded from the handoff).
    let two_tablets = async {
        loop {
            if nodes.iter().any(|node| node.metadata().tablets.len() >= 2) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(20), two_tablets)
        .await
        .expect("split tablet did not appear in the map within 20s");
    await_value(&clients, b"k9", b"upper", 30).await;

    // Restart the whole cluster on the SAME data dir (clean shutdown frees the
    // ports; on-disk state — the bootstrap engine and each split tablet's
    // `db-t{id}-` engine — is intact). Fresh ephemeral ports each incarnation.
    for node in nodes {
        node.shutdown();
    }
    sleep(Duration::from_millis(200)).await;

    let nodes = start_cluster(bind_cluster(3, ip, dir.path()).await.unwrap())
        .await
        .unwrap();
    await_bootstrap(&nodes).await;
    let clients: Vec<SocketAddr> = nodes.iter().map(Node::client_addr).collect();

    // The map still has both tablets (recovered from the control-plane WAL).
    let two_tablets_again = async {
        loop {
            if nodes.iter().all(|node| node.metadata().tablets.len() >= 2) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(20), two_tablets_again)
        .await
        .expect("both tablets did not recover into the map within 20s");

    // The split-off (upper) key survives the restart: its tablet's group was
    // re-hosted from disk, not lost. The lower key survives too (the bootstrap
    // tablet recovers as before).
    await_value(&clients, b"k9", b"upper", 40).await;
    await_value(&clients, b"k1", b"lower", 40).await;

    for node in nodes {
        node.shutdown();
    }
}
