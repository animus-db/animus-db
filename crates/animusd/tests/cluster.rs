//! End-to-end test of a runnable cluster over real TCP (`ProdEnv`), exercised
//! through the client request API exactly as the `animus` CLI does.
//!
//! Unlike the simulation tests, this uses real time and sockets, so it polls
//! with generous timeouts rather than asserting deterministic timing.

use std::time::Duration;

use animus_env::nid;
use animusd::{ClientRequest, ClientResponse, Node, bind_cluster, read_frame, start_cluster};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

/// Send one request to a node's client address and return the reply.
async fn call(addr: std::net::SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect to node");
    animusd::write_frame(&mut stream, &req)
        .await
        .expect("send request");
    read_frame(&mut stream)
        .await
        .expect("read reply")
        .expect("a reply")
}

/// A plain `Put`, retried on ANY `ClientResponse::Error` reply for up to
/// 20s: a put is idempotent, and the first write against a fresh table
/// right after bootstrap can legitimately race the tablet-host reconciler
/// (`docs/engineering-lessons.md`'s "CP write-forward path has no
/// retry-on-not-the-leader-here" entry) or hit the confirm-loop
/// futility-retry shape (issue #268).
async fn put_retry(addr: std::net::SocketAddr, key: &[u8], value: &[u8]) -> ClientResponse {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let resp = call(
            addr,
            ClientRequest::Put {
                key: key.to_vec(),
                value: value.to_vec(),
                table: "kv".to_string(),
            },
        )
        .await;
        match resp {
            ClientResponse::PutOk => return resp,
            ClientResponse::Error(_) if tokio::time::Instant::now() < deadline => {
                sleep(Duration::from_millis(150)).await;
            }
            other => return other,
        }
    }
}

/// Wait until every node has the bootstrap tablet replicated, or panic.
async fn await_bootstrap(nodes: &[Node]) {
    let ready = async {
        loop {
            let leader = nodes.iter().any(Node::is_control_leader);
            let everyone_has_tablet = nodes.iter().all(|n| !n.metadata().members.is_empty());
            if leader && everyone_has_tablet {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), ready)
        .await
        .expect("cluster did not elect a leader and bootstrap within 20s");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_serves_put_get_and_status_over_tcp() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap(); // R = W = 2 over 3 replicas

    await_bootstrap(&nodes).await;

    // Status reflects the bootstrapped tablet and membership.
    let addr0 = nodes[0].client_addr();
    match call(addr0, ClientRequest::Status).await {
        ClientResponse::Status {
            metadata: meta,
            control_voters,
            ..
        } => {
            // ADR 0023: a fresh cluster has **no** data tablet until the first write
            // provisions one (the put below auto-provisions the `kv` table's tablet).
            assert_eq!(meta.tablets.len(), 0, "no data tablet until first write");
            assert_eq!(meta.members.len(), 3, "expected three members");
            // ADR 0037 PR2: a combined-mode node is a genuine control-group
            // voter (`ControlHandle::Local`), so its own `Status` reply
            // carries the live 3-member control-voter set straight off
            // `RaftCore::config()` — ids 0..3 by this cluster's id scheme
            // (`config::control_id`).
            assert_eq!(
                control_voters,
                std::collections::BTreeSet::from([0, 1, 2])
                    .into_iter()
                    .map(nid)
                    .collect(),
                "combined node's Status did not carry the live control-voter set"
            );
        }
        other => panic!("unexpected status response: {other:?}"),
    }

    // Put on one node, read back on another (quorum write/read over the cluster).
    let put = put_retry(addr0, b"hello", b"world").await;
    assert!(matches!(put, ClientResponse::PutOk), "put failed: {put:?}");

    let addr1 = nodes[1].client_addr();
    let got = call(
        addr1,
        ClientRequest::Get {
            key: b"hello".to_vec(),
            table: "kv".to_string(),
            stale: false,
        },
    )
    .await;
    assert_eq!(got, ClientResponse::Value(Some(b"world".to_vec())));

    // A missing key reaches quorum and reports absence.
    let missing = call(
        addr1,
        ClientRequest::Get {
            key: b"nope".to_vec(),
            table: "kv".to_string(),
            stale: false,
        },
    )
    .await;
    assert_eq!(missing, ClientResponse::Value(None));

    // An overwrite issued through a *different* coordinator node must win
    // (regression test: version assignment is quorum-derived, not a per-node
    // counter, so cross-node overwrites are not silently lost).
    let put2 = put_retry(addr1, b"hello", b"again").await;
    assert!(
        matches!(put2, ClientResponse::PutOk),
        "cross-node overwrite failed: {put2:?}"
    );
    let addr2 = nodes[2].client_addr();
    let got = call(
        addr2,
        ClientRequest::Get {
            key: b"hello".to_vec(),
            table: "kv".to_string(),
            stale: false,
        },
    )
    .await;
    assert_eq!(
        got,
        ClientResponse::Value(Some(b"again".to_vec())),
        "cross-node overwrite lost"
    );
}
