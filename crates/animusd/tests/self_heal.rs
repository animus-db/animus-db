//! Concurrency smoke test for the assembled `animusd` node (v1, ADR 0019: CP-only).
//!
//! Mirroring `animus-storage/tests/lsm_concurrent.rs`: many clients hammering the
//! assembled node concurrently complete without deadlock, over real `ProdEnv`/TCP.
//! Every op routes to the leaderful CP per-tablet Raft group (ADR 0017 #3a).
//!
//! (Autonomous **self-healing** of the AP plane — failure detection + tablet
//! re-placement — was the v0 behavior proven here; v1 drops the AP plane, and
//! automatic CP failure-detection / reconfigure over `ProdEnv` is later v1 work.
//! The control-plane mechanisms themselves remain proven deterministically in
//! `animus-control` under `SimEnv`.)

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, Node, bind_cluster, start_cluster};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect to node");
    animusd::write_frame(&mut stream, &req).await.expect("send");
    animusd::read_frame(&mut stream)
        .await
        .expect("read")
        .expect("a reply")
}

/// A plain `Put`, retried on ANY `ClientResponse::Error` reply for up to
/// 20s: a put is idempotent, and 16 concurrent first-writers hammering a
/// freshly auto-provisioned table right after bootstrap can legitimately
/// collide with the tablet-host reconciler or the confirm-loop
/// futility-retry shape (issue #268) — a retryable error here is not the
/// deadlock this test is actually checking for (a real deadlock still
/// exhausts the outer 30s bound).
async fn put_retry(addr: SocketAddr, key: &[u8], value: &[u8]) -> ClientResponse {
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

/// Wait until a leader is elected and every node has the bootstrap tablet.
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
async fn assembled_node_handles_concurrent_client_load_without_deadlock() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;

    let addr = nodes[0].client_addr();

    // Many clients concurrently put-then-get distinct keys. Each op routes to the
    // CP group leader; this asserts the assembled node serves concurrent load
    // without deadlocking or starving.
    let mut handles = Vec::new();
    for c in 0..16u32 {
        handles.push(tokio::spawn(async move {
            for r in 0..8u32 {
                let key = format!("c{c}-r{r}").into_bytes();
                let value = format!("v{c}-{r}").into_bytes();
                let put = put_retry(addr, &key, &value).await;
                assert!(matches!(put, ClientResponse::PutOk), "put failed: {put:?}");
                let got = call(
                    addr,
                    ClientRequest::Get {
                        key,
                        table: "kv".to_string(),
                    },
                )
                .await;
                assert_eq!(got, ClientResponse::Value(Some(value)));
            }
        }));
    }

    // The whole concurrent workload must finish well within this bound; a
    // deadlock would hang until the timeout fires.
    let all = async {
        for h in handles {
            h.await.expect("client task panicked");
        }
    };
    timeout(Duration::from_secs(30), all)
        .await
        .expect("concurrent client load did not complete in 30s (possible deadlock)");

    for node in &nodes {
        node.shutdown();
    }
}
