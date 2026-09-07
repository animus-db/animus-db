//! End-to-end test of the DynamoDB JSON wire endpoint over real TCP/HTTP.
//!
//! Like the other `animusd` tests this uses real time and sockets, so it
//! polls with generous timeouts.
//!
//! **`dynamo_wire_put_get_delete_round_trip` moved to `SimCluster`**
//! (ADR 0061 rung D3, redundancy-audit follow-up) — item decode/dispatch is
//! proven by `sim_cluster_dynamo.rs::
//! put_then_consistent_get_through_wire_from_a_non_leader_node`
//! (`crates/animusd/src/sim_cluster_dynamo.rs`); the HTTP framing layer it
//! also touched stays covered by the ~60 other `dynamo_*.rs` real-socket
//! binaries. `dynamo_wire_rejects_bad_requests` stays here: unknown-op/
//! malformed-body rejection over the real listener has no sim analog.

use std::time::Duration;

use animusd::{Node, bind_cluster, start_cluster};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

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

/// One DynamoDB request over a fresh HTTP/1.1 connection. Returns
/// `(status_code, body)`.
async fn dynamo(addr: std::net::SocketAddr, target: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to dynamo");
    let request = format!(
        "POST / HTTP/1.1\r\n\
         Host: animus\r\n\
         X-Amz-Target: {target}\r\n\
         Content-Type: application/x-amz-json-1.0\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len(),
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("send request");
    stream.flush().await.expect("flush");

    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .await
        .expect("read full response");
    let text = String::from_utf8(raw).expect("utf8 response");
    let (head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status line");
    (status, payload.to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dynamo_wire_rejects_bad_requests() {
    let dir = support::panic_safe_tempdir();
    let bound = bind_cluster(1, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    // A genuinely unknown operation.
    let (status, body) = dynamo(addr, "DynamoDB_20120810.NoSuchThing", "{}").await;
    assert_eq!(status, 400);
    assert!(body.contains("UnknownOperationException"), "got: {body}");

    // `BatchGetItem` is supported now, so a malformed body is a validation
    // error rather than an unknown operation.
    let (status, body) = dynamo(addr, "DynamoDB_20120810.BatchGetItem", "{}").await;
    assert_eq!(status, 400);
    assert!(body.contains("ValidationException"), "got: {body}");

    // PutItem missing the partition-key attribute.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"t","Item":{"name":{"S":"x"}}}"#,
    )
    .await;
    assert_eq!(status, 400);
    assert!(body.contains("ValidationException"), "got: {body}");
}
