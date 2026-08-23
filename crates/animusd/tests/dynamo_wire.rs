//! End-to-end test of the DynamoDB JSON wire endpoint over real TCP/HTTP.
//!
//! Starts a 3-node in-process cluster, then drives `PutItem` → `GetItem` →
//! `DeleteItem` against one node's `dynamo` endpoint by speaking the actual
//! DynamoDB JSON protocol (an `X-Amz-Target` header + AttributeValue-JSON body
//! over hand-written HTTP/1.1). Like the other `animusd` tests this uses real
//! time and sockets, so it polls with generous timeouts.

use std::time::Duration;

use animusd::{Node, bind_cluster, start_cluster};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

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
async fn dynamo_wire_put_get_delete_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap(); // R = W = 2 over 3 replicas
    await_bootstrap(&nodes).await;

    let addr0 = nodes[0].dynamo_addr();
    let addr1 = nodes[1].dynamo_addr();

    // PutItem on node 0.
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"users","Item":{"pk":{"S":"u1"},"name":{"S":"Ada"},
            "score":{"N":"42"},"admin":{"BOOL":true}}}"#,
    )
    .await;
    assert_eq!(status, 200, "PutItem failed: {body}");
    assert_eq!(body, "{}");

    // GetItem on node 1, `ConsistentRead: true` (ADR 0055): this reads back
    // the write just made on node 0, so it needs the linearizable path — the
    // wire default is now a genuinely eventually-consistent read.
    let (status, body) = dynamo(
        addr1,
        "DynamoDB_20120810.GetItem",
        r#"{"ConsistentRead":true,"TableName":"users","Key":{"pk":{"S":"u1"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "GetItem failed: {body}");
    assert!(body.contains(r#""name":{"S":"Ada"}"#), "got: {body}");
    assert!(body.contains(r#""score":{"N":"42"}"#), "got: {body}");
    assert!(body.contains(r#""admin":{"BOOL":true}"#), "got: {body}");

    // A missing key returns 200 with an empty body (DynamoDB semantics).
    let (status, body) = dynamo(
        addr1,
        "DynamoDB_20120810.GetItem",
        r#"{"TableName":"users","Key":{"pk":{"S":"nobody"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body, "{}", "absent item should yield an empty body");

    // DeleteItem on node 1, then GetItem on node 0 sees it gone (the delete is a
    // tombstone in the data plane, read back as absent).
    let (status, _) = dynamo(
        addr1,
        "DynamoDB_20120810.DeleteItem",
        r#"{"TableName":"users","Key":{"pk":{"S":"u1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);

    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.GetItem",
        r#"{"ConsistentRead":true,"TableName":"users","Key":{"pk":{"S":"u1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body, "{}", "deleted item should read as absent");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dynamo_wire_rejects_bad_requests() {
    let dir = tempfile::tempdir().unwrap();
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
