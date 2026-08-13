//! `ConsistentRead` fidelity (ADR 0041 §5), end to end over real HTTP: a
//! `Query` with `ConsistentRead: true` against a **GSI** is rejected
//! (`ValidationException`, matching DynamoDB's own contract — a GSI is
//! maintained asynchronously); against an **LSI** — or the base table — it is
//! legal and already true, since every non-GSI read here is linearizable.
//!
//! Mirrors `dynamo_documents.rs`/`dynamo_gsi_drain.rs`'s bring-up and
//! converged-or-timeout idiom (a GSI is eventually consistent by DynamoDB's
//! own contract, so proving it drained before asserting the rejection needs a
//! poll; the LSI/base acceptance checks stay plain immediate assertions).

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{Node, bind_cluster, start_cluster};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

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
    timeout(Duration::from_secs(20), ready)
        .await
        .expect("cluster did not bootstrap within 20s");
}

/// One DynamoDB JSON request over the real HTTP wire.
async fn dynamo(addr: SocketAddr, target: &str, body: &str) -> (u16, String) {
    let mut s = TcpStream::connect(addr).await.expect("connect");
    let req = format!(
        "POST / HTTP/1.1\r\nHost: x\r\nX-Amz-Target: {target}\r\n\
         Connection: close\r\n\
         Content-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes()).await.expect("write");
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.expect("read");
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let body = text.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or("");
    (status, body.to_owned())
}

/// Poll a `Query` until `accept` is satisfied, returning the last body
/// observed — the GSI-side convergence idiom every ADR 0041 test file
/// repeats (see e.g. `dynamo_gsi_drain.rs::await_gsi_query`).
async fn await_query(addr: SocketAddr, body: &str, accept: impl Fn(u16, &str) -> bool) -> String {
    let last = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let seen = std::sync::Arc::clone(&last);
    let converged = async move {
        loop {
            let (status, got) = dynamo(addr, "DynamoDB_20120810.Query", body).await;
            if accept(status, &got) {
                return got;
            }
            *seen.lock().unwrap() = got;
            sleep(Duration::from_millis(100)).await;
        }
    };
    match timeout(Duration::from_secs(15), converged).await {
        Ok(body) => body,
        Err(_) => panic!(
            "query never converged within 15s (last saw: {})",
            last.lock().unwrap()
        ),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consistent_read_rejects_gsi_query_but_accepts_lsi_and_base() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;

    let addr = nodes[0].dynamo_addr();

    // A composite table with one GSI (its own async hash keyspace) and one
    // LSI (colocated, atomic with the base row).
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"events",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-kind",
                 "KeySchema":[{"AttributeName":"kind","KeyType":"HASH"}]}],
            "LocalSecondaryIndexes":[
                {"IndexName":"by-ts",
                 "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                              {"AttributeName":"ts","KeyType":"RANGE"}]}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"events","Item":{
            "pk":{"S":"p1"},"sk":{"S":"a"},"kind":{"S":"click"},"ts":{"S":"10"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "PutItem failed: {body}");

    // Wait for the GSI to actually drain before probing its `ConsistentRead`
    // rejection — the rejection must happen up front, before ever touching
    // the (possibly still-empty) hidden table, but converging first makes
    // this test robust to either implementation choice and matches every
    // other GSI assertion's discipline in this suite.
    await_query(
        addr,
        r#"{"TableName":"events","IndexName":"by-kind",
            "KeyConditionExpression":"kind = :k",
            "ExpressionAttributeValues":{":k":{"S":"click"}}}"#,
        |status, body| status == 200 && body.contains("\"Count\":1"),
    )
    .await;

    // `ConsistentRead: true` against the GSI is a `ValidationException` —
    // DynamoDB's own contract, since a GSI is asynchronously maintained.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"events","IndexName":"by-kind","ConsistentRead":true,
            "KeyConditionExpression":"kind = :k",
            "ExpressionAttributeValues":{":k":{"S":"click"}}}"#,
    )
    .await;
    assert_eq!(status, 400, "GSI ConsistentRead should be rejected: {body}");
    assert!(
        body.contains("ValidationException"),
        "expected ValidationException: {body}"
    );

    // `ConsistentRead: true` against the LSI is legal and already true (the
    // LSI row commits atomically with the base row) — no drain to wait on.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"events","IndexName":"by-ts","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "LSI ConsistentRead should be accepted: {body}");
    assert!(body.contains("\"Count\":1"), "LSI query result: {body}");

    // `ConsistentRead: true` against the base table (no `IndexName`) is
    // likewise legal and already true.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}}}"#,
    )
    .await;
    assert_eq!(
        status, 200,
        "base ConsistentRead should be accepted: {body}"
    );
    assert!(body.contains("\"Count\":1"), "base query result: {body}");

    // `ConsistentRead: true` against a plain `GetItem` is likewise
    // accept-and-ignore.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.GetItem",
        r#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a"}},
            "ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(
        status, 200,
        "GetItem ConsistentRead should be accepted: {body}"
    );
    assert!(body.contains("\"Item\""), "GetItem result: {body}");
}
