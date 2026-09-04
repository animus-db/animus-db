//! The LSI `Query` native read path (ADR 0041 §5), end to end over the real
//! DynamoDB JSON/HTTP wire — specifically the **forwarded** `KindScan` path
//! `ClientCtx::cp_scan_kind` uses when the receiving node does not lead the
//! base tablet's group: a 3-node cluster, one un-split table (one tablet, one
//! leader), and the identical LSI `Query` issued through every node's own
//! dynamo listener in turn — the house pattern for exactly the bimodal
//! per-process flake class an un-forwarded internal RPC would be (see the
//! root `CLAUDE.md`'s house lesson on adding a variant to a forwarded command
//! enum, and `dynamo_txn.rs`'s identical multi-node dispatch style).
//!
//! Also covers the refusal half: a bare (non-`Forwarded`) `KindScan` over the
//! plain client protocol must be rejected, mirroring `KindWrite`'s own
//! bare-refusal contract.

use std::net::SocketAddr;
use std::time::Duration;

use animus_cp_data::KIND_LSI;
use animusd::{ClientRequest, ClientResponse, Node, bind_cluster, read_frame, start_cluster};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

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

/// An LSI `Query`, issued through **every** node of a 3-node cluster in turn,
/// must succeed regardless of which node happens to lead the base tablet's
/// Raft group — the follower-connected forwarding regression `KindScan`
/// exists for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lsi_query_succeeds_through_every_node_including_non_leaders() {
    let dir = support::panic_safe_tempdir();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;

    let create_addr = nodes[0].dynamo_addr();
    let (status, body) = dynamo(
        create_addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"events","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"},{"AttributeName":"ts","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "LocalSecondaryIndexes":[
                {"IndexName":"by-ts",
                 "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                              {"AttributeName":"ts","KeyType":"RANGE"}]}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    // Three items in one partition, distinct sort keys and `ts` values — one
    // un-split table has exactly one tablet, hence exactly one leader; at
    // least two of the three nodes below are therefore NOT that leader.
    for (sk, ts) in [("a", "30"), ("b", "10"), ("c", "20")] {
        let (status, body) = dynamo(
            create_addr,
            "DynamoDB_20120810.PutItem",
            &format!(
                r#"{{"TableName":"events","Item":{{
                    "pk":{{"S":"p1"}},"sk":{{"S":"{sk}"}},"ts":{{"S":"{ts}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "PutItem({sk}) failed: {body}");
    }

    // LSI rows are written atomically with the base row (ADR 0041 §2), so a
    // *strongly consistent* query sees them immediately — no polling needed,
    // unlike a GSI. But since ADR 0055 `ConsistentRead` selects a real read
    // path and defaults to `false` (the eventual, replica-local one), this
    // write-verification read must ask for `ConsistentRead: true` explicitly
    // — which also happens to be what exercises the forwarded ReadIndex path
    // through non-leaders this test is actually after.
    for (i, node) in nodes.iter().enumerate() {
        let (status, body) = dynamo(
            node.dynamo_addr(),
            "DynamoDB_20120810.Query",
            r#"{"TableName":"events","IndexName":"by-ts","ConsistentRead":true,
                "KeyConditionExpression":"pk = :p",
                "ExpressionAttributeValues":{":p":{"S":"p1"}}}"#,
        )
        .await;
        assert_eq!(status, 200, "LSI query via node {i} failed: {body}");
        assert!(body.contains("\"Count\":3"), "node {i}: {body}");
        // ts-ordered: 10 (b), 20 (c), 30 (a).
        let b = body
            .find(r#""sk":{"S":"b"}"#)
            .unwrap_or_else(|| panic!("node {i}: b present: {body}"));
        let c = body
            .find(r#""sk":{"S":"c"}"#)
            .unwrap_or_else(|| panic!("node {i}: c present: {body}"));
        let a = body
            .find(r#""sk":{"S":"a"}"#)
            .unwrap_or_else(|| panic!("node {i}: a present: {body}"));
        assert!(b < c && c < a, "node {i}: LSI not ts-ordered: {body}");
    }
}

/// A bare (non-`Forwarded`) `KindScan` over the plain client protocol must be
/// refused — the read-side dual of `KindWrite`'s identical bare refusal (ADR
/// 0041 §5): a client could otherwise read a table's LSI/change-log/footprint
/// bytes directly by kind number, bypassing the DynamoDB surface that
/// interprets them. **ADR 0047**: `KindScan` is `Surface::Intra`, so this
/// refusal now comes from `handle_request`'s client-port guard, not the
/// match arm's own "must be sent wrapped" text (still reachable, just only
/// via the intra port now — see `intra_port_split.rs`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bare_kind_scan_is_refused() {
    let dir = support::panic_safe_tempdir();
    let bound = bind_cluster(1, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;

    let mut stream = TcpStream::connect(nodes[0].client_addr())
        .await
        .expect("connect to client port");
    let request = ClientRequest::KindScan {
        table: "events".to_owned(),
        kind: KIND_LSI,
        start: Vec::new(),
        end: Some(vec![0xFF]),
        limit: None,
        reverse: false,
        stale: false,
    };
    animusd::write_frame(&mut stream, &request)
        .await
        .expect("write frame");
    let response: ClientResponse = read_frame(&mut stream)
        .await
        .expect("read frame")
        .expect("connection stayed open for a reply");
    match response {
        ClientResponse::Error(msg) => {
            assert!(
                msg.contains("cluster-internal request"),
                "expected the ADR 0047 client-port refusal message, got: {msg}"
            );
        }
        other => panic!("expected a bare-request refusal, got: {other:?}"),
    }
}
