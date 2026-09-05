//! End-to-end test of the extended DynamoDB JSON wire surface over real
//! TCP/HTTP: `CreateTable`, `Query` (partition + sort-key conditions), and
//! conditional writes (`attribute_not_exists`). Mirrors `dynamo_wire.rs`: a
//! 3-node in-process cluster, driven by the actual DynamoDB JSON protocol
//! (`X-Amz-Target` header + AttributeValue-JSON body) over hand-written
//! HTTP/1.1. Real time/sockets, so it polls with generous timeouts.

use std::time::Duration;

use animusd::{Node, bind_cluster, start_cluster};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

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

/// One DynamoDB request over a fresh HTTP/1.1 connection → `(status, body)`.
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
async fn create_table_query_and_conditional_writes() {
    let dir = support::panic_safe_tempdir();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap(); // R = W = 2 over 3 replicas
    await_bootstrap(&nodes).await;

    let addr0 = nodes[0].dynamo_addr();
    let addr1 = nodes[1].dynamo_addr();

    // CreateTable with a composite (pk, sk) schema.
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"events",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},
                                    {"AttributeName":"sk","AttributeType":"S"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    assert!(body.contains("\"TableStatus\":\"ACTIVE\""), "got: {body}");

    // Re-creating the same table is rejected (ResourceInUseException, 400).
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"events","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}]}"#,
    )
    .await;
    assert_eq!(status, 400);
    assert!(body.contains("ResourceInUseException"), "got: {body}");

    // Put three items in partition "u1" with sort keys a, b, c (out of order).
    for sk in ["c", "a", "b"] {
        let (status, body) = dynamo(
            addr0,
            "DynamoDB_20120810.PutItem",
            &format!(
                r#"{{"TableName":"events","Item":{{"pk":{{"S":"u1"}},
                    "sk":{{"S":"{sk}"}},"v":{{"N":"{}"}}}}}}"#,
                sk.as_bytes()[0]
            ),
        )
        .await;
        assert_eq!(status, 200, "PutItem({sk}) failed: {body}");
    }
    // And one item in a different partition, to prove isolation.
    let (status, _) = dynamo(
        addr0,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"events","Item":{"pk":{"S":"u2"},"sk":{"S":"z"}}}"#,
    )
    .await;
    assert_eq!(status, 200);

    // Query the whole partition (from node 1, a quorum read across the cluster):
    // items come back in sort order a, b, c.
    let (status, body) = dynamo(
        addr1,
        "DynamoDB_20120810.Query",
        // ConsistentRead: true (ADR 0055, #604): asserts on the test's own
        // just-written rows.
        r#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"u1"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "Query failed: {body}");
    assert!(body.contains("\"Count\":3"), "got: {body}");
    let a = body.find(r#""sk":{"S":"a"}"#).expect("a present");
    let b = body.find(r#""sk":{"S":"b"}"#).expect("b present");
    let c = body.find(r#""sk":{"S":"c"}"#).expect("c present");
    assert!(a < b && b < c, "items not in sort order: {body}");

    // Query with begins_with and BETWEEN sort conditions.
    let (status, body) = dynamo(
        addr1,
        "DynamoDB_20120810.Query",
        // ConsistentRead: true (ADR 0055, #604): asserts on the test's own
        // just-written rows.
        r#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p AND sk = :s",
            "ExpressionAttributeValues":{":p":{"S":"u1"},":s":{"S":"b"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert!(body.contains("\"Count\":1"), "got: {body}");
    assert!(body.contains(r#""sk":{"S":"b"}"#), "got: {body}");

    let (status, body) = dynamo(
        addr1,
        "DynamoDB_20120810.Query",
        // ConsistentRead: true (ADR 0055, #604): asserts on the test's own
        // just-written rows.
        r#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p AND sk BETWEEN :lo AND :hi",
            "ExpressionAttributeValues":{":p":{"S":"u1"},":lo":{"S":"a"},":hi":{"S":"b"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert!(body.contains("\"Count\":2"), "BETWEEN got: {body}");

    // Conditional write: attribute_not_exists(pk) succeeds for a new key...
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"events","Item":{"pk":{"S":"u3"},"sk":{"S":"x"}},
            "ConditionExpression":"attribute_not_exists(pk)"}"#,
    )
    .await;
    assert_eq!(status, 200, "first conditional put failed: {body}");

    // ...and fails for the same key the second time (ConditionalCheckFailed).
    let (status, body) = dynamo(
        addr1,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"events","Item":{"pk":{"S":"u3"},"sk":{"S":"x"}},
            "ConditionExpression":"attribute_not_exists(pk)"}"#,
    )
    .await;
    assert_eq!(status, 400);
    assert!(
        body.contains("ConditionalCheckFailedException"),
        "got: {body}"
    );

    // A Query against a never-created table is a ResourceNotFoundException.
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"ghost","KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"u1"}}}"#,
    )
    .await;
    assert_eq!(status, 400);
    assert!(body.contains("ResourceNotFoundException"), "got: {body}");
}

/// Regression: **concurrent** conditional puts are serialized by the per-node
/// `rmw_lock` (which the DynamoDB edge once never took): two simultaneous
/// `attribute_not_exists(pk)` `PutItem`s on the same key through the same node
/// must yield exactly one success and one `ConditionalCheckFailedException` —
/// without the lock both read "absent" and both succeed (a lost update / double
/// create). Runs several rounds on distinct keys so a lucky interleaving can't
/// mask the race, timeout-guarded like every real-time `animusd` test.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_conditional_puts_one_wins() {
    let dir = support::panic_safe_tempdir();
    let bound = bind_cluster(1, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    let rounds = async {
        for round in 0..10 {
            let body = format!(
                r#"{{"TableName":"claims","Item":{{"pk":{{"S":"claim-{round}"}},"owner":{{"S":"me"}}}},
                    "ConditionExpression":"attribute_not_exists(pk)"}}"#
            );
            // Two clients race the same key through the same node.
            let (a, b) = tokio::join!(
                dynamo(addr, "DynamoDB_20120810.PutItem", &body),
                dynamo(addr, "DynamoDB_20120810.PutItem", &body),
            );
            let outcomes = [&a, &b];
            let wins = outcomes.iter().filter(|(s, _)| *s == 200).count();
            assert_eq!(
                wins, 1,
                "round {round}: exactly one conditional put must win, got {a:?} / {b:?}"
            );
            let loser = outcomes.iter().find(|(s, _)| *s != 200).unwrap();
            assert_eq!(loser.0, 400, "round {round}: loser status: {loser:?}");
            assert!(
                loser.1.contains("ConditionalCheckFailedException"),
                "round {round}: loser must fail the condition, got: {}",
                loser.1
            );
        }
    };
    timeout(Duration::from_secs(60), rounds)
        .await
        .expect("concurrent conditional puts did not settle within 60s");
}
