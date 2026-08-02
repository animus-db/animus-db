//! End-to-end test of the extended DynamoDB JSON wire surface over real
//! TCP/HTTP: document/set attribute types (`M`/`L`/`SS`/`NS`/`BS`), projection
//! expressions, `ReturnValues: ALL_OLD`, multiple + composite GSIs, and a local
//! secondary index. Mirrors `dynamo_extended.rs`: a 3-node in-process cluster
//! driven by the actual DynamoDB JSON protocol over hand-written HTTP/1.1. Real
//! time/sockets, so it polls with generous timeouts.

use std::time::Duration;

use animusd::{Node, bind_cluster, start_cluster};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

async fn await_bootstrap(nodes: &[Node]) {
    let ready = async {
        loop {
            let leader = nodes.iter().any(Node::is_control_leader);
            let everyone_has_tablet = nodes.iter().all(|n| !n.metadata().tablets.is_empty());
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
async fn document_set_types_projection_and_return_values() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound, 2, 2).await.unwrap(); // R = W = 2 over 3 replicas
    await_bootstrap(&nodes).await;

    let addr0 = nodes[0].dynamo_addr();
    let addr1 = nodes[1].dynamo_addr();

    let (status, _) = dynamo(
        addr0,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"profiles",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    )
    .await;
    assert_eq!(status, 200);

    // PutItem carrying a map, a list, and a string set.
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"profiles","Item":{
            "id":{"S":"u1"},
            "name":{"S":"Ada"},
            "address":{"M":{"city":{"S":"London"},"zip":{"N":"7"}}},
            "scores":{"L":[{"N":"1"},{"N":"2"},{"S":"x"}]},
            "tags":{"SS":["b","a","a"]}
        }}"#,
    )
    .await;
    assert_eq!(status, 200, "PutItem with document types failed: {body}");

    // GetItem round-trips the document/set types (set is sorted/deduped).
    let (status, body) = dynamo(
        addr1,
        "DynamoDB_20120810.GetItem",
        r#"{"TableName":"profiles","Key":{"id":{"S":"u1"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "GetItem failed: {body}");
    assert!(
        body.contains(r#""M":{"city":{"S":"London"}"#),
        "map: {body}"
    );
    assert!(
        body.contains(r#""L":[{"N":"1"},{"N":"2"},{"S":"x"}]"#),
        "list: {body}"
    );
    assert!(
        body.contains(r#""SS":["a","b"]"#),
        "set sorted/deduped: {body}"
    );

    // GetItem with a ProjectionExpression (with a #name alias): only id + name.
    let (status, body) = dynamo(
        addr1,
        "DynamoDB_20120810.GetItem",
        r##"{"TableName":"profiles","Key":{"id":{"S":"u1"}},
            "ProjectionExpression":"id, #n",
            "ExpressionAttributeNames":{"#n":"name"}}"##,
    )
    .await;
    assert_eq!(status, 200, "projected GetItem failed: {body}");
    assert!(body.contains(r#""id":{"S":"u1"}"#), "id kept: {body}");
    assert!(body.contains(r#""name":{"S":"Ada"}"#), "name kept: {body}");
    assert!(!body.contains("address"), "address projected out: {body}");
    assert!(!body.contains("tags"), "tags projected out: {body}");

    // ReturnValues: ALL_OLD on an overwrite echoes the prior item.
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"profiles","Item":{"id":{"S":"u1"},"name":{"S":"Grace"}},
            "ReturnValues":"ALL_OLD"}"#,
    )
    .await;
    assert_eq!(status, 200, "ALL_OLD put failed: {body}");
    assert!(body.contains("\"Attributes\""), "has Attributes: {body}");
    assert!(
        body.contains(r#""name":{"S":"Ada"}"#),
        "old name echoed: {body}"
    );

    // ReturnValues: ALL_OLD on DeleteItem echoes the deleted item.
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.DeleteItem",
        r#"{"TableName":"profiles","Key":{"id":{"S":"u1"}},"ReturnValues":"ALL_OLD"}"#,
    )
    .await;
    assert_eq!(status, 200, "ALL_OLD delete failed: {body}");
    assert!(body.contains("\"Attributes\""), "has Attributes: {body}");
    assert!(
        body.contains(r#""name":{"S":"Grace"}"#),
        "deleted item echoed: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multiple_gsis_composite_gsi_and_lsi() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound, 2, 2).await.unwrap();
    await_bootstrap(&nodes).await;

    let addr0 = nodes[0].dynamo_addr();
    let addr1 = nodes[1].dynamo_addr();

    // A composite (pk, sk) table with: two GSIs (one hash-only, one composite)
    // and one LSI (alternate sort attribute within the base partition).
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"events",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-kind",
                 "KeySchema":[{"AttributeName":"kind","KeyType":"HASH"}]},
                {"IndexName":"by-actor-ts",
                 "KeySchema":[{"AttributeName":"actor","KeyType":"HASH"},
                              {"AttributeName":"ts","KeyType":"RANGE"}]}],
            "LocalSecondaryIndexes":[
                {"IndexName":"by-ts",
                 "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                              {"AttributeName":"ts","KeyType":"RANGE"}]}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    assert!(body.contains("\"IndexName\":\"by-kind\""), "gsi1: {body}");
    assert!(
        body.contains("\"IndexName\":\"by-actor-ts\""),
        "gsi2: {body}"
    );
    assert!(
        body.contains("\"LocalSecondaryIndexes\""),
        "lsi present: {body}"
    );

    // Items in partition "p1" with sort keys + a `kind`, `actor`, `ts`.
    let put = |addr, pk: &str, sk: &str, kind: &str, actor: &str, ts: &str| {
        let body = format!(
            r#"{{"TableName":"events","Item":{{
                "pk":{{"S":"{pk}"}},"sk":{{"S":"{sk}"}},
                "kind":{{"S":"{kind}"}},"actor":{{"S":"{actor}"}},"ts":{{"S":"{ts}"}}}}}}"#
        );
        async move {
            let (status, b) = dynamo(addr, "DynamoDB_20120810.PutItem", &body).await;
            assert_eq!(status, 200, "PutItem failed: {b}");
        }
    };
    put(addr0, "p1", "a", "click", "alice", "30").await;
    put(addr0, "p1", "b", "view", "alice", "10").await;
    put(addr0, "p1", "c", "click", "bob", "20").await;
    put(addr0, "p2", "a", "click", "alice", "05").await;

    // Hash-only GSI by-kind = click: three items (p1/a, p1/c, p2/a).
    let (status, body) = dynamo(
        addr1,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"events","IndexName":"by-kind",
            "KeyConditionExpression":"kind = :k",
            "ExpressionAttributeValues":{":k":{"S":"click"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "GSI by-kind failed: {body}");
    assert!(body.contains("\"Count\":3"), "by-kind: {body}");

    // Composite GSI by-actor-ts: actor=alice, ts BETWEEN 10 AND 30 → p1/a, p1/b.
    let (status, body) = dynamo(
        addr1,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"events","IndexName":"by-actor-ts",
            "KeyConditionExpression":"actor = :a AND ts BETWEEN :lo AND :hi",
            "ExpressionAttributeValues":{":a":{"S":"alice"},":lo":{"S":"10"},":hi":{"S":"30"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "composite GSI failed: {body}");
    assert!(body.contains("\"Count\":2"), "by-actor-ts count: {body}");
    // p2/a (ts 05) is excluded by the BETWEEN; the items are ts-ordered (b, a).
    let b = body.find(r#""sk":{"S":"b"}"#).expect("b present");
    let a = body.find(r#""sk":{"S":"a"}"#).expect("a present");
    assert!(b < a, "composite GSI not ts-ordered: {body}");
    assert!(!body.contains(r#""pk":{"S":"p2"}"#), "p2 excluded: {body}");

    // LSI by-ts within partition p1, ordered by ts: 10 (b), 20 (c), 30 (a).
    let (status, body) = dynamo(
        addr1,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"events","IndexName":"by-ts",
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "LSI failed: {body}");
    assert!(body.contains("\"Count\":3"), "LSI count: {body}");
    let b = body.find(r#""sk":{"S":"b"}"#).expect("b present");
    let c = body.find(r#""sk":{"S":"c"}"#).expect("c present");
    let a = body.find(r#""sk":{"S":"a"}"#).expect("a present");
    assert!(b < c && c < a, "LSI not ts-ordered: {body}");

    // A sort condition on the hash-only GSI is rejected.
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"events","IndexName":"by-kind",
            "KeyConditionExpression":"kind = :k AND ts = :t",
            "ExpressionAttributeValues":{":k":{"S":"click"},":t":{"S":"30"}}}"#,
    )
    .await;
    assert_eq!(status, 400, "expected rejection: {body}");
    assert!(body.contains("ValidationException"), "got: {body}");
}
