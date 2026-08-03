//! End-to-end test of the DynamoDB `Scan` and global-secondary-index surface
//! over the real TCP/HTTP wire. Mirrors `dynamo_extended.rs`: a 3-node
//! in-process cluster, driven by the actual DynamoDB JSON protocol
//! (`X-Amz-Target` header + AttributeValue-JSON body) over hand-written
//! HTTP/1.1. Real time/sockets, so it polls with generous timeouts.

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

/// Wait (bounded) until `index` on `table` is visible in `node`'s replicated
/// catalog. A `CreateTable` ack means the definition is durable on the *leader*; a
/// follower applies the replicated entry only after its own WAL fsync
/// (durable-before-visible, ADR 0009), so a cross-node query issued immediately
/// can race replication to that node. Wait for the definition before querying it.
async fn await_table_index(node: &Node, table: &str, index: &str) {
    let visible = async {
        loop {
            if node
                .metadata()
                .table_indexes(table)
                .iter()
                .any(|d| d.name == index)
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), visible)
        .await
        .unwrap_or_else(|_| panic!("index {index} on {table} not visible within 20s"));
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
async fn scan_paginates_a_whole_table() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap(); // R = W = 2 over 3 replicas
    await_bootstrap(&nodes).await;

    let addr0 = nodes[0].dynamo_addr();
    let addr1 = nodes[1].dynamo_addr();

    // A simple (id-only) table.
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"docs",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    // Five items. (Numbers carried as text, sorting lexicographically — fine
    // here, the ids are single digits.)
    for id in 0..5 {
        let (status, body) = dynamo(
            addr0,
            "DynamoDB_20120810.PutItem",
            &format!(
                r#"{{"TableName":"docs","Item":{{"id":{{"S":"{id}"}},
                    "kind":{{"S":"{}"}}}}}}"#,
                if id % 2 == 0 { "even" } else { "odd" }
            ),
        )
        .await;
        assert_eq!(status, 200, "PutItem({id}) failed: {body}");
    }

    // Scan page 1 with Limit 2: expect 2 items + a LastEvaluatedKey cursor.
    let (status, page1) = dynamo(
        addr1,
        "DynamoDB_20120810.Scan",
        r#"{"TableName":"docs","Limit":2}"#,
    )
    .await;
    assert_eq!(status, 200, "Scan page1 failed: {page1}");
    assert!(page1.contains("\"Count\":2"), "page1: {page1}");
    assert!(page1.contains("\"LastEvaluatedKey\""), "page1: {page1}");

    // Pull the cursor's id value out of the LastEvaluatedKey to continue.
    let cursor_id = extract_cursor_id(&page1);

    // Scan page 2 from the cursor: the remaining 3 items, no more cursor.
    let (status, page2) = dynamo(
        addr1,
        "DynamoDB_20120810.Scan",
        &format!(r#"{{"TableName":"docs","ExclusiveStartKey":{{"id":{{"S":"{cursor_id}"}}}}}}"#),
    )
    .await;
    assert_eq!(status, 200, "Scan page2 failed: {page2}");
    assert!(page2.contains("\"Count\":3"), "page2: {page2}");
    assert!(!page2.contains("LastEvaluatedKey"), "page2: {page2}");

    // A full scan returns all five.
    let (status, all) = dynamo(addr0, "DynamoDB_20120810.Scan", r#"{"TableName":"docs"}"#).await;
    assert_eq!(status, 200);
    assert!(all.contains("\"Count\":5"), "all: {all}");

    // A filtered scan: only the "even" items (3 of them: 0, 2, 4).
    let (status, even) = dynamo(
        addr1,
        "DynamoDB_20120810.Scan",
        r#"{"TableName":"docs","FilterExpression":"kind = :k",
            "ExpressionAttributeValues":{":k":{"S":"even"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "filtered scan failed: {even}");
    assert!(even.contains("\"Count\":3"), "even: {even}");
    assert!(even.contains("\"ScannedCount\":5"), "even: {even}");

    // Scan against a never-created table is a ResourceNotFoundException.
    let (status, body) = dynamo(addr0, "DynamoDB_20120810.Scan", r#"{"TableName":"ghost"}"#).await;
    assert_eq!(status, 400);
    assert!(body.contains("ResourceNotFoundException"), "got: {body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gsi_write_then_query() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;

    let addr0 = nodes[0].dynamo_addr();
    let addr1 = nodes[1].dynamo_addr();

    // CreateTable with a GSI on the `email` attribute.
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"users",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-email",
                 "KeySchema":[{"AttributeName":"email","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"ALL"}}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    assert!(body.contains("\"IndexName\":\"by-email\""), "got: {body}");

    // Three users; two share an email.
    for (id, email) in [("u1", "a@x"), ("u2", "b@x"), ("u3", "a@x")] {
        let (status, body) = dynamo(
            addr0,
            "DynamoDB_20120810.PutItem",
            &format!(
                r#"{{"TableName":"users","Item":{{"id":{{"S":"{id}"}},
                    "email":{{"S":"{email}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "PutItem({id}) failed: {body}");
    }

    // Wait for the GSI definition to replicate to node 1 before querying it there
    // (the query target ≠ the create node; cross-node reads race replication).
    await_table_index(&nodes[1], "users", "by-email").await;

    // Query the GSI for a@x (from a different node → quorum read): u1 and u3.
    let (status, body) = dynamo(
        addr1,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"users","IndexName":"by-email",
            "KeyConditionExpression":"email = :e",
            "ExpressionAttributeValues":{":e":{"S":"a@x"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "GSI query failed: {body}");
    assert!(body.contains("\"Count\":2"), "got: {body}");
    assert!(body.contains(r#""id":{"S":"u1"}"#), "got: {body}");
    assert!(body.contains(r#""id":{"S":"u3"}"#), "got: {body}");
    assert!(!body.contains(r#""id":{"S":"u2"}"#), "got: {body}");

    // Deleting u3 removes it from the index.
    let (status, _) = dynamo(
        addr0,
        "DynamoDB_20120810.DeleteItem",
        r#"{"TableName":"users","Key":{"id":{"S":"u3"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    let (status, body) = dynamo(
        addr1,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"users","IndexName":"by-email",
            "KeyConditionExpression":"email = :e",
            "ExpressionAttributeValues":{":e":{"S":"a@x"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert!(body.contains("\"Count\":1"), "after delete: {body}");
    assert!(body.contains(r#""id":{"S":"u1"}"#), "after delete: {body}");

    // Querying an undeclared index is a ValidationException.
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"users","IndexName":"nope",
            "KeyConditionExpression":"email = :e",
            "ExpressionAttributeValues":{":e":{"S":"a@x"}}}"#,
    )
    .await;
    assert_eq!(status, 400);
    assert!(body.contains("ValidationException"), "got: {body}");
}

/// A `Scan` after a `DeleteItem` must (a) omit the deleted item and (b) keep
/// pagination correct even when the page boundary would have fallen on the deleted
/// item — the `Limit` counts only live, decoded items, so a tombstone value never
/// consumes a slot or strands the cursor. This guards the native-scan rewrite,
/// where a DynamoDB delete stores a *tombstone value* the data plane still returns
/// as a live pair.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_skips_deleted_items_and_paginates() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"docs","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    )
    .await;
    assert_eq!(status, 200);

    // Five items 0..5.
    for id in 0..5 {
        let (status, _) = dynamo(
            addr,
            "DynamoDB_20120810.PutItem",
            &format!(r#"{{"TableName":"docs","Item":{{"id":{{"S":"{id}"}}}}}}"#),
        )
        .await;
        assert_eq!(status, 200);
    }
    // Delete "1" — it stores a tombstone value (a live data-plane pair).
    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.DeleteItem",
        r#"{"TableName":"docs","Key":{"id":{"S":"1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);

    // A full scan omits the deleted item: 4 live items, no "1".
    let (status, all) = dynamo(addr, "DynamoDB_20120810.Scan", r#"{"TableName":"docs"}"#).await;
    assert_eq!(status, 200, "scan: {all}");
    assert!(
        all.contains("\"Count\":4"),
        "deleted item not omitted: {all}"
    );
    assert!(
        !all.contains(r#""id":{"S":"1"}"#),
        "deleted item present: {all}"
    );

    // Limit 2 with the deleted item near the boundary: still exactly 2 live items
    // and a usable cursor (the tombstone neither fills a slot nor strands it).
    let (status, page1) = dynamo(
        addr,
        "DynamoDB_20120810.Scan",
        r#"{"TableName":"docs","Limit":2}"#,
    )
    .await;
    assert_eq!(status, 200, "page1: {page1}");
    assert!(page1.contains("\"Count\":2"), "page1 count: {page1}");
    assert!(
        page1.contains("\"LastEvaluatedKey\""),
        "page1 cursor: {page1}"
    );
    let cursor_id = extract_cursor_id(&page1);

    // Continue: the remaining 2 live items (0,2,3,4 minus the first two), no cursor.
    let (status, page2) = dynamo(
        addr,
        "DynamoDB_20120810.Scan",
        &format!(r#"{{"TableName":"docs","ExclusiveStartKey":{{"id":{{"S":"{cursor_id}"}}}}}}"#),
    )
    .await;
    assert_eq!(status, 200, "page2: {page2}");
    assert!(page2.contains("\"Count\":2"), "page2 count: {page2}");
    assert!(
        !page2.contains(r#""id":{"S":"1"}"#),
        "deleted item paged in: {page2}"
    );
}

/// Pull the `id` string out of a `LastEvaluatedKey` of the form
/// `"LastEvaluatedKey":{"id":{"S":"<v>"}}` in a scan response body.
fn extract_cursor_id(body: &str) -> String {
    let marker = "\"LastEvaluatedKey\":{\"id\":{\"S\":\"";
    let start = body.find(marker).expect("LastEvaluatedKey present") + marker.len();
    let end = start + body[start..].find('"').expect("closing quote");
    body[start..end].to_string()
}
