//! End-to-end tests of the DynamoDB edge consuming the **replicated schema
//! catalog** (ADR 0013) and the **extended surface** (per-index projections,
//! document-path projections, `UpdateItem`/`BatchWriteItem`/`TransactWriteItems`)
//! over the real DynamoDB JSON/HTTP wire.
//!
//! - `create_table_survives_node_restart` mirrors `durable_restart.rs`: a
//!   single-node cluster `CreateTable`s, the node is stopped and restarted on the
//!   **same dir + addresses**, and the table is still known (its key schema rode
//!   the control-plane Raft WAL, not the in-memory registry). This is the headline
//!   ADR 0013 consumption: a created table is now durable + cluster-agreed.
//! - `extended_surface` mirrors `dynamo_extended.rs`: a 3-node in-process cluster
//!   exercises UpdateItem, BatchWriteItem, TransactWriteItems, a document-path
//!   projection, and a `KEYS_ONLY` GSI projection.
//!
//! Real time/sockets (the ProdEnv edge), so we poll with generous timeouts.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{ClusterConfig, Node, RoleAddrs, bind_cluster, start_cluster};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

/// One DynamoDB request over a fresh HTTP/1.1 connection → `(status, body)`.
async fn dynamo(addr: SocketAddr, target: &str, body: &str) -> (u16, String) {
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

async fn await_cluster_bootstrap(nodes: &[Node]) {
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
        .expect("cluster did not bootstrap within 20s");
}

async fn await_node_bootstrap(node: &Node) {
    let ready = async {
        loop {
            if node.is_control_leader() && !node.metadata().tablets.is_empty() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), ready)
        .await
        .expect("node did not bootstrap within 20s");
}

fn fixed_addrs(count: usize) -> Vec<SocketAddr> {
    let listeners: Vec<std::net::TcpListener> = (0..count)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    listeners.iter().map(|l| l.local_addr().unwrap()).collect()
}

fn single_node_config() -> ClusterConfig {
    let a = fixed_addrs(6);
    ClusterConfig {
        nodes: vec![RoleAddrs {
            control: a[0],
            data: a[1],
            coord: a[2],
            client: a[3],
            dynamo: a[4],
            cql: a[5],
        }],
        r: 1,
        w: 1,
    }
}

async fn stop(node: Node) {
    node.shutdown();
    drop(node);
    sleep(Duration::from_millis(200)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_table_survives_node_restart() {
    let config = single_node_config();
    let dynamo_addr = config.nodes[0].dynamo;
    let dir = tempfile::tempdir().unwrap();
    let node_dir = dir.path().join("node-0");

    // --- First incarnation: create a composite table, write + read an item. ---
    let node = animusd::run_node(&config, 0, &node_dir)
        .await
        .expect("first start");
    await_node_bootstrap(&node).await;

    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"events",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},
                                    {"AttributeName":"sk","AttributeType":"S"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    let (status, _) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"events","Item":{"pk":{"S":"u1"},"sk":{"S":"a"},"v":{"N":"1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);

    stop(node).await;

    // --- Second incarnation: SAME dir + addresses. The schema must survive, so a
    // bare PutItem (no re-CreateTable) resolves the composite key correctly and
    // a re-CreateTable is rejected as already existing. ---
    let node = animusd::run_node(&config, 0, &node_dir)
        .await
        .expect("restart");
    await_node_bootstrap(&node).await;

    // Re-creating the surviving table is rejected (ResourceInUseException).
    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"events",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}]}"#,
    )
    .await;
    assert_eq!(status, 400, "re-create should be rejected: {body}");
    assert!(
        body.contains("ResourceInUseException"),
        "expected ResourceInUseException, got: {body}"
    );

    // The previously-written item is still readable using the surviving composite
    // schema (its data rode the durable LSM; the schema rode the Raft WAL).
    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.GetItem",
        r#"{"TableName":"events","Key":{"pk":{"S":"u1"},"sk":{"S":"a"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "GetItem failed: {body}");
    assert!(body.contains(r#""v":{"N":"1"}"#), "item missing: {body}");

    stop(node).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extended_surface() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound, 2, 2).await.unwrap();
    await_cluster_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    // A simple table with a KEYS_ONLY GSI on `email`.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"users",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-email",
                 "KeySchema":[{"AttributeName":"email","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"KEYS_ONLY"}}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    // PutItem with a nested map attribute (for the document-path projection).
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"users","Item":{"id":{"S":"u1"},"email":{"S":"a@x"},
            "secret":{"S":"hush"},
            "profile":{"M":{"city":{"S":"Paris"},"zip":{"S":"75001"}}}}}"#,
    )
    .await;
    assert_eq!(status, 200, "PutItem failed: {body}");

    // UpdateItem: SET a new attr + REMOVE the secret, return ALL_NEW.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.UpdateItem",
        r#"{"TableName":"users","Key":{"id":{"S":"u1"}},
            "UpdateExpression":"SET age = :a REMOVE secret",
            "ExpressionAttributeValues":{":a":{"N":"30"}},
            "ReturnValues":"ALL_NEW"}"#,
    )
    .await;
    assert_eq!(status, 200, "UpdateItem failed: {body}");
    assert!(body.contains(r#""age":{"N":"30"}"#), "age not set: {body}");
    assert!(!body.contains("\"secret\""), "secret not removed: {body}");

    // Document-path projection: only `profile.city`.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.GetItem",
        r#"{"TableName":"users","Key":{"id":{"S":"u1"}},
            "ProjectionExpression":"profile.city"}"#,
    )
    .await;
    assert_eq!(status, 200, "GetItem failed: {body}");
    assert!(body.contains("Paris"), "city missing: {body}");
    assert!(
        !body.contains("75001"),
        "zip should be projected out: {body}"
    );
    assert!(
        !body.contains("\"age\""),
        "age should be projected out: {body}"
    );

    // KEYS_ONLY GSI query returns only the key attributes (id + email), not the
    // base item's other attributes (age/profile).
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"users","IndexName":"by-email",
            "KeyConditionExpression":"email = :e",
            "ExpressionAttributeValues":{":e":{"S":"a@x"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "index Query failed: {body}");
    assert!(body.contains("\"Count\":1"), "expected one match: {body}");
    assert!(body.contains(r#""id":{"S":"u1"}"#), "id missing: {body}");
    assert!(
        body.contains(r#""email":{"S":"a@x"}"#),
        "email missing: {body}"
    );
    assert!(!body.contains("\"age\""), "KEYS_ONLY leaked age: {body}");
    assert!(
        !body.contains("\"profile\""),
        "KEYS_ONLY leaked profile: {body}"
    );

    // BatchWriteItem: put two more users + delete u1.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.BatchWriteItem",
        r#"{"RequestItems":{"users":[
            {"PutRequest":{"Item":{"id":{"S":"u2"},"email":{"S":"b@x"}}}},
            {"PutRequest":{"Item":{"id":{"S":"u3"},"email":{"S":"c@x"}}}},
            {"DeleteRequest":{"Key":{"id":{"S":"u1"}}}}]}}"#,
    )
    .await;
    assert_eq!(status, 200, "BatchWriteItem failed: {body}");
    // u1 is now gone.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.GetItem",
        r#"{"TableName":"users","Key":{"id":{"S":"u1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body, "{}", "u1 should be deleted: {body}");

    // TransactWriteItems: a ConditionCheck that u2 exists + a conditional Put of
    // u4 only if absent. Both hold, so it succeeds.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.TransactWriteItems",
        r#"{"TransactItems":[
            {"ConditionCheck":{"TableName":"users","Key":{"id":{"S":"u2"}},
                               "ConditionExpression":"attribute_exists(id)"}},
            {"Put":{"TableName":"users","Item":{"id":{"S":"u4"},"email":{"S":"d@x"}},
                    "ConditionExpression":"attribute_not_exists(id)"}}]}"#,
    )
    .await;
    assert_eq!(status, 200, "TransactWriteItems failed: {body}");
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.GetItem",
        r#"{"TableName":"users","Key":{"id":{"S":"u4"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        body.contains(r#""email":{"S":"d@x"}"#),
        "u4 not written: {body}"
    );

    // A failing transaction condition rejects the whole request.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.TransactWriteItems",
        r#"{"TransactItems":[
            {"ConditionCheck":{"TableName":"users","Key":{"id":{"S":"nope"}},
                               "ConditionExpression":"attribute_exists(id)"}}]}"#,
    )
    .await;
    assert_eq!(status, 400, "expected condition failure: {body}");
    assert!(
        body.contains("ConditionalCheckFailedException"),
        "expected ConditionalCheckFailedException, got: {body}"
    );
}
