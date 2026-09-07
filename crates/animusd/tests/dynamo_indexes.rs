//! End-to-end test of the DynamoDB global-secondary-index surface over the
//! real TCP/HTTP wire: `CreateTable` with a declared GSI, write-then-query,
//! delete-removes-from-index, and querying an undeclared index.
//!
//! **ADR 0061 rung D3 PR 3a moved this file's other two tests to
//! `SimCluster`** (`crates/animusd/src/sim_cluster_dynamo_indexes.rs`) —
//! `scan_paginates_a_whole_table`/`scan_skips_deleted_items_and_paginates`,
//! both base-table `Scan` with no index at all. **`gsi_write_then_query`
//! stays here, deliberately never converted**: ADR 0061 rung D2 PR 1 names
//! it as the real-socket proof that `run_operation`'s own dispatch works
//! independently of `dynamo::dispatch_item_op` — and it also reads a
//! materialized GSI row, which `SimCluster` cannot produce anyway (no
//! `index_drain::change_consumer_loop`).
//!
//! Real time/sockets, so this assertion polls with generous timeouts.

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

/// Poll a GSI `Query` until `accept` is satisfied, returning the last body
/// observed. A GSI is materialized **asynchronously** by the drain (ADR 0041
/// §4/§5) — DynamoDB's own eventually-consistent contract — so every
/// assertion against one must be a converged-or-timeout poll, never a fixed
/// sleep followed by a one-shot check.
async fn await_gsi_query(
    addr: std::net::SocketAddr,
    body: &str,
    accept: impl Fn(&str) -> bool,
) -> String {
    let last = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let seen = std::sync::Arc::clone(&last);
    let converged = async move {
        loop {
            let (status, got) = dynamo(addr, "DynamoDB_20120810.Query", body).await;
            if status == 200 && accept(&got) {
                return got;
            }
            *seen.lock().unwrap() = got;
            sleep(Duration::from_millis(100)).await;
        }
    };
    match timeout(Duration::from_secs(15), converged).await {
        Ok(body) => body,
        Err(_) => panic!(
            "GSI query never converged within 15s (last saw: {})",
            last.lock().unwrap()
        ),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gsi_write_then_query() {
    let dir = support::panic_safe_tempdir();
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
        r#"{"TableName":"users","AttributeDefinitions":[{"AttributeName":"email","AttributeType":"S"},{"AttributeName":"id","AttributeType":"S"}],
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

    // Query the GSI for a@x (from a different node → native scan of the hidden
    // index table): u1 and u3. A GSI is materialized **asynchronously** by the
    // drain (ADR 0041 §4/§5), so this is a converged-or-timeout poll, never a
    // fixed sleep + one-shot assert.
    let body = await_gsi_query(
        addr1,
        r#"{"TableName":"users","IndexName":"by-email",
            "KeyConditionExpression":"email = :e",
            "ExpressionAttributeValues":{":e":{"S":"a@x"}}}"#,
        |b| {
            b.contains("\"Count\":2")
                && b.contains(r#""id":{"S":"u1"}"#)
                && b.contains(r#""id":{"S":"u3"}"#)
        },
    )
    .await;
    assert!(!body.contains(r#""id":{"S":"u2"}"#), "got: {body}");

    // Deleting u3 removes it from the index.
    let (status, _) = dynamo(
        addr0,
        "DynamoDB_20120810.DeleteItem",
        r#"{"ConsistentRead":true,"TableName":"users","Key":{"id":{"S":"u3"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    let body = await_gsi_query(
        addr1,
        r#"{"TableName":"users","IndexName":"by-email",
            "KeyConditionExpression":"email = :e",
            "ExpressionAttributeValues":{":e":{"S":"a@x"}}}"#,
        |b| b.contains("\"Count\":1") && b.contains(r#""id":{"S":"u1"}"#),
    )
    .await;
    assert!(!body.contains(r#""id":{"S":"u3"}"#), "after delete: {body}");

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
