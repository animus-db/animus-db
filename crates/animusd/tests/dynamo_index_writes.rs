//! Every DynamoDB write op maintains a table's secondary indexes (ADR 0041
//! §2/§4, the 2026-08-13 write-coverage fix). `PutItem`/`DeleteItem` already
//! went through `index_aware_write`; this file covers the three ops that
//! didn't:
//!
//! - `update_item_and_batch_write_item_maintain_secondary_indexes` —
//!   `UpdateItem` setting a not-yet-indexed attribute, `UpdateItem` moving an
//!   already-indexed one, and `BatchWriteItem` puts + a delete, all against
//!   one table with both an LSI and a GSI.
//! - `transact_write_items_rejected_on_indexed_table` — `TransactWriteItems`
//!   is the one op that still can't participate: a write action against an
//!   indexed table is rejected wholesale (nothing commits), while a
//!   `ConditionCheck`-only action against one, alongside a genuine write on
//!   an unindexed table, still succeeds.
//!
//! Mirrors `dynamo_documents.rs`/`dynamo_gsi_drain.rs`: a 3-node in-process
//! cluster driven by the actual DynamoDB JSON protocol over hand-written
//! HTTP/1.1. An LSI is written atomically with the base row, so its
//! assertions are plain immediate checks; a GSI is materialized
//! **asynchronously** by the drain (DynamoDB's own contract), so every GSI
//! assertion here is a converged-or-timeout poll, never a fixed sleep
//! followed by a one-shot check.

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

/// One DynamoDB JSON request over a fresh HTTP/1.1 connection → `(status,
/// body)`. `Connection: close` is load-bearing: this helper reads to EOF, and
/// a keep-alive response would never finish reading.
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

/// Poll a `Query` (base or index) until `accept` is satisfied, returning the
/// last body observed. A GSI is materialized **asynchronously** by the drain
/// (ADR 0041 §4/§5) — DynamoDB's own eventually-consistent contract — so
/// every GSI assertion must be a converged-or-timeout poll.
async fn await_query(addr: SocketAddr, body: &str, accept: impl Fn(&str) -> bool) -> String {
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
    match timeout(Duration::from_secs(30), converged).await {
        Ok(body) => body,
        Err(_) => panic!(
            "query never converged within 30s (last saw: {})",
            last.lock().unwrap()
        ),
    }
}

/// `UpdateItem` (setting a not-yet-indexed attribute, then moving an
/// already-indexed one) and `BatchWriteItem` (puts + a delete) all maintain a
/// table's LSI rows and GSI change-log record — the ADR 0041 gap this PR
/// closes for both ops.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_item_and_batch_write_item_maintain_secondary_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    // A composite (id, sk) table with one LSI (alternate sort attribute
    // `alt`, within the base partition — written atomically with the base
    // row) and one GSI (hash on `tag`, `ALL` projection — materialized
    // asynchronously by the drain).
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"items",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-tag",
                 "KeySchema":[{"AttributeName":"tag","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"ALL"}}],
            "LocalSecondaryIndexes":[
                {"IndexName":"by-alt",
                 "KeySchema":[{"AttributeName":"id","KeyType":"HASH"},
                              {"AttributeName":"alt","KeyType":"RANGE"}]}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    // p1 starts with no `alt` attribute at all — no LSI row yet.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"items","Item":{"id":{"S":"p1"},"sk":{"S":"a"},"tag":{"S":"red"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "PutItem(p1) failed: {body}");
    await_query(
        addr,
        r#"{"TableName":"items","IndexName":"by-tag",
            "KeyConditionExpression":"tag = :t",
            "ExpressionAttributeValues":{":t":{"S":"red"}}}"#,
        |b| b.contains("\"Count\":1"),
    )
    .await;

    // --- (a) UpdateItem sets the LSI's alternate sort attribute for the
    // first time. The LSI row is written atomically with the base row, so a
    // Query against it is a plain immediate assertion (no polling); the GSI
    // change record it also produces converges to the same new image.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.UpdateItem",
        r#"{"TableName":"items","Key":{"id":{"S":"p1"},"sk":{"S":"a"}},
            "UpdateExpression":"SET alt = :v",
            "ExpressionAttributeValues":{":v":{"S":"mid"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "UpdateItem(set alt) failed: {body}");

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"items","IndexName":"by-alt",
            "KeyConditionExpression":"id = :i AND alt = :a",
            "ExpressionAttributeValues":{":i":{"S":"p1"},":a":{"S":"mid"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "LSI query failed: {body}");
    assert!(
        body.contains("\"Count\":1"),
        "LSI row for alt=mid missing immediately after UpdateItem: {body}"
    );

    await_query(
        addr,
        r#"{"TableName":"items","IndexName":"by-tag",
            "KeyConditionExpression":"tag = :t",
            "ExpressionAttributeValues":{":t":{"S":"red"}}}"#,
        |b| b.contains(r#""alt":{"S":"mid"}"#),
    )
    .await;

    // --- (b) UpdateItem CHANGES the already-indexed attribute. The LSI row
    // must MOVE: the old sort value's query no longer returns it, the new
    // one does — both immediate. The GSI converges to the same move.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.UpdateItem",
        r#"{"TableName":"items","Key":{"id":{"S":"p1"},"sk":{"S":"a"}},
            "UpdateExpression":"SET alt = :v",
            "ExpressionAttributeValues":{":v":{"S":"high"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "UpdateItem(move alt) failed: {body}");

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"items","IndexName":"by-alt",
            "KeyConditionExpression":"id = :i AND alt = :a",
            "ExpressionAttributeValues":{":i":{"S":"p1"},":a":{"S":"mid"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        body.contains("\"Count\":0"),
        "stale LSI row at alt=mid must be gone immediately: {body}"
    );
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"items","IndexName":"by-alt",
            "KeyConditionExpression":"id = :i AND alt = :a",
            "ExpressionAttributeValues":{":i":{"S":"p1"},":a":{"S":"high"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        body.contains("\"Count\":1"),
        "moved LSI row at alt=high missing immediately: {body}"
    );

    await_query(
        addr,
        r#"{"TableName":"items","IndexName":"by-tag",
            "KeyConditionExpression":"tag = :t",
            "ExpressionAttributeValues":{":t":{"S":"red"}}}"#,
        |b| b.contains(r#""alt":{"S":"high"}"#),
    )
    .await;

    // --- (c) BatchWriteItem: two puts (each with an `alt`) and a delete of
    // p1 on this same indexed table. LSI effects are immediate — new rows
    // appear, and p1's row is gone, in the very same batch entry as the
    // tombstone; the GSI converges to the puts and to pruning p1's row.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.BatchWriteItem",
        r#"{"RequestItems":{"items":[
            {"PutRequest":{"Item":{"id":{"S":"p2"},"sk":{"S":"b"},"tag":{"S":"blue"},"alt":{"S":"1"}}}},
            {"PutRequest":{"Item":{"id":{"S":"p3"},"sk":{"S":"c"},"tag":{"S":"blue"},"alt":{"S":"2"}}}},
            {"DeleteRequest":{"Key":{"id":{"S":"p1"},"sk":{"S":"a"}}}}]}}"#,
    )
    .await;
    assert_eq!(status, 200, "BatchWriteItem failed: {body}");

    // LSI: p2/p3's new rows exist immediately, and p1's old row is gone.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"items","IndexName":"by-alt",
            "KeyConditionExpression":"id = :i AND alt = :a",
            "ExpressionAttributeValues":{":i":{"S":"p2"},":a":{"S":"1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert!(body.contains("\"Count\":1"), "p2's LSI row missing: {body}");
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"items","IndexName":"by-alt",
            "KeyConditionExpression":"id = :i AND alt = :a",
            "ExpressionAttributeValues":{":i":{"S":"p3"},":a":{"S":"2"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert!(body.contains("\"Count\":1"), "p3's LSI row missing: {body}");
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"items","IndexName":"by-alt",
            "KeyConditionExpression":"id = :i",
            "ExpressionAttributeValues":{":i":{"S":"p1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        body.contains("\"Count\":0"),
        "p1's LSI row must be gone immediately after the batch delete: {body}"
    );

    // GSI: converges to the two new blue items, and to pruning p1's red row.
    await_query(
        addr,
        r#"{"TableName":"items","IndexName":"by-tag",
            "KeyConditionExpression":"tag = :t",
            "ExpressionAttributeValues":{":t":{"S":"blue"}}}"#,
        |b| b.contains("\"Count\":2"),
    )
    .await;
    await_query(
        addr,
        r#"{"TableName":"items","IndexName":"by-tag",
            "KeyConditionExpression":"tag = :t",
            "ExpressionAttributeValues":{":t":{"S":"red"}}}"#,
        |b| b.contains("\"Count\":0"),
    )
    .await;

    // The base table itself reflects the batch too (unaffected sanity check).
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.GetItem",
        r#"{"TableName":"items","Key":{"id":{"S":"p1"},"sk":{"S":"a"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body, "{}", "p1 should be deleted: {body}");
}

/// `TransactWriteItems` is the one op ADR 0041's write-coverage fix does
/// **not** extend to maintaining indexes: `cp_txn`'s `KvCommand::TxnStage` has
/// no multi-kind-write extension yet, so staging just the base row inside a
/// transaction would leave an indexed table's LSI rows / GSI change records
/// permanently stale with no signal. Instead, a transaction with any
/// `Put`/`Delete`/`Update` action against a table that has at least one
/// secondary index is rejected wholesale, before anything commits. A
/// `ConditionCheck`-only action against an indexed table doesn't count (it
/// writes nothing) — paired with a genuine write on an unindexed table, the
/// transaction still succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transact_write_items_rejected_on_indexed_table() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"indexed",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-email",
                 "KeySchema":[{"AttributeName":"email","KeyType":"HASH"}]}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable(indexed) failed: {body}");
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"plain","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable(plain) failed: {body}");

    // A Put on the indexed table is rejected outright — nothing committed.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.TransactWriteItems",
        r#"{"TransactItems":[
            {"Put":{"TableName":"indexed","Item":{"id":{"S":"x1"},"email":{"S":"a@x"}}}}]}"#,
    )
    .await;
    assert_eq!(status, 400, "expected rejection: {body}");
    assert!(
        body.contains("ValidationException"),
        "expected ValidationException, got: {body}"
    );
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.GetItem",
        r#"{"TableName":"indexed","Key":{"id":{"S":"x1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body, "{}", "x1 must not have committed: {body}");

    // A Delete on the indexed table is likewise rejected.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"indexed","Item":{"id":{"S":"x2"},"email":{"S":"b@x"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "seed PutItem(x2) failed: {body}");
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.TransactWriteItems",
        r#"{"TransactItems":[
            {"Delete":{"TableName":"indexed","Key":{"id":{"S":"x2"}}}}]}"#,
    )
    .await;
    assert_eq!(status, 400, "expected rejection: {body}");
    assert!(
        body.contains("ValidationException"),
        "expected ValidationException, got: {body}"
    );
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.GetItem",
        r#"{"TableName":"indexed","Key":{"id":{"S":"x2"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        body.contains(r#""email":{"S":"b@x"}"#),
        "x2 must still exist, the rejected Delete never committed: {body}"
    );

    // A bare ConditionCheck against the indexed table, alongside a genuine
    // write on an unindexed one, still succeeds — only a *write* on an
    // indexed table is rejected.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.TransactWriteItems",
        r#"{"TransactItems":[
            {"ConditionCheck":{"TableName":"indexed","Key":{"id":{"S":"x2"}},
                               "ConditionExpression":"attribute_exists(id)"}},
            {"Put":{"TableName":"plain","Item":{"id":{"S":"p1"}}}}]}"#,
    )
    .await;
    assert_eq!(
        status, 200,
        "ConditionCheck-only on indexed table should succeed: {body}"
    );
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.GetItem",
        r#"{"TableName":"plain","Key":{"id":{"S":"p1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert!(!body.is_empty() && body != "{}", "p1 not written: {body}");
}
