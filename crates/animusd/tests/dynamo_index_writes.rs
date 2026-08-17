//! Every DynamoDB write op maintains a table's secondary indexes (ADR 0041
//! §2/§4, the 2026-08-13 write-coverage fix). `PutItem`/`DeleteItem` already
//! went through `index_aware_write`; this file covers the three ops that
//! didn't:
//!
//! - `update_item_and_batch_write_item_maintain_secondary_indexes` —
//!   `UpdateItem` setting a not-yet-indexed attribute, `UpdateItem` moving an
//!   already-indexed one, and `BatchWriteItem` puts + a delete, all against
//!   one table with both an LSI and a GSI.
//! - `transact_write_items_maintains_lsi_and_gsi_across_a_split_table` /
//!   `transact_write_items_abort_leaves_no_lsi_row_and_no_gsi_row` —
//!   `TransactWriteItems` now maintains indexes atomically too (ADR 0046
//!   A1/U3, `TxnStage` kind-writes stack): a cross-tablet transaction's
//!   writes materialize their LSI rows and GSI change records exactly like
//!   a plain `PutItem`/`UpdateItem` would, and an aborted transaction
//!   leaves neither behind. Supersedes the old wholesale-rejection this
//!   file used to test (ADR 0041 §2/ADR 0042 §16's now-superseded
//!   TxnStage-has-no-multi-kind-write-extension rationale).
//! - `unconditional_put_and_delete_maintain_lsi_without_a_condition_or_all_old`
//!   — the old-image-starvation fix: `PutItem`/`DeleteItem` with **no**
//!   `ConditionExpression` and **no** `ReturnValues: ALL_OLD` must still fetch
//!   the prior item on an indexed table, because `kind_writes_for_item`'s own
//!   LSI diff needs it to clean up the stale row. Before the fix, `needs_old`
//!   was gated only on the condition/`ALL_OLD` check, so an unconditional
//!   replace/delete silently left the old LSI row behind.
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

use animus_dynamo::{AttributeValue, storage_key};
use animus_tablet::partition_token;
use animusd::{Node, bind_cluster, start_cluster};
use serde_json::Value;
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

/// **`TransactWriteItems` now maintains indexes atomically (ADR 0046 A1/U3,
/// `TxnStage` kind-writes stack)** — the rejection this test used to prove
/// is gone: `TxnStage` stages a write's derived LSI rows/change record
/// inside its own intent, materialized by `TxnResolve` at resolve, and each
/// write action's kind payload is evaluated at ITS OWN tablet's leader
/// (never precomputed by the coordinator). Splits the table so the two
/// items genuinely land on **different tablets**, proving the mechanism
/// composes with the 2PC's own cross-tablet atomicity, not just within one
/// group.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transact_write_items_maintains_lsi_and_gsi_across_a_split_table() {
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
    assert_eq!(status, 200, "CreateTable(indexed) failed: {body}");

    // Wait for the table's bootstrap tablet — `CreateTable` provisions it.
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes.iter().any(|n| {
                n.metadata()
                    .tablets
                    .contains_key(&animus_tablet::TabletId(1))
            }) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("bootstrap tablet was never provisioned");

    // Split strictly between "p1" and "p2"'s own tokens (mirrors
    // `dynamo_index_scan.rs`'s identical split-key construction) so the
    // transaction's two items genuinely land on different tablets.
    //
    // ADR 0050 (Train B rung 1): the client-facing split surface is
    // disabled during the storage pivot; propose the metadata command
    // directly. Sound ONLY because the table is still EMPTY — this gives
    // the transaction a genuine two-group topology, it does not exercise
    // split itself (see cp_txn.rs's split_and_settle for the full note).
    let token_p1 = partition_token(&storage_key(&AttributeValue::S("p1".into()), None));
    let token_p2 = partition_token(&storage_key(&AttributeValue::S("p2".into()), None));
    let split_key = token_p1.max(token_p2).to_vec();
    let meta = nodes
        .iter()
        .map(|n| n.metadata())
        .find(|m| m.tablets.contains_key(&animus_tablet::TabletId(1)))
        .expect("just observed");
    let source = animus_tablet::TabletId(1);
    let cmd = animus_control::MetaCommand::SplitTablet {
        tablet: source,
        expected_epoch: meta.tablets[&source].epoch,
        split_key,
        new_id: meta.next_free_tablet_id(),
    };
    let accepted = nodes.iter().any(|n| n.propose_meta(cmd.clone()));
    assert!(
        accepted,
        "no node's control handle accepted the harness split proposal"
    );
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes[0].metadata().tablets.len() >= 2 {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("split was not recorded within 20s");

    // The transaction: one Put per partition, each setting both the LSI's
    // `alt` sort attribute and the GSI's `tag` hash attribute.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.TransactWriteItems",
        r#"{"TransactItems":[
            {"Put":{"TableName":"indexed",
                    "Item":{"id":{"S":"p1"},"sk":{"S":"a"},
                            "alt":{"S":"mid"},"tag":{"S":"red"}}}},
            {"Put":{"TableName":"indexed",
                    "Item":{"id":{"S":"p2"},"sk":{"S":"a"},
                            "alt":{"S":"high"},"tag":{"S":"blue"}}}}]}"#,
    )
    .await;
    assert_eq!(
        status, 200,
        "cross-tablet indexed transaction failed: {body}"
    );

    // Both base rows visible immediately (the anchor's ack already implies
    // both committed — 2PC atomicity).
    for (id, alt) in [("p1", "mid"), ("p2", "high")] {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.GetItem",
            &format!(r#"{{"TableName":"indexed","Key":{{"id":{{"S":"{id}"}},"sk":{{"S":"a"}}}}}}"#),
        )
        .await;
        assert_eq!(status, 200);
        assert!(
            body.contains(&format!(r#""alt":{{"S":"{alt}"}}"#)),
            "{id}'s base row missing/wrong after the transaction: {body}"
        );
    }

    // Both LSI rows correct — same-entry-synchronous, so a plain immediate
    // assertion (ADR 0046's "what this model does not change": LSI stays
    // strongly consistent).
    for (id, alt) in [("p1", "mid"), ("p2", "high")] {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.Query",
            &format!(
                r#"{{"TableName":"indexed","IndexName":"by-alt",
                    "KeyConditionExpression":"id = :i AND alt = :a",
                    "ExpressionAttributeValues":{{":i":{{"S":"{id}"}},":a":{{"S":"{alt}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "LSI query failed for {id}: {body}");
        assert!(
            body.contains("\"Count\":1"),
            "LSI row for {id}/alt={alt} missing immediately after the transaction: {body}"
        );
    }

    // Both change records land — awaited-resolve (ADR 0046 D1) means this
    // should already be true, but the GSI drain is itself asynchronous
    // (DynamoDB's own eventually-consistent contract), so poll.
    await_query(
        addr,
        r#"{"TableName":"indexed","IndexName":"by-tag",
            "KeyConditionExpression":"tag = :t",
            "ExpressionAttributeValues":{":t":{"S":"red"}}}"#,
        |b| b.contains("\"Count\":1"),
    )
    .await;
    await_query(
        addr,
        r#"{"TableName":"indexed","IndexName":"by-tag",
            "KeyConditionExpression":"tag = :t",
            "ExpressionAttributeValues":{":t":{"S":"blue"}}}"#,
        |b| b.contains("\"Count\":1"),
    )
    .await;
}

/// **Abort case**: a `TransactWriteItems` that fails a `ConditionCheck`
/// leaves no trace on an indexed table — no base row, no LSI row, no GSI
/// change record ever materialized. Proves ADR 0046 A1's "abort discards
/// the kind-writes payload entirely" at the wire level.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transact_write_items_abort_leaves_no_lsi_row_and_no_gsi_row() {
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
                {"IndexName":"by-tag",
                 "KeySchema":[{"AttributeName":"tag","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"ALL"}}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    // The ConditionCheck targets a key that does not exist — fails
    // `attribute_exists`, so the whole transaction (including the Put on
    // the indexed table) must abort before anything commits.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.TransactWriteItems",
        r#"{"TransactItems":[
            {"ConditionCheck":{"TableName":"indexed","Key":{"id":{"S":"missing"}},
                               "ConditionExpression":"attribute_exists(id)"}},
            {"Put":{"TableName":"indexed","Item":{"id":{"S":"x1"},"tag":{"S":"red"}}}}]}"#,
    )
    .await;
    assert_eq!(status, 400, "expected TransactionCanceledException: {body}");
    assert!(
        body.contains("TransactionCanceledException"),
        "expected TransactionCanceledException, got: {body}"
    );

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.GetItem",
        r#"{"TableName":"indexed","Key":{"id":{"S":"x1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body, "{}", "x1 must not have committed: {body}");

    // No GSI row either — give the drain a real window to have (wrongly)
    // materialized one before asserting its absence, rather than winning on
    // a race.
    sleep(Duration::from_secs(2)).await;
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"indexed","IndexName":"by-tag",
            "KeyConditionExpression":"tag = :t",
            "ExpressionAttributeValues":{":t":{"S":"red"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        body.contains("\"Count\":0"),
        "aborted transaction must never materialize a GSI row: {body}"
    );
}

/// The old-image-starvation fix: an **unconditional** `PutItem` (no
/// `ConditionExpression`, no `ReturnValues: ALL_OLD`) that replaces an item's
/// indexed alt-sort attribute must still move its LSI row, and an
/// **unconditional** `DeleteItem` must still remove it. Before the fix,
/// `needs_old` was computed from `condition.is_some() ||
/// return_values == ReturnValues::AllOld` alone — neither is true here, so the
/// prior item was never read, `kind_writes_for_item`'s LSI diff saw `old_alt
/// == None`, and the stale LSI row was left behind (query-visible: the old
/// alt value's row never disappears, and the base table quietly diverges from
/// its own LSI forever).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unconditional_put_and_delete_maintain_lsi_without_a_condition_or_all_old() {
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
        r#"{"TableName":"items",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "LocalSecondaryIndexes":[
                {"IndexName":"by-alt",
                 "KeySchema":[{"AttributeName":"id","KeyType":"HASH"},
                              {"AttributeName":"alt","KeyType":"RANGE"}]}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    // p1 starts at alt=A — its LSI row is immediate (written atomically with
    // the base row).
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"items","Item":{"id":{"S":"p1"},"sk":{"S":"a"},"alt":{"S":"A"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "PutItem(p1, alt=A) failed: {body}");
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"items","IndexName":"by-alt",
            "KeyConditionExpression":"id = :i AND alt = :a",
            "ExpressionAttributeValues":{":i":{"S":"p1"},":a":{"S":"A"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        body.contains("\"Count\":1"),
        "LSI row for alt=A missing right after the first PutItem: {body}"
    );

    // --- An UNCONDITIONAL PutItem (no ConditionExpression, no ALL_OLD)
    // replaces p1 with alt=B. The stale alt=A row must be gone and the new
    // alt=B row must exist, both immediately (the LSI is written atomically
    // with the base row) — this is the exact case the old `needs_old` gate
    // silently skipped.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"items","Item":{"id":{"S":"p1"},"sk":{"S":"a"},"alt":{"S":"B"}}}"#,
    )
    .await;
    assert_eq!(
        status, 200,
        "unconditional PutItem(p1, alt=B) failed: {body}"
    );

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"items","IndexName":"by-alt",
            "KeyConditionExpression":"id = :i AND alt = :a",
            "ExpressionAttributeValues":{":i":{"S":"p1"},":a":{"S":"A"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        body.contains("\"Count\":0"),
        "stale LSI row at alt=A must be gone after an unconditional replace: {body}"
    );
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"items","IndexName":"by-alt",
            "KeyConditionExpression":"id = :i AND alt = :a",
            "ExpressionAttributeValues":{":i":{"S":"p1"},":a":{"S":"B"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        body.contains("\"Count\":1"),
        "new LSI row at alt=B missing after an unconditional replace: {body}"
    );

    // --- An UNCONDITIONAL DeleteItem (no ConditionExpression, no ALL_OLD)
    // removes p1. Its LSI row (alt=B) must be gone immediately too — the
    // identical gap in `DeleteItem`'s own `needs_old`.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.DeleteItem",
        r#"{"TableName":"items","Key":{"id":{"S":"p1"},"sk":{"S":"a"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "unconditional DeleteItem(p1) failed: {body}");

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
        "p1's LSI row must be gone after an unconditional delete: {body}"
    );
}

/// One hammer loop's outcome: how many of its `iterations` unconditional
/// `PutItem`s the server acknowledged with `200`, and how many returned
/// something else. A non-200 is **tolerated**, never panicked on — on the
/// unfixed baseline under sustained cross-node contention a write's
/// client-visible outcome can be genuinely ambiguous (it may time out at
/// *confirm* while still committing moments later), so asserting on any
/// one write's own result would be asserting against an outcome that
/// isn't actually known (see `docs/engineering-lessons.md`). This loop's
/// only job is to keep pushing writes long enough for the race to have a
/// chance to fire; the final check below reads the cluster's own
/// **converged** state afterward and never consults this outcome
/// directly — `acked`/`failed` exist purely as run-health diagnostics (a
/// loop that acks almost nothing didn't meaningfully exercise anything).
struct HammerOutcome {
    acked: u32,
    failed: u32,
}

/// One loop's worth of unconditional `PutItem`s against the SAME item key
/// (`id`/`sk` fixed), each cycling a distinct `alt` value tagged with this
/// loop's own `tag` and an increasing counter — `dynamo_txn.rs`'s racing
/// pattern, generalized to a sustained hammer instead of one single-shot
/// race.
async fn hammer_puts(
    addr: SocketAddr,
    table: &str,
    id: &str,
    sk: &str,
    tag: &str,
    iterations: u32,
) -> HammerOutcome {
    let mut acked = 0;
    let mut failed = 0;
    for i in 0..iterations {
        let alt = format!("{tag}-{i:05}");
        let body = format!(
            r#"{{"TableName":"{table}","Item":{{"id":{{"S":"{id}"}},"sk":{{"S":"{sk}"}},"alt":{{"S":"{alt}"}}}}}}"#
        );
        let (status, _resp_body) = dynamo(addr, "DynamoDB_20120810.PutItem", &body).await;
        if status == 200 {
            acked += 1;
        } else {
            failed += 1;
        }
    }
    HammerOutcome { acked, failed }
}

/// A few sequential `PutItem`s from EACH node against a key the real race
/// below never touches (`id = "warmup"`, vs. the race's own `"shared"`),
/// each retried on a non-200 for up to 5s — settling the freshly created
/// table's tablet routing (its lone tablet's own Raft group has completed
/// leader election, and both edge nodes' outbound relay connections to
/// that leader are already warm) before the concurrent hammer starts.
///
/// **Diagnoses a real, but separate, t=0 provisioning/election race**:
/// without this, the very FIRST write of either hammer loop reliably (8/8
/// observed) failed outright on the unfixed baseline with a hard
/// `InternalServerError` ("relay to peer node failed" / "CP kind write did
/// not commit in time") — before the cross-node LSI-diff race this test
/// exists to provoke ever got a chance to fire. A brand-new tablet's own
/// Raft group has to complete its own (normally sub-second) leader
/// election, and every node's tablet-host reconciler has to observe it
/// should host/relay for that tablet, before either edge node's first
/// request can resolve a route — and `CreateTable`'s own success only
/// guarantees the *catalog* entry committed, not that every node has
/// already reconciled hosting for it. The unfixed baseline's
/// `index_aware_write` does two sequential forwarded hops per write (a
/// `cp_get` read, then a separate `cp_kind_write` propose) where the fixed
/// path does one (`KindWriteItem`), doubling this window's exposure — which
/// is why the fixed path can look like it "masks" the race rather than
/// truly closing it. This is plausibly reachable on `main` today for *any*
/// table (indexed or not) hit by two nodes' very first concurrent writes
/// immediately after `CreateTable`, not specific to the evaluate-at-leader
/// change this test's real assertion covers — noted here for a separate
/// report, deliberately not fixed in this stack.
async fn settle_tablet_routing(addr_a: SocketAddr, addr_b: SocketAddr, table: &str) {
    for addr in [addr_a, addr_b] {
        for i in 0..3 {
            let body = format!(
                r#"{{"TableName":"{table}","Item":{{"id":{{"S":"warmup"}},"sk":{{"S":"w"}},"alt":{{"S":"w{i}"}}}}}}"#
            );
            let mut ok = false;
            for _retry in 0..50 {
                let (status, _) = dynamo(addr, "DynamoDB_20120810.PutItem", &body).await;
                if status == 200 {
                    ok = true;
                    break;
                }
                sleep(Duration::from_millis(100)).await;
            }
            assert!(
                ok,
                "warm-up PutItem from {addr} never succeeded after 5s of retries — the cluster \
                 itself looks unhealthy, not just cold"
            );
        }
    }
}

/// The `alt` attribute's string value out of a `GetItem`/`Query` JSON body
/// (`{"Item": {"alt": {"S": "..."}, ...}}` or one entry of `{"Items": [{...}]}`).
fn item_alt(item: &Value) -> Option<String> {
    item.get("alt")?.get("S")?.as_str().map(str::to_owned)
}

/// Poll `GetItem` on `key_json` until its `alt` value reads identically on
/// three consecutive samples 150ms apart (a settle detector), then return
/// it. Required now that the hammer loops tolerate transient per-write
/// errors: a write whose *client* saw a confirm-poll timeout is not
/// guaranteed to have already lost its race with Raft — it can still land
/// after the loop that issued it has already returned. A single plain read
/// right after both loops finish (sound only when every issued write is
/// known to have already applied) is therefore not sound here; only a
/// genuine quiescence poll is.
async fn await_settled_alt(addr: SocketAddr, key_json: &str) -> String {
    let mut streak: u32 = 0;
    let mut last: Option<String> = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let (status, body) = dynamo(addr, "DynamoDB_20120810.GetItem", key_json).await;
        assert_eq!(status, 200, "GetItem failed while settling: {body}");
        let reply: Value = serde_json::from_str(&body).expect("GetItem reply is valid JSON");
        let alt = item_alt(reply.get("Item").expect("item must exist while settling"));
        if alt == last {
            streak += 1;
            if streak >= 3 {
                return alt.expect("item must carry an alt attribute");
            }
        } else {
            streak = 0;
            last = alt;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("base item's alt never settled within 20s (last saw {last:?})");
        }
        sleep(Duration::from_millis(150)).await;
    }
}

/// **ADR 0046 U3 red→green repro**: the cross-node LSI orphan race
/// `index_aware_write`'s edge-evaluated design had. Two unsynchronized
/// loops hammer unconditional `PutItem`s against the SAME item key, one
/// through `nodes[0]`'s DynamoDB endpoint, the other through `nodes[1]`'s —
/// each cycling its own tag's `alt` values, so whichever loop's write lands
/// last determines the base item's final `alt`. Before the fix: each edge
/// node's `index_aware_write` reads the prior item and diffs the LSI
/// **locally**, under a **node-local** `rmw_lock` — the two loops' writes
/// never contend on the same lock (they're on different nodes), so both can
/// read the same stale prior item and compute a diff against it, silently
/// producing an orphaned stale LSI row alongside the correct current one
/// (nothing reconciles a stale LSI row — only a GSI drain self-heals).
/// After the fix, every write of this item — from either node — evaluates
/// on the item's own tablet leader, serialized by that leader's own
/// `rmw_lock`, so no diff is ever computed against a value another node's
/// write has already superseded.
///
/// A one-time settling `settle_tablet_routing` call precedes the hammer (see
/// its own doc for the unrelated t=0 race it gets past), and each loop
/// tolerates its own per-write transient errors (`HammerOutcome`) rather
/// than panicking on one — both needed to reach the actual assertion below
/// on the unfixed baseline; see the module doc for the full red/green story.
/// `await_settled_alt` (converged-or-timeout, never a single plain read —
/// see its own doc) gets the base item's final value once both loops finish
/// (LSI writes are strongly consistent — same Raft entry as the base row —
/// so once the base item itself stops changing, its LSI row set is already
/// final too); then assert **exactly one** live LSI row for this partition,
/// and that its `alt` matches the base item's own current `alt` — an
/// orphan means either a second row (extra) or a mismatch (the surviving
/// row names a `alt` the base item no longer holds).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_node_racing_unconditional_puts_never_orphan_an_lsi_row() {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;
    let addr_a = nodes[0].dynamo_addr();
    let addr_b = nodes[1].dynamo_addr();
    let addr_c = nodes[2].dynamo_addr();

    let (status, body) = dynamo(
        addr_a,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"race",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "LocalSecondaryIndexes":[
                {"IndexName":"by-alt",
                 "KeySchema":[{"AttributeName":"id","KeyType":"HASH"},
                              {"AttributeName":"alt","KeyType":"RANGE"}]}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    // Get past the t=0 provisioning/election race before starting the real
    // hammer — see `settle_tablet_routing`'s own doc.
    settle_tablet_routing(addr_a, addr_b, "race").await;

    // Env-tunable so the red-run investigation can sweep iteration counts
    // without editing the source; defaults to the plan's own starting point.
    let iterations: u32 = std::env::var("RACE_ITERATIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500);

    let (outcome_a, outcome_b) = tokio::join!(
        hammer_puts(addr_a, "race", "shared", "x", "A", iterations),
        hammer_puts(addr_b, "race", "shared", "x", "B", iterations),
    );
    let total_acked = outcome_a.acked + outcome_b.acked;
    eprintln!(
        "hammer outcome: loop A {}/{iterations} acked ({} failed), loop B {}/{iterations} acked \
         ({} failed)",
        outcome_a.acked, outcome_a.failed, outcome_b.acked, outcome_b.failed
    );
    // A run health gate, not a correctness assertion: the final check below
    // reads converged state and doesn't care which individual writes were
    // acked — but a run where almost nothing landed didn't meaningfully
    // exercise cross-node contention at all, and would make "no orphan
    // found" a vacuous pass rather than real evidence.
    assert!(
        total_acked >= iterations,
        "too few writes acked ({total_acked} of {}) to exercise real cross-node contention — \
         the cluster looks unhealthy beyond ordinary transient contention",
        2 * iterations
    );

    // Converged-or-timeout poll from a THIRD node (never touched by either
    // loop) — required now that each loop tolerates its own transient
    // errors; see `await_settled_alt`'s own doc for why a single plain read
    // is no longer sound here.
    let base_alt = await_settled_alt(
        addr_c,
        r#"{"TableName":"race","Key":{"id":{"S":"shared"},"sk":{"S":"x"}}}"#,
    )
    .await;

    // The whole-partition LSI scan (no `alt` filter — see `by-alt` Queries
    // elsewhere in this file for the identical shape) is the orphan
    // detector: more than one row means a stale diff computed against an
    // already-superseded value left its old row behind.
    let (status, body) = dynamo(
        addr_c,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"race","IndexName":"by-alt",
            "KeyConditionExpression":"id = :i",
            "ExpressionAttributeValues":{":i":{"S":"shared"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "LSI query failed: {body}");
    let query_reply: Value = serde_json::from_str(&body).expect("Query reply is valid JSON");
    let items = query_reply["Items"]
        .as_array()
        .expect("Items is an array")
        .clone();
    assert_eq!(
        items.len(),
        1,
        "expected exactly one live LSI row for partition `shared`, got {} \
         (an orphan stale row from a cross-node racing diff) — base item alt={base_alt}, \
         rows={items:?}",
        items.len()
    );
    let row_alt = item_alt(&items[0]).expect("LSI row must carry alt");
    assert_eq!(
        row_alt, base_alt,
        "the one surviving LSI row's alt ({row_alt}) must match the base item's own current \
         alt ({base_alt}) — a mismatch means the surviving row is itself stale"
    );
}
