//! **Per-action `CancellationReasons` on `TransactionCanceledException`**
//! (ADR 0018's 2026-08-24 `CancellationReasons` amendment, issue #374 C2),
//! over `ProdEnv` — real HTTP against the DynamoDB wire edge, mirroring
//! `dynamo_txn.rs`'s harness style (its `bring_up`/`dynamo`/
//! `create_table_pre_split` idioms are copied here rather than shared,
//! matching that file's own precedent of not factoring per-suite harnesses
//! into `support`).
//!
//! C2a covers only `ConditionCheck` failures — the two coordinator-side
//! evaluation sites in `dynamo.rs::run_transact` that already know exactly
//! which action failed before ever calling `cp_txn` (the in-loop check
//! against a transaction that also has write actions, and the
//! all-`ConditionCheck` fallback that never calls `cp_txn` at all). A write
//! action's own condition/conflict, which crosses the `cp_txn` 2PC boundary,
//! is C2b's addition to this file.
//!
//! Real TCP/time → polls with generous timeouts, never a fixed sleep.

use std::net::SocketAddr;
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

// ---------------------------------------------------------------------------
// Shared bring-up + protocol helpers (mirrors dynamo_txn.rs).
// ---------------------------------------------------------------------------

async fn bring_up(n: usize, dir: &std::path::Path) -> (Vec<animusd::Node>, animusd::ClusterConfig) {
    for attempt in 0..16 {
        let addrs = support::free_addrs(n * 6);
        let nodes_cfg: Vec<animusd::RoleAddrs> = (0..n)
            .map(|i| animusd::RoleAddrs {
                id: animusd::config::node_id(i),
                role: animusd::config::NodeRole::Both,
                internal: addrs[6 * i],
                client: addrs[6 * i + 1],
                dynamo: addrs[6 * i + 2],
                admin: addrs[6 * i + 3],
                intra: addrs[6 * i + 4],
                console: addrs[6 * i + 5],
            })
            .collect();
        let config = animusd::ClusterConfig {
            nodes: nodes_cfg,
            dynamo_auth: None,
        };
        let mut nodes = Vec::new();
        let mut failed = false;
        for i in 0..n {
            match animusd::run_node(&config, i, dir.join(format!("node-{attempt}-{i}"))).await {
                Ok(node) => nodes.push(node),
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        if !failed {
            return (nodes, config);
        }
        for node in &nodes {
            node.shutdown_graceful().await;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("could not bring up cluster after retries (ports kept getting stolen)");
}

async fn await_bootstrap(nodes: &[animusd::Node]) {
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes.iter().any(animusd::Node::is_control_leader)
                && nodes.iter().all(|n| !n.metadata().members.is_empty())
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("cluster did not bootstrap in 20s");
}

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

/// `CreateTable` with a simple `id: S` partition key — no split. Every C2a
/// test here is single-tablet: a `ConditionCheck` failure is flagged by
/// `dynamo.rs::run_transact` itself, entirely before any cross-tablet
/// coordination, so a genuinely split table adds nothing these tests need
/// (unlike `dynamo_txn.rs`'s own atomicity suite, or this file's own C2b
/// additions below, which specifically need multiple tablets).
async fn create_table(dynamo_addr: SocketAddr, table: &str) {
    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.CreateTable",
        &format!(
            r#"{{"TableName":"{table}",
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
                "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}]}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "CreateTable({table}) failed: {body}");
}

/// Parse a `TransactionCanceledException` body's `CancellationReasons` array.
fn cancellation_reasons(body: &str) -> Vec<Value> {
    let json: Value =
        serde_json::from_str(body).unwrap_or_else(|e| panic!("body is not JSON ({e}): {body}"));
    assert_eq!(
        json["__type"], "com.amazonaws.dynamodb.v20120810#TransactionCanceledException",
        "expected TransactionCanceledException, got: {body}"
    );
    json["CancellationReasons"]
        .as_array()
        .unwrap_or_else(|| panic!("no CancellationReasons array in: {body}"))
        .clone()
}

// ---------------------------------------------------------------------------
// C2a: ConditionCheck failures (dynamo.rs::run_transact sites 1 and 2).
// ---------------------------------------------------------------------------

/// **Site 1** — a transaction with a write action AND `ConditionCheck`s: the
/// failing check is flagged at its own index, every other action (including
/// the unconditioned `Put`, which the coordinator never even evaluates a
/// condition for) reports `None`.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn condition_check_failure_flags_the_right_action_index() {
    let n = 3;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].dynamo;

    create_table(addr0, "cxl_a").await;

    // A guard item that DOES exist, so `attribute_not_exists(id)` fails.
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"cxl_a","Item":{"id":{"S":"guard"},"v":{"S":"present"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "seed put failed: {body}");

    // [0] Put (unconditioned, always applies in isolation)
    // [1] ConditionCheck on an absent key — passes
    // [2] ConditionCheck on "guard" — fails (guard exists)
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.TransactWriteItems",
        r#"{"TransactItems":[
            {"Put":{"TableName":"cxl_a","Item":{"id":{"S":"other"},"v":{"S":"x"}}}},
            {"ConditionCheck":{"TableName":"cxl_a","Key":{"id":{"S":"absent-key"}},
                               "ConditionExpression":"attribute_not_exists(id)"}},
            {"ConditionCheck":{"TableName":"cxl_a","Key":{"id":{"S":"guard"}},
                               "ConditionExpression":"attribute_not_exists(id)"}}]}"#,
    )
    .await;
    assert_eq!(status, 400, "expected the guard check to cancel: {body}");

    let reasons = cancellation_reasons(&body);
    assert_eq!(reasons.len(), 3, "one entry per action: {body}");
    assert_eq!(reasons[0]["Code"], "None");
    assert_eq!(reasons[0]["Message"], Value::Null);
    assert_eq!(reasons[1]["Code"], "None");
    assert_eq!(reasons[2]["Code"], "ConditionalCheckFailed");
    assert_eq!(reasons[2]["Message"], "The conditional request failed");
    assert!(
        reasons[2].get("Item").is_none(),
        "no ReturnValuesOnConditionCheckFailure requested ⇒ no Item: {body}"
    );
    assert_eq!(
        json_str(&body, "message"),
        "Transaction cancelled, please refer cancellation reasons for specific reasons \
         [None, None, ConditionalCheckFailed]"
    );

    // Whole-or-nothing: the unconditioned `Put` at index 0 must not have landed.
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.GetItem",
        r#"{"ConsistentRead":true,"TableName":"cxl_a","Key":{"id":{"S":"other"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        body, "{}",
        "cancelled transaction must not have written: {body}"
    );

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

/// **`Item` echo**: the same failing check, but requesting
/// `ReturnValuesOnConditionCheckFailure: "ALL_OLD"` — the failing entry's
/// `Item` field must echo the guard's current attributes.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn condition_check_failure_echoes_item_when_all_old_requested() {
    let n = 3;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].dynamo;

    create_table(addr0, "cxl_b").await;

    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"cxl_b","Item":{"id":{"S":"guard"},"v":{"N":"7"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "seed put failed: {body}");

    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.TransactWriteItems",
        r#"{"TransactItems":[
            {"Put":{"TableName":"cxl_b","Item":{"id":{"S":"other"},"v":{"S":"x"}}}},
            {"ConditionCheck":{"TableName":"cxl_b","Key":{"id":{"S":"guard"}},
                               "ConditionExpression":"attribute_not_exists(id)",
                               "ReturnValuesOnConditionCheckFailure":"ALL_OLD"}}]}"#,
    )
    .await;
    assert_eq!(status, 400, "expected the guard check to cancel: {body}");

    let reasons = cancellation_reasons(&body);
    assert_eq!(reasons.len(), 2);
    assert_eq!(reasons[0]["Code"], "None");
    assert_eq!(reasons[1]["Code"], "ConditionalCheckFailed");
    assert_eq!(
        reasons[1]["Item"]["id"]["S"], "guard",
        "ALL_OLD must echo the item's current image: {body}"
    );
    assert_eq!(reasons[1]["Item"]["v"]["N"], "7");

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

/// **Site 2** — every action is a `ConditionCheck` (no write at all, so
/// `run_transact` never calls `cp_txn`): the failing check is still flagged
/// at its own index, not just reported in aggregate.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn all_condition_check_transaction_flags_the_right_index() {
    let n = 3;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].dynamo;

    create_table(addr0, "cxl_c").await;

    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"cxl_c","Item":{"id":{"S":"guard"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "seed put failed: {body}");

    // [0] passes (absent key, attribute_not_exists holds)
    // [1] fails (guard exists)
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.TransactWriteItems",
        r#"{"TransactItems":[
            {"ConditionCheck":{"TableName":"cxl_c","Key":{"id":{"S":"absent-key"}},
                               "ConditionExpression":"attribute_not_exists(id)"}},
            {"ConditionCheck":{"TableName":"cxl_c","Key":{"id":{"S":"guard"}},
                               "ConditionExpression":"attribute_not_exists(id)"}}]}"#,
    )
    .await;
    assert_eq!(status, 400, "expected the guard check to cancel: {body}");

    let reasons = cancellation_reasons(&body);
    assert_eq!(reasons.len(), 2, "one entry per action: {body}");
    assert_eq!(reasons[0]["Code"], "None");
    assert_eq!(reasons[1]["Code"], "ConditionalCheckFailed");

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

/// A successful all-`ConditionCheck` transaction (the `writes.is_empty()`
/// fallback's happy path) carries no `CancellationReasons` at all — a
/// sanity check that the new machinery didn't leak into the success path.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_successful_transact_write_has_no_cancellation_reasons() {
    let n = 1;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].dynamo;

    create_table(addr0, "cxl_d").await;
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"cxl_d","Item":{"id":{"S":"guard"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "seed put failed: {body}");

    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.TransactWriteItems",
        r#"{"TransactItems":[
            {"ConditionCheck":{"TableName":"cxl_d","Key":{"id":{"S":"guard"}},
                               "ConditionExpression":"attribute_exists(id)"}}]}"#,
    )
    .await;
    assert_eq!(status, 200, "expected the check to pass: {body}");
    assert_eq!(body, "{}");

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

/// Small helper: pull a top-level string field out of a JSON body (used for
/// the aggregate `message` assertion above, where comparing the whole `Value`
/// would also have to restate the exact `__type`/`CancellationReasons`
/// shape already asserted elsewhere).
fn json_str(body: &str, field: &str) -> String {
    let json: Value = serde_json::from_str(body).expect("json body");
    json[field]
        .as_str()
        .unwrap_or_else(|| panic!("field `{field}` missing or not a string in: {body}"))
        .to_owned()
}
