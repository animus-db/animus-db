//! **Per-action `CancellationReasons` on `TransactionCanceledException`**
//! (ADR 0018's 2026-08-24 `CancellationReasons` amendment, issue #374 C2),
//! over `ProdEnv` — real HTTP against the DynamoDB wire edge, mirroring
//! `dynamo_txn.rs`'s harness style (its `bring_up`/`dynamo`/
//! `create_table_pre_split` idioms are copied here rather than shared,
//! matching that file's own precedent of not factoring per-suite harnesses
//! into `support`).
//!
//! C2a (above) covers only `ConditionCheck` failures — the two
//! coordinator-side evaluation sites in `dynamo.rs::run_transact` that
//! already know exactly which action failed before ever calling `cp_txn`
//! (the in-loop check against a transaction that also has write actions,
//! and the all-`ConditionCheck` fallback that never calls `cp_txn` at all).
//!
//! C2b (below) covers a write action's own condition/conflict, which
//! crosses the `cp_txn` 2PC boundary as a typed `TxnAbortReason`
//! (`crates/animusd/src/lib.rs`) — including the forwarded `TxnPrepare` hop,
//! proven by routing a request to a node that does NOT host the failing
//! key's own tablet leader.
//!
//! Real TCP/time → polls with generous timeouts, never a fixed sleep.

use std::net::SocketAddr;
use std::time::Duration;

use animus_dynamo::AttributeValue;
use animusd::{ClientRequest, ClientResponse, read_frame};
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
                advertise_host: None,
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

/// The exact data-plane key `dynamo.rs::item_key` computes for a simple
/// (partition-key-only) item: `partition_token(escape(pk)) ||
/// escape(pk)` — copied from `dynamo_txn.rs`'s helper of the same name (see
/// that file's own doc for why there is no other way to predict a
/// DynamoDB item's tablet placement from outside the edge).
fn item_key(pk: &str) -> Vec<u8> {
    let av = AttributeValue::S(pk.to_string());
    let escaped = animus_dynamo::storage_key(&av, None);
    let token = animus_tablet::partition_token(&escaped);
    let mut key = token.to_vec();
    key.extend_from_slice(&escaped);
    key
}

/// `CreateTable` (simple `id: S` partition key) then split its bootstrap
/// tablet so a chosen pair of item ids lands in different tablets — copied
/// from `dynamo_txn.rs`'s helper of the same name (see that file's own doc
/// for the ADR 0050 Train B rung-1 rationale behind proposing the split
/// metadata command directly rather than through the disabled client-facing
/// split surface).
async fn create_table_pre_split(
    nodes: &[animusd::Node],
    dynamo_addr: SocketAddr,
    client_addr: SocketAddr,
    table: &str,
) -> (String, String) {
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

    let mut candidates: Vec<(String, Vec<u8>)> = (0..40)
        .map(|i| {
            let id = format!("item{i:03}");
            let key = item_key(&id);
            (id, key)
        })
        .collect();
    candidates.sort_by(|a, b| a.1.cmp(&b.1));
    let mid = candidates.len() / 2;
    let (lower_id, lower_key) = candidates[mid - 1].clone();
    let (upper_id, upper_key) = candidates[mid].clone();
    assert!(lower_key < upper_key, "candidates must be strictly ordered");

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
    .expect("bootstrap tablet was never provisioned before the harness split");
    match call(
        client_addr,
        ClientRequest::SplitTablet {
            tablet: 1,
            split_key: upper_key,
        },
    )
    .await
    {
        ClientResponse::PutOk => {}
        other => panic!("split kickoff refused: {other:?}"),
    }
    timeout(Duration::from_secs(30), async {
        loop {
            if nodes.iter().all(|n| {
                let m = n.metadata();
                m.tablets.len() == 2 && !m.tablets.contains_key(&animus_tablet::TabletId(1))
            }) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the split workflow did not cut over within 30s");

    for id in [&lower_id, &upper_id] {
        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.PutItem",
            &format!(r#"{{"TableName":"{table}","Item":{{"id":{{"S":"probe-{id}"}}}}}}"#),
        )
        .await;
        assert_eq!(status, 200, "probe put for {id} failed: {body}");
    }

    (lower_id, upper_id)
}

async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    animusd::write_frame(&mut stream, &req).await.expect("send");
    read_frame(&mut stream)
        .await
        .expect("read")
        .expect("a reply")
}

/// Send `request` wrapped in `ClientRequest::Forwarded` directly to `addr` —
/// copied from `cp_txn.rs`'s helper of the same name (see that file's own
/// doc for why this bypasses `ClientCtx::cp_txn`'s own routing on purpose).
async fn call_forwarded(addr: SocketAddr, request: ClientRequest) -> ClientResponse {
    call(
        addr,
        ClientRequest::Forwarded {
            request: Box::new(request),
            traceparent: None,
        },
    )
    .await
}

/// Provision `table`'s bootstrap tablet via the plain client protocol's
/// auto-provisioning `Put` — copied from `cp_txn.rs`'s helper of the same
/// name (retries: provisioning a fresh table can race the control plane's
/// own tablet-map propagation).
async fn put_until_ok(addr: SocketAddr, table: &str, key: &[u8], value: &[u8]) {
    timeout(Duration::from_secs(25), async {
        loop {
            match call(
                addr,
                ClientRequest::Put {
                    key: key.to_vec(),
                    value: value.to_vec(),
                    table: table.to_string(),
                },
            )
            .await
            {
                ClientResponse::PutOk => return,
                ClientResponse::Error(_) => sleep(Duration::from_millis(150)).await,
                other => panic!("unexpected put response: {other:?}"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("put {table}/{key:?} did not succeed in 25s"));
}

/// `Forwarded { TxnPrepare }` only succeeds against a tablet's own current
/// leader — cycle every node until one replies, mirroring `cp_txn.rs`'s
/// helper of the same name.
async fn prepare_via_any_node(
    addrs: &[SocketAddr],
    request: ClientRequest,
) -> (
    animus_cp_data::TxnId,
    Vec<u8>,
    String,
    animus_cp_data::hlc::HlcTimestamp,
) {
    timeout(Duration::from_secs(20), async {
        loop {
            for &addr in addrs {
                if let ClientResponse::TxnPrepared {
                    txn_id,
                    record_key,
                    record_table,
                    ts,
                    outcome: _,
                } = call_forwarded(addr, request.clone()).await
                {
                    return (txn_id, record_key, record_table, ts);
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("prepare did not succeed against any node within 20s")
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

// ---------------------------------------------------------------------------
// C2b: a write action's own condition/conflict, crossing the cp_txn 2PC
// boundary as a typed TxnAbortReason.
// ---------------------------------------------------------------------------

/// A `Put` action's own `ConditionExpression` fails at its participant
/// leader (`ClientCtx::txn_stage_local`, never the coordinator-side
/// preflight C2a covers) — the failing index is still flagged, proving
/// `TxnAbortReason::ConditionFailed` survives `cp_txn`'s own boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn write_action_condition_failure_flags_the_right_action_index() {
    let n = 3;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].dynamo;

    create_table(addr0, "cxl_e").await;

    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"cxl_e","Item":{"id":{"S":"guard"},"v":{"S":"present"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "seed put failed: {body}");

    // [0] Put on an absent key with an unconditioned write — has nothing to
    //     do with the failure, must report None.
    // [1] Put on "guard", conditioned on absence — fails, since guard exists.
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.TransactWriteItems",
        r#"{"TransactItems":[
            {"Put":{"TableName":"cxl_e","Item":{"id":{"S":"other"},"v":{"S":"x"}}}},
            {"Put":{"TableName":"cxl_e","Item":{"id":{"S":"guard"},"v":{"S":"should-not-land"}},
                    "ConditionExpression":"attribute_not_exists(id)"}}]}"#,
    )
    .await;
    assert_eq!(
        status, 400,
        "expected the guard write's own condition to cancel: {body}"
    );

    let reasons = cancellation_reasons(&body);
    assert_eq!(reasons.len(), 2, "one entry per action: {body}");
    assert_eq!(reasons[0]["Code"], "None");
    assert_eq!(reasons[1]["Code"], "ConditionalCheckFailed");

    // Whole-or-nothing across the 2PC boundary too: "other" must not have
    // landed even though it precedes the failing write in list order.
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.GetItem",
        r#"{"ConsistentRead":true,"TableName":"cxl_e","Key":{"id":{"S":"other"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        body, "{}",
        "cancelled transaction must not have written: {body}"
    );

    // And "guard" itself must be unchanged.
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.GetItem",
        r#"{"ConsistentRead":true,"TableName":"cxl_e","Key":{"id":{"S":"guard"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        body.contains("\"present\""),
        "the guard item's own value must be unchanged: {body}"
    );

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

/// **Cross-tablet, cross-node correlation**: the same write-action-condition
/// failure as above, but the failing key lives on a *different* tablet from
/// the anchor, and the request is sent to a node that hosts neither
/// participant's leader — proving the typed reason survives both the 2PC
/// fan-out and the forwarded `TxnPrepare` hop (`TxnAbortReason::encode`/
/// `decode`), not just the local/single-node case above.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn write_action_condition_failure_survives_the_forwarding_hop() {
    let n = 3;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].dynamo;
    let client0 = config.nodes[0].client;

    let (lower_id, upper_id) = create_table_pre_split(&nodes, addr0, client0, "cxl_f").await;

    // Seed the upper-half key so its own `attribute_not_exists` condition
    // fails.
    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.PutItem",
        &format!(
            r#"{{"TableName":"cxl_f","Item":{{"id":{{"S":"{upper_id}"}},"v":{{"S":"present"}}}}}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "seed put failed: {body}");

    // Every node's own Dynamo listener — proving this is not sensitive to
    // which node the request happens to land on (some route locally, some
    // forward one or two hops).
    for (i, node_cfg) in config.nodes.iter().enumerate() {
        let (status, body) = dynamo(
            node_cfg.dynamo,
            "DynamoDB_20120810.TransactWriteItems",
            &format!(
                r#"{{"TransactItems":[
                    {{"Put":{{"TableName":"cxl_f","Item":{{"id":{{"S":"{lower_id}"}},"v":{{"S":"x"}}}}}}}},
                    {{"Put":{{"TableName":"cxl_f","Item":{{"id":{{"S":"{upper_id}"}},"v":{{"S":"should-not-land"}}}},
                            "ConditionExpression":"attribute_not_exists(id)"}}}}]}}"#
            ),
        )
        .await;
        assert_eq!(
            status, 400,
            "node {i}: expected the upper-half write's own condition to cancel: {body}"
        );
        let reasons = cancellation_reasons(&body);
        assert_eq!(reasons.len(), 2, "node {i}: one entry per action: {body}");
        assert_eq!(reasons[0]["Code"], "None", "node {i}: {body}");
        assert_eq!(
            reasons[1]["Code"], "ConditionalCheckFailed",
            "node {i}: the cross-tablet participant's own condition must still be flagged \
             at its own index: {body}"
        );
    }

    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.GetItem",
        &format!(
            r#"{{"ConsistentRead":true,"TableName":"cxl_f","Key":{{"id":{{"S":"{lower_id}"}}}}}}"#
        ),
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

/// **`TransactionConflict` reachability — and why this test does NOT go
/// through the DynamoDB wire, unlike every other test in this file.**
///
/// Every DynamoDB write action is a kind-write-path action (ADR 0049's
/// universal write-path gate): `ClientCtx::txn_stage_local` reads the item's
/// current value ([`dynamo::eval_kind_txn_write`]) *before* ever staging
/// anything. When another transaction already holds an unresolved intent on
/// that same key, this read is exactly what blocks/errors first — the apply
/// path's own writer-push-intents guard (`StageOutcome::IntentBlocked`,
/// which is what `TransactionConflict` maps from) is never reached at all,
/// confirmed empirically while building this test (the DynamoDB-edge
/// version consistently produced `TxnAbortReason::Other("...old-image read
/// failed: CP group leader moved; retry")` instead, after blocking for
/// `INTENT_WAIT_TIMEOUT`). `TransactionConflict` is real machinery — reached
/// by the RAW client protocol's plain writes (`TxnTableWrite::plain`, e.g.
/// `animus-cli`), which propose directly with no preceding read — so this
/// test proves it through `ClientRequest::Txn` instead, the same primitive
/// [`cp_txn.rs`](../tests/cp_txn.rs)'s own suite drives directly. See
/// `docs/adr/0018-cross-tablet-transactions.md`'s 2026-08-24
/// `CancellationReasons` amendment for this scoped-reachability note in
/// full.
///
/// Stages a raw `TxnPrepare` (the `cp_txn.rs::prepare_via_any_node` idiom)
/// directly against a key — an intent lands, but nobody ever sends
/// `TxnDecide`, simulating a coordinator that crashed between prepare and
/// decide. A second, real `ClientRequest::Txn` plain write on that same key
/// then loses the race for the whole of `txn_prepare_pushing`'s bounded
/// retry budget, well inside `RECOVERY_GRACE` — `TransactionConflict`,
/// never `ConditionalCheckFailed`. Resolves the parked intent afterward so
/// teardown doesn't hang on it.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn write_action_intent_conflict_flags_transaction_conflict() {
    let n = 1;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let client0 = config.nodes[0].client;
    let intra0 = config.nodes[0].intra; // ADR 0047: Forwarded is intra-only

    let table = "cxl_h".to_string();
    let blocked_key = item_key("conflict-key");

    // `TxnPrepare` (unlike `cp_txn`) does NOT auto-provision — provision the
    // table's bootstrap tablet with a throwaway plain put first (mirrors
    // `cp_txn.rs`'s own use of `put_until_ok` ahead of its raw prepares).
    put_until_ok(client0, &table, b"unrelated-probe", b"x").await;

    // Stage the anchor directly — an intent lands, and (deliberately) is
    // never decided.
    let (txn_id, record_key, record_table, ts) = prepare_via_any_node(
        &[intra0],
        ClientRequest::TxnPrepare {
            table: table.clone(),
            anchor: None,
            writes: vec![animus_cp_data::TxnWrite::plain(
                blocked_key.clone(),
                Some(b"parked-by-a-crashed-coordinator".to_vec()),
            )],
            conditions: Vec::new(),
            participant_spans: Vec::new(),
            pending_kind_writes: Vec::new(),
        },
    )
    .await;

    // A second, real transaction on the same key, well within
    // RECOVERY_GRACE (5s) — `txn_prepare_pushing`'s own bounded retry
    // budget (3 attempts, 250ms backoff) exhausts long before the
    // background resolver would ever push the parked intent.
    let response = timeout(
        Duration::from_secs(20),
        call(
            client0,
            ClientRequest::Txn {
                writes: vec![animusd::TxnTableWrite::plain(
                    table.clone(),
                    blocked_key.clone(),
                    Some(b"should-not-land".to_vec()),
                )],
                preconditions: Vec::new(),
                write_conditions: Vec::new(),
            },
        ),
    )
    .await
    .expect("the conflicting Txn did not even respond within 20s");
    let ClientResponse::Error(message) = response else {
        panic!("expected the intent-blocked key to cancel the transaction, got: {response:?}");
    };
    assert!(
        message.contains("lost a race against another in-flight transaction"),
        "a lost race against another transaction's own unresolved intent is \
         TransactionConflict, never ConditionalCheckFailed/Other: {message}"
    );

    // Clean up: abort the parked intent, then resolve it, so node shutdown
    // doesn't have to wait out `RECOVERY_GRACE`/the resolver loop for it.
    let outcome = match call_forwarded(
        intra0,
        ClientRequest::TxnDecide {
            table: record_table.clone(),
            txn_id: txn_id.clone(),
            record_key: record_key.clone(),
            commit: false,
            min_commit_ts: ts,
            orphan_created_ts: None,
        },
    )
    .await
    {
        ClientResponse::TxnDecided { outcome } => outcome,
        other => panic!("cleanup TxnDecide failed: {other:?}"),
    };
    match call_forwarded(
        intra0,
        ClientRequest::TxnResolve {
            table: record_table,
            txn_id,
            record_key,
            keys: vec![blocked_key],
            outcome,
        },
    )
    .await
    {
        ClientResponse::TxnResolved { outcome } => assert_eq!(
            outcome,
            animus_cp_data::ResolveOutcome::Resolved,
            "cleanup resolve must actually land, not fence-miss"
        ),
        other => panic!("cleanup TxnResolve failed: {other:?}"),
    }

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
