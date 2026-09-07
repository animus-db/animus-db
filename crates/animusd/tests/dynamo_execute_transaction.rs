//! End-to-end tests for `ExecuteTransaction` (ADR 0071, W-07 PR 5, closing
//! the W-07 PartiQL train) — over the real DynamoDB JSON/HTTP wire,
//! mirroring `dynamo_txn.rs`/`dynamo_txn_cancellation.rs`'s harness style
//! (their `bring_up`/`dynamo` idioms are copied here rather than shared,
//! per this crate's own "sibling test modules keep their own fixtures
//! independent" precedent) but issuing `ExecuteTransaction` requests
//! instead of raw `TransactWriteItems`/`TransactGetItems`.
//!
//! Proves: an all-write transaction commits atomically across two tables;
//! an unmet condition (a duplicate `INSERT`) cancels the whole transaction
//! with `CancellationReasons` naming the right statement index and leaves
//! nothing written; an all-`SELECT` transaction returns items and misses in
//! request order; a mixed `SELECT`+`INSERT` transaction is a
//! `ValidationException`; 0 and 26 statements are each a
//! `ValidationException`; a `ClientRequestToken` replay returns the cached
//! outcome without re-applying; and a follower-connected node can execute
//! the transaction. The underlying atomicity/2PC mechanics themselves are
//! proven at `dynamo_txn.rs`'s level; this suite's job is narrower —
//! proving the PartiQL lowering + dispatch this ADR adds composes with that
//! existing machinery.
//!
//! Real TCP/time → polls with generous timeouts, never a fixed sleep.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{ClusterConfig, Node, RoleAddrs};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

// ---------------------------------------------------------------------------
// Shared bring-up + protocol helpers (mirrors dynamo_txn_cancellation.rs).
// ---------------------------------------------------------------------------

async fn bring_up(n: usize, dir: &std::path::Path) -> (Vec<Node>, ClusterConfig) {
    for attempt in 0..16 {
        let addrs = support::free_addrs(n * 6);
        let nodes_cfg: Vec<RoleAddrs> = (0..n)
            .map(|i| RoleAddrs {
                id: animusd::config::node_id(i),
                role: animusd::config::NodeRole::Both,
                internal: addrs[6 * i],
                client: addrs[6 * i + 1],
                dynamo: addrs[6 * i + 2],
                admin: addrs[6 * i + 3],
                intra: addrs[6 * i + 4],
                console: addrs[6 * i + 5],
                advertise_host: None,
                tls: None,
                encryption_key_path: None,
            })
            .collect();
        let config = ClusterConfig {
            nodes: nodes_cfg,
            dynamo_auth: None,
            cluster_settings: None,
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

async fn await_bootstrap(nodes: &[Node]) {
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
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

/// `dynamo`, retried on a transient `500` — the identical helper every
/// PartiQL/txn test suite in this crate uses (`dynamo_partiql.rs`'s own
/// `dynamo_retry`).
async fn dynamo_retry(addr: SocketAddr, target: &str, body: &str) -> (u16, String) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let (status, resp) = dynamo(addr, target, body).await;
        if status != 500 || tokio::time::Instant::now() >= deadline {
            return (status, resp);
        }
        sleep(Duration::from_millis(150)).await;
    }
}

async fn execute_transaction(addr: SocketAddr, body: &str) -> (u16, Value) {
    let (status, resp) = dynamo_retry(addr, "DynamoDB_20120810.ExecuteTransaction", body).await;
    let json: Value =
        serde_json::from_str(&resp).unwrap_or_else(|e| panic!("response is not JSON: {e}: {resp}"));
    (status, json)
}

async fn create_table(dynamo_addr: SocketAddr, table: &str) {
    let (status, body) = dynamo_retry(
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

async fn get_item(dynamo_addr: SocketAddr, table: &str, id: &str) -> (u16, Value) {
    let (status, resp) = dynamo_retry(
        dynamo_addr,
        "DynamoDB_20120810.GetItem",
        &format!(
            r#"{{"TableName":"{table}","Key":{{"id":{{"S":"{id}"}}}},"ConsistentRead":true}}"#
        ),
    )
    .await;
    let json: Value =
        serde_json::from_str(&resp).unwrap_or_else(|e| panic!("response is not JSON: {e}: {resp}"));
    (status, json)
}

/// Parse a `TransactionCanceledException` body's `CancellationReasons`
/// array — copied from `dynamo_txn_cancellation.rs`'s helper of the same
/// name.
fn cancellation_reasons(v: &Value) -> Vec<Value> {
    assert_eq!(
        v["__type"], "com.amazonaws.dynamodb.v20120810#TransactionCanceledException",
        "expected TransactionCanceledException, got: {v}"
    );
    v["CancellationReasons"]
        .as_array()
        .unwrap_or_else(|| panic!("no CancellationReasons array in: {v}"))
        .clone()
}

// ---------------------------------------------------------------------------
// (a) An all-write transaction commits atomically across two tables.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn execute_transaction_write_commits_atomically_across_two_tables() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(1, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr = config.nodes[0].dynamo;

    create_table(addr, "xact_a").await;
    create_table(addr, "xact_b").await;

    let (status, resp) = execute_transaction(
        addr,
        r#"{"TransactStatements":[
            {"Statement":"INSERT INTO xact_a VALUE {'id': ?, 'v': ?}",
             "Parameters":[{"S":"1"},{"S":"lo"}]},
            {"Statement":"INSERT INTO xact_b VALUE {'id': ?, 'v': ?}",
             "Parameters":[{"S":"1"},{"S":"hi"}]}]}"#,
    )
    .await;
    assert_eq!(status, 200, "{resp}");
    let responses = resp["Responses"].as_array().expect("Responses array");
    assert_eq!(responses.len(), 2);
    for r in responses {
        assert_eq!(
            r.as_object().unwrap().len(),
            0,
            "a write transaction's own Responses entries must be empty objects: {resp}"
        );
    }

    let (status, got) = get_item(addr, "xact_a", "1").await;
    assert_eq!(status, 200, "{got}");
    assert_eq!(got["Item"]["v"]["S"], "lo");
    let (status, got) = get_item(addr, "xact_b", "1").await;
    assert_eq!(status, 200, "{got}");
    assert_eq!(got["Item"]["v"]["S"], "hi");

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

// ---------------------------------------------------------------------------
// (b) An unmet condition cancels the WHOLE transaction, nothing written.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn execute_transaction_write_cancels_whole_on_duplicate_insert() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(1, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr = config.nodes[0].dynamo;

    create_table(addr, "xact_c").await;

    // Seed an existing item at id "2" so the second statement's INSERT
    // fails its implicit attribute_not_exists(id) condition.
    let (status, resp) = execute_transaction(
        addr,
        r#"{"TransactStatements":[
            {"Statement":"INSERT INTO xact_c VALUE {'id': ?}","Parameters":[{"S":"2"}]}]}"#,
    )
    .await;
    assert_eq!(status, 200, "seed: {resp}");

    // A transaction: [0] a fresh INSERT that would succeed in isolation,
    // [1] a duplicate INSERT at the already-seeded key — must cancel BOTH.
    let (status, resp) = execute_transaction(
        addr,
        r#"{"TransactStatements":[
            {"Statement":"INSERT INTO xact_c VALUE {'id': ?}","Parameters":[{"S":"should-not-land"}]},
            {"Statement":"INSERT INTO xact_c VALUE {'id': ?}","Parameters":[{"S":"2"}]}]}"#,
    )
    .await;
    assert_eq!(status, 400, "{resp}");
    let reasons = cancellation_reasons(&resp);
    assert_eq!(reasons.len(), 2);
    assert_eq!(reasons[0]["Code"], "None");
    assert_eq!(reasons[1]["Code"], "ConditionalCheckFailed");

    let (status, got) = get_item(addr, "xact_c", "should-not-land").await;
    assert_eq!(status, 200, "{got}");
    assert!(
        got.get("Item").is_none(),
        "the first statement's item must NOT have been written by a cancelled \
         transaction: {got}"
    );

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

// ---------------------------------------------------------------------------
// (c) An all-SELECT transaction returns items and misses, in order.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn execute_transaction_all_select_returns_items_and_misses_in_order() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(1, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr = config.nodes[0].dynamo;

    create_table(addr, "xact_d").await;
    let (status, resp) = dynamo_retry(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"xact_d","Item":{"id":{"S":"a"},"v":{"S":"va"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "{resp}");

    let (status, resp) = execute_transaction(
        addr,
        r#"{"TransactStatements":[
            {"Statement":"SELECT * FROM xact_d WHERE id = ?","Parameters":[{"S":"a"}]},
            {"Statement":"SELECT * FROM xact_d WHERE id = ?","Parameters":[{"S":"missing"}]}]}"#,
    )
    .await;
    assert_eq!(status, 200, "{resp}");
    let responses = resp["Responses"].as_array().expect("Responses array");
    assert_eq!(responses.len(), 2);
    assert_eq!(responses[0]["Item"]["v"]["S"], "va");
    assert_eq!(
        responses[1].as_object().unwrap().len(),
        0,
        "a miss must be an empty object entry: {resp}"
    );

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

// ---------------------------------------------------------------------------
// (d) Validation: mixed SELECT+mutation, and statement-count bounds.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn execute_transaction_mixed_select_and_insert_is_validation_exception() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(1, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr = config.nodes[0].dynamo;

    create_table(addr, "xact_e").await;

    let (status, resp) = execute_transaction(
        addr,
        r#"{"TransactStatements":[
            {"Statement":"SELECT * FROM xact_e WHERE id = ?","Parameters":[{"S":"a"}]},
            {"Statement":"INSERT INTO xact_e VALUE {'id': ?}","Parameters":[{"S":"b"}]}]}"#,
    )
    .await;
    assert_eq!(status, 400, "{resp}");
    assert_eq!(
        resp["__type"], "com.amazonaws.dynamodb.v20120810#ValidationException",
        "{resp}"
    );

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn execute_transaction_rejects_zero_and_too_many_statements() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(1, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr = config.nodes[0].dynamo;

    let (status, resp) = execute_transaction(addr, r#"{"TransactStatements":[]}"#).await;
    assert_eq!(status, 400, "{resp}");
    assert_eq!(
        resp["__type"], "com.amazonaws.dynamodb.v20120810#ValidationException",
        "{resp}"
    );

    create_table(addr, "xact_f").await;
    let statements: Vec<String> = (0..26)
        .map(|i| {
            format!(
                r#"{{"Statement":"SELECT * FROM xact_f WHERE id = ?","Parameters":[{{"S":"i{i}"}}]}}"#
            )
        })
        .collect();
    let body = format!(r#"{{"TransactStatements":[{}]}}"#, statements.join(","));
    let (status, resp) = execute_transaction(addr, &body).await;
    assert_eq!(status, 400, "{resp}");
    assert_eq!(
        resp["__type"], "com.amazonaws.dynamodb.v20120810#ValidationException",
        "{resp}"
    );

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

// ---------------------------------------------------------------------------
// (e) `ClientRequestToken` idempotency (mirrors dynamo_txn_idempotency.rs).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn execute_transaction_client_request_token_replay_is_cached() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(1, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr = config.nodes[0].dynamo;

    create_table(addr, "xact_g").await;

    let body = r#"{"ClientRequestToken":"tok-exec-txn-1",
        "TransactStatements":[
            {"Statement":"INSERT INTO xact_g VALUE {'id': ?, 'v': ?}",
             "Parameters":[{"S":"1"},{"S":"first"}]}]}"#;

    let (status, resp) = execute_transaction(addr, body).await;
    assert_eq!(status, 200, "first attempt: {resp}");

    // A retry with the identical token AND identical statements must be a
    // cached no-op — 200 again, never a DuplicateItemException/
    // TransactionCanceledException, and the original value must survive
    // (proving no second execution ran).
    let (status, resp) = execute_transaction(addr, body).await;
    assert_eq!(status, 200, "replay: {resp}");

    let (status, got) = get_item(addr, "xact_g", "1").await;
    assert_eq!(status, 200, "{got}");
    assert_eq!(got["Item"]["v"]["S"], "first");

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

// ---------------------------------------------------------------------------
// (f) A follower-connected node can execute the transaction.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn execute_transaction_over_a_follower_connected_node() {
    let n = 2;
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].dynamo;

    create_table(addr0, "xact_h").await;

    // Issue the transaction through node 1 — not necessarily the leader of
    // any tablet involved, exercising the forwarding path.
    let addr1 = config.nodes[1 % n].dynamo;
    let (status, resp) = execute_transaction(
        addr1,
        r#"{"TransactStatements":[
            {"Statement":"INSERT INTO xact_h VALUE {'id': ?, 'v': ?}",
             "Parameters":[{"S":"1"},{"S":"via-follower"}]}]}"#,
    )
    .await;
    assert_eq!(status, 200, "{resp}");

    // Read back through the OTHER node, confirming it's genuinely visible
    // cluster-wide, not just locally cached on whichever node served it.
    let (status, got) = get_item(addr0, "xact_h", "1").await;
    assert_eq!(status, 200, "{got}");
    assert_eq!(got["Item"]["v"]["S"], "via-follower");

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}
