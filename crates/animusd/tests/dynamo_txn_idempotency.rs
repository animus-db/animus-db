//! **`ClientRequestToken` idempotency for `TransactWriteItems`** (ADR 0018's
//! 2026-08-24 amendment), over a real `ProdEnv` multi-process cluster —
//! mirroring `dynamo_txn.rs`'s harness style, but exercising the
//! `ClientRequestToken` preflight/outcome protocol
//! (`dynamo.rs::run_transact`'s own doc has the full state machine) rather
//! than atomicity itself.
//!
//! 1. `same_token_same_fingerprint_retry_after_commit_is_cached` — a retry
//!    with the identical token AND actions after a commit returns 200 both
//!    times, with the write effect applied exactly once (no re-run).
//! 2. `same_token_different_actions_is_a_parameter_mismatch` — a retry with
//!    the same token but different actions is a 400
//!    `IdempotentParameterMismatchException`.
//! 3. `token_dedup_survives_a_leader_failover_of_the_internal_tablet` — a
//!    commit registers a token, the node currently leading the internal
//!    `__animus_txn_idempotency` table's own tablet is killed, and a retry
//!    of the same token via a surviving node still returns the cached
//!    success with the effect applied exactly once.
//! 4. `the_internal_table_is_invisible_and_unreachable` — `ListTables` omits
//!    it, a direct `PutItem`/`GetItem` naming it is `ResourceNotFoundException`,
//!    and `CreateTable` of that name is `ValidationException`.
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
// Shared bring-up + protocol helpers (mirrors dynamo_txn.rs).
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
            })
            .collect();
        let config = ClusterConfig {
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

/// One admin HTTP/1.0 request → `(status, parsed JSON)` (mirrors
/// `dynamo_txn.rs`'s helper of the same name).
async fn admin_get(addr: SocketAddr, path: &str) -> (u16, Value) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to admin");
    let request = format!("GET {path} HTTP/1.0\r\nHost: animus\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("send request");
    stream.flush().await.expect("flush");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read response");
    let text = String::from_utf8(raw).expect("utf8 response");
    let (head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status line");
    let value: Value = serde_json::from_str(payload)
        .unwrap_or_else(|e| panic!("admin body is not JSON ({e}): {payload}"));
    (status, value)
}

/// `CreateTable` (simple `id: S` partition key), waiting for it to be
/// serveable.
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

/// The current numeric value of `attr` on `id`, via a strongly-consistent
/// `GetItem` — `0` if the item or attribute is absent (an `ADD` on a fresh
/// key upserts starting from nothing, which this helper treats the same
/// way for a simpler call site).
async fn read_counter(dynamo_addr: SocketAddr, table: &str, id: &str, attr: &str) -> i64 {
    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.GetItem",
        &format!(
            r#"{{"ConsistentRead":true,"TableName":"{table}","Key":{{"id":{{"S":"{id}"}}}}}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "GetItem({id}) failed: {body}");
    let v: Value = serde_json::from_str(&body).expect("valid JSON GetItem response");
    v["Item"][attr]["N"]
        .as_str()
        .map_or(0, |s| s.parse().expect("N is a valid integer"))
}

// ---------------------------------------------------------------------------
// (1) Same token, same fingerprint: cached, no re-run.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn same_token_same_fingerprint_retry_after_commit_is_cached() {
    let n = 2;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr = config.nodes[0].dynamo;

    create_table(addr, "ctr1").await;

    let body = r#"{"ClientRequestToken":"retry-token-1",
        "TransactItems":[{"Update":{"TableName":"ctr1","Key":{"id":{"S":"c"}},
            "UpdateExpression":"ADD hits :one",
            "ExpressionAttributeValues":{":one":{"N":"1"}}}}]}"#;

    let (status1, body1) = dynamo(addr, "DynamoDB_20120810.TransactWriteItems", body).await;
    assert_eq!(status1, 200, "first attempt failed: {body1}");
    assert_eq!(read_counter(addr, "ctr1", "c", "hits").await, 1);

    // Same token, byte-identical actions: must be cached — 200 again, no
    // second `ADD` (else the counter would read 2, not 1).
    let (status2, body2) = dynamo(addr, "DynamoDB_20120810.TransactWriteItems", body).await;
    assert_eq!(status2, 200, "retried attempt failed: {body2}");
    assert_eq!(
        read_counter(addr, "ctr1", "c", "hits").await,
        1,
        "a same-token retry must not re-run the transaction"
    );

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

// ---------------------------------------------------------------------------
// (2) Same token, different actions: IdempotentParameterMismatchException.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn same_token_different_actions_is_a_parameter_mismatch() {
    let n = 2;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr = config.nodes[0].dynamo;

    create_table(addr, "mismatch1").await;

    let first = r#"{"ClientRequestToken":"reused-token",
        "TransactItems":[{"Put":{"TableName":"mismatch1","Item":{"id":{"S":"a"}}}}]}"#;
    let (status1, body1) = dynamo(addr, "DynamoDB_20120810.TransactWriteItems", first).await;
    assert_eq!(status1, 200, "first attempt failed: {body1}");

    // Same token, a genuinely different action set (different key).
    let second = r#"{"ClientRequestToken":"reused-token",
        "TransactItems":[{"Put":{"TableName":"mismatch1","Item":{"id":{"S":"b"}}}}]}"#;
    let (status2, body2) = dynamo(addr, "DynamoDB_20120810.TransactWriteItems", second).await;
    assert_eq!(status2, 400, "mismatched retry should be rejected: {body2}");
    assert!(
        body2.contains("IdempotentParameterMismatchException"),
        "expected IdempotentParameterMismatchException, got: {body2}"
    );

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

// ---------------------------------------------------------------------------
// (3) Token dedup survives a leader failover of the internal tablet.
// ---------------------------------------------------------------------------

/// The local group's `is_leader` for tablet `tablet`, from this node's
/// node-local admin view (mirrors `cp_reconfigure.rs`'s `group_view`).
async fn group_is_leader(admin_addr: SocketAddr, tablet: u64) -> bool {
    let (_s, v) = admin_get(admin_addr, "/admin/raftkv").await;
    v["groups"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|g| g["tablet"].as_u64() == Some(tablet) && g["is_leader"].as_bool() == Some(true))
}

/// The index (into `config.nodes`) of the node currently leading `tablet`,
/// polling until one node reports leadership.
async fn await_tablet_leader_index(config: &ClusterConfig, tablet: u64) -> usize {
    timeout(Duration::from_secs(20), async {
        loop {
            for (i, addrs) in config.nodes.iter().enumerate() {
                if group_is_leader(addrs.admin, tablet).await {
                    return i;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("no node ever reported leading the tablet within 20s")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn token_dedup_survives_a_leader_failover_of_the_internal_tablet() {
    let n = 3;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].dynamo;

    create_table(addr0, "failover1").await;

    let body = r#"{"ClientRequestToken":"failover-token",
        "TransactItems":[{"Update":{"TableName":"failover1","Key":{"id":{"S":"c"}},
            "UpdateExpression":"ADD hits :one",
            "ExpressionAttributeValues":{":one":{"N":"1"}}}}]}"#;

    let (status1, body1) = dynamo(addr0, "DynamoDB_20120810.TransactWriteItems", body).await;
    assert_eq!(status1, 200, "first attempt failed: {body1}");
    assert_eq!(read_counter(addr0, "failover1", "c", "hits").await, 1);

    // Find the internal idempotency table's own tablet id, then which node
    // currently leads it — both only knowable after the table's own lazy
    // bootstrap has actually run, which the commit above just did.
    let tablet = timeout(Duration::from_secs(20), async {
        loop {
            for node in &nodes {
                if let Some((tablet, _)) = node
                    .metadata()
                    .tablets_for_table(animus_dynamo::TXN_IDEMPOTENCY_TABLE)
                    .next()
                {
                    return tablet.0;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the internal table's tablet never appeared in metadata within 20s");

    let leader_idx = await_tablet_leader_index(&config, tablet).await;

    // Kill the node currently leading the internal table's tablet — a fresh
    // election must land the tablet elsewhere, and the retry below must
    // still find (and honor) the durable idempotency record.
    nodes[leader_idx].shutdown_graceful().await;
    let mut surviving: Vec<SocketAddr> = config
        .nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != leader_idx)
        .map(|(_, a)| a.dynamo)
        .collect();
    assert!(!surviving.is_empty(), "at least one node must survive");

    // Retry the identical token/actions via a surviving node. Allow retry
    // loops for post-failover election latency (converged-or-timeout, never
    // a fixed-deadline one-shot assert) — the new leader must first be
    // elected, and this node's own routing must resolve to it.
    timeout(Duration::from_secs(30), async {
        loop {
            let target = surviving[0];
            let (status, resp_body) =
                dynamo(target, "DynamoDB_20120810.TransactWriteItems", body).await;
            if status == 200 {
                return;
            }
            surviving.rotate_left(1);
            let _ = resp_body;
            sleep(Duration::from_millis(150)).await;
        }
    })
    .await
    .expect("the retried TransactWriteItems never succeeded within 30s of the failover");

    assert_eq!(
        read_counter(surviving[0], "failover1", "c", "hits").await,
        1,
        "the effect must still have applied exactly once across the failover"
    );

    for (i, node) in nodes.iter().enumerate() {
        if i != leader_idx {
            node.shutdown_graceful().await;
        }
    }
}

// ---------------------------------------------------------------------------
// (4) Visibility guards.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn the_internal_table_is_invisible_and_unreachable() {
    let n = 1;
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(n, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr = config.nodes[0].dynamo;

    create_table(addr, "visibility1").await;

    // Bootstrap the internal table via one token-bearing transaction first,
    // so `ListTables`'s exclusion is proven against a table that genuinely
    // exists in the catalog, not merely one that was never created.
    let body = r#"{"ClientRequestToken":"visibility-token",
        "TransactItems":[{"Put":{"TableName":"visibility1","Item":{"id":{"S":"a"}}}}]}"#;
    let (status, resp) = dynamo(addr, "DynamoDB_20120810.TransactWriteItems", body).await;
    assert_eq!(status, 200, "bootstrap transaction failed: {resp}");

    // `ListTables` must never mention it.
    let (status, body) = dynamo(addr, "DynamoDB_20120810.ListTables", "{}").await;
    assert_eq!(status, 200, "ListTables failed: {body}");
    assert!(
        !body.contains(animus_dynamo::TXN_IDEMPOTENCY_TABLE),
        "ListTables must never list the internal table: {body}"
    );

    // A direct `PutItem`/`GetItem` naming it must 404, like any nonexistent
    // table — never expose the internal record shape to a client.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        &format!(
            r#"{{"TableName":"{}","Item":{{"pk":{{"S":"x"}}}}}}"#,
            animus_dynamo::TXN_IDEMPOTENCY_TABLE
        ),
    )
    .await;
    assert_eq!(status, 400, "PutItem on the internal table: {body}");
    assert!(
        body.contains("ResourceNotFoundException"),
        "expected ResourceNotFoundException, got: {body}"
    );

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.GetItem",
        &format!(
            r#"{{"TableName":"{}","Key":{{"pk":{{"S":"x"}}}}}}"#,
            animus_dynamo::TXN_IDEMPOTENCY_TABLE
        ),
    )
    .await;
    assert_eq!(status, 400, "GetItem on the internal table: {body}");
    assert!(
        body.contains("ResourceNotFoundException"),
        "expected ResourceNotFoundException, got: {body}"
    );

    // `CreateTable` of that exact name is a reserved-name `ValidationException`
    // — the name genuinely is reserved, distinct from "does not exist".
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        &format!(
            r#"{{"TableName":"{}",
                "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
                "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]}}"#,
            animus_dynamo::TXN_IDEMPOTENCY_TABLE
        ),
    )
    .await;
    assert_eq!(
        status, 400,
        "CreateTable of the internal table name: {body}"
    );
    assert!(
        body.contains("ValidationException"),
        "expected ValidationException, got: {body}"
    );

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}
