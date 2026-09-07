//! End-to-end test of `Query`'s `ScanIndexForward` over the real DynamoDB
//! JSON/HTTP wire, against a **GSI** — the descending read, and its
//! `ScanIndexForward` numeric ordering (ADR 0063).
//!
//! **ADR 0061 rung D3 PR 3a moved six of this file's eight tests to
//! `SimCluster`** (`crates/animusd/src/sim_cluster_dynamo_scan_index_
//! forward.rs`) — the base-table and LSI descending/pagination/filter
//! tests, and the LSI numeric-ordering test; see that module's own doc.
//! **`descending_applies_to_a_gsi_query` and `gsi_scan_index_forward_
//! orders_n_sort_keys_numerically` stay here**: both read a materialized
//! GSI row, which `SimCluster` cannot produce (no `index_drain::
//! change_consumer_loop`).
//!
//! Real time/sockets, so these assertions poll with generous timeouts.

use std::net::SocketAddr;
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

/// `dynamo`, retried on a retryable `500 InternalServerError` for up to 20s —
/// a read is trivially idempotent, so retrying it is always safe. See
/// `dynamo_index_scan.rs`'s identical helper for the full rationale (the CP
/// data plane's transient "not the leader here"/leadership-churn refusal
/// surfaces as a clean `500`, including well after initial cluster
/// formation).
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

/// Poll a GSI `Query` until `accept` is satisfied. A GSI is materialized
/// **asynchronously** by the drain (ADR 0041 §4/§5) — DynamoDB's own
/// eventually-consistent contract — so every assertion against one must be a
/// converged-or-timeout poll, never a fixed sleep + one-shot check.
async fn await_gsi_query(addr: SocketAddr, body: &str, accept: impl Fn(&str) -> bool) -> String {
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

/// Stand up a 3-node cluster with one table (`events`, composite key
/// `pk`/`sk`) carrying a hash-only GSI (`by-cat`, hash `cat`) and an LSI
/// (`by-score`, alt-sort `score`) — six items, all in base partition
/// `pk = "p1"` and all sharing GSI hash `cat = "X"`, so one fixture serves the
/// base, GSI and LSI filter tests alike.
///
/// The filterable attribute is `parity`, a **non-key** attribute on every
/// index involved, set to `even` on the three even `sk`s and `odd` on the
/// three odd ones. Half the partition matching is what makes the
/// fewer-than-`Limit` page observable.
///
/// | sk | cat | score | parity |
/// |----|-----|-------|--------|
/// | a0 | X   | s0    | even   |
/// | a1 | X   | s1    | odd    |
/// | a2 | X   | s2    | even   |
/// | a3 | X   | s3    | odd    |
/// | a4 | X   | s4    | even   |
/// | a5 | X   | s5    | odd    |
async fn setup() -> (support::PanicSafeTempDir, Vec<Node>, Vec<SocketAddr>) {
    let dir = support::panic_safe_tempdir();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;
    let addrs: Vec<SocketAddr> = nodes.iter().map(Node::dynamo_addr).collect();

    let (status, body) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"events","AttributeDefinitions":[{"AttributeName":"cat","AttributeType":"S"},{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"score","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-cat",
                 "KeySchema":[{"AttributeName":"cat","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"ALL"}}],
            "LocalSecondaryIndexes":[
                {"IndexName":"by-score",
                 "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                              {"AttributeName":"score","KeyType":"RANGE"}]}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    for i in 0..6 {
        let parity = if i % 2 == 0 { "even" } else { "odd" };
        let (status, body) = dynamo_retry(
            addrs[0],
            "DynamoDB_20120810.PutItem",
            &format!(
                r#"{{"TableName":"events","Item":{{
                    "pk":{{"S":"p1"}},"sk":{{"S":"a{i}"}},"cat":{{"S":"X"}},
                    "score":{{"S":"s{i}"}},"parity":{{"S":"{parity}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "PutItem(a{i}) failed: {body}");
    }
    (dir, nodes, addrs)
}

/// The order of `sk`s in a response body, by first appearance.
fn sk_order(body: &str) -> Vec<String> {
    let mut order = Vec::new();
    // Stop at `LastEvaluatedKey` — the cursor is itself an item key, so
    // scanning the whole body would count the page-boundary item twice.
    let mut rest = body
        .find("\"LastEvaluatedKey\":")
        .map_or(body, |at| &body[..at]);
    while let Some(at) = rest.find("\"sk\":{\"S\":\"") {
        let after = &rest[at + "\"sk\":{\"S\":\"".len()..];
        let endq = after.find('"').expect("closing quote");
        order.push(after[..endq].to_string());
        rest = &after[endq..];
    }
    order
}

/// Stand up a 3-node cluster with one table (`readings`, composite key
/// `pk`/`sk`, `sk` declared `N`) carrying a composite GSI (`by-device-value`,
/// hash `device`, sort `value`, `N`) and a composite LSI (`by-alt`, alt-sort
/// `alt`, `N`) — the identical fixture shape `dynamo_query_range.rs::setup`
/// uses, reproduced here (rather than shared) since that file's `setup` also
/// seeds a *different* value set into a *different* table name and this
/// suite's own `sk_order`/helpers are file-local. Every item in partition
/// `pk = "p1"` carries the same numeric text in `sk`/`value`/`alt`, mixed
/// digit counts, negatives, and a decimal — the exact shape a byte-text
/// compare gets wrong (ADR 0063).
async fn setup_n_sort_keys() -> (support::PanicSafeTempDir, Vec<Node>, Vec<SocketAddr>) {
    let dir = support::panic_safe_tempdir();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;
    let addrs: Vec<SocketAddr> = nodes.iter().map(Node::dynamo_addr).collect();

    let (status, body) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"readings","AttributeDefinitions":[{"AttributeName":"device","AttributeType":"S"},{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"N"},{"AttributeName":"value","AttributeType":"N"},{"AttributeName":"alt","AttributeType":"N"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-device-value",
                 "KeySchema":[{"AttributeName":"device","KeyType":"HASH"},
                              {"AttributeName":"value","KeyType":"RANGE"}],
                 "Projection":{"ProjectionType":"ALL"}}],
            "LocalSecondaryIndexes":[
                {"IndexName":"by-alt",
                 "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                              {"AttributeName":"alt","KeyType":"RANGE"}]}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    for sk in ["-10", "-5", "0", "2", "10", "100", "0.5"] {
        let (status, body) = dynamo_retry(
            addrs[0],
            "DynamoDB_20120810.PutItem",
            &format!(
                r#"{{"TableName":"readings","Item":{{
                    "pk":{{"S":"p1"}},"sk":{{"N":"{sk}"}},
                    "device":{{"S":"d1"}},"value":{{"N":"{sk}"}},
                    "alt":{{"N":"{sk}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "PutItem(sk={sk}) failed: {body}");
    }
    (dir, nodes, addrs)
}

/// The `field`'s `N` values in a response body, in **first-appearance
/// order** — the position-aware form an ordering assertion needs (mirroring
/// `sk_order` above, and `dynamo_query_range.rs`'s identical `sk_order` vs.
/// `sk_values` split: do NOT sort this, or an ordering assertion built on it
/// becomes vacuous).
fn n_order(body: &str, field: &str) -> Vec<String> {
    let marker = format!("\"{field}\":{{\"N\":\"");
    let mut out = Vec::new();
    // Stop at `LastEvaluatedKey` for the same reason `sk_order` does.
    let mut rest = body
        .find("\"LastEvaluatedKey\":")
        .map_or(body, |at| &body[..at]);
    while let Some(at) = rest.find(&marker) {
        let after = &rest[at + marker.len()..];
        let endq = after.find('"').expect("closing quote");
        out.push(after[..endq].to_string());
        rest = &after[endq..];
    }
    out
}

/// [`n_order`], numerically sorted — the order-**independent** form for a
/// membership assertion (a range predicate's result *set*), never for an
/// ordering assertion.
fn n_values_numeric_sorted(body: &str, field: &str) -> Vec<String> {
    let mut out = n_order(body, field);
    out.sort_by(|a, b| {
        a.parse::<f64>()
            .unwrap()
            .partial_cmp(&b.parse::<f64>().unwrap())
            .unwrap()
    });
    out
}

/// Descending reaches a **GSI** query, whose rows live in their own hidden
/// table. Materialized asynchronously, so this converges-or-times-out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn descending_applies_to_a_gsi_query() {
    let (_dir, nodes, addrs) = setup().await;

    let body = await_gsi_query(
        addrs[0],
        r#"{"TableName":"events","IndexName":"by-cat",
            "KeyConditionExpression":"cat = :c",
            "ExpressionAttributeValues":{":c":{"S":"X"}},
            "ScanIndexForward":false}"#,
        |got| sk_order(got).len() == 6,
    )
    .await;
    let order = sk_order(&body);
    assert_eq!(
        order,
        vec!["a5", "a4", "a3", "a2", "a1", "a0"],
        "GSI descending order: {body}"
    );
    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// **GSI `ScanIndexForward` is numeric order for an `N` sort key (ADR
/// 0063)**: the GSI row's own sort component (`index::gsi_row_key`) goes
/// through the same order-preserving `numkey` codec as the base table, and
/// its recovered sort segment (`index::parse_gsi_row_key`) is what
/// `matches_raw` filters against — so both the returned *order* and a range
/// `KeyConditionExpression`'s returned *set* must be numerically correct
/// across mixed digit counts, negatives, and a decimal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gsi_scan_index_forward_orders_n_sort_keys_numerically() {
    let (_dir, nodes, addrs) = setup_n_sort_keys().await;

    let asc = await_gsi_query(
        addrs[1],
        r#"{"TableName":"readings","IndexName":"by-device-value","ScanIndexForward":true,
            "KeyConditionExpression":"device = :d",
            "ExpressionAttributeValues":{":d":{"S":"d1"}}}"#,
        |b| n_order(b, "value").len() == 7,
    )
    .await;
    assert_eq!(
        n_order(&asc, "value"),
        vec!["-10", "-5", "0", "0.5", "2", "10", "100"],
        "GSI ScanIndexForward:true ascending numeric order: {asc}"
    );

    let desc = await_gsi_query(
        addrs[2],
        r#"{"TableName":"readings","IndexName":"by-device-value","ScanIndexForward":false,
            "KeyConditionExpression":"device = :d",
            "ExpressionAttributeValues":{":d":{"S":"d1"}}}"#,
        |b| n_order(b, "value").len() == 7,
    )
    .await;
    assert_eq!(
        n_order(&desc, "value"),
        vec!["100", "10", "2", "0.5", "0", "-5", "-10"],
        "GSI ScanIndexForward:false descending numeric order: {desc}"
    );

    // BETWEEN -5 AND 2: -5, 0, 0.5, 2 — not -10, 10, or 100.
    let between = await_gsi_query(
        addrs[0],
        r#"{"TableName":"readings","IndexName":"by-device-value",
            "KeyConditionExpression":"device = :d AND value BETWEEN :lo AND :hi",
            "ExpressionAttributeValues":{":d":{"S":"d1"},":lo":{"N":"-5"},":hi":{"N":"2"}}}"#,
        |b| n_order(b, "value").len() == 4,
    )
    .await;
    assert_eq!(
        n_values_numeric_sorted(&between, "value"),
        vec!["-5", "0", "0.5", "2"],
        "GSI value BETWEEN -5 AND 2: {between}"
    );

    // `<` 0: -10, -5 only — a byte-text compare would wrongly admit "0.5".
    let lt = await_gsi_query(
        addrs[1],
        r#"{"TableName":"readings","IndexName":"by-device-value",
            "KeyConditionExpression":"device = :d AND value < :v",
            "ExpressionAttributeValues":{":d":{"S":"d1"},":v":{"N":"0"}}}"#,
        |b| n_order(b, "value").len() == 2,
    )
    .await;
    assert_eq!(
        n_values_numeric_sorted(&lt, "value"),
        vec!["-10", "-5"],
        "GSI value < 0: {lt}"
    );

    // `>=` 10: 10, 100 — a byte-text compare would wrongly exclude "100"
    // (since `"100" < "10"` lexicographically is false but the multi-digit
    // shape is exactly what a naive compare gets wrong elsewhere in range
    // 1..=99) or admit "2".
    let ge = await_gsi_query(
        addrs[2],
        r#"{"TableName":"readings","IndexName":"by-device-value",
            "KeyConditionExpression":"device = :d AND value >= :v",
            "ExpressionAttributeValues":{":d":{"S":"d1"},":v":{"N":"10"}}}"#,
        |b| n_order(b, "value").len() == 2,
    )
    .await;
    assert_eq!(
        n_values_numeric_sorted(&ge, "value"),
        vec!["10", "100"],
        "GSI value >= 10: {ge}"
    );

    for n in nodes {
        n.shutdown_graceful().await;
    }
}
