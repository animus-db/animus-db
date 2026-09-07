//! End-to-end tests for `ExecuteStatement`'s PartiQL subset — `SELECT` (ADR
//! 0071, W-07 PR 2) and `INSERT`/`UPDATE`/`DELETE` (W-07 PR 3) — over the
//! real DynamoDB JSON/HTTP wire — proving the lowering this ADR builds
//! actually composes with the rest of the stack (schema resolution, GSI
//! drain, pagination, the ADR 0063 numeric key ordering, conditional
//! writes, throttling), not just the pure `partiql.rs` unit tests.
//!
//! Two tables share one cluster: `events` (composite key `pk`(S)/`sk`(N),
//! a `region` filter attribute, and a GSI `by-cat` on `cat`) covers
//! partition-equality → `Query`, sort-key comparators/`BETWEEN`, ORDER BY,
//! non-key `WHERE` → `Scan`-with-filter, projection, index `FROM`,
//! pagination/`NextToken`, and (PR 3) `INSERT`/`UPDATE`/`DELETE` including
//! `RETURNING` and a GSI-projected attribute update; `logs` (composite key
//! `pk`(S)/`sk`(S)) covers `begins_with` as a **sort-key** condition
//! (`events`' sort key is numeric, so it can't) and PR 3's `DELETE`.
//! A dedicated, self-contained cluster covers PR 3's throttling inheritance
//! (a fresh `PROVISIONED`-billing table needs its own fixture, so it
//! doesn't share `setup()`).

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{Node, bind_cluster, start_cluster};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

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

/// One DynamoDB JSON request over the real HTTP wire — `dynamo_table_ops.rs`'s
/// identical helper.
async fn dynamo(addr: SocketAddr, target: &str, body: &str) -> (u16, String) {
    let mut s = TcpStream::connect(addr).await.expect("connect");
    let req = format!(
        "POST / HTTP/1.1\r\nHost: x\r\nX-Amz-Target: {target}\r\n\
         Connection: close\r\n\
         Content-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes()).await.expect("write");
    s.flush().await.expect("flush");
    let mut raw = Vec::new();
    s.read_to_end(&mut raw).await.expect("read");
    let text = String::from_utf8(raw).expect("utf8");
    let (head, payload) = text.split_once("\r\n\r\n").expect("has body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse().ok())
        .expect("status line");
    (status, payload.to_string())
}

/// `dynamo`, retried on a transient `500` — see `dynamo_query_pagination.rs`'s
/// identical helper for the full rationale.
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

async fn execute_statement(addr: SocketAddr, body: &str) -> (u16, Value) {
    let (status, resp) = dynamo_retry(addr, "DynamoDB_20120810.ExecuteStatement", body).await;
    let json: Value =
        serde_json::from_str(&resp).unwrap_or_else(|e| panic!("response is not JSON: {e}: {resp}"));
    (status, json)
}

async fn query(addr: SocketAddr, body: &str) -> (u16, Value) {
    let (status, resp) = dynamo_retry(addr, "DynamoDB_20120810.Query", body).await;
    let json: Value =
        serde_json::from_str(&resp).unwrap_or_else(|e| panic!("response is not JSON: {e}: {resp}"));
    (status, json)
}

/// The `sk` values of a `Query`/`ExecuteStatement` response's `Items`, in
/// response order — `N` items parsed as `f64` (test-scale values only, no
/// precision concern), `S` items as their raw string.
fn item_sks(resp: &Value) -> Vec<String> {
    resp["Items"]
        .as_array()
        .expect("Items array")
        .iter()
        .map(|item| {
            if let Some(n) = item["sk"].get("N").and_then(Value::as_str) {
                n.to_string()
            } else {
                item["sk"]["S"].as_str().expect("sk").to_string()
            }
        })
        .collect()
}

/// Poll a GSI-backed `ExecuteStatement` until `accept` is satisfied — a GSI
/// is materialized asynchronously (ADR 0041 §4/§5), mirroring
/// `dynamo_query_pagination.rs`'s `await_gsi_query`.
async fn await_gsi_select(addr: SocketAddr, body: &str, accept: impl Fn(&Value) -> bool) -> Value {
    let converged = async {
        loop {
            let (status, got) = execute_statement(addr, body).await;
            if status == 200 && accept(&got) {
                return got;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(15), converged)
        .await
        .expect("GSI-backed SELECT never converged within 15s")
}

/// Stand up a 1-node cluster with `events` (`pk` S / `sk` N, `region` S,
/// `cat` S with GSI `by-cat`) and `logs` (`pk` S / `sk` S).
///
/// | events | pk | sk   | region | cat |
/// |--------|----|------|--------|-----|
/// |        | p1 | -5   | us     | X   |
/// |        | p1 | -1.5 | eu     | X   |
/// |        | p1 | 0    | us     | X   |
/// |        | p1 | 2    | eu     | X   |
/// |        | p1 | 4.5  | us     | X   |
/// |        | p1 | 10   | eu     | X   |
///
/// | logs | pk | sk     |
/// |------|----|--------|
/// |      | p1 | alpha  |
/// |      | p1 | alpha2 |
/// |      | p1 | beta   |
/// |      | p1 | gamma  |
async fn setup() -> (support::PanicSafeTempDir, Vec<Node>, SocketAddr) {
    let dir = support::panic_safe_tempdir();
    let bound = bind_cluster(1, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    let (status, body) = dynamo_retry(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"events","AttributeDefinitions":[
                {"AttributeName":"pk","AttributeType":"S"},
                {"AttributeName":"sk","AttributeType":"N"},
                {"AttributeName":"cat","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-cat",
                 "KeySchema":[{"AttributeName":"cat","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"ALL"}}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable events: {body}");

    let (status, body) = dynamo_retry(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"logs","AttributeDefinitions":[
                {"AttributeName":"pk","AttributeType":"S"},
                {"AttributeName":"sk","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable logs: {body}");

    for (sk, region) in [
        ("-5", "us"),
        ("-1.5", "eu"),
        ("0", "us"),
        ("2", "eu"),
        ("4.5", "us"),
        ("10", "eu"),
    ] {
        let item = format!(
            r#"{{"TableName":"events","Item":{{"pk":{{"S":"p1"}},"sk":{{"N":"{sk}"}},
                "region":{{"S":"{region}"}},"cat":{{"S":"X"}}}}}}"#
        );
        let (status, body) = dynamo_retry(addr, "DynamoDB_20120810.PutItem", &item).await;
        assert_eq!(status, 200, "PutItem events sk={sk}: {body}");
    }

    for sk in ["alpha", "alpha2", "beta", "gamma"] {
        let item =
            format!(r#"{{"TableName":"logs","Item":{{"pk":{{"S":"p1"}},"sk":{{"S":"{sk}"}}}}}}"#);
        let (status, body) = dynamo_retry(addr, "DynamoDB_20120810.PutItem", &item).await;
        assert_eq!(status, 200, "PutItem logs sk={sk}: {body}");
    }

    (dir, nodes, addr)
}

#[tokio::test(flavor = "multi_thread")]
async fn select_partition_equality_matches_query() {
    let (_dir, _nodes, addr) = setup().await;

    let (status, select_resp) = execute_statement(
        addr,
        r#"{"Statement":"SELECT * FROM events WHERE pk = ?",
            "Parameters":[{"S":"p1"}],"ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(status, 200, "{select_resp}");

    let (status, query_resp) = query(
        addr,
        r#"{"TableName":"events","KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}},"ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(status, 200, "{query_resp}");

    let mut select_sks = item_sks(&select_resp);
    let mut query_sks = item_sks(&query_resp);
    select_sks.sort_by(|a, b| {
        a.parse::<f64>()
            .unwrap()
            .total_cmp(&b.parse::<f64>().unwrap())
    });
    query_sks.sort_by(|a, b| {
        a.parse::<f64>()
            .unwrap()
            .total_cmp(&b.parse::<f64>().unwrap())
    });
    assert_eq!(select_sks, query_sks);
    assert_eq!(select_sks.len(), 6);
    assert!(select_resp.get("NextToken").is_none());
}

/// The numeric sort-key ordering (ADR 0063: negative, fractional, and
/// positive `N` values all compare correctly) — a `sk > ?` sort condition
/// and a `BETWEEN` both give the same item set/order `Query` gives.
#[tokio::test(flavor = "multi_thread")]
async fn select_sort_comparator_and_between_match_query_numeric_ordering() {
    let (_dir, _nodes, addr) = setup().await;

    // `sk > -1.5` should keep {0, 2, 4.5, 10}, ascending.
    let (status, gt_resp) = execute_statement(
        addr,
        r#"{"Statement":"SELECT * FROM events WHERE pk = ? AND sk > ?",
            "Parameters":[{"S":"p1"},{"N":"-1.5"}],"ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(status, 200, "{gt_resp}");
    assert_eq!(item_sks(&gt_resp), vec!["0", "2", "4.5", "10"]);

    // BETWEEN -5 AND 2 should keep {-5, -1.5, 0, 2}, ascending — matches
    // Query's own BETWEEN sort condition exactly.
    let (status, between_resp) = execute_statement(
        addr,
        r#"{"Statement":"SELECT * FROM events WHERE pk = ? AND sk BETWEEN ? AND ?",
            "Parameters":[{"S":"p1"},{"N":"-5"},{"N":"2"}],"ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(status, 200, "{between_resp}");

    let (status, query_between) = query(
        addr,
        r#"{"TableName":"events","KeyConditionExpression":"pk = :p AND sk BETWEEN :lo AND :hi",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":lo":{"N":"-5"},":hi":{"N":"2"}},
            "ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(status, 200, "{query_between}");
    assert_eq!(item_sks(&between_resp), item_sks(&query_between));
    assert_eq!(item_sks(&between_resp), vec!["-5", "-1.5", "0", "2"]);
}

/// `begins_with` as a **sort-key** condition (not just a filter) — `events`'
/// sort key is numeric, so this uses `logs` (`sk` is `S`).
#[tokio::test(flavor = "multi_thread")]
async fn select_begins_with_sort_key_matches_query() {
    let (_dir, _nodes, addr) = setup().await;

    let (status, select_resp) = execute_statement(
        addr,
        r#"{"Statement":"SELECT * FROM logs WHERE pk = ? AND begins_with(sk, ?)",
            "Parameters":[{"S":"p1"},{"S":"alpha"}],"ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(status, 200, "{select_resp}");
    assert_eq!(item_sks(&select_resp), vec!["alpha", "alpha2"]);

    let (status, query_resp) = query(
        addr,
        r#"{"TableName":"logs","KeyConditionExpression":"pk = :p AND begins_with(sk, :p2)",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":p2":{"S":"alpha"}},
            "ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(status, 200, "{query_resp}");
    assert_eq!(item_sks(&select_resp), item_sks(&query_resp));
}

/// A `WHERE` with no partition-key equality term lowers to `Scan` with a
/// filter (ADR 0071 §4.1) — matches a hand-built `Scan` exactly.
#[tokio::test(flavor = "multi_thread")]
async fn select_non_key_where_matches_scan_with_filter() {
    let (_dir, _nodes, addr) = setup().await;

    let (status, select_resp) = execute_statement(
        addr,
        r#"{"Statement":"SELECT * FROM events WHERE region = ?",
            "Parameters":[{"S":"us"}],"ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(status, 200, "{select_resp}");

    let (status, scan_resp) = execute_statement(
        addr,
        r#"{"Statement":"SELECT * FROM events WHERE region = ?",
            "Parameters":[{"S":"us"}],"ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(status, 200, "{scan_resp}");

    let (status, scan_json) = {
        let (status, resp) = dynamo_retry(
            addr,
            "DynamoDB_20120810.Scan",
            r#"{"TableName":"events","FilterExpression":"region = :r",
                "ExpressionAttributeValues":{":r":{"S":"us"}},"ConsistentRead":true}"#,
        )
        .await;
        (status, serde_json::from_str::<Value>(&resp).expect("json"))
    };
    assert_eq!(status, 200, "{scan_json}");

    let mut select_sks = item_sks(&select_resp);
    let mut scan_sks = item_sks(&scan_json);
    select_sks.sort_by(|a, b| {
        a.parse::<f64>()
            .unwrap()
            .total_cmp(&b.parse::<f64>().unwrap())
    });
    scan_sks.sort_by(|a, b| {
        a.parse::<f64>()
            .unwrap()
            .total_cmp(&b.parse::<f64>().unwrap())
    });
    assert_eq!(select_sks, scan_sks);
    assert_eq!(select_sks, vec!["-5", "0", "4.5"]);
}

/// `SELECT a, b` narrows the response the same way `ProjectionExpression`
/// does — only the named attributes come back.
#[tokio::test(flavor = "multi_thread")]
async fn select_projection_narrows_returned_attributes() {
    let (_dir, _nodes, addr) = setup().await;

    let (status, resp) = execute_statement(
        addr,
        r#"{"Statement":"SELECT pk, region FROM events WHERE pk = ? AND sk = ?",
            "Parameters":[{"S":"p1"},{"N":"0"}],"ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(status, 200, "{resp}");
    let items = resp["Items"].as_array().expect("Items");
    assert_eq!(items.len(), 1);
    let item = items[0].as_object().expect("item object");
    assert!(item.contains_key("pk"), "{item:?}");
    assert!(item.contains_key("region"), "{item:?}");
    assert!(!item.contains_key("sk"), "{item:?}");
    assert!(!item.contains_key("cat"), "{item:?}");
}

/// `FROM "table"."index"` queries the named GSI, converging once the drain
/// materializes it (ADR 0041 §4/§5).
#[tokio::test(flavor = "multi_thread")]
async fn select_from_table_dot_index_queries_the_gsi() {
    let (_dir, _nodes, addr) = setup().await;

    let body = r#"{"Statement":"SELECT * FROM \"events\".\"by-cat\" WHERE cat = ?",
                   "Parameters":[{"S":"X"}]}"#;
    let resp = await_gsi_select(addr, body, |v| {
        v["Items"].as_array().is_some_and(|a| a.len() == 6)
    })
    .await;
    assert_eq!(item_sks(&resp).len(), 6);
}

/// `Limit`+`NextToken` pagination walks the exact same item sequence a
/// `Query`'s own `Limit`/`ExclusiveStartKey` walk gives (ADR 0071 §9).
#[tokio::test(flavor = "multi_thread")]
async fn pagination_next_token_matches_query_last_evaluated_key_walk() {
    let (_dir, _nodes, addr) = setup().await;

    let statement = "SELECT * FROM events WHERE pk = ?";
    let mut select_sks = Vec::new();
    let mut next_token: Option<String> = None;
    for _ in 0..10 {
        let token_field = match &next_token {
            Some(t) => format!(",\"NextToken\":{}", serde_json::to_string(t).unwrap()),
            None => String::new(),
        };
        let body = format!(
            r#"{{"Statement":"{statement}","Parameters":[{{"S":"p1"}}],
                "ConsistentRead":true,"Limit":2{token_field}}}"#
        );
        let (status, resp) = execute_statement(addr, &body).await;
        assert_eq!(status, 200, "{resp}");
        select_sks.extend(item_sks(&resp));
        match resp.get("NextToken").and_then(Value::as_str) {
            Some(t) => next_token = Some(t.to_string()),
            None => break,
        }
    }

    let mut query_sks = Vec::new();
    let mut cursor: Option<Value> = None;
    for _ in 0..10 {
        let esk_field = match &cursor {
            Some(c) => format!(",\"ExclusiveStartKey\":{c}"),
            None => String::new(),
        };
        let body = format!(
            r#"{{"TableName":"events","KeyConditionExpression":"pk = :p",
                "ExpressionAttributeValues":{{":p":{{"S":"p1"}}}},
                "ConsistentRead":true,"Limit":2{esk_field}}}"#
        );
        let (status, resp) = query(addr, &body).await;
        assert_eq!(status, 200, "{resp}");
        query_sks.extend(item_sks(&resp));
        match resp.get("LastEvaluatedKey") {
            Some(k) if !k.is_null() => cursor = Some(k.clone()),
            _ => break,
        }
    }

    assert_eq!(select_sks, query_sks);
    assert_eq!(select_sks, vec!["-5", "-1.5", "0", "2", "4.5", "10"]);
}

/// A `NextToken` minted for one statement is rejected when replayed against
/// a different statement (ADR 0071 §9).
#[tokio::test(flavor = "multi_thread")]
async fn next_token_rejected_when_replayed_against_a_different_statement() {
    let (_dir, _nodes, addr) = setup().await;

    let (status, first) = execute_statement(
        addr,
        r#"{"Statement":"SELECT * FROM events WHERE pk = ?",
            "Parameters":[{"S":"p1"}],"ConsistentRead":true,"Limit":2}"#,
    )
    .await;
    assert_eq!(status, 200, "{first}");
    let token = first["NextToken"]
        .as_str()
        .expect("first page is truncated and carries a NextToken");

    let body = format!(
        r#"{{"Statement":"SELECT * FROM events WHERE pk = ? AND sk = ?",
            "Parameters":[{{"S":"p1"}},{{"N":"0"}}],"ConsistentRead":true,
            "NextToken":{}}}"#,
        serde_json::to_string(token).unwrap()
    );
    let (status, resp) = execute_statement(addr, &body).await;
    assert_eq!(status, 400, "{resp}");
    assert_eq!(
        resp["__type"],
        "com.amazonaws.dynamodb.v20120810#ValidationException"
    );
    assert!(
        resp["message"].as_str().unwrap().contains("does not match"),
        "{resp}"
    );
}

/// `ORDER BY sk DESC` reverses the walk, matching `Query`'s own
/// `ScanIndexForward: false`.
#[tokio::test(flavor = "multi_thread")]
async fn order_by_desc_matches_scan_index_forward_false() {
    let (_dir, _nodes, addr) = setup().await;

    let (status, resp) = execute_statement(
        addr,
        r#"{"Statement":"SELECT * FROM events WHERE pk = ? ORDER BY sk DESC",
            "Parameters":[{"S":"p1"}],"ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(status, 200, "{resp}");
    assert_eq!(item_sks(&resp), vec!["10", "4.5", "2", "0", "-1.5", "-5"]);
}

/// A literal value in `WHERE` (instead of a `?` placeholder) is rejected —
/// ADR 0071 §2's placeholder-only discipline, enforced end to end.
#[tokio::test(flavor = "multi_thread")]
async fn literal_value_in_where_is_rejected() {
    let (_dir, _nodes, addr) = setup().await;

    let (status, resp) = execute_statement(
        addr,
        r#"{"Statement":"SELECT * FROM events WHERE pk = 'p1'"}"#,
    )
    .await;
    assert_eq!(status, 400, "{resp}");
    assert_eq!(
        resp["__type"],
        "com.amazonaws.dynamodb.v20120810#ValidationException"
    );
    assert!(
        resp["message"].as_str().unwrap().contains("literal values"),
        "{resp}"
    );
}

/// A malformed statement is a `ValidationException`, not a 500 or a panic.
#[tokio::test(flavor = "multi_thread")]
async fn malformed_statement_is_a_validation_exception() {
    let (_dir, _nodes, addr) = setup().await;

    let (status, resp) = execute_statement(addr, r#"{"Statement":"NOT EVEN SQL"}"#).await;
    assert_eq!(status, 400, "{resp}");
    assert_eq!(
        resp["__type"],
        "com.amazonaws.dynamodb.v20120810#ValidationException"
    );
}

/// A statement naming a table that doesn't exist is `ResourceNotFoundException`
/// (through the same catalog check every other operation uses), not a
/// PartiQL-specific error shape.
#[tokio::test(flavor = "multi_thread")]
async fn unknown_table_is_resource_not_found() {
    let (_dir, _nodes, addr) = setup().await;

    let (status, resp) = execute_statement(
        addr,
        r#"{"Statement":"SELECT * FROM nope WHERE pk = ?","Parameters":[{"S":"p1"}]}"#,
    )
    .await;
    assert_eq!(status, 400, "{resp}");
    assert_eq!(
        resp["__type"],
        "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException"
    );
}

// ---------------------------------------------------------------------------
// PR 3: INSERT/UPDATE/DELETE (ADR 0071, W-07 PR 3 of 5)
// ---------------------------------------------------------------------------

/// `INSERT` then `SELECT` sees it — no `RETURNING` means an empty `Items`
/// array on the `INSERT` itself (never omitted).
#[tokio::test(flavor = "multi_thread")]
async fn insert_then_select_sees_it() {
    let (_dir, _nodes, addr) = setup().await;

    let (status, resp) = execute_statement(
        addr,
        r#"{"Statement":"INSERT INTO events VALUE {'pk': ?, 'sk': ?, 'region': ?}",
            "Parameters":[{"S":"p2"},{"N":"1"},{"S":"eu"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "{resp}");
    assert!(resp["Items"].as_array().unwrap().is_empty());

    let (status, sel) = execute_statement(
        addr,
        r#"{"Statement":"SELECT * FROM events WHERE pk = ? AND sk = ?",
            "Parameters":[{"S":"p2"},{"N":"1"}],"ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(status, 200, "{sel}");
    assert_eq!(item_sks(&sel), vec!["1"]);
    assert_eq!(sel["Items"][0]["region"]["S"], "eu");
}

/// A second `INSERT` at the same key is `DuplicateItemException` and leaves
/// the first item's attributes unchanged (ADR 0071's lowering table:
/// `attribute_not_exists(pk)` maps the underlying `ConditionalCheckFailedException`).
#[tokio::test(flavor = "multi_thread")]
async fn duplicate_insert_gives_duplicate_item_exception_and_leaves_item_unchanged() {
    let (_dir, _nodes, addr) = setup().await;

    let (status, resp) = execute_statement(
        addr,
        r#"{"Statement":"INSERT INTO events VALUE {'pk': ?, 'sk': ?, 'region': ?}",
            "Parameters":[{"S":"p3"},{"N":"1"},{"S":"eu"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "{resp}");

    let (status, dup) = execute_statement(
        addr,
        r#"{"Statement":"INSERT INTO events VALUE {'pk': ?, 'sk': ?, 'region': ?}",
            "Parameters":[{"S":"p3"},{"N":"1"},{"S":"us"}]}"#,
    )
    .await;
    assert_eq!(status, 400, "{dup}");
    assert_eq!(
        dup["__type"],
        "com.amazonaws.dynamodb.v20120810#DuplicateItemException"
    );

    let (status, sel) = execute_statement(
        addr,
        r#"{"Statement":"SELECT region FROM events WHERE pk = ? AND sk = ?",
            "Parameters":[{"S":"p3"},{"N":"1"}],"ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(status, 200, "{sel}");
    let items = sel["Items"].as_array().expect("Items");
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0]["region"]["S"], "eu",
        "the original item's value must survive the rejected duplicate INSERT"
    );
}

/// `ON CONFLICT DO NOTHING` swallows the duplicate-key failure as a silent
/// no-op instead of raising `DuplicateItemException`.
#[tokio::test(flavor = "multi_thread")]
async fn insert_on_conflict_do_nothing_swallows_duplicate() {
    let (_dir, _nodes, addr) = setup().await;

    let (status, _) = execute_statement(
        addr,
        r#"{"Statement":"INSERT INTO events VALUE {'pk': ?, 'sk': ?, 'region': ?}",
            "Parameters":[{"S":"p4"},{"N":"1"},{"S":"eu"}]}"#,
    )
    .await;
    assert_eq!(status, 200);

    let (status, resp) = execute_statement(
        addr,
        r#"{"Statement":"INSERT INTO events VALUE {'pk': ?, 'sk': ?, 'region': ?} ON CONFLICT DO NOTHING",
            "Parameters":[{"S":"p4"},{"N":"1"},{"S":"us"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "{resp}");
    assert!(resp["Items"].as_array().unwrap().is_empty());

    // The original item's value must survive.
    let (status, sel) = execute_statement(
        addr,
        r#"{"Statement":"SELECT region FROM events WHERE pk = ? AND sk = ?",
            "Parameters":[{"S":"p4"},{"N":"1"}],"ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(status, 200, "{sel}");
    assert_eq!(sel["Items"][0]["region"]["S"], "eu");
}

/// `UPDATE ... SET ... RETURNING ALL NEW *` on an existing item echoes the
/// post-update image.
#[tokio::test(flavor = "multi_thread")]
async fn update_set_on_existing_item_with_returning_all_new() {
    let (_dir, _nodes, addr) = setup().await;

    let (status, resp) = execute_statement(
        addr,
        r#"{"Statement":"UPDATE events SET region = ? WHERE pk = ? AND sk = ? RETURNING ALL NEW *",
            "Parameters":[{"S":"apac"},{"S":"p1"},{"N":"0"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "{resp}");
    let items = resp["Items"].as_array().expect("Items");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["region"]["S"], "apac");
    assert_eq!(items[0]["pk"]["S"], "p1");
}

/// `UPDATE` of a key that doesn't exist fails — the implicit
/// `attribute_exists(pk)` condition (ADR 0071's lowering table).
#[tokio::test(flavor = "multi_thread")]
async fn update_of_missing_item_fails() {
    let (_dir, _nodes, addr) = setup().await;

    let (status, resp) = execute_statement(
        addr,
        r#"{"Statement":"UPDATE events SET region = ? WHERE pk = ? AND sk = ?",
            "Parameters":[{"S":"apac"},{"S":"nope"},{"N":"999"}]}"#,
    )
    .await;
    assert_eq!(status, 400, "{resp}");
    assert_eq!(
        resp["__type"],
        "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException"
    );
}

/// A non-key `WHERE` term becomes the `UpdateItem`'s own `ConditionExpression`
/// — both the met and unmet case.
#[tokio::test(flavor = "multi_thread")]
async fn update_with_non_key_where_term_as_condition_met_and_unmet() {
    let (_dir, _nodes, addr) = setup().await;

    // Met: sk=4.5's region is "us" per the fixture.
    let (status, resp) = execute_statement(
        addr,
        r#"{"Statement":"UPDATE events SET region = ? WHERE pk = ? AND sk = ? AND region = ? RETURNING ALL OLD *",
            "Parameters":[{"S":"apac"},{"S":"p1"},{"N":"4.5"},{"S":"us"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "{resp}");
    let items = resp["Items"].as_array().expect("Items");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["region"]["S"], "us");

    // Unmet: region is now "apac", not "us" any more.
    let (status, resp) = execute_statement(
        addr,
        r#"{"Statement":"UPDATE events SET region = ? WHERE pk = ? AND sk = ? AND region = ?",
            "Parameters":[{"S":"eu"},{"S":"p1"},{"N":"4.5"},{"S":"us"}]}"#,
    )
    .await;
    assert_eq!(status, 400, "{resp}");
    assert_eq!(
        resp["__type"],
        "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException"
    );
}

/// `DELETE ... RETURNING ALL OLD *` echoes the deleted item and the item is
/// actually gone afterward.
#[tokio::test(flavor = "multi_thread")]
async fn delete_with_returning_all_old() {
    let (_dir, _nodes, addr) = setup().await;

    let (status, resp) = execute_statement(
        addr,
        r#"{"Statement":"DELETE FROM logs WHERE pk = ? AND sk = ? RETURNING ALL OLD *",
            "Parameters":[{"S":"p1"},{"S":"beta"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "{resp}");
    let items = resp["Items"].as_array().expect("Items");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["sk"]["S"], "beta");

    let (status, sel) = execute_statement(
        addr,
        r#"{"Statement":"SELECT * FROM logs WHERE pk = ? AND sk = ?",
            "Parameters":[{"S":"p1"},{"S":"beta"}],"ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(status, 200, "{sel}");
    assert!(sel["Items"].as_array().unwrap().is_empty());
}

/// `DELETE` of a missing key is a silent success (no implicit existence
/// condition, matching plain `DeleteItem`'s own AWS semantics) — empty
/// `Items`, never an error.
#[tokio::test(flavor = "multi_thread")]
async fn delete_of_missing_key_is_a_silent_success() {
    let (_dir, _nodes, addr) = setup().await;

    let (status, resp) = execute_statement(
        addr,
        r#"{"Statement":"DELETE FROM logs WHERE pk = ? AND sk = ?",
            "Parameters":[{"S":"nope"},{"S":"nope"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "{resp}");
    assert!(resp["Items"].as_array().unwrap().is_empty());
}

/// A GSI-projected attribute (`cat`) updated via PartiQL `UPDATE` is visible
/// through a `SELECT` on the index — index maintenance is inherited from
/// the ordinary `UpdateItem` write path, not reimplemented.
#[tokio::test(flavor = "multi_thread")]
async fn gsi_projected_attribute_updated_via_partiql_is_visible_through_index_query() {
    let (_dir, _nodes, addr) = setup().await;

    let (status, resp) = execute_statement(
        addr,
        r#"{"Statement":"UPDATE events SET cat = ? WHERE pk = ? AND sk = ?",
            "Parameters":[{"S":"Y"},{"S":"p1"},{"N":"2"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "{resp}");

    let body = r#"{"Statement":"SELECT * FROM \"events\".\"by-cat\" WHERE cat = ?",
                   "Parameters":[{"S":"Y"}]}"#;
    let resp = await_gsi_select(addr, body, |v| {
        v["Items"].as_array().is_some_and(|a| a.len() == 1)
    })
    .await;
    assert_eq!(item_sks(&resp), vec!["2"]);
}

/// `WHERE` missing the partition key entirely (an `UPDATE`/`DELETE` can
/// never widen into a scan-and-mutate) is a `ValidationException`.
#[tokio::test(flavor = "multi_thread")]
async fn update_where_missing_partition_key_is_a_validation_exception() {
    let (_dir, _nodes, addr) = setup().await;

    let (status, resp) = execute_statement(
        addr,
        r#"{"Statement":"UPDATE events SET region = ? WHERE region = ?",
            "Parameters":[{"S":"apac"},{"S":"us"}]}"#,
    )
    .await;
    assert_eq!(status, 400, "{resp}");
    assert_eq!(
        resp["__type"],
        "com.amazonaws.dynamodb.v20120810#ValidationException"
    );
    assert!(
        resp["message"].as_str().unwrap().contains("partition key"),
        "{resp}"
    );
}

/// A missing sort-key term on a composite-key table is likewise rejected
/// rather than silently narrowed to a partial-key operation.
#[tokio::test(flavor = "multi_thread")]
async fn delete_where_missing_sort_key_is_a_validation_exception() {
    let (_dir, _nodes, addr) = setup().await;

    let (status, resp) = execute_statement(
        addr,
        r#"{"Statement":"DELETE FROM events WHERE pk = ?","Parameters":[{"S":"p1"}]}"#,
    )
    .await;
    assert_eq!(status, 400, "{resp}");
    assert_eq!(
        resp["__type"],
        "com.amazonaws.dynamodb.v20120810#ValidationException"
    );
    assert!(
        resp["message"].as_str().unwrap().contains("sort key"),
        "{resp}"
    );
}

/// `RETURNING ALL NEW *` on `DELETE` is rejected at parse time — there is no
/// new image for a delete.
#[tokio::test(flavor = "multi_thread")]
async fn delete_returning_all_new_is_rejected() {
    let (_dir, _nodes, addr) = setup().await;

    let (status, resp) = execute_statement(
        addr,
        r#"{"Statement":"DELETE FROM logs WHERE pk = ? AND sk = ? RETURNING ALL NEW *",
            "Parameters":[{"S":"p1"},{"S":"gamma"}]}"#,
    )
    .await;
    assert_eq!(status, 400, "{resp}");
    assert_eq!(
        resp["__type"],
        "com.amazonaws.dynamodb.v20120810#ValidationException"
    );
}

/// A `PROVISIONED`-billing table's write budget throttles a PartiQL
/// `INSERT` exactly like a client-built `PutItem` would (ADR 0065 — the
/// throttling check lives at the leader-evaluated write funnel, which the
/// lowered `PutItem` (an `INSERT` always carries a condition, so it always
/// takes that path) reaches unmodified).
#[tokio::test(flavor = "multi_thread")]
async fn throttled_table_throttles_a_partiql_insert() {
    let dir = support::panic_safe_tempdir();
    let bound = bind_cluster(1, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    let (status, body) = dynamo_retry(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"thr","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
            "BillingMode":"PROVISIONED",
            "ProvisionedThroughput":{"ReadCapacityUnits":1,"WriteCapacityUnits":1}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable(thr): {body}");

    // A big value costs many write-capacity-units per item, so a handful of
    // real HTTP round trips exhausts the 300-unit burst window — the same
    // "big value, few items" shortcut `dynamo_throttling.rs` uses.
    let big = "x".repeat(256 * 1024);
    let mut throttled = false;
    for i in 0..8 {
        let stmt = format!(
            r#"{{"Statement":"INSERT INTO thr VALUE {{'pk': ?, 'v': ?}}",
                "Parameters":[{{"S":"k{i}"}},{{"S":"{big}"}}]}}"#
        );
        let (status, resp) = execute_statement(addr, &stmt).await;
        if status == 400
            && resp["__type"]
                == "com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException"
        {
            throttled = true;
            break;
        }
        assert_eq!(status, 200, "INSERT #{i} unexpectedly failed: {resp}");
    }
    assert!(
        throttled,
        "expected a PartiQL INSERT to be throttled once the write budget was exhausted"
    );
}
