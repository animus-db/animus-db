//! End-to-end tests for `ReturnConsumedCapacity` over the real DynamoDB
//! JSON/HTTP wire (ADR 0006).
//!
//! The fixture is built so the three charges are *individually* readable rather
//! than only in aggregate. One table, one big item:
//!
//! - the **base** item is ~1.5 KB, so it costs 2 write units, not 1;
//! - the **LSI** projects `ALL`, so its row is the same size and costs 2 as
//!   well;
//! - the **GSI** projects `KEYS_ONLY`, so its row is ~11 bytes and costs 1.
//!
//! That 2/2/1 split is the point. If each index were charged on the *base*
//! item's size rather than on its own row — the obvious wrong implementation —
//! the GSI would report 2 and the total 6, and every assertion here would move
//! together. They are pinned separately so it cannot regress quietly.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{Node, bind_cluster, start_cluster};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

/// The oversized attribute that pushes the base item past one write unit.
const BLOB: &str = concat!(
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "0123456789012345678901234567890123456789012345678901234567890123",
);

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

/// `dynamo`, retried on a retryable `500 InternalServerError` for up to 20s.
/// See `dynamo_index_scan.rs`'s identical helper for the rationale (the CP data
/// plane's transient "not the leader here" refusal surfaces as a clean `500`).
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

/// The parsed `ConsumedCapacity` of a successful request.
async fn capacity_of(addr: SocketAddr, target: &str, body: &str) -> Value {
    let (status, resp) = dynamo_retry(addr, target, body).await;
    assert_eq!(status, 200, "{target} failed: {resp}");
    let parsed: Value = serde_json::from_str(&resp).expect("json response");
    parsed
        .get("ConsumedCapacity")
        .unwrap_or_else(|| panic!("{target} returned no ConsumedCapacity: {resp}"))
        .clone()
}

/// A 3-node cluster with table `caps` (composite `pk`/`sk`), a `KEYS_ONLY` GSI
/// on `cat` and an `ALL`-projecting LSI on `score`.
async fn setup() -> (tempfile::TempDir, Vec<Node>, Vec<SocketAddr>) {
    let dir = tempfile::tempdir().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;
    let addrs: Vec<SocketAddr> = nodes.iter().map(Node::dynamo_addr).collect();

    let (status, body) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"caps",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-cat",
                 "KeySchema":[{"AttributeName":"cat","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"KEYS_ONLY"}}],
            "LocalSecondaryIndexes":[
                {"IndexName":"by-score",
                 "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                              {"AttributeName":"score","KeyType":"RANGE"}],
                 "Projection":{"ProjectionType":"ALL"}}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    (dir, nodes, addrs)
}

/// The big item's body, as a `PutItem` `Item` fragment.
fn big_item(sk: &str) -> String {
    format!(
        r#"{{"pk":{{"S":"p1"}},"sk":{{"S":"{sk}"}},
             "cat":{{"S":"X"}},"score":{{"S":"s1"}},
             "blob":{{"S":"{BLOB}"}}}}"#
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn no_consumed_capacity_is_reported_unless_it_was_asked_for() {
    let (_dir, _nodes, addrs) = setup().await;

    // The default is `NONE`, and `NONE` means the field is absent entirely —
    // not present-and-zero. A client that never asked must see no change.
    for (target, body) in [
        (
            "DynamoDB_20120810.PutItem",
            format!(r#"{{"TableName":"caps","Item":{}}}"#, big_item("a0")),
        ),
        (
            "DynamoDB_20120810.GetItem",
            r#"{"TableName":"caps","Key":{"pk":{"S":"p1"},"sk":{"S":"a0"}}}"#.to_string(),
        ),
        (
            "DynamoDB_20120810.UpdateItem",
            r#"{"TableName":"caps","Key":{"pk":{"S":"p1"},"sk":{"S":"a0"}},
                "UpdateExpression":"SET note = :v",
                "ExpressionAttributeValues":{":v":{"S":"hi"}}}"#
                .to_string(),
        ),
        (
            "DynamoDB_20120810.DeleteItem",
            r#"{"TableName":"caps","Key":{"pk":{"S":"p1"},"sk":{"S":"a0"}}}"#.to_string(),
        ),
    ] {
        let (status, resp) = dynamo_retry(addrs[0], target, &body).await;
        assert_eq!(status, 200, "{target} failed: {resp}");
        assert!(
            !resp.contains("ConsumedCapacity"),
            "{target} reported capacity nobody asked for: {resp}"
        );
    }

    // ...and an unrecognised level is refused rather than quietly downgraded.
    let (status, resp) = dynamo(
        addrs[0],
        "DynamoDB_20120810.GetItem",
        r#"{"TableName":"caps","Key":{"pk":{"S":"p1"},"sk":{"S":"a0"}},
            "ReturnConsumedCapacity":"SOMETIMES"}"#,
    )
    .await;
    assert_eq!(status, 400, "{resp}");
    assert!(resp.contains("ValidationException"), "{resp}");
}

#[tokio::test(flavor = "multi_thread")]
async fn total_aggregates_the_table_and_its_indexes_into_one_number() {
    let (_dir, _nodes, addrs) = setup().await;

    let cc = capacity_of(
        addrs[0],
        "DynamoDB_20120810.PutItem",
        &format!(
            r#"{{"TableName":"caps","Item":{},"ReturnConsumedCapacity":"TOTAL"}}"#,
            big_item("a0")
        ),
    )
    .await;

    assert_eq!(cc["TableName"], "caps");
    // base 2 + LSI(ALL) 2 + GSI(KEYS_ONLY) 1.
    assert_eq!(cc["CapacityUnits"], 5.0, "{cc}");
    // `TOTAL` is the aggregate and nothing else — no breakdown leaks in.
    assert!(cc.get("Table").is_none(), "{cc}");
    assert!(cc.get("GlobalSecondaryIndexes").is_none(), "{cc}");
    assert!(cc.get("LocalSecondaryIndexes").is_none(), "{cc}");
}

#[tokio::test(flavor = "multi_thread")]
async fn indexes_charges_each_index_on_its_own_row_not_on_the_base_item() {
    let (_dir, _nodes, addrs) = setup().await;

    let cc = capacity_of(
        addrs[0],
        "DynamoDB_20120810.PutItem",
        &format!(
            r#"{{"TableName":"caps","Item":{},"ReturnConsumedCapacity":"INDEXES"}}"#,
            big_item("a0")
        ),
    )
    .await;

    // The aggregate agrees with `TOTAL`, and the breakdown sums to it.
    assert_eq!(cc["CapacityUnits"], 5.0, "{cc}");
    assert_eq!(cc["Table"]["CapacityUnits"], 2.0, "{cc}");
    // The `ALL`-projecting LSI carries the whole item, so it costs what the
    // table costs...
    assert_eq!(
        cc["LocalSecondaryIndexes"]["by-score"]["CapacityUnits"],
        2.0
    );
    // ...while the `KEYS_ONLY` GSI carries three short keys and costs the
    // floor. This is the assertion that would fail if index rows were charged
    // on the base item's size.
    assert_eq!(cc["GlobalSecondaryIndexes"]["by-cat"]["CapacityUnits"], 1.0);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_item_that_is_not_indexed_is_charged_for_no_index() {
    let (_dir, _nodes, addrs) = setup().await;

    // No `cat`, no `score`: this item materializes no index row at all, so it
    // must be charged for none. Reporting a charge for a row that was never
    // written is the kind of wrong a client can never detect on its own.
    let cc = capacity_of(
        addrs[0],
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"caps",
            "Item":{"pk":{"S":"p1"},"sk":{"S":"bare"},"note":{"S":"x"}},
            "ReturnConsumedCapacity":"INDEXES"}"#,
    )
    .await;

    assert_eq!(cc["CapacityUnits"], 1.0, "{cc}");
    assert_eq!(cc["Table"]["CapacityUnits"], 1.0, "{cc}");
    assert!(cc.get("GlobalSecondaryIndexes").is_none(), "{cc}");
    assert!(cc.get("LocalSecondaryIndexes").is_none(), "{cc}");

    // Half-indexed: `cat` present but `score` absent charges the GSI only.
    let cc = capacity_of(
        addrs[0],
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"caps",
            "Item":{"pk":{"S":"p1"},"sk":{"S":"half"},"cat":{"S":"X"}},
            "ReturnConsumedCapacity":"INDEXES"}"#,
    )
    .await;
    assert_eq!(cc["GlobalSecondaryIndexes"]["by-cat"]["CapacityUnits"], 1.0);
    assert!(cc.get("LocalSecondaryIndexes").is_none(), "{cc}");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_eventually_consistent_read_is_charged_half_a_unit() {
    let (_dir, _nodes, addrs) = setup().await;

    let (status, body) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.PutItem",
        &format!(r#"{{"TableName":"caps","Item":{}}}"#, big_item("a0")),
    )
    .await;
    assert_eq!(status, 200, "seed failed: {body}");

    let key = r#""Key":{"pk":{"S":"p1"},"sk":{"S":"a0"}}"#;

    // `ConsistentRead` defaults to false, and DynamoDB bills an eventually-
    // consistent read at half price. Our base read is linearizable either way
    // (ADR 0041 §5), but a client that asked for the cheap read is told it got
    // the cheap read.
    let cc = capacity_of(
        addrs[0],
        "DynamoDB_20120810.GetItem",
        &format!(r#"{{"TableName":"caps",{key},"ReturnConsumedCapacity":"TOTAL"}}"#),
    )
    .await;
    assert_eq!(cc["CapacityUnits"], 0.5, "{cc}");

    let cc = capacity_of(
        addrs[0],
        "DynamoDB_20120810.GetItem",
        &format!(
            r#"{{"TableName":"caps",{key},"ConsistentRead":true,
                 "ReturnConsumedCapacity":"TOTAL"}}"#
        ),
    )
    .await;
    assert_eq!(cc["CapacityUnits"], 1.0, "{cc}");

    // A read is charged against the table alone — reading an item never
    // touches its index rows.
    let cc = capacity_of(
        addrs[0],
        "DynamoDB_20120810.GetItem",
        &format!(
            r#"{{"TableName":"caps",{key},"ConsistentRead":true,
                 "ReturnConsumedCapacity":"INDEXES"}}"#
        ),
    )
    .await;
    assert_eq!(cc["Table"]["CapacityUnits"], 1.0, "{cc}");
    assert!(cc.get("GlobalSecondaryIndexes").is_none(), "{cc}");

    // A projection narrows the response but not the cost: DynamoDB reads the
    // whole item and projects on the way out.
    let cc = capacity_of(
        addrs[0],
        "DynamoDB_20120810.GetItem",
        &format!(
            r#"{{"TableName":"caps",{key},"ConsistentRead":true,
                 "ProjectionExpression":"pk",
                 "ReturnConsumedCapacity":"TOTAL"}}"#
        ),
    )
    .await;
    assert_eq!(cc["CapacityUnits"], 1.0, "{cc}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_delete_is_charged_on_the_item_it_removed() {
    let (_dir, _nodes, addrs) = setup().await;

    let (status, body) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.PutItem",
        &format!(r#"{{"TableName":"caps","Item":{}}}"#, big_item("a0")),
    )
    .await;
    assert_eq!(status, 200, "seed failed: {body}");

    // Removing the big indexed item costs exactly what writing it did: the
    // base row and both index rows all have to be written away. Charging the
    // one-unit floor here — which is all the write path's fast arm could know
    // without reading — would understate this by 4 units.
    let cc = capacity_of(
        addrs[0],
        "DynamoDB_20120810.DeleteItem",
        r#"{"TableName":"caps","Key":{"pk":{"S":"p1"},"sk":{"S":"a0"}},
            "ReturnConsumedCapacity":"INDEXES"}"#,
    )
    .await;
    assert_eq!(cc["CapacityUnits"], 5.0, "{cc}");
    assert_eq!(cc["Table"]["CapacityUnits"], 2.0, "{cc}");
    assert_eq!(
        cc["LocalSecondaryIndexes"]["by-score"]["CapacityUnits"],
        2.0
    );
    assert_eq!(cc["GlobalSecondaryIndexes"]["by-cat"]["CapacityUnits"], 1.0);

    // Deleting a key that was never there is still a write, and still the
    // floor — never zero, and never an error.
    let cc = capacity_of(
        addrs[0],
        "DynamoDB_20120810.DeleteItem",
        r#"{"TableName":"caps","Key":{"pk":{"S":"p1"},"sk":{"S":"ghost"}},
            "ReturnConsumedCapacity":"INDEXES"}"#,
    )
    .await;
    assert_eq!(cc["CapacityUnits"], 1.0, "{cc}");
    assert!(cc.get("GlobalSecondaryIndexes").is_none(), "{cc}");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_update_is_charged_on_the_larger_of_the_two_images() {
    let (_dir, _nodes, addrs) = setup().await;

    // Start small.
    let (status, body) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"caps",
            "Item":{"pk":{"S":"p1"},"sk":{"S":"u"},"cat":{"S":"X"},"score":{"S":"s1"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "seed failed: {body}");

    // Growing past 1 KB is charged on the *new*, larger image.
    let cc = capacity_of(
        addrs[0],
        "DynamoDB_20120810.UpdateItem",
        &format!(
            r#"{{"TableName":"caps","Key":{{"pk":{{"S":"p1"}},"sk":{{"S":"u"}}}},
                 "UpdateExpression":"SET blob = :b",
                 "ExpressionAttributeValues":{{":b":{{"S":"{BLOB}"}}}},
                 "ReturnConsumedCapacity":"INDEXES"}}"#
        ),
    )
    .await;
    assert_eq!(cc["Table"]["CapacityUnits"], 2.0, "{cc}");
    assert_eq!(
        cc["LocalSecondaryIndexes"]["by-score"]["CapacityUnits"],
        2.0
    );

    // Shrinking back is charged on the *old*, larger image — the write still
    // had to move the bigger of the two. Charging the new image here would let
    // a caller shrink an item for a discount it never earned.
    let cc = capacity_of(
        addrs[0],
        "DynamoDB_20120810.UpdateItem",
        r#"{"TableName":"caps","Key":{"pk":{"S":"p1"},"sk":{"S":"u"}},
            "UpdateExpression":"REMOVE blob",
            "ReturnConsumedCapacity":"INDEXES"}"#,
    )
    .await;
    assert_eq!(cc["Table"]["CapacityUnits"], 2.0, "{cc}");
}
