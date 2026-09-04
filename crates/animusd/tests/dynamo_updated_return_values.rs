//! End-to-end tests for `UpdateItem`'s `UPDATED_OLD` / `UPDATED_NEW` return
//! values over the real DynamoDB JSON/HTTP wire.
//!
//! These two differ from `ALL_OLD`/`ALL_NEW` by reporting **only the
//! attributes the update actually changed**, which makes them a diff of the
//! two images rather than a projection of one. The asymmetry is the part worth
//! pinning: an attribute the update *created* has no previous value, so
//! `UPDATED_OLD` omits it, and one it *removed* has no new value, so
//! `UPDATED_NEW` omits it.

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
/// | sk | cat | score | parity | seq (N) |
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
                    "score":{{"S":"s{i}"}},"parity":{{"S":"{parity}"}},
                    "seq":{{"N":"{i}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "PutItem(a{i}) failed: {body}");
    }
    (dir, nodes, addrs)
}

/// The diff, both directions, in one update: one attribute edited, one
/// created, one removed, one left alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn updated_old_and_new_report_only_what_changed() {
    let (_dir, nodes, addrs) = setup().await;

    // Seed an item with a `doomed` attribute the update will remove.
    let (status, seeded) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"events","Item":{"pk":{"S":"p1"},"sk":{"S":"a0"},
            "cat":{"S":"X"},"doomed":{"S":"bye"},"parity":{"S":"even"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "seed failed: {seeded}");

    let (status, old) = dynamo_retry(
        addrs[1],
        "DynamoDB_20120810.UpdateItem",
        r#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a0"}},
            "UpdateExpression":"SET cat = :y, born = :b REMOVE doomed",
            "ExpressionAttributeValues":{":y":{"S":"Y"},":b":{"S":"new"}},
            "ReturnValues":"UPDATED_OLD"}"#,
    )
    .await;
    assert_eq!(status, 200, "UPDATED_OLD failed: {old}");
    assert!(
        old.contains(r#""cat":{"S":"X"}"#),
        "the edited attribute's old value: {old}"
    );
    assert!(
        old.contains(r#""doomed":{"S":"bye"}"#),
        "a removed attribute has an old value: {old}"
    );
    assert!(
        !old.contains("born"),
        "a created attribute has no old value: {old}"
    );
    assert!(
        !old.contains("parity"),
        "an untouched attribute is not reported: {old}"
    );
    assert!(!old.contains(r#""sk""#), "the key never changes: {old}");

    // Now the same shape in the other direction.
    let (status, new) = dynamo_retry(
        addrs[2],
        "DynamoDB_20120810.UpdateItem",
        r#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a0"}},
            "UpdateExpression":"SET cat = :z, later = :l REMOVE born",
            "ExpressionAttributeValues":{":z":{"S":"Z"},":l":{"S":"yes"}},
            "ReturnValues":"UPDATED_NEW"}"#,
    )
    .await;
    assert_eq!(status, 200, "UPDATED_NEW failed: {new}");
    assert!(
        new.contains(r#""cat":{"S":"Z"}"#),
        "the edited attribute's new value: {new}"
    );
    assert!(
        new.contains(r#""later":{"S":"yes"}"#),
        "a created attribute has a new value: {new}"
    );
    assert!(
        !new.contains("born"),
        "a removed attribute has no new value: {new}"
    );
    assert!(!new.contains("parity"), "{new}");

    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// An update that changes nothing reports no `Attributes` at all, rather than
/// an empty map.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_update_that_changes_nothing_reports_no_attributes() {
    let (_dir, nodes, addrs) = setup().await;

    // Set `cat` to the value it already has.
    let (status, body) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.UpdateItem",
        r#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a1"}},
            "UpdateExpression":"SET cat = :same",
            "ExpressionAttributeValues":{":same":{"S":"X"}},
            "ReturnValues":"UPDATED_NEW"}"#,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert!(
        !body.contains("Attributes"),
        "nothing changed, so Attributes is omitted entirely: {body}"
    );

    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// `ALL_OLD`/`ALL_NEW` still return the whole item — the contrast that shows
/// `UPDATED_*` really is narrowing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_variants_still_return_the_whole_item() {
    let (_dir, nodes, addrs) = setup().await;

    let (status, body) = dynamo_retry(
        addrs[1],
        "DynamoDB_20120810.UpdateItem",
        r#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a2"}},
            "UpdateExpression":"SET cat = :y",
            "ExpressionAttributeValues":{":y":{"S":"Y"}},
            "ReturnValues":"ALL_NEW"}"#,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains(r#""cat":{"S":"Y"}"#), "{body}");
    assert!(
        body.contains("parity") && body.contains(r#""sk""#),
        "ALL_NEW carries untouched attributes and the key: {body}"
    );

    for n in nodes {
        n.shutdown_graceful().await;
    }
}
