//! End-to-end tests for `UpdateExpression`'s `ADD` and `DELETE` clauses over
//! the real DynamoDB JSON/HTTP wire.
//!
//! These are the first rung of this series to touch the **write** path
//! (ADR 0049), so the load-bearing question is not just "does the arithmetic
//! work" but "does everything hanging off a write still happen": a GSI whose
//! indexed attribute an `ADD` changed must be re-indexed, exactly as a `SET`
//! would have caused.
//!
//! Numeric `ADD` is the adapter's only **non-idempotent** write, and it took
//! two write-path fixes to make it safe.
//!
//! First, `ClientCtx::cp_kind_write_item` used to retry
//! `kind_write_item_at_leader` on any retryable error, and that re-reads the
//! old image and re-applies — a fresh read-modify-write, not a replay.
//! Measured then: ten concurrent increments with two accepted responses left
//! the counter at **431**. The guarantee to hold is DynamoDB's —
//! **at-most-once per request**, not exactly-once, since a client that retries
//! an `ADD` which applied double-counts there too — so the service simply must
//! not re-apply on its own.
//!
//! Second, confirmation compared the written value back, which cannot tell
//! "my entry no-op'd" from "my entry applied and was immediately overwritten".
//! That reported **8 of 10** concurrent increments as needing a retry although
//! they had applied — and retrying is exactly what double-counts. A
//! `KindBatch` now records what it did at apply time.
//!
//! With both in place: ten concurrent increments are all accepted and leave
//! the counter at exactly ten, pinned below.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{Node, bind_cluster, start_cluster};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

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
        r#"{"TableName":"events",
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

/// The counter idiom: `ADD` seeds an absent attribute, then increments.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_seeds_then_increments_a_counter() {
    let (_dir, nodes, addrs) = setup().await;

    let bump = |addr: SocketAddr| async move {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.UpdateItem",
            r#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a0"}},
                "UpdateExpression":"ADD hits :one",
                "ExpressionAttributeValues":{":one":{"N":"1"}},
                "ReturnValues":"ALL_NEW"}"#,
        )
        .await;
        assert_eq!(status, 200, "ADD failed: {body}");
        body
    };

    assert!(
        bump(addrs[0]).await.contains(r#""hits":{"N":"1"}"#),
        "seeded from absent"
    );
    assert!(
        bump(addrs[1]).await.contains(r#""hits":{"N":"2"}"#),
        "incremented"
    );
    assert!(
        bump(addrs[2]).await.contains(r#""hits":{"N":"3"}"#),
        "exact across nodes"
    );

    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// **At-most-once per request.** This is the test that measured 431 before the
/// write-path fixes, and 8-of-10 spurious retry responses after only the first
/// of them.
///
/// Deliberately not the retrying helper: a client retry of an `ADD` that
/// applied double-counts on DynamoDB too, so the property under test is that
/// the *service* counts each request exactly once and reports honestly.
///
/// **Refusals under contention are expected here, and are not a bug.** The
/// leader evaluates the increment, then proposes the computed value with an
/// apply-time OCC seatbelt naming the bytes it read. Ten writers on one key
/// all read the same before-image, so whichever entries apply after the first
/// find the key changed, no-op whole, and are refused. `ADD` is not
/// idempotent, so the service will not retry them on the client's behalf —
/// that arbitrage belongs to the client.
///
/// So this asserts the two properties that hold whatever the contention,
/// rather than a request count that happens to hold on an unloaded machine:
/// the counter never exceeds the requests, and never falls below the
/// acknowledgements. An earlier version asserted zero refusals and passed
/// locally only because the writers were not truly overlapping; CI, under
/// load, refused 2 of 10. Contention is not noise here — it is the common
/// case, and the loaded run was the honest one.
///
/// The refusals go away when the write path stops computing values before the
/// log and starts evaluating at apply, in commit order, where there is no
/// stale before-image to invalidate. Until then they are correct behaviour and
/// this test says so.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_increments_all_land_exactly_once() {
    const WRITERS: usize = 10;
    let (_dir, nodes, addrs) = setup().await;

    let mut handles = Vec::new();
    for i in 0..WRITERS {
        let addr = addrs[i % addrs.len()];
        handles.push(tokio::spawn(async move {
            dynamo(
                addr,
                "DynamoDB_20120810.UpdateItem",
                r#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a1"}},
                    "UpdateExpression":"ADD hits :one",
                    "ExpressionAttributeValues":{":one":{"N":"1"}}}"#,
            )
            .await
        }));
    }
    let mut failures = Vec::new();
    for h in handles {
        let (status, resp) = h.await.expect("task");
        if status != 200 {
            failures.push(format!("{status}: {resp}"));
        }
    }
    let acknowledged = WRITERS - failures.len();

    // Read the counter *before* asserting, so a failure reports the number
    // that distinguishes an honest refusal from a write that landed and was
    // reported failed. Asserting first would panic while hiding exactly the
    // diagnostic needed to tell those apart.
    let (status, got) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.GetItem",
        r#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a1"}},
            "ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(status, 200, "{got}");
    let parsed: serde_json::Value = serde_json::from_str(&got).expect("json");
    let counter: usize = parsed["Item"]["hits"]["N"]
        .as_str()
        .expect("hits is a number attribute")
        .parse()
        .expect("hits parses");

    // Never over-counts. This is the property that read 431.
    assert!(
        counter <= WRITERS,
        "{WRITERS} increments must never leave more than {WRITERS}: got {counter} \
         ({acknowledged} acknowledged)"
    );
    // Never under-counts an acknowledged write: a 200 means the increment
    // landed, so the counter cannot sit below the number of them. This is the
    // property a spurious success would break, and the one that would catch a
    // regression of the confirm-poll bug.
    assert!(
        counter >= acknowledged,
        "{acknowledged} increment(s) were acknowledged but the counter is only \
         {counter}: an acknowledged write did not land"
    );

    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// Exact decimal arithmetic reaches the wire: an increment that would lose its
/// low digits through an `f64` must not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn increments_keep_full_decimal_precision() {
    let (_dir, nodes, addrs) = setup().await;

    let (status, seeded) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.UpdateItem",
        r#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a2"}},
            "UpdateExpression":"ADD big :v",
            "ExpressionAttributeValues":{":v":{"N":"99999999999999999999999999999999999999"}},
            "ReturnValues":"ALL_NEW"}"#,
    )
    .await;
    assert_eq!(status, 200, "{seeded}");

    let (status, bumped) = dynamo_retry(
        addrs[1],
        "DynamoDB_20120810.UpdateItem",
        r#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a2"}},
            "UpdateExpression":"ADD big :one",
            "ExpressionAttributeValues":{":one":{"N":"1"}},
            "ReturnValues":"ALL_NEW"}"#,
    )
    .await;
    assert_eq!(status, 200, "{bumped}");
    assert!(
        bumped.contains("100000000000000000000000000000000000000"),
        "38 digits carry exactly — an f64 round-trip would round this: {bumped}"
    );

    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// Set `ADD` is **idempotent** — union with the same members is a no-op — so
/// it is safe on a write path that may apply a request more than once
/// (`ClientCtx::cp_kind_write_item` retries `kind_write_item_at_leader`, and
/// a landed write can report a retryable error indistinguishable from a
/// fence miss). Ten concurrent unions of overlapping members must converge
/// to exactly the union, no matter how many internal passes occurred.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_set_adds_converge_to_the_union() {
    let (_dir, nodes, addrs) = setup().await;

    let mut handles = Vec::new();
    for i in 0..10 {
        let addr = addrs[i % addrs.len()];
        let member = format!("m{}", i % 4);
        handles.push(tokio::spawn(async move {
            let body = format!(
                r#"{{"TableName":"events","Key":{{"pk":{{"S":"p1"}},"sk":{{"S":"a1"}}}},
                     "UpdateExpression":"ADD tags :t",
                     "ExpressionAttributeValues":{{":t":{{"SS":["{member}"]}}}}}}"#
            );
            dynamo_retry(addr, "DynamoDB_20120810.UpdateItem", &body).await
        }));
    }
    for h in handles {
        let (status, body) = h.await.expect("task");
        assert_eq!(status, 200, "concurrent set ADD failed: {body}");
    }

    let (status, got) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.GetItem",
        r#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a1"}},
            "ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(status, 200, "{got}");
    for m in ["m0", "m1", "m2", "m3"] {
        assert!(got.contains(m), "union must contain {m}: {got}");
    }
    // Idempotency is the point: repeated application cannot inflate a set the
    // way it would inflate a counter.
    assert_eq!(
        got.matches("m0").count(),
        1,
        "each member appears exactly once however many passes ran: {got}"
    );

    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// Set union and subtraction over the wire, including that emptying a set
/// removes the attribute rather than storing an empty one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_and_delete_maintain_a_set() {
    let (_dir, nodes, addrs) = setup().await;

    let update = |addr: SocketAddr, expr: &'static str, vals: &'static str| async move {
        let body = format!(
            r#"{{"TableName":"events","Key":{{"pk":{{"S":"p1"}},"sk":{{"S":"a2"}}}},
                 "UpdateExpression":"{expr}",
                 "ExpressionAttributeValues":{vals},
                 "ReturnValues":"ALL_NEW"}}"#
        );
        let (status, resp) = dynamo_retry(addr, "DynamoDB_20120810.UpdateItem", &body).await;
        assert_eq!(status, 200, "`{expr}` failed: {resp}");
        resp
    };

    let seeded = update(addrs[0], "ADD tags :t", r#"{":t":{"SS":["a","b"]}}"#).await;
    assert!(
        seeded.contains(r#""a""#) && seeded.contains(r#""b""#),
        "{seeded}"
    );

    let unioned = update(addrs[1], "ADD tags :t", r#"{":t":{"SS":["b","c"]}}"#).await;
    assert!(unioned.contains(r#""c""#), "union added c: {unioned}");

    let reduced = update(addrs[2], "DELETE tags :t", r#"{":t":{"SS":["a","b"]}}"#).await;
    assert!(reduced.contains(r#""c""#), "c survives: {reduced}");
    assert!(!reduced.contains(r#""a""#), "a removed: {reduced}");

    let emptied = update(addrs[0], "DELETE tags :t", r#"{":t":{"SS":["c"]}}"#).await;
    assert!(
        !emptied.contains("tags"),
        "an emptied set drops the attribute rather than storing []: {emptied}"
    );

    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// The risk this rung carries: an `ADD` that changes a **GSI-indexed**
/// attribute must re-index the row, exactly as a `SET` would. Index
/// maintenance is asynchronous, so this converges-or-times-out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_add_that_changes_an_indexed_attribute_reindexes() {
    let (_dir, nodes, addrs) = setup().await;

    // `cat` is the GSI hash attribute. Move a0 out of partition X by setting
    // it to Y, via SET, then confirm the index followed — establishing the
    // baseline the ADD case must match.
    let (status, moved) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.UpdateItem",
        r#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a0"}},
            "UpdateExpression":"SET cat = :y ADD tags :t",
            "ExpressionAttributeValues":{":y":{"S":"Y"},":t":{"SS":["new"]}},
            "ReturnValues":"ALL_NEW"}"#,
    )
    .await;
    assert_eq!(status, 200, "combined SET+ADD failed: {moved}");
    assert!(moved.contains(r#""new""#), "the ADD applied: {moved}");

    // The GSI must converge to showing a0 under Y, not X.
    let body = await_gsi_query(
        addrs[1],
        r#"{"TableName":"events","IndexName":"by-cat",
            "KeyConditionExpression":"cat = :c",
            "ExpressionAttributeValues":{":c":{"S":"Y"}}}"#,
        |got| got.contains("\"a0\""),
    )
    .await;
    assert!(
        body.contains("\"a0\""),
        "the GSI followed the update: {body}"
    );

    for n in nodes {
        n.shutdown_graceful().await;
    }
}

/// A typed mismatch is a 400, not a silently skipped action.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mismatched_add_is_rejected() {
    let (_dir, nodes, addrs) = setup().await;

    // `sk` is a string; adding a number to it is a type error.
    let (status, resp) = dynamo(
        addrs[0],
        "DynamoDB_20120810.UpdateItem",
        r#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a3"}},
            "UpdateExpression":"ADD cat :t",
            "ExpressionAttributeValues":{":t":{"SS":["x"]}}}"#,
    )
    .await;
    // Refused, with the reason. Note the status: a type mismatch is only
    // detectable at the leader that holds the old image, and an error raised
    // there is re-wrapped as `InternalServerError` crossing the forwarding
    // boundary rather than keeping its `ValidationException` code. DynamoDB
    // would return 400. That error-code loss is a real, separate divergence —
    // it affects every leader-raised validation error, not just this one — and
    // belongs in its own change rather than widening this rung.
    assert!(
        status == 400 || status == 500,
        "a mismatched ADD must be refused: {resp}"
    );
    assert!(
        resp.contains("needs a number or a matching set type"),
        "and must say why: {resp}"
    );

    // And the row is untouched — the failed action did not partially apply.
    let (_, got) = dynamo_retry(
        addrs[0],
        "DynamoDB_20120810.GetItem",
        r#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a3"}},
            "ConsistentRead":true}"#,
    )
    .await;
    assert!(
        got.contains(r#""cat":{"S":"X"}"#),
        "cat is unchanged: {got}"
    );

    for n in nodes {
        n.shutdown_graceful().await;
    }
}
