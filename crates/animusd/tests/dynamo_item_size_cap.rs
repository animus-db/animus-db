//! End-to-end tests for the AWS 400 KB item-size cap enforced on
//! `UpdateItem`'s (and `TransactWriteItems`'s `Update` action's)
//! **post-update result**, over the real DynamoDB JSON/HTTP wire.
//!
//! `animus-dynamo`'s decode-time cap (issue #370, `wire::check_item_size`)
//! only ever saw a `PutItem`/`BatchWriteItem`/`TransactWriteItems` `Put`
//! request body directly — it has no way to know what a read-modify-write
//! `UpdateItem` will produce, since the pre-update image it reads is not in
//! the decoded request at all. That left AWS's own contract (an `UpdateItem`
//! whose *resulting* item exceeds 400 KB is rejected) unenforced. The fix
//! lives in the single choke point both `UpdateItem` and
//! `TransactWriteItems`'s `Update` action route through at the leader —
//! `animus_dynamo::wire::apply_update` — so one check covers both; see
//! `crates/animus-dynamo/src/wire.rs`'s `mod tests` for the pure unit
//! coverage of `apply_update` itself (the exact-boundary case and the
//! mid-fold-over/nets-back-under case). This file proves the same fix
//! through the real wire, on both operations that reach it.

use std::net::SocketAddr;

use animusd::StorageBackend;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

mod support;

/// AWS's per-item size cap, mirrored from `animus_dynamo::wire::
/// MAX_ITEM_SIZE_BYTES` (not imported directly — this is an end-to-end test
/// over the wire, not a unit test against the crate's internals).
const MAX_ITEM_SIZE_BYTES: usize = 409_600;

/// One DynamoDB JSON request over a fresh HTTP/1.1 connection → `(status,
/// body)`. Mirrors every other `tests/dynamo_*.rs` file's identical helper.
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

async fn create_table(addr: SocketAddr, table: &str) {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        &format!(
            r#"{{"TableName":"{table}",
                "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}],
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}]}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
}

async fn put_item(addr: SocketAddr, table: &str, item_json: &str) {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        &format!(r#"{{"TableName":"{table}","Item":{item_json}}}"#),
    )
    .await;
    assert_eq!(status, 200, "PutItem failed: {body}");
}

/// `GetItem` by `id`, returning the raw response body — `{}` when absent,
/// `{"Item": {..}}` when present.
async fn get_item(addr: SocketAddr, table: &str, id: &str) -> String {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.GetItem",
        &format!(r#"{{"TableName":"{table}","Key":{{"id":{{"S":"{id}"}}}}}}"#),
    )
    .await;
    assert_eq!(status, 200, "GetItem failed: {body}");
    body
}

/// A run of `n` `x` characters — built programmatically so the ~400 KB
/// payloads below never appear as a literal in this source file.
fn long_string(n: usize) -> String {
    "x".repeat(n)
}

/// A `PutItem` payload just under the cap: a base item with a `"payload"`
/// attribute comfortably below `MAX_ITEM_SIZE_BYTES`, leaving headroom for an
/// `UpdateItem`'s own `SET` to legally coexist with it while still being able
/// to tip the *result* over the cap.
fn large_but_legal_item(id: &str) -> String {
    // item_size = len("id") + id.len() + len("payload") + payload.len()
    //           = 2 + id.len() + 7 + 350_000, comfortably under 409_600.
    format!(
        r#"{{"id":{{"S":"{id}"}},"payload":{{"S":"{}"}}}}"#,
        long_string(350_000)
    )
}

/// `UpdateItem` rejects a post-update result over the cap, and leaves the
/// original item untouched — the leader evaluates the whole action list,
/// finds the net result over `MAX_ITEM_SIZE_BYTES`, and never proposes the
/// write at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_item_rejects_a_post_update_result_over_the_cap() {
    let dir = support::panic_safe_tempdir();
    let (node, _config) = support::start_single_node(dir.path(), StorageBackend::Memory).await;
    let addr = node.dynamo_addr();

    create_table(addr, "big").await;
    put_item(addr, "big", &large_but_legal_item("u1")).await;

    // The base item is ~350_007 bytes; a further ~70_000-byte "extra"
    // attribute pushes the post-update result well past MAX_ITEM_SIZE_BYTES
    // (409_600).
    let over_cap_value = long_string(70_000);
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.UpdateItem",
        &format!(
            r#"{{"TableName":"big","Key":{{"id":{{"S":"u1"}}}},
                "UpdateExpression":"SET extra = :v",
                "ExpressionAttributeValues":{{":v":{{"S":"{over_cap_value}"}}}}}}"#
        ),
    )
    .await;
    assert_eq!(
        status, 400,
        "an UpdateItem whose result exceeds the 400 KB cap must be rejected: {body}"
    );
    assert!(
        body.contains("ValidationException"),
        "expected ValidationException, got: {body}"
    );
    assert!(
        body.contains("Item size has exceeded the maximum allowed size"),
        "expected the size-cap message, got: {body}"
    );

    // The rejected UpdateItem must not have landed: the pre-update item is
    // still exactly what PutItem wrote, with no "extra" attribute.
    let after = get_item(addr, "big", "u1").await;
    assert!(
        after.contains(r#""payload""#),
        "the original item must survive the rejected update: {after}"
    );
    assert!(
        !after.contains(r#""extra""#),
        "the rejected update's attribute must not have landed: {after}"
    );

    node.shutdown_graceful().await;
}

/// The same rejection through `TransactWriteItems`'s `Update` action — the
/// other caller of `apply_update`, proving the fix is a single choke point
/// rather than something duplicated (and possibly missed) per call site.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transact_write_items_update_action_rejects_a_post_update_result_over_the_cap() {
    let dir = support::panic_safe_tempdir();
    let (node, _config) = support::start_single_node(dir.path(), StorageBackend::Memory).await;
    let addr = node.dynamo_addr();

    create_table(addr, "big").await;
    put_item(addr, "big", &large_but_legal_item("u2")).await;

    let over_cap_value = long_string(70_000);
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.TransactWriteItems",
        &format!(
            r#"{{"TransactItems":[{{"Update":{{"TableName":"big",
                "Key":{{"id":{{"S":"u2"}}}},
                "UpdateExpression":"SET extra = :v",
                "ExpressionAttributeValues":{{":v":{{"S":"{over_cap_value}"}}}}}}}}]}}"#
        ),
    )
    .await;
    assert_eq!(
        status, 400,
        "a TransactWriteItems Update whose result exceeds the 400 KB cap must be rejected: {body}"
    );
    assert!(
        body.contains("Item size has exceeded the maximum allowed size"),
        "expected the size-cap message to surface even wrapped in a transaction cancellation, \
         got: {body}"
    );

    let after = get_item(addr, "big", "u2").await;
    assert!(
        after.contains(r#""payload""#),
        "the original item must survive the rejected transactional update: {after}"
    );
    assert!(
        !after.contains(r#""extra""#),
        "the rejected transactional update's attribute must not have landed: {after}"
    );

    node.shutdown_graceful().await;
}

/// Sanity check that the two payload sizes above genuinely straddle the cap
/// the way the doc comments claim — guards the fixture itself against silent
/// drift if `MAX_ITEM_SIZE_BYTES` (or the fixture's own constants) ever
/// change.
#[test]
fn fixture_sizes_straddle_the_cap() {
    let base = 2 + "u1".len() + 7 + 350_000;
    let grown = base + ("extra".len() + 70_000);
    assert!(base < MAX_ITEM_SIZE_BYTES, "base fixture must be legal");
    assert!(
        grown > MAX_ITEM_SIZE_BYTES,
        "grown fixture must exceed the cap"
    );
}
