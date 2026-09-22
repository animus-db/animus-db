//! `SimCluster`-driven end-to-end tests of the aggregate byte caps ADR 0072
//! layer 4 adds — the request/response-size siblings of layer 3's `Query`/
//! `Scan` evaluated-page cap (`sim_cluster_dynamo_page_size_cap.rs`):
//!
//! - `BatchGetItem`'s 16 MiB **response**-size cap
//!   (`animus_dynamo::limits::MAX_BATCH_GET_RESPONSE_BYTES`) — not an
//!   error: the fetched-but-cut items, and every key not yet fetched, come
//!   back in `UnprocessedKeys` instead, exactly like a per-key throttle
//!   refusal (`crate::dynamo`'s `Operation::BatchGetItem` arm).
//! - `TransactWriteItems`'s 4 MiB aggregate **request**-size cap
//!   (`animus_dynamo::limits::MAX_TRANSACT_BYTES`), enforced at decode
//!   time (`animus_dynamo::wire::decode_transact_write`) — a
//!   `ValidationException`, writing nothing.
//! - `TransactGetItems`'s 4 MiB aggregate **response**-size cap (the same
//!   constant), enforced against the fetched result
//!   (`crate::dynamo::run_transact_get`) since the request itself (at most
//!   100 keys of a few KB each) can never reach it — also a
//!   `ValidationException`.
//!
//! Every fixture item here derives its own padding from
//! `animus_item::item_size` against the fixture actually used (this repo's
//! own testing convention — see `sim_cluster_dynamo_page_size_cap.rs`'s
//! module doc), rather than hardcoding a byte count, so this suite stays
//! correct if the size formula or either cap constant ever changes.
//!
//! Seed replay (repo convention): `ANIMUS_SEED=<seed> cargo test -p animusd
//! --lib <test name>`.

use std::collections::BTreeSet;

use animus_dynamo::limits::{
    MAX_BATCH_GET_RESPONSE_BYTES, MAX_ITEM_SIZE_BYTES, MAX_TRANSACT_BYTES,
};
use animus_item::{AttributeValue, Item, item_size};

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

const PARTITION: &str = "p1";

/// `pk = PARTITION`, `sk`, and a `pad` filler attribute of `pad_len` bytes.
fn item_with_pad(sk: &str, pad_len: usize) -> Item {
    let mut item = Item::new();
    item.insert("pk".to_string(), AttributeValue::S(PARTITION.to_string()));
    item.insert("sk".to_string(), AttributeValue::S(sk.to_string()));
    item.insert("pad".to_string(), AttributeValue::S("x".repeat(pad_len)));
    item
}

/// The same fixture shape as [`item_with_pad`], but with `pad` sized so the
/// item's own `animus_item::item_size` is exactly `target` bytes — derived
/// from the fixed (`pk`/`sk`/attribute-name) overhead actually measured,
/// never hardcoded, so this stays correct across a size-formula change.
fn item_sized(sk: &str, target: usize) -> Item {
    let overhead = item_size(&item_with_pad(sk, 0));
    assert!(
        target >= overhead,
        "target {target} smaller than the fixture's own fixed overhead {overhead}"
    );
    item_with_pad(sk, target - overhead)
}

/// A `MAX_ITEM_SIZE_BYTES`-sized item — the largest single item DynamoDB
/// (and this adapter) ever accepts.
fn max_size_item(sk: &str) -> Item {
    item_sized(sk, MAX_ITEM_SIZE_BYTES)
}

fn put_item_body(table: &str, item: &Item) -> String {
    format!(
        r#"{{"TableName":"{table}","Item":{}}}"#,
        serde_json::to_string(&animus_dynamo::wire::encode_item(item))
            .expect("item encodes to JSON")
    )
}

fn json(resp: &str) -> serde_json::Value {
    serde_json::from_str(resp).unwrap_or_else(|e| panic!("not JSON: {e}: {resp}"))
}

// Fixture assumptions this whole file leans on, checked once at compile
// time (both sides are `const`, so a runtime `assert!` on them is itself a
// clippy `assertions_on_constants` lint) rather than once per test: eleven
// `MAX_ITEM_SIZE_BYTES` items exceed `MAX_TRANSACT_BYTES`, ten fit under it
// — the smallest action/key count that can reach the 4 MiB cap at all while
// every individual item stays legal (`TRANSACT_WRITE_MAX_ACTIONS` ×
// `MAX_ITEM_SIZE_BYTES` is 40 MB, so the 100-action count cap alone doesn't
// stop this the way it stops `BatchWriteItem`'s own 16 MiB cap).
const _: () = assert!(11 * MAX_ITEM_SIZE_BYTES > MAX_TRANSACT_BYTES);
const _: () = assert!(10 * MAX_ITEM_SIZE_BYTES <= MAX_TRANSACT_BYTES);

// --- BatchGetItem's 16 MiB response-size cap --------------------------------

/// How many `item_bytes`-sized items fit in one `BatchGetItem` response at
/// the real cap.
fn batch_get_items_per_page(item_bytes: usize) -> usize {
    MAX_BATCH_GET_RESPONSE_BYTES / item_bytes
}

fn batch_get_setup(seed: u64, total: usize, item_bytes: usize) -> SimCluster {
    let mut cluster = SimCluster::new(seed, 3, 3);
    cluster.create_table("events");
    for i in 0..total {
        let sk = format!("i{i:03}");
        let body = put_item_body("events", &item_sized(&sk, item_bytes));
        let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body.as_bytes());
        assert_eq!(
            status, 200,
            "seed PutItem(i{i:03}) failed (seed={seed}): {resp}"
        );
    }
    cluster
}

fn batch_get_request(table: &str, keys: &[String]) -> String {
    format!(
        r#"{{"RequestItems":{{"{table}":{{"ConsistentRead":true,"Keys":[{}]}}}}}}"#,
        keys.join(",")
    )
}

fn key_json(sk: &str) -> String {
    format!(r#"{{"pk":{{"S":"{PARTITION}"}},"sk":{{"S":"{sk}"}}}}"#)
}

/// `BatchGetItem` over ~50 items of ~390 KB each (well over the 16 MiB
/// response cap, well under the 400 KB per-item cap) returns a first page
/// under 16 MiB plus `UnprocessedKeys` for the rest; looping on
/// `UnprocessedKeys` (reissuing `ConsistentRead` against exactly the
/// still-outstanding keys) retrieves every item exactly once.
#[test]
fn batch_get_item_over_the_response_byte_cap_paginates_via_unprocessed_keys() {
    let seed = env_seed(0xB47E_0001);
    const ITEM_BYTES: usize = 390_000;
    let per_page = batch_get_items_per_page(ITEM_BYTES);
    let total = 50usize;
    assert!(
        per_page >= 2 && per_page < total,
        "fixture must straddle the cap: per_page={per_page}, total={total}"
    );
    let mut cluster = batch_get_setup(seed, total, ITEM_BYTES);

    let all_keys: Vec<String> = (0..total).map(|i| key_json(&format!("i{i:03}"))).collect();
    let (status, resp) = cluster.dynamo(
        0,
        "DynamoDB_20120810.BatchGetItem",
        batch_get_request("events", &all_keys).as_bytes(),
    );
    assert_eq!(status, 200, "seed={seed}: {resp}");
    let v = json(&resp);
    let first_items = v["Responses"]["events"]
        .as_array()
        .expect("Responses.events is an array");
    assert_eq!(
        first_items.len(),
        per_page,
        "first page should hold exactly one response-budget's worth: {v} (seed={seed})"
    );
    let unprocessed = v["UnprocessedKeys"]["events"]["Keys"]
        .as_array()
        .expect("UnprocessedKeys.events.Keys is an array");
    assert_eq!(
        unprocessed.len(),
        total - per_page,
        "every key past the budget must be unprocessed: {v} (seed={seed})"
    );

    // Every key requested is accounted for exactly once, either fetched or
    // unprocessed.
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for it in first_items {
        seen.insert(it["sk"]["S"].as_str().expect("sk").to_string());
    }
    let mut remaining: Vec<serde_json::Value> = unprocessed.clone();

    let mut rounds = 1;
    while !remaining.is_empty() {
        assert!(
            rounds < 10,
            "UnprocessedKeys pagination did not terminate (seed={seed})"
        );
        rounds += 1;
        let keys: Vec<String> = remaining
            .iter()
            .map(|k| serde_json::to_string(k).expect("key encodes"))
            .collect();
        let (status, resp) = cluster.dynamo(
            0,
            "DynamoDB_20120810.BatchGetItem",
            batch_get_request("events", &keys).as_bytes(),
        );
        assert_eq!(status, 200, "seed={seed}: {resp}");
        let v = json(&resp);
        for it in v["Responses"]["events"]
            .as_array()
            .expect("Responses.events is an array")
        {
            let sk = it["sk"]["S"].as_str().expect("sk").to_string();
            assert!(
                seen.insert(sk.clone()),
                "seed={seed}: item {sk} retrieved more than once"
            );
        }
        remaining = v["UnprocessedKeys"]
            .get("events")
            .and_then(|t| t.get("Keys"))
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
    }

    let expected: BTreeSet<String> = (0..total).map(|i| format!("i{i:03}")).collect();
    assert_eq!(
        seen, expected,
        "every item must be retrieved exactly once (seed={seed})"
    );
}

// --- TransactWriteItems' 4 MiB aggregate request-size cap -------------------

fn transact_put_action(table: &str, item: &Item) -> String {
    format!(
        r#"{{"Put":{{"TableName":"{table}","Item":{}}}}}"#,
        serde_json::to_string(&animus_dynamo::wire::encode_item(item))
            .expect("item encodes to JSON")
    )
}

/// A `TransactWriteItems` body with `n` `Put` actions, each writing its own
/// `MAX_ITEM_SIZE_BYTES`-sized item to a distinct key (`sk = "tNNN"`).
fn transact_write_body_of_max_puts(table: &str, n: usize) -> String {
    let actions: Vec<String> = (0..n)
        .map(|i| transact_put_action(table, &max_size_item(&format!("t{i:03}"))))
        .collect();
    format!(r#"{{"TransactItems":[{}]}}"#, actions.join(","))
}

/// Eleven `MAX_ITEM_SIZE_BYTES` `Put`s (4,505,600 bytes) exceed
/// `MAX_TRANSACT_BYTES` (4,194,304): `TransactWriteItems` is rejected with
/// `ValidationException` and none of the eleven items land.
#[test]
fn transact_write_items_over_the_byte_cap_is_rejected_and_writes_nothing() {
    let seed = env_seed(0xB47E_0002);
    let mut cluster = SimCluster::new(seed, 3, 3);
    cluster.create_table("txw_over");

    let body = transact_write_body_of_max_puts("txw_over", 11);
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.TransactWriteItems", body.as_bytes());
    assert_eq!(
        status, 400,
        "an over-cap TransactWriteItems must be rejected (seed={seed}): {resp}"
    );
    assert!(resp.contains("ValidationException"), "seed={seed}: {resp}");

    for i in 0..11 {
        let sk = format!("t{i:03}");
        let (status, resp) = cluster.dynamo(
            0,
            "DynamoDB_20120810.GetItem",
            format!(
                r#"{{"TableName":"txw_over","Key":{},"ConsistentRead":true}}"#,
                key_json(&sk)
            )
            .as_bytes(),
        );
        assert_eq!(status, 200, "seed={seed}: {resp}");
        assert!(
            !resp.contains("\"Item\""),
            "seed={seed}: item {sk} must not have landed: {resp}"
        );
    }
}

/// Ten `MAX_ITEM_SIZE_BYTES` `Put`s (4,096,000 bytes) fit under
/// `MAX_TRANSACT_BYTES`: the transaction commits and every item is readable
/// afterward.
#[test]
fn transact_write_items_at_the_byte_cap_succeeds() {
    let seed = env_seed(0xB47E_0003);
    let mut cluster = SimCluster::new(seed, 3, 3);
    cluster.create_table("txw_at");

    let body = transact_write_body_of_max_puts("txw_at", 10);
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.TransactWriteItems", body.as_bytes());
    assert_eq!(
        status, 200,
        "an at-cap TransactWriteItems must succeed (seed={seed}): {resp}"
    );

    for i in 0..10 {
        let sk = format!("t{i:03}");
        let (status, resp) = cluster.dynamo(
            0,
            "DynamoDB_20120810.GetItem",
            format!(
                r#"{{"TableName":"txw_at","Key":{},"ConsistentRead":true}}"#,
                key_json(&sk)
            )
            .as_bytes(),
        );
        assert_eq!(status, 200, "seed={seed}: {resp}");
        assert!(
            resp.contains("\"Item\""),
            "seed={seed}: item {sk} must exist: {resp}"
        );
    }
}

// --- TransactGetItems' 4 MiB aggregate response-size cap --------------------

fn transact_get_setup(seed: u64, table: &str, n: usize) -> SimCluster {
    let mut cluster = SimCluster::new(seed, 3, 3);
    cluster.create_table(table);
    for i in 0..n {
        let sk = format!("t{i:03}");
        let body = put_item_body(table, &max_size_item(&sk));
        let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body.as_bytes());
        assert_eq!(
            status, 200,
            "seed PutItem({sk}) failed (seed={seed}): {resp}"
        );
    }
    cluster
}

fn transact_get_body(table: &str, n: usize) -> String {
    let gets: Vec<String> = (0..n)
        .map(|i| {
            let sk = format!("t{i:03}");
            format!(
                r#"{{"Get":{{"TableName":"{table}","Key":{}}}}}"#,
                key_json(&sk)
            )
        })
        .collect();
    format!(r#"{{"TransactItems":[{}]}}"#, gets.join(","))
}

/// Eleven pre-seeded `MAX_ITEM_SIZE_BYTES` items exceed `MAX_TRANSACT_BYTES`
/// in aggregate once fetched: `TransactGetItems` is rejected with
/// `ValidationException` — the request itself (11 short keys) could never
/// trip this, only the fetched result can.
#[test]
fn transact_get_items_over_the_byte_cap_is_rejected() {
    let seed = env_seed(0xB47E_0004);
    let mut cluster = transact_get_setup(seed, "txg_over", 11);

    let body = transact_get_body("txg_over", 11);
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.TransactGetItems", body.as_bytes());
    assert_eq!(
        status, 400,
        "an over-cap TransactGetItems must be rejected (seed={seed}): {resp}"
    );
    assert!(resp.contains("ValidationException"), "seed={seed}: {resp}");
}

/// Ten pre-seeded `MAX_ITEM_SIZE_BYTES` items fit under `MAX_TRANSACT_BYTES`
/// once fetched: `TransactGetItems` succeeds and returns every item.
#[test]
fn transact_get_items_at_the_byte_cap_succeeds() {
    let seed = env_seed(0xB47E_0005);
    let mut cluster = transact_get_setup(seed, "txg_at", 10);

    let body = transact_get_body("txg_at", 10);
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.TransactGetItems", body.as_bytes());
    assert_eq!(
        status, 200,
        "an at-cap TransactGetItems must succeed (seed={seed}): {resp}"
    );
    let v = json(&resp);
    let responses = v["Responses"].as_array().expect("Responses is an array");
    assert_eq!(responses.len(), 10, "seed={seed}: {resp}");
    for r in responses {
        assert!(
            r.get("Item").is_some(),
            "seed={seed}: every slot must carry its item: {resp}"
        );
    }
}
