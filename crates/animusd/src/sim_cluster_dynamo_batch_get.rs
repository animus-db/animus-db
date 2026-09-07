//! `SimCluster`-driven end-to-end tests for `BatchGetItem` over the
//! DynamoDB wire (ADR 0061 rung D3 PR 1) — replaces the real-socket
//! `ProdEnv` binary `crates/animusd/tests/dynamo_batch_get.rs`, whose four
//! tests are all base-table-only (the GSI/LSI the original fixture declared
//! were never actually queried by any of them). Driven through
//! `SimCluster::dynamo`, the same generic `dynamo::dispatch_item_op` core
//! production uses — see `sim_cluster_dynamo.rs`'s own module doc for the
//! full "what's generic now" account.
//!
//! `BatchGetItem` is deliberately not transactional: DynamoDB's own
//! contract gives no cross-item atomicity, so this reuses the ordinary
//! `GetItem` read path per key. Misses are reported by *omission* from the
//! table's list, not positionally.
//!
//! Seed replay (repo convention): `ANIMUS_SEED=<seed> cargo test -p animusd
//! --lib <test name>`.

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Seed a 3-node RF3 cluster with one base table (`events`, composite
/// `pk`/`sk`) and six items in partition `pk = "p1"`, `sk = "a0".."a5"`,
/// alternating `parity`. Mirrors the `ProdEnv` fixture's own data shape,
/// minus the GSI (`by-cat`)/LSI (`by-score`) declarations neither test in
/// this file ever queries.
fn setup(seed: u64) -> SimCluster {
    let mut cluster = SimCluster::new(seed, 3, 3);
    cluster.create_table("events");
    for i in 0..6u32 {
        let parity = if i % 2 == 0 { "even" } else { "odd" };
        let body = format!(
            r#"{{"TableName":"events","Item":{{
                "pk":{{"S":"p1"}},"sk":{{"S":"a{i}"}},"cat":{{"S":"X"}},
                "score":{{"S":"s{i}"}},"parity":{{"S":"{parity}"}},
                "seq":{{"N":"{i}"}}}}}}"#
        );
        let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body.as_bytes());
        assert_eq!(
            status, 200,
            "seed PutItem(a{i}) failed (seed={seed}): {resp}"
        );
    }
    cluster
}

/// Reads across two tables in one call, grouped by table in the response.
#[test]
fn batch_get_reads_across_tables() {
    let seed = env_seed(0xBA76_0001);
    let mut cluster = setup(seed);

    cluster.create_table("other");
    let (status, resp) = cluster.dynamo(
        0,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"other","Item":{"pk":{"S":"o1"},"sk":{"S":"only"},"v":{"S":"vee"}}}"#,
    );
    assert_eq!(status, 200, "PutItem(other) failed (seed={seed}): {resp}");

    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.BatchGetItem",
        // ConsistentRead: true per table request (ADR 0055): reads back
        // items this test just wrote.
        br#"{"RequestItems":{
            "events":{"ConsistentRead":true,
                      "Keys":[{"pk":{"S":"p1"},"sk":{"S":"a0"}},
                              {"pk":{"S":"p1"},"sk":{"S":"a2"}}]},
            "other":{"ConsistentRead":true,"Keys":[{"pk":{"S":"o1"},"sk":{"S":"only"}}]}}}"#,
    );
    assert_eq!(status, 200, "BatchGetItem failed (seed={seed}): {body}");
    assert!(
        body.contains("\"a0\"") && body.contains("\"a2\""),
        "both event keys (seed={seed}): {body}"
    );
    assert!(
        body.contains("\"vee\""),
        "and the other table's item (seed={seed}): {body}"
    );
    assert!(
        body.contains(r#""UnprocessedKeys":{}"#),
        "seed={seed}: {body}"
    );
}

/// A key that matches nothing is omitted, not reported as an empty slot.
#[test]
fn a_missing_key_is_omitted_from_the_response() {
    let seed = env_seed(0xBA76_0002);
    let mut cluster = setup(seed);

    let (status, body) = cluster.dynamo(
        2,
        "DynamoDB_20120810.BatchGetItem",
        br#"{"RequestItems":{"events":{"ConsistentRead":true,"Keys":[
            {"pk":{"S":"p1"},"sk":{"S":"a0"}},
            {"pk":{"S":"p1"},"sk":{"S":"nope"}}]}}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {body}");
    assert!(
        body.contains("\"a0\""),
        "the hit is present (seed={seed}): {body}"
    );
    assert!(
        !body.contains("nope"),
        "the miss is simply absent (seed={seed}): {body}"
    );
}

/// The projection is scoped to the table and applies to every key under it.
#[test]
fn a_table_scoped_projection_applies_to_every_key() {
    let seed = env_seed(0xBA76_0003);
    let mut cluster = setup(seed);

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.BatchGetItem",
        br#"{"RequestItems":{"events":{
            "ConsistentRead":true,
            "Keys":[{"pk":{"S":"p1"},"sk":{"S":"a0"}},{"pk":{"S":"p1"},"sk":{"S":"a1"}}],
            "ProjectionExpression":"sk"}}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {body}");
    assert!(
        body.contains("\"a0\"") && body.contains("\"a1\""),
        "seed={seed}: {body}"
    );
    assert!(
        !body.contains("parity"),
        "an unprojected attribute must not appear for any key (seed={seed}): {body}"
    );
}

/// An unknown table is a `ResourceNotFoundException`, not a silent empty list.
#[test]
fn an_unknown_table_is_reported() {
    let seed = env_seed(0xBA76_0004);
    let mut cluster = setup(seed);

    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.BatchGetItem",
        br#"{"RequestItems":{"ghost":{"Keys":[{"id":{"S":"x"}}]}}}"#,
    );
    assert_eq!(
        status, 400,
        "unknown table must be reported (seed={seed}): {body}"
    );
    assert!(body.contains("ResourceNotFound"), "seed={seed}: {body}");
}
