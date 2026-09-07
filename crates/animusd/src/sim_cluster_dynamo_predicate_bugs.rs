//! `SimCluster`-driven end-to-end tests for the three silently-wrong
//! predicate-parser bugs (ADR 0061 rung D3 PR 1) — replaces the real-socket
//! `ProdEnv` binary `crates/animusd/tests/dynamo_predicate_bugs.rs`, whose
//! five tests are all base-table-only. Driven through `SimCluster::dynamo`
//! — see `sim_cluster_dynamo.rs`'s own module doc.
//!
//! All three shared one root cause — a naive `split_once('=')` and a
//! discarded attribute name — and all three failed *silently*: the caller
//! got a plausible-looking empty (or wrong) result set rather than an
//! error.
//!
//! Seed replay: `ANIMUS_SEED=<seed> cargo test -p animusd --lib <test name>`.

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn setup(seed: u64) -> SimCluster {
    let mut cluster = SimCluster::new(seed, 3, 3);
    cluster.create_table("events");
    for i in 0..6u32 {
        let parity = if i % 2 == 0 { "even" } else { "odd" };
        let body = format!(
            r#"{{"TableName":"events","Item":{{
                "pk":{{"S":"p1"}},"sk":{{"S":"a{i}"}},"cat":{{"S":"X"}},
                "score":{{"S":"s{i}"}},"parity":{{"S":"{parity}"}}}}}}"#
        );
        let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body.as_bytes());
        assert_eq!(
            status, 200,
            "seed PutItem(a{i}) failed (seed={seed}): {resp}"
        );
    }
    cluster
}

/// The alias fix, end to end: a filter written with `#alias` must actually
/// filter. Before, this returned zero items with a 200.
#[test]
fn an_aliased_filter_actually_filters() {
    let seed = env_seed(0xB096_0001);
    let mut cluster = setup(seed);

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Query",
        br##"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "FilterExpression":"#par = :v",
            "ExpressionAttributeNames":{"#par":"parity"},
            "ExpressionAttributeValues":{":p":{"S":"p1"},":v":{"S":"even"}}}"##,
    );
    assert_eq!(status, 200, "aliased filter failed (seed={seed}): {body}");
    assert!(
        body.contains("\"a0\""),
        "the aliased filter must match real items, not an attribute named `#par` (seed={seed}): {body}"
    );
    assert!(
        !body.contains("\"a1\""),
        "and must still exclude non-matching ones (seed={seed}): {body}"
    );
}

/// The comparison operators are now served. What must never come back is
/// the old behaviour: a 200 with an empty page because `sk >= :v` had been
/// truncated into an equality on `sk >`.
#[test]
fn comparisons_filter_rather_than_matching_nothing() {
    let seed = env_seed(0xB096_0002);
    let mut cluster = setup(seed);

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "FilterExpression":"sk >= :v",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":v":{"S":"a3"}}}"#,
    );
    assert_eq!(status, 200, "`>=` is served now (seed={seed}): {body}");
    assert!(
        body.contains("\"a3\"") && body.contains("\"a5\""),
        "it must actually match — the truncated form matched nothing (seed={seed}): {body}"
    );
    assert!(
        !body.contains("\"a2\""),
        "and still be bounded (seed={seed}): {body}"
    );
}

/// A sort-key range comparator is genuinely served, not silently narrowed
/// to an equality the way the pre-fix `>=` truncation used to behave.
#[test]
fn a_sort_key_range_is_served_rather_than_narrowed() {
    let seed = env_seed(0xB096_0003);
    let mut cluster = setup(seed);

    // `<>` is still rejected: it is not in AWS's own KeyConditionExpression
    // grammar (there is no not-equal *range*), unlike the other five.
    let (status, resp) = cluster.dynamo(
        1,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","KeyConditionExpression":"pk = :p AND sk <> :s",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":s":{"S":"a3"}}}"#,
    );
    assert_eq!(status, 400, "`<>` must stay rejected (seed={seed}): {resp}");
    assert!(resp.contains("ValidationException"), "seed={seed}: {resp}");

    // `>=`, which now genuinely narrows the range rather than being rejected.
    let (status, ge) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p AND sk >= :s",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":s":{"S":"a3"}}}"#,
    );
    assert_eq!(status, 200, "`>=` is served now (seed={seed}): {ge}");
    assert!(
        ge.contains("\"a3\"") && ge.contains("\"a5\""),
        "it must actually match — the pre-fix truncated form matched nothing (seed={seed}): {ge}"
    );
    assert!(
        !ge.contains("\"a2\""),
        "and still be bounded (seed={seed}): {ge}"
    );

    // BETWEEN, which was already supported, still works over the same data.
    let (status, ok) = cluster.dynamo(
        1,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p AND sk BETWEEN :lo AND :hi",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":lo":{"S":"a1"},":hi":{"S":"a3"}}}"#,
    );
    assert_eq!(status, 200, "BETWEEN still works (seed={seed}): {ok}");
    assert!(
        ok.contains("\"a2\""),
        "the range really is served (seed={seed}): {ok}"
    );
    assert!(
        !ok.contains("\"a5\""),
        "and really is bounded (seed={seed}): {ok}"
    );
}

/// The edge-side half of the fix: a key condition naming an attribute that
/// is not the table's partition key is rejected. Before, the name was
/// discarded and the query was served against whatever value it named.
#[test]
fn a_key_condition_naming_a_non_key_attribute_is_rejected() {
    let seed = env_seed(0xB096_0004);
    let mut cluster = setup(seed);

    let (status, resp) = cluster.dynamo(
        2,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","KeyConditionExpression":"cat = :c",
            "ExpressionAttributeValues":{":c":{"S":"X"}}}"#,
    );
    assert_eq!(
        status, 400,
        "`cat` is a real attribute but not the partition key (seed={seed}): {resp}"
    );
    assert!(resp.contains("ValidationException"), "seed={seed}: {resp}");

    // Naming a sort key the table does not have is likewise rejected.
    let (status, resp2) = cluster.dynamo(
        2,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","KeyConditionExpression":"pk = :p AND cat = :c",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":c":{"S":"X"}}}"#,
    );
    assert_eq!(
        status, 400,
        "`cat` is not the sort key (seed={seed}): {resp2}"
    );
    assert!(
        resp2.contains("ValidationException"),
        "seed={seed}: {resp2}"
    );
}

/// The valid forms still work — including an aliased key condition, which
/// is how a table whose key collides with a reserved word must be queried.
#[test]
fn aliased_and_plain_key_conditions_both_still_serve() {
    let seed = env_seed(0xB096_0005);
    let mut cluster = setup(seed);

    for body in [
        br#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}}}"#
            .as_slice(),
        br##"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"#k = :p",
            "ExpressionAttributeNames":{"#k":"pk"},
            "ExpressionAttributeValues":{":p":{"S":"p1"}}}"##
            .as_slice(),
    ] {
        let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.Query", body);
        assert_eq!(
            status, 200,
            "valid key condition failed (seed={seed}): {resp}"
        );
        assert!(
            resp.contains("\"a0\""),
            "and returns the partition (seed={seed}): {resp}"
        );
    }
}
