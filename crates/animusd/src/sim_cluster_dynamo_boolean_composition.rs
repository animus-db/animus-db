//! `SimCluster`-driven end-to-end tests for `AND`/`OR`/`NOT` composition in
//! `FilterExpression`/`ConditionExpression` over the DynamoDB wire (ADR
//! 0061 rung D3 PR 1) — replaces the real-socket `ProdEnv` binary
//! `crates/animusd/tests/dynamo_boolean_composition.rs`, whose four tests
//! are all base-table-only. Driven through `SimCluster::dynamo` — see
//! `sim_cluster_dynamo.rs`'s own module doc.
//!
//! Precedence is the whole point: `NOT` binds tightest, then `AND`, then
//! `OR`, and parentheses override. `a BETWEEN :lo AND :hi` contains an
//! `AND` belonging to the term, not the combinator.
//!
//! Seed replay: `ANIMUS_SEED=<seed> cargo test -p animusd --lib <test name>`.

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Seed a 3-node RF3 cluster with one base table (`events`) and six items
/// in partition `pk = "p1"`, mirroring the `ProdEnv` fixture minus the
/// GSI/LSI declarations no test here queries.
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

/// `AND` narrows, `OR` widens, and both reach the wire.
#[test]
fn and_or_compose_over_the_wire() {
    let seed = env_seed(0xB001_0001);
    let mut cluster = setup(seed);

    let q = |cluster: &mut SimCluster, frag: &str| -> (u16, String) {
        let body = format!(
            r#"{{"TableName":"events","ConsistentRead":true,
                 "KeyConditionExpression":"pk = :p",
                 "FilterExpression":"{frag}",
                 "ExpressionAttributeValues":{{":p":{{"S":"p1"}},
                    ":even":{{"S":"even"}},":three":{{"N":"3"}},
                    ":one":{{"N":"1"}},":five":{{"N":"5"}}}}}}"#
        );
        cluster.dynamo(0, "DynamoDB_20120810.Query", body.as_bytes())
    };

    // even AND seq >= 3  -> a4 only (a0,a2 are even but below 3)
    let (status, both) = q(&mut cluster, "parity = :even AND seq >= :three");
    assert_eq!(status, 200, "AND failed (seed={seed}): {both}");
    assert!(both.contains("\"a4\""), "seed={seed}: {both}");
    assert!(
        !both.contains("\"a0\"") && !both.contains("\"a3\""),
        "seed={seed}: {both}"
    );

    // seq = 1 OR seq = 5  -> a1 and a5
    let (_, either) = q(&mut cluster, "seq = :one OR seq = :five");
    assert!(
        either.contains("\"a1\"") && either.contains("\"a5\""),
        "seed={seed}: {either}"
    );
    assert!(!either.contains("\"a2\""), "seed={seed}: {either}");

    // NOT even  -> the odd ones
    let (_, negated) = q(&mut cluster, "NOT parity = :even");
    assert!(negated.contains("\"a1\""), "seed={seed}: {negated}");
    assert!(!negated.contains("\"a0\""), "seed={seed}: {negated}");
}

/// Precedence, demonstrated by the same leaves under two trees returning
/// different rows. `a OR b AND c` must group as `a OR (b AND c)`.
#[test]
fn precedence_and_parentheses_change_the_answer() {
    let seed = env_seed(0xB001_0002);
    let mut cluster = setup(seed);

    let q = |cluster: &mut SimCluster, frag: &str| -> String {
        let body = format!(
            r#"{{"TableName":"events","ConsistentRead":true,
                 "KeyConditionExpression":"pk = :p",
                 "FilterExpression":"{frag}",
                 "ExpressionAttributeValues":{{":p":{{"S":"p1"}},
                    ":zero":{{"N":"0"}},":odd":{{"S":"odd"}},":five":{{"N":"5"}}}}}}"#
        );
        let (status, resp) = cluster.dynamo(1, "DynamoDB_20120810.Query", body.as_bytes());
        assert_eq!(status, 200, "`{frag}` failed (seed={seed}): {resp}");
        resp
    };

    // seq = 0 OR parity = odd AND seq = 5
    //   default grouping: seq=0 OR (odd AND seq=5)  -> a0 and a5
    let default = q(&mut cluster, "seq = :zero OR parity = :odd AND seq = :five");
    assert!(
        default.contains("\"a0\""),
        "a0 via the left disjunct (seed={seed}): {default}"
    );
    assert!(
        default.contains("\"a5\""),
        "a5 via the right conjunction (seed={seed}): {default}"
    );
    assert!(
        !default.contains("\"a1\""),
        "a1 is odd but not seq=5 (seed={seed}): {default}"
    );

    //   parenthesised the other way: (seq=0 OR odd) AND seq=5  -> a5 only
    let grouped = q(
        &mut cluster,
        "(seq = :zero OR parity = :odd) AND seq = :five",
    );
    assert!(grouped.contains("\"a5\""), "seed={seed}: {grouped}");
    assert!(
        !grouped.contains("\"a0\""),
        "a0 must drop out once the AND applies to the whole disjunction (seed={seed}): {grouped}"
    );
}

/// The `BETWEEN … AND …` trap, end to end: the first `AND` closes the
/// range, the second joins terms.
#[test]
fn between_composes_with_a_following_and() {
    let seed = env_seed(0xB001_0003);
    let mut cluster = setup(seed);

    let (status, body) = cluster.dynamo(
        2,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "FilterExpression":"seq BETWEEN :lo AND :hi AND parity = :even",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":lo":{"N":"1"},
                ":hi":{"N":"4"},":even":{"S":"even"}}}"#,
    );
    assert_eq!(status, 200, "BETWEEN + AND failed (seed={seed}): {body}");
    assert!(
        body.contains("\"a2\"") && body.contains("\"a4\""),
        "seed={seed}: {body}"
    );
    assert!(
        !body.contains("\"a3\""),
        "a3 is in range but odd (seed={seed}): {body}"
    );
    assert!(
        !body.contains("\"a0\""),
        "a0 is even but below the range (seed={seed}): {body}"
    );
}

/// Composition reaches conditional writes too, since one decoder serves both.
#[test]
fn conditional_writes_accept_composed_conditions() {
    let seed = env_seed(0xB001_0004);
    let mut cluster = setup(seed);

    let (status, ok) = cluster.dynamo(
        0,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"events","Item":{"pk":{"S":"p1"},"sk":{"S":"a0"},"seq":{"N":"0"},"parity":{"S":"even"}},
            "ConditionExpression":"attribute_exists(sk) AND seq < :one",
            "ExpressionAttributeValues":{":one":{"N":"1"}}}"#,
    );
    assert_eq!(
        status, 200,
        "a satisfied conjunction must let the write through (seed={seed}): {ok}"
    );

    let (status, no) = cluster.dynamo(
        0,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"events","Item":{"pk":{"S":"p1"},"sk":{"S":"a0"},"seq":{"N":"0"}},
            "ConditionExpression":"attribute_exists(sk) AND seq > :one",
            "ExpressionAttributeValues":{":one":{"N":"1"}}}"#,
    );
    assert_eq!(
        status, 400,
        "one false conjunct must refuse the write (seed={seed}): {no}"
    );
    assert!(no.contains("ConditionalCheckFailed"), "seed={seed}: {no}");
}
