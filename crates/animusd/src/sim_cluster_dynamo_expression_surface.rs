//! `SimCluster`-driven end-to-end tests for the full
//! `FilterExpression`/`ConditionExpression` surface over the DynamoDB wire
//! (ADR 0061 rung D3 PR 1) — replaces the real-socket `ProdEnv` binary
//! `crates/animusd/tests/dynamo_expression_surface.rs`, whose four tests
//! are all base-table-only. Driven through `SimCluster::dynamo` — see
//! `sim_cluster_dynamo.rs`'s own module doc.
//!
//! The property worth pinning is that **numbers compare numerically**
//! (`price > :p` must not fall back to a byte-text compare, where 9 would
//! outrank 10).
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

/// Comparators against a numeric attribute, over the wire. The 9-vs-10 case
/// is the one a lexicographic shortcut gets wrong.
#[test]
fn numeric_comparators_filter_numerically() {
    let seed = env_seed(0xE5F0_0001);
    let mut cluster = setup(seed);

    let q = |cluster: &mut SimCluster, op: &str, v: &str| -> (u16, String) {
        let body = format!(
            r#"{{"TableName":"events","ConsistentRead":true,
                 "KeyConditionExpression":"pk = :p",
                 "FilterExpression":"seq {op} :v",
                 "ExpressionAttributeValues":{{":p":{{"S":"p1"}},":v":{{"N":"{v}"}}}}}}"#
        );
        cluster.dynamo(0, "DynamoDB_20120810.Query", body.as_bytes())
    };

    let (status, ge) = q(&mut cluster, ">=", "3");
    assert_eq!(status, 200, "`>=` must now be served (seed={seed}): {ge}");
    assert!(
        ge.contains("\"a3\"") && ge.contains("\"a5\""),
        "seed={seed}: {ge}"
    );
    assert!(
        !ge.contains("\"a2\""),
        "and bounded below (seed={seed}): {ge}"
    );

    let (_, lt) = q(&mut cluster, "<", "2");
    assert!(
        lt.contains("\"a0\"") && lt.contains("\"a1\""),
        "seed={seed}: {lt}"
    );
    assert!(!lt.contains("\"a2\""), "seed={seed}: {lt}");

    let (_, ne) = q(&mut cluster, "<>", "0");
    assert!(
        !ne.contains("\"a0\""),
        "`<>` excludes the equal one (seed={seed}): {ne}"
    );
    assert!(ne.contains("\"a5\""), "seed={seed}: {ne}");
}

/// BETWEEN, IN, begins_with, contains, attribute_type and size over the wire.
#[test]
fn the_function_and_range_forms_serve() {
    let seed = env_seed(0xE5F0_0002);
    let mut cluster = setup(seed);

    let run = |cluster: &mut SimCluster, frag: &str| -> (u16, String) {
        let body = format!(
            r#"{{"TableName":"events","ConsistentRead":true,
                 "KeyConditionExpression":"pk = :p",
                 "FilterExpression":"{frag}",
                 "ExpressionAttributeValues":{{":p":{{"S":"p1"}},
                    ":lo":{{"N":"1"}},":hi":{{"N":"3"}},
                    ":one":{{"N":"1"}},":five":{{"N":"5"}},
                    ":pre":{{"S":"a"}},":sub":{{"S":"3"}},
                    ":ty":{{"S":"S"}},":two":{{"N":"2"}}}}}}"#
        );
        cluster.dynamo(1, "DynamoDB_20120810.Query", body.as_bytes())
    };

    let (status, between) = run(&mut cluster, "seq BETWEEN :lo AND :hi");
    assert_eq!(status, 200, "BETWEEN failed (seed={seed}): {between}");
    assert!(
        between.contains("\"a1\"") && between.contains("\"a3\""),
        "seed={seed}: {between}"
    );
    assert!(
        !between.contains("\"a0\"") && !between.contains("\"a4\""),
        "seed={seed}: {between}"
    );

    let (_, in_) = run(&mut cluster, "seq IN (:one, :five)");
    assert!(
        in_.contains("\"a1\"") && in_.contains("\"a5\""),
        "seed={seed}: {in_}"
    );
    assert!(!in_.contains("\"a2\""), "seed={seed}: {in_}");

    let (_, begins) = run(&mut cluster, "begins_with(sk, :pre)");
    assert!(
        begins.contains("\"a0\""),
        "every sk begins with `a` (seed={seed}): {begins}"
    );

    let (_, contains) = run(&mut cluster, "contains(sk, :sub)");
    assert!(contains.contains("\"a3\""), "seed={seed}: {contains}");
    assert!(!contains.contains("\"a1\""), "seed={seed}: {contains}");

    let (_, ty) = run(&mut cluster, "attribute_type(sk, :ty)");
    assert!(ty.contains("\"a0\""), "sk is a string (seed={seed}): {ty}");

    let (_, size) = run(&mut cluster, "size(sk) = :two");
    assert!(
        size.contains("\"a0\""),
        "`a0` is two bytes (seed={seed}): {size}"
    );
}

/// `size()` on an attribute that **exists** with a type it has no size for
/// (`N`) is a real DynamoDB `ValidationException`, not a false filter
/// match. Covers both evaluation paths the wire shares: a
/// `Scan`/`Query` `FilterExpression` and a `PutItem` `ConditionExpression`,
/// the latter issued through **every** node so at least one forwarded
/// `KindWriteItem` hop is exercised (the original defect this test guards
/// was placement-dependent: the leader-local send returned the correct 400
/// while a forwarded one degraded to a 500).
#[test]
fn size_of_an_existing_number_attribute_is_a_validation_exception() {
    let seed = env_seed(0xE5F0_0003);
    let mut cluster = setup(seed);

    // `seq` is an `N` on every seeded item — `size()` has no meaning for it.
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Scan",
        br#"{"TableName":"events","ConsistentRead":true,
            "FilterExpression":"size(seq) > :zero",
            "ExpressionAttributeValues":{":zero":{"N":"0"}}}"#,
    );
    assert_eq!(
        status, 400,
        "size() on an existing N attribute must be rejected, not just false (seed={seed}): {body}"
    );
    assert!(body.contains("ValidationException"), "seed={seed}: {body}");
    assert!(
        body.contains("operator or function: size, operand type: N"),
        "message should match AWS's own wording (seed={seed}): {body}"
    );

    for node in 0..cluster.node_count() as u64 {
        let (status, body) = cluster.dynamo(
            node,
            "DynamoDB_20120810.PutItem",
            br#"{"TableName":"events","Item":{"pk":{"S":"p1"},"sk":{"S":"a0"},"seq":{"N":"0"}},
                "ConditionExpression":"size(seq) > :zero",
                "ExpressionAttributeValues":{":zero":{"N":"0"}}}"#,
        );
        assert_eq!(
            status, 400,
            "node {node}: the same size()-on-N error must reach a conditional \
             write too (a forwarded hop must not degrade it to a 500) (seed={seed}): {body}"
        );
        assert!(
            body.contains("ValidationException"),
            "node {node} seed={seed}: {body}"
        );
        assert!(
            !body.contains("ConditionalCheckFailed"),
            "node {node} seed={seed}: an operand-type violation is a ValidationException, \
             not a failed condition check: {body}"
        );
    }
}

/// The same surface reaches a **conditional write**, which shares the decoder.
#[test]
fn conditional_writes_use_the_same_surface() {
    let seed = env_seed(0xE5F0_0004);
    let mut cluster = setup(seed);

    // seq of a0 is 0; require seq < 1, which holds.
    let (status, ok) = cluster.dynamo(
        2,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"events","Item":{"pk":{"S":"p1"},"sk":{"S":"a0"},"seq":{"N":"0"},"won":{"S":"yes"}},
            "ConditionExpression":"seq < :one",
            "ExpressionAttributeValues":{":one":{"N":"1"}}}"#,
    );
    assert_eq!(
        status, 200,
        "a satisfiable comparison must let the write through (seed={seed}): {ok}"
    );

    // And one that does not hold is refused, not silently applied.
    let (status, no) = cluster.dynamo(
        2,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"events","Item":{"pk":{"S":"p1"},"sk":{"S":"a0"},"seq":{"N":"0"}},
            "ConditionExpression":"seq > :one",
            "ExpressionAttributeValues":{":one":{"N":"1"}}}"#,
    );
    assert_eq!(
        status, 400,
        "an unsatisfied condition must refuse (seed={seed}): {no}"
    );
    assert!(no.contains("ConditionalCheckFailed"), "seed={seed}: {no}");
}
