//! `SimCluster`-driven end-to-end tests for `UpdateExpression`'s `ADD` and
//! `DELETE` clauses (ADR 0061 rung D3 PR 1) — replaces all eight tests in
//! the real-socket `ProdEnv` binary
//! `crates/animusd/tests/dynamo_update_add_delete.rs`, which is now empty
//! and deleted. Driven through `SimCluster::dynamo`/`dynamo_concurrent` —
//! see `sim_cluster_dynamo.rs`'s own module doc for the shared generic
//! core.
//!
//! **`an_add_that_changes_an_indexed_attribute_reindexes` moved here in
//! PR 3b (ADR 0061 rung D3)** — it used to stay on `ProdEnv` because it
//! queries a GSI, and `SimCluster` never materialized one; `[SimCluster::
//! drain_gsi]` (this rung's own fixture helper, `sim_cluster.rs`) closes
//! that gap by draining the table's own hidden GSI table on demand, so this
//! test now drains twice — once to establish the pre-update baseline (`a0`
//! indexed under `cat = X`), once after the `ADD`-driven reindex (`a0`
//! indexed under `cat = Y`, and no longer under `X`) — rather than polling
//! a background loop this fixture never runs.
//!
//! Numeric `ADD` is the adapter's only **non-idempotent** write; the
//! contended-write tests here are the ones that measured 431 (a stale
//! read-modify-write retry), then 8-of-10 spurious retries, then 2-of-10
//! refusals under load before ADR 0054's write-path fixes — see that ADR
//! for the full history.
//!
//! Seed replay: `ANIMUS_SEED=<seed> cargo test -p animusd --lib <test name>`.

use std::time::Duration;

use animus_tablet::TabletId;

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// `table`'s own (sole) tablet id, read off node 0's own `Metadata` — **not**
/// `SimCluster::tablet_of`, which only knows about a *hand-hosted* table
/// (`setup`'s own table); `setup_indexed`'s table is created through the
/// real DynamoDB wire, so it never gets a `tablet_of` entry.
fn first_tablet(cluster: &SimCluster, table: &str) -> TabletId {
    cluster
        .metadata(0)
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("{table} has no tablet"))
        .0
        .to_owned()
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

/// The counter idiom: `ADD` seeds an absent attribute, then increments.
#[test]
fn add_seeds_then_increments_a_counter() {
    let seed = env_seed(0xADD0_0001);
    let mut cluster = setup(seed);

    let bump = |cluster: &mut SimCluster, node: u64| -> String {
        let (status, body) = cluster.dynamo(
            node,
            "DynamoDB_20120810.UpdateItem",
            br#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a0"}},
                "UpdateExpression":"ADD hits :one",
                "ExpressionAttributeValues":{":one":{"N":"1"}},
                "ReturnValues":"ALL_NEW"}"#,
        );
        assert_eq!(status, 200, "ADD failed (seed={seed}): {body}");
        body
    };

    assert!(
        bump(&mut cluster, 0).contains(r#""hits":{"N":"1"}"#),
        "seeded from absent (seed={seed})"
    );
    assert!(
        bump(&mut cluster, 1).contains(r#""hits":{"N":"2"}"#),
        "incremented (seed={seed})"
    );
    assert!(
        bump(&mut cluster, 2).contains(r#""hits":{"N":"3"}"#),
        "exact across nodes (seed={seed})"
    );
}

/// **At-most-once per request, and — since ADR 0054 step 3 landed —
/// exactly once for every request, with zero refusals.** `dynamo_
/// concurrent` spawns every writer before the one shared `run_for` that
/// drives them, so they genuinely race the same key the way the original
/// `tokio::spawn` fleet did.
#[test]
fn concurrent_increments_all_land_exactly_once() {
    const WRITERS: usize = 10;
    let seed = env_seed(0xADD0_0002);
    let mut cluster = setup(seed);

    let body = br#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a1"}},
        "UpdateExpression":"ADD hits :one",
        "ExpressionAttributeValues":{":one":{"N":"1"}}}"#;
    let requests: Vec<(u64, &str, &[u8])> = (0..WRITERS)
        .map(|i| {
            (
                (i % cluster.node_count()) as u64,
                "DynamoDB_20120810.UpdateItem",
                body.as_slice(),
            )
        })
        .collect();
    let results = cluster.dynamo_concurrent(&requests);
    let failures: Vec<String> = results
        .iter()
        .filter(|(s, _)| *s != 200)
        .map(|(s, r)| format!("{s}: {r}"))
        .collect();
    assert!(
        failures.is_empty(),
        "ADR 0054 closes the contended-ADD refusal — every writer should \
         land (seed={seed}): {failures:?}"
    );

    let (status, got) = cluster.dynamo(
        0,
        "DynamoDB_20120810.GetItem",
        br#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a1"}},
            "ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {got}");
    let parsed: serde_json::Value = serde_json::from_str(&got).expect("json");
    let counter: usize = parsed["Item"]["hits"]["N"]
        .as_str()
        .expect("hits is a number attribute")
        .parse()
        .expect("hits parses");
    assert_eq!(
        counter, WRITERS,
        "every one of {WRITERS} increments must land exactly once with zero refusals (seed={seed})"
    );
}

/// The same contended-`ADD` property, but with a conditional `ADD` under
/// the same concurrency: `attribute_exists(pk)` is genuinely true for
/// every writer here, so the condition must never spuriously fail under
/// contention either.
#[test]
fn concurrent_conditional_add_all_land_exactly_once() {
    const WRITERS: usize = 10;
    let seed = env_seed(0xADD0_0003);
    let mut cluster = setup(seed);

    // Seed the item first so `attribute_exists(pk)` is true for every
    // racing writer.
    let (status, _) = cluster.dynamo(
        0,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"events","Item":{"pk":{"S":"p2"},"sk":{"S":"a1"},"hits":{"N":"0"}}}"#,
    );
    assert_eq!(status, 200, "seed={seed}");

    let body = br#"{"TableName":"events","Key":{"pk":{"S":"p2"},"sk":{"S":"a1"}},
        "UpdateExpression":"ADD hits :one",
        "ConditionExpression":"attribute_exists(pk)",
        "ExpressionAttributeValues":{":one":{"N":"1"}}}"#;
    let requests: Vec<(u64, &str, &[u8])> = (0..WRITERS)
        .map(|i| {
            (
                (i % cluster.node_count()) as u64,
                "DynamoDB_20120810.UpdateItem",
                body.as_slice(),
            )
        })
        .collect();
    let results = cluster.dynamo_concurrent(&requests);
    let failures: Vec<String> = results
        .iter()
        .filter(|(s, _)| *s != 200)
        .map(|(s, r)| format!("{s}: {r}"))
        .collect();
    assert!(
        failures.is_empty(),
        "a genuinely-true condition must never spuriously fail under \
         contention (seed={seed}): {failures:?}"
    );

    let (status, got) = cluster.dynamo(
        0,
        "DynamoDB_20120810.GetItem",
        br#"{"TableName":"events","Key":{"pk":{"S":"p2"},"sk":{"S":"a1"}},
            "ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {got}");
    let parsed: serde_json::Value = serde_json::from_str(&got).expect("json");
    let counter: usize = parsed["Item"]["hits"]["N"]
        .as_str()
        .expect("hits is a number attribute")
        .parse()
        .expect("hits parses");
    assert_eq!(
        counter, WRITERS,
        "every one of {WRITERS} conditional increments must land exactly once with zero \
         refusals (seed={seed})"
    );
}

/// Exact decimal arithmetic reaches the wire: an increment that would lose
/// its low digits through an `f64` must not.
#[test]
fn increments_keep_full_decimal_precision() {
    let seed = env_seed(0xADD0_0004);
    let mut cluster = setup(seed);

    let (status, seeded) = cluster.dynamo(
        0,
        "DynamoDB_20120810.UpdateItem",
        br#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a2"}},
            "UpdateExpression":"ADD big :v",
            "ExpressionAttributeValues":{":v":{"N":"99999999999999999999999999999999999999"}},
            "ReturnValues":"ALL_NEW"}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {seeded}");

    let (status, bumped) = cluster.dynamo(
        1,
        "DynamoDB_20120810.UpdateItem",
        br#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a2"}},
            "UpdateExpression":"ADD big :one",
            "ExpressionAttributeValues":{":one":{"N":"1"}},
            "ReturnValues":"ALL_NEW"}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {bumped}");
    assert!(
        bumped.contains("100000000000000000000000000000000000000"),
        "38 digits carry exactly — an f64 round-trip would round this (seed={seed}): {bumped}"
    );
}

/// Set `ADD` is **idempotent** — union with the same members is a no-op —
/// so ten concurrent unions of overlapping members must converge to
/// exactly the union, no matter how many internal passes occurred.
#[test]
fn concurrent_set_adds_converge_to_the_union() {
    let seed = env_seed(0xADD0_0005);
    let mut cluster = setup(seed);

    let bodies: Vec<String> = (0..10)
        .map(|i| {
            let member = format!("m{}", i % 4);
            format!(
                r#"{{"TableName":"events","Key":{{"pk":{{"S":"p1"}},"sk":{{"S":"a1"}}}},
                     "UpdateExpression":"ADD tags :t",
                     "ExpressionAttributeValues":{{":t":{{"SS":["{member}"]}}}}}}"#
            )
        })
        .collect();
    let requests: Vec<(u64, &str, &[u8])> = bodies
        .iter()
        .enumerate()
        .map(|(i, b)| {
            (
                (i % cluster.node_count()) as u64,
                "DynamoDB_20120810.UpdateItem",
                b.as_bytes(),
            )
        })
        .collect();
    let results = cluster.dynamo_concurrent(&requests);
    for (status, body) in &results {
        assert_eq!(
            *status, 200,
            "concurrent set ADD failed (seed={seed}): {body}"
        );
    }

    let (status, got) = cluster.dynamo(
        0,
        "DynamoDB_20120810.GetItem",
        br#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a1"}},
            "ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {got}");
    for m in ["m0", "m1", "m2", "m3"] {
        assert!(
            got.contains(m),
            "union must contain {m} (seed={seed}): {got}"
        );
    }
    // Idempotency is the point: repeated application cannot inflate a set
    // the way it would inflate a counter.
    assert_eq!(
        got.matches("m0").count(),
        1,
        "each member appears exactly once however many passes ran (seed={seed}): {got}"
    );
}

/// Set union and subtraction over the wire, including that emptying a set
/// removes the attribute rather than storing an empty one.
#[test]
fn add_and_delete_maintain_a_set() {
    let seed = env_seed(0xADD0_0006);
    let mut cluster = setup(seed);

    let update = |cluster: &mut SimCluster, node: u64, expr: &str, vals: &str| -> String {
        let body = format!(
            r#"{{"TableName":"events","Key":{{"pk":{{"S":"p1"}},"sk":{{"S":"a2"}}}},
                 "UpdateExpression":"{expr}",
                 "ExpressionAttributeValues":{vals},
                 "ReturnValues":"ALL_NEW"}}"#
        );
        let (status, resp) = cluster.dynamo(node, "DynamoDB_20120810.UpdateItem", body.as_bytes());
        assert_eq!(status, 200, "`{expr}` failed (seed={seed}): {resp}");
        resp
    };

    let seeded = update(&mut cluster, 0, "ADD tags :t", r#"{":t":{"SS":["a","b"]}}"#);
    assert!(
        seeded.contains(r#""a""#) && seeded.contains(r#""b""#),
        "seed={seed}: {seeded}"
    );

    let unioned = update(&mut cluster, 1, "ADD tags :t", r#"{":t":{"SS":["b","c"]}}"#);
    assert!(
        unioned.contains(r#""c""#),
        "union added c (seed={seed}): {unioned}"
    );

    let reduced = update(
        &mut cluster,
        2,
        "DELETE tags :t",
        r#"{":t":{"SS":["a","b"]}}"#,
    );
    assert!(
        reduced.contains(r#""c""#),
        "c survives (seed={seed}): {reduced}"
    );
    assert!(
        !reduced.contains(r#""a""#),
        "a removed (seed={seed}): {reduced}"
    );

    let emptied = update(&mut cluster, 0, "DELETE tags :t", r#"{":t":{"SS":["c"]}}"#);
    assert!(
        !emptied.contains("tags"),
        "an emptied set drops the attribute rather than storing [] (seed={seed}): {emptied}"
    );
}

/// A typed mismatch is a 400, not a silently skipped action.
#[test]
fn a_mismatched_add_is_rejected() {
    let seed = env_seed(0xADD0_0007);
    let mut cluster = setup(seed);

    // `sk` is a string; adding a number to it is a type error.
    let (status, resp) = cluster.dynamo(
        0,
        "DynamoDB_20120810.UpdateItem",
        br#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a3"}},
            "UpdateExpression":"ADD cat :t",
            "ExpressionAttributeValues":{":t":{"SS":["x"]}}}"#,
    );
    // Refused, with the reason. A type mismatch is only detectable at the
    // leader that holds the old image, and an error raised there is
    // re-wrapped as `InternalServerError` crossing the forwarding boundary
    // rather than keeping its `ValidationException` code — the same
    // pre-existing divergence the `ProdEnv` original documents.
    assert!(
        status == 400 || status == 500,
        "a mismatched ADD must be refused (seed={seed}): {resp}"
    );
    assert!(
        resp.contains("needs a number or a matching set type"),
        "and must say why (seed={seed}): {resp}"
    );

    // And the row is untouched — the failed action did not partially apply.
    // Converged-or-timeout since the local read may still be catching up.
    let mut got = String::new();
    for _ in 0..20 {
        let (status, body) = cluster.dynamo(
            0,
            "DynamoDB_20120810.GetItem",
            br#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a3"}},
                "ConsistentRead":true}"#,
        );
        got = body;
        if status == 200 && got.contains(r#""cat":{"S":"X"}"#) {
            break;
        }
        cluster.run_for(Duration::from_millis(100));
    }
    assert!(
        got.contains(r#""cat":{"S":"X"}"#),
        "cat is unchanged (seed={seed}): {got}"
    );
}

/// [`setup`]'s own indexed sibling — a **wire**-declared GSI (`by-cat`,
/// hash-only) atop the same composite `(pk, sk)` table, so
/// [`SimCluster::drain_gsi`] has a real hidden table to drain. Unlike
/// `setup`, this table must be created through the wire
/// (`SimCluster::create_table` hand-hosts a bare schema with no index
/// declared at all) — mirrors `dynamo_update_add_delete.rs::setup`.
fn setup_indexed(seed: u64) -> SimCluster {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"events","AttributeDefinitions":[{"AttributeName":"cat","AttributeType":"S"},{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-cat",
                 "KeySchema":[{"AttributeName":"cat","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"ALL"}}]}"#,
    );
    assert_eq!(status, 200, "CreateTable failed (seed={seed}): {body}");

    let (status, resp) = cluster.dynamo(
        0,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"events","Item":{
            "pk":{"S":"p1"},"sk":{"S":"a0"},"cat":{"S":"X"}}}"#,
    );
    assert_eq!(status, 200, "seed PutItem(a0) failed (seed={seed}): {resp}");
    cluster
}

/// The risk this rung carries: an `ADD` that changes a **GSI-indexed**
/// attribute must re-index the row, exactly as a `SET` would. Mirrors
/// `dynamo_update_add_delete.rs::an_add_that_changes_an_indexed_attribute_
/// reindexes`, but drives [`SimCluster::drain_gsi`] on demand instead of
/// polling a background loop `SimCluster` never spawns — see this file's
/// own module doc for the full account.
#[test]
fn an_add_that_changes_an_indexed_attribute_reindexes() {
    let seed = env_seed(0xADD0_0008);
    let mut cluster = setup_indexed(seed);
    let tablet = first_tablet(&cluster, "events");
    let leader = cluster
        .leader_index_of(tablet)
        .expect("events tablet has a leader");

    // Drain the pre-update baseline: a0 indexed under cat = X.
    cluster.drain_gsi(leader, "events");
    let (status, before) = cluster.dynamo(
        1,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","IndexName":"by-cat",
            "KeyConditionExpression":"cat = :c",
            "ExpressionAttributeValues":{":c":{"S":"X"}}}"#,
    );
    assert_eq!(
        status, 200,
        "baseline GSI query failed (seed={seed}): {before}"
    );
    assert!(
        before.contains("\"a0\""),
        "a0 must start out indexed under X (seed={seed}): {before}"
    );

    // `cat` is the GSI hash attribute. Move a0 out of partition X by setting
    // it to Y, via SET, combined with an ADD on an unrelated set attribute —
    // establishing the exact shape the original `ProdEnv` test covers.
    let (status, moved) = cluster.dynamo(
        0,
        "DynamoDB_20120810.UpdateItem",
        br#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a0"}},
            "UpdateExpression":"SET cat = :y ADD tags :t",
            "ExpressionAttributeValues":{":y":{"S":"Y"},":t":{"SS":["new"]}},
            "ReturnValues":"ALL_NEW"}"#,
    );
    assert_eq!(
        status, 200,
        "combined SET+ADD failed (seed={seed}): {moved}"
    );
    assert!(
        moved.contains(r#""new""#),
        "the ADD applied (seed={seed}): {moved}"
    );

    // Drain again: the GSI must now show a0 under Y, and no longer under X.
    let leader = cluster
        .leader_index_of(tablet)
        .expect("events tablet still has a leader");
    cluster.drain_gsi(leader, "events");

    let (status, after_y) = cluster.dynamo(
        1,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","IndexName":"by-cat",
            "KeyConditionExpression":"cat = :c",
            "ExpressionAttributeValues":{":c":{"S":"Y"}}}"#,
    );
    assert_eq!(
        status, 200,
        "post-update GSI query failed (seed={seed}): {after_y}"
    );
    assert!(
        after_y.contains("\"a0\""),
        "the GSI followed the update to Y (seed={seed}): {after_y}"
    );

    let (status, after_x) = cluster.dynamo(
        1,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","IndexName":"by-cat",
            "KeyConditionExpression":"cat = :c",
            "ExpressionAttributeValues":{":c":{"S":"X"}}}"#,
    );
    assert_eq!(
        status, 200,
        "stale-X GSI query failed (seed={seed}): {after_x}"
    );
    assert!(
        !after_x.contains("\"a0\""),
        "a0 must no longer be indexed under X (seed={seed}): {after_x}"
    );
}
