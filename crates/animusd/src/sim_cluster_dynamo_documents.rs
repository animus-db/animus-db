//! `SimCluster`-driven end-to-end tests of the extended DynamoDB JSON wire
//! surface (ADR 0061 rung D3 PR 3b, C-04 D3): document/set attribute types
//! (`M`/`L`/`SS`/`NS`/`BS`), projection expressions, `ReturnValues: ALL_OLD`,
//! multiple + composite GSIs alongside a local secondary index, and `N`-typed
//! partition-key routing — driven through `[crate::dynamo::dispatch_item_op]`
//! and, for the GSI half, `[SimCluster::drain_gsi]`.
//!
//! Replaces all three of `crates/animusd/tests/dynamo_documents.rs`'s tests
//! (now empty and deleted): `document_set_types_projection_and_return_
//! values`, `multiple_gsis_composite_gsi_and_lsi`, `n_partition_key_routes_
//! and_reads_correctly`. Only the middle one touches a GSI at all — the
//! other two are plain base-table exercises that never needed the drain.
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

/// `table`'s own (sole) tablet id, read off node 0's own `Metadata` — see
/// `sim_cluster_dynamo_query_pagination.rs`'s identical helper for why
/// `SimCluster::tablet_of` doesn't work here.
fn first_tablet(cluster: &SimCluster, table: &str) -> animus_tablet::TabletId {
    cluster
        .metadata(0)
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("{table} has no tablet"))
        .0
        .to_owned()
}

/// Document/set attribute types, projection expressions (including a
/// list-index projection), and `ReturnValues: ALL_OLD` on both `PutItem` and
/// `DeleteItem`. Mirrors `dynamo_documents.rs::
/// document_set_types_projection_and_return_values`.
#[test]
fn document_set_types_projection_and_return_values() {
    let seed = env_seed(0xE4D0_0001);
    let mut cluster = SimCluster::new(seed, 3, 3);

    let (status, _) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"profiles","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    );
    assert_eq!(status, 200);

    // PutItem carrying a map, a list, and a string set.
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"profiles","Item":{
            "id":{"S":"u1"},
            "name":{"S":"Ada"},
            "address":{"M":{"city":{"S":"London"},"zip":{"N":"7"}}},
            "scores":{"L":[{"N":"1"},{"N":"2"},{"S":"x"}]},
            "tags":{"SS":["b","a","a"]}
        }}"#,
    );
    assert_eq!(
        status, 200,
        "PutItem with document types failed (seed={seed}): {body}"
    );

    // GetItem round-trips the document/set types (set is sorted/deduped).
    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.GetItem",
        br#"{"ConsistentRead":true,"TableName":"profiles","Key":{"id":{"S":"u1"}}}"#,
    );
    assert_eq!(status, 200, "GetItem failed (seed={seed}): {body}");
    assert!(
        body.contains(r#""M":{"city":{"S":"London"}"#),
        "map (seed={seed}): {body}"
    );
    assert!(
        body.contains(r#""L":[{"N":"1"},{"N":"2"},{"S":"x"}]"#),
        "list (seed={seed}): {body}"
    );
    assert!(
        body.contains(r#""SS":["a","b"]"#),
        "set sorted/deduped (seed={seed}): {body}"
    );

    // GetItem with a ProjectionExpression (with a #name alias): only id + name.
    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.GetItem",
        br##"{"ConsistentRead":true,"TableName":"profiles","Key":{"id":{"S":"u1"}},
            "ProjectionExpression":"id, #n",
            "ExpressionAttributeNames":{"#n":"name"}}"##,
    );
    assert_eq!(
        status, 200,
        "projected GetItem failed (seed={seed}): {body}"
    );
    assert!(
        body.contains(r#""id":{"S":"u1"}"#),
        "id kept (seed={seed}): {body}"
    );
    assert!(
        body.contains(r#""name":{"S":"Ada"}"#),
        "name kept (seed={seed}): {body}"
    );
    assert!(
        !body.contains("address"),
        "address projected out (seed={seed}): {body}"
    );
    assert!(
        !body.contains("tags"),
        "tags projected out (seed={seed}): {body}"
    );

    // GetItem with a list-index ProjectionExpression: scores[0] and scores[2]
    // out of the 3-element list yield a *compacted* 2-element list (W-02).
    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.GetItem",
        br#"{"ConsistentRead":true,"TableName":"profiles","Key":{"id":{"S":"u1"}},
            "ProjectionExpression":"scores[0], scores[2]"}"#,
    );
    assert_eq!(
        status, 200,
        "list-index projected GetItem failed (seed={seed}): {body}"
    );
    assert!(
        body.contains(r#""scores":{"L":[{"N":"1"},{"S":"x"}]}"#),
        "compacted list projection (seed={seed}): {body}"
    );
    assert!(
        !body.contains("\"name\""),
        "name projected out (seed={seed}): {body}"
    );

    // A malformed list-index projection is a ValidationException, not a 500.
    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.GetItem",
        br#"{"TableName":"profiles","Key":{"id":{"S":"u1"}},
            "ProjectionExpression":"scores[x]"}"#,
    );
    assert_eq!(status, 400, "malformed list index (seed={seed}): {body}");
    assert!(body.contains("ValidationException"), "got: {body}");

    // ReturnValues: ALL_OLD on an overwrite echoes the prior item.
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"profiles","Item":{"id":{"S":"u1"},"name":{"S":"Grace"}},
            "ReturnValues":"ALL_OLD"}"#,
    );
    assert_eq!(status, 200, "ALL_OLD put failed (seed={seed}): {body}");
    assert!(
        body.contains("\"Attributes\""),
        "has Attributes (seed={seed}): {body}"
    );
    assert!(
        body.contains(r#""name":{"S":"Ada"}"#),
        "old name echoed (seed={seed}): {body}"
    );

    // ReturnValues: ALL_OLD on DeleteItem echoes the deleted item.
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.DeleteItem",
        br#"{"TableName":"profiles","Key":{"id":{"S":"u1"}},"ReturnValues":"ALL_OLD"}"#,
    );
    assert_eq!(status, 200, "ALL_OLD delete failed (seed={seed}): {body}");
    assert!(
        body.contains("\"Attributes\""),
        "has Attributes (seed={seed}): {body}"
    );
    assert!(
        body.contains(r#""name":{"S":"Grace"}"#),
        "deleted item echoed (seed={seed}): {body}"
    );
}

/// Two GSIs (one hash-only, one composite) and one LSI, all on the same
/// table. Mirrors `dynamo_documents.rs::multiple_gsis_composite_gsi_and_lsi`,
/// but drains the GSIs on demand via `[SimCluster::drain_gsi]` instead of
/// polling a background loop this fixture never runs.
#[test]
fn multiple_gsis_composite_gsi_and_lsi() {
    let seed = env_seed(0xE4D0_0002);
    let mut cluster = SimCluster::new(seed, 3, 3);

    // A composite (pk, sk) table with: two GSIs (one hash-only, one composite)
    // and one LSI (alternate sort attribute within the base partition).
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"events","AttributeDefinitions":[{"AttributeName":"actor","AttributeType":"S"},{"AttributeName":"kind","AttributeType":"S"},{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"},{"AttributeName":"ts","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-kind",
                 "KeySchema":[{"AttributeName":"kind","KeyType":"HASH"}]},
                {"IndexName":"by-actor-ts",
                 "KeySchema":[{"AttributeName":"actor","KeyType":"HASH"},
                              {"AttributeName":"ts","KeyType":"RANGE"}]}],
            "LocalSecondaryIndexes":[
                {"IndexName":"by-ts",
                 "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                              {"AttributeName":"ts","KeyType":"RANGE"}]}]}"#,
    );
    assert_eq!(status, 200, "CreateTable failed (seed={seed}): {body}");
    assert!(
        body.contains("\"IndexName\":\"by-kind\""),
        "gsi1 (seed={seed}): {body}"
    );
    assert!(
        body.contains("\"IndexName\":\"by-actor-ts\""),
        "gsi2 (seed={seed}): {body}"
    );
    assert!(
        body.contains("\"LocalSecondaryIndexes\""),
        "lsi present (seed={seed}): {body}"
    );

    // Items in partition "p1" with sort keys + a `kind`, `actor`, `ts`.
    let put = |cluster: &mut SimCluster, pk: &str, sk: &str, kind: &str, actor: &str, ts: &str| {
        let body = format!(
            r#"{{"TableName":"events","Item":{{
                "pk":{{"S":"{pk}"}},"sk":{{"S":"{sk}"}},
                "kind":{{"S":"{kind}"}},"actor":{{"S":"{actor}"}},"ts":{{"S":"{ts}"}}}}}}"#
        );
        let (status, b) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body.as_bytes());
        assert_eq!(status, 200, "PutItem failed (seed={seed}): {b}");
    };
    put(&mut cluster, "p1", "a", "click", "alice", "30");
    put(&mut cluster, "p1", "b", "view", "alice", "10");
    put(&mut cluster, "p1", "c", "click", "bob", "20");
    put(&mut cluster, "p2", "a", "click", "alice", "05");

    let tablet = first_tablet(&cluster, "events");
    let leader = cluster
        .leader_index_of(tablet)
        .expect("events tablet has a leader");
    cluster.drain_gsi(leader, "events");

    // Hash-only GSI by-kind = click: three items (p1/a, p1/c, p2/a).
    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","IndexName":"by-kind",
            "KeyConditionExpression":"kind = :k",
            "ExpressionAttributeValues":{":k":{"S":"click"}}}"#,
    );
    assert_eq!(status, 200, "by-kind query failed (seed={seed}): {body}");
    assert!(body.contains("\"Count\":3"), "seed={seed}: {body}");

    // Composite GSI by-actor-ts: actor=alice, ts BETWEEN 10 AND 30 → p1/a, p1/b.
    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","IndexName":"by-actor-ts",
            "KeyConditionExpression":"actor = :a AND ts BETWEEN :lo AND :hi",
            "ExpressionAttributeValues":{":a":{"S":"alice"},":lo":{"S":"10"},":hi":{"S":"30"}}}"#,
    );
    assert_eq!(
        status, 200,
        "by-actor-ts query failed (seed={seed}): {body}"
    );
    assert!(body.contains("\"Count\":2"), "seed={seed}: {body}");
    // p2/a (ts 05) is excluded by the BETWEEN; the items are ts-ordered (b, a).
    let b = body.find(r#""sk":{"S":"b"}"#).expect("b present");
    let a = body.find(r#""sk":{"S":"a"}"#).expect("a present");
    assert!(b < a, "composite GSI not ts-ordered (seed={seed}): {body}");
    assert!(
        !body.contains(r#""pk":{"S":"p2"}"#),
        "p2 excluded (seed={seed}): {body}"
    );

    // LSI by-ts within partition p1, ordered by ts: 10 (b), 20 (c), 30 (a).
    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.Query",
        br#"{"ConsistentRead":true,"TableName":"events","IndexName":"by-ts",
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}}}"#,
    );
    assert_eq!(status, 200, "LSI failed (seed={seed}): {body}");
    assert!(
        body.contains("\"Count\":3"),
        "LSI count (seed={seed}): {body}"
    );
    let b = body.find(r#""sk":{"S":"b"}"#).expect("b present");
    let c = body.find(r#""sk":{"S":"c"}"#).expect("c present");
    let a = body.find(r#""sk":{"S":"a"}"#).expect("a present");
    assert!(b < c && c < a, "LSI not ts-ordered (seed={seed}): {body}");

    // A sort condition on the hash-only GSI is rejected.
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","IndexName":"by-kind",
            "KeyConditionExpression":"kind = :k AND ts = :t",
            "ExpressionAttributeValues":{":k":{"S":"click"},":t":{"S":"30"}}}"#,
    );
    assert_eq!(status, 400, "expected rejection (seed={seed}): {body}");
    assert!(body.contains("ValidationException"), "got: {body}");
}

/// An `N` partition key routes and reads correctly (ADR 0063): the
/// canonicalized `numkey` bytes, not the raw decimal text, feed
/// `partition_token` — across mixed digit counts, a negative value, and an
/// exponent-notation literal (`1e2`) that canonicalizes to the same bytes a
/// plain `100` spelling would. Mirrors `dynamo_documents.rs::
/// n_partition_key_routes_and_reads_correctly`.
#[test]
fn n_partition_key_routes_and_reads_correctly() {
    let seed = env_seed(0xE4D0_0003);
    let mut cluster = SimCluster::new(seed, 3, 3);

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"sensors","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"N"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    );
    assert_eq!(status, 200, "CreateTable failed (seed={seed}): {body}");

    // Mixed digit counts, a negative, and an exponent-notation spelling of
    // 100 — distinct from every other value here once canonicalized.
    let ids = ["1", "10", "2", "-3", "1e2"];
    for id in ids {
        let (status, body) = cluster.dynamo(
            0,
            "DynamoDB_20120810.PutItem",
            format!(
                r#"{{"TableName":"sensors","Item":{{"id":{{"N":"{id}"}},"tag":{{"S":"t-{id}"}}}}}}"#
            )
            .as_bytes(),
        );
        assert_eq!(status, 200, "PutItem(id={id}) failed (seed={seed}): {body}");
    }

    // GetItem round-trips each value from a DIFFERENT node.
    for id in ids {
        let (status, body) = cluster.dynamo(
            1,
            "DynamoDB_20120810.GetItem",
            format!(
                r#"{{"TableName":"sensors","Key":{{"id":{{"N":"{id}"}}}},"ConsistentRead":true}}"#
            )
            .as_bytes(),
        );
        assert_eq!(status, 200, "GetItem(id={id}) failed (seed={seed}): {body}");
        assert!(
            body.contains(&format!(r#""id":{{"N":"{id}"}}"#)),
            "GetItem(id={id}) missing its own key (seed={seed}): {body}"
        );
        assert!(
            body.contains(&format!(r#""tag":{{"S":"t-{id}"}}"#)),
            "GetItem(id={id}) missing its own value (seed={seed}): {body}"
        );
    }

    // Query by an exact partition value.
    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"sensors","ConsistentRead":true,
            "KeyConditionExpression":"id = :v",
            "ExpressionAttributeValues":{":v":{"N":"-3"}}}"#,
    );
    assert_eq!(status, 200, "Query failed (seed={seed}): {body}");
    assert!(body.contains("\"Count\":1"), "seed={seed}: {body}");
    assert!(
        body.contains(r#""tag":{"S":"t--3"}"#),
        "seed={seed}: {body}"
    );

    // Scan returns exactly the five distinct rows — `1e2` colliding with a
    // separately-written `100` would show up here as a missing/duplicate row.
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Scan",
        br#"{"TableName":"sensors","ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "Scan failed (seed={seed}): {body}");
    assert!(body.contains("\"Count\":5"), "seed={seed}: {body}");

    // BatchGetItem recovers a subset by their N partition keys.
    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.BatchGetItem",
        br#"{"RequestItems":{"sensors":{"Keys":[{"id":{"N":"1"}},{"id":{"N":"1e2"}}]}}}"#,
    );
    assert_eq!(status, 200, "BatchGetItem failed (seed={seed}): {body}");
    assert!(body.contains(r#""tag":{"S":"t-1"}"#), "seed={seed}: {body}");
    assert!(
        body.contains(r#""tag":{"S":"t-1e2"}"#),
        "seed={seed}: {body}"
    );
    assert!(
        !body.contains("\"t-10\""),
        "unexpected id=10 row (seed={seed}): {body}"
    );

    // DeleteItem on one, then confirm it is really gone.
    let (status, _) = cluster.dynamo(
        0,
        "DynamoDB_20120810.DeleteItem",
        br#"{"TableName":"sensors","Key":{"id":{"N":"2"}}}"#,
    );
    assert_eq!(status, 200);
    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.GetItem",
        br#"{"TableName":"sensors","Key":{"id":{"N":"2"}},"ConsistentRead":true}"#,
    );
    assert_eq!(
        status, 200,
        "GetItem after delete failed (seed={seed}): {body}"
    );
    assert_eq!(body, "{}", "id=2 must be gone (seed={seed}): {body}");
}
