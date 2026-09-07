//! `SimCluster`-driven end-to-end tests for `UpdateItem`'s `UPDATED_OLD` /
//! `UPDATED_NEW` return values (ADR 0061 rung D3 PR 1) — replaces the
//! real-socket `ProdEnv` binary
//! `crates/animusd/tests/dynamo_updated_return_values.rs`, whose three
//! tests are all base-table-only. Driven through `SimCluster::dynamo` —
//! see `sim_cluster_dynamo.rs`'s own module doc.
//!
//! `UPDATED_OLD`/`UPDATED_NEW` differ from `ALL_OLD`/`ALL_NEW` by reporting
//! **only the attributes the update actually changed** — a diff of the two
//! images rather than a projection of one.
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

/// The diff, both directions, in one update: one attribute edited, one
/// created, one removed, one left alone.
#[test]
fn updated_old_and_new_report_only_what_changed() {
    let seed = env_seed(0x0DA7_0001);
    let mut cluster = setup(seed);

    // Seed an item with a `doomed` attribute the update will remove.
    let (status, seeded) = cluster.dynamo(
        0,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"events","Item":{"pk":{"S":"p1"},"sk":{"S":"a0"},
            "cat":{"S":"X"},"doomed":{"S":"bye"},"parity":{"S":"even"}}}"#,
    );
    assert_eq!(status, 200, "seed failed (seed={seed}): {seeded}");

    let (status, old) = cluster.dynamo(
        1,
        "DynamoDB_20120810.UpdateItem",
        br#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a0"}},
            "UpdateExpression":"SET cat = :y, born = :b REMOVE doomed",
            "ExpressionAttributeValues":{":y":{"S":"Y"},":b":{"S":"new"}},
            "ReturnValues":"UPDATED_OLD"}"#,
    );
    assert_eq!(status, 200, "UPDATED_OLD failed (seed={seed}): {old}");
    assert!(
        old.contains(r#""cat":{"S":"X"}"#),
        "the edited attribute's old value (seed={seed}): {old}"
    );
    assert!(
        old.contains(r#""doomed":{"S":"bye"}"#),
        "a removed attribute has an old value (seed={seed}): {old}"
    );
    assert!(
        !old.contains("born"),
        "a created attribute has no old value (seed={seed}): {old}"
    );
    assert!(
        !old.contains("parity"),
        "an untouched attribute is not reported (seed={seed}): {old}"
    );
    assert!(
        !old.contains(r#""sk""#),
        "the key never changes (seed={seed}): {old}"
    );

    // Now the same shape in the other direction.
    let (status, new) = cluster.dynamo(
        2,
        "DynamoDB_20120810.UpdateItem",
        br#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a0"}},
            "UpdateExpression":"SET cat = :z, later = :l REMOVE born",
            "ExpressionAttributeValues":{":z":{"S":"Z"},":l":{"S":"yes"}},
            "ReturnValues":"UPDATED_NEW"}"#,
    );
    assert_eq!(status, 200, "UPDATED_NEW failed (seed={seed}): {new}");
    assert!(
        new.contains(r#""cat":{"S":"Z"}"#),
        "the edited attribute's new value (seed={seed}): {new}"
    );
    assert!(
        new.contains(r#""later":{"S":"yes"}"#),
        "a created attribute has a new value (seed={seed}): {new}"
    );
    assert!(
        !new.contains("born"),
        "a removed attribute has no new value (seed={seed}): {new}"
    );
    assert!(!new.contains("parity"), "seed={seed}: {new}");
}

/// An update that changes nothing reports no `Attributes` at all, rather
/// than an empty map.
#[test]
fn an_update_that_changes_nothing_reports_no_attributes() {
    let seed = env_seed(0x0DA7_0002);
    let mut cluster = setup(seed);

    // Set `cat` to the value it already has.
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.UpdateItem",
        br#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a1"}},
            "UpdateExpression":"SET cat = :same",
            "ExpressionAttributeValues":{":same":{"S":"X"}},
            "ReturnValues":"UPDATED_NEW"}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {body}");
    assert!(
        !body.contains("Attributes"),
        "nothing changed, so Attributes is omitted entirely (seed={seed}): {body}"
    );
}

/// `ALL_OLD`/`ALL_NEW` still return the whole item — the contrast that
/// shows `UPDATED_*` really is narrowing.
#[test]
fn all_variants_still_return_the_whole_item() {
    let seed = env_seed(0x0DA7_0003);
    let mut cluster = setup(seed);

    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.UpdateItem",
        br#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"a2"}},
            "UpdateExpression":"SET cat = :y",
            "ExpressionAttributeValues":{":y":{"S":"Y"}},
            "ReturnValues":"ALL_NEW"}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {body}");
    assert!(body.contains(r#""cat":{"S":"Y"}"#), "seed={seed}: {body}");
    assert!(
        body.contains("parity") && body.contains(r#""sk""#),
        "ALL_NEW carries untouched attributes and the key (seed={seed}): {body}"
    );
}
