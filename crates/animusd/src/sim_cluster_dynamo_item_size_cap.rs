//! `SimCluster`-driven end-to-end test for the AWS 400 KB item-size cap
//! enforced on `UpdateItem`'s post-update result (ADR 0061 rung D3 PR 1) —
//! replaces the `update_item_rejects_a_post_update_result_over_the_cap`
//! test from the real-socket `ProdEnv` binary
//! `crates/animusd/tests/dynamo_item_size_cap.rs`. That file's sibling test,
//! `transact_write_items_update_action_rejects_a_post_update_result_over_the_cap`,
//! exercises `TransactWriteItems`, which `dynamo::dispatch_item_op` does
//! not cover yet (ADR 0061 rung D2 PR 2's own residual list) — it **stays
//! on `ProdEnv`**, along with the pure fixture-math sanity test
//! `fixture_sizes_straddle_the_cap`, both left in the original file.
//!
//! The `ProdEnv` fixture used a hash-only table (`"big"`, key attribute
//! `id`); `SimCluster::create_table` only builds a composite `(pk, sk)`
//! schema with those exact attribute names, so this uses `pk`/`sk` in
//! place of `id` — a fixture data-shape change only, since the size-cap
//! logic under test doesn't care about attribute names.
//!
//! Seed replay: `ANIMUS_SEED=<seed> cargo test -p animusd --lib <test name>`.

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// AWS's per-item size cap, mirrored from `animus_dynamo::wire::
/// MAX_ITEM_SIZE_BYTES`.
const MAX_ITEM_SIZE_BYTES: usize = 409_600;

fn long_string(n: usize) -> String {
    "x".repeat(n)
}

/// `UpdateItem` rejects a post-update result over the cap, and leaves the
/// original item untouched — the leader evaluates the whole action list,
/// finds the net result over `MAX_ITEM_SIZE_BYTES`, and never proposes the
/// write at all.
#[test]
fn update_item_rejects_a_post_update_result_over_the_cap() {
    let seed = env_seed(0x512E_0001);
    let mut cluster = SimCluster::new(seed, 1, 1);
    cluster.create_table("big");

    // item_size = len("pk")+len(id)+len("sk")+len(sk)+len("payload")+payload.len()
    //           = 2+2 + 2+1 + 7 + 350_000, comfortably under 409_600.
    let seed_item = format!(
        r#"{{"TableName":"big","Item":{{"pk":{{"S":"u1"}},"sk":{{"S":"s"}},
            "payload":{{"S":"{}"}}}}}}"#,
        long_string(350_000)
    );
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", seed_item.as_bytes());
    assert_eq!(status, 200, "seed PutItem failed (seed={seed}): {resp}");

    // A further ~70_000-byte "extra" attribute pushes the post-update
    // result well past MAX_ITEM_SIZE_BYTES.
    let over_cap_value = long_string(70_000);
    let update = format!(
        r#"{{"TableName":"big","Key":{{"pk":{{"S":"u1"}},"sk":{{"S":"s"}}}},
            "UpdateExpression":"SET extra = :v",
            "ExpressionAttributeValues":{{":v":{{"S":"{over_cap_value}"}}}}}}"#
    );
    let (status, body) = cluster.dynamo(0, "DynamoDB_20120810.UpdateItem", update.as_bytes());
    assert_eq!(
        status, 400,
        "an UpdateItem whose result exceeds the 400 KB cap must be rejected (seed={seed}): {body}"
    );
    assert!(
        body.contains("ValidationException"),
        "expected ValidationException (seed={seed}), got: {body}"
    );
    assert!(
        body.contains("Item size has exceeded the maximum allowed size"),
        "expected the size-cap message (seed={seed}), got: {body}"
    );

    // The rejected UpdateItem must not have landed: the pre-update item is
    // still exactly what PutItem wrote, with no "extra" attribute.
    let (status, after) = cluster.dynamo(
        0,
        "DynamoDB_20120810.GetItem",
        br#"{"TableName":"big","Key":{"pk":{"S":"u1"},"sk":{"S":"s"}},"ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {after}");
    assert!(
        after.contains(r#""payload""#),
        "the original item must survive the rejected update (seed={seed}): {after}"
    );
    assert!(
        !after.contains(r#""extra""#),
        "the rejected update's attribute must not have landed (seed={seed}): {after}"
    );
}

/// Sanity check that the payload sizes above genuinely straddle the cap —
/// mirrors `dynamo_item_size_cap.rs`'s own `fixture_sizes_straddle_the_cap`
/// for this file's own (renamed-attribute) fixture shape.
#[test]
fn fixture_sizes_straddle_the_cap() {
    let base = 2 + 2 + 2 + 1 + 7 + 350_000; // "pk"+"u1" + "sk"+"s" + "payload"+bytes
    let grown = base + ("extra".len() + 70_000);
    assert!(base < MAX_ITEM_SIZE_BYTES, "base fixture must be legal");
    assert!(
        grown > MAX_ITEM_SIZE_BYTES,
        "grown fixture must exceed the cap"
    );
}
