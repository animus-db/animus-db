//! Issue #1203: a single-item write with **no** `ConditionExpression` that
//! finds another transaction's unresolved write intent on its key must answer
//! `TransactionConflictException` (HTTP 400, as AWS does), not
//! `ConditionalCheckFailedException`. A genuinely false condition must keep
//! answering `ConditionalCheckFailedException`.
//!
//! The intent is staged directly on the item's physical base key with
//! `SimCluster::txn_prepare_only` (a prepared-but-never-resolved
//! transaction), then `PutItem`/`UpdateItem`/`DeleteItem` are driven over the
//! real wire from every node. `PutItem`/`DeleteItem` ask for `ALL_OLD` so they
//! take the evaluated (leader-side `KindEval`) path; a blind, condition-free
//! `Put`/`Delete` on a plain table takes `fast_marker_write` and never
//! evaluates, so it cannot observe an intent at all.
//!
//! Seed replay: `ANIMUS_SEED=<seed> cargo test -p animusd --lib
//! single_item_write_blocked_by_a_transaction_intent`.

use animus_dynamo::AttributeValue;

use super::dynamo::item_key;
use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn assert_conflict(seed: u64, node: u64, what: &str, (status, body): (u16, String)) {
    assert_eq!(status, 400, "seed={seed}: {what} on node {node}: {body}");
    assert!(
        body.contains("TransactionConflictException"),
        "seed={seed}: {what} on node {node} must be TransactionConflictException: {body}"
    );
    assert!(
        !body.contains("ConditionalCheckFailed"),
        "seed={seed}: {what} on node {node} must not be a condition failure: {body}"
    );
}

#[test]
fn single_item_write_blocked_by_a_transaction_intent_is_a_transaction_conflict() {
    let seed = env_seed(0x1203_0001);
    let mut cluster = SimCluster::new(seed, 3, 3);

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"intents","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable: {body}");

    // A committed neighbour with a known value, for the genuine-condition case.
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"intents","Item":{"pk":{"S":"free"},"n":{"N":"1"}}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: seed PutItem: {body}");

    // Leave a prepared-but-unresolved transaction intent on key "held".
    let held = item_key(&AttributeValue::S("held".into()), None);
    let _ = cluster.txn_prepare_only(
        0,
        "intents",
        None,
        Vec::new(),
        held.clone(),
        Some(b"x".to_vec()),
    );

    for n in 0..3u64 {
        assert_conflict(
            seed,
            n,
            "unconditional PutItem (ALL_OLD)",
            cluster.dynamo(
                n,
                "DynamoDB_20120810.PutItem",
                br#"{"TableName":"intents","Item":{"pk":{"S":"held"}},"ReturnValues":"ALL_OLD"}"#,
            ),
        );
        assert_conflict(
            seed,
            n,
            "unconditional UpdateItem",
            cluster.dynamo(
                n,
                "DynamoDB_20120810.UpdateItem",
                br#"{"TableName":"intents","Key":{"pk":{"S":"held"}},
                    "UpdateExpression":"SET items = list_append(if_not_exists(items, :e), :v)",
                    "ExpressionAttributeValues":{":e":{"L":[]},":v":{"L":[{"S":"a"}]}}}"#,
            ),
        );
        assert_conflict(
            seed,
            n,
            "unconditional DeleteItem (ALL_OLD)",
            cluster.dynamo(
                n,
                "DynamoDB_20120810.DeleteItem",
                br#"{"TableName":"intents","Key":{"pk":{"S":"held"}},"ReturnValues":"ALL_OLD"}"#,
            ),
        );
    }

    // A genuinely false condition on an unblocked key is still a condition failure.
    for (target, body) in [
        (
            "DynamoDB_20120810.PutItem",
            &br#"{"TableName":"intents","Item":{"pk":{"S":"free"}},
                "ConditionExpression":"attribute_not_exists(pk)"}"#[..],
        ),
        (
            "DynamoDB_20120810.UpdateItem",
            &br#"{"TableName":"intents","Key":{"pk":{"S":"free"}},
                "UpdateExpression":"SET n = :z","ConditionExpression":"n = :bad",
                "ExpressionAttributeValues":{":z":{"N":"0"},":bad":{"N":"99"}}}"#[..],
        ),
        (
            "DynamoDB_20120810.DeleteItem",
            &br#"{"TableName":"intents","Key":{"pk":{"S":"free"}},
                "ConditionExpression":"n = :bad",
                "ExpressionAttributeValues":{":bad":{"N":"99"}}}"#[..],
        ),
    ] {
        let (status, resp) = cluster.dynamo(1, target, body);
        assert_eq!(status, 400, "seed={seed}: {target}: {resp}");
        assert!(
            resp.contains("ConditionalCheckFailedException"),
            "seed={seed}: {target} must stay a condition failure: {resp}"
        );
    }
}
