//! `SimCluster`-driven end-to-end tests for the `KindBatch` apply-time
//! outcome channel (ADR 0061 rung D3 PR 1) — replaces the real-socket
//! `ProdEnv` binary `crates/animusd/tests/kind_batch_outcome.rs`, whose
//! three tests are all base-table-only. Driven through
//! `SimCluster::dynamo`/`dynamo_concurrent` — see `sim_cluster_dynamo.rs`'s
//! own module doc for the shared generic core.
//!
//! A CP kind write used to be confirmed by reading the key back and
//! comparing values, which cannot distinguish "my entry no-op'd" from "my
//! entry applied and a concurrent write then overwrote it" (a success). The
//! entry now records what it did, keyed by its Raft log index, so the
//! proposer asks the entry rather than guessing from the value — see
//! `write_path.rs`'s own `poll_probe` doc for the mechanism.
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

/// The headline property: concurrent writers to one key all succeed.
/// Before the outcome channel this reported ~6 failures in 10 in the real
/// `ProdEnv` fixture; every write here genuinely applies (last-writer-wins
/// on the same key), so every request must be acknowledged.
#[test]
fn concurrent_writes_to_one_key_all_succeed() {
    let seed = env_seed(0xBA7C_0001);
    let mut cluster = setup(seed);

    const WRITERS: usize = 10;
    let bodies: Vec<String> = (0..WRITERS)
        .map(|i| {
            format!(
                r#"{{"TableName":"events","Item":{{"pk":{{"S":"p1"}},"sk":{{"S":"hot"}},
                     "who":{{"S":"w{i}"}},"cat":{{"S":"X"}}}}}}"#
            )
        })
        .collect();
    let requests: Vec<(u64, &str, &[u8])> = bodies
        .iter()
        .enumerate()
        .map(|(i, b)| {
            (
                (i % cluster.node_count()) as u64,
                "DynamoDB_20120810.PutItem",
                b.as_bytes(),
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
        "every concurrent write applied, so every one must be acknowledged (seed={seed}); \
         got {} failure(s): {failures:#?}",
        failures.len()
    );

    // And the row holds exactly one of the ten values — last writer wins.
    let (status, got) = cluster.dynamo(
        0,
        "DynamoDB_20120810.GetItem",
        br#"{"TableName":"events","Key":{"pk":{"S":"p1"},"sk":{"S":"hot"}},
            "ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {got}");
    let winners = (0..WRITERS)
        .filter(|i| got.contains(&format!("\"w{i}\"")))
        .count();
    assert_eq!(
        winners, 1,
        "exactly one writer's value survives (seed={seed}): {got}"
    );
}

/// A genuinely failed condition must still be reported — the outcome
/// channel has to keep rejecting, not just start accepting everything.
#[test]
fn a_failed_condition_is_still_reported() {
    let seed = env_seed(0xBA7C_0002);
    let mut cluster = setup(seed);

    // `a0` exists, so attribute_not_exists(sk) must fail.
    let (status, resp) = cluster.dynamo(
        1,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"events","Item":{"pk":{"S":"p1"},"sk":{"S":"a0"},"cat":{"S":"X"}},
            "ConditionExpression":"attribute_not_exists(sk)"}"#,
    );
    assert_eq!(
        status, 400,
        "an unmet condition must be refused (seed={seed}): {resp}"
    );
    assert!(
        resp.contains("ConditionalCheckFailed"),
        "seed={seed}: {resp}"
    );

    // And one that does hold still succeeds.
    let (status, ok) = cluster.dynamo(
        1,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"events","Item":{"pk":{"S":"p1"},"sk":{"S":"brand-new"},"cat":{"S":"X"}},
            "ConditionExpression":"attribute_not_exists(sk)"}"#,
    );
    assert_eq!(
        status, 200,
        "a met condition must apply (seed={seed}): {ok}"
    );
}

/// Contention plus conditions: racing conditional writes on one key must
/// resolve to exactly one winner, with the losers told their condition
/// failed rather than given an ambiguous error.
#[test]
fn racing_conditional_writes_yield_exactly_one_winner() {
    let seed = env_seed(0xBA7C_0003);
    let mut cluster = setup(seed);

    const WRITERS: usize = 6;
    let bodies: Vec<String> = (0..WRITERS)
        .map(|i| {
            format!(
                r#"{{"TableName":"events","Item":{{"pk":{{"S":"p1"}},"sk":{{"S":"once"}},
                     "who":{{"S":"w{i}"}},"cat":{{"S":"X"}}}},
                     "ConditionExpression":"attribute_not_exists(sk)"}}"#
            )
        })
        .collect();
    let requests: Vec<(u64, &str, &[u8])> = bodies
        .iter()
        .enumerate()
        .map(|(i, b)| {
            (
                (i % cluster.node_count()) as u64,
                "DynamoDB_20120810.PutItem",
                b.as_bytes(),
            )
        })
        .collect();
    let results = cluster.dynamo_concurrent(&requests);

    let mut won = 0;
    let mut condition_failed = 0;
    let mut ambiguous = Vec::new();
    for (status, resp) in &results {
        if *status == 200 {
            won += 1;
        } else if resp.contains("ConditionalCheckFailed") {
            condition_failed += 1;
        } else {
            ambiguous.push(format!("{status}: {resp}"));
        }
    }
    assert_eq!(won, 1, "exactly one create wins (seed={seed})");
    assert!(
        ambiguous.is_empty(),
        "the losers must be told their condition failed, not given an \
         ambiguous error (seed={seed}): {ambiguous:#?}"
    );
    assert_eq!(
        condition_failed, 5,
        "and the other five lost on the condition (seed={seed})"
    );
}
