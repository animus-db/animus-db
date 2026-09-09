//! `SimCluster`-driven conversion of `crates/animusd/tests/dynamo_
//! throttling.rs`'s four remaining sim-reachable scenarios (ADR 0061 rung
//! K, C-11 PR 2) — `BatchWriteItem`/`BatchGetItem` shedding into
//! `UnprocessedItems`/`UnprocessedKeys`, `TransactWriteItems`' `Throttling
//! Error` cancellation reason, and a forwarded write throttled on the
//! tablet's actual leader. All four already route through the generic
//! `dynamo::dispatch_item_op` core (`Operation::BatchWriteItem`/
//! `BatchGetItem`/`TransactWriteItems` arms, `dynamo.rs` lines ~1480/1247/
//! 1607 — `TransactWriteItems` via [`crate::dynamo::run_transact`], already
//! `<E: Env, R: RelayClient>`-generic since C-06 PR 2) — **no `dynamo.rs`/
//! `write_path.rs`/`read_path.rs`/`txn_coordinator.rs` change was needed
//! at all**, this PR is pure test authorship, the identical "widen once,
//! then just write tests against it" shape every rung since D3 has used.
//!
//! **Corrects a stale claim `sim_cluster_throttle.rs`'s own module doc has
//! carried since D2 PR 1 landed** (`ThrottledWrites`/`ThrottledReads`
//! "never increment under `SimCluster`"): every node built by `SimCluster::
//! new` has carried a real `DataRole` — and therefore a real, per-node
//! `raftkv_metrics` sink — since ADR 0061 rung D2 PR 1 (`sim_cluster.rs`'s
//! own `data: Some(DataRole { raftkv_metrics: node_metrics[i].clone(), ..
//! })`, not `data: None`); the counters were simply never *exercised* by
//! any `SimCluster` fixture until this PR issues a real refusal against
//! one. [`a_forwarded_write_is_throttled_on_the_leader`] does not itself
//! assert the counter (mirroring the real-socket original, which also
//! only asserts the error shape here — `admin_metrics_reports_nonzero_
//! throttled_counters` is `dynamo_throttling.rs`'s own separate,
//! not-yet-converted scenario for that), but its very existence — a real
//! refusal, reached through `kind_write_item_at_leader`'s `Metric::
//! ThrottledWrites` increment — is itself proof the sink is live under this
//! fixture. See `crates/animusd/CLAUDE.md`'s matching rung K PR 2 appendix
//! and `docs/engineering-lessons.md` for the general lesson (a module doc's
//! "never happens under this fixture" claim needs its own expiry check
//! whenever the fixture it describes gains a capability, not just when the
//! scenario using it is written).
//!
//! `crates/animusd/tests/dynamo_throttling.rs` stays byte-identical and
//! untrimmed in this PR — its own module doc already documents these four
//! as candidates with "no sim analog" (stale as of this PR; left
//! uncorrected here since trimming and re-documenting that file is PR 3's
//! own job, per this rung's own D3-discipline instruction: prove the
//! untrimmed baseline green before removing anything).
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

/// `CreateTable` for a plain single-key (`id`, string) table — mirrors
/// `dynamo_throttling.rs::create_table`'s own key shape verbatim (this
/// crate's per-file-fixture convention favors a literal re-declaration
/// over reaching into a sibling module's `pub(crate)` helper).
fn create_table(cluster: &mut SimCluster, node: u64, table: &str) -> (u16, String) {
    let body = format!(
        r#"{{"TableName":"{table}",
            "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}]}}"#
    );
    cluster.dynamo(node, "DynamoDB_20120810.CreateTable", body.as_bytes())
}

/// A **streamed** table — `table_change_records_carry_images` is then
/// true, so `BatchWriteItem` takes the per-item evaluate-at-leader funnel
/// instead of the ADR 0049 marker fast arm's single-Raft-entry-per-tablet
/// commit — mirrors `dynamo_throttling.rs::create_streamed_table`'s own
/// doc: a marker table's batch commits (or sheds) a whole tablet-group
/// together, so only a streamed table's per-item granularity lets
/// [`batch_write_item_sheds_throttled_rows_into_unprocessed_items`]
/// observe a genuinely partial shed.
fn create_streamed_table(cluster: &mut SimCluster, node: u64, table: &str) -> (u16, String) {
    let body = format!(
        r#"{{"TableName":"{table}",
            "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}],
            "StreamSpecification":{{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}}}"#
    );
    cluster.dynamo(node, "DynamoDB_20120810.CreateTable", body.as_bytes())
}

/// A large (~256 KiB), JSON-safe attribute value — mirrors
/// `dynamo_throttling.rs::big_value`: big enough that a single `PutItem`/
/// `GetItem` costs many capacity units, clearing `SimCluster::dynamo`'s own
/// per-call `OP_BUDGET` (12s of virtual-clock refill) by a wide margin, the
/// same idiom `sim_cluster_throttle.rs`/`sim_cluster_dynamo_update_table.rs`
/// already use.
fn big_value() -> String {
    "x".repeat(256 * 1024)
}

fn put_body(table: &str, id: &str, value: &str) -> String {
    format!(r#"{{"TableName":"{table}","Item":{{"id":{{"S":"{id}"}},"v":{{"S":"{value}"}}}}}}"#)
}

/// Parse an error body's `__type` field — mirrors `dynamo_throttling.rs::
/// error_type`/every sibling `sim_cluster_dynamo_*.rs` module's own copy.
fn error_type(body: &str) -> String {
    let json: serde_json::Value =
        serde_json::from_str(body).unwrap_or_else(|e| panic!("body is not JSON ({e}): {body}"));
    json["__type"]
        .as_str()
        .unwrap_or_else(|| panic!("no __type in: {body}"))
        .to_string()
}

// ---------------------------------------------------------------------------
// (1) BatchWriteItem sheds the throttled subset into UnprocessedItems.
// ---------------------------------------------------------------------------

fn run_batch_write_item_sheds_throttled_rows_into_unprocessed_items(seed: u64) {
    let mut cluster = SimCluster::new(seed, 1, 1);
    let (status, body) = create_streamed_table(&mut cluster, 0, "thr_bw");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(thr_bw) failed: {body}"
    );
    cluster.set_throttle_defaults_all(None, Some(1));

    // Deliberately smaller than `big_value()`: 8 of these must fit in one
    // HTTP-shaped request body while their summed cost (~400 WCU) still
    // exceeds the 300-unit burst (ADR 0065 Decision 4) — mirrors
    // `dynamo_throttling.rs`'s own identical sizing.
    let value = "x".repeat(50 * 1024);
    let items: Vec<String> = (0..8)
        .map(|i| {
            format!(r#"{{"PutRequest":{{"Item":{{"id":{{"S":"bw{i}"}},"v":{{"S":"{value}"}}}}}}}}"#)
        })
        .collect();
    let body = format!(r#"{{"RequestItems":{{"thr_bw":[{}]}}}}"#, items.join(","));
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.BatchWriteItem", body.as_bytes());
    assert_eq!(
        status, 200,
        "seed={seed}: BatchWriteItem itself must not fail: {resp}"
    );
    let json: serde_json::Value = serde_json::from_str(&resp).expect("valid JSON");
    let unprocessed = json["UnprocessedItems"]["thr_bw"]
        .as_array()
        .unwrap_or_else(|| panic!("seed={seed}: no UnprocessedItems.thr_bw array in: {resp}"));
    assert!(
        !unprocessed.is_empty(),
        "seed={seed}: expected at least one throttled item under UnprocessedItems: {resp}"
    );
    assert!(
        unprocessed.len() < items.len(),
        "seed={seed}: expected SOME items to still have committed: {resp}"
    );
    for entry in unprocessed {
        assert!(
            entry.get("PutRequest").is_some(),
            "seed={seed}: unprocessed entry lost its PutRequest shape: {entry}"
        );
    }
}

#[test]
fn batch_write_item_sheds_throttled_rows_into_unprocessed_items() {
    run_batch_write_item_sheds_throttled_rows_into_unprocessed_items(env_seed(0xC11B_0001));
}

#[test]
fn batch_write_item_sheds_throttled_rows_into_unprocessed_items_over_seeds() {
    for i in 0..5 {
        run_batch_write_item_sheds_throttled_rows_into_unprocessed_items(0xC11B_1000 + i);
    }
}

// ---------------------------------------------------------------------------
// (2) BatchGetItem sheds the throttled subset into UnprocessedKeys.
// ---------------------------------------------------------------------------

fn run_batch_get_item_sheds_throttled_keys_into_unprocessed_keys(seed: u64) {
    let mut cluster = SimCluster::new(seed, 1, 1);
    let (status, body) = create_table(&mut cluster, 0, "thr_bg");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(thr_bg) failed: {body}"
    );

    let value = big_value();
    for i in 0..8 {
        let (status, body) = cluster.dynamo(
            0,
            "DynamoDB_20120810.PutItem",
            put_body("thr_bg", &format!("bg{i}"), &value).as_bytes(),
        );
        assert_eq!(status, 200, "seed={seed}: seed put {i} failed: {body}");
    }
    cluster.set_throttle_defaults_all(Some(1), None);

    let keys: Vec<String> = (0..8)
        .map(|i| format!(r#"{{"id":{{"S":"bg{i}"}}}}"#))
        .collect();
    let body = format!(
        r#"{{"RequestItems":{{"thr_bg":{{"Keys":[{}],"ConsistentRead":true}}}}}}"#,
        keys.join(",")
    );
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.BatchGetItem", body.as_bytes());
    assert_eq!(
        status, 200,
        "seed={seed}: BatchGetItem itself must not fail: {resp}"
    );
    let json: serde_json::Value = serde_json::from_str(&resp).expect("valid JSON");
    let unprocessed = json["UnprocessedKeys"]["thr_bg"]["Keys"]
        .as_array()
        .unwrap_or_else(|| panic!("seed={seed}: no UnprocessedKeys.thr_bg.Keys array in: {resp}"));
    assert!(
        !unprocessed.is_empty(),
        "seed={seed}: expected at least one throttled key under UnprocessedKeys: {resp}"
    );
    assert!(
        unprocessed.len() < keys.len(),
        "seed={seed}: expected SOME keys to still have been read: {resp}"
    );
}

#[test]
fn batch_get_item_sheds_throttled_keys_into_unprocessed_keys() {
    run_batch_get_item_sheds_throttled_keys_into_unprocessed_keys(env_seed(0xC11B_0002));
}

#[test]
fn batch_get_item_sheds_throttled_keys_into_unprocessed_keys_over_seeds() {
    for i in 0..5 {
        run_batch_get_item_sheds_throttled_keys_into_unprocessed_keys(0xC11B_2000 + i);
    }
}

// ---------------------------------------------------------------------------
// (3) TransactWriteItems cancels with a ThrottlingError cancellation
// reason.
// ---------------------------------------------------------------------------

fn run_transact_write_items_cancels_with_throttling_error(seed: u64) {
    let mut cluster = SimCluster::new(seed, 1, 1);
    let (status, body) = create_table(&mut cluster, 0, "thr_txn");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(thr_txn) failed: {body}"
    );
    cluster.set_throttle_defaults_all(None, Some(1));

    let value = big_value();
    // Drain the 300-unit burst with ordinary puts first (each ~256 WCU).
    for i in 0..6 {
        let _ = cluster.dynamo(
            0,
            "DynamoDB_20120810.PutItem",
            put_body("thr_txn", &format!("drain{i}"), &value).as_bytes(),
        );
    }
    // A transactional write costs 2x — with the bucket already drained,
    // this must cancel rather than partially/fully commit.
    let body = format!(
        r#"{{"TransactItems":[{{"Put":{{"TableName":"thr_txn","Item":{{"id":{{"S":"txn1"}},"v":{{"S":"{value}"}}}}}}}}]}}"#
    );
    let (status, body) = cluster.dynamo(0, "DynamoDB_20120810.TransactWriteItems", body.as_bytes());
    assert_eq!(
        status, 400,
        "seed={seed}: expected the transaction to be cancelled: {body}"
    );
    assert_eq!(
        error_type(&body),
        "com.amazonaws.dynamodb.v20120810#TransactionCanceledException",
        "seed={seed}: unexpected error body: {body}"
    );
    let json: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
    let reasons = json["CancellationReasons"]
        .as_array()
        .unwrap_or_else(|| panic!("seed={seed}: no CancellationReasons in: {body}"));
    assert_eq!(reasons.len(), 1, "seed={seed}: {body}");
    assert_eq!(
        reasons[0]["Code"], "ThrottlingError",
        "seed={seed}: expected the single action's own reason to be ThrottlingError: {body}"
    );

    // The item must genuinely not have committed.
    let get_body = r#"{"TableName":"thr_txn","ConsistentRead":true,"Key":{"id":{"S":"txn1"}}}"#;
    let (status, body) = cluster.dynamo(0, "DynamoDB_20120810.GetItem", get_body.as_bytes());
    assert_eq!(status, 200, "seed={seed}: {body}");
    let json: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
    assert!(
        json.get("Item").is_none(),
        "seed={seed}: the cancelled transaction's write must not have landed: {body}"
    );
}

#[test]
fn transact_write_items_cancels_with_throttling_error() {
    run_transact_write_items_cancels_with_throttling_error(env_seed(0xC11B_0003));
}

#[test]
fn transact_write_items_cancels_with_throttling_error_over_seeds() {
    for i in 0..5 {
        run_transact_write_items_cancels_with_throttling_error(0xC11B_3000 + i);
    }
}

// ---------------------------------------------------------------------------
// (4) A write issued on a non-leader node is throttled by the tablet's
// actual leader's own bucket.
// ---------------------------------------------------------------------------

/// Issues every write from ONE fixed non-leader node (rather than
/// `dynamo_throttling.rs`'s own round-robin across all three) — a stronger,
/// more direct proof of the same claim its own doc names: the check runs
/// after forwarding resolves the real leader, not merely at the receiving
/// edge, since the leader's own bucket is what refuses regardless of which
/// node the client dialed.
fn run_a_forwarded_write_is_throttled_on_the_leader(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, 0, "thr_fwd");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(thr_fwd) failed: {body}"
    );
    cluster.set_throttle_defaults_all(None, Some(1));

    let tablet = cluster
        .metadata(0)
        .tablets_for_table("thr_fwd")
        .next()
        .map(|(id, _)| *id)
        .unwrap_or_else(|| panic!("seed={seed}: thr_fwd has no tablet"));
    let leader = cluster
        .leader_index_of(tablet)
        .unwrap_or_else(|| panic!("seed={seed}: thr_fwd's tablet has no elected leader"));
    // Replication is 3 == node count, so every node hosts this tablet —
    // any node other than the leader is a genuine forwarding entry point.
    let non_leader = (0..3u64)
        .find(|&n| n != leader)
        .unwrap_or_else(|| panic!("seed={seed}: no non-leader node among 3"));

    let value = big_value();
    let mut refused = None;
    for i in 0..20 {
        let (status, body) = cluster.dynamo(
            non_leader,
            "DynamoDB_20120810.PutItem",
            put_body("thr_fwd", &format!("k{i}"), &value).as_bytes(),
        );
        if status == 400 {
            refused = Some(body);
            break;
        }
        assert_eq!(
            status, 200,
            "seed={seed}: unexpected PutItem failure on non-leader node {non_leader}: {body}"
        );
    }
    let body = refused.unwrap_or_else(|| {
        panic!(
            "seed={seed}: expected the write burst to eventually be throttled on the leader \
             even though every request was issued on non-leader node {non_leader}"
        )
    });
    assert_eq!(
        error_type(&body),
        "com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException",
        "seed={seed}: unexpected error body: {body}"
    );
}

#[test]
fn a_forwarded_write_is_throttled_on_the_leader() {
    run_a_forwarded_write_is_throttled_on_the_leader(env_seed(0xC11B_0004));
}

#[test]
fn a_forwarded_write_is_throttled_on_the_leader_over_seeds() {
    for i in 0..5 {
        run_a_forwarded_write_is_throttled_on_the_leader(0xC11B_4000 + i);
    }
}
