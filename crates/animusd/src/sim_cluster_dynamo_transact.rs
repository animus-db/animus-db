//! `SimCluster`-driven deterministic coverage for `TransactWriteItems`/
//! `TransactGetItems` (ADR 0061 rung F, C-06 PR 3) — the first proof that a
//! real DynamoDB-wire transaction executes through the generic dispatch
//! core, reachable under `SimEnv` for the first time.
//!
//! **The dispatch change (`dynamo.rs`)**: `dispatch_item_op` (ADR 0061 rung
//! D2 PR 1's item/query core, already `<E: Env, R: RelayClient>`-generic)
//! gained two more match arms — `Operation::TransactWriteItems`/
//! `Operation::TransactGetItems`, calling [`crate::dynamo::run_transact`]/
//! [`crate::dynamo::run_transact_get`] the exact call shape `run_operation`'s
//! own arms already use (`principal` threaded through identically for the
//! per-table `authz` check). Both functions were already widened to
//! `<E, R>` by C-06 PR 2 (a pure signature-and-timer conversion, zero new
//! mechanism) — this PR is purely the routing half. `run_operation`,
//! `execute_statement`, `execute_transaction`, and
//! `run_batch_execute_statement` are byte-identical, unchanged — see the
//! root `CLAUDE.md`'s "a narrowed generic split of a dispatcher must not
//! become the production dispatcher's ONLY path" lesson, which this PR
//! deliberately does not repeat: `run_operation`'s own `TransactWriteItems`/
//! `TransactGetItems` arms still call `run_transact`/`run_transact_get`
//! directly, monomorphized at `E = ProdEnv, R = AnimusdRelayClient`, exactly
//! as before this rung.
//!
//! **Two new small `SimCluster` primitives (`sim_cluster.rs`)**, both used
//! only by scenario (g) below: [`SimCluster::txn_prepare_only`] (stage one
//! write of a raw 2PC transaction via `ClientCtx::txn_prepare` directly,
//! deliberately never deciding/resolving — this fixture's own way of
//! expressing "the coordinator crashed right after prepare," mirroring
//! `cp_txn.rs`'s own `prepare_via_any_node` idiom) and [`SimCluster::
//! raw_get`] (a routed read of an arbitrary physical key, the raw-plain-KV
//! sibling of `SimClusterHandle::get`'s item-shaped read).
//!
//! # Scenarios (seed-parameterized, `_over_seeds` at 5 seeds each)
//!
//! (a) [`commit_across_two_tables_with_condition_check`] — two `Put`s across
//!     two different tables plus a passing `ConditionCheck`, committed
//!     atomically and readable afterward with `ConsistentRead: true` from a
//!     different node.
//! (b) [`condition_failure_cancels_with_per_action_reasons`] — a failing
//!     `ConditionCheck` cancels the whole transaction with a
//!     `TransactionCanceledException` carrying per-action `CancellationReasons`
//!     and no partial write, even though both `Put`s precede the failing
//!     check in list order.
//! (c) [`client_request_token_idempotency`] — a same-token retry after
//!     commit returns the cached outcome and does not re-run the
//!     transaction (an `ADD` counter would double if it did); a different
//!     payload under the same token is rejected
//!     `IdempotentParameterMismatchException`.
//! (d) [`transact_get_items_never_observes_a_torn_pair`] — a background
//!     writer repeatedly updates two keys so they always sum to zero; a
//!     concurrent `TransactGetItems` reader must never observe a torn pair
//!     (ADR 0018 §2's per-key non-blocking snapshot) — driven as genuinely
//!     interleaved tasks racing before one shared `Simulator::run_for`,
//!     mirroring `SimCluster::dynamo_concurrent`'s own shape but with a
//!     writer that issues several sequential calls from one task.
//! (e) [`transaction_from_a_non_participant_node_is_forwarded_and_commits`]
//!     — a transaction issued on a node hosting no replica of either
//!     table's tablet is forwarded and commits (a 7-node cluster with two
//!     RF-3 tables always leaves at least one idle node; found via
//!     [`SimCluster::hosted_tablets`], never by parsing a `NodeId` back
//!     into an index — see `sim_cluster.rs`'s own doc on why that fails).
//! (f) [`idempotency_table_bootstrap_race_between_two_first_callers`] — the
//!     risk ADR 0061 rung F's own amendment named up front: two
//!     token-bearing transactions from two different nodes, both racing
//!     `ensure_txn_idempotency_table`'s `CreateTableSchema` propose in the
//!     same tick (`SimCluster::dynamo_concurrent`), have never run under a
//!     fault-injecting simulator before this PR. **No product bug found**
//!     — exactly one proposal wins (first-committer-wins,
//!     `Metadata`'s own schema-catalog exclusivity), and both transactions
//!     commit regardless of which one won.
//! (g) [`coordinator_never_finished_past_prepare_recovers_atomically`] — the
//!     coordinator stages (prepares) both participants of a cross-table
//!     transaction and never decides — the shape a coordinator process
//!     crashing right after `cp_txn`'s prepare phase takes (ADR 0018 §2
//!     recovery) — with virtual time advanced between the last prepare and
//!     a real `SimCluster::crash` of the coordinator node, then a
//!     `SimCluster::restart`. Intended to prove a strong read of the
//!     **participant** key from a different, live node (a genuinely
//!     foreign intent) triggers `confirm_or_push`/`txn_recover` on demand
//!     once the record has sat `Pending` past `RECOVERY_GRACE` (5s), with
//!     both keys converging together, never partially.
//!
//!     **`#[ignore]`d — a SECOND, deeper finding, issue #731's own fix
//!     applied and confirmed but not sufficient.** `animus_node::sim_relay::
//!     SimRelayClient::serve_loop`'s single-task inline dispatch (issue
//!     #731) is fixed — see `crates/animus-node/src/sim_relay.rs`'s own
//!     "One task per inbound request" doc section and ADR 0061's "#731
//!     closed" addendum — and the fix is directly confirmed here: every
//!     poll no longer returns `SimRelayClient::relay`'s timeout text at
//!     all; it returns `Err("transaction covering this key is still
//!     pending; retry")` instead, unchanging across the full 40s budget at
//!     every seed tried. That text is `cp_get_local_resolving_inner`'s own
//!     `TxnDecisionStatus::Pending` arm — `confirm_or_push`/`txn_recover`
//!     run to completion every time (no more deadlock), but `txn_recover`
//!     itself never gets past its own grace check. Root cause, traced in
//!     `txn_coordinator.rs::txn_recover`: whenever `self.cp_route(record_table,
//!     record_key)` resolves to anything **other than** `CpRoute::Local` —
//!     the ordinary case for this scenario's own on-demand push, since it
//!     runs on the *reading* node's own leader (the participant's tablet),
//!     not necessarily the *anchor*'s tablet leader — `now_ms` is computed
//!     as `self.env.now().duration_since(self.env.now())`, the elapsed gap
//!     between two back-to-back clock reads (near-zero), not an absolute
//!     timestamp. Checked against `now_ms < view.created_ts.wall_ms +
//!     RECOVERY_GRACE`, a near-zero `now_ms` makes this comparison true
//!     forever, so the grace check never passes and `txn_recover` declines
//!     (`Pending`) on every single call, permanently. This is a genuine,
//!     pre-existing bug — unrelated to and unmodified by the relay-dispatch
//!     fix — introduced (and knowingly, deliberately left unfixed as
//!     out-of-scope) by ADR 0061 rung C5 step 3b's `tokio::time::Instant::
//!     now().elapsed()` → `Env` conversion; see that rung's own `CLAUDE.md`
//!     entry ("Two `tokio::time::Instant::now().elapsed()` reads... had no
//!     literal translation... reproducing the identical near-zero result
//!     rather than 'fixing' what reads like a pre-existing latent bug — an
//!     incidental bug gets its own PR"). It was unreachable before this PR
//!     because the relay deadlock (issue #731) intercepted every recovery
//!     attempt before `txn_recover` was ever called at all. **Issue to be
//!     filed** against `ClientCtx::txn_recover`'s non-local grace-check
//!     branch (`crates/animusd/src/txn_coordinator.rs`).
//!
//! Replays (a) with `ANIMUS_SEED` per the repo convention:
//! `ANIMUS_SEED=<seed> cargo test -p animusd --lib
//! commit_across_two_tables_with_condition_check`. Scenario (g) replays the
//! same way but needs `-- --ignored` appended (see its own doc). Handle
//! recorded from this rung's own gate run: seed `0xC06F_0001` (scenario a,
//! green) and `0xC06F_0007` (scenario g, the second finding above —
//! reproduces identically at every `_over_seeds` seed tried too).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::EnvExt;
use animus_tablet::TOKEN_BYTES;

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// One DynamoDB wire `CreateTable` for a plain single-key (`pk`, string)
/// table named `table`, issued from `node` — mirrors `sim_cluster_dynamo_
/// table_ops.rs`'s own identically-named helper.
fn create_table(cluster: &mut SimCluster, node: u64, table: &str) -> (u16, String) {
    let body = format!(
        r#"{{"TableName":"{table}",
            "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]}}"#
    );
    cluster.dynamo(node, "DynamoDB_20120810.CreateTable", body.as_bytes())
}

fn put_item(cluster: &mut SimCluster, node: u64, table: &str, pk: &str, v: &str) -> (u16, String) {
    let body =
        format!(r#"{{"TableName":"{table}","Item":{{"pk":{{"S":"{pk}"}},"v":{{"S":"{v}"}}}}}}"#);
    cluster.dynamo(node, "DynamoDB_20120810.PutItem", body.as_bytes())
}

fn get_item(
    cluster: &mut SimCluster,
    node: u64,
    table: &str,
    pk: &str,
    consistent: bool,
) -> (u16, String) {
    let body = format!(
        r#"{{"ConsistentRead":{consistent},"TableName":"{table}","Key":{{"pk":{{"S":"{pk}"}}}}}}"#
    );
    cluster.dynamo(node, "DynamoDB_20120810.GetItem", body.as_bytes())
}

/// Parse a `TransactionCanceledException` body's `CancellationReasons`
/// array — mirrors `dynamo_txn_cancellation.rs`'s own helper.
fn cancellation_reasons(body: &str) -> Vec<serde_json::Value> {
    let json: serde_json::Value = serde_json::from_str(body).expect("valid JSON error body");
    json["CancellationReasons"]
        .as_array()
        .cloned()
        .unwrap_or_else(|| panic!("no CancellationReasons array in: {body}"))
}

/// A raw plain-KV key of at least `TOKEN_BYTES` length — mirrors
/// `cp_txn.rs`'s own `txn_key` helper (a tablet's routing token prefix
/// needs a key at least this long).
fn txn_key(prefix: &str, suffix: &str) -> Vec<u8> {
    let mut s = format!("{prefix}{suffix}");
    while s.len() < TOKEN_BYTES {
        s.push('_');
    }
    s.into_bytes()
}

// ---------------------------------------------------------------------------
// (a) A commit across two tables including a ConditionCheck.
// ---------------------------------------------------------------------------

fn run_commit_across_two_tables_with_condition_check(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, 0, "txg_a");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(txg_a) failed: {body}"
    );
    let (status, body) = create_table(&mut cluster, 0, "txg_b");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(txg_b) failed: {body}"
    );

    let body = r#"{"TransactItems":[
        {"Put":{"TableName":"txg_a","Item":{"pk":{"S":"a1"},"v":{"S":"lo"}}}},
        {"Put":{"TableName":"txg_b","Item":{"pk":{"S":"b1"},"v":{"S":"hi"}}}},
        {"ConditionCheck":{"TableName":"txg_a","Key":{"pk":{"S":"absent-guard"}},
                           "ConditionExpression":"attribute_not_exists(pk)"}}]}"#;
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.TransactWriteItems", body.as_bytes());
    assert_eq!(
        status, 200,
        "seed={seed}: TransactWriteItems failed: {resp}"
    );

    // Read both back from a DIFFERENT node than the one the transaction was
    // issued through, `ConsistentRead: true` (ADR 0055 — an unqualified
    // read gives no read-your-writes guarantee).
    let reader = 1u64;
    let (status, resp) = get_item(&mut cluster, reader, "txg_a", "a1", true);
    assert_eq!(status, 200, "seed={seed}: GetItem(a1) failed: {resp}");
    assert!(
        resp.contains(r#""v":{"S":"lo"}"#),
        "seed={seed}: a1 missing/wrong: {resp}"
    );
    let (status, resp) = get_item(&mut cluster, reader, "txg_b", "b1", true);
    assert_eq!(status, 200, "seed={seed}: GetItem(b1) failed: {resp}");
    assert!(
        resp.contains(r#""v":{"S":"hi"}"#),
        "seed={seed}: b1 missing/wrong: {resp}"
    );
}

/// `ANIMUS_SEED=<seed> cargo test -p animusd --lib
/// commit_across_two_tables_with_condition_check` replays this scenario at
/// a specific seed (repo convention).
#[test]
fn commit_across_two_tables_with_condition_check() {
    run_commit_across_two_tables_with_condition_check(env_seed(0xC06F_0001));
}

#[test]
fn commit_across_two_tables_with_condition_check_over_seeds() {
    for i in 0..5 {
        run_commit_across_two_tables_with_condition_check(0xC06F_1000 + i);
    }
}

// ---------------------------------------------------------------------------
// (b) A failing ConditionCheck cancels the WHOLE transaction, per-action
// CancellationReasons.
// ---------------------------------------------------------------------------

fn run_condition_failure_cancels_with_per_action_reasons(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, 0, "cxl_a");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(cxl_a) failed: {body}"
    );
    let (status, body) = create_table(&mut cluster, 0, "cxl_b");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(cxl_b) failed: {body}"
    );

    // A guard item that DOES exist, so `attribute_not_exists(pk)` fails.
    let (status, body) = put_item(&mut cluster, 0, "cxl_a", "guard", "present");
    assert_eq!(status, 200, "seed={seed}: seed put failed: {body}");

    // [0] Put on cxl_a (would land in isolation)
    // [1] Put on cxl_b (would land in isolation)
    // [2] ConditionCheck on "guard" — fails (guard exists)
    let body = r#"{"TransactItems":[
        {"Put":{"TableName":"cxl_a","Item":{"pk":{"S":"a1"},"v":{"S":"should-not-land"}}}},
        {"Put":{"TableName":"cxl_b","Item":{"pk":{"S":"b1"},"v":{"S":"should-not-land-either"}}}},
        {"ConditionCheck":{"TableName":"cxl_a","Key":{"pk":{"S":"guard"}},
                           "ConditionExpression":"attribute_not_exists(pk)"}}]}"#;
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.TransactWriteItems", body.as_bytes());
    assert_eq!(
        status, 400,
        "seed={seed}: expected the guard check to cancel: {resp}"
    );
    assert!(
        resp.contains("TransactionCanceledException"),
        "seed={seed}: expected TransactionCanceledException, got: {resp}"
    );

    let reasons = cancellation_reasons(&resp);
    assert_eq!(
        reasons.len(),
        3,
        "seed={seed}: one entry per action: {resp}"
    );
    assert_eq!(reasons[0]["Code"], "None");
    assert_eq!(reasons[1]["Code"], "None");
    assert_eq!(reasons[2]["Code"], "ConditionalCheckFailed");

    // Whole-or-nothing: NEITHER Put landed, even though both precede the
    // failing check in list order.
    for (table, pk) in [("cxl_a", "a1"), ("cxl_b", "b1")] {
        let (status, resp) = get_item(&mut cluster, 0, table, pk, true);
        assert_eq!(
            status, 200,
            "seed={seed}: GetItem({table}/{pk}) failed: {resp}"
        );
        assert_eq!(
            resp, "{}",
            "seed={seed}: {table}/{pk} must NOT have been written by a cancelled \
             transaction: {resp}"
        );
    }
}

#[test]
fn condition_failure_cancels_with_per_action_reasons() {
    run_condition_failure_cancels_with_per_action_reasons(env_seed(0xC06F_0002));
}

#[test]
fn condition_failure_cancels_with_per_action_reasons_over_seeds() {
    for i in 0..5 {
        run_condition_failure_cancels_with_per_action_reasons(0xC06F_2000 + i);
    }
}

// ---------------------------------------------------------------------------
// (c) ClientRequestToken idempotency: cached retry, mismatched retry.
// ---------------------------------------------------------------------------

fn read_counter(cluster: &mut SimCluster, table: &str, pk: &str) -> i64 {
    let (status, body) = get_item(cluster, 0, table, pk, true);
    assert_eq!(status, 200, "GetItem({table}/{pk}) failed: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
    v["Item"]["hits"]["N"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

fn run_client_request_token_idempotency(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, 0, "ctr1");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    let body = r#"{"ClientRequestToken":"retry-token-1",
        "TransactItems":[{"Update":{"TableName":"ctr1","Key":{"pk":{"S":"c"}},
            "UpdateExpression":"ADD hits :one",
            "ExpressionAttributeValues":{":one":{"N":"1"}}}}]}"#;
    let (status1, body1) =
        cluster.dynamo(0, "DynamoDB_20120810.TransactWriteItems", body.as_bytes());
    assert_eq!(status1, 200, "seed={seed}: first attempt failed: {body1}");
    assert_eq!(read_counter(&mut cluster, "ctr1", "c"), 1);

    // Same token, byte-identical actions: cached — 200 again, no second
    // `ADD` (else the counter would read 2, not 1).
    let (status2, body2) =
        cluster.dynamo(0, "DynamoDB_20120810.TransactWriteItems", body.as_bytes());
    assert_eq!(status2, 200, "seed={seed}: retried attempt failed: {body2}");
    assert_eq!(
        read_counter(&mut cluster, "ctr1", "c"),
        1,
        "seed={seed}: a same-token retry must not re-run the transaction"
    );

    // Same token, a genuinely different payload: rejected.
    let mismatched = r#"{"ClientRequestToken":"retry-token-1",
        "TransactItems":[{"Put":{"TableName":"ctr1","Item":{"pk":{"S":"other"}}}}]}"#;
    let (status3, body3) = cluster.dynamo(
        0,
        "DynamoDB_20120810.TransactWriteItems",
        mismatched.as_bytes(),
    );
    assert_eq!(
        status3, 400,
        "seed={seed}: mismatched retry should be rejected: {body3}"
    );
    assert!(
        body3.contains("IdempotentParameterMismatchException"),
        "seed={seed}: expected IdempotentParameterMismatchException, got: {body3}"
    );
}

#[test]
fn client_request_token_idempotency() {
    run_client_request_token_idempotency(env_seed(0xC06F_0003));
}

#[test]
fn client_request_token_idempotency_over_seeds() {
    for i in 0..5 {
        run_client_request_token_idempotency(0xC06F_3000 + i);
    }
}

// ---------------------------------------------------------------------------
// (d) TransactGetItems never observes a torn pair under a concurrent
// writer.
// ---------------------------------------------------------------------------

/// A background writer repeatedly `TransactWriteItems`-updates two keys so
/// they always sum to zero (`a = n`, `b = -n`); two concurrent readers each
/// repeatedly `TransactGetItems`-read the pair and every observed pair must
/// be one of the writer's own legal atomic states — a torn read (the
/// writer's old `a` paired with its new `b`, or vice versa) would sum to
/// something else. Driven as genuinely racing tasks (writer + both readers
/// spawned before one shared [`SimCluster::run_for`]), mirroring
/// `SimCluster::dynamo_concurrent`'s own shape but with a writer that
/// issues several sequential calls from inside one task rather than one
/// call per spawned task.
fn run_transact_get_items_never_observes_a_torn_pair(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, 0, "txsnap");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    for pk in ["a", "b"] {
        let put_body =
            format!(r#"{{"TableName":"txsnap","Item":{{"pk":{{"S":"{pk}"}},"n":{{"N":"0"}}}}}}"#);
        let (status, body) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", put_body.as_bytes());
        assert_eq!(status, 200, "seed={seed}: seed put({pk}) failed: {body}");
    }

    let handle = cluster.handle();

    let writer_env = handle.env(0);
    let writer_handle = handle.clone();
    let writer_seed = seed;
    writer_env.spawn_task(async move {
        for step in 1..=15i64 {
            let write_body = format!(
                r#"{{"TransactItems":[
                    {{"Put":{{"TableName":"txsnap","Item":{{"pk":{{"S":"a"}},"n":{{"N":"{step}"}}}}}}}},
                    {{"Put":{{"TableName":"txsnap","Item":{{"pk":{{"S":"b"}},"n":{{"N":"{}"}}}}}}}}]}}"#,
                -step
            );
            let (status, resp) = writer_handle
                .dynamo(0, "DynamoDB_20120810.TransactWriteItems", write_body.as_bytes())
                .await;
            assert_eq!(
                status, 200,
                "seed={writer_seed}: writer step {step} failed: {resp}"
            );
        }
    });

    let rounds = Arc::new(Mutex::new(0u32));
    for reader_node in [1u64, 2u64] {
        let reader_env = handle.env(reader_node);
        let reader_handle = handle.clone();
        let rounds = rounds.clone();
        let reader_seed = seed;
        reader_env.spawn_task(async move {
            for _ in 0..30 {
                let get_body = r#"{"TransactItems":[
                    {"Get":{"TableName":"txsnap","Key":{"pk":{"S":"a"}}}},
                    {"Get":{"TableName":"txsnap","Key":{"pk":{"S":"b"}}}}]}"#;
                let (status, resp) = reader_handle
                    .dynamo(
                        reader_node,
                        "DynamoDB_20120810.TransactGetItems",
                        get_body.as_bytes(),
                    )
                    .await;
                if status != 200 {
                    assert!(
                        resp.contains("TransactionCanceledException"),
                        "seed={reader_seed}: unexpected TransactGetItems failure: {resp}"
                    );
                    continue;
                }
                let v: serde_json::Value = serde_json::from_str(&resp).expect("valid JSON");
                let responses = v["Responses"].as_array().expect("Responses array");
                assert_eq!(
                    responses.len(),
                    2,
                    "seed={reader_seed}: expected 2 responses: {resp}"
                );
                let n_of = |idx: usize| -> Option<i64> {
                    responses[idx]["Item"]["n"]["N"]
                        .as_str()
                        .and_then(|s| s.parse::<i64>().ok())
                };
                if let (Some(a), Some(b)) = (n_of(0), n_of(1)) {
                    assert_eq!(
                        a + b,
                        0,
                        "seed={reader_seed}: torn TransactGetItems snapshot: a={a}, b={b}, \
                         body={resp}"
                    );
                    *rounds.lock().expect("rounds poisoned") += 1;
                }
            }
        });
    }

    cluster.run_for(Duration::from_secs(60));
    assert!(
        *rounds.lock().expect("rounds poisoned") > 0,
        "seed={seed}: neither reader ever observed a single consistent snapshot"
    );
}

#[test]
fn transact_get_items_never_observes_a_torn_pair() {
    run_transact_get_items_never_observes_a_torn_pair(env_seed(0xC06F_0004));
}

#[test]
fn transact_get_items_never_observes_a_torn_pair_over_seeds() {
    for i in 0..5 {
        run_transact_get_items_never_observes_a_torn_pair(0xC06F_4000 + i);
    }
}

// ---------------------------------------------------------------------------
// (e) A transaction issued on a node hosting no replica of either table is
// forwarded and commits.
// ---------------------------------------------------------------------------

/// A 7-node cluster with two RF-3 tables always leaves at least one node
/// hosting neither table's tablet, regardless of how the two replica sets
/// overlap (the union of two size-3 sets is at most 6 of 7). Found via
/// [`SimCluster::hosted_tablets`] (a node's real, reconciler-converged
/// hosting set) — never by parsing a `NodeId` back into a `u64` index, the
/// documented `sim_cluster.rs` gotcha (`nid`'s `"n{n}"` encoding is not
/// something a caller of this fixture should ever need to reverse).
fn run_transaction_from_a_non_participant_node_is_forwarded_and_commits(seed: u64) {
    let mut cluster = SimCluster::new(seed, 7, 3);
    let (status, body) = create_table(&mut cluster, 0, "fwd_a");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(fwd_a) failed: {body}"
    );
    let (status, body) = create_table(&mut cluster, 0, "fwd_b");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(fwd_b) failed: {body}"
    );

    let tablet_a = cluster
        .metadata(0)
        .tablets_for_table("fwd_a")
        .next()
        .map(|(id, _)| *id)
        .unwrap_or_else(|| panic!("seed={seed}: fwd_a has no tablet"));
    let tablet_b = cluster
        .metadata(0)
        .tablets_for_table("fwd_b")
        .next()
        .map(|(id, _)| *id)
        .unwrap_or_else(|| panic!("seed={seed}: fwd_b has no tablet"));

    // Let the reconciler on every node converge before sampling who hosts
    // what.
    cluster.run_for(Duration::from_secs(5));

    let idle = (0..7u64)
        .find(|&n| {
            let hosted = cluster.hosted_tablets(n);
            !hosted.contains(&tablet_a) && !hosted.contains(&tablet_b)
        })
        .unwrap_or_else(|| {
            panic!(
                "seed={seed}: no idle node among 7 for two RF-3 tables (fwd_a's replicas \
                 and fwd_b's replicas together covered every node)"
            )
        });

    let body = r#"{"TransactItems":[
        {"Put":{"TableName":"fwd_a","Item":{"pk":{"S":"x1"},"v":{"S":"one"}}}},
        {"Put":{"TableName":"fwd_b","Item":{"pk":{"S":"x2"},"v":{"S":"two"}}}}]}"#;
    let (status, resp) = cluster.dynamo(
        idle,
        "DynamoDB_20120810.TransactWriteItems",
        body.as_bytes(),
    );
    assert_eq!(
        status, 200,
        "seed={seed}: transaction from idle node {idle} (hosting neither table) failed: {resp}"
    );

    let (status, resp) = get_item(&mut cluster, idle, "fwd_a", "x1", true);
    assert_eq!(status, 200, "seed={seed}: GetItem(x1) failed: {resp}");
    assert!(
        resp.contains(r#""v":{"S":"one"}"#),
        "seed={seed}: x1 missing/wrong: {resp}"
    );
    let (status, resp) = get_item(&mut cluster, idle, "fwd_b", "x2", true);
    assert_eq!(status, 200, "seed={seed}: GetItem(x2) failed: {resp}");
    assert!(
        resp.contains(r#""v":{"S":"two"}"#),
        "seed={seed}: x2 missing/wrong: {resp}"
    );
}

#[test]
fn transaction_from_a_non_participant_node_is_forwarded_and_commits() {
    run_transaction_from_a_non_participant_node_is_forwarded_and_commits(env_seed(0xC06F_0005));
}

#[test]
fn transaction_from_a_non_participant_node_is_forwarded_and_commits_over_seeds() {
    for i in 0..5 {
        run_transaction_from_a_non_participant_node_is_forwarded_and_commits(0xC06F_5000 + i);
    }
}

// ---------------------------------------------------------------------------
// (f) The internal idempotency-table bootstrap race: two first callers on
// different nodes, in the same tick.
// ---------------------------------------------------------------------------

/// **The risk ADR 0061 rung F's own amendment named up front, budgeted
/// for**: `ensure_txn_idempotency_table` is a propose-and-poll-to-commit
/// dance any number of concurrent first callers can race into
/// simultaneously — nothing serializes callers before the schema check.
/// Two token-bearing `TransactWriteItems` calls, from two different nodes,
/// spawned before one shared [`SimCluster::dynamo_concurrent`] batch, so
/// both genuinely race `MetaCommand::CreateTableSchema` for the reserved
/// `__animus_txn_idempotency` table in the same tick — this path has never
/// run under a fault-injecting simulator before this PR.
///
/// **No product bug found**: `Metadata`'s own schema-catalog exclusivity
/// (first-committer-wins) means exactly one proposal wins, and both
/// transactions still commit regardless of which racer won — the loser's
/// own `ensure_txn_idempotency_table` simply observes the table already
/// exists (its own doc: "a second caller's redundant proposals simply
/// commit as no-ops") and proceeds.
fn run_idempotency_table_bootstrap_race_between_two_first_callers(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, 0, "boot1");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(boot1) failed: {body}"
    );
    let (status, body) = create_table(&mut cluster, 0, "boot2");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(boot2) failed: {body}"
    );

    let body1 = br#"{"ClientRequestToken":"race-token-1",
        "TransactItems":[{"Put":{"TableName":"boot1","Item":{"pk":{"S":"x"},"v":{"S":"one"}}}}]}"#;
    let body2 = br#"{"ClientRequestToken":"race-token-2",
        "TransactItems":[{"Put":{"TableName":"boot2","Item":{"pk":{"S":"y"},"v":{"S":"two"}}}}]}"#;

    let results = cluster.dynamo_concurrent(&[
        (
            0u64,
            "DynamoDB_20120810.TransactWriteItems",
            body1.as_slice(),
        ),
        (
            1u64,
            "DynamoDB_20120810.TransactWriteItems",
            body2.as_slice(),
        ),
    ]);
    assert_eq!(
        results[0].0, 200,
        "seed={seed}: first-caller transaction on node 0 failed: {}",
        results[0].1
    );
    assert_eq!(
        results[1].0, 200,
        "seed={seed}: first-caller transaction on node 1 failed: {}",
        results[1].1
    );

    assert!(
        cluster
            .metadata(0)
            .has_table_schema(animus_dynamo::internal_tables::TXN_IDEMPOTENCY_TABLE),
        "seed={seed}: the internal idempotency table must exist after the race"
    );

    let (status, resp) = get_item(&mut cluster, 2, "boot1", "x", true);
    assert_eq!(status, 200, "seed={seed}: GetItem(x) failed: {resp}");
    assert!(
        resp.contains(r#""v":{"S":"one"}"#),
        "seed={seed}: x missing/wrong: {resp}"
    );
    let (status, resp) = get_item(&mut cluster, 2, "boot2", "y", true);
    assert_eq!(status, 200, "seed={seed}: GetItem(y) failed: {resp}");
    assert!(
        resp.contains(r#""v":{"S":"two"}"#),
        "seed={seed}: y missing/wrong: {resp}"
    );
}

#[test]
fn idempotency_table_bootstrap_race_between_two_first_callers() {
    run_idempotency_table_bootstrap_race_between_two_first_callers(env_seed(0xC06F_0006));
}

#[test]
fn idempotency_table_bootstrap_race_between_two_first_callers_over_seeds() {
    for i in 0..5 {
        run_idempotency_table_bootstrap_race_between_two_first_callers(0xC06F_6000 + i);
    }
}

// ---------------------------------------------------------------------------
// (g) The coordinator never finished past the prepare phase — ADR 0018 §2
// recovery, atomic commit on every replica.
// ---------------------------------------------------------------------------

/// **The coordinator crashed after the prepare phase.** Stages (prepares)
/// both participants of a cross-table transaction directly via
/// [`SimCluster::txn_prepare_only`] — never decides, never resolves; the
/// same "drive prepare, then simply stop" idiom `cp_txn.rs`'s own
/// `coordinator_crash_between_prepare_and_decide_recovers_to_commit` uses
/// to express a vanished coordinator over a real cluster. Time is advanced
/// between the last prepare and a real [`SimCluster::crash`] of the
/// coordinator node (the general propose-then-crash idiom — a bare crash
/// immediately after a propose can strand it unreplicated; see
/// `docs/engineering-lessons.md`), then the coordinator is
/// [`SimCluster::restart`]ed.
///
/// A strong read of the **participant** key from a different, live node
/// triggers recovery: the participant's own covering (anchor) record lives
/// on a genuinely different tablet (two different tables), so this read
/// hits `FastRead::Foreign` and `confirm_or_push`/`txn_recover` (`read_
/// path.rs`) — on demand, from the read path itself, once the record has
/// sat `Pending` past `RECOVERY_GRACE` (5s). This fixture never spawns
/// `txn_resolver_loop` (production's background sweep), so this on-demand
/// push is the ONLY mechanism that can ever resolve this transaction here
/// — a genuine, not merely convenient, proof that recovery does not depend
/// on any particular coordinator or background loop.
///
/// **Atomicity**: once the participant key is observed committed, the
/// anchor's own key must ALSO already be committed — never a partial
/// outcome. The restarted coordinator's own view is polled to agree too.
fn run_coordinator_never_finished_past_prepare_recovers_atomically(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, 0, "rcg_a");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(rcg_a) failed: {body}"
    );
    let (status, body) = create_table(&mut cluster, 0, "rcg_b");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(rcg_b) failed: {body}"
    );

    let coordinator = 0u64;
    let anchor_key = txn_key("anchor", "-rcg");
    let participant_key = txn_key("participant", "-rcg");

    // The anchor's own record must name the participant's `(table, span)`
    // up front (`ClientCtx::cp_txn`'s own anchor-stage shape) — otherwise
    // in-doubt recovery's `all_staged` check never learns the participant
    // exists at all, and can never resolve it.
    let mut participant_span_end = participant_key.clone();
    participant_span_end.push(0);
    let participant_spans = vec![(
        "rcg_b".to_owned(),
        animus_tablet::KeyRange::new(participant_key.clone(), Some(participant_span_end)),
    )];

    let (txn_id, record_key, record_table) = cluster.txn_prepare_only(
        coordinator,
        "rcg_a",
        None,
        participant_spans,
        anchor_key.clone(),
        Some(b"anchor-committed".to_vec()),
    );
    let _ = cluster.txn_prepare_only(
        coordinator,
        "rcg_b",
        Some((txn_id, record_key, record_table)),
        Vec::new(),
        participant_key.clone(),
        Some(b"participant-committed".to_vec()),
    );

    // Time advances between the propose (the two prepares above) and the
    // crash — never crash in the same instant as a propose.
    cluster.run_for(Duration::from_millis(500));
    cluster.crash(coordinator);

    let reader = 1u64;
    let mut converged = false;
    for _ in 0..40 {
        cluster.run_for(Duration::from_secs(1));
        let r = cluster.raw_get(reader, "rcg_b", participant_key.clone(), true);
        if let Ok(Some(v)) = r {
            assert_eq!(
                v,
                b"participant-committed".to_vec(),
                "seed={seed}: recovered participant value mismatch"
            );
            converged = true;
            break;
        }
    }
    assert!(
        converged,
        "seed={seed}: the participant key never recovered to a committed value within budget"
    );

    // Atomicity: the anchor's own key must ALSO be visible now, never left
    // behind by a partial commit.
    let anchor_val = cluster
        .raw_get(reader, "rcg_a", anchor_key.clone(), true)
        .unwrap_or_else(|e| panic!("seed={seed}: anchor read failed: {e}"));
    assert_eq!(
        anchor_val,
        Some(b"anchor-committed".to_vec()),
        "seed={seed}: the anchor key must also be committed (atomicity) once the \
         participant key is"
    );

    cluster.restart(coordinator);
    let mut coord_converged = false;
    for _ in 0..40 {
        cluster.run_for(Duration::from_secs(1));
        if cluster.raw_get(coordinator, "rcg_b", participant_key.clone(), true)
            == Ok(Some(b"participant-committed".to_vec()))
        {
            coord_converged = true;
            break;
        }
    }
    assert!(
        coord_converged,
        "seed={seed}: the restarted coordinator's own view never converged to the \
         committed value"
    );
}

/// **Issue #731 (`SimRelayClient`'s deadlock) is fixed and directly
/// confirmed here — but a SECOND, distinct bug in `txn_recover` itself now
/// blocks this scenario, and that one is out of scope for this PR.**
///
/// Diagnosed, not merely observed, at both stages:
///
/// **Stage 1 (issue #731, fixed)**: at the pinned seed, every attempt to
/// read the participant key after the crash used to return the identical
/// `Err("sim relay: timed out waiting for a reply to req_id=N")` —
/// `SimRelayClient::relay`'s own timeout text — repeated, unchanging,
/// across the full 40s poll budget, while a control probe (a plain,
/// non-transactional `PutItem`/`GetItem` on a fresh key of the SAME table,
/// through the SAME reader node, in the SAME post-crash cluster state)
/// succeeded immediately — ordinary one-hop forwarding to the live new
/// leader was fine. The failure was specific to a **forwarded** read that
/// lands on a `FastRead::Foreign` intent: the request is forwarded once
/// (node1 → node2, tablet `rcg_b`'s new leader) exactly like the
/// successful probe, but node2's own `cp_serve_forwarded` handler then
/// needed a SECOND, nested outbound relay call of its own (`confirm_or_push`
/// → `ClientCtx::txn_status`, forwarding to whichever node leads the
/// anchor's tablet `rcg_a`) before it could finish answering the first —
/// and `SimRelayClient::serve_loop` (`crates/animus-node/src/sim_relay.rs`)
/// used to be one task per node processing `env.recv_stream(RELAY_STREAM)`
/// messages **strictly sequentially**, `.await`-ing each inbound request's
/// handler **inline**, so that nested call's own reply could only ever be
/// delivered by the same, currently-blocked `serve_loop` task: a genuine
/// self-deadlock, broken only by the nested call's own timeout — the
/// repeating, never-changing failure that was observed.
///
/// **Fixed**: `serve_loop` now dispatches each inbound `Request` onto its
/// own `env.spawn_task`ed task instead of awaiting it inline — see
/// `sim_relay.rs`'s own "One task per inbound request" doc section and ADR
/// 0061's "#731 closed" addendum for the full mechanism. Mirrors
/// production's own `AnimusdRelayClient` shape (one `tokio::spawn`ed task
/// per inbound TCP connection) — the bug was always `SimRelayClient`-only,
/// fixture-only, never reachable in a real cluster.
///
/// **Confirmed directly, not assumed**: with the fix applied, this
/// scenario's own poll loop no longer returns the relay timeout text at
/// all, at either seed tried — it returns `Err("transaction covering this
/// key is still pending; retry")` instead, unchanging across the full 40s
/// budget. That text is `cp_get_local_resolving_inner`'s own
/// `TxnDecisionStatus::Pending` arm, reached only *after*
/// `confirm_or_push`/`txn_recover` have both run to completion — proof the
/// nested relay hop(s) now succeed.
///
/// **Stage 2 (a second, distinct, pre-existing bug — NOT fixed by this
/// PR, and NOT caused by the relay change)**: `txn_recover`
/// (`crates/animusd/src/txn_coordinator.rs`) never gets past its own grace
/// check. Traced directly: whenever `self.cp_route(record_table,
/// record_key)` resolves to anything other than `CpRoute::Local` — the
/// ordinary case here, since this on-demand push runs on the *reading*
/// node's own leader (the participant's tablet, `rcg_b`), not necessarily
/// the *anchor*'s tablet leader (`rcg_a`) — `now_ms` is computed as
/// `self.env.now().duration_since(self.env.now())`: the elapsed gap
/// between two back-to-back clock reads, near-zero, not an absolute
/// timestamp. Checked against `now_ms < view.created_ts.wall_ms +
/// RECOVERY_GRACE`, a near-zero `now_ms` makes that comparison true
/// forever, so the grace check never passes and `txn_recover` declines
/// (`Pending`) on every call, permanently — regardless of how much virtual
/// time has actually elapsed. This is pre-existing, unrelated to and
/// unmodified by the relay-dispatch fix: it was introduced (knowingly, and
/// deliberately left unfixed as out of scope) by ADR 0061 rung C5 step
/// 3b's `tokio::time::Instant::now().elapsed()` → `Env` conversion — see
/// that rung's own `crates/animusd/CLAUDE.md` entry ("Two
/// `tokio::time::Instant::now().elapsed()` reads... had no literal
/// translation... reproducing the identical near-zero result rather than
/// 'fixing' what reads like a pre-existing latent bug — an incidental bug
/// gets its own PR"). It was unreachable before this PR only because issue
/// #731's deadlock intercepted every recovery attempt before
/// `txn_recover` was ever actually called.
///
/// **Scope**: fixing `txn_recover`'s grace-check (an absolute
/// virtual-time read is needed on the non-local branch too, not an
/// elapsed-duration one) is a change to a different subsystem (2PC
/// recovery, `txn_coordinator.rs`) than this PR's own scope (the
/// `SimRelayClient` dispatch fix). Kept `#[ignore]`d as a characterization
/// test with the full diagnosis in this doc comment, per the maintainer
/// standing instruction on a real finding — not silently dropped, not
/// worked around. **Issue to be filed** against `ClientCtx::txn_recover`'s
/// non-local grace-check branch.
///
/// `ANIMUS_SEED=<seed> cargo test -p animusd --lib
/// coordinator_never_finished_past_prepare_recovers_atomically -- --ignored`
/// replays this scenario at a specific seed (repo convention; note the
/// trailing `--ignored`, needed since both tests below are `#[ignore]`d).
#[test]
#[ignore = "FINDING (issue to be filed): ClientCtx::txn_recover's non-local grace-check \
            computes an elapsed near-zero duration instead of an absolute timestamp, so it \
            never passes and recovery declines forever when pushed from a node that is not \
            the anchor's own tablet leader — see this function's own doc for the full \
            diagnosis. Issue #731 (SimRelayClient's deadlock) IS fixed and confirmed here; \
            this is a second, distinct, pre-existing bug it uncovered, out of scope for the \
            #731 fix."]
fn coordinator_never_finished_past_prepare_recovers_atomically() {
    run_coordinator_never_finished_past_prepare_recovers_atomically(env_seed(0xC06F_0007));
}

#[test]
#[ignore = "FINDING (issue to be filed): see coordinator_never_finished_past_prepare_\
            recovers_atomically's own doc — ClientCtx::txn_recover's non-local grace-check \
            never passes, a second bug issue #731's own fix uncovered but does not cause"]
fn coordinator_never_finished_past_prepare_recovers_atomically_over_seeds() {
    for i in 0..5 {
        run_coordinator_never_finished_past_prepare_recovers_atomically(0xC06F_7000 + i);
    }
}
