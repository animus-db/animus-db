//! `SimCluster`-driven deterministic reachability smoke for `ExecuteStatement`/
//! `BatchExecuteStatement`/`ExecuteTransaction` (PartiQL, ADR 0071) — the
//! first proof any of the three execute through the generic dispatch core,
//! reachable under `SimEnv` for the first time (ADR 0061 rung F, C-06
//! PR 5).
//!
//! **The dispatch change (`dynamo.rs`)**: three new parallel generic
//! siblings — [`crate::dynamo::execute_statement_as`]/
//! [`crate::dynamo::execute_transaction_as`]/
//! `run_batch_execute_statement_as` (plus its own private
//! `execute_one_batch_statement_as` helper) — the PartiQL analogue of
//! `execute_item_op_as` (ADR 0061 rung D2 PR 1) — and three new
//! `dispatch_item_op` match arms routing `Operation::ExecuteStatement`/
//! `Operation::BatchExecuteStatement`/`Operation::ExecuteTransaction` to
//! them. `run_operation`, `execute_statement`, `execute_transaction`,
//! `run_batch_execute_statement`, and `execute_one_batch_statement` are all
//! byte-identical, unchanged — see `dynamo.rs`'s own "SimEnv-capable
//! PartiQL siblings" section header for the full account, including the two
//! distinct mutual-recursion cycles this rung closes: `execute_statement`'s
//! own pre-existing `Box::pin(run_operation(..))` (untouched — it still
//! recurses into the concrete, production-only `run_operation` for its
//! `INSERT`/`UPDATE`/`DELETE` arms), and a **second, new** cycle —
//! `dispatch_item_op`'s own new `ExecuteStatement` arm calls
//! `execute_statement_as`, whose `INSERT`/`UPDATE`/`DELETE` arms call a new
//! `dispatch_lowered_write_as` helper, which calls back into
//! `dispatch_item_op` — closed by boxing `dispatch_lowered_write_as`'s own
//! call, not `execute_statement_as`'s call sites (found by the compiler's
//! `E0733` the moment `dispatch_item_op` gained the `ExecuteStatement` arm,
//! not anticipated up front — see `docs/engineering-lessons.md`'s matching
//! entry).
//!
//! Deeper PartiQL fault-injection coverage (GSI/LSI `SELECT`, a corpus
//! equivalence cell) is PR 6's own scope (ADR 0061 rung F) — this module is
//! reachability smoke only, following C-06 PR 3's own precedent for
//! Transact (`sim_cluster_dynamo_transact.rs`): a handful of scenarios
//! proving the wire request actually runs through the generic path, each
//! issued from a **non-leader** node so the forwarding path is exercised
//! too (mirroring `sim_cluster_dynamo.rs`'s own `put_then_consistent_get_
//! through_wire_from_a_non_leader_node`).
//!
//! # Scenarios (seed-parameterized, `_over_seeds` at 5 seeds each)
//!
//! (a) [`insert_then_select_sees_it`] — an `INSERT` (no `RETURNING`, so an
//!     empty `Items` array) then a `SELECT` with an exact-key `WHERE` sees
//!     it.
//! (b) [`update_and_delete_with_returning`] — `UPDATE ... RETURNING ALL
//!     NEW *` echoes the post-update image; `DELETE ... RETURNING ALL
//!     OLD *` echoes the pre-delete image and the item is actually gone
//!     afterward.
//! (c) [`batch_execute_statement_mixed_batch_runs_in_order`] — a mixed
//!     `BatchExecuteStatement` (`INSERT`, a `SELECT` hit, `UPDATE ...
//!     RETURNING ALL NEW *`, `DELETE ... RETURNING ALL OLD *` on the SAME
//!     batch's own just-inserted item) runs every statement, in request
//!     order, with no cross-statement atomicity — the `DELETE`'s own
//!     `RETURNING` proves in-batch sequential ordering, since it echoes a
//!     value only the batch's own earlier `INSERT` could have written.
//! (d) [`execute_transaction_commits_across_two_tables`] — an all-`INSERT`
//!     `ExecuteTransaction` commits atomically across two tables.
//! (e) [`execute_transaction_condition_failure_cancels`] — a duplicate
//!     `INSERT` inside a transaction cancels the WHOLE transaction (per-
//!     action `CancellationReasons`, and the transaction's other statement
//!     does not land even though it precedes the failing one in list
//!     order).
//!
//! Replays (repo convention): `ANIMUS_SEED=<seed> cargo test -p animusd
//! --lib insert_then_select_sees_it`.

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// One DynamoDB wire `CreateTable` for a plain single-key (`pk`, string)
/// table named `table`, issued from node 0 — mirrors `sim_cluster_dynamo_
/// transact.rs`'s own identically-named helper.
fn create_table(cluster: &mut SimCluster, table: &str) -> (u16, String) {
    let body = format!(
        r#"{{"TableName":"{table}",
            "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]}}"#
    );
    cluster.dynamo(0, "DynamoDB_20120810.CreateTable", body.as_bytes())
}

/// The node id of a **non-leader** replica of `table`'s tablet on this
/// (3-node, RF3) `SimCluster` — mirrors `sim_cluster_dynamo.rs`'s own
/// `put_then_consistent_get_through_wire_from_a_non_leader_node` helper, so
/// every scenario below exercises the forwarding path, not just the local
/// leader-served one. `table` was created over the DynamoDB wire (this
/// module's own `create_table`), not `SimCluster::create_table` itself, so
/// its tablet is looked up from the replicated catalog
/// (`Metadata::tablets_for_table`) rather than `SimCluster::tablet_of`
/// (which only tracks tablets its own in-process `create_table` minted) —
/// mirrors `sim_cluster_dynamo_transact.rs`'s identical lookup for a
/// wire-created table.
fn non_leader(cluster: &SimCluster, table: &str) -> u64 {
    let tablet = cluster
        .metadata(0)
        .tablets_for_table(table)
        .next()
        .map(|(id, _)| *id)
        .unwrap_or_else(|| panic!("{table} has no tablet"));
    let leader = cluster
        .leader_index_of(tablet)
        .expect("the fresh group elected a leader");
    (0..cluster.node_count() as u64)
        .find(|&n| n != leader)
        .expect("a 3-node cluster has a non-leader node")
}

fn execute_statement(cluster: &mut SimCluster, node: u64, body: &str) -> (u16, serde_json::Value) {
    let (status, resp) =
        cluster.dynamo(node, "DynamoDB_20120810.ExecuteStatement", body.as_bytes());
    let json: serde_json::Value =
        serde_json::from_str(&resp).unwrap_or_else(|e| panic!("response is not JSON: {e}: {resp}"));
    (status, json)
}

fn batch_execute_statement(
    cluster: &mut SimCluster,
    node: u64,
    body: &str,
) -> (u16, serde_json::Value) {
    let (status, resp) = cluster.dynamo(
        node,
        "DynamoDB_20120810.BatchExecuteStatement",
        body.as_bytes(),
    );
    let json: serde_json::Value =
        serde_json::from_str(&resp).unwrap_or_else(|e| panic!("response is not JSON: {e}: {resp}"));
    (status, json)
}

fn execute_transaction(
    cluster: &mut SimCluster,
    node: u64,
    body: &str,
) -> (u16, serde_json::Value) {
    let (status, resp) = cluster.dynamo(
        node,
        "DynamoDB_20120810.ExecuteTransaction",
        body.as_bytes(),
    );
    let json: serde_json::Value =
        serde_json::from_str(&resp).unwrap_or_else(|e| panic!("response is not JSON: {e}: {resp}"));
    (status, json)
}

/// Parse a `TransactionCanceledException` body's `CancellationReasons`
/// array — mirrors `sim_cluster_dynamo_transact.rs`'s own helper, over an
/// already-parsed [`serde_json::Value`] rather than raw text.
fn cancellation_reasons(body: &serde_json::Value) -> Vec<serde_json::Value> {
    body["CancellationReasons"]
        .as_array()
        .cloned()
        .unwrap_or_else(|| panic!("no CancellationReasons array in: {body}"))
}

// ---------------------------------------------------------------------------
// (a) INSERT then SELECT sees it.
// ---------------------------------------------------------------------------

fn run_insert_then_select_sees_it(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, "pql_a");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(pql_a) failed: {body}"
    );

    let node = non_leader(&cluster, "pql_a");

    let (status, resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"INSERT INTO pql_a VALUE {'pk': ?, 'v': ?}",
            "Parameters":[{"S":"k1"},{"S":"hello"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: INSERT failed: {resp}");
    assert!(
        resp["Items"].as_array().unwrap().is_empty(),
        "seed={seed}: no RETURNING clause means an empty Items array: {resp}"
    );

    let (status, sel) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"SELECT * FROM pql_a WHERE pk = ?",
            "Parameters":[{"S":"k1"}],"ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: SELECT failed: {sel}");
    let items = sel["Items"].as_array().expect("Items");
    assert_eq!(items.len(), 1, "seed={seed}: {sel}");
    assert_eq!(items[0]["v"]["S"], "hello");
}

/// `ANIMUS_SEED=<seed> cargo test -p animusd --lib insert_then_select_sees_it`
/// replays this scenario at a specific seed (repo convention).
#[test]
fn insert_then_select_sees_it() {
    run_insert_then_select_sees_it(env_seed(0xC06F_5001));
}

#[test]
fn insert_then_select_sees_it_over_seeds() {
    for i in 0..5 {
        run_insert_then_select_sees_it(0xC06F_5100 + i);
    }
}

// ---------------------------------------------------------------------------
// (b) UPDATE ... RETURNING ALL NEW *, then DELETE ... RETURNING ALL OLD *.
// ---------------------------------------------------------------------------

fn run_update_and_delete_with_returning(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, "pql_b");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(pql_b) failed: {body}"
    );
    let node = non_leader(&cluster, "pql_b");

    let (status, resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"INSERT INTO pql_b VALUE {'pk': ?, 'v': ?}",
            "Parameters":[{"S":"k1"},{"S":"lo"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: seed INSERT failed: {resp}");

    let (status, resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"UPDATE pql_b SET v = ? WHERE pk = ? RETURNING ALL NEW *",
            "Parameters":[{"S":"hi"},{"S":"k1"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: UPDATE failed: {resp}");
    let items = resp["Items"].as_array().expect("Items");
    assert_eq!(items.len(), 1, "seed={seed}: {resp}");
    assert_eq!(items[0]["v"]["S"], "hi");

    let (status, resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"DELETE FROM pql_b WHERE pk = ? RETURNING ALL OLD *",
            "Parameters":[{"S":"k1"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: DELETE failed: {resp}");
    let items = resp["Items"].as_array().expect("Items");
    assert_eq!(items.len(), 1, "seed={seed}: {resp}");
    assert_eq!(
        items[0]["v"]["S"], "hi",
        "seed={seed}: DELETE's RETURNING ALL OLD * must echo the UPDATE's own value: {resp}"
    );

    let (status, sel) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"SELECT * FROM pql_b WHERE pk = ?",
            "Parameters":[{"S":"k1"}],"ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: SELECT failed: {sel}");
    assert!(
        sel["Items"].as_array().unwrap().is_empty(),
        "seed={seed}: item must be gone after DELETE: {sel}"
    );
}

#[test]
fn update_and_delete_with_returning() {
    run_update_and_delete_with_returning(env_seed(0xC06F_5002));
}

#[test]
fn update_and_delete_with_returning_over_seeds() {
    for i in 0..5 {
        run_update_and_delete_with_returning(0xC06F_5200 + i);
    }
}

// ---------------------------------------------------------------------------
// (c) BatchExecuteStatement: a mixed batch runs every statement, in order,
// with no cross-statement atomicity.
// ---------------------------------------------------------------------------

fn run_batch_execute_statement_mixed_batch_runs_in_order(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, "pql_c");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(pql_c) failed: {body}"
    );
    let node = non_leader(&cluster, "pql_c");

    let (status, resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"INSERT INTO pql_c VALUE {'pk': ?, 'v': ?}",
            "Parameters":[{"S":"alpha"},{"S":"a"}]}"#,
    );
    assert_eq!(
        status, 200,
        "seed={seed}: seed INSERT(alpha) failed: {resp}"
    );
    let (status, resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"INSERT INTO pql_c VALUE {'pk': ?, 'v': ?}",
            "Parameters":[{"S":"beta"},{"S":"b"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: seed INSERT(beta) failed: {resp}");

    // [0] INSERT a fresh key ("delta"). [1] SELECT the pre-seeded "alpha".
    // [2] UPDATE the pre-seeded "beta" with RETURNING. [3] DELETE the SAME
    // "delta" [0] just inserted, with RETURNING — its echoed value can only
    // be "d" if [0] already landed by the time [3] runs, proving in-batch
    // sequential (not concurrent, not reordered) execution.
    let body = r#"{"Statements":[
        {"Statement":"INSERT INTO pql_c VALUE {'pk': ?, 'v': ?}","Parameters":[{"S":"delta"},{"S":"d"}]},
        {"Statement":"SELECT * FROM pql_c WHERE pk = ?","Parameters":[{"S":"alpha"}],"ConsistentRead":true},
        {"Statement":"UPDATE pql_c SET v = ? WHERE pk = ? RETURNING ALL NEW *","Parameters":[{"S":"hi"},{"S":"beta"}]},
        {"Statement":"DELETE FROM pql_c WHERE pk = ? RETURNING ALL OLD *","Parameters":[{"S":"delta"}]}
    ]}"#;
    let (status, resp) = batch_execute_statement(&mut cluster, node, body);
    assert_eq!(status, 200, "seed={seed}: {resp}");
    let responses = resp["Responses"].as_array().expect("Responses array");
    assert_eq!(responses.len(), 4, "seed={seed}: {responses:?}");

    assert_eq!(responses[0]["TableName"], "pql_c");
    assert!(
        responses[0].get("Item").is_none(),
        "seed={seed}: {responses:?}"
    );
    assert!(
        responses[0].get("Error").is_none(),
        "seed={seed}: {responses:?}"
    );

    assert_eq!(
        responses[1]["Item"]["v"]["S"], "a",
        "seed={seed}: {responses:?}"
    );
    assert_eq!(
        responses[2]["Item"]["v"]["S"], "hi",
        "seed={seed}: {responses:?}"
    );
    assert_eq!(
        responses[3]["Item"]["v"]["S"], "d",
        "seed={seed}: DELETE's own RETURNING must echo this batch's earlier INSERT, proving \
         in-order execution: {responses:?}"
    );

    // The batch's own writes actually landed cluster-wide: "delta" (written
    // by [0], removed by [3]) is gone, and "beta" carries the UPDATE.
    let (status, sel) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"SELECT * FROM pql_c WHERE pk = ?",
            "Parameters":[{"S":"delta"}],"ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {sel}");
    assert!(
        sel["Items"].as_array().unwrap().is_empty(),
        "seed={seed}: the batch's own DELETE must have removed delta: {sel}"
    );
    let (status, sel) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"SELECT * FROM pql_c WHERE pk = ?",
            "Parameters":[{"S":"beta"}],"ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {sel}");
    assert_eq!(sel["Items"][0]["v"]["S"], "hi");
}

#[test]
fn batch_execute_statement_mixed_batch_runs_in_order() {
    run_batch_execute_statement_mixed_batch_runs_in_order(env_seed(0xC06F_5003));
}

#[test]
fn batch_execute_statement_mixed_batch_runs_in_order_over_seeds() {
    for i in 0..5 {
        run_batch_execute_statement_mixed_batch_runs_in_order(0xC06F_5300 + i);
    }
}

// ---------------------------------------------------------------------------
// (d) ExecuteTransaction: an all-INSERT transaction commits across two
// tables.
// ---------------------------------------------------------------------------

fn run_execute_transaction_commits_across_two_tables(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, "pql_d1");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(pql_d1) failed: {body}"
    );
    let (status, body) = create_table(&mut cluster, "pql_d2");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(pql_d2) failed: {body}"
    );

    let node = non_leader(&cluster, "pql_d1");

    let (status, resp) = execute_transaction(
        &mut cluster,
        node,
        r#"{"TransactStatements":[
            {"Statement":"INSERT INTO pql_d1 VALUE {'pk': ?, 'v': ?}",
             "Parameters":[{"S":"x1"},{"S":"lo"}]},
            {"Statement":"INSERT INTO pql_d2 VALUE {'pk': ?, 'v': ?}",
             "Parameters":[{"S":"x2"},{"S":"hi"}]}]}"#,
    );
    assert_eq!(
        status, 200,
        "seed={seed}: ExecuteTransaction failed: {resp}"
    );
    let responses = resp["Responses"].as_array().expect("Responses array");
    assert_eq!(responses.len(), 2, "seed={seed}: {responses:?}");
    for r in responses {
        assert_eq!(
            r.as_object().unwrap().len(),
            0,
            "seed={seed}: a write transaction's own Responses entries must be empty objects: \
             {resp}"
        );
    }

    let (status, sel) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"SELECT * FROM pql_d1 WHERE pk = ?",
            "Parameters":[{"S":"x1"}],"ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {sel}");
    assert_eq!(sel["Items"][0]["v"]["S"], "lo");
    let (status, sel) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"SELECT * FROM pql_d2 WHERE pk = ?",
            "Parameters":[{"S":"x2"}],"ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {sel}");
    assert_eq!(sel["Items"][0]["v"]["S"], "hi");
}

#[test]
fn execute_transaction_commits_across_two_tables() {
    run_execute_transaction_commits_across_two_tables(env_seed(0xC06F_5004));
}

#[test]
fn execute_transaction_commits_across_two_tables_over_seeds() {
    for i in 0..5 {
        run_execute_transaction_commits_across_two_tables(0xC06F_5400 + i);
    }
}

// ---------------------------------------------------------------------------
// (e) ExecuteTransaction: a duplicate INSERT cancels the WHOLE transaction.
// ---------------------------------------------------------------------------

fn run_execute_transaction_condition_failure_cancels(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, "pql_e");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(pql_e) failed: {body}"
    );
    let node = non_leader(&cluster, "pql_e");

    // Seed an existing item so the transaction's own duplicate INSERT fails
    // its implicit attribute_not_exists(pk) condition.
    let (status, resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"INSERT INTO pql_e VALUE {'pk': ?}","Parameters":[{"S":"dup"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: seed INSERT failed: {resp}");

    // [0] a fresh INSERT that would succeed in isolation, [1] a duplicate
    // INSERT at the already-seeded key — must cancel BOTH.
    let (status, resp) = execute_transaction(
        &mut cluster,
        node,
        r#"{"TransactStatements":[
            {"Statement":"INSERT INTO pql_e VALUE {'pk': ?}","Parameters":[{"S":"should-not-land"}]},
            {"Statement":"INSERT INTO pql_e VALUE {'pk': ?}","Parameters":[{"S":"dup"}]}]}"#,
    );
    assert_eq!(
        status, 400,
        "seed={seed}: expected the duplicate to cancel: {resp}"
    );
    assert_eq!(
        resp["__type"], "com.amazonaws.dynamodb.v20120810#TransactionCanceledException",
        "seed={seed}: {resp}"
    );
    let reasons = cancellation_reasons(&resp);
    assert_eq!(reasons.len(), 2, "seed={seed}: {resp}");
    assert_eq!(reasons[0]["Code"], "None");
    assert_eq!(reasons[1]["Code"], "ConditionalCheckFailed");

    let (status, sel) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"SELECT * FROM pql_e WHERE pk = ?",
            "Parameters":[{"S":"should-not-land"}],"ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {sel}");
    assert!(
        sel["Items"].as_array().unwrap().is_empty(),
        "seed={seed}: the first statement's item must NOT have been written by a cancelled \
         transaction: {sel}"
    );
}

#[test]
fn execute_transaction_condition_failure_cancels() {
    run_execute_transaction_condition_failure_cancels(env_seed(0xC06F_5005));
}

#[test]
fn execute_transaction_condition_failure_cancels_over_seeds() {
    for i in 0..5 {
        run_execute_transaction_condition_failure_cancels(0xC06F_5500 + i);
    }
}
