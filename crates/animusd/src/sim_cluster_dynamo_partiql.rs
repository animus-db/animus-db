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
//!
//! # C-06 PR 6 (ADR 0061 rung F): deeper deterministic PartiQL coverage
//!
//! PR 5's five scenarios above proved the generic dispatch path exists at
//! all. This PR adds 27 more `SimCluster` siblings, one per named real-socket
//! LOGIC test in `tests/dynamo_partiql.rs`/`tests/dynamo_execute_transaction.
//! rs` — statement semantics, error mapping, pagination shape, index
//! routing, and cancellation reasons — each named identically to the
//! real-socket test it mirrors, each issued from a **non-leader** node of a
//! 3-node RF3 `SimCluster` (exercising the forwarding path every scenario in
//! this module already relies on), each with a `_over_seeds` sibling at 5
//! seeds. `setup_events_logs` reproduces `dynamo_partiql.rs::setup()`'s own
//! two-table fixture (`events`: composite `pk`(S)/`sk`(N) + `region` +
//! `cat` with a `by-cat` GSI; `logs`: composite `pk`(S)/`sk`(S)) verbatim,
//! minus the GSI backfill itself — this fixture has no background drain
//! loop, so a GSI-touching scenario calls [`SimCluster::drain_gsi`]
//! explicitly instead of polling for eventual convergence the way the
//! real-socket suite's `await_gsi_select` does.
//!
//! **Converted (27, by real-socket source file):**
//!
//! From `tests/dynamo_partiql.rs` (20): `select_partition_equality_
//! matches_query`, `select_begins_with_sort_key_matches_query`,
//! `select_sort_comparator_and_between_match_query_numeric_ordering`,
//! `select_non_key_where_matches_scan_with_filter`, `select_projection_
//! narrows_returned_attributes`, `select_from_table_dot_index_queries_the_
//! gsi`, `gsi_projected_attribute_updated_via_partiql_is_visible_through_
//! index_query`, `order_by_desc_matches_scan_index_forward_false`,
//! `pagination_next_token_matches_query_last_evaluated_key_walk`, `next_
//! token_rejected_when_replayed_against_a_different_statement`, `insert_on_
//! conflict_do_nothing_swallows_duplicate`, `duplicate_insert_gives_
//! duplicate_item_exception_and_leaves_item_unchanged`, `update_set_on_
//! existing_item_with_returning_all_new`, `update_of_missing_item_fails`,
//! `update_with_non_key_where_term_as_condition_met_and_unmet`, `update_
//! where_missing_partition_key_is_a_validation_exception`, `delete_where_
//! missing_sort_key_is_a_validation_exception`, `unknown_table_is_resource_
//! not_found`, `malformed_statement_is_a_validation_exception`, `literal_
//! value_in_where_is_rejected`.
//!
//! From `tests/dynamo_execute_transaction.rs` (7): `execute_transaction_
//! write_commits_atomically_across_two_tables` (a fresh sibling matching the
//! real test's own name/shape, distinct from PR 5's own similarly-shaped but
//! differently-named `execute_transaction_commits_across_two_tables`
//! smoke — both are kept, per this PR's own instruction not to delete or
//! rename PR 5's scenarios), `execute_transaction_write_cancels_whole_on_
//! duplicate_insert`, `execute_transaction_rejects_zero_and_too_many_
//! statements`, `execute_transaction_mixed_select_and_insert_is_validation_
//! exception`, `execute_transaction_client_request_token_replay_is_cached`,
//! `execute_transaction_all_select_returns_items_and_misses_in_order`,
//! `execute_transaction_over_a_follower_connected_node` (issued from a
//! non-leader node by this module's own standing construction — every
//! scenario here already exercises the identical forwarding path the real
//! test's dedicated 2-node cluster sets up specifically to prove, so no
//! special-cased fixture was needed).
//!
//! **Deliberately not converted, and why** (real-socket-only):
//!
//! - `throttled_table_throttles_a_partiql_insert` — throttle-window timing,
//!   named explicitly out of scope by this PR's own brief; `SimCluster`'s
//!   `ThrottleBucket` corpus already lives in `sim_cluster_throttle.rs` and
//!   has no PartiQL-specific angle worth duplicating here.
//! - `insert_then_select_sees_it` (`dynamo_partiql.rs`) — already PR 5's own
//!   scenario (a) of the identical name; converting it a second time under
//!   this PR would just be the same test twice.
//! - `delete_with_returning_all_old` (`dynamo_partiql.rs`) — subsumed by PR
//!   5's own scenario (b), `update_and_delete_with_returning`, which already
//!   proves `DELETE ... RETURNING ALL OLD *`'s exact shape (echoing the
//!   pre-delete image, item gone afterward) as its own second half.
//! - `delete_of_missing_key_is_a_silent_success`, `delete_returning_all_new_
//!   is_rejected` (`dynamo_partiql.rs`), and every `batch_execute_statement_
//!   *` test beyond PR 5's own scenario (c) (`one_failure_does_not_block_
//!   the_others`, `select_rejects_a_sort_key_range`, `zero_and_over_cap_
//!   are_top_level_validation_exceptions`, `through_a_follower_connected_
//!   node`) — not named in this PR's own candidate list; each is a genuine,
//!   distinct LOGIC test with no `SimCluster` blocker, left for a future
//!   pass rather than converted here, per this PR's own scope (the
//!   coordinator's enumerated candidate list is authoritative for what this
//!   PR converts).
//!
//! **No bug found.** Every converted scenario passed at its pinned seed and
//! every `_over_seeds` seed on the first clean run — the generic dispatch
//! path PR 5 wired (`execute_statement_as`/`execute_transaction_as`/
//! `run_batch_execute_statement_as`, unchanged by this PR) behaves
//! identically to the concrete, real-socket-only path for every scenario
//! converted here.
//!
//! Replays (repo convention): `ANIMUS_SEED=<seed> cargo test -p animusd
//! --lib select_partition_equality_matches_query`.

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

// ---------------------------------------------------------------------------
// C-06 PR 6: helpers shared by the 27 siblings below.
// ---------------------------------------------------------------------------

/// Stand up the `events`/`logs` two-table fixture, mirroring
/// `dynamo_partiql.rs::setup()`'s shapes and seed data exactly (same
/// partition key `p1`, same 6 `events` rows, same 4 `logs` rows) — minus the
/// GSI backfill, which this fixture materializes on demand via
/// [`SimCluster::drain_gsi`] rather than a background drain loop.
///
/// | events | pk | sk   | region | cat |
/// |--------|----|------|--------|-----|
/// |        | p1 | -5   | us     | X   |
/// |        | p1 | -1.5 | eu     | X   |
/// |        | p1 | 0    | us     | X   |
/// |        | p1 | 2    | eu     | X   |
/// |        | p1 | 4.5  | us     | X   |
/// |        | p1 | 10   | eu     | X   |
///
/// | logs | pk | sk     |
/// |------|----|--------|
/// |      | p1 | alpha  |
/// |      | p1 | alpha2 |
/// |      | p1 | beta   |
/// |      | p1 | gamma  |
fn setup_events_logs(cluster: &mut SimCluster) {
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"events","AttributeDefinitions":[
                {"AttributeName":"pk","AttributeType":"S"},
                {"AttributeName":"sk","AttributeType":"N"},
                {"AttributeName":"cat","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-cat",
                 "KeySchema":[{"AttributeName":"cat","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"ALL"}}]}"#,
    );
    assert_eq!(status, 200, "CreateTable(events): {body}");

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"logs","AttributeDefinitions":[
                {"AttributeName":"pk","AttributeType":"S"},
                {"AttributeName":"sk","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}]}"#,
    );
    assert_eq!(status, 200, "CreateTable(logs): {body}");

    for (sk, region) in [
        ("-5", "us"),
        ("-1.5", "eu"),
        ("0", "us"),
        ("2", "eu"),
        ("4.5", "us"),
        ("10", "eu"),
    ] {
        let item = format!(
            r#"{{"TableName":"events","Item":{{"pk":{{"S":"p1"}},"sk":{{"N":"{sk}"}},
                "region":{{"S":"{region}"}},"cat":{{"S":"X"}}}}}}"#
        );
        let (status, body) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", item.as_bytes());
        assert_eq!(status, 200, "PutItem(events sk={sk}): {body}");
    }

    for sk in ["alpha", "alpha2", "beta", "gamma"] {
        let item =
            format!(r#"{{"TableName":"logs","Item":{{"pk":{{"S":"p1"}},"sk":{{"S":"{sk}"}}}}}}"#);
        let (status, body) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", item.as_bytes());
        assert_eq!(status, 200, "PutItem(logs sk={sk}): {body}");
    }
}

/// The `sk` values of a response's `Items`, in response order — mirrors
/// `dynamo_partiql.rs::item_sks`.
fn item_sks(resp: &serde_json::Value) -> Vec<String> {
    resp["Items"]
        .as_array()
        .expect("Items array")
        .iter()
        .map(|item| {
            if let Some(n) = item["sk"].get("N").and_then(serde_json::Value::as_str) {
                n.to_string()
            } else {
                item["sk"]["S"].as_str().expect("sk").to_string()
            }
        })
        .collect()
}

/// Ascending numeric sort of a `sk`-string vector, matching a `Query`'s own
/// numeric ordering — mirrors `dynamo_partiql.rs`'s repeated inline
/// `sort_by(|a, b| a.parse::<f64>()... .total_cmp(...))` idiom, factored out
/// once here since several scenarios below need it.
fn sort_numeric(v: &mut [String]) {
    v.sort_by(|a, b| {
        a.parse::<f64>()
            .unwrap()
            .total_cmp(&b.parse::<f64>().unwrap())
    });
}

fn query(cluster: &mut SimCluster, node: u64, body: &str) -> (u16, serde_json::Value) {
    let (status, resp) = cluster.dynamo(node, "DynamoDB_20120810.Query", body.as_bytes());
    let json: serde_json::Value =
        serde_json::from_str(&resp).unwrap_or_else(|e| panic!("response is not JSON: {e}: {resp}"));
    (status, json)
}

fn scan(cluster: &mut SimCluster, node: u64, body: &str) -> (u16, serde_json::Value) {
    let (status, resp) = cluster.dynamo(node, "DynamoDB_20120810.Scan", body.as_bytes());
    let json: serde_json::Value =
        serde_json::from_str(&resp).unwrap_or_else(|e| panic!("response is not JSON: {e}: {resp}"));
    (status, json)
}

/// Drain `events`' own GSI (`by-cat`) into its hidden table — this fixture
/// materializes a GSI on demand ([`SimCluster::drain_gsi`]) rather than
/// running a background drain loop; `drain_gsi` must be called against the
/// tablet's own LEADER, not an arbitrary node (mirrors `sim_cluster_dynamo_
/// indexes.rs::gsi_write_then_query_sim`'s identical lookup).
fn drain_events_gsi(cluster: &mut SimCluster) {
    let tablet = {
        let meta = cluster.metadata(0);
        *meta
            .tablets_for_table("events")
            .next()
            .expect("events has a tablet")
            .0
    };
    let leader = cluster
        .leader_index_of(tablet)
        .expect("events tablet has a leader");
    cluster.drain_gsi(leader, "events");
}

// ---------------------------------------------------------------------------
// (1) select_partition_equality_matches_query
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_partiql.rs::select_partition_equality_matches_query`.
fn run_select_partition_equality_matches_query(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    setup_events_logs(&mut cluster);
    let node = non_leader(&cluster, "events");

    let (status, select_resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"SELECT * FROM events WHERE pk = ?",
            "Parameters":[{"S":"p1"}],"ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {select_resp}");

    let (status, query_resp) = query(
        &mut cluster,
        node,
        r#"{"TableName":"events","KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}},"ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {query_resp}");

    let mut select_sks = item_sks(&select_resp);
    let mut query_sks = item_sks(&query_resp);
    sort_numeric(&mut select_sks);
    sort_numeric(&mut query_sks);
    assert_eq!(select_sks, query_sks, "seed={seed}");
    assert_eq!(select_sks.len(), 6, "seed={seed}");
    assert!(
        select_resp.get("NextToken").is_none(),
        "seed={seed}: {select_resp}"
    );
}

#[test]
fn select_partition_equality_matches_query() {
    run_select_partition_equality_matches_query(env_seed(0xC06F_6001));
}

#[test]
fn select_partition_equality_matches_query_over_seeds() {
    for i in 0..5 {
        run_select_partition_equality_matches_query(0xC06F_7010 + i);
    }
}

// ---------------------------------------------------------------------------
// (2) select_begins_with_sort_key_matches_query
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_partiql.rs::select_begins_with_sort_key_matches_query`.
fn run_select_begins_with_sort_key_matches_query(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    setup_events_logs(&mut cluster);
    let node = non_leader(&cluster, "logs");

    let (status, select_resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"SELECT * FROM logs WHERE pk = ? AND begins_with(sk, ?)",
            "Parameters":[{"S":"p1"},{"S":"alpha"}],"ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {select_resp}");
    assert_eq!(
        item_sks(&select_resp),
        vec!["alpha", "alpha2"],
        "seed={seed}"
    );

    let (status, query_resp) = query(
        &mut cluster,
        node,
        r#"{"TableName":"logs","KeyConditionExpression":"pk = :p AND begins_with(sk, :p2)",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":p2":{"S":"alpha"}},
            "ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {query_resp}");
    assert_eq!(item_sks(&select_resp), item_sks(&query_resp), "seed={seed}");
}

#[test]
fn select_begins_with_sort_key_matches_query() {
    run_select_begins_with_sort_key_matches_query(env_seed(0xC06F_6002));
}

#[test]
fn select_begins_with_sort_key_matches_query_over_seeds() {
    for i in 0..5 {
        run_select_begins_with_sort_key_matches_query(0xC06F_7020 + i);
    }
}

// ---------------------------------------------------------------------------
// (3) select_sort_comparator_and_between_match_query_numeric_ordering
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_partiql.rs::select_sort_comparator_and_between_match_
/// query_numeric_ordering` — ADR 0063's numeric sort-key ordering (negative,
/// fractional, positive `N` values all compare correctly).
fn run_select_sort_comparator_and_between_match_query_numeric_ordering(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    setup_events_logs(&mut cluster);
    let node = non_leader(&cluster, "events");

    let (status, gt_resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"SELECT * FROM events WHERE pk = ? AND sk > ?",
            "Parameters":[{"S":"p1"},{"N":"-1.5"}],"ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {gt_resp}");
    assert_eq!(
        item_sks(&gt_resp),
        vec!["0", "2", "4.5", "10"],
        "seed={seed}"
    );

    let (status, between_resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"SELECT * FROM events WHERE pk = ? AND sk BETWEEN ? AND ?",
            "Parameters":[{"S":"p1"},{"N":"-5"},{"N":"2"}],"ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {between_resp}");

    let (status, query_between) = query(
        &mut cluster,
        node,
        r#"{"TableName":"events","KeyConditionExpression":"pk = :p AND sk BETWEEN :lo AND :hi",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":lo":{"N":"-5"},":hi":{"N":"2"}},
            "ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {query_between}");
    assert_eq!(
        item_sks(&between_resp),
        item_sks(&query_between),
        "seed={seed}"
    );
    assert_eq!(
        item_sks(&between_resp),
        vec!["-5", "-1.5", "0", "2"],
        "seed={seed}"
    );
}

#[test]
fn select_sort_comparator_and_between_match_query_numeric_ordering() {
    run_select_sort_comparator_and_between_match_query_numeric_ordering(env_seed(0xC06F_6003));
}

#[test]
fn select_sort_comparator_and_between_match_query_numeric_ordering_over_seeds() {
    for i in 0..5 {
        run_select_sort_comparator_and_between_match_query_numeric_ordering(0xC06F_7030 + i);
    }
}

// ---------------------------------------------------------------------------
// (4) select_non_key_where_matches_scan_with_filter
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_partiql.rs::select_non_key_where_matches_scan_with_
/// filter` — the fuller real-test shape (an actual `Scan` comparison, not a
/// simplified stand-in): a `WHERE` with no partition-key equality term lowers
/// to `Scan` with a filter, matching a hand-built `Scan` exactly.
fn run_select_non_key_where_matches_scan_with_filter(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    setup_events_logs(&mut cluster);
    let node = non_leader(&cluster, "events");

    let (status, select_resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"SELECT * FROM events WHERE region = ?",
            "Parameters":[{"S":"us"}],"ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {select_resp}");

    let (status, scan_resp) = scan(
        &mut cluster,
        node,
        r#"{"TableName":"events","FilterExpression":"region = :r",
            "ExpressionAttributeValues":{":r":{"S":"us"}},"ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {scan_resp}");

    let mut select_sks = item_sks(&select_resp);
    let mut scan_sks = item_sks(&scan_resp);
    sort_numeric(&mut select_sks);
    sort_numeric(&mut scan_sks);
    assert_eq!(select_sks, scan_sks, "seed={seed}");
    assert_eq!(select_sks, vec!["-5", "0", "4.5"], "seed={seed}");
}

#[test]
fn select_non_key_where_matches_scan_with_filter() {
    run_select_non_key_where_matches_scan_with_filter(env_seed(0xC06F_6004));
}

#[test]
fn select_non_key_where_matches_scan_with_filter_over_seeds() {
    for i in 0..5 {
        run_select_non_key_where_matches_scan_with_filter(0xC06F_7040 + i);
    }
}

// ---------------------------------------------------------------------------
// (5) select_projection_narrows_returned_attributes
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_partiql.rs::select_projection_narrows_returned_
/// attributes`.
fn run_select_projection_narrows_returned_attributes(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    setup_events_logs(&mut cluster);
    let node = non_leader(&cluster, "events");

    let (status, resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"SELECT pk, region FROM events WHERE pk = ? AND sk = ?",
            "Parameters":[{"S":"p1"},{"N":"0"}],"ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {resp}");
    let items = resp["Items"].as_array().expect("Items");
    assert_eq!(items.len(), 1, "seed={seed}: {resp}");
    let item = items[0].as_object().expect("item object");
    assert!(item.contains_key("pk"), "seed={seed}: {item:?}");
    assert!(item.contains_key("region"), "seed={seed}: {item:?}");
    assert!(!item.contains_key("sk"), "seed={seed}: {item:?}");
    assert!(!item.contains_key("cat"), "seed={seed}: {item:?}");
}

#[test]
fn select_projection_narrows_returned_attributes() {
    run_select_projection_narrows_returned_attributes(env_seed(0xC06F_6005));
}

#[test]
fn select_projection_narrows_returned_attributes_over_seeds() {
    for i in 0..5 {
        run_select_projection_narrows_returned_attributes(0xC06F_7050 + i);
    }
}

// ---------------------------------------------------------------------------
// (6) select_from_table_dot_index_queries_the_gsi
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_partiql.rs::select_from_table_dot_index_queries_the_gsi`
/// — `FROM "table"."index"` queries the named GSI; materialized here via
/// [`drain_events_gsi`] instead of the real test's own `await_gsi_select`
/// poll (deterministic, no background drain loop to wait on).
fn run_select_from_table_dot_index_queries_the_gsi(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    setup_events_logs(&mut cluster);
    drain_events_gsi(&mut cluster);
    let node = non_leader(&cluster, "events");

    let (status, resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"SELECT * FROM \"events\".\"by-cat\" WHERE cat = ?",
            "Parameters":[{"S":"X"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {resp}");
    assert_eq!(item_sks(&resp).len(), 6, "seed={seed}: {resp}");
}

#[test]
fn select_from_table_dot_index_queries_the_gsi() {
    run_select_from_table_dot_index_queries_the_gsi(env_seed(0xC06F_6006));
}

#[test]
fn select_from_table_dot_index_queries_the_gsi_over_seeds() {
    for i in 0..5 {
        run_select_from_table_dot_index_queries_the_gsi(0xC06F_7060 + i);
    }
}

// ---------------------------------------------------------------------------
// (7) gsi_projected_attribute_updated_via_partiql_is_visible_through_index_query
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_partiql.rs::gsi_projected_attribute_updated_via_partiql_
/// is_visible_through_index_query` — a GSI-projected attribute (`cat`)
/// updated via PartiQL `UPDATE` is visible through a `SELECT` on the index,
/// once re-drained.
fn run_gsi_projected_attribute_updated_via_partiql_is_visible_through_index_query(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    setup_events_logs(&mut cluster);
    let node = non_leader(&cluster, "events");

    let (status, resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"UPDATE events SET cat = ? WHERE pk = ? AND sk = ?",
            "Parameters":[{"S":"Y"},{"S":"p1"},{"N":"2"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {resp}");

    drain_events_gsi(&mut cluster);

    let (status, resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"SELECT * FROM \"events\".\"by-cat\" WHERE cat = ?",
            "Parameters":[{"S":"Y"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {resp}");
    assert_eq!(item_sks(&resp), vec!["2"], "seed={seed}: {resp}");
}

#[test]
fn gsi_projected_attribute_updated_via_partiql_is_visible_through_index_query() {
    run_gsi_projected_attribute_updated_via_partiql_is_visible_through_index_query(env_seed(
        0xC06F_6007,
    ));
}

#[test]
fn gsi_projected_attribute_updated_via_partiql_is_visible_through_index_query_over_seeds() {
    for i in 0..5 {
        run_gsi_projected_attribute_updated_via_partiql_is_visible_through_index_query(
            0xC06F_7070 + i,
        );
    }
}

// ---------------------------------------------------------------------------
// (8) order_by_desc_matches_scan_index_forward_false
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_partiql.rs::order_by_desc_matches_scan_index_forward_
/// false`.
fn run_order_by_desc_matches_scan_index_forward_false(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    setup_events_logs(&mut cluster);
    let node = non_leader(&cluster, "events");

    let (status, resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"SELECT * FROM events WHERE pk = ? ORDER BY sk DESC",
            "Parameters":[{"S":"p1"}],"ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {resp}");
    assert_eq!(
        item_sks(&resp),
        vec!["10", "4.5", "2", "0", "-1.5", "-5"],
        "seed={seed}"
    );
}

#[test]
fn order_by_desc_matches_scan_index_forward_false() {
    run_order_by_desc_matches_scan_index_forward_false(env_seed(0xC06F_6008));
}

#[test]
fn order_by_desc_matches_scan_index_forward_false_over_seeds() {
    for i in 0..5 {
        run_order_by_desc_matches_scan_index_forward_false(0xC06F_7080 + i);
    }
}

// ---------------------------------------------------------------------------
// (9) pagination_next_token_matches_query_last_evaluated_key_walk
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_partiql.rs::pagination_next_token_matches_query_last_
/// evaluated_key_walk` — `Limit`+`NextToken` pagination walks the exact same
/// item sequence a `Query`'s own `Limit`/`ExclusiveStartKey` walk gives.
fn run_pagination_next_token_matches_query_last_evaluated_key_walk(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    setup_events_logs(&mut cluster);
    let node = non_leader(&cluster, "events");

    let statement = "SELECT * FROM events WHERE pk = ?";
    let mut select_sks = Vec::new();
    let mut next_token: Option<String> = None;
    for _ in 0..10 {
        let token_field = match &next_token {
            Some(t) => format!(",\"NextToken\":{}", serde_json::to_string(t).unwrap()),
            None => String::new(),
        };
        let body = format!(
            r#"{{"Statement":"{statement}","Parameters":[{{"S":"p1"}}],
                "ConsistentRead":true,"Limit":2{token_field}}}"#
        );
        let (status, resp) = execute_statement(&mut cluster, node, &body);
        assert_eq!(status, 200, "seed={seed}: {resp}");
        select_sks.extend(item_sks(&resp));
        match resp.get("NextToken").and_then(serde_json::Value::as_str) {
            Some(t) => next_token = Some(t.to_string()),
            None => break,
        }
    }

    let mut query_sks = Vec::new();
    let mut cursor: Option<serde_json::Value> = None;
    for _ in 0..10 {
        let esk_field = match &cursor {
            Some(c) => format!(",\"ExclusiveStartKey\":{c}"),
            None => String::new(),
        };
        let body = format!(
            r#"{{"TableName":"events","KeyConditionExpression":"pk = :p",
                "ExpressionAttributeValues":{{":p":{{"S":"p1"}}}},
                "ConsistentRead":true,"Limit":2{esk_field}}}"#
        );
        let (status, resp) = query(&mut cluster, node, &body);
        assert_eq!(status, 200, "seed={seed}: {resp}");
        query_sks.extend(item_sks(&resp));
        match resp.get("LastEvaluatedKey") {
            Some(k) if !k.is_null() => cursor = Some(k.clone()),
            _ => break,
        }
    }

    assert_eq!(select_sks, query_sks, "seed={seed}");
    assert_eq!(
        select_sks,
        vec!["-5", "-1.5", "0", "2", "4.5", "10"],
        "seed={seed}"
    );
}

#[test]
fn pagination_next_token_matches_query_last_evaluated_key_walk() {
    run_pagination_next_token_matches_query_last_evaluated_key_walk(env_seed(0xC06F_6009));
}

#[test]
fn pagination_next_token_matches_query_last_evaluated_key_walk_over_seeds() {
    for i in 0..5 {
        run_pagination_next_token_matches_query_last_evaluated_key_walk(0xC06F_7090 + i);
    }
}

// ---------------------------------------------------------------------------
// (10) next_token_rejected_when_replayed_against_a_different_statement
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_partiql.rs::next_token_rejected_when_replayed_against_a_
/// different_statement`.
fn run_next_token_rejected_when_replayed_against_a_different_statement(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    setup_events_logs(&mut cluster);
    let node = non_leader(&cluster, "events");

    let (status, first) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"SELECT * FROM events WHERE pk = ?",
            "Parameters":[{"S":"p1"}],"ConsistentRead":true,"Limit":2}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {first}");
    let token = first["NextToken"]
        .as_str()
        .unwrap_or_else(|| panic!("seed={seed}: first page must carry a NextToken: {first}"));

    let body = format!(
        r#"{{"Statement":"SELECT * FROM events WHERE pk = ? AND sk = ?",
            "Parameters":[{{"S":"p1"}},{{"N":"0"}}],"ConsistentRead":true,
            "NextToken":{}}}"#,
        serde_json::to_string(token).unwrap()
    );
    let (status, resp) = execute_statement(&mut cluster, node, &body);
    assert_eq!(status, 400, "seed={seed}: {resp}");
    assert_eq!(
        resp["__type"], "com.amazonaws.dynamodb.v20120810#ValidationException",
        "seed={seed}: {resp}"
    );
    assert!(
        resp["message"].as_str().unwrap().contains("does not match"),
        "seed={seed}: {resp}"
    );
}

#[test]
fn next_token_rejected_when_replayed_against_a_different_statement() {
    run_next_token_rejected_when_replayed_against_a_different_statement(env_seed(0xC06F_600A));
}

#[test]
fn next_token_rejected_when_replayed_against_a_different_statement_over_seeds() {
    for i in 0..5 {
        run_next_token_rejected_when_replayed_against_a_different_statement(0xC06F_70A0 + i);
    }
}

// ---------------------------------------------------------------------------
// (11) insert_on_conflict_do_nothing_swallows_duplicate
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_partiql.rs::insert_on_conflict_do_nothing_swallows_
/// duplicate`.
fn run_insert_on_conflict_do_nothing_swallows_duplicate(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    setup_events_logs(&mut cluster);
    let node = non_leader(&cluster, "events");

    let (status, _) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"INSERT INTO events VALUE {'pk': ?, 'sk': ?, 'region': ?}",
            "Parameters":[{"S":"p4"},{"N":"1"},{"S":"eu"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}");

    let (status, resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"INSERT INTO events VALUE {'pk': ?, 'sk': ?, 'region': ?} ON CONFLICT DO NOTHING",
            "Parameters":[{"S":"p4"},{"N":"1"},{"S":"us"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {resp}");
    assert!(
        resp["Items"].as_array().unwrap().is_empty(),
        "seed={seed}: {resp}"
    );

    let (status, sel) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"SELECT region FROM events WHERE pk = ? AND sk = ?",
            "Parameters":[{"S":"p4"},{"N":"1"}],"ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {sel}");
    assert_eq!(sel["Items"][0]["region"]["S"], "eu", "seed={seed}: {sel}");
}

#[test]
fn insert_on_conflict_do_nothing_swallows_duplicate() {
    run_insert_on_conflict_do_nothing_swallows_duplicate(env_seed(0xC06F_600B));
}

#[test]
fn insert_on_conflict_do_nothing_swallows_duplicate_over_seeds() {
    for i in 0..5 {
        run_insert_on_conflict_do_nothing_swallows_duplicate(0xC06F_70B0 + i);
    }
}

// ---------------------------------------------------------------------------
// (12) duplicate_insert_gives_duplicate_item_exception_and_leaves_item_unchanged
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_partiql.rs::duplicate_insert_gives_duplicate_item_
/// exception_and_leaves_item_unchanged`.
fn run_duplicate_insert_gives_duplicate_item_exception_and_leaves_item_unchanged(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    setup_events_logs(&mut cluster);
    let node = non_leader(&cluster, "events");

    let (status, resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"INSERT INTO events VALUE {'pk': ?, 'sk': ?, 'region': ?}",
            "Parameters":[{"S":"p3"},{"N":"1"},{"S":"eu"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {resp}");

    let (status, dup) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"INSERT INTO events VALUE {'pk': ?, 'sk': ?, 'region': ?}",
            "Parameters":[{"S":"p3"},{"N":"1"},{"S":"us"}]}"#,
    );
    assert_eq!(status, 400, "seed={seed}: {dup}");
    assert_eq!(
        dup["__type"], "com.amazonaws.dynamodb.v20120810#DuplicateItemException",
        "seed={seed}: {dup}"
    );

    let (status, sel) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"SELECT region FROM events WHERE pk = ? AND sk = ?",
            "Parameters":[{"S":"p3"},{"N":"1"}],"ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {sel}");
    let items = sel["Items"].as_array().expect("Items");
    assert_eq!(items.len(), 1, "seed={seed}: {sel}");
    assert_eq!(
        items[0]["region"]["S"], "eu",
        "seed={seed}: the original item's value must survive the rejected duplicate INSERT: {sel}"
    );
}

#[test]
fn duplicate_insert_gives_duplicate_item_exception_and_leaves_item_unchanged() {
    run_duplicate_insert_gives_duplicate_item_exception_and_leaves_item_unchanged(env_seed(
        0xC06F_600C,
    ));
}

#[test]
fn duplicate_insert_gives_duplicate_item_exception_and_leaves_item_unchanged_over_seeds() {
    for i in 0..5 {
        run_duplicate_insert_gives_duplicate_item_exception_and_leaves_item_unchanged(
            0xC06F_70C0 + i,
        );
    }
}

// ---------------------------------------------------------------------------
// (13) update_set_on_existing_item_with_returning_all_new
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_partiql.rs::update_set_on_existing_item_with_returning_
/// all_new`.
fn run_update_set_on_existing_item_with_returning_all_new(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    setup_events_logs(&mut cluster);
    let node = non_leader(&cluster, "events");

    let (status, resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"UPDATE events SET region = ? WHERE pk = ? AND sk = ? RETURNING ALL NEW *",
            "Parameters":[{"S":"apac"},{"S":"p1"},{"N":"0"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {resp}");
    let items = resp["Items"].as_array().expect("Items");
    assert_eq!(items.len(), 1, "seed={seed}: {resp}");
    assert_eq!(items[0]["region"]["S"], "apac", "seed={seed}: {resp}");
    assert_eq!(items[0]["pk"]["S"], "p1", "seed={seed}: {resp}");
}

#[test]
fn update_set_on_existing_item_with_returning_all_new() {
    run_update_set_on_existing_item_with_returning_all_new(env_seed(0xC06F_600D));
}

#[test]
fn update_set_on_existing_item_with_returning_all_new_over_seeds() {
    for i in 0..5 {
        run_update_set_on_existing_item_with_returning_all_new(0xC06F_70D0 + i);
    }
}

// ---------------------------------------------------------------------------
// (14) update_of_missing_item_fails
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_partiql.rs::update_of_missing_item_fails` — `UPDATE` of a
/// key that doesn't exist fails the implicit `attribute_exists(pk)`
/// condition.
fn run_update_of_missing_item_fails(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    setup_events_logs(&mut cluster);
    let node = non_leader(&cluster, "events");

    let (status, resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"UPDATE events SET region = ? WHERE pk = ? AND sk = ?",
            "Parameters":[{"S":"apac"},{"S":"nope"},{"N":"999"}]}"#,
    );
    assert_eq!(status, 400, "seed={seed}: {resp}");
    assert_eq!(
        resp["__type"], "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException",
        "seed={seed}: {resp}"
    );
}

#[test]
fn update_of_missing_item_fails() {
    run_update_of_missing_item_fails(env_seed(0xC06F_600E));
}

#[test]
fn update_of_missing_item_fails_over_seeds() {
    for i in 0..5 {
        run_update_of_missing_item_fails(0xC06F_70E0 + i);
    }
}

// ---------------------------------------------------------------------------
// (15) update_with_non_key_where_term_as_condition_met_and_unmet
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_partiql.rs::update_with_non_key_where_term_as_condition_
/// met_and_unmet` — a non-key `WHERE` term becomes the `UpdateItem`'s own
/// `ConditionExpression`, both the met and unmet case.
fn run_update_with_non_key_where_term_as_condition_met_and_unmet(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    setup_events_logs(&mut cluster);
    let node = non_leader(&cluster, "events");

    // Met: sk=4.5's region is "us" per the fixture.
    let (status, resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"UPDATE events SET region = ? WHERE pk = ? AND sk = ? AND region = ? RETURNING ALL OLD *",
            "Parameters":[{"S":"apac"},{"S":"p1"},{"N":"4.5"},{"S":"us"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {resp}");
    let items = resp["Items"].as_array().expect("Items");
    assert_eq!(items.len(), 1, "seed={seed}: {resp}");
    assert_eq!(items[0]["region"]["S"], "us", "seed={seed}: {resp}");

    // Unmet: region is now "apac", not "us" any more.
    let (status, resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"UPDATE events SET region = ? WHERE pk = ? AND sk = ? AND region = ?",
            "Parameters":[{"S":"eu"},{"S":"p1"},{"N":"4.5"},{"S":"us"}]}"#,
    );
    assert_eq!(status, 400, "seed={seed}: {resp}");
    assert_eq!(
        resp["__type"], "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException",
        "seed={seed}: {resp}"
    );
}

#[test]
fn update_with_non_key_where_term_as_condition_met_and_unmet() {
    run_update_with_non_key_where_term_as_condition_met_and_unmet(env_seed(0xC06F_600F));
}

#[test]
fn update_with_non_key_where_term_as_condition_met_and_unmet_over_seeds() {
    for i in 0..5 {
        run_update_with_non_key_where_term_as_condition_met_and_unmet(0xC06F_70F0 + i);
    }
}

// ---------------------------------------------------------------------------
// (16) update_where_missing_partition_key_is_a_validation_exception
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_partiql.rs::update_where_missing_partition_key_is_a_
/// validation_exception` — a `WHERE` missing the partition key entirely (an
/// `UPDATE`/`DELETE` can never widen into a scan-and-mutate).
fn run_update_where_missing_partition_key_is_a_validation_exception(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    setup_events_logs(&mut cluster);
    let node = non_leader(&cluster, "events");

    let (status, resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"UPDATE events SET region = ? WHERE region = ?",
            "Parameters":[{"S":"apac"},{"S":"us"}]}"#,
    );
    assert_eq!(status, 400, "seed={seed}: {resp}");
    assert_eq!(
        resp["__type"], "com.amazonaws.dynamodb.v20120810#ValidationException",
        "seed={seed}: {resp}"
    );
    assert!(
        resp["message"].as_str().unwrap().contains("partition key"),
        "seed={seed}: {resp}"
    );
}

#[test]
fn update_where_missing_partition_key_is_a_validation_exception() {
    run_update_where_missing_partition_key_is_a_validation_exception(env_seed(0xC06F_6010));
}

#[test]
fn update_where_missing_partition_key_is_a_validation_exception_over_seeds() {
    for i in 0..5 {
        run_update_where_missing_partition_key_is_a_validation_exception(0xC06F_7100 + i);
    }
}

// ---------------------------------------------------------------------------
// (17) delete_where_missing_sort_key_is_a_validation_exception
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_partiql.rs::delete_where_missing_sort_key_is_a_
/// validation_exception` — a missing sort-key term on a composite-key table
/// is rejected rather than silently narrowed to a partial-key operation.
fn run_delete_where_missing_sort_key_is_a_validation_exception(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    setup_events_logs(&mut cluster);
    let node = non_leader(&cluster, "events");

    let (status, resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"DELETE FROM events WHERE pk = ?","Parameters":[{"S":"p1"}]}"#,
    );
    assert_eq!(status, 400, "seed={seed}: {resp}");
    assert_eq!(
        resp["__type"], "com.amazonaws.dynamodb.v20120810#ValidationException",
        "seed={seed}: {resp}"
    );
    assert!(
        resp["message"].as_str().unwrap().contains("sort key"),
        "seed={seed}: {resp}"
    );
}

#[test]
fn delete_where_missing_sort_key_is_a_validation_exception() {
    run_delete_where_missing_sort_key_is_a_validation_exception(env_seed(0xC06F_6011));
}

#[test]
fn delete_where_missing_sort_key_is_a_validation_exception_over_seeds() {
    for i in 0..5 {
        run_delete_where_missing_sort_key_is_a_validation_exception(0xC06F_7110 + i);
    }
}

// ---------------------------------------------------------------------------
// (18) unknown_table_is_resource_not_found
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_partiql.rs::unknown_table_is_resource_not_found` — a
/// statement naming a table that doesn't exist is `ResourceNotFoundException`
/// through the same catalog check every other operation uses.
fn run_unknown_table_is_resource_not_found(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    setup_events_logs(&mut cluster);
    let node = non_leader(&cluster, "events");

    let (status, resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"SELECT * FROM nope WHERE pk = ?","Parameters":[{"S":"p1"}]}"#,
    );
    assert_eq!(status, 400, "seed={seed}: {resp}");
    assert_eq!(
        resp["__type"], "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException",
        "seed={seed}: {resp}"
    );
}

#[test]
fn unknown_table_is_resource_not_found() {
    run_unknown_table_is_resource_not_found(env_seed(0xC06F_6012));
}

#[test]
fn unknown_table_is_resource_not_found_over_seeds() {
    for i in 0..5 {
        run_unknown_table_is_resource_not_found(0xC06F_7120 + i);
    }
}

// ---------------------------------------------------------------------------
// (19) malformed_statement_is_a_validation_exception
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_partiql.rs::malformed_statement_is_a_validation_
/// exception` — a malformed statement is a `ValidationException`, not a 500
/// or a panic.
fn run_malformed_statement_is_a_validation_exception(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    setup_events_logs(&mut cluster);
    let node = non_leader(&cluster, "events");

    let (status, resp) = execute_statement(&mut cluster, node, r#"{"Statement":"NOT EVEN SQL"}"#);
    assert_eq!(status, 400, "seed={seed}: {resp}");
    assert_eq!(
        resp["__type"], "com.amazonaws.dynamodb.v20120810#ValidationException",
        "seed={seed}: {resp}"
    );
}

#[test]
fn malformed_statement_is_a_validation_exception() {
    run_malformed_statement_is_a_validation_exception(env_seed(0xC06F_6013));
}

#[test]
fn malformed_statement_is_a_validation_exception_over_seeds() {
    for i in 0..5 {
        run_malformed_statement_is_a_validation_exception(0xC06F_7130 + i);
    }
}

// ---------------------------------------------------------------------------
// (20) literal_value_in_where_is_rejected
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_partiql.rs::literal_value_in_where_is_rejected` — ADR
/// 0071 §2's placeholder-only discipline, enforced end to end.
fn run_literal_value_in_where_is_rejected(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    setup_events_logs(&mut cluster);
    let node = non_leader(&cluster, "events");

    let (status, resp) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"SELECT * FROM events WHERE pk = 'p1'"}"#,
    );
    assert_eq!(status, 400, "seed={seed}: {resp}");
    assert_eq!(
        resp["__type"], "com.amazonaws.dynamodb.v20120810#ValidationException",
        "seed={seed}: {resp}"
    );
    assert!(
        resp["message"].as_str().unwrap().contains("literal values"),
        "seed={seed}: {resp}"
    );
}

#[test]
fn literal_value_in_where_is_rejected() {
    run_literal_value_in_where_is_rejected(env_seed(0xC06F_6014));
}

#[test]
fn literal_value_in_where_is_rejected_over_seeds() {
    for i in 0..5 {
        run_literal_value_in_where_is_rejected(0xC06F_7140 + i);
    }
}

// ---------------------------------------------------------------------------
// (21) execute_transaction_write_commits_atomically_across_two_tables
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_execute_transaction.rs::execute_transaction_write_
/// commits_atomically_across_two_tables` — a fresh sibling matching the real
/// test's own name/shape, kept distinct from PR 5's own similarly-shaped but
/// differently-named `execute_transaction_commits_across_two_tables` smoke
/// (both are kept; this PR neither deletes nor renames PR 5's scenarios).
fn run_execute_transaction_write_commits_atomically_across_two_tables(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, "xact_a");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(xact_a) failed: {body}"
    );
    let (status, body) = create_table(&mut cluster, "xact_b");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(xact_b) failed: {body}"
    );

    let node = non_leader(&cluster, "xact_a");

    let (status, resp) = execute_transaction(
        &mut cluster,
        node,
        r#"{"TransactStatements":[
            {"Statement":"INSERT INTO xact_a VALUE {'pk': ?, 'v': ?}",
             "Parameters":[{"S":"1"},{"S":"lo"}]},
            {"Statement":"INSERT INTO xact_b VALUE {'pk': ?, 'v': ?}",
             "Parameters":[{"S":"1"},{"S":"hi"}]}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {resp}");
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
        r#"{"Statement":"SELECT * FROM xact_a WHERE pk = ?",
            "Parameters":[{"S":"1"}],"ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {sel}");
    assert_eq!(sel["Items"][0]["v"]["S"], "lo", "seed={seed}: {sel}");
    let (status, sel) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"SELECT * FROM xact_b WHERE pk = ?",
            "Parameters":[{"S":"1"}],"ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {sel}");
    assert_eq!(sel["Items"][0]["v"]["S"], "hi", "seed={seed}: {sel}");
}

#[test]
fn execute_transaction_write_commits_atomically_across_two_tables() {
    run_execute_transaction_write_commits_atomically_across_two_tables(env_seed(0xC06F_6015));
}

#[test]
fn execute_transaction_write_commits_atomically_across_two_tables_over_seeds() {
    for i in 0..5 {
        run_execute_transaction_write_commits_atomically_across_two_tables(0xC06F_7150 + i);
    }
}

// ---------------------------------------------------------------------------
// (22) execute_transaction_write_cancels_whole_on_duplicate_insert
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_execute_transaction.rs::execute_transaction_write_
/// cancels_whole_on_duplicate_insert`.
fn run_execute_transaction_write_cancels_whole_on_duplicate_insert(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, "xact_c");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(xact_c) failed: {body}"
    );
    let node = non_leader(&cluster, "xact_c");

    // Seed an existing item so the second statement's INSERT fails its
    // implicit attribute_not_exists(pk) condition.
    let (status, resp) = execute_transaction(
        &mut cluster,
        node,
        r#"{"TransactStatements":[
            {"Statement":"INSERT INTO xact_c VALUE {'pk': ?}","Parameters":[{"S":"2"}]}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: seed txn failed: {resp}");

    // [0] a fresh INSERT that would succeed in isolation, [1] a duplicate
    // INSERT at the already-seeded key — must cancel BOTH.
    let (status, resp) = execute_transaction(
        &mut cluster,
        node,
        r#"{"TransactStatements":[
            {"Statement":"INSERT INTO xact_c VALUE {'pk': ?}","Parameters":[{"S":"should-not-land"}]},
            {"Statement":"INSERT INTO xact_c VALUE {'pk': ?}","Parameters":[{"S":"2"}]}]}"#,
    );
    assert_eq!(status, 400, "seed={seed}: {resp}");
    let reasons = cancellation_reasons(&resp);
    assert_eq!(reasons.len(), 2, "seed={seed}: {resp}");
    assert_eq!(reasons[0]["Code"], "None");
    assert_eq!(reasons[1]["Code"], "ConditionalCheckFailed");

    let (status, sel) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"SELECT * FROM xact_c WHERE pk = ?",
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
fn execute_transaction_write_cancels_whole_on_duplicate_insert() {
    run_execute_transaction_write_cancels_whole_on_duplicate_insert(env_seed(0xC06F_6016));
}

#[test]
fn execute_transaction_write_cancels_whole_on_duplicate_insert_over_seeds() {
    for i in 0..5 {
        run_execute_transaction_write_cancels_whole_on_duplicate_insert(0xC06F_7160 + i);
    }
}

// ---------------------------------------------------------------------------
// (23) execute_transaction_rejects_zero_and_too_many_statements
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_execute_transaction.rs::execute_transaction_rejects_
/// zero_and_too_many_statements`.
fn run_execute_transaction_rejects_zero_and_too_many_statements(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, "xact_f");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(xact_f) failed: {body}"
    );
    let node = non_leader(&cluster, "xact_f");

    let (status, resp) = execute_transaction(&mut cluster, node, r#"{"TransactStatements":[]}"#);
    assert_eq!(status, 400, "seed={seed}: {resp}");
    assert_eq!(
        resp["__type"], "com.amazonaws.dynamodb.v20120810#ValidationException",
        "seed={seed}: {resp}"
    );

    let statements: Vec<String> = (0..26)
        .map(|i| {
            format!(
                r#"{{"Statement":"SELECT * FROM xact_f WHERE pk = ?","Parameters":[{{"S":"i{i}"}}]}}"#
            )
        })
        .collect();
    let body = format!(r#"{{"TransactStatements":[{}]}}"#, statements.join(","));
    let (status, resp) = execute_transaction(&mut cluster, node, &body);
    assert_eq!(status, 400, "seed={seed}: {resp}");
    assert_eq!(
        resp["__type"], "com.amazonaws.dynamodb.v20120810#ValidationException",
        "seed={seed}: {resp}"
    );
}

#[test]
fn execute_transaction_rejects_zero_and_too_many_statements() {
    run_execute_transaction_rejects_zero_and_too_many_statements(env_seed(0xC06F_6017));
}

#[test]
fn execute_transaction_rejects_zero_and_too_many_statements_over_seeds() {
    for i in 0..5 {
        run_execute_transaction_rejects_zero_and_too_many_statements(0xC06F_7170 + i);
    }
}

// ---------------------------------------------------------------------------
// (24) execute_transaction_mixed_select_and_insert_is_validation_exception
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_execute_transaction.rs::execute_transaction_mixed_
/// select_and_insert_is_validation_exception`.
fn run_execute_transaction_mixed_select_and_insert_is_validation_exception(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, "xact_e");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(xact_e) failed: {body}"
    );
    let node = non_leader(&cluster, "xact_e");

    let (status, resp) = execute_transaction(
        &mut cluster,
        node,
        r#"{"TransactStatements":[
            {"Statement":"SELECT * FROM xact_e WHERE pk = ?","Parameters":[{"S":"a"}]},
            {"Statement":"INSERT INTO xact_e VALUE {'pk': ?}","Parameters":[{"S":"b"}]}]}"#,
    );
    assert_eq!(status, 400, "seed={seed}: {resp}");
    assert_eq!(
        resp["__type"], "com.amazonaws.dynamodb.v20120810#ValidationException",
        "seed={seed}: {resp}"
    );
}

#[test]
fn execute_transaction_mixed_select_and_insert_is_validation_exception() {
    run_execute_transaction_mixed_select_and_insert_is_validation_exception(env_seed(
        0xC06F_6018,
    ));
}

#[test]
fn execute_transaction_mixed_select_and_insert_is_validation_exception_over_seeds() {
    for i in 0..5 {
        run_execute_transaction_mixed_select_and_insert_is_validation_exception(0xC06F_7180 + i);
    }
}

// ---------------------------------------------------------------------------
// (25) execute_transaction_client_request_token_replay_is_cached
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_execute_transaction.rs::execute_transaction_client_
/// request_token_replay_is_cached`.
fn run_execute_transaction_client_request_token_replay_is_cached(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, "xact_g");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(xact_g) failed: {body}"
    );
    let node = non_leader(&cluster, "xact_g");

    let body = r#"{"ClientRequestToken":"tok-exec-txn-1",
        "TransactStatements":[
            {"Statement":"INSERT INTO xact_g VALUE {'pk': ?, 'v': ?}",
             "Parameters":[{"S":"1"},{"S":"first"}]}]}"#;

    let (status, resp) = execute_transaction(&mut cluster, node, body);
    assert_eq!(status, 200, "seed={seed}: first attempt: {resp}");

    // A retry with the identical token AND identical statements must be a
    // cached no-op — 200 again, and the original value must survive
    // (proving no second execution ran).
    let (status, resp) = execute_transaction(&mut cluster, node, body);
    assert_eq!(status, 200, "seed={seed}: replay: {resp}");

    let (status, sel) = execute_statement(
        &mut cluster,
        node,
        r#"{"Statement":"SELECT * FROM xact_g WHERE pk = ?",
            "Parameters":[{"S":"1"}],"ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {sel}");
    assert_eq!(sel["Items"][0]["v"]["S"], "first", "seed={seed}: {sel}");
}

#[test]
fn execute_transaction_client_request_token_replay_is_cached() {
    run_execute_transaction_client_request_token_replay_is_cached(env_seed(0xC06F_6019));
}

#[test]
fn execute_transaction_client_request_token_replay_is_cached_over_seeds() {
    for i in 0..5 {
        run_execute_transaction_client_request_token_replay_is_cached(0xC06F_7190 + i);
    }
}

// ---------------------------------------------------------------------------
// (26) execute_transaction_all_select_returns_items_and_misses_in_order
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_execute_transaction.rs::execute_transaction_all_select_
/// returns_items_and_misses_in_order`.
fn run_execute_transaction_all_select_returns_items_and_misses_in_order(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, "xact_d");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(xact_d) failed: {body}"
    );
    let node = non_leader(&cluster, "xact_d");

    let (status, resp) = cluster.dynamo(
        node,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"xact_d","Item":{"pk":{"S":"a"},"v":{"S":"va"}}}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {resp}");

    let (status, resp) = execute_transaction(
        &mut cluster,
        node,
        r#"{"TransactStatements":[
            {"Statement":"SELECT * FROM xact_d WHERE pk = ?","Parameters":[{"S":"a"}]},
            {"Statement":"SELECT * FROM xact_d WHERE pk = ?","Parameters":[{"S":"missing"}]}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {resp}");
    let responses = resp["Responses"].as_array().expect("Responses array");
    assert_eq!(responses.len(), 2, "seed={seed}: {responses:?}");
    assert_eq!(responses[0]["Item"]["v"]["S"], "va", "seed={seed}: {resp}");
    assert_eq!(
        responses[1].as_object().unwrap().len(),
        0,
        "seed={seed}: a miss must be an empty object entry: {resp}"
    );
}

#[test]
fn execute_transaction_all_select_returns_items_and_misses_in_order() {
    run_execute_transaction_all_select_returns_items_and_misses_in_order(env_seed(0xC06F_601A));
}

#[test]
fn execute_transaction_all_select_returns_items_and_misses_in_order_over_seeds() {
    for i in 0..5 {
        run_execute_transaction_all_select_returns_items_and_misses_in_order(0xC06F_71A0 + i);
    }
}

// ---------------------------------------------------------------------------
// (27) execute_transaction_over_a_follower_connected_node
// ---------------------------------------------------------------------------

/// Mirrors `dynamo_execute_transaction.rs::execute_transaction_over_a_
/// follower_connected_node` — the sim version issues from a follower by
/// construction: every scenario in this module already routes through a
/// non-leader replica, exercising the identical forwarding path the real
/// test's dedicated 2-node cluster sets up specifically to prove.
fn run_execute_transaction_over_a_follower_connected_node(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, "xact_h");
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable(xact_h) failed: {body}"
    );

    let node = non_leader(&cluster, "xact_h");
    let (status, resp) = execute_transaction(
        &mut cluster,
        node,
        r#"{"TransactStatements":[
            {"Statement":"INSERT INTO xact_h VALUE {'pk': ?, 'v': ?}",
             "Parameters":[{"S":"1"},{"S":"via-follower"}]}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {resp}");

    // Read back through a DIFFERENT node, confirming the write is visible
    // cluster-wide, not just locally cached on whichever node served it.
    let other = (0..cluster.node_count() as u64)
        .find(|&n| n != node)
        .expect("a 3-node cluster has another node");
    let (status, sel) = execute_statement(
        &mut cluster,
        other,
        r#"{"Statement":"SELECT * FROM xact_h WHERE pk = ?",
            "Parameters":[{"S":"1"}],"ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "seed={seed}: {sel}");
    assert_eq!(
        sel["Items"][0]["v"]["S"], "via-follower",
        "seed={seed}: {sel}"
    );
}

#[test]
fn execute_transaction_over_a_follower_connected_node() {
    run_execute_transaction_over_a_follower_connected_node(env_seed(0xC06F_601B));
}

#[test]
fn execute_transaction_over_a_follower_connected_node_over_seeds() {
    for i in 0..5 {
        run_execute_transaction_over_a_follower_connected_node(0xC06F_71B0 + i);
    }
}
