//! A first deterministic smoke over the DynamoDB wire edge driven against a
//! real [`SimCluster`] (ADR 0061 rung D2 PR 1) — requests decoded by
//! `animus_dynamo::wire::decode_request` and run through `dynamo::
//! dispatch_item_op`, the exact same generic core `dynamo::run_operation`'s
//! own item-op arms call in production (`dynamo.rs`'s own doc on that
//! function has the full "what moved, what stayed `ProdEnv`-only, why"
//! account). This module proves the wiring end to end for the eight
//! operations that core covers today — PutItem, GetItem, UpdateItem,
//! DeleteItem, Query (base-table), Scan (base-table), BatchGetItem,
//! BatchWriteItem — with a handful of hand-picked scenarios, not yet the
//! full nemesis corpus.
//!
//! **This is PR 1 of D2's own two-PR plan, not the corpus.** PR 2 adds:
//! GSI/LSI Query/Scan (needs `run_index_query`/`run_gsi_query`/
//! `run_lsi_query`/`run_index_scan`/`run_gsi_scan`/`run_lsi_scan`/
//! `paginated_kind_examine`/`paginated_kind_examine_one` made generic too —
//! deferred here purely to keep this PR's signature count reviewable, see
//! `dispatch_item_op`'s own doc for the exact count); `TransactWriteItems`/
//! `TransactGetItems` (blocked on proving `ClientCtx::propose_schema`'s
//! *relayed* path reaches a genuine multi-voter `SimEnv` control quorum for
//! the internal idempotency table's auto-provisioning — ADR 0061 rung D1's
//! own "what remains unexercised" note, never yet exercised by any `SimEnv`
//! fixture in this crate); `ExecuteStatement`/`BatchExecuteStatement`/
//! `ExecuteTransaction` (PartiQL — no new logic, but they lower onto
//! whichever of the above they need, so they need all of it generic
//! first); and, once those land, the real point of D2 — a `Recorder`/
//! `History` model over `SimClusterHandle::dynamo`, `check_cycles`/
//! `check_durability`/`check_convergence`, an `ANIMUS_DYNAMO_WIRE_SEEDS`
//! depth knob, and a `corpus-deep.yml` nightly tier, mirroring
//! `sim_cluster_corpus`'s own shape exactly.
//!
//! Declared `#[cfg(test)] mod sim_cluster_dynamo;` from `lib.rs`, a sibling
//! of `sim_cluster`/`sim_cluster_corpus`/`sim_cluster_throttle`, for the
//! identical "descendant of the crate root, `SimCluster`'s own `pub(crate)`
//! surface stays reachable with no visibility widened" reason those three
//! already document.

use std::time::Duration;

use super::sim_cluster::SimCluster;

/// Decode `body` as JSON and return the value at `field`, or `None` if the
/// body isn't an object or doesn't carry that field — a thin helper so each
/// scenario's own assertions read as "the response has X", not a manual
/// `serde_json::Value` walk repeated per test.
fn body_field(body: &str, field: &str) -> Option<serde_json::Value> {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()?
        .get(field)
        .cloned()
}

/// Poll [`SimCluster::dynamo`]'s `GetItem` response for `expected_contains`
/// (a raw substring of the encoded JSON body — matching every other
/// wire-level test file's own `body.contains(..)` idiom, e.g.
/// `crates/animusd/tests/dynamo_wire.rs`) up to a few converged-or-timeout
/// attempts, mirroring `sim_cluster.rs::tests::poll_until_get_eq`'s own
/// shape (that helper itself is private to that file's own `mod tests`, so
/// this is a from-scratch sibling rather than a shared one — see this
/// module's own doc for why it's a separate file at all).
fn poll_until_get_contains(
    cluster: &mut SimCluster,
    node: u64,
    get_body: &str,
    expected_contains: &str,
    settle: Duration,
) -> String {
    const ATTEMPTS: usize = 5;
    let seed = cluster.seed();
    let mut last = String::new();
    for _ in 0..ATTEMPTS {
        let (status, body) = cluster.dynamo(node, "DynamoDB_20120810.GetItem", get_body.as_bytes());
        if status == 200 && body.contains(expected_contains) {
            return body;
        }
        last = format!("status={status} body={body}");
        cluster.run_for(settle);
    }
    panic!(
        "node {node}'s own GetItem never converged to contain {expected_contains:?} within \
         {ATTEMPTS} attempts (last={last}, seed={seed})"
    );
}

/// PutItem → GetItem(`ConsistentRead: true`) through the wire on a 3-node
/// RF3 `SimCluster`, both issued from a **non-leader** node — proving the
/// generic `dispatch_item_op` path forwards over the real `SimRelayClient`
/// wire exactly like the direct `cp_kind_write_raw`/`cp_get` scenarios in
/// `sim_cluster.rs` already do, just reached through the DynamoDB JSON
/// decode this time.
#[test]
fn put_then_consistent_get_through_wire_from_a_non_leader_node() {
    run_put_then_consistent_get(0xD2C1_0001);
}

#[test]
fn put_then_consistent_get_through_wire_from_a_non_leader_node_seed2() {
    run_put_then_consistent_get(0xD2C1_0002);
}

/// Replay proof (repo convention): `ANIMUS_SEED=<seed> cargo test -p
/// animusd --lib replays_dynamo_wire_put_get_from_an_explicit_env_seed`.
#[test]
fn replays_dynamo_wire_put_get_from_an_explicit_env_seed() {
    let seed = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0xD2C1_0003);
    run_put_then_consistent_get(seed);
}

fn run_put_then_consistent_get(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    cluster.create_table("orders");
    let tablet = cluster.tablet_of("orders").expect("just created");
    let leader = cluster
        .leader_index_of(tablet)
        .expect("the fresh group elected a leader");
    let non_leader = (0..cluster.node_count() as u64)
        .find(|&n| n != leader)
        .expect("a 3-node cluster has a non-leader node");

    let (status, body) = cluster.dynamo(
        non_leader,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"orders","Item":{"pk":{"S":"cust-1"},"sk":{"S":"order-1"},
             "total":{"N":"42"}}}"#,
    );
    assert_eq!(
        status, 200,
        "PutItem via the wire must succeed (seed={seed}): {body}"
    );

    let get_body = r#"{"ConsistentRead":true,"TableName":"orders",
        "Key":{"pk":{"S":"cust-1"},"sk":{"S":"order-1"}}}"#;
    let body = poll_until_get_contains(
        &mut cluster,
        non_leader,
        get_body,
        r#""total":{"N":"42"}"#,
        Duration::from_secs(1),
    );
    assert!(
        body.contains(r#""pk":{"S":"cust-1"}"#),
        "GetItem response must echo the item back whole (seed={seed}): {body}"
    );
}

/// `UpdateItem` with a `ConditionExpression`, through the wire: a condition
/// that holds applies the update (`ADD total :inc`), and one that fails
/// returns `ConditionalCheckFailedException` without changing the item —
/// both through `dispatch_item_op`'s `cp_kind_write_item` evaluate-at-leader
/// path (ADR 0046 U3), never the unconditioned fast arm `PutItem`'s own
/// scenario above exercises.
#[test]
fn update_item_with_a_condition_through_wire() {
    run_update_item_with_condition(0xD2C2_0001);
}

#[test]
fn update_item_with_a_condition_through_wire_seed2() {
    run_update_item_with_condition(0xD2C2_0002);
}

fn run_update_item_with_condition(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    cluster.create_table("orders");
    let tablet = cluster.tablet_of("orders").expect("just created");
    let leader = cluster
        .leader_index_of(tablet)
        .expect("the fresh group elected a leader");

    let (status, body) = cluster.dynamo(
        leader,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"orders","Item":{"pk":{"S":"cust-2"},"sk":{"S":"order-1"},
             "total":{"N":"10"}}}"#,
    );
    assert_eq!(
        status, 200,
        "seed PutItem must succeed (seed={seed}): {body}"
    );

    // A condition that holds: `attribute_exists(pk)` on an item that exists.
    let (status, body) = cluster.dynamo(
        leader,
        "DynamoDB_20120810.UpdateItem",
        br#"{"TableName":"orders","Key":{"pk":{"S":"cust-2"},"sk":{"S":"order-1"}},
             "UpdateExpression":"ADD total :inc",
             "ConditionExpression":"attribute_exists(pk)",
             "ExpressionAttributeValues":{":inc":{"N":"5"}}}"#,
    );
    assert_eq!(
        status, 200,
        "UpdateItem with a satisfied condition must succeed (seed={seed}): {body}"
    );
    let get_body = r#"{"ConsistentRead":true,"TableName":"orders",
        "Key":{"pk":{"S":"cust-2"},"sk":{"S":"order-1"}}}"#;
    poll_until_get_contains(
        &mut cluster,
        leader,
        get_body,
        r#""total":{"N":"15"}"#,
        Duration::from_secs(1),
    );

    // A condition that fails: `attribute_not_exists(pk)` on the same,
    // now-present item — rejected, and the value from above is unchanged.
    let (status, body) = cluster.dynamo(
        leader,
        "DynamoDB_20120810.UpdateItem",
        br#"{"TableName":"orders","Key":{"pk":{"S":"cust-2"},"sk":{"S":"order-1"}},
             "UpdateExpression":"ADD total :inc",
             "ConditionExpression":"attribute_not_exists(pk)",
             "ExpressionAttributeValues":{":inc":{"N":"5"}}}"#,
    );
    assert_eq!(status, 400, "seed={seed}: {body}");
    assert!(
        body.contains("ConditionalCheckFailedException"),
        "seed={seed}: {body}"
    );
    poll_until_get_contains(
        &mut cluster,
        leader,
        get_body,
        r#""total":{"N":"15"}"#,
        Duration::from_secs(1),
    );
}

/// `Query` over a composite `(pk, sk)` table through the wire — several
/// items under one partition, one under another, a `KeyConditionExpression`
/// naming only the partition returns exactly the matching partition's rows
/// (proving `dispatch_item_op`'s base-table `Query` arm, which delegates to
/// the same generic `run_base_query` production's own non-index `Query`
/// uses).
#[test]
fn query_over_a_composite_key_table_through_wire() {
    run_query_composite_table(0xD2C3_0001);
}

#[test]
fn query_over_a_composite_key_table_through_wire_seed2() {
    run_query_composite_table(0xD2C3_0002);
}

fn run_query_composite_table(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    cluster.create_table("orders");
    let tablet = cluster.tablet_of("orders").expect("just created");
    let leader = cluster
        .leader_index_of(tablet)
        .expect("the fresh group elected a leader");

    for (pk, sk, total) in [
        ("cust-3", "order-1", 1),
        ("cust-3", "order-2", 2),
        ("cust-4", "order-1", 99),
    ] {
        let body = format!(
            r#"{{"TableName":"orders","Item":{{"pk":{{"S":"{pk}"}},"sk":{{"S":"{sk}"}},
                 "total":{{"N":"{total}"}}}}}}"#
        );
        let (status, resp) = cluster.dynamo(leader, "DynamoDB_20120810.PutItem", body.as_bytes());
        assert_eq!(
            status, 200,
            "seed PutItem must succeed (seed={seed}): {resp}"
        );
    }
    // Converged-or-timeout: let the writes settle before querying, mirroring
    // every other eventual-property poll in this crate.
    cluster.run_for(Duration::from_millis(200));

    let (status, body) = cluster.dynamo(
        leader,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"orders","ConsistentRead":true,
             "KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"cust-3"}}}"#,
    );
    assert_eq!(
        status, 200,
        "Query via the wire must succeed (seed={seed}): {body}"
    );
    let items = body_field(&body, "Items").unwrap_or_else(|| panic!("no Items field: {body}"));
    let items = items.as_array().expect("Items is an array");
    assert_eq!(
        items.len(),
        2,
        "Query must return exactly cust-3's two rows, not cust-4's (seed={seed}): {body}"
    );
    assert!(
        body.contains(r#""S":"cust-3"#) && !body.contains(r#""S":"cust-4"#),
        "seed={seed}: {body}"
    );
}

/// `BatchWriteItem` through the wire: several `PutRequest`s in one call,
/// each read back individually — proving `dispatch_item_op`'s
/// `BatchWriteItem` arm, which routes through the same `marker_batch_write`
/// single-Raft-entry-per-tablet path production uses (ADR 0049 §1/§4).
#[test]
fn batch_write_item_through_wire() {
    run_batch_write_item(0xD2C4_0001);
}

#[test]
fn batch_write_item_through_wire_seed2() {
    run_batch_write_item(0xD2C4_0002);
}

fn run_batch_write_item(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    cluster.create_table("orders");
    let tablet = cluster.tablet_of("orders").expect("just created");
    let leader = cluster
        .leader_index_of(tablet)
        .expect("the fresh group elected a leader");

    let body = br#"{"RequestItems":{"orders":[
        {"PutRequest":{"Item":{"pk":{"S":"cust-5"},"sk":{"S":"order-1"},"total":{"N":"1"}}}},
        {"PutRequest":{"Item":{"pk":{"S":"cust-5"},"sk":{"S":"order-2"},"total":{"N":"2"}}}}
    ]}}"#;
    let (status, resp) = cluster.dynamo(leader, "DynamoDB_20120810.BatchWriteItem", body);
    assert_eq!(
        status, 200,
        "BatchWriteItem via the wire must succeed (seed={seed}): {resp}"
    );

    for (sk, total) in [("order-1", "1"), ("order-2", "2")] {
        let get_body = format!(
            r#"{{"ConsistentRead":true,"TableName":"orders",
                "Key":{{"pk":{{"S":"cust-5"}},"sk":{{"S":"{sk}"}}}}}}"#
        );
        poll_until_get_contains(
            &mut cluster,
            leader,
            &get_body,
            &format!(r#""total":{{"N":"{total}"}}"#),
            Duration::from_secs(1),
        );
    }
}

/// One leader crash + restart, with a converged-or-timeout **wire** read
/// afterward — the fault-injection half of `sim_cluster.rs`'s own scenario
/// 3, reached through the DynamoDB wire this time rather than
/// `cp_kind_write_raw`/`cp_get` directly.
#[test]
fn leader_crash_restart_then_wire_read_converges() {
    run_leader_crash_restart(0xD2C5_0001);
}

#[test]
fn leader_crash_restart_then_wire_read_converges_seed2() {
    run_leader_crash_restart(0xD2C5_0002);
}

fn run_leader_crash_restart(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    cluster.create_table("orders");
    let tablet = cluster.tablet_of("orders").expect("just created");
    let leader = cluster
        .leader_index_of(tablet)
        .expect("the fresh group elected a leader");

    let (status, body) = cluster.dynamo(
        leader,
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"orders","Item":{"pk":{"S":"cust-6"},"sk":{"S":"order-1"},
             "total":{"N":"7"}}}"#,
    );
    assert_eq!(
        status, 200,
        "seed PutItem must succeed (seed={seed}): {body}"
    );

    cluster.crash(leader);
    // Hold the fault open across an election window before writing through
    // a survivor — mirrors `sim_cluster.rs`'s own scenario 3. Note: a
    // crashed (muted, not stopped) node's own `is_leader_local` read stays
    // frozen at whatever it last locally believed — nothing tells it
    // otherwise, since its inbox is cleared, not its internal Raft state —
    // so `leader_index_of` is not a safe way to find the *new* leader here.
    // Route through any survivor instead, exactly like `sim_cluster.rs`'s
    // own scenario 3: `dispatch_item_op`'s own `cp_kind_write_item` forwards
    // internally to whichever replica actually holds the post-election
    // leadership, via the real hint-chasing `forward_to_tablet_leader` loop.
    cluster.run_for(Duration::from_secs(2));
    let survivor = (0..cluster.node_count() as u64)
        .find(|&n| n != leader)
        .expect("a 3-node cluster has a survivor");

    let (status, body) = cluster.dynamo(
        survivor,
        "DynamoDB_20120810.UpdateItem",
        br#"{"TableName":"orders","Key":{"pk":{"S":"cust-6"},"sk":{"S":"order-1"}},
             "UpdateExpression":"ADD total :inc","ExpressionAttributeValues":{":inc":{"N":"3"}}}"#,
    );
    assert_eq!(
        status, 200,
        "UpdateItem through a survivor while the old leader is down must succeed (seed={seed}): {body}"
    );

    cluster.restart(leader);

    // Converged-or-timeout wire read from the whole cluster, including the
    // just-restarted former leader.
    for node in 0..cluster.node_count() as u64 {
        let get_body = r#"{"ConsistentRead":true,"TableName":"orders",
            "Key":{"pk":{"S":"cust-6"},"sk":{"S":"order-1"}}}"#;
        poll_until_get_contains(
            &mut cluster,
            node,
            get_body,
            r#""total":{"N":"10"}"#,
            Duration::from_secs(1),
        );
    }
}
