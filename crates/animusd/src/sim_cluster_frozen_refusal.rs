//! `SimCluster`-driven regression for issue #994: a retry budget exhausted
//! on a **transient** refusal (a split-cutover freeze,
//! `decide::FROZEN_REFUSAL`; an exhausted forward chase still citing a
//! transient last hop, `forwarding::FORWARD_BUDGET_EXHAUSTED`) must reach
//! the DynamoDB client as a retryable `503 ServiceUnavailable`, never a
//! terminal `500 InternalServerError` — see `dynamo.rs`'s `error_status`/
//! `map_throttleable_error` and `write_path.rs::cp_kind_write_item`'s own
//! terminal-return mapping for the fix itself.
//!
//! **Scenario A** (both mapping sites at once): a 3-node RF-3 cluster with
//! two tables — a **plain** table (the ADR 0049 fast-marker
//! `cp_kind_write_raw` path, `dynamo::fast_marker_write` →
//! `map_throttleable_error`) and a table with a declared **GSI** (the
//! evaluate-at-leader `cp_kind_write_item` path, `dynamo::
//! kind_write_item_at_leader` → `write_path::cp_kind_write_item`'s own
//! terminal mapping) — each frozen directly ([`SimCluster::freeze_tablet`],
//! a fixture-only stand-in for a real in-place split's own data-plane fork
//! reaching its latched-frozen state, without waiting out the fork/cutover
//! window itself). `PutItem` from both a non-leader (forwarded) and the
//! leader (local) node on each frozen table must return `503`/
//! `ServiceUnavailable` with a `"; retry"`-suffixed message; a
//! `ConsistentRead: true` `GetItem` on the same (frozen) table must still
//! succeed — reads are deliberately not gated (`decide::frozen_refusal`'s
//! own doc).
//!
//! **Scenario B**: a REAL in-place split, kicked off through `POST
//! /admin/tablet/split` and forked by this fixture's own always-on
//! `animus_cp_data::host::Reconciler` (no test-only freeze injection) —
//! deliberately never draining [`SimCluster::drive_inplace_split_cutover`]
//! while a `PutItem` is issued, so the fork's own genuine freeze outlasts
//! `CLIENT_TIMEOUT` and the call sees `503`/`ServiceUnavailable`; then the
//! cutover is driven to convergence and a retried `PutItem` succeeds,
//! served by whichever child now owns the key.
//!
//! Declared `#[cfg(test)] mod sim_cluster_frozen_refusal;` from `lib.rs`, a
//! sibling of `sim_cluster`/`sim_cluster_dynamo`/`sim_cluster_admin_actions`
//! for the identical "descendant of the crate root, `SimCluster`'s own
//! `pub(crate)` surface (here, the new `freeze_tablet`/`wait_for_fork_freeze`
//! helpers) stays reachable with no visibility widened" reason those
//! already document.

use std::time::Duration;

use animus_control::Metadata;
use animus_tablet::TabletId;

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// `table`'s (sole) tablet id and its current leader's node index — mirrors
/// `sim_cluster_index_ddl.rs::leader_of_table`'s own idiom, which every
/// wire-created table in this crate's own sibling modules already uses
/// (`SimClusterHandle::tablet_of` only tracks a **hand-hosted**
/// `create_table`/`create_table_with_replication` table's own bookkeeping —
/// see that method's own doc — so a wire-created table must be looked up
/// through the live replicated `Metadata` instead).
fn tablet_and_leader_of(cluster: &SimCluster, table: &str) -> (TabletId, u64) {
    let meta: Metadata = cluster.metadata(0);
    let (tablet, _) = meta
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("{table} has no tablet"));
    let tablet = *tablet;
    drop(meta);
    let leader = cluster
        .leader_index_of(tablet)
        .unwrap_or_else(|| panic!("{table}'s tablet has no leader"));
    (tablet, leader)
}

/// A node index in `0..cluster.node_count()` that is NOT `leader` — every
/// scenario here runs on a 3+-node cluster, so one always exists.
fn a_non_leader(cluster: &SimCluster, leader: u64) -> u64 {
    (0..cluster.node_count() as u64)
        .find(|&n| n != leader)
        .expect("a multi-node cluster has a non-leader node")
}

/// Assert `cluster.dynamo(node, target, body)` returns `503` with the
/// `ServiceUnavailable` `__type` and a `"; retry"`-suffixed message — the
/// one shape this whole regression exists to prove, checked identically at
/// every call site so a future fix that narrows the mapping (e.g. dropping
/// the `"; retry"` suffix through a relayed hop) fails loudly here.
fn assert_frozen_refusal_is_service_unavailable(
    cluster: &mut SimCluster,
    node: u64,
    target: &str,
    body: &[u8],
    seed: u64,
    what: &str,
) {
    assert_frozen_refusal_is_service_unavailable_inner(
        cluster, node, target, body, seed, what, false,
    );
}

/// [`assert_frozen_refusal_is_service_unavailable`]'s stricter sibling for
/// the directly-injected freeze (Scenario A): also requires the 503 body's
/// message to contain `decide::FROZEN_REFUSAL`'s own distinguishing text,
/// "tablet frozen for split cutover" — proving the retry-budget-exhausted
/// mapping (issue #994's own fix, `write_path.rs::cp_kind_write_item`'s
/// post-sleep deadline re-check) preserved the ORIGINAL, informative
/// refusal all the way through, rather than merely landing on *a* 503 for
/// *some* reason. Checked on both the leader (local) and non-leader
/// (forwarded) paths: the non-leader case only carries this text because
/// `decode_relayed_error` falls back to `internal(raw)` for an unmarked
/// (i.e. `InternalServerError`-coded) relayed message, which preserves the
/// leader's own `FROZEN_REFUSAL` string verbatim rather than replacing it
/// with a generic code-derived message.
fn assert_frozen_refusal_carries_the_frozen_text(
    cluster: &mut SimCluster,
    node: u64,
    target: &str,
    body: &[u8],
    seed: u64,
    what: &str,
) {
    assert_frozen_refusal_is_service_unavailable_inner(
        cluster, node, target, body, seed, what, true,
    );
}

fn assert_frozen_refusal_is_service_unavailable_inner(
    cluster: &mut SimCluster,
    node: u64,
    target: &str,
    body: &[u8],
    seed: u64,
    what: &str,
    require_frozen_text: bool,
) {
    let (status, resp_body) = cluster.dynamo(node, target, body);
    assert_eq!(
        status, 503,
        "seed={seed}: {what} on a frozen tablet must return 503, got {status}: {resp_body}"
    );
    assert!(
        resp_body.contains("ServiceUnavailable"),
        "seed={seed}: {what}'s 503 body must carry the ServiceUnavailable __type: {resp_body}"
    );
    assert!(
        resp_body.contains("; retry"),
        "seed={seed}: {what}'s 503 body must keep the house retryable-message suffix \
         so a caller's own retry loop (and the DynamoDB client) see it as transient: \
         {resp_body}"
    );
    if require_frozen_text {
        assert!(
            resp_body.contains("tablet frozen for split cutover"),
            "seed={seed}: {what}'s 503 body must preserve the original FROZEN_REFUSAL \
             text (issue #994's post-sleep deadline re-check must return the last real \
             refusal, not a generic budget-exhausted one), got: {resp_body}"
        );
    }
}

// ---------------------------------------------------------------------------
// Scenario A: two frozen tables (plain fast-marker path, GSI evaluate-at-
// leader path), each hit from a non-leader and the leader node.
// ---------------------------------------------------------------------------

fn run_frozen_tables_return_service_unavailable(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);

    // --- Table 1: plain (unindexed, unstreamed) -- the fast-marker path. ---
    let plain_table = "plain_orders";
    cluster.create_table(plain_table);
    let (plain_tablet, plain_leader) = {
        let tablet = cluster
            .tablet_of(plain_table)
            .expect("just created via create_table");
        let leader = cluster
            .leader_index_of(tablet)
            .expect("the fresh group elected a leader");
        (tablet, leader)
    };
    let plain_non_leader = a_non_leader(&cluster, plain_leader);

    let plain_put = br#"{"TableName":"plain_orders","Item":{"pk":{"S":"cust-1"},
        "sk":{"S":"order-1"},"total":{"N":"42"}}}"#;
    let (status, body) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", plain_put);
    assert_eq!(
        status, 200,
        "seed={seed}: seed PutItem on plain_orders (before freeze) must succeed: {body}"
    );

    // --- Table 2: a table with a declared GSI -- the evaluate-at-leader
    // (`cp_kind_write_item`) path.
    let gsi_table = "gsi_users";
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"gsi_users",
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"},
                                     {"AttributeName":"email","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-email",
                 "KeySchema":[{"AttributeName":"email","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"ALL"}}]}"#,
    );
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable with a declared GSI failed: {body}"
    );
    let gsi_put = br#"{"TableName":"gsi_users","Item":{"id":{"S":"u1"},"email":{"S":"a@x"}}}"#;
    let (status, body) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", gsi_put);
    assert_eq!(
        status, 200,
        "seed={seed}: seed PutItem on gsi_users (before freeze) must succeed: {body}"
    );
    let (gsi_tablet, gsi_leader) = tablet_and_leader_of(&cluster, gsi_table);
    let gsi_non_leader = a_non_leader(&cluster, gsi_leader);

    // --- Freeze both tablets (the fixture's own stand-in for a real
    // in-place split's own data-plane fork latching frozen -- Scenario B,
    // below, proves the real mechanism reaches the identical state). ---
    cluster.freeze_tablet(plain_leader, plain_tablet);
    cluster.freeze_tablet(gsi_leader, gsi_tablet);

    // --- PutItem on the frozen plain table, from the non-leader (forwarded,
    // one hop) and the leader (local) -- both must retry for the full
    // CLIENT_TIMEOUT and then report 503 ServiceUnavailable, never 500. ---
    assert_frozen_refusal_carries_the_frozen_text(
        &mut cluster,
        plain_non_leader,
        "DynamoDB_20120810.PutItem",
        plain_put,
        seed,
        "PutItem(plain, non-leader)",
    );
    assert_frozen_refusal_carries_the_frozen_text(
        &mut cluster,
        plain_leader,
        "DynamoDB_20120810.PutItem",
        plain_put,
        seed,
        "PutItem(plain, leader)",
    );

    // --- Same, for the GSI table's evaluate-at-leader path. ---
    assert_frozen_refusal_carries_the_frozen_text(
        &mut cluster,
        gsi_non_leader,
        "DynamoDB_20120810.PutItem",
        gsi_put,
        seed,
        "PutItem(gsi, non-leader)",
    );
    assert_frozen_refusal_carries_the_frozen_text(
        &mut cluster,
        gsi_leader,
        "DynamoDB_20120810.PutItem",
        gsi_put,
        seed,
        "PutItem(gsi, leader)",
    );

    // --- Reads are NOT gated by a freeze (`decide::frozen_refusal`'s own
    // doc): a strongly-consistent GetItem on either frozen table must
    // still succeed and return the seeded value. ---
    let (status, body) = cluster.dynamo(
        plain_non_leader,
        "DynamoDB_20120810.GetItem",
        br#"{"ConsistentRead":true,"TableName":"plain_orders",
            "Key":{"pk":{"S":"cust-1"},"sk":{"S":"order-1"}}}"#,
    );
    assert_eq!(
        status, 200,
        "seed={seed}: a ConsistentRead GetItem on a frozen table must still succeed: {body}"
    );
    assert!(
        body.contains(r#""total":{"N":"42"}"#),
        "seed={seed}: the frozen table's GetItem must still return the seeded item: {body}"
    );

    let (status, body) = cluster.dynamo(
        gsi_non_leader,
        "DynamoDB_20120810.GetItem",
        br#"{"ConsistentRead":true,"TableName":"gsi_users","Key":{"id":{"S":"u1"}}}"#,
    );
    assert_eq!(
        status, 200,
        "seed={seed}: a ConsistentRead GetItem on a frozen GSI table must still succeed: {body}"
    );
    assert!(
        body.contains(r#""email":{"S":"a@x"}"#),
        "seed={seed}: the frozen GSI table's GetItem must still return the seeded item: {body}"
    );
}

#[test]
fn frozen_tables_return_service_unavailable_not_internal_server_error() {
    run_frozen_tables_return_service_unavailable(env_seed(0x994A_0001));
}

#[test]
fn frozen_tables_return_service_unavailable_not_internal_server_error_over_seeds() {
    for i in 0..5 {
        run_frozen_tables_return_service_unavailable(0x994A_0400 + i);
    }
}

/// Replay proof (repo convention): `ANIMUS_SEED=<seed> cargo test -p
/// animusd --lib replays_frozen_refusal_service_unavailable_from_an_explicit_env_seed`.
#[test]
fn replays_frozen_refusal_service_unavailable_from_an_explicit_env_seed() {
    run_frozen_tables_return_service_unavailable(env_seed(0x994A_0002));
}

// ---------------------------------------------------------------------------
// Scenario A': the same frozen-GSI-table refusal, but through
// `BatchWriteItem` (`write_path.rs::cp_kind_write_batch`, issue #996 layer
// 2) rather than a single `PutItem` (`cp_kind_write_item`) — the semantic-
// merge gap issue #996 left behind when it copied `cp_kind_write_item`'s
// retry loop before PR #1017 (issue #994) landed the post-sleep deadline
// re-check and the terminal `ServiceUnavailable` mapping on the ORIGINAL.
// Without both ports, a `BatchWriteItem` whose whole group hits an
// exhausted-budget freeze reports a bare `500 InternalServerError` instead
// of `503 ServiceUnavailable`, exactly the mixed signal #994 removed for
// the single-item path.
// ---------------------------------------------------------------------------

fn run_frozen_gsi_table_batch_write_item_returns_service_unavailable(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);

    // A table with a declared GSI -- `table_change_records_carry_images`
    // routes every `BatchWriteItem` request against it through the
    // images-carrying arm (`dynamo.rs`), which calls
    // `ClientCtx::cp_kind_write_batch` per tablet-group instead of the
    // fast-marker `cp_kind_write_raw` path a plain table would take.
    let table = "gsi_batch_users";
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"gsi_batch_users",
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"},
                                     {"AttributeName":"email","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-email",
                 "KeySchema":[{"AttributeName":"email","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"ALL"}}]}"#,
    );
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable with a declared GSI failed: {body}"
    );

    let batch_put = br#"{"RequestItems":{"gsi_batch_users":[
        {"PutRequest":{"Item":{"id":{"S":"u1"},"email":{"S":"a@x"}}}},
        {"PutRequest":{"Item":{"id":{"S":"u2"},"email":{"S":"b@x"}}}},
        {"PutRequest":{"Item":{"id":{"S":"u3"},"email":{"S":"c@x"}}}}]}}"#;
    let (status, body) = cluster.dynamo(0, "DynamoDB_20120810.BatchWriteItem", batch_put);
    assert_eq!(
        status, 200,
        "seed={seed}: seed BatchWriteItem on gsi_batch_users (before freeze) must succeed: {body}"
    );
    assert_eq!(
        body, r#"{"UnprocessedItems":{}}"#,
        "seed={seed}: the seed batch must land every item, none unprocessed: {body}"
    );

    let (tablet, leader) = tablet_and_leader_of(&cluster, table);
    let non_leader = a_non_leader(&cluster, leader);

    // The fixture's own stand-in for a real in-place split's own data-plane
    // fork latching frozen -- identical to Scenario A above, just aimed at
    // the batch path instead of the single-item one.
    cluster.freeze_tablet(leader, tablet);

    assert_frozen_refusal_carries_the_frozen_text(
        &mut cluster,
        non_leader,
        "DynamoDB_20120810.BatchWriteItem",
        batch_put,
        seed,
        "BatchWriteItem(gsi, non-leader)",
    );
    assert_frozen_refusal_carries_the_frozen_text(
        &mut cluster,
        leader,
        "DynamoDB_20120810.BatchWriteItem",
        batch_put,
        seed,
        "BatchWriteItem(gsi, leader)",
    );
}

#[test]
fn frozen_gsi_table_batch_write_item_returns_service_unavailable_not_internal_server_error() {
    run_frozen_gsi_table_batch_write_item_returns_service_unavailable(env_seed(0x996E_0001));
}

/// Replay proof (repo convention): `ANIMUS_SEED=<seed> cargo test -p
/// animusd --lib replays_frozen_gsi_table_batch_write_item_service_unavailable_from_an_explicit_env_seed`.
#[test]
fn replays_frozen_gsi_table_batch_write_item_service_unavailable_from_an_explicit_env_seed() {
    run_frozen_gsi_table_batch_write_item_returns_service_unavailable(env_seed(0x996E_0002));
}

// ---------------------------------------------------------------------------
// Scenario B: a REAL in-place split's own data-plane fork (no test-only
// freeze injection) outlasts CLIENT_TIMEOUT, then converges.
// ---------------------------------------------------------------------------

/// Repeatedly drive [`SimCluster::drive_inplace_split_cutover`] on every
/// node (the real per-tablet cutover driver this fixture never runs as a
/// background loop, see that method's own doc) until `tablet` no longer
/// appears in the replicated `Metadata` at all -- i.e. `CutoverSplit`
/// committed and the parent retired -- or `rounds` is exhausted.
fn converge_split_cutover(cluster: &mut SimCluster, tablet: TabletId, rounds: usize) -> bool {
    for _ in 0..rounds {
        if !cluster.metadata(0).tablets.contains_key(&tablet) {
            return true;
        }
        for node in 0..cluster.node_count() as u64 {
            cluster.drive_inplace_split_cutover(node);
        }
        cluster.run_for(Duration::from_millis(200));
    }
    !cluster.metadata(0).tablets.contains_key(&tablet)
}

fn run_frozen_by_a_real_inplace_split_returns_service_unavailable_then_succeeds(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);

    let table = "split_orders";
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"split_orders",
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},
                                     {"AttributeName":"sk","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}]}"#,
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let (tablet, leader) = tablet_and_leader_of(&cluster, table);

    let item_body = br#"{"TableName":"split_orders","Item":{"pk":{"S":"cust-1"},
        "sk":{"S":"order-1"},"total":{"N":"7"}}}"#;
    let (status, body) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", item_body);
    assert_eq!(
        status, 200,
        "seed={seed}: seed PutItem (pre-split) must succeed: {body}"
    );

    // Kick a REAL in-place split (ADR 0058) -- any interior split key works,
    // since the whole range is `[b"", None)` and this test never depends on
    // which side of the boundary the seeded item ends up on. `split_key` is
    // a plain, human-readable literal (`ClientCtx::trigger_split`'s own
    // `Vec<u8>` contract), not the item's own token-prefixed physical key --
    // see `SimClusterHandle::put_raw`'s doc for why a literal split key
    // never needs to match the table's real key encoding.
    let split_body = format!(r#"{{"tablet":{},"split_key":"m"}}"#, tablet.0);
    let (status, resp) = cluster.admin(0, "POST", "/admin/tablet/split", "", split_body.as_bytes());
    assert_eq!(
        status, 200,
        "seed={seed}: split kickoff must succeed: {resp}"
    );

    // Wait for the CP data plane's own always-on `host::Reconciler` to add
    // learners, catch them up, and fork the parent's group on its own
    // (`KvCommand::SplitTablet`) -- this is the REAL mechanism, not the
    // `freeze_tablet` injection Scenario A uses. `Metadata`'s own
    // `Splitting` intent (committed at kickoff) is NOT proof of this --
    // `wait_for_fork_freeze` polls the data-plane latch itself.
    cluster.wait_for_fork_freeze(leader, tablet, Duration::from_secs(30));

    // Deliberately do NOT drive the cutover here: the fork's own freeze is
    // now genuinely latched and nothing un-freezes it until `CutoverSplit`
    // commits, so a `PutItem` issued now must retry for the whole
    // `CLIENT_TIMEOUT` and then report 503, exactly like Scenario A's
    // directly-injected freeze.
    let non_leader = a_non_leader(&cluster, leader);
    assert_frozen_refusal_is_service_unavailable(
        &mut cluster,
        non_leader,
        "DynamoDB_20120810.PutItem",
        item_body,
        seed,
        "PutItem(real in-place split, mid-freeze)",
    );

    // Now drive the cutover to convergence -- the pre-cutover vetoes (no
    // GSI/stream on this table, so both pass trivially) plus
    // `MetaCommand::CutoverSplit` retiring the parent.
    assert!(
        converge_split_cutover(&mut cluster, tablet, 60),
        "seed={seed}: the in-place split's own cutover did not converge (parent tablet \
         {tablet:?} still present) within 60 rounds"
    );

    // A retried PutItem must now succeed -- served by whichever child owns
    // the key post-cutover, transparent to the caller.
    let (status, body) = cluster.dynamo(non_leader, "DynamoDB_20120810.PutItem", item_body);
    assert_eq!(
        status, 200,
        "seed={seed}: PutItem after the split converged must succeed: {body}"
    );
}

#[test]
fn frozen_by_a_real_inplace_split_returns_service_unavailable_then_succeeds() {
    run_frozen_by_a_real_inplace_split_returns_service_unavailable_then_succeeds(env_seed(
        0x994B_0001,
    ));
}

#[test]
fn frozen_by_a_real_inplace_split_returns_service_unavailable_then_succeeds_over_seeds() {
    for i in 0..3 {
        run_frozen_by_a_real_inplace_split_returns_service_unavailable_then_succeeds(
            0x994B_0500 + i,
        );
    }
}
