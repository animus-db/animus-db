//! Two pinned-seed smokes for the DynamoDB-style TTL reaper (ADR 0051)
//! under a real [`SimCluster`] (ADR 0061 rung I, C-09 PR 2) — the first
//! proof that `animus_node::ttl_reaper::ttl_reaper_loop`, spawned
//! unconditionally on every node by [`SimCluster::new`]/`restart`
//! (`SIM_TTL_SWEEP_INTERVAL`, 200ms), actually reaps an expired item under
//! `SimEnv`'s own deterministic wall clock (`animus_sim::SimEnv::
//! wall_now`, a pure function of virtual time — no real clock/timer
//! anywhere in this run), and that [`SimCluster::drive_ttl_sweep`] (a
//! test-only convenience for asserting an intermediate, pre-cadence state)
//! correctly leaves a not-yet-expired item alone.
//!
//! **Groundwork this PR delivers, in `client_ctx_host.rs`/`ttl_reaper.rs`/
//! `sim_cluster.rs`**: `impl TtlReaperProgressHost for ClientCtx` widened
//! from the bare concrete alias to `impl<E: Env, R: RelayClient>
//! TtlReaperProgressHost for ClientCtx<E, R>` (mirroring
//! `BackupJanitorProgressHost`'s own D4 PR 5 widening immediately above it
//! in that file — `TtlScanHost` was already generic there since the same
//! rung); `animusd::ttl_reaper::ttl_reaper_loop`'s thin wrapper widened to
//! `<E: Env, R: RelayClient>` the identical way (every production spawn
//! site passes a bare `ctx.clone()`, so both keep inferring `E = ProdEnv,
//! R = AnimusdRelayClient` and stay byte-identical — confirmed by
//! `tests/dynamo_ttl.rs`'s untrimmed 9-test suite staying green both
//! before and after this widening). `animus_node::ttl_reaper::
//! ttl_sweep_one_tablet` widened from private to `pub` (a pure visibility
//! change, no behavior change) so [`SimCluster::drive_ttl_sweep`] can
//! drive the SAME per-tablet sweep function the always-on loop already
//! calls every tick, rather than reimplementing the scan/expire/delete
//! control flow a second time — the identical "drive the real primitive on
//! demand" shape [`SimCluster::drive_stream_seal`]/[`SimCluster::
//! drain_gsi`] already have for their own once-per-tick production loops.
//! Nothing in `animus-node`'s own trait definitions changed, and
//! `ttl_reaper_loop`'s own body has zero `HashMap`/`Instant::now`/
//! `tokio::spawn` sites — it already reads `env.wall_now()`/`env.sleep()`
//! exclusively, unlike a few `tokio::time`-body findings this same PR
//! template has caught in sibling rungs (`index_drain::seal_now`,
//! `ClientCtx::admin_transfer_control_leadership`) — this module's own
//! scenario (a) below is the first thing to actually prove that in
//! practice, not just by reading the source.
//!
//! ## Scenarios
//!
//! (a) [`run_expired_item_is_reaped_by_the_always_on_loop`] — an item
//!     written with a TTL attribute a few virtual seconds in the past is
//!     reaped by the always-on loop within one `run_for` past
//!     `SIM_TTL_SWEEP_INTERVAL`, read back with `ConsistentRead: true` as
//!     absent (ADR 0055 — the wire default gives no read-your-writes
//!     guarantee, and this assertion needs one).
//! (b) [`run_future_expiry_survives_a_manual_sweep`] — an item with a
//!     future expiry survives a [`SimCluster::drive_ttl_sweep`] call on its
//!     own tablet leader.
//!
//! Both issue their `PutItem`/`GetItem` from a **non-leader** of the
//! table's own tablet, mirroring every `sim_cluster_dynamo_*` sibling's own
//! forwarding-path convention; `CreateTable`/`UpdateTimeToLive` are issued
//! from node 0 (a schema-catalog mutation, no tablet leader to route
//! around). Replays (repo convention): `ANIMUS_SEED=<seed> cargo test -p
//! animusd --lib <scenario name>`.
//!
//! ## PR 3: the remainder of `tests/dynamo_ttl.rs` (scenarios (e)-(i))
//!
//! ## PR 5 (this addition): `DescribeTimeToLive` scenarios (c)/(d)
//!
//! `dynamo::dispatch_item_op` gained a `DescribeTimeToLive` arm (ADR 0061
//! rung I, C-09 PR 5 — see `dynamo.rs::describe_time_to_live`'s own doc),
//! closing the gap PR 3 hit and had to revert these two scenarios over.
//!
//! (c) [`run_update_time_to_live_enable_and_disable_round_trip`] — pure DDL:
//!     `UpdateTimeToLive` enable/disable round-tripped through
//!     `DescribeTimeToLive` (ADR 0051 §2's `AttributeName`-omitted-when-
//!     disabled rule). No wall clock, no reaper.
//! (d) [`run_disable_with_a_mismatched_attribute_name_is_rejected`] — pure
//!     DDL: disabling with the wrong `AttributeName` is refused client-side
//!     and leaves the catalog untouched.
//!
//! (e) [`run_future_ttl_item_is_never_deleted`] — a future-expiry item rides
//!     out several always-on-loop sweep cycles (`SimCluster::run_for`, no
//!     wire call in between) and stays present — the always-on-loop sibling
//!     of scenario (b) above, which drives a single sweep manually instead.
//! (f) [`run_wrong_type_ttl_attribute_is_never_deleted`] — a TTL attribute
//!     of the wrong DynamoDB type (`S`, not `N`) is silently never-expiring.
//! (g) [`run_absurdly_past_ttl_is_never_deleted_the_five_year_safety_window`]
//!     — an expiry more than [`animus_dynamo::MAX_PAST_EXPIRY_SECS`] in the
//!     past is treated as not-expired (ADR 0051 §5's milliseconds-vs-
//!     seconds guard).
//! (h) [`run_refreshed_ttl_survives_the_reaper`] — an item's TTL refreshed
//!     to the future via `UpdateItem` survives, whether the reaper's
//!     conditional delete was skipped outright or the item was recreated by
//!     the same upsert-on-missing `UpdateItem` semantics AWS itself defines
//!     — both converge on the same observable outcome this scenario checks
//!     (see its own doc for why the tighter timing claim doesn't need
//!     reproducing here, mirroring the original real-socket test's own
//!     disclaimer).
//! (i) [`run_ttl_deletion_is_visible_in_the_stream_with_a_service_user_identity`]
//!     — a TTL-reaper delete's stream record carries `userIdentity:
//!     {"PrincipalId": "dynamodb.amazonaws.com", "Type": "Service"}`; an
//!     ordinary client `DeleteItem` carries none (ADR 0051 §7), reusing C-07
//!     PR 3's `GetShardIterator`/`GetRecords` wire shapes.
//!
//! **One residual, kept in `tests/dynamo_ttl.rs` with its own reason comment
//! rather than converted** (PR 3's own two `DescribeTimeToLive`-blocked
//! residuals, scenarios (c) and (d) above, are closed by this PR):
//!
//! - `expired_item_is_still_readable_immediately`. Every
//!   `SimCluster::dynamo`/`put`/etc. call unconditionally advances the
//!   shared simulator clock to `now + OP_BUDGET` (12s) —
//!   `spawn_and_capture`'s own `self.sim.run_for(OP_BUDGET)`, and
//!   `animus_sim::Simulator::run_until` always drains every scheduled event
//!   up to that deadline before returning, never stopping early just
//!   because the awaited future already resolved. Since the always-on TTL
//!   reaper ticks every `SIM_TTL_SWEEP_INTERVAL` (200ms), a single wire
//!   call already spans 60 sweep opportunities — so a `PutItem` writing an
//!   already-expired attribute has, by the time it returns, already handed
//!   the reaper dozens of chances to reap it before any subsequent
//!   `GetItem` could observe the pre-reap state. `drive_ttl_sweep` only
//!   adds a sweep on demand; there is no "drive zero sweeps between these
//!   two wire calls" primitive to hold the reaper back the way the real
//!   `ProdEnv` test does by using the *slow* production interval. This is a
//!   real property of the fixture, not this scenario's design.

use std::time::Duration;

use super::sim_cluster::SimCluster;
use super::sim_cluster_console::{
    create_table_via_wire, env_seed, get_item_via_wire, json, leader_of_table, non_leader_of_table,
    put_item_via_wire,
};

/// Bounded converged-or-timeout poll for the always-on reaper — a fixed
/// number of `run_for(SIM_TTL_SWEEP_INTERVAL * 2)` steps past `settle`,
/// checked after each, mirroring `sim_cluster_dynamo.rs::
/// poll_until_get_contains`'s own shape but for the absence case (a bare
/// `GetItem` response with no `"Item"` key at all).
fn poll_until_reaped(cluster: &mut SimCluster, node: u64, get_body: &str, seed: u64) {
    const ATTEMPTS: usize = 20;
    const STEP: Duration = Duration::from_millis(400);
    let mut last = String::new();
    for _ in 0..ATTEMPTS {
        let (status, body) = get_item_via_wire(cluster, node, get_body);
        if status == 200 && !body.contains("\"Item\"") {
            return;
        }
        last = format!("status={status} body={body}");
        cluster.run_for(STEP);
    }
    panic!(
        "item was never reaped by the always-on TTL loop within {} attempts \
         (last={last}, seed={seed})",
        ATTEMPTS
    );
}

// ---------------------------------------------------------------------------
// (a) an expired item is reaped by the always-on loop
// ---------------------------------------------------------------------------

#[test]
fn expired_item_is_reaped_by_the_always_on_loop() {
    run_expired_item_is_reaped_by_the_always_on_loop(env_seed(0xC091_0001));
}

#[test]
fn expired_item_is_reaped_by_the_always_on_loop_over_seeds() {
    for i in 0..5 {
        run_expired_item_is_reaped_by_the_always_on_loop(0xC091_1000 + i);
    }
}

fn run_expired_item_is_reaped_by_the_always_on_loop(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"ttl_reap",
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    );
    assert_eq!(status, 200, "CreateTable failed (seed={seed}): {body}");

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.UpdateTimeToLive",
        br#"{"TableName":"ttl_reap",
             "TimeToLiveSpecification":{"Enabled":true,"AttributeName":"expiresAt"}}"#,
    );
    assert_eq!(
        status, 200,
        "UpdateTimeToLive(enable) failed (seed={seed}): {body}"
    );

    let non_leader = non_leader_of_table(&cluster, "ttl_reap");
    let past = cluster.wall_now_secs(non_leader).saturating_sub(5);
    let (status, body) = put_item_via_wire(
        &mut cluster,
        non_leader,
        &format!(
            r#"{{"TableName":"ttl_reap","Item":{{"id":{{"S":"a"}},"expiresAt":{{"N":"{past}"}}}}}}"#
        ),
    );
    assert_eq!(status, 200, "PutItem failed (seed={seed}): {body}");

    let get_body = r#"{"ConsistentRead":true,"TableName":"ttl_reap","Key":{"id":{"S":"a"}}}"#;
    poll_until_reaped(&mut cluster, non_leader, get_body, seed);

    // The tablet's own leader is the node whose `ttl_reaper_loop` actually
    // proposed the delete (`TtlScanHost::ttl_delete_if_attribute_equals`
    // wakes and proposes on the group it holds locally) — its own
    // `GET /admin/ttl`-shaped progress snapshot should show at least one
    // real deletion, proving the whole pipeline (scan → expire → delete →
    // progress publish), not just the read-side absence the poll above
    // already confirmed.
    let leader = leader_of_table(&cluster, "ttl_reap");
    let progress = cluster.ttl_reaper_progress(leader);
    assert!(
        progress.deleted_total >= 1,
        "the reaping node's own TtlReaperProgress must record at least one \
         deletion (seed={seed}): {progress:?}"
    );
}

// ---------------------------------------------------------------------------
// (b) a future-expiry item survives a manual `drive_ttl_sweep`
// ---------------------------------------------------------------------------

#[test]
fn future_expiry_survives_a_manual_sweep() {
    run_future_expiry_survives_a_manual_sweep(env_seed(0xC091_0002));
}

#[test]
fn future_expiry_survives_a_manual_sweep_over_seeds() {
    for i in 0..5 {
        run_future_expiry_survives_a_manual_sweep(0xC091_2000 + i);
    }
}

fn run_future_expiry_survives_a_manual_sweep(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        r#"{"TableName":"ttl_survive",
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    );
    assert_eq!(status, 200, "CreateTable failed (seed={seed}): {body}");

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.UpdateTimeToLive",
        br#"{"TableName":"ttl_survive",
             "TimeToLiveSpecification":{"Enabled":true,"AttributeName":"expiresAt"}}"#,
    );
    assert_eq!(
        status, 200,
        "UpdateTimeToLive(enable) failed (seed={seed}): {body}"
    );

    let non_leader = non_leader_of_table(&cluster, "ttl_survive");
    let future = cluster.wall_now_secs(non_leader) + 3600;
    let (status, body) = put_item_via_wire(
        &mut cluster,
        non_leader,
        &format!(
            r#"{{"TableName":"ttl_survive","Item":{{"id":{{"S":"a"}},"expiresAt":{{"N":"{future}"}}}}}}"#
        ),
    );
    assert_eq!(status, 200, "PutItem failed (seed={seed}): {body}");

    let leader = leader_of_table(&cluster, "ttl_survive");
    cluster.drive_ttl_sweep(leader);

    let get_body = r#"{"ConsistentRead":true,"TableName":"ttl_survive","Key":{"id":{"S":"a"}}}"#;
    let (status, body) = get_item_via_wire(&mut cluster, non_leader, get_body);
    assert_eq!(status, 200, "GetItem failed (seed={seed}): {body}");
    assert!(
        body.contains("\"Item\""),
        "a future TTL must never be treated as expired (seed={seed}): {body}"
    );
}

// ---------------------------------------------------------------------------
// PR 3 shared helpers
// ---------------------------------------------------------------------------

/// Mirrors `sim_cluster.rs`'s own private `SIM_TTL_SWEEP_INTERVAL` (200ms) —
/// not importable across modules (it's a private `const`), so this is an
/// independent constant of the same value, exactly like [`poll_until_reaped`]'s
/// own `STEP` above. Used to "ride out" several always-on-loop sweep cycles
/// via [`SimCluster::run_for`] between two wire calls, mirroring the
/// original real-socket tests' own `sleep(TEST_TTL_SWEEP_INTERVAL * 6)`.
const RIDE_OUT_SEVERAL_CYCLES: Duration = Duration::from_millis(200 * 6);

fn update_ttl_via_wire(
    cluster: &mut SimCluster,
    node: u64,
    table: &str,
    enabled: bool,
    attribute: &str,
) -> (u16, String) {
    let body = format!(
        r#"{{"TableName":"{table}","TimeToLiveSpecification":{{"Enabled":{enabled},"AttributeName":"{attribute}"}}}}"#
    );
    cluster.dynamo(node, "DynamoDB_20120810.UpdateTimeToLive", body.as_bytes())
}

/// `DescribeTimeToLive`, issued from `node` — added alongside
/// `dynamo::dispatch_item_op`'s new `DescribeTimeToLive` arm (ADR 0061 rung
/// I, C-09 PR 5); mirrors [`describe_stream_via_wire`]'s own
/// `(u16, serde_json::Value)` shape.
fn describe_ttl_via_wire(
    cluster: &mut SimCluster,
    node: u64,
    table: &str,
) -> (u16, serde_json::Value) {
    let body = format!(r#"{{"TableName":"{table}"}}"#);
    let (status, resp) = cluster.dynamo(
        node,
        "DynamoDB_20120810.DescribeTimeToLive",
        body.as_bytes(),
    );
    (status, json(&resp))
}

fn update_item_via_wire(cluster: &mut SimCluster, node: u64, body: &str) -> (u16, String) {
    cluster.dynamo(node, "DynamoDB_20120810.UpdateItem", body.as_bytes())
}

fn delete_item_via_wire(cluster: &mut SimCluster, node: u64, body: &str) -> (u16, String) {
    cluster.dynamo(node, "DynamoDB_20120810.DeleteItem", body.as_bytes())
}

/// `DescribeStream`, issued from `node` — mirrors `sim_cluster_dynamo_
/// streams.rs`'s own helper of the same name (module-private there, so not
/// reused directly; this is an independent copy of the identical shape).
fn describe_stream_via_wire(
    cluster: &mut SimCluster,
    node: u64,
    stream_arn: &str,
) -> (u16, serde_json::Value) {
    let body = format!(r#"{{"StreamArn":"{stream_arn}"}}"#);
    let (status, resp) = cluster.dynamo_streams(
        node,
        "DynamoDBStreams_20120810.DescribeStream",
        body.as_bytes(),
    );
    (status, json(&resp))
}

/// `GetShardIterator` (`TRIM_HORIZON`), issued from `node` — panics on a
/// non-200 (this module's own scenario only ever expects a valid iterator).
fn get_shard_iterator_via_wire(
    cluster: &mut SimCluster,
    node: u64,
    stream_arn: &str,
    shard_id: &str,
) -> String {
    let body = format!(
        r#"{{"StreamArn":"{stream_arn}","ShardId":"{shard_id}","ShardIteratorType":"TRIM_HORIZON"}}"#
    );
    let (status, resp) = cluster.dynamo_streams(
        node,
        "DynamoDBStreams_20120810.GetShardIterator",
        body.as_bytes(),
    );
    assert_eq!(status, 200, "GetShardIterator failed: {resp}");
    json(&resp)["ShardIterator"]
        .as_str()
        .unwrap_or_else(|| panic!("no ShardIterator in: {resp}"))
        .to_owned()
}

/// `GetRecords`, issued from `node` — panics on a non-200 (the identical
/// contract [`get_shard_iterator_via_wire`] documents).
fn get_records_via_wire(
    cluster: &mut SimCluster,
    node: u64,
    iterator: &str,
) -> (Vec<serde_json::Value>, Option<String>) {
    let body = format!(r#"{{"ShardIterator":"{iterator}"}}"#);
    let (status, resp) =
        cluster.dynamo_streams(node, "DynamoDBStreams_20120810.GetRecords", body.as_bytes());
    assert_eq!(status, 200, "GetRecords failed: {resp}");
    let v = json(&resp);
    let records = v["Records"].as_array().cloned().unwrap_or_default();
    let next = v["NextShardIterator"].as_str().map(str::to_owned);
    (records, next)
}

// ---------------------------------------------------------------------------
// (c) UpdateTimeToLive enable/disable round trip (pure DDL, no wall clock)
// ---------------------------------------------------------------------------

/// `UpdateTimeToLive` enable -> `DescribeTimeToLive` reports `ENABLED` plus
/// the attribute name; disable -> `DISABLED` with **no** `AttributeName`
/// (ADR 0051 §2, matching AWS's own omission rule). Pure DDL over the
/// replicated schema catalog — no wall clock, no reaper involved at all.
#[test]
fn update_time_to_live_enable_and_disable_round_trip() {
    run_update_time_to_live_enable_and_disable_round_trip(env_seed(0xC091_0003));
}

#[test]
fn update_time_to_live_enable_and_disable_round_trip_over_seeds() {
    for i in 0..5 {
        run_update_time_to_live_enable_and_disable_round_trip(0xC091_3000 + i);
    }
}

fn run_update_time_to_live_enable_and_disable_round_trip(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "ttl_ddl";

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        &format!(
            r#"{{"TableName":"{table}",
                "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}],
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}]}}"#
        ),
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    let (status, body) = update_ttl_via_wire(&mut cluster, 0, table, true, "expiresAt");
    assert_eq!(
        status, 200,
        "seed={seed}: UpdateTimeToLive(enable) failed: {body}"
    );

    let (status, desc) = describe_ttl_via_wire(&mut cluster, 0, table);
    assert_eq!(
        status, 200,
        "seed={seed}: DescribeTimeToLive failed: {desc}"
    );
    assert_eq!(
        desc["TimeToLiveDescription"]["TimeToLiveStatus"], "ENABLED",
        "seed={seed}: {desc}"
    );
    assert_eq!(
        desc["TimeToLiveDescription"]["AttributeName"], "expiresAt",
        "seed={seed}: {desc}"
    );

    let (status, body) = update_ttl_via_wire(&mut cluster, 0, table, false, "expiresAt");
    assert_eq!(
        status, 200,
        "seed={seed}: UpdateTimeToLive(disable) failed: {body}"
    );

    let (status, desc) = describe_ttl_via_wire(&mut cluster, 0, table);
    assert_eq!(
        status, 200,
        "seed={seed}: DescribeTimeToLive failed: {desc}"
    );
    assert_eq!(
        desc["TimeToLiveDescription"]["TimeToLiveStatus"], "DISABLED",
        "seed={seed}: {desc}"
    );
    assert!(
        desc["TimeToLiveDescription"].get("AttributeName").is_none(),
        "seed={seed}: a disabled table must omit `AttributeName` entirely: {desc}"
    );
}

// ---------------------------------------------------------------------------
// (d) a mismatched disable AttributeName is rejected (pure DDL)
// ---------------------------------------------------------------------------

/// Disabling with an `AttributeName` that doesn't match the currently
/// enabled one is rejected client-side (ADR 0051's `UpdateTimeToLive`
/// contract) — never silently accepted or silently disabling the wrong
/// attribute; the catalog must stay `ENABLED` under the original attribute.
#[test]
fn disable_with_a_mismatched_attribute_name_is_rejected() {
    run_disable_with_a_mismatched_attribute_name_is_rejected(env_seed(0xC091_0004));
}

#[test]
fn disable_with_a_mismatched_attribute_name_is_rejected_over_seeds() {
    for i in 0..5 {
        run_disable_with_a_mismatched_attribute_name_is_rejected(0xC091_4000 + i);
    }
}

fn run_disable_with_a_mismatched_attribute_name_is_rejected(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "ttl_mismatch";

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        &format!(
            r#"{{"TableName":"{table}",
                "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}],
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}]}}"#
        ),
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let (status, body) = update_ttl_via_wire(&mut cluster, 0, table, true, "expiresAt");
    assert_eq!(
        status, 200,
        "seed={seed}: UpdateTimeToLive(enable) failed: {body}"
    );

    let (status, body) = update_ttl_via_wire(&mut cluster, 0, table, false, "wrongAttr");
    assert_eq!(status, 400, "seed={seed}: {body}");

    // TTL is still enabled under the original attribute — the rejected call
    // must not have taken effect.
    let (status, desc) = describe_ttl_via_wire(&mut cluster, 0, table);
    assert_eq!(
        status, 200,
        "seed={seed}: DescribeTimeToLive failed: {desc}"
    );
    assert_eq!(
        desc["TimeToLiveDescription"]["TimeToLiveStatus"], "ENABLED",
        "seed={seed}: {desc}"
    );
}

// ---------------------------------------------------------------------------
// (e) future TTL survives many always-on-loop cycles (no manual drive)
// ---------------------------------------------------------------------------

/// An item with a future TTL is never deleted — ridden out over several
/// always-on-loop sweep cycles via [`SimCluster::run_for`] (no manual
/// [`SimCluster::drive_ttl_sweep`] involved), the always-on-loop sibling of
/// scenario (b) above.
#[test]
fn future_ttl_item_is_never_deleted() {
    run_future_ttl_item_is_never_deleted(env_seed(0xC091_0005));
}

#[test]
fn future_ttl_item_is_never_deleted_over_seeds() {
    for i in 0..5 {
        run_future_ttl_item_is_never_deleted(0xC091_5000 + i);
    }
}

fn run_future_ttl_item_is_never_deleted(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "ttl_never_future";

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        &format!(
            r#"{{"TableName":"{table}",
                "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}],
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}]}}"#
        ),
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let (status, body) = update_ttl_via_wire(&mut cluster, 0, table, true, "expiresAt");
    assert_eq!(
        status, 200,
        "seed={seed}: UpdateTimeToLive(enable) failed: {body}"
    );

    let non_leader = non_leader_of_table(&cluster, table);
    let future = cluster.wall_now_secs(non_leader) + 3600;
    let (status, body) = put_item_via_wire(
        &mut cluster,
        non_leader,
        &format!(
            r#"{{"TableName":"{table}","Item":{{"id":{{"S":"a"}},"expiresAt":{{"N":"{future}"}}}}}}"#
        ),
    );
    assert_eq!(status, 200, "seed={seed}: PutItem failed: {body}");

    // Ride out several sweep intervals via the always-on loop itself — a
    // stable negative, not an eventual property to converge on.
    cluster.run_for(RIDE_OUT_SEVERAL_CYCLES);

    let get_body =
        format!(r#"{{"ConsistentRead":true,"TableName":"{table}","Key":{{"id":{{"S":"a"}}}}}}"#);
    let (status, body) = get_item_via_wire(&mut cluster, non_leader, &get_body);
    assert_eq!(status, 200, "seed={seed}: GetItem failed: {body}");
    assert!(
        body.contains("\"Item\""),
        "seed={seed}: a future TTL must never be treated as expired: {body}"
    );
}

// ---------------------------------------------------------------------------
// (f) a wrong-type TTL attribute is never deleted
// ---------------------------------------------------------------------------

/// A TTL attribute of the wrong DynamoDB type (`S` instead of `N`) is
/// silently never-expiring, matching AWS.
#[test]
fn wrong_type_ttl_attribute_is_never_deleted() {
    run_wrong_type_ttl_attribute_is_never_deleted(env_seed(0xC091_0006));
}

#[test]
fn wrong_type_ttl_attribute_is_never_deleted_over_seeds() {
    for i in 0..5 {
        run_wrong_type_ttl_attribute_is_never_deleted(0xC091_6000 + i);
    }
}

fn run_wrong_type_ttl_attribute_is_never_deleted(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "ttl_wrong_type";

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        &format!(
            r#"{{"TableName":"{table}",
                "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}],
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}]}}"#
        ),
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let (status, body) = update_ttl_via_wire(&mut cluster, 0, table, true, "expiresAt");
    assert_eq!(
        status, 200,
        "seed={seed}: UpdateTimeToLive(enable) failed: {body}"
    );

    let non_leader = non_leader_of_table(&cluster, table);
    // A `String` far in the "past" by any calendar reading, but the wrong
    // DynamoDB type — `is_expired` must read this as absent, not expired.
    let (status, body) = put_item_via_wire(
        &mut cluster,
        non_leader,
        &format!(
            r#"{{"TableName":"{table}","Item":{{"id":{{"S":"a"}},"expiresAt":{{"S":"1999-01-01"}}}}}}"#
        ),
    );
    assert_eq!(status, 200, "seed={seed}: PutItem failed: {body}");

    cluster.run_for(RIDE_OUT_SEVERAL_CYCLES);

    let get_body =
        format!(r#"{{"ConsistentRead":true,"TableName":"{table}","Key":{{"id":{{"S":"a"}}}}}}"#);
    let (status, body) = get_item_via_wire(&mut cluster, non_leader, &get_body);
    assert_eq!(status, 200, "seed={seed}: GetItem failed: {body}");
    assert!(
        body.contains("\"Item\""),
        "seed={seed}: a wrong-type TTL attribute must never be treated as expired: {body}"
    );
}

// ---------------------------------------------------------------------------
// (g) the five-year safety window
// ---------------------------------------------------------------------------

/// **The most important test in the feature** (ADR 0051 §5): an expiry
/// further than `animus_dynamo::MAX_PAST_EXPIRY_SECS` in the past — the
/// signature of a client writing milliseconds where seconds were expected,
/// or otherwise unit-confused — is treated as **not expired**, guarding an
/// entire table against instant mass deletion the moment TTL is enabled.
#[test]
fn absurdly_past_ttl_is_never_deleted_the_five_year_safety_window() {
    run_absurdly_past_ttl_is_never_deleted_the_five_year_safety_window(env_seed(0xC091_0007));
}

#[test]
fn absurdly_past_ttl_is_never_deleted_the_five_year_safety_window_over_seeds() {
    for i in 0..5 {
        run_absurdly_past_ttl_is_never_deleted_the_five_year_safety_window(0xC091_7000 + i);
    }
}

fn run_absurdly_past_ttl_is_never_deleted_the_five_year_safety_window(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "ttl_five_year";

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        &format!(
            r#"{{"TableName":"{table}",
                "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}],
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}]}}"#
        ),
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let (status, body) = update_ttl_via_wire(&mut cluster, 0, table, true, "expiresAt");
    assert_eq!(
        status, 200,
        "seed={seed}: UpdateTimeToLive(enable) failed: {body}"
    );

    let non_leader = non_leader_of_table(&cluster, table);
    // Ten years in the past — well beyond the 5-year window.
    let absurdly_past = cluster
        .wall_now_secs(non_leader)
        .saturating_sub(10 * 365 * 24 * 60 * 60);
    let (status, body) = put_item_via_wire(
        &mut cluster,
        non_leader,
        &format!(
            r#"{{"TableName":"{table}","Item":{{"id":{{"S":"a"}},"expiresAt":{{"N":"{absurdly_past}"}}}}}}"#
        ),
    );
    assert_eq!(status, 200, "seed={seed}: PutItem failed: {body}");

    cluster.run_for(RIDE_OUT_SEVERAL_CYCLES);

    let get_body =
        format!(r#"{{"ConsistentRead":true,"TableName":"{table}","Key":{{"id":{{"S":"a"}}}}}}"#);
    let (status, body) = get_item_via_wire(&mut cluster, non_leader, &get_body);
    assert_eq!(status, 200, "seed={seed}: GetItem failed: {body}");
    assert!(
        body.contains("\"Item\""),
        "seed={seed}: an expiry more than 5 years in the past must never be \
         deleted (the milliseconds-vs-seconds safety guard): {body}"
    );
}

// ---------------------------------------------------------------------------
// (h) a refreshed TTL survives the reaper
// ---------------------------------------------------------------------------

/// The conditional delete's outcome (ADR 0051 §4): an item whose TTL is
/// refreshed to the future before the reaper's delete lands survives — and
/// even if the reaper's conditional delete had already fired first (this
/// fixture's `OP_BUDGET`-per-call granularity cannot exclude that the way
/// the original real-socket test's fast-but-still-real interval could), the
/// refreshing `UpdateItem` re-creates the item via the identical
/// upsert-on-missing semantics AWS itself defines, so the observable
/// contract this scenario checks — item present, carrying the refreshed
/// value — holds regardless of which path was taken. The original test's
/// own doc already scopes itself the same way: the tighter scan-vs-propose
/// race window is a leader-side, sub-millisecond internal race covered by
/// `animus-cp-data`'s own `KindBatch.conditions` OCC seatbelt tests, not
/// reproducible from a black-box wire client either way.
#[test]
fn refreshed_ttl_survives_the_reaper() {
    run_refreshed_ttl_survives_the_reaper(env_seed(0xC091_0008));
}

#[test]
fn refreshed_ttl_survives_the_reaper_over_seeds() {
    for i in 0..5 {
        run_refreshed_ttl_survives_the_reaper(0xC091_8000 + i);
    }
}

fn run_refreshed_ttl_survives_the_reaper(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "ttl_refresh";

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        &format!(
            r#"{{"TableName":"{table}",
                "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}],
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}]}}"#
        ),
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let (status, body) = update_ttl_via_wire(&mut cluster, 0, table, true, "expiresAt");
    assert_eq!(
        status, 200,
        "seed={seed}: UpdateTimeToLive(enable) failed: {body}"
    );

    let non_leader = non_leader_of_table(&cluster, table);
    let past = cluster.wall_now_secs(non_leader).saturating_sub(3600);
    let (status, body) = put_item_via_wire(
        &mut cluster,
        non_leader,
        &format!(
            r#"{{"TableName":"{table}","Item":{{"id":{{"S":"a"}},"expiresAt":{{"N":"{past}"}}}}}}"#
        ),
    );
    assert_eq!(status, 200, "seed={seed}: PutItem failed: {body}");

    let future = cluster.wall_now_secs(non_leader) + 3600;
    let (status, body) = update_item_via_wire(
        &mut cluster,
        non_leader,
        &format!(
            r#"{{"TableName":"{table}","Key":{{"id":{{"S":"a"}}}},
                "UpdateExpression":"SET expiresAt = :v",
                "ExpressionAttributeValues":{{":v":{{"N":"{future}"}}}}}}"#
        ),
    );
    assert_eq!(
        status, 200,
        "seed={seed}: refreshing UpdateItem failed: {body}"
    );

    cluster.run_for(RIDE_OUT_SEVERAL_CYCLES);

    let get_body =
        format!(r#"{{"ConsistentRead":true,"TableName":"{table}","Key":{{"id":{{"S":"a"}}}}}}"#);
    let (status, body) = get_item_via_wire(&mut cluster, non_leader, &get_body);
    assert_eq!(status, 200, "seed={seed}: GetItem failed: {body}");
    assert!(
        body.contains("\"Item\""),
        "seed={seed}: an item refreshed to a future TTL before the reaper \
         reached it must survive: {body}"
    );
    assert!(
        body.contains(&format!("\"N\":\"{future}\"")),
        "seed={seed}: the surviving item must carry the refreshed value: {body}"
    );
}

// ---------------------------------------------------------------------------
// (i) a TTL deletion carries the service userIdentity in the stream
// ---------------------------------------------------------------------------

/// ADR 0051 §7: a TTL-reaper delete's stream record carries `userIdentity:
/// {"PrincipalId": "dynamodb.amazonaws.com", "Type": "Service"}`; an
/// ordinary client `DeleteItem` carries none at all.
#[test]
fn ttl_deletion_is_visible_in_the_stream_with_a_service_user_identity() {
    run_ttl_deletion_is_visible_in_the_stream_with_a_service_user_identity(env_seed(0xC091_0009));
}

#[test]
fn ttl_deletion_is_visible_in_the_stream_with_a_service_user_identity_over_seeds() {
    for i in 0..5 {
        run_ttl_deletion_is_visible_in_the_stream_with_a_service_user_identity(0xC091_9000 + i);
    }
}

fn run_ttl_deletion_is_visible_in_the_stream_with_a_service_user_identity(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "ttl_stream";

    let (status, body) = create_table_via_wire(
        &mut cluster,
        0,
        &format!(
            r#"{{"TableName":"{table}",
                "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}],
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
                "StreamSpecification":{{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}}}"#
        ),
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let stream_arn = json(&body)["TableDescription"]["LatestStreamArn"]
        .as_str()
        .unwrap_or_else(|| panic!("seed={seed}: no LatestStreamArn: {body}"))
        .to_owned();
    let (status, body) = update_ttl_via_wire(&mut cluster, 0, table, true, "expiresAt");
    assert_eq!(
        status, 200,
        "seed={seed}: UpdateTimeToLive(enable) failed: {body}"
    );

    let non_leader = non_leader_of_table(&cluster, table);

    // `ttl-item`: expired, reaped by the always-on loop.
    let past = cluster.wall_now_secs(non_leader).saturating_sub(3600);
    let (status, body) = put_item_via_wire(
        &mut cluster,
        non_leader,
        &format!(
            r#"{{"TableName":"{table}","Item":{{"id":{{"S":"ttl-item"}},"expiresAt":{{"N":"{past}"}}}}}}"#
        ),
    );
    assert_eq!(status, 200, "seed={seed}: PutItem(ttl-item) failed: {body}");

    // `client-item`: never expires, deleted by the client itself.
    let (status, body) = put_item_via_wire(
        &mut cluster,
        non_leader,
        &format!(r#"{{"TableName":"{table}","Item":{{"id":{{"S":"client-item"}}}}}}"#),
    );
    assert_eq!(
        status, 200,
        "seed={seed}: PutItem(client-item) failed: {body}"
    );
    let (status, body) = delete_item_via_wire(
        &mut cluster,
        non_leader,
        &format!(r#"{{"TableName":"{table}","Key":{{"id":{{"S":"client-item"}}}}}}"#),
    );
    assert_eq!(status, 200, "seed={seed}: client DeleteItem failed: {body}");

    let get_body = format!(
        r#"{{"ConsistentRead":true,"TableName":"{table}","Key":{{"id":{{"S":"ttl-item"}}}}}}"#
    );
    poll_until_reaped(&mut cluster, non_leader, &get_body, seed);

    // Walk the open shard's hot tail from TRIM_HORIZON until both REMOVE
    // events are seen — both deletes are already committed by this point
    // (the poll above only returns once `ttl-item` is gone, and the client
    // `DeleteItem` above already returned 200), so this is a pagination
    // walk, not a further convergence wait.
    let (status, v) = describe_stream_via_wire(&mut cluster, non_leader, &stream_arn);
    assert_eq!(status, 200, "seed={seed}: DescribeStream failed: {v}");
    let shard_id = v["StreamDescription"]["Shards"][0]["ShardId"]
        .as_str()
        .unwrap_or_else(|| panic!("seed={seed}: no shard: {v}"))
        .to_owned();

    let mut iterator =
        get_shard_iterator_via_wire(&mut cluster, non_leader, &stream_arn, &shard_id);
    let (mut ttl_record, mut client_record) = (None, None);
    for _ in 0..10 {
        if ttl_record.is_some() && client_record.is_some() {
            break;
        }
        let (records, next) = get_records_via_wire(&mut cluster, non_leader, &iterator);
        for record in records {
            if record["eventName"] != "REMOVE" {
                continue;
            }
            match record["dynamodb"]["Keys"]["id"]["S"].as_str().unwrap_or("") {
                "ttl-item" => ttl_record = Some(record),
                "client-item" => client_record = Some(record),
                _ => {}
            }
        }
        match next {
            Some(n) => iterator = n,
            None => break,
        }
    }

    let ttl_record = ttl_record
        .unwrap_or_else(|| panic!("seed={seed}: the TTL delete's REMOVE record never appeared"));
    let client_record = client_record
        .unwrap_or_else(|| panic!("seed={seed}: the client delete's REMOVE record never appeared"));

    assert_eq!(
        ttl_record["userIdentity"]["PrincipalId"], "dynamodb.amazonaws.com",
        "seed={seed}: a TTL delete must carry the service userIdentity: {ttl_record}"
    );
    assert_eq!(
        ttl_record["userIdentity"]["Type"], "Service",
        "seed={seed}: {ttl_record}"
    );
    assert!(
        client_record.get("userIdentity").is_none(),
        "seed={seed}: a client delete must carry no userIdentity at all: {client_record}"
    );
}
