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
//! PR 3 extends this module with the remainder of `tests/dynamo_ttl.rs`'s
//! own scenarios (the conditional-delete outcome, the wrong-type/5-year-
//! safety-window never-expire cases, the refreshed-TTL survives-the-reaper
//! case, and the stream `userIdentity`) — see `crates/animusd/CLAUDE.md`'s
//! matching C-09 appendix.

use std::time::Duration;

use super::sim_cluster::SimCluster;
use super::sim_cluster_console::{
    create_table_via_wire, env_seed, get_item_via_wire, leader_of_table, non_leader_of_table,
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
