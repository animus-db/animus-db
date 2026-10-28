//! End-to-end test for DynamoDB-style TTL (ADR 0051) over the real
//! DynamoDB JSON/HTTP wire, for the one scenario of the original 9-test
//! suite that `crates/animusd/src/sim_cluster_ttl.rs` (ADR 0061 rung I,
//! C-09 PR 3/PR 5) cannot convert. The other 8 converted cleanly — see
//! `sim_cluster_ttl.rs`'s own module doc for the full mapping.
//!
//! `update_time_to_live_enable_and_disable_round_trip` and
//! `disable_with_a_mismatched_attribute_name_is_rejected` — the two
//! scenarios PR 3 had to revert here because `DescribeTimeToLive` had no
//! `dynamo::dispatch_item_op` arm — moved to `sim_cluster_ttl.rs` as
//! scenarios (c)/(d) once C-09 PR 5 added that arm.
//!
//! **Why `expired_item_is_still_readable_immediately` stays here.**
//! `SimCluster::dynamo`/`put`/etc. (and every other wire-shaped op call)
//! unconditionally advance the shared simulator's virtual clock to `now +
//! OP_BUDGET` (12s) before returning — `spawn_and_capture`'s own
//! `self.sim.run_for(OP_BUDGET)`, and `animus_sim::Simulator::run_until`
//! always drains every scheduled event up to that deadline, never stopping
//! early just because the awaited future already resolved. `SimCluster`
//! also spawns the TTL reaper unconditionally on every node at a 200ms sim
//! interval (`SIM_TTL_SWEEP_INTERVAL`), so a single wire call already spans
//! 60 sweep opportunities. That makes this test — which needs to observe an
//! already-expired item still present *before* the reaper has had any
//! chance to reap it — structurally unreachable through the fixture: by the
//! time a `PutItem` writing a past-expiry attribute has returned, the
//! always-on reaper has already had dozens of chances to delete it, and
//! there is no "drive zero sweeps between these two wire calls" primitive to
//! hold it back the way this test's own real interval (production-scale, on
//! the order of a minute) does below.
//!
//! The `UpdateTimeToLive` follower-relay regression (`is_relayable_command`
//! must allow `MetaCommand::SetTableTtl`) lives in
//! `tests/schema_ddl_relay.rs`, mirroring that file's own DDL-relay suite —
//! not duplicated here.

use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use animusd::StorageBackend;
use tokio::time::{sleep, timeout};

mod support;
use support::dynamo;

/// The current wall-clock epoch second — real `SystemTime`, since this is a
/// real `ProdEnv` test (never `animus_env::Clock` from a test binary; see
/// `ttl_reaper.rs`'s own doc for why `ProdEnv::wall_now()` reads the same
/// clock).
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_secs()
}

async fn create_table(addr: SocketAddr, table: &str) {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        &format!(
            r#"{{"TableName":"{table}",
                "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}],
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}]}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
}

async fn enable_ttl(addr: SocketAddr, table: &str, attribute: &str) {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.UpdateTimeToLive",
        &format!(
            r#"{{"TableName":"{table}","TimeToLiveSpecification":{{"Enabled":true,"AttributeName":"{attribute}"}}}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "UpdateTimeToLive(enable) failed: {body}");
}

async fn put_item(addr: SocketAddr, table: &str, item_json: &str) {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        &format!(r#"{{"TableName":"{table}","Item":{item_json}}}"#),
    )
    .await;
    assert_eq!(status, 200, "PutItem failed: {body}");
}

/// `GetItem` by `id`, returning the raw response body — `{}` when absent,
/// `{"Item": {..}}` when present.
async fn get_item(addr: SocketAddr, table: &str, id: &str) -> String {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.GetItem",
        &format!(r#"{{"TableName":"{table}","Key":{{"id":{{"S":"{id}"}}}}}}"#),
    )
    .await;
    assert_eq!(status, 200, "GetItem failed: {body}");
    body
}

async fn item_present(addr: SocketAddr, table: &str, id: &str) -> bool {
    get_item(addr, table, id).await.contains("\"Item\"")
}

async fn await_node_bootstrap(node: &animusd::Node) {
    timeout(Duration::from_secs(20), async {
        loop {
            if node.is_control_leader() && !node.metadata().members.is_empty() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("node did not bootstrap within 20s");
}

/// ADR 0051 §3: an expired item is **AWS-faithfully visible** immediately
/// after its TTL passes — no read path filters it. Uses the *production*
/// sweep interval (a minute) precisely so this assertion cannot race the
/// reaper: `GetItem` runs well within the first interval of node start.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expired_item_is_still_readable_immediately() {
    let dir = support::panic_safe_tempdir();
    let (node, config) =
        support::start_single_node(&dir.path().join("n"), StorageBackend::default()).await;
    await_node_bootstrap(&node).await;
    let addr = config.nodes[0].dynamo;
    create_table(addr, "tbl").await;
    enable_ttl(addr, "tbl", "expiresAt").await;

    let past = now_secs() - 3600;
    put_item(
        addr,
        "tbl",
        &format!(r#"{{"id":{{"S":"a"}},"expiresAt":{{"N":"{past}"}}}}"#),
    )
    .await;
    assert!(
        item_present(addr, "tbl", "a").await,
        "an expired item must stay visible until the reaper actually deletes it"
    );

    node.shutdown_graceful().await;
}
