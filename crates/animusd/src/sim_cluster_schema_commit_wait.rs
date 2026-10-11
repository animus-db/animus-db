//! Issue #1292: `ClientCtx::propose_and_await`'s commit-wait on a node that
//! is itself the control leader.
//!
//! **The defect**: `UpdateContinuousBackups(disable)` 500ed "did not commit
//! to the control plane in time (no leader reachable?)" in the single-node
//! `ProdEnv` stress test `dynamo_pitr::disable_survives_concurrent_periodic_
//! seal_on_local_route`, with its own leader alive and applying. A commit
//! there is a pipeline (persist round `fsync`, then the apply task's merge
//! and publish), each stage queued behind the `fsync`s ahead of it; under
//! the test's write load that took longer than the handler's fixed 5s
//! deadline while every stage was still moving. The handler also
//! resubmitted its command on every 50ms poll, appending a duplicate entry
//! to the same pipeline each time.
//!
//! **The scenario here**: one node, every disk `append` and `sync` slowed to
//! [`SYNC_DELAY`], and a filler command already being persisted when the
//! waited-on command is proposed — so the waited-on command needs two full
//! persist rounds (longer than [`SCHEMA_COMMIT_TIMEOUT`]) while the apply
//! watermark visibly advances in between. It must commit, not time out.
//! The negative control slows the disk past the whole budget, so nothing
//! advances: the same wait must still give up after one plain timeout.
//!
//! Replay one seed: `ANIMUS_SEED=<seed> cargo test -p animusd --lib
//! sim_cluster_schema_commit_wait`.

use std::time::Duration;

use animus_control::MetaCommand;
use animus_env::Clock;

use super::dynamo::SCHEMA_COMMIT_TIMEOUT;
use super::sim_cluster::SimCluster;

/// Per-`append`/`sync` delay: one persist round (an append then a sync) is
/// shorter than [`SCHEMA_COMMIT_TIMEOUT`], two back to back are longer.
const SYNC_DELAY: Duration = Duration::from_secs(2);

const TABLE: &str = "orders";

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn create_table(cluster: &mut SimCluster) {
    let body = format!(
        r#"{{"TableName":"{TABLE}",
            "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]}}"#
    );
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.CreateTable", body.as_bytes());
    assert_eq!(status, 200, "CreateTable failed: {resp}");
}

/// Slow every disk `append` and `sync` to `delay`, put a filler command into the persist
/// pipeline, then `propose_and_await` enabling PITR right behind it.
/// Returns whether the wait succeeded and how much virtual time it took.
fn enable_pitr_behind_a_slow_round(seed: u64, delay: Duration) -> (bool, Duration) {
    let mut cluster = SimCluster::new(seed, 1, 1);
    create_table(&mut cluster);
    let mut disk = animus_sim::DiskConfig::default();
    disk.set_sync_delay(delay);
    cluster.set_disk_config(disk);

    cluster
        .run_with_ctx(0, |ctx| async move {
            let leader = ctx
                .edge
                .leader_handle()
                .expect("a single node is its own control leader");
            let _ = leader.propose(MetaCommand::SetTableTtl {
                table: TABLE.to_owned(),
                spec: None,
            });
            // Let the driver pick the filler up and start its persist round
            // (well inside that round's slowed append), so the waited-on
            // command lands in the next round rather than sharing this one.
            ctx.env.sleep(Duration::from_millis(500)).await;
            let start = ctx.env.now();
            let committed = ctx
                .propose_and_await(
                    MetaCommand::UpdateContinuousBackups {
                        table: TABLE.to_owned(),
                        enabled: true,
                        wall_ms: ctx.env.wall_now().0,
                    },
                    SCHEMA_COMMIT_TIMEOUT,
                    || async { ctx.metadata_fresh().await.table_pitr(TABLE).map(|_| ()) },
                )
                .await;
            let elapsed = Duration::from_nanos(ctx.env.now().0 - start.0);
            (committed.is_ok(), elapsed)
        })
        .unwrap_or_else(|| panic!("seed={seed}: the commit-wait never resolved"))
}

fn slow_but_live_commit_is_awaited(seed: u64) {
    let (committed, elapsed) = enable_pitr_behind_a_slow_round(seed, SYNC_DELAY);
    assert!(
        committed,
        "seed={seed}: a commit whose pipeline was still advancing timed out after {elapsed:?}"
    );
    assert!(
        elapsed > SCHEMA_COMMIT_TIMEOUT,
        "seed={seed}: the commit took {elapsed:?}, inside the plain timeout, so this run \
         did not exercise a pipeline slower than SCHEMA_COMMIT_TIMEOUT"
    );
}

#[test]
fn slow_but_live_commit_is_awaited_past_the_plain_timeout() {
    slow_but_live_commit_is_awaited(env_seed(0x1292));
}

#[test]
fn slow_but_live_commit_is_awaited_past_the_plain_timeout_over_seeds() {
    for seed in 1..=5 {
        slow_but_live_commit_is_awaited(seed);
    }
}

/// Negative control: a pipeline that makes no progress at all still times
/// out after one plain [`SCHEMA_COMMIT_TIMEOUT`].
#[test]
fn stalled_commit_still_times_out() {
    let seed = env_seed(0x1292);
    let (committed, elapsed) = enable_pitr_behind_a_slow_round(seed, Duration::from_secs(60));
    assert!(!committed, "seed={seed}: nothing could have committed");
    assert!(
        elapsed >= SCHEMA_COMMIT_TIMEOUT && elapsed < SCHEMA_COMMIT_TIMEOUT * 2,
        "seed={seed}: a stalled commit-wait gave up after {elapsed:?}"
    );
}
