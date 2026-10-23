//! `SimCluster` regression for **issue #1369**: an in-doubt transaction's
//! recovery must not wait behind the decided-record backlog.
//!
//! # The mechanism
//!
//! A tablet group's `TxnTracker` is rebuilt from its record rows when a
//! replica restarts, and every decided record becomes "decided but not yet
//! locally resolved" (`unresolved_decided`): nothing durable says a resolve
//! already ran. The first sweep on the node that then leads the group
//! proposes one `TxnResolve` per record, serially, for the group's whole
//! history. `txn_resolver_loop` used to do that inside the same task, in
//! tablet order, as its in-doubt (`Pending`) recovery. The soak (3 nodes, a
//! node killed every 120 s, ~6500 records of backlog by epoch 25) therefore
//! left a `Pending` record anchored on a later tablet unrecovered for
//! minutes, and a consistent read of a key under its intent answered nothing
//! for 60 s.
//!
//! # What this module pins
//!
//! One node leads two tablets: a low-id one holding `BACKLOG` decided,
//! unresolved records (decided through the anchor, never resolved), and a
//! higher-id one holding one stale `Pending` record. Links carry a WAN-like
//! delay so every resolve costs real virtual time. The production sweep
//! (`txn_resolver_pending_pass` and `txn_resolver_decided_pass` on separate
//! tasks, exactly `lib.rs`'s `txn_resolver_loop` and `txn_decided_sweep_loop`)
//! must recover the `Pending` record within `PENDING_BOUND`, a small fraction
//! of the time the backlog takes. The negative control runs both passes on
//! ONE task, decided first (the old layout for this tablet order) and must
//! blow the bound.
//!
//! Replay one seed with `ANIMUS_SEED=<seed>`.

use std::time::Duration;

use animus_cp_data::hlc::HlcTimestamp;
use animus_dynamo::AttributeValue;
use animus_env::{Clock, Metric};
use animus_sim::NetConfig;
use animus_tablet::TabletId;

use super::dynamo::item_key;
use super::sim_cluster::SimCluster;
use super::sim_cluster_grow_retired::move_leader;
use crate::{
    TXN_RESOLVER_INTERVAL, TxnResolverState, txn_resolver_decided_pass, txn_resolver_pending_pass,
};

const BACKLOG_TABLE: &str = "txb_backlog";
const PENDING_TABLE: &str = "txb_pending";
/// Decided-but-unresolved records on the low-id tablet.
const BACKLOG: usize = 120;
const STEP: Duration = Duration::from_millis(250);
/// The in-doubt record must be decided this soon after the sweep starts:
/// one resolver tick plus the verify/decide round trips, far below the
/// backlog sweep (`BACKLOG` x two round trips of 2 x `LINK_DELAY` and more).
const PENDING_BOUND: Duration = Duration::from_secs(8);
const LINK_DELAY: Duration = Duration::from_millis(25);

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Layout {
    /// What `lib.rs` runs: the two passes on separate tasks.
    Production,
    /// Negative control: one task, the decided pass before the in-doubt pass.
    OneTaskDecidedFirst,
}

fn create_table(cluster: &mut SimCluster, name: &str, seed: u64) {
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        format!(
            r#"{{"TableName":"{name}","KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
                "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]}}"#
        )
        .as_bytes(),
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable {name}: {body}");
}

fn only_tablet(cluster: &SimCluster, table: &str) -> TabletId {
    let meta = cluster.metadata(0);
    let mut it = meta.tablets_for_table(table).map(|(id, _)| *id);
    let id = it.next().expect("table has a tablet");
    assert!(it.next().is_none(), "single-tablet table expected");
    id
}

fn spawn_sweep(cluster: &SimCluster, node: u64, layout: Layout) {
    match layout {
        Layout::Production => {
            let _pending = cluster.spawn_detached(node, |ctx| async move {
                let mut state = TxnResolverState::default();
                loop {
                    ctx.env.sleep(TXN_RESOLVER_INTERVAL).await;
                    txn_resolver_pending_pass(&ctx, &mut state).await;
                }
            });
            let _decided = cluster.spawn_detached(node, |ctx| async move {
                let mut state = TxnResolverState::default();
                loop {
                    ctx.env.sleep(TXN_RESOLVER_INTERVAL).await;
                    txn_resolver_decided_pass(&ctx, &mut state).await;
                }
            });
        }
        Layout::OneTaskDecidedFirst => {
            let _both = cluster.spawn_detached(node, |ctx| async move {
                let mut state = TxnResolverState::default();
                loop {
                    ctx.env.sleep(TXN_RESOLVER_INTERVAL).await;
                    txn_resolver_decided_pass(&ctx, &mut state).await;
                    txn_resolver_pending_pass(&ctx, &mut state).await;
                }
            });
        }
    }
}

/// Runs the scenario and returns how long after the sweep started the
/// `Pending` record was decided (`None`: not within `limit`).
fn run(seed: u64, layout: Layout, limit: Duration) -> Option<Duration> {
    let mut cluster = SimCluster::new(seed, 3, 3);
    create_table(&mut cluster, BACKLOG_TABLE, seed);
    create_table(&mut cluster, PENDING_TABLE, seed);
    let low = only_tablet(&cluster, BACKLOG_TABLE);
    let high = only_tablet(&cluster, PENDING_TABLE);
    assert!(
        low < high,
        "seed={seed}: the backlog tablet must sort first"
    );
    // One node leads both, so one resolver walks both.
    move_leader(&mut cluster, low, 0);
    move_leader(&mut cluster, high, 0);

    // The stale in-doubt record: prepared, coordinator gone, never decided.
    let key_p = item_key(&AttributeValue::S("pending".into()), None);
    cluster.txn_prepare_only(
        0,
        PENDING_TABLE,
        None,
        Vec::new(),
        key_p,
        Some(b"p".to_vec()),
    );

    // The backlog: prepared and decided through the anchor, never resolved.
    for i in 0..BACKLOG {
        let key = item_key(&AttributeValue::S(format!("b{i}")), None);
        let (txn, record_key, _table) = cluster.txn_prepare_only(
            0,
            BACKLOG_TABLE,
            None,
            Vec::new(),
            key,
            Some(format!("v{i}").into_bytes()),
        );
        cluster
            .txn_decide_anchor(
                0,
                BACKLOG_TABLE,
                txn,
                record_key,
                true,
                HlcTimestamp::zero(),
            )
            .unwrap_or_else(|e| panic!("seed={seed}: decide backlog {i}: {e}"));
    }
    // Well past RECOVERY_GRACE, so the record is eligible from the first sweep.
    cluster.run_for(Duration::from_secs(6));

    let mut slow = NetConfig::default();
    slow.base_delay = LINK_DELAY;
    slow.max_jitter = Duration::from_millis(5);
    for a in 0..3u64 {
        for b in 0..3u64 {
            if a != b {
                cluster.set_link_net_config(a, b, slow.clone());
            }
        }
    }

    let before = cluster.metric(0, Metric::CpTxnRecoveredCommitted)
        + cluster.metric(0, Metric::CpTxnRecoveredAborted);
    spawn_sweep(&cluster, 0, layout);
    let mut waited = Duration::ZERO;
    while waited < limit {
        cluster.run_for(STEP);
        waited += STEP;
        let now = cluster.metric(0, Metric::CpTxnRecoveredCommitted)
            + cluster.metric(0, Metric::CpTxnRecoveredAborted);
        if now > before {
            eprintln!("seed={seed} {layout:?}: pending record decided after {waited:?}");
            return Some(waited);
        }
    }
    eprintln!("seed={seed} {layout:?}: pending record NOT decided within {limit:?}");
    None
}

#[test]
fn pending_recovery_is_not_delayed_by_the_decided_backlog() {
    let seed = env_seed(0x1369_0001);
    let got = run(seed, Layout::Production, PENDING_BOUND);
    assert!(
        got.is_some(),
        "seed={seed}: the in-doubt record was not recovered within {PENDING_BOUND:?} of virtual \
         time while {BACKLOG} decided records were awaiting their resolve sweep (issue #1369)"
    );
}

#[test]
fn control_one_task_with_the_backlog_first_starves_pending_recovery() {
    let seed = env_seed(0x1369_0002);
    let got = run(seed, Layout::OneTaskDecidedFirst, PENDING_BOUND);
    assert!(
        got.is_none(),
        "seed={seed}: negative control did not bite: with one task sweeping the backlog first the \
         in-doubt record was still decided within {PENDING_BOUND:?} ({got:?}); the scenario no \
         longer exercises the starvation, so the real test above proves nothing"
    );
}
