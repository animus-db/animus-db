//! `SimCluster` regression for the **transaction-recovery sweep across an
//! in-place split** (issue #298, PR #401's `b296b21`): `txn_resolver_loop`
//! must still drive an undecided transaction to a decision once the tablet
//! that anchors it has forked.
//!
//! # The mechanism (as it exists on `main` today)
//!
//! `txn_resolver_loop` sweeps every led group's `pending_txns()` (records the
//! group anchors and holds `Pending`) and calls `ClientCtx::txn_recover` for
//! each. `txn_recover` first reads the record back through the routed
//! `txn_record_view`: `Ok(None)` is "no record at this key", and only then
//! does the intent-timestamp hint matter. With `None` that branch declines
//! forever; with `Some(created_ts)` (the fix, `b296b21`) it grace-gates and
//! then aborts through `txn_abort_orphan`. The loop used to pass `None`
//! behind an "unreachable by construction" claim that a split invalidates:
//! the record is an ordinary in-scope key of its anchor tablet and rides the
//! fork's clone/trim like every other row, so a lookup for a record the loop
//! enumerated moments earlier can come back empty.
//!
//! # What this module pins
//!
//! The body of `txn_resolver_loop` is `txn_resolver_pending_pass` (split out so this
//! fixture runs the production code; `SimCluster` spawns no background
//! loops). A prepared-but-undecided two-table transaction (the coordinator
//! "crashed" right after prepare, `SimCluster::txn_prepare_only`) has its
//! anchor tablet forked and cut over through the real driver, then a
//! resolver loop runs on every node:
//!
//! - `a_split_anchor_tablet_still_has_its_transaction_recovered`: the real
//!   fork. The record travels to exactly one child; the sweep must commit
//!   the transaction (every span is staged) and both keys must read back.
//!   This passes with either hint: on `main` a split does not lose the
//!   record.
//! - `a_record_the_split_lost_is_aborted_through_the_hint`: the same, plus
//!   the fault `b296b21` hedges against, injected after the cutover (the
//!   record row is removed from every replica of the child that owns it,
//!   exactly "a split misplaced or dropped it"). The group's tracker still
//!   lists the transaction, the routed lookup says `Ok(None)`, and the sweep
//!   must grace-gate and then abort it (an absent record can only abort),
//!   leaving neither write visible. This is the cell that fails with the
//!   hint reverted to `None`: the record stays `Pending` forever.
//!
//! Replay one seed with `ANIMUS_SEED=<seed>`.

use std::time::Duration;

use animus_cp_data::KIND_BASE;
use animus_dynamo::AttributeValue;
use animus_env::{Clock, Metric};
use animus_tablet::{KeyRange, TabletId, TabletState};

use super::dynamo::item_key;
use super::sim_cluster::SimCluster;
use super::sim_cluster_grow_retired::leader_node;
use super::sim_support::{Tick, env_seed, spawn_driver_ticks};
use crate::{
    CpGroup, TXN_RESOLVER_INTERVAL, TxnResolverState, txn_resolver_decided_pass,
    txn_resolver_pending_pass,
};

const ANCHOR: &str = "txr_anchor";
const PART: &str = "txr_part";
const STEP: Duration = Duration::from_millis(250);
/// `RECOVERY_GRACE` is 5 s; a decision plus its resolves follows within a
/// few sweeps.
const RECOVER_BOUND: Duration = Duration::from_secs(40);

/// The production sweep on `node`: the in-doubt pass and the decided-record
/// pass, each every `TXN_RESOLVER_INTERVAL` on a task of its own, forever
/// (`SimEnv` tasks, dropped with the node).
fn spawn_resolver(cluster: &SimCluster, node: u64) {
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

fn live(cluster: &SimCluster, table: &str) -> Vec<(TabletId, TabletState)> {
    cluster
        .metadata(0)
        .tablets_for_table(table)
        .map(|(id, t)| (*id, t.state))
        .collect()
}

/// `(node, tablet)` pairs whose `pending_txns()` currently lists `txn`.
fn pending_on(cluster: &SimCluster, txn: &animus_cp_data::TxnId) -> Vec<(u64, TabletId)> {
    let mut out = Vec::new();
    for n in 0..cluster.node_count() as u64 {
        let ctx = cluster.handle().node_ctx(n);
        for (tablet, group) in ctx.edge.hosted_groups() {
            let listed = match &group {
                CpGroup::Lsm(g) => g.pending_txns().contains_key(txn),
                CpGroup::Mem(g) => g.pending_txns().contains_key(txn),
            };
            if listed {
                out.push((n, tablet));
            }
        }
    }
    out
}

fn metric_sum(cluster: &SimCluster, metric: Metric) -> u64 {
    (0..cluster.node_count() as u64)
        .map(|n| cluster.metric(n, metric))
        .sum()
}

fn run(seed: u64, lose_record: bool) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    create_table(&mut cluster, ANCHOR, seed);
    create_table(&mut cluster, PART, seed);
    let anchor_tablet = live(&cluster, ANCHOR)[0].0;

    // The coordinator "crashes" after prepare: an anchor intent + a Pending
    // record on ANCHOR, a participant intent on PART, no decision.
    let key_a = item_key(&AttributeValue::S("a-key".into()), None);
    let key_b = item_key(&AttributeValue::S("b-key".into()), None);
    let mut end_b = key_b.clone();
    end_b.push(0);
    let (txn, record_key, record_table) = cluster.txn_prepare_only(
        0,
        ANCHOR,
        None,
        vec![(PART.to_owned(), KeyRange::new(key_b.clone(), Some(end_b)))],
        key_a.clone(),
        Some(b"va".to_vec()),
    );
    assert_eq!(record_table, ANCHOR, "seed={seed}");
    cluster.txn_prepare_only(
        0,
        PART,
        Some((txn.clone(), record_key.clone(), record_table)),
        Vec::new(),
        key_b.clone(),
        Some(b"vb".to_vec()),
    );
    assert_eq!(
        pending_on(&cluster, &txn)
            .iter()
            .filter(|(_, t)| *t == anchor_tablet)
            .count(),
        3,
        "seed={seed}: every replica of the anchor tablet tracks the Pending record"
    );

    // The anchor tablet forks (the real reconciler) and is cut over by the
    // real driver while the transaction is still undecided.
    let leader = leader_node(&cluster, anchor_tablet);
    let (s, r) = cluster.admin(
        leader,
        "POST",
        "/admin/tablet/split",
        "",
        format!(r#"{{"tablet":{},"split_key":"m"}}"#, anchor_tablet.0).as_bytes(),
    );
    assert_eq!(s, 200, "seed={seed}: split kickoff: {r}");
    cluster.wait_for_fork_freeze(leader, anchor_tablet, Duration::from_secs(30));
    let mut ticks: Vec<Tick> = Vec::new();
    let mut elapsed = Duration::ZERO;
    loop {
        ticks.retain(|t| t.lock().expect("tick slot").is_none());
        if ticks.is_empty() {
            for n in 0..cluster.node_count() as u64 {
                ticks.push(spawn_driver_ticks(&cluster, n));
            }
        }
        let t = live(&cluster, ANCHOR);
        if t.len() >= 2 && t.iter().all(|(_, s)| *s == TabletState::Active) {
            break;
        }
        assert!(
            elapsed < Duration::from_secs(60),
            "seed={seed}: the anchor split did not cut over: {t:?}"
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    }
    cluster.run_for(Duration::from_secs(2));

    // The record moved to the child owning its token, and is tracked there
    // (on that child's replicas only).
    let owners = pending_on(&cluster, &txn);
    let owner_tablet = owners.first().map(|(_, t)| *t);
    assert!(
        !owners.is_empty()
            && owners.iter().all(|(_, t)| Some(*t) == owner_tablet)
            && owner_tablet != Some(anchor_tablet),
        "seed={seed}: after the fork exactly one child must track the Pending record: {owners:?}"
    );
    let owner_tablet = owner_tablet.expect("checked above");

    if lose_record {
        // The fault: the split dropped the record row on every replica.
        for (n, _) in &owners {
            let engine = cluster.storage(*n, owner_tablet);
            let group = cluster
                .handle()
                .node_ctx(*n)
                .edge
                .local_cp(owner_tablet)
                .expect("owner replica hosted");
            let physical = match &group {
                CpGroup::Lsm(g) => g.physical_key(KIND_BASE, &record_key),
                CpGroup::Mem(g) => g.physical_key(KIND_BASE, &record_key),
            };
            let removed = cluster.run_with_ctx(*n, move |_ctx| async move {
                use animus_storage::StorageEngine;
                let vv = engine.get(&physical).await.expect("engine get");
                // A delete must be newer than everything the engine holds.
                let latest = engine
                    .entries_with_tombstones()
                    .await
                    .expect("engine scan")
                    .into_iter()
                    .map(|(_, _, v)| v)
                    .max()
                    .unwrap_or(0);
                match vv {
                    Some(_) => {
                        engine
                            .delete(&physical, latest + 1)
                            .await
                            .expect("engine delete");
                        true
                    }
                    None => false,
                }
            });
            assert_eq!(
                removed,
                Some(true),
                "seed={seed}: the record row must exist on node {n} before the injected loss"
            );
        }
    }

    // The sweep, on every node, from here on.
    let committed0 = metric_sum(&cluster, Metric::CpTxnRecoveredCommitted);
    let aborted0 = metric_sum(&cluster, Metric::CpTxnRecoveredAborted);
    for n in 0..cluster.node_count() as u64 {
        spawn_resolver(&cluster, n);
    }
    let want = if lose_record {
        Metric::CpTxnRecoveredAborted
    } else {
        Metric::CpTxnRecoveredCommitted
    };
    let base = if lose_record { aborted0 } else { committed0 };
    let mut waited = Duration::ZERO;
    // After a real commit the apply clears the tracker; an orphan abort
    // (the injected-loss cell) writes a tombstone for a record the tracker
    // never saw decided, so its stale `pending` entry lingers (harmless: the
    // next sweeps see the tombstone and return `Aborted`), and only the
    // decision metric is a signal there.
    while metric_sum(&cluster, want) <= base
        || (!lose_record
            && pending_on(&cluster, &txn)
                .iter()
                .any(|(_, t)| *t == owner_tablet))
    {
        assert!(
            waited < RECOVER_BOUND,
            "seed={seed} lose_record={lose_record}: the sweep did not decide the transaction \
             within {RECOVER_BOUND:?} of virtual time (committed {} -> {}, aborted {} -> {}, \
             still tracked Pending on {:?}): the record is stuck",
            committed0,
            metric_sum(&cluster, Metric::CpTxnRecoveredCommitted),
            aborted0,
            metric_sum(&cluster, Metric::CpTxnRecoveredAborted),
            pending_on(&cluster, &txn),
        );
        cluster.run_for(STEP);
        waited += STEP;
    }
    eprintln!(
        "seed={seed} lose_record={lose_record}: leader={leader} owner={owner_tablet:?} \
         decided_after={waited:?}"
    );

    // Atomic outcome, by the decision.
    cluster.run_for(Duration::from_secs(3));
    let a = cluster
        .raw_get(1, ANCHOR, key_a.clone(), true)
        .unwrap_or_else(|e| panic!("seed={seed}: read a: {e}"));
    let b = cluster
        .raw_get(2, PART, key_b.clone(), true)
        .unwrap_or_else(|e| panic!("seed={seed}: read b: {e}"));
    if lose_record {
        assert_eq!(
            (a, b),
            (None, None),
            "seed={seed}: an aborted transaction leaves neither write visible"
        );
    } else {
        assert_eq!(
            (a, b),
            (Some(b"va".to_vec()), Some(b"vb".to_vec())),
            "seed={seed}: the recovered commit makes both writes visible"
        );
    }
}

#[test]
fn a_split_anchor_tablet_still_has_its_transaction_recovered() {
    run(env_seed(0x0401_1001), false);
}

#[test]
fn a_record_the_split_lost_is_aborted_through_the_hint() {
    run(env_seed(0x0401_1002), true);
}

#[test]
fn split_anchor_recovery_over_seeds() {
    if std::env::var("ANIMUS_SEED").is_ok() {
        return;
    }
    for i in 0..3 {
        run(0x0401_1100 + i, false);
        run(0x0401_1200 + i, true);
    }
}
