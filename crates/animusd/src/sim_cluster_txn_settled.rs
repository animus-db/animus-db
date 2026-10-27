//! `SimCluster` regression for **issue #1380**: a decided transaction record
//! that every participant has already resolved must be *settled* once
//! (`Gate::TxnSettled`, `KvCommand::TxnSettle`), so a restart never re-resolves
//! it, a follower's tracker drains, the group can quiesce, and the stub is
//! reaped after `TXN_SETTLED_REAP_AGE`.
//!
//! # The mechanism being fixed
//!
//! A tablet group's `TxnTracker` is rebuilt at start from its record rows, and
//! every decided record became "decided, not yet locally resolved"
//! (`unresolved_decided`): nothing durable said a resolve already ran. After a
//! restart the decided pass proposed one `TxnResolve` per record of the group's
//! whole history, a follower's tracker never drained (it only sees resolves
//! that apply on its own tablet), and a non-empty tracker vetoes quiescence.
//!
//! # What this pins
//!
//! Three nodes (RF 3, real `LsmEngine`s so a restart reopens its disk, CP
//! quiescence on), two tables, `TXNS` two-tablet transactions (alternately
//! committed and aborted) decided through the anchor and resolved on both
//! participants. The production decided pass (`txn_resolver_decided_pass`, as
//! `lib.rs`'s `txn_decided_sweep_loop` runs it) runs on every node.
//!
//! * **fixed**: with the gate open every replica of the anchor tablet ends with
//!   no unresolved/unsettled record and `TXNS` stubs; every node is restarted
//!   and the rebuilt trackers are *still* empty (stubs aside); no entry is
//!   proposed on the anchor group afterwards (its commit index does not move);
//!   a status lookup of a stub still answers
//!   `Committed`/`Aborted`; committed values stay readable and aborted ones
//!   restore the prior state; the stubs survive until the reap age and are
//!   deleted (record row gone on every replica) after it.
//! * **control** (the pre-fix behaviour, gate never opened): after the restart
//!   every replica's tracker holds all `TXNS` records again, the decided pass
//!   re-proposes work on the anchor group.
//!
//! Quiescence itself is not asserted here: a restarted `SimCluster` group never
//! re-quiesces, with or without transactions (probed on a txn-free cluster), so
//! the veto's input -- the rebuilt tracker -- is asserted directly instead.
//!
//! Replay one seed with `ANIMUS_SEED=<seed>`.

use std::time::Duration;

use animus_control::sim_versions::BinaryProfile;
use animus_control::version::{Gate, MAX_SUPPORTED, VersionRange};
use animus_cp_data::hlc::HlcTimestamp;
use animus_cp_data::{TxnDecisionStatus, TxnOutcome};
use animus_dynamo::AttributeValue;
use animus_env::Clock;
use animus_tablet::{KeyRange, TabletId};

use super::dynamo::item_key;
use super::sim_cluster::SimCluster;
use super::sim_world_mrec_overrides_tests::open_overrides_gate;
use super::sim_world_mrec_tests::open_mrec_gate;
use crate::config::NodeRole;
use crate::{
    TXN_RESOLVER_INTERVAL, TxnResolverState, txn_resolver_decided_pass, txn_resolver_pending_pass,
};

const TABLE_A: &str = "txs_a";
const TABLE_B: &str = "txs_b";
const NODES: u64 = 3;
/// Two-tablet transactions: even ones commit, odd ones abort.
const TXNS: usize = 10;
/// Filler writes that push the anchor group's log past its compaction threshold.
const FILLER_PUTS: usize = 150;
const QUIESCE_AFTER: Duration = Duration::from_secs(5);
const STEP: Duration = Duration::from_millis(500);

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

struct Txn {
    record_key: Vec<u8>,
    key_a: Vec<u8>,
    key_b: Vec<u8>,
    commit: bool,
}

/// Run the production decided + in-doubt passes on every node (a restart drops
/// the previous tasks, so this is re-called after one).
fn spawn_sweeps(cluster: &SimCluster) {
    for node in 0..NODES {
        let _decided = cluster.spawn_detached(node, |ctx| async move {
            let mut state = TxnResolverState::default();
            loop {
                ctx.env.sleep(TXN_RESOLVER_INTERVAL).await;
                txn_resolver_decided_pass(&ctx, &mut state).await;
            }
        });
        let _pending = cluster.spawn_detached(node, |ctx| async move {
            let mut state = TxnResolverState::default();
            loop {
                ctx.env.sleep(TXN_RESOLVER_INTERVAL).await;
                txn_resolver_pending_pass(&ctx, &mut state).await;
            }
        });
    }
}

fn wait_for(
    cluster: &mut SimCluster,
    seed: u64,
    what: &str,
    limit: Duration,
    cond: impl Fn(&SimCluster) -> bool,
) {
    let mut waited = Duration::ZERO;
    while !cond(cluster) {
        assert!(
            waited < limit,
            "seed={seed}: timed out after {limit:?} waiting for {what}: {:?}",
            (0..NODES)
                .map(|n| (cluster.group_states(n), cluster.version_halt(n)))
                .collect::<Vec<_>>()
        );
        cluster.run_for(STEP);
        waited += STEP;
    }
}

fn leaders_up(cluster: &SimCluster, tablets: &[TabletId]) -> bool {
    tablets
        .iter()
        .all(|t| cluster.leader_index_of(*t).is_some())
}

fn counts(
    cluster: &SimCluster,
    node: u64,
    tablet: TabletId,
) -> Option<(animus_cp_data::TxnTrackerCounts, u64)> {
    cluster.txn_tracker_state(node, tablet)
}

/// `pred` over every replica's tracker counts of `tablet`.
fn all_replicas(
    cluster: &SimCluster,
    tablet: TabletId,
    pred: impl Fn(&animus_cp_data::TxnTrackerCounts) -> bool,
) -> bool {
    (0..NODES).all(|n| counts(cluster, n, tablet).is_some_and(|(c, _)| pred(&c)))
}

/// Stage, decide (through the anchor) and resolve `TXNS` two-tablet
/// transactions, exactly the three steps `ClientCtx::cp_txn` runs.
fn run_txns(cluster: &mut SimCluster, seed: u64) -> Vec<Txn> {
    let mut out = Vec::new();
    for i in 0..TXNS {
        let key_a = item_key(&AttributeValue::S(format!("a{i}")), None);
        let key_b = item_key(&AttributeValue::S(format!("b{i}")), None);
        let commit = i % 2 == 0;
        let span_b = KeyRange::new(
            key_b.clone(),
            Some({
                let mut end = key_b.clone();
                end.push(0);
                end
            }),
        );
        let (id, record_key, record_table) = cluster.txn_prepare_only(
            0,
            TABLE_A,
            None,
            vec![(TABLE_B.to_string(), span_b)],
            key_a.clone(),
            Some(format!("va{i}").into_bytes()),
        );
        cluster.txn_prepare_only(
            0,
            TABLE_B,
            Some((id.clone(), record_key.clone(), record_table)),
            Vec::new(),
            key_b.clone(),
            Some(format!("vb{i}").into_bytes()),
        );
        let outcome = cluster
            .txn_decide_anchor(
                0,
                TABLE_A,
                id.clone(),
                record_key.clone(),
                commit,
                HlcTimestamp::zero(),
            )
            .unwrap_or_else(|e| panic!("seed={seed}: decide {i}: {e}"));
        assert_eq!(
            matches!(outcome, TxnOutcome::Committed { .. }),
            commit,
            "seed={seed}: txn {i} decided {outcome:?}"
        );
        for (table, key) in [(TABLE_A, &key_a), (TABLE_B, &key_b)] {
            cluster
                .txn_resolve_participant(
                    0,
                    table,
                    id.clone(),
                    record_key.clone(),
                    vec![key.clone()],
                    outcome.clone(),
                )
                .unwrap_or_else(|e| panic!("seed={seed}: resolve {i} on {table}: {e}"));
        }
        out.push(Txn {
            record_key,
            key_a,
            key_b,
            commit,
        });
    }
    out
}

/// The virtual time on node 0's clock, in milliseconds (the clock the HLC and
/// the reaper's age test run on).
fn now_ms(cluster: &mut SimCluster) -> u64 {
    cluster
        .run_with_ctx(0, |ctx| async move { ctx.env.now().0 / 1_000_000 })
        .expect("clock read completed")
}

fn read(cluster: &mut SimCluster, table: &str, key: &[u8]) -> Option<Vec<u8>> {
    cluster
        .raw_get(0, table, key.to_vec(), true)
        .unwrap_or_else(|e| panic!("read {table}: {e}"))
}

/// Committed values are readable, aborted ones left nothing behind.
fn check_data(cluster: &mut SimCluster, seed: u64, txns: &[Txn]) {
    for (i, t) in txns.iter().enumerate() {
        let (a, b) = (
            read(cluster, TABLE_A, &t.key_a),
            read(cluster, TABLE_B, &t.key_b),
        );
        if t.commit {
            assert_eq!(
                a,
                Some(format!("va{i}").into_bytes()),
                "seed={seed}: txn {i}"
            );
            assert_eq!(
                b,
                Some(format!("vb{i}").into_bytes()),
                "seed={seed}: txn {i}"
            );
        } else {
            assert_eq!(
                (a, b),
                (None, None),
                "seed={seed}: aborted txn {i} left a value"
            );
        }
    }
}

/// A status lookup (what a late participant/pusher does) of every record.
fn lookup(
    cluster: &mut SimCluster,
    txns: &[Txn],
) -> Vec<Result<Option<animus_cp_data::TxnRecordView>, String>> {
    txns.iter()
        .map(|t| {
            let rk = t.record_key.clone();
            cluster
                .run_with_ctx(0, move |ctx| async move {
                    ctx.txn_record_view(TABLE_A, &rk).await
                })
                .expect("lookup completed")
        })
        .collect()
}

/// Enough ordinary writes on `table` that the group compacts its Raft log past
/// every txn entry (`COMPACT_THRESHOLD` is 64 entries): after that a restart can
/// only learn a record's history from the record row itself, which is exactly
/// the production situation (hours of history, a log tail of a few hundred
/// entries) the bug lives in.
fn compact_past_the_txns(cluster: &mut SimCluster, table: &str, seed: u64) {
    for i in 0..FILLER_PUTS {
        cluster
            .put(0, table, "filler", &format!("{i}"), b"x")
            .unwrap_or_else(|e| panic!("seed={seed}: filler put {i} on {table}: {e}"));
    }
    cluster.run_for(Duration::from_secs(5));
}

fn wake(cluster: &mut SimCluster, round: u32) {
    for table in [TABLE_A, TABLE_B] {
        cluster
            .put(0, table, "wake", &format!("{round}"), b"x")
            .unwrap_or_else(|e| panic!("wake put on {table}: {e}"));
    }
}

#[derive(Debug)]
struct Report {
    /// Every anchor-replica tracker, right after the txns settled (or not).
    settled_counts: Vec<animus_cp_data::TxnTrackerCounts>,
    /// After restarting every node and letting the sweeps run.
    restarted_counts: Vec<animus_cp_data::TxnTrackerCounts>,
    /// Anchor-group commit index growth on its leader across the post-restart
    /// observation window.
    entries_after_restart: u64,
}

fn run(seed: u64, gate: bool) -> Report {
    let mut cluster = SimCluster::new_with_lsm_engines(
        seed,
        &[NodeRole::Both; NODES as usize],
        NODES as usize,
        Some(QUIESCE_AFTER),
    );
    if gate {
        open_mrec_gate(&mut cluster, seed);
        open_overrides_gate(&mut cluster, seed);
        assert!(
            (0..NODES).all(|n| cluster.features(n).is_open(Gate::TxnSettled)),
            "seed={seed}: TxnSettled gate did not open"
        );
    }
    let a = cluster.create_table(TABLE_A);
    let b = cluster.create_table(TABLE_B);
    let tablets = [a, b];
    wait_for(
        &mut cluster,
        seed,
        "leaders",
        Duration::from_secs(30),
        |c| leaders_up(c, &tablets),
    );

    let txns = run_txns(&mut cluster, seed);
    // Before any sweep nothing is settled, whatever the gate: every record is
    // decided and tracked.
    let leader = cluster.leader_index_of(a).expect("anchor leader");
    let (c, _) = counts(&cluster, leader, a).expect("anchor replica");
    assert_eq!(c.unsettled, TXNS, "seed={seed}: {c:?}");
    assert_eq!(c.settled, 0, "seed={seed}: {c:?}");

    spawn_sweeps(&cluster);
    if gate {
        wait_for(
            &mut cluster,
            seed,
            "every replica to settle every record",
            Duration::from_secs(120),
            |c| all_replicas(c, a, |t| t.unsettled == 0 && t.settled == TXNS),
        );
    } else {
        cluster.run_for(Duration::from_secs(30));
    }
    // An upper bound on when the stubs were written (the reap age runs from the
    // `TxnSettle` entries' HLC; the sim burns minutes of virtual time per
    // client call, so the checkpoints below are measured, not assumed).
    let t_settled_ms = now_ms(&mut cluster);
    let settled_counts: Vec<_> = (0..NODES)
        .map(|n| counts(&cluster, n, a).expect("replica").0)
        .collect();
    check_data(&mut cluster, seed, &txns);

    // The stub (or, without the gate, the decided record) answers the lookup.
    for (i, (t, view)) in txns.iter().zip(lookup(&mut cluster, &txns)).enumerate() {
        let view = view
            .unwrap_or_else(|e| panic!("seed={seed}: lookup {i}: {e}"))
            .unwrap_or_else(|| panic!("seed={seed}: txn {i}'s record is gone"));
        match (&view.status, t.commit) {
            (TxnDecisionStatus::Committed { .. }, true) | (TxnDecisionStatus::Aborted, false) => {}
            other => panic!("seed={seed}: txn {i}: status {other:?}"),
        }
        if gate {
            assert!(
                view.intent_spans.is_empty(),
                "seed={seed}: a stub keeps no spans"
            );
        }
    }

    compact_past_the_txns(&mut cluster, TABLE_A, seed);
    // Restart every node, one at a time as a roll would. `SimCluster::restart`
    // re-applies the node's *binary profile* (default: a Phase 1 binary, which
    // an era-active cluster's peers refuse), so an era-on cluster names the
    // current release first -- the same pair of calls the mixed-version corpus
    // makes when it "rolls" a node.
    for node in 0..NODES {
        if gate {
            cluster.set_binary_profile(node, BinaryProfile::Release(MAX_SUPPORTED));
        }
        cluster.restart(node);
        if gate {
            cluster.set_node_version(node, Some(VersionRange::new(1, MAX_SUPPORTED)));
        }
        wait_for(
            &mut cluster,
            seed,
            "the restarted node's groups to rejoin",
            Duration::from_secs(60),
            |c| {
                leaders_up(c, &tablets)
                    && (0..NODES).all(|n| {
                        counts(c, n, a).is_some()
                            && c.group_states(n).iter().all(|s| !s.contains("Candidate"))
                    })
            },
        );
    }
    // Let the election's own entries (the new leader's no-op) commit, then
    // baseline the anchor group *before* the sweeps start: everything after is
    // the decided pass's doing.
    cluster.run_for(Duration::from_secs(5));
    let restarted_counts: Vec<_> = (0..NODES)
        .map(|n| counts(&cluster, n, a).expect("replica").0)
        .collect();
    let leader = cluster.leader_index_of(a).expect("anchor leader");
    let before = counts(&cluster, leader, a).expect("leader").1;
    spawn_sweeps(&cluster);
    cluster.run_for(Duration::from_secs(40));
    let leader_after = cluster.leader_index_of(a).expect("anchor leader");
    assert_eq!(
        leader, leader_after,
        "seed={seed}: the anchor group changed leader inside the observation window"
    );
    let entries_after_restart = counts(&cluster, leader, a).expect("leader").1 - before;

    let report = Report {
        settled_counts,
        restarted_counts,
        entries_after_restart,
    };
    if gate {
        check_data(&mut cluster, seed, &txns);
        reap_phase(&mut cluster, seed, a, &txns, t_settled_ms);
    }
    report
}

/// The reaper: stubs survive until `TXN_SETTLED_REAP_AGE`, then go.
fn reap_phase(cluster: &mut SimCluster, seed: u64, a: TabletId, txns: &[Txn], t_settled_ms: u64) {
    let age_ms = animus_cp_data::TXN_SETTLED_REAP_AGE.as_millis() as u64;
    // Young: the groups are idle (quiesced); a wake-up write runs the reaper
    // pass, which must find every stub too young to delete.
    wake(cluster, 1);
    cluster.run_for(Duration::from_secs(15));
    let elapsed_ms = now_ms(cluster) - t_settled_ms;
    assert!(
        elapsed_ms + 60_000 < age_ms,
        "seed={seed}: the scenario burned {elapsed_ms} ms before the young checkpoint; it no \
         longer proves the stubs survive until the reap age ({age_ms} ms)"
    );
    assert!(
        all_replicas(cluster, a, |t| t.settled == TXNS),
        "seed={seed}: stubs were reaped {elapsed_ms} ms after settling, before the reap age: {:?}",
        (0..NODES)
            .map(|n| counts(cluster, n, a))
            .collect::<Vec<_>>()
    );
    // Past the age (and a margin): the next wake-up reaps them everywhere.
    cluster.run_for(Duration::from_millis(age_ms - elapsed_ms) + Duration::from_secs(60));
    wake(cluster, 2);
    wait_for(
        cluster,
        seed,
        "every replica to reap every stub",
        Duration::from_secs(120),
        |c| all_replicas(c, a, |t| t.settled == 0),
    );
    for node in 0..NODES {
        for (i, t) in txns.iter().enumerate() {
            let bytes = cluster.txn_record_bytes_on(node, a, t.record_key.clone());
            assert!(
                bytes.is_none(),
                "seed={seed}: node {node} still holds txn {i}'s record row"
            );
        }
    }
    let views = lookup(cluster, txns);
    assert!(
        views.iter().all(|v| matches!(v, Ok(None))),
        "seed={seed}: reaped records must be absent: {views:?}"
    );
    // The data the transactions wrote is untouched by the reap.
    check_data(cluster, seed, txns);
}

#[test]
fn settled_records_are_not_re_resolved_after_a_restart_and_drain_everywhere() {
    let seed = env_seed(0x1380_0001);
    let r = run(seed, true);
    eprintln!("seed={seed} fixed: {r:?}");
    for (n, c) in r.settled_counts.iter().enumerate() {
        assert_eq!(
            (c.unsettled, c.unresolved_decided, c.settled),
            (0, 0, TXNS),
            "seed={seed}: node {n} after settling: {c:?}"
        );
    }
    for (n, c) in r.restarted_counts.iter().enumerate() {
        assert_eq!(
            (c.pending, c.unresolved_decided, c.unsettled, c.settled),
            (0, 0, 0, TXNS),
            "seed={seed}: node {n}'s rebuilt tracker must hold stubs only: {c:?}"
        );
    }
    assert_eq!(
        r.entries_after_restart, 0,
        "seed={seed}: the anchor group proposed entries after the restart (settled records were \
         re-resolved)"
    );
}

/// Negative control: the same scenario without the gate is the pre-fix
/// behaviour. If this ever stops re-resolving, the scenario no longer
/// exercises the bug and the real test above proves nothing.
#[test]
fn control_without_the_gate_re_resolves_every_record_after_a_restart() {
    let seed = env_seed(0x1380_0002);
    let r = run(seed, false);
    eprintln!("seed={seed} control: {r:?}");
    for (n, c) in r.restarted_counts.iter().enumerate() {
        assert_eq!(
            c.unresolved_decided, TXNS,
            "seed={seed}: node {n}: the pre-fix rebuild must re-queue every decided record: {c:?}"
        );
        assert_eq!(c.settled, 0, "seed={seed}: node {n}: {c:?}");
    }
    assert!(
        r.entries_after_restart > 0,
        "seed={seed}: the pre-fix decided pass must re-propose resolves after a restart"
    );
}
