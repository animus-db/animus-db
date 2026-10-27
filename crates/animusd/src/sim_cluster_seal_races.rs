//! `SimCluster` regressions for the dueling-seal (audit gap F) and
//! stale-quiesce-veto (gap G) races. See each test's doc for the issue it
//! pins and the constructed window it needs.
//!
//! Gap H (issue #454, `grow_stream` vs a cascade split) lives in
//! `sim_cluster_grow_retired.rs` (issue #1345).
//!
//! # Why a stand-in for the periodic seal arm
//!
//! `index_drain::change_consumer_loop` (the production periodic arm) is a
//! concrete `ProdEnv` loop and `SimCluster` never spawns it. Its seal arm
//! (`seal_tick`) ends in exactly one call, `index_drain::seal_now`
//! (`pitr_tick` -> `pitr_seal_now`), so a dueling seal is reproduced here by
//! running that same function from a sim task that is already in flight when
//! the path under test starts. Nothing about the race depends on `seal_tick`'s
//! size/age trigger; the loss is decided entirely inside `seal_now`.
//!
//! # The dueling-seal window (specification)
//!
//! `seal_now` reads `metadata_fresh()` (on a `Both` node, its local applied
//! catalog), scans the change log, and proposes `SealStreamShard(tablet,
//! next_epoch)`. Two callers that read the catalog before either commit pick
//! the same `next_epoch`; the control log applies the first proposal and
//! rejects the second, and the loser (when its range reaches further than the
//! winner's) returns the retryable `"... lost to a concurrent seal ...;
//! retry"` error.
//!
//! The harness makes the loser's range reach further and keeps its catalog
//! stale by splitting the two planes. The tablet leader `L` and the control
//! leader `C` (moved off `L`) are joined by a slow, jitter-free link
//! (`SLOW`, both directions); the third node `F` stays fast, so the tablet
//! group still commits through `{L, F}`. Then, in virtual time:
//!
//! - `t0`: seal A (the periodic arm's stand-in) scans 3 records and proposes;
//!   the proposal reaches `C` at `t0 + SLOW`.
//! - `t0 + 20 ms`: a fourth write commits on the fast quorum, well before
//!   `t0 + SLOW`.
//! - the instant that write is acknowledged: seal B (the path under test)
//!   starts. Its catalog read cannot contain A's row (A's commit is not even
//!   proposed to `C` yet, and `C -> L` is slow too), so it computes the same
//!   epoch with a range of 4 records, and its proposal queues behind A's on
//!   the same link (no jitter, so no reordering).
//!
//! A wins epoch 0 with 3 records; B must observe that, retry, and seal the
//! residual record under epoch 1. Without the retry B's first `Err` reaches
//! the caller. The end state (epoch 0 holds exactly 3 records, epoch 1
//! exactly 1) is itself the proof that B lost: had B won, epoch 0 would hold
//! 4.

use super::sim_support::env_seed;
use std::time::Duration;

use animus_sim::NetConfig;
use animus_tablet::TabletId;

use super::sim_cluster::SimCluster;

/// One-way delay of the deliberately slow control-plane link.
const SLOW: Duration = Duration::from_millis(300);
/// Records written before the race window opens (the loser's range reaches
/// one record further).
const PRELOAD: u64 = 3;

fn create_table(cluster: &mut SimCluster, node: u64, table: &str, streamed: bool) {
    let stream = if streamed {
        r#","StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}"#
    } else {
        ""
    };
    let body = format!(
        r#"{{"TableName":"{table}",
            "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]{stream}}}"#
    );
    let (status, resp) = cluster.dynamo(node, "DynamoDB_20120810.CreateTable", body.as_bytes());
    assert_eq!(status, 200, "seed={}: CreateTable: {resp}", cluster.seed());
}

fn put_body(table: &str, pk: &str) -> Vec<u8> {
    format!(r#"{{"TableName":"{table}","Item":{{"pk":{{"S":"{pk}"}}}}}}"#).into_bytes()
}

fn disable_stream_body(table: &str) -> Vec<u8> {
    format!(r#"{{"TableName":"{table}","StreamSpecification":{{"StreamEnabled":false}}}}"#)
        .into_bytes()
}

fn slow_link() -> NetConfig {
    let mut cfg = NetConfig::default();
    cfg.base_delay = SLOW;
    cfg.max_jitter = Duration::ZERO;
    cfg
}

/// Advance in 1 ms slices until `slot` fills; the slice bounds the observation
/// error. Panics with the seed if it never fills within `budget`.
fn await_slot<T: Clone>(
    cluster: &mut SimCluster,
    slot: &std::sync::Arc<std::sync::Mutex<Option<T>>>,
    step: Duration,
    budget: Duration,
    what: &str,
) -> T {
    let mut elapsed = Duration::ZERO;
    loop {
        if let Some(v) = slot.lock().expect("slot poisoned").clone() {
            return v;
        }
        assert!(
            elapsed < budget,
            "seed={}: {what} did not resolve within {budget:?}",
            cluster.seed()
        );
        cluster.run_for(step);
        elapsed += step;
    }
}

/// Sealed rows of `tablet` as `(epoch, (start_exclusive, end_inclusive), count)`.
fn sealed_rows(
    cluster: &SimCluster,
    node: u64,
    tablet: TabletId,
    pitr: bool,
) -> Vec<(u64, (u64, u64), u64)> {
    let meta = cluster.metadata(node);
    if pitr {
        meta.pitr_segments
            .range((tablet, 0)..=(tablet, u64::MAX))
            .map(|((_, e), r)| (*e, r.hlc_range, r.count))
            .collect()
    } else {
        meta.stream_shards
            .range((tablet, 0)..=(tablet, u64::MAX))
            .map(|((_, e), r)| (*e, r.hlc_range, r.count))
            .collect()
    }
}

/// The exactly-once oracle shared by every scenario: the tablet's sealed rows
/// partition `total` records into contiguous, non-overlapping ranges.
fn assert_exactly_once(rows: &[(u64, (u64, u64), u64)], total: u64, seed: u64, what: &str) {
    assert!(!rows.is_empty(), "seed={seed}: {what}: no sealed rows");
    assert_eq!(rows[0].1.0, 0, "seed={seed}: {what}: first row {rows:?}");
    for w in rows.windows(2) {
        assert_eq!(
            w[0].0 + 1,
            w[1].0,
            "seed={seed}: {what}: epochs not consecutive: {rows:?}"
        );
        assert_eq!(
            w[0].1.1, w[1].1.0,
            "seed={seed}: {what}: ranges overlap or leave a gap: {rows:?}"
        );
    }
    let sum: u64 = rows.iter().map(|r| r.2).sum();
    assert_eq!(
        sum, total,
        "seed={seed}: {what}: lost or duplicated records: {rows:?}"
    );
}

/// A 3-node cluster with one tablet (`table`), `PRELOAD` records written, the
/// control leader moved off the tablet leader and the L<->C link slowed.
struct Race {
    cluster: SimCluster,
    table: &'static str,
    tablet: TabletId,
    l: u64,
}

fn setup(seed: u64, pitr: bool) -> Race {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "sealrace";
    create_table(&mut cluster, 0, table, !pitr);
    if pitr {
        cluster.enable_pitr(table);
    }
    let tablet = *cluster
        .metadata(0)
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("seed={seed}: no tablet"))
        .0;
    let l = (0..3u64)
        .find(|&n| cluster.is_leader_local(n, tablet))
        .unwrap_or_else(|| panic!("seed={seed}: tablet has no leader"));
    for i in 0..PRELOAD {
        let (s, b) = cluster.dynamo(
            l,
            "DynamoDB_20120810.PutItem",
            &put_body(table, &format!("p{i}")),
        );
        assert_eq!(s, 200, "seed={seed}: preload put: {b}");
    }
    if cluster.control_leader_index() as u64 == l {
        cluster.transfer_control_leadership_to((l + 1) % 3);
    }
    let c = cluster.control_leader_index() as u64;
    assert_ne!(
        c, l,
        "seed={seed}: control leader must be off the tablet leader"
    );
    cluster.set_link_net_config(l, c, slow_link());
    cluster.set_link_net_config(c, l, slow_link());
    Race {
        cluster,
        table,
        tablet,
        l,
    }
}

/// Start seal A on `l` (stream or PITR flavour), write one more record on the
/// fast quorum, and return A's slot once that write is acknowledged, with the
/// precondition (A unresolved, L's catalog without A's row) asserted.
type SealSlot = std::sync::Arc<std::sync::Mutex<Option<Result<Option<u64>, String>>>>;

fn start_a_then_write(r: &mut Race, pitr: bool) -> SealSlot {
    let seed = r.cluster.seed();
    let (table, tablet) = (r.table, r.tablet);
    let a = r.cluster.spawn_detached(r.l, move |ctx| async move {
        let (_, group) = ctx
            .edge
            .hosted_groups()
            .into_iter()
            .find(|(t, _)| *t == tablet)
            .expect("tablet hosted on its leader");
        if pitr {
            crate::index_drain::pitr_seal_now(&ctx, table, tablet, &group).await
        } else {
            crate::index_drain::seal_now(&ctx, table, tablet, &group).await
        }
    });
    r.cluster.run_for(Duration::from_millis(20));
    let w =
        r.cluster
            .spawn_dynamo_detached(r.l, "DynamoDB_20120810.PutItem", &put_body(table, "late"));
    let (status, body) = await_slot(
        &mut r.cluster,
        &w,
        Duration::from_millis(1),
        Duration::from_millis(250),
        "the late write (must commit on the fast quorum inside the window)",
    );
    assert_eq!(status, 200, "seed={seed}: late write: {body}");
    assert!(
        a.lock().expect("slot poisoned").is_none(),
        "seed={seed}: seal A resolved before the window opened (SLOW too short?)"
    );
    assert!(
        sealed_rows(&r.cluster, r.l, tablet, pitr).is_empty(),
        "seed={seed}: L already sees A's row; the window is closed"
    );
    a
}

fn run_forced_seal_loses_to_periodic_arm(seed: u64, pitr: bool) {
    let mut r = setup(seed, pitr);
    let a = start_a_then_write(&mut r, pitr);
    let (table, tablet, l) = (r.table, r.tablet, r.l);
    let what = if pitr {
        "PITR disable"
    } else {
        "stream disable"
    };

    // Path under test, on the tablet leader's own node (CpRoute::Local).
    let outcome: Result<(), String> = if pitr {
        let forced = r.cluster.spawn_detached(l, move |ctx| async move {
            ctx.force_pitr_seal_tablet(tablet).await
        });
        await_slot(
            &mut r.cluster,
            &forced,
            Duration::from_millis(10),
            Duration::from_secs(30),
            what,
        )
    } else {
        let wire = r.cluster.spawn_dynamo_detached(
            l,
            "DynamoDB_20120810.UpdateTable",
            &disable_stream_body(table),
        );
        let (status, body) = await_slot(
            &mut r.cluster,
            &wire,
            Duration::from_millis(10),
            Duration::from_secs(30),
            what,
        );
        if status == 200 {
            Ok(())
        } else {
            Err(format!("{status}: {body}"))
        }
    };
    assert!(
        outcome.is_ok(),
        "seed={seed}: {what} must retry the lost dueling seal, not fail: {outcome:?}"
    );

    // A (the periodic arm's seal) ran to completion and won epoch 0.
    let a_out = await_slot(
        &mut r.cluster,
        &a,
        Duration::from_millis(10),
        Duration::from_secs(30),
        "seal A",
    );
    assert_eq!(a_out, Ok(Some(0)), "seed={seed}: seal A");

    // Converge every node's catalog, then check the exactly-once oracle.
    let mut elapsed = Duration::ZERO;
    let total = PRELOAD + 1;
    loop {
        let settled = (0..3u64).all(|n| {
            sealed_rows(&r.cluster, n, tablet, pitr)
                .iter()
                .map(|x| x.2)
                .sum::<u64>()
                == total
        });
        if settled {
            break;
        }
        assert!(
            elapsed < Duration::from_secs(20),
            "seed={seed}: catalogs did not converge on {total} sealed records: {:?}",
            sealed_rows(&r.cluster, r.l, tablet, pitr)
        );
        r.cluster.run_for(Duration::from_millis(50));
        elapsed += Duration::from_millis(50);
    }
    for n in 0..3u64 {
        let rows = sealed_rows(&r.cluster, n, tablet, pitr);
        assert_exactly_once(&rows, total, seed, what);
        assert_eq!(
            rows.iter().map(|x| (x.0, x.2)).collect::<Vec<_>>(),
            vec![(0, PRELOAD), (1, 1)],
            "seed={seed}: node {n}: epoch 0 must be A's 3 records and epoch 1 B's retried residual"
        );
    }
    if !pitr {
        for n in 0..3u64 {
            assert!(
                r.cluster.metadata(n).table_stream(table).is_none(),
                "seed={seed}: node {n} still reports an enabled stream after disable"
            );
        }
    }
}

/// Issue #572 / PR #574 (stream half): `force_seal_tablet`'s `CpRoute::Local`
/// branch (reached by `UpdateTable(StreamEnabled=false)` on the tablet
/// leader's own node) must retry a final seal that lost the slot to the
/// periodic arm, not surface it as a 500. Without the retry the disable
/// returns `InternalServerError: ... lost to a concurrent seal ...; retry`.
#[test]
fn disable_stream_survives_a_concurrent_periodic_seal_on_the_local_route() {
    run_forced_seal_loses_to_periodic_arm(env_seed(0x5EA1_0001), false);
}

#[test]
fn disable_stream_survives_a_concurrent_periodic_seal_on_the_local_route_over_seeds() {
    for i in 0..5 {
        run_forced_seal_loses_to_periodic_arm(0x5EA1_1000 + i, false);
    }
}

/// Issue #572 / PR #574 (PITR half): `force_pitr_seal_tablet`'s local route,
/// raced against `pitr_seal_now` (the PITR periodic arm's seal).
#[test]
fn disable_pitr_survives_a_concurrent_periodic_seal_on_the_local_route() {
    run_forced_seal_loses_to_periodic_arm(env_seed(0x5EA1_0002), true);
}

#[test]
fn disable_pitr_survives_a_concurrent_periodic_seal_on_the_local_route_over_seeds() {
    for i in 0..5 {
        run_forced_seal_loses_to_periodic_arm(0x5EA1_2000 + i, true);
    }
}

// ---------------------------------------------------------------------------
// The in-place split driver's final seal loop (issues #580 / #582)
// ---------------------------------------------------------------------------

fn link(ms: u64) -> NetConfig {
    let mut cfg = NetConfig::default();
    cfg.base_delay = Duration::from_millis(ms);
    cfg.max_jitter = Duration::ZERO;
    cfg
}

type TickSlot = std::sync::Arc<std::sync::Mutex<Option<Result<(), String>>>>;

/// Drive one `inplace_split_driver_tick` per `Splitting` tablet `node` leads,
/// exactly as `change_consumer_loop` does (a fresh `effective_metadata()` per
/// tick), without blocking the simulator.
fn spawn_driver_ticks(cluster: &SimCluster, node: u64) -> TickSlot {
    cluster.spawn_detached(node, move |ctx| async move {
        let meta = ctx.effective_metadata();
        for (tablet, group) in ctx.edge.hosted_groups() {
            if !group.is_leader() {
                continue;
            }
            let splitting = meta
                .tablets
                .get(&tablet)
                .is_some_and(|t| t.state == animus_tablet::TabletState::Splitting);
            if splitting {
                crate::index_drain::inplace_split_driver_tick(&ctx, &meta, tablet, &group).await?;
            }
        }
        Ok(())
    })
}

/// Issues #580 / #582: `inplace_split_driver_tick`'s final-seal loops
/// (`seal_now` for a streamed table, `pitr_seal_now` for a PITR table) must
/// retry a lost dueling seal in place (`SPLIT_DRIVER_SEAL_RETRIES`), not
/// propagate the first `"... lost to a concurrent seal ...; retry"`, which
/// aborts the whole tick (GSI veto, backfill veto, `CutoverSplit` included).
///
/// Window: the split fork freezes the parent's change log, so the duelling
/// sealer must have scanned an *earlier, shorter* prefix and still win the
/// slot. That is a lagging replica `F` of the tablet (a deposed or lagging
/// leader whose periodic arm still ticks): the L->F data link is slow, so `F`
/// has not yet applied the last write when seal A scans there, and the F->C
/// link is slower still, so A's proposal reaches the control leader only
/// after the split's `BeginSplitInPlace` committed and the fork finished. The
/// C->L link is slow too, so `L`'s catalog is stale when the driver's seal B
/// starts: it sees all 4 records, A (3 records) wins epoch 0, B must retry
/// and seal the residual under epoch 1.
fn run_split_driver_retries_a_lost_dueling_seal(seed: u64, pitr: bool) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "sealrace";
    create_table(&mut cluster, 0, table, !pitr);
    if pitr {
        cluster.enable_pitr(table);
    }
    let tablet = *cluster
        .metadata(0)
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("seed={seed}: no tablet"))
        .0;
    let l = (0..3u64)
        .find(|&n| cluster.is_leader_local(n, tablet))
        .unwrap_or_else(|| panic!("seed={seed}: tablet has no leader"));
    for i in 0..PRELOAD {
        let (s, b) = cluster.dynamo(
            l,
            "DynamoDB_20120810.PutItem",
            &put_body(table, &format!("p{i}")),
        );
        assert_eq!(s, 200, "seed={seed}: preload put: {b}");
    }
    if cluster.control_leader_index() as u64 == l {
        cluster.transfer_control_leadership_to((l + 1) % 3);
    }
    let c = cluster.control_leader_index() as u64;
    let f = (0..3u64).find(|&n| n != l && n != c).expect("third node");
    cluster.set_link_net_config(l, f, link(150));
    cluster.set_link_net_config(f, c, link(3000));
    cluster.set_link_net_config(c, l, link(400));

    // The last write: committed through {L, C}; F applies it ~150-300 ms later.
    let w = cluster.spawn_dynamo_detached(l, "DynamoDB_20120810.PutItem", &put_body(table, "late"));
    let (status, body) = await_slot(
        &mut cluster,
        &w,
        Duration::from_millis(1),
        Duration::from_secs(2),
        "the late write",
    );
    assert_eq!(status, 200, "seed={seed}: late write: {body}");
    cluster.run_for(Duration::from_millis(10));

    // Seal A on the lagging replica (sees 3 of 4 records), then the split.
    let a = cluster.spawn_detached(f, move |ctx| async move {
        let (_, group) = ctx
            .edge
            .hosted_groups()
            .into_iter()
            .find(|(t, _)| *t == tablet)
            .expect("tablet hosted on F");
        if pitr {
            crate::index_drain::pitr_seal_now(&ctx, table, tablet, &group).await
        } else {
            crate::index_drain::seal_now(&ctx, table, tablet, &group).await
        }
    });
    let grow = cluster.spawn_detached(
        l,
        move |ctx| async move { ctx.grow_stream_tablet(tablet).await },
    );
    // Wait (1 ms slices) until A's row is committed at C: L's catalog is then
    // stale by the slow C->L link. Start the driver tick at exactly that
    // instant.
    let mut waited = Duration::ZERO;
    while sealed_rows(&cluster, c, tablet, pitr).is_empty() {
        cluster.run_for(Duration::from_millis(1));
        waited += Duration::from_millis(1);
        assert!(
            waited < Duration::from_secs(20),
            "seed={seed}: A's seal never committed at C (grow={:?})",
            grow.lock().unwrap()
        );
    }
    assert!(
        sealed_rows(&cluster, l, tablet, pitr).is_empty(),
        "seed={seed}: L already sees A's row; the window is closed"
    );
    let tick = spawn_driver_ticks(&cluster, l);
    let first = await_slot(
        &mut cluster,
        &tick,
        Duration::from_millis(5),
        Duration::from_secs(30),
        "first driver tick",
    );
    assert_eq!(
        first,
        Ok(()),
        "seed={seed}: the split driver's first tick must ride out the lost dueling seal \
         (retry in place), not abort"
    );

    // The split then completes, and the parent's shard chain is exactly-once.
    let mut inflight: Vec<TickSlot> = Vec::new();
    let mut elapsed = Duration::ZERO;
    loop {
        inflight.retain(|s| s.lock().expect("slot poisoned").is_none());
        if inflight.is_empty() {
            for n in 0..3u64 {
                inflight.push(spawn_driver_ticks(&cluster, n));
            }
        }
        cluster.run_for(Duration::from_millis(100));
        elapsed += Duration::from_millis(100);
        let done = (0..3u64).all(|n| {
            let m = cluster.metadata(n);
            let ts: Vec<_> = m.tablets_for_table(table).map(|(_, t)| t.state).collect();
            !m.tablets.contains_key(&tablet)
                && ts.len() == 2
                && ts.iter().all(|x| *x == animus_tablet::TabletState::Active)
        });
        if done {
            break;
        }
        assert!(
            elapsed < Duration::from_secs(90),
            "seed={seed}: split did not converge: tablets {:?}, ticks {:?}, rows {:?}",
            cluster
                .metadata(l)
                .tablets_for_table(table)
                .map(|(i, t)| (i.0, t.state))
                .collect::<Vec<_>>(),
            inflight
                .iter()
                .map(|s| s.lock().unwrap().clone())
                .collect::<Vec<_>>(),
            sealed_rows(&cluster, l, tablet, pitr)
        );
    }
    let a_out = await_slot(
        &mut cluster,
        &a,
        Duration::from_millis(10),
        Duration::from_secs(20),
        "seal A",
    );
    assert_eq!(a_out, Ok(Some(0)), "seed={seed}: seal A");
    for n in 0..3u64 {
        let rows = sealed_rows(&cluster, n, tablet, pitr);
        assert_exactly_once(&rows, PRELOAD + 1, seed, "split parent");
        assert_eq!(
            rows.iter().map(|x| (x.0, x.2)).collect::<Vec<_>>(),
            vec![(0, PRELOAD), (1, 1)],
            "seed={seed}: node {n}: epoch 0 = A's 3 records, epoch 1 = the driver's retried residual"
        );
    }
}

#[test]
fn split_driver_retries_a_lost_dueling_stream_seal() {
    run_split_driver_retries_a_lost_dueling_seal(env_seed(0x5EA1_3001), false);
}

#[test]
fn split_driver_retries_a_lost_dueling_stream_seal_over_seeds() {
    for i in 0..5 {
        run_split_driver_retries_a_lost_dueling_seal(0x5EA1_3100 + i, false);
    }
}

#[test]
fn split_driver_retries_a_lost_dueling_pitr_seal() {
    run_split_driver_retries_a_lost_dueling_seal(env_seed(0x5EA1_3002), true);
}

#[test]
fn split_driver_retries_a_lost_dueling_pitr_seal_over_seeds() {
    for i in 0..5 {
        run_split_driver_retries_a_lost_dueling_seal(0x5EA1_3200 + i, true);
    }
}

// ---------------------------------------------------------------------------
// The stale quiesce veto (issues #302 / PR #314)
// ---------------------------------------------------------------------------

/// How long the stand-in change-consumer tick spends on its *other* work (set
/// only after every group has had its first, empty sweep: a group that was
/// never swept keeps the freshness default `u64::MAX`, "no constraint", by
/// design — `Building` children and hidden index tablets are never visited)
/// after the veto sweep (sibling tablets' seal commit-waits, trim, GSI
/// drain): the real loop is one sequential task, so its sweep period is the
/// 200 ms sleep PLUS this, not 200 ms. It is what lets a sweep arrive later
/// than `quiesce_after`.
const SWEEP_BUSY: Duration = Duration::from_millis(2600);

/// The stream-only shape of `change_consumer_loop`'s per-led-tablet body, in
/// the order the real loop runs it: read the applied index BEFORE any scan,
/// skip a quiesced group (the sweeper never revisits it), hold the veto while
/// the change log is non-empty, then run the seal arm (unconditionally here,
/// no size/age gate). The real loop is `ProdEnv`-only and never spawned by
/// `SimCluster`.
async fn sweeper_standin(
    ctx: super::sim_cluster::SimNodeCtx,
    busy_ms: std::sync::Arc<std::sync::atomic::AtomicU64>,
    empty_sweeps: std::sync::Arc<std::sync::atomic::AtomicUsize>,
) {
    use animus_env::Clock;
    loop {
        ctx.env
            .sleep(crate::index_drain::INDEX_DRAIN_INTERVAL)
            .await;
        let meta = ctx.effective_metadata();
        for (tablet, group) in ctx.edge.hosted_groups() {
            if !group.is_leader() {
                continue;
            }
            let veto_fresh_through = group.engine_applied_index();
            if group.is_quiesced() {
                continue;
            }
            let Some(table) = meta.tablets.get(&tablet).and_then(|t| t.table.clone()) else {
                continue;
            };
            if meta.table_stream(&table).is_none() {
                continue;
            }
            let hot = !group.pending_changes_key_order().await.is_empty();
            if !hot {
                empty_sweeps.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            group.set_quiesce_veto(hot, veto_fresh_through);
            if hot {
                let _ = crate::index_drain::seal_now(&ctx, &table, tablet, &group).await;
            }
        }
        ctx.env
            .sleep(Duration::from_millis(
                busy_ms.load(std::sync::atomic::Ordering::SeqCst),
            ))
            .await;
    }
}

fn run_a_stale_veto_never_quiesces_a_group_with_an_unsealed_change_log(seed: u64) {
    const TABLES: u64 = 3;
    // Spaced 700 ms: three points on the 2.8 s sweep circle, so at least one
    // lands in the >2 s arc before the next sweep.
    const WRITE_SPACING: Duration = Duration::from_millis(700);
    let mut cluster =
        SimCluster::new_with_cp_quiescence(seed, 3, 3, Some(crate::MIN_QUIESCE_AFTER));
    let names: Vec<String> = (0..TABLES).map(|i| format!("qveto{i}")).collect();
    // Sweepers first, at the plain 200 ms cadence, so every new group gets its
    // empty sweep (veto released, freshness stamped) while it is still awake.
    let busy_ms = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let empty_sweeps = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    for n in 0..3u64 {
        let (b, e) = (busy_ms.clone(), empty_sweeps.clone());
        let _ = cluster.spawn_detached(n, move |ctx| sweeper_standin(ctx, b, e));
    }
    for name in &names {
        create_table(&mut cluster, 0, name, true);
    }
    assert!(
        empty_sweeps.load(std::sync::atomic::Ordering::SeqCst) >= TABLES as usize,
        "seed={seed}: precondition: every new group must have had an empty sweep"
    );
    // From here the sweeper's period (200 ms + this) exceeds quiesce_after.
    busy_ms.store(
        SWEEP_BUSY.as_millis() as u64,
        std::sync::atomic::Ordering::SeqCst,
    );
    let tablets: Vec<TabletId> = names
        .iter()
        .map(|t| {
            *cluster
                .metadata(0)
                .tablets_for_table(t)
                .next()
                .expect("tablet")
                .0
        })
        .collect();
    let leader_of = |c: &SimCluster, t: TabletId| {
        (0..3u64)
            .find(|&n| c.is_leader_local(n, t))
            .unwrap_or_else(|| panic!("seed={seed}: {t:?} has no leader"))
    };

    // Let every group get one empty sweep (veto released, freshness stamped)
    // and then go idle long enough to quiesce.
    cluster.run_for(Duration::from_secs(6));
    let probe = |cluster: &mut SimCluster, t: TabletId| -> (bool, usize) {
        let l = leader_of(cluster, t);
        let slot = cluster.spawn_detached(l, move |ctx| async move {
            let (_, g) = ctx
                .edge
                .hosted_groups()
                .into_iter()
                .find(|(x, _)| *x == t)
                .expect("hosted");
            (g.is_quiesced(), g.pending_changes_key_order().await.len())
        });
        await_slot(
            cluster,
            &slot,
            Duration::from_millis(1),
            Duration::from_secs(2),
            "probe",
        )
    };
    for &t in &tablets {
        let (quiesced, owed) = probe(&mut cluster, t);
        assert!(
            quiesced && owed == 0,
            "seed={seed}: {t:?} must start quiesced and empty (quiesced={quiesced}, owed={owed})"
        );
    }

    // One write per table, 200 ms apart, so (sweep period 600 ms) at least
    // two land more than quiesce_after before the next sweep.
    let mut writes: Vec<_> = Vec::new();
    let mut violations: Vec<String> = Vec::new();
    for (i, name) in names.iter().enumerate() {
        let l = leader_of(&cluster, tablets[i]);
        writes.push(cluster.spawn_dynamo_detached(
            l,
            "DynamoDB_20120810.PutItem",
            &put_body(name, "w"),
        ));
        cluster.run_for(WRITE_SPACING);
    }
    // Sample the invariant every 5 ms while the sweeper keeps running: a
    // quiesced group must have an empty change log.
    for _ in 0..600 {
        cluster.run_for(Duration::from_millis(5));
        for &t in &tablets {
            let (quiesced, owed) = probe(&mut cluster, t);
            if quiesced && owed > 0 {
                violations.push(format!(
                    "{t:?} quiesced with {owed} unsealed change record(s) at {:?}",
                    cluster.simulator().now()
                ));
            }
        }
        if !violations.is_empty() {
            break;
        }
    }
    for w in &writes {
        let (status, body) = w.lock().unwrap().clone().expect("write resolved");
        assert_eq!(status, 200, "seed={seed}: write: {body}");
    }
    assert!(
        violations.is_empty(),
        "seed={seed}: stale quiesce veto: {violations:?}"
    );

    // Every write's change record reached a sealed shard.
    cluster.run_for(Duration::from_secs(8));
    for (i, &t) in tablets.iter().enumerate() {
        let rows = sealed_rows(&cluster, leader_of(&cluster, t), t, false);
        assert_exactly_once(
            &rows,
            1,
            seed,
            &format!("{} (stranded change record)", names[i]),
        );
    }
}

/// Issues #302 / PR #314: a group whose stream change log is non-empty must
/// not quiesce on a stale "nothing owed" veto observation. The sweeper's last
/// observation (empty log) is older than a write that woke the group; with
/// `quiesce_after` (the product floor, `MIN_QUIESCE_AFTER` = 200 ms) shorter
/// than the sweeper's real period (200 ms sleep + its other work), pre-fix the
/// group re-quiesced before the sweeper looked again and its record was
/// stranded (the sweeper skips quiesced groups). Fixed by the log-index
/// freshness stamp (`set_quiesce_veto(held, fresh_through)`).
#[test]
fn a_stale_veto_never_quiesces_a_group_with_an_unsealed_change_log() {
    run_a_stale_veto_never_quiesces_a_group_with_an_unsealed_change_log(env_seed(0x5EA1_5001));
}

#[test]
fn a_stale_veto_never_quiesces_a_group_with_an_unsealed_change_log_over_seeds() {
    for i in 0..5 {
        run_a_stale_veto_never_quiesces_a_group_with_an_unsealed_change_log(0x5EA1_5100 + i);
    }
}
