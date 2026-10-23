//! `SimCluster` regression for the GSI-drain **frozen-endgame self-deadlock**
//! (issue #298, PR #401's `8ccea7f`): the split driver's GSI-drain veto must
//! never block its own node's loop on a write only that same loop can
//! unblock.
//!
//! # The mechanism (as it exists on `main` today)
//!
//! `index_drain::inplace_split_driver_tick` is the per-tablet step of
//! `change_consumer_loop`: once a tablet has forked (frozen, `Splitting`) it
//! must drain the table's GSIs before it may retire. A drain pass writes the
//! GSI rows (`reconcile_partition` -> `cp_kind_write_raw_once`) into the
//! table's hidden **index table**, a different tablet. Under a cascade that
//! index tablet can itself be forked and frozen at the same moment, and when
//! both tablets are led by one node, only that node's own driver loop can
//! cut the index tablet over, and the loop only reaches it after returning
//! from the base tablet's tick. A write that retries a frozen refusal for
//! `CLIENT_TIMEOUT` (10 s) per pass, 64 passes, is a self-inflicted stall of
//! minutes; `cp_kind_write_raw_once` fails fast (a `"; retry"` error,
//! `index_drain::is_retryable_elsewhere`) so the tick returns, the loop moves
//! on to the index tablet, and the next tick's write succeeds.
//!
//! The copy-based split's `Freeze` endgame is gone (2026-09-01); the in-place
//! fork keeps the identical shape (ADR 0058: the parent is frozen from the
//! fork until `CutoverSplit`, and the fork runs the same GSI-drain veto).
//!
//! # The window (built deliberately)
//!
//! RF 3 on three nodes; `users` has a GSI so a second, hidden tablet
//! (`users$by-email`'s) exists. Both tablets are moved under one leader node
//! `L`, items are written (the GSI rows are NOT drained: this fixture runs no
//! `change_consumer_loop`), and both tablets are split through
//! `POST /admin/tablet/split` and forked by the always-on reconciler, so both
//! sit frozen and `Splitting`. The base tablet has the lower id, so `L`'s
//! driver loop (`BTreeMap` order) reaches it first. A single driver task per
//! node then runs, exactly as the production loop does.
//!
//! Oracles: both tables converge to `Active` children within
//! [`CONVERGE_BOUND`] of virtual time (well under one `CLIENT_TIMEOUT`, which
//! the unfixed blocking write alone costs), the base tablet's first tick
//! returns within [`FIRST_TICK_BOUND`], and after convergence every GSI
//! partition answers a query with all its items (nothing lost to the
//! early return).
//!
//! Replay one seed with `ANIMUS_SEED=<seed>`.

use std::time::Duration;

use animus_tablet::{TabletId, TabletState};

use super::sim_cluster::SimCluster;
use super::sim_cluster_grow_retired::{leader_node, move_leader};

const TABLE: &str = "users";
const INDEX: &str = "by-email";
const STEP: Duration = Duration::from_millis(50);
const ITEMS: usize = 12;
const EMAILS: usize = 4;
/// Convergence of both splits. The product's per-write retry budget is
/// `CLIENT_TIMEOUT` (10 s); the fix costs a few driver ticks.
const CONVERGE_BOUND: Duration = Duration::from_secs(8);
/// The base tablet's tick must not wait out a frozen index tablet.
const FIRST_TICK_BOUND: Duration = Duration::from_secs(5);

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

type Tick = std::sync::Arc<std::sync::Mutex<Option<Result<(), String>>>>;

/// One `inplace_split_driver_tick` per `Splitting` tablet `node` leads, in
/// hosted-group order: what `change_consumer_loop` does in one pass.
fn spawn_driver_ticks(cluster: &SimCluster, node: u64) -> Tick {
    cluster.spawn_detached(node, move |ctx| async move {
        let meta = ctx.effective_metadata();
        for (tablet, group) in ctx.edge.hosted_groups() {
            if !group.is_leader() {
                continue;
            }
            let splitting = meta
                .tablets
                .get(&tablet)
                .is_some_and(|t| t.state == TabletState::Splitting);
            if splitting {
                crate::index_drain::inplace_split_driver_tick(&ctx, &meta, tablet, &group).await?;
            }
        }
        Ok(())
    })
}

fn tablets_of(cluster: &SimCluster, table: &str) -> Vec<(TabletId, TabletState)> {
    cluster
        .metadata(0)
        .tablets_for_table(table)
        .map(|(id, t)| (*id, t.state))
        .collect()
}

fn converged(cluster: &SimCluster, table: &str) -> bool {
    let t = tablets_of(cluster, table);
    t.len() >= 2 && t.iter().all(|(_, s)| *s == TabletState::Active)
}

fn run(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        format!(
            r#"{{"TableName":"{TABLE}",
                "AttributeDefinitions":[{{"AttributeName":"email","AttributeType":"S"}},
                                        {{"AttributeName":"id","AttributeType":"S"}}],
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
                "GlobalSecondaryIndexes":[
                    {{"IndexName":"{INDEX}",
                      "KeySchema":[{{"AttributeName":"email","KeyType":"HASH"}}],
                      "Projection":{{"ProjectionType":"ALL"}}}}]}}"#
        )
        .as_bytes(),
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable: {body}");
    let index_table = animus_dynamo::index_table_name(TABLE, INDEX);
    let base = tablets_of(&cluster, TABLE)[0].0;
    let put = |cluster: &mut SimCluster, i: usize| {
        let (s, b) = cluster.dynamo(
            0,
            "DynamoDB_20120810.PutItem",
            format!(
                r#"{{"TableName":"{TABLE}","Item":{{"id":{{"S":"u{i:02}"}},"email":{{"S":"e{}"}}}}}}"#,
                i % EMAILS
            )
            .as_bytes(),
        );
        assert_eq!(s, 200, "seed={seed}: put u{i:02}: {b}");
    };
    // The hidden index table's tablet is provisioned lazily by the first
    // drain, so drain once (a first batch), then dirty the partitions again:
    // nothing else drains them in this fixture.
    for i in 0..ITEMS / 2 {
        put(&mut cluster, i);
    }
    let base_leader = leader_node(&cluster, base);
    cluster.drain_gsi(base_leader, TABLE);
    let index = tablets_of(&cluster, &index_table)
        .first()
        .unwrap_or_else(|| panic!("seed={seed}: the first drain must provision the index tablet"))
        .0;
    assert!(
        base < index,
        "seed={seed}: the driver loop must reach the base tablet first ({base:?} < {index:?})"
    );
    for i in ITEMS / 2..ITEMS {
        put(&mut cluster, i);
    }

    // One leader for both tablets: only that node's loop can cut either over.
    let lb = leader_node(&cluster, base);
    if leader_node(&cluster, index) != lb {
        move_leader(&mut cluster, index, lb);
    }
    assert_eq!(leader_node(&cluster, base), lb, "seed={seed}");
    assert_eq!(leader_node(&cluster, index), lb, "seed={seed}");

    for tablet in [base, index] {
        let (s, r) = cluster.admin(
            lb,
            "POST",
            "/admin/tablet/split",
            "",
            format!(r#"{{"tablet":{},"split_key":"m"}}"#, tablet.0).as_bytes(),
        );
        assert_eq!(s, 200, "seed={seed}: split kickoff for {tablet:?}: {r}");
    }
    // Both forked (frozen) before any driver tick runs.
    cluster.wait_for_fork_freeze(lb, base, Duration::from_secs(30));
    cluster.wait_for_fork_freeze(lb, index, Duration::from_secs(30));
    for tablet in [base, index] {
        let state = cluster.metadata(lb).tablets.get(&tablet).map(|t| t.state);
        assert_eq!(
            state,
            Some(TabletState::Splitting),
            "seed={seed}: {tablet:?} must still be Splitting (frozen, awaiting cutover)"
        );
    }

    // The production loop, one driver task per node, until both converge.
    let mut ticks: Vec<Tick> = Vec::new();
    let mut elapsed = Duration::ZERO;
    let mut first_tick: Option<Duration> = None;
    let lb_first = spawn_driver_ticks(&cluster, lb);
    loop {
        if first_tick.is_none() && lb_first.lock().expect("tick slot").is_some() {
            first_tick = Some(elapsed);
        }
        ticks.retain(|t| t.lock().expect("tick slot").is_none());
        if ticks.is_empty() && first_tick.is_some() {
            for n in 0..cluster.node_count() as u64 {
                ticks.push(spawn_driver_ticks(&cluster, n));
            }
        }
        if converged(&cluster, TABLE) && converged(&cluster, &index_table) {
            break;
        }
        assert!(
            elapsed < CONVERGE_BOUND,
            "seed={seed}: base and index splits did not both converge within {CONVERGE_BOUND:?} \
             (base tablets {:?}, index tablets {:?}, first tick returned at {first_tick:?}): the \
             base tablet's GSI-drain write is blocking the one loop that can cut the frozen \
             index tablet over",
            tablets_of(&cluster, TABLE),
            tablets_of(&cluster, &index_table),
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    }
    let first_tick = first_tick.unwrap_or(elapsed);
    assert!(
        first_tick <= FIRST_TICK_BOUND,
        "seed={seed}: the base tablet's first driver tick took {first_tick:?} (> \
         {FIRST_TICK_BOUND:?}): it waited out a frozen index tablet instead of failing fast"
    );
    eprintln!(
        "seed={seed}: leader={lb} first_tick={first_tick:?} converged_after={elapsed:?} \
         base={base:?} index={index:?}"
    );
    let first = lb_first.lock().expect("tick slot").clone();
    assert!(
        matches!(first, Some(Ok(()))),
        "seed={seed}: first tick result {first:?}"
    );

    // Nothing lost: drain the leftovers and every GSI partition is whole.
    cluster.run_for(Duration::from_secs(3));
    for n in 0..cluster.node_count() as u64 {
        cluster.drain_gsi(n, TABLE);
    }
    for e in 0..EMAILS {
        let (s, b) = cluster.dynamo(
            1,
            "DynamoDB_20120810.Query",
            format!(
                r#"{{"TableName":"{TABLE}","IndexName":"{INDEX}",
                    "KeyConditionExpression":"email = :e",
                    "ExpressionAttributeValues":{{":e":{{"S":"e{e}"}}}}}}"#
            )
            .as_bytes(),
        );
        assert_eq!(s, 200, "seed={seed}: query e{e}: {b}");
        let want = (0..ITEMS).filter(|i| i % EMAILS == e).count();
        assert!(
            b.contains(&format!("\"Count\":{want}")),
            "seed={seed}: GSI partition e{e} must hold {want} items: {b}"
        );
    }
}

#[test]
fn a_frozen_index_tablet_never_stalls_the_base_tablets_gsi_drain_loop() {
    run(env_seed(0x0401_0001));
}

#[test]
fn a_frozen_index_tablet_never_stalls_the_gsi_drain_loop_seed2() {
    run(env_seed(0x0401_0002));
}

#[test]
fn a_frozen_index_tablet_never_stalls_the_gsi_drain_loop_over_seeds() {
    if std::env::var("ANIMUS_SEED").is_ok() {
        return;
    }
    for i in 0..4 {
        run(0x0401_0100 + i);
    }
}
