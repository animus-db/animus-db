//! `SimCluster`-driven deterministic coverage for the auto-split **BYTE**
//! trigger (ADR 0034) — C-04 / ADR 0061 rung D4 PR 2.
//!
//! `auto_split_loop` (`lib.rs`) — the leader-driven background trigger that
//! proposes an in-place split (`ClientCtx::trigger_split` → `MetaCommand::
//! BeginSplitInPlace`) once a led tablet's bytes exceed a configured
//! threshold — was concrete `ClientCtx`/`tokio::time::Instant` (i.e.
//! `ProdEnv`-only) before this rung. It is now `<E: Env, R: RelayClient>`-
//! generic, with its two per-tablet cooldown/confirm-cadence maps re-typed
//! from `BTreeMap<TabletId, tokio::time::Instant>` to `BTreeMap<TabletId,
//! animus_env::Nanos>` (read via `ctx.env.now()`), so it is drivable under
//! `SimEnv` — see that function's own doc for the exact conversion and
//! `crates/animusd/CLAUDE.md`'s matching entry for the signature-count
//! accounting.
//!
//! **`SimCluster::set_auto_split_thresholds` (`sim_cluster.rs`) is the new
//! opt-in knob**, spawning `auto_split_loop` on EVERY node right now —
//! mirroring `SimCluster::new`'s own `heartbeat_loop` spawn (D4 PR 1)
//! exactly — and storing the configuration so [`SimCluster::restart`]
//! respawns it identically on a restarted node. **Defaulted OFF**: every
//! existing scenario across every other `sim_cluster_*` module never calls
//! it, so it is unaffected by this rung.
//!
//! **A second, smaller widening was needed to make the fork's own CUTOVER
//! reachable at all**: since ADR 0058, the CP data plane's real `host::
//! Reconciler` (already running here, D4 PR 1) only forks the tablet
//! (`KvCommand::SplitTablet`, minting both children's engines) — the
//! control-plane `MetaCommand::CutoverSplit` that actually activates both
//! children and retires the parent is proposed by a SEPARATE, previously
//! `ProdEnv`-only per-node loop, `index_drain::change_consumer_loop`
//! (`inplace_split_driver_tick`), which `SimCluster` has never spawned (see
//! `SimClusterHandle::recompute_any_table_throughput_all`'s own doc for the
//! identical reason it never spawns the whole loop). Rather than widen and
//! spawn that whole loop (out of this rung's own scope — it also drives
//! Streams/PITR/GSI-drain machinery this fixture has no need to exercise),
//! `inplace_split_driver_tick`/`gsi_caught_up` were widened to `<E: Env, R:
//! RelayClient>`/`<E: Env>` (their own callees — `seal_now`/`pitr_seal_now`/
//! `drain_tablet`/`ClientCtx::propose_schema` — were already generic since
//! rung C5 step 3b) and [`SimCluster::drive_inplace_split_cutover`] drives
//! ONE pass of it per call, mirroring [`SimCluster::drain_gsi`]'s own
//! manual-drive shape. A scenario below polls it alongside `run_for` until
//! convergence — see [`poll_split_converged`]'s own doc.
//!
//! **Nothing here fires unless virtual time actually advances past
//! `AUTO_SPLIT_INTERVAL` (2s) + `AUTO_SPLIT_COOLDOWN` (15s)** — every
//! scenario below explicitly `run_for`s (directly or via a poll loop) well
//! past both, stated per scenario. `ANIMUS_SEED=<seed> cargo test -p
//! animusd --lib <test name>` replays any one of them (repo convention).
//!
//! **Scenarios** (seed-parameterized, replayed at 5 seeds each via a
//! `_over_seeds` sibling): (a) write past a small byte threshold on one
//! table's sole tablet — exactly one fork: two `Active` children, no
//! `Splitting` parent, every pre-split key still readable through the wire;
//! (b) below the threshold, nothing splits over a long window; (c) after
//! one fork, modest further writes (still below threshold on both
//! children) cause no second fork, and a genuine burst that pushes a child
//! back over the SAME threshold does trigger a further, independent fork —
//! proving the widened `Nanos`-keyed cooldown/confirm map handles a tablet
//! id minted mid-run (a freshly-forked child), not just one present at loop
//! start; (d) a non-leader node's own `ctx.edge.cp_leader(tablet)` gate
//! answers `None` (structurally, the precondition every node's copy of the
//! loop relies on to skip), and a leadership move mid-window (the original
//! leader crashed) still yields exactly one fork, driven by the newly
//! elected leader's own loop instance; (e) a node crashed before the fork
//! ever starts, restarted only after the fork has fully converged
//! elsewhere, itself converges too — no zombie groups, and its own
//! leftover PARENT engine is reclaimed via the issue #722 fix (`host::
//! Reconciler`'s `EngineFactory::local_tablets` second fact source).

use std::time::Duration;

use animus_storage::StorageEngine;
use animus_tablet::{TabletId, TabletState};

use super::AutoSplitThresholds;
use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// A single-hash-key (`pk`, string) `CreateTable`, issued from `node` —
/// mirrors `sim_cluster_dynamo_drop_table.rs`'s own identically-named
/// helper.
fn create_table(cluster: &mut SimCluster, node: u64, table: &str) -> (u16, String) {
    let body = format!(
        r#"{{"TableName":"{table}",
            "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]}}"#
    );
    cluster.dynamo(node, "DynamoDB_20120810.CreateTable", body.as_bytes())
}

/// `table`'s own tablet id, per `node`'s own view of `Metadata` — panics if
/// `node` sees no tablet for `table`. **Reads `Metadata` directly, never
/// `SimCluster::tablet_of`**: that accessor only knows about a table
/// [`SimCluster::create_table_with_replication`] hand-hosted (its own
/// `TabletInfo` bookkeeping) — a wire-provisioned table (every table this
/// module creates) never has an entry there at all, the identical gotcha
/// `SimClusterHandle::leader_index_of`'s own doc names for `replicas_of`.
/// Mirrors `sim_cluster_dynamo_drop_table.rs`'s own identically-named
/// helper.
fn tablet_of(cluster: &SimCluster, node: u64, table: &str) -> TabletId {
    let meta = cluster.metadata(node);
    *meta
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("table {table} has no tablet on node {node}'s own view"))
        .0
}

/// `PutItem` with a `pad` string attribute of `pad_len` bytes — the byte
/// trigger's own knob: a caller controls exactly how many total bytes a
/// batch of keys contributes by choosing `pad_len` and the key count
/// together.
fn put_item(
    cluster: &mut SimCluster,
    node: u64,
    table: &str,
    pk: &str,
    pad_len: usize,
) -> (u16, String) {
    let pad = "x".repeat(pad_len);
    let body = format!(
        r#"{{"TableName":"{table}","Item":{{"pk":{{"S":"{pk}"}},"pad":{{"S":"{pad}"}}}}}}"#
    );
    cluster.dynamo(node, "DynamoDB_20120810.PutItem", body.as_bytes())
}

/// [`put_item`], retried while the target tablet is transiently frozen
/// mid-split-cutover (`"tablet frozen for split cutover ... ; retry"`,
/// `index_drain::is_retryable_elsewhere`'s own house convention) — a real,
/// expected condition once the auto-split loop is armed and this fixture's
/// own `SimCluster::dynamo`/`put`/etc. calls advance virtual time
/// internally (`spawn_and_capture`'s own `run_for`), so a burst of writes
/// issued AFTER [`SimCluster::set_auto_split_thresholds`] can genuinely
/// land on a tablet mid-fork. **Drives [`SimCluster::
/// drive_inplace_split_cutover`] on every id in `nodes` on each retry** —
/// without it the freeze would never clear at all (this fixture never
/// spawns that driver as a background loop, see the module doc), so a
/// plain wait-and-retry would spin for `MAX_ATTEMPTS` and then fail.
/// Panics on a non-transient failure, or if the freeze never clears within
/// `MAX_ATTEMPTS` short waits.
fn put_item_retry(
    cluster: &mut SimCluster,
    node: u64,
    nodes: &[u64],
    table: &str,
    pk: &str,
    pad_len: usize,
) {
    const MAX_ATTEMPTS: u32 = 30;
    for attempt in 0..MAX_ATTEMPTS {
        let (status, body) = put_item(cluster, node, table, pk, pad_len);
        if status == 200 {
            return;
        }
        assert!(
            body.contains("; retry"),
            "seed={}: PutItem({pk}) on node {node} failed non-transiently: {body}",
            cluster.seed()
        );
        assert!(
            attempt + 1 < MAX_ATTEMPTS,
            "seed={}: PutItem({pk}) on node {node} kept hitting a transient split-freeze \
             after {MAX_ATTEMPTS} attempts: {body}",
            cluster.seed()
        );
        for &n in nodes {
            cluster.drive_inplace_split_cutover(n);
        }
        cluster.run_for(Duration::from_millis(200));
    }
}

fn get_item(cluster: &mut SimCluster, node: u64, table: &str, pk: &str) -> (u16, String) {
    let body =
        format!(r#"{{"TableName":"{table}","ConsistentRead":true,"Key":{{"pk":{{"S":"{pk}"}}}}}}"#);
    cluster.dynamo(node, "DynamoDB_20120810.GetItem", body.as_bytes())
}

fn scan_count(cluster: &mut SimCluster, node: u64, table: &str) -> (u16, usize) {
    let body = format!(r#"{{"TableName":"{table}","ConsistentRead":true}}"#);
    let (status, resp) = cluster.dynamo(node, "DynamoDB_20120810.Scan", body.as_bytes());
    if status != 200 {
        return (status, 0);
    }
    let count: usize = resp
        .split("\"Count\":")
        .nth(1)
        .and_then(|s| s.split([',', '}']).next())
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or_else(|| panic!("Scan response has no \"Count\" field: {resp}"));
    (status, count)
}

/// `AUTO_SPLIT_INTERVAL` + `AUTO_SPLIT_COOLDOWN` (17s) plus headroom — the
/// floor every convergence poll below budgets AT LEAST this much virtual
/// time for, per the module doc's own "nothing fires unless virtual time
/// actually advances" rule.
const SETTLE_FLOOR: Duration = Duration::from_secs(20);

/// Poll `run_for(STEP)` interleaved with [`SimCluster::drive_inplace_split_
/// cutover`] on every node (the cutover driver this fixture never spawns as
/// a background loop — see the module doc) until `table` shows exactly
/// TWO `Active` tablets and zero `Splitting` ones, on EVERY id in `nodes`,
/// or `budget` is exceeded. Never a one-shot assert — a fork's own
/// materialization (the host reconciler) and its cutover (this poll's own
/// manual drive) each need their own tick to land.
fn poll_split_converged(cluster: &mut SimCluster, table: &str, nodes: &[u64], budget: Duration) {
    const STEP: Duration = Duration::from_millis(100);
    let seed = cluster.seed();
    let mut elapsed = Duration::ZERO;
    loop {
        for &n in nodes {
            cluster.drive_inplace_split_cutover(n);
        }
        let mut converged = true;
        for &n in nodes {
            let meta = cluster.metadata(n);
            let mut active = 0;
            let mut splitting = 0;
            for (_, t) in meta.tablets_for_table(table) {
                match t.state {
                    TabletState::Active => active += 1,
                    TabletState::Splitting => splitting += 1,
                    _ => {}
                }
            }
            if active != 2 || splitting != 0 {
                converged = false;
                break;
            }
        }
        if converged {
            return;
        }
        assert!(
            elapsed < budget,
            "split of {table} did not converge to two Active children within \
             {budget:?} (seed={seed})"
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    }
}

/// Poll the same way as [`poll_split_converged`], but for "MORE than
/// `min_active` `Active` tablets, zero `Splitting`" — used by scenario (c),
/// which doesn't pin an exact final tablet count (a burst spread across
/// many new keys may cross the threshold on one or both freshly-forked
/// children).
fn poll_more_than_converged(
    cluster: &mut SimCluster,
    table: &str,
    nodes: &[u64],
    min_active: usize,
    budget: Duration,
) {
    const STEP: Duration = Duration::from_millis(100);
    let seed = cluster.seed();
    let mut elapsed = Duration::ZERO;
    loop {
        for &n in nodes {
            cluster.drive_inplace_split_cutover(n);
        }
        let mut converged = true;
        for &n in nodes {
            let meta = cluster.metadata(n);
            let mut active = 0;
            let mut splitting = 0;
            for (_, t) in meta.tablets_for_table(table) {
                match t.state {
                    TabletState::Active => active += 1,
                    TabletState::Splitting => splitting += 1,
                    _ => {}
                }
            }
            if active <= min_active || splitting != 0 {
                converged = false;
                break;
            }
        }
        if converged {
            return;
        }
        assert!(
            elapsed < budget,
            "{table} did not converge to more than {min_active} Active tablets within \
             {budget:?} (seed={seed})"
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    }
}

/// Total `Active`-tablet count for `table`, per `node`'s own view.
fn active_tablet_count(cluster: &SimCluster, node: u64, table: &str) -> usize {
    cluster
        .metadata(node)
        .tablets_for_table(table)
        .filter(|(_, t)| t.state == TabletState::Active)
        .count()
}

const BYTES_THRESHOLD: u64 = 2_000;
const PAD_LEN: usize = 300;
const NUM_KEYS: usize = 8;

fn thresholds() -> AutoSplitThresholds {
    AutoSplitThresholds {
        bytes: Some(BYTES_THRESHOLD),
        change_rate: None,
        ops_rate: None,
        tablet_capacity_ceilings: Default::default(),
    }
}

// ---------------------------------------------------------------------------
// Scenario (a): write past the threshold on one table's sole tablet — an
// exactly-once fork, both children serving, every pre-split key still
// readable.
// ---------------------------------------------------------------------------

fn run_a_byte_threshold_crossing_forks_exactly_once(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, 0, "orders");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let parent = tablet_of(&cluster, 0, "orders");

    let keys: Vec<String> = (0..NUM_KEYS).map(|i| format!("k{i}")).collect();
    for pk in &keys {
        let (status, body) = put_item(&mut cluster, 0, "orders", pk, PAD_LEN);
        assert_eq!(status, 200, "seed={seed}: PutItem({pk}) failed: {body}");
    }

    cluster.set_auto_split_thresholds(thresholds());

    let nodes: Vec<u64> = (0..cluster.node_count() as u64).collect();
    // Past AUTO_SPLIT_INTERVAL so the loop's first tick runs, plus the
    // in-place-split materialize-settle gate (250ms) and headroom for the
    // control-plane commit + host reconciler fallback.
    poll_split_converged(&mut cluster, "orders", &nodes, SETTLE_FLOOR);

    // Exactly one fork: the ORIGINAL parent id is gone from the tablet map
    // entirely (retired at cutover), and exactly two DIFFERENT children
    // exist.
    let children: Vec<TabletId> = cluster
        .metadata(0)
        .tablets_for_table("orders")
        .map(|(&id, _)| id)
        .collect();
    assert_eq!(
        children.len(),
        2,
        "seed={seed}: expected exactly two children, got {children:?}"
    );
    assert!(
        !children.contains(&parent),
        "seed={seed}: the parent tablet {parent:?} must be retired, not among {children:?}"
    );

    // Every pre-split key is still readable, through the wire, on every
    // node (proves both children serve reads for their own key halves —
    // a key that landed on the "wrong" child would read back absent or
    // wrong).
    for &node in &nodes {
        for pk in &keys {
            let (status, body) = get_item(&mut cluster, node, "orders", pk);
            assert_eq!(
                status, 200,
                "seed={seed}: GetItem({pk}) on node {node} failed: {body}"
            );
            assert!(
                body.contains(&format!(r#""pk":{{"S":"{pk}"}}"#)),
                "seed={seed}: GetItem({pk}) on node {node} missing the key: {body}"
            );
        }
        let (status, count) = scan_count(&mut cluster, node, "orders");
        assert_eq!(status, 200, "seed={seed}: Scan on node {node} failed");
        assert_eq!(
            count, NUM_KEYS,
            "seed={seed}: Scan on node {node} should see all {NUM_KEYS} pre-split rows"
        );
    }
}

#[test]
fn a_byte_threshold_crossing_forks_exactly_once() {
    run_a_byte_threshold_crossing_forks_exactly_once(env_seed(0xA5F1_0001));
}

#[test]
fn a_byte_threshold_crossing_forks_exactly_once_over_seeds() {
    for i in 0..5 {
        run_a_byte_threshold_crossing_forks_exactly_once(0xA5F1_1000 + i);
    }
}

// ---------------------------------------------------------------------------
// Scenario (b): below the threshold, nothing splits over a long window.
// ---------------------------------------------------------------------------

fn run_b_below_threshold_never_splits(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, 0, "small");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    // Two tiny items — nowhere close to BYTES_THRESHOLD.
    for pk in ["a", "b"] {
        let (status, body) = put_item(&mut cluster, 0, "small", pk, 8);
        assert_eq!(status, 200, "seed={seed}: PutItem({pk}) failed: {body}");
    }

    cluster.set_auto_split_thresholds(thresholds());

    // A long window: several AUTO_SPLIT_INTERVAL ticks AND past
    // AUTO_SPLIT_COOLDOWN, with driving the cutover path too (a no-op here,
    // since nothing ever forks) — mirrors the poll shape above without the
    // convergence assertion, since "stays at one tablet" is what's being
    // proven.
    const STEP: Duration = Duration::from_millis(500);
    let mut elapsed = Duration::ZERO;
    while elapsed < SETTLE_FLOOR {
        cluster.drive_inplace_split_cutover(0);
        cluster.run_for(STEP);
        elapsed += STEP;
    }

    let count = active_tablet_count(&cluster, 0, "small");
    assert_eq!(
        count, 1,
        "seed={seed}: a table whose bytes never crossed the threshold must \
         never split (got {count} Active tablets)"
    );
    let meta = cluster.metadata(0);
    assert!(
        meta.tablets_for_table("small")
            .all(|(_, t)| t.state == TabletState::Active),
        "seed={seed}: no tablet should ever have entered Splitting"
    );
}

#[test]
fn b_below_threshold_never_splits() {
    run_b_below_threshold_never_splits(env_seed(0xA5F1_0002));
}

#[test]
fn b_below_threshold_never_splits_over_seeds() {
    for i in 0..5 {
        run_b_below_threshold_never_splits(0xA5F1_2000 + i);
    }
}

// ---------------------------------------------------------------------------
// Scenario (c): after one fork, modest writes don't cause a second one; a
// genuine burst that pushes a child back over the SAME threshold does.
// ---------------------------------------------------------------------------

fn run_c_a_regrown_child_forks_again(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, 0, "growing");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    let keys: Vec<String> = (0..NUM_KEYS).map(|i| format!("k{i}")).collect();
    for pk in &keys {
        let (status, body) = put_item(&mut cluster, 0, "growing", pk, PAD_LEN);
        assert_eq!(status, 200, "seed={seed}: PutItem({pk}) failed: {body}");
    }

    cluster.set_auto_split_thresholds(thresholds());
    let nodes: Vec<u64> = (0..cluster.node_count() as u64).collect();
    poll_split_converged(&mut cluster, "growing", &nodes, SETTLE_FLOOR);
    assert_eq!(
        active_tablet_count(&cluster, 0, "growing"),
        2,
        "seed={seed}: the first fork must have converged before this scenario continues"
    );

    // Modest further writes — well below BYTES_THRESHOLD on their own —
    // over a window past AUTO_SPLIT_COOLDOWN: no second fork. Reuses
    // `poll_split_converged`'s own two-Active/zero-Splitting shape as a
    // plain settle-and-check (not a convergence wait — the point is that
    // NOTHING changes).
    for i in 0..2 {
        let pk = format!("small{i}");
        put_item_retry(&mut cluster, 0, &nodes, "growing", &pk, 8);
    }
    const STEP: Duration = Duration::from_millis(500);
    let mut elapsed = Duration::ZERO;
    while elapsed < SETTLE_FLOOR {
        for &n in &nodes {
            cluster.drive_inplace_split_cutover(n);
        }
        cluster.run_for(STEP);
        elapsed += STEP;
    }
    assert_eq!(
        active_tablet_count(&cluster, 0, "growing"),
        2,
        "seed={seed}: modest writes below threshold must not trigger a second fork"
    );

    // A genuine burst, spread across many NEW keys, big enough that at
    // least one of the two current children clearly crosses
    // BYTES_THRESHOLD on its own accumulated bytes.
    let more_keys: Vec<String> = (0..NUM_KEYS * 2).map(|i| format!("burst{i}")).collect();
    for pk in &more_keys {
        put_item_retry(&mut cluster, 0, &nodes, "growing", pk, PAD_LEN);
    }
    poll_more_than_converged(&mut cluster, "growing", &nodes, 2, SETTLE_FLOOR);

    // Every original + new key is still readable across however many
    // leaves the table now has.
    for pk in keys.iter().chain(more_keys.iter()) {
        let (status, body) = get_item(&mut cluster, 0, "growing", pk);
        assert_eq!(status, 200, "seed={seed}: GetItem({pk}) failed: {body}");
        assert!(
            body.contains(&format!(r#""pk":{{"S":"{pk}"}}"#)),
            "seed={seed}: GetItem({pk}) missing the key: {body}"
        );
    }
}

#[test]
fn c_a_regrown_child_forks_again() {
    run_c_a_regrown_child_forks_again(env_seed(0xA5F1_0003));
}

#[test]
fn c_a_regrown_child_forks_again_over_seeds() {
    for i in 0..5 {
        run_c_a_regrown_child_forks_again(0xA5F1_3000 + i);
    }
}

// ---------------------------------------------------------------------------
// Scenario (d): leader-gated (a non-leader's own `cp_leader` gate is
// `None`), and a leadership move mid-window still yields exactly one fork.
// ---------------------------------------------------------------------------

fn run_d_leadership_move_mid_window_still_forks_exactly_once(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, 0, "moved");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let tablet = tablet_of(&cluster, 0, "moved");

    // Already over threshold BEFORE the loop's first tick — every node's
    // `auto_split_loop` will find the condition true on tick 1, whichever
    // one ends up leading by then.
    let keys: Vec<String> = (0..NUM_KEYS).map(|i| format!("k{i}")).collect();
    for pk in &keys {
        let (status, body) = put_item(&mut cluster, 0, "moved", pk, PAD_LEN);
        assert_eq!(status, 200, "seed={seed}: PutItem({pk}) failed: {body}");
    }

    let original_leader = cluster
        .leader_index_of(tablet)
        .unwrap_or_else(|| panic!("seed={seed}: {tablet:?} has no leader"));

    // Structural proof of the leader gate itself: exactly one node
    // currently believes it leads; every other node's own
    // `ctx.edge.cp_leader(tablet)` — the precondition `auto_split_loop`
    // checks before ever calling `trigger_split` — answers `None`.
    for node in 0..cluster.node_count() as u64 {
        let believes_leader = cluster.is_leader_local(node, tablet);
        assert_eq!(
            believes_leader,
            node == original_leader,
            "seed={seed}: node {node} leader belief mismatch (leader is {original_leader})"
        );
    }

    cluster.set_auto_split_thresholds(thresholds());

    // Crash the leader before any virtual time has passed since spawning
    // the loop — no node's first tick has fired yet under `SimEnv`, so
    // this deterministically forces the eventual trigger to come from
    // whichever node the cluster re-elects, never the original leader.
    cluster.crash(original_leader);

    // Let the control + data planes elect a new leader. Scan only the LIVE
    // nodes (never `SimCluster::leader_index_of`, which scans every node
    // including the crashed one) — a crashed-but-muted node's own
    // `RaftKvNode` never receives a higher-term message telling it to step
    // down, so its own stale, frozen "I am still leader" belief would
    // otherwise make this loop spin forever finding the SAME (crashed)
    // leader on every poll.
    let live_nodes: Vec<u64> = (0..cluster.node_count() as u64)
        .filter(|&n| n != original_leader)
        .collect();
    const STEP: Duration = Duration::from_millis(100);
    let mut elapsed = Duration::ZERO;
    let new_leader = loop {
        if let Some(&l) = live_nodes
            .iter()
            .find(|&&n| cluster.is_leader_local(n, tablet))
        {
            break l;
        }
        assert!(
            elapsed < Duration::from_secs(5),
            "seed={seed}: no new leader elected for {tablet:?} after crashing \
             {original_leader} within 5s"
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    };

    poll_split_converged(&mut cluster, "moved", &live_nodes, SETTLE_FLOOR);

    let children: Vec<TabletId> = cluster
        .metadata(new_leader)
        .tablets_for_table("moved")
        .map(|(&id, _)| id)
        .collect();
    assert_eq!(
        children.len(),
        2,
        "seed={seed}: expected exactly one fork (two children) despite the mid-window \
         leadership move, got {children:?}"
    );
    for pk in &keys {
        let (status, body) = get_item(&mut cluster, new_leader, "moved", pk);
        assert_eq!(status, 200, "seed={seed}: GetItem({pk}) failed: {body}");
    }
}

#[test]
fn d_leadership_move_mid_window_still_forks_exactly_once() {
    run_d_leadership_move_mid_window_still_forks_exactly_once(env_seed(0xA5F1_0004));
}

#[test]
fn d_leadership_move_mid_window_still_forks_exactly_once_over_seeds() {
    for i in 0..5 {
        run_d_leadership_move_mid_window_still_forks_exactly_once(0xA5F1_4000 + i);
    }
}

// ---------------------------------------------------------------------------
// Scenario (e): a node crashed before the fork, restarted only after it
// converged elsewhere — itself converges too, no zombie groups, its own
// leftover PARENT engine reclaimed (issue #722).
// ---------------------------------------------------------------------------

fn run_e_a_crashed_and_restarted_node_converges(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, 0, "resilient");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let parent = tablet_of(&cluster, 0, "resilient");

    let keys: Vec<String> = (0..NUM_KEYS).map(|i| format!("k{i}")).collect();
    for pk in &keys {
        let (status, body) = put_item(&mut cluster, 0, "resilient", pk, PAD_LEN);
        assert_eq!(status, 200, "seed={seed}: PutItem({pk}) failed: {body}");
    }

    let leader = cluster
        .leader_index_of(parent)
        .unwrap_or_else(|| panic!("seed={seed}: {parent:?} has no leader"));
    // Neither the tablet's own leader nor the control-plane leader (the
    // identical `sim_cluster_dynamo_drop_table.rs` scenario-4 discipline —
    // see that module's own doc for why crashing the control leader too
    // would exercise an unrelated control-plane-election scenario).
    let control_leader = cluster.control_leader_index() as u64;
    let victim = (0..cluster.node_count() as u64)
        .find(|&n| n != leader && n != control_leader)
        .unwrap_or_else(|| {
            panic!(
                "seed={seed}: a 3-node cluster must have a node that is neither the \
                 tablet leader ({leader}) nor the control leader ({control_leader})"
            )
        });

    cluster.set_auto_split_thresholds(thresholds());
    // Crash the victim before any virtual time has passed — it never
    // observes the fork at all, and never materializes either child
    // locally; its own local engine still holds the whole PARENT tablet's
    // data (asserted below) when it eventually restarts.
    cluster.crash(victim);

    let live_nodes: Vec<u64> = (0..cluster.node_count() as u64)
        .filter(|&n| n != victim)
        .collect();
    poll_split_converged(&mut cluster, "resilient", &live_nodes, SETTLE_FLOOR);

    let children: Vec<TabletId> = cluster
        .metadata(leader)
        .tablets_for_table("resilient")
        .map(|(&id, _)| id)
        .collect();
    assert_eq!(
        children.len(),
        2,
        "seed={seed}: the fork must have fully converged on the live nodes before \
         the victim ever restarts, got {children:?}"
    );

    // The victim genuinely holds the PARENT's own data before it restarts
    // — the reclaim below is of real content, not an already-empty engine.
    let victim_had_data = futures::executor::block_on(async {
        !cluster
            .storage(victim, parent)
            .entries()
            .await
            .expect("engine read ok")
            .is_empty()
    });
    assert!(
        victim_had_data,
        "seed={seed}: the victim must actually host the parent's data before crashing"
    );

    cluster.restart(victim);

    // The restarted node converges too: hosts both children (ordinary
    // catch-up — it was never named in the fork's own materialization),
    // no longer hosts the retired parent, and its own leftover parent
    // engine is reclaimed (issue #722's `EngineFactory::local_tablets`
    // second fact source, `animus-cp-data::host::Reconciler`).
    let all_nodes: Vec<u64> = (0..cluster.node_count() as u64).collect();
    poll_split_converged(&mut cluster, "resilient", &all_nodes, SETTLE_FLOOR);

    assert!(
        !cluster.hosted_tablets(victim).contains(&parent),
        "seed={seed}: the restarted victim must not still host the retired parent \
         {parent:?}"
    );
    let victim_parent_engine_empty = futures::executor::block_on(async {
        cluster
            .storage(victim, parent)
            .entries()
            .await
            .expect("engine read ok")
            .is_empty()
    });
    assert!(
        victim_parent_engine_empty,
        "seed={seed}: the restarted victim's own leftover parent engine must be \
         reclaimed (issue #722)"
    );
    for &child in &children {
        assert!(
            cluster.hosted_tablets(victim).contains(&child),
            "seed={seed}: the restarted victim must host child {child:?} via ordinary \
             catch-up"
        );
    }

    // No zombie groups anywhere: every node's own hosted set matches
    // `Metadata`'s own replica set for that node, mirroring
    // `sim_cluster_corpus.rs::check_no_zombie_groups`'s own invariant.
    for &node in &all_nodes {
        let meta = cluster.metadata(node);
        let expected: std::collections::BTreeSet<TabletId> = meta
            .tablets
            .iter()
            .filter(|(_, t)| t.replicas.contains(&animus_env::nid(node)))
            .map(|(&id, _)| id)
            .collect();
        assert_eq!(
            cluster.hosted_tablets(node),
            expected,
            "seed={seed}: node {node}'s hosted set must match its own replica set"
        );
    }

    for pk in &keys {
        let (status, body) = get_item(&mut cluster, victim, "resilient", pk);
        assert_eq!(
            status, 200,
            "seed={seed}: GetItem({pk}) on the restarted victim failed: {body}"
        );
    }
}

#[test]
fn e_a_crashed_and_restarted_node_converges() {
    run_e_a_crashed_and_restarted_node_converges(env_seed(0xA5F1_0005));
}

#[test]
fn e_a_crashed_and_restarted_node_converges_over_seeds() {
    for i in 0..5 {
        run_e_a_crashed_and_restarted_node_converges(0xA5F1_5000 + i);
    }
}
