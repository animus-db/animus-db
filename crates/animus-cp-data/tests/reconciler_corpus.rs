//! ADR 0031 PR5: a deterministic **`SimEnv` lifecycle corpus** for the per-node
//! tablet-host reconciler (`animus_cp_data::host::Reconciler`).
//!
//! Before this file, the tablet lifecycle (host → narrow-on-split →
//! reconfigure → release → reclaim) had exactly one focused `SimEnv` test
//! (`tests/reconciler.rs`) plus the flaky-by-nature wall-clock `ProdEnv`
//! integration tests in `animusd` (real timing, not seed-reproducible). This
//! corpus follows the house doctrine (ADR 0014): a **frozen, name-seeded**
//! scenario set, each scenario a named function in [`scenarios`], with a depth
//! knob (`ANIMUS_RECONCILER_SEEDS`) for scaling coverage — mirroring
//! `raftkv_linearizable.rs`'s `ANIMUS_RAFTKV_SEEDS` pattern exactly. Unlike that
//! corpus (a stochastic multi-round workload), each scenario here is a fixed,
//! hand-written **lifecycle script** (host this, narrow that, crash this node,
//! partition these two, tick this view N times, ...); seed depth still has
//! teeth because Raft election/heartbeat timing is seed-derived randomness
//! (`env.next_u64`/`gen_below`), so a different seed genuinely explores
//! different election/catch-up interleavings of the same script.
//!
//! **Harness shape.** One [`Simulator`], each scenario building its own small
//! [`Cluster`] of `Reconciler<SimEnv, MemoryEngine>` instances (one per
//! participating node id, each with its own `MemoryEngine`) driven by feeding
//! each node's reconciler a sequence of [`MetadataView`]s scripted by the
//! scenario itself — standing in for the control plane's actual output, so
//! this corpus does not need a live control-plane `RaftNode`. Real
//! `RaftKvNode` Raft groups form/elect/replicate under `SimEnv` underneath
//! each reconciler exactly as they would in production.
//!
//! **Critical `SimEnv` gotcha (see the root `CLAUDE.md`): never `block_on` a
//! `tick()` whose planned action tears a group down** — `Reconciler::teardown`
//! internally polls `env.sleep()` while waiting for the driver to stop, which
//! only resolves while [`Simulator::run_for`] is advancing virtual time. Every
//! scenario therefore runs as a **spawned task** driven by [`run`] (this
//! file's `poll_until`-based harness), never a bare `futures::executor::block_on`.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::host::{MetadataView, Reconciler};
use animus_cp_data::{RaftKvNode, StorageScope};
use animus_env::{Clock, EnvExt, NodeId, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{MemoryEngine, StorageEngine};
use animus_tablet::{Epoch, KeyRange, Tablet, TabletId};
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;
type Recon = Reconciler<SimEnv, MemoryEngine>;

const TABLE: &str = "t";

/// A driver env id used only to host each scenario's own top-level script
/// task — never a cluster node id (900 is well clear of every scenario's
/// {A,B,C} = {300,301,302}).
fn driver_id() -> NodeId {
    nid(900)
}
fn a() -> NodeId {
    nid(300)
}
fn b() -> NodeId {
    nid(301)
}
fn node_c() -> NodeId {
    nid(302)
}

const SCENARIO_BUDGET: Duration = Duration::from_secs(150);
const SCENARIO_STEP: Duration = Duration::from_secs(1);

// ---------------------------------------------------------------------------
// Small builders (mirrors `tests/reconciler.rs`'s helpers).
// ---------------------------------------------------------------------------

fn prefix_for(table: &str) -> Vec<u8> {
    table.as_bytes().to_vec()
}

/// ADR 0041 §3: a group's physical prefix is its parent scope's prefix plus the
/// row-kind byte — ordinary data is `KIND_BASE`.
fn physical(key: &[u8]) -> Vec<u8> {
    let mut out = prefix_for(TABLE);
    out.push(animus_cp_data::KIND_BASE);
    out.extend_from_slice(key);
    out
}

fn tablet(id: u64, start: &[u8], end: Option<&[u8]>, replicas: Vec<NodeId>) -> Tablet {
    Tablet::new_for_table(
        TabletId(id),
        TABLE,
        KeyRange::new(start.to_vec(), end.map(<[u8]>::to_vec)),
        replicas,
    )
}

/// [`tablet`], but at an explicit (possibly bumped) epoch — every scenario
/// that needs to simulate "some unrelated placement event already bumped this
/// tablet's epoch" (a restart racing a reconfigure, a replay artifact, a
/// spare join) builds its view this way, since `MetadataView` is a plain
/// caller-supplied projection with no obligation to reflect a *real* sequence
/// of control-plane commands — only its *shape* matters to `plan`.
fn tablet_at_epoch(
    id: u64,
    start: &[u8],
    end: Option<&[u8]>,
    replicas: Vec<NodeId>,
    epoch: u64,
) -> Tablet {
    let mut t = tablet(id, start, end, replicas);
    t.epoch = Epoch(epoch);
    t
}

fn view(tablets: impl IntoIterator<Item = Tablet>) -> MetadataView {
    MetadataView {
        tablets: tablets.into_iter().map(|t| (t.id, t)).collect(),
        ..Default::default()
    }
}

fn view_with_down(
    tablets: impl IntoIterator<Item = Tablet>,
    down: impl IntoIterator<Item = NodeId>,
) -> MetadataView {
    MetadataView {
        tablets: tablets.into_iter().map(|t| (t.id, t)).collect(),
        down: down.into_iter().collect(),
        ..Default::default()
    }
}

/// FNV-1a over a scenario's name — the frozen, name-derived seed (ADR 0014
/// style; identical algorithm to `raftkv_linearizable.rs::name_seed`).
fn name_seed(name: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in name.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

// ---------------------------------------------------------------------------
// The cluster harness: N nodes, each its own `Reconciler` + `MemoryEngine`.
// ---------------------------------------------------------------------------

struct ClusterNode {
    reconciler: Recon,
    storage: MemoryEngine,
    hosted_log: Arc<Mutex<Vec<TabletId>>>,
    teardown_log: Arc<Mutex<Vec<TabletId>>>,
}

struct Cluster {
    sim: Simulator,
    nodes: BTreeMap<NodeId, ClusterNode>,
}

impl Cluster {
    fn new(sim: Simulator) -> Self {
        Cluster {
            sim,
            nodes: BTreeMap::new(),
        }
    }

    fn add_node(&mut self, id: NodeId) {
        self.add_node_with_storage(id, MemoryEngine::new());
    }

    /// Add (or, after [`crash_restart`](Self::crash_restart), re-add) a node
    /// with a specific `storage` handle — reusing the SAME `MemoryEngine`
    /// object across a restart is how this corpus models a durable engine
    /// (`LsmEngine` in production) surviving a process crash: `MemoryEngine`
    /// is a plain heap structure, untouched by `Simulator::stop`'s disk model,
    /// so keeping the same clone alive across the restart is the correct
    /// stand-in without needing to model on-disk persistence for this corpus
    /// (that is `raftkv_linearizable.rs`'s job, over the LSM engine tier).
    fn add_node_with_storage(&mut self, id: NodeId, storage: MemoryEngine) {
        let hosted_log: Arc<Mutex<Vec<TabletId>>> = Arc::new(Mutex::new(Vec::new()));
        let teardown_log: Arc<Mutex<Vec<TabletId>>> = Arc::new(Mutex::new(Vec::new()));
        let hl = Arc::clone(&hosted_log);
        let tl = Arc::clone(&teardown_log);
        let reconciler: Recon = Reconciler::new(
            self.sim.env(id.clone()),
            storage.clone(),
            id.clone(),
            prefix_for,
            move |t, _n| hl.lock().unwrap().push(t),
            move |t| tl.lock().unwrap().push(t),
        );
        self.nodes.insert(
            id,
            ClusterNode {
                reconciler,
                storage,
                hosted_log,
                teardown_log,
            },
        );
    }

    /// Simulate a process crash+restart of `id`: `Simulator::stop` kills
    /// every task this node owns (every `RaftKvNode` driver+apply task the
    /// reconciler spawned — their durable Raft WAL bytes survive on `env`'s
    /// disk, per `stop`'s contract), then this node's `Reconciler` is dropped
    /// and rebuilt fresh (`LocalState::default()`, empty `hosted` map) —
    /// exactly as a real process restart re-derives its lifecycle state from
    /// scratch — reusing the SAME `MemoryEngine` (see
    /// [`add_node_with_storage`](Self::add_node_with_storage)'s doc).
    fn crash_restart(&mut self, id: NodeId) {
        self.sim.stop(id.clone());
        let storage = self.nodes.remove(&id).expect("node exists").storage;
        self.add_node_with_storage(id, storage);
    }

    async fn tick(&mut self, id: NodeId, view: &MetadataView) {
        self.nodes
            .get_mut(&id)
            .expect("node exists")
            .reconciler
            .tick(view)
            .await;
    }

    async fn tick_all(&mut self, ids: &[NodeId], view: &MetadataView) {
        for id in ids {
            self.tick(id.clone(), view).await;
        }
    }

    fn node(&self, id: NodeId) -> &Recon {
        &self.nodes[&id].reconciler
    }

    fn storage(&self, id: NodeId) -> &MemoryEngine {
        &self.nodes[&id].storage
    }

    fn hosted_log(&self, id: NodeId) -> Vec<TabletId> {
        self.nodes[&id].hosted_log.lock().unwrap().clone()
    }

    fn teardown_log(&self, id: NodeId) -> Vec<TabletId> {
        self.nodes[&id].teardown_log.lock().unwrap().clone()
    }

    fn hosted_set(&self, id: NodeId) -> BTreeSet<TabletId> {
        self.node(id).local_state().hosted.clone()
    }
}

// ---------------------------------------------------------------------------
// Shared assertion helpers — the generic invariant checks every scenario runs.
// ---------------------------------------------------------------------------

/// Poll `check` every `step`, up to `tries` times; returns whether it ever
/// held. a() bounded, `env.sleep`-driven wait for a *real* Raft convergence
/// (election, replication, membership change) inside one scenario's own
/// script — independent of the outer [`run`] harness's sim-wide safety net.
async fn wait_until(
    env: &SimEnv,
    tries: usize,
    step: Duration,
    mut check: impl FnMut() -> bool,
) -> bool {
    for _ in 0..tries {
        if check() {
            return true;
        }
        env.sleep(step).await;
    }
    check()
}

/// Force a REAL Raft membership removal excluding `victim` from a group,
/// using `heir` (a genuine second voter) to perform it — mirrors
/// `tests/reconciler.rs`'s dance: if `victim` currently leads, transfer
/// leadership to `heir` first (`change_membership` forbids leader
/// self-removal), then `heir` removes `victim` outright. Retries the whole
/// dance every poll tick rather than asserting on a single attempt — at
/// depth (many seeds), a `transfer_leadership`/`change_membership` call can
/// transiently race a concurrent election (e.g. the node believed to be
/// leader stepped down to re-campaign between our `is_leader()` check and
/// the propose call, returning `NotLeader`), which is a benign, retryable
/// timing blip, not a reason to fail outright. Blocks (bounded) until
/// `victim`'s OWN durable Raft config actually excludes it — the
/// release-gate anchor (`TabletFacts::config_excludes_me`) every
/// release-testing scenario needs to be a *real*, not injected, fact.
async fn remove_replica_for_real(
    env: &SimEnv,
    victim: &KvNode,
    victim_id: NodeId,
    heir: &KvNode,
    heir_id: NodeId,
    remaining: BTreeSet<NodeId>,
) {
    let excluded = wait_until(env, 150, Duration::from_millis(100), || {
        if !victim.config().contains(&victim_id) {
            return true;
        }
        if victim.is_leader() {
            victim.transfer_leadership(heir_id.clone());
        } else if heir.is_leader() {
            let _: ProposeResult = heir.change_membership(remaining.clone());
        }
        false
    })
    .await;
    assert!(
        excluded,
        "victim's own durable Raft config never excluded it after removal"
    );
}

/// (a) **Hosting convergence** — `node`'s `LocalState::hosted` equals
/// `expected`.
fn assert_hosted_converged(
    c: &Cluster,
    node: NodeId,
    expected: impl IntoIterator<Item = TabletId>,
) {
    let expected: BTreeSet<TabletId> = expected.into_iter().collect();
    assert_eq!(
        c.hosted_set(node.clone()),
        expected,
        "node {node}: hosted set did not converge to the expected final placement"
    );
}

/// (b) **Data safety, present** — `key` reads back as `value` from `node`'s
/// raw local engine (a physical, already-prefixed key). Every caller here
/// passes the plain client-level `value`; this strips the ADR 0018 §2/PR3
/// committed-value envelope (a leading `0` tag byte every apply-path write
/// now wraps its value in — `animus_cp_data::txn`, internal to the crate)
/// so callers don't each need to know about it, mirroring what
/// `RaftKvNode::local_get` does for a scoped read.
async fn assert_present(storage: &MemoryEngine, key: &[u8], value: &[u8]) {
    let got = storage
        .get(key)
        .await
        .expect("engine read ok")
        .map(|vv| vv.value);
    let unwrapped = got.as_deref().map(|raw| {
        assert_eq!(
            raw.first().copied(),
            Some(0u8),
            "key {key:?}: expected a committed-value envelope (tag 0), got {raw:?}"
        );
        &raw[1..]
    });
    assert_eq!(
        unwrapped,
        Some(value),
        "key {key:?} must be present with value {value:?}, got {got:?}"
    );
}

/// (b) **Data safety, absent** — `key` must have been erased from `node`'s
/// raw local engine.
async fn assert_absent(storage: &MemoryEngine, key: &[u8]) {
    let got = storage.get(key).await.expect("engine read ok");
    assert!(got.is_none(), "key {key:?} must be erased, found {got:?}");
}

/// (c) **No zombie groups** — every handle in `handles` (captured right
/// before its tablet was expected to be released/reclaimed) has fully
/// stopped both its consensus loop and its apply task.
fn assert_all_stopped(handles: &[KvNode]) {
    for h in handles {
        assert!(
            h.is_stopped(),
            "a torn-down group's driver never fully stopped"
        );
    }
}

/// (d) **Idempotence** — re-tick `node` once more against the SAME (already
/// converged) `view` and assert no observable drift: the hosted set, every
/// hosted tablet's live scope range and Raft voter config, and the
/// `on_host`/`on_teardown` call counts are all unchanged.
///
/// Note this is **not** "the second tick emits zero actions" — `plan`'s own
/// doc states `HostAction::Reconfigure` is replanned every tick this node
/// leads a tablet's group, converged or not (harmless: `reconfigure_step` is
/// itself a no-op once the group matches `desired`). "Idempotent" here means
/// the observable *state* does not drift, which is the property that
/// actually matters to a caller.
async fn assert_idempotent(c: &mut Cluster, node: NodeId, view: &MetadataView) {
    let before_hosted = c.hosted_set(node.clone());
    let before_hosted_log = c.hosted_log(node.clone());
    let before_teardown_log = c.teardown_log(node.clone());
    let mut before_scopes = BTreeMap::new();
    let mut before_configs = BTreeMap::new();
    for &t in &before_hosted {
        if let Some(h) = c.node(node.clone()).hosted_node(t) {
            before_scopes.insert(t, h.scope_range());
            before_configs.insert(t, h.config());
        }
    }

    c.tick(node.clone(), view).await;

    assert_eq!(
        c.hosted_set(node.clone()),
        before_hosted,
        "idempotence: node {node}'s hosted set drifted on a repeat tick"
    );
    assert_eq!(
        c.hosted_log(node.clone()),
        before_hosted_log,
        "idempotence: on_host fired again on a repeat tick"
    );
    assert_eq!(
        c.teardown_log(node.clone()),
        before_teardown_log,
        "idempotence: on_teardown fired again on a repeat tick"
    );
    for &t in &before_hosted {
        if let Some(h) = c.node(node.clone()).hosted_node(t) {
            assert_eq!(
                h.scope_range(),
                before_scopes[&t],
                "idempotence: tablet {t:?}'s scope drifted on a repeat tick"
            );
            assert_eq!(
                h.config(),
                before_configs[&t],
                "idempotence: tablet {t:?}'s voter config drifted on a repeat tick"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// The generic runner: spawn a scenario's script as a task, drive `Simulator`
// until it completes or the budget is exhausted (never a bare `block_on` —
// see this file's top doc).
// ---------------------------------------------------------------------------

fn poll_until(
    sim: &mut Simulator,
    budget: Duration,
    step: Duration,
    msg: &str,
    mut check: impl FnMut() -> bool,
) {
    let mut waited = Duration::ZERO;
    while waited < budget {
        sim.run_for(step);
        waited += step;
        if check() {
            return;
        }
    }
    panic!("{msg} (seed={})", sim.seed());
}

fn run<F, Fut>(seed: u64, body: F)
where
    F: FnOnce(Simulator) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let mut sim = Simulator::new(seed);
    let driver_env = sim.env(driver_id());
    let done = Arc::new(Mutex::new(false));
    let done2 = Arc::clone(&done);
    let sim_in_task = sim.clone();
    driver_env.spawn_task(async move {
        body(sim_in_task).await;
        *done2.lock().unwrap() = true;
    });
    poll_until(
        &mut sim,
        SCENARIO_BUDGET,
        SCENARIO_STEP,
        "scenario never completed",
        move || *done.lock().unwrap(),
    );
}

// ---------------------------------------------------------------------------
// The frozen corpus: named scenarios, each a fixed lifecycle script.
// ---------------------------------------------------------------------------

struct Scenario {
    name: &'static str,
    seed: u64,
    run: fn(u64),
}

/// Expand each cell into `k` seed variants (ADR 0014 style): variant 0 keeps
/// the cell's canonical (frozen) name + seed, so `k=1` is byte-identical to
/// the always-on default; variants `1..k` get a `_sNN` suffix and a fresh
/// name-derived seed, exercising the SAME script under different
/// Raft-election-timing interleavings.
fn seed_expand(cells: Vec<Scenario>, k: usize) -> Vec<Scenario> {
    if k <= 1 {
        return cells;
    }
    let mut out = Vec::with_capacity(cells.len() * k);
    for cell in cells {
        for i in 0..k {
            if i == 0 {
                out.push(Scenario {
                    name: cell.name,
                    seed: cell.seed,
                    run: cell.run,
                });
            } else {
                // Leak a small, bounded number of strings for `'static` names —
                // acceptable in a test binary's corpus expansion (mirrors the
                // sibling corpora's approach of keeping names `'static`).
                let name: &'static str =
                    Box::leak(format!("{}_s{i:02}", cell.name).into_boxed_str());
                out.push(Scenario {
                    name,
                    seed: name_seed(name),
                    run: cell.run,
                });
            }
        }
    }
    out
}

/// Depth knob (`ANIMUS_RECONCILER_SEEDS`, default 1) — mirrors
/// `ANIMUS_RAFTKV_SEEDS`/`ANIMUS_CORPUS_SEEDS`.
fn seeds_per_cell() -> usize {
    std::env::var("ANIMUS_RECONCILER_SEEDS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1)
        .max(1)
}

macro_rules! scenario {
    ($name:expr, $f:ident) => {
        Scenario {
            name: $name,
            seed: name_seed($name),
            run: $f,
        }
    };
}

fn scenario_cells() -> Vec<Scenario> {
    vec![
        // --- Lifecycle shapes -------------------------------------------------
        scenario!(
            "fresh_whole_keyspace_host_elect_serve",
            scenario_fresh_single_node
        ),
        scenario!(
            "fresh_two_replica_group_hosts_on_both_nodes",
            scenario_fresh_two_replica
        ),
        scenario!(
            "split_narrows_source_hosts_sibling_no_double_count",
            scenario_split_narrow_sibling
        ),
        scenario!(
            "rebalance_off_releases_with_bounded_erase_sparing_sibling",
            scenario_rebalance_off_release
        ),
        scenario!(
            "drop_table_reclaims_a_hosted_tablet",
            scenario_drop_table_reclaim
        ),
        scenario!(
            "spare_join_as_non_voter_then_promoted_by_leader",
            scenario_spare_join_promoted
        ),
        scenario!(
            "growth_node_first_view_arrives_late_still_converges",
            scenario_growth_node_late_view
        ),
        scenario!(
            "reconfigure_removes_a_down_replica_first",
            scenario_reconfigure_down_replica
        ),
        scenario!(
            "idempotent_tick_on_converged_multi_tablet_state",
            scenario_idempotent_multi_tablet
        ),
        scenario!(
            "reconfigure_transfers_leadership_before_removing_the_leader",
            scenario_reconfigure_self_removal
        ),
        // --- Fault axes crossed in ---------------------------------------------
        scenario!(
            "crash_restart_single_replica_upgrades_via_has_data",
            scenario_crash_restart_single
        ),
        scenario!(
            "crash_restart_follower_in_two_replica_group_rejoins_no_loss",
            scenario_crash_restart_follower
        ),
        scenario!(
            "replay_epoch_flicker_mid_release_count_resets_then_releases",
            scenario_replay_epoch_flicker
        ),
        scenario!(
            "replay_absent_then_present_reclaims_then_rehosts_empty",
            scenario_replay_absent_then_present
        ),
        scenario!(
            "partition_during_removal_blocks_release_until_healed",
            scenario_partition_blocks_release
        ),
        scenario!(
            "split_then_immediate_release_zero_ticks_spares_sibling",
            scenario_split_then_immediate_release
        ),
        scenario!(
            "re_add_after_exclusion_cancels_pending_release",
            scenario_re_add_cancels_release
        ),
        // --- ADR 0018 §2 amendment fix (the split_cluster.rs livelock) ---------
        scenario!(
            "narrow_seal_survives_a_late_promotion_after_narrowing_as_a_follower",
            scenario_narrow_seal_survives_a_late_promotion
        ),
        // --- ADR 0044 phase-1 PR4 (wake-on-demand + fork H) ---------------------
        scenario!(
            "quiesced_group_wakes_when_a_replica_goes_down",
            scenario_quiesced_group_wakes_when_a_replica_goes_down
        ),
        scenario!(
            "quiesce_races_a_split_seal_handoff",
            scenario_quiesce_races_a_split_seal_handoff
        ),
    ]
}

fn corpus() -> Vec<Scenario> {
    seed_expand(scenario_cells(), seeds_per_cell())
}

// ---------------------------------------------------------------------------
// Scenario 1: fresh whole-keyspace tablet, single node, host/elect/serve.
// ---------------------------------------------------------------------------

fn scenario_fresh_single_node(seed: u64) {
    run(seed, |sim| async move {
        let env = sim.env(a());
        let mut c = Cluster::new(sim);
        c.add_node(a());

        let v = view([tablet(1, b"", None, vec![a()])]);
        c.tick(a(), &v).await;
        env.sleep(Duration::from_secs(2)).await; // a lone voter self-elects fast

        let h = c.node(a()).hosted_node(TabletId(1)).unwrap().clone();
        assert!(h.is_leader(), "a lone voter must self-elect");
        h.put(b"k1".to_vec(), b"v1".to_vec());
        env.sleep(Duration::from_secs(1)).await;
        assert_eq!(h.linearizable_get(b"k1").await, Some(b"v1".to_vec()));

        assert_hosted_converged(&c, a(), [TabletId(1)]);
        assert_eq!(c.hosted_log(a()), vec![TabletId(1)]);
        assert!(c.teardown_log(a()).is_empty());
        assert_idempotent(&mut c, a(), &v).await;
    });
}

// ---------------------------------------------------------------------------
// Scenario 2: a fresh 2-replica group, both nodes' own reconcilers host it.
// ---------------------------------------------------------------------------

fn scenario_fresh_two_replica(seed: u64) {
    run(seed, |sim| async move {
        let env = sim.env(a());
        let mut c = Cluster::new(sim);
        c.add_node(a());
        c.add_node(b());

        let v = view([tablet(1, b"", None, vec![a(), b()])]);
        c.tick_all(&[a(), b()], &v).await;
        env.sleep(Duration::from_secs(2)).await; // elect

        let ha = c.node(a()).hosted_node(TabletId(1)).unwrap().clone();
        let hb = c.node(b()).hosted_node(TabletId(1)).unwrap().clone();
        let leader = if ha.is_leader() { &ha } else { &hb };
        leader.put(b"k".to_vec(), b"v".to_vec());
        env.sleep(Duration::from_secs(1)).await;
        // `linearizable_get` only serves on the confirmed leader (a follower's
        // ReadIndex barrier always fails, by design) — check the leader
        // linearizably and the follower via a raw `local_get` (proves
        // replication reached it, not linearizability).
        assert_eq!(leader.linearizable_get(b"k").await, Some(b"v".to_vec()));
        let follower = if std::ptr::eq(leader, &ha) { &hb } else { &ha };
        assert!(
            wait_until(&env, 30, Duration::from_millis(100), || {
                block_on(follower.local_get(b"k")) == Some(b"v".to_vec())
            })
            .await,
            "the follower never replicated the write"
        );

        assert_hosted_converged(&c, a(), [TabletId(1)]);
        assert_hosted_converged(&c, b(), [TabletId(1)]);
        assert_idempotent(&mut c, a(), &v).await;
        assert_idempotent(&mut c, b(), &v).await;
    });
}

// ---------------------------------------------------------------------------
// Scenario 3: split narrows the source and hosts a co-hosted sibling, single
// node, no membership faults — the pure host+narrow shape.
// ---------------------------------------------------------------------------

const BOUNDARY: &[u8] = b"m";

fn scenario_split_narrow_sibling(seed: u64) {
    run(seed, |sim| async move {
        let env = sim.env(a());
        let mut c = Cluster::new(sim);
        c.add_node(a());

        let v1 = view([tablet(1, b"", None, vec![a()])]);
        c.tick(a(), &v1).await;
        env.sleep(Duration::from_secs(2)).await;

        let h1 = c.node(a()).hosted_node(TabletId(1)).unwrap().clone();
        for i in 0..5u64 {
            h1.put(
                format!("a{i:02}").into_bytes(),
                format!("lo{i}").into_bytes(),
            );
            h1.put(
                format!("z{i:02}").into_bytes(),
                format!("hi{i}").into_bytes(),
            );
        }
        env.sleep(Duration::from_secs(1)).await;

        // Narrow the source to the lower half (the split's source-side effect).
        let v2 = view([tablet(1, b"", Some(BOUNDARY), vec![a()])]);
        c.tick(a(), &v2).await;
        assert_eq!(
            h1.scope_range(),
            KeyRange::new(b"".to_vec(), Some(BOUNDARY.to_vec()))
        );

        // Host the sibling covering the upper half — has_data finds the "z.."
        // keys already present, so it forms as a fresh, full-voter tablet.
        let v3 = view([
            tablet(1, b"", Some(BOUNDARY), vec![a()]),
            tablet(2, BOUNDARY, None, vec![a()]),
        ]);
        c.tick(a(), &v3).await;
        env.sleep(Duration::from_secs(2)).await;

        let h2 = c.node(a()).hosted_node(TabletId(2)).unwrap().clone();
        assert!(h2.is_leader(), "the fresh sibling must self-elect");
        for i in 0..5u64 {
            assert_eq!(
                h1.local_get(format!("a{i:02}").as_bytes()).await,
                Some(format!("lo{i}").into_bytes()),
                "source tablet must still see its own (lower-half) data"
            );
            assert_eq!(
                h2.local_get(format!("z{i:02}").as_bytes()).await,
                Some(format!("hi{i}").into_bytes()),
                "sibling tablet must see its own (upper-half) data — no double count"
            );
        }

        assert_hosted_converged(&c, a(), [TabletId(1), TabletId(2)]);
        assert_idempotent(&mut c, a(), &v3).await;
    });
}

// ---------------------------------------------------------------------------
// Scenario 4: rebalance-off — narrow, host a co-hosted sibling, a REAL
// membership removal excludes this node from tablet 1's group, then the
// release-confirm dampener fires and erases ONLY tablet 1's (narrow) range —
// the sibling-sparing invariant.
// ---------------------------------------------------------------------------

fn scenario_rebalance_off_release(seed: u64) {
    run(seed, |sim| async move {
        let env = sim.env(a());
        let other_env = sim.env(b());
        let mut c = Cluster::new(sim);
        c.add_node(a());

        let v1 = view([tablet(1, b"", None, vec![a(), b()])]);
        c.tick(a(), &v1).await;
        env.sleep(Duration::from_secs(2)).await;

        // b(): a genuine second voter, constructed directly (this scenario only
        // needs a real second voter, not a second node's full lifecycle).
        let b_storage = MemoryEngine::new();
        let hb = KvNode::start_hosted(
            other_env,
            vec![a(), b()],
            b_storage,
            StorageScope::new(prefix_for(TABLE), KeyRange::whole()),
            1,
        );
        env.sleep(Duration::from_secs(2)).await;

        let ha = c.node(a()).hosted_node(TabletId(1)).unwrap().clone();
        for i in 0..5u64 {
            let leader = if ha.is_leader() { &ha } else { &hb };
            leader.put(
                format!("a{i:02}").into_bytes(),
                format!("lo{i}").into_bytes(),
            );
            leader.put(
                format!("z{i:02}").into_bytes(),
                format!("hi{i}").into_bytes(),
            );
        }
        env.sleep(Duration::from_secs(2)).await;

        // Narrow tablet 1 (the split's source-side effect) then host the
        // co-hosted sibling covering the upper half.
        let v2 = view([tablet(1, b"", Some(BOUNDARY), vec![a(), b()])]);
        c.tick(a(), &v2).await;
        let v3 = view([
            tablet(1, b"", Some(BOUNDARY), vec![a(), b()]),
            tablet(2, BOUNDARY, None, vec![a()]),
        ]);
        c.tick(a(), &v3).await;
        env.sleep(Duration::from_secs(2)).await;

        let h2 = c.node(a()).hosted_node(TabletId(2)).unwrap().clone();
        assert_eq!(
            h2.local_get(b"z00").await,
            Some(b"hi0".to_vec()),
            "sanity: sibling data present before release"
        );

        remove_replica_for_real(&env, &ha, a(), &hb, b(), [b()].into_iter().collect()).await;

        // Drive the release-confirm dampener to completion.
        let v4 = view([
            tablet(1, b"", Some(BOUNDARY), vec![b()]),
            tablet(2, BOUNDARY, None, vec![a()]),
        ]);
        for _ in 0..10 {
            c.tick(a(), &v4).await;
            env.sleep(Duration::from_millis(50)).await;
        }

        assert_hosted_converged(&c, a(), [TabletId(2)]);
        assert_eq!(c.hosted_log(a()), vec![TabletId(1), TabletId(2)]);
        assert_eq!(c.teardown_log(a()), vec![TabletId(1)]);
        assert_all_stopped(&[ha]);

        for i in 0..5u64 {
            assert_absent(
                c.storage(a()),
                &physical(format!("a{i:02}").into_bytes().as_slice()),
            )
            .await;
            assert_present(
                c.storage(a()),
                &physical(format!("z{i:02}").into_bytes().as_slice()),
                format!("hi{i}").into_bytes().as_slice(),
            )
            .await;
        }

        assert_idempotent(&mut c, a(), &v4).await;
    });
}

// ---------------------------------------------------------------------------
// Scenario 5: drop-table reclaim — a hosted tablet vanishes from the map.
// ---------------------------------------------------------------------------

fn scenario_drop_table_reclaim(seed: u64) {
    run(seed, |sim| async move {
        let env = sim.env(a());
        let mut c = Cluster::new(sim);
        c.add_node(a());

        let v1 = view([tablet(1, b"", None, vec![a()])]);
        c.tick(a(), &v1).await;
        env.sleep(Duration::from_secs(2)).await;

        let h1 = c.node(a()).hosted_node(TabletId(1)).unwrap().clone();
        h1.put(b"k".to_vec(), b"v".to_vec());
        env.sleep(Duration::from_secs(1)).await;

        let v2 = view([]); // the whole table dropped
        c.tick(a(), &v2).await;

        assert_hosted_converged(&c, a(), []);
        assert_eq!(c.teardown_log(a()), vec![TabletId(1)]);
        assert_all_stopped(&[h1]);
        assert_absent(c.storage(a()), &physical(b"k")).await;

        assert_idempotent(&mut c, a(), &v2).await;
    });
}

// ---------------------------------------------------------------------------
// Scenario 6: a spare joins as a non-voter, then the leader promotes it.
// ---------------------------------------------------------------------------

fn scenario_spare_join_promoted(seed: u64) {
    run(seed, |sim| async move {
        let env = sim.env(a());
        let mut c = Cluster::new(sim);
        c.add_node(a());
        c.add_node(b());

        let v1 = view([tablet(1, b"", None, vec![a()])]);
        c.tick(a(), &v1).await;
        env.sleep(Duration::from_secs(2)).await;
        let ha = c.node(a()).hosted_node(TabletId(1)).unwrap().clone();
        assert!(ha.is_leader());

        // b() is a spare: the tablet's replica set now includes it, at a bumped
        // epoch — `plan_join_host` says "join as non-voter."
        let v2 = tablet_at_epoch(1, b"", None, vec![a(), b()], 2);
        let v2 = view([v2]);
        c.tick(b(), &v2).await;
        let hb = c.node(b()).hosted_node(TabletId(1)).unwrap().clone();
        assert!(
            !hb.config().contains(&b()),
            "a spare must start as a non-voter, not already in its own config"
        );

        // The leader's own Reconfigure action must promote b() to a real voter.
        for _ in 0..40 {
            c.tick(a(), &v2).await;
            env.sleep(Duration::from_millis(100)).await;
            if ha.config().contains(&b()) {
                break;
            }
        }
        assert!(
            ha.config().contains(&b()),
            "the leader never promoted the spare to a voter"
        );

        assert_hosted_converged(&c, a(), [TabletId(1)]);
        assert_hosted_converged(&c, b(), [TabletId(1)]);
        assert_idempotent(&mut c, a(), &v2).await;
        assert_idempotent(&mut c, b(), &v2).await;
    });
}

// ---------------------------------------------------------------------------
// Scenario 7: a "growth node" whose first view arrives late.
// ---------------------------------------------------------------------------

fn scenario_growth_node_late_view(seed: u64) {
    run(seed, |sim| async move {
        let env = sim.env(a());
        let mut c = Cluster::new(sim);
        c.add_node(a());
        c.add_node(b());

        let v = view([tablet(1, b"", None, vec![a(), b()])]);
        // a() ticks immediately and hosts; b()'s own reconciler is not ticked at
        // all yet — modelling a node whose control-plane view arrives late
        // (a just-grown cluster member's own control raft lagging, ADR 0030).
        c.tick(a(), &v).await;
        env.sleep(Duration::from_secs(5)).await;
        assert_hosted_converged(&c, b(), []); // b() genuinely never ticked

        // b()'s view finally arrives.
        c.tick(b(), &v).await;
        env.sleep(Duration::from_secs(2)).await;

        let ha = c.node(a()).hosted_node(TabletId(1)).unwrap().clone();
        let hb = c.node(b()).hosted_node(TabletId(1)).unwrap().clone();
        let leader = if ha.is_leader() { &ha } else { &hb };
        leader.put(b"k".to_vec(), b"v".to_vec());
        env.sleep(Duration::from_secs(1)).await;
        assert_eq!(leader.linearizable_get(b"k").await, Some(b"v".to_vec()));
        let follower = if std::ptr::eq(leader, &ha) { &hb } else { &ha };
        assert!(
            wait_until(&env, 30, Duration::from_millis(100), || {
                block_on(follower.local_get(b"k")) == Some(b"v".to_vec())
            })
            .await,
            "the follower never replicated the write"
        );

        assert_hosted_converged(&c, a(), [TabletId(1)]);
        assert_hosted_converged(&c, b(), [TabletId(1)]);
        assert_idempotent(&mut c, a(), &v).await;
        assert_idempotent(&mut c, b(), &v).await;
    });
}

// ---------------------------------------------------------------------------
// Scenario 8: reconfigure removes a Down replica first (no spare added).
// ---------------------------------------------------------------------------

fn scenario_reconfigure_down_replica(seed: u64) {
    run(seed, |sim| async move {
        let env = sim.env(a());
        let mut c = Cluster::new(sim);
        c.add_node(a());
        c.add_node(b());
        c.add_node(node_c());

        let v1 = view([tablet(1, b"", None, vec![a(), b(), node_c()])]);
        c.tick_all(&[a(), b(), node_c()], &v1).await;
        env.sleep(Duration::from_secs(2)).await;

        let ha = c.node(a()).hosted_node(TabletId(1)).unwrap().clone();
        let hb = c.node(b()).hosted_node(TabletId(1)).unwrap().clone();

        // The control plane marks node_c() down and no longer desires it. Tick
        // EVERY node each round (not just whoever led at the start) — if node_c()
        // itself happens to be the leader, `reconfigure_step` can't remove
        // itself directly (a Down *self* isn't eligible for the ungated
        // down-extra removal, which explicitly excludes `me`); it must first
        // transfer leadership to a member of `desired`, and the NEW leader's
        // OWN next tick is what actually performs the removal.
        let v2 = view_with_down([tablet(1, b"", None, vec![a(), b()])], [node_c()]);
        let target: BTreeSet<NodeId> = [a(), b()].into_iter().collect();
        for _ in 0..80 {
            c.tick_all(&[a(), b(), node_c()], &v2).await;
            env.sleep(Duration::from_millis(100)).await;
            if ha.config() == target && hb.config() == target {
                break;
            }
        }
        assert_eq!(
            ha.config(),
            [a(), b()].into_iter().collect::<BTreeSet<_>>(),
            "the down replica was never removed from a()'s view of the group"
        );
        assert_eq!(
            hb.config(),
            [a(), b()].into_iter().collect::<BTreeSet<_>>(),
            "the down replica was never removed from b()'s view of the group"
        );
    });
}

// ---------------------------------------------------------------------------
// Scenario 10: idempotence on a converged multi-tablet, multi-node state.
// ---------------------------------------------------------------------------

fn scenario_idempotent_multi_tablet(seed: u64) {
    run(seed, |sim| async move {
        let env = sim.env(a());
        let mut c = Cluster::new(sim);
        c.add_node(a());
        c.add_node(b());

        let v = view([
            tablet(1, b"", Some(BOUNDARY), vec![a(), b()]),
            tablet(2, BOUNDARY, None, vec![a()]),
        ]);
        c.tick_all(&[a(), b()], &v).await;
        env.sleep(Duration::from_secs(2)).await;

        assert_hosted_converged(&c, a(), [TabletId(1), TabletId(2)]);
        assert_hosted_converged(&c, b(), [TabletId(1)]);

        // Two extra ticks each, no drift at any point.
        assert_idempotent(&mut c, a(), &v).await;
        assert_idempotent(&mut c, a(), &v).await;
        assert_idempotent(&mut c, b(), &v).await;
        assert_idempotent(&mut c, b(), &v).await;
    });
}

// ---------------------------------------------------------------------------
// Scenario 11: the leader itself must be removed — reconfigure transfers
// leadership first, then the new leader removes the old one.
// ---------------------------------------------------------------------------

fn scenario_reconfigure_self_removal(seed: u64) {
    run(seed, |sim| async move {
        let env = sim.env(a());
        let mut c = Cluster::new(sim);
        c.add_node(a());
        c.add_node(b());
        c.add_node(node_c());

        let v1 = view([tablet(1, b"", None, vec![a(), b(), node_c()])]);
        c.tick_all(&[a(), b(), node_c()], &v1).await;
        env.sleep(Duration::from_secs(2)).await;

        let ha = c.node(a()).hosted_node(TabletId(1)).unwrap().clone();
        let hb = c.node(b()).hosted_node(TabletId(1)).unwrap().clone();
        let hc = c.node(node_c()).hosted_node(TabletId(1)).unwrap().clone();
        // a() little write traffic so `commit_index` genuinely advances (the
        // transfer target must be caught up to it).
        let leader0 = if ha.is_leader() {
            &ha
        } else if hb.is_leader() {
            &hb
        } else {
            &hc
        };
        for i in 0..5u64 {
            leader0.put(format!("k{i}").into_bytes(), b"v".to_vec());
        }
        env.sleep(Duration::from_secs(2)).await;

        // Find whoever leads NOW and desire everyone else — forcing the
        // "must remove the leader itself" branch regardless of who won.
        let (leader_id, leader): (NodeId, &KvNode) = if ha.is_leader() {
            (a(), &ha)
        } else if hb.is_leader() {
            (b(), &hb)
        } else {
            (node_c(), &hc)
        };
        let desired: BTreeSet<NodeId> = [a(), b(), node_c()]
            .into_iter()
            .filter(|n| *n != leader_id)
            .collect();
        let v2 = view([tablet(1, b"", None, desired.iter().cloned().collect())]);

        // Tick EVERY node each round: `reconfigure_step` first arms a
        // leadership transfer (the old leader can't remove itself directly),
        // so the NEW leader's own next tick is what actually performs the
        // removal — ticking only the original leader id would stall forever
        // the instant leadership moves.
        for _ in 0..60 {
            c.tick_all(&[a(), b(), node_c()], &v2).await;
            env.sleep(Duration::from_millis(100)).await;
            if !leader.config().contains(&leader_id) {
                break;
            }
        }
        assert!(
            !leader.config().contains(&leader_id),
            "the old leader was never removed after a transfer"
        );
    });
}

// ---------------------------------------------------------------------------
// Scenario 12: crash+restart of a single-replica tablet's sole host —
// `has_data` must force full-voter re-formation despite a bumped epoch.
// ---------------------------------------------------------------------------

fn scenario_crash_restart_single(seed: u64) {
    run(seed, |sim| async move {
        let env = sim.env(a());
        let mut c = Cluster::new(sim);
        c.add_node(a());

        let v1 = view([tablet(1, b"", None, vec![a()])]);
        c.tick(a(), &v1).await;
        env.sleep(Duration::from_secs(2)).await;
        let h1 = c.node(a()).hosted_node(TabletId(1)).unwrap().clone();
        for i in 0..5u64 {
            h1.put(format!("k{i}").into_bytes(), format!("v{i}").into_bytes());
        }
        env.sleep(Duration::from_secs(1)).await;

        c.crash_restart(a());
        // Note: `env` (a `SimEnv` handle for node a()) stays valid across the
        // crash — `Clock::sleep` schedules against the simulator's global
        // timeline, not any per-node task list, so it's unaffected by
        // `Simulator::stop`'s task-killing (only tasks *spawned* on a node's
        // env, like `RaftKvNode`'s driver loop, get killed).

        // The restart view: unchanged replica set, but a bumped epoch — as if
        // some unrelated placement event advanced it while a() was down. WITHOUT
        // the `has_data` restart-upgrade, `plan_join_host` would say "join as
        // non-voter", which for a single-replica tablet can NEVER elect.
        let v2 = view([tablet_at_epoch(1, b"", None, vec![a()], 5)]);
        c.tick(a(), &v2).await;
        env.sleep(Duration::from_secs(2)).await;

        let h1b = c.node(a()).hosted_node(TabletId(1)).unwrap().clone();
        assert!(
            wait_until(&env, 50, Duration::from_millis(100), || h1b.is_leader()).await,
            "the restarted sole replica never re-elected itself (has_data upgrade failed)"
        );
        for i in 0..5u64 {
            assert_eq!(
                h1b.linearizable_get(format!("k{i}").as_bytes()).await,
                Some(format!("v{i}").into_bytes()),
                "data written before the crash must survive the restart"
            );
        }

        assert_hosted_converged(&c, a(), [TabletId(1)]);
        assert_idempotent(&mut c, a(), &v2).await;
    });
}

// ---------------------------------------------------------------------------
// Scenario 13: crash+restart of a FOLLOWER in a 2-replica group.
// ---------------------------------------------------------------------------

fn scenario_crash_restart_follower(seed: u64) {
    run(seed, |sim| async move {
        let env_a = sim.env(a());
        let mut c = Cluster::new(sim);
        c.add_node(a());
        c.add_node(b());

        let v1 = view([tablet(1, b"", None, vec![a(), b()])]);
        c.tick_all(&[a(), b()], &v1).await;
        env_a.sleep(Duration::from_secs(2)).await;

        let ha = c.node(a()).hosted_node(TabletId(1)).unwrap().clone();
        let hb = c.node(b()).hosted_node(TabletId(1)).unwrap().clone();
        let leader = if ha.is_leader() { &ha } else { &hb };
        for i in 0..5u64 {
            leader.put(format!("k{i}").into_bytes(), format!("v{i}").into_bytes());
        }
        env_a.sleep(Duration::from_secs(2)).await;

        // Restart b() (whichever role it currently holds), at a bumped epoch —
        // simulating an unrelated earlier reconfigure event, so b()'s own
        // `has_data` (not epoch<=INITIAL) is what must drive full re-formation.
        c.crash_restart(b());
        let v2 = view([tablet_at_epoch(1, b"", None, vec![a(), b()], 3)]);
        c.tick(b(), &v2).await;
        env_a.sleep(Duration::from_secs(1)).await;
        c.tick(a(), &v2).await; // a()'s own Reconfigure/no-op pass on the same view
        env_a.sleep(Duration::from_secs(3)).await;

        let hb2 = c.node(b()).hosted_node(TabletId(1)).unwrap().clone();
        assert!(
            wait_until(&env_a, 80, Duration::from_millis(100), || {
                hb2.config().contains(&b())
            })
            .await,
            "the restarted follower never rejoined as a real voter"
        );
        for i in 0..5u64 {
            assert_eq!(
                hb2.local_get(format!("k{i}").as_bytes()).await,
                Some(format!("v{i}").into_bytes()),
                "the restarted follower must recover all prior data (no loss)"
            );
        }

        assert_hosted_converged(&c, a(), [TabletId(1)]);
        assert_hosted_converged(&c, b(), [TabletId(1)]);
        assert_idempotent(&mut c, a(), &v2).await;
        assert_idempotent(&mut c, b(), &v2).await;
    });
}

// ---------------------------------------------------------------------------
// Scenario 14: a real removal, then a "control-plane replay" epoch flicker —
// the release-confirm dampener must reset on each epoch change and only
// fire once the excluded state has been stable for RELEASE_CONFIRM_TICKS.
// ---------------------------------------------------------------------------

fn scenario_replay_epoch_flicker(seed: u64) {
    run(seed, |sim| async move {
        let env = sim.env(a());
        let other_env = sim.env(b());
        let mut c = Cluster::new(sim);
        c.add_node(a());

        let v1 = view([tablet(1, b"", None, vec![a(), b()])]);
        c.tick(a(), &v1).await;
        env.sleep(Duration::from_secs(2)).await;

        let b_storage = MemoryEngine::new();
        let hb = KvNode::start_hosted(
            other_env,
            vec![a(), b()],
            b_storage,
            StorageScope::new(prefix_for(TABLE), KeyRange::whole()),
            1,
        );
        env.sleep(Duration::from_secs(2)).await;
        let ha = c.node(a()).hosted_node(TabletId(1)).unwrap().clone();

        remove_replica_for_real(&env, &ha, a(), &hb, b(), [b()].into_iter().collect()).await;

        // One qualifying tick at epoch 2 (count -> 1).
        let excluded_e2 = view([tablet_at_epoch(1, b"", None, vec![b()], 2)]);
        c.tick(a(), &excluded_e2).await;
        assert!(
            c.node(a())
                .local_state()
                .pending_release
                .get(&TabletId(1))
                .is_some_and(|&(_, t)| t == 1),
            "the first qualifying tick must start the confirm counter"
        );

        // a() "replay" epoch bump WHILE still excluded (e.g. an unrelated
        // placement event) — the dampener must RESET, not advance.
        let excluded_e3 = view([tablet_at_epoch(1, b"", None, vec![b()], 3)]);
        c.tick(a(), &excluded_e3).await;
        assert!(
            c.node(a())
                .local_state()
                .pending_release
                .get(&TabletId(1))
                .is_some_and(|&(_, t)| t == 1),
            "an epoch change mid-count must reset the confirm counter, not advance it"
        );
        assert!(
            c.hosted_set(a()).contains(&TabletId(1)),
            "no premature release"
        );

        // Now hold epoch 3 stable for the remaining confirm ticks.
        for _ in 0..5 {
            c.tick(a(), &excluded_e3).await;
            env.sleep(Duration::from_millis(50)).await;
        }

        assert_hosted_converged(&c, a(), []);
        assert_eq!(c.teardown_log(a()), vec![TabletId(1)]);
        assert_all_stopped(&[ha]);
        assert_idempotent(&mut c, a(), &excluded_e3).await;
    });
}

// ---------------------------------------------------------------------------
// Scenario 15: a boundary/contract test — feeding a present->absent->present
// sequence (bypassing the caller's `last_applied == 0` recovery guard, which
// `Reconciler`/`plan` deliberately do NOT take themselves — see `host.rs`'s
// doc on `tablets_to_reclaim`) DOES reclaim+erase on the transient absence,
// since `Reclaim` (unlike `Release`) has no dampener. This documents WHY the
// caller-side guard is load-bearing, not a bug in this crate.
// ---------------------------------------------------------------------------

fn scenario_replay_absent_then_present(seed: u64) {
    run(seed, |sim| async move {
        let env = sim.env(a());
        let mut c = Cluster::new(sim);
        c.add_node(a());

        let v1 = view([tablet(1, b"", None, vec![a()])]);
        c.tick(a(), &v1).await;
        env.sleep(Duration::from_secs(2)).await;
        let h1 = c.node(a()).hosted_node(TabletId(1)).unwrap().clone();
        h1.put(b"k".to_vec(), b"v".to_vec());
        env.sleep(Duration::from_secs(1)).await;
        assert_present(c.storage(a()), &physical(b"k"), b"v").await;

        // a() transient "absent" view (e.g. the caller ticked mid control-plane
        // WAL replay, before recovery reached the tablet's re-creation entry).
        let v_absent = view([]);
        c.tick(a(), &v_absent).await;
        assert_hosted_converged(&c, a(), []);
        assert_eq!(c.teardown_log(a()), vec![TabletId(1)]);
        assert_all_stopped(&[h1]);
        assert_absent(c.storage(a()), &physical(b"k")).await;

        // The tablet "reappears" (replay catches up to its final, settled
        // state) — a brand-new Host, with no memory of the erased data.
        let v_present_again = view([tablet(1, b"", None, vec![a()])]);
        c.tick(a(), &v_present_again).await;
        env.sleep(Duration::from_secs(2)).await;
        let h1b = c.node(a()).hosted_node(TabletId(1)).unwrap().clone();
        assert!(h1b.is_leader());
        assert_eq!(
            h1b.linearizable_get(b"k").await,
            None,
            "Reclaim has no dampener by design — a caller that ticks during \
             replay (skipping the documented last_applied==0 guard) genuinely \
             loses data; this is the contract boundary, not a bug here"
        );

        assert_hosted_converged(&c, a(), [TabletId(1)]);
        assert_idempotent(&mut c, a(), &v_present_again).await;
    });
}

// ---------------------------------------------------------------------------
// Scenario 16: a network partition prevents the removal entry from ever
// reaching the excluded node — Release must NOT fire until the partition
// heals and `config_excludes_me` becomes a real, durably-observed fact.
// ---------------------------------------------------------------------------

fn scenario_partition_blocks_release(seed: u64) {
    run(seed, |sim| async move {
        let sim2 = sim.clone();
        let env = sim.env(a());
        let other_env = sim.env(b());
        let mut c = Cluster::new(sim);
        c.add_node(a());

        let v1 = view([tablet(1, b"", None, vec![a(), b()])]);
        c.tick(a(), &v1).await;
        env.sleep(Duration::from_secs(2)).await;

        let b_storage = MemoryEngine::new();
        let hb = KvNode::start_hosted(
            other_env,
            vec![a(), b()],
            b_storage,
            StorageScope::new(prefix_for(TABLE), KeyRange::whole()),
            1,
        );
        env.sleep(Duration::from_secs(2)).await;
        let ha = c.node(a()).hosted_node(TabletId(1)).unwrap().clone();
        for i in 0..3u64 {
            let leader = if ha.is_leader() { &ha } else { &hb };
            leader.put(format!("k{i}").into_bytes(), format!("v{i}").into_bytes());
        }
        env.sleep(Duration::from_secs(1)).await;

        // If b() currently leads, that's fine — either way we need b() to be able
        // to remove a() while a() is unreachable, so make b() the leader first.
        // Retried (never single-shot): `transfer_leadership`/`is_leader` can
        // transiently disagree with a concurrent election at seed depth.
        let became_leader = wait_until(&env, 80, Duration::from_millis(100), || {
            if hb.is_leader() {
                return true;
            }
            if ha.is_leader() {
                ha.transfer_leadership(b());
            }
            false
        })
        .await;
        assert!(became_leader, "b() never took over leadership");
        env.sleep(Duration::from_millis(200)).await; // let any armed-transfer freeze clear

        // Partition a() away from b(), then get b()'s removal proposal accepted
        // (it must NOT commit while partitioned — that's the whole point of
        // this scenario). Retried: `change_membership` can transiently
        // return `NotLeader` (e.g. a leadership-transfer freeze elsewhere, or
        // a fresh election) — a benign timing blip, not a reason to fail.
        sim2.partition_pair(a(), b());
        let accepted = wait_until(&env, 80, Duration::from_millis(100), || {
            matches!(
                hb.change_membership([b()].into_iter().collect()),
                ProposeResult::Accepted { .. }
            )
        })
        .await;
        assert!(
            accepted,
            "b() never got its removal proposal accepted while partitioned"
        );
        env.sleep(Duration::from_secs(3)).await;
        assert!(
            ha.config().contains(&a()),
            "a partitioned-away node must not observe an exclusion it never received"
        );

        // Feed the excluded metadata view anyway — Release must NOT fire since
        // the safety anchor (this node's own durable config) disagrees.
        let v2 = view([tablet(1, b"", None, vec![b()])]);
        for _ in 0..8 {
            c.tick(a(), &v2).await;
            env.sleep(Duration::from_millis(50)).await;
        }
        assert_hosted_converged(&c, a(), [TabletId(1)]);
        assert!(
            c.teardown_log(a()).is_empty(),
            "release must not fire while partitioned"
        );
        assert_present(c.storage(a()), &physical(b"k0"), b"v0").await;

        // Heal — the removal entry finally reaches a(), then release proceeds.
        sim2.heal(a(), b());
        assert!(
            wait_until(&env, 80, Duration::from_millis(100), || !ha
                .config()
                .contains(&a()))
            .await,
            "a()'s own durable config never excluded it after healing"
        );
        for _ in 0..8 {
            c.tick(a(), &v2).await;
            env.sleep(Duration::from_millis(50)).await;
        }

        assert_hosted_converged(&c, a(), []);
        assert_eq!(c.teardown_log(a()), vec![TabletId(1)]);
        assert_all_stopped(&[ha]);
        assert_idempotent(&mut c, a(), &v2).await;
    });
}

// ---------------------------------------------------------------------------
// Scenario 17: the split-then-immediate-release race, driven deterministically
// at ZERO ticks between the narrow-view and the release-eligible view — this
// node never observes "narrowed but still included", only "narrowed AND
// excluded" in one leap. The sibling-sparing invariant must still hold
// (`erase_bound` is always the CURRENT metadata range, never the group's own
// possibly stale-wide `scope_range()` fact).
// ---------------------------------------------------------------------------

fn scenario_split_then_immediate_release(seed: u64) {
    run(seed, |sim| async move {
        let env = sim.env(a());
        let other_env = sim.env(b());
        let mut c = Cluster::new(sim);
        c.add_node(a());

        let v1 = view([tablet(1, b"", None, vec![a(), b()])]);
        c.tick(a(), &v1).await;
        env.sleep(Duration::from_secs(2)).await;

        let b_storage = MemoryEngine::new();
        let hb = KvNode::start_hosted(
            other_env,
            vec![a(), b()],
            b_storage,
            StorageScope::new(prefix_for(TABLE), KeyRange::whole()),
            1,
        );
        env.sleep(Duration::from_secs(2)).await;
        let ha = c.node(a()).hosted_node(TabletId(1)).unwrap().clone();
        for i in 0..5u64 {
            let leader = if ha.is_leader() { &ha } else { &hb };
            leader.put(
                format!("a{i:02}").into_bytes(),
                format!("lo{i}").into_bytes(),
            );
            leader.put(
                format!("z{i:02}").into_bytes(),
                format!("hi{i}").into_bytes(),
            );
        }
        env.sleep(Duration::from_secs(1)).await;

        // Host the sibling (co-hosted, upper half) BEFORE the removal — this
        // node keeps replicating it after tablet 1 is released.
        let v_with_sibling = view([
            tablet(1, b"", None, vec![a(), b()]), // tablet 1 still WIDE (pre-split) here
            tablet(2, BOUNDARY, None, vec![a()]),
        ]);
        c.tick(a(), &v_with_sibling).await;
        env.sleep(Duration::from_secs(2)).await;
        let h2 = c.node(a()).hosted_node(TabletId(2)).unwrap().clone();
        assert_eq!(h2.local_get(b"z00").await, Some(b"hi0".to_vec()));

        // The real removal completes WHILE tablet 1's scope is still WIDE —
        // a()'s reconciler never got a chance to run NarrowScope for it (once
        // excluded, `plan_join_host` returns None and phase 1 skips it
        // entirely — see `plan`'s Phase 1 doc).
        remove_replica_for_real(&env, &ha, a(), &hb, b(), [b()].into_iter().collect()).await;
        assert_eq!(
            ha.scope_range(),
            KeyRange::whole(),
            "sanity: tablet 1's live scope is still stale-wide at the moment of exclusion"
        );

        // In ONE leap, this node's view shows tablet 1 BOTH narrowed (the
        // split committed) AND excluding it — zero ticks in between.
        let v_final = view([
            tablet(1, b"", Some(BOUNDARY), vec![b()]),
            tablet(2, BOUNDARY, None, vec![a()]),
        ]);
        for _ in 0..8 {
            c.tick(a(), &v_final).await;
            env.sleep(Duration::from_millis(50)).await;
        }

        assert_hosted_converged(&c, a(), [TabletId(2)]);
        assert_eq!(c.teardown_log(a()), vec![TabletId(1)]);
        assert_all_stopped(&[ha]);

        for i in 0..5u64 {
            assert_absent(
                c.storage(a()),
                &physical(format!("a{i:02}").into_bytes().as_slice()),
            )
            .await;
            assert_present(
                c.storage(a()),
                &physical(format!("z{i:02}").into_bytes().as_slice()),
                format!("hi{i}").into_bytes().as_slice(),
            )
            .await;
        }

        assert_idempotent(&mut c, a(), &v_final).await;
    });
}

// ---------------------------------------------------------------------------
// Scenario 18: a re-add after exclusion cancels a pending release outright
// (not just delays it) — with a REAL removal providing `config_excludes_me`.
// ---------------------------------------------------------------------------

fn scenario_re_add_cancels_release(seed: u64) {
    run(seed, |sim| async move {
        let env = sim.env(a());
        let other_env = sim.env(b());
        let mut c = Cluster::new(sim);
        c.add_node(a());

        let v1 = view([tablet(1, b"", None, vec![a(), b()])]);
        c.tick(a(), &v1).await;
        env.sleep(Duration::from_secs(2)).await;

        let b_storage = MemoryEngine::new();
        let hb = KvNode::start_hosted(
            other_env,
            vec![a(), b()],
            b_storage,
            StorageScope::new(prefix_for(TABLE), KeyRange::whole()),
            1,
        );
        env.sleep(Duration::from_secs(2)).await;
        let ha = c.node(a()).hosted_node(TabletId(1)).unwrap().clone();

        remove_replica_for_real(&env, &ha, a(), &hb, b(), [b()].into_iter().collect()).await;

        let excluded = view([tablet(1, b"", None, vec![b()])]);
        c.tick(a(), &excluded).await;
        assert!(
            c.node(a())
                .local_state()
                .pending_release
                .contains_key(&TabletId(1)),
            "the first qualifying tick must start the confirm counter"
        );

        // Metadata gains a() back (a re-add — purely a metadata-level fact for
        // this scenario, mirroring `host.rs`'s `a_re_add_cancels_a_pending_release`
        // unit test: the candidacy check only inspects `Metadata.tablets[t]
        // .replicas`, independent of what this node's own durable Raft config
        // still (separately) says).
        let readded = view([tablet(1, b"", None, vec![a(), b()])]);
        c.tick(a(), &readded).await;
        assert!(
            !c.node(a())
                .local_state()
                .pending_release
                .contains_key(&TabletId(1)),
            "a re-add must cancel the pending release outright"
        );
        assert_hosted_converged(&c, a(), [TabletId(1)]);
        assert!(
            c.teardown_log(a()).is_empty(),
            "the tablet must never have been released"
        );

        // Ticking the re-added view repeatedly must not spuriously release it.
        for _ in 0..5 {
            c.tick(a(), &readded).await;
            env.sleep(Duration::from_millis(50)).await;
        }
        assert_hosted_converged(&c, a(), [TabletId(1)]);
        assert!(c.teardown_log(a()).is_empty());

        assert_idempotent(&mut c, a(), &readded).await;
    });
}

// ---------------------------------------------------------------------------
// Scenario 19: ADR 0018 §2 amendment fix (the split_cluster.rs livelock) —
// the replica that first narrows its own scope while a
// FOLLOWER, and is only LATER promoted to sole leader (its own leadership
// transfer having removed the original leader outright), must still
// eventually propose the split's range-seal. A one-shot design — propose
// only as an immediate side effect of the same tick that narrows — can
// never satisfy this: by the time this replica is promoted, its own local
// scope already matches the target range, so the mismatch that would have
// re-triggered the attempt is permanently gone. `pending_seals` must be a
// condition re-derived fresh every tick, independent of local scope state.
// ---------------------------------------------------------------------------

fn scenario_narrow_seal_survives_a_late_promotion(seed: u64) {
    run(seed, |sim| async move {
        let env = sim.env(a());
        let mut c = Cluster::new(sim);
        c.add_node(a());
        c.add_node(b());

        let v1 = view([tablet(1, b"", None, vec![a(), b()])]);
        c.tick_all(&[a(), b()], &v1).await;
        env.sleep(Duration::from_secs(2)).await; // elect

        let h1a = c.node(a()).hosted_node(TabletId(1)).unwrap().clone();
        let h1b = c.node(b()).hosted_node(TabletId(1)).unwrap().clone();
        let (leader0_id, leader0, follower_id, follower) = if h1a.is_leader() {
            (a(), h1a.clone(), b(), h1b.clone())
        } else {
            assert!(h1b.is_leader(), "tablet 1 must have elected some leader");
            (b(), h1b.clone(), a(), h1a.clone())
        };

        // The split's atomic control-plane effect (ADR 0028): the source
        // narrows AND the fresh child appears in the SAME view, with
        // provenance recorded — mirrors `MetaCommand::SplitTablet`'s single
        // apply exactly (unlike `scenario_split_narrow_sibling`'s
        // deliberately staged introduction, which isn't required to be
        // atomic since it only exercises `plan`'s reaction one step at a
        // time).
        let v2 = MetadataView {
            tablets: [
                tablet(1, b"", Some(BOUNDARY), vec![a(), b()]),
                tablet(2, BOUNDARY, None, vec![a(), b()]),
            ]
            .into_iter()
            .map(|t| (t.id, t))
            .collect(),
            split_parent: [(TabletId(2), TabletId(1))].into_iter().collect(),
            ..Default::default()
        };

        // Tick ONLY the follower — it narrows its own scope unconditionally
        // (regardless of leadership) but, correctly, proposes nothing (not
        // leader). The leader is never ticked with `v2` at all here.
        c.tick(follower_id.clone(), &v2).await;
        assert_eq!(
            follower.scope_range(),
            KeyRange::new(b"".to_vec(), Some(BOUNDARY.to_vec())),
            "the follower must still narrow its own scope locally"
        );
        assert!(
            c.node(follower_id.clone())
                .hosted_node(TabletId(2))
                .is_none(),
            "the child must not host until the parent's seal is observed"
        );

        // Force a REAL leadership change: remove the ORIGINAL leader from
        // tablet 1's Raft config outright, leaving the follower — the one
        // that already narrowed while it was NOT leader — as the sole
        // remaining voter (hence trivially its new leader).
        remove_replica_for_real(
            &env,
            &leader0,
            leader0_id.clone(),
            &follower,
            follower_id.clone(),
            [follower_id.clone()].into_iter().collect(),
        )
        .await;
        assert!(follower.is_leader(), "the sole remaining voter must lead");

        // Tick the (now-leader, already-narrowed) follower again with the
        // SAME view. Its own local scope already matches `v2`'s target
        // range, so a one-shot "propose only as a side effect of narrowing"
        // design has nothing left to trigger on — this is the exact
        // regression: `pending_seals` must instead keep naming this range
        // until a covering seal is actually observed, independent of local
        // scope state, so this now-leader tick finally proposes it.
        let mut child_hosted = false;
        for _ in 0..20 {
            c.tick(follower_id.clone(), &v2).await;
            env.sleep(Duration::from_millis(200)).await;
            if c.node(follower_id.clone())
                .hosted_node(TabletId(2))
                .is_some()
            {
                child_hosted = true;
                break;
            }
        }
        assert!(
            child_hosted,
            "the split's child never hosted — the promoted replica never proposed the \
             parent's range-seal (the split_cluster.rs regression)"
        );

        assert_hosted_converged(&c, follower_id, [TabletId(1), TabletId(2)]);
    });
}

// ---------------------------------------------------------------------------
// ADR 0044 phase-1 PR4: wake-on-demand + fork H (proactive wake on `down`).
//
// Neither scenario wires production `--quiesce-after` (that's PR7's job) —
// each calls `RaftKvNode::enable_quiescence` directly on every replica right
// after hosting, exactly like `animus-cp-data/tests/quiescence.rs` does for
// the pure-core corpus.
// ---------------------------------------------------------------------------

/// Short relative to this file's own `env.sleep` step granularity, long
/// enough that ordinary election/heartbeat settle traffic (well within a
/// couple hundred ms) has already died down before the idle clock starts
/// counting — mirrors `tests/quiescence.rs`'s own `QUIESCE_AFTER` choice.
const RECONCILER_QUIESCE_AFTER: Duration = Duration::from_millis(200);

/// Fork H's whole reason to exist: a quiesced group's consensus loop parks
/// with **no timer at all** (`RaftCore::next_deadline() == None`), so a
/// follower whose leader genuinely dies while both are dormant has nothing
/// that will ever wake it on its own — worse availability than before
/// quiescence existed, the TiKV-hibernate-regions hazard. Kill the leader's
/// own tasks outright (its own group handle goes inert; the survivors' own
/// envs/streams are untouched) and drive only `Reconciler::tick` (never a
/// raw message/timer) on the survivors with a view marking the dead node
/// `down` — if `tick` doesn't call `RaftKvNode::wake()` on the affected
/// group, this scenario times out with no new leader ever elected.
fn scenario_quiesced_group_wakes_when_a_replica_goes_down(seed: u64) {
    run(seed, |sim| async move {
        let env = sim.env(a());
        let mut c = Cluster::new(sim);
        c.add_node(a());
        c.add_node(b());
        c.add_node(node_c());

        let v1 = view([tablet(1, b"", None, vec![a(), b(), node_c()])]);
        c.tick_all(&[a(), b(), node_c()], &v1).await;
        env.sleep(Duration::from_secs(2)).await; // elect

        let ha = c.node(a()).hosted_node(TabletId(1)).unwrap().clone();
        let hb = c.node(b()).hosted_node(TabletId(1)).unwrap().clone();
        let hc = c.node(node_c()).hosted_node(TabletId(1)).unwrap().clone();
        for h in [&ha, &hb, &hc] {
            h.enable_quiescence(RECONCILER_QUIESCE_AFTER);
        }

        let quiesced = wait_until(&env, 150, Duration::from_millis(50), || {
            ha.is_quiesced() && hb.is_quiesced() && hc.is_quiesced()
        })
        .await;
        assert!(
            quiesced,
            "every replica should reach quiescence while genuinely idle"
        );

        let (leader_id, survivors): (NodeId, Vec<(NodeId, KvNode)>) = if ha.is_leader() {
            (a(), vec![(b(), hb.clone()), (node_c(), hc.clone())])
        } else if hb.is_leader() {
            (b(), vec![(a(), ha.clone()), (node_c(), hc.clone())])
        } else {
            assert!(hc.is_leader(), "tablet 1 must have elected some leader");
            (node_c(), vec![(a(), ha.clone()), (b(), hb.clone())])
        };

        // Kill the leader's own tasks — the survivors' own reconcilers/
        // driver tasks run on independent envs/streams, untouched.
        c.sim.stop(leader_id.clone());

        let survivor_ids: Vec<NodeId> = survivors.iter().map(|(id, _)| id.clone()).collect();
        let down_view = view_with_down(
            [tablet(1, b"", None, [a(), b(), node_c()].to_vec())],
            [leader_id.clone()],
        );

        let mut elected = false;
        for _ in 0..200 {
            c.tick_all(&survivor_ids, &down_view).await;
            env.sleep(Duration::from_millis(50)).await;
            if survivors.iter().any(|(_, h)| h.is_leader()) {
                elected = true;
                break;
            }
        }
        assert!(
            elected,
            "a quiesced group's surviving replicas never noticed their leader \
             die and elect a new one — fork H's proactive wake regression"
        );

        let new_leader = survivors
            .iter()
            .find(|(_, h)| h.is_leader())
            .expect("checked above")
            .1
            .clone();
        let pr = new_leader.put(b"k".to_vec(), b"v".to_vec());
        assert!(
            matches!(pr, ProposeResult::Accepted { .. }),
            "the recovered leader must still accept writes: {pr:?}"
        );
    });
}

/// Quiescence must not interfere with an in-flight split: a quiesced
/// source's own `narrow_scope` is a pure local mutation the reconciler
/// performs directly (no Raft round trip, unaffected by whether the
/// consensus loop currently ticks), and its `ProposeSeal` action relies on
/// the pre-existing wake-on-propose plumbing to transparently un-quiesce the
/// group for its own seal proposal.
fn scenario_quiesce_races_a_split_seal_handoff(seed: u64) {
    run(seed, |sim| async move {
        let env = sim.env(a());
        let mut c = Cluster::new(sim);
        c.add_node(a());

        let v1 = view([tablet(1, b"", None, vec![a()])]);
        c.tick(a(), &v1).await;
        env.sleep(Duration::from_secs(2)).await;

        let h1 = c.node(a()).hosted_node(TabletId(1)).unwrap().clone();
        h1.enable_quiescence(RECONCILER_QUIESCE_AFTER);
        for i in 0..5u64 {
            h1.put(
                format!("a{i:02}").into_bytes(),
                format!("lo{i}").into_bytes(),
            );
            h1.put(
                format!("z{i:02}").into_bytes(),
                format!("hi{i}").into_bytes(),
            );
        }

        let quiesced = wait_until(&env, 150, Duration::from_millis(50), || h1.is_quiesced()).await;
        assert!(
            quiesced,
            "the source tablet should quiesce while idle, even with a split about to land"
        );

        // The split lands: narrow the source. This must take effect on a
        // currently-quiesced group.
        let v2 = view([tablet(1, b"", Some(BOUNDARY), vec![a()])]);
        c.tick(a(), &v2).await;
        assert_eq!(
            h1.scope_range(),
            KeyRange::new(b"".to_vec(), Some(BOUNDARY.to_vec())),
            "a quiesced group's own reconciler must still narrow its scope on a split"
        );

        let v3 = view([
            tablet(1, b"", Some(BOUNDARY), vec![a()]),
            tablet(2, BOUNDARY, None, vec![a()]),
        ]);
        c.tick(a(), &v3).await;
        env.sleep(Duration::from_secs(2)).await;

        let h2 = c.node(a()).hosted_node(TabletId(2)).unwrap().clone();
        assert!(
            h2.is_leader(),
            "the fresh sibling must self-elect even though its source quiesced"
        );
        for i in 0..5u64 {
            assert_eq!(
                h1.local_get(format!("a{i:02}").as_bytes()).await,
                Some(format!("lo{i}").into_bytes()),
                "source tablet must still see its own (lower-half) data"
            );
            assert_eq!(
                h2.local_get(format!("z{i:02}").as_bytes()).await,
                Some(format!("hi{i}").into_bytes()),
                "sibling tablet must see its own (upper-half) data — no double count"
            );
        }
        assert_hosted_converged(&c, a(), [TabletId(1), TabletId(2)]);
        assert_idempotent(&mut c, a(), &v3).await;
    });
}

// ---------------------------------------------------------------------------
// The tests.
// ---------------------------------------------------------------------------

#[test]
fn reconciler_corpus_runs_every_scenario() {
    for s in corpus() {
        (s.run)(s.seed);
    }
}

/// Coverage/structural guard: names + seeds are unique, the frozen cells keep
/// their canonical name-derived seeds, and the corpus has not silently shrunk.
#[test]
fn reconciler_corpus_names_and_seeds_are_unique_and_frozen() {
    let cells = scenario_cells();
    assert!(
        cells.len() >= 15,
        "corpus shrank unexpectedly to {} cells",
        cells.len()
    );

    let names: BTreeSet<&str> = cells.iter().map(|s| s.name).collect();
    assert_eq!(names.len(), cells.len(), "corpus names must be unique");
    let seeds: BTreeSet<u64> = cells.iter().map(|s| s.seed).collect();
    assert_eq!(seeds.len(), cells.len(), "corpus seeds must be unique");

    for cell in &cells {
        assert_eq!(
            cell.seed,
            name_seed(cell.name),
            "frozen seed moved for {}",
            cell.name
        );
    }
}

/// Seed-depth lever (`ANIMUS_RECONCILER_SEEDS`): expanding by `k` yields
/// exactly `k×` scenarios, all uniquely named/seeded, and **variant 0
/// preserves the canonical (frozen) name+seed** — growing depth never moves a
/// regression seed. Structural only (mirrors the sibling corpora's guard).
#[test]
fn reconciler_corpus_seed_expansion_is_additive_and_unique() {
    let base = scenario_cells();
    let k = 3;
    let expanded = seed_expand(scenario_cells(), k);
    assert_eq!(expanded.len(), base.len() * k);

    let names: BTreeSet<&str> = expanded.iter().map(|s| s.name).collect();
    assert_eq!(names.len(), expanded.len(), "expanded names must be unique");
    let seeds: BTreeSet<u64> = expanded.iter().map(|s| s.seed).collect();
    assert_eq!(seeds.len(), expanded.len(), "expanded seeds must be unique");

    for b in &base {
        let kept = expanded
            .iter()
            .find(|s| s.name == b.name)
            .unwrap_or_else(|| panic!("base scenario {} missing after expansion", b.name));
        assert_eq!(kept.seed, b.seed, "seed moved for {}", b.name);
    }
    assert_eq!(seed_expand(scenario_cells(), 1).len(), base.len());
}

/// a() single deterministic replay of one scenario twice must behave
/// identically (ADR 0003) — same converged hosted set, same data. a() cheap
/// smoke test rather than a byte-identical history dump (this corpus has no
/// `History` recorder, unlike the Elle corpora); reruns the cheapest scenario
/// twice and cross-checks its own internal assertions pass both times (they
/// already run under a fixed seed, so a flake here would mean a real
/// determinism hole).
#[test]
fn reconciler_scenario_is_reproducible_from_its_seed() {
    let seed = name_seed("fresh_whole_keyspace_host_elect_serve");
    scenario_fresh_single_node(seed);
    scenario_fresh_single_node(seed);
}
