//! The first cycles/durability corpus over [`super::sim_cluster::
//! SimCluster`] (ADR 0061 rung D1, C-04 D1 step 3): a list-append
//! [`Recorder`]/[`History`] model over `SimClusterHandle::put`/`get`/
//! `delete`, checked with `animus_test::check::{check_cycles,
//! check_durability, check_convergence}` — the identical oracle
//! `animus-test/tests/raftkv_linearizable.rs` (the "flagship" corpus, and
//! this file's own model) uses for the single-tablet leaderful data plane,
//! now driven over a real multi-node, multi-tablet `animusd::ClientCtx`
//! cluster (control-plane Raft + the CP data plane + the ADR 0061 rung
//! C3d relay wire) instead of a bare `RaftKvNode`.
//!
//! # Why this needed [`SimClusterHandle`], not just `SimCluster`
//!
//! `SimCluster` itself (see that module's own doc) is `&mut self`
//! everywhere — every fault-injection method (`crash`/`restart`/
//! `partition`/`heal_all`) and even its own `put`/`get`/`delete`/`scan`
//! take `&mut self`, because each of those blocking calls drives the
//! simulator (`Simulator::run_for`) internally. That works for the
//! fixture's own 11 tests (one client, one op at a time, from the test's
//! own thread) but not for a corpus workload, which needs **several
//! concurrent client tasks** issuing overlapping ops from different nodes
//! while the *driver* separately advances virtual time and injects faults
//! — exactly `raftkv_linearizable.rs`'s own `client_loop`/`Group::apply`
//! split, generalized to a whole cluster. `SimClusterHandle` (added by
//! this rung, `sim_cluster.rs`) is the `Clone`-able, `Mutex`-backed handle
//! that makes that split possible here too: its `put`/`get`/`delete`/
//! `scan` are plain `&self` async methods with no internal simulator
//! driving (each is already self-bounded by `ClientCtx`'s own internal
//! `CLIENT_TIMEOUT`-scoped retry loops — see that type's own doc), so a
//! client task spawned via `env.spawn_task` can `.await` one directly
//! while this file's own runner drives the simulator forward exactly once
//! per scenario, in one loop, exactly like `raftkv_linearizable.rs`'s own
//! `run_scenario_on`.
//!
//! # The workload
//!
//! Single-writer-per-key list-append, precisely `raftkv_linearizable.rs`'s
//! own workload shape (see that file's module doc for why this makes
//! `check_cycles` sound and non-vacuous at all): each of `clients`
//! concurrent client tasks owns a fixed subset of keys (`key % clients ==
//! proc`) and only ever appends to its own; a read may observe any key.
//! Every op's issuing **node** is drawn fresh from the scenario's own
//! seeded RNG each round (`env.gen_below(node_count)`) — not fixed to the
//! tablet's leader or to node 0 — so a scenario with more nodes than
//! replicas (`forward_heavy`, below) routinely issues ops from a node
//! hosting no local replica at all, exercising `ClientCtx::forward_to_
//! tablet_leader`/`cp_serve_forwarded` over the real `SimRelayClient` wire
//! rather than only ever a local leader-side call.
//!
//! **Multiple tables share one `Key` space without conflating their
//! histories** (needed by the `two_tables` cell): a `Key` embeds which
//! table it belongs to in its own high-order digits
//! (`TABLE_KEY_STRIDE`, [`table_pk_sk`]) rather than the `Mop`/`History`
//! model gaining a table field of its own (a change to `animus-test`'s
//! shared, cross-crate `Mop` type this one corpus doesn't need). Two
//! different tables' own key `0` therefore recover as two *different*
//! `Key` values to the checker, so `check_cycles`'s `recover` never tries
//! to reconcile one table's observed list against another's — the
//! precise false-positive `raftkv_linearizable.rs`'s own crate guide
//! entry on "single-writer-per-key... a workload-design tool" warns a
//! multi-writer key would manufacture, here for a multi-*table* key
//! instead.
//!
//! # `delete` is exercised, but deliberately kept OUT of the Elle model
//!
//! `check_cycles`'s `recover` (`animus-test::check`) requires every
//! **observed** read of a key to be a *prefix* of that key's single
//! recovered (longest-observed) append order — a list-append CRDT
//! invariant that a `delete` (which does not append, it clears) cannot
//! satisfy: a deleted-then-reappended key's later observations would
//! legitimately NOT be a superset of an earlier one, which the checker
//! would (correctly, given its own model) flag as a divergence — a
//! false-positive artifact of the *workload*, not a real bug, exactly the
//! trap `raftkv_linearizable.rs`'s own doc names for value reuse. Rather
//! than teach the shared, cross-crate `check_cycles` a tombstone
//! semantics it has no other caller for, `delete` gets its own **direct**
//! correctness check instead ([`run_delete_probe`]): after every
//! scenario's fault schedule has healed and drained, this file separately
//! puts, reads, deletes, and re-reads one dedicated key **from every node
//! in the cluster in turn** — `node_count` round trips, so a cell with a
//! node hosting no tablet replica (`forward_heavy`) proves a forwarded
//! `delete` too, not just a forwarded `put`/`get`. This exercises the real
//! `cp_kind_write_raw` delete path (a `None` value write, ADR 0049) end to
//! end without ever feeding a delete into the append-only checker.
//!
//! # The cells
//!
//! `baseline` (no faults, 3 nodes, RF 3), `leader_crash` (crash the
//! tablet leader), `follower_crash` (crash a non-leader replica),
//! `stop_restart` (a **true process restart** — `SimCluster::restart`'s
//! wipe-and-rejoin, not a mute — of the tablet leader), `leader_partition`
//! (isolate the tablet leader from every other node, heal), `split_brain`
//! (partition the whole cluster into two non-empty halves — unlike
//! `leader_partition`, not keyed to where the leader happens to be, so the
//! leader can land on either side), `forward_heavy` (RF 2 of 4 nodes, no
//! fault — most ops route through a node hosting no local replica purely
//! from the node/replica-count mismatch), and `two_tables` (two
//! independently-provisioned tables/tablets, ops interleaved across both
//! by the same client tasks — see the `Key`-space note above).
//!
//! # Depth knob
//!
//! `ANIMUS_SIMCLUSTER_SEEDS` (default 1 = the 8 cells above, byte-
//! identical to the committed set) — `corpus::seed_expand` over
//! [`corpus_cells`], the same house convention every other corpus in this
//! workspace uses. Run via `cargo test -p animusd --lib sim_cluster_corpus`.
//!
//! # Shrink wiring (ADR 0061 rung B4)
//!
//! Mirrors `raftkv_linearizable.rs`'s own wiring exactly: [`Scenario`]/
//! [`Nemesis`] derive `Serialize`/`Deserialize`, [`scenario_candidates`]
//! reduces `faults`/`window`/`rounds`/`keyspace`/`clients` (never `name`/
//! `seed`/`nodes`/`replication`/`tables`, for the identical reason
//! `raftkv_linearizable.rs` keeps `replicas` fixed: some cells are only
//! meaningful at a specific node/replication shape), [`shrink_and_report`]
//! runs under `ANIMUS_SHRINK=1` only after a scenario is already known to
//! have failed, and `sim_cluster_shrink_replay` (`#[ignore]`d) reads
//! `ANIMUS_SHRINK_REPLAY` to re-run a printed minimized case.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use animus_env::{Clock, EnvExt, Rng};
use animus_sim::SimEnv;
use animus_test::corpus::{self, SeedVariant};
use animus_test::history::{Key, Mop, Process};
use animus_test::shrink::{self, ShrinkReport};
use animus_test::{CheckReport, Recorder, check_convergence, check_cycles, check_durability};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};

use super::sim_cluster::{SimCluster, SimClusterHandle};
use super::*;

/// Settle time before the workload starts (let both the control quorum and
/// the fresh tablet group elect) — `SimCluster::new`/`create_table`
/// already internally poll to convergence before returning, so this is
/// just headroom for the client tasks' own very first round, mirroring
/// `raftkv_linearizable.rs`'s identical `SETTLE`.
const SETTLE: Duration = Duration::from_millis(300);
/// Inter-round gap so client tasks interleave rather than lock-stepping.
const POLL: Duration = Duration::from_millis(80);
/// How long the runner holds a scheduled fault's *own* outage window open
/// before healing — the group must genuinely ride it out (an election, a
/// stalled commit, a restart racing live traffic), not just have the fault
/// injected and instantly reversed.
const FAULT_WINDOW: Duration = Duration::from_millis(1200);
/// Post-heal drain: run the workload tail to completion before taking the
/// history snapshot the `cycles` verdict is checked against.
const DRAIN: Duration = Duration::from_secs(6);
/// Converged-or-timeout poll step + budget for the durability/convergence
/// checks (an eventual property may need a beat past `DRAIN` to settle a
/// lagging replica — never a fixed-deadline one-shot read).
const CONVERGENCE_POLL_STEP: Duration = Duration::from_secs(1);
const CONVERGENCE_BUDGET: Duration = Duration::from_secs(15);

/// A `Key`'s high-order digits name which table it belongs to (see the
/// module doc's "multiple tables share one `Key` space" note) — chosen
/// comfortably larger than any scenario's own `keyspace` so two tables'
/// logical key ranges never overlap.
const TABLE_KEY_STRIDE: Key = 1_000_000;

/// Split `key` back into `(table name, item pk, item sk)` — the exact
/// inverse of how [`client_loop`] builds a `Key` from a scenario's own
/// `(table index, logical key)` pair. Every table this fixture creates has
/// a `(pk, sk)` composite schema (`SimCluster::create_table`'s own
/// contract), so every item needs both.
fn table_pk_sk(key: Key) -> (String, String, String) {
    let table = key / TABLE_KEY_STRIDE;
    let logical = key % TABLE_KEY_STRIDE;
    (
        format!("t{table}"),
        format!("item-{logical}"),
        "v".to_owned(),
    )
}

/// This fixture's own list-value encoding — u64 elements, 8 big-endian
/// bytes each, identical to `raftkv_linearizable.rs`'s own (duplicated
/// rather than shared: each corpus's encoding is a private workload
/// detail, not a shared contract).
fn encode_list(list: &[u64]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(list.len() * 8);
    for v in list {
        bytes.extend_from_slice(&v.to_be_bytes());
    }
    bytes
}

fn decode_list(bytes: &[u8]) -> Vec<u64> {
    bytes
        .chunks_exact(8)
        .map(|c| u64::from_be_bytes(c.try_into().expect("8-byte chunk")))
        .collect()
}

// ---------------------------------------------------------------------------
// Declarative scenario model.
// ---------------------------------------------------------------------------

/// A fault the runner injects once, at a scheduled offset from the start of
/// the workload, resolved against the live cluster at run time (so a
/// scenario is shape-relative, not node-id-relative).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Nemesis {
    /// Crash the tablet's current leader (mute, tasks stay alive) — the
    /// survivors must elect a new leader and keep serving.
    LeaderCrash,
    /// Crash a non-leader replica of the tablet. Quorum is unaffected; the
    /// crashed node must catch up after `heal_all`'s restart.
    FollowerCrash,
    /// A true **process restart** of the tablet's current leader
    /// (`SimCluster::restart`'s wipe-and-rejoin — fresh `RaftNode`/
    /// `RaftKvNode` on the same id, recovering via peer catch-up, never a
    /// WAL replay on this fixture's `MemoryEngine` tier).
    StopRestart,
    /// Partition the tablet's current leader away from every other node in
    /// the cluster (not just the tablet's own replicas) — the majority
    /// elects a fresh leader while the deposed one cannot commit or serve
    /// a linearizable read.
    LeaderPartition,
    /// Partition the WHOLE cluster into two non-empty halves (nodes
    /// `0..half` vs `half..nodes`) — unlike `LeaderPartition`, not keyed
    /// to wherever the leader happens to be, so the leader can end up on
    /// either side of the cut.
    SplitBrain,
}

impl Nemesis {
    /// Resolve and inject this fault against `cluster`'s current state for
    /// `tablet` — mirrors `raftkv_linearizable.rs`'s own `Group::apply`.
    fn apply(self, cluster: &mut SimCluster, tablet: TabletId) {
        match self {
            Nemesis::LeaderCrash => {
                if let Some(leader) = cluster.leader_index_of(tablet) {
                    cluster.crash(leader);
                }
            }
            Nemesis::FollowerCrash => {
                if let Some(leader) = cluster.leader_index_of(tablet) {
                    let replicas = cluster.handle().replicas_of(tablet);
                    if let Some(&follower) = replicas.iter().find(|&&n| n != leader) {
                        cluster.crash(follower);
                    }
                }
            }
            Nemesis::StopRestart => {
                let victim = cluster.leader_index_of(tablet).unwrap_or(0);
                cluster.restart(victim);
            }
            Nemesis::LeaderPartition => {
                if let Some(leader) = cluster.leader_index_of(tablet) {
                    for n in 0..cluster.node_count() as u64 {
                        if n != leader {
                            cluster.partition(leader, n);
                        }
                    }
                }
            }
            Nemesis::SplitBrain => {
                let n = cluster.node_count() as u64;
                let half = n.div_ceil(2);
                for a in 0..half {
                    for b in half..n {
                        cluster.partition(a, b);
                    }
                }
            }
        }
    }
}

/// A seed-reproducible scenario: cluster shape + workload + at most one
/// scheduled fault (a `Vec`, like `raftkv_linearizable.rs`'s own `faults`
/// field, purely so [`scenario_candidates`] can drop it the same way —
/// every cell in [`corpus_cells`] schedules zero or one).
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Scenario {
    name: String,
    seed: u64,
    /// Total node count — every id in `0..nodes` is a control-plane voter
    /// (`SimCluster::new`'s own contract).
    nodes: usize,
    /// Replication factor every table in this scenario is created with.
    replication: usize,
    /// How many independently-provisioned tables this scenario creates
    /// (`t0`, `t1`, ...) — 1 for every cell except `two_tables`.
    tables: usize,
    clients: usize,
    rounds: u64,
    /// Per-table keyspace (see [`TABLE_KEY_STRIDE`] for how multiple
    /// tables' key ranges stay disjoint in the shared `Key` space).
    keyspace: u64,
    read_pct: u64,
    faults: Vec<(Duration, Nemesis)>,
    /// How long the runner holds the last scheduled fault open before
    /// healing (zero for a fault-free cell).
    window: Duration,
}

impl SeedVariant for Scenario {
    fn scenario_name(&self) -> &str {
        &self.name
    }
    fn reseeded(&self, name: String, seed: u64) -> Self {
        Scenario {
            name,
            seed,
            ..self.clone()
        }
    }
}

/// A contended, small workload shared by every cell — differs only in
/// cluster shape/table count/fault schedule.
fn base_workload(
    name: &str,
    nodes: usize,
    replication: usize,
    tables: usize,
    faults: Vec<(Duration, Nemesis)>,
    window: Duration,
) -> Scenario {
    Scenario {
        seed: corpus::name_seed(name),
        name: name.to_owned(),
        nodes,
        replication,
        tables,
        clients: 3,
        rounds: 6,
        keyspace: 3,
        read_pct: 40,
        faults,
        window,
    }
}

/// The 8 named cells this corpus ships with (see the module doc's own
/// per-cell summary). Frozen, name-seeded — a suite run is the same set
/// every time (the house corpus doctrine, `animus_test::corpus`'s own
/// doc).
fn corpus_cells() -> Vec<Scenario> {
    const FAULT_AT: Duration = Duration::from_millis(900);
    vec![
        base_workload("simcluster_baseline", 3, 3, 1, vec![], Duration::ZERO),
        base_workload(
            "simcluster_leader_crash",
            3,
            3,
            1,
            vec![(FAULT_AT, Nemesis::LeaderCrash)],
            FAULT_WINDOW,
        ),
        base_workload(
            "simcluster_follower_crash",
            3,
            3,
            1,
            vec![(FAULT_AT, Nemesis::FollowerCrash)],
            FAULT_WINDOW,
        ),
        base_workload(
            "simcluster_stop_restart",
            3,
            3,
            1,
            vec![(FAULT_AT, Nemesis::StopRestart)],
            FAULT_WINDOW,
        ),
        base_workload(
            "simcluster_leader_partition",
            3,
            3,
            1,
            vec![(FAULT_AT, Nemesis::LeaderPartition)],
            FAULT_WINDOW,
        ),
        base_workload(
            "simcluster_split_brain",
            3,
            3,
            1,
            vec![(FAULT_AT, Nemesis::SplitBrain)],
            FAULT_WINDOW,
        ),
        base_workload("simcluster_forward_heavy", 4, 2, 1, vec![], Duration::ZERO),
        base_workload("simcluster_two_tables", 3, 3, 2, vec![], Duration::ZERO),
    ]
}

/// Seeds per cell (`ANIMUS_SIMCLUSTER_SEEDS`, default 1 — byte-identical to
/// the committed 8-cell set).
fn seeds_per_cell() -> usize {
    corpus::seeds_from_env("ANIMUS_SIMCLUSTER_SEEDS")
}

/// The corpus the flagship test runs: [`corpus_cells`], seed-expanded by
/// the depth knob.
fn corpus() -> Vec<Scenario> {
    corpus::seed_expand(corpus_cells(), seeds_per_cell())
}

// ---------------------------------------------------------------------------
// The workload.
// ---------------------------------------------------------------------------

struct Shared {
    rec: Mutex<Recorder>,
    next_value: Mutex<u64>,
    /// Acked-write count per **issuing node** — the non-vacuity signal for
    /// `forward_heavy`-shaped cells: at least one node hosting no local
    /// replica of the tablet must still have gotten an ack, proving the
    /// forward actually landed rather than merely being attempted.
    ok_writes_by_node: Mutex<BTreeMap<u64, usize>>,
}

impl Shared {
    fn fresh_value(&self) -> u64 {
        let mut v = self.next_value.lock().expect("next_value poisoned");
        *v += 1;
        *v
    }

    fn record_ok_write(&self, node: u64) {
        *self
            .ok_writes_by_node
            .lock()
            .expect("ok_writes_by_node poisoned")
            .entry(node)
            .or_default() += 1;
    }
}

/// Run a single-key **list-append** write, issued from `node`'s own
/// `ClientCtx` via `handle`: append this op's globally-unique value to the
/// client's authoritative list for `key`, `put` the whole new list, and
/// record `ok`/`info` — never `fail` (indeterminate outcomes, per the
/// house rule `animus-test`'s crate guide states). `SimClusterHandle::put`
/// is already `CLIENT_TIMEOUT`-bounded internally (see this module's own
/// doc), so a single `.await` here — no extra retry loop, unlike
/// `raftkv_linearizable.rs`'s own `run_write`, whose bare `RaftKvNode`
/// `put`/`linearizable_get` calls carry no such internal bound — is
/// already the whole op.
#[allow(clippy::too_many_arguments)]
async fn run_write(
    env: &SimEnv,
    handle: &SimClusterHandle,
    shared: &Arc<Shared>,
    proc: Process,
    key: Key,
    node: u64,
    my_lists: &mut BTreeMap<Key, Vec<u64>>,
) {
    let value = shared.fresh_value();
    let list = my_lists.entry(key).or_default();
    list.push(value);
    let encoded = encode_list(list);
    let (table, pk, sk) = table_pk_sk(key);
    let mops = vec![Mop::Append { key, value }];
    shared
        .rec
        .lock()
        .expect("recorder poisoned")
        .invoke(proc, env.now().0, mops.clone());

    let result = handle.put(node, &table, &pk, &sk, &encoded).await;
    let mut rec = shared.rec.lock().expect("recorder poisoned");
    match result {
        Ok(()) => {
            rec.ok(proc, env.now().0, mops);
            drop(rec);
            shared.record_ok_write(node);
        }
        Err(_) => {
            // Indeterminate — the write may yet have committed. Never `fail`.
            rec.info(proc, env.now().0, mops);
        }
    }
}

/// Run a single-key **linearizable read**, issued from `node`'s own
/// `ClientCtx` — [`run_write`]'s read sibling. Always `ConsistentRead:
/// true` (`SimClusterHandle::get`'s `consistent` argument): the
/// checker needs a real linearizable observation, exactly the discipline
/// `crates/animusd/CLAUDE.md`'s ADR 0055 testing-gotcha entry states ("a
/// read that verifies a write must ask for `ConsistentRead: true`").
async fn run_read(
    env: &SimEnv,
    handle: &SimClusterHandle,
    shared: &Arc<Shared>,
    proc: Process,
    key: Key,
    node: u64,
) {
    let (table, pk, sk) = table_pk_sk(key);
    let invoke = vec![Mop::Read {
        key,
        observed: None,
    }];
    shared
        .rec
        .lock()
        .expect("recorder poisoned")
        .invoke(proc, env.now().0, invoke);

    let result = handle.get(node, &table, &pk, &sk, true).await;
    let mut rec = shared.rec.lock().expect("recorder poisoned");
    match result {
        Ok(v) => {
            let list = v.map(|b| decode_list(&b)).unwrap_or_default();
            rec.ok(
                proc,
                env.now().0,
                vec![Mop::Read {
                    key,
                    observed: Some(list),
                }],
            );
        }
        Err(_) => {
            rec.info(
                proc,
                env.now().0,
                vec![Mop::Read {
                    key,
                    observed: None,
                }],
            );
        }
    }
}

/// One client's loop: each round, draw a fresh **issuing node** from the
/// scenario's own seed (`env.gen_below(node_count)` — never a fixed leader
/// or node 0), then run a single-key read or write (single-writer for
/// writes, across every table this scenario provisioned — see the module
/// doc's `Key`-space note).
#[allow(clippy::too_many_arguments)]
async fn client_loop(
    env: SimEnv,
    handle: SimClusterHandle,
    shared: Arc<Shared>,
    proc: Process,
    clients: usize,
    rounds: u64,
    tables: usize,
    keyspace: u64,
    read_pct: u64,
    node_count: usize,
) {
    let mut my_lists: BTreeMap<Key, Vec<u64>> = BTreeMap::new();
    let owned: Vec<Key> = (0..tables as u64)
        .flat_map(|t| (0..keyspace).map(move |k| t * TABLE_KEY_STRIDE + k))
        .filter(|&k| k % clients as u64 == proc)
        .collect();
    for _round in 0..rounds {
        let node = env.gen_below(node_count as u64);
        let is_read = env.gen_below(100) < read_pct;
        if is_read {
            let t = env.gen_below(tables as u64);
            let k = env.gen_below(keyspace);
            let key = t * TABLE_KEY_STRIDE + k;
            run_read(&env, &handle, &shared, proc, key, node).await;
        } else if !owned.is_empty() {
            let key = owned[env.gen_below(owned.len() as u64) as usize];
            run_write(&env, &handle, &shared, proc, key, node, &mut my_lists).await;
        }
        env.sleep(POLL).await;
    }
}

/// Put → consistent-get(present) → delete → consistent-get(absent), issued
/// **from every node in the cluster in turn** — the direct correctness
/// check for `delete` the module doc's own "kept OUT of the Elle model"
/// section explains. `Ok(n)` on success (`n == node_count`, every node
/// proven); `Err` names which node/step failed. Run only after the
/// scenario's own fault schedule has healed and drained, so this check is
/// never itself subject to an in-flight fault.
///
/// **Deliberately drives `cluster`'s own synchronous `put`/`get`/`delete`
/// (`&mut SimCluster`, each already spawn-and-drive its own op via
/// `spawn_and_capture`), never `SimClusterHandle` + a bare `block_on`.**
/// Unlike [`final_state`]'s `local_value` (a pure local engine read that
/// resolves on its very first poll, so `block_on` alone is sound — the
/// same reasoning `raftkv_linearizable.rs`'s own `final_state` relies on),
/// `SimClusterHandle::put`/`get`/`delete` internally `.await` real
/// `env.sleep()`-paced route/confirm-poll loops — `block_on`ing one of
/// those directly from this function, with no simulator concurrently
/// driving virtual time forward, hangs forever (found live building this
/// probe: the very first `cargo test` run never returned). `SimCluster`'s
/// own driver methods are the correct way to issue one op synchronously
/// from a plain (non-spawned) caller.
fn run_delete_probe(
    cluster: &mut SimCluster,
    table: &str,
    node_count: usize,
) -> Result<usize, String> {
    for node in 0..node_count as u64 {
        let pk = format!("delete-probe-{node}");
        let sk = "v";
        cluster
            .put(node, table, &pk, sk, b"probe")
            .map_err(|e| format!("node {node}: put failed: {e}"))?;
        let seen = cluster
            .get(node, table, &pk, sk, true)
            .map_err(|e| format!("node {node}: get after put failed: {e}"))?;
        if seen.as_deref() != Some(b"probe".as_slice()) {
            return Err(format!(
                "node {node}: put not visible via a consistent get (seen={seen:?})"
            ));
        }
        cluster
            .delete(node, table, &pk, sk)
            .map_err(|e| format!("node {node}: delete failed: {e}"))?;
        let after = cluster
            .get(node, table, &pk, sk, true)
            .map_err(|e| format!("node {node}: get after delete failed: {e}"))?;
        if after.is_some() {
            return Err(format!(
                "node {node}: item still present after delete ({after:?})"
            ));
        }
    }
    Ok(node_count)
}

// ---------------------------------------------------------------------------
// The scenario runner.
// ---------------------------------------------------------------------------

struct ScenarioResult {
    cycles: CheckReport,
    durability: CheckReport,
    convergence: CheckReport,
    ok_writes: usize,
    nonempty_reads: usize,
    /// Acked writes issued from a node hosting NO local replica of the
    /// primary tablet — the `forward_heavy` non-vacuity signal.
    non_hosting_ok_writes: usize,
    /// `Ok(node_count)` (every node proved) from [`run_delete_probe`].
    delete_probe: Result<usize, String>,
}

fn combine_reports(seed: u64, reports: impl Iterator<Item = CheckReport>) -> CheckReport {
    let mut violations = Vec::new();
    for r in reports {
        violations.extend(r.violations);
    }
    CheckReport {
        ok: violations.is_empty(),
        violations,
        seed,
    }
}

/// Read every known key's raw value straight off `node`'s own local
/// engine (never routed — `SimClusterHandle::local_value`), across every
/// table this scenario provisioned. Mirrors `raftkv_linearizable.rs`'s own
/// `final_state`: two *distinct* replicas' own raw state is what makes
/// `check_convergence` a real cross-replica agreement check and
/// `check_durability` meaningful under single-writer-per-key.
fn final_state(
    handle: &SimClusterHandle,
    tablets: &BTreeMap<u64, TabletId>,
    node: u64,
    tables: usize,
    keyspace: u64,
) -> BTreeMap<Key, Vec<u64>> {
    let mut map = BTreeMap::new();
    for t in 0..tables as u64 {
        let Some(&tablet) = tablets.get(&t) else {
            continue;
        };
        for k in 0..keyspace {
            let key = t * TABLE_KEY_STRIDE + k;
            let (_, pk, sk) = table_pk_sk(key);
            let list = block_on(handle.local_value(node, tablet, &pk, &sk))
                .map(|b| decode_list(&b))
                .unwrap_or_default();
            map.insert(key, list);
        }
    }
    map
}

fn run_scenario(s: &Scenario) -> ScenarioResult {
    let mut cluster = SimCluster::new(s.seed, s.nodes, s.replication);
    let table_names: Vec<String> = (0..s.tables).map(|t| format!("t{t}")).collect();
    let mut tablets: BTreeMap<u64, TabletId> = BTreeMap::new();
    for (i, name) in table_names.iter().enumerate() {
        let tablet = cluster.create_table_with_replication(name, s.replication);
        tablets.insert(i as u64, tablet);
    }
    let primary_tablet = tablets
        .get(&0)
        .cloned()
        .expect("table 0 must exist — every scenario creates at least one table");

    let shared = Arc::new(Shared {
        rec: Mutex::new(Recorder::new(s.seed)),
        next_value: Mutex::new(0),
        ok_writes_by_node: Mutex::new(BTreeMap::new()),
    });

    let handle = cluster.handle();
    for c in 0..s.clients {
        let env = cluster.client_env(c as u64);
        let handle = handle.clone();
        let shared = Arc::clone(&shared);
        let (tables, rounds, keyspace, read_pct, node_count, clients) = (
            s.tables, s.rounds, s.keyspace, s.read_pct, s.nodes, s.clients,
        );
        let proc = c as Process;
        env.clone().spawn_task(async move {
            client_loop(
                env, handle, shared, proc, clients, rounds, tables, keyspace, read_pct, node_count,
            )
            .await;
        });
    }

    cluster.run_for(SETTLE);

    // Walk the (at most one) scheduled fault, then hold its outage window
    // open before healing — mirrors `raftkv_linearizable.rs`'s own
    // `run_scenario_on`, minus the multi-fault sort (every cell here
    // schedules 0 or 1).
    let mut elapsed = Duration::ZERO;
    for (at, nem) in s.faults.clone() {
        if at > elapsed {
            cluster.run_for(at - elapsed);
            elapsed = at;
        }
        nem.apply(&mut cluster, primary_tablet);
    }
    if !s.window.is_zero() {
        cluster.run_for(s.window);
    }
    cluster.heal_all();
    cluster.run_for(DRAIN);

    let history = shared
        .rec
        .lock()
        .expect("recorder poisoned")
        .history()
        .clone();
    let cycles = check_cycles(&history);

    let replicas = handle.replicas_of(primary_tablet);
    let all_states = |c: &SimCluster| -> Vec<BTreeMap<Key, Vec<u64>>> {
        let h = c.handle();
        replicas
            .iter()
            .map(|&n| final_state(&h, &tablets, n, s.tables, s.keyspace))
            .collect()
    };
    let mut states = all_states(&cluster);
    let mut durability = combine_reports(
        s.seed,
        states.iter().map(|st| check_durability(&history, st)),
    );
    let mut convergence = combine_reports(
        s.seed,
        states[1..]
            .iter()
            .map(|st| check_convergence(s.seed, &states[0], st)),
    );
    let poll_deadline_steps = CONVERGENCE_BUDGET.as_millis() / CONVERGENCE_POLL_STEP.as_millis();
    let mut polled: u128 = 0;
    while !(durability.ok && convergence.ok) && polled < poll_deadline_steps {
        cluster.run_for(CONVERGENCE_POLL_STEP);
        states = all_states(&cluster);
        durability = combine_reports(
            s.seed,
            states.iter().map(|st| check_durability(&history, st)),
        );
        convergence = combine_reports(
            s.seed,
            states[1..]
                .iter()
                .map(|st| check_convergence(s.seed, &states[0], st)),
        );
        polled += 1;
    }

    let ok_writes = history
        .ok_entries()
        .flat_map(|e| &e.mops)
        .filter(|m| matches!(m, Mop::Append { .. }))
        .count();
    let nonempty_reads = history
        .ok_entries()
        .filter(|e| {
            e.mops
                .iter()
                .any(|m| matches!(m, Mop::Read { observed: Some(l), .. } if !l.is_empty()))
        })
        .count();
    let ok_writes_by_node = shared
        .ok_writes_by_node
        .lock()
        .expect("ok_writes_by_node poisoned")
        .clone();
    let non_hosting_ok_writes: usize = (0..s.nodes as u64)
        .filter(|n| !replicas.contains(n))
        .map(|n| ok_writes_by_node.get(&n).copied().unwrap_or(0))
        .sum();

    let delete_probe = run_delete_probe(&mut cluster, &table_names[0], s.nodes);

    ScenarioResult {
        cycles,
        durability,
        convergence,
        ok_writes,
        nonempty_reads,
        non_hosting_ok_writes,
        delete_probe,
    }
}

/// Run one scenario, naming it via `eprintln!` up front and re-raising a
/// setup panic with `scenario=<name> seed=<seed>` prepended — mirrors
/// `raftkv_linearizable.rs`'s own `run_scenario_identified` (issue #554's
/// fix: a panic during setup must still name a replayable seed).
fn run_scenario_identified(s: &Scenario) -> ScenarioResult {
    eprintln!("scenario={} seed={}", s.name, s.seed);
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_scenario(s))) {
        Ok(r) => r,
        Err(payload) => {
            let msg = payload
                .downcast_ref::<&str>()
                .map(|m| m.to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "<non-string panic payload>".to_string());
            panic!("scenario={} seed={}: {msg}", s.name, s.seed);
        }
    }
}

fn scenario_failed(r: &ScenarioResult) -> bool {
    !r.cycles.ok || !r.durability.ok || !r.convergence.ok || r.delete_probe.is_err()
}

fn assert_scenario_ok(s: &Scenario, r: &ScenarioResult) {
    assert!(
        r.cycles.ok,
        "scenario {} not serializable: {:?} (seed={})",
        s.name, r.cycles.violations, s.seed
    );
    assert!(
        r.durability.ok,
        "scenario {} lost an acked append: {:?} (seed={})",
        s.name, r.durability.violations, s.seed
    );
    assert!(
        r.convergence.ok,
        "scenario {} did not converge: {:?} (seed={})",
        s.name, r.convergence.violations, s.seed
    );
    assert!(
        r.delete_probe.is_ok(),
        "scenario {} delete probe failed: {:?} (seed={})",
        s.name,
        r.delete_probe,
        s.seed
    );
}

// ---------------------------------------------------------------------------
// Failure minimization (ADR 0061 rung B4) — mirrors `raftkv_linearizable.
// rs`'s own wiring exactly; see that file's doc and `animus-test`'s crate
// guide for the algorithm.
// ---------------------------------------------------------------------------

/// Delta-debugging candidates for a [`Scenario`]: drop the scheduled fault
/// (if any), zero a real outage window, halve `rounds`/`keyspace` (floor
/// 1), decrement `clients` (floor 1). Deliberately not touched: `name`
/// (identity), `seed` (fixed throughout), `nodes`/`replication`/`tables`
/// (some cells are only meaningful at a specific shape — `forward_heavy`'s
/// whole point is `nodes > replication`, `two_tables`' is `tables == 2`).
fn scenario_candidates(s: &Scenario) -> Vec<Scenario> {
    let mut out = Vec::new();
    if !s.faults.is_empty() {
        out.push(Scenario {
            faults: Vec::new(),
            ..s.clone()
        });
    }
    if !s.window.is_zero() {
        out.push(Scenario {
            window: Duration::ZERO,
            ..s.clone()
        });
    }
    if s.rounds > 1 {
        out.push(Scenario {
            rounds: (s.rounds / 2).max(1),
            ..s.clone()
        });
    }
    if s.keyspace > 1 {
        out.push(Scenario {
            keyspace: (s.keyspace / 2).max(1),
            ..s.clone()
        });
    }
    if s.clients > 1 {
        out.push(Scenario {
            clients: s.clients - 1,
            ..s.clone()
        });
    }
    out
}

/// Shrink one observed failure and print a report + a copy-pasteable
/// replay handle — called only after a scenario is already known to have
/// failed, and only under `ANIMUS_SHRINK=1` (never on a green run's hot
/// path).
fn shrink_and_report(s: &Scenario) -> ShrinkReport<Scenario> {
    let report = shrink::minimize(
        s.clone(),
        scenario_candidates,
        |cand| scenario_failed(&run_scenario(cand)),
        shrink::budget_from_env(),
    );
    eprintln!("{}", shrink::describe(&s.name, &report));
    match shrink::replay_json(&report) {
        Ok(json) => {
            eprintln!("  replay handle (JSON): {json}");
            eprintln!(
                "  Replay directly: ANIMUS_SHRINK_REPLAY='{json}' \\\n    \
                 cargo test -p animusd --lib sim_cluster_shrink_replay \\\n    \
                 -- --ignored --nocapture"
            );
        }
        Err(e) => eprintln!("  (failed to serialize replay handle: {e})"),
    }
    report
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

#[test]
fn sim_cluster_baseline_is_consistent() {
    let scenario = base_workload("simcluster_baseline", 3, 3, 1, vec![], Duration::ZERO);
    let r = run_scenario(&scenario);
    assert_scenario_ok(&scenario, &r);
    assert!(r.ok_writes > 0, "no acked writes — vacuous run");
    assert!(
        r.nonempty_reads > 0,
        "no non-empty reads — checker had nothing to chew"
    );
}

#[test]
fn sim_cluster_corpus_is_consistent() {
    let scenarios = corpus();
    let mut total_ok_writes = 0usize;
    for s in &scenarios {
        let r = run_scenario_identified(s);
        if scenario_failed(&r) && shrink::shrink_enabled() {
            // Never on the hot path of a green run — only after we
            // already know this scenario failed, and only opted in via
            // ANIMUS_SHRINK=1.
            shrink_and_report(s);
        }
        assert_scenario_ok(s, &r);
        assert!(
            r.ok_writes > 0,
            "scenario {} did no acked writes — vacuous run (seed={})",
            s.name,
            s.seed
        );
        // Forward-heavy teeth: a cell with more nodes than replicas must
        // actually route at least one ack'd write through a node hosting
        // no local replica of the tablet, not merely attempt to.
        if s.nodes > s.replication {
            assert!(
                r.non_hosting_ok_writes > 0,
                "scenario {}: no write issued from a non-hosting node ever succeeded \
                 (seed={}) — forwarding may be broken, or the workload never actually \
                 exercised it",
                s.name,
                s.seed
            );
        }
        total_ok_writes += r.ok_writes;
    }
    assert!(
        total_ok_writes > scenarios.len(),
        "corpus too vacuous: only {total_ok_writes} acked writes across {} scenarios",
        scenarios.len()
    );
}

/// Coverage guard: every named fault class plus the fault-free forwarding
/// and multi-table cells must still be present at the frozen depth — a
/// dimension silently dropped from [`corpus_cells`] would otherwise go
/// unnoticed. Structural only; no scenario is run.
#[test]
fn sim_cluster_corpus_covers_every_cell_shape() {
    let cells = corpus_cells();
    assert_eq!(cells.len(), 8, "expected exactly 8 named cells");
    assert!(
        cells
            .iter()
            .any(|s| s.faults.is_empty() && s.tables == 1 && s.nodes == s.replication)
    );
    for nem in [
        Nemesis::LeaderCrash,
        Nemesis::FollowerCrash,
        Nemesis::StopRestart,
        Nemesis::LeaderPartition,
        Nemesis::SplitBrain,
    ] {
        assert!(
            cells
                .iter()
                .any(|s| s.faults.iter().any(|(_, f)| *f == nem)),
            "no cell schedules {nem:?}"
        );
    }
    assert!(
        cells.iter().any(|s| s.nodes > s.replication),
        "no forward-heavy (nodes > replication) cell"
    );
    assert!(cells.iter().any(|s| s.tables > 1), "no multi-table cell");
}

/// The replay entry point named in every `ANIMUS_SHRINK` report
/// (`shrink_and_report`'s printed instructions): paste the JSON a shrink
/// run printed into `ANIMUS_SHRINK_REPLAY` and run this single test to
/// re-confirm the minimized case still reproduces. Inert (does nothing,
/// asserts nothing) when the env var is unset.
#[test]
#[ignore = "opt-in replay entry point — set ANIMUS_SHRINK_REPLAY to a shrink report's printed JSON"]
fn sim_cluster_shrink_replay() {
    let Ok(json) = std::env::var("ANIMUS_SHRINK_REPLAY") else {
        eprintln!(
            "sim_cluster_shrink_replay: skipped — set ANIMUS_SHRINK_REPLAY to a \
             shrink report's printed JSON to replay it"
        );
        return;
    };
    let scenario: Scenario =
        serde_json::from_str(&json).expect("ANIMUS_SHRINK_REPLAY must be a Scenario JSON blob");
    let r = run_scenario(&scenario);
    eprintln!(
        "replayed '{}' (seed={}): cycles.ok={} durability.ok={} convergence.ok={} \
         delete_probe={:?} ok_writes={}",
        scenario.name,
        scenario.seed,
        r.cycles.ok,
        r.durability.ok,
        r.convergence.ok,
        r.delete_probe,
        r.ok_writes
    );
    assert!(
        scenario_failed(&r),
        "replayed scenario '{}' (seed={}) did NOT reproduce the failure",
        scenario.name,
        scenario.seed
    );
}
