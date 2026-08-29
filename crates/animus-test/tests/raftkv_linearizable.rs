//! ADR 0017 / ADR 0016 step 4 / ADR 0014: an **Elle linearizability corpus for the
//! per-tablet Raft KV data plane** (`animus-cp-data`).
//!
//! The leaderful data plane offers **single-tablet linearizable KV**
//! (`put`/`delete`/`linearizable_get`) — *not* multi-key transactions — so a
//! multi-key transactional workload does not apply to it. This is therefore a
//! self-contained harness that reuses the proven **checkers** (`check_cycles`/
//! `check_durability`/`check_convergence`) and the `Recorder`/`History` model,
//! driving a **single-key list-append** workload over one Raft group.
//!
//! **Why serializability (`check_cycles`) is sound here — and scaled to depth.** A
//! single Raft group orders every operation through one committed log: a write is
//! a list-append to one key, a read is a `linearizable_get` (ReadIndex — a quorum
//! read-barrier, no wall clock). Linearizability is a **safety** property, so a
//! forked/stale read — exactly the failure a deposed leader serving a stale value
//! would cause — manifests as a contradictory recovered order, i.e. a cycle the
//! checker flags. Unlike the AP `Frontier` topology there is no eventually-
//! consistent read path to produce torn-read false positives: the plane *is* the
//! serialization authority. So all three checks (cycles + durability +
//! convergence) are asserted on this one layer. Convergence + durability remain
//! **eventual** (a lagging follower catches up via anti-entropy / snapshot), so
//! they get a **converged-or-timeout** poll, the pattern every corpus in this
//! crate uses (see `animus-test`'s crate guide).
//!
//! Single-writer-per-key (`owner(key) = key % clients`) is load-bearing here:
//! per-key LWW (the Raft log index is the MVCC
//! version) would otherwise *lose* a concurrent writer's append, a data-model
//! artefact rather than a consistency bug. A client builds each append on its own
//! authoritative in-memory list (it is the sole writer and runs serially) and
//! writes the whole new list back, so an indeterminate (`info`) write that later
//! re-commits restores the prefix.
//!
//! **The deepened fault matrix (frozen alongside the original cells).** Beyond the
//! original single-fault vocabulary (leader/follower kill, leader partition,
//! lossy), the corpus covers a deepened set of fault classes:
//!
//! - `StopRestart` — a true **process restart**: `sim.stop` drops the victim's
//!   tasks + volatile state (its in-memory `RaftCore` is *gone*), then a fresh
//!   `RaftKvNode::start` on the same node id recovers from the **durable WAL** and
//!   must rejoin the group. This is the CP recovery path (`RaftCore::recovered`),
//!   untestable by `crash`/`restart` (which re-arm the *same* tasks with their
//!   in-memory state intact).
//! - `SplitBrain` — a full-mesh partition (every replica an island, **no** side
//!   has a majority): commits stall until heal; the group must re-form and stay
//!   linearizable.
//! - `LeaderMinority` — the leader isolated **with a minority** (5 replicas:
//!   leader + 1 vs 3): the majority elects a fresh leader while the deposed one
//!   still believes it leads — the classic stale-read window a linearizable read
//!   must not serve from.
//! - Compound `Lossy` + `StopRestart` — a WAL recovery racing a degraded
//!   network; historically the class that surfaced real findings at depth.
//!
//! The new cells also carry a non-zero **fault window** (`Scenario::window`): the
//! runner holds the last fault open for that long before healing, so the group
//! genuinely rides out the outage (elections, stalled commits, recovery racing
//! live traffic). The original frozen cells keep `window == 0` — their runs stay
//! **byte-identical** to the committed corpus (frozen regression seeds never move).
//!
//! **Engine tiers.** The corpus runs over `MemoryEngine` (fast, always-on) and —
//! for the durable path no corpus exercised before — over **`LsmEngine<SimEnv>`**
//! (real WAL/SSTable recovery through the deterministic disk seam). A
//! representative LSM subset runs by default; `ANIMUS_RAFTKV_LSM=1` runs the whole
//! corpus over the LSM engine (the deep/nightly tier).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{Clock, EnvExt, Rng, nid};
use animus_sim::{NetConfig, SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, MemoryEngine, StorageEngine};
use animus_test::corpus::{self, SeedVariant};
use animus_test::history::{Key, Mop, Process};
use animus_test::shrink::{self, ShrinkReport};
use animus_test::{History, Recorder, check_convergence, check_cycles, check_durability};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};

/// A group node over a chosen engine tier.
type Node<S> = RaftKvNode<SimEnv, S>;
/// The live replica set. `StopRestart` replaces a slot with a fresh node (same
/// node id, recovered from its durable WAL), so the vec needs interior
/// mutability; clients clone the `Arc` out and never hold the lock across an
/// `.await`.
type Nodes<S> = Arc<Mutex<Vec<Arc<Node<S>>>>>;
/// How a tier opens (or re-opens, after a stop) a node's storage engine.
type EngineFactory<S> = fn(&Simulator, u64) -> S;

/// Raft-group replica node ids (a single tablet's group). A scenario uses a
/// prefix (3 or 5 replicas).
const GROUP_IDS: [u64; 5] = [0, 1, 2, 3, 4];
/// Per-client driver env ids — disjoint from the group, **never faulted**, so a
/// client task always makes progress (it routes its ops to whichever group node
/// currently leads, tolerating crashes/partitions of the group). The inbox of
/// these ids is unused (clients never `recv`).
const CLIENT_IDS: [u64; 5] = [100, 101, 102, 103, 104];

/// How long a single client op polls for commit before recording it indeterminate
/// (`info`). Generous: the ReadIndex read timeout is 5s, so a deposed-leader read
/// must be allowed to expire without being misclassified.
const OP_BUDGET: Duration = Duration::from_secs(9);
/// Poll granularity while a client waits for its op to commit / its read to land.
const POLL: Duration = Duration::from_millis(100);
/// Settle time before the workload starts (let the group elect a leader).
const SETTLE: Duration = Duration::from_millis(800);
/// Post-heal drain: run the workload tail to completion before the convergence
/// poll. After this the history is stable, so the `cycles` verdict is snapshotted
/// here.
const DRAIN: Duration = Duration::from_secs(40);
/// Converged-or-timeout poll step + budget: eventual
/// properties get a generous bounded poll rather than a fixed-drain snapshot.
const CONVERGENCE_POLL_STEP: Duration = Duration::from_secs(2);
const CONVERGENCE_BUDGET: Duration = Duration::from_secs(120);

// ---------------------------------------------------------------------------
// Declarative scenario model (focused on this plane's fault vocabulary, with the
// fault vocabulary the *leaderful* plane actually exercises).
// ---------------------------------------------------------------------------

/// A fault the nemesis injects at a scheduled virtual time, resolved against the
/// live group at run time (so a scenario is shape-relative).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
enum Nemesis {
    /// Crash the current leader (drops un-synced disk + inbox, mutes sends). The
    /// survivors must elect a new leader and keep serving; every acked write must
    /// survive. Restarted by `HealAll`.
    LeaderKill,
    /// Crash a follower (a non-leader replica). Quorum is unaffected; the crashed
    /// node must catch up (log or snapshot) after restart.
    FollowerKill,
    /// Partition the current leader away from every other replica (split brain):
    /// the majority elects a fresh leader, the isolated old leader cannot commit
    /// or serve a linearizable read. Healed by `HealAll`.
    PartitionLeader,
    /// Inject lossy links (independent per-message drop) for the rest of the run.
    Lossy,
    /// Stop the current leader's **process** (`sim.stop`: tasks + in-memory
    /// `RaftCore` + volatile state die; durable disk survives) and immediately
    /// start a fresh node on the same id. The fresh node recovers from its WAL
    /// (`RaftCore::recovered` — and for the LSM tier, the engine's own
    /// WAL/SSTable recovery) and must rejoin while the workload keeps running.
    /// Falls back to the first live replica if no node currently leads.
    StopRestart,
    /// Partition every replica from every other (a full mesh of islands): **no**
    /// side has a majority, so commits stall entirely until `HealAll` — a true
    /// split brain. The group must re-form afterward with no acked write lost.
    SplitBrain,
    /// Partition the current leader **plus one follower** away from the rest.
    /// Meaningful on a 5-replica group (2 vs 3): the majority side elects a fresh
    /// leader while the deposed one still holds a minority — the stale-read
    /// hazard a linearizable read must not fall into. (On a 3-replica group the
    /// leader side would *be* the majority, so the corpus only schedules this on
    /// 5 replicas.)
    LeaderMinority,
}

/// A seed-reproducible scenario: a named group size + workload + an explicit fault
/// schedule (virtual time → nemesis). `HealAll` is always applied by the runner at
/// the end, so it is not scheduled here.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Scenario {
    name: String,
    seed: u64,
    replicas: usize,
    clients: usize,
    rounds: u64,
    keyspace: u64,
    read_pct: u64,
    faults: Vec<(Duration, Nemesis)>,
    /// How long the runner keeps the *last* fault open before `HealAll` — the
    /// outage window the group must ride out. The original frozen cells use
    /// `ZERO` (heal immediately after the fault lands, byte-identical to the
    /// committed corpus); the deepened cells hold a real window so elections /
    /// stalled commits / WAL recovery race live traffic.
    window: Duration,
}

// ---------------------------------------------------------------------------
// The frozen corpus: a committed, deterministic generator. Each scenario's seed
// is a stable hash of its name (FNV-1a — no `std::hash` nondeterminism, see
// `animus_test::corpus`), so the suite runs the SAME set every run and a
// failure names one scenario + seed.
// ---------------------------------------------------------------------------

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

/// Single-fault nemeses sampled across the original (window-less) corpus cells.
const CORPUS_FAULTS: [(&str, Nemesis); 4] = [
    ("leader_kill", Nemesis::LeaderKill),
    ("follower_kill", Nemesis::FollowerKill),
    ("partition_leader", Nemesis::PartitionLeader),
    ("lossy", Nemesis::Lossy),
];

/// Single-fault nemeses of the **deepened** tier (appended after the original
/// cells; every cell carries a real outage window). `LeaderMinority` is not here
/// because it is only meaningful on a 5-replica group — it gets a single explicit
/// cell instead.
const CORPUS_FAULTS_DEEP: [(&str, Nemesis); 2] = [
    ("stop_restart", Nemesis::StopRestart),
    ("split_brain", Nemesis::SplitBrain),
];

/// Fault timing relative to the workload's life: early / mid / late.
const CORPUS_TIMINGS: [(&str, Duration); 3] = [
    ("early", Duration::from_millis(700)),
    ("mid", Duration::from_millis(2200)),
    ("late", Duration::from_millis(3800)),
];

/// The outage window of the deepened cells: long enough that survivors must
/// genuinely operate through the fault (elections, stalled commits, a restarted
/// node's recovery racing live traffic), short enough that in-window ops stay
/// inside their `OP_BUDGET` and resolve after heal.
const DEEP_WINDOW: Duration = Duration::from_millis(2500);

/// A small high-contention workload: enough clients to make a key hot, a tiny key
/// space, a mix of reads and writes. Single-key ops (the plane is non-transactional).
fn base_workload(name: &str, replicas: usize, faults: Vec<(Duration, Nemesis)>) -> Scenario {
    Scenario {
        seed: corpus::name_seed(name),
        name: name.to_string(),
        replicas,
        clients: 3,
        rounds: 6,
        keyspace: 3,
        read_pct: 45,
        faults,
        window: Duration::ZERO,
    }
}

/// A deepened-tier cell: the same contended workload with more rounds (so live
/// traffic spans the outage window) and a real fault window before heal.
fn windowed_workload(name: &str, replicas: usize, faults: Vec<(Duration, Nemesis)>) -> Scenario {
    Scenario {
        rounds: 10,
        window: DEEP_WINDOW,
        ..base_workload(name, replicas, faults)
    }
}

/// The structural cells of the corpus: baselines (no fault, both shapes) + every
/// (fault × timing) over a 3-replica group + a 5-replica spot-check per fault.
///
/// The **deepened tier** (ADR 0014's missing fault classes ported to the CP
/// plane) is appended *after* the original cells: `stop_restart` / `split_brain`
/// over the same timing grid, the 5-replica `leader_minority`, and the compound
/// `lossy`+`stop_restart` schedules. Names are new, seeds are name-derived, and
/// the original cells are emitted first and untouched — so every pre-existing
/// name/seed (and its run, `window == 0`) stays byte-identical.
fn corpus_cells() -> Vec<Scenario> {
    let mut out = Vec::new();
    out.push(base_workload("baseline_3", 3, vec![]));
    out.push(base_workload("baseline_5", 5, vec![]));
    for (fname, fault) in CORPUS_FAULTS {
        for (tname, at) in CORPUS_TIMINGS {
            let name = format!("{fname}_{tname}_3");
            out.push(base_workload(&name, 3, vec![(at, fault)]));
        }
        // One 5-replica spot-check (mid timing) per fault — larger quorum surface.
        let name = format!("{fname}_mid_5");
        out.push(base_workload(
            &name,
            5,
            vec![(Duration::from_millis(2200), fault)],
        ));
    }

    // --- The deepened tier (appended; original names/seeds frozen above). ---
    for (fname, fault) in CORPUS_FAULTS_DEEP {
        for (tname, at) in CORPUS_TIMINGS {
            let name = format!("{fname}_{tname}_3");
            out.push(windowed_workload(&name, 3, vec![(at, fault)]));
        }
        let name = format!("{fname}_mid_5");
        out.push(windowed_workload(
            &name,
            5,
            vec![(Duration::from_millis(2200), fault)],
        ));
    }
    // Leader isolated with a minority — 5 replicas only (on 3 the leader side
    // would be the majority and nothing interesting happens).
    out.push(windowed_workload(
        "leader_minority_mid_5",
        5,
        vec![(Duration::from_millis(2200), Nemesis::LeaderMinority)],
    ));
    // Compound faults: a WAL-recovering restart under a degraded network — the
    // class that has historically surfaced real findings at depth.
    out.push(windowed_workload(
        "lossy_stop_restart_3",
        3,
        vec![
            (Duration::from_millis(700), Nemesis::Lossy),
            (Duration::from_millis(2200), Nemesis::StopRestart),
        ],
    ));
    out.push(windowed_workload(
        "lossy_stop_restart_5",
        5,
        vec![
            (Duration::from_millis(700), Nemesis::Lossy),
            (Duration::from_millis(2200), Nemesis::StopRestart),
        ],
    ));
    out
}

/// Seeds per structural cell (`ANIMUS_RAFTKV_SEEDS`, default 1) — the *depth* knob
/// One structural cell × many
/// interleavings is the dominant bug-finding lever; `K=1` is byte-identical to the
/// committed frozen set.
fn seeds_per_cell() -> usize {
    corpus::seeds_from_env("ANIMUS_RAFTKV_SEEDS")
}

/// Whether the full-corpus LSM tier is enabled (`ANIMUS_RAFTKV_LSM` set to a
/// non-empty, non-`0`/`false` value). Default off: the always-on suite runs a
/// representative LSM subset plus the whole corpus on `MemoryEngine`.
fn lsm_full_enabled() -> bool {
    match std::env::var("ANIMUS_RAFTKV_LSM") {
        Ok(v) => {
            let v = v.trim();
            !v.is_empty() && v != "0" && !v.eq_ignore_ascii_case("false")
        }
        Err(_) => false,
    }
}

/// The corpus the headline test runs: the frozen cells, seed-expanded by the
/// depth knob.
fn corpus() -> Vec<Scenario> {
    corpus::seed_expand(corpus_cells(), seeds_per_cell())
}

// ---------------------------------------------------------------------------
// Engine tiers: the corpus runs over `MemoryEngine` (fast, always-on) and over
// `LsmEngine<SimEnv>` (the durable path: real WAL/SSTable recovery through the
// deterministic disk seam — what production actually runs on).
// ---------------------------------------------------------------------------

fn mem_engine(_sim: &Simulator, _id: u64) -> MemoryEngine {
    MemoryEngine::new()
}

/// Small thresholds so a corpus-sized workload genuinely exercises flush, WAL
/// segment rotation, and compaction — not just the memtable.
fn lsm_opts() -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 256,
        compaction_trigger: 3,
        target_table_bytes: 1024,
        level_fanout: 2,
        wal_segment_bytes: 256,
        // Large grace: this corpus asserts consistency/durability, not GC.
        tombstone_grace_versions: 1 << 20,
        // Keep both opt-in perf features off: this corpus asserts correctness
        // under faults, not the fast paths they trade for (an LWW-check skip
        // and off-loop maintenance) — exercise the default, always-safe path.
        trust_monotonic_versions: false,
        background_maintenance: false,
    }
}

/// Open (or, after a `StopRestart`, **re-open**) the durable engine on `id`'s
/// env-scoped disk. Re-opening the same prefix is the recovery path: the engine
/// replays its own WAL + manifest, then the Raft driver replays `raftkv.wal` on
/// top (idempotent re-apply).
fn lsm_engine(sim: &Simulator, id: u64) -> LsmEngine<SimEnv> {
    block_on(LsmEngine::open_with(sim.env(nid(id)), "lsm/", lsm_opts())).expect("open lsm engine")
}

// ---------------------------------------------------------------------------
// List value encoding (u64 elements, 8 big-endian
// bytes each). An empty list encodes to empty bytes.
// ---------------------------------------------------------------------------

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
        .map(|c| u64::from_be_bytes(c.try_into().unwrap()))
        .collect()
}

fn key_bytes(key: Key) -> Vec<u8> {
    key.to_be_bytes().to_vec()
}

fn lossy(p: f64) -> NetConfig {
    let mut cfg = NetConfig::default();
    cfg.set_drop_prob(p);
    cfg
}

/// The owner client of `key` under single-writer-per-key: `key % clients`.
fn owner(key: Key, clients: usize) -> Process {
    key % clients as u64
}

// ---------------------------------------------------------------------------
// The running group.
// ---------------------------------------------------------------------------

struct Shared {
    rec: Mutex<Recorder>,
    /// Monotonic source of globally-unique appended values (Elle uniqueness).
    next_value: Mutex<u64>,
    /// Bumped whenever a node object is **replaced** (`StopRestart`): a client
    /// that proposed to the old object must re-propose to the fresh one (its
    /// proposal died with the stopped process). Stays 0 in the original frozen
    /// cells, so their runs are unchanged.
    epoch: Mutex<u64>,
}

impl Shared {
    fn fresh_value(&self) -> u64 {
        let mut v = self.next_value.lock().unwrap();
        *v += 1;
        *v
    }

    fn epoch(&self) -> u64 {
        *self.epoch.lock().unwrap()
    }

    fn bump_epoch(&self) {
        *self.epoch.lock().unwrap() += 1;
    }
}

/// Index + handle of a group node that currently believes it leads (lowest index
/// if more than one — possible transiently under a partition). `None` if none
/// does. Clones the `Arc` out so no lock is held across an `.await`.
fn leader_slot<S: StorageEngine + 'static>(nodes: &Nodes<S>) -> Option<(usize, Arc<Node<S>>)> {
    let guard = nodes.lock().unwrap();
    guard
        .iter()
        .position(|n| n.is_leader())
        .map(|i| (i, Arc::clone(&guard[i])))
}

struct Group<S: StorageEngine + 'static> {
    sim: Simulator,
    nodes: Nodes<S>,
    replicas: usize,
    shared: Arc<Shared>,
    /// Group ids crashed and not yet restarted.
    crashed: BTreeSet<u64>,
    /// How this tier opens / re-opens a node's engine (used by `StopRestart`).
    factory: EngineFactory<S>,
}

impl<S: StorageEngine + 'static> Group<S> {
    fn start(seed: u64, replicas: usize, factory: EngineFactory<S>) -> Group<S> {
        assert!((3..=5).contains(&replicas));
        let sim = Simulator::new(seed);
        let ids: Vec<u64> = GROUP_IDS[..replicas].to_vec();
        let nodes: Vec<Arc<Node<S>>> = ids
            .iter()
            .map(|&id| {
                Arc::new(RaftKvNode::start(
                    sim.env(nid(id)),
                    ids.clone().into_iter().map(nid).collect(),
                    factory(&sim, id),
                ))
            })
            .collect();
        Group {
            sim,
            nodes: Arc::new(Mutex::new(nodes)),
            replicas,
            shared: Arc::new(Shared {
                rec: Mutex::new(Recorder::new(seed)),
                next_value: Mutex::new(0),
                epoch: Mutex::new(0),
            }),
            crashed: BTreeSet::new(),
            factory,
        }
    }

    /// Spawn `clients` concurrent client tasks, each on its own never-faulted
    /// driver env, routing single-key ops to whichever group node currently leads.
    fn spawn_workload(&mut self, clients: usize, rounds: u64, keyspace: u64, read_pct: u64) {
        for (c, &client_id) in CLIENT_IDS.iter().enumerate().take(clients) {
            let env = self.sim.env(nid(client_id));
            let nodes = Arc::clone(&self.nodes);
            let shared = Arc::clone(&self.shared);
            let proc = c as Process;
            env.clone().spawn_task(async move {
                client_loop(
                    env, nodes, shared, proc, clients, rounds, keyspace, read_pct,
                )
                .await;
            });
        }
    }

    fn apply(&mut self, nem: Nemesis) {
        let ids: Vec<u64> = GROUP_IDS[..self.replicas].to_vec();
        match nem {
            Nemesis::LeaderKill => {
                if let Some((li, _)) = leader_slot(&self.nodes) {
                    self.sim.crash(nid(ids[li]));
                    self.crashed.insert(ids[li]);
                }
            }
            Nemesis::FollowerKill => {
                let leader = leader_slot(&self.nodes).map(|(i, _)| i);
                // First non-leader, live replica.
                let victim = (0..self.replicas)
                    .find(|&i| Some(i) != leader && !self.crashed.contains(&ids[i]));
                if let Some(i) = victim {
                    self.sim.crash(nid(ids[i]));
                    self.crashed.insert(ids[i]);
                }
            }
            Nemesis::PartitionLeader => {
                if let Some((li, _)) = leader_slot(&self.nodes) {
                    for j in 0..self.replicas {
                        if j != li {
                            self.sim.partition_pair(nid(ids[li]), nid(ids[j]));
                        }
                    }
                }
            }
            Nemesis::Lossy => {
                self.sim.set_net_config(lossy(0.1));
            }
            Nemesis::StopRestart => {
                // Victim: the current leader (the sharpest restart — forces both
                // a WAL recovery and an election); first live replica if none
                // leads. Never a sim-crashed node (its restart belongs to
                // `heal_all`).
                let vi = leader_slot(&self.nodes)
                    .map(|(i, _)| i)
                    .filter(|&i| !self.crashed.contains(&ids[i]))
                    .or_else(|| (0..self.replicas).find(|&i| !self.crashed.contains(&ids[i])));
                let Some(vi) = vi else { return };
                let vid = ids[vi];
                // Process exit: tasks + in-memory RaftCore + un-synced disk die;
                // the synced WAL (and, on the LSM tier, the engine's files)
                // survive.
                self.sim.stop(nid(vid));
                // A fresh node on the same id: re-open the engine from disk and
                // recover the Raft state from the durable WAL, then rejoin.
                let engine = (self.factory)(&self.sim, vid);
                let fresh = Arc::new(RaftKvNode::start(
                    self.sim.env(nid(vid)),
                    ids.clone().into_iter().map(nid).collect(),
                    engine,
                ));
                self.nodes.lock().unwrap()[vi] = fresh;
                // Proposals made to the old node object died with it.
                self.shared.bump_epoch();
            }
            Nemesis::SplitBrain => {
                // Every replica an island: no majority anywhere, commits stall.
                for i in 0..self.replicas {
                    for j in (i + 1)..self.replicas {
                        self.sim.partition_pair(nid(ids[i]), nid(ids[j]));
                    }
                }
            }
            Nemesis::LeaderMinority => {
                if let Some((li, _)) = leader_slot(&self.nodes) {
                    // Minority = the leader + the first other replica.
                    let partner = (0..self.replicas).find(|&i| i != li);
                    let minority: BTreeSet<usize> =
                        [Some(li), partner].into_iter().flatten().collect();
                    for &m in &minority {
                        for o in 0..self.replicas {
                            if !minority.contains(&o) {
                                self.sim.partition_pair(nid(ids[m]), nid(ids[o]));
                            }
                        }
                    }
                }
            }
        }
    }

    /// Heal every partition, restart every crashed node, restore default links.
    fn heal_all(&mut self) {
        let ids: Vec<u64> = GROUP_IDS[..self.replicas].to_vec();
        for i in 0..ids.len() {
            for j in (i + 1)..ids.len() {
                self.sim.heal(nid(ids[i]), nid(ids[j]));
            }
        }
        let crashed: Vec<u64> = self.crashed.iter().copied().collect();
        for v in crashed {
            self.sim.restart(nid(v));
        }
        self.crashed.clear();
        self.sim.set_net_config(NetConfig::default());
    }
}

/// One client's loop: each round, run a single-key read or write op (single-writer
/// for writes), then record the outcome.
#[allow(clippy::too_many_arguments)]
async fn client_loop<S: StorageEngine + 'static>(
    env: SimEnv,
    nodes: Nodes<S>,
    shared: Arc<Shared>,
    proc: Process,
    clients: usize,
    rounds: u64,
    keyspace: u64,
    read_pct: u64,
) {
    // This client's own authoritative list per owned key (it is the sole writer,
    // runs serially) — each append builds on it, not a begin-time read.
    let mut my_lists: BTreeMap<Key, Vec<u64>> = BTreeMap::new();
    let owned: Vec<Key> = (0..keyspace)
        .filter(|&k| owner(k, clients) == proc)
        .collect();
    for _round in 0..rounds {
        let is_read = env.gen_below(100) < read_pct;
        if is_read {
            // A read may observe any key in the shared space.
            let key = env.gen_below(keyspace);
            run_read(&env, &nodes, &shared, proc, key).await;
        } else if !owned.is_empty() {
            // A write only appends to a key this client owns (single-writer).
            let key = owned[env.gen_below(owned.len() as u64) as usize];
            run_write(&env, &nodes, &shared, proc, key, &mut my_lists).await;
        }
        // Small gap so clients interleave.
        env.sleep(POLL).await;
    }
}

/// Run a single-key **list-append** write: append this op's globally-unique value
/// to the client's authoritative list for `key`, propose `put(key, whole list)` on
/// the current leader, then poll a `linearizable_get` until the value is visible
/// (committed + durable + applied) → `ok`, else `info`. Idempotent re-proposes on
/// leader change (the stored value is the whole list, so a re-propose is a no-op)
/// — and on a node **replacement** (`StopRestart` bumps the epoch: a proposal made
/// to the stopped process died with it).
async fn run_write<S: StorageEngine + 'static>(
    env: &SimEnv,
    nodes: &Nodes<S>,
    shared: &Arc<Shared>,
    proc: Process,
    key: Key,
    my_lists: &mut BTreeMap<Key, Vec<u64>>,
) {
    let value = shared.fresh_value();
    let list = my_lists.entry(key).or_default();
    list.push(value);
    let encoded = encode_list(list);
    let kb = key_bytes(key);
    let mops = vec![Mop::Append { key, value }];
    shared
        .rec
        .lock()
        .unwrap()
        .invoke(proc, env.now().0, mops.clone());

    let deadline = env.now().0 + OP_BUDGET.as_nanos() as u64;
    let mut proposed_on: Option<(usize, u64)> = None;
    let mut committed = false;
    while env.now().0 < deadline {
        let epoch = shared.epoch();
        if let Some((li, node)) = leader_slot(nodes)
            && proposed_on != Some((li, epoch))
            && let ProposeResult::Accepted { .. } = node.put(kb.clone(), encoded.clone())
        {
            proposed_on = Some((li, epoch));
        }
        env.sleep(POLL).await;
        if let Some((_, node)) = leader_slot(nodes)
            && let Some(bytes) = node.linearizable_get(&kb).await
            && decode_list(&bytes).contains(&value)
        {
            committed = true;
            break;
        }
    }
    let mut rec = shared.rec.lock().unwrap();
    if committed {
        rec.ok(proc, env.now().0, mops);
    } else {
        // Indeterminate — the proposal may yet commit. Never `fail`.
        rec.info(proc, env.now().0, mops);
    }
}

/// Run a single-key **read**: poll `linearizable_get` on the current leader until
/// it confirms a value, recording the observed list (`ok`). A `None` return is
/// ambiguous (genuinely-absent key vs. a deposed leader that cannot confirm a read
/// quorum), so a read that never observes a value within budget is recorded `info`
/// — conservative (an `info` read forms no dependency edges), never asserting a
/// possibly-failed read as a definite empty observation.
async fn run_read<S: StorageEngine + 'static>(
    env: &SimEnv,
    nodes: &Nodes<S>,
    shared: &Arc<Shared>,
    proc: Process,
    key: Key,
) {
    let kb = key_bytes(key);
    let invoke = vec![Mop::Read {
        key,
        observed: None,
    }];
    shared.rec.lock().unwrap().invoke(proc, env.now().0, invoke);

    let deadline = env.now().0 + OP_BUDGET.as_nanos() as u64;
    let mut observed: Option<Vec<u64>> = None;
    while env.now().0 < deadline {
        if let Some((_, node)) = leader_slot(nodes)
            && let Some(bytes) = node.linearizable_get(&kb).await
        {
            observed = Some(decode_list(&bytes));
            break;
        }
        env.sleep(POLL).await;
    }
    let mut rec = shared.rec.lock().unwrap();
    match observed {
        Some(list) => rec.ok(
            proc,
            env.now().0,
            vec![Mop::Read {
                key,
                observed: Some(list),
            }],
        ),
        None => rec.info(
            proc,
            env.now().0,
            vec![Mop::Read {
                key,
                observed: None,
            }],
        ),
    }
}

// ---------------------------------------------------------------------------
// The scenario runner + result.
// ---------------------------------------------------------------------------

struct ScenarioResult {
    cycles: animus_test::CheckReport,
    durability: animus_test::CheckReport,
    convergence: animus_test::CheckReport,
    history: History,
    ok_writes: usize,
    nonempty_reads: usize,
    contended: bool,
}

/// The **final list state** read straight from group node `idx`'s engine
/// (`local_get` — the replica's raw stored value), decoded per key. Reading from
/// two *distinct* replicas keeps `check_convergence` a real cross-replica agreement
/// check and `check_durability` ("every acked append is in the final list")
/// meaningful under single-writer-per-key.
fn final_state<S: StorageEngine + 'static>(
    nodes: &Nodes<S>,
    idx: usize,
    keyspace: u64,
) -> BTreeMap<Key, Vec<u64>> {
    let node = Arc::clone(&nodes.lock().unwrap()[idx]);
    let mut map = BTreeMap::new();
    for key in 0..keyspace {
        let list = block_on(node.local_get(&key_bytes(key)))
            .map(|b| decode_list(&b))
            .unwrap_or_default();
        map.insert(key, list);
    }
    map
}

/// Run `scenario` over the engine tier `factory` builds. The `MemoryEngine`
/// wrapper [`run_scenario`] is the always-on default; [`lsm_engine`] is the
/// durable tier.
fn run_scenario_on<S: StorageEngine + 'static>(
    scenario: &Scenario,
    factory: EngineFactory<S>,
) -> ScenarioResult {
    let mut group = Group::start(scenario.seed, scenario.replicas, factory);

    // Let the group elect a leader, then start the concurrent workload.
    group.sim.run_for(SETTLE);
    group.spawn_workload(
        scenario.clients,
        scenario.rounds,
        scenario.keyspace,
        scenario.read_pct,
    );

    // Walk the fault schedule in virtual-time order.
    let mut faults = scenario.faults.clone();
    faults.sort_by_key(|(at, _)| *at);
    let base = group.sim.now().0;
    for (at, nem) in faults {
        let target = base + at.as_nanos() as u64;
        if target > group.sim.now().0 {
            group.sim.run_until(animus_env::Nanos(target));
        }
        group.apply(nem);
    }

    // Hold the last fault open for the scenario's outage window (zero for the
    // original frozen cells — their runs are byte-identical to the committed
    // corpus), so the group must genuinely ride the fault out before heal.
    if !scenario.window.is_zero() {
        group.sim.run_for(scenario.window);
    }

    // End healthy so the workload tail + final reads can make a quorum.
    group.heal_all();
    group.sim.run_for(DRAIN);

    let keys = scenario.keyspace;
    let history = group.shared.rec.lock().unwrap().history().clone();
    let cycles = check_cycles(&history);

    // Converged-or-timeout poll for the eventual properties: a lagging follower
    // may still be catching up at the fixed drain, so
    // re-read in bounded increments and stop early once both hold.
    let last = group.replicas - 1;
    let mut a = final_state(&group.nodes, 0, keys);
    let mut b = final_state(&group.nodes, last, keys);
    let mut durability = check_durability(&history, &a);
    let mut convergence = check_convergence(scenario.seed, &a, &b);
    let poll_deadline = group.sim.now().0 + CONVERGENCE_BUDGET.as_nanos() as u64;
    while !(convergence.ok && durability.ok) && group.sim.now().0 < poll_deadline {
        group.sim.run_for(CONVERGENCE_POLL_STEP);
        a = final_state(&group.nodes, 0, keys);
        b = final_state(&group.nodes, last, keys);
        durability = check_durability(&history, &a);
        convergence = check_convergence(scenario.seed, &a, &b);
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
    let mut per_key: BTreeMap<Key, usize> = BTreeMap::new();
    for e in history.ok_entries() {
        for m in &e.mops {
            if let Mop::Append { key, .. } = m {
                *per_key.entry(*key).or_default() += 1;
            }
        }
    }
    let contended = per_key.values().any(|&c| c >= 2);

    ScenarioResult {
        cycles,
        durability,
        convergence,
        history,
        ok_writes,
        nonempty_reads,
        contended,
    }
}

fn run_scenario(scenario: &Scenario) -> ScenarioResult {
    run_scenario_on(scenario, mem_engine)
}

/// Assert the three checks on one scenario result, labelling the engine tier in
/// the failure message. Serializability is a **safety** property (hard assert at
/// any depth); durability + convergence already sat behind the converged-or-
/// timeout poll, so a failure here means the budget was genuinely exhausted.
fn assert_scenario_ok(tier: &str, s: &Scenario, r: &ScenarioResult) {
    assert!(
        r.cycles.ok,
        "[{tier}] scenario {} not serializable: {:?} (seed={})",
        s.name, r.cycles.violations, s.seed
    );
    assert!(
        r.durability.ok,
        "[{tier}] scenario {} lost an acked append: {:?} (seed={})",
        s.name, r.durability.violations, s.seed
    );
    assert!(
        r.convergence.ok,
        "[{tier}] scenario {} did not converge: {:?} (seed={})",
        s.name, r.convergence.violations, s.seed
    );
}

// ---------------------------------------------------------------------------
// Failure minimization (ADR 0061 rung B4): `ANIMUS_SHRINK=1` wiring for this
// corpus, on top of `animus_test::shrink`'s generic engine. Strategy (a)
// (scenario-parameter minimization) per that module's doc — the seed stays
// fixed throughout, only `Scenario`'s own explicit fields (never RNG-drawn
// here) are reduced.
// ---------------------------------------------------------------------------

/// Whether a scenario result violates any of the three checks — the shared
/// failure predicate for both `assert_scenario_ok` (via the checks it makes)
/// and the shrink wiring below (which needs a plain `bool`, not an assert).
fn scenario_failed(r: &ScenarioResult) -> bool {
    !r.cycles.ok || !r.durability.ok || !r.convergence.ok
}

/// Delta-debugging candidates for a [`Scenario`]: every "one step smaller"
/// variant, in a fixed, deterministic order. Each candidate changes exactly
/// one dimension:
///
/// - drop one scheduled fault (tried first — usually the highest-leverage
///   move, and the one this corpus's `faults: Vec<(Duration, Nemesis)>` field
///   makes possible for free, since it's already an explicit, un-randomized
///   list rather than something drawn from `NetConfig`'s ambient probability
///   — see `animus_test::shrink`'s module doc on why that granularity is
///   reachable under strategy (a) here specifically);
/// - zero a real outage window;
/// - halve `rounds` / `keyspace` (floor 1);
/// - decrement `clients` (floor 1).
///
/// Deliberately **not** touched: `name` (identity), `seed` (fixed throughout
/// — strategy (a)'s whole premise), and `replicas` (some nemeses, e.g.
/// `LeaderMinority`, are only meaningful at a specific group size; reducing
/// it risks silently changing which fault the scenario even models, which
/// would no longer be minimizing the *same* failure).
fn scenario_candidates(s: &Scenario) -> Vec<Scenario> {
    let mut out = Vec::new();
    for i in 0..s.faults.len() {
        let mut faults = s.faults.clone();
        faults.remove(i);
        out.push(Scenario {
            faults,
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

/// Shrink one observed failure and print a report + a copy-pasteable replay
/// handle. Called only after a scenario is already known to have failed —
/// never on the hot path of a green run — and only when `ANIMUS_SHRINK=1`, so
/// this adds zero cost/behavior to a normal corpus run (the default).
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
                 cargo test -p animus-test --test raftkv_linearizable \\\n    \
                 raftkv_shrink_replay -- --ignored --nocapture"
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
fn raftkv_baseline_is_linearizable() {
    let scenario = base_workload("baseline_3", 3, vec![]);
    let r = run_scenario(&scenario);
    assert!(
        r.cycles.ok,
        "baseline must be serializable: {:?} (seed={})",
        r.cycles.violations, scenario.seed
    );
    assert!(
        r.durability.ok,
        "baseline must be durable: {:?} (seed={})",
        r.durability.violations, scenario.seed
    );
    assert!(
        r.convergence.ok,
        "baseline must converge: {:?} (seed={})",
        r.convergence.violations, scenario.seed
    );
    // Teeth: the run must have actually done linearizable work, and contended.
    assert!(r.ok_writes > 0, "no acked writes — vacuous run");
    assert!(
        r.nonempty_reads > 0,
        "no non-empty reads — checker had nothing to chew"
    );
    assert!(
        r.contended,
        "no key saw ≥ 2 acked appends — workload did not contend (seed={})",
        scenario.seed
    );
}

#[test]
fn raftkv_corpus_is_linearizable() {
    let scenarios = corpus();
    let mut total_ok_writes = 0usize;
    let mut faulted_with_acks = 0usize;
    for s in &scenarios {
        let r = run_scenario(s);
        // Serializability is a SAFETY property — it must hold on every scenario.
        // Convergence + durability are EVENTUAL — the poll gives them room to
        // heal; only budget exhaustion is a genuine failure.
        if scenario_failed(&r) && shrink::shrink_enabled() {
            // Never on the hot path of a green run — only after we already
            // know this scenario failed, and only opted in via
            // ANIMUS_SHRINK=1 (see the root CLAUDE.md knob table). Still
            // asserts below exactly as before: minimization only adds a
            // diagnostic, it never softens the failure.
            shrink_and_report(s);
        }
        assert_scenario_ok("mem", s, &r);
        total_ok_writes += r.ok_writes;
        if !s.faults.is_empty() && r.ok_writes > 0 {
            faulted_with_acks += 1;
        }
    }
    // Non-vacuity guards: the corpus as a whole did real, fault-tolerant work.
    assert!(
        total_ok_writes > scenarios.len(),
        "corpus too vacuous: only {total_ok_writes} acked writes across {} scenarios",
        scenarios.len()
    );
    assert!(
        faulted_with_acks >= scenarios.len() / 2,
        "too few faulted scenarios kept serving writes ({faulted_with_acks}) — \
         faults may be downing the group entirely"
    );
}

/// Coverage guard:
/// the generator must keep exercising every fault class, both group shapes,
/// compound schedules, and real outage windows — otherwise a dimension silently
/// stopped being tested. Structural only (no scenario runs). Also pins the
/// frozen-name discipline: the original cells stay present with their canonical
/// name-derived seeds.
#[test]
fn raftkv_corpus_covers_the_fault_matrix() {
    let cells = corpus_cells();

    let mut seen_faults: BTreeSet<Nemesis> = BTreeSet::new();
    let mut seen_shapes: BTreeSet<usize> = BTreeSet::new();
    let mut compound = 0usize;
    let mut windowed = 0usize;
    let mut baselines = 0usize;
    for s in &cells {
        seen_shapes.insert(s.replicas);
        if s.faults.len() > 1 {
            compound += 1;
        }
        if !s.window.is_zero() {
            windowed += 1;
        }
        if s.faults.is_empty() {
            baselines += 1;
        }
        for (_, f) in &s.faults {
            seen_faults.insert(*f);
        }
    }

    for f in [
        Nemesis::LeaderKill,
        Nemesis::FollowerKill,
        Nemesis::PartitionLeader,
        Nemesis::Lossy,
        Nemesis::StopRestart,
        Nemesis::SplitBrain,
        Nemesis::LeaderMinority,
    ] {
        assert!(
            seen_faults.contains(&f),
            "fault {f:?} is not covered by any corpus scenario"
        );
    }
    assert!(
        seen_shapes.contains(&3) && seen_shapes.contains(&5),
        "both 3- and 5-replica shapes must be covered: {seen_shapes:?}"
    );
    assert!(baselines >= 2, "expected ≥ 2 no-fault baselines");
    assert!(
        compound >= 2,
        "expected ≥ 2 compound (multi-fault) scenarios, found {compound}"
    );
    assert!(
        windowed >= 8,
        "expected ≥ 8 scenarios with a real outage window, found {windowed}"
    );
    assert!(
        cells.len() >= 29,
        "corpus shrank unexpectedly to {} cells",
        cells.len()
    );

    // Names + seeds are unique (a failure unambiguously names one scenario).
    let names: BTreeSet<&str> = cells.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names.len(), cells.len(), "corpus names must be unique");
    let seeds: BTreeSet<u64> = cells.iter().map(|s| s.seed).collect();
    assert_eq!(seeds.len(), cells.len(), "corpus seeds must be unique");

    // Frozen-name discipline: the original cells are still present, seeded by
    // their own names, and window-less (their runs stay byte-identical).
    for legacy in [
        "baseline_3",
        "baseline_5",
        "leader_kill_early_3",
        "follower_kill_mid_3",
        "partition_leader_late_3",
        "lossy_mid_5",
    ] {
        let cell = cells
            .iter()
            .find(|s| s.name == legacy)
            .unwrap_or_else(|| panic!("frozen cell {legacy} disappeared from the corpus"));
        assert_eq!(
            cell.seed,
            corpus::name_seed(legacy),
            "frozen seed moved for {legacy}"
        );
        assert!(
            cell.window.is_zero(),
            "frozen cell {legacy} grew an outage window (its run must stay byte-identical)"
        );
    }
}

/// Seed-depth lever (`ANIMUS_RAFTKV_SEEDS`): expanding the cells by `k` yields
/// exactly `k×` scenarios, names/seeds stay unique, and **variant 0 preserves the
/// canonical (frozen) name+seed** — growing depth never moves a regression seed.
/// Structural only.
#[test]
fn raftkv_seed_expansion_is_additive_and_unique() {
    let base = corpus_cells();
    let k = 3;
    let expanded = corpus::seed_expand(base.clone(), k);
    assert_eq!(expanded.len(), base.len() * k);

    let names: BTreeSet<&str> = expanded.iter().map(|s| s.name.as_str()).collect();
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
    // k == 1 is the identity (the always-on default is byte-identical to base).
    assert_eq!(corpus::seed_expand(base.clone(), 1).len(), base.len());
}

#[test]
fn raftkv_run_is_deterministic() {
    // Same scenario twice → byte-identical recorded history (ADR 0003).
    let scenario = base_workload(
        "leader_kill_mid_3",
        3,
        vec![(Duration::from_millis(2200), Nemesis::LeaderKill)],
    );
    let a = run_scenario(&scenario);
    let b = run_scenario(&scenario);
    assert_eq!(
        serde_json::to_string(&a.history).unwrap(),
        serde_json::to_string(&b.history).unwrap(),
        "history not reproducible for seed {}",
        scenario.seed
    );
}

// ---------------------------------------------------------------------------
// The LsmEngine tier: the durable path (real WAL/SSTable recovery through the
// deterministic disk seam) under faults — what production actually runs on, and
// what no corpus exercised before.
// ---------------------------------------------------------------------------

/// Always-on representative subset over `LsmEngine<SimEnv>`: a healthy baseline,
/// a crash, the WAL-recovering restart (the scenario whose semantics *differ* on
/// a durable engine — the fresh node recovers its state from the LSM files + Raft
/// WAL, not an empty `MemoryEngine`), and the compound lossy+restart. Kept small
/// so the default `cargo test` stays fast; `ANIMUS_RAFTKV_LSM=1` runs the whole
/// corpus on this tier.
const LSM_REPRESENTATIVE: [&str; 4] = [
    "baseline_3",
    "leader_kill_mid_3",
    "stop_restart_mid_3",
    "lossy_stop_restart_3",
];

#[test]
fn raftkv_lsm_representative_is_linearizable() {
    let cells = corpus_cells();
    let mut ran = 0usize;
    let mut ok_writes = 0usize;
    for name in LSM_REPRESENTATIVE {
        let s = cells
            .iter()
            .find(|s| s.name == name)
            .unwrap_or_else(|| panic!("representative LSM scenario {name} not in the corpus"));
        let r = run_scenario_on(s, lsm_engine);
        assert_scenario_ok("lsm", s, &r);
        ran += 1;
        ok_writes += r.ok_writes;
    }
    assert_eq!(ran, LSM_REPRESENTATIVE.len());
    assert!(
        ok_writes > ran,
        "LSM tier too vacuous: only {ok_writes} acked writes across {ran} scenarios"
    );
}

/// The **full corpus over the LSM engine** — the deep/nightly tier
/// (`ANIMUS_RAFTKV_LSM=1`, composable with `ANIMUS_RAFTKV_SEEDS` for depth).
/// Default off so the always-on suite stays fast; the representative subset above
/// keeps the tier from going stale.
#[test]
fn raftkv_lsm_full_corpus_is_linearizable() {
    if !lsm_full_enabled() {
        eprintln!("raftkv_lsm_full_corpus_is_linearizable: skipped (set ANIMUS_RAFTKV_LSM=1)");
        return;
    }
    let scenarios = corpus();
    let mut faulted_with_acks = 0usize;
    for s in &scenarios {
        let r = run_scenario_on(s, lsm_engine);
        assert_scenario_ok("lsm", s, &r);
        if !s.faults.is_empty() && r.ok_writes > 0 {
            faulted_with_acks += 1;
        }
    }
    assert!(
        faulted_with_acks >= scenarios.len() / 2,
        "too few faulted LSM scenarios kept serving writes ({faulted_with_acks})"
    );
}

/// The LSM tier is deterministic too: the same scenario (WAL recovery included)
/// twice → byte-identical histories (ADR 0003 extends through the disk seam).
#[test]
fn raftkv_lsm_run_is_deterministic() {
    let cells = corpus_cells();
    let scenario = cells
        .iter()
        .find(|s| s.name == "stop_restart_mid_3")
        .expect("stop_restart_mid_3 exists");
    let a = run_scenario_on(scenario, lsm_engine);
    let b = run_scenario_on(scenario, lsm_engine);
    assert_eq!(
        serde_json::to_string(&a.history).unwrap(),
        serde_json::to_string(&b.history).unwrap(),
        "LSM history not reproducible for seed {}",
        scenario.seed
    );
}

// ---------------------------------------------------------------------------
// Failure minimization (ADR 0061 rung B4) — the end-to-end proof against the
// real simulator (`animus_test::shrink`'s own crate-local unit tests already
// prove the algorithm itself on a fast synthetic case; this proves the
// *wiring* against this corpus's real `Scenario`/`run_scenario`/`RaftKvNode`
// stack).
// ---------------------------------------------------------------------------

/// A manufactured, deterministic regression — never touches the corpus's own
/// pass/fail assertions (`assert_scenario_ok`) or any invariant the real
/// system claims — that gives the shrinker a **genuine, real-simulator-
/// derived** failure to reduce rather than a synthetic int predicate:
/// `read_pct = 100` makes `client_loop` structurally never choose a write
/// (`is_read = gen_below(100) < read_pct` is always true — see
/// `client_loop` above), so `ok_writes == 0` for **any** fault schedule,
/// group shape, or workload size — a real property of a real run, just an
/// uninteresting one, chosen because it makes the "this dimension is a red
/// herring" claim checkable by construction rather than by luck.
///
/// Reproduces `ANIMUS_SHRINK`'s real corpus wiring end to end: the four
/// scheduled faults and the workload's own size are every dimension
/// [`scenario_candidates`] knows how to reduce, none of them affect this
/// failure at all, so a correct minimizer must strip **all** of them to
/// their floor while the (real, simulator-derived) failure keeps
/// reproducing throughout — proving the wiring against genuine `RaftKvNode`/
/// `Simulator` runs, not a unit-test stand-in.
#[test]
fn raftkv_shrink_reduces_a_real_regression_to_its_minimal_repro() {
    let mut scenario = base_workload(
        "shrink_demo_read_only_never_writes",
        3,
        vec![
            (Duration::from_millis(700), Nemesis::LeaderKill),
            (Duration::from_millis(1500), Nemesis::FollowerKill),
            (Duration::from_millis(2200), Nemesis::PartitionLeader),
            (Duration::from_millis(3000), Nemesis::Lossy),
        ],
    );
    scenario.read_pct = 100;
    scenario.rounds = 6;
    scenario.clients = 3;
    scenario.keyspace = 3;

    fn regression_reproduces(s: &Scenario) -> bool {
        run_scenario(s).ok_writes == 0
    }

    assert!(
        regression_reproduces(&scenario),
        "sanity: the manufactured case must genuinely fail (0 acked writes) \
         before shrinking it"
    );

    let report = shrink::minimize(
        scenario.clone(),
        scenario_candidates,
        regression_reproduces,
        shrink::ShrinkBudget::default(),
    );
    eprintln!("{}", shrink::describe(&scenario.name, &report));

    assert!(
        report.converged(),
        "expected a genuine fixpoint within the default budget: {report:?}"
    );
    assert!(
        report.minimized.faults.is_empty(),
        "every scheduled fault is a red herring for this regression and must \
         be stripped, got {:?}",
        report.minimized.faults
    );
    assert_eq!(
        report.minimized.rounds, 1,
        "round count is irrelevant to this regression and must hit its floor"
    );
    assert_eq!(
        report.minimized.keyspace, 1,
        "keyspace is irrelevant to this regression and must hit its floor"
    );
    assert_eq!(
        report.minimized.clients, 1,
        "client count is irrelevant to this regression and must hit its floor"
    );
    assert_eq!(
        report.minimized.seed, scenario.seed,
        "strategy (a) never changes the seed — see animus_test::shrink's module doc"
    );
    assert!(
        regression_reproduces(&report.minimized),
        "the minimized case must still reproduce the original failure"
    );

    // The printed replay handle round-trips through serde exactly as a
    // developer pasting it into ANIMUS_SHRINK_REPLAY (`raftkv_shrink_replay`,
    // below) would rely on — proving the handle itself is actually
    // reusable, not just that this process's own in-memory report looks
    // right. (`raftkv_shrink_replay`'s own predicate is `scenario_failed`,
    // the real corpus's failure notion — not this demo's manufactured
    // `ok_writes == 0` regression — so it is exercised separately, in
    // `raftkv_corpus_is_linearizable`'s own `ANIMUS_SHRINK=1` path.)
    let json = shrink::replay_json(&report).expect("Scenario must serialize");
    let replayed: Scenario = serde_json::from_str(&json).expect("Scenario must round-trip");
    assert_eq!(replayed.seed, report.minimized.seed);
    assert_eq!(replayed.faults.len(), report.minimized.faults.len());
    assert!(regression_reproduces(&replayed));
}

/// The replay entry point named in every `ANIMUS_SHRINK` report
/// (`shrink_and_report`'s printed instructions): paste the JSON a shrink run
/// printed into `ANIMUS_SHRINK_REPLAY` and run this single test to re-run
/// exactly that minimized case and confirm it still reproduces. `#[ignore]`d
/// like any other opt-in diagnostic entry point — it does nothing (and
/// asserts nothing) when the env var is unset, so it is inert in a normal
/// `cargo test` run.
#[test]
#[ignore = "opt-in replay entry point — set ANIMUS_SHRINK_REPLAY to a shrink report's printed JSON"]
fn raftkv_shrink_replay() {
    let Ok(json) = std::env::var("ANIMUS_SHRINK_REPLAY") else {
        eprintln!(
            "raftkv_shrink_replay: skipped — set ANIMUS_SHRINK_REPLAY to a \
             shrink report's printed JSON to replay it"
        );
        return;
    };
    let scenario: Scenario =
        serde_json::from_str(&json).expect("ANIMUS_SHRINK_REPLAY must be a Scenario JSON blob");
    let r = run_scenario(&scenario);
    eprintln!(
        "replayed '{}' (seed={}): cycles.ok={} durability.ok={} convergence.ok={} ok_writes={}",
        scenario.name, scenario.seed, r.cycles.ok, r.durability.ok, r.convergence.ok, r.ok_writes
    );
    assert!(
        scenario_failed(&r),
        "replayed scenario '{}' (seed={}) did NOT reproduce the failure — \
         cycles.ok={} durability.ok={} convergence.ok={}",
        scenario.name,
        scenario.seed,
        r.cycles.ok,
        r.durability.ok,
        r.convergence.ok
    );
}
