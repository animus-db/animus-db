//! Shared harness for the **Elle-against-Accord** consistency milestone.
//!
//! This module assembles an Accord transaction-consensus replica set wired to
//! the replicated data plane over `SimEnv`, drives **concurrent, conflicting**
//! multi-key transactions through it, records the run as an Elle list-append
//! [`History`], and runs the `animus-test` checkers over it. It also defines the
//! declarative [`Scenario`] / [`NemesisAction`] model and the [`run_scenario`]
//! runner that the frozen corpus (`corpus.rs`) is built from.
//!
//! # Genuine black-box list-append over Accord (ADR 0014, closed limitation)
//!
//! Accord is the layer that *claims* a consistent serialization order, so it is
//! where a serializability checker has teeth (the AP/LWW data plane only offers
//! convergence/read-your-writes — checked elsewhere). Earlier this harness had a
//! limitation: Accord's execution effect was hard-coded to "write my transaction
//! id" (a register), so the harness *reconstructed* each read's observed list
//! from a replica's `applied_order` rather than from actually-stored state. That
//! limited the checker's teeth to cross-replica order divergence.
//!
//! With **arbitrary caller-supplied write values** (ADR 0011) that limitation is
//! closed: each key stores a *real list value*, and the workload is genuine
//! black-box list-append:
//!
//! - A key's value is an encoded `Vec<u64>` (the list). A **write** op is a real
//!   **read-modify-write** (`InteractiveTxn`: `read_value` the current list,
//!   append a **globally-unique** element, `write_value` the new list back) — so
//!   the stored bytes *are* the list, ordered by Accord. Recorded as
//!   `Append { k, value }` for each written key.
//! - A **read** op observes the **actual stored list** (decoded from the bytes a
//!   read transaction returns via `read_value_result`), recorded as
//!   `Read { k, observed: Some(list) }`.
//!
//! The order is now recovered from observed *values* by Elle's `recover`, **not**
//! from `applied_order`. So `check_cycles` is a real black-box serializability
//! check: a single globally-agreed-but-non-serializable order would surface as a
//! dependency cycle, not merely as cross-replica divergence.
//!
//! **Single-writer-per-key (the LWW guard).** Each key is written by exactly one
//! client (`owner(key) = key % clients`); a write transaction only appends to the
//! keys it owns. Concurrent writers to one key would lose updates by the *data
//! model* (per-key LWW) — not a consistency bug, and it would drown the checker
//! in false positives. Cross-transaction conflict (the wr/rw/ww edges the cycle
//! checker chews on) still comes from **multi-key transactions** and from reads
//! observing keys *other* clients write. (See `animus-test` CLAUDE.md.)
//!
//! All nondeterminism is the simulator's (ADR 0003); a run is a pure function of
//! its seed. The Accord driver has a perpetual retry timer, so we always drive
//! bounded virtual time (`run_for`), never `run()`.

#![allow(dead_code)] // shared across test binaries; not every item is used by each.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_consensus::{AccordNode, Key, TxnId};
use animus_data::{TabletView, serve_replica};
use animus_env::{Clock, EnvExt, Rng};
use animus_sim::{NetConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{Epoch, KeyRange, Tablet, TabletId};
use animus_test::history::{Mop, Process};
use animus_test::{History, Recorder, check_convergence, check_cycles, check_durability};

// --- Topology. One inbox per node id (single-consumer), so every role gets a
// distinct id. Accord replicas, each Accord node's own data-plane coordinator,
// the data-plane replicas, and a standalone verifier. We size for up to 5 Accord
// replicas + 5 data replicas; smaller clusters use a prefix. ---

/// Accord consensus replica node ids.
const ACCORD_IDS: [u64; 5] = [0, 1, 2, 3, 4];
/// Per-Accord-node data-plane coordinator ids (distinct inbox per coordinator).
const COORD_IDS: [u64; 5] = [10, 11, 12, 13, 14];
/// Data-plane replica node ids.
const DATA_IDS: [u64; 5] = [20, 21, 22, 23, 24];
/// Standalone verifier coordinator for final quorum snapshots.
const VERIFIER: u64 = 30;

/// Quorum read/write thresholds for the data plane. With the default 3 data
/// replicas this is the usual `R + W > N`.
const R: usize = 2;
const W: usize = 2;

/// How long a single client op waits (polling `is_applied`) before recording it
/// as indeterminate (`info`). Generous so a slow but eventually-consistent commit
/// is not misclassified — only a genuinely stranded op times out.
const OP_BUDGET: Duration = Duration::from_secs(8);
/// Poll granularity while a client waits for its transaction to execute.
const POLL: Duration = Duration::from_millis(100);

// ---------------------------------------------------------------------------
// Declarative scenario model (deliverable 3).
// ---------------------------------------------------------------------------

/// The cluster shape a scenario runs over.
#[derive(Clone, Copy, Debug)]
pub struct ClusterShape {
    /// Number of Accord consensus replicas (3 or 5).
    pub accord_replicas: usize,
    /// Number of data-plane replicas (3 or 5).
    pub data_replicas: usize,
}

impl ClusterShape {
    /// The default 3+3 cluster.
    pub const SMALL: ClusterShape = ClusterShape {
        accord_replicas: 3,
        data_replicas: 3,
    };
    /// A 5+5 cluster (more replicas → larger quorums, more partition surface).
    pub const LARGE: ClusterShape = ClusterShape {
        accord_replicas: 5,
        data_replicas: 5,
    };
}

/// The shape of the concurrent workload a scenario drives.
#[derive(Clone, Copy, Debug)]
pub struct WorkloadSpec {
    /// Number of concurrent client coordinators issuing transactions.
    pub clients: usize,
    /// Rounds each client runs.
    pub rounds: u64,
    /// Size of the shared key space. Smaller → more contention.
    pub keyspace: u64,
    /// Number of keys touched per transaction (≥ 2 makes multi-key conflicts).
    pub keys_per_txn: usize,
    /// Probability (out of 100) that a given op is a read rather than a write.
    pub read_pct: u64,
}

impl WorkloadSpec {
    /// A high-contention default: a few clients hammering a tiny key space with
    /// multi-key read/write transactions — exactly the regime where the
    /// serializability checker can form a cycle if the ordering layer is wrong.
    pub const CONTENDED: WorkloadSpec = WorkloadSpec {
        clients: 4,
        rounds: 6,
        keyspace: 4,
        keys_per_txn: 2,
        read_pct: 40,
    };
}

/// A fault the nemesis injects at a scheduled virtual time. Targets are resolved
/// against the live cluster shape at run time (so a scenario is shape-relative).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum NemesisAction {
    /// Partition a *minority* of the Accord replicas away from the majority
    /// (consensus still reachable). The minority is the highest-indexed
    /// `floor((n-1)/2)` replicas.
    PartitionMinority,
    /// Partition the Accord replicas into two halves with **no** majority on
    /// either side (consensus stalls until healed) — a true split brain.
    PartitionMajority,
    /// Isolate a single Accord replica from every other node (Accord + data +
    /// coordinators).
    IsolateOne,
    /// Crash one Accord replica (drops un-synced disk + inbox, mutes its sends)
    /// without restarting it within the scenario.
    Crash,
    /// Stop one Accord replica's process and start a fresh node on the same id
    /// (recovers from its durable WAL) — the restart-and-rejoin path.
    StopRestart,
    /// Crash the Accord replica acting as the data-plane "leader" stand-in: the
    /// first data replica (the one most quorums include). Models losing a hot
    /// node. (Accord is leaderless, so this targets the data plane's primary.)
    LeaderKill,
    /// Heal every partition and uncrash/restart anything the schedule downed, so
    /// the workload's tail and the final snapshot run on a healthy cluster.
    HealAll,
    /// Inject lossy links (independent per-message drop) for the rest of the run.
    Lossy,
}

/// A declarative, seed-reproducible test scenario: a named cluster shape +
/// workload + an explicit fault schedule (virtual time → action).
#[derive(Clone, Debug)]
pub struct Scenario {
    /// A stable, human-readable name (also used in failure messages).
    pub name: String,
    /// The run seed (the scenario is a pure function of it).
    pub seed: u64,
    /// The cluster shape.
    pub cluster: ClusterShape,
    /// The workload.
    pub workload: WorkloadSpec,
    /// The fault schedule: `(at_virtual_time, action)`, applied in order.
    pub faults: Vec<(Duration, NemesisAction)>,
}

// ---------------------------------------------------------------------------
// The frozen corpus (deliverable 4): a committed, deterministic generator that
// materializes a fixed, named, indexed set of scenarios with combinatorial
// coverage of the fault matrix. NOT a live-random test — every entry has a fixed
// seed and a stable name, so the suite runs the SAME scenarios every time and a
// failure names the specific scenario (and carries its seed for replay).
// ---------------------------------------------------------------------------

/// The single-fault nemesis actions sampled across the corpus (each pairs a fault
/// *type* with an implicit *target class*). `HealAll` is always auto-applied at
/// the end by the runner, so it is not scheduled here; `Lossy` appears as a
/// background modifier on some scenarios rather than a one-shot.
const CORPUS_FAULTS: [(&str, NemesisAction); 6] = [
    ("part_minority", NemesisAction::PartitionMinority),
    ("part_majority", NemesisAction::PartitionMajority),
    ("isolate_one", NemesisAction::IsolateOne),
    ("crash", NemesisAction::Crash),
    ("stop_restart", NemesisAction::StopRestart),
    ("leader_kill", NemesisAction::LeaderKill),
];

/// Timing of a one-shot fault relative to the workload's life: early (just after
/// the workload starts), mid (steady state), late (as it winds down). Covering
/// all three exercises a fault hitting a transaction in PreAccept vs Commit vs
/// post-commit execution.
const CORPUS_TIMINGS: [(&str, Duration); 3] = [
    ("early", Duration::from_millis(800)),
    ("mid", Duration::from_millis(2500)),
    ("late", Duration::from_millis(4200)),
];

/// Workload shapes sampled across the corpus, each a distinct contention regime.
fn corpus_workloads() -> [(&'static str, WorkloadSpec); 3] {
    [
        // Tight contention: 4 clients, 4 keys, 2-key txns — heavy overlap.
        ("tight", WorkloadSpec::CONTENDED),
        // Wider key space, more clients, write-heavy.
        (
            "wide_write",
            WorkloadSpec {
                clients: 5,
                rounds: 5,
                keyspace: 6,
                keys_per_txn: 3,
                read_pct: 25,
            },
        ),
        // Read-heavy: more reads → more wr/rw edges for the checker to chew on.
        (
            "read_heavy",
            WorkloadSpec {
                clients: 4,
                rounds: 6,
                keyspace: 4,
                keys_per_txn: 2,
                read_pct: 65,
            },
        ),
    ]
}

/// Build the **frozen scenario corpus**: a deterministic, structured cross-product
/// over { fault type × timing × workload shape × cluster shape }, plus a handful
/// of no-fault and lossy/compound baselines. Each scenario is named (so a failure
/// is attributable) and seeded by a stable hash of its coordinates (so the same
/// scenario reproduces every run, and growing the corpus does not perturb
/// existing seeds).
///
/// This is the explicit "regenerate" step: editing this function changes the
/// corpus. A scenario that ever catches a bug stays here forever as a regression.
pub fn corpus() -> Vec<Scenario> {
    let mut out = Vec::new();
    let mut idx: u64 = 0;

    // A stable per-scenario seed from its coordinates and ordinal. Keeping the
    // ordinal out of the *hash inputs* would be fragile; instead we fold a fixed
    // salt with the coordinate string so names map 1:1 to seeds.
    let seed_for = |name: &str| -> u64 {
        // FNV-1a over the name — deterministic, no std Hasher nondeterminism.
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for b in name.as_bytes() {
            h ^= u64::from(*b);
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        h
    };

    let shapes = [("c33", ClusterShape::SMALL), ("c55", ClusterShape::LARGE)];

    // No-fault baselines (one per workload shape, small cluster) — prove the
    // checker passes a clean contended run and acts as a control for the faulted
    // ones.
    for (wname, w) in corpus_workloads() {
        let name = format!("{idx:03}_baseline_{wname}");
        out.push(Scenario {
            seed: seed_for(&name),
            cluster: ClusterShape::SMALL,
            workload: w,
            faults: Vec::new(),
            name,
        });
        idx += 1;
    }

    // The main matrix: fault × timing × workload × cluster.
    for (sname, shape) in shapes {
        for (wname, w) in corpus_workloads() {
            for (fname, fault) in CORPUS_FAULTS {
                for (tname, at) in CORPUS_TIMINGS {
                    let name = format!("{idx:03}_{sname}_{wname}_{fname}_{tname}");
                    out.push(Scenario {
                        seed: seed_for(&name),
                        cluster: shape,
                        workload: w,
                        faults: vec![(at, fault)],
                        name,
                    });
                    idx += 1;
                }
            }
        }
    }

    // Compound / lossy scenarios: a background lossy network plus a one-shot
    // fault, and a two-fault overlap (partition then crash) — coverage of
    // faults stacking, which single-fault entries miss.
    let (_, tight) = corpus_workloads()[0];
    for (fname, fault) in CORPUS_FAULTS {
        let name = format!("{idx:03}_lossy_{fname}_mid");
        out.push(Scenario {
            seed: seed_for(&name),
            cluster: ClusterShape::SMALL,
            workload: tight,
            faults: vec![
                (Duration::from_millis(300), NemesisAction::Lossy),
                (Duration::from_millis(2500), fault),
            ],
            name,
        });
        idx += 1;
    }
    // Overlapping two-fault scenarios.
    let overlaps = [
        (
            "minority_then_crash",
            NemesisAction::PartitionMinority,
            NemesisAction::Crash,
        ),
        (
            "isolate_then_leaderkill",
            NemesisAction::IsolateOne,
            NemesisAction::LeaderKill,
        ),
    ];
    for (oname, f1, f2) in overlaps {
        let name = format!("{idx:03}_overlap_{oname}");
        out.push(Scenario {
            seed: seed_for(&name),
            cluster: ClusterShape::LARGE,
            workload: tight,
            faults: vec![
                (Duration::from_millis(1500), f1),
                (Duration::from_millis(3000), f2),
            ],
            name,
        });
        idx += 1;
    }

    out
}

// ---------------------------------------------------------------------------
// The running cluster.
// ---------------------------------------------------------------------------

/// A live Accord-over-data-plane cluster plus the shared workload recorder.
pub struct Cluster {
    sim: Simulator,
    nodes: Vec<AccordNode<SimEnv>>,
    view: TabletView,
    shape: ClusterShape,
    shared: Arc<Shared>,
    /// Accord replica ids that have been stopped and not yet re-started.
    stopped: BTreeSet<u64>,
    /// Accord replica ids that have been crashed and not yet healed.
    crashed: BTreeSet<u64>,
}

/// Shared state across the concurrent client tasks.
struct Shared {
    rec: Mutex<Recorder>,
    /// Monotonic source of globally-unique appended values (the Elle uniqueness
    /// requirement — every appended element is distinct across the whole run).
    next_value: Mutex<u64>,
}

impl Shared {
    fn fresh_value(&self) -> u64 {
        let mut v = self.next_value.lock().unwrap();
        *v += 1;
        *v
    }
}

fn accord_ids(n: usize) -> Vec<u64> {
    ACCORD_IDS[..n].to_vec()
}

/// Encode a list value (`Vec<u64>`) as the stored bytes: each element as 8
/// big-endian bytes, concatenated. The empty list encodes to empty bytes.
fn encode_list(list: &[u64]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(list.len() * 8);
    for v in list {
        bytes.extend_from_slice(&v.to_be_bytes());
    }
    bytes
}

/// Decode stored bytes back into a list value (inverse of [`encode_list`]). A
/// length not a multiple of 8 (never produced by [`encode_list`]) decodes the
/// whole-8-byte prefix.
fn decode_list(bytes: &[u8]) -> Vec<u64> {
    bytes
        .chunks_exact(8)
        .map(|c| u64::from_be_bytes(c.try_into().unwrap()))
        .collect()
}

fn lossy(p: f64) -> NetConfig {
    let mut cfg = NetConfig::default();
    cfg.set_drop_prob(p);
    cfg
}

impl Cluster {
    /// Bring up an Accord replica set wired to a single-tablet data plane.
    pub fn start(seed: u64, shape: ClusterShape) -> Cluster {
        let sim = Simulator::new(seed);
        let a = shape.accord_replicas;
        let d = shape.data_replicas;
        assert!((3..=5).contains(&a) && (3..=5).contains(&d));

        // Data-plane replicas over the whole key space.
        for &id in &DATA_IDS[..d] {
            serve_replica(sim.env(id), MemoryEngine::new(), Epoch::INITIAL);
        }
        let tablet = Tablet::new(TabletId(1), KeyRange::whole(), DATA_IDS[..d].to_vec());
        let view = TabletView::from_tablet(&tablet, R, W);

        let all = accord_ids(a);
        let nodes: Vec<AccordNode<SimEnv>> = (0..a)
            .map(|i| {
                AccordNode::start_with_data_plane(
                    sim.env(ACCORD_IDS[i]),
                    all.clone(),
                    MemoryEngine::new(),
                    sim.env(COORD_IDS[i]),
                    view.clone(),
                )
            })
            .collect();

        let shared = Arc::new(Shared {
            rec: Mutex::new(Recorder::new(seed)),
            next_value: Mutex::new(0),
        });

        Cluster {
            sim,
            nodes,
            view,
            shape,
            shared,
            stopped: BTreeSet::new(),
            crashed: BTreeSet::new(),
        }
    }

    /// Spawn `clients` concurrent client coordinators, each running `rounds` of
    /// the workload. Each client coordinates through a *distinct* Accord replica
    /// (round-robin) so transactions originate from across the cluster.
    pub fn spawn_workload(&mut self, spec: WorkloadSpec) {
        for c in 0..spec.clients {
            let node = self.nodes[c % self.nodes.len()].clone();
            let shared = Arc::clone(&self.shared);
            let env = node.env().clone();
            let proc = c as Process;
            env.clone().spawn_task(async move {
                client_loop(node, shared, proc, spec).await;
            });
        }
    }

    /// Apply one nemesis action against the live cluster shape.
    fn apply(&mut self, action: NemesisAction) {
        let a = self.shape.accord_replicas;
        let ids: Vec<u64> = accord_ids(a);
        match action {
            NemesisAction::PartitionMinority => {
                // Minority = the highest-indexed floor((n-1)/2) replicas.
                let minority = (a - 1) / 2;
                let cut = a - minority;
                for &m in &ids[cut..] {
                    for &o in &ids[..cut] {
                        self.sim.partition_pair(m, o);
                    }
                }
            }
            NemesisAction::PartitionMajority => {
                // Split into two halves, neither a majority (true split brain):
                // left = ceil(n/2) cannot reach right = floor(n/2); but we also
                // cut the left into two so no side has > n/2 reachable. Simplest
                // robust split: isolate each replica into its own island for the
                // window (a full mesh partition) — consensus cannot make a quorum.
                for i in 0..a {
                    for j in (i + 1)..a {
                        self.sim.partition_pair(ids[i], ids[j]);
                    }
                }
            }
            NemesisAction::IsolateOne => {
                let victim = ids[a - 1];
                // Isolate from all Accord peers, all data replicas, and all
                // coordinators.
                for &o in &ids {
                    if o != victim {
                        self.sim.partition_pair(victim, o);
                    }
                }
                for &d in &DATA_IDS[..self.shape.data_replicas] {
                    self.sim.partition_pair(victim, d);
                }
                for &co in &COORD_IDS[..a] {
                    self.sim.partition_pair(victim, co);
                }
            }
            NemesisAction::Crash => {
                let victim = ids[a - 1];
                self.sim.crash(victim);
                self.crashed.insert(victim);
            }
            NemesisAction::StopRestart => {
                let victim = ids[a - 1];
                self.sim.stop(victim);
                // Start a fresh node on the same id; it recovers from its WAL.
                let fresh = AccordNode::start_with_data_plane(
                    self.sim.env(victim),
                    ids.clone(),
                    MemoryEngine::new(),
                    self.sim.env(COORD_IDS[a - 1]),
                    self.view.clone(),
                );
                self.nodes[a - 1] = fresh;
            }
            NemesisAction::LeaderKill => {
                // Accord is leaderless; the data plane's primary is the first
                // data replica (in every R=2 quorum that includes it). Crashing
                // it forces quorums onto the remaining replicas.
                let victim = DATA_IDS[0];
                self.sim.crash(victim);
                self.crashed.insert(victim);
            }
            NemesisAction::HealAll => {
                // Heal every partition among Accord/data/coordinator nodes.
                let mut all: Vec<u64> = ids.clone();
                all.extend_from_slice(&DATA_IDS[..self.shape.data_replicas]);
                all.extend_from_slice(&COORD_IDS[..a]);
                for i in 0..all.len() {
                    for j in (i + 1)..all.len() {
                        self.sim.heal(all[i], all[j]);
                    }
                }
                // Restart anything still crashed.
                let crashed: Vec<u64> = self.crashed.iter().copied().collect();
                for v in crashed {
                    self.sim.restart(v);
                }
                self.crashed.clear();
                self.sim.set_net_config(NetConfig::default());
            }
            NemesisAction::Lossy => {
                self.sim.set_net_config(lossy(0.1));
            }
        }
    }
}

/// One client coordinator's loop: in each round, run a read or a write
/// transaction, then wait (bounded) for it to execute and record the outcome.
///
/// **Single-writer-per-key (the LWW guard).** A write transaction only touches
/// keys this client *owns* (`owner(key) = key % clients`); a read may touch any
/// key. So no two clients ever write the same key (per-key LWW would otherwise
/// lose appends — a data-model artefact, not a consistency bug). Cross-transaction
/// conflict (the wr/rw/ww edges the cycle checker chews on) still comes from
/// multi-key transactions and from a read observing keys *other* clients wrote.
async fn client_loop(
    node: AccordNode<SimEnv>,
    shared: Arc<Shared>,
    proc: Process,
    spec: WorkloadSpec,
) {
    let env = node.env().clone();
    // This client's own view of the keys it owns (it is the *sole* writer of
    // those keys, single-writer-per-key). It builds each append on top of its own
    // last-written list rather than a begin-time quorum read — a begin-time read
    // can lag the previous write's data-plane propagation (the apply marks the txn
    // `Applied` before the fire-and-forget quorum write lands), which would make
    // the RMW read a stale base and *lose* its own earlier appends. Because the
    // client is the only writer and runs its rounds serially, this in-memory list
    // is exactly the authoritative state of the key.
    let mut my_lists: BTreeMap<Key, Vec<u64>> = BTreeMap::new();
    for round in 0..spec.rounds {
        // Deterministic key selection from the simulator RNG (seeded), so the
        // workload is a pure function of the seed.
        let is_read = env.gen_below(100) < spec.read_pct;
        if is_read {
            // A read may observe any key in the shared space.
            let keys = pick_keys(&env, spec.keyspace, spec.keys_per_txn);
            run_read(&node, &shared, proc, round, keys).await;
        } else {
            // A write only appends to keys this client owns (single-writer).
            let keys = pick_owned_keys(&env, spec, proc);
            run_write(&node, &shared, proc, round, keys, &mut my_lists).await;
        }
        // Small gap between this client's ops so others interleave.
        env.sleep(POLL).await;
    }
}

/// Pick `count` distinct keys from `0..keyspace` using the seeded simulator RNG.
fn pick_keys(env: &SimEnv, keyspace: u64, count: usize) -> BTreeSet<Key> {
    let mut keys = BTreeSet::new();
    let mut guard = 0;
    while keys.len() < count && guard < count * 8 {
        keys.insert(env.gen_below(keyspace));
        guard += 1;
    }
    // Ensure non-empty even in a 1-key space.
    if keys.is_empty() {
        keys.insert(0);
    }
    keys
}

/// The owner client of `key` under single-writer-per-key: `key % clients`.
fn owner(key: Key, clients: usize) -> Process {
    (key % clients as u64) as Process
}

/// Pick up to `keys_per_txn` distinct keys this `proc` *owns* (`owner(k) ==
/// proc`) from the shared key space, using the seeded RNG. Always returns ≥ 1
/// owned key (a client always owns at least its own residue class, present iff
/// `keyspace > proc`). Falls back to `proc` itself if the keyspace is too small
/// to contain an owned key by sampling.
fn pick_owned_keys(env: &SimEnv, spec: WorkloadSpec, proc: Process) -> BTreeSet<Key> {
    let owned: Vec<Key> = (0..spec.keyspace)
        .filter(|&k| owner(k, spec.clients) == proc)
        .collect();
    if owned.is_empty() {
        // No key in the space maps to this client; nothing to write this round.
        // (Should not happen for keyspace ≥ clients, which the corpus ensures.)
        return BTreeSet::new();
    }
    let mut keys = BTreeSet::new();
    let mut guard = 0;
    let want = spec.keys_per_txn.min(owned.len());
    while keys.len() < want && guard < want * 8 {
        let pick = owned[env.gen_below(owned.len() as u64) as usize];
        keys.insert(pick);
        guard += 1;
    }
    if keys.is_empty() {
        keys.insert(owned[0]);
    }
    keys
}

/// Run a **write** transaction over `keys` (all owned by `proc`) as a genuine
/// **list-append with real values** (ADR 0011 arbitrary write values; ADR 0014
/// true black-box list-append): for each owned key, append this transaction's
/// globally-unique value to the client's own authoritative list for that key and
/// write the **whole new list** back as the real stored value (via the
/// value-carrying [`AccordNode::submit_writes`]). Reads later observe exactly
/// these stored bytes, so the recovered order is genuinely from observed values.
///
/// `my_lists` is this client's own per-key list — it is the *sole* writer of its
/// owned keys and runs serially, so this is the authoritative state; building the
/// append on it (rather than a begin-time quorum read that can lag the previous
/// write's propagation) keeps appends from being lost. Record `invoke` then `ok`
/// (it applied) or `info` (indeterminate — never `fail`); each key as
/// `Append { k, value }`.
async fn run_write(
    node: &AccordNode<SimEnv>,
    shared: &Arc<Shared>,
    proc: Process,
    _round: u64,
    keys: BTreeSet<Key>,
    my_lists: &mut BTreeMap<Key, Vec<u64>>,
) {
    if keys.is_empty() {
        return; // this client owns no key in the space — nothing to append.
    }
    let env = node.env().clone();
    let value = shared.fresh_value();
    let mops: Vec<Mop> = keys
        .iter()
        .map(|&k| Mop::Append { key: k, value })
        .collect();
    shared
        .rec
        .lock()
        .unwrap()
        .invoke(proc, env.now().0, mops.clone());

    // Append our unique value to each owned key's authoritative list and write the
    // whole new list back as the real stored value.
    let mut writes: BTreeMap<Key, Vec<u8>> = BTreeMap::new();
    for &k in &keys {
        let list = my_lists.entry(k).or_default();
        list.push(value);
        writes.insert(k, encode_list(list));
    }
    let txn = node.submit_writes(writes);

    if wait_applied(node, txn).await {
        shared.rec.lock().unwrap().ok(proc, env.now().0, mops);
    } else {
        // Indeterminate: the transaction may yet commit later. Never `fail`.
        shared.rec.lock().unwrap().info(proc, env.now().0, mops);
    }
}

/// Submit a **read** transaction over `keys`, wait for it to execute, and record
/// the per-key **actually-observed list** — decoded from the bytes the read
/// transaction returns ([`AccordNode::read_value_result`]). This is genuine
/// black-box observation: the recovered order comes from these observed values
/// (Elle's `recover`), not from any out-of-band `applied_order` reconstruction.
async fn run_read(
    node: &AccordNode<SimEnv>,
    shared: &Arc<Shared>,
    proc: Process,
    _round: u64,
    keys: BTreeSet<Key>,
) {
    let env = node.env().clone();
    let invoke_mops: Vec<Mop> = keys
        .iter()
        .map(|&k| Mop::Read {
            key: k,
            observed: None,
        })
        .collect();
    shared
        .rec
        .lock()
        .unwrap()
        .invoke(proc, env.now().0, invoke_mops);

    let txn = node.submit_read(keys.clone());
    let observed = if wait_applied(node, txn).await {
        node.read_value_result(txn)
    } else {
        None
    };
    match observed {
        Some(result) => {
            // Decode each key's actually-stored list from the observed bytes.
            let mops: Vec<Mop> = keys
                .iter()
                .map(|&k| {
                    let list = result
                        .get(&k)
                        .and_then(|o| o.as_ref())
                        .map(|bytes| decode_list(bytes))
                        .unwrap_or_default();
                    Mop::Read {
                        key: k,
                        observed: Some(list),
                    }
                })
                .collect();
            shared.rec.lock().unwrap().ok(proc, env.now().0, mops);
        }
        None => {
            let info_mops: Vec<Mop> = keys
                .iter()
                .map(|&k| Mop::Read {
                    key: k,
                    observed: None,
                })
                .collect();
            shared
                .rec
                .lock()
                .unwrap()
                .info(proc, env.now().0, info_mops);
        }
    }
}

/// Poll `node.is_applied(txn)` on the simulator clock up to [`OP_BUDGET`],
/// yielding (`env.sleep`) between polls so other tasks run. Returns whether the
/// transaction executed within budget.
async fn wait_applied(node: &AccordNode<SimEnv>, txn: TxnId) -> bool {
    let env = node.env().clone();
    let deadline = env.now().0 + OP_BUDGET.as_nanos() as u64;
    loop {
        if node.is_applied(txn) {
            return true;
        }
        if env.now().0 >= deadline {
            return false;
        }
        env.sleep(POLL).await;
    }
}

// ---------------------------------------------------------------------------
// The scenario runner (deliverable 3) + the result the corpus asserts on.
// ---------------------------------------------------------------------------

/// The result of running a scenario: the recorded history, the three checker
/// reports, and coverage counters used to guard against a vacuous (all-`info`)
/// run.
pub struct ScenarioResult {
    pub history: History,
    pub cycles: animus_test::CheckReport,
    pub durability: animus_test::CheckReport,
    pub convergence: animus_test::CheckReport,
    /// Count of acknowledged (`ok`) write ops.
    pub ok_writes: usize,
    /// Count of acknowledged (`ok`) read ops that observed a non-empty list.
    pub nonempty_reads: usize,
    /// Whether the workload genuinely contended (≥ 2 ok writes to a shared key).
    pub contended: bool,
}

/// Run a scenario end to end: bring up the cluster, spawn the workload, apply the
/// fault schedule at the listed virtual times while the workload runs, heal,
/// quiesce, snapshot two final quorum reads, and run all three checkers.
pub fn run_scenario(scenario: &Scenario) -> ScenarioResult {
    let mut cluster = Cluster::start(scenario.seed, scenario.cluster);

    // Let the cluster settle, then start the concurrent workload.
    cluster.sim.run_for(Duration::from_millis(500));
    cluster.spawn_workload(scenario.workload);

    // Walk the fault schedule in virtual-time order, advancing the sim to each
    // fault's timestamp and applying it. (The schedule is authored sorted; we
    // sort defensively.)
    let mut faults = scenario.faults.clone();
    faults.sort_by_key(|(at, _)| *at);
    let base = cluster.sim.now().0;
    for (at, action) in faults {
        let target = base + at.as_nanos() as u64;
        if target > cluster.sim.now().0 {
            cluster.sim.run_until(animus_env::Nanos(target));
        }
        cluster.apply(action);
    }

    // Ensure the cluster ends healthy so the workload tail and final reads can
    // make a quorum (a scenario that ends partitioned would otherwise report a
    // spurious "lost write"/non-convergence that is really just unavailability).
    cluster.apply(NemesisAction::HealAll);

    // Drive long enough for in-flight transactions to drain and execute, plus the
    // workload to finish (clients run rounds * (op budget + poll) at most).
    cluster.sim.run_for(Duration::from_secs(40));

    // Final list-append state, read straight from each key's actually-stored
    // value on two *distinct* Accord replicas (genuine black-box final state, not
    // a reconstruction). Reading from two different replicas makes convergence a
    // real cross-replica agreement check, and durability ("every ok append is in
    // the final list") meaningful under single-writer-per-key.
    let keys: Vec<Key> = (0..scenario.workload.keyspace).collect();
    let final_a = list_state(&cluster, 0, &keys);
    let final_b = list_state(&cluster, cluster.nodes.len() - 1, &keys);

    let history = cluster.shared.rec.lock().unwrap().history().clone();
    let cycles = check_cycles(&history);
    let durability = check_durability(&history, &final_a);
    let convergence = check_convergence(scenario.seed, &final_a, &final_b);

    // Coverage counters.
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
    // Contention witness: some key has ≥ 2 acknowledged appends.
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
        history,
        cycles,
        durability,
        convergence,
        ok_writes,
        nonempty_reads,
        contended,
    }
}

/// The **final list-append state** read straight from Accord replica
/// `node_idx`'s **actually-stored** state: for each key, the list decoded from
/// the bytes currently winning at that key in the replica's executed store
/// (`store_value`). This is genuine black-box final state — the real list each
/// key holds, not a reconstruction from the consensus order.
///
/// Reading this from two *distinct* replicas keeps [`check_convergence`] a real
/// cross-replica agreement check (do both replicas' stored lists agree?), and
/// [`check_durability`] ("every acknowledged append is in the final list")
/// meaningful: single-writer-per-key means appends accumulate, so an `ok` append
/// must be present in its key's final list.
fn list_state(cluster: &Cluster, node_idx: usize, keys: &[Key]) -> BTreeMap<Key, Vec<u64>> {
    let node = &cluster.nodes[node_idx];
    let mut map: BTreeMap<Key, Vec<u64>> = BTreeMap::new();
    for &k in keys {
        let list = futures::executor::block_on(node.store_value(k))
            .map(|bytes| decode_list(&bytes))
            .unwrap_or_default();
        map.insert(k, list);
    }
    map
}
