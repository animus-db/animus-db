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
// the data-plane replicas, and a standalone verifier. We size for up to 7 Accord
// replicas + 7 data replicas; smaller clusters use a prefix. The id *bands*
// (0.., 10.., 20..) stay disjoint up to 7 each so a 7+7 shape never collides. ---

/// Accord consensus replica node ids.
const ACCORD_IDS: [u64; 7] = [0, 1, 2, 3, 4, 5, 6];
/// Per-Accord-node data-plane coordinator ids (distinct inbox per coordinator).
const COORD_IDS: [u64; 7] = [10, 11, 12, 13, 14, 15, 16];
/// Data-plane replica node ids.
const DATA_IDS: [u64; 7] = [20, 21, 22, 23, 24, 25, 26];
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
    /// A 7+7 cluster (extended tier): the largest quorums and partition surface.
    pub const HUGE: ClusterShape = ClusterShape {
        accord_replicas: 7,
        data_replicas: 7,
    };
    /// Asymmetric: a small consensus group over a wider data fan-out.
    pub const ACCORD_LIGHT: ClusterShape = ClusterShape {
        accord_replicas: 3,
        data_replicas: 5,
    };
    /// Asymmetric: a wider consensus group over a small data fan-out.
    pub const DATA_LIGHT: ClusterShape = ClusterShape {
        accord_replicas: 5,
        data_replicas: 3,
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
    /// Inject high-latency links (large base delay + jitter, no drops) for the
    /// rest of the run. Models a degraded-but-connected network: a coordinator is
    /// *slow*, not dead. This is the adversary a timeout-based failure detector
    /// must not mistake for a crash — recovering a live-but-slow coordinator
    /// re-orders its transaction and can silently lose later same-key writes (the
    /// failure-detector-bound hazard documented in the root CLAUDE.md). Healed by
    /// `HealAll` (which restores the default `NetConfig`).
    SlowLinks,
}

/// Which layer the cluster's reads observe — i.e. *what the checkers can soundly
/// assert*. This is the load-bearing distinction the repo principle demands:
/// serializability is a property of **Accord's order**, not of the AP data plane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Topology {
    /// **Pure Accord** (no data-plane sink): each replica executes the agreed
    /// order into its **local** store and a read is a versioned snapshot
    /// (`get_at(key, execute_at)`) — exactly the writes ordered before it, none
    /// after, identically on every replica. This is the serialization-authoritative
    /// layer and the **only sound target for the cycle (serializability) check**.
    /// Robust to faults: a fault delays/strands consensus but never makes a read
    /// observe a torn or stale order.
    Authoritative,
    /// **Accord wired to the replicated AP data-plane frontier**
    /// (`start_with_data_plane`): a committed write is pushed through the quorum
    /// (fire-and-forget) and a read is a *current quorum read*. Eventually
    /// consistent: under a data-replica fault an acked multi-key write can be
    /// transiently torn/stale at a read quorum (it converges via anti-entropy).
    /// So this layer is checked for **convergence + durability** — what the AP
    /// data plane offers — **never** serializability.
    Frontier,
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

/// A stable per-scenario seed from its name. FNV-1a over the bytes — deterministic
/// (no `std::hash` nondeterminism), so a name maps 1:1 to a seed and the same
/// scenario reproduces every run. Growing the corpus never perturbs an existing
/// scenario's seed because the seed depends only on that scenario's own name.
fn seed_for(name: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in name.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Read a `usize` env knob, falling back to `default` when unset/empty/unparsable.
/// Env access is the only nondeterminism here and it happens *before* any sim run,
/// purely to size the corpus — each scenario itself is still a pure function of
/// its seed.
fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(default)
}

/// Whether the *extended* dimension tier is enabled (`ANIMUS_CORPUS_FULL` set to a
/// non-empty, non-`0`/`false` value).
fn corpus_full_enabled() -> bool {
    match std::env::var("ANIMUS_CORPUS_FULL") {
        Ok(v) => {
            let v = v.trim();
            !v.is_empty() && v != "0" && !v.eq_ignore_ascii_case("false")
        }
        Err(_) => false,
    }
}

/// Number of seeds per structural cell (`ANIMUS_CORPUS_SEEDS`, default 1). Each
/// extra seed re-runs the same structural scenario down a *different* interleaving
/// — the depth lever that surfaces schedule-dependent bugs a single frozen seed
/// misses. Clamped to ≥ 1.
fn corpus_seeds_per_cell() -> usize {
    env_usize("ANIMUS_CORPUS_SEEDS", 1).max(1)
}

/// The **scenario corpus**, tiered by two env knobs so the default `cargo test`
/// stays fast while a nightly/deep run can scale coverage by orders of magnitude:
///
/// - `ANIMUS_CORPUS_SEEDS=K` (default 1) — *depth*: emit `K` seed variants of every
///   structural cell. Variant 0 keeps the cell's canonical (frozen) name+seed; the
///   rest are `…_sNN` variants seeded by their own name.
/// - `ANIMUS_CORPUS_FULL=1` (default off) — *breadth*: add the extended dimension
///   set (7-node + asymmetric shapes, the `SlowLinks` fault, extra timings/workloads,
///   richer multi-fault schedules), named `ext_…` so they never perturb base names.
///
/// With both at their defaults this returns exactly the frozen base set (every name
/// and seed byte-identical to the committed corpus), so the always-on suite is
/// unchanged. The structural guards in `corpus.rs` deliberately call
/// [`corpus_base`] (env-independent) so they stay stable and fast regardless.
pub fn corpus() -> Vec<Scenario> {
    let mut cells = corpus_base();
    if corpus_full_enabled() {
        cells.extend(corpus_extended());
    }
    seed_expand(cells, corpus_seeds_per_cell())
}

/// Expand each structural cell into `k` seed variants. Variant 0 is the cell itself
/// (canonical name + seed preserved, so a frozen regression seed never moves);
/// variants `1..k` get a `_sNN` name suffix and a fresh name-derived seed. With
/// `k == 1` this is the identity, so the default corpus is byte-identical to the
/// base set.
pub fn seed_expand(cells: Vec<Scenario>, k: usize) -> Vec<Scenario> {
    if k <= 1 {
        return cells;
    }
    let mut out = Vec::with_capacity(cells.len() * k);
    for cell in cells {
        for i in 0..k {
            if i == 0 {
                out.push(cell.clone());
            } else {
                let name = format!("{}_s{i:02}", cell.name);
                out.push(Scenario {
                    seed: seed_for(&name),
                    cluster: cell.cluster,
                    workload: cell.workload,
                    faults: cell.faults.clone(),
                    name,
                });
            }
        }
    }
    out
}

/// Build the **frozen base scenario corpus**: a deterministic, structured
/// cross-product over { fault type × timing × workload shape × cluster shape },
/// plus no-fault and lossy/compound baselines. Each scenario is named (so a failure
/// is attributable) and seeded by a stable hash of its name. This is the canonical,
/// always-on set; [`corpus`] layers seed-depth and the extended tier on top.
///
/// This is the explicit "regenerate" step: editing this function changes the
/// corpus. A scenario that ever catches a bug stays here forever as a regression.
pub fn corpus_base() -> Vec<Scenario> {
    let mut out = Vec::new();
    let mut idx: u64 = 0;

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

/// Push a named scenario, deriving its seed from its name (so the name alone fixes
/// the run). Used by the extended generator to keep each cell a one-liner.
fn push_named(
    out: &mut Vec<Scenario>,
    name: String,
    cluster: ClusterShape,
    workload: WorkloadSpec,
    faults: Vec<(Duration, NemesisAction)>,
) {
    out.push(Scenario {
        seed: seed_for(&name),
        cluster,
        workload,
        faults,
        name,
    });
}

/// Extended workload regimes the base set lacks (only used in the FULL tier).
fn ext_workloads() -> [(&'static str, WorkloadSpec); 3] {
    [
        // Write-only: no reads → pure ww-ordering stress at maximum append load.
        (
            "write_only",
            WorkloadSpec {
                clients: 4,
                rounds: 6,
                keyspace: 4,
                keys_per_txn: 2,
                read_pct: 0,
            },
        ),
        // Big transactions: wide write sets (more keys per txn) → more overlap and
        // larger conflict graphs for the checker.
        (
            "big_txn",
            WorkloadSpec {
                clients: 4,
                rounds: 5,
                keyspace: 8,
                keys_per_txn: 4,
                read_pct: 30,
            },
        ),
        // Low-contention control: a large key space → few conflicts. Should always
        // pass; a failure here points at the harness, not the system.
        (
            "low_contention",
            WorkloadSpec {
                clients: 3,
                rounds: 5,
                keyspace: 16,
                keys_per_txn: 2,
                read_pct: 50,
            },
        ),
    ]
}

/// Build the **extended dimension tier** (only included when `ANIMUS_CORPUS_FULL`
/// is set). Everything here is named `ext_…` so it can never collide with or
/// perturb a base-corpus name/seed. It widens the matrix along axes the base set
/// fixes: the `SlowLinks` fault, 7-node + asymmetric cluster shapes, extra fault
/// timings, extra workload regimes, and richer multi-fault schedules.
pub fn corpus_extended() -> Vec<Scenario> {
    let mut out = Vec::new();
    let ws = corpus_workloads();
    let (_, tight) = ws[0];
    let (_, mid) = CORPUS_TIMINGS[1];

    // (1) SlowLinks — the degraded-but-connected fault (a coordinator looks *slow*,
    // not dead) — across all timings, both base shapes, and the tight + read-heavy
    // regimes most likely to trip a failure detector's slow-vs-dead bound.
    for (sname, shape) in [("c33", ClusterShape::SMALL), ("c55", ClusterShape::LARGE)] {
        for (wname, w) in [ws[0], ws[2]] {
            for (tname, at) in CORPUS_TIMINGS {
                push_named(
                    &mut out,
                    format!("ext_slow_{sname}_{wname}_{tname}"),
                    shape,
                    w,
                    vec![(at, NemesisAction::SlowLinks)],
                );
            }
        }
    }

    // (2) Extended cluster shapes: the base single-fault matrix at the steady-state
    // 'mid' timing on the 7+7 and the two asymmetric shapes, tight + wide_write.
    let ext_shapes = [
        ("c77", ClusterShape::HUGE),
        ("a35", ClusterShape::ACCORD_LIGHT),
        ("d53", ClusterShape::DATA_LIGHT),
    ];
    for (sname, shape) in ext_shapes {
        for (wname, w) in [ws[0], ws[1]] {
            for (fname, fault) in CORPUS_FAULTS {
                push_named(
                    &mut out,
                    format!("ext_{sname}_{wname}_{fname}_mid"),
                    shape,
                    w,
                    vec![(mid, fault)],
                );
            }
        }
    }

    // (3) Extra fault timings: very early (a fault landing during PreAccept) and at
    // wind-down (during execution/drain), across the base faults, small + tight.
    let ext_timings = [
        ("vearly", Duration::from_millis(300)),
        ("winddown", Duration::from_millis(5500)),
    ];
    for (tname, at) in ext_timings {
        for (fname, fault) in CORPUS_FAULTS {
            push_named(
                &mut out,
                format!("ext_t_{fname}_{tname}"),
                ClusterShape::SMALL,
                tight,
                vec![(at, fault)],
            );
        }
    }

    // (4) Extended workloads: a no-fault baseline + a crash-mid run for each.
    for (wname, w) in ext_workloads() {
        push_named(
            &mut out,
            format!("ext_baseline_{wname}"),
            ClusterShape::SMALL,
            w,
            Vec::new(),
        );
        push_named(
            &mut out,
            format!("ext_{wname}_crash_mid"),
            ClusterShape::SMALL,
            w,
            vec![(mid, NemesisAction::Crash)],
        );
    }

    // (5) Richer multi-fault schedules on the large + huge shapes: a three-fault
    // stack (slow → partition → crash) and a partition→heal→repartition flap.
    for (sname, shape) in [("c55", ClusterShape::LARGE), ("c77", ClusterShape::HUGE)] {
        push_named(
            &mut out,
            format!("ext_triple_{sname}"),
            shape,
            tight,
            vec![
                (Duration::from_millis(800), NemesisAction::SlowLinks),
                (
                    Duration::from_millis(2000),
                    NemesisAction::PartitionMinority,
                ),
                (Duration::from_millis(3500), NemesisAction::Crash),
            ],
        );
        push_named(
            &mut out,
            format!("ext_flap_{sname}"),
            shape,
            tight,
            vec![
                (
                    Duration::from_millis(1000),
                    NemesisAction::PartitionMinority,
                ),
                (Duration::from_millis(2200), NemesisAction::HealAll),
                (
                    Duration::from_millis(3000),
                    NemesisAction::PartitionMinority,
                ),
            ],
        );
    }

    out
}

// ---------------------------------------------------------------------------
// The running cluster.
// ---------------------------------------------------------------------------

/// A live Accord cluster plus the shared workload recorder. The cluster is either
/// pure-Accord ([`Topology::Authoritative`]) or wired to the AP data-plane
/// frontier ([`Topology::Frontier`]); the [`Cluster::topology`] field records
/// which, so faults that target the data plane behave correctly in each.
pub struct Cluster {
    sim: Simulator,
    nodes: Vec<AccordNode<SimEnv>>,
    view: TabletView,
    shape: ClusterShape,
    topology: Topology,
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

/// A degraded-but-connected network: large base delay + jitter, no drops. The
/// numbers are well above a healthy heartbeat interval so a coordinator looks
/// *slow* (not dead) to a peer's failure detector.
fn slow_links() -> NetConfig {
    let mut cfg = NetConfig::default();
    cfg.base_delay = Duration::from_millis(400);
    cfg.max_jitter = Duration::from_millis(400);
    cfg
}

/// Construct Accord replica `i` in the given topology: pure-Accord
/// ([`AccordNode::start`] — local execution + snapshot reads, the serialization
/// authority) or wired to the data-plane frontier ([`AccordNode::start_with_data_plane`]).
fn make_node(
    sim: &Simulator,
    all: &[u64],
    i: usize,
    topology: Topology,
    view: &TabletView,
) -> AccordNode<SimEnv> {
    match topology {
        Topology::Authoritative => AccordNode::start(sim.env(ACCORD_IDS[i]), all.to_vec()),
        Topology::Frontier => AccordNode::start_with_data_plane(
            sim.env(ACCORD_IDS[i]),
            all.to_vec(),
            MemoryEngine::new(),
            sim.env(COORD_IDS[i]),
            view.clone(),
        ),
    }
}

impl Cluster {
    /// Bring up an Accord replica set in the given [`Topology`]. In both modes the
    /// data-plane replicas + `view` are created (so partition/crash/heal nemeses
    /// behave identically); the modes differ only in how the Accord nodes are
    /// wired — pure (local execution + snapshot reads) vs. frontier (data-plane
    /// quorum writes/reads).
    pub fn start(seed: u64, shape: ClusterShape, topology: Topology) -> Cluster {
        let sim = Simulator::new(seed);
        let a = shape.accord_replicas;
        let d = shape.data_replicas;
        assert!((3..=7).contains(&a) && (3..=7).contains(&d));

        // Data-plane replicas over the whole key space.
        for &id in &DATA_IDS[..d] {
            serve_replica(sim.env(id), MemoryEngine::new(), Epoch::INITIAL);
        }
        let tablet = Tablet::new(TabletId(1), KeyRange::whole(), DATA_IDS[..d].to_vec());
        let view = TabletView::from_tablet(&tablet, R, W);

        let all = accord_ids(a);
        let nodes: Vec<AccordNode<SimEnv>> = (0..a)
            .map(|i| make_node(&sim, &all, i, topology, &view))
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
            topology,
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
                // Start a fresh node on the same id (recovers from its WAL), in the
                // same topology as the rest of the cluster.
                let fresh = make_node(&self.sim, &ids, a - 1, self.topology, &self.view);
                self.nodes[a - 1] = fresh;
            }
            NemesisAction::LeaderKill => {
                // In Frontier mode, crash the first *data* replica (the one in every
                // R=2 quorum) to force quorums onto the rest. In Authoritative mode
                // the data plane is idle, so target the first *Accord* replica
                // instead (distinct from `Crash`, which downs the last one) — losing
                // a hot consensus node.
                let victim = match self.topology {
                    Topology::Frontier => DATA_IDS[0],
                    Topology::Authoritative => ids[0],
                };
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
            NemesisAction::SlowLinks => {
                self.sim.set_net_config(slow_links());
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

/// Run a scenario against the **serialization-authoritative** topology (pure
/// Accord). This is the sound target for the cycle (serializability) check; all
/// three checkers are meaningful and robust to faults. The corpus uses this.
pub fn run_scenario(scenario: &Scenario) -> ScenarioResult {
    run_scenario_with(scenario, Topology::Authoritative)
}

/// Run a scenario end to end in the given [`Topology`]: bring up the cluster,
/// spawn the workload, apply the fault schedule at the listed virtual times while
/// the workload runs, heal, quiesce, snapshot two final reads, and run all three
/// checkers. In [`Topology::Frontier`] the `cycles` report is **not** sound to
/// assert (the AP read path is only eventually consistent under faults) — assert
/// `convergence` + `durability` there, which is what the data plane offers.
pub fn run_scenario_with(scenario: &Scenario, topology: Topology) -> ScenarioResult {
    let mut cluster = Cluster::start(scenario.seed, scenario.cluster, topology);

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
