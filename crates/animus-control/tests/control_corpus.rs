//! A fault-injected, seed-reproducible **corpus for the control plane's own
//! machinery** — the ADR 0038 async apply task, the replicated schema
//! catalog's exclusivity guarantee (ADR 0013), and (PR②/③ to come) tablet-id
//! allocation and apply-task crash recovery.
//!
//! `learner_corpus.rs` already covers the learner/membership-class fault
//! vocabulary at seed depth (ADR 0058 Train 1); this corpus is the sibling
//! that exercises everything else about `Metadata`/`RaftNode` under real
//! fault injection instead of the ~30 fixed-single-seed acceptance tests
//! this crate otherwise has. **This is PR① of a 3-4 PR stacked series: the
//! harness architecture + a baseline + a schema-catalog-race workload.**
//! PR②/③ extend the fault vocabulary (learner faults, `StopRestart`) and add
//! two more workloads (allocator-race, apply-task-recovery) — see the
//! "Future invariants" section near the bottom for the hooks left for them.
//!
//! **Harness shape**, deliberately mirroring
//! `crates/animus-test/tests/raftkv_linearizable.rs` (the flagship corpus in
//! this repo — read it first if this file is unfamiliar): a declarative
//! [`Scenario`] (name + seed + replica count + [`Workload`] + a scheduled
//! [`Nemesis`] list + an optional outage `window`), a [`Group`] that owns the
//! live `RaftNode` set and knows how to `apply`/`heal_all` each nemesis, and
//! a `run_scenario`/`assert_scenario_ok` pair the tests drive. **One
//! adaptation**: unlike raftkv's single generic client loop (every scenario
//! drives the identical single-key list-append workload), this plane's
//! interesting scenarios need genuinely different workload *shapes*
//! (concurrent schema proposers vs. plain no-contention churn) — so
//! `Scenario` carries a `workload: Workload` field selecting which
//! `spawn_*_workload` function the runner drives, the same pattern
//! `crates/animus-test/tests/txn_serializable.rs`'s own `Workload` struct
//! uses for its several read/write/rmw shapes.
//!
//! **What "correctness" means here — and why there's no `check_cycles`.**
//! Unlike the per-tablet KV plane (`animus-cp-data`, `raftkv_linearizable.rs`),
//! this plane has no client-visible read/write history to build an Elle
//! dependency graph over: a single Raft log total-orders every `MetaCommand`,
//! so the interesting property is **convergence + safety invariants**, not
//! serializability. Three checks, asserted on every scenario
//! (`assert_scenario_ok`):
//!
//! 1. **Convergence** — `nodes[i].metadata() == nodes[j].metadata()` for
//!    every pair of replicas, via a converged-or-timeout poll (mirroring
//!    raftkv's `CONVERGENCE_POLL_STEP`/`CONVERGENCE_BUDGET` exactly).
//! 2. **Durability** — an effect a proposer's own retry loop actually
//!    *confirmed* (read back, byte-identical, after proposing — never merely
//!    `ProposeResult::Accepted`, which only means "appended to the leader's
//!    log," see `ProposeResult`'s own doc) must still be present in the
//!    final converged state. Mirrors raftkv's ok/info confirm discipline,
//!    minus the `info`-recording machinery this plane doesn't need (a
//!    proposer that never confirms simply contributes nothing to this
//!    check, rather than needing an explicit indeterminate outcome
//!    recorded).
//! 3. **Schema-catalog exclusivity** (a *safety* property, checked
//!    unconditionally on every scenario, fault or not) — for every table
//!    name two or more racers proposed, `MetaCommand::CreateTableSchema`'s
//!    apply-time semantics (`meta.rs`: rejects outright if a schema for the
//!    name already exists — **not** idempotent-on-identical the way
//!    `RegisterNode`'s CAS is; first-committer-wins, full stop) mean at most
//!    one racing schema can ever take effect. So on every replica: the
//!    table's final schema (if present) is byte-identical to exactly one of
//!    the racing proposals — never a hybrid — and it is never absent if any
//!    racer's proposal was ever durably confirmed by that racer's own retry
//!    loop.
//!
//! **Nemesis set for this PR** (a subset of the eventual vocabulary — PR②
//! adds learner-class faults, PR③ adds `StopRestart`): `LeaderKill`
//! (`sim.crash` the current leader), `FollowerKill` (`sim.crash` a
//! non-leader), `PartitionLeader` (isolate the leader from the rest),
//! `SplitBrain` (full-mesh partition, no majority anywhere), `Lossy`
//! (`NetConfig::set_drop_prob`). **`StopRestart` is deliberately NOT here**
//! — it needs `RaftNode::start` to reopen the *same* `StorageEngine` handle
//! the crashed node used (this plane always needs one, unlike raftkv's
//! simpler always-`MemoryEngine`-is-fine shape when the tier itself is
//! `MemoryEngine`) plus the apply-task-recovery invariant (#5 below) it
//! exists to probe — both deferred to PR③.
//!
//! **Workloads for this PR**: [`Workload::SchemaRace`] — 2-3 concurrent
//! proposers each racing `MetaCommand::CreateTableSchema`, either for the
//! SAME table name with distinct schemas (`same_table: true`, the exclusivity
//! teeth) or for distinct names each (`same_table: false`, a
//! lower-contention baseline where every racer should win its own name); and
//! [`Workload::PlainChurn`] — trivial no-contention `UpsertMember` proposals,
//! the non-vacuity floor every corpus in this repo needs (mirroring
//! `control_raft.rs`'s own baseline style). Every proposer retries against
//! whichever node currently reports itself leader, exactly the
//! `propose`/`NotLeader`-hint retry idiom `register_node_cas.rs` already
//! uses in this crate.
//!
//! **Depth knob**: `ANIMUS_CONTROL_SEEDS` (default 1 = the frozen cells,
//! byte-identical run-to-run), wired via `animus_test::corpus`'s
//! `seeds_from_env`/`seed_expand` exactly like every other corpus in this
//! repo.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::{ColumnType, MetaCommand, Metadata, NodeStatus, RaftNode, TableSchema};
use animus_env::{Clock, EnvExt, NodeId, nid};
use animus_sim::{NetConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_test::corpus::{self, SeedVariant};

/// A control-group node under `SimEnv`.
type Node = RaftNode<SimEnv>;
/// The live replica set. Interior mutability so a fault-injecting `Group`
/// method and concurrently-running client tasks can both hold a handle;
/// clients clone the `Arc` out and never hold the lock across an `.await`
/// (mirroring `raftkv_linearizable.rs`'s identical `Nodes` shape — this PR
/// never *replaces* a slot the way that corpus's `StopRestart` does, but
/// keeping the same shape now is what makes PR③'s `StopRestart` a
/// non-invasive addition later).
type Nodes = Arc<Mutex<Vec<Arc<Node>>>>;

/// Control-group replica node ids. A scenario uses a prefix (3 or 5
/// replicas), exactly `raftkv_linearizable.rs`'s convention.
const GROUP_IDS: [u64; 5] = [0, 1, 2, 3, 4];
/// Per-proposer driver env ids — disjoint from the group, **never faulted**,
/// so a proposer task always makes progress (it routes its own proposals to
/// whichever group node currently leads, tolerating crashes/partitions of
/// the group itself).
const CLIENT_IDS: [u64; 5] = [100, 101, 102, 103, 104];

/// How long a single proposer keeps retrying before giving up on ever
/// confirming its own effect. Generous, mirroring raftkv's `OP_BUDGET`
/// reasoning: a proposal racing a `SplitBrain`/`PartitionLeader` fault must
/// be allowed to ride out an election plus catch-up without being
/// misclassified as permanently lost.
const OP_BUDGET: Duration = Duration::from_secs(15);
/// Poll granularity while a proposer waits for its own effect to land.
const POLL: Duration = Duration::from_millis(100);
/// Settle time before the workload starts (let the group elect a leader).
const SETTLE: Duration = Duration::from_millis(800);
/// Post-heal drain: run the workload tail to completion (every proposer's own
/// `OP_BUDGET` clock keeps ticking through this) before snapshotting final
/// state for the checks.
const DRAIN: Duration = Duration::from_secs(25);
/// Converged-or-timeout poll step + budget for cross-replica agreement —
/// mirrors raftkv's identical constants and poll-loop shape.
const CONVERGENCE_POLL_STEP: Duration = Duration::from_secs(2);
const CONVERGENCE_BUDGET: Duration = Duration::from_secs(120);
/// Rounds a `PlainChurn` proposer runs — small, since the point is a
/// non-vacuity floor, not a stress test.
const CHURN_ROUNDS: u64 = 3;

// ---------------------------------------------------------------------------
// Declarative scenario model.
// ---------------------------------------------------------------------------

/// A fault the nemesis injects at a scheduled virtual time, resolved against
/// the live group at run time. Subset of the eventual vocabulary — see this
/// file's top doc for what's deferred to PR②/③ and why.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Nemesis {
    /// Crash the current leader. Survivors must elect a new leader and keep
    /// serving; every confirmed effect must survive. Restarted by
    /// `heal_all`.
    LeaderKill,
    /// Crash a follower (a non-leader replica). Quorum is unaffected; the
    /// crashed node must catch up after restart.
    FollowerKill,
    /// Partition the current leader away from every other replica: the
    /// majority elects a fresh leader, the isolated old leader can neither
    /// commit nor confirm a proposal. Healed by `heal_all`.
    PartitionLeader,
    /// Partition every replica from every other (full-mesh islands): **no**
    /// side has a majority, so commits stall entirely until heal.
    SplitBrain,
    /// Inject lossy links (independent per-message drop) for the rest of the
    /// run.
    Lossy,
}

/// Which workload shape a scenario drives — this plane's own reason for a
/// `Workload` field (see this file's top doc): its interesting scenarios
/// need genuinely different client behavior, not just different parameters
/// of one shared loop.
#[derive(Clone, Debug)]
enum Workload {
    /// `proposers` concurrent racers, each attempting
    /// `MetaCommand::CreateTableSchema`. `same_table: true` races them all
    /// against the identical table name with distinct schemas (the
    /// exclusivity teeth); `same_table: false` gives each racer its own
    /// name (a lower-contention baseline — every racer should win).
    SchemaRace { proposers: usize, same_table: bool },
    /// `proposers` concurrent, non-contending `UpsertMember` proposers — the
    /// non-vacuity floor.
    PlainChurn { proposers: usize },
}

/// A seed-reproducible scenario: a named group size + workload + an explicit
/// fault schedule (virtual time → nemesis) + an optional outage window.
/// `heal_all` is always applied by the runner at the end, so it is never
/// scheduled here (mirrors raftkv's `Scenario` exactly, minus the engine-tier
/// concern that harness carries and this one doesn't need yet).
#[derive(Clone, Debug)]
struct Scenario {
    name: String,
    seed: u64,
    replicas: usize,
    workload: Workload,
    faults: Vec<(Duration, Nemesis)>,
    /// How long the runner keeps the *last* fault open before healing. Every
    /// cell in this PR uses `ZERO` (no deepened tier yet — see the top doc);
    /// carried as a field now so PR②/③ can add windowed cells without
    /// reshaping `Scenario`.
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

/// Fault timing relative to the workload's life: early / mid / late —
/// identical convention to raftkv's `CORPUS_TIMINGS`.
const CORPUS_TIMINGS: [(&str, Duration); 3] = [
    ("early", Duration::from_millis(700)),
    ("mid", Duration::from_millis(2200)),
    ("late", Duration::from_millis(3800)),
];

fn schema_race_scenario(
    name: &str,
    replicas: usize,
    proposers: usize,
    same_table: bool,
    faults: Vec<(Duration, Nemesis)>,
) -> Scenario {
    Scenario {
        seed: corpus::name_seed(name),
        name: name.to_string(),
        replicas,
        workload: Workload::SchemaRace {
            proposers,
            same_table,
        },
        faults,
        window: Duration::ZERO,
    }
}

fn plain_churn_scenario(
    name: &str,
    replicas: usize,
    proposers: usize,
    faults: Vec<(Duration, Nemesis)>,
) -> Scenario {
    Scenario {
        seed: corpus::name_seed(name),
        name: name.to_string(),
        replicas,
        workload: Workload::PlainChurn { proposers },
        faults,
        window: Duration::ZERO,
    }
}

/// The structural cells of this PR's corpus. Every `Nemesis` variant and
/// every `Workload` variant appears at least once (checked by
/// `control_corpus_covers_the_fault_matrix`, below) — `FollowerKill`/`Lossy`
/// get a single spot-check cell each here; PR② is expected to deepen those
/// into their own early/mid/late/5-replica grids the way raftkv's corpus
/// does for its own fault set.
fn corpus_cells() -> Vec<Scenario> {
    let mut out = Vec::new();

    // --- Non-vacuity floor: no-fault PlainChurn baselines, both shapes. ---
    out.push(plain_churn_scenario("baseline_3", 3, 3, vec![]));
    out.push(plain_churn_scenario("baseline_5", 5, 3, vec![]));

    // --- Fault-free schema race: exclusivity must hold even absent any
    //     fault — not previously proven at any seed depth. ---
    out.push(schema_race_scenario(
        "schema_race_baseline_3",
        3,
        2,
        true,
        vec![],
    ));

    // --- LeaderKill x early/mid/late, same-table race, 3 replicas. ---
    for (tname, at) in CORPUS_TIMINGS {
        let name = format!("schema_race_leader_kill_{tname}_3");
        out.push(schema_race_scenario(
            &name,
            3,
            2,
            true,
            vec![(at, Nemesis::LeaderKill)],
        ));
    }

    // --- PartitionLeader mid-race. ---
    out.push(schema_race_scenario(
        "schema_race_partition_mid_3",
        3,
        2,
        true,
        vec![(Duration::from_millis(2200), Nemesis::PartitionLeader)],
    ));

    // --- FollowerKill / Lossy spot-checks, so the coverage guard has a real
    //     scenario for each of this PR's remaining nemesis variants. ---
    out.push(schema_race_scenario(
        "schema_race_follower_kill_mid_3",
        3,
        2,
        true,
        vec![(Duration::from_millis(2200), Nemesis::FollowerKill)],
    ));
    out.push(schema_race_scenario(
        "schema_race_lossy_mid_3",
        3,
        2,
        true,
        vec![(Duration::from_millis(2200), Nemesis::Lossy)],
    ));

    // --- Distinct-name race under a full split brain, 5 replicas. ---
    out.push(schema_race_scenario(
        "schema_race_distinct_names_split_brain_5",
        5,
        3,
        false,
        vec![(Duration::from_millis(2200), Nemesis::SplitBrain)],
    ));

    out
}

/// Seeds per structural cell (`ANIMUS_CONTROL_SEEDS`, default 1) — `K=1` is
/// byte-identical to the committed frozen set.
fn seeds_per_cell() -> usize {
    corpus::seeds_from_env("ANIMUS_CONTROL_SEEDS")
}

/// The corpus the headline test runs: the frozen cells, seed-expanded by the
/// depth knob.
fn corpus() -> Vec<Scenario> {
    corpus::seed_expand(corpus_cells(), seeds_per_cell())
}

fn lossy(p: f64) -> NetConfig {
    let mut cfg = NetConfig::default();
    cfg.set_drop_prob(p);
    cfg
}

// ---------------------------------------------------------------------------
// The running group.
// ---------------------------------------------------------------------------

/// Everything a proposer's confirm loop reports back, and everything the
/// exclusivity check needs about the full field of racing attempts.
struct Shared {
    /// Every `CreateTableSchema` attempt any racer ever made, recorded once
    /// at attempt time regardless of outcome — invariant #3 (exclusivity) is
    /// checked against this full candidate set, not just what one
    /// proposer's own confirm loop happened to observe win.
    schema_attempts: Mutex<Vec<(String, TableSchema)>>,
    /// Attempts a proposer's own confirm loop actually observed committed:
    /// its own proposed schema, byte-identical, durably visible on a read
    /// after proposing. Invariant #2 (durability) requires every one of
    /// these to still be present in the final converged state.
    confirmed_schemas: Mutex<Vec<(String, TableSchema)>>,
    /// `PlainChurn`: member ids a proposer's own confirm loop saw land — the
    /// same durability obligation, over `Metadata::members` instead of
    /// `Metadata::schemas`.
    confirmed_members: Mutex<BTreeSet<NodeId>>,
}

impl Shared {
    fn new() -> Self {
        Shared {
            schema_attempts: Mutex::new(Vec::new()),
            confirmed_schemas: Mutex::new(Vec::new()),
            confirmed_members: Mutex::new(BTreeSet::new()),
        }
    }

    fn record_schema_attempt(&self, table: &str, schema: &TableSchema) {
        self.schema_attempts
            .lock()
            .unwrap()
            .push((table.to_string(), schema.clone()));
    }

    fn confirm_schema(&self, table: &str, schema: &TableSchema) {
        self.confirmed_schemas
            .lock()
            .unwrap()
            .push((table.to_string(), schema.clone()));
    }

    fn confirm_member(&self, node: NodeId) {
        self.confirmed_members.lock().unwrap().insert(node);
    }

    fn confirmed_count(&self) -> usize {
        self.confirmed_schemas.lock().unwrap().len() + self.confirmed_members.lock().unwrap().len()
    }
}

/// Index + handle of a group node that currently believes it leads (lowest
/// index if more than one — possible transiently under a partition). `None`
/// if none does. Clones the `Arc` out so no lock is held across an `.await`.
fn leader_slot(nodes: &Nodes) -> Option<(usize, Arc<Node>)> {
    let guard = nodes.lock().unwrap();
    guard
        .iter()
        .position(|n| n.is_leader())
        .map(|i| (i, Arc::clone(&guard[i])))
}

struct Group {
    sim: Simulator,
    nodes: Nodes,
    replicas: usize,
    shared: Arc<Shared>,
    /// Group ids crashed and not yet restarted.
    crashed: BTreeSet<u64>,
}

impl Group {
    fn start(seed: u64, replicas: usize) -> Group {
        assert!((3..=5).contains(&replicas));
        let sim = Simulator::new(seed);
        let ids: Vec<u64> = GROUP_IDS[..replicas].to_vec();
        let nodes: Vec<Arc<Node>> = ids
            .iter()
            .map(|&id| {
                Arc::new(RaftNode::start(
                    sim.env(nid(id)),
                    ids.iter().copied().map(nid).collect(),
                    MemoryEngine::new(),
                ))
            })
            .collect();
        Group {
            sim,
            nodes: Arc::new(Mutex::new(nodes)),
            replicas,
            shared: Arc::new(Shared::new()),
            crashed: BTreeSet::new(),
        }
    }

    fn spawn_workload(&mut self, workload: &Workload) {
        match *workload {
            Workload::SchemaRace {
                proposers,
                same_table,
            } => self.spawn_schema_race_workload(proposers, same_table),
            Workload::PlainChurn { proposers } => self.spawn_plain_churn_workload(proposers),
        }
    }

    /// `proposers` concurrent racers, each on its own never-faulted driver
    /// env (mirroring raftkv's `CLIENT_IDS` discipline), each racing
    /// `MetaCommand::CreateTableSchema` for either the SAME table name (with
    /// distinct schemas — a different `partition_key` name per proposer, so
    /// two racers' proposals are always structurally distinguishable) or
    /// distinct names each.
    fn spawn_schema_race_workload(&mut self, proposers: usize, same_table: bool) {
        for (p, &client_id) in CLIENT_IDS.iter().enumerate().take(proposers) {
            let env = self.sim.env(nid(client_id));
            let nodes = Arc::clone(&self.nodes);
            let shared = Arc::clone(&self.shared);
            let table = if same_table {
                "ks.race".to_string()
            } else {
                format!("ks.race_{p}")
            };
            let schema = TableSchema::simple(format!("pk_{p}"), ColumnType::String);
            env.clone().spawn_task(async move {
                schema_race_client(env, nodes, shared, table, schema).await;
            });
        }
    }

    /// `proposers` concurrent, non-contending `UpsertMember` proposers —
    /// each proposes `CHURN_ROUNDS` distinct member ids of its own (never
    /// colliding with another proposer's), confirming each before moving on.
    fn spawn_plain_churn_workload(&mut self, proposers: usize) {
        for (p, &client_id) in CLIENT_IDS.iter().enumerate().take(proposers) {
            let env = self.sim.env(nid(client_id));
            let nodes = Arc::clone(&self.nodes);
            let shared = Arc::clone(&self.shared);
            let base = 900 + (p as u64) * 10;
            env.clone().spawn_task(async move {
                plain_churn_client(env, nodes, shared, base).await;
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
            Nemesis::SplitBrain => {
                for i in 0..self.replicas {
                    for j in (i + 1)..self.replicas {
                        self.sim.partition_pair(nid(ids[i]), nid(ids[j]));
                    }
                }
            }
            Nemesis::Lossy => {
                self.sim.set_net_config(lossy(0.1));
            }
        }
    }

    /// Heal every partition, restart every crashed node, restore default
    /// links.
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

/// One schema-race proposer: repeatedly (re)proposes its own `(table,
/// schema)` pair against whichever node currently leads, until either its
/// own schema is durably visible (it won) or a DIFFERENT schema is already
/// durably visible for `table` (it lost — `CreateTableSchema` rejects
/// outright on an existing name, so there is nothing left to retry). Never
/// asserts anything itself: outcomes feed `Shared`, checked once by the
/// runner after the whole scenario settles (the same "indeterminate outcomes
/// are data, not an in-task assertion" discipline raftkv's `run_write`/
/// `run_read` follow).
async fn schema_race_client(
    env: SimEnv,
    nodes: Nodes,
    shared: Arc<Shared>,
    table: String,
    schema: TableSchema,
) {
    shared.record_schema_attempt(&table, &schema);
    let deadline = env.now().0 + OP_BUDGET.as_nanos() as u64;
    while env.now().0 < deadline {
        if let Some((_, node)) = leader_slot(&nodes) {
            node.propose(MetaCommand::CreateTableSchema {
                table: table.clone(),
                schema: schema.clone(),
            });
        }
        env.sleep(POLL).await;
        if let Some((_, node)) = leader_slot(&nodes) {
            let meta = node.metadata();
            match meta.table_schema(&table) {
                Some(existing) if *existing == schema => {
                    shared.confirm_schema(&table, &schema);
                    return;
                }
                Some(_) => return, // lost the race: a different schema already won
                None => {}
            }
        }
    }
}

/// One `PlainChurn` proposer: `CHURN_ROUNDS` distinct `UpsertMember`
/// proposals, each retried against the current leader and confirmed via a
/// subsequent read before moving to the next.
async fn plain_churn_client(env: SimEnv, nodes: Nodes, shared: Arc<Shared>, base: u64) {
    for i in 0..CHURN_ROUNDS {
        let member = nid(base + i);
        let cmd = MetaCommand::UpsertMember {
            node: member.clone(),
            labels: BTreeMap::new(),
            status: NodeStatus::Active,
        };
        let deadline = env.now().0 + OP_BUDGET.as_nanos() as u64;
        let mut confirmed = false;
        while env.now().0 < deadline && !confirmed {
            if let Some((_, node)) = leader_slot(&nodes) {
                node.propose(cmd.clone());
            }
            env.sleep(POLL).await;
            if let Some((_, node)) = leader_slot(&nodes)
                && node.metadata().members.contains_key(&member)
            {
                confirmed = true;
            }
        }
        if confirmed {
            shared.confirm_member(member);
        }
    }
}

// ---------------------------------------------------------------------------
// Checks. No `check_cycles` here (see this file's top doc for why) — plain,
// self-contained verdicts over `Metadata` equality/presence instead of
// `animus_test`'s Elle/list-append `CheckReport` machinery, which doesn't
// apply to this plane's command-log model.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Verdict {
    ok: bool,
    violations: Vec<String>,
}

fn verdict(violations: Vec<String>) -> Verdict {
    let ok = violations.is_empty();
    Verdict { ok, violations }
}

/// Invariant #1 (convergence): every replica's own applied-state cache
/// agrees with replica 0's.
fn check_convergence_meta(metas: &[Metadata]) -> Verdict {
    let mut violations = Vec::new();
    for (i, m) in metas.iter().enumerate().skip(1) {
        if m != &metas[0] {
            violations.push(format!("replica {i} metadata diverged from replica 0"));
        }
    }
    verdict(violations)
}

/// Invariant #2 (durability): every effect a proposer's own confirm loop
/// actually observed committed must still be present in `reference` (the
/// converged final state).
fn check_durability_meta(shared: &Shared, reference: &Metadata) -> Verdict {
    let mut violations = Vec::new();
    for (table, schema) in shared.confirmed_schemas.lock().unwrap().iter() {
        match reference.table_schema(table) {
            Some(existing) if existing == schema => {}
            Some(other) => violations.push(format!(
                "confirmed schema for {table} lost: final state holds a DIFFERENT schema \
                 ({other:?} != {schema:?})"
            )),
            None => violations.push(format!(
                "confirmed schema for {table} lost: absent from final state"
            )),
        }
    }
    for member in shared.confirmed_members.lock().unwrap().iter() {
        if !reference.members.contains_key(member) {
            violations.push(format!("confirmed member {member:?} lost from final state"));
        }
    }
    verdict(violations)
}

/// Invariant #3 (schema-catalog exclusivity, checked unconditionally — see
/// this file's top doc for the full argument). Groups every attempted
/// `CreateTableSchema` by table name; a name only one proposer ever
/// attempted has nothing to check here (durability above already covers
/// it). For a name **two or more** proposers raced: on every replica, the
/// table's schema (if present) must byte-match exactly one of the racing
/// attempts, and must be present if any attempt was ever durably confirmed.
fn check_schema_exclusivity(shared: &Shared, metas: &[Metadata]) -> Verdict {
    let attempts = shared.schema_attempts.lock().unwrap();
    let confirmed_tables: BTreeSet<String> = shared
        .confirmed_schemas
        .lock()
        .unwrap()
        .iter()
        .map(|(t, _)| t.clone())
        .collect();
    let mut by_table: BTreeMap<&str, Vec<&TableSchema>> = BTreeMap::new();
    for (table, schema) in attempts.iter() {
        by_table.entry(table.as_str()).or_default().push(schema);
    }

    let mut violations = Vec::new();
    for (table, schemas) in &by_table {
        if schemas.len() < 2 {
            continue; // no race on this table name
        }
        for (i, meta) in metas.iter().enumerate() {
            match meta.table_schema(table) {
                None => {
                    if confirmed_tables.contains(*table) {
                        violations.push(format!(
                            "table {table} raced by {} proposers but ABSENT on replica {i}, \
                             though a racing schema was durably confirmed",
                            schemas.len()
                        ));
                    }
                }
                Some(winner) => {
                    if !schemas.contains(&winner) {
                        violations.push(format!(
                            "table {table} on replica {i} holds a schema matching NONE of the \
                             {} racing proposals (a hybrid/corrupted result): {winner:?}",
                            schemas.len()
                        ));
                    }
                }
            }
        }
    }
    verdict(violations)
}

// ---------------------------------------------------------------------------
// The scenario runner + result.
// ---------------------------------------------------------------------------

struct ScenarioResult {
    convergence: Verdict,
    durability: Verdict,
    exclusivity: Verdict,
    final_metas: Vec<Metadata>,
    schema_attempts: Vec<(String, TableSchema)>,
    confirmed_count: usize,
}

fn read_all_metadata(nodes: &Nodes) -> Vec<Metadata> {
    nodes.lock().unwrap().iter().map(|n| n.metadata()).collect()
}

fn run_scenario(scenario: &Scenario) -> ScenarioResult {
    let mut group = Group::start(scenario.seed, scenario.replicas);

    // Let the group elect a leader, then start the concurrent workload.
    group.sim.run_for(SETTLE);
    group.spawn_workload(&scenario.workload);

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

    // Hold the last fault open for the scenario's outage window (zero for
    // every cell in this PR — see `Scenario::window`'s own doc).
    if !scenario.window.is_zero() {
        group.sim.run_for(scenario.window);
    }

    // End healthy so the workload tail + final reads can make a quorum.
    group.heal_all();
    group.sim.run_for(DRAIN);

    // Converged-or-timeout poll for cross-replica agreement: a lagging
    // replica may still be catching up at the fixed drain, so re-read in
    // bounded increments and stop early once convergence holds.
    let mut metas = read_all_metadata(&group.nodes);
    let mut convergence = check_convergence_meta(&metas);
    let poll_deadline = group.sim.now().0 + CONVERGENCE_BUDGET.as_nanos() as u64;
    while !convergence.ok && group.sim.now().0 < poll_deadline {
        group.sim.run_for(CONVERGENCE_POLL_STEP);
        metas = read_all_metadata(&group.nodes);
        convergence = check_convergence_meta(&metas);
    }

    let durability = check_durability_meta(&group.shared, &metas[0]);
    let exclusivity = check_schema_exclusivity(&group.shared, &metas);
    let schema_attempts = group.shared.schema_attempts.lock().unwrap().clone();
    let confirmed_count = group.shared.confirmed_count();

    ScenarioResult {
        convergence,
        durability,
        exclusivity,
        final_metas: metas,
        schema_attempts,
        confirmed_count,
    }
}

/// Assert all three checks on one scenario result, labelling the scenario in
/// the failure message. Exclusivity + convergence are **safety** properties
/// (hard assert at any depth); durability is already behind the
/// converged-or-timeout poll, so a failure here means the budget was
/// genuinely exhausted.
fn assert_scenario_ok(s: &Scenario, r: &ScenarioResult) {
    assert!(
        r.convergence.ok,
        "scenario {} did not converge: {:?} (seed={})",
        s.name, r.convergence.violations, s.seed
    );
    assert!(
        r.durability.ok,
        "scenario {} lost a confirmed effect: {:?} (seed={})",
        s.name, r.durability.violations, s.seed
    );
    assert!(
        r.exclusivity.ok,
        "scenario {} violated schema-catalog exclusivity: {:?} (seed={})",
        s.name, r.exclusivity.violations, s.seed
    );
}

// ---------------------------------------------------------------------------
// Future invariants (PR②/③) — not implemented here, deliberately left as
// hooks so this harness doesn't need reshaping to add them:
//
// - **#4 allocator injectivity.** Every id one of `Metadata`'s monotonic
//   allocators hands out (tablet ids via `CreateTablet`/`BeginSplit`, the
//   `RegisterNode` claim path) stays globally unique even when two
//   proposers race the allocator concurrently under fault injection. PR②
//   is expected to add a dedicated `Workload::AllocatorRace` variant plus a
//   `check_allocator_injectivity` alongside the three checks above.
// - **#5 apply-task no-double-apply (ADR 0038).** The async apply task never
//   re-applies the same committed log index twice after a crash-recovery
//   cycle. This needs the `StopRestart` nemesis (a true process restart:
//   `sim.stop` + a fresh `RaftNode::start` reopening the SAME
//   `StorageEngine` handle the crashed node used — see this file's top doc
//   for why that's deferred to PR③) plus a way to observe the apply task's
//   own idempotency, not just `Metadata`'s converged end state.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

#[test]
fn control_plain_churn_baseline_converges() {
    let scenario = corpus_cells()
        .into_iter()
        .find(|s| s.name == "baseline_3")
        .expect("baseline_3 exists");
    let r = run_scenario(&scenario);
    assert_scenario_ok(&scenario, &r);
    assert!(
        r.confirmed_count > 0,
        "no confirmed churn — vacuous run (seed={})",
        scenario.seed
    );
}

#[test]
fn control_schema_race_baseline_holds_exclusivity() {
    let scenario = corpus_cells()
        .into_iter()
        .find(|s| s.name == "schema_race_baseline_3")
        .expect("schema_race_baseline_3 exists");
    let r = run_scenario(&scenario);
    assert_scenario_ok(&scenario, &r);
    assert!(
        r.confirmed_count > 0,
        "no confirmed schema — vacuous run (seed={})",
        scenario.seed
    );
    // Teeth: this must actually have been a race (>= 2 attempts on the one
    // shared table name), or the exclusivity check above was vacuous.
    let attempts_on_shared = r
        .schema_attempts
        .iter()
        .filter(|(t, _)| t == "ks.race")
        .count();
    assert!(
        attempts_on_shared >= 2,
        "workload did not actually race the same table name (seed={})",
        scenario.seed
    );
}

#[test]
fn control_corpus_is_convergent_and_durable() {
    let scenarios = corpus();
    let mut total_confirmed = 0usize;
    let mut faulted_with_confirms = 0usize;
    let mut faulted_total = 0usize;
    for s in &scenarios {
        let r = run_scenario(s);
        assert_scenario_ok(s, &r);
        total_confirmed += r.confirmed_count;
        if !s.faults.is_empty() {
            faulted_total += 1;
            if r.confirmed_count > 0 {
                faulted_with_confirms += 1;
            }
        }
    }
    // Non-vacuity guards: the corpus as a whole did real, fault-tolerant
    // work — mirrors raftkv's identical guard shape.
    assert!(
        total_confirmed > 0,
        "corpus too vacuous: no confirmed effects across {} scenarios",
        scenarios.len()
    );
    assert!(
        faulted_with_confirms >= faulted_total / 2,
        "too few faulted scenarios kept making confirmed progress \
         ({faulted_with_confirms}/{faulted_total}) — faults may be downing the group entirely"
    );
}

/// Coverage guard, mirroring `raftkv_corpus_covers_the_fault_matrix`: the
/// generator must keep exercising every fault class this PR's `Nemesis`
/// vocabulary defines, both workload shapes, and both group sizes —
/// otherwise a dimension silently stopped being tested. Structural only (no
/// scenario runs).
#[test]
fn control_corpus_covers_the_fault_matrix() {
    let cells = corpus_cells();

    let mut seen_faults: BTreeSet<Nemesis> = BTreeSet::new();
    let mut seen_workloads: BTreeSet<&str> = BTreeSet::new();
    let mut seen_shapes: BTreeSet<usize> = BTreeSet::new();
    let mut baselines = 0usize;
    for s in &cells {
        seen_shapes.insert(s.replicas);
        if s.faults.is_empty() {
            baselines += 1;
        }
        for (_, f) in &s.faults {
            seen_faults.insert(*f);
        }
        seen_workloads.insert(match s.workload {
            Workload::SchemaRace { .. } => "schema_race",
            Workload::PlainChurn { .. } => "plain_churn",
        });
    }

    for f in [
        Nemesis::LeaderKill,
        Nemesis::FollowerKill,
        Nemesis::PartitionLeader,
        Nemesis::SplitBrain,
        Nemesis::Lossy,
    ] {
        assert!(
            seen_faults.contains(&f),
            "fault {f:?} is not covered by any corpus scenario"
        );
    }
    for w in ["schema_race", "plain_churn"] {
        assert!(
            seen_workloads.contains(w),
            "workload {w} is not covered by any corpus scenario"
        );
    }
    assert!(
        seen_shapes.contains(&3) && seen_shapes.contains(&5),
        "both 3- and 5-replica shapes must be covered: {seen_shapes:?}"
    );
    assert!(baselines >= 2, "expected >= 2 no-fault baselines");

    let names: BTreeSet<&str> = cells.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names.len(), cells.len(), "corpus names must be unique");
    let seeds: BTreeSet<u64> = cells.iter().map(|s| s.seed).collect();
    assert_eq!(seeds.len(), cells.len(), "corpus seeds must be unique");
}

/// Seed-depth lever (`ANIMUS_CONTROL_SEEDS`): expanding the cells by `k`
/// yields exactly `k×` scenarios, names/seeds stay unique, and **variant 0
/// preserves the canonical (frozen) name+seed**. Structural only.
#[test]
fn control_seed_expansion_is_additive_and_unique() {
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
    // k == 1 is the identity.
    assert_eq!(corpus::seed_expand(base.clone(), 1).len(), base.len());
}

#[test]
fn control_run_is_deterministic() {
    // Same scenario twice → byte-identical final metadata and attempt log
    // (ADR 0003).
    let scenario = schema_race_scenario(
        "determinism_check",
        3,
        2,
        true,
        vec![(Duration::from_millis(2200), Nemesis::LeaderKill)],
    );
    let a = run_scenario(&scenario);
    let b = run_scenario(&scenario);
    assert_eq!(
        a.final_metas, b.final_metas,
        "final metadata not reproducible for seed {}",
        scenario.seed
    );
    assert_eq!(
        a.schema_attempts, b.schema_attempts,
        "schema-attempt log not reproducible for seed {}",
        scenario.seed
    );
}
