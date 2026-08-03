//! ADR 0017 / ADR 0016 step 4 / ADR 0014: an **Elle linearizability corpus for the
//! per-tablet Raft KV data plane** (`animus-raftdata`).
//!
//! This is the CP counterpart of the Accord corpus in `corpus.rs`. The leaderful
//! data plane offers **single-tablet linearizable KV** (`put`/`delete`/
//! `linearizable_get`) — *not* multi-key transactions — so it cannot reuse the
//! Accord harness's multi-key list-append workload. Instead this is a
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
//! they get the same **converged-or-timeout** poll the Accord runner uses.
//!
//! Single-writer-per-key (`owner(key) = key % clients`) is load-bearing for the
//! same reason as the Accord corpus: per-key LWW (the Raft log index is the MVCC
//! version) would otherwise *lose* a concurrent writer's append, a data-model
//! artefact rather than a consistency bug. A client builds each append on its own
//! authoritative in-memory list (it is the sole writer and runs serially) and
//! writes the whole new list back, so an indeterminate (`info`) write that later
//! re-commits restores the prefix.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_env::{Clock, EnvExt, Rng};
use animus_raftdata::RaftKvNode;
use animus_sim::{NetConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_test::history::{Key, Mop, Process};
use animus_test::{History, Recorder, check_convergence, check_cycles, check_durability};
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

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
/// Converged-or-timeout poll step + budget (mirrors the Accord runner): eventual
/// properties get a generous bounded poll rather than a fixed-drain snapshot.
const CONVERGENCE_POLL_STEP: Duration = Duration::from_secs(2);
const CONVERGENCE_BUDGET: Duration = Duration::from_secs(120);

// ---------------------------------------------------------------------------
// Declarative scenario model (a focused mirror of the Accord corpus's, with the
// fault vocabulary the *leaderful* plane actually exercises).
// ---------------------------------------------------------------------------

/// A fault the nemesis injects at a scheduled virtual time, resolved against the
/// live group at run time (so a scenario is shape-relative).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
}

/// A seed-reproducible scenario: a named group size + workload + an explicit fault
/// schedule (virtual time → nemesis). `HealAll` is always applied by the runner at
/// the end, so it is not scheduled here.
#[derive(Clone, Debug)]
struct Scenario {
    name: String,
    seed: u64,
    replicas: usize,
    clients: usize,
    rounds: u64,
    keyspace: u64,
    read_pct: u64,
    faults: Vec<(Duration, Nemesis)>,
}

// ---------------------------------------------------------------------------
// The frozen corpus: a committed, deterministic generator. Each scenario's seed
// is a stable hash of its name (FNV-1a — no `std::hash` nondeterminism), so the
// suite runs the SAME set every run and a failure names one scenario + seed.
// ---------------------------------------------------------------------------

/// FNV-1a over the name's bytes — a deterministic name→seed map.
fn name_seed(name: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in name.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Single-fault nemeses sampled across the corpus.
const CORPUS_FAULTS: [(&str, Nemesis); 4] = [
    ("leader_kill", Nemesis::LeaderKill),
    ("follower_kill", Nemesis::FollowerKill),
    ("partition_leader", Nemesis::PartitionLeader),
    ("lossy", Nemesis::Lossy),
];

/// Fault timing relative to the workload's life: early / mid / late.
const CORPUS_TIMINGS: [(&str, Duration); 3] = [
    ("early", Duration::from_millis(700)),
    ("mid", Duration::from_millis(2200)),
    ("late", Duration::from_millis(3800)),
];

/// A small high-contention workload: enough clients to make a key hot, a tiny key
/// space, a mix of reads and writes. Single-key ops (the plane is non-transactional).
fn base_workload(name: &str, replicas: usize, faults: Vec<(Duration, Nemesis)>) -> Scenario {
    Scenario {
        seed: name_seed(name),
        name: name.to_string(),
        replicas,
        clients: 3,
        rounds: 6,
        keyspace: 3,
        read_pct: 45,
        faults,
    }
}

/// The structural cells of the corpus: baselines (no fault, both shapes) + every
/// (fault × timing) over a 3-replica group + a 5-replica spot-check per fault.
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
    out
}

/// Seeds per structural cell (`ANIMUS_RAFTKV_SEEDS`, default 1) — the *depth* knob
/// (mirrors the Accord corpus's `ANIMUS_CORPUS_SEEDS`). One structural cell × many
/// interleavings is the dominant bug-finding lever; `K=1` is byte-identical to the
/// committed frozen set.
fn seeds_per_cell() -> usize {
    std::env::var("ANIMUS_RAFTKV_SEEDS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1)
        .max(1)
}

/// Expand each cell into `k` seed variants. Variant 0 keeps the cell's canonical
/// (frozen) name + seed (so `k=1` is the identity); variants `1..k` get a `_sNN`
/// suffix and a fresh name-derived seed.
fn seed_expand(cells: Vec<Scenario>, k: usize) -> Vec<Scenario> {
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
                    seed: name_seed(&name),
                    replicas: cell.replicas,
                    clients: cell.clients,
                    rounds: cell.rounds,
                    keyspace: cell.keyspace,
                    read_pct: cell.read_pct,
                    faults: cell.faults.clone(),
                    name,
                });
            }
        }
    }
    out
}

/// The corpus the headline test runs: the frozen cells, seed-expanded by the
/// depth knob.
fn corpus() -> Vec<Scenario> {
    seed_expand(corpus_cells(), seeds_per_cell())
}

// ---------------------------------------------------------------------------
// List value encoding (matches the Accord harness: u64 elements, 8 big-endian
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
}

impl Shared {
    fn fresh_value(&self) -> u64 {
        let mut v = self.next_value.lock().unwrap();
        *v += 1;
        *v
    }
}

struct Group {
    sim: Simulator,
    nodes: Arc<Vec<KvNode>>,
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
        let nodes: Vec<KvNode> = ids
            .iter()
            .map(|&id| RaftKvNode::start(sim.env(id), ids.clone(), MemoryEngine::new()))
            .collect();
        Group {
            sim,
            nodes: Arc::new(nodes),
            replicas,
            shared: Arc::new(Shared {
                rec: Mutex::new(Recorder::new(seed)),
                next_value: Mutex::new(0),
            }),
            crashed: BTreeSet::new(),
        }
    }

    /// Spawn `clients` concurrent client tasks, each on its own never-faulted
    /// driver env, routing single-key ops to whichever group node currently leads.
    fn spawn_workload(&mut self, clients: usize, rounds: u64, keyspace: u64, read_pct: u64) {
        for (c, &client_id) in CLIENT_IDS.iter().enumerate().take(clients) {
            let env = self.sim.env(client_id);
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

    /// Index of a group node that currently believes it leads (lowest id if more
    /// than one — possible transiently under a partition). `None` if none does.
    fn current_leader(nodes: &[KvNode]) -> Option<usize> {
        nodes.iter().position(|n| n.is_leader())
    }

    fn apply(&mut self, nem: Nemesis) {
        let ids: Vec<u64> = GROUP_IDS[..self.replicas].to_vec();
        match nem {
            Nemesis::LeaderKill => {
                if let Some(li) = Self::current_leader(&self.nodes) {
                    self.sim.crash(ids[li]);
                    self.crashed.insert(ids[li]);
                }
            }
            Nemesis::FollowerKill => {
                let leader = Self::current_leader(&self.nodes);
                // First non-leader, live replica.
                let victim = (0..self.replicas)
                    .find(|&i| Some(i) != leader && !self.crashed.contains(&ids[i]));
                if let Some(i) = victim {
                    self.sim.crash(ids[i]);
                    self.crashed.insert(ids[i]);
                }
            }
            Nemesis::PartitionLeader => {
                if let Some(li) = Self::current_leader(&self.nodes) {
                    for j in 0..self.replicas {
                        if j != li {
                            self.sim.partition_pair(ids[li], ids[j]);
                        }
                    }
                }
            }
            Nemesis::Lossy => {
                self.sim.set_net_config(lossy(0.1));
            }
        }
    }

    /// Heal every partition, restart every crashed node, restore default links.
    fn heal_all(&mut self) {
        let ids: Vec<u64> = GROUP_IDS[..self.replicas].to_vec();
        for i in 0..ids.len() {
            for j in (i + 1)..ids.len() {
                self.sim.heal(ids[i], ids[j]);
            }
        }
        let crashed: Vec<u64> = self.crashed.iter().copied().collect();
        for v in crashed {
            self.sim.restart(v);
        }
        self.crashed.clear();
        self.sim.set_net_config(NetConfig::default());
    }
}

/// One client's loop: each round, run a single-key read or write op (single-writer
/// for writes), then record the outcome.
#[allow(clippy::too_many_arguments)]
async fn client_loop(
    env: SimEnv,
    nodes: Arc<Vec<KvNode>>,
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
/// leader change (the stored value is the whole list, so a re-propose is a no-op).
async fn run_write(
    env: &SimEnv,
    nodes: &[KvNode],
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
    let mut proposed_on: Option<usize> = None;
    let mut committed = false;
    while env.now().0 < deadline {
        if let Some(li) = Group::current_leader(nodes) {
            if proposed_on != Some(li) {
                if let ProposeResult::Accepted { .. } = nodes[li].put(kb.clone(), encoded.clone()) {
                    proposed_on = Some(li);
                }
            }
        }
        env.sleep(POLL).await;
        if let Some(li) = Group::current_leader(nodes) {
            if let Some(bytes) = nodes[li].linearizable_get(&kb).await {
                if decode_list(&bytes).contains(&value) {
                    committed = true;
                    break;
                }
            }
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
async fn run_read(env: &SimEnv, nodes: &[KvNode], shared: &Arc<Shared>, proc: Process, key: Key) {
    let kb = key_bytes(key);
    let invoke = vec![Mop::Read {
        key,
        observed: None,
    }];
    shared.rec.lock().unwrap().invoke(proc, env.now().0, invoke);

    let deadline = env.now().0 + OP_BUDGET.as_nanos() as u64;
    let mut observed: Option<Vec<u64>> = None;
    while env.now().0 < deadline {
        if let Some(li) = Group::current_leader(nodes) {
            if let Some(bytes) = nodes[li].linearizable_get(&kb).await {
                observed = Some(decode_list(&bytes));
                break;
            }
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
fn final_state(nodes: &[KvNode], idx: usize, keyspace: u64) -> BTreeMap<Key, Vec<u64>> {
    let mut map = BTreeMap::new();
    for key in 0..keyspace {
        let list = block_on(nodes[idx].local_get(&key_bytes(key)))
            .map(|b| decode_list(&b))
            .unwrap_or_default();
        map.insert(key, list);
    }
    map
}

fn run_scenario(scenario: &Scenario) -> ScenarioResult {
    let mut group = Group::start(scenario.seed, scenario.replicas);

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

    // End healthy so the workload tail + final reads can make a quorum.
    group.heal_all();
    group.sim.run_for(DRAIN);

    let keys = scenario.keyspace;
    let history = group.shared.rec.lock().unwrap().history().clone();
    let cycles = check_cycles(&history);

    // Converged-or-timeout poll for the eventual properties (mirrors the Accord
    // runner): a lagging follower may still be catching up at the fixed drain, so
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
        assert!(
            r.cycles.ok,
            "scenario {} not serializable: {:?} (seed={})",
            s.name, r.cycles.violations, s.seed
        );
        // Convergence + durability are EVENTUAL — the poll gives them room to heal;
        // only budget exhaustion is a genuine failure.
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
