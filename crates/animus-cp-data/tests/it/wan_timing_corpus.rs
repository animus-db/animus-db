//! ADR 0075 section 3.4 (G-01 stage G-c groundwork): a seed-reproducible
//! `SimEnv` corpus for the **per-group WAN Raft timing profile**.
//!
//! Three regions, one node each, every inter-region link 60-90 ms one-way
//! (`Simulator::set_link_net_config`, with a small jitter and an occasional
//! heavy-tail spike, which is what a real WAN link does). A tablet group
//! replicated across all three is hosted by the real per-node
//! `host::Reconciler`, fed a `MetadataView` whose `regions` map stands in for
//! the replicated `Member.labels`. With the labels present the reconciler
//! installs the WAN timing pair (`TimingProfile::Wan`) on the group; without
//! them the group keeps the historical LAN constants (150 ms election base,
//! 50 ms heartbeat).
//!
//! Cells (each a fixed script, seed-expanded by `ANIMUS_WAN_TIMING_SEEDS`):
//! - `wan_steady`: leadership is stable and writes commit.
//! - `wan_kill_leader_region`: the leader's node dies (process stop), the
//!   other two regions elect a new leader and keep committing, the dead node
//!   restarts and re-hosts; no acked write is lost.
//! - `wan_partition_region_heal`: the leader's region is partitioned away,
//!   then healed; the majority side keeps committing; no acked write is lost.
//!
//! **Negative control** (`lan_profile_forced_shows_failure`): the identical
//! scripts with the reconciler fed *no* region labels, i.e. forced onto the
//! LAN profile. On the fault cells (a re-election is required) the LAN-forced
//! group shows election churn (term growth up to 30, against the WAN group's
//! one or two) and stalled commits; the test asserts that failure signature
//! on most cells, so the corpus provably has teeth against what the profile
//! fixes.
//!
//! Measured finding: a LAN-forced group that merely *keeps* its leader stays
//! quiet even on these links, because pre-vote and its lease absorb late
//! heartbeats; the profile matters when the group must re-elect over a noisy
//! WAN, so the controls are the fault cells.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::host::{MemoryTabletEngines, MetadataView, Reconciler};
use animus_env::{NodeId, nid};
use animus_sim::{NetConfig, SimEnv, Simulator};
use animus_storage::{MemoryEngine, StorageEngine};
use animus_tablet::{KeyRange, Tablet, TabletId};
use animus_test::corpus::{self, SeedVariant};
use futures::executor::block_on;

type Recon = Reconciler<SimEnv, MemoryEngine>;

const TABLE: &str = "t";
const TABLET: TabletId = TabletId(1);
/// The configured max inter-region round trip handed to the reconciler.
/// One-way latency is at most ~90 ms plus jitter, so a round trip is at most
/// ~200 ms; the operator-facing knob is sized to that.
const MAX_REGION_RTT: Duration = Duration::from_millis(200);
const WRITE_PERIOD: Duration = Duration::from_millis(500);
/// Link noise on top of the 60-90 ms one-way base: a congested WAN link.
/// **Measured, not assumed** (see the module doc): with only a few ms of
/// jitter the LAN profile survives these latencies too (pre-vote and its
/// leader lease absorb late heartbeats, and pipelined heartbeats keep
/// arriving every 50 ms), so a negative control built on latency alone does
/// not bite. The WAN profile earns its keep when the link also has tail
/// latency and the group must *re-elect*: 30% of messages take up to 400 ms
/// extra. Held to the same 60/75/90 ms bases the roadmap names.
const JITTER: Duration = Duration::from_millis(40);
const TAIL_JITTER: Duration = Duration::from_millis(400);
const TAIL_PROB: f64 = 0.30;
/// Shared by the WAN cells (must stay within) and the LAN-forced control
/// (must exceed on most fault cells). Calibrated over 50 seeds per cell.
const MAX_TERM_GROWTH: u64 = 3;
const MIN_ACKED_DURING_FAULT: usize = 18;
/// The negative control is statistical (it must fail on *most* LAN-forced
/// fault cells, and a single seed can get lucky: at one seed per cell it hit
/// 1 of 2), so it always runs at least this many seeds per cell regardless of
/// `ANIMUS_WAN_TIMING_SEEDS`.
const NEGATIVE_CONTROL_MIN_SEEDS: usize = 10;

fn node(i: usize) -> NodeId {
    nid(700 + i as u64)
}
fn region_of(i: usize) -> String {
    ["region-a", "region-b", "region-c"][i].to_string()
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    /// Region labels present: the reconciler derives the WAN profile.
    Wan,
    /// Negative control: no labels, so the group is forced onto LAN timing.
    LanForced,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Fault {
    None,
    KillLeader,
    PartitionLeader,
}

struct World {
    sim: Simulator,
    mode: Mode,
    engines: Vec<MemoryTabletEngines>,
    recons: Vec<Option<Recon>>,
    alive: [bool; 3],
    isolated: Option<usize>,
    /// Writes we proposed: (node idx, key, index, term).
    pending: Vec<(usize, Vec<u8>, u64, u64)>,
    /// Keys whose commit we observed (the engine applied them on the proposer).
    acked: Vec<Vec<u8>>,
    next_key: u64,
}

fn view(mode: Mode) -> MetadataView {
    let replicas: Vec<NodeId> = (0..3).map(node).collect();
    let t = Tablet::new_for_table(TABLET, TABLE, KeyRange::new(Vec::new(), None), replicas);
    let regions: BTreeMap<NodeId, String> = if mode == Mode::Wan {
        (0..3).map(|i| (node(i), region_of(i))).collect()
    } else {
        BTreeMap::new()
    };
    MetadataView {
        tablets: [(t.id, t)].into_iter().collect(),
        regions,
        ..Default::default()
    }
}

impl World {
    fn new(seed: u64, mode: Mode) -> World {
        let sim = Simulator::new(seed);
        // 60 / 75 / 90 ms one-way between the three region pairs, symmetric,
        // `JITTER` and a heavy tail (see those consts), ~1 ms inside a region
        // (unused: one node per region).
        let one_way = [((0, 1), 60u64), ((0, 2), 75), ((1, 2), 90)];
        for ((i, j), ms) in one_way {
            let mut cfg = NetConfig::default();
            cfg.base_delay = Duration::from_millis(ms);
            cfg.max_jitter = JITTER;
            cfg.heavy_tail_max_jitter = TAIL_JITTER;
            cfg.set_heavy_tail_prob(TAIL_PROB);
            sim.set_link_net_config(node(i), node(j), cfg.clone());
            sim.set_link_net_config(node(j), node(i), cfg);
        }
        let mut w = World {
            sim,
            mode,
            engines: (0..3).map(|_| MemoryTabletEngines::new()).collect(),
            recons: (0..3).map(|_| None).collect(),
            alive: [true; 3],
            isolated: None,
            pending: Vec::new(),
            acked: Vec::new(),
            next_key: 0,
        };
        for i in 0..3 {
            w.start_node(i);
        }
        w
    }

    /// (Re)build node `i`'s reconciler over its (durable) engine registry and
    /// host the tablet. Host-only ticks never tear a group down, so a
    /// `block_on` is safe here (see `reconciler_corpus.rs`'s gotcha).
    fn start_node(&mut self, i: usize) {
        let mut r: Recon = Reconciler::new(
            self.sim.env(node(i)),
            self.engines[i].clone(),
            node(i),
            |_t, _n| {},
            |_t| {},
        );
        r.set_max_region_rtt(MAX_REGION_RTT);
        block_on(r.tick(&view(self.mode)));
        self.recons[i] = Some(r);
        self.alive[i] = true;
    }

    fn handle(&self, i: usize) -> Option<&animus_cp_data::RaftKvNode<SimEnv, MemoryEngine>> {
        if !self.alive[i] {
            return None;
        }
        self.recons[i].as_ref()?.hosted_node(TABLET)
    }

    fn max_term(&self) -> u64 {
        (0..3)
            .filter_map(|i| self.handle(i).map(|h| h.term()))
            .max()
            .unwrap_or(0)
    }

    /// The believed leader with the highest term among live, non-isolated
    /// nodes.
    fn leader(&self) -> Option<usize> {
        (0..3)
            .filter(|i| Some(*i) != self.isolated)
            .filter_map(|i| {
                self.handle(i)
                    .filter(|h| h.is_leader())
                    .map(|h| (h.term(), i))
            })
            .max()
            .map(|(_, i)| i)
    }

    fn advance(&mut self, d: Duration) {
        self.sim.run_for(d);
    }

    fn write_one(&mut self) {
        let Some(l) = self.leader() else { return };
        let key = format!("k{:05}", self.next_key).into_bytes();
        self.next_key += 1;
        let h = self.handle(l).expect("leader handle");
        if let ProposeResult::Accepted { index, term } = h.put(key.clone(), b"v".to_vec()) {
            self.pending.push((l, key, index, term));
        }
    }

    /// Promote pending proposals the proposer's engine has since applied
    /// (applied implies committed) to `acked`.
    fn settle_acks(&mut self) {
        let mut still = Vec::new();
        for (i, key, index, term) in std::mem::take(&mut self.pending) {
            match self.handle(i) {
                Some(h) if h.term() == term && h.engine_applied_index() >= index => {
                    self.acked.push(key);
                }
                Some(h) if h.term() == term => still.push((i, key, index, term)),
                _ => {} // proposer died or moved on a term: not acked, may be lost
            }
        }
        self.pending = still;
    }

    /// Drive `total` of virtual time, proposing one write per `WRITE_PERIOD`.
    /// Returns how many writes were acked during the window.
    fn run_writing(&mut self, total: Duration) -> usize {
        let before = self.acked.len();
        let mut elapsed = Duration::ZERO;
        while elapsed < total {
            self.write_one();
            self.advance(WRITE_PERIOD);
            self.settle_acks();
            elapsed += WRITE_PERIOD;
        }
        self.acked.len() - before
    }

    fn key_set(&self, i: usize) -> BTreeSet<Vec<u8>> {
        let eng = self.engines[i].engine(TABLET);
        let mut out = BTreeSet::new();
        for k in 0..self.next_key {
            let key = format!("k{k:05}").into_bytes();
            let mut phys = vec![animus_cp_data::KIND_BASE];
            phys.extend_from_slice(&key);
            if block_on(eng.get(&phys)).expect("engine read").is_some() {
                out.insert(key);
            }
        }
        out
    }
}

/// What one scripted run observed.
#[derive(Debug)]
struct Report {
    /// Raft term growth over the whole run, after the initial election settled.
    term_growth: u64,
    /// Term growth during the steady (pre-fault) window alone.
    steady_term_growth: u64,
    acked_steady: usize,
    acked_during_fault: usize,
    acked_total: usize,
    /// Every acked key present on every replica's engine at the end.
    all_acked_durable: bool,
    /// All three replicas hold the same key set at the end.
    replicas_converged: bool,
    /// The election-timeout base the hosted group actually runs.
    election_timeout: Duration,
}

fn run_script(seed: u64, mode: Mode, fault: Fault) -> Report {
    let mut w = World::new(seed, mode);
    // Initial election (a WAN group's first timeout is up to 2x its base).
    w.advance(Duration::from_secs(25));
    let settled = w.max_term();
    let election_timeout = (0..3)
        .find_map(|i| w.handle(i).map(|h| h.election_timeout()))
        .expect("a hosted group");

    let acked_steady = w.run_writing(Duration::from_secs(12));
    let steady_term_growth = w.max_term().saturating_sub(settled);

    let mut acked_during_fault = 0;
    match fault {
        Fault::None => {
            acked_during_fault = w.run_writing(Duration::from_secs(12));
        }
        Fault::KillLeader => {
            if let Some(victim) = w.leader() {
                w.sim.stop(node(victim));
                w.alive[victim] = false;
                w.recons[victim] = None;
                acked_during_fault = w.run_writing(Duration::from_secs(15));
                w.start_node(victim);
            }
        }
        Fault::PartitionLeader => {
            if let Some(victim) = w.leader() {
                for other in (0..3).filter(|o| *o != victim) {
                    w.sim.partition_pair(node(victim), node(other));
                }
                w.isolated = Some(victim);
                acked_during_fault = w.run_writing(Duration::from_secs(15));
                for other in (0..3).filter(|o| *o != victim) {
                    w.sim.heal(node(victim), node(other));
                }
                w.isolated = None;
            }
        }
    }
    w.run_writing(Duration::from_secs(10));
    // Quiesce: no writes, let every replica converge.
    w.advance(Duration::from_secs(25));
    w.settle_acks();

    let sets: Vec<BTreeSet<Vec<u8>>> = (0..3).map(|i| w.key_set(i)).collect();
    let all_acked_durable = w.acked.iter().all(|k| sets.iter().all(|s| s.contains(k)));
    let replicas_converged = sets.windows(2).all(|p| p[0] == p[1]);
    Report {
        term_growth: w.max_term().saturating_sub(settled),
        steady_term_growth,
        acked_steady,
        acked_during_fault,
        acked_total: w.acked.len(),
        all_acked_durable,
        replicas_converged,
        election_timeout,
    }
}

// ---------------------------------------------------------------------------
// The corpus.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Cell {
    name: String,
    seed: u64,
    mode: Mode,
    fault: Fault,
}

impl SeedVariant for Cell {
    fn scenario_name(&self) -> &str {
        &self.name
    }
    fn reseeded(&self, name: String, seed: u64) -> Self {
        Cell {
            name,
            seed,
            ..self.clone()
        }
    }
}

fn cell(name: &str, mode: Mode, fault: Fault) -> Cell {
    Cell {
        name: name.to_string(),
        seed: corpus::name_seed(name),
        mode,
        fault,
    }
}

/// Depth knob (`ANIMUS_WAN_TIMING_SEEDS`, default 1).
fn seeds_per_cell() -> usize {
    corpus::seeds_from_env("ANIMUS_WAN_TIMING_SEEDS")
}

fn wan_cells() -> Vec<Cell> {
    vec![
        cell("wan_steady", Mode::Wan, Fault::None),
        cell("wan_kill_leader_region", Mode::Wan, Fault::KillLeader),
        cell(
            "wan_partition_region_heal",
            Mode::Wan,
            Fault::PartitionLeader,
        ),
    ]
}

fn negative_cells() -> Vec<Cell> {
    // No steady-state cell: measured, a LAN-forced group stays quiet while it
    // has a leader (pre-vote + lease absorb the late heartbeats). The failure
    // appears when it has to re-elect, so only the fault cells are controls.
    vec![
        cell(
            "lan_forced_kill_leader_region",
            Mode::LanForced,
            Fault::KillLeader,
        ),
        cell(
            "lan_forced_partition_region_heal",
            Mode::LanForced,
            Fault::PartitionLeader,
        ),
    ]
}

#[test]
fn wan_profile_keeps_leadership_stable_and_commits_durably() {
    let observed = Arc::new(Mutex::new(Vec::new()));
    for c in corpus::seed_expand(wan_cells(), seeds_per_cell()) {
        let r = run_script(c.seed, c.mode, c.fault);
        observed
            .lock()
            .unwrap()
            .push((c.name.clone(), format!("{r:?}")));
        let ctx = format!("{} seed={:#x} {r:?}", c.name, c.seed);
        assert_eq!(
            r.steady_term_growth, 0,
            "leadership must not churn on a healthy WAN: {ctx}"
        );
        // The reconciler really installed the WAN pair (5 x the configured RTT).
        assert_eq!(
            r.election_timeout,
            MAX_REGION_RTT * 5,
            "WAN profile not installed: {ctx}"
        );
        // One failover (or one partition + one rejoin) usually moves the term
        // once; an occasional split vote makes it two. Never churn.
        assert!(
            r.term_growth <= MAX_TERM_GROWTH,
            "bounded term changes: {ctx}"
        );
        assert!(
            r.acked_steady >= 22,
            "steady-state writes commit (>= 22 of 24): {ctx}"
        );
        if c.fault != Fault::None {
            assert!(
                r.acked_during_fault >= MIN_ACKED_DURING_FAULT,
                "writes keep committing through the fault: {ctx}"
            );
        }
        assert!(r.acked_total > 0, "nothing was ever acked: {ctx}");
        assert!(r.all_acked_durable, "an acked write was lost: {ctx}");
        assert!(r.replicas_converged, "replicas diverged: {ctx}");
    }
    for (n, r) in observed.lock().unwrap().iter() {
        eprintln!("WANTIMING {n}: {r}");
    }
}

/// NEGATIVE CONTROL: forced onto the LAN profile, the same links must show
/// failure (election churn and/or stalled commits). If this ever passes
/// "cleanly", the corpus has lost its teeth and the WAN cells above prove
/// nothing.
#[test]
fn lan_profile_forced_shows_failure() {
    let mut bad = 0usize;
    let mut total = 0usize;
    for c in corpus::seed_expand(
        negative_cells(),
        seeds_per_cell().max(NEGATIVE_CONTROL_MIN_SEEDS),
    ) {
        let r = run_script(c.seed, c.mode, c.fault);
        eprintln!("WANTIMING-NEG {} seed={:#x} {r:?}", c.name, c.seed);
        assert_eq!(
            r.election_timeout,
            Duration::from_millis(150),
            "the control must really run the LAN pair"
        );
        // Safety still holds under LAN timing (churn costs liveness, never
        // durability): the control fails on liveness only.
        assert!(
            r.all_acked_durable && r.replicas_converged,
            "{}: {r:?}",
            c.name
        );
        total += 1;
        // The WAN cells measure term_growth == 1 and >= 24 of 30 acked.
        let churn = r.term_growth > MAX_TERM_GROWTH;
        let stalled = r.acked_during_fault < MIN_ACKED_DURING_FAULT;
        if churn || stalled {
            bad += 1;
        }
    }
    assert!(
        bad * 2 > total,
        "the LAN-forced negative control must fail on most cells ({bad}/{total}); \
         the WAN cells would prove nothing"
    );
}

#[test]
fn wan_corpus_names_and_seeds_are_unique_and_frozen() {
    let all: Vec<Cell> = wan_cells().into_iter().chain(negative_cells()).collect();
    let names: BTreeSet<&str> = all.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names.len(), all.len());
    let seeds: BTreeSet<u64> = all.iter().map(|c| c.seed).collect();
    assert_eq!(seeds.len(), all.len());
    for c in &all {
        assert_eq!(c.seed, corpus::name_seed(&c.name));
    }
    let k = 3;
    assert_eq!(
        corpus::seed_expand(wan_cells(), k).len(),
        wan_cells().len() * k
    );
}
