//! ADR 0078 (election backpressure) Tier 1: a seed-reproducible `SimEnv`
//! corpus of **many Raft groups sharing three starved nodes**.
//!
//! Three physical nodes host `G` RF3 groups (one stream per group, the
//! per-node heartbeat batcher on, `MemoryEngine`), the same shape as
//! `heartbeat_batch_corpus`. Each node is given a finite virtual CPU with
//! `Simulator::set_cpu_cost` (ADR 0078 "step zero"): its timer fires and
//! deliveries are serialized and cost `per_event` each. The cost is chosen as
//! a multiple of the *steady* per-node event demand ([`EVENTS_PER_GROUP_PER_S`]
//! x `G`), so "1.15x capacity" means what it says whatever `G` is. That is
//! the issue #1199 collapse without wall time: a node a little past its
//! capacity answers heartbeats late, followers pre-vote and campaign, the
//! campaigns cost more than the heartbeats they replace, and the node gets
//! later still.
//!
//! **Structure.** A cell is data ([`Cell`]): what to run ([`Kind`]), how
//! loaded ([`Cell::load`] is a multiple of capacity), and which of the three
//! ADR 0078 mechanisms are on ([`Mechanisms`]). Later steps of the ADR plug
//! the campaign governor, the lag stretch and staggered hosting into
//! [`Mechanisms`]/[`Rig::host`] and add cells 2-12 and negative controls
//! (b)-(f) to [`cells`]/[`controls`]; the runner, the sampler and the oracle
//! below do not change. At this step (ADR step 1) no mechanism exists, so
//! every cell runs the unprotected code.
//!
//! **Oracle** ([`Report`], [`Limits::violations`]) is asserted on every cell:
//! - *safety* (never relaxed, also in the negative control): no two leaders in
//!   one `(group, term)` at any sample, and every write acked by its leader
//!   is on every replica at the end;
//! - *led count*: time to all groups led, and no group left leaderless at the
//!   end once the fault (the overload) has cleared;
//! - *term churn*: mean `max_term - 1` per group below a ceiling;
//! - *monotone led count* after the first full convergence: the shape the
//!   issue shows failing (led count falling over time).
//!
//! **Negative control (a)** (`control_a_*`): cell 5 with no mechanism must
//! reproduce the collapse, and the test asserts the oracle *fails* on it
//! (and which clauses), so the oracle provably bites.
//!
//! Knobs: `ANIMUS_ELECTION_BP_SEEDS=K` (default 1, [`corpus::seed_expand`]),
//! `ANIMUS_ELECTION_BP_CELL=<substring>` narrows the cells,
//! `ANIMUS_SEED=<seed>` replays one run, `ANIMUS_ELECTION_BP_GROUPS=G`
//! overrides the group count (default [`GROUPS`]; the nightly size is 2000).

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::heartbeat_batch::{DEFAULT_HEARTBEAT_BATCH_INTERVAL, HeartbeatBatcher};
use animus_cp_data::{KIND_BASE, RaftKvNode, StorageScope};
use animus_env::{MetricsHandle, nid};
use animus_sim::{CpuOrder, SimEnv, Simulator};
use animus_storage::{MemoryEngine, StorageEngine};
use animus_test::corpus::{self, SeedVariant};
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

const NODES: [u64; 3] = [0, 1, 2];
/// Per-push group count (ADR 0078 Tier 1: G=300).
const GROUPS: usize = 300;
/// First stream id; group `g` lives on stream `STREAM0 + g`.
const STREAM0: u64 = 100;
/// Sampler period (virtual time).
const SAMPLE: Duration = Duration::from_millis(500);

/// Steady-state events (timer fires + deliveries) per group per second, per
/// node, in this rig (3 nodes, batcher on, leaders spread by the natural
/// election). **Measured**, not derived: the test
/// `calibration_matches_measurement` re-measures it and fails if this drifts
/// by more than 15%. A node's steady demand is this x `G`; a cell's
/// [`Cell::load`] scales it into a `per_event` cost.
const EVENTS_PER_GROUP_PER_S: f64 = 24.6;

// ---------------------------------------------------------------------------
// Cells as data.
// ---------------------------------------------------------------------------

/// Which ADR 0078 mechanisms are on. Step 1 of the implementation plan has
/// none of them; steps 3 to 5 add a field each and a branch in [`Rig::host`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
struct Mechanisms {
    // campaign_governor: bool   (step 3)
    // lag_stretch: bool         (step 4)
    // staggered_hosting: bool   (step 5)
}

impl Mechanisms {
    /// Nothing on: the code as it was before ADR 0078.
    const NONE: Mechanisms = Mechanisms {};
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    /// Cell 1: every group is hosted at once from a cold start, the node CPU
    /// already at `load` x steady demand.
    ColdStart,
    /// Cell 5: converge unloaded, then raise the cost to `load` x steady
    /// demand for [`OVERLOAD`] of virtual time.
    SustainedOverload,
}

#[derive(Clone)]
struct Cell {
    name: String,
    seed: u64,
    kind: Kind,
    /// Offered load as a multiple of node capacity (1.15 = demand is 1.15x
    /// what the node can do).
    load: f64,
    mech: Mechanisms,
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

fn cell(name: &str, kind: Kind, load: f64, mech: Mechanisms) -> Cell {
    Cell {
        name: name.to_string(),
        seed: corpus::name_seed(name),
        kind,
        load,
        mech,
    }
}

/// Cells whose oracle must pass. Grows with the ADR's implementation plan.
fn cells() -> Vec<Cell> {
    vec![cell(
        "cell_1_cold_start_all_at_once",
        Kind::ColdStart,
        0.9,
        Mechanisms::NONE,
    )]
}

/// Negative controls: cells whose oracle must FAIL (it bites).
fn controls() -> Vec<Cell> {
    vec![cell(
        "control_a_no_mechanisms_sustained_overload",
        Kind::SustainedOverload,
        1.25,
        Mechanisms::NONE,
    )]
}

fn seeds_per_cell() -> usize {
    corpus::seeds_from_env("ANIMUS_ELECTION_BP_SEEDS")
}

fn groups() -> usize {
    std::env::var("ANIMUS_ELECTION_BP_GROUPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|g| *g > 0)
        .unwrap_or(GROUPS)
}

/// `ANIMUS_ELECTION_BP_CELL` narrows by substring; `ANIMUS_SEED` replays one
/// run (it replaces every selected cell's seed).
fn select(cells: Vec<Cell>) -> Vec<Cell> {
    let filter = std::env::var("ANIMUS_ELECTION_BP_CELL").ok();
    let replay = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok());
    corpus::seed_expand(cells, seeds_per_cell())
        .into_iter()
        .filter(|c| filter.as_ref().is_none_or(|f| c.name.contains(f.as_str())))
        .map(|mut c| {
            if let Some(s) = replay {
                c.seed = s;
            }
            c
        })
        .collect()
}

// ---------------------------------------------------------------------------
// The rig: G groups on three shared nodes.
// ---------------------------------------------------------------------------

struct Rig {
    sim: Simulator,
    /// `[group][replica]`, replica `i` lives on `NODES[i]`.
    nodes: Vec<Vec<KvNode>>,
    g: usize,
}

impl Rig {
    fn new(seed: u64, g: usize) -> Rig {
        Rig {
            sim: Simulator::new(seed),
            nodes: Vec::new(),
            g,
        }
    }

    /// Host all `g` groups at once (ADR 0078 cell 1's "hosting stagger off").
    /// `mech` is where later steps branch.
    fn host(&mut self, _mech: Mechanisms) {
        let handles: Vec<MetricsHandle> =
            NODES.iter().map(|_| MetricsHandle::recording()).collect();
        let batchers: Vec<HeartbeatBatcher<SimEnv>> = NODES
            .iter()
            .enumerate()
            .map(|(i, &id)| {
                HeartbeatBatcher::new(
                    self.sim.env(nid(id)),
                    DEFAULT_HEARTBEAT_BATCH_INTERVAL,
                    handles[i].clone(),
                )
            })
            .collect();
        for gi in 0..self.g {
            let stream = STREAM0 + gi as u64;
            let replicas = NODES
                .iter()
                .enumerate()
                .map(|(i, &id)| {
                    RaftKvNode::start_hosted_with_batcher(
                        self.sim.env(nid(id)),
                        NODES.iter().copied().map(nid).collect(),
                        MemoryEngine::new(),
                        StorageScope::whole(),
                        stream,
                        Some(batchers[i].clone()),
                    )
                })
                .collect();
            self.nodes.push(replicas);
        }
    }

    /// Give every node the CPU cost that makes steady demand `load` x
    /// capacity. `load == 0` removes the cost.
    fn set_load(&self, load: f64) {
        for &id in &NODES {
            // Random service order (see the module doc): a FIFO backlog is a
            // uniformly slow clock and never false-triggers a follower.
            self.sim.set_cpu_order(nid(id), CpuOrder::Random);
            let cost = if load <= 0.0 {
                Duration::ZERO
            } else {
                let demand = EVENTS_PER_GROUP_PER_S * self.g as f64;
                Duration::from_secs_f64(load / demand)
            };
            self.sim.set_cpu_cost(nid(id), cost);
        }
    }

    fn led(&self) -> usize {
        self.nodes
            .iter()
            .filter(|rs| rs.iter().any(KvNode::is_leader))
            .count()
    }

    /// Sum over groups of `max replica term - 1` (the term churn), and the
    /// largest single term.
    fn churn(&self) -> (u64, u64) {
        let mut sum = 0;
        let mut max = 0;
        for rs in &self.nodes {
            let t = rs.iter().map(KvNode::term).max().unwrap_or(0);
            sum += t.saturating_sub(1);
            max = max.max(t);
        }
        (sum, max)
    }
}

// ---------------------------------------------------------------------------
// Sampling and the oracle.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct Sample {
    /// Virtual seconds since the run started.
    at_s: f64,
    led: usize,
    churn_sum: u64,
    max_term: u64,
}

/// Watches the run: a time series plus the safety oracle's state.
#[derive(Default)]
struct Sampler {
    series: Vec<Sample>,
    /// `(group, term) -> replica indices seen leading in that term`.
    leaders: BTreeMap<(usize, u64), BTreeSet<usize>>,
    /// First sample at which every group was led.
    all_led_at_s: Option<f64>,
    /// Lowest led count after `all_led_at_s`.
    min_led_after_convergence: Option<usize>,
}

impl Sampler {
    fn sample(&mut self, rig: &Rig, at_s: f64) {
        let led = rig.led();
        let (churn_sum, max_term) = rig.churn();
        for (gi, rs) in rig.nodes.iter().enumerate() {
            for (ri, r) in rs.iter().enumerate() {
                if r.is_leader() {
                    self.leaders.entry((gi, r.term())).or_default().insert(ri);
                }
            }
        }
        if self.all_led_at_s.is_none() && led == rig.g {
            self.all_led_at_s = Some(at_s);
        }
        if self.all_led_at_s.is_some() {
            self.min_led_after_convergence =
                Some(self.min_led_after_convergence.map_or(led, |m| m.min(led)));
        }
        self.series.push(Sample {
            at_s,
            led,
            churn_sum,
            max_term,
        });
    }

    /// `(group, term)` pairs that had two different leaders.
    fn split_brain(&self) -> Vec<(usize, u64)> {
        self.leaders
            .iter()
            .filter(|(_, rs)| rs.len() > 1)
            .map(|(k, _)| *k)
            .collect()
    }
}

/// What one run observed.
#[derive(Debug)]
struct Report {
    groups: usize,
    /// Virtual seconds until every group was first led (None = never).
    time_to_all_led_s: Option<f64>,
    led_at_end: usize,
    /// Mean `max_term - 1` per group at the end.
    churn_per_group: f64,
    max_term: u64,
    /// Groups short of full after the first full convergence (0 = monotone).
    led_drop: usize,
    split_brain: Vec<(usize, u64)>,
    acked: usize,
    lost_acked: usize,
}

/// Oracle thresholds. One struct so a later step can state a cell-specific
/// ceiling next to the cell.
#[derive(Clone, Copy)]
struct Limits {
    /// Seconds for every group to get a leader.
    all_led_s: f64,
    /// Mean `max_term - 1` per group.
    churn_per_group: f64,
}

impl Limits {
    const DEFAULT: Limits = Limits {
        all_led_s: 10.0,
        churn_per_group: 0.25,
    };

    /// The clauses of the oracle the report breaks; empty = oracle passes.
    /// `safety` clauses are listed first and are never expected to fail.
    fn violations(&self, r: &Report) -> Vec<&'static str> {
        let mut v = Vec::new();
        if !r.split_brain.is_empty() {
            v.push("safety: two leaders in one term");
        }
        if r.lost_acked > 0 {
            v.push("safety: an acked write is missing on a replica");
        }
        match r.time_to_all_led_s {
            None => v.push("liveness: groups never all led"),
            Some(t) if t > self.all_led_s => v.push("liveness: time to all led above bound"),
            Some(_) => {}
        }
        if r.led_at_end < r.groups {
            v.push("liveness: groups leaderless at the end");
        }
        if r.churn_per_group > self.churn_per_group {
            v.push("churn: terms per group above ceiling");
        }
        if r.acked == 0 {
            v.push("harness: no write was ever acked, the durability check is vacuous");
        }
        if r.led_drop > 0 {
            v.push("monotone: led count fell after first convergence");
        }
        v
    }
}

fn is_safety(clause: &str) -> bool {
    clause.starts_with("safety")
}

// ---------------------------------------------------------------------------
// Writes: committed-write durability (the second half of the safety oracle).
// ---------------------------------------------------------------------------

/// Writes proposed through current leaders: `(group, replica, key, index,
/// term)`, promoted to `acked` once the proposer applied them in that term.
#[derive(Default)]
struct Writes {
    pending: Vec<(usize, usize, Vec<u8>, u64, u64)>,
    acked: Vec<(usize, Vec<u8>)>,
    next: u64,
}

impl Writes {
    /// Propose one write on every `every`-th group's current leader.
    fn propose(&mut self, rig: &Rig, every: usize) {
        for gi in (0..rig.g).step_by(every) {
            let Some((ri, n)) = rig.nodes[gi]
                .iter()
                .enumerate()
                .find(|(_, n)| n.is_leader())
            else {
                continue;
            };
            let key = format!("w{:06}", self.next).into_bytes();
            self.next += 1;
            if let ProposeResult::Accepted { index, term } = n.put(key.clone(), b"v".to_vec()) {
                self.pending.push((gi, ri, key, index, term));
            }
        }
    }

    fn settle(&mut self, rig: &Rig) {
        for (gi, ri, key, index, term) in std::mem::take(&mut self.pending) {
            let n = &rig.nodes[gi][ri];
            if n.term() == term && n.engine_applied_index() >= index {
                self.acked.push((gi, key));
            } else if n.term() == term {
                self.pending.push((gi, ri, key, index, term));
            }
        }
    }

    /// Acked keys absent from some replica's engine.
    fn lost(&self, rig: &Rig) -> usize {
        self.acked
            .iter()
            .filter(|(gi, key)| {
                rig.nodes[*gi].iter().any(|n| {
                    let phys = n.physical_key(KIND_BASE, key);
                    block_on(n.storage().get(&phys))
                        .expect("engine read")
                        .is_none()
                })
            })
            .count()
    }
}

// ---------------------------------------------------------------------------
// Running a cell.
// ---------------------------------------------------------------------------

/// Virtual time of the unloaded convergence phase of `SustainedOverload`.
const CONVERGE: Duration = Duration::from_secs(5);
/// Virtual time of the overload (ADR 0078 cell 5: 60 s).
const OVERLOAD: Duration = Duration::from_secs(60);
/// Virtual time after the load is removed, for the "no group permanently
/// leaderless once the fault clears" clause.
const RELIEF: Duration = Duration::from_secs(15);
/// Cold start observation window.
const COLD_WINDOW: Duration = Duration::from_secs(20);

fn run_cell(c: &Cell, g: usize) -> Report {
    let mut rig = Rig::new(c.seed, g);
    let mut s = Sampler::default();
    let mut w = Writes::default();
    let mut at = Duration::ZERO;

    let mut advance = |rig: &mut Rig, s: &mut Sampler, w: &mut Writes, d: Duration, write: bool| {
        let end = at + d;
        while at < end {
            rig.sim.run_for(SAMPLE);
            at += SAMPLE;
            s.sample(rig, at.as_secs_f64());
            if write {
                w.settle(rig);
            }
        }
    };

    match c.kind {
        Kind::ColdStart => {
            rig.set_load(c.load);
            rig.host(c.mech);
            advance(&mut rig, &mut s, &mut w, COLD_WINDOW, false);
            w.propose(&rig, 10);
            advance(&mut rig, &mut s, &mut w, Duration::from_secs(5), true);
        }
        Kind::SustainedOverload => {
            rig.host(c.mech);
            advance(&mut rig, &mut s, &mut w, CONVERGE, false);
            w.propose(&rig, 10);
            advance(&mut rig, &mut s, &mut w, Duration::from_secs(2), true);
            rig.set_load(c.load);
            advance(&mut rig, &mut s, &mut w, OVERLOAD, true);
            rig.set_load(0.0);
            advance(&mut rig, &mut s, &mut w, RELIEF, true);
        }
    }

    let (churn_sum, max_term) = rig.churn();
    let report = Report {
        groups: g,
        time_to_all_led_s: s.all_led_at_s,
        led_at_end: rig.led(),
        churn_per_group: churn_sum as f64 / g as f64,
        max_term,
        led_drop: s
            .min_led_after_convergence
            .map_or(0, |m| g.saturating_sub(m)),
        split_brain: s.split_brain(),
        acked: w.acked.len(),
        lost_acked: w.lost(&rig),
    };
    if std::env::var("ANIMUS_ELECTION_BP_TRACE").is_ok() {
        for x in s.series.iter().step_by(4) {
            eprintln!(
                "BP {} t={:.1} led={} churn_sum={} max_term={}",
                c.name, x.at_s, x.led, x.churn_sum, x.max_term
            );
        }
    }
    drop(rig.nodes);
    rig.sim.shutdown();
    report
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

/// Every cell whose oracle must pass.
#[test]
fn election_backpressure_corpus_cells_pass_the_oracle() {
    for c in select(cells()) {
        let r = run_cell(&c, groups());
        let v = Limits::DEFAULT.violations(&r);
        eprintln!("BP-CELL {} seed={:#x} {r:?}", c.name, c.seed);
        assert!(v.is_empty(), "{} seed={:#x}: {v:?} {r:?}", c.name, c.seed);
    }
}

/// NEGATIVE CONTROL (a): with no mechanism, sustained overload (1.25x) must
/// collapse (ADR 0078, issue #1199), and the oracle must say so. Safety must
/// still hold: the collapse is a liveness failure, never a durability one.
#[test]
fn control_a_no_mechanisms_reproduces_the_collapse() {
    for c in select(controls()) {
        let r = run_cell(&c, groups());
        let v = Limits::DEFAULT.violations(&r);
        eprintln!("BP-CONTROL {} seed={:#x} {v:?} {r:?}", c.name, c.seed);
        assert!(
            v.iter().all(|x| !is_safety(x)),
            "{} seed={:#x}: safety must hold even in the collapse: {v:?} {r:?}",
            c.name,
            c.seed
        );
        assert!(
            v.contains(&"monotone: led count fell after first convergence"),
            "{} seed={:#x}: the led count must fall under overload: {v:?} {r:?}",
            c.name,
            c.seed
        );
        assert!(
            r.max_term > 3,
            "{} seed={:#x}: terms must run away: {r:?}",
            c.name,
            c.seed
        );
        assert!(
            v.contains(&"churn: terms per group above ceiling"),
            "{} seed={:#x}: term churn must exceed the ceiling: {v:?} {r:?}",
            c.name,
            c.seed
        );
    }
}

/// The calibration constant still matches the rig.
#[test]
fn calibration_matches_measurement() {
    let g = 100;
    let mut rig = Rig::new(corpus::name_seed("election_bp_calibration"), g);
    rig.host(Mechanisms::NONE);
    rig.sim.run_for(Duration::from_secs(10));
    assert_eq!(rig.led(), g, "calibration needs a converged rig");
    let before = rig.sim.stats();
    let window = 20.0;
    rig.sim.run_for(Duration::from_secs_f64(window));
    let after = rig.sim.stats();
    let per_node_per_group =
        (after.timer_fires - before.timer_fires) as f64 / window / NODES.len() as f64 / g as f64;
    eprintln!("BP-CALIBRATION events/group/node/s = {per_node_per_group:.4}");
    drop(rig.nodes);
    rig.sim.shutdown();
    assert!(
        (per_node_per_group / EVENTS_PER_GROUP_PER_S - 1.0).abs() < 0.15,
        "EVENTS_PER_GROUP_PER_S is stale: measured {per_node_per_group:.4}"
    );
}

#[test]
fn names_and_seeds_are_unique_and_frozen() {
    let all: Vec<Cell> = cells().into_iter().chain(controls()).collect();
    let names: BTreeSet<&str> = all.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names.len(), all.len());
    for c in &all {
        assert_eq!(c.seed, corpus::name_seed(&c.name));
    }
}

// ---------------------------------------------------------------------------
// Cell 12: the mechanisms-off path is byte-for-byte the pre-change behaviour.
// ---------------------------------------------------------------------------

/// A compact fingerprint of one small run: the simulator's event counts and,
/// per group, `(leader replica, term)`. Pinned below; later steps (governor,
/// lag stretch, staggering) must leave it unchanged while their knobs are off
/// (ADR 0078, cell 12).
fn digest(load: f64) -> (u64, u64, Vec<(usize, u64)>) {
    let g = 30;
    let mut rig = Rig::new(corpus::name_seed("election_bp_cell_12"), g);
    rig.host(Mechanisms::NONE);
    rig.set_load(load);
    rig.sim.run_for(Duration::from_secs(8));
    let st = rig.sim.stats();
    let per_group = rig
        .nodes
        .iter()
        .map(|rs| {
            let lead = rs.iter().position(KvNode::is_leader).unwrap_or(usize::MAX);
            let term = rs.iter().map(KvNode::term).max().unwrap_or(0);
            (lead, term)
        })
        .collect();
    drop(rig.nodes);
    rig.sim.shutdown();
    (st.task_polls, st.timer_fires, per_group)
}

fn fold(d: &(u64, u64, Vec<(usize, u64)>)) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325_u64;
    let mut mix = |x: u64| {
        h ^= x;
        h = h.wrapping_mul(0x0100_0000_01b3);
    };
    mix(d.0);
    mix(d.1);
    for (l, t) in &d.2 {
        mix(*l as u64);
        mix(*t);
    }
    h
}

#[test]
fn cell_12_mechanisms_off_digest_is_pinned() {
    for (load, pinned) in [(0.0, PINNED_IDLE), (0.9, PINNED_LOADED)] {
        let a = digest(load);
        let b = digest(load);
        assert_eq!(a, b, "load {load}: the run is not deterministic");
        let h = fold(&a);
        eprintln!("BP-DIGEST load={load} {h:#x} polls={} fires={}", a.0, a.1);
        assert_eq!(
            h, pinned,
            "load {load}: the mechanisms-off path changed ({a:?})"
        );
    }
}

const PINNED_IDLE: u64 = 0x889e0d5bdd9161f8;
const PINNED_LOADED: u64 = 0x4a3916c2f8d0bef1;
