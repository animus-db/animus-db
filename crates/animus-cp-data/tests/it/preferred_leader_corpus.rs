//! ADR 0075 section 3.3 / 3.6 (G-01 stage G-c, M2): a seed-reproducible
//! `SimEnv` corpus for the **preferred-leader mechanism** of the tablet-host
//! reconciler (`host::Reconciler::preferred_leader_step`).
//!
//! Same world as `wan_timing_corpus`: three regions, one node each, every
//! inter-region link 60-90 ms one-way with a jitter and a heavy tail, one
//! tablet replicated on all three, hosted by the real per-node
//! `host::Reconciler` fed a `MetadataView` (`regions` + `preferred_leader`).
//! The reconciler is ticked every 500 ms of virtual time (the production loop
//! ticks on a metadata change and on a fallback interval).
//!
//! Every cell first lets the group elect whoever it elects, **then sets the
//! preferred region to a region that does NOT hold the leader** (the moral
//! equivalent of an operator's `SetGlobalPreferredLeader`), so a transfer is
//! genuinely required. Convergence is a **converged-or-timeout poll** on "the
//! leader is in the preferred region", never a fixed-deadline assert.
//!
//! Cells (seed-expanded by `ANIMUS_MRSC_SEEDS`; `ANIMUS_MRSC_CELL=<substring>`
//! narrows, `ANIMUS_SEED=<seed>` replays one):
//! - `steady_converges_to_preferred`: the leader moves to the preferred region
//!   and stays there (no flapping, bounded transfers, writes ack).
//! - `leader_region_kill_then_return`: the preferred (leader) node is killed
//!   and later restarted; the leader returns to the preferred region.
//! - `preferred_region_partition_heal`: the preferred region is partitioned
//!   away and healed; the leader returns.
//! - `witness_region_never_leads`: the leader sits in the witness region while
//!   the preferred region is down; it is transferred to the other full replica,
//!   never left in the witness region, and returns to the preferred region
//!   when it is back.
//! - `lagging_preferred_under_write_load`: the preferred node's links are slow
//!   under a continuous writer; the transfer arm is refused/retried but never
//!   stalls the group, and it converges.
//! - `flapping_link_no_flap`: a link of the preferred node flaps; the
//!   stability window and the per-group minimum interval bound the transfers.
//!
//! Oracles: every acked write is durable on every replica and the replicas
//! converge; the transfer count is bounded; a witness region never keeps the
//! leader in steady state.
//!
//! **Negative control** (`no_step_leaves_leader_outside_preferred`): the
//! identical scripts with the reconciler fed an *empty* `preferred_leader`
//! (the step has nothing to act on) must leave the leader outside the
//! preferred region on every seed. If this ever passes, the positive cells
//! prove nothing.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::host::{LeaderPreference, MemoryTabletEngines, MetadataView, Reconciler};
use animus_env::{Env, Metric, NodeId, nid};
use animus_sim::{NetConfig, SimEnv, Simulator};
use animus_storage::{MemoryEngine, StorageEngine};
use animus_tablet::{KeyRange, Tablet, TabletId};
use animus_test::corpus::{self, SeedVariant};
use futures::executor::block_on;

type Recon = Reconciler<SimEnv, MemoryEngine>;

const TABLE: &str = "t";
const TABLET: TabletId = TabletId(1);
const MAX_REGION_RTT: Duration = Duration::from_millis(200);
const TICK: Duration = Duration::from_millis(500);
/// Convergence budget of every poll (virtual time). The WAN election base is
/// 1 s, the stability window 2 s and the minimum interval 10 s, so a healthy
/// convergence takes seconds; this leaves a wide margin for re-elections over
/// the heavy-tailed links.
const CONVERGE_BUDGET: Duration = Duration::from_secs(120);
const JITTER: Duration = Duration::from_millis(40);
const TAIL_JITTER: Duration = Duration::from_millis(400);
const TAIL_PROB: f64 = 0.30;
/// Bound on armed transfers over one whole script (all three nodes' sum): one
/// to reach the preferred region, one to return after a fault, one spare.
const MAX_TRANSFERS: u64 = 4;
/// The flapping-link cell's bound: the per-group minimum interval (10 election
/// timeouts = 10 s) caps each node at one transfer per interval.
const MAX_TRANSFERS_FLAPPING: u64 = 8;

fn node(i: usize) -> NodeId {
    nid(800 + i as u64)
}
fn region_of(i: usize) -> String {
    ["region-a", "region-b", "region-c"][i].to_string()
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    /// The view carries the preferred-leader map: the step is active.
    Step,
    /// Negative control: the view's `preferred_leader` stays empty.
    NoStep,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Script {
    Steady,
    KillPreferredReturn,
    PartitionPreferredHeal,
    WitnessFallback,
    LaggingPreferred,
    FlappingLink,
}

struct World {
    sim: Simulator,
    mode: Mode,
    engines: Vec<MemoryTabletEngines>,
    recons: Vec<Option<Recon>>,
    alive: [bool; 3],
    isolated: Option<usize>,
    /// What the control plane's failure detector reports `Down`.
    down: BTreeSet<NodeId>,
    /// The table's current leader preference (what the view advertises when the
    /// step is active).
    pref: Option<LeaderPreference>,
    pending: Vec<(usize, Vec<u8>, u64, u64)>,
    acked: Vec<Vec<u8>>,
    next_key: u64,
    write_every_tick: bool,
    /// Opt every hosted group into quiescence (ADR 0048).
    quiesce: bool,
    /// Every sampled (leader region) in the order observed, for the witness
    /// oracle.
    leader_samples: Vec<Option<usize>>,
}

impl World {
    fn new(seed: u64, mode: Mode) -> World {
        Self::new_with(seed, mode, false, false)
    }

    fn new_with(seed: u64, mode: Mode, quiesce: bool, fast_links: bool) -> World {
        let sim = Simulator::new(seed);
        let one_way = [((0, 1), 60u64), ((0, 2), 75), ((1, 2), 90)];
        for ((i, j), ms) in one_way {
            let cfg = if fast_links {
                // A quiet, near-LAN link (the quiescence cell: see its doc).
                let mut cfg = NetConfig::default();
                cfg.base_delay = Duration::from_millis(1);
                cfg
            } else {
                Self::link(ms)
            };
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
            down: BTreeSet::new(),
            pref: None,
            pending: Vec::new(),
            acked: Vec::new(),
            next_key: 0,
            write_every_tick: false,
            quiesce,
            leader_samples: Vec::new(),
        };
        for i in 0..3 {
            w.start_node(i);
        }
        w
    }

    fn link(ms: u64) -> NetConfig {
        let mut cfg = NetConfig::default();
        cfg.base_delay = Duration::from_millis(ms);
        cfg.max_jitter = JITTER;
        cfg.heavy_tail_max_jitter = TAIL_JITTER;
        cfg.set_heavy_tail_prob(TAIL_PROB);
        cfg
    }

    fn view(&self) -> MetadataView {
        let replicas: Vec<NodeId> = (0..3).map(node).collect();
        let t = Tablet::new_for_table(TABLET, TABLE, KeyRange::new(Vec::new(), None), replicas);
        let regions: BTreeMap<NodeId, String> = (0..3).map(|i| (node(i), region_of(i))).collect();
        let preferred_leader = match (&self.pref, self.mode) {
            (Some(p), Mode::Step) => [(TABLET, p.clone())].into_iter().collect(),
            _ => BTreeMap::new(),
        };
        MetadataView {
            tablets: [(t.id, t)].into_iter().collect(),
            down: self.down.clone(),
            regions,
            preferred_leader,
        }
    }

    fn start_node(&mut self, i: usize) {
        let mut r: Recon = Reconciler::new(
            self.sim.env(node(i)),
            self.engines[i].clone(),
            node(i),
            |_t, _n| {},
            |_t| {},
        );
        r.set_max_region_rtt(MAX_REGION_RTT);
        if self.quiesce {
            r.enable_quiescence(Duration::from_secs(2));
        }
        block_on(r.tick(&self.view()));
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

    /// The believed leader with the highest term among live, non-isolated nodes.
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

    fn transfers(&self) -> u64 {
        (0..3)
            .map(|i| {
                self.sim
                    .env(node(i))
                    .metrics()
                    .get(Metric::CpPreferredLeaderTransfers)
            })
            .sum()
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

    fn settle_acks(&mut self) {
        let mut still = Vec::new();
        for (i, key, index, term) in std::mem::take(&mut self.pending) {
            match self.handle(i) {
                Some(h) if h.term() == term && h.engine_applied_index() >= index => {
                    self.acked.push(key);
                }
                Some(h) if h.term() == term => still.push((i, key, index, term)),
                _ => {}
            }
        }
        self.pending = still;
    }

    /// One tick of virtual time: tick every live reconciler, optionally write,
    /// advance, settle acks.
    fn step(&mut self, write: bool) {
        let view = self.view();
        for i in 0..3 {
            if self.alive[i]
                && let Some(r) = self.recons[i].as_mut()
            {
                block_on(r.tick(&view));
            }
        }
        if write || self.write_every_tick {
            self.write_one();
        }
        self.sim.run_for(TICK);
        self.settle_acks();
        self.leader_samples.push(self.leader());
    }

    fn run(&mut self, total: Duration, write: bool) {
        let mut elapsed = Duration::ZERO;
        while elapsed < total {
            self.step(write);
            elapsed += TICK;
        }
    }

    /// Converged-or-timeout poll: step (writing) until `cond` holds, up to
    /// `CONVERGE_BUDGET`. Returns whether it converged.
    fn poll(&mut self, cond: impl Fn(&World) -> bool) -> bool {
        let mut elapsed = Duration::ZERO;
        while elapsed < CONVERGE_BUDGET {
            if cond(self) {
                return true;
            }
            self.step(true);
            elapsed += TICK;
        }
        cond(self)
    }

    fn leader_in(&self, region: usize) -> bool {
        self.leader() == Some(region)
    }

    fn kill(&mut self, i: usize) {
        self.sim.stop(node(i));
        self.alive[i] = false;
        self.recons[i] = None;
        self.down.insert(node(i));
    }

    fn restart(&mut self, i: usize) {
        self.down.remove(&node(i));
        self.start_node(i);
    }

    fn partition(&mut self, i: usize) {
        for o in (0..3).filter(|o| *o != i) {
            self.sim.partition_pair(node(i), node(o));
        }
        self.isolated = Some(i);
        self.down.insert(node(i));
    }

    fn heal(&mut self, i: usize) {
        for o in (0..3).filter(|o| *o != i) {
            self.sim.heal(node(i), node(o));
        }
        self.isolated = None;
        self.down.remove(&node(i));
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

#[derive(Debug)]
struct Report {
    /// The witness script only: the group really did sit in the witness region
    /// first (so the fallback path was exercised, not skipped), and the witness
    /// replica declined replica-local eventual reads.
    witness_exercised: bool,
    /// The leader reached the preferred region within the budget.
    converged: bool,
    /// After convergence, the leader stayed there for the whole settle window
    /// with no term change.
    stayed: bool,
    transfers: u64,
    /// The leader was in the witness region at some sample after the final
    /// convergence (witness script only).
    witness_led_after_convergence: bool,
    acked_total: usize,
    all_acked_durable: bool,
    replicas_converged: bool,
}

fn run_script(seed: u64, mode: Mode, script: Script) -> Report {
    let mut w = World::new(seed, mode);
    // Initial election, whoever wins; the step is inert (no preference yet).
    w.run(Duration::from_secs(25), false);
    w.run(Duration::from_secs(6), true);
    let first = w.leader().expect("a leader after the initial election");

    // The preferred region: never the elected leader's. The witness script
    // wants a fixed shape (preferred a, witness c) and drives the leader into
    // the witness region first.
    let (preferred, witness) = match script {
        Script::WitnessFallback => (0usize, Some(2usize)),
        _ => ((first + 1 + (seed & 1) as usize) % 3, None),
    };
    let pref_of = |p: usize, wit: Option<usize>| LeaderPreference {
        region: region_of(p),
        witness: wit.map(region_of),
    };

    if script == Script::WitnessFallback {
        // Phase 1: steer the leader into region c (no witness yet).
        w.pref = Some(pref_of(2, None));
        let in_witness = w.poll(|w| mode == Mode::NoStep || w.leader_in(2));
        // Phase 2: c becomes the witness, a the preferred region, and a dies:
        // the witness-region leader must be transferred to b (the only other
        // full replica) and a must get it back after it returns.
        w.pref = Some(pref_of(0, Some(2)));
        w.kill(0);
        let _ = w.poll(|w| w.leader_in(1) || mode == Mode::NoStep && w.leader().is_some());
        w.run(Duration::from_secs(10), true);
        w.leader_samples.clear();
        w.run(Duration::from_secs(15), true);
        let witness_led_steady = w.leader_samples.contains(&Some(2));
        let declines = w
            .handle(2)
            .is_some_and(|h| h.is_witness() && !h.stale_read_ready());
        let serves_elsewhere = w.handle(1).is_some_and(|h| !h.is_witness());
        w.restart(0);
        let converged = w.poll(|w| w.leader_in(0));
        let mut r = finish(w, converged, witness_led_steady);
        r.witness_exercised = in_witness && declines && serves_elsewhere;
        return r;
    }

    w.pref = Some(pref_of(preferred, witness));
    let converged_first = w.poll(|w| w.leader_in(preferred));

    match script {
        Script::Steady => {}
        Script::KillPreferredReturn => {
            // (The control's preferred node is not the leader: killing it
            // still exercises the restart path.)
            if converged_first || mode == Mode::NoStep {
                w.kill(preferred);
                w.run(Duration::from_secs(15), true);
                w.restart(preferred);
            }
        }
        Script::PartitionPreferredHeal => {
            if converged_first {
                w.partition(preferred);
                w.run(Duration::from_secs(15), true);
                w.heal(preferred);
            }
        }
        Script::LaggingPreferred => {
            // Slow every link of the preferred node, with a writer every tick.
            w.write_every_tick = true;
            for o in (0..3).filter(|o| *o != preferred) {
                let mut cfg = World::link(90);
                cfg.max_jitter = Duration::from_millis(150);
                cfg.heavy_tail_max_jitter = Duration::from_millis(800);
                w.sim
                    .set_link_net_config(node(preferred), node(o), cfg.clone());
                w.sim.set_link_net_config(node(o), node(preferred), cfg);
            }
            w.run(Duration::from_secs(20), true);
        }
        Script::FlappingLink => {
            let other = (preferred + 1) % 3;
            for _ in 0..12 {
                w.sim.partition_pair(node(preferred), node(other));
                w.run(Duration::from_millis(1500), true);
                w.sim.heal(node(preferred), node(other));
                w.run(Duration::from_millis(3500), true);
            }
        }
        Script::WitnessFallback => unreachable!(),
    }
    let converged = w.poll(|w| w.leader_in(preferred));
    // Settle: the leader must stay put and the term must not move.
    let term = w.max_term();
    w.run(Duration::from_secs(20), true);
    let stayed = converged && w.leader_in(preferred) && w.max_term() == term;
    let mut r = finish(w, converged, false);
    r.stayed = stayed;
    r
}

fn finish(mut w: World, converged: bool, witness_led: bool) -> Report {
    // Quiesce: no writes, let every replica converge.
    w.write_every_tick = false;
    w.run(Duration::from_secs(25), false);
    let sets: Vec<BTreeSet<Vec<u8>>> = (0..3).map(|i| w.key_set(i)).collect();
    Report {
        witness_exercised: false,
        converged,
        stayed: converged,
        transfers: w.transfers(),
        witness_led_after_convergence: witness_led,
        acked_total: w.acked.len(),
        all_acked_durable: w.acked.iter().all(|k| sets.iter().all(|s| s.contains(k))),
        replicas_converged: sets.windows(2).all(|p| p[0] == p[1]),
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
    script: Script,
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

fn cell(name: &str, mode: Mode, script: Script) -> Cell {
    Cell {
        name: name.to_string(),
        seed: corpus::name_seed(name),
        mode,
        script,
    }
}

/// Depth knob (`ANIMUS_MRSC_SEEDS`, default 1).
fn seeds_per_cell() -> usize {
    corpus::seeds_from_env("ANIMUS_MRSC_SEEDS")
}

/// `ANIMUS_MRSC_CELL=<substring>` narrows; `ANIMUS_SEED=<seed>` replays one.
fn select(cells: Vec<Cell>, k: usize) -> Vec<Cell> {
    let filter = std::env::var("ANIMUS_MRSC_CELL").ok();
    let replay = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.trim_start_matches("0x").parse::<u64>().ok());
    corpus::seed_expand(cells, k)
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

fn positive_cells() -> Vec<Cell> {
    vec![
        cell("steady_converges_to_preferred", Mode::Step, Script::Steady),
        cell(
            "leader_region_kill_then_return",
            Mode::Step,
            Script::KillPreferredReturn,
        ),
        cell(
            "preferred_region_partition_heal",
            Mode::Step,
            Script::PartitionPreferredHeal,
        ),
        cell(
            "witness_region_never_leads",
            Mode::Step,
            Script::WitnessFallback,
        ),
        cell(
            "lagging_preferred_under_write_load",
            Mode::Step,
            Script::LaggingPreferred,
        ),
        cell("flapping_link_no_flap", Mode::Step, Script::FlappingLink),
    ]
}

fn negative_cells() -> Vec<Cell> {
    vec![
        cell(
            "no_step_leaves_leader_outside_preferred_steady",
            Mode::NoStep,
            Script::Steady,
        ),
        cell(
            "no_step_leaves_leader_outside_preferred_kill",
            Mode::NoStep,
            Script::KillPreferredReturn,
        ),
    ]
}

#[test]
fn preferred_leader_converges_stays_and_stays_bounded() {
    let observed = Arc::new(Mutex::new(Vec::new()));
    for c in select(positive_cells(), seeds_per_cell()) {
        let r = run_script(c.seed, c.mode, c.script);
        observed
            .lock()
            .unwrap()
            .push((c.name.clone(), format!("{r:?}")));
        let ctx = format!("{} seed={:#x} {r:?}", c.name, c.seed);
        assert!(
            r.converged,
            "the leader never reached the preferred region: {ctx}"
        );
        assert!(r.stayed, "the leader flapped after converging: {ctx}");
        let bound = if c.script == Script::FlappingLink {
            MAX_TRANSFERS_FLAPPING
        } else {
            MAX_TRANSFERS
        };
        assert!(
            r.transfers >= 1 && r.transfers <= bound,
            "armed transfers out of bounds (1..={bound}): {ctx}"
        );
        assert!(
            !r.witness_led_after_convergence,
            "the witness region kept the leader in steady state: {ctx}"
        );
        if c.script == Script::WitnessFallback {
            assert!(
                r.witness_exercised,
                "the witness cell never put the leader in the witness region, or the \
                 witness replica served an eventual read: {ctx}"
            );
        }
        assert!(r.acked_total > 0, "nothing was ever acked: {ctx}");
        assert!(r.all_acked_durable, "an acked write was lost: {ctx}");
        assert!(r.replicas_converged, "replicas diverged: {ctx}");
    }
    for (n, r) in observed.lock().unwrap().iter() {
        eprintln!("MRSC-LEADER {n}: {r}");
    }
}

/// NEGATIVE CONTROL: with the step fed nothing (empty `preferred_leader`) the
/// leader must stay outside the preferred region on **every** seed — the
/// scripts always pick a preferred region that does not hold the leader.
#[test]
fn no_step_leaves_leader_outside_preferred() {
    for c in select(negative_cells(), seeds_per_cell()) {
        let r = run_script(c.seed, c.mode, c.script);
        eprintln!("MRSC-LEADER-NEG {} seed={:#x} {r:?}", c.name, c.seed);
        assert_eq!(
            r.transfers, 0,
            "the control must not transfer anything: {} {r:?}",
            c.name
        );
        assert!(
            !r.converged,
            "with no step the leader reached the preferred region on its own, so the \
             positive cells prove nothing: {} seed={:#x} {r:?}",
            c.name, c.seed
        );
        // Safety is untouched: the control fails on placement only.
        assert!(
            r.all_acked_durable && r.replicas_converged,
            "{}: {r:?}",
            c.name
        );
    }
}

/// ADR 0048: the step must not fight quiescence. Runs on quiet 1 ms links
/// because, as built, a quiesced leader is woken by *any* inbound message and
/// on links whose round trip exceeds the heartbeat interval the acks still in
/// flight at the quiesce instant defeat quiescence on every settle (a
/// pre-existing interaction of ADR 0048 with the WAN profile, independent of
/// this step; issue #1226). An idle group whose leader is
/// already in the preferred region stays asleep (the step never pokes it); an
/// idle group whose leader is misplaced is moved by exactly one transfer (the
/// only thing that wakes it), then falls asleep again.
#[test]
fn the_step_does_not_wake_a_correctly_placed_idle_group_and_moves_a_misplaced_one() {
    for c in select(
        vec![cell("quiesced_idle_group", Mode::Step, Script::Steady)],
        seeds_per_cell(),
    ) {
        let mut w = World::new_with(c.seed, Mode::Step, true, true);
        w.run(Duration::from_secs(25), false);
        w.run(Duration::from_secs(6), true);
        let first = w.leader().expect("a leader");
        // Go idle until the group quiesces.
        w.run(Duration::from_secs(30), false);
        let asleep = |w: &World| w.handle(first).is_some_and(|h| h.is_quiesced());
        assert!(
            asleep(&w),
            "seed={:#x}: the idle group never quiesced",
            c.seed
        );

        // Preferred == the leader's region: nothing to do, nothing wakes.
        w.pref = Some(LeaderPreference {
            region: region_of(first),
            witness: None,
        });
        w.run(Duration::from_secs(60), false);
        assert_eq!(w.transfers(), 0, "seed={:#x}", c.seed);
        assert!(
            asleep(&w),
            "seed={:#x}: the step woke a correctly placed idle group",
            c.seed
        );
        assert_eq!(w.leader(), Some(first), "seed={:#x}", c.seed);

        // Preferred != the leader's region: one transfer, then asleep again.
        let target = (first + 1) % 3;
        w.pref = Some(LeaderPreference {
            region: region_of(target),
            witness: None,
        });
        let mut elapsed = Duration::ZERO;
        while elapsed < CONVERGE_BUDGET && !w.leader_in(target) {
            w.step(false);
            elapsed += TICK;
        }
        assert!(
            w.leader_in(target),
            "seed={:#x}: the idle misplaced leader never moved",
            c.seed
        );
        assert_eq!(w.transfers(), 1, "seed={:#x}", c.seed);
        w.run(Duration::from_secs(60), false);
        assert_eq!(w.transfers(), 1, "seed={:#x}: flapped", c.seed);
        assert!(
            w.handle(target).is_some_and(|h| h.is_quiesced()),
            "seed={:#x}: the group never went back to sleep",
            c.seed
        );
    }
}

#[test]
fn mrsc_corpus_names_and_seeds_are_unique_and_frozen() {
    let all: Vec<Cell> = positive_cells()
        .into_iter()
        .chain(negative_cells())
        .collect();
    let names: BTreeSet<&str> = all.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names.len(), all.len());
    let seeds: BTreeSet<u64> = all.iter().map(|c| c.seed).collect();
    assert_eq!(seeds.len(), all.len());
    for c in &all {
        assert_eq!(c.seed, corpus::name_seed(&c.name));
    }
    assert_eq!(
        corpus::seed_expand(positive_cells(), 3).len(),
        positive_cells().len() * 3
    );
}
