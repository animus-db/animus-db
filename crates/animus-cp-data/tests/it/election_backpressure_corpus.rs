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
//!
//! **ADR step 3 (campaign governor)** adds the governor to [`Mechanisms`] and
//! cells 1 (now with the governor), 4 (leader-node loss with every group
//! quiesced), 7 (one-node partition and heal), 8 (a leadership transfer while
//! the governor is saturated), 10 (the lost-leader bound, asserted literally)
//! and 11 (learners, single-voter and quiesced groups take no permits), plus
//! negative controls (c) (the governor also gating the responder side) and (d)
//! (permits that are never returned). A "ghost" is a harness client of a node's
//! governor that behaves like a group that cannot win: it holds a permit for a
//! full `2 * election_base`, drops it and asks again (so it re-queues at the
//! back). Ghosts are how cells 8 and 10 saturate the governor deterministically.

//!
//! **ADR step 4 (load monitor, lag stretch)** adds `Mechanisms::stretch_x` and
//! cells 4 with all mechanisms, 4b (a node loss as the load ends, so the cap on
//! the stretch alone bounds the failover), 5 (sustained overload) and 6
//! (overload then relief), plus negative controls (b) (stretch off on cell 5),
//! (e) (`S_max = inf` on 4b, sharing 4b's seed) and `a6` (cell 6 without
//! mechanisms). The CPU-starvation model serves timers ahead of deliveries, so
//! the monitor's own timer-lateness probe reads ~0 here; the cells feed a
//! detached monitor from the simulator's queue ([`Rig::probe`]) and
//! `load_monitor_task_measures_a_paused_node` covers the real task.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::campaign_governor::{
    Admission, CampaignGovernor, CampaignWake, ElectionBackpressure, GovernorFault,
};
use animus_cp_data::heartbeat_batch::{DEFAULT_HEARTBEAT_BATCH_INTERVAL, HeartbeatBatcher};
use animus_cp_data::load_monitor::LoadMonitor;
use animus_cp_data::{HostedOptions, KIND_BASE, RaftKvNode, StorageScope};
use animus_env::{Clock, EnvExt, Metric, MetricsHandle, nid};
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

/// `K`, the campaign cap the cells run with (ADR 0078 default).
const K: usize = 32;
/// The election base of the LAN profile, as the groups run it.
const BASE: Duration = Duration::from_millis(150);

/// Which ADR 0078 mechanisms are on. Step 3 adds the campaign governor; step 4
/// adds the lag stretch and step 5 staggered hosting as fields here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Mechanisms {
    /// Campaign governor `K` (`0` = off).
    governor_k: usize,
    /// A deliberate governor bug (negative controls (c) and (d)).
    fault: GovernorFault,
    /// `lag_stretch_max_x` of the load monitor (`0` = off, the monitor is not
    /// even started).
    stretch_x: u64,
}

impl Mechanisms {
    /// Nothing on: the code as it was before ADR 0078.
    const NONE: Mechanisms = Mechanisms {
        governor_k: 0,
        fault: GovernorFault::None,
        stretch_x: 0,
    };
    /// The campaign governor at the default `K`.
    const GOVERNOR: Mechanisms = Mechanisms {
        governor_k: K,
        fault: GovernorFault::None,
        stretch_x: 0,
    };
    /// Governor plus lag stretch at the default `lag_stretch_max_x` (4): what
    /// `animusd` runs by default (ADR 0078 steps 3 and 4).
    const ALL: Mechanisms = Mechanisms {
        governor_k: K,
        fault: GovernorFault::None,
        stretch_x: 4,
    };
    /// Governor plus an effectively unbounded stretch (`S_max = inf`,
    /// negative control (e)).
    const UNBOUNDED_STRETCH: Mechanisms = Mechanisms {
        governor_k: K,
        fault: GovernorFault::None,
        stretch_x: 1_000_000,
    };
    /// The governor with a deliberate bug.
    const fn faulty(fault: GovernorFault) -> Mechanisms {
        Mechanisms {
            governor_k: K,
            fault,
            stretch_x: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    /// Cell 1: every group is hosted at once from a cold start, the node CPU
    /// already at `load` x steady demand.
    ColdStart,
    /// Cell 5: converge unloaded, then raise the cost to `load` x steady
    /// demand for [`OVERLOAD`] of virtual time.
    SustainedOverload,
    /// Cell 4: every group quiesced, then the node leading a third of them is
    /// killed and the survivors are woken together (the reconciler's `down`
    /// wake), at `load` x steady demand.
    LeaderNodeLoss,
    /// Cell 7: node 2 is cut off from the other two for [`PARTITION`], then
    /// healed. Its groups cannot win; the healthy pair's groups must not be
    /// slowed.
    PartitionHeal,
    /// Cell 8: the governor is saturated by ghosts, then one group's leader
    /// transfers leadership; the exempt `TimeoutNow` campaign must finish
    /// within one election timeout.
    TransferUnderSaturation,
    /// Cell 10: the governor has `Q` queued groups, then one group's leader
    /// is lost; the new leader must arrive within `2*base + ceil(Q/K)*2*base`.
    LostLeaderBound,
    /// Cell 4b (and control (e)): groups left active, the nodes pushed to
    /// `load` x steady demand until the load monitor reads seconds of lag, then
    /// the load ends and, in the same instant, the node leading a third of the
    /// groups is killed. The monitor's lag decays over seconds, so the
    /// followers of the dead node's groups still wait out a stretched election
    /// timeout, while the surviving nodes are no longer overloaded: the stretch
    /// cap is what bounds the failover, and nothing else is in play.
    LeaderLossUnderLag,
    /// Cell 6: converge, overload for [`OVERLOAD_SHORT`], then lower the cost;
    /// every group must be led again within the recovery bound without a
    /// restart.
    OverloadThenRelief,
    /// Cell 11: learners (non-voters), single-voter groups and quiesced groups
    /// never take a permit.
    NoPermits,
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
    vec![
        // Cell 1 without any mechanism: the baseline the governor's numbers
        // are read against (it passes the oracle at 0.9x on its own).
        cell(
            "cell_1_cold_start_no_governor",
            Kind::ColdStart,
            0.9,
            Mechanisms::NONE,
        ),
        cell(
            "cell_1_cold_start_all_at_once",
            Kind::ColdStart,
            0.9,
            Mechanisms::GOVERNOR,
        ),
        cell(
            "cell_4_leader_node_loss_quiesced",
            Kind::LeaderNodeLoss,
            0.9,
            Mechanisms::GOVERNOR,
        ),
        cell(
            "cell_4_leader_node_loss_quiesced_all_mechanisms",
            Kind::LeaderNodeLoss,
            0.9,
            Mechanisms::ALL,
        ),
        cell(
            "cell_4b_leader_node_loss_under_lag",
            Kind::LeaderLossUnderLag,
            1.25,
            Mechanisms::ALL,
        ),
        cell(
            "cell_5_sustained_overload",
            Kind::SustainedOverload,
            1.25,
            Mechanisms::ALL,
        ),
        cell(
            "cell_6_overload_then_relief",
            Kind::OverloadThenRelief,
            1.25,
            Mechanisms::ALL,
        ),
        cell(
            "cell_7_partition_and_heal",
            Kind::PartitionHeal,
            0.9,
            Mechanisms::GOVERNOR,
        ),
        cell(
            "cell_8_transfer_under_saturation",
            Kind::TransferUnderSaturation,
            0.0,
            Mechanisms::GOVERNOR,
        ),
        cell(
            "cell_10_lost_leader_bound",
            Kind::LostLeaderBound,
            0.0,
            Mechanisms::GOVERNOR,
        ),
        cell(
            "cell_11_no_permits_for_learners_single_voter_quiesced",
            Kind::NoPermits,
            0.0,
            Mechanisms::GOVERNOR,
        ),
    ]
}

/// Negative control (a): cells whose oracle must FAIL (it bites).
fn controls() -> Vec<Cell> {
    vec![
        cell(
            "control_a_no_mechanisms_sustained_overload",
            Kind::SustainedOverload,
            1.25,
            Mechanisms::NONE,
        ),
        // Cell 6's shape without mechanisms (reported alongside cell 6).
        cell(
            "control_a6_no_mechanisms_overload_then_relief",
            Kind::OverloadThenRelief,
            1.25,
            Mechanisms::NONE,
        ),
    ]
}

/// Negative control (b): the governor on, the lag stretch off, on cell 5.
fn controls_b() -> Vec<Cell> {
    vec![cell(
        "control_b_stretch_off_sustained_overload",
        Kind::SustainedOverload,
        1.25,
        Mechanisms::GOVERNOR,
    )]
}

/// Negative control (e): an unbounded stretch on the leader-node-loss cell.
fn controls_e() -> Vec<Cell> {
    let mut c = cell(
        "control_e_unbounded_stretch_leader_node_loss_under_lag",
        Kind::LeaderLossUnderLag,
        1.25,
        Mechanisms::UNBOUNDED_STRETCH,
    );
    // Same seed as cell 4b, so the two runs differ only in the stretch cap.
    c.seed = corpus::name_seed("cell_4b_leader_node_loss_under_lag");
    vec![c]
}

/// Negative control (c): admission also gating the responder side.
fn controls_c() -> Vec<Cell> {
    vec![cell(
        "control_c_governor_gates_the_responder_leader_node_loss",
        Kind::LeaderNodeLoss,
        0.9,
        Mechanisms::faulty(GovernorFault::RearmOnDefer),
    )]
}

/// Negative control (d): permits that are never returned (no RAII release).
fn controls_d() -> Vec<Cell> {
    vec![cell(
        "control_d_permit_leak_lost_leader_bound",
        Kind::LostLeaderBound,
        0.0,
        Mechanisms::faulty(GovernorFault::LeakPermits),
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

/// First stream id of the harness's ghost clients of a governor.
const GHOST0: u64 = 1_000_000;

struct Rig {
    sim: Simulator,
    /// `[group][replica]`; `placement[group][replica]` is its node index.
    nodes: Vec<Vec<KvNode>>,
    placement: Vec<Vec<usize>>,
    g: usize,
    handles: Vec<MetricsHandle>,
    batchers: Vec<HeartbeatBatcher<SimEnv>>,
    /// One governor per node (`None` = off).
    governors: Vec<Option<CampaignGovernor>>,
    /// One load monitor per node (`None` = lag stretch off).
    monitors: Vec<Option<LoadMonitor>>,
    /// The per-event CPU cost (ns) last set by [`Rig::set_load`], for the
    /// model-derived lag probe ([`Rig::probe`]).
    cost_ns: std::cell::Cell<u64>,
    /// Start the production probe task (`LoadMonitor::new`) instead of feeding
    /// the monitor from the simulator's CPU queue (only the idle-identity test
    /// wants it, see [`Rig::setup`]).
    real_probe: bool,
    /// Nodes that are neither killed nor cut off.
    live: [bool; 3],
    /// Replicas whose driver was halted (`shutdown`): their frozen handle
    /// must not count as a leader.
    halted: Vec<(usize, usize)>,
}

impl Rig {
    fn new(seed: u64, g: usize) -> Rig {
        Rig {
            sim: Simulator::new(seed),
            nodes: Vec::new(),
            placement: Vec::new(),
            g,
            handles: NODES.iter().map(|_| MetricsHandle::recording()).collect(),
            batchers: Vec::new(),
            governors: Vec::new(),
            monitors: Vec::new(),
            cost_ns: std::cell::Cell::new(0),
            real_probe: false,
            live: [true; 3],
            halted: Vec::new(),
        }
    }

    /// The per-node batchers and governors (ADR 0044 phase 2, ADR 0078).
    fn setup(&mut self, mech: Mechanisms) {
        self.batchers = NODES
            .iter()
            .enumerate()
            .map(|(i, &id)| {
                HeartbeatBatcher::new(
                    self.sim.env(nid(id)),
                    DEFAULT_HEARTBEAT_BATCH_INTERVAL,
                    self.handles[i].clone(),
                )
            })
            .collect();
        self.governors = (0..NODES.len())
            .map(|i| {
                (mech.governor_k > 0).then(|| {
                    CampaignGovernor::with_fault(
                        mech.governor_k,
                        self.handles[i].clone(),
                        mech.fault,
                    )
                })
            })
            .collect();
        self.monitors = (0..NODES.len())
            .map(|i| {
                // The corpus feeds the monitor from the simulator's CPU queue
                // ([`Rig::probe`]) rather than from a probe task: the CPU model
                // serves expired timers ahead of waiting deliveries, so the
                // timer-lateness probe of `LoadMonitor::new` reads ~0 here
                // (it is exercised on a paused node by
                // `load_monitor_task_measures_a_paused_node`).
                (mech.stretch_x > 0).then(|| {
                    if self.real_probe {
                        return LoadMonitor::new(
                            &self.sim.env(nid(NODES[i])),
                            mech.stretch_x,
                            BASE,
                            self.handles[i].clone(),
                            self.governors[i].clone(),
                        );
                    }
                    LoadMonitor::detached(
                        mech.stretch_x,
                        BASE,
                        self.handles[i].clone(),
                        self.governors[i].clone(),
                    )
                })
            })
            .collect();
    }

    fn options(&self, node: usize) -> HostedOptions<SimEnv> {
        HostedOptions {
            heartbeat_batcher: Some(self.batchers[node].clone()),
            backpressure: ElectionBackpressure {
                governor: self.governors[node].clone(),
                monitor: self.monitors[node].clone(),
            },
            ..HostedOptions::default()
        }
    }

    /// Start one replica of the group on `stream` on `node`, with `voters` as
    /// its configuration.
    fn start_replica(&self, node: usize, stream: u64, voters: &[usize]) -> KvNode {
        RaftKvNode::start_hosted_with_options(
            self.sim.env(nid(NODES[node])),
            voters.iter().map(|&v| nid(NODES[v])).collect(),
            MemoryEngine::new(),
            StorageScope::whole(),
            stream,
            self.options(node),
        )
    }

    /// Host all `g` RF3 groups at once (ADR 0078 cell 1's "hosting stagger
    /// off"). `mech` is where later steps branch.
    fn host(&mut self, mech: Mechanisms) {
        self.setup(mech);
        for gi in 0..self.g {
            let stream = STREAM0 + gi as u64;
            let replicas = (0..NODES.len())
                .map(|i| self.start_replica(i, stream, &[0, 1, 2]))
                .collect();
            self.nodes.push(replicas);
            self.placement.push(vec![0, 1, 2]);
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
            self.cost_ns
                .set(u64::try_from(cost.as_nanos()).unwrap_or(u64::MAX));
        }
    }

    /// One lag probe: the sample is how long a delivery arriving now would
    /// wait for the node's virtual CPU, `waiting deliveries x per-event cost`
    /// (the queue-wait that makes heartbeats late; see [`Rig::setup`]).
    fn probe(&self) {
        let cost = self.cost_ns.get();
        for (i, m) in self.monitors.iter().enumerate() {
            if let Some(m) = m {
                let (_, waiting) = self.sim.cpu_backlog(&nid(NODES[i]));
                m.observe((waiting as u64).saturating_mul(cost));
            }
        }
    }

    /// Whether replica `ri` of group `gi` sits on a live node.
    fn replica_live(&self, gi: usize, ri: usize) -> bool {
        self.live[self.placement[gi][ri]] && !self.halted.contains(&(gi, ri))
    }

    /// The index of the replica of `gi` leading on a live node, if any.
    fn live_leader(&self, gi: usize) -> Option<usize> {
        self.nodes[gi]
            .iter()
            .enumerate()
            .position(|(ri, n)| self.replica_live(gi, ri) && n.is_leader())
    }

    /// Groups led by a replica on a live node.
    fn led(&self) -> usize {
        (0..self.nodes.len())
            .filter(|&gi| self.live_leader(gi).is_some())
            .count()
    }

    /// Groups with exactly one leader among all replicas (live or not).
    fn exactly_one_leader(&self) -> usize {
        self.nodes
            .iter()
            .filter(|rs| rs.iter().filter(|n| n.is_leader()).count() == 1)
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

    /// Kill node `idx` for good (its tasks are dropped, so their permits are
    /// returned by the RAII release).
    fn kill(&mut self, idx: usize) {
        self.sim.stop(nid(NODES[idx]));
        self.sim.crash(nid(NODES[idx]));
        self.live[idx] = false;
    }

    /// Cut node `idx` off from the other two.
    fn isolate(&mut self, idx: usize) {
        for (j, &other) in NODES.iter().enumerate() {
            if j != idx {
                self.sim.partition_pair(nid(NODES[idx]), nid(other));
            }
        }
        self.live[idx] = false;
    }

    fn heal(&mut self, idx: usize) {
        for (j, &other) in NODES.iter().enumerate() {
            if j != idx {
                self.sim.heal(nid(NODES[idx]), nid(other));
            }
        }
        self.live[idx] = true;
    }

    /// `ghosts` harness clients per node's governor that behave like groups
    /// that cannot win: hold a permit for `2 * election_base`, drop it, ask
    /// again (a re-ask queues at the back); while queued, re-check every
    /// `election_base` like a deferred group's backstop.
    fn spawn_ghosts(&self, ghosts: usize) {
        for (i, &id) in NODES.iter().enumerate() {
            let Some(gov) = self.governors[i].clone() else {
                continue;
            };
            let env = self.sim.env(nid(id));
            for j in 0..ghosts {
                let (env2, gov) = (env.clone(), gov.clone());
                env.spawn_task(async move {
                    let wake: CampaignWake = Arc::new(|| {});
                    let stream = GHOST0 + j as u64;
                    loop {
                        match gov.try_admit(stream, env2.now(), BASE * 4, &wake) {
                            Admission::Admitted(p) => {
                                env2.sleep(BASE * 2).await;
                                drop(p);
                            }
                            Admission::Deferred => env2.sleep(BASE).await,
                        }
                    }
                });
            }
        }
    }

    /// Governor numbers summed over the nodes (max for the high-water marks).
    fn gov_totals(&self) -> GovTotals {
        let mut t = GovTotals::default();
        for g in self.governors.iter().flatten() {
            let s = g.stats();
            t.inflight_hwm = t.inflight_hwm.max(s.inflight_hwm);
            t.nonexempt_hwm = t.nonexempt_hwm.max(s.nonexempt_hwm);
            t.queue_now = t.queue_now.max(s.queue_depth);
            t.admitted += s.admitted;
            t.deferred += s.deferred;
            t.expired += s.expired_permits;
        }
        t
    }
}

/// What the governors saw (zero when off).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct GovTotals {
    inflight_hwm: usize,
    nonexempt_hwm: usize,
    queue_now: usize,
    admitted: u64,
    deferred: u64,
    expired: u64,
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
    /// When the fault of a fault cell began (and so the monotone clause starts
    /// over: the led count falls at the fault by design).
    fault_at_s: Option<f64>,
    /// First sample at or after the fault at which every group was led again
    /// (by a live node).
    recovered_at_s: Option<f64>,
    /// Whether the current phase has fully converged (the monotone clause
    /// applies from then on).
    converged: bool,
    /// Lowest led count since the current phase converged.
    min_led_after_convergence: Option<usize>,
}

impl Sampler {
    fn begin_fault(&mut self, at_s: f64) {
        self.fault_at_s = Some(at_s);
        self.converged = false;
        self.min_led_after_convergence = None;
    }

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
        if led == rig.g {
            if self.all_led_at_s.is_none() {
                self.all_led_at_s = Some(at_s);
            }
            if self.fault_at_s.is_some() && self.recovered_at_s.is_none() {
                self.recovered_at_s = Some(at_s);
            }
            self.converged = true;
        }
        if self.converged {
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
#[derive(Debug, Default)]
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
    /// Fault cells: seconds from the fault until every group was led by a live
    /// node again.
    recovery_s: Option<f64>,
    gov: GovTotals,
    /// Highest dead-banded monitor lag seen at a sample, in ms (0 = stretch off
    /// or never above the dead band), and the highest stretch it implied.
    max_lag_ms: u64,
    max_stretch_ms: u64,
    /// Cell-specific clauses the runner found broken, plus the numbers behind
    /// them (`detail`).
    extra: Vec<&'static str>,
    detail: String,
}

/// Oracle thresholds. One struct so each cell states its own ceilings next to
/// the cell.
#[derive(Clone, Copy)]
struct Limits {
    /// Seconds for every group to get a leader.
    all_led_s: f64,
    /// Mean `max_term - 1` per group.
    churn_per_group: f64,
    /// Fault cells: seconds from the fault to every group led by a live node.
    recovery_s: Option<f64>,
    /// `K` the governor ran with (`0` = no governor to check).
    governor_k: usize,
    /// `lag_stretch_max_x` the monitor ran with (`0` = no stretch to check).
    stretch_x: u64,
    /// Cells that overload the node: the monitor must have read a lag, or the
    /// stretch clauses are vacuous.
    expect_lag: bool,
}

impl Limits {
    const DEFAULT: Limits = Limits {
        all_led_s: 10.0,
        churn_per_group: 0.25,
        recovery_s: None,
        governor_k: 0,
        stretch_x: 0,
        expect_lag: false,
    };

    fn for_cell(c: &Cell) -> Limits {
        let base = Limits {
            governor_k: c.mech.governor_k,
            stretch_x: c.mech.stretch_x,
            expect_lag: matches!(
                c.kind,
                Kind::SustainedOverload | Kind::OverloadThenRelief | Kind::LeaderLossUnderLag
            ),
            ..Limits::DEFAULT
        };
        match c.kind {
            // A third of the groups lose their leader by design: one term each,
            // plus the odd split vote.
            Kind::LeaderNodeLoss | Kind::PartitionHeal => Limits {
                churn_per_group: 0.75,
                recovery_s: Some(5.0),
                ..base
            },
            Kind::TransferUnderSaturation | Kind::LostLeaderBound | Kind::NoPermits => Limits {
                churn_per_group: 0.5,
                ..base
            },
            Kind::ColdStart => base,
            // With the stretch on, a sustained overload moves no term (0.0
            // measured); the default ceiling is for the unprotected code.
            Kind::SustainedOverload if c.mech.stretch_x > 0 => Limits {
                churn_per_group: 0.05,
                ..base
            },
            Kind::SustainedOverload => base,
            Kind::LeaderLossUnderLag => Limits {
                churn_per_group: 0.75,
                recovery_s: Some(5.0),
                ..base
            },
            Kind::OverloadThenRelief => Limits {
                churn_per_group: 0.05,
                recovery_s: Some(10.0),
                ..base
            },
        }
    }

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
        if let Some(bound) = self.recovery_s {
            match r.recovery_s {
                None => v.push("liveness: groups never recovered after the fault"),
                Some(t) if t > bound => v.push("liveness: recovery after the fault above bound"),
                Some(_) => {}
            }
        }
        if self.governor_k > 0 && r.gov.nonexempt_hwm > self.governor_k {
            v.push("governor: more than K non-exempt campaigns in flight");
        }
        if self.stretch_x > 0 {
            // The stretch is `min(4 * lag, S_max)`: never above `S_max`, on
            // any node, at any sample (the unbounded control breaks this).
            let cap_ms = self.stretch_x * BASE.as_millis() as u64;
            if r.max_stretch_ms > cap_ms {
                v.push("stretch: above S_max = lag_stretch_max_x * election_base");
            }
            if self.expect_lag && r.max_lag_ms == 0 {
                v.push("harness: the load monitor never read a lag, the stretch is vacuous");
            }
        }
        v.extend(r.extra.iter().copied());
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
    /// Propose one write on every `every`-th group's current (live) leader.
    fn propose(&mut self, rig: &Rig, every: usize) {
        for gi in (0..rig.nodes.len()).step_by(every) {
            let Some(ri) = rig.live_leader(gi) else {
                continue;
            };
            let n = &rig.nodes[gi][ri];
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
            } else if n.term() == term && rig.replica_live(gi, ri) {
                self.pending.push((gi, ri, key, index, term));
            }
        }
    }

    /// Acked keys absent from some live voter's engine.
    fn lost(&self, rig: &Rig) -> usize {
        self.acked
            .iter()
            .filter(|(gi, key)| {
                rig.nodes[*gi].iter().enumerate().any(|(ri, n)| {
                    if !rig.replica_live(*gi, ri)
                        || !n.config().contains(&nid(NODES[rig.placement[*gi][ri]]))
                    {
                        return false;
                    }
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
/// Load phase before the node loss of cell 4b / control (e).
const LAG_BUILDUP: Duration = Duration::from_secs(10);
/// Virtual time of the overload of cell 6.
const OVERLOAD_SHORT: Duration = Duration::from_secs(30);
/// Virtual time after the load is removed, for the "no group permanently
/// leaderless once the fault clears" clause.
const RELIEF: Duration = Duration::from_secs(15);
/// Cold start observation window.
const COLD_WINDOW: Duration = Duration::from_secs(20);
/// Observation window after a node loss.
const FAULT_WINDOW: Duration = Duration::from_secs(20);
/// How long node 2 stays cut off in cell 7.
const PARTITION: Duration = Duration::from_secs(10);
/// Fine sampling step of the fault cells (recovery-time resolution).
const FINE: Duration = Duration::from_millis(100);

/// One run in progress: the rig, the sampler, the writes and the clock.
struct Run {
    rig: Rig,
    s: Sampler,
    w: Writes,
    at: Duration,
    cell: String,
    max_lag: Duration,
    max_stretch: Duration,
    since_probe: Duration,
}

impl Run {
    fn new(c: &Cell, g: usize) -> Run {
        Run {
            rig: Rig::new(c.seed, g),
            s: Sampler::default(),
            w: Writes::default(),
            at: Duration::ZERO,
            cell: c.name.clone(),
            max_lag: Duration::ZERO,
            max_stretch: Duration::ZERO,
            since_probe: Duration::ZERO,
        }
    }

    /// Run the simulator for `step`, taking a lag probe every
    /// [`animus_cp_data::load_monitor::PROBE`] of virtual time.
    fn run_probed(&mut self, step: Duration) {
        if self.rig.monitors.iter().all(Option::is_none) {
            self.rig.sim.run_for(step);
            return;
        }
        let mut left = step;
        while !left.is_zero() {
            let chunk = left.min(animus_cp_data::load_monitor::PROBE - self.since_probe);
            self.rig.sim.run_for(chunk);
            left -= chunk;
            self.since_probe += chunk;
            if self.since_probe >= animus_cp_data::load_monitor::PROBE {
                self.since_probe = Duration::ZERO;
                self.rig.probe();
            }
        }
    }

    /// Advance `d` of virtual time, sampling every `step`.
    fn advance(&mut self, d: Duration, step: Duration, write: bool) {
        let end = self.at + d;
        while self.at < end {
            self.run_probed(step);
            self.at += step;
            self.s.sample(&self.rig, self.at.as_secs_f64());
            for m in self.rig.monitors.iter().flatten() {
                self.max_lag = self.max_lag.max(m.lag());
                self.max_stretch = self.max_stretch.max(m.stretch(BASE));
            }
            if write {
                self.w.settle(&self.rig);
            }
        }
    }

    /// Converge unloaded, then put a few writes through and let them apply.
    fn converge_and_write(&mut self) {
        self.advance(CONVERGE, SAMPLE, false);
        self.w.propose(&self.rig, 10);
        self.advance(Duration::from_secs(2), SAMPLE, true);
    }

    /// Advance until `done` or `max`, in `step`s; returns the virtual seconds
    /// it took when `done` held.
    fn advance_until(
        &mut self,
        max: Duration,
        step: Duration,
        mut done: impl FnMut(&Rig) -> bool,
    ) -> Option<f64> {
        let start = self.at;
        while self.at < start + max {
            if done(&self.rig) {
                return Some((self.at - start).as_secs_f64());
            }
            self.advance(step, step, false);
        }
        done(&self.rig).then(|| (self.at - start).as_secs_f64())
    }

    fn finish(self, extra: Vec<&'static str>, detail: String) -> Report {
        let Run {
            rig,
            s,
            w,
            cell,
            max_lag,
            max_stretch,
            ..
        } = self;
        let g = rig.g;
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
            recovery_s: s.recovered_at_s.zip(s.fault_at_s).map(|(r, f)| r - f),
            gov: rig.gov_totals(),
            max_lag_ms: max_lag.as_millis() as u64,
            max_stretch_ms: max_stretch.as_millis() as u64,
            extra,
            detail,
        };
        if std::env::var("ANIMUS_ELECTION_BP_TRACE").is_ok() {
            for x in s.series.iter().step_by(4) {
                eprintln!(
                    "BP {} t={:.1} led={} churn_sum={} max_term={}",
                    cell, x.at_s, x.led, x.churn_sum, x.max_term
                );
            }
        }
        drop(rig.nodes);
        rig.sim.shutdown();
        report
    }
}

fn run_cell(c: &Cell, g: usize) -> Report {
    let mut r = Run::new(c, g);
    match c.kind {
        Kind::ColdStart => {
            r.rig.set_load(c.load);
            r.rig.host(c.mech);
            r.advance(COLD_WINDOW, SAMPLE, false);
            r.w.propose(&r.rig, 10);
            r.advance(Duration::from_secs(5), SAMPLE, true);
            r.finish(Vec::new(), String::new())
        }
        Kind::SustainedOverload => {
            r.rig.host(c.mech);
            r.converge_and_write();
            r.rig.set_load(c.load);
            r.advance(OVERLOAD, SAMPLE, true);
            r.rig.set_load(0.0);
            r.advance(RELIEF, SAMPLE, true);
            r.finish(Vec::new(), String::new())
        }
        Kind::OverloadThenRelief => {
            r.rig.host(c.mech);
            r.converge_and_write();
            r.rig.set_load(c.load);
            r.advance(OVERLOAD_SHORT, SAMPLE, true);
            r.rig.set_load(0.0);
            // From here the oracle measures recovery, not the overload.
            r.s.begin_fault(r.at.as_secs_f64());
            r.advance(RELIEF, FINE, true);
            r.finish(Vec::new(), String::new())
        }
        Kind::LeaderLossUnderLag => {
            r.rig.host(c.mech);
            r.converge_and_write();
            r.rig.set_load(c.load);
            r.advance(LAG_BUILDUP, SAMPLE, true);
            let led_by_dead = r.rig.nodes.iter().filter(|rs| rs[0].is_leader()).count();
            r.rig.set_load(0.0);
            r.s.begin_fault(r.at.as_secs_f64());
            r.rig.kill(0);
            r.advance(Duration::from_secs(30), FINE, true);
            let detail = format!("led_by_dead_node={led_by_dead}");
            r.finish(Vec::new(), detail)
        }
        Kind::LeaderNodeLoss => run_leader_node_loss(r, c),
        Kind::PartitionHeal => run_partition_heal(r, c),
        Kind::TransferUnderSaturation => run_transfer(r, c),
        Kind::LostLeaderBound => run_lost_leader(r, c),
        Kind::NoPermits => run_no_permits(r, c),
    }
}

/// Cell 4 (and control (c)): quiesce everything, kill the node leading a third
/// of the groups and wake the survivors together.
fn run_leader_node_loss(mut r: Run, c: &Cell) -> Report {
    r.rig.host(c.mech);
    r.converge_and_write();
    for rs in &r.rig.nodes {
        for n in rs {
            n.enable_quiescence(Duration::from_secs(2));
        }
    }
    let quiesced = r.advance_until(Duration::from_secs(40), Duration::from_secs(1), |rig| {
        rig.nodes
            .iter()
            .all(|rs| rs.iter().all(KvNode::is_quiesced))
    });
    let mut extra = Vec::new();
    if quiesced.is_none() {
        extra.push("harness: the groups did not all quiesce before the fault");
    }
    let led_by_dead = r.rig.nodes.iter().filter(|rs| rs[0].is_leader()).count();
    r.rig.set_load(c.load);
    r.s.begin_fault(r.at.as_secs_f64());
    r.rig.kill(0);
    // The reconciler's `down`-driven wake: every group whose replica set
    // contains the down node is woken on the survivors, all in one pass.
    for rs in &r.rig.nodes {
        for n in &rs[1..] {
            n.wake();
        }
    }
    r.advance(FAULT_WINDOW, FINE, false);
    r.w.propose(&r.rig, 10);
    r.advance(Duration::from_secs(5), SAMPLE, true);
    let detail = format!("led_by_dead_node={led_by_dead} quiesce_s={quiesced:?}");
    r.finish(extra, detail)
}

/// Cell 7: cut node 2 off, then heal.
fn run_partition_heal(mut r: Run, c: &Cell) -> Report {
    r.rig.host(c.mech);
    r.converge_and_write();
    r.rig.set_load(c.load);
    r.s.begin_fault(r.at.as_secs_f64());
    r.rig.isolate(2);
    r.advance(PARTITION, FINE, false);
    let isolated_gov = r.rig.governors[2].as_ref().map(CampaignGovernor::stats);
    r.rig.heal(2);
    // After the heal every group must settle on exactly one leader.
    let healed = r.advance_until(Duration::from_secs(15), FINE, |rig| {
        rig.exactly_one_leader() == rig.g
    });
    let mut extra = Vec::new();
    if healed.is_none() {
        extra.push("liveness: a group has no single leader after the heal");
    }
    r.w.propose(&r.rig, 10);
    r.advance(Duration::from_secs(5), SAMPLE, true);
    let detail =
        format!("heal_to_single_leader_s={healed:?} isolated_node_governor={isolated_gov:?}");
    r.finish(extra, detail)
}

/// Cell 8: a transfer must not queue behind a saturated governor.
fn run_transfer(mut r: Run, c: &Cell) -> Report {
    r.rig.host(c.mech);
    r.converge_and_write();
    r.rig.spawn_ghosts(3 * K);
    r.advance(Duration::from_secs(3), SAMPLE, true);
    let mut extra = Vec::new();
    let gi = 0;
    let from = r.rig.nodes[gi]
        .iter()
        .position(KvNode::is_leader)
        .expect("group 0 is led");
    let to = (from + 1) % NODES.len();
    let armed = r.rig.nodes[gi][from].transfer_leadership(nid(NODES[to]));
    let took = r.advance_until(Duration::from_secs(2), Duration::from_millis(10), |rig| {
        rig.nodes[gi][to].is_leader()
    });
    if !armed {
        extra.push("harness: the transfer was not armed");
    }
    // One election timeout is the transfer's own budget (ADR 0029); 50 ms of
    // slack for the delivery of the `TimeoutNow` and the vote.
    match took {
        Some(t) if t <= BASE.as_secs_f64() + 0.05 => {}
        _ => extra.push("transfer: slower than one election timeout under a saturated governor"),
    }
    let hwm = r.rig.gov_totals();
    if hwm.inflight_hwm <= K {
        extra.push("harness: the governor was not saturated when the transfer ran");
    }
    r.advance(Duration::from_secs(3), SAMPLE, true);
    let detail = format!("transfer_s={took:?} governor={hwm:?}");
    r.finish(extra, detail)
}

/// Cell 10 (and control (d)): the lost-leader bound, asserted literally.
fn run_lost_leader(mut r: Run, c: &Cell) -> Report {
    r.rig.host(c.mech);
    r.converge_and_write();
    r.rig.spawn_ghosts(3 * K);
    r.advance(Duration::from_secs(3), SAMPLE, true);
    // `Q`: groups queued ahead of a group that asks now.
    let q = r
        .rig
        .governors
        .iter()
        .flatten()
        .map(|g| g.stats().queue_depth)
        .max()
        .unwrap_or(0);
    let gi = 0;
    let ri = r.rig.nodes[gi]
        .iter()
        .position(KvNode::is_leader)
        .expect("group 0 is led");
    r.s.begin_fault(r.at.as_secs_f64());
    r.rig.nodes[gi][ri].shutdown();
    // The halted replica's frozen handle still reports `is_leader`.
    r.rig.halted.push((gi, ri));
    let delay = r.advance_until(Duration::from_secs(8), Duration::from_millis(10), |rig| {
        rig.nodes[gi]
            .iter()
            .enumerate()
            .any(|(j, n)| j != ri && n.is_leader())
    });
    // `delay <= 2*base_eff + ceil(Q/K_eff) * T_hold_max`, `T_hold_max =
    // 2*base_eff` (ADR 0078, "Lost-leader liveness bound"); `base_eff = base`
    // here (no lag stretch).
    // The ADR's formula stops at admission; the admitted group still needs its
    // own pre-vote / vote / commit round trips and the probe resolution is
    // 10 ms, so the harness adds `OWN_ELECTION_SLACK` (50 ms) to the literal
    // bound. A leak (control (d)) misses it by seconds, not milliseconds.
    const OWN_ELECTION_SLACK: f64 = 0.05;
    let bound = 2.0 * BASE.as_secs_f64()
        + (q.div_ceil(K) as f64) * 2.0 * BASE.as_secs_f64()
        + OWN_ELECTION_SLACK;
    let mut extra = Vec::new();
    match delay {
        None => extra.push("bound: the lost group never got a leader (starved)"),
        Some(d) if d > bound => {
            extra.push("bound: lost-leader delay above 2*base + ceil(Q/K)*T_hold_max")
        }
        Some(_) => {}
    }
    let detail = format!("q={q} k={K} delay_s={delay:?} bound_s={bound:.3}");
    // The group stays on the books of the run: keep going so the end-state
    // clause sees every other group still led.
    r.advance(Duration::from_secs(3), SAMPLE, false);
    r.finish(extra, detail)
}

/// Cell 11: learners, single-voter groups and quiesced groups take no permit.
fn run_no_permits(mut r: Run, c: &Cell) -> Report {
    const VOTER_GROUPS: usize = 60;
    const SOLO_GROUPS: usize = 20;
    r.rig.setup(c.mech);
    // Voters on nodes 0 and 1; node 2's replica is started outside the
    // configuration, a quiet non-voter (a learner as far as campaigning goes).
    for gi in 0..VOTER_GROUPS {
        let stream = STREAM0 + gi as u64;
        let replicas = (0..3)
            .map(|i| r.rig.start_replica(i, stream, &[0, 1]))
            .collect();
        r.rig.nodes.push(replicas);
        r.rig.placement.push(vec![0, 1, 2]);
    }
    // Single-voter groups, one replica each, spread over the nodes.
    for gi in VOTER_GROUPS..VOTER_GROUPS + SOLO_GROUPS {
        let stream = STREAM0 + gi as u64;
        let node = gi % 3;
        let replica = r.rig.start_replica(node, stream, &[node]);
        r.rig.nodes.push(vec![replica]);
        r.rig.placement.push(vec![node]);
    }
    r.rig.g = VOTER_GROUPS + SOLO_GROUPS;
    r.advance(Duration::from_secs(10), SAMPLE, false);
    r.w.propose(&r.rig, 7);
    r.advance(Duration::from_secs(2), SAMPLE, true);

    let learner_gov = r.rig.governors[2].as_ref().expect("governor on");
    let mut extra = Vec::new();
    let s2 = learner_gov.stats();
    if s2.admitted != 0 || s2.deferred != 0 || s2.inflight_hwm != 0 {
        extra.push("permits: a learner or single-voter group touched node 2's governor");
    }
    // Quiesce everything and watch the permit count stay put.
    for rs in &r.rig.nodes {
        for n in rs {
            n.enable_quiescence(Duration::from_secs(2));
        }
    }
    let quiesced = r.advance_until(Duration::from_secs(40), Duration::from_secs(1), |rig| {
        rig.nodes[..VOTER_GROUPS]
            .iter()
            .all(|rs| rs[..2].iter().all(KvNode::is_quiesced))
    });
    if quiesced.is_none() {
        extra.push("harness: the voter groups did not quiesce");
    }
    let before = r.rig.gov_totals();
    r.advance(Duration::from_secs(10), SAMPLE, false);
    let after = r.rig.gov_totals();
    if before.admitted != after.admitted || before.deferred != after.deferred {
        extra.push("permits: a quiesced group asked for a permit");
    }
    for (i, h) in r.rig.handles.iter().enumerate() {
        if h.get(Metric::CpCampaignInflight) != 0 {
            extra.push("permits: a permit is still held with every group idle");
        }
        if i == 2 && h.get(Metric::CpCampaignInflightHwm) != 0 {
            extra.push("permits: node 2's in-flight high-water mark is not 0");
        }
    }
    if r.rig.led() != r.rig.g {
        extra.push("liveness: a voter or single-voter group is not led");
    }
    let detail = format!("node2={s2:?} before_quiesce={before:?} after={after:?}");
    r.finish(extra, detail)
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

/// Every cell whose oracle must pass.
#[test]
fn election_backpressure_corpus_cells_pass_the_oracle() {
    for c in select(cells()) {
        let r = run_cell(&c, groups());
        let v = Limits::for_cell(&c).violations(&r);
        eprintln!("BP-CELL {} seed={:#x} {r:?}", c.name, c.seed);
        eprintln!("BP-DETAIL {} {}", c.name, r.detail);
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
        let v = Limits::for_cell(&c).violations(&r);
        eprintln!("BP-CONTROL {} seed={:#x} {v:?} {r:?}", c.name, c.seed);
        eprintln!("BP-DETAIL {} {}", c.name, r.detail);
        assert!(
            v.iter().all(|x| !is_safety(x)),
            "{} seed={:#x}: safety must hold even in the collapse: {v:?} {r:?}",
            c.name,
            c.seed
        );
        assert!(
            c.kind != Kind::SustainedOverload
                || v.contains(&"monotone: led count fell after first convergence"),
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

/// The monitor's own probe task (`LoadMonitor::new`, the production path)
/// measures timer lateness: a node paused for 400 ms wakes its probe 400 ms
/// late, the reading and stretch follow, the governor's `K_eff` drops, and all
/// of it decays back once the node is healthy.
#[test]
fn load_monitor_task_measures_a_paused_node() {
    let mut sim = Simulator::new(corpus::name_seed("election_bp_monitor_probe"));
    let h = MetricsHandle::recording();
    let gov = CampaignGovernor::new(K, h.clone());
    let m = LoadMonitor::new(&sim.env(nid(0)), 4, BASE, h.clone(), Some(gov.clone()));
    sim.run_for(Duration::from_secs(1));
    assert_eq!(m.lag(), Duration::ZERO, "an idle node reads zero");
    assert_eq!(m.stretch(BASE), Duration::ZERO);
    assert_eq!(gov.k_eff(), K);
    assert_eq!(h.get(Metric::CpElectionLagNanos), 0);

    sim.pause(nid(0), Duration::from_millis(400));
    sim.run_for(Duration::from_millis(450));
    let lag = m.lag();
    assert!(
        lag >= Duration::from_millis(300) && lag <= Duration::from_millis(400),
        "the probe fires ~380 ms late: {lag:?}"
    );
    assert_eq!(m.stretch(BASE), BASE * 4, "400 ms of lag hits the 4x cap");
    assert_eq!(gov.k_eff(), K / 4);
    assert!(h.get(Metric::CpElectionLagNanos) >= 300_000_000);
    assert_eq!(
        h.get(Metric::CpElectionStretchNanos),
        BASE.as_nanos() as u64 * 4
    );

    // Slow integer decay: it takes a few seconds, not a probe, to forget.
    sim.run_for(Duration::from_millis(100));
    assert!(m.lag() > Duration::from_millis(200), "decays slowly");
    sim.run_for(Duration::from_secs(8));
    assert_eq!(m.lag(), Duration::ZERO, "back inside the dead band");
    assert_eq!(gov.k_eff(), K);
    sim.shutdown();
}

/// NEGATIVE CONTROL (b): the governor on, the lag stretch off. Cell 5's load
/// must still break the oracle, so the stretch is load-bearing on its own
/// (safety still holds).
#[test]
fn control_b_stretch_off_still_breaks_under_overload() {
    for c in select(controls_b()) {
        let r = run_cell(&c, groups());
        let v = Limits::for_cell(&c).violations(&r);
        eprintln!("BP-CONTROL {} seed={:#x} {v:?} {r:?}", c.name, c.seed);
        eprintln!("BP-DETAIL {} {}", c.name, r.detail);
        assert!(
            v.iter().all(|x| !is_safety(x)),
            "{} seed={:#x}: safety must hold: {v:?} {r:?}",
            c.name,
            c.seed
        );
        assert!(
            v.contains(&"churn: terms per group above ceiling")
                || v.contains(&"monotone: led count fell after first convergence"),
            "{} seed={:#x}: without the stretch the oracle must fail: {v:?} {r:?}",
            c.name,
            c.seed
        );
    }
}

/// NEGATIVE CONTROL (e): `S_max = inf`. A stretch with no cap turns the
/// node's lag into unavailability: the failover after a node loss must miss
/// the bound that the capped stretch (cell 4) meets.
#[test]
fn control_e_unbounded_stretch_slows_failover() {
    for c in select(controls_e()) {
        let r = run_cell(&c, groups());
        let v = Limits::for_cell(&c).violations(&r);
        eprintln!("BP-CONTROL {} seed={:#x} {v:?} {r:?}", c.name, c.seed);
        eprintln!("BP-DETAIL {} {}", c.name, r.detail);
        assert!(
            v.iter().all(|x| !is_safety(x)),
            "{} seed={:#x}: safety must hold: {v:?} {r:?}",
            c.name,
            c.seed
        );
        assert!(
            v.contains(&"liveness: recovery after the fault above bound")
                || v.contains(&"liveness: groups never recovered after the fault"),
            "{} seed={:#x}: failover must exceed the bound: {v:?} {r:?}",
            c.name,
            c.seed
        );
    }
}

/// NEGATIVE CONTROL (c): admission that also gates the responder side (a
/// deferred group re-arms its election timer, so it refuses pre-votes). The
/// admitted campaigners then cannot reach a majority, permits are held until
/// their attempt times out, and the recovery after a leader-node loss is slower
/// than the real governor's. Safety still holds.
#[test]
fn control_c_gating_the_responder_slows_recovery() {
    for c in select(controls_c()) {
        let r = run_cell(&c, groups());
        let v = Limits::for_cell(&c).violations(&r);
        eprintln!("BP-CONTROL {} seed={:#x} {v:?} {r:?}", c.name, c.seed);
        eprintln!("BP-DETAIL {} {}", c.name, r.detail);
        assert!(
            v.iter().all(|x| !is_safety(x)),
            "{} seed={:#x}: safety must hold: {v:?} {r:?}",
            c.name,
            c.seed
        );
        assert!(
            v.contains(&"liveness: recovery after the fault above bound")
                || v.contains(&"liveness: groups never recovered after the fault"),
            "{} seed={:#x}: recovery must exceed the bound: {v:?} {r:?}",
            c.name,
            c.seed
        );
    }
}

/// NEGATIVE CONTROL (d): permits that are never returned (no RAII release).
/// The lost-leader bound of cell 10 must be violated.
#[test]
fn control_d_leaked_permits_break_the_lost_leader_bound() {
    for c in select(controls_d()) {
        let r = run_cell(&c, groups());
        let v = Limits::for_cell(&c).violations(&r);
        eprintln!("BP-CONTROL {} seed={:#x} {v:?} {r:?}", c.name, c.seed);
        eprintln!("BP-DETAIL {} {}", c.name, r.detail);
        assert!(
            v.iter().all(|x| !is_safety(x)),
            "{} seed={:#x}: safety must hold: {v:?} {r:?}",
            c.name,
            c.seed
        );
        assert!(
            v.iter().any(|x| x.starts_with("bound:")),
            "{} seed={:#x}: the lost-leader bound must be violated: {v:?} {r:?}",
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
    let all: Vec<Cell> = cells()
        .into_iter()
        .chain(controls())
        .chain(controls_b())
        .chain(controls_c())
        .chain(controls_d())
        .chain(controls_e())
        .collect();
    let names: BTreeSet<&str> = all.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names.len(), all.len());
    for c in &all {
        // Control (e) deliberately shares cell 4b's seed.
        if !c.name.starts_with("control_e") {
            assert_eq!(c.seed, corpus::name_seed(&c.name));
        }
    }
}

// ---------------------------------------------------------------------------
// Cell 12: the mechanisms-off path is byte-for-byte the pre-change behaviour.
// ---------------------------------------------------------------------------

/// A compact fingerprint of one small run: the simulator's event counts and,
/// per group, `(leader replica, term)`. Pinned below; later steps (governor,
/// lag stretch, staggering) must leave it unchanged while their knobs are off
/// (ADR 0078, cell 12).
fn digest(load: f64, mech: Mechanisms) -> (u64, u64, Vec<(usize, u64)>) {
    let g = 30;
    let mut rig = Rig::new(corpus::name_seed("election_bp_cell_12"), g);
    rig.host(mech);
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
        let a = digest(load, Mechanisms::NONE);
        let b = digest(load, Mechanisms::NONE);
        assert_eq!(a, b, "load {load}: the run is not deterministic");
        let h = fold(&a);
        eprintln!("BP-DIGEST load={load} {h:#x} polls={} fires={}", a.0, a.1);
        assert_eq!(
            h, pinned,
            "load {load}: the mechanisms-off path changed ({a:?})"
        );
    }
}

/// With the governor on and no contention (30 groups, so never more than `K`
/// campaigns at once, no deferral) the run is the same bit for bit: the
/// governor draws no entropy and posts no event of its own.
#[test]
fn cell_12_governor_on_without_contention_is_identical() {
    for (load, pinned) in [(0.0, PINNED_IDLE), (0.9, PINNED_LOADED)] {
        let d = digest(load, Mechanisms::GOVERNOR);
        assert_eq!(
            fold(&d),
            pinned,
            "load {load}: an idle governor changed the run ({d:?})"
        );
    }
}

/// With the production probe task running and no load, the monitor reads zero
/// the whole time, so every group's leader and term are those of the run with
/// no mechanism at all (the probe's own timers add events, so only the
/// per-group outcome is compared).
#[test]
fn cell_12_idle_probe_task_changes_no_group_outcome() {
    fn run(mech: Mechanisms) -> (Vec<(usize, u64)>, u64) {
        let mut rig = Rig::new(corpus::name_seed("election_bp_cell_12"), 30);
        rig.real_probe = true;
        rig.host(mech);
        rig.sim.run_for(Duration::from_secs(8));
        let per_group = rig
            .nodes
            .iter()
            .map(|rs| {
                let lead = rs.iter().position(KvNode::is_leader).unwrap_or(usize::MAX);
                (lead, rs.iter().map(KvNode::term).max().unwrap_or(0))
            })
            .collect();
        let lag = rig
            .monitors
            .iter()
            .flatten()
            .map(|m| m.lag().as_nanos() as u64)
            .max()
            .unwrap_or(0);
        drop(rig.nodes);
        rig.sim.shutdown();
        (per_group, lag)
    }
    let (off, _) = run(Mechanisms::NONE);
    let (all, lag) = run(Mechanisms::ALL);
    assert_eq!(lag, 0, "an unloaded node reads zero lag");
    assert_eq!(all, off, "the idle monitor changed a group's outcome");
}

const PINNED_IDLE: u64 = 0x889e0d5bdd9161f8;
const PINNED_LOADED: u64 = 0x4a3916c2f8d0bef1;
