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
use animus_cp_data::host::{
    DEFAULT_HOST_SETTLE_TIMEOUT, LeaderPreference, MemoryTabletEngines, MetadataView, Reconciler,
};
use animus_cp_data::load_monitor::{LoadMonitor, STRETCH_C};
use animus_cp_data::{HostedOptions, KIND_BASE, RaftKvNode, StorageScope};
use animus_env::{Clock, Env, EnvExt, Metric, MetricsHandle, NodeId, nid};
use animus_sim::{CpuOrder, SimEnv, Simulator};
use animus_storage::{MemoryEngine, StorageEngine};
use animus_tablet::{KeyRange, Tablet, TabletId};
use animus_test::corpus::{self, SeedVariant};
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;
type Recon = Reconciler<SimEnv, MemoryEngine>;

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
/// `H`, the hosting stagger the reconciled cells run with (ADR 0078 default).
const HOST_H: usize = 100;
/// The election base of the LAN profile, as the groups run it.
const BASE: Duration = Duration::from_millis(150);

/// Probe resolution of `recovery_s` (`SAMPLE`/`FINE` steps), in seconds.
const RECOVERY_NOISE_S: f64 = 0.1;

/// Cell 10's livelock guard (issue #1306 review): the most lost campaign
/// attempts a lost-leader re-election may take, independent of the
/// `attempts`-scaled delay bound (which a livelock would only widen).
/// Derivation: over 47 reseeded runs of cell 10 (`ANIMUS_ELECTION_BP_SEEDS=64`)
/// the attempt count was 1:20 2:8 3:8 4:4 5:1 6:4 7:2, mean 2.5, so one attempt
/// succeeds with probability `s ~= 1/2.5 = 0.4` and the count is roughly
/// geometric (`P(n > m) = 0.6^m`). A per-run false-failure budget of `1e-6`
/// (so a deep nightly of thousands of seeds stays green) gives
/// `m = ceil(ln 1e-6 / ln 0.6) = 28`. Re-derive it if the geometry (K, Q, base)
/// or the election timing changes.
const ATTEMPT_SUCCESS_P: f64 = 0.4;
const ATTEMPT_FALSE_FAIL: f64 = 1e-6;
/// Control (g): the survivors are partitioned for longer than the cap allows
/// (28 attempts at 1.5 s each when partitioned, measured).
const LOSS_WINDOW: Duration = Duration::from_secs(60);

fn attempt_cap() -> u64 {
    (ATTEMPT_FALSE_FAIL.ln() / (1.0 - ATTEMPT_SUCCESS_P).ln()).ceil() as u64
}

/// Which ADR 0078 mechanisms are on, and with which parameters (the calibration
/// sweep varies `governor_k`, `gain`, `stretch_x` and `host_h`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Mechanisms {
    /// Campaign governor `K` (`0` = off).
    governor_k: usize,
    /// A deliberate governor bug (negative controls (c) and (d)).
    fault: GovernorFault,
    /// `lag_stretch_max_x` of the load monitor (`0` = off, the monitor is not
    /// even started).
    stretch_x: u64,
    /// The stretch gain `c` (production: [`STRETCH_C`]).
    gain: u64,
    /// `host_unsettled_max` `H` of the host reconciler (`0` = off: every
    /// pending tablet is hosted in one tick). Only cells that host through the
    /// real `Reconciler` ([`Rig::host_reconciled`]) look at it.
    host_h: usize,
}

impl Mechanisms {
    /// Nothing on: the code as it was before ADR 0078.
    const NONE: Mechanisms = Mechanisms {
        governor_k: 0,
        fault: GovernorFault::None,
        stretch_x: 0,
        gain: STRETCH_C,
        host_h: 0,
    };
    /// The campaign governor at the default `K`.
    const GOVERNOR: Mechanisms = Mechanisms {
        governor_k: K,
        ..Mechanisms::NONE
    };
    /// Governor plus lag stretch at the default `lag_stretch_max_x` (4), the
    /// hosting stagger off: what `animusd` ran after ADR 0078 steps 3 and 4.
    const ALL: Mechanisms = Mechanisms {
        governor_k: K,
        stretch_x: 4,
        ..Mechanisms::NONE
    };
    /// All three mechanisms at their defaults (`K=32`, `c=4`, `S_max=4x`,
    /// `H=100`): the production configuration. Cells that host through the
    /// `Reconciler` use it.
    const ALL_STAGGERED: Mechanisms = Mechanisms {
        host_h: HOST_H,
        ..Mechanisms::ALL
    };
    /// Governor plus an effectively unbounded stretch (`S_max = inf`,
    /// negative control (e)).
    const UNBOUNDED_STRETCH: Mechanisms = Mechanisms {
        stretch_x: 1_000_000,
        ..Mechanisms::GOVERNOR
    };
    /// The governor with a deliberate bug.
    const fn faulty(fault: GovernorFault) -> Mechanisms {
        Mechanisms {
            fault,
            ..Mechanisms::GOVERNOR
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
    /// Cell 2: every group is hosted from a cold start through the real
    /// per-node `Reconciler` (all three mechanisms at their defaults), the node
    /// CPU already at `load` x steady demand. The ramp is observed: never more
    /// than `H` unsettled groups, all hosted within `ceil(G/H) * T_settle`.
    ColdStartStaggered,
    /// Cell 3: converge, then stop node 1 (it leads a third of the groups),
    /// keep it down for [`RESTART_GAP`], restart it: its groups re-host with the
    /// stagger while the survivors have already re-elected.
    NodeRestart,
    /// Cell 2b and control (f): converge, then stop all three nodes and restart
    /// them together. With `shuffled_restart` each node re-hosts in its own
    /// shuffled order instead of ascending `TabletId`.
    WholeClusterRestart,
    /// Cell 9: three regions on WAN links, every tablet prefers region a; the
    /// governors are saturated by ghosts and the CPU is loaded. The reconciler's
    /// preferred-leader step (transfers, exempt from the governor) must still
    /// move every leader to region a, with the WAN-profile term bound of
    /// `wan_timing_corpus`.
    PreferredLeaderStep,
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
    /// Control (f): the restarted nodes re-host in per-node shuffled order.
    shuffled_restart: bool,
    /// Control (g): the lost group's two survivors are cut off from each other
    /// for `LOSS_WINDOW` after the kill, so every campaign attempt is lost.
    lose_attempts: bool,
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
        shuffled_restart: false,
        lose_attempts: false,
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
        cell(
            "cell_2_cold_start_staggered_all_mechanisms",
            Kind::ColdStartStaggered,
            0.9,
            Mechanisms::ALL_STAGGERED,
        ),
        cell(
            "cell_3_node_restart_rehosts_with_stagger",
            Kind::NodeRestart,
            0.9,
            Mechanisms::ALL_STAGGERED,
        ),
        // The ascending-order whole-cluster restart control (f) is read against.
        cell(
            "cell_2b_whole_cluster_restart_ascending",
            Kind::WholeClusterRestart,
            0.9,
            Mechanisms::ALL_STAGGERED,
        ),
        cell(
            "cell_9_preferred_leader_step_under_saturation_wan",
            Kind::PreferredLeaderStep,
            0.9,
            Mechanisms::ALL_STAGGERED,
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

/// Negative control (f): per-node random hosting order on a whole-cluster
/// restart. It shares cell 2b's seed, so the two runs are identical until the
/// restart and differ only in the hosting order.
fn controls_f() -> Vec<Cell> {
    let mut c = cell(
        "control_f_per_node_random_hosting_order_whole_cluster_restart",
        Kind::WholeClusterRestart,
        0.9,
        Mechanisms::ALL_STAGGERED,
    );
    c.seed = corpus::name_seed("cell_2b_whole_cluster_restart_ascending");
    c.shuffled_restart = true;
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

fn controls_g() -> Vec<Cell> {
    let mut c = cell(
        "control_g_lost_attempts_livelock_lost_leader_bound",
        Kind::LostLeaderBound,
        0.0,
        Mechanisms::GOVERNOR,
    );
    c.lose_attempts = true;
    vec![c]
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
    /// The mechanisms the rig was set up with (restarted nodes rebuild theirs).
    mech: Mechanisms,
    /// Hosting goes through the real per-node `host::Reconciler` (cells 2, 3, 9
    /// and control (f)) instead of starting every replica by hand: `nodes` is
    /// then empty and a replica is looked up in its node's reconciler
    /// ([`Rig::replica`]); `placement[gi] == [0, 1, 2]`, so replica index ==
    /// node index.
    reconciled: bool,
    /// Per-node durable engine registries, kept across a node restart.
    engines: Vec<MemoryTabletEngines>,
    recons: Vec<Option<Recon>>,
    /// The metadata every reconciler is ticked with.
    view: MetadataView,
    /// Control (f): every node hosts fresh tablets in its own shuffled order
    /// instead of ascending `TabletId`.
    shuffled_hosting: bool,
    /// Cell 9: the nodes sit in three regions on WAN links.
    wan: bool,
    /// Cell 9: every tablet advertises region `a` as its preferred leader
    /// region, so the reconcilers' preferred-leader step acts.
    prefer: bool,
    /// Governors of node incarnations that were stopped (kept for the totals).
    retired_governors: Vec<CampaignGovernor>,
    /// Highest `hosting_ramp().unsettled` / `.deferred` any live reconciler
    /// reported after a tick.
    max_unsettled: usize,
    max_deferred: usize,
    /// How soon the reconciler loop wants its next tick (the production loop's
    /// 500 ms fallback, or the ramp's own short wake).
    next_recon_in: Duration,
}

/// The production reconciler loop's fallback cadence.
const RECON_FALLBACK: Duration = Duration::from_millis(500);
/// Seed of control (f)'s per-node hosting shuffles.
const SHUFFLE_SEED: u64 = 0x5EED_0F0F;

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
            mech: Mechanisms::NONE,
            reconciled: false,
            engines: Vec::new(),
            recons: Vec::new(),
            view: MetadataView::default(),
            shuffled_hosting: false,
            wan: false,
            prefer: false,
            retired_governors: Vec::new(),
            max_unsettled: 0,
            max_deferred: 0,
            next_recon_in: Duration::ZERO,
        }
    }

    /// One node's campaign governor (`None` = off).
    fn make_governor(&self, i: usize) -> Option<CampaignGovernor> {
        (self.mech.governor_k > 0).then(|| {
            CampaignGovernor::with_fault(
                self.mech.governor_k,
                self.handles[i].clone(),
                self.mech.fault,
            )
        })
    }

    /// One node's load monitor over its governor (`None` = lag stretch off).
    /// The corpus feeds the monitor from the simulator's CPU queue
    /// ([`Rig::probe`]) rather than from a probe task: the CPU model serves
    /// expired timers ahead of waiting deliveries, so the timer-lateness probe
    /// of `LoadMonitor::new` reads ~0 here (it is exercised on a paused node by
    /// `load_monitor_task_measures_a_paused_node`).
    fn make_monitor(&self, i: usize) -> Option<LoadMonitor> {
        (self.mech.stretch_x > 0).then(|| {
            if self.real_probe {
                return LoadMonitor::new(
                    &self.sim.env(nid(NODES[i])),
                    self.mech.stretch_x,
                    BASE,
                    self.handles[i].clone(),
                    self.governors[i].clone(),
                );
            }
            LoadMonitor::detached_with_gain(
                self.mech.gain,
                self.mech.stretch_x,
                BASE,
                self.handles[i].clone(),
                self.governors[i].clone(),
            )
        })
    }

    /// The per-node batchers, governors and monitors (ADR 0044 phase 2, ADR
    /// 0078). A reconciled rig leaves the batchers to the reconcilers.
    fn setup(&mut self, mech: Mechanisms) {
        self.mech = mech;
        self.batchers = if self.reconciled {
            Vec::new()
        } else {
            NODES
                .iter()
                .enumerate()
                .map(|(i, &id)| {
                    HeartbeatBatcher::new(
                        self.sim.env(nid(id)),
                        DEFAULT_HEARTBEAT_BATCH_INTERVAL,
                        self.handles[i].clone(),
                    )
                })
                .collect()
        };
        self.governors = (0..NODES.len()).map(|i| self.make_governor(i)).collect();
        let monitors: Vec<_> = (0..NODES.len()).map(|i| self.make_monitor(i)).collect();
        self.monitors = monitors;
    }

    fn options(&self, node: usize) -> HostedOptions<SimEnv> {
        HostedOptions {
            heartbeat_batcher: Some(self.batchers[node].clone()),
            backpressure: ElectionBackpressure {
                governor: self.governors[node].clone(),
                monitor: self.monitors[node].clone(),
                ..ElectionBackpressure::default()
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

    /// Region label of node `i` (cell 9).
    fn region(i: usize) -> String {
        ["region-a", "region-b", "region-c"][i].to_string()
    }

    /// The metadata every reconciler is ticked with: every group's tablet
    /// replicated on all three nodes; in cell 9 also the node regions and the
    /// preferred leader (region a) of every tablet.
    fn build_view(&self) -> MetadataView {
        let replicas: Vec<NodeId> = NODES.iter().map(|&n| nid(n)).collect();
        let tablets = (0..self.g)
            .map(|gi| {
                let id = Self::tablet(gi);
                (
                    id,
                    Tablet::new_for_table(id, "t", KeyRange::whole(), replicas.clone()),
                )
            })
            .collect();
        let (regions, preferred_leader) = if self.wan {
            (
                (0..3).map(|i| (nid(NODES[i]), Self::region(i))).collect(),
                (0..self.g)
                    .filter(|_| self.prefer)
                    .map(|gi| {
                        (
                            Self::tablet(gi),
                            LeaderPreference {
                                region: Self::region(0),
                                witness: None,
                            },
                        )
                    })
                    .collect(),
            )
        } else {
            (BTreeMap::new(), BTreeMap::new())
        };
        MetadataView {
            tablets,
            regions,
            preferred_leader,
            ..Default::default()
        }
    }

    /// Start advertising region `a` as every tablet's preferred leader region.
    fn set_preferred(&mut self) {
        self.prefer = true;
        self.view = self.build_view();
    }

    fn tablet(gi: usize) -> TabletId {
        TabletId(STREAM0 + gi as u64)
    }

    /// A fresh per-node `Reconciler` in the production configuration (heartbeat
    /// batching, the node's governor and monitor, the hosting stagger).
    fn make_recon(&self, i: usize) -> Recon {
        let id = nid(NODES[i]);
        let mut r: Recon = Reconciler::new(
            self.sim.env(id.clone()),
            self.engines[i].clone(),
            id,
            |_t, _n| {},
            |_t| {},
        );
        r.enable_heartbeat_batching();
        r.set_election_backpressure(ElectionBackpressure {
            governor: self.governors[i].clone(),
            monitor: self.monitors[i].clone(),
            ..ElectionBackpressure::default()
        });
        r.set_host_stagger(self.mech.host_h, DEFAULT_HOST_SETTLE_TIMEOUT);
        if self.shuffled_hosting {
            r.set_host_order_fault(Some(SHUFFLE_SEED + i as u64));
        }
        if self.wan {
            r.set_max_region_rtt(crate::wan_timing_corpus::MAX_REGION_RTT);
        }
        r
    }

    /// Host all `g` RF3 groups through the real per-node `Reconciler`s (cells
    /// 2, 3, 9 and control (f)): no group exists until a reconciler tick hosts
    /// it, `mech.host_h` waves of them. [`Rig::reconcile`] ticks them.
    ///
    /// **Why a reconciler in the rig and not `host::plan` with a budget:** the
    /// stagger is a property of the loop (the settle tracker, the ramp's own
    /// short wake and the per-tick budget all live in `Reconciler::tick`), so
    /// driving `plan` alone would test the arithmetic and not the loop. A
    /// reconciler tick that only hosts never awaits a timer, so `block_on` is
    /// safe (the `wan_timing_corpus` convention).
    fn host_reconciled(&mut self, mech: Mechanisms) {
        self.reconciled = true;
        self.engines = (0..NODES.len())
            .map(|_| MemoryTabletEngines::new())
            .collect();
        self.setup(mech);
        self.placement = vec![vec![0, 1, 2]; self.g];
        self.view = self.build_view();
        self.recons = (0..NODES.len()).map(|i| Some(self.make_recon(i))).collect();
        self.next_recon_in = Duration::ZERO;
    }

    /// One reconciler tick on every live node, then record the ramp numbers
    /// and decide when the loop wants its next tick (the ramp's short wake, else
    /// the 500 ms fallback).
    fn reconcile(&mut self) {
        let mut next = RECON_FALLBACK;
        let Rig { recons, view, .. } = self;
        for r in recons.iter_mut().flatten() {
            block_on(r.tick(view));
            let ramp = r.hosting_ramp();
            self.max_unsettled = self.max_unsettled.max(ramp.unsettled);
            self.max_deferred = self.max_deferred.max(ramp.deferred);
            if let Some(w) = r.next_wake() {
                next = next.min(w);
            }
        }
        self.next_recon_in = next;
    }

    /// Whether every live node's reconciler has hosted every tablet.
    fn all_hosted(&self) -> bool {
        self.recons
            .iter()
            .flatten()
            .all(|r| r.local_state().hosted.len() == self.g)
    }

    /// Stop node `i` as a process exit would: its tasks and reconciler are
    /// gone, its durable disk and engine registry stay.
    fn stop_node(&mut self, i: usize) {
        self.sim.stop(nid(NODES[i]));
        self.recons[i] = None;
        self.live[i] = false;
    }

    /// Start a fresh process on node `i` after [`Rig::stop_node`]: new governor,
    /// monitor and reconciler (no memory of the old incarnation), the same
    /// engine registry and Raft WAL, `LocalState::default()`.
    fn start_node(&mut self, i: usize) {
        if let Some(g) = self.governors[i].take() {
            self.retired_governors.push(g);
        }
        self.governors[i] = self.make_governor(i);
        self.monitors[i] = self.make_monitor(i);
        self.recons[i] = Some(self.make_recon(i));
        self.live[i] = true;
        self.next_recon_in = Duration::ZERO;
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

    /// Replica `ri` of group `gi`, if it is hosted (always, unless the rig
    /// hosts through reconcilers and this one has not been hosted yet or its
    /// node is stopped).
    fn replica(&self, gi: usize, ri: usize) -> Option<&KvNode> {
        if self.reconciled {
            self.recons[self.placement[gi][ri]]
                .as_ref()?
                .hosted_node(Self::tablet(gi))
        } else {
            self.nodes.get(gi)?.get(ri)
        }
    }

    /// The hosted replicas of group `gi` with their replica index.
    fn replicas(&self, gi: usize) -> Vec<(usize, &KvNode)> {
        (0..self.placement[gi].len())
            .filter_map(|ri| self.replica(gi, ri).map(|n| (ri, n)))
            .collect()
    }

    /// Every hosted replica of every group.
    fn all_replicas(&self) -> Vec<&KvNode> {
        (0..self.g)
            .flat_map(|gi| self.replicas(gi).into_iter().map(|(_, n)| n))
            .collect()
    }

    /// The index of the replica of `gi` leading on a live node, if any.
    fn live_leader(&self, gi: usize) -> Option<usize> {
        self.replicas(gi)
            .into_iter()
            .find(|(ri, n)| self.replica_live(gi, *ri) && n.is_leader())
            .map(|(ri, _)| ri)
    }

    /// Groups led by a replica on a live node.
    fn led(&self) -> usize {
        (0..self.g)
            .filter(|&gi| self.live_leader(gi).is_some())
            .count()
    }

    /// Groups whose replica on node `ni` is the leader.
    fn led_by(&self, ni: usize) -> usize {
        (0..self.g)
            .filter(|&gi| self.replica(gi, ni).is_some_and(KvNode::is_leader))
            .count()
    }

    /// Groups with exactly one leader among all replicas (live or not).
    fn exactly_one_leader(&self) -> usize {
        (0..self.g)
            .filter(|&gi| {
                self.replicas(gi)
                    .iter()
                    .filter(|(_, n)| n.is_leader())
                    .count()
                    == 1
            })
            .count()
    }

    /// Sum over groups of `max replica term - 1` (the term churn), and the
    /// largest single term.
    fn churn(&self) -> (u64, u64) {
        let mut sum = 0;
        let mut max = 0;
        for gi in 0..self.g {
            let t = self
                .replicas(gi)
                .iter()
                .map(|(_, n)| n.term())
                .max()
                .unwrap_or(0);
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
        for g in self
            .governors
            .iter()
            .flatten()
            .chain(&self.retired_governors)
        {
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
        for gi in 0..rig.g {
            for (ri, r) in rig.replicas(gi) {
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
    /// Reconciled cells: the largest `hosting_ramp().unsettled` / `.deferred` a
    /// live reconciler reported (since the last restart, in restart cells).
    max_unsettled: usize,
    max_deferred: usize,
    /// Reconciled cells: seconds until every live reconciler had hosted every
    /// tablet (from the start, or from the restart in restart cells).
    hosted_all_s: Option<f64>,
    /// Restart cells: seconds from the (last) node start until every group was
    /// led by a live node.
    since_restart_s: Option<f64>,
    /// Restart cells: group-seconds without a leader since the restart (the
    /// area under the leaderless count; finer than the first-all-led sample).
    leaderless_gs: f64,
    /// Cell 9: the largest per-group term growth after the governors were
    /// saturated, and how many groups ended led from region a.
    term_growth: u64,
    led_in_a: usize,
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
    /// `H` the reconcilers ran with (`0` = no ramp clauses).
    host_h: usize,
    /// Check that every tablet was hosted everywhere within the ramp bound.
    ramp_bound: bool,
    /// Whether the led count must stay monotone after convergence (not in a
    /// cell whose steady state moves leaders on purpose).
    monotone: bool,
}

impl Limits {
    const DEFAULT: Limits = Limits {
        all_led_s: 10.0,
        churn_per_group: 0.25,
        recovery_s: None,
        governor_k: 0,
        stretch_x: 0,
        expect_lag: false,
        host_h: 0,
        ramp_bound: false,
        monotone: true,
    };

    fn for_cell(c: &Cell) -> Limits {
        let base = Limits {
            governor_k: c.mech.governor_k,
            stretch_x: c.mech.stretch_x,
            host_h: c.mech.host_h,
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
            Kind::ColdStartStaggered => Limits {
                ramp_bound: true,
                ..base
            },
            // One third of the groups lose their leader with the node.
            Kind::NodeRestart => Limits {
                churn_per_group: 0.75,
                recovery_s: Some(5.0),
                ramp_bound: true,
                ..base
            },
            // Every group re-elects after the restart: about a term each.
            Kind::WholeClusterRestart => Limits {
                churn_per_group: 1.5,
                recovery_s: Some(15.0),
                ramp_bound: true,
                ..base
            },
            // Leaders move on purpose, so the led count is not monotone; the
            // WAN groups' initial election is slow.
            Kind::PreferredLeaderStep => Limits {
                all_led_s: 20.0,
                churn_per_group: 2.0,
                monotone: false,
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
        if self.monotone && r.led_drop > 0 {
            v.push("monotone: led count fell after first convergence");
        }
        if self.host_h > 0 {
            if r.max_unsettled > self.host_h {
                v.push("ramp: more than H freshly hosted groups were unsettled at once");
            }
            if r.max_deferred == 0 && r.groups > self.host_h {
                v.push(
                    "harness: the stagger never deferred a tablet, the ramp clauses are vacuous",
                );
            }
        }
        if self.ramp_bound {
            match r.hosted_all_s {
                None => v.push("liveness: a node never hosted every tablet"),
                Some(t) if t > ramp_bound_s(r.groups, self.host_h) => {
                    v.push("ramp: hosting took longer than ceil(G/H) * T_settle");
                }
                Some(_) => {}
            }
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

/// ADR 0078 "Staggered hosting", liveness: the ramp takes at most
/// `ceil(G/H) * T_settle` (`T_settle` the settle timeout, the worst case of a
/// group that never hears a leader), plus one reconciler wake (50 ms) per wave
/// and 100 ms of slack. `H = 0` (no stagger) is one tick.
fn ramp_bound_s(groups: usize, h: usize) -> f64 {
    let waves = if h == 0 { 1 } else { groups.div_ceil(h) } as f64;
    waves * DEFAULT_HOST_SETTLE_TIMEOUT.as_secs_f64() + waves * 0.05 + 0.1
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
        for gi in (0..rig.g).step_by(every) {
            let Some(ri) = rig.live_leader(gi) else {
                continue;
            };
            let Some(n) = rig.replica(gi, ri) else {
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
            // A replica that is gone (its node restarted) never acks.
            let Some(n) = rig.replica(gi, ri) else {
                continue;
            };
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
                rig.replicas(*gi).into_iter().any(|(ri, n)| {
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
    /// Reconciled cells: when hosting (or the last restart) began, and how long
    /// until every live reconciler had hosted every tablet.
    hosting_from: Duration,
    hosted_all_s: Option<f64>,
    /// When the last node (re)start happened, in seconds.
    restart_at_s: Option<f64>,
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
            hosting_from: Duration::ZERO,
            hosted_all_s: None,
            restart_at_s: None,
        }
    }

    /// A node (re)start begins now: the ramp readings start over.
    fn begin_restart(&mut self) {
        self.hosting_from = self.at;
        self.hosted_all_s = None;
        self.restart_at_s = Some(self.at.as_secs_f64());
        self.rig.max_unsettled = 0;
        self.rig.max_deferred = 0;
    }

    /// Run the simulator for `step`, taking a lag probe every
    /// [`animus_cp_data::load_monitor::PROBE`] of virtual time and, on a
    /// reconciled rig, ticking the reconcilers on their own cadence (the
    /// ramp's 50 ms wake, else the 500 ms fallback).
    fn run_probed(&mut self, step: Duration) {
        let probed = self.rig.monitors.iter().any(Option::is_some);
        let reconciled = self.rig.reconciled;
        if !probed && !reconciled {
            self.rig.sim.run_for(step);
            return;
        }
        let mut left = step;
        while !left.is_zero() {
            if reconciled && self.rig.next_recon_in.is_zero() {
                self.rig.reconcile();
            }
            let mut chunk = left;
            if probed {
                chunk = chunk.min(animus_cp_data::load_monitor::PROBE - self.since_probe);
            }
            if reconciled {
                chunk = chunk.min(self.rig.next_recon_in);
            }
            self.rig.sim.run_for(chunk);
            left -= chunk;
            if reconciled {
                self.rig.next_recon_in -= chunk;
            }
            if probed {
                self.since_probe += chunk;
                if self.since_probe >= animus_cp_data::load_monitor::PROBE {
                    self.since_probe = Duration::ZERO;
                    self.rig.probe();
                }
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
            if self.rig.reconciled && self.hosted_all_s.is_none() && self.rig.all_hosted() {
                self.hosted_all_s = Some((self.at - self.hosting_from).as_secs_f64());
            }
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
            hosted_all_s,
            restart_at_s,
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
            max_unsettled: rig.max_unsettled,
            max_deferred: rig.max_deferred,
            hosted_all_s,
            since_restart_s: s.recovered_at_s.zip(restart_at_s).map(|(r, f)| r - f),
            leaderless_gs: restart_at_s.map_or(0.0, |r0| {
                s.series
                    .windows(2)
                    .filter(|w| w[1].at_s > r0)
                    .map(|w| (g - w[1].led) as f64 * (w[1].at_s - w[0].at_s))
                    .sum()
            }),
            term_growth: 0,
            led_in_a: 0,
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
        drop(rig.recons);
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
            let led_by_dead = r.rig.led_by(0);
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
        Kind::ColdStartStaggered => run_cold_start_staggered(r, c),
        Kind::NodeRestart => run_node_restart(r, c),
        Kind::WholeClusterRestart => run_whole_cluster_restart(r, c),
        Kind::PreferredLeaderStep => run_preferred_leader_step(r, c),
    }
}

/// How long node 1 stays down in cell 3.
const RESTART_GAP: Duration = Duration::from_secs(3);

/// Cell 2: cold start through the reconcilers, all mechanisms on.
fn run_cold_start_staggered(mut r: Run, c: &Cell) -> Report {
    r.rig.set_load(c.load);
    r.rig.host_reconciled(c.mech);
    r.advance(COLD_WINDOW, SAMPLE, false);
    r.w.propose(&r.rig, 10);
    r.advance(Duration::from_secs(5), SAMPLE, true);
    r.finish(Vec::new(), String::new())
}

/// Cell 3: node 1 (a third of the leaders) is stopped and restarted; its
/// groups re-host in waves.
fn run_node_restart(mut r: Run, c: &Cell) -> Report {
    r.rig.host_reconciled(c.mech);
    r.converge_and_write();
    r.rig.set_load(c.load);
    let led_by_stopped = r.rig.led_by(1);
    r.s.begin_fault(r.at.as_secs_f64());
    r.rig.stop_node(1);
    r.advance(RESTART_GAP, FINE, true);
    // The survivors have re-elected by now; the restarted node then re-hosts
    // its 300 groups under the stagger.
    let survivors_led = r.rig.led();
    r.begin_restart();
    r.rig.start_node(1);
    r.advance(FAULT_WINDOW, FINE, true);
    r.w.propose(&r.rig, 10);
    r.advance(Duration::from_secs(5), SAMPLE, true);
    let mut extra = Vec::new();
    if survivors_led != r.rig.g {
        extra.push("liveness: the survivors had not re-elected every group before the restart");
    }
    let detail = format!("led_by_stopped_node={led_by_stopped} survivors_led={survivors_led}");
    r.finish(extra, detail)
}

/// Cell 2b and control (f): all three nodes stop and restart together.
fn run_whole_cluster_restart(mut r: Run, c: &Cell) -> Report {
    r.rig.host_reconciled(c.mech);
    r.converge_and_write();
    r.rig.set_load(c.load);
    r.s.begin_fault(r.at.as_secs_f64());
    for i in 0..NODES.len() {
        r.rig.stop_node(i);
    }
    r.advance(Duration::from_secs(1), FINE, false);
    // The hosting order is the one variable of control (f).
    r.rig.shuffled_hosting = c.shuffled_restart;
    r.begin_restart();
    for i in 0..NODES.len() {
        r.rig.start_node(i);
    }
    r.advance(Duration::from_secs(30), FINE, false);
    r.w.propose(&r.rig, 10);
    r.advance(Duration::from_secs(5), SAMPLE, true);
    r.finish(Vec::new(), String::new())
}

/// Cell 9: the preferred-leader step on three WAN regions with saturated
/// governors.
fn run_preferred_leader_step(mut r: Run, c: &Cell) -> Report {
    use crate::wan_timing_corpus::{MAX_TERM_GROWTH, WAN_ONE_WAY_MS, wan_link};
    for ((i, j), ms) in WAN_ONE_WAY_MS {
        let cfg = wan_link(ms);
        r.rig
            .sim
            .set_link_net_config(nid(NODES[i]), nid(NODES[j]), cfg.clone());
        r.rig
            .sim
            .set_link_net_config(nid(NODES[j]), nid(NODES[i]), cfg);
    }
    r.rig.wan = true;
    r.rig.host_reconciled(c.mech);
    // The WAN groups' first election takes up to two of their (>= 1 s) bases.
    r.advance(Duration::from_secs(8), SAMPLE, false);
    r.w.propose(&r.rig, 10);
    r.advance(Duration::from_secs(2), SAMPLE, true);
    let base_terms: Vec<u64> = (0..r.rig.g)
        .map(|gi| {
            r.rig
                .replicas(gi)
                .iter()
                .map(|(_, n)| n.term())
                .max()
                .unwrap_or(0)
        })
        .collect();
    // From here the governors are saturated and the CPU is loaded: the step's
    // transfers must still go through.
    r.rig.spawn_ghosts(3 * K);
    r.rig.set_load(c.load);
    r.advance(Duration::from_secs(3), SAMPLE, true);
    // Now the operator's `SetGlobalPreferredLeader`: a transfer is needed for
    // the two thirds of the groups whose leader sits in region b or c.
    r.rig.set_preferred();
    let all_in_a = |rig: &Rig| rig.led_by(0) == rig.g;
    let converged = r.advance_until(Duration::from_secs(120), SAMPLE, all_in_a);
    r.advance(Duration::from_secs(20), SAMPLE, true);
    let mut extra = Vec::new();
    if converged.is_none() {
        extra.push("preferred: not every leader reached the preferred region");
    }
    let led_in_a = r.rig.led_by(0);
    if led_in_a != r.rig.g {
        extra.push("preferred: a leader left the preferred region again");
    }
    let growth = (0..r.rig.g)
        .map(|gi| {
            let t = r
                .rig
                .replicas(gi)
                .iter()
                .map(|(_, n)| n.term())
                .max()
                .unwrap_or(0);
            t.saturating_sub(base_terms[gi])
        })
        .max()
        .unwrap_or(0);
    // `wan_timing_corpus`'s bound, unchanged: a group's term moves by at most
    // `MAX_TERM_GROWTH` after its first election (here: one transfer plus the
    // odd retry).
    if growth > MAX_TERM_GROWTH {
        extra.push("wan: a group's term grew by more than wan_timing_corpus' MAX_TERM_GROWTH");
    }
    let hwm = r.rig.gov_totals();
    if hwm.inflight_hwm <= K {
        extra.push("harness: the governors were not saturated");
    }
    let transfers: u64 = (0..NODES.len())
        .map(|i| {
            r.rig
                .sim
                .env(nid(NODES[i]))
                .metrics()
                .get(Metric::CpPreferredLeaderTransfers)
        })
        .sum();
    if transfers == 0 {
        extra.push("harness: the preferred-leader step never transferred anything");
    }
    let detail = format!(
        "to_region_a_s={converged:?} term_growth={growth} transfers={transfers} governor={hwm:?}"
    );
    let mut report = r.finish(extra, detail);
    report.term_growth = growth;
    report.led_in_a = led_in_a;
    report
}

/// Cell 4 (and control (c)): quiesce everything, kill the node leading a third
/// of the groups and wake the survivors together.
fn run_leader_node_loss(mut r: Run, c: &Cell) -> Report {
    r.rig.host(c.mech);
    r.converge_and_write();
    for n in r.rig.all_replicas() {
        n.enable_quiescence(Duration::from_secs(2));
    }
    let quiesced = r.advance_until(Duration::from_secs(40), Duration::from_secs(1), |rig| {
        rig.all_replicas().into_iter().all(KvNode::is_quiesced)
    });
    let mut extra = Vec::new();
    if quiesced.is_none() {
        extra.push("harness: the groups did not all quiesce before the fault");
    }
    let led_by_dead = r.rig.led_by(0);
    r.rig.set_load(c.load);
    r.s.begin_fault(r.at.as_secs_f64());
    r.rig.kill(0);
    // The reconciler's `down`-driven wake: every group whose replica set
    // contains the down node is woken on the survivors, all in one pass.
    for gi in 0..r.rig.g {
        for (_, n) in r.rig.replicas(gi).into_iter().filter(|(ri, _)| *ri > 0) {
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
    let from = r
        .rig
        .replicas(gi)
        .iter()
        .position(|(_, n)| n.is_leader())
        .expect("group 0 is led");
    let to = (from + 1) % NODES.len();
    let armed = r.rig.nodes[gi][from].transfer_leadership(nid(NODES[to]));
    let took = r.advance_until(Duration::from_secs(2), Duration::from_millis(10), |rig| {
        rig.replica(gi, to).is_some_and(KvNode::is_leader)
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
    let ri = r
        .rig
        .replicas(gi)
        .iter()
        .position(|(_, n)| n.is_leader())
        .expect("group 0 is led");
    // Pre-vote rounds each survivor's core has started for group `gi`: the
    // per-group attempt counter (the node-wide `CpCampaignAdmitted` counts
    // every hosted group and the ghosts, so it would make the bound vacuous).
    let rounds_of = |rig: &Rig, n: usize| {
        rig.replica(gi, n)
            .map_or(0, |k| k.campaign_stats().rounds_started)
    };
    let rounds_before: Vec<u64> = (0..NODES.len()).map(|n| rounds_of(&r.rig, n)).collect();
    r.s.begin_fault(r.at.as_secs_f64());
    r.rig.replica(gi, ri).expect("hosted").shutdown();
    // The halted replica's frozen handle still reports `is_leader`.
    r.rig.halted.push((gi, ri));
    // Control (g): every attempt is lost while the survivors cannot talk.
    let mut loss_s = 0.0;
    if c.lose_attempts {
        let (a, b) = match ri {
            0 => (1, 2),
            1 => (0, 2),
            _ => (0, 1),
        };
        r.rig.sim.partition_pair(nid(NODES[a]), nid(NODES[b]));
        r.advance(LOSS_WINDOW, SAMPLE, false);
        r.rig.sim.heal(nid(NODES[a]), nid(NODES[b]));
        loss_s = LOSS_WINDOW.as_secs_f64();
    }
    let delay = r.advance_until(Duration::from_secs(8), Duration::from_millis(10), |rig| {
        rig.replicas(gi)
            .iter()
            .any(|(j, n)| *j != ri && n.is_leader())
    });
    // ADR 0078, "Lost-leader liveness bound": per campaign attempt,
    // `delay <= 2*base_eff + ceil(Q/K_eff) * T_hold_max`, `T_hold_max =
    // 2*base_eff`; `base_eff = base` here (no lag stretch). Two harness terms
    // the literal formula does not carry (issue #1306):
    //
    // * The queue ahead of the lost group is the ghosts (the awake groups are
    //   served by the lost-leader bypass, ADR 0079). A ghost holds its permit
    //   for `2*base` like a real campaign, but it is not woken on a grant: it
    //   re-asks on a `base` sleep (`spawn_ghosts`), and the governor protects
    //   the queue head, so a grant waits up to one `GHOST_REASK = base` for the
    //   ghost at the head to come back. Each of the `ceil(Q/K)` passes
    //   therefore costs `2*base + GHOST_REASK`, not `2*base`. (A real group is
    //   woken by its callback at once, so production has no such term.) The
    //   canonical seed lands inside the literal bound only by phase luck;
    //   reseeded variants measured 1.21 s against 0.95 s.
    // * The formula stops at admission; the admitted group still needs its own
    //   pre-vote / vote / commit round trips and the probe resolution is 10 ms,
    //   so `OWN_ELECTION_SLACK` (50 ms) is added.
    //
    // A campaign attempt can also be lost without a term bump (a pre-vote
    // round that fails when both followers pre-campaign together, or a split
    // vote), costing a further full window; `attempts` is the most pre-vote
    // rounds either survivor started for this group since the kill (floor 1).
    // A permit leak (control (d)) admits nothing, so it still gets the
    // single-attempt bound and misses it by seconds.
    const OWN_ELECTION_SLACK: f64 = 0.05;
    const GHOST_REASK: f64 = BASE.as_secs_f64();
    let attempts = (0..NODES.len())
        .filter(|&n| n != ri)
        .map(|n| rounds_of(&r.rig, n) - rounds_before[n])
        .max()
        .unwrap_or(1)
        .max(1);
    let round = 2.0 * BASE.as_secs_f64()
        + (q.div_ceil(K) as f64) * (2.0 * BASE.as_secs_f64() + GHOST_REASK);
    let bound = round * attempts as f64 + OWN_ELECTION_SLACK;
    let delay = delay.map(|d| d + loss_s);
    let mut extra = Vec::new();
    if attempts > attempt_cap() {
        extra.push("livelock: more lost campaign attempts than the geometry allows");
    }
    match delay {
        None => extra.push("bound: the lost group never got a leader (starved)"),
        Some(d) if d > bound => {
            extra.push("bound: lost-leader delay above attempts * (2*base + ceil(Q/K)*T_hold_max)")
        }
        Some(_) => {}
    }
    let detail = format!("q={q} k={K} attempts={attempts} delay_s={delay:?} bound_s={bound:.3}");
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
    for n in r.rig.all_replicas() {
        n.enable_quiescence(Duration::from_secs(2));
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
    let clean = cells()
        .into_iter()
        .find(|c| c.name == "cell_4_leader_node_loss_quiesced")
        .expect("cell 4");
    for c in select(controls_c()) {
        let r = run_cell(&c, groups());
        let mut k = clean.clone();
        k.seed = c.seed;
        let rk = run_cell(&k, groups());
        let v = Limits::for_cell(&c).violations(&r);
        eprintln!("BP-CONTROL {} seed={:#x} {v:?} {r:?}", c.name, c.seed);
        eprintln!("BP-CONTROL-CLEAN {} {rk:?}", c.name);
        eprintln!("BP-DETAIL {} {}", c.name, r.detail);
        assert!(
            v.iter().all(|x| !is_safety(x)),
            "{} seed={:#x}: safety must hold: {v:?} {r:?}",
            c.name,
            c.seed
        );
        // The deadlock needs admitted campaigners to meet deferred voters in
        // the same wake wave, which a seed may or may not arrange. The frozen
        // canonical seed must break the oracle's bound outright; a reseeded
        // variant must recover no faster than the real governor on the same
        // seed (it may only tie or lose).
        let canonical = controls_c().iter().any(|b| b.name == c.name);
        if canonical {
            assert!(
                v.contains(&"liveness: recovery after the fault above bound")
                    || v.contains(&"liveness: groups never recovered after the fault"),
                "{} seed={:#x}: recovery must exceed the bound: {v:?} {r:?}",
                c.name,
                c.seed
            );
        } else {
            let clean_s = rk.recovery_s.expect("the real governor recovers");
            let ok = r
                .recovery_s
                .is_none_or(|t| t + RECOVERY_NOISE_S + 1e-9 >= clean_s);
            assert!(
                ok,
                "{} seed={:#x}: the faulty run recovered faster than the real governor \
                 ({:?} vs {clean_s}): {r:?}",
                c.name, c.seed, r.recovery_s
            );
        }
    }
}

/// NEGATIVE CONTROL (f): a per-node random hosting order instead of ascending
/// `TabletId`. On a whole-cluster restart every node derives the same ascending
/// order from replicated metadata, so a tablet's three replicas are in the
/// same wave and can elect together; per-node orders spread them over different
/// waves, and the time until every group has a leader (a quorum) is longer.
/// The two runs share a seed and are identical until the restart.
#[test]
fn control_f_per_node_hosting_order_delays_quorum() {
    let ascending = cells()
        .into_iter()
        .find(|c| c.name == "cell_2b_whole_cluster_restart_ascending")
        .expect("cell 2b");
    for f in select(controls_f()) {
        let mut a = ascending.clone();
        a.seed = f.seed;
        let ra = run_cell(&a, groups());
        let rf = run_cell(&f, groups());
        let va = Limits::for_cell(&a).violations(&ra);
        let vf = Limits::for_cell(&f).violations(&rf);
        eprintln!("BP-CONTROL {} seed={:#x} {vf:?} {rf:?}", f.name, f.seed);
        eprintln!("BP-CONTROL-BASELINE ascending {va:?} {ra:?}");
        assert!(va.is_empty(), "the ascending run must pass: {va:?} {ra:?}");
        assert!(
            vf.iter().all(|x| !is_safety(x)),
            "{} seed={:#x}: safety must hold: {vf:?} {rf:?}",
            f.name,
            f.seed
        );
        let asc = ra.since_restart_s.expect("ascending run recovers");
        // A shuffled run that never recovers is the deferred-retry permit leak
        // (a deferred PreCandidate re-took an exempt slot and left the queue),
        // not a slower order: it is a failure, not a pass.
        // The cost of the shuffled order is the integral of leaderless groups
        // (`leaderless_gs`, an average over all 300 groups: 196..208 against
        // 138..142 group-s on the 4 measured seeds, a margin of 40 percent).
        // `since_restart_s` is a maximum over those groups, set by the single
        // slowest one; on reseeded variants it ties or dips below the
        // ascending run by up to 0.2 s (1.2 s against 1.4 s on `s03`), so
        // only the frozen canonical seed is held to it.
        let canonical = controls_f().iter().any(|b| b.name == f.name);
        match rf.since_restart_s {
            None => panic!("{} seed={:#x}: never recovered: {rf:?}", f.name, f.seed),
            Some(t) => {
                assert!(
                    (!canonical || t >= asc) && rf.leaderless_gs > ra.leaderless_gs,
                    "{} seed={:#x}: shuffled order took {t}s / {} leaderless group-s, \
                     not worse than ascending {asc}s / {}",
                    f.name,
                    f.seed,
                    rf.leaderless_gs,
                    ra.leaderless_gs
                );
            }
        }
    }
}

/// NEGATIVE CONTROL (g): the survivors of the lost group cannot talk for
/// `LOSS_WINDOW`, so every campaign attempt is lost and the attempt count
/// climbs. The delay bound scales with `attempts` and still holds; only the
/// livelock guard (`attempt_cap`) may fail it.
#[test]
fn control_g_lost_attempts_livelock_is_caught_by_the_attempt_cap() {
    for c in select(controls_g()) {
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
            v.iter().any(|x| x.starts_with("livelock:")),
            "{} seed={:#x}: the attempt cap must fire: {v:?} {r:?}",
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

/// ADR 0078 step 6: the calibration sweep. One parameter varies at a time
/// around the defaults (`K=32`, `c=4`, `S_max=4x`, `H=100`) over the five cells
/// that stress it; the table is printed as `BP-SWEEP` lines. Not an assertion
/// test (the defaults are pinned by the corpus cells): it exists so the numbers
/// in the ADR can be regenerated.
///
/// ```text
/// cargo test --release -p animus-cp-data --test it \
///   election_backpressure_corpus::calibration_sweep -- --ignored --nocapture
/// ```
#[test]
#[ignore = "manual calibration sweep (ADR 0078 step 6); minutes in release"]
fn calibration_sweep() {
    let wanted = [
        "cell_5_sustained_overload",
        "cell_4b_leader_node_loss_under_lag",
        "cell_2_cold_start_staggered_all_mechanisms",
        "cell_2b_whole_cluster_restart_ascending",
        "cell_10_lost_leader_bound",
    ];
    let base: Vec<Cell> = cells()
        .into_iter()
        .filter(|c| wanted.contains(&c.name.as_str()))
        .collect();
    assert_eq!(base.len(), wanted.len(), "a sweep cell was renamed");
    // `(label, edit)`; `u64`/`usize` values are what the cell's `Mechanisms`
    // takes. "unbounded" K is a cap no cell can reach.
    type Edit = (&'static str, fn(&mut Mechanisms));
    let variants: Vec<Edit> = vec![
        ("default", |_| {}),
        ("K=8", |m| m.governor_k = 8),
        ("K=16", |m| m.governor_k = 16),
        ("K=64", |m| m.governor_k = 64),
        ("K=unbounded", |m| m.governor_k = 100_000),
        ("c=2", |m| m.gain = 2),
        ("c=8", |m| m.gain = 8),
        ("S_max=2x", |m| m.stretch_x = 2),
        ("S_max=8x", |m| m.stretch_x = 8),
        ("H=50", |m| m.host_h = 50),
        ("H=200", |m| m.host_h = 200),
    ];
    for (label, edit) in variants {
        for b in &base {
            let mut c = b.clone();
            edit(&mut c.mech);
            // H only means something where the Reconciler hosts: test the
            // unedited cell, the edit itself sets `host_h`.
            if label.starts_with("H=") && b.mech.host_h == 0 {
                continue;
            }
            let r = run_cell(&c, groups());
            let v = Limits::for_cell(&c).violations(&r);
            eprintln!(
                "BP-SWEEP {label:>11} {:<46} all_led={:?} churn={:.3} max_term={} \
                 recovery={:?} since_restart={:?} leaderless_gs={:.1} inflight_hwm={} \
                 deferred={} max_stretch_ms={} violations={v:?}",
                c.name,
                r.time_to_all_led_s,
                r.churn_per_group,
                r.max_term,
                r.recovery_s,
                r.since_restart_s,
                r.leaderless_gs,
                r.gov.inflight_hwm,
                r.gov.deferred,
                r.max_stretch_ms,
            );
        }
    }
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
        .chain(controls_f())
        .chain(controls_g())
        .collect();
    let names: BTreeSet<&str> = all.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names.len(), all.len());
    for c in &all {
        // Controls (e) and (f) deliberately share a cell's seed.
        if !c.name.starts_with("control_e") && !c.name.starts_with("control_f") {
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
