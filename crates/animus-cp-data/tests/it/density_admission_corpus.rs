//! ADR 0079 (per-node density, layer 2: awake-group admission) Tier 1: a
//! seed-reproducible `SimEnv` corpus of **many Raft groups on three nodes
//! that each cap their awake population**.
//!
//! Three nodes host `G` RF3 groups through the real per-node
//! `host::Reconciler` in the production configuration (heartbeat batching,
//! quiescence after 2 s, the ADR 0078 campaign governor and load monitor, the
//! hosting stagger) plus the node's [`AwakeAdmission`] with cap `A`. A cell is
//! data ([`Kind`]) and runs on the same rig; every cell asserts the Raft
//! safety oracle (no two leaders in one `(group, term)`, no acked write lost)
//! and the clauses below it names.
//!
//! Cells (numbering is the ADR's Tier 1 list):
//!
//! 1. **mass wake**: every group quiesced, node 0 killed and marked `Down`;
//!    the reconcilers wake every group touching it at once. The awake
//!    high-water of each survivor stays at `A_eff` plus the counted over-cap
//!    wakes (and far below the uncapped baseline run on the same seed), the
//!    wake queue was used, and every group is served (led, writable).
//! 2. **write to a deferred group**: the caps are full, a group's followers
//!    sit in the wake queue, a client writes to its leader. The write is
//!    served and replicated, the followers leave the queue, the wake is counted
//!    as over-cap (the cap is soft); a client `wake()` of another quiesced
//!    follower is admitted over the cap as well.
//! 3. **awake overload, cap vs no cap**: the CPU model makes `A` awake groups
//!    exactly a node's capacity; the same mass wake runs with and without the
//!    cap. Capped, the scheduling lag stays below the uncapped run's, every
//!    group is led at the end and the led count never falls once converged.
//!    Cell 3b (ADR 0079 step 7), **absorbable overload**: the same node loss, but
//!    the CPU model gives each survivor capacity above what the lost-leader
//!    groups plus `A` need (`1.5 x (G/N + A)` awake groups), so the capped run is
//!    not saturated by design. Capped, the worst scheduling lag is not worse than
//!    uncapped and no fewer groups are led under load.
//! 4. **cap and lag**: lag is injected into node 1's monitor; its `A_eff`
//!    falls to `A/4` and bulk wakes stop at that bound while node 0 (no lag)
//!    still admits `A`; when the lag clears `A_eff` recovers and the queue
//!    drains.
//! 5. **the cap does not break elections**: the governor saturated by ghosts,
//!    the wake queue full of real deferred groups, an awake group's leader
//!    lost: ADR 0078 cell 10's lost-leader bound still holds.
//!
//! Negative controls (each asserts the oracle FAILS):
//!
//! - **(b)** a hard cap that refuses wakes ([`AwakeFault::HardCap`]) on cell 2:
//!   the client write never replicates.
//! - **(c)** no lag reduction ([`AwakeFault::IgnoreLag`]) on cell 4: bulk wakes
//!   continue past `A/4` under lag.
//!
//! Knobs: `ANIMUS_DENSITY_SEEDS=K` (default 1, [`corpus::seed_expand`]),
//! `ANIMUS_DENSITY_CELL=<substring>` narrows, `ANIMUS_SEED=<seed>` replays one
//! run, `ANIMUS_DENSITY_GROUPS=G` resizes (default [`GROUPS`]; `A` is `G/10`).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::awake_admission::{AwakeAdmission, AwakeFault, AwakeStats};
use animus_cp_data::campaign_governor::{
    Admission, CampaignGovernor, CampaignWake, ElectionBackpressure,
};
use animus_cp_data::host::{
    DEFAULT_HOST_SETTLE_TIMEOUT, MemoryTabletEngines, MetadataView, Reconciler,
};
use animus_cp_data::load_monitor::{LoadMonitor, PROBE};
use animus_cp_data::{KIND_BASE, RaftKvNode};
use animus_env::{Clock, EnvExt, Metric, MetricsHandle, Nanos, nid};
use animus_sim::{CpuOrder, SimEnv, Simulator};
use animus_storage::{MemoryEngine, StorageEngine};
use animus_tablet::{KeyRange, Tablet, TabletId};
use animus_test::corpus::{self, SeedVariant};
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;
type Recon = Reconciler<SimEnv, MemoryEngine>;

const NODES: [u64; 3] = [0, 1, 2];
/// Per-push group count; `A` is a tenth of it (per node every group has a replica).
const GROUPS: usize = 60;
const STREAM0: u64 = 100;
/// First stream id of the harness's ghost clients.
const GHOST0: u64 = 1_000_000;
/// The election base of the LAN profile as the groups run it.
const BASE: Duration = Duration::from_millis(150);
/// `K`, the campaign cap (ADR 0078 default).
const K: usize = 32;
const HOST_H: usize = 100;
/// `--quiesce-after` the cells run with.
const QUIESCE_AFTER: Duration = Duration::from_secs(2);
const STEP: Duration = Duration::from_millis(100);
const RECON_EVERY: Duration = Duration::from_millis(500);
/// Steady events per group per second per node (the ADR 0078 corpus's
/// measured figure; the same rig shape).
const EVENTS_PER_GROUP_PER_S: f64 = 24.6;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    MassWake,
    WriteToDeferred,
    OverloadCapped,
    AbsorbableOverload,
    CapAndLag,
    ElectionsUnderCap,
}

#[derive(Clone)]
struct Cell {
    name: String,
    seed: u64,
    kind: Kind,
    fault: AwakeFault,
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

fn cell(name: &str, kind: Kind, fault: AwakeFault) -> Cell {
    Cell {
        name: name.to_string(),
        seed: corpus::name_seed(name),
        kind,
        fault,
    }
}

fn cells() -> Vec<Cell> {
    vec![
        cell("cell_1_mass_wake", Kind::MassWake, AwakeFault::None),
        cell(
            "cell_2_write_to_deferred_group",
            Kind::WriteToDeferred,
            AwakeFault::None,
        ),
        cell(
            "cell_3_awake_overload_capped",
            Kind::OverloadCapped,
            AwakeFault::None,
        ),
        cell(
            "cell_3b_absorbable_overload_capped",
            Kind::AbsorbableOverload,
            AwakeFault::None,
        ),
        cell("cell_4_cap_and_lag", Kind::CapAndLag, AwakeFault::None),
        cell(
            "cell_5_elections_under_full_queue",
            Kind::ElectionsUnderCap,
            AwakeFault::None,
        ),
    ]
}

fn control_b() -> Vec<Cell> {
    vec![cell(
        "control_b_hard_cap_refuses_the_write",
        Kind::WriteToDeferred,
        AwakeFault::HardCap,
    )]
}

fn control_c() -> Vec<Cell> {
    vec![cell(
        "control_c_no_lag_reduction",
        Kind::CapAndLag,
        AwakeFault::IgnoreLag,
    )]
}

fn control_d() -> Vec<Cell> {
    vec![cell(
        "control_d_no_lost_leader_bypass",
        Kind::MassWake,
        AwakeFault::NoLostLeaderBypass,
    )]
}

fn groups() -> usize {
    std::env::var("ANIMUS_DENSITY_GROUPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|g| *g >= 20)
        .unwrap_or(GROUPS)
}

/// `ANIMUS_DENSITY_CELL` narrows by substring; `ANIMUS_SEED` replays one run.
fn select(cells: Vec<Cell>) -> Vec<Cell> {
    let filter = std::env::var("ANIMUS_DENSITY_CELL").ok();
    let replay = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok());
    corpus::seed_expand(cells, corpus::seeds_from_env("ANIMUS_DENSITY_SEEDS"))
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
// The rig.
// ---------------------------------------------------------------------------

struct Rig {
    sim: Simulator,
    g: usize,
    /// `A` per node (`0` = no awake admission: the uncapped baseline).
    a: usize,
    handles: Vec<MetricsHandle>,
    awake: Vec<Option<AwakeAdmission>>,
    governors: Vec<CampaignGovernor>,
    monitors: Vec<LoadMonitor>,
    engines: Vec<MemoryTabletEngines>,
    recons: Vec<Option<Recon>>,
    view: MetadataView,
    live: [bool; 3],
    cost_ns: u64,
    /// Whether the lag probe is fed from the simulator's CPU queue.
    probing: bool,
    /// When set, the awake-group capacity the CPU model is sized for (instead
    /// of `2A`): cell 3b gives the survivors room for the lost-leader groups.
    capacity_groups: Option<f64>,
    at: Duration,
    since_probe: Duration,
    since_recon: Duration,
    max_lag: Duration,
    /// `(group, term) -> leader node`, for the single-leader oracle.
    leaders: BTreeMap<(usize, u64), usize>,
    split_brain: usize,
    /// The led count at the last sample while [`Rig::track_led`] is on.
    tracked_led: Option<usize>,
    led_regress: usize,
    writes: Vec<(usize, usize, Vec<u8>, u64, u64)>,
    acked: Vec<(usize, Vec<u8>)>,
    next_key: u64,
}

impl Rig {
    fn new(seed: u64, g: usize, a: usize, fault: AwakeFault) -> Rig {
        let sim = Simulator::new(seed);
        let handles: Vec<MetricsHandle> =
            NODES.iter().map(|_| MetricsHandle::recording()).collect();
        let mut awake = Vec::new();
        let mut governors = Vec::new();
        let mut monitors = Vec::new();
        for h in &handles {
            let adm = (a > 0).then(|| AwakeAdmission::with_fault(a, h.clone(), fault));
            let gov = CampaignGovernor::new(K, h.clone());
            let mon = LoadMonitor::detached(4, BASE, h.clone(), Some(gov.clone()));
            if let Some(adm) = &adm {
                mon.attach_awake(adm.clone());
            }
            awake.push(adm);
            governors.push(gov);
            monitors.push(mon);
        }
        let replicas = NODES.iter().map(|&n| nid(n)).collect::<Vec<_>>();
        let tablets = (0..g)
            .map(|gi| {
                let id = Self::tablet(gi);
                (
                    id,
                    Tablet::new_for_table(id, "t", KeyRange::whole(), replicas.clone()),
                )
            })
            .collect();
        let view = MetadataView {
            tablets,
            ..Default::default()
        };
        let engines: Vec<_> = NODES.iter().map(|_| MemoryTabletEngines::new()).collect();
        let mut rig = Rig {
            sim,
            g,
            a,
            handles,
            awake,
            governors,
            monitors,
            engines,
            recons: Vec::new(),
            view,
            live: [true; 3],
            cost_ns: 0,
            probing: false,
            capacity_groups: None,
            at: Duration::ZERO,
            since_probe: Duration::ZERO,
            since_recon: RECON_EVERY,
            max_lag: Duration::ZERO,
            leaders: BTreeMap::new(),
            split_brain: 0,
            tracked_led: None,
            led_regress: 0,
            writes: Vec::new(),
            acked: Vec::new(),
            next_key: 0,
        };
        rig.recons = (0..NODES.len()).map(|i| Some(rig.make_recon(i))).collect();
        rig
    }

    fn tablet(gi: usize) -> TabletId {
        TabletId(STREAM0 + gi as u64)
    }

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
        r.enable_quiescence(QUIESCE_AFTER);
        r.set_election_backpressure(ElectionBackpressure {
            governor: Some(self.governors[i].clone()),
            monitor: Some(self.monitors[i].clone()),
            awake: self.awake[i].clone(),
            budget: None,
            admission: None,
        });
        r.set_host_stagger(HOST_H, DEFAULT_HOST_SETTLE_TIMEOUT);
        r
    }

    fn reconcile(&mut self) {
        let Rig { recons, view, .. } = self;
        for r in recons.iter_mut().flatten() {
            block_on(r.tick(view));
        }
    }

    fn replica(&self, gi: usize, ni: usize) -> Option<&KvNode> {
        self.recons[ni].as_ref()?.hosted_node(Self::tablet(gi))
    }

    fn all_hosted(&self) -> bool {
        self.recons
            .iter()
            .flatten()
            .all(|r| r.local_state().hosted.len() == self.g)
    }

    fn live_leader(&self, gi: usize) -> Option<usize> {
        (0..NODES.len())
            .filter(|&ni| self.live[ni])
            .find(|&ni| self.replica(gi, ni).is_some_and(KvNode::is_leader))
    }

    fn led(&self) -> usize {
        (0..self.g)
            .filter(|&gi| self.live_leader(gi).is_some())
            .count()
    }

    fn all_quiesced(&self) -> bool {
        (0..self.g).all(|gi| {
            (0..NODES.len())
                .filter(|&ni| self.live[ni])
                .all(|ni| self.replica(gi, ni).is_some_and(KvNode::is_quiesced))
        })
    }

    fn kill(&mut self, idx: usize) {
        self.sim.stop(nid(NODES[idx]));
        self.sim.crash(nid(NODES[idx]));
        self.recons[idx] = None;
        self.live[idx] = false;
    }

    fn mark_down(&mut self, idx: usize) {
        self.view.down.insert(nid(NODES[idx]));
    }

    /// Give every node the CPU cost at which `2A` awake groups are exactly its
    /// capacity (`load` x that): `A` is the conservative figure (ADR 0079
    /// takes 1500 against a measured 1900-2000), so a node holding `A` awake
    /// groups runs at half its capacity and one holding `10A` at five times.
    fn set_load(&mut self, load: f64) {
        let capacity_groups = self.capacity_groups.unwrap_or(2.0 * self.a.max(1) as f64);
        for &id in &NODES {
            self.sim.set_cpu_order(nid(id), CpuOrder::Random);
            let cost = if load <= 0.0 {
                Duration::ZERO
            } else {
                Duration::from_secs_f64(load / (EVENTS_PER_GROUP_PER_S * capacity_groups))
            };
            self.sim.set_cpu_cost(nid(id), cost);
            self.cost_ns = u64::try_from(cost.as_nanos()).unwrap_or(u64::MAX);
        }
        self.probing = load > 0.0;
    }

    /// One lag probe from the simulator's CPU queue (the ADR 0078 corpus's
    /// convention: the CPU model serves timers first, so the production
    /// timer-lateness probe reads ~0 here).
    fn probe(&self) {
        for (i, m) in self.monitors.iter().enumerate() {
            let (_, waiting) = self.sim.cpu_backlog(&nid(NODES[i]));
            m.observe((waiting as u64).saturating_mul(self.cost_ns));
        }
    }

    /// Advance `d`: the simulator in `STEP`s, the reconcilers every
    /// `RECON_EVERY`, the lag probe every `PROBE` (when the CPU model is on),
    /// and the oracle's sample after each step.
    fn advance(&mut self, d: Duration) {
        self.advance_by(d, STEP);
    }

    /// [`advance`](Self::advance) with an explicit step (the oracle's time
    /// resolution).
    fn advance_by(&mut self, d: Duration, step: Duration) {
        let end = self.at + d;
        while self.at < end {
            if self.since_recon >= RECON_EVERY {
                self.since_recon = Duration::ZERO;
                self.reconcile();
            }
            if self.probing {
                // Probe on the monitor's cadence inside the step.
                let mut left = step;
                while !left.is_zero() {
                    let chunk = left.min(PROBE - self.since_probe);
                    self.sim.run_for(chunk);
                    left -= chunk;
                    self.since_probe += chunk;
                    if self.since_probe >= PROBE {
                        self.since_probe = Duration::ZERO;
                        self.probe();
                    }
                }
            } else {
                self.sim.run_for(step);
            }
            self.at += step;
            self.since_recon += step;
            self.sample();
            self.settle_writes();
        }
    }

    fn advance_until(&mut self, max: Duration, mut done: impl FnMut(&Rig) -> bool) -> Option<f64> {
        let start = self.at;
        while self.at < start + max {
            if done(self) {
                return Some((self.at - start).as_secs_f64());
            }
            self.advance(Duration::from_millis(500));
        }
        done(self).then(|| (self.at - start).as_secs_f64())
    }

    /// The oracle's per-step sample: single leader per term, the led count's
    /// regressions while tracked, the worst lag.
    fn sample(&mut self) {
        for gi in 0..self.g {
            for ni in 0..NODES.len() {
                if !self.live[ni] {
                    continue;
                }
                let Some(n) = self.replica(gi, ni) else {
                    continue;
                };
                if n.is_leader() {
                    let t = n.term();
                    match self.leaders.get(&(gi, t)) {
                        Some(&prev) if prev != ni => self.split_brain += 1,
                        None => {
                            self.leaders.insert((gi, t), ni);
                        }
                        _ => {}
                    }
                }
            }
        }
        let led = self.led();
        if let Some(prev) = self.tracked_led {
            if led < prev {
                self.led_regress += 1;
            }
            self.tracked_led = Some(led);
        }
        for m in &self.monitors {
            self.max_lag = self.max_lag.max(m.lag());
        }
    }

    /// From now on a fall of the led count is a regression (the oracle's
    /// "monotone led count" once a fault has been absorbed).
    fn track_led(&mut self) {
        self.sample();
        self.tracked_led = Some(self.led());
    }

    /// The groups currently led by node `ni` (the lost-leader set once `ni`
    /// is killed).
    fn led_by(&self, ni: usize) -> Vec<usize> {
        (0..self.g)
            .filter(|&gi| self.live_leader(gi) == Some(ni))
            .collect()
    }

    /// ADR 0079 step 4 follow-up oracle: every group in `lost` (its leader
    /// was on the killed node) gets a live leader with NO client touch inside
    /// `within`. A group with only a Down follower is not in `lost` and may
    /// stay queued.
    fn lost_leaders_recover(&mut self, lost: &[usize], within: Duration) -> bool {
        let lost = lost.to_vec();
        self.advance_until(within, move |rig| {
            lost.iter().all(|&gi| rig.live_leader(gi).is_some())
        })
        .is_some()
    }

    /// Propose one write on group `gi`'s live leader; returns its key when it
    /// was accepted (an unaccepted write is not owed an ack).
    fn put(&mut self, gi: usize) -> Option<Vec<u8>> {
        let ni = self.live_leader(gi)?;
        let key = format!("w{:06}", self.next_key).into_bytes();
        self.next_key += 1;
        let n = self.replica(gi, ni)?;
        if let ProposeResult::Accepted { index, term } = n.put(key.clone(), b"v".to_vec()) {
            self.writes.push((gi, ni, key.clone(), index, term));
            Some(key)
        } else {
            None
        }
    }

    fn is_acked(&self, gi: usize, key: &[u8]) -> bool {
        self.acked.iter().any(|(g, k)| *g == gi && k == key)
    }

    fn settle_writes(&mut self) {
        for (gi, ni, key, index, term) in std::mem::take(&mut self.writes) {
            let Some(n) = self.replica(gi, ni) else {
                continue;
            };
            if n.term() == term && n.engine_applied_index() >= index {
                self.acked.push((gi, key));
            } else if n.term() == term && self.live[ni] {
                self.writes.push((gi, ni, key, index, term));
            }
        }
    }

    /// Acked keys missing from some live replica's engine.
    fn lost(&self) -> usize {
        self.acked
            .iter()
            .filter(|(gi, key)| {
                (0..NODES.len()).filter(|&ni| self.live[ni]).any(|ni| {
                    self.replica(*gi, ni).is_some_and(|n| {
                        let phys = n.physical_key(KIND_BASE, key);
                        block_on(n.storage().get(&phys))
                            .expect("engine read")
                            .is_none()
                    })
                })
            })
            .count()
    }

    fn stats(&self, ni: usize) -> Option<AwakeStats> {
        self.awake[ni].as_ref().map(AwakeAdmission::stats)
    }

    fn reset_hwm(&self) {
        for a in self.awake.iter().flatten() {
            a.reset_hwm();
        }
    }

    /// Host everything, elect, write a little, and let every group quiesce.
    /// Hosting is exempt from the cap, so the first wave is above `A`.
    fn bring_up(&mut self) -> Vec<&'static str> {
        let mut bad = Vec::new();
        if self
            .advance_until(Duration::from_secs(30), Rig::all_hosted)
            .is_none()
        {
            bad.push("harness: the groups were not all hosted");
        }
        if self
            .advance_until(Duration::from_secs(30), |r| r.led() == r.g)
            .is_none()
        {
            bad.push("harness: the groups were not all led");
        }
        for gi in (0..self.g).step_by(5) {
            self.put(gi);
        }
        if self
            .advance_until(Duration::from_secs(90), Rig::all_quiesced)
            .is_none()
        {
            bad.push("harness: the groups did not all quiesce");
        }
        self.reset_hwm();
        bad
    }

    fn finish(self, mut bad: Vec<&'static str>, detail: String) -> Report {
        if self.split_brain > 0 {
            bad.push("safety: two leaders in one (group, term)");
        }
        let lost = self.lost();
        if lost > 0 {
            bad.push("safety: an acked write is missing from a replica");
        }
        let stats = (0..NODES.len()).map(|i| self.stats(i)).collect();
        let report = Report {
            violations: bad,
            led_at_end: self.led(),
            groups: self.g,
            max_lag_ms: self.max_lag.as_millis() as u64,
            max_term: (0..self.g)
                .map(|gi| {
                    (0..NODES.len())
                        .filter_map(|ni| self.replica(gi, ni).map(KvNode::term))
                        .max()
                        .unwrap_or(0)
                })
                .max()
                .unwrap_or(0),
            led_regress: self.led_regress,
            acked: self.acked.len(),
            stats,
            detail,
        };
        drop(self.recons);
        self.sim.shutdown();
        report
    }
}

#[derive(Debug)]
struct Report {
    violations: Vec<&'static str>,
    led_at_end: usize,
    groups: usize,
    max_lag_ms: u64,
    max_term: u64,
    led_regress: usize,
    acked: usize,
    stats: Vec<Option<AwakeStats>>,
    detail: String,
}

// ---------------------------------------------------------------------------
// Cells.
// ---------------------------------------------------------------------------

fn cap_for(g: usize) -> usize {
    (g / 10).max(4)
}

/// What cell 1 measures in one run.
struct MassWake {
    report: Report,
    /// The survivors' admission numbers at the end of the bulk phase.
    bulk: Vec<AwakeStats>,
    /// Groups still without a leader when the bulk phase ended.
    leaderless: usize,
}

/// Cell 1's run: node 0 is killed and marked `Down`, the reconcilers wake
/// every group touching it. `a == 0` is the uncapped baseline.
///
/// Two phases. **Bulk** (40 s): only the reconcilers act, so only the queue
/// admits wakes. A group that was woken is awake until the dead node's replica
/// is replaced (a leader cannot quiesce with a voter that never catches up),
/// so the slots do not turn over and the groups still queued stay quiesced.
/// **Clients**: a request to a group that has no leader makes the edge wake
/// its local replicas (`RaftKvNode::wake`, never deferred: the cap is soft),
/// after which the group elects and takes the write.
fn mass_wake_run(seed: u64, g: usize, a: usize, fault: AwakeFault) -> MassWake {
    let mut r = Rig::new(seed, g, a, fault);
    let mut bad = r.bring_up();
    let lost = r.led_by(0);
    r.kill(0);
    r.mark_down(0);
    r.advance(Duration::from_secs(40));
    let bulk: Vec<AwakeStats> = (1..NODES.len())
        .map(|i| {
            r.stats(i).unwrap_or(AwakeStats {
                awake: r.g,
                awake_hwm: r.g,
                awake_max: 0,
                awake_max_effective: 0,
                queue_depth: 0,
                queue_hwm: 0,
                admitted: 0,
                admit_awake_hwm: 0,
                deferred: 0,
                overcap: 0,
            })
        })
        .collect();
    // No client ever touches a group: every lost-leader group must have
    // been admitted over the cap and elected on its own.
    if !r.lost_leaders_recover(&lost, Duration::from_secs(20)) {
        bad.push("lost-leader: a group whose leader died stayed leaderless without a client");
    }
    let leaderless = r.g - r.led();
    let served = r.advance_until(Duration::from_secs(60), |rig| rig.led() == rig.g);
    if served.is_none() {
        bad.push("served: a group never got a live leader");
    }
    let keys: Vec<(usize, Vec<u8>)> = (0..r.g)
        .filter_map(|gi| r.put(gi).map(|k| (gi, k)))
        .collect();
    r.advance(Duration::from_secs(10));
    if keys.len() < r.g || keys.iter().any(|(gi, k)| !r.is_acked(*gi, k)) {
        bad.push("served: a write to some group was never applied");
    }
    let detail = format!("served_s={served:?} leaderless_after_bulk={leaderless}");
    MassWake {
        report: r.finish(bad, detail),
        bulk,
        leaderless,
    }
}

fn run_mass_wake(c: &Cell, g: usize) -> Report {
    let a = cap_for(g);
    let base = mass_wake_run(c.seed, g, 0, AwakeFault::None);
    let capped = mass_wake_run(c.seed, g, a, c.fault);
    let mut rep = capped.report;
    for (i, s) in capped.bulk.iter().enumerate() {
        // The accounting identity, at the end of the bulk phase: every awake
        // replica past `A` came in over the cap (a peer message) and was
        // counted.
        if s.admit_awake_hwm > s.awake_max_effective {
            rep.violations
                .push("cap: a bulk wake was admitted past A_eff");
        }
        if s.awake_hwm > s.awake_max_effective + s.overcap as usize {
            rep.violations
                .push("cap: awake high-water above A plus counted overcap");
        }
        if s.deferred == 0 {
            rep.violations.push("cap: the wake queue was never used");
        }
        // The lost-leader groups (about a third of them) are admitted over the
        // cap, so the high-water is no longer "half the uncapped run"; it must
        // still sit clearly below it (the Down-follower groups stay queued).
        if s.awake_hwm * 5 > base.bulk[i].awake_hwm * 4 {
            rep.violations
                .push("cap: high-water not below the uncapped run");
        }
    }
    // Groups with only a Down follower stay queued (node 0 never returns in
    // this cell, so no repair frees their slots); the queue is bounded by G.
    if rep.stats.iter().flatten().any(|s| s.queue_depth > g) {
        rep.violations.push("cap: the wake queue grew past G");
    }
    rep.violations.extend(base.report.violations);
    rep.detail = format!(
        "{} a={a} capped_bulk={:?} uncapped_bulk_hwm={:?} capped_leaderless={}",
        rep.detail,
        capped.bulk,
        base.bulk.iter().map(|s| s.awake_hwm).collect::<Vec<_>>(),
        capped.leaderless
    );
    rep
}

/// Fill every slot of every node's cap with ghost groups.
fn fill_caps(r: &Rig) {
    for a in r.awake.iter().flatten() {
        for j in 0..r.a {
            a.register(GHOST0 + j as u64);
        }
    }
}

/// Cell 2 and control (b).
fn run_write_to_deferred(c: &Cell, g: usize) -> Report {
    let mut r = Rig::new(c.seed, g, cap_for(g), c.fault);
    let mut bad = r.bring_up();
    fill_caps(&r);
    let gi = 0;
    let leader = r.live_leader(gi).expect("group 0 is led");
    let followers: Vec<usize> = (0..NODES.len()).filter(|&n| n != leader).collect();
    // The bulk wake of the group's followers: every slot is taken, so both
    // are deferred and stay quiesced.
    for &f in &followers {
        r.replica(gi, f).expect("hosted").wake_bulk();
    }
    r.advance(Duration::from_secs(1));
    for &f in &followers {
        let s = r.stats(f).expect("stats");
        if s.queue_depth == 0 || s.deferred == 0 {
            bad.push("setup: the follower was not deferred");
        }
        if !r.replica(gi, f).expect("hosted").is_quiesced() {
            bad.push("setup: a deferred follower woke");
        }
    }
    let overcap_before: u64 = (0..NODES.len())
        .map(|i| r.stats(i).expect("stats").overcap)
        .sum();
    // The client write to the deferred group.
    let key = r.put(gi);
    if key.is_none() {
        bad.push("write: the leader refused the write");
    }
    let key = key.unwrap_or_default();
    let applied = r.advance_until(Duration::from_secs(10), |rig| rig.is_acked(gi, &key));
    if applied.is_none() {
        bad.push("write: the write to a deferred group was not served");
    }
    let overcap_after: u64 = (0..NODES.len())
        .map(|i| r.stats(i).expect("stats").overcap)
        .sum();
    if overcap_after <= overcap_before {
        bad.push("soft cap: the over-cap wake was not counted");
    }
    for &f in &followers {
        if r.stats(f).expect("stats").queue_depth != 0
            && r.replica(gi, f).is_some_and(|n| !n.is_quiesced())
        {
            bad.push("soft cap: a woken follower stayed queued");
        }
    }
    // A client (edge) wake of another quiesced follower is admitted over the
    // cap too.
    let gj = 1;
    let lj = r.live_leader(gj).expect("group 1 is led");
    let fj = (0..NODES.len()).find(|&n| n != lj).expect("a follower");
    if let Some(n) = r.replica(gj, fj)
        && n.is_quiesced()
    {
        n.wake();
        r.advance(Duration::from_secs(1));
        if r.replica(gj, fj).is_some_and(KvNode::is_quiesced) {
            bad.push("soft cap: a client wake was refused");
        }
    }
    let detail = format!(
        "applied_s={applied:?} overcap={overcap_before}->{overcap_after} stats={:?}",
        (0..NODES.len()).map(|i| r.stats(i)).collect::<Vec<_>>()
    );
    r.finish(bad, detail)
}

/// Cell 3: the same node loss and mass wake with the CPU model on, cap vs no
/// cap. `a == 0` is the uncapped baseline (the CPU model is sized from the
/// cap in both arms, so both see the same node capacity).
fn overload_run(seed: u64, g: usize, a: usize) -> (Report, usize, usize) {
    overload_run_sized(seed, g, a, false)
}

/// [`overload_run`] with the CPU model sized, when `absorbable`, for the
/// survivors to absorb the lost-leader groups plus `A` (cell 3b).
fn overload_run_sized(seed: u64, g: usize, a: usize, absorbable: bool) -> (Report, usize, usize) {
    let mut r = Rig::new(seed, g, a, AwakeFault::None);
    let mut bad = r.bring_up();
    let lost = r.led_by(0);
    r.a = cap_for(g);
    if absorbable {
        r.capacity_groups = Some(1.5 * (g / NODES.len() + cap_for(g)) as f64);
    }
    r.set_load(1.0);
    r.kill(0);
    r.mark_down(0);
    r.track_led();
    r.advance(Duration::from_secs(60));
    let led_under_load = r.led();
    let backlog = r.sim.cpu_backlog(&nid(1)).1;
    r.set_load(0.0);
    // No client touch: lost-leader groups elect on their own.
    if !r.lost_leaders_recover(&lost, Duration::from_secs(30)) {
        bad.push("lost-leader: a group whose leader died stayed leaderless without a client");
    }
    let served = r.advance_until(Duration::from_secs(60), |rig| rig.led() == rig.g);
    if served.is_none() {
        bad.push("served: a group never got a live leader");
    }
    let detail = format!(
        "served_s={served:?} hwm={:?}",
        r.stats(1).map(|s| s.awake_hwm)
    );
    (r.finish(bad, detail), led_under_load, backlog)
}

fn run_overload(c: &Cell, g: usize) -> Report {
    let a = cap_for(g);
    let (base, base_led, base_backlog) = overload_run(c.seed, g, 0);
    let (mut rep, led, backlog) = overload_run(c.seed, g, a);
    if rep.led_regress > 0 {
        rep.violations
            .push("monotone: the led count fell after the fault");
    }
    // ADR 0079 step 4 follow-up. The bypass admits the groups whose leader
    // died over the cap, so the lag comparison step 4 asserted ("several times
    // calmer than uncapped") is replaced; it was root-caused (measured on seed
    // 0x1c45947310301007: node 1's message backlog grew ~150/s capped against
    // ~22/s uncapped) to the capped run actually electing the lost groups
    // (53 of 60 led after 60 s against 41 uncapped, whose lost groups starve
    // and never elect): arrivals per led group are the same (~8/s) in both, so
    // the extra lag is the heartbeat and election traffic of 12 more live
    // leaders, not admission churn. What the cap must still deliver under
    // overload: no fewer groups led than uncapped. Lag and backlog are
    // reported, not asserted.
    if led < base_led {
        rep.violations
            .push("overload: the capped run led fewer groups than the uncapped one");
    }
    rep.violations
        .extend(base.violations.iter().filter(|v| v.starts_with("safety")));
    rep.detail = format!(
        "{} led_under_load={led}/{base_led} backlog={backlog}/{base_backlog} capped_lag_ms={} uncapped_lag_ms={} capped_term={} uncapped_term={} uncapped_regress={} uncapped_led={}/{}",
        rep.detail,
        rep.max_lag_ms,
        base.max_lag_ms,
        rep.max_term,
        base.max_term,
        base.led_regress,
        base.led_at_end,
        base.groups,
    );
    rep
}

/// Cell 3b (ADR 0079 step 7). Cell 3 sizes the CPU model at `2A` awake groups,
/// which the lost-leader bypass (about `G/N` groups over the cap) already
/// exceeds, so its capped run is saturated by construction and can only be
/// held to "no fewer groups led". Here each survivor has capacity for
/// `1.5 x (G/N + A)` awake groups: the capped population (`A_eff` plus the
/// bypassed lost-leader elections) fits, the uncapped one (every group awake)
/// does not. The capped run must then be no worse than the uncapped one on
/// both axes: worst scheduling lag, and groups led under load. The run's
/// counters (awake high-water, over-cap wakes, queue use, campaign admits, lag
/// and backlog) are in the detail line so a failure can be root-caused.
fn run_absorbable_overload(c: &Cell, g: usize) -> Report {
    let a = cap_for(g);
    let (base, base_led, base_backlog) = overload_run_sized(c.seed, g, 0, true);
    let (mut rep, led, backlog) = overload_run_sized(c.seed, g, a, true);
    if rep.led_regress > 0 {
        rep.violations
            .push("monotone: the led count fell after the fault");
    }
    if led < base_led {
        rep.violations
            .push("overload: the capped run led fewer groups than the uncapped one");
    }
    if rep.max_lag_ms > base.max_lag_ms {
        rep.violations
            .push("overload: the capped run's worst scheduling lag exceeds the uncapped one's");
    }
    rep.violations
        .extend(base.violations.iter().filter(|v| v.starts_with("safety")));
    rep.detail = format!(
        "{} led_under_load={led}/{base_led} backlog={backlog}/{base_backlog} capped_lag_ms={} uncapped_lag_ms={} capped_term={} uncapped_term={}",
        rep.detail, rep.max_lag_ms, base.max_lag_ms, rep.max_term, base.max_term,
    );
    rep
}

/// Cell 4 and control (c). Node 2 is marked `Down` but stays alive, so the
/// woken groups quiesce again and the slots turn over: the reconcilers keep
/// waking, the admission keeps pacing. Node 1 has injected lag.
fn run_cap_and_lag(c: &Cell, g: usize) -> Report {
    let a = cap_for(g);
    let mut r = Rig::new(c.seed, g, a, c.fault);
    let mut bad = r.bring_up();
    let lag = 120_000_000;
    r.monitors[1].observe(lag);
    let a_eff = a / 4;
    let s1 = r.stats(1).expect("stats");
    if c.fault == AwakeFault::None && s1.awake_max_effective != a_eff {
        bad.push("lag: A_eff did not fall to A/4");
    }
    if c.fault == AwakeFault::None && r.handles[1].get(Metric::CpAwakeMaxEffective) != a_eff as u64
    {
        bad.push("lag: CpAwakeMaxEffective did not follow");
    }
    r.mark_down(2);
    // Hold the lag while the bulk wakes arrive.
    for _ in 0..20 {
        r.advance(Duration::from_secs(1));
        r.monitors[1].observe(lag);
    }
    let s1 = r.stats(1).expect("stats");
    let s0 = r.stats(0).expect("stats");
    // Bulk wakes stop at the reduced bound: no admission ever leaves the node
    // above `A_eff`.
    if s1.admit_awake_hwm > a_eff {
        bad.push("lag: bulk wakes continued past A_eff under lag");
    }
    if s0.admit_awake_hwm <= a_eff + 1 {
        bad.push("lag: the unloaded node should admit well past A/4");
    }
    if s0.admitted <= s1.admitted {
        bad.push("lag: the lagged node admitted as many wakes as the healthy one");
    }
    // Clear the lag: the monitor decays, A_eff recovers, admissions resume.
    let mut feeds = 0;
    while feeds < 400 && r.monitors[1].lag() > Duration::ZERO {
        r.monitors[1].observe(0);
        feeds += 1;
    }
    r.advance(Duration::from_secs(1));
    if r.stats(1).expect("stats").awake_max_effective != a {
        bad.push("lag: A_eff did not recover when the lag cleared");
    }
    let before = r.stats(1).expect("stats");
    r.advance(Duration::from_secs(20));
    let after = r.stats(1).expect("stats");
    if after.admitted - before.admitted <= s1.admitted {
        bad.push("lag: admissions did not resume at the full rate once the lag cleared");
    }
    if after.admit_awake_hwm <= a_eff {
        bad.push("lag: no wake was admitted past A/4 after the lag cleared");
    }
    if r.led() != r.g {
        bad.push("served: a group lost its leader");
    }
    let detail = format!(
        "a={a} a_eff={a_eff} during: node1={s1:?} node0={s0:?}; after: node1={after:?} feeds={feeds}"
    );
    r.finish(bad, detail)
}

/// Cell 5: ADR 0078 cell 10's lost-leader bound with the governor saturated by
/// ghosts and the wake queue full of real deferred groups.
fn run_elections(c: &Cell, g: usize) -> Report {
    let a = cap_for(g);
    let mut r = Rig::new(c.seed, g, a, c.fault);
    let mut bad = r.bring_up();
    // Group 0 stays awake: a write every 500 ms keeps it (and its followers)
    // out of quiescence.
    // Saturate the governors with ghosts that cannot win.
    for (i, &id) in NODES.iter().enumerate() {
        let env = r.sim.env(nid(id));
        for j in 0..3 * K {
            let (env2, gov) = (env.clone(), r.governors[i].clone());
            env.spawn_task(async move {
                let wake: CampaignWake = Arc::new(|| {});
                let stream = GHOST0 + 500 + j as u64;
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
    // Fill the awake caps and bulk-wake every quiesced group so the wake
    // queues hold real deferred groups.
    fill_caps(&r);
    for gi in 1..r.g {
        for ni in 0..NODES.len() {
            if let Some(n) = r.replica(gi, ni) {
                n.wake_bulk();
            }
        }
    }
    r.put(0);
    r.advance(Duration::from_secs(1));
    for _ in 0..4 {
        r.put(0);
        r.advance(Duration::from_millis(500));
    }
    let queued: Vec<usize> = (0..NODES.len())
        .map(|i| r.stats(i).expect("stats").queue_depth)
        .collect();
    if queued.iter().all(|q| *q == 0) {
        bad.push("setup: the wake queues were empty");
    }
    let q = r
        .governors
        .iter()
        .map(|gv| gv.stats().queue_depth)
        .max()
        .unwrap_or(0);
    let gi = 0;
    let ni = r.live_leader(gi).expect("group 0 is led");
    // Per-group attempt counter: pre-vote rounds each node's core has started
    // for group `gi`. (The node-wide `CpCampaignAdmitted` counts every hosted
    // group and the ghosts, so using it made the bound below vacuous.)
    let rounds_of = |r: &Rig, n: usize| {
        r.replica(gi, n)
            .map_or(0, |k| k.campaign_stats().rounds_started)
    };
    let rounds_before: Vec<u64> = (0..NODES.len()).map(|n| rounds_of(&r, n)).collect();
    r.replica(gi, ni).expect("hosted").shutdown();
    let start = r.at;
    let others: Vec<usize> = (0..NODES.len()).filter(|&n| n != ni).collect();
    let mut delay = None;
    while r.at < start + Duration::from_secs(8) {
        r.advance_by(Duration::from_millis(10), Duration::from_millis(10));
        if others
            .iter()
            .any(|&o| r.replica(gi, o).is_some_and(KvNode::is_leader))
        {
            delay = Some((r.at - start).as_secs_f64());
            break;
        }
    }
    const OWN_ELECTION_SLACK: f64 = 0.05;
    // The queue ahead is the ghosts above: not woken by a grant, they re-ask on
    // a `BASE` sleep and the governor protects the queue head, so each of the
    // `ceil(Q/K)` passes costs `2*BASE + BASE` (issue #1306; the same term as
    // `election_backpressure_corpus` cell 10).
    const GHOST_REASK: f64 = BASE.as_secs_f64();
    let round = 2.0 * BASE.as_secs_f64()
        + (q.div_ceil(K) as f64) * (2.0 * BASE.as_secs_f64() + GHOST_REASK);
    // ADR 0078's literal bound holds per campaign attempt: a split vote or a
    // pre-vote round that fails (no governor bound excludes either; they are
    // not specific to the awake cap, the cap-less rig misses the literal
    // bound on some seeds too) costs a further attempt. `rounds` counts the
    // pre-vote rounds the survivors started for this group while it had no
    // leader (floor 1; a deferred campaign starts none).
    let rounds = others
        .iter()
        .map(|&o| rounds_of(&r, o) - rounds_before[o])
        .max()
        .unwrap_or(1)
        .max(1);
    let bound = round * rounds as f64 + OWN_ELECTION_SLACK;
    // Livelock guard, independent of the `rounds`-scaled bound (which a
    // livelock would only widen): same cap and derivation as
    // `election_backpressure_corpus::attempt_cap` (28 = ceil(ln 1e-6 / ln 0.6));
    // this rig measured 1..6 rounds over 16 seeds.
    if rounds > 28 {
        bad.push("livelock: more lost campaign attempts than the geometry allows");
    }
    match delay {
        None => bad.push("bound: the lost group never got a leader (starved)"),
        Some(d) if d > bound => {
            bad.push("bound: lost-leader delay above attempts * (2*base + ceil(Q/K)*T_hold_max)");
        }
        Some(_) => {}
    }
    let detail =
        format!("queued={queued:?} q={q} rounds={rounds} delay_s={delay:?} bound_s={bound:.3}");
    r.finish(bad, detail)
}

fn run_cell(c: &Cell, g: usize) -> Report {
    match c.kind {
        Kind::MassWake => run_mass_wake(c, g),
        Kind::WriteToDeferred => run_write_to_deferred(c, g),
        Kind::OverloadCapped => run_overload(c, g),
        Kind::AbsorbableOverload => run_absorbable_overload(c, g),
        Kind::CapAndLag => run_cap_and_lag(c, g),
        Kind::ElectionsUnderCap => run_elections(c, g),
    }
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

#[test]
fn density_admission_cells_pass_the_oracle() {
    for c in select(cells()) {
        let r = run_cell(&c, groups());
        eprintln!("DENSITY-CELL {} seed={:#x} {r:?}", c.name, c.seed);
        assert!(
            r.violations.is_empty(),
            "{} seed={:#x}: {:?} {r:?}",
            c.name,
            c.seed,
            r.violations
        );
    }
}

/// NEGATIVE CONTROL (b): a hard cap that refuses wakes. Cell 2's write to a
/// deferred group must not be served, and the oracle must say so.
#[test]
fn control_b_a_hard_cap_refuses_the_write() {
    for c in select(control_b()) {
        let r = run_cell(&c, groups());
        eprintln!("DENSITY-CONTROL {} seed={:#x} {r:?}", c.name, c.seed);
        assert!(
            r.violations.iter().all(|v| !v.starts_with("safety")),
            "{}: safety must hold even for a refused write: {:?}",
            c.name,
            r.violations
        );
        assert!(
            r.violations
                .contains(&"write: the write to a deferred group was not served"),
            "{} seed={:#x}: the hard cap must stall the write: {:?} {r:?}",
            c.name,
            c.seed,
            r.violations
        );
    }
}

/// NEGATIVE CONTROL (c): `A_eff` ignores the lag. Cell 4's bulk wakes must
/// continue past `A/4` under lag, and the oracle must say so.
#[test]
fn control_c_no_lag_reduction_keeps_waking() {
    for c in select(control_c()) {
        let r = run_cell(&c, groups());
        eprintln!("DENSITY-CONTROL {} seed={:#x} {r:?}", c.name, c.seed);
        assert!(
            r.violations
                .contains(&"lag: bulk wakes continued past A_eff under lag"),
            "{} seed={:#x}: without the lag reduction bulk wakes must continue: {:?} {r:?}",
            c.name,
            c.seed,
            r.violations
        );
    }
}

/// NEGATIVE CONTROL (d): the lost-leader bypass is off
/// ([`AwakeFault::NoLostLeaderBypass`]). Cell 1's groups whose leader died are
/// deferred like any bulk wake and stay leaderless without a client, and the
/// oracle must say so.
#[test]
fn control_d_no_lost_leader_bypass_strands_groups() {
    for c in select(control_d()) {
        let r = run_cell(&c, groups());
        eprintln!("DENSITY-CONTROL {} seed={:#x} {r:?}", c.name, c.seed);
        assert!(
            r.violations.iter().all(|v| !v.starts_with("safety")),
            "{}: safety must hold: {:?}",
            c.name,
            r.violations
        );
        assert!(
            r.violations.iter().any(|v| v.starts_with("lost-leader:")),
            "{} seed={:#x}: without the bypass a lost-leader group must stay leaderless: {:?} {r:?}",
            c.name,
            c.seed,
            r.violations
        );
    }
}

#[test]
fn names_and_seeds_are_unique_and_frozen() {
    let all: Vec<Cell> = cells()
        .into_iter()
        .chain(control_b())
        .chain(control_c())
        .chain(control_d())
        .collect();
    let names: BTreeSet<&str> = all.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names.len(), all.len());
    for c in &all {
        assert_eq!(c.seed, corpus::name_seed(&c.name));
    }
}

/// The same seed replays to the same numbers (no new nondeterminism).
#[test]
fn a_run_is_a_pure_function_of_its_seed() {
    let c = cell("determinism", Kind::CapAndLag, AwakeFault::None);
    let run = |c: &Cell| {
        let r = run_cap_and_lag(c, 40);
        (r.detail, r.max_term, r.led_at_end, r.acked)
    };
    assert_eq!(run(&c), run(&c));
}

/// A cap that can never bind changes nothing observable: the same seed gives
/// the same leaders and terms with and without the admission installed.
#[test]
fn a_cap_that_never_binds_is_a_no_op() {
    fn run(a: usize) -> (usize, u64, usize) {
        let mut r = Rig::new(0xD15C0, 30, a, AwakeFault::None);
        let bad = r.bring_up();
        assert!(bad.is_empty(), "{bad:?}");
        r.kill(0);
        r.mark_down(0);
        r.advance(Duration::from_secs(30));
        let rep = r.finish(Vec::new(), String::new());
        (rep.led_at_end, rep.max_term, rep.acked)
    }
    assert_eq!(run(0), run(1_000_000));
}

// ===========================================================================
// ADR 0079 step 5 (layer 3: the background budget): cells 6-8 and controls.
//
// 6.  **snapshot storm**: `G` learners, one per group, on one node all need a
//     real `InstallSnapshot` at once. Per node the in-flight sends, receives
//     and catch-up learners never pass their pool (2 / 2 / 4); the chunks the
//     pools refuse are dropped unacknowledged and resent by the sender's own
//     backoff; every learner still converges and is promoted; and every
//     permit is back at the end.
// 6b. **unacknowledged transfers release their send permit**: a leader's only
//     send slot goes to a transfer whose receiver never answers; the slot is
//     released by the unacknowledged bound (`snapshot_unacked_bound`), so the
//     next group's learner gets it and converges instead of starving.
// 7.  **split storm**: more eligible tablets than the split pool; driven
//     through the real `SplitPermits` against a real `Metadata` (BeginSplit,
//     CutoverSplit, MarkSplitPlacingDone applied by the pure state machine):
//     in-progress splits never pass the pool and every tablet splits.
// 8.  **compaction under write load** (`LsmEngine<SimEnv>` with a sync delay,
//     so runs interleave): admitted compactions never pass the pool, the L0
//     safety valve is the only way past it and is counted, no acked write is
//     lost, and after the load every engine's L0 settles under the trigger
//     while the node's lag keeps the pool at its minimum.
//
// Negative controls (each asserts the oracle FAILS). The ADR names them (d)
// and (e); this file's pre-existing awake-cap control (d) (the lost-leader
// bypass) already owns that name, so these are `bg_d`, `bg_d2` and `bg_e`.
//
// - **bg_d** pool permit leak (`BudgetFault::LeakPermits`) on cell 6: the
//   learner node's pools fill with permits nobody returns, later learners never
//   catch up.
// - **bg_d2** no unacknowledged-transfer bound (`BudgetFault::NoIdleBound`) on
//   cell 6b: the dead receiver holds the send slot forever, the next learner
//   starves.
// - **bg_e** pool minimum 0 under lag (`BudgetFault::PoolMinZero`) on cell 8:
//   nothing is admitted, L0 only ever drains through the safety valve and the
//   engines are left over the trigger.
// ===========================================================================

use animus_control::{ApplyOutcome, MetaCommand, Metadata, NodeStatus, PlacementPolicy};
use animus_cp_data::background_budget::{
    BackgroundBudget, BgPool, BudgetFault, BudgetSizes, SplitAdmission, SplitPermits,
    snapshot_unacked_bound,
};
use animus_cp_data::{HostedOptions, StorageScope};
use animus_storage::{LsmEngine, LsmOptions};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BgKind {
    SnapshotStorm,
    UnackedBound,
    SplitStorm,
    CompactionLoad,
}

#[derive(Clone)]
struct BgCell {
    name: String,
    seed: u64,
    kind: BgKind,
    fault: BudgetFault,
}

impl SeedVariant for BgCell {
    fn scenario_name(&self) -> &str {
        &self.name
    }
    fn reseeded(&self, name: String, seed: u64) -> Self {
        BgCell {
            name,
            seed,
            ..self.clone()
        }
    }
}

fn bg_cell(name: &str, kind: BgKind, fault: BudgetFault) -> BgCell {
    BgCell {
        name: name.to_string(),
        seed: corpus::name_seed(name),
        kind,
        fault,
    }
}

fn bg_cells() -> Vec<BgCell> {
    vec![
        bg_cell(
            "cell_6_snapshot_storm",
            BgKind::SnapshotStorm,
            BudgetFault::None,
        ),
        bg_cell(
            "cell_6b_unacked_transfer_releases_its_permit",
            BgKind::UnackedBound,
            BudgetFault::None,
        ),
        bg_cell("cell_7_split_storm", BgKind::SplitStorm, BudgetFault::None),
        bg_cell(
            "cell_8_compaction_under_write_load",
            BgKind::CompactionLoad,
            BudgetFault::None,
        ),
    ]
}

fn control_bg_d() -> Vec<BgCell> {
    vec![bg_cell(
        "control_bg_d_permit_leak",
        BgKind::SnapshotStorm,
        BudgetFault::LeakPermits,
    )]
}

fn control_bg_d2() -> Vec<BgCell> {
    vec![bg_cell(
        "control_bg_d2_no_unacked_bound",
        BgKind::UnackedBound,
        BudgetFault::NoIdleBound,
    )]
}

fn control_bg_e() -> Vec<BgCell> {
    vec![bg_cell(
        "control_bg_e_pool_minimum_zero",
        BgKind::CompactionLoad,
        BudgetFault::PoolMinZero,
    )]
}

fn bg_select(cells: Vec<BgCell>) -> Vec<BgCell> {
    let filter = std::env::var("ANIMUS_DENSITY_CELL").ok();
    let replay = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok());
    corpus::seed_expand(cells, corpus::seeds_from_env("ANIMUS_DENSITY_SEEDS"))
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

/// Learners (= groups) in the snapshot storm; `ANIMUS_DENSITY_BG_GROUPS`
/// resizes (must exceed the catch-up pool of 4 to press on it).
fn bg_groups() -> usize {
    std::env::var("ANIMUS_DENSITY_BG_GROUPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|g| *g > 4)
        .unwrap_or(16)
}

#[derive(Debug, Default)]
struct BgReport {
    violations: Vec<String>,
    /// Deterministic numbers (used for the replay check and the eprintln).
    detail: String,
}

impl BgReport {
    fn bad(&mut self, v: String) {
        self.violations.push(v);
    }
}

const BG_STREAM0: u64 = 500;
const WARM_WRITES: u64 = 150;

fn voters3() -> Vec<animus_env::NodeId> {
    NODES.iter().copied().map(nid).collect()
}

fn bg_start(
    sim: &Simulator,
    node: u64,
    stream: u64,
    budget: &BackgroundBudget,
) -> (KvNode, MemoryEngine) {
    let engine = MemoryEngine::new();
    let n = RaftKvNode::start_hosted_with_options(
        sim.env(nid(node)),
        voters3(),
        engine.clone(),
        StorageScope::whole(),
        stream,
        HostedOptions {
            backpressure: ElectionBackpressure {
                budget: Some(budget.clone()),
                ..ElectionBackpressure::default()
            },
            ..HostedOptions::default()
        },
    );
    (n, engine)
}

fn bg_leader(group: &[KvNode]) -> Option<usize> {
    let ls: Vec<usize> = (0..group.len()).filter(|&i| group[i].is_leader()).collect();
    (ls.len() == 1).then(|| ls[0])
}

fn bg_warm(group: &[KvNode], l: usize, tag: &str, r: &mut BgReport) {
    for i in 0..WARM_WRITES {
        let res = group[l].put(
            format!("{tag}-{i:03}").into_bytes(),
            format!("v{tag}-{i}").into_bytes(),
        );
        if !matches!(res, ProposeResult::Accepted { .. }) {
            r.bad(format!("warm: {tag} write {i} refused by its leader"));
            return;
        }
    }
}

/// Whether `node`'s local engine holds every warm key of `tag`.
fn bg_has_all(node: &KvNode, tag: &str) -> bool {
    [0, 1, 64, 100, WARM_WRITES - 1].iter().all(|&i| {
        block_on(node.local_get(format!("{tag}-{i:03}").as_bytes()))
            == Some(format!("v{tag}-{i}").into_bytes())
    })
}

// --- cell 6 -----------------------------------------------------------------

fn run_snapshot_storm(c: &BgCell, g: usize) -> BgReport {
    let mut r = BgReport::default();
    let mut sim = Simulator::new(c.seed);
    let sizes = BudgetSizes::resolve(None, None, None, None, None);
    // Nodes 0-2 vote; node 3 hosts every group's learner (and so every pool the
    // storm presses on). The fault, when any, is the learner node's.
    let budgets: Vec<BackgroundBudget> = (0..4)
        .map(|n| {
            let fault = if n == 3 { c.fault } else { BudgetFault::None };
            BackgroundBudget::with_fault(sizes, MetricsHandle::recording(), fault)
        })
        .collect();
    let groups: Vec<Vec<KvNode>> = (0..g)
        .map(|gi| {
            (0..3)
                .map(|n| bg_start(&sim, n as u64, BG_STREAM0 + gi as u64, &budgets[n]).0)
                .collect()
        })
        .collect();
    sim.run_for(Duration::from_secs(3));
    let mut leaders = Vec::new();
    for (gi, grp) in groups.iter().enumerate() {
        match bg_leader(grp) {
            Some(l) => leaders.push(l),
            None => {
                r.bad(format!("bring-up: group {gi} has no single leader"));
                return r;
            }
        }
    }
    for (gi, grp) in groups.iter().enumerate() {
        bg_warm(grp, leaders[gi], &format!("g{gi}"), &mut r);
    }
    sim.run_for(Duration::from_secs(4));

    // The storm: every learner joins at the same instant.
    let learners: Vec<KvNode> = (0..g)
        .map(|gi| bg_start(&sim, 3, BG_STREAM0 + gi as u64, &budgets[3]).0)
        .collect();
    for (gi, grp) in groups.iter().enumerate() {
        if !matches!(
            grp[leaders[gi]].add_learner(nid(3)),
            ProposeResult::Accepted { .. }
        ) {
            r.bad(format!("storm: add_learner refused for group {gi}"));
        }
    }
    let mut promoted = vec![false; g];
    let mut seen: BTreeMap<(usize, u64), usize> = BTreeMap::new();
    let mut done_at = None;
    let start = Duration::ZERO;
    let mut t = start;
    while t < Duration::from_secs(150) {
        sim.run_for(Duration::from_millis(250));
        t += Duration::from_millis(250);
        for (gi, grp) in groups.iter().enumerate() {
            for (ni, n) in grp.iter().enumerate() {
                if n.is_leader() {
                    match seen.insert((gi, n.term()), ni) {
                        Some(prev) if prev != ni => r.bad(format!(
                            "safety: two leaders in group {gi} term {}",
                            n.term()
                        )),
                        _ => {}
                    }
                }
            }
            if !promoted[gi]
                && let Some(l) = bg_leader(grp)
                && grp[l].learner_caught_up(&nid(3), 4)
                && matches!(
                    grp[l].promote_learner(nid(3)),
                    ProposeResult::Accepted { .. }
                )
            {
                promoted[gi] = true;
            }
        }
        if promoted.iter().all(|p| *p)
            && learners
                .iter()
                .enumerate()
                .all(|(gi, n)| bg_has_all(n, &format!("g{gi}")))
        {
            done_at = Some(t);
            break;
        }
    }
    sim.run_for(Duration::from_secs(10));

    let converged = (0..g)
        .filter(|&gi| promoted[gi] && bg_has_all(&learners[gi], &format!("g{gi}")))
        .count();
    if converged < g {
        r.bad(format!("converge: {converged} of {g} learners caught up"));
    }
    // Pools: never past their size.
    let caps = [
        (BgPool::SnapshotSend, 2usize),
        (BgPool::SnapshotRecv, 2),
        (BgPool::Catchup, 4),
    ];
    let mut detail = format!("done_at={done_at:?} converged={converged}/{g}");
    for (n, b) in budgets.iter().enumerate() {
        for (pool, cap) in caps {
            let s = b.stats(pool);
            if s.hwm > cap {
                r.bad(format!("pool: node {n} {pool:?} hwm {} > {cap}", s.hwm));
            }
            if s.size_eff.is_some_and(|e| e == 0) {
                r.bad(format!("pool: node {n} {pool:?} stuck at size 0"));
            }
            if s.inflight > 0 {
                r.bad(format!(
                    "leak: node {n} {pool:?} still holds {} permits after the storm",
                    s.inflight
                ));
            }
            detail += &format!(
                " n{n}.{pool:?}(hwm={} adm={} def={} exp={})",
                s.hwm, s.admitted, s.deferred, s.expired
            );
        }
    }
    // Pressure on any pool of any node: which pool fills first depends on the
    // seed (the senders' pools or the learner node's), the mechanism engaging
    // somewhere is the coverage claim.
    let pressed: u64 = budgets
        .iter()
        .flat_map(|b| {
            [BgPool::SnapshotSend, BgPool::SnapshotRecv, BgPool::Catchup]
                .map(|p| b.stats(p).deferred)
        })
        .sum();
    if pressed == 0 {
        r.bad("pressure: the storm never found a full pool".to_string());
    }
    r.detail = detail;
    r
}

// --- cell 6b ----------------------------------------------------------------

fn run_unacked_bound(c: &BgCell) -> BgReport {
    let mut r = BgReport::default();
    let mut sim = Simulator::new(c.seed);
    // One send slot, shared by both groups' leaders: the contention is the
    // point, whichever voters lead.
    let mut sizes = BudgetSizes::off();
    sizes.snapshot_send = Some(1);
    let budget = BackgroundBudget::with_fault(sizes, MetricsHandle::recording(), c.fault);
    // Node 3 hosts group A's learner and has NO receive room: its pool (size 1)
    // is held by a transfer that never finishes, so it refuses A's first chunk
    // by not acknowledging it. Node 4 (group B's learner) has room.
    let mut room = BudgetSizes::off();
    room.snapshot_recv = Some(1);
    let full = BackgroundBudget::new(room, MetricsHandle::recording());
    let _hog = full
        .try_acquire(BgPool::SnapshotRecv, Nanos(0), None)
        .expect("the hog takes the only receive slot");
    let mk = |stream: u64| -> Vec<KvNode> {
        (0..3)
            .map(|n| bg_start(&sim, n as u64, stream, &budget).0)
            .collect()
    };
    let (a, b) = (mk(BG_STREAM0), mk(BG_STREAM0 + 1));
    sim.run_for(Duration::from_secs(3));
    let (Some(la), Some(lb)) = (bg_leader(&a), bg_leader(&b)) else {
        r.bad("bring-up: no single leader".into());
        return r;
    };
    bg_warm(&a, la, "ga", &mut r);
    bg_warm(&b, lb, "gb", &mut r);
    sim.run_for(Duration::from_secs(4));

    // Group A's learner takes the only send slot first and is never answered.
    let (_learner_a, _) = bg_start(&sim, 3, BG_STREAM0, &full);
    let _ = a[la].add_learner(nid(3));
    sim.run_for(Duration::from_secs(1));
    let sent_to_a = budget.stats(BgPool::SnapshotSend);
    if sent_to_a.inflight != 1 {
        r.bad(format!(
            "setup: group A's transfer should hold the send slot ({sent_to_a:?})"
        ));
    }
    let (learner_b, _) = bg_start(&sim, 4, BG_STREAM0 + 1, &budget);
    let _ = b[lb].add_learner(nid(4));
    let hb = Duration::from_millis(50);
    let bound = snapshot_unacked_bound(hb);
    let limit = bound * 2 + Duration::from_secs(15);
    let mut waited = Duration::ZERO;
    let mut done = None;
    while waited < limit {
        sim.run_for(Duration::from_millis(250));
        waited += Duration::from_millis(250);
        if bg_has_all(&learner_b, "gb") {
            done = Some(waited);
            break;
        }
    }
    let s = budget.stats(BgPool::SnapshotSend);
    if done.is_none() {
        r.bad(format!(
            "starved: group B's learner never got the send slot held by a transfer \
             nobody acknowledges (waited {waited:?}, bound {bound:?})"
        ));
    }
    if c.fault == BudgetFault::None && s.expired == 0 {
        r.bad("bound: the unacknowledged transfer's permit was never released".into());
    }
    if s.hwm > 1 {
        r.bad(format!("pool: send hwm {} > 1", s.hwm));
    }
    r.detail = format!(
        "done={done:?} bound={bound:?} send(hwm={} adm={} def={} exp={})",
        s.hwm, s.admitted, s.deferred, s.expired
    );
    r
}

// --- cell 7 -----------------------------------------------------------------

/// `splitmix64` step (a seeded, dependency-free generator for the model's
/// durations).
fn mix(x: &mut u64) -> u64 {
    *x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn run_split_storm(c: &BgCell) -> BgReport {
    const TABLETS: usize = 12;
    const POOL: usize = 1;
    let mut r = BgReport::default();
    let mut rng = c.seed;
    let mut m = Metadata::default();
    for n in [1u64, 2, 3, 4] {
        let _ = m.apply(&MetaCommand::UpsertMember {
            node: nid(n),
            labels: BTreeMap::new(),
            status: NodeStatus::Active,
        });
    }
    // One tablet per table (a table's tablets partition its own ring), RF3 on
    // n2,n3,n4 so each child's directed Placing target (n1,n2,n3) differs and
    // a `split_placing` obligation exists until it is marked done.
    for i in 0..TABLETS {
        let t = TabletId(i as u64 + 1);
        let table = format!("t{i}");
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: t,
                table: Some(table.clone()),
                range: KeyRange::whole(),
                replicas: vec![nid(2), nid(3), nid(4)],
            }),
            ApplyOutcome::Applied
        );
        let _ = m.apply(&MetaCommand::SetTabletPolicy {
            tablet: t,
            policy: Some(PlacementPolicy::simple(&table, 3)),
        });
    }
    let budget = BackgroundBudget::with_fault(
        {
            let mut s = BudgetSizes::off();
            s.splits = Some(POOL);
            s
        },
        MetricsHandle::recording(),
        c.fault,
    );
    let permits = SplitPermits::new(budget.clone());
    let split_key = 0x8000_0000_0000_0000u64.to_be_bytes().to_vec();

    // The model: a started split finishes its fork after `fork`, then its two
    // children's Placing after `place` more (seeded, a few seconds each).
    struct Split {
        parent: TabletId,
        children: [TabletId; 2],
        cut_at: Duration,
        done_at: Duration,
        cut: bool,
        done: bool,
    }
    let mut running: Vec<Split> = Vec::new();
    let mut started: BTreeSet<TabletId> = BTreeSet::new();
    let mut next_child = TABLETS as u64 + 1;
    let mut max_in_progress = 0usize;
    let mut busy_refusals = 0u64;
    let mut placing_rows = 0usize;
    let mut now = Duration::ZERO;
    let sweep = Duration::from_secs(2);
    let mut next_sweep = Duration::ZERO;
    let limit = Duration::from_secs(60 * 10);
    while now < limit {
        let ns = Nanos(now.as_nanos() as u64);
        // Progress the model.
        for s in running.iter_mut() {
            if !s.cut && now >= s.cut_at {
                let epoch = m.tablets[&s.parent].epoch;
                assert_eq!(
                    m.apply(&MetaCommand::CutoverSplit {
                        parent: s.parent,
                        expected_epoch: epoch,
                        cutover_wall_ms: 1,
                    }),
                    ApplyOutcome::Applied
                );
                s.cut = true;
                // No directed-Placing obligation (the children already sit on
                // their targets): the split is finished at cutover.
                s.done = s
                    .children
                    .iter()
                    .all(|ch| m.split_placing.get(ch).is_none_or(|p| p.done));
                placing_rows += s
                    .children
                    .iter()
                    .filter(|ch| m.split_placing.contains_key(ch))
                    .count();
            }
            if s.cut && !s.done && now >= s.done_at {
                for ch in s.children {
                    if m.split_placing.get(&ch).is_some_and(|p| !p.done) {
                        let epoch = m.tablets[&ch].epoch;
                        let _ = m.apply(&MetaCommand::MarkSplitPlacingDone {
                            tablet: ch,
                            expected_epoch: epoch,
                        });
                    }
                }
                s.done = true;
            }
        }
        running.retain(|s| !s.done);
        // The auto-split sweep: every eligible tablet asks, ascending id.
        if now >= next_sweep {
            next_sweep += sweep;
            let eligible: Vec<TabletId> = (1..=TABLETS as u64)
                .map(TabletId)
                .filter(|t| !started.contains(t))
                .collect();
            for t in eligible {
                if permits.pool_full(t, &m, ns) {
                    busy_refusals += 1;
                    continue;
                }
                match permits.admit(t, &m, ns) {
                    SplitAdmission::Busy => busy_refusals += 1,
                    SplitAdmission::Granted(p) => {
                        let left = TabletId(next_child);
                        let right = TabletId(next_child + 1);
                        next_child += 2;
                        let epoch = m.tablets[&t].epoch;
                        assert_eq!(
                            m.apply(&MetaCommand::BeginSplitInPlace {
                                parent: t,
                                expected_epoch: epoch,
                                split_key: split_key.clone(),
                                children: [
                                    (left, vec![nid(2), nid(3), nid(4)]),
                                    (right, vec![nid(2), nid(3), nid(4)]),
                                ],
                            }),
                            ApplyOutcome::Applied
                        );
                        if let Some(p) = p {
                            permits.hold(t, p);
                        }
                        started.insert(t);
                        let fork = Duration::from_millis(500 + mix(&mut rng) % 2_500);
                        let place = Duration::from_millis(1_000 + mix(&mut rng) % 5_000);
                        running.push(Split {
                            parent: t,
                            children: [left, right],
                            cut_at: now + fork,
                            done_at: now + fork + place,
                            cut: false,
                            done: false,
                        });
                    }
                }
            }
        }
        max_in_progress = max_in_progress.max(running.len());
        if running.len() > POOL {
            r.bad(format!(
                "pool: {} splits in progress at {now:?}, pool is {POOL}",
                running.len()
            ));
            break;
        }
        if started.len() == TABLETS && running.is_empty() {
            break;
        }
        now += Duration::from_millis(100);
    }
    // One last release pass, as the next sweep would do.
    permits.release_finished(&m, Nanos(now.as_nanos() as u64));
    let split = (1..=TABLETS as u64)
        .filter(|t| !m.tablets.contains_key(&TabletId(*t)))
        .count();
    if split < TABLETS {
        r.bad(format!("progress: only {split} of {TABLETS} tablets split"));
    }
    let s = budget.stats(BgPool::Split);
    if s.admit_hwm > POOL {
        r.bad(format!("pool: admit hwm {} > {POOL}", s.admit_hwm));
    }
    if s.inflight > 0 || permits.held() > 0 {
        r.bad(format!(
            "leak: {} permits / {} held splits after every split finished",
            s.inflight,
            permits.held()
        ));
    }
    if busy_refusals == 0 || s.deferred == 0 {
        r.bad("pressure: the storm never found the pool full".into());
    }
    r.detail = format!(
        "split={split}/{TABLETS} end={now:?} max_in_progress={max_in_progress} \
         placing_rows={placing_rows} refusals={busy_refusals} adm={} def={}",
        s.admitted, s.deferred
    );
    r
}

// --- cell 8 -----------------------------------------------------------------

fn run_compaction_load(c: &BgCell) -> BgReport {
    const ENGINES: usize = 6;
    const POOL: usize = 2;
    const WRITES: u64 = 400;
    const TRIGGER: usize = 4;
    let mut r = BgReport::default();
    let mut sim = Simulator::new(c.seed);
    // A sync delay makes flush and compaction I/O yield, so runs on different
    // engines genuinely overlap under SimEnv.
    let mut disk = animus_sim::DiskConfig::default();
    disk.set_sync_delay(Duration::from_millis(4));
    sim.set_disk_config_for(nid(0), disk);
    let env = sim.env(nid(0));
    let budget = BackgroundBudget::with_fault(
        {
            let mut s = BudgetSizes::off();
            s.compactions = Some(POOL);
            s
        },
        MetricsHandle::recording(),
        c.fault,
    );
    // The node is under sustained load: the pool sits at its lag-reduced size.
    budget.set_lag(500_000_000);
    let opts = LsmOptions {
        flush_threshold_bytes: 256,
        compaction_trigger: TRIGGER,
        target_table_bytes: 1024,
        level_fanout: 2,
        wal_segment_bytes: 4096,
        tombstone_grace_versions: 1 << 20,
        trust_monotonic_versions: false,
        background_maintenance: true,
    };
    let engines: Vec<LsmEngine<SimEnv>> = (0..ENGINES)
        .map(|i| {
            let e =
                block_on(LsmEngine::open_with(env.clone(), format!("e{i}/"), opts)).expect("open");
            let envc = env.clone();
            let gate = budget
                .compaction_gate(Arc::new(move || envc.now()))
                .expect("compaction pool on");
            e.set_compaction_gate(gate);
            e
        })
        .collect();
    let acked: Arc<std::sync::Mutex<Vec<(usize, u64)>>> = Arc::default();
    for (i, e) in engines.iter().enumerate() {
        let e = e.clone();
        let acked = Arc::clone(&acked);
        let wenv = env.clone();
        env.spawn_task(async move {
            for k in 0..WRITES {
                let key = format!("k{k:05}");
                let val = format!("v{i}-{k}");
                if e.put(key.as_bytes(), val.as_bytes(), k + 1).await.is_ok() {
                    acked.lock().unwrap().push((i, k));
                }
                wenv.sleep(Duration::from_millis(2)).await;
            }
        });
    }
    let mut max_l0 = 0usize;
    let mut t = Duration::ZERO;
    while t < Duration::from_secs(40)
        && (acked.lock().unwrap().len() as u64) < ENGINES as u64 * WRITES
    {
        sim.run_for(Duration::from_millis(50));
        t += Duration::from_millis(50);
        for e in &engines {
            max_l0 = max_l0.max(e.l0_table_count());
        }
    }
    let load_ended = t;
    // Load over: the pool alone must settle every engine under the trigger.
    let mut settled = None;
    let mut waited = Duration::ZERO;
    while waited < Duration::from_secs(60) {
        if engines.iter().all(|e| e.l0_table_count() < TRIGGER) {
            settled = Some(waited);
            break;
        }
        sim.run_for(Duration::from_millis(250));
        waited += Duration::from_millis(250);
    }
    // A run past the trigger (a deeper-level compaction) may still be in
    // flight when L0 settles: let every in-flight run end before the leak
    // check, which asserts the slots come back once the work is done.
    let mut drain = Duration::ZERO;
    while budget.stats(BgPool::Compaction).inflight > 0 && drain < Duration::from_secs(30) {
        sim.run_for(Duration::from_millis(250));
        drain += Duration::from_millis(250);
    }
    let s = budget.stats(BgPool::Compaction);
    if s.admit_hwm > POOL {
        r.bad(format!(
            "pool: admitted compactions hwm {} > {POOL}",
            s.admit_hwm
        ));
    }
    // A bypass is the only way past the pool, and each engine runs one
    // maintenance pass at a time.
    if s.hwm > POOL + ENGINES {
        r.bad(format!(
            "bypass: hwm {} > pool {POOL} + one bypass per engine {ENGINES}",
            s.hwm
        ));
    }
    if s.bypassed > 0 && max_l0 < 2 * TRIGGER {
        r.bad(format!(
            "bypass: {} bypasses but L0 never reached 2 x trigger (max {max_l0})",
            s.bypassed
        ));
    }
    if s.admitted == 0 {
        r.bad("stalled: the pool admitted no compaction at all".into());
    }
    if settled.is_none() {
        let left: Vec<usize> = engines.iter().map(LsmEngine::l0_table_count).collect();
        r.bad(format!(
            "settle: L0 never fell under the trigger after the load (L0 per engine {left:?})"
        ));
    }
    let acked = acked.lock().unwrap().clone();
    if acked.len() as u64 != ENGINES as u64 * WRITES {
        r.bad(format!(
            "writes: {} of {} writes acked",
            acked.len(),
            ENGINES as u64 * WRITES
        ));
    }
    let lost = acked
        .iter()
        .filter(|(i, k)| {
            let got = block_on(engines[*i].get(format!("k{k:05}").as_bytes()))
                .ok()
                .flatten()
                .map(|v| v.value);
            got != Some(format!("v{i}-{k}").into_bytes())
        })
        .count();
    if lost > 0 {
        r.bad(format!("safety: {lost} acked writes lost or wrong"));
    }
    if s.inflight > 0 {
        r.bad(format!(
            "leak: {} compaction permits still held",
            s.inflight
        ));
    }
    r.detail = format!(
        "load_ended={load_ended:?} max_l0={max_l0} settled={settled:?} adm={} def={} bypass={} hwm={} admit_hwm={} \
         compactions={}",
        s.admitted,
        s.deferred,
        s.bypassed,
        s.hwm,
        s.admit_hwm,
        engines.iter().map(LsmEngine::compaction_count).sum::<u64>()
    );
    r
}

fn run_bg_cell(c: &BgCell) -> BgReport {
    match c.kind {
        BgKind::SnapshotStorm => run_snapshot_storm(c, bg_groups()),
        BgKind::UnackedBound => run_unacked_bound(c),
        BgKind::SplitStorm => run_split_storm(c),
        BgKind::CompactionLoad => run_compaction_load(c),
    }
}

#[test]
fn background_budget_cells_pass_the_oracle() {
    for c in bg_select(bg_cells()) {
        let r = run_bg_cell(&c);
        eprintln!(
            "DENSITY-BG-CELL {} seed={:#x} {} {:?}",
            c.name, c.seed, r.detail, r.violations
        );
        assert!(
            r.violations.is_empty(),
            "{} seed={:#x}: {:?} ({})",
            c.name,
            c.seed,
            r.violations,
            r.detail
        );
    }
}

/// NEGATIVE CONTROL bg_d (ADR control (d)): permits never returned. The learner
/// node's catch-up pool fills with permits of promoted learners, the rest never
/// catch up, and the oracle must say so.
#[test]
fn control_bg_d_a_permit_leak_strands_learners() {
    for c in bg_select(control_bg_d()) {
        let r = run_bg_cell(&c);
        eprintln!(
            "DENSITY-BG-CONTROL {} seed={:#x} {} {:?}",
            c.name, c.seed, r.detail, r.violations
        );
        assert!(
            r.violations.iter().all(|v| !v.starts_with("safety")),
            "{}: safety must hold: {:?}",
            c.name,
            r.violations
        );
        assert!(
            r.violations.iter().any(|v| v.starts_with("converge:")),
            "{} seed={:#x}: a leaking pool must strand learners: {:?} ({})",
            c.name,
            c.seed,
            r.violations,
            r.detail
        );
        assert!(
            r.violations.iter().any(|v| v.starts_with("leak:")),
            "{} seed={:#x}: the leak must be visible as held permits: {:?}",
            c.name,
            c.seed,
            r.violations
        );
    }
}

/// NEGATIVE CONTROL bg_d2: no unacknowledged-transfer bound. A dead receiver
/// keeps the only send slot and the next group's learner starves.
#[test]
fn control_bg_d2_no_unacked_bound_starves_the_next_transfer() {
    for c in bg_select(control_bg_d2()) {
        let r = run_bg_cell(&c);
        eprintln!(
            "DENSITY-BG-CONTROL {} seed={:#x} {} {:?}",
            c.name, c.seed, r.detail, r.violations
        );
        assert!(
            r.violations.iter().any(|v| v.starts_with("starved:")),
            "{} seed={:#x}: without the bound the next learner must starve: {:?} ({})",
            c.name,
            c.seed,
            r.violations,
            r.detail
        );
    }
}

/// NEGATIVE CONTROL bg_e (ADR control (e)): the pool may reach size 0 under
/// lag. Nothing is admitted by the pool; L0 drains only through the safety
/// valve (every compaction is a bypass), which the "stalled" oracle catches.
#[test]
fn control_bg_e_pool_minimum_zero_stalls_compaction() {
    for c in bg_select(control_bg_e()) {
        let r = run_bg_cell(&c);
        eprintln!(
            "DENSITY-BG-CONTROL {} seed={:#x} {} {:?}",
            c.name, c.seed, r.detail, r.violations
        );
        assert!(
            r.violations.iter().all(|v| !v.starts_with("safety")),
            "{}: no acked write may be lost even when compaction stalls: {:?}",
            c.name,
            r.violations
        );
        assert!(
            r.violations.iter().any(|v| v.starts_with("stalled:")),
            "{} seed={:#x}: a pool that can reach 0 must stall compaction: {:?} ({})",
            c.name,
            c.seed,
            r.violations,
            r.detail
        );
    }
}

#[test]
fn background_cell_names_and_seeds_are_unique_and_frozen() {
    let all: Vec<BgCell> = bg_cells()
        .into_iter()
        .chain(control_bg_d())
        .chain(control_bg_d2())
        .chain(control_bg_e())
        .collect();
    let names: BTreeSet<&str> = all.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names.len(), all.len());
    for c in &all {
        assert_eq!(c.seed, corpus::name_seed(&c.name));
    }
}

/// The same seed replays to the same numbers (no new nondeterminism).
#[test]
fn a_background_run_is_a_pure_function_of_its_seed() {
    for kind in [BgKind::SplitStorm, BgKind::CompactionLoad] {
        let c = bg_cell("bg_determinism", kind, BudgetFault::None);
        assert_eq!(run_bg_cell(&c).detail, run_bg_cell(&c).detail, "{kind:?}");
    }
}
