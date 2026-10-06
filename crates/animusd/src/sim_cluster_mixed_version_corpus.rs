//! `sim_cluster_mixed_version_corpus` — ADR 0073 Phase 2, workstream P2-D,
//! **cluster tier**: a rolling Phase 1 -> B2 upgrade over `SimCluster`
//! (roles `[Both, Both, Both, Data]`, RF 3, Memory backend) under the existing
//! linearizable DynamoDB-wire workload, with fault injection and control-leader
//! kills mid-roll. The pure per-`RaftNode` tier is
//! `animus-control/tests/it/version_mixed_corpus.rs`; this tier adds the node
//! assembly (tablet hosting, reconciler, DynamoDB wire, the data-only node's
//! mirror) on top.
//!
//! Each node plays a `BinaryProfile` (`SimCluster::set_binary_profile`, one
//! `apply_profile` helper shared with `restart`, so a restarted node never
//! silently reverts to Phase 1): its handshake `ext`, and, when control-bearing,
//! its `RaftNode`'s own range, build and capped decode (a Phase 1 node drops what
//! the real older binary could not decode, logging it).
//!
//! # Oracle
//!
//! - `check_cycles` over the whole history (`ConsistentRead: true` reads: the
//!   linearizability check this repo uses);
//! - `check_durability` / `check_convergence` per live replica of the table's
//!   tablet after a converged-or-timeout poll;
//! - **delivery**: no capped node ever rejected a delivery in a positive cell;
//! - **era safety** (every 50 ms of virtual time): the era is never active
//!   while any node still plays Phase 1; a Phase 1 control node's applied
//!   `Metadata` never shows versioning fields;
//! - **liveness**: after the last node is B2 the era actually starts and every
//!   node is recorded; every control replica applies what the leader committed;
//! - **non-vacuity**: acks before the roll, during it and after the era.
//!
//! # Cells (`ANIMUS_UPGRADE_SEEDS` x variants, `ANIMUS_UPGRADE_CELL=<substring>`,
//! `ANIMUS_SEED=<seed>` replay)
//!
//! - `roll_*`: rolling node by node (ascending / descending / data-node first /
//!   shuffled order) with a control-leader kill mid-roll and a transient
//!   partition;
//! - `kill_phase1_leader` / `kill_b2_leader`: deterministic leader-by-profile
//!   coverage (both profiles lead with the other profile's peers);
//! - `member_down_blocks_era`: the data-only node is down at P; no era until it
//!   returns as B2;
//! - `negative_control_premature_era_variant` (N1): a buggy proposer emits an era
//!   variant through the control leader while a Phase 1 voter exists; the oracle
//!   MUST fail with the exact expected violations.
//!
//! - `ladder_finalize_each_gate`: the synthetic gate ladder over `SimCluster`
//!   (every node `Release(3)` except one `Release(2)`): gate 2 opens at the
//!   first finalize on every node (the data-only node through its mirror),
//!   gate 3 stays closed and the second finalize is refused by name while the
//!   `[1,2]` node is recorded, opens after it rolls, and each gate's command is
//!   accepted everywhere with no capped rejection;
//! - `negative_control_ungated_variant` / `_ungated_field` / `_stale_view`
//!   (N2-N4): at cluster version 2 a gate-3 value is emitted the wrong way
//!   (gate check skipped, a payload field the classifier does not know, a
//!   feature handle opened on a forged view). The oracle MUST report the
//!   capped rejection at the `Release(2)` node, exactly that node wedged and
//!   the value applied before its gate;
//! - `joiner_phase1_after_era` / `joiner_phase1_dials_data_only_node`: a Phase
//!   1 joiner is refused at the seed's handshake (counted, never registered);
//!   the data-only variant dials a node whose require-peer-ext flag is latched
//!   by the version feeder from the mirror (`ControlHandle::Remote`); a B2
//!   joiner through the same seed is admitted and recorded;
//! - `joiner_range_checks`: the joiner's discovery check against
//!   `JoinInfo.cluster_version` (above max / below min refused by name before
//!   a `RegisterNode`, in-range joiners recorded with their range; a disjoint
//!   range is refused one layer earlier, at the handshake).
//!
//! - `release1_to_release2_global_gate`: the first real gate (`Gate::
//!   GlobalTables`, cluster version 2): B2 (`[1,1]`) -> `Release(2)` (`[1,2]`)
//!   with a table present; a relayed `ConvertTableToGlobal` is refused by name
//!   by the receiver before the finalize (counted, nothing appended), accepted
//!   after it, and the conversion (spec + pinned policies) lands on every node
//!   including the data-only node's mirror, with no capped rejection;
//! - `negative_control_global_gate_emitted_early` (N5): the same command
//!   appended ungated with a B2 voter present; the oracle MUST report the
//!   capped rejection at exactly that voter, wedged and never appended.
//!
//! - `release2_to_release3_mrec_gate`: the second real gate (`Gate::
//!   MrecReplication`, cluster version 3, G-01 stage G-d): `Release(2)`
//!   (`[1,2]`, finalized at 2) -> `Release(3)` (`[1,3]`); a relayed
//!   `ConvertTableToMrec` / `AddMrecReplica` is refused by name before the
//!   finalize (counted, nothing appended), a finalize to 3 is refused while a
//!   `[1,2]` node is recorded, and after the finalize the identical relays
//!   are accepted and the MREC spec lands on every node (data-only mirror
//!   included) without pinning any placement policy;
//! - `negative_control_mrec_gate_emitted_early` (N6): `ConvertTableToMrec`
//!   appended ungated at version 2 with a `Release(2)` voter; the oracle MUST
//!   report the capped rejection at exactly that voter, wedged and never
//!   appended, and the MREC spec applied before its gate on the others.
//!
//! # Not covered
//!
//! - Roll orders / leader kills over `Release(1) -> Release(2)`: the pure
//!   tier covers the per-node matrix; the ladder covers the finalize flow.
//!
//! Each cell runs on its own OS thread under a wall-clock watchdog
//! ([`CELL_WATCHDOG`], a bound on a hang, never a verdict).

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::sim_versions::BinaryProfile;
use animus_control::{MetaCommand, version::VersionRange};
use animus_env::nid;
use animus_test::corpus::{self, SeedVariant};
use animus_test::history::Key;
use animus_test::{Recorder, check_convergence, check_cycles, check_durability};

use super::sim_cluster::SimCluster;
use super::sim_cluster_upgrade_corpus::{
    CELL_WATCHDOG, DRAIN, NODES, SETTLE, Shared, TBL, combine, converge, create_table_body,
    final_state, live_replicas, ok_appends, run_until_clients_done, spawn_clients, tablet_of,
};
use super::*;
use crate::config::NodeRole;

const ROLES: [NodeRole; 4] = [
    NodeRole::Both,
    NodeRole::Both,
    NodeRole::Both,
    NodeRole::Data,
];
const REPLICATION: usize = 3;
const CONTROL: [u64; 3] = [0, 1, 2];
const ROUNDS_ROLL: u64 = 120;
const ROUNDS_PHASE2: u64 = 6;
const TICK: Duration = Duration::from_millis(50);

// ---------------------------------------------------------------------------
// Cells
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    /// Roll in `order` (index into [`ORDERS`]).
    Roll(usize),
    KillPhase1Leader,
    KillB2Leader,
    MemberDown,
    NegativeControl,
    /// The synthetic gate ladder: gate 2 then gate 3 open at their finalizes.
    Ladder,
    /// N2/N3/N4 over the ladder at cluster version 2.
    LadderNegative(Neg),
    /// A Phase 1 joiner after the era (`via_data`: dialing the data-only node).
    Phase1Joiner {
        via_data: bool,
    },
    /// Joiner range checks at discovery (`RegisterNode` joiners).
    JoinerRange,
    /// B2 -> Release(2) over `Gate::GlobalTables` (the first real gate).
    GlobalGate,
    /// N5: `ConvertTableToGlobal` emitted ungated with a B2 voter.
    GlobalGateNegative,
    /// Release(2) -> Release(3) over `Gate::MrecReplication` (G-01 stage G-d).
    MrecGate,
    /// N6: `ConvertTableToMrec` emitted ungated with a Release(2) voter.
    MrecGateNegative,
}

/// The three ladder negative controls (ADR 0073 section 7).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Neg {
    /// The emitter skips the gate check.
    UngatedVariant,
    /// A payload field the classifier does not know (`required_gate` = Base).
    UngatedField,
    /// The emitter's feature handle opened on a forged, newer view.
    StaleView,
}

const ORDERS: [[u64; 4]; 4] = [[0, 1, 2, 3], [3, 2, 1, 0], [3, 0, 1, 2], [2, 3, 0, 1]];

#[derive(Clone, Debug)]
struct Cell {
    name: String,
    seed: u64,
    kind: Kind,
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

fn cell(name: &str, kind: Kind) -> Cell {
    Cell {
        seed: corpus::name_seed(&format!("sim_cluster_mixed_version/{name}")),
        name: name.to_string(),
        kind,
    }
}

fn cells() -> Vec<Cell> {
    vec![
        cell("roll_ascending", Kind::Roll(0)),
        cell("roll_descending", Kind::Roll(1)),
        cell("roll_data_first", Kind::Roll(2)),
        cell("roll_shuffled", Kind::Roll(3)),
        cell("kill_phase1_leader", Kind::KillPhase1Leader),
        cell("kill_b2_leader", Kind::KillB2Leader),
        cell("member_down_blocks_era", Kind::MemberDown),
        cell(
            "negative_control_premature_era_variant",
            Kind::NegativeControl,
        ),
        cell("ladder_finalize_each_gate", Kind::Ladder),
        cell(
            "negative_control_ungated_variant",
            Kind::LadderNegative(Neg::UngatedVariant),
        ),
        cell(
            "negative_control_ungated_field",
            Kind::LadderNegative(Neg::UngatedField),
        ),
        cell(
            "negative_control_stale_view",
            Kind::LadderNegative(Neg::StaleView),
        ),
        cell(
            "joiner_phase1_after_era",
            Kind::Phase1Joiner { via_data: false },
        ),
        cell(
            "joiner_phase1_dials_data_only_node",
            Kind::Phase1Joiner { via_data: true },
        ),
        cell("joiner_range_checks", Kind::JoinerRange),
        cell("release1_to_release2_global_gate", Kind::GlobalGate),
        cell(
            "negative_control_global_gate_emitted_early",
            Kind::GlobalGateNegative,
        ),
        cell("release2_to_release3_mrec_gate", Kind::MrecGate),
        cell(
            "negative_control_mrec_gate_emitted_early",
            Kind::MrecGateNegative,
        ),
    ]
}

fn seeds_per_cell() -> usize {
    corpus::seeds_from_env("ANIMUS_UPGRADE_SEEDS")
}

/// The cells after the depth knob, `ANIMUS_SEED` replay and the optional
/// `ANIMUS_UPGRADE_CELL` name filter.
fn corpus_cells() -> Vec<Cell> {
    let filter = std::env::var("ANIMUS_UPGRADE_CELL").ok();
    let expanded = if let Some(seed) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        cells()
            .into_iter()
            .map(|c| {
                let name = c.name.clone();
                c.reseeded(name, seed)
            })
            .collect()
    } else {
        corpus::seed_expand(cells(), seeds_per_cell())
    };
    expanded
        .into_iter()
        .filter(|c| filter.as_ref().is_none_or(|f| c.name.contains(f.as_str())))
        .collect()
}

// ---------------------------------------------------------------------------
// Verdict + watchdog
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Verdict {
    cell: String,
    seed: u64,
    violations: Vec<String>,
    acks: (usize, usize, usize),
    /// N1 only: the capped decode rejected the premature variant.
    cap_fired: bool,
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|m| (*m).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<non-string panic payload>".to_string())
}

fn run_cell(c: &Cell) -> Verdict {
    let (tx, rx) = std::sync::mpsc::channel();
    let cc = c.clone();
    std::thread::Builder::new()
        .name(format!("sim_cluster_mixed_version/{}", c.name))
        .stack_size(16 * 1024 * 1024)
        .spawn(move || {
            eprintln!(
                "sim_cluster_mixed_version cell={} seed={}",
                cc.name, cc.seed
            );
            let v = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&cc))) {
                Ok(v) => v,
                Err(p) => Verdict {
                    cell: cc.name.clone(),
                    seed: cc.seed,
                    violations: vec![format!("panic: {}", panic_message(p))],
                    acks: (0, 0, 0),
                    cap_fired: false,
                },
            };
            let _ = tx.send(v);
        })
        .expect("spawn the cell thread");
    match rx.recv_timeout(CELL_WATCHDOG) {
        Ok(v) => v,
        Err(e) => panic!(
            "sim_cluster_mixed_version cell={} HUNG or died (seed={}): no verdict within {:?} \
             ({e}) -- replay with ANIMUS_SEED={} ANIMUS_UPGRADE_CELL={}",
            c.name, c.seed, CELL_WATCHDOG, c.seed, c.name
        ),
    }
}

fn assert_ok(v: &Verdict) {
    assert!(
        v.violations.is_empty(),
        "sim_cluster_mixed_version cell={} FAILED (seed={}): {:#?}",
        v.cell,
        v.seed,
        v.violations
    );
}

fn run(c: &Cell) -> Verdict {
    match c.kind {
        Kind::NegativeControl => run_negative_control(c),
        Kind::Ladder => run_ladder(c),
        Kind::LadderNegative(n) => run_ladder_negative(c, n),
        Kind::Phase1Joiner { via_data } => run_phase1_joiner(c, via_data),
        Kind::JoinerRange => run_joiner_range(c),
        Kind::GlobalGate => run_global_gate(c),
        Kind::GlobalGateNegative => run_global_negative(c),
        Kind::MrecGate => run_mrec_gate(c),
        Kind::MrecGateNegative => run_mrec_negative(c),
        _ => run_roll(c),
    }
}

// ---------------------------------------------------------------------------
// The oracle
// ---------------------------------------------------------------------------

/// Instantaneous safety violations.
fn instant_violations(cluster: &SimCluster, delivery: bool) -> Vec<String> {
    let mut v = Vec::new();
    let any_phase1 = (0..NODES).any(|n| cluster.profile_of(n) == BinaryProfile::Phase1);
    for n in CONTROL {
        let m = cluster.metadata(n);
        if m.versioning_active() && any_phase1 {
            v.push(format!(
                "era active on node {n} while a node still plays Phase 1"
            ));
        }
        if cluster.profile_of(n) == BinaryProfile::Phase1
            && (m.versioning_active() || m.cluster_version() != 1)
        {
            v.push(format!(
                "Phase 1 node {n}'s applied Metadata shows versioning fields"
            ));
        }
    }
    // Gate discipline at apply: a value carrying a synthetic gate/field label
    // `n` is applied only once the cluster version reached `n`.
    for n in CONTROL {
        let m = cluster.metadata(n);
        for (node, mem) in &m.members {
            for k in [
                animus_control::version::SYNTHETIC_GATE_LABEL,
                animus_control::sim_versions::SYNTHETIC_FIELD_LABEL,
            ] {
                if let Some(need) = mem.labels.get(k).and_then(|x| x.parse::<u32>().ok())
                    && m.cluster_version() < need
                {
                    v.push(format!(
                        "gate applied early: node {n} applied {k}={need} on {node:?} at \
                         cluster version {}",
                        m.cluster_version()
                    ));
                }
            }
        }
    }
    // ADR 0075 G-d: an MREC spec is applied only once the cluster version
    // reached 3 (`Gate::MrecReplication`).
    for n in CONTROL {
        let m = cluster.metadata(n);
        if m.cluster_version() < 3 && m.table_global(GLOBAL_TABLE).is_some_and(|g| g.is_mrec()) {
            v.push(format!(
                "gate applied early: node {n} holds an MREC spec at cluster version {}",
                m.cluster_version()
            ));
        }
    }
    if delivery {
        for (n, r) in cluster.cap_rejections() {
            v.push(format!(
                "delivery: node {n} ({:?}) rejected a {:?} message from {:?}",
                r.profile, r.gate, r.from
            ));
        }
    }
    v
}

/// Control replicas whose applied index trails the (live) leader's commit.
fn wedged_control(cluster: &SimCluster) -> Vec<u64> {
    let commit = CONTROL
        .iter()
        .filter(|&&n| cluster.is_control_leader(n))
        .map(|&n| cluster.control_raft_indices(n).0)
        .max();
    let Some(commit) = commit else {
        return CONTROL.to_vec();
    };
    CONTROL
        .iter()
        .copied()
        .filter(|&n| cluster.control_raft_indices(n).1 < commit)
        .collect()
}

fn era_fully_recorded(cluster: &SimCluster) -> bool {
    (0..NODES).all(|n| {
        let m = cluster.metadata(n);
        m.versioning_active() && m.node_versions.len() == NODES as usize
    })
}

/// Advance `dur` in 50 ms slices, collecting instant violations (once each)
/// and which leader profiles were seen (bit 0 Phase 1, bit 1 B2).
struct Watch {
    violations: Vec<String>,
    leaders: u8,
    delivery: bool,
}

impl Watch {
    fn new(delivery: bool) -> Self {
        Self {
            violations: Vec::new(),
            leaders: 0,
            delivery,
        }
    }

    fn run(&mut self, cluster: &mut SimCluster, dur: Duration) {
        let mut left = dur;
        while !left.is_zero() {
            let d = left.min(TICK);
            cluster.run_for(d);
            left -= d;
            self.sample(cluster);
        }
    }

    fn sample(&mut self, cluster: &SimCluster) {
        for s in instant_violations(cluster, self.delivery) {
            if !self.violations.contains(&s) {
                self.violations.push(s);
            }
        }
        for n in CONTROL {
            if cluster.is_control_leader(n) {
                self.leaders |= if cluster.profile_of(n) == BinaryProfile::Phase1 {
                    1
                } else {
                    2
                };
            }
        }
    }

    fn assert_no_era(&mut self, cluster: &SimCluster, why: &str) {
        if CONTROL
            .iter()
            .any(|&n| cluster.metadata(n).versioning_active())
        {
            self.violations.push(format!("era started: {why}"));
        }
    }
}

fn leader_of_control(cluster: &SimCluster, down: &BTreeSet<u64>) -> Option<u64> {
    CONTROL
        .iter()
        .copied()
        .find(|n| !down.contains(n) && cluster.is_control_leader(*n))
}

fn roll(cluster: &mut SimCluster, w: &mut Watch, node: u64, restart: bool) {
    cluster.set_binary_profile(node, BinaryProfile::B2);
    if restart {
        cluster.restart(node);
    }
    w.run(cluster, Duration::from_millis(500));
}

// ---------------------------------------------------------------------------
// Rolling cells
// ---------------------------------------------------------------------------

fn setup(seed: u64) -> (SimCluster, Arc<Shared>) {
    let mut cluster = SimCluster::new_with_roles_and_segment_janitor_retention_and_cp_quiescence(
        seed,
        &ROLES,
        REPLICATION,
        Duration::from_secs(3600),
        Some(Duration::from_secs(DEFAULT_QUIESCE_AFTER_SECS)),
    );
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        create_table_body(TBL, false).as_bytes(),
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    cluster.run_for(SETTLE);
    // Install the Phase 1 profile (and with it the capped decode) explicitly:
    // a SimCluster node that was never given a profile has no cap at all.
    for n in 0..NODES {
        cluster.set_binary_profile(n, BinaryProfile::Phase1);
    }
    let shared = Arc::new(Shared {
        rec: Mutex::new(Recorder::new(seed)),
        next_value: Mutex::new(0),
        done: Mutex::new(0),
    });
    (cluster, shared)
}

fn run_roll(c: &Cell) -> Verdict {
    let seed = c.seed;
    let (mut cluster, shared) = setup(seed);
    let mut w = Watch::new(true);
    let mut down: BTreeSet<u64> = BTreeSet::new();
    let _clients = spawn_clients(&cluster, &shared, 0, ROUNDS_ROLL);
    w.run(&mut cluster, Duration::from_secs(1));
    let acks_before = ok_appends(&shared.history());
    let tablet = tablet_of(&cluster, 0, TBL).expect("table has a tablet");

    match c.kind {
        Kind::Roll(o) => {
            for (i, &node) in ORDERS[o].iter().enumerate() {
                if i == 2 {
                    // Mid-roll: kill the control leader, transiently partition
                    // two nodes, with a Phase 1 / B2 mix standing.
                    let mixed = (0..NODES).any(|n| cluster.profile_of(n) == BinaryProfile::Phase1)
                        && (0..NODES).any(|n| cluster.profile_of(n) != BinaryProfile::Phase1);
                    if !mixed {
                        w.violations
                            .push("vacuous: no profile mix at the kill".into());
                    }
                    if let Some(l) = leader_of_control(&cluster, &down) {
                        cluster.crash(l);
                        down.insert(l);
                        w.run(&mut cluster, Duration::from_millis(1500));
                        cluster.heal_all();
                        down.clear();
                    }
                    cluster.partition(0, 3);
                    w.run(&mut cluster, Duration::from_millis(800));
                    cluster.heal_all();
                }
                roll(&mut cluster, &mut w, node, true);
            }
        }
        Kind::KillPhase1Leader | Kind::KillB2Leader => {
            let l = leader_of_control(&cluster, &down).expect("a control leader");
            if c.kind == Kind::KillPhase1Leader {
                for n in (0..NODES).filter(|&n| n != l) {
                    cluster.set_binary_profile(n, BinaryProfile::B2);
                }
            } else {
                cluster.set_binary_profile(l, BinaryProfile::B2);
            }
            w.run(&mut cluster, Duration::from_secs(4));
            if !cluster.is_control_leader(l) {
                w.violations.push("leadership moved before the kill".into());
            }
            w.assert_no_era(&cluster, "a leader/peer profile mix");
            cluster.crash(l);
            down.insert(l);
            w.run(&mut cluster, Duration::from_secs(3));
            w.assert_no_era(&cluster, "after the leader kill");
            cluster.heal_all();
            down.clear();
            w.run(&mut cluster, Duration::from_secs(3));
            w.assert_no_era(&cluster, "leader back, mix remains");
            for n in 0..NODES {
                if cluster.profile_of(n) == BinaryProfile::Phase1 {
                    roll(&mut cluster, &mut w, n, true);
                }
            }
            if w.leaders != 3 {
                w.violations.push(format!(
                    "the roll must see both a Phase 1 and a B2 leader (mask {})",
                    w.leaders
                ));
            }
        }
        Kind::MemberDown => {
            // The data-only node is down at P: roll the control nodes, wait.
            cluster.crash(3);
            for n in CONTROL {
                roll(&mut cluster, &mut w, n, true);
            }
            w.run(&mut cluster, Duration::from_secs(6));
            w.assert_no_era(&cluster, "the data-only node was never observed as B2");
            cluster.heal_all();
            roll(&mut cluster, &mut w, 3, true);
        }
        Kind::NegativeControl
        | Kind::Ladder
        | Kind::LadderNegative(_)
        | Kind::Phase1Joiner { .. }
        | Kind::JoinerRange
        | Kind::GlobalGate
        | Kind::GlobalGateNegative
        | Kind::MrecGate
        | Kind::MrecGateNegative => unreachable!(),
    }

    // Liveness: the era starts once the last node is B2, every node recorded.
    cluster.heal_all();
    let started = converge(&mut cluster, |c| {
        w.sample(c);
        era_fully_recorded(c)
    });
    if !started {
        w.violations
            .push("the era never started after the last node became B2".into());
    }
    let acks_during = ok_appends(&shared.history());
    if !run_until_clients_done(&mut cluster, &shared) {
        w.violations
            .push("the workload did not finish within its budget".into());
    }
    cluster.run_for(DRAIN);
    // Phase 2: the cluster still serves on the new era.
    let _ids = spawn_clients(&cluster, &shared, 1, ROUNDS_PHASE2);
    if !run_until_clients_done(&mut cluster, &shared) {
        w.violations
            .push("phase-2 workload did not finish within its budget".into());
    }
    cluster.run_for(DRAIN);
    w.sample(&cluster);

    let wedged = {
        let mut last = wedged_control(&cluster);
        let _ = converge(&mut cluster, |c| {
            last = wedged_control(c);
            last.is_empty()
        });
        last
    };
    if !wedged.is_empty() {
        w.violations.push(format!(
            "control replicas {wedged:?} wedged: applied < leader commit"
        ));
    }

    let history = shared.history();
    let cycles = check_cycles(&history);
    w.violations.extend(
        cycles
            .violations
            .into_iter()
            .map(|v| format!("cycles: {v}")),
    );
    let handle = cluster.handle();
    let states = |c: &SimCluster| -> Vec<std::collections::BTreeMap<Key, Vec<u64>>> {
        live_replicas(c, tablet)
            .iter()
            .map(|&n| final_state(&handle, tablet, n))
            .collect()
    };
    let verdict = |c: &SimCluster| {
        let st = states(c);
        let durability = combine(seed, st.iter().map(|s| check_durability(&history, s)));
        let convergence = combine(
            seed,
            st.iter()
                .skip(1)
                .map(|s| check_convergence(seed, &st[0], s)),
        );
        (st.len(), durability, convergence)
    };
    let mut last = verdict(&cluster);
    let _ = converge(&mut cluster, |c| {
        last = verdict(c);
        last.0 > 0 && last.1.ok && last.2.ok
    });
    if last.0 == 0 {
        w.violations.push("no live replica of the table".into());
    }
    w.violations.extend(
        last.1
            .violations
            .into_iter()
            .map(|v| format!("durability: {v}")),
    );
    w.violations.extend(
        last.2
            .violations
            .into_iter()
            .map(|v| format!("convergence: {v}")),
    );
    let total = ok_appends(&history);
    if acks_before == 0 {
        w.violations.push("vacuous: no ack before the roll".into());
    }
    // The member-down cell crashes a node the shared client loop still routes
    // 1/NODES of its ops to (each stalls for the wire timeout), so acks during
    // its roll are legitimately sparse; it asserts acks after the era instead.
    if acks_during <= acks_before && c.kind != Kind::MemberDown {
        w.violations.push("vacuous: no ack during the roll".into());
    }
    if total <= acks_during {
        w.violations.push("vacuous: no ack after the era".into());
    }
    Verdict {
        cell: c.name.clone(),
        seed,
        violations: w.violations,
        acks: (acks_before, acks_during, total),
        cap_fired: false,
    }
}

// ---------------------------------------------------------------------------
// N1: the negative control
// ---------------------------------------------------------------------------

/// A premature era variant while a Phase 1 voter exists: the test acts as the
/// buggy emitter, proposing `ReportNodeVersion` through the control leader and
/// bypassing precondition P (which lives in the version loop, so no production
/// code is touched). The oracle MUST fail with the exact expected violations.
fn run_negative_control(c: &Cell) -> Verdict {
    let seed = c.seed;
    let (mut cluster, _shared) = setup(seed);
    let mut w = Watch::new(true);
    // A B2 leader, a B2 follower and one Phase 1 voter.
    let leader = leader_of_control(&cluster, &BTreeSet::new()).expect("a control leader");
    let victim = CONTROL
        .iter()
        .copied()
        .find(|&n| n != leader)
        .expect("a follower");
    for n in (0..NODES).filter(|&n| n != victim) {
        cluster.set_binary_profile(n, BinaryProfile::B2);
    }
    w.run(&mut cluster, Duration::from_millis(300));
    let pre = instant_violations(&cluster, true);
    let pre_log = cluster.control_raft_indices(victim);
    // The buggy emitter.
    let mut accepted = false;
    // Index of the premature entry: the leader's last log index right after the
    // first accepted propose (single-threaded sim, nothing interleaves).
    let mut era_idx = None;
    for _ in 0..40 {
        let ok = matches!(
            cluster.propose_meta_ungated(MetaCommand::ReportNodeVersion {
                node: nid(leader),
                range: VersionRange::new(1, 1),
                build: "b2".into(),
            }),
            ProposeResult::Accepted { .. }
        );
        accepted |= ok;
        if ok && era_idx.is_none() {
            era_idx = Some(cluster.control_last_log_index(leader));
        }
        w.run(&mut cluster, Duration::from_millis(100));
        if cluster.metadata(leader).versioning_active() {
            break;
        }
    }
    w.run(&mut cluster, Duration::from_secs(5));
    let mut v: Vec<String> = Vec::new();
    // Exact expected outcome.
    if !pre.is_empty() {
        v.push(format!("violations before the premature emit: {pre:?}"));
    }
    if !accepted {
        v.push("the premature emit was never accepted".into());
    }
    let wedged = wedged_control(&cluster);
    if wedged != vec![victim] {
        v.push(format!(
            "expected only node {victim} wedged, got {wedged:?}"
        ));
    }
    if cluster.control_raft_indices(victim).0 > pre_log.0 + 2 {
        v.push("the Phase 1 replica advanced its commit past the era variant".into());
    }
    // The capped decode took the undecodable branch: the batch never entered
    // the Phase 1 replica's log (a "logs but delivers" cap appends it).
    if era_idx.is_some_and(|i| cluster.control_last_log_index(victim) >= i) {
        v.push("the Phase 1 replica appended the era variant".into());
    }
    let cap_fired = w
        .violations
        .iter()
        .any(|s| s.starts_with(&format!("delivery: node {victim} (Phase1)")));
    if !cap_fired && cluster.simulator().protocol_refusals(&nid(victim)) == 0 {
        v.push("neither the cap nor the handshake kept the variant from the victim".into());
    }
    if !w
        .violations
        .iter()
        .any(|s| s.contains("era active on node") && s.contains("still plays Phase 1"))
    {
        v.push(format!("era safety did not trip: {:?}", w.violations));
    }
    // A passing (no violation) result here is the bug; invert: return the
    // *list of shortfalls* as violations, so an empty list means the oracle
    // caught the premature variant exactly as expected.
    Verdict {
        cell: c.name.clone(),
        seed,
        violations: v,
        acks: (0, 0, 0),
        cap_fired,
    }
}

// ---------------------------------------------------------------------------
// P2-D cells that need no workload: the synthetic gate ladder, N2-N4, joiners
// ---------------------------------------------------------------------------

/// The previous-release node of the ladder cells: `Release(2)`, range `[1,2]`.
const OLD: u64 = 2;
/// The data-only node.
const DATA: u64 = 3;

fn light_cluster(seed: u64) -> SimCluster {
    let mut cluster = SimCluster::new_with_roles(seed, &ROLES, 2);
    let _ = cluster.control_leader_index();
    cluster
}

fn control_leader(cluster: &mut SimCluster) -> u64 {
    let idx = cluster.control_leader_index();
    cluster.control_node_id(idx)
}

/// Every node plays a `Release(n)` binary: `OLD` is `[1,2]`, the rest `[1,3]`.
fn play_ladder_binaries(cluster: &mut SimCluster) {
    for n in 0..NODES {
        let (lo, hi) = if n == OLD { (1, 2) } else { (1, 3) };
        cluster.set_binary_profile(n, BinaryProfile::Release(hi));
        cluster.set_node_version(n, Some(VersionRange::new(lo, hi)));
    }
}

fn marked(label: &str, gate: u32) -> MetaCommand {
    MetaCommand::UpsertMember {
        node: nid(DATA),
        labels: [(label.to_string(), gate.to_string())].into(),
        status: animus_control::NodeStatus::Active,
    }
}

fn marked_applied(m: &animus_control::Metadata, label: &str, gate: u32) -> bool {
    m.members
        .get(&nid(DATA))
        .is_some_and(|mem| mem.labels.get(label) == Some(&gate.to_string()))
}

fn finalize(cluster: &mut SimCluster, node: u64, body: &str) -> (u16, String) {
    cluster.admin(
        node,
        "POST",
        "/admin/cluster-version/finalize",
        "",
        body.as_bytes(),
    )
}

/// Era on and every node recorded, then finalize to 2 and wait for every
/// node's feature handle (the data-only node's through its mirror) to show it.
fn ladder_to_v2(cluster: &mut SimCluster, w: &mut Watch) {
    play_ladder_binaries(cluster);
    if !converge(cluster, |c| {
        w.sample(c);
        (0..NODES).all(|n| c.features(n).era_active())
            && (0..NODES).all(|n| c.metadata(n).node_versions.len() == NODES as usize)
    }) {
        w.violations.push("the era never started".into());
    }
    let l = control_leader(cluster);
    let (status, body) = finalize(cluster, l, r#"{"to":2,"expected":1}"#);
    if status != 200 {
        w.violations
            .push(format!("finalize 1 -> 2: {status} {body}"));
    }
    if !converge(cluster, |c| {
        w.sample(c);
        (0..NODES).all(|n| c.features(n).cluster_version() == 2)
    }) {
        w.violations
            .push("not every node observed cluster version 2".into());
    }
}

fn verdict_of(c: &Cell, violations: Vec<String>, cap_fired: bool) -> Verdict {
    Verdict {
        cell: c.name.clone(),
        seed: c.seed,
        violations,
        acks: (0, 0, 0),
        cap_fired,
    }
}

fn run_ladder(c: &Cell) -> Verdict {
    use animus_control::version::{Gate, SYNTHETIC_GATE_LABEL};
    let mut cluster = light_cluster(c.seed);
    let mut w = Watch::new(true);
    ladder_to_v2(&mut cluster, &mut w);
    // Gate 2 is open on every node, gate 3 closed.
    for n in 0..NODES {
        let f = cluster.features(n);
        if !f.is_open(Gate::Synthetic(2)) || f.is_open(Gate::Synthetic(3)) {
            w.violations
                .push(format!("node {n}: gate 2/3 not open/closed at version 2"));
        }
    }
    if !matches!(
        cluster.propose_meta(marked(SYNTHETIC_GATE_LABEL, 2)),
        ProposeResult::Accepted { .. }
    ) {
        w.violations
            .push("a gate-2 command was refused at version 2".into());
    }
    if !converge(&mut cluster, |c| {
        w.sample(c);
        CONTROL
            .iter()
            .all(|&n| marked_applied(&c.metadata(n), SYNTHETIC_GATE_LABEL, 2))
    }) {
        w.violations
            .push("the gate-2 command never applied everywhere".into());
    }
    // Finalize to 3 is blocked, by name, by the previous-release node.
    let l = control_leader(&mut cluster);
    let old_id = cluster.handle().env(OLD).node_id();
    let (status, body) = finalize(&mut cluster, l, "{}");
    // Named either way: a leader that is not the old node names it as the
    // blocker; when the old node itself leads, its own max (2) refuses first.
    let named = (body.contains(&old_id.to_string()) && body.contains("excludes target 3"))
        || (l == OLD && body.contains("cannot finalize 3"));
    if status != 409 || !named {
        w.violations.push(format!(
            "finalize 2 -> 3 not blocked by name: {status} {body}"
        ));
    }
    w.run(&mut cluster, Duration::from_secs(1));
    if (0..NODES).any(|n| cluster.features(n).is_open(Gate::Synthetic(3))) {
        w.violations
            .push("gate 3 opened with a [1,2] node recorded".into());
    }
    // The node rolls to the next release; gate 3 opens at the finalize.
    cluster.set_binary_profile(OLD, BinaryProfile::Release(3));
    cluster.set_node_version(OLD, Some(VersionRange::new(2, 3)));
    if !converge(&mut cluster, |c| {
        w.sample(c);
        let l = (0..NODES).find(|&n| c.is_control_leader(n));
        l.is_some_and(|l| {
            c.metadata(l)
                .node_versions
                .get(&old_id)
                .is_some_and(|v| v.range == VersionRange::new(2, 3))
        })
    }) {
        w.violations
            .push("the rolled node's [2,3] record never landed".into());
    }
    let l = control_leader(&mut cluster);
    let (status, body) = finalize(&mut cluster, l, r#"{"to":3,"expected":2}"#);
    if status != 200 {
        w.violations
            .push(format!("finalize 2 -> 3: {status} {body}"));
    }
    if !converge(&mut cluster, |c| {
        w.sample(c);
        (0..NODES).all(|n| c.features(n).is_open(Gate::Synthetic(3)))
    }) {
        w.violations
            .push("gate 3 never opened on every node".into());
    }
    if !matches!(
        cluster.propose_meta(marked(SYNTHETIC_GATE_LABEL, 3)),
        ProposeResult::Accepted { .. }
    ) {
        w.violations
            .push("a gate-3 command was refused at version 3".into());
    }
    if !converge(&mut cluster, |c| {
        w.sample(c);
        CONTROL
            .iter()
            .all(|&n| marked_applied(&c.metadata(n), SYNTHETIC_GATE_LABEL, 3))
    }) {
        w.violations
            .push("the gate-3 command never applied everywhere".into());
    }
    // Every hosted CP group's handle (fed from the same control view) agrees.
    for n in 0..NODES {
        let hosted = cluster.hosted_group_features(n);
        if hosted.iter().any(|f| !f.is_open(Gate::Synthetic(3))) {
            w.violations.push(format!(
                "a group hosted on node {n} still has gate 3 closed"
            ));
        }
    }
    let wedged = {
        let mut last = wedged_control(&cluster);
        let _ = converge(&mut cluster, |c| {
            last = wedged_control(c);
            last.is_empty()
        });
        last
    };
    if !wedged.is_empty() {
        w.violations
            .push(format!("control replicas {wedged:?} wedged"));
    }
    w.sample(&cluster);
    verdict_of(c, w.violations, false)
}

/// At cluster version 2 with a `Release(2)` voter, emit a gate-3 value the
/// wrong way. The verdict lists every shortfall of the oracle (an empty list
/// is the pass: the oracle caught the bad emit with the exact violations).
fn run_ladder_negative(c: &Cell, neg: Neg) -> Verdict {
    use animus_control::sim_versions::SYNTHETIC_FIELD_LABEL;
    use animus_control::version::SYNTHETIC_GATE_LABEL;
    let mut cluster = light_cluster(c.seed);
    let mut w = Watch::new(true);
    ladder_to_v2(&mut cluster, &mut w);
    let setup = std::mem::take(&mut w.violations);
    // A leader that is not the previous-release node.
    if control_leader(&mut cluster) == OLD {
        let _ = cluster.transfer_leadership(OLD, 0);
        w.run(&mut cluster, Duration::from_secs(2));
    }
    let leader = control_leader(&mut cluster);
    let mut v: Vec<String> = setup;
    if leader == OLD {
        v.push("could not move leadership off the previous-release node".into());
    }
    let pre = instant_violations(&cluster, true);
    if !pre.is_empty() {
        v.push(format!("violations before the bad emit: {pre:?}"));
    }
    let pre_log = cluster.control_last_log_index(OLD);
    let accepted = match neg {
        Neg::UngatedVariant => cluster.propose_meta_ungated(marked(SYNTHETIC_GATE_LABEL, 3)),
        Neg::UngatedField => {
            let cmd = marked(SYNTHETIC_FIELD_LABEL, 3);
            if animus_control::version::GatedCommand::required_gate(&cmd)
                != animus_control::version::Gate::Base
            {
                v.push("the field marker is classified: the control is vacuous".into());
            }
            cluster.propose_meta(cmd)
        }
        Neg::StaleView => {
            let mut forged = cluster.metadata(leader);
            let old_id = cluster.handle().env(OLD).node_id();
            if let Some(nv) = forged.node_versions.get_mut(&old_id) {
                nv.range = VersionRange::new(2, 3);
            }
            let out = forged.apply(&MetaCommand::FinalizeClusterVersion {
                expected: 2,
                target: 3,
            });
            if format!("{out:?}") != "Applied" {
                v.push(format!("the forged view did not finalize: {out:?}"));
            }
            cluster.control_features(leader).update(&forged);
            cluster.propose_meta(marked(SYNTHETIC_GATE_LABEL, 3))
        }
    };
    if !matches!(accepted, ProposeResult::Accepted { .. }) {
        v.push(format!("the bad emit was not accepted: {accepted:?}"));
    }
    w.run(&mut cluster, Duration::from_secs(6));
    let wedged = wedged_control(&cluster);
    if wedged != vec![OLD] {
        v.push(format!("expected only node {OLD} wedged, got {wedged:?}"));
    }
    let cap_fired = w
        .violations
        .iter()
        .any(|s| s.starts_with(&format!("delivery: node {OLD} (Release(2))")));
    if !cap_fired {
        v.push(format!(
            "the cap never rejected the gate-3 value: {:?}",
            w.violations
        ));
    }
    if !w
        .violations
        .iter()
        .any(|s| s.starts_with("gate applied early"))
    {
        v.push(format!(
            "early application did not trip: {:?}",
            w.violations
        ));
    }
    if cluster.control_last_log_index(OLD) != pre_log {
        v.push("the previous-release replica appended the gate-3 value".into());
    }
    verdict_of(c, v, cap_fired)
}

// ---------------------------------------------------------------------------
// The first real gate: `Gate::GlobalTables` (cluster version 2, G-01 stage G-c)
// ---------------------------------------------------------------------------

const GLOBAL_TABLE: &str = "g1";

fn global_spec() -> animus_control::GlobalTableSpec {
    animus_control::GlobalTableSpec {
        consistency: animus_control::MultiRegionConsistency::Strong,
        regions: vec!["a".into(), "b".into(), "c".into()],
        witness: None,
        preferred_leader_region: "a".into(),
        replicas: Vec::new(),
    }
}

fn convert_cmd() -> MetaCommand {
    MetaCommand::ConvertTableToGlobal {
        table: GLOBAL_TABLE.to_string(),
        spec: global_spec(),
    }
}

/// The table is global with its tablets' policies pinned to the spec's regions.
fn converted(m: &animus_control::Metadata) -> bool {
    m.schemas
        .get(GLOBAL_TABLE)
        .is_some_and(|s| s.global == Some(global_spec()))
        && m.tablets_for_table(GLOBAL_TABLE).count() > 0
        && m.tablets_for_table(GLOBAL_TABLE)
            .all(|(id, _)| m.policies.get(id).is_some_and(|p| p.is_pinned()))
}

/// Every node plays B2 (`[1,1]`, the previous release), the era starts, and a
/// table exists (created pre-era, `Gate::Base`). `OLD` stays B2; every other
/// node rolls to `Release(2)` (`[1,2]`) when `roll_all` is false, all of them
/// when true.
fn global_setup(cluster: &mut SimCluster, w: &mut Watch, roll_all: bool) {
    let _ = cluster.create_table(GLOBAL_TABLE);
    for n in 0..NODES {
        cluster.set_binary_profile(n, BinaryProfile::B2);
        cluster.set_node_version(n, Some(VersionRange::new(1, 1)));
    }
    if !converge(cluster, |c| {
        w.sample(c);
        (0..NODES).all(|n| c.features(n).era_active())
            && (0..NODES).all(|n| c.metadata(n).node_versions.len() == NODES as usize)
    }) {
        w.violations.push("the era never started".into());
    }
    for n in 0..NODES {
        if roll_all || n != OLD {
            cluster.set_binary_profile(n, BinaryProfile::Release(2));
            cluster.set_node_version(n, Some(VersionRange::new(1, 2)));
            w.run(cluster, Duration::from_millis(500));
        }
    }
}

fn propose_convert_request() -> animus_node::ClientRequest {
    animus_node::ClientRequest::ProposeSchema(convert_cmd())
}

/// Roll B2 -> Release(2) over a table; before the finalize a relayed
/// `ConvertTableToGlobal` is refused by name by the receiver (counted,
/// nothing appended); after it the identical relay is accepted and the
/// conversion (spec + pinned policies) lands on every node, the data-only
/// node's mirror included. No capped rejection, no wedged control replica.
fn run_global_gate(c: &Cell) -> Verdict {
    use animus_control::version::Gate;
    let mut cluster = light_cluster(c.seed);
    let mut w = Watch::new(true);
    global_setup(&mut cluster, &mut w, true);
    if !converge(&mut cluster, |c| {
        w.sample(c);
        (0..NODES).all(|n| {
            c.metadata(n)
                .node_versions
                .values()
                .all(|v| v.range == VersionRange::new(1, 2))
                && c.metadata(n).node_versions.len() == NODES as usize
        })
    }) {
        w.violations
            .push("the [1,2] records never landed on every node".into());
    }
    let leader = control_leader(&mut cluster);
    let follower = (0..3u64).find(|n| *n != leader).expect("a follower");
    // Version 1: the gate is closed everywhere.
    if (0..NODES).any(|n| cluster.features(n).is_open(Gate::GlobalTables)) {
        w.violations
            .push("GlobalTables open before the finalize".into());
    }
    let pre_log: Vec<u64> = CONTROL
        .iter()
        .map(|&n| cluster.control_last_log_index(n))
        .collect();
    for (from, to) in [(follower, leader), (leader, follower)] {
        let before = cluster.metric(to, animus_env::Metric::ClusterGateRelayRefused);
        match cluster.relay_request(from, to, propose_convert_request()) {
            Some(animus_node::ClientResponse::Error(msg))
                if msg.contains("relayed command refused") && msg.contains("GlobalTables") => {}
            other => w
                .violations
                .push(format!("relay {from}->{to} not refused by name: {other:?}")),
        }
        if cluster.metric(to, animus_env::Metric::ClusterGateRelayRefused) != before + 1 {
            w.violations
                .push(format!("refusal not counted on node {to}"));
        }
    }
    w.run(&mut cluster, Duration::from_secs(2));
    let post_log: Vec<u64> = CONTROL
        .iter()
        .map(|&n| cluster.control_last_log_index(n))
        .collect();
    if post_log != pre_log {
        w.violations
            .push("a refused relay appended to the control log".into());
    }
    if (0..NODES).any(|n| {
        cluster
            .metadata(n)
            .schemas
            .get(GLOBAL_TABLE)
            .is_some_and(|s| s.global.is_some())
    }) {
        w.violations
            .push("the table became global before the gate opened".into());
    }
    // Finalize to 2: the gate opens on every node (the data-only one through
    // its mirror) and on every hosted group's handle.
    let l = control_leader(&mut cluster);
    let (status, body) = finalize(&mut cluster, l, r#"{"to":2,"expected":1}"#);
    if status != 200 {
        w.violations
            .push(format!("finalize 1 -> 2: {status} {body}"));
    }
    if !converge(&mut cluster, |c| {
        w.sample(c);
        (0..NODES).all(|n| c.features(n).is_open(Gate::GlobalTables))
    }) {
        w.violations
            .push("GlobalTables never opened on every node".into());
    }
    // The identical relay is now accepted and lands everywhere.
    let leader = control_leader(&mut cluster);
    let follower = (0..3u64).find(|n| *n != leader).expect("a follower");
    match cluster.relay_request(follower, leader, propose_convert_request()) {
        Some(animus_node::ClientResponse::PutOk) => {}
        other => w
            .violations
            .push(format!("the post-finalize relay was refused: {other:?}")),
    }
    if !converge(&mut cluster, |c| {
        w.sample(c);
        (0..NODES).all(|n| converted(&c.metadata(n)))
    }) {
        w.violations
            .push("the conversion never landed on every node (mirror included)".into());
    }
    let wedged = {
        let mut last = wedged_control(&cluster);
        let _ = converge(&mut cluster, |c| {
            last = wedged_control(c);
            last.is_empty()
        });
        last
    };
    if !wedged.is_empty() {
        w.violations
            .push(format!("control replicas {wedged:?} wedged"));
    }
    w.sample(&cluster);
    verdict_of(c, w.violations, false)
}

/// N5: a buggy emitter appends `ConvertTableToGlobal` at cluster version 1
/// with a previous-release (B2) voter. The cap must reject it at exactly that
/// voter, which is wedged and never appended the entry.
fn run_global_negative(c: &Cell) -> Verdict {
    let mut cluster = light_cluster(c.seed);
    let mut w = Watch::new(true);
    global_setup(&mut cluster, &mut w, false);
    let setup = std::mem::take(&mut w.violations);
    if control_leader(&mut cluster) == OLD {
        let _ = cluster.transfer_leadership(OLD, 0);
        w.run(&mut cluster, Duration::from_secs(2));
    }
    let mut v: Vec<String> = setup;
    if control_leader(&mut cluster) == OLD {
        v.push("could not move leadership off the previous-release node".into());
    }
    let pre = instant_violations(&cluster, true);
    if !pre.is_empty() {
        v.push(format!("violations before the bad emit: {pre:?}"));
    }
    let pre_log = cluster.control_last_log_index(OLD);
    let accepted = cluster.propose_meta_ungated(convert_cmd());
    if !matches!(accepted, ProposeResult::Accepted { .. }) {
        v.push(format!("the bad emit was not accepted: {accepted:?}"));
    }
    w.run(&mut cluster, Duration::from_secs(6));
    let wedged = wedged_control(&cluster);
    if wedged != vec![OLD] {
        v.push(format!("expected only node {OLD} wedged, got {wedged:?}"));
    }
    let cap_fired = w.violations.iter().any(|s| {
        s.starts_with(&format!("delivery: node {OLD} (B2)")) && s.contains("GlobalTables")
    });
    if !cap_fired {
        v.push(format!(
            "the cap never rejected the GlobalTables value: {:?}",
            w.violations
        ));
    }
    if cluster.control_last_log_index(OLD) != pre_log {
        v.push("the previous-release replica appended the GlobalTables value".into());
    }
    verdict_of(c, v, cap_fired)
}

// ---------------------------------------------------------------------------
// The second real gate: `Gate::MrecReplication` (cluster version 3, G-01 stage G-d)
// ---------------------------------------------------------------------------

fn mrec_convert_cmd() -> MetaCommand {
    MetaCommand::ConvertTableToMrec {
        table: GLOBAL_TABLE.to_string(),
        local_region: "us".to_string(),
        region_id: animus_control::mrec_region_id("us"),
    }
}

fn mrec_add_cmd() -> MetaCommand {
    MetaCommand::AddMrecReplica {
        table: GLOBAL_TABLE.to_string(),
        region: "eu".to_string(),
        region_id: animus_control::mrec_region_id("eu"),
    }
}

/// The table is an MREC table with this cluster's local replica `us` and the
/// peer `eu` recorded.
fn mrec_converted(m: &animus_control::Metadata) -> bool {
    m.table_global(GLOBAL_TABLE).is_some_and(|g| {
        g.is_mrec()
            && g.replicas.iter().any(|r| r.local && r.region == "us")
            && g.replicas.iter().any(|r| r.region == "eu")
    })
}

/// Every node `Release(2)` (`[1, 2]`, the G-c binary as it shipped) and the
/// cluster finalized to version 2, a table present. `keep_old` leaves node
/// `OLD` as the only `Release(2)` once the others roll to `Release(3)`
/// (`[1, 3]`); otherwise nobody rolls yet (the caller rolls).
fn mrec_setup(cluster: &mut SimCluster, w: &mut Watch) {
    global_setup(cluster, w, true);
    if !converge(cluster, |c| {
        w.sample(c);
        (0..NODES).all(|n| {
            c.metadata(n)
                .node_versions
                .values()
                .all(|v| v.range == VersionRange::new(1, 2))
                && c.metadata(n).node_versions.len() == NODES as usize
        })
    }) {
        w.violations
            .push("the [1,2] records never landed on every node".into());
    }
    let l = control_leader(cluster);
    let (status, body) = finalize(cluster, l, r#"{"to":2,"expected":1}"#);
    if status != 200 {
        w.violations
            .push(format!("finalize 1 -> 2: {status} {body}"));
    }
    if !converge(cluster, |c| {
        w.sample(c);
        (0..NODES).all(|n| c.metadata(n).cluster_version() == 2)
    }) {
        w.violations
            .push("cluster version 2 never reached on every node".into());
    }
}

fn roll_to_release3(cluster: &mut SimCluster, w: &mut Watch, nodes: &[u64]) {
    for &n in nodes {
        cluster.set_binary_profile(n, BinaryProfile::Release(3));
        cluster.set_node_version(n, Some(VersionRange::new(1, 3)));
        w.run(cluster, Duration::from_millis(500));
    }
}

/// Roll `Release(2)` -> `Release(3)` over a table; before the finalize a
/// relayed `ConvertTableToMrec` is refused by name by the receiver (counted,
/// nothing appended), a finalize to 3 is refused by apply while a `[1,2]`
/// node is recorded; after the finalize the identical relay is accepted and
/// the MREC spec lands on every node, the data-only node's mirror included,
/// untouched placement, no capped rejection, no wedged control replica.
fn run_mrec_gate(c: &Cell) -> Verdict {
    use animus_control::version::Gate;
    let mut cluster = light_cluster(c.seed);
    let mut w = Watch::new(true);
    mrec_setup(&mut cluster, &mut w);
    if (0..NODES).any(|n| cluster.features(n).is_open(Gate::MrecReplication)) {
        w.violations
            .push("MrecReplication open at version 2".into());
    }
    // A half-rolled mix (nodes 0,1 and 3 are Release(3), node 2 is not): the
    // gate is closed and a finalize to 3 is refused by name while node 2
    // records `[1,2]`.
    roll_to_release3(&mut cluster, &mut w, &[0, 1, DATA]);
    let leader = control_leader(&mut cluster);
    let (status, body) = finalize(&mut cluster, leader, r#"{"to":3,"expected":2}"#);
    if status == 200 {
        w.violations.push(format!(
            "a finalize to 3 was accepted with a [1,2] node: {body}"
        ));
    }
    roll_to_release3(&mut cluster, &mut w, &[OLD]);
    if !converge(&mut cluster, |c| {
        w.sample(c);
        (0..NODES).all(|n| {
            c.metadata(n)
                .node_versions
                .values()
                .all(|v| v.range == VersionRange::new(1, 3))
                && c.metadata(n).node_versions.len() == NODES as usize
        })
    }) {
        w.violations
            .push("the [1,3] records never landed on every node".into());
    }
    let leader = control_leader(&mut cluster);
    let follower = (0..3u64).find(|n| *n != leader).expect("a follower");
    if (0..NODES).any(|n| cluster.features(n).is_open(Gate::MrecReplication)) {
        w.violations
            .push("MrecReplication open before the finalize".into());
    }
    let pre_log: Vec<u64> = CONTROL
        .iter()
        .map(|&n| cluster.control_last_log_index(n))
        .collect();
    for cmd in [mrec_convert_cmd(), mrec_add_cmd()] {
        for (from, to) in [(follower, leader), (leader, follower)] {
            let before = cluster.metric(to, animus_env::Metric::ClusterGateRelayRefused);
            match cluster.relay_request(
                from,
                to,
                animus_node::ClientRequest::ProposeSchema(cmd.clone()),
            ) {
                Some(animus_node::ClientResponse::Error(msg))
                    if msg.contains("relayed command refused")
                        && msg.contains("MrecReplication") => {}
                other => w
                    .violations
                    .push(format!("relay {from}->{to} not refused by name: {other:?}")),
            }
            if cluster.metric(to, animus_env::Metric::ClusterGateRelayRefused) != before + 1 {
                w.violations
                    .push(format!("refusal not counted on node {to}"));
            }
        }
    }
    w.run(&mut cluster, Duration::from_secs(2));
    let post_log: Vec<u64> = CONTROL
        .iter()
        .map(|&n| cluster.control_last_log_index(n))
        .collect();
    if post_log != pre_log {
        w.violations
            .push("a refused relay appended to the control log".into());
    }
    // Finalize 2 -> 3: the gate opens on every node (the data-only one
    // through its mirror).
    let l = control_leader(&mut cluster);
    let (status, body) = finalize(&mut cluster, l, r#"{"to":3,"expected":2}"#);
    if status != 200 {
        w.violations
            .push(format!("finalize 2 -> 3: {status} {body}"));
    }
    if !converge(&mut cluster, |c| {
        w.sample(c);
        (0..NODES).all(|n| c.features(n).is_open(Gate::MrecReplication))
    }) {
        w.violations
            .push("MrecReplication never opened on every node".into());
    }
    let leader = control_leader(&mut cluster);
    let follower = (0..3u64).find(|n| *n != leader).expect("a follower");
    for cmd in [mrec_convert_cmd(), mrec_add_cmd()] {
        match cluster.relay_request(
            follower,
            leader,
            animus_node::ClientRequest::ProposeSchema(cmd),
        ) {
            Some(animus_node::ClientResponse::PutOk) => {}
            other => w
                .violations
                .push(format!("the post-finalize relay was refused: {other:?}")),
        }
        w.run(&mut cluster, Duration::from_millis(500));
    }
    if !converge(&mut cluster, |c| {
        w.sample(c);
        (0..NODES).all(|n| mrec_converted(&c.metadata(n)))
    }) {
        w.violations
            .push("the MREC spec never landed on every node (mirror included)".into());
    }
    // MREC never pins placement.
    if (0..NODES).any(|n| {
        cluster
            .metadata(n)
            .tablets_for_table(GLOBAL_TABLE)
            .any(|(id, _)| {
                cluster
                    .metadata(n)
                    .policies
                    .get(id)
                    .is_some_and(|p| p.is_pinned())
            })
    }) {
        w.violations
            .push("an MREC conversion pinned a policy".into());
    }
    let wedged = {
        let mut last = wedged_control(&cluster);
        let _ = converge(&mut cluster, |c| {
            last = wedged_control(c);
            last.is_empty()
        });
        last
    };
    if !wedged.is_empty() {
        w.violations
            .push(format!("control replicas {wedged:?} wedged"));
    }
    w.sample(&cluster);
    verdict_of(c, w.violations, false)
}

/// N6: a buggy emitter appends `ConvertTableToMrec` at cluster version 2 with
/// a `Release(2)` voter. The cap must reject it at exactly that voter, which
/// is wedged and never appended the entry; the early application on the
/// others is reported too.
fn run_mrec_negative(c: &Cell) -> Verdict {
    let mut cluster = light_cluster(c.seed);
    let mut w = Watch::new(true);
    mrec_setup(&mut cluster, &mut w);
    let others: Vec<u64> = (0..NODES).filter(|&n| n != OLD).collect();
    roll_to_release3(&mut cluster, &mut w, &others);
    let setup = std::mem::take(&mut w.violations);
    if control_leader(&mut cluster) == OLD {
        let _ = cluster.transfer_leadership(OLD, 0);
        w.run(&mut cluster, Duration::from_secs(2));
    }
    let mut v: Vec<String> = setup;
    if control_leader(&mut cluster) == OLD {
        v.push("could not move leadership off the Release(2) node".into());
    }
    let pre = instant_violations(&cluster, true);
    if !pre.is_empty() {
        v.push(format!("violations before the bad emit: {pre:?}"));
    }
    let pre_log = cluster.control_last_log_index(OLD);
    let accepted = cluster.propose_meta_ungated(mrec_convert_cmd());
    if !matches!(accepted, ProposeResult::Accepted { .. }) {
        v.push(format!("the bad emit was not accepted: {accepted:?}"));
    }
    w.run(&mut cluster, Duration::from_secs(6));
    let wedged = wedged_control(&cluster);
    if wedged != vec![OLD] {
        v.push(format!("expected only node {OLD} wedged, got {wedged:?}"));
    }
    let cap_fired = w.violations.iter().any(|s| {
        s.starts_with(&format!("delivery: node {OLD} (Release(2))"))
            && s.contains("MrecReplication")
    });
    if !cap_fired {
        v.push(format!(
            "the cap never rejected the MrecReplication value: {:?}",
            w.violations
        ));
    }
    if !w
        .violations
        .iter()
        .any(|s| s.starts_with("gate applied early"))
    {
        v.push("the early MREC application went unnoticed".into());
    }
    if cluster.control_last_log_index(OLD) != pre_log {
        v.push("the Release(2) replica appended the MrecReplication value".into());
    }
    verdict_of(c, v, cap_fired)
}

/// A Phase 1 binary joining after the era is refused at the seed's handshake
/// (counted, never a member); the cluster is unaffected; a versioned joiner
/// through the same seed is admitted and recorded. `via_data` dials the
/// data-only node, whose require-peer-ext flag is latched by the version
/// feeder from the mirror (`ControlHandle::Remote`), not by a local apply task.
fn run_phase1_joiner(c: &Cell, via_data: bool) -> Verdict {
    let mut cluster = light_cluster(c.seed);
    let mut w = Watch::new(true);
    for n in 0..NODES {
        cluster.set_binary_profile(n, BinaryProfile::B2);
        cluster.set_node_version(n, Some(VersionRange::new(1, 1)));
    }
    if !converge(&mut cluster, |cl| {
        w.sample(cl);
        (0..NODES).all(|n| cl.features(n).era_active())
            && (0..NODES).all(|n| cl.metadata(n).node_versions.len() == NODES as usize)
    }) {
        w.violations.push("the era never started".into());
    }
    let seed_node = if via_data { DATA } else { 0 };
    let leader = control_leader(&mut cluster);
    let members_before = cluster.metadata(leader).node_addrs.len();
    let nodes_before = cluster.node_count();
    let refusals_before = cluster.simulator().protocol_refusals(&nid(seed_node));
    let r = cluster.try_join_via_seed_as(seed_node as usize, NodeRole::Data, None);
    if r.is_ok() {
        w.violations
            .push("a Phase 1 joiner was admitted after the era".into());
    }
    if cluster.simulator().protocol_refusals(&nid(seed_node)) <= refusals_before {
        w.violations.push(format!(
            "node {seed_node} never refused the Phase 1 joiner at the handshake"
        ));
    }
    let leader = control_leader(&mut cluster);
    if cluster.node_count() != nodes_before
        || cluster.metadata(leader).node_addrs.len() != members_before
    {
        w.violations
            .push("the refused Phase 1 joiner was registered".into());
    }
    // The cluster keeps committing.
    let probe = MetaCommand::UpsertMember {
        node: nid(DATA),
        labels: [("probe".to_string(), "1".to_string())].into(),
        status: animus_control::NodeStatus::Active,
    };
    if !matches!(cluster.propose_meta(probe), ProposeResult::Accepted { .. }) {
        w.violations
            .push("the cluster stopped committing after the refusal".into());
    }
    // Control: a versioned joiner through the same seed is admitted + recorded.
    match cluster.try_join_via_seed_as(
        seed_node as usize,
        NodeRole::Data,
        Some(VersionRange::new(1, 1)),
    ) {
        Err(e) => w
            .violations
            .push(format!("a B2 joiner was refused after the era: {e}")),
        Ok(idx) => {
            let id = cluster.handle().env(idx).node_id();
            if !converge(&mut cluster, |cl| {
                w.sample(cl);
                let l = (0..NODES).find(|&n| cl.is_control_leader(n));
                l.is_some_and(|l| cl.metadata(l).node_versions.contains_key(&id))
            }) {
                w.violations
                    .push("the B2 joiner's version record never landed".into());
            }
        }
    }
    w.sample(&cluster);
    verdict_of(c, w.violations, false)
}

/// The joiner's range check at discovery (`JoinInfo.cluster_version`): a
/// binary whose range excludes the cluster version is refused by name before
/// it claims an identity (`RegisterNode`); an in-range one joins and is
/// recorded with its range.
fn run_joiner_range(c: &Cell) -> Verdict {
    let mut w = Watch::new(true);
    let mut v = Vec::new();
    // At cluster version 2, every node a `[1,3]` binary (so a joiner whose
    // range merely excludes 2 still passes the handshake's range-overlap
    // check and reaches the discovery check).
    let mut cluster = light_cluster(c.seed);
    for n in 0..NODES {
        cluster.set_binary_profile(n, BinaryProfile::Release(3));
        cluster.set_node_version(n, Some(VersionRange::new(1, 3)));
    }
    if !converge(&mut cluster, |cl| {
        w.sample(cl);
        (0..NODES).all(|n| cl.features(n).era_active())
            && (0..NODES).all(|n| cl.metadata(n).node_versions.len() == NODES as usize)
    }) {
        v.push("the era never started".into());
    }
    let l = control_leader(&mut cluster);
    let (status, body) = finalize(&mut cluster, l, r#"{"to":2,"expected":1}"#);
    if status != 200 {
        v.push(format!("finalize 1 -> 2: {status} {body}"));
    }
    if !converge(&mut cluster, |cl| {
        w.sample(cl);
        (0..NODES).all(|n| cl.features(n).cluster_version() == 2)
    }) {
        v.push("not every node observed cluster version 2".into());
    }
    let nodes = cluster.node_count();
    for (range, want) in [
        (
            VersionRange::new(3, 4),
            "cluster version 2 is below this binary's min 3",
        ),
        (
            VersionRange::new(1, 1),
            "cluster version 2 is above this binary's max 1 (downgrade is not supported)",
        ),
    ] {
        match cluster.try_join_via_seed_as(0, NodeRole::Data, Some(range)) {
            Ok(_) => v.push(format!("a {range:?} joiner joined a version-2 cluster")),
            Err(e) if e.contains(want) => {}
            Err(e) => v.push(format!("{range:?}: expected {want:?}, got {e}")),
        }
    }
    if cluster.node_count() != nodes {
        v.push("a refused joiner was assembled into the cluster".into());
    }
    // A range disjoint from every node's is refused one layer earlier, by the
    // handshake (counted at the seed; discovery never answers).
    let before = cluster.simulator().protocol_refusals(&nid(0));
    if cluster
        .try_join_via_seed_as(0, NodeRole::Data, Some(VersionRange::new(4, 5)))
        .is_ok()
    {
        v.push("a disjoint-range joiner joined".into());
    }
    if cluster.simulator().protocol_refusals(&nid(0)) <= before {
        v.push("the handshake never refused the disjoint-range joiner".into());
    }
    let leader = control_leader(&mut cluster);
    let registered = cluster.metadata(leader).node_addrs.len();
    if registered != NODES as usize {
        v.push(format!("a refused joiner registered ({registered} rows)"));
    }
    for range in [VersionRange::new(1, 2), VersionRange::new(2, 3)] {
        match cluster.try_join_via_seed_as(0, NodeRole::Data, Some(range)) {
            Err(e) => v.push(format!("an in-range {range:?} joiner was refused: {e}")),
            Ok(idx) => {
                let id = cluster.handle().env(idx).node_id();
                if !converge(&mut cluster, |cl| {
                    w.sample(cl);
                    let l = (0..NODES).find(|&n| cl.is_control_leader(n));
                    l.is_some_and(|l| {
                        cl.metadata(l)
                            .node_versions
                            .get(&id)
                            .is_some_and(|nv| nv.range == range)
                    })
                }) {
                    v.push(format!("the {range:?} joiner's record never landed"));
                }
            }
        }
    }
    v.extend(w.violations);
    verdict_of(c, v, false)
}

// ---------------------------------------------------------------------------
// The tests
// ---------------------------------------------------------------------------

/// One test per cell family so nextest spreads them across its shards.
fn run_family(prefix: &str) {
    let cs: Vec<Cell> = corpus_cells()
        .into_iter()
        .filter(|c| c.name.starts_with(prefix))
        .collect();
    let narrowed = std::env::var("ANIMUS_UPGRADE_CELL").is_ok();
    assert!(
        narrowed || !cs.is_empty(),
        "no cell selected for family {prefix}"
    );
    let mut fired = false;
    for c in &cs {
        let v = run_cell(c);
        fired |= v.cap_fired;
        eprintln!(
            "  {} violations={} acks before/during/total={:?}",
            v.cell,
            v.violations.len(),
            v.acks
        );
        assert_ok(&v);
    }
    // N1: the capped decode must have rejected the premature variant on at
    // least one seed (it can lose a race to the era-on handshake refusal on
    // another, which wedges the replica just the same).
    if prefix == "negative_control_premature" && !narrowed && std::env::var("ANIMUS_SEED").is_err()
    {
        assert!(
            fired,
            "the capped decode never rejected the premature variant"
        );
    }
}

#[test]
fn sim_cluster_mixed_version_corpus_roll() {
    run_family("roll_");
}

#[test]
fn sim_cluster_mixed_version_corpus_leader_kills() {
    run_family("kill_");
}

#[test]
fn sim_cluster_mixed_version_corpus_member_down() {
    run_family("member_down");
}

/// N1: the verdict of the negative-control cell lists a shortfall for anything
/// the oracle should have caught but did not; an empty list is the pass.
#[test]
fn sim_cluster_mixed_version_corpus_negative_control_is_caught() {
    run_family("negative_control_premature");
}

#[test]
fn sim_cluster_mixed_version_corpus_ladder() {
    run_family("ladder_");
}

#[test]
fn sim_cluster_mixed_version_corpus_negative_control_ungated_variant() {
    run_family("negative_control_ungated_variant");
}

#[test]
fn sim_cluster_mixed_version_corpus_negative_control_ungated_field() {
    run_family("negative_control_ungated_field");
}

#[test]
fn sim_cluster_mixed_version_corpus_negative_control_stale_view() {
    run_family("negative_control_stale_view");
}

#[test]
fn sim_cluster_mixed_version_corpus_phase1_joiner_after_era() {
    run_family("joiner_phase1_after_era");
}

#[test]
fn sim_cluster_mixed_version_corpus_data_only_node_refuses_phase1_joiner() {
    run_family("joiner_phase1_dials_data_only_node");
}

#[test]
fn sim_cluster_mixed_version_corpus_joiner_range_checks() {
    run_family("joiner_range_checks");
}

#[test]
fn sim_cluster_mixed_version_corpus_global_gate() {
    run_family("release1_to_release2_global_gate");
}

#[test]
fn sim_cluster_mixed_version_corpus_negative_control_global_gate() {
    run_family("negative_control_global_gate_emitted_early");
}

#[test]
fn sim_cluster_mixed_version_corpus_mrec_gate() {
    run_family("release2_to_release3_mrec_gate");
}

#[test]
fn sim_cluster_mixed_version_corpus_negative_control_mrec_gate() {
    run_family("negative_control_mrec_gate_emitted_early");
}

#[test]
fn sim_cluster_mixed_version_cell_names_and_seeds_are_unique() {
    let cs = corpus::seed_expand(cells(), 3);
    let names: BTreeSet<_> = cs.iter().map(|c| c.name.clone()).collect();
    let seeds: BTreeSet<_> = cs.iter().map(|c| c.seed).collect();
    assert_eq!(names.len(), cs.len(), "duplicate cell name");
    assert_eq!(seeds.len(), cs.len(), "duplicate cell seed");
    // Every family test selects at least one cell (a filter that matches
    // nothing must not pass green).
    for prefix in [
        "roll_",
        "kill_",
        "member_down",
        "negative_control_premature",
        "ladder_",
        "negative_control_ungated_variant",
        "negative_control_ungated_field",
        "negative_control_stale_view",
        "joiner_phase1_after_era",
        "joiner_phase1_dials_data_only_node",
        "joiner_range_checks",
        "release1_to_release2_global_gate",
        "negative_control_global_gate_emitted_early",
        "release2_to_release3_mrec_gate",
        "negative_control_mrec_gate_emitted_early",
    ] {
        assert!(
            cells().iter().any(|c| c.name.starts_with(prefix)),
            "family {prefix} is empty"
        );
    }
}

#[test]
fn sim_cluster_mixed_version_run_is_deterministic() {
    let c = cell("roll_ascending", Kind::Roll(0));
    let a = run_cell(&c);
    let b = run_cell(&c);
    assert_eq!(a.violations, b.violations, "same seed, different verdict");
    assert_eq!(a.acks, b.acks, "same seed, different acks");
}
