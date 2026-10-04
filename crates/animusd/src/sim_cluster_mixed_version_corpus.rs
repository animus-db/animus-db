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
//! # Pending (not registered, so nothing passes vacuously)
//!
//! - Phase 1 *joiners* after the era and the data-only node's own era flag: P2-C
//!   (`ControlHandle::Remote` flips `require_peer_ext`; `RegisterNode` joiners).
//! - "Gate 2 opens only after every row reports" with a real emitter, and the
//!   synthetic gate ladder: P2-B (`required_gate` tables) + P2-C (`ClusterFeatures`
//!   fed into emitters).
//! - `Release(N-1) -> Release(N)` rolls over real gates: first real gate.
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
        Kind::NegativeControl => unreachable!(),
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
    if prefix == "negative_control" && !narrowed && std::env::var("ANIMUS_SEED").is_err() {
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
    run_family("negative_control");
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
    for prefix in ["roll_", "kill_", "member_down", "negative_control"] {
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
