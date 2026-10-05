//! ADR 0073 Phase 2 (P2-D) mixed-version corpus, **pure tier**: control-plane
//! `RaftNode`s playing `BinaryProfile`s (`Phase1`, `B2`, `Release(N)`) over the
//! shared `version_world` harness in its *faithful* mode, where every node's
//! binary is applied atomically through `RaftNode::set_binary_profile` (own
//! range + build + the capped decode that drops what the real older binary
//! could not decode, with a rejection log). P2-A's own cells
//! (`version_observe_corpus`, `version_era_on`) are untouched; these add the
//! faithful-profile oracle on top.
//!
//! The oracle (`violations`) checks, every 25 ms of virtual time and at the end:
//! - **era safety**: the era is never active while a required node is still a
//!   Phase 1 binary;
//! - **delivery**: no capped node ever rejected a delivery (the "no Phase 1
//!   node ever receives a byte it cannot decode" assertion, read off the
//!   production drop point's log);
//! - **state**: a Phase 1 node's applied `Metadata` never shows versioning
//!   fields (a snapshot would carry them; the real Phase 1 serde drops them);
//! - **wedge**: at quiescence every live replica has applied what the leader
//!   committed.
//!
//! Cells (`ANIMUS_UPGRADE_SEEDS` x variants; `ANIMUS_UPGRADE_CELL=<substring>`
//! selects cells; `ANIMUS_SEED=<seed>` replays one; every assertion prints the
//! seed):
//! - `roll_orders_with_faults`: Phase1 -> B2 in several orders (voters first,
//!   members first, shuffled), a forced leader kill mid-roll plus random faults
//!   and restarts; the era starts only after the last node.
//! - `kill_phase1_leader` / `kill_b2_leader`: a Phase 1 (resp. B2) leader with
//!   B2 (resp. Phase 1) peers, deterministically; the era cannot start; the
//!   leader is killed; both profiles lead during the roll.
//! - `kill_leader_around_p`: the leader is killed at swept offsets after the
//!   last flip, around precondition P's hold window.
//! - `member_down_blocks_era`: a registered member never observed (or back as
//!   Phase 1) blocks the era until B2 or removed.
//! - `phase1_after_the_era_is_refused`: a Phase 1 binary joining / reinstalled
//!   after the era is refused at the handshake, counted, never recorded; the
//!   cluster is unaffected.
//! - `finalize_and_range`: early Finalize rejected by name (era off; blocker
//!   present), finalize to 2 once every row reports, a node whose range
//!   excludes the new version halts by name.
//! - `release1_to_release2_global_gate` (G-01 stage G-c, the first **real**
//!   gate, `Gate::GlobalTables` at cluster version 2): the whole cluster rolls
//!   `Phase1 -> B2 (= Release(1)) -> Release(2)` under faults; a
//!   `ConvertTableToGlobal` proposed mid-roll is refused at the proposer and
//!   counted (never appended, no capped node ever rejects a delivery); after
//!   the last node the cluster finalizes to 2 and only then is the command
//!   accepted, applied identically by every replica.
//! - `negative_control_global_gate_emitted_early` (N5): `ConvertTableToGlobal`
//!   emitted with the gate check skipped while a `Release(1)` voter exists
//!   wedges exactly that voter; the oracle MUST report it.
//! - `negative_control_premature_era_variant`: N1. A buggy proposer emits an
//!   era variant while a Phase 1 voter exists (bypassing P); the oracle MUST
//!   fail with the exact expected violations.

use std::collections::BTreeSet;
use std::time::Duration;

use animus_control::meta::NodeAddrs;
use animus_control::version::VersionRange;
use animus_control::{MetaCommand, NodeStatus};
use animus_env::{NodeId, nid};

use super::version_world::{Fault, Rng, World, era, records};

const VOTERS: [u64; 3] = [0, 1, 2];
const LEARNER: u64 = 3;
const MEMBERS: [u64; 2] = [10, 11];

/// `(cell name, variants per seed unit)`. The single registry both the seed
/// helper and the self-test read, so a cell cannot silently vanish.
const CELLS: &[(&str, usize)] = &[
    ("roll_orders_with_faults", 12),
    ("kill_phase1_leader", 4),
    ("kill_b2_leader", 4),
    ("kill_leader_around_p", 8),
    ("member_down_blocks_era", 6),
    ("phase1_after_the_era_is_refused", 6),
    ("finalize_and_range", 4),
    ("negative_control_premature_era_variant", 4),
    ("synthetic_gate_ladder", 4),
    ("negative_control_ungated_variant", 2),
    ("negative_control_ungated_field", 2),
    ("negative_control_stale_view", 2),
    ("release1_to_release2_global_gate", 6),
    ("negative_control_global_gate_emitted_early", 2),
];

/// The seeds a cell runs: `ANIMUS_UPGRADE_CELL` (substring) filters cells,
/// `ANIMUS_SEED` replays one seed, else `variants x ANIMUS_UPGRADE_SEEDS`
/// name-derived seeds.
fn cell_seeds(cell: &str) -> Vec<u64> {
    let variants = CELLS
        .iter()
        .find(|(n, _)| *n == cell)
        .unwrap_or_else(|| panic!("unregistered cell {cell}"))
        .1;
    if let Ok(f) = std::env::var("ANIMUS_UPGRADE_CELL")
        && !f.is_empty()
        && !cell.contains(&f)
    {
        return Vec::new();
    }
    if let Some(seed) = std::env::var("ANIMUS_SEED").ok().and_then(|v| {
        v.parse::<u64>()
            .ok()
            .or_else(|| u64::from_str_radix(v.trim_start_matches("0x"), 16).ok())
    }) {
        return vec![seed];
    }
    let k = animus_test::corpus::seeds_from_env("ANIMUS_UPGRADE_SEEDS");
    let mut out = Vec::new();
    animus_test::corpus::for_each_seed(&format!("mixed_version/{cell}"), variants * k, |s| {
        out.push(s)
    });
    out
}

#[test]
fn cell_registry_is_unique_and_each_cell_has_seeds() {
    let names: BTreeSet<_> = CELLS.iter().map(|(n, _)| *n).collect();
    assert_eq!(names.len(), CELLS.len(), "duplicate cell name");
    // Only meaningful when no env narrowing is active (a CI step that sets a
    // filter that matches nothing must not pass green: see the cell tests,
    // which assert their seed list is non-empty unless a filter is set).
    if std::env::var("ANIMUS_UPGRADE_CELL").is_err() && std::env::var("ANIMUS_SEED").is_err() {
        for (n, v) in CELLS {
            assert!(cell_seeds(n).len() >= *v, "cell {n} has no seeds");
        }
    }
}

fn run_cell(cell: &str, mut body: impl FnMut(u64, u64)) {
    let seeds = cell_seeds(cell);
    if std::env::var("ANIMUS_UPGRADE_CELL").is_err() && std::env::var("ANIMUS_SEED").is_err() {
        assert!(!seeds.is_empty(), "cell {cell} selected no seeds");
    }
    for (i, seed) in seeds.into_iter().enumerate() {
        body(seed, i as u64);
    }
}

fn all_ids() -> Vec<u64> {
    VOTERS
        .iter()
        .chain(&[LEARNER])
        .chain(&MEMBERS)
        .copied()
        .collect()
}

fn nid_set(ids: &[u64]) -> BTreeSet<NodeId> {
    ids.iter().copied().map(nid).collect()
}

fn shuffled(rng: &mut Rng, mut v: Vec<u64>) -> Vec<u64> {
    for i in (1..v.len()).rev() {
        let j = rng.below(i as u64 + 1) as usize;
        v.swap(i, j);
    }
    v
}

fn ms(rng: &mut Rng, lo: u64, hi: u64) -> Duration {
    Duration::from_millis(lo + rng.below(hi - lo + 1))
}

fn pick_fault(rng: &mut Rng) -> Fault {
    match rng.below(4) {
        0 => Fault::LeaderKill,
        1 => Fault::PartitionLeader,
        2 => Fault::Lossy,
        _ => Fault::CutMember,
    }
}

/// Instantaneous violations (cheap, every 25 ms).
fn instant_violations(w: &World) -> Vec<String> {
    violations_with(w, true)
}

/// `era_onset_safety = false` once the era is legitimately on and a Phase 1
/// arrival is being tested (that node is required but never flipped).
fn violations_with(w: &World, era_onset_safety: bool) -> Vec<String> {
    let mut v = Vec::new();
    let flipped = nid_set(&w.flipped.iter().copied().collect::<Vec<_>>());
    for (id, n) in &w.nodes {
        let m = n.metadata();
        if era_onset_safety && era(&m) {
            for r in m.required_version_set() {
                if !flipped.contains(&r) {
                    v.push(format!(
                        "era active on node {id} while required node {r:?} is still Phase 1"
                    ));
                }
            }
        }
        if w.profile_of(*id) == animus_control::sim_versions::BinaryProfile::Phase1
            && (era(&m) || m.cluster_version() != 1)
        {
            v.push(format!(
                "Phase 1 node {id}'s applied Metadata shows versioning fields"
            ));
        }
    }
    // gate discipline at apply: a value carrying a synthetic gate/field label
    // `n` is applied only once the cluster version reached `n`.
    for (id, n) in &w.nodes {
        let m = n.metadata();
        for (node, mem) in &m.members {
            for k in [
                animus_control::version::SYNTHETIC_GATE_LABEL,
                animus_control::sim_versions::SYNTHETIC_FIELD_LABEL,
            ] {
                if let Some(need) = mem.labels.get(k).and_then(|x| x.parse::<u32>().ok())
                    && m.cluster_version() < need
                {
                    v.push(format!(
                        "gate applied early: node {id} applied {k}={need} on {node:?} at cluster version {}",
                        m.cluster_version()
                    ));
                }
            }
        }
    }
    for (id, r) in w.cap_rejections() {
        v.push(format!(
            "delivery: node {id} ({:?}) rejected a {:?} message from {:?}",
            r.profile, r.gate, r.from
        ));
    }
    v
}

/// Live replicas whose applied index trails the leader's commit.
fn wedged_replicas(w: &World) -> Vec<u64> {
    let Some(l) = w.leader() else {
        return Vec::new();
    };
    let commit = w.nodes[&l].commit_index();
    w.nodes
        .iter()
        .filter(|(_, n)| n.engine_applied_index() < commit)
        .map(|(&i, _)| i)
        .collect()
}

fn check(w: &World) {
    let v = instant_violations(w);
    assert!(v.is_empty(), "seed={}: {v:#?}", w.seed);
}

/// Every id in `ids` is recorded B2 on every control node; all agree.
fn fully_recorded(w: &World, ids: &[u64]) -> bool {
    let want = nid_set(ids);
    let mut first = None;
    for n in w.nodes.values() {
        let m = n.metadata();
        if !era(&m) {
            return false;
        }
        let r = records(&m);
        if r.keys().cloned().collect::<BTreeSet<_>>() != want
            || !r
                .values()
                .all(|(range, b)| *range == VersionRange::new(1, 1) && b == "b2")
        {
            return false;
        }
        match &first {
            None => first = Some(r),
            Some(f) if *f != r => return false,
            Some(_) => {}
        }
    }
    true
}

/// Final liveness + wedge + delivery verdict for a positive cell.
fn finish(w: &mut World, ids: &[u64]) {
    w.heal_all();
    let mut c = |w: &World| check(w);
    w.poll(
        Duration::from_secs(60),
        "era started and every node recorded",
        &mut c,
        &|w| fully_recorded(w, ids),
    );
    w.poll(
        Duration::from_secs(30),
        "every replica applied what the leader committed",
        &mut c,
        &|w| wedged_replicas(w).is_empty(),
    );
    check(w);
}

fn assert_no_era(w: &World, why: &str) {
    assert!(
        w.nodes.values().all(|n| !era(&n.metadata())),
        "seed={}: era started: {why}",
        w.seed
    );
}

// ---- roll_orders_with_faults ----

fn run_roll(seed: u64, variant: u64) {
    let mut w = World::new_faithful(seed, &VOTERS, &[LEARNER], &MEMBERS);
    w.bootstrap();
    let mut c = |w: &World| check(w);
    w.run(Duration::from_millis(500), &mut c);
    assert_no_era(&w, "at the start");
    let order = match variant % 3 {
        0 => vec![0, 1, 2, 3, 10, 11],
        1 => vec![10, 11, 3, 2, 1, 0],
        _ => shuffled(&mut w.rng.fork(), all_ids()),
    };
    for (n, id) in order.into_iter().enumerate() {
        if n == 2 {
            // The forced mid-roll leader kill, with a Phase 1 and a B2 mix.
            assert!(
                w.flipped.len() == 2 && w.flipped.len() < all_ids().len(),
                "seed={seed}: vacuous mix"
            );
            if let Some(h) = w.inject(Fault::LeaderKill) {
                let d = ms(&mut w.rng, 100, 900);
                w.run(d, &mut c);
                w.heal(h);
            }
        }
        for _ in 0..w.rng.below(3) {
            let f = pick_fault(&mut w.rng);
            if let Some(h) = w.inject(f) {
                let d = ms(&mut w.rng, 100, 900);
                w.run(d, &mut c);
                w.heal(h);
            }
            let d = ms(&mut w.rng, 50, 400);
            w.run(d, &mut c);
        }
        let restart = w.rng.chance(50);
        w.flip(id, restart);
        let d = ms(&mut w.rng, 50, 600);
        w.run(d, &mut c);
    }
    finish(&mut w, &all_ids());
}

#[test]
fn roll_orders_with_faults() {
    run_cell("roll_orders_with_faults", run_roll);
}

// ---- kill_phase1_leader / kill_b2_leader ----

/// Bit 0: a Phase 1 leader was seen; bit 1: a B2 leader was seen.
fn leader_profile_bit(w: &World) -> u8 {
    match w.leader() {
        Some(l) if w.profile_of(l) == animus_control::sim_versions::BinaryProfile::Phase1 => 1,
        Some(_) => 2,
        None => 0,
    }
}

fn run_kill_leader(seed: u64, phase1_leader: bool) {
    let mut w = World::new_faithful(seed, &VOTERS, &[LEARNER], &MEMBERS);
    w.bootstrap();
    w.run(Duration::from_secs(1), &mut |w| check(w));
    let l = w.leader().expect("leader");
    let mut seen = 0u8;
    let mut c = |w: &World| {
        check(w);
        seen |= leader_profile_bit(w);
    };
    if phase1_leader {
        // Everyone but the leader becomes B2 in place (no restart, so the
        // leader stays the leader): a Phase 1 leader with B2 peers.
        for id in all_ids().into_iter().filter(|&i| i != l) {
            w.flip(id, false);
        }
    } else {
        // The leader becomes B2 in place: a B2 leader with Phase 1 peers.
        w.flip(l, false);
    }
    w.run(Duration::from_secs(4), &mut c);
    assert_eq!(w.leader(), Some(l), "seed={seed}: leadership moved");
    assert_no_era(&w, "a leader/peer profile mix");
    // Kill the leader: the other profile takes over, still no era.
    let h = w.inject(Fault::LeaderKill).expect("leader to kill");
    w.run(Duration::from_secs(3), &mut c);
    assert_no_era(&w, "after the leader kill");
    w.heal(h);
    w.run(Duration::from_secs(3), &mut c);
    assert_no_era(&w, "leader back, mix remains");
    // Finish the roll.
    let rest: Vec<u64> = all_ids()
        .into_iter()
        .filter(|id| !w.flipped.contains(id))
        .collect();
    for id in rest {
        let restart = w.rng.chance(50);
        w.flip(id, restart);
        let d = ms(&mut w.rng, 50, 400);
        w.run(d, &mut c);
    }
    let ids = all_ids();
    finish_with(&mut w, &ids, &mut c);
    assert_eq!(
        seen, 3,
        "seed={seed}: the roll must see both a Phase 1 and a B2 leader (mask {seen})"
    );
}

/// [`finish`] reusing a caller's per-tick check.
fn finish_with(w: &mut World, ids: &[u64], c: &mut dyn FnMut(&World)) {
    w.heal_all();
    w.poll(
        Duration::from_secs(60),
        "era started and every node recorded",
        c,
        &|w| fully_recorded(w, ids),
    );
    w.poll(
        Duration::from_secs(30),
        "every replica applied what the leader committed",
        c,
        &|w| wedged_replicas(w).is_empty(),
    );
    check(w);
}

#[test]
fn kill_phase1_leader() {
    run_cell("kill_phase1_leader", |seed, _| run_kill_leader(seed, true));
}

#[test]
fn kill_b2_leader() {
    run_cell("kill_b2_leader", |seed, _| run_kill_leader(seed, false));
}

// ---- kill_leader_around_p ----

fn run_around_p(seed: u64, variant: u64) {
    // P needs one OBSERVATION_WINDOW (300 ms) of held leadership; sweep a kill
    // from "right at the last flip" to well past the window.
    const OFFSETS_MS: [u64; 8] = [0, 40, 120, 200, 280, 330, 450, 700];
    let mut w = World::new_faithful(seed, &VOTERS, &[LEARNER], &MEMBERS);
    w.bootstrap();
    let mut c = |w: &World| check(w);
    w.run(Duration::from_millis(500), &mut c);
    for id in shuffled(&mut w.rng.fork(), all_ids()) {
        w.flip(id, false);
        w.run(Duration::from_millis(30), &mut c);
    }
    w.run(
        Duration::from_millis(OFFSETS_MS[(variant % 8) as usize]),
        &mut c,
    );
    let h = w.inject(Fault::LeaderKill);
    let d = ms(&mut w.rng, 200, 800);
    w.run(d, &mut c);
    if let Some(h) = h {
        w.heal(h);
    }
    finish_with(&mut w, &all_ids(), &mut c);
}

#[test]
fn kill_leader_around_p() {
    run_cell("kill_leader_around_p", run_around_p);
}

// ---- member_down_blocks_era ----

fn run_member_down(seed: u64, variant: u64) {
    let mut w = World::new_faithful(seed, &VOTERS, &[LEARNER], &[10, 11, 12]);
    // Member 12 is registered but its process never runs.
    w.sim.stop(nid(12));
    w.bootstrap();
    let mut c = |w: &World| check(w);
    for id in shuffled(&mut w.rng.fork(), vec![0, 1, 2, 3, 10, 11]) {
        let restart = w.rng.chance(50);
        w.flip(id, restart);
        let d = ms(&mut w.rng, 50, 300);
        w.run(d, &mut c);
    }
    w.run(Duration::from_secs(6), &mut c);
    assert_no_era(&w, "a registered member was never observed");
    let mut ids = vec![0, 1, 2, 3, 10, 11];
    match variant % 3 {
        0 => {
            // Back as a Phase 1 binary: still blocks.
            let env = w.sim.env(nid(12));
            use animus_env::EnvExt;
            env.spawn_task(animus_control::node::heartbeat_loop(
                env.clone(),
                VOTERS.iter().copied().map(nid).collect(),
            ));
            w.run(Duration::from_secs(4), &mut c);
            assert_no_era(&w, "member 12 heartbeats as Phase 1");
            w.flip(12, true);
            ids.push(12);
        }
        1 => {
            w.flip(12, true);
            ids.push(12);
        }
        _ => {
            w.propose_confirmed(
                &MetaCommand::RemoveMember { node: nid(12) },
                &|m| !m.members.contains_key(&nid(12)),
                "removal of the down member",
            );
        }
    }
    finish_with(&mut w, &ids, &mut c);
}

#[test]
fn member_down_blocks_era() {
    run_cell("member_down_blocks_era", run_member_down);
}

// ---- phase1_after_the_era_is_refused ----

fn refusals_at_voters(w: &World) -> u64 {
    VOTERS
        .iter()
        .map(|&v| w.sim.protocol_refusals(&nid(v)))
        .sum()
}

/// A harmless commit that adds no required node: relabel existing member 10.
fn probe(round: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node: nid(10),
        labels: [("probe".to_string(), round.to_string())].into(),
        status: NodeStatus::Active,
    }
}

fn probed(m: &animus_control::Metadata, round: u64) -> bool {
    m.members
        .get(&nid(10))
        .is_some_and(|mem| mem.labels.get("probe") == Some(&round.to_string()))
}

fn run_phase1_after_era(seed: u64, variant: u64) {
    let mut w = World::new_faithful(seed, &VOTERS, &[LEARNER], &MEMBERS);
    w.bootstrap();
    let mut c = |w: &World| check(w);
    for id in all_ids() {
        w.flip(id, false);
    }
    w.poll(
        Duration::from_secs(40),
        "era on with every record",
        &mut c,
        &|w| fully_recorded(w, &all_ids()),
    );
    // From here a Phase 1 arrival is required-but-never-flipped by design.
    let mut c = |w: &World| {
        let v = violations_with(w, false);
        assert!(v.is_empty(), "seed={}: {v:#?}", w.seed);
    };
    let before = refusals_at_voters(&w);
    // The Phase 1 arrival: a brand-new registered member (even variants), or
    // an existing member reinstalled as a Phase 1 binary (odd).
    let victim = if variant.is_multiple_of(2) {
        w.members.push(12);
        w.propose_confirmed(
            &MetaCommand::RegisterNode {
                node: nid(12),
                addrs: NodeAddrs {
                    internal: "127.0.0.1:9312".into(),
                    client: "127.0.0.1:9012".into(),
                    admin: "127.0.0.1:9512".into(),
                    intra: "127.0.0.1:9612".into(),
                    role: "data".into(),
                },
                labels: Default::default(),
            },
            &|m| m.node_addrs.contains_key(&nid(12)),
            "registration of the Phase 1 arrival",
        );
        let env = w.sim.env(nid(12));
        use animus_env::EnvExt;
        env.spawn_task(animus_control::node::heartbeat_loop(
            env.clone(),
            VOTERS.iter().copied().map(nid).collect(),
        ));
        12
    } else {
        w.downgrade_to_phase1(MEMBERS[(variant as usize / 2) % 2]);
        MEMBERS[(variant as usize / 2) % 2]
    };
    w.run(Duration::from_secs(5), &mut c);
    assert!(
        refusals_at_voters(&w) > before,
        "seed={seed}: the Phase 1 node {victim} was never refused"
    );
    // The rest keeps committing.
    for round in 0..3u64 {
        w.propose_confirmed(
            &probe(100 + round),
            &|m| probed(m, 100 + round),
            "a commit with a Phase 1 node refused",
        );
    }
    if variant.is_multiple_of(2) {
        // Never recorded; Finalize is blocked by name until it is removed.
        let m = w.leader_meta().expect("leader");
        assert!(
            !m.node_versions.contains_key(&nid(12)),
            "seed={seed}: refused Phase 1 node was recorded"
        );
        let mut probe = m.clone();
        let out = probe.apply(&MetaCommand::FinalizeClusterVersion {
            expected: 1,
            target: 2,
        });
        // Every recorded node is [1,1] (excludes the target), 12 never
        // reported and (registered, never heard from) is a `Down` member: any
        // named blocker is correct; an Applied is the bug.
        assert!(
            format!("{out:?}").starts_with("Rejected(\"blocked: a "),
            "seed={seed}: {out:?}"
        );
    }
    let v = violations_with(&w, false);
    assert!(v.is_empty(), "seed={seed}: {v:#?}");
    let wedged = wedged_replicas(&w);
    assert!(wedged.is_empty(), "seed={seed}: wedged {wedged:?}");
}

#[test]
fn phase1_after_the_era_is_refused() {
    run_cell("phase1_after_the_era_is_refused", |seed, i| {
        run_phase1_after_era(seed, i)
    });
}

// ---- finalize_and_range ----

fn rejected_by(m: &animus_control::Metadata, cmd: &MetaCommand) -> String {
    let mut probe = m.clone();
    format!("{:?}", probe.apply(cmd))
}

fn run_finalize(seed: u64) {
    const FIN: MetaCommand = MetaCommand::FinalizeClusterVersion {
        expected: 1,
        target: 2,
    };
    let mut w = World::new_faithful(seed, &VOTERS, &[LEARNER], &MEMBERS);
    w.bootstrap();
    let mut c = |w: &World| check(w);
    // Era off: Finalize is rejected by name.
    let m = w.leader_meta().expect("leader");
    assert_eq!(
        rejected_by(&m, &FIN),
        "Rejected(\"version era not active\")",
        "seed={seed}"
    );
    // Everyone is a [1,2] binary except member 11 (a plain B2, [1,1]).
    for id in all_ids() {
        if id == 11 {
            w.flip(id, false);
        } else {
            w.flip_range(id, (1, 2), false);
        }
    }
    w.poll(
        Duration::from_secs(40),
        "era on, every record present",
        &mut c,
        &|w| {
            w.nodes.values().all(|n| {
                let m = n.metadata();
                era(&m) && m.node_versions.len() == all_ids().len()
            })
        },
    );
    // Early Finalize (blocker present): rejected by name; cluster unaffected.
    let m = w.leader_meta().expect("leader");
    assert_eq!(
        rejected_by(&m, &FIN),
        "Rejected(\"blocked: a registered node's range excludes the target version\")",
        "seed={seed}"
    );
    if let Some(l) = w.leader() {
        let _ = w.nodes[&l].propose(FIN);
    }
    w.run(Duration::from_secs(2), &mut c);
    assert!(
        w.nodes
            .values()
            .all(|n| n.metadata().cluster_version() == 1),
        "seed={seed}: an early Finalize changed the cluster version"
    );
    w.propose_confirmed(
        &probe(200),
        &|m| probed(m, 200),
        "a commit after the rejected Finalize",
    );
    // The blocker rolls to [1,2]; Finalize now applies everywhere.
    w.flip_range(11, (1, 2), true);
    w.poll(
        Duration::from_secs(40),
        "the blocker's [1,2] record",
        &mut c,
        &|w| {
            w.nodes.values().all(|n| {
                n.metadata()
                    .node_versions
                    .get(&nid(11))
                    .is_some_and(|v| v.range == VersionRange::new(1, 2))
            })
        },
    );
    w.propose_confirmed(
        &FIN,
        &|m| m.cluster_version() == 2,
        "FinalizeClusterVersion 1 -> 2",
    );
    w.poll(
        Duration::from_secs(30),
        "every node agrees on cluster_version 2",
        &mut c,
        &|w| {
            w.nodes
                .values()
                .all(|n| n.metadata().cluster_version() == 2)
        },
    );
    // Out-of-range binaries halt by name; ranges containing 2 never halt.
    let leader = w.leader().expect("leader");
    let followers: Vec<u64> = VOTERS.iter().copied().filter(|&v| v != leader).collect();
    let (above, below) = (followers[0], followers[1]);
    w.restart_with_own(above, Some(VersionRange::new(1, 1)));
    w.restart_with_own(below, Some(VersionRange::new(3, 4)));
    w.poll(
        Duration::from_secs(30),
        "out-of-range nodes halt",
        &mut c,
        &|w| w.nodes[&above].is_halted() && w.nodes[&below].is_halted(),
    );
    assert_eq!(
        w.nodes[&above].halt_reason().as_deref(),
        Some("cluster version 2 is above this binary's max 1 (downgrade is not supported)"),
        "seed={seed}"
    );
    assert!(
        w.nodes[&below]
            .halt_reason()
            .is_some_and(|r| r.contains("below this binary's min 3")),
        "seed={seed}: {:?}",
        w.nodes[&below].halt_reason()
    );
    assert!(
        !w.nodes[&leader].is_halted(),
        "seed={seed}: an in-range node halted"
    );
    // Delivery: nobody rejected anything in a clean roll.
    let rej = w.cap_rejections();
    assert!(rej.is_empty(), "seed={seed}: {rej:?}");
}

#[test]
fn finalize_and_range() {
    run_cell("finalize_and_range", |seed, _| run_finalize(seed));
}

// ---- N1: the negative control ----

/// A premature era variant while a Phase 1 voter exists: the test acts as the
/// buggy emitter (bypassing precondition P, which lives in the version loop,
/// so no production code is changed). The oracle MUST fail, with the exact
/// expected violations.
fn run_n1(seed: u64) -> bool {
    let mut w = World::new_faithful(seed, &VOTERS, &[], &[]);
    w.bootstrap();
    w.run(Duration::from_millis(500), &mut |_| {});
    // 0 and 1 are B2; 2 is the Phase 1 voter that will be wedged.
    w.flip(0, false);
    w.flip(1, false);
    // Make the leader a B2 node.
    for _ in 0..10 {
        if matches!(w.leader(), Some(l) if l != 2) {
            break;
        }
        if let Some(h) = w.inject(Fault::LeaderKill) {
            w.run(Duration::from_secs(2), &mut |_| {});
            w.heal(h);
            w.run(Duration::from_secs(1), &mut |_| {});
        }
    }
    let l = w.leader().expect("leader");
    assert_ne!(l, 2, "seed={seed}: could not get a B2 leader");
    assert!(
        instant_violations(&w).is_empty(),
        "seed={seed}: violations before the premature emit"
    );
    let pre_log = w.nodes[&2].last_log_index();
    // The buggy emitter.
    for _ in 0..50 {
        let leader = w.leader().expect("leader");
        // Ungated on purpose: the buggy emitter (P2-B's `propose` would
        // refuse a closed-gate era variant and the control would be vacuous).
        let _ =
            w.nodes[&leader].propose_ungated_for_negative_control(MetaCommand::ReportNodeVersion {
                node: nid(0),
                range: VersionRange::new(1, 1),
                build: "b2".into(),
            });
        w.run(Duration::from_millis(100), &mut |_| {});
        if era(&w.nodes[&0].metadata()) {
            break;
        }
    }
    w.run(Duration::from_secs(5), &mut |_| {});
    let v = instant_violations(&w);
    let wedged = wedged_replicas(&w);
    // Exact expected outcome.
    assert_eq!(
        wedged,
        vec![2],
        "seed={seed}: only the Phase 1 replica is wedged"
    );
    // Either the capped decode rejected the batch (the delivery observable),
    // or the era-on dial-side handshake refusal beat it to the Phase 1 node
    // (the leader applied the entry first and stopped dialing): both leave the
    // replica wedged. The caller requires the first to fire on some seed.
    let rejected = v.iter().any(|s| s.starts_with("delivery: node 2 (Phase1)"));
    assert!(
        rejected || w.sim.protocol_refusals(&nid(2)) > 0,
        "seed={seed}: neither the cap nor the handshake kept the era variant from node 2: {v:#?}"
    );
    assert!(
        v.iter()
            .any(|s| s.contains("while required node") && s.contains("is still Phase 1")),
        "seed={seed}: era safety did not trip: {v:#?}"
    );
    assert!(
        !era(&w.nodes[&2].metadata()),
        "seed={seed}: the Phase 1 replica applied the era variant"
    );
    // The cap took the undecodable branch: the batch never entered its log.
    assert_eq!(
        w.nodes[&2].last_log_index(),
        pre_log,
        "seed={seed}: the Phase 1 replica appended the era variant"
    );
    rejected
}

#[test]
fn negative_control_premature_era_variant() {
    let mut fired = 0;
    run_cell("negative_control_premature_era_variant", |seed, _| {
        fired += usize::from(run_n1(seed));
    });
    if std::env::var("ANIMUS_SEED").is_err() && std::env::var("ANIMUS_UPGRADE_CELL").is_err() {
        assert!(
            fired >= 1,
            "the capped decode never rejected the premature era variant"
        );
    }
}

// ---- synthetic_gate_ladder + N2-N4 ----

/// Node 2 is the "previous release" voter: `Release(2)`, range `[1, 2]`.
const OLD: u64 = 2;

/// Roll everything to a range-`[1,3]` binary except `OLD` (`[1,2]`), wait for
/// the era and every record, then finalize to 2 (every range contains 2).
fn ladder_world(seed: u64) -> World {
    let mut w = World::new_faithful(seed, &VOTERS, &[LEARNER], &MEMBERS);
    w.bootstrap();
    let mut c = |w: &World| check(w);
    w.run(Duration::from_millis(500), &mut c);
    for id in all_ids() {
        w.flip_range(id, if id == OLD { (1, 2) } else { (1, 3) }, false);
    }
    w.poll(
        Duration::from_secs(40),
        "era on, every record present",
        &mut c,
        &|w| {
            w.nodes.values().all(|n| {
                let m = n.metadata();
                era(&m) && m.node_versions.len() == all_ids().len()
            })
        },
    );
    w.propose_confirmed(
        &MetaCommand::FinalizeClusterVersion {
            expected: 1,
            target: 2,
        },
        &|m| m.cluster_version() == 2,
        "finalize 1 -> 2",
    );
    w.poll(
        Duration::from_secs(30),
        "every node at cluster version 2",
        &mut c,
        &|w| {
            w.nodes
                .values()
                .all(|n| n.metadata().cluster_version() == 2)
        },
    );
    w
}

fn marked(label: &str, gate: u32) -> MetaCommand {
    MetaCommand::UpsertMember {
        node: nid(10),
        labels: [(label.to_string(), gate.to_string())].into(),
        status: NodeStatus::Active,
    }
}

fn marked_applied(m: &animus_control::Metadata, label: &str, gate: u32) -> bool {
    m.members
        .get(&nid(10))
        .is_some_and(|mem| mem.labels.get(label) == Some(&gate.to_string()))
}

fn run_ladder(seed: u64) {
    use animus_control::version::{Gate, SYNTHETIC_GATE_LABEL};
    let mut w = ladder_world(seed);
    let mut c = |w: &World| check(w);
    // Gate 2 is open (and known to every binary), gate 3 is closed.
    let leader = w.leader().expect("leader");
    let f = w.nodes[&leader].features();
    assert!(f.is_open(Gate::Synthetic(2)) && !f.is_open(Gate::Synthetic(3)));
    w.propose_confirmed(
        &marked(SYNTHETIC_GATE_LABEL, 2),
        &|m| marked_applied(m, SYNTHETIC_GATE_LABEL, 2),
        "a gate-2 command at cluster version 2",
    );
    // Finalize to 3 is blocked by the previous-release node's range, by name.
    let m = w.leader_meta().expect("leader");
    assert_eq!(
        rejected_by(
            &m,
            &MetaCommand::FinalizeClusterVersion {
                expected: 2,
                target: 3,
            }
        ),
        "Rejected(\"blocked: a registered node's range excludes the target version\")",
        "seed={seed}"
    );
    w.run(Duration::from_secs(1), &mut c);
    assert!(
        !w.nodes[&w.leader().expect("leader")]
            .features()
            .is_open(Gate::Synthetic(3)),
        "seed={seed}: gate 3 opened with a [1,2] node recorded"
    );
    // The node rolls to the next release; gate 3 opens at the finalize, not before.
    w.flip_range(OLD, (2, 3), true);
    w.poll(
        Duration::from_secs(40),
        "the rolled node's [2,3] record",
        &mut c,
        &|w| {
            w.nodes.values().all(|n| {
                n.metadata()
                    .node_versions
                    .get(&nid(OLD))
                    .is_some_and(|v| v.range == VersionRange::new(2, 3))
            })
        },
    );
    assert!(
        !w.nodes[&w.leader().expect("leader")]
            .features()
            .is_open(Gate::Synthetic(3)),
        "seed={seed}: gate 3 opened before the finalize"
    );
    w.propose_confirmed(
        &MetaCommand::FinalizeClusterVersion {
            expected: 2,
            target: 3,
        },
        &|m| m.cluster_version() == 3,
        "finalize 2 -> 3",
    );
    w.propose_confirmed(
        &marked(SYNTHETIC_GATE_LABEL, 3),
        &|m| marked_applied(m, SYNTHETIC_GATE_LABEL, 3),
        "a gate-3 command at cluster version 3",
    );
    w.poll(
        Duration::from_secs(30),
        "every replica applied it",
        &mut c,
        &|w| {
            wedged_replicas(w).is_empty()
                && w.nodes
                    .values()
                    .all(|n| marked_applied(&n.metadata(), SYNTHETIC_GATE_LABEL, 3))
        },
    );
    let rej = w.cap_rejections();
    assert!(rej.is_empty(), "seed={seed}: {rej:?}");
    check(&w);
}

#[test]
fn synthetic_gate_ladder() {
    run_cell("synthetic_gate_ladder", |seed, _| run_ladder(seed));
}

#[derive(Clone, Copy)]
enum Neg {
    /// The emitter skips the gate check (`propose_ungated_for_negative_control`).
    UngatedVariant,
    /// A new payload field the classifier does not know: `required_gate` says
    /// `Base`, so the normal `propose` passes, but the older binary's decode
    /// cannot read it.
    UngatedField,
    /// The emitter's feature handle was opened on a view that is not the
    /// replicated state (a forged cluster version 3).
    StaleView,
}

/// At cluster version 2 with a `Release(2)` voter, emit a gate-3 value the
/// wrong way; the oracle MUST report the delivery to the old binary, the
/// wedge, and the early application.
fn run_neg(seed: u64, neg: Neg) {
    use animus_control::sim_versions::SYNTHETIC_FIELD_LABEL;
    use animus_control::version::SYNTHETIC_GATE_LABEL;
    let mut w = ladder_world(seed);
    // A leader that is not the previous-release node.
    for _ in 0..10 {
        if matches!(w.leader(), Some(l) if l != OLD) {
            break;
        }
        if let Some(h) = w.inject(Fault::LeaderKill) {
            w.run(Duration::from_secs(2), &mut |_| {});
            w.heal(h);
            w.run(Duration::from_secs(1), &mut |_| {});
        }
    }
    let l = w.leader().expect("leader");
    assert_ne!(l, OLD, "seed={seed}: could not get a non-old leader");
    assert!(
        instant_violations(&w).is_empty(),
        "seed={seed}: violations before the bad emit"
    );
    let pre_log = w.nodes[&OLD].last_log_index();
    let leader = &w.nodes[&l];
    match neg {
        Neg::UngatedVariant => {
            let _ = leader.propose_ungated_for_negative_control(marked(SYNTHETIC_GATE_LABEL, 3));
        }
        Neg::UngatedField => {
            // Base-classified: the production `propose` accepts it.
            let cmd = marked(SYNTHETIC_FIELD_LABEL, 3);
            assert_eq!(
                animus_control::version::GatedCommand::required_gate(&cmd),
                animus_control::version::Gate::Base
            );
            let _ = leader.propose(cmd);
        }
        Neg::StaleView => {
            let mut forged = w.leader_meta().expect("leader");
            let mut v = forged.node_versions[&nid(OLD)].clone();
            v.range = VersionRange::new(2, 3);
            forged.node_versions.insert(nid(OLD), v);
            let out = forged.apply(&MetaCommand::FinalizeClusterVersion {
                expected: 2,
                target: 3,
            });
            assert_eq!(format!("{out:?}"), "Applied", "seed={seed}: forge failed");
            leader.features().update(&forged);
            let _ = leader.propose(marked(SYNTHETIC_GATE_LABEL, 3));
        }
    }
    w.run(Duration::from_secs(5), &mut |_| {});
    let v = instant_violations(&w);
    assert_eq!(
        wedged_replicas(&w),
        vec![OLD],
        "seed={seed}: only the previous-release replica is wedged"
    );
    assert!(
        v.iter()
            .any(|s| s.starts_with("delivery: node 2 (Release(2))")),
        "seed={seed}: the cap never rejected the gate-3 value: {v:#?}"
    );
    assert!(
        v.iter().any(|s| s.starts_with("gate applied early")),
        "seed={seed}: early application did not trip: {v:#?}"
    );
    assert_eq!(
        w.nodes[&OLD].last_log_index(),
        pre_log,
        "seed={seed}: the old replica appended the gate-3 value"
    );
}

#[test]
fn negative_control_ungated_variant() {
    run_cell("negative_control_ungated_variant", |seed, _| {
        run_neg(seed, Neg::UngatedVariant)
    });
}

#[test]
fn negative_control_ungated_field() {
    run_cell("negative_control_ungated_field", |seed, _| {
        run_neg(seed, Neg::UngatedField)
    });
}

#[test]
fn negative_control_stale_view() {
    run_cell("negative_control_stale_view", |seed, _| {
        run_neg(seed, Neg::StaleView)
    });
}

// ---- release1_to_release2_global_gate + N5 (G-01 stage G-c) ----

const TABLE: &str = "orders";

fn global_spec() -> animus_control::GlobalTableSpec {
    animus_control::GlobalTableSpec {
        consistency: animus_control::MultiRegionConsistency::Strong,
        regions: vec!["a".into(), "b".into(), "c".into()],
        witness: None,
        preferred_leader_region: "a".into(),
    }
}

fn convert_cmd() -> MetaCommand {
    MetaCommand::ConvertTableToGlobal {
        table: TABLE.to_string(),
        spec: global_spec(),
    }
}

/// Every id in `ids` is recorded with exactly `range` on every control node.
fn all_recorded_with(w: &World, ids: &[u64], range: VersionRange) -> bool {
    let want = nid_set(ids);
    w.nodes.values().all(|n| {
        let m = n.metadata();
        let r = records(&m);
        era(&m)
            && r.keys().cloned().collect::<BTreeSet<_>>() == want
            && r.values().all(|(got, _)| *got == range)
    })
}

fn is_global(m: &animus_control::Metadata) -> bool {
    m.schemas
        .get(TABLE)
        .is_some_and(|s| s.global == Some(global_spec()))
}

/// A table with one tablet, created before any version era (both commands are
/// `Gate::Base`), so there is something for `ConvertTableToGlobal` to convert.
fn seed_table(w: &mut World) {
    w.propose_confirmed(
        &MetaCommand::CreateTableSchema {
            table: TABLE.to_string(),
            schema: animus_control::TableSchema::simple("id", animus_control::ColumnType::String),
        },
        &|m| m.schemas.get(TABLE).is_some(),
        "table schema",
    );
    w.propose_confirmed(
        &MetaCommand::CreateTablet {
            tablet: animus_tablet::TabletId(1),
            table: Some(TABLE.to_string()),
            range: animus_tablet::KeyRange::whole(),
            replicas: vec![nid(10), nid(11), nid(0)],
        },
        &|m| m.has_table_tablet(TABLE),
        "table tablet",
    );
}

/// Propose `ConvertTableToGlobal` while the gate is closed, through the
/// production proposer: refused and counted, never appended. (In a debug
/// build the `debug_assert!` in `ClusterFeatures::check` fires first, which is
/// the point of the assert; see `gate_enforcement.rs`.)
fn propose_while_closed(w: &World, seed: u64) {
    propose_cmd_while_closed(w, seed, convert_cmd);
    propose_cmd_while_closed(w, seed, preferred_cmd);
}

/// `SetGlobalPreferredLeader` (same gate as the conversion: one gate per
/// release surface, ADR 0075 plan D1).
fn preferred_cmd() -> MetaCommand {
    MetaCommand::SetGlobalPreferredLeader {
        table: TABLE.to_string(),
        region: "b".to_string(),
    }
}

fn propose_cmd_while_closed(w: &World, seed: u64, cmd: fn() -> MetaCommand) {
    let l = w.leader().expect("leader");
    let before = w.nodes[&l]
        .features()
        .violations(animus_control::version::GateSurface::MetaCommand);
    let last = w.nodes[&l].last_log_index();
    if cfg!(debug_assertions) {
        let r =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| w.nodes[&l].propose(cmd())));
        assert!(r.is_err(), "seed={seed}: a closed gate must debug_assert");
    } else {
        assert!(
            matches!(
                w.nodes[&l].propose(cmd()),
                animus_control::raft::ProposeResult::NotLeader { leader: None }
            ),
            "seed={seed}: a closed gate must refuse"
        );
    }
    assert_eq!(
        w.nodes[&l]
            .features()
            .violations(animus_control::version::GateSurface::MetaCommand),
        before + 1,
        "seed={seed}: the refusal is counted"
    );
    assert_eq!(
        w.nodes[&l].last_log_index(),
        last,
        "seed={seed}: a refused command was appended"
    );
}

fn run_global_gate(seed: u64, variant: u64) {
    use animus_control::version::Gate;
    let mut w = World::new_faithful(seed, &VOTERS, &[LEARNER], &MEMBERS);
    w.bootstrap();
    let mut c = |w: &World| check(w);
    w.run(Duration::from_millis(500), &mut c);
    seed_table(&mut w);

    // Phase 1 -> B2 (= Release(1), range [1, 1]) everywhere; the era starts.
    for id in all_ids() {
        w.flip(id, false);
    }
    let ids = all_ids();
    finish_with(&mut w, &ids, &mut c);
    assert!(
        w.nodes
            .values()
            .all(|n| !n.features().is_open(Gate::GlobalTables)),
        "seed={seed}: the gate is closed at cluster version 1"
    );

    // B2 -> Release(2) (range [1, 2]), in a seed-dependent order, with faults;
    // a mid-roll proposal of the gated command must be refused at the proposer.
    let order = match variant % 3 {
        0 => vec![0, 1, 2, 3, 10, 11],
        1 => vec![10, 11, 3, 2, 1, 0],
        _ => shuffled(&mut w.rng.fork(), all_ids()),
    };
    for (n, id) in order.into_iter().enumerate() {
        if n == 3 {
            // A genuine mix: three Release(2) binaries, three B2 binaries.
            w.run(Duration::from_millis(300), &mut c);
            propose_while_closed(&w, seed);
        }
        for _ in 0..w.rng.below(2) {
            let f = pick_fault(&mut w.rng);
            if let Some(h) = w.inject(f) {
                let d = ms(&mut w.rng, 100, 700);
                w.run(d, &mut c);
                w.heal(h);
            }
            let d = ms(&mut w.rng, 50, 300);
            w.run(d, &mut c);
        }
        let restart = w.rng.chance(50);
        w.flip_range(id, (1, 2), restart);
        let d = ms(&mut w.rng, 50, 500);
        w.run(d, &mut c);
    }
    w.heal_all();
    w.poll(
        Duration::from_secs(60),
        "every node recorded as a [1,2] binary",
        &mut c,
        &|w| all_recorded_with(w, &ids, VersionRange::new(1, 2)),
    );
    // Rolled but not finalized: the gate is still closed.
    let l = w.leader().expect("leader");
    assert!(
        !w.nodes[&l].features().is_open(Gate::GlobalTables),
        "seed={seed}: the gate opened before the finalize"
    );
    propose_while_closed(&w, seed);

    // Finalize 1 -> 2: the gate opens on every node, and only then is the
    // command accepted.
    w.propose_confirmed(
        &MetaCommand::FinalizeClusterVersion {
            expected: 1,
            target: 2,
        },
        &|m| m.cluster_version() == 2,
        "finalize 1 -> 2",
    );
    w.poll(
        Duration::from_secs(30),
        "every node at cluster version 2 with the gate open",
        &mut c,
        &|w| {
            w.nodes.values().all(|n| {
                n.metadata().cluster_version() == 2 && n.features().is_open(Gate::GlobalTables)
            })
        },
    );
    w.propose_confirmed(
        &convert_cmd(),
        &is_global,
        "ConvertTableToGlobal at version 2",
    );
    w.poll(
        Duration::from_secs(30),
        "every replica applied the conversion and agrees",
        &mut c,
        &|w| {
            wedged_replicas(w).is_empty()
                && w.nodes.values().all(|n| {
                    let m = n.metadata();
                    is_global(&m)
                        && m.policies
                            .values()
                            .all(|p| p.is_pinned() && p.allowed_values.len() == 1)
                })
        },
    );
    // The preferred-leader command shares the gate: accepted now.
    w.propose_confirmed(
        &preferred_cmd(),
        &|m| {
            m.schemas
                .get(TABLE)
                .and_then(|s| s.global.as_ref())
                .is_some_and(|g| g.preferred_leader_region == "b")
        },
        "SetGlobalPreferredLeader at version 2",
    );
    w.poll(
        Duration::from_secs(30),
        "every replica applied the preferred-leader change and agrees",
        &mut c,
        &|w| {
            wedged_replicas(w).is_empty()
                && w.nodes.values().all(|n| {
                    n.metadata()
                        .schemas
                        .get(TABLE)
                        .and_then(|s| s.global.as_ref())
                        .is_some_and(|g| g.preferred_leader_region == "b")
                })
        },
    );
    let metas: Vec<_> = w.nodes.values().map(|n| n.metadata()).collect();
    assert!(
        metas
            .windows(2)
            .all(|p| p[0].schemas == p[1].schemas && p[0].policies == p[1].policies),
        "seed={seed}: replicas disagree on the global schema/policy"
    );
    // Delivery: no capped binary ever rejected anything in a clean roll.
    let rej = w.cap_rejections();
    assert!(rej.is_empty(), "seed={seed}: {rej:?}");
    check(&w);
}

#[test]
fn release1_to_release2_global_gate() {
    run_cell("release1_to_release2_global_gate", run_global_gate);
}

/// N5: at cluster version 1 with one `Release(1)` (B2) voter, emit
/// `ConvertTableToGlobal` with the gate check skipped. The oracle MUST report
/// the delivery to the old binary and exactly that replica wedged.
fn run_n5(seed: u64) {
    let mut w = World::new_faithful(seed, &VOTERS, &[LEARNER], &MEMBERS);
    w.bootstrap();
    let mut c = |w: &World| check(w);
    w.run(Duration::from_millis(500), &mut c);
    seed_table(&mut w);
    for id in all_ids() {
        w.flip_range(id, if id == OLD { (1, 1) } else { (1, 2) }, false);
    }
    w.poll(
        Duration::from_secs(40),
        "era on, every record present",
        &mut c,
        &|w| {
            w.nodes.values().all(|n| {
                let m = n.metadata();
                era(&m) && m.node_versions.len() == all_ids().len()
            })
        },
    );
    // A leader that is not the old node (the cluster stays at version 1: the
    // old node's [1,1] range blocks every finalize).
    for _ in 0..10 {
        if matches!(w.leader(), Some(l) if l != OLD) {
            break;
        }
        if let Some(h) = w.inject(Fault::LeaderKill) {
            w.run(Duration::from_secs(2), &mut |_| {});
            w.heal(h);
            w.run(Duration::from_secs(1), &mut |_| {});
        }
    }
    let l = w.leader().expect("leader");
    assert_ne!(l, OLD, "seed={seed}: could not get a non-old leader");
    assert!(
        w.nodes
            .values()
            .all(|n| n.metadata().cluster_version() == 1),
        "seed={seed}: the cluster must still be at version 1"
    );
    assert!(
        instant_violations(&w).is_empty(),
        "seed={seed}: violations before the bad emit"
    );
    let pre_log = w.nodes[&OLD].last_log_index();
    let _ = w.nodes[&l].propose_ungated_for_negative_control(convert_cmd());
    w.run(Duration::from_secs(5), &mut |_| {});
    let v = instant_violations(&w);
    assert_eq!(
        wedged_replicas(&w),
        vec![OLD],
        "seed={seed}: only the Release(1) replica is wedged"
    );
    assert!(
        v.iter()
            .any(|s| s.starts_with("delivery: node 2 (B2) rejected a GlobalTables")),
        "seed={seed}: the cap never rejected the ConvertTableToGlobal entry: {v:#?}"
    );
    assert_eq!(
        w.nodes[&OLD].last_log_index(),
        pre_log,
        "seed={seed}: the old replica appended the gated command"
    );
}

#[test]
fn negative_control_global_gate_emitted_early() {
    run_cell("negative_control_global_gate_emitted_early", |seed, _| {
        run_n5(seed)
    });
}
