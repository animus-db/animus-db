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
        // Every recorded node is [1,1] (excludes the target) and 12 never
        // reported: either named blocker is correct; an Applied is the bug.
        assert!(
            format!("{out:?}").starts_with("Rejected(\"blocked: a registered node"),
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
        let _ = w.nodes[&leader].propose(MetaCommand::ReportNodeVersion {
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
