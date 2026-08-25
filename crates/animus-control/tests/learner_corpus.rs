//! Fault-injected, seed-reproducible corpus for the **learner** membership
//! class (ADR 0058 Train 1), per the ADR's "Testing plan (Train 1)":
//!
//! - learner catch-up under partition (falls behind, reconnects, still
//!   promotes correctly);
//! - a leader change while a learner is mid-catch-up (the new leader must
//!   inherit/re-derive the learner's `match_index` bookkeeping and keep
//!   replicating to it);
//! - a snapshot race (a learner receiving `InstallSnapshot` while the leader
//!   concurrently commits further entries);
//! - the depth-scaling knob (`ANIMUS_LEARNER_SEEDS`), following the existing
//!   corpus-depth convention (`ANIMUS_RAFTKV_SEEDS` et al.) — each named
//!   scenario below runs once at its canonical (frozen) seed by default, and
//!   `ANIMUS_LEARNER_SEEDS=K` additionally sweeps the partition/leader-change
//!   scenario across `K` fresh, name-derived seeds.
//!
//! The core structural safety property — **a learner never appears in any
//! majority computation, and its liveness or death never flips a commit or
//! election outcome** — is proven directly (not just by absence-of-failure)
//! in `learner_membership.rs`'s
//! `a_dead_learner_never_blocks_commit_or_election_a_live_learner_never_helps_either`;
//! every scenario here additionally re-asserts it opportunistically wherever
//! it's cheap to (the learner's own `config()` never contains it before an
//! explicit promotion).

use std::collections::BTreeSet;
use std::time::Duration;

use animus_control::{MetaCommand, NodeStatus, ProposeResult, RaftNode};
use animus_env::{NodeId, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

const VOTERS: [u64; 3] = [0, 1, 2];
const LEARNER: u64 = 3;
/// Promotion-criterion threshold used throughout this corpus: "caught up"
/// means within this many log entries of the leader's own tip.
const CATCH_UP_THRESHOLD: u64 = 4;

fn set(ids: &[u64]) -> BTreeSet<NodeId> {
    ids.iter().copied().map(nid).collect()
}

fn upsert(node: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node: nid(node),
        labels: std::collections::BTreeMap::new(),
        status: NodeStatus::Active,
    }
}

fn cluster(seed: u64, ids: &[u64]) -> (Simulator, Vec<RaftNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes = ids
        .iter()
        .map(|&id| {
            RaftNode::start(
                sim.env(nid(id)),
                ids.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    (sim, nodes)
}

fn unique_leader(nodes: &[RaftNode<SimEnv>], live: &[usize], seed: u64) -> usize {
    let leaders: Vec<usize> = live
        .iter()
        .copied()
        .filter(|&i| nodes[i].is_leader())
        .collect();
    assert_eq!(
        leaders.len(),
        1,
        "expected exactly one leader among {live:?}, found {leaders:?} (seed={seed})"
    );
    leaders[0]
}

/// Poll-to-convergence (never a fixed-deadline one-shot assert, per the
/// repo's testing lessons): keep proposing nothing and just running short
/// slices of virtual time until `pred` holds or `attempts` are exhausted.
fn converge(
    sim: &mut Simulator,
    attempts: u32,
    slice: Duration,
    mut pred: impl FnMut() -> bool,
) -> bool {
    for _ in 0..attempts {
        if pred() {
            return true;
        }
        sim.run_for(slice);
    }
    pred()
}

/// **Scenario 1 — learner catch-up under partition.** The learner is added,
/// immediately partitioned from every voter, falls behind while writes keep
/// landing, reconnects, and must still promote correctly once caught up.
fn scenario_catch_up_under_partition(seed: u64) {
    let (mut sim, nodes) = cluster(seed, &VOTERS);
    sim.run_for(Duration::from_secs(2));
    let l = unique_leader(&nodes, &[0, 1, 2], seed);

    let learner = RaftNode::start(
        sim.env(nid(LEARNER)),
        VOTERS.iter().copied().map(nid).collect(),
        MemoryEngine::new(),
    );
    assert!(
        matches!(
            nodes[l].add_learner(nid(LEARNER)),
            ProposeResult::Accepted { .. }
        ),
        "seed={seed}"
    );

    // Partition it away before it catches up at all.
    for &v in &VOTERS {
        sim.partition_pair(nid(LEARNER), nid(v));
    }
    for i in 0..8u64 {
        assert!(
            matches!(
                nodes[l].propose(upsert(1000 + i)),
                ProposeResult::Accepted { .. }
            ),
            "seed={seed}"
        );
        sim.run_for(Duration::from_millis(150));
    }
    assert_eq!(
        learner.config(),
        set(&VOTERS),
        "seed={seed}: still not a voter while partitioned"
    );
    // The leader-side view is the one that matters for the promotion
    // criterion; a partitioned learner cannot possibly have caught up.
    assert!(
        !nodes[l].learner_caught_up(&nid(LEARNER), CATCH_UP_THRESHOLD),
        "seed={seed}: a partitioned learner cannot have caught up"
    );

    // Heal and let it catch up.
    for &v in &VOTERS {
        sim.heal(nid(LEARNER), nid(v));
    }
    let caught_up = converge(&mut sim, 40, Duration::from_millis(200), || {
        nodes[l].learner_caught_up(&nid(LEARNER), CATCH_UP_THRESHOLD)
    });
    assert!(caught_up, "seed={seed}: learner must catch up once healed");

    assert!(
        matches!(
            nodes[l].promote_learner(nid(LEARNER)),
            ProposeResult::Accepted { .. }
        ),
        "seed={seed}: promotion must succeed once caught up"
    );
    sim.run_for(Duration::from_secs(2));
    assert!(
        learner.config().contains(&nid(LEARNER)),
        "seed={seed}: the learner itself must have adopted its own promotion"
    );
    for &i in &[0usize, 1, 2] {
        assert_eq!(
            nodes[i].config(),
            set(&[0, 1, 2, LEARNER]),
            "seed={seed}: node {i} must see the promoted voter set"
        );
    }
}

/// **Scenario 2 — leader change while a learner is mid-catch-up.** The
/// leader that added the learner crashes before the learner is caught up; a
/// new leader (elected from the surviving **voters only** — the learner must
/// never be a candidate) must pick up replicating to the learner and let it
/// eventually promote.
fn scenario_leader_change_mid_catch_up(seed: u64) {
    let (mut sim, nodes) = cluster(seed, &VOTERS);
    sim.run_for(Duration::from_secs(2));
    let l = unique_leader(&nodes, &[0, 1, 2], seed);

    let learner = RaftNode::start(
        sim.env(nid(LEARNER)),
        VOTERS.iter().copied().map(nid).collect(),
        MemoryEngine::new(),
    );
    assert!(matches!(
        nodes[l].add_learner(nid(LEARNER)),
        ProposeResult::Accepted { .. }
    ));
    // A couple of ticks so the learner starts, but not enough to converge —
    // then the leader dies mid-flight.
    sim.run_for(Duration::from_millis(80));
    sim.crash(nid(l as u64));

    let survivors: Vec<usize> = [0usize, 1, 2].into_iter().filter(|&i| i != l).collect();
    sim.run_for(Duration::from_secs(3));
    let l2 = unique_leader(&nodes, &survivors, seed);
    assert_ne!(l2, l, "seed={seed}: a new leader must have taken over");
    assert!(
        !learner.is_leader(),
        "seed={seed}: the learner must never win the election its old leader's death triggers"
    );

    // The new leader keeps writing; the learner must keep catching up
    // through it (not stall just because its original leader is gone).
    for i in 0..6u64 {
        assert!(
            matches!(
                nodes[l2].propose(upsert(2000 + i)),
                ProposeResult::Accepted { .. }
            ),
            "seed={seed}"
        );
        sim.run_for(Duration::from_millis(150));
    }
    let caught_up = converge(&mut sim, 40, Duration::from_millis(200), || {
        nodes[l2].learner_caught_up(&nid(LEARNER), CATCH_UP_THRESHOLD)
    });
    assert!(
        caught_up,
        "seed={seed}: the learner must still converge via the new leader"
    );
    assert!(
        matches!(
            nodes[l2].promote_learner(nid(LEARNER)),
            ProposeResult::Accepted { .. }
        ),
        "seed={seed}"
    );
    sim.run_for(Duration::from_secs(2));
    assert!(
        learner.config().contains(&nid(LEARNER)),
        "seed={seed}: promoted via the post-failover leader"
    );
}

/// **Scenario 3 — snapshot race.** The leader has already compacted past
/// `SNAPSHOT_THRESHOLD` entries by the time the learner joins (so it must
/// catch up via `InstallSnapshot`, not a plain log replay), and the leader
/// keeps committing fresh entries *while* that transfer is in flight — the
/// exact race the ADR names: "a learner receiving `InstallSnapshot` while
/// the leader concurrently commits further entries."
fn scenario_snapshot_race(seed: u64) {
    let (mut sim, nodes) = cluster(seed, &VOTERS);
    sim.run_for(Duration::from_secs(2));
    let l = unique_leader(&nodes, &[0, 1, 2], seed);

    // Push the leader's log well past `SNAPSHOT_THRESHOLD` (64) so it
    // compacts before the learner ever joins — the learner's first catch-up
    // message can only be an `InstallSnapshot`.
    for i in 0..90u64 {
        assert!(matches!(
            nodes[l].propose(upsert(3000 + i)),
            ProposeResult::Accepted { .. }
        ));
    }
    sim.run_for(Duration::from_secs(3));
    assert!(
        nodes[l].snapshot_index() > 0,
        "seed={seed}: the leader must have compacted by now"
    );

    let learner = RaftNode::start(
        sim.env(nid(LEARNER)),
        VOTERS.iter().copied().map(nid).collect(),
        MemoryEngine::new(),
    );
    assert!(matches!(
        nodes[l].add_learner(nid(LEARNER)),
        ProposeResult::Accepted { .. }
    ));

    // Immediately race more commits against the in-flight
    // snapshot-then-catch-up: a handful of short ticks, interleaved with
    // fresh proposals, so at least one lands mid-transfer.
    for i in 0..15u64 {
        sim.run_for(Duration::from_millis(30));
        assert!(matches!(
            nodes[l].propose(upsert(4000 + i)),
            ProposeResult::Accepted { .. }
        ));
    }

    let converged = converge(&mut sim, 60, Duration::from_millis(200), || {
        learner.metadata() == nodes[l].metadata()
    });
    assert!(
        converged,
        "seed={seed}: the learner must converge to the leader's full state \
         despite the interleaved snapshot + fresh commits"
    );

    let caught_up = converge(&mut sim, 20, Duration::from_millis(200), || {
        nodes[l].learner_caught_up(&nid(LEARNER), CATCH_UP_THRESHOLD)
    });
    assert!(caught_up, "seed={seed}");
    assert!(matches!(
        nodes[l].promote_learner(nid(LEARNER)),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));
    assert!(learner.config().contains(&nid(LEARNER)), "seed={seed}");
}

#[test]
fn learner_catches_up_under_partition_then_promotes() {
    scenario_catch_up_under_partition(0x1EA5_0001);
}

#[test]
fn leader_change_mid_learner_catch_up_still_converges_and_promotes() {
    scenario_leader_change_mid_catch_up(0x1EA5_0002);
}

#[test]
fn learner_survives_a_snapshot_race_with_concurrent_commits() {
    scenario_snapshot_race(0x1EA5_0003);
}

/// Depth knob (`ANIMUS_LEARNER_SEEDS`, default 1 = just the two frozen
/// partition/leader-change seeds above, run again here for uniformity) —
/// following the existing corpus-depth convention (`ANIMUS_RAFTKV_SEEDS` et
/// al., see the root `CLAUDE.md` test-scaling table). `K > 1` additionally
/// derives `K - 1` fresh seeds (a simple splitmix-style hash of the scenario
/// name + index, matching this crate's existing seed-derivation style) and
/// runs both fault scenarios again at each one.
fn seeds_per_scenario() -> usize {
    std::env::var("ANIMUS_LEARNER_SEEDS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1)
        .max(1)
}

fn name_seed(name: &str) -> u64 {
    // A small, fixed, deterministic string hash (FNV-1a) — no RNG, no
    // dependency on hash-map iteration order, matching this repo's existing
    // "seed derived from a scenario's own name" convention.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in name.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
    }
    h
}

#[test]
fn learner_corpus_runs_at_configured_depth() {
    let k = seeds_per_scenario();
    for i in 0..k {
        let catch_up_seed = if i == 0 {
            0x1EA5_0001
        } else {
            name_seed(&format!("catch_up_under_partition_s{i:03}"))
        };
        scenario_catch_up_under_partition(catch_up_seed);

        let failover_seed = if i == 0 {
            0x1EA5_0002
        } else {
            name_seed(&format!("leader_change_mid_catch_up_s{i:03}"))
        };
        scenario_leader_change_mid_catch_up(failover_seed);

        let snapshot_seed = if i == 0 {
            0x1EA5_0003
        } else {
            name_seed(&format!("snapshot_race_s{i:03}"))
        };
        scenario_snapshot_race(snapshot_seed);
    }
}
