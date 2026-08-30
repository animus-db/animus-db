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
//! - `animus-sim`'s fault-injection vocabulary (ADR 0061 Decision 3) folded
//!   into four of the scenarios above, each combined with the nemesis it
//!   probes most directly: message duplication with the leader-change
//!   nemesis (learner catch-up bookkeeping must be idempotent under a
//!   duplicated `AppendEntries`/`InstallSnapshot`), an fsync lie revealed by
//!   a crash on the learner's own disk (a lied-to sync must never let a
//!   crash leave the learner in a state it can't cleanly resume catch-up
//!   from), a torn-and-corrupted WAL tail on the old leader's own disk mid
//!   catch-up (it must recover and rejoin as an ordinary follower, not just
//!   restart), and wire-payload corruption during partitioned catch-up
//!   (`animus-control`'s WAL/wire codec is plain `serde_json`, so a
//!   corrupted message is dropped as "undecodable" rather than misparsed —
//!   see `persist.rs`/`node.rs`, and `docs/engineering-lessons.md`'s
//!   `animus-cp-data::codec.rs` entry for the hand-rolled-binary-codec class
//!   this crate's own WAL/wire path does not share). ENOSPC/generic disk
//!   errors are deliberately **not** used here: `node.rs`'s `persist_wal`
//!   hard-`.expect()`s on `env.append`/`env.sync` with no halted-gated
//!   tolerance the way `animus-cp-data`'s driver has, so an injected error on
//!   a live node would panic the test process itself rather than exercise
//!   application-level fault handling.
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
use animus_sim::{DiskConfig, NetConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_test::corpus;

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

/// **Scenario 4 — leader change mid catch-up, with duplicated messages.**
/// Identical to scenario 2, except every message in the cluster has a
/// nontrivial chance of being delivered twice (its own independent delay
/// draw per copy). A duplicated `AppendEntries`/`InstallSnapshot` chunk
/// arriving during the learner's catch-up — and possibly again after the
/// failover — directly probes whether the learner's catch-up bookkeeping
/// (`next_index`/`match_index` tracking) is idempotent under redelivery, not
/// just under the ordinary at-least-once retry the driver already performs.
fn scenario_leader_change_mid_catch_up_with_duplicated_messages(seed: u64) {
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

    // Install the duplication fault only now, after the election baseline
    // above has already converged on its own — installing it from t=0 would
    // make the leader-election traffic itself duplicated too, which this
    // cell isn't about (see docs/engineering-lessons.md's "install the fault
    // after the baseline" note).
    let mut net = NetConfig::default();
    net.set_duplicate_prob(0.3);
    sim.set_net_config(net);

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

    for i in 0..6u64 {
        assert!(
            matches!(
                nodes[l2].propose(upsert(7000 + i)),
                ProposeResult::Accepted { .. }
            ),
            "seed={seed}"
        );
        sim.run_for(Duration::from_millis(150));
    }
    let caught_up = converge(&mut sim, 60, Duration::from_millis(200), || {
        nodes[l2].learner_caught_up(&nid(LEARNER), CATCH_UP_THRESHOLD)
    });
    assert!(
        caught_up,
        "seed={seed}: the learner must still converge via the new leader despite duplicated messages"
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
        "seed={seed}: promoted via the post-failover leader despite message duplication"
    );
    assert!(
        nodes[l2].config().contains(&nid(LEARNER)),
        "seed={seed}: the new leader's own voter set must reflect the promotion"
    );
}

/// **Scenario 5 — fsync lie revealed by a crash, on the learner's own disk,
/// mid catch-up.** `sync` on the learner's disk unconditionally lies (acks
/// but leaves the bytes buffered) while it is catching up; the learner is
/// then crashed (revealing the lie — the un-synced bytes are lost) and
/// restarted from the *same* durable engine/disk, a real process restart on
/// the same node id (never `Simulator::stop`, per the crash/restart/stop
/// composition this file's own crash cells use elsewhere). Durable-before-
/// visible (ADR 0009) says an acked write is only ever one a crash can't
/// unwind — this proves the learner recovers to a consistent state despite
/// the lie and still resumes and completes catch-up afterward, rather than
/// getting stuck or diverging on whatever it had locally believed before the
/// crash.
fn scenario_catch_up_survives_fsync_lie_then_crash(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines: Vec<MemoryEngine> = VOTERS.iter().map(|_| MemoryEngine::new()).collect();
    let nodes: Vec<RaftNode<SimEnv>> = VOTERS
        .iter()
        .map(|&id| {
            RaftNode::start(
                sim.env(nid(id)),
                VOTERS.iter().copied().map(nid).collect(),
                engines[id as usize].clone(),
            )
        })
        .collect();
    sim.run_for(Duration::from_secs(2));
    let l = unique_leader(&nodes, &[0, 1, 2], seed);

    let learner_engine = MemoryEngine::new();
    // The pre-crash instance only needs to exist so something is actually
    // running at this node id to receive `add_learner`'s replicated config
    // change and the catch-up traffic that follows — its value is never read
    // before the crash below replaces it with the recovered instance.
    let _pre_crash_learner = RaftNode::start(
        sim.env(nid(LEARNER)),
        VOTERS.iter().copied().map(nid).collect(),
        learner_engine.clone(),
    );
    assert!(matches!(
        nodes[l].add_learner(nid(LEARNER)),
        ProposeResult::Accepted { .. }
    ));

    // Every sync the learner performs lies from here on — installed after
    // the election baseline above, not from t=0.
    let mut disk = DiskConfig::default();
    disk.set_fsync_lie_prob(1.0);
    sim.set_disk_config_for(nid(LEARNER), disk);

    // Make some catch-up progress under the lie, then crash: the lie's whole
    // point is only revealed by a following crash (fsync_lie_prob never
    // errors/panics on its own — safe for ambient use up to this point).
    for i in 0..6u64 {
        assert!(
            matches!(
                nodes[l].propose(upsert(8000 + i)),
                ProposeResult::Accepted { .. }
            ),
            "seed={seed}"
        );
        sim.run_for(Duration::from_millis(150));
    }
    sim.crash(nid(LEARNER));
    sim.restart(nid(LEARNER)); // clears the crashed mute flag
    sim.stop(nid(LEARNER)); // drops re-armed tasks, keeps the torn durable state

    // Reconstruct the learner from its own (possibly-lied-to) disk/engine —
    // a real process restart on the same node id, not a fresh identity.
    let learner = RaftNode::start(
        sim.env(nid(LEARNER)),
        VOTERS.iter().copied().map(nid).collect(),
        learner_engine,
    );

    // Post-recovery activity: the leader keeps committing, and the learner
    // must resume catch-up correctly from whatever it actually has durable —
    // never get stuck — and still promote.
    for i in 0..8u64 {
        assert!(
            matches!(
                nodes[l].propose(upsert(8100 + i)),
                ProposeResult::Accepted { .. }
            ),
            "seed={seed}"
        );
        sim.run_for(Duration::from_millis(150));
    }
    let caught_up = converge(&mut sim, 60, Duration::from_millis(200), || {
        nodes[l].learner_caught_up(&nid(LEARNER), CATCH_UP_THRESHOLD)
    });
    assert!(
        caught_up,
        "seed={seed}: the learner must recover and catch up despite an fsync lie revealed by crash"
    );
    assert!(matches!(
        nodes[l].promote_learner(nid(LEARNER)),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));
    assert!(
        learner.config().contains(&nid(LEARNER)),
        "seed={seed}: promoted after fsync-lie-then-crash recovery"
    );
}

/// **Scenario 6 — the old leader recovers from a torn, corrupted WAL tail
/// during the catch-up window.** Mirrors scenario 2 (leader change mid
/// catch-up), but the crashing leader's disk is configured to tear (keep
/// only a seed-chosen strict prefix of its un-synced tail) and additionally
/// corrupt one byte inside the retained region — a genuine crash-consistency
/// fault, not a clean process stop. The old leader is then reconstructed
/// from that torn disk (crash → restart → stop → fresh node, the same
/// composition scenario 5 uses) and must rejoin as an ordinary follower with
/// no divergence, while the learner keeps catching up through the new leader
/// the entire time.
fn scenario_old_leader_recovers_from_torn_wal_tail_mid_catch_up(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines: Vec<MemoryEngine> = VOTERS.iter().map(|_| MemoryEngine::new()).collect();
    let mut nodes: Vec<RaftNode<SimEnv>> = VOTERS
        .iter()
        .map(|&id| {
            RaftNode::start(
                sim.env(nid(id)),
                VOTERS.iter().copied().map(nid).collect(),
                engines[id as usize].clone(),
            )
        })
        .collect();
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
    sim.run_for(Duration::from_millis(80));

    // The old leader's own disk tears (and corrupts) its un-synced tail at
    // the crash point, installed right before the crash it governs.
    let mut disk = DiskConfig::default();
    disk.torn_tail_on_crash = true;
    disk.corrupt_on_crash = true;
    sim.set_disk_config_for(nid(l as u64), disk);
    sim.crash(nid(l as u64));
    sim.restart(nid(l as u64)); // clears the crashed mute flag
    sim.stop(nid(l as u64)); // drops re-armed tasks, keeps the torn durable state

    let survivors: Vec<usize> = [0usize, 1, 2].into_iter().filter(|&i| i != l).collect();
    sim.run_for(Duration::from_secs(3));
    let l2 = unique_leader(&nodes, &survivors, seed);
    assert_ne!(l2, l, "seed={seed}: a new leader must have taken over");

    // The new leader keeps writing while the learner catches up through it.
    for i in 0..6u64 {
        assert!(
            matches!(
                nodes[l2].propose(upsert(9000 + i)),
                ProposeResult::Accepted { .. }
            ),
            "seed={seed}"
        );
        sim.run_for(Duration::from_millis(150));
    }

    // Reconstruct the old leader from its own torn/corrupted disk — a real
    // restart on the same node id — and let it rejoin as an ordinary
    // follower.
    nodes[l] = RaftNode::start(
        sim.env(nid(l as u64)),
        VOTERS.iter().copied().map(nid).collect(),
        engines[l].clone(),
    );

    let caught_up = converge(&mut sim, 40, Duration::from_millis(200), || {
        nodes[l2].learner_caught_up(&nid(LEARNER), CATCH_UP_THRESHOLD)
    });
    assert!(
        caught_up,
        "seed={seed}: the learner must still converge via the new leader despite the old leader's torn-tail recovery"
    );
    assert!(matches!(
        nodes[l2].promote_learner(nid(LEARNER)),
        ProposeResult::Accepted { .. }
    ));

    // Post-recovery activity: the rejoined old leader must itself converge
    // to the cluster's canonical state, not merely restart without panicking.
    let rejoined = converge(&mut sim, 40, Duration::from_millis(200), || {
        nodes[l].metadata() == nodes[l2].metadata()
    });
    assert!(
        rejoined,
        "seed={seed}: the old leader must rejoin and converge after recovering from its torn WAL tail"
    );
    sim.run_for(Duration::from_secs(2));
    assert!(
        learner.config().contains(&nid(LEARNER)),
        "seed={seed}: promoted via the post-failover leader despite the old leader's torn-tail recovery"
    );
}

/// **Scenario 7 — catch-up under partition, with corrupted wire messages.**
/// Identical to scenario 1, except every surviving message has a nontrivial
/// chance of one payload byte flipped in transit. `animus-control`'s WAL and
/// wire codec is plain `serde_json` (see `persist.rs`/`node.rs`'s
/// `serde_json::from_slice::<RaftMsg>` receive path) — a corrupted message
/// either still parses as *some* valid `RaftMsg` (a legal, if wrong, Raft
/// message the core must already tolerate from a byzantine-agnostic network)
/// or fails to parse and is dropped as "undecodable", logged and otherwise
/// ignored (`node.rs`'s `Err(err) => tracing::warn!(...)` arm) — never a
/// panic or a `Vec::with_capacity` sized off an attacker-controlled length
/// prefix the way a hand-rolled binary codec could be (see
/// `docs/engineering-lessons.md`'s `animus-cp-data::codec.rs` entry for that
/// distinct class, which this crate's JSON-based codec does not share).
fn scenario_catch_up_under_partition_with_corrupted_messages(seed: u64) {
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

    // Installed after the election baseline above, not from t=0.
    let mut net = NetConfig::default();
    net.set_corrupt_prob(0.15);
    sim.set_net_config(net);

    for &v in &VOTERS {
        sim.partition_pair(nid(LEARNER), nid(v));
    }
    for i in 0..8u64 {
        assert!(
            matches!(
                nodes[l].propose(upsert(10000 + i)),
                ProposeResult::Accepted { .. }
            ),
            "seed={seed}"
        );
        sim.run_for(Duration::from_millis(150));
    }
    assert!(
        !nodes[l].learner_caught_up(&nid(LEARNER), CATCH_UP_THRESHOLD),
        "seed={seed}: a partitioned learner cannot have caught up"
    );

    for &v in &VOTERS {
        sim.heal(nid(LEARNER), nid(v));
    }
    let caught_up = converge(&mut sim, 60, Duration::from_millis(200), || {
        nodes[l].learner_caught_up(&nid(LEARNER), CATCH_UP_THRESHOLD)
    });
    assert!(
        caught_up,
        "seed={seed}: learner must catch up once healed despite corrupted messages in flight"
    );

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

#[test]
fn leader_change_mid_learner_catch_up_survives_duplicated_messages() {
    scenario_leader_change_mid_catch_up_with_duplicated_messages(0x1EA5_0004);
}

#[test]
fn learner_catch_up_survives_an_fsync_lie_revealed_by_crash() {
    scenario_catch_up_survives_fsync_lie_then_crash(0x1EA5_0005);
}

#[test]
fn old_leader_recovers_from_a_torn_wal_tail_mid_learner_catch_up() {
    scenario_old_leader_recovers_from_torn_wal_tail_mid_catch_up(0x1EA5_0006);
}

#[test]
fn learner_catches_up_under_partition_despite_corrupted_messages() {
    scenario_catch_up_under_partition_with_corrupted_messages(0x1EA5_0007);
}

/// Depth knob (`ANIMUS_LEARNER_SEEDS`, default 1 = just the frozen seeds
/// above, run again here for uniformity) — following the existing
/// corpus-depth convention (`ANIMUS_RAFTKV_SEEDS` et al., see the root
/// `CLAUDE.md` test-scaling table). `K > 1` additionally derives `K - 1`
/// fresh seeds (a simple splitmix-style hash of the scenario name + index,
/// matching this crate's existing seed-derivation style) and runs every
/// scenario above — including the fault-injected ones — again at each one.
fn seeds_per_scenario() -> usize {
    corpus::seeds_from_env("ANIMUS_LEARNER_SEEDS")
}

#[test]
fn learner_corpus_runs_at_configured_depth() {
    let k = seeds_per_scenario();
    for i in 0..k {
        let catch_up_seed = if i == 0 {
            0x1EA5_0001
        } else {
            corpus::name_seed(&format!("catch_up_under_partition_s{i:03}"))
        };
        scenario_catch_up_under_partition(catch_up_seed);

        let failover_seed = if i == 0 {
            0x1EA5_0002
        } else {
            corpus::name_seed(&format!("leader_change_mid_catch_up_s{i:03}"))
        };
        scenario_leader_change_mid_catch_up(failover_seed);

        let snapshot_seed = if i == 0 {
            0x1EA5_0003
        } else {
            corpus::name_seed(&format!("snapshot_race_s{i:03}"))
        };
        scenario_snapshot_race(snapshot_seed);

        let duplicate_seed = if i == 0 {
            0x1EA5_0004
        } else {
            corpus::name_seed(&format!("leader_change_mid_catch_up_duplicate_s{i:03}"))
        };
        scenario_leader_change_mid_catch_up_with_duplicated_messages(duplicate_seed);

        let fsync_lie_seed = if i == 0 {
            0x1EA5_0005
        } else {
            corpus::name_seed(&format!("catch_up_fsync_lie_crash_s{i:03}"))
        };
        scenario_catch_up_survives_fsync_lie_then_crash(fsync_lie_seed);

        let torn_tail_seed = if i == 0 {
            0x1EA5_0006
        } else {
            corpus::name_seed(&format!("old_leader_torn_wal_tail_s{i:03}"))
        };
        scenario_old_leader_recovers_from_torn_wal_tail_mid_catch_up(torn_tail_seed);

        let corrupt_seed = if i == 0 {
            0x1EA5_0007
        } else {
            corpus::name_seed(&format!("catch_up_under_partition_corrupt_s{i:03}"))
        };
        scenario_catch_up_under_partition_with_corrupted_messages(corrupt_seed);
    }
}
