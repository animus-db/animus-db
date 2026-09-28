//! Regression for issue #1064's own heavier-load finding (the second,
//! separate defect the `directed_placing_under_sustained_load.rs` module
//! doc names but deliberately does not exercise): even with
//! `RaftCore::learner_caught_up`'s baseline-metric fix in place, a joining
//! learner that needs a real, multi-chunk `InstallSnapshot` can still never
//! converge under a genuinely sustained writer, because
//! `RaftCore::snapshot_upto` unconditionally invalidates any in-flight
//! transfer the moment the snapshot base moves again, and
//! `apply_and_compact`'s own `COMPACT_DEFER_EMERGENCY_CEILING` — a
//! WAL-retention safety valve sized in LOG ENTRIES — used to force that
//! invalidation regardless of whether the transfer was genuinely
//! progressing, the instant `behind` (which grows at the WRITE rate, not at
//! the transfer's own completion rate) crossed it.
//!
//! **Why this file uses `RaftKvNode::set_compact_tuning_for_test` instead of
//! the compiled-in production ceiling (4096 entries).** Reaching
//! `COMPACT_DEFER_EMERGENCY_CEILING` at its real production size could not
//! be done by an unambiguous margin within a real-time-affordable step
//! budget. Since virtual time inside `SimEnv` is free and only the number of
//! *steps* costs anything, the fix is not a slower test, but a **smaller
//! ceiling** — this file overrides it to 200 entries (`TEST_EMERGENCY_
//! CEILING`, within the coordinator-suggested 128–256 range) via the
//! test-only seam `CompactTuning` (`crates/animus-cp-data/src/lib.rs`)
//! added alongside this test. See `docs/lessons/testing/2026-09-28-reach-
//! the-real-trigger-dont-shrink-the-test-around-a-fixed-constant.md` for
//! the full account of getting here, including a write-rate-vs-throughput
//! lesson this file's own control scenario caught along the way.
//!
//! **Why writes are issued in BURSTS between `Simulator::run_for` calls,
//! never one `run_for` per single propose** — `learner_catchup_under_
//! load.rs`'s own module doc already profiled this exact tradeoff: a
//! `run_for` call's real wall-clock cost scales with how much work it has
//! to process (message/event volume), not with the virtual duration
//! requested — so a design that produces a large backlog per call (one
//! propose, one `run_for`, repeated thousands of times) is far more
//! expensive in real time than the identical aggregate write volume issued
//! in synchronous bursts (which also lets `replicate_now`'s wake-on-propose
//! coalesce a burst into one physical `AppendEntries`, rather than one per
//! write).
//!
//! **What this file actually proves, and what it does NOT (found through
//! extensive empirical tuning — read before changing any constant here).**
//! An earlier design tried to prove the learner reaches, and then SUSTAINS,
//! `RaftCore::learner_caught_up`'s tight absolute-gap bound
//! (`RECONFIGURE_LEARNER_CATCH_UP_THRESHOLD` = 4 entries) while the writer
//! never stops. That turned out to be a materially harder, and separately
//! confounded, claim: even the AppendEntries-only CONTROL (no snapshot
//! involved at all) could not sustain a 4-entry gap against a genuinely
//! continuous high-volume writer — every burst instantaneously reopens a
//! `BURST_LEN`-sized gap, and `RaftCore::compaction_floor`'s own
//! voters-only design (deliberate, see `animus-control/CLAUDE.md`) means
//! nothing holds a learner's position the way it holds a lagging voter's.
//! Chasing a literal `learner_caught_up` streak long enough eventually ran
//! into a **third**, unrelated, already-flagged-for-separate-filing defect
//! (`handle_append_resp`'s ordinary-ack `next_index` update via a bare
//! `insert` rather than a monotonic `max` — found investigating this exact
//! issue, explicitly left alone per this session's own instructions) at
//! long enough run lengths, which can silently stop a leader from ever
//! replicating anything further to a peer at all. **This file therefore
//! proves the actual mechanism under test — whether the LEARNER's transfer
//! is allowed to land and make real progress, or is forced back to chunk 0
//! forever — directly, via `Metric::CpSnapshotInstalls`/
//! `CpSnapshotTransferRestarts`/`RaftKvNode::engine_applied_index`, at a
//! bounded run length chosen to stay well clear of that third, unrelated
//! defect** — see `assert_late_join_converges_while_writing`'s own
//! assertions and the measured before/after numbers below, rather than
//! requiring the much stronger (and, at this scale, currently unachievable
//! for reasons unrelated to this fix) "sustained tight-threshold catch-up"
//! claim.
//!
//! Two scenarios, same write rate and learner disk cost throughout (the
//! control proves the rate itself is not the problem):
//! 1. `an_appendentries_only_learner_converges_issue_1064_control`: a
//!    learner that joins BEFORE any compaction has happened, so it starts
//!    fully caught up — proves the chosen rate/disk-delay combination is a
//!    load the learner can genuinely sustain, not a capacity mismatch
//!    dressed up as this bug (a small, fixed tolerance on `Metric::
//!    CpSnapshotImageBuilds`/`CpSnapshotTransferRestarts`, not a strict
//!    zero — even a healthy replica can need one early on-demand image
//!    before its own steady-state rhythm settles).
//! 2. `a_late_joining_learner_converges_despite_needing_a_multi_chunk_
//!    install_snapshot_issue_1064`: a learner that joins only after the
//!    leader has already compacted a warmed-up log, needing a real,
//!    multi-round-trip `InstallSnapshot` the instant it joins, under the
//!    SAME sustained writer as the control, with `COMPACT_DEFER_EMERGENCY_
//!    CEILING` overridden down to a value the sustained writer crosses many
//!    times before a transfer can naturally land.
//!
//! **Confirmed red on the pre-fix mechanism, green on the fix (5c3ead84),
//! measured live at this file's exact seed and constants (150 bursts, 300ms
//! virtual apart)**: temporarily reverting `emergency_ceiling_hit`'s own
//! `!learner_transfer_in_flight` exemption in `lib.rs` (i.e. restoring the
//! pre-fix `behind >= compact_emergency_ceiling` alone) turns this scenario
//! from `restarts=6, installs=2 (successful), learner's own applied index
//! reaching 11914 of the leader's 17502 commits — genuine, substantial,
//! ongoing progress` into `restarts=75, installs=0 (the learner NEVER
//! completes a single InstallSnapshot, ever), learner's own applied index
//! stuck at 0 for the entire run` — a livelock matching the issue's own
//! description exactly, not a mere slowdown. `MAX_TOLERATED_RESTARTS` (15)
//! and `MIN_LEARNER_PROGRESS_PERCENT` (below) are both chosen with generous
//! margin around the FIXED code's own measured 6 restarts / 68% progress,
//! while remaining far below the pre-fix code's measured 75 restarts / 0%
//! progress.

use std::time::{Duration, Instant};

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{Metric, MetricsHandle, NodeId, nid};
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_test::corpus;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn leader_among(nodes: &[KvNode]) -> Option<usize> {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    if ls.len() == 1 { Some(ls[0]) } else { None }
}

/// The test-only override for `COMPACT_DEFER_EMERGENCY_CEILING` (production:
/// 4096) — see the module doc for why a small override, not a slower test,
/// is what makes this file's own writer genuinely cross it many times while
/// a real multi-chunk transfer is in flight. Within the coordinator-
/// suggested 128–256 range. `COMPACT_THRESHOLD` itself is left at its
/// production default (64).
const TEST_EMERGENCY_CEILING: u64 = 200;

/// The learner's own disk round-trip cost — one simulated fsync per
/// RECEIVED message, however many entries that message carries (batching
/// amortizes this the same way a real WAL does).
const LEARNER_SYNC_DELAY: Duration = Duration::from_millis(20);

/// Writes issued synchronously (no yield) per warm-up burst, and the
/// virtual gap after each burst before the next. Building a log large
/// enough to need several `SNAPSHOT_CHUNK_BYTES` (64 KiB) chunks needs
/// several thousand small rows; batching them into a handful of bursts (not
/// thousands of individual `run_for` calls) is what keeps this cheap in
/// real time — see the module doc.
const WARMUP_BURST_LEN: u64 = 250;
const WARMUP_BURST_GAP: Duration = Duration::from_millis(50);
const WARMUP_BURSTS: u64 = 10;

/// The sustained (bursted) write phase: `BURST_LEN` writes issued
/// synchronously, then `BURST_GAP` of virtual time, repeated `ROUNDS`
/// times. Picked (with `LEARNER_SYNC_DELAY`) as a matched pair: the message
/// rate (one coalesced `AppendEntries`/chunk-worth per burst) stays well
/// under the learner's own per-message disk cost, so the CONTROL scenario's
/// own image-build/restart counts stay small — while the aggregate write
/// volume is still high enough to cross `TEST_EMERGENCY_CEILING` many times
/// over during the regression scenario's transfer.
const BURST_LEN: u64 = 100;
const BURST_GAP: Duration = Duration::from_millis(100);
/// Bounded to stay well clear of a third, unrelated, already-flagged
/// defect (`handle_append_resp`'s non-monotonic `next_index` update — see
/// the module doc) that a much longer sustained run can eventually trip
/// regardless of this fix, silently halting ALL further replication to a
/// peer. 150 bursts (15 virtual seconds) was confirmed clear of it at this
/// file's exact seed/constants, both with and without this fix in place —
/// long enough to show a dramatic, unambiguous restart-count and progress
/// difference between the two (see the module doc's own measured numbers).
const ROUNDS: u64 = 150;

/// A generous real-time watchdog, mirroring every other continuous-writer
/// test in this crate — a deliberate, narrow exception to the
/// `Instant::now`/`SimEnv`-clock discipline (see
/// `learner_catchup_under_load.rs`'s own identical allow for why).
#[allow(
    clippy::disallowed_methods,
    reason = "real-time watchdog against unbounded per-round CPU work — SimEnv's virtual clock cannot see this"
)]
fn real_budget() -> Duration {
    Duration::from_secs(60)
}

/// Runs a 3-voter group through the sustained, bursted write phase above,
/// joining a learner (its own disk throttled) either immediately
/// (`warmup_bursts == 0`, the AppendEntries-only control) or after
/// `warmup_bursts` warm-up bursts of prior writes (the actual #1064
/// regression shape). The writer never stops before every metric below is
/// read — see the module doc. Returns `(restarts, image_builds, installs,
/// learner_applied, leader_commit)`.
fn run_scenario(seed: u64, warmup_bursts: u64) -> (u64, u64, u64, u64, u64) {
    let mut sim = Simulator::new(seed);
    let ids = [0u64, 1, 2];
    let handles: Vec<MetricsHandle> = ids.iter().map(|_| MetricsHandle::recording()).collect();
    let nodes: Vec<KvNode> = ids
        .iter()
        .enumerate()
        .map(|(i, &id)| {
            RaftKvNode::start_with_metrics(
                sim.env(nid(id)),
                ids.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
                handles[i].clone(),
            )
        })
        .collect();
    // Every replica compacts locally — apply to all three so leadership
    // changes (none expected in this fault-free scenario, but cheap
    // insurance) never fall back to the un-overridden production ceiling.
    for node in &nodes {
        node.set_compact_tuning_for_test(None, Some(TEST_EMERGENCY_CEILING));
    }
    sim.run_for(Duration::from_secs(2));
    let l = leader_among(&nodes).expect("an initial leader");

    for b in 0..warmup_bursts {
        for i in 0..WARMUP_BURST_LEN {
            let key = format!("warm-{b}-{i}").into_bytes();
            assert!(
                matches!(
                    nodes[l].put(key, vec![b'v'; 256]),
                    ProposeResult::Accepted { .. }
                ),
                "seed={seed:#x}: warm-up write {b}-{i} must be locally accepted"
            );
        }
        sim.run_for(WARMUP_BURST_GAP);
    }

    let mut learner_disk = DiskConfig::default();
    learner_disk.set_sync_delay(LEARNER_SYNC_DELAY);
    sim.set_disk_config_for(nid(3), learner_disk);

    let voters: Vec<NodeId> = ids.iter().copied().map(nid).collect();
    let learner = nid(3);
    let learner_metrics = MetricsHandle::recording();
    let node3 = RaftKvNode::start_with_metrics(
        sim.env(learner.clone()),
        voters,
        MemoryEngine::new(),
        learner_metrics.clone(),
    );
    node3.set_compact_tuning_for_test(None, Some(TEST_EMERGENCY_CEILING));
    assert!(
        matches!(
            nodes[l].add_learner(learner.clone()),
            ProposeResult::Accepted { .. }
        ),
        "seed={seed:#x}: add_learner must be accepted by the leader"
    );

    #[allow(
        clippy::disallowed_methods,
        reason = "real-time watchdog against unbounded per-round CPU work — SimEnv's virtual clock cannot see this"
    )]
    let start = Instant::now();
    let budget = real_budget();

    for round in 0..ROUNDS {
        for i in 0..BURST_LEN {
            let key = format!("k-{round}-{i}").into_bytes();
            let res = nodes[l].put(key, vec![b'v'; 256]);
            assert!(
                matches!(res, ProposeResult::Accepted { .. }),
                "seed={seed:#x}: write {round}-{i} must be locally accepted by the leader, got \
                 {res:?}"
            );
        }
        sim.run_for(BURST_GAP);
        if start.elapsed() >= budget {
            break;
        }
    }

    // Every metric below is read with the writer having just issued its
    // LAST burst above and NOT been stopped or drained — the "while
    // writing" property the module doc describes.
    let restarts: u64 = handles
        .iter()
        .map(|h| h.get(Metric::CpSnapshotTransferRestarts))
        .sum::<u64>()
        + learner_metrics.get(Metric::CpSnapshotTransferRestarts);
    let image_builds: u64 = handles
        .iter()
        .map(|h| h.get(Metric::CpSnapshotImageBuilds))
        .sum::<u64>()
        + learner_metrics.get(Metric::CpSnapshotImageBuilds);
    let installs: u64 = handles
        .iter()
        .map(|h| h.get(Metric::CpSnapshotInstalls))
        .sum::<u64>()
        + learner_metrics.get(Metric::CpSnapshotInstalls);
    let learner_applied = node3.engine_applied_index();
    let leader_commit = nodes[l].commit_index();

    eprintln!(
        "seed={seed:#x} warmup_bursts={warmup_bursts}: real_elapsed={:.2}s restarts={restarts} \
         image_builds={image_builds} installs={installs} learner_applied={learner_applied} \
         leader_commit={leader_commit}",
        start.elapsed().as_secs_f64(),
    );

    (
        restarts,
        image_builds,
        installs,
        learner_applied,
        leader_commit,
    )
}

/// The control: a learner that joins the group BEFORE any writes (and
/// therefore before any compaction) at the SAME sustained rate the
/// regression below uses. `Metric::CpSnapshotTransferRestarts` staying
/// small proves this rate/disk-delay combination is not itself a capacity
/// mismatch dressed up as issue #1064's own mechanism.
#[test]
fn an_appendentries_only_learner_converges_issue_1064_control() {
    let seed = 0x1064_1101;
    let (restarts, image_builds, _installs, _learner_applied, _leader_commit) =
        run_scenario(seed, 0);
    // A small, fixed tolerance rather than a strict zero: even a
    // well-behaved replica joining right as a continuous writer starts can
    // need a handful of on-demand images before its own steady-state
    // rhythm settles (confirmed live — a bursty writer instantaneously
    // reopens a `BURST_LEN`-sized gap every round, which `RaftCore::
    // compaction_floor`'s own voters-only design, deliberate, never
    // protects a learner's position against). The property this control
    // actually needs to prove is "not a runaway, ever-climbing flood,"
    // which `assert_late_join_converges_while_writing`'s own much larger
    // bound is calibrated against.
    const CONTROL_TOLERANCE: u64 = 10;
    assert!(
        image_builds <= CONTROL_TOLERANCE,
        "seed={seed:#x}: an early-joining learner needed {image_builds} on-demand snapshot \
         image(s) at a rate meant to isolate the AppendEntries-only control from the \
         InstallSnapshot mechanism under test"
    );
    assert!(
        restarts <= CONTROL_TOLERANCE,
        "seed={seed:#x}: an AppendEntries-only learner triggered {restarts} snapshot transfer \
         restart(s) — should stay small, not grow with run length, at this rate"
    );
}

const BASE_LATE_JOIN_SEED: u64 = 0x1064_1102;

/// Generous margins around the FIXED code's own measured numbers (6
/// restarts, at least one successful install, the learner's own applied
/// index reaching well over half the leader's commit index) — see the
/// module doc for the full before/after comparison against the pre-fix
/// code (75 restarts, ZERO successful installs, learner applied index
/// stuck at 0 for the entire run).
const MAX_TOLERATED_RESTARTS: u64 = 15;
const MIN_LEARNER_PROGRESS_PERCENT: u64 = 25;

fn assert_late_join_converges_while_writing(seed: u64) {
    let (restarts, _image_builds, installs, learner_applied, leader_commit) =
        run_scenario(seed, WARMUP_BURSTS);
    assert!(
        installs >= 1,
        "seed={seed:#x}: a learner needing a real, multi-chunk InstallSnapshot never completed \
         a SINGLE install over the whole run (a livelock matching issue #1064's own \
         description exactly) — RaftCore::snapshot_upto is invalidating its in-flight transfer \
         faster than it can ever land"
    );
    assert!(
        restarts <= MAX_TOLERATED_RESTARTS,
        "seed={seed:#x}: {restarts} snapshot transfer restarts over {ROUNDS} bursts of a writer \
         that never stopped — COMPACT_DEFER_EMERGENCY_CEILING is still forcing out a genuinely \
         progressing learner transfer (issue #1064)"
    );
    assert!(
        learner_applied.saturating_mul(100)
            >= leader_commit.saturating_mul(MIN_LEARNER_PROGRESS_PERCENT),
        "seed={seed:#x}: the learner's own applied index ({learner_applied}) never reached even \
         {MIN_LEARNER_PROGRESS_PERCENT}% of the leader's commit index ({leader_commit}) while \
         the writer kept running — not the substantial, ongoing progress the fix is supposed to \
         let it make"
    );
}

/// The regression: a learner that joins only after the leader has already
/// compacted a warmed-up log, needing a real, multi-round-trip
/// `InstallSnapshot`, under the SAME sustained writer as the control above,
/// with the emergency ceiling overridden down to a size this writer crosses
/// many times before a transfer could naturally land if forced out. Must
/// land at least one snapshot, keep `CpSnapshotTransferRestarts` bounded,
/// and make substantial, ongoing progress — all measured WHILE the writer
/// keeps running.
#[test]
fn a_late_joining_learner_converges_despite_needing_a_multi_chunk_install_snapshot_issue_1064() {
    assert_late_join_converges_while_writing(BASE_LATE_JOIN_SEED);
}

/// A small, fixed-quality mixing hash (splitmix64) — deliberately NOT the
/// `Simulator`'s own seeded RNG, mirroring `directed_placing_under_
/// sustained_load.rs`'s own convention for deriving extra corpus seeds from
/// one base seed.
fn splitmix64(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^= x >> 33;
    x
}

/// Depth knob (`ANIMUS_LEARNER_SNAPSHOT_LIVELOCK_SEEDS`, default 1 = just
/// the frozen seed above), root `CLAUDE.md`'s test-scaling table — following
/// the same `ANIMUS_*_SEEDS` corpus-depth convention every other fault-
/// injecting suite in this repo uses. Scoped to the actual regression (the
/// late-joining scenario) only — the AppendEntries-only control's whole job
/// is proving ONE specific rate is sustainable, which a seed sweep doesn't
/// add signal to.
#[test]
fn late_joining_learner_corpus_runs_at_configured_depth() {
    let k = corpus::seeds_from_env("ANIMUS_LEARNER_SNAPSHOT_LIVELOCK_SEEDS");
    for i in 0..k {
        let seed = if i == 0 {
            BASE_LATE_JOIN_SEED
        } else {
            splitmix64(BASE_LATE_JOIN_SEED ^ (i as u64))
        };
        assert_late_join_converges_while_writing(seed);
    }
}
