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
//! **This file therefore proves the actual mechanism under test — whether
//! the LEARNER's transfer is allowed to land and make real progress, or is
//! forced back to chunk 0 forever — directly, via `Metric::
//! CpSnapshotInstalls`/`CpSnapshotTransferRestarts`/`RaftKvNode::
//! engine_applied_index`** — see `assert_late_join_converges_while_writing`'s
//! own assertions and the measured before/after numbers below, rather than
//! requiring the much stronger (and still not a claim this file makes)
//! "sustained tight-threshold catch-up" property.
//!
//! **A third, unrelated defect this file's own tuning run originally
//! surfaced (issue #1070, NOW FIXED, `handle_append_resp`'s non-monotonic
//! `next_index` update on an ordinary success ack) used to force `ROUNDS`
//! to stay well clear of 300 bursts** to avoid it silently halting all
//! further replication to the learner (a permanent stall, not merely a
//! slowdown — confirmed live at `ROUNDS = 300` with the pre-#1070-fix code:
//! `learner_applied` reached `22414` (of `25102` commits) at the
//! three-quarter sample point, then stayed PINNED at exactly `22414` for
//! the rest of the run while `commit_index` climbed a further `7400` to
//! `32502` — a real freeze, not a slow trickle, and the reason
//! `assert_late_join_converges_while_writing`'s own final-quarter-progress
//! check below exists: the overall/whole-run applied-vs-commit ratio alone
//! (`22414` of `32502`, `69%`) stays deceptively healthy-looking for a long
//! time after forward progress has actually stopped, since it also
//! reflects everything applied BEFORE the freeze. With issue #1070 fixed,
//! this file runs `ROUNDS = 300` and stays green (a genuinely progressing
//! learner reaches `28714` applied of `32502` commits, `88%`, with real
//! progress in the final quarter too) — see `docs/lessons/testing/
//! 2026-09-28-an-ordinary-appendentries-acks-next-index-update-is-not-
//! monotonic.md` for the fix's own full before/after numbers at this exact
//! file's seed/constants.
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
//! measured live at this file's exact seed and constants (`ROUNDS = 300`,
//! 100ms virtual apart)**: temporarily reverting `emergency_ceiling_hit`'s
//! own `!learner_transfer_in_flight` exemption in `lib.rs` (i.e. restoring
//! the pre-fix `behind >= compact_emergency_ceiling` alone) turns this
//! scenario from `restarts=11, installs=4, learner's own applied index
//! reaching 28714 of the leader's 32502 commits (88%) — genuine,
//! substantial, ongoing progress` into `restarts=150, installs=0 (the
//! learner NEVER completes a single InstallSnapshot, ever), learner's own
//! applied index stuck at 0 for the entire run` — a livelock matching the
//! issue's own description exactly, not a mere slowdown. `MAX_TOLERATED_
//! RESTARTS` (15) and `MIN_LEARNER_PROGRESS_PERCENT` (below) are both
//! chosen with generous margin around the FIXED code's own measured 11
//! restarts / 88% progress, while remaining far below the pre-fix code's
//! measured 150 restarts / 0% progress.

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

/// The learner's own disk round-trip cost — the sim applies it to every
/// `append` and every `sync`, and a persist round is ONE append + ONE sync
/// however many entries the received message carries (issue #1092: it used
/// to be one append per record, i.e. ~10 s for a 512-entry batch, which this
/// file's own final-quarter window could not tell apart from a stall).
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
/// The exact length that used to trip issue #1070's now-fixed non-monotonic
/// `next_index` defect (a permanent replication freeze, not just a
/// slowdown) — see the module doc for the measured before/after numbers.
/// Long enough to exercise both this file's own emergency-ceiling
/// mechanism AND issue #1070's fix in one run.
const ROUNDS: u64 = 300;

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
/// read — see the module doc.
///
/// `run_scenario`'s own result is a named struct rather than a growing
/// tuple, since `late_applied`/`late_commit` (added for issue #1070's own
/// "still advancing near the end," not just "reached a healthy total
/// overall" check) made a positional tuple unwieldy.
struct ScenarioResult {
    restarts: u64,
    image_builds: u64,
    installs: u64,
    learner_applied: u64,
    leader_commit: u64,
    /// `RaftKvNode::engine_applied_index`/`commit_index` sampled at the
    /// three-quarter point of the run (`(ROUNDS * 3) / 4`) — see the call
    /// site's own doc for why late, not halfway.
    late_applied: u64,
    late_commit: u64,
}

fn run_scenario(seed: u64, warmup_bursts: u64) -> ScenarioResult {
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

    // Sampled at the THREE-QUARTER point (not halfway) so the final
    // assertions can check the learner is STILL advancing in the run's own
    // final quarter, not merely that it reached some healthy-looking total
    // overall — a learner that made good progress early and then genuinely
    // froze (issue #1070's own shape: `engine_applied_index` pinned while
    // `commit_index` keeps climbing) can still show a deceptively high
    // final applied/commit RATIO for a long time after freezing, since that
    // ratio also reflects everything applied before the freeze; a halfway
    // sample was tried first and was still too early to reliably land after
    // the freeze's own onset at this file's exact seed/constants.
    let mut late_applied = 0u64;
    let mut late_commit = 0u64;
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
        if round == (ROUNDS * 3) / 4 {
            late_applied = node3.engine_applied_index();
            late_commit = nodes[l].commit_index();
        }
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

    ScenarioResult {
        restarts,
        image_builds,
        installs,
        learner_applied,
        leader_commit,
        late_applied,
        late_commit,
    }
}

/// The control: a learner that joins the group BEFORE any writes (and
/// therefore before any compaction) at the SAME sustained rate the
/// regression below uses. `Metric::CpSnapshotTransferRestarts` staying
/// small proves this rate/disk-delay combination is not itself a capacity
/// mismatch dressed up as issue #1064's own mechanism.
#[test]
fn an_appendentries_only_learner_converges_issue_1064_control() {
    let seed = 0x1064_1101;
    let ScenarioResult {
        restarts,
        image_builds,
        ..
    } = run_scenario(seed, 0);
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
    const CONTROL_TOLERANCE: u64 = 20;
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

/// Generous margins around the FIXED code's own measured numbers (11
/// restarts, at least one successful install, the learner's own applied
/// index reaching 88% of the leader's commit index) — see the module doc
/// for the full before/after comparison against the pre-fix code (150
/// restarts, ZERO successful installs, learner applied index stuck at 0
/// for the entire run).
const MAX_TOLERATED_RESTARTS: u64 = 15;
const MIN_LEARNER_PROGRESS_PERCENT: u64 = 25;
/// Of the log growth in the run's OWN final quarter (from the three-quarter
/// sample to the end), the learner's own applied index must cover at least
/// this fraction — see `ScenarioResult::late_applied`'s own doc for why an
/// overall (start-to-end) ratio alone can't catch a learner that made good
/// progress early and then genuinely froze (issue #1070's own shape): a
/// high enough starting base can keep the OVERALL ratio looking healthy for
/// a long time after forward progress has actually stopped.
const MIN_FINAL_QUARTER_PROGRESS_PERCENT: u64 = 25;

fn assert_late_join_converges_while_writing(seed: u64) {
    let r = run_scenario(seed, WARMUP_BURSTS);
    assert!(
        r.installs >= 1,
        "seed={seed:#x}: a learner needing a real, multi-chunk InstallSnapshot never completed \
         a SINGLE install over the whole run (a livelock matching issue #1064's own \
         description exactly) — RaftCore::snapshot_upto is invalidating its in-flight transfer \
         faster than it can ever land"
    );
    assert!(
        r.restarts <= MAX_TOLERATED_RESTARTS,
        "seed={seed:#x}: {} snapshot transfer restarts over {ROUNDS} bursts of a writer that \
         never stopped — COMPACT_DEFER_EMERGENCY_CEILING is still forcing out a genuinely \
         progressing learner transfer (issue #1064)",
        r.restarts,
    );
    assert!(
        r.learner_applied.saturating_mul(100)
            >= r.leader_commit.saturating_mul(MIN_LEARNER_PROGRESS_PERCENT),
        "seed={seed:#x}: the learner's own applied index ({}) never reached even \
         {MIN_LEARNER_PROGRESS_PERCENT}% of the leader's commit index ({}) while the writer \
         kept running — not the substantial, ongoing progress the fix is supposed to let it make",
        r.learner_applied,
        r.leader_commit,
    );
    let final_quarter_commit_growth = r.leader_commit.saturating_sub(r.late_commit);
    let final_quarter_applied_growth = r.learner_applied.saturating_sub(r.late_applied);
    assert!(
        final_quarter_applied_growth.saturating_mul(100)
            >= final_quarter_commit_growth.saturating_mul(MIN_FINAL_QUARTER_PROGRESS_PERCENT),
        "seed={seed:#x}: the learner's own applied index advanced by only \
         {final_quarter_applied_growth} in the run's own final quarter, against \
         {final_quarter_commit_growth} more commits landing in that same window — it made good \
         progress early ({} applied of {} commits at the three-quarter point) and then \
         stalled, exactly the shape issue #1070's non-monotonic next_index update produces (a \
         high overall ratio can hide a real, late freeze — see this file's own module doc)",
        r.late_applied,
        r.late_commit,
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

/// Issue #1092: the corpus's second seed (`splitmix64(BASE ^ 1)`) used to
/// fail the final-quarter progress assert at depth 2 — deterministically, not
/// flakily. Root cause was not this file's protocol mechanism at all: the
/// learner's WAL persist round for one 512-entry `AppendEntries` batch cost
/// one disk latency PER RECORD (`persist_wal` appended record by record; a
/// `SimEnv` `sync_delay` applies to every `append`), ~10 s of virtual time,
/// longer than the run's final quarter (7.5 s) — the learner's ack (correctly
/// held until durable) simply had not been released yet. `persist_wal` now
/// coalesces a round into one append (`wal_round_single_append.rs` is the
/// mechanism-level regression); this pins the exact failing seed.
#[test]
fn a_late_joining_learner_converges_at_the_issue_1092_seed() {
    assert_late_join_converges_while_writing(0xfa2b_71bc_5313_f78c);
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
