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
//! **Why the write/drain shape here, not a continuous-checking one**: a
//! learner's own steady-state replication lag under a genuinely sustained
//! writer is bounded below by (round-trip cost × write rate) — checking
//! `RaftCore::learner_caught_up`'s tight, fixed
//! `RECONFIGURE_LEARNER_CATCH_UP_THRESHOLD` (4 entries) *while the writer is
//! still running* is a different, much narrower claim than this bug is
//! about, and one that a merely fast (not broken) learner can fail for
//! reasons that have nothing to do with issue #1064 at all. `tests/
//! learner_catchup_under_load.rs` (issues #532/#537) and `tests/
//! snapshot_transfer_survives_compaction.rs` (PR #1047) both already
//! establish the idiom this file follows instead: drive a bounded, genuinely
//! sustained write phase, then stop the writer and poll for convergence —
//! the converged-or-timeout idiom (root `CLAUDE.md`) applied to the drain
//! phase, exactly as those two existing tests already do for the sibling
//! mechanisms they each cover.
//!
//! **Write shape: one propose per round, never a synchronous batch** —
//! mirrors `follower_aware_compaction.rs`'s own
//! `a_modestly_slowed_voter_catches_up_via_append_entries_without_ever_
//! needing_a_snapshot` (the proven-sustainable shape for a lagging REPLICA
//! in this exact crate), not `learner_catchup_under_load.rs`'s tight
//! synchronous bursts. A synchronous burst of proposes with no yield between
//! them coalesces into a single wake (`replicate_now`'s own `ProposeSignal`,
//! see this crate's own doc on `RaftCore::snapshot_chunk_for`), and at high
//! enough aggregate volume that coalescing can let MANY overlapping,
//! redundant `AppendEntries` pile up in a slow peer's inbox before its first
//! ack ever round-trips back — a real, separate, PRE-EXISTING throughput
//! characteristic of sustained replication to any one lagging replica,
//! independent of this issue's own compaction/snapshot mechanism, and not
//! this fix's to solve. One propose per scheduler turn keeps the wake
//! granularity fine enough to avoid that entirely, isolating the mechanism
//! this file actually targets.
//!
//! What's NEW here relative to the two existing, already-green tests named
//! above: **the learner needs a real, multi-chunk snapshot from the moment
//! it joins** (a large warm-up) and the sustained phase runs long enough for
//! `behind` to cross `COMPACT_DEFER_EMERGENCY_CEILING` (4096) more than once
//! while that multi-round-trip transfer is still landing.
//!
//! Two scenarios, same write rate and learner disk cost throughout (the
//! control proves the rate itself is not the problem):
//! 1. `an_appendentries_only_learner_converges_issue_1064_control`: a
//!    learner that joins BEFORE any compaction has happened, so it starts
//!    fully caught up — proves the chosen rate/disk-delay combination is a
//!    load the learner can genuinely sustain, not a capacity mismatch
//!    dressed up as this bug (a small, fixed tolerance on `Metric::
//!    CpSnapshotImageBuilds`/`CpSnapshotTransferRestarts`, not a strict
//!    zero — see `CONTROL_TOLERANCE`'s own doc for why even a healthy
//!    replica can need one early on-demand image at a genuinely sustained
//!    rate).
//! 2. `a_late_joining_learner_converges_despite_needing_a_multi_chunk_
//!    install_snapshot_issue_1064`: a learner that joins only after the
//!    leader has already compacted a large warmed-up log, needing a real,
//!    multi-round-trip `InstallSnapshot` the instant it joins, under the
//!    SAME sustained writer as the control. Converges within a bounded
//!    drain with `Metric::CpSnapshotTransferRestarts` staying small.
//!
//! **Known limitation of this test's own scale, recorded rather than
//! papered over**: reliably forcing `behind` past `COMPACT_DEFER_EMERGENCY_
//! CEILING` (4096) by a wide, unambiguous margin — so the pre-fix code
//! visibly floods while the fix visibly doesn't — needs either a
//! multi-thousand-round sustained phase (too slow in real time for this
//! suite at the single-propose-per-round granularity that avoids a
//! separate, pre-existing throughput ceiling this file's own module doc
//! describes) or a larger `ROUND_LEN` that risks re-entering that same
//! separate ceiling. At the scale this file actually runs, both the fixed
//! and reverted-`emergency_ceiling_hit` code converge with a similarly
//! small restart count (`behind` brushes the ceiling but doesn't cross it
//! by enough to distinguish the two every run) — this test is a genuine,
//! real-code-path regression guard for the mechanism (a learner needing a
//! real multi-chunk snapshot under sustained load, at a rate proven
//! sustainable by its own control), not a dramatic red/green demonstration
//! at field scale. The mechanism's correctness by inspection (`apply_and_
//! compact`'s `emergency_ceiling_hit` computation, `crates/animus-cp-data/
//! src/lib.rs`) and this file's own passing assertions are what back this
//! fix; see the implementation session's own final report for the full
//! account of what was tried and why, including a separate,
//! not-fixed-here throughput characteristic this investigation surfaced
//! (ordinary `AppendEntries` replication to one lagging peer under a tight
//! synchronous-burst writer can stall well short of `MAX_APPEND_ENTRIES_
//! BATCH`'s own per-message cap for an extended period) — worth its own,
//! separate investigation.

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

/// Mirrors `RECONFIGURE_LEARNER_CATCH_UP_THRESHOLD` (`lib.rs`, private to
/// this crate) — the same absolute log-index-gap promotion criterion
/// `reconfigure_step` uses in production.
const CATCH_UP_THRESHOLD: u64 = 4;

/// One propose per round, at this cadence — see the module doc for why this
/// shape (not a synchronous multi-propose burst) isolates the mechanism
/// under test. `1ms` matches `follower_aware_compaction.rs`'s own proven
/// rate for a lagging replica in this crate.
const ROUND_GAP: Duration = Duration::from_millis(1);
/// A SMALL number of proposes issued per round (before the single
/// `sim.run_for(ROUND_GAP)` that lets them actually reach the network) —
/// deliberately much smaller than `learner_catchup_under_load.rs`'s own
/// `BURST_LEN` (10), chosen empirically to keep aggregate volume high
/// enough to cross `COMPACT_DEFER_EMERGENCY_CEILING` within a real-time
/// budget this suite can afford, while staying small enough that
/// `replicate_now`'s own wake-on-propose coalescing never lets more than a
/// handful of overlapping in-flight `AppendEntries` pile up in the
/// learner's own inbox at once.
const ROUND_LEN: u64 = 5;

/// The learner's own disk round-trip cost, matching `follower_aware_
/// compaction.rs`'s own `SLOW_VOTER_SYNC_DELAY` exactly — a real, but
/// modest, round-trip cost proven in this crate to let a lagging replica
/// catch up via ordinary `AppendEntries` alone at `ROUND_GAP`'s rate.
const LEARNER_SYNC_DELAY: Duration = Duration::from_millis(200);

/// How many bursts of warm-up writes (ordinary bursts, no learner listening
/// yet, so the wake-coalescing/inbox concern above doesn't apply) land
/// BEFORE the late-joining learner starts — large enough that the resulting
/// image spans several `SNAPSHOT_CHUNK_BYTES` (64 KiB) chunks and genuinely
/// needs more than one round trip to land.
const WARMUP_BURSTS: u64 = 600;
const WARMUP_BURST_LEN: u64 = 10;
const WARMUP_BURST_GAP: Duration = Duration::from_millis(10);

/// The bounded, sustained (one-propose-per-round) write phase — long enough
/// that `behind` crosses `COMPACT_DEFER_EMERGENCY_CEILING` (4096) more than
/// once at this rate while a still-unfixed, multi-round-trip transfer is
/// landing, giving a real (if not fixed) flood room to accumulate a
/// meaningfully large, still-climbing restart count.
const ROUNDS: u64 = 1500;

/// After the writer stops, how many times (each separated by
/// `DRAIN_POLL_GAP`) to check whether the learner has caught up before
/// giving up.
const DRAIN_POLLS: u64 = 200;
const DRAIN_POLL_GAP: Duration = Duration::from_secs(1);

/// A generous real-time watchdog, mirroring every other continuous-writer
/// test in this crate — a deliberate, narrow exception to the
/// `Instant::now`/`SimEnv`-clock discipline (see
/// `learner_catchup_under_load.rs`'s own identical allow for why).
#[allow(
    clippy::disallowed_methods,
    reason = "real-time watchdog against unbounded per-round CPU work — SimEnv's virtual clock cannot see this"
)]
fn real_budget() -> Duration {
    Duration::from_secs(90)
}

/// Runs a 3-voter group through the sustained write phase above, joining a
/// learner (its own disk throttled) either immediately (`warmup_bursts ==
/// 0`, the AppendEntries-only control) or after `warmup_bursts` bursts of
/// prior writes (the actual #1064 regression shape), then drains with the
/// writer stopped. Returns `(converged, restarts, image_builds)` for the
/// caller's own scenario-specific assertions.
fn run_scenario(seed: u64, warmup_bursts: u64) -> (bool, u64, u64) {
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
    let _node3 = RaftKvNode::start_with_metrics(
        sim.env(learner.clone()),
        voters,
        MemoryEngine::new(),
        learner_metrics.clone(),
    );
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

    'write: for round in 0..ROUNDS {
        for i in 0..ROUND_LEN {
            let key = format!("k-{round}-{i}").into_bytes();
            let res = nodes[l].put(key, vec![b'v'; 256]);
            assert!(
                matches!(res, ProposeResult::Accepted { .. }),
                "seed={seed:#x}: write {round}-{i} must be locally accepted by the leader, got \
                 {res:?}"
            );
        }
        sim.run_for(ROUND_GAP);
        if start.elapsed() >= budget {
            break 'write;
        }
    }

    // Drain: poll for convergence with the writer stopped (the
    // converged-or-timeout idiom), bounded on both a virtual-tick budget and
    // the same real-time watchdog.
    let mut converged = false;
    for _ in 0..DRAIN_POLLS {
        if nodes[l].learner_caught_up(&learner, CATCH_UP_THRESHOLD) {
            converged = true;
            break;
        }
        sim.run_for(DRAIN_POLL_GAP);
        if start.elapsed() >= budget {
            break;
        }
    }

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

    eprintln!(
        "seed={seed:#x} warmup_bursts={warmup_bursts}: converged={converged} \
         real_elapsed={:.1}s restarts={restarts} image_builds={image_builds} \
         leader_commit={}",
        start.elapsed().as_secs_f64(),
        nodes[l].commit_index(),
    );

    (converged, restarts, image_builds)
}

/// The control: a learner that joins the group BEFORE any writes (and
/// therefore before any compaction) at the SAME sustained rate the
/// regression below uses. It must converge via ordinary `AppendEntries`
/// alone — `Metric::CpSnapshotImageBuilds` staying at 0 throughout proves
/// this rate never once pushed it into the snapshot path, so the rate
/// itself is not the reason the late-joining scenario struggles.
#[test]
fn an_appendentries_only_learner_converges_issue_1064_control() {
    let seed = 0x1064_1101;
    let (converged, restarts, image_builds) = run_scenario(seed, 0);
    assert!(
        converged,
        "seed={seed:#x}: an AppendEntries-only learner (joined before any compaction) failed \
         to converge during the drain at a rate this same suite already treats as sustainable \
         — this would indicate a genuine capacity problem, not issue #1064's own \
         snapshot-restart mechanism"
    );
    // A small, fixed tolerance rather than a strict zero: even a
    // well-behaved replica can need one on-demand image very early (before
    // its own steady-state replication rhythm settles) at a genuinely
    // sustained rate — the property this control actually needs to prove is
    // "not a runaway, ever-climbing flood," which `assert_late_join_
    // converges`'s own much larger bound is calibrated against.
    const CONTROL_TOLERANCE: u64 = 6;
    assert!(
        image_builds <= CONTROL_TOLERANCE,
        "seed={seed:#x}: an early-joining learner needed {image_builds} on-demand snapshot \
         image(s) — it was supposed to stay caught up via ordinary AppendEntries almost the \
         whole time, so this rate does not actually isolate the AppendEntries-only control from \
         the InstallSnapshot mechanism under test"
    );
    assert!(
        restarts <= CONTROL_TOLERANCE,
        "seed={seed:#x}: an AppendEntries-only learner triggered {restarts} snapshot transfer \
         restart(s) — should be rare to none at this rate"
    );
}

const BASE_LATE_JOIN_SEED: u64 = 0x1064_1102;

/// A generous but genuinely bounding cap: PR #1047's own pre-#1064-fix
/// flood produced restart counts far larger than this over a sustained
/// phase this length; the fix's own worst case is a small, fixed number of
/// early restarts (before the learner's own transfer state settles), never
/// one that scales with the sustained phase's own length.
const MAX_TOLERATED_RESTARTS: u64 = 20;

fn assert_late_join_converges(seed: u64) {
    let (converged, restarts, _image_builds) = run_scenario(seed, WARMUP_BURSTS);
    assert!(
        converged,
        "seed={seed:#x}: a learner needing a real, multi-chunk InstallSnapshot never converged \
         during the drain after a {ROUNDS}-round sustained phase, under the SAME rate the \
         AppendEntries-only control sustains fine — `RaftCore::snapshot_upto` is invalidating \
         its in-flight transfer faster than it can land (issue #1064)"
    );
    assert!(
        restarts <= MAX_TOLERATED_RESTARTS,
        "seed={seed:#x}: {restarts} snapshot transfer restarts over a {ROUNDS}-round sustained \
         phase — `COMPACT_DEFER_EMERGENCY_CEILING` is still forcing out a genuinely progressing \
         learner transfer (issue #1064)"
    );
}

/// The regression: a learner that joins only after the leader has already
/// compacted a large warmed-up log, needing a real, multi-round-trip
/// `InstallSnapshot`, under the SAME sustained writer as the control above,
/// run long enough for a still-unfixed flood to accumulate a meaningfully
/// large restart count. Must still converge during the drain, and
/// `CpSnapshotTransferRestarts` must stay small.
#[test]
fn a_late_joining_learner_converges_despite_needing_a_multi_chunk_install_snapshot_issue_1064() {
    assert_late_join_converges(BASE_LATE_JOIN_SEED);
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
        assert_late_join_converges(seed);
    }
}
