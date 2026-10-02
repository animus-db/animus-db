//! Regression for issue #1064: a directed-Placing-style multi-replica
//! reconfigure (desired differs from current by MORE than one member,
//! mirroring ADR 0062's post-split-cutover retarget) permanently wedges
//! under a CONTINUOUS (never-stopping) sustained writer, because
//! `RaftCore::learner_caught_up`'s promotion predicate (ADR 0058 Train 1)
//! used to compare a learner's own tracked `match_index` against the
//! LEADER'S OWN `last_log_index()` — which, under continuous writes,
//! includes whatever the leader has just appended for ITSELF in the current
//! tick, before that entry has even been sent to (let alone acked by)
//! anyone, voter or learner alike. A learner that is otherwise fully caught
//! up with everything the group has actually **committed** never satisfies
//! an absolute-gap check against that perpetually-fresher tip once a single
//! write burst exceeds `RECONFIGURE_LEARNER_CATCH_UP_THRESHOLD` (4) entries
//! — every sample lands mid-burst, so it can never be promoted, the
//! reconciler can never add the second desired replica or drop either
//! stale voter, and the group sits wedged at the old 3-voter config
//! forever, no matter how much real replication time it's given.
//!
//! Unlike `tests/learner_catchup_under_load.rs` (a BOUNDED write window,
//! then drain-and-converge), this test keeps the writer running THE WHOLE
//! TIME `reconfigure_step` is being driven — the shape the field report
//! (issue #1064) actually hit: `animusd --cluster-control 3 --cluster-data 5
//! --auto-split-bytes 1000000` under continuous seed/PutItem load, never a
//! bounded burst — and `reconfigure_step` is deliberately sampled
//! IMMEDIATELY after each write burst is proposed, before `Simulator::
//! run_for` lets any of it replicate. That is not a test-harness artifact:
//! it is the worst-case (and, under a genuinely saturating writer, the
//! TYPICAL-case) relationship between a production reconciler's own
//! independent tick and a continuous client write stream.
//!
//! **`BURST_GAP` is deliberately generous (300ms) relative to the learner's
//! own round-trip cost (a 10ms disk delay)** — this isolates the exact
//! defect under test (the WRONG BASELINE METRIC) from a completely
//! different, already-understood concern (`learner_catchup_under_load.rs`'s
//! own issues #532/#537: a write rate that outpaces what a lagging peer can
//! ever physically absorb, or repeated `InstallSnapshot` invalidation under
//! heavy compaction churn — see that test's module doc, and this one's own
//! issue #1064 investigation notes in `docs/lessons/testing/`). With ample
//! settle time between bursts, `commit_index()` and every voter's/learner's
//! own `match_index` are given every chance to fully equalize between
//! samples — so a persistent, un-closing gap under `last_log_index()` (this
//! test, on unfixed `main`) is unambiguously the baseline-metric defect,
//! not a capacity limit or a snapshot-transfer race.
//!
//! Asserts, on a bounded virtual-time AND real-time budget:
//! 1. The group's committed voter config converges to the directed-Placing
//!    target `{n0, n3, n4}` with no learners left dangling.
//! 2. Liveness throughout: the leader's `commit_index` never stalls for
//!    more than a small bounded number of bursts — the reconfigure/learner
//!    machinery under test must never itself stall ordinary writes.
//!
//! `ANIMUS_DIRECTED_PLACING_LOAD_SEEDS=K` (default 1 = just the frozen seed
//! below) additionally runs `K - 1` fresh derived seeds, following this
//! repo's existing `ANIMUS_*_SEEDS` corpus-depth convention (root
//! `CLAUDE.md`'s test-scaling table).

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{NodeId, nid};
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_test::corpus;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn leader_among(nodes: &[Option<KvNode>]) -> Option<usize> {
    let ls: Vec<usize> = (0..nodes.len())
        .filter(|&i| nodes[i].as_ref().is_some_and(|n| n.is_leader()))
        .collect();
    if ls.len() == 1 { Some(ls[0]) } else { None }
}

/// A small, fixed-quality mixing hash (splitmix64) — deliberately NOT the
/// `Simulator`'s own seeded RNG, following `tests/raftkv_linearizable.rs`'s
/// own convention for deriving extra corpus seeds from a base one.
fn splitmix64(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^= x >> 33;
    x
}

const BASE_SEED: u64 = 0x1064_0001;

/// Must exceed `RECONFIGURE_LEARNER_CATCH_UP_THRESHOLD` (4,
/// `animus-cp-data/src/lib.rs`, private to that crate) — every burst must
/// exceed it for the old `last_log_index()`-baselined predicate to be
/// structurally unable to see a satisfied gap immediately after any burst
/// is proposed.
const BURST_LEN: u64 = 5;
/// Generous relative to the learner's own 10ms disk round-trip — see the
/// module doc for why: this isolates the baseline-metric defect from any
/// write-rate/capacity concern.
const BURST_GAP: Duration = Duration::from_millis(300);
/// Bounded — see the module doc: with the fix, convergence needs only a
/// handful of `reconfigure_step` calls (one learner add + promote per new
/// member, then two old-voter removes), each needing at most one `BURST_GAP`
/// of settle time; on unfixed `main` the wedge is structural (every sample
/// lands mid-burst, regardless of how many bursts follow), so this bound is
/// generous enough to make that unambiguous without paying for a long run.
const TOTAL_BURSTS: u64 = 60;
/// How many consecutive bursts the leader's own `commit_index` is allowed to
/// sit still before the liveness assertion fires — ordinary writes must
/// never stall just because the learner/reconfigure machinery under test is
/// busy, regardless of whether the directed-Placing target has converged
/// yet.
const MAX_STALL_BURSTS: u64 = 20;

/// Runs the whole scenario once for `seed` — shared by the frozen regression
/// test and the depth-scaled corpus below.
fn run_scenario(seed: u64) {
    let mut sim = Simulator::new(seed);
    // Nodes 0,1,2 are the initial group; 3,4 are the directed-placing
    // target's fresh replacements (mirroring a post-split retarget onto
    // previously-idle nodes). All five are constructed up front so we can
    // reconfigure toward {0,3,4} without any "who hosts this replica" host
    // reconciler in the loop (this crate tests RaftKvNode directly).
    let all_ids: [u64; 5] = [0, 1, 2, 3, 4];
    let initial_voters: Vec<NodeId> = [0u64, 1, 2].iter().copied().map(nid).collect();

    let mut nodes: Vec<Option<KvNode>> = Vec::new();
    for &id in &all_ids[..3] {
        nodes.push(Some(RaftKvNode::start(
            sim.env(nid(id)),
            initial_voters.clone(),
            MemoryEngine::new(),
        )));
    }
    // Slots for 3, 4 — not started yet (they join later, mirroring a
    // directed-placing target naming a replica that isn't hosted until the
    // reconciler notices).
    nodes.push(None);
    nodes.push(None);

    sim.run_for(Duration::from_secs(2));
    let l0 = leader_among(&nodes).expect("an initial leader among 0,1,2");

    // A small warm-up, mirroring learner_catchup_under_load.rs's own shape
    // (a nonempty log before a learner ever joins), kept small deliberately
    // — see the module doc: this test isolates the baseline-metric defect,
    // not compaction/snapshot-transfer behavior, so it never needs the log
    // to grow large.
    for i in 0..3u64 {
        let key = format!("warm-{i}").into_bytes();
        assert!(
            matches!(
                nodes[l0].as_ref().unwrap().put(key, b"v".to_vec()),
                ProposeResult::Accepted { .. }
            ),
            "seed={seed:#x}: warm-up write must be locally accepted"
        );
    }
    sim.run_for(BURST_GAP);

    // Nodes 3 and 4 now "materialize" (host reconciler equivalent): each
    // gets a modest fixed disk round-trip cost, mirroring contended disk/CPU
    // on a busy node under concurrent seeding + PutItem load in production.
    for &extra in &[3u64, 4u64] {
        let mut disk = DiskConfig::default();
        disk.set_sync_delay(Duration::from_millis(10));
        sim.set_disk_config_for(nid(extra), disk);
        // A joining node starts as a quiet non-voter knowing only the
        // CURRENT voters (the "pre-start a to-be-added node" gotcha).
        nodes[extra as usize] = Some(RaftKvNode::start(
            sim.env(nid(extra)),
            initial_voters.clone(),
            MemoryEngine::new(),
        ));
    }

    // Directed-placing target: {0, 3, 4} — keep node 0, replace 1 and 2 with
    // the two fresh nodes. This is a 2-of-3 diff, mirroring a post-split
    // retarget onto previously-idle nodes.
    let desired: BTreeSet<NodeId> = [0u64, 3, 4].iter().copied().map(nid).collect();
    let down: BTreeSet<NodeId> = BTreeSet::new();

    #[allow(
        clippy::disallowed_methods,
        reason = "real-time watchdog against unbounded per-round CPU work — SimEnv's virtual clock cannot see this"
    )]
    let start = Instant::now();
    let real_budget = Duration::from_secs(60);

    let mut converged_at_burst: Option<u64> = None;
    let mut last_commit_index: u64 = 0;
    let mut bursts_since_commit_advance: u64 = 0;

    for burst in 0..TOTAL_BURSTS {
        // Continuous writer: never stops, interleaved with reconfigure_step,
        // and sampled in the worst-case (for the old predicate) order —
        // propose first, THEN call reconfigure_step, THEN let time pass. See
        // the module doc for why this is the realistic ordering, not an
        // artifact.
        let cur_leader = leader_among(&nodes);
        if let Some(l) = cur_leader {
            for i in 0..BURST_LEN {
                let key = format!("k-{burst}-{i}").into_bytes();
                let _ = nodes[l].as_ref().unwrap().put(key, b"v".to_vec());
            }
            let _ = nodes[l].as_ref().unwrap().reconfigure_step(&desired, &down);

            // Liveness: the leader's own commit_index must keep advancing —
            // a continuous writer's commits are never allowed to stall just
            // because the reconfigure/learner machinery is busy.
            let ci = nodes[l].as_ref().unwrap().commit_index();
            if ci > last_commit_index {
                last_commit_index = ci;
                bursts_since_commit_advance = 0;
            } else {
                bursts_since_commit_advance += 1;
            }
            assert!(
                bursts_since_commit_advance <= MAX_STALL_BURSTS,
                "seed={seed:#x}: leader commit_index stalled at {last_commit_index} for \
                 {bursts_since_commit_advance} consecutive bursts (burst={burst}) — ordinary \
                 writes must keep committing throughout a directed-Placing reconfigure"
            );
        }
        sim.run_for(BURST_GAP);

        // Check convergence: the leader's own live config()/learners() must
        // reach the directed-Placing target with no learner left dangling.
        if let Some(l) = leader_among(&nodes) {
            let node = nodes[l].as_ref().unwrap();
            if node.config() == desired && node.learners().is_empty() {
                converged_at_burst = Some(burst);
                break;
            }
        }

        if start.elapsed() >= real_budget {
            break;
        }
    }

    let final_leader = leader_among(&nodes);
    let (final_config, final_learners) = match final_leader {
        Some(l) => {
            let node = nodes[l].as_ref().unwrap();
            (node.config(), node.learners())
        }
        None => (BTreeSet::new(), BTreeSet::new()),
    };

    eprintln!(
        "seed={seed:#x}: converged_at_burst={converged_at_burst:?} real_elapsed={:.1}s \
         final_leader={final_leader:?} final_config={final_config:?} \
         final_learners={final_learners:?} desired={desired:?}",
        start.elapsed().as_secs_f64()
    );

    for (i, n) in nodes.iter().enumerate() {
        if let Some(n) = n {
            eprintln!(
                "  node {i}: is_leader={} config={:?} learners={:?} commit_index={} \
                 last_applied={}",
                n.is_leader(),
                n.config(),
                n.learners(),
                n.commit_index(),
                n.last_applied()
            );
        } else {
            eprintln!("  node {i}: not hosted");
        }
    }

    assert!(
        converged_at_burst.is_some(),
        "seed={seed:#x}: directed-placing target {desired:?} never converged under a CONTINUOUS \
         writer after {TOTAL_BURSTS} bursts x {BURST_LEN} proposes ({:.1}s real time) — \
         final config={final_config:?} learners={final_learners:?} (issue #1064)",
        start.elapsed().as_secs_f64()
    );
    assert!(
        final_learners.is_empty(),
        "seed={seed:#x}: converged config must leave no dangling learner"
    );
}

#[test]
fn directed_placing_two_of_three_diff_under_continuous_writer() {
    run_scenario(BASE_SEED);
}

/// Depth knob (`ANIMUS_DIRECTED_PLACING_LOAD_SEEDS`, default 1 = just the
/// frozen seed above) — following the existing corpus-depth convention
/// (`ANIMUS_RAFTKV_SEEDS` et al., see the root `CLAUDE.md` test-scaling
/// table). `K > 1` additionally derives `K - 1` fresh seeds and reruns the
/// whole scenario at each one.
#[test]
fn directed_placing_under_sustained_load_corpus_runs_at_configured_depth() {
    let k = corpus::seeds_from_env("ANIMUS_DIRECTED_PLACING_LOAD_SEEDS");
    for i in 0..k {
        let seed = if i == 0 {
            BASE_SEED
        } else {
            splitmix64(BASE_SEED ^ (i as u64))
        };
        run_scenario(seed);
    }
}
