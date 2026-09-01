//! Regression for issues #532/#537: a freshly-added learner on a CP-data
//! Raft group falls behind and its commit/durable/applied state can freeze
//! for an extended run under a sustained per-item writer to the same
//! tablet. Two cooperating mechanisms, both in the shared `RaftCore`
//! (`animus-control/src/raft.rs`, reused unchanged by this crate's own
//! `RaftKvNode` driver):
//!
//! 1. **Unbounded `AppendEntries` batches.** With no per-peer cap,
//!    `replicate_to` shipped a lagging peer the ENTIRE outstanding tail
//!    (`next_index..=last_log_index`) in one message, cloned fresh
//!    (`self.log.iter().filter(..).cloned().collect()`) — and
//!    `propose_ordered`'s wake-on-propose (`RaftCore::replicate_now`, this
//!    crate's `lib.rs`) re-broadcasts that unbounded send on every single
//!    write, regardless of whether the peer had acked the last one yet.
//!    Fixed by `MAX_APPEND_ENTRIES_BATCH`.
//! 2. **Snapshot-transfer invalidation under repeated compaction** (found
//!    investigating this same issue, empirically confirmed to matter for
//!    actual convergence): once a lagging peer falls far enough behind to
//!    need a chunked `InstallSnapshot` instead, `RaftCore::snapshot_upto`
//!    unconditionally drops that in-flight transfer's own progress the
//!    moment the snapshot base moves again (required for correctness — see
//!    that method's own doc). Under sustained writes, `COMPACT_THRESHOLD`
//!    can re-cross before the peer's own multi-chunk transfer completes,
//!    restarting it from chunk 0 forever. Fixed by `COMPACT_DEFER_CEILING`
//!    (`lib.rs`): a threshold-triggered compaction is deferred while a
//!    transfer is genuinely in flight, up to a bounded emergency ceiling
//!    that still guarantees the WAL bounds regardless.
//!
//! **Why this needs a `sync_delay`-throttled peer to show up under
//! `SimEnv`**: `SimEnv`'s disk model charges *zero* virtual time for
//! `append`/`sync` unless a delay is configured, and a message's delivery
//! delay does not scale with its payload size — so with no artificial
//! per-round cost, a lagging peer catches up to a huge `AppendEntries` (or a
//! snapshot) the moment it is delivered, regardless of either fix. The real
//! pathology is a **wall-clock cost** (repeatedly cloning/rebuilding
//! ever-larger state while a peer's round is still in flight), not a
//! virtual-time one — exactly this issue's own field diagnosis ("a
//! real-time race"). `DiskConfig::set_sync_delay` on the learner's own disk
//! reproduces "this peer's round-trip is slow enough that real work piles
//! up against it before it acks" deterministically.
//!
//! **Why the writer stops rather than running forever**: a write rate that
//! genuinely exceeds a peer's maximum deliverable catch-up throughput can
//! never converge under any fix — that is a capacity limit, not a bug. This
//! scenario drives a bounded, sustained write window (the shape that
//! actually starved the pre-fix code) and then polls for convergence with
//! the writer stopped, the converged-or-timeout idiom (root `CLAUDE.md`)
//! applied to the drain phase.
//!
//! Writes are issued in tight bursts between `Simulator::run_for` calls,
//! never one `run_for` per propose — `run_for` itself carries real per-call
//! overhead unrelated to this mechanism (profiled while building this test:
//! 2,000 single proposes one `run_for(1ms)` apart cost ~12s of real time on
//! an unmodified, no-learner 3-node group from call overhead alone; an
//! equivalent-throughput batched shape cost ~0.25s).
//!
//! This is the centerpiece SimEnv red/green proof the fix's regression
//! suite is built around (root `CLAUDE.md`'s "every distributed behavior
//! lands with a fault-injecting simulation test... reproducible from a
//! seed"): asserted RED with both fixes reverted (`MAX_APPEND_ENTRIES_
//! BATCH` set to `usize::MAX` in `animus-control/src/raft.rs`, and
//! `lib.rs`'s `threshold_hit` gate reduced to plain `behind >=
//! COMPACT_THRESHOLD`) — see the fix's own final report for the quoted red
//! output — GREEN with both fixes in place. Mirrors `tests/
//! learner_reconfigure.rs`'s harness shape (`RaftKvNode::start`/`SimEnv`/
//! `Simulator`).

use std::time::{Duration, Instant};

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{NodeId, nid};
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn leader_among(nodes: &[KvNode]) -> Option<usize> {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    if ls.len() == 1 { Some(ls[0]) } else { None }
}

/// Mirrors `RECONFIGURE_LEARNER_CATCH_UP_THRESHOLD` (`lib.rs`, private to
/// this crate) — the same absolute log-index-gap promotion criterion
/// `reconfigure_step` uses in production.
const CATCH_UP_THRESHOLD: u64 = 4;

/// Proposes per burst between `run_for` calls, and the virtual duration each
/// burst is followed by. `BURST_LEN` proposes land essentially at once, then
/// `BURST_GAP` of virtual time lets the network/persist machinery run — a
/// sustained writer's aggregate rate (`BURST_LEN`/`BURST_GAP`) is what
/// matters to the mechanism, not the exact within-burst spacing.
const BURST_LEN: u64 = 10;
const BURST_GAP: Duration = Duration::from_millis(10);
/// How many write bursts the sustained writer issues before it stops (the
/// bounded "sustained write window" — see the module doc for why this test
/// does not run the writer forever).
const WRITE_BURSTS: u64 = 300;
/// After the writer stops, how many times (each separated by
/// `DRAIN_POLL_GAP` of virtual time) to check whether the learner has
/// caught up before giving up.
const DRAIN_POLLS: u64 = 300;
const DRAIN_POLL_GAP: Duration = Duration::from_millis(10);

/// A sustained per-item writer keeps proposing to the leader while a
/// freshly-added learner (its own disk deliberately throttled, see the
/// module doc) tries to catch up; once the writer stops, the learner's
/// tracked `match_index` must converge to within `CATCH_UP_THRESHOLD` of
/// the leader's own log tail inside a bounded virtual-tick AND real-time
/// budget — never freeze indefinitely.
#[test]
fn learner_converges_under_a_sustained_per_item_writer_issue_532_537() {
    let seed = 0x5324_0001;
    let mut sim = Simulator::new(seed);
    let ids = [0u64, 1, 2];
    let nodes: Vec<KvNode> = ids
        .iter()
        .map(|&id| {
            RaftKvNode::start(
                sim.env(nid(id)),
                ids.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    sim.run_for(Duration::from_secs(2));
    let l = leader_among(&nodes).expect("an initial leader");

    // Warm the log up before the learner ever joins, so the leader's log is
    // already nontrivially long by the time it has to replicate the whole
    // tail to a fresh learner (the field repro's own shape: the stall was
    // observed against an already-growing log, not an empty one).
    for b in 0..8u64 {
        for i in 0..BURST_LEN {
            let key = format!("warm-{b}-{i}").into_bytes();
            assert!(
                matches!(
                    nodes[l].put(key, b"v".to_vec()),
                    ProposeResult::Accepted { .. }
                ),
                "seed={seed}: warm-up write {b}-{i} must be locally accepted"
            );
        }
        sim.run_for(BURST_GAP);
    }

    // Node 3 joins as a quiet non-voter, knowing only the current voters
    // (the "pre-start a to-be-added node" gotcha, `animus-cp-data/CLAUDE.md`).
    // Its own disk carries a modest fixed round-trip cost (10ms) — see the
    // module doc for why this is what makes the pathology observable under
    // `SimEnv` at all.
    let mut learner_disk = DiskConfig::default();
    learner_disk.set_sync_delay(Duration::from_millis(10));
    sim.set_disk_config_for(nid(3), learner_disk);

    let voters: Vec<NodeId> = ids.iter().copied().map(nid).collect();
    let learner = nid(3);
    let _node3 = RaftKvNode::start(sim.env(learner.clone()), voters, MemoryEngine::new());

    assert!(
        matches!(
            nodes[l].add_learner(learner.clone()),
            ProposeResult::Accepted { .. }
        ),
        "seed={seed}: add_learner must be accepted by the leader"
    );

    // The sustained per-item writer keeps going for a bounded window — long
    // enough to exercise both the unbounded-`AppendEntries` amplification
    // and the compaction/snapshot-invalidation cycle (module doc) — then
    // stops. Convergence is polled AFTER the writer stops (the
    // converged-or-timeout idiom, root `CLAUDE.md`), bounded on both a
    // virtual-tick budget and a real-time watchdog (so an unbounded-CPU
    // pre-fix run fails promptly instead of hanging the test suite).
    // A deliberate, narrow exception to the `Instant::now`/`SimEnv`-clock
    // discipline: this watchdog exists specifically to bound *real* CPU
    // work per round (module doc) — `env.now()` reads `SimEnv`'s virtual
    // clock, which does not advance for CPU time at all, so it cannot serve
    // this purpose.
    #[allow(
        clippy::disallowed_methods,
        reason = "real-time watchdog against unbounded per-round CPU work — SimEnv's virtual clock cannot see this"
    )]
    let start = Instant::now();
    let real_budget = Duration::from_secs(45);
    for burst in 0..WRITE_BURSTS {
        for i in 0..BURST_LEN {
            let key = format!("k-{burst}-{i}").into_bytes();
            let _ = nodes[l].put(key, b"v".to_vec());
        }
        sim.run_for(BURST_GAP);
    }

    let mut caught_up = false;
    for _ in 0..DRAIN_POLLS {
        if nodes[l].learner_caught_up(&learner, CATCH_UP_THRESHOLD) {
            caught_up = true;
            break;
        }
        sim.run_for(DRAIN_POLL_GAP);
        if start.elapsed() >= real_budget {
            // The real-time watchdog fired: the pre-fix pathology is
            // exactly this — unbounded CPU work per round, not just "the
            // learner needs a few more rounds." Stop early rather than
            // burning the whole suite's time budget; report as not caught
            // up, same as running out of the virtual-tick budget.
            break;
        }
    }
    let last_seen_commit = nodes[l].commit_index();
    assert!(
        caught_up,
        "seed={seed}: the learner never caught up to within {CATCH_UP_THRESHOLD} of the \
         leader's log after a sustained per-item writer of {WRITE_BURSTS} bursts x \
         {BURST_LEN} proposes, drained for up to {:.1}s of real time (leader commit_index \
         observed at {last_seen_commit}) — issues #532/#537",
        start.elapsed().as_secs_f64(),
    );
}
