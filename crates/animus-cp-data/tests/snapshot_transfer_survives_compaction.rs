//! Regression for PR #1047: a threshold-triggered compaction must not
//! force a still-*advancing* chunked `InstallSnapshot` transfer back to
//! chunk 0 just because `behind` crossed a fixed, small ceiling.
//!
//! `RaftCore::snapshot_upto` (`animus-control/src/raft.rs`) unconditionally
//! invalidates every peer's in-flight transfer the moment the snapshot base
//! moves again (required for correctness against a lazily-built
//! `DRIVER_APPLIED` image — see that method's own doc). Before this fix,
//! `apply_and_compact`'s own defer gate (`lib.rs`) forced the base forward
//! — restarting any in-flight transfer from chunk 0 — the moment `behind`
//! reached `COMPACT_DEFER_CEILING` (`COMPACT_THRESHOLD * 8` = 512),
//! *regardless of whether that transfer was genuinely making progress*.
//! Under sustained writes fast enough relative to a slow/contended peer's
//! own round trip (a write roughly every millisecond against a peer with a
//! ~200ms disk round trip is enough — modeling many hosted tablet groups
//! contending for one node's CPU, the field shape), `behind` re-crossed 512
//! long before a real multi-chunk transfer could land, so the transfer
//! restarted from chunk 0 forever: tens of thousands of chunk ships, zero
//! completed installs, the learner's `match_index` pinned for the entire
//! run.
//!
//! The fix (`lib.rs`'s `COMPACT_DEFER_EMERGENCY_CEILING`/
//! `COMPACT_DEFER_IDLE_CEILING`): a transfer that keeps making genuine
//! forward progress (`RaftCore::snapshot_chunk_advances` changing pass over
//! pass) is never forced out by `behind` alone any more — only sitting IDLE
//! (no progress at all) for `COMPACT_DEFER_IDLE_CEILING` (2s) can force a
//! still-in-flight transfer out early, with a much higher `behind`-sized
//! ceiling (`COMPACT_THRESHOLD * 64` = 4096) as the last-resort WAL bound
//! for the pathological "makes just enough progress to keep resetting the
//! idle clock, but never lands" case.
//!
//! Two scenarios:
//! - `snapshot_transfer_lands_under_sustained_writes_and_a_slow_learner`:
//!   the slow-but-live peer above must actually catch up, with a bounded
//!   number of forced restarts (`Metric::CpSnapshotTransferRestarts`) and a
//!   bounded ships-per-genuine-advance ratio (the flood signature is a huge
//!   ratio with zero installs).
//! - `a_stalled_partitioned_peer_does_not_block_compaction_forever`: a
//!   peer that never acks *at all* (fully partitioned) must still not wedge
//!   this leader's own compaction forever — its log/WAL must stay bounded
//!   even though the "peer" never makes any progress and never catches up.
//!
//! Asserted RED against `origin/claude/snapshot-chunk-flood` (pre-fix
//! `threshold_hit` gate — `behind >= COMPACT_DEFER_CEILING` alone, no
//! progress awareness): the first scenario's learner never caught up
//! (`caught_up == false`), `CpSnapshotInstalls` stayed at `0`, and
//! `CpSnapshotTransferRestarts` (backported for the RED measurement) ran
//! into the thousands over the write window. GREEN with the fix in place.

use std::time::{Duration, Instant};

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{Metric, MetricsHandle, NodeId, nid};
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

/// A sustained per-item writer (roughly one propose per virtual
/// millisecond, the field repro's own rate) against a learner whose disk
/// round trip is slow enough (200ms) to model heavy real contention across
/// many hosted tablet groups on one node. Before the fix, `behind` re-crossed
/// the old fixed 512-entry ceiling long before a real multi-chunk transfer
/// to this peer could land, so `snapshot_upto` restarted it from chunk 0
/// over and over — the learner never caught up. After the fix, a
/// genuinely-advancing transfer is no longer forced out by `behind` alone,
/// so it gets a real chance to land.
#[test]
fn snapshot_transfer_lands_under_sustained_writes_and_a_slow_learner() {
    let seed = 0x000F_100D_0001;
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

    // Warm the log so it is already nontrivially long — a fresh learner
    // cannot catch up on it via plain `AppendEntries` alone, forcing the
    // chunked `InstallSnapshot` path this test exercises.
    for b in 0..8u64 {
        for i in 0..10u64 {
            let key = format!("warm-{b}-{i}").into_bytes();
            assert!(
                matches!(
                    nodes[l].put(key, vec![b'v'; 256]),
                    ProposeResult::Accepted { .. }
                ),
                "seed={seed}: warm-up write {b}-{i} must be locally accepted"
            );
        }
        sim.run_for(Duration::from_millis(10));
    }

    // The learner's own disk carries a large round-trip cost — modeling a
    // node under heavy real CPU contention across many tablet groups
    // (30-90 groups/node, single-threaded consensus loop per node, the
    // field evidence PR #1047 was opened against).
    let mut learner_disk = DiskConfig::default();
    learner_disk.set_sync_delay(Duration::from_millis(200));
    sim.set_disk_config_for(nid(3), learner_disk);

    let voters: Vec<NodeId> = ids.iter().copied().map(nid).collect();
    let learner = nid(3);
    // The learner's own metrics matter too: `CpSnapshotInstalls` is recorded
    // on the RECEIVING side (the follower that just finished installing —
    // see that `Metric`'s own doc), not the leader that shipped the chunks.
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
        "seed={seed}: add_learner must be accepted by the leader"
    );

    // A bounded, sustained write window — long enough for the pre-fix flood
    // to run for thousands of forced restarts, short enough to keep the
    // suite fast. Writes are issued in bursts between `sim.run_for` calls,
    // never one `run_for` per propose — `run_for` itself carries real
    // per-call overhead unrelated to the mechanism under test (profiled by
    // this crate's `learner_catchup_under_load.rs` sibling: thousands of
    // individual single-propose `run_for` calls cost tens of seconds of
    // real time from call overhead alone; an equivalent-throughput batched
    // shape costs a fraction of that). `BURST_LEN`/`BURST_GAP`'s ratio
    // matches the field's own sustained per-write rate.
    const BURST_LEN: u64 = 10;
    const BURST_GAP: Duration = Duration::from_millis(10);
    const WRITE_BURSTS: u64 = 400;
    #[allow(
        clippy::disallowed_methods,
        reason = "real-time watchdog against unbounded per-round CPU work \
                  (the pre-fix flood itself) — SimEnv's virtual clock cannot see this"
    )]
    let start = Instant::now();
    let real_budget = Duration::from_secs(90);
    'write: for burst in 0..WRITE_BURSTS {
        for i in 0..BURST_LEN {
            let key = format!("k-{burst}-{i}").into_bytes();
            let _ = nodes[l].put(key, vec![b'v'; 256]);
        }
        sim.run_for(BURST_GAP);
        if start.elapsed() >= real_budget {
            break 'write;
        }
    }

    // Drain: poll for convergence with the writer stopped (the
    // converged-or-timeout idiom, root `CLAUDE.md`), bounded on both a
    // virtual-tick budget and the same real-time watchdog.
    // The learner's own disk round trip (200ms) is 20x the sibling test's
    // (10ms), so the drain needs proportionally more virtual time to let a
    // genuinely-progressing (never-restarted) transfer actually land —
    // `SimEnv`'s real per-call overhead is what the watchdog bounds, not
    // the amount of virtual time simulated, so this costs little real time.
    let mut caught_up = false;
    for _ in 0..3000 {
        if nodes[l].learner_caught_up(&learner, CATCH_UP_THRESHOLD) {
            caught_up = true;
            break;
        }
        sim.run_for(Duration::from_millis(100));
        if start.elapsed() >= real_budget {
            break;
        }
    }

    let total_ships: u64 = handles
        .iter()
        .map(|h| h.get(Metric::CpSnapshotShips))
        .sum::<u64>()
        + learner_metrics.get(Metric::CpSnapshotShips);
    let total_installs: u64 = handles
        .iter()
        .map(|h| h.get(Metric::CpSnapshotInstalls))
        .sum::<u64>()
        + learner_metrics.get(Metric::CpSnapshotInstalls);
    let total_restarts: u64 = handles
        .iter()
        .map(|h| h.get(Metric::CpSnapshotTransferRestarts))
        .sum::<u64>()
        + learner_metrics.get(Metric::CpSnapshotTransferRestarts);
    let advances = nodes[l].snapshot_chunk_advances(&learner);
    let last_seen_commit = nodes[l].commit_index();

    eprintln!(
        "seed={seed} rounds={WRITE_BURSTS}x{BURST_LEN}: total_ships={total_ships} total_installs={total_installs} \
         genuine_advances={advances} transfer_restarts={total_restarts} caught_up={caught_up}",
    );

    assert!(
        caught_up,
        "seed={seed}: the learner never caught up to within {CATCH_UP_THRESHOLD} of the \
         leader's log after a sustained per-item writer of {WRITE_BURSTS}x{BURST_LEN} writes, drained for up \
         to {:.1}s of real time (leader commit_index observed at {last_seen_commit}, \
         total_ships={total_ships}, total_installs={total_installs}, \
         transfer_restarts={total_restarts}) — PR #1047",
        start.elapsed().as_secs_f64(),
    );
    assert!(
        total_installs > 0,
        "seed={seed}: the learner caught up (checked above) but no InstallSnapshot transfer \
         was ever recorded as completed (total_installs={total_installs}) — a suspicious \
         result worth investigating on its own, not the flood this test targets"
    );
    // The flood signature: many forced restarts, few/no completed
    // installs. A small, bounded number of restarts is expected and fine
    // (e.g. one while the leader's own log was still warming, before the
    // learner ever engaged the chunked path) — thousands, as the pre-fix
    // code produced, is not.
    assert!(
        total_restarts <= 5,
        "seed={seed}: {total_restarts} forced snapshot-transfer restarts recorded (a \
         genuinely-advancing transfer should not be forced back to chunk 0 by `behind` alone \
         any more) — PR #1047's flood signature"
    );
    // Every chunk shipped per genuine offset advance should stay small —
    // the pre-fix flood shipped the SAME unacked offset over and over
    // (confirmed live: tens of thousands of ships against a handful of
    // real advances).
    if let Some(ships_per_advance) = total_ships.checked_div(advances) {
        assert!(
            ships_per_advance <= 20,
            "seed={seed}: {ships_per_advance} chunk ships per genuine offset advance \
             ({total_ships} ships / {advances} advances) — PR #1047's flood signature"
        );
    }
}

/// A peer that never acks *at all* (fully, symmetrically partitioned before
/// it ever joins) must still not wedge this leader's own compaction
/// forever. `COMPACT_DEFER_IDLE_CEILING` (2s of no forward progress) is the
/// mechanism that must fire here — `behind` alone growing past the old,
/// now-much-higher `COMPACT_DEFER_EMERGENCY_CEILING` would take far longer
/// under this write rate than the idle ceiling, so this specifically proves
/// the idle path, not the emergency one.
#[test]
fn a_stalled_partitioned_peer_does_not_block_compaction_forever() {
    let seed = 0x000F_100D_0002;
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

    for b in 0..8u64 {
        for i in 0..10u64 {
            let key = format!("warm-{b}-{i}").into_bytes();
            let _ = nodes[l].put(key, vec![b'v'; 256]);
        }
        sim.run_for(Duration::from_millis(10));
    }

    let voters: Vec<NodeId> = ids.iter().copied().map(nid).collect();
    let learner = nid(3);
    let _node3 = RaftKvNode::start(sim.env(learner.clone()), voters, MemoryEngine::new());

    // Fully partition the learner from every voter BEFORE it is ever added,
    // so it never receives a single byte of the transfer it is about to be
    // promised — the "down, partitioned, or never started" case.
    for &id in &ids {
        sim.partition_pair(nid(id), learner.clone());
    }

    assert!(matches!(
        nodes[l].add_learner(learner.clone()),
        ProposeResult::Accepted { .. }
    ));

    // Sustained writes for a bounded window with the "peer" permanently
    // unreachable — this is exactly the shape that must not let the
    // leader's own log/WAL grow without bound.
    const ROUNDS: u64 = 4000;
    for round in 0..ROUNDS {
        let key = format!("k-{round}").into_bytes();
        let _ = nodes[l].put(key, vec![b'v'; 256]);
        sim.run_for(Duration::from_millis(1));
    }
    // Give the idle ceiling (2s) room to fire on the final stretch too.
    sim.run_for(Duration::from_secs(3));

    let log_len = nodes[l].log_len();
    let snapshot_index = nodes[l].snapshot_index();
    let commit_index = nodes[l].commit_index();

    eprintln!(
        "seed={seed} rounds={ROUNDS}: log_len={log_len} snapshot_index={snapshot_index} \
         commit_index={commit_index}"
    );

    // The uncompacted tail must stay a small, bounded multiple of the
    // compaction threshold — not proportional to `ROUNDS` (4000 writes).
    // A generous bound (well above the emergency ceiling) still catches an
    // unbounded-growth regression while leaving headroom for the idle
    // ceiling's own timing slack.
    assert!(
        log_len < 1000,
        "seed={seed}: leader's own uncompacted log tail grew to {log_len} entries against a \
         permanently partitioned peer (snapshot_index={snapshot_index}, \
         commit_index={commit_index}) — compaction must proceed despite a stalled transfer \
         (COMPACT_DEFER_IDLE_CEILING), never wedge on it forever — PR #1047"
    );
    assert!(
        snapshot_index > 0,
        "seed={seed}: the leader's own snapshot base never advanced at all against a \
         permanently partitioned peer — compaction is wedged"
    );
}
