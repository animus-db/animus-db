//! Regression for the compaction-flood follow-up (ADR 0017's 2026-09-27
//! amendment, PR #1047 follow-up): a leader's threshold-triggered
//! compaction must not advance `snapshot_index` past a merely-lagging
//! peer's own `match_index` — see `RaftCore::compaction_floor`
//! (`animus-control/src/raft.rs`) and `apply_and_compact`'s
//! `COMPACT_RETENTION_CAP_ENTRIES` (`animus-cp-data/src/lib.rs`) for the
//! mechanism.
//!
//! Before this fix, compaction was driven purely by the leader's own
//! applied progress (`COMPACT_THRESHOLD = 64`), with no awareness of any
//! peer's actual replication position until a chunked transfer was already
//! in flight. A peer only briefly slow — an fsync stall, a scheduling
//! hiccup, one contended tablet group crowding out another's turn on a
//! node hosting dozens of them — fell off the compacted log every ~64
//! applies, forcing a full `InstallSnapshot` every time. Measured live: a
//! 31-minute run produced 18,816 installs / 4.27M chunks / 1,029 forced
//! transfer restarts and RSS growing ~250 MB/min to OOM — snapshots had
//! become the *normal* catch-up path.
//!
//! Both scenarios below use ordinary VOTERS, never a learner:
//! `RaftCore::compaction_floor` deliberately retains for voters only — see
//! that method's own doc for why a learner's already-snapshot-based catch-up
//! contract (ADR 0058 Train 1) makes it the wrong scope for this floor.
//!
//! Two scenarios:
//! - `a_modestly_slowed_voter_catches_up_via_append_entries_without_ever_
//!   needing_a_snapshot`: a voter with a real, but modest, disk round-trip
//!   cost under a sustained writer must catch up via ordinary
//!   `AppendEntries`, needing at most a small, bounded number of
//!   `InstallSnapshot` installs — nowhere near the flood. **Red on
//!   `claude/snapshot-chunk-flood`** (pre-retention): the pre-fix code
//!   compacts to `engine_applied` on every threshold crossing regardless of
//!   the slow voter's own position, so it repeatedly falls below
//!   `snapshot_index` and needs far more `InstallSnapshot` transfers.
//!   **Green** with the fix: the slow voter's own `match_index` bounds
//!   compaction, so it rarely falls behind `snapshot_index` at all.
//! - `a_voter_partitioned_well_past_the_retention_cap_still_gets_a_
//!   snapshot_and_log_growth_stays_bounded`: a voter that falls further
//!   behind than `COMPACT_RETENTION_CAP_ENTRIES` is excluded from the
//!   floor — the leader's own log growth stays bounded by the cap
//!   throughout, and once the voter is healed it catches up via a real
//!   `InstallSnapshot`, exactly as an unreachable/dead peer always has.

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{Metric, MetricsHandle, nid};
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn leader_among(nodes: &[KvNode]) -> Option<usize> {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    if ls.len() == 1 { Some(ls[0]) } else { None }
}

/// One propose per scheduler turn (never a synchronous batch) — the shape
/// that lets `replicate_now`'s wake-on-propose fire close to once per
/// write, matching the field's own per-write rate (mirrors
/// `snapshot_resend_bound.rs`'s identical choice, see that test's own doc
/// for why a batched burst hides the mechanism).
const ROUNDS: u64 = 1200;
const ROUND_GAP: Duration = Duration::from_millis(1);

/// A voter's disk round trip modest enough that it is never anywhere close
/// to `COMPACT_RETENTION_CAP_ENTRIES` (4096) behind — real contention (a
/// slow fsync, a busy scheduler), not a partition or a crash.
const SLOW_VOTER_SYNC_DELAY: Duration = Duration::from_millis(8);

/// To confirm this test is RED on `claude/snapshot-chunk-flood`, run it
/// with `apply_and_compact`'s `compact_target` temporarily forced back to
/// `ea` (reverting this branch's own retention clamp) — this is exactly
/// what pre-retention `apply_and_compact` did unconditionally on every
/// threshold crossing.
#[test]
fn a_modestly_slowed_voter_catches_up_via_append_entries_without_ever_needing_a_snapshot() {
    let seed = 0x5324_1001;
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
    let slow = (0..3).find(|&i| i != l).expect("a non-leader voter exists");

    // The slow voter's disk carries a real, but modest, round-trip cost —
    // set only AFTER the initial election so it never influences who wins
    // it.
    let mut slow_disk = DiskConfig::default();
    slow_disk.set_sync_delay(SLOW_VOTER_SYNC_DELAY);
    sim.set_disk_config_for(nid(ids[slow]), slow_disk);

    for round in 0..ROUNDS {
        let key = format!("k-{round}").into_bytes();
        let res = nodes[l].put(key, b"v".to_vec());
        assert!(
            matches!(res, ProposeResult::Accepted { .. }),
            "seed={seed}: write {round} must be locally accepted by the leader, got {res:?}"
        );
        sim.run_for(ROUND_GAP);
    }
    // Drain: let the slow voter finish catching up with the writer stopped.
    for _ in 0..200 {
        sim.run_for(Duration::from_millis(100));
        if nodes[slow].engine_applied_index() >= nodes[l].commit_index() {
            break;
        }
    }

    let total_installs: u64 = handles
        .iter()
        .map(|h| h.get(Metric::CpSnapshotInstalls))
        .sum();
    let leader_commit = nodes[l].commit_index();
    let slow_applied = nodes[slow].engine_applied_index();
    let leader_log_len = nodes[l].log_len();

    eprintln!(
        "seed={seed} rounds={ROUNDS}: total_installs={total_installs} \
         leader_commit={leader_commit} slow_applied={slow_applied} leader_log_len={leader_log_len}"
    );

    // A small, bounded number of installs is tolerated (this codebase's
    // separate `state_machine_behind`/`needs_snapshot` machinery, issue
    // #554, can occasionally re-enter the snapshot path while a receiver's
    // own async apply task is still digesting a prior install — an
    // existing interaction, unrelated to retention's own correctness, that
    // an ordinary Raft `next_index` backoff can also trigger). The bound
    // below is what this PR's fix must hold to: nowhere close to the flood
    // (thousands to tens of thousands of installs/chunks for a peer this
    // mildly behind) that motivated it.
    const MAX_TOLERATED_INSTALLS: u64 = ROUNDS / 20;
    assert!(
        total_installs <= MAX_TOLERATED_INSTALLS,
        "seed={seed}: a voter only modestly slowed (sync_delay={SLOW_VOTER_SYNC_DELAY:?}), \
         never partitioned, never crashed, required {total_installs} InstallSnapshot \
         install(s) (bound {MAX_TOLERATED_INSTALLS}) to catch up — compaction must retain \
         enough log for it to catch up via ordinary AppendEntries instead, not repeatedly fall \
         back to full snapshots (ADR 0017's 2026-09-27 amendment)"
    );
    assert!(
        slow_applied + 4 >= leader_commit,
        "seed={seed}: the slow voter never caught up (its own applied index {slow_applied} \
         vs. the leader's commit_index {leader_commit}) — the retention floor must not starve \
         real replication progress, only avoid unnecessary snapshots"
    );
    // Compaction must still be genuinely happening (not merely deferred to
    // infinity because retention always wins) — the leader's own log tail
    // should stay a small, bounded multiple of COMPACT_THRESHOLD, nowhere
    // near ROUNDS.
    assert!(
        leader_log_len < 500,
        "seed={seed}: the leader's own uncompacted log tail grew to {leader_log_len} entries \
         against a merely-slowed (never partitioned) voter — compaction should still be \
         proceeding, bounded by the voter's own (advancing) match_index"
    );
}

/// A peer that falls further behind than `COMPACT_RETENTION_CAP_ENTRIES`
/// (4096) must be excluded from the compaction floor — the leader's own
/// log growth stays bounded by the cap, and once the peer is reachable
/// again it catches up via a real `InstallSnapshot`, exactly as an
/// unreachable/dead peer always has (retention only helps a peer with a
/// genuine chance of catching up cheaply; it must never let a truly-gone
/// peer wedge the leader's own compaction forever — that's PR #1047's own
/// `a_stalled_partitioned_peer_does_not_block_compaction_forever`
/// scenario, which this test complements by also proving the OTHER half:
/// the excluded peer genuinely does get a snapshot and does catch up once
/// reachable, not merely "the leader's log stays bounded").
#[test]
fn a_voter_partitioned_well_past_the_retention_cap_still_gets_a_snapshot_and_log_growth_stays_bounded()
 {
    let seed = 0x5324_1002;
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
    let stuck = (0..3).find(|&i| i != l).expect("a non-leader voter exists");

    // Fully partition this voter from the leader BEFORE any writes — it
    // must never receive a single byte while the leader writes well past
    // the retention cap. (The remaining third voter keeps the leader's
    // writes committing on its own.)
    sim.partition_pair(nid(ids[l]), nid(ids[stuck]));

    // Comfortably past COMPACT_RETENTION_CAP_ENTRIES (4096) — the stuck
    // voter's match_index reads 0 the whole time (never acked anything), so
    // it must be excluded from the floor well before this loop ends.
    const ROUNDS: u64 = 4600;
    for round in 0..ROUNDS {
        let key = format!("k-{round}").into_bytes();
        let _ = nodes[l].put(key, b"v".to_vec());
        sim.run_for(Duration::from_millis(1));
    }
    // Give the idle ceiling room too, so compaction has definitely run.
    sim.run_for(Duration::from_secs(3));

    let log_len_while_partitioned = nodes[l].log_len();
    eprintln!("seed={seed} rounds={ROUNDS}: log_len_while_partitioned={log_len_while_partitioned}");
    assert!(
        log_len_while_partitioned < 1000,
        "seed={seed}: the leader's own uncompacted log tail grew to \
         {log_len_while_partitioned} entries against a voter partitioned well past the \
         retention cap — it must be excluded from the floor and compaction must proceed \
         regardless"
    );

    // Heal — the excluded voter must now catch up via a genuine
    // InstallSnapshot (its own log start is long gone).
    sim.heal(nid(ids[l]), nid(ids[stuck]));
    sim.heal(nid(ids[stuck]), nid(ids[l]));
    let mut caught_up = false;
    for _ in 0..600 {
        sim.run_for(Duration::from_millis(50));
        if handles[stuck].get(Metric::CpSnapshotInstalls) > 0
            && nodes[stuck].engine_applied_index() > 0
        {
            caught_up = true;
            break;
        }
    }
    let total_installs_after_heal: u64 = handles
        .iter()
        .map(|h| h.get(Metric::CpSnapshotInstalls))
        .sum();

    eprintln!(
        "seed={seed}: caught_up={caught_up} total_installs_after_heal={total_installs_after_heal}"
    );
    assert!(
        caught_up && total_installs_after_heal > 0,
        "seed={seed}: the excluded, now-reconnected voter never received an InstallSnapshot \
         (total_installs_after_heal={total_installs_after_heal}) — a peer this far behind must \
         still be able to catch up via a real snapshot, not be stuck forever just because \
         retention exists"
    );
}
