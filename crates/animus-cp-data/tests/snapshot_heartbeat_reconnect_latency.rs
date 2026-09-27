//! Regression for the review finding on `SnapshotResend::Backoff`'s first
//! cut (`animus-control/src/raft.rs`): exponential doubling with **no
//! ceiling** bounds total resend volume within any fixed window (it's
//! merely logarithmic, which is why `snapshot_heartbeat_resend_unbounded.rs`
//! stays green either way — see that test's own module doc), but it does
//! **not** bound the GAP between consecutive resends, which keeps doubling
//! right alongside the count. A peer stuck for a long stretch can end up
//! with its next scheduled chunk a very long time away — and, since a
//! snapshot-mode peer's outstanding chunk is its ONLY leader-liveness
//! signal (`replicate_to` never sends a plain `AppendEntries` while
//! `next_index <= snapshot_index`), that same long gap is also how long it
//! can take to resume catch-up once a genuinely stuck-but-reachable peer
//! reconnects — up to another full gap-length wait, not merely "the next
//! heartbeat."
//!
//! This test partitions a follower after forcing it into chunked
//! `InstallSnapshot` mode (same setup as `snapshot_heartbeat_resend_
//! unbounded.rs`), lets it sit stuck for several minutes of idle virtual
//! time (long enough that an uncapped doubling schedule's own next
//! scheduled resend is far in the future the instant we heal), heals the
//! partition, and asserts the follower finishes catching up within a small,
//! bounded window afterward. A ceiling that flattens the schedule into a
//! steady resend (`SNAPSHOT_HEARTBEAT_BACKOFF_MAX_TICKS`) bounds this wait
//! to that steady period regardless of how long the stall ran; the
//! uncapped, ever-doubling schedule does not.

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{NodeId, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn leader_among(nodes: &[KvNode]) -> Option<usize> {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    if ls.len() == 1 { Some(ls[0]) } else { None }
}

/// How long the follower sits partitioned, deep in chunked-snapshot mode,
/// before healing. Several minutes of virtual time — long enough that an
/// UNCAPPED exponential-doubling schedule's own next scheduled resend, at
/// the moment we heal, is (for this fixed seed) many multiples of the
/// bounded design's own steady-state period away.
const STUCK_WINDOW: Duration = Duration::from_secs(300);

/// After healing, a genuinely bounded resend mechanism must resume shipping
/// chunks (and the follower must finish catching up) within this long —
/// comfortably above `SNAPSHOT_HEARTBEAT_BACKOFF_MAX_TICKS`'s own ~1.6s
/// steady-state period (with slack for however many chunks the transfer
/// itself needs) but far below what an uncapped doubling schedule can leave
/// outstanding after a multi-minute stall.
const CATCH_UP_BUDGET: Duration = Duration::from_secs(10);

/// Poll in small increments up to `CATCH_UP_BUDGET`, returning `true` the
/// moment the follower's own applied index reaches the leader's.
fn catches_up_within_budget(
    sim: &mut Simulator,
    nodes: &[KvNode],
    leader: usize,
    follower: usize,
) -> bool {
    let step = Duration::from_millis(50);
    let mut waited = Duration::ZERO;
    while waited < CATCH_UP_BUDGET {
        sim.run_for(step);
        waited += step;
        if nodes[follower].engine_applied_index() == nodes[leader].engine_applied_index() {
            return true;
        }
    }
    false
}

#[test]
fn partitioned_snapshot_mode_voter_resumes_catch_up_promptly_after_reconnect() {
    let seed = 0x5324_0005;
    let mut sim = Simulator::new(seed);
    let ids = [0u64, 1, 2];
    let voters: Vec<NodeId> = ids.iter().copied().map(nid).collect();
    let nodes: Vec<KvNode> = ids
        .iter()
        .map(|&id| RaftKvNode::start(sim.env(nid(id)), voters.clone(), MemoryEngine::new()))
        .collect();
    sim.run_for(Duration::from_secs(2));
    let l = leader_among(&nodes).expect("an initial leader");
    let stuck = (l + 1) % 3;
    let stuck_id = nid(ids[stuck]);

    // Partition the doomed follower FIRST, then write enough to force the
    // leader's log past `COMPACT_THRESHOLD` while it's cut off — so once
    // healed, it can only ever catch up via a chunked `InstallSnapshot`.
    sim.partition_pair(nid(ids[l]), stuck_id.clone());
    for b in 0..20u64 {
        for i in 0..10u64 {
            let key = format!("k-{b}-{i}").into_bytes();
            assert!(
                matches!(
                    nodes[l].put(key, b"v".to_vec()),
                    ProposeResult::Accepted { .. }
                ),
                "seed={seed}: write {b}-{i} must be locally accepted (majority is the other two)"
            );
        }
        sim.run_for(Duration::from_millis(10));
    }

    assert!(
        nodes[l].snapshot_chunk_advances(&stuck_id) > 0,
        "seed={seed}: the partitioned peer never even started needing a snapshot — \
         test setup didn't force the chunked path"
    );
    assert_ne!(
        nodes[stuck].engine_applied_index(),
        nodes[l].engine_applied_index(),
        "seed={seed}: sanity — the partitioned peer must genuinely be behind before we \
         measure its reconnect latency, or this test proves nothing"
    );

    // Sit stuck, deep in chunked-snapshot backoff, for a long idle window —
    // nothing here can make genuine progress (still partitioned), so
    // whatever `SnapshotResend::Backoff` schedule is running keeps
    // advancing its own per-offset attempt count the whole time.
    sim.run_for(STUCK_WINDOW);
    assert_ne!(
        nodes[stuck].engine_applied_index(),
        nodes[l].engine_applied_index(),
        "seed={seed}: the partitioned peer must still be behind right before we heal, or \
         this test isn't exercising a multi-minute stall at all"
    );

    // Heal — this follower is now genuinely reachable again. The property
    // under test: catch-up must resume within a small, bounded window, not
    // whenever an ever-doubling schedule's own next resend happens to fall.
    sim.heal(nid(ids[l]), stuck_id.clone());
    sim.heal(stuck_id, nid(ids[l]));

    assert!(
        catches_up_within_budget(&mut sim, &nodes, l, stuck),
        "seed={seed}: the reconnected peer did not finish catching up within \
         {CATCH_UP_BUDGET:?} of healing a {STUCK_WINDOW:?} partition — an uncapped, \
         ever-doubling `SnapshotResend::Backoff` schedule can leave its next scheduled \
         resend an unboundedly long time away after a long stall, so reconnecting doesn't \
         help until that far-off resend finally comes due; a schedule that flattens into a \
         bounded steady-state period resumes promptly regardless of how long the stall ran"
    );
}
