//! Regression for an unbounded-**over-time** `InstallSnapshot` chunk-resend
//! flood, distinct from `snapshot_resend_bound.rs`'s own per-offset-window
//! bound. `RaftCore::tick`'s heartbeat branch resends a peer's still-
//! outstanding chunk under `SnapshotResend::Always`
//! (`animus-control/src/raft.rs`, "Heartbeat cadence (write-rate-
//! independent) is one of the bounded retries a genuinely stuck snapshot
//! chunk gets — always allowed") — and, unlike every other
//! `SnapshotResend::Capped` call site, `Always` has **no ceiling on the
//! total number of times it may resend one unchanged offset**. For a peer
//! that can make genuine progress this is harmless (the offset soon
//! advances, restarting the count). But for a peer that can **never**
//! finish — a real network partition, a permanently wedged disk, or (per
//! this crate's own `apply_and_compact` doc) a sustained write rate that
//! genuinely exceeds what the peer can ever absorb — the leader resends the
//! SAME chunk every single heartbeat interval **forever**, for as long as
//! the peer stays stuck: total volume for that one peer grows linearly,
//! without bound, in elapsed real/virtual time, not just per genuine
//! offset advance. Aggregated over many concurrently-stuck tablet groups
//! (an auto-splitting cluster under heavy write load, ADR 0034/0058) this
//! is exactly the field's own live symptom: `cp_snapshot_ships` climbing
//! far faster than `cp_commits`/genuine snapshot installs, with process
//! memory climbing alongside it (every resend is a fresh `Vec<u8>` chunk
//! queued for send).
//!
//! This test partitions a follower's network link **after** it has already
//! fallen behind the leader's compacted log (so it genuinely needs a
//! chunked `InstallSnapshot`, not merely a caught-up `AppendEntries`), then
//! lets the cluster idle — no further writes, nothing that could make
//! `resends_so_far` advance for a fresh offset — for a long virtual-time
//! window and measures `Metric::CpSnapshotShips` in two successive halves
//! of that window. A bounded mechanism ships close to zero more chunks in
//! the second half once its cap is exhausted; the unbounded `Always`
//! heartbeat path keeps shipping at essentially the same rate in both
//! halves, since nothing ever silences it.

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{Metric, MetricsHandle, NodeId, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn leader_among(nodes: &[KvNode]) -> Option<usize> {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    if ls.len() == 1 { Some(ls[0]) } else { None }
}

/// How many chunk-worthy resends a genuinely bounded mechanism should have
/// mostly exhausted well before this much idle virtual time has passed —
/// `SNAPSHOT_ACK_RESEND_CAP` (8) plus a handful of heartbeat-interval
/// (50ms) ticks is a few hundred ms; this window is two full orders of
/// magnitude past that, so any resend mechanism that is genuinely bounded
/// per stuck episode has long since gone quiet, while an `Always`,
/// unbounded-over-time one keeps ticking at essentially the same rate
/// throughout.
const HALF_WINDOW: Duration = Duration::from_secs(30);

/// A bounded mechanism must ship no more than this many chunks in the
/// SECOND half of the idle window (well past any legitimate warm-up/first-
/// contact resend burst in the first half). An unbounded `Always`-only
/// heartbeat resend ships roughly `HALF_WINDOW / heartbeat_interval` (~600
/// at the 50ms default) in the second half alone, far over this bound.
const MAX_SECOND_HALF_SHIPS: u64 = 60;

#[test]
fn install_snapshot_heartbeat_resend_does_not_grow_unbounded_with_idle_time() {
    let seed = 0x5324_0004;
    let mut sim = Simulator::new(seed);
    let ids = [0u64, 1, 2];
    let handles: Vec<MetricsHandle> = ids.iter().map(|_| MetricsHandle::recording()).collect();
    let voters: Vec<NodeId> = ids.iter().copied().map(nid).collect();
    let nodes: Vec<KvNode> = ids
        .iter()
        .enumerate()
        .map(|(i, &id)| {
            RaftKvNode::start_with_metrics(
                sim.env(nid(id)),
                voters.clone(),
                MemoryEngine::new(),
                handles[i].clone(),
            )
        })
        .collect();
    sim.run_for(Duration::from_secs(2));
    let l = leader_among(&nodes).expect("an initial leader");
    let stuck = (l + 1) % 3;
    let stuck_id = nid(ids[stuck]);

    // Partition the doomed follower FIRST, then write enough to force the
    // leader's log past `COMPACT_THRESHOLD` while it's cut off — so once
    // healed-from-the-follower's-own-perspective-never, it can only ever
    // catch up via a chunked `InstallSnapshot`, never plain `AppendEntries`.
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

    // Confirm the leader has in fact registered this peer as needing (or
    // already mid-) a snapshot transfer before measuring — otherwise the
    // test would prove nothing.
    assert!(
        nodes[l].snapshot_chunk_advances(&stuck_id) > 0
            || handles[l].get(Metric::CpSnapshotShips) > 0,
        "seed={seed}: the partitioned peer never even started needing a snapshot — \
         test setup didn't force the chunked path"
    );

    // First half of the idle window: whatever legitimate first-contact
    // resend burst happens, happens here.
    let before_first_half = handles[l].get(Metric::CpSnapshotShips);
    sim.run_for(HALF_WINDOW);
    let after_first_half = handles[l].get(Metric::CpSnapshotShips);

    // Second half: nothing new can possibly make genuine progress (the
    // follower is still unreachable), so any further shipping is pure
    // resend of the identical stuck offset.
    sim.run_for(HALF_WINDOW);
    let after_second_half = handles[l].get(Metric::CpSnapshotShips);

    let first_half_ships = after_first_half.saturating_sub(before_first_half);
    let second_half_ships = after_second_half.saturating_sub(after_first_half);
    assert!(
        second_half_ships <= MAX_SECOND_HALF_SHIPS,
        "seed={seed}: {second_half_ships} InstallSnapshot chunks shipped to a permanently \
         unreachable peer in the second {HALF_WINDOW:?} of an idle window alone (first half: \
         {first_half_ships}), bound {MAX_SECOND_HALF_SHIPS} — the heartbeat-tick resend path \
         has no ceiling on total resends of an unchanged offset, so it keeps shipping at \
         essentially the full heartbeat rate for as long as the peer stays stuck, unbounded \
         in elapsed time",
    );
}
