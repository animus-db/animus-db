//! Issue #898 (recurrence of #741's mechanism, one layer deeper): a follower
//! catching up via chunked `InstallSnapshot` must not stall forever when the
//! leader's own apply task keeps re-crossing `SNAPSHOT_THRESHOLD` (sustained
//! metadata churn) WHILE that follower's transfer is still in flight.
//!
//! Two compounding gaps, both fixed here:
//!
//! 1. `RaftCore::snapshot_upto` unconditionally invalidates every peer's
//!    in-flight chunked transfer the moment the snapshot base moves again (a
//!    `DRIVER_APPLIED` state machine's blob is rebuilt lazily, so a moved
//!    base drops the stale image and clears `snapshot_offset`/
//!    `snapshot_chunk_sent` for every peer — see that method's own doc).
//!    `animus-cp-data`'s `apply_and_compact` already defers a
//!    THRESHOLD-triggered (never an `image_needed`-triggered) compaction
//!    while `RaftCore::snapshot_transfer_in_flight()` is true, up to
//!    `COMPACT_DEFER_CEILING` (see that constant's own doc: "confirmed
//!    live: a learner's own `match_index` pinned for an entire run while
//!    the leader's log kept growing"). `animus-control`'s
//!    `meta_apply_and_compact` — despite its own doc claiming it "mirrors
//!    `animus-cp-data::apply_and_compact`'s shape/ordering precisely" —
//!    never adopted this defer, even though the core-level primitive it
//!    needs (`snapshot_transfer_in_flight`) already exists and is already
//!    referenced by this very crate's own doc comments (`raft.rs`'s
//!    `SnapshotResend` doc cites `COMPACT_DEFER_CEILING` as an established
//!    concept). Fixed by adding `SNAPSHOT_COMPACT_DEFER_CEILING` and the
//!    matching gate to `meta_apply_and_compact`, mirroring the data plane.
//!
//! 2. Adding that gate ALONE still did not fix this test: `RaftCore::
//!    snapshot_transfer_in_flight()` was defined purely in terms of
//!    `snapshot_offset`, which is populated only once a peer's FIRST ack is
//!    processed — not the moment a chunk is actually sent
//!    (`snapshot_chunk_for` records that in `snapshot_chunk_sent` instead).
//!    That leaves a real window, from "leader ships chunk 0" to "leader
//!    processes the first ack" (stretchable arbitrarily far by a slow link
//!    or a contended peer — exactly `ProdEnv`'s real-thread CI shape), during
//!    which a transfer is genuinely on the wire but this accessor reports
//!    `false`, so a threshold crossing landing in that window still
//!    invalidates it — the defer gate never engages during the window it
//!    matters most. Fixed by widening `snapshot_transfer_in_flight` to also
//!    check `snapshot_chunk_sent`; both maps are already cleared together at
//!    every existing invalidation/completion point, so this is a strictly
//!    more accurate reading of the same "is a chunk genuinely outstanding"
//!    fact, benefiting `animus-cp-data` for free (same shared `RaftCore`).
//!
//! This file's own seed reproduces the stall deterministically under
//! `SimEnv` with only fix 1 applied (an artificially slow leader<->follower
//! link stands in for `ProdEnv`'s real contention, widening the send-to-ack
//! window enough for a small, bounded churn burst to land inside it), and
//! passes with both fixes in place.
//!
//! **This same seed later caught a rejected fix to a THIRD, separate gap**
//! (issue #898 follow-up, `node.rs`'s `SNAPSHOT_COMPACT_DEFER_TIME_CEILING`):
//! an early attempt gated `snapshot_transfer_in_flight` on a per-peer
//! heartbeat-resend COUNT (give up deferring for a peer once its un-acked
//! resend count crossed a threshold), meant to stop a never-acking peer from
//! wedging compaction forever. This test's own deliberately-slow link needed
//! ~40 heartbeat-driven resends before its peer's first-EVER ack — a real,
//! healthy, merely-slow transfer — landing right at the chosen threshold and
//! reopening the exact stall fixes 1+2 above close. The shipped fix instead
//! bounds elapsed **time** (`env.now()`, tracked by the apply-loop driver,
//! not `RaftCore` itself), leaving `snapshot_transfer_in_flight` unchanged;
//! see `SNAPSHOT_COMPACT_DEFER_TIME_CEILING`'s own doc and `docs/lessons/
//! testing/2026-09-14-control-snapshot-catch-up-stall.md` for the full
//! account of why a resend count is the wrong proxy for elapsed time.

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::{MetaCommand, NodeStatus, RaftNode};
use animus_env::{Env, NodeId, nid};
use animus_sim::{NetConfig, Simulator};
use animus_storage::MemoryEngine;

fn upsert(node: u64, tag: u64) -> MetaCommand {
    let mut labels = BTreeMap::new();
    labels.insert("tag".to_string(), tag.to_string());
    MetaCommand::UpsertMember {
        node: nid(node),
        labels,
        status: NodeStatus::Joining,
    }
}

/// A fresh follower (started dark, well after the leader has already
/// compacted its log) must catch up to the leader's compacted state within a
/// bounded amount of virtual time, even while the leader keeps taking on new
/// commands (and therefore keeps re-triggering `SNAPSHOT_THRESHOLD`
/// compactions) throughout the catch-up window.
#[test]
fn late_follower_catches_up_despite_sustained_recompaction() {
    let seed = 0x_5714_C0DE;
    let mut sim = Simulator::new(seed);
    let group: Vec<NodeId> = vec![nid(0), nid(1), nid(2)];

    let node0 = RaftNode::start(sim.env(nid(0)), group.clone(), MemoryEngine::new());
    let node1 = RaftNode::start(sim.env(nid(1)), group.clone(), MemoryEngine::new());
    sim.run_for(Duration::from_secs(1)); // elect

    let leader = if node0.is_leader() { &node0 } else { &node1 };

    // Get the leader well past one compaction (so a fresh follower needs an
    // `InstallSnapshot`, not a plain log replay).
    for i in 0..300u64 {
        leader.propose(upsert(200 + i, i));
    }
    sim.run_for(Duration::from_secs(2));
    assert!(
        leader.snapshot_index() > 0,
        "leader should have compacted at least once before node 2 joins"
    );

    // Node 2 joins dark, late — it needs a chunked `InstallSnapshot`. While it
    // catches up, keep feeding the leader fresh commands in small virtual-time
    // slices, so its apply task keeps re-crossing `SNAPSHOT_THRESHOLD` (and
    // therefore keeps invalidating any in-flight transfer to node 2) for the
    // whole window — the sustained-churn shape that raced `COMPACT_DEFER_CEILING`
    // into existence in the data plane.
    // Slow the link in both directions between the leader and node 2 (well
    // past a single `SNAPSHOT_THRESHOLD`-crossing round of churn below) so a
    // chunk round trip takes noticeably longer than one churn round —
    // without this, SimEnv's near-zero-latency default lets a small
    // synthetic transfer complete faster than churn can re-cross the
    // threshold, and the race this test targets never gets a chance to bite.
    let leader_id = leader.env().node_id();
    let mut slow = NetConfig::default();
    slow.base_delay = Duration::from_millis(120);
    slow.max_jitter = Duration::from_millis(20);
    sim.set_link_net_config(leader_id.clone(), nid(2), slow.clone());
    sim.set_link_net_config(nid(2), leader_id, slow);

    let node2 = RaftNode::start(sim.env(nid(2)), group.clone(), MemoryEngine::new());
    // A brief burst of additional churn right as node 2 starts catching up —
    // this is the realistic shape (a handful of trailing commits landing
    // while a freshly-joined follower's very first transfer is still in
    // flight, not perpetual load for the whole catch-up window): each burst
    // is well under `SNAPSHOT_COMPACT_DEFER_CEILING`'s own multiple of
    // `SNAPSHOT_THRESHOLD`, so a correct defer should absorb every one of
    // them without ever invalidating node 2's transfer, while the unfixed
    // (undeferred) code invalidates it on every single burst.
    let target = leader.snapshot_index();
    for round in 0..6u64 {
        for i in 0..40u64 {
            leader.propose(upsert(600 + round * 40 + i, round));
        }
        sim.run_for(Duration::from_millis(60));
        if node2.snapshot_index() >= target || node2.last_applied() >= target {
            break;
        }
    }
    // Give any final in-flight transfer room to land once the churn stops.
    sim.run_for(Duration::from_secs(3));

    assert!(
        node2.snapshot_index() >= target || node2.last_applied() >= target,
        "node 2 never caught up under sustained recompaction: node2 snapshot_index={} \
         last_applied={} engine_applied={}, leader snapshot_index={} (last observed target \
         {target}) — a threshold-triggered compaction is invalidating node 2's in-flight \
         InstallSnapshot transfer faster than it can complete (issue #898: missing \
         compaction defer in meta_apply_and_compact, and/or snapshot_transfer_in_flight's \
         send-to-first-ack gap)",
        node2.snapshot_index(),
        node2.last_applied(),
        node2.engine_applied_index(),
        leader.snapshot_index(),
    );
}
