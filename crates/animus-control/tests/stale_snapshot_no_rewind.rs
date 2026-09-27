//! Regression: a **stale** `InstallSnapshot` transfer — one whose
//! `last_index` the receiver has already committed and applied past, via
//! ordinary `AppendEntries` replication that outran a slow/duplicated
//! snapshot transfer — must never be installed. Installing it rewinds
//! `last_applied`/`commit_index`/the log back down to the stale
//! `last_index`, and the apply task then re-plays already-applied entries,
//! which is exactly what tripped `animus-cp-data`'s `assert_ts_monotonic`
//! live: a follower that had already applied through index 162 installed a
//! stale snapshot at index 160, then re-applied index 161's (older) HLC
//! timestamp on top of the high-water mark index 162 had already set.
//!
//! `RaftCore::handle_install_snapshot`'s "already at least this far along"
//! short-circuit used to compare the offer's `last_index` only against
//! `self.snapshot_index` — the log's last COMPACTION point, which lags
//! `last_applied` whenever entries commit between compactions (the common
//! case under any real write load). A stale transfer built at an index
//! between the two sailed through as "not yet redundant." The fix compares
//! against `self.last_applied` instead, which is always `>= snapshot_index`
//! and reflects the receiver's true up-to-date position.

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::raft::SNAPSHOT_CHUNK_BYTES;
use animus_control::{MetaCommand, NodeStatus, RaftCore, RaftMsg, RaftNode};
use animus_env::{Nanos, NodeId, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

fn upsert(node: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node: nid(node),
        labels: BTreeMap::new(),
        status: NodeStatus::Active,
    }
}

/// Elect `nid(0)` leader of the 2-node group `[nid(0), nid(1)]`.
fn elect_leader_of_pair(now: Nanos) -> RaftCore {
    elect_leader(&[nid(0), nid(1)], now)
}

/// Elect `nid(0)` leader of `members` (`nid(0)` must be first).
fn elect_leader(members: &[NodeId], now: Nanos) -> RaftCore {
    let mut leader: RaftCore = RaftCore::new(nid(0), members, Nanos(0), 7);
    let _ = leader.tick(now, 7); // election timeout -> pre-candidate, PreVote
    let _ = leader.handle(
        nid(1),
        RaftMsg::PreVoteResp {
            term: leader.term() + 1,
            granted: true,
        },
        now,
        7,
    );
    let _ = leader.handle(
        nid(1),
        RaftMsg::RequestVoteResp {
            term: leader.term(),
            granted: true,
        },
        now,
        7,
    );
    assert!(leader.is_leader(), "node 0 should have won the election");
    leader
}

/// Pump messages between `leader` (`nid(0)`) and `follower` (`nid(1)`) to
/// quiescence. A message addressed to any other node id (e.g. a third,
/// absent member) is dropped, mirroring `install_snapshot.rs::pump_snapshot`.
fn pump(
    leader: &mut RaftCore,
    follower: &mut RaftCore,
    now: Nanos,
    mut pending: Vec<(NodeId, RaftMsg)>,
) {
    let mut steps = 0;
    while !pending.is_empty() {
        steps += 1;
        assert!(steps < 2000, "replication did not converge");
        let mut next: Vec<(NodeId, RaftMsg)> = Vec::new();
        for (to, msg) in pending {
            if to == nid(1) {
                next.extend(follower.handle(nid(0), msg, now, 7));
            } else if to == nid(0) {
                next.extend(leader.handle(nid(1), msg, now, 7));
            }
        }
        pending = next;
    }
}

/// Fully replicate `leader`'s current log to `follower` and let both sides
/// apply: durability + one heartbeat round to ship the entries, then a
/// SECOND heartbeat round so the follower learns the leader's now-advanced
/// `leader_commit` (which only piggybacks on the *next* `AppendEntries`,
/// never delivered out of band) and applies too.
fn replicate_and_apply(leader: &mut RaftCore, follower: &mut RaftCore, now: Nanos) -> Nanos {
    leader.mark_durable_through(leader.last_log_index());
    let hb1 = Nanos(now.0 + 1_000_000_000);
    let pending1 = leader.tick(hb1, 7);
    pump(leader, follower, hb1, pending1);
    let hb2 = Nanos(hb1.0 + 1_000_000_000);
    let pending2 = leader.tick(hb2, 7);
    pump(leader, follower, hb2, pending2);
    hb2
}

/// `RaftCore`-level reproduction: drive a real leader/follower pair through
/// ordinary replication until the follower has applied well past some index
/// `S`, then hand-deliver a synthetic **stale** `InstallSnapshot` final
/// chunk claiming `last_index == S` — standing in for a leftover/duplicated
/// chunk from an earlier, now-obsolete transfer (e.g. built at the leader's
/// prior compaction point, delayed in flight while the follower caught up
/// the ordinary way in the meantime). The follower must ack it as redundant
/// and must NOT rewind.
#[test]
fn stale_install_snapshot_below_last_applied_is_rejected() {
    let now = Nanos(1_000_000_000);
    let mut leader = elect_leader_of_pair(now);
    let mut follower: RaftCore = RaftCore::new(nid(1), &[nid(0), nid(1)], Nanos(0), 7);

    for i in 0..40u64 {
        let _ = leader.propose(upsert(i));
    }
    let last = replicate_and_apply(&mut leader, &mut follower, now);

    let applied_before = follower.last_applied();
    assert!(
        applied_before >= 40,
        "follower should have applied all 40 proposed entries via ordinary \
         AppendEntries, got last_applied={applied_before}"
    );
    // No compaction has happened anywhere in this test: `snapshot_index`
    // lags `last_applied` exactly the way it does live under real write load.
    assert_eq!(
        follower.snapshot_index(),
        0,
        "test setup should not have triggered compaction"
    );
    assert!(
        !follower.state_machine_behind(),
        "a follower that caught up normally must not be flagged behind"
    );

    // A stale, single-chunk `InstallSnapshot` transfer claiming a
    // `last_index` the follower has already applied well past — the exact
    // shape of a late-arriving/duplicated chunk from an earlier, now-
    // obsolete transfer.
    let stale_last_index = 10u64;
    assert!(stale_last_index < applied_before);
    let stale = RaftMsg::InstallSnapshot {
        term: leader.term(),
        leader: nid(0),
        last_index: stale_last_index,
        last_term: leader.term(),
        offset: 0,
        data: vec![0xAAu8; 16],
        total: 16,
        done: true,
        config: None,
        learners: None,
    };
    let resp = follower.handle(nid(0), stale, last, 7);

    assert_eq!(
        follower.last_applied(),
        applied_before,
        "a stale InstallSnapshot must never rewind last_applied — this is the \
         exact rewind that corrupted `max_applied_ts` ordering live"
    );
    assert_eq!(
        follower.snapshot_index(),
        0,
        "a stale InstallSnapshot must not install and move snapshot_index \
         backwards below what the follower already applied"
    );
    assert!(
        resp.iter().any(|(_, m)| matches!(
            m,
            RaftMsg::InstallSnapshotResp { last_index, .. } if *last_index == 0
        )),
        "the follower should simply ack its position as redundant, not go \
         through the install-completion reply path: {resp:?}"
    );
}

/// End-to-end shape closer to the maintainer's live repro: a leader builds a
/// chunked snapshot transfer at an early compaction point, but by the time
/// its (stale/late) final chunk lands, the follower has already caught all
/// the way up past that point via ordinary `AppendEntries`. The stale
/// transfer must not rewind the follower once it does land.
#[test]
fn follower_outruns_inflight_snapshot_via_append_entries() {
    let now = Nanos(1_000_000_000);
    let follower_id = nid(1);
    let members = [nid(0), follower_id.clone()];
    // Single-chunk (fits in one `InstallSnapshot` message, `done: true`
    // immediately) — the guard runs on every chunk, including the first,
    // so what matters for this repro is that the offer actually COMPLETES;
    // multi-chunk reassembly is exercised elsewhere (`install_snapshot.rs`).
    let image = vec![0xCDu8; SNAPSHOT_CHUNK_BYTES / 2];

    let mut leader = elect_leader(&members, now);
    let mut follower: RaftCore = RaftCore::new(follower_id.clone(), &members, Nanos(0), 7);

    // First batch: replicate normally to the REAL follower (ordinary
    // `AppendEntries`, no snapshot involved — this is the common case, a
    // follower that is fully caught up) and only THEN compact the leader's
    // log behind it. This mirrors the live shape: compaction runs behind an
    // already-current follower, so the follower never actually needs the
    // resulting image.
    for i in 0..60u64 {
        let _ = leader.propose(upsert(i));
    }
    let mut last = replicate_and_apply(&mut leader, &mut follower, now);
    assert_eq!(follower.last_applied(), leader.last_applied());

    leader.snapshot();
    let early_snapshot_index = leader.snapshot_index();
    assert!(early_snapshot_index > 0, "leader should have a snapshot");
    assert_eq!(
        early_snapshot_index,
        follower.last_applied(),
        "the compacted prefix should exactly match what the follower already has"
    );
    leader.set_snapshot_blob(image.clone());

    // Manufacture a stale chunked transfer's final chunk at that (already
    // fully redundant) base — standing in for a chunk that was built/queued
    // around the time of that compaction and is only now, late, arriving at
    // the follower (e.g. a leader that floods/restarts transfers under
    // load, or a duplicated/reordered network delivery).
    let stale_final_chunk = RaftMsg::InstallSnapshot {
        term: leader.term(),
        leader: nid(0),
        last_index: early_snapshot_index,
        last_term: leader.term(),
        offset: 0,
        data: image[..image.len().min(SNAPSHOT_CHUNK_BYTES)].to_vec(),
        total: image.len() as u64,
        done: image.len() <= SNAPSHOT_CHUNK_BYTES,
        config: None,
        learners: None,
    };

    // The follower keeps catching up via ordinary log replication, past the
    // early snapshot base — the leader's log tail past `snapshot_index` is
    // exactly what a real leader still has and replicates normally.
    for i in 60..120u64 {
        let _ = leader.propose(upsert(i));
    }
    last = replicate_and_apply(&mut leader, &mut follower, last);

    let applied_before = follower.last_applied();
    assert!(
        applied_before > early_snapshot_index,
        "follower should have caught all the way up past the early snapshot \
         base ({early_snapshot_index}) via AppendEntries, got \
         last_applied={applied_before}"
    );

    // Now the stale, late chunk lands.
    let resp = follower.handle(nid(0), stale_final_chunk, last, 7);

    assert_eq!(
        follower.last_applied(),
        applied_before,
        "a stale, late-arriving InstallSnapshot chunk must never rewind a \
         follower that has already caught up past it via AppendEntries"
    );
    assert!(
        resp.iter().any(|(_, m)| matches!(
            m,
            RaftMsg::InstallSnapshotResp { last_index, .. } if *last_index == 0
        )),
        "expected a plain redundant ack, not an install: {resp:?}"
    );
}

/// Full-node `SimEnv` sanity check that the fix doesn't regress ordinary
/// catch-up: a genuinely far-behind, partitioned follower still converges
/// via `InstallSnapshot` once healed (mirrors
/// `install_snapshot.rs::partitioned_follower_catches_up_via_install_snapshot`,
/// kept here so this file also exercises the real driver/engine path, not
/// only hand-driven cores).
#[test]
fn genuinely_behind_follower_still_converges() {
    let seed = 0x57A1E;
    let sim = Simulator::new(seed);
    let ids = [nid(0), nid(1), nid(2)];
    let nodes: Vec<RaftNode<SimEnv>> = ids
        .iter()
        .map(|id| RaftNode::start(sim.env(id.clone()), ids.to_vec(), MemoryEngine::new()))
        .collect();
    let mut sim = sim;
    sim.run_for(Duration::from_secs(2));
    let leader = (0..3).find(|&i| nodes[i].is_leader()).expect("a leader");
    let follower = (0..3).find(|&i| i != leader).unwrap();
    let follower_id = ids[follower].clone();

    for peer in &ids {
        if *peer != follower_id {
            sim.partition_pair(follower_id.clone(), peer.clone());
        }
    }
    for i in 0..100 {
        nodes[leader].propose(upsert(i));
    }
    sim.run_for(Duration::from_secs(4));
    for peer in &ids {
        if *peer != follower_id {
            sim.heal(follower_id.clone(), peer.clone());
        }
    }
    sim.run_for(Duration::from_secs(4));

    assert_eq!(
        nodes[follower].metadata(),
        nodes[leader].metadata(),
        "follower did not converge after InstallSnapshot (seed={seed})"
    );
}
