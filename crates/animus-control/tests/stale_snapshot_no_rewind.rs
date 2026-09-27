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

/// Regression for a second, wire-shape bug in the same short-circuit
/// (found alongside PR #1048's `last_applied`-comparison fix above): once a
/// follower has EVER compacted its own log (`snapshot_index() > 0`), the
/// short-circuit's reply used to echo that follower's own `snapshot_index`
/// as `last_index` — nonzero, and therefore byte-for-byte indistinguishable
/// on the wire from a genuine just-completed install. Both tests above
/// happen NOT to exercise this: their follower never calls `snapshot()`
/// itself, so `follower.snapshot_index()` is always `0` there and the old
/// buggy echo coincided with the fixed code's `last_index: 0` by accident.
/// This test drives the follower into having actually compacted first.
#[test]
fn stale_offer_after_follower_has_compacted_does_not_impersonate_completion() {
    let now = Nanos(1_000_000_000);
    let mut leader = elect_leader_of_pair(now);
    let mut follower: RaftCore = RaftCore::new(nid(1), &[nid(0), nid(1)], Nanos(0), 7);

    // First batch: replicate normally, then have the FOLLOWER itself
    // compact (unlike the tests above, where only the leader ever calls
    // `snapshot()`), so `follower.snapshot_index() > 0`.
    for i in 0..40u64 {
        let _ = leader.propose(upsert(i));
    }
    let mut last = replicate_and_apply(&mut leader, &mut follower, now);
    follower.snapshot();
    let follower_snapshot_index = follower.snapshot_index();
    assert!(
        follower_snapshot_index > 0,
        "test setup should have actually compacted the follower"
    );

    // Advance the follower well past its own compaction point via ordinary
    // AppendEntries, and have the LEADER also compact past the follower's
    // old snapshot base — so a wrongly-regressed `next_index` can no longer
    // find a log entry to send and would fall back to a brand-new
    // InstallSnapshot, exactly the live regression
    // (`old_match=702 new_match=702 old_next=703 new_next=191`).
    for i in 40..80u64 {
        let _ = leader.propose(upsert(i));
    }
    last = replicate_and_apply(&mut leader, &mut follower, last);
    let applied_before = follower.last_applied();
    assert!(
        applied_before > follower_snapshot_index,
        "follower should have caught up well past its own compaction point"
    );
    leader.snapshot();
    assert!(
        leader.snapshot_index() >= follower_snapshot_index,
        "leader should have compacted past the follower's old snapshot base"
    );

    // A stale/duplicate InstallSnapshot offer — standing in for a leftover
    // chunk from an earlier, now wholly obsolete transfer — lands at the
    // follower. Its own `last_index` is <= the follower's `last_applied`,
    // so it hits the "already at least this far along" short-circuit.
    let stale = RaftMsg::InstallSnapshot {
        term: leader.term(),
        leader: nid(0),
        last_index: follower_snapshot_index,
        last_term: leader.term(),
        offset: 0,
        data: vec![0xAAu8; 16],
        total: 16,
        done: true,
        config: None,
        learners: None,
    };
    let resp = follower.handle(nid(0), stale, last, 7);
    let (_, resp_msg) = resp
        .into_iter()
        .find(|(to, _)| *to == nid(0))
        .expect("follower should reply to the leader");
    let resp_last_index = match &resp_msg {
        RaftMsg::InstallSnapshotResp { last_index, .. } => *last_index,
        other => panic!("expected an InstallSnapshotResp, got {other:?}"),
    };
    assert_eq!(
        resp_last_index, 0,
        "a redundant offer must not reply with the same wire shape as a \
         genuine completion just because this follower has ever compacted \
         (snapshot_index={follower_snapshot_index} > 0) — a leader-side \
         consumer that trusts last_index > 0 as \"install complete\" \
         (animus-cp-data's CpSnapshotInstalls metric, and the \
         next_index-reset branch checked below) would wrongly treat this \
         redundant ack as one"
    );

    // Deliver that reply to the LEADER and confirm it neither regresses this
    // peer's replication progress nor issues an unnecessary fresh
    // InstallSnapshot — `handle_install_snapshot_resp`'s "transfer complete"
    // branch calls `replicate_to` synchronously right after updating
    // `next_index`, so a regression shows up directly in this call's output.
    let leader_out = leader.handle(nid(1), resp_msg, last, 7);
    assert!(
        !leader_out
            .iter()
            .any(|(_, m)| matches!(m, RaftMsg::InstallSnapshot { .. })),
        "the leader must not fall back to a fresh InstallSnapshot for a peer \
         that was already caught up — this only happens if next_index was \
         wrongly regressed to the stale offer's base: {leader_out:?}"
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

/// Regression for `RaftCore::last_installed_index` (ADR 0017's
/// `SNAPSHOT_CHUNK_BYTES` bump follow-up, 2026-09-27 amendment): a resent
/// duplicate of a final `InstallSnapshot` chunk — the leader's
/// `SnapshotResend::Always` firing again before it has processed the
/// receiver's first completion ack — must not be reprocessed as a fresh
/// install while the receiver's own driver is still digesting the first
/// copy (`state_machine_behind` still `true`, since that flag is cleared
/// only by the driver's own async apply task, never by
/// `handle_install_snapshot` itself). Before this fix, the top-of-function
/// short-circuit fell through to full reassembly unconditionally whenever
/// `state_machine_behind` was `true` — correct for the wipe-recovery case
/// it exists for (a resent offer at the SAME `last_index` a fresh, empty
/// engine must not discard as "already have it"), but it also meant a
/// resent duplicate of the exact same completed image re-completed from
/// scratch every time, each one counted as a genuine
/// `Metric::CpSnapshotInstalls` and each forcing another WAL rewrite (149
/// counted installs for a single genuine image at the ADR's own pinned
/// seed). `last_installed_index` (volatile, `None` every fresh lifetime —
/// unlike `snapshot_index`/`last_applied`, which the wipe-recovery guard
/// deliberately does NOT trust while `state_machine_behind`) catches an
/// exact duplicate of the last completed install regardless of
/// `state_machine_behind`, while a genuine first offer at that same index —
/// including on a freshly recovered core, whose `last_installed_index` is
/// `None` this lifetime no matter what `snapshot_index` its WAL restored —
/// still falls through to a real install exactly as before this fix.
#[test]
fn resent_duplicate_final_chunk_is_rejected_while_still_digesting_then_a_fresh_core_still_installs()
{
    let now = Nanos(1_000_000_000);
    let leader = elect_leader_of_pair(now);
    let image = vec![0xEEu8; 32];
    let install_index = 50u64;
    let chunk = |last_index: u64| RaftMsg::InstallSnapshot {
        term: leader.term(),
        leader: nid(0),
        last_index,
        last_term: leader.term(),
        offset: 0,
        data: image.clone(),
        total: image.len() as u64,
        done: true,
        config: None,
        learners: None,
    };
    fn resp_last_index(resp: &[(NodeId, RaftMsg)]) -> u64 {
        let (_, msg) = resp
            .iter()
            .find(|(to, _)| *to == nid(0))
            .expect("a reply to the leader");
        match msg {
            RaftMsg::InstallSnapshotResp { last_index, .. } => *last_index,
            other => panic!("expected an InstallSnapshotResp, got {other:?}"),
        }
    }

    // A follower whose engine is still digesting a prior install (the
    // wipe-recovery contract: `state_machine_behind` stays `true` across
    // every one of its acks until the driver's own async apply task
    // catches up — `RaftCore` itself never clears it).
    let mut follower: RaftCore = RaftCore::new(nid(1), &[nid(0), nid(1)], Nanos(0), 7);
    follower.set_state_machine_behind(true);

    // First offer at `install_index`: a genuine completion. It must fall
    // through to a real install despite `state_machine_behind` — the
    // wipe-recovery case this guard must not break.
    let resp1 = follower.handle(nid(0), chunk(install_index), now, 7);
    assert_eq!(
        resp_last_index(&resp1),
        install_index,
        "the first offer at a never-before-installed index must complete a genuine install"
    );
    assert_eq!(follower.snapshot_index(), install_index);
    assert_eq!(follower.last_applied(), install_index);
    let pending = follower.drain_pending_install();
    assert_eq!(
        pending.as_ref().map(|(idx, _)| *idx),
        Some(install_index),
        "the genuine install must have queued a pending install for the driver"
    );
    // Still digesting from this replica's own point of view — the driver
    // has not yet run and cleared this.
    assert!(follower.state_machine_behind());

    // A resent duplicate of the SAME final chunk lands again (the exact
    // shape `SnapshotResend::Always` produces before the leader has
    // processed the first completion ack).
    let resp2 = follower.handle(nid(0), chunk(install_index), now, 7);
    assert_eq!(
        resp_last_index(&resp2),
        0,
        "a resent duplicate of an already-installed image must be answered as \
         redundant (last_index: 0), not reprocessed as a fresh completion"
    );
    // No second install happened: state is unchanged, and nothing new was
    // queued for the driver to (re-)apply.
    assert_eq!(follower.snapshot_index(), install_index);
    assert_eq!(follower.last_applied(), install_index);
    assert_eq!(
        follower.drain_pending_install(),
        None,
        "a rejected duplicate must not queue a second pending install for the driver"
    );

    // A fresh core — standing in for a restart, across which
    // `last_installed_index` does NOT survive (unlike `snapshot_index`/
    // `last_applied`, which a WAL replay would restore) — must still accept
    // a GENUINE install at the exact same index: the wipe-recovery path
    // this guard must not break.
    let mut fresh: RaftCore = RaftCore::new(nid(1), &[nid(0), nid(1)], Nanos(0), 7);
    fresh.set_state_machine_behind(true);
    let resp3 = fresh.handle(nid(0), chunk(install_index), now, 7);
    assert_eq!(
        resp_last_index(&resp3),
        install_index,
        "a fresh core (last_installed_index == None this lifetime) must still accept a \
         genuine install at the same index a prior lifetime already installed — the \
         wipe-recovery path `engine_wipe_needs_snapshot.rs` exercises end-to-end"
    );
    assert_eq!(fresh.snapshot_index(), install_index);
    assert_eq!(fresh.last_applied(), install_index);
    assert_eq!(
        fresh.drain_pending_install().map(|(idx, _)| idx),
        Some(install_index),
        "the fresh core's genuine install must also queue a pending install for its own driver"
    );
}
