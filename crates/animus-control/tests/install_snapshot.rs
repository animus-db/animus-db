//! Log truncation + `InstallSnapshot`: when the leader compacts its log past a
//! follower that fell behind (e.g. was partitioned), it ships its snapshot to
//! bring the follower back up rather than replaying entries it no longer has.
//!
//! ADR 0038 PR3: `Metadata` is `DRIVER_APPLIED`, so the raw `RaftCore`'s
//! snapshot *image* is no longer an eagerly-serialized `Metadata` — it is
//! built **lazily**, on demand, by an external driver (the real apply task's
//! engine scan in production; see `node.rs`'s `meta_apply_and_compact`) and
//! installed via `set_snapshot_blob`. The hand-driven `RaftCore`-level tests
//! below (which have no real `StorageEngine` in the loop at all) stand in for
//! that driver themselves: right after a source node's `snapshot()` call
//! (whenever its `snapshot_index` becomes newly positive), the test supplies
//! a synthetic image via `set_snapshot_blob` — exactly the same "regenerate
//! from whatever backs `snapshot_index > 0`" contract `snapshot_chunk_for`'s
//! doc describes, just with a plain byte blob standing in for a real engine
//! scan. This decouples these chunk-mechanics tests from `Metadata`'s own
//! serialization entirely, which is arguably tighter scoping: the real
//! engine-backed image path (`syskv_image`/`install_syskv_image`) is
//! exercised by `wal_compaction.rs` and `src/node.rs`'s own tests. The one
//! fully end-to-end test below (`partitioned_follower_catches_up_via_install_
//! snapshot`) drives real `RaftNode`s with real engines, so the real apply
//! task services `take_snapshot_needed` itself — no manual step needed there.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use animus_control::raft::SNAPSHOT_CHUNK_BYTES;
use animus_control::{MetaCommand, NodeStatus, RaftCore, RaftMsg, RaftNode};
use animus_env::{Nanos, NodeId, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

fn member_ids() -> [NodeId; 3] {
    [nid(0), nid(1), nid(2)]
}

fn upsert(node: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node: nid(node),
        labels: BTreeMap::new(),
        status: NodeStatus::Active,
    }
}

fn cluster(seed: u64) -> (Simulator, Vec<RaftNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes = member_ids()
        .iter()
        .map(|id| {
            RaftNode::start(
                sim.env(id.clone()),
                member_ids().to_vec(),
                MemoryEngine::new(),
            )
        })
        .collect();
    (sim, nodes)
}

fn unique_leader(nodes: &[RaftNode<SimEnv>], seed: u64) -> usize {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(ls.len(), 1, "expected one leader, got {ls:?} (seed={seed})");
    ls[0]
}

#[test]
fn partitioned_follower_catches_up_via_install_snapshot() {
    let seed = 0x5A95;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&nodes, seed);
    let follower = (0..3).find(|&i| i != leader).unwrap();
    let follower_id = member_ids()[follower].clone();

    // Isolate the follower from the rest of the cluster.
    for peer in &member_ids() {
        if *peer != follower_id {
            sim.partition_pair(follower_id.clone(), peer.clone());
        }
    }

    // Drive enough writes that the leader commits them (with the *other*
    // follower for majority) and compacts its log past the isolated follower.
    for i in 0..100 {
        nodes[leader].propose(upsert(i));
    }
    sim.run_for(Duration::from_secs(4));

    // The leader truncated its log; the isolated follower learned nothing.
    assert!(
        nodes[leader].snapshot_index() >= 60,
        "leader should have snapshotted/truncated, got {}",
        nodes[leader].snapshot_index()
    );
    assert_eq!(
        nodes[follower].snapshot_index(),
        0,
        "isolated follower should be stuck with no snapshot"
    );

    // Heal the partition; the leader can no longer send the missing entries by
    // AppendEntries (they're compacted), so it must InstallSnapshot — the real
    // apply task builds the on-demand image from its own engine on this path
    // (no manual intervention needed, unlike the hand-driven tests below).
    for peer in &member_ids() {
        if *peer != follower_id {
            sim.heal(follower_id.clone(), peer.clone());
        }
    }
    sim.run_for(Duration::from_secs(4));

    // The follower installed the snapshot (a base it never reached by applying)
    // and converged on the leader's state.
    assert!(
        nodes[follower].snapshot_index() > 0,
        "follower never installed a snapshot"
    );
    assert_eq!(
        nodes[follower].metadata(),
        nodes[leader].metadata(),
        "follower did not converge after InstallSnapshot (seed={seed})"
    );
    assert_eq!(nodes[follower].metadata().members.len(), 100);
}

/// A far-behind follower catches up via a **multi-chunk** `InstallSnapshot`:
/// drives a leader and follower `RaftCore` (the deterministic state machine)
/// directly, asserting the transfer spans more than one offset-addressed chunk
/// and the follower receives the full image.
///
/// Driving the cores rather than the full sim lets the test observe the wire
/// messages and count chunks unambiguously, while still exercising the real
/// chunk-production (leader) and reassembly (follower) paths.
#[test]
fn follower_catches_up_via_multi_chunk_snapshot() {
    let pair: [NodeId; 2] = [nid(0), nid(1)];
    let now = Nanos(1_000_000_000);

    // A synthetic system-keyspace image, several chunks long — stands in for
    // a real apply task's engine scan (see this file's module doc).
    let image = vec![0xABu8; 5 * SNAPSHOT_CHUNK_BYTES + 137];

    // Elect node 0 leader of a two-node group: time out into a candidacy, then
    // feed it node 1's granted vote.
    let mut leader: RaftCore = RaftCore::new(nid(0), &pair, Nanos(0), 7);
    let _ = leader.tick(now, 7); // election timeout -> pre-candidate, PreVote
    // A pre-vote grant tips the pre-candidacy into a real, term-bumping election.
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

    // Commit enough members that the log is compacted past the fresh follower.
    // With node 1 acking, commit advances; then snapshot to compact the prefix.
    let n_members = 300u64;
    for i in 0..n_members {
        if let animus_control::ProposeResult::Accepted { index, .. } = leader.propose(upsert(i)) {
            let _ = leader.handle(
                nid(1),
                RaftMsg::AppendEntriesResp {
                    term: leader.term(),
                    success: true,
                    match_index: index,
                    needs_snapshot: false,
                },
                now,
                7,
            );
        }
    }
    // Simulate the leader's fsync so its committed entries are durable and thus
    // applicable (durable-before-visible, ADR 0009): `snapshot()` compacts the
    // *applied* prefix, so the watermark must advance first.
    leader.mark_durable_through(leader.last_log_index());
    leader.snapshot();
    assert!(
        leader.snapshot_index() > 0,
        "leader should have a snapshot to ship"
    );
    // Supply the (synthetic) engine image — the driver's job in production.
    leader.set_snapshot_blob(image.clone());

    // Fresh follower; drive the chunk exchange to completion, counting the
    // distinct chunk offsets the leader sends.
    let mut follower: RaftCore = RaftCore::new(nid(1), &pair, Nanos(0), 7);
    let mut offsets_sent: BTreeSet<u64> = BTreeSet::new();

    // Prime with a heartbeat. The fresh follower rejects the append (its log is
    // far behind), so the leader backtracks `next_index` until it falls below the
    // compacted snapshot base, then switches to shipping snapshot chunks — the
    // real lagging-follower catch-up path.
    let hb = Nanos(now.0 + 1_000_000_000); // past the heartbeat deadline
    let mut pending: Vec<(NodeId, RaftMsg)> = leader.tick(hb, 7);
    assert!(
        !pending.is_empty(),
        "heartbeat should emit a replication message"
    );
    // Pump messages back and forth until the leader stops emitting (transfer
    // done and follower caught up). Each round: leader -> follower -> leader.
    let mut steps = 0;
    while !pending.is_empty() {
        steps += 1;
        assert!(steps < 1000, "chunk exchange did not terminate");
        let mut next: Vec<(NodeId, RaftMsg)> = Vec::new();
        for (to, msg) in pending {
            if let RaftMsg::InstallSnapshot { offset, .. } = &msg {
                offsets_sent.insert(*offset);
            }
            // Deliver to the right core and collect its replies.
            let replies = if to == nid(1) {
                follower.handle(nid(0), msg, now, 7)
            } else {
                leader.handle(nid(1), msg, now, 7)
            };
            next.extend(replies);
        }
        pending = next;
    }

    assert!(
        offsets_sent.len() > 1,
        "expected a multi-chunk transfer, but only {} chunk offset(s) were sent: {offsets_sent:?}",
        offsets_sent.len()
    );
    assert_eq!(follower.snapshot_index(), leader.snapshot_index());
}

/// Driver-liveness (deferred fix #5): shipping a **large** snapshot to a lagging
/// follower must cost O(chunk) per `InstallSnapshot` message, not O(state). Before
/// the fix, [`RaftCore::snapshot_chunk_for`] re-serialized the whole `Metadata` per
/// 1KB chunk, so a ~1MB metadata (~1000 chunks) cost ~1000 × O(state) serializes —
/// tens of seconds of wall time pinning the consensus loop (a self-sustaining
/// election storm during any large-state catch-up, ADR 0017). Slicing the cached
/// `snapshot_blob` makes the whole transfer complete in milliseconds.
///
/// This is deterministic in the *work done* (a pure hand-pumped transfer, fixed
/// chunk count for a seed), with a **wall-clock upper bound** as the liveness
/// assertion — the property `SimEnv`'s virtual time cannot express. The margin is
/// enormous (fix: ~ms; regression: tens of seconds), so the bound is not flaky. It
/// is `RaftCore`-level rather than a live `ProdEnv` cluster because a live cluster
/// catch-up races leadership/AppendEntries and does not reliably traverse a long
/// chunk-stream; here the transfer is forced end-to-end.
#[test]
// ADR 0003 / ADR 0061 Decision 4 (rung B5): the rest of this file is
// deterministic SimEnv logic — this one function is the exception, a plain
// synchronous wall-clock perf-regression timing (see the doc comment above:
// "the property SimEnv's virtual time cannot express"), not system logic.
#[allow(
    clippy::disallowed_methods,
    reason = "wall-clock perf-regression timing, not system logic under the Env seam (see the doc comment above); ADR 0061 Decision 4"
)]
fn large_snapshot_ships_in_o_chunk_time_not_o_state() {
    let pair: [NodeId; 2] = [nid(0), nid(1)];
    let now = Nanos(1_000_000_000);

    // Elect node 0 leader of a two-node group: time out into a pre-candidacy, take a
    // pre-vote grant to tip into a real (term-bumping) election, then the vote.
    let mut leader: RaftCore = RaftCore::new(nid(0), &pair, Nanos(0), 7);
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

    for i in 0..130u64 {
        if let animus_control::ProposeResult::Accepted { index, .. } = leader.propose(upsert(i)) {
            let _ = leader.handle(
                nid(1),
                RaftMsg::AppendEntriesResp {
                    term: leader.term(),
                    success: true,
                    match_index: index,
                    needs_snapshot: false,
                },
                now,
                7,
            );
        }
    }
    leader.mark_durable_through(leader.last_log_index());
    leader.snapshot();

    // A large synthetic image (~1.1MB, ~1100 chunks) — before the fix each
    // chunk re-serialized all of `Metadata` (~50ms), so the transfer would
    // take ~55s; the cached-blob slicing makes it ~ms, independent of what the
    // bytes actually are (see this file's module doc).
    let snap_bytes = 1_100_000usize;
    let image = vec![0xCDu8; snap_bytes];
    assert!(
        snap_bytes > 500 * SNAPSHOT_CHUNK_BYTES,
        "image ({snap_bytes} bytes) should be many hundreds of chunks to exercise the \
         per-chunk cost; got {} chunks",
        snap_bytes / SNAPSHOT_CHUNK_BYTES
    );
    leader.set_snapshot_blob(image);

    // Pump a full multi-chunk transfer to a fresh follower, timing the wall clock.
    let mut follower: RaftCore = RaftCore::new(nid(1), &pair, Nanos(0), 7);
    let started = std::time::Instant::now();
    let hb = Nanos(now.0 + 1_000_000_000);
    let mut pending: Vec<(NodeId, RaftMsg)> = leader.tick(hb, 7);
    let mut chunks = 0u64;
    let mut steps = 0;
    while !pending.is_empty() {
        steps += 1;
        assert!(steps < 100_000, "chunk exchange did not terminate");
        let mut next: Vec<(NodeId, RaftMsg)> = Vec::new();
        for (to, msg) in pending {
            if matches!(&msg, RaftMsg::InstallSnapshot { .. }) {
                chunks += 1;
            }
            let replies = if to == nid(1) {
                follower.handle(nid(0), msg, now, 7)
            } else {
                leader.handle(nid(1), msg, now, 7)
            };
            next.extend(replies);
        }
        pending = next;
    }
    let elapsed = started.elapsed();

    assert!(
        chunks > 1,
        "expected a multi-chunk transfer, got {chunks} chunk(s)"
    );
    assert_eq!(follower.snapshot_index(), leader.snapshot_index());
    // The liveness bound: with the fix (O(chunk) slicing) this runs in ~ms; a
    // per-chunk re-serialize would need ~50ms × ~1100 chunks ≈ 55s. 5s is >100x the
    // fixed time yet <1/10 the regression time — a huge, non-flaky margin.
    assert!(
        elapsed < Duration::from_secs(5),
        "shipping a {snap_bytes}-byte snapshot in {chunks} chunks took {elapsed:?} — \
         snapshot_chunk_for is likely re-serializing the whole image per chunk (O(state) \
         per InstallSnapshot message) instead of slicing the cached blob"
    );
}

/// Drive the `InstallSnapshot` exchange between `src` (leader) and `dst` (a fresh
/// follower) to completion, dropping messages to the absent third node. Returns the
/// `total` byte counts the source shipped (so a caller can assert a non-empty image
/// moved — the re-ship regression below hinges on this being non-zero).
fn pump_snapshot(
    src: &mut RaftCore,
    dst: &mut RaftCore,
    src_id: NodeId,
    dst_id: NodeId,
    mut pending: Vec<(NodeId, RaftMsg)>,
) -> Vec<u64> {
    let now = Nanos(1_000_000_000);
    let mut totals = Vec::new();
    let mut steps = 0;
    while !pending.is_empty() {
        steps += 1;
        assert!(steps < 2000, "chunk exchange did not terminate");
        let mut next: Vec<(NodeId, RaftMsg)> = Vec::new();
        for (to, msg) in pending {
            if to == dst_id {
                if let RaftMsg::InstallSnapshot { total, .. } = &msg {
                    totals.push(*total);
                }
                next.extend(dst.handle(src_id.clone(), msg, now, 7));
            } else if to == src_id {
                next.extend(src.handle(dst_id.clone(), msg, now, 7));
            }
            // Messages to the absent third node are dropped.
        }
        pending = next;
    }
    totals
}

/// Regression (the control-plane counterpart of
/// `driver_applied_sm.rs::caught_up_node_reships_non_empty_snapshot`): a node that
/// itself caught up via a received `InstallSnapshot` must be able to **re-ship** a
/// *non-empty* snapshot once it becomes leader. In production this is the "any
/// node with `snapshot_index > 0` can regenerate its image on demand" contract
/// (the apply task rebuilds from its own engine); here, mirroring this file's
/// module doc, the test supplies the same synthetic image again right before
/// `mid`'s own replication starts — standing in for that regenerate step (the
/// core itself drops a `DRIVER_APPLIED` blob once idle, so without this the
/// re-ship would have nothing to send). Node 0 ships to node 1, then node 1
/// becomes leader and must catch a fresh node 2 up with a non-empty image.
#[test]
fn caught_up_control_node_reships_non_empty() {
    let now = Nanos(1_000_000_000);
    let image = vec![0xEFu8; 3 * SNAPSHOT_CHUNK_BYTES + 42];

    // --- Source leader (node 0): commit enough members to compact a real snapshot.
    let mut src: RaftCore = RaftCore::new(nid(0), &member_ids(), Nanos(0), 7);
    let _ = src.tick(now, 7); // election timeout -> pre-candidate, PreVote
    // A pre-vote grant (self + node 1 = majority) tips into a real election.
    let _ = src.handle(
        nid(1),
        RaftMsg::PreVoteResp {
            term: src.term() + 1,
            granted: true,
        },
        now,
        7,
    );
    let _ = src.handle(
        nid(1),
        RaftMsg::RequestVoteResp {
            term: src.term(),
            granted: true,
        },
        now,
        7,
    );
    assert!(src.is_leader(), "node 0 should have won");
    for i in 0..200u64 {
        if let animus_control::ProposeResult::Accepted { index, .. } = src.propose(upsert(i)) {
            let _ = src.handle(
                nid(1),
                RaftMsg::AppendEntriesResp {
                    term: src.term(),
                    success: true,
                    match_index: index,
                    needs_snapshot: false,
                },
                now,
                7,
            );
        }
    }
    src.mark_durable_through(src.last_log_index());
    src.snapshot();
    assert!(src.snapshot_index() > 0, "source should have a snapshot");
    src.set_snapshot_blob(image.clone());

    // --- Node 1 catches up from node 0 via InstallSnapshot.
    let mut mid: RaftCore = RaftCore::new(nid(1), &member_ids(), Nanos(0), 7);
    let hb = Nanos(now.0 + 1_000_000_000);
    let pending = src.tick(hb, 7);
    let totals = pump_snapshot(&mut src, &mut mid, nid(0), nid(1), pending);
    assert!(
        totals.iter().any(|&t| t > 0),
        "node 0 should have shipped a non-empty image to node 1, totals={totals:?}"
    );
    assert_eq!(
        mid.snapshot_index(),
        src.snapshot_index(),
        "node 1 caught up"
    );

    // --- Node 1 becomes leader (higher term) and must re-ship to a fresh node 2.
    let later = Nanos(hb.0 + 1_000_000_000);
    let _ = mid.tick(later, 7); // election timeout -> pre-candidate, PreVote
    // A pre-vote grant (self + node 2 = majority) tips into a real, term-bumping election.
    let _ = mid.handle(
        nid(2),
        RaftMsg::PreVoteResp {
            term: mid.term() + 1,
            granted: true,
        },
        later,
        7,
    );
    let _ = mid.handle(
        nid(2),
        RaftMsg::RequestVoteResp {
            term: mid.term(),
            granted: true,
        },
        later,
        7,
    );
    assert!(mid.is_leader(), "node 1 should have won the re-election");
    // Node 1's own driver rebuilds the same logical image from its
    // (now-current) engine before its next replication attempt.
    mid.set_snapshot_blob(image.clone());

    let mut fresh: RaftCore = RaftCore::new(nid(2), &member_ids(), Nanos(0), 7);
    let hb2 = Nanos(later.0 + 1_000_000_000);
    let pending2 = mid.tick(hb2, 7);
    let totals2 = pump_snapshot(&mut mid, &mut fresh, nid(1), nid(2), pending2);

    // The crux: node 1 — which only ever obtained its state via an install — ships a
    // NON-EMPTY image, so the fresh node reassembles it.
    assert!(
        totals2.iter().any(|&t| t > 0),
        "re-shipped snapshot was EMPTY (the bug): a node that caught up via install \
         shipped 0 bytes; totals={totals2:?}"
    );
    assert_eq!(
        fresh.snapshot_index(),
        mid.snapshot_index(),
        "node 2 caught up"
    );
    assert_eq!(
        totals2.iter().max(),
        Some(&(image.len() as u64)),
        "node 2 reassembled the full original image via the re-shipped chunks"
    );
}
