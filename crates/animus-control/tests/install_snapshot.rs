//! Log truncation + `InstallSnapshot`: when the leader compacts its log past a
//! follower that fell behind (e.g. was partitioned), it ships its snapshot to
//! bring the follower back up rather than replaying entries it no longer has.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use animus_control::raft::SNAPSHOT_CHUNK_BYTES;
use animus_control::{MetaCommand, NodeStatus, RaftCore, RaftMsg, RaftNode};
use animus_env::{Nanos, NodeId};
use animus_sim::{SimEnv, Simulator};

const NODES: [u64; 3] = [0, 1, 2];

fn upsert(node: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node,
        labels: BTreeMap::new(),
        status: NodeStatus::Active,
    }
}

/// An `UpsertMember` with a labels map of `n_keys` entries, so a handful of these
/// build a large, structurally-rich `Metadata` whose serialization is expensive.
fn fat_upsert(node: u64, n_keys: usize) -> MetaCommand {
    let mut labels = BTreeMap::new();
    for k in 0..n_keys {
        labels.insert(format!("k{node}_{k}"), format!("v{k}"));
    }
    MetaCommand::UpsertMember {
        node,
        labels,
        status: NodeStatus::Active,
    }
}

fn cluster(seed: u64) -> (Simulator, Vec<RaftNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| RaftNode::start(sim.env(id), NODES.to_vec()))
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
    let follower = (0..3).find(|&i| i != leader).unwrap() as u64;

    // Isolate the follower from the rest of the cluster.
    for &peer in &NODES {
        if peer != follower {
            sim.partition_pair(follower, peer);
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
        nodes[follower as usize].snapshot_index(),
        0,
        "isolated follower should be stuck with no snapshot"
    );

    // Heal the partition; the leader can no longer send the missing entries by
    // AppendEntries (they're compacted), so it must InstallSnapshot.
    for &peer in &NODES {
        if peer != follower {
            sim.heal(follower, peer);
        }
    }
    sim.run_for(Duration::from_secs(4));

    // The follower installed the snapshot (a base it never reached by applying)
    // and converged on the leader's state.
    assert!(
        nodes[follower as usize].snapshot_index() > 0,
        "follower never installed a snapshot"
    );
    assert_eq!(
        nodes[follower as usize].metadata(),
        nodes[leader].metadata(),
        "follower did not converge after InstallSnapshot (seed={seed})"
    );
    assert_eq!(nodes[follower as usize].metadata().members.len(), 100);
}

/// A far-behind follower catches up via a **multi-chunk** `InstallSnapshot`:
/// drives a leader and follower `RaftCore` (the deterministic state machine)
/// directly, asserting the transfer spans more than one offset-addressed chunk
/// and the follower converges on the leader's metadata.
///
/// Driving the cores rather than the full sim lets the test observe the wire
/// messages and count chunks unambiguously, while still exercising the real
/// chunk-production (leader) and reassembly (follower) paths.
#[test]
fn follower_catches_up_via_multi_chunk_snapshot() {
    const PAIR: [NodeId; 2] = [0, 1];
    let now = Nanos(1_000_000_000);

    // Elect node 0 leader of a two-node group: time out into a candidacy, then
    // feed it node 1's granted vote.
    let mut leader = RaftCore::new(0, &PAIR, Nanos(0), 7);
    let _ = leader.tick(now, 7); // election timeout -> pre-candidate, PreVote
    // A pre-vote grant tips the pre-candidacy into a real, term-bumping election.
    let _ = leader.handle(
        1,
        RaftMsg::PreVoteResp {
            term: leader.term() + 1,
            granted: true,
        },
        now,
        7,
    );
    let _ = leader.handle(
        1,
        RaftMsg::RequestVoteResp {
            term: leader.term(),
            granted: true,
        },
        now,
        7,
    );
    assert!(leader.is_leader(), "node 0 should have won the election");

    // Commit enough members that the serialized snapshot is several chunks long.
    // With node 1 acking, commit advances; then snapshot to compact the prefix.
    let n_members = 300u64;
    for i in 0..n_members {
        if let animus_control::ProposeResult::Accepted { index } = leader.propose(upsert(i)) {
            let _ = leader.handle(
                1,
                RaftMsg::AppendEntriesResp {
                    term: leader.term(),
                    success: true,
                    match_index: index,
                },
                now,
                7,
            );
        }
    }
    // Simulate the leader's fsync so its committed entries are durable and thus
    // applied (durable-before-visible, ADR 0009): `snapshot()` compacts the
    // *applied* prefix, so the watermark must advance first.
    leader.mark_durable_through(leader.last_log_index());
    leader.snapshot();
    assert!(
        leader.snapshot_index() > 0,
        "leader should have a snapshot to ship"
    );
    let serialized_len = serde_json::to_vec(&leader.metadata()).unwrap().len();
    assert!(
        serialized_len > SNAPSHOT_CHUNK_BYTES,
        "snapshot ({serialized_len} bytes) must exceed one chunk ({SNAPSHOT_CHUNK_BYTES}) to \
         exercise multi-chunk transfer"
    );

    // Fresh follower; drive the chunk exchange to completion, counting the
    // distinct chunk offsets the leader sends.
    let mut follower = RaftCore::new(1, &PAIR, Nanos(0), 7);
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
            let replies = if to == 1 {
                follower.handle(0, msg, now, 7)
            } else {
                leader.handle(1, msg, now, 7)
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
    assert_eq!(
        follower.metadata(),
        leader.metadata(),
        "follower did not converge on the leader's metadata after reassembly"
    );
    assert_eq!(follower.snapshot_index(), leader.snapshot_index());
    assert_eq!(follower.metadata().members.len() as u64, n_members);
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
fn large_snapshot_ships_in_o_chunk_time_not_o_state() {
    const PAIR: [NodeId; 2] = [0, 1];
    let now = Nanos(1_000_000_000);

    // Elect node 0 leader of a two-node group.
    let mut leader = RaftCore::new(0, &PAIR, Nanos(0), 7);
    let _ = leader.tick(now, 7);
    let _ = leader.handle(
        1,
        RaftMsg::RequestVoteResp {
            term: leader.term(),
            granted: true,
        },
        now,
        7,
    );
    assert!(leader.is_leader(), "node 0 should have won the election");

    // Build a large metadata: ~130 members * 500 label entries ≈ 1.1MB, ~1100 chunks.
    // Before the fix each chunk re-serialized all ~1.1MB (~50ms), so the transfer
    // would take ~55s; the cached blob makes it ~ms.
    for i in 0..130u64 {
        if let animus_control::ProposeResult::Accepted { index } =
            leader.propose(fat_upsert(i, 500))
        {
            let _ = leader.handle(
                1,
                RaftMsg::AppendEntriesResp {
                    term: leader.term(),
                    success: true,
                    match_index: index,
                },
                now,
                7,
            );
        }
    }
    leader.mark_durable_through(leader.last_log_index());
    leader.snapshot();
    let snap_bytes = serde_json::to_vec(&leader.metadata()).unwrap().len();
    assert!(
        snap_bytes > 500 * SNAPSHOT_CHUNK_BYTES,
        "snapshot ({snap_bytes} bytes) should be many hundreds of chunks to exercise the \
         per-chunk cost; got {} chunks",
        snap_bytes / SNAPSHOT_CHUNK_BYTES
    );

    // Pump a full multi-chunk transfer to a fresh follower, timing the wall clock.
    let mut follower = RaftCore::new(1, &PAIR, Nanos(0), 7);
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
            let replies = if to == 1 {
                follower.handle(0, msg, now, 7)
            } else {
                leader.handle(1, msg, now, 7)
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
    assert_eq!(
        follower.metadata(),
        leader.metadata(),
        "follower did not converge on the leader's metadata"
    );
    // The liveness bound: with the fix (O(chunk) slicing) this runs in ~ms; a
    // per-chunk re-serialize would need ~50ms × ~1100 chunks ≈ 55s. 5s is >100x the
    // fixed time yet <1/10 the regression time — a huge, non-flaky margin.
    assert!(
        elapsed < Duration::from_secs(5),
        "shipping a {snap_bytes}-byte snapshot in {chunks} chunks took {elapsed:?} — \
         snapshot_chunk_for is likely re-serializing the whole Metadata per chunk (O(state) \
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
                next.extend(dst.handle(src_id, msg, now, 7));
            } else if to == src_id {
                next.extend(src.handle(dst_id, msg, now, 7));
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
/// *non-empty* snapshot. Now that [`snapshot_chunk_for`] ships the cached
/// `snapshot_blob` (rather than re-serializing `metadata` per chunk — the
/// driver-liveness fix), the install path must retain the received image so a
/// just-caught-up node that later leads doesn't ship 0 bytes. Node 0 ships to node 1,
/// then node 1 becomes leader and must catch a fresh node 2 up with a non-empty image.
#[test]
fn caught_up_control_node_reships_non_empty() {
    let now = Nanos(1_000_000_000);

    // --- Source leader (node 0): commit enough members to compact a real snapshot.
    let mut src = RaftCore::new(0, &NODES, Nanos(0), 7);
    let _ = src.tick(now, 7);
    let _ = src.handle(
        1,
        RaftMsg::RequestVoteResp {
            term: src.term(),
            granted: true,
        },
        now,
        7,
    );
    assert!(src.is_leader(), "node 0 should have won");
    for i in 0..200u64 {
        if let animus_control::ProposeResult::Accepted { index } = src.propose(upsert(i)) {
            let _ = src.handle(
                1,
                RaftMsg::AppendEntriesResp {
                    term: src.term(),
                    success: true,
                    match_index: index,
                },
                now,
                7,
            );
        }
    }
    src.mark_durable_through(src.last_log_index());
    src.snapshot();
    assert!(src.snapshot_index() > 0, "source should have a snapshot");

    // --- Node 1 catches up from node 0 via InstallSnapshot.
    let mut mid = RaftCore::new(1, &NODES, Nanos(0), 7);
    let hb = Nanos(now.0 + 1_000_000_000);
    let pending = src.tick(hb, 7);
    let totals = pump_snapshot(&mut src, &mut mid, 0, 1, pending);
    assert!(
        totals.iter().any(|&t| t > 0),
        "node 0 should have shipped a non-empty image to node 1, totals={totals:?}"
    );
    assert_eq!(
        mid.snapshot_index(),
        src.snapshot_index(),
        "node 1 caught up"
    );
    assert_eq!(mid.metadata(), src.metadata(), "node 1 has the real state");

    // --- Node 1 becomes leader (higher term) and must re-ship to a fresh node 2.
    let later = Nanos(hb.0 + 1_000_000_000);
    let _ = mid.tick(later, 7); // -> Candidate, term bumps above node 0's
    let _ = mid.handle(
        2,
        RaftMsg::RequestVoteResp {
            term: mid.term(),
            granted: true,
        },
        later,
        7,
    );
    assert!(mid.is_leader(), "node 1 should have won the re-election");

    let mut fresh = RaftCore::new(2, &NODES, Nanos(0), 7);
    let hb2 = Nanos(later.0 + 1_000_000_000);
    let pending2 = mid.tick(hb2, 7);
    let totals2 = pump_snapshot(&mut mid, &mut fresh, 1, 2, pending2);

    // The crux: node 1 — which only ever obtained its state via an install — ships a
    // NON-EMPTY image, so the fresh node reassembles the real metadata.
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
        fresh.metadata(),
        src.metadata(),
        "node 2 reassembled the original metadata via the re-shipped image"
    );
}
