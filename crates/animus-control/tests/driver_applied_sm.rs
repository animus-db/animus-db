//! Stage A.1 of the per-tablet Raft data plane (ADR 0017): a `DRIVER_APPLIED`
//! state machine does **not** apply in-core — the core buffers each committed-and-
//! durable command as an effect for an async driver to apply to a real engine,
//! drained via `RaftCore::drain_apply`. (The driver + engine wiring is Stage B;
//! here we prove the *core mechanism*: effects are exactly the committed-durable
//! commands in commit order, the in-core path is bypassed, and durable-before-
//! visible still gates which commands are handed out.)

use animus_control::persist::WalRecord;
use animus_control::raft::{RaftCore, StateMachine};
use animus_control::{ProposeResult, RaftMsg};
use animus_env::{Nanos, NodeId, nid};
use serde::{Deserialize, Serialize};

/// A toy key-value command (what a tablet's Raft log would carry).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum KvCommand {
    Put { key: u64, value: u64 },
    NoOp,
}

/// A `DRIVER_APPLIED` placeholder state machine: the real applied state lives in
/// an engine the driver owns, so the in-core image is unit and `apply` is never
/// called by the core.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct KvUnit;

impl StateMachine<KvCommand> for KvUnit {
    const DRIVER_APPLIED: bool = true;
    fn apply(&mut self, _command: &KvCommand) {
        unreachable!("a DRIVER_APPLIED state machine is never applied in-core");
    }
    fn noop() -> KvCommand {
        KvCommand::NoOp
    }
}

type KvCore = RaftCore<KvCommand, KvUnit>;

#[test]
fn driver_applied_core_buffers_effects_instead_of_applying_in_core() {
    let mut wal: Vec<WalRecord<KvCommand, KvUnit>> = Vec::new();
    let mut core: KvCore = RaftCore::new(nid(0), &[nid(0)], Nanos(0), 7);
    core.tick(Nanos(1_000_000_000), 7); // election timeout -> sole leader (appends a NoOp at index 1)
    assert!(core.is_leader());

    core.propose(KvCommand::Put { key: 1, value: 10 }); // index 2
    core.propose(KvCommand::Put { key: 2, value: 20 }); // index 3

    // Committed (single-node) but NOT yet fsynced: durable-before-visible gates
    // apply, so nothing is handed to the driver yet.
    assert!(core.commit_index() > core.durable_index());
    assert!(
        core.drain_apply().is_empty(),
        "no effects before the WAL is durable"
    );

    // Simulate the driver's fsync: drain the WAL records and advance the durable
    // watermark, which runs apply.
    let through = core.last_log_index();
    wal.extend(core.drain_persist());
    core.mark_durable_through(through);

    // The effects are exactly the committed-durable commands, in commit order:
    // the election no-op, then the two puts. All three ride the sole election's
    // term (1) — `drain_apply` hands out `(index, term, command)` so a driver-side
    // outcome channel can prove entry identity across a truncation.
    let effects = core.drain_apply();
    assert_eq!(
        effects,
        vec![
            (1, 1, KvCommand::NoOp),
            (2, 1, KvCommand::Put { key: 1, value: 10 }),
            (3, 1, KvCommand::Put { key: 2, value: 20 }),
        ],
        "effects are the committed-durable commands in commit order"
    );

    // The in-core path was bypassed entirely: no in-core state, no `applied` log.
    assert_eq!(core.state(), KvUnit, "nothing applied in-core");
    assert!(
        core.applied().is_empty(),
        "the in-core applied log stays empty"
    );

    // Effects are drained exactly once.
    assert!(
        core.drain_apply().is_empty(),
        "effects are handed to the driver exactly once"
    );
}

/// Elect node 0 the leader of a 3-node group by timing it out into a candidacy
/// and feeding it one granted vote (its own + node 1 = majority of 3).
fn elect_sole_leader(group: &[NodeId]) -> KvCore {
    let now = Nanos(1_000_000_000);
    let mut leader: KvCore = RaftCore::new(group[0].clone(), group, Nanos(0), 7);
    let _ = leader.tick(now, 7); // election timeout -> pre-candidate, PreVote
    // A pre-vote grant tips the pre-candidacy into a real, term-bumping election.
    let _ = leader.handle(
        group[1].clone(),
        RaftMsg::PreVoteResp {
            term: leader.term() + 1,
            granted: true,
        },
        now,
        7,
    );
    let _ = leader.handle(
        group[1].clone(),
        RaftMsg::RequestVoteResp {
            term: leader.term(),
            granted: true,
        },
        now,
        7,
    );
    assert!(leader.is_leader(), "node {} should have won", group[0]);
    leader
}

/// Pump messages between `leader` and a single `follower` until the leader stops
/// emitting. Messages addressed to any *other* peer (the absent third node of
/// the group) are dropped — modeling a transfer that involves only these two.
/// Returns the `InstallSnapshot` byte totals shipped to the follower (so the
/// test can assert a non-empty image actually moved).
fn pump_to_follower(
    leader: &mut KvCore,
    follower: &mut KvCore,
    leader_id: NodeId,
    follower_id: NodeId,
    mut pending: Vec<(NodeId, RaftMsg<KvCommand>)>,
) -> Vec<u64> {
    let now = Nanos(1_000_000_000);
    let mut snapshot_totals: Vec<u64> = Vec::new();
    let mut steps = 0;
    while !pending.is_empty() {
        steps += 1;
        assert!(steps < 1000, "message exchange did not terminate");
        let mut next: Vec<(NodeId, RaftMsg<KvCommand>)> = Vec::new();
        for (to, msg) in pending {
            if to == follower_id {
                if let RaftMsg::InstallSnapshot { total, .. } = &msg {
                    snapshot_totals.push(*total);
                }
                next.extend(follower.handle(leader_id.clone(), msg, now, 7));
            } else if to == leader_id {
                next.extend(leader.handle(follower_id.clone(), msg, now, 7));
            }
            // Messages to any other peer are dropped (that node is absent here).
        }
        pending = next;
    }
    snapshot_totals
}

/// Regression (second-hop invariant): a node that itself caught up via a
/// received `InstallSnapshot` must be able to **re-ship** a *non-empty*
/// snapshot. With **lazy on-demand images** the invariant is *"any node with
/// `snapshot_index > 0` regenerates the image from its engine when a
/// replication attempt raises `take_snapshot_needed`"* — strictly stronger
/// than the old blob-retention fix, and this test drives both hops through the
/// request→build→ship cycle, asserting a 0-byte image never ships. Original
/// bug shape: a `DRIVER_APPLIED` snapshot image lives in the engine, so
/// `snapshot_chunk_for` ships `snapshot_blob` — historically set only by the
/// driver on *compaction*. The
/// install path advances `snapshot_index` but (before the fix) left
/// `snapshot_blob = None`, so when the just-caught-up node later became the
/// source it shipped 0 bytes, the receiver decoded an empty image
/// (`EOF while parsing a value`) and could never catch up — the "CP split: new
/// tablet never appeared" failure. Here node 0 ships to node 1, then node 1
/// becomes leader and must ship a non-empty image to a fresh node 2.
#[test]
fn caught_up_node_reships_non_empty_snapshot() {
    let group: [NodeId; 3] = [nid(0), nid(1), nid(2)];
    let now = Nanos(1_000_000_000);

    // --- Source leader (node 0): commit some commands, set an engine image, snapshot.
    let mut src = elect_sole_leader(&group);
    for i in 0..5u64 {
        if let ProposeResult::Accepted { index, .. } =
            src.propose(KvCommand::Put { key: i, value: i })
        {
            // One follower ack (node 1) is a majority of 3 -> commit advances.
            let _ = src.handle(
                nid(1),
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
    // Threshold compaction truncates the log WITHOUT materializing an image —
    // `DRIVER_APPLIED` images are built lazily, only when a follower actually
    // needs an `InstallSnapshot` (audit P1/P5).
    src.snapshot();
    assert!(src.snapshot_index() > 0, "source should have a snapshot");
    assert!(
        !src.take_snapshot_needed(),
        "no follower needs a snapshot yet — compaction alone must not request an image"
    );

    // A non-empty, round-trippable blob standing in for the engine's serialized
    // contents (what the driver's on-demand `engine_image` scan would produce).
    let image: Vec<u8> = serde_json::to_vec(&[("k", 1u64), ("k2", 2u64)]).unwrap();
    assert!(!image.is_empty());

    // --- Node 1 catches up from node 0 via InstallSnapshot.
    let mut mid: KvCore = RaftCore::new(nid(1), &group, Nanos(0), 7);
    let hb = Nanos(now.0 + 1_000_000_000); // past the heartbeat deadline
    // First exchange: node 0 backtracks to the compacted prefix, finds no
    // materialized image, sends nothing, and raises the lazy-build request.
    let pending = src.tick(hb, 7);
    let totals = pump_to_follower(&mut src, &mut mid, nid(0), nid(1), pending);
    assert!(
        totals.iter().all(|&t| t == 0),
        "nothing (and certainly no 0-byte chunk labeled as an image) ships before \
         the driver materializes one, totals={totals:?}"
    );
    assert!(
        src.take_snapshot_needed(),
        "the blocked replication must request an on-demand image build"
    );
    // Simulate the driver: scan the engine into an image and install it.
    src.set_snapshot_blob(image.clone());
    // The next heartbeat retry actually ships the chunks.
    let hb1b = Nanos(hb.0 + 1_000_000_000);
    let pending = src.tick(hb1b, 7);
    let totals = pump_to_follower(&mut src, &mut mid, nid(0), nid(1), pending);
    assert!(
        totals.iter().any(|&t| t > 0),
        "node 0 should have shipped a non-empty image to node 1, totals={totals:?}"
    );
    assert_eq!(
        mid.snapshot_index(),
        src.snapshot_index(),
        "node 1 caught up"
    );
    let mid_installed = mid
        .drain_pending_install()
        .expect("node 1 installed an image");
    assert_eq!(
        mid_installed.1, image,
        "node 1 received the source image intact"
    );

    // --- Node 1 becomes leader (higher term) and must re-ship to a fresh node 2.
    let later = Nanos(hb.0 + 1_000_000_000);
    let _ = mid.tick(later, 7); // -> pre-candidate (term unchanged)
    // A pre-vote grant tips it into the real election, bumping the term above node 0's.
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

    let mut fresh: KvCore = RaftCore::new(nid(2), &group, Nanos(0), 7);
    // The crux (the second-hop invariant, now in its **lazy** form): node 1 only
    // ever obtained its state via an install and retains no image bytes in the
    // core — it must *request a regeneration* rather than ship a 0-byte image
    // (the original bug's failure mode: the receiver decoded an empty image and
    // never caught up).
    let hb2 = Nanos(later.0 + 1_000_000_000);
    let pending2 = mid.tick(hb2, 7);
    let totals2 = pump_to_follower(&mut mid, &mut fresh, nid(1), nid(2), pending2);
    assert!(
        totals2.iter().all(|&t| t == 0),
        "no 0-byte image may ship while unmaterialized, totals={totals2:?}"
    );
    assert!(
        mid.take_snapshot_needed(),
        "the second hop must raise the on-demand image request"
    );
    // Node 1's driver regenerates the image from its engine — which holds
    // exactly the bytes it installed (`drain_pending_install` wrote them there).
    mid.set_snapshot_blob(mid_installed.1.clone());
    let hb2b = Nanos(hb2.0 + 1_000_000_000);
    let pending2 = mid.tick(hb2b, 7);
    let totals2 = pump_to_follower(&mut mid, &mut fresh, nid(1), nid(2), pending2);
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
    let fresh_installed = fresh
        .drain_pending_install()
        .expect("node 2 installed an image");
    assert!(
        !fresh_installed.1.is_empty(),
        "node 2 installed an EMPTY image (the EOF bug)"
    );
    assert_eq!(
        fresh_installed.1, image,
        "node 2 received the original image intact"
    );
}
