//! `RaftCore<C, S>` is generic over its state machine (ADR 0009 generalization,
//! the linchpin for the per-tablet Raft data plane of ADR 0016). This test proves
//! the generalization actually generalizes: it drives the *same* consensus core
//! with a toy key-value state machine — a different command and image type from
//! the control plane's `MetaCommand`/`Metadata` — through propose → durable-apply
//! → snapshot → WAL recovery, with no control-plane types involved.

use animus_control::persist::PersistedState;
use animus_control::raft::{RaftCore, StateMachine};
use animus_control::{ProposeResult, RaftMsg};
use animus_env::{Nanos, NodeId, nid};
use serde::{Deserialize, Serialize};

/// A toy KV command: set a key, or the election no-op.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum KvCommand {
    Put { key: u64, value: Vec<u8> },
    NoOp,
}

/// A toy KV store: the applied state machine + the snapshot image.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct KvStore {
    map: std::collections::BTreeMap<u64, Vec<u8>>,
}

impl StateMachine<KvCommand> for KvStore {
    fn apply(&mut self, command: &KvCommand) {
        if let KvCommand::Put { key, value } = command {
            self.map.insert(*key, value.clone());
        }
    }
    fn noop() -> KvCommand {
        KvCommand::NoOp
    }
}

type KvCore = RaftCore<KvCommand, KvStore>;

/// Simulate the driver's persist step: drain the pending WAL records (the
/// "fsync") and advance the durable watermark so committed entries apply
/// (durable-before-visible, ADR 0009). A hand-driven core must do this or its
/// applied state never moves.
fn persist(
    core: &mut KvCore,
    wal: &mut Vec<animus_control::persist::WalRecord<KvCommand, KvStore>>,
) {
    let through = core.last_log_index();
    wal.extend(core.drain_persist());
    core.mark_durable_through(through);
}

#[test]
fn raft_core_drives_an_arbitrary_state_machine() {
    let mut wal = Vec::new();
    let mut core: KvCore = RaftCore::new(nid(0), &[nid(0)], Nanos(0), 7);
    core.tick(Nanos(1_000_000_000), 7); // election timeout -> sole leader
    persist(&mut core, &mut wal);
    assert!(core.is_leader(), "single-node group elects itself");

    // Propose two writes; on a single-node group they commit immediately, but
    // only become visible after the simulated fsync (durable-before-visible).
    core.propose(KvCommand::Put {
        key: 1,
        value: b"alpha".to_vec(),
    });
    core.propose(KvCommand::Put {
        key: 2,
        value: b"beta".to_vec(),
    });
    assert!(
        core.state().map.is_empty(),
        "committed-but-unsynced writes are not yet applied"
    );
    persist(&mut core, &mut wal);

    let state = core.state();
    assert_eq!(
        state.map.get(&1).map(Vec::as_slice),
        Some(b"alpha".as_ref())
    );
    assert_eq!(state.map.get(&2).map(Vec::as_slice), Some(b"beta".as_ref()));

    // Snapshot truncates the applied prefix; the WAL image replays to the same
    // store — the generic snapshot/recovery path works for a non-Metadata SM.
    core.snapshot();
    let image = core.wal_image();
    let recovered: KvCore = RaftCore::recovered(
        nid(0),
        &[nid(0)],
        PersistedState::replay(image),
        Nanos(0),
        7,
    );
    assert_eq!(
        recovered.state(),
        core.state(),
        "snapshot recovered the KV state machine exactly"
    );

    // And recovery from the full WAL tail (no snapshot) re-applies correctly.
    let from_wal: KvCore =
        RaftCore::recovered(nid(0), &[nid(0)], PersistedState::replay(wal), Nanos(0), 7);
    // The recovered node re-elects and re-advances commit over its tail.
    let mut from_wal = from_wal;
    from_wal.tick(Nanos(2_000_000_000), 7);
    from_wal.propose(KvCommand::NoOp);
    let through = from_wal.last_log_index();
    let _ = from_wal.drain_persist();
    from_wal.mark_durable_through(through);
    assert_eq!(
        from_wal.state().map.get(&1).map(Vec::as_slice),
        Some(b"alpha".as_ref()),
        "WAL-tail recovery re-applied the KV writes"
    );
}

/// `RaftCore`'s own generic `!S::DRIVER_APPLIED` `InstallSnapshot`
/// image — `snapshot_upto`'s eager `serde_json::to_vec(&self.metadata)` —
/// is now wrapped in the `CONTROL_SNAPSHOT` (`CSN1`) envelope (ADR 0073
/// Phase 0 workstream B). This proves the wrap/unwrap round-trips through a
/// real two-node chunked transfer for a non-`Metadata` state machine (the
/// only place this generic path is exercised in this workspace — every real
/// `S` here is `DRIVER_APPLIED`, which builds its own image lazily via
/// `node.rs`'s `syskv_image`/`install_syskv_image` instead).
#[test]
fn kv_follower_catches_up_via_install_snapshot() {
    let pair: [NodeId; 2] = [nid(0), nid(1)];
    let now = Nanos(1_000_000_000);

    // Elect node 0 leader of a two-node group (same hand-pumped shape
    // `install_snapshot.rs`'s `follower_catches_up_via_multi_chunk_snapshot`
    // uses for the `DRIVER_APPLIED` control plane).
    let mut leader: KvCore = RaftCore::new(nid(0), &pair, Nanos(0), 7);
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

    // Commit enough entries that the log is compacted past the fresh follower.
    for i in 0..300u64 {
        if let ProposeResult::Accepted { index, .. } = leader.propose(KvCommand::Put {
            key: i,
            value: vec![i as u8; 8],
        }) {
            let _ = leader.handle(
                nid(1),
                RaftMsg::AppendEntriesResp {
                    term: leader.term(),
                    success: true,
                    match_index: index,
                    needs_snapshot: false,
                    check_pending: false,
                },
                now,
                7,
            );
        }
    }
    leader.mark_durable_through(leader.last_log_index());
    leader.snapshot();
    assert!(
        leader.snapshot_index() > 0,
        "leader should have a snapshot to ship"
    );

    // Fresh follower; drive the chunk exchange to completion.
    let mut follower: KvCore = RaftCore::new(nid(1), &pair, Nanos(0), 7);
    let hb = Nanos(now.0 + 1_000_000_000); // past the heartbeat deadline
    let mut pending = leader.tick(hb, 7);
    let mut steps = 0;
    while !pending.is_empty() {
        steps += 1;
        assert!(steps < 1000, "chunk exchange did not terminate");
        let mut next: Vec<(NodeId, RaftMsg<KvCommand>)> = Vec::new();
        for (to, msg) in pending {
            let replies = if to == nid(1) {
                follower.handle(nid(0), msg, now, 7)
            } else {
                leader.handle(nid(1), msg, now, 7)
            };
            next.extend(replies);
        }
        pending = next;
    }

    assert_eq!(follower.snapshot_index(), leader.snapshot_index());
    assert_eq!(
        follower.state(),
        leader.state(),
        "follower converged via the CSN1-wrapped InstallSnapshot image"
    );
}

/// The dual: an `InstallSnapshot` chunk carrying bytes with no `CSN1` tag at
/// all (what a pre-baseline or corrupted image looks like to this decoder)
/// must install **nothing** and tell the leader to restart the transfer
/// (`InstallSnapshotResp{last_index: 0, next_offset: 0}`) rather than
/// decoding garbage into the state machine.
#[test]
fn kv_follower_refuses_an_untagged_snapshot_image() {
    let pair: [NodeId; 2] = [nid(0), nid(1)];
    let now = Nanos(1_000_000_000);
    let mut follower: KvCore = RaftCore::new(nid(1), &pair, Nanos(0), 7);

    let corrupt = b"not a csn1 envelope, just some bytes".to_vec();
    let total = corrupt.len() as u64;
    let replies = follower.handle(
        nid(0),
        RaftMsg::InstallSnapshot {
            term: 1,
            leader: nid(0),
            last_index: 5,
            last_term: 1,
            offset: 0,
            data: corrupt,
            total,
            done: true,
            config: None,
            learners: None,
        },
        now,
        7,
    );

    assert_eq!(replies.len(), 1);
    let (to, msg) = &replies[0];
    assert_eq!(*to, nid(0));
    match msg {
        RaftMsg::InstallSnapshotResp {
            last_index,
            next_offset,
            ..
        } => {
            assert_eq!(
                *last_index, 0,
                "a decode failure must never report an installed index"
            );
            assert_eq!(
                *next_offset, 0,
                "the leader must be told to restart the transfer from scratch"
            );
        }
        other => panic!("expected InstallSnapshotResp, got {other:?}"),
    }
    assert_eq!(follower.snapshot_index(), 0, "nothing was installed");
    assert!(
        follower.state().map.is_empty(),
        "the state machine must be untouched by a rejected image"
    );
}
