//! `RaftCore<C, S>` is generic over its state machine (ADR 0009 generalization,
//! the linchpin for the per-tablet Raft data plane of ADR 0016). This test proves
//! the generalization actually generalizes: it drives the *same* consensus core
//! with a toy key-value state machine — a different command and image type from
//! the control plane's `MetaCommand`/`Metadata` — through propose → durable-apply
//! → snapshot → WAL recovery, with no control-plane types involved.

use animus_control::persist::PersistedState;
use animus_control::raft::{RaftCore, StateMachine};
use animus_env::{Nanos, nid};
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
