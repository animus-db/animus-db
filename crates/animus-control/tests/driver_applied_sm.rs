//! Stage A.1 of the per-tablet Raft data plane (ADR 0017): a `DRIVER_APPLIED`
//! state machine does **not** apply in-core — the core buffers each committed-and-
//! durable command as an effect for an async driver to apply to a real engine,
//! drained via `RaftCore::drain_apply`. (The driver + engine wiring is Stage B;
//! here we prove the *core mechanism*: effects are exactly the committed-durable
//! commands in commit order, the in-core path is bypassed, and durable-before-
//! visible still gates which commands are handed out.)

use animus_control::persist::WalRecord;
use animus_control::raft::{RaftCore, StateMachine};
use animus_env::Nanos;
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
    let mut core: KvCore = RaftCore::new(0, &[0], Nanos(0), 7);
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
    // the election no-op, then the two puts.
    let effects = core.drain_apply();
    assert_eq!(
        effects,
        vec![
            (1, KvCommand::NoOp),
            (2, KvCommand::Put { key: 1, value: 10 }),
            (3, KvCommand::Put { key: 2, value: 20 }),
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
