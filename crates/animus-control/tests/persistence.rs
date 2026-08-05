//! Raft durability: the write-ahead log round-trips, and a core recovered from
//! its WAL restores term/vote/log and then re-applies its tail from the snapshot
//! base *exactly once* (so a compare-and-swap is not double-applied).

use animus_control::MetaCommand;
use animus_control::persist::{PersistedState, WalRecord};
use animus_control::raft::RaftCore;
use animus_env::Nanos;
use animus_tablet::{Epoch, KeyRange, TabletId};

/// Simulate the driver's persist step: drain the core's pending WAL records into
/// `wal` (the "fsync") **and** advance the durable watermark, so committed entries
/// become applied/visible. The driver does exactly this after `env.sync(WAL)`
/// (durable-before-visible, ADR 0009) — a core driven by hand must too, or its
/// `metadata()` never reflects the proposals.
fn persist(core: &mut RaftCore, wal: &mut Vec<WalRecord>) {
    let through = core.last_log_index();
    wal.extend(core.drain_persist());
    core.mark_durable_through(through);
}

/// Drive a single-node group (majority = 1) so it commits and applies on
/// `propose`, collecting every WAL record it emits along the way.
fn run_single_node() -> (RaftCore, Vec<WalRecord>) {
    let mut wal = Vec::new();
    let mut core = RaftCore::new(0, &[0], Nanos(0), 7);
    persist(&mut core, &mut wal);

    // Election timeout → becomes leader (sole voter).
    core.tick(Nanos(1_000_000_000), 7);
    persist(&mut core, &mut wal);

    core.propose(MetaCommand::CreateTablet {
        tablet: TabletId(1),
        table: None,
        range: KeyRange::whole(),
        replicas: vec![0],
    });
    persist(&mut core, &mut wal);

    // A successful CAS bumps the epoch from 1 to 2.
    core.propose(MetaCommand::CasTabletReplicas {
        tablet: TabletId(1),
        expected_epoch: Epoch::INITIAL,
        replicas: vec![0],
    });
    persist(&mut core, &mut wal);

    (core, wal)
}

#[test]
fn recovery_reapplies_the_tail_exactly_once() {
    let (original, wal) = run_single_node();
    assert!(original.is_leader());
    assert_eq!(
        original.metadata().tablets[&TabletId(1)].epoch,
        Epoch(2),
        "precondition: one successful CAS"
    );

    let state = PersistedState::replay(wal);
    let mut recovered = RaftCore::recovered(0, &[0], state, Nanos(0), 7);
    assert_eq!(recovered.term(), original.term(), "term not recovered");

    // Drive the recovered node: it re-elects (sole voter) and re-advances commit
    // over its recovered log, re-applying the tail. A current-term entry (the
    // proposed no-op) is what lets prior-term entries commit.
    recovered.tick(Nanos(2_000_000_000), 7);
    recovered.propose(MetaCommand::NoOp);

    // The CAS landed exactly once — epoch 2, not 3 — and the state matches.
    assert_eq!(
        recovered.metadata().tablets[&TabletId(1)].epoch,
        Epoch(2),
        "tail re-applied more than once (double-applied CAS)"
    );
    assert_eq!(recovered.metadata(), original.metadata());
}

#[test]
fn wal_bytes_round_trip_and_tolerate_a_torn_tail() {
    let (_core, wal) = run_single_node();
    assert!(!wal.is_empty());

    // Encode the records as the on-disk WAL would, then decode.
    let mut bytes: Vec<u8> = Vec::new();
    for record in &wal {
        bytes.extend(PersistedState::encode_record(record));
    }
    let decoded: Vec<WalRecord> = PersistedState::decode(&bytes);
    let from_records = PersistedState::replay(wal);
    let from_bytes = PersistedState::replay(decoded);
    assert_eq!(from_bytes.term, from_records.term);
    assert_eq!(from_bytes.log.len(), from_records.log.len());
    assert_eq!(
        from_bytes.snapshot.is_some(),
        from_records.snapshot.is_some()
    );

    // A crash mid-write leaves a partial trailing line; it must be ignored, and
    // the recovered state must equal the pre-torn-write state.
    bytes.extend_from_slice(b"{\"Hard\":{\"term\":99");
    let torn_records: Vec<WalRecord> = PersistedState::decode(&bytes);
    let torn = PersistedState::replay(torn_records);
    assert_eq!(
        torn.term, from_bytes.term,
        "a torn trailing record must be ignored"
    );
    assert_ne!(torn.term, 99);
}

/// **Durable-before-visible (ADR 0009).** A committed command does not become
/// client-visible (via `metadata()`, what a proposer waits on) until its WAL
/// entry is durably fsynced. So a crash in the commit→fsync window cannot lose a
/// command a client already observed — the window that flaked
/// `animusd`'s `create_table_survives_node_restart`.
#[test]
fn a_command_is_visible_only_after_it_is_durable() {
    let mut wal = Vec::new();
    let mut core = RaftCore::new(0, &[0], Nanos(0), 7);
    core.tick(Nanos(1_000_000_000), 7); // election timeout -> sole leader
    persist(&mut core, &mut wal); // durable through the leader's initial no-op

    // Propose a tablet. On a single-node group it commits immediately (the order
    // is agreed), but it is NOT yet fsynced...
    core.propose(MetaCommand::CreateTablet {
        tablet: TabletId(1),
        table: None,
        range: KeyRange::whole(),
        replicas: vec![0],
    });
    assert!(
        core.commit_index() > core.durable_index(),
        "precondition: committed ahead of the durable frontier"
    );
    // ...so it must not be visible yet.
    assert!(
        core.metadata().tablets.is_empty(),
        "a committed-but-unsynced command must not be client-visible"
    );

    // Crash *before* the fsync: recover from the WAL as it actually stands (the
    // un-synced CreateTablet was never written). It is gone — but it was never
    // visible, so nothing a client could have observed is lost.
    let crashed = RaftCore::recovered(0, &[0], PersistedState::replay(wal.clone()), Nanos(0), 7);
    assert!(
        crashed.metadata().tablets.is_empty(),
        "an un-fsynced command does not survive a crash (and was never acked)"
    );

    // Now fsync: it becomes durable and only then visible.
    persist(&mut core, &mut wal);
    assert_eq!(
        core.durable_index(),
        core.commit_index(),
        "now fully durable"
    );
    assert!(
        core.metadata().tablets.contains_key(&TabletId(1)),
        "after the fsync the command is durable and visible"
    );

    // And now it survives a crash, because it was fsynced before it was visible.
    let mut survivor = RaftCore::recovered(0, &[0], PersistedState::replay(wal), Nanos(0), 7);
    survivor.tick(Nanos(2_000_000_000), 7); // re-elect + a current-term entry
    survivor.propose(MetaCommand::NoOp); // lets the recovered tail re-commit + apply
    assert!(
        survivor.metadata().tablets.contains_key(&TabletId(1)),
        "a durable (visible) command survives the restart"
    );
}
