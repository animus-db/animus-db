//! Raft durability: the write-ahead log round-trips, and a core recovered from
//! its WAL restores the log, term/vote, and the applied state machine *without
//! re-applying* committed commands (which would double-apply a compare-and-swap).

use custos_control::MetaCommand;
use custos_control::persist::{PersistedState, WalRecord};
use custos_control::raft::RaftCore;
use custos_env::Nanos;
use custos_tablet::{Epoch, KeyRange, TabletId};

/// Drive a single-node group (majority = 1) so it commits and applies on
/// `propose`, collecting every WAL record it emits along the way.
fn run_single_node() -> (RaftCore, Vec<WalRecord>) {
    let mut wal = Vec::new();
    let mut core = RaftCore::new(0, &[0], Nanos(0), 7);
    wal.extend(core.drain_persist());

    // Election timeout → becomes leader (sole voter).
    core.tick(Nanos(1_000_000_000), 7);
    wal.extend(core.drain_persist());

    core.propose(MetaCommand::CreateTablet {
        tablet: TabletId(1),
        range: KeyRange::whole(),
        replicas: vec![0],
    });
    wal.extend(core.drain_persist());

    // A successful CAS bumps the epoch from 1 to 2.
    core.propose(MetaCommand::CasTabletReplicas {
        tablet: TabletId(1),
        expected_epoch: Epoch::INITIAL,
        replicas: vec![0],
    });
    wal.extend(core.drain_persist());

    (core, wal)
}

#[test]
fn recovered_core_matches_and_does_not_double_apply() {
    let (original, wal) = run_single_node();
    assert!(original.is_leader());
    assert_eq!(
        original.metadata().tablets[&TabletId(1)].epoch,
        Epoch(2),
        "precondition: one successful CAS"
    );

    let state = PersistedState::replay(wal);
    let recovered = RaftCore::recovered(0, &[0], state, Nanos(0), 7);

    assert_eq!(recovered.term(), original.term(), "term not recovered");
    assert_eq!(
        recovered.last_applied(),
        original.last_applied(),
        "applied index not recovered"
    );
    assert_eq!(
        recovered.metadata(),
        original.metadata(),
        "state machine not recovered exactly"
    );
    // The decisive check: the CAS was applied exactly once across the restart.
    assert_eq!(
        recovered.metadata().tablets[&TabletId(1)].epoch,
        Epoch(2),
        "recovery re-applied the log and double-applied the CAS"
    );
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
    let decoded = PersistedState::decode(&bytes);
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
    let torn = PersistedState::replay(PersistedState::decode(&bytes));
    assert_eq!(
        torn.term, from_bytes.term,
        "a torn trailing record must be ignored"
    );
    assert_ne!(torn.term, 99);
}
