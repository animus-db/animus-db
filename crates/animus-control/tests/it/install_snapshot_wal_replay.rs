//! Issue #1368: an `InstallSnapshot` moves the follower's log base and clears
//! its in-memory log, but the WAL file is only rewritten later (the apply task
//! installs the engine image first). The consensus loop meanwhile persists the
//! leader's next `AppendEntries` -- indexes past the new base -- onto the OLD
//! file. `PersistedState::replay` rebuilds the log by position, so a crash in
//! that window recovered `[old tail] ++ [new entries]`: a gap (or a repeated
//! range) in which every position holds the wrong index. `term_at` and the
//! apply task's index labels are positional, so the leader's resend then
//! appended one entry twice and the apply task panicked on two equal HLC
//! timestamps (`assert_ts_monotonic`).
//!
//! Hand-drives one follower `RaftCore` and its WAL exactly like a driver does:
//! drain the pending records into the WAL after every step, then "crash" by
//! replaying only that WAL.

use animus_control::persist::{PersistedState, WalRecord};
use animus_control::{LogEntry, MetaCommand, NodeStatus, RaftCore, RaftMsg};
use animus_env::{Nanos, nid};
use std::collections::BTreeMap;

const NOW: Nanos = Nanos(1_000_000_000);

fn entry(index: u64) -> LogEntry {
    LogEntry {
        term: 1,
        index,
        command: MetaCommand::UpsertMember {
            node: nid(600 + index),
            labels: BTreeMap::new(),
            status: NodeStatus::Active,
        },
        config: None,
        learners: None,
    }
}

fn append(prev: u64, entries: std::ops::RangeInclusive<u64>) -> RaftMsg {
    RaftMsg::AppendEntries {
        term: 1,
        leader: nid(0),
        prev_log_index: prev,
        prev_log_term: u64::from(prev > 0),
        entries: entries.map(entry).collect(),
        leader_commit: 0,
    }
}

fn install(last_index: u64) -> RaftMsg {
    RaftMsg::InstallSnapshot {
        term: 1,
        leader: nid(0),
        last_index,
        last_term: 1,
        offset: 0,
        data: vec![1, 2, 3],
        total: 3,
        done: true,
        config: None,
        learners: None,
    }
}

/// A follower configured like the data-plane driver (`animus-cp-data`), the
/// only one that opts into logging an installed snapshot's base immediately.
fn data_plane_follower() -> RaftCore {
    let mut f: RaftCore = RaftCore::new(nid(1), &[nid(0), nid(1), nid(2)], Nanos(0), 3);
    f.set_wal_records_install(true);
    f
}

/// The driver's persist step: drain the pending records into the WAL (the
/// "fsync") and advance the durable watermark.
fn persist(core: &mut RaftCore, wal: &mut Vec<WalRecord>) {
    let through = core.last_log_index();
    wal.extend(core.drain_persist());
    core.mark_durable_through(through);
}

fn replay(records: impl IntoIterator<Item = WalRecord>) -> PersistedState {
    PersistedState::replay(records)
}

fn crash_and_replay(wal: &[WalRecord]) -> PersistedState {
    replay(wal.iter().cloned())
}

fn indexes(state: &PersistedState) -> Vec<u64> {
    state.log.iter().map(|e| e.index).collect()
}

/// A follower holding 1..=5 is behind the leader's compacted base 8, installs
/// that snapshot, and persists the leader's next entries 9..=11 -- all before
/// the WAL rewrite. A crash there must recover the post-install state, not a
/// gapped `[1..=5, 9..=11]` log.
#[test]
fn a_crash_between_an_install_and_the_wal_rewrite_recovers_the_installed_base() {
    let mut f = data_plane_follower();
    let mut wal = Vec::new();

    let _ = f.handle(nid(0), append(0, 1..=5), NOW, 3);
    persist(&mut f, &mut wal);
    let _ = f.handle(nid(0), install(8), NOW, 3);
    let _ = f.handle(nid(0), append(8, 9..=11), NOW, 3);
    persist(&mut f, &mut wal);
    assert_eq!(f.snapshot_index(), 8);
    assert_eq!(f.last_log_index(), 11);

    let state = crash_and_replay(&wal);
    assert_eq!(
        state.snapshot.as_ref().map(|s| s.1),
        Some(8),
        "the installed base must be durable"
    );
    assert_eq!(
        indexes(&state),
        vec![9, 10, 11],
        "the pre-install tail is gone"
    );
    state
        .check_log_contiguous()
        .expect("the replayed log must sit at its own positions");
}

/// The end-to-end symptom: after the crash the leader re-offers the tail.
/// With a shifted recovered log the follower's positional `term_at` rejected a
/// matching entry as a conflict, truncated nothing, and appended it a second
/// time.
#[test]
fn the_leader_resend_after_such_a_crash_never_duplicates_an_entry() {
    let mut f = data_plane_follower();
    let mut wal = Vec::new();
    let _ = f.handle(nid(0), append(0, 1..=5), NOW, 3);
    persist(&mut f, &mut wal);
    let _ = f.handle(nid(0), install(6), NOW, 3);
    let _ = f.handle(nid(0), append(6, 7..=11), NOW, 3);
    persist(&mut f, &mut wal);

    let state = crash_and_replay(&wal);
    let mut r: RaftCore = RaftCore::recovered(nid(1), &[nid(0), nid(1), nid(2)], state, NOW, 3);
    r.set_wal_records_install(true);
    // The leader walks back and re-sends from just past the base.
    let _ = r.handle(nid(0), append(6, 7..=12), NOW, 3);
    persist(&mut r, &mut wal);

    let again = crash_and_replay(&wal);
    assert_eq!(indexes(&again), vec![7, 8, 9, 10, 11, 12]);
    let image = replay(r.wal_image());
    assert_eq!(indexes(&image), indexes(&again), "WAL replay == live log");
}

/// A Snapshot record that is not the first record (what an install now emits)
/// resets the log; one that IS first (every rewritten image) is unaffected.
#[test]
fn a_snapshot_record_resets_the_replayed_log() {
    let snap = |last_index| WalRecord::Snapshot {
        metadata: animus_control::Metadata::default(),
        last_index,
        last_term: 1,
        config: None,
        learners: None,
    };
    let state = replay([
        WalRecord::Append(entry(1)),
        WalRecord::Append(entry(2)),
        snap(5),
        WalRecord::Append(entry(6)),
    ]);
    assert_eq!(indexes(&state), vec![6]);
    let image_first = replay([snap(5), WalRecord::Append(entry(6))]);
    assert_eq!(indexes(&image_first), vec![6]);
}

/// The defensive check: a misaligned replayed log is reported, not recovered.
#[test]
fn a_misaligned_replayed_log_is_rejected() {
    let gap = replay([
        WalRecord::Append(entry(1)),
        WalRecord::Append(entry(2)),
        WalRecord::Append(entry(4)),
    ]);
    assert!(gap.check_log_contiguous().is_err());
    let dup = replay([
        WalRecord::Append(entry(1)),
        WalRecord::Append(entry(2)),
        WalRecord::Append(entry(2)),
    ]);
    assert!(dup.check_log_contiguous().is_err());
    let fine = replay([WalRecord::Append(entry(1)), WalRecord::Append(entry(2))]);
    assert!(fine.check_log_contiguous().is_ok());
}
