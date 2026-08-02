//! Accord durable state (ADR 0011 follow-up).
//!
//! [`AccordCore`](crate::core::AccordCore) is pure and does no I/O; instead it
//! emits [`WalRecord`]s describing changes to a transaction's durable state,
//! which the node driver appends to a write-ahead log on the `Env` disk and
//! `fsync`s **before** acting on them (replying to a peer, or applying an effect
//! to the store). On startup the driver replays the log into a
//! [`PersistedState`] and recovers the core. This mirrors the control plane's
//! `animus-control::persist` exactly.
//!
//! **Snapshotting / log truncation** (ADR 0011), mirroring the control-plane
//! Raft's compaction. Left unchecked the WAL grows with every *phase transition*
//! of every transaction (a transaction emits up to four-plus records:
//! `PreAccepted`, `Accepted`, `Promised`, `Committed`, `Applied`). The core can
//! take a [`WalRecord::Snapshot`] — a single record carrying the compact
//! [`PersistedState`] image of every transaction it still tracks — and the driver
//! atomically **replaces** the WAL with `[Snapshot] ++ [live tail]`, collapsing
//! each transaction's multi-record history into one and so bounding the WAL to
//! roughly one record per live transaction. Recovery replays the snapshot first,
//! then folds the tail on top.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::core::{Key, Phase, TxnId};
use crate::timestamp::{Ballot, Timestamp};

/// One durable change to a single transaction's replica state, appended to the
/// write-ahead log. Each record is self-describing for the transaction it names,
/// so replaying them in order rebuilds every replica fact the core needs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalRecord {
    /// A transaction was witnessed via `PreAccept`: its keys and the best-known
    /// (proposed) execution timestamp at this replica.
    PreAccepted {
        txn: TxnId,
        keys: BTreeSet<Key>,
        /// The subset of `keys` the transaction writes (recovers the write
        /// effect's target keys for a read-modify-write).
        #[serde(default)]
        write_keys: BTreeSet<Key>,
        /// Caller-supplied value bytes per written key (arbitrary write values,
        /// ADR 0011); recovers the executed value. A key absent executes as the
        /// txn id.
        #[serde(default)]
        write_values: BTreeMap<Key, Vec<u8>>,
        execute_at: Timestamp,
        deps: BTreeSet<TxnId>,
        /// Whether the transaction is read-only (recovers the execution effect
        /// kind: a read snapshot vs. a write).
        #[serde(default)]
        read_only: bool,
    },
    /// A coordinator-chosen execution timestamp and dependency set were adopted
    /// via `Accept`. `accepted_ballot` is the proposal ballot under which they
    /// were adopted (ADR 0011, recovery ballots): the original coordinator's
    /// [`Ballot::ZERO`], or a recovery coordinator's higher ballot. A recovering
    /// node reports it in `RecoverOk` so a later recoverer adopts the
    /// highest-ballot proposal, and a restarted replica keeps its promise.
    Accepted {
        txn: TxnId,
        execute_at: Timestamp,
        deps: BTreeSet<TxnId>,
        /// The ballot under which this `(execute_at, deps)` was accepted.
        /// `#[serde(default)]` ⇒ [`Ballot::ZERO`] for forward-compat.
        #[serde(default)]
        accepted_ballot: Ballot,
    },
    /// A replica **promised** a recovery ballot for `txn` (answered a `Recover`),
    /// so it will reject any later `Recover`/`Accept` below it (ADR 0011, duelling
    /// recoverers). Recorded durably so a restarted replica does not renege on a
    /// promise and let a superseded recoverer win. Additive — older WALs simply
    /// have no `Promised` records, so a recovered replica's promise floor is
    /// [`Ballot::ZERO`].
    Promised { txn: TxnId, ballot: Ballot },
    /// The final `(execute_at, deps)` was recorded via `Commit`. This is the
    /// durable agreement point: after this record is fsynced the replica may
    /// execute the transaction once its dependencies have executed.
    Committed {
        txn: TxnId,
        keys: BTreeSet<Key>,
        /// The subset of `keys` the transaction writes.
        #[serde(default)]
        write_keys: BTreeSet<Key>,
        /// Caller-supplied value bytes per written key (arbitrary write values,
        /// ADR 0011); recovers the executed value. A key absent executes as the
        /// txn id.
        #[serde(default)]
        write_values: BTreeMap<Key, Vec<u8>>,
        execute_at: Timestamp,
        deps: BTreeSet<TxnId>,
        /// Whether the transaction is read-only.
        #[serde(default)]
        read_only: bool,
        /// The **ballot** this commit was decided under (ADR 0011): `Ballot::ZERO`
        /// for the original coordinator, the recovery ballot for a recovered one.
        /// Recorded so a restarted replica keeps the highest commit-ballot it saw
        /// and still fences a stale lower-ballot `Commit`. `#[serde(default)]` ⇒
        /// `Ballot::ZERO` for forward-compat.
        #[serde(default)]
        commit_ballot: Ballot,
    },
    /// The transaction's effect was applied to the store (executed). Recorded so
    /// a recovered replica does not re-apply an already-applied effect and
    /// reconstructs the same execution order.
    Applied { txn: TxnId },
    /// A **snapshot** of the durable replica state (ADR 0011, log truncation): a
    /// single record carrying the compact image of every transaction the core
    /// still tracks, plus the recovered execution order. The driver writes this at
    /// the head of a freshly-**replaced** WAL, collapsing the per-phase history of
    /// every covered transaction into this one record, then appends only the live
    /// tail afterwards. Replay folds it **first** (it is the base every later record
    /// builds on); a WAL with no `Snapshot` replays exactly as before (additive).
    ///
    /// The per-transaction facts ride as a **`Vec` of `(TxnId, PersistedTxn)`
    /// entries**, not a `BTreeMap` — a JSON object cannot key on a `Timestamp`
    /// struct. [`PersistedState::snapshot_record`] / replay convert to and from the
    /// map form.
    Snapshot {
        txns: Vec<(TxnId, PersistedTxn)>,
        applied_order: Vec<TxnId>,
    },
}

impl WalRecord {
    /// The transaction this record concerns. A [`WalRecord::Snapshot`] covers many
    /// transactions at once, so it has no single id — it returns
    /// [`TxnId::ZERO`](crate::Timestamp::ZERO); callers handle `Snapshot`
    /// explicitly (see [`PersistedState::replay`]) and never key it by `txn()`.
    #[must_use]
    pub fn txn(&self) -> TxnId {
        match self {
            WalRecord::PreAccepted { txn, .. }
            | WalRecord::Accepted { txn, .. }
            | WalRecord::Committed { txn, .. }
            | WalRecord::Promised { txn, .. }
            | WalRecord::Applied { txn } => *txn,
            WalRecord::Snapshot { .. } => crate::timestamp::Timestamp::ZERO,
        }
    }
}

/// The durable replica facts about one transaction, rebuilt by folding the WAL.
/// `Serialize`/`Deserialize` so it can ride inside a [`WalRecord::Snapshot`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedTxn {
    /// The transaction's conflict key set (known once PreAccepted or Committed).
    pub keys: BTreeSet<Key>,
    /// The subset of `keys` the transaction writes (its write effect's targets).
    pub write_keys: BTreeSet<Key>,
    /// Caller-supplied value bytes per written key (arbitrary write values, ADR
    /// 0011). A `write_keys` entry absent here executes as the txn id.
    pub write_values: BTreeMap<Key, Vec<u8>>,
    /// Best-known execution timestamp.
    pub execute_at: Timestamp,
    /// Best-known dependency set.
    pub deps: BTreeSet<TxnId>,
    /// The furthest phase reached durably.
    pub phase: Phase,
    /// Whether the transaction's effect was applied (executed) durably.
    pub applied: bool,
    /// Whether the transaction is read-only (a read snapshot, no write effect).
    pub read_only: bool,
    /// The highest recovery ballot this replica promised durably (ADR 0011). A
    /// restarted replica must not accept a `Recover`/`Accept` below this, or a
    /// superseded recoverer could win. [`Ballot::ZERO`] if it never promised.
    pub promised: Ballot,
    /// The ballot under which `execute_at`/`deps` were last accepted (via
    /// `Accept`). Reported in `RecoverOk` so a later recoverer adopts the
    /// highest-ballot proposal. [`Ballot::ZERO`] if only PreAccepted.
    pub accepted_ballot: Ballot,
    /// The highest ballot a **`Commit`** for this transaction was decided under
    /// (ADR 0011). A restarted replica fences any later `Commit` below this, so a
    /// stale original-coordinator commit cannot revert a recovered decision.
    /// [`Ballot::ZERO`] if not committed, or committed only by the original.
    pub commit_ballot: Ballot,
}

/// Durable Accord replica state reconstructed by replaying the write-ahead log.
/// `Serialize`/`Deserialize` so the whole image can ride inside a single
/// [`WalRecord::Snapshot`] for log truncation.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedState {
    /// Per-transaction durable facts, keyed by transaction id (== `t0`).
    pub txns: BTreeMap<TxnId, PersistedTxn>,
    /// The order in which transactions were applied (recovered execution order).
    pub applied_order: Vec<TxnId>,
}

impl PersistedState {
    /// Whether the log was empty (a never-before-run node).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.txns.is_empty()
    }

    /// Reconstruct durable state by folding the WAL records in order.
    pub fn replay(records: impl IntoIterator<Item = WalRecord>) -> Self {
        let mut state = Self::default();
        for record in records {
            let txn = record.txn();
            match record {
                WalRecord::PreAccepted {
                    keys,
                    write_keys,
                    write_values,
                    execute_at,
                    deps,
                    read_only,
                    ..
                } => {
                    let entry = state.txns.entry(txn).or_default();
                    entry.keys.extend(keys);
                    entry.write_keys.extend(write_keys);
                    entry.write_values.extend(write_values);
                    entry.execute_at = entry.execute_at.max(execute_at);
                    entry.deps.extend(deps);
                    entry.phase = entry.phase.max_phase(Phase::PreAccepted);
                    entry.read_only |= read_only;
                }
                WalRecord::Accepted {
                    execute_at,
                    deps,
                    accepted_ballot,
                    ..
                } => {
                    let entry = state.txns.entry(txn).or_default();
                    entry.execute_at = entry.execute_at.max(execute_at);
                    entry.deps.extend(deps);
                    entry.phase = entry.phase.max_phase(Phase::Accepted);
                    // The accepted ballot only advances (a later Accept ran under
                    // a higher ballot); keep the max so the recovered replica
                    // reports the most-recent proposal it ever accepted.
                    entry.accepted_ballot = entry.accepted_ballot.max(accepted_ballot);
                    // Accepting under a ballot also implies having promised it.
                    entry.promised = entry.promised.max(accepted_ballot);
                }
                WalRecord::Promised { ballot, .. } => {
                    let entry = state.txns.entry(txn).or_default();
                    entry.promised = entry.promised.max(ballot);
                }
                WalRecord::Committed {
                    keys,
                    write_keys,
                    write_values,
                    execute_at,
                    deps,
                    read_only,
                    commit_ballot,
                    ..
                } => {
                    let entry = state.txns.entry(txn).or_default();
                    // Keys/values/read-only are monotone facts — always fold them.
                    entry.keys.extend(keys);
                    entry.write_keys.extend(write_keys);
                    entry.write_values.extend(write_values);
                    entry.read_only |= read_only;
                    // Fence the **decision** by commit ballot: a `Commit` at a ballot
                    // below the highest already committed here is stale (e.g. a late
                    // original-coordinator commit replayed after a recovered one) and
                    // must not overwrite the higher-ballot `(execute_at, deps)`.
                    let not_yet_committed = entry.phase < Phase::Committed;
                    if not_yet_committed || commit_ballot >= entry.commit_ballot {
                        entry.execute_at = execute_at;
                        entry.deps = deps;
                        entry.commit_ballot = entry.commit_ballot.max(commit_ballot);
                    }
                    entry.phase = entry.phase.max_phase(Phase::Committed);
                }
                WalRecord::Applied { .. } => {
                    let entry = state.txns.entry(txn).or_default();
                    if !entry.applied {
                        entry.applied = true;
                        state.applied_order.push(txn);
                    }
                }
                WalRecord::Snapshot {
                    txns: snap_txns,
                    applied_order: snap_order,
                } => {
                    // The compact base every later record builds on. Fold it in
                    // monotonically (max/union) so replay stays order-insensitive
                    // even if a stray later record duplicates a snapshotted fact.
                    for (stxn, st) in snap_txns {
                        let entry = state.txns.entry(stxn).or_default();
                        entry.keys.extend(st.keys);
                        entry.write_keys.extend(st.write_keys);
                        entry.write_values.extend(st.write_values);
                        entry.execute_at = entry.execute_at.max(st.execute_at);
                        entry.deps.extend(st.deps);
                        entry.phase = entry.phase.max_phase(st.phase);
                        entry.applied |= st.applied;
                        entry.read_only |= st.read_only;
                        entry.promised = entry.promised.max(st.promised);
                        entry.accepted_ballot = entry.accepted_ballot.max(st.accepted_ballot);
                        entry.commit_ballot = entry.commit_ballot.max(st.commit_ballot);
                    }
                    // Preserve the snapshotted execution order, appending any txns
                    // not already recorded (the snapshot is the prefix; the tail
                    // adds later applies).
                    for stxn in snap_order {
                        if !state.applied_order.contains(&stxn) {
                            state.applied_order.push(stxn);
                        }
                    }
                }
            }
        }
        state
    }

    /// Encode a single record as one newline-terminated JSON line for the WAL.
    /// (`serde_json` never emits raw newlines, so the framing is unambiguous.)
    #[must_use]
    pub fn encode_record(record: &WalRecord) -> Vec<u8> {
        let mut bytes = serde_json::to_vec(record).expect("wal record serializes");
        bytes.push(b'\n');
        bytes
    }

    /// Decode the WAL bytes back into records, ignoring a trailing partial line
    /// (a write torn by a crash — its effect was never acted on).
    pub fn decode(bytes: &[u8]) -> Vec<WalRecord> {
        bytes
            .split(|&b| b == b'\n')
            .filter(|line| !line.is_empty())
            .filter_map(|line| serde_json::from_slice(line).ok())
            .collect()
    }
}
