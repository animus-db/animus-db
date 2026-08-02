//! The Accord wire messages exchanged between replicas.
//!
//! Higher layers (de)serialize these with `serde_json` over the `Vec<u8>`
//! payloads the `Network` moves, exactly like the control plane's `RaftMsg`.

use std::collections::{BTreeMap, BTreeSet};

use animus_env::NodeId;
use serde::{Deserialize, Serialize};

use crate::core::{Key, Phase, TxnId};
use crate::timestamp::{Ballot, Timestamp};

/// A message between Accord replicas. The happy path is
/// `PreAccept`/`PreAcceptOk` then `Commit`; the slow path inserts an
/// `Accept`/`AcceptOk` round when the fast quorum did not agree on `t0`.
///
/// On top of the steady-state protocol there is a **recovery** sub-protocol
/// (`Recover` / `RecoverOk`) that a *new* coordinator runs to take over a
/// transaction whose original coordinator died mid-flight before the replicas
/// learned the `Commit` — see [`crate::core`] and ADR 0011.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccordMsg {
    /// Coordinator → replicas: proposes transaction `txn` (its `t0`) over `keys`
    /// (its full conflict set — every key read *or* written). `write_keys` is the
    /// subset it writes (empty ⇒ a pure read; equal to `keys` ⇒ a pure write; a
    /// non-empty strict subset ⇒ a read-modify-write whose extra `keys` are
    /// read-only and participate only in conflict/dependency tracking).
    /// `read_only` marks a transaction that writes nothing — ordered exactly like
    /// a write, only its execution *effect* differs.
    PreAccept {
        txn: TxnId,
        keys: BTreeSet<Key>,
        #[serde(default)]
        write_keys: BTreeSet<Key>,
        /// Caller-supplied value bytes per written key (arbitrary write values,
        /// ADR 0011). A `write_keys` entry absent here executes as the txn id.
        /// Empty for valueless callers (`submit`/`submit_rw`).
        #[serde(default)]
        write_values: BTreeMap<Key, Vec<u8>>,
        read_only: bool,
    },
    /// Replica → coordinator: the timestamp this replica proposes for `txn`
    /// (`t0` unless a conflict bumped it) and the conflicting transactions it
    /// has seen (`txn`'s dependencies).
    PreAcceptOk {
        txn: TxnId,
        ts: Timestamp,
        deps: BTreeSet<TxnId>,
    },
    /// Coordinator → replicas (slow path): adopt this execution timestamp and
    /// dependency set for `txn`. `ballot` is the proposal number this round runs
    /// under (ADR 0011, recovery ballots): the **original** coordinator uses the
    /// implicit [`Ballot::ZERO`](crate::Ballot::ZERO); a **recovery** coordinator
    /// uses the higher ballot it adopted. A replica rejects an `Accept` whose
    /// ballot is below the one it has promised (a higher recoverer superseded
    /// it), replying [`AccordMsg::AcceptNack`] with the promised ballot.
    Accept {
        txn: TxnId,
        /// The proposal ballot this `Accept` runs under (`Ballot::ZERO` for the
        /// original coordinator). `#[serde(default)]` so the field is additive.
        #[serde(default)]
        ballot: Ballot,
        execute_at: Timestamp,
        deps: BTreeSet<TxnId>,
    },
    /// Replica → coordinator: acknowledges the `Accept` (the replica promised
    /// this `ballot` and adopted the `(execute_at, deps)`).
    AcceptOk { txn: TxnId },
    /// Replica → coordinator: **rejects** an `Accept` whose ballot is below the
    /// ballot this replica has already promised — a higher recovery coordinator
    /// has superseded the sender. `promised` is that higher ballot, so the sender
    /// (a stale recoverer or the original coordinator) learns it has been
    /// superseded and must not proceed. (ADR 0011, duelling recoverers.)
    AcceptNack {
        txn: TxnId,
        /// The highest ballot this replica has promised (strictly above the
        /// rejected `Accept`'s ballot).
        promised: Ballot,
    },
    /// Coordinator → replicas: the agreed final execution timestamp and deps.
    /// Carries `read_only` so a replica that learns the transaction only at
    /// `Commit` (missed its `PreAccept`) still knows to execute it as a read.
    Commit {
        txn: TxnId,
        /// The **ballot** this commit was decided under (ADR 0011): the original
        /// coordinator commits at the implicit [`Ballot::ZERO`](crate::Ballot::ZERO);
        /// a *recovery* coordinator at the higher ballot it ran. A replica records
        /// the highest commit-ballot it has seen and **ignores a `Commit` whose
        /// ballot is below it**, so a late original-coordinator commit cannot revert
        /// a higher-ballot recovered decision after a heal (the failure-detector
        /// heal race). `#[serde(default)]` for additivity (absent ⇒ `Ballot::ZERO`).
        #[serde(default)]
        ballot: Ballot,
        execute_at: Timestamp,
        deps: BTreeSet<TxnId>,
        /// The subset of the transaction's keys it writes (so a replica that
        /// learns the transaction only at `Commit` still executes the correct
        /// write effect). Empty for a read-only transaction.
        #[serde(default)]
        write_keys: BTreeSet<Key>,
        /// Caller-supplied value bytes per written key (arbitrary write values,
        /// ADR 0011), so a replica that learns the transaction only at `Commit`
        /// writes the right value. A key absent executes as the txn id.
        #[serde(default)]
        write_values: BTreeMap<Key, Vec<u8>>,
        read_only: bool,
    },
    /// Replica → coordinator: acknowledges the `Commit` was recorded. `Commit`
    /// is otherwise fire-and-forget; this ack lets the coordinator's retry tick
    /// stop re-sending `Commit` to a replica once it has it (ADR 0011, message
    /// retry). Idempotent — a duplicate `Commit` produces a duplicate ack.
    CommitAck { txn: TxnId },
    /// Recovery coordinator → replicas: "tell me everything you recorded about
    /// `txn`, and **promise not to accept a lower ballot**". Sent by a *new*
    /// coordinator taking over a transaction whose original coordinator is
    /// suspected dead. `ballot` is the proposal number this recoverer runs under
    /// (ADR 0011, recovery ballots): a replica promises it (rejecting any later,
    /// lower ballot) iff it is `>=` the highest ballot the replica has promised;
    /// otherwise the replica replies [`AccordMsg::RecoverNack`] so this recoverer
    /// learns it was superseded and must retry at a higher ballot.
    Recover {
        txn: TxnId,
        /// The recovery ballot this query runs under. `#[serde(default)]` for
        /// additivity (an absent ballot decodes to [`Ballot::ZERO`]).
        #[serde(default)]
        ballot: Ballot,
    },
    /// Replica → recovery coordinator: this replica's recorded state for `txn`,
    /// or a default (PreAccepted-with-`t0`) view if it had never heard of it (it
    /// witnesses `txn` as part of replying, so it now participates). Sent only
    /// when the replica **promised** the `Recover`'s ballot.
    RecoverOk {
        txn: TxnId,
        /// The ballot the recoverer queried under and this replica promised
        /// (echoed so a recoverer ignores a `RecoverOk` for a superseded ballot).
        #[serde(default)]
        ballot: Ballot,
        /// The furthest phase this replica reached for `txn`.
        phase: Phase,
        /// The best-known execution timestamp (`t0` until raised).
        execute_at: Timestamp,
        /// The best-known dependency set.
        deps: BTreeSet<TxnId>,
        /// The ballot under which this replica last **accepted** the reported
        /// `(execute_at, deps)` (via `Accept`), or [`Ballot::ZERO`] if it has
        /// only PreAccepted. The recoverer adopts the `(execute_at, deps)` of the
        /// reply with the **highest** accepted ballot — the most recent proposal
        /// any replica committed to — so duelling recoverers converge. (ADR 0011.)
        #[serde(default)]
        accepted_ballot: Ballot,
        /// The transaction's full conflict key set, as known to this replica.
        keys: BTreeSet<Key>,
        /// The subset of `keys` the transaction writes, as known to this replica.
        #[serde(default)]
        write_keys: BTreeSet<Key>,
        /// The caller-supplied write values this replica recorded (arbitrary
        /// write values, ADR 0011); recovery unions them across the quorum.
        #[serde(default)]
        write_values: BTreeMap<Key, Vec<u8>>,
        /// Whether this replica recorded the transaction as read-only.
        read_only: bool,
    },
    /// Replica → recovery coordinator: **rejects** a `Recover` whose ballot is
    /// below the ballot this replica has already promised — a higher recovery
    /// coordinator exists. `promised` is that higher ballot, so the superseded
    /// recoverer can retry at a strictly higher ballot (or give up). (ADR 0011,
    /// duelling recoverers.)
    RecoverNack {
        txn: TxnId,
        /// The highest ballot this replica has promised (above the rejected
        /// `Recover`'s ballot).
        promised: Ballot,
    },
}

/// An outbound message: `(destination, message)`.
pub type Out = (NodeId, AccordMsg);
