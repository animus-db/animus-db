//! The Accord wire messages exchanged between replicas.
//!
//! Higher layers (de)serialize these with `serde_json` over the `Vec<u8>`
//! payloads the `Network` moves, exactly like the control plane's `RaftMsg`.

use std::collections::BTreeSet;

use animus_env::NodeId;
use serde::{Deserialize, Serialize};

use crate::core::{Key, Phase, TxnId};
use crate::timestamp::Timestamp;

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
    /// dependency set for `txn`.
    Accept {
        txn: TxnId,
        execute_at: Timestamp,
        deps: BTreeSet<TxnId>,
    },
    /// Replica → coordinator: acknowledges the `Accept`.
    AcceptOk { txn: TxnId },
    /// Coordinator → replicas: the agreed final execution timestamp and deps.
    /// Carries `read_only` so a replica that learns the transaction only at
    /// `Commit` (missed its `PreAccept`) still knows to execute it as a read.
    Commit {
        txn: TxnId,
        execute_at: Timestamp,
        deps: BTreeSet<TxnId>,
        /// The subset of the transaction's keys it writes (so a replica that
        /// learns the transaction only at `Commit` still executes the correct
        /// write effect). Empty for a read-only transaction.
        #[serde(default)]
        write_keys: BTreeSet<Key>,
        read_only: bool,
    },
    /// Replica → coordinator: acknowledges the `Commit` was recorded. `Commit`
    /// is otherwise fire-and-forget; this ack lets the coordinator's retry tick
    /// stop re-sending `Commit` to a replica once it has it (ADR 0011, message
    /// retry). Idempotent — a duplicate `Commit` produces a duplicate ack.
    CommitAck { txn: TxnId },
    /// Recovery coordinator → replicas: "tell me everything you recorded about
    /// `txn`". Sent by a *new* coordinator taking over a transaction whose
    /// original coordinator is suspected dead.
    Recover { txn: TxnId },
    /// Replica → recovery coordinator: this replica's recorded state for `txn`,
    /// or a default (PreAccepted-with-`t0`) view if it had never heard of it (it
    /// witnesses `txn` as part of replying, so it now participates).
    RecoverOk {
        txn: TxnId,
        /// The furthest phase this replica reached for `txn`.
        phase: Phase,
        /// The best-known execution timestamp (`t0` until raised).
        execute_at: Timestamp,
        /// The best-known dependency set.
        deps: BTreeSet<TxnId>,
        /// The transaction's full conflict key set, as known to this replica.
        keys: BTreeSet<Key>,
        /// The subset of `keys` the transaction writes, as known to this replica.
        #[serde(default)]
        write_keys: BTreeSet<Key>,
        /// Whether this replica recorded the transaction as read-only.
        read_only: bool,
    },
}

/// An outbound message: `(destination, message)`.
pub type Out = (NodeId, AccordMsg);
