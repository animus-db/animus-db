//! The Accord wire messages exchanged between replicas.
//!
//! Higher layers (de)serialize these with `serde_json` over the `Vec<u8>`
//! payloads the `Network` moves, exactly like the control plane's `RaftMsg`.

use std::collections::BTreeSet;

use custos_env::NodeId;
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
    /// Coordinator → replicas: proposes transaction `txn` (its `t0`) over `keys`.
    PreAccept { txn: TxnId, keys: BTreeSet<Key> },
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
    Commit {
        txn: TxnId,
        execute_at: Timestamp,
        deps: BTreeSet<TxnId>,
    },
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
        /// The transaction's key set, as known to this replica.
        keys: BTreeSet<Key>,
    },
}

/// An outbound message: `(destination, message)`.
pub type Out = (NodeId, AccordMsg);
