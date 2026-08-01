//! The Accord wire messages exchanged between replicas.
//!
//! Higher layers (de)serialize these with `serde_json` over the `Vec<u8>`
//! payloads the `Network` moves, exactly like the control plane's `RaftMsg`.

use std::collections::BTreeSet;

use custos_env::NodeId;
use serde::{Deserialize, Serialize};

use crate::core::{Key, TxnId};
use crate::timestamp::Timestamp;

/// A message between Accord replicas. The happy path is
/// `PreAccept`/`PreAcceptOk` then `Commit`; the slow path inserts an
/// `Accept`/`AcceptOk` round when the fast quorum did not agree on `t0`.
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
}

/// An outbound message: `(destination, message)`.
pub type Out = (NodeId, AccordMsg);
