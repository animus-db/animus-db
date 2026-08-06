//! The EPaxos wire messages exchanged between replicas.
//!
//! Higher layers (de)serialize these with `serde_json` over the `Vec<u8>`
//! payloads the `Network` moves, exactly like `animus-consensus`'s `AccordMsg`
//! and the control plane's `RaftMsg`.
//!
//! The steady-state protocol is `PreAccept`/`PreAcceptOk` then `Commit` on the
//! fast path, inserting an `Accept`/`AcceptOk` round on the slow path when the
//! fast quorum did not report identical attributes. The **recovery** sub-protocol
//! (`Prepare`/`PrepareOk`, the piece EPaxos is notorious for) is **deferred** —
//! see the crate docs — so a dead command leader currently strands its instance.

use std::collections::BTreeSet;

use animus_env::NodeId;
use serde::{Deserialize, Serialize};

use crate::core::Key;
use crate::instance::InstanceId;

/// A message between EPaxos replicas.
///
/// Every message names the [`InstanceId`] it concerns. `seq` and `deps` are the
/// two command *attributes* EPaxos agrees on: `deps` is the set of interfering
/// instances (the dependency graph edges) and `seq` is a sequence number one
/// greater than the max `seq` of those deps (the cycle-breaker the execution
/// order uses inside a strongly-connected component).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EPaxosMsg {
    /// Command leader → replicas: propose `instance` over `keys` with the
    /// leader's initial `(seq, deps)`.
    PreAccept {
        instance: InstanceId,
        keys: BTreeSet<Key>,
        seq: u64,
        deps: BTreeSet<InstanceId>,
    },
    /// Replica → leader: the replica's view of `(seq, deps)` after merging its
    /// own conflicting instances into the leader's proposal. The fast path fires
    /// iff every fast-quorum reply reports **identical** attributes.
    PreAcceptOk {
        instance: InstanceId,
        seq: u64,
        deps: BTreeSet<InstanceId>,
    },
    /// Command leader → replicas (slow path): adopt this `(seq, deps)` — the max
    /// `seq` and union of `deps` across the PreAccept replies.
    Accept {
        instance: InstanceId,
        keys: BTreeSet<Key>,
        seq: u64,
        deps: BTreeSet<InstanceId>,
    },
    /// Replica → leader: acknowledges the `Accept`.
    AcceptOk { instance: InstanceId },
    /// Command leader → replicas: the final agreed `(seq, deps)`. A replica that
    /// learns the command only at `Commit` (missed its `PreAccept`) still gets the
    /// full `keys`/attributes, so it can order and (once the executor lands)
    /// execute it.
    Commit {
        instance: InstanceId,
        keys: BTreeSet<Key>,
        seq: u64,
        deps: BTreeSet<InstanceId>,
    },
}

/// An outbound message: `(destination, message)`.
pub type Out = (NodeId, EPaxosMsg);
