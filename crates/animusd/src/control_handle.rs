//! The `ControlHandle` seam (ADR 0035 PR1).
//!
//! Today every `animusd` node's [`crate::ClientCtx`] reaches the control plane
//! through a bare `RaftNode<ProdEnv>` — this process's own in-process control
//! Raft replica. ADR 0035 splits `animusd` into a small control-only
//! deployment and a data-only fleet with **no local control `RaftCore` at
//! all**, reaching the control deployment over the network instead. This
//! module introduces the seam that split needs, but wires up only the
//! `Local` variant: every method here delegates straight to the same
//! `RaftNode<ProdEnv>` calls `ClientCtx` already made directly, so this PR is
//! a pure refactor with no behavior change on any node that exists today
//! (every one of them is `Local`).
//!
//! PR4 adds `ControlHandle::Remote(RemoteControlClient)` for a data-only node
//! and implements its side of the two read methods below (a polled/long-poll
//! mirror for [`metadata_cached`](ControlHandle::metadata_cached), a
//! leader-directed `Status` fetch for
//! [`metadata_fresh`](ControlHandle::metadata_fresh)) — see ADR 0035 §"Key
//! mechanism decisions" #1.

use std::collections::BTreeSet;

use animus_control::{Metadata, MetadataWatch, RaftNode, Role};
use animus_env::{MetricsHandle, NodeId, ProdEnv};

/// This node's access to the control plane's replicated [`Metadata`] and
/// this-node's-own-Raft-state introspection (`/admin/raft`, `/metrics`).
///
/// **Not** a proposal path: proposing a `MetaCommand` always goes through
/// whichever locally-registered handle currently believes it is leader
/// (`ClusterEdgeState::leader_handle()`, deliberately kept as a concrete
/// `RaftNode<ProdEnv>` — see that type's doc), never through `self.control`
/// directly, because "propose" is inherently a *local-Raft-log* operation
/// that only a genuine control-group voter can perform at all; a future
/// `Remote` handle (no local `RaftCore`) has no such operation to expose
/// here — it would have to relay over the network instead, which is exactly
/// what the leader-handle relay already does. `ControlHandle::propose`/
/// `flush` were dropped from this seam for exactly that reason (no current
/// call site needs them, and no future `Remote` variant would implement them
/// the same way its `Local` sibling does).
///
/// `Local` wraps this process's own in-process control [`RaftNode`] — today's
/// only variant; every existing `animusd` node (combined mode, or an ADR 0030
/// growth node) holds one. `Clone` because `RaftNode` is `Arc`-backed
/// internally (cheap to clone, shares state); `ControlHandle` inherits that.
///
/// Reads are split by freshness contract, mirroring the pre-existing
/// `RaftNode::metadata()` / `ClientCtx::effective_metadata()` split:
/// - [`metadata_cached`](Self::metadata_cached) — staleness-tolerant. For
///   `Local` this is just `RaftNode::metadata()`; `ClientCtx::
///   effective_metadata()` layers the ADR 0030 growth-node mirror on top of
///   this for the call sites that need it, exactly as it does today.
/// - [`metadata_fresh`](Self::metadata_fresh) — must reflect the control
///   leader's own committed state (read-your-writes), never a mirror
///   substitution. For `Local` this is identical to `metadata_cached` today —
///   a control-group voter's own applied state already *is* the RYW source
///   (the growth-node mirror is a `ClientCtx`-level concern, layered in
///   `effective_metadata`, not something `Local` itself ever substitutes).
///   This method exists so PR4's `Remote` can implement it as a
///   leader-directed fetch (proxying a `Status` request to the control
///   deployment, mirroring `propose_schema`'s existing relay shape) instead.
#[derive(Clone)]
pub(crate) enum ControlHandle {
    Local(RaftNode<ProdEnv>),
}

impl ControlHandle {
    /// Staleness-tolerant read of the control plane's replicated `Metadata` —
    /// see the type doc for the freshness contract. `ClientCtx::
    /// effective_metadata()` is almost always the right thing to call
    /// instead of this directly (it layers the growth-node mirror on top);
    /// this is the primitive that both it and `metadata_fresh` (today) build
    /// on.
    pub(crate) fn metadata_cached(&self) -> Metadata {
        match self {
            Self::Local(raft) => raft.metadata(),
        }
    }

    /// Read-your-writes read of the control plane's replicated `Metadata` —
    /// see the type doc for the freshness contract. Used by the schema
    /// commit-wait polls and the DynamoDB conditional-write existence gate,
    /// which must observe their own just-proposed command landing in the
    /// authoritative state, never a mirror that could still be a poll
    /// interval behind.
    pub(crate) fn metadata_fresh(&self) -> Metadata {
        match self {
            Self::Local(raft) => raft.metadata(),
        }
    }

    /// Whether this handle currently believes it is the control-plane leader.
    pub(crate) fn is_leader(&self) -> bool {
        match self {
            Self::Local(raft) => raft.is_leader(),
        }
    }

    /// The current leader's id, if this handle knows one.
    pub(crate) fn leader(&self) -> Option<NodeId> {
        match self {
            Self::Local(raft) => raft.leader(),
        }
    }

    /// This handle's current Raft role (introspection, `/admin/raft`).
    pub(crate) fn role(&self) -> Role {
        match self {
            Self::Local(raft) => raft.role(),
        }
    }

    /// This handle's current Raft term (introspection, `/admin/raft`).
    pub(crate) fn term(&self) -> u64 {
        match self {
            Self::Local(raft) => raft.term(),
        }
    }

    /// This handle's commit index (introspection, `/admin/raft`).
    pub(crate) fn commit_index(&self) -> u64 {
        match self {
            Self::Local(raft) => raft.commit_index(),
        }
    }

    /// This handle's last-applied index — also the pre-recovery-replay guard
    /// the tablet-host reconciler trigger reads directly (see
    /// `tablet_host_reconciler_loop`'s doc).
    pub(crate) fn last_applied(&self) -> u64 {
        match self {
            Self::Local(raft) => raft.last_applied(),
        }
    }

    /// This handle's durable (fsynced) index (introspection, `/admin/raft`).
    pub(crate) fn durable_index(&self) -> u64 {
        match self {
            Self::Local(raft) => raft.durable_index(),
        }
    }

    /// This handle's snapshot index (introspection, `/admin/raft`).
    pub(crate) fn snapshot_index(&self) -> u64 {
        match self {
            Self::Local(raft) => raft.snapshot_index(),
        }
    }

    /// This handle's in-memory log length (introspection, `/admin/raft`).
    pub(crate) fn log_len(&self) -> usize {
        match self {
            Self::Local(raft) => raft.log_len(),
        }
    }

    /// This handle's current voter configuration (introspection,
    /// `/admin/raft`).
    pub(crate) fn config(&self) -> BTreeSet<NodeId> {
        match self {
            Self::Local(raft) => raft.config(),
        }
    }

    /// This handle's recording metrics sink (aggregated into the `/metrics`
    /// export alongside the raftkv-role sink).
    pub(crate) fn metrics(&self) -> &MetricsHandle {
        match self {
            Self::Local(raft) => raft.metrics(),
        }
    }

    /// An executor-agnostic "applied index advanced" notification — see
    /// `MetadataWatch`'s own doc (ADR 0031 §trigger). Same-process only; PR5
    /// upgrades a `Remote` handle to a long-poll equivalent (ADR 0035 §4).
    pub(crate) fn metadata_watch(&self) -> MetadataWatch {
        match self {
            Self::Local(raft) => raft.metadata_watch(),
        }
    }

    /// Whether this handle's failure detector currently believes `member` is
    /// alive (`/admin/raft`'s per-member view).
    pub(crate) fn believes_alive(&self, member: NodeId) -> bool {
        match self {
            Self::Local(raft) => raft.believes_alive(member),
        }
    }
}
