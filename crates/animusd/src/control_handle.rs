//! The `ControlHandle` seam (ADR 0035 PR1, extended by PR4 and PR5).
//!
//! Before this seam, every `animusd` node's [`crate::ClientCtx`] reached the
//! control plane through a bare `RaftNode<ProdEnv>` — this process's own
//! in-process control Raft replica. ADR 0035 splits `animusd` into a small
//! control-only deployment and a data-only fleet with **no local control
//! `RaftCore` at all**, reaching the control deployment over the network
//! instead. PR1 introduced the seam with only the `Local` variant wired up
//! (a pure refactor — every method delegated straight to the same
//! `RaftNode<ProdEnv>` calls `ClientCtx` already made directly, so no node
//! that existed then — every one of them `Local` — changed behavior).
//!
//! **PR4 adds `ControlHandle::Remote(RemoteControlClient)`** for a data-only
//! node: a polled mirror for
//! [`metadata_cached`](ControlHandle::metadata_cached) (kept fresh by the
//! generalized `remote_metadata_sync_loop`, ADR 0035 §4) and a genuine
//! leader-directed network fetch for
//! [`metadata_fresh`](ControlHandle::metadata_fresh) (ADR 0035 §1) — see
//! [`RemoteControlClient`]'s own doc for both, plus the leader-hint lifecycle
//! that backs [`ControlHandle::leader_addr_hint`]. PR5 upgrades
//! `metadata_cached`'s fixed-interval poll to a long-poll watch and audits
//! every `Remote` degrade below against real staleness/liveness traffic.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use animus_control::{Metadata, MetadataWatch, RaftNode, Role};
use animus_env::{MetricsHandle, NodeId, ProdEnv};

use crate::{ClientRequest, ClientResponse, relay_request};

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
    /// A data-only node's control-plane access (ADR 0035 PR4): no local
    /// `RaftCore` at all. See [`RemoteControlClient`]'s own doc.
    Remote(RemoteControlClient),
}

/// A data-only node's (ADR 0035 PR4) access to the **separately-deployed**
/// control plane: no local `RaftCore`, so every read is either a poll of a
/// mirror (`metadata_cached`, refreshed by the generalized
/// `remote_metadata_sync_loop`) or a direct live fetch (`metadata_fresh`).
/// Cheap to clone (every field is `Arc`-backed), mirroring `RaftNode`'s own
/// shape.
///
/// **Also reused, standalone, by an ADR 0030 growth node (ADR 0035 PR5).** A
/// growth node's `ClientCtx.control` stays `ControlHandle::Local` (it is a
/// real, permanently non-voting control-group member, not a data-only node),
/// so it never holds one of these inside a `ControlHandle::Remote` — but
/// `crate::remote_metadata_sync_loop`'s growth-node branch constructs one
/// directly via [`with_mirror`](Self::with_mirror), sharing
/// `ClientCtx.remote_metadata` as its `mirror`, purely to drive
/// `crate::remote_metadata_watch_loop` (the identical long-poll logic) —
/// see that constructor's doc.
///
/// **Leader-hint lifecycle (ADR 0035 §1).** Every `ClientRequest::Status`
/// reply now carries the answering node's own `self.control.leader()` +
/// `route_addr(leader_id)` as `leader_hint: Option<(NodeId, SocketAddr)>`
/// (`#[serde(default)]`, so an old reply without the field still parses as
/// `None`). This handle keeps the most recent one it has seen — updated by
/// the periodic sync loop *and* by [`metadata_fresh`](Self::metadata_fresh)'s
/// own live fetch — and hands it out via [`leader`](Self::leader) (the id)
/// and [`leader_addr_hint`](Self::leader_addr_hint) (the client-API address,
/// which `ClientCtx::propose_schema` prefers over a `route_addr` lookup —
/// see that method's doc). The hint is *never* independently verified against
/// the answering node's own role; see `metadata_fresh`'s doc for the
/// consequence — audited in PR5 (see `crate::remote_metadata_watch_loop`'s
/// doc) and found self-healing in practice: every control node's own
/// `leader()` is kept current by real Raft heartbeats/AppendEntries (not the
/// ADR 0035 mirror-poll-interval class of staleness), and the periodic
/// full-seed-list sync (not just the hint) refreshes it from whichever
/// control node answers, so a stale hint corrects itself within one sync
/// cycle even if the hinted address itself has gone unreachable.
///
/// **Applied-index watch (ADR 0035 PR5).** `watch` is this handle's own
/// same-process [`MetadataWatch`] — disconnected from any `RaftCore` (this
/// node has none), but driven directly by [`observe`](Self::observe) from
/// the watermark every `Status`/`WatchMetadata` reply now carries. This is
/// what lets [`ControlHandle::metadata_watch`] hand the tablet-host
/// reconciler a *real* wake-on-change signal for a data-only node instead of
/// the permanently-disconnected default PR4 shipped with — see
/// `crate::remote_metadata_watch_loop`'s doc for how it's kept current.
#[derive(Clone)]
pub(crate) struct RemoteControlClient {
    /// The control deployment's own **client**-API addresses — the discovery
    /// root for the mirror sync loop and `metadata_fresh`'s fallback scan.
    /// Static for this node's lifetime (the control group itself is static,
    /// ADR 0030); every actual hop still prefers `leader_hint` first.
    seeds: Vec<SocketAddr>,
    /// The last `Metadata` observed from any control node's `Status` reply.
    /// `None` until the very first sync — the readiness signal
    /// [`has_synced`](Self::has_synced) exposes for the tablet-host
    /// reconciler's pre-recovery guard, which otherwise has no local
    /// `last_applied()` to gate on (this handle's is pinned at 0 forever).
    mirror: Arc<Mutex<Option<Metadata>>>,
    /// The last-known control-plane leader `(id, client address)` — see the
    /// type doc's "leader-hint lifecycle" section.
    leader_hint: Arc<Mutex<Option<(NodeId, SocketAddr)>>>,
    /// This handle's own applied-index watch (ADR 0035 PR5) — see the type
    /// doc's "applied-index watch" section. Bumped only by
    /// [`observe`](Self::observe); handed out (cloned) via
    /// [`metadata_watch`](Self::metadata_watch).
    watch: MetadataWatch,
    /// No-op metrics sink: a data-only node's control-plane access has no
    /// local Raft loops to instrument (unlike `Local`, whose `RaftNode`
    /// records into its own env's real sink).
    metrics: MetricsHandle,
}

impl RemoteControlClient {
    pub(crate) fn new(seeds: Vec<SocketAddr>) -> Self {
        Self::with_mirror(seeds, Arc::new(Mutex::new(None)))
    }

    /// Like [`new`](Self::new), but shares an existing `mirror` `Arc` instead
    /// of starting a fresh, empty one.
    ///
    /// Used by `crate::remote_metadata_sync_loop`'s ADR 0030 growth-node
    /// branch (ADR 0035 PR5's long-poll port of it): a growth node's
    /// `ClientCtx.control` stays `ControlHandle::Local` (it *is* a real, if
    /// permanently non-voting, control-group member — unlike a genuine ADR
    /// 0035 PR4 data-only node), so there is no `ControlHandle::Remote` to
    /// hold a full `RemoteControlClient`. But the long-poll watch loop
    /// (`crate::remote_metadata_watch_loop`) is otherwise identical for both
    /// cases, so the growth-node branch constructs a standalone
    /// `RemoteControlClient` here — never installed into `ControlHandle` —
    /// sharing `ClientCtx.remote_metadata` directly as its `mirror` so every
    /// existing reader of that field (`effective_metadata()`) keeps working
    /// unchanged, with no separate copy of the mirror to keep in sync.
    pub(crate) fn with_mirror(
        seeds: Vec<SocketAddr>,
        mirror: Arc<Mutex<Option<Metadata>>>,
    ) -> Self {
        Self {
            seeds,
            mirror,
            leader_hint: Arc::new(Mutex::new(None)),
            watch: MetadataWatch::default(),
            metrics: MetricsHandle::noop(),
        }
    }

    /// The mirror's last synced value, or a default-empty `Metadata` if it
    /// has never synced (see [`has_synced`](Self::has_synced) for telling the
    /// two apart).
    pub(crate) fn metadata_cached(&self) -> Metadata {
        self.mirror
            .lock()
            .expect("remote control mirror poisoned")
            .clone()
            .unwrap_or_default()
    }

    /// Whether this handle's mirror has synced at least once.
    pub(crate) fn has_synced(&self) -> bool {
        self.mirror
            .lock()
            .expect("remote control mirror poisoned")
            .is_some()
    }

    pub(crate) fn leader(&self) -> Option<NodeId> {
        self.leader_hint
            .lock()
            .expect("leader hint poisoned")
            .as_ref()
            .map(|(id, _)| *id)
    }

    pub(crate) fn leader_addr_hint(&self) -> Option<SocketAddr> {
        self.leader_hint
            .lock()
            .expect("leader hint poisoned")
            .as_ref()
            .map(|(_, addr)| *addr)
    }

    /// This handle's own applied-index watch (ADR 0035 PR5) — see the type
    /// doc's "applied-index watch" section.
    pub(crate) fn metadata_watch(&self) -> MetadataWatch {
        self.watch.clone()
    }

    /// Record a `Status`/`WatchMetadata` reply's metadata + leader hint +
    /// applied-index watermark. Called by [`crate::remote_metadata_watch_loop`]
    /// and by [`metadata_fresh`](Self::metadata_fresh)'s own live fetch — both
    /// observe the identical wire shape, so both refresh the same state.
    ///
    /// **Non-regression guard (ADR 0035 PR5):** any control node may answer —
    /// not necessarily the most caught-up one — so a reply from a replica
    /// lagging behind one this handle already observed must not overwrite a
    /// fresher mirror with a staler snapshot. `watermark` is the answering
    /// node's own applied index at reply time, a monotonic proxy for how
    /// fresh `metadata` is; the mirror + watch only advance together
    /// (`watermark >= watch.latest()`, so a same-watermark reply still
    /// refreshes the mirror — e.g. an unchanged snapshot re-observed after a
    /// timed-out long-poll retry). The leader hint is taken unconditionally
    /// regardless — it self-heals independently (see the type doc) and isn't
    /// a snapshot whose *content* can regress the same way.
    pub(crate) fn observe(
        &self,
        metadata: Metadata,
        leader_hint: Option<(NodeId, SocketAddr)>,
        watermark: u64,
    ) {
        if let Some(hint) = leader_hint {
            *self.leader_hint.lock().expect("leader hint poisoned") = Some(hint);
        }
        if watermark >= self.watch.latest() {
            *self.mirror.lock().expect("remote control mirror poisoned") = Some(metadata);
            self.watch.bump(watermark);
        }
    }

    /// A live, leader-directed `Status` fetch (ADR 0035 §1): try the current
    /// leader hint first, falling back to a scan of every seed — mirroring
    /// `ClientCtx::propose_schema`'s own hint-first-then-scan relay shape
    /// (this handle has no `ClientCtx` of its own to reach `propose_schema`
    /// through, so it repeats the same policy directly over
    /// [`relay_request`](crate::relay_request)). Whichever reply lands also
    /// refreshes the mirror + hint + watch, exactly like the periodic sync
    /// loop.
    ///
    /// **Known looseness, audited in PR5** (see the type doc's "leader-hint
    /// lifecycle" section): like `propose_schema`'s broadcast fallback, this
    /// trusts whichever node answers — it does not independently verify the
    /// responder self-reports as the leader. A stale hint can therefore serve
    /// a reply one hop behind the real leader; the caller's own retry loop
    /// (e.g. `ClientCtx::propose_and_await`) re-invokes this on its next poll
    /// tick, and a non-leader's own `Status` reply carries *its* leader hint,
    /// so staleness self-heals within a couple of hops rather than
    /// compounding. Falls back to the current mirror if no seed answers at
    /// all, rather than blocking — the caller's own poll loop bounds the
    /// wait.
    pub(crate) async fn metadata_fresh(&self) -> Metadata {
        let mut candidates = Vec::with_capacity(self.seeds.len() + 1);
        if let Some(addr) = self.leader_addr_hint() {
            candidates.push(addr);
        }
        candidates.extend(self.seeds.iter().copied());
        for addr in candidates {
            if let ClientResponse::Status {
                metadata,
                leader_hint,
                watermark,
            } = relay_request(addr, &ClientRequest::Status).await
            {
                self.observe(metadata.clone(), leader_hint, watermark);
                return metadata;
            }
        }
        self.metadata_cached()
    }
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
            Self::Remote(remote) => remote.metadata_cached(),
        }
    }

    /// Read-your-writes read of the control plane's replicated `Metadata` —
    /// see the type doc for the freshness contract. Used by the schema
    /// commit-wait polls and the DynamoDB conditional-write existence gate,
    /// which must observe their own just-proposed command landing in the
    /// authoritative state, never a mirror that could still be a poll
    /// interval behind.
    ///
    /// `Local` stays a synchronous passthrough to `metadata_cached` (this
    /// node's own applied state already *is* the RYW source); `Remote`
    /// performs a genuine network round trip — see
    /// [`RemoteControlClient::metadata_fresh`]'s doc for its leader-directed
    /// fetch + fallback policy.
    pub(crate) async fn metadata_fresh(&self) -> Metadata {
        match self {
            Self::Local(raft) => raft.metadata(),
            Self::Remote(remote) => remote.metadata_fresh().await,
        }
    }

    /// Whether this handle currently believes it is the control-plane leader.
    /// Always `false` for `Remote` — a data-only node never holds any
    /// control-group Raft role at all, so "am I the leader" is meaningless
    /// for it (`ClientCtx::propose_schema`/`admin_drain`/`admin_remove_member`
    /// already treat "not the leader" as "relay or ask the operator to retry
    /// on the leader", which is exactly the right degrade here).
    pub(crate) fn is_leader(&self) -> bool {
        match self {
            Self::Local(raft) => raft.is_leader(),
            Self::Remote(_) => false,
        }
    }

    /// The current leader's id, if this handle knows one. For `Remote`, the
    /// id half of the [leader hint](RemoteControlClient) — see that type's
    /// doc.
    pub(crate) fn leader(&self) -> Option<NodeId> {
        match self {
            Self::Local(raft) => raft.leader(),
            Self::Remote(remote) => remote.leader(),
        }
    }

    /// The current leader's **client-API** address, if directly known (ADR
    /// 0035 PR4) — always `None` for `Local` (a genuine control-group voter
    /// has no separate notion of "the leader's client address"; callers that
    /// need one resolve it via `ClientCtx::route_addr` on `leader()`'s id).
    /// For `Remote`, this is the [leader hint](RemoteControlClient) a
    /// `Status` reply carries, which `ClientCtx::propose_schema` prefers over
    /// a `route_addr` lookup — the hint is strictly fresher for a data-only
    /// node, since it comes straight off the same `Status` reply that filled
    /// the mirror, whereas `route_addr` needs the leader's address to have
    /// separately synced into the replicated node-address book.
    pub(crate) fn leader_addr_hint(&self) -> Option<SocketAddr> {
        match self {
            Self::Local(_) => None,
            Self::Remote(remote) => remote.leader_addr_hint(),
        }
    }

    /// This handle's current Raft role (introspection, `/admin/raft`).
    /// `Remote` reports `Follower` — the closest honest approximation (it
    /// holds no vote, campaigns for nothing, and is certainly not `Leader`).
    pub(crate) fn role(&self) -> Role {
        match self {
            Self::Local(raft) => raft.role(),
            Self::Remote(_) => Role::Follower,
        }
    }

    /// This handle's current Raft term (introspection, `/admin/raft`). Always
    /// `0` for `Remote` — it has no local Raft term at all.
    pub(crate) fn term(&self) -> u64 {
        match self {
            Self::Local(raft) => raft.term(),
            Self::Remote(_) => 0,
        }
    }

    /// This handle's commit index (introspection, `/admin/raft`). Always `0`
    /// for `Remote` — see [`term`](Self::term)'s doc.
    pub(crate) fn commit_index(&self) -> u64 {
        match self {
            Self::Local(raft) => raft.commit_index(),
            Self::Remote(_) => 0,
        }
    }

    /// This handle's last-applied index — also the pre-recovery-replay guard
    /// the tablet-host reconciler trigger reads directly (see
    /// `tablet_host_reconciler_loop`'s doc). **Pinned at `0` forever for
    /// `Remote`** (it has no local Raft log to apply at all) — the
    /// reconciler's guard also ORs in [`has_synced_metadata`]
    /// (Self::has_synced_metadata) so a data-only node isn't permanently
    /// gated off by a signal that can never become true for it.
    pub(crate) fn last_applied(&self) -> u64 {
        match self {
            Self::Local(raft) => raft.last_applied(),
            Self::Remote(_) => 0,
        }
    }

    /// This handle's durable (fsynced) index (introspection, `/admin/raft`).
    /// Always `0` for `Remote` — see [`term`](Self::term)'s doc.
    pub(crate) fn durable_index(&self) -> u64 {
        match self {
            Self::Local(raft) => raft.durable_index(),
            Self::Remote(_) => 0,
        }
    }

    /// This handle's snapshot index (introspection, `/admin/raft`). Always
    /// `0` for `Remote` — see [`term`](Self::term)'s doc.
    pub(crate) fn snapshot_index(&self) -> u64 {
        match self {
            Self::Local(raft) => raft.snapshot_index(),
            Self::Remote(_) => 0,
        }
    }

    /// This handle's in-memory log length (introspection, `/admin/raft`).
    /// Always `0` for `Remote` — see [`term`](Self::term)'s doc.
    pub(crate) fn log_len(&self) -> usize {
        match self {
            Self::Local(raft) => raft.log_len(),
            Self::Remote(_) => 0,
        }
    }

    /// This handle's current voter configuration (introspection,
    /// `/admin/raft`). Always empty for `Remote` — it is not a voter of
    /// anything.
    pub(crate) fn config(&self) -> BTreeSet<NodeId> {
        match self {
            Self::Local(raft) => raft.config(),
            Self::Remote(_) => BTreeSet::new(),
        }
    }

    /// This handle's recording metrics sink (aggregated into the `/metrics`
    /// export alongside the raftkv-role sink). A `Remote` handle's sink is a
    /// permanent no-op (see [`RemoteControlClient`]'s doc) — the raftkv-role
    /// sink still records real counters on a data-only node.
    pub(crate) fn metrics(&self) -> &MetricsHandle {
        match self {
            Self::Local(raft) => raft.metrics(),
            Self::Remote(remote) => &remote.metrics,
        }
    }

    /// An executor-agnostic "applied index advanced" notification — see
    /// `MetadataWatch`'s own doc (ADR 0031 §trigger). Same-process only (the
    /// primitive itself never crosses a network hop) — but as of **ADR 0035
    /// PR5**, a `Remote` handle no longer hands out a disconnected default:
    /// it returns [`RemoteControlClient`]'s own `watch`, which
    /// [`crate::remote_metadata_watch_loop`] drives from the applied-index
    /// watermark carried on every `WatchMetadata`/`Status` reply — so
    /// `tablet_host_reconciler_loop`'s `select!` on this wakes on a real
    /// (network-relayed, long-poll-latency-bounded) metadata change instead
    /// of always falling through to its `RECONCILE_FALLBACK_INTERVAL` sleep
    /// arm. The fallback still fires normally whenever the watch loop itself
    /// is between polls or degraded to its plain-`Status` fallback — it was
    /// never *only* a `Remote`-specific safety net (see that constant's doc).
    pub(crate) fn metadata_watch(&self) -> MetadataWatch {
        match self {
            Self::Local(raft) => raft.metadata_watch(),
            Self::Remote(remote) => remote.metadata_watch(),
        }
    }

    /// Whether this handle's failure detector currently believes `member` is
    /// alive (`/admin/raft`'s per-member view). Always `false` for `Remote`
    /// — a data-only node runs no failure detector of its own (that lives on
    /// the control deployment).
    pub(crate) fn believes_alive(&self, member: NodeId) -> bool {
        match self {
            Self::Local(raft) => raft.believes_alive(member),
            Self::Remote(_) => false,
        }
    }

    /// Whether this handle's view of `Metadata` has ever synced at all — the
    /// readiness signal `tablet_host_reconciler_loop`'s pre-recovery guard
    /// needs in place of `last_applied() > 0`, which is pinned at `0` forever
    /// for `Remote` (see that method's doc). `Local` answers `false`
    /// unconditionally: its own `last_applied()` (or, for an ADR 0030 growth
    /// node, `ClientCtx.remote_metadata`) already tells this story, so this
    /// method contributes nothing new for it — the guard ORs all three
    /// signals together.
    pub(crate) fn has_synced_metadata(&self) -> bool {
        match self {
            Self::Local(_) => false,
            Self::Remote(remote) => remote.has_synced(),
        }
    }
}
