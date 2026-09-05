//! The `ControlHandle` seam (ADR 0035 PR1, extended by PR4 and PR5; moved
//! here and genericized over `E: Env`/`R: RelayClient` by ADR 0061 rung
//! C3c, the third 2026-08-28 amendment).
//!
//! Before this seam, every `animusd` node's `ClientCtx` reached the control
//! plane through a bare `RaftNode<ProdEnv>` — this process's own in-process
//! control Raft replica. ADR 0035 splits `animusd` into a small control-only
//! deployment and a data-only fleet with **no local control `RaftCore` at
//! all**, reaching the control deployment over the network instead. PR1
//! introduced the seam with only the `Local` variant wired up (a pure
//! refactor — every method delegated straight to the same `RaftNode<ProdEnv>`
//! calls `ClientCtx` already made directly, so no node that existed then —
//! every one of them `Local` — changed behavior).
//!
//! **PR4 adds `ControlHandle::Remote(RemoteControlClient)`** for a data-only
//! node: a polled mirror for
//! [`metadata_cached`](ControlHandle::metadata_cached) (kept fresh by
//! `animusd`'s `remote_metadata_sync_loop`, ADR 0035 §4) and a genuine
//! leader-directed network fetch for
//! [`metadata_fresh`](ControlHandle::metadata_fresh) (ADR 0035 §1) — see
//! [`RemoteControlClient`]'s own doc for both, plus the leader-hint lifecycle
//! that backs [`ControlHandle::leader_addr_hint`]. PR5 upgrades
//! `metadata_cached`'s fixed-interval poll to a long-poll watch and audits
//! every `Remote` degrade below against real staleness/liveness traffic.
//!
//! **Rung C3c genericizes both types.** `ControlHandle::Local` held a
//! concrete `RaftNode<ProdEnv>` — it now holds `RaftNode<E>`, mechanical
//! since every `Local` arm was already a synchronous passthrough to an
//! `E`-generic `RaftNode<E>` accessor. `RemoteControlClient`'s only
//! real-I/O method, [`metadata_fresh`](RemoteControlClient::metadata_fresh),
//! used to call `animusd`'s free `relay_request` (a raw `TcpStream` dial);
//! it now calls [`crate::host::RelayClient::relay`] on a generic `R`
//! instead, so this crate never names a socket type. Every other field and
//! method here was already plain data (`Vec<String>`, `Arc<Mutex<..>>`,
//! `MetadataWatch`, `MetricsHandle`) with nothing `ProdEnv`-typed — moving
//! them needed no change beyond adding the `R` parameter they now carry
//! alongside. `animusd` instantiates both as `ControlHandle<ProdEnv,
//! AnimusdRelayClient>` via a crate-local type alias — see that crate's
//! `control_handle.rs`, now just the alias plus the `RelayClient` impl over
//! its own unchanged `relay_request`/`relay_request_with_timeout`.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::mirror::{self, KeyWrite};
use animus_control::{Metadata, MetadataWatch, RaftNode, Role};
use animus_env::{Env, MetricsHandle, NodeId};

use crate::host::RelayClient;
use crate::wire::{ClientRequest, ClientResponse};

/// This node's access to the control plane's replicated [`Metadata`] and
/// this-node's-own-Raft-state introspection (`/admin/raft`, `/metrics`).
///
/// **Not** a proposal path: proposing a `MetaCommand` always goes through
/// whichever locally-registered handle currently believes it is leader
/// (`animusd`'s `ClusterEdgeState::leader_handle()`, deliberately kept as a
/// concrete `RaftNode<ProdEnv>` — see that type's doc), never through
/// `self.control` directly, because "propose" is inherently a *local-Raft-
/// log* operation that only a genuine control-group voter can perform at
/// all; the `Remote` variant (no local `RaftCore`) has no such operation to
/// expose here — it would have to relay over the network instead, which is
/// exactly what the leader-handle relay already does. `ControlHandle::
/// propose`/`flush` were dropped from this seam for exactly that reason (no
/// call site needs them, and `Remote` would not implement them the same way
/// its `Local` sibling does).
///
/// `Local` wraps a node's own in-process control [`RaftNode`] — today's
/// only variant `animusd` ever constructs; every existing `animusd` node
/// (combined mode, or an ADR 0030 growth node) holds one. `Clone` because
/// `RaftNode` is `Arc`-backed internally (cheap to clone, shares state);
/// `ControlHandle` inherits that.
///
/// Reads are split by freshness contract, mirroring the pre-existing
/// `RaftNode::metadata()` / `animusd`'s `ClientCtx::effective_metadata()`
/// split:
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
///   This method exists so `Remote` can implement it as a leader-directed
///   fetch (proxying a `Status` request to the control deployment,
///   mirroring `animusd`'s `propose_schema` relay shape) instead.
#[derive(Clone)]
pub enum ControlHandle<E: Env, R: RelayClient> {
    Local(RaftNode<E>),
    /// A data-only node's control-plane access (ADR 0035 PR4): no local
    /// `RaftCore` at all. See [`RemoteControlClient`]'s own doc.
    Remote(RemoteControlClient<R>),
}

/// A data-only node's (ADR 0035 PR4) access to the **separately-deployed**
/// control plane: no local `RaftCore`, so every read is either a poll of a
/// mirror (`metadata_cached`, refreshed by `animusd`'s
/// `remote_metadata_sync_loop`) or a direct live fetch (`metadata_fresh`).
/// Cheap to clone (every field is `Arc`-backed, or — for `relay`/`timeout` —
/// a value the `RelayClient` implementor itself must make cheap to clone;
/// `animusd`'s own implementor is zero-sized), mirroring `RaftNode`'s own
/// shape.
///
/// **Also reused, standalone, by an ADR 0030 growth node (ADR 0035 PR5).** A
/// growth node's `ClientCtx.control` stays `ControlHandle::Local` (it is a
/// real, permanently non-voting control-group member, not a data-only node),
/// so it never holds one of these inside a `ControlHandle::Remote` — but
/// `animusd`'s `remote_metadata_sync_loop`'s growth-node branch constructs
/// one directly via [`with_mirror`](Self::with_mirror), sharing
/// `ClientCtx.remote_metadata` as its `mirror`, purely to drive `animusd`'s
/// `remote_metadata_watch_loop` (the identical long-poll logic) — see that
/// constructor's doc.
///
/// **Leader-hint lifecycle (ADR 0035 §1).** Every `ClientRequest::Status`
/// reply now carries the answering node's own `self.control.leader()` +
/// `route_addr(leader_id)` as `leader_hint: Option<(NodeId, String)>`
/// (`#[serde(default)]`, so an old reply without the field still parses as
/// `None`). This handle keeps the most recent one it has seen — updated by
/// the periodic sync loop *and* by [`metadata_fresh`](Self::metadata_fresh)'s
/// own live fetch — and hands it out via [`leader`](Self::leader) (the id)
/// and [`leader_addr_hint`](Self::leader_addr_hint) (the client-API address,
/// which `animusd`'s `ClientCtx::propose_schema` prefers over a `route_addr`
/// lookup — see that method's doc). The hint is *never* independently
/// verified against the answering node's own role; see `metadata_fresh`'s
/// doc for the consequence — audited in PR5 and found self-healing in
/// practice: every control node's own `leader()` is kept current by real
/// Raft heartbeats/AppendEntries (not the ADR 0035 mirror-poll-interval
/// class of staleness), and the periodic full-seed-list sync (not just the
/// hint) refreshes it from whichever control node answers, so a stale hint
/// corrects itself within one sync cycle even if the hinted address itself
/// has gone unreachable.
///
/// **Applied-index watch (ADR 0035 PR5).** `watch` is this handle's own
/// same-process [`MetadataWatch`] — disconnected from any `RaftCore` (this
/// node has none), but driven directly by [`observe`](Self::observe) from
/// the watermark every `Status`/`WatchMetadata` reply now carries. This is
/// what lets [`ControlHandle::metadata_watch`] hand the tablet-host
/// reconciler a *real* wake-on-change signal for a data-only node instead of
/// a permanently-disconnected default — see `animusd`'s
/// `remote_metadata_watch_loop`'s doc for how it's kept current.
#[derive(Clone)]
pub struct RemoteControlClient<R: RelayClient> {
    /// The control deployment's own **intra-cluster** addresses (ADR 0047 —
    /// changed from the pre-ADR-0047 client-API addresses, since
    /// `WatchMetadata` is intra-only): the discovery root for `animusd`'s
    /// `remote_metadata_watch_loop` and `metadata_fresh`'s fallback scan.
    /// Static for this node's lifetime (the control group itself is static,
    /// ADR 0030); every actual hop still prefers a leader hint first
    /// (`intra_leader_hint` for the watch loop; `leader_hint` here in
    /// `metadata_fresh`, unchanged — `Status` is served on both surfaces, so
    /// mixing hint flavors in one candidate list is harmless, just not worth
    /// the extra plumbing to make uniform).
    seeds: Vec<String>,
    /// The last `Metadata` observed from any control node's `Status` reply.
    /// `None` until the very first sync — the readiness signal
    /// [`has_synced`](Self::has_synced) exposes for the tablet-host
    /// reconciler's pre-recovery guard, which otherwise has no local
    /// `last_applied()` to gate on (this handle's is pinned at 0 forever).
    mirror: Arc<Mutex<Option<Metadata>>>,
    /// The last-known control-plane leader `(id, client address)` — see the
    /// type doc's "leader-hint lifecycle" section.
    leader_hint: Arc<Mutex<Option<(NodeId, String)>>>,
    /// The **intra-cluster** dual of `leader_hint` (ADR 0047) — the same
    /// lifecycle, populated from the identical `Status`/`WatchMetadata`
    /// reply's `intra_leader_hint` field, but resolved through the
    /// answering node's `intra_addr(leader_id)` instead of `route_addr`.
    /// **Not** a repoint of `leader_hint`: the two serve different
    /// audiences (this one is machine-relay-only — `propose_schema`,
    /// `remote_metadata_watch_loop` — never a human-facing message; see the
    /// root `CLAUDE.md`'s hint-field-conflation lesson for why they must
    /// stay separate fields).
    intra_leader_hint: Arc<Mutex<Option<(NodeId, String)>>>,
    /// This handle's own applied-index watch (ADR 0035 PR5) — see the type
    /// doc's "applied-index watch" section. Bumped only by
    /// [`observe`](Self::observe); handed out (cloned) via
    /// [`metadata_watch`](Self::metadata_watch).
    watch: MetadataWatch,
    /// The last **live control-voter set** observed from any control node's
    /// `Status`/`WatchMetadata` reply (ADR 0037 PR2) — `None` until the
    /// first one lands, so a caller can tell "never fetched" apart from "the
    /// control group genuinely has zero voters" (which can't happen in
    /// practice, but this handle doesn't get to assume that on a caller's
    /// behalf). This is deliberately **not** the same thing as `mirror`'s
    /// `Metadata.node_addrs` (the replicated address *bookkeeping*, which can
    /// list a node with `role: "control"` whether or not it is *currently* a
    /// live Raft voter) — this is the actual `RaftCore::config()` a genuine
    /// control-group replica would read locally, echoed over the wire so a
    /// data-only node (or a future admin/CLI caller riding this same handle)
    /// can learn it without one. Updated by [`observe`](Self::observe) under
    /// the identical freshness gate as the metadata mirror.
    control_voters: Arc<Mutex<Option<BTreeSet<NodeId>>>>,
    /// No-op metrics sink: a data-only node's control-plane access has no
    /// local Raft loops to instrument (unlike `Local`, whose `RaftNode`
    /// records into its own env's real sink).
    metrics: MetricsHandle,
    /// The [`RelayClient`] implementor this handle relays `Status` fetches
    /// through (rung C3b/C3c) — `animusd`'s own is a thin wrapper over its
    /// unchanged `relay_request_with_timeout`, still a raw `TcpStream` dial
    /// on the `intra`/`client` ports. Generic here, never a concrete socket
    /// type: this crate depends on no `tokio`.
    relay: R,
    /// The transport timeout `metadata_fresh` hands to every
    /// [`RelayClient::relay`] call — supplied by the caller at construction
    /// (`animusd` passes its own `CLIENT_TIMEOUT`) rather than duplicated as
    /// a constant here, since only the host crate knows the value it wants.
    timeout: Duration,
}

impl<R: RelayClient> RemoteControlClient<R> {
    pub fn new(seeds: Vec<String>, relay: R, timeout: Duration) -> Self {
        Self::with_mirror(seeds, Arc::new(Mutex::new(None)), relay, timeout)
    }

    /// Like [`new`](Self::new), but shares an existing `mirror` `Arc` instead
    /// of starting a fresh, empty one.
    ///
    /// Used by `animusd`'s `remote_metadata_sync_loop`'s ADR 0030
    /// growth-node branch (ADR 0035 PR5's long-poll port of it): a growth
    /// node's `ClientCtx.control` stays `ControlHandle::Local` (it *is* a
    /// real, if permanently non-voting, control-group member — unlike a
    /// genuine ADR 0035 PR4 data-only node), so there is no
    /// `ControlHandle::Remote` to hold a full `RemoteControlClient`. But the
    /// long-poll watch loop (`animusd`'s `remote_metadata_watch_loop`) is
    /// otherwise identical for both cases, so the growth-node branch
    /// constructs a standalone `RemoteControlClient` here — never installed
    /// into `ControlHandle` — sharing `ClientCtx.remote_metadata` directly
    /// as its `mirror` so every existing reader of that field
    /// (`effective_metadata()`) keeps working unchanged, with no separate
    /// copy of the mirror to keep in sync.
    pub fn with_mirror(
        seeds: Vec<String>,
        mirror: Arc<Mutex<Option<Metadata>>>,
        relay: R,
        timeout: Duration,
    ) -> Self {
        Self {
            seeds,
            mirror,
            leader_hint: Arc::new(Mutex::new(None)),
            intra_leader_hint: Arc::new(Mutex::new(None)),
            watch: MetadataWatch::default(),
            control_voters: Arc::new(Mutex::new(None)),
            metrics: MetricsHandle::noop(),
            relay,
            timeout,
        }
    }

    /// The mirror's last synced value, or a default-empty `Metadata` if it
    /// has never synced (see [`has_synced`](Self::has_synced) for telling the
    /// two apart).
    pub fn metadata_cached(&self) -> Metadata {
        self.mirror
            .lock()
            .expect("remote control mirror poisoned")
            .clone()
            .unwrap_or_default()
    }

    /// Whether this handle's mirror has synced at least once.
    pub fn has_synced(&self) -> bool {
        self.mirror
            .lock()
            .expect("remote control mirror poisoned")
            .is_some()
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.leader_hint
            .lock()
            .expect("leader hint poisoned")
            .as_ref()
            .map(|(id, _)| id.clone())
    }

    pub fn leader_addr_hint(&self) -> Option<String> {
        self.leader_hint
            .lock()
            .expect("leader hint poisoned")
            .as_ref()
            .map(|(_, addr)| addr.clone())
    }

    /// The intra-cluster dual of [`leader_addr_hint`](Self::leader_addr_hint)
    /// (ADR 0047) — see `intra_leader_hint`'s own field doc for why this is a
    /// parallel field, not a repoint.
    pub fn intra_leader_addr_hint(&self) -> Option<String> {
        self.intra_leader_hint
            .lock()
            .expect("intra leader hint poisoned")
            .as_ref()
            .map(|(_, addr)| addr.clone())
    }

    /// This handle's own applied-index watch (ADR 0035 PR5) — see the type
    /// doc's "applied-index watch" section.
    pub fn metadata_watch(&self) -> MetadataWatch {
        self.watch.clone()
    }

    /// This handle's own [`RelayClient`] implementor (ADR 0064, S-01
    /// commit 2) — lets a caller outside this type's own `metadata_fresh`
    /// reach the identical relay path (and its TLS material, for whichever
    /// concrete `R` a host crate supplies) instead of re-dialing by hand.
    /// `animusd`'s `remote_metadata_watch_loop` is the motivating caller:
    /// it drives its own `WatchMetadata`/`Status` round trips outside
    /// `metadata_fresh`, but must still speak whatever transport (plain or
    /// mutual TLS) this handle's own `AnimusdRelayClient` was built with.
    pub fn relay(&self) -> &R {
        &self.relay
    }

    /// The last live control-voter set observed on the wire (ADR 0037 PR2) —
    /// see the `control_voters` field's doc for what this is (and is not)
    /// the same thing as. `None` until [`observe`](Self::observe) has landed
    /// at least one reply.
    pub fn control_voters(&self) -> Option<BTreeSet<NodeId>> {
        self.control_voters
            .lock()
            .expect("remote control-voters poisoned")
            .clone()
    }

    /// Record a `Status`/`WatchMetadata` reply's metadata + leader hint +
    /// applied-index watermark + live control-voter set. Called by
    /// `animusd`'s `remote_metadata_watch_loop` and by
    /// [`metadata_fresh`](Self::metadata_fresh)'s own live fetch — both
    /// observe the identical wire shape, so both refresh the same state.
    ///
    /// **Non-regression guard (ADR 0035 PR5):** any control node may answer —
    /// not necessarily the most caught-up one — so a reply from a replica
    /// lagging behind one this handle already observed must not overwrite a
    /// fresher mirror with a staler snapshot. `watermark` is the answering
    /// node's own applied index at reply time, a monotonic proxy for how
    /// fresh `metadata` is; the mirror + watch (and, ADR 0037 PR2, the
    /// observed voter set) only advance together (`watermark >= watch.
    /// latest()`, so a same-watermark reply still refreshes them — e.g. an
    /// unchanged snapshot re-observed after a timed-out long-poll retry).
    /// The leader hint is taken unconditionally regardless — it self-heals
    /// independently (see the type doc) and isn't a snapshot whose *content*
    /// can regress the same way.
    ///
    /// The whole check-then-mutate-then-bump sequence runs under `mirror`'s
    /// own lock (ADR 0038 PR5 tightening — previously the `watermark >=
    /// watch.latest()` check and the mutation that followed it were two
    /// separate steps, racy against a concurrent [`observe_delta`] call
    /// applying a delta computed against a *different* `last_seen` basis; see
    /// that method's doc for why a delta apply needs this to be atomic in a
    /// way a full replace never did).
    pub fn observe(
        &self,
        metadata: Metadata,
        leader_hint: Option<(NodeId, String)>,
        intra_leader_hint: Option<(NodeId, String)>,
        watermark: u64,
        control_voters: BTreeSet<NodeId>,
    ) {
        if let Some(hint) = leader_hint {
            *self.leader_hint.lock().expect("leader hint poisoned") = Some(hint);
        }
        if let Some(hint) = intra_leader_hint {
            *self
                .intra_leader_hint
                .lock()
                .expect("intra leader hint poisoned") = Some(hint);
        }
        let mut cached = self.mirror.lock().expect("remote control mirror poisoned");
        if watermark >= self.watch.latest() {
            *cached = Some(metadata);
            *self
                .control_voters
                .lock()
                .expect("remote control-voters poisoned") = Some(control_voters);
            self.watch.bump(watermark);
        }
    }

    /// The incremental counterpart to [`observe`](Self::observe) (ADR 0038
    /// PR5): install a `WatchMetadata` [`ClientResponse::MetadataDelta`]
    /// reply's [`KeyWrite`]s onto this handle's own cached `Metadata` via
    /// [`mirror::apply_key_write`], never replaying a `MetaCommand` itself —
    /// this crate carries no control-plane business logic to do that
    /// correctly, by design (see that request variant's doc).
    ///
    /// `last_seen` is the exact watermark this delta was requested relative
    /// to (i.e. what [`metadata_watch`](Self::metadata_watch)`.latest()` was
    /// *when the request was sent*), not merely "some watermark `<=
    /// watermark`": applying a delta is a **sequential** operation — unlike
    /// [`observe`](Self::observe)'s full replace, which is order-independent
    /// modulo the monotonic watermark guard, installing `(last_seen,
    /// watermark]`'s writes is only correct if the cached `Metadata` this
    /// handle currently holds is *exactly* the state as of `last_seen`. A
    /// concurrent [`observe`](Self::observe)/`observe_delta` call (this
    /// handle is shared — `metadata_fresh()` and the background watch loop
    /// both drive it) can move the mirror past `last_seen` before this reply
    /// lands; detected here as `self.watch.latest() != last_seen` and
    /// **dropped** rather than mis-applied — a false negative here just
    /// means the next long-poll iteration re-requests with the
    /// now-corrected `last_seen` (self-healing, no correctness impact, at
    /// most one extra round trip). Returns whether the delta was applied
    /// (test/observability only — the caller has nothing else to do either
    /// way, since the watch loop's next iteration reads the current
    /// `last_seen` fresh regardless of outcome).
    pub fn observe_delta(
        &self,
        last_seen: u64,
        writes: &[KeyWrite],
        leader_hint: Option<(NodeId, String)>,
        intra_leader_hint: Option<(NodeId, String)>,
        watermark: u64,
        control_voters: BTreeSet<NodeId>,
    ) -> bool {
        if let Some(hint) = leader_hint {
            *self.leader_hint.lock().expect("leader hint poisoned") = Some(hint);
        }
        if let Some(hint) = intra_leader_hint {
            *self
                .intra_leader_hint
                .lock()
                .expect("intra leader hint poisoned") = Some(hint);
        }
        let mut cached = self.mirror.lock().expect("remote control mirror poisoned");
        if self.watch.latest() != last_seen {
            // Stale relative to a concurrent update — see the doc above.
            return false;
        }
        let mut meta = cached.clone().unwrap_or_default();
        for write in writes {
            mirror::apply_key_write(&mut meta, write);
        }
        *cached = Some(meta);
        *self
            .control_voters
            .lock()
            .expect("remote control-voters poisoned") = Some(control_voters);
        self.watch.bump(watermark);
        true
    }

    /// A live, leader-directed `Status` fetch (ADR 0035 §1): try the current
    /// leader hint first, falling back to a scan of every seed — mirroring
    /// `animusd`'s `ClientCtx::propose_schema`'s own hint-first-then-scan
    /// relay shape (this handle has no `ClientCtx` of its own to reach
    /// `propose_schema` through, so it repeats the same policy directly over
    /// its own [`RelayClient`]).
    /// Whichever reply lands also refreshes the mirror + hint + watch,
    /// exactly like the periodic sync loop.
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
    pub async fn metadata_fresh(&self) -> Metadata {
        let mut candidates = Vec::with_capacity(self.seeds.len() + 1);
        if let Some(addr) = self.leader_addr_hint() {
            candidates.push(addr);
        }
        candidates.extend(self.seeds.iter().cloned());
        for addr in candidates {
            if let ClientResponse::Status {
                metadata,
                leader_hint,
                intra_leader_hint,
                watermark,
                control_voters,
            } = self
                .relay
                .relay(addr, &ClientRequest::Status, self.timeout)
                .await
            {
                self.observe(
                    metadata.clone(),
                    leader_hint,
                    intra_leader_hint,
                    watermark,
                    control_voters,
                );
                return metadata;
            }
        }
        self.metadata_cached()
    }
}

impl<E: Env, R: RelayClient> ControlHandle<E, R> {
    /// Staleness-tolerant read of the control plane's replicated `Metadata` —
    /// see the type doc for the freshness contract. `ClientCtx::
    /// effective_metadata()` is almost always the right thing to call
    /// instead of this directly (it layers the growth-node mirror on top);
    /// this is the primitive that both it and `metadata_fresh` (today) build
    /// on.
    pub fn metadata_cached(&self) -> Metadata {
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
    pub async fn metadata_fresh(&self) -> Metadata {
        match self {
            Self::Local(raft) => raft.metadata(),
            Self::Remote(remote) => remote.metadata_fresh().await,
        }
    }

    /// Whether this handle currently believes it is the control-plane leader.
    /// Always `false` for `Remote` — a data-only node never holds any
    /// control-group Raft role at all, so "am I the leader" is meaningless
    /// for it (`animusd`'s `ClientCtx::propose_schema`/`admin_drain`/
    /// `admin_remove_member` already treat "not the leader" as "relay or ask
    /// the operator to retry on the leader", which is exactly the right
    /// degrade here).
    pub fn is_leader(&self) -> bool {
        match self {
            Self::Local(raft) => raft.is_leader(),
            Self::Remote(_) => false,
        }
    }

    /// The current leader's id, if this handle knows one. For `Remote`, the
    /// id half of the [leader hint](RemoteControlClient) — see that type's
    /// doc.
    ///
    /// **This is the raw consensus belief** (`RaftCore::leader_id` for
    /// `Local`) — an operational (health/readiness) reader should call
    /// [`leader_within`](Self::leader_within) instead (issue #595): see
    /// `RaftCore::leader`'s own doc for why this one has a false-negative
    /// window on a transient one-sided delay.
    pub fn leader(&self) -> Option<NodeId> {
        match self {
            Self::Local(raft) => raft.leader(),
            Self::Remote(remote) => remote.leader(),
        }
    }

    /// Hysteresis-bearing leader read for an operational reader (issue
    /// #595) — see `RaftCore::leader_within`'s own doc for the full
    /// contract and rationale.
    ///
    /// `Local` delegates straight to `RaftNode::leader_within`, which has a
    /// real `last_leader_contact` timestamp to consult. **`Remote` has no
    /// local `RaftCore` and the wire carries no contact timestamp today**
    /// (`Status`/`WatchMetadata` echo `leader_hint`, not a contact age) —
    /// so for now it simply falls back to [`leader`](Self::leader), meaning
    /// the hysteresis this method exists to provide is **`Local`-only**.
    /// This is an honest degrade, not a bug: a data-only node's own
    /// `/admin/health` was never gated on a local Raft belief to begin with
    /// (`ControlHandle::is_leader` is already always `false` for `Remote`),
    /// and widening the wire to carry a contact age is a separate, later
    /// change if a data-only node's health probe is ever found to need the
    /// same false-negative-window protection.
    pub fn leader_within(&self, max_age: Duration) -> Option<NodeId> {
        match self {
            Self::Local(raft) => raft.leader_within(max_age),
            Self::Remote(remote) => remote.leader(),
        }
    }

    /// This handle's election-timeout base, for sizing a `leader_within`
    /// grace window (e.g. `animusd::admin::health`'s `HEALTH_LEADER_GRACE`).
    /// `Remote` has no local `RaftCore` at all, so it answers the same
    /// compile-time default every `RaftCore` is constructed with
    /// (`Duration::from_millis(150)`, `RaftCore::new`) — the control
    /// deployment's own real value, which is never independently
    /// configurable per ADR 0009's "no `set_election_timeout`" note.
    pub fn election_timeout(&self) -> Duration {
        match self {
            Self::Local(raft) => raft.election_timeout(),
            Self::Remote(_) => Duration::from_millis(150),
        }
    }

    /// The current leader's **client-API** address, if directly known (ADR
    /// 0035 PR4) — always `None` for `Local` (a genuine control-group voter
    /// has no separate notion of "the leader's client address"; callers that
    /// need one resolve it via `ClientCtx::route_addr` on `leader()`'s id).
    /// For `Remote`, this is the [leader hint](RemoteControlClient) a
    /// `Status` reply carries, which `animusd`'s `ClientCtx::propose_schema`
    /// prefers over a `route_addr` lookup — the hint is strictly fresher for
    /// a data-only node, since it comes straight off the same `Status` reply
    /// that filled the mirror, whereas `route_addr` needs the leader's
    /// address to have separately synced into the replicated node-address
    /// book.
    pub fn leader_addr_hint(&self) -> Option<String> {
        match self {
            Self::Local(_) => None,
            Self::Remote(remote) => remote.leader_addr_hint(),
        }
    }

    /// The intra-cluster dual of [`leader_addr_hint`](Self::leader_addr_hint)
    /// (ADR 0047) — machine-relay-only (`propose_schema`,
    /// `remote_metadata_watch_loop`'s dial candidates), never surfaced to a
    /// human. Always `None` for `Local`, same reasoning as
    /// `leader_addr_hint`.
    pub fn intra_leader_addr_hint(&self) -> Option<String> {
        match self {
            Self::Local(_) => None,
            Self::Remote(remote) => remote.intra_leader_addr_hint(),
        }
    }

    /// This handle's current Raft role (introspection, `/admin/raft`).
    /// `Remote` reports `Follower` — the closest honest approximation (it
    /// holds no vote, campaigns for nothing, and is certainly not `Leader`).
    pub fn role(&self) -> Role {
        match self {
            Self::Local(raft) => raft.role(),
            Self::Remote(_) => Role::Follower,
        }
    }

    /// This handle's current Raft term (introspection, `/admin/raft`). Always
    /// `0` for `Remote` — it has no local Raft term at all.
    pub fn term(&self) -> u64 {
        match self {
            Self::Local(raft) => raft.term(),
            Self::Remote(_) => 0,
        }
    }

    /// This handle's commit index (introspection, `/admin/raft`). Always `0`
    /// for `Remote` — see [`term`](Self::term)'s doc.
    pub fn commit_index(&self) -> u64 {
        match self {
            Self::Local(raft) => raft.commit_index(),
            Self::Remote(_) => 0,
        }
    }

    /// This handle's last-applied index — also the pre-recovery-replay guard
    /// the tablet-host reconciler trigger reads directly (see `animusd`'s
    /// `tablet_host_reconciler_loop`'s doc). **Pinned at `0` forever for
    /// `Remote`** (it has no local Raft log to apply at all) — the
    /// reconciler's guard also ORs in [`has_synced_metadata`]
    /// (Self::has_synced_metadata) so a data-only node isn't permanently
    /// gated off by a signal that can never become true for it.
    pub fn last_applied(&self) -> u64 {
        match self {
            Self::Local(raft) => raft.last_applied(),
            Self::Remote(_) => 0,
        }
    }

    /// This handle's durable (fsynced) index (introspection, `/admin/raft`).
    /// Always `0` for `Remote` — see [`term`](Self::term)'s doc.
    pub fn durable_index(&self) -> u64 {
        match self {
            Self::Local(raft) => raft.durable_index(),
            Self::Remote(_) => 0,
        }
    }

    /// This handle's snapshot index (introspection, `/admin/raft`). Always
    /// `0` for `Remote` — see [`term`](Self::term)'s doc.
    pub fn snapshot_index(&self) -> u64 {
        match self {
            Self::Local(raft) => raft.snapshot_index(),
            Self::Remote(_) => 0,
        }
    }

    /// This handle's in-memory log length (introspection, `/admin/raft`).
    /// Always `0` for `Remote` — see [`term`](Self::term)'s doc.
    pub fn log_len(&self) -> usize {
        match self {
            Self::Local(raft) => raft.log_len(),
            Self::Remote(_) => 0,
        }
    }

    /// The highest Raft log index this node's `Metadata` **async apply
    /// task** has durably merged into the published `cache` (introspection,
    /// `/admin/raft`; ADR 0038 PR3) — distinct from, and can meaningfully lag
    /// behind, [`last_applied`](Self::last_applied): the consensus loop
    /// advances `last_applied` off the sync core alone (deliberately no
    /// engine I/O, so a slow/contended engine merge never risks tripping an
    /// election), while this value only advances once that engine write for
    /// a batch has actually landed — the same decoupling that lets
    /// `/admin/status` (which reads `Metadata` off this apply task's
    /// `cache`, not the core) lag `/admin/raft`'s own
    /// `commit_index`/`last_applied` by an amount bounded only by how
    /// starved the apply task is, not by Raft. Always `0` for `Remote` — it
    /// has no local apply task at all (see [`term`](Self::term)'s doc for
    /// the same "no local Raft" reasoning).
    pub fn engine_applied_index(&self) -> u64 {
        match self {
            Self::Local(raft) => raft.engine_applied_index(),
            Self::Remote(_) => 0,
        }
    }

    /// This handle's view of the **live** control-voter configuration —
    /// introspection (`/admin/raft`) and the wire-discovery field
    /// `ClientResponse::Status::control_voters` (see [`crate::ClientResponse`])
    /// both read this (ADR 0037 PR2).
    ///
    /// `Local` always knows this outright: it's a genuine control-group
    /// replica reading its own `RaftCore::config()` — `Some(..)`,
    /// unconditionally. `Remote` has no local `RaftCore` to read at all, so
    /// this instead reports the last voter set observed on any
    /// `Status`/`WatchMetadata` reply from the control deployment (see
    /// [`RemoteControlClient::control_voters`]) — `None` until the first one
    /// lands. The `Option` is deliberate: "unknown, never fetched" (`None`)
    /// and "genuinely zero voters" (`Some(empty set)`, which can't happen in
    /// practice, but this method doesn't get to assume that on a caller's
    /// behalf — see the plan's "honesty" requirement) must stay
    /// distinguishable, unlike the pre-ADR-0037 behavior this replaces (a
    /// bare, always-empty `BTreeSet` for `Remote`, which silently conflated
    /// the two).
    pub fn config(&self) -> Option<BTreeSet<NodeId>> {
        match self {
            Self::Local(raft) => Some(raft.config()),
            Self::Remote(remote) => remote.control_voters(),
        }
    }

    /// The voter this handle's own leader is currently handing leadership
    /// off to, if a transfer is armed right now (`/admin/raft`, issue #313
    /// — previously invisible short of reading the driver's own abort log).
    /// `Local` reads its live `RaftNode::transfer_target()` directly —
    /// `None` covers both "not this node's own transfer" (irrelevant if
    /// this node isn't leader) and "no transfer armed," which is fine here
    /// unlike [`config`](Self::config)'s `Option`: a transfer is
    /// leader-local volatile state, not something "never observed yet" is
    /// even a meaningful distinction for. `Remote` has no local `RaftCore`
    /// to read at all and always answers `None` — the wire carries no
    /// transfer-in-progress signal (unlike `control_voters`), so this is
    /// honestly "unknown," not "none armed."
    pub fn transfer_target(&self) -> Option<NodeId> {
        match self {
            Self::Local(raft) => raft.transfer_target(),
            Self::Remote(_) => None,
        }
    }

    /// This handle's recording metrics sink (aggregated into the `/metrics`
    /// export alongside the raftkv-role sink). A `Remote` handle's sink is a
    /// permanent no-op (see [`RemoteControlClient`]'s doc) — the raftkv-role
    /// sink still records real counters on a data-only node.
    pub fn metrics(&self) -> &MetricsHandle {
        match self {
            Self::Local(raft) => raft.metrics(),
            Self::Remote(remote) => &remote.metrics,
        }
    }

    /// An executor-agnostic "applied index advanced" notification — see
    /// `MetadataWatch`'s own doc (ADR 0031 §trigger). Same-process only (the
    /// primitive itself never crosses a network hop) — but as of **ADR 0035
    /// PR5**, a `Remote` handle no longer hands out a disconnected default:
    /// it returns [`RemoteControlClient`]'s own `watch`, which `animusd`'s
    /// `remote_metadata_watch_loop` drives from the applied-index watermark
    /// carried on every `WatchMetadata`/`Status` reply — so
    /// `tablet_host_reconciler_loop`'s `select!` on this wakes on a real
    /// (network-relayed, long-poll-latency-bounded) metadata change instead
    /// of always falling through to its `RECONCILE_FALLBACK_INTERVAL` sleep
    /// arm. The fallback still fires normally whenever the watch loop itself
    /// is between polls or degraded to its plain-`Status` fallback — it was
    /// never *only* a `Remote`-specific safety net (see that constant's doc).
    pub fn metadata_watch(&self) -> MetadataWatch {
        match self {
            Self::Local(raft) => raft.metadata_watch(),
            Self::Remote(remote) => remote.metadata_watch(),
        }
    }

    /// Whether this handle's failure detector currently believes `member` is
    /// alive (`/admin/raft`'s per-member view). Always `false` for `Remote`
    /// — a data-only node runs no failure detector of its own (that lives on
    /// the control deployment).
    pub fn believes_alive(&self, member: NodeId) -> bool {
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
    pub fn has_synced_metadata(&self) -> bool {
        match self {
            Self::Local(_) => false,
            Self::Remote(remote) => remote.has_synced(),
        }
    }
}
