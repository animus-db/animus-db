//! Narrow **host-capability traits** (ADR 0061 rung C2, the second
//! 2026-08-28 amendment) that let a leaf background loop move into this
//! crate ahead of `ClientCtx` itself, which does not move until rung C5.
//!
//! Every one of `animusd`'s six background loops takes `ClientCtx` (four
//! also take `&RaftNode<ProdEnv>`), and `ClientCtx` is the 5,569-line brain
//! C5 moves last — so none of them can move as originally planned without
//! either dragging `ClientCtx` into this crate early or reimplementing its
//! logic here. Scoping each loop found it uses only a small, named slice of
//! `ClientCtx`'s surface: a control-plane leader handle, a read of
//! replicated `Metadata`, a handful of I/O primitives against a backup
//! object store, or (for the TTL reaper) a per-tablet scan/write pair. The
//! traits below name exactly that slice, one small trait per cohesive
//! capability rather than one fat trait — a loop's own generic bound is the
//! intersection of only the traits it actually needs, and no trait exists
//! that nothing but one loop uses only part of.
//!
//! `animusd`'s `ClientCtx` implements every trait here as a **thin,
//! logic-free delegation** to its own existing (unmoved) methods — see
//! `animusd/src/client_ctx_host.rs`. The loop's own control flow (the
//! sleep/tick/decide/propose shape, and — for the TTL reaper — the scan
//! cursor and per-item expiry decision) is what actually moves into this
//! crate; the deeper mechanics each capability method delegates to (Raft
//! apply, the ADR 0049 kind-write path, OCC, the segment-store wire format)
//! stay exactly where they are today, untouched, until C5 or later rungs
//! choose to move them on their own merits.

use std::io;
use std::time::Duration;

use animus_control::{Metadata, RaftNode};
use animus_dynamo::AttributeValue;
use animus_env::{Env, NodeId};
use animus_tablet::TabletId;
use async_trait::async_trait;

use crate::wire::{ClientRequest, ClientResponse};

/// This node's control-plane leader handle, if it currently believes it
/// leads — the one capability every loop in this rung needs, since each
/// self-gates its own tick on "am I the control-plane leader right now"
/// (`animusd`'s "run everywhere, self-gate" spawn pattern — see e.g.
/// `segment_janitor.rs`'s module doc). A control-only node and a combined
/// node answer this identically; a data-only node, which never runs a
/// local control `RaftNode` at all, always answers `None`.
///
/// Deliberately hands back a whole [`RaftNode<E>`] rather than exposing
/// `metadata()`/`propose()` as two separate trait methods: `RaftNode<E>` is
/// already `E`-generic (`animus-control`, no `ClientCtx` involved) and
/// already the type every loop's own tick function wants to hold locally
/// across an `await` — wrapping it a second time would add a layer with no
/// benefit.
pub trait ControlLeaderHost<E: Env> {
    /// The control-plane leader handle, or `None` if this node doesn't
    /// currently believe it leads.
    fn control_leader(&self) -> Option<RaftNode<E>>;
}

/// Durable object I/O for the **backup store** (ADR 0059 Train 1/3) — the
/// capability `backup_completion`/`backup_janitor`/`pitr_janitor` each use
/// a different slice of. Every method returns `None` on a **control-only**
/// leader, which provisions no backup-store handle at all (the documented
/// control-only-leader scope gap each of those modules' own doc restates
/// for its own phase) — a caller checks for `None` the same way the
/// pre-move code checked `ClientCtx::data_opt()`.
///
/// All four methods mirror `animusd`'s own `BackupStoreHandle` contract
/// exactly (see that type's doc): `put` returns the replica set the object
/// landed on (empty for a single-directory store); `list_local`/
/// `delete_local` are the local-only debug/sweep primitives
/// `SegmentStore::list`'s own contract licenses; `delete_at` targets a
/// specific, already-known replica set.
#[async_trait]
pub trait BackupObjectStore: Send + Sync {
    /// Durably store `bytes` at `id`, returning the replica set it landed
    /// on. `None` on a control-only leader.
    async fn backup_put(&self, id: &str, bytes: &[u8]) -> Option<io::Result<Vec<NodeId>>>;

    /// List this node's own locally-held object ids under `prefix`. `None`
    /// on a control-only leader.
    async fn backup_list_local(&self, prefix: &str) -> Option<io::Result<Vec<String>>>;

    /// Delete one of this node's own locally-held objects. `None` on a
    /// control-only leader.
    async fn backup_delete_local(&self, id: &str) -> Option<io::Result<()>>;

    /// Delete the object at `id` from every one of `replicas`. `None` on a
    /// control-only leader.
    async fn backup_delete_at(&self, replicas: &[NodeId], id: &str) -> Option<io::Result<()>>;
}

/// The TTL reaper's own narrow slice: a `Metadata` read, which tablets this
/// node leads, a pure local non-waking scan, and the one conditional-delete
/// write the reaper ever performs — see `ttl_reaper`'s module doc for the
/// full per-item contract each method must uphold (in particular: never
/// wake a quiesced group on a read, and every delete is conditional on the
/// exact TTL attribute value the scan observed).
#[async_trait]
pub trait TtlScanHost {
    /// This node's current view of replicated `Metadata` — mirrors
    /// `ClientCtx::effective_metadata()`'s own freshness contract (a
    /// possibly-cached read; see that method's doc for which callers must
    /// use the fresher alternative instead — the TTL reaper isn't one of
    /// them, since every decision here is idempotent and re-derived fresh
    /// next tick regardless).
    fn ttl_metadata(&self) -> Metadata;

    /// Every tablet id this node currently leads (any table, any state) —
    /// the reaper filters by table/TTL/state itself.
    fn led_tablets(&self) -> Vec<TabletId>;

    /// A pure **local, non-waking** scan of `tablet`'s own base-row kind,
    /// starting at `start` (inclusive), returning at most `limit` rows in
    /// key order. Must never wake a quiesced group (ADR 0048) — see
    /// `ttl_reaper`'s module doc for why that is load-bearing, not just an
    /// optimization.
    async fn scan_base_capped(
        &self,
        tablet: TabletId,
        start: &[u8],
        limit: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)>;

    /// Delete `table`'s item (`pk`/`sk`) on `tablet` through the ordinary
    /// kind-write path, conditional on `attribute` still holding exactly
    /// `expected` — a client's concurrent TTL refresh/removal must make
    /// this a no-op rather than raced. Tags the resulting change record as
    /// a TTL-reaper deletion (ADR 0051 §7). Wakes the group first (it is
    /// about to propose), mirroring the pre-move code's own
    /// wake-only-to-delete discipline.
    ///
    /// `Ok(true)` — deleted. `Ok(false)` — the condition failed (routine,
    /// not an error: the item was no longer known-expired by the time the
    /// delete reached the leader). `Err` — the write failed outright.
    #[allow(clippy::too_many_arguments)]
    async fn ttl_delete_if_attribute_equals(
        &self,
        tablet: TabletId,
        table: &str,
        pk: &AttributeValue,
        sk: Option<&AttributeValue>,
        attribute: &str,
        expected: AttributeValue,
    ) -> Result<bool, String>;
}

/// A synchronous call/await RPC to another node's client API (ADR 0061 rung
/// C3b, the third 2026-08-28 amendment) — the capability behind
/// `control_handle::RemoteControlClient::metadata_fresh`'s leader-directed
/// `Status` fetch, and the seam a future sim-only implementor (rung C3d,
/// deferred) will need to let a `SimEnv`-driven cluster relay at all.
///
/// **Why this is a capability trait and not `Network` (ADR 0026)**:
/// `Network` is fire-and-forget `send_stream`/single-consumer `recv_stream`
/// with no request/response correlation, while relay is synchronous call/
/// await — riding it on `Network` would need `req_id`-correlated pending-
/// reply machinery this rung deliberately does not build (deferred whole to
/// C3d). Worse, `Network`'s `ProdEnv` impl dials the **`internal`** port
/// (raw Raft/`KvWire` frames) while relay dials **`intra`**/`client`; riding
/// relay on `Network` would collapse ADR 0047's port split, a production
/// wire-topology change this testability rung disclaims. See ADR 0061's
/// third 2026-08-28 amendment for the full argument.
///
/// `animusd` implements this over its own **unchanged** `relay_request`/
/// `relay_request_with_timeout` — still a fresh `TcpStream` per call, still
/// dialing the `intra`/`client` ports, still framed via [`crate::codec`]'s
/// C3a functions. **The transport timeout stays entirely inside the
/// implementor**: this trait takes `timeout` as a plain value handed to
/// whatever the implementor uses to enforce it (`animusd`'s impl wraps the
/// call in `tokio::time::timeout`, which cannot live in this crate at all —
/// see this crate's own `CLAUDE.md`'s "no tokio" invariant). A default
/// method here calling `tokio::time::timeout` would violate that same
/// invariant just as surely as a call site would, so there is deliberately
/// no default method — every implementor supplies its own enforcement.
///
/// Returns [`ClientResponse`] directly, never a `Result`: this mirrors
/// `animusd`'s existing `relay_request`, which already folds every
/// transport failure (connect/write/read/decode/timeout) into
/// `ClientResponse::Error(..)` rather than a separate error channel — a
/// caller that already branches on `ClientResponse` (every existing relay
/// call site) needs no new failure shape to handle.
#[async_trait]
pub trait RelayClient {
    /// Relay `request` to `addr`'s client API and return its reply, or a
    /// `ClientResponse::Error` describing any transport failure
    /// (connect/write/read/decode/timeout) — never a panic, never a hang
    /// past `timeout`.
    async fn relay(
        &self,
        addr: String,
        request: &ClientRequest,
        timeout: Duration,
    ) -> ClientResponse;
}
