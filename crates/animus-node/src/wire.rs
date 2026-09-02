//! The client-facing wire types (ADR 0061 rung C1): [`ClientRequest`] /
//! [`ClientResponse`], the [`Surface`] classification of where a request may
//! be received bare, [`is_relayable_command`] (whether a `MetaCommand` may
//! ride the `ProposeSchema` relay envelope), plus the plain-data types they
//! embed. Ordinary serde data — no `ProdEnv`, no `CpGroup`/`RaftNode`/
//! `RaftKvNode`/`LsmEngine` handle, no tokio type — moved here verbatim from
//! `animusd::lib` (see that crate's `CLAUDE.md` and this crate's own for the
//! carve-out this is part of).
//!
//! `animusd::lib` re-exports everything in this module at its crate root so
//! the ~500 existing call sites across that crate keep compiling unchanged.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;

use animus_control::{MetaCommand, Metadata, NodeStatus};
use animus_cp_data::hlc::HlcTimestamp;
use animus_cp_data::{
    ResolveOutcome, StageOutcome, TxnDecisionStatus, TxnId, TxnOutcome, TxnRecordView,
};
use animus_env::NodeId;
use animus_tablet::KeyRange;

/// A `cp_txn` precondition (ADR 0018 §2/PR4): `(table, key, expected)` —
/// `expected: None` means "must be absent".
pub type TxnPrecondition = (String, Vec<u8>, Option<Vec<u8>>);

/// One [`ClientRequest::KindWriteItem`] operation — a DynamoDB `PutItem`/
/// `DeleteItem`/`UpdateItem`'s payload, self-contained enough for the
/// tablet leader to evaluate a `condition` and compute the item's new value
/// **itself**, rather than trusting a pre-computed value from the
/// (possibly stale, possibly racing) edge that received the request — see
/// [`ClientRequest::KindWriteItem`]'s doc for why (ADR 0046 "evaluate at
/// leader", U3).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum KindWriteOp {
    /// `PutItem`'s full new item.
    Put(animus_dynamo::Item),
    /// `DeleteItem` — the leader writes the tombstone sentinel itself.
    Delete,
    /// `UpdateItem`'s raw `SET`/`REMOVE` actions — the leader applies them
    /// to the old image it itself reads (so the leader resolves the
    /// base-value read-modify-write hazard too, not just the LSI/
    /// change-record one `PutItem`/`DeleteItem` already close). `key_item`
    /// covers the upsert-from-key-attributes-when-absent behavior
    /// ([`animus_dynamo::wire::apply_update`]'s existing contract) when the
    /// item doesn't exist yet.
    Update {
        key_item: animus_dynamo::Item,
        actions: Vec<animus_dynamo::wire::UpdateAction>,
    },
}

/// A `cp_txn`/`ClientRequest::Txn` write spanning tables (ADR 0018 §2/PR4;
/// ADR 0046 U3 kind-writes extension): `(table, key)` plus **either**
/// `value: Some(..)` (a plain, already-known write — a staged delete is
/// `Some(tombstone_bytes)`, matching the Dynamo edge's own delete-marker
/// convention, never engine-level `None`) **or** `pending: Some(..)` (a
/// write against an indexed/streamed table, evaluated at the participant
/// leader instead — see [`PendingKindWrite`]'s doc). Exactly one of
/// `value`/`pending` is ever `Some` for a real write; `cp_txn` treats
/// `value: None, pending: None` as a caller error.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TxnTableWrite {
    pub table: String,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    #[serde(default)]
    pub pending: Option<PendingKindWrite>,
}

impl TxnTableWrite {
    /// A plain (already-known-value) write — the common case, and the only
    /// shape available to a caller outside this crate (every field here is
    /// `pub(crate)`, so an integration test builds one through this
    /// constructor rather than the struct literal).
    #[must_use]
    pub fn plain(table: String, key: Vec<u8>, value: Option<Vec<u8>>) -> Self {
        TxnTableWrite {
            table,
            key,
            value,
            pending: None,
        }
    }
}

/// A `cp_txn`/`ClientRequest::Txn` **write-key condition** (ADR 0018 §2
/// apply-time write-key conditions amendment): `(table, key, expected)` —
/// structurally identical to [`TxnPrecondition`], but semantically distinct
/// and **must not be confused with it**: `key` here MUST also be one of
/// `writes`' own keys (this transaction's own about-to-be-written value),
/// checked at *apply* time against the tablet's own byte-level OCC
/// (`animus_cp_data::KvCommand::TxnStage`'s `conditions` field) rather than
/// re-read cross-key before staging/committing like an ordinary
/// [`TxnPrecondition`]. Feeding an own-key condition through
/// [`TxnPrecondition`] instead is exactly the self-referential-stall bug
/// the ADR 0018 PR7 amendment documented and this amendment closes — see
/// `ClientCtx::cp_txn`'s doc (`animusd`).
pub type TxnWriteCondition = (String, Vec<u8>, Option<Vec<u8>>);

/// A `cp_txn`/`ClientRequest::Txn`'s participant-write against an
/// indexed/streamed table (ADR 0046 U3, `TxnStage` kind-writes stack PR2):
/// the leader-evaluated dual of [`ClientRequest::KindWriteItem`]'s payload,
/// staged instead of proposed directly. Evaluated at the participant's own
/// tablet leader — never at the coordinator/edge — for the identical
/// cross-node-race reason `KindWriteItem`'s own doc explains (see
/// `ClientCtx::txn_prepare`, `animusd`).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PendingKindWrite {
    pub pk: animus_dynamo::AttributeValue,
    pub sk: Option<animus_dynamo::AttributeValue>,
    pub op: KindWriteOp,
    #[serde(default)]
    pub condition: Option<animus_dynamo::ConditionExpression>,
}

/// A request from a client to a node (length-prefixed JSON over TCP).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ClientRequest {
    /// Read the node's cached cluster metadata.
    Status,
    /// Store `value` at `key` of `table`. **Every key names a table** (ADR 0023):
    /// `table` is a **required** field — there is no unscoped keyspace, so a
    /// table-less frame fails to decode (the type cannot express one). The write
    /// routes to `table`'s tablet group leader (CP, linearizable; forwarded
    /// cross-process if this node isn't it).
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        table: String,
    },
    /// Store many `(key, value)` pairs of `table` as **one Raft log entry** on the
    /// tablet group leader (one propose → one commit round → one apply) — the
    /// bulk-write throughput primitive behind DynamoDB `BatchWriteItem` and the bulk
    /// seeder. Every entry here belongs to the **same tablet** (the caller groups by
    /// tablet before proposing, so within one tablet the batch is atomic); it is
    /// also the cross-process forwarding payload for a batch (ADR 0017 #3b). `table`
    /// is **required** (ADR 0023).
    PutBatch {
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        table: String,
    },
    /// **Internal index-maintenance RPC — never sent bare, only wrapped in
    /// [`Forwarded`](Self::Forwarded)** (ADR 0041 §3/§4): commit `writes`
    /// spanning several of one tablet's row-kind scopes, plus an optional
    /// change-log record, as **one** `KvCommand::KindBatch` Raft entry.
    ///
    /// Every key here belongs to the **same tablet** (they share a partition
    /// key, hence a token — the caller checks this before proposing), which is
    /// what makes an LSI row atomic with the base row it derives from.
    ///
    /// Bare delivery is rejected because this is the DynamoDB edge's own
    /// maintenance primitive, not a client operation: a client sending one
    /// could write arbitrary bytes straight into a table's LSI/change-log
    /// scopes and desynchronise its indexes from its base rows. `Put`/
    /// `PutBatch` remain the client-facing writes, and they only ever reach
    /// the base kind.
    KindWrite {
        table: String,
        writes: Vec<(u8, Vec<u8>, Option<Vec<u8>>)>,
        #[serde(default)]
        change_log: Vec<(Vec<u8>, Vec<u8>)>,
    },
    /// **Internal index-read RPC — never sent bare, only wrapped in
    /// [`Forwarded`](Self::Forwarded)** (ADR 0041 §5): a **linearizable**
    /// range scan of one of `table`'s non-base row-kind scopes over
    /// `[start, end)` — the per-tablet forwarding payload behind both an LSI
    /// `Query` (single-tablet, `ClientCtx::cp_scan_kind`) and an LSI `Scan`'s
    /// table-wide fan-out (`ClientCtx::cp_scan_kind_table`, one `KindScan`
    /// per overlapping tablet). `end: None` is unbounded above — the tail
    /// tablet of a table-wide fan-out, mirroring [`Scan`](Self::Scan)'s own
    /// `end: None` unbounded-above convention.
    ///
    /// Bare delivery is rejected for the same reason [`KindWrite`](Self::KindWrite)
    /// is: this reads a scope a client operation never names directly, and
    /// exposing it bare would let an arbitrary caller read a table's
    /// LSI/change-log/footprint bytes by kind number rather than through the
    /// DynamoDB surface that interprets them.
    ///
    /// `limit` (ADR 0041 §5 as-built) is [`ClientCtx::cp_scan_kind_table`]'s
    /// per-tablet cap — **not pushdown**: `StorageEngine::scan` has no limit
    /// parameter, so the tablet still reads its whole `[start, end)`
    /// sub-range; the win is a smaller wire payload for this reply and less
    /// coordinator-side memory, never reduced engine I/O. `#[serde(default)]`
    /// so an older peer's un-limited `KindScan` (equivalent to `None`)
    /// decodes on a newer one. `#[serde(default)]` on `end` predates this and
    /// is unrelated.
    KindScan {
        table: String,
        kind: u8,
        start: Vec<u8>,
        #[serde(default)]
        end: Option<Vec<u8>>,
        #[serde(default)]
        limit: Option<usize>,
        /// Descending (an LSI `Query` with `ScanIndexForward: false`).
        /// `#[serde(default)]` so a peer predating the field still decodes as
        /// the ascending scan this has always been.
        #[serde(default)]
        reverse: bool,
        /// ADR 0055: serve this read from **any** replica's applied state
        /// (`ConsistentRead: false`) instead of the linearizable ReadIndex
        /// path. The receiver serves it only if it holds a replica that
        /// passes `RaftKvNode::stale_read_ready`, and refuses otherwise —
        /// the sender then falls back to the strong path, so a refusal is
        /// never a client-visible failure. `#[serde(default)]`: a peer
        /// predating the field decodes as the linearizable read this has
        /// always been.
        #[serde(default)]
        stale: bool,
    },
    /// **Internal seal-trigger RPC — never sent bare, only wrapped in
    /// [`Forwarded`](Self::Forwarded)** (ADR 0042/0043, round-3 sealer PR):
    /// unconditionally run one seal pass (`index_drain::seal_now`) of
    /// `tablet`'s own `KIND_CHANGE` hot tail — the same mechanism the
    /// per-tablet `change_consumer_loop`'s periodic seal arm calls, just
    /// invoked once, out of band, regardless of the size/age triggers. The
    /// **one** production use is `ClientCtx::force_seal_tablet`, itself
    /// called by the DynamoDB `UpdateTable` disable path (F12-b's
    /// disable-triggered final seal) for every tablet of a table whose
    /// stream is being disabled — a table's tablets can be led on any node,
    /// not necessarily the one that received the `UpdateTable` request, so
    /// this needs the identical one-hop forward/leader-resolution machinery
    /// every other CP op already has. Addressed by `tablet` directly (the
    /// caller already knows it — there is no client key to derive it from,
    /// unlike `KindWrite`/`KindScan`). Bare delivery is refused for the same
    /// reason those two are: an arbitrary caller must not be able to force a
    /// tablet's own leader to seal on demand outside the one sanctioned
    /// call path. Not a `MetaCommand`, so `is_relayable_command` does not
    /// apply; real handling lives in `cp_serve_forwarded`'s match, reached
    /// only through the `Forwarded` arm.
    ForceSeal { tablet: u64 },
    /// **The PITR force-seal RPC (ADR 0059 §9, Train 3)** — the PITR twin of
    /// [`ForceSeal`](Self::ForceSeal), identical shape and identical
    /// reasoning (addressed by `tablet` directly; bare delivery refused;
    /// not a `MetaCommand`, so `is_relayable_command` does not apply; real
    /// handling lives in `cp_serve_forwarded`'s match). Used only by
    /// `dynamo.rs`'s `update_continuous_backups` disable path
    /// (`ClientCtx::force_pitr_seal_tablet`), mirroring `disable_stream`'s
    /// own `force_seal_tablet` call.
    ForcePitrSeal { tablet: u64 },
    /// **Internal manual-growth split-trigger RPC — never sent bare, only
    /// wrapped in [`Forwarded`](Self::Forwarded)** (ADR 0042 §14, growth
    /// PR3's `POST /admin/stream/grow`): materialize `tablet`'s own live
    /// pairs and, if it has at least 2 distinct keys, split it at their
    /// byte-weighted median (ADR 0034) via [`ClientCtx::trigger_split`] —
    /// which independently applies F11's token-rounding and Fork E's
    /// single-token skip, exactly as it does for every other split
    /// proposer. Mirrors [`ForceSeal`](Self::ForceSeal)'s shape: addressed
    /// by `tablet` directly (the caller already knows it from the table's
    /// own tablet map, not a client key), bare delivery refused for the
    /// same reason. The **one** production use is `ClientCtx::
    /// grow_stream_tablet`, called once per tablet of a streamed table by
    /// `ClientCtx::grow_stream` — a table's tablets can be led on any node,
    /// not necessarily the one that received the admin request, so this
    /// needs the identical one-hop forward/leader-resolution machinery
    /// every other CP op already has. Not a `MetaCommand`, so
    /// `is_relayable_command` does not apply; real handling lives in
    /// `cp_serve_forwarded`'s match, reached only through the `Forwarded`
    /// arm. A tablet with fewer than 2 distinct keys has no legal interior
    /// split point at all (regardless of tokens) and answers
    /// `ClientResponse::Error` naming that, distinct from Fork E's
    /// single-token-collapse message — both are skips the caller
    /// classifies as such, never hard failures.
    TriggerAutoSplit { tablet: u64 },
    /// **Internal open-shard hot-read RPC — never sent bare, only wrapped in
    /// [`Forwarded`](Self::Forwarded)** (ADR 0042 §7/§8, PR6's `GetRecords`
    /// read API): a leader-local, non-linearizable scan of `tablet`'s own
    /// `KIND_CHANGE` hot tail for records with packed HLC strictly greater
    /// than `from_position`, up to `limit`, sorted by HLC —
    /// `index_drain::hot_read`'s own wire payload. **Deliberately no
    /// `ReadIndex` barrier** (F8: the worst this can produce is a stale
    /// prefix, never a fabricated record — see that function's doc; this
    /// must never be "upgraded" to a linearizable read). The **one**
    /// production use is `ClientCtx::read_stream_hot_records`, called by the
    /// DynamoDB Streams `GetRecords` handler's open-shard path — a shard's
    /// tablet can be led on any node, not necessarily the one that received
    /// the `GetRecords` request, so this needs the identical one-hop
    /// forward/leader-resolution machinery every other CP op already has.
    /// Addressed by `tablet` directly, mirroring [`ForceSeal`](Self::ForceSeal)
    /// (there is no client key to derive it from). Bare delivery is refused
    /// for the same reason `ForceSeal`/`KindScan` are: an arbitrary caller
    /// must not be able to read a tablet's own change-log bytes by kind
    /// number, bypassing the DynamoDB Streams surface that interprets them.
    /// Not a `MetaCommand`, so `is_relayable_command` does not apply; real
    /// handling lives in `cp_serve_forwarded`'s match, reached only through
    /// the `Forwarded` arm. Answered with `ClientResponse::Pairs` (the
    /// filtered/sorted/limited `(source_key, change_record bytes)` list —
    /// the same shape `Scan`/`KindScan` already use for a raw key/value
    /// list; the packed HLC each key's own trailing 8 bytes encode is
    /// recovered by the caller, not carried out-of-band).
    StreamHotRead {
        tablet: u64,
        from_position: u64,
        limit: usize,
    },
    /// **Internal backfill-cursor-cleanup RPC — never sent bare, only
    /// wrapped in [`Forwarded`](Self::Forwarded)** (ADR 0045 §5 step 3):
    /// delete `tablet`'s own backfill cursor row for `index` (`KIND_CURSOR`,
    /// tag `backfill:{index}`) — an idempotent tombstone write, a no-op if
    /// already absent. The **one** production use is `ClientCtx::
    /// clear_backfill_cursor_for_table`, called by the DynamoDB `UpdateTable`
    /// index-delete cascade (`dynamo.rs::drop_index`) for every one of the
    /// base table's *current* tablets, so a later `CreateTableIndex` of the
    /// exact same name never silently resumes from a stale position the
    /// deleted index's own backfill sweep left behind (see
    /// `index_drain::clear_backfill_cursor`'s doc for the full argument).
    /// Addressed by `tablet` directly, mirroring [`ForceSeal`](Self::ForceSeal)/
    /// [`StreamHotRead`](Self::StreamHotRead) (there is no client key to
    /// derive it from). Bare delivery is refused for the same reason those
    /// two are. Not a `MetaCommand`, so `is_relayable_command` does not
    /// apply; real handling lives in `cp_serve_forwarded`'s match, reached
    /// only through the `Forwarded` arm.
    ClearBackfillCursor { tablet: u64, index: String },
    /// **Internal evaluate-at-leader write RPC — never sent bare, only
    /// wrapped in [`Forwarded`](Self::Forwarded)** (ADR 0046 "evaluate at
    /// leader", U3, tracked against `docs/adr-tablet-log-model` #222): the
    /// fix for a cross-node race the edge-evaluated
    /// `index_aware_write`/`kind_writes_for_item` design had — two edge
    /// nodes writing the same item never contended on the same
    /// **node-local** `rmw_lock`, so both could diff against the same stale
    /// old image and the loser's stale LSI row orphaned forever (nothing
    /// reconciles an LSI; only a GSI drain self-heals). Moving the
    /// read → evaluate-`condition` → diff span onto the item's own tablet
    /// **leader** — which every write of this item reaches regardless of
    /// which edge node received it — closes the race structurally: `pk`/
    /// `sk` name the item, `op` is the write itself (self-contained enough
    /// for the leader to compute the new value without trusting anything
    /// the caller precomputed), and `condition` (now `Serialize`/
    /// `Deserialize`, ADR 0046 U3) is the caller's own `ConditionExpression`,
    /// evaluated by the leader against its own read rather than the
    /// caller's.
    ///
    /// Folds in `UpdateItem` too (`KindWriteOp::Update`): its base-value
    /// read-modify-write had the identical cross-node hazard, closed by the
    /// same mechanism at no extra cost.
    ///
    /// Bare delivery is refused for the same reason [`KindWrite`](Self::KindWrite)
    /// is — a client could otherwise force an arbitrary write into a
    /// table's base/LSI/change scopes bypassing every DynamoDB-level
    /// validation. Not a `MetaCommand`, so `is_relayable_command` does not
    /// apply; real handling lives in `cp_serve_forwarded`'s match, reached
    /// only through the `Forwarded` arm. Answered with
    /// [`KindWriteOk`](ClientResponse::KindWriteOk) or
    /// [`ConditionFailed`](ClientResponse::ConditionFailed).
    KindWriteItem {
        table: String,
        pk: animus_dynamo::AttributeValue,
        sk: Option<animus_dynamo::AttributeValue>,
        op: KindWriteOp,
        #[serde(default)]
        condition: Option<animus_dynamo::ConditionExpression>,
    },
    /// Read the latest value at `key` of `table` (linearizable CP ReadIndex on the
    /// group leader). `table` is **required** (ADR 0023).
    Get {
        key: Vec<u8>,
        table: String,
        /// ADR 0055: serve this read from **any** replica's applied state
        /// (`ConsistentRead: false`) instead of the linearizable ReadIndex
        /// path. The receiver serves it only if it holds a replica that
        /// passes `RaftKvNode::stale_read_ready`, and refuses otherwise —
        /// the sender then falls back to the strong path, so a refusal is
        /// never a client-visible failure. `#[serde(default)]`: a peer
        /// predating the field decodes as the linearizable read this has
        /// always been.
        #[serde(default)]
        stale: bool,
    },
    /// **Internal non-blocking single-shot read RPC — never sent bare, only
    /// wrapped in [`Forwarded`](Self::Forwarded)** (ADR 0018 §2, the
    /// torn-pair-fix stack's PR2 amendment): the point-in-time analog of
    /// [`Get`](Self::Get) — one [`RaftKvNode::linearizable_get_served_fast`]
    /// attempt plus, on a `Pending`/`Foreign` intent, one status-query +
    /// push, **never** the bounded local wait `Get`'s own forwarding path
    /// gets. The forwarding payload behind
    /// [`ClientCtx::cp_read_snapshot`], itself the read primitive backing
    /// `TransactGetItems`'s quiescent round (`dynamo::quiescent_multi_get`)
    /// — see that function's doc for why every key of a round needs this
    /// uniform shape instead of `Get`'s.
    ///
    /// Bare delivery is refused for the same reason `KindWrite`/`KindScan`
    /// are: this is the DynamoDB edge's own internal read-shape primitive,
    /// not a client operation — a bare client asking for it would get
    /// exactly `Get`'s own blocking semantics silently ignored instead of a
    /// clear refusal, which is worse than refusing outright. Real handling
    /// lives in `ClientCtx::cp_serve_forwarded`'s match, reached only
    /// through the `Forwarded` arm; not a `MetaCommand`, so
    /// `is_relayable_command` does not apply.
    GetSnapshot { key: Vec<u8>, table: String },
    /// Delete `key` of `table` from the **CP** plane (a Raft-committed tombstone).
    /// `table` is **required** (ADR 0023).
    Delete { key: Vec<u8>, table: String },
    /// A **linearizable range scan** of `table` over `[start, end)`, up to `limit`
    /// keys, served from the group leader (ReadIndex). The CP read primitive behind
    /// the DynamoDB `Query`/`Scan` edges; also the cross-process
    /// forwarding payload for a scan (ADR 0017 #3b). `table` is **required** (ADR
    /// 0023) — scans are per-table fan-outs.
    Scan {
        start: Vec<u8>,
        /// Exclusive upper bound, or `None` for **unbounded above** — a whole-table
        /// scan (ADR 0023), since a per-table tablet's engine has no finite max key.
        end: Option<Vec<u8>>,
        #[serde(default)]
        limit: Option<usize>,
        /// Descending: `limit` keeps the *highest* rows of the range and they
        /// come back highest-key-first (a `Query` with `ScanIndexForward:
        /// false`). `#[serde(default)]` so a peer that predates the field —
        /// or any of the many ascending constructors — still decodes as the
        /// ascending scan this has always been.
        #[serde(default)]
        reverse: bool,
        table: String,
        /// ADR 0055: serve this read from **any** replica's applied state
        /// (`ConsistentRead: false`) instead of the linearizable ReadIndex
        /// path. The receiver serves it only if it holds a replica that
        /// passes `RaftKvNode::stale_read_ready`, and refuses otherwise —
        /// the sender then falls back to the strong path, so a refusal is
        /// never a client-visible failure. `#[serde(default)]`: a peer
        /// predating the field decodes as the linearizable read this has
        /// always been.
        #[serde(default)]
        stale: bool,
    },
    /// A CP op **forwarded** from a node that received it but does not host the CP
    /// group leader, to the leader's node (ADR 0017 #3b cross-process routing). The
    /// receiving node serves it locally **iff** it is the leader; it never
    /// re-forwards, so routing is bounded to one hop (a stale hint errors and the
    /// client retries with fresh routing). Carries the original [`Put`]/[`Get`].
    Forwarded {
        request: Box<ClientRequest>,
        /// The W3C `traceparent` of the span that initiated this forward, if
        /// distributed tracing export is active (ADR 0027) — `None` is the
        /// default/no-op case. Lets the receiving node's span join the same
        /// trace instead of starting one disconnected from the origin.
        #[serde(default)]
        traceparent: Option<String>,
    },
    /// Relay a **schema-catalog** `MetaCommand` to be proposed on the control-plane
    /// leader (v1 Phase 1 / A2, ADR 0013): a node that received a `CreateTable` /
    /// `CREATE TABLE` / `CreateTableIndex` but isn't the control leader sends this to
    /// the leader's node, so DDL on a follower-connected client still commits. Any
    /// node accepts it, **gates** it to schema-catalog commands (membership /
    /// placement commands are rejected — not a general "propose anything" surface),
    /// and routes it to the control leader (locally if it is the leader, else
    /// relaying toward the leader — bounded, since a relay only targets a known
    /// leader). The result replicates back to every node's `Metadata` as usual; the
    /// caller confirms by polling its own replicated view.
    ProposeSchema(MetaCommand),
    /// **Admin: split a CP tablet** at `split_key`. A single, atomic control-plane
    /// command (`MetaCommand::SplitTablet`, epoch-CAS gated): the source tablet's
    /// range narrows and a new sibling tablet is minted covering `[split_key, ∞)`,
    /// both served by this node's *existing* per-node shared engine — no data
    /// moves, and no second, data-plane step is needed (the old two-phase split is
    /// gone; see the root `CLAUDE.md`). The interim manual trigger; an automatic
    /// size-telemetry trigger is `auto_split_loop`.
    SplitTablet { tablet: u64, split_key: Vec<u8> },
    /// **Join discovery** (ADR 0032 PR2, `animusd join`): a node that knows only
    /// a *seed* address (any already-running node's **intra-cluster** address
    /// — ADR 0047, was the client address pre-ADR-0047; old or newly grown,
    /// PR1 made every node's address book equally current) asks
    /// for enough information to start as a growth member without an
    /// operator-assembled expanded `ClusterConfig`. Any node can answer — the
    /// reply is built entirely from the receiving node's own knowledge (its
    /// captured `AdminInfo` + its live `client_route`/`intra_route`), no
    /// forwarding needed. This is itself served on the intra listener only
    /// (`JoinInfo` is `Surface::Intra`).
    /// An additive variant: both sides of a cluster are the same build in
    /// this repo's pre-alpha stance, so no version negotiation is needed for
    /// an older peer that predates it.
    JoinInfo,
    /// **Long-poll metadata watch** (ADR 0035 PR5): park on the answering
    /// node's own [`animus_control::MetadataWatch`] for up to
    /// `WATCH_METADATA_SERVER_TIMEOUT` (`animusd`) and reply once it advances
    /// past `last_seen` **or** the bound elapses (a normal, not-an-error
    /// outcome — the caller just retries with the same `last_seen`, exactly
    /// like a `Status` poll that happened not to see a change). Replaces the
    /// old fixed-interval `Status` poll both a data-only node's and (ADR
    /// 0035 PR5) an ADR 0030 growth node's mirror sync used, closing most of
    /// the latency gap between "control commits" and "the mirror observes
    /// it" without a new push mechanism. Only a genuine control-group
    /// replica (`ControlHandle::Local`) serves this — see
    /// `ClientCtx::watch_metadata`'s doc (`animusd`) for why a `Remote` node
    /// rejects it instead of degrading. Replies with the same
    /// [`ClientResponse::Status`] shape a plain `Status` request gets,
    /// carrying the watermark to pass back as the next call's `last_seen`.
    WatchMetadata { last_seen: u64 },
    /// **Multi-participant transaction** (ADR 0018 §2/PR4): atomically write
    /// every `(table, key, Option<value>)` in `writes` — `None` is a staged
    /// delete — across however many tablets (possibly several tables) they
    /// span. `preconditions` (optional; empty for a plain transaction) is
    /// `(table, key, expected)` — a `TransactWriteItems`-shaped condition
    /// check (`expected: None` means "must be absent"): if any precondition
    /// no longer matches by the time the transaction is ready to commit, the
    /// whole thing aborts with a retryable conflict error instead of
    /// committing. The client-facing entry point; the coordinator drives it
    /// via `ClientCtx::cp_txn`. The single-tablet case is not special-cased
    /// on the wire — it degenerates to zero participants, the same three log
    /// entries (stage/commit/resolve) `RaftKvNode::txn_write` uses.
    Txn {
        writes: Vec<TxnTableWrite>,
        #[serde(default)]
        preconditions: Vec<TxnPrecondition>,
        /// **Write-key conditions** (ADR 0018 §2 apply-time write-key
        /// conditions amendment): `(table, key, expected)` where `key` is
        /// one of `writes`' own keys — see [`TxnWriteCondition`]'s doc for
        /// why this must stay distinct from `preconditions`.
        #[serde(default)]
        write_conditions: Vec<TxnWriteCondition>,
    },
    /// **Internal 2PC coordinator RPC — never sent bare, only wrapped in
    /// [`Forwarded`](Self::Forwarded)** (ADR 0018 §2/PR4): stage `writes` as
    /// intents on `table`'s tablet leader. `anchor: None` is the **anchor**
    /// stage — mint a fresh txn id/record key and create the `Pending`
    /// record (`RaftKvNode::txn_stage`). `anchor: Some((txn_id, record_key,
    /// record_table))` is a **participant** stage referencing an
    /// already-known anchor record (`RaftKvNode::txn_stage_participant`) —
    /// no record is created or touched here. See `ClientCtx::txn_prepare`,
    /// the one caller (`animusd`).
    ///
    /// **`conditions`** (ADR 0018 §2 apply-time write-key conditions
    /// amendment): own-key byte-level OCC preconditions for this stage's
    /// own `writes` — see `animus_cp_data::KvCommand::TxnStage`'s doc.
    ///
    /// **`participant_spans`** (ADR 0018 §2/PR5, task #18 fix): every
    /// *other* participant's `(table, span)` pairs — meaningful, and
    /// merged into the freshly-created record's `intent_spans`, only for
    /// the anchor case (`anchor: None`); a participant's own stage ignores
    /// it (it creates no record). Both `#[serde(default)]` so these stay
    /// internal-only wire shape additions, no back-compat concern (house
    /// convention: no live deployments).
    TxnPrepare {
        table: String,
        anchor: Option<(TxnId, Vec<u8>, String)>,
        writes: Vec<animus_cp_data::TxnWrite>,
        #[serde(default)]
        conditions: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        #[serde(default)]
        participant_spans: Vec<(String, KeyRange)>,
        /// **ADR 0046 U3, `TxnStage` kind-writes stack PR2**: writes against
        /// an indexed/streamed table, evaluated at THIS receiving leader
        /// (never precomputed by the coordinator) and merged into `writes`
        /// before staging — see [`PendingKindWrite`]'s doc. `#[serde(default)]`,
        /// same house convention as `conditions`/`participant_spans` above.
        #[serde(default)]
        pending_kind_writes: Vec<PendingKindWrite>,
    },
    /// **Internal 2PC coordinator RPC — anchor only, never sent bare**
    /// (ADR 0018 §2/PR4): commit or abort `txn_id`'s record at `record_key`
    /// (on `table`'s tablet leader — always the anchor's own table).
    /// `commit: true` uses `min_commit_ts` as the floor for
    /// `RaftKvNode::txn_commit_at_least` (the coordinator's candidate commit
    /// timestamp — the max of every participant's acked stage ts, per the
    /// protocol); `commit: false` uses `RaftKvNode::txn_abort`.
    ///
    /// **Resolves nothing** (ADR 0018 §2/PR5, a change from PR4's shape):
    /// the caller resolves every participant, the anchor's own keys
    /// included, uniformly via a separate `TxnResolve` — see
    /// `ClientCtx::txn_decide_anchor`, the one caller (`animusd`), for the
    /// full rationale (including why the reply now carries the record's
    /// **actual** decided outcome, not just a proposed ts).
    ///
    /// **`orphan_created_ts` (ADR 0018 §2/PR5's orphan-record fix)**:
    /// `Some(created_ts)` means "no record exists at all — synthesize an
    /// `Aborted` tombstone directly" (`RaftKvNode::txn_abort_orphan`),
    /// overriding `commit`/`min_commit_ts` entirely (an orphan can only
    /// ever be aborted — see `ClientCtx::txn_recover`'s doc for why). `None`
    /// is the ordinary commit/abort-of-an-existing-record case, unchanged.
    TxnDecide {
        table: String,
        txn_id: TxnId,
        record_key: Vec<u8>,
        commit: bool,
        min_commit_ts: HlcTimestamp,
        #[serde(default)]
        orphan_created_ts: Option<HlcTimestamp>,
    },
    /// **Internal 2PC coordinator RPC — every participant including the
    /// anchor, never sent bare** (ADR 0018 §2/PR4): resolve `keys` on
    /// `table`'s tablet leader per the already-decided `outcome`
    /// (`RaftKvNode::txn_resolve`) — routed by one of `keys` itself, **not**
    /// `record_key` (which, for a non-anchor participant, lives in a
    /// different table's keyspace entirely). See
    /// `ClientCtx::txn_resolve_participant`, the one caller (`animusd`).
    TxnResolve {
        table: String,
        txn_id: TxnId,
        record_key: Vec<u8>,
        keys: Vec<Vec<u8>>,
        outcome: TxnOutcome,
    },
    /// **Internal cross-tablet status query — never sent bare** (ADR 0018
    /// §2/PR4): a reader that hit a foreign intent (its covering record
    /// lives on a *different* tablet than the one it's reading) routes this
    /// to `record_key`'s own owning table/tablet leader
    /// (`RaftKvNode::txn_status_local`) to learn the transaction's decided
    /// (or still-pending) status. See `ClientCtx::txn_status`, the one
    /// caller (`animusd`, from `cp_get_local`'s foreign-intent path).
    TxnStatus { table: String, record_key: Vec<u8> },
    /// **Internal recovery RPC — never sent bare** (ADR 0018 §2/PR5): the
    /// recovery-view dual of [`TxnStatus`](Self::TxnStatus) — like it, but
    /// also returns `intent_spans`/`created_ts`, everything a recovery
    /// pusher needs (`RaftKvNode::txn_record_view`). See
    /// `ClientCtx::txn_record_view`, the one caller (`animusd`).
    TxnRecordView { table: String, record_key: Vec<u8> },
    /// **Internal recovery RPC — never sent bare** (ADR 0018 §2/PR5): does
    /// `table`'s tablet leader still hold a live intent for `txn_id`
    /// anywhere in `span` (`RaftKvNode::txn_verify_staged`)? A recovery
    /// pusher sends one of these per `(table, span)` entry in a record's
    /// `intent_spans` before deciding whether every participant staged.
    /// See `ClientCtx::txn_verify`, the one caller (`animusd`).
    TxnVerify {
        table: String,
        span: KeyRange,
        txn_id: TxnId,
    },
}

/// Where a [`ClientRequest`] variant may be received **bare** — a
/// classification result, not a listener identity (`animusd::ListenerKind`
/// is the distinct listener-identity type; see `surface_of`'s doc for why
/// sharing one enum for both would make the refusal rule look symmetric
/// when it is not). [`surface_of`] is the one exhaustive table computing
/// this; `animusd::handle_request`'s one guard clause is the one place it
/// is consulted.
///
/// **`Intra` is a superset of `Public`, not a disjoint partition**: nothing
/// stops the intra listener from also serving a `Public`-surfaced request —
/// deliberately, since neither port has authentication yet at this
/// milestone, and intra is meant to be the more, not less, trusted segment.
/// A future reader should not "fix" this into a second refusal layer without
/// re-reading ADR 0047's rationale.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Surface {
    /// Reachable on either listener.
    Public,
    /// Reachable **only** on the intra listener — refused bare on `Client`.
    Intra,
}

/// The single source of truth for which listener(s) may receive a bare
/// [`ClientRequest`] variant (ADR 0047). A free function beside
/// `animusd`'s `request_kind`, same convention: **no wildcard arm**, so
/// adding a `ClientRequest` variant anywhere is a compile error here until
/// it is explicitly classified.
///
/// Every internal-only forwarding payload is listed explicitly here even
/// though [`Forwarded`](ClientRequest::Forwarded)'s own `Intra`
/// classification already makes them transitively unreachable via the
/// client listener (a bare send of one of them is refused independently of
/// ever being wrapped) — leaving that implicit would mean "is this variant
/// reachable on the client port" depends on reasoning about two gates
/// together instead of this one table being the complete answer on its own.
///
/// **Scope note**: this table's exhaustiveness retires the standing "grep
/// every gating site" lesson (root `CLAUDE.md`) *only* for the
/// client-vs-intra reachability axis. It does **not** touch
/// [`is_relayable_command`] (whether a `MetaCommand` may ride the
/// `ProposeSchema` relay envelope) or `ClientCtx::cp_serve_forwarded`'s own
/// match (`animusd`; whether real handling exists for a forwarded payload)
/// — both stay exactly as grep-dependent as before; unrelated axes.
pub fn surface_of(request: &ClientRequest) -> Surface {
    match request {
        ClientRequest::Status
        | ClientRequest::Put { .. }
        | ClientRequest::PutBatch { .. }
        | ClientRequest::Get { .. }
        | ClientRequest::Scan { .. }
        | ClientRequest::Delete { .. }
        | ClientRequest::Txn { .. }
        | ClientRequest::SplitTablet { .. } => Surface::Public,

        ClientRequest::Forwarded { .. }
        | ClientRequest::ProposeSchema(_)
        | ClientRequest::JoinInfo
        | ClientRequest::WatchMetadata { .. }
        | ClientRequest::KindWrite { .. }
        | ClientRequest::KindScan { .. }
        | ClientRequest::GetSnapshot { .. }
        | ClientRequest::ForceSeal { .. }
        | ClientRequest::ForcePitrSeal { .. }
        | ClientRequest::TriggerAutoSplit { .. }
        | ClientRequest::StreamHotRead { .. }
        | ClientRequest::ClearBackfillCursor { .. }
        | ClientRequest::KindWriteItem { .. }
        | ClientRequest::TxnPrepare { .. }
        | ClientRequest::TxnDecide { .. }
        | ClientRequest::TxnResolve { .. }
        | ClientRequest::TxnStatus { .. }
        | ClientRequest::TxnRecordView { .. }
        | ClientRequest::TxnVerify { .. } => Surface::Intra,
    }
}

/// Whether `command` may be **relayed to the control leader** via
/// [`ClientRequest::ProposeSchema`]: the schema-catalog mutations (ADR 0013) that a
/// wire client drives, plus [`MetaCommand::RegisterCpAddr`] (Phase 2.3a) — a node's
/// own CP-address self-registration — plus [`MetaCommand::SplitTablet`] (D2), the
/// metadata half of the admin split trigger (already client-exposed via
/// [`ClientRequest::SplitTablet`], so relaying it adds no new authority — it lets the
/// trigger reach the control leader cross-process when the split is driven from a
/// follower). Other placement / tablet commands are control-plane-internal and are
/// **not** accepted over this path.
///
/// A `Down`-registering [`MetaCommand::UpsertMember`] is relayable too (ADR
/// 0030's admin add-member action, `ClientCtx::admin_add_member`): unlike
/// `admin_drain`'s `Leaving` transition (an operator action on an *existing,
/// already-Active* member, deliberately kept local-leader-only so it can't be
/// triggered accidentally through a relay chain), registering a **new** member
/// as `Down` carries no comparable risk — it grants no placement eligibility by
/// itself (the detector promotes it to `Active` only on a real heartbeat, ADR
/// 0012), and the whole point of online growth is that the admin caller may not
/// be connected to the control leader (e.g. a growth node's own control role is
/// never a real voter, ADR 0030, so relaying is its *only* way to reach the real
/// leader at all). Deliberately scoped to the `Down` status only — an
/// `Active`/`Leaving` transition on an *existing* member stays off this path,
/// same as before.
///
/// [`MetaCommand::RemoveMember`] (ADR 0032 PR3, decommission) is deliberately
/// **excluded** — symmetric with `admin_drain`'s `Leaving` transition, not
/// with the `Down` add-member case above: removing a member is a destructive,
/// rare operator action, and `ClientCtx::admin_remove_member` (`animusd`) is
/// local-control-leader-only by design (an operator retries on the leader, the
/// same UX `admin_drain` already has), so it must never reach the control
/// leader through a relay chain from a node that may not even know who leads.
///
/// **[`MetaCommand::SealStreamShard`] (ADR 0042/0043) is relayable** — a
/// tablet's own leader can be hosted on *any* data node, not necessarily one
/// that also happens to be the control-plane leader (or even control-connected
/// at all, on a split deployment), so the sealer (a later PR) needs the exact
/// same follower-connected relay path `SplitTablet`/`CreateTablet` already
/// use to reach the control leader from wherever it actually runs.
///
/// **[`MetaCommand::ExpireStreamShards`] is deliberately excluded** — unlike
/// `SealStreamShard`, its only intended production caller (the segment
/// janitor, ADR 0043 §A9, a later PR) is itself a **control-plane-leader-only**
/// background loop (the same class as `detect_loop`/`orphan_sweep_loop`
/// already are): it only ever runs — and only ever proposes — from inside a
/// process that already holds a live `RaftNode` handle for the control group
/// at the moment it decides to act, so it has no structural need for a relay
/// path at all (it proposes directly, the same way those two loops do, never
/// through [`ClientRequest::ProposeSchema`]). Leaving it off this allowlist is
/// therefore not a missing feature — it is the same deliberate access
/// restriction `RemoveMember` gets just above: a destructive-ish housekeeping
/// action (marking rows expired, then physically deleting them) has no
/// legitimate reason to be triggerable by an arbitrary relay chain from a
/// node that isn't even running the janitor.
///
/// **As built (ADR 0061 rung C1)**: rewritten from a `matches!` (no
/// exhaustiveness requirement — a new `MetaCommand` variant used to default
/// silently to "not relayable" with zero compiler signal, exactly the
/// bimodal per-process flake the root `CLAUDE.md` warns about) into this
/// exhaustive `match` with no `_ =>` arm, preserving the exact prior
/// classification for every existing variant. `tests::classification_is_pinned`
/// pins it.
pub fn is_relayable_command(command: &MetaCommand) -> bool {
    match command {
        MetaCommand::CreateTableSchema { .. }
        // Atomic `ALTER TABLE` (in-place schema replacement): a follower-
        // connected ALTER must relay like the create/drop it replaces.
        | MetaCommand::ReplaceTableSchema { .. }
        | MetaCommand::DropTableSchema { .. }
        | MetaCommand::CreateTableIndex { .. }
        | MetaCommand::DropTableIndex { .. }
        // Index status transition (ADR 0045): same schema-catalog class as
        // `CreateTableIndex`/`DropTableIndex` — the backfill
        // seeder/aggregator (`animusd`) may propose it from wherever the
        // relevant tablet/control leader actually runs.
        | MetaCommand::SetIndexStatus { .. }
        // Backfill-completion catalog commit (ADR 0045 §4): a tablet
        // leader's own "I finished seeding this index" proposal, from
        // wherever that leader actually runs — same relay reasoning as
        // `SealStreamShard` just below. `index_backfill_loop` (`animusd`,
        // the aggregator that reads this catalog and flips a table's index
        // to `Active`) is control-plane-leader-only and proposes
        // `SetIndexStatus` directly, exactly like `SealStreamShard`'s own
        // aggregator does, so it needs no relay path of its own.
        | MetaCommand::MarkIndexBackfilled { .. }
        // DynamoDB Streams enable/disable (ADR 0042): schema-catalog
        // class, same relay reason as `CreateTableIndex` — a
        // follower-connected `CreateTable`/`UpdateTable` must reach the
        // control leader.
        | MetaCommand::SetTableStream { .. }
        // DynamoDB-style TTL configuration (ADR 0051): schema-catalog
        // class, same relay reason as `SetTableStream` — a
        // follower-connected `UpdateTimeToLive` must reach the control
        // leader.
        | MetaCommand::SetTableTtl { .. }
        // Stream-shard catalog commit (ADR 0042/0043): a tablet leader's
        // own seal proposal, from wherever that leader actually runs — see
        // this function's own doc for why `ExpireStreamShards` is
        // deliberately NOT included here.
        | MetaCommand::SealStreamShard { .. }
        | MetaCommand::RegisterCpAddr { .. }
        // Node address book (ADR 0032 PR1): every node self-registers its
        // full address set at startup, from whichever node it happens to
        // connect to for control-plane proposals — must relay like
        // `RegisterCpAddr` (a follower-connected node has no other way to
        // reach the control leader).
        | MetaCommand::RegisterNodeAddrs { .. }
        // In-place split workflow (ADR 0058 Train 2 rung 3): `trigger_split`
        // proposes `BeginSplitInPlace` from whichever node's admin/
        // auto-split surface fired it, and `CutoverSplit` is proposed by
        // the parent's own leader node once its data-plane fork has
        // completed and its pre-cutover vetoes pass — both need the
        // identical follower-connected relay path `SplitTablet` already
        // has (the same relay reasoning the now-deleted copy-based
        // `BeginSplit`/`CutoverSplit` pair, ADR 0050, used to carry).
        | MetaCommand::BeginSplitInPlace { .. }
        | MetaCommand::CutoverSplit { .. }
        // Provision-at-create (ADR 0023): a `CreateTable` on a follower-connected
        // client relays the table's tablet creation + RF policy to the control
        // leader. Scoped to one tablet per table by the state machine's guard.
        | MetaCommand::CreateTablet { .. }
        | MetaCommand::SetTabletPolicy { .. }
        // Drop-table GC (ADR 0024): a `DROP TABLE` on a follower-connected
        // client relays the table's tablet removal to the control leader.
        | MetaCommand::DropTableTablets { .. }
        // Registration CAS (ADR 0040 Decision C): a joining process has
        // no local control role at all yet (it hasn't even bound its
        // listeners), so relaying `RegisterNode` via `ProposeSchema` is
        // its *only* way to reach the real leader — the identical
        // `Down`-registering `UpsertMember` case handled below, and safe
        // for the identical reason: an unclaimed `RegisterNode` apply
        // always registers the new member `Down` (never any other
        // status), granting no placement eligibility by itself — the
        // detector still requires a real heartbeat before anything
        // happens to it; a *claimed* id's apply is a `NoOp`/`Rejected`
        // no-op either way. Missing this arm would be the exact bimodal
        // per-process flake the root `CLAUDE.md` warns about: a join
        // that happens to land on a follower-connected seed would hang
        // until `JOIN_DISCOVERY_BUDGET` expires, indistinguishable from
        // "no seed answered" — see `tests/seed_join_allocated.rs`'s
        // follower-connected-seed case (`animusd`).
        | MetaCommand::RegisterNode { .. }
        // `CreateBackup` wire operation (ADR 0059 §3/§4, Train 1 PR④):
        // an operator's `CreateBackup` call may land on any node, exactly
        // like `CreateTable`/`UpdateTimeToLive` — relaying `BeginBackup`
        // (`animusd::dynamo::create_backup`'s own propose) to the control
        // leader is the same schema-catalog-class need every DDL-shaped
        // wire mutation above already has. Missing this arm is the exact
        // bimodal per-process flake the root `CLAUDE.md` warns about: a
        // `CreateBackup` that happens to land on a follower-connected
        // node would hang for `SCHEMA_COMMIT_TIMEOUT` instead of relaying
        // — see `tests/schema_ddl_relay.rs`'s
        // `create_backup_on_a_follower_is_relayed_to_the_leader`
        // (`animusd`).
        | MetaCommand::BeginBackup { .. }
        // On-demand backup per-tablet completion record (ADR 0059 §3/§4):
        // a tablet leader's own "I finished capturing my share of this
        // backup" proposal, from wherever that leader actually runs —
        // the identical relay reasoning as `MarkIndexBackfilled`/
        // `SealStreamShard` just above. `CompleteBackup`/`FailBackup`
        // are deliberately NOT included here: their only intended
        // production caller (the completion aggregator, a
        // control-plane-leader-only background loop, `animusd`) already
        // proposes them directly off its own live `RaftNode` handle,
        // exactly the same `ExpireStreamShards` precedent this function's
        // own doc states above — no wire surface exists yet for either
        // (Train 1 PR④'s concern) to need a relay path of its own.
        | MetaCommand::RecordBackupTabletComplete { .. }
        // `DeleteBackup` wire operation (ADR 0059 §3, Train 1 PR④): an
        // operator's `DeleteBackup` call may land on any node, exactly
        // like `CreateTable`/`UpdateTimeToLive` — relaying
        // `MarkBackupDeleted` (the two-phase janitor's own **mark**
        // step) to the control leader is the same schema-catalog-class
        // need every DDL-shaped wire mutation above already has.
        // `DeleteBackup` (the existing, unmodified **finalizing**
        // command) is deliberately NOT added here — its only intended
        // caller is the backup janitor (`animusd::backup_janitor`), a
        // control-plane-leader-only background loop that already holds
        // a live `RaftNode` handle, the identical `ExpireStreamShards`
        // precedent this function's own doc states above.
        | MetaCommand::MarkBackupDeleted { .. }
        // Restore workflow (ADR 0059 §7, Train 2): `RestoreTableFromBackup`
        // (`animusd::dynamo::restore_table_from_backup`) may land on any
        // node, exactly like `CreateTable`; the restore driver
        // (`animusd::backup_restore`) proposes `CompleteRestore`/
        // `FailRestore` from wherever its target tablet's own leader
        // happens to run — the identical relay reasoning as
        // `RecordBackupTabletComplete` above.
        | MetaCommand::BeginRestore { .. }
        | MetaCommand::CompleteRestore { .. }
        | MetaCommand::FailRestore { .. }
        // PITR (ADR 0059 §9, Train 3): `UpdateContinuousBackups` (the
        // wire operation's own catalog toggle) may land on any node,
        // exactly like `UpdateTimeToLive` — schema-catalog class, same
        // relay reason as `SetTableTtl` above.
        | MetaCommand::UpdateContinuousBackups { .. }
        // PITR segment catalog commit: a tablet leader's own seal
        // proposal, from wherever that leader actually runs — the
        // identical relay reasoning as `SealStreamShard` above.
        | MetaCommand::SealPitrSegment { .. }
        // Tagging a `BeginBackup` row as a PITR base snapshot: proposed
        // by `pitr_janitor::pitr_snapshot_loop` (`animusd`), a
        // control-plane-leader-only background loop with its own live
        // `RaftNode` handle on every node shape it runs on — this crate
        // never relays it through the wire edge today, but it is
        // included here defensively, mirroring `SealPitrSegment`'s own
        // class, rather than surfacing as an opaque relay refusal if
        // that ever changes. `ExpirePitrSegments` is deliberately NOT
        // included, for the identical reason `ExpireStreamShards` isn't:
        // its only intended caller (`pitr_janitor::pitr_janitor_loop`)
        // already proposes directly off its own live `RaftNode` handle.
        | MetaCommand::MarkBackupPitrBase { .. }
        // Directed-Placing completion record (ADR 0062 §3): a tablet
        // leader's own "my locally-driven Raft membership has converged
        // to this child's placement target" report, from wherever that
        // leader actually runs — the identical relay reasoning as
        // `MarkIndexBackfilled`/`SealStreamShard` above. The aggregating
        // side of this catalog is the reconcile loop's own directed-Placing
        // phase (a later rung), which is control-plane-leader-only and
        // proposes `CasTabletReplicas` directly off its own live
        // `RaftNode` handle, so it needs no relay path of its own.
        | MetaCommand::MarkSplitPlacingDone { .. } => true,

        // Online growth (ADR 0030): admin add-member registers a new raftkv
        // id as `Down` — see the doc above for why this is safe to relay
        // unlike drain (which stays local-leader-only). Any other status
        // transition on an *existing* member stays off this path.
        MetaCommand::UpsertMember {
            status: NodeStatus::Down,
            ..
        } => true,
        MetaCommand::UpsertMember { .. } => false,

        // No commit-log bookkeeping to relay — never sent over this path.
        MetaCommand::NoOp => false,
        // Epoch-CAS placement command — control-plane-internal, proposed
        // directly by the placement reconciler off its own live `RaftNode`
        // handle, never through the wire relay.
        MetaCommand::CasTabletReplicas { .. } => false,
        // Directed-Placing dwell-gated retarget (ADR 0062 §2, issue #528
        // fix): proposed directly by the control-plane leader's own
        // `reconcile_loop` off its own live `RaftNode` handle — the same
        // class as `CasTabletReplicas` just above, never a tablet leader's
        // own report (that's `MarkSplitPlacingDone`, which IS relayable).
        MetaCommand::RetargetSplitPlacing { .. } => false,
        // See this function's own doc: destructive/housekeeping actions
        // whose only sanctioned caller already holds a live `RaftNode`
        // handle (or, for `RemoveMember`, is deliberately local-leader-only)
        // rather than a relay path.
        MetaCommand::ExpireStreamShards { .. } => false,
        MetaCommand::ExpirePitrSegments { .. } => false,
        MetaCommand::RemoveMember { .. } => false,
        MetaCommand::CompleteBackup { .. } => false,
        MetaCommand::FailBackup { .. } => false,
        MetaCommand::DeleteBackup { .. } => false,
    }
}

/// A node's reply to a [`ClientRequest`].
// `Status` carries a whole `Metadata` by design (it IS the metadata reply);
// the size skew vs. unit-ish variants is inherent to the wire protocol, and a
// response is built, serialized, and dropped — never stored in bulk.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ClientResponse {
    /// Cached cluster metadata (membership + tablet map), plus (ADR 0035 §1)
    /// the answering node's own best-known control-plane leader —
    /// `self.control.leader()` + `ClientCtx::route_addr(leader_id)`. Serves
    /// two ADR 0035 PR4 needs from one reply: `ControlHandle::Remote`'s
    /// `leader()`/`leader_addr_hint()`, and `metadata_fresh`'s leader-directed
    /// retry target. **`propose_schema`'s own relay tier no longer uses this
    /// field** (ADR 0047) — it uses the parallel `intra_leader_hint` below,
    /// since its relay target must be the intra address, not this
    /// client-flavored one. `#[serde(default)]` so an older node's reply
    /// (predating either field) still parses, decoding to `None`/`0`.
    ///
    /// **`watermark` (ADR 0035 PR5)**: the answering node's own applied-index
    /// watch (`ControlHandle::metadata_watch().latest()`) at reply time — the
    /// value a caller passes back as the next
    /// [`ClientRequest::WatchMetadata`]'s `last_seen`, and the monotonic
    /// freshness proxy `control_handle::RemoteControlClient::observe`
    /// (`animusd`) uses to reject a reply from a replica lagging behind one
    /// it already saw.
    ///
    /// **`control_voters` (ADR 0037 PR2)**: the answering node's own live
    /// control-voter set (`ControlHandle::config()`, `unwrap_or_default()`'d
    /// — see that method's doc for the `Option`'s meaning) at reply time.
    /// This is the wire echo of `RaftCore::config()` — the actual, live Raft
    /// configuration that governs control-plane quorum — as opposed to
    /// `metadata.node_addrs`' `role: "control"` bookkeeping entries (a
    /// discovery *hint*, not this authority; a node can be registered with
    /// the control role and still not be a live voter, e.g. before its
    /// `change_membership` lands or after it's been removed). Lets a
    /// `ControlHandle::Remote` data-only node, an admin caller, or a future
    /// CLI learn "who can I even try talking to for a control-plane
    /// proposal" without a local `RaftCore` of its own — see
    /// `RemoteControlClient::control_voters`'s doc (`animusd`).
    /// `#[serde(default)]` so an older node's reply (predating this field)
    /// still parses, decoding to an empty set — indistinguishable on the
    /// wire from "this replica genuinely reported zero voters" for an old
    /// peer, but no worse than that field's total absence was before this
    /// PR, and every in-process consumer that cares about telling
    /// "unknown" apart from "empty" reads `ControlHandle::config()`'s
    /// `Option` directly rather than round-tripping through this wire copy.
    Status {
        metadata: Metadata,
        #[serde(default)]
        leader_hint: Option<(NodeId, String)>,
        /// The intra-cluster dual of `leader_hint` (ADR 0047) — machine-
        /// relay-only, never surfaced to a human (see the root `CLAUDE.md`'s
        /// hint-field-conflation lesson). `#[serde(default)]`, same
        /// robustness pattern as `leader_hint`.
        #[serde(default)]
        intra_leader_hint: Option<(NodeId, String)>,
        #[serde(default)]
        watermark: u64,
        #[serde(default)]
        control_voters: BTreeSet<NodeId>,
    },
    /// A write reached its quorum.
    PutOk,
    /// A read reached its quorum; the value (or `None` if absent).
    Value(Option<Vec<u8>>),
    /// Reply to [`KindWriteItem`](ClientRequest::KindWriteItem): the write
    /// landed (base row, plus its LSI rows/change-log record if the table
    /// takes the kind-write path) — `old`/`new` are exactly what
    /// `index_aware_write`'s edge-evaluated design used to hand back to its
    /// own caller directly, needed for `ReturnValues`/`UpdateReturnValues`
    /// echo. `new: None` for a `Delete` op.
    ///
    /// `collection_bytes` is the leader's own base + LSI byte total for the
    /// tablet that hosts the item — the DynamoDB `ItemCollectionMetrics`
    /// size input (see `dynamo::collection_bytes_at_leader`, `animusd`). It
    /// is produced *at the leader* because that is the only node that holds
    /// the tablet's engine; the receiving edge has no way to price a tablet
    /// it does not host, which is precisely why this rides back on the
    /// reply rather than being computed after the hop. `#[serde(default)]`
    /// so a peer predating the field still decodes, reporting no estimate
    /// rather than a wrong one.
    KindWriteOk {
        old: Option<animus_dynamo::Item>,
        new: Option<animus_dynamo::Item>,
        #[serde(default)]
        collection_bytes: Option<u64>,
    },
    /// Reply to [`KindWriteItem`](ClientRequest::KindWriteItem): the
    /// caller's own `condition` did not match the leader's own read of the
    /// current item. Distinguishable on the wire from
    /// [`Error`](Self::Error) — like [`Unresolved`](Self::Unresolved) just
    /// below, this is an expected outcome a caller acts on directly (compose
    /// `WireError::conditional_check_failed`), not a transient failure to
    /// retry.
    ConditionFailed,
    /// Reply to [`GetSnapshot`](ClientRequest::GetSnapshot): the queried
    /// key's covering transaction did not resolve within one single,
    /// point-in-time attempt (ADR 0018 §2, torn-pair-fix stack PR2) — see
    /// `SnapshotRead::Unresolved`'s doc (`animusd`). Distinguishable on the
    /// wire from [`Error`](Self::Error) so the caller's round-level retry
    /// (never a per-key one) is the only thing that acts on it — an
    /// ordinary transient routing/leadership failure still comes back as
    /// `Error` and is retried exactly like any other CP op.
    Unresolved,
    /// A range scan's live `(key, value)` pairs in key order (reply to
    /// [`Scan`](ClientRequest::Scan)).
    Pairs(Vec<(Vec<u8>, Vec<u8>)>),
    /// The operation could not be served (no quorum, no tablet, etc.).
    Error(String),
    /// Reply to [`JoinInfo`](ClientRequest::JoinInfo) (ADR 0032 PR2): everything
    /// a joining node (`animusd join`) needs to start as a growth member —
    /// this cluster's **pre-growth** control group (`original_control_ids` for
    /// `run_node_growth`/`run_node_join`, `animusd`), the answering node's
    /// internal peer book (`AdminInfo.peers`), its live client-op routing
    /// table (`ClientCtx::route_snapshot`, kept fresh by `route_sync_loop`,
    /// ADR 0032 PR1), and every known admin address (the dashboard fan-out
    /// seed).
    JoinInfo {
        control_ids: Vec<NodeId>,
        peers: BTreeMap<NodeId, String>,
        client_route: BTreeMap<NodeId, String>,
        /// The answering node's live intra-cluster routing table (ADR 0047),
        /// paralleling `client_route` — the joining node seeds its own
        /// `ctx.intra_route` from this, load-bearing for the exact same
        /// reason `client_route` is: the growth-node-mirror branch inside
        /// `BoundNode::start_with_streams` resolves `ctx.intra_addr(id)`
        /// synchronously, before this node's own `intra_route_sync_loop` has
        /// had a chance to tick.
        intra_route: BTreeMap<NodeId, String>,
        admin_addrs: Vec<SocketAddr>,
    },
    /// **Incremental long-poll reply to
    /// [`WatchMetadata`](ClientRequest::WatchMetadata)** (ADR 0038 PR5): the
    /// answering node's own [`animus_control::RaftNode::watch_delta_since`]
    /// covered `(last_seen, watermark]` contiguously in its bounded
    /// system-keyspace delta ring, so instead of a full [`Status`](Self::Status)
    /// clone this carries just the [`animus_control::mirror::KeyWrite`]s
    /// those commits produced, in commit order. The caller
    /// (`control_handle::RemoteControlClient::observe_delta`, `animusd`)
    /// installs them verbatim onto its own cached `Metadata` via
    /// `animus_control::mirror::apply_key_write`, never replaying a
    /// `MetaCommand` itself — a mirror would otherwise need this crate's
    /// full control-plane business logic, exactly the design constraint
    /// `Status`'s original whole-`Metadata`-clone shape was chosen to avoid
    /// duplicating.
    ///
    /// **Only ever a `WatchMetadata` reply** — a plain
    /// [`Status`](ClientRequest::Status) request always gets the full
    /// [`Status`](Self::Status) reply, unconditionally; `WatchMetadata`
    /// itself falls back to [`Status`](Self::Status) whenever the ring
    /// doesn't (or no longer) cover the requested range (a fresh, lagging,
    /// or just-recovered replica, or a caller whose `last_seen` aged out of
    /// the bounded window) — see `ClientCtx::watch_metadata`'s doc
    /// (`animusd`). `writes` is empty exactly when `watermark == last_seen`
    /// (the timeout-elapsed, nothing-changed case) — cheaper than a full
    /// `Metadata` clone even then.
    MetadataDelta {
        writes: Vec<animus_control::mirror::KeyWrite>,
        watermark: u64,
        leader_hint: Option<(NodeId, String)>,
        /// The intra-cluster dual of `leader_hint` (ADR 0047) — see
        /// `Status`'s own field doc.
        #[serde(default)]
        intra_leader_hint: Option<(NodeId, String)>,
        control_voters: BTreeSet<NodeId>,
    },
    /// Reply to [`Txn`](ClientRequest::Txn): the transaction committed at
    /// `commit_ts` (ADR 0018 §2/PR4).
    TxnCommitted { commit_ts: HlcTimestamp },
    /// Reply to [`TxnPrepare`](ClientRequest::TxnPrepare): this participant's
    /// (or the anchor's own) stage entry committed and applied at `ts`.
    /// `txn_id`/`record_key`/`record_table` are echoed back for a
    /// participant stage (the caller already knows them) and freshly minted
    /// for an anchor stage (the caller learns them from this reply).
    ///
    /// **`outcome`** (ADR 0018 §2 apply-time write-key conditions
    /// amendment): whether the stage actually landed, and if not, why —
    /// `ts == txn_id.ts` for the anchor case, matching the pre-existing
    /// contract, but a caller must check `outcome` before trusting that its
    /// writes actually staged: `animus_cp_data::StageOutcome::Staged` is the
    /// only success case; `ConditionFailed` is final (never retry);
    /// `IntentBlocked` is retryable (push the named blocker); `Fenced` is a
    /// structural rejection (a stale route, or an already-decided race).
    TxnPrepared {
        txn_id: TxnId,
        record_key: Vec<u8>,
        record_table: String,
        ts: HlcTimestamp,
        outcome: StageOutcome,
    },
    /// Reply to [`TxnDecide`](ClientRequest::TxnDecide): the record's
    /// **actual, applied** decision (ADR 0018 §2/PR5 — no longer just "the
    /// ts my own proposal landed at": with recovery, a coordinator's
    /// commit/abort proposal can lose to a concurrent recovery decision on
    /// the same record, so the caller must report the record's real,
    /// possibly-different outcome, never assume its own proposal won — see
    /// the decision-semantics amendment).
    TxnDecided { outcome: TxnOutcome },
    /// Reply to [`TxnStatus`](ClientRequest::TxnStatus): the record's
    /// current (possibly still-`Pending`) status.
    TxnStatusReply { status: TxnDecisionStatus },
    /// Reply to [`TxnRecordView`](ClientRequest::TxnRecordView) (ADR 0018
    /// §2/PR5). `None` means the answering leader's own read barrier
    /// confirmed **definitively no record** at this key — distinct from a
    /// `ClientResponse::Error` reply, which means the query could not be
    /// served at all (issue #298 shape B fix: the caller must never treat
    /// the two as interchangeable, see `RaftKvNode::txn_record_view`'s doc).
    TxnRecordViewReply { view: Option<TxnRecordView> },
    /// Reply to [`TxnVerify`](ClientRequest::TxnVerify) (ADR 0018 §2/PR5):
    /// does the answering tablet still hold a live intent for the queried
    /// `txn_id` over the queried span?
    TxnVerifyReply { staged: bool },
    /// Reply to [`TxnResolve`](ClientRequest::TxnResolve) (ADR 0018 §2
    /// write-loss amendment §3/§6): the resolve entry committed and
    /// applied, carrying the actual [`ResolveOutcome`] it produced.
    ///
    /// **A caller must check `outcome` before treating this as done** —
    /// `Resolved` is the only success case; `Fenced`/`OutcomeMismatch`
    /// mean nothing here actually resolved (most commonly a concurrent
    /// split moved the target key's range out from under the caller's
    /// routing decision between `cp_route` and this entry's actual apply),
    /// and the caller must re-route with fresh metadata and retry against
    /// whichever tablet(s) now actually own these keys. Before this
    /// variant existed, every case here (including a fence-miss no-op)
    /// replied `ClientResponse::PutOk`, indistinguishable from a genuine
    /// resolve — the exact gap the amendment names.
    TxnResolved { outcome: ResolveOutcome },
}

#[cfg(test)]
mod tests {
    use animus_control::{
        ColumnType, IndexDef, IndexKind, IndexProjection, IndexStatus, NodeAddrs, StreamSpec,
        StreamViewType, TableSchema, TtlSpec,
    };
    use animus_env::nid;
    use animus_tablet::{Epoch, KeyRange, TabletId};

    use super::*;

    /// Pins `is_relayable_command`'s classification of every `MetaCommand`
    /// variant (ADR 0061 rung C1's `matches!` -> exhaustive `match`
    /// hardening) so a future change to it is visible in a diff instead of
    /// silently defaulting a new/moved variant to "not relayable". Kept as
    /// one flat table (not per-variant `#[test]`s) so it reads as the same
    /// allowlist the function's own doc describes.
    #[test]
    fn classification_is_pinned() {
        let table = "t".to_string();
        let schema = TableSchema::simple("pk", ColumnType::String);
        let index = IndexDef {
            name: "by_x".to_string(),
            kind: IndexKind::Local,
            hash_attribute: "pk".to_string(),
            sort_attribute: Some("x".to_string()),
            projection: IndexProjection::All,
            status: IndexStatus::active(),
        };
        let addrs = NodeAddrs {
            internal: String::new(),
            client: String::new(),
            intra: String::new(),
            admin: String::new(),
            role: "combined".to_string(),
        };

        let true_cases: Vec<MetaCommand> = vec![
            MetaCommand::CreateTableSchema {
                table: table.clone(),
                schema: schema.clone(),
            },
            MetaCommand::DropTableSchema {
                table: table.clone(),
            },
            MetaCommand::ReplaceTableSchema {
                table: table.clone(),
                schema: schema.clone(),
            },
            MetaCommand::CreateTableIndex {
                table: table.clone(),
                index: index.clone(),
            },
            MetaCommand::DropTableIndex {
                table: table.clone(),
                index: "by_x".to_string(),
            },
            MetaCommand::SetIndexStatus {
                table: table.clone(),
                index: "by_x".to_string(),
                status: IndexStatus::Active,
            },
            MetaCommand::MarkIndexBackfilled {
                table: table.clone(),
                index: "by_x".to_string(),
                tablet: TabletId(1),
            },
            MetaCommand::SetTableStream {
                table: table.clone(),
                spec: Some(StreamSpec {
                    view_type: StreamViewType::NewAndOldImages,
                    label: "L".to_string(),
                }),
            },
            MetaCommand::SetTableTtl {
                table: table.clone(),
                spec: Some(TtlSpec {
                    attribute_name: "expires_at".to_string(),
                }),
            },
            MetaCommand::SealStreamShard {
                table: table.clone(),
                label: "L".to_string(),
                tablet: TabletId(1),
                epoch: 0,
                view_type: StreamViewType::NewAndOldImages,
                hlc_range: (0, 0),
                count: 0,
                seal_wall_ms: 0,
                replicas: vec![],
                object_id: "obj".to_string(),
            },
            MetaCommand::RegisterCpAddr {
                id: nid(1),
                addr: "127.0.0.1:1".to_string(),
                tablet: None,
            },
            MetaCommand::RegisterNodeAddrs {
                node: nid(1),
                addrs: addrs.clone(),
            },
            MetaCommand::CutoverSplit {
                parent: TabletId(1),
                expected_epoch: Epoch::INITIAL,
                cutover_wall_ms: 0,
            },
            MetaCommand::BeginSplitInPlace {
                parent: TabletId(1),
                expected_epoch: Epoch::INITIAL,
                split_key: vec![1],
                children: [(TabletId(2), vec![]), (TabletId(3), vec![])],
            },
            MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some(table.clone()),
                range: KeyRange::whole(),
                replicas: vec![],
            },
            MetaCommand::SetTabletPolicy {
                tablet: TabletId(1),
                policy: None,
            },
            MetaCommand::DropTableTablets {
                table: table.clone(),
            },
            MetaCommand::UpsertMember {
                node: nid(1),
                labels: Default::default(),
                status: NodeStatus::Down,
            },
            MetaCommand::RegisterNode {
                node: nid(1),
                addrs: addrs.clone(),
                labels: Default::default(),
            },
            MetaCommand::BeginBackup {
                backup_id: "b1".to_string(),
                table: table.clone(),
                created_wall_ms: 0,
                backup_name: "b1-name".to_string(),
            },
            MetaCommand::RecordBackupTabletComplete {
                backup_id: "b1".to_string(),
                tablet: TabletId(1),
                cut_version: 0,
                bytes: 0,
            },
            MetaCommand::MarkBackupDeleted {
                backup_id: "b1".to_string(),
            },
            MetaCommand::BeginRestore {
                restore_id: "r1".to_string(),
                backup_id: "b1".to_string(),
                source_table: table.clone(),
                target_table: "t2".to_string(),
                tablet: TabletId(1),
                replicas: vec![],
                gsi_defs: vec![],
                pitr: None,
            },
            MetaCommand::CompleteRestore {
                restore_id: "r1".to_string(),
            },
            MetaCommand::FailRestore {
                restore_id: "r1".to_string(),
                reason: "x".to_string(),
            },
            MetaCommand::UpdateContinuousBackups {
                table: table.clone(),
                enabled: true,
                wall_ms: 0,
            },
            MetaCommand::SealPitrSegment {
                table: table.clone(),
                generation: 1,
                tablet: TabletId(1),
                epoch: 0,
                hlc_range: (0, 0),
                count: 0,
                seal_wall_ms: 0,
                replicas: vec![],
                object_id: "obj".to_string(),
            },
            MetaCommand::MarkBackupPitrBase {
                backup_id: "b1".to_string(),
            },
            MetaCommand::MarkSplitPlacingDone {
                tablet: TabletId(2),
                expected_epoch: Epoch::INITIAL,
            },
        ];
        for cmd in &true_cases {
            assert!(is_relayable_command(cmd), "expected relayable: {cmd:?}");
        }

        let false_cases: Vec<MetaCommand> = vec![
            MetaCommand::NoOp,
            MetaCommand::UpsertMember {
                node: nid(1),
                labels: Default::default(),
                status: NodeStatus::Active,
            },
            MetaCommand::CasTabletReplicas {
                tablet: TabletId(1),
                expected_epoch: Epoch::INITIAL,
                replicas: vec![],
            },
            MetaCommand::RetargetSplitPlacing {
                tablet: TabletId(2),
                expected_epoch: Epoch::INITIAL,
                target: Some(vec![nid(1)]),
            },
            MetaCommand::ExpireStreamShards {
                rows: vec![],
                remove: false,
            },
            MetaCommand::ExpirePitrSegments {
                rows: vec![],
                remove: false,
            },
            MetaCommand::RemoveMember { node: nid(1) },
            MetaCommand::CompleteBackup {
                backup_id: "b1".to_string(),
            },
            MetaCommand::FailBackup {
                backup_id: "b1".to_string(),
                reason: "x".to_string(),
            },
            MetaCommand::DeleteBackup {
                backup_id: "b1".to_string(),
            },
        ];
        for cmd in &false_cases {
            assert!(
                !is_relayable_command(cmd),
                "expected NOT relayable: {cmd:?}"
            );
        }
    }
}
