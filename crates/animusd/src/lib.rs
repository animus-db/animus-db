//! Node assembly: wires the control plane (`RaftNode`), the **CP data plane**
//! (`RaftKvNode`, the leaderful per-tablet Raft group), and a client-facing
//! request server into a runnable AnimusDB node over `ProdEnv`. v1 (ADR 0019) is
//! CP-only — the leaderless AP plane (`animus-data`) is gone.
//!
//! ## Roles and the single-consumer rule
//!
//! A node's [`Network`] inbox is single-consumer, so each protocol that does its
//! own `recv` gets a **distinct node id and `ProdEnv`** (a distinct listener):
//!
//! - **control** — the Raft `RaftNode` (cluster metadata: membership, tablet map,
//!   the schema catalog),
//! - **raftkv** — the leaderful **CP** per-tablet Raft group (`RaftKvNode`,
//!   ADR 0017 #3a), the linearizable data plane that serves all reads/writes.
//!
//! The **client API is a plain request/reply TCP server** (length-prefixed
//! JSON), *not* part of the `Network` abstraction: a node that does not host the
//! CP group leader **forwards** a data op to the leader's node over a fresh client
//! connection (ADR 0017 #3b), so dynamic client addresses never touch the internal
//! network.
//!
//! Construction is two-phase so a whole cluster can bind to ephemeral ports
//! first and then exchange addresses: [`Node::bind`] → assemble the peer book →
//! [`BoundNode::start`]. [`bind_cluster`] / [`start_cluster`] do this for an
//! in-process cluster (used by the binary's `--cluster` mode and the tests).

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub mod config;
mod index_drain;
pub mod otel;
pub use config::{ClusterConfig, DynamoAuthConfig, SplitMode};
// Re-exported so callers (CLI, tests, operators) can inspect a node's cached
// cluster metadata — membership status and the tablet map — without depending on
// `animus-control` directly.
pub use animus_control::{
    ColumnDef, ColumnType, IndexStatus, MetaCommand, Metadata, NodeAddrs, NodeStatus, TableSchema,
};

mod admin;
mod console;
mod control_handle;
mod dashboard;
mod dynamo;
mod dynamo_streams;
mod http;
mod index_backfill;
mod segment_janitor;
mod topology;
mod ttl_reaper;

use control_handle::{ControlHandle, RemoteControlClient};

use animus_control::node::{DEFAULT_ORPHAN_SWEEP_AFTER, HEARTBEAT_INTERVAL, send_heartbeat};
use animus_control::{PlacementPolicy, ProposeResult, RaftNode};
use animus_cp_data::hlc::HlcTimestamp;
use animus_cp_data::host::{MemoryTabletEngines, MetadataView, Reconciler};
use animus_cp_data::{
    FastRead, IntentInfo, KindBatchOutcome, RaftKvNode, StageOutcome, TxnDecisionStatus, TxnId,
    TxnOutcome, TxnRecordView,
};
use animus_env::{Clock, Disk, Env, FsSegmentStore, Metric, MetricsHandle, NodeId, ProdEnv};
use animus_storage::{
    Key, LsmEngine, MemoryEngine, SsTableView, StorageEngine, StorageError, VersionedValue,
    WalRecordView,
};
use animus_tablet::{KeyRange, TOKEN_BYTES, TabletId, TabletState};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::Instrument;
// Pure CP-topology decision logic (routing predicates), extracted into
// `topology` for unit-test coverage — called fully-qualified
// (`topology::decide_cp_route` etc.) at each call site. The per-node
// hosting/GC decisions this module used to hold (`plan_join_host`,
// `tablets_to_reclaim`, `tablets_to_release`) moved to
// `animus_cp_data::host` (ADR 0031 PR3/PR4), which now owns both the
// decision and its execution.

/// A `(key, value)` pair — a scan row / batch-write element.
type KvPair = (Vec<u8>, Vec<u8>);

/// A single write within a multi-participant transaction (ADR 0018 §2/PR4;
/// ADR 0046 A1 kind-writes payload) — a direct alias of
/// `animus_cp_data::TxnWrite`, never a locally-duplicated shape: `key`/
/// `value` (`None` is a staged delete) plus, for a write against an
/// indexed/streamed table, the derived `kind_writes`/`change_log` payload
/// materialized at resolve. Matches `RaftKvNode::txn_stage_anchor`/
/// `txn_stage_participant`'s own `writes` shape exactly, so it rides through
/// with zero conversion.
type TxnWrite = animus_cp_data::TxnWrite;
/// A `cp_txn` precondition (ADR 0018 §2/PR4): `(table, key, expected)` —
/// `expected: None` means "must be absent".
type TxnPrecondition = (String, Vec<u8>, Option<Vec<u8>>);
/// One item write of an indexed/streamed table's transaction (ADR 0046 U3,
/// `TxnStage` kind-writes stack PR2): the leader-evaluated dual of
/// [`ClientRequest::KindWriteItem`]'s payload, staged instead of proposed
/// directly. `run_transact` builds one of these per write action against a
/// `table_takes_kind_write_path` table rather than precomputing the item's
/// new value/diff itself (a stale coordinator-local read is exactly the
/// cross-node race ADR 0046 U3 closes for the *ordinary* write path —
/// `dynamo::kind_write_item_at_leader` — and closes here identically):
/// [`ClientCtx::txn_prepare`] evaluates it **at the participant's own tablet
/// leader**, under the same `ctx.data().rmw_lock` `kind_write_item_at_leader`
/// takes, immediately before staging — never at the coordinator/edge.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PendingKindWrite {
    pub(crate) pk: animus_dynamo::AttributeValue,
    pub(crate) sk: Option<animus_dynamo::AttributeValue>,
    pub(crate) op: KindWriteOp,
    #[serde(default)]
    pub(crate) condition: Option<animus_dynamo::ConditionExpression>,
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
    pub(crate) table: String,
    pub(crate) key: Vec<u8>,
    pub(crate) value: Option<Vec<u8>>,
    #[serde(default)]
    pub(crate) pending: Option<PendingKindWrite>,
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
/// [`ClientCtx::cp_txn`]'s doc.
type TxnWriteCondition = (String, Vec<u8>, Option<Vec<u8>>);
/// A stage's own-key conditions, scoped to one (table, tablet) group — the
/// `animus_cp_data::KvCommand::TxnStage`-shaped `(key, expected)` list
/// [`ClientCtx::cp_txn`]/`txn_prepare`/`txn_prepare_pushing` pass through to
/// `CpGroup::txn_stage`/`txn_stage_participant`. Named to keep the
/// `BTreeMap<(String, TabletId), _>` grouping map under clippy's
/// `type_complexity` bar.
type StageConditions = Vec<(Vec<u8>, Option<Vec<u8>>)>;

/// Why a [`ClientCtx::cp_txn`] 2PC attempt aborted (ADR 0018's 2026-08-24
/// `CancellationReasons` amendment, issue #374 C2b) — carried across the 2PC
/// boundary, including the forwarded `TxnPrepare` hop (via [`Self::encode`]/
/// [`Self::decode`], mirroring `dynamo::encode_relayed_error`/
/// `decode_relayed_error`'s marker-prefixed-string convention), so
/// `dynamo::run_transact` can flag the exact action index responsible
/// instead of falling back to an aggregate-only message.
///
/// **Never conflate [`Self::ConditionFailed`] with
/// [`Self::TransactionConflict`]**: the former is a **permanent**
/// `ConditionalCheckFailedException` — the condition was evaluated against a
/// fixed observed value, so retrying the identical request changes nothing.
/// The latter is a **lost race** against another transaction's own
/// still-unresolved intent (`animus_cp_data::txn::StageOutcome::
/// IntentBlocked`, ADR 0018 §2/PR6) surviving `txn_prepare_pushing`'s own
/// bounded retry budget — transient, and a client's own retry can succeed.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum TxnAbortReason {
    /// A write action's own `ConditionExpression` (evaluated at its
    /// participant leader, `ClientCtx::txn_stage_local`) — or the identical
    /// condition re-checked at apply time
    /// (`animus_cp_data::txn::StageOutcome::ConditionFailed`) — evaluated to
    /// false against `key`'s current value.
    ConditionFailed { table: String, key: Vec<u8> },
    /// `key` already held a different, still-unresolved transaction's own
    /// intent (`StageOutcome::IntentBlocked`) even after
    /// `txn_prepare_pushing`'s bounded retry budget.
    TransactionConflict { table: String, key: Vec<u8> },
    /// Every other abort reason: a routing failure, a structural `Fenced`
    /// rejection, a precondition re-check mismatch, or any other internal
    /// error — carries only a human message, the same fidelity
    /// `WireError::transaction_canceled` (the aggregate-only constructor)
    /// always had.
    Other(String),
}

impl TxnAbortReason {
    /// Marker prefix distinguishing an encoded [`TxnAbortReason`] from a
    /// plain, pre-existing error string on the forwarded `TxnPrepare` hop —
    /// same convention as `dynamo::RELAYED_WIRE_ERROR_MARK`.
    const MARK: &'static str = "txn-abort-reason:";

    /// Encode for `ClientResponse::Error` (the forwarded `TxnPrepare` hop's
    /// only error channel) so [`Self::decode`] can recover the typed reason
    /// on the far side.
    fn encode(&self) -> String {
        match serde_json::to_string(self) {
            Ok(json) => format!("{}{json}", Self::MARK),
            // Unreachable in practice (every field here is a plain String/
            // Vec<u8>, both infallibly serializable) — degrade to the
            // aggregate-only shape rather than panic on a forwarded reply.
            Err(_) => Self::Other(self.to_string()).to_string(),
        }
    }

    /// [`Self::encode`]'s inverse. An unmarked string (a plain internal
    /// error from any pre-this-amendment call site, or a peer running an
    /// older build) degrades to `Other(raw)` — never a panic, never a
    /// silently-wrong variant.
    fn decode(raw: &str) -> Self {
        raw.strip_prefix(Self::MARK)
            .and_then(|json| serde_json::from_str(json).ok())
            .unwrap_or_else(|| TxnAbortReason::Other(raw.to_owned()))
    }
}

impl std::fmt::Display for TxnAbortReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TxnAbortReason::ConditionFailed { table, key } => {
                write!(f, "a condition on table `{table}` key {key:?} was not met")
            }
            TxnAbortReason::TransactionConflict { table, key } => write!(
                f,
                "table `{table}` key {key:?} lost a race against another in-flight transaction"
            ),
            TxnAbortReason::Other(msg) => write!(f, "{msg}"),
        }
    }
}

/// `TxnAbortReason::encode`/`decode` are `pub(crate)` (no external `tests/`
/// binary can reach them), so this lives as an in-crate `#[cfg(test)]`
/// module (same idiom as `kind_batch_signal_tests` above) — no cluster
/// bring-up needed, just the pure marker-prefixed-string round trip (ADR
/// 0018's 2026-08-24 `CancellationReasons` amendment, issue #374 C2b).
#[cfg(test)]
mod txn_abort_reason_tests {
    use super::TxnAbortReason;

    #[test]
    fn condition_failed_round_trips_through_encode_decode() {
        let reason = TxnAbortReason::ConditionFailed {
            table: "t".into(),
            key: vec![1, 2, 3],
        };
        assert_eq!(TxnAbortReason::decode(&reason.encode()), reason);
    }

    #[test]
    fn transaction_conflict_round_trips_through_encode_decode() {
        let reason = TxnAbortReason::TransactionConflict {
            table: "t".into(),
            key: vec![9],
        };
        assert_eq!(TxnAbortReason::decode(&reason.encode()), reason);
    }

    /// The reachability case the e2e suite calls out as impractical to
    /// exercise end to end: a peer's plain, unmarked error string (a
    /// pre-this-amendment build, or any genuinely internal failure that
    /// never went through `encode`) must degrade to `Other`, never panic
    /// or silently misparse as a different variant.
    #[test]
    fn an_unmarked_string_degrades_to_other() {
        assert_eq!(
            TxnAbortReason::decode("no CP group leader reachable for txn prepare"),
            TxnAbortReason::Other("no CP group leader reachable for txn prepare".into())
        );
    }

    /// A marked-but-corrupted payload (mismatched build, truncated in
    /// transit) degrades the same way — never a panic.
    #[test]
    fn a_marked_but_undecodable_payload_degrades_to_other() {
        let raw = format!("{}not valid json", TxnAbortReason::MARK);
        assert_eq!(TxnAbortReason::decode(&raw), TxnAbortReason::Other(raw));
    }
}

/// A decided [`TxnOutcome`]'s public-status mirror (ADR 0018 §2/PR5) — the
/// two types mean the same thing (`Committed`/`Aborted`) but come from
/// different call sites (`TxnOutcome` is what a coordinator/recovery pusher
/// constructs and resolves with; `TxnDecisionStatus` is what a status/record
/// view read reports, and additionally has a `Pending` variant a decided
/// outcome can never be).
fn outcome_to_status(o: &TxnOutcome) -> TxnDecisionStatus {
    match o {
        TxnOutcome::Committed { commit_ts } => TxnDecisionStatus::Committed {
            commit_ts: *commit_ts,
        },
        TxnOutcome::Aborted => TxnDecisionStatus::Aborted,
    }
}

/// A hosted leaderful CP per-tablet Raft group on this node (ADR 0017 #3a) — the
/// v1 data plane (ADR 0019). It is backed by either the durable on-disk
/// [`LsmEngine`] or the volatile [`MemoryEngine`], chosen by [`StorageBackend`] at
/// start; the enum lets the node hold one regardless of engine. `RaftKvNode` is
/// cheap to clone (clones share the core + engine), so the variants clone too.
#[derive(Clone)]
enum CpGroup {
    /// Durable on-disk LSM (default; survives a restart).
    Lsm(RaftKvNode<ProdEnv, LsmEngine<ProdEnv>>),
    /// Volatile in-memory engine (ephemeral runs).
    Mem(RaftKvNode<ProdEnv, MemoryEngine>),
}

impl CpGroup {
    /// Propose a write to the group (honored on the leader), stamping `fence`
    /// Propose a write to this group. See [`RaftKvNode::put`].
    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> ProposeResult {
        match self {
            CpGroup::Lsm(n) => n.put(key, value),
            CpGroup::Mem(n) => n.put(key, value),
        }
    }

    /// As [`put`](Self::put), but for a **batch put** — commit every
    /// `(key, value)` as one Raft entry. See [`RaftKvNode::put_batch`].
    fn put_batch(&self, puts: Vec<(Vec<u8>, Vec<u8>)>) -> ProposeResult {
        match self {
            CpGroup::Lsm(n) => n.put_batch(puts),
            CpGroup::Mem(n) => n.put_batch(puts),
        }
    }

    /// As [`put`](Self::put), but for a **multi-kind atomic batch** — base
    /// row, LSI rows, footprint and optional change-log records as one Raft
    /// entry (ADR 0041 §3/§4). See
    /// [`RaftKvNode::put_kind_batch_conditioned`].
    fn put_kind_batch_conditioned(
        &self,
        writes: Vec<(u8, Vec<u8>, Option<Vec<u8>>)>,
        change_log: Vec<(Vec<u8>, Vec<u8>)>,
        conditions: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    ) -> ProposeResult {
        match self {
            CpGroup::Lsm(n) => n.put_kind_batch_conditioned(writes, change_log, conditions),
            CpGroup::Mem(n) => n.put_kind_batch_conditioned(writes, change_log, conditions),
        }
    }

    /// Every pending change-log record this tablet holds, in commit order
    /// (ADR 0041 §4). See [`RaftKvNode::pending_changes`].
    pub(crate) async fn pending_changes(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        match self {
            CpGroup::Lsm(n) => n.pending_changes().await,
            CpGroup::Mem(n) => n.pending_changes().await,
        }
    }

    /// This group's current Raft term — one axis of the ledger-named-object
    /// amendment's per-attempt segment id (ADR 0042 §10/ADR 0043 §A3,
    /// `index_drain::seal_now`): a node that crashes and later resumes
    /// leading this same group again does so at a strictly higher term
    /// (Raft's own guarantee), so folding it into the id disambiguates a
    /// same-node restart even against an RNG stream that happened to
    /// replay identically. See [`RaftKvNode::term`].
    pub(crate) fn term(&self) -> u64 {
        match self {
            CpGroup::Lsm(n) => n.term(),
            CpGroup::Mem(n) => n.term(),
        }
    }

    /// A bounded base-scope scan over `[start, end)` in key order — the
    /// partition-range read the GSI drain recomputes an item's index rows from.
    pub(crate) async fn local_scan_bounded(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        match self {
            CpGroup::Lsm(n) => n.local_scan(start, Some(end), None).await,
            CpGroup::Mem(n) => n.local_scan(start, Some(end), None).await,
        }
    }

    /// An unbounded-above base-scope scan starting at `start`, truncated to
    /// `limit` rows — the backfill seeder's own "peek ahead one partition at
    /// a time" primitive (ADR 0045 §2), unlike [`local_scan_bounded`](
    /// Self::local_scan_bounded)'s single-partition-width bound. `end: None`
    /// is still bounded to *this tablet's own live range*, never a
    /// whole-engine scan — see [`RaftKvNode::local_scan`]'s own doc.
    pub(crate) async fn local_scan_from(
        &self,
        start: &[u8],
        limit: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        match self {
            CpGroup::Lsm(n) => n.local_scan(start, None, Some(limit)).await,
            CpGroup::Mem(n) => n.local_scan(start, None, Some(limit)).await,
        }
    }

    /// Read one key of a non-base row-kind scope (ADR 0041 §3). See
    /// [`RaftKvNode::local_get_kind`].
    pub(crate) async fn local_get_kind(&self, kind: u8, key: &[u8]) -> Option<Vec<u8>> {
        match self {
            CpGroup::Lsm(n) => n.local_get_kind(kind, key).await,
            CpGroup::Mem(n) => n.local_get_kind(kind, key).await,
        }
    }

    // ---- eventually-consistent reads (ADR 0055) --------------------------

    /// Whether this replica may serve an eventually-consistent read — the
    /// purely local freshness gate. See [`RaftKvNode::stale_read_ready`].
    pub(crate) fn stale_read_ready(&self) -> bool {
        match self {
            CpGroup::Lsm(n) => n.stale_read_ready(),
            CpGroup::Mem(n) => n.stale_read_ready(),
        }
    }

    /// An eventually-consistent point read from this replica's own engine.
    /// See [`RaftKvNode::stale_get_served`] — outer `None` is "not served",
    /// never absence.
    pub(crate) async fn stale_get_served(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        match self {
            CpGroup::Lsm(n) => n.stale_get_served(key).await,
            CpGroup::Mem(n) => n.stale_get_served(key).await,
        }
    }

    /// An eventually-consistent base-scope range scan of this replica's own
    /// engine. See [`RaftKvNode::stale_scan`]/[`RaftKvNode::stale_scan_rev`].
    pub(crate) async fn stale_scan(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        match (self, reverse) {
            (CpGroup::Lsm(n), false) => n.stale_scan(start, end, limit).await,
            (CpGroup::Lsm(n), true) => n.stale_scan_rev(start, end, limit).await,
            (CpGroup::Mem(n), false) => n.stale_scan(start, end, limit).await,
            (CpGroup::Mem(n), true) => n.stale_scan_rev(start, end, limit).await,
        }
    }

    /// An eventually-consistent **kind-scoped** range scan of this replica's
    /// own engine (ADR 0041 §3 scopes; the LSI/GSI-hidden-table read path).
    ///
    /// This is plain [`RaftKvNode::local_scan_kind`] and needs no
    /// stale-specific envelope resolution: a non-base scope only ever holds
    /// **committed** values (only `KvCommand::KindBatch` writes them, and it
    /// always commits outright), so there is no intent for an eventual read
    /// to fall back past — the difference from the linearizable form is
    /// purely the missing ReadIndex barrier.
    pub(crate) async fn stale_scan_kind(
        &self,
        kind: u8,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        match (self, reverse) {
            (CpGroup::Lsm(n), false) => n.local_scan_kind(kind, start, end, limit).await,
            (CpGroup::Lsm(n), true) => n.local_scan_kind_rev(kind, start, end, limit).await,
            (CpGroup::Mem(n), false) => n.local_scan_kind(kind, start, end, limit).await,
            (CpGroup::Mem(n), true) => n.local_scan_kind_rev(kind, start, end, limit).await,
        }
    }

    /// A non-linearizable, bounded scan of one non-base row-kind scope (ADR
    /// 0041 §3) — the raw kind-scan primitive tests use to prove exactly
    /// which kinds an entry wrote (e.g. a streamed-unindexed table's write
    /// commits base + change only, never an LSI/footprint row). `end: None`
    /// is unbounded above. See [`RaftKvNode::local_scan_kind`].
    ///
    /// Only called from `dynamo::stream_write_path_tests` today — the
    /// `cfg_attr` below is a **precise**, not blanket, dead-code allowance
    /// (only in effect for the non-`cfg(test)` build the `tests/` binaries
    /// and the release lib link against; the `cargo test -p animusd --lib`
    /// build, which actually exercises it, sees no allowance at all).
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn local_scan_kind_bounded(
        &self,
        kind: u8,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        match self {
            CpGroup::Lsm(n) => n.local_scan_kind(kind, start, end, None).await,
            CpGroup::Mem(n) => n.local_scan_kind(kind, start, end, None).await,
        }
    }

    /// As [`local_scan_kind_bounded`](Self::local_scan_kind_bounded), but
    /// with a real row cap threaded through — the TTL reaper's own per-tick
    /// scan bound (`ttl_reaper.rs`, ADR 0051 §4/§6: a local, non-waking
    /// read, capped so one huge TTL-enabled table's tablet cannot
    /// monopolize one tick). See [`RaftKvNode::local_scan_kind`]'s own
    /// `limit` doc — a per-tablet cap on the *returned* rows, not scan
    /// pushdown.
    pub(crate) async fn local_scan_kind_capped(
        &self,
        kind: u8,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        match self {
            CpGroup::Lsm(n) => n.local_scan_kind(kind, start, end, Some(limit)).await,
            CpGroup::Mem(n) => n.local_scan_kind(kind, start, end, Some(limit)).await,
        }
    }

    /// This tablet's own ADR 0042 §7 min-over-rows cursor watermark for
    /// `consumer`. See [`RaftKvNode::cursor_min_watermark`].
    pub(crate) async fn cursor_min_watermark(&self, consumer: &str) -> Option<HlcTimestamp> {
        match self {
            CpGroup::Lsm(n) => n.cursor_min_watermark(consumer).await,
            CpGroup::Mem(n) => n.cursor_min_watermark(consumer).await,
        }
    }

    /// Linearizable ReadIndex range scan of a non-base row-kind scope (ADR
    /// 0041 §3) — the LSI `Query`/`Scan` read primitive. `end: None` is
    /// unbounded above; `limit` is a **per-tablet cap, not pushdown** (see
    /// [`RaftKvNode::local_scan_kind`]'s doc). See
    /// [`RaftKvNode::linearizable_scan_kind`].
    async fn linearizable_scan_kind(
        &self,
        kind: u8,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
        match self {
            CpGroup::Lsm(n) => n.linearizable_scan_kind(kind, start, end, limit).await,
            CpGroup::Mem(n) => n.linearizable_scan_kind(kind, start, end, limit).await,
        }
    }

    /// Descending kind-scoped ReadIndex scan.
    /// See [`RaftKvNode::linearizable_scan_kind_rev`].
    async fn linearizable_scan_kind_rev(
        &self,
        kind: u8,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
        match self {
            CpGroup::Lsm(n) => n.linearizable_scan_kind_rev(kind, start, end, limit).await,
            CpGroup::Mem(n) => n.linearizable_scan_kind_rev(kind, start, end, limit).await,
        }
    }

    /// As [`put`](Self::put), but for a delete (tombstone). See
    /// [`RaftKvNode::delete`].
    fn delete(&self, key: Vec<u8>) -> ProposeResult {
        match self {
            CpGroup::Lsm(n) => n.delete(key),
            CpGroup::Mem(n) => n.delete(key),
        }
    }

    /// Linearizable ReadIndex read with "not served" disambiguated from
    /// "served, absent" — see [`RaftKvNode::linearizable_get_served`]. Every
    /// client-facing get MUST use this (never the collapsed
    /// `RaftKvNode::linearizable_get`, whose single `None` would report a
    /// read-barrier failure as a definitive "key absent" — the ADR 0033
    /// read-path fix; this crate deliberately has no wrapper for the
    /// collapsed variant so the unsafe shape can't be reached here).
    async fn linearizable_get_served(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        match self {
            CpGroup::Lsm(n) => n.linearizable_get_served(key).await,
            CpGroup::Mem(n) => n.linearizable_get_served(key).await,
        }
    }

    /// Read `key` from this node's **local** engine — *not* linearizable (no
    /// ReadIndex barrier). See [`RaftKvNode::local_get`]. Used only to confirm a
    /// write **we proposed on this leader** has committed+applied (the leader
    /// applies only after a quorum commit + WAL fsync, so a local read reflecting
    /// our value means it is durable) — cheap enough to do under heavy concurrent
    /// load, where a per-write quorum barrier would not scale.
    async fn local_get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self {
            CpGroup::Lsm(n) => n.local_get(key).await,
            CpGroup::Mem(n) => n.local_get(key).await,
        }
    }

    /// Linearizable ReadIndex range scan. See [`RaftKvNode::linearizable_scan`].
    async fn linearizable_scan(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
        match self {
            CpGroup::Lsm(n) => n.linearizable_scan(start, end, limit).await,
            CpGroup::Mem(n) => n.linearizable_scan(start, end, limit).await,
        }
    }

    /// Descending ReadIndex range scan. See [`RaftKvNode::linearizable_scan_rev`].
    async fn linearizable_scan_rev(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
        match self {
            CpGroup::Lsm(n) => n.linearizable_scan_rev(start, end, limit).await,
            CpGroup::Mem(n) => n.linearizable_scan_rev(start, end, limit).await,
        }
    }

    /// Whether this node currently believes it leads the group.
    fn is_leader(&self) -> bool {
        match self {
            CpGroup::Lsm(n) => n.is_leader(),
            CpGroup::Mem(n) => n.is_leader(),
        }
    }

    /// Explicitly wake this group's consensus loop for one extra pass (ADR
    /// 0044 phase-1 PR4) — see [`RaftKvNode::wake`]. Idempotent and safe on
    /// every state.
    fn wake(&self) {
        match self {
            CpGroup::Lsm(n) => n.wake(),
            CpGroup::Mem(n) => n.wake(),
        }
    }

    /// ADR 0044 phase-1 PR5, fork D: hold or release this group's external
    /// quiesce veto. `fresh_through` is a freshness contract (issue #302
    /// fix) — see [`RaftKvNode::set_quiesce_veto`]'s doc before passing
    /// anything other than an `engine_applied_index()` read strictly
    /// *before* the observation that decided `held`.
    fn set_quiesce_veto(&self, held: bool, fresh_through: u64) {
        match self {
            CpGroup::Lsm(n) => n.set_quiesce_veto(held, fresh_through),
            CpGroup::Mem(n) => n.set_quiesce_veto(held, fresh_through),
        }
    }

    /// Whether this replica currently considers its own group quiesced (ADR
    /// 0044 phase-1) — the sweeper-skip gate every per-node background loop
    /// checks first (ADR 0044 phase-1 PR6). See [`RaftKvNode::is_quiesced`].
    fn is_quiesced(&self) -> bool {
        match self {
            CpGroup::Lsm(n) => n.is_quiesced(),
            CpGroup::Mem(n) => n.is_quiesced(),
        }
    }

    /// Whether this group has applied its split-cutover freeze (ADR 0050
    /// rung 5) — a pure flag read, never a wake. Consulted by every local
    /// write/txn propose helper before proposing; see
    /// [`RaftKvNode::is_frozen`] and [`frozen_refusal`].
    pub(crate) fn is_frozen(&self) -> bool {
        match self {
            CpGroup::Lsm(n) => n.is_frozen(),
            CpGroup::Mem(n) => n.is_frozen(),
        }
    }

    /// This group's pending (or already-applied) in-place split fork, if
    /// any (ADR 0058 Train 2 rung 3) — the `animusd`-level in-place cutover
    /// driver's (`index_drain.rs::inplace_split_driver_tick`) own signal
    /// that the CP data plane's own fork (`KvCommand::SplitTablet`) has
    /// completed and both children now exist locally, fully formed. See
    /// [`RaftKvNode::pending_split`].
    pub(crate) async fn pending_split(&self) -> Option<animus_cp_data::PendingSplit> {
        match self {
            CpGroup::Lsm(n) => n.pending_split().await,
            CpGroup::Mem(n) => n.pending_split().await,
        }
    }

    /// This replica's Raft commit index — the rung-5 endgame's
    /// apply-catch-up floor read. See [`RaftKvNode::commit_index`].
    pub(crate) fn commit_index(&self) -> u64 {
        match self {
            CpGroup::Lsm(n) => n.commit_index(),
            CpGroup::Mem(n) => n.commit_index(),
        }
    }

    /// Propose the split-cutover freeze on this (parent) group (ADR 0050
    /// rung 5). See [`RaftKvNode::propose_freeze`].
    pub(crate) fn propose_freeze(&self) -> animus_control::ProposeResult {
        match self {
            CpGroup::Lsm(n) => n.propose_freeze(),
            CpGroup::Mem(n) => n.propose_freeze(),
        }
    }

    /// This replica's `engine_applied_index()` — the confirm-by-index
    /// primitive linearizable reads themselves gate on. See
    /// [`RaftKvNode::engine_applied_index`]. Used by the backfill seeder
    /// (`index_drain.rs`) to confirm a change-log-only `KindBatch` (no base/
    /// kind write to probe a value on, unlike every other confirm path in
    /// this file) actually landed, without needing to know the entry's
    /// leader-minted `ts` up front.
    pub(crate) fn engine_applied_index(&self) -> u64 {
        match self {
            CpGroup::Lsm(n) => n.engine_applied_index(),
            CpGroup::Mem(n) => n.engine_applied_index(),
        }
    }

    /// What the `KindBatch` at `index` did, paired with the entry's own
    /// term. See [`RaftKvNode::kind_batch_outcome`].
    pub(crate) fn kind_batch_outcome(&self, index: u64) -> Option<(u64, KindBatchOutcome)> {
        match self {
            CpGroup::Lsm(n) => n.kind_batch_outcome(index),
            CpGroup::Mem(n) => n.kind_batch_outcome(index),
        }
    }

    /// Propose a split-build seed chunk into this (child) group's own log
    /// (ADR 0050 Train B rung 4). See [`RaftKvNode::propose_seed_batch`].
    pub(crate) fn propose_seed_batch(
        &self,
        rows: Vec<animus_cp_data::SeedRow>,
    ) -> animus_control::ProposeResult {
        match self {
            CpGroup::Lsm(n) => n.propose_seed_batch(rows),
            CpGroup::Mem(n) => n.propose_seed_batch(rows),
        }
    }

    /// The split-build driver's raw row read — one kind scope's rows with
    /// tombstones and versions, optionally bounded to a logical range. See
    /// [`RaftKvNode::seed_rows_kind`].
    pub(crate) async fn seed_rows_kind(
        &self,
        kind_idx: usize,
        logical_range: Option<(&[u8], &[u8])>,
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>, u64)> {
        match self {
            CpGroup::Lsm(n) => n.seed_rows_kind(kind_idx, logical_range).await,
            CpGroup::Mem(n) => n.seed_rows_kind(kind_idx, logical_range).await,
        }
    }

    /// The group's current leader id as this node sees it (for cross-process
    /// routing). See [`RaftKvNode::leader`].
    fn leader(&self) -> Option<NodeId> {
        match self {
            CpGroup::Lsm(n) => n.leader(),
            CpGroup::Mem(n) => n.leader(),
        }
    }

    /// Ask the group's driver loop to exit (drop-table GC, ADR 0024). See
    /// [`RaftKvNode::shutdown`]; poll [`is_stopped`](Self::is_stopped) for the
    /// actual exit before touching the group's on-disk artifacts.
    fn shutdown(&self) {
        match self {
            CpGroup::Lsm(n) => n.shutdown(),
            CpGroup::Mem(n) => n.shutdown(),
        }
    }

    /// Whether the driver loop has exited after [`shutdown`](Self::shutdown).
    fn is_stopped(&self) -> bool {
        match self {
            CpGroup::Lsm(n) => n.is_stopped(),
            CpGroup::Mem(n) => n.is_stopped(),
        }
    }

    /// Whether [`shutdown`](Self::shutdown) has latched this group's
    /// `halted` flag — the durability-assert tolerance gate `persist_wal`/
    /// `flush_pending` check (`animus-cp-data`'s `CLAUDE.md`), distinct from
    /// [`is_stopped`](Self::is_stopped) (whether the driver has actually
    /// exited yet). See [`RaftKvNode::is_halted`].
    #[cfg(test)]
    fn is_halted(&self) -> bool {
        match self {
            CpGroup::Lsm(n) => n.is_halted(),
            CpGroup::Mem(n) => n.is_halted(),
        }
    }

    /// The node's `raftkv` env this group runs on. Since ADR 0026 Stage B every
    /// tablet a node hosts shares this **same** env (stream-addressed, not a
    /// distinct per-tablet id/env) — used to identify *this node's* handle in
    /// the shared edge registry (`node_id()`). Per-tablet files are located by
    /// the engine factory's own `db-t{t}-` naming (ADR 0050 rung 1), not by
    /// env identity.
    fn env(&self) -> &ProdEnv {
        match self {
            CpGroup::Lsm(n) => n.env(),
            CpGroup::Mem(n) => n.env(),
        }
    }

    /// This node's live `(key, value)` pairs for the group, in key order, from the
    /// **local** engine (no quorum barrier). Meaningful on the leader (its committed
    /// state); the auto-split loop materializes it to confirm an over-threshold
    /// tablet + pick a median split key (Phase 2.4) — gated behind
    /// [`approx_key_count`](Self::approx_key_count), since this reads the whole
    /// tablet. See [`RaftKvNode::range_snapshot`].
    async fn local_pairs(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        match self {
            CpGroup::Lsm(n) => n.local_scan(&[], None, None).await,
            CpGroup::Mem(n) => n.local_scan(&[], None, None).await,
        }
    }

    /// A cheap, non-materializing key-count **(over-)estimate** for the auto-split
    /// gate (Phase 2.4): the memtable's key count (exact for data still in the
    /// memtable — the common case for a not-yet-split tablet) plus the SSTable
    /// bytes over a deliberately small assumed entry size, so the estimate errs
    /// toward *over*-counting — a tablet that might need splitting gets confirmed
    /// by a real count rather than silently missed. `None` on the memory backend
    /// (no cheap counter); the caller falls back to its slow confirm cadence.
    fn approx_key_count(&self) -> Option<usize> {
        let (memtable_keys, _bytes) = self.lsm_memtable()?;
        let sst_bytes: u64 = self.lsm_sstables()?.iter().map(|v| v.file_size).sum();
        Some(
            memtable_keys
                + usize::try_from(sst_bytes / AUTO_SPLIT_EST_ENTRY_BYTES).unwrap_or(usize::MAX),
        )
    }

    /// A cheap, non-materializing **byte** estimate for the byte-based
    /// auto-split gate (ADR 0034) — this tablet's own scoped bytes
    /// (`RaftKvNode::approx_bytes`, over its live `StorageScope`), on
    /// **either** backend (unlike [`approx_key_count`](Self::approx_key_count),
    /// which is LSM-only and returns `None` on the memory backend). See
    /// `RaftKvNode::approx_bytes`'s doc for the estimator + its bias
    /// direction; the auto-split loop's materializing confirm step
    /// (`local_pairs`) corrects it before a split actually commits.
    async fn approx_bytes(&self) -> u64 {
        match self {
            CpGroup::Lsm(n) => n.approx_bytes().await,
            CpGroup::Mem(n) => n.approx_bytes().await,
        }
    }

    /// [`approx_bytes`](Self::approx_bytes)'s kind-scoped sibling
    /// (`RaftKvNode::approx_bytes_kind`) — the seal arm's own size-trigger
    /// input, `KIND_CHANGE`'s bytes specifically, never the base row bytes
    /// `approx_bytes` measures.
    pub(crate) async fn approx_bytes_kind(&self, kind: u8) -> u64 {
        match self {
            CpGroup::Lsm(n) => n.approx_bytes_kind(kind).await,
            CpGroup::Mem(n) => n.approx_bytes_kind(kind).await,
        }
    }

    /// The first `limit` live `(key, value)` pairs with `key >= start`, in key
    /// order, from the **local** engine — the admin "browse keys" view (ADR 0021).
    /// Node-local introspection like the other `/admin/storage/*` routes, so it
    /// reads this replica's engine directly rather than via a quorum scan. Reuses
    /// `range_snapshot` and truncates — fine for a debug surface on dev-sized
    /// tablets (it materializes the live range from `start` before truncating).
    async fn local_scan(&self, start: &[u8], limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut pairs = match self {
            CpGroup::Lsm(n) => n.local_scan(start, None, None).await,
            CpGroup::Mem(n) => n.local_scan(start, None, None).await,
        };
        pairs.truncate(limit);
        pairs
    }

    // ---- admin / debug introspection (ADR 0020) -------------------------

    /// Which storage engine backs this group (`"lsm"` durable / `"memory"`).
    fn backend_name(&self) -> &'static str {
        match self {
            CpGroup::Lsm(_) => "lsm",
            CpGroup::Mem(_) => "memory",
        }
    }

    /// This group's Raft state for the `/admin/raftkv` view. The two engine arms
    /// call the identical `RaftKvNode` accessors, so a local macro keeps it DRY.
    ///
    /// **`key_count`/`byte_size` are the cheap
    /// [`approx_key_count`](Self::approx_key_count) /
    /// [`approx_bytes`](Self::approx_bytes) estimates unless `exact` is set**
    /// (`GET /admin/raftkv?exact=1`), in which case they are this tablet's
    /// own exact count/total from [`local_pairs`](Self::local_pairs).
    ///
    /// The default flipped to the estimates because this route is **polled**,
    /// not merely browsed: the Console fetches it from every node every 5s by
    /// default, and materializing every hosted tablet's rows per request costs
    /// O(dataset) per node per poll. Measured on a 20,000-row table mid-split,
    /// that polling inflated the split's own build ~9× (41.8s vs 4.5s) — an
    /// observer that materially perturbs what it observes. The estimates are
    /// what `auto_split_loop` itself gates on, so the Console's
    /// over-threshold pills now agree with the trigger that will actually
    /// fire, and `?exact=1` still answers precisely for one deliberate look.
    ///
    /// Two honest differences in the default, both documented on
    /// `admin::CpRaftView`: `approx_key_count` is `None` on the memory
    /// backend (no cheap counter — the field renders as "—"), and
    /// `approx_bytes` is **base-scoped** (ADR 0034) where the exact sum
    /// covers every kind in the tablet's engine.
    async fn raft_view(&self, tablet: TabletId, exact: bool) -> admin::CpRaftView {
        // Since ADR 0026 Stage B / ADR 0028 a tablet's CP group member id **is**
        // simply the base `raftkv` id — no more derived-id translation needed.
        let node = self.env().node_id();
        let (key_count, byte_size) = if exact {
            let pairs = self.local_pairs().await;
            (
                Some(pairs.len()),
                Some(
                    pairs
                        .iter()
                        .map(|(k, v)| (k.len() + v.len()) as u64)
                        .sum::<u64>(),
                ),
            )
        } else {
            (self.approx_key_count(), Some(self.approx_bytes().await))
        };
        macro_rules! view {
            ($n:expr) => {
                admin::CpRaftView {
                    tablet: tablet.0,
                    node,
                    backend: self.backend_name(),
                    role: format!("{:?}", $n.role()),
                    is_leader: $n.is_leader(),
                    leader: $n.leader(),
                    term: $n.term(),
                    commit_index: $n.commit_index(),
                    last_applied: $n.last_applied(),
                    durable_index: $n.durable_index(),
                    snapshot_index: $n.snapshot_index(),
                    log_len: $n.log_len(),
                    voters: $n.config().into_iter().collect(),
                    learners: $n.learners().into_iter().collect(),
                    key_count,
                    byte_size,
                    quiesced: $n.is_quiesced(),
                    // Overlaid by `admin::raftkv_view` from the data role's
                    // split-build mirror (ADR 0050 rung 4); this constructor
                    // has no `ClientCtx` to read it from.
                    split_rows_shipped: None,
                    split_converged: None,
                    split_phase: None,
                }
            };
        }
        match self {
            CpGroup::Lsm(n) => view!(n),
            CpGroup::Mem(n) => view!(n),
        }
    }

    /// This group's transaction-tracker view for `/admin/txns` (ADR 0018 §2/
    /// PR7): `pending_txns()`/`unresolved_decided()` (cheap lock-and-clone, no
    /// barrier — see `TxnTracker`'s doc in `animus-cp-data`) plus, for each
    /// pending record, a best-effort `txn_record_view` (a real ReadIndex
    /// round trip) for its `intent_spans` — acceptable since a tablet
    /// anchors only a handful of pending transactions at once. `age_ms`/
    /// `past_grace` are computed against this node's own clock at request
    /// time (`env().now()`), mirroring `ClientCtx::txn_recover`'s own
    /// `now_ms` derivation.
    async fn txn_view(&self, tablet: TabletId) -> admin::CpTxnView {
        let node = self.env().node_id();
        let now_ms = self.env().now().0 / 1_000_000;

        macro_rules! pending_and_unresolved {
            ($n:expr) => {
                ($n.pending_txns(), $n.unresolved_decided())
            };
        }
        let (pending, unresolved_decided) = match self {
            CpGroup::Lsm(n) => pending_and_unresolved!(n),
            CpGroup::Mem(n) => pending_and_unresolved!(n),
        };

        let mut pending_views = Vec::with_capacity(pending.len());
        for (txn_id, (record_key, created_ts)) in pending {
            let view: Option<TxnRecordView> = match self {
                CpGroup::Lsm(n) => n.txn_record_view(&record_key).await,
                CpGroup::Mem(n) => n.txn_record_view(&record_key).await,
            };
            let intent_spans = view.map(|v| {
                v.intent_spans
                    .iter()
                    .map(|(table, span)| {
                        let end = span
                            .end
                            .as_deref()
                            .map(admin::key_display)
                            .unwrap_or_else(|| "..".to_owned());
                        format!("{table}: {}..{end}", admin::key_display(&span.start))
                    })
                    .collect()
            });
            let age_ms = now_ms.saturating_sub(created_ts.wall_ms);
            pending_views.push(admin::PendingTxnView {
                txn_id: format!("{txn_id:?}"),
                record_key: admin::key_display(&record_key),
                created_wall_ms: created_ts.wall_ms,
                age_ms,
                past_grace: age_ms >= animus_cp_data::RECOVERY_GRACE.as_millis() as u64,
                intent_spans,
            });
        }

        let unresolved_views = unresolved_decided
            .into_iter()
            .map(|(txn_id, (record_key, outcome))| admin::UnresolvedTxnView {
                txn_id: format!("{txn_id:?}"),
                record_key: admin::key_display(&record_key),
                outcome: format!("{outcome:?}"),
            })
            .collect();

        admin::CpTxnView {
            tablet: tablet.0,
            node,
            pending: pending_views,
            unresolved_decided: unresolved_views,
        }
    }

    /// Live SSTable views, or `None` on the volatile memory backend (no SSTables).
    fn lsm_sstables(&self) -> Option<Vec<SsTableView>> {
        match self {
            CpGroup::Lsm(n) => Some(n.storage().sstable_views()),
            CpGroup::Mem(_) => None,
        }
    }

    /// `(memtable key count, approx bytes)`, or `None` on the memory backend.
    fn lsm_memtable(&self) -> Option<(usize, usize)> {
        match self {
            CpGroup::Lsm(n) => Some((n.storage().memtable_len(), n.storage().memtable_bytes())),
            CpGroup::Mem(_) => None,
        }
    }

    /// Live WAL segments + byte sizes, or `None` on the memory backend.
    async fn wal_segment_sizes(&self) -> Option<Vec<(u64, u64)>> {
        match self {
            CpGroup::Lsm(n) => Some(n.storage().wal_segment_sizes().await),
            CpGroup::Mem(_) => None,
        }
    }

    /// `(durable_seq, rotation_count)`, or `None` on the memory backend.
    fn wal_stats(&self) -> Option<(u64, u64)> {
        match self {
            CpGroup::Lsm(n) => Some((
                n.storage().wal_durable_seq(),
                n.storage().wal_rotation_count(),
            )),
            CpGroup::Mem(_) => None,
        }
    }

    /// Decoded records of WAL segment `seg`, or `None` on the memory backend.
    async fn wal_records(&self, seg: u64) -> Option<Vec<WalRecordView>> {
        match self {
            CpGroup::Lsm(n) => Some(n.storage().wal_segment_records(seg).await),
            CpGroup::Mem(_) => None,
        }
    }

    /// Every on-disk `(version, is_tombstone)` for `key`, or `None` on memory.
    async fn disk_versions(&self, key: &[u8]) -> Option<Vec<(u64, bool)>> {
        match self {
            CpGroup::Lsm(n) => Some(n.storage().test_disk_versions_of(key).await),
            CpGroup::Mem(_) => None,
        }
    }

    /// **Admin action:** force a flush+compaction (LSM only); `None` on memory.
    async fn flush_now(&self) -> Option<Result<(), String>> {
        match self {
            CpGroup::Lsm(n) => Some(n.storage().flush_now().await.map_err(|e| e.to_string())),
            CpGroup::Mem(_) => None,
        }
    }

    /// **Admin action:** force a compaction pass (LSM only); `None` on memory.
    async fn compact_now(&self) -> Option<Result<(), String>> {
        match self {
            CpGroup::Lsm(n) => Some(n.storage().compact_now().await.map_err(|e| e.to_string())),
            CpGroup::Mem(_) => None,
        }
    }

    /// **Admin action:** take one single-server reconfigure step toward `desired`
    /// (the `change_membership` contract), returning the voter set it proposed, or
    /// `None` if no step is needed / this node isn't the leader. `down` is this
    /// tablet's currently-`Down` members — see [`RaftKvNode::reconfigure_step`]
    /// (ADR 0029) for the priority order it drives.
    fn reconfigure_step(
        &self,
        desired: &BTreeSet<NodeId>,
        down: &BTreeSet<NodeId>,
    ) -> Option<BTreeSet<NodeId>> {
        match self {
            CpGroup::Lsm(n) => n.reconfigure_step(desired, down),
            CpGroup::Mem(n) => n.reconfigure_step(desired, down),
        }
    }

    /// This group's own current `StorageScope` range (ADR 0028 write-fence
    /// wiring): the pre-propose fence check + fence-to-stamp source for
    /// [`ClientCtx::cp_put_local`]/[`cp_delete_local`]/[`cp_batch_propose`].
    /// See [`RaftKvNode::scope_range`].
    fn scope_range(&self) -> KeyRange {
        match self {
            CpGroup::Lsm(n) => n.scope_range(),
            CpGroup::Mem(n) => n.scope_range(),
        }
    }

    // ---- multi-participant transactions (ADR 0018 §2/PR4) ----------------

    /// **Anchor stage.** See [`RaftKvNode::txn_stage_anchor`] — this
    /// wrapper always calls it directly (never the single-participant
    /// `txn_stage` convenience) so `participant_spans` (ADR 0018 §2/PR5,
    /// task #18 fix) actually reaches the freshly-created record's
    /// `intent_spans`. `conditions` is ADR 0018 §2's apply-time write-key
    /// conditions amendment.
    async fn txn_stage(
        &self,
        table: &str,
        writes: Vec<TxnWrite>,
        participant_spans: Vec<(String, KeyRange)>,
        conditions: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    ) -> Option<(TxnId, Vec<u8>, StageOutcome)> {
        match self {
            CpGroup::Lsm(n) => {
                n.txn_stage_anchor(table, writes, participant_spans, conditions)
                    .await
            }
            CpGroup::Mem(n) => {
                n.txn_stage_anchor(table, writes, participant_spans, conditions)
                    .await
            }
        }
    }

    /// **Participant stage.** See [`RaftKvNode::txn_stage_participant`].
    async fn txn_stage_participant(
        &self,
        txn_id: TxnId,
        record_key: Vec<u8>,
        record_table: String,
        writes: Vec<TxnWrite>,
        conditions: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    ) -> Option<(HlcTimestamp, StageOutcome)> {
        match self {
            CpGroup::Lsm(n) => {
                n.txn_stage_participant(txn_id, record_key, record_table, writes, conditions)
                    .await
            }
            CpGroup::Mem(n) => {
                n.txn_stage_participant(txn_id, record_key, record_table, writes, conditions)
                    .await
            }
        }
    }

    /// **Anchor commit** at (at least) `min_ts`. See
    /// [`RaftKvNode::txn_commit_at_least`].
    async fn txn_commit_at_least(
        &self,
        txn_id: TxnId,
        record_key: Vec<u8>,
        min_ts: HlcTimestamp,
    ) -> Option<HlcTimestamp> {
        match self {
            CpGroup::Lsm(n) => n.txn_commit_at_least(txn_id, record_key, min_ts).await,
            CpGroup::Mem(n) => n.txn_commit_at_least(txn_id, record_key, min_ts).await,
        }
    }

    /// **Resolve** intents on this group given an already-decided outcome.
    /// See [`RaftKvNode::txn_resolve`].
    async fn txn_resolve(
        &self,
        txn_id: TxnId,
        record_key: Vec<u8>,
        keys: Vec<Vec<u8>>,
        outcome: TxnOutcome,
    ) -> Option<HlcTimestamp> {
        match self {
            CpGroup::Lsm(n) => n.txn_resolve(txn_id, record_key, keys, outcome).await,
            CpGroup::Mem(n) => n.txn_resolve(txn_id, record_key, keys, outcome).await,
        }
    }

    /// **Status query** against this group's own record. See
    /// [`RaftKvNode::txn_status_local`].
    async fn txn_status_local(&self, record_key: &[u8]) -> Option<TxnDecisionStatus> {
        match self {
            CpGroup::Lsm(n) => n.txn_status_local(record_key).await,
            CpGroup::Mem(n) => n.txn_status_local(record_key).await,
        }
    }

    /// **Non-blocking, single-attempt linearizable read.** See
    /// [`RaftKvNode::linearizable_get_served_fast`].
    async fn linearizable_get_served_fast(&self, key: &[u8]) -> Option<FastRead> {
        match self {
            CpGroup::Lsm(n) => n.linearizable_get_served_fast(key).await,
            CpGroup::Mem(n) => n.linearizable_get_served_fast(key).await,
        }
    }

    /// **Resolve an intent given an externally-determined status.** See
    /// [`RaftKvNode::resolve_intent_given_status`].
    async fn resolve_intent_given_status(
        &self,
        key: &[u8],
        txn_id: &TxnId,
        status: TxnDecisionStatus,
    ) -> Option<Option<Vec<u8>>> {
        match self {
            CpGroup::Lsm(n) => {
                n.resolve_intent_given_status(key, None, txn_id, status)
                    .await
            }
            CpGroup::Mem(n) => {
                n.resolve_intent_given_status(key, None, txn_id, status)
                    .await
            }
        }
    }

    // ---- in-doubt transaction recovery (ADR 0018 §2/PR5) ------------------

    /// **Abort-only** (no inline resolve). See [`RaftKvNode::txn_abort`].
    async fn txn_abort(&self, txn_id: TxnId, record_key: Vec<u8>) -> Option<HlcTimestamp> {
        match self {
            CpGroup::Lsm(n) => n.txn_abort(txn_id, record_key).await,
            CpGroup::Mem(n) => n.txn_abort(txn_id, record_key).await,
        }
    }

    /// **Abort an orphan intent with no record at all** (a fresh `Aborted`
    /// tombstone). See [`RaftKvNode::txn_abort_orphan`].
    async fn txn_abort_orphan(
        &self,
        txn_id: TxnId,
        record_key: Vec<u8>,
        created_ts: HlcTimestamp,
    ) -> Option<HlcTimestamp> {
        match self {
            CpGroup::Lsm(n) => n.txn_abort_orphan(txn_id, record_key, created_ts).await,
            CpGroup::Mem(n) => n.txn_abort_orphan(txn_id, record_key, created_ts).await,
        }
    }

    /// **Recovery view of a record** (status + intent_spans + created_ts).
    /// See [`RaftKvNode::txn_record_view`].
    async fn txn_record_view(&self, record_key: &[u8]) -> Option<animus_cp_data::TxnRecordView> {
        match self {
            CpGroup::Lsm(n) => n.txn_record_view(record_key).await,
            CpGroup::Mem(n) => n.txn_record_view(record_key).await,
        }
    }

    /// **Does this tablet still hold a live intent for `txn_id` over
    /// `span`?** See [`RaftKvNode::txn_verify_staged`].
    async fn txn_verify_staged(&self, span: &KeyRange, txn_id: &TxnId) -> Option<bool> {
        match self {
            CpGroup::Lsm(n) => n.txn_verify_staged(span, txn_id).await,
            CpGroup::Mem(n) => n.txn_verify_staged(span, txn_id).await,
        }
    }

    /// This group's currently-tracked `Pending` records. See
    /// [`RaftKvNode::pending_txns`].
    fn pending_txns(&self) -> BTreeMap<TxnId, (Vec<u8>, HlcTimestamp)> {
        match self {
            CpGroup::Lsm(n) => n.pending_txns(),
            CpGroup::Mem(n) => n.pending_txns(),
        }
    }

    /// This group's currently-tracked decided-but-unresolved records. See
    /// [`RaftKvNode::unresolved_decided`].
    fn unresolved_decided(&self) -> BTreeMap<TxnId, (Vec<u8>, TxnOutcome)> {
        match self {
            CpGroup::Lsm(n) => n.unresolved_decided(),
            CpGroup::Mem(n) => n.unresolved_decided(),
        }
    }

    /// This group's active Raft voter configuration, as **this node's** own
    /// durable log sees it. The safety anchor for release GC (ADR 0029): a
    /// removed node only stops being a voter here once it has adopted the config
    /// entry that excludes it — a replay-independent, node-local signal (unlike
    /// replicated `Metadata`, which a restarting node replays through historical
    /// states). See [`RaftKvNode::config`].
    fn config(&self) -> BTreeSet<NodeId> {
        match self {
            CpGroup::Lsm(n) => n.config(),
            CpGroup::Mem(n) => n.config(),
        }
    }
}

/// How a CP op originating on this node reaches the group leader
/// ([`ClientCtx::cp_route`]).
// Transient per-request value: created, matched once, dropped — never stored.
// Boxing `Local`'s `CpGroup` would put a heap allocation on the read/write hot
// path just to shrink a stack value that lives for one match.
#[allow(clippy::large_enum_variant)]
enum CpRoute {
    /// This node hosts the current leader — serve from `leader` directly.
    Local(CpGroup),
    /// Forward to the leader's node at this client-API address (ADR 0017 #3b).
    Forward(SocketAddr),
    /// No leader reachable (no local leader, no route, election did not settle).
    None,
}

/// Which consistency a CP read is asking for (ADR 0055) — this crate's
/// spelling of DynamoDB's own per-request `ConsistentRead` flag, threaded
/// from the wire edge down through [`ClientCtx::cp_read`]/
/// [`ClientCtx::cp_scan`]/[`ClientCtx::cp_scan_kind`] to the read primitive
/// that serves it.
///
/// The two are genuinely different reads, not two cost tiers of one read:
/// `Strong` is the ReadIndex path every read took before ADR 0055
/// (leader-only, quorum-confirmed, linearizable); `Eventual` is served from
/// **any** replica's own applied state with no barrier and no leader hop,
/// and may return an older — but genuinely committed — state of the tablet.
///
/// `Eventual` is only ever a *preference*: every read falls back to the
/// `Strong` path when no replica can serve it cheaply, so the weaker request
/// can never fail where the stronger one would have succeeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadConsistency {
    /// Linearizable: ReadIndex on the tablet's group leader (ADR 0017).
    Strong,
    /// Eventually consistent: any replica's applied state (ADR 0055).
    Eventual,
}

impl ReadConsistency {
    /// The DynamoDB `ConsistentRead` flag as this crate spells it. `false`
    /// — the wire default, and by far the common case — is `Eventual`.
    pub(crate) fn from_consistent_read(consistent: bool) -> Self {
        if consistent {
            Self::Strong
        } else {
            Self::Eventual
        }
    }

    /// Whether the cheap path should be tried first.
    fn is_eventual(self) -> bool {
        matches!(self, Self::Eventual)
    }
}

/// How a [`ClientCtx::poll_probe`] confirm wait ended: the probed effect
/// appeared (`Confirmed`), the wait became provably futile before the
/// deadline (`Superseded` — see [`ClientCtx::confirm_wait_is_futile`]), or
/// the deadline elapsed with the accepted entry still plausibly in flight
/// (`TimedOut`).
enum ProbeWait {
    Confirmed,
    Superseded,
    TimedOut,
}

/// What a `KindBatch` apply-time outcome, read alone (no value probe), proves
/// about the entry [`ProposeResult::Accepted`] named — the pure decision
/// [`ClientCtx::poll_probe`] makes at each poll. Factored out so the
/// index+term identity check (below) is directly unit-testable rather than
/// only reachable through a full multi-node truncation scenario.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KindBatchSignal {
    /// Provably **this proposer's own entry** applied and its effects are
    /// merged into the engine — safe to ack.
    Confirm,
    /// Provably **nothing** landed at this index (a no-op is a no-op
    /// regardless of whose entry it was — see [`KindBatchOutcome`]'s doc) —
    /// safe to give up and tell the caller to retry.
    NoOp,
    /// No conclusion from the outcome channel alone: not yet applied, aged
    /// out of the bounded map, applied but not yet readable, or — the
    /// identity check this exists for — `Applied` recorded under a
    /// **different** term than this proposer's own `accepted_term`. A term
    /// mismatch is not proof of failure either (the value probe still
    /// confirms if the reoccupying entry's content happens to be identical);
    /// it is simply not proof of *this* proposer's success. The caller falls
    /// through to the value probe either way.
    Inconclusive,
}

/// The identity check at the heart of the KindBatch outcome false-ack fix: a
/// recorded `Applied` outcome only proves *this* `accepted_index`/
/// `accepted_term` entry applied when its own recorded term matches —
/// otherwise a *different* command (one that reoccupied the index after a
/// leadership change truncated the original entry) is the one that actually
/// applied, and index alone cannot tell the two apart (see
/// [`ProposeResult::Accepted`]'s doc for the log-matching argument).
/// `ConditionFailed`/`Sealed` need no term check — see their own variant
/// docs on why a no-op is sound regardless of whose entry it was.
fn classify_kind_batch_outcome(
    outcome: Option<(u64, KindBatchOutcome)>,
    accepted_term: u64,
    effects_readable: bool,
) -> KindBatchSignal {
    match outcome {
        Some((term, KindBatchOutcome::Applied)) if term == accepted_term && effects_readable => {
            KindBatchSignal::Confirm
        }
        Some((_, KindBatchOutcome::ConditionFailed { .. } | KindBatchOutcome::Sealed { .. })) => {
            KindBatchSignal::NoOp
        }
        _ => KindBatchSignal::Inconclusive,
    }
}

#[cfg(test)]
mod kind_batch_signal_tests {
    use super::{KindBatchOutcome, KindBatchSignal, classify_kind_batch_outcome};

    const ACCEPTED_TERM: u64 = 7;

    /// The confirm this whole channel exists to grant: my own entry, same
    /// term, effects merged and readable.
    #[test]
    fn same_term_applied_and_readable_confirms() {
        assert_eq!(
            classify_kind_batch_outcome(
                Some((ACCEPTED_TERM, KindBatchOutcome::Applied)),
                ACCEPTED_TERM,
                true,
            ),
            KindBatchSignal::Confirm
        );
    }

    /// Recorded, but the apply task hasn't merged it into the readable
    /// engine state yet — must not confirm (the durable-before-visible
    /// rule), but it's still provably mine, so it isn't a `NoOp` either.
    #[test]
    fn same_term_applied_but_not_yet_readable_is_inconclusive() {
        assert_eq!(
            classify_kind_batch_outcome(
                Some((ACCEPTED_TERM, KindBatchOutcome::Applied)),
                ACCEPTED_TERM,
                false,
            ),
            KindBatchSignal::Inconclusive
        );
    }

    /// **The regression this suite exists for.** A truncated entry's index
    /// reoccupied by a different command, at a different term, that
    /// genuinely applied: index-alone would have called this `Confirm`
    /// (the bug found in review of PR #334) — the fix must call it
    /// `Inconclusive` (falls through to a value probe) instead, no matter
    /// how "ready" the engine looks.
    #[test]
    fn a_different_terms_applied_outcome_never_confirms() {
        let other_term = ACCEPTED_TERM + 1;
        assert_eq!(
            classify_kind_batch_outcome(
                Some((other_term, KindBatchOutcome::Applied)),
                ACCEPTED_TERM,
                true,
            ),
            KindBatchSignal::Inconclusive,
            "a term mismatch must never be treated as a confirm of the \
             original entry — this is the false-ack the fix closes"
        );
        // Also true for a lower term (a stale replay), not just a higher one.
        assert_eq!(
            classify_kind_batch_outcome(
                Some((ACCEPTED_TERM.saturating_sub(1), KindBatchOutcome::Applied)),
                ACCEPTED_TERM,
                true,
            ),
            KindBatchSignal::Inconclusive
        );
    }

    /// A no-op is a no-op regardless of whose entry occupies the index or
    /// what term it carries — no term check gates this branch.
    #[test]
    fn condition_failed_and_sealed_are_no_ops_at_any_term() {
        for term in [
            ACCEPTED_TERM,
            ACCEPTED_TERM + 1,
            ACCEPTED_TERM.saturating_sub(1),
        ] {
            assert_eq!(
                classify_kind_batch_outcome(
                    Some((
                        term,
                        KindBatchOutcome::ConditionFailed { key: b"k".to_vec() }
                    )),
                    ACCEPTED_TERM,
                    true,
                ),
                KindBatchSignal::NoOp,
                "ConditionFailed at term {term}"
            );
            assert_eq!(
                classify_kind_batch_outcome(
                    Some((term, KindBatchOutcome::Sealed { key: b"k".to_vec() })),
                    ACCEPTED_TERM,
                    true,
                ),
                KindBatchSignal::NoOp,
                "Sealed at term {term}"
            );
        }
    }

    /// No record at all (not yet applied, or aged out of the bounded map) —
    /// nothing to conclude either way.
    #[test]
    fn no_record_is_inconclusive() {
        assert_eq!(
            classify_kind_batch_outcome(None, ACCEPTED_TERM, true),
            KindBatchSignal::Inconclusive
        );
    }
}

/// The point-in-time outcome of
/// [`ClientCtx::cp_get_local_snapshot`]/[`ClientCtx::cp_read_snapshot`] (ADR
/// 0018 §2, the torn-pair-fix stack's PR2 amendment) — see those methods'
/// docs, and `dynamo::quiescent_multi_get`'s module-level rationale, for why
/// `TransactGetItems`'s quiescent-round reader needs a third outcome
/// alongside "resolved" and "routing failed."
pub(crate) enum SnapshotRead {
    /// The value is already resolved (present, or genuinely absent) — the
    /// identical shape [`ClientResponse::Value`] carries.
    Value(Option<Vec<u8>>),
    /// This key's covering transaction did not resolve within one single,
    /// point-in-time attempt (a local-`Pending` or `Foreign` intent, still
    /// `Pending` after one `confirm_or_push` attempt, or racing another
    /// resolver) — the round this read belongs to must be discarded, never
    /// fed into the two-round agreement check: the whole point of a
    /// quiescent round is that every key samples *the same instant*, which
    /// an unresolved key cannot promise. Only the caller's own ROUND-level
    /// retry may act on this, never a per-key wait.
    Unresolved,
}

/// How long a CP op (`cp_route` + forward) waits for the tablet's group to be
/// reachable before giving up. Generous because a table's group now forms **in
/// band** on the first access (ADR 0023) — the first op after a `CreateTable`/
/// first-write waits out the join-host + election, which under heavy load takes
/// longer than a steady-state op. No happy-path cost: `cp_route` returns as soon as
/// a leader is reachable; the cap only bounds the wait when the group is forming.
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

/// ADR 0055: the refusal a node returns for a **forwarded** eventual read it
/// cannot serve — it holds no serveable replica of the tablet, or the one it
/// holds does not cover the requested range.
///
/// Deliberately **not** a `"; retry"` error and deliberately not a
/// not-the-leader refusal: neither retrying here nor chasing a leader is the
/// right response. The forwarder's answer to this is to stop being cheap and
/// take the linearizable path, which always works — so this string is only
/// ever a fallback signal, never something a client sees.
const STALE_READ_REFUSAL: &str =
    "no replica here can serve an eventually-consistent read (ADR 0055)";

/// How long a **forwarded** eventually-consistent read (ADR 0055) waits on
/// its one-shot relay before giving up and falling back to the linearizable
/// path.
///
/// Deliberately far below [`CLIENT_TIMEOUT`], and deliberately not shared
/// with it: a cheap read that sits ten seconds on an unresponsive replica
/// has already lost every property it was chosen for. Failing fast into the
/// strong path costs one leader hop and always answers; waiting does not.
const STALE_READ_FORWARD_TIMEOUT: Duration = Duration::from_secs(2);

/// ADR 0050 rung 5: the retryable refusal every mutating propose helper
/// returns for a frozen split parent (post-`Freeze`, pre-cutover). Ends in
/// `"; retry"` (the house retryability convention) so every existing client
/// retry loop re-resolves routing; distinct wording so tests/admin can tell
/// frozen from a fence/stale-routing refusal.
const FROZEN_REFUSAL: &str =
    "tablet frozen for split cutover (ADR 0050); a child will serve this range shortly; retry";
/// How long [`ClientCtx::cp_forward`] backs off between retry passes when every
/// candidate replica refused a forwarded op with `leader_hint=none` — i.e. the
/// tablet's group has no elected leader *yet* (a split-child/first-provision
/// formation window, or a crashed leader mid-election). Roughly one election
/// timeout: long enough that a couple of passes span a real election, short
/// enough that the total wait stays a small fraction of [`CLIENT_TIMEOUT`]
/// (which still hard-bounds the whole sequence).
const FORWARD_ELECTION_BACKOFF: Duration = Duration::from_millis(100);
/// Bounded attempts [`ClientCtx::txn_prepare_pushing`] gives a stage blocked
/// by another transaction's unresolved intent (ADR 0018 §2/PR6, task #16)
/// before giving up and reporting a client-facing conflict error.
const TXN_STAGE_PUSH_ATTEMPTS: u32 = 3;
/// Backoff between [`ClientCtx::txn_prepare_pushing`]'s retry attempts —
/// room for the blocking transaction to clear (its own coordinator
/// finishing, or `txn_resolver_loop`'s passive sweep once past
/// `animus_cp_data::RECOVERY_GRACE`), not a hard liveness bound.
const TXN_STAGE_PUSH_BACKOFF: Duration = Duration::from_millis(250);
/// ADR 0046 D1: how long [`ClientCtx::cp_txn`] awaits `resolve_all` before
/// acking anyway, for a transaction that touches at least one kind-write-path
/// table (a plain transaction keeps the original fire-and-forget spawn,
/// unaffected). A timeout here never denies the commit — it only means the
/// LSI/GSI/stream materialization the client's own immediate follow-up read
/// might race is left for `txn_resolver_loop`'s passive sweep, exactly as a
/// plain transaction's async resolve always could race a follow-up read on
/// its own participant tables.
const TXN_RESOLVE_ALL_AWAIT_BUDGET: Duration = Duration::from_secs(2);
/// The bootstrap CP group's replication factor (ADR 0017 #3a): the group spans the
/// first `min(N, MAX_REPLICATION_FACTOR)` nodes' `raftkv` ids. Dynamic CP placement
/// over more nodes is later v1 work.
const MAX_REPLICATION_FACTOR: usize = 3;
/// Filename prefix namespacing the node's **one shared** on-disk LSM under its
/// `raftkv` `ProdEnv` directory (its files become `db-MANIFEST`/`db-wal`/
/// `db-sst-*`). Every tablet this node hosts shares this **same** engine —
/// opened once, cloned into every tablet group's [`RaftKvNode`] — confined
/// from each other by a [`StorageScope`] (table-id key prefix + tablet range),
/// not by separate files. The prefix is a flat filename prefix, **not** a
/// subdirectory (no `/`): `ProdEnv`'s disk opens files directly under the
/// role's data dir and does not create intermediate directories. `pub` for
/// the same reason [`SYSKV_LSM_PREFIX`] is (ADR 0038 PR4): an integration
/// test can reopen a combined node's shared engine directly (over a fresh
/// `ProdEnv` bound to the same `raftkv` directory) to verify its
/// control-plane system-keyspace contents survive a restart independent of
/// any node's own in-memory state, mirroring the control-only-node check
/// `SYSKV_LSM_PREFIX` already backs.
pub const LSM_PREFIX: &str = "db-";

/// Filename prefix for a **control-only** node's dedicated ADR 0038 PR2
/// system-keyspace mirror engine, opened on the same `control` `ProdEnv`
/// directory the control Raft's own `raft.wal` already lives in (a
/// control-only node has no separate `raftkv` env/dir the way a combined
/// node does) — distinct from [`LSM_PREFIX`] and from the fixed `raft.wal`
/// filename, so the two never collide on one directory. `pub` so an
/// integration test can reopen the same on-disk engine directly (by
/// constructing its own `ProdEnv` over the same directory) to verify it
/// survives a real process restart, without `animusd` needing to expose the
/// live mirror handle itself.
pub const SYSKV_LSM_PREFIX: &str = "syskv-";

/// Which storage engine backs a node's CP group.
///
/// The default, [`StorageBackend::Lsm`], is the durable on-disk
/// [`LsmEngine`] over the node's `raftkv` `ProdEnv` — data survives a process
/// restart. [`StorageBackend::Memory`] is the volatile [`MemoryEngine`], for
/// ephemeral/dev runs that intentionally start empty each time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StorageBackend {
    /// Durable on-disk LSM (default).
    #[default]
    Lsm,
    /// Volatile in-memory engine (ephemeral runs).
    Memory,
}

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
    /// **Internal split-build seed RPC — never sent bare, only wrapped in
    /// [`Forwarded`](Self::Forwarded)** (ADR 0050 Train B rung 4): propose
    /// one `KvCommand::SeedBatch` chunk — `(kind, logical key,
    /// value-or-tombstone, MVCC version)` rows — into `tablet`'s (a
    /// `Building` split child's) own Raft group, applied as
    /// version-carrying merges. The **one** production sender is the
    /// split-build driver (`index_drain::split_driver_tick` via
    /// `ClientCtx::seed_child_rows`), running on the *parent* tablet's
    /// leader node — a child's own leader can live anywhere (fork F5,
    /// placement-chosen homes), so this needs the identical one-hop
    /// forward/leader-resolution machinery every other CP op has.
    /// Addressed by `tablet` directly, mirroring
    /// [`ForceSeal`](Self::ForceSeal) (a `Building` child is deliberately
    /// unroutable by key). Bare delivery is refused for the same reason
    /// `KindWrite`'s is: an arbitrary caller must never install raw rows
    /// at arbitrary versions into a tablet's scopes. Not a `MetaCommand`,
    /// so `is_relayable_command` does not apply; real handling lives in
    /// `cp_serve_forwarded`'s match, reached only through the `Forwarded`
    /// arm.
    SeedRows {
        tablet: u64,
        rows: Vec<animus_cp_data::SeedRow>,
    },
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
    /// [`WATCH_METADATA_SERVER_TIMEOUT`] and reply once it advances past
    /// `last_seen` **or** the bound elapses (a normal, not-an-error outcome —
    /// the caller just retries with the same `last_seen`, exactly like a
    /// `Status` poll that happened not to see a change). Replaces the old
    /// fixed-interval `Status` poll both a data-only node's and (ADR 0035 PR5)
    /// an ADR 0030 growth node's mirror sync used, closing most of the
    /// latency gap between "control commits" and "the mirror observes it"
    /// without a new push mechanism. Only a
    /// genuine control-group replica (`ControlHandle::Local`) serves this —
    /// see [`ClientCtx::watch_metadata`]'s doc for why a `Remote` node
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
    /// the one caller.
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
        writes: Vec<TxnWrite>,
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
    /// `ClientCtx::txn_decide_anchor`, the one caller, for the full
    /// rationale (including why the reply now carries the record's
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
    /// `ClientCtx::txn_resolve_participant`, the one caller.
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
    /// caller (from `cp_get_local`'s foreign-intent path).
    TxnStatus { table: String, record_key: Vec<u8> },
    /// **Internal recovery RPC — never sent bare** (ADR 0018 §2/PR5): the
    /// recovery-view dual of [`TxnStatus`](Self::TxnStatus) — like it, but
    /// also returns `intent_spans`/`created_ts`, everything a recovery
    /// pusher needs (`RaftKvNode::txn_record_view`). See
    /// `ClientCtx::txn_record_view`, the one caller.
    TxnRecordView { table: String, record_key: Vec<u8> },
    /// **Internal recovery RPC — never sent bare** (ADR 0018 §2/PR5): does
    /// `table`'s tablet leader still hold a live intent for `txn_id`
    /// anywhere in `span` (`RaftKvNode::txn_verify_staged`)? A recovery
    /// pusher sends one of these per `(table, span)` entry in a record's
    /// `intent_spans` before deciding whether every participant staged.
    /// See `ClientCtx::txn_verify`, the one caller.
    TxnVerify {
        table: String,
        span: KeyRange,
        txn_id: TxnId,
    },
}

/// Which listener a connection came in on (ADR 0047). Kept as a distinct
/// type from [`Surface`] even though both are 2-variant enums over the same
/// two concepts — see that type's doc for why sharing one enum for both
/// would make the refusal rule look symmetric when it is not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListenerKind {
    /// The external, DynamoDB-adjacent client port.
    Client,
    /// The cluster-internal RPC port (ADR 0047) — the more-trusted network
    /// segment; the operator's Kubernetes topology keeps it off any
    /// externally-reachable Service.
    Intra,
}

/// Where a [`ClientRequest`] variant may be received **bare** — a
/// classification result, not a listener identity (see [`ListenerKind`]'s
/// doc for why these are two distinct types). [`surface_of`] is the one
/// exhaustive table computing this; [`handle_request`]'s one guard clause is
/// the one place it is consulted.
///
/// **`Intra` is a superset of `Public`, not a disjoint partition**: nothing
/// stops the intra listener from also serving a `Public`-surfaced request —
/// deliberately, since neither port has authentication yet at this
/// milestone, and intra is meant to be the more, not less, trusted segment.
/// A future reader should not "fix" this into a second refusal layer without
/// re-reading ADR 0047's rationale.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Surface {
    /// Reachable on either listener.
    Public,
    /// Reachable **only** on the intra listener — refused bare on `Client`.
    Intra,
}

/// The single source of truth for which listener(s) may receive a bare
/// [`ClientRequest`] variant (ADR 0047). A free function beside
/// [`request_kind`], same convention: **no wildcard arm**, so adding a
/// `ClientRequest` variant anywhere is a compile error here until it is
/// explicitly classified.
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
/// `ProposeSchema` relay envelope) or [`ClientCtx::cp_serve_forwarded`]'s own
/// match (whether real handling exists for a forwarded payload) — both stay
/// exactly as grep-dependent as before; unrelated axes.
fn surface_of(request: &ClientRequest) -> Surface {
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
        | ClientRequest::TriggerAutoSplit { .. }
        | ClientRequest::StreamHotRead { .. }
        | ClientRequest::ClearBackfillCursor { .. }
        | ClientRequest::SeedRows { .. }
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
/// rare operator action, and [`ClientCtx::admin_remove_member`] is
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
fn is_relayable_command(command: &MetaCommand) -> bool {
    matches!(
        command,
        MetaCommand::CreateTableSchema { .. }
            | MetaCommand::DropTableSchema { .. }
            // Atomic `ALTER TABLE` (in-place schema replacement): a follower-
            // connected ALTER must relay like the create/drop it replaces.
            | MetaCommand::ReplaceTableSchema { .. }
            | MetaCommand::CreateTableIndex { .. }
            | MetaCommand::DropTableIndex { .. }
            // Index status transition (ADR 0045): same schema-catalog class as
            // `CreateTableIndex`/`DropTableIndex` — the backfill
            // seeder/aggregator (this crate) may propose it from wherever the
            // relevant tablet/control leader actually runs.
            | MetaCommand::SetIndexStatus { .. }
            // Backfill-completion catalog commit (ADR 0045 §4): a tablet
            // leader's own "I finished seeding this index" proposal, from
            // wherever that leader actually runs — same relay reasoning as
            // `SealStreamShard` just below. `index_backfill_loop` (the
            // aggregator that reads this catalog and flips a table's index
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
            // own seal proposal, from wherever that leader actually runs —
            // see this function's own doc for why `ExpireStreamShards` is
            // deliberately NOT included here.
            | MetaCommand::SealStreamShard { .. }
            | MetaCommand::RegisterCpAddr { .. }
            // Node address book (ADR 0032 PR1): every node self-registers its
            // full address set at startup, from whichever node it happens to
            // connect to for control-plane proposals — must relay like
            // `RegisterCpAddr` (a follower-connected node has no other way to
            // reach the control leader).
            | MetaCommand::RegisterNodeAddrs { .. }
            // Copy-based split workflow (ADR 0050): `trigger_split` proposes
            // `BeginSplit` from whichever node's admin/auto-split surface
            // fired it, and the split driver (B4/B5) proposes `CutoverSplit`
            // from the parent's own leader node — both need the identical
            // follower-connected relay path `SplitTablet` already has.
            | MetaCommand::BeginSplit { .. }
            | MetaCommand::CutoverSplit { .. }
            // In-place split workflow (ADR 0058 Train 2 rung 3): the SAME
            // relay reasoning as the copy-based `BeginSplit`/`CutoverSplit`
            // pair above — `trigger_split` (in-place mode) proposes
            // `BeginSplitInPlace` from whichever node's admin/auto-split
            // surface fired it, and `CutoverSplit` here is proposed by the
            // parent's own leader node once its data-plane fork has
            // completed and its pre-cutover vetoes pass, exactly the same
            // follower-connected relay need.
            | MetaCommand::BeginSplitInPlace { .. }
            // Provision-at-create (ADR 0023): a `CreateTable` on a follower-connected
            // client relays the table's tablet creation + RF policy to the control
            // leader. Scoped to one tablet per table by the state machine's guard.
            | MetaCommand::CreateTablet { .. }
            | MetaCommand::SetTabletPolicy { .. }
            // Drop-table GC (ADR 0024): a `DROP TABLE` on a follower-connected
            // client relays the table's tablet removal to the control leader.
            | MetaCommand::DropTableTablets { .. }
            // Online growth (ADR 0030): admin add-member registers a new raftkv
            // id as `Down` — see the doc above for why this is safe to relay
            // unlike drain (which stays local-leader-only).
            | MetaCommand::UpsertMember {
                status: NodeStatus::Down,
                ..
            }
            // Registration CAS (ADR 0040 Decision C): a joining process has
            // no local control role at all yet (it hasn't even bound its
            // listeners), so relaying `RegisterNode` via `ProposeSchema` is
            // its *only* way to reach the real leader — exactly the
            // `Down`-registering `UpsertMember` case just above, and safe
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
            // follower-connected-seed case.
            | MetaCommand::RegisterNode { .. }
    )
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
    /// freshness proxy [`control_handle::RemoteControlClient::observe`] uses
    /// to reject a reply from a replica lagging behind one it already saw.
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
    /// `RemoteControlClient::control_voters`'s doc. `#[serde(default)]` so an
    /// older node's reply (predating this field) still parses, decoding to
    /// an empty set — indistinguishable on the wire from "this replica
    /// genuinely reported zero voters" for an old peer, but no worse than
    /// that field's total absence was before this PR, and every in-process
    /// consumer that cares about telling "unknown" apart from "empty" reads
    /// `ControlHandle::config()`'s `Option` directly rather than round-
    /// tripping through this wire copy.
    Status {
        metadata: Metadata,
        #[serde(default)]
        leader_hint: Option<(NodeId, SocketAddr)>,
        /// The intra-cluster dual of `leader_hint` (ADR 0047) — machine-
        /// relay-only, never surfaced to a human (see the root `CLAUDE.md`'s
        /// hint-field-conflation lesson). `#[serde(default)]`, same
        /// robustness pattern as `leader_hint`.
        #[serde(default)]
        intra_leader_hint: Option<(NodeId, SocketAddr)>,
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
    /// size input (see `dynamo::collection_bytes_at_leader`). It is produced
    /// *at the leader* because that is the only node that holds the tablet's
    /// engine; the receiving edge has no way to price a tablet it does not
    /// host, which is precisely why this rides back on the reply rather than
    /// being computed after the hop. `#[serde(default)]` so a peer predating
    /// the field still decodes, reporting no estimate rather than a wrong one.
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
    /// [`SnapshotRead::Unresolved`]'s doc. Distinguishable on the
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
    /// [`run_node_growth`]/[`run_node_join`]), the answering node's internal
    /// peer book (`AdminInfo.peers`), its live client-op routing table
    /// (`ClientCtx::route_snapshot`, kept fresh by `route_sync_loop`, ADR
    /// 0032 PR1), and every known admin address (the dashboard fan-out seed).
    JoinInfo {
        control_ids: Vec<NodeId>,
        peers: BTreeMap<NodeId, SocketAddr>,
        client_route: BTreeMap<NodeId, SocketAddr>,
        /// The answering node's live intra-cluster routing table (ADR 0047),
        /// paralleling `client_route` — the joining node seeds its own
        /// `ctx.intra_route` from this, load-bearing for the exact same
        /// reason `client_route` is: the growth-node-mirror branch inside
        /// `BoundNode::start_with_streams` resolves `ctx.intra_addr(id)`
        /// synchronously, before this node's own `intra_route_sync_loop` has
        /// had a chance to tick.
        intra_route: BTreeMap<NodeId, SocketAddr>,
        admin_addrs: Vec<SocketAddr>,
    },
    /// **Incremental long-poll reply to
    /// [`WatchMetadata`](ClientRequest::WatchMetadata)** (ADR 0038 PR5): the
    /// answering node's own [`animus_control::RaftNode::watch_delta_since`]
    /// covered `(last_seen, watermark]` contiguously in its bounded
    /// system-keyspace delta ring, so instead of a full [`Status`](Self::Status)
    /// clone this carries just the [`animus_control::mirror::KeyWrite`]s
    /// those commits produced, in commit order. The caller
    /// (`control_handle::RemoteControlClient::observe_delta`) installs them
    /// verbatim onto its own cached `Metadata` via
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
    /// the bounded window) — see [`ClientCtx::watch_metadata`]'s doc.
    /// `writes` is empty exactly when `watermark == last_seen` (the
    /// timeout-elapsed, nothing-changed case) — cheaper than a full
    /// `Metadata` clone even then.
    MetadataDelta {
        writes: Vec<animus_control::mirror::KeyWrite>,
        watermark: u64,
        leader_hint: Option<(NodeId, SocketAddr)>,
        /// The intra-cluster dual of `leader_hint` (ADR 0047) — see
        /// `Status`'s own field doc.
        #[serde(default)]
        intra_leader_hint: Option<(NodeId, SocketAddr)>,
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
    /// §2/PR5).
    TxnRecordViewReply {
        status: TxnDecisionStatus,
        intent_spans: Vec<(String, KeyRange)>,
        created_ts: HlcTimestamp,
    },
    /// Reply to [`TxnVerify`](ClientRequest::TxnVerify) (ADR 0018 §2/PR5):
    /// does the answering tablet still hold a live intent for the queried
    /// `txn_id` over the queried span?
    TxnVerifyReply { staged: bool },
}

/// Listen addresses for a node's endpoints (use port 0 for ephemeral): one
/// **internal** `ProdEnv` (ADR 0040 PR1 — one identity per node: the control
/// Raft rides stream 0, every per-tablet Raft group its own stream ≥ 1) + the
/// client API + the DynamoDB HTTP endpoint. v1 (ADR 0019) is
/// CP-only — the AP `data`/`coord` roles are gone.
///
/// **ADR 0035** adds [`role`](Self::role): a node declares whether it runs the
/// control role, the data role, or both (`Both`, the default — and, before
/// this ADR, the *only* shape). `internal` is required for every role (a
/// control-only node needs it for the control Raft; a data-only node needs it
/// for its per-tablet Raft groups **and** for heartbeating the control group,
/// ADR 0012) — only `dynamo` stays meaningfully role-gated in practice
/// (unused by a control-only node), and it stays a plain `SocketAddr` as
/// before. See `crate::config::NodeRole` for the role-derived `ClusterConfig`
/// helpers (`control_ids`/`data_ids`/`peer_book`) that key off this field.
///
/// **Clean break (ADR 0040)**: this merges the pre-existing `control`/
/// `raftkv` `Option<SocketAddr>` pair into this one required field — no
/// wire/config back-compat with a pre-ADR-0040 deployment (fresh clusters
/// required).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RoleAddrs {
    /// This node's self-minted/operator-proposed identity (ADR 0040 PR3):
    /// every config entry now carries its own id explicitly instead of it
    /// being purely derived from the entry's position in `nodes` — required
    /// (no default; a clean break, fresh clusters only), and validated
    /// unique across the whole config at load (`ClusterConfig::from_json`).
    pub id: NodeId,
    /// Which role(s) this node runs (ADR 0035). Defaults to
    /// [`Both`](config::NodeRole::Both) when absent — the shape every config
    /// used before this field existed.
    #[serde(default)]
    pub role: config::NodeRole,
    /// This node's one internal `ProdEnv` listen address (ADR 0040 PR1): the
    /// control-plane Raft (stream 0) and every per-tablet Raft group this
    /// node hosts (stream = tablet id ≥ 1, ADR 0026) share it. Required for
    /// every role.
    ///
    /// **Naming note (ADR 0047)**: `internal` is the raw `ProdEnv`/Raft-wire
    /// transport — not the same thing as [`intra`](Self::intra) below, one
    /// letter-swap away and a recurring source of confusion. `intra` is the
    /// **`ClientRequest`/`ClientResponse`-framed** node-to-node RPC port
    /// (same length-prefixed JSON framing as `client`, just a disjoint
    /// allowed-variant set); `internal` is never dialed with that framing.
    pub internal: SocketAddr,
    pub client: SocketAddr,
    /// This node's **intra-cluster** RPC listen address (ADR 0047): every
    /// internal-only `ClientRequest` variant (`Forwarded`, `ProposeSchema`,
    /// `WatchMetadata`, `JoinInfo`, and the internal-only forwarding
    /// payloads) is served here instead of on `client`. Required for every
    /// role — a control-only node receives `ProposeSchema` relays and serves
    /// `WatchMetadata` long-polls; a data-only node originates both. No
    /// default (a deliberate clean break, matching `internal`/`client`'s own
    /// no-default convention — no live deployments to keep back-compat
    /// with). See [`internal`](Self::internal)'s doc for the naming
    /// distinction from that field.
    pub intra: SocketAddr,
    /// The DynamoDB JSON-over-HTTP endpoint. Defaults (when absent in older
    /// configs) to an ephemeral loopback port.
    #[serde(default = "default_ephemeral_addr")]
    pub dynamo: SocketAddr,
    /// The **admin / debug** HTTP-JSON endpoint (ADR 0020) — a read-only
    /// introspection + operator-action surface on its own port, isolated from the
    /// client/dynamo data edges. Defaults (when absent in older configs) to an
    /// ephemeral loopback port.
    #[serde(default = "default_ephemeral_addr")]
    pub admin: SocketAddr,
    /// The **AnimusDB Console** (ADR 0052) — a DynamoDB-shaped data app for
    /// application developers, deliberately separate from the operator
    /// dashboard the admin port serves (ADR 0021): it must never surface
    /// cluster-shaped state (nodes, replicas, tablets, Raft, quorum,
    /// leaders, placement, health). It gets its **own** port rather than
    /// riding the admin listener (documented no-auth, trusted-interface-only,
    /// ADR 0020) or the DynamoDB listener (a wire protocol, not an HTTP app) —
    /// the same reasoning ADR 0047 used to split node-to-node RPC off the
    /// client port. Bound on combined and data-only nodes (both host CP-data
    /// tablets, the console's actual subject matter); **not** bound on a
    /// control-only node (ADR 0035) — it hosts no tablet, so it has nothing
    /// for the console to show. No default (a deliberate clean break,
    /// matching `intra`'s own no-default convention — no live deployments to
    /// keep back-compat with).
    pub console: SocketAddr,
}

/// Fallback endpoint for configs written before a field existed: an ephemeral
/// port on the loopback (the real port is learned after bind).
fn default_ephemeral_addr() -> SocketAddr {
    SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0)
}

/// A node whose listeners are bound but whose protocols are not yet started.
/// Expose the bound addresses, assemble the cluster peer book, then
/// [`start`](BoundNode::start).
pub struct BoundNode {
    id: NodeId,
    env: ProdEnv,
    /// This node's own data directory (the `data_dir` [`Node::bind`] was
    /// given) — kept, unlike before ADR 0043's sealer PR, so [`start_with`]
    /// can root this node's local segment-store building block
    /// (`FsSegmentStore`, ADR 0043 §A7b) at `dir.join("segments")`, a
    /// sibling of the `internal/` subdirectory `ProdEnv::bind` already owns.
    dir: PathBuf,
    internal_addr: SocketAddr,
    client_listener: TcpListener,
    client_addr: SocketAddr,
    dynamo_listener: TcpListener,
    dynamo_addr: SocketAddr,
    admin_listener: TcpListener,
    admin_addr: SocketAddr,
    /// The intra-cluster RPC listener (ADR 0047) — bound but not yet served
    /// in this PR; carried through to [`start_with`](Self::start_with) so a
    /// later PR can spawn `serve_requests` on it without touching the bind
    /// sequence.
    intra_listener: TcpListener,
    intra_addr: SocketAddr,
    /// The AnimusDB Data Console's own listener (ADR 0052) — a combined node
    /// hosts CP-data tablets, so it always binds one; see
    /// [`console`](crate::console)'s module doc.
    console_listener: TcpListener,
    console_addr: SocketAddr,
}

/// A node's identity + bound addresses, captured for the admin `/admin/config`
/// view (ADR 0020). Held behind an `Arc` in [`ClientCtx`] so it is cheap to clone
/// onto every connection. The live CP-member address map is read from replicated
/// `Metadata` at request time, not cached here.
pub(crate) struct AdminInfo {
    /// This node's one id (ADR 0040 PR1). `None` on a **data-only** node
    /// (`ControlHandle::Remote`) — it has no local control `RaftCore`... but
    /// under Option B a data-only node still has its own internal env/id, so
    /// this is `None` only if this node has no internal role at all (never
    /// happens today: every bound node has an id). Kept `Option` for the
    /// admin-JSON call sites that used to distinguish "no control role"/"no
    /// data role" — see [`internal_addr`](Self::internal_addr).
    pub(crate) node_id: Option<NodeId>,
    /// This node's one internal `ProdEnv` listen address (ADR 0040 PR1) —
    /// carries the control Raft (stream 0) and every hosted tablet's Raft
    /// group (stream ≥ 1). `None` only for a hand-built `AdminInfo` with no
    /// internal role at all (doesn't occur in practice).
    pub(crate) internal_addr: Option<SocketAddr>,
    pub(crate) client_addr: SocketAddr,
    /// `None` on a control-only node (the DynamoDB listener is never bound
    /// there, ADR 0035 PR3).
    pub(crate) dynamo_addr: Option<SocketAddr>,
    pub(crate) admin_addr: SocketAddr,
    /// This node's own intra-cluster RPC address (ADR 0047) — used to
    /// self-skip in `propose_schema`'s broadcast fallback (the intra-flavored
    /// dual of the old `client_addr` self-skip check).
    pub(crate) intra_addr: SocketAddr,
    /// This node's own deployment role (ADR 0035; ADR 0040 PR1 — no longer
    /// inferred from `control_id`/`raftkv_id` presence, since there is only
    /// one id now): `"control"`/`"data"`/`"combined"`, stamped literally by
    /// whichever `start_*` assembled this node — the same string it also
    /// self-registers into replicated `NodeAddrs.role`.
    pub(crate) role: &'static str,
    /// The control-plane Raft group (all control ids).
    pub(crate) control_ids: Vec<NodeId>,
    /// The static peer address book this node was started with.
    pub(crate) peers: BTreeMap<NodeId, SocketAddr>,
    /// Every node's **admin** address — the seed list the web dashboard (ADR 0021)
    /// fans out to. Each process knows the whole cluster's addresses (its
    /// `ClusterConfig` per-process, or the in-process bring-up). Falls back to just
    /// this node's admin address when the full set is unknown (the simple
    /// [`BoundNode::start`] path / hand-built nodes).
    pub(crate) admin_addrs: Vec<SocketAddr>,
    /// The `--auto-split K` key-count threshold this node was started with, if
    /// any (`--cluster N --auto-split K`; the per-process `--config`/`--node`
    /// path has no auto-split support yet, so this is always `None` there).
    /// Surfaced on `/admin/config` so the dashboard can flag a tablet as
    /// "over threshold, about to split" without hardcoding the value.
    pub(crate) auto_split_threshold: Option<usize>,
    /// The `--auto-split-bytes B` threshold (ADR 0034), if any — same
    /// `--cluster N`-only scoping as `auto_split_threshold` above. A
    /// CP-hosting node splits a tablet it leads once **either** configured
    /// threshold is exceeded.
    pub(crate) auto_split_bytes_threshold: Option<u64>,
}

/// Project the replicated schema catalog into the AnimusDB Data Console's own
/// [`console::TableSummary`] rows (ADR 0052 PR2 — the tables-list screen's
/// data source). Lives here, in `lib.rs` — not in `console.rs` — on purpose:
/// this is the one function in the whole node that reads `Metadata`'s schema
/// catalog on the console's behalf, so `console.rs` itself never needs to
/// import `Metadata`/`TableSchema`/`IndexKind`/any other schema-catalog type,
/// only the plain owned fields [`console::TableSummary`] is built from. See
/// `console`'s own module doc for why that boundary is load-bearing, not
/// incidental.
fn console_table_summaries(metadata: &Metadata) -> Vec<console::TableSummary> {
    metadata
        .schemas
        .iter()
        // A GSI's hidden materialization table (`<base>$<index>`, ADR 0041)
        // never actually gets a `Metadata::schemas` entry of its own — only
        // its tablets exist once the drain lazily provisions them (see
        // `admin.rs`'s own note on this) — but the filter is kept anyway as
        // the same belt-and-suspenders discipline `ClientCtx::drop_table`'s
        // own cascade uses: cheap, and it is what actually earns "excluded
        // server-side" as a property a regression test can assert on, rather
        // than resting on an invariant that holds elsewhere in the codebase
        // today but that this function has no way to enforce if it changes.
        .filter(|(name, _)| !animus_dynamo::index::is_index_table_name(name))
        // The reserved internal table (ADR 0018's 2026-08-24 amendment) is an
        // ordinary schema-registered table once its lazy bootstrap has run —
        // same belt-and-suspenders discipline as the filter just above.
        .filter(|(name, _)| !animus_dynamo::is_internal_table_name(name))
        .map(|(name, schema)| {
            let partition_key = console_key_summary(schema, &schema.partition_key);
            // DynamoDB has at most one sort key — the one-element case of
            // `clustering_keys` (`animus_dynamo::schema::to_dynamo` reads the
            // same first element back out for the identical reason).
            let sort_key = schema
                .clustering_keys
                .first()
                .map(|sk| console_key_summary(schema, sk));
            let gsi_count = schema
                .indexes
                .iter()
                .filter(|idx| idx.kind == animus_control::IndexKind::Global)
                .count() as u32;
            // An LSI shares the base partition key and adds an alternate
            // sort key, so a table with no sort key structurally cannot have
            // one — `None` here is that structural absence, not a count of
            // zero; the console renders the two differently (a dash vs.
            // `0`).
            let lsi_count = sort_key.as_ref().map(|_| {
                schema
                    .indexes
                    .iter()
                    .filter(|idx| idx.kind == animus_control::IndexKind::Local)
                    .count() as u32
            });
            console::TableSummary {
                name: name.clone(),
                partition_key,
                sort_key,
                gsi_count,
                lsi_count,
                stream: console_stream_summary(schema),
                ttl: console_ttl_summary(schema),
            }
        })
        .collect()
}

/// One column's name + declared DynamoDB `AttributeType`, console-shaped —
/// the shared building block [`console_table_summaries`] and
/// [`console_table_detail`] (ADR 0052 PR3) both use for every key attribute
/// they render. An attribute absent from `schema.columns` (never declared —
/// e.g. a just-added GSI's own hash attribute, which this adapter's
/// `UpdateTable` decoder does not require an `AttributeDefinitions` entry
/// for, unlike real DynamoDB) defaults to `"S"`, matching
/// `schema_bridge`'s own missing-type default.
fn console_key_summary(schema: &TableSchema, column_name: &str) -> console::KeySummary {
    console::KeySummary {
        name: column_name.to_string(),
        attribute_type: schema
            .column(column_name)
            .map(|c| animus_dynamo::schema::attribute_type_for(c.ty))
            .unwrap_or("S")
            .to_string(),
    }
}

/// The same projection for an *index* key attribute, which — unlike a base
/// table's own key — may genuinely have no declared type to report. See
/// [`console::IndexKeySummary`]: `IndexDef` stores only the attribute name,
/// so a type exists only when that attribute is also a declared column of
/// the base table. `None` (rather than [`console_key_summary`]'s `"S"`
/// fallback) so the console renders a bare name instead of asserting a type
/// nobody recorded.
fn console_index_key_summary(schema: &TableSchema, column_name: &str) -> console::IndexKeySummary {
    console::IndexKeySummary {
        name: column_name.to_string(),
        attribute_type: schema
            .column(column_name)
            .map(|c| animus_dynamo::schema::attribute_type_for(c.ty).to_string()),
    }
}

/// A table's stream configuration, console-shaped — shared by
/// [`console_table_summaries`]/[`console_table_detail`] and the
/// [`console::ConsoleBackend`] impl below (a `set_stream` call re-reads the
/// committed schema through this same projection rather than hand-building
/// its own).
fn console_stream_summary(schema: &TableSchema) -> console::StreamSummary {
    console::StreamSummary {
        enabled: schema.stream.is_some(),
        view_type: schema
            .stream
            .as_ref()
            .map(|s| stream_view_type_label(s.view_type).to_string()),
    }
}

/// A table's TTL configuration, console-shaped — the `set_ttl` sibling of
/// [`console_stream_summary`] above.
fn console_ttl_summary(schema: &TableSchema) -> console::TtlSummary {
    console::TtlSummary {
        enabled: schema.ttl.is_some(),
        attribute_name: schema.ttl.as_ref().map(|t| t.attribute_name.clone()),
    }
}

/// An [`animus_control::IndexStatus`]'s DynamoDB wire label
/// (`"CREATING"`/`"ACTIVE"`/`"DELETING"`) — `console.rs` never imports
/// `IndexStatus` itself (see that module's doc), so this is where the
/// translation happens, mirroring `stream_view_type_label`'s own precedent
/// (`animus_dynamo::wire::index_status_str` has the identical mapping but is
/// private to that crate).
fn console_index_status_label(status: IndexStatus) -> &'static str {
    match status {
        IndexStatus::Creating => "CREATING",
        IndexStatus::Active => "ACTIVE",
        IndexStatus::Deleting => "DELETING",
    }
}

/// An [`animus_control::IndexProjection`]'s console-shaped mirror — shared
/// by [`console_gsi_detail`] and the create-table endpoint's own response
/// (both read the projection back off the committed `IndexDef`, never
/// re-echo what the client asked for, so a decode-time normalization —
/// e.g. an omitted `Projection` defaulting to `ALL`, ADR 0052's create-table
/// amendment — is reflected honestly).
fn console_projection_summary(p: &animus_control::IndexProjection) -> console::ProjectionSummary {
    match p {
        animus_control::IndexProjection::All => console::ProjectionSummary {
            projection_type: "ALL".to_string(),
            non_key_attributes: None,
        },
        animus_control::IndexProjection::KeysOnly => console::ProjectionSummary {
            projection_type: "KEYS_ONLY".to_string(),
            non_key_attributes: None,
        },
        animus_control::IndexProjection::Include(names) => console::ProjectionSummary {
            projection_type: "INCLUDE".to_string(),
            non_key_attributes: Some(names.clone()),
        },
    }
}

/// One global secondary index, console-shaped — shared by
/// [`console_table_detail`] (every GSI on a table) and the
/// [`console::ConsoleBackend`] impl's `add_gsi`/`create_table` (the one
/// just-created index).
fn console_gsi_detail(schema: &TableSchema, idx: &animus_control::IndexDef) -> console::GsiDetail {
    console::GsiDetail {
        name: idx.name.clone(),
        hash_attribute: console_index_key_summary(schema, &idx.hash_attribute),
        sort_attribute: idx
            .sort_attribute
            .as_deref()
            .map(|a| console_index_key_summary(schema, a)),
        status: console_index_status_label(idx.status).to_string(),
        projection: console_projection_summary(&idx.projection),
    }
}

/// Project one table's full configuration for the Data Console's table page
/// Config tab (ADR 0052 PR3, `GET /console/api/tables/{name}`) — the
/// `TableDetail`-shaped sibling of [`console_table_summaries`]'s per-table
/// `TableSummary` (every count there becomes a full declaration here).
/// `None` for a table with no schema, **including** a GSI's own hidden
/// `<base>$<index>` materialization table — mirrors
/// [`console_table_summaries`]'s own exclusion filter, since that table has
/// no `Metadata::schemas` entry of its own to find in the first place
/// (`meta.table_schema` already returns `None` for it; the explicit
/// `is_index_table_name` check here is belt-and-suspenders, matching that
/// function's own comment on why it keeps the filter despite the invariant
/// holding elsewhere today).
fn console_table_detail(meta: &Metadata, table: &str) -> Option<console::TableDetail> {
    if animus_dynamo::index::is_index_table_name(table) {
        return None;
    }
    // Same reserved-internal-table exclusion as `console_table_summaries`
    // (ADR 0018's 2026-08-24 amendment) — a direct `GET /console/api/tables/
    // {name}` naming it must 404 like any other nonexistent table.
    if animus_dynamo::is_internal_table_name(table) {
        return None;
    }
    let schema = meta.table_schema(table)?;
    let partition_key = console_key_summary(schema, &schema.partition_key);
    let sort_key = schema
        .clustering_keys
        .first()
        .map(|sk| console_key_summary(schema, sk));
    let gsis = schema
        .indexes
        .iter()
        .filter(|idx| idx.kind == animus_control::IndexKind::Global)
        .map(|idx| console_gsi_detail(schema, idx))
        .collect();
    let lsis = schema
        .indexes
        .iter()
        .filter(|idx| idx.kind == animus_control::IndexKind::Local)
        .map(|idx| {
            // Always present for an LSI (`IndexDef`'s own invariant, enforced
            // at decode time by `animus_dynamo::wire::decode_indexes`); the
            // empty-string fallback is defense-in-depth only, never expected
            // to render.
            let sort_name = idx.sort_attribute.as_deref().unwrap_or_default();
            console::LsiDetail {
                name: idx.name.clone(),
                sort_attribute: console_index_key_summary(schema, sort_name),
            }
        })
        .collect();
    Some(console::TableDetail {
        name: table.to_string(),
        partition_key,
        sort_key,
        gsis,
        lsis,
        stream: console_stream_summary(schema),
        ttl: console_ttl_summary(schema),
    })
}

/// Translate a `dynamo::execute_routed` failure (a DynamoDB wire error JSON
/// body, `{"__type":..,"message":..}`) into a [`console::ConsoleError`] —
/// every mutating [`console::ConsoleBackend`] method's error path, so the
/// console surfaces the exact same status/message a real DynamoDB client
/// hitting the same `UpdateTable`/`UpdateTimeToLive` call would see, per
/// this PR's "reuse the existing execution path" rule (see `console.rs`'s
/// module doc and ADR 0052's amendment). Falls back to the raw body text if
/// it isn't the expected error shape (defensive only — `execute_routed`
/// always returns one of these two shapes).
fn console_wire_error(status: u16, body: &str) -> console::ConsoleError {
    let message = serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|v| {
            v.get("message")
                .and_then(|m| m.as_str())
                .map(str::to_string)
        })
        .unwrap_or_else(|| body.to_string());
    console::ConsoleError::new(status, message)
}

/// The three DynamoDB key `AttributeType`s (`S`/`N`/`B`) — every closed set
/// the create-table form's key-attribute-type controls can send.
fn is_valid_key_attribute_type(t: &str) -> bool {
    matches!(t, "S" | "N" | "B")
}

/// The Data Console's mutating-endpoint seam (ADR 0052 PR3, widened by PR6's
/// `create_table`) — [`console::ConsoleBackend`]'s one implementor. Every
/// method either reuses the same DynamoDB wire path the real edge/
/// `/admin/data/dynamo` use (`crate::dynamo::execute_routed`, this PR's
/// "reuse the existing execution path" rule) or, for `delete_table` (not a
/// DynamoDB wire operation at all), the same [`ClientCtx::drop_table`] the
/// admin dashboard's own drop-table action calls. See `console.rs`'s module
/// doc for why widening this trait never widens what `console.rs` itself can
/// see: every method here builds its request/response JSON and reads
/// `Metadata` on the console's behalf, so no schema-catalog type ever
/// crosses into that module.
#[async_trait::async_trait]
impl console::ConsoleBackend for ClientCtx {
    async fn create_table(
        &self,
        req: console::CreateTableRequest,
    ) -> Result<console::TableDetail, console::ConsoleError> {
        // -- client-side validation: every case that would otherwise reach
        // the wire only to bounce back as a decode error gets a clear
        // message here instead, and the two cases this PR's brief calls out
        // by name (an LSI with no sort key attribute of its own, and a
        // table declaring no sort key at all while still declaring an LSI)
        // are both rejected before a single byte reaches `execute_routed`.
        let table_name = req.table_name.trim();
        if table_name.is_empty() {
            return Err(console::ConsoleError::new(400, "table_name is required"));
        }
        if req.partition_key.name.trim().is_empty() {
            return Err(console::ConsoleError::new(
                400,
                "partition key name is required",
            ));
        }
        if !is_valid_key_attribute_type(&req.partition_key.attribute_type) {
            return Err(console::ConsoleError::new(
                400,
                "partition key attribute_type must be S, N, or B",
            ));
        }
        if let Some(sk) = &req.sort_key {
            if sk.name.trim().is_empty() {
                return Err(console::ConsoleError::new(400, "sort key name is required"));
            }
            if !is_valid_key_attribute_type(&sk.attribute_type) {
                return Err(console::ConsoleError::new(
                    400,
                    "sort key attribute_type must be S, N, or B",
                ));
            }
        }
        for lsi in &req.lsis {
            if lsi.index_name.trim().is_empty() {
                return Err(console::ConsoleError::new(
                    400,
                    "LSI index_name is required",
                ));
            }
            if lsi.sort_attribute.trim().is_empty() {
                return Err(console::ConsoleError::new(
                    400,
                    format!("LSI `{}` needs a sort key attribute", lsi.index_name),
                ));
            }
            if req.sort_key.is_none() {
                return Err(console::ConsoleError::new(
                    400,
                    "declaring an LSI requires the table to have its own sort key",
                ));
            }
        }
        for gsi in &req.gsis {
            if gsi.index_name.trim().is_empty() {
                return Err(console::ConsoleError::new(
                    400,
                    "GSI index_name is required",
                ));
            }
            if gsi.hash_attribute.trim().is_empty() {
                return Err(console::ConsoleError::new(
                    400,
                    format!("GSI `{}` needs a hash attribute", gsi.index_name),
                ));
            }
            if gsi.projection_type == "INCLUDE"
                && gsi
                    .projection_non_key_attributes
                    .as_ref()
                    .is_none_or(|a| a.is_empty())
            {
                return Err(console::ConsoleError::new(
                    400,
                    format!(
                        "GSI `{}`'s INCLUDE projection needs at least one attribute",
                        gsi.index_name
                    ),
                ));
            }
        }
        if req.stream_enabled
            && req
                .stream_view_type
                .as_deref()
                .is_none_or(|s| s.trim().is_empty())
        {
            return Err(console::ConsoleError::new(
                400,
                "stream_view_type is required to enable a stream",
            ));
        }
        if req.ttl_enabled
            && req
                .ttl_attribute_name
                .as_deref()
                .is_none_or(|s| s.trim().is_empty())
        {
            return Err(console::ConsoleError::new(
                400,
                "ttl_attribute_name is required to enable TTL",
            ));
        }

        // -- build the real CreateTable wire body. Deliberately no
        // `AttributeDefinitions` entry for any GSI/LSI key attribute — see
        // `console::CreateTableRequest`'s own doc for why sending one would
        // misrepresent what actually gets recorded.
        let mut key_schema = vec![serde_json::json!({
            "AttributeName": req.partition_key.name, "KeyType": "HASH",
        })];
        let mut attribute_definitions = vec![serde_json::json!({
            "AttributeName": req.partition_key.name,
            "AttributeType": req.partition_key.attribute_type,
        })];
        if let Some(sk) = &req.sort_key {
            key_schema.push(serde_json::json!({
                "AttributeName": sk.name, "KeyType": "RANGE",
            }));
            attribute_definitions.push(serde_json::json!({
                "AttributeName": sk.name, "AttributeType": sk.attribute_type,
            }));
        }
        let mut body = serde_json::json!({
            "TableName": table_name,
            "KeySchema": key_schema,
            "AttributeDefinitions": attribute_definitions,
        });
        if !req.gsis.is_empty() {
            let gsis: Vec<serde_json::Value> = req
                .gsis
                .iter()
                .map(|g| {
                    let mut key_schema = vec![serde_json::json!({
                        "AttributeName": g.hash_attribute, "KeyType": "HASH",
                    })];
                    if let Some(sort) = g.sort_attribute.as_deref().filter(|s| !s.trim().is_empty())
                    {
                        key_schema.push(serde_json::json!({
                            "AttributeName": sort, "KeyType": "RANGE",
                        }));
                    }
                    let mut projection = serde_json::json!({ "ProjectionType": g.projection_type });
                    if g.projection_type == "INCLUDE" {
                        projection["NonKeyAttributes"] = serde_json::Value::Array(
                            g.projection_non_key_attributes
                                .clone()
                                .unwrap_or_default()
                                .into_iter()
                                .map(serde_json::Value::String)
                                .collect(),
                        );
                    }
                    serde_json::json!({
                        "IndexName": g.index_name,
                        "KeySchema": key_schema,
                        "Projection": projection,
                    })
                })
                .collect();
            body["GlobalSecondaryIndexes"] = serde_json::Value::Array(gsis);
        }
        if !req.lsis.is_empty() {
            let lsis: Vec<serde_json::Value> = req
                .lsis
                .iter()
                .map(|l| {
                    serde_json::json!({
                        "IndexName": l.index_name,
                        "KeySchema": [
                            {"AttributeName": req.partition_key.name, "KeyType": "HASH"},
                            {"AttributeName": l.sort_attribute, "KeyType": "RANGE"},
                        ],
                    })
                })
                .collect();
            body["LocalSecondaryIndexes"] = serde_json::Value::Array(lsis);
        }
        if req.stream_enabled {
            body["StreamSpecification"] = serde_json::json!({
                "StreamEnabled": true,
                "StreamViewType": req.stream_view_type.as_deref().unwrap_or_default(),
            });
        }
        let payload = serde_json::to_vec(&body).unwrap_or_default();
        let (status, resp_body) =
            crate::dynamo::execute_routed(self, "DynamoDB_20120810.CreateTable", &payload).await;
        if status != 200 {
            return Err(console_wire_error(status, &resp_body));
        }

        // The table now exists; TTL is not part of `CreateTable`'s own wire
        // shape (`animus_dynamo::wire::Operation::CreateTable` carries no
        // TTL field at all — ADR 0051's `UpdateTimeToLive` is a separate
        // call even for a brand-new table), so enable it as a follow-up
        // call, same shape `set_ttl` already uses.
        if req.ttl_enabled {
            let ttl_body = serde_json::json!({
                "TableName": table_name,
                "TimeToLiveSpecification": {
                    "Enabled": true,
                    "AttributeName": req.ttl_attribute_name.as_deref().unwrap_or_default(),
                },
            });
            let payload = serde_json::to_vec(&ttl_body).unwrap_or_default();
            let (status, resp_body) =
                crate::dynamo::execute_routed(self, "DynamoDB_20120810.UpdateTimeToLive", &payload)
                    .await;
            if status != 200 {
                return Err(console_wire_error(status, &resp_body));
            }
        }

        let meta = self.metadata_fresh().await;
        console_table_detail(&meta, table_name).ok_or_else(|| {
            console::ConsoleError::new(500, "table created but not found in the catalog")
        })
    }

    async fn table_detail(&self, table: &str) -> Option<console::TableDetail> {
        console_table_detail(&self.effective_metadata(), table)
    }

    async fn add_gsi(
        &self,
        table: &str,
        req: console::AddGsiRequest,
    ) -> Result<console::GsiDetail, console::ConsoleError> {
        if req.index_name.trim().is_empty() {
            return Err(console::ConsoleError::new(400, "index_name is required"));
        }
        if req.hash_attribute.trim().is_empty() {
            return Err(console::ConsoleError::new(
                400,
                "hash_attribute is required",
            ));
        }
        let mut key_schema = vec![serde_json::json!({
            "AttributeName": req.hash_attribute, "KeyType": "HASH",
        })];
        if let Some(sort_attribute) = req
            .sort_attribute
            .as_deref()
            .filter(|s| !s.trim().is_empty())
        {
            key_schema.push(serde_json::json!({
                "AttributeName": sort_attribute, "KeyType": "RANGE",
            }));
        }
        // Deliberately no `AttributeDefinitions`: this adapter's
        // `GlobalSecondaryIndexUpdates` decoder never reads one (issue #319),
        // so sending types here would look like it recorded them while the
        // index read back untyped. See `console::AddGsiRequest`.
        let body = serde_json::json!({
            "TableName": table,
            "GlobalSecondaryIndexUpdates": [
                {"Create": {"IndexName": req.index_name, "KeySchema": key_schema}}
            ],
        });
        let payload = serde_json::to_vec(&body).unwrap_or_default();
        let (status, resp_body) =
            crate::dynamo::execute_routed(self, "DynamoDB_20120810.UpdateTable", &payload).await;
        if status != 200 {
            return Err(console_wire_error(status, &resp_body));
        }
        let meta = self.metadata_fresh().await;
        let Some(schema) = meta.table_schema(table) else {
            return Err(console::ConsoleError::new(
                500,
                "GSI committed but the table's schema is gone",
            ));
        };
        meta.table_indexes(table)
            .iter()
            .find(|d| d.name == req.index_name)
            .map(|idx| console_gsi_detail(schema, idx))
            .ok_or_else(|| {
                console::ConsoleError::new(500, "GSI committed but not found in the catalog")
            })
    }

    async fn drop_gsi(&self, table: &str, index: &str) -> Result<(), console::ConsoleError> {
        let body = serde_json::json!({
            "TableName": table,
            "GlobalSecondaryIndexUpdates": [ {"Delete": {"IndexName": index}} ],
        });
        let payload = serde_json::to_vec(&body).unwrap_or_default();
        let (status, resp_body) =
            crate::dynamo::execute_routed(self, "DynamoDB_20120810.UpdateTable", &payload).await;
        if status != 200 {
            return Err(console_wire_error(status, &resp_body));
        }
        Ok(())
    }

    async fn set_stream(
        &self,
        table: &str,
        req: console::SetStreamRequest,
    ) -> Result<console::StreamSummary, console::ConsoleError> {
        let body = if req.enabled {
            let Some(view_type) = req.view_type.as_deref().filter(|s| !s.trim().is_empty()) else {
                return Err(console::ConsoleError::new(
                    400,
                    "view_type is required to enable a stream",
                ));
            };
            serde_json::json!({
                "TableName": table,
                "StreamSpecification": {"StreamEnabled": true, "StreamViewType": view_type},
            })
        } else {
            serde_json::json!({
                "TableName": table,
                "StreamSpecification": {"StreamEnabled": false},
            })
        };
        let payload = serde_json::to_vec(&body).unwrap_or_default();
        let (status, resp_body) =
            crate::dynamo::execute_routed(self, "DynamoDB_20120810.UpdateTable", &payload).await;
        if status != 200 {
            return Err(console_wire_error(status, &resp_body));
        }
        let meta = self.metadata_fresh().await;
        let Some(schema) = meta.table_schema(table) else {
            return Err(console::ConsoleError::new(404, "no such table"));
        };
        Ok(console_stream_summary(schema))
    }

    async fn set_ttl(
        &self,
        table: &str,
        req: console::SetTtlRequest,
    ) -> Result<console::TtlSummary, console::ConsoleError> {
        let Some(attribute_name) = req
            .attribute_name
            .as_deref()
            .filter(|s| !s.trim().is_empty())
        else {
            return Err(console::ConsoleError::new(
                400,
                "attribute_name is required",
            ));
        };
        let body = serde_json::json!({
            "TableName": table,
            "TimeToLiveSpecification": {"Enabled": req.enabled, "AttributeName": attribute_name},
        });
        let payload = serde_json::to_vec(&body).unwrap_or_default();
        let (status, resp_body) =
            crate::dynamo::execute_routed(self, "DynamoDB_20120810.UpdateTimeToLive", &payload)
                .await;
        if status != 200 {
            return Err(console_wire_error(status, &resp_body));
        }
        let meta = self.metadata_fresh().await;
        let Some(schema) = meta.table_schema(table) else {
            return Err(console::ConsoleError::new(404, "no such table"));
        };
        Ok(console_ttl_summary(schema))
    }

    async fn delete_table(&self, table: &str) -> Result<(), console::ConsoleError> {
        if !self.metadata_fresh().await.has_table_schema(table) {
            return Err(console::ConsoleError::new(404, "no such table"));
        }
        self.drop_table(table.to_string())
            .await
            .map_err(|e| console::ConsoleError::new(409, e))
    }

    async fn scan_items(
        &self,
        table: &str,
        req: console::ScanItemsRequest,
    ) -> Result<console::ItemsPage, console::ConsoleError> {
        let mut body = serde_json::json!({ "TableName": table });
        if let Some(index_name) = &req.index_name {
            body["IndexName"] = serde_json::Value::String(index_name.clone());
        }
        if let Some(limit) = req.limit {
            body["Limit"] = serde_json::Value::from(limit);
        }
        if let Some(key) = req.exclusive_start_key {
            body["ExclusiveStartKey"] = serde_json::Value::Object(key);
        }
        let payload = serde_json::to_vec(&body).unwrap_or_default();
        let (status, resp_body) =
            crate::dynamo::execute_routed(self, "DynamoDB_20120810.Scan", &payload).await;
        if status != 200 {
            return Err(console_wire_error(status, &resp_body));
        }
        console_parse_items_page(&resp_body)
    }

    async fn query_items(
        &self,
        table: &str,
        req: console::QueryItemsRequest,
    ) -> Result<console::ItemsPage, console::ConsoleError> {
        let meta = self.effective_metadata();
        let Some(schema) = meta.table_schema(table) else {
            return Err(console::ConsoleError::new(404, "no such table"));
        };
        // Resolve the partition/sort attribute *names* to query by, server-side
        // — `console.rs` never imports a schema-catalog type, so the client
        // sends only the index name (a real closed set, from this same
        // table's own `TableDetail`) and the raw key *values*, never a
        // hand-typed attribute name. See `console::QueryItemsRequest`'s doc.
        let (pk_name, sk_name) = match &req.index_name {
            None => (
                schema.partition_key.clone(),
                schema.clustering_keys.first().cloned(),
            ),
            Some(index_name) => {
                let Some(idx) = schema.indexes.iter().find(|i| &i.name == index_name) else {
                    return Err(console::ConsoleError::new(404, "no such index"));
                };
                (idx.hash_attribute.clone(), idx.sort_attribute.clone())
            }
        };
        let mut key_condition = format!("{pk_name} = :pk_value");
        let mut expr_values = serde_json::Map::new();
        expr_values.insert(":pk_value".to_string(), req.partition_value.clone());
        if let Some(sort_condition) = &req.sort_condition {
            let Some(sk_name) = &sk_name else {
                return Err(console::ConsoleError::new(
                    400,
                    "this table/index has no sort key to condition on",
                ));
            };
            match sort_condition {
                console::SortKeyQuery::Equals { value } => {
                    key_condition.push_str(&format!(" AND {sk_name} = :sk_value"));
                    expr_values.insert(":sk_value".to_string(), value.clone());
                }
                console::SortKeyQuery::Between { lo, hi } => {
                    key_condition.push_str(&format!(" AND {sk_name} BETWEEN :sk_lo AND :sk_hi"));
                    expr_values.insert(":sk_lo".to_string(), lo.clone());
                    expr_values.insert(":sk_hi".to_string(), hi.clone());
                }
                console::SortKeyQuery::BeginsWith { value } => {
                    key_condition.push_str(&format!(" AND begins_with({sk_name}, :sk_value)"));
                    expr_values.insert(":sk_value".to_string(), value.clone());
                }
            }
        }
        let mut body = serde_json::json!({
            "TableName": table,
            "KeyConditionExpression": key_condition,
            "ExpressionAttributeValues": expr_values,
        });
        if let Some(index_name) = &req.index_name {
            body["IndexName"] = serde_json::Value::String(index_name.clone());
        }
        let payload = serde_json::to_vec(&body).unwrap_or_default();
        let (status, resp_body) =
            crate::dynamo::execute_routed(self, "DynamoDB_20120810.Query", &payload).await;
        if status != 200 {
            return Err(console_wire_error(status, &resp_body));
        }
        console_parse_items_page(&resp_body)
    }

    async fn get_item(
        &self,
        table: &str,
        key: console::WireItem,
    ) -> Result<Option<console::WireItem>, console::ConsoleError> {
        let body = serde_json::json!({ "TableName": table, "Key": serde_json::Value::Object(key) });
        let payload = serde_json::to_vec(&body).unwrap_or_default();
        let (status, resp_body) =
            crate::dynamo::execute_routed(self, "DynamoDB_20120810.GetItem", &payload).await;
        if status != 200 {
            return Err(console_wire_error(status, &resp_body));
        }
        let value: serde_json::Value = serde_json::from_str(&resp_body).map_err(|e| {
            console::ConsoleError::new(500, format!("malformed GetItem response: {e}"))
        })?;
        Ok(value.get("Item").and_then(|v| v.as_object().cloned()))
    }

    async fn put_item(
        &self,
        table: &str,
        item: console::WireItem,
    ) -> Result<(), console::ConsoleError> {
        let body =
            serde_json::json!({ "TableName": table, "Item": serde_json::Value::Object(item) });
        let payload = serde_json::to_vec(&body).unwrap_or_default();
        let (status, resp_body) =
            crate::dynamo::execute_routed(self, "DynamoDB_20120810.PutItem", &payload).await;
        if status != 200 {
            return Err(console_wire_error(status, &resp_body));
        }
        Ok(())
    }

    async fn delete_item(
        &self,
        table: &str,
        key: console::WireItem,
    ) -> Result<(), console::ConsoleError> {
        let body = serde_json::json!({ "TableName": table, "Key": serde_json::Value::Object(key) });
        let payload = serde_json::to_vec(&body).unwrap_or_default();
        let (status, resp_body) =
            crate::dynamo::execute_routed(self, "DynamoDB_20120810.DeleteItem", &payload).await;
        if status != 200 {
            return Err(console_wire_error(status, &resp_body));
        }
        Ok(())
    }

    async fn stream_shards(
        &self,
        table: &str,
        req: console::StreamShardsRequest,
    ) -> Result<console::StreamShardsPage, console::ConsoleError> {
        let meta = self.effective_metadata();
        if !meta.has_table_schema(table) {
            return Err(console::ConsoleError::new(404, "no such table"));
        }
        let Some(spec) = meta.table_stream(table) else {
            // The honest "no stream enabled" answer — see
            // `console::StreamShardsPage`'s own doc: a plain `200`, never a
            // `404`/error, since a table with no stream is the common case.
            return Ok(console::StreamShardsPage {
                enabled: false,
                view_type: None,
                stream_arn: None,
                shards: Vec::new(),
                last_evaluated_shard_id: None,
            });
        };
        let stream_arn = animus_dynamo::wire::stream_arn(table, &spec.label);
        let mut body = serde_json::json!({ "StreamArn": stream_arn });
        if let Some(start) = &req.exclusive_start_shard_id {
            body["ExclusiveStartShardId"] = serde_json::Value::String(start.clone());
        }
        let payload = serde_json::to_vec(&body).unwrap_or_default();
        let (status, resp_body) = crate::dynamo::execute_routed(
            self,
            "DynamoDBStreams_20120810.DescribeStream",
            &payload,
        )
        .await;
        if status != 200 {
            return Err(console_wire_error(status, &resp_body));
        }
        let value: serde_json::Value = serde_json::from_str(&resp_body).map_err(|e| {
            console::ConsoleError::new(500, format!("malformed DescribeStream response: {e}"))
        })?;
        let sd = &value["StreamDescription"];
        let shards = sd["Shards"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|s| console::ShardSummary {
                shard_id: s["ShardId"].as_str().unwrap_or_default().to_string(),
                parent_shard_id: s["ParentShardId"].as_str().map(str::to_string),
                starting_sequence_number: s["SequenceNumberRange"]["StartingSequenceNumber"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                ending_sequence_number: s["SequenceNumberRange"]["EndingSequenceNumber"]
                    .as_str()
                    .map(str::to_string),
            })
            .collect();
        Ok(console::StreamShardsPage {
            enabled: true,
            view_type: Some(stream_view_type_label(spec.view_type).to_string()),
            stream_arn: Some(stream_arn),
            shards,
            last_evaluated_shard_id: sd["LastEvaluatedShardId"].as_str().map(str::to_string),
        })
    }

    async fn get_shard_iterator(
        &self,
        table: &str,
        req: console::GetShardIteratorRequest,
    ) -> Result<String, console::ConsoleError> {
        let meta = self.effective_metadata();
        if !meta.has_table_schema(table) {
            return Err(console::ConsoleError::new(404, "no such table"));
        }
        let Some(spec) = meta.table_stream(table) else {
            return Err(console::ConsoleError::new(
                400,
                "this table has no stream enabled",
            ));
        };
        let stream_arn = animus_dynamo::wire::stream_arn(table, &spec.label);
        let mut body = serde_json::json!({
            "StreamArn": stream_arn,
            "ShardId": req.shard_id,
            "ShardIteratorType": req.iterator_type,
        });
        if let Some(seq) = &req.sequence_number {
            body["SequenceNumber"] = serde_json::Value::String(seq.clone());
        }
        let payload = serde_json::to_vec(&body).unwrap_or_default();
        let (status, resp_body) = crate::dynamo::execute_routed(
            self,
            "DynamoDBStreams_20120810.GetShardIterator",
            &payload,
        )
        .await;
        if status != 200 {
            return Err(console_wire_error(status, &resp_body));
        }
        let value: serde_json::Value = serde_json::from_str(&resp_body).map_err(|e| {
            console::ConsoleError::new(500, format!("malformed GetShardIterator response: {e}"))
        })?;
        value["ShardIterator"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| console::ConsoleError::new(500, "GetShardIterator returned no iterator"))
    }

    async fn get_stream_records(
        &self,
        _table: &str,
        req: console::GetStreamRecordsRequest,
    ) -> Result<console::StreamRecordsPage, console::ConsoleError> {
        // No `table`/label check here: `req.shard_iterator` is an opaque
        // token this same backend's `get_shard_iterator` already minted
        // against a resolved `StreamArn`, and the real `GetRecords` wire
        // path (`dynamo_streams::get_records`) independently re-validates
        // the token's own label against the catalog — a second check here
        // would just duplicate that gate, not add one.
        let mut body = serde_json::json!({ "ShardIterator": req.shard_iterator });
        if let Some(limit) = req.limit {
            body["Limit"] = serde_json::Value::from(limit);
        }
        let payload = serde_json::to_vec(&body).unwrap_or_default();
        let (status, resp_body) =
            crate::dynamo::execute_routed(self, "DynamoDBStreams_20120810.GetRecords", &payload)
                .await;
        if status != 200 {
            return Err(console_wire_error(status, &resp_body));
        }
        let value: serde_json::Value = serde_json::from_str(&resp_body).map_err(|e| {
            console::ConsoleError::new(500, format!("malformed GetRecords response: {e}"))
        })?;
        let records = value["Records"].as_array().cloned().unwrap_or_default();
        let next_shard_iterator = value["NextShardIterator"].as_str().map(str::to_string);
        Ok(console::StreamRecordsPage {
            records,
            next_shard_iterator,
        })
    }
}

/// Decode a `Scan`/`Query` wire response body (`{"Items": [...], "Count": n,
/// "ScannedCount": n[, "LastEvaluatedKey": {...}]}`) into an
/// [`console::ItemsPage`] — shared by [`ConsoleBackend::scan_items`] and
/// [`ConsoleBackend::query_items`] above. `Query` now paginates on the wire
/// (`animus_dynamo::wire::scan_response` is the response encoder for both
/// operations), but [`ConsoleBackend::query_items`] doesn't yet send a
/// `Limit`/`ExclusiveStartKey` of its own, so `LastEvaluatedKey` still comes
/// back absent in practice there — see [`console::ItemsPage`]'s own doc for
/// why threading the console's Items tab onto real `Query` pagination is a
/// deliberately separate, not-yet-done follow-up.
fn console_parse_items_page(body: &str) -> Result<console::ItemsPage, console::ConsoleError> {
    let value: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| console::ConsoleError::new(500, format!("malformed items response: {e}")))?;
    let items: Vec<console::WireItem> = value
        .get("Items")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_object().cloned()).collect())
        .unwrap_or_default();
    let scanned_count = value
        .get("ScannedCount")
        .and_then(|v| v.as_u64())
        .unwrap_or(items.len() as u64);
    let last_evaluated_key = value
        .get("LastEvaluatedKey")
        .and_then(|v| v.as_object().cloned());
    Ok(console::ItemsPage {
        items,
        scanned_count,
        last_evaluated_key,
    })
}

/// An [`animus_control::StreamViewType`]'s DynamoDB wire label — the same
/// vocabulary `StreamSpecification`/`DescribeStream` already use (ADR 0052:
/// "explains that in DynamoDB's own vocabulary"). `animus_dynamo::wire`
/// already has this exact mapping (`stream_view_type_str`), but it is
/// `pub(crate)` to that crate; duplicating four match arms here follows the
/// same precedent `animus-dynamo/CLAUDE.md` documents for its own
/// `streams_wire` module re-deriving small byte-shape functions rather than
/// widening a sibling crate's public surface for one caller.
fn stream_view_type_label(view_type: animus_control::StreamViewType) -> &'static str {
    match view_type {
        animus_control::StreamViewType::NewAndOldImages => "NEW_AND_OLD_IMAGES",
        animus_control::StreamViewType::NewImage => "NEW_IMAGE",
        animus_control::StreamViewType::OldImage => "OLD_IMAGE",
        animus_control::StreamViewType::KeysOnly => "KEYS_ONLY",
    }
}

/// The common assembly tail shared by every node shape (ADR 0035 PR3):
/// build the [`ClientCtx`] and spawn the tasks every node needs regardless of
/// role — control-only ([`BoundControlNode::start_control_with`]), or
/// combined/data-role ([`BoundNode::start_with`]): `route_sync_loop`/
/// `intra_route_sync_loop`, `metrics_sample_loop`, this node's own one-shot
/// `register_node_addrs` self-registration, **both** client-protocol
/// listeners (ADR 0047 — `serve_requests` spawned once per `ListenerKind`,
/// see that function's doc), and the admin HTTP endpoint (ADR 0020).
/// Returns the built `ClientCtx` — so the caller can
/// spawn whatever role-specific tasks it still needs (`bootstrap`/
/// `peer_sync_loop`/the growth-node mirror/`heartbeat_loop`/the tablet-host
/// reconciler/`auto_split_loop`/the dynamo listener for a data-capable
/// node; nothing more for a control-only one) — plus the join handles
/// spawned here, which the caller folds into its own task list so
/// [`Node::shutdown`] aborts all of it.
///
/// `self_addrs` is `(id, addrs)` for this node's own `register_node_addrs`
/// self-registration (ADR 0040 PR1: one id, one `internal` address, for
/// every role — a control-only node registers a real `internal` address
/// too, since it needs it for its own control Raft).
#[allow(clippy::too_many_arguments)] // node assembly: control handle + edge + role + admin + routing
fn spawn_common_tail(
    control: ControlHandle,
    edge: ClusterEdgeState,
    data: Option<DataRole>,
    admin_info: Arc<AdminInfo>,
    client_route: BTreeMap<NodeId, SocketAddr>,
    intra_route: BTreeMap<NodeId, SocketAddr>,
    self_addrs: (NodeId, NodeAddrs),
    client_listener: TcpListener,
    admin_listener: TcpListener,
    intra_listener: TcpListener,
    console_listener: Option<TcpListener>,
    control_storage: Option<SharedEngine>,
    env: ProdEnv,
    dynamo_auth: Option<Arc<BTreeMap<String, String>>>,
    split_mode: SplitMode,
) -> (ClientCtx, Vec<tokio::task::JoinHandle<()>>) {
    // The seed `route_sync_loop` (below) re-overlays `Metadata.node_addrs[*].client`
    // onto every tick (ADR 0032 PR1) — the same static-base pattern
    // `peer_sync_loop` uses for the raftkv-env peer book.
    let static_route = client_route.clone();
    // The `intra_route` sibling (ADR 0047) — see `intra_route_sync_loop`'s
    // doc for why this static seed is load-bearing, not just an
    // optimization.
    let static_intra_route = intra_route.clone();
    let ctx = ClientCtx {
        control,
        edge,
        env,
        data,
        client_route: Arc::new(Mutex::new(client_route)),
        intra_route: Arc::new(Mutex::new(intra_route)),
        admin: admin_info,
        metrics_history: Arc::new(Mutex::new(VecDeque::with_capacity(METRICS_HISTORY_CAP))),
        remote_metadata: Arc::new(Mutex::new(None)),
        control_storage,
        dynamo_auth,
        split_mode,
    };

    let mut tasks = Vec::with_capacity(5);
    // Route-sync loop (ADR 0032 PR1): keep `ctx.client_route` = the static seed
    // above ∪ `Metadata.node_addrs[*].client`, so a node grown in after this
    // node's own startup still becomes a valid client-op forward target
    // (`propose_schema`'s relay/broadcast reads `ctx.intra_route` instead,
    // ADR 0047 — see `intra_route_sync_loop` just below). Runs on every node,
    // including a growth node (reads `effective_metadata()`, so it syncs off
    // its own remote mirror) and a control-only node.
    tasks.push(tokio::spawn(route_sync_loop(ctx.clone(), static_route)));
    // Intra-route sync loop (ADR 0047) — the `route_sync_loop` sibling for
    // `ctx.intra_route`; see `intra_route_sync_loop`'s own doc for why it
    // needs no static seed. Runs on every node, same as `route_sync_loop`.
    tasks.push(tokio::spawn(intra_route_sync_loop(
        ctx.clone(),
        static_intra_route,
    )));
    // Metrics-history sampler (ADR 0020 dashboard sparklines): periodic
    // snapshots of this node's own aggregated counters. Runs on every node —
    // a control-only node's snapshot is just the control sink (`metrics_text`/
    // `metrics_json` skip the raftkv sink when `ctx.data` is `None`).
    tasks.push(tokio::spawn(metrics_sample_loop(ctx.clone())));
    // This node's own identity self-registration (ADR 0032 PR1; ADR 0040
    // Decision C since PR4 — the registration CAS is now the mechanism, not
    // just an address-book update): one-shot, so peer-sync (internal
    // addresses) and any node's route/peers views (client/admin addresses)
    // can resolve it regardless of when this node joined relative to the
    // reader. Every node shape reaches this — a fresh bootstrap node whose
    // id `bootstrap()`'s own `UpsertMember`/`admin_add_member` also claims
    // (harmless, order-independent: `RegisterNode`'s collision check is
    // addrs-only, so it never fights over labels/status another command
    // already owns) and a growth node with no other claim path at all (e.g.
    // a control-only permanently-non-voter — `BoundControlNode::
    // start_control_with` has no `admin_add_member` call of its own; this is
    // its *only* claim). No labels here (this is a bare identity/address
    // claim, not an operator-labeled add) — `admin_add_member`/
    // `admin_add_control_member` are where real labels are set, and
    // `RegisterNode`'s apply never overwrites an already-`members`-present
    // entry's labels, so this can never clobber them.
    {
        let ctx = ctx.clone();
        let (node, addrs) = self_addrs;
        tasks.push(tokio::spawn(async move {
            let _ = ctx.register_node(node, addrs, BTreeMap::new()).await;
        }));
    }
    // The two client-protocol listeners (ADR 0047): one parameterized
    // `serve_requests` function, not a fork — see that function's doc.
    // `Client` refuses every `Surface::Intra` request (`handle_request`'s one
    // guard clause); `Intra` serves everything (a deliberate superset, not a
    // partition — see `Surface`'s doc).
    tasks.push(tokio::spawn(serve_requests(
        client_listener,
        ctx.clone(),
        ListenerKind::Client,
    )));
    tasks.push(tokio::spawn(serve_requests(
        intra_listener,
        ctx.clone(),
        ListenerKind::Intra,
    )));
    // The admin / debug HTTP-JSON endpoint on its own port (ADR 0020).
    tasks.push(tokio::spawn(admin::serve(admin_listener, ctx.clone())));
    // The AnimusDB Data Console (ADR 0052) — `None` on a control-only node
    // (it hosts no CP-data tablet, so it has nothing for the console to
    // show; see `BoundControlNode::start_control_with`, the only caller that
    // passes `None`). Still takes no `ClientCtx` directly: a
    // `console::TableSnapshotFn` closure (PR2's tables-list screen, built
    // from `ctx.effective_metadata()` + `console_table_summaries` below) and
    // — PR3's table page — an `Arc<dyn console::ConsoleBackend>` built from
    // `ClientCtx`'s own impl of that trait just above. So `console.rs`
    // itself never sees `Metadata`/`ClientCtx`/any other cluster-shaped
    // type, only the plain console types those two seams hand it. See
    // `console`'s module doc for why that boundary matters.
    if let Some(console_listener) = console_listener {
        let table_source: console::TableSnapshotFn = {
            let ctx = ctx.clone();
            Arc::new(move || console_table_summaries(&ctx.effective_metadata()))
        };
        let backend: Arc<dyn console::ConsoleBackend> = Arc::new(ctx.clone());
        tasks.push(tokio::spawn(console::serve(
            console_listener,
            table_source,
            backend,
        )));
    }

    (ctx, tasks)
}

impl BoundNode {
    /// `(id, addr)` — the one entry this node contributes to the cluster peer
    /// book (ADR 0040 PR1: one identity, one internal `ProdEnv`, per node).
    pub fn peer_entries(&self) -> [(NodeId, SocketAddr); 1] {
        [(self.id.clone(), self.internal_addr)]
    }

    /// The address clients connect to.
    pub fn client_addr(&self) -> SocketAddr {
        self.client_addr
    }

    /// The address the DynamoDB JSON/HTTP endpoint listens on.
    pub fn dynamo_addr(&self) -> SocketAddr {
        self.dynamo_addr
    }

    /// The address the admin / debug HTTP endpoint listens on (ADR 0020).
    pub fn admin_addr(&self) -> SocketAddr {
        self.admin_addr
    }

    /// The address the intra-cluster RPC endpoint listens on (ADR 0047).
    pub fn intra_addr(&self) -> SocketAddr {
        self.intra_addr
    }

    /// Wire the peer address book into the node's one env and start all
    /// protocols, with the CP group backed by the durable on-disk
    /// [`LsmEngine`] ([`StorageBackend::Lsm`]). `control_ids` is the full
    /// control group. Combined-mode-only convenience: derives the `data_ids`
    /// [`start_with`](Self::start_with) now takes explicitly by assuming
    /// every id in `control_ids` is also a data-role node's id — trivially
    /// true post-ADR-0040 (one identity per node) for every caller of this
    /// simpler entry point.
    ///
    /// # Errors
    /// Propagates a failure to open the CP group's on-disk engine.
    pub async fn start(
        self,
        peers: BTreeMap<NodeId, SocketAddr>,
        control_ids: Vec<NodeId>,
    ) -> std::io::Result<Node> {
        let admin_addr = self.admin_addr;
        let data_ids = control_ids.clone();
        self.start_with(
            peers,
            control_ids,
            data_ids,
            StorageBackend::default(),
            ClusterEdgeState::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            None,
            None,
            vec![admin_addr],
            DEFAULT_ORPHAN_SWEEP_AFTER,
        )
        .await
    }

    /// Like [`start`](Self::start), but selects the CP group's storage engine and
    /// options. [`StorageBackend::Lsm`] is durable (survives restart);
    /// [`StorageBackend::Memory`] is volatile (ephemeral runs). `auto_split_threshold`
    /// opts a CP-hosting node into the automatic key-count split trigger (Phase
    /// 2.4): when a tablet it leads exceeds that many keys, it splits. Sibling
    /// `auto_split_bytes_threshold` (ADR 0034) does the same for an
    /// (approximate) scoped-bytes trigger. Either, both, or neither may be
    /// `Some`; `(None, None)` (the default) disables auto-split entirely.
    ///
    /// `data_ids` is the set of ids [`bootstrap`] auto-registers as `Active`
    /// data members — i.e. the ids of nodes that actually run the **data**
    /// role. Callers compute it explicitly (in combined mode, every control
    /// id is also a data id post-ADR-0040 — one identity per node — see
    /// [`ClusterConfig::data_ids`]). A growth/join caller passes the
    /// **pre-growth** set here too, mirroring `control_ids`: bootstrap must
    /// never auto-register a growth node itself (it self-registers `Down`
    /// via `admin_add_member` instead, promoted to `Active` by its own
    /// heartbeat — see `run_node_growth`'s doc).
    ///
    /// # Errors
    /// Propagates a failure to open the CP group's on-disk engine (LSM backend
    /// only).
    #[allow(clippy::too_many_arguments)] // node assembly: ids + backend + edge + route + split opts
    #[allow(clippy::too_many_arguments)]
    pub async fn start_with(
        self,
        peers: BTreeMap<NodeId, SocketAddr>,
        control_ids: Vec<NodeId>,
        data_ids: Vec<NodeId>,
        backend: StorageBackend,
        edge: ClusterEdgeState,
        client_route: BTreeMap<NodeId, SocketAddr>,
        intra_route: BTreeMap<NodeId, SocketAddr>,
        auto_split_threshold: Option<usize>,
        auto_split_bytes_threshold: Option<u64>,
        cluster_admin_addrs: Vec<SocketAddr>,
        orphan_sweep_after: Duration,
    ) -> std::io::Result<Node> {
        self.start_with_streams(
            peers,
            control_ids,
            data_ids,
            backend,
            edge,
            client_route,
            intra_route,
            auto_split_threshold,
            auto_split_bytes_threshold,
            cluster_admin_addrs,
            orphan_sweep_after,
            StreamSealKnobs::default(),
            SegmentStoreConfig::default(),
            DEFAULT_STREAM_RETENTION,
        )
        .await
    }

    /// Like [`start_with`](Self::start_with), with explicit DynamoDB Streams
    /// sealer knobs, segment-store selection, and the segment-janitor's own
    /// retention grace period (ADR 0042/0043's round-3 sealer + janitor PRs)
    /// — the same layered-wrapper convention `_with_orphan_sweep_after`
    /// already established (see that entry in the `CLAUDE.md` engineering
    /// log): every existing `start_with` call site (the whole pre-existing
    /// test suite) keeps compiling and behaving identically, defaulting
    /// internally to production knobs and the default cluster-replicated
    /// store; a test that needs tiny seal/retention thresholds (this
    /// codebase's own testing discipline: never wait out a 4-hour age
    /// trigger, a 24-hour retention window, or write 4 MiB to trip a size
    /// one) calls this directly. Also spawns the **segment janitor**
    /// (`segment_janitor::segment_janitor_loop`, ADR 0043 §A9) — see that
    /// module's own doc for why it is spawned unconditionally here (a
    /// combined node can always become the control-plane leader) and
    /// self-gates every tick on `ctx.edge.leader_handle()`, the identical
    /// pattern `auto_split_loop`/`txn_resolver_loop` already use. Defaults
    /// [`start_with_growth`](Self::start_with_growth)'s own
    /// `auto_split_change_rate` to `None` — see that method's doc.
    #[allow(clippy::too_many_arguments)]
    pub async fn start_with_streams(
        self,
        peers: BTreeMap<NodeId, SocketAddr>,
        control_ids: Vec<NodeId>,
        data_ids: Vec<NodeId>,
        backend: StorageBackend,
        edge: ClusterEdgeState,
        client_route: BTreeMap<NodeId, SocketAddr>,
        intra_route: BTreeMap<NodeId, SocketAddr>,
        auto_split_threshold: Option<usize>,
        auto_split_bytes_threshold: Option<u64>,
        cluster_admin_addrs: Vec<SocketAddr>,
        orphan_sweep_after: Duration,
        stream_seal_knobs: StreamSealKnobs,
        segment_store_config: SegmentStoreConfig,
        stream_retention: Duration,
    ) -> std::io::Result<Node> {
        self.start_with_growth(
            peers,
            control_ids,
            data_ids,
            backend,
            edge,
            client_route,
            intra_route,
            auto_split_threshold,
            auto_split_bytes_threshold,
            cluster_admin_addrs,
            orphan_sweep_after,
            stream_seal_knobs,
            segment_store_config,
            stream_retention,
            None,
            Duration::ZERO,
            ttl_reaper::DEFAULT_TTL_SWEEP_INTERVAL,
            None,
            SplitMode::default(),
            BackupStoreConfig::default(),
        )
        .await
    }

    /// Like [`start_with_streams`](Self::start_with_streams), with the
    /// opt-in **change-rate** auto-split trigger (ADR 0042 §14, growth PR3
    /// Fork F): `--auto-split-change-rate RATE` — a streamed led tablet
    /// whose own smoothed change-append rate ([`ChangeRateTracker`],
    /// bytes/sec) sustains above `RATE` triggers the same `trigger_split`
    /// path every other trigger uses. `None` (the default every other
    /// entry point still passes) disables it entirely — zero behavior
    /// change for an existing deployment/test. See [`AutoSplitThresholds::
    /// change_rate`]'s own doc for why this needs its own signal at all
    /// (the base-scoped byte/key thresholds structurally can't see
    /// change-log churn).
    ///
    /// `ttl_sweep_interval` (ADR 0051) is the TTL reaper's own sweep cadence
    /// — see `ttl_reaper::DEFAULT_TTL_SWEEP_INTERVAL`'s doc for why it
    /// defaults to a minute. Every caller above `start_with_growth` in this
    /// layered stack passes that default; a test that needs a fast sweep
    /// calls this method (or `run_node_with_ttl_sweep_interval`) directly,
    /// the same "widen the innermost layer, mint a thin test-facing
    /// wrapper" convention `quiesce_after` established.
    ///
    /// `dynamo_auth` (ADR 0057) is the client DynamoDB port's SigV4
    /// credential store — `None` (every caller above this layer) disables
    /// auth entirely, byte-identical to pre-ADR-0057 behavior. A caller that
    /// wants it set (`run_node_with_streams_quiesce_and_ttl_sweep_interval`,
    /// reading `ClusterConfig::dynamo_auth`, or `start_cluster_inner` for
    /// `--cluster N`) calls this method directly, the same layered-wrapper
    /// convention as every other knob here.
    ///
    /// `split_mode` (ADR 0058 Train 2 rung 3) selects which workflow this
    /// node's `ClientCtx::trigger_split` proposes — `SplitMode::Copy`
    /// (every caller above this layer) is byte-for-byte the original ADR
    /// 0050 workflow. See [`SplitMode`]'s own doc.
    ///
    /// `backup_store_config` (ADR 0059 §1) selects this node's second,
    /// backup-dedicated [`BackupStoreHandle`] — `BackupStoreConfig::Cluster`
    /// (every caller above this layer) is the default K-replicated store;
    /// `--config`/`--node`'s and `--cluster N`'s own `--backup-store
    /// cluster|fs:PATH` CLI flag threads through here. **Plumbing only**
    /// (ADR 0059 Train 1 PR②) — nothing yet reads or writes through the
    /// resulting handle.
    #[allow(clippy::too_many_arguments)]
    pub async fn start_with_growth(
        self,
        peers: BTreeMap<NodeId, SocketAddr>,
        control_ids: Vec<NodeId>,
        data_ids: Vec<NodeId>,
        backend: StorageBackend,
        edge: ClusterEdgeState,
        client_route: BTreeMap<NodeId, SocketAddr>,
        intra_route: BTreeMap<NodeId, SocketAddr>,
        auto_split_threshold: Option<usize>,
        auto_split_bytes_threshold: Option<u64>,
        cluster_admin_addrs: Vec<SocketAddr>,
        orphan_sweep_after: Duration,
        stream_seal_knobs: StreamSealKnobs,
        segment_store_config: SegmentStoreConfig,
        stream_retention: Duration,
        auto_split_change_rate: Option<u64>,
        quiesce_after: Duration,
        ttl_sweep_interval: Duration,
        dynamo_auth: Option<Arc<BTreeMap<String, String>>>,
        split_mode: SplitMode,
        backup_store_config: BackupStoreConfig,
    ) -> std::io::Result<Node> {
        self.env.set_peers(peers.clone());
        // The initial (static) peer book + an env clone, kept for the
        // **peer-sync loop** (ADR 0040 PR1: one identity per node, one
        // shared internal env — this collapses the pre-PR1 `peer_sync_loop`/
        // `control_peer_sync_loop` pair into one loop over one book): it
        // rebuilds this node's env peer book as `static ∪ Metadata.
        // cp_member_addrs ∪ Metadata.node_addrs[*].internal` so a
        // runtime-joined member (CP group replica, split sibling, or
        // runtime-added control voter alike) becomes reachable.
        let static_peers = peers;
        let sync_env = self.env.clone();
        // An env clone for the per-node **tablet-host reconciler** (ADR
        // 0031 PR4): every tablet's group this node stands up runs on it,
        // stream-addressed by tablet id (ADR 0026 Stage B) — the same env
        // that also carries the control-plane Raft on stream 0.
        let hook_env = self.env.clone();
        // An env clone for the **failure-detection heartbeat loop** (#3): each
        // node heartbeats the control group *as its own member id* (the
        // cluster members are node ids), so the control plane's `detect_loop`
        // marks a crashed node `Down`.
        let hb_env = self.env.clone();
        let my_id = self.id.clone();
        let my_addr = self.internal_addr;
        // Captured here (all `SocketAddr`, `Copy`) for the node-address-book
        // self-registration below (ADR 0032 PR1) — `self.client_listener`/
        // `self.admin_listener` (not `Copy`) are moved into their `serve` tasks
        // further down, but the addresses themselves are needed there too.
        let my_client_addr = self.client_addr;
        let my_admin_addr = self.admin_addr;
        let my_intra_addr = self.intra_addr;

        // The node's identity + bound addresses for the admin `/admin/config`
        // view (ADR 0020), captured before the env is consumed below.
        let admin_info = Arc::new(AdminInfo {
            node_id: Some(self.id.clone()),
            internal_addr: Some(self.internal_addr),
            client_addr: self.client_addr,
            dynamo_addr: Some(self.dynamo_addr),
            admin_addr: self.admin_addr,
            intra_addr: self.intra_addr,
            role: "combined",
            control_ids: control_ids.clone(),
            peers: static_peers.clone(),
            admin_addrs: if cluster_admin_addrs.is_empty() {
                vec![self.admin_addr]
            } else {
                cluster_admin_addrs
            },
            auto_split_threshold,
            auto_split_bytes_threshold,
        });

        // Keep a clone of the one internal env so [`Node::shutdown`] can abort
        // every task it owns (both Raft drivers + the accept loop), freeing its
        // listener port for a restart. ADR 0040 PR1: one shared env, so just
        // one entry — kept as a `Vec` for shape-parity with the control-only/
        // data-only `Node` variants (both single-env already).
        let envs = vec![self.env.clone()];

        // The one shared metrics sink (ADR 0040 PR1: control Raft and CP group
        // now record into the *same* env's sink, not two distinct ones — see
        // `ClientCtx::metrics_text`'s `is_same_sink` dedup). Captured before
        // the env is consumed below.
        let raftkv_metrics = self.env.metrics();

        // This node's **one shared storage engine** (ADR 0026/0028): every tablet
        // this node ever hosts — across every table — merges into it, confined by
        // its own `StorageScope` (a table-id prefix + the tablet's own key range).
        // Opened once, here, and cloned into each tablet's `RaftKvNode` as the
        // per-node tablet-host reconciler (ADR 0031 PR4) stands groups up. A
        // restart just re-opens the same engine (`LsmEngine::open` recovers its
        // durable state) and the reconciler re-discovers every tablet to host
        // from replicated `Metadata` — there is no more per-tablet durable
        // marker to load.
        //
        // Opened **before** `RaftNode::start` (below), a hangover from when this
        // node's own CP-side reconfigure loop polled on a fixed period racing the
        // control plane's own `reconcile_loop` (ADR 0031 amended this out: the
        // reconciler now reacts to a `metadata_watch` wake, not a fixed cadence,
        // so it no longer needs a head start to win that race — see
        // `tablet_host_reconciler_loop`'s doc). No harm in keeping the order.
        let storage = match backend {
            StorageBackend::Lsm => match LsmEngine::open(self.env.clone(), LSM_PREFIX).await {
                Ok(lsm) => SharedEngine::Lsm(lsm),
                Err(e) => {
                    return Err(std::io::Error::other(format!(
                        "opening the node's shared CP storage engine: {e}"
                    )));
                }
            },
            StorageBackend::Memory => SharedEngine::Mem(MemoryEngine::new()),
        };

        // The control plane's system-keyspace engine (ADR 0038): `Metadata` is
        // `DRIVER_APPLIED`, so this **same already-open shared engine** is now
        // the durable home of the apply task's published `Metadata` cache, not
        // just a shadow mirror of an in-core copy. Keys are globally
        // namespaced under `syskv::RESERVED_NAMESPACE` (PR1's reserved-name
        // rejection guarantees no user table/keyspace can ever collide with
        // it), so no `StorageScope` wrapper is needed the way a per-tablet CP
        // group needs one — this is a genuinely global, node-wide keyspace,
        // not a per-tenant slice of one.
        //
        // ADR 0040 PR1: `RaftNode::start_with_metrics` gets its own clone of
        // this node's one shared env — the control Raft rides stream 0
        // (`PRIMARY_STREAM`, its `env.recv()`/`env.send()` default), and every
        // per-tablet Raft group this node hosts rides its own tablet-id stream
        // (≥ 1, ADR 0026) on a separate clone below, so the two never collide
        // on the same inbox despite sharing one `NodeId`.
        let control_metrics = self.env.metrics();
        // ADR 0040 PR6: `orphan_sweep_after` (config/CLI-knob, `Duration::ZERO`
        // disables) is the same grace period the leader's own volatile
        // orphan-member-sweep timer uses — see `animus_control::node`'s doc.
        let raft = match &storage {
            SharedEngine::Lsm(lsm) => RaftNode::start_with_orphan_sweep_after(
                self.env.clone(),
                control_ids.clone(),
                control_metrics,
                lsm.clone(),
                animus_control::DeltaRing::default(),
                orphan_sweep_after,
            ),
            SharedEngine::Mem(mem) => RaftNode::start_with_orphan_sweep_after(
                self.env.clone(),
                control_ids.clone(),
                control_metrics,
                mem.clone(),
                animus_control::DeltaRing::default(),
                orphan_sweep_after,
            ),
        };
        // Register this node's control handle in this **node's own**
        // `ClusterEdgeState` (ADR 0013/ADR 0031 PR2 — edge state is always
        // per-node, in `--cluster N` exactly as in one-process-per-node), so
        // `propose_schema` can propose locally when this node happens to be the
        // control leader. When it isn't, `propose_schema` relays
        // `ClientRequest::ProposeSchema` one hop to the leader's node via
        // `intra_route` (ADR 0047; was `client_route` pre-ADR-0047) — the same
        // relay path a follower-connected DDL always used in
        // one-process-per-node mode (`tests/schema_ddl_relay.rs`); a
        // `--cluster N` in-process node now exercises it too instead of always
        // finding the leader's handle locally.
        edge.register_control(raft.clone());

        // **Leaderful CP per-tablet Raft group** (ADR 0017 #3a) — the v1 data plane
        // (ADR 0019). Stage 3a hosts a single, statically-placed CP group spanning
        // the first `min(N, MAX_REPLICATION_FACTOR)` nodes' `raftkv` ids. A node in
        // that set runs a `RaftKvNode` on its `raftkv_env` (own id/port/dir — the
        // single-consumer inbox rule), backed by its own engine; the handle is
        // registered in this node's own edge state so the wire edges route a table's
        // reads/writes locally when this node leads (else forward, via
        // `client_route`). The group is started with a **split
        // hook** (Phase 2.2): on a committed `Split` it mints the new tablet's
        // co-resident group. Dynamic CP reconfigure over `ProdEnv` is later v1 work.
        //
        // The shared client context is built **here** (via the tail every node
        // shape shares, `spawn_common_tail` — ADR 0035 PR3), before the CP
        // hosting block, so the split-seed + re-host paths can publish a new
        // member's address through it (`register_node_addrs` relays to the
        // control leader cross-process via `client_route` — #4 cross-process
        // split-address relay), not just via a local control-leader handle.
        // `spawn_common_tail` also spawns `route_sync_loop`/`metrics_sample_loop`/
        // this node's own `register_node_addrs` self-registration/
        // `serve_requests` (both listeners)/`admin::serve` — every task a control-only node needs
        // too (see [`BoundControlNode::start_control_with`]); the tasks spawned
        // below this point are combined-mode/data-role-only.
        // This node's stream-shard segment store (ADR 0043 §A7b, round-3
        // sealer PR): built and started (its serving task claims this
        // node's own `SEGMENT_STREAM` inbox, ADR 0026) here, alongside the
        // other per-node infrastructure this same section already builds.
        let segment_store = build_segment_store(
            &self.env,
            &self.dir,
            ControlHandle::Local(raft.clone()),
            my_id.clone(),
            &segment_store_config,
        );
        // This node's backup store (ADR 0059 §1) — a second, independently
        // configured handle alongside `segment_store` above; see
        // `build_backup_store`'s own doc. Plumbing only (Train 1 PR②): no
        // consumer reads or writes through it yet.
        let backup_store = build_backup_store(
            &self.env,
            &self.dir,
            ControlHandle::Local(raft.clone()),
            my_id.clone(),
            &backup_store_config,
        );
        let data_role = DataRole {
            rmw_lock: Arc::new(tokio::sync::Mutex::new(())),
            raftkv_metrics,
            base_id: my_id.clone(),
            segment_store,
            backup_store,
            stream_seal_knobs,
            change_rates: ChangeRateTracker::default(),
            split_builds: Arc::new(Mutex::new(BTreeMap::new())),
        };
        let (ctx, mut tasks) = spawn_common_tail(
            ControlHandle::Local(raft.clone()),
            edge.clone(),
            Some(data_role),
            admin_info,
            client_route,
            intra_route,
            (
                my_id.clone(),
                NodeAddrs {
                    internal: my_addr.to_string(),
                    client: my_client_addr.to_string(),
                    admin: my_admin_addr.to_string(),
                    intra: my_intra_addr.to_string(),
                    role: "combined".to_string(),
                },
            ),
            self.client_listener,
            self.admin_listener,
            self.intra_listener,
            Some(self.console_listener),
            Some(storage.clone()),
            self.env.clone(),
            dynamo_auth,
            split_mode,
        );

        // The per-node **tablet-host reconciler** (ADR 0031 PR4): the single
        // writer of "does this node host tablet T" — see
        // `tablet_host_reconciler_loop`'s doc for the event-driven trigger.
        // `on_host`/`on_teardown` mirror every hosting change into this node's
        // own `ClusterEdgeState` (routing), which becomes a read-only mirror of
        // the reconciler's own bookkeeping — never a second writer. Built
        // unconditionally: the reconciler runs on **every** node (a spare not
        // yet placed on any tablet still hosts one later, once the placement
        // reconciler places it there). No CP group is stood up at node start
        // (ADR 0023): a fresh cluster has zero data tablets; the reconciler
        // stands each table's group up once `CreateTable` provisions its
        // tablet, and re-forms it from the shared engine's already-durable
        // data on restart.
        let mut reconciler = {
            let host_edge = edge.clone();
            let teardown_edge = edge.clone();
            let base_id = my_id.clone();
            let on_teardown = move |tablet: TabletId| {
                teardown_edge.unregister_raftkv(tablet, base_id.clone());
            };
            // ADR 0050 rung 1: the reconciler no longer receives the node's
            // shared engine — it opens ONE PRIVATE ENGINE PER HOSTED TABLET
            // through the factory seam (the node's `storage` above now backs
            // only the control plane's system keyspace, ADR 0038).
            match &storage {
                SharedEngine::Lsm(_) => CpReconciler::Lsm(Reconciler::new(
                    hook_env.clone(),
                    LsmTabletFactory { env: hook_env },
                    my_id.clone(),
                    move |tablet, node: &RaftKvNode<ProdEnv, LsmEngine<ProdEnv>>| {
                        host_edge.register_raftkv(tablet, CpGroup::Lsm(node.clone()));
                    },
                    on_teardown,
                )),
                SharedEngine::Mem(_) => CpReconciler::Mem(Reconciler::new(
                    hook_env,
                    MemoryTabletEngines::new(),
                    my_id.clone(),
                    move |tablet, node: &RaftKvNode<ProdEnv, MemoryEngine>| {
                        host_edge.register_raftkv(tablet, CpGroup::Mem(node.clone()));
                    },
                    on_teardown,
                )),
            }
        };
        // ADR 0044 phase-1 PR4 production wiring (PR7 layers the
        // `--quiesce-after` CLI flag on top of this same knob):
        // `Duration::ZERO` (every existing call site) disables it entirely —
        // zero behavior change. Data-plane groups only (fork G).
        if !quiesce_after.is_zero() {
            // See `MIN_QUIESCE_AFTER`'s own doc for the full argument. The
            // CLI's own parser is the primary enforcement (a release build
            // still refuses a misconfigured `--quiesce-after`); this is the
            // second-layer belt for any other caller reaching this method.
            debug_assert!(
                quiesce_after >= MIN_QUIESCE_AFTER,
                "quiesce_after ({quiesce_after:?}) must be at least \
                 MIN_QUIESCE_AFTER ({MIN_QUIESCE_AFTER:?}) or 0 to disable \
                 quiescence — see that constant's own doc"
            );
            reconciler.enable_quiescence(quiesce_after);
        }

        // Bootstrap: whichever node is leader registers membership (no data tablet)
        // (idempotent). `spawn_common_tail` (above) already started `tasks` with
        // the tail every node shape shares (`route_sync_loop`/
        // `metrics_sample_loop`/this node's own `register_node_addrs`
        // self-registration/`serve_requests` (both listeners)/`admin::serve`) — everything below is
        // combined-mode/data-role-only, tracked in the same task list so
        // `shutdown` aborts all of it and releases the client/dynamo
        // listener ports (these run on plain `tokio::spawn`, off the `Env`
        // network).
        // `data_ids` is caller-supplied (see `start_with`'s doc) — a caller
        // that scopes it to only the data-role nodes (or, for growth/join, the
        // pre-growth set) is respected exactly.
        tasks.push(tokio::spawn(bootstrap(raft.clone(), data_ids)));

        // Peer-sync loop (ADR 0040 PR1: one loop over one shared env — this
        // collapses the pre-PR1 `peer_sync_loop`/`control_peer_sync_loop`
        // pair): keep this node's env peer book = `static ∪ Metadata.
        // cp_member_addrs ∪ Metadata.node_addrs[*].internal`, so a
        // runtime-registered member (split sibling / joined node / a control
        // voter added at runtime) becomes reachable for both the control
        // Raft and this node's per-tablet Raft groups alike (same env, same
        // book). Runs on every node.
        tasks.push(tokio::spawn(peer_sync_loop(
            ctx.clone(),
            sync_env,
            static_peers.clone(),
        )));

        // **Control-plane-follower-less growth node mirror** (ADR 0030): this
        // node's own control role is a genuine voter of `control_ids` iff its own
        // id is *in* that set — the common case for every node started
        // the normal way (`start`/`run_node_with`/`start_cluster_*`, which always
        // pass a `control_ids` that includes `self.id`). A node started
        // via `run_node_growth` deliberately passes the **pre-growth** control
        // group instead (it "needs no control-voter slot" — see that fn's doc),
        // so its own `RaftCore` permanently sits outside `control_ids`: it can
        // never become a voter, campaign, or receive real AppendEntries from the
        // real leader (whose own peer set is derived from *its* config, which
        // never learned of this node — the control group stays static, ADR
        // 0030's documented v1 limitation). Such a node instead mirrors real
        // cluster state by polling `ClientRequest::Status` from one of the
        // pre-growth control nodes' **intra** addresses (ADR 0047 — this
        // node's own `WatchMetadata` long-poll is intra-only; derived from
        // `intra_route`, which growth's expanded config populates for every
        // node it lists, mirroring `client_route`) into `ctx.remote_metadata`,
        // read via `effective_metadata()`. A no-op (empty seed list, loop
        // returns immediately) for every other node.
        if !control_ids.contains(&self.id) {
            let seeds: Vec<SocketAddr> = control_ids
                .iter()
                .filter_map(|id| ctx.intra_addr(id.clone()))
                .collect();
            tasks.push(tokio::spawn(remote_metadata_sync_loop(ctx.clone(), seeds)));

            // Self-registration (ADR 0032 PR2): every growth node — whether
            // started via `run_node_growth`'s "operator calls `POST
            // /admin/member/add` first" flow or the newer seed/join
            // `run_node_join` (no operator hand-holding at all) — must become
            // a real `Metadata` member before the placement reconciler can
            // ever place a tablet on it. `admin_add_member` is idempotent (a
            // no-op success if already registered, ADR 0030's own doc), so
            // folding it in here simplifies `run_node_growth` too: an
            // operator's own explicit add-member call (still supported —
            // `tests/cluster_growth.rs` keeps its explicit
            // `POST /admin/member/add` as a regression for exactly that
            // idempotent path) becomes a redundant, harmless confirmation
            // rather than the only path a growth node has in.
            {
                let ctx = ctx.clone();
                let node = my_id;
                tasks.push(tokio::spawn(async move {
                    let _ = ctx.admin_add_member(node, BTreeMap::new()).await;
                }));
            }
        }

        // **Failure-detection heartbeat loop** (#3 / ADR 0012): every node heartbeats
        // the control group *as its own member id* (the cluster members are
        // node ids, registered by `bootstrap`), so the control leader's
        // `detect_loop` marks a crashed node `Down`. Runs on every node; the
        // peer book includes the control addrs (the static book), so the
        // heartbeats reach the control group. **Live destinations (ADR 0037
        // closing PR)**: `heartbeat_loop_live` re-derives the target list from
        // `ctx.control.config()` each tick (falling back to this node's own
        // static `control_ids` snapshot only until the first live read
        // lands), so a control voter added at runtime is heartbeated without
        // needing this node to restart — see that function's doc.
        tasks.push(tokio::spawn(heartbeat_loop_live(
            ctx.clone(),
            hb_env,
            control_ids,
        )));

        // **Tablet-host reconciler trigger** (ADR 0031 PR4): replaces the three
        // loops above (`cp_reconfigure_loop`, `cp_join_host_loop`, `cp_gc_loop`)
        // with one per-node reaction to `Metadata` changes — narrow/host/
        // reconfigure/release/reclaim, in that fixed order, driven by
        // `animus_cp_data::host::Reconciler`. Runs on **every** node (hosting is
        // dynamic — a node hosts a tablet's group once `CreateTable`/the
        // placement reconciler places it here).
        tasks.push(tokio::spawn(tablet_host_reconciler_loop(
            ctx.clone(),
            reconciler,
        )));

        // **In-doubt transaction recovery + resolver** (ADR 0018 §2/PR5):
        // periodically pushes stale `Pending` records past their grace
        // period and fans out `TxnResolve` for decided-but-unresolved ones,
        // over every tablet this node currently leads. Data-role-only (it
        // walks `ctx.edge.hosted_groups()`, empty on a control-only node) —
        // harmless to run on every data-capable node the same way
        // `auto_split_loop`/the reconciler do (each tick checks leadership
        // per tablet).
        tasks.push(tokio::spawn(txn_resolver_loop(ctx.clone())));

        // GSI drain (ADR 0041 §4): materializes global secondary indexes from
        // the change records indexed writes leave behind. Data-role-only and
        // per-tablet leadership-checked, exactly like `txn_resolver_loop` above
        // — a node that leads no tablet does nothing each tick.
        tasks.push(tokio::spawn(index_drain::change_consumer_loop(ctx.clone())));

        // The TTL reaper (ADR 0051 §4/§6): deletes items whose declared TTL
        // has passed, on every led tablet of a TTL-enabled table. Same
        // "run everywhere, self-gate per tablet on `group.is_leader()`"
        // shape as the GSI drain just above — see `ttl_reaper.rs`'s own
        // module doc for the quiescence/conditional-delete contracts.
        tasks.push(tokio::spawn(ttl_reaper::ttl_reaper_loop(
            ctx.clone(),
            ttl_sweep_interval,
        )));

        // The segment janitor (ADR 0043 §A9, round-3 PR7): retention +
        // replica repair over the whole stream-shard catalog. Control-
        // plane-leader-only (self-gated every tick, `segment_janitor.rs`'s
        // own doc) — spawned unconditionally here, exactly like
        // `auto_split_loop`/`txn_resolver_loop` above self-gate on
        // per-tablet leadership.
        tasks.push(tokio::spawn(segment_janitor::segment_janitor_loop(
            ctx.clone(),
            stream_retention,
        )));

        // The secondary-index backfill-completion aggregator (ADR 0045 §4):
        // flips a table's index from `Creating` to `Active` once every one
        // of its tablets has reported a finished backfill scan.
        // Control-plane-leader-only (self-gated every tick,
        // `index_backfill.rs`'s own doc) — spawned unconditionally here,
        // exactly like the segment janitor just above.
        tasks.push(tokio::spawn(index_backfill::index_backfill_loop(
            ctx.clone(),
        )));

        // Auto-split loop (Phase 2.4 / ADR 0034), opt-in: a node splits a tablet
        // it leads once it exceeds **either** configured threshold (it checks
        // leadership per tablet, so running it on every node is harmless).
        // Growth PR3 Fork F: `auto_split_change_rate` joins the same
        // either-triggers-fires gate, opt-in and streamed-tables-only.
        if auto_split_threshold.is_some()
            || auto_split_bytes_threshold.is_some()
            || auto_split_change_rate.is_some()
        {
            tasks.push(tokio::spawn(auto_split_loop(
                ctx.clone(),
                AutoSplitThresholds {
                    keys: auto_split_threshold,
                    bytes: auto_split_bytes_threshold,
                    change_rate: auto_split_change_rate,
                },
            )));
        }
        // The DynamoDB JSON/HTTP endpoint — data-role-only, unlike the
        // plain client server + admin endpoint (already spawned by
        // `spawn_common_tail`, which every node shape runs).
        tasks.push(tokio::spawn(dynamo::serve(
            self.dynamo_listener,
            ctx.clone(),
        )));

        Ok(Node {
            raft: ControlHandle::Local(raft),
            envs,
            tasks,
            edge: ctx.edge.clone(),
            client_addr: self.client_addr,
            dynamo_addr: Some(self.dynamo_addr),
            admin_addr: self.admin_addr,
            intra_addr: self.intra_addr,
            console_addr: Some(self.console_addr),
            #[cfg(test)]
            test_ctx: ctx,
        })
    }
}

/// A running node. Holds the handles that keep its envs and tasks alive.
///
/// **ADR 0035 PR3**: this one type now backs both a combined-mode/data-role
/// node (two internal `ProdEnv` roles, both listeners bound) and a
/// control-only node (one internal role, no `raftkv`/dynamo listeners at
/// all) — see [`BoundControlNode::start_control_with`]. `envs` is therefore a
/// `Vec` (1 or 2 entries) rather than a fixed-size array, and `dynamo_addr`
/// is `Option` internally; the public accessor below still
/// return a bare `SocketAddr` (panicking if absent) so every existing
/// combined-mode caller — which only ever holds a `Some` — is unaffected.
pub struct Node {
    /// This node's control-plane access (ADR 0035 PR1/PR4) — `Local` for
    /// combined mode and a control-only node (both hold a real local
    /// `RaftNode`); `Remote` for a data-only node (ADR 0035 PR4, no local
    /// control `RaftCore` at all). [`is_control_leader`](Self::is_control_leader)/
    /// [`metadata`](Self::metadata)/[`propose_meta`](Self::propose_meta)
    /// degrade accordingly for `Remote` — see each method's doc.
    raft: ControlHandle,
    /// This node's internal `ProdEnv` role(s) — control + raftkv for
    /// combined mode, control only for a control-only node (ADR 0035 PR3),
    /// raftkv only for a data-only node (ADR 0035 PR4) — kept so
    /// [`shutdown`](Node::shutdown) can abort every task they own and free
    /// their listener ports.
    envs: Vec<ProdEnv>,
    /// The client-facing listener tasks (client TCP / dynamo HTTP), which
    /// run on plain `tokio::spawn` off the `Env` network; aborted on shutdown.
    tasks: Vec<tokio::task::JoinHandle<()>>,
    /// This node's own edge state (ADR 0031 PR2 — cheap to clone, `Arc`-wrapped
    /// internally), kept so [`shutdown_graceful`](Self::shutdown_graceful) can
    /// gracefully halt every CP group *this node* hosts before the hard abort in
    /// [`shutdown`](Self::shutdown). Always empty on a control-only node (it
    /// hosts no CP group), so the graceful halt there is a no-op.
    edge: ClusterEdgeState,
    client_addr: SocketAddr,
    /// `None` on a control-only node (ADR 0035 PR3) — the DynamoDB listener is
    /// never bound there. See [`dynamo_addr`](Self::dynamo_addr)'s doc.
    dynamo_addr: Option<SocketAddr>,
    admin_addr: SocketAddr,
    /// This node's intra-cluster RPC listen address (ADR 0047). Always
    /// populated — every deployment shape binds and (from `intra/2-cutover`
    /// onward) serves it.
    intra_addr: SocketAddr,
    /// `None` on a control-only node (ADR 0052) — the AnimusDB Data Console
    /// listener is never bound there (it hosts no CP-data tablet). See
    /// [`console_addr`](Self::console_addr)'s doc.
    console_addr: Option<SocketAddr>,
    /// Test-only: a clone of this node's own [`ClientCtx`] (the exact one
    /// `spawn_common_tail` built and handed to this node's listeners/
    /// background loops), so an in-crate test module can call a
    /// `ClientCtx`-scoped `pub(crate)` primitive (e.g.
    /// [`dynamo::kind_write_item_at_leader`]) directly — sharing this node's
    /// real `rmw_lock`/routing/edge state, not a hand-rolled stand-in — the
    /// same reason `confirm_futility_tests` already reaches into `node.edge`.
    /// `#[cfg(test)]`-only: no production cost, and no confusion with the
    /// single source of truth for a live connection's own `ClientCtx`
    /// (`serve_requests`' per-connection clone).
    #[cfg(test)]
    test_ctx: ClientCtx,
}

impl Node {
    /// Bind this node's listeners (the one internal env + the client TCP
    /// server + the DynamoDB HTTP endpoint) and create its data
    /// directory (ADR 0040 PR1: one identity, one internal `ProdEnv`, per
    /// node — the control Raft and every per-tablet Raft group this node
    /// hosts share it, disambiguated by stream, ADR 0026).
    ///
    /// # Errors
    /// Propagates any bind / directory-creation failure.
    pub async fn bind(
        id: NodeId,
        addrs: RoleAddrs,
        data_dir: impl Into<PathBuf>,
    ) -> std::io::Result<BoundNode> {
        let dir = data_dir.into();
        let (env, internal_addr) =
            ProdEnv::bind(id.clone(), addrs.internal, dir.join("internal")).await?;
        let client_listener = TcpListener::bind(addrs.client).await?;
        let client_addr = client_listener.local_addr()?;
        let dynamo_listener = TcpListener::bind(addrs.dynamo).await?;
        let dynamo_addr = dynamo_listener.local_addr()?;
        let admin_listener = TcpListener::bind(addrs.admin).await?;
        let admin_addr = admin_listener.local_addr()?;
        let intra_listener = TcpListener::bind(addrs.intra).await?;
        let intra_addr = intra_listener.local_addr()?;
        let console_listener = TcpListener::bind(addrs.console).await?;
        let console_addr = console_listener.local_addr()?;
        Ok(BoundNode {
            id,
            env,
            dir,
            internal_addr,
            client_listener,
            client_addr,
            dynamo_listener,
            dynamo_addr,
            admin_listener,
            admin_addr,
            intra_listener,
            intra_addr,
            console_listener,
            console_addr,
        })
    }

    /// Bind a **control-only** node's listeners (ADR 0035 PR3): the internal
    /// `ProdEnv` (control Raft only — it hosts no tablet, so no stream ever
    /// rides above 0) plus the client + admin TCP listeners only — no
    /// dynamo listener, and (ADR 0052) no console listener either: a
    /// control-only node hosts no CP-data tablet, so it has nothing the
    /// console could show — see [`console`](crate::console)'s module doc.
    ///
    /// # Errors
    /// Propagates any bind / directory-creation failure.
    pub async fn bind_control(
        id: NodeId,
        addrs: RoleAddrs,
        data_dir: impl Into<PathBuf>,
    ) -> std::io::Result<BoundControlNode> {
        let dir = data_dir.into();
        let (env, internal_addr) =
            ProdEnv::bind(id.clone(), addrs.internal, dir.join("internal")).await?;
        let client_listener = TcpListener::bind(addrs.client).await?;
        let client_addr = client_listener.local_addr()?;
        let admin_listener = TcpListener::bind(addrs.admin).await?;
        let admin_addr = admin_listener.local_addr()?;
        let intra_listener = TcpListener::bind(addrs.intra).await?;
        let intra_addr = intra_listener.local_addr()?;
        Ok(BoundControlNode {
            id,
            env,
            internal_addr,
            client_listener,
            client_addr,
            admin_listener,
            admin_addr,
            intra_listener,
            intra_addr,
        })
    }

    /// Bind a **data-only** node's listeners (ADR 0035 PR4): the internal
    /// `ProdEnv` (every per-tablet Raft group this node hosts, plus its own
    /// failure-detection heartbeats to the control group — no local control
    /// `RaftCore` at all, `Node::bind_control`'s exact dual) plus the
    /// client/dynamo/admin/console TCP listeners — a data-only node hosts
    /// real CP-data tablets, so it binds the console listener (ADR 0052) just
    /// like a combined node.
    ///
    /// # Errors
    /// Propagates any bind / directory-creation failure.
    pub async fn bind_data(
        id: NodeId,
        addrs: RoleAddrs,
        data_dir: impl Into<PathBuf>,
    ) -> std::io::Result<BoundDataNode> {
        let dir = data_dir.into();
        let (env, internal_addr) =
            ProdEnv::bind(id.clone(), addrs.internal, dir.join("internal")).await?;
        let client_listener = TcpListener::bind(addrs.client).await?;
        let client_addr = client_listener.local_addr()?;
        let dynamo_listener = TcpListener::bind(addrs.dynamo).await?;
        let dynamo_addr = dynamo_listener.local_addr()?;
        let admin_listener = TcpListener::bind(addrs.admin).await?;
        let admin_addr = admin_listener.local_addr()?;
        let intra_listener = TcpListener::bind(addrs.intra).await?;
        let intra_addr = intra_listener.local_addr()?;
        let console_listener = TcpListener::bind(addrs.console).await?;
        let console_addr = console_listener.local_addr()?;
        Ok(BoundDataNode {
            id,
            env,
            dir,
            internal_addr,
            client_listener,
            client_addr,
            dynamo_listener,
            dynamo_addr,
            admin_listener,
            admin_addr,
            intra_listener,
            intra_addr,
            console_listener,
            console_addr,
        })
    }

    /// The address clients connect to.
    pub fn client_addr(&self) -> SocketAddr {
        self.client_addr
    }

    /// The address the DynamoDB JSON/HTTP endpoint listens on.
    ///
    /// # Panics
    /// If this node has no data role (ADR 0035 PR3 control-only node) — the
    /// listener is never bound there. Every real caller (the CLI printouts,
    /// the test suite) only ever holds a combined-mode/data-role `Node`.
    pub fn dynamo_addr(&self) -> SocketAddr {
        self.dynamo_addr
            .expect("dynamo_addr: this node has no data role (ADR 0035 PR3 control-only)")
    }

    /// The address the admin / debug HTTP endpoint listens on (ADR 0020).
    pub fn admin_addr(&self) -> SocketAddr {
        self.admin_addr
    }

    /// The address the intra-cluster RPC endpoint listens on (ADR 0047).
    pub fn intra_addr(&self) -> SocketAddr {
        self.intra_addr
    }

    /// The address the AnimusDB Data Console listens on (ADR 0052).
    ///
    /// # Panics
    /// If this node has no data role — see [`dynamo_addr`](Self::dynamo_addr)'s
    /// doc; the console has nothing to show on a control-only node either.
    pub fn console_addr(&self) -> SocketAddr {
        self.console_addr
            .expect("console_addr: this node has no data role (ADR 0035 PR3 control-only)")
    }

    /// Whether this node's control replica currently believes it is leader.
    /// Always `false` for a data-only node (ADR 0035 PR4) — it holds no
    /// control-plane Raft role at all.
    pub fn is_control_leader(&self) -> bool {
        self.raft.is_leader()
    }

    /// This node's cached cluster metadata. For a data-only node (ADR 0035
    /// PR4) this is its own polled mirror of the control deployment — see
    /// `ControlHandle::metadata_cached`'s doc — rather than a local Raft
    /// replica's applied state.
    pub fn metadata(&self) -> Metadata {
        self.raft.metadata_cached()
    }

    /// Test-only: this node's own `ClientCtx` — see the `test_ctx` field's
    /// doc for why an in-crate test needs the real one rather than a
    /// hand-built stand-in.
    #[cfg(test)]
    pub(crate) fn ctx_for_test(&self) -> ClientCtx {
        self.test_ctx.clone()
    }

    /// Propose a control-plane [`MetaCommand`] on this node's control replica,
    /// returning whether it was accepted (i.e. this node is the leader). The
    /// interim admin hook for cluster metadata operations the wire edges do not
    /// yet expose. A non-leader proposal is dropped (`false`); the
    /// caller retries on the current leader. Replication + durability are the
    /// control plane's (the command commits through Raft).
    ///
    /// Always `false` for a data-only node (ADR 0035 PR4): proposing is
    /// inherently a local-Raft-log operation (`ControlHandle`'s own doc), and
    /// a `Remote` handle has no local log to append to — the caller must
    /// target a real control-group member instead.
    pub fn propose_meta(&self, command: MetaCommand) -> bool {
        match &self.raft {
            ControlHandle::Local(raft) => {
                matches!(raft.propose(command), ProposeResult::Accepted { .. })
            }
            ControlHandle::Remote(_) => false,
        }
    }

    /// Gracefully stop the node: abort its client-facing listeners (client, plus
    /// dynamo on a data-role node) and every task its internal `ProdEnv`
    /// role(s) own (the control Raft driver, plus the CP Raft driver on a
    /// data-role node, and the internal accept loops). This releases every
    /// listener port so a replacement node can rebind the same addresses on
    /// the same data directory — the clean teardown a stopped OS process would
    /// otherwise provide. Idempotent.
    ///
    /// On-disk state is unaffected: a value already acked to a client was Raft-
    /// committed + fsynced to the CP group's LSM WAL before the ack, so it survives
    /// the restart.
    ///
    /// **`abort()` on a `JoinHandle`/`AbortHandle` only *requests* cancellation
    /// — it doesn't wait for the task to actually stop**, so the listener a
    /// just-aborted task owns (e.g. the admin/client TCP listener, or a
    /// `ProdEnv` role's internal accept loop) isn't guaranteed dropped, and its
    /// port isn't guaranteed free, the instant this call returns. A caller that
    /// immediately rebinds the same address (a same-address restart) needs
    /// [`shutdown_and_wait`](Self::shutdown_and_wait) instead — plain `shutdown`
    /// remains for callers that only need the node to stop (most simulated-crash
    /// tests never rebind the killed node's own address in the same process).
    ///
    /// **Latches every hosted CP group's `halted` flag first** (issue #282):
    /// unlike [`shutdown_graceful`](Self::shutdown_graceful), this bare path has
    /// no grace period at all before the hard `task.abort()`/`ProdEnv::shutdown()`
    /// below, so a killed node's driver can land mid-WAL-I/O with `halted` still
    /// unset — the exact window `persist_wal`'s/`flush_pending`'s halted-gated
    /// assert (`animus-cp-data`'s `CLAUDE.md`) otherwise turns into an
    /// unconditional panic on a racing I/O hiccup, indistinguishable from a real
    /// durability fault. [`ClusterEdgeState::halt_hosted_cp_groups`] is a plain
    /// atomic store plus two wakes per group — cheap, synchronous, no wait for
    /// `is_stopped()` — so it costs this fire-and-forget path nothing and keeps
    /// its contract (request the stop, don't wait for it) intact.
    pub fn shutdown(&self) {
        self.edge.halt_hosted_cp_groups();
        for task in &self.tasks {
            task.abort();
        }
        for env in &self.envs {
            env.shutdown();
        }
    }

    /// Like [`shutdown`](Self::shutdown), but also waits (bounded,
    /// best-effort) for every aborted task — this node's client-facing
    /// listeners and each internal `ProdEnv` role's accept loop alike — to
    /// actually finish unwinding before returning, so every listener this
    /// node owns is genuinely dropped, and every port genuinely free, by the
    /// time this call completes.
    ///
    /// Root-causes the `full_split_cluster_restart_recovers_metadata_and_data`
    /// flake (`AddrInUse` on rebind under `cargo test --workspace`-level
    /// contention, see `docs/engineering-lessons.md`): a bare `shutdown`
    /// followed immediately by a same-address rebind can race this *same*
    /// process's own not-yet-unwound listener task for the port, and under
    /// enough CPU contention that race can outlast even a generous
    /// rebind-retry bound. [`shutdown_graceful`](Self::shutdown_graceful) —
    /// what every restart test already calls before rebinding — uses this
    /// instead of the plain `shutdown` for exactly this reason.
    ///
    /// Latches every hosted CP group's `halted` flag first, exactly like
    /// [`shutdown`](Self::shutdown) — see that method's doc; this path
    /// hard-aborts the same driver tasks, just with an added wait afterward.
    pub async fn shutdown_and_wait(&self) {
        self.edge.halt_hosted_cp_groups();
        for task in &self.tasks {
            task.abort();
        }
        wait_all_finished(&self.tasks).await;
        for env in &self.envs {
            env.shutdown_and_wait().await;
        }
    }

    /// Graceful teardown: durably flush the control-plane WAL, then gracefully
    /// halt every hosted CP group, **before** the hard-abort [`shutdown`](Self::shutdown).
    ///
    /// `shutdown` alone aborts the Raft driver, but a `MetaCommand` (e.g. a
    /// `CreateTable` schema proposal) is applied + acked **synchronously** in
    /// `propose` while the driver fsyncs the WAL asynchronously — and the driver is
    /// usually parked between ticks. So a bare `shutdown` can abort the driver in
    /// the apply→fsync window and lose an *acked* schema across a restart (the
    /// flaky `tests/dynamo_schema.rs::create_table_survives_node_restart`).
    /// `RaftNode::flush` syncs that pending tail first, so a clean teardown is
    /// actually durable — which is what a restart test (a clean teardown standing
    /// in for an OS process restart) needs.
    ///
    /// A raw `shutdown()` also hard-`abort()`s the CP-data apply task via
    /// `ProdEnv::shutdown()`, which can land mid-`storage.merge(..).await` and
    /// surface as a `tokio::fs` background-task panic when the runtime's blocking
    /// pool is torn down underneath it (harmless to durability — an un-acked
    /// write just isn't durable yet — but a noisy, uncontrolled panic on every
    /// real shutdown). [`ClusterEdgeState::shutdown_all_cp_groups`] stops each CP
    /// group's driver cleanly (the same shutdown-then-wait pattern the per-node
    /// tablet-host reconciler's own teardown uses, ADR 0031 PR4) first, so
    /// `shutdown`'s abort has nothing in flight to race. (A
    /// `kill -9` is still exposed; the durable-before-ack control-plane fix is a
    /// tracked follow-up.)
    ///
    /// Ends in [`shutdown_and_wait`](Self::shutdown_and_wait), not the plain
    /// hard-abort [`shutdown`](Self::shutdown) — every caller of
    /// `shutdown_graceful` in this codebase is a restart test that rebinds
    /// this node's own addresses right afterward, so it needs the "listener
    /// really is dropped, port really is free" guarantee (see that method's
    /// doc for why a bare `abort()` doesn't provide one).
    pub async fn shutdown_graceful(&self) {
        // A data-only node (ADR 0035 PR4) has no local control WAL to flush —
        // `RaftNode::flush` only exists on a genuine local Raft replica.
        if let ControlHandle::Local(raft) = &self.raft {
            raft.flush().await;
        }
        self.edge.shutdown_all_cp_groups().await;
        self.shutdown_and_wait().await;
    }
}

/// Panic-unwind safety net (issue #279's panic half): a test that panics
/// mid-poll (a converged-or-timeout assert, say) drops its `Vec<Node>` with
/// no explicit `shutdown()` call at all, and the `#[tokio::test(multi_thread)]`
/// runtime's own teardown then hard-cancels every still-live driver task —
/// including one sitting mid-`tokio::fs` op — moments later, with nothing
/// having latched any hosted CP group's `halted` flag first. That is the
/// identical unconditional-panic window bare [`Node::shutdown`]'s own doc
/// describes, just reached by a runtime's implicit teardown instead of an
/// explicit call.
///
/// Latching here closes it the same way: synchronously, unconditionally, and
/// first — before anything else in this drop glue (or the runtime's own
/// later cancellation) can touch a driver task.
/// [`ClusterEdgeState::halt_hosted_cp_groups`] is safe to call from `Drop`
/// specifically because it bottoms out in `RaftKvNode::shutdown`, which is a
/// plain `AtomicBool` store plus two `Notify` wakes — no `.await`, no lock
/// held across one, no dependency on a live tokio runtime (`Drop` can run
/// inside or outside one), so it can never block or panic here.
///
/// Deliberately does **not** abort this node's own tasks or tear down its
/// envs — unlike `shutdown()`, a `Node` dropped without an explicit
/// `shutdown()` call still leaves its tasks running exactly as before this
/// fix (see `shutdown()`'s own doc); only the durability assert those tasks
/// can now safely race against an eventual abrupt stop is fixed.
impl Drop for Node {
    fn drop(&mut self) {
        self.edge.halt_hosted_cp_groups();
    }
}

/// How long [`Node::shutdown_and_wait`] polls for every aborted listener task
/// to report finished before giving up. Generous — this only ever matters
/// under heavy host-level contention — but bounded so a caller can never hang
/// forever on a task that, for some unforeseen reason, is never polled again.
/// Mirrors `animus_env::ProdEnv::shutdown_and_wait`'s identical constant one
/// layer down (this node's own listener tasks vs. each internal role env's).
const NODE_SHUTDOWN_WAIT_TIMEOUT: Duration = Duration::from_secs(10);

/// Poll `JoinHandle::is_finished` on every task until they've all reported
/// finished or [`NODE_SHUTDOWN_WAIT_TIMEOUT`] elapses, whichever comes
/// first — [`Node::shutdown_and_wait`]'s "actually wait for the abort to take
/// effect" step. Best-effort: a timeout here is silently swallowed (the tasks
/// were already aborted; the caller proceeds regardless), matching
/// `shutdown`'s existing fire-and-forget failure mode for the pathological
/// case, while still turning the common case into a genuine guarantee.
async fn wait_all_finished(tasks: &[tokio::task::JoinHandle<()>]) {
    let poll = async {
        loop {
            if tasks.iter().all(tokio::task::JoinHandle::is_finished) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    let _ = tokio::time::timeout(NODE_SHUTDOWN_WAIT_TIMEOUT, poll).await;
}

/// How long a graceful process teardown ([`Node::shutdown_graceful`], via
/// [`ClusterEdgeState::shutdown_all_cp_groups`]) waits for each hosted CP
/// group's driver to actually stop before giving up and proceeding to the
/// hard `abort()` anyway (the process is exiting either way). Also the bound
/// the per-node tablet-host reconciler's own teardown uses for the identical
/// shutdown-then-wait wait (`animus_cp_data::host::RECLAIM_STOP_TIMEOUT` —
/// kept as a separate constant here since this one guards an unrelated,
/// whole-process concern, not a single tablet's release/reclaim).
const CP_GC_STOP_TIMEOUT: Duration = Duration::from_secs(10);

/// A **control-only** node (ADR 0035 PR3) whose listeners are bound but not
/// yet started — the control-only counterpart of [`BoundNode`]. Binds the
/// one internal `ProdEnv` (ADR 0040 PR1) plus the client + admin TCP
/// listeners; no dynamo listener, no CP storage engine (a control node
/// never hosts a tablet or speaks a data-plane wire protocol). See
/// [`Node::bind_control`] to construct one and
/// [`start_control_with`](Self::start_control_with) to start it.
pub struct BoundControlNode {
    id: NodeId,
    env: ProdEnv,
    internal_addr: SocketAddr,
    client_listener: TcpListener,
    client_addr: SocketAddr,
    admin_listener: TcpListener,
    admin_addr: SocketAddr,
    intra_listener: TcpListener,
    intra_addr: SocketAddr,
}

impl BoundControlNode {
    /// The address clients connect to.
    pub fn client_addr(&self) -> SocketAddr {
        self.client_addr
    }

    /// The control-plane Raft listen address.
    pub fn control_addr(&self) -> SocketAddr {
        self.internal_addr
    }

    /// The address the admin / debug HTTP endpoint listens on (ADR 0020).
    pub fn admin_addr(&self) -> SocketAddr {
        self.admin_addr
    }

    /// The address the intra-cluster RPC endpoint listens on (ADR 0047).
    pub fn intra_addr(&self) -> SocketAddr {
        self.intra_addr
    }

    /// `(id, addr)` — this node's entry in the cluster's peer book.
    pub fn peer_entry(&self) -> (NodeId, SocketAddr) {
        (self.id.clone(), self.internal_addr)
    }

    /// Wire the peer address book into the control env and start the control
    /// role's protocols: the control [`RaftNode`] — its own `reconcile_loop`
    /// (placement) and `detect_loop` (failure detection) run **inside**
    /// `RaftNode::start` unconditionally, exactly as on a combined-mode node;
    /// both are pure control-plane logic that runs identically whether or not
    /// any data node exists yet — plus the tail every node shape shares
    /// ([`spawn_common_tail`]): `route_sync_loop`, `metrics_sample_loop`,
    /// this node's one-shot `register_node_addrs` self-registration (keyed by
    /// its own **control** id — a control-only node has no `raftkv` id), the
    /// plain client-request server, and the admin HTTP endpoint.
    ///
    /// Deliberately spawns **none** of: `bootstrap` (registers data
    /// members — combined-mode-only, ADR 0035 PR2), `peer_sync_loop` /
    /// `heartbeat_loop` (raftkv-env-specific — this node has no raftkv env to
    /// sync or heartbeat from), the tablet-host reconciler / `auto_split_loop`
    /// (nothing to host, no engine to sample), or the dynamo listener
    /// (never bound here). Every client-request dispatch path this node *can*
    /// reach (`Status`/`ProposeSchema`/`JoinInfo`/`SplitTablet`,
    /// and the data ops `Put`/`Get`/`Scan`/`Delete`/`PutBatch`) already works
    /// correctly with `ClientCtx.data == None`: the schema/admin ops only ever
    /// touch control `Metadata`, and a data op degrades exactly like any other
    /// node that hosts zero local replicas — it forwards via `client_route`
    /// (see `ClientCtx::resolve_cp_route`'s doc).
    ///
    /// `control_ids` is the control-plane Raft membership (this node's own
    /// control id must be a member of it — a control-only node's control
    /// group is never a non-voter/growth shape, unlike a data node's absent
    /// control role entirely). `client_route`/`cluster_admin_addrs` seed this
    /// node's forwarding table / dashboard fan-out exactly as
    /// [`BoundNode::start_with`]'s do; both are kept live thereafter by
    /// `route_sync_loop` / the replicated node address book.
    ///
    /// `backend` (ADR 0038) selects this control-only node's **dedicated**
    /// system-keyspace engine (`StorageBackend::Lsm` durable by default,
    /// `::Memory` under `--ephemeral`) — a control-only node has no separate
    /// `raftkv` env/dir the way a combined node's [`BoundNode::start_with`]
    /// does (which reuses its already-open *shared* engine), so this
    /// provisions a small engine just for `Metadata`, now the durable home
    /// of the apply task's published cache (`Metadata: DRIVER_APPLIED`)
    /// rather than an optional shadow mirror.
    ///
    /// `split_mode` (ADR 0058 Train 2 rung 3) selects which workflow this
    /// node's own `ClientCtx::trigger_split` proposes when it receives one
    /// (a relayed admin/client `SplitTablet` request, or a follower-
    /// connected `BeginSplit`/`BeginSplitInPlace` propose) — `SplitMode::
    /// Copy` is byte-for-byte the original ADR 0050 workflow.
    ///
    /// # Errors
    /// Propagates a failure to open the dedicated engine (LSM backend only).
    #[allow(clippy::too_many_arguments)]
    pub async fn start_control_with(
        self,
        peers: BTreeMap<NodeId, SocketAddr>,
        control_ids: Vec<NodeId>,
        client_route: BTreeMap<NodeId, SocketAddr>,
        intra_route: BTreeMap<NodeId, SocketAddr>,
        cluster_admin_addrs: Vec<SocketAddr>,
        backend: StorageBackend,
        orphan_sweep_after: Duration,
        split_mode: SplitMode,
    ) -> std::io::Result<Node> {
        self.env.set_peers(peers.clone());
        let envs = vec![self.env.clone()];

        let admin_info = Arc::new(AdminInfo {
            node_id: Some(self.id.clone()),
            internal_addr: Some(self.internal_addr),
            client_addr: self.client_addr,
            dynamo_addr: None,
            admin_addr: self.admin_addr,
            intra_addr: self.intra_addr,
            role: "control",
            control_ids: control_ids.clone(),
            peers: peers.clone(),
            admin_addrs: if cluster_admin_addrs.is_empty() {
                vec![self.admin_addr]
            } else {
                cluster_admin_addrs
            },
            auto_split_threshold: None,
            auto_split_bytes_threshold: None,
        });

        let control_metrics = self.env.metrics();
        // A control-only node has only its own one env/dir to open a
        // dedicated engine on — clone it for the engine, keep the original
        // for the `RaftNode` itself.
        let engine_env = self.env.clone();
        // Keep a clone for peer-sync (below) before the env is consumed.
        let sync_env = self.env.clone();
        // Keep a clone of this control-only node's dedicated engine for admin
        // introspection (`/admin/storage/control`, ADR 0038 PR4) — a second,
        // read-only handle onto the same live engine; the apply task's own
        // handle (moved into `RaftNode::start_with_metrics` below) stays the
        // sole writer.
        let (raft, control_storage) = match backend {
            StorageBackend::Lsm => match LsmEngine::open(engine_env, SYSKV_LSM_PREFIX).await {
                Ok(lsm) => (
                    RaftNode::start_with_orphan_sweep_after(
                        self.env,
                        control_ids.clone(),
                        control_metrics,
                        lsm.clone(),
                        animus_control::DeltaRing::default(),
                        orphan_sweep_after,
                    ),
                    SharedEngine::Lsm(lsm),
                ),
                Err(e) => {
                    return Err(std::io::Error::other(format!(
                        "opening the control-only node's system-keyspace engine: {e}"
                    )));
                }
            },
            StorageBackend::Memory => {
                let mem = MemoryEngine::new();
                (
                    RaftNode::start_with_orphan_sweep_after(
                        self.env,
                        control_ids.clone(),
                        control_metrics,
                        mem.clone(),
                        animus_control::DeltaRing::default(),
                        orphan_sweep_after,
                    ),
                    SharedEngine::Mem(mem),
                )
            }
        };
        // A fresh, node-local edge state (ADR 0031 PR2 doctrine — every node
        // gets its own, never shared); it stays permanently empty of CP group
        // handles (`raftkv`) since this node hosts none, but `register_control`
        // still lets `propose_schema` (and the client dispatch paths above)
        // propose locally when this node is the control leader.
        let edge = ClusterEdgeState::new();
        edge.register_control(raft.clone());

        let (ctx, mut tasks) = spawn_common_tail(
            ControlHandle::Local(raft.clone()),
            edge,
            None,
            admin_info,
            client_route,
            intra_route,
            (
                self.id,
                NodeAddrs {
                    internal: self.internal_addr.to_string(),
                    client: self.client_addr.to_string(),
                    admin: self.admin_addr.to_string(),
                    intra: self.intra_addr.to_string(),
                    role: "control".to_string(),
                },
            ),
            self.client_listener,
            self.admin_listener,
            self.intra_listener,
            None, // ADR 0052: a control-only node hosts no CP-data tablet, so it binds no console listener.
            Some(control_storage),
            sync_env.clone(),
            // A control-only node never binds the dynamo listener (ADR
            // 0057) — nothing here would ever read `ClientCtx::dynamo_auth`.
            None,
            split_mode,
        );

        // Peer-sync loop (ADR 0040 PR1) — a control-only node needs it
        // exactly as much as a combined node does, to reach a runtime-added
        // control voter's address.
        tasks.push(tokio::spawn(peer_sync_loop(ctx.clone(), sync_env, peers)));

        // The segment janitor (ADR 0043 §A9, round-3 PR7): a control-only
        // node can genuinely become the control-plane leader (ADR 0035
        // split deployment), so it needs this loop too — retention
        // *marking* and the drop-table retention-zero rule need only
        // `Metadata`, which this node has. See `segment_janitor.rs`'s own
        // doc for the documented gap this leaves (phases 2/3 — object
        // deletion and replica repair — need a `SegmentStoreHandle`, which
        // no control-only node provisions; a **pure** split deployment
        // therefore never runs those two phases today). No CLI/config knob
        // exists yet for a control-only node's own retention period —
        // mirroring `StreamSealKnobs`/`SegmentStoreConfig`'s own documented
        // "the split-deployment CLI path is a named follow-up" precedent —
        // so this always uses the production default.
        tasks.push(tokio::spawn(segment_janitor::segment_janitor_loop(
            ctx.clone(),
            DEFAULT_STREAM_RETENTION,
        )));

        // The secondary-index backfill-completion aggregator (ADR 0045 §4):
        // a control-only node can genuinely become the control-plane leader
        // (ADR 0035 split deployment), and — unlike the segment janitor —
        // this loop has no data-role dependency at all, so it needs no
        // documented scope gap here.
        tasks.push(tokio::spawn(index_backfill::index_backfill_loop(
            ctx.clone(),
        )));

        Ok(Node {
            raft: ControlHandle::Local(raft),
            envs,
            tasks,
            edge: ctx.edge.clone(),
            client_addr: self.client_addr,
            dynamo_addr: None,
            admin_addr: self.admin_addr,
            intra_addr: self.intra_addr,
            console_addr: None, // ADR 0052: a control-only node hosts no CP-data tablet.
            #[cfg(test)]
            test_ctx: ctx,
        })
    }
}

/// A **data-only** node (ADR 0035 PR4) whose listeners are bound but not yet
/// started — the data-only counterpart of [`BoundNode`] (which is
/// [`BoundControlNode`]'s own dual). Binds the one internal `ProdEnv` (ADR
/// 0040 PR1) plus the client/dynamo/admin TCP listeners; no local
/// control `RaftCore`, no bootstrap. See [`Node::bind_data`] to
/// construct one and [`start_data_with`](Self::start_data_with) to start it.
pub struct BoundDataNode {
    id: NodeId,
    env: ProdEnv,
    /// See [`BoundNode::dir`]'s doc — the identical local segment-store
    /// building-block rationale.
    dir: PathBuf,
    internal_addr: SocketAddr,
    client_listener: TcpListener,
    client_addr: SocketAddr,
    dynamo_listener: TcpListener,
    dynamo_addr: SocketAddr,
    admin_listener: TcpListener,
    admin_addr: SocketAddr,
    intra_listener: TcpListener,
    intra_addr: SocketAddr,
    /// The AnimusDB Data Console's own listener (ADR 0052) — a data-only
    /// node hosts real CP-data tablets, so it always binds one; see
    /// [`console`](crate::console)'s module doc.
    console_listener: TcpListener,
    console_addr: SocketAddr,
}

impl BoundDataNode {
    /// The address clients connect to.
    pub fn client_addr(&self) -> SocketAddr {
        self.client_addr
    }

    /// The address the DynamoDB JSON/HTTP endpoint listens on.
    pub fn dynamo_addr(&self) -> SocketAddr {
        self.dynamo_addr
    }

    /// The address the admin / debug HTTP endpoint listens on (ADR 0020).
    pub fn admin_addr(&self) -> SocketAddr {
        self.admin_addr
    }

    /// The address the AnimusDB Data Console listens on (ADR 0052).
    pub fn console_addr(&self) -> SocketAddr {
        self.console_addr
    }

    /// The address the intra-cluster RPC endpoint listens on (ADR 0047).
    pub fn intra_addr(&self) -> SocketAddr {
        self.intra_addr
    }

    /// `(id, addr)` — this node's entry in the cluster's *raftkv* peer
    /// book (the [`BoundNode::peer_entries`] dual, minus the `control` entry
    /// a data-only node has none of).
    pub fn peer_entry(&self) -> (NodeId, SocketAddr) {
        (self.id.clone(), self.internal_addr)
    }

    /// Wire the peer address book into the `raftkv` env and start the data
    /// role's protocols: **no local control `RaftCore` at all** — this node's
    /// [`ControlHandle`] is [`Remote`](ControlHandle::Remote), reaching the
    /// separately-deployed control plane exclusively via `control_seeds`
    /// (its **client**-API addresses — the discovery root for the mirror
    /// sync loop, the leader-hint-directed live fetch, and `propose_schema`'s
    /// relay/broadcast tiers, ADR 0035 §1/§4).
    ///
    /// `peers` is this node's **raftkv env's** peer book — per
    /// `ClusterConfig::control_peer_book`'s doc, this must be the *union* of
    /// the data fleet's own raftkv addresses and the control deployment's
    /// control addresses (`ClusterConfig::peer_book`), not
    /// `raftkv_peer_book()` alone: `heartbeat_loop` (below) sends
    /// `RaftMsg::Heartbeat` to `control_ids` over this very env, and those
    /// ids resolve through `peers`, not through `control_seeds` (a separate,
    /// client-API-address axis entirely — the internal `Env` `Network` never
    /// touches a client port). `control_ids` is the control deployment's
    /// control-plane Raft membership (the failure-detection heartbeat
    /// target); it plays no role in address resolution.
    ///
    /// Otherwise mirrors [`BoundNode::start_with`]'s data-role assembly
    /// exactly (the shared storage engine, the tablet-host reconciler, the
    /// dynamo listener) minus everything control-plane-specific
    /// (`bootstrap`, `edge.register_control`) — see that method's doc for
    /// what each shared piece does. `spawn_common_tail` still runs
    /// unconditionally (`route_sync_loop`/`metrics_sample_loop`/this node's
    /// own `register_node_addrs` self-registration/`serve_requests` (both
    /// listeners)/`admin::serve`), and this node's own `admin_add_member` self-registers
    /// its membership exactly like an ADR 0030 growth node's does (relayed —
    /// a data-only node can never satisfy `propose_schema`'s local-leader
    /// branch itself).
    ///
    /// # Errors
    /// Propagates a failure to open the CP group's on-disk engine (LSM
    /// backend only).
    #[allow(clippy::too_many_arguments)] // node assembly: mirrors `BoundNode::start_with`'s arity
    #[allow(clippy::too_many_arguments)]
    pub async fn start_data_with(
        self,
        peers: BTreeMap<NodeId, SocketAddr>,
        control_ids: Vec<NodeId>,
        control_seeds: Vec<SocketAddr>,
        backend: StorageBackend,
        edge: ClusterEdgeState,
        client_route: BTreeMap<NodeId, SocketAddr>,
        intra_route: BTreeMap<NodeId, SocketAddr>,
        auto_split_threshold: Option<usize>,
        auto_split_bytes_threshold: Option<u64>,
        cluster_admin_addrs: Vec<SocketAddr>,
    ) -> std::io::Result<Node> {
        self.start_data_with_streams(
            peers,
            control_ids,
            control_seeds,
            backend,
            edge,
            client_route,
            intra_route,
            auto_split_threshold,
            auto_split_bytes_threshold,
            cluster_admin_addrs,
            StreamSealKnobs::default(),
            SegmentStoreConfig::default(),
        )
        .await
    }

    /// Like [`start_data_with`](Self::start_data_with) — see
    /// [`BoundNode::start_with_streams`]'s doc for the layered-wrapper
    /// rationale. Defaults [`start_data_with_growth`](Self::start_data_with_growth)'s
    /// own `auto_split_change_rate` to `None`.
    #[allow(clippy::too_many_arguments)]
    pub async fn start_data_with_streams(
        self,
        peers: BTreeMap<NodeId, SocketAddr>,
        control_ids: Vec<NodeId>,
        control_seeds: Vec<SocketAddr>,
        backend: StorageBackend,
        edge: ClusterEdgeState,
        client_route: BTreeMap<NodeId, SocketAddr>,
        intra_route: BTreeMap<NodeId, SocketAddr>,
        auto_split_threshold: Option<usize>,
        auto_split_bytes_threshold: Option<u64>,
        cluster_admin_addrs: Vec<SocketAddr>,
        stream_seal_knobs: StreamSealKnobs,
        segment_store_config: SegmentStoreConfig,
    ) -> std::io::Result<Node> {
        self.start_data_with_growth(
            peers,
            control_ids,
            control_seeds,
            backend,
            edge,
            client_route,
            intra_route,
            auto_split_threshold,
            auto_split_bytes_threshold,
            cluster_admin_addrs,
            stream_seal_knobs,
            segment_store_config,
            None,
            None,
            SplitMode::default(),
            BackupStoreConfig::default(),
        )
        .await
    }

    /// Like [`start_data_with_streams`](Self::start_data_with_streams), with
    /// the opt-in **change-rate** auto-split trigger — see
    /// [`BoundNode::start_with_growth`]'s doc for the full design (identical
    /// here; a data-only node runs the same `auto_split_loop`).
    ///
    /// `dynamo_auth` (ADR 0057) — see [`BoundNode::start_with_growth`]'s doc:
    /// same knob, same default-`None`-disables contract. A data-only node
    /// binds the dynamo listener (ADR 0035 PR4) just like a combined node,
    /// so this is threaded here too, not skipped.
    ///
    /// `split_mode` (ADR 0058 Train 2 rung 3) — see
    /// [`BoundNode::start_with_growth`]'s doc: same knob, same
    /// `SplitMode::Copy`-is-byte-for-byte-original contract.
    ///
    /// `backup_store_config` (ADR 0059 §1) — see [`BoundNode::
    /// start_with_growth`]'s doc: same knob, same default-`Cluster`
    /// contract. A data-only node gets a real, independently-configured
    /// backup store handle too (ADR 0059's own asymmetry is that a
    /// *control-only* node gets none — see [`BoundControlNode::
    /// start_control_with`], which takes no such parameter at all).
    #[allow(clippy::too_many_arguments)]
    pub async fn start_data_with_growth(
        self,
        peers: BTreeMap<NodeId, SocketAddr>,
        control_ids: Vec<NodeId>,
        control_seeds: Vec<SocketAddr>,
        backend: StorageBackend,
        edge: ClusterEdgeState,
        client_route: BTreeMap<NodeId, SocketAddr>,
        intra_route: BTreeMap<NodeId, SocketAddr>,
        auto_split_threshold: Option<usize>,
        auto_split_bytes_threshold: Option<u64>,
        cluster_admin_addrs: Vec<SocketAddr>,
        stream_seal_knobs: StreamSealKnobs,
        segment_store_config: SegmentStoreConfig,
        auto_split_change_rate: Option<u64>,
        dynamo_auth: Option<Arc<BTreeMap<String, String>>>,
        split_mode: SplitMode,
        backup_store_config: BackupStoreConfig,
    ) -> std::io::Result<Node> {
        self.env.set_peers(peers.clone());
        let static_peers = peers;
        let sync_env = self.env.clone();
        let hook_env = self.env.clone();
        let hb_env = self.env.clone();
        let my_id = self.id.clone();
        let my_addr = self.internal_addr;
        let my_client_addr = self.client_addr;
        let my_admin_addr = self.admin_addr;
        let my_intra_addr = self.intra_addr;

        let control = ControlHandle::Remote(RemoteControlClient::new(control_seeds.clone()));

        let admin_info = Arc::new(AdminInfo {
            node_id: Some(self.id.clone()),
            internal_addr: Some(self.internal_addr),
            client_addr: self.client_addr,
            dynamo_addr: Some(self.dynamo_addr),
            admin_addr: self.admin_addr,
            intra_addr: self.intra_addr,
            role: "data",
            control_ids: control_ids.clone(),
            peers: static_peers.clone(),
            admin_addrs: if cluster_admin_addrs.is_empty() {
                vec![self.admin_addr]
            } else {
                cluster_admin_addrs
            },
            auto_split_threshold,
            auto_split_bytes_threshold,
        });

        let envs = vec![self.env.clone()];
        let raftkv_metrics = self.env.metrics();

        // Same shared-engine assembly as `BoundNode::start_with` — see that
        // method's doc.
        let storage = match backend {
            StorageBackend::Lsm => match LsmEngine::open(self.env.clone(), LSM_PREFIX).await {
                Ok(lsm) => SharedEngine::Lsm(lsm),
                Err(e) => {
                    return Err(std::io::Error::other(format!(
                        "opening the node's shared CP storage engine: {e}"
                    )));
                }
            },
            StorageBackend::Memory => SharedEngine::Mem(MemoryEngine::new()),
        };

        // This node's stream-shard segment store (ADR 0043 §A7b) — see
        // `BoundNode::start_with_streams`'s identical construction; `control`
        // here is `ControlHandle::Remote` (this node's own polled mirror),
        // which `ControlPlacementView` reads through unchanged.
        let segment_store = build_segment_store(
            &self.env,
            &self.dir,
            control.clone(),
            my_id.clone(),
            &segment_store_config,
        );
        // This node's backup store (ADR 0059 §1) — see `BoundNode::
        // start_with_growth`'s identical construction; `control` here is
        // `ControlHandle::Remote`, which `ControlPlacementView` reads
        // through unchanged, exactly as `segment_store` above does.
        let backup_store = build_backup_store(
            &self.env,
            &self.dir,
            control.clone(),
            my_id.clone(),
            &backup_store_config,
        );
        let data_role = DataRole {
            rmw_lock: Arc::new(tokio::sync::Mutex::new(())),
            raftkv_metrics,
            base_id: my_id.clone(),
            segment_store,
            backup_store,
            stream_seal_knobs,
            change_rates: ChangeRateTracker::default(),
            split_builds: Arc::new(Mutex::new(BTreeMap::new())),
        };
        let (ctx, mut tasks) = spawn_common_tail(
            control,
            edge.clone(),
            Some(data_role),
            admin_info,
            client_route,
            intra_route,
            (
                my_id.clone(),
                NodeAddrs {
                    internal: my_addr.to_string(),
                    client: my_client_addr.to_string(),
                    admin: my_admin_addr.to_string(),
                    intra: my_intra_addr.to_string(),
                    role: "data".to_string(),
                },
            ),
            self.client_listener,
            self.admin_listener,
            self.intra_listener,
            Some(self.console_listener),
            // A data-only node has no local control role at all (ADR 0035) —
            // no system-keyspace engine to surface (ADR 0038 PR4).
            None,
            self.env.clone(),
            dynamo_auth,
            split_mode,
        );

        // The per-node tablet-host reconciler (ADR 0031 PR4) — identical
        // shape to `BoundNode::start_with`'s.
        let reconciler = {
            let host_edge = edge.clone();
            let teardown_edge = edge.clone();
            let base_id = my_id.clone();
            let on_teardown = move |tablet: TabletId| {
                teardown_edge.unregister_raftkv(tablet, base_id.clone());
            };
            // ADR 0050 rung 1: the reconciler no longer receives the node's
            // shared engine — it opens ONE PRIVATE ENGINE PER HOSTED TABLET
            // through the factory seam (the node's `storage` above now backs
            // only the control plane's system keyspace, ADR 0038).
            match &storage {
                SharedEngine::Lsm(_) => CpReconciler::Lsm(Reconciler::new(
                    hook_env.clone(),
                    LsmTabletFactory { env: hook_env },
                    my_id.clone(),
                    move |tablet, node: &RaftKvNode<ProdEnv, LsmEngine<ProdEnv>>| {
                        host_edge.register_raftkv(tablet, CpGroup::Lsm(node.clone()));
                    },
                    on_teardown,
                )),
                SharedEngine::Mem(_) => CpReconciler::Mem(Reconciler::new(
                    hook_env,
                    MemoryTabletEngines::new(),
                    my_id.clone(),
                    move |tablet, node: &RaftKvNode<ProdEnv, MemoryEngine>| {
                        host_edge.register_raftkv(tablet, CpGroup::Mem(node.clone()));
                    },
                    on_teardown,
                )),
            }
        };

        // No `bootstrap` — a data-only node holds no control-plane Raft role
        // to register members against; that is entirely the control
        // deployment's own concern (its `bootstrap`, run by the combined-mode
        // or control-only nodes that actually host it).

        tasks.push(tokio::spawn(peer_sync_loop(
            ctx.clone(),
            sync_env,
            static_peers.clone(),
        )));

        // The generalized mirror + leader-hint sync loop (ADR 0035 §4) —
        // every data-only node's *only* way to see `Metadata` at all (unlike
        // an ADR 0030 growth node, this is never conditional: a data-only
        // node has no local control raft to ever be a "genuine voter"
        // instead).
        tasks.push(tokio::spawn(remote_metadata_sync_loop(
            ctx.clone(),
            control_seeds,
        )));

        // Self-registration (ADR 0032 PR2/PR4): a data-only node has no
        // local control leader to propose against, so this always relays —
        // `propose_schema`'s `leader_addr_hint`-then-`route_addr`-then-
        // broadcast tiers are this node's *only* path to the real cluster.
        {
            let ctx = ctx.clone();
            let node = my_id;
            tasks.push(tokio::spawn(async move {
                let _ = ctx.admin_add_member(node, BTreeMap::new()).await;
            }));
        }

        // Live destinations (ADR 0037 closing PR) — see `heartbeat_loop_live`'s
        // doc; on this data-only node `ctx.control` is `ControlHandle::Remote`,
        // so the live list comes from the last `Status`/`WatchMetadata` reply's
        // `control_voters`, falling back to this node's static `control_ids`
        // seed until the first one lands.
        tasks.push(tokio::spawn(heartbeat_loop_live(
            ctx.clone(),
            hb_env,
            control_ids,
        )));

        tasks.push(tokio::spawn(tablet_host_reconciler_loop(
            ctx.clone(),
            reconciler,
        )));

        // GSI drain (ADR 0041 §4): materializes global secondary indexes from
        // the change records indexed writes leave behind. Data-role-only and
        // per-tablet leadership-checked, exactly like `txn_resolver_loop` above
        // — a node that leads no tablet does nothing each tick.
        tasks.push(tokio::spawn(index_drain::change_consumer_loop(ctx.clone())));

        // The TTL reaper (ADR 0051 §4/§6) — same shape as the GSI drain
        // just above. No test-tunable interval knob on this data-only path
        // yet (mirrors `quiesce_after`'s own documented gap for
        // `start_data_with_growth`, `animusd/CLAUDE.md`'s Quiescence
        // section) — always the production default.
        tasks.push(tokio::spawn(ttl_reaper::ttl_reaper_loop(
            ctx.clone(),
            ttl_reaper::DEFAULT_TTL_SWEEP_INTERVAL,
        )));

        if auto_split_threshold.is_some()
            || auto_split_bytes_threshold.is_some()
            || auto_split_change_rate.is_some()
        {
            tasks.push(tokio::spawn(auto_split_loop(
                ctx.clone(),
                AutoSplitThresholds {
                    keys: auto_split_threshold,
                    bytes: auto_split_bytes_threshold,
                    change_rate: auto_split_change_rate,
                },
            )));
        }
        tasks.push(tokio::spawn(dynamo::serve(
            self.dynamo_listener,
            ctx.clone(),
        )));

        Ok(Node {
            raft: ctx.control.clone(),
            envs,
            tasks,
            edge: ctx.edge.clone(),
            client_addr: self.client_addr,
            dynamo_addr: Some(self.dynamo_addr),
            admin_addr: self.admin_addr,
            intra_addr: self.intra_addr,
            console_addr: Some(self.console_addr),
            #[cfg(test)]
            test_ctx: ctx,
        })
    }
}

/// The wire edges' mutable state, scoped to **one node** (ADR 0013; made
/// genuinely per-node by ADR 0031 PR2 — see the historical note below) rather
/// than to the whole process or, in `--cluster N`, the whole in-process
/// cluster. Holding it here — threaded through [`ClientCtx`] — instead of in
/// `OnceLock` process statics is what lets a test harness run several
/// independent clusters (and, within a cluster, several independent nodes) in
/// one process without their edge state leaking across each other
/// (registries, prepared statements, and the control/CP-group handles each
/// node registers).
///
/// Cloning shares the same underlying state (it is `Arc`-backed) — cheap, and
/// used to hand every connection *of the same node* the same handle set. A
/// fresh [`ClusterEdgeState::new`] is a distinct, isolated set, and
/// [`start_cluster_with`] (the `--cluster N` in-process bring-up) now creates
/// one **per node**, not one shared by the whole cluster.
///
/// **Historical note (ADR 0031 PR2):** before this change, `--cluster N`
/// created a *single* `ClusterEdgeState` shared by every in-process node —
/// convenient (any node's edge reached every other node's handles directly,
/// in-process), but it made every `edge.*` read answer "does *anyone* in the
/// cluster satisfy this" instead of "does *this node*" — masking real
/// cross-process leader-routing / DDL-relay / per-node-dedup bugs that only
/// showed up in a genuine one-process-per-node deployment (several are
/// recorded in the root `CLAUDE.md` Engineering Practices section). `--cluster
/// N` now behaves identically to one-process-per-node: every node gets its own
/// edge state, and cross-node reach happens only through the real
/// client-protocol forwarding (`cp_route`/`cp_forward`) and schema-DDL relay
/// (`propose_schema`) paths, both proven by the per-process test suite
/// already. A few fields below still carry stale "shared in `--cluster N`"
/// commentary describing that retired shape; treat any such comment as
/// historical, not current behavior.
#[derive(Clone)]
pub struct ClusterEdgeState {
    /// This **node's own** control `RaftNode` handle (at most one entry — see
    /// [`register_control`](Self::register_control)), so `propose_schema` can
    /// propose a schema `MetaCommand` **locally** when this node is the
    /// control-plane leader. When it isn't, `propose_schema` relays
    /// [`ClientRequest::ProposeSchema`] one hop to the leader's node via
    /// `intra_route` (ADR 0047; ADR 0013 originally routed this via
    /// `client_route`) — the same path every follower-connected DDL
    /// in a one-process-per-node deployment always used.
    control: Arc<Mutex<Vec<RaftNode<ProdEnv>>>>,
    /// The DynamoDB edge's in-memory GSI declarations + observation-built
    /// written-key index (ADR 0006). Not durable / not replicated; per-node.
    dynamo_registry: Arc<Mutex<animus_dynamo::SchemaRegistry>>,
    /// This **node's own** hosted **leaderful CP** per-tablet Raft group
    /// handles (ADR 0017 #3a), **keyed by tablet** so a wire edge routes a key
    /// to its owning tablet's group **leader** when this node hosts it, or
    /// forwards otherwise (`cp_route`/`client_route`). Each tablet maps to the
    /// handle(s) *this node* locally hosts for it (in practice at most one,
    /// since a node hosts at most one replica of a given tablet).
    raftkv: Arc<Mutex<BTreeMap<TabletId, Vec<CpGroup>>>>,
}

impl Default for ClusterEdgeState {
    fn default() -> Self {
        Self::new()
    }
}

impl ClusterEdgeState {
    /// A fresh, isolated edge-state set for one cluster.
    pub fn new() -> Self {
        Self {
            control: Arc::new(Mutex::new(Vec::new())),
            dynamo_registry: Arc::new(Mutex::new(animus_dynamo::SchemaRegistry::new())),
            raftkv: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Register a node's control handle for schema-proposal routing. Called once
    /// per node in [`BoundNode::start_with`].
    fn register_control(&self, raft: RaftNode<ProdEnv>) {
        self.control
            .lock()
            .expect("control handles poisoned")
            .push(raft);
    }

    /// Register a node's CP group handle for `tablet` (ADR 0017 #3a / Phase 2).
    /// Called on each node that hosts a replica of `tablet`.
    fn register_raftkv(&self, tablet: TabletId, cp: CpGroup) {
        self.raftkv
            .lock()
            .expect("raftkv handles poisoned")
            .entry(tablet)
            .or_default()
            .push(cp);
    }

    /// Remove and return this (node-local) edge's registered handle for
    /// `tablet` — the one whose env runs as group member `member` — dropping
    /// the tablet's entry once the last handle is gone (drop-table GC, ADR
    /// 0024). Matched per member id defensively (this edge only ever holds
    /// this node's own handles since ADR 0031 PR2, but a tablet could in
    /// principle have more than one locally-registered entry across its
    /// lifetime). `None` if no such handle is registered (e.g. the stand-up
    /// path claimed the tablet but has not registered yet — the caller
    /// retries on a later tick rather than GC-ing a group mid-standup).
    fn unregister_raftkv(&self, tablet: TabletId, member: NodeId) -> Option<CpGroup> {
        let mut map = self.raftkv.lock().expect("raftkv handles poisoned");
        let groups = map.get_mut(&tablet)?;
        let at = groups.iter().position(|g| g.env().node_id() == member)?;
        let group = groups.remove(at);
        if groups.is_empty() {
            map.remove(&tablet);
        }
        Some(group)
    }

    /// Synchronously latch **every** locally-registered CP group's `halted`
    /// flag (`CpGroup::shutdown` — a plain atomic store plus two `Notify`
    /// wakes, no I/O, no `.await`) and hand back the snapshot this took, so
    /// a caller that also needs to wait for the driver to actually exit
    /// (`shutdown_all_cp_groups`, below) can reuse it without a second lock
    /// round trip.
    ///
    /// This is the one shared first step **every** path that can abruptly
    /// stop a group's driver needs before it touches that driver at all
    /// (issues #282/#279): the graceful process teardown below, bare
    /// [`Node::shutdown`]/[`shutdown_and_wait`](Node::shutdown_and_wait)
    /// (a raw `task.abort()` + `ProdEnv::shutdown()`, the doc-blessed "kill
    /// node N" fault-injection idiom with no grace period at all), and
    /// [`Node`]'s `Drop` impl (a panicking test's `Vec<Node>` unwind, which
    /// leaves the driver tasks for the test runtime's own teardown to
    /// hard-cancel later, mid-I/O, with nothing having latched `halted` at
    /// all). Without this latch, an abruptly-cancelled driver can land
    /// inside `persist_wal`/`flush_pending`'s halted-gated I/O-error assert
    /// (`animus-cp-data`'s `CLAUDE.md`) with `halted` still `false` — an
    /// unconditional panic indistinguishable from a genuine live durability
    /// fault. Deliberately does **not** poll `is_stopped()` — that wait is
    /// this method's own caller's job when it needs one; every bare-abort
    /// caller above wants fire-and-forget, exactly like `CpGroup::shutdown`
    /// itself already promises.
    fn halt_hosted_cp_groups(&self) -> Vec<CpGroup> {
        let groups: Vec<CpGroup> = self
            .raftkv
            .lock()
            .expect("raftkv handles poisoned")
            .values()
            .flatten()
            .cloned()
            .collect();
        for group in &groups {
            group.shutdown();
        }
        groups
    }

    /// Gracefully halt every CP group registered here (process shutdown, not
    /// drop-table GC — see [`shutdown_graceful`](Node::shutdown_graceful)). A raw
    /// `ProdEnv::shutdown()` hard-`abort()`s the CP-data driver/apply tasks, which
    /// can land mid-`storage.merge(..).await` inside `apply_and_compact` and
    /// surface as a `tokio::fs` background-task panic
    /// (`Backend("background task failed")`/`Backend("task was cancelled")`) when
    /// the runtime's blocking pool is torn down underneath it. [`halt_hosted_cp_groups`](
    /// Self::halt_hosted_cp_groups) only latches a flag the driver observes *between*
    /// full apply passes, so we must poll [`is_stopped`](CpGroup::is_stopped) before
    /// the caller proceeds to abort anything else — the same shutdown-then-wait shape
    /// the per-node tablet-host reconciler's own teardown uses (ADR 0031 PR4) before
    /// deleting a dropped tablet's files. Bounded by `CP_GC_STOP_TIMEOUT`; a group
    /// that doesn't stop in time is logged and left for the subsequent hard abort
    /// (the process is exiting either way).
    async fn shutdown_all_cp_groups(&self) {
        let groups = self.halt_hosted_cp_groups();
        let deadline = tokio::time::Instant::now() + CP_GC_STOP_TIMEOUT;
        for group in &groups {
            while !group.is_stopped() {
                if tokio::time::Instant::now() >= deadline {
                    tracing::warn!("shutdown: a CP group driver did not stop in time");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    /// The CP group handle for `tablet` that currently believes it is leader, if
    /// any. The route target for a key in `tablet`'s range. Normally exactly one
    /// registered handle leads; a deposed leader's `linearizable_get` returns `None`
    /// (never stale) and its `put` returns `NotLeader`, so picking the first
    /// self-styled leader is safe.
    fn cp_leader(&self, tablet: TabletId) -> Option<CpGroup> {
        self.raftkv
            .lock()
            .expect("raftkv handles poisoned")
            .get(&tablet)?
            .iter()
            .find(|n| n.is_leader())
            .cloned()
    }

    /// Any locally-registered CP group handle for `tablet` (the first), regardless
    /// of leadership — used to read the group's current leader *hint* for
    /// cross-process forwarding (ADR 0017 #3b). `None` if this node hosts no replica
    /// of `tablet`.
    fn local_cp(&self, tablet: TabletId) -> Option<CpGroup> {
        self.raftkv
            .lock()
            .expect("raftkv handles poisoned")
            .get(&tablet)?
            .first()
            .cloned()
    }

    /// Every CP group this node hosts, as `(tablet, group)` pairs in tablet order
    /// — for the admin `/admin/raftkv` view (ADR 0020). Clones the cheap handles.
    fn hosted_groups(&self) -> Vec<(TabletId, CpGroup)> {
        self.raftkv
            .lock()
            .expect("raftkv handles poisoned")
            .iter()
            .flat_map(|(t, groups)| groups.iter().map(move |g| (*t, g.clone())))
            .collect()
    }

    /// The control handle that currently believes it is leader, if any.
    pub(crate) fn leader_handle(&self) -> Option<RaftNode<ProdEnv>> {
        self.control
            .lock()
            .expect("control handles poisoned")
            .iter()
            .find(|r| r.is_leader())
            .cloned()
    }

    /// The DynamoDB edge's per-node registry.
    pub(crate) fn dynamo_registry(&self) -> &Arc<Mutex<animus_dynamo::SchemaRegistry>> {
        &self.dynamo_registry
    }
}

/// The DynamoDB Streams sealer's own knobs (ADR 0042 §13, F6): size/age seal
/// triggers, evaluated by the per-tablet `change_consumer_loop`'s seal arm
/// (`index_drain.rs`). `Default` gives the ADR's own documented production
/// defaults; a test constructs a tiny-knobbed value directly (this
/// codebase's house testing discipline — see `--auto-split-bytes`'s own
/// precedent — never the production defaults, or a size/age-triggered test
/// would need to write megabytes/wait hours to trip).
#[derive(Clone, Copy, Debug)]
pub struct StreamSealKnobs {
    /// `--stream-seal-bytes`: seal once a led tablet's `KIND_CHANGE` scope's
    /// approximate size (`CpGroup::approx_bytes`) exceeds this many bytes.
    pub seal_bytes: u64,
    /// `--stream-seal-age`: seal once the oldest unsealed `KIND_CHANGE`
    /// record's age — measured against the loop's own `env` clock, never
    /// `std::time` directly (ADR 0003) — exceeds this.
    pub seal_age: Duration,
}

impl Default for StreamSealKnobs {
    fn default() -> Self {
        StreamSealKnobs {
            seal_bytes: 4 * 1024 * 1024,
            seal_age: Duration::from_secs(4 * 60 * 60),
        }
    }
}

/// The segment janitor's own retention grace period (ADR 0042 §13/ADR 0043
/// §A9, `--stream-retention`, round-3 PR7): a catalog row past this age
/// (measured from its own `seal_wall_ms`, the loop's `env` clock) becomes
/// eligible for the two-phase reclaim. The ADR's own documented production
/// default; a test constructs a tiny value directly (this codebase's house
/// testing discipline — see [`StreamSealKnobs::default`]'s own precedent).
pub const DEFAULT_STREAM_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);

/// **The `--quiesce-after` correctness floor (issue #302 fix).** A nonzero
/// `quiesce_after` shorter than this can reintroduce the stale-veto race the
/// fix closes: `RaftCore::quiesce_entry_ok`'s freshness clause only rejects
/// an observation that's gone stale *since it was made* — it has no teeth
/// against a tablet `change_consumer_loop` has never observed at all (see
/// that field's own doc for why the "never engaged" sentinel must impose no
/// constraint). The remaining soundness argument is structural: the loop's
/// own period bounds how long a hosted tablet can go unobserved, so as long
/// as `quiesce_after` gives it at least one full period of headroom before
/// the group's *own* idle-clock clause could first fire, at least one real
/// sweep is guaranteed to have landed first. `0` (disabling quiescence
/// entirely) is exempt — this floor only constrains a genuinely-enabled
/// value. `main`'s CLI parsing rejects a smaller `--quiesce-after` outright;
/// `Node::start_with_growth` `debug_assert`s it too, as a second layer for
/// any caller that reaches `enable_quiescence` without going through the
/// CLI (a test, or a future embedder).
pub const MIN_QUIESCE_AFTER: Duration = index_drain::INDEX_DRAIN_INTERVAL;

/// This node's stream-shard [`SegmentStore`](animus_env::SegmentStore) handle
/// (ADR 0043 §A7b) — either the **default**
/// [`ClusterSegmentStore`](animus_cp_data::cluster_segment_store::ClusterSegmentStore)
/// (K-way replicated across nodes' own local segment directories, each
/// backed by [`FsSegmentStore`]) or, opted into via `--segment-store
/// dir:PATH`, a bare single-directory [`FsSegmentStore`] — dev use, or a
/// genuinely shared mount every node in the cluster can reach at the
/// identical path (the caveat `--segment-store`'s own CLI doc names: this
/// mode gives up the K-replication durability upgrade F5 mandates for the
/// *default*, in exchange for needing no cluster wiring at all — a single
/// shared filesystem is its own, external, single point of failure/
/// consistency the operator is choosing to accept).
#[derive(Clone)]
pub(crate) enum SegmentStoreHandle {
    Cluster(animus_cp_data::cluster_segment_store::ClusterSegmentStore<ProdEnv, FsSegmentStore>),
    Fs(FsSegmentStore),
}

impl SegmentStoreHandle {
    /// Push a sealed segment's bytes durably to this store (the sealer's own
    /// `SegmentStore::put`, ADR 0043 §A3 step 2), returning the replica set
    /// to record in the `SealStreamShard` catalog row's own `replicas`
    /// field (ADR 0043 §A3 step 3) — the **cluster** store's own sorted
    /// K-replica set, or an **empty** one for the single-directory
    /// `FsSegmentStore` opt-in: there is no per-node replica concept for a
    /// store every node already reads the identical physical directory
    /// through, so an empty `replicas` list is this PR's documented signal
    /// for "no cluster replica set — ask any node" (the read path, a later
    /// PR, is what interprets it).
    async fn put_sealed(&self, id: &str, bytes: &[u8]) -> std::io::Result<Vec<NodeId>> {
        match self {
            SegmentStoreHandle::Cluster(c) => c.put_replicated(id, bytes).await,
            SegmentStoreHandle::Fs(fs) => {
                use animus_env::SegmentStore;
                fs.put(id, bytes).await?;
                Ok(Vec::new())
            }
        }
    }

    /// Fetch a sealed segment's bytes (PR6's `GetRecords` sealed-shard read
    /// path, ADR 0042/0043 §A7b) — served by **any** node, no forwarding,
    /// since the segment store's own `get`/`get_from` already fan out to a
    /// live replica. `replicas` is the catalog row's own recorded set
    /// (`StreamShardRow::replicas`); for the single-directory
    /// `FsSegmentStore` opt-in there is no per-node replica concept (every
    /// node already reads the identical shared directory), so `replicas` is
    /// ignored there — the empty list `put_sealed` records for that variant
    /// is exactly this "ask any node" signal. `Ok(None)` means the object is
    /// genuinely gone (deleted by the retention sweep) — a `TrimmedDataAccess`
    /// outcome to the client, never an error.
    pub(crate) async fn get_sealed(
        &self,
        replicas: &[NodeId],
        id: &str,
    ) -> std::io::Result<Option<Vec<u8>>> {
        use animus_env::SegmentStore;
        match self {
            SegmentStoreHandle::Cluster(c) => c.get_from(replicas, id).await,
            SegmentStoreHandle::Fs(fs) => fs.get(id).await,
        }
    }

    /// Delete a sealed segment's object at every one of `replicas` (the
    /// segment janitor's own reclaim step, ADR 0043 §A9, round-3 PR7):
    /// idempotent, all-or-error at the recorded `replicas` list — see
    /// [`ClusterSegmentStore::delete_from`]'s own doc for the exact
    /// contract. For the single-directory `Fs` opt-in, `replicas` is
    /// ignored — every node already shares the identical directory, so a
    /// plain local delete is the whole cluster's delete (mirroring
    /// `get_sealed`'s identical "replicas ignored" convention there).
    ///
    /// [`ClusterSegmentStore::delete_from`]: animus_cp_data::cluster_segment_store::ClusterSegmentStore::delete_from
    pub(crate) async fn delete_sealed(&self, replicas: &[NodeId], id: &str) -> std::io::Result<()> {
        use animus_env::SegmentStore;
        match self {
            SegmentStoreHandle::Cluster(c) => c.delete_from(replicas, id).await,
            SegmentStoreHandle::Fs(fs) => fs.delete(id).await,
        }
    }

    /// Re-replicate a live shard's object to enough freshly-chosen targets
    /// to restore `target_k` (the segment janitor's own replica-repair
    /// step, ADR 0043 §A9, round-3 PR7) — delegates to
    /// [`ClusterSegmentStore::repair`] for the default `Cluster` variant
    /// (see that method's doc for the degraded-mode/candidate-exclusion
    /// contract); a bare `Ok(surviving.to_vec())` no-op for the
    /// single-directory `Fs` opt-in, since there is no per-node replica
    /// concept to repair there at all — every node already reads the
    /// identical shared directory, so "repair" is meaningless (the
    /// janitor's own caller never calls this for an `Fs`-backed row in the
    /// first place: such a row's own `replicas` field is always empty, the
    /// signal `put_sealed`/`get_sealed` already document).
    ///
    /// [`ClusterSegmentStore::repair`]: animus_cp_data::cluster_segment_store::ClusterSegmentStore::repair
    pub(crate) async fn repair_replicas(
        &self,
        id: &str,
        bytes: &[u8],
        surviving: &[NodeId],
        target_k: usize,
    ) -> std::io::Result<Vec<NodeId>> {
        match self {
            SegmentStoreHandle::Cluster(c) => c.repair(id, bytes, surviving, target_k).await,
            SegmentStoreHandle::Fs(_) => Ok(surviving.to_vec()),
        }
    }

    /// List every id starting with `prefix` on **this node's own local**
    /// segment directory (the segment janitor's orphan sweep, ADR 0042
    /// §10/ADR 0043 §A3 as-built amendment) — never cluster-wide, mirroring
    /// [`SegmentStore::list`](animus_env::SegmentStore::list)'s own
    /// documented "local-only, debug/sweep-only" contract. For the
    /// `Cluster` variant this deliberately bypasses replication/placement
    /// entirely (`ClusterSegmentStore::local()`), so a single tick only
    /// ever discovers this one node's own copies — see the orphan sweep's
    /// own doc for why that is an accepted, honestly-documented limitation
    /// rather than a bug.
    pub(crate) async fn list_local(&self, prefix: &str) -> std::io::Result<Vec<String>> {
        use animus_env::SegmentStore;
        match self {
            SegmentStoreHandle::Cluster(c) => c.local().list(prefix).await,
            SegmentStoreHandle::Fs(fs) => fs.list(prefix).await,
        }
    }

    /// Fetch `id` from **this node's own local** segment directory — the
    /// orphan sweep's own read, paired with [`list_local`](Self::list_local)
    /// (an id `list_local` just returned is, by construction, already local
    /// to this same store).
    pub(crate) async fn get_local(&self, id: &str) -> std::io::Result<Option<Vec<u8>>> {
        use animus_env::SegmentStore;
        match self {
            SegmentStoreHandle::Cluster(c) => c.local().get(id).await,
            SegmentStoreHandle::Fs(fs) => fs.get(id).await,
        }
    }

    /// Delete `id` from **this node's own local** segment directory only —
    /// the orphan sweep's own reclaim step. Deliberately not a
    /// cluster-replicated delete (`delete_from`): an orphan was never
    /// cataloged, so there is no `replicas` set to consult, and each node
    /// that ever becomes the control leader sweeps its own local copies as
    /// leadership rotates (see [`list_local`](Self::list_local)'s doc).
    pub(crate) async fn delete_local(&self, id: &str) -> std::io::Result<()> {
        use animus_env::SegmentStore;
        match self {
            SegmentStoreHandle::Cluster(c) => c.local().delete(id).await,
            SegmentStoreHandle::Fs(fs) => fs.delete(id).await,
        }
    }
}

/// A [`PlacementView`](animus_cp_data::cluster_segment_store::PlacementView)
/// backed by this node's own control handle (ADR 0043 §A7b's wiring PR): the
/// current candidate set is every member this node's replicated `Metadata`
/// currently believes `Active` — the same "live, data-capable member" pool
/// `ClientCtx::provision_tablet`'s own initial replica-set selection draws
/// from. Deliberately label-blind, matching `cluster_segment_store.rs`'s own
/// module doc (`choose_targets`'s policy is already label-blind today) — a
/// future PR that wants failure-domain-aware segment placement would read
/// each candidate's real `Metadata.node_addrs`/member labels here too,
/// without changing the trait's shape. Uses `metadata_cached()`, not
/// `effective_metadata()`: `PlacementView::candidates` is a **synchronous**
/// trait method with no `.await` point to reach a growth node's polled
/// mirror through, and `ClusterSegmentStore` is not wired onto a control-
/// plane-follower-less growth node in this PR anyway (see
/// [`BoundNode::start_with_streams`]'s own doc).
#[derive(Clone)]
struct ControlPlacementView {
    control: ControlHandle,
    self_id: NodeId,
}

impl animus_cp_data::cluster_segment_store::PlacementView for ControlPlacementView {
    fn self_id(&self) -> NodeId {
        self.self_id.clone()
    }

    fn candidates(&self) -> Vec<NodeId> {
        self.control
            .metadata_cached()
            .members
            .iter()
            .filter(|(_, m)| m.status == NodeStatus::Active)
            .map(|(id, _)| id.clone())
            .collect()
    }
}

/// `--segment-store` CLI opt-in (ADR 0043 §A7b): the default,
/// [`SegmentStoreConfig::Cluster`], selects [`SegmentStoreHandle::Cluster`]
/// (the K-replicated default store, F5's durability mandate);
/// `Fs(PATH)` (parsed by `main.rs` from `--segment-store dir:PATH`) selects a
/// bare, single-directory `FsSegmentStore` at `PATH` instead — dev use, or a
/// directory every node in the cluster mounts at the identical path (NFS or
/// similar). **The shared-mount caveat**: this opt-in trades away the
/// K-replication durability upgrade the *default* store exists to provide
/// (ADR 0043's whole "the default store must uphold this database's own
/// durability bar" argument) for needing no cluster wiring at all — the
/// shared filesystem itself becomes a single point of failure/consistency
/// this adapter no longer protects against, which is exactly the trade a dev
/// setup or an operator with its own already-durable shared storage is
/// choosing to accept.
#[derive(Clone, Debug, Default)]
pub enum SegmentStoreConfig {
    #[default]
    Cluster,
    Fs(PathBuf),
}

/// Build (and, for the cluster variant, **start**) this node's
/// [`SegmentStoreHandle`] (ADR 0043 §A7b) per `config`. `dir` is this node's
/// own data directory ([`BoundNode::dir`]/[`BoundDataNode::dir`]) — the
/// cluster variant's per-node local `FsSegmentStore` building block roots at
/// `dir.join("segments")`, a sibling of the `internal/` subdirectory
/// `ProdEnv::bind` already owns.
fn build_segment_store(
    env: &ProdEnv,
    dir: &Path,
    control: ControlHandle,
    self_id: NodeId,
    config: &SegmentStoreConfig,
) -> SegmentStoreHandle {
    match config {
        SegmentStoreConfig::Cluster => {
            let local = FsSegmentStore::new(dir.join("segments"));
            let placement: Arc<dyn animus_cp_data::cluster_segment_store::PlacementView> =
                Arc::new(ControlPlacementView { control, self_id });
            SegmentStoreHandle::Cluster(
                animus_cp_data::cluster_segment_store::ClusterSegmentStore::start(
                    env.clone(),
                    local,
                    placement,
                    animus_cp_data::cluster_segment_store::SEGMENT_STREAM,
                ),
            )
        }
        SegmentStoreConfig::Fs(path) => SegmentStoreHandle::Fs(FsSegmentStore::new(path.clone())),
    }
}

/// This node's **backup** [`SegmentStore`](animus_env::SegmentStore) handle
/// (ADR 0059 §1) — a second, backup-dedicated instance built the same way
/// [`SegmentStoreHandle`] is (`ClusterSegmentStore<ProdEnv, FsSegmentStore>`/
/// `FsSegmentStore` — this crate has no `SimEnv` dependency at all, ADR 0043
/// §A7b's `SimSegmentStore` variant is `animus-cp-data`'s own sim-corpus
/// concern, never reached from here), but from its own `--backup-store` CLI
/// knob and its own object namespace
/// (`animus_cp_data::backup`'s `backup/{backup_id}/...` ids, disjoint from
/// the stream sealer's `{table}/{label}/{tablet}/{epoch}` shape — see that
/// module's own doc). **Plumbing only** (ADR 0059 Train 1 PR②): nothing yet
/// reads or writes through this handle — no capture driver, no janitor, no
/// wire surface — it is threaded down to where a later PR's capture driver
/// will live (alongside [`DataRole::segment_store`]) and no further.
///
/// A distinct type from [`SegmentStoreHandle`], not a second value of that
/// same type, so a reader can never mix up which knob/object-namespace a
/// given handle answers for — the two are mechanically identical today
/// (same variant shapes, same underlying store types) but are documented,
/// configured, and will evolve independently (ADR 0059 §1's own
/// `fs:PATH`-durability-tradeoff note is a backup-specific operational
/// concern the streams knob doesn't share).
// `#[allow(dead_code)]` on the enum and its impl below: ADR 0059 Train 1 PR②
// ships this handle with no reader at all — the capture driver that will
// actually `put`/`get`/`delete`/`list_local` through it is a later PR. Left
// in (rather than stubbed to a unit-shaped placeholder) so that PR's diff is
// "wire in a caller," not "also invent this type" — see the module-level
// doc above for the full rationale.
#[allow(dead_code)]
#[derive(Clone)]
pub(crate) enum BackupStoreHandle {
    Cluster(animus_cp_data::cluster_segment_store::ClusterSegmentStore<ProdEnv, FsSegmentStore>),
    Fs(FsSegmentStore),
}

#[allow(dead_code)]
impl BackupStoreHandle {
    /// Push a backup object's bytes durably to this store, returning the
    /// replica set a later PR's catalog bookkeeping would record — mirrors
    /// [`SegmentStoreHandle::put_sealed`]'s exact contract (an empty
    /// `Vec` for the single-directory `Fs` opt-in, the same "no per-node
    /// replica concept, ask any node" signal).
    pub(crate) async fn put(&self, id: &str, bytes: &[u8]) -> std::io::Result<Vec<NodeId>> {
        match self {
            BackupStoreHandle::Cluster(c) => c.put_replicated(id, bytes).await,
            BackupStoreHandle::Fs(fs) => {
                use animus_env::SegmentStore;
                fs.put(id, bytes).await?;
                Ok(Vec::new())
            }
        }
    }

    /// Fetch a backup object's bytes — mirrors
    /// [`SegmentStoreHandle::get_sealed`]'s exact contract (`replicas`
    /// ignored for the single-directory `Fs` opt-in).
    pub(crate) async fn get(
        &self,
        replicas: &[NodeId],
        id: &str,
    ) -> std::io::Result<Option<Vec<u8>>> {
        use animus_env::SegmentStore;
        match self {
            BackupStoreHandle::Cluster(c) => c.get_from(replicas, id).await,
            BackupStoreHandle::Fs(fs) => fs.get(id).await,
        }
    }

    /// Delete a backup object at every one of `replicas` — mirrors
    /// [`SegmentStoreHandle::delete_sealed`]'s exact contract.
    pub(crate) async fn delete(&self, replicas: &[NodeId], id: &str) -> std::io::Result<()> {
        use animus_env::SegmentStore;
        match self {
            BackupStoreHandle::Cluster(c) => c.delete_from(replicas, id).await,
            BackupStoreHandle::Fs(fs) => fs.delete(id).await,
        }
    }

    /// List every id starting with `prefix` on **this node's own local**
    /// backup directory — mirrors [`SegmentStoreHandle::list_local`]'s exact
    /// contract (debug/sweep only, never load-bearing for correctness: the
    /// replicated backup catalog, ADR 0059 §3, is the sole authority for
    /// what backup data exists).
    pub(crate) async fn list_local(&self, prefix: &str) -> std::io::Result<Vec<String>> {
        use animus_env::SegmentStore;
        match self {
            BackupStoreHandle::Cluster(c) => c.local().list(prefix).await,
            BackupStoreHandle::Fs(fs) => fs.list(prefix).await,
        }
    }
}

/// `--backup-store cluster|fs:PATH` CLI opt-in (ADR 0059 §1), defaulting to
/// `Cluster` — the existing K-replicated `ClusterSegmentStore`, so a fresh
/// install needs nothing extra configured. `Fs(PATH)` (parsed by `main.rs`
/// from `--backup-store fs:PATH`) selects a bare, single-directory
/// `FsSegmentStore` at `PATH` instead.
///
/// **Unlike [`SegmentStoreConfig`]'s identically-shaped `--segment-store
/// dir:PATH` knob, the ADR spells out an explicit `cluster` keyword as well
/// as `fs:PATH`** (`parse_backup_store` in `main.rs` accepts both the
/// omitted flag and the literal string `cluster` as `Cluster`) — kept as a
/// distinct enum from `SegmentStoreConfig`, not a reuse, for the same reason
/// [`BackupStoreHandle`] is its own type: a backup store's durability
/// tradeoff is worth documenting and configuring on its own terms, even
/// though the two enums are shaped identically today.
///
/// **The default (`Cluster`) does not survive a whole-cluster loss** — it
/// replicates within the same cluster the backups protect data *from*
/// (operator/application mistakes), not from a total cluster failure.
/// `fs:PATH` pointed at separately backed-up or replicated storage — and,
/// later, an S3 backend (ADR 0059's own named follow-up) — is the actual
/// disaster-recovery story. Stated here once, plainly, per the ADR's own
/// instruction that this must not be left to be discovered the hard way.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum BackupStoreConfig {
    #[default]
    Cluster,
    Fs(PathBuf),
}

/// Build (and, for the cluster variant, **start**) this node's
/// [`BackupStoreHandle`] (ADR 0059 §1) — mirrors [`build_segment_store`]'s
/// exact shape, rooting the cluster variant's per-node local
/// `FsSegmentStore` building block at `dir.join("backups")` rather than
/// `dir.join("segments")` — kept physically separate from the streams
/// store's own local directory even though the two stores' object
/// namespaces are already disjoint (`animus_cp_data::backup`'s own module
/// doc), the same belt-and-suspenders posture ADR 0059 §1 takes for the
/// namespace split itself.
fn build_backup_store(
    env: &ProdEnv,
    dir: &Path,
    control: ControlHandle,
    self_id: NodeId,
    config: &BackupStoreConfig,
) -> BackupStoreHandle {
    match config {
        BackupStoreConfig::Cluster => {
            let local = FsSegmentStore::new(dir.join("backups"));
            let placement: Arc<dyn animus_cp_data::cluster_segment_store::PlacementView> =
                Arc::new(ControlPlacementView { control, self_id });
            BackupStoreHandle::Cluster(
                animus_cp_data::cluster_segment_store::ClusterSegmentStore::start(
                    env.clone(),
                    local,
                    placement,
                    animus_cp_data::backup::BACKUP_SEGMENT_STREAM,
                ),
            )
        }
        BackupStoreConfig::Fs(path) => BackupStoreHandle::Fs(FsSegmentStore::new(path.clone())),
    }
}

/// Growth PR3 Fork F (ADR 0042 §14): a per-node, per-tablet estimate of a
/// streamed tablet's own change-append rate (bytes/sec of `KIND_CHANGE`
/// growth) — derived entirely from data `index_drain::seal_tick` already
/// computes every tick (`CpGroup::approx_bytes_kind(KIND_CHANGE)`, the same
/// level [`Metric::StreamHotBytes`] reads), never a new scan.
/// `CpGroup::approx_bytes` is deliberately **base**-scoped (ADR 0034's own
/// fix, so auto-split's byte trigger can't react to change-log churn) —
/// which structurally means a high-churn, small-footprint streamed table
/// can write forever without ever crossing a byte/key threshold and
/// gaining a second shard, regardless of write rate (the exact gap this
/// tracker exists to close, per the growth plan's Fork F).
///
/// A simple EWMA over each tick's own instantaneous bytes-delta ÷ elapsed
/// (`ALPHA`), so one noisy tick doesn't whipsaw the signal; floored at zero
/// (a seal + the hot-trim arm's later reclaim can shrink the hot scope
/// between ticks, which is not a *negative* append rate — just this tick's
/// own contribution being nothing). Surfaced read-only via
/// `/admin/metrics`'s `stream_change_rates` array (`admin::metrics_view`)
/// and consumed by the opt-in `--auto-split-change-rate` trigger
/// (`auto_split_loop`, streamed tables only). A plain `std::sync::Mutex` is
/// fine: every access is a quick lock/mutate/drop with no `.await` held
/// across it, the same discipline `ClientCtx::metrics_history` already
/// uses.
/// One `/admin/raftkv` split-build mirror entry (ADR 0050):
/// `(rows_shipped, converged, phase)`.
pub(crate) type SplitBuildView = (u64, bool, &'static str);

#[derive(Clone, Default)]
pub(crate) struct ChangeRateTracker {
    inner: Arc<Mutex<BTreeMap<TabletId, RateSample>>>,
}

#[derive(Clone, Copy)]
struct RateSample {
    bytes_per_sec: f64,
    last_bytes: u64,
    last_at: tokio::time::Instant,
}

/// The EWMA smoothing factor for [`ChangeRateTracker::observe`] — closer to
/// 1.0 tracks the latest tick more closely (noisier); closer to 0.0 smooths
/// harder (slower to react). Chosen to settle within a handful of
/// `INDEX_DRAIN_INTERVAL` ticks (~1s) without being so reactive that a
/// single large write's own tick dominates the reading.
const CHANGE_RATE_EWMA_ALPHA: f64 = 0.3;

impl ChangeRateTracker {
    /// Record this tick's own `KIND_CHANGE` byte level for `tablet` and
    /// return the freshly-updated smoothed rate (bytes/sec).
    pub(crate) fn observe(&self, tablet: TabletId, bytes_now: u64) -> f64 {
        let now = tokio::time::Instant::now();
        let mut inner = self.inner.lock().expect("change-rate tracker lock");
        let rate = match inner.get(&tablet) {
            None => 0.0,
            Some(prev) => {
                let elapsed = now.saturating_duration_since(prev.last_at).as_secs_f64();
                if elapsed <= 0.0 {
                    prev.bytes_per_sec
                } else {
                    let instantaneous = bytes_now.saturating_sub(prev.last_bytes) as f64 / elapsed;
                    CHANGE_RATE_EWMA_ALPHA * instantaneous
                        + (1.0 - CHANGE_RATE_EWMA_ALPHA) * prev.bytes_per_sec
                }
            }
        };
        inner.insert(
            tablet,
            RateSample {
                bytes_per_sec: rate,
                last_bytes: bytes_now,
                last_at: now,
            },
        );
        rate
    }

    /// The current smoothed rate for `tablet` (bytes/sec), or `0.0` if
    /// never observed (e.g. an unstreamed tablet, or one this node has
    /// never led a `seal_tick` pass for).
    pub(crate) fn get(&self, tablet: TabletId) -> f64 {
        self.inner
            .lock()
            .expect("change-rate tracker lock")
            .get(&tablet)
            .map_or(0.0, |s| s.bytes_per_sec)
    }

    /// Every currently-tracked tablet's own smoothed rate, in tablet-id
    /// order — for `/admin/metrics`'s `stream_change_rates` array.
    pub(crate) fn snapshot(&self) -> Vec<(TabletId, f64)> {
        self.inner
            .lock()
            .expect("change-rate tracker lock")
            .iter()
            .map(|(&t, s)| (t, s.bytes_per_sec))
            .collect()
    }

    /// Drop every tracked tablet no longer present in `meta` — a cheap
    /// `BTreeMap` retain, never a data scan, bounding this map the same
    /// way `change_consumer_loop`'s own `first_hot_seen` fallback map
    /// bounds itself.
    pub(crate) fn retain_existing(&self, meta: &Metadata) {
        self.inner
            .lock()
            .expect("change-rate tracker lock")
            .retain(|t, _| meta.tablets.contains_key(t));
    }
}

/// This node's data-plane fields (ADR 0035 PR3) — present in [`ClientCtx`]
/// iff this node runs the data role (`NodeRole::Data`/`Both`); `None` on a
/// control-only node, which never hosts a tablet and never runs the CP/
/// DynamoDB machinery these back. Grouping them under one `Option`
/// (rather than three loose `Option` fields on `ClientCtx`) means "does this
/// node have a data role" is answered once, at the type level, instead of
/// re-derived from whether several unrelated fields all happen to be `Some`.
#[derive(Clone)]
struct DataRole {
    /// Serializes a node's read-modify-writes so a DynamoDB RMW (linearizable
    /// CP read → CP write) is atomic *per node*. Cross-node atomicity (a CAS on the
    /// CP group) is later v1 work. Accessed only from the dynamo wire edge,
    /// whose listener is never bound on a control-only node.
    pub(crate) rmw_lock: Arc<tokio::sync::Mutex<()>>,
    /// The raftkv-role env's recording metrics sink (the CP group records here).
    /// Aggregated into the `/metrics` export (ADR 0015) alongside the control
    /// sink, which every node has.
    pub(crate) raftkv_metrics: MetricsHandle,
    /// This node's **base `raftkv` id** — its identity in a tablet's replica set
    /// (ADR 0023). Used by routing to tell "this node is a replica of the tablet, so
    /// wait for its own group to form" from "this node hosts nothing for the tablet,
    /// so forward."
    pub(crate) base_id: NodeId,
    /// This node's stream-shard [`SegmentStoreHandle`] (ADR 0043 §A7b) — the
    /// sealer's `SegmentStore::put` target, `index_drain.rs`'s
    /// `change_consumer_loop` seal arm's only consumer today.
    pub(crate) segment_store: SegmentStoreHandle,
    /// This node's **backup** [`BackupStoreHandle`] (ADR 0059 §1) — a
    /// second, backup-dedicated store handle alongside `segment_store`
    /// above, built from its own `--backup-store` CLI knob. **Unused today**
    /// (ADR 0059 Train 1 PR②, plumbing only): no capture driver, janitor, or
    /// wire surface reads or writes through it yet — a later PR's capture
    /// driver is this field's first real consumer.
    #[allow(dead_code)]
    pub(crate) backup_store: BackupStoreHandle,
    /// The DynamoDB Streams sealer's own size/age knobs (ADR 0042 §13).
    pub(crate) stream_seal_knobs: StreamSealKnobs,
    /// Growth PR3 Fork F (ADR 0042 §14): this node's own per-tablet
    /// change-append-rate estimates, written by `index_drain::seal_tick`
    /// and read by `/admin/metrics` and the opt-in `--auto-split-change-
    /// rate` trigger (`auto_split_loop`). See [`ChangeRateTracker`]'s own
    /// doc for the full design.
    pub(crate) change_rates: ChangeRateTracker,
    /// ADR 0050 Train B rung 4: this node's own per-parent-tablet split-build
    /// progress — `(rows shipped, build converged)` — written by
    /// `index_drain`'s split-driver arm on the parent's leader node, read by
    /// `/admin/raftkv` (`CpRaftView.split_rows_shipped`/`split_converged`).
    /// Driver-local observability only, NEVER correctness state: a leader
    /// change starts a fresh entry on the new leader's node (the build
    /// re-runs idempotently), and B5's freeze/cutover decisions read the
    /// tail's own convergence directly, not this mirror.
    pub(crate) split_builds: Arc<Mutex<BTreeMap<u64, SplitBuildView>>>,
}

/// Shared context for the client request server and the DynamoDB endpoint:
/// the control-plane handle (for cached metadata + schema proposals — a
/// [`ControlHandle`], ADR 0035 PR1), this node's own wire-edge state (incl. the
/// CP group handles it hosts), the cross-node CP routing table, and — iff this
/// node runs the data role (ADR 0035 PR3) — its [`DataRole`] fields.
#[derive(Clone)]
pub(crate) struct ClientCtx {
    control: ControlHandle,
    pub(crate) edge: ClusterEdgeState,
    /// This node's one internal `ProdEnv` (ADR 0040 PR1) — every role's
    /// clone of the same handle. The **only** `Env`-seam access point this
    /// context exposes to the wire edges: e.g. minting a DynamoDB Streams
    /// label at enable time (ADR 0042 §4) goes through `ctx.env.now()`,
    /// never `std::time` directly (ADR 0003's determinism rule — this crate
    /// is production-only `ProdEnv` wiring, but the seam convention still
    /// holds so nothing here quietly grows a second, ambient time source).
    pub(crate) env: ProdEnv,
    /// This node's data-plane fields, if it runs the data role — see
    /// [`DataRole`]'s doc. `None` on a control-only node (ADR 0035 PR3).
    /// Access via [`data`](Self::data), not directly.
    data: Option<DataRole>,
    /// CP-group routing table: each CP group member id (`raftkv_id`, `300+i`) → the
    /// **client API** address of its hosting node (ADR 0017 #3b). Lets a node that
    /// received a CP op but doesn't host the group leader **forward** the request to
    /// the leader's node. Seeded from the cluster config/bound addresses at startup
    /// (ADR 0031 PR2: `start_cluster_with`'s in-process `--cluster N` bring-up
    /// builds this the same way `run_node_with` does, since each node now has its
    /// own `ClusterEdgeState` and must genuinely forward to reach another node's
    /// group) and kept **live** thereafter by [`route_sync_loop`] (ADR 0032 PR1):
    /// each tick overlays `Metadata.node_addrs[*].client` on top of the static
    /// seed, so a node grown in *after* this node's own startup still becomes a
    /// valid forward target — closing the ADR 0030 residual gap where this map
    /// was a process-start-only snapshot. `Arc<Mutex<_>>` so the sync loop can
    /// replace it in place while every clone of this `ClientCtx` (one per
    /// connection) observes the update; read via [`route_addr`](Self::route_addr)
    /// / [`route_snapshot`](Self::route_snapshot), never locked across an
    /// `.await`.
    client_route: Arc<Mutex<BTreeMap<NodeId, SocketAddr>>>,
    /// **Intra-cluster routing table (ADR 0047)** — the exact `client_route`
    /// shape above, mirrored for the intra port: each CP group member id →
    /// the **intra** address of its hosting node. Kept live by
    /// [`intra_route_sync_loop`] (overlaying `Metadata.node_addrs[*].intra`
    /// on a static seed, exactly like `route_sync_loop` does for
    /// `client_route`). Every machine-relay consumer that used to read
    /// `client_route`/`route_addr`/`route_snapshot` for a node-to-node hop —
    /// `cp_leader_hint`, `other_tablet_replica_addr`, `propose_schema`'s
    /// relay/broadcast tiers — reads this instead via
    /// [`intra_addr`](Self::intra_addr)/[`intra_route_snapshot`](Self::intra_route_snapshot).
    /// Human-facing consumers (`not_leader_error`, the admin dashboard's
    /// `leader_hint` display) keep reading `client_route`/`leader_addr_hint`
    /// unchanged — see the root `CLAUDE.md`'s hint-field-conflation lesson.
    intra_route: Arc<Mutex<BTreeMap<NodeId, SocketAddr>>>,
    /// This node's identity + bound addresses for the admin `/admin/config` view
    /// (ADR 0020). `Arc` so cloning the ctx onto each connection is cheap.
    admin: Arc<AdminInfo>,
    /// Ring buffer of periodic `metrics_json()` snapshots, filled by
    /// [`metrics_sample_loop`] — backs `/admin/metrics/history`'s sparklines.
    /// A plain `std::sync::Mutex` is fine: every access is a quick lock/mutate/
    /// drop with no `.await` held across it.
    metrics_history: Arc<Mutex<VecDeque<MetricsSample>>>,
    /// A **control-plane-follower-less growth node's** (ADR 0030) mirror of the
    /// real cluster's replicated `Metadata`, refreshed by
    /// [`remote_metadata_sync_loop`] polling `ClientRequest::Status` against one
    /// of the pre-growth control nodes. `None` for every node that is a genuine
    /// voter of `self.control`'s own control group (the overwhelming common case
    /// — the control group is static, ADR 0030's documented v1 limitation, so
    /// this is only ever populated on a node started via [`run_node_growth`]).
    /// Read through [`effective_metadata`](Self::effective_metadata), never
    /// directly — see that method's doc for which call sites must use it.
    remote_metadata: Arc<Mutex<Option<Metadata>>>,
    /// This node's own control-plane **system-keyspace** engine handle (ADR
    /// 0038 PR4), if it has a `ControlHandle::Local` control role — a clone
    /// of exactly the engine handle passed to `RaftNode::start_with_metrics`
    /// (a combined node's already-open *shared* CP-data engine; a
    /// control-only node's own small *dedicated* engine). `None` on a
    /// data-only node (no local control role at all). Read-only: this is a
    /// second handle onto the same live engine purely for admin
    /// introspection (`/admin/storage/control`) — the apply task's own
    /// handle (moved into `RaftNode::start_with_metrics`) remains the sole
    /// writer.
    pub(crate) control_storage: Option<SharedEngine>,
    /// The client DynamoDB port's SigV4 credential store (ADR 0057):
    /// `access_key_id → secret_access_key`, from the cluster config's
    /// `dynamo_auth` section (or `--dynamo-auth PATH` on a config-less
    /// startup mode). `None` — every existing config/test/deployment —
    /// means auth is **disabled**: `dynamo::handle_conn` skips verification
    /// entirely, zero-cost and behavior-identical to before this ADR.
    /// `Arc`-wrapped (not `Arc<Mutex<_>>`) because this is a **static**
    /// load-time credential set with no runtime mutation path (ADR 0057's
    /// "explicitly out of scope: rotation, dynamic credential API") — cheap
    /// to clone onto each connection's `ClientCtx`, never locked.
    pub(crate) dynamo_auth: Option<Arc<BTreeMap<String, String>>>,
    /// Which tablet-split workflow this node proposes when it drives
    /// `trigger_split` (ADR 0058 Train 2 rung 3's `animusd`-level driver
    /// residue) — `Copy` (every existing deployment/test, since this is
    /// what every constructor below defaults to) is byte-for-byte the
    /// original ADR 0050 workflow. See [`SplitMode`]'s own doc. Threaded
    /// from the `--split-mode {copy,inplace}` CLI flag (`--config`/
    /// `--cluster N` only, mirroring `--quiesce-after`'s own scope) —
    /// plain per-node config, not gated by [`DataRole`], since a
    /// control-only node's `trigger_split` calls need it too.
    pub(crate) split_mode: SplitMode,
}

impl ClientCtx {
    /// This node's [`DataRole`] fields — see that type's doc.
    ///
    /// # Panics
    /// If this node has no data role (ADR 0035 PR3 control-only). Every call
    /// site must be reachable only from a path that structurally cannot run
    /// on a control-only node: the dynamo wire edge (its listener is
    /// never bound there) or an internal loop `start_with` only spawns for a
    /// data-capable node (`auto_split_loop`). **Never** call this from a
    /// client-request dispatch path a control-only node can reach — CP
    /// routing (`resolve_cp_route`) handles the `None` case explicitly
    /// instead, precisely because it must not panic there.
    pub(crate) fn data(&self) -> &DataRole {
        self.data
            .as_ref()
            .expect("ClientCtx::data called on a control-only node (ADR 0035 PR3)")
    }

    /// Like [`data`](Self::data), but a non-panicking `Option` — for a path
    /// that genuinely may run on a control-only node and must degrade
    /// gracefully instead of asserting a data role exists. The segment
    /// janitor (`segment_janitor.rs`, ADR 0043 §A9, round-3 PR7) is the one
    /// caller today: it may run on **any** node that can become the
    /// control-plane leader, including a control-only one (ADR 0035),
    /// which has no [`SegmentStoreHandle`] at all — see that module's own
    /// doc for the documented scope this gates.
    pub(crate) fn data_opt(&self) -> Option<&DataRole> {
        self.data.as_ref()
    }

    /// This node's best available **cache-tolerant** view of the cluster's
    /// replicated `Metadata`: this node's own control handle's
    /// [`ControlHandle::metadata_cached`] for a genuine control-group voter
    /// (the common case — reflects committed state via real Raft replication),
    /// or the **mirrored** snapshot [`remote_metadata_sync_loop`] maintains for
    /// a control-plane-follower-less growth node (ADR 0030) whose own control
    /// `RaftCore` never receives real Raft traffic for a group it was never a
    /// voter of (`self.remote_metadata` stays `None` for every other node, so
    /// this is a plain passthrough to `self.control.metadata_cached()`
    /// everywhere else — zero behavior change).
    ///
    /// **Use this, not `self.control.metadata_cached()` directly, for anything
    /// that must work on a growth node**: CP routing (`tablet_for`/
    /// `resolve_cp_route`/`cp_scan`), the per-node join-host/reconfigure loops,
    /// this node's own address-registration commit check, the raftkv peer-sync
    /// loop, the split trigger's precondition reads, and (ADR 0035 PR1)
    /// the general-purpose schema-catalog reads (`table_schema`/
    /// `has_table_schema`) the DynamoDB wire edge uses for
    /// everything except its own commit-wait polls (see
    /// [`metadata_fresh`](Self::metadata_fresh) for those).
    fn effective_metadata(&self) -> Metadata {
        if let Some(meta) = self
            .remote_metadata
            .lock()
            .expect("remote metadata poisoned")
            .clone()
        {
            return meta;
        }
        self.control.metadata_cached()
    }

    /// This node's **read-your-writes** view of the control plane's replicated
    /// `Metadata` (ADR 0035 PR1) — never the growth-node mirror
    /// [`effective_metadata`](Self::effective_metadata) substitutes. For every
    /// node today (`ControlHandle::Local`) this is this node's own control
    /// handle's applied state, unconditionally — including on a growth node,
    /// where it stays exactly as fresh (or as stuck) as it always was before
    /// this seam existed.
    ///
    /// Used by the schema commit-wait polls (`drop_table_schema`/`trigger_split`
    /// below) and the DynamoDB conditional-write existence
    /// gate (`dynamo.rs::quorum_read`'s live re-check on a snapshot miss) —
    /// each must observe its own just-proposed command (or a concurrent
    /// writer's) landing in the authoritative state, not a possibly-stale
    /// mirror.
    ///
    /// **Async since ADR 0035 PR4**: `Local` stays a synchronous-in-substance
    /// passthrough (no `.await` point actually yields), but `Remote`
    /// performs a genuine leader-directed network round trip — see
    /// [`ControlHandle::metadata_fresh`]'s doc.
    async fn metadata_fresh(&self) -> Metadata {
        self.control.metadata_fresh().await
    }

    /// Serve a long-poll [`ClientRequest::WatchMetadata`] (ADR 0035 PR5 for
    /// the long-poll mechanism itself; ADR 0038 PR5 for the incremental
    /// reply shape below): park on this node's own
    /// [`ControlHandle::metadata_watch`] for up to
    /// [`WATCH_METADATA_SERVER_TIMEOUT`], then reply — either because the
    /// watch genuinely advanced past `last_seen`, or because the bound
    /// elapsed with nothing new (a normal outcome, not an error; the caller
    /// just retries with the same `last_seen`, exactly like a `Status` poll
    /// that happened not to observe a change).
    ///
    /// Only a genuine control-group replica (`ControlHandle::Local`) can
    /// serve this. A `Remote` data-only node **rejects** it instead of
    /// degrading: its own `ControlHandle::metadata_watch()` is itself driven
    /// by replies to *this exact request* (see
    /// [`control_handle::RemoteControlClient`]'s doc), so serving it here
    /// would only let a misdirected watch (e.g. a stale `client_route` entry
    /// pointing at a data node instead of a control node) degrade silently to
    /// an effective ~[`WATCH_METADATA_SERVER_TIMEOUT`]-second poll — worse
    /// than the pre-PR5 fixed-interval poll, not better. Rejecting fails the
    /// misdirected watch fast instead.
    ///
    /// **Incremental reply (ADR 0038 PR5)**: once the watch resolves, try
    /// this node's own [`RaftNode::watch_delta_since`] first — if its bounded
    /// delta ring covers `(last_seen, watermark]`, reply with
    /// [`ClientResponse::MetadataDelta`] instead of a full [`ClientResponse::
    /// Status`] clone. Falls back to the full reply whenever the ring
    /// doesn't cover the range (a fresh/lagging/just-recovered replica, or a
    /// caller whose `last_seen` aged out of the window) — the log-tail vs
    /// `InstallSnapshot` fallback shape this plane already has. **Also**
    /// falls back while the ADR 0030 growth-node mirror overlay is active on
    /// this node (`self.remote_metadata` populated): that overlay serves
    /// `effective_metadata()` from a *different* source than this node's own
    /// (on a growth node, permanently inert) local ring, so a delta off that
    /// ring would answer the wrong question.
    pub(crate) async fn watch_metadata(&self, last_seen: u64) -> ClientResponse {
        let ControlHandle::Local(raft) = &self.control else {
            return ClientResponse::Error(
                "this node has no local control-plane watch to serve (ADR 0035 data-only node); \
                 watch a control-plane node instead"
                    .into(),
            );
        };
        let watch = raft.metadata_watch();
        tokio::select! {
            _ = watch.changed(last_seen) => {}
            () = tokio::time::sleep(WATCH_METADATA_SERVER_TIMEOUT) => {}
        }
        let leader_hint = self.control_leader_hint();
        // Intra-cluster dual (ADR 0047) — the same `self.control.leader()`
        // id, resolved through `intra_addr` instead of `route_addr`. This is
        // the field `remote_metadata_watch_loop`'s own dial candidates read
        // (via `RemoteControlClient::intra_leader_addr_hint`), never the
        // human-facing `leader_hint` above.
        let intra_leader_hint = self.intra_control_leader_hint();
        let control_voters = self.control.config().unwrap_or_default();
        let growth_mirror_active = self
            .remote_metadata
            .lock()
            .expect("remote metadata poisoned")
            .is_some();
        if !growth_mirror_active && let Some(reply) = raft.watch_delta_since(last_seen) {
            return ClientResponse::MetadataDelta {
                writes: reply.writes,
                watermark: reply.watermark,
                leader_hint,
                intra_leader_hint,
                control_voters,
            };
        }
        ClientResponse::Status {
            metadata: self.effective_metadata(),
            leader_hint,
            intra_leader_hint,
            watermark: watch.latest(),
            control_voters,
        }
    }

    /// The client-API address `id` currently routes to, if known (ADR 0032
    /// PR1) — a single lookup into the live [`client_route`](Self::client_route)
    /// map, kept fresh by [`route_sync_loop`]. Never holds the lock across an
    /// `.await`.
    fn route_addr(&self, id: NodeId) -> Option<SocketAddr> {
        self.client_route
            .lock()
            .expect("client route poisoned")
            .get(&id)
            .copied()
    }

    /// A clone of the whole live `client_route` map (ADR 0032 PR1), for a
    /// caller that needs to search/iterate it — cloning out under the lock
    /// keeps every subsequent lookup lock-free (and safe to hold across an
    /// `.await`).
    fn route_snapshot(&self) -> BTreeMap<NodeId, SocketAddr> {
        self.client_route
            .lock()
            .expect("client route poisoned")
            .clone()
    }

    /// This node's own best-known control-plane leader, as `(id, client-API
    /// address)` — the `leader_hint` every `Status` reply now carries (ADR
    /// 0035 §1), so a `Remote` data node's mirror-sync/live-fetch loop can
    /// hop toward the real leader without a separate `route_addr` lookup on
    /// the *answering* side. `None` if this node doesn't currently know a
    /// leader (mid-election, or — for this node itself, if it's a growth/data
    /// node — no leader signal at all).
    fn control_leader_hint(&self) -> Option<(NodeId, SocketAddr)> {
        let id = self.control.leader()?;
        let addr = self.route_addr(id.clone())?;
        Some((id, addr))
    }

    /// The intra-cluster RPC address `id` currently routes to (ADR 0047) — the
    /// [`route_addr`](Self::route_addr) sibling for machine-to-machine hops:
    /// `cp_leader_hint`/`other_tablet_replica_addr`/`propose_schema`'s relay
    /// all resolve a forwarding target through this, never through
    /// `route_addr`, since the receiving end (`cp_serve_forwarded`, the
    /// relayed `ProposeSchema`) is only ever reachable on the intra listener.
    /// Kept fresh by [`intra_route_sync_loop`].
    fn intra_addr(&self, id: NodeId) -> Option<SocketAddr> {
        self.intra_route
            .lock()
            .expect("intra route poisoned")
            .get(&id)
            .copied()
    }

    /// The [`route_snapshot`](Self::route_snapshot) sibling for the intra
    /// routing table (ADR 0047).
    fn intra_route_snapshot(&self) -> BTreeMap<NodeId, SocketAddr> {
        self.intra_route
            .lock()
            .expect("intra route poisoned")
            .clone()
    }

    /// This node's own best-known control-plane leader, as `(id, intra
    /// address)` (ADR 0047) — the [`control_leader_hint`](Self::control_leader_hint)
    /// sibling that feeds `intra_leader_hint` on `ClientResponse::Status`/
    /// `MetadataDelta`, and `remote_metadata_watch_loop`'s own dial
    /// candidates via `RemoteControlClient::intra_leader_addr_hint`. Machine-
    /// relay-only — never surfaced to a human (see the root `CLAUDE.md`'s
    /// hint-field-conflation lesson: anything a human reads keeps using
    /// `control_leader_hint`/`leader_hint`).
    fn intra_control_leader_hint(&self) -> Option<(NodeId, SocketAddr)> {
        let id = self.control.leader()?;
        let addr = self.intra_addr(id.clone())?;
        Some((id, addr))
    }

    /// The standard "retry on the leader" refusal for a local-leader-only
    /// admin action ([`admin_drain`](Self::admin_drain)/
    /// [`admin_remove_member`](Self::admin_remove_member)) — carries the
    /// control handle's own [`leader_addr_hint`](ControlHandle::leader_addr_hint)
    /// when one is known (ADR 0035 PR4: always populated for a `Remote` data
    /// node once its mirror has synced at least once, since neither admin
    /// action is relayable and a data-only node can never satisfy either
    /// itself), so an operator hitting this on a data-only node gets a
    /// concrete address to retry against instead of a bare "retry on the
    /// leader".
    fn not_leader_error(&self) -> String {
        match self.control.leader_addr_hint() {
            Some(addr) => format!("this node is not the control-plane leader; retry on {addr}"),
            None => "this node is not the control-plane leader; retry on the leader".into(),
        }
    }

    /// The id of the tablet whose key range covers `key`, from this node's cached
    /// `Metadata` tablet map (the control plane's placement authority). `None` if no
    /// tablet covers it yet (the cluster is still bootstrapping its first tablet).
    ///
    /// **Table-scoped routing (ADR 0023).** Every table owns its own tablet(s):
    /// a key of table `T` is encoded `escape(T) || …` and routes to the
    /// **table-scoped tablet** (`table: Some(T)`) whose range contains it. There is
    /// no whole-keyspace fallback for table data — a table that has not yet had its
    /// tablet provisioned returns `None` (the caller waits), so a write is never
    /// silently absorbed by a catch-all tablet. A legacy `table: None` tablet may
    /// still exist in a snapshot written before scoping; it is the last-resort owner
    /// only for a **raw, non-table-prefixed** key (e.g. the plain test client),
    /// never for a table whose own tablet exists. Iteration is over a `BTreeMap`, so
    /// the choice is deterministic on every node.
    fn tablet_for(&self, table: &str, key: &[u8]) -> Option<TabletId> {
        // Table-scoped routing (ADR 0023): the table is the routing dimension and the
        // key is `token(pk) || escape(pk) || rk` (no table prefix). We look only at
        // `table`'s tablets and match the key's leading token against their token
        // sub-ranges. Two tables' tablets may share a token range, so we never scan
        // the global tablet map. No catch-all: a key of an unprovisioned table yields
        // `None` and the caller waits. The range-match lookup itself is pure — see
        // `topology::tablet_for_key`.
        topology::tablet_for_key(self.effective_metadata().tablets_for_table(table), key)
    }

    /// Resolve how to reach the CP group leader for an op on `key` (shared by every
    /// CP op — read/write/delete/scan — so the leader-resolution + forwarding policy
    /// lives in one place). The key first resolves to its **owning tablet** (Phase 2
    /// multi-tablet CP), then to that tablet's group:
    ///
    /// - this node hosts the tablet's current leader → serve **locally**;
    /// - this node hosts a replica that points at a **remote** leader → **forward**
    ///   there (ADR 0017 #3b);
    /// - this node hosts a replica but the group is still electing → **wait** for it
    ///   to settle (don't forward — the only "route" might be this very node, and
    ///   forwarding a CP op to a non-leader just errors; the edges must not flap
    ///   during election);
    /// - this node hosts **no** replica of the tablet → it can never serve locally,
    ///   so forward to any known route (the receiver serves iff it is the leader,
    ///   else the client retries with fresh routing);
    /// - the tablet itself is not in the map yet (bootstrap) → **wait** for it.
    async fn cp_route(&self, table: &str, key: &[u8]) -> CpRoute {
        let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
        loop {
            if let Some(tablet) = self.tablet_for(table, key)
                && let Some(route) = self.resolve_cp_route(tablet)
            {
                return route;
            }
            if tokio::time::Instant::now() >= deadline {
                return CpRoute::None;
            }
            tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// One attempt at resolving a *known* tablet's group leader to a [`CpRoute`], or
    /// `None` if it isn't settled yet (caller should wait + retry). The leader-
    /// resolution policy behind [`cp_route`](Self::cp_route) (key→tablet→leader).
    ///
    /// The branching itself — serve locally / forward-to-hint / forward-anywhere
    /// / wait — is the pure [`topology::decide_cp_route`]; this method's job is
    /// only to gather its inputs (cheaply, and lazily where a fact needs a
    /// `Metadata` deep clone) and execute the resulting decision.
    fn resolve_cp_route(&self, tablet: TabletId) -> Option<CpRoute> {
        // ADR 0044 phase-1 PR4 (the wake-on-demand edge): wake any locally
        // registered replica of this tablet before deciding anything, so a
        // first touch on a possibly-quiesced cold group doesn't wait out its
        // own idle-detection latency on top of ordinary election-wait.
        // `RaftKvNode::wake()` is cheap and safe on every other state (not
        // quiesced, or this node isn't the leader) — an idempotent notify
        // that costs one inert extra loop iteration at worst.
        if let Some(group) = self.edge.local_cp(tablet) {
            group.wake();
        }
        let leader = self.edge.cp_leader(tablet);
        if let Some(leader) = leader {
            return Some(CpRoute::Local(leader));
        }
        // Forward only to a concrete leader *hint* a local replica gives us.
        let forward_hint = self.cp_forward_target(tablet);
        if let Some(addr) = forward_hint {
            return Some(CpRoute::Forward(addr));
        }
        // No local leader and no leader hint. Whether this node hosts *any* local
        // handle for the group is cheap (no `Metadata` clone); only fetch the
        // metadata-derived facts (`is_replica`, a fallback forward address) in the
        // one case that needs them, matching `decide_cp_route`'s own short-circuit
        // order (avoids the "re-clone `Metadata` per request" cost the wire edges
        // already learned to snapshot around).
        //
        // A registered local handle only counts as "this node hosts a replica"
        // for routing if this node's *own durable Raft config* still lists it as
        // a voter (a local, non-`Metadata` check — `CpGroup::config()`). ADR 0029
        // introduced a window where that is not true: a node moved off a tablet
        // (a healthy rebalance/repair swap) keeps its handle registered until the
        // release-GC's grace period confirms the move and erases it. Before that
        // gate closes, `local_cp` still returns `Some`, and this used to make
        // every branch below short-circuit to `Wait` forever — routing waited on
        // a group this node had already left, instead of forwarding to the
        // node(s) that actually replicate it now. A departing/stale handle must
        // fall through to the metadata-derived path below exactly as if there
        // were no local handle at all.
        //
        // A control-only node (ADR 0035 PR3, `self.data` is `None`) never hosts
        // a local handle at all (`local_group` is always `None` for it), so
        // `has_local_replica`/`is_replica` are correctly `false` without ever
        // needing a real `base_id` — this is the "zero new rejection code"
        // degrade path: a control node is just the limit case of "hosts
        // nothing," handled by the same logic every other non-replica node
        // already goes through.
        let local_group = self.edge.local_cp(tablet);
        let has_local_replica = match (&self.data, local_group) {
            (Some(data), Some(g)) => g.config().contains(&data.base_id),
            _ => false,
        };
        let (is_replica, fallback_forward) = if has_local_replica {
            (false, None)
        } else {
            let meta = self.effective_metadata();
            let replicas = meta.tablets.get(&tablet).map(|t| &t.replicas);
            let is_replica = self
                .data
                .as_ref()
                .is_some_and(|data| replicas.is_some_and(|r| r.contains(&data.base_id)));
            // Intra-flavored (ADR 0047): this is a forwarding target — the
            // receiving node's `cp_serve_forwarded` is only reachable on the
            // intra listener.
            let route = self.intra_route_snapshot();
            let fallback = replicas
                .into_iter()
                .flatten()
                .find_map(|id| route.get(id).copied())
                .or_else(|| route.values().next().copied());
            (is_replica, fallback)
        };
        // `has_local_leader: false` and `forward_hint: None` here are exactly the
        // facts already established by the two early returns above — `Local` is
        // therefore unreachable from this call by construction.
        if let topology::RouteDecision::Forward(addr) =
            topology::decide_cp_route(false, None, has_local_replica, is_replica, fallback_forward)
        {
            return Some(CpRoute::Forward(addr));
        }
        None
    }

    // ---- eventually-consistent read routing (ADR 0055) -------------------
    //
    // `ConsistentRead: false` reads take a route the linearizable path does
    // not have: they are served by ANY replica of the key's tablet, so a
    // node that hosts one answers with zero network hops and zero consensus
    // work, and reads scale across a tablet's replicas instead of all
    // landing on its leader.
    //
    // Every function here is **best-effort by construction**: each returns
    // `None` for "could not serve this cheaply", and every caller falls
    // straight through to the ordinary linearizable path on `None`. That is
    // what keeps the whole feature a strict optimization — there is no
    // eventual-read-specific failure a client can ever observe, only an
    // eventual read that quietly cost what a strong one costs.

    /// The local replica this node may serve an **eventually-consistent**
    /// read of `tablet` from (ADR 0055), or `None` if it may not.
    ///
    /// Three conditions, all local and all cheap:
    ///
    /// - this node has a data role at all (a control-only node hosts no
    ///   tablet, ADR 0035);
    /// - the local handle is a voter in the group's **own durable Raft
    ///   config** — the identical check
    ///   [`resolve_cp_route`](Self::resolve_cp_route) makes, and for the
    ///   identical reason: a node moved off a tablet by a rebalance keeps
    ///   its handle registered until the release-GC erases it (ADR 0029),
    ///   and that departing handle's engine is not this tablet's state to
    ///   serve;
    /// - the replica passes [`RaftKvNode::stale_read_ready`] — it knows a
    ///   leader and its engine holds everything it knows to be committed.
    ///
    /// **Deliberately does not `wake()` the group**, unlike
    /// `resolve_cp_route`'s wake-on-demand edge (ADR 0048 PR4). An eventual
    /// read needs no Raft activity whatsoever, and a quiesced group is idle
    /// by construction — hence fully applied, hence exactly as current as it
    /// will ever be. Waking a fleet's worth of cold groups to serve reads
    /// that do not need them waking is precisely the cost ADR 0044's
    /// cheap-groups roadmap exists to avoid.
    /// Count one eventually-consistent read's outcome (ADR 0055, ADR 0015).
    ///
    /// Silently a no-op on a control-only node, which has no data-role
    /// metrics sink — and no replicas either, so it can only ever record
    /// fallbacks. `self.data()` would panic there; `resolve_cp_route`'s own
    /// rule (this path must never panic) applies just as much to counting as
    /// to routing.
    fn record_eventual_read(&self, metric: Metric) {
        if let Some(data) = self.data.as_ref() {
            data.raftkv_metrics.incr(metric);
        }
    }

    fn cp_stale_local(&self, tablet: TabletId) -> Option<CpGroup> {
        let data = self.data.as_ref()?;
        let group = self.edge.local_cp(tablet)?;
        (group.config().contains(&data.base_id) && group.stale_read_ready()).then_some(group)
    }

    /// Where to send an eventually-consistent read this node cannot serve
    /// itself (ADR 0055): **any** replica of `tablet`, deliberately not its
    /// leader.
    ///
    /// This is the one place the eventual path's routing genuinely differs
    /// in kind rather than in cost from [`cp_forward_target`](Self::cp_forward_target):
    /// there is no leader to resolve, nothing to hint at, and nothing to
    /// chase — every voter holds an answer this read is allowed to return.
    /// Intra-flavored (ADR 0047), like every forwarding target: the
    /// receiving node's `cp_serve_forwarded` is only reachable there.
    ///
    /// Picks the first **other** replica with a known intra address, in
    /// `NodeId` order, which is deterministic on every node. This node is
    /// excluded deliberately: this is only reached after
    /// [`cp_stale_local`](Self::cp_stale_local) already declined, so relaying
    /// to ourselves would spend a round trip re-deriving the identical
    /// refusal.
    ///
    /// It deliberately does **not** spread a table's forwarded eventual reads
    /// across its replicas — read-spreading here comes from clients reaching
    /// different nodes, each answering locally, not from a coordinator fanning
    /// out. A replica-picking policy (latency, load) is a later question and a
    /// bigger one; this returns a correct, stable answer until it is asked.
    fn cp_stale_forward_target(&self, tablet: TabletId) -> Option<SocketAddr> {
        let meta = self.effective_metadata();
        let replicas = &meta.tablets.get(&tablet)?.replicas;
        let me = self.data.as_ref().map(|d| &d.base_id);
        let route = self.intra_route_snapshot();
        replicas
            .iter()
            .filter(|id| Some(*id) != me)
            .find_map(|id| route.get(id).copied())
    }

    /// One-shot `Forwarded` relay for an eventually-consistent read (ADR
    /// 0055).
    ///
    /// Deliberately **not** [`forward_to_tablet_leader`](Self::forward_to_tablet_leader):
    /// that function's whole job is chasing a group's leader through
    /// not-the-leader refusals and election backoff, and an eventual read
    /// has no leader to chase — a refusal from the replica it asked means
    /// "not cheaply, then", which is a fallback signal, not something to
    /// retry. One connection, one reply, [`STALE_READ_FORWARD_TIMEOUT`],
    /// no retries, no waiting out an election.
    async fn relay_stale_read(&self, addr: SocketAddr, request: ClientRequest) -> ClientResponse {
        relay_request_with_timeout(
            addr,
            &ClientRequest::Forwarded {
                request: Box::new(request),
                traceparent: crate::otel::current_traceparent(),
            },
            STALE_READ_FORWARD_TIMEOUT,
        )
        .await
    }

    /// One attempt at serving an **eventually-consistent** point read of
    /// `key` cheaply (ADR 0055) — locally if this node holds a serveable
    /// replica, else one forwarded hop to a replica that might.
    ///
    /// `None` means "not served cheaply"; the caller
    /// ([`cp_read`](Self::cp_read)) falls through to the linearizable path,
    /// which is always correct. `Some(v)` is a served answer, with the
    /// inner `Option` carrying genuine presence/absence exactly as
    /// [`RaftKvNode::stale_get_served`] defines it.
    async fn cp_read_eventual(&self, table: &str, key: &[u8]) -> Option<Option<Vec<u8>>> {
        let served = self.cp_read_eventual_inner(table, key).await;
        if served.is_none() {
            self.record_eventual_read(Metric::CpEventualReadsFellBack);
        }
        served
    }

    /// [`cp_read_eventual`](Self::cp_read_eventual)'s body, split out only so
    /// the fallback counter has exactly one place to live rather than one per
    /// `return None`.
    async fn cp_read_eventual_inner(&self, table: &str, key: &[u8]) -> Option<Option<Vec<u8>>> {
        let tablet = self.tablet_for(table, key)?;
        if let Some(group) = self.cp_stale_local(tablet) {
            // The same read-side scope pre-check the linearizable local arm
            // makes (ADR 0033): routing that has raced a split crossover
            // must fall back, never answer from a scope that does not own
            // the key.
            if !group.scope_range().contains(key) {
                return None;
            }
            let served = group.stale_get_served(key).await?;
            self.record_eventual_read(Metric::CpEventualReadsLocal);
            return Some(served);
        }
        let addr = self.cp_stale_forward_target(tablet)?;
        let request = ClientRequest::Get {
            key: key.to_vec(),
            table: table.to_owned(),
            stale: true,
        };
        match self.relay_stale_read(addr, request).await {
            ClientResponse::Value(v) => {
                self.record_eventual_read(Metric::CpEventualReadsForwarded);
                Some(v)
            }
            _ => None,
        }
    }

    /// [`cp_read_eventual`](Self::cp_read_eventual)'s scan twin — one
    /// attempt at serving one tablet's share of an eventually-consistent
    /// base-scope range scan (ADR 0055). `None` falls back to
    /// [`cp_scan_one`](Self::cp_scan_one)'s linearizable loop.
    async fn cp_scan_one_eventual(
        &self,
        table: &str,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
        let tablet = self.tablet_for(table, start)?;
        if let Some(group) = self.cp_stale_local(tablet) {
            // The scan-side scope pre-check (ADR 0033, `cp_scan_local`'s own
            // rationale): a scope narrower than the requested window would
            // silently truncate the page rather than error, so fall back
            // instead of serving a short answer.
            let requested = KeyRange::new(start.to_vec(), end.map(<[u8]>::to_vec));
            if !group.scope_range().contains_range(&requested) {
                return None;
            }
            return Some(group.stale_scan(start, end, limit, reverse).await);
        }
        let addr = self.cp_stale_forward_target(tablet)?;
        let request = ClientRequest::Scan {
            start: start.to_vec(),
            end: end.map(<[u8]>::to_vec),
            limit,
            reverse,
            table: table.to_owned(),
            stale: true,
        };
        match self.relay_stale_read(addr, request).await {
            ClientResponse::Pairs(p) => Some(p),
            _ => None,
        }
    }

    /// [`cp_scan_one_eventual`](Self::cp_scan_one_eventual)'s **kind-scoped**
    /// sibling (ADR 0041 §3 scopes) — one tablet's share of an
    /// eventually-consistent LSI `Query`/`Scan`. `None` falls back to
    /// [`cp_scan_kind_one`](Self::cp_scan_kind_one)'s linearizable loop.
    async fn cp_scan_kind_one_eventual(
        &self,
        table: &str,
        kind: u8,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
        let tablet = self.tablet_for(table, start)?;
        if let Some(group) = self.cp_stale_local(tablet) {
            let requested = KeyRange::new(start.to_vec(), end.map(<[u8]>::to_vec));
            if !group.scope_range().contains_range(&requested) {
                return None;
            }
            self.record_eventual_read(Metric::CpEventualReadsLocal);
            return Some(
                group
                    .stale_scan_kind(kind, start, end, limit, reverse)
                    .await,
            );
        }
        let addr = self.cp_stale_forward_target(tablet)?;
        let request = ClientRequest::KindScan {
            table: table.to_owned(),
            kind,
            start: start.to_vec(),
            end: end.map(<[u8]>::to_vec),
            limit,
            reverse,
            stale: true,
        };
        match self.relay_stale_read(addr, request).await {
            ClientResponse::Pairs(p) => {
                self.record_eventual_read(Metric::CpEventualReadsForwarded);
                Some(p)
            }
            _ => None,
        }
    }

    /// Whether a CP read error is a **transient routing/leadership/scope race**
    /// the reader should retry with re-resolved routing (the `"; retry"` shape
    /// every such error in this file carries), as opposed to a genuine failure
    /// to surface. Shared by [`cp_read`](Self::cp_read)/[`cp_scan_one`]'s
    /// internal retry loops.
    fn read_should_retry(e: &str) -> bool {
        e.ends_with("; retry")
    }

    /// As [`cp_get_local`](Self::cp_get_local), but additionally chases a
    /// **foreign intent** (ADR 0018 §2/PR4 — a multi-participant
    /// transaction's intent whose covering record lives on a *different*
    /// tablet, so this replica has no local copy to resolve against): tries
    /// the non-blocking [`RaftKvNode::linearizable_get_served_fast`] first;
    /// on `Foreign`, routes a [`ClientCtx::txn_status`] query to the
    /// record's actual owner and, once decided, finishes the read via
    /// [`RaftKvNode::resolve_intent_given_status`] — the exact round trip
    /// `foreign_intent_resolves_via_the_anchor_records_status` (`animus-cp-
    /// data`'s `tests/txn_multi.rs`) proves at the primitive level.
    ///
    /// **ADR 0018 §2/PR5 (lifting PR4's deferral)**: a still-`Pending`
    /// status (or a failed status query — the same "can't confirm, treat
    /// conservatively" posture) no longer immediately reports "retry" —
    /// this pushes the transaction via [`txn_recover`](Self::txn_recover)
    /// first. `txn_recover` itself declines (returns `Pending`, unchanged
    /// behavior) while the record hasn't sat `Pending` past
    /// [`animus_cp_data::RECOVERY_GRACE`] yet — a still-live coordinator's
    /// ordinary in-flight commit is never disturbed by this.
    ///
    /// Falls back to the bounded local wait
    /// ([`cp_get_local`](Self::cp_get_local)) for a **locally**-`Pending`
    /// intent (the single-participant/anchor case, unchanged from PR3 — the
    /// background `txn_resolver_loop`, not this synchronous read path, is
    /// what eventually pushes a stale local record) and for the
    /// still-undecided foreign case after a declined push (the caller's own
    /// retry loop — `cp_read`'s `"; retry"` handling — tries again).
    async fn cp_get_local_resolving(
        &self,
        leader: &CpGroup,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, String> {
        if !leader.scope_range().contains(key) {
            return Err(format!(
                "key {key:?} outside tablet's current range (stale routing, likely a split crossover); retry"
            ));
        }
        match leader.linearizable_get_served_fast(key).await {
            Some(FastRead::Value(v)) => Ok(v),
            // Deliberately still the *blocking* chase, unchanged from PR3
            // (ADR 0018 §2, torn-pair-fix stack PR2's own doc): correct for
            // a genuinely single-key read (`cp_read`/plain `GetItem`), where
            // waiting out a contended local intent is the right behavior —
            // never for a `TransactGetItems` round, which uses
            // `cp_get_local_snapshot` below instead. `info` (now carried by
            // this variant, ADR 0018 §2 amendment) is unused on this arm;
            // see `cp_get_local_snapshot` for the single-shot alternative
            // that *does* need it.
            Some(FastRead::Pending(_)) => match leader.linearizable_get_served(key).await {
                Some(v) => Ok(v),
                None => Err("CP group leader moved; retry".into()),
            },
            Some(FastRead::Foreign(info)) => {
                let status = self.confirm_or_push(&info).await;
                match status {
                    TxnDecisionStatus::Committed { .. } | TxnDecisionStatus::Aborted => {
                        match leader
                            .resolve_intent_given_status(key, &info.txn_id, status)
                            .await
                        {
                            Some(v) => Ok(v),
                            None => Err("transaction resolution race; retry".into()),
                        }
                    }
                    TxnDecisionStatus::Pending => {
                        Err("transaction covering this key is still pending; retry".into())
                    }
                }
            }
            None => Err("CP group leader moved; retry".into()),
        }
    }

    /// The shared "confirm-or-push" step behind both
    /// [`cp_get_local_resolving`](Self::cp_get_local_resolving)'s
    /// foreign-intent arm and [`cp_get_local_snapshot`](Self::cp_get_local_snapshot)
    /// (ADR 0018 §2, torn-pair-fix stack PR2): a single status query for
    /// the transaction `info` describes, routed to its actual owner
    /// (`ClientCtx::txn_status`, transparently local when `info.record_key`
    /// happens to resolve back to this same tablet — see [`IntentInfo`]'s
    /// updated doc). A still-`Pending` status (or a failed query — the same
    /// "can't confirm, treat conservatively" posture) pushes it once via
    /// [`txn_recover`](Self::txn_recover) before giving up (`txn_recover`
    /// itself declines, returning `Pending` unchanged, while the record
    /// hasn't sat `Pending` past [`animus_cp_data::RECOVERY_GRACE`] yet — a
    /// still-live coordinator's ordinary in-flight commit is never
    /// disturbed). Never retries past that single push — the two callers
    /// differ only in what they do with a still-`Pending` result
    /// afterwards (one reports a retryable error for `cp_read`'s own outer
    /// loop to chase; the other reports "unresolved this instant" for a
    /// quiescent round to discard).
    async fn confirm_or_push(&self, info: &IntentInfo) -> TxnDecisionStatus {
        match self.txn_status(&info.record_table, &info.record_key).await {
            Ok(TxnDecisionStatus::Pending) | Err(_) => match self
                .txn_recover(
                    &info.record_table,
                    &info.record_key,
                    &info.txn_id,
                    Some(info.version),
                )
                .await
            {
                Ok(s) => s,
                Err(_) => TxnDecisionStatus::Pending,
            },
            Ok(s) => s,
        }
    }

    /// Non-blocking, single-shot analog of
    /// [`cp_get_local_resolving`](Self::cp_get_local_resolving) — the read
    /// primitive `TransactGetItems`'s quiescent round needs (ADR 0018 §2,
    /// torn-pair-fix stack PR2): every branch below makes **exactly one**
    /// resolution attempt, never a per-key wait/retry, so every key of a
    /// round samples at approximately the same instant regardless of
    /// whether its own intent happened to be local or foreign — the
    /// asymmetry `cp_get_local_resolving` deliberately keeps (a correct,
    /// intentional design for a genuinely single-key read) is exactly what
    /// let a `TransactGetItems` round accept a torn snapshot: seed
    /// `[`FastRead::Pending`]'s bounded *blocking* chase against
    /// `[`FastRead::Foreign`]'s *immediate*-give-up-and-outer-retry shape,
    /// and under a tight back-to-back writer the two keys of one round
    /// systematically sample different instants — a corpus/production
    /// reproduction that stabilized as a genuine, repeatable failure (see
    /// `docs/engineering-lessons.md`'s Testing entries on this
    /// investigation, and the ADR 0018 §2 amendment for the full account).
    ///
    /// Both `Pending` and `Foreign` now carry the identical [`IntentInfo`]
    /// shape (this same amendment), so [`confirm_or_push`](Self::confirm_or_push)
    /// handles them in one arm: one status query (transparently local or
    /// cross-tablet) plus, if still `Pending`, one push attempt — never a
    /// second query, never a sleep-and-retry. A still-undecided outcome, or
    /// a resolve landing on a race (something else already resolved this
    /// exact key underneath), maps to [`SnapshotRead::Unresolved`] rather
    /// than the retryable `"; retry"` error `cp_get_local_resolving` would
    /// report — this function's caller (the round loop, not this call)
    /// decides what "unresolved" means.
    async fn cp_get_local_snapshot(
        &self,
        leader: &CpGroup,
        key: &[u8],
    ) -> Result<SnapshotRead, String> {
        if !leader.scope_range().contains(key) {
            return Err(format!(
                "key {key:?} outside tablet's current range (stale routing, likely a split crossover); retry"
            ));
        }
        match leader.linearizable_get_served_fast(key).await {
            Some(FastRead::Value(v)) => Ok(SnapshotRead::Value(v)),
            Some(FastRead::Pending(info)) | Some(FastRead::Foreign(info)) => {
                let status = self.confirm_or_push(&info).await;
                match status {
                    TxnDecisionStatus::Committed { .. } | TxnDecisionStatus::Aborted => {
                        match leader
                            .resolve_intent_given_status(key, &info.txn_id, status)
                            .await
                        {
                            Some(v) => Ok(SnapshotRead::Value(v)),
                            // A resolution race (something else resolved or
                            // overwrote this key between the status query
                            // and here) — not sampled cleanly this instant,
                            // never a hard error; the round loop retries.
                            None => Ok(SnapshotRead::Unresolved),
                        }
                    }
                    TxnDecisionStatus::Pending => Ok(SnapshotRead::Unresolved),
                }
            }
            None => Err("CP group leader moved; retry".into()),
        }
    }

    /// Non-blocking analog of [`cp_read`](Self::cp_read), backing
    /// `TransactGetItems`'s quiescent round (`dynamo::quiescent_multi_get`,
    /// ADR 0018 §2, torn-pair-fix stack PR2): routing/leadership failures
    /// ("leader moved", stale scope) are retried internally exactly like
    /// `cp_read` — bounded by [`CLIENT_TIMEOUT`], the same routing
    /// discipline every CP primitive shares — since those are never a
    /// meaningful round-level signal; only an unresolved intent
    /// ([`SnapshotRead::Unresolved`]) is surfaced to the caller, since only
    /// the round loop (never this per-key call) may retry on that.
    pub(crate) async fn cp_read_snapshot(
        &self,
        table: &str,
        key: Vec<u8>,
    ) -> Result<SnapshotRead, String> {
        let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
        loop {
            let err = match self.cp_route(table, &key).await {
                CpRoute::Local(leader) => match self.cp_get_local_snapshot(&leader, &key).await {
                    Ok(outcome) => return Ok(outcome),
                    Err(e) => e,
                },
                CpRoute::Forward(addr) => {
                    match self
                        .cp_forward(
                            table,
                            &key,
                            addr,
                            ClientRequest::GetSnapshot {
                                key: key.clone(),
                                table: table.to_owned(),
                            },
                        )
                        .await
                    {
                        ClientResponse::Value(v) => return Ok(SnapshotRead::Value(v)),
                        ClientResponse::Unresolved => return Ok(SnapshotRead::Unresolved),
                        ClientResponse::Error(e) => e,
                        other => {
                            return Err(format!(
                                "unexpected reply to forwarded CP snapshot read: {other:?}"
                            ));
                        }
                    }
                }
                CpRoute::None => return Err("no CP group leader reachable".into()),
            };
            if !Self::read_should_retry(&err) || tokio::time::Instant::now() >= deadline {
                return Err(err);
            }
            tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// Serve a linearizable **scan** on a known-leader local handle, enforcing
    /// the read-side scope pre-check — the scan flavor of
    /// [`cp_get_local`](Self::cp_get_local): `linearizable_scan` filters every
    /// row through the group's live scope (`strip_in_range`), so a scope that
    /// has not yet caught up to the metadata-derived request window (a
    /// split's narrow in flight) would **silently truncate** the results
    /// rather than error. Shared by [`cp_scan_one`] and `cp_serve_forwarded`'s
    /// `Scan` arm.
    async fn cp_scan_local(
        leader: &CpGroup,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let requested = KeyRange::new(start.to_vec(), end.map(<[u8]>::to_vec));
        if !leader.scope_range().contains_range(&requested) {
            return Err(format!(
                "scan window {requested:?} outside tablet's current range (stale routing, likely a split crossover); retry"
            ));
        }
        // Same range, same barrier either way — `reverse` only decides which
        // end of it `limit` keeps and what order the rows come back in.
        let served = if reverse {
            leader.linearizable_scan_rev(start, end, limit).await
        } else {
            leader.linearizable_scan(start, end, limit).await
        };
        match served {
            Some(p) => Ok(p),
            None => Err("CP group leader moved; retry".into()),
        }
    }

    /// Linearizable CP **read** of `key` (ADR 0017): ReadIndex on the group leader,
    /// forwarded to the leader's node if this node isn't it. `Ok(None)` is an
    /// absent key — and **only** a genuinely served absent (ADR 0033 read-path
    /// fix): a read-barrier failure (deposed/mid-election leader) is a
    /// retryable condition, never reported as absence, and a leader whose live
    /// `scope_range()` does not contain `key` (this node's routing raced a
    /// split's narrow — metadata says the group owns the
    /// key, its scope hasn't caught up) is likewise retried until routing and
    /// scope agree, mirroring the write side's pre-propose range check. `Err`
    /// is "no leader reachable / did not become serveable in time". The CP
    /// read primitive the wire edges call directly.
    pub(crate) async fn cp_read(
        &self,
        table: &str,
        key: Vec<u8>,
        consistency: ReadConsistency,
    ) -> Result<Option<Vec<u8>>, String> {
        // ADR 0055: try the cheap path first for a `ConsistentRead: false`
        // read, and fall straight through to the linearizable loop below
        // when no replica can serve it. The strong path is untouched — a
        // `Strong` read compiles down to exactly what it always did.
        if consistency.is_eventual()
            && let Some(v) = self.cp_read_eventual(table, &key).await
        {
            return Ok(v);
        }
        let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
        loop {
            let err = match self.cp_route(table, &key).await {
                CpRoute::Local(leader) => match self.cp_get_local_resolving(&leader, &key).await {
                    Ok(v) => return Ok(v),
                    Err(e) => e,
                },
                CpRoute::Forward(addr) => {
                    match self
                        .cp_forward(
                            table,
                            &key,
                            addr,
                            ClientRequest::Get {
                                key: key.clone(),
                                table: table.to_owned(),
                                stale: false,
                            },
                        )
                        .await
                    {
                        ClientResponse::Value(v) => return Ok(v),
                        ClientResponse::Error(e) => e,
                        other => {
                            return Err(format!(
                                "unexpected reply to forwarded CP read: {other:?}"
                            ));
                        }
                    }
                }
                CpRoute::None => return Err("no CP group leader reachable".into()),
            };
            if !Self::read_should_retry(&err) || tokio::time::Instant::now() >= deadline {
                return Err(err);
            }
            tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// **The evaluate-at-leader write primitive (ADR 0046 U3)** —
    /// `PutItem`/`DeleteItem`/`UpdateItem`'s entry point on an indexed or
    /// streamed table, replacing the edge-evaluated
    /// `index_aware_write`/`ClientCtx::cp_kind_write` pairing those three
    /// call sites (plus `BatchWriteItem`'s indexed branch) used to go
    /// through. Resolves the item's own base key (recomputed from `pk`/`sk`,
    /// the single source of truth — never trusted from a caller-supplied
    /// key), then either serves **locally** (zero hops — this node hosts
    /// the leader, so [`dynamo::kind_write_item_at_leader`] runs in-process)
    /// or **forwards** [`ClientRequest::KindWriteItem`] one hop to the
    /// leader's node, inheriting `cp_forward`'s hinted-retry/backoff/
    /// election-wait exactly like every other CP write. See
    /// [`ClientRequest::KindWriteItem`]'s doc for why this closes the
    /// cross-node LSI/change-record orphan race `index_aware_write`'s
    /// design had.
    ///
    /// **Retries the retryable freeze refusal (issue #288).** A tablet mid
    /// split-cutover freeze (`FROZEN_REFUSAL`, ADR 0050 rung 5) refuses
    /// every mutating propose with a `"; retry"`-suffixed error *before*
    /// ever proposing — from `kind_write_item_at_leader`'s own pre-propose
    /// check when local, or the forwarded leader's identical check when not
    /// — so it's cheap and safe to retry. Mirrors [`cp_read`](Self::cp_read)'s
    /// deadline-bounded loop: bounded by [`CLIENT_TIMEOUT`], re-resolving
    /// `cp_route` every attempt (essential — after cutover the key routes to
    /// a child tablet, not the frozen parent), retrying only while
    /// [`read_should_retry`](Self::read_should_retry) matches the error.
    /// Before this fix a client writing during a split's freeze window got a
    /// terminal error instead of the write succeeding once the child
    /// activates a moment later. The retry loop lives *outside*
    /// `kind_write_item_at_leader`'s own `rmw_lock` scope (issue #285 narrowed
    /// that lock to read+evaluate only), so retrying here — including the
    /// sleep between attempts — never pins the lock across the wait.
    pub(crate) async fn cp_kind_write_item(
        &self,
        meta: &Metadata,
        table: &str,
        pk: &animus_dynamo::AttributeValue,
        sk: Option<&animus_dynamo::AttributeValue>,
        op: KindWriteOp,
        condition: Option<&animus_dynamo::ConditionExpression>,
    ) -> Result<dynamo::KindWriteOutcome, animus_dynamo::wire::WireError> {
        // Auto-provision the table's tablet on first write (ADR 0023), as
        // `cp_kind_write` does — an indexed/streamed table's first item write
        // can race its own `CreateTable`'s tablet provisioning. Stays
        // outside the retry loop below: provisioning is itself idempotent,
        // so re-checking it every retry pass would just be a wasted
        // metadata read once the tablet exists.
        if !self.effective_metadata().has_table_tablet(table) {
            self.provision_tablet(table)
                .await
                .map_err(|e| dynamo::internal(&e))?;
        }
        let base_key = dynamo::item_key(pk, sk);
        // Whether this write may be safely re-applied; see the retry decision
        // at the bottom of the loop.
        let idempotent = dynamo::kind_write_is_idempotent(&op);
        let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
        loop {
            let err = match self.cp_route(table, &base_key).await {
                CpRoute::Local(leader) => {
                    match dynamo::kind_write_item_at_leader(
                        self,
                        &leader,
                        meta,
                        table,
                        pk,
                        sk,
                        op.clone(),
                        condition,
                        // Ordinary client write — never the TTL reaper's own
                        // service identity (ADR 0051 §7; the reaper never
                        // calls through this routed helper — see
                        // `ttl_reaper.rs`).
                        false,
                    )
                    .await
                    {
                        Ok(outcome) => return Ok(outcome),
                        Err(e) => e,
                    }
                }
                CpRoute::Forward(addr) => {
                    let request = ClientRequest::KindWriteItem {
                        table: table.to_owned(),
                        pk: pk.clone(),
                        sk: sk.cloned(),
                        op: op.clone(),
                        condition: condition.cloned(),
                    };
                    match self.cp_forward(table, &base_key, addr, request).await {
                        ClientResponse::KindWriteOk {
                            old,
                            new,
                            collection_bytes,
                        } => {
                            return Ok(dynamo::KindWriteOutcome::Ok {
                                old,
                                new,
                                collection_bytes,
                            });
                        }
                        ClientResponse::ConditionFailed => {
                            return Ok(dynamo::KindWriteOutcome::ConditionFailed);
                        }
                        // The far side may carry a typed error's own code
                        // in the string (`dynamo::encode_relayed_error`);
                        // an unmarked string decodes to `internal`, the
                        // pre-marker behavior.
                        ClientResponse::Error(e) => dynamo::decode_relayed_error(&e),
                        other => {
                            return Err(dynamo::internal(&format!(
                                "unexpected reply to forwarded kind write item: {other:?}"
                            )));
                        }
                    }
                }
                CpRoute::None => dynamo::internal("no CP group leader reachable"),
            };
            // **At-most-once for a non-idempotent write.** This loop re-enters
            // `kind_write_item_at_leader`, which re-reads the old image and
            // re-applies the actions — a fresh read-modify-write, not a replay
            // of the original proposal. For every idempotent op (Put, Delete,
            // SET, REMOVE, a set union or difference) that converges to the
            // same state and the retry is free. A numeric `ADD` does not
            // converge, and a retryable error is not proof the write missed:
            // a failed OCC seatbelt applies as a silent no-op that the
            // confirm-poll reports exactly like a fence miss, so a write that
            // landed can still come back retryable. Retrying then counts twice.
            //
            // DynamoDB's guarantee is at-most-once **per request**, not
            // exactly-once: a *client* that retries an `ADD` which actually
            // applied does double-count there too. So the fix is not an
            // idempotency token — it is simply that the service must not
            // re-apply on its own. A non-idempotent write therefore gets one
            // attempt, and any transient failure is surfaced for the caller to
            // decide about, exactly as DynamoDB would.
            if !idempotent
                || !Self::read_should_retry(&err.message)
                || tokio::time::Instant::now() >= deadline
            {
                return Err(err);
            }
            tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// As [`cp_kind_write`](Self::cp_kind_write), but for a batch with **no
    /// base-kind write** — a GSI reconciliation's footprint/cursor-row
    /// update, or the trim janitor's change-record deletions (ADR 0042 §7/§8)
    /// — none of which touch a client-visible row.
    ///
    /// Confirmation therefore cannot probe a base row; it instead confirms
    /// the batch's **last** write actually landed (`local_get_kind`
    /// returning exactly what was asked for — `Some(value)` for a put,
    /// `None` for a tombstone) rather than stopping at `Accepted`, which only
    /// means "appended to the leader's log". A fenced-out entry commits as a
    /// no-op, so acking on acceptance alone would report an effect that
    /// never landed — and since the whole batch is **one** atomic Raft entry
    /// (`KvCommand::KindBatch`'s own whole-or-nothing apply gate), any single
    /// write's landed effect proves every other write in the same entry
    /// landed too; the last one is picked so a caller that orders its own
    /// "this batch is durable" signal last (the GSI drain's cursor-row bump,
    /// the trim janitor's final deletion) gets it confirmed, not merely an
    /// earlier entry in the same batch.
    ///
    /// **Retries the retryable freeze refusal (issue #288)** — the fast/
    /// marker-write arm's own share of the gap: this primitive backs plain
    /// (unindexed, unstreamed) Dynamo writes and the
    /// raw client protocol, none of which used to retry `FROZEN_REFUSAL`
    /// either. Same shape as [`cp_kind_write_item`](Self::cp_kind_write_item)'s
    /// identical fix: a deadline-bounded loop mirroring
    /// [`cp_read`](Self::cp_read), re-resolving `cp_route` every attempt so a
    /// post-cutover retry lands on the child tablet, retrying only while
    /// [`read_should_retry`](Self::read_should_retry) matches the error.
    pub(crate) async fn cp_kind_write_raw(
        &self,
        table: &str,
        writes: Vec<(u8, Vec<u8>, Option<Vec<u8>>)>,
        change_log: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), String> {
        let Some(first) = writes.first().map(|(_, k, _)| k.clone()) else {
            return Ok(());
        };
        let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
        loop {
            let err = match self.cp_route(table, &first).await {
                CpRoute::Local(leader) => {
                    match Self::cp_kind_raw_local(&leader, writes.clone(), change_log.clone()).await
                    {
                        Ok(()) => return Ok(()),
                        Err(e) => e,
                    }
                }
                CpRoute::Forward(addr) => {
                    let request = ClientRequest::KindWrite {
                        table: table.to_owned(),
                        writes: writes.clone(),
                        change_log: change_log.clone(),
                    };
                    match Self::ok_or_err(
                        self.cp_forward(table, &first, addr, request).await,
                        "forwarded CP kind write",
                    ) {
                        Ok(()) => return Ok(()),
                        Err(e) => e,
                    }
                }
                CpRoute::None => "no CP group leader reachable".to_string(),
            };
            if !Self::read_should_retry(&err) || tokio::time::Instant::now() >= deadline {
                return Err(err);
            }
            tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// The **known-leader** local half of [`cp_kind_write_raw`](Self::
    /// cp_kind_write_raw): fence pre-check, propose, then confirm on the
    /// batch's **last** write — `Some(value)` for a put, `None` for a
    /// ADR 0050 rung 5: the shared pre-propose freeze refusal. A frozen
    /// split parent (post-`KvCommand::Freeze`, pre-cutover/retire) refuses
    /// every mutating propose with this retryable error, so the caller's
    /// ordinary retry loop re-resolves routing and lands on a child once
    /// `CutoverSplit` activates them — the same client shape as an election
    /// wait. Reads are deliberately NOT gated (a frozen parent's state IS
    /// current until cutover). The apply-time whole-range seal remains the
    /// backstop for the propose-vs-apply sliver.
    fn frozen_refusal(leader: &CpGroup) -> Result<(), String> {
        if leader.is_frozen() {
            return Err(FROZEN_REFUSAL.into());
        }
        Ok(())
    }

    /// tombstone (see `cp_kind_write_raw`'s doc for why the last write
    /// proves the whole entry). The ONE confirm implementation for a raw
    /// kind batch, shared by `cp_kind_write_raw`'s `Local` arm and
    /// `cp_serve_forwarded`'s `KindWrite` arm — they diverged once
    /// (the serve arm used [`cp_kind_local`](Self::cp_kind_local), whose
    /// confirm *requires* a `Some`-valued base write), so a raw batch whose
    /// base write is a tombstone erred iff the connected node did not lead
    /// the tablet (leader-placement-bimodal).
    /// Propose one split-build seed chunk on a **known-leader** local handle
    /// of the child group and confirm it applied (ADR 0050 Train B rung 4).
    ///
    /// The ONE local implementation, shared by `cp_serve_forwarded`'s
    /// `SeedRows` arm and `seed_child_rows`' own local branch — never two
    /// copies (the A2-rebase lesson: one confirm implementation per RPC).
    /// Confirmation is **by applied index**, not a value probe: seed rows
    /// merge at *carried* versions, so a legitimately newer row on the child
    /// (per-key LWW — a later tail pass already shipped a fresher version)
    /// would make a value probe hang forever on a batch that correctly
    /// no-opped.
    async fn seed_rows_local(
        leader: &CpGroup,
        rows: Vec<animus_cp_data::SeedRow>,
    ) -> Result<(), String> {
        if rows.is_empty() {
            return Ok(());
        }
        Self::frozen_refusal(leader)?;
        let index = match leader.propose_seed_batch(rows) {
            animus_control::ProposeResult::Accepted { index, .. } => index,
            other => return Err(format!("seed batch not accepted: {other:?}; retry")),
        };
        let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
        let mut poll = CP_CONFIRM_POLL_INIT;
        while tokio::time::Instant::now() < deadline {
            if leader.engine_applied_index() >= index {
                return Ok(());
            }
            tokio::time::sleep(poll).await;
            poll = (poll * 2).min(CP_CONFIRM_POLL_MAX);
        }
        Err("seed batch did not apply in time; retry".into())
    }

    /// Ship one seed chunk to a split child's group leader, wherever it
    /// lives (ADR 0050 Train B rung 4): local if this node leads the child,
    /// else one `Forwarded { SeedRows }` hop chased through the standard
    /// hint machinery — the identical resolve/relay shape
    /// `grow_stream_tablet` uses. Idempotent (a duplicate chunk re-merges
    /// the same versions), so the caller may retry freely.
    pub(crate) async fn seed_child_rows(
        &self,
        child: TabletId,
        rows: Vec<animus_cp_data::SeedRow>,
    ) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
        loop {
            match self.resolve_cp_route(child) {
                Some(CpRoute::Local(leader)) => {
                    return Self::seed_rows_local(&leader, rows).await;
                }
                Some(CpRoute::Forward(addr)) => {
                    // Hint-chasing forward (`forward_to_tablet_leader`), never a
                    // single blind relay: fork F5 places a child at fresh homes,
                    // so this node (the parent's leader) may host NO replica of
                    // it — `resolve_cp_route`'s fallback is then only a first
                    // guess among the child's replicas, and only the refusal's
                    // own leader hint can correct it.
                    let request = ClientRequest::SeedRows {
                        tablet: child.0,
                        rows: rows.clone(),
                    };
                    match self
                        .forward_to_tablet_leader(Some(child), addr, request)
                        .await
                    {
                        ClientResponse::PutOk => return Ok(()),
                        ClientResponse::Error(e)
                            if topology::parse_not_leader_refusal(&e).is_some() => {} // chase exhausted mid-election, retry below
                        ClientResponse::Error(e) => return Err(e),
                        other => return Err(format!("unexpected seed reply: {other:?}")),
                    }
                }
                Some(CpRoute::None) | None => {} // child group not settled yet, retry
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("seed: did not reach the child's leader in time; retry".into());
            }
            tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    async fn cp_kind_raw_local(
        leader: &CpGroup,
        writes: Vec<(u8, Vec<u8>, Option<Vec<u8>>)>,
        change_log: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), String> {
        // ADR 0050 rung 5: a frozen split parent refuses USER data (base/
        // LSI writes) but not consumer bookkeeping (cursor/footprint-only
        // batches — the GSI drain's own writes), which must keep flowing so
        // the drain can finish the frozen log and release the cutover veto
        // (the apply-time gate makes the identical distinction).
        if writes.iter().any(|(kind, _, _)| {
            *kind == animus_cp_data::KIND_BASE || *kind == animus_cp_data::KIND_LSI
        }) {
            Self::frozen_refusal(leader)?;
        }
        let fence = leader.scope_range();
        for (_, key, _) in &writes {
            if !fence.contains(key) {
                return Err("kind write outside this group's live range; retry".into());
            }
        }
        let Some((probe_kind, probe_key, probe_value)) = writes
            .last()
            .map(|(kind, key, value)| (*kind, key.clone(), value.clone()))
        else {
            return Ok(()); // empty batch is a no-op
        };
        let accepted_index = match leader.put_kind_batch_conditioned(writes, change_log, Vec::new())
        {
            ProposeResult::Accepted { index, .. } => index,
            other => return Err(format!("kind write not accepted: {other:?}")),
        };
        let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
        // The same exponential confirm back-off `cp_put_local` uses — NOT the
        // drain's old flat 10ms sleep. This is a client hot path since ADR
        // 0049 routed every plain Dynamo/raw-protocol write through it,
        // and a flat 10ms floor put one whole tick under nearly every
        // sequential write (measured on the ADR 0049 §5 bench: ~13.6 ms/op
        // vs the pre-train ~4.7 — the poll cadence, not the marker bytes).
        let mut poll = CP_CONFIRM_POLL_INIT;
        while tokio::time::Instant::now() < deadline {
            if leader.local_get_kind(probe_kind, &probe_key).await == probe_value {
                return Ok(());
            }
            if Self::confirm_wait_is_futile(leader, accepted_index) {
                // Close the probe-vs-apply race: the entry may have applied
                // between the probe above and the futility read.
                if leader.local_get_kind(probe_kind, &probe_key).await == probe_value {
                    return Ok(());
                }
                return Err(
                    "kind batch superseded before its effect appeared (leadership churn \
                     or an apply-time no-op); retry"
                        .into(),
                );
            }
            tokio::time::sleep(poll).await;
            poll = (poll * 2).min(CP_CONFIRM_POLL_MAX);
        }
        Err("kind batch did not apply in time".into())
    }

    /// Propose a `KindBatch` on a **known-leader** local handle and confirm it.
    ///
    /// Confirmation probes the batch's **base-kind** write, the one row a
    /// client can observe: `poll_probe` reads through the group's base scope,
    /// so an LSI/footprint/change-log write is not observable to it. Every
    /// caller includes a base write (a put's item, or a delete's tombstone
    /// *value*), so there is always a probe; a batch with none is refused
    /// rather than acked unconfirmed — a fenced-out entry commits as a no-op,
    /// so acking without a probe would falsely report a write that never
    /// happened (the hazard `cp_batch_local`'s doc spells out).
    ///
    /// **`conditions` (ADR 0046 U3, `pub(crate)` since [`dynamo::
    /// kind_write_item_at_leader`] calls this from outside `impl ClientCtx`)**:
    /// threaded straight through to `put_kind_batch_fenced`'s own
    /// `KvCommand::KindBatch.conditions` field — see that field's doc. Every
    /// pre-existing caller here passes an empty `Vec` (zero behavior
    /// change); `kind_write_item_at_leader` is the one caller that supplies
    /// its own-key OCC seatbelt. A failed condition no-ops the whole batch
    /// silently, indistinguishable from a fence miss, so it surfaces through
    /// this same function's existing `"CP kind write did not commit in
    /// time"` timeout — deliberately no new outcome channel.
    pub(crate) async fn cp_kind_local(
        leader: &CpGroup,
        writes: Vec<(u8, Vec<u8>, Option<Vec<u8>>)>,
        change_log: Vec<(Vec<u8>, Vec<u8>)>,
        conditions: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    ) -> Result<(), String> {
        let probe = writes
            .iter()
            .find(|(kind, _, v)| *kind == animus_cp_data::KIND_BASE && v.is_some())
            .map(|(_, k, v)| (k.clone(), v.clone().expect("filtered to Some")));
        let Some((probe_key, probe_val)) = probe else {
            return Err("a kind batch must carry a base-kind write to confirm on".into());
        };
        Self::frozen_refusal(leader)?;
        // Pre-propose range check, the same reasoning as `cp_batch_propose`:
        // a fenced-out entry applies as a no-op, and the probe below would then
        // just time out with a generic error instead of a clean routing error.
        let fence = leader.scope_range();
        for (_, key, _) in &writes {
            if !fence.contains(key) {
                return Err("kind write outside this group's live range; retry".into());
            }
        }
        let (accepted_index, accepted_term) =
            match leader.put_kind_batch_conditioned(writes, change_log, conditions) {
                ProposeResult::Accepted { index, term } => (index, term),
                other => return Err(format!("kind write not accepted: {other:?}")),
            };
        let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
        match Self::poll_probe(
            leader,
            accepted_index,
            accepted_term,
            &probe_key,
            &probe_val,
            deadline,
        )
        .await
        {
            ProbeWait::Confirmed => Ok(()),
            // A failed own-key `conditions` entry lands here too: the entry
            // applies as a silent no-op (see `KindBatch.conditions`' doc in
            // `animus-cp-data`), so "superseded" is the caller's cue to
            // re-read and re-evaluate — the ordinary OCC retry round.
            ProbeWait::Superseded => Err(
                "CP kind write superseded before its effect appeared (leadership churn, an \
                 apply-time no-op, or a failed write condition); retry"
                    .into(),
            ),
            ProbeWait::TimedOut => Err("CP kind write did not commit in time".into()),
        }
    }

    /// Propose a `Batch` on a **known-leader** local handle, returning the probe
    /// `(key, value)` to confirm on success — the batch analog of `put`, split out
    /// from confirmation so a caller can poll for confirmation more than once
    /// without proposing more than once. `Err` means the batch was **never**
    /// accepted anywhere (the leader moved) — a fresh retry is free. `Ok` means it
    /// was appended to the leader's log; the caller must still confirm via
    /// [`poll_probe`] before treating it as durable, and must not call this again
    /// for the same data while a poll is still pending (see
    /// [`ClientCtx::cp_batch_write_patient`]'s doc for why re-proposing an
    /// already-accepted-but-unconfirmed batch is actively harmful).
    ///
    /// **Pre-propose range check (ADR 0028 write fences).** `cp_route` can
    /// resolve `Local` off a stale `Metadata` view during a split's crossover
    /// window (this node still thinks it hosts the leader for a wider range
    /// than the tablet's group has actually narrowed to). Proposing anyway
    /// and relying solely on the *embedded* fence to no-op the entry at apply
    /// time is not enough here: `cp_batch_local`'s confirm loop
    /// ([`poll_probe`]) waits for the **last key's value to read back**, and a
    /// fenced-out batch never writes anything — so the loop just times out
    /// with a generic "did not commit" error rather than a clean routing
    /// error, and (see `cp_put_local`'s doc for the sharper version of this
    /// hazard) a confirm mechanism keyed on a coarser signal than value
    /// equality (e.g. an engine-applied index, which a no-op still advances)
    /// could go further and **falsely ack** a write that never happened. So
    /// every key is checked against the leader's own live
    /// [`RaftKvNode::scope_range`] *before* proposing: on a miss, this
    /// returns `Err` **without proposing**, in the same shape as the
    /// `NotLeader` case below, so `cp_batch_write`/`cp_batch_write_patient`'s
    /// caller sees an ordinary routing failure and retries (re-resolving
    /// `cp_route`, which reaches the correct child once this node's own view
    /// of the split has caught up). The embedded `fence` (stamped from this
    /// same read) still rides the proposed entry regardless, covering the
    /// residual race between this check and the entry's actual apply — see
    /// [`RaftKvNode::scope_range`]'s doc for why that sliver can't be closed
    /// for free; an out-of-range write landing in that sliver is *dropped*
    /// (a safe no-op), never mis-applied, so the residual risk is a
    /// mis-timed error, not silent corruption.
    fn cp_batch_propose(
        leader: &CpGroup,
        group: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<Option<(u64, u64, KvPair)>, String> {
        Self::frozen_refusal(leader)?;
        let probe = group.last().cloned();
        let fence = leader.scope_range();
        if let Some((bad_key, _)) = group.iter().find(|(k, _)| !fence.contains(k)) {
            return Err(format!(
                "key {bad_key:?} outside tablet's current range (stale routing, likely a split crossover); retry"
            ));
        }
        match leader.put_batch(group) {
            ProposeResult::Accepted { index, term } => Ok(probe.map(|p| (index, term, p))),
            ProposeResult::NotLeader { .. } => Err("CP group leader moved; retry".into()),
        }
    }

    /// Whether waiting any longer for `accepted_index`'s effect to appear can
    /// still succeed — the confirm-side dual of `RaftKvNode::
    /// wait_stage_outcome`'s own `!is_leader()` bail (ADR 0018 §2). Two
    /// futility signals, either of which ends the wait:
    ///
    /// - **The group has applied past the accepted entry's own log index
    ///   without the probed effect appearing** (the caller re-probes once
    ///   after this returns `true`, closing the probe-vs-apply race):
    ///   whatever occupied that log position either no-opped at apply (a
    ///   freeze/seal miss, a failed `KindBatch` condition) or is a different
    ///   entry entirely (the accepted one was truncated by a leadership
    ///   change, and the new leader's election no-op has already applied
    ///   past it). Either way the effect will never appear from *this*
    ///   propose — only a fresh retry can land it. Sound because the apply
    ///   task advances `engine_applied` only after the entries it covers are
    ///   merged and readable (see `animus-cp-data`'s apply-loop doc).
    /// - **This node no longer leads the group**: the accepted entry may yet
    ///   commit under the new leader (a retry is then a harmless idempotent
    ///   duplicate — per-key LWW converges), or it may have been truncated —
    ///   this node cannot tell which within bounded time, and the caller's
    ///   retry re-resolves routing to wherever the leader now is.
    ///
    /// These confirm loops used to poll out the full [`CLIENT_TIMEOUT`] in
    /// both states ("we time out, which is correct: the write did not
    /// commit") — correct, but a 10s client-visible stall *per attempt*
    /// under leadership churn, which is exactly what a resource-starved CI
    /// runner's slow fsyncs produce (issue #268: two such burns exceed a
    /// test's whole 25s put budget; the stall also hits real clients). A
    /// futile wait now fails fast with the house retryable-error shape so
    /// the caller's own retry loop makes progress instead. **Success still
    /// requires exact effect equality** — this coarser signal only ever ends
    /// a wait, never acks one (the false-ack hazard `cp_put_local`'s doc
    /// spells out).
    fn confirm_wait_is_futile(leader: &CpGroup, accepted_index: u64) -> bool {
        leader.engine_applied_index() >= accepted_index || !leader.is_leader()
    }

    /// Poll `leader`'s local engine for `probe_key` to reflect `probe_val` until
    /// `deadline` — the durable-before-ack confirm wait shared by every CP write
    /// path (mirrors [`cp_put_local`](Self::cp_put_local)). Ends early, with
    /// [`ProbeWait::Superseded`], once [`confirm_wait_is_futile`](Self::
    /// confirm_wait_is_futile) says the accepted entry's effect can no longer
    /// appear.
    ///
    /// `accepted_term` is the term [`ProposeResult::Accepted`] carried
    /// alongside `accepted_index` — see the identity-check note below.
    async fn poll_probe(
        leader: &CpGroup,
        accepted_index: u64,
        accepted_term: u64,
        probe_key: &[u8],
        probe_val: &[u8],
        deadline: tokio::time::Instant,
    ) -> ProbeWait {
        loop {
            // Ask the entry what it did, in preference to reading the value
            // back. Value equality cannot tell "my entry no-op'd" from "my
            // entry applied and a concurrent write then overwrote it" — the
            // second is a success, and reporting it as a failure made a
            // contended key fail spuriously (measured: ten concurrent
            // `PutItem`s to one key, six "superseded" errors). The outcome is
            // recorded per Raft index at apply time and is identical on every
            // replica.
            // **Both halves are required.** The outcome says whether the entry
            // did anything; `engine_applied_index` says whether its effects are
            // merged and readable. The outcome is recorded as the entry is
            // processed, *before* its writes are flushed into the engine, so
            // acking on it alone would ack a write that is not yet visible —
            // the durable-before-visible rule, and precisely the false-ack
            // hazard `cp_put_local`'s doc warns about. Value equality used to
            // imply both at once; splitting them means saying so explicitly.
            //
            // A no-op needs no such wait: it wrote nothing, so there is
            // nothing to become readable and its outcome is final immediately.
            //
            // **`classify_kind_batch_outcome` additionally requires `term ==
            // accepted_term` before treating `Applied` as a confirm (a
            // false-ack found in review of PR #334's KindBatch apply-time
            // outcome channel).** The outcome map is keyed by Raft log index
            // alone, and an *accepted* (appended-locally) entry is not yet a
            // *committed* one — if this node loses leadership before commit,
            // log-matching truncates the accepted entry and a completely
            // different command can commit and apply at the identical index,
            // recording `Applied` there for *its* content, not ours. Index
            // alone cannot tell "my entry applied" from "a different entry
            // now occupies my old index" — only the pair (index, term) can,
            // by Raft's log-matching property (see `ProposeResult::
            // Accepted`'s doc). A term mismatch is classified `Inconclusive`
            // exactly like `None`: not proof of failure either (the value
            // probe below still confirms if the reoccupying entry's content
            // happens to be identical), just not proof of success. See
            // `kind_batch_signal_tests` for the identity check exercised in
            // isolation.
            let effects_readable = leader.engine_applied_index() >= accepted_index;
            match classify_kind_batch_outcome(
                leader.kind_batch_outcome(accepted_index),
                accepted_term,
                effects_readable,
            ) {
                KindBatchSignal::Confirm => return ProbeWait::Confirmed,
                // The caller's OCC round (re-read, re-evaluate) or a re-route.
                KindBatchSignal::NoOp => return ProbeWait::Superseded,
                // Fall through to the value probe, which is the pre-existing
                // behaviour.
                KindBatchSignal::Inconclusive => {}
            }
            if leader.local_get(probe_key).await.as_deref() == Some(probe_val) {
                return ProbeWait::Confirmed;
            }
            if Self::confirm_wait_is_futile(leader, accepted_index) {
                // Close the probe-vs-apply race before giving up: re-check the
                // outcome first, then the value. `confirm_wait_is_futile` can
                // have returned `true` via its `!is_leader()` clause alone,
                // with `engine_applied_index()` still behind `accepted_index`
                // — so `effects_readable` must be recomputed here, not
                // assumed `true` from the fact that we're in this branch.
                if classify_kind_batch_outcome(
                    leader.kind_batch_outcome(accepted_index),
                    accepted_term,
                    leader.engine_applied_index() >= accepted_index,
                ) == KindBatchSignal::Confirm
                {
                    return ProbeWait::Confirmed;
                }
                if leader.local_get(probe_key).await.as_deref() == Some(probe_val) {
                    return ProbeWait::Confirmed;
                }
                return ProbeWait::Superseded;
            }
            if tokio::time::Instant::now() >= deadline {
                return ProbeWait::TimedOut;
            }
            tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// Propose a `Batch` on a **known-leader** local handle and wait until it is
    /// committed + durable + applied — durable-before-ack. The whole batch is one
    /// Raft entry, so confirming the **last** key reflects our value on the leader's
    /// local engine means the entry committed + applied and the whole batch is
    /// durable (the leader applies only after a quorum commit + WAL fsync, as in
    /// [`cp_put_local`](Self::cp_put_local); a per-batch quorum barrier would not
    /// scale under load).
    async fn cp_batch_local(
        leader: &CpGroup,
        group: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), String> {
        let Some((accepted_index, accepted_term, (probe_key, probe_val))) =
            Self::cp_batch_propose(leader, group)?
        else {
            return Ok(());
        };
        let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
        match Self::poll_probe(
            leader,
            accepted_index,
            accepted_term,
            &probe_key,
            &probe_val,
            deadline,
        )
        .await
        {
            ProbeWait::Confirmed => Ok(()),
            ProbeWait::Superseded => Err(
                "CP batch write superseded before its effect appeared (leadership churn or an \
                 apply-time no-op); retry"
                    .into(),
            ),
            ProbeWait::TimedOut => Err("CP batch write did not commit in time".into()),
        }
    }

    // ---- multi-participant transactions (ADR 0018 §2/PR4) --------------------

    /// **The one place a stage actually executes on the leader's own node**
    /// (ADR 0046 U3, `TxnStage` kind-writes stack PR2) — shared by
    /// [`txn_prepare`](Self::txn_prepare)'s own `CpRoute::Local` branch (no
    /// forward needed) and `cp_serve_forwarded`'s `TxnPrepare` arm (a
    /// forwarded hop just landed on the real leader). Evaluates every
    /// `pending_kind_writes` entry **here**, under `ctx.data().rmw_lock` —
    /// the identical lock [`dynamo::kind_write_item_at_leader`] takes for
    /// the ordinary (non-transactional) write path — merging the result
    /// into `writes` immediately before staging, never at the coordinator/
    /// edge (see [`PendingKindWrite`]'s doc for the cross-node race this
    /// closes). Every evaluated write also gets a mandatory own-key
    /// `conditions` entry (ADR 0046 Fork C1: `(key, raw_old)`, the exact
    /// bytes just read) — belt-and-suspenders against the residual window
    /// between this read and the propose call a few lines down, even
    /// though holding `rmw_lock` across both already closes it for every
    /// write this node's own lock covers (a `txn_resolver_loop` recovery
    /// push resolving a *different* transaction's intent never takes it).
    #[allow(clippy::too_many_arguments)] // mirrors ClientRequest::TxnPrepare's own field count
    async fn txn_stage_local(
        &self,
        leader: &CpGroup,
        table: &str,
        anchor: Option<(TxnId, Vec<u8>, String)>,
        mut writes: Vec<TxnWrite>,
        mut conditions: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        participant_spans: Vec<(String, KeyRange)>,
        pending_kind_writes: Vec<PendingKindWrite>,
    ) -> Result<(TxnId, Vec<u8>, String, HlcTimestamp, StageOutcome), TxnAbortReason> {
        Self::frozen_refusal(leader).map_err(TxnAbortReason::Other)?;
        if !pending_kind_writes.is_empty() {
            let meta = self.effective_metadata();
            let _rmw = self.data().rmw_lock.lock().await;
            for p in pending_kind_writes {
                let evaluated = dynamo::eval_kind_txn_write(
                    self,
                    leader,
                    &meta,
                    table,
                    &p.pk,
                    p.sk.as_ref(),
                    &p.op,
                    p.condition.as_ref(),
                )
                .await
                .map_err(|e| {
                    TxnAbortReason::Other(format!(
                        "txn prepare: leader-side evaluation failed: {e}"
                    ))
                })?;
                // ADR 0018's 2026-08-24 `CancellationReasons` amendment
                // (issue #374 C2b): a write action's own condition failing
                // here is a **permanent** `ConditionalCheckFailedException`,
                // never a `TransactionConflict` — `key` is this exact item's
                // own data-plane key, recovered from `p.pk`/`p.sk` the same
                // way `dynamo::item_key` derives it everywhere else.
                let Some(eval) = evaluated else {
                    return Err(TxnAbortReason::ConditionFailed {
                        table: table.to_owned(),
                        key: dynamo::item_key(&p.pk, p.sk.as_ref()),
                    });
                };
                conditions.push((eval.key.clone(), eval.raw_old.clone()));
                writes.push(animus_cp_data::TxnWrite {
                    key: eval.key,
                    value: Some(eval.value),
                    kind_writes: eval.kind_writes,
                    change_log: eval.change_log,
                    stage_marker: Some(eval.stage_marker),
                });
            }
        }
        match anchor {
            None => {
                let (txn_id, record_key, outcome) = leader
                    .txn_stage(table, writes, participant_spans, conditions)
                    .await
                    .ok_or_else(|| {
                        TxnAbortReason::Other(
                            "CP group leader moved during anchor stage; retry".into(),
                        )
                    })?;
                let ts = txn_id.ts;
                Ok((txn_id, record_key, table.to_owned(), ts, outcome))
            }
            Some((txn_id, record_key, record_table)) => {
                let (ts, outcome) = leader
                    .txn_stage_participant(
                        txn_id.clone(),
                        record_key.clone(),
                        record_table.clone(),
                        writes,
                        conditions,
                    )
                    .await
                    .ok_or_else(|| {
                        TxnAbortReason::Other(
                            "CP group leader moved during participant stage; retry".into(),
                        )
                    })?;
                Ok((txn_id, record_key, record_table, ts, outcome))
            }
        }
    }

    /// **Stage** `writes` on `table`'s tablet leader — the anchor
    /// (`anchor: None`, mints a fresh `TxnId`/record key) or a participant
    /// (`anchor: Some((txn_id, record_key, record_table))`, referencing an
    /// already-known anchor record). Routes exactly like every other CP op
    /// (serve locally, or forward one hop via [`ClientRequest::TxnPrepare`]).
    /// Returns `(txn_id, record_key, record_table, stage_ts, outcome)` — for
    /// the anchor case `stage_ts == txn_id.ts` by construction
    /// (`RaftKvNode::txn_stage_anchor` mints the record's own
    /// commit-attempt timestamp as its stage ts). `Err` here means the
    /// stage entry never even *applied* (not leader, or it timed out) —
    /// `outcome` is what the caller checks to learn whether the entry that
    /// did apply actually staged (see [`ClientResponse::TxnPrepared`]'s
    /// doc). `conditions` is ADR 0018 §2's apply-time write-key conditions
    /// amendment (own-key byte-level OCC — empty for a plain transaction).
    ///
    /// **`participant_spans`** (ADR 0018 §2/PR5, task #18 fix): every
    /// *other* participant's `(table, span)` pairs, meaningful only for the
    /// anchor case (`anchor: None`) — merged into the freshly-created
    /// record's `intent_spans` alongside the anchor's own writes, so
    /// in-doubt recovery's `all_staged` check (`ClientCtx::txn_recover`)
    /// can actually verify every participant, not just the anchor. Ignored
    /// for a participant's own stage (`anchor: Some(..)`), which never
    /// creates a record to populate.
    async fn txn_prepare(
        &self,
        table: &str,
        anchor: Option<(TxnId, Vec<u8>, String)>,
        writes: Vec<TxnWrite>,
        conditions: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        participant_spans: Vec<(String, KeyRange)>,
        pending_kind_writes: Vec<PendingKindWrite>,
    ) -> Result<(TxnId, Vec<u8>, String, HlcTimestamp, StageOutcome), TxnAbortReason> {
        let Some(first) = writes.first().map(|w| w.key.clone()).or_else(|| {
            pending_kind_writes
                .first()
                .map(|p| dynamo::item_key(&p.pk, p.sk.as_ref()))
        }) else {
            return Err(TxnAbortReason::Other(
                "txn prepare: writes must be non-empty".into(),
            ));
        };
        match self.cp_route(table, &first).await {
            CpRoute::Local(leader) => {
                self.txn_stage_local(
                    &leader,
                    table,
                    anchor,
                    writes,
                    conditions,
                    participant_spans,
                    pending_kind_writes,
                )
                .await
            }
            CpRoute::Forward(addr) => {
                let request = ClientRequest::TxnPrepare {
                    table: table.to_owned(),
                    anchor,
                    writes,
                    conditions,
                    participant_spans,
                    pending_kind_writes,
                };
                match self.cp_forward(table, &first, addr, request).await {
                    ClientResponse::TxnPrepared {
                        txn_id,
                        record_key,
                        record_table,
                        ts,
                        outcome,
                    } => Ok((txn_id, record_key, record_table, ts, outcome)),
                    // ADR 0018's 2026-08-24 `CancellationReasons` amendment
                    // (issue #374 C2b): recover the typed reason a remote
                    // `txn_stage_local` minted, `TxnAbortReason::encode`d
                    // into this same `ClientResponse::Error` string —
                    // `decode` degrades a peer's plain (pre-amendment, or
                    // genuinely unmarked) error to `Other` automatically.
                    ClientResponse::Error(e) => Err(TxnAbortReason::decode(&e)),
                    other => Err(TxnAbortReason::Other(format!(
                        "unexpected reply to forwarded TxnPrepare: {other:?}"
                    ))),
                }
            }
            CpRoute::None => Err(TxnAbortReason::Other(
                "no CP group leader reachable for txn prepare".into(),
            )),
        }
    }

    /// [`txn_prepare`](Self::txn_prepare), verified: a stage attempt
    /// returning `Ok(..)` only means its *entry applied* — since ADR 0018
    /// §2/PR6 (task #16), it can still have no-op'd internally if any
    /// target key already held another transaction's unresolved intent
    /// (the apply-time writer-push-intents guard `KvCommand::TxnStage`'s
    /// doc describes, closing the chained-stale-intent durability hole a
    /// corpus depth run found). Without checking the returned
    /// `StageOutcome`, a blocked stage would look identical to a genuine
    /// one at the propose layer, and the transaction would go on to commit
    /// **without that key's write ever having happened** — a new, worse
    /// atomicity violation than the one this whole fix exists to close.
    ///
    /// **Since the ADR 0018 §2 apply-time write-key conditions amendment**:
    /// branches directly on `txn_prepare`'s own returned `StageOutcome`
    /// instead of a separate post-hoc `ClientCtx::txn_verify` round trip
    /// (the apply path already knows definitively whether — and why — this
    /// exact stage no-op'd, so a second read to re-derive the same fact was
    /// redundant once the apply arm started reporting it). `Staged` returns
    /// success; `IntentBlocked` retries the whole stage after a short
    /// backoff — bounded (`TXN_STAGE_PUSH_ATTEMPTS`), mirroring the bounded
    /// retry a *read* already does against a foreign pending intent (the
    /// backoff alone, not an explicit push of the blocker — that gives the
    /// blocking transaction room to clear on its own: its own coordinator
    /// finishing, or `txn_resolver_loop`'s passive per-second sweep pushing
    /// it once past `RECOVERY_GRACE`); `ConditionFailed`/`Fenced` are both
    /// **final** — retrying an identical stage changes nothing, so these
    /// return a client-facing error immediately, never looping.
    async fn txn_prepare_pushing(
        &self,
        table: &str,
        anchor: Option<(TxnId, Vec<u8>, String)>,
        writes: Vec<TxnWrite>,
        conditions: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        participant_spans: Vec<(String, KeyRange)>,
        pending_kind_writes: Vec<PendingKindWrite>,
    ) -> Result<(TxnId, Vec<u8>, String, HlcTimestamp), TxnAbortReason> {
        // ADR 0018's 2026-08-24 `CancellationReasons` amendment (issue #374
        // C2b): the last-seen `IntentBlocked` key, so exhausting every retry
        // attempt can still name the specific key that never cleared —
        // `TransactionConflict`, never `ConditionFailed` (a lost race, not a
        // permanent condition failure).
        let mut last_blocked: Option<Vec<u8>> = None;
        for attempt in 0..TXN_STAGE_PUSH_ATTEMPTS {
            let (txn_id, record_key, record_table, ts, outcome) = self
                .txn_prepare(
                    table,
                    anchor.clone(),
                    writes.clone(),
                    conditions.clone(),
                    participant_spans.clone(),
                    pending_kind_writes.clone(),
                )
                .await?;
            match outcome {
                StageOutcome::Staged => return Ok((txn_id, record_key, record_table, ts)),
                StageOutcome::IntentBlocked {
                    key,
                    txn_id: blocker,
                } => {
                    tracing::debug!(
                        table,
                        ?key,
                        blocking_txn = ?blocker,
                        attempt,
                        "txn prepare: stage blocked by another transaction's unresolved intent; \
                         retrying"
                    );
                    last_blocked = Some(key);
                }
                StageOutcome::ConditionFailed { key } => {
                    return Err(TxnAbortReason::ConditionFailed {
                        table: table.to_owned(),
                        key,
                    });
                }
                StageOutcome::Fenced => {
                    return Err(TxnAbortReason::Other(format!(
                        "txn prepare: stage on table `{table}` was rejected (a stale route, an \
                         already-sealed/out-of-fence range, or a concurrent in-doubt-recovery \
                         decision); retry"
                    )));
                }
            }
            if attempt + 1 < TXN_STAGE_PUSH_ATTEMPTS {
                tokio::time::sleep(TXN_STAGE_PUSH_BACKOFF).await;
            }
        }
        match last_blocked {
            Some(key) => Err(TxnAbortReason::TransactionConflict {
                table: table.to_owned(),
                key,
            }),
            // Every `TXN_STAGE_PUSH_ATTEMPTS` attempt returning `Ok` with an
            // outcome other than `Staged`/`IntentBlocked`/`ConditionFailed`/
            // `Fenced` is unreachable (`StageOutcome` is exhaustively
            // matched above) — kept as a typed fallback rather than an
            // `unreachable!()` so a future `StageOutcome` variant fails soft
            // here instead of panicking a live node.
            None => Err(TxnAbortReason::Other(format!(
                "txn prepare: stage on table `{table}` did not converge after \
                 {TXN_STAGE_PUSH_ATTEMPTS} attempts"
            ))),
        }
    }

    /// **Commit or abort** `txn_id`'s record at `record_key` on `table`'s
    /// (the anchor's own) tablet leader — the wire-routed counterpart of
    /// [`RaftKvNode::txn_commit_at_least`] (`commit: true`, floored at
    /// `min_commit_ts`) / [`RaftKvNode::txn_abort`] (`commit: false`).
    ///
    /// **Deliberately resolves nothing** (ADR 0018 §2/PR5 — a change from
    /// the PR4 shape, which bundled the anchor's own keys' resolve into
    /// this call): resolving every participant, the anchor's own keys
    /// included, is now the caller's uniform job (`cp_txn`'s `resolve_all`),
    /// so a record's `intent_spans` — and hence what a recovery pusher
    /// verifies/resolves — never has to special-case "the anchor's keys are
    /// resolved differently from everyone else's."
    ///
    /// **Returns the record's ACTUAL, applied decision** (ADR 0018 §2/PR5
    /// decision-semantics amendment), never just "the ts my own proposal
    /// landed at": recovery makes duelling deciders legal, so this
    /// `commit`/`abort` proposal can lose to a concurrent recovery decision
    /// on the very same record (the anchor's own Raft log position is the
    /// sole arbiter — see `apply_and_compact`'s `TxnCommit`/`TxnAbort`
    /// arms). The caller MUST act on the returned outcome, never assume the
    /// decision it asked for is the one that happened.
    ///
    /// `orphan_created_ts: Some(created_ts)` (ADR 0018 §2/PR5's
    /// orphan-record fix) overrides `commit`/`min_commit_ts` entirely: this
    /// call is a recovery pusher that found **no record at all** for
    /// `txn_id` (see [`txn_recover`](Self::txn_recover)'s doc) and must
    /// synthesize an `Aborted` tombstone (`RaftKvNode::txn_abort_orphan`)
    /// rather than proposing against a record that doesn't exist.
    async fn txn_decide_anchor(
        &self,
        table: &str,
        txn_id: TxnId,
        record_key: Vec<u8>,
        commit: bool,
        min_commit_ts: HlcTimestamp,
        orphan_created_ts: Option<HlcTimestamp>,
    ) -> Result<TxnOutcome, String> {
        match self.cp_route(table, &record_key).await {
            CpRoute::Local(leader) => {
                Self::frozen_refusal(&leader)?;
                if let Some(created_ts) = orphan_created_ts {
                    leader
                        .txn_abort_orphan(txn_id.clone(), record_key.clone(), created_ts)
                        .await
                        .ok_or("CP group leader moved during orphan abort; retry")?;
                } else if commit {
                    leader
                        .txn_commit_at_least(txn_id.clone(), record_key.clone(), min_commit_ts)
                        .await
                        .ok_or("CP group leader moved during anchor commit; retry")?;
                } else {
                    leader
                        .txn_abort(txn_id.clone(), record_key.clone())
                        .await
                        .ok_or("CP group leader moved during anchor abort; retry")?;
                }
                match leader.txn_status_local(&record_key).await {
                    Some(TxnDecisionStatus::Committed { commit_ts }) => {
                        Ok(TxnOutcome::Committed { commit_ts })
                    }
                    Some(TxnDecisionStatus::Aborted) => Ok(TxnOutcome::Aborted),
                    Some(TxnDecisionStatus::Pending) => Err(
                        "txn decide: record still Pending immediately after its own decide \
                         applied — protocol bug"
                            .into(),
                    ),
                    None => Err("CP group leader moved after decide; retry".into()),
                }
            }
            CpRoute::Forward(addr) => {
                let request = ClientRequest::TxnDecide {
                    table: table.to_owned(),
                    txn_id,
                    record_key: record_key.clone(),
                    commit,
                    min_commit_ts,
                    orphan_created_ts,
                };
                match self.cp_forward(table, &record_key, addr, request).await {
                    ClientResponse::TxnDecided { outcome } => Ok(outcome),
                    ClientResponse::Error(e) => Err(e),
                    other => Err(format!(
                        "unexpected reply to forwarded TxnDecide: {other:?}"
                    )),
                }
            }
            CpRoute::None => Err("no CP group leader reachable for txn decide".into()),
        }
    }

    /// **Resolve** `keys` on `table`'s tablet leader per the already-decided
    /// `outcome` — the wire-routed counterpart of [`RaftKvNode::txn_resolve`],
    /// used for every participant (the anchor's own keys included, via the
    /// same routing as any other CP op) once the coordinator has a final
    /// decision. Routed by `keys[0]`, never `record_key` (see
    /// [`ClientRequest::TxnResolve`]'s doc).
    async fn txn_resolve_participant(
        &self,
        table: &str,
        txn_id: TxnId,
        record_key: Vec<u8>,
        keys: Vec<Vec<u8>>,
        outcome: TxnOutcome,
    ) -> Result<(), String> {
        let Some(first) = keys.first().cloned() else {
            return Ok(()); // nothing to resolve
        };
        match self.cp_route(table, &first).await {
            CpRoute::Local(leader) => {
                // ADR 0050 rung 5 (fork F7): a resolve landing on a frozen
                // parent is refused retryably — post-cutover the identical
                // resolve re-routes to the child, which holds the copied
                // intent + record and materializes at its own position.
                Self::frozen_refusal(&leader)?;
                leader.txn_resolve(txn_id, record_key, keys, outcome).await;
                Ok(())
            }
            CpRoute::Forward(addr) => {
                let request = ClientRequest::TxnResolve {
                    table: table.to_owned(),
                    txn_id,
                    record_key,
                    keys,
                    outcome,
                };
                // Best-effort regardless of outcome: a resolve failure never
                // blocks the client-visible commit (already durable on the
                // anchor); it just leaves the intent for a later resolver
                // (PR5) or a reader hitting it (the foreign-intent path).
                let _ = self.cp_forward(table, &first, addr, request).await;
                Ok(())
            }
            CpRoute::None => Ok(()),
        }
    }

    /// **Cross-tablet status query** for `txn_id`'s record at `record_key`
    /// (`record_table`'s own tablet) — the wire-routed counterpart of
    /// [`RaftKvNode::txn_status_local`], used by [`cp_get_local`](Self::cp_get_local)'s
    /// foreign-intent path.
    async fn txn_status(
        &self,
        record_table: &str,
        record_key: &[u8],
    ) -> Result<TxnDecisionStatus, String> {
        match self.cp_route(record_table, record_key).await {
            CpRoute::Local(leader) => leader
                .txn_status_local(record_key)
                .await
                .ok_or_else(|| "CP group leader moved, or no record yet; retry".to_string()),
            CpRoute::Forward(addr) => {
                let request = ClientRequest::TxnStatus {
                    table: record_table.to_owned(),
                    record_key: record_key.to_vec(),
                };
                match self
                    .cp_forward(record_table, record_key, addr, request)
                    .await
                {
                    ClientResponse::TxnStatusReply { status } => Ok(status),
                    ClientResponse::Error(e) => Err(e),
                    other => Err(format!(
                        "unexpected reply to forwarded TxnStatus: {other:?}"
                    )),
                }
            }
            CpRoute::None => Err("no CP group leader reachable for txn status".into()),
        }
    }

    // ---- in-doubt transaction recovery (ADR 0018 §2/PR5) ------------------

    /// **Cross-tablet recovery view** for `txn_id`'s record at `record_key`
    /// (`record_table`'s own tablet) — the recovery-view dual of
    /// [`txn_status`](Self::txn_status): also returns `intent_spans`/
    /// `created_ts`, everything [`txn_recover`](Self::txn_recover) needs.
    async fn txn_record_view(
        &self,
        record_table: &str,
        record_key: &[u8],
    ) -> Result<animus_cp_data::TxnRecordView, String> {
        match self.cp_route(record_table, record_key).await {
            CpRoute::Local(leader) => leader
                .txn_record_view(record_key)
                .await
                .ok_or_else(|| "CP group leader moved, or no record yet; retry".to_string()),
            CpRoute::Forward(addr) => {
                let request = ClientRequest::TxnRecordView {
                    table: record_table.to_owned(),
                    record_key: record_key.to_vec(),
                };
                match self
                    .cp_forward(record_table, record_key, addr, request)
                    .await
                {
                    ClientResponse::TxnRecordViewReply {
                        status,
                        intent_spans,
                        created_ts,
                    } => Ok(animus_cp_data::TxnRecordView {
                        status,
                        intent_spans,
                        created_ts,
                    }),
                    ClientResponse::Error(e) => Err(e),
                    other => Err(format!(
                        "unexpected reply to forwarded TxnRecordView: {other:?}"
                    )),
                }
            }
            CpRoute::None => Err("no CP group leader reachable for txn record view".into()),
        }
    }

    /// **Cross-tablet staged-intent check**: does `table`'s tablet leader
    /// still hold a live intent for `txn_id` anywhere in `span`? Routed by
    /// `span.start` (an exact key — every span a record carries is the
    /// point-span shape `txn::immediate_successor` builds).
    async fn txn_verify(
        &self,
        table: &str,
        span: &KeyRange,
        txn_id: &TxnId,
    ) -> Result<bool, String> {
        match self.cp_route(table, &span.start).await {
            CpRoute::Local(leader) => leader
                .txn_verify_staged(span, txn_id)
                .await
                .ok_or_else(|| "CP group leader moved during txn verify; retry".to_string()),
            CpRoute::Forward(addr) => {
                let request = ClientRequest::TxnVerify {
                    table: table.to_owned(),
                    span: span.clone(),
                    txn_id: txn_id.clone(),
                };
                match self.cp_forward(table, &span.start, addr, request).await {
                    ClientResponse::TxnVerifyReply { staged } => Ok(staged),
                    ClientResponse::Error(e) => Err(e),
                    other => Err(format!(
                        "unexpected reply to forwarded TxnVerify: {other:?}"
                    )),
                }
            }
            CpRoute::None => Err("no CP group leader reachable for txn verify".into()),
        }
    }

    /// Resolve every `(table, span)` in `intent_spans` per `status`
    /// (best-effort, fire-and-forget on any individual routing failure —
    /// see [`txn_resolve_participant`](Self::txn_resolve_participant)'s own
    /// doc): groups spans by **`(table, tablet)`** (a span's own exact key,
    /// `span.start`, is the key to resolve — every span this crate ever
    /// builds is a single-key point-span) and issues one
    /// `txn_resolve_participant` call per **tablet**. A no-op if `status` is
    /// still `Pending` (nothing to resolve yet).
    ///
    /// **ADR 0018 §2 write-loss amendment (Bug 3): grouping by table name
    /// alone used to be the bug.** `intent_spans` only ever names a
    /// `(table, span)` — a table, not a tablet — because a span is recorded
    /// at STAGE time from the writer's own key alone (`ClientCtx::cp_txn`'s
    /// `participant_spans`), never from a specific tablet id. A table with
    /// more than one tablet (any split table) can have two participants'
    /// keys share one table name but live on two different Raft groups.
    /// Grouping by table name alone used to bundle both into one
    /// `txn_resolve_participant` call; that call's own `cp_route(table,
    /// &first)` picks a single leader from the *first* key alone, so the
    /// rest of the bundle silently rode along to the wrong tablet. Because
    /// `KvCommand::TxnResolve` used to carry no fence at all, the wrong
    /// tablet applied the write anyway — onto the *same physical key*
    /// (ADR 0028: a table's tablets share one `StorageScope` prefix), MVCC-
    /// stamped with the wrong tablet's own clock. The right tablet's own
    /// clock never learns of that foreign version and can never mint above
    /// it again: every future write to that key silently loses the per-key
    /// LWW race, forever. Re-resolving each key's own **current** tablet
    /// here (immediately before grouping, via the same [`tablet_for`]
    /// [`ClientCtx::cp_txn`] itself uses at stage time) closes this at the
    /// source; [`KvCommand::TxnResolve`]'s own apply-time fence (added in
    /// the same amendment, mirroring `TxnStage`'s) is the structural
    /// seatbelt for every other caller (present or future) that might make
    /// the identical mistake. A key whose tablet can't be resolved right
    /// now (a genuinely transient routing gap) is skipped, not failed
    /// whole-batch — this whole call is best-effort fire-and-forget by
    /// design (a later resolver-loop tick, or the live coordinator's own
    /// resolve, picks up anything left over).
    ///
    /// ADR 0018 §2/PR6 torn-resolve audit: `status` must always be a
    /// **post-decision re-read** (`txn_status_local`/`txn_record_view`,
    /// or `TxnTracker::unresolved_decided`'s own tracked outcome — itself
    /// only ever inserted at the moment this group's own apply flips
    /// `Pending -> Committed`/`Aborted`, ADR 0018 §2/PR5), never a
    /// decider's own candidate/proposed ts. Every caller in this crate
    /// (`cp_txn`'s `resolve_all`, `txn_recover` below,
    /// `txn_resolver_loop`) already satisfies this; verified by this
    /// audit, not merely assumed.
    async fn recovery_resolve(
        &self,
        txn_id: TxnId,
        record_key: Vec<u8>,
        intent_spans: &[(String, KeyRange)],
        status: &TxnDecisionStatus,
    ) {
        let outcome = match status {
            TxnDecisionStatus::Committed { commit_ts } => TxnOutcome::Committed {
                commit_ts: *commit_ts,
            },
            TxnDecisionStatus::Aborted => TxnOutcome::Aborted,
            TxnDecisionStatus::Pending => return,
        };
        let mut by_table_tablet: BTreeMap<(String, TabletId), Vec<Vec<u8>>> = BTreeMap::new();
        for (table, span) in intent_spans {
            let key = span.start.clone();
            // Re-resolve NOW, not at stage time (`intent_spans` carries no
            // tablet id at all — see this method's own doc) — a genuinely
            // unroutable key (table/tablet not currently resolvable) is
            // skipped, not fatal to the rest of this best-effort resolve.
            let Some(tablet) = self.tablet_for(table, &key) else {
                continue;
            };
            by_table_tablet
                .entry((table.clone(), tablet))
                .or_default()
                .push(key);
        }
        for ((table, _tablet), keys) in by_table_tablet {
            let _ = self
                .txn_resolve_participant(
                    &table,
                    txn_id.clone(),
                    record_key.clone(),
                    keys,
                    outcome.clone(),
                )
                .await;
        }
    }

    /// **Push a transaction record to a decision** (ADR 0018 §2/PR5's
    /// "recovery" mechanism — the CockroachDB "no blocking on a dead
    /// coordinator" property the Decision section's Recovery bullet
    /// promises): any actor holding a foreign-or-local `Pending` intent past
    /// [`animus_cp_data::RECOVERY_GRACE`] may call this to drive the
    /// transaction to a decision. Callable both from a reader that just hit
    /// a stale `Pending` intent (the read-path push) and from
    /// `txn_resolver_loop`'s own periodic sweep.
    ///
    /// **Protocol** (see the ADR's PR5 amendment for the full safety
    /// argument):
    /// 1. Read the record ([`txn_record_view`](Self::txn_record_view)). If
    ///    already decided, resolve every participant and return the
    ///    decision — no need to re-decide.
    /// 2. **If no record exists at all** (ADR 0018 §2/PR5's orphan-record
    ///    fix — a real, already-acknowledged possibility: PR4's prepare
    ///    phase is concurrent, so a participant's own stage can succeed and
    ///    be discovered by a reader while the *anchor's* `TxnStage` — which
    ///    would create this transaction's record — never lands at all,
    ///    e.g. a fence/seal miss the coordinator's propose outcome alone
    ///    can't distinguish from a genuine stage, PR4's own documented gap,
    ///    now applying to the anchor's own stage too): there is no
    ///    `created_ts` to grace-gate against. `intent_ts_hint` (typically
    ///    the orphaned intent's own applied timestamp,
    ///    [`animus_cp_data::IntentInfo::version`]) is the pusher's only
    ///    trustworthy substitute; with none supplied, decline
    ///    conservatively (never wrongly abort something we can't even
    ///    time-bound). Past grace on that substitute, this can ONLY ever
    ///    decide **abort** — an absent record means there is no candidate
    ///    participant list to verify "all staged" against, so committing
    ///    would be unsound; aborting is always safe (see
    ///    [`RaftKvNode::txn_abort_orphan`]'s doc). The synthesized
    ///    tombstone also closes a related hazard: a **late-arriving**
    ///    genuine anchor `TxnStage` for this same `txn_id` finds it and
    ///    no-ops instead of resurrecting a `Pending` record
    ///    (`KvCommand::TxnStage`'s own resurrection guard).
    /// 3. If `Pending` and not yet past grace, decline (`Pending`) — a live
    ///    coordinator may still be working on it.
    /// 4. If `Pending` and stale: verify every `(table, span)` in
    ///    `intent_spans` ([`txn_verify`](Self::txn_verify)). All staged →
    ///    propose `TxnCommit`; any missing (or any verify query itself
    ///    failing — conservatively treated as "not confirmed staged") →
    ///    propose `TxnAbort`.
    /// 5. Either proposal may **lose** to a concurrent decision (a
    ///    still-live coordinator, or a duelling recoverer) — re-read the
    ///    record's actual status and act on THAT, never on what was
    ///    proposed (see `txn_decide_anchor`'s doc for the identical
    ///    argument on the coordinator side).
    /// 6. Resolve every participant per the final, actual decision.
    ///
    /// **Grace is liveness-only**: whether this call even attempts step 3
    /// affects only *when* a decision might be pushed, never *what* it
    /// decides once pushed — a recovery commit requires every span
    /// independently verified staged, exactly the coordinator's own commit
    /// precondition, so a recovery commit and a coordinator's own commit
    /// are the SAME decision; a recovery abort can only ever race a
    /// still-live coordinator's late prepare, in which case the
    /// coordinator's own subsequent commit attempt simply loses (step 4's
    /// mechanism) and the client correctly sees an abort.
    pub(crate) async fn txn_recover(
        &self,
        record_table: &str,
        record_key: &[u8],
        txn_id: &TxnId,
        intent_ts_hint: Option<HlcTimestamp>,
    ) -> Result<TxnDecisionStatus, String> {
        let view = match self.txn_record_view(record_table, record_key).await {
            Ok(view) => view,
            Err(_) => {
                // Step 1b: no record at all. Without a substitute clock we
                // cannot tell "genuinely stale" from "the anchor's stage is
                // simply still in flight" — decline rather than guess.
                let Some(hint_ts) = intent_ts_hint else {
                    return Ok(TxnDecisionStatus::Pending);
                };
                let now_ms = match self.cp_route(record_table, record_key).await {
                    CpRoute::Local(leader) => leader.env().now().0 / 1_000_000,
                    _ => tokio::time::Instant::now().elapsed().as_millis() as u64,
                };
                if now_ms < hint_ts.wall_ms + animus_cp_data::RECOVERY_GRACE.as_millis() as u64 {
                    return Ok(TxnDecisionStatus::Pending);
                }
                // Always an abort — see this method's own doc for why an
                // absent record can never safely commit.
                let proposed = self
                    .txn_decide_anchor(
                        record_table,
                        txn_id.clone(),
                        record_key.to_vec(),
                        false,
                        HlcTimestamp::zero(),
                        Some(hint_ts),
                    )
                    .await?;
                let decided_status = outcome_to_status(&proposed);
                // Re-read for whatever `intent_spans` now exist (typically
                // empty for a fresh tombstone — this pusher only ever knew
                // about the one intent that triggered it, not the whole
                // transaction's participant set, since no record existed
                // to learn that from). A failure here is harmless: the
                // caller that triggered this push (e.g.
                // `cp_get_local_resolving`) still finishes its own read
                // off the returned status regardless of whether this
                // fan-out resolve runs.
                if let Ok(final_view) = self.txn_record_view(record_table, record_key).await {
                    self.recovery_resolve(
                        txn_id.clone(),
                        record_key.to_vec(),
                        &final_view.intent_spans,
                        &decided_status,
                    )
                    .await;
                }
                self.record_recovery_metric(&proposed);
                return Ok(decided_status);
            }
        };
        if !matches!(view.status, TxnDecisionStatus::Pending) {
            self.recovery_resolve(
                txn_id.clone(),
                record_key.to_vec(),
                &view.intent_spans,
                &view.status,
            )
            .await;
            return Ok(view.status);
        }

        // Grace check (liveness-only — see this method's own doc): compare
        // against any reachable env's wall clock, since the pusher may be a
        // different node than the one that minted the record. `cp_route`
        // always resolves *some* local or forwarded leader for `record_key`
        // itself, so re-route here rather than plumb a fresh `Env` handle
        // through just for a clock read.
        let now_ms = match self.cp_route(record_table, record_key).await {
            CpRoute::Local(leader) => leader.env().now().0 / 1_000_000,
            // A forwarded caller has no local env to read; approximate with
            // this node's own — the grace window is generous (seconds) and
            // liveness-only, so modest cross-node clock skew here is
            // harmless (it can only shift *when* a push is attempted).
            _ => tokio::time::Instant::now().elapsed().as_millis() as u64,
        };
        if now_ms < view.created_ts.wall_ms + animus_cp_data::RECOVERY_GRACE.as_millis() as u64 {
            return Ok(TxnDecisionStatus::Pending);
        }

        let mut all_staged = true;
        for (table, span) in &view.intent_spans {
            match self.txn_verify(table, span, txn_id).await {
                Ok(true) => {}
                Ok(false) | Err(_) => all_staged = false,
            }
        }

        let candidate = view.created_ts;
        let proposed = self
            .txn_decide_anchor(
                record_table,
                txn_id.clone(),
                record_key.to_vec(),
                all_staged,
                candidate,
                None,
            )
            .await?;

        let decided_status = outcome_to_status(&proposed);
        self.recovery_resolve(
            txn_id.clone(),
            record_key.to_vec(),
            &view.intent_spans,
            &decided_status,
        )
        .await;

        self.record_recovery_metric(&proposed);
        Ok(decided_status)
    }

    /// Records the `CpTxnRecoveredCommitted`/`CpTxnRecoveredAborted` metric
    /// for a just-completed recovery decision — shared by both
    /// [`txn_recover`](Self::txn_recover) branches (the ordinary decided-
    /// record path and the orphan-record path).
    fn record_recovery_metric(&self, proposed: &TxnOutcome) {
        if let Some(data) = self.data.as_ref() {
            match proposed {
                TxnOutcome::Committed { .. } => {
                    data.raftkv_metrics.incr(Metric::CpTxnRecoveredCommitted);
                }
                TxnOutcome::Aborted => {
                    data.raftkv_metrics.incr(Metric::CpTxnRecoveredAborted);
                }
            }
        }
    }

    /// **Multi-participant transaction** (ADR 0018 §2/PR4): atomically write
    /// every `(table, key, Option<value>)` in `writes` across however many
    /// tablets they span. `preconditions` — `(table, key, expected)`,
    /// `expected: None` meaning "must be absent" — are checked once before
    /// staging and **re-checked right before the commit decision**; a
    /// precondition that no longer matches aborts the whole transaction with
    /// a retryable conflict error instead of committing.
    ///
    /// **A deliberate simplification versus the ADR's precise design** (see
    /// the PR4 amendment): the ADR describes evaluating preconditions at a
    /// specific read timestamp `R` and refreshing via an HLC-timestamped
    /// re-read only if the final `commit_ts` exceeds `R`. Exposing a read's
    /// serve timestamp back to a wire caller (so it could later be compared
    /// against the eventual `commit_ts`) is not yet wired on the client
    /// protocol — only `read_at` (an explicit, caller-chosen `ts`) is, not
    /// "tell me the `ts` an ordinary linearizable read happened to serve
    /// at". This re-checks by **value** (an ordinary linearizable read,
    /// twice) instead, bounding the same race (a conflicting write landing
    /// between prepare and commit) without that extra wire primitive —
    /// correct for the stated goal, but not byte-for-byte the ADR's
    /// mechanism. Flagged here and in the ADR amendment as a follow-up.
    ///
    /// **Flow** (ADR 0018 §3, the PR4 amendment, and the PR5 amendment
    /// lifting its one deliberate deviation): group `writes` by owning
    /// tablet; the first write's tablet is the **anchor** (stages first,
    /// synchronously — it mints the `TxnId`/record key every participant
    /// needs, and its record's `intent_spans` name **every** participant,
    /// ADR 0018 §2/PR5). Every other participant then stages
    /// **concurrently** (`futures::future::join_all`). `staged` tracks
    /// every participant that actually needs resolving, the anchor's own
    /// keys included (PR5: `txn_decide_anchor` no longer resolves anything
    /// inline). Any prepare failure — or a failed pre-commit precondition
    /// re-check — proposes an abort on the anchor; on success, `commit_ts`
    /// is the anchor's own `txn_commit_at_least` result, floored at the max
    /// of every participant's acked stage ts — the single Raft commit on
    /// the anchor's record IS the atomic commit point.
    ///
    /// **Every decide attempt reports the record's ACTUAL outcome, not what
    /// was asked for** (ADR 0018 §2/PR5 decision-semantics amendment): with
    /// recovery, a duelling decider is legal — an abort attempt can lose to
    /// a concurrent recovery *commit* (every participant genuinely staged,
    /// from recovery's independent point of view), and a commit attempt can
    /// lose to a concurrent recovery *abort*. This method always branches
    /// on what actually happened, never on which decision it proposed.
    ///
    /// **Resolve is asynchronous, post-ack, on the successful-commit path**
    /// (ADR 0018 §2/PR5 — the PR4 amendment's own flagged deviation, now
    /// lifted): once the anchor's commit is durable, this returns
    /// immediately and spawns a best-effort resolve of every participant
    /// (anchor's own keys included) in the background — safe to leave
    /// un-awaited now that `txn_resolver_loop` exists as the safety net
    /// that eventually finishes any resolve this spawn doesn't get to (a
    /// crash, a transient forward failure). The abort paths still resolve
    /// synchronously before returning — there is no successful ack to speed
    /// up on an error return, so the extra safety margin costs nothing.
    ///
    /// **ADR 0046 D1 amendment (re-scoped under ADR 0049)**: for a
    /// transaction touching at least one **images-carrying** table (an
    /// index or a stream — `dynamo::txn_resolve_awaited`), the async-spawn
    /// above is instead an **awaited, bounded** resolve
    /// (`TXN_RESOLVE_ALL_AWAIT_BUDGET`, parallelized across participants
    /// via `resolve_all_parallel`) — LSI rows and the GSI/stream change
    /// record only appear at resolve (materialize-at-resolve, A1), so an
    /// ack-then-async-resolve window would leave a committed write readable
    /// on the base table but transiently absent from its index/stream. A
    /// timeout still acks (delayed, never denied). Every other transaction
    /// — including a marker-table one, which since ADR 0049 also stages
    /// `pending` kind writes but has no index/stream consumer to protect —
    /// keeps the fire-and-forget spawn and the **sequential** `resolve_all`
    /// (parallelizing it universally measurably destabilized the torn-pair
    /// hard-gate test under concurrent load, twice now: once during the D1
    /// delivery, and again when ADR 0049's constant-true gate briefly
    /// re-universalized it by implication; see `resolve_all_parallel`'s own
    /// doc and `dynamo::txn_resolve_awaited`'s for the full account).
    ///
    /// **`write_conditions`** (ADR 0018 §2 apply-time write-key conditions
    /// amendment) — `(table, key, expected)` own-key byte-level OCC
    /// conditions checked at *apply* time on the key's own tablet, upgrading
    /// a write action's own condition from same-node-only protection
    /// (`ctx.data().rmw_lock`) to full cross-node correctness: `key` MUST be
    /// one of `writes`' own keys (an `Err` otherwise) — a condition on a key
    /// this transaction does not write belongs in `preconditions` instead
    /// (see [`TxnWriteCondition`]'s doc for why mixing them up is exactly
    /// the self-referential-stall bug the PR7 amendment documented).
    /// Split one (table, tablet) group's [`TxnTableWrite`]s into the
    /// already-concrete writes `RaftKvNode::txn_stage_anchor`/
    /// `txn_stage_participant` can take directly, and the pending
    /// kind-write-path ones [`ClientCtx::txn_stage_local`] must still
    /// evaluate at the leader (ADR 0046 U3, PR2) — see [`TxnTableWrite`]'s
    /// doc for why exactly one of `value`/`pending` is ever `Some`.
    fn split_group(
        group: Vec<TxnTableWrite>,
    ) -> Result<(Vec<TxnWrite>, Vec<PendingKindWrite>), String> {
        let mut writes = Vec::new();
        let mut pending = Vec::new();
        for w in group {
            match (w.value, w.pending) {
                // A plain-value transactional write only ever comes from the
                // raw client protocol now (`ClientRequest::Txn` — the Dynamo
                // edge's `run_transact` always builds `pending` kind-write
                // specs under ADR 0049's constant-true gate), and it too must
                // leave the ADR 0049 §3 stage marker: a raw write staged
                // during an ADR 0050 split build would otherwise be invisible
                // to the build's change-log tail until resolve — which can
                // land after the parent is gone. Prefix = the write's own
                // full key bytes (a raw write has no pk/sk decomposition —
                // the finest per-key dirty hint, leading with the key's own
                // token so the apply-time token validation holds), `base_sk`
                // empty.
                (Some(value), None) => {
                    let marker = crate::dynamo::stage_marker_change_log(&w.key, Vec::new());
                    let mut write = TxnWrite::plain(w.key, Some(value));
                    write.stage_marker = Some(marker);
                    writes.push(write);
                }
                (None, Some(p)) => pending.push(p),
                (None, None) => {
                    return Err(format!(
                        "cp_txn: write to table `{}` key {:?} has neither a value nor a \
                         pending kind-write spec",
                        w.table, w.key
                    ));
                }
                (Some(_), Some(_)) => {
                    return Err(format!(
                        "cp_txn: write to table `{}` key {:?} has both a value and a pending \
                         kind-write spec (exactly one is expected)",
                        w.table, w.key
                    ));
                }
            }
        }
        Ok((writes, pending))
    }

    pub(crate) async fn cp_txn(
        &self,
        writes: Vec<TxnTableWrite>,
        preconditions: Vec<TxnPrecondition>,
        write_conditions: Vec<TxnWriteCondition>,
    ) -> Result<HlcTimestamp, TxnAbortReason> {
        if writes.is_empty() {
            return Err(TxnAbortReason::Other(
                "cp_txn: writes must be non-empty".into(),
            ));
        }
        // **Load-bearing validation, not a redundant belt-and-suspenders
        // check**: `RaftKvNode::txn_stage` (the anchor's own stage) hard-
        // `assert!`s its anchor key is at least `TOKEN_BYTES` long (ADR
        // 0022) — a sound invariant when only trusted internal callers
        // (a test, or the Dynamo edge, which always builds ADR-0022-shaped
        // keys) ever reached it. This is the **first** wire-facing caller
        // that can hand it an arbitrary client-supplied key — a short key
        // would panic this whole node process (a real DoS vector), not
        // fail gracefully. Validate every write's key up front (not just
        // the anchor's — a future reordering of `writes` should not
        // resurface this) and return a client-facing error instead of ever
        // reaching that assert.
        if let Some(w) = writes.iter().find(|w| w.key.len() < TOKEN_BYTES) {
            return Err(TxnAbortReason::Other(format!(
                "txn key {:?} of table `{}` must be at least {TOKEN_BYTES} bytes long \
                 (ADR 0022) for a multi-participant transaction",
                w.key, w.table
            )));
        }

        // Auto-provision every distinct table's first tablet on demand, like
        // `cp_write`.
        let mut seen_tables: BTreeSet<String> = BTreeSet::new();
        for w in &writes {
            if seen_tables.insert(w.table.clone())
                && !self.effective_metadata().has_table_tablet(&w.table)
            {
                self.provision_tablet(&w.table)
                    .await
                    .map_err(TxnAbortReason::Other)?;
            }
        }

        // Precondition check #1 (pre-stage). A mismatch here is a
        // `ConditionCheck` action's own cross-key OCC (`preconditions`, never
        // a write's own key — see `TxnWriteCondition`'s doc) — not one of
        // this amendment's two typed reasons (ADR 0018's 2026-08-24
        // `CancellationReasons` amendment, issue #374 C2b left this path
        // aggregate-only; `dynamo.rs::run_transact`'s own coordinator-side
        // preflight already flags a `ConditionCheck` failure by index before
        // `cp_txn` is ever called, so this re-check only fires on a genuine
        // race the preflight couldn't have seen).
        let observed = self
            .check_preconditions(&preconditions)
            .await
            .map_err(TxnAbortReason::Other)?;

        // Own-key condition lookup, consumed (via `remove`) as `writes` is
        // grouped below — whatever's left over named a key that isn't one
        // of `writes`' own, a caller error (see `write_conditions`'s doc).
        let mut condition_map: BTreeMap<(String, Vec<u8>), Option<Vec<u8>>> = BTreeMap::new();
        for (table, key, expected) in write_conditions {
            condition_map.insert((table, key), expected);
        }

        // ADR 0046 D1 (re-scoped under ADR 0049): whether this transaction
        // must AWAIT its post-commit resolve — only when a pending write
        // targets a table whose change records carry images (an index or a
        // stream; the consumer-visibility rationale D1 actually rests on).
        // Since ADR 0049's constant-true write-path gate, `pending.is_some()`
        // alone is true for EVERY transaction, and keying this branch on it
        // silently universalized the awaited `resolve_all_parallel`
        // configuration that `resolve_all_parallel`'s own comment records as
        // reproduced-red on the torn-pair hard-gate test — which duly went
        // intermittently red again. See `dynamo::txn_resolve_awaited`'s doc.
        let awaits_resolve = {
            let meta = self.effective_metadata();
            dynamo::txn_resolve_awaited(&meta, &writes)
        };

        // Group by (table, tablet), preserving first-seen order — `order[0]`
        // is the anchor. `condition_groups` mirrors `groups`' keying, only
        // populated for a (table, tablet) that owns at least one
        // conditioned key. Kept as the un-split `TxnTableWrite` (ADR 0046
        // U3, PR2) here — a group can mix plain (already-known) writes and
        // pending kind-write-path ones; [`split_group`] separates them right
        // before each group is actually staged.
        let mut order: Vec<(String, TabletId)> = Vec::new();
        let mut groups: BTreeMap<(String, TabletId), Vec<TxnTableWrite>> = BTreeMap::new();
        let mut condition_groups: BTreeMap<(String, TabletId), StageConditions> = BTreeMap::new();
        for w in writes {
            let tablet = self.tablet_for(&w.table, &w.key).ok_or_else(|| {
                TxnAbortReason::Other(format!("no tablet owns a txn key of table `{}`", w.table))
            })?;
            if let Some(expected) = condition_map.remove(&(w.table.clone(), w.key.clone())) {
                condition_groups
                    .entry((w.table.clone(), tablet))
                    .or_default()
                    .push((w.key.clone(), expected));
            }
            let gk = (w.table.clone(), tablet);
            if let std::collections::btree_map::Entry::Vacant(e) = groups.entry(gk.clone()) {
                e.insert(Vec::new());
                order.push(gk.clone());
            }
            groups.get_mut(&gk).expect("just inserted").push(w);
        }
        if let Some(((table, key), _)) = condition_map.into_iter().next() {
            return Err(TxnAbortReason::Other(format!(
                "cp_txn: a write-key condition named {table}/{key:?}, which is not one of this \
                 transaction's own write keys — use `preconditions` for a condition on a key \
                 this transaction does not write"
            )));
        }

        let anchor_gk = order[0].clone();
        let anchor_group = groups.remove(&anchor_gk).expect("anchor group present");
        let anchor_conditions = condition_groups.remove(&anchor_gk).unwrap_or_default();
        let (anchor_table, _anchor_tablet) = anchor_gk;
        let anchor_keys: Vec<Vec<u8>> = anchor_group.iter().map(|w| w.key.clone()).collect();
        let (anchor_writes, anchor_pending) =
            Self::split_group(anchor_group).map_err(TxnAbortReason::Other)?;

        // ADR 0018 §2/PR5 (task #18 fix): the anchor's record must name
        // every OTHER participant's `(table, span)` pairs up front, not
        // just its own — `groups` (with the anchor's own entry already
        // removed above) holds exactly that. Without this, in-doubt
        // recovery's `all_staged` check (`ClientCtx::txn_recover`) only
        // ever verifies the anchor's own keys against `intent_spans`,
        // trivially reporting "all staged" even when a real participant
        // never staged at all — a genuine cross-tablet atomicity
        // violation on the recovery path (see `docs/adr/
        // 0018-cross-tablet-transactions.md`'s corrective note on this).
        let participant_spans: Vec<(String, KeyRange)> = groups
            .iter()
            .flat_map(|((table, _tablet), group)| {
                let table = table.clone();
                group.iter().map(move |w| {
                    let mut end = w.key.clone();
                    end.push(0);
                    (table.clone(), KeyRange::new(w.key.clone(), Some(end)))
                })
            })
            .collect();

        let (txn_id, record_key, record_table, anchor_ts) = self
            .txn_prepare_pushing(
                &anchor_table,
                None,
                anchor_writes,
                anchor_conditions,
                participant_spans,
                anchor_pending,
            )
            .await?;

        // Every other participant stages concurrently.
        let participant_gks: Vec<(String, TabletId)> = order.into_iter().skip(1).collect();
        let participant_futs = participant_gks.iter().map(|gk| {
            let table = gk.0.clone();
            let group = groups.get(gk).expect("group present").clone();
            let conditions = condition_groups.get(gk).cloned().unwrap_or_default();
            let keys: Vec<Vec<u8>> = group.iter().map(|w| w.key.clone()).collect();
            let txn_id = txn_id.clone();
            let record_key = record_key.clone();
            let record_table = record_table.clone();
            async move {
                let (writes, pending) = match Self::split_group(group) {
                    Ok(split) => split,
                    Err(e) => return (table, keys, Err(TxnAbortReason::Other(e))),
                };
                let result = self
                    .txn_prepare_pushing(
                        &table,
                        Some((txn_id, record_key, record_table)),
                        writes,
                        conditions,
                        Vec::new(), // unused: a participant's own stage creates no record.
                        pending,
                    )
                    .await;
                (table, keys, result)
            }
        });
        let participant_results = futures::future::join_all(participant_futs).await;

        // ADR 0018 §2/PR5: `staged` now tracks *every* participant this
        // transaction actually touches, the anchor's own keys included —
        // `txn_decide_anchor` no longer resolves anything inline (recovery
        // needs the record's `intent_spans` to already list every
        // participant uniformly, and the resolve fan-out below treats them
        // identically too).
        let mut candidate = anchor_ts;
        let mut staged: Vec<(String, Vec<Vec<u8>>)> = vec![(anchor_table.clone(), anchor_keys)];
        let mut first_err: Option<TxnAbortReason> = None;
        for (table, keys, result) in participant_results {
            match result {
                Ok((_, _, _, ts)) => {
                    candidate = candidate.max(ts);
                    staged.push((table, keys));
                }
                Err(e) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
        }

        // Resolve every staged participant (best-effort, fire-and-forget —
        // a resolve failure never blocks a decision already durable on the
        // anchor; the resolver loop, ADR 0018 §2/PR5, is the safety net
        // that eventually finishes it).
        //
        // ADR 0018 §2/PR6 torn-resolve audit: every call site below passes
        // an `outcome` sourced from `txn_decide_anchor`'s own `Ok(..)`
        // return, which is itself always a **post-decision re-read**
        // (`txn_status_local`, inside `txn_decide_anchor`) — never the
        // caller's own proposed/candidate ts. This is load-bearing: once a
        // same-outcome-different-ts duplicate commit is a legal no-op
        // (ADR 0018 §2/PR6) rather than an assert, resolving with a
        // losing decider's own candidate instead of the actual, winning
        // decision would be exactly the torn-resolve hazard that
        // amendment's own review flagged.
        let resolve_all = |outcome: TxnOutcome, staged: Vec<(String, Vec<Vec<u8>>)>| {
            let this = self.clone();
            let txn_id = txn_id.clone();
            let record_key = record_key.clone();
            async move {
                for (table, keys) in staged {
                    let _ = this
                        .txn_resolve_participant(
                            &table,
                            txn_id.clone(),
                            record_key.clone(),
                            keys,
                            outcome.clone(),
                        )
                        .await;
                }
            }
        };
        // ADR 0046 D1: a **parallel** sibling of `resolve_all` above, used
        // only by the awaited-bounded branch further down (a transaction
        // touching a kind-write-path table) — fanning out to every
        // participant's own tablet leader concurrently instead of one at a
        // time is what makes a short fixed budget plausible at all once
        // there's more than one participant. Deliberately **not** used for
        // the ordinary fire-and-forget spawn path above: that path's
        // resolves already run fully in the background with no latency
        // budget to protect, and switching it to `join_all` measurably
        // destabilized a pre-existing, already-timing-sensitive regression
        // test (`dynamo_txn.rs`'s
        // `transact_get_items_never_observes_a_torn_pair_under_concurrent_writes`,
        // a tight concurrent-writer loop where a resolve's own wall-clock
        // latency doubles as the next transaction's own staging retry
        // budget) — reproduced red with the parallel version applied
        // universally, green again scoped like this. Not fully root-caused
        // (plausibly increased concurrent Raft/network load momentarily
        // slowing an individual resolve under this test's specific tight
        // loop, not a correctness bug — every resolve still completes,
        // `txn_resolver_loop` is the safety net either way), but the
        // sequential default is the proven-stable one, so parallelism stays
        // opt-in to where D1 actually needs it.
        //
        // ADR 0049 postscript, proving the scoping is load-bearing: when the
        // constant-true write-path gate made every transaction stage
        // `pending` kind writes, the awaited branch below (then keyed on
        // "any pending") re-universalized this parallel path by implication
        // — and this same test went intermittently red again (a
        // budget-expired ack racing the writer's next same-key stage into
        // `TXN_STAGE_PUSH_ATTEMPTS` exhaustion). The branch is now keyed on
        // `dynamo::txn_resolve_awaited` (images-carrying tables only), which
        // restores the exact pre-ADR-0049 behavior for every marker-only
        // transaction.
        let resolve_all_parallel = |outcome: TxnOutcome, staged: Vec<(String, Vec<Vec<u8>>)>| {
            let this = self.clone();
            let txn_id = txn_id.clone();
            let record_key = record_key.clone();
            async move {
                let futs = staged.into_iter().map(|(table, keys)| {
                    let this = this.clone();
                    let txn_id = txn_id.clone();
                    let record_key = record_key.clone();
                    let outcome = outcome.clone();
                    async move {
                        let _ = this
                            .txn_resolve_participant(&table, txn_id, record_key, keys, outcome)
                            .await;
                    }
                });
                futures::future::join_all(futs).await;
            }
        };

        if let Some(reason) = first_err {
            // ADR 0018 §2/PR5 decision-semantics amendment: this abort
            // attempt can itself lose to a concurrent recovery *commit* on
            // the same record (every participant genuinely staged after
            // all, from recovery's independent point of view) — report the
            // record's **actual** outcome, never assume the abort we asked
            // for is what happened.
            match self
                .txn_decide_anchor(
                    &anchor_table,
                    txn_id.clone(),
                    record_key.clone(),
                    false,
                    candidate,
                    None,
                )
                .await
            {
                Ok(TxnOutcome::Aborted) => {
                    resolve_all(TxnOutcome::Aborted, staged).await;
                    // Propagate the participant's own typed reason verbatim
                    // (ADR 0018's 2026-08-24 `CancellationReasons`
                    // amendment) — it already names the responsible action;
                    // wrapping it in another `Other` string here would erase
                    // the `ConditionFailed`/`TransactionConflict` distinction
                    // `dynamo.rs::run_transact` needs to flag the right index.
                    Err(reason)
                }
                Ok(TxnOutcome::Committed { commit_ts }) => {
                    resolve_all(TxnOutcome::Committed { commit_ts }, staged).await;
                    Ok(commit_ts)
                }
                Err(e) => Err(TxnAbortReason::Other(format!(
                    "transaction aborted: {reason} (and abort itself failed: {e})"
                ))),
            }
        } else if !preconditions.is_empty()
            && self
                .check_preconditions(&preconditions)
                .await
                .map_err(TxnAbortReason::Other)?
                != observed
        {
            // Precondition check #2 (pre-commit refresh — see this method's own
            // doc for why this is a value re-check, not the ADR's ts-based one).
            match self
                .txn_decide_anchor(
                    &anchor_table,
                    txn_id.clone(),
                    record_key.clone(),
                    false,
                    candidate,
                    None,
                )
                .await
            {
                Ok(TxnOutcome::Aborted) => {
                    resolve_all(TxnOutcome::Aborted, staged).await;
                    Err(TxnAbortReason::Other(
                        "a precondition changed between prepare and commit; retry".into(),
                    ))
                }
                Ok(TxnOutcome::Committed { commit_ts }) => {
                    resolve_all(TxnOutcome::Committed { commit_ts }, staged).await;
                    Ok(commit_ts)
                }
                Err(e) => Err(TxnAbortReason::Other(format!(
                    "transaction aborted: a precondition changed between prepare and commit \
                     (and abort itself failed: {e})"
                ))),
            }
        } else {
            match self
                .txn_decide_anchor(
                    &anchor_table,
                    txn_id.clone(),
                    record_key.clone(),
                    true,
                    candidate,
                    None,
                )
                .await
                .map_err(TxnAbortReason::Other)?
            {
                TxnOutcome::Committed { commit_ts } => {
                    // ADR 0018 §2/PR5: the deviation PR4 flagged, lifted —
                    // the anchor's commit is already durable and IS the
                    // atomic commit point (the client can be told "done"
                    // right now); every participant's resolve (anchor's own
                    // keys included) is best-effort and can safely happen
                    // after the ack, since a crash here leaves nothing
                    // ambiguous — `txn_resolver_loop` finishes it. This is
                    // strictly safer than the interim synchronous shape,
                    // not merely faster: it no longer holds the client
                    // response hostage to every participant's own
                    // liveness/latency.
                    //
                    // ADR 0046 D1: for a transaction touching any
                    // images-carrying table (index/stream), LSI rows and
                    // the stream/GSI change record only appear at resolve
                    // (materialize-at-resolve, A1) — an ack-then-async-
                    // resolve window would leave a committed write readable
                    // on the base table but transiently absent from its
                    // index/stream. Await `resolve_all` under a short
                    // bounded budget first; a timeout still acks (delayed,
                    // never denied — `txn_resolver_loop` remains the safety
                    // net for whatever the bound didn't cover). Every other
                    // transaction — marker-only tables included, since ADR
                    // 0049 — keeps the original fire-and-forget sequential
                    // spawn unchanged (see `dynamo::txn_resolve_awaited`).
                    if awaits_resolve {
                        let _ = tokio::time::timeout(
                            TXN_RESOLVE_ALL_AWAIT_BUDGET,
                            resolve_all_parallel(TxnOutcome::Committed { commit_ts }, staged),
                        )
                        .await;
                    } else {
                        tokio::spawn(resolve_all(TxnOutcome::Committed { commit_ts }, staged));
                    }
                    Ok(commit_ts)
                }
                // The anchor's own commit lost to a concurrent recovery
                // abort (a duelling decider, ADR 0018 §2/PR5) — report the
                // abort honestly rather than a false success.
                TxnOutcome::Aborted => {
                    resolve_all(TxnOutcome::Aborted, staged).await;
                    Err(TxnAbortReason::Other(
                        "transaction aborted: lost to a concurrent in-doubt-recovery decision"
                            .into(),
                    ))
                }
            }
        }
    }

    /// Read every `(table, key)` in `preconditions` (an ordinary
    /// linearizable read) and compare to its `expected` value (`None` =
    /// "must be absent"); `Err` on the first mismatch (a genuine, immediate
    /// precondition failure — not a routing error, so never retried).
    /// Returns the observed `(table, key, actual)` triples so
    /// [`cp_txn`](Self::cp_txn) can compare them again later (the pre-commit
    /// refresh check).
    async fn check_preconditions(
        &self,
        preconditions: &[TxnPrecondition],
    ) -> Result<Vec<TxnPrecondition>, String> {
        let mut observed = Vec::with_capacity(preconditions.len());
        for (table, key, expected) in preconditions {
            // A transaction precondition is an OCC check the commit
            // decision rests on: always linearizable (ADR 0055), never the
            // cheap path, whatever the client's own reads asked for.
            let actual = self
                .cp_read(table, key.clone(), ReadConsistency::Strong)
                .await?;
            if &actual != expected {
                return Err(format!(
                    "transaction precondition failed for {table}/{key:?}: expected {expected:?}, \
                     found {actual:?}"
                ));
            }
            observed.push((table.clone(), key.clone(), actual));
        }
        Ok(observed)
    }

    /// Linearizable CP range **scan** of `table` over `[start, end)` up to `limit`
    /// keys (ADR 0017/0023): a **per-table fan-out**. The scan is split across the
    /// `table`'s tablets whose token sub-range overlaps `[start, end)` (token order),
    /// each scanned on its own group leader (ReadIndex, forwarded if this node isn't
    /// it) and merged — so the result is in token order, the only order a hash ring
    /// offers. A freshly created table has a single whole-ring tablet, so the loop
    /// runs once; a split table fans out across its halves.
    pub(crate) async fn cp_scan(
        &self,
        table: &str,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: Option<usize>,
        reverse: bool,
        consistency: ReadConsistency,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        // The table's tablets overlapping [start, end), in token (range.start) order.
        // `end == None` is unbounded above (a whole-table scan).
        //
        // `effective_metadata()`, not `self.control.metadata_cached()`
        // directly (ADR 0035 PR5 staleness-audit fix): the latter is
        // permanently empty on a control-plane-follower-less growth node
        // (ADR 0030), which would silently compute zero overlapping ranges
        // and make `cp_scan` return an empty result forever on such a
        // node — the exact staleness class `cp_put`/`cp_get`/`cp_batch_write`'s
        // `has_table_tablet` gate already guards against, just missed here.
        let mut ranges: Vec<KeyRange> = self
            .effective_metadata()
            .tablets_for_table(table)
            // ADR 0050: a `Building` split child overlaps its still-serving
            // parent — scanning both would double-serve (or serve a
            // half-copied engine's slice of) the overlap.
            .filter(|(_, t)| t.is_routable())
            .map(|(_, t)| t.range.clone())
            .filter(|r| {
                // [r.start, r.end) overlaps [start, end), each upper bound optional.
                end.as_deref().is_none_or(|e| r.start.as_slice() < e)
                    && r.end.as_deref().is_none_or(|re| start.as_slice() < re)
            })
            .collect();
        ranges.sort();
        // Descending: visit the overlapping tablets highest-token-first too,
        // so `limit` fills from the top of the whole scanned span rather than
        // from the top of merely its lowest tablet.
        if reverse {
            ranges.reverse();
        }
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for r in ranges {
            if let Some(l) = limit
                && out.len() >= l
            {
                break;
            }
            // Clip the scan window to this tablet's sub-range; the exclusive upper
            // bound is the lesser of the tablet's end and the scan's end (None = ∞).
            let sub_start = start.clone().max(r.start);
            let sub_end: Option<Vec<u8>> = match (r.end, &end) {
                (None, e) => e.clone(),
                (Some(re), None) => Some(re),
                (Some(re), Some(e)) => Some(re.min(e.clone())),
            };
            if let Some(se) = &sub_end
                && sub_start.as_slice() >= se.as_slice()
            {
                continue;
            }
            let remaining = limit.map(|l| l - out.len());
            out.extend(
                self.cp_scan_one(table, sub_start, sub_end, remaining, reverse, consistency)
                    .await?,
            );
        }
        if let Some(l) = limit {
            out.truncate(l);
        }
        Ok(out)
    }

    /// Scan a single tablet's sub-range on its group leader (the body the fan-out
    /// [`cp_scan`](Self::cp_scan) calls per overlapping tablet). `start` resolves to
    /// exactly one tablet of `table`, so it routes/forwards like any other CP op.
    /// `end == None` is unbounded above (the last tablet of a whole-table scan).
    async fn cp_scan_one(
        &self,
        table: &str,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: Option<usize>,
        reverse: bool,
        consistency: ReadConsistency,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        // ADR 0055: the cheap path, per tablet — a fan-out falls back only
        // for the sub-ranges no replica could serve, never wholesale.
        if consistency.is_eventual()
            && let Some(p) = self
                .cp_scan_one_eventual(table, &start, end.as_deref(), limit, reverse)
                .await
        {
            return Ok(p);
        }
        let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
        loop {
            let err = match self.cp_route(table, &start).await {
                CpRoute::Local(leader) => {
                    match Self::cp_scan_local(&leader, &start, end.as_deref(), limit, reverse).await
                    {
                        Ok(p) => return Ok(p),
                        Err(e) => e,
                    }
                }
                CpRoute::Forward(addr) => {
                    let request = ClientRequest::Scan {
                        start: start.clone(),
                        end: end.clone(),
                        limit,
                        reverse,
                        table: table.to_owned(),
                        stale: false,
                    };
                    match self.cp_forward(table, &start, addr, request).await {
                        ClientResponse::Pairs(p) => return Ok(p),
                        ClientResponse::Error(e) => e,
                        other => {
                            return Err(format!(
                                "unexpected reply to forwarded CP scan: {other:?}"
                            ));
                        }
                    }
                }
                CpRoute::None => return Err("no CP group leader reachable".into()),
            };
            if !Self::read_should_retry(&err) || tokio::time::Instant::now() >= deadline {
                return Err(err);
            }
            tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// Linearizable CP range scan of one of `table`'s non-base row-kind
    /// scopes over `[start, end)` (ADR 0041 §3/§5) — the LSI `Query` read
    /// primitive. **Not** a per-table fan-out like [`cp_scan`](Self::cp_scan):
    /// an LSI query is scoped to one base partition, which is one tablet by
    /// construction (the same tablet the base row itself lives on), so
    /// `start` and `end` must resolve to that same tablet — checked here
    /// rather than assumed, mirroring [`cp_kind_write`](Self::cp_kind_write)'s
    /// cross-tablet guard: silently scanning only the first tablet's share of
    /// a straddling range would be a silent partial read. `limit` is pushed
    /// down to [`cp_scan_kind_one`](Self::cp_scan_kind_one) — the LSI `Query`
    /// pagination primitive (`animusd::dynamo`'s bounded, windowed
    /// `paginated_kind_examine_one`) now pages the same way a base/GSI
    /// `Query` does, rather than the `None`-always gap this used to have.
    #[allow(clippy::too_many_arguments)] // one kind-scoped Query's full shape
    pub(crate) async fn cp_scan_kind(
        &self,
        table: &str,
        kind: u8,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: Option<usize>,
        reverse: bool,
        consistency: ReadConsistency,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let start_tablet = self
            .tablet_for(table, &start)
            .ok_or_else(|| format!("no tablet owns the kind-scan start of table `{table}`"))?;
        if self.tablet_for(table, &end) != Some(start_tablet) {
            return Err(format!(
                "kind-scan range of table `{table}` spans more than one tablet; \
                 an LSI query is scoped to one partition"
            ));
        }
        self.cp_scan_kind_one(table, kind, start, Some(end), limit, reverse, consistency)
            .await
    }

    /// A **table-wide fan-out** of the kind-scoped scan (ADR 0041 §5) — the
    /// LSI `Scan` read primitive. Unlike [`cp_scan_kind`](Self::cp_scan_kind)'s
    /// single-tablet routing (an LSI `Query` is scoped to one base partition,
    /// hence one tablet by construction), a table-wide `Scan` against an LSI
    /// sweeps every tablet of `table`'s own ring in token order — mirroring
    /// [`cp_scan`](Self::cp_scan)'s per-table fan-out exactly, but scanning
    /// each overlapping tablet's `kind`-scoped scope instead of its base
    /// scope. `end == None` is unbounded above (a whole-table scan); the one
    /// tablet whose *own* metadata range end is also `None` (an unsplit or
    /// not-yet-split tail tablet) is asked to scan `[sub_start, None)` too —
    /// no finite byte string can bound a kind scope's logical keyspace in
    /// general (see [`RaftKvNode::linearizable_scan_kind`]'s doc), so that
    /// bound is derived inside the primitive itself, not computed here.
    pub(crate) async fn cp_scan_kind_table(
        &self,
        table: &str,
        kind: u8,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: Option<usize>,
        consistency: ReadConsistency,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        // The table's tablets overlapping [start, end), in token order — the
        // identical range math `cp_scan` uses (see that method's doc for the
        // `effective_metadata()` staleness-audit rationale, which applies
        // here unchanged).
        let mut ranges: Vec<KeyRange> = self
            .effective_metadata()
            .tablets_for_table(table)
            // ADR 0050: skip `Building` children (see `cp_scan`'s own filter).
            .filter(|(_, t)| t.is_routable())
            .map(|(_, t)| t.range.clone())
            .filter(|r| {
                end.as_deref().is_none_or(|e| r.start.as_slice() < e)
                    && r.end.as_deref().is_none_or(|re| start.as_slice() < re)
            })
            .collect();
        ranges.sort();
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for r in ranges {
            if let Some(l) = limit
                && out.len() >= l
            {
                break;
            }
            let sub_start = start.clone().max(r.start);
            let sub_end: Option<Vec<u8>> = match (r.end, &end) {
                (None, e) => e.clone(),
                (Some(re), None) => Some(re),
                (Some(re), Some(e)) => Some(re.min(e.clone())),
            };
            if let Some(se) = &sub_end
                && sub_start.as_slice() >= se.as_slice()
            {
                continue;
            }
            // Per-tablet cap (ADR 0041 §5 as-built) — the identical
            // `remaining` math `cp_scan` applies across its own tablets: how
            // many more rows this table-wide fan-out still needs after what
            // prior tablets already contributed. Threaded into the
            // `KindScan` request so a tablet with far more matching rows
            // than `remaining` doesn't ship its whole sub-range over the
            // wire only to be truncated here — this is still **not
            // pushdown** (`StorageEngine::scan` has no limit of its own; see
            // `RaftKvNode::local_scan_kind`'s doc), just a smaller reply and
            // less coordinator-side memory.
            let remaining = limit.map(|l| l - out.len());
            out.extend(
                self.cp_scan_kind_one(
                    table,
                    kind,
                    sub_start,
                    sub_end,
                    remaining,
                    false,
                    consistency,
                )
                .await?,
            );
        }
        if let Some(l) = limit {
            out.truncate(l);
        }
        Ok(out)
    }

    /// Scan a single tablet's kind-scoped sub-range on its group leader (the
    /// body both [`cp_scan_kind`](Self::cp_scan_kind) and
    /// [`cp_scan_kind_table`](Self::cp_scan_kind_table) call). `start`
    /// resolves to exactly one tablet of `table`, so it routes/forwards like
    /// any other CP op. `end == None` is unbounded above. `limit` is a
    /// **per-tablet cap, not pushdown** (see `RaftKvNode::local_scan_kind`'s
    /// doc) — [`cp_scan_kind`](Self::cp_scan_kind) always passes `None` (an
    /// LSI `Query` has no `Limit`, ADR 0041); only
    /// [`cp_scan_kind_table`](Self::cp_scan_kind_table) passes a real value.
    #[allow(clippy::too_many_arguments)] // one tablet's kind-scoped page, plus consistency
    async fn cp_scan_kind_one(
        &self,
        table: &str,
        kind: u8,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: Option<usize>,
        reverse: bool,
        consistency: ReadConsistency,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        // ADR 0055, per tablet — see `cp_scan_one`'s identical arm.
        if consistency.is_eventual()
            && let Some(p) = self
                .cp_scan_kind_one_eventual(table, kind, &start, end.as_deref(), limit, reverse)
                .await
        {
            return Ok(p);
        }
        let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
        loop {
            let err = match self.cp_route(table, &start).await {
                CpRoute::Local(leader) => {
                    match Self::cp_scan_kind_local(
                        &leader,
                        kind,
                        &start,
                        end.as_deref(),
                        limit,
                        reverse,
                    )
                    .await
                    {
                        Ok(p) => return Ok(p),
                        Err(e) => e,
                    }
                }
                CpRoute::Forward(addr) => {
                    let request = ClientRequest::KindScan {
                        table: table.to_owned(),
                        kind,
                        start: start.clone(),
                        end: end.clone(),
                        limit,
                        reverse,
                        stale: false,
                    };
                    match self.cp_forward(table, &start, addr, request).await {
                        ClientResponse::Pairs(p) => return Ok(p),
                        ClientResponse::Error(e) => e,
                        other => {
                            return Err(format!(
                                "unexpected reply to forwarded CP kind scan: {other:?}"
                            ));
                        }
                    }
                }
                CpRoute::None => return Err("no CP group leader reachable".into()),
            };
            if !Self::read_should_retry(&err) || tokio::time::Instant::now() >= deadline {
                return Err(err);
            }
            tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// Serve a linearizable **kind-scoped scan** on a known-leader local
    /// handle, enforcing the read-side scope pre-check (ADR 0033) — the
    /// kind-scan dual of [`cp_scan_local`](Self::cp_scan_local): a scope that
    /// has not yet caught up to the metadata-derived request window (a
    /// split's narrow in flight) would otherwise silently truncate the
    /// results rather than error. `end == None` is unbounded above; `limit`
    /// is a **per-tablet cap, not pushdown** (see
    /// `RaftKvNode::local_scan_kind`'s doc).
    async fn cp_scan_kind_local(
        leader: &CpGroup,
        kind: u8,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let requested = KeyRange::new(start.to_vec(), end.map(<[u8]>::to_vec));
        if !leader.scope_range().contains_range(&requested) {
            return Err(format!(
                "scan window {requested:?} outside tablet's current range (stale routing, likely a split crossover); retry"
            ));
        }
        let served = if reverse {
            leader
                .linearizable_scan_kind_rev(kind, start, end, limit)
                .await
        } else {
            leader.linearizable_scan_kind(kind, start, end, limit).await
        };
        match served {
            Some(p) => Ok(p),
            None => Err("CP group leader moved; retry".into()),
        }
    }

    /// Propose a CP write on a **known-leader** local handle and wait until it is
    /// committed + durable + applied before returning — durable-before-ack.
    ///
    /// We confirm via a **local** read on the leader, not a linearizable ReadIndex
    /// barrier: the leader applies an entry only after a quorum commit + WAL fsync
    /// (durable-before-visible in `animus-cp-data`), so the leader's local read
    /// reflecting our value means it is durable. A per-write quorum barrier would
    /// not scale under concurrent load. (If we lose leadership before commit, the
    /// entry may be truncated and never appear locally — the confirm loop then
    /// ends early via [`confirm_wait_is_futile`](Self::confirm_wait_is_futile)
    /// with a retryable error rather than polling out the whole
    /// [`CLIENT_TIMEOUT`]: the write did not confirm, and the caller's retry
    /// re-resolves routing.)
    ///
    /// **Pre-propose range check (ADR 0028 write fences).** `cp_route` can hand
    /// us a `Local` leader off a stale `Metadata` view during a split's
    /// crossover window — this node still believes it hosts the leader for a
    /// range wider than the tablet's group has actually narrowed to (e.g. this
    /// key now belongs to a just-minted sibling on the same shared engine).
    /// Stamping the leader's own `fence` on the proposed entry (below) is
    /// necessary but **not sufficient on its own**: a fenced-out entry still
    /// commits and applies as a no-op, and *if* a confirm mechanism ever keyed
    /// success on a coarser signal than exact value equality (e.g. "has this
    /// index applied yet" — a no-op still advances that watermark) it would
    /// **falsely ack** a write that never actually landed anywhere. This confirm
    /// loop polls value equality (success is never keyed on the coarser
    /// applied-index signal — [`confirm_wait_is_futile`](Self::
    /// confirm_wait_is_futile) only ever ends a wait *early with an error*),
    /// which degrades that hazard to "returns a retryable error" rather than a
    /// false ack — but that is a property of *this* poll, not a defense to rely on, so the
    /// explicit pre-check below is the actual guard: reject an out-of-range key
    /// **before proposing at all**, in the same `Err` shape as the `NotLeader`
    /// case, so the caller (`cp_write`) sees an ordinary routing failure and its
    /// own retry re-resolves `cp_route` (reaching the correct child once this
    /// node's view of the split has caught up), instead of a write that silently
    /// shadowed/corrupted the child's data. The embedded `fence` (stamped from
    /// the *same* `scope_range()` read used for the check) still rides the
    /// entry regardless, to cover the residual race between this check and the
    /// entry's actual apply (the scope can narrow further in between) — see
    /// [`RaftKvNode::scope_range`]'s doc for why that sliver isn't free to
    /// close; a write landing in it is *dropped* (a safe no-op that this loop
    /// times out on), never mis-applied.
    async fn cp_put_local(leader: &CpGroup, key: Vec<u8>, value: Vec<u8>) -> Result<(), String> {
        Self::frozen_refusal(leader)?;
        let fence = leader.scope_range();
        if !fence.contains(&key) {
            return Err(
                "key outside tablet's current range (stale routing, likely a split crossover); retry".into(),
            );
        }
        match leader.put(key.clone(), value.clone()) {
            ProposeResult::Accepted { index, .. } => {
                let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
                let mut poll = CP_CONFIRM_POLL_INIT;
                loop {
                    if leader.local_get(&key).await.as_deref() == Some(value.as_slice()) {
                        return Ok(());
                    }
                    if Self::confirm_wait_is_futile(leader, index) {
                        // Close the probe-vs-apply race before giving up.
                        if leader.local_get(&key).await.as_deref() == Some(value.as_slice()) {
                            return Ok(());
                        }
                        return Err(
                            "CP write superseded before its effect appeared (leadership churn \
                             or an apply-time no-op); retry"
                                .into(),
                        );
                    }
                    if tokio::time::Instant::now() >= deadline {
                        return Err("CP write did not commit in time".into());
                    }
                    tokio::time::sleep(poll).await;
                    poll = (poll * 2).min(CP_CONFIRM_POLL_MAX);
                }
            }
            ProposeResult::NotLeader { .. } => Err("CP group leader moved; retry".into()),
        }
    }

    /// Propose a CP delete on a **known-leader** local handle and wait until the
    /// key reads absent locally (committed + durable + applied tombstone) —
    /// durable-before-ack. Local read, not a barrier, as in
    /// [`cp_put_local`](Self::cp_put_local) — and the **same pre-propose range
    /// check** against the leader's live `scope_range()` before proposing, for
    /// the same reason: a stale-routed delete for a key that now belongs to a
    /// split sibling must not be silently accepted as a fenced-out no-op (which
    /// would otherwise leave the sibling's real value untouched but let the
    /// caller believe the delete succeeded once the parent's own read of that
    /// physical key coincidentally reads absent — see `cp_put_local`'s doc for
    /// the full hazard and why the pre-check, not just the embedded fence, is
    /// the actual guard).
    async fn cp_delete_local(leader: &CpGroup, key: Vec<u8>) -> Result<(), String> {
        Self::frozen_refusal(leader)?;
        let fence = leader.scope_range();
        if !fence.contains(&key) {
            return Err(
                "key outside tablet's current range (stale routing, likely a split crossover); retry".into(),
            );
        }
        match leader.delete(key.clone()) {
            ProposeResult::Accepted { index, .. } => {
                let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
                let mut poll = CP_CONFIRM_POLL_INIT;
                loop {
                    if leader.local_get(&key).await.is_none() {
                        return Ok(());
                    }
                    if Self::confirm_wait_is_futile(leader, index) {
                        // Close the probe-vs-apply race before giving up.
                        if leader.local_get(&key).await.is_none() {
                            return Ok(());
                        }
                        return Err(
                            "CP delete superseded before its effect appeared (leadership churn \
                             or an apply-time no-op); retry"
                                .into(),
                        );
                    }
                    if tokio::time::Instant::now() >= deadline {
                        return Err("CP delete did not commit in time".into());
                    }
                    tokio::time::sleep(poll).await;
                    poll = (poll * 2).min(CP_CONFIRM_POLL_MAX);
                }
            }
            ProposeResult::NotLeader { .. } => Err("CP group leader moved; retry".into()),
        }
    }

    /// Map a forwarded-op reply that should be a bare ack into `Result<(), String>`.
    fn ok_or_err(resp: ClientResponse, what: &str) -> Result<(), String> {
        match resp {
            ClientResponse::PutOk => Ok(()),
            ClientResponse::Error(e) => Err(e),
            other => Err(format!("unexpected reply to {what}: {other:?}")),
        }
    }

    /// Route a CP-mode **read** for the plain client API (returns a wire
    /// [`ClientResponse`]). Thin adapter over [`cp_read`](Self::cp_read).
    async fn cp_get(&self, table: &str, key: Vec<u8>, stale: bool) -> ClientResponse {
        // A table with no tablet has no data (ADR 0023) — absent, no routing wait.
        // `effective_metadata` (not `metadata_cached()` directly): on a growth
        // node (ADR 0030) the local raft never reflects a table created
        // before it existed.
        if !self.effective_metadata().has_table_tablet(table) {
            return ClientResponse::Value(None);
        }
        match self
            .cp_read(table, key, ReadConsistency::from_consistent_read(!stale))
            .await
        {
            Ok(v) => ClientResponse::Value(v),
            Err(e) => ClientResponse::Error(e),
        }
    }

    /// This node's local replica's own leader **hint** for `tablet` — `(id,
    /// client-API address)` — as it currently sees it (`leader()` hint →
    /// `client_route`). `None` if this node hosts no replica of `tablet`, the
    /// replica has no leader hint yet (mid-election), or the hinted id has no
    /// known route. The one shared lookup behind both
    /// [`cp_forward_target`](Self::cp_forward_target) (this node deciding
    /// where to route/forward *before* proposing) and a "not the leader
    /// here" refusal's embedded hint
    /// ([`topology::format_not_leader_refusal`]) — a node refusing a
    /// forwarded op always hosts *some* local replica of the tablet (that's
    /// why it was targeted), so its own knowledge of the group's leader is
    /// exactly the hint a forwarder chasing a wrong first guess needs.
    fn cp_leader_hint(&self, tablet: TabletId) -> Option<(NodeId, SocketAddr)> {
        // Since ADR 0026 Stage B a tablet's CP group member id **is** simply the
        // base `raftkv` id, so the local replica's leader hint is already an
        // `intra_route` key — no more base<->member translation needed.
        // Intra-flavored (ADR 0047): the receiving end of a forward
        // (`cp_serve_forwarded`) is only ever reachable on the intra port.
        let leader = self.edge.local_cp(tablet).and_then(|n| n.leader())?;
        let addr = self.intra_addr(leader.clone())?;
        Some((leader, addr))
    }

    /// The intra-cluster address to forward a `tablet` op to — see
    /// [`cp_leader_hint`](Self::cp_leader_hint) (the caller waits rather than
    /// guessing when there is no hint yet, so it never forwards a CP op to a
    /// non-leader, including itself).
    fn cp_forward_target(&self, tablet: TabletId) -> Option<SocketAddr> {
        self.cp_leader_hint(tablet).map(|(_, addr)| addr)
    }

    /// A "not the leader here" refusal for a forwarded CP op that resolved to
    /// `tablet` (or `None`, if this node couldn't even resolve which tablet
    /// the op belongs to) — enriched with this node's own
    /// [`cp_leader_hint`](Self::cp_leader_hint) for `tablet`, if it has one.
    fn not_leader_refusal(&self, tablet: Option<TabletId>) -> ClientResponse {
        let hint = tablet.and_then(|t| self.cp_leader_hint(t));
        ClientResponse::Error(topology::format_not_leader_refusal(hint))
    }

    /// Another known client-API address for `tablet`, distinct from every
    /// address already in `tried` — the fallback
    /// [`cp_forward`](Self::cp_forward)'s hinted retry chases once the
    /// refusal's own leader hint is exhausted (already tried, or absent
    /// because the refusing node's own replica was mid-election). Walks the
    /// tablet's replicas in `Metadata` order (deterministic); `None` once
    /// every known replica address has been tried (or the tablet/its route
    /// isn't known at all).
    fn other_tablet_replica_addr(
        &self,
        tablet: TabletId,
        tried: &BTreeSet<SocketAddr>,
    ) -> Option<SocketAddr> {
        let meta = self.effective_metadata();
        let replicas = meta.tablets.get(&tablet)?.replicas.clone();
        // Intra-flavored (ADR 0047): this is a forwarding fallback, same as
        // `cp_leader_hint` above.
        let route = self.intra_route_snapshot();
        replicas
            .into_iter()
            .find_map(|id| route.get(&id).copied().filter(|a| !tried.contains(a)))
    }

    /// Forward a CP op for `(table, key)` to `addr` (wrapped so the receiver
    /// serves-or-errors, never re-forwards) and relay its reply. Carries the
    /// current span's trace context (ADR 0027) so the receiving node's
    /// handling of the forwarded op joins the same distributed trace.
    ///
    /// **Hinted retry — closes the "zero-replica blind-forward" hazard (root
    /// `CLAUDE.md`).** A node with no local replica of the op's tablet can
    /// only *guess* a first forward target among the tablet's replicas
    /// (`resolve_cp_route`'s no-local-replica fallback); previously a wrong
    /// guess errored forever, because the receiver never re-forwards
    /// (routing stays bounded to one hop by design) and this method had no
    /// better address to retry with. Now a "not the leader here" refusal
    /// carries the refusing (replica-hosting) node's own leader hint
    /// (`topology::format_not_leader_refusal`), and this is the single choke
    /// point every CP forward call goes through (all six call sites), so the
    /// retry lives here once: on a parseable not-leader refusal, retry at the
    /// hint's address if untried, else at another of the tablet's known
    /// replica addresses ([`other_tablet_replica_addr`](Self::other_tablet_replica_addr)),
    /// skipping every address already tried. Bounded to at most one pass
    /// over {hint} ∪ replicas (each address tried at most once — the
    /// tablet's replica set is small and finite) and to the overall
    /// [`CLIENT_TIMEOUT`] budget for the *whole* sequence, not per attempt,
    /// so a forwarder chasing a bad guess still fails within one hop's usual
    /// time budget rather than several multiples of it. The one-hop
    /// invariant itself is unchanged: only the *forwarder* retries; the
    /// receiver ([`cp_serve_forwarded`](Self::cp_serve_forwarded)) still only
    /// ever serves-or-refuses, never re-forwards.
    ///
    /// **Leaderless pass — wait out the election, don't give up.** When a
    /// whole pass exhausts with every candidate refusing `leader_hint=none`,
    /// the tablet's group has no elected leader *yet* — the split-child /
    /// first-provision formation window, or a leader crash mid-election —
    /// a state that resolves itself within an election timeout or two. The
    /// local-serve path already waits for exactly this
    /// (`RouteDecision::Wait`); the forwarded path now does too: back off
    /// [`FORWARD_ELECTION_BACKOFF`], clear the tried-set, and run another
    /// pass, still hard-bounded by the same overall deadline. Gated on the
    /// tablet being resolvable so an op this node can't even map to a tablet
    /// keeps failing fast instead of consuming the whole budget.
    async fn cp_forward(
        &self,
        table: &str,
        key: &[u8],
        addr: SocketAddr,
        request: ClientRequest,
    ) -> ClientResponse {
        let tablet = self.tablet_for(table, key);
        self.forward_to_tablet_leader(tablet, addr, request).await
    }

    /// The tablet-id-addressed core of [`cp_forward`](Self::cp_forward) —
    /// the ONE hint-chasing forward implementation, shared with every
    /// internal RPC addressed by **tablet id** rather than by a client key
    /// ([`seed_child_rows`](Self::seed_child_rows), [`force_seal_tablet`](Self::force_seal_tablet),
    /// [`grow_stream_tablet`](Self::grow_stream_tablet),
    /// `clear_backfill_cursor_tablet`, [`read_stream_hot_records`](Self::read_stream_hot_records)).
    ///
    /// Those callers used to relay once and, on a "not the leader here"
    /// refusal, re-run `resolve_cp_route` from scratch — which **never
    /// converges when this node hosts no replica of the target tablet**:
    /// the no-local-replica fallback deterministically returns the same
    /// first replica address every time, that follower refuses with a
    /// leader hint every time, and the hint was thrown away every time.
    /// The split-build driver hit exactly this (ADR 0050 fork F5 places a
    /// child at fresh homes, so on a >RF-node cluster the parent's leader
    /// routinely hosts no replica of one child): seeding that child spun
    /// against the same follower forever and the split never converged,
    /// never froze, never cut over — the parent kept all its keys with two
    /// empty/half-seeded `Building` children parked beside it, indefinitely.
    /// Chasing the refusal's own embedded hint here (identically to a
    /// client-key forward) is what actually reaches the leader.
    async fn forward_to_tablet_leader(
        &self,
        tablet: Option<TabletId>,
        addr: SocketAddr,
        request: ClientRequest,
    ) -> ClientResponse {
        let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
        let mut tried: BTreeSet<SocketAddr> = BTreeSet::new();
        let mut next = addr;
        loop {
            tried.insert(next);
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let resp = relay_request_with_timeout(
                next,
                &ClientRequest::Forwarded {
                    request: Box::new(request.clone()),
                    traceparent: crate::otel::current_traceparent(),
                },
                remaining,
            )
            .await;
            let ClientResponse::Error(e) = &resp else {
                return resp;
            };
            let Some(hint) = topology::parse_not_leader_refusal(e) else {
                return resp;
            };
            if tokio::time::Instant::now() >= deadline {
                return resp;
            }
            let candidate = hint
                .filter(|(_, a)| !tried.contains(a))
                .map(|(_, a)| a)
                .or_else(|| tablet.and_then(|t| self.other_tablet_replica_addr(t, &tried)));
            match candidate {
                Some(a) => next = a,
                None if tablet.is_some() => {
                    // Every known candidate refused with no leader to point
                    // at: the group is mid-election (formation window after a
                    // split/provision, or a crashed leader). Wait it out and
                    // re-run the pass — bounded by the same overall deadline.
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    if remaining.is_zero() {
                        return resp;
                    }
                    tokio::time::sleep(FORWARD_ELECTION_BACKOFF.min(remaining)).await;
                    tried.clear();
                    // `next` unchanged: re-probe the same replica first — once
                    // the election completes it either serves or hints.
                }
                None => return resp,
            }
        }
    }

    /// Send `request` to a peer node's client API over a fresh connection and
    /// return its reply (or an error on any transport failure). The cross-node
    /// relay primitive for CP forwarding (A1) and schema-DDL relay (A2). Thin
    /// wrapper over the free [`relay_request`] (ADR 0035 PR4 — extracted so
    /// [`control_handle::RemoteControlClient`], which has no `ClientCtx` of
    /// its own, can use the identical wire primitive).
    async fn relay(&self, addr: SocketAddr, request: ClientRequest) -> ClientResponse {
        relay_request(addr, &request).await
    }

    /// Propose a **schema-catalog** `command` toward the control-plane leader
    /// (v1 Phase 1 / A2): propose locally if this node is the control leader, else
    /// relay [`ClientRequest::ProposeSchema`] to the leader's node. Best-effort per
    /// call — the caller polls its replicated `Metadata` for the commit and
    /// re-invokes, so a transient relay failure is retried with a re-resolved
    /// leader. The result replicates to every node via Raft.
    ///
    /// Returns whether this call has reason to believe `command` reached *some*
    /// leader's Raft log (a local `Accepted`, or a relay that didn't visibly
    /// fail) — `false` only when nothing was sent anywhere (no leader
    /// known/reachable, or a local propose lost a leadership race).
    /// [`propose_and_await`](Self::propose_and_await) uses this to decide
    /// whether to back off before resubmitting: re-proposing an
    /// already-in-flight command on every poll tick just appends a duplicate
    /// log entry (harmless to apply for an idempotent command like
    /// `SplitTablet` — its `new_id` guard rejects the duplicate — but still
    /// wasted WAL/replication work, worse under exactly the load/latency that
    /// caused the wait in the first place). Same shape as the already-fixed
    /// `cp_batch_write_patient`/`propose_and_confirm_split` retry-amplification
    /// bugs, applied to the schema-proposal path.
    pub(crate) async fn propose_schema(&self, command: &MetaCommand) -> bool {
        if let Some(leader) = self.edge.leader_handle() {
            return matches!(
                leader.propose(command.clone()),
                ProposeResult::Accepted { .. }
            );
        }
        // Prefer the control handle's own **intra** leader-address hint (ADR
        // 0047; ADR 0035 PR4's original `leader_addr_hint` populated directly
        // from `Status` replies for a `Remote` data node) over an
        // `intra_addr` lookup — the hint is strictly fresher for a data-only
        // node, since it rides the very `Status` reply that filled the
        // mirror, whereas `intra_addr` needs this leader's address to have
        // separately synced into the replicated node-address book. This is a
        // machine-to-machine relay, so it uses the intra hint/route, never
        // the human-facing `leader_addr_hint`/`route_addr` (see the root
        // `CLAUDE.md`'s hint-field-conflation lesson). A no-op for `Local`
        // (always `None`).
        if let Some(addr) = self.control.intra_leader_addr_hint() {
            return !matches!(
                self.relay(addr, ClientRequest::ProposeSchema(command.clone()))
                    .await,
                ClientResponse::Error(_)
            );
        }
        if let Some(leader_id) = self.control.leader()
            && let Some(addr) = self.intra_addr(leader_id)
        {
            return !matches!(
                self.relay(addr, ClientRequest::ProposeSchema(command.clone()))
                    .await,
                ClientResponse::Error(_)
            );
        }
        // No locally-known leader. The common cause is a real control-group
        // voter mid-election (rare, brief); the other is a **control-plane-
        // follower-less growth node** (ADR 0030) whose own control `RaftCore`
        // never learns a leader at all, since it never receives real Raft
        // traffic for a group it was never a voter of — for it, this is the
        // *only* path that can ever reach the real cluster (its own local
        // `propose` always fails, and it has no leader hint to relay a single
        // hop to). Broadcast to every other known **intra** address instead:
        // a real control-group member among them resolves the actual leader
        // itself (one more hop — `ProposeSchema`'s handler is a single,
        // bounded relay, never a chain). Returns true on the first address that
        // connects, regardless of what its own `propose_schema` achieves
        // (best-effort, same as every other branch here — the caller confirms
        // via replicated `Metadata`, not this return value).
        for addr in self.intra_route_snapshot().into_values() {
            if addr == self.admin.intra_addr {
                continue;
            }
            if !matches!(
                self.relay(addr, ClientRequest::ProposeSchema(command.clone()))
                    .await,
                ClientResponse::Error(_)
            ) {
                return true;
            }
        }
        false
    }

    /// Provision the **first tablet** of `table` (ADR 0023): a fresh cluster has no
    /// data tablet, so `CreateTable` stands one up — a single tablet covering the
    /// whole token ring, scoped to `table`, which splits on demand as it grows. The
    /// replica set is the first `min(N, RF)` `Active` CP members. Relays
    /// `CreateTablet` to the control leader and waits until it appears, then attaches
    /// an RF `SetTabletPolicy` (so the reconciler auto-replaces a `Down` replica) on
    /// the committed tablet id. Idempotent + race-safe: the state machine admits only
    /// one `CreateTablet` per table, so concurrent callers converge on one tablet.
    pub(crate) async fn provision_tablet(&self, table: &str) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + SCHEMA_COMMIT_TIMEOUT;
        // Propose-side patience (the `propose_and_await` discipline — see the
        // retry-amplification entries in `docs/engineering-lessons.md`): the
        // *poll* below stays at `SCHEMA_POLL_INTERVAL` so a commit is observed
        // promptly, but a command believed to have reached a leader's log is
        // not re-proposed until `SCHEMA_PROPOSE_PATIENCE` elapses. This loop
        // used to re-propose on every 50ms poll tick, appending a duplicate
        // control-log entry each time — harmless to apply (`CreateTablet` is
        // first-committer-wins per table) but real WAL/replication/apply work
        // piled onto the control plane under exactly the slow-commit
        // conditions that make this wait long in the first place (measured on
        // a deliberately slowed disk: a six-table concurrent bring-up
        // proposed `CreateTablet` 264 times and `SetTabletPolicy` 240 times
        // for what should be ~6+6 — the self-amplification behind issue
        // #268's 25s seed-put flake on starved CI runners). It cannot simply
        // ride `propose_and_await`: the create arm must re-derive its tablet
        // id and replica set from fresh metadata per proposal (the
        // `trigger_split` stale-allocator lesson), and the needed command
        // switches to `SetTabletPolicy` once the tablet exists — hence an
        // inline pacer, reset on the phase switch so the policy proposal is
        // not held back by the create proposal's own patience window.
        let mut next_propose_at = tokio::time::Instant::now();
        let mut last_proposed_create: Option<bool> = None;
        loop {
            // Fresh, not `metadata_cached()` (ADR 0035 PR4): the "no tablet
            // yet" branch below picks the tablet's *initial* replica set from
            // `meta.members`, and a `Remote` data node's mirror is routinely a
            // poll interval stale (ADR 0035 §5) — `metadata_fresh()` avoids
            // needlessly under-sizing that initial set on a node whose own
            // read is avoidably behind.
            //
            // But freshness of the READ is not enough on its own to make the
            // recorded POLICY correct, and — after this exact race recurred
            // under `cluster_growth.rs`'s heavy three-concurrent-cluster load
            // (see `docs/engineering-lessons.md`) — the policy below is
            // deliberately no longer derived from `t.replicas.len()` at all.
            // **The invariant is: the policy always records the *target* RF
            // (`MAX_REPLICATION_FACTOR`), never whatever the replica set's
            // size happened to be at creation.** `CreateTablet` only ever
            // succeeds once per table (idempotent, first-committer wins) and
            // may legitimately mint a *smaller* initial set if fewer than
            // `MAX_REPLICATION_FACTOR` members are `Active` yet at that
            // instant — even a maximally fresh read can observe a cluster
            // that is still mid-bootstrap, promoting its own members one
            // commit at a time. Recording the *target* rather than the
            // *observation* is what makes that best-effort initial set
            // self-heal: `reconcile_placement`'s existing violation-repair
            // path (the same one that replaces a later-killed replica)
            // proposes a `CasTabletReplicas` growing it to
            // `MAX_REPLICATION_FACTOR` the moment enough candidates are
            // `Active`, with no separate "did the RF ever get set right"
            // mechanism needed. A too-low RF baked from a point-in-time
            // observation, by contrast, is invisible to that machinery
            // forever — `reconcile_placement` only fixes *violations of the
            // recorded policy*, so an under-observed RF just becomes a new,
            // permanently-satisfied target.
            let meta = self.control.metadata_fresh().await;
            if let Some((&tablet, _)) = meta.tablets_for_table(table).next() {
                // The tablet exists; ensure its RF policy is set, then we're done. The
                // caller's op routes through `cp_route`, which itself waits for the
                // group to form/elect (`CLIENT_TIMEOUT`), so provisioning need not
                // block on serveability here.
                if meta.policies.contains_key(&tablet) {
                    return Ok(());
                }
                let now = tokio::time::Instant::now();
                if last_proposed_create != Some(false) || now >= next_propose_at {
                    let sent = self
                        .propose_schema(&MetaCommand::SetTabletPolicy {
                            tablet,
                            policy: Some(PlacementPolicy::simple("cp-rf", MAX_REPLICATION_FACTOR)),
                        })
                        .await;
                    last_proposed_create = Some(false);
                    next_propose_at = now
                        + if sent {
                            SCHEMA_PROPOSE_PATIENCE
                        } else {
                            Duration::ZERO
                        };
                }
            } else {
                // No tablet yet: pick the first min(N, RF) Active CP members and
                // propose its creation toward the control leader.
                let mut replicas: Vec<NodeId> = meta
                    .members
                    .iter()
                    .filter(|(_, m)| m.status == NodeStatus::Active)
                    .map(|(id, _)| id.clone())
                    .collect();
                replicas.truncate(MAX_REPLICATION_FACTOR);
                let now = tokio::time::Instant::now();
                if !replicas.is_empty()
                    && (last_proposed_create != Some(true) || now >= next_propose_at)
                {
                    // The id and replica set are re-derived fresh per
                    // (re)proposal, never captured once outside the loop — a
                    // stale allocator-derived id is the `trigger_split`
                    // collision lesson (`docs/engineering-lessons.md`).
                    let sent = self
                        .propose_schema(&MetaCommand::CreateTablet {
                            tablet: meta.next_free_tablet_id(),
                            table: Some(table.to_owned()),
                            range: KeyRange::whole(),
                            replicas,
                        })
                        .await;
                    last_proposed_create = Some(true);
                    next_propose_at = now
                        + if sent {
                            SCHEMA_PROPOSE_PATIENCE
                        } else {
                            Duration::ZERO
                        };
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("table tablet did not provision in time".into());
            }
            tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// Wait until `table`'s just-provisioned tablet can actually **serve** a
    /// request, by issuing a linearizable probe read through the ordinary
    /// [`cp_read`](Self::cp_read) routing machinery (ReadIndex on the group
    /// leader, local or forwarded) until it succeeds — a converged-or-timeout
    /// poll, never a fixed sleep.
    ///
    /// [`provision_tablet`](Self::provision_tablet) deliberately confirms only
    /// the **metadata** commit; the tablet's Raft group then forms and elects
    /// asynchronously (each replica's tablet-host reconciler, ADR 0031). A
    /// caller that *acks table creation to a client* (the DynamoDB
    /// `CreateTable` edge) must call this before
    /// replying, or the ack races the formation window: the client's
    /// immediately-following first write only lands via the election-wait
    /// machinery (`cp_forward`'s backoff pass / the local
    /// `RouteDecision::Wait`) and, under unlucky timing, can burn much of its
    /// own `CLIENT_TIMEOUT` or fail outright. First-*write* auto-provision
    /// paths (`cp_kind_write_item`, `fast_marker_write`, …) need no such call
    /// — their own op routes through `cp_route`, which already waits.
    ///
    /// The probe key is the empty key: a freshly-provisioned table has one
    /// tablet over the whole ring (`KeyRange::whole()`), whose range contains
    /// every key, so the probe routes to it without minting a token-prefixed
    /// key — and a served read of an absent key still proves the full path
    /// (leader elected, ReadIndex barrier satisfied) that a first write needs.
    /// A ReadIndex success requires the leader to confirm quorum contact, so
    /// "readable" here implies "can commit a write promptly" too.
    ///
    /// On timeout the table + tablet already exist (both commits confirmed
    /// upstream) — the error only means the group did not become serveable
    /// within the budget, exactly the state a retried data op's own routing
    /// wait would then contend with.
    pub(crate) async fn await_table_serveable(&self, table: &str) -> Result<(), String> {
        // One `cp_read` is already internally bounded (`cp_route`'s wait and
        // `cp_forward`'s election backoff are both capped by `CLIENT_TIMEOUT`),
        // but it can surface a non-retryable-shaped transient early (e.g. a
        // forwarding hop's transport error mid-formation) — so wrap it in the
        // house converged-or-timeout retry loop with its own overall deadline.
        let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
        loop {
            // Deliberately `Strong` (ADR 0055): this probe exists to prove
            // the group has actually elected and can serve a linearizable
            // read before `CreateTable` acks — an eventual read would pass
            // against a replica that has merely applied something, which is
            // precisely the formation window the probe must not hand the
            // client (ADR 0023's 2026-08-17 amendment).
            let err = match self
                .cp_read(table, Vec::new(), ReadConsistency::Strong)
                .await
            {
                Ok(_) => return Ok(()),
                Err(e) => e,
            };
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "table `{table}` was created but its tablet did not become \
                     serveable in time: {err}"
                ));
            }
            tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// Serve a **forwarded** CP op locally: this node must lead the op's tablet (it
    /// does not re-forward — bounding routing to one hop). The op's `(table, key)`
    /// resolves to its owning tablet, then to that tablet's leader on this node.
    async fn cp_serve_forwarded(&self, inner: ClientRequest) -> ClientResponse {
        match inner {
            ClientRequest::Put { key, value, table } => {
                let tablet = self.tablet_for(&table, &key);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                match Self::cp_put_local(&leader, key, value).await {
                    Ok(()) => ClientResponse::PutOk,
                    Err(e) => ClientResponse::Error(e),
                }
            }
            ClientRequest::PutBatch { entries, table } => {
                // All entries share one tablet (the forwarder grouped by tablet), so
                // resolve the leader by the first key and serve the whole batch here.
                let Some(first) = entries.first().map(|(k, _)| k.clone()) else {
                    return ClientResponse::PutOk; // empty batch is a no-op
                };
                let tablet = self.tablet_for(&table, &first);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                match Self::cp_batch_local(&leader, entries).await {
                    Ok(()) => ClientResponse::PutOk,
                    Err(e) => ClientResponse::Error(e),
                }
            }
            ClientRequest::KindWrite {
                table,
                writes,
                change_log,
            } => {
                // Every write shares one tablet (they share a partition key), so
                // resolve the leader by the first key and serve the whole entry.
                let Some(first) = writes.first().map(|(_, k, _)| k.clone()) else {
                    return ClientResponse::PutOk; // empty batch is a no-op
                };
                let tablet = self.tablet_for(&table, &first);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                // The identical confirm `cp_kind_write_raw`'s own Local arm
                // runs — never a second implementation (`cp_kind_local`'s
                // Some-base-write requirement wrongly refused a forwarded
                // whole-partition raw DELETE, whose base write is a
                // tombstone; see `cp_kind_raw_local`'s doc).
                match Self::cp_kind_raw_local(&leader, writes, change_log).await {
                    Ok(()) => ClientResponse::PutOk,
                    Err(e) => ClientResponse::Error(e),
                }
            }
            // ADR 0046 U3: the evaluate-at-leader write RPC — resolve the
            // leader by the item's own base key, recomputed here from
            // `pk`/`sk` rather than trusted from the caller (the same
            // discipline `Get`'s arm below already follows), then defer to
            // the identical leader-side evaluator `ClientCtx::
            // cp_kind_write_item`'s own `Local` branch calls in-process.
            ClientRequest::KindWriteItem {
                table,
                pk,
                sk,
                op,
                condition,
            } => {
                let key = dynamo::item_key(&pk, sk.as_ref());
                let tablet = self.tablet_for(&table, &key);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                let meta = self.effective_metadata();
                match dynamo::kind_write_item_at_leader(
                    self,
                    &leader,
                    &meta,
                    &table,
                    &pk,
                    sk.as_ref(),
                    op,
                    condition.as_ref(),
                    // `ClientRequest::KindWriteItem` is always a forwarded
                    // *client* write (ADR 0051 §7) — the TTL reaper only
                    // ever acts on a tablet it already leads, so it never
                    // forwards through this arm.
                    false,
                )
                .await
                {
                    Ok(dynamo::KindWriteOutcome::Ok {
                        old,
                        new,
                        collection_bytes,
                    }) => ClientResponse::KindWriteOk {
                        old,
                        new,
                        collection_bytes,
                    },
                    Ok(dynamo::KindWriteOutcome::ConditionFailed) => {
                        ClientResponse::ConditionFailed
                    }
                    // Preserve the error's own code across the hop (a typed
                    // evaluation error — e.g. size() on an N attribute, a
                    // real ValidationException — must not degrade to a 500
                    // just because the leader was remote); see
                    // `dynamo::encode_relayed_error`.
                    Err(e) => ClientResponse::Error(dynamo::encode_relayed_error(&e)),
                }
            }
            // ADR 0055: an eventual read is answered by whichever replica
            // of the tablet this node happens to hold — the forwarder chose
            // this node for hosting one, not for leading it. Serve-or-refuse
            // only, exactly like the strong arm below: the refusal is the
            // forwarder's signal to fall back to the linearizable path, so
            // it never re-forwards and never waits out an election.
            ClientRequest::Get {
                key,
                table,
                stale: true,
            } => {
                let Some(tablet) = self.tablet_for(&table, &key) else {
                    return ClientResponse::Error(STALE_READ_REFUSAL.into());
                };
                match self.cp_stale_local(tablet) {
                    Some(group) if group.scope_range().contains(&key) => {
                        match group.stale_get_served(&key).await {
                            Some(v) => ClientResponse::Value(v),
                            None => ClientResponse::Error(STALE_READ_REFUSAL.into()),
                        }
                    }
                    _ => ClientResponse::Error(STALE_READ_REFUSAL.into()),
                }
            }
            ClientRequest::Get {
                key,
                table,
                stale: false,
            } => {
                let tablet = self.tablet_for(&table, &key);
                match tablet.and_then(|t| self.edge.cp_leader(t)) {
                    // Read-side scope pre-check + served/absent disambiguation
                    // (ADR 0033) — the same `cp_get_local` decision as `cp_read`'s
                    // Local arm. Serve-or-error only (never re-forward, never
                    // wait): the forwarder's own retry loop re-resolves routing on
                    // a `"; retry"` error.
                    Some(leader) => match self.cp_get_local_resolving(&leader, &key).await {
                        Ok(v) => ClientResponse::Value(v),
                        Err(e) => ClientResponse::Error(e),
                    },
                    None => self.not_leader_refusal(tablet),
                }
            }
            // ADR 0018 §2, torn-pair-fix stack PR2: the non-blocking
            // single-shot analog of `Get` just above, the forwarding
            // payload behind `ClientCtx::cp_read_snapshot` — see
            // `GetSnapshot`'s own doc. Same serve-or-error discipline as
            // `Get` (never re-forward, never wait) — a still-`Pending`
            // outcome maps to `ClientResponse::Unresolved`, distinct from
            // `Get`'s own `"; retry"` `Error`, since the two callers'
            // outer loops act on those differently.
            ClientRequest::GetSnapshot { key, table } => {
                let tablet = self.tablet_for(&table, &key);
                match tablet.and_then(|t| self.edge.cp_leader(t)) {
                    Some(leader) => match self.cp_get_local_snapshot(&leader, &key).await {
                        Ok(SnapshotRead::Value(v)) => ClientResponse::Value(v),
                        Ok(SnapshotRead::Unresolved) => ClientResponse::Unresolved,
                        Err(e) => ClientResponse::Error(e),
                    },
                    None => self.not_leader_refusal(tablet),
                }
            }
            ClientRequest::Delete { key, table } => {
                let tablet = self.tablet_for(&table, &key);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                match Self::cp_delete_local(&leader, key).await {
                    Ok(()) => ClientResponse::PutOk,
                    Err(e) => ClientResponse::Error(e),
                }
            }
            // ADR 0055's scan arm — same serve-or-refuse discipline as the
            // eventual `Get` above.
            ClientRequest::Scan {
                start,
                end,
                limit,
                reverse,
                table,
                stale: true,
            } => {
                let Some(tablet) = self.tablet_for(&table, &start) else {
                    return ClientResponse::Error(STALE_READ_REFUSAL.into());
                };
                let requested = KeyRange::new(start.clone(), end.clone());
                match self.cp_stale_local(tablet) {
                    Some(group) if group.scope_range().contains_range(&requested) => {
                        ClientResponse::Pairs(
                            group
                                .stale_scan(&start, end.as_deref(), limit, reverse)
                                .await,
                        )
                    }
                    _ => ClientResponse::Error(STALE_READ_REFUSAL.into()),
                }
            }
            ClientRequest::Scan {
                start,
                end,
                limit,
                reverse,
                table,
                stale: false,
            } => {
                let tablet = self.tablet_for(&table, &start);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                // Read-side scope pre-check (ADR 0033) — the same
                // `cp_scan_local` decision as `cp_scan_one`'s Local arm: a
                // scope lagging the metadata-derived scan window would
                // silently truncate results, not error.
                match Self::cp_scan_local(&leader, &start, end.as_deref(), limit, reverse).await {
                    Ok(p) => ClientResponse::Pairs(p),
                    Err(e) => ClientResponse::Error(e),
                }
            }
            // ADR 0041 §5: the LSI `Query` forwarding payload. `start`/`end`
            // resolve to one tablet by construction (the forwarder already
            // checked this in `cp_scan_kind`), so resolve the leader by
            // `start` alone.
            // ADR 0055's kind-scoped scan arm (an eventual LSI/GSI page).
            ClientRequest::KindScan {
                table,
                kind,
                start,
                end,
                limit,
                reverse,
                stale: true,
            } => {
                let Some(tablet) = self.tablet_for(&table, &start) else {
                    return ClientResponse::Error(STALE_READ_REFUSAL.into());
                };
                let requested = KeyRange::new(start.clone(), end.clone());
                match self.cp_stale_local(tablet) {
                    Some(group) if group.scope_range().contains_range(&requested) => {
                        ClientResponse::Pairs(
                            group
                                .stale_scan_kind(kind, &start, end.as_deref(), limit, reverse)
                                .await,
                        )
                    }
                    _ => ClientResponse::Error(STALE_READ_REFUSAL.into()),
                }
            }
            ClientRequest::KindScan {
                table,
                kind,
                start,
                end,
                limit,
                reverse,
                stale: false,
            } => {
                let tablet = self.tablet_for(&table, &start);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                match Self::cp_scan_kind_local(
                    &leader,
                    kind,
                    &start,
                    end.as_deref(),
                    limit,
                    reverse,
                )
                .await
                {
                    Ok(p) => ClientResponse::Pairs(p),
                    Err(e) => ClientResponse::Error(e),
                }
            }
            // ADR 0042/0043 round-3 sealer PR: the force-seal RPC —
            // addressed by `tablet` directly (see the variant's own doc for
            // why there is no client key to derive it from).
            ClientRequest::ForceSeal { tablet } => {
                let tablet = TabletId(tablet);
                let Some(leader) = self.edge.cp_leader(tablet) else {
                    return self.not_leader_refusal(Some(tablet));
                };
                let table = self
                    .effective_metadata()
                    .tablets
                    .get(&tablet)
                    .and_then(|t| t.table.clone());
                let Some(table) = table else {
                    return ClientResponse::Error("no such tablet".into());
                };
                match index_drain::seal_now(self, &table, tablet, &leader).await {
                    Ok(_) => ClientResponse::PutOk,
                    Err(e) => ClientResponse::Error(e),
                }
            }
            // ADR 0050 Train B rung 4: the split-build seed RPC — addressed
            // by `tablet` (a Building child, deliberately unroutable by
            // key) directly, mirroring `ForceSeal` just above. One shared
            // local implementation with `seed_child_rows`' own local
            // branch (`seed_rows_local`), never a second confirm copy.
            ClientRequest::SeedRows { tablet, rows } => {
                let tablet = TabletId(tablet);
                let Some(leader) = self.edge.cp_leader(tablet) else {
                    return self.not_leader_refusal(Some(tablet));
                };
                match Self::seed_rows_local(&leader, rows).await {
                    Ok(()) => ClientResponse::PutOk,
                    Err(e) => ClientResponse::Error(e),
                }
            }
            // Growth PR3 (ADR 0042 §14): the manual-growth split-trigger
            // RPC — addressed by `tablet` directly, mirroring `ForceSeal`
            // just above. Materializes this tablet's own live pairs
            // (leader-local — only reachable once this arm confirms this
            // node hosts it) and splits at their byte-weighted median via
            // `trigger_split`, which itself applies F11 rounding and Fork
            // E's single-token skip.
            ClientRequest::TriggerAutoSplit { tablet } => {
                let tablet = TabletId(tablet);
                let Some(leader) = self.edge.cp_leader(tablet) else {
                    return self.not_leader_refusal(Some(tablet));
                };
                match median_split_key(&leader).await {
                    None => ClientResponse::Error(STREAM_GROW_NO_SPLIT_POINT.into()),
                    Some(split_key) => self.trigger_split(tablet, split_key).await,
                }
            }
            // ADR 0042 §7/§8, PR6: the open-shard hot-read RPC — addressed
            // by `tablet` directly, mirroring `ForceSeal` just above (see
            // this variant's own doc for why). Leader-local, no ReadIndex
            // barrier (F8) — `index_drain::hot_read` is the one function
            // that knows how to filter/sort/limit the tablet's own hot tail.
            ClientRequest::StreamHotRead {
                tablet,
                from_position,
                limit,
            } => {
                let tablet = TabletId(tablet);
                let Some(leader) = self.edge.cp_leader(tablet) else {
                    return self.not_leader_refusal(Some(tablet));
                };
                // The ADR 0048 scope-transition latch died with the mutable
                // scope (ADR 0050 rung 7): ranges are immutable and a split
                // retires the parent whole, so there is no transition window
                // left to latch.
                let pairs = index_drain::hot_read(&leader, from_position, limit)
                    .await
                    .into_iter()
                    .map(|(key, _, value)| (key, value))
                    .collect();
                ClientResponse::Pairs(pairs)
            }
            // ADR 0045 §5 step 3: the backfill-cursor-cleanup RPC —
            // addressed by `tablet` directly, mirroring `ForceSeal`/
            // `StreamHotRead` above (see this variant's own doc for why).
            ClientRequest::ClearBackfillCursor { tablet, index } => {
                let tablet = TabletId(tablet);
                let Some(leader) = self.edge.cp_leader(tablet) else {
                    return self.not_leader_refusal(Some(tablet));
                };
                match index_drain::clear_backfill_cursor(&leader, &index).await {
                    Ok(()) => ClientResponse::PutOk,
                    Err(e) => ClientResponse::Error(e),
                }
            }
            // ADR 0018 §2/PR4: the four internal 2PC coordinator RPCs.
            // Routed by the first write key (`TxnPrepare`) or one of `keys`
            // (`TxnResolve`) — **never** `record_key` for a non-anchor
            // participant, whose own tablet is a different table's keyspace
            // entirely (see each variant's doc). `TxnDecide`/`TxnStatus`
            // always target the anchor's own tablet, so `record_key` (which
            // lives there by construction) is the right routing key.
            ClientRequest::TxnPrepare {
                table,
                anchor,
                writes,
                conditions,
                participant_spans,
                pending_kind_writes,
            } => {
                let Some(first) = writes.first().map(|w| w.key.clone()).or_else(|| {
                    pending_kind_writes
                        .first()
                        .map(|p| dynamo::item_key(&p.pk, p.sk.as_ref()))
                }) else {
                    return ClientResponse::Error(
                        TxnAbortReason::Other("txn prepare: writes must be non-empty".into())
                            .encode(),
                    );
                };
                let tablet = self.tablet_for(&table, &first);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                // ADR 0046 U3 (PR2): the shared local-stage step also used by
                // `txn_prepare`'s own `CpRoute::Local` branch — see
                // `txn_stage_local`'s doc.
                match self
                    .txn_stage_local(
                        &leader,
                        &table,
                        anchor,
                        writes,
                        conditions,
                        participant_spans,
                        pending_kind_writes,
                    )
                    .await
                {
                    Ok((txn_id, record_key, record_table, ts, outcome)) => {
                        ClientResponse::TxnPrepared {
                            txn_id,
                            record_key,
                            record_table,
                            ts,
                            outcome,
                        }
                    }
                    // ADR 0018's 2026-08-24 `CancellationReasons` amendment
                    // (issue #374 C2b): encode the typed reason into this
                    // hop's only error channel — `txn_prepare`'s `Forward`
                    // branch decodes it back out via `TxnAbortReason::decode`.
                    Err(e) => ClientResponse::Error(e.encode()),
                }
            }
            ClientRequest::TxnDecide {
                table,
                txn_id,
                record_key,
                commit,
                min_commit_ts,
                orphan_created_ts,
            } => {
                let tablet = self.tablet_for(&table, &record_key);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                // ADR 0018 §2/PR5: resolves nothing here (the caller does,
                // uniformly, for every participant including the anchor's
                // own keys) — and reports the record's ACTUAL decision,
                // which may differ from what was proposed (a duelling
                // recovery decision may have already won). See
                // `ClientCtx::txn_decide_anchor`'s doc for the full account.
                // `orphan_created_ts` overrides `commit`/`min_commit_ts`
                // entirely — a recovery pusher that found no record at all
                // (the orphan-record fix).
                let decide_ok = if let Some(created_ts) = orphan_created_ts {
                    leader
                        .txn_abort_orphan(txn_id.clone(), record_key.clone(), created_ts)
                        .await
                        .is_some()
                } else if commit {
                    leader
                        .txn_commit_at_least(txn_id.clone(), record_key.clone(), min_commit_ts)
                        .await
                        .is_some()
                } else {
                    leader
                        .txn_abort(txn_id.clone(), record_key.clone())
                        .await
                        .is_some()
                };
                if !decide_ok {
                    return ClientResponse::Error(
                        "CP group leader moved during anchor decide; retry".into(),
                    );
                }
                match leader.txn_status_local(&record_key).await {
                    Some(TxnDecisionStatus::Committed { commit_ts }) => {
                        ClientResponse::TxnDecided {
                            outcome: TxnOutcome::Committed { commit_ts },
                        }
                    }
                    Some(TxnDecisionStatus::Aborted) => ClientResponse::TxnDecided {
                        outcome: TxnOutcome::Aborted,
                    },
                    Some(TxnDecisionStatus::Pending) => ClientResponse::Error(
                        "txn decide: record still Pending immediately after its own decide \
                         applied — protocol bug"
                            .into(),
                    ),
                    None => {
                        ClientResponse::Error("CP group leader moved after decide; retry".into())
                    }
                }
            }
            ClientRequest::TxnResolve {
                table,
                txn_id,
                record_key,
                keys,
                outcome,
            } => {
                let Some(first) = keys.first().cloned() else {
                    return ClientResponse::PutOk; // nothing to resolve
                };
                let tablet = self.tablet_for(&table, &first);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                match leader.txn_resolve(txn_id, record_key, keys, outcome).await {
                    Some(_) => ClientResponse::PutOk,
                    None => {
                        ClientResponse::Error("CP group leader moved during resolve; retry".into())
                    }
                }
            }
            ClientRequest::TxnStatus { table, record_key } => {
                let tablet = self.tablet_for(&table, &record_key);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                match leader.txn_status_local(&record_key).await {
                    Some(status) => ClientResponse::TxnStatusReply { status },
                    None => ClientResponse::Error(
                        "CP group leader moved, or no record yet, during status query; retry"
                            .into(),
                    ),
                }
            }
            // ADR 0018 §2/PR5: the two recovery-only internal RPCs — see
            // `ClientCtx::txn_record_view`/`txn_verify`, the one callers.
            ClientRequest::TxnRecordView { table, record_key } => {
                let tablet = self.tablet_for(&table, &record_key);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                match leader.txn_record_view(&record_key).await {
                    Some(view) => ClientResponse::TxnRecordViewReply {
                        status: view.status,
                        intent_spans: view.intent_spans,
                        created_ts: view.created_ts,
                    },
                    None => ClientResponse::Error(
                        "CP group leader moved, or no record yet, during record view query; retry"
                            .into(),
                    ),
                }
            }
            ClientRequest::TxnVerify {
                table,
                span,
                txn_id,
            } => {
                let tablet = self.tablet_for(&table, &span.start);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                match leader.txn_verify_staged(&span, &txn_id).await {
                    Some(staged) => ClientResponse::TxnVerifyReply { staged },
                    None => ClientResponse::Error(
                        "CP group leader moved during txn verify; retry".into(),
                    ),
                }
            }
            _ => ClientResponse::Error("unexpected forwarded request".into()),
        }
    }

    /// Render this node's **live** metrics as the ADR 0015 text export
    /// (`name value` lines), aggregated across the node's role sink(s).
    ///
    /// A combined node (ADR 0040 PR1: one identity, one internal `ProdEnv`
    /// per node) records the control Raft and the CP group into the **same**
    /// sink now — `self.control.metrics()` and `data.raftkv_metrics` are the
    /// identical handle there, so this only pushes the raftkv-role snapshot
    /// when it is a genuinely distinct sink (a `ControlHandle::Remote`
    /// data-only node, whose `metrics()` is a permanent no-op) — else summing
    /// would double-count every counter. A control-only node (ADR 0035 PR3)
    /// has only the control sink — there is no data role to aggregate. The
    /// snapshots are read **at call time**, so the export reflects current
    /// activity rather than a cached value.
    pub(crate) fn metrics_text(&self) -> String {
        let mut snaps = vec![self.control.metrics().snapshot()];
        if let Some(data) = &self.data
            && !data.raftkv_metrics.is_same_sink(self.control.metrics())
        {
            snaps.push(data.raftkv_metrics.snapshot());
        }
        let mut counters: BTreeMap<Metric, u64> = BTreeMap::new();
        let mut is_leader: i64 = 0;
        for snap in &snaps {
            for (&metric, &value) in &snap.counters {
                *counters.entry(metric).or_insert(0) += value;
            }
            is_leader = is_leader.max(snap.is_leader);
        }
        // Render in the same stable order as `MetricSnapshot::to_text`.
        let mut out = String::new();
        for m in Metric::ALL {
            let v = counters.get(&m).copied().unwrap_or(0);
            out.push_str(m.name());
            out.push(' ');
            out.push_str(&v.to_string());
            out.push('\n');
        }
        out.push_str("control_is_leader ");
        out.push_str(&is_leader.to_string());
        out.push('\n');
        out
    }

    /// The same aggregated metrics as [`metrics_text`](Self::metrics_text), but as
    /// a `(name -> value, is_leader)` pair for the admin `/admin/metrics` JSON view
    /// (ADR 0020). Read live at call time and summed across the node's role
    /// sink(s), exactly as the text export.
    pub(crate) fn metrics_json(&self) -> (BTreeMap<String, u64>, i64) {
        let mut snaps = vec![self.control.metrics().snapshot()];
        if let Some(data) = &self.data
            && !data.raftkv_metrics.is_same_sink(self.control.metrics())
        {
            snaps.push(data.raftkv_metrics.snapshot());
        }
        let mut counters: BTreeMap<String, u64> = BTreeMap::new();
        let mut is_leader: i64 = 0;
        for m in Metric::ALL {
            counters.insert(m.name().to_string(), 0);
        }
        for snap in &snaps {
            for (&metric, &value) in &snap.counters {
                *counters.entry(metric.name().to_string()).or_insert(0) += value;
            }
            is_leader = is_leader.max(snap.is_leader);
        }
        (counters, is_leader)
    }

    /// Growth PR3 Fork F (ADR 0042 §14): every currently-tracked tablet's
    /// own smoothed change-append rate (bytes/sec), for `/admin/metrics`'s
    /// `stream_change_rates` array — empty on a control-only node (no
    /// [`DataRole`] at all, so nothing was ever tracked).
    pub(crate) fn stream_change_rates(&self) -> Vec<(TabletId, f64)> {
        self.data
            .as_ref()
            .map(|d| d.change_rates.snapshot())
            .unwrap_or_default()
    }

    /// A snapshot of this node's metrics-history ring buffer (oldest first),
    /// for the admin `/admin/metrics/history` view (ADR 0020) backing the
    /// dashboard's sparklines. Cloned out from under the lock so the caller
    /// never holds it across serialization.
    pub(crate) fn metrics_history(&self) -> Vec<MetricsSample> {
        self.metrics_history
            .lock()
            .expect("metrics history poisoned")
            .iter()
            .cloned()
            .collect()
    }

    /// **Admin action (ADR 0020):** mark `node` `Leaving` so the placement
    /// reconciler moves its replicas off. Proposed on the **local** control leader
    /// handle (membership commands are control-plane-internal and not relayable, so
    /// this requires the receiving node to be the control leader; a follower
    /// returns an error and the operator retries on the leader). Preserves the
    /// member's existing labels. Returns the accepted state or an error.
    pub(crate) fn admin_drain(&self, node: NodeId) -> Result<(), String> {
        // Check leadership BEFORE reading `self.control.metadata_cached()`
        // for the member lookup below (ADR 0035 PR5 staleness-audit fix,
        // mirroring `admin_remove_member`'s already-fixed ordering — same
        // reasoning: a follower's own replica can lag the leader's
        // just-committed membership state under load, so evaluating "is this
        // a member" off a follower's stale view can misfire as "not a
        // cluster member" instead of the intended "retry on the leader"
        // routing error).
        let Some(leader) = self.edge.leader_handle() else {
            return Err(self.not_leader_error());
        };
        let meta = self.control.metadata_cached();
        let Some(member) = meta.members.get(&node) else {
            return Err(format!("node {node} is not a cluster member"));
        };
        let labels = member.labels.clone();
        match leader.propose(MetaCommand::UpsertMember {
            node,
            labels,
            status: NodeStatus::Leaving,
        }) {
            ProposeResult::Accepted { .. } => Ok(()),
            ProposeResult::NotLeader { .. } => {
                Err("control leadership moved; retry on the leader".into())
            }
        }
    }

    /// **Admin action (ADR 0030): register a new node for online cluster growth.**
    /// Proposes `UpsertMember{node, labels, status: Down}` for `node` (the new
    /// node's **raftkv** id) — deliberately `Down`, not `Active`: the failure
    /// detector promotes `Down` → `Active` on the node's *first real heartbeat*
    /// (ADR 0012's existing, unmodified promotion chain — `FailureDetector::
    /// observe` starts tracking a member on its first heartbeat and reports it
    /// alive from that same instant, so `detect_loop`'s very next tick proposes
    /// the promotion), so a declared-but-never-booted node never becomes placement-
    /// eligible — see [`is_relayable_command`]'s doc for why `Down` specifically is
    /// safe to relay. Unlike [`admin_drain`](Self::admin_drain) (an operator action
    /// on an *existing* member, local-leader-only by design), this **relays**
    /// through [`propose_and_await`](Self::propose_and_await), so it works from
    /// any node reachable from an operator's shell — including the new node's own
    /// admin port, whose control role is never a real control-group voter (a
    /// control-plane-follower-less growth node relays every proposal, ADR 0030).
    /// Idempotent: re-adding an already-registered member (any status) is a no-op
    /// success — this action's job is only "make sure it's registered at all",
    /// not to force it back to `Down`.
    pub(crate) async fn admin_add_member(
        &self,
        node: NodeId,
        labels: BTreeMap<String, String>,
    ) -> Result<(), String> {
        if self.effective_metadata().members.contains_key(&node) {
            return Ok(());
        }
        self.propose_and_await(
            MetaCommand::UpsertMember {
                node: node.clone(),
                labels,
                status: NodeStatus::Down,
            },
            SCHEMA_COMMIT_TIMEOUT,
            || async {
                self.effective_metadata()
                    .members
                    .contains_key(&node)
                    .then_some(())
            },
        )
        .await
        .map_err(|()| {
            format!(
                "add-member for node {node} did not commit within {}s \
                 (no control-plane leader reachable?)",
                SCHEMA_COMMIT_TIMEOUT.as_secs()
            )
        })
    }

    /// **Admin action (ADR 0032 PR3): decommission a drained member.**
    ///
    /// Proposed on the **local** control leader handle, exactly like
    /// [`admin_drain`](Self::admin_drain) — deliberately **not** relayed (see
    /// [`is_relayable_command`]'s doc): a destructive, rare operator action
    /// should not silently reach the real leader through a relay chain from a
    /// node that may not even know who leads.
    ///
    /// Two refusals happen here, **before ever proposing** — friendlier than a
    /// bare Raft rejection string, though `Metadata::apply`'s own guard remains
    /// the actual authority (a race between two admin callers is still
    /// resolved there, deterministically, same as every other CAS-style
    /// command in this codebase):
    /// - `node` is itself a **currently live** control-plane voter (ADR 0037
    ///   — this reads `self.control.config()`, the live Raft config, **not**
    ///   a static original-members list; ADR 0040 PR1 removed the old
    ///   raftkv-id-to-control-id arithmetic bridge — a node has only one id
    ///   now, so this is a direct membership check). Before ADR 0037 the
    ///   control group was static (ADR 0030) and
    ///   this check read `self.admin.control_ids`, the process-start
    ///   snapshot — a genuine "is this id part of the control plane" decision
    ///   that a static read gets wrong the instant the group becomes elastic
    ///   (the exact class of bug the ADR 0029 ReadIndex-quorum lesson warns
    ///   about, see `docs/engineering-lessons.md`): a control-removed id must
    ///   become decommissionable, and a still-live voter — even one added at
    ///   runtime, an id `self.admin.control_ids` never even knew about — must
    ///   still be refused. `animus admin decommission --force-control-remove`
    ///   drives the two-phase flow this refusal points the operator at:
    ///   control-remove first, then this call.
    /// - the member is not drained: still `Active`/`Joining`, or still
    ///   referenced by any tablet ([`Metadata::tablets_referencing`]) — refused
    ///   with the same counts `/admin/member/drain-status` reports, rather
    ///   than a bare Raft `"Rejected"` string.
    ///
    /// **Removal is not a fence.** A removed node whose *process* keeps
    /// running stays removed (self-registration — `RegisterNodeAddrs` /
    /// `admin_add_member` — is a one-shot at startup, never repeated). But a
    /// **restart** of that process (or a fresh one at the same raftkv id)
    /// re-registers `Down` and rejoins exactly as a fresh join would: removal
    /// followed by a restart is, by design, equivalent to a fresh rejoin at
    /// the same id (`tests/decommission.rs` proves id reuse). The
    /// decommission flow's real last step is stopping the process, not this
    /// call.
    pub(crate) fn admin_remove_member(&self, node: NodeId) -> Result<(), String> {
        // Check leadership BEFORE reading `self.control.config()` (the
        // control-voter refusal below) or `self.control.metadata_cached()`
        // (the drain-status refusals below): a follower's own applied state
        // can lag the leader's just-committed control-membership change or
        // rebalance/release-GC move (real replication lag, not a bug), so
        // evaluating any of these off a follower's stale view can misfire
        // instead of the intended "retry on the leader" routing error. The
        // leader's own state is what actually gates the apply, so checking
        // leadership first makes every refusal here trustworthy.
        let Some(leader) = self.edge.leader_handle() else {
            return Err(self.not_leader_error());
        };
        if self.control.config().unwrap_or_default().contains(&node) {
            return Err(format!(
                "node {node} is a CURRENT control-plane voter; the control group \
                 is elastic now (ADR 0037) — control-remove it first (`animus \
                 admin control-remove`), or run `animus admin decommission \
                 --force-control-remove`, which does that for you before \
                 proceeding"
            ));
        }
        let meta = self.control.metadata_cached();
        let Some(member) = meta.members.get(&node) else {
            return Err(format!("node {node} is not a cluster member"));
        };
        if matches!(member.status, NodeStatus::Active | NodeStatus::Joining) {
            return Err(format!(
                "node {node} is not drained: status is {:?}; drain it first",
                member.status
            ));
        }
        let referenced = meta.tablets_referencing(&node);
        if referenced > 0 {
            return Err(format!(
                "node {node} still referenced by {referenced} tablet(s); wait for draining to \
                 complete"
            ));
        }
        match leader.propose(MetaCommand::RemoveMember { node }) {
            ProposeResult::Accepted { .. } => Ok(()),
            ProposeResult::NotLeader { .. } => {
                Err("control leadership moved; retry on the leader".into())
            }
        }
    }

    /// **Admin action (ADR 0037 PR3): add a control-plane voter.**
    ///
    /// Local-control-leader-only, deliberately **not** relayed (unlike
    /// [`admin_add_member`](Self::admin_add_member)'s data-plane counterpart) —
    /// a brand-new control node has no established control-group peer at all
    /// yet, so there is no meaningful "relay from the new node itself" case;
    /// the *operator* calling this must already know a reachable control
    /// leader, the same discipline [`admin_drain`](Self::admin_drain)/
    /// [`admin_remove_member`](Self::admin_remove_member) already hold to. Not
    /// added to [`is_relayable_command`] for the same reason: this isn't even
    /// a `MetaCommand` proposal at the top level — the actual membership
    /// change is `RaftNode::change_membership`, a distinct method only a
    /// genuine control-group voter's own in-process handle can call.
    ///
    /// `node`, when `Some`, is an **operator-supplied** id — validated via
    /// [`NodeId::propose`] (the sanctioned re-validation every intake
    /// boundary must run on an id that arrived through `serde`, which skips
    /// [`NodeId::propose`]'s charset check by design). ADR 0040 PR4 deletes
    /// the old `ALLOC_ID_BASE`-range refusal entirely — there is no more
    /// reserved numeric range to keep clear of, since ids are opaque strings
    /// and uniqueness is enforced structurally by [`register_node`](
    /// Self::register_node)'s CAS, not by a magnitude convention. The target
    /// must **already be a registered member** (its own prior
    /// self-registration, e.g. an already-running combined node being
    /// promoted to a control voter) **or get registered in this same
    /// action** — there is no third case and no refusal for "already
    /// exists": promoting an existing member to a control voter is the
    /// common case, not a conflict (one identity per node, ADR 0040 PR1 —
    /// there is no longer a separate control-id space an existing member
    /// could collide with).
    ///
    /// `node: None` (ADR 0037 hardening trio's PR3, re-based onto ADR 0040
    /// Decision C) mints a fresh id via [`NodeId::mint`] off **this** leader's
    /// own bound `Env` (`leader.env()`) — **not**
    /// [`animus_env::prod::PreBindRng`], the pre-bind CLI-boundary exception:
    /// this method runs in-process on a live control leader a `SimEnv` test
    /// can (and, per this PR's own tests, does) drive, so the `Env`-seam rule
    /// (ADR 0003) applies here with no exception to invoke. A minted
    /// collision (astronomically unlikely, but structurally possible) simply
    /// re-mints and retries, up to [`MAX_MINT_ATTEMPTS`] times — mirroring the
    /// CLI join path's identical retry shape.
    ///
    /// Three steps, honestly partial on a failure between any of them
    /// (mirroring [`admin_add_member`]'s "both-or-honest-partial-state"
    /// idempotence, since a retry of this whole call is always safe): (a)
    /// [`register_node`](Self::register_node)'s CAS claims `node` if it
    /// isn't already a member (a no-op if it already is, matching
    /// `RegisterNodeAddrs`'s idempotent update contract) or updates its
    /// `internal` address if it is; (b) merges `addr` into the **local
    /// leader's own** `ProdEnv` peer book ([`animus_env::ProdEnv::
    /// merge_peer`]) so its very next `AppendEntries`/`InstallSnapshot` to
    /// `node` has somewhere to go; (c) calls `change_membership` to actually
    /// add the voter. See `ProdEnv::merge_peer`'s doc for the known scope
    /// limit this leaves (only *this* env's peer book is updated — a later
    /// leadership change needs its own follow-up, deliberately deferred out
    /// of this PR).
    ///
    /// Returns the **effective** [`NodeId`] either way — the operator-supplied
    /// one echoed back, or the freshly-minted one — so the caller (`admin.rs`,
    /// the CLI) can tell the operator what id the new process should actually
    /// come up as.
    pub(crate) async fn admin_add_control_member(
        &self,
        node: Option<NodeId>,
        addr: SocketAddr,
        labels: BTreeMap<String, String>,
    ) -> Result<NodeId, String> {
        let Some(leader) = self.edge.leader_handle() else {
            return Err(self.not_leader_error());
        };
        let (mut node, minted) = match node {
            Some(node) => (
                NodeId::propose(node.as_str()).map_err(|e| format!("invalid node id: {e}"))?,
                false,
            ),
            None => (NodeId::mint(leader.env()), true),
        };

        let current = self.control.config().unwrap_or_default();
        if current.contains(&node) {
            // Idempotent: already a voter. Still worth refreshing this env's
            // own local peer-book entry in case `addr` changed (e.g. a
            // replacement process at the same id) — cheap and harmless
            // either way. Also re-propose the *replicated* address if it
            // changed: `merge_peer` alone only updates this leader's own
            // env — every other control-role node's `peer_sync_loop` only
            // ever learns an updated address from `Metadata.node_addrs`,
            // never from this call's local `merge_peer` side effect.
            leader.env().merge_peer(node.clone(), addr);
            let meta = self.control.metadata_cached();
            if let Some(mut addrs) = meta.node_addrs.get(&node).cloned()
                && addrs.internal != addr.to_string()
            {
                addrs.internal = addr.to_string();
                let _ = leader.propose(MetaCommand::RegisterNodeAddrs {
                    node: node.clone(),
                    addrs,
                });
            }
            return Ok(node);
        }

        let meta = self.control.metadata_cached();
        if meta.members.contains_key(&node) {
            // Already a registered member (its own self-registration, or a
            // prior admin action) — just make sure its `internal` address
            // (this action's whole purpose) matches `addr`. Never touches
            // labels/status: those belong to whatever registered it, not to
            // this control-voter promotion.
            let mut addrs = meta.node_addrs.get(&node).cloned().unwrap_or(NodeAddrs {
                internal: String::new(),
                client: String::new(),
                admin: String::new(),
                intra: String::new(),
                role: "control".to_string(),
            });
            if addrs.internal != addr.to_string() {
                addrs.internal = addr.to_string();
                if let ProposeResult::NotLeader { .. } =
                    leader.propose(MetaCommand::RegisterNodeAddrs {
                        node: node.clone(),
                        addrs,
                    })
                {
                    return Err("control leadership moved; retry on the leader".into());
                }
            }
        } else {
            // Genuinely unclaimed: the sole claim path (ADR 0040 Decision C).
            // A **minted** id re-mints and retries on collision
            // (astronomically unlikely, but structurally possible — nothing
            // needs rebinding, since ports are never derived from ids); a
            // **proposed** id fails loudly on the first collision instead —
            // an operator/config conflict is a real problem to report, not
            // to paper over by silently trying something else.
            let mut attempts_left = if minted { MAX_MINT_ATTEMPTS } else { 1 };
            loop {
                // Merge into whatever address-book entry this id's own
                // self-registration may already have made (e.g. a
                // permanently-non-voter control-only growth node that
                // published its real `client`/`admin` addresses before this
                // action ever ran) rather than blindly constructing a fresh,
                // empty one — `RegisterNode`'s CAS would otherwise see this
                // call's empty `client`/`admin` as a *different* entry and
                // reject it as a collision against its own node's earlier
                // self-registration.
                let mut addrs = self
                    .control
                    .metadata_cached()
                    .node_addrs
                    .get(&node)
                    .cloned()
                    .unwrap_or(NodeAddrs {
                        internal: String::new(),
                        client: String::new(),
                        admin: String::new(),
                        intra: String::new(),
                        role: "control".to_string(),
                    });
                addrs.internal = addr.to_string();
                match self
                    .register_node(node.clone(), addrs, labels.clone())
                    .await
                {
                    Ok(RegisterOutcome::Registered) => break,
                    Ok(RegisterOutcome::Collision) if minted && attempts_left > 1 => {
                        attempts_left -= 1;
                        node = NodeId::mint(leader.env());
                    }
                    Ok(RegisterOutcome::Collision) => {
                        return Err(format!(
                            "node {node} is already claimed by a different registration \
                             (data-plane, control-core, or another admin action); pick a \
                             different id"
                        ));
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        leader.env().merge_peer(node.clone(), addr);
        let mut voters = current;
        voters.insert(node.clone());
        match leader.change_membership(voters) {
            ProposeResult::Accepted { .. } => Ok(node),
            ProposeResult::NotLeader { .. } => Err(
                "control leadership moved, or a membership change is already in \
                 flight; the address book was updated (retry-safe) but the voter \
                 was not added — retry on the leader"
                    .into(),
            ),
        }
    }

    /// **Admin action (ADR 0037 PR3): remove a control-plane voter.**
    ///
    /// Local-control-leader-only, not relayed — same rationale as
    /// [`admin_add_control_member`](Self::admin_add_control_member).
    ///
    /// Refusals, before ever touching Raft:
    /// - `node` is not currently a live voter (`self.control.config()`):
    ///   idempotent success (mirrors [`admin_remove_member`]'s idempotent
    ///   philosophy for an already-absent member).
    /// - removing `node` would leave **zero** voters (`current.len() <= 1`):
    ///   refused outright — there is no admin action that can recover a
    ///   control group with no voters at all.
    ///
    /// **Quorum-loss policy (ADR 0037 §2, a deliberate decision, not a TODO):**
    /// a removal that leaves exactly **one** voter (no fault tolerance) still
    /// **proceeds** — Raft itself tolerates it — but the success carries a
    /// non-empty `warning` the caller (admin.rs, the CLI) must surface, never
    /// silently swallow.
    ///
    /// The plan's second warning trigger — "every *other* remaining voter is
    /// currently marked Down" — was **historically** not implementable via
    /// `ControlHandle::believes_alive` (pre-ADR-0040, that signal was keyed
    /// on a distinct **raftkv** id space the control ids didn't share, ADR
    /// 0012 — see `docs/engineering-lessons.md`'s "id-space mismatch" entry
    /// for the full story). ADR 0037 hardening PR2 instead added a genuinely
    /// control-id-native signal (`RaftCore::peer_last_contact`/
    /// `RaftNode::control_peer_believed_alive`, see below) rather than wait
    /// for the id-space unification; ADR 0040 PR1 has since dissolved the
    /// mismatch structurally (one id per node), but the dedicated
    /// control-Raft-traffic-based signal remains the more precise one (a
    /// voter can be reachable-for-heartbeats but unable to actually
    /// replicate, or vice versa) and is unchanged here.
    ///
    /// **Removing the current leader's own slot** needs a leadership transfer
    /// first (`RaftCore::change_membership` always rejects leader self-
    /// removal) — this method arms one to another live voter and polls
    /// (bounded by [`CONTROL_TRANSFER_POLL_TIMEOUT`]) for this node to step
    /// down. On success it does **not** silently retry the removal itself —
    /// once this node has stepped down it may no longer be the leader of
    /// *any* process reachable from this call, so it returns the same
    /// familiar "control leadership moved; retry on the leader" refusal every
    /// other not-leader case here already uses (now proactively triggered
    /// rather than discovered), telling the caller exactly what
    /// `admin_remove_member`'s not-leader case already tells them: retry
    /// against the leader (now a different node). A transfer that never
    /// completes in time surfaces as its own, distinct timeout error.
    pub(crate) async fn admin_remove_control_member(
        &self,
        node: NodeId,
        force: bool,
    ) -> Result<ControlRemoveOutcome, String> {
        let Some(leader) = self.edge.leader_handle() else {
            return Err(self.not_leader_error());
        };
        let my_id = leader.env().node_id();
        let current = self.control.config().unwrap_or_default();
        if !current.contains(&node) {
            // Idempotent: already not a voter.
            return Ok(ControlRemoveOutcome { warning: None });
        }
        if current.len() <= 1 {
            // Never forceable: there is no admin action that can recover a
            // control group with zero voters, so `force` cannot buy this one.
            return Err(format!(
                "refusing to remove control voter {node}: only {} voter(s) remain; \
                 this would leave zero",
                current.len()
            ));
        }
        let remaining: BTreeSet<NodeId> =
            current.iter().filter(|&id| *id != node).cloned().collect();
        // Liveness-aware quorum-loss guard (ADR 0037 hardening PR2). The
        // original ADR 0037 guard counted only the *resulting* voter count
        // (refuse `< 1`, warn `== 1`) — which looks complete but misses the
        // case a different, already-dead survivor is left in `remaining`: an
        // odd-sized group (tolerates one failure) going to an even-sized one
        // (tolerates none) with a dead member carries no warning at all if
        // the resulting count is 2 or more, yet the group is now permanently
        // wedged (its own config-change entry can never commit, so every
        // further membership change fails "already in flight" forever — see
        // ADR 0037's Consequences section). `node` itself is excluded from
        // `remaining` already, so removing the actually-dead voter needs no
        // `--force` — only a *different* already-dead survivor trips this.
        // Deliberately independent of `--force-control-remove`
        // (`admin_remove_member`'s decommission integration, ADR 0037 PR4):
        // that flag only means "run control-remove as part of decommission,"
        // never "and skip control-remove's own safety checks" — the two
        // flags are separate and each must be independently explicit.
        if !force {
            let dead: Vec<NodeId> = remaining
                .iter()
                .filter(|id| !leader.control_peer_believed_alive((*id).clone()))
                .cloned()
                .collect();
            let live = remaining.len() - dead.len();
            let majority = remaining.len() / 2 + 1;
            if live < majority {
                let dead_list = dead
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(format!(
                    "refusing to remove control voter {node}: only {live} of the \
                     remaining {} voter(s) are reachable (need {majority} for \
                     quorum) — apparently-dead voter(s): {dead_list}; retry with \
                     --force to remove anyway",
                    remaining.len()
                ));
            }
        }
        if node == my_id {
            let Some(target) = current.iter().find(|&id| *id != my_id).cloned() else {
                return Err("no other control voter available to transfer leadership to".into());
            };
            if !leader.transfer_leadership(target.clone()) {
                return Err(format!(
                    "could not arm a leadership transfer to node {target} (already \
                     mid-transfer, or {target} has not caught up); retry"
                ));
            }
            let deadline = tokio::time::Instant::now() + CONTROL_TRANSFER_POLL_TIMEOUT;
            loop {
                if !leader.is_leader() {
                    return Err(format!(
                        "control leadership transferred away (to node {target}) so this \
                         node can complete the removal itself; retry on the leader"
                    ));
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(format!(
                        "leadership transfer to node {target} did not complete within \
                         {}s; retry",
                        CONTROL_TRANSFER_POLL_TIMEOUT.as_secs()
                    ));
                }
                tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
            }
        }
        let warning = if remaining.len() == 1 {
            Some(format!(
                "removing node {node} leaves only 1 control voter: no fault tolerance"
            ))
        } else {
            None
        };
        match leader.change_membership(remaining) {
            ProposeResult::Accepted { .. } => {
                // ADR 0040 PR1: there is no more separate `control` address
                // field to prune — `node`'s one `internal` address is shared
                // by every role it runs, and it may still be a data-role/
                // combined cluster member after losing its control-voter
                // status, so `Metadata.node_addrs[node]` is left exactly as
                // is (its own `peer_sync_loop`'s ordinary self-registration
                // keeps it current regardless of voter status).
                Ok(ControlRemoveOutcome { warning })
            }
            ProposeResult::NotLeader { .. } => Err(
                "control leadership moved, or a membership change is already in \
                     flight; retry on the leader"
                    .into(),
            ),
        }
    }
}

/// How long [`ClientCtx::admin_remove_control_member`] polls for a self-removal's
/// leadership transfer to complete before giving up with an honest timeout error
/// — generous relative to the default 150ms election timeout (several rounds of
/// pre-vote + real vote + `TimeoutNow` under real scheduling jitter), mirroring
/// the other bounded admin polls in this file (e.g. [`SCHEMA_COMMIT_TIMEOUT`]).
const CONTROL_TRANSFER_POLL_TIMEOUT: Duration = Duration::from_secs(5);

/// The result of a successful [`ClientCtx::admin_remove_control_member`] call —
/// `warning` is `Some` for the deliberately-allowed-but-risky quorum-loss cases
/// (ADR 0037 §2: down to one voter, or every other remaining voter looks `Down`)
/// that the caller (`admin.rs`, the CLI) must surface, never silently drop.
pub(crate) struct ControlRemoveOutcome {
    pub(crate) warning: Option<String>,
}

/// How many times a **minted** [`NodeId`] is allowed to collide (ADR 0040
/// Decision C) before giving up and reporting a real error — a 128-bit mint
/// colliding even once is already astronomically unlikely; this only guards
/// against a genuine bug (e.g. a broken `Rng`) looping forever rather than
/// ever expecting to be exhausted in practice.
const MAX_MINT_ATTEMPTS: u32 = 8;

/// The observable outcome of [`ClientCtx::register_node`]'s propose-then-poll
/// registration CAS (ADR 0040 Decision C) — see that method's own doc for
/// exactly what each variant means and why they cover every case (a fresh
/// claim, an idempotent replay, and a genuine collision) with only two
/// values: the caller only ever needs to know "is `node_addrs[node]` now
/// mine, or someone else's."
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RegisterOutcome {
    /// `node_addrs[node]` holds exactly the `addrs`/`labels` this call
    /// proposed — whether from this call's own commit, an idempotent
    /// replay, or a concurrent identical registration.
    Registered,
    /// `node_addrs[node]` holds a **different** entry — the id is already
    /// claimed by someone else. A minted caller re-mints and retries with a
    /// different id; a caller with an operator-/config-proposed id must
    /// fail loudly instead.
    Collision,
}

/// The leader's one-time cluster bootstrap, retried on a timer until it lands.
///
/// It registers the cluster's **CP data nodes** (the `raftkv` ids) as `Active`
/// members and records the single bootstrap **CP tablet** covering the whole
/// keyspace, placed on the first `min(N, MAX_REPLICATION_FACTOR)` of them — the
/// same set the CP group spans in [`BoundNode::start_with`]. This populates the
/// replicated `Metadata` (so `status`/`metadata().tablets` are meaningful and
/// dynamic CP reconfigure can later read `tablets[t].replicas`). Idempotent (skips
/// once the tablet exists), so only the first leader to win does the work and a
/// re-election does not duplicate it. The CP group itself is statically formed at
/// node start; automatic CP failure-detection / reconfigure is later v1 work, so no
/// `PlacementPolicy` is attached.
///
/// **Registers `Active` (ADR 0030 phantom-member hardening — option (a), not
/// (b)).** Registering `Down` instead (promoted only by a real heartbeat, the
/// same mechanism [`ClientCtx::admin_add_member`] relies on for online growth)
/// was tried first and reverted: bootstrap's *every* declared node is expected
/// to already be booting in the same process-start window, so a still-electing
/// leader or a slow first heartbeat can commit `CreateTable`'s provisioning
/// (`ClientCtx::provision_tablet`, which seeds a tablet's replica set from
/// whichever members are `Active` *right now*) against a **transiently
/// under-replicated** membership — `tests/cp_cross_process.rs` caught this
/// exactly (a table provisioned with a 2-of-3 replica set because the third
/// bootstrap member hadn't yet heartbeated its way to `Active`), a real,
/// non-trivial regression the spec's own contingency called for. Registering
/// `Active` immediately (as before ADR 0030) restores that guarantee. The
/// phantom hole this used to leave open — a *declared-but-never-booted* node
/// staying placement-eligible forever, since nothing ever judges an `Active`
/// member the detector has never heard from — is closed instead in
/// `animus-control`'s `detect_loop` (see its doc): a member the detector
/// doesn't yet track is now given a synthetic first observation the moment the
/// leader notices it declared `Active`, which starts the same silence clock a
/// real heartbeat would — so a node that never actually heartbeats is demoted
/// to `Down` after one ordinary `DETECT_TIMEOUT`, same as any other failure,
/// while a node whose real heartbeat arrives promptly (the overwhelmingly
/// common case) is unaffected.
///
/// **`raftkv_ids` is caller-supplied (ADR 0035 PR2)** — the raftkv ids of
/// nodes that actually run the **data** role, scoped by
/// [`BoundNode::start_with`]'s `data_raftkv_ids` parameter, not derived here
/// from a bare node count. In combined mode (every node `Both`, still the
/// only shape any entry point actually assembles) this is every control id's
/// paired `raftkv_id`, unchanged from before this ADR.
async fn bootstrap(raft: RaftNode<ProdEnv>, raftkv_ids: Vec<NodeId>) {
    loop {
        if raft.is_leader() {
            // Register the CP `raftkv` ids as `Active` members — the cluster's data
            // nodes (the control-group ids are only the metadata consensus group).
            // No data tablet is created here (ADR 0023): a fresh cluster has zero
            // data tablets; the first `CreateTable` provisions a table-scoped tablet
            // (`ClientCtx::provision_tablet`), and the per-node join-host loop stands
            // its group up. Idempotent: only members not yet present are proposed.
            let meta = raft.metadata();
            for node in &raftkv_ids {
                if !meta.members.contains_key(node) {
                    raft.propose(MetaCommand::UpsertMember {
                        node: node.clone(),
                        labels: BTreeMap::new(),
                        status: NodeStatus::Active,
                    });
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// How often the peer-sync loop rebuilds the `raftkv` peer book from replicated
/// `Metadata` (Phase 2.3a). Brisk so a runtime-registered CP member becomes
/// reachable promptly; the work is a cheap map rebuild + `set_peers`.
const PEER_SYNC_INTERVAL: Duration = Duration::from_millis(200);

/// Keep this node's one internal env's peer book = the **static** book ∪ the
/// replicated `Metadata.cp_member_addrs` ∪ `Metadata.node_addrs[*].internal`
/// (ADR 0032 PR1's node address book, ADR 0040 PR1's merge of the old
/// `raftkv`/`control` address pair into one `internal` field — a runtime-
/// registered member's address, whatever role it runs, lands in this one
/// book: this is the same env that carries the control Raft, every hosted
/// tablet's Raft group, and this node's own failure-detection heartbeats, so
/// there is exactly one peer book to keep current, not the pre-PR1
/// `peer_sync_loop`/`control_peer_sync_loop` pair). `set_peers` replaces the
/// book each tick; idempotent, runs for the life of the node (a perpetual
/// loop, aborted on `shutdown`). A peer entry whose address fails to parse is
/// skipped (the control plane stores it opaquely).
///
/// Takes the whole [`ClientCtx`] (not a bare `RaftNode`) so a control-plane-
/// follower-less growth node (ADR 0030) reads `effective_metadata` — its mirror
/// of the real cluster's `cp_member_addrs`/`node_addrs` — instead of its own
/// never-replicated local raft; every other node is unaffected (`effective_metadata`
/// passes through to `raft.metadata()` there).
async fn peer_sync_loop(ctx: ClientCtx, env: ProdEnv, static_peers: BTreeMap<NodeId, SocketAddr>) {
    loop {
        let mut book = static_peers.clone();
        let meta = ctx.effective_metadata();
        for (id, addr) in meta.cp_member_addrs {
            if let Ok(sa) = addr.parse::<SocketAddr>() {
                book.insert(id, sa);
            }
        }
        for (id, addrs) in meta.node_addrs {
            if let Ok(sa) = addrs.internal.parse::<SocketAddr>() {
                book.insert(id, sa);
            }
        }
        env.set_peers(book);
        tokio::time::sleep(PEER_SYNC_INTERVAL).await;
    }
}

/// Keep `ctx.client_route` = the **static** seed (this node's own config-time
/// route table) ∪ the replicated `Metadata.node_addrs[*].client` (ADR 0032 PR1),
/// so a node grown in after this node's own startup becomes a valid forward
/// target for a client op (`propose_schema`'s own relay reads `intra_route`
/// instead, ADR 0047) — closing the ADR 0030 residual gap where `client_route`
/// was a process-start-only snapshot. Sibling
/// of [`peer_sync_loop`] in every respect: same [`PEER_SYNC_INTERVAL`] cadence,
/// same static-base-∪-replicated-overlay shape, reads
/// [`ClientCtx::effective_metadata`] so a control-plane-follower-less growth
/// node (ADR 0030) syncs off its own remote mirror instead of its
/// never-replicated local raft. A `node_addrs` entry whose `client` address
/// fails to parse is skipped.
async fn route_sync_loop(ctx: ClientCtx, static_route: BTreeMap<NodeId, SocketAddr>) {
    loop {
        let mut book = static_route.clone();
        for (id, addrs) in ctx.effective_metadata().node_addrs {
            if let Ok(sa) = addrs.client.parse::<SocketAddr>() {
                book.insert(id, sa);
            }
        }
        *ctx.client_route.lock().expect("client route poisoned") = book;
        tokio::time::sleep(PEER_SYNC_INTERVAL).await;
    }
}

/// The [`route_sync_loop`] sibling for `ctx.intra_route` (ADR 0047): keeps it
/// = the static seed (this node's own config/discovery-time knowledge of
/// every peer's intra address, exactly mirroring `client_route`'s own static
/// seed) ∪ the replicated `Metadata.node_addrs[*].intra`, same cadence/
/// shape/`effective_metadata` sourcing as `route_sync_loop`. A real static
/// seed (not an empty one) is load-bearing here, not just an optimization:
/// unlike `client_route`'s consumers, `cp_leader_hint`/`propose_schema`'s
/// relay and (critically) the **growth-node mirror's own seed-building**
/// (`start_with_streams`'s `ctx.intra_addr(id)` call, feeding
/// `remote_metadata_sync_loop`) run synchronously at ctx-construction time,
/// before this loop's first tick — an empty seed there would make a growth
/// node's very first mirror-poll attempt see zero addresses and never
/// recover (this loop's *next* tick can't help, since `remote_metadata_sync_
/// loop` captures its `seeds` argument once, at spawn time).
async fn intra_route_sync_loop(ctx: ClientCtx, static_route: BTreeMap<NodeId, SocketAddr>) {
    loop {
        let mut book = static_route.clone();
        for (id, addrs) in ctx.effective_metadata().node_addrs {
            if let Ok(sa) = addrs.intra.parse::<SocketAddr>() {
                book.insert(id, sa);
            }
        }
        *ctx.intra_route.lock().expect("intra route poisoned") = book;
        tokio::time::sleep(PEER_SYNC_INTERVAL).await;
    }
}

/// Failure-detection heartbeat loop with a **live** destination list (ADR
/// 0037 "known deferrals" #1, closed by this PR): every [`HEARTBEAT_INTERVAL`],
/// re-derive the control-group heartbeat targets from this node's own
/// [`ControlHandle::config`] (`ctx.control`) instead of the bring-up-time
/// `static_control_ids` snapshot [`heartbeat_loop`] was pinned to forever —
/// so a control voter added at runtime (`admin_add_control_member`, ADR 0037
/// PR3) starts receiving this node's heartbeats on the very next tick, not
/// only after this node itself restarts.
///
/// `ctx.control.config()` is `Some(..)` unconditionally for a genuine control
/// voter (`ControlHandle::Local`, always fresh — it's this node's own
/// `RaftCore::config()`) and, for a data-only node (`ControlHandle::Remote`),
/// the last voter set observed on any `Status`/`WatchMetadata` reply — `None`
/// until the first one lands, in which case this falls back to
/// `static_control_ids` (the config-file seed every node still has at
/// bring-up) so a freshly-started data-only node's heartbeats aren't dropped
/// entirely for the one tick before its first reply arrives.
///
/// Only fixes *which ids* this node targets — see `peer_sync_loop`'s doc for
/// this loop's other half: without also merging `Metadata.node_addrs[*]
/// .control`/`.raftkv` into the raftkv env's own peer book, a live id this
/// loop names still cannot be dialed (`ProdEnv::send` silently drops an
/// address-less peer). Both halves must ship together — see
/// `docs/engineering-lessons.md`'s entry on this PR for the two-staleness-
/// axes lesson.
///
/// Deliberately **not** a change to [`animus_control::node::heartbeat_loop`]
/// itself — that function (and its sim call sites) keeps its original
/// static-list contract; this is an animusd-local wrapper around the
/// already-`pub` [`send_heartbeat`] built specifically for the two real-node
/// call sites ([`BoundNode::start_with`], [`BoundNode::start_data_with`]).
async fn heartbeat_loop_live(ctx: ClientCtx, env: ProdEnv, static_control_ids: Vec<NodeId>) {
    loop {
        let control_ids: Vec<NodeId> = match ctx.control.config() {
            Some(voters) => voters.into_iter().collect(),
            None => static_control_ids.clone(),
        };
        send_heartbeat(&env, &control_ids).await;
        tokio::time::sleep(HEARTBEAT_INTERVAL).await;
    }
}

/// Upper bound the serving node parks on its own [`animus_control::MetadataWatch`]
/// before replying to a [`ClientRequest::WatchMetadata`] anyway with whatever
/// `Metadata` is current (ADR 0035 PR5) — see [`ClientCtx::watch_metadata`]'s
/// doc. Bounded so a long-poll connection never ties up a serving task (or a
/// caller's own connection) forever when nothing changes, and short enough
/// that [`WATCH_METADATA_CLIENT_TIMEOUT`]'s transport timeout always has
/// comfortable margin to receive the reply.
const WATCH_METADATA_SERVER_TIMEOUT: Duration = Duration::from_secs(8);

/// Transport timeout for a [`ClientRequest::WatchMetadata`] round trip
/// (via [`relay_request_with_timeout`]) — deliberately **not**
/// [`CLIENT_TIMEOUT`], the generic per-hop timeout for ordinary,
/// non-blocking requests: reusing that here would race the serving node's
/// own [`WATCH_METADATA_SERVER_TIMEOUT`] park (both are 10s-scale), so a
/// slow-but-legitimate "nothing changed yet" reply could be spuriously
/// reported as a transport failure right as the server was about to send it.
/// This exceeds the server's own bound by a comfortable margin instead.
const WATCH_METADATA_CLIENT_TIMEOUT: Duration = Duration::from_secs(12);

/// Backoff after a [`remote_metadata_watch_loop`] long-poll attempt fails at
/// the *transport* level (every seed unreachable, or an explicit rejection —
/// e.g. a misdirected watch against a `Remote` node, see
/// [`ClientCtx::watch_metadata`]'s doc) — as opposed to the serving node's own
/// bounded park, which is a normal "nothing changed yet" outcome, not a
/// failure, and needs no extra backoff (the server-side bound already
/// throttles the loop). Avoids busy-looping against an unreachable control
/// deployment.
const REMOTE_WATCH_RETRY_BACKOFF: Duration = Duration::from_millis(500);

/// Mirror the real cluster's replicated `Metadata` — **generalized (ADR 0035
/// §4) from "the fallback for a control-plane-follower-less growth node" to
/// "how every node with no *real* local Raft replication for `Metadata` stays
/// current"**, covering two shapes that now share **one** mechanism (ADR 0035
/// PR5): an ADR 0030 **growth node** (`seeds` = the pre-growth control nodes'
/// client addresses; `ctx.control` is `Local`, so the mirror lands in
/// `ctx.remote_metadata` and `effective_metadata` prefers it) and an ADR 0035
/// PR4 **data-only node** (`seeds` = the control deployment's client
/// addresses; `ctx.control` is `Remote`) both long-poll via
/// [`remote_metadata_watch_loop`] — the growth-node branch constructs a
/// standalone [`RemoteControlClient`] sharing `ctx.remote_metadata` as its
/// mirror (`RemoteControlClient::with_mirror`) purely to drive the identical
/// loop, since `ctx.control` itself is `Local`, not `Remote`, for a growth
/// node (see that constructor's doc). A no-op (returns immediately) when
/// `seeds` is empty — the case for every node that *is* a real control-group
/// voter, since `effective_metadata` then passes straight through to
/// `self.control.metadata_cached()` and nothing needs mirroring.
async fn remote_metadata_sync_loop(ctx: ClientCtx, seeds: Vec<SocketAddr>) {
    if seeds.is_empty() {
        return;
    }
    if let ControlHandle::Remote(remote) = &ctx.control {
        return remote_metadata_watch_loop(remote.clone(), seeds).await;
    }
    // Growth-node (ADR 0030) branch (ADR 0035 PR5: now long-polls, like the
    // data-only branch above, instead of a fixed-200ms `Status` poll) — this
    // node's own `ClientCtx.control` is `ControlHandle::Local` (a growth node
    // is a real, if permanently non-voting, control-group member), so there
    // is no `ControlHandle::Remote` to share; construct a standalone
    // `RemoteControlClient` that shares `ctx.remote_metadata` directly as its
    // mirror instead, so `effective_metadata()` keeps reading the same field
    // it always has.
    let remote = RemoteControlClient::with_mirror(seeds.clone(), ctx.remote_metadata.clone());
    remote_metadata_watch_loop(remote, seeds).await
}

/// **Long-poll metadata sync, shared by both mirror shapes** [`remote_metadata_sync_loop`]
/// drives (ADR 0035 PR5): replaces a fixed-interval `Status` poll with a
/// [`ClientRequest::WatchMetadata`] round trip parked on the answering
/// control node's own `MetadataWatch` — so a metadata change is observed
/// roughly as soon as the control leader's own commit makes it visible plus
/// one network hop, not up to one poll cycle later. Tries the current leader
/// hint first (mirroring [`RemoteControlClient::metadata_fresh`]'s own
/// candidate order — the leader is the node most likely to have just applied
/// the change this loop is waiting for), then every seed in order.
///
/// **Never busy-loops**: either the serving node's own bounded park
/// ([`WATCH_METADATA_SERVER_TIMEOUT`]) or, when every candidate fails at the
/// transport level, a plain `Status` poll plus [`REMOTE_WATCH_RETRY_BACKOFF`]
/// always separates consecutive attempts — there is no code path that retries
/// immediately in a tight loop.
async fn remote_metadata_watch_loop(remote: RemoteControlClient, seeds: Vec<SocketAddr>) {
    loop {
        let last_seen = remote.metadata_watch().latest();
        let mut candidates = Vec::with_capacity(seeds.len() + 1);
        // Intra-flavored (ADR 0047): `WatchMetadata` is intra-only, so the
        // dial candidates must be intra addresses, never the human-facing
        // `leader_addr_hint`/`seeds` this loop used before the port split
        // (`seeds` itself is now intra-flavored too — see
        // `RemoteControlClient.seeds`'s doc).
        if let Some(addr) = remote.intra_leader_addr_hint() {
            candidates.push(addr);
        }
        candidates.extend(seeds.iter().copied());

        let mut synced = false;
        for addr in candidates {
            match relay_request_with_timeout(
                addr,
                &ClientRequest::WatchMetadata { last_seen },
                WATCH_METADATA_CLIENT_TIMEOUT,
            )
            .await
            {
                ClientResponse::Status {
                    metadata,
                    leader_hint,
                    intra_leader_hint,
                    watermark,
                    control_voters,
                } => {
                    remote.observe(
                        metadata,
                        leader_hint,
                        intra_leader_hint,
                        watermark,
                        control_voters,
                    );
                    synced = true;
                    break;
                }
                // ADR 0038 PR5: the incremental reply — a stale-relative-to-
                // a-concurrent-update drop (`observe_delta` returning `false`)
                // is still a normal round trip, not a transport failure; the
                // next iteration re-requests with the corrected `last_seen`.
                ClientResponse::MetadataDelta {
                    writes,
                    watermark,
                    leader_hint,
                    intra_leader_hint,
                    control_voters,
                } => {
                    remote.observe_delta(
                        last_seen,
                        &writes,
                        leader_hint,
                        intra_leader_hint,
                        watermark,
                        control_voters,
                    );
                    synced = true;
                    break;
                }
                _ => {}
            }
        }
        if synced {
            // Either a real change resolved the watch, or the serving node's
            // own bound elapsed and it replied anyway — both are a normal
            // round trip; the server-side bound is itself the throttle, so
            // loop straight into the next long poll with no added sleep.
            continue;
        }
        // Every candidate failed at the transport level (unreachable, or an
        // explicit rejection — e.g. a stale hint pointing at a `Remote` node,
        // which rejects `WatchMetadata` outright, see
        // `ClientCtx::watch_metadata`'s doc). Fall back to a plain `Status`
        // poll before retrying, rather than hammering unreachable seeds in a
        // tight loop.
        for &addr in &seeds {
            if let ClientResponse::Status {
                metadata,
                leader_hint,
                intra_leader_hint,
                watermark,
                control_voters,
            } = relay_request(addr, &ClientRequest::Status).await
            {
                remote.observe(
                    metadata,
                    leader_hint,
                    intra_leader_hint,
                    watermark,
                    control_voters,
                );
                break;
            }
        }
        tokio::time::sleep(REMOTE_WATCH_RETRY_BACKOFF).await;
    }
}

/// A node's **one shared** storage engine (ADR 0026/0028): every tablet the node
/// hosts, across every table, merges into it — confined by its own
/// [`StorageScope`], not by separate files. Mirrors [`CpGroup`]'s two-backend
/// shape; cheap to clone (clones share state), so the per-node [`CpReconciler`]
/// (ADR 0031 PR4) can hand every tablet's group its own clone.
#[derive(Clone)]
enum SharedEngine {
    /// Durable on-disk LSM (default; survives a restart).
    Lsm(LsmEngine<ProdEnv>),
    /// Volatile in-memory engine (ephemeral runs).
    Mem(MemoryEngine),
}

impl SharedEngine {
    // ---- admin / debug introspection (ADR 0020, extended ADR 0038 PR4) ----
    // Mirrors `CpGroup`'s own identically-shaped introspection methods
    // (`backend_name`/`lsm_sstables`/`lsm_memtable`/`wal_segment_sizes`/
    // `wal_stats`) verbatim, one level shallower: `CpGroup` reads through
    // `RaftKvNode::storage()` to reach the shared engine `SharedEngine`
    // already *is* here, so these call straight into the engine's own
    // methods. Used by `/admin/storage/control` (ADR 0038 PR4) to surface
    // the control-plane system-keyspace engine's own stats — on a combined
    // node this is the exact same physical engine/files a hosted tablet's
    // `/admin/storage/lsm` already shows (the control plane's `Metadata`
    // just lives at a reserved key prefix within it); on a control-only node
    // it is this node's own small dedicated engine, otherwise invisible to
    // any `/admin/storage/*` route (which are all `ctx.edge.local_cp`-keyed,
    // and a control-only node hosts no CP groups at all).
    fn backend_name(&self) -> &'static str {
        match self {
            SharedEngine::Lsm(_) => "lsm",
            SharedEngine::Mem(_) => "memory",
        }
    }

    /// Live SSTable views, or `None` on the volatile memory backend.
    fn lsm_sstables(&self) -> Option<Vec<SsTableView>> {
        match self {
            SharedEngine::Lsm(e) => Some(e.sstable_views()),
            SharedEngine::Mem(_) => None,
        }
    }

    /// `(memtable key count, approx bytes)`, or `None` on the memory backend.
    fn lsm_memtable(&self) -> Option<(usize, usize)> {
        match self {
            SharedEngine::Lsm(e) => Some((e.memtable_len(), e.memtable_bytes())),
            SharedEngine::Mem(_) => None,
        }
    }

    /// Live WAL segments + byte sizes, or `None` on the memory backend.
    async fn wal_segment_sizes(&self) -> Option<Vec<(u64, u64)>> {
        match self {
            SharedEngine::Lsm(e) => Some(e.wal_segment_sizes().await),
            SharedEngine::Mem(_) => None,
        }
    }

    /// `(durable_seq, rotation_count)`, or `None` on the memory backend.
    fn wal_stats(&self) -> Option<(u64, u64)> {
        match self {
            SharedEngine::Lsm(e) => Some((e.wal_durable_seq(), e.wal_rotation_count())),
            SharedEngine::Mem(_) => None,
        }
    }

    // ---- plain `StorageEngine` passthroughs (plan-syskv-ui) ----------------
    // `GET /admin/system-table`'s system-keyspace browse surface reads this
    // engine directly — a dedicated point read of the `_applied_index`
    // watermark key, plus one bounded range scan over
    // `animus_control::syskv::reserved_scan_bounds()` — rather than through
    // any tablet-shaped wrapper (there is none here; this engine may not
    // host any CP tablet at all on a control-only node). `SharedEngine`
    // doesn't otherwise implement `StorageEngine` itself (its `Snapshot`
    // associated type would have to pick one arm arbitrarily), so these are
    // two plain inherent methods forwarding to whichever concrete engine
    // this node chose, exactly like every other method in this impl block.

    /// A dedicated point read at `key` (used for the `_applied_index`
    /// watermark — never scraped from a scan window).
    async fn get(&self, key: &[u8]) -> Result<Option<VersionedValue>, StorageError> {
        match self {
            SharedEngine::Lsm(e) => e.get(key).await,
            SharedEngine::Mem(e) => e.get(key).await,
        }
    }

    /// The live `[start, end)` pairs, in key order.
    async fn scan(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Key, VersionedValue)>, StorageError> {
        match self {
            SharedEngine::Lsm(e) => e.scan(start, end).await,
            SharedEngine::Mem(e) => e.scan(start, end).await,
        }
    }
}

/// A tablet's own LSM filename prefix on this node's `Disk` (ADR 0050 rung
/// 1: per-tablet engines — naming is identity, the same mechanism
/// `raftkv.wal.<tablet>` uses). The trailing `-` is load-bearing: it keeps
/// `db-t5-*` from prefix-matching `db-t51-*`, and no tablet file ever
/// collides with the node's own control/syskv engine (whose files are
/// `db-MANIFEST`/`db-wal-*`/`db-sst-*` under the bare [`LSM_PREFIX`] — the
/// `t` disambiguates).
fn tablet_lsm_prefix(tablet: u64) -> String {
    format!("{LSM_PREFIX}t{tablet}-")
}

/// The [`LsmEngine`] implementation of the per-tablet engine seam (ADR 0050
/// rung 1): one private on-disk engine per hosted tablet, opened/probed/
/// destroyed by filename prefix over this node's one `ProdEnv` disk.
struct LsmTabletFactory {
    env: ProdEnv,
}

#[async_trait::async_trait]
impl animus_cp_data::host::EngineFactory<LsmEngine<ProdEnv>> for LsmTabletFactory {
    async fn open(&self, tablet: TabletId) -> Result<LsmEngine<ProdEnv>, String> {
        LsmEngine::open(self.env.clone(), &tablet_lsm_prefix(tablet.0))
            .await
            .map_err(|e| e.to_string())
    }

    async fn probe(&self, tablet: TabletId) -> bool {
        // Durable engine state exists iff any file carries this tablet's own
        // prefix — an `LsmEngine` writes its first file (a WAL segment) on
        // the first write, so a never-written tablet correctly probes false.
        let prefix = tablet_lsm_prefix(tablet.0);
        self.env
            .list()
            .await
            .unwrap_or_default()
            .iter()
            .any(|f| f.starts_with(&prefix))
    }

    async fn destroy(&self, tablet: TabletId) {
        let prefix = tablet_lsm_prefix(tablet.0);
        for f in self.env.list().await.unwrap_or_default() {
            if f.starts_with(&prefix)
                && let Err(e) = self.env.remove(&f).await
            {
                tracing::warn!(?e, file = %f, "deleting a reclaimed tablet's engine file");
            }
        }
    }

    async fn clone_engine(
        &self,
        source: &LsmEngine<ProdEnv>,
        target: TabletId,
    ) -> Result<LsmEngine<ProdEnv>, String> {
        // ADR 0058 Train 2 rung 3: the in-place split's Stage 3
        // materialization, over this node's real on-disk backend —
        // `LsmEngine::clone_to` (ADR 0058 rung 2) is the SSTable-hard-link
        // clone; the target's own filename prefix is the SAME per-tablet
        // naming convention every other tablet engine uses, so a restart's
        // ordinary `open(target)` recovers it identically to any other
        // hosted tablet. `source` is the caller's own already-open handle
        // (see the trait's own doc for why this method never re-opens it).
        source
            .clone_to(tablet_lsm_prefix(target.0))
            .await
            .map_err(|e| e.to_string())
    }
}

/// This node's own tablet-host reconciler (ADR 0031 PR4) — wraps whichever
/// backend [`SharedEngine`] chose at start, mirroring [`CpGroup`]'s own
/// two-backend shape: the reconciler (`animus_cp_data::host::Reconciler`) is
/// generic over the concrete storage engine type, but a node's backend choice
/// is a runtime value (`StorageBackend`), so this enum picks the
/// instantiation exactly like `CpGroup` does for `RaftKvNode` itself.
enum CpReconciler {
    Lsm(Reconciler<ProdEnv, LsmEngine<ProdEnv>>),
    Mem(Reconciler<ProdEnv, MemoryEngine>),
}

impl CpReconciler {
    /// One reconcile tick — see [`Reconciler::tick`]'s doc for the pure
    /// `plan` decision plus its own execution of the returned actions.
    async fn tick(&mut self, view: &MetadataView) {
        match self {
            CpReconciler::Lsm(r) => r.tick(view).await,
            CpReconciler::Mem(r) => r.tick(view).await,
        }
    }

    /// ADR 0044 phase-1 PR4 production wiring — see
    /// [`Reconciler::enable_quiescence`]'s doc.
    fn enable_quiescence(&mut self, after: Duration) {
        match self {
            CpReconciler::Lsm(r) => r.enable_quiescence(after),
            CpReconciler::Mem(r) => r.enable_quiescence(after),
        }
    }
}

/// How often [`tablet_host_reconciler_loop`] falls back to a plain poll when
/// no `metadata_watch` wake arrives before this elapses. The trigger is
/// event-driven now (ADR 0031 PR4), so this is **not** the primary cadence —
/// it exists so a node whose own control-plane raft never advances (a
/// control-plane-follower-less growth node, ADR 0030, which reads
/// `effective_metadata()` from the `remote_metadata_sync_loop` mirror
/// instead of real Raft replication) still reconciles periodically. Matches
/// the old `CP_JOIN_HOST_INTERVAL`'s cadence, which served the same
/// "responsive enough, cheap enough" role for every node before this PR.
const RECONCILE_FALLBACK_INTERVAL: Duration = Duration::from_millis(500);

/// ADR 0058 Train 2 rung 3 residue: [`tablet_host_reconciler_loop`]'s own
/// fallback interval while **any** tablet cluster-wide currently carries an
/// in-place split intent (`Tablet::inplace_split.is_some()`) — far shorter
/// than [`RECONCILE_FALLBACK_INTERVAL`].
///
/// **Why this exists**: Stage 1-3 of an in-place split (learner add,
/// catch-up, the fork itself) happen entirely on the CP data plane's own
/// per-tablet Raft log — none of it commits a control-plane command, so
/// `metadata_watch` never fires again after `BeginSplitInPlace`'s own
/// commit until `CutoverSplit` eventually does. Between those two points,
/// EVERY replica's reconciler (this loop) is the only thing that ever
/// notices "this replica's own fork has completed, materialize both
/// children" (`HostAction::MaterializeSplitChild`, keyed on
/// `Tablet::inplace_split` still being present) — so its own tick cadence
/// during that window is a direct, hard bound on how long a fork can sit
/// un-materialized on a given replica before the `animusd`-level cutover
/// driver (`index_drain.rs::inplace_split_driver_tick`) might legally
/// propose `CutoverSplit` and remove that signal out from under it. At the
/// ordinary [`RECONCILE_FALLBACK_INTERVAL`] (500ms), that driver's own
/// (much faster, `INDEX_DRAIN_INTERVAL`-paced) observation of the local
/// fork routinely outraces this loop's next scheduled tick — proposing
/// cutover before every fork participant has even had a chance to
/// materialize, which is silent, permanent data loss for whichever replica
/// loses that race (see `inplace_split_driver_tick`'s own doc for the full
/// argument and the `ProdEnv` regression that found it). Shortening this
/// loop's own cadence specifically during an active split closes that gap
/// on every node — leader and follower alike, all of whom independently
/// observe the same `BeginSplitInPlace` commit and so all flip into fast
/// polling together — while adding no measurable steady-state cost (a
/// cluster with no in-place split in flight never engages it).
const INPLACE_SPLIT_RECONCILE_INTERVAL: Duration = Duration::from_millis(50);

/// The single per-node **tablet-host reconciler trigger** (ADR 0031 PR4):
/// replaces the three loops this file used to run independently
/// (`cp_reconfigure_loop`, `cp_join_host_loop`, `cp_gc_loop`'s reclaim +
/// release phases) with one reaction to `Metadata` changes, driving
/// `animus_cp_data::host::Reconciler` — the pure `plan` decision plus its own
/// execution of the returned actions (`animus-cp-data`'s `host` module doc
/// covers the full lifecycle: narrow an already-hosted tablet's scope, host a
/// newly-placed one, reconfigure a group this node leads toward its
/// replicated replica set, then release/reclaim a tablet moved off or
/// dropped — always in that fixed order, so "narrow before erase" and
/// "reconfigure only a hosted tablet" are structural properties of the
/// planner's output, not properties some ordering of independent loop ticks
/// happened to provide).
///
/// Each firing takes exactly **one** `Metadata` snapshot
/// (`ctx.effective_metadata()`, so a control-plane-follower-less growth node
/// reads the same mirror the old loops did) and calls
/// [`CpReconciler::tick`] once.
///
/// **Event-driven, with a periodic fallback**: races
/// `ctx.control.metadata_watch().changed(last_seen)` (an executor-agnostic
/// "applied index advanced" notification, ADR 0031 §trigger) against a
/// [`RECONCILE_FALLBACK_INTERVAL`] sleep. The fallback is load-bearing, not
/// just a safety net: a control-plane-follower-less growth node's own
/// `RaftCore` never receives real Raft replication for a group it was never a
/// voter of (ADR 0030's documented v1 limitation), so its `metadata_watch`
/// never fires — such a node's reconciler only ever ticks off the fallback,
/// reading `effective_metadata()`'s `remote_metadata_sync_loop` mirror
/// instead. Whichever branch wakes the loop, **coalesce to the freshest
/// observed index** (`watch.latest()`) before the next wait — a burst of
/// several commits under bulk load collapses into one tick, not one per
/// entry.
///
/// The `last_applied() == 0` pre-recovery guard (see
/// `animus_cp_data::host::plan`'s own doc: deciding "dropped" from *absence*
/// is sound only over recovered, durable metadata — an empty pre-recovery
/// `Metadata` would otherwise read as "everything dropped" and spuriously
/// reclaim/release real, still-hosted tablets) stays here, as a live
/// `RaftNode` read the pure planner has no business taking. It is gated on
/// **this node's own local control raft specifically**, not on
/// `effective_metadata()`'s availability — a control-plane-follower-less
/// growth node's local raft never leaves `last_applied() == 0` (it is a
/// permanent non-voter of a group it never replicates), so the guard also
/// requires its remote mirror to still be empty before skipping a tick, or a
/// growth node's reconciler would never tick at all. **ADR 0035 PR4** adds a
/// third OR-term, `ctx.control.has_synced_metadata()`: a data-only node's
/// `ControlHandle::Remote` has no local raft at all, so its `last_applied()`
/// is pinned at `0` forever (never a "recovered" signal) and it never
/// populates `ctx.remote_metadata` (that field is the ADR 0030 growth-node
/// mirror specifically — `Remote` keeps its own, read straight through
/// `metadata_cached()`); without this third term the guard would never
/// release a data-only node's reconciler, ever.
async fn tablet_host_reconciler_loop(ctx: ClientCtx, mut reconciler: CpReconciler) {
    let watch = ctx.control.metadata_watch();
    let mut last_seen = watch.latest();
    // ADR 0058 Train 2 rung 3 residue: whether the LAST snapshot this loop
    // saw carried an active in-place split intent anywhere — see
    // `INPLACE_SPLIT_RECONCILE_INTERVAL`'s own doc. Starts `false`
    // (ordinary cadence) — the very first tick after any `BeginSplitInPlace`
    // commits arrives via `watch.changed()` regardless (a real commit always
    // wakes this loop immediately), so this never delays noticing a split's
    // start; it only shortens every tick *after* that one, for as long as
    // the intent remains present.
    let mut inplace_split_active = false;
    loop {
        let fallback = if inplace_split_active {
            INPLACE_SPLIT_RECONCILE_INTERVAL
        } else {
            RECONCILE_FALLBACK_INTERVAL
        };
        tokio::select! {
            _ = watch.changed(last_seen) => {}
            _ = tokio::time::sleep(fallback) => {}
        }
        // Coalesce: take the freshest observed index regardless of which arm
        // woke the loop (the `changed()` future's own resolved value is not
        // enough — `latest()` may have advanced further still), so a burst of
        // commits under load collapses into one tick instead of one per entry.
        last_seen = watch.latest();

        // Recovery guard (see doc above): skip entirely before this node has
        // *some* trustworthy view of `Metadata` — either its own recovered
        // local control raft, or (for a growth node) a populated remote
        // mirror. Before either exists, `effective_metadata()` reads as a
        // default, empty `Metadata`, which would otherwise look like
        // "everything dropped" to the reclaim/release phases.
        if ctx.control.last_applied() == 0
            && ctx
                .remote_metadata
                .lock()
                .expect("remote metadata poisoned")
                .is_none()
            && !ctx.control.has_synced_metadata()
        {
            continue;
        }

        let meta = ctx.effective_metadata();
        let down: BTreeSet<NodeId> = meta
            .members
            .iter()
            .filter(|(_, m)| m.status == NodeStatus::Down)
            .map(|(id, _)| id.clone())
            .collect();
        inplace_split_active = meta.tablets.values().any(|t| t.inplace_split.is_some());
        let view = MetadataView {
            tablets: meta.tablets,
            down,
        };
        reconciler.tick(&view).await;
    }
}

/// How often [`txn_resolver_loop`] sweeps this node's locally-led tablet
/// groups (ADR 0018 §2/PR5). A plain fixed interval — no jitter — matching
/// the existing `RECONCILE_FALLBACK_INTERVAL`/`AUTO_SPLIT_INTERVAL` loops'
/// own shape; this is a background safety net, not a latency-sensitive
/// path, so the simpler fixed cadence was preferred over adding a jitter
/// source for a background loop none of its siblings use either.
const TXN_RESOLVER_INTERVAL: Duration = Duration::from_secs(1);

/// The **intent-resolver background task** (ADR 0018 §2/PR5) — what makes a
/// crashed coordinator harmless (the Decision section's Recovery bullet)
/// and lets [`ClientCtx::cp_txn`]'s successful-commit resolve be async/
/// best-effort rather than synchronous: on every tick, for each tablet
/// group this node currently **leads** (`ctx.edge.hosted_groups()`, no-op
/// on a control-only node — it hosts none), push every stale `Pending`
/// record ([`RaftKvNode::pending_txns`], via
/// [`ClientCtx::txn_recover`]) and fan out a resolve for every
/// decided-but-not-yet-locally-resolved one
/// ([`RaftKvNode::unresolved_decided`]). Errors are logged and swallowed —
/// this is a best-effort background sweep; the next tick retries.
async fn txn_resolver_loop(ctx: ClientCtx) {
    loop {
        tokio::time::sleep(TXN_RESOLVER_INTERVAL).await;
        for (tablet, group) in ctx.edge.hosted_groups() {
            if !group.is_leader() {
                continue;
            }
            // ADR 0044 phase-1 PR6: sound by the identical argument
            // `change_consumer_loop`'s own gate gives — a quiesced group's
            // `TxnTracker` is, by construction, empty (PR5's in-crate veto
            // is exactly "a non-empty tracker never quiesces"), so both
            // loops below are guaranteed no-ops here; skip the
            // `pending_txns()`/`unresolved_decided()` clones entirely.
            if group.is_quiesced() {
                continue;
            }
            let Some(table) = ctx
                .effective_metadata()
                .tablets
                .get(&tablet)
                .and_then(|t| t.table.clone())
            else {
                continue; // legacy whole-keyspace tablet, or a stale view — skip this tick
            };

            for (txn_id, (record_key, _created_ts)) in group.pending_txns() {
                // No intent hint needed/available here — `pending_txns` only
                // ever tracks a genuine, locally-anchored `Pending` record
                // (never an orphan), so `txn_recover`'s record-absent branch
                // is unreachable from this caller by construction.
                if let Err(e) = ctx.txn_recover(&table, &record_key, &txn_id, None).await {
                    tracing::debug!(
                        tablet = tablet.0,
                        ?txn_id,
                        error = %e,
                        "txn_resolver_loop: recovery push failed this tick"
                    );
                }
            }
            for (txn_id, (record_key, outcome)) in group.unresolved_decided() {
                // `unresolved_decided` only carries `(record_key, outcome)`
                // — re-read the record's own `intent_spans` (every
                // participant table this transaction touched) rather than
                // guess it.
                let Ok(view) = ctx.txn_record_view(&table, &record_key).await else {
                    continue; // transient — retried next tick
                };
                ctx.recovery_resolve(
                    txn_id,
                    record_key,
                    &view.intent_spans,
                    &outcome_to_status(&outcome),
                )
                .await;
            }
        }
        if let Some(data) = ctx.data.as_ref() {
            data.raftkv_metrics.incr(Metric::CpTxnResolverRuns);
        }
    }
}

/// How often [`metrics_sample_loop`] takes a metrics snapshot for the
/// dashboard's history sparklines. Not the determinism-critical `Metric`/
/// `MetricSink` seam itself (that stays timestamp-free) — this loop only
/// *reads* it, on a real wall clock, from `animusd`'s already-`ProdEnv`-only
/// code, matching the `PEER_SYNC_INTERVAL`-style loops above.
const METRICS_SAMPLE_INTERVAL: Duration = Duration::from_secs(10);
/// How many samples the in-memory ring buffer keeps — at the interval above,
/// ~2 hours. Not persisted (ADR 0020 admin surfaces are live introspection,
/// not a time-series database); enough to see a recent trend, not a history.
const METRICS_HISTORY_CAP: usize = 720;

/// One sample in the metrics-history ring buffer: a snapshot of
/// [`ClientCtx::metrics_json`] plus a wall-clock timestamp (Unix millis).
#[derive(Serialize, Clone)]
pub(crate) struct MetricsSample {
    ts_ms: u64,
    counters: BTreeMap<String, u64>,
    is_leader: i64,
}

/// Appends a [`MetricsSample`] to `ctx`'s ring buffer every
/// [`METRICS_SAMPLE_INTERVAL`], capped at [`METRICS_HISTORY_CAP`] entries —
/// backing the dashboard's metrics-history sparklines (`/admin/metrics/history`).
/// Real wall-clock sleep/timestamp: `animusd` is outside the `Env` determinism
/// boundary (ADR 0003 only binds sim-tested core crates), so this is exactly
/// as legitimate as the other `tokio::time`-driven loops in this file.
async fn metrics_sample_loop(ctx: ClientCtx) {
    loop {
        tokio::time::sleep(METRICS_SAMPLE_INTERVAL).await;
        // ADR 0044 phase-1 PR7: a level gauge — how many of this node's
        // currently-hosted CP-data groups this sample found quiesced.
        // `CpGroup::is_quiesced()` reads a frozen accessor and never itself
        // wakes a group (fork F: admin/dashboard reads must never disturb
        // the fleet-wide idle-cost win quiescence exists for), so sampling
        // this on the same cadence as every other metrics snapshot is free.
        // A control-only node's `ctx.edge.hosted_groups()` is always empty
        // (it never registers a raftkv handle), so this is a no-op there;
        // gated on `ctx.data` regardless, matching every other raftkv-only
        // metric this loop's sibling background loops record.
        if let Some(data) = ctx.data.as_ref() {
            let quiesced = ctx
                .edge
                .hosted_groups()
                .iter()
                .filter(|(_, g)| g.is_quiesced())
                .count() as u64;
            data.raftkv_metrics.set(Metric::CpGroupsQuiesced, quiesced);
        }
        let (counters, is_leader) = ctx.metrics_json();
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let mut history = ctx
            .metrics_history
            .lock()
            .expect("metrics history poisoned");
        if history.len() >= METRICS_HISTORY_CAP {
            history.pop_front();
        }
        history.push_back(MetricsSample {
            ts_ms,
            counters,
            is_leader,
        });
    }
}

/// How often the auto-split loop samples tablet sizes (Phase 2.4). A slow
/// background activity, well off any request path; spaced so a triggered split
/// settles (metadata + the new group) before the next sample re-reads sizes.
const AUTO_SPLIT_INTERVAL: Duration = Duration::from_secs(2);
/// After triggering a tablet's split, the loop skips re-triggering that tablet for
/// this long — long enough for the split to apply (the parent tablet's key count
/// then halves below the threshold, so it won't re-trigger anyway, but this guards
/// the in-flight window against a duplicate trigger).
const AUTO_SPLIT_COOLDOWN: Duration = Duration::from_secs(15);
/// Assumed bytes per SSTable entry when converting on-disk bytes into the
/// auto-split gate's key-count **estimate** ([`CpGroup::approx_key_count`]).
/// Deliberately small (real entries are larger), so bytes ÷ this *over*-estimates
/// the key count — the gate then errs toward confirming with a real count rather
/// than missing a split. The periodic confirm (one full count per tablet per
/// [`AUTO_SPLIT_COOLDOWN`]) bounds the miss window even if compression pushes a
/// table's real bytes-per-entry below this.
const AUTO_SPLIT_EST_ENTRY_BYTES: u64 = 32;

/// The auto-split trigger's configured thresholds (ADR 0034). Either, both, or
/// neither field may be `Some`; `auto_split_loop` is only spawned when at
/// least one is (see [`BoundNode::start_with`]'s doc). When both are set,
/// **either** exceeding its threshold fires a split — a byte-heavy tablet of
/// few huge values and a key-heavy tablet of many tiny ones are different
/// failure modes (snapshot/compaction/replica-move/recovery cost scales with
/// bytes; some operational costs still scale with key count, e.g. bulk scan
/// iteration overhead), so neither trigger alone dominates the other.
#[derive(Clone, Copy, Debug)]
struct AutoSplitThresholds {
    /// `--auto-split K`: split once a led tablet holds more than `K` keys.
    keys: Option<usize>,
    /// `--auto-split-bytes B` (ADR 0034): split once a led tablet's
    /// (approximate) scoped bytes exceed `B`.
    bytes: Option<u64>,
    /// `--auto-split-change-rate RATE` (ADR 0042 §14, growth PR3 Fork F):
    /// split once a **streamed** led tablet's own smoothed change-append
    /// rate ([`ChangeRateTracker`], bytes/sec) exceeds `RATE`. Absent by
    /// default (opt-in only, no surprise splits on an existing deployment)
    /// — an unstreamed table is never subject to this trigger regardless
    /// of this setting, since the rate is only ever tracked for a streamed
    /// tablet in the first place (`index_drain::seal_tick` only runs its
    /// seal arm, which feeds the tracker, when `stream_enabled`).
    change_rate: Option<u64>,
}

/// F11 (ADR 0042 §14): the exact error [`ClientCtx::trigger_split`] returns
/// when [`align_split_key`] finds a streamed table's split key rounds down
/// onto the target tablet's own `range.start` — matched by `auto_split_loop`
/// to downgrade its logging (Fork E: skip + meter via
/// [`Metric::StreamSplitSingleTokenSkipped`], never its ordinary "split did
/// not commit" warning, which would otherwise fire every cooldown, forever,
/// for a single-token hot partition that structurally cannot split).
const SPLIT_KEY_NOT_TOKEN_VIABLE: &str =
    "split key rounds onto the tablet's own range start (single-token hot partition)";

/// F11 (ADR 0042 §14, Fork D): round `split_key` down to its own 8-byte
/// token boundary (`TOKEN_BYTES`) if `tablet`'s table is streamed, so one
/// partition key's records — and hence one shard's, ADR 0043 §A4 — never
/// separate across sibling tablets. A plain, unstreamed table's split key is
/// returned unchanged. Called from the **one** choke point every split
/// proposer funnels through ([`ClientCtx::trigger_split`]), so this can
/// never be forgotten by a future caller the way the pre-PR2 code (rounding
/// done only inside `auto_split_loop`) could be bypassed by the two manual
/// paths (`POST /admin/tablet/split`, `ClientRequest::SplitTablet`).
///
/// Returns `(key, viable)`: `viable` is whether the (possibly rounded) key
/// is still a legal **interior** split point for `tablet`'s current range
/// (`KeyRange::split_at`'s own "strictly inside" rule). Rounding a hot
/// single-token partition's own key can collapse it onto `range.start` —
/// Fork E's accepted single-token hot-partition limit: one very hot
/// partition key ends up owning the tablet's entire range, and it can never
/// legally split without separating that same token's records across
/// siblings — the exact affinity F11 exists to protect. `viable == false`
/// for an unknown tablet too (the caller's own subsequent lookup reports
/// that more precisely; this just never claims a key is fine for a tablet
/// this function can't even see).
fn align_split_key(meta: &Metadata, tablet: TabletId, split_key: Vec<u8>) -> (Vec<u8>, bool) {
    let Some(t) = meta.tablets.get(&tablet) else {
        return (split_key, false);
    };
    let streamed = t
        .table
        .as_deref()
        .is_some_and(|table| meta.table_stream(table).is_some());
    let key = if streamed {
        split_key[..TOKEN_BYTES.min(split_key.len())].to_vec()
    } else {
        split_key
    };
    let viable = t.range.split_at(&key).is_some();
    (key, viable)
}

/// Choose the two `BeginSplit` children's replica sets (ADR 0050 fork F5:
/// **fresh placement at mint** — children are born at their final homes, so
/// the copy-based build is the only data movement a split ever makes).
///
/// Pure over the given `Metadata` snapshot: candidates are the `Active`
/// members (the same liveness rule `Metadata::reconcile` applies), the
/// parent's own placement policy carries RF/residency/spread, and per-node
/// load is the current replica count across every tablet (seeded `0` for
/// every candidate — a fresh member is a genuine minimum, matching
/// `rebalance_step`'s rule). The second child's selection sees the first
/// child's picks as load, so the two don't pile onto the same least-loaded
/// nodes. A parent with no recorded policy (not a state `provision_tablet`
/// produces, but reachable via a hand-built cluster) falls back to
/// inheriting the parent's own replica set for both children — the
/// pre-fork-F5 behavior, safe if never balanced.
fn split_child_placement(meta: &Metadata, parent: TabletId) -> Result<[Vec<NodeId>; 2], String> {
    let Some(parent_tablet) = meta.tablets.get(&parent) else {
        return Err("no such tablet".into());
    };
    let Some(policy) = meta.policies.get(&parent) else {
        return Ok([
            parent_tablet.replicas.clone(),
            parent_tablet.replicas.clone(),
        ]);
    };
    let candidates: Vec<animus_control::Candidate> = meta
        .members
        .iter()
        .filter(|(_, m)| m.status == NodeStatus::Active)
        .map(|(id, m)| animus_control::Candidate::new(id.clone(), m.labels.clone()))
        .collect();
    let mut load: BTreeMap<NodeId, usize> =
        candidates.iter().map(|c| (c.node.clone(), 0)).collect();
    for t in meta.tablets.values() {
        for r in &t.replicas {
            if let Some(n) = load.get_mut(r) {
                *n += 1;
            }
        }
    }
    // Placement can be under-satisfiable right now (a dev/single-node
    // cluster whose recorded RF exceeds the live member count — the exact
    // state `provision_tablet` already documents as legitimate, with repair
    // self-healing once members exist). Fall back to inheriting the
    // parent's own replica set then, exactly like the no-policy case above:
    // best-effort placement now, `reconcile_placement` fixes the children
    // up after cutover once they are `Active` again.
    let pick = |load: &BTreeMap<NodeId, usize>| {
        animus_control::select_replicas_balanced(&candidates, policy, load)
            .unwrap_or_else(|_| parent_tablet.replicas.clone())
    };
    let left = pick(&load);
    for r in &left {
        if let Some(n) = load.get_mut(r) {
            *n += 1;
        }
    }
    let right = pick(&load);
    Ok([left, right])
}

/// The leader-driven **automatic split trigger**: on each tick, for every tablet
/// whose CP group this node currently **leads**, take the leader's **cheap
/// estimates** ([`CpGroup::approx_key_count`]/[`CpGroup::approx_bytes`] —
/// memtable + SSTable metadata, no materialization) and only when one says the
/// tablet might exceed its configured threshold (or on a slow per-tablet confirm
/// cadence) materialize the live pairs once — the authoritative key count, byte
/// total, and (if over threshold) **split key** all come from that one snapshot.
/// Per-tablet cooldown avoids a duplicate trigger while a split is in flight;
/// once it applies, the parent's counts halve below both thresholds.
///
/// **The split point is byte-weighted whenever a byte threshold is configured**
/// (ADR 0034 — [`byte_weighted_median`], the key that roughly bisects the
/// tablet's *bytes*, not just its key count): with skewed value sizes a plain
/// positional median can leave one huge half and one tiny half, which
/// immediately re-triggers on the huge side. A key-count-only configuration
/// (`bytes: None`) keeps the plain positional median byte-for-byte unchanged
/// from before this ADR, so existing key-count auto-split behavior/tests are
/// untouched.
///
/// Since a split is now a **single, atomic, epoch-CAS-gated** control-plane
/// command (`ClientCtx::trigger_split`, mirroring `CasTabletReplicas`), there is
/// no second, independently-failable data-plane step and therefore no orphan
/// tablet it could leave behind — the whole two-phase `pending`/`claim_auto_split`
/// retry-and-cleanup machinery this loop used to need is gone. A losing proposer's
/// `SplitTablet` is rejected cleanly at propose time (stale epoch); the winner's
/// commit is the entire operation.
///
/// Only the node hosting a tablet's leader reads `local_pairs`/triggers — `edge`
/// is per-node (ADR 0031 PR2), so `ctx.edge.cp_leader(tablet)` only returns
/// `Some` on the one node that actually leads that tablet's group, in both
/// one-process-per-node and `--cluster N`. A genuine same-tick race is still
/// possible (e.g. a leadership handoff mid-tick, or two distinct trigger
/// sources such as a manual split racing this loop) — harmless: the epoch CAS
/// lets exactly one win, and the loser just tries again (or backs off) next
/// tick.
async fn auto_split_loop(ctx: ClientCtx, thresholds: AutoSplitThresholds) {
    let mut last_triggered: BTreeMap<TabletId, tokio::time::Instant> = BTreeMap::new();
    // When each tablet last had a *full* (materializing) count — the expensive
    // confirm is rate-limited per tablet, not run every tick.
    let mut last_counted: BTreeMap<TabletId, tokio::time::Instant> = BTreeMap::new();
    loop {
        tokio::time::sleep(AUTO_SPLIT_INTERVAL).await;

        // `effective_metadata()` so a mirror-fed node (ADR 0030 / ADR 0035 PR4)
        // sees the live tablet map, not an empty local core's. Held for the
        // whole tick (not just the key-collection step) so the F11
        // token-alignment check below (ADR 0042 §14) shares one snapshot
        // with the tablet-list read, rather than paying a second clone.
        let meta = ctx.effective_metadata();
        // ADR 0050: only an `Active` tablet is auto-splittable — a
        // `Splitting` parent is already mid-workflow (one split at a time)
        // and a `Building` child is still being seeded.
        let tablets: Vec<TabletId> = meta
            .tablets
            .iter()
            .filter(|(_, t)| t.state == TabletState::Active)
            .map(|(&id, _)| id)
            .collect();
        for tablet in tablets {
            if matches!(last_triggered.get(&tablet), Some(at) if at.elapsed() < AUTO_SPLIT_COOLDOWN)
            {
                continue;
            }
            // Only the leader's host reads + triggers (else this node doesn't have
            // the leader handle).
            let Some(leader) = ctx.edge.cp_leader(tablet) else {
                continue;
            };
            // ADR 0044 phase-1 PR6: a quiesced group's bytes/key-count are,
            // by construction, static — no activity for `quiesce_after`
            // means no new writes since it quiesced, and a write is the
            // only way either could ever change. Whatever this tablet's
            // last pre-quiescence tick already checked (over threshold ⇒
            // triggered; under ⇒ correctly left alone) still holds, so
            // re-estimating/re-materializing it here (including the
            // periodic `due_confirm` correction below, which exists only
            // to catch estimate drift from *new* data) is pure waste until
            // something un-quiesces it.
            if leader.is_quiesced() {
                continue;
            }
            // Cheap per-tick gate: materializing every led tablet's live pairs
            // every tick is O(total data) per 2s — instead, take the free
            // (over-)estimate(s) and only materialize when one says the tablet
            // might exceed its threshold, or on a slow per-tablet confirm cadence
            // (bounded by `AUTO_SPLIT_COOLDOWN`) that corrects estimate error
            // (compression can push real bytes-per-entry below the assumed size;
            // the memory backend has no key-count estimate at all, though it does
            // have a byte estimate — `approx_bytes` works on any backend).
            let due_confirm = last_counted
                .get(&tablet)
                .is_none_or(|at| at.elapsed() >= AUTO_SPLIT_COOLDOWN);
            let key_hot = thresholds.keys.is_some_and(|t| {
                leader
                    .approx_key_count()
                    .is_some_and(|estimate| estimate > t)
            });
            let byte_hot = match thresholds.bytes {
                Some(t) => leader.approx_bytes().await > t,
                None => false,
            };
            // Growth PR3 Fork F (ADR 0042 §14): the opt-in change-append-rate
            // trigger — a streamed tablet's own smoothed rate
            // ([`ChangeRateTracker`]) is already cheap to read (no
            // materialization), exactly like the key/byte estimates above.
            // Reads as `0.0` (never hot) for an unstreamed tablet, since
            // nothing ever calls `ChangeRateTracker::observe` for one.
            let change_rate_hot = thresholds
                .change_rate
                .is_some_and(|t| ctx.data().change_rates.get(tablet) > t as f64);
            if !key_hot && !byte_hot && !change_rate_hot && !due_confirm {
                continue;
            }
            // Materialize once: the authoritative count, byte total, and (if over
            // threshold) the split key all come from the same snapshot.
            let pairs = leader.local_pairs().await;
            last_counted.insert(tablet, tokio::time::Instant::now());
            let key_count = pairs.len();
            let over_key_threshold = thresholds.keys.is_some_and(|t| key_count > t);
            let over_byte_threshold = thresholds.bytes.is_some_and(|t| {
                let total_bytes: u64 = pairs.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum();
                total_bytes > t
            });
            // Re-read (not reused from the cheap gate above): a materializing
            // confirm pass is exactly the point at which every other trigger
            // here re-derives its own authoritative value from this same
            // snapshot, and the tracker is cheap enough that re-reading it
            // costs nothing extra.
            let over_change_rate_threshold = thresholds
                .change_rate
                .is_some_and(|t| ctx.data().change_rates.get(tablet) > t as f64);
            // Need at least 2 distinct keys for any split to have an interior
            // point (`SplitTablet` requires `start < at < end`).
            if key_count < 2
                || (!over_key_threshold && !over_byte_threshold && !over_change_rate_threshold)
            {
                continue;
            }
            // A byte- or change-rate-configured cluster uses the
            // byte-weighted median (ADR 0034) so a skewed value-size
            // distribution still bisects the tablet's *bytes* roughly
            // evenly; a key-count-only cluster keeps the plain positional
            // median unchanged from before this ADR (the interior key of
            // `> threshold >= 2` distinct keys `SplitTablet` accepts).
            let split_key = if thresholds.bytes.is_some() || over_change_rate_threshold {
                byte_weighted_median(&pairs)
            } else {
                pairs[pairs.len() / 2].0.clone()
            };
            // F11 (ADR 0042 §14, Fork D): the token-alignment rounding itself
            // now lives inside `ClientCtx::trigger_split` — the one choke
            // point every split proposer (this loop, `POST
            // /admin/tablet/split`, `ClientRequest::SplitTablet`) funnels
            // through, so it can't be forgotten by a future caller the way
            // the pre-PR2 code (rounding done only here) let the two manual
            // paths bypass it. `trigger_split` returns immediately (no
            // propose attempt) with `SPLIT_KEY_NOT_TOKEN_VIABLE` for Fork
            // E's accepted single-token hot-partition limit — matched below
            // so this loop's own "split did not commit" warning never fires
            // for that expected, already-metered outcome (it would
            // otherwise fire every single cooldown, forever, for a tablet
            // that structurally cannot split).
            last_triggered.insert(tablet, tokio::time::Instant::now());
            let span = tracing::info_span!("auto_split", tablet = tablet.0);
            let response = ctx.trigger_split(tablet, split_key).instrument(span).await;
            match &response {
                ClientResponse::PutOk => {}
                ClientResponse::Error(msg) if msg == SPLIT_KEY_NOT_TOKEN_VIABLE => {}
                other => {
                    tracing::warn!(
                        tablet = tablet.0,
                        ?other,
                        "auto_split: split did not commit"
                    );
                }
            }
        }
    }
}

/// The key that most closely bisects `pairs`' **total bytes** (`key.len() +
/// value.len()` per pair), not just its key count (ADR 0034): among every
/// interior split point `i` (`1 <= i <= pairs.len() - 1`, splitting into
/// `pairs[..i]` / `pairs[i..]`), returns `pairs[i].0` for the `i` whose left
/// side's byte total is closest to half the whole. A key's bytes can never be
/// divided across the split (a split point is always a whole key boundary),
/// so when one key's own bytes are a large fraction of the total, the
/// closest achievable split may still be lopsided — this picks the least
/// lopsided **achievable** boundary, which is the best any key-boundary split
/// can do. With skewed value sizes this avoids the plain positional median's
/// failure mode: a few huge values among many tiny ones would otherwise put
/// almost all the bytes on one side regardless of how many *keys* end up on
/// each side, which immediately re-triggers a split on the huge side instead
/// of settling below threshold.
///
/// Always returns an **interior** key (`i >= 1`, so never `pairs[0].0`,
/// matching the positional median's own "index > 0" guarantee). Requires
/// `pairs.len() >= 2` (the same precondition `auto_split_loop` already checks
/// before calling this — there is no meaningful split point for 0 or 1 keys).
fn byte_weighted_median(pairs: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    debug_assert!(
        pairs.len() >= 2,
        "need >= 2 keys for an interior split point"
    );
    let total: u64 = pairs.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum();
    let half = total / 2;
    let mut best_idx = 1;
    let mut best_diff = u64::MAX;
    let mut prefix: u64 = 0; // bytes of pairs[0..i], updated *before* considering split `i`.
    for (i, (key, value)) in pairs.iter().enumerate() {
        if i >= 1 {
            let diff = prefix.abs_diff(half);
            if diff < best_diff {
                best_diff = diff;
                best_idx = i;
            }
        }
        prefix += (key.len() + value.len()) as u64;
    }
    pairs[best_idx].0.clone()
}

/// Growth PR3 (ADR 0042 §14): the exact error [`ClientRequest::
/// TriggerAutoSplit`]'s handler (and, table-wide, [`ClientCtx::
/// grow_stream`]) returns for a tablet with fewer than 2 distinct keys — no
/// legal interior split point exists at all, regardless of tokens (the same
/// precondition `auto_split_loop` checks before ever computing a median).
/// Distinct from [`SPLIT_KEY_NOT_TOKEN_VIABLE`] (a real single-token
/// hot-partition collapse) so a caller can tell "nothing to split" from
/// "one partition owns everything" — both are skips, never hard failures.
const STREAM_GROW_NO_SPLIT_POINT: &str =
    "tablet has fewer than 2 distinct keys — no legal interior split point";

/// Expected per-tablet skip [`ClientCtx::grow_stream`] reports for a tablet
/// already inside the ADR 0050 split workflow — a `Splitting` parent (its
/// split is already in flight, so this call performs nothing for it) or a
/// `Building` child (unsplittable until activation). Reported *instead of*
/// calling [`ClientCtx::grow_stream_tablet`] at all: routing a median read
/// at a mid-split tablet is wasted work, and `trigger_split`'s own
/// idempotent `PutOk` for a `Splitting` parent would otherwise be counted
/// by the admin summary as a split *this call* performed (Train B rung 6;
/// was rung 3's noted "mid-split cosmetic"). A skip, never a failure —
/// classified alongside [`STREAM_GROW_NO_SPLIT_POINT`]/
/// [`SPLIT_KEY_NOT_TOKEN_VIABLE`] by `admin::action_stream_grow`.
const STREAM_GROW_MID_SPLIT: &str = "tablet is mid-split — its split workflow is already in flight";

/// Materialize `group`'s own live pairs and compute their byte-weighted
/// median (ADR 0034's [`byte_weighted_median`]) — the same key
/// `auto_split_loop` computes for a byte-configured cluster, reused here for
/// growth PR3's manual `POST /admin/stream/grow` trigger and Fork F's
/// change-rate auto-trigger, neither of which has (or needs) a byte/key
/// **threshold** of its own: an explicit trigger always uses the
/// byte-weighted metric, unconditionally. Returns `None` for fewer than 2
/// distinct keys (no legal interior split point regardless of tokens) —
/// the caller answers [`STREAM_GROW_NO_SPLIT_POINT`] rather than ever
/// calling [`ClientCtx::trigger_split`] with a meaningless key.
async fn median_split_key(group: &CpGroup) -> Option<Vec<u8>> {
    let pairs = group.local_pairs().await;
    if pairs.len() < 2 {
        return None;
    }
    Some(byte_weighted_median(&pairs))
}

/// Accept loop shared by **both** listeners (ADR 0047) — the client port and
/// the intra-cluster port alike — parameterized by [`ListenerKind`] rather
/// than forked: `spawn_common_tail` spawns two instantiations of this same
/// function, one per listener, and threads `listener` straight through
/// [`handle_connection`] into [`handle_request`]'s one guard clause. Replaces
/// the pre-ADR-0047 `serve_clients`/`handle_client` pair, which only ever
/// served the client port.
async fn serve_requests(listener_socket: TcpListener, ctx: ClientCtx, listener: ListenerKind) {
    loop {
        match listener_socket.accept().await {
            Ok((stream, _addr)) => {
                let ctx = ctx.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(stream, ctx, listener).await {
                        tracing::debug!(?err, "connection closed");
                    }
                });
            }
            Err(err) => {
                tracing::warn!(?err, "accept failed");
                return;
            }
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    ctx: ClientCtx,
    listener: ListenerKind,
) -> std::io::Result<()> {
    while let Some(request) = read_frame::<ClientRequest>(&mut stream).await? {
        // Every accepted request is a root span (ADR 0027): this is what gives
        // `otel::current_traceparent()` something to inject if the request's
        // handling ends up forwarding to another node (`cp_forward`), and what
        // a `Forwarded` request's own span below joins as a child of the
        // originating node's trace.
        let span = tracing::info_span!("client_request", request = request_kind(&request));
        if let ClientRequest::Forwarded {
            traceparent: Some(tp),
            ..
        } = &request
        {
            otel::set_parent_traceparent(&span, tp);
        }
        let response = handle_request(&ctx, request, listener)
            .instrument(span)
            .await;
        write_frame(&mut stream, &response).await?;
    }
    Ok(())
}

/// A short, closed label for `ClientRequest`'s variant — the `client_request`
/// span's `request` field (ADR 0027 field vocabulary).
fn request_kind(request: &ClientRequest) -> &'static str {
    match request {
        ClientRequest::Status => "status",
        ClientRequest::Put { .. } => "put",
        ClientRequest::PutBatch { .. } => "put_batch",
        ClientRequest::KindWrite { .. } => "kind_write",
        ClientRequest::KindWriteItem { .. } => "kind_write_item",
        ClientRequest::KindScan { .. } => "kind_scan",
        ClientRequest::ForceSeal { .. } => "force_seal",
        ClientRequest::TriggerAutoSplit { .. } => "trigger_auto_split",
        ClientRequest::StreamHotRead { .. } => "stream_hot_read",
        ClientRequest::ClearBackfillCursor { .. } => "clear_backfill_cursor",
        ClientRequest::SeedRows { .. } => "seed_rows",
        ClientRequest::Get { .. } => "get",
        ClientRequest::GetSnapshot { .. } => "get_snapshot",
        ClientRequest::Scan { .. } => "scan",
        ClientRequest::Delete { .. } => "delete",
        ClientRequest::Forwarded { .. } => "forwarded",
        ClientRequest::ProposeSchema(_) => "propose_schema",
        ClientRequest::SplitTablet { .. } => "split_tablet",
        ClientRequest::JoinInfo => "join_info",
        ClientRequest::WatchMetadata { .. } => "watch_metadata",
        ClientRequest::Txn { .. } => "txn",
        ClientRequest::TxnPrepare { .. } => "txn_prepare",
        ClientRequest::TxnDecide { .. } => "txn_decide",
        ClientRequest::TxnResolve { .. } => "txn_resolve",
        ClientRequest::TxnStatus { .. } => "txn_status",
        ClientRequest::TxnRecordView { .. } => "txn_record_view",
        ClientRequest::TxnVerify { .. } => "txn_verify",
    }
}

async fn handle_request(
    ctx: &ClientCtx,
    request: ClientRequest,
    listener: ListenerKind,
) -> ClientResponse {
    // The one asymmetric refusal rule (ADR 0047): only a `Client`-listener
    // connection asking for an `Intra`-surfaced variant is refused — the
    // reverse (an `Intra` listener serving a `Public` variant) is fine by
    // design, since intra is the more-trusted segment and neither port has
    // auth yet (see `Surface`'s doc). Everything below this guard is the
    // pre-ADR-0047 match, untouched.
    if listener == ListenerKind::Client && surface_of(&request) == Surface::Intra {
        return ClientResponse::Error(format!(
            "{} is a cluster-internal request; send it to this node's intra port",
            request_kind(&request)
        ));
    }
    match request {
        // `effective_metadata`, not `ctx.control.metadata_cached()` directly (mirroring
        // `/admin/status`, ADR 0030): on a control-plane-follower-less growth
        // node the local raft never replicates, so a bare `metadata_cached()`
        // would answer with a permanently-empty cluster — misleading for an
        // `animus status` CLI call, and a vacuous collision guard for an ADR
        // 0032 PR2 joiner that picked this (grown) node as its seed. Safe for
        // `remote_metadata_sync_loop`'s own polling: its seeds are always the
        // pre-growth control nodes (genuine voters, where this is a plain
        // passthrough), so no mirror ever feeds another mirror.
        ClientRequest::Status => ClientResponse::Status {
            metadata: ctx.effective_metadata(),
            leader_hint: ctx.control_leader_hint(),
            intra_leader_hint: ctx.intra_control_leader_hint(),
            watermark: ctx.control.metadata_watch().latest(),
            control_voters: ctx.control.config().unwrap_or_default(),
        },
        // All data ops route to the leaderful CP per-tablet Raft group (ADR 0017
        // #3a), scoped to the named table (ADR 0023). `table` is a required field
        // on the request type, so there is no unscoped data op to reject here.
        //
        // The plain client protocol is a real write surface (`animus-cli
        // put`), so since ADR 0049 (Train A rung 5) its mutations ride the
        // kind path and leave an image-less marker record like every other
        // edge's — a raw key has no `pk`/`sk` decomposition, so the marker
        // uses the full-key-as-prefix convention (`dynamo::
        // marker_change_log`'s doc). Always a marker, never images, even on
        // a streamed/indexed table: a raw value isn't a Dynamo item, so
        // there is no image to carry — but the write is at least observable
        // to change-log consumers (the old plain path emitted nothing at
        // all, the same silent-loss shape PR #249 fixed for
        // `BatchWriteItem`).
        ClientRequest::Put { key, value, table } => {
            let marker = dynamo::marker_change_log(&key, Vec::new());
            match dynamo::marker_batch_write_raw(
                ctx,
                &table,
                vec![(key, Some(value), marker)],
                true,
            )
            .await
            {
                Ok(()) => ClientResponse::PutOk,
                Err(e) => ClientResponse::Error(e),
            }
        }
        ClientRequest::PutBatch { entries, table } => {
            let rows = entries
                .into_iter()
                .map(|(key, value)| {
                    let marker = dynamo::marker_change_log(&key, Vec::new());
                    (key, Some(value), marker)
                })
                .collect();
            match dynamo::marker_batch_write_raw(ctx, &table, rows, true).await {
                Ok(()) => ClientResponse::PutOk,
                Err(e) => ClientResponse::Error(e),
            }
        }
        ClientRequest::Get { key, table, stale } => ctx.cp_get(&table, key, stale).await,
        ClientRequest::Scan {
            start,
            end,
            limit,
            reverse,
            table,
            stale,
        } => match ctx
            .cp_scan(
                &table,
                start,
                end,
                limit,
                reverse,
                ReadConsistency::from_consistent_read(!stale),
            )
            .await
        {
            Ok(pairs) => ClientResponse::Pairs(pairs),
            Err(e) => ClientResponse::Error(e),
        },
        ClientRequest::Delete { key, table } => {
            // A genuine engine delete + marker (see the `Put` arm's comment).
            // No auto-provision — the old `cp_delete` never conjured an
            // empty tablet for a table nothing provisioned.
            let marker = dynamo::marker_change_log(&key, Vec::new());
            match dynamo::marker_batch_write_raw(ctx, &table, vec![(key, None, marker)], false)
                .await
            {
                Ok(()) => ClientResponse::PutOk,
                Err(e) => ClientResponse::Error(e),
            }
        }
        // Admin: split a CP tablet — a single atomic control-plane command.
        ClientRequest::SplitTablet { tablet, split_key } => {
            ctx.trigger_split(TabletId(tablet), split_key).await
        }
        // A CP op forwarded from another node (cross-process routing, ADR 0017
        // #3b): serve locally iff we are the leader; never re-forward. The
        // enclosing `client_request` span (in `handle_connection`) was already
        // re-parented onto the originating node's trace (ADR 0027) before this
        // request reached here.
        ClientRequest::Forwarded { request, .. } => ctx.cp_serve_forwarded(*request).await,
        // A metadata command relayed to the control leader (A2 schema DDL, or a
        // Phase 2.3a CP-address registration). Gate to the relayable set, then
        // propose iff we are the leader (no re-relay — bounded one hop; the
        // relayer retries with fresh routing).
        ClientRequest::ProposeSchema(command) => {
            if !is_relayable_command(&command) {
                ClientResponse::Error("command not allowed over the relay path".into())
            } else {
                // Propose on the control leader (locally if we are it, else relay
                // toward it). The caller confirms the commit via replicated
                // `Metadata`. Cannot loop: a relay only targets a known leader.
                ctx.propose_schema(&command).await;
                ClientResponse::PutOk
            }
        }
        // Join discovery (ADR 0032 PR2): any node answers from its own
        // knowledge — no forwarding, no leader resolution needed.
        ClientRequest::JoinInfo => ClientResponse::JoinInfo {
            control_ids: ctx.admin.control_ids.clone(),
            peers: ctx.admin.peers.clone(),
            client_route: ctx.route_snapshot(),
            intra_route: ctx.intra_route_snapshot(),
            admin_addrs: ctx.admin.admin_addrs.clone(),
        },
        // Long-poll metadata watch (ADR 0035 PR5) — see `ClientCtx::
        // watch_metadata`'s doc.
        ClientRequest::WatchMetadata { last_seen } => ctx.watch_metadata(last_seen).await,
        // Multi-participant transaction (ADR 0018 §2/PR4): the client-facing
        // entry point. `ClientCtx::cp_txn` is itself the coordinator — it
        // resolves every participant tablet (forwarding as needed, exactly
        // like every other CP op) and drives the whole 2PC.
        ClientRequest::Txn {
            writes,
            preconditions,
            write_conditions,
        } => match ctx.cp_txn(writes, preconditions, write_conditions).await {
            Ok(commit_ts) => ClientResponse::TxnCommitted { commit_ts },
            // The raw client protocol's `Txn` reply carries a plain string
            // (unchanged wire shape) — `TxnAbortReason`'s `Display` is the
            // same human message `dynamo.rs::run_transact`'s own aggregate
            // fallback would have shown; only that Dynamo edge needs the
            // typed reason (ADR 0018's 2026-08-24 `CancellationReasons`
            // amendment, issue #374 C2b) to flag a specific action's index.
            Err(e) => ClientResponse::Error(e.to_string()),
        },
        // The six internal 2PC/recovery coordinator RPCs below are **never
        // sent as a bare top-level request** — a coordinator only ever
        // reaches them wrapped in `Forwarded` (even a Local route calls the
        // `CpGroup` method directly, in-process, no wire round trip at all
        // — see `ClientCtx::txn_prepare`/`txn_decide_anchor`/
        // `txn_resolve_participant`/`txn_status`/`txn_record_view`/
        // `txn_verify`). Grepped alongside every other gating site per the
        // house lesson on adding a variant to a forwarded command enum
        // (`docs/engineering-lessons.md`): these are data-plane RPCs, not
        // `MetaCommand`s, so `is_relayable_command` (control-plane
        // schema-DDL relay gating) does not apply to them — their real
        // handling lives in `ClientCtx::cp_serve_forwarded`'s match,
        // reached only via the `Forwarded` arm above.
        // ADR 0041 §3/§4: the DynamoDB edge's index-maintenance primitive, not
        // a client operation — see `ClientRequest::KindWrite`'s doc for why a
        // bare one is refused rather than served. Like the 2PC RPCs below it is
        // a data-plane request, not a `MetaCommand`, so `is_relayable_command`
        // (control-plane schema-DDL relay gating) does not apply; its real
        // handling lives in `cp_serve_forwarded`'s match, reached only through
        // the `Forwarded` arm above.
        ClientRequest::KindWrite { .. } => ClientResponse::Error(
            "this request is an internal index-maintenance RPC and must be sent wrapped in \
             `Forwarded`"
                .into(),
        ),
        // ADR 0046 U3: the evaluate-at-leader write RPC, refused bare for the
        // identical reason `KindWrite` just above is — see
        // `ClientRequest::KindWriteItem`'s doc. Real handling lives in
        // `cp_serve_forwarded`'s match, reached only through `Forwarded`; not
        // a `MetaCommand`, so `is_relayable_command` does not apply.
        ClientRequest::KindWriteItem { .. } => ClientResponse::Error(
            "this request is an internal evaluate-at-leader write RPC and must be sent wrapped \
             in `Forwarded`"
                .into(),
        ),
        // ADR 0041 §5: the LSI `Query` read primitive, the read-side dual of
        // `KindWrite` just above and refused for the identical reason — a
        // bare caller could otherwise read a table's LSI/change-log/
        // footprint bytes by kind number directly, bypassing the DynamoDB
        // surface that interprets them. Not a `MetaCommand`, so
        // `is_relayable_command` does not apply; real handling lives in
        // `cp_serve_forwarded`'s match, reached only through `Forwarded`.
        ClientRequest::KindScan { .. } => ClientResponse::Error(
            "this request is an internal index-read RPC and must be sent wrapped in `Forwarded`"
                .into(),
        ),
        // ADR 0018 §2, torn-pair-fix stack PR2: the `TransactGetItems`
        // quiescent-round non-blocking read primitive — refused bare for
        // the same reason `KindWrite`/`KindScan` are (see `GetSnapshot`'s
        // own doc). Real handling lives in `cp_serve_forwarded`'s match,
        // reached only through `Forwarded`; not a `MetaCommand`, so
        // `is_relayable_command` does not apply.
        ClientRequest::GetSnapshot { .. } => ClientResponse::Error(
            "this request is an internal non-blocking read RPC and must be sent wrapped in \
             `Forwarded`"
                .into(),
        ),
        ClientRequest::ForceSeal { .. } => ClientResponse::Error(
            "this request is an internal seal-trigger RPC and must be sent wrapped in \
             `Forwarded`"
                .into(),
        ),
        ClientRequest::TriggerAutoSplit { .. } => ClientResponse::Error(
            "this request is an internal growth split-trigger RPC and must be sent wrapped in \
             `Forwarded`"
                .into(),
        ),
        ClientRequest::StreamHotRead { .. } => ClientResponse::Error(
            "this request is an internal open-shard hot-read RPC and must be sent wrapped in \
             `Forwarded`"
                .into(),
        ),
        ClientRequest::SeedRows { .. } => ClientResponse::Error(
            "this request is an internal split-build seed RPC and must be sent wrapped in \
             `Forwarded`"
                .into(),
        ),
        ClientRequest::ClearBackfillCursor { .. } => ClientResponse::Error(
            "this request is an internal backfill-cursor-cleanup RPC and must be sent wrapped \
             in `Forwarded`"
                .into(),
        ),
        ClientRequest::TxnPrepare { .. }
        | ClientRequest::TxnDecide { .. }
        | ClientRequest::TxnResolve { .. }
        | ClientRequest::TxnStatus { .. }
        | ClientRequest::TxnRecordView { .. }
        | ClientRequest::TxnVerify { .. } => ClientResponse::Error(
            "this request is an internal 2PC coordinator RPC and must be sent wrapped in \
             Forwarded, never as a bare top-level request"
                .into(),
        ),
    }
}

/// How long the DynamoDB edge waits for a proposed schema `MetaCommand`
/// (`CreateTableSchema`/`DropTableSchema`) to commit through the control plane
/// before giving up. Generous: a fresh cluster may still be electing a leader.
const SCHEMA_COMMIT_TIMEOUT: Duration = Duration::from_secs(10);
/// Poll interval while waiting for a proposed schema command to commit / for a
/// leader to settle so the proposal can be (re)submitted.
const SCHEMA_POLL_INTERVAL: Duration = Duration::from_millis(50);
/// How long [`ClientCtx::propose_and_await`] waits, after a proposal it
/// believes reached a leader's log, before resubmitting it — see
/// [`ClientCtx::propose_schema`]'s doc for why blindly resubmitting every
/// [`SCHEMA_POLL_INTERVAL`] tick is a retry-amplification bug. A proposal
/// known *not* to have been sent anywhere (no leader reachable) is retried
/// every tick regardless, since that costs nothing.
const SCHEMA_PROPOSE_PATIENCE: Duration = Duration::from_secs(1);

/// Initial poll granularity while a CP **write/delete** waits for its value to
/// become locally durable+applied on the leader (the durable-before-ack confirm in
/// [`ClientCtx::cp_put_local`]/[`cp_delete_local`](ClientCtx::cp_delete_local)).
/// Far finer than [`SCHEMA_POLL_INTERVAL`]: paired with the cp-data
/// wake-on-propose, a write that commits+applies in a few ms now returns in ~1ms
/// instead of eating a fixed 50ms poll floor.
const CP_CONFIRM_POLL_INIT: Duration = Duration::from_micros(200);
/// Cap for the CP-confirm poll's exponential back-off: a fast write returns after a
/// sub-ms poll, but a slow/contended write backs off to this ceiling rather than
/// busy-spinning the CPU while it waits.
const CP_CONFIRM_POLL_MAX: Duration = Duration::from_millis(5);

impl ClientCtx {
    /// Kick off a split of `tablet` at `split_key` — either the **copy-based**
    /// workflow (ADR 0050: propose `MetaCommand::BeginSplit` — parent to
    /// `Splitting`, still fully serving, two `Building` children minted at
    /// **placement-chosen final homes**, fork F5, [`split_child_placement`])
    /// or the **in-place** workflow (ADR 0058 Train 2: propose
    /// `MetaCommand::BeginSplitInPlace` — parent to `Splitting`, no tablet-map
    /// rows minted, the intent recorded directly on the parent for the CP
    /// data plane's own host reconciler to drive), selected by
    /// [`ClientCtx::split_mode`] — the ONE branch point between the two;
    /// everything else on this call (the idempotent already-`Splitting`
    /// handling, the confirm loop, the child-id allocation, F11 token
    /// alignment) is shared verbatim. Confirms by observing the parent's own
    /// state become `Splitting` (state-based, replacing the old zero-copy
    /// epoch-advance confirm: a rebalance's `CasTabletReplicas` also bumps
    /// the epoch, so an epoch advance alone proves nothing about a split;
    /// observing the state does, and on a stray epoch bump the loop re-arms
    /// its CAS instead of mis-reporting).
    ///
    /// **Asynchronous by design**: success means *the split workflow
    /// started* — a copy-based split's own driver (ADR 0050 stages 2–4)
    /// seeds the children and performs the freeze/cutover; an in-place
    /// split's fork happens entirely inside the CP data plane's own Raft
    /// apply (ADR 0058 Stage 3) and its cutover is driven by
    /// `index_drain.rs`'s `inplace_split_driver_tick`. This call never waits
    /// for either. Calling on a tablet already `Splitting` returns success
    /// immediately ("already in flight" — the caller's intent is
    /// accomplished-in-progress, and kickoff is idempotent) **regardless of
    /// which workflow is running** — a stale-configured caller can never
    /// re-trigger a split that already started under the other mode.
    ///
    /// Routed to the control leader (relayable, [`is_relayable_command`]), so
    /// this works from any node the client happens to be connected to.
    #[tracing::instrument(
        name = "split_tablet",
        skip(self, split_key),
        fields(tablet = tablet.0, new_id = tracing::field::Empty)
    )]
    async fn trigger_split(&self, tablet: TabletId, split_key: Vec<u8>) -> ClientResponse {
        // `effective_metadata()`, not `self.control.metadata_cached()`
        // directly (ADR 0035 PR5 staleness-audit fix): unlike a plain stale
        // read racing a *concurrent* epoch bump — which the CAS below catches
        // cleanly, since `expected_epoch` would just fail to match at apply
        // time — `metadata_cached()` is *permanently* empty on a
        // control-plane-follower-less growth node (ADR 0030), so the
        // `tablets.get(&tablet)` lookup below would unconditionally miss and
        // this would always return "no such tablet" before ever proposing
        // anything, on every call, regardless of whether the tablet actually
        // exists on the real cluster. The CAS only protects against
        // staleness *after* a read succeeds; it can't rescue a read that
        // never has anything to see.
        let mut initial_epoch = match self.effective_metadata().tablets.get(&tablet) {
            None => return ClientResponse::Error("no such tablet".into()),
            Some(t) if t.state == TabletState::Splitting => {
                // Already mid-workflow: kickoff is idempotent.
                return ClientResponse::PutOk;
            }
            Some(t) if t.state == TabletState::Building => {
                return ClientResponse::Error(
                    "tablet is a Building split child - not splittable".into(),
                );
            }
            Some(t) => t.epoch,
        };
        let deadline = tokio::time::Instant::now() + SCHEMA_COMMIT_TIMEOUT;
        let mut next_propose_at = tokio::time::Instant::now();
        loop {
            // Confirmed: the parent's own STATE became `Splitting` — the one
            // transition only a committed `BeginSplit` of this exact tablet
            // performs (ours, or a racing proposer's that landed first —
            // harmless: "this tablet's split workflow is running" is what
            // the caller wanted either way). Deliberately state-based, not
            // the old epoch-advance confirm: a rebalance's
            // `CasTabletReplicas` also bumps a tablet's epoch, so an epoch
            // advance alone can't distinguish "my split landed" from "an
            // unrelated placement move landed"; a stray epoch bump instead
            // RE-ARMS the CAS below so the next propose attempt carries the
            // fresh epoch rather than being rejected forever. (The old
            // confirm's own hazard — two proposers computing one `new_id`
            // from equally-stale reads — still shapes the id choice below:
            // child ids are recomputed fresh from the allocator on every
            // attempt, never once up front.)
            let meta = self.effective_metadata();
            match meta.tablets.get(&tablet) {
                None => return ClientResponse::Error("no such tablet".into()),
                Some(t) if t.state == TabletState::Splitting => return ClientResponse::PutOk,
                Some(t) if t.epoch != initial_epoch => {
                    // An unrelated epoch bump (rebalance/repair CAS): re-arm.
                    initial_epoch = t.epoch;
                }
                Some(_) => {}
            }
            if tokio::time::Instant::now() >= deadline {
                return ClientResponse::Error("split did not begin in time".into());
            }
            if tokio::time::Instant::now() >= next_propose_at {
                // Child ids come from the **monotonic allocator**
                // (`next_free_tablet_id`, ADR 0023 — the same allocator
                // provisioning uses), *not* `max(existing ids) + 1`, which
                // could re-mint a freed id after a `DropTableTablets`.
                // Recomputed fresh on **every** propose attempt (not once
                // up front) — the collision-race fix inherited from the old
                // confirm's rewrite: a later attempt, once this node's own
                // metadata has caught up, sees the allocator floor moved
                // past whatever else was created meanwhile and mints
                // genuinely free ids instead of repeating doomed ones.
                let left_id = meta.next_free_tablet_id();
                let right_id = TabletId(left_id.0 + 1);
                // F11 (ADR 0042 §14, Fork D): this is the ONE choke point
                // every split proposer funnels through — `auto_split_loop`,
                // `POST /admin/tablet/split` (`admin::action_split`), and
                // `ClientRequest::SplitTablet`'s handler all call this
                // method and nothing else, so rounding here (rather than in
                // each caller) structurally can't be forgotten by a future
                // one. See `align_split_key`'s own doc for the rounding +
                // single-token-skip rule (Fork E). `tablet`'s range cannot
                // have changed since `initial_epoch` was captured (the loop
                // only reaches here while the epoch check above still
                // matches), so recomputing this every attempt is
                // equivalent to computing it once — just simpler to read
                // alongside the fresh `new_id` above.
                let (aligned_key, viable) = align_split_key(&meta, tablet, split_key.clone());
                if !viable {
                    self.control
                        .metrics()
                        .incr(Metric::StreamSplitSingleTokenSkipped);
                    return ClientResponse::Error(SPLIT_KEY_NOT_TOKEN_VIABLE.into());
                }
                tracing::Span::current().record("new_id", left_id.0);
                // Fork F5: children are minted at placement-chosen final
                // homes — the one data movement of a copy-based split is the
                // build itself, so the mint must pick the real destinations.
                let children_replicas = match split_child_placement(&meta, tablet) {
                    Ok(sets) => sets,
                    Err(e) => return ClientResponse::Error(e),
                };
                let [left_replicas, right_replicas] = children_replicas;
                // ADR 0058 Train 2 rung 3 residue: `self.split_mode` is the
                // ONE branch point between the two workflows — both
                // commands share the identical `{parent, expected_epoch,
                // split_key, children}` shape (`BeginSplitInPlace`'s own
                // doc), the idempotent already-`Splitting` handling and the
                // confirm loop above are unchanged either way, and neither
                // `auto_split_loop` nor any other caller of `trigger_split`
                // needs to know which one ran.
                let cmd = match self.split_mode {
                    SplitMode::Copy => MetaCommand::BeginSplit {
                        parent: tablet,
                        expected_epoch: initial_epoch,
                        split_key: aligned_key,
                        children: [(left_id, left_replicas), (right_id, right_replicas)],
                    },
                    SplitMode::InPlace => MetaCommand::BeginSplitInPlace {
                        parent: tablet,
                        expected_epoch: initial_epoch,
                        split_key: aligned_key,
                        children: [(left_id, left_replicas), (right_id, right_replicas)],
                    },
                };
                let sent = self.propose_schema(&cmd).await;
                next_propose_at = tokio::time::Instant::now()
                    + if sent {
                        SCHEMA_PROPOSE_PATIENCE
                    } else {
                        Duration::ZERO
                    };
            }
            tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// **Registration compare-and-swap** (ADR 0040 Decision C): propose
    /// `MetaCommand::RegisterNode` on the control-plane leader (locally if
    /// we are it, else relayed — [`is_relayable_command`] allows this
    /// command) and wait for the claim to commit + replicate here,
    /// structurally identical to [`trigger_split`](Self::trigger_split) —
    /// propose, then poll for the exact effect. Primarily polls
    /// [`metadata_fresh`](Self::metadata_fresh) (a genuine read-your-writes
    /// round trip on `Remote`) rather than `effective_metadata()`/
    /// `metadata_cached()`: a wrong "collision" verdict here has real,
    /// structural consequences for the caller (a minted id re-mints and
    /// retries; a proposed id fails loudly) that a possibly-stale cached
    /// read could get wrong.
    ///
    /// **Falls back to [`effective_metadata`](Self::effective_metadata)
    /// when `metadata_fresh()` hasn't (yet) confirmed anything** (root-cause
    /// fix for a decommission-vs-self-registration race, see
    /// `docs/engineering-lessons.md`): `metadata_fresh()`'s own doc already
    /// documents that a growth/permanently-non-voting node's local
    /// `RaftNode` "stays exactly as stuck" as it always was — its own local
    /// Raft log never independently advances, by ADR 0030 design, so this
    /// confirmation could **never** succeed for exactly the shape of caller
    /// this function itself names as its primary one: `spawn_common_tail`'s
    /// one-shot self-registration, which runs on *every* node shape,
    /// including a growth node. Without the fallback, that self-registration
    /// silently burns the *entire* `SCHEMA_COMMIT_TIMEOUT` re-proposing an
    /// already-successful, already-committed `RegisterNode` on every single
    /// join (never observing its own success), and if an operator drains +
    /// removes that same node while this futile retry loop is still live,
    /// the stale re-propose can land *after* `RemoveMember` clears
    /// `node_addrs`/`members` — indistinguishable, at apply time, from a
    /// genuinely fresh claim (`MetaCommand::RegisterNode`'s own apply arm
    /// has no notion of "this identity was just decommissioned") — silently
    /// resurrecting the just-removed node as a fresh `Down` member, which a
    /// live heartbeat then promotes straight back to `Active`. The fallback
    /// only ever *widens* when this converges (never narrows: `metadata_
    /// fresh()` is still tried first, unchanged, so a genuine voter — for
    /// which the two reads coincide, no mirror overlay ever being active —
    /// sees no behavior change at all) — it makes a growth node's own
    /// self-registration observe its own already-committed success
    /// immediately (one `SCHEMA_POLL_INTERVAL` tick) instead of blindly
    /// re-proposing for a full 10s, closing the race window this caused.
    /// The other caller, `admin_add_control_member`, only ever runs from a
    /// genuine control-group leader — the fallback is inert there too.
    ///
    /// Returns [`RegisterOutcome::Registered`] once `node_addrs[node]`
    /// equals exactly the `addrs` just proposed (whether from this call's
    /// own `Applied`, an idempotent `NoOp` replay of an identical prior
    /// claim, or a concurrent identical registration that landed first —
    /// all indistinguishable on purpose, since only the observable state
    /// matters); [`RegisterOutcome::Collision`] once `node_addrs[node]` is
    /// visibly a **different** entry — a durable fact, not a timing fluke,
    /// so a caller never needs to poll further once it sees this.
    pub(crate) async fn register_node(
        &self,
        node: NodeId,
        addrs: NodeAddrs,
        labels: BTreeMap<String, String>,
    ) -> Result<RegisterOutcome, String> {
        let cmd = MetaCommand::RegisterNode {
            node: node.clone(),
            addrs: addrs.clone(),
            labels,
        };
        match self
            .propose_and_await(cmd, SCHEMA_COMMIT_TIMEOUT, || async {
                if let Some(outcome) = Self::register_outcome_from(
                    &self.metadata_fresh().await.node_addrs,
                    &node,
                    &addrs,
                ) {
                    return Some(outcome);
                }
                Self::register_outcome_from(&self.effective_metadata().node_addrs, &node, &addrs)
            })
            .await
        {
            Ok(outcome) => Ok(outcome),
            Err(()) => Err(format!(
                "node registration for {node} did not commit within {}s \
                 (no control-plane leader reachable?)",
                SCHEMA_COMMIT_TIMEOUT.as_secs()
            )),
        }
    }

    /// Shared verdict for [`register_node`](Self::register_node)'s two reads
    /// (`metadata_fresh()` then, on `None`, the `effective_metadata()`
    /// fallback): `node_addrs`'s entry for `node`, if any, exactly matches
    /// `addrs` (`Registered`), is visibly something else (`Collision`), or
    /// is absent (`None`, not yet observable from *this* source — the caller
    /// tries the next one, or waits for the next poll tick).
    fn register_outcome_from(
        node_addrs: &BTreeMap<NodeId, NodeAddrs>,
        node: &NodeId,
        addrs: &NodeAddrs,
    ) -> Option<RegisterOutcome> {
        match node_addrs.get(node) {
            Some(existing) if existing == addrs => Some(RegisterOutcome::Registered),
            Some(_) => Some(RegisterOutcome::Collision),
            None => None,
        }
    }

    /// Drop `table` **and garbage-collect its data** (ADR 0024), cascading to
    /// every GSI's hidden index table (ADR 0041 §5): remove the schema from the
    /// replicated catalog, then remove every affected table's tablets from the
    /// replicated tablet map — the trigger each hosting node's per-node
    /// tablet-host reconciler (ADR 0031 PR4) converges on by stopping its
    /// local group and deleting its engine + WAL files. This is the real
    /// `DeleteTable` sink (the DynamoDB edge + the admin
    /// dashboard); [`drop_table_schema`](Self::drop_table_schema) alone remains
    /// the schema-only primitive (the admin panel's schema-only drop).
    /// Returns once the schema and every tablet (base **and** hidden index)
    /// have left this node's replicated metadata; the per-node file
    /// reclamation continues asynchronously on every replica.
    ///
    /// **Cascade order is load-bearing for convergence under a crash-and-retry
    /// (ADR 0041 §5's as-built note)**:
    ///
    /// 1. **Enumerate the table's GSIs and drop each hidden table's tablets
    ///    first**, while the definitions are still enumerable — a base
    ///    table's LSIs need no separate step (colocated in the base table's
    ///    own tablets, reclaimed by step 3's `erase_scope`, which walks every
    ///    row kind). The read is [`metadata_fresh`](Self::metadata_fresh), not
    ///    a cached/mirrored view: this is a **permanent** decision (once step
    ///    2 removes the schema, the defs are gone for good), so it must not
    ///    read stale. A crash here leaves the base schema and its defs
    ///    intact, so a retry re-enumerates and finishes any hidden table this
    ///    attempt didn't reach.
    /// 2. **Drop the base schema** (which deletes the GSI/LSI *definitions*
    ///    with it). A crash here leaves a state where step 1's hidden-table
    ///    drops already landed but the base tablets have not — a retry's
    ///    step 1 finds no GSIs left to enumerate (already gone) and proceeds
    ///    straight to step 3.
    /// 3. **Drop the base table's own tablets** (base rows, colocated LSI
    ///    rows, the change log, and footprints — all four `StorageScope`
    ///    kinds sharing one tablet group, reclaimed together by
    ///    `CpGroup::erase_scope` iterating `kind_scopes`). A crash here
    ///    leaves the schema gone but the base tablets present — a retry's
    ///    steps 1/2 are no-ops (idempotent) and it finishes step 3.
    ///
    /// **Belt-and-suspenders second sweep**: the GSI drain provisions a
    /// hidden table's first tablet *lazily*, and can do so **concurrently**
    /// with this drop (a change record drained mid-drop, racing step 1's
    /// enumeration). After step 3, sweep the tablet map itself — not the
    /// now-gone index definitions — for any tablet named `<table>$<index>`
    /// ([`animus_dynamo::split_index_table_name`]) and drop those too. This
    /// is keyed on the tablet map, so it also cleans up any orphan left by a
    /// **pre-fix** drop that never cascaded at all. `drain_tablet`'s own
    /// provisioning and `reconcile_partition`'s writes race this drop
    /// harmlessly — both error paths are logged-and-swallowed by
    /// `change_consumer_loop` (best-effort convergence; the next tick just
    /// retries), and once this table's groups leave `hosted_groups()` (the
    /// reconciler's `Reclaim` teardown), the drain simply stops sweeping
    /// them.
    pub(crate) async fn drop_table(&self, table: String) -> Result<(), String> {
        let indexes = self.metadata_fresh().await.table_indexes(&table).to_vec();
        for idx in indexes
            .iter()
            .filter(|idx| idx.kind == animus_control::schema::IndexKind::Global)
        {
            let index_table = animus_dynamo::index_table_name(&table, &idx.name);
            self.drop_table_tablets(index_table).await?;
        }

        self.drop_table_schema(table.clone()).await?;
        self.drop_table_tablets(table.clone()).await?;

        let orphans: BTreeSet<String> = self
            .effective_metadata()
            .tablets
            .values()
            .filter_map(|t| t.table.as_deref())
            .filter(|name| {
                animus_dynamo::split_index_table_name(name).is_some_and(|(base, _)| base == table)
            })
            .map(str::to_owned)
            .collect();
        for orphan in orphans {
            self.drop_table_tablets(orphan).await?;
        }
        Ok(())
    }

    /// Propose `MetaCommand::DropTableTablets` for `table` and wait until every
    /// tablet scoped to it has left this node's replicated metadata (ADR 0024).
    /// Shared by [`drop_table`](Self::drop_table)'s base-table drop, its
    /// GSI-hidden-table cascade (ADR 0041 §5), and `dynamo.rs`'s single-index
    /// drop cascade (ADR 0045 §5, `drop_index`) — same command, same
    /// commit-wait discipline in all three. `pub(crate)` (not module-private)
    /// for exactly that last caller, a sibling module. `table` need not have
    /// a schema entry: a hidden index table never has one, and
    /// `DropTableTablets`'s apply is keyed purely on the tablet map
    /// (`tablets_for_table`), not the schema catalog.
    pub(crate) async fn drop_table_tablets(&self, table: String) -> Result<(), String> {
        let command = MetaCommand::DropTableTablets {
            table: table.clone(),
        };
        // Always propose at least once — never gate on "no tablets in *this*
        // node's metadata": a lagging replica may not have applied the tablet's
        // creation yet, so local absence cannot prove there is nothing to drop
        // (and `propose_and_await` returns on its first poll in that state).
        // The command is idempotent (`NoOp`) on the leader when there truly is
        // nothing. (A *schema'd* base table is safe either way — the
        // schema-drop wait already forced this replica past the tablet's
        // creation in the log — but a plain-client table, or a hidden index
        // table with no schema wait at all, skips that forcing.)
        self.propose_schema(&command).await;
        // `effective_metadata()`, not `self.control.metadata_cached()`
        // directly (ADR 0035 PR5 staleness-audit fix): the latter is
        // permanently empty on a control-plane-follower-less growth node
        // (ADR 0030), so `tablets_for_table(&table).next().is_none()` was
        // unconditionally `true` there — reporting a false success on the
        // very first poll regardless of whether the drop actually committed,
        // not merely timing out. `effective_metadata()`'s mirror is the
        // right contract here (this poll confirms *absence*, which the
        // cache-tolerant view proves just as soundly as a fresh one once it
        // has synced at all).
        self.propose_and_await(command, SCHEMA_COMMIT_TIMEOUT, || async {
            self.effective_metadata()
                .tablets_for_table(&table)
                .next()
                .is_none()
                .then_some(())
        })
        .await
        .map_err(|()| {
            format!(
                "DROP TABLE `{table}`: tablet GC did not commit within {}s (no control-plane leader reachable?)",
                SCHEMA_COMMIT_TIMEOUT.as_secs()
            )
        })
    }

    /// Propose `MetaCommand::DropTableSchema` and wait for the table to disappear
    /// from the replicated catalog (ADR 0013). Idempotent: dropping an absent
    /// table returns `Ok(())` immediately. Schema-only: does
    /// **not** touch the table's tablets/data (the admin panel's schema-only
    /// drop uses this); a real drop goes through [`drop_table`](Self::drop_table).
    pub(crate) async fn drop_table_schema(&self, table: String) -> Result<(), String> {
        // Fresh, not a cache-tolerant read (ADR 0035 PR1): this is a
        // commit-wait poll, which must observe its own just-proposed
        // command landing in the authoritative state.
        if !self.metadata_fresh().await.has_table_schema(&table) {
            return Ok(());
        }
        let command = MetaCommand::DropTableSchema {
            table: table.clone(),
        };
        self.propose_and_await(command, SCHEMA_COMMIT_TIMEOUT, || async {
            (!self.metadata_fresh().await.has_table_schema(&table)).then_some(())
        })
        .await
        .map_err(|()| {
            format!(
                "DROP TABLE `{table}` did not commit within {}s (no control-plane leader reachable?)",
                SCHEMA_COMMIT_TIMEOUT.as_secs()
            )
        })
    }

    /// Propose `command` on the current leader and poll `committed` until it
    /// reports the change visible in this node's replicated metadata (or time
    /// out). Resubmits the proposal on a leader change or transient failure, but
    /// **not** on every poll tick while a prior attempt is still believed
    /// in-flight — see [`propose_schema`](Self::propose_schema)'s doc; that
    /// backs off for [`SCHEMA_PROPOSE_PATIENCE`] after a proposal we believe
    /// reached a leader's log, only resubmitting immediately when we know it
    /// wasn't sent anywhere. Returns the committed value `committed` observed,
    /// or `Err(())` on timeout.
    ///
    /// `committed` is an **async** closure (ADR 0035 PR4 — [`metadata_fresh`]
    /// is now a genuine network round trip on a `Remote` handle, so every
    /// caller's commit-wait predicate must be able to `.await` it; every
    /// existing call site's predicate — sync in substance for a `Local`
    /// handle — just gained an `async` wrapper with no behavior change).
    async fn propose_and_await<T, Fut>(
        &self,
        command: MetaCommand,
        timeout: Duration,
        committed: impl Fn() -> Fut,
    ) -> Result<T, ()>
    where
        Fut: std::future::Future<Output = Option<T>>,
    {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut next_propose_at = tokio::time::Instant::now();
        loop {
            if let Some(value) = committed().await {
                return Ok(value);
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err(());
            }
            if now >= next_propose_at {
                let sent = self.propose_schema(&command).await;
                next_propose_at = now
                    + if sent {
                        SCHEMA_PROPOSE_PATIENCE
                    } else {
                        Duration::ZERO
                    };
            }
            tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// Force one seal pass of `tablet`'s own hot tail (ADR 0042/0043's F12-b
    /// disable-triggered final seal), wherever that tablet's leader actually
    /// runs — the caller (`dynamo.rs`'s disable flow) may be connected to
    /// any node, not necessarily one that leads any of the table's tablets.
    ///
    /// Forwards via [`forward_to_tablet_leader`](Self::forward_to_tablet_leader)
    /// (the hint-chasing shape) — an earlier revision relayed once and
    /// re-resolved `resolve_cp_route` from scratch instead, which never
    /// converges when this node hosts no replica of `tablet` (see the
    /// helper's doc); the outer loop here still re-resolves between chases
    /// as its converged-or-timeout backstop.
    pub(crate) async fn force_seal_tablet(&self, tablet: TabletId) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + SCHEMA_COMMIT_TIMEOUT;
        loop {
            match self.resolve_cp_route(tablet) {
                Some(CpRoute::Local(leader)) => {
                    let table = self
                        .effective_metadata()
                        .tablets
                        .get(&tablet)
                        .and_then(|t| t.table.clone());
                    let Some(table) = table else {
                        return Err("no such tablet".into());
                    };
                    return index_drain::seal_now(self, &table, tablet, &leader)
                        .await
                        .map(|_| ());
                }
                Some(CpRoute::Forward(addr)) => {
                    let request = ClientRequest::ForceSeal { tablet: tablet.0 };
                    match self
                        .forward_to_tablet_leader(Some(tablet), addr, request)
                        .await
                    {
                        ClientResponse::PutOk => return Ok(()),
                        ClientResponse::Error(e) if tokio::time::Instant::now() >= deadline => {
                            return Err(e);
                        }
                        ClientResponse::Error(_) => {} // retry below
                        other => {
                            return Err(format!(
                                "unexpected reply to forwarded force-seal: {other:?}"
                            ));
                        }
                    }
                }
                Some(CpRoute::None) | None => {} // not settled yet, retry
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("force-seal did not reach a tablet leader in time".into());
            }
            tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// One tablet's own share of growth PR3's manual trigger (`POST
    /// /admin/stream/grow`, [`grow_stream`](Self::grow_stream)'s per-tablet
    /// call): wherever `tablet`'s own CP group leader actually runs,
    /// materialize its live pairs and split at their byte-weighted median
    /// ([`median_split_key`]) via [`trigger_split`](Self::trigger_split) —
    /// which independently applies F11's token-rounding and Fork E's
    /// single-token skip, exactly as every other split proposer does.
    /// Returns the tablet's own [`ClientResponse`] verbatim: `PutOk` for a
    /// genuine split, or an `Error` naming [`STREAM_GROW_NO_SPLIT_POINT`]/
    /// [`SPLIT_KEY_NOT_TOKEN_VIABLE`] for an expected skip (or any other
    /// real error) — the caller (`admin::action_stream_grow`) classifies
    /// these, never treating one tablet's skip as a failure of the whole
    /// multi-tablet action. Same shape as
    /// [`force_seal_tablet`](Self::force_seal_tablet) (resolve → local or
    /// forward, retry until a deadline), except a `Forward` reply is
    /// returned immediately unless it is specifically a stale "not leader
    /// here" refusal (`topology::parse_not_leader_refusal`) — every other
    /// error (including this action's own expected skips) is a terminal
    /// outcome, not a signal to keep retrying.
    pub(crate) async fn grow_stream_tablet(&self, tablet: TabletId) -> ClientResponse {
        let deadline = tokio::time::Instant::now() + SCHEMA_COMMIT_TIMEOUT;
        loop {
            match self.resolve_cp_route(tablet) {
                Some(CpRoute::Local(leader)) => {
                    return match median_split_key(&leader).await {
                        None => ClientResponse::Error(STREAM_GROW_NO_SPLIT_POINT.into()),
                        Some(split_key) => self.trigger_split(tablet, split_key).await,
                    };
                }
                Some(CpRoute::Forward(addr)) => {
                    let request = ClientRequest::TriggerAutoSplit { tablet: tablet.0 };
                    match self
                        .forward_to_tablet_leader(Some(tablet), addr, request)
                        .await
                    {
                        ClientResponse::Error(e)
                            if topology::parse_not_leader_refusal(&e).is_some() => {} // chase exhausted mid-election, retry below
                        other => return other,
                    }
                }
                Some(CpRoute::None) | None => {} // not settled yet, retry
            }
            if tokio::time::Instant::now() >= deadline {
                return ClientResponse::Error(
                    "stream grow: did not reach this tablet's leader in time".into(),
                );
            }
            tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// Growth PR3 (ADR 0042 §14): split EVERY tablet of streamed `table` at
    /// its own byte-weighted median, in one action (`POST
    /// /admin/stream/grow`) — each child mints exactly one
    /// `ParentShardId`, so the table's shard count doubles (minus any
    /// tablet [`grow_stream_tablet`](Self::grow_stream_tablet) skips: Fork
    /// E's single-token limit, or an empty/singleton tablet). `Err` only
    /// for a request-shaped problem (the table has no stream, or no
    /// tablets at all yet); a per-tablet skip/error is reported inside the
    /// returned vector, never escalated into the whole call failing — the
    /// caller (`admin::action_stream_grow`) classifies each entry.
    pub(crate) async fn grow_stream(
        &self,
        table: &str,
    ) -> Result<Vec<(TabletId, ClientResponse)>, String> {
        let meta = self.effective_metadata();
        if meta.table_stream(table).is_none() {
            return Err(format!("table `{table}` has no stream enabled"));
        }
        let tablets: Vec<(TabletId, TabletState)> = meta
            .tablets_for_table(table)
            .map(|(id, t)| (*id, t.state))
            .collect();
        if tablets.is_empty() {
            return Err(format!("table `{table}` has no tablets yet"));
        }
        let mut results = Vec::with_capacity(tablets.len());
        for (tablet, state) in tablets {
            // A mid-split tablet is classified up front (`STREAM_GROW_MID_
            // SPLIT`), never routed to: a `Splitting` parent's workflow is
            // already running (kicking it again is an idempotent no-op that
            // the summary would miscount as a fresh split), and a `Building`
            // child refuses splits until activation anyway.
            let response = match state {
                TabletState::Active => self.grow_stream_tablet(tablet).await,
                TabletState::Splitting | TabletState::Building => {
                    ClientResponse::Error(STREAM_GROW_MID_SPLIT.into())
                }
            };
            results.push((tablet, response));
        }
        Ok(results)
    }

    /// Delete `index`'s own backfill cursor row (ADR 0045 §5 step 3) on
    /// **every** tablet currently scoped to `table`, wherever each one's
    /// own leader actually runs — the table-wide sibling of
    /// [`clear_backfill_cursor_tablet`](Self::clear_backfill_cursor_tablet),
    /// called once per tablet since each tablet is its own Raft group with
    /// its own cursor row. See `dynamo.rs::drop_index`'s own doc for why
    /// this step exists (a stale cursor row would otherwise silently
    /// poison a later same-named `CreateTableIndex`'s fresh backfill) and
    /// exactly when it runs.
    pub(crate) async fn clear_backfill_cursor_for_table(
        &self,
        table: &str,
        index: &str,
    ) -> Result<(), String> {
        let tablets: Vec<TabletId> = self
            .effective_metadata()
            .tablets_for_table(table)
            .map(|(&id, _)| id)
            .collect();
        for tablet in tablets {
            self.clear_backfill_cursor_tablet(tablet, index).await?;
        }
        Ok(())
    }

    /// Delete `index`'s own backfill cursor row on one `tablet`, wherever
    /// its leader actually runs — mirrors
    /// [`force_seal_tablet`](Self::force_seal_tablet)'s per-tablet
    /// forward/retry shape exactly (a hint-chasing
    /// [`forward_to_tablet_leader`](Self::forward_to_tablet_leader) per
    /// attempt, re-resolving [`resolve_cp_route`](Self::resolve_cp_route)
    /// between chases as the converged-or-timeout backstop).
    async fn clear_backfill_cursor_tablet(
        &self,
        tablet: TabletId,
        index: &str,
    ) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + SCHEMA_COMMIT_TIMEOUT;
        loop {
            match self.resolve_cp_route(tablet) {
                Some(CpRoute::Local(leader)) => {
                    return index_drain::clear_backfill_cursor(&leader, index).await;
                }
                Some(CpRoute::Forward(addr)) => {
                    let request = ClientRequest::ClearBackfillCursor {
                        tablet: tablet.0,
                        index: index.to_owned(),
                    };
                    match self
                        .forward_to_tablet_leader(Some(tablet), addr, request)
                        .await
                    {
                        ClientResponse::PutOk => return Ok(()),
                        ClientResponse::Error(e) if tokio::time::Instant::now() >= deadline => {
                            return Err(e);
                        }
                        ClientResponse::Error(_) => {} // retry below
                        other => {
                            return Err(format!(
                                "unexpected reply to forwarded cursor-clear: {other:?}"
                            ));
                        }
                    }
                }
                Some(CpRoute::None) | None => {} // not settled yet, retry
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("backfill-cursor clear did not reach a tablet leader in time".into());
            }
            tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// The hot_read scope-transition latch (ADR 0044 phase-1 PR4, narrowing
    /// the ADR 0043 `hot_read` residual — see [[split-seal-duplication-bug]]
    /// and `docs/adr/0043-*.md`'s amendment on the #220 fix): refuses a
    /// hot-read retryably instead of ever risking a stale-wide answer,
    /// whenever this node's own **live** `scope_range()` for `tablet` (the
    /// exact field `animus_cp_data::host::Reconciler::tick` mutates via
    /// `narrow_scope` — see that module's doc) is currently **wider** than
    /// the tablet's range per `meta`.
    ///
    /// **`meta` must come from [`metadata_fresh`](Self::metadata_fresh),
    /// never [`effective_metadata`](Self::effective_metadata)/
    /// `metadata_cached()`.** `index_drain::hot_read`'s own pre-existing
    /// `in_declared_range` filter (2026-08-15) already checks a record's key
    /// against a caller-supplied snapshot, but every prior call site sourced
    /// that snapshot from the possibly-stale `effective_metadata()` mirror.
    /// Reading the group's own live scope needs no new shared state at all —
    /// it is always exactly current the instant the reconciler narrows it
    /// (`RaftKvNode::narrow_scope` sets it synchronously, no propagation
    /// delay) — so cross-checking it against a **freshly fetched** declared
    /// range closes two of the three staleness axes `in_declared_range`
    /// alone left open: (a) a data-only/growth node's ADR 0030 mirror
    /// lagging a `SplitTablet` commit by its own refresh interval, and (b)
    /// this node's own reconciler having observed the split in its cached
    /// `Metadata` but not yet having ticked `narrow_scope` locally.
    ///
    /// **This narrows, but does not fully close, the residual — the same
    /// layer-2 structure the #220 investigation found on the write side.**
    /// For a `ControlHandle::Local` node (every combined node — the common
    /// case), `metadata_fresh()` resolves to `raft.metadata()`, the ADR 0038
    /// published cache a **local, asynchronous control apply task**
    /// maintains, not the control Raft's own commit index directly. In the
    /// sub-window between a `SplitTablet` actually committing and this
    /// node's own control apply task catching its published cache up to it,
    /// `meta` and the live scope are stale **together**: the declared range
    /// still shows the pre-split width, so this check passes and a hot-read
    /// can still observe the fabrication class ADR 0043 describes. Full
    /// closure of this sub-window would need a per-read control-leader
    /// Fetch up to `limit` of `tablet`'s own open-shard hot records with
    /// packed HLC strictly greater than `from_position` (ADR 0042 §7/§8,
    /// PR6's `GetRecords` open-shard path) — the internal `ClientRequest::
    /// StreamHotRead` RPC, forwarded to whichever node currently leads
    /// `tablet`. Mirrors [`force_seal_tablet`](Self::force_seal_tablet)'s
    /// retry shape exactly (there is no client key to derive routing from,
    /// so each attempt is a hint-chasing
    /// [`forward_to_tablet_leader`](Self::forward_to_tablet_leader), with a
    /// fresh [`resolve_cp_route`](Self::resolve_cp_route) between chases) —
    /// acceptable for a `GetRecords` poll, which already
    /// tolerates "not there yet, poll again" as part of the stream's own
    /// eventually consistent contract.
    pub(crate) async fn read_stream_hot_records(
        &self,
        tablet: TabletId,
        from_position: u64,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let deadline = tokio::time::Instant::now() + SCHEMA_COMMIT_TIMEOUT;
        loop {
            match self.resolve_cp_route(tablet) {
                Some(CpRoute::Local(leader)) => {
                    // The ADR 0048 scope-transition latch died with the
                    // mutable scope (ADR 0050 rung 7) — immutable ranges
                    // leave no transition window to latch.
                    return Ok(index_drain::hot_read(&leader, from_position, limit)
                        .await
                        .into_iter()
                        .map(|(key, _, value)| (key, value))
                        .collect());
                }
                Some(CpRoute::Forward(addr)) => {
                    let request = ClientRequest::StreamHotRead {
                        tablet: tablet.0,
                        from_position,
                        limit,
                    };
                    match self
                        .forward_to_tablet_leader(Some(tablet), addr, request)
                        .await
                    {
                        ClientResponse::Pairs(pairs) => return Ok(pairs),
                        ClientResponse::Error(e) if tokio::time::Instant::now() >= deadline => {
                            return Err(e);
                        }
                        ClientResponse::Error(_) => {} // retry below
                        other => {
                            return Err(format!(
                                "unexpected reply to forwarded stream hot read: {other:?}"
                            ));
                        }
                    }
                }
                Some(CpRoute::None) | None => {} // not settled yet, retry
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("stream hot read did not reach a tablet leader in time".into());
            }
            tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }
}

/// Bind an `n`-node cluster on `ip` with ephemeral ports and the conventional
/// ids (node `i`, ADR 0040 PR1), each under `dir/node-i`.
///
/// # Errors
/// Propagates any bind failure.
pub async fn bind_cluster(
    n: usize,
    ip: std::net::IpAddr,
    dir: impl Into<PathBuf>,
) -> std::io::Result<Vec<BoundNode>> {
    let dir = dir.into();
    let mut nodes = Vec::with_capacity(n);
    for i in 0..n {
        let addr = || SocketAddr::new(ip, 0);
        let addrs = RoleAddrs {
            id: config::node_id(i),
            role: config::NodeRole::Both,
            internal: addr(),
            client: addr(),
            dynamo: addr(),
            admin: addr(),
            intra: addr(),
            console: addr(),
        };
        let node = Node::bind(config::node_id(i), addrs, dir.join(format!("node-{i}"))).await?;
        nodes.push(node);
    }
    Ok(nodes)
}

/// Start a cluster previously bound with [`bind_cluster`], each node's CP group
/// backed by the durable on-disk [`LsmEngine`].
///
/// # Errors
/// Propagates a failure to open any node's CP group engine.
pub async fn start_cluster(bound: Vec<BoundNode>) -> std::io::Result<Vec<Node>> {
    start_cluster_with(bound, StorageBackend::default()).await
}

/// Like [`start_cluster`], but selects the CP groups' storage `backend`.
///
/// # Errors
/// Propagates a failure to open any node's CP group engine (LSM backend only).
pub async fn start_cluster_with(
    bound: Vec<BoundNode>,
    backend: StorageBackend,
) -> std::io::Result<Vec<Node>> {
    start_cluster_inner(
        bound,
        backend,
        None,
        None,
        DEFAULT_ORPHAN_SWEEP_AFTER,
        StreamSealKnobs::default(),
        SegmentStoreConfig::default(),
        DEFAULT_STREAM_RETENTION,
        None,
        Duration::ZERO,
        None,
        SplitMode::default(),
        BackupStoreConfig::default(),
    )
    .await
}

/// Like [`start_cluster`], but enables the **automatic split trigger** (Phase 2.4)
/// with the given key-count `threshold`: a CP-hosting node splits a tablet it leads
/// once it exceeds that many keys. For tests/dev that want to exercise auto-sharding
/// without the (high, size-based) production threshold.
///
/// # Errors
/// Propagates a failure to open any node's CP group engine.
pub async fn start_cluster_auto_split(
    bound: Vec<BoundNode>,
    threshold: usize,
) -> std::io::Result<Vec<Node>> {
    start_cluster_inner(
        bound,
        StorageBackend::default(),
        Some(threshold),
        None,
        DEFAULT_ORPHAN_SWEEP_AFTER,
        StreamSealKnobs::default(),
        SegmentStoreConfig::default(),
        DEFAULT_STREAM_RETENTION,
        None,
        Duration::ZERO,
        None,
        SplitMode::default(),
        BackupStoreConfig::default(),
    )
    .await
}

/// Like [`start_cluster_with`], but also enables the **automatic split trigger**
/// (Phase 2.4) when `auto_split` is `Some(threshold)` — so the chosen `backend`
/// and the trigger can be combined (e.g. `--cluster N --ephemeral --auto-split K`).
///
/// # Errors
/// Propagates a failure to open any node's CP group engine.
pub async fn start_cluster_with_auto_split(
    bound: Vec<BoundNode>,
    backend: StorageBackend,
    auto_split: Option<usize>,
) -> std::io::Result<Vec<Node>> {
    start_cluster_inner(
        bound,
        backend,
        auto_split,
        None,
        DEFAULT_ORPHAN_SWEEP_AFTER,
        StreamSealKnobs::default(),
        SegmentStoreConfig::default(),
        DEFAULT_STREAM_RETENTION,
        None,
        Duration::ZERO,
        None,
        SplitMode::default(),
        BackupStoreConfig::default(),
    )
    .await
}

/// Like [`start_cluster_with_auto_split`], but also configures the **byte**
/// auto-split threshold (ADR 0034): a CP-hosting node splits a tablet it
/// leads once **either** `auto_split_keys` or `auto_split_bytes` is exceeded
/// (each independently optional). The plain key-count-only entry points
/// above are kept as thin wrappers over this one for back-compat.
///
/// # Errors
/// Propagates a failure to open any node's CP group engine.
pub async fn start_cluster_with_auto_split_bytes(
    bound: Vec<BoundNode>,
    backend: StorageBackend,
    auto_split_keys: Option<usize>,
    auto_split_bytes: Option<u64>,
) -> std::io::Result<Vec<Node>> {
    start_cluster_inner(
        bound,
        backend,
        auto_split_keys,
        auto_split_bytes,
        DEFAULT_ORPHAN_SWEEP_AFTER,
        StreamSealKnobs::default(),
        SegmentStoreConfig::default(),
        DEFAULT_STREAM_RETENTION,
        None,
        Duration::ZERO,
        None,
        SplitMode::default(),
        BackupStoreConfig::default(),
    )
    .await
}

/// Like [`start_cluster_with_auto_split_bytes`], but also configures the
/// **orphan-member sweep** grace period (ADR 0040 PR6, `animus_control::node`'s
/// `orphan_sweep_after`) instead of [`DEFAULT_ORPHAN_SWEEP_AFTER`] —
/// `Duration::ZERO` disables the sweep entirely. The knob `--cluster`'s
/// `--orphan-sweep-after SECS` CLI flag threads through here.
///
/// # Errors
/// Propagates a failure to open any node's CP group engine.
pub async fn start_cluster_with_auto_split_bytes_and_orphan_sweep_after(
    bound: Vec<BoundNode>,
    backend: StorageBackend,
    auto_split_keys: Option<usize>,
    auto_split_bytes: Option<u64>,
    orphan_sweep_after: Duration,
) -> std::io::Result<Vec<Node>> {
    start_cluster_inner(
        bound,
        backend,
        auto_split_keys,
        auto_split_bytes,
        orphan_sweep_after,
        StreamSealKnobs::default(),
        SegmentStoreConfig::default(),
        DEFAULT_STREAM_RETENTION,
        None,
        Duration::ZERO,
        None,
        SplitMode::default(),
        BackupStoreConfig::default(),
    )
    .await
}

/// Like [`start_cluster_with_auto_split_bytes_and_orphan_sweep_after`], with
/// explicit DynamoDB Streams sealer knobs, segment-store selection, and the
/// segment-janitor's own retention grace period — see
/// [`BoundNode::start_with_streams`]'s doc for the layered-wrapper
/// rationale. `--cluster N`'s `--stream-seal-bytes`/`--stream-seal-age`/
/// `--stream-retention`/`--segment-store` CLI flags thread through here.
/// Defaults [`start_cluster_with_growth`]'s own `auto_split_change_rate` to
/// `None`.
///
/// # Errors
/// Propagates a failure to open any node's CP group engine.
#[allow(clippy::too_many_arguments)]
pub async fn start_cluster_with_streams(
    bound: Vec<BoundNode>,
    backend: StorageBackend,
    auto_split_keys: Option<usize>,
    auto_split_bytes: Option<u64>,
    orphan_sweep_after: Duration,
    stream_seal_knobs: StreamSealKnobs,
    segment_store_config: SegmentStoreConfig,
    stream_retention: Duration,
) -> std::io::Result<Vec<Node>> {
    start_cluster_inner(
        bound,
        backend,
        auto_split_keys,
        auto_split_bytes,
        orphan_sweep_after,
        stream_seal_knobs,
        segment_store_config,
        stream_retention,
        None,
        Duration::ZERO,
        None,
        SplitMode::default(),
        BackupStoreConfig::default(),
    )
    .await
}

/// Like [`start_cluster_with_streams`], with the opt-in **change-rate**
/// auto-split trigger (ADR 0042 §14, growth PR3 Fork F) — see
/// [`BoundNode::start_with_growth`]'s doc for the full design. `--cluster
/// N`'s `--auto-split-change-rate RATE` CLI flag threads through here.
///
/// # Errors
/// Propagates a failure to open any node's CP group engine.
#[allow(clippy::too_many_arguments)]
pub async fn start_cluster_with_growth(
    bound: Vec<BoundNode>,
    backend: StorageBackend,
    auto_split_keys: Option<usize>,
    auto_split_bytes: Option<u64>,
    orphan_sweep_after: Duration,
    stream_seal_knobs: StreamSealKnobs,
    segment_store_config: SegmentStoreConfig,
    stream_retention: Duration,
    auto_split_change_rate: Option<u64>,
) -> std::io::Result<Vec<Node>> {
    start_cluster_inner(
        bound,
        backend,
        auto_split_keys,
        auto_split_bytes,
        orphan_sweep_after,
        stream_seal_knobs,
        segment_store_config,
        stream_retention,
        auto_split_change_rate,
        Duration::ZERO,
        None,
        SplitMode::default(),
        BackupStoreConfig::default(),
    )
    .await
}

/// Like [`start_cluster_with_growth`], but also opts every **data-plane** CP
/// group into quiescence (ADR 0044 phase-1 PR4/PR7) with the given idle
/// threshold — `Duration::ZERO` (every other entry point above) disables it
/// entirely, zero behavior change. Test-only today (no CLI flag threads
/// through this specific wrapper yet — PR7 adds `--quiesce-after SECS` to
/// the per-process `run_node*`/`gen-config` paths); combined-mode
/// (`--cluster N`) only, mirroring every other knob in this file's layered
/// stack.
///
/// # Errors
/// Propagates a failure to open any node's CP group engine.
#[allow(clippy::too_many_arguments)]
pub async fn start_cluster_with_quiesce_after(
    bound: Vec<BoundNode>,
    backend: StorageBackend,
    auto_split_keys: Option<usize>,
    auto_split_bytes: Option<u64>,
    quiesce_after: Duration,
) -> std::io::Result<Vec<Node>> {
    start_cluster_inner(
        bound,
        backend,
        auto_split_keys,
        auto_split_bytes,
        DEFAULT_ORPHAN_SWEEP_AFTER,
        StreamSealKnobs::default(),
        SegmentStoreConfig::default(),
        DEFAULT_STREAM_RETENTION,
        None,
        quiesce_after,
        None,
        SplitMode::default(),
        BackupStoreConfig::default(),
    )
    .await
}

/// Like [`start_cluster_with_growth`], but also opts every **data-plane** CP
/// group into quiescence (ADR 0044 phase-1 PR7) with the given idle
/// threshold — `Duration::ZERO` disables it entirely, zero behavior change.
/// `--cluster N`'s `--quiesce-after SECS` CLI flag threads through here
/// (the full-combination sibling of [`start_cluster_with_quiesce_after`],
/// which predates the streams/change-rate knobs being combinable with
/// quiescence).
///
/// `dynamo_auth` (ADR 0057) is the client DynamoDB port's SigV4 credential
/// store for the whole in-process cluster — `--cluster N`'s `--dynamo-auth
/// PATH` CLI flag threads through here (a config-less dev shape, so there is
/// no `ClusterConfig::dynamo_auth` section to read instead). `None` (every
/// other wrapper above) disables auth entirely, byte-identical to
/// pre-ADR-0057 behavior.
///
/// `split_mode` (ADR 0058 Train 2 rung 3) selects which split workflow the
/// whole in-process cluster runs — `--cluster N`'s `--split-mode
/// {copy,inplace}` CLI flag threads through here; `SplitMode::Copy` (every
/// other wrapper above) is byte-for-byte the original ADR 0050 workflow.
///
/// `backup_store_config` (ADR 0059 §1) selects the whole in-process
/// cluster's backup store — `--cluster N`'s `--backup-store cluster|fs:PATH`
/// CLI flag threads through here; `BackupStoreConfig::Cluster` (every other
/// wrapper above) is the default. Plumbing only (ADR 0059 Train 1 PR②).
///
/// # Errors
/// Propagates a failure to open any node's CP group engine.
#[allow(clippy::too_many_arguments)]
pub async fn start_cluster_with_growth_and_quiesce_after(
    bound: Vec<BoundNode>,
    backend: StorageBackend,
    auto_split_keys: Option<usize>,
    auto_split_bytes: Option<u64>,
    orphan_sweep_after: Duration,
    stream_seal_knobs: StreamSealKnobs,
    segment_store_config: SegmentStoreConfig,
    stream_retention: Duration,
    auto_split_change_rate: Option<u64>,
    quiesce_after: Duration,
    dynamo_auth: Option<Arc<BTreeMap<String, String>>>,
    split_mode: SplitMode,
    backup_store_config: BackupStoreConfig,
) -> std::io::Result<Vec<Node>> {
    start_cluster_inner(
        bound,
        backend,
        auto_split_keys,
        auto_split_bytes,
        orphan_sweep_after,
        stream_seal_knobs,
        segment_store_config,
        stream_retention,
        auto_split_change_rate,
        quiesce_after,
        dynamo_auth,
        split_mode,
        backup_store_config,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn start_cluster_inner(
    bound: Vec<BoundNode>,
    backend: StorageBackend,
    auto_split_threshold: Option<usize>,
    auto_split_bytes_threshold: Option<u64>,
    orphan_sweep_after: Duration,
    stream_seal_knobs: StreamSealKnobs,
    segment_store_config: SegmentStoreConfig,
    stream_retention: Duration,
    auto_split_change_rate: Option<u64>,
    quiesce_after: Duration,
    dynamo_auth: Option<Arc<BTreeMap<String, String>>>,
    split_mode: SplitMode,
    backup_store_config: BackupStoreConfig,
) -> std::io::Result<Vec<Node>> {
    let n = bound.len();
    let control_ids: Vec<NodeId> = (0..n).map(config::node_id).collect();
    // ADR 0040 PR1: `bind_cluster` (the only producer of a `Vec<BoundNode>`
    // this function is ever called with) always assembles combined-mode
    // (`Both`-role) nodes, so every bound node's own id is a genuine
    // data-role member too (one identity per node) — read straight off each
    // `BoundNode` rather than re-deriving from `control_ids`, so this stays
    // correct even if a future caller's `bound` isn't a contiguous `0..n`
    // index range.
    let data_ids: Vec<NodeId> = bound.iter().map(|b| b.id.clone()).collect();
    let peers: BTreeMap<NodeId, SocketAddr> =
        bound.iter().flat_map(BoundNode::peer_entries).collect();
    // Cross-node routing (ADR 0017 #3b / ADR 0013): map each node's one id to
    // that node's client API address, so an op landing on a node that isn't
    // the relevant leader forwards to the leader's node — identical to the
    // per-process path (`run_node_with`). `--cluster N` gives **each node its
    // own `ClusterEdgeState`** (below), matching one-process-per-node
    // exactly: cross-node reach happens only through this real
    // forwarding/relay path, never a shared in-process registry (root
    // `CLAUDE.md`'s documented "shared edge masks per-node bugs" gotcha —
    // this removes the sharing). This is only the **static seed**:
    // `start_with` hands it to each node's own `route_sync_loop`, which keeps
    // it live thereafter by overlaying `Metadata.node_addrs[*].client` (ADR
    // 0032 PR1) — so a node grown into the cluster later is still reachable
    // from every original node.
    let client_route: BTreeMap<NodeId, SocketAddr> = bound
        .iter()
        .map(|b| (b.id.clone(), b.client_addr))
        .collect();
    // The `intra_route` sibling (ADR 0047) — identical static-seed shape,
    // sourced from each bound node's intra address instead of its client one.
    let intra_route: BTreeMap<NodeId, SocketAddr> = bound
        .iter()
        .map(|b| (b.id.clone(), b.intra_addr()))
        .collect();
    // Every node's admin address, so each node's dashboard (ADR 0021) can fan out
    // to the whole in-process cluster.
    let admin_addrs: Vec<SocketAddr> = bound.iter().map(BoundNode::admin_addr).collect();
    let mut nodes = Vec::with_capacity(n);
    for b in bound {
        let node = b
            .start_with_growth(
                peers.clone(),
                control_ids.clone(),
                data_ids.clone(),
                backend,
                // A fresh, node-local edge-state set per node — never shared
                // across the in-process cluster (see the `client_route`
                // comment above).
                ClusterEdgeState::new(),
                client_route.clone(),
                intra_route.clone(),
                auto_split_threshold,
                auto_split_bytes_threshold,
                admin_addrs.clone(),
                orphan_sweep_after,
                stream_seal_knobs,
                segment_store_config.clone(),
                stream_retention,
                auto_split_change_rate,
                quiesce_after,
                // `--cluster N` has no ttl-sweep-interval knob of its own
                // yet (mirrors `stream_retention`'s own layered-stack
                // precedent for a not-yet-CLI-exposed knob) — production
                // default; a test that needs a fast sweep uses the
                // per-process `run_node_with_ttl_sweep_interval` instead.
                ttl_reaper::DEFAULT_TTL_SWEEP_INTERVAL,
                dynamo_auth.clone(),
                split_mode,
                backup_store_config.clone(),
            )
            .await?;
        nodes.push(node);
    }
    Ok(nodes)
}

/// Bind and start a whole **split-deployment** cluster in one process
/// (`--cluster-control N --cluster-data M`): `control_n` control-only nodes
/// (`Node::bind_control`/`BoundControlNode::start_control_with`, ADR 0035
/// PR3) followed by `data_n` data-only nodes
/// (`Node::bind_data`/`BoundDataNode::start_data_with`, ADR 0035 PR4) — no
/// combined-mode node anywhere. The in-process, single-command counterpart of
/// [`ClusterConfig::generate_split`] + `animusd control`/`animusd data`
/// (real separate processes): same id/index convention (control-role
/// indexes `0..control_n`, data-role indexes `control_n..control_n+data_n`,
/// `config::node_id` applied straight to those indexes) and same per-node
/// `dir/node-{index}` subdirectory layout as
/// [`bind_cluster`]/[`start_cluster_inner`], just role-split instead of every
/// node being `Both`.
///
/// Every node gets its **own** [`ClusterEdgeState`] (ADR 0031 PR2 doctrine —
/// never shared across the in-process cluster) and reaches every other node
/// only through the same forwarding/relay/mirror paths a genuine
/// one-process-per-node split deployment uses ([`BoundDataNode::start_data_with`]'s
/// `ControlHandle::Remote`, `client_route`, `route_sync_loop`) — nothing here
/// shortcuts cross-node reach through shared in-process state. `ip` binds
/// every listener at an ephemeral port on that address (mirroring
/// [`bind_cluster`]'s own `SocketAddr::new(ip, 0)` convention); `backend` and
/// the two auto-split thresholds apply to the **data** nodes only (a
/// control-only node hosts no storage engine to split).
///
/// # Errors
/// Propagates any bind failure or a failure to open a data node's CP group
/// engine (LSM backend only).
pub async fn start_split_cluster_with(
    control_n: usize,
    data_n: usize,
    dir: impl Into<PathBuf>,
    ip: std::net::IpAddr,
    backend: StorageBackend,
    auto_split_threshold: Option<usize>,
    auto_split_bytes_threshold: Option<u64>,
) -> std::io::Result<Vec<Node>> {
    start_split_cluster_with_orphan_sweep_after(
        control_n,
        data_n,
        dir,
        ip,
        backend,
        auto_split_threshold,
        auto_split_bytes_threshold,
        DEFAULT_ORPHAN_SWEEP_AFTER,
    )
    .await
}

/// Like [`start_split_cluster_with`], but also configures the
/// **orphan-member sweep** grace period (ADR 0040 PR6) on every control-role
/// node instead of [`DEFAULT_ORPHAN_SWEEP_AFTER`] — `Duration::ZERO` disables
/// it entirely. The knob `--cluster-control`/`--cluster-data`'s
/// `--orphan-sweep-after SECS` CLI flag threads through here.
///
/// # Errors
/// As [`start_split_cluster_with`].
#[allow(clippy::too_many_arguments)] // mirrors `start_split_cluster_with`'s own arity plus one knob
pub async fn start_split_cluster_with_orphan_sweep_after(
    control_n: usize,
    data_n: usize,
    dir: impl Into<PathBuf>,
    ip: std::net::IpAddr,
    backend: StorageBackend,
    auto_split_threshold: Option<usize>,
    auto_split_bytes_threshold: Option<u64>,
    orphan_sweep_after: Duration,
) -> std::io::Result<Vec<Node>> {
    start_split_cluster_with_growth(
        control_n,
        data_n,
        dir,
        ip,
        backend,
        auto_split_threshold,
        auto_split_bytes_threshold,
        orphan_sweep_after,
        None,
        None,
    )
    .await
}

/// Like [`start_split_cluster_with_orphan_sweep_after`], with the opt-in
/// **change-rate** auto-split trigger (ADR 0042 §14, growth PR3 Fork F) on
/// every data-role node — see [`BoundNode::start_with_growth`]'s doc for
/// the full design. `--cluster-control`/`--cluster-data`'s
/// `--auto-split-change-rate RATE` CLI flag threads through here.
///
/// # Errors
/// As [`start_split_cluster_with`].
#[allow(clippy::too_many_arguments)]
pub async fn start_split_cluster_with_growth(
    control_n: usize,
    data_n: usize,
    dir: impl Into<PathBuf>,
    ip: std::net::IpAddr,
    backend: StorageBackend,
    auto_split_threshold: Option<usize>,
    auto_split_bytes_threshold: Option<u64>,
    orphan_sweep_after: Duration,
    auto_split_change_rate: Option<u64>,
    dynamo_auth: Option<Arc<BTreeMap<String, String>>>,
) -> std::io::Result<Vec<Node>> {
    let dir = dir.into();
    let total = control_n + data_n;
    let ephemeral = || SocketAddr::new(ip, 0);

    let mut control_bound = Vec::with_capacity(control_n);
    for i in 0..control_n {
        let addrs = RoleAddrs {
            id: config::node_id(i),
            role: config::NodeRole::Control,
            internal: ephemeral(),
            client: ephemeral(),
            dynamo: ephemeral(),
            admin: ephemeral(),
            intra: ephemeral(),
            console: ephemeral(),
        };
        control_bound.push(
            Node::bind_control(config::node_id(i), addrs, dir.join(format!("node-{i}"))).await?,
        );
    }
    let mut data_bound = Vec::with_capacity(data_n);
    for i in control_n..total {
        let addrs = RoleAddrs {
            id: config::node_id(i),
            role: config::NodeRole::Data,
            internal: ephemeral(),
            client: ephemeral(),
            dynamo: ephemeral(),
            admin: ephemeral(),
            intra: ephemeral(),
            console: ephemeral(),
        };
        data_bound
            .push(Node::bind_data(config::node_id(i), addrs, dir.join(format!("node-{i}"))).await?);
    }

    let control_ids: Vec<NodeId> = (0..control_n).map(config::node_id).collect();

    // Each role's own internal peer book, plus the union a data node's single
    // internal env needs (its `heartbeat_loop` targets the control ids over
    // that same env).
    let control_peer_book: BTreeMap<NodeId, SocketAddr> = control_bound
        .iter()
        .map(|b| (b.id.clone(), b.internal_addr))
        .collect();
    let raftkv_peer_book: BTreeMap<NodeId, SocketAddr> = data_bound
        .iter()
        .map(|b| (b.id.clone(), b.internal_addr))
        .collect();
    let mut data_env_peers = raftkv_peer_book;
    data_env_peers.extend(control_peer_book.clone());

    // Cross-node routing (ADR 0017 #3b / ADR 0013): every node's id resolves
    // to its node's client API address, exactly like
    // `run_node_control`/`run_node_data`'s per-process assembly.
    let mut client_route: BTreeMap<NodeId, SocketAddr> = BTreeMap::new();
    for b in &control_bound {
        client_route.insert(b.id.clone(), b.client_addr);
    }
    for b in &data_bound {
        client_route.insert(b.id.clone(), b.client_addr);
    }

    // The `intra_route` sibling (ADR 0047) — identical shape, `.intra_addr`
    // instead of `.client_addr`.
    let mut intra_route: BTreeMap<NodeId, SocketAddr> = BTreeMap::new();
    for b in &control_bound {
        intra_route.insert(b.id.clone(), b.intra_addr);
    }
    for b in &data_bound {
        intra_route.insert(b.id.clone(), b.intra_addr);
    }

    // The control deployment's **intra** addresses (ADR 0047) — the
    // discovery root each data node's `ControlHandle::Remote` mirrors from
    // (`WatchMetadata` is intra-only, so this must not be the client
    // address).
    let control_intra_addrs: Vec<SocketAddr> = control_bound.iter().map(|b| b.intra_addr).collect();

    // Every node's admin address, so each node's dashboard (ADR 0021) fans
    // out to the whole split deployment.
    let admin_addrs: Vec<SocketAddr> = control_bound
        .iter()
        .map(|b| b.admin_addr)
        .chain(data_bound.iter().map(|b| b.admin_addr))
        .collect();

    let mut nodes = Vec::with_capacity(total);
    for b in control_bound {
        nodes.push(
            b.start_control_with(
                control_peer_book.clone(),
                control_ids.clone(),
                client_route.clone(),
                intra_route.clone(),
                admin_addrs.clone(),
                backend,
                orphan_sweep_after,
                // `split_mode` does not thread through the split-deployment
                // dev path yet — same documented gap this function already
                // has for `quiesce_after` (`main.rs::run`'s own comment) and
                // `--stream-seal-*`/`--segment-store` below: always the
                // byte-for-byte original ADR 0050 workflow here.
                SplitMode::default(),
            )
            .await?,
        );
    }
    for b in data_bound {
        nodes.push(
            b.start_data_with_growth(
                data_env_peers.clone(),
                control_ids.clone(),
                control_intra_addrs.clone(),
                backend,
                // A fresh, node-local edge-state set per node — never shared
                // (see the doc comment above).
                ClusterEdgeState::new(),
                client_route.clone(),
                intra_route.clone(),
                auto_split_threshold,
                auto_split_bytes_threshold,
                admin_addrs.clone(),
                StreamSealKnobs::default(),
                SegmentStoreConfig::default(),
                auto_split_change_rate,
                dynamo_auth.clone(),
                // Same documented gap as the control-role loop above.
                SplitMode::default(),
                BackupStoreConfig::default(),
            )
            .await?,
        );
    }
    Ok(nodes)
}

/// Start the single node at `index` in `config` (per-process deployment): bind
/// this node's configured listeners, wire the cluster's peer address book from
/// the config, and start its protocols with the durable on-disk [`LsmEngine`] CP
/// group.
///
/// # Errors
/// Returns `InvalidInput` if `index` is out of range, or propagates a bind /
/// engine-open failure.
pub async fn run_node(
    config: &ClusterConfig,
    index: usize,
    dir: impl Into<PathBuf>,
) -> std::io::Result<Node> {
    run_node_with(config, index, dir, StorageBackend::default()).await
}

/// Like [`run_node`], but selects the CP group's storage `backend`.
///
/// # Errors
/// As [`run_node`].
pub async fn run_node_with(
    config: &ClusterConfig,
    index: usize,
    dir: impl Into<PathBuf>,
    backend: StorageBackend,
) -> std::io::Result<Node> {
    run_node_with_orphan_sweep_after(config, index, dir, backend, DEFAULT_ORPHAN_SWEEP_AFTER).await
}

/// Like [`run_node_with`], but also configures the **orphan-member sweep**
/// grace period (ADR 0040 PR6) instead of [`DEFAULT_ORPHAN_SWEEP_AFTER`] —
/// `Duration::ZERO` disables it entirely. The knob `--config FILE --node I`'s
/// `--orphan-sweep-after SECS` CLI flag threads through here.
///
/// # Errors
/// As [`run_node_with`].
pub async fn run_node_with_orphan_sweep_after(
    config: &ClusterConfig,
    index: usize,
    dir: impl Into<PathBuf>,
    backend: StorageBackend,
    orphan_sweep_after: Duration,
) -> std::io::Result<Node> {
    run_node_with_streams(
        config,
        index,
        dir,
        backend,
        orphan_sweep_after,
        StreamSealKnobs::default(),
        SegmentStoreConfig::default(),
        DEFAULT_STREAM_RETENTION,
    )
    .await
}

/// Like [`run_node_with_orphan_sweep_after`], with explicit DynamoDB Streams
/// sealer knobs, segment-store selection, and the segment-janitor's own
/// retention grace period — see [`BoundNode::start_with_streams`]'s doc for
/// the layered-wrapper rationale. A test that needs tiny seal/retention
/// thresholds (this codebase's own testing discipline — never wait out the
/// 4-hour/4-MiB/24-hour production defaults) calls this directly.
///
/// # Errors
/// As [`run_node_with`].
#[allow(clippy::too_many_arguments)]
pub async fn run_node_with_streams(
    config: &ClusterConfig,
    index: usize,
    dir: impl Into<PathBuf>,
    backend: StorageBackend,
    orphan_sweep_after: Duration,
    stream_seal_knobs: StreamSealKnobs,
    segment_store_config: SegmentStoreConfig,
    stream_retention: Duration,
) -> std::io::Result<Node> {
    run_node_with_streams_and_quiesce_after(
        config,
        index,
        dir,
        backend,
        orphan_sweep_after,
        stream_seal_knobs,
        segment_store_config,
        stream_retention,
        Duration::ZERO,
    )
    .await
}

/// Like [`run_node_with_streams`], but also opts this node's **data-plane**
/// CP groups into quiescence (ADR 0044 phase-1 PR7) with the given idle
/// threshold — `Duration::ZERO` (every other entry point above) disables it
/// entirely, zero behavior change. `--config FILE --node I`'s
/// `--quiesce-after SECS` CLI flag threads through here. Defaults
/// [`run_node_with_streams_quiesce_and_ttl_sweep_interval`]'s own
/// `ttl_sweep_interval` to [`ttl_reaper::DEFAULT_TTL_SWEEP_INTERVAL`].
///
/// # Errors
/// As [`run_node_with`].
#[allow(clippy::too_many_arguments)]
pub async fn run_node_with_streams_and_quiesce_after(
    config: &ClusterConfig,
    index: usize,
    dir: impl Into<PathBuf>,
    backend: StorageBackend,
    orphan_sweep_after: Duration,
    stream_seal_knobs: StreamSealKnobs,
    segment_store_config: SegmentStoreConfig,
    stream_retention: Duration,
    quiesce_after: Duration,
) -> std::io::Result<Node> {
    run_node_with_streams_quiesce_and_ttl_sweep_interval(
        config,
        index,
        dir,
        backend,
        orphan_sweep_after,
        stream_seal_knobs,
        segment_store_config,
        stream_retention,
        quiesce_after,
        ttl_reaper::DEFAULT_TTL_SWEEP_INTERVAL,
        SplitMode::default(),
        BackupStoreConfig::default(),
    )
    .await
}

/// Like [`run_node_with_streams_and_quiesce_after`], but also selects the
/// **split workflow** (ADR 0058 Train 2 rung 3) instead of [`SplitMode::
/// Copy`] — `--config FILE --node I`'s `--split-mode {copy,inplace}` CLI
/// flag threads through here. The same layered-wrapper convention as
/// [`run_node_with_streams_quiesce_and_ttl_sweep_interval`]'s own
/// `ttl_sweep_interval` knob: every existing call site above keeps
/// compiling and behaving identically at `SplitMode::Copy`.
///
/// `backup_store_config` (ADR 0059 §1) — `--config FILE --node I`'s own
/// `--backup-store cluster|fs:PATH` CLI flag threads through here. Plumbing
/// only (ADR 0059 Train 1 PR②).
///
/// # Errors
/// As [`run_node_with`].
#[allow(clippy::too_many_arguments)]
pub async fn run_node_with_streams_quiesce_and_split_mode(
    config: &ClusterConfig,
    index: usize,
    dir: impl Into<PathBuf>,
    backend: StorageBackend,
    orphan_sweep_after: Duration,
    stream_seal_knobs: StreamSealKnobs,
    segment_store_config: SegmentStoreConfig,
    stream_retention: Duration,
    quiesce_after: Duration,
    split_mode: SplitMode,
    backup_store_config: BackupStoreConfig,
) -> std::io::Result<Node> {
    run_node_with_streams_quiesce_and_ttl_sweep_interval(
        config,
        index,
        dir,
        backend,
        orphan_sweep_after,
        stream_seal_knobs,
        segment_store_config,
        stream_retention,
        quiesce_after,
        ttl_reaper::DEFAULT_TTL_SWEEP_INTERVAL,
        split_mode,
        backup_store_config,
    )
    .await
}

/// Like [`run_node_with_streams_and_quiesce_after`], but also exposes the
/// TTL reaper's own sweep interval (ADR 0051) instead of pinning it at
/// [`ttl_reaper::DEFAULT_TTL_SWEEP_INTERVAL`] — the same layered-wrapper
/// convention `_with_orphan_sweep_after`/`_and_quiesce_after` already
/// established (`animusd/CLAUDE.md`'s engineering-lessons entry): every
/// existing call site above keeps compiling and behaving identically; a
/// test that needs a fast TTL sweep (this codebase's own testing
/// discipline — never wait out a real minute) calls this directly, or its
/// single-knob convenience sibling [`run_node_with_ttl_sweep_interval`].
///
/// # Errors
/// As [`run_node_with`].
#[allow(clippy::too_many_arguments)]
pub async fn run_node_with_streams_quiesce_and_ttl_sweep_interval(
    config: &ClusterConfig,
    index: usize,
    dir: impl Into<PathBuf>,
    backend: StorageBackend,
    orphan_sweep_after: Duration,
    stream_seal_knobs: StreamSealKnobs,
    segment_store_config: SegmentStoreConfig,
    stream_retention: Duration,
    quiesce_after: Duration,
    ttl_sweep_interval: Duration,
    split_mode: SplitMode,
    backup_store_config: BackupStoreConfig,
) -> std::io::Result<Node> {
    let addrs = config.nodes.get(index).cloned().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "node index out of range")
    })?;
    let bound = Node::bind(config::node_id(index), addrs, dir).await?;
    // One node per process: a fresh per-process edge-state set (it registers only
    // this node's control handle — cross-process proposal forwarding is future
    // work, ADR 0013).
    //
    // Cross-process routing (ADR 0017 #3b): map each node's one id to that
    // node's **client API** address, so an op landing on a node that isn't
    // the relevant leader forwards to the leader's node.
    let mut client_route: BTreeMap<NodeId, SocketAddr> = BTreeMap::new();
    for (i, addrs) in config.nodes.iter().enumerate() {
        client_route.insert(config::node_id(i), addrs.client);
    }
    // The `intra_route` sibling (ADR 0047) — identical shape, `.intra`
    // instead of `.client`.
    let mut intra_route: BTreeMap<NodeId, SocketAddr> = BTreeMap::new();
    for (i, addrs) in config.nodes.iter().enumerate() {
        intra_route.insert(config::node_id(i), addrs.intra);
    }
    // Every node's admin address from the shared config, so this node's dashboard
    // (ADR 0021) can fan out to the whole cluster.
    let admin_addrs: Vec<SocketAddr> = config.nodes.iter().map(|n| n.admin).collect();
    // ADR 0057: the cluster config's `dynamo_auth` section (already validated
    // non-empty at `ClusterConfig::from_json` load time), or `None` — disables
    // auth entirely, byte-identical to pre-ADR-0057 behavior.
    let dynamo_auth = config
        .dynamo_auth
        .as_ref()
        .map(|cfg| Arc::new(cfg.credentials.clone()));
    bound
        .start_with_growth(
            config.peer_book(),
            config.control_ids(),
            config.data_ids(),
            backend,
            ClusterEdgeState::new(),
            client_route,
            intra_route,
            None,
            None,
            admin_addrs,
            orphan_sweep_after,
            stream_seal_knobs,
            segment_store_config,
            stream_retention,
            None,
            quiesce_after,
            ttl_sweep_interval,
            dynamo_auth,
            split_mode,
            backup_store_config,
        )
        .await
}

/// Like [`run_node_with`], but with a test-tunable TTL reaper sweep
/// interval (ADR 0051) instead of [`ttl_reaper::DEFAULT_TTL_SWEEP_INTERVAL`]
/// — the single-knob convenience shape [`run_node_with_orphan_sweep_after`]
/// establishes for its own knob, so a TTL end-to-end test doesn't need to
/// spell out every other layer's default explicitly. Every other knob stays
/// at its production default (no quiescence, the default orphan-sweep
/// grace, production stream-seal/segment-store/retention settings).
///
/// # Errors
/// As [`run_node_with`].
pub async fn run_node_with_ttl_sweep_interval(
    config: &ClusterConfig,
    index: usize,
    dir: impl Into<PathBuf>,
    backend: StorageBackend,
    ttl_sweep_interval: Duration,
) -> std::io::Result<Node> {
    run_node_with_streams_quiesce_and_ttl_sweep_interval(
        config,
        index,
        dir,
        backend,
        DEFAULT_ORPHAN_SWEEP_AFTER,
        StreamSealKnobs::default(),
        SegmentStoreConfig::default(),
        DEFAULT_STREAM_RETENTION,
        Duration::ZERO,
        ttl_sweep_interval,
        SplitMode::default(),
        BackupStoreConfig::default(),
    )
    .await
}

/// Start node `index` from `config` as a **control-only** node (ADR 0035
/// PR3, `animusd control`): binds only the control internal `ProdEnv` role
/// plus the client + admin listeners, and runs only the control [`RaftNode`]
/// (its own `reconcile_loop`/`detect_loop`) plus the tail every node shape
/// shares (`route_sync_loop`/`metrics_sample_loop`/self-registration/
/// `serve_requests` (both listeners)/admin `serve`, via
/// [`BoundControlNode::start_control_with`]) — no CP data storage engine, no
/// `raftkv` env, no DynamoDB listener. `backend` (ADR 0038) selects the
/// **dedicated** system-keyspace engine this control-only node provisions
/// (`StorageBackend::Lsm` durable by default, `::Memory` under `--ephemeral`)
/// — now the durable home of the apply task's published `Metadata` cache
/// (`Metadata: DRIVER_APPLIED`).
///
/// `config`'s control-role entries (`ClusterConfig::control_ids`) are this
/// node's control-plane voter set — `index` must be one of them. `config` may
/// also list data-role entries (a split-deployment config,
/// [`ClusterConfig::generate_split`]) — they are not this node's concern
/// beyond appearing in `client_route` (so a data op landing here forwards
/// correctly to a data node) and `admin_addrs` (so the dashboard fans out to
/// them too).
///
/// # Errors
/// Returns `InvalidInput` if `index` is out of range or does not run the
/// control role, or propagates a bind failure or a system-keyspace-engine-open
/// failure.
pub async fn run_node_control(
    config: &ClusterConfig,
    index: usize,
    dir: impl Into<PathBuf>,
    backend: StorageBackend,
) -> std::io::Result<Node> {
    run_node_control_with_orphan_sweep_after(
        config,
        index,
        dir,
        backend,
        DEFAULT_ORPHAN_SWEEP_AFTER,
    )
    .await
}

/// Like [`run_node_control`], but also configures the **orphan-member sweep**
/// grace period (ADR 0040 PR6) instead of [`DEFAULT_ORPHAN_SWEEP_AFTER`] —
/// `Duration::ZERO` disables it entirely. The knob `animusd control`'s
/// `--orphan-sweep-after SECS` CLI flag threads through here.
///
/// # Errors
/// As [`run_node_control`].
pub async fn run_node_control_with_orphan_sweep_after(
    config: &ClusterConfig,
    index: usize,
    dir: impl Into<PathBuf>,
    backend: StorageBackend,
    orphan_sweep_after: Duration,
) -> std::io::Result<Node> {
    let addrs = config.nodes.get(index).cloned().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "node index out of range")
    })?;
    if !addrs.role.has_control() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "node index does not run the control role",
        ));
    }
    let bound = Node::bind_control(config::node_id(index), addrs, dir).await?;

    // Cross-node routing (ADR 0017 #3b / ADR 0013): map every node's id to
    // its client API address, so a data op or a schema-DDL relay landing on
    // this control node forwards to the right node — the same shape
    // `run_node_with` builds.
    let mut client_route: BTreeMap<NodeId, SocketAddr> = BTreeMap::new();
    for (i, a) in config.nodes.iter().enumerate() {
        client_route.insert(config::node_id(i), a.client);
    }
    // The `intra_route` sibling (ADR 0047) — identical shape, `.intra`
    // instead of `.client`.
    let mut intra_route: BTreeMap<NodeId, SocketAddr> = BTreeMap::new();
    for (i, a) in config.nodes.iter().enumerate() {
        intra_route.insert(config::node_id(i), a.intra);
    }
    // Every node's admin address from the shared config, so this node's
    // dashboard (ADR 0021) can fan out to the whole cluster (control and data
    // nodes alike).
    let admin_addrs: Vec<SocketAddr> = config.nodes.iter().map(|n| n.admin).collect();

    bound
        .start_control_with(
            config.peer_book(),
            config.control_ids(),
            client_route,
            intra_route,
            admin_addrs,
            backend,
            orphan_sweep_after,
            // `animusd control`'s CLI surface has no `--split-mode` flag of
            // its own (mirroring `--quiesce-after`'s identical scope gap
            // for this same subcommand) — a control-only node's
            // `trigger_split` calls always default to the byte-for-byte
            // original ADR 0050 workflow.
            SplitMode::default(),
        )
        .await
}

/// Start node `index` from `config` as a **data-only** node (ADR 0035 PR4,
/// `animusd data`): binds only the `raftkv` internal `ProdEnv` role plus the
/// client/dynamo/admin listeners, and runs no local control `RaftCore` at
/// all — `Metadata` comes from a polled mirror of the control deployment
/// (`ControlHandle::Remote`, [`BoundDataNode::start_data_with`]) rather than
/// this process's own Raft replication.
///
/// `config`'s data-role entries (`ClusterConfig::data_indexes`) are this
/// node's data fleet — `index` must be one of them. `config`'s control-role
/// entries (`ClusterConfig::control_ids`) are the **separately-deployed**
/// control plane this node mirrors: their **intra** addresses (ADR 0047; was
/// **client** pre-ADR-0047) seed the mirror + leader-hint sync loop and
/// `propose_schema`'s relay/broadcast tiers (ADR 0035 §1/§4), and their
/// **control** ids are what this node's own
/// `heartbeat_loop` targets (unchanged ADR 0012 failure-detection semantics —
/// see `ClusterConfig::control_peer_book`'s doc for why this node's `raftkv`
/// env peer book must union both address books, not `raftkv_peer_book()`
/// alone).
///
/// # Errors
/// Returns `InvalidInput` if `index` is out of range, does not run the data
/// role, or `config` has no control-role entry for this node to mirror; or
/// propagates a bind / engine-open failure.
pub async fn run_node_data(
    config: &ClusterConfig,
    index: usize,
    dir: impl Into<PathBuf>,
    backend: StorageBackend,
) -> std::io::Result<Node> {
    let addrs = config.nodes.get(index).cloned().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "node index out of range")
    })?;
    if !addrs.role.has_data() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "node index does not run the data role",
        ));
    }
    let control_ids = config.control_ids();
    if control_ids.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "config has no control-role node for this data node to mirror",
        ));
    }
    let bound = Node::bind_data(config::node_id(index), addrs, dir).await?;

    // The control deployment's **intra**-cluster addresses (ADR 0047) — the
    // mirror/leader-hint discovery root (ADR 0035 §1/§4; `WatchMetadata` is
    // intra-only, so this must be the intra address, not the client one), a
    // wholly different address axis from the internal env peer book below.
    let control_intra_addrs: Vec<SocketAddr> = config
        .nodes
        .iter()
        .filter(|a| a.role.has_control())
        .map(|a| a.intra)
        .collect();

    // Cross-node routing (ADR 0017 #3b / ADR 0013): map every node's id to
    // its client API address — the same shape `run_node_control` builds.
    let mut client_route: BTreeMap<NodeId, SocketAddr> = BTreeMap::new();
    for (i, a) in config.nodes.iter().enumerate() {
        client_route.insert(config::node_id(i), a.client);
    }
    // The `intra_route` sibling (ADR 0047) — identical shape, `.intra`
    // instead of `.client`.
    let mut intra_route: BTreeMap<NodeId, SocketAddr> = BTreeMap::new();
    for (i, a) in config.nodes.iter().enumerate() {
        intra_route.insert(config::node_id(i), a.intra);
    }
    // Every node's admin address from the shared config, so this node's
    // dashboard fan-out (ADR 0021) covers the whole split deployment.
    let admin_addrs: Vec<SocketAddr> = config.nodes.iter().map(|n| n.admin).collect();
    // ADR 0057: same `dynamo_auth` section, same non-empty validation, as
    // the combined-mode path above (`run_node_with_streams_quiesce_and_ttl_
    // sweep_interval`) — a data-only node binds the dynamo listener too
    // (ADR 0035 PR4), so it needs the credential store just the same.
    let dynamo_auth = config
        .dynamo_auth
        .as_ref()
        .map(|cfg| Arc::new(cfg.credentials.clone()));

    // Calls `start_data_with_growth` directly (skipping the `start_data_with`/
    // `start_data_with_streams` wrapper layers) so `dynamo_auth` can be
    // threaded in — the same "call the innermost layer directly for a
    // knob its wrappers don't expose" convention the combined-mode path
    // above uses.
    bound
        .start_data_with_growth(
            // This node's internal env peer book: every node in the
            // deployment (`ClusterConfig::peer_book`) — `heartbeat_loop`
            // below sends to `control_ids` over this very env.
            config.peer_book(),
            control_ids,
            control_intra_addrs,
            backend,
            ClusterEdgeState::new(),
            client_route,
            intra_route,
            None,
            None,
            admin_addrs,
            StreamSealKnobs::default(),
            SegmentStoreConfig::default(),
            None,
            dynamo_auth,
            // `animusd data --config`'s CLI surface has no `--split-mode`
            // flag of its own (mirrors `run_node_control_with_orphan_sweep_
            // after`'s identical scope gap) — always the byte-for-byte
            // original ADR 0050 workflow.
            SplitMode::default(),
            // Same documented gap for `--backup-store` (ADR 0059 §1): no
            // CLI flag reaches `animusd data --config` yet, so this always
            // gets the default `Cluster` store.
            BackupStoreConfig::default(),
        )
        .await
}

/// Start node `index` from `config` as a **control-plane-follower-less growth
/// member** (ADR 0030): online cluster growth, data-plane only. `config` is an
/// **expanded** config — it lists every pre-growth node plus every node added so
/// far, `index` among them — so this node's peer book / `client_route` / admin
/// fan-out are all complete from the moment it starts (same as
/// [`run_node_with`]). The one deliberate difference is `original_control_ids`:
/// the control group that existed **before** this node did, passed to
/// [`BoundNode::start_with`] in place of `config.control_ids()`.
///
/// This node's own control role therefore starts genuinely **outside** that
/// group's voter config (it "needs no control-voter slot" — verified: a Raft
/// node whose id was never in `all_nodes` at construction is a permanent,
/// harmless non-voter — `is_voter()` gates campaigning cleanly and it never
/// disrupts the real cluster, the same safety property an already-removed
/// voter relies on). The control group genuinely **never grows** — restarting
/// the pre-growth nodes with a wider `all_nodes` was considered and rejected:
/// it would work (a control-plane WAL with no prior config-changing entry
/// falls back to whatever `all_nodes` a restart supplies), but requires a
/// coordinated restart of the *existing* cluster, which is not "online" growth
/// and would violate the "control group stays static" scope decision (ADR
/// 0030) for a capability this slice does not need.
///
/// Consequently this node's own `RaftCore` never receives real Raft
/// replication for that group — the real leader's own peer set is derived from
/// *its* `all_nodes`, which never learned of this id — so `start_with` spawns
/// [`remote_metadata_sync_loop`] for it instead, mirroring the real cluster's
/// `Metadata` via `ClientRequest::Status` polls against `original_control_ids`'s
/// client addresses (resolved through the now-complete `client_route`).
/// Everything that must work on a growth node (CP routing, join-host, its own
/// address self-registration) reads through `ClientCtx::effective_metadata`,
/// which transparently prefers the mirror when populated.
///
/// # Errors
/// As [`run_node_with`].
pub async fn run_node_growth(
    config: &ClusterConfig,
    index: usize,
    original_control_ids: Vec<NodeId>,
    dir: impl Into<PathBuf>,
    backend: StorageBackend,
) -> std::io::Result<Node> {
    let addrs = config.nodes.get(index).cloned().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "node index out of range")
    })?;
    let bound = Node::bind(config::node_id(index), addrs, dir).await?;
    let mut client_route: BTreeMap<NodeId, SocketAddr> = BTreeMap::new();
    for (i, addrs) in config.nodes.iter().enumerate() {
        client_route.insert(config::node_id(i), addrs.client);
    }
    // The `intra_route` sibling (ADR 0047) — identical shape, `.intra`
    // instead of `.client`; this is what makes the growth-node mirror's own
    // seed-building (`start_with_streams`'s `ctx.intra_addr(id)` call) resolve
    // correctly from this node's very first tick.
    let mut intra_route: BTreeMap<NodeId, SocketAddr> = BTreeMap::new();
    for (i, addrs) in config.nodes.iter().enumerate() {
        intra_route.insert(config::node_id(i), addrs.intra);
    }
    let admin_addrs: Vec<SocketAddr> = config.nodes.iter().map(|n| n.admin).collect();
    // `bootstrap` must never auto-register this growth node itself (it
    // self-registers `Down` via `admin_add_member` instead, see this fn's
    // doc) — so, mirroring `original_control_ids`, scope `data_ids` to the
    // **pre-growth** set (one identity per node, ADR 0040 PR1 — the
    // pre-growth control ids ARE the pre-growth data ids), not `config`'s
    // (expanded) `data_ids()`.
    let data_ids: Vec<NodeId> = original_control_ids.clone();
    bound
        .start_with(
            config.peer_book(),
            original_control_ids,
            data_ids,
            backend,
            ClusterEdgeState::new(),
            client_route,
            intra_route,
            None,
            None,
            admin_addrs,
            DEFAULT_ORPHAN_SWEEP_AFTER,
        )
        .await
}

/// How long a single connection attempt to a join seed may take before giving
/// up on it and trying the next one in the list (mirrors [`ClientCtx::relay`]'s
/// per-hop timeout — see [`CLIENT_TIMEOUT`]).
const JOIN_ATTEMPT_TIMEOUT: Duration = CLIENT_TIMEOUT;
/// How long [`poll_seeds_for`] waits between passes over the whole seed list
/// while none has answered (a fresh seed cluster may still be electing).
const JOIN_RETRY_INTERVAL: Duration = Duration::from_millis(200);
/// Total budget [`run_node_join`] gives [`poll_seeds_for`] to reach *any* seed
/// for a [`ClientRequest::JoinInfo`] / [`ClientRequest::Status`] reply before
/// giving up and failing startup — generous, matching [`SCHEMA_COMMIT_TIMEOUT`],
/// since a seed may itself still be electing a leader or mid-restart.
const JOIN_DISCOVERY_BUDGET: Duration = SCHEMA_COMMIT_TIMEOUT;

/// One pass over `seeds`, trying each in order for `request`; returns the
/// first non-[`Error`](ClientResponse::Error) reply. Standalone (not a
/// [`ClientCtx`] method) because a joining node has no context yet — this is
/// exactly what it's discovering.
async fn join_request(seeds: &[SocketAddr], request: &ClientRequest) -> Option<ClientResponse> {
    for &addr in seeds {
        let reply = tokio::time::timeout(JOIN_ATTEMPT_TIMEOUT, async {
            let mut stream = TcpStream::connect(addr).await.ok()?;
            write_frame(&mut stream, request).await.ok()?;
            read_frame::<ClientResponse>(&mut stream).await.ok()?
        })
        .await;
        if let Ok(Some(resp)) = reply
            && !matches!(resp, ClientResponse::Error(_))
        {
            return Some(resp);
        }
    }
    None
}

/// Poll `seeds` for `request` (one [`join_request`] pass per [`JOIN_RETRY_INTERVAL`])
/// until one answers or `budget` elapses.
///
/// # Errors
/// A `TimedOut` error if no seed answers within `budget`.
async fn poll_seeds_for(
    seeds: &[SocketAddr],
    request: &ClientRequest,
    budget: Duration,
) -> std::io::Result<ClientResponse> {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        if let Some(resp) = join_request(seeds, request).await {
            return Ok(resp);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("no seed in {seeds:?} answered within {budget:?}"),
            ));
        }
        tokio::time::sleep(JOIN_RETRY_INTERVAL).await;
    }
}

/// Start a node as a **seed/join growth member** (ADR 0032 PR2, `animusd
/// join`): unlike [`run_node_growth`], which needs an operator-assembled
/// *expanded* `ClusterConfig` listing every node's addresses up front, this
/// entry point needs only `addrs` (this node's own six addresses) and
/// `seeds` (any already-running node's **intra-cluster** address — ADR
/// 0047, was the client address pre-ADR-0047; old or newly grown, it no
/// longer matters which, since ADR 0032 PR1 made every node's address book
/// equally current). Joining is a cluster-membership action — the joiner is
/// about to become an internal `ProdEnv`/Raft peer too — so the intra
/// address is the honest seed, not a compromise.
///
/// **ADR 0040 PR4 clean break**: `--node I` is gone from the join path
/// entirely (no operator-index sugar) — `id` is either an explicit,
/// already-validated identity (`--id NAME`, [`NodeId::propose`] having run at
/// the CLI boundary) or `None` to self-mint (ADR 0040 Decision B). Identity
/// is claimed **before binding anything**, over the bare wire (this process
/// has no `ClientCtx`/env yet): [`claim_join_identity`] proposes
/// `MetaCommand::RegisterNode` via `ClientRequest::ProposeSchema` (relayed —
/// see [`is_relayable_command`]'s allowlist) and polls a `ClientRequest::
/// Status` reply's `node_addrs` for the same claim, exactly the propose-then-
/// poll shape [`ClientCtx::register_node`] uses post-bind — just reached
/// through the raw wire primitives every join entry point already has
/// ([`join_request`]/[`poll_seeds_for`]), since there is no `ClientCtx` yet
/// to call a method on. A **minted** id re-mints and retries on collision; a
/// **proposed** id fails loudly (`AlreadyExists`) naming the conflict — see
/// [`claim_join_identity`]'s own doc.
///
/// Once identity is settled, this contacts a seed for a
/// [`ClientRequest::JoinInfo`] reply (the pre-growth control group + the
/// answering node's internal peer book + its live client-op route + every
/// known admin address) and hands the discovered `original_control_ids` +
/// merged peer/route/admin sets straight into [`BoundNode::start_with`]
/// exactly like [`run_node_growth`] does — the ADR 0030 growth machinery
/// engages automatically, including this node's own ADR 0032 PR1 address
/// self-registration (now an idempotent `RegisterNodeAddrs` update-only
/// re-affirmation of the claim [`claim_join_identity`] already made) and its
/// own [`ClientCtx::admin_add_member`] self-registration (a harmless no-op —
/// `RegisterNode` already registered the member) — no separate step is
/// needed here for either.
///
/// # Errors
/// An `io::Error` (`TimedOut`) if no seed answers within
/// [`JOIN_DISCOVERY_BUDGET`], `AlreadyExists` if an explicit `--id` collides
/// with a different existing registration, or (as [`run_node_growth`]) a
/// bind / engine-open failure.
pub async fn run_node_join(
    seeds: Vec<SocketAddr>,
    id: Option<NodeId>,
    addrs: RoleAddrs,
    dir: &Path,
    backend: StorageBackend,
    labels: BTreeMap<String, String>,
) -> std::io::Result<Node> {
    if seeds.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "join needs at least one --seed address",
        ));
    }
    let (original_control_ids, peers, client_route, intra_route, admin_addrs) =
        discover_join_info(&seeds).await?;

    let mine = NodeAddrs {
        internal: addrs.internal.to_string(),
        client: addrs.client.to_string(),
        admin: addrs.admin.to_string(),
        intra: addrs.intra.to_string(),
        role: "combined".to_string(),
    };
    let my_id = claim_join_identity(&seeds, id, &mine, &labels).await?;
    let my_client_addr = addrs.client;
    let my_admin_addr = addrs.admin;
    let my_intra_addr = addrs.intra;

    let bound = Node::bind(my_id.clone(), addrs, dir).await?;

    finish_combined_join(
        bound,
        my_id,
        my_client_addr,
        my_admin_addr,
        my_intra_addr,
        original_control_ids,
        peers,
        client_route,
        intra_route,
        admin_addrs,
        backend,
    )
    .await
}

/// The shared "merge discovered peers/route/admin → build `data_ids` →
/// `start_with`" tail of [`run_node_join`]: once a joiner has bound its
/// listener and knows its own (claimed) id and client address, finishing the
/// join is identical regardless of whether that id was proposed or minted.
/// Merges this node's own entries into the discovered peer/route/admin sets
/// — the same union `run_node_growth`'s expanded-config construction already
/// produces, just built from a discovery reply instead of a pre-assembled
/// config — then starts the node exactly like `run_node_growth` does:
/// `bootstrap` must never auto-register the joining node itself, so
/// `data_ids` is scoped to the pre-growth set discovered via `JoinInfo`
/// (one identity per node, ADR 0040 PR1 — the pre-growth control ids ARE the
/// pre-growth data ids), never including this node.
#[allow(clippy::too_many_arguments)] // a join's id + addrs + discovered sets, no natural grouping
async fn finish_combined_join(
    bound: BoundNode,
    my_id: NodeId,
    my_client_addr: SocketAddr,
    my_admin_addr: SocketAddr,
    my_intra_addr: SocketAddr,
    original_control_ids: Vec<NodeId>,
    mut peers: BTreeMap<NodeId, SocketAddr>,
    mut client_route: BTreeMap<NodeId, SocketAddr>,
    mut intra_route: BTreeMap<NodeId, SocketAddr>,
    mut admin_addrs: Vec<SocketAddr>,
    backend: StorageBackend,
) -> std::io::Result<Node> {
    for (id, addr) in bound.peer_entries() {
        peers.insert(id, addr);
    }
    client_route.insert(my_id.clone(), my_client_addr);
    // The `intra_route` sibling (ADR 0047) — see `ClientResponse::JoinInfo`'s
    // own field doc for why this must be a real, discovered seed, not empty.
    intra_route.insert(my_id, my_intra_addr);
    if !admin_addrs.contains(&my_admin_addr) {
        admin_addrs.push(my_admin_addr);
    }

    let data_ids: Vec<NodeId> = original_control_ids.clone();
    bound
        .start_with(
            peers,
            original_control_ids,
            data_ids,
            backend,
            ClusterEdgeState::new(),
            client_route,
            intra_route,
            None,
            None,
            admin_addrs,
            DEFAULT_ORPHAN_SWEEP_AFTER,
        )
        .await
}

/// The `JoinInfo` discovery half of [`run_node_join`]/[`run_node_data_join`]
/// (ADR 0035 PR5 — factored out so the data-only join variant can reuse it
/// verbatim instead of duplicating the poll/match/error-format boilerplate):
/// polls `seeds` for a [`ClientResponse::JoinInfo`] reply within
/// [`JOIN_DISCOVERY_BUDGET`].
async fn discover_join_info(
    seeds: &[SocketAddr],
) -> std::io::Result<(
    Vec<NodeId>,
    BTreeMap<NodeId, SocketAddr>,
    BTreeMap<NodeId, SocketAddr>,
    BTreeMap<NodeId, SocketAddr>,
    Vec<SocketAddr>,
)> {
    match poll_seeds_for(seeds, &ClientRequest::JoinInfo, JOIN_DISCOVERY_BUDGET).await? {
        ClientResponse::JoinInfo {
            control_ids,
            peers,
            client_route,
            intra_route,
            admin_addrs,
        } => Ok((control_ids, peers, client_route, intra_route, admin_addrs)),
        other => Err(std::io::Error::other(format!(
            "seed returned an unexpected reply to JoinInfo: {other:?}"
        ))),
    }
}

/// How many times a **minted** join identity is allowed to collide (ADR 0040
/// Decision C) before giving up — see [`MAX_MINT_ATTEMPTS`]'s own doc for why
/// this bound is never expected to be hit in practice.
const MAX_JOIN_MINT_ATTEMPTS: u32 = MAX_MINT_ATTEMPTS;

/// The pre-bind (no `ClientCtx`/env yet) counterpart of [`ClientCtx::
/// register_node`]'s propose-then-poll registration CAS — used by every join
/// entry point before this process's own listeners exist. Same CAS/
/// observable-state contract, reached over the bare wire primitives every
/// join entry point already uses ([`join_request`]/[`poll_seeds_for`])
/// instead of a genuine `ClientCtx`: (re-)propose `MetaCommand::RegisterNode`
/// via `ClientRequest::ProposeSchema` every [`JOIN_RETRY_INTERVAL`], polling
/// a `ClientRequest::Status` reply's `node_addrs` for the same observable
/// outcome `register_node` confirms — `Registered` once it holds exactly
/// `addrs`, `Collision` once it visibly holds something else.
async fn register_node_over_wire(
    seeds: &[SocketAddr],
    node: &NodeId,
    addrs: &NodeAddrs,
    labels: &BTreeMap<String, String>,
) -> std::io::Result<RegisterOutcome> {
    let deadline = tokio::time::Instant::now() + JOIN_DISCOVERY_BUDGET;
    let command = MetaCommand::RegisterNode {
        node: node.clone(),
        addrs: addrs.clone(),
        labels: labels.clone(),
    };
    loop {
        // Best-effort (re-)propose — a relay/leader race just gets retried
        // next pass, exactly like every other join round trip here.
        let _ = join_request(seeds, &ClientRequest::ProposeSchema(command.clone())).await;
        if let Some(ClientResponse::Status { metadata, .. }) =
            join_request(seeds, &ClientRequest::Status).await
        {
            match metadata.node_addrs.get(node) {
                Some(existing) if existing == addrs => return Ok(RegisterOutcome::Registered),
                Some(_) => return Ok(RegisterOutcome::Collision),
                None => {}
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "no seed in {seeds:?} confirmed registration for node {node} within \
                     {JOIN_DISCOVERY_BUDGET:?}"
                ),
            ));
        }
        tokio::time::sleep(JOIN_RETRY_INTERVAL).await;
    }
}

/// Claim this join's identity, **pre-bind** (ADR 0040 Decision B/C):
/// `explicit_id` is a `--id NAME` proposal (already validated —
/// [`NodeId::propose`] ran at the CLI boundary), registered with one attempt
/// and a loud, named failure on collision; `None` self-mints
/// ([`NodeId::mint`] over [`animus_env::prod::PreBindRng`] — the sanctioned
/// pre-bind entropy source that replaces `generate_join_nonce`'s narrower,
/// bespoke exception) and re-mints on collision, up to
/// [`MAX_JOIN_MINT_ATTEMPTS`] tries (astronomically unlikely to ever be
/// needed — a 128-bit mint colliding once is already vanishing, so this
/// bound only guards against a genuine bug looping forever).
async fn claim_join_identity(
    seeds: &[SocketAddr],
    explicit_id: Option<NodeId>,
    addrs: &NodeAddrs,
    labels: &BTreeMap<String, String>,
) -> std::io::Result<NodeId> {
    match explicit_id {
        Some(id) => match register_node_over_wire(seeds, &id, addrs, labels).await? {
            RegisterOutcome::Registered => Ok(id),
            RegisterOutcome::Collision => Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!(
                    "node id `{id}` is already claimed by a different registration \
                     (different addresses/labels) — pick a different --id"
                ),
            )),
        },
        None => {
            for _ in 0..MAX_JOIN_MINT_ATTEMPTS {
                let candidate = NodeId::mint(&animus_env::prod::PreBindRng);
                match register_node_over_wire(seeds, &candidate, addrs, labels).await? {
                    RegisterOutcome::Registered => return Ok(candidate),
                    RegisterOutcome::Collision => continue,
                }
            }
            Err(std::io::Error::other(format!(
                "exhausted {MAX_JOIN_MINT_ATTEMPTS} self-minted id collisions in a row \
                 (practically impossible) — this points at a real bug, not bad luck"
            )))
        }
    }
}

/// Start a node as a **data-only seed/join member** (ADR 0035 PR5): the
/// data-only counterpart of [`run_node_join`], reusing its `JoinInfo`
/// discovery + identity-claim shape verbatim
/// ([`discover_join_info`]/[`claim_join_identity`]) but constructing the
/// **`Remote`** data-role assembly ([`BoundDataNode::start_data_with`])
/// instead of a combined-mode node with a local control `RaftCore`. CLI:
/// `animusd data --seed ADDR[,ADDR...] [--id NAME] --base-port P [--dir D]
/// [--ephemeral]`.
///
/// The discovered `original_control_ids` (the seed's `JoinInfo`
/// reply) feed both `heartbeat_loop`'s failure-detection target and, via the
/// merged `intra_route` (ADR 0047 — `WatchMetadata` is intra-only, so
/// `control_seeds` must be intra addresses, not `client_route`'s),
/// [`RemoteControlClient::new`]'s `control_seeds` — the
/// discovery root this node's mirror sync/long-poll watch loop
/// ([`remote_metadata_watch_loop`]) polls from then on. Mirrors
/// [`run_node_data`]'s own note on why the internal `raftkv` env's peer book
/// must stay the **union** of data + control addresses (`peers`, built from
/// the discovery reply's `peers` map, which already carries both axes) rather
/// than data-only addresses alone: `heartbeat_loop` sends to `control_ids`
/// over that very env.
///
/// # Errors
/// As [`run_node_join`]: an `io::Error` (`InvalidInput`) if `addrs` has the
/// wrong role shape, `TimedOut` if no seed answers within
/// [`JOIN_DISCOVERY_BUDGET`], `AlreadyExists` if an explicit `--id` collides
/// with a different existing registration, or a bind / engine-open failure.
pub async fn run_node_data_join(
    seeds: Vec<SocketAddr>,
    id: Option<NodeId>,
    addrs: RoleAddrs,
    dir: &Path,
    backend: StorageBackend,
    labels: BTreeMap<String, String>,
    dynamo_auth: Option<Arc<BTreeMap<String, String>>>,
) -> std::io::Result<Node> {
    if seeds.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "join needs at least one --seed address",
        ));
    }
    let (original_control_ids, peers, client_route, intra_route, admin_addrs) =
        discover_join_info(&seeds).await?;

    let mine = NodeAddrs {
        internal: addrs.internal.to_string(),
        client: addrs.client.to_string(),
        admin: addrs.admin.to_string(),
        intra: addrs.intra.to_string(),
        role: "data".to_string(),
    };
    let my_id = claim_join_identity(&seeds, id, &mine, &labels).await?;
    let my_client_addr = addrs.client;
    let my_admin_addr = addrs.admin;
    let my_intra_addr = addrs.intra;

    let bound = Node::bind_data(my_id.clone(), addrs, dir).await?;

    finish_data_join(
        bound,
        my_id,
        my_client_addr,
        my_admin_addr,
        my_intra_addr,
        original_control_ids,
        peers,
        client_route,
        intra_route,
        admin_addrs,
        backend,
        dynamo_auth,
    )
    .await
}

/// The **data-only** dual of [`finish_combined_join`]: the shared "merge
/// discovered peers/route/admin → derive control seeds → `start_data_with`"
/// tail of [`run_node_data_join`].
#[allow(clippy::too_many_arguments)] // mirrors `finish_combined_join`'s shape
async fn finish_data_join(
    bound: BoundDataNode,
    my_id: NodeId,
    my_client_addr: SocketAddr,
    my_admin_addr: SocketAddr,
    my_intra_addr: SocketAddr,
    original_control_ids: Vec<NodeId>,
    mut peers: BTreeMap<NodeId, SocketAddr>,
    mut client_route: BTreeMap<NodeId, SocketAddr>,
    mut intra_route: BTreeMap<NodeId, SocketAddr>,
    mut admin_addrs: Vec<SocketAddr>,
    backend: StorageBackend,
    dynamo_auth: Option<Arc<BTreeMap<String, String>>>,
) -> std::io::Result<Node> {
    // The data-only dual of `finish_combined_join`'s merge (a single raftkv
    // peer entry, no control id of its own to add).
    let (peer_id, peer_addr) = bound.peer_entry();
    peers.insert(peer_id, peer_addr);
    client_route.insert(my_id.clone(), my_client_addr);
    // The `intra_route` sibling (ADR 0047) — see `finish_combined_join`'s
    // identical treatment.
    intra_route.insert(my_id, my_intra_addr);
    if !admin_addrs.contains(&my_admin_addr) {
        admin_addrs.push(my_admin_addr);
    }

    // The control deployment's **intra** addresses (ADR 0047; `WatchMetadata`
    // is intra-only) — the same derivation `run_node_data` does from a static
    // `ClusterConfig`, here from the merged, discovery-built `intra_route`
    // instead.
    let control_seeds: Vec<SocketAddr> = original_control_ids
        .iter()
        .filter_map(|id| intra_route.get(id).copied())
        .collect();

    // Calls `start_data_with_growth` directly (skipping the layered wrapper
    // shape) — see `run_node_data`'s identical note.
    bound
        .start_data_with_growth(
            peers,
            original_control_ids,
            control_seeds,
            backend,
            ClusterEdgeState::new(),
            client_route,
            intra_route,
            None,
            None,
            admin_addrs,
            StreamSealKnobs::default(),
            SegmentStoreConfig::default(),
            None,
            dynamo_auth,
            // `animusd data --seed` join has no `--split-mode` flag of its
            // own (same documented gap as `run_node_data`) — always the
            // byte-for-byte original ADR 0050 workflow.
            SplitMode::default(),
            // Same documented gap for `--backup-store` as `run_node_data`.
            BackupStoreConfig::default(),
        )
        .await
}

/// Upper bound on a client-protocol frame (the `u32` length prefix is
/// **untrusted** input on the client + cross-node relay ports — without a cap,
/// four bytes from any dialer forces up to a 4 GiB allocation in [`read_frame`]).
///
/// Sized comfortably above the largest legitimate frames this protocol carries:
/// - a single client/forwarded `Put` — its value enters via the HTTP edges,
///   whose bodies cap at 1 MiB (`http::MAX_BODY`), and JSON-encodes a `Vec<u8>`
///   at ≤ 4 chars per byte → ~4 MiB;
/// - a forwarded `PutBatch` from the admin bulk seeder — bounded to
///   `SEED_BATCH_MAX_BYTES` (4 MiB) of raw entry bytes per batch → ~17 MiB JSON;
/// - everything else (`Get`/`Scan`/`ProposeSchema`/split triggers) is tiny.
///
/// An over-cap length prefix is rejected with a clean `InvalidData` error (the
/// connection closes) before any allocation, never a panic or an OOM.
pub const MAX_FRAME_LEN: usize = 64 << 20;

/// Send `request` to a peer node's client API over a fresh connection and
/// return its reply (or a [`ClientResponse::Error`] on any transport
/// failure). Free function, not a [`ClientCtx`] method (ADR 0035 PR4): the
/// data-only node's [`control_handle::RemoteControlClient`] has no `ClientCtx`
/// of its own to reach through, but needs the exact same wire primitive every
/// other cross-node relay in this crate uses — [`ClientCtx::relay`] is now a
/// thin wrapper over this.
pub(crate) async fn relay_request(addr: SocketAddr, request: &ClientRequest) -> ClientResponse {
    relay_request_with_timeout(addr, request, CLIENT_TIMEOUT).await
}

/// Like [`relay_request`], but with an explicit transport timeout instead of
/// the default [`CLIENT_TIMEOUT`] (ADR 0035 PR5) — needed by
/// [`remote_metadata_watch_loop`], whose long-poll request's own
/// [`WATCH_METADATA_CLIENT_TIMEOUT`] must exceed the serving node's
/// [`WATCH_METADATA_SERVER_TIMEOUT`] bound by a comfortable margin; reusing
/// the generic [`CLIENT_TIMEOUT`] here would race the server's own reply.
async fn relay_request_with_timeout(
    addr: SocketAddr,
    request: &ClientRequest,
    timeout: Duration,
) -> ClientResponse {
    match tokio::time::timeout(timeout, async {
        let mut stream = TcpStream::connect(addr).await.ok()?;
        write_frame(&mut stream, request).await.ok()?;
        read_frame::<ClientResponse>(&mut stream).await.ok()?
    })
    .await
    {
        Ok(Some(resp)) => resp,
        _ => ClientResponse::Error("relay to peer node failed".into()),
    }
}

/// Write a length-prefixed (`u32` big-endian) JSON frame.
///
/// # Errors
/// Propagates write failures; rejects a frame over [`MAX_FRAME_LEN`] (the
/// receiver would drop the connection anyway — failing at the sender names the
/// culprit instead of surfacing as a mysterious peer hang-up).
pub async fn write_frame<T: Serialize>(stream: &mut TcpStream, msg: &T) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(msg).expect("client message serializes");
    if bytes.len() > MAX_FRAME_LEN {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "frame of {} bytes exceeds MAX_FRAME_LEN ({MAX_FRAME_LEN})",
                bytes.len()
            ),
        ));
    }
    stream.write_u32(bytes.len() as u32).await?;
    stream.write_all(&bytes).await?;
    stream.flush().await?;
    Ok(())
}

/// Read a length-prefixed JSON frame, or `None` at clean EOF.
///
/// # Errors
/// Propagates read failures and decode errors; a declared length over
/// [`MAX_FRAME_LEN`] is an `InvalidData` error **before any allocation** (the
/// length prefix is untrusted — see [`MAX_FRAME_LEN`]).
pub async fn read_frame<T: DeserializeOwned>(stream: &mut TcpStream) -> std::io::Result<Option<T>> {
    let len = match stream.read_u32().await {
        Ok(len) => len as usize,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    };
    if len > MAX_FRAME_LEN {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("declared frame length {len} exceeds MAX_FRAME_LEN ({MAX_FRAME_LEN})"),
        ));
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    let msg = serde_json::from_slice(&buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(Some(msg))
}

/// Unit tests for [`byte_weighted_median`] (ADR 0034) — a private free
/// function, so this lives as an in-crate `#[cfg(test)]` module (like
/// `split_fence_tests` above) rather than under `tests/`, which can't reach
/// it.
#[cfg(test)]
mod auto_split_median_tests {
    use super::byte_weighted_median;

    fn pair(key: &str, value_len: usize) -> (Vec<u8>, Vec<u8>) {
        (key.as_bytes().to_vec(), vec![b'x'; value_len])
    }

    /// Many tiny rows plus a **few huge** ones: the byte-weighted median must
    /// land near where the *bytes* are roughly halved, which — because the
    /// huge values dominate the total — is a very different key than the
    /// plain positional median (`pairs.len() / 2`, dead center by count).
    #[test]
    fn skewed_value_sizes_bisect_by_bytes_not_position() {
        // 20 tiny rows (~1 byte value each) then 2 huge rows (~10,000 bytes
        // each) at the end: positionally the median sits deep in the tiny
        // run (index 11 of 22), but two rows alone hold the vast majority of
        // the bytes.
        let mut pairs: Vec<(Vec<u8>, Vec<u8>)> =
            (0..20).map(|i| pair(&format!("k{i:03}"), 1)).collect();
        pairs.push(pair("y0", 10_000));
        pairs.push(pair("y1", 10_000));

        let total: u64 = pairs.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum();
        let positional_median = pairs[pairs.len() / 2].0.clone();
        // The positional median is one of the tiny keys, deep in the small
        // run — nowhere near where the bytes actually split.
        assert!(
            positional_median.starts_with(b"k"),
            "sanity: the plain positional median is a tiny-row key, not a \
             huge-row one, proving the two metrics genuinely disagree here"
        );

        let split = byte_weighted_median(&pairs);
        let split_idx = pairs
            .iter()
            .position(|(k, _)| k == &split)
            .expect("split key is one of the pairs");

        // Sum of bytes strictly before the split point vs. at/after it — both
        // halves should be within a small tolerance of `total / 2`, unlike
        // the positional median which would leave ~20KB on one side and a
        // few dozen bytes on the other.
        let left_bytes: u64 = pairs[..split_idx]
            .iter()
            .map(|(k, v)| (k.len() + v.len()) as u64)
            .sum();
        let right_bytes: u64 = total - left_bytes;
        let half = total / 2;
        // Since the two huge rows dominate, the byte-weighted cut must fall
        // at or after the first huge row (index 20) — i.e. not inside the
        // tiny-row run at all.
        assert!(
            split_idx >= 20,
            "byte-weighted median (index {split_idx}) must fall at/after the \
             first huge value, not inside the tiny-row run — total={total}"
        );
        // And it must actually roughly bisect the bytes: neither side should
        // hold less than a third of the total (a loose bound — this is a
        // heuristic estimator, not an exact bisection).
        assert!(
            left_bytes >= half / 3 && right_bytes >= half / 3,
            "split should roughly halve bytes: left={left_bytes} right={right_bytes} half={half}"
        );
    }

    /// Uniform value sizes: the byte-weighted median should land at (or very
    /// near) the plain positional median, since bytes-per-row is constant.
    #[test]
    fn uniform_value_sizes_agree_with_positional_median() {
        let pairs: Vec<(Vec<u8>, Vec<u8>)> =
            (0..10).map(|i| pair(&format!("k{i:03}"), 8)).collect();
        let positional = pairs[pairs.len() / 2].0.clone();
        let weighted = byte_weighted_median(&pairs);
        assert_eq!(
            weighted, positional,
            "uniform row sizes: byte-weighted and positional medians should coincide"
        );
    }

    /// Always returns an interior key (never the very first key), matching
    /// the positional median's own "index > 0" guarantee — required so
    /// `SplitTablet` always sees a valid `start < at` split point.
    #[test]
    fn never_returns_the_first_key() {
        let pairs = vec![pair("a", 100_000), pair("b", 1), pair("c", 1)];
        let split = byte_weighted_median(&pairs);
        assert_ne!(split, b"a".to_vec(), "must not return the first key");
    }
}

/// Regression tests for [`ClientCtx::confirm_wait_is_futile`] (issue #268) —
/// in-crate because they need a private [`CpGroup`] handle and the
/// `pub(crate)` [`ClientCtx::cp_kind_local`], which no external `tests/`
/// file can reach (the same reason `gsi_drain_cursor_tests` lives inside
/// `index_drain.rs`). Run via `cargo test -p animusd --lib`.
#[cfg(test)]
mod confirm_futility_tests {
    use std::net::SocketAddr;
    use std::path::Path;
    use std::time::{Duration, Instant};

    use tokio::time::{sleep, timeout};

    use crate::config::NodeRole;
    use crate::{
        ClientCtx, ClientRequest, ClientResponse, ClusterConfig, Node, RoleAddrs, read_frame,
        run_node, write_frame,
    };

    fn free_addrs(count: usize) -> Vec<SocketAddr> {
        let ls: Vec<std::net::TcpListener> = (0..count)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        ls.iter().map(|l| l.local_addr().unwrap()).collect()
    }

    fn single_node_config() -> ClusterConfig {
        let addrs = free_addrs(6);
        ClusterConfig {
            nodes: vec![RoleAddrs {
                id: crate::config::node_id(0),
                role: NodeRole::Both,
                internal: addrs[0],
                client: addrs[1],
                dynamo: addrs[2],
                admin: addrs[3],
                intra: addrs[4],
                console: addrs[5],
            }],
            dynamo_auth: None,
        }
    }

    async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
        let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
        write_frame(&mut stream, &req).await.expect("send");
        read_frame(&mut stream)
            .await
            .expect("read")
            .expect("a reply")
    }

    async fn put_until_ok(addr: SocketAddr, table: &str, key: &[u8], value: &[u8]) {
        timeout(Duration::from_secs(20), async {
            loop {
                match call(
                    addr,
                    ClientRequest::Put {
                        key: key.to_vec(),
                        value: value.to_vec(),
                        table: table.to_string(),
                    },
                )
                .await
                {
                    ClientResponse::PutOk => return,
                    ClientResponse::Error(_) => sleep(Duration::from_millis(100)).await,
                    other => panic!("unexpected put response: {other:?}"),
                }
            }
        })
        .await
        .expect("seed put did not succeed in 20s");
    }

    /// Bring up a single node, retrying against the documented port-TOCTOU
    /// race (`docs/engineering-lessons.md`): `single_node_config()`'s
    /// `free_addrs` probe releases its ports before the real bind, so
    /// another test binary can steal one under `cargo test --workspace`
    /// contention. Each attempt allocates a **fresh** config.
    async fn single_node(dir: &Path) -> (Node, ClusterConfig) {
        let mut last_err = None;
        for attempt in 0..16 {
            let config = single_node_config();
            match run_node(&config, 0, dir.join(format!("node-{attempt}"))).await {
                Ok(node) => return (node, config),
                Err(e) => {
                    last_err = Some(e);
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }
        panic!(
            "could not bring up single node after retries (ports kept getting stolen): {last_err:?}"
        );
    }

    /// **The futility early-exit (issue #268).** A `KindBatch` whose own-key
    /// condition fails applies as a silent no-op (`KindBatch.conditions`,
    /// `animus-cp-data`) — the probed effect never appears even though the
    /// accepted entry committed and applied fine. Pre-fix, `cp_kind_local`'s
    /// confirm loop polled value equality for the whole `CLIENT_TIMEOUT`
    /// (10s) before erring — the exact per-attempt burn that let brief
    /// leadership churn on a starved CI runner stack two 10s stalls into one
    /// 25s client budget (the cp_txn.rs seed-put flake). Post-fix the loop
    /// notices `engine_applied_index()` passed the accepted entry without
    /// its effect and errs immediately, in the house retryable shape.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_condition_failed_kind_batch_fails_fast_with_a_retryable_error() {
        let dir = tempfile::tempdir().unwrap();
        let (node, config) = single_node(dir.path()).await;
        let client = config.nodes[0].client;

        // Seed put: provisions the table's first tablet and proves the
        // single-voter group elected and serves.
        put_until_ok(client, "cf_t", b"cf-seed", b"seed").await;
        let tablet = *node
            .metadata()
            .tablets_for_table("cf_t")
            .next()
            .expect("seed put provisioned a tablet")
            .0;
        let group = node
            .edge
            .local_cp(tablet)
            .expect("this node hosts the tablet");
        assert!(group.is_leader(), "single-voter group leads locally");

        // A batch guarded by a condition that cannot hold (the key was never
        // written): accepted + applied as a no-op, effect never appears.
        let started = tokio::time::Instant::now();
        let err = ClientCtx::cp_kind_local(
            &group,
            vec![(
                animus_cp_data::KIND_BASE,
                b"cf-target".to_vec(),
                Some(b"must-not-land".to_vec()),
            )],
            Vec::new(),
            vec![(b"cf-guard".to_vec(), Some(b"wrong-expected".to_vec()))],
        )
        .await
        .expect_err("a condition-failed kind batch must not confirm");
        let elapsed = started.elapsed();

        assert!(
            err.ends_with("; retry"),
            "the failure must carry the house retryable shape so caller loops re-route: {err}"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "a provably-futile confirm wait must end fast (pre-fix: polled out the whole \
             10s CLIENT_TIMEOUT): took {elapsed:?}"
        );

        // The early exit fired on the no-op, not on a broken group: an
        // ordinary unconditioned write through the same path still confirms.
        ClientCtx::cp_kind_local(
            &group,
            vec![(
                animus_cp_data::KIND_BASE,
                b"cf-after".to_vec(),
                Some(b"lands".to_vec()),
            )],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("an ordinary write after the futile one still confirms");

        node.shutdown();
    }

    /// Regression for issue #285: `dynamo::kind_write_item_at_leader` used to
    /// hold `ctx.data().rmw_lock` across the whole `cp_kind_local` propose+
    /// confirm-poll, not just its own read+evaluate — so one item's slow
    /// confirm (apply backlog stretches this even with the #268 fast-fail
    /// above) stalled *every other* evaluated write on the node behind it,
    /// including a write to a completely unrelated tablet.
    ///
    /// A `ConditionExpression` failure can't reproduce this: it returns
    /// (`ConditionFailed`) before `cp_kind_local` is ever called, so the
    /// lock is released at the same point regardless of the fix — the bug
    /// is specifically about the propose+confirm phase, which a failed
    /// eval-time condition never reaches.
    ///
    /// **Why this doesn't race a real apply backlog.** An earlier version of
    /// this test built the "slow propose+confirm" scenario for real, with a
    /// concurrent filler flood against the write's own tablet running for a
    /// fixed wall-clock window, hoping the flood's own commits would grow
    /// the tablet's apply backlog faster than the target write's confirm
    /// could drain it. That is a real race, not a guarantee: on a CPU-
    /// starved runner the flood is starved right along with everything
    /// else, so it can fail to build any backlog at all — observed in CI on
    /// commit `97289e2`, where two parallel runs of the identical code came
    /// back one green and one red, the red one logging `DIAG: unrelated
    /// write (group B) took 103.937566ms` with the "slow" write having
    /// *already finished*. This test now uses
    /// `dynamo::rmw285_confirm_gate` (see its own doc) to hold write A's
    /// propose+confirm phase open for a fixed, generous delay under this
    /// test's own control instead of hoping a flood wins a scheduling race
    /// — the in-flight window this regresses against no longer depends on
    /// how contended the machine happens to be.
    ///
    /// A second, wholly unrelated tablet (its own independent Raft group and
    /// apply pipeline) then proves the point: pre-fix, a write to it queues
    /// behind the node-wide lock held for write A's entire gated
    /// read+propose+confirm; post-fix the lock is released the moment
    /// write A's read+evaluate finishes, so the second write is unaffected
    /// by write A's still-ongoing (artificially held-open) confirm phase.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn an_unrelated_evaluated_write_is_not_stalled_behind_another_writes_confirm_wait() {
        let dir = tempfile::tempdir().unwrap();
        let (node, _config) = single_node(dir.path()).await;
        let ctx = node.ctx_for_test();

        ctx.provision_tablet("rmw_285_a")
            .await
            .expect("provisioning table A");
        ctx.provision_tablet("rmw_285_b")
            .await
            .expect("provisioning table B");
        let meta = node.metadata();
        let tablet_a = *meta
            .tablets_for_table("rmw_285_a")
            .next()
            .expect("table A has a tablet")
            .0;
        let tablet_b = *meta
            .tablets_for_table("rmw_285_b")
            .next()
            .expect("table B has a tablet")
            .0;
        let group_a = node
            .edge
            .local_cp(tablet_a)
            .expect("this node hosts table A's tablet");
        let group_b = node
            .edge
            .local_cp(tablet_b)
            .expect("this node hosts table B's tablet");
        // `provision_tablet` alone does not wait for the group to actually
        // elect (its own doc: an ordinary caller's routed op does that via
        // `cp_route`) — poll rather than assert immediately.
        for group in [&group_a, &group_b] {
            timeout(Duration::from_secs(10), async {
                while !group.is_leader() {
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("tablet group did not elect a local leader in time");
        }

        // Arm write A's propose+confirm phase to hold open for a fixed
        // delay once it releases `rmw_lock` (see `dynamo::
        // rmw285_confirm_gate`'s doc for why this replaced a real,
        // load-sensitive filler flood). `GATE_DELAY` only needs to
        // comfortably outlast an ordinary unrelated write's own
        // read+evaluate+propose+confirm — including under real contention:
        // CI observed 104ms for that under load (commit `97289e2`), so this
        // leaves roughly a 20x margin, not a hand-tuned near-miss.
        const GATE_DELAY: Duration = Duration::from_secs(2);
        crate::dynamo::rmw285_confirm_gate::arm("rmw_285_a", GATE_DELAY);

        let mut item_a = animus_dynamo::Item::new();
        item_a.insert(
            "pk".to_string(),
            animus_dynamo::AttributeValue::S("slow-item".to_string()),
        );
        let pk_a = animus_dynamo::AttributeValue::S("slow-item".to_string());
        let slow = tokio::spawn({
            let ctx = ctx.clone();
            let group_a = group_a.clone();
            let meta = meta.clone();
            async move {
                crate::dynamo::kind_write_item_at_leader(
                    &ctx,
                    &group_a,
                    &meta,
                    "rmw_285_a",
                    &pk_a,
                    None,
                    crate::KindWriteOp::Put(item_a),
                    None,
                    false,
                )
                .await
            }
        });

        // Cosmetic pacing only (not load-bearing): give write A's task a
        // moment to actually start running before write B is spawned, so
        // the two don't merely race to get scheduled at all. The gate above
        // is what actually makes write A's in-flight window deterministic.
        sleep(Duration::from_millis(10)).await;

        let mut item_b = animus_dynamo::Item::new();
        item_b.insert(
            "pk".to_string(),
            animus_dynamo::AttributeValue::S("unrelated-item".to_string()),
        );
        let pk_b = animus_dynamo::AttributeValue::S("unrelated-item".to_string());
        let started = Instant::now();
        let outcome = timeout(
            Duration::from_secs(60),
            crate::dynamo::kind_write_item_at_leader(
                &ctx,
                &group_b,
                &meta,
                "rmw_285_b",
                &pk_b,
                None,
                crate::KindWriteOp::Put(item_b),
                None,
                false,
            ),
        )
        .await
        .expect("the unrelated write must not need the outer 60s safety timeout")
        .expect("the unrelated write must itself succeed");
        let elapsed = started.elapsed();
        eprintln!("DIAG: unrelated write (group B) took {elapsed:?}");
        assert!(
            matches!(outcome, crate::dynamo::KindWriteOutcome::Ok { .. }),
            "the unrelated write must actually land, not just return some outcome"
        );

        // What this actually proves, and what it does not. Pre-fix,
        // `rmw_lock` is one node-wide lock held across write A's whole
        // call, so write B cannot even *start* its own read until write A's
        // ENTIRE call (read+evaluate+propose+confirm) returns and drops the
        // guard — under that code, write B could never observe write A as
        // still in flight. Post-fix, write A drops the lock the moment its
        // own read+evaluate finishes, then (via the gate armed above) sits
        // in its propose+confirm phase for a fixed `GATE_DELAY` before ever
        // proposing — so write B, unblocked as soon as the lock frees,
        // reliably finishes and returns while write A is still gated.
        //
        // This is *not* a hard ordering guarantee in the way the assertion
        // below reads on its own: it holds because `GATE_DELAY` was chosen
        // to comfortably outlast write B's own real duration (see that
        // constant's doc), not because the two are ordered by construction.
        // A version of write B slow enough to exceed `GATE_DELAY` — which
        // the `elapsed` check right below also guards against — could in
        // principle still invert it. What *is* load-independent is the
        // mechanism: write A's in-flight window no longer depends on a
        // flood winning a real-time race to build apply backlog, only on
        // write B finishing inside a fixed, generous budget.
        assert!(
            !slow.is_finished(),
            "the gated write (group A) must still be in flight when the unrelated write \
             (group B) returns — pre-fix, the unrelated write cannot even start until the \
             gated write's ENTIRE call (including its confirm-poll) has already returned and \
             released the node-wide rmw_lock, so it could never observe this"
        );
        // The load-bearing margin for the assertion above: write B must
        // finish well inside `GATE_DELAY`, not just inside some loose
        // hang-guard ceiling — a regression that re-widens `rmw_lock`'s
        // scope would force write B to wait out (most of) `GATE_DELAY`
        // itself, which this catches even if `slow.is_finished()` above
        // somehow didn't.
        assert!(
            elapsed < GATE_DELAY / 2,
            "the unrelated write took implausibly long relative to GATE_DELAY={GATE_DELAY:?} — \
             either implausible CI noise, or rmw_lock's scope regressed to cover write A's \
             gated propose/confirm phase again: {elapsed:?}"
        );

        let slow_started = Instant::now();
        slow.await
            .expect("slow task panicked")
            .expect("the gated write must itself eventually succeed too");
        eprintln!(
            "DIAG: slow task (group A) finished {:?} after the unrelated write returned",
            slow_started.elapsed()
        );
        node.shutdown();
    }
}

/// Regression for issues #282/#279's fix: bare [`Node::shutdown`] and
/// [`Node`]'s `Drop` impl both latch every hosted CP group's `halted` flag —
/// see each's own doc for the full rationale. This module needs
/// `CpGroup::is_halted` (`#[cfg(test)]`-only, no external `tests/` binary can
/// reach a private `CpGroup`) and `node.edge.local_cp`, hence in-crate like
/// `confirm_futility_tests` above; `ProdEnv` has no fault-injection knob (that
/// lives only in `animus_sim::SimEnv`), so this doesn't attempt to race a real
/// disk fault — the deterministic proof that a halted-latched group tolerates
/// one lives in `animus-cp-data`'s `tests/shutdown.rs`. This just proves the
/// latch itself actually reaches every hosted group on both paths, and that
/// neither path panics doing it.
#[cfg(test)]
mod halted_shutdown_tests {
    use std::net::SocketAddr;
    use std::path::Path;
    use std::time::Duration;

    use tokio::time::sleep;

    use crate::config::NodeRole;
    use crate::{ClusterConfig, Node, RoleAddrs, run_node};

    fn free_addrs(count: usize) -> Vec<SocketAddr> {
        let ls: Vec<std::net::TcpListener> = (0..count)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        ls.iter().map(|l| l.local_addr().unwrap()).collect()
    }

    fn single_node_config() -> ClusterConfig {
        let addrs = free_addrs(6);
        ClusterConfig {
            nodes: vec![RoleAddrs {
                id: crate::config::node_id(0),
                role: NodeRole::Both,
                internal: addrs[0],
                client: addrs[1],
                dynamo: addrs[2],
                admin: addrs[3],
                intra: addrs[4],
                console: addrs[5],
            }],
            dynamo_auth: None,
        }
    }

    /// Bring up a single node, retrying against the documented port-TOCTOU
    /// race (`docs/engineering-lessons.md`), mirroring
    /// `confirm_futility_tests::single_node`.
    async fn single_node(dir: &Path) -> Node {
        let mut last_err = None;
        for attempt in 0..16 {
            let config = single_node_config();
            match run_node(&config, 0, dir.join(format!("node-{attempt}"))).await {
                Ok(node) => return node,
                Err(e) => {
                    last_err = Some(e);
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }
        panic!(
            "could not bring up single node after retries (ports kept getting stolen): {last_err:?}"
        );
    }

    /// Seed a put so the single-voter group provisions its first tablet and
    /// elects, then return that tablet's locally-hosted group handle.
    async fn provision_and_get_group(node: &Node) -> crate::CpGroup {
        use crate::{ClientRequest, ClientResponse, read_frame, write_frame};

        let client = node.client_addr();
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let mut stream = tokio::net::TcpStream::connect(client)
                    .await
                    .expect("connect");
                write_frame(
                    &mut stream,
                    &ClientRequest::Put {
                        key: b"seed".to_vec(),
                        value: b"seed".to_vec(),
                        table: "halt_t".to_string(),
                    },
                )
                .await
                .expect("send");
                match read_frame(&mut stream)
                    .await
                    .expect("read")
                    .expect("a reply")
                {
                    ClientResponse::PutOk => return,
                    ClientResponse::Error(_) => sleep(Duration::from_millis(100)).await,
                    other => panic!("unexpected put response: {other:?}"),
                }
            }
        })
        .await
        .expect("seed put did not succeed in 20s");

        let tablet = *node
            .metadata()
            .tablets_for_table("halt_t")
            .next()
            .expect("seed put provisioned a tablet")
            .0;
        node.edge
            .local_cp(tablet)
            .expect("this single-voter node hosts the tablet")
    }

    /// Bare `Node::shutdown()` — the doc-blessed "kill node N" idiom — must
    /// latch `halted` on every hosted CP group before it returns, with no
    /// panic and no wait for the driver to actually stop.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bare_shutdown_latches_halted_on_every_hosted_group() {
        let dir = tempfile::tempdir().unwrap();
        let node = single_node(dir.path()).await;
        let group = provision_and_get_group(&node).await;

        assert!(
            !group.is_halted(),
            "a freshly-provisioned group must not start out halted"
        );
        node.shutdown();
        assert!(
            group.is_halted(),
            "bare Node::shutdown() must latch halted on every hosted group"
        );
    }

    /// Dropping a `Node` that was never explicitly `shutdown()` (a panic
    /// mid-test unwinding its `Vec<Node>`, per issue #279's panic half) must
    /// latch `halted` on every hosted CP group too, via `Node`'s `Drop` impl.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dropping_an_unshutdown_node_latches_halted_on_every_hosted_group() {
        let dir = tempfile::tempdir().unwrap();
        let node = single_node(dir.path()).await;
        let group = provision_and_get_group(&node).await;

        assert!(!group.is_halted());
        drop(node);
        assert!(
            group.is_halted(),
            "dropping an un-shutdown Node must latch halted via its Drop impl"
        );
    }
}

/// Unit tests for [`align_split_key`] (F11, ADR 0042 §14, growth PR2) — a
/// private free function, so this lives as an in-crate `#[cfg(test)]`
/// module (like `auto_split_median_tests` above) rather than under
/// `tests/`, which can't reach it. `manual_split_with_unaligned_key_on_
/// streamed_table_rounds_to_token_boundary` (`tests/f11_split_alignment.rs`)
/// covers the same rounding end to end, through a real cluster's admin HTTP
/// surface; these are the fast, pure-function siblings for the rounding
/// rule itself and the Fork E degenerate case.
#[cfg(test)]
mod align_split_key_tests {
    use animus_control::{MetaCommand, Metadata, StreamSpec, StreamViewType};
    use animus_tablet::{KeyRange, TabletId};

    use super::align_split_key;

    fn streamed_metadata_with_tablet(tablet: TabletId, range: KeyRange) -> Metadata {
        let mut m = Metadata::default();
        assert!(matches!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "orders".to_owned(),
                schema: animus_control::TableSchema::simple(
                    "pk",
                    animus_control::ColumnType::String
                ),
            }),
            animus_control::ApplyOutcome::Applied
        ));
        assert!(matches!(
            m.apply(&MetaCommand::SetTableStream {
                table: "orders".to_owned(),
                spec: Some(StreamSpec {
                    view_type: StreamViewType::NewAndOldImages,
                    label: "L1".to_owned(),
                }),
            }),
            animus_control::ApplyOutcome::Applied
        ));
        assert!(matches!(
            m.apply(&MetaCommand::CreateTablet {
                tablet,
                table: Some("orders".to_owned()),
                range,
                replicas: Vec::new(),
            }),
            animus_control::ApplyOutcome::Applied
        ));
        m
    }

    /// A streamed table's split key rounds down to exactly `TOKEN_BYTES`,
    /// discarding everything past the token — the F11 rule.
    #[test]
    fn rounds_a_streamed_tables_key_down_to_the_token_boundary() {
        let tablet = TabletId(1);
        let m = streamed_metadata_with_tablet(tablet, KeyRange::whole());
        let raw = b"orders-mXX".to_vec(); // 10 bytes.
        let (rounded, viable) = align_split_key(&m, tablet, raw);
        assert_eq!(rounded, b"orders-m".to_vec());
        assert!(
            viable,
            "the rounded key is still strictly inside the whole range"
        );
    }

    /// An unstreamed table's split key is returned byte-for-byte unchanged,
    /// whatever its length — F11 only applies to a streamed table.
    #[test]
    fn leaves_an_unstreamed_tables_key_untouched() {
        let mut m = Metadata::default();
        assert!(matches!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some("plain".to_owned()),
                range: KeyRange::whole(),
                replicas: Vec::new(),
            }),
            animus_control::ApplyOutcome::Applied
        ));
        let raw = b"any-length-key-at-all".to_vec();
        let (key, viable) = align_split_key(&m, TabletId(1), raw.clone());
        assert_eq!(key, raw);
        assert!(viable);
    }

    /// A key already exactly `TOKEN_BYTES` long round-trips unchanged (the
    /// idempotent case: `trigger_split` re-rounding a key `auto_split_loop`
    /// or a prior caller already rounded).
    #[test]
    fn a_key_already_token_aligned_is_unchanged() {
        let tablet = TabletId(1);
        let m = streamed_metadata_with_tablet(tablet, KeyRange::whole());
        let raw = 0x8000_0000_0000_0000u64.to_be_bytes().to_vec();
        let (rounded, viable) = align_split_key(&m, tablet, raw.clone());
        assert_eq!(rounded, raw);
        assert!(viable);
    }

    /// Fork E (ADR 0042 §14): a single very hot partition token owns the
    /// whole tablet — rounding its own key collapses it onto `range.start`,
    /// so `viable` reports `false` (the degenerate, structurally
    /// unsplittable case `trigger_split` turns into a metered skip rather
    /// than ever proposing).
    #[test]
    fn reports_not_viable_when_the_rounded_key_collapses_onto_range_start() {
        // Range starting at the token "orders-m" itself (as a real sibling
        // tablet's own `range.start` would look after a prior split) — any
        // key beginning with that same 8-byte prefix rounds right back onto
        // it.
        let tablet = TabletId(2);
        let range = KeyRange {
            start: b"orders-m".to_vec(),
            end: None,
        };
        let m = streamed_metadata_with_tablet(tablet, range);
        let (rounded, viable) = align_split_key(&m, tablet, b"orders-mZZ".to_vec());
        assert_eq!(rounded, b"orders-m".to_vec());
        assert!(
            !viable,
            "a token-rounded key equal to the tablet's own range.start is not a legal split point"
        );
    }

    /// An unknown tablet id reports `viable == false` (nothing to check
    /// against) rather than optimistically claiming any key would work.
    #[test]
    fn reports_not_viable_for_an_unknown_tablet() {
        let m = Metadata::default();
        let (key, viable) = align_split_key(&m, TabletId(999), b"whatever".to_vec());
        assert_eq!(key, b"whatever".to_vec());
        assert!(!viable);
    }
}

/// Back-compat coverage for [`ClientResponse::Status`]'s additive fields
/// (ADR 0037 PR2's `control_voters`, mirroring `leader_hint`/`watermark`'s
/// own `#[serde(default)]` discipline when they were added) — a pre-existing
/// binary's wire reply (predating a field) must still decode on a peer that
/// has since upgraded, and vice versa is guaranteed by the same
/// `#[serde(default)]` on the older side once it upgrades. Free functions
/// under test are `pub`, so this could live under `tests/`, but the JSON
/// surgery is a pure serde round trip with no process/socket involved —
/// kept as an in-crate unit test alongside the other pure-function test
/// modules in this file.
#[cfg(test)]
mod status_wire_compat_tests {
    use animus_env::nid;

    use crate::ClientResponse;

    /// A `Status` reply serialized before `control_voters` existed (no such
    /// key at all) still decodes, defaulting to an empty set — the same
    /// "missing key, not just null" back-compat shape `leader_hint`/
    /// `watermark` already established for this variant.
    #[test]
    fn status_without_control_voters_field_still_decodes() {
        let reply = ClientResponse::Status {
            metadata: Default::default(),
            leader_hint: None,
            intra_leader_hint: None,
            watermark: 7,
            control_voters: [0, 1, 2].into_iter().map(nid).collect(),
        };
        let mut value = serde_json::to_value(&reply).expect("Status serializes");
        // `ClientResponse` derives `Serialize`/`Deserialize` via serde's
        // default (externally tagged) enum representation: `{"Status":
        // {...fields...}}`. Drill into the inner object to drop the field,
        // exactly like `meta.rs`'s `NodeAddrs` back-compat test does for its
        // own struct.
        value
            .get_mut("Status")
            .and_then(|s| s.as_object_mut())
            .expect("Status is a JSON object")
            .remove("control_voters");
        let decoded: ClientResponse =
            serde_json::from_value(value).expect("Status without control_voters still decodes");
        match decoded {
            ClientResponse::Status {
                control_voters,
                watermark,
                ..
            } => {
                assert!(
                    control_voters.is_empty(),
                    "missing control_voters must default to empty, not fail to decode"
                );
                assert_eq!(watermark, 7, "sibling field must decode unaffected");
            }
            other => panic!("expected a Status reply, got {other:?}"),
        }
    }
}
