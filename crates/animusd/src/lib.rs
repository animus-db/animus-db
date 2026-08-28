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
// ADR 0061 rung C1: the client-facing wire types moved to the `E: Env`-generic
// `animus-node` crate — see that crate's `CLAUDE.md`. `topology` (CP-route
// resolution) and `decide` (the pure decision predicates lifted out of
// `ClientCtx` by rung A6) move alongside them in this same commit. All
// re-exported here so the ~500 existing call sites across this crate (bare
// `ClientRequest`, `topology::decide_cp_route`, `decide::frozen_refusal`,
// `use crate::KindWriteOp`, etc.) keep compiling unchanged.
pub use animus_node::{
    ClientRequest, ClientResponse, KindWriteOp, PendingKindWrite, Surface, TxnPrecondition,
    TxnTableWrite, TxnWriteCondition, decide, is_relayable_command, surface_of, topology,
};
// ADR 0061 rung C5 step 3a: `ClientCtx`'s own `control` field needs the
// *generic* `ControlHandle<E, R>` (not this crate's `E = ProdEnv`/`R =
// AnimusdRelayClient`-bound `control_handle::ControlHandle` alias below,
// which cannot be matched against inside a `impl<E: Env, R: RelayClient>
// ClientCtx<E, R>` body — see `crates/animusd/CLAUDE.md`'s elision gotcha)
// and the `RelayClient` bound itself.
use animus_node::control_handle::ControlHandle as GenericControlHandle;
use animus_node::host::RelayClient;

mod admin;
mod backup_capture;
mod backup_completion;
mod backup_janitor;
mod backup_restore;
mod client_ctx_host;
mod console;
mod control_handle;
mod dashboard;
mod dynamo;
mod dynamo_streams;
mod forwarding;
mod http;
mod index_backfill;
mod pitr_janitor;
mod read_path;
mod schema;
mod segment_janitor;
mod ttl_reaper;
mod txn_coordinator;
mod write_path;

use control_handle::{AnimusdRelayClient, ControlHandle, RemoteControlClient};

use animus_control::node::{DEFAULT_ORPHAN_SWEEP_AFTER, HEARTBEAT_INTERVAL, send_heartbeat};
use animus_control::{PlacementPolicy, ProposeResult, RaftNode};
use animus_cp_data::hlc::HlcTimestamp;
use animus_cp_data::host::{MemoryTabletEngines, MetadataView, Reconciler};
use animus_cp_data::{
    FastRead, KindBatchOutcome, RaftKvNode, StageOutcome, TxnDecisionStatus, TxnId, TxnOutcome,
    TxnRecordView,
};
use animus_env::{Clock, Disk, Env, FsSegmentStore, Metric, MetricsHandle, NodeId, ProdEnv};
use animus_storage::{
    Key, LsmEngine, MemoryEngine, SsTableView, StorageEngine, StorageError, VersionedValue,
    WalRecordView,
};
use animus_tablet::{KeyRange, TabletId, TabletState};
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

    /// Whether this reason means "this coordinator could not confirm the
    /// transaction committed" (a `"; retry"`-suffixed `Other` — the same
    /// house-wide retryability convention [`decide::read_should_retry`]
    /// already tests) as opposed to "this
    /// coordinator definitively knows the transaction did not commit"
    /// (`ConditionFailed`/`TransactionConflict`, or an `Other` whose message
    /// does not end in `"; retry"`).
    ///
    /// **Load-bearing for `ClientRequestToken` idempotency** (ADR 0018's
    /// issue #298 "deep shape A" amendment): recording an ambiguous outcome
    /// as `CANCELLED` in the idempotency table would tell a future
    /// same-token retry (and the client) the write definitely never
    /// happened, when it may already have — `dynamo::run_transact` uses this
    /// predicate to leave the idempotency record `PENDING` instead of
    /// writing a possibly wrong `CANCELLED`, the same "an unconfirmed
    /// outcome is UNKNOWN, never evidence of a specific result" discipline
    /// `docs/engineering-lessons.md`'s issue #298 shape B/shape A amendments
    /// already applied to `txn_recover`'s own queries.
    ///
    /// **This is a superset of [`Self::is_safe_to_retry_fresh`]** — see that
    /// method's own doc for the narrower, retry-eligible subset and why the
    /// distinction matters. Every ambiguous reason still gets the same
    /// "never record `CANCELLED`" treatment; only the narrow allowlisted
    /// subset is eligible to be retried with a fresh `TxnId`.
    pub(crate) fn is_ambiguous(&self) -> bool {
        matches!(self, TxnAbortReason::Other(msg) if msg.ends_with("; retry"))
    }

    /// The narrow, **allowlisted** subset of [`Self::is_ambiguous`] where the
    /// failure is PROVABLY a no-op for this entire transaction attempt —
    /// occurring before any Raft propose for it was ever attempted (a
    /// frozen-tablet refusal, no route reachable, a leader-side read
    /// failure) or as a stage-time STRUCTURAL rejection that never wrote
    /// anything (`StageOutcome::Fenced`'s stale-route/out-of-fence causes —
    /// see below for why its rarer third cause is still safe here). Safe to
    /// retry with a fresh `TxnId` regardless of which hop failed or whether
    /// an earlier hop (e.g. the anchor) already staged, because 2PC's own
    /// atomicity guarantee means an anchor staged without every participant
    /// confirming can only ever be recovered as `Abort`
    /// (`ClientCtx::txn_recover`'s `all_staged` check requires a genuinely-
    /// verified `Ok(true)` from every participant to decide `Commit`) — it
    /// can never spuriously commit later.
    ///
    /// **Deliberately an ALLOWLIST, not a denylist** — the inverse of this
    /// crate's own first attempt at this predicate, which named the known-
    /// dangerous messages and treated everything else as safe. That
    /// approach missed the DECIDE-phase confirmation-loss messages entirely
    /// ("CP group leader moved during anchor commit/abort", "after decide",
    /// "during orphan abort" — `resolve_all`'s own `.ok_or` sites) — a
    /// confirmed DECIDE (unlike a confirmed STAGE) fully materializes every
    /// participant's derived writes, so retrying one of these with a fresh
    /// `TxnId` is exactly the double-materialize race this amendment
    /// exists to close, and doing so reproduced the literal
    /// `delivered=146/144` duplicate-pair signature live during this
    /// amendment's own proof-soak. An allowlist fails safe against every
    /// message this file does not yet know about (a future call site's own
    /// new `"; retry"` wording included) by construction — a denylist fails
    /// unsafe against exactly that.
    ///
    /// **On `StageOutcome::Fenced`**: its own message text names three
    /// possible causes ("a stale route, an already-sealed/out-of-fence
    /// range, or a concurrent in-doubt-recovery decision") with no way to
    /// tell which fired. Tracing `animus_cp_data::lib.rs`'s apply arm
    /// (`already_decided`, the only source of the third cause) shows it
    /// requires the STORED record's `txn_id` to equal the CURRENT stage
    /// attempt's own — i.e. it can only ever fire for a stage sharing an
    /// identity with a record that already exists, never for a freshly-
    /// minted, never-before-seen `TxnId` (what every retry in this file
    /// mints). A fresh retry hitting `Fenced` is therefore, in practice,
    /// always one of the other two structural causes — provably a no-op.
    pub(crate) fn is_safe_to_retry_fresh(&self) -> bool {
        matches!(self, TxnAbortReason::Other(msg) if
        msg.as_str() == FROZEN_REFUSAL
            || msg.contains("no CP group leader reachable for txn prepare")
            || msg.starts_with("txn prepare: leader-side evaluation failed:")
            || msg.contains(
                "was rejected (a stale route, an already-sealed/out-of-fence range, \
                 or a concurrent in-doubt-recovery decision)",
            ))
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

    /// [`TxnAbortReason::is_ambiguous`] (ADR 0018's issue #298 "deep shape A"
    /// amendment): the two typed variants are always definite (a condition
    /// genuinely evaluated false, or an intent genuinely still blocked past
    /// every retry) regardless of their own message text; an `Other` is
    /// ambiguous exactly when — and only when — it carries the house-wide
    /// `"; retry"` retryability suffix (`decide::read_should_retry` tests the
    /// identical shape for the unrelated CP-read retry loop).
    #[test]
    fn is_ambiguous_classifies_by_the_house_retry_suffix() {
        assert!(
            !TxnAbortReason::ConditionFailed {
                table: "t".into(),
                key: vec![1],
            }
            .is_ambiguous()
        );
        assert!(
            !TxnAbortReason::TransactionConflict {
                table: "t".into(),
                key: vec![1],
            }
            .is_ambiguous()
        );
        assert!(
            TxnAbortReason::Other("CP group leader moved during participant stage; retry".into())
                .is_ambiguous()
        );
        assert!(
            TxnAbortReason::Other("no CP group leader reachable for txn prepare; retry".into())
                .is_ambiguous()
        );
        assert!(
            !TxnAbortReason::Other("txn prepare: writes must be non-empty".into()).is_ambiguous()
        );
        assert!(
            !TxnAbortReason::Other("unexpected reply to forwarded TxnPrepare: Value(None)".into())
                .is_ambiguous()
        );
    }

    /// [`TxnAbortReason::is_safe_to_retry_fresh`] (ADR 0018's issue #298
    /// "deep shape A" amendment, the allowlist correction): only the
    /// provably-pre-propose reasons are retry-eligible; every DECIDE-phase
    /// confirmation loss (which can follow a fully-materialized commit) and
    /// the stage-time leader-moved case (a propose was actually attempted)
    /// must answer `false`, even though both are `is_ambiguous() == true`.
    #[test]
    fn is_safe_to_retry_fresh_is_a_narrow_allowlist_not_a_denylist() {
        // Allowlisted: provably nothing was proposed for this transaction.
        assert!(TxnAbortReason::Other(super::FROZEN_REFUSAL.into()).is_safe_to_retry_fresh());
        assert!(
            TxnAbortReason::Other("no CP group leader reachable for txn prepare; retry".into())
                .is_safe_to_retry_fresh()
        );
        assert!(
            TxnAbortReason::Other(
                "txn prepare: leader-side evaluation failed: InternalServerError: leader-side \
                 old-image read failed: CP group leader moved; retry"
                    .into()
            )
            .is_safe_to_retry_fresh()
        );
        assert!(
            TxnAbortReason::Other(
                "txn prepare: stage on table `t` was rejected (a stale route, an \
                 already-sealed/out-of-fence range, or a concurrent in-doubt-recovery \
                 decision); retry"
                    .into()
            )
            .is_safe_to_retry_fresh()
        );

        // NOT allowlisted: a propose was actually attempted (stage-time) or
        // a decision may have actually applied (decide-time) — retrying
        // fresh here is exactly the double-materialize race this amendment
        // closes.
        assert!(
            !TxnAbortReason::Other("CP group leader moved during anchor stage; retry".into())
                .is_safe_to_retry_fresh()
        );
        assert!(
            !TxnAbortReason::Other("CP group leader moved during participant stage; retry".into())
                .is_safe_to_retry_fresh()
        );
        assert!(
            !TxnAbortReason::Other("CP group leader moved during anchor commit; retry".into())
                .is_safe_to_retry_fresh()
        );
        assert!(
            !TxnAbortReason::Other("CP group leader moved during anchor abort; retry".into())
                .is_safe_to_retry_fresh()
        );
        assert!(
            !TxnAbortReason::Other("CP group leader moved during orphan abort; retry".into())
                .is_safe_to_retry_fresh()
        );
        assert!(
            !TxnAbortReason::Other("CP group leader moved after decide; retry".into())
                .is_safe_to_retry_fresh()
        );
        assert!(
            !TxnAbortReason::Other("CP group leader moved during anchor decide; retry".into())
                .is_safe_to_retry_fresh()
        );
        assert!(
            !TxnAbortReason::Other("CP group leader moved during resolve; retry".into())
                .is_safe_to_retry_fresh()
        );

        // The two typed variants are never retry-eligible either way.
        assert!(
            !TxnAbortReason::ConditionFailed {
                table: "t".into(),
                key: vec![1],
            }
            .is_safe_to_retry_fresh()
        );
        assert!(
            !TxnAbortReason::TransactionConflict {
                table: "t".into(),
                key: vec![1],
            }
            .is_safe_to_retry_fresh()
        );
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
///
/// Generic over `E: Env` (ADR 0061 rung C5 step 1) — the default binds `E =
/// ProdEnv`, so every pre-existing bare `CpGroup` reference in this crate
/// (~180 call sites, none of which name a type parameter) keeps compiling
/// unchanged; this default is this checkpoint's "type alias at the
/// instantiation site" equivalent, containing the blast radius to this
/// definition rather than touching every call site.
#[derive(Clone)]
enum CpGroup<E: Env = ProdEnv> {
    /// Durable on-disk LSM (default; survives a restart).
    Lsm(RaftKvNode<E, LsmEngine<E>>),
    /// Volatile in-memory engine (ephemeral runs).
    Mem(RaftKvNode<E, MemoryEngine>),
}

impl<E: Env> CpGroup<E> {
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

    /// This replica's current engine watermark — the backup capture
    /// driver's own snapshot-pin primitive (ADR 0059 §4). See
    /// [`RaftKvNode::engine_latest_version`].
    pub(crate) fn engine_latest_version(&self) -> u64 {
        match self {
            CpGroup::Lsm(n) => n.engine_latest_version(),
            CpGroup::Mem(n) => n.engine_latest_version(),
        }
    }

    /// A snapshot-pinned, intent-resolved, resumable-cursor sweep of a kind
    /// scope — the backup capture driver's own read primitive (ADR 0059
    /// §4/§5). See [`RaftKvNode::local_scan_kind_snapshot`].
    pub(crate) async fn local_scan_kind_snapshot(
        &self,
        kind: u8,
        start: &[u8],
        version_ceiling: u64,
        limit: usize,
    ) -> (Vec<(Vec<u8>, Vec<u8>, u64)>, Option<Vec<u8>>) {
        match self {
            CpGroup::Lsm(n) => {
                n.local_scan_kind_snapshot(kind, start, version_ceiling, limit)
                    .await
            }
            CpGroup::Mem(n) => {
                n.local_scan_kind_snapshot(kind, start, version_ceiling, limit)
                    .await
            }
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
    /// [`RaftKvNode::is_frozen`] and [`decide::frozen_refusal`].
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
    fn env(&self) -> &E {
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
            // Best-effort admin diagnostic only — collapsing "not served"
            // and "genuinely no record" (`flatten`) is fine here, unlike
            // `ClientCtx::txn_recover`'s own use of this same primitive,
            // which must keep the two distinguishable (see
            // `RaftKvNode::txn_record_view`'s doc, issue #298 shape B fix).
            let view: Option<TxnRecordView> = match self {
                CpGroup::Lsm(n) => n.txn_record_view(&record_key).await,
                CpGroup::Mem(n) => n.txn_record_view(&record_key).await,
            }
            .flatten();
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
    /// See [`RaftKvNode::txn_record_view`] — outer `None` = not served,
    /// `Some(None)` = definitively no record, `Some(Some(view))` = found.
    async fn txn_record_view(
        &self,
        record_key: &[u8],
    ) -> Option<Option<animus_cp_data::TxnRecordView>> {
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
enum CpRoute<E: Env = ProdEnv> {
    /// This node hosts the current leader — serve from `leader` directly.
    Local(CpGroup<E>),
    /// Forward to the leader's node at this client-API address (ADR 0017 #3b).
    Forward(String),
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
/// deadline (`Superseded` — see [`decide::confirm_wait_is_futile`]), or
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

// `FROZEN_REFUSAL` moved to `decide` (ADR 0061 A6) alongside the
// `frozen_refusal` predicate it belongs to; imported below so every
// pre-existing bare reference in this file keeps compiling unchanged.
use decide::FROZEN_REFUSAL;

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
    /// **animusd console** (ADR 0052's "AnimusDB Data Console") — a DynamoDB-shaped data app for
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
    /// This node's advertised hostname (ADR 0060's advertise/dial split),
    /// shared across every role/port this entry binds — `None` (every
    /// existing config, `#[serde(default)]`) means today's behavior
    /// unchanged: every `NodeAddrs` field this node self-registers is
    /// derived straight from the bind address itself
    /// (`bind_addr.to_string()`). `Some(host)` means self-registration
    /// instead advertises `format!("{host}:{port}")` per port — the bind
    /// address a listener actually opens on stays numeric and untouched
    /// (e.g. `0.0.0.0:P`, the shape a Kubernetes pod binds), only what
    /// this node tells the rest of the cluster to *dial* it at changes.
    /// This is what lets a pod that binds a wildcard/pod-IP address still
    /// be reached by its own stable DNS name after a reschedule (the IP
    /// changes; the name doesn't) — see the `--advertise-host` CLI flag
    /// and the `RoleAddrs -> NodeAddrs` self-registration call sites
    /// (`BoundNode`/`BoundControlNode`/`BoundDataNode::start_*`) for where
    /// this actually gets used. One shared host for every role/port this
    /// entry binds, deliberately not per-port — a real deployment
    /// advertises one pod identity, not six.
    #[serde(default)]
    pub advertise_host: Option<String>,
}

/// Fallback endpoint for configs written before a field existed: an ephemeral
/// port on the loopback (the real port is learned after bind).
fn default_ephemeral_addr() -> SocketAddr {
    SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0)
}

/// This node's own advertised `host:port` for one of its bound addresses
/// (ADR 0060's advertise/dial split, [`RoleAddrs::advertise_host`]'s own
/// doc): `Some(host)` means `format!("{host}:{port}")`, `bind_addr`'s own
/// port with the advertised host in place of the bind address itself;
/// `None` (every existing config) is byte-identical to before this ADR —
/// `bind_addr.to_string()`, unchanged. Every self-registration call site
/// (`NodeAddrs` construction in `BoundNode`/`BoundControlNode`/
/// `BoundDataNode::start_*`, the join chain's own `mine: NodeAddrs`, a
/// node's own peer-book entry) and every `ClusterConfig`-derived static
/// route/peer-book seed (`ClusterConfig::peer_book`/the `client_route`/
/// `intra_route` builders in `run_node_with`/`run_node_control`/
/// `run_node_data`/`run_node_growth`) goes through this — the one place a
/// bind address becomes the string a peer actually dials.
pub(crate) fn advertised_addr(advertise_host: Option<&str>, bind_addr: SocketAddr) -> String {
    match advertise_host {
        Some(host) => format!("{host}:{}", bind_addr.port()),
        None => bind_addr.to_string(),
    }
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
    /// animusd console's own listener (ADR 0052's "AnimusDB Data Console") — a combined node
    /// hosts CP-data tablets, so it always binds one; see
    /// [`console`](crate::console)'s module doc.
    console_listener: TcpListener,
    console_addr: SocketAddr,
    /// This node's advertised hostname (ADR 0060), from the [`RoleAddrs`]
    /// [`Node::bind`] was given — see [`advertised_addr`] and
    /// [`RoleAddrs::advertise_host`]'s own doc.
    advertise_host: Option<String>,
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
    /// This node's own deployment role (ADR 0035; ADR 0040 PR1 — no longer
    /// inferred from `control_id`/`raftkv_id` presence, since there is only
    /// one id now): `"control"`/`"data"`/`"combined"`, stamped literally by
    /// whichever `start_*` assembled this node — the same string it also
    /// self-registers into replicated `NodeAddrs.role`.
    pub(crate) role: &'static str,
    /// The control-plane Raft group (all control ids).
    pub(crate) control_ids: Vec<NodeId>,
    /// The static peer address book this node was started with.
    pub(crate) peers: BTreeMap<NodeId, String>,
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

/// Project the replicated schema catalog into animusd console's own
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

/// Project one table's full configuration for animusd console's table page
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

/// animusd console's mutating-endpoint seam (ADR 0052 PR3, widened by PR6's
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
    client_route: BTreeMap<NodeId, String>,
    intra_route: BTreeMap<NodeId, String>,
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
    // animusd console (ADR 0052's "AnimusDB Data Console") — `None` on a control-only node
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
    /// `addr` is this node's own advertised `host:port` (ADR 0060) — see
    /// [`advertised_addr`].
    pub fn peer_entries(&self) -> [(NodeId, String); 1] {
        [(
            self.id.clone(),
            advertised_addr(self.advertise_host.as_deref(), self.internal_addr),
        )]
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
        peers: BTreeMap<NodeId, String>,
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
        peers: BTreeMap<NodeId, String>,
        control_ids: Vec<NodeId>,
        data_ids: Vec<NodeId>,
        backend: StorageBackend,
        edge: ClusterEdgeState,
        client_route: BTreeMap<NodeId, String>,
        intra_route: BTreeMap<NodeId, String>,
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
        peers: BTreeMap<NodeId, String>,
        control_ids: Vec<NodeId>,
        data_ids: Vec<NodeId>,
        backend: StorageBackend,
        edge: ClusterEdgeState,
        client_route: BTreeMap<NodeId, String>,
        intra_route: BTreeMap<NodeId, String>,
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
            pitr_janitor::DEFAULT_PITR_SNAPSHOT_CADENCE,
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
    /// node's `ClientCtx::trigger_split` proposes — `SplitMode::default()`
    /// (every caller above this layer) is `InPlace` since rung 4 layer 2.
    /// See [`SplitMode`]'s own doc.
    ///
    /// `backup_store_config` (ADR 0059 §1) selects this node's second,
    /// backup-dedicated [`BackupStoreHandle`] — `BackupStoreConfig::Cluster`
    /// (every caller above this layer) is the default K-replicated store;
    /// `--config`/`--node`'s and `--cluster N`'s own `--backup-store
    /// cluster|fs:PATH` CLI flag threads through here. **Plumbing only**
    /// (ADR 0059 Train 1 PR②) — nothing yet reads or writes through the
    /// resulting handle.
    ///
    /// `pitr_snapshot_cadence` (ADR 0059 §9/§10, Train 3) is `pitr_janitor::
    /// pitr_snapshot_loop`'s own periodic-base-snapshot interval —
    /// `pitr_janitor::DEFAULT_PITR_SNAPSHOT_CADENCE` (6 hours; every caller
    /// above this layer) is the production default, the identical
    /// "no CLI knob yet" shape that module's own doc already names. A test
    /// that needs a PITR base snapshot to actually exist within its own
    /// budget (`RestoreTableToPointInTime`'s own e2e coverage) calls
    /// [`run_node_with_streams_and_pitr_snapshot_cadence`] instead of
    /// waiting out six hours.
    #[allow(clippy::too_many_arguments)]
    pub async fn start_with_growth(
        self,
        peers: BTreeMap<NodeId, String>,
        control_ids: Vec<NodeId>,
        data_ids: Vec<NodeId>,
        backend: StorageBackend,
        edge: ClusterEdgeState,
        client_route: BTreeMap<NodeId, String>,
        intra_route: BTreeMap<NodeId, String>,
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
        pitr_snapshot_cadence: Duration,
    ) -> std::io::Result<Node> {
        // ProdEnv's peer book is now keyed by address string (advertise/dial
        // split groundwork) — this boundary still deals in `SocketAddr`
        // until a later change moves the surrounding route/peer plumbing
        // itself onto strings.
        self.env.set_peers(
            peers
                .iter()
                .map(|(id, addr)| (id.clone(), addr.to_string()))
                .collect(),
        );
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
                    internal: advertised_addr(self.advertise_host.as_deref(), my_addr),
                    client: advertised_addr(self.advertise_host.as_deref(), my_client_addr),
                    admin: advertised_addr(self.advertise_host.as_deref(), my_admin_addr),
                    intra: advertised_addr(self.advertise_host.as_deref(), my_intra_addr),
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
            static_peers
                .iter()
                .map(|(id, addr)| (id.clone(), addr.to_string()))
                .collect(),
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
            let seeds: Vec<String> = control_ids
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

        // The on-demand backup capture driver (ADR 0059 §4/§5/§6, Train 1
        // PR③): sweeps a `Creating` backup's pinned/re-planned tablets into
        // chunked backup-store objects. Same "run everywhere, self-gate per
        // tablet on leadership" shape as the GSI drain/TTL reaper above.
        tasks.push(tokio::spawn(backup_capture::backup_capture_loop(
            ctx.clone(),
        )));

        // The restore driver (ADR 0059 §7, Train 2): seeds a `Seeding`
        // restore's single `Building` tablet from its backup's data
        // objects, then activates it. Same "run everywhere, self-gate per
        // tablet on leadership" shape as the backup capture driver above.
        tasks.push(tokio::spawn(backup_restore::backup_restore_loop(
            ctx.clone(),
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

        // The on-demand backup completion aggregator (ADR 0059 §3/§4, Train
        // 1 PR③): completes/fails a `Creating` backup once every pinned (or
        // re-planned) tablet has reported, or a stuck-timeout elapses.
        // Control-plane-leader-only (self-gated every tick,
        // `backup_completion.rs`'s own doc) — spawned unconditionally here,
        // exactly like the segment janitor/backfill aggregator above.
        tasks.push(tokio::spawn(backup_completion::backup_completion_loop(
            ctx.clone(),
        )));

        // The on-demand backup janitor (ADR 0059 §3, Train 1 PR④): two-phase
        // reclaim of a `DeleteBackup`-marked (or stuck-`Failed`) backup's
        // store objects, then the row itself. Control-plane-leader-only
        // (self-gated every tick, `backup_janitor.rs`'s own doc) — spawned
        // unconditionally here, exactly like the segment janitor/backfill/
        // backup-completion aggregators above.
        tasks.push(tokio::spawn(backup_janitor::backup_janitor_loop(
            ctx.clone(),
        )));

        // PITR periodic base snapshots + retention (ADR 0059 §9, Train 3):
        // two control-plane-leader-only loops, mirroring the segment/backup
        // janitors above exactly (self-gated every tick,
        // `pitr_janitor.rs`'s own doc) — spawned unconditionally here.
        tasks.push(tokio::spawn(pitr_janitor::pitr_snapshot_loop(
            ctx.clone(),
            pitr_snapshot_cadence,
        )));
        tasks.push(tokio::spawn(pitr_janitor::pitr_janitor_loop(
            ctx.clone(),
            pitr_janitor::DEFAULT_PITR_RETENTION,
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
    /// `None` on a control-only node (ADR 0052) — the animusd console
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
            advertise_host: addrs.advertise_host,
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
            advertise_host: addrs.advertise_host,
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
            advertise_host: addrs.advertise_host,
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

    /// The address animusd console listens on (ADR 0052).
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
    /// See [`BoundNode::advertise_host`]'s doc.
    advertise_host: Option<String>,
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

    /// `(id, addr)` — this node's entry in the cluster's peer book. `addr`
    /// is this node's own advertised `host:port` (ADR 0060) — see
    /// [`advertised_addr`].
    pub fn peer_entry(&self) -> (NodeId, String) {
        (
            self.id.clone(),
            advertised_addr(self.advertise_host.as_deref(), self.internal_addr),
        )
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
    /// connected `BeginSplit`/`BeginSplitInPlace` propose) — no explicit
    /// caller override picks up `SplitMode::default()` = `InPlace` since
    /// rung 4 layer 2; `Copy` stays selectable.
    ///
    /// # Errors
    /// Propagates a failure to open the dedicated engine (LSM backend only).
    #[allow(clippy::too_many_arguments)]
    pub async fn start_control_with(
        self,
        peers: BTreeMap<NodeId, String>,
        control_ids: Vec<NodeId>,
        client_route: BTreeMap<NodeId, String>,
        intra_route: BTreeMap<NodeId, String>,
        cluster_admin_addrs: Vec<SocketAddr>,
        backend: StorageBackend,
        orphan_sweep_after: Duration,
        split_mode: SplitMode,
    ) -> std::io::Result<Node> {
        // ProdEnv's peer book is now keyed by address string (advertise/dial
        // split groundwork) — this boundary still deals in `SocketAddr`
        // until a later change moves the surrounding route/peer plumbing
        // itself onto strings.
        self.env.set_peers(
            peers
                .iter()
                .map(|(id, addr)| (id.clone(), addr.to_string()))
                .collect(),
        );
        let envs = vec![self.env.clone()];

        let admin_info = Arc::new(AdminInfo {
            node_id: Some(self.id.clone()),
            internal_addr: Some(self.internal_addr),
            client_addr: self.client_addr,
            dynamo_addr: None,
            admin_addr: self.admin_addr,
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
                    internal: advertised_addr(self.advertise_host.as_deref(), self.internal_addr),
                    client: advertised_addr(self.advertise_host.as_deref(), self.client_addr),
                    admin: advertised_addr(self.advertise_host.as_deref(), self.admin_addr),
                    intra: advertised_addr(self.advertise_host.as_deref(), self.intra_addr),
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
        tasks.push(tokio::spawn(peer_sync_loop(
            ctx.clone(),
            sync_env,
            peers
                .iter()
                .map(|(id, addr)| (id.clone(), addr.to_string()))
                .collect(),
        )));

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

        // The on-demand backup completion aggregator (ADR 0059 §3/§4, Train
        // 1 PR③): a control-only node can genuinely become the control-plane
        // leader (ADR 0035 split deployment), so it needs this loop too —
        // *failing* a stuck backup needs only `Metadata`, but *completing*
        // one needs a `BackupStoreHandle` to durably `put` the manifest,
        // which no control-only node provisions — see
        // `backup_completion.rs`'s own doc for the documented gap this
        // leaves (a pure split deployment's control-only leader marks a
        // fully-captured backup ready but cannot itself flip it to
        // `Available`, until a data-capable node takes the lead instead).
        tasks.push(tokio::spawn(backup_completion::backup_completion_loop(
            ctx.clone(),
        )));

        // The on-demand backup janitor (ADR 0059 §3, Train 1 PR④): a
        // control-only node can genuinely become the control-plane leader
        // (ADR 0035 split deployment), so it needs this loop too — the
        // *mark* half (a wire `DeleteBackup` proposing `MarkBackupDeleted`)
        // needs only `Metadata`, but the actual object reclaim needs a
        // `BackupStoreHandle`, which no control-only node provisions — see
        // `backup_janitor.rs`'s own doc for the documented gap this leaves
        // (identical shape to the segment janitor's own, above): rows
        // accumulate marked-but-unreclaimed until a data-capable node takes
        // the lead instead.
        tasks.push(tokio::spawn(backup_janitor::backup_janitor_loop(
            ctx.clone(),
        )));

        // PITR periodic base snapshots + retention (ADR 0059 §9, Train 3): a
        // control-only node can genuinely become the control-plane leader
        // (ADR 0035 split deployment), so it needs both loops too — the
        // identical documented gap as the segment/backup janitors above:
        // marking needs only `Metadata`, but the snapshot loop's own
        // `BeginBackup` capture and the retention loop's own segment-object
        // reclaim both need a data role no control-only node provisions
        // (see `pitr_janitor.rs`'s own doc).
        tasks.push(tokio::spawn(pitr_janitor::pitr_snapshot_loop(
            ctx.clone(),
            pitr_janitor::DEFAULT_PITR_SNAPSHOT_CADENCE,
        )));
        tasks.push(tokio::spawn(pitr_janitor::pitr_janitor_loop(
            ctx.clone(),
            pitr_janitor::DEFAULT_PITR_RETENTION,
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
    /// animusd console's own listener (ADR 0052's "AnimusDB Data Console") — a data-only
    /// node hosts real CP-data tablets, so it always binds one; see
    /// [`console`](crate::console)'s module doc.
    console_listener: TcpListener,
    console_addr: SocketAddr,
    /// See [`BoundNode::advertise_host`]'s doc.
    advertise_host: Option<String>,
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

    /// The address animusd console listens on (ADR 0052).
    pub fn console_addr(&self) -> SocketAddr {
        self.console_addr
    }

    /// The address the intra-cluster RPC endpoint listens on (ADR 0047).
    pub fn intra_addr(&self) -> SocketAddr {
        self.intra_addr
    }

    /// `(id, addr)` — this node's entry in the cluster's *raftkv* peer
    /// book (the [`BoundNode::peer_entries`] dual, minus the `control` entry
    /// a data-only node has none of). `addr` is this node's own advertised
    /// `host:port` (ADR 0060) — see [`advertised_addr`].
    pub fn peer_entry(&self) -> (NodeId, String) {
        (
            self.id.clone(),
            advertised_addr(self.advertise_host.as_deref(), self.internal_addr),
        )
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
        peers: BTreeMap<NodeId, String>,
        control_ids: Vec<NodeId>,
        control_seeds: Vec<String>,
        backend: StorageBackend,
        edge: ClusterEdgeState,
        client_route: BTreeMap<NodeId, String>,
        intra_route: BTreeMap<NodeId, String>,
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
        peers: BTreeMap<NodeId, String>,
        control_ids: Vec<NodeId>,
        control_seeds: Vec<String>,
        backend: StorageBackend,
        edge: ClusterEdgeState,
        client_route: BTreeMap<NodeId, String>,
        intra_route: BTreeMap<NodeId, String>,
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
    /// `SplitMode::default()` (`InPlace` since rung 4 layer 2) contract.
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
        peers: BTreeMap<NodeId, String>,
        control_ids: Vec<NodeId>,
        control_seeds: Vec<String>,
        backend: StorageBackend,
        edge: ClusterEdgeState,
        client_route: BTreeMap<NodeId, String>,
        intra_route: BTreeMap<NodeId, String>,
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
        // ProdEnv's peer book is now keyed by address string (advertise/dial
        // split groundwork) — this boundary still deals in `SocketAddr`
        // until a later change moves the surrounding route/peer plumbing
        // itself onto strings.
        self.env.set_peers(
            peers
                .iter()
                .map(|(id, addr)| (id.clone(), addr.to_string()))
                .collect(),
        );
        let static_peers = peers;
        let sync_env = self.env.clone();
        let hook_env = self.env.clone();
        let hb_env = self.env.clone();
        let my_id = self.id.clone();
        let my_addr = self.internal_addr;
        let my_client_addr = self.client_addr;
        let my_admin_addr = self.admin_addr;
        let my_intra_addr = self.intra_addr;

        let control = ControlHandle::Remote(RemoteControlClient::new(
            control_seeds.clone(),
            AnimusdRelayClient,
            CLIENT_TIMEOUT,
        ));

        let admin_info = Arc::new(AdminInfo {
            node_id: Some(self.id.clone()),
            internal_addr: Some(self.internal_addr),
            client_addr: self.client_addr,
            dynamo_addr: Some(self.dynamo_addr),
            admin_addr: self.admin_addr,
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
                    internal: advertised_addr(self.advertise_host.as_deref(), my_addr),
                    client: advertised_addr(self.advertise_host.as_deref(), my_client_addr),
                    admin: advertised_addr(self.advertise_host.as_deref(), my_admin_addr),
                    intra: advertised_addr(self.advertise_host.as_deref(), my_intra_addr),
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
            static_peers
                .iter()
                .map(|(id, addr)| (id.clone(), addr.to_string()))
                .collect(),
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

        // The on-demand backup capture driver (ADR 0059 §4/§5/§6, Train 1
        // PR③) — same shape as the GSI drain/TTL reaper just above. No
        // completion aggregator on this data-only path: a data-only node's
        // control handle is `ControlHandle::Remote` (never a genuine
        // control-plane leader), so `backup_completion_loop`'s own
        // `ctx.edge.leader_handle()` self-gate would always be `None` here
        // — spawning it would be pure dead weight, the same reasoning
        // `index_backfill_loop`/`segment_janitor_loop` already apply by
        // their own absence from this spawn site.
        tasks.push(tokio::spawn(backup_capture::backup_capture_loop(
            ctx.clone(),
        )));

        // The restore driver (ADR 0059 §7, Train 2) — same shape/reasoning
        // as the backup capture driver just above (data-role-only, no
        // control-plane-leader dependency).
        tasks.push(tokio::spawn(backup_restore::backup_restore_loop(
            ctx.clone(),
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
///
/// Generic over `E: Env` (ADR 0061 rung C5 step 1), same default-binds-
/// `ProdEnv` shape as [`CpGroup`]/[`SharedEngine`] — the `raftkv` field is
/// the one that actually varies with `E` (it stores `CpGroup<E>` handles);
/// `control`'s `RaftNode<ProdEnv>` stays hardcoded (the control-plane Raft
/// binding is not part of this rung's cut — see `ClientCtx::control`'s own
/// note).
#[derive(Clone)]
pub struct ClusterEdgeState<E: Env = ProdEnv> {
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
    raftkv: Arc<Mutex<BTreeMap<TabletId, Vec<CpGroup<E>>>>>,
}

impl<E: Env> Default for ClusterEdgeState<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: Env> ClusterEdgeState<E> {
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
    fn register_raftkv(&self, tablet: TabletId, cp: CpGroup<E>) {
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
    fn unregister_raftkv(&self, tablet: TabletId, member: NodeId) -> Option<CpGroup<E>> {
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
    fn halt_hosted_cp_groups(&self) -> Vec<CpGroup<E>> {
        let groups: Vec<CpGroup<E>> = self
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
    fn cp_leader(&self, tablet: TabletId) -> Option<CpGroup<E>> {
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
    fn local_cp(&self, tablet: TabletId) -> Option<CpGroup<E>> {
        self.raftkv
            .lock()
            .expect("raftkv handles poisoned")
            .get(&tablet)?
            .first()
            .cloned()
    }

    /// Every CP group this node hosts, as `(tablet, group)` pairs in tablet order
    /// — for the admin `/admin/raftkv` view (ADR 0020). Clones the cheap handles.
    fn hosted_groups(&self) -> Vec<(TabletId, CpGroup<E>)> {
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
///
/// **Consumed since ADR 0059 Train 1 PR③**: the per-tablet capture driver
/// (`backup_capture.rs`) `put`s chunked data objects through this handle,
/// and the completion aggregator (`backup_completion.rs`) `put`s the
/// manifest object — see each module's own doc.
#[derive(Clone)]
pub(crate) enum BackupStoreHandle {
    Cluster(animus_cp_data::cluster_segment_store::ClusterSegmentStore<ProdEnv, FsSegmentStore>),
    Fs(FsSegmentStore),
}

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
    /// ignored for the single-directory `Fs` opt-in). **Unused until
    /// Train 2** (`RestoreTableFromBackup` is this method's first reader) —
    /// left in rather than stubbed, per this type's own module-level doc.
    #[allow(dead_code)]
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

    /// Fetch a backup object's bytes from **any** reachable copy — the
    /// restore driver's own read primitive (ADR 0059 §7, Train 2). Unlike
    /// [`get`](Self::get), this needs no recorded `replicas` list (no
    /// backup object carries one, see [`get`](Self::get)'s own doc and
    /// `backup_janitor.rs`'s module doc for why): it goes through the
    /// trait's own [`animus_env::SegmentStore::get`], which for `Cluster`
    /// tries the local copy first, then every one of the store's *current*
    /// placement candidates — a best-effort "ask any node" contract,
    /// exactly what a restore reading immutable, already-`Available`
    /// backup objects needs (this is the identical primitive
    /// `list_local`/`delete_local` deliberately do NOT use, since those two
    /// are scoped local-only by the janitor's own design; a read has no
    /// such constraint).
    pub(crate) async fn get_any(&self, id: &str) -> std::io::Result<Option<Vec<u8>>> {
        use animus_env::SegmentStore;
        match self {
            BackupStoreHandle::Cluster(c) => c.get(id).await,
            BackupStoreHandle::Fs(fs) => fs.get(id).await,
        }
    }

    /// Delete a backup object at every one of `replicas` — mirrors
    /// [`SegmentStoreHandle::delete_sealed`]'s exact contract. **Unused
    /// until the retention janitor's reclaim phase** (ADR 0059 §3/§9, a
    /// later train) — left in rather than stubbed, per this type's own
    /// module-level doc.
    #[allow(dead_code)]
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
    /// what backup data exists). **Consumed since Train 1 PR④** by the
    /// backup janitor's own reclaim sweep (`backup_janitor.rs`) — see that
    /// module's own doc for why a *local-only* sweep is this train's
    /// deliberate, documented simplification for backup object reclaim
    /// specifically (unlike the segment janitor's own cataloged-row phase,
    /// a backup object carries no recorded per-object `replicas` list to
    /// reclaim against directly).
    pub(crate) async fn list_local(&self, prefix: &str) -> std::io::Result<Vec<String>> {
        use animus_env::SegmentStore;
        match self {
            BackupStoreHandle::Cluster(c) => c.local().list(prefix).await,
            BackupStoreHandle::Fs(fs) => fs.list(prefix).await,
        }
    }

    /// Delete `id` from **this node's own local** backup directory only —
    /// the backup janitor's own reclaim step (`backup_janitor.rs`), mirroring
    /// [`SegmentStoreHandle::delete_local`]'s identical "no recorded
    /// `replicas` to consult" shape and its own doc's reasoning.
    pub(crate) async fn delete_local(&self, id: &str) -> std::io::Result<()> {
        use animus_env::SegmentStore;
        match self {
            BackupStoreHandle::Cluster(c) => c.local().delete(id).await,
            BackupStoreHandle::Fs(fs) => fs.delete(id).await,
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
    /// above, built from its own `--backup-store` CLI knob. Consumed by the
    /// per-tablet capture driver (`backup_capture.rs`) and the completion
    /// aggregator (`backup_completion.rs`) since ADR 0059 Train 1 PR③.
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
///
/// Generic over `E: Env` (ADR 0061 rung C5 step 1), same default-binds-
/// `ProdEnv` shape as [`CpGroup`]/[`SharedEngine`]/[`ClusterEdgeState`] —
/// see those types' docs. **Also generic over `R: RelayClient` (ADR 0061
/// rung C5 step 3a, the sixth 2026-08-28 amendment)**: `control` is now
/// [`GenericControlHandle<E, R>`] rather than this crate's fixed
/// `ProdEnv`/`AnimusdRelayClient`-bound `control_handle::ControlHandle`
/// alias — `schema.rs`'s `watch_metadata` and `forwarding.rs`'s leader
/// routing both read `self.control` from inside a `ClientCtx<E, R>`-generic
/// `impl` block, so the field itself has to be generic too. Both `E` and `R`
/// default (`ProdEnv`/[`AnimusdRelayClient`]), so every pre-existing bare
/// `ClientCtx` reference across this crate (`admin.rs`, `dynamo.rs`, the
/// background loops, `tests/`) keeps compiling unchanged — same
/// default-type-parameter containment step 1 used for `E` alone. `R` needed
/// its own `Clone + Send + Sync + 'static` supertrait bounds added to
/// [`RelayClient`] itself (`animus-node`) for `ClientCtx<E, R>`'s own
/// `#[derive(Clone)]` and its one `env.spawn_task` capturing a cloned
/// `ClientCtx` (`txn_coordinator.rs`'s `cp_txn`) to typecheck generically —
/// the same `Clone + Send + Sync + 'static` shape `Env`'s own supertrait
/// bound already carries, so this mirrors an established precedent rather
/// than inventing a new one.
#[derive(Clone)]
pub(crate) struct ClientCtx<E: Env = ProdEnv, R: RelayClient = AnimusdRelayClient> {
    control: GenericControlHandle<E, R>,
    pub(crate) edge: ClusterEdgeState<E>,
    /// This node's one internal `ProdEnv` (ADR 0040 PR1) — every role's
    /// clone of the same handle. The **only** `Env`-seam access point this
    /// context exposes to the wire edges: e.g. minting a DynamoDB Streams
    /// label at enable time (ADR 0042 §4) goes through `ctx.env.now()`,
    /// never `std::time` directly (ADR 0003's determinism rule — this crate
    /// is production-only `ProdEnv` wiring, but the seam convention still
    /// holds so nothing here quietly grows a second, ambient time source).
    pub(crate) env: E,
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
    client_route: Arc<Mutex<BTreeMap<NodeId, String>>>,
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
    intra_route: Arc<Mutex<BTreeMap<NodeId, String>>>,
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
    pub(crate) control_storage: Option<SharedEngine<E>>,
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
    /// residue) — `InPlace` (every constructor below that doesn't take an
    /// explicit override defaults to `SplitMode::default()`, `InPlace`
    /// since rung 4 layer 2) is the ADR 0058 in-place single-entry atomic
    /// fork; `Copy` is the original ADR 0050 workflow, still selectable.
    /// See [`SplitMode`]'s own doc. Threaded
    /// from the `--split-mode {copy,inplace}` CLI flag (`--config`/
    /// `--cluster N` only, mirroring `--quiesce-after`'s own scope) —
    /// plain per-node config, not gated by [`DataRole`], since a
    /// control-only node's `trigger_split` calls need it too.
    pub(crate) split_mode: SplitMode,
}

impl<E: Env, R: RelayClient> ClientCtx<E, R> {
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

    // The futility predicate this used to hold (`confirm_wait_is_futile`)
    // moved to [`decide::confirm_wait_is_futile`] (ADR 0061 A6) — see that
    // function's own doc for the full two-signal rationale (issue #268).

    // ---- multi-participant transactions (ADR 0018 §2/PR4) --------------------

    // ---- in-doubt transaction recovery (ADR 0018 §2/PR5) ------------------

    // `ok_or_err` moved to [`decide::ok_or_err`] (ADR 0061 A6) — a plain
    // `ClientResponse -> Result` map with nothing to gather from `self`.

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
            leader.env().merge_peer(node.clone(), addr.to_string());
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
        leader.env().merge_peer(node.clone(), addr.to_string());
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
async fn peer_sync_loop(ctx: ClientCtx, env: ProdEnv, static_peers: BTreeMap<NodeId, String>) {
    loop {
        let mut book = static_peers.clone();
        let meta = ctx.effective_metadata();
        // `Metadata`'s own address book is already `host:port` strings —
        // ProdEnv's peer book is too (the advertise/dial split groundwork),
        // so both overlays are now straight inserts, no parse/re-stringify
        // boundary crossing at every tick.
        for (id, addr) in meta.cp_member_addrs {
            book.insert(id, addr);
        }
        for (id, addrs) in meta.node_addrs {
            book.insert(id, addrs.internal);
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
/// never-replicated local raft. `client_route`'s value is now the same
/// `host:port` string `Metadata.node_addrs[*].client` already carries — no
/// parse/re-stringify boundary crossing left at this join point.
async fn route_sync_loop(ctx: ClientCtx, static_route: BTreeMap<NodeId, String>) {
    loop {
        let mut book = static_route.clone();
        for (id, addrs) in ctx.effective_metadata().node_addrs {
            book.insert(id, addrs.client);
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
async fn intra_route_sync_loop(ctx: ClientCtx, static_route: BTreeMap<NodeId, String>) {
    loop {
        let mut book = static_route.clone();
        for (id, addrs) in ctx.effective_metadata().node_addrs {
            book.insert(id, addrs.intra);
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
async fn remote_metadata_sync_loop(ctx: ClientCtx, seeds: Vec<String>) {
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
    let remote = RemoteControlClient::with_mirror(
        seeds.clone(),
        ctx.remote_metadata.clone(),
        AnimusdRelayClient,
        CLIENT_TIMEOUT,
    );
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
async fn remote_metadata_watch_loop(remote: RemoteControlClient, seeds: Vec<String>) {
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
        candidates.extend(seeds.iter().cloned());

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
        for addr in &seeds {
            if let ClientResponse::Status {
                metadata,
                leader_hint,
                intra_leader_hint,
                watermark,
                control_voters,
            } = relay_request(addr.clone(), &ClientRequest::Status).await
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
///
/// Generic over `E: Env` (ADR 0061 rung C5 step 1), same default-binds-
/// `ProdEnv` shape as [`CpGroup`] — see that type's doc for why the default
/// parameter, not a rename + separate alias, is this rung's containment
/// mechanism.
#[derive(Clone)]
enum SharedEngine<E: Env = ProdEnv> {
    /// Durable on-disk LSM (default; survives a restart).
    Lsm(LsmEngine<E>),
    /// Volatile in-memory engine (ephemeral runs).
    Mem(MemoryEngine),
}

impl<E: Env> SharedEngine<E> {
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

    /// ADR 0058 Train 2 rung 4 layer 1 — see [`Reconciler::fork_wake`]'s doc.
    async fn fork_wake(&self) {
        match self {
            CpReconciler::Lsm(r) => r.fork_wake().await,
            CpReconciler::Mem(r) => r.fork_wake().await,
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
/// **A third arm, `reconciler.fork_wake()` (ADR 0058 Train 2 rung 4 layer
/// 1)**: resolves the instant any tablet this node currently hosts observes
/// its own local `SplitTablet` fork apply, so this node's own tick fires
/// immediately rather than riding out its own next scheduled wake (a real
/// residual the rung-4 measurement addendum found — a freshly-forked
/// child's first election needs a SECOND voter's own `materialize_split_
/// child` to run before it can win a quorum, and that voter's own
/// materialization used to ride its next poll rather than the fork itself).
/// Harmless to race unconditionally: `CpReconciler::fork_wake` never
/// resolves on its own when this node hosts nothing, and a spurious extra
/// tick is exactly as cheap as the fallback sleep's own — `plan` is pure and
/// idempotent regardless of what woke the loop.
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
            // ADR 0058 Train 2 rung 4 layer 1: wake the instant ANY hosted
            // tablet's own apply task observes a local `SplitTablet` fork,
            // instead of waiting out this loop's own next scheduled tick
            // (previously up to `INPLACE_SPLIT_RECONCILE_INTERVAL`, 50ms, per
            // replica) — see `CpReconciler::fork_wake`'s doc for why this is
            // safe to race unconditionally alongside the two arms above.
            _ = reconciler.fork_wake() => {}
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

/// How long a record may stay `Pending` (measured from its own
/// `created_ts`, past `RECOVERY_GRACE`'s own initial window) before
/// `txn_resolver_loop` treats it as **stuck** and logs+meters once (issue
/// #298 shape B fix) — a generous multiple of `RECOVERY_GRACE` so ordinary
/// grace-window/occasional-inconclusive-verify Pending never trips it: only
/// a record that has been declining a decision (via `txn_recover`'s own
/// "an `Err` from `txn_verify` is never evidence of not-staged" discipline)
/// tick after tick for this long is worth an operator's attention. Purely a
/// liveness signal — correctness never depends on this firing, or on how
/// long it takes to.
const RECOVERY_STUCK_GRACE: Duration = Duration::from_secs(30);

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
    // Driver-local grace tracker for `unresolved_decided()` entries whose
    // `txn_record_view` lookup fails (issue #298 residuals) — same
    // ownership/bounding discipline as `index_drain::change_consumer_loop`'s
    // `first_hot_seen`/`marker_bytes_seen`: per-driver, in-memory only,
    // pruned every tick against whatever `unresolved_decided()` actually
    // reported this tick (never against a fixed universe, since a stuck
    // entry's own tablet may itself retire while it's being tracked — the
    // exact shape this exists for). Keyed by `TxnId` rather than
    // `(tablet, TxnId)`: `unresolved_decided()` is already scoped to one
    // `group`/tablet per call, and a `TxnId` never legitimately appears
    // `Pending`/decided-unresolved on two different tablets' anchors at
    // once, so the flat key is unambiguous and survives a same-node
    // re-host under a different tablet id across a split with no special
    // case. `first_seen` is milliseconds off whichever led group's own
    // `Env::now()` first observed the failure — not wall-clock-comparable
    // across nodes, but this map never leaves this node, so that's fine.
    let mut unresolved_first_seen: BTreeMap<TxnId, u64> = BTreeMap::new();
    // Entries already past grace for which this loop has already logged +
    // metered the "giving up on background resolution for now" signal —
    // separate from `unresolved_first_seen` so that signal fires exactly
    // once per stuck episode instead of every tick thereafter.
    let mut unresolved_stuck_warned: BTreeSet<TxnId> = BTreeSet::new();
    // Stuck-recovery grace tracker (issue #298 shape B fix): `txn_id`s this
    // loop has already logged+metered as stuck past `RECOVERY_STUCK_GRACE`
    // — mirrors the driver-local, tick-pruned memo discipline
    // `index_drain::change_consumer_loop`'s own `first_hot_seen`/
    // `marker_bytes_seen` use (and the identical shape a sibling
    // `unresolved_decided`-lookup-failure tracker uses for the same reason:
    // fire the signal exactly once per stuck episode, not every tick
    // thereafter). Pruned every tick against whatever `pending_txns()`
    // actually reports, so a txn that finally decides (or whose tablet
    // moves off this node) drops out with no explicit cleanup needed.
    let mut stuck_recovery_warned: BTreeSet<TxnId> = BTreeSet::new();
    loop {
        tokio::time::sleep(TXN_RESOLVER_INTERVAL).await;
        // Every `TxnId` `unresolved_decided()` reports this tick, across
        // every hosted group — the live set the two maps above are pruned
        // against once the tick's tablet loop below finishes.
        let mut unresolved_seen_this_tick: BTreeSet<TxnId> = BTreeSet::new();
        let mut pending_seen_this_tick: BTreeSet<TxnId> = BTreeSet::new();
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

            for (txn_id, (record_key, created_ts)) in group.pending_txns() {
                pending_seen_this_tick.insert(txn_id.clone());
                // `created_ts` as the orphan-path hint (issue #298): passing
                // `None` here rested on "`pending_txns` only ever tracks a
                // genuine, locally-anchored `Pending` record, so
                // `txn_recover`'s record-absent branch is unreachable from
                // this caller by construction" — true only as long as the
                // record stays reachable at that same logical position for
                // the whole recovery window. A txn record is an ordinary
                // in-scope logical key of its anchor tablet (`txn.rs`'s own
                // doc), so it rides the identical split clone/trim path
                // every other row does — if a split ever misplaces or drops
                // it (the same class of bug a live base-row investigation
                // under this issue found, still open), `txn_record_view`'s
                // lookup inside `txn_recover` genuinely can fail for a
                // record this loop just enumerated moments ago, and with
                // `None` there is no fallback: the record-absent branch
                // immediately returns `Pending` with no grace-period-then-
                // decide path at all, so this call would retry forever
                // without ever making progress — the exact "stuck reporting
                // TransactionConflict indefinitely" shape observed. Passing
                // the hint costs nothing when the comment's own assumption
                // holds (the record-absent branch simply never runs), and
                // gives the existing, already-reviewed orphan-abort
                // fallback a real timestamp to bound its grace period by
                // when it doesn't.
                match ctx
                    .txn_recover(&table, &record_key, &txn_id, Some(created_ts))
                    .await
                {
                    Ok(TxnDecisionStatus::Pending) => {
                        // Still undecided after this tick's own attempt —
                        // either genuinely within `RECOVERY_GRACE` (routine,
                        // never worth a signal) or `txn_recover` declined on
                        // an inconclusive `txn_verify` (also routine in
                        // isolation — a single split/cutover blip). Only a
                        // record that has been `Pending` this long past its
                        // OWN creation is worth flagging, and only once.
                        let now_ms = group.env().now().0 / 1_000_000;
                        if now_ms.saturating_sub(created_ts.wall_ms)
                            >= RECOVERY_STUCK_GRACE.as_millis() as u64
                            && stuck_recovery_warned.insert(txn_id.clone())
                        {
                            tracing::warn!(
                                tablet = tablet.0,
                                ?txn_id,
                                "txn_resolver_loop: recovery has been unable to reach a \
                                 decision past RECOVERY_STUCK_GRACE (repeated inconclusive \
                                 txn_verify, or a persistently unreachable participant) — \
                                 correctness is unaffected (the record stays safely Pending, \
                                 never wrongly decided); this is a liveness signal only"
                            );
                            if let Some(data) = ctx.data.as_ref() {
                                data.raftkv_metrics
                                    .incr(Metric::CpTxnRecoveryStuckInconclusive);
                            }
                        }
                    }
                    Ok(_) => {
                        stuck_recovery_warned.remove(&txn_id);
                    }
                    Err(e) => {
                        tracing::debug!(
                            tablet = tablet.0,
                            ?txn_id,
                            error = %e,
                            "txn_resolver_loop: recovery push failed this tick"
                        );
                    }
                }
            }
            for (txn_id, (record_key, outcome)) in group.unresolved_decided() {
                unresolved_seen_this_tick.insert(txn_id.clone());
                // `unresolved_decided` only carries `(record_key, outcome)`
                // — re-read the record's own `intent_spans` (every
                // participant table this transaction touched) rather than
                // guess it.
                let Ok(Some(view)) = ctx.txn_record_view(&table, &record_key).await else {
                    // Lookup-failure fallback (issue #298 residuals): unlike
                    // `pending_txns`'s orphan path above, there is no
                    // decision left to make here — `outcome` is already
                    // known (`Committed`/`Aborted`); a failed
                    // `txn_record_view` only means `intent_spans` (which
                    // keys/tables to actually resolve) is unreachable right
                    // now, e.g. because the record's own tablet retired
                    // mid-recovery (the same class of split-clone/trim
                    // hazard `pending_txns`'s own comment names — a txn
                    // record is an ordinary logical key of its anchor
                    // tablet, so it rides the identical path every other
                    // row does). Retrying forever on a permanent case would
                    // burn a tick's work on every hosted group forever with
                    // no bound and no signal — the exact silent-stall shape
                    // this issue is about. So: track first-seen per
                    // `txn_id`, keep quietly retrying (a transient failure —
                    // a route flap, a not-yet-caught-up follower — should
                    // still self-heal next tick), and once
                    // `RECOVERY_GRACE` has passed with no success, log +
                    // meter it once and move on. This never fabricates an
                    // `intent_spans` list to resolve against, so it is
                    // conservative by construction — it can only ever make
                    // this loop STOP claiming background progress it isn't
                    // making; it cannot mis-resolve anything. Correctness
                    // for the stuck transaction's participants still comes
                    // from the independent on-demand foreign-intent
                    // read-path push (ADR 0018 §2/PR5 §3): any reader that
                    // later hits one of its intents directly resolves it
                    // right then, regardless of what this background loop
                    // has given up on.
                    let now_ms = group.env().now().0 / 1_000_000;
                    let first_seen = *unresolved_first_seen
                        .entry(txn_id.clone())
                        .or_insert(now_ms);
                    if now_ms.saturating_sub(first_seen)
                        >= animus_cp_data::RECOVERY_GRACE.as_millis() as u64
                        && unresolved_stuck_warned.insert(txn_id.clone())
                    {
                        tracing::warn!(
                            tablet = tablet.0,
                            ?txn_id,
                            "txn_resolver_loop: unresolved_decided record has \
                             been unreachable past RECOVERY_GRACE — giving up \
                             on background resolution for now (no readable \
                             intent_spans to act on); correctness is still \
                             covered by the on-demand foreign-intent read-path \
                             push (ADR 0018 §2/PR5 §3)"
                        );
                        if let Some(data) = ctx.data.as_ref() {
                            data.raftkv_metrics
                                .incr(Metric::CpTxnUnresolvedDecidedStuck);
                        }
                    }
                    continue; // transient-or-parked — retried next tick regardless
                };
                unresolved_first_seen.remove(&txn_id);
                unresolved_stuck_warned.remove(&txn_id);
                ctx.recovery_resolve(
                    txn_id,
                    record_key,
                    &view.intent_spans,
                    &outcome_to_status(&outcome),
                )
                .await;
            }
        }
        unresolved_first_seen.retain(|id, _| unresolved_seen_this_tick.contains(id));
        unresolved_stuck_warned.retain(|id| unresolved_seen_this_tick.contains(id));
        stuck_recovery_warned.retain(|id| pending_seen_this_tick.contains(id));
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
/// when [`decide::align_split_key`] finds a streamed table's split key rounds down
/// onto the target tablet's own `range.start` — matched by `auto_split_loop`
/// to downgrade its logging (Fork E: skip + meter via
/// [`Metric::StreamSplitSingleTokenSkipped`], never its ordinary "split did
/// not commit" warning, which would otherwise fire every cooldown, forever,
/// for a single-token hot partition that structurally cannot split).
const SPLIT_KEY_NOT_TOKEN_VIABLE: &str =
    "split key rounds onto the tablet's own range start (single-token hot partition)";

// F11 (ADR 0042 §14, Fork D): the split-key token-rounding predicate moved
// to [`decide::align_split_key`] (ADR 0061 A6) — round-trip-pure over a
// `Metadata` snapshot, no `self`/network/lock access. Called from the
// **one** choke point every split proposer funnels through
// ([`ClientCtx::trigger_split`]), so this can never be forgotten by a
// future caller the way the pre-PR2 code (rounding done only inside
// `auto_split_loop`) could be bypassed by the two manual paths (`POST
// /admin/tablet/split`, `ClientRequest::SplitTablet`). See that function's
// own doc for the full F11/Fork E rationale.

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
/// (ADR 0034 — [`decide::byte_weighted_median`], the key that roughly bisects the
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
                decide::byte_weighted_median(&pairs)
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

// ADR 0034: the byte-weighted median moved to
// [`decide::byte_weighted_median`] (ADR 0061 A6) — pure over the materialized
// pairs, no `self`/network/lock access. See that function's own doc for the
// full "closest achievable boundary" rationale.

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
/// median (ADR 0034's [`decide::byte_weighted_median`]) — the same key
/// `auto_split_loop` computes for a byte-configured cluster, reused here for
/// growth PR3's manual `POST /admin/stream/grow` trigger and Fork F's
/// change-rate auto-trigger, neither of which has (or needs) a byte/key
/// **threshold** of its own: an explicit trigger always uses the
/// byte-weighted metric, unconditionally. Returns `None` for fewer than 2
/// distinct keys (no legal interior split point regardless of tokens) —
/// the caller answers [`STREAM_GROW_NO_SPLIT_POINT`] rather than ever
/// calling [`ClientCtx::trigger_split`] with a meaningless key.
async fn median_split_key<E: Env>(group: &CpGroup<E>) -> Option<Vec<u8>> {
    let pairs = group.local_pairs().await;
    if pairs.len() < 2 {
        return None;
    }
    Some(decide::byte_weighted_median(&pairs))
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
        ClientRequest::ForcePitrSeal { .. } => "force_pitr_seal",
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
        ClientRequest::ForcePitrSeal { .. } => ClientResponse::Error(
            "this request is an internal PITR seal-trigger RPC and must be sent wrapped in \
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

/// Bind an `n`-node cluster on `ip` with ephemeral ports and the conventional
/// ids (node `i`, ADR 0040 PR1), each under `dir/node-i`. Every node's
/// `advertise_host` is unset — see
/// [`bind_cluster_with_advertise_host`] for the dev-`--cluster N` variant
/// that sets one.
///
/// # Errors
/// Propagates any bind failure.
pub async fn bind_cluster(
    n: usize,
    ip: std::net::IpAddr,
    dir: impl Into<PathBuf>,
) -> std::io::Result<Vec<BoundNode>> {
    bind_cluster_with_advertise_host(n, ip, dir, None).await
}

/// [`bind_cluster`], with every node's [`RoleAddrs::advertise_host`] set to
/// `advertise_host` (ADR 0060) — the same shared host for every node in the
/// in-process cluster; each still binds its own distinct ephemeral port on
/// `ip`, so `{advertise_host}:{that node's own port}` remains a unique,
/// dialable identity per node. `None` is byte-identical to [`bind_cluster`].
///
/// # Errors
/// Propagates any bind failure.
pub async fn bind_cluster_with_advertise_host(
    n: usize,
    ip: std::net::IpAddr,
    dir: impl Into<PathBuf>,
    advertise_host: Option<String>,
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
            advertise_host: advertise_host.clone(),
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
/// {copy,inplace}` CLI flag threads through here; `SplitMode::default()`
/// (every other wrapper above) is `InPlace` since rung 4 layer 2 — `Copy`
/// stays selectable via the flag pending its own deletion.
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
    let peers: BTreeMap<NodeId, String> = bound.iter().flat_map(BoundNode::peer_entries).collect();
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
    let client_route: BTreeMap<NodeId, String> = bound
        .iter()
        .map(|b| {
            (
                b.id.clone(),
                advertised_addr(b.advertise_host.as_deref(), b.client_addr),
            )
        })
        .collect();
    // The `intra_route` sibling (ADR 0047) — identical static-seed shape,
    // sourced from each bound node's intra address instead of its client one.
    let intra_route: BTreeMap<NodeId, String> = bound
        .iter()
        .map(|b| {
            (
                b.id.clone(),
                advertised_addr(b.advertise_host.as_deref(), b.intra_addr()),
            )
        })
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
                pitr_janitor::DEFAULT_PITR_SNAPSHOT_CADENCE,
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
            advertise_host: None,
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
            advertise_host: None,
        };
        data_bound
            .push(Node::bind_data(config::node_id(i), addrs, dir.join(format!("node-{i}"))).await?);
    }

    let control_ids: Vec<NodeId> = (0..control_n).map(config::node_id).collect();

    // Each role's own internal peer book, plus the union a data node's single
    // internal env needs (its `heartbeat_loop` targets the control ids over
    // that same env).
    let control_peer_book: BTreeMap<NodeId, String> = control_bound
        .iter()
        .map(|b| {
            (
                b.id.clone(),
                advertised_addr(b.advertise_host.as_deref(), b.internal_addr),
            )
        })
        .collect();
    let raftkv_peer_book: BTreeMap<NodeId, String> = data_bound
        .iter()
        .map(|b| {
            (
                b.id.clone(),
                advertised_addr(b.advertise_host.as_deref(), b.internal_addr),
            )
        })
        .collect();
    let mut data_env_peers = raftkv_peer_book;
    data_env_peers.extend(control_peer_book.clone());

    // Cross-node routing (ADR 0017 #3b / ADR 0013): every node's id resolves
    // to its node's client API address, exactly like
    // `run_node_control`/`run_node_data`'s per-process assembly.
    let mut client_route: BTreeMap<NodeId, String> = BTreeMap::new();
    for b in &control_bound {
        client_route.insert(b.id.clone(), b.client_addr.to_string());
    }
    for b in &data_bound {
        client_route.insert(b.id.clone(), b.client_addr.to_string());
    }

    // The `intra_route` sibling (ADR 0047) — identical shape, `.intra_addr`
    // instead of `.client_addr`.
    let mut intra_route: BTreeMap<NodeId, String> = BTreeMap::new();
    for b in &control_bound {
        intra_route.insert(b.id.clone(), b.intra_addr.to_string());
    }
    for b in &data_bound {
        intra_route.insert(b.id.clone(), b.intra_addr.to_string());
    }

    // The control deployment's **intra** addresses (ADR 0047) — the
    // discovery root each data node's `ControlHandle::Remote` mirrors from
    // (`WatchMetadata` is intra-only, so this must not be the client
    // address).
    let control_intra_addrs: Vec<String> = control_bound
        .iter()
        .map(|b| advertised_addr(b.advertise_host.as_deref(), b.intra_addr))
        .collect();

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
                // `--stream-seal-*`/`--segment-store` below: always
                // `SplitMode::default()`, `InPlace` since rung 4 layer 2
                // (was `Copy`) — this dev shape has no `--split-mode` flag
                // to override it with.
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
        pitr_janitor::DEFAULT_PITR_SNAPSHOT_CADENCE,
    )
    .await
}

/// Like [`run_node_with_streams`], but with an explicit **PITR periodic
/// base-snapshot cadence** (ADR 0059 §9/§10, Train 3) instead of
/// [`pitr_janitor::DEFAULT_PITR_SNAPSHOT_CADENCE`] (6 hours) — the same
/// "widen the innermost layer, mint a thin test-facing wrapper" convention
/// `quiesce_after`/`ttl_sweep_interval` already established. A
/// `RestoreTableToPointInTime` end-to-end test needs at least one PITR base
/// snapshot to actually exist within its own budget (this codebase's own
/// testing discipline: never wait out a real 6-hour production cadence) —
/// calls this directly with a millisecond-scale duration instead.
///
/// # Errors
/// As [`run_node_with`].
#[allow(clippy::too_many_arguments)]
pub async fn run_node_with_streams_and_pitr_snapshot_cadence(
    config: &ClusterConfig,
    index: usize,
    dir: impl Into<PathBuf>,
    backend: StorageBackend,
    orphan_sweep_after: Duration,
    stream_seal_knobs: StreamSealKnobs,
    segment_store_config: SegmentStoreConfig,
    stream_retention: Duration,
    pitr_snapshot_cadence: Duration,
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
        Duration::ZERO,
        ttl_reaper::DEFAULT_TTL_SWEEP_INTERVAL,
        SplitMode::default(),
        BackupStoreConfig::default(),
        pitr_snapshot_cadence,
    )
    .await
}

/// Like [`run_node_with_streams_and_quiesce_after`], but also selects the
/// **split workflow** (ADR 0058 Train 2 rung 3) explicitly instead of
/// [`SplitMode::default`] — `--config FILE --node I`'s `--split-mode
/// {copy,inplace}` CLI flag threads through here. The same layered-wrapper
/// convention as [`run_node_with_streams_quiesce_and_ttl_sweep_interval`]'s
/// own `ttl_sweep_interval` knob: every existing call site above keeps
/// compiling and behaving identically at `SplitMode::default()` (`InPlace`
/// since rung 4 layer 2).
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
        pitr_janitor::DEFAULT_PITR_SNAPSHOT_CADENCE,
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
    pitr_snapshot_cadence: Duration,
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
    let mut client_route: BTreeMap<NodeId, String> = BTreeMap::new();
    for (i, addrs) in config.nodes.iter().enumerate() {
        client_route.insert(
            config::node_id(i),
            advertised_addr(addrs.advertise_host.as_deref(), addrs.client),
        );
    }
    // The `intra_route` sibling (ADR 0047) — identical shape, `.intra`
    // instead of `.client`.
    let mut intra_route: BTreeMap<NodeId, String> = BTreeMap::new();
    for (i, addrs) in config.nodes.iter().enumerate() {
        intra_route.insert(
            config::node_id(i),
            advertised_addr(addrs.advertise_host.as_deref(), addrs.intra),
        );
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
            pitr_snapshot_cadence,
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
        pitr_janitor::DEFAULT_PITR_SNAPSHOT_CADENCE,
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
    let mut client_route: BTreeMap<NodeId, String> = BTreeMap::new();
    for (i, a) in config.nodes.iter().enumerate() {
        client_route.insert(
            config::node_id(i),
            advertised_addr(a.advertise_host.as_deref(), a.client),
        );
    }
    // The `intra_route` sibling (ADR 0047) — identical shape, `.intra`
    // instead of `.client`.
    let mut intra_route: BTreeMap<NodeId, String> = BTreeMap::new();
    for (i, a) in config.nodes.iter().enumerate() {
        intra_route.insert(
            config::node_id(i),
            advertised_addr(a.advertise_host.as_deref(), a.intra),
        );
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
            // `trigger_split` calls always default to `SplitMode::default()`,
            // `InPlace` since rung 4 layer 2 (was `Copy`); `--config`'s own
            // `run` path is still the only way to pin `Copy` explicitly.
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
    let control_intra_addrs: Vec<String> = config
        .nodes
        .iter()
        .filter(|a| a.role.has_control())
        .map(|a| advertised_addr(a.advertise_host.as_deref(), a.intra))
        .collect();

    // Cross-node routing (ADR 0017 #3b / ADR 0013): map every node's id to
    // its client API address — the same shape `run_node_control` builds.
    let mut client_route: BTreeMap<NodeId, String> = BTreeMap::new();
    for (i, a) in config.nodes.iter().enumerate() {
        client_route.insert(
            config::node_id(i),
            advertised_addr(a.advertise_host.as_deref(), a.client),
        );
    }
    // The `intra_route` sibling (ADR 0047) — identical shape, `.intra`
    // instead of `.client`.
    let mut intra_route: BTreeMap<NodeId, String> = BTreeMap::new();
    for (i, a) in config.nodes.iter().enumerate() {
        intra_route.insert(
            config::node_id(i),
            advertised_addr(a.advertise_host.as_deref(), a.intra),
        );
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
            // after`'s identical scope gap) — always `SplitMode::default()`,
            // `InPlace` since rung 4 layer 2 (was `Copy`).
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
    let mut client_route: BTreeMap<NodeId, String> = BTreeMap::new();
    for (i, addrs) in config.nodes.iter().enumerate() {
        client_route.insert(
            config::node_id(i),
            advertised_addr(addrs.advertise_host.as_deref(), addrs.client),
        );
    }
    // The `intra_route` sibling (ADR 0047) — identical shape, `.intra`
    // instead of `.client`; this is what makes the growth-node mirror's own
    // seed-building (`start_with_streams`'s `ctx.intra_addr(id)` call) resolve
    // correctly from this node's very first tick.
    let mut intra_route: BTreeMap<NodeId, String> = BTreeMap::new();
    for (i, addrs) in config.nodes.iter().enumerate() {
        intra_route.insert(
            config::node_id(i),
            advertised_addr(addrs.advertise_host.as_deref(), addrs.intra),
        );
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
async fn join_request(seeds: &[String], request: &ClientRequest) -> Option<ClientResponse> {
    for addr in seeds {
        let reply = tokio::time::timeout(JOIN_ATTEMPT_TIMEOUT, async {
            let mut stream = TcpStream::connect(addr.as_str()).await.ok()?;
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
    seeds: &[String],
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
    seeds: Vec<String>,
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
        internal: advertised_addr(addrs.advertise_host.as_deref(), addrs.internal),
        client: advertised_addr(addrs.advertise_host.as_deref(), addrs.client),
        admin: advertised_addr(addrs.advertise_host.as_deref(), addrs.admin),
        intra: advertised_addr(addrs.advertise_host.as_deref(), addrs.intra),
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
    mut peers: BTreeMap<NodeId, String>,
    mut client_route: BTreeMap<NodeId, String>,
    mut intra_route: BTreeMap<NodeId, String>,
    mut admin_addrs: Vec<SocketAddr>,
    backend: StorageBackend,
) -> std::io::Result<Node> {
    for (id, addr) in bound.peer_entries() {
        peers.insert(id, addr);
    }
    client_route.insert(
        my_id.clone(),
        advertised_addr(bound.advertise_host.as_deref(), my_client_addr),
    );
    // The `intra_route` sibling (ADR 0047) — see `ClientResponse::JoinInfo`'s
    // own field doc for why this must be a real, discovered seed, not empty.
    intra_route.insert(
        my_id,
        advertised_addr(bound.advertise_host.as_deref(), my_intra_addr),
    );
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
    seeds: &[String],
) -> std::io::Result<(
    Vec<NodeId>,
    BTreeMap<NodeId, String>,
    BTreeMap<NodeId, String>,
    BTreeMap<NodeId, String>,
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
    seeds: &[String],
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
    seeds: &[String],
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
    seeds: Vec<String>,
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
        internal: advertised_addr(addrs.advertise_host.as_deref(), addrs.internal),
        client: advertised_addr(addrs.advertise_host.as_deref(), addrs.client),
        admin: advertised_addr(addrs.advertise_host.as_deref(), addrs.admin),
        intra: advertised_addr(addrs.advertise_host.as_deref(), addrs.intra),
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
    mut peers: BTreeMap<NodeId, String>,
    mut client_route: BTreeMap<NodeId, String>,
    mut intra_route: BTreeMap<NodeId, String>,
    mut admin_addrs: Vec<SocketAddr>,
    backend: StorageBackend,
    dynamo_auth: Option<Arc<BTreeMap<String, String>>>,
) -> std::io::Result<Node> {
    // The data-only dual of `finish_combined_join`'s merge (a single raftkv
    // peer entry, no control id of its own to add).
    let (peer_id, peer_addr) = bound.peer_entry();
    peers.insert(peer_id, peer_addr);
    client_route.insert(
        my_id.clone(),
        advertised_addr(bound.advertise_host.as_deref(), my_client_addr),
    );
    // The `intra_route` sibling (ADR 0047) — see `finish_combined_join`'s
    // identical treatment.
    intra_route.insert(
        my_id,
        advertised_addr(bound.advertise_host.as_deref(), my_intra_addr),
    );
    if !admin_addrs.contains(&my_admin_addr) {
        admin_addrs.push(my_admin_addr);
    }

    // The control deployment's **intra** addresses (ADR 0047; `WatchMetadata`
    // is intra-only) — the same derivation `run_node_data` does from a static
    // `ClusterConfig`, here from the merged, discovery-built `intra_route`
    // instead.
    let control_seeds: Vec<String> = original_control_ids
        .iter()
        .filter_map(|id| intra_route.get(id).cloned())
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
            // own (same documented gap as `run_node_data`) — always
            // `SplitMode::default()`, `InPlace` since rung 4 layer 2 (was
            // `Copy`).
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
pub use animus_node::MAX_FRAME_LEN;

/// Send `request` to a peer node's client API over a fresh connection and
/// return its reply (or a [`ClientResponse::Error`] on any transport
/// failure). Free function, not a [`ClientCtx`] method (ADR 0035 PR4): the
/// data-only node's [`control_handle::RemoteControlClient`] has no `ClientCtx`
/// of its own to reach through, but needs the exact same wire primitive every
/// other cross-node relay in this crate uses — [`ClientCtx::relay`] is now a
/// thin wrapper over this.
pub(crate) async fn relay_request(addr: String, request: &ClientRequest) -> ClientResponse {
    relay_request_with_timeout(addr, request, CLIENT_TIMEOUT).await
}

/// Like [`relay_request`], but with an explicit transport timeout instead of
/// the default [`CLIENT_TIMEOUT`] (ADR 0035 PR5) — needed by
/// [`remote_metadata_watch_loop`], whose long-poll request's own
/// [`WATCH_METADATA_CLIENT_TIMEOUT`] must exceed the serving node's
/// [`WATCH_METADATA_SERVER_TIMEOUT`] bound by a comfortable margin; reusing
/// the generic [`CLIENT_TIMEOUT`] here would race the server's own reply.
async fn relay_request_with_timeout(
    addr: String,
    request: &ClientRequest,
    timeout: Duration,
) -> ClientResponse {
    match tokio::time::timeout(timeout, async {
        let mut stream = TcpStream::connect(addr.as_str()).await.ok()?;
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
/// **The pure framing arithmetic — the length-prefix encoding, the
/// [`MAX_FRAME_LEN`] bound check, and the `serde_json` encode itself — moved
/// to [`animus_node::codec::encode_client_frame`] (ADR 0061 rung C3a)**;
/// this function keeps only the actual socket write, which needs a real
/// `TcpStream` and so cannot move into `animus-node` (no `tokio`
/// dependency there at all).
///
/// # Errors
/// Propagates write failures; rejects a frame over [`MAX_FRAME_LEN`] (the
/// receiver would drop the connection anyway — failing at the sender names the
/// culprit instead of surfacing as a mysterious peer hang-up).
pub async fn write_frame<T: Serialize>(stream: &mut TcpStream, msg: &T) -> std::io::Result<()> {
    let framed = animus_node::codec::encode_client_frame(msg)?;
    stream.write_all(&framed).await?;
    stream.flush().await?;
    Ok(())
}

/// Read a length-prefixed JSON frame, or `None` at clean EOF.
///
/// **The pure framing arithmetic — the [`MAX_FRAME_LEN`] bound check on the
/// declared length, and the `serde_json` decode itself — moved to
/// [`animus_node::codec::frame_payload_len`]/
/// [`animus_node::codec::decode_client_frame`] (ADR 0061 rung C3a)**; this
/// function keeps only the actual socket reads, which need a real
/// `TcpStream`.
///
/// # Errors
/// Propagates read failures and decode errors; a declared length over
/// [`MAX_FRAME_LEN`] is an `InvalidData` error **before any allocation** (the
/// length prefix is untrusted — see [`MAX_FRAME_LEN`]).
pub async fn read_frame<T: DeserializeOwned>(stream: &mut TcpStream) -> std::io::Result<Option<T>> {
    let raw_len = match stream.read_u32().await {
        Ok(len) => len,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    };
    let len = animus_node::codec::frame_payload_len(raw_len)?;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    let msg = animus_node::codec::decode_client_frame(&buf)?;
    Ok(Some(msg))
}

// `byte_weighted_median`'s own unit tests moved to `decide`'s test module
// alongside the function itself (ADR 0061 A6, formerly `auto_split_median_tests`
// here).

/// Regression tests for the end-to-end fast-fail behavior
/// [`decide::confirm_wait_is_futile`] enables (issue #268) — in-crate
/// because they need a private [`CpGroup`] handle and the `pub(crate)`
/// [`ClientCtx::cp_kind_local`], which no external `tests/` file can reach
/// (the same reason `gsi_drain_cursor_tests` lives inside `index_drain.rs`).
/// Run via `cargo test -p animusd --lib`.
///
/// **Deliberately not moved into `decide`'s own test module (ADR 0061 A6):**
/// unlike `decide::confirm_wait_is_futile`'s own direct unit tests (a plain
/// truth table over the predicate's three primitive inputs), these prove
/// the *wired* behavior — a real `CpGroup` propose/apply/poll round trip
/// through `cp_kind_local`, with real timing assertions — which needs a
/// live single-node cluster regardless of how pure the underlying predicate
/// is. `decide`'s own module stays bring-up-free, matching `topology.rs`.
#[cfg(test)]
mod confirm_futility_tests {
    use std::net::SocketAddr;
    use std::path::Path;
    use std::time::{Duration, Instant};

    use tokio::time::{sleep, timeout};

    use animus_env::ProdEnv;

    use crate::config::NodeRole;
    use crate::{
        AnimusdRelayClient, ClientCtx, ClientRequest, ClientResponse, ClusterConfig, Node,
        RoleAddrs, read_frame, run_node, write_frame,
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
                advertise_host: None,
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
        // Turbofish required (ADR 0061 rung C5 step 3a): `cp_kind_local`
        // takes no `self`/`R`-typed argument, so nothing here pins down `R`
        // for the now-generic `ClientCtx<E, R>` path.
        let err = ClientCtx::<ProdEnv, AnimusdRelayClient>::cp_kind_local(
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
        ClientCtx::<ProdEnv, AnimusdRelayClient>::cp_kind_local(
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
                advertise_host: None,
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

// `align_split_key`'s own unit tests moved to `decide`'s test module
// alongside the function itself (ADR 0061 A6, formerly `align_split_key_tests`
// here). `manual_split_with_unaligned_key_on_streamed_table_rounds_to_
// token_boundary` (`tests/f11_split_alignment.rs`) still covers the same
// rounding end to end, through a real cluster's admin HTTP surface.

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

/// Issue #412 regression: a leader-side old-image read failure with the
/// house `"; retry"` shape (a leader-moved/no-longer-leader condition) must
/// never surface as a terminal error while retries remain, for either the
/// ordinary evaluate-at-leader write path (`dynamo::
/// kind_write_item_at_leader` via `ClientCtx::cp_kind_write_item`) or its
/// transactional twin (`dynamo::eval_kind_txn_write` via `ClientCtx::
/// txn_prepare_pushing`). Uses `dynamo::leader_read_failure_gate` to inject
/// the failure deterministically rather than orchestrating a real
/// leadership change — same idiom as `dynamo::rmw285_confirm_gate`.
#[cfg(test)]
mod issue_412_tests {
    use std::net::SocketAddr;
    use std::path::Path;
    use std::time::Duration;

    use tokio::time::{sleep, timeout};

    use crate::config::NodeRole;
    use crate::dynamo::{self, leader_read_failure_gate};
    use crate::{ClientCtx, ClusterConfig, KindWriteOp, Node, RoleAddrs, run_node};

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
                advertise_host: None,
            }],
            dynamo_auth: None,
        }
    }

    /// Same bounded fresh-config retry every in-crate bring-up in this
    /// crate uses (`docs/engineering-lessons.md`) against the port-TOCTOU
    /// race under `cargo test --workspace` contention.
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

    /// Provisions `table`'s first tablet and waits for its single-voter
    /// group to actually elect locally — `provision_tablet` alone does not
    /// wait for that (`confirm_futility_tests`'s identical polling doc).
    async fn provision_and_await_leader(node: &Node, ctx: &ClientCtx, table: &str) {
        ctx.provision_tablet(table)
            .await
            .expect("provisioning table");
        let tablet = *node
            .metadata()
            .tablets_for_table(table)
            .next()
            .expect("provisioning created a tablet")
            .0;
        let group = node
            .edge
            .local_cp(tablet)
            .expect("this single node hosts the tablet");
        timeout(Duration::from_secs(10), async {
            while !group.is_leader() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("tablet group did not elect a local leader in time");
    }

    /// The ordinary (non-transactional) evaluate-at-leader write path
    /// retries a leader-moved-shaped read failure to success —
    /// `ClientCtx::cp_kind_write_item`'s issue #288 retry loop already
    /// re-resolves routing on this exact `"; retry"` shape (confirming this
    /// half of #412 was already sound; the txn-side twin below is the
    /// actual fix).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn kind_write_item_retries_a_leader_moved_read_failure_to_success() {
        let dir = tempfile::tempdir().unwrap();
        let node = single_node(dir.path()).await;
        let ctx = node.ctx_for_test();
        provision_and_await_leader(&node, &ctx, "issue412_plain").await;
        let meta = node.metadata();

        leader_read_failure_gate::arm("issue412_plain", 2);

        let mut item = animus_dynamo::Item::new();
        item.insert(
            "pk".to_string(),
            animus_dynamo::AttributeValue::S("k1".to_string()),
        );
        let pk = animus_dynamo::AttributeValue::S("k1".to_string());
        let outcome = ctx
            .cp_kind_write_item(
                &meta,
                "issue412_plain",
                &pk,
                None,
                KindWriteOp::Put(item),
                None,
            )
            .await
            .expect(
                "a retryable leader-moved read failure must be retried to success, \
                 never surfaced as a terminal error",
            );
        assert!(matches!(outcome, dynamo::KindWriteOutcome::Ok { .. }));

        node.shutdown();
    }

    /// Issue #412's actual fix: the transactional stage-time evaluator
    /// (`dynamo::eval_kind_txn_write`, reached via `TransactWriteItems`)
    /// hits the identical leader-moved read failure — pre-fix, it escaped
    /// `ClientCtx::txn_prepare_pushing`'s bounded retry loop via `?` on the
    /// very first attempt and would have surfaced as a terminal whole-txn
    /// cancel. Calls `txn_prepare_pushing` directly (the function whose
    /// retry loop this fixes) with a single anchor-only pending kind write,
    /// so a failure here can only mean that loop itself didn't retry.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn txn_prepare_pushing_retries_a_leader_moved_read_failure_to_success() {
        let dir = tempfile::tempdir().unwrap();
        let node = single_node(dir.path()).await;
        let ctx = node.ctx_for_test();
        provision_and_await_leader(&node, &ctx, "issue412_txn").await;

        leader_read_failure_gate::arm("issue412_txn", 2);

        let mut item = animus_dynamo::Item::new();
        item.insert(
            "pk".to_string(),
            animus_dynamo::AttributeValue::S("k1".to_string()),
        );
        let pk = animus_dynamo::AttributeValue::S("k1".to_string());
        let pending = crate::PendingKindWrite {
            pk,
            sk: None,
            op: KindWriteOp::Put(item),
            condition: None,
        };
        ctx.txn_prepare_pushing(
            "issue412_txn",
            None,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![pending],
        )
        .await
        .expect(
            "a retryable leader-moved read failure inside the stage-time evaluator must be \
             retried to success, never a terminal whole-txn cancel",
        );

        node.shutdown();
    }
}
