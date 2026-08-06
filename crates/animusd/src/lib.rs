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

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub mod config;
pub mod otel;
pub use config::ClusterConfig;
// Re-exported so callers (CLI, tests, operators) can inspect a node's cached
// cluster metadata — membership status and the tablet map — without depending on
// `animus-control` directly.
pub use animus_control::{
    ColumnDef, ColumnType, MetaCommand, Metadata, NodeStatus, ReplicationMode, TableSchema,
};

mod admin;
mod cql;
mod cql_client;
mod dashboard;
mod dynamo;
mod http;
mod topology;

use animus_control::node::heartbeat_loop;
use animus_control::{PlacementPolicy, ProposeResult, RaftNode};
use animus_cp_data::{RaftKvNode, SplitHook};
use animus_env::{Coresident, Disk, Env, Metric, MetricsHandle, NodeId, ProdEnv};
use animus_storage::{LsmEngine, MemoryEngine, SsTableView, StorageEngine, WalRecordView};
use animus_tablet::{KeyRange, TabletId};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::Instrument;
// Pure CP-topology decision logic (id translation, routing, join-host/GC
// predicates), extracted into `topology` for unit-test coverage.
use topology::{cp_base_id, cp_member_id, cp_members_for};

/// A list of `(key, value)` pairs — the payload of a batch write (one Raft
/// `KvCommand::Batch` entry per tablet). Named to keep the batch grouping map
/// (`BTreeMap<TabletId, KvPairs>`) under clippy's `type_complexity` bar.
type KvPair = (Vec<u8>, Vec<u8>);
type KvPairs = Vec<KvPair>;

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
    /// Propose a write to the group (honored on the leader). See
    /// [`RaftKvNode::put`].
    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> ProposeResult {
        match self {
            CpGroup::Lsm(n) => n.put(key, value),
            CpGroup::Mem(n) => n.put(key, value),
        }
    }

    /// Propose a **batch put** — commit every `(key, value)` as one Raft entry. See
    /// [`RaftKvNode::put_batch`].
    fn put_batch(&self, puts: Vec<(Vec<u8>, Vec<u8>)>) -> ProposeResult {
        match self {
            CpGroup::Lsm(n) => n.put_batch(puts),
            CpGroup::Mem(n) => n.put_batch(puts),
        }
    }

    /// Propose a delete (tombstone) to the group. See [`RaftKvNode::delete`].
    fn delete(&self, key: Vec<u8>) -> ProposeResult {
        match self {
            CpGroup::Lsm(n) => n.delete(key),
            CpGroup::Mem(n) => n.delete(key),
        }
    }

    /// Linearizable ReadIndex read. See [`RaftKvNode::linearizable_get`].
    async fn linearizable_get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self {
            CpGroup::Lsm(n) => n.linearizable_get(key).await,
            CpGroup::Mem(n) => n.linearizable_get(key).await,
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

    /// Whether this node currently believes it leads the group.
    fn is_leader(&self) -> bool {
        match self {
            CpGroup::Lsm(n) => n.is_leader(),
            CpGroup::Mem(n) => n.is_leader(),
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

    /// The env this group member runs on: its node id is the member's **derived
    /// group id** (`cp_member_id`), and its disk holds the member's engine +
    /// `raftkv.wal` files — which is what lets the GC both identify *this node's*
    /// handle in the shared edge registry and delete the right files.
    fn env(&self) -> &ProdEnv {
        match self {
            CpGroup::Lsm(n) => n.env(),
            CpGroup::Mem(n) => n.env(),
        }
    }

    /// Propose a **tablet split** at `at` (Phase 2.2): keys `>= at` move to a new
    /// tablet. Leader-only. On commit every replica tombstones `[at, ∞)` and the
    /// node's split hook mints the new tablet's co-resident group. See
    /// [`RaftKvNode::propose_split`].
    fn propose_split(&self, at: Vec<u8>) -> ProposeResult {
        match self {
            CpGroup::Lsm(n) => n.propose_split(at),
            CpGroup::Mem(n) => n.propose_split(at),
        }
    }

    /// The `at` key of this group's applied `Split` (at most once, ever), or
    /// `None` — the confirm-by-key primitive a
    /// [`propose_split`](Self::propose_split) caller must poll:
    /// `ProposeResult::Accepted` only means the entry was appended to the leader's
    /// local log, not that it committed, and comparing against the *specific* key
    /// proposed (not just "has it split at all") is what catches a same-tick
    /// same-tablet race where a *different* median won. See
    /// [`RaftKvNode::applied_split_key`]'s doc.
    fn applied_split_key(&self) -> Option<Vec<u8>> {
        match self {
            CpGroup::Lsm(n) => n.applied_split_key(),
            CpGroup::Mem(n) => n.applied_split_key(),
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
            CpGroup::Lsm(n) => n.range_snapshot(&[]).await,
            CpGroup::Mem(n) => n.range_snapshot(&[]).await,
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

    /// The first `limit` live `(key, value)` pairs with `key >= start`, in key
    /// order, from the **local** engine — the admin "browse keys" view (ADR 0021).
    /// Node-local introspection like the other `/admin/storage/*` routes, so it
    /// reads this replica's engine directly rather than via a quorum scan. Reuses
    /// `range_snapshot` and truncates — fine for a debug surface on dev-sized
    /// tablets (it materializes the live range from `start` before truncating).
    async fn local_scan(&self, start: &[u8], limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut pairs = match self {
            CpGroup::Lsm(n) => n.range_snapshot(start).await,
            CpGroup::Mem(n) => n.range_snapshot(start).await,
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
    fn raft_view(&self, tablet: TabletId) -> admin::CpRaftView {
        macro_rules! view {
            ($n:expr) => {
                admin::CpRaftView {
                    tablet: tablet.0,
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
                }
            };
        }
        match self {
            CpGroup::Lsm(n) => view!(n),
            CpGroup::Mem(n) => view!(n),
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
    /// `None` if no step is needed / this node isn't the leader.
    fn reconfigure_step(&self, desired: &BTreeSet<NodeId>) -> Option<BTreeSet<NodeId>> {
        match self {
            CpGroup::Lsm(n) => n.reconfigure_step(desired),
            CpGroup::Mem(n) => n.reconfigure_step(desired),
        }
    }
}

/// How a CP op originating on this node reaches the group leader
/// ([`ClientCtx::cp_route`]).
enum CpRoute {
    /// This node hosts the current leader — serve from `leader` directly.
    Local(CpGroup),
    /// Forward to the leader's node at this client-API address (ADR 0017 #3b).
    Forward(SocketAddr),
    /// No leader reachable (no local leader, no route, election did not settle).
    None,
}

/// The tablet id of the **first table provisioned** (ADR 0023): the tablet-id
/// allocator hands out `1` first, and `cp_member_id`/`cp_join_host` host tablet `1`
/// on the node's **main** `raftkv` env (member id == base id) while later tablets
/// use minted siblings. So the first `CreateTable` lands on the main env at no extra
/// cost, and there is no separate always-on "bootstrap" data tablet (a fresh cluster
/// has zero data tablets until the first `CreateTable`).
const TABLET: TabletId = TabletId(1);
/// How long a CP op (`cp_route` + forward) waits for the tablet's group to be
/// reachable before giving up. Generous because a table's group now forms **in
/// band** on the first access (ADR 0023) — the first op after a `CreateTable`/
/// first-write waits out the join-host + election, which under heavy load takes
/// longer than a steady-state op. No happy-path cost: `cp_route` returns as soon as
/// a leader is reachable; the cap only bounds the wait when the group is forming.
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);
/// The bootstrap CP group's replication factor (ADR 0017 #3a): the group spans the
/// first `min(N, MAX_REPLICATION_FACTOR)` nodes' `raftkv` ids. Dynamic CP placement
/// over more nodes is later v1 work.
const MAX_REPLICATION_FACTOR: usize = 3;
/// Filename prefix namespacing the CP group's on-disk LSM under the node's `raftkv`
/// `ProdEnv` directory (its files become `db-MANIFEST`/`db-wal`/`db-sst-*`).
///
/// The prefix is a flat filename prefix, **not** a subdirectory (no `/`):
/// `ProdEnv`'s disk opens files directly under the role's data dir and does not
/// create intermediate directories, so a slash-bearing prefix would fail to
/// create the engine's files. The role's dir is dedicated to this group, so a flat
/// prefix already isolates it. A split-created tablet's group uses a per-tablet
/// prefix `db-t{id}-` so co-resident groups' LSM files never collide.
const LSM_PREFIX: &str = "db-";

/// Size of each `raftkv` env's pre-bound **sibling listener pool** (Phase 2.2): the
/// number of co-resident CP groups a node can host **beyond** its bootstrap group —
/// i.e. the max split children per node. A node hosting tablets `1..=T` needs `T-1`
/// slots, so this caps a node at `CP_SIBLING_POOL + 1` tablets; exceeding it panics
/// the split-hook task (`Coresident::sibling`), leaving the over-cap tablet
/// leaderless. Sized generously so realistic sharding tests (bulk-seed → auto-split)
/// don't hit it; each slot is a cheap pre-bound loopback listener + accept loop.
/// (A truly unbounded fix needs on-demand sibling binding — an `async`/fallible
/// `Coresident::sibling` — which is a larger ADR-0017 change; deferred.)
const CP_SIBLING_POOL: usize = 64;

/// Stride for deriving a split-created tablet's CP **member ids** from the parent
/// group's, deterministically + identically on every replica (Phase 2.2): a new
/// member is `parent_member + new_tablet_id * CP_SPLIT_ID_STRIDE`. Wide enough that
/// `300 + i` bootstrap ids and per-tablet bands never overlap for small clusters /
/// tablet counts. (Deep-split id allocation — a flat allocator that survives many
/// generations — is a later refinement.)
const CP_SPLIT_ID_STRIDE: NodeId = 1000;

/// Durable per-node record of the **split-created CP tablets this node hosts on
/// disk** (#2 tablet-map-driven hosting). Written under the node's `raftkv`
/// `ProdEnv` directory by the split-seed path once a new tablet's group is stood
/// up + seeded durably; read at node start to **re-host** those tablets after a
/// restart (their `db-t{id}-` engines are on disk, recovered via `start_seeded`
/// with an empty seed). This is genuinely *local* state — which co-resident engines
/// physically exist on this node — not derivable from the replicated tablet map
/// (which records the placement in stable base ids, not the per-tablet sibling
/// engines), so it is a durable marker rather than a cache of replicated state.
///
/// It also gives **split crash-idempotency** (#4): pre-populating the per-node
/// `minted` set from this marker at start means the parent group re-applying its
/// committed `Split` on WAL recovery finds the tablet already hosted and does not
/// mint the sibling a second time.
const CP_HOSTED_FILE: &str = "cp-hosted";

/// One entry in the [`CP_HOSTED_FILE`] marker: a split-created tablet this node
/// hosts, with this node's **member id** in that tablet's group and the group's
/// full member-id set, both already in the derived id space
/// (`base + tablet * CP_SPLIT_ID_STRIDE`) so re-hosting needs no re-derivation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct HostedCpTablet {
    tablet: u64,
    member: NodeId,
    members: Vec<NodeId>,
}

/// Read this node's durable [`HostedCpTablet`] set from the `raftkv` env disk
/// (empty if the marker does not exist or is unreadable/corrupt — a missing or
/// damaged marker degrades to "host nothing extra", never a hard failure at
/// start). Generic over `E: Env` so the `Disk` supertrait methods are in scope.
async fn load_hosted_cp<E: Env>(env: &E) -> Vec<HostedCpTablet> {
    let bytes = env.read(CP_HOSTED_FILE).await.unwrap_or_default();
    if bytes.is_empty() {
        return Vec::new();
    }
    serde_json::from_slice(&bytes).unwrap_or_else(|e| {
        tracing::warn!(?e, "CP hosted marker is corrupt; ignoring");
        Vec::new()
    })
}

/// Atomically persist this node's [`HostedCpTablet`] set to the `raftkv` env disk
/// (durable on return — `Disk::replace` is temp-file + rename, so a crash sees the
/// whole old or whole new marker, never a mix). Called when the split-seed path
/// stands up a new tablet's group, so a subsequent restart re-hosts it.
async fn save_hosted_cp<E: Env>(env: &E, hosted: &[HostedCpTablet]) {
    let bytes = serde_json::to_vec(hosted).expect("hosted CP marker serializes");
    if let Err(e) = env.replace(CP_HOSTED_FILE, &bytes).await {
        tracing::error!(?e, "persisting the CP hosted marker");
    }
}

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
    /// Read the latest value at `key` of `table` (linearizable CP ReadIndex on the
    /// group leader). `table` is **required** (ADR 0023).
    Get { key: Vec<u8>, table: String },
    /// Delete `key` of `table` from the **CP** plane (a Raft-committed tombstone) —
    /// the CQL edge's whole-partition delete. `table` is **required** (ADR 0023).
    Delete { key: Vec<u8>, table: String },
    /// A **linearizable range scan** of `table` over `[start, end)`, up to `limit`
    /// keys, served from the group leader (ReadIndex). The CP read primitive behind
    /// the DynamoDB `Query`/`Scan` and CQL `SELECT` edges; also the cross-process
    /// forwarding payload for a scan (ADR 0017 #3b). `table` is **required** (ADR
    /// 0023) — scans are per-table fan-outs.
    Scan {
        start: Vec<u8>,
        /// Exclusive upper bound, or `None` for **unbounded above** — a whole-table
        /// scan (ADR 0023), since a per-table tablet's engine has no finite max key.
        end: Option<Vec<u8>>,
        #[serde(default)]
        limit: Option<usize>,
        table: String,
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
    /// `CREATE TABLE` / `SetTableMode` but isn't the control leader sends this to
    /// the leader's node, so DDL on a follower-connected client still commits. Any
    /// node accepts it, **gates** it to schema-catalog commands (membership /
    /// placement commands are rejected — not a general "propose anything" surface),
    /// and routes it to the control leader (locally if it is the leader, else
    /// relaying toward the leader — bounded, since a relay only targets a known
    /// leader). The result replicates back to every node's `Metadata` as usual; the
    /// caller confirms by polling its own replicated view.
    ProposeSchema(MetaCommand),
    /// **Admin: split a CP tablet** at `split_key` (Phase 2.2). The receiving node
    /// records the split in the control plane (`SplitTablet`, minting a new tablet
    /// id) and proposes the data-plane split on the tablet's CP group leader, which
    /// on commit hands the upper range `[split_key, ∞)` to a new co-resident group.
    /// The interim manual trigger; an automatic size-telemetry trigger is later work.
    SplitTablet { tablet: u64, split_key: Vec<u8> },
    /// The **data-plane half** of a split, forwarded to the CP leader's node (D2
    /// cross-process split trigger). After [`SplitTablet`](ClientRequest::SplitTablet)
    /// records the split in the control plane, the data-plane `propose_split` must run
    /// on the node hosting the tablet's CP-group leader, which may differ from the
    /// control leader. A node that recorded the metadata but doesn't host the CP
    /// leader forwards this; the receiver proposes the split **iff** it is the CP
    /// leader (one hop, no re-forward, no metadata) — else errors and the client
    /// retries with fresh routing.
    CpSplit { tablet: u64, split_key: Vec<u8> },
}

/// Whether `command` may be **relayed to the control leader** via
/// [`ClientRequest::ProposeSchema`]: the schema-catalog mutations (ADR 0013) that a
/// wire client drives, plus [`MetaCommand::RegisterCpAddr`] (Phase 2.3a) — a node's
/// own CP-address self-registration — plus [`MetaCommand::SplitTablet`] (D2), the
/// metadata half of the admin split trigger (already client-exposed via
/// [`ClientRequest::SplitTablet`], so relaying it adds no new authority — it lets the
/// trigger reach the control leader cross-process when the split is driven from a
/// follower). Other membership / placement / tablet commands are control-plane-
/// internal and are **not** accepted over this path.
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
            | MetaCommand::SetTableMode { .. }
            | MetaCommand::CreateKeyspace { .. }
            | MetaCommand::DropKeyspace { .. }
            | MetaCommand::RegisterCpAddr { .. }
            | MetaCommand::SplitTablet { .. }
            // Provision-at-create (ADR 0023): a `CreateTable` on a follower-connected
            // client relays the table's tablet creation + RF policy to the control
            // leader. Scoped to one tablet per table by the state machine's guard.
            | MetaCommand::CreateTablet { .. }
            | MetaCommand::SetTabletPolicy { .. }
            // Drop-table GC (ADR 0024): a `DROP TABLE` on a follower-connected
            // client relays the table's tablet removal to the control leader.
            | MetaCommand::DropTableTablets { .. }
    )
}

/// A node's reply to a [`ClientRequest`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ClientResponse {
    /// Cached cluster metadata (membership + tablet map).
    Status(Metadata),
    /// A write reached its quorum.
    PutOk,
    /// A read reached its quorum; the value (or `None` if absent).
    Value(Option<Vec<u8>>),
    /// A range scan's live `(key, value)` pairs in key order (reply to
    /// [`Scan`](ClientRequest::Scan)).
    Pairs(Vec<(Vec<u8>, Vec<u8>)>),
    /// The operation could not be served (no quorum, no tablet, etc.).
    Error(String),
}

/// Listen addresses for a node's endpoints (use port 0 for ephemeral): the
/// control + **raftkv** (CP per-tablet Raft) internal `ProdEnv` roles + the client
/// API + the DynamoDB HTTP and CQL endpoints. v1 (ADR 0019) is CP-only — the AP
/// `data`/`coord` roles are gone.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct RoleAddrs {
    pub control: SocketAddr,
    pub client: SocketAddr,
    /// The DynamoDB JSON-over-HTTP endpoint. Defaults (when absent in older
    /// configs) to an ephemeral loopback port.
    #[serde(default = "default_ephemeral_addr")]
    pub dynamo: SocketAddr,
    /// The CQL (Cassandra) binary-protocol endpoint. Defaults (when absent in
    /// older configs) to an ephemeral loopback port.
    #[serde(default = "default_ephemeral_addr")]
    pub cql: SocketAddr,
    /// The **leaderful CP** per-tablet Raft role's internal `ProdEnv` listen
    /// address (ADR 0017 #3a) — the data plane. Defaults (when absent in older
    /// configs) to an ephemeral loopback port.
    #[serde(default = "default_ephemeral_addr")]
    pub raftkv: SocketAddr,
    /// The **admin / debug** HTTP-JSON endpoint (ADR 0020) — a read-only
    /// introspection + operator-action surface on its own port, isolated from the
    /// client/dynamo/cql data edges. Defaults (when absent in older configs) to an
    /// ephemeral loopback port.
    #[serde(default = "default_ephemeral_addr")]
    pub admin: SocketAddr,
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
    control_id: NodeId,
    raftkv_id: NodeId,
    control_env: ProdEnv,
    raftkv_env: ProdEnv,
    control_addr: SocketAddr,
    raftkv_addr: SocketAddr,
    client_listener: TcpListener,
    client_addr: SocketAddr,
    dynamo_listener: TcpListener,
    dynamo_addr: SocketAddr,
    cql_listener: TcpListener,
    cql_addr: SocketAddr,
    admin_listener: TcpListener,
    admin_addr: SocketAddr,
}

/// A node's identity + bound addresses, captured for the admin `/admin/config`
/// view (ADR 0020). Held behind an `Arc` in [`ClientCtx`] so it is cheap to clone
/// onto every connection. The live CP-member address map is read from replicated
/// `Metadata` at request time, not cached here.
pub(crate) struct AdminInfo {
    pub(crate) control_id: NodeId,
    pub(crate) raftkv_id: NodeId,
    pub(crate) control_addr: SocketAddr,
    pub(crate) raftkv_addr: SocketAddr,
    pub(crate) client_addr: SocketAddr,
    pub(crate) dynamo_addr: SocketAddr,
    pub(crate) cql_addr: SocketAddr,
    pub(crate) admin_addr: SocketAddr,
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
}

impl BoundNode {
    /// `(control_id, addr)`, `(raftkv_id, addr)` — the entries this node
    /// contributes to the cluster peer book (the two internal `ProdEnv` roles).
    pub fn peer_entries(&self) -> [(NodeId, SocketAddr); 2] {
        [
            (self.control_id, self.control_addr),
            (self.raftkv_id, self.raftkv_addr),
        ]
    }

    /// The address clients connect to.
    pub fn client_addr(&self) -> SocketAddr {
        self.client_addr
    }

    /// The address the DynamoDB JSON/HTTP endpoint listens on.
    pub fn dynamo_addr(&self) -> SocketAddr {
        self.dynamo_addr
    }

    /// The address the CQL binary-protocol endpoint listens on.
    pub fn cql_addr(&self) -> SocketAddr {
        self.cql_addr
    }

    /// The address the admin / debug HTTP endpoint listens on (ADR 0020).
    pub fn admin_addr(&self) -> SocketAddr {
        self.admin_addr
    }

    /// Wire the peer address book into every env and start all protocols, with
    /// the CP group backed by the durable on-disk [`LsmEngine`]
    /// ([`StorageBackend::Lsm`]). `control_ids` is the full control group.
    ///
    /// # Errors
    /// Propagates a failure to open the CP group's on-disk engine.
    pub async fn start(
        self,
        peers: BTreeMap<NodeId, SocketAddr>,
        control_ids: Vec<NodeId>,
    ) -> std::io::Result<Node> {
        let admin_addr = self.admin_addr;
        self.start_with(
            peers,
            control_ids,
            StorageBackend::default(),
            ClusterEdgeState::new(),
            BTreeMap::new(),
            None,
            vec![admin_addr],
        )
        .await
    }

    /// Like [`start`](Self::start), but selects the CP group's storage engine and
    /// options. [`StorageBackend::Lsm`] is durable (survives restart);
    /// [`StorageBackend::Memory`] is volatile (ephemeral runs). `auto_split_threshold`
    /// opts a CP-hosting node into the automatic size-telemetry split trigger (Phase
    /// 2.4): when a tablet it leads exceeds that many keys, it splits at the median;
    /// `None` (the default) disables it.
    ///
    /// # Errors
    /// Propagates a failure to open the CP group's on-disk engine (LSM backend
    /// only).
    #[allow(clippy::too_many_arguments)] // node assembly: ids + backend + edge + route + split opt
    pub async fn start_with(
        self,
        peers: BTreeMap<NodeId, SocketAddr>,
        control_ids: Vec<NodeId>,
        backend: StorageBackend,
        edge: ClusterEdgeState,
        client_route: BTreeMap<NodeId, SocketAddr>,
        auto_split_threshold: Option<usize>,
        cluster_admin_addrs: Vec<SocketAddr>,
    ) -> std::io::Result<Node> {
        self.control_env.set_peers(peers.clone());
        self.raftkv_env.set_peers(peers.clone());
        // The initial (static) peer book + a `raftkv`-env clone, kept for the
        // **peer-sync loop** (Phase 2.3a): it rebuilds the raftkv family's peer book
        // as `static ∪ Metadata.cp_member_addrs` so a runtime-created CP member (a
        // split sibling, a joined node) becomes reachable. `set_peers` replaces the
        // book and a sibling env shares the same book Arc, so syncing the raftkv env
        // reaches every co-resident group.
        let static_peers = peers;
        let raftkv_sync_env = self.raftkv_env.clone();
        // A `raftkv`-env clone for the shared **CP hosting context** (`CpHostCtx`):
        // the split hook, re-host, and join-host paths all mint their sibling inboxes
        // from it (and host the bootstrap tablet on it directly when this node joins
        // that tablet). Shares the pool + peer book with the group's env.
        let raftkv_hook_env = self.raftkv_env.clone();
        // A `raftkv`-env clone for the **failure-detection heartbeat loop** (#3): each
        // node heartbeats the control group *as its `raftkv` member id* (the cluster
        // members are the `raftkv` ids), so the control plane's `detect_loop` marks a
        // crashed CP node `Down`.
        let raftkv_hb_env = self.raftkv_env.clone();
        let my_raftkv_id = self.raftkv_id;
        let my_raftkv_addr = self.raftkv_addr;

        // The node's identity + bound addresses for the admin `/admin/config`
        // view (ADR 0020), captured before the envs are consumed below.
        let admin_info = Arc::new(AdminInfo {
            control_id: self.control_id,
            raftkv_id: self.raftkv_id,
            control_addr: self.control_addr,
            raftkv_addr: self.raftkv_addr,
            client_addr: self.client_addr,
            dynamo_addr: self.dynamo_addr,
            cql_addr: self.cql_addr,
            admin_addr: self.admin_addr,
            control_ids: control_ids.clone(),
            peers: static_peers.clone(),
            admin_addrs: if cluster_admin_addrs.is_empty() {
                vec![self.admin_addr]
            } else {
                cluster_admin_addrs
            },
        });

        // Keep clones of the two internal envs so [`Node::shutdown`] can abort
        // every task they own (the two Raft drivers + accept loops), freeing their
        // listener ports for a restart.
        let envs = [self.control_env.clone(), self.raftkv_env.clone()];

        // Capture the raftkv-role metrics sink before its env is consumed below.
        // The control-plane sink is reached at request time via `raft.metrics()`
        // (`RaftNode::start` records into `control_env.metrics()`); the CP group
        // records into its own role env's sink. The `/metrics` endpoint aggregates
        // both (ADR 0015).
        let raftkv_metrics = self.raftkv_env.metrics();

        let raft = RaftNode::start(self.control_env, control_ids.clone());
        // Register this node's control handle in the **per-cluster** set the wire
        // edges use to reach the control-plane leader for schema proposals
        // (ADR 0013). In `--cluster N` mode this lets any node's CQL/DynamoDB edge
        // propose a `CreateTableSchema` on whichever in-process node is currently
        // leader, so DDL on a follower-connected client still commits. The set is
        // owned by the `ClusterEdgeState` (one per cluster), not a process global,
        // so two in-process clusters in one test do not share handles.
        edge.register_control(raft.clone());

        // **Leaderful CP per-tablet Raft group** (ADR 0017 #3a) — the v1 data plane
        // (ADR 0019). Stage 3a hosts a single, statically-placed CP group spanning
        // the first `min(N, MAX_REPLICATION_FACTOR)` nodes' `raftkv` ids. A node in
        // that set runs a `RaftKvNode` on its `raftkv_env` (own id/port/dir — the
        // single-consumer inbox rule), backed by its own engine; the handle is
        // registered in the per-cluster edge state so the wire edges route a table's
        // reads/writes to the group leader. The group is started with a **split
        // hook** (Phase 2.2): on a committed `Split` it mints the new tablet's
        // co-resident group. Dynamic CP reconfigure over `ProdEnv` is later v1 work.
        //
        // The shared client context is built **here**, before the CP hosting block,
        // so the split-seed + re-host paths can publish a new member's address
        // through it (`register_cp_addr` relays to the control leader cross-process
        // via `client_route` — #4 cross-process split-address relay), not just via a
        // local control-leader handle.
        let ctx = ClientCtx {
            raft: raft.clone(),
            rmw_lock: Arc::new(tokio::sync::Mutex::new(())),
            edge: edge.clone(),
            raftkv_metrics,
            client_route,
            base_id: my_raftkv_id,
            admin: admin_info,
        };
        let n = control_ids.len();

        // The shared **CP hosting context** (D3): bundles the env to mint siblings,
        // the client context, backend, this node's base id, and the per-node `minted`
        // (claim) + `hosted` (durable-marker mirror) sets — so every group started on
        // this node (bootstrap, split child, re-hosted, joined) carries a split hook
        // of its own, which is what lets a split tablet be split again. Built
        // unconditionally: the join-host + re-host paths run on **every** node (a
        // spare not in the bootstrap set still hosts a tablet later placed on it).
        //
        // Load the durable marker first and pre-populate `minted`, so the bootstrap
        // group re-applying a committed `Split` on WAL recovery finds the tablet
        // already hosted and does not mint the sibling twice (#4 crash-idempotency).
        let recorded = load_hosted_cp(&self.raftkv_env).await;
        let minted: Arc<Mutex<BTreeSet<TabletId>>> = Arc::new(Mutex::new(
            recorded.iter().map(|h| TabletId(h.tablet)).collect(),
        ));
        let hosted = Arc::new(Mutex::new(recorded.clone()));
        let host = CpHostCtx {
            raftkv_env: raftkv_hook_env,
            ctx: ctx.clone(),
            backend,
            base_id: my_raftkv_id,
            minted: minted.clone(),
            hosted,
        };
        // Re-host each previously-split tablet from its on-disk engine (#2). Spawned so
        // a slow control plane (the address publish) does not block node start.
        for h in recorded {
            tokio::spawn(cp_rehost(host.clone(), h));
        }
        // No CP group is stood up at node start (ADR 0023): a fresh cluster has zero
        // data tablets. The per-node join-host loop (below) stands up each table's
        // group when `CreateTable` provisions its tablet — the first table's tablet
        // (`TABLET` = id 1) lands on this node's **main** `raftkv` env via
        // `cp_member_id`/`cp_join_host`, later tablets on minted siblings. On restart
        // the same loop re-hosts a provisioned tablet from its on-disk engine
        // (`LsmEngine::open` recovers), and `cp_rehost` re-hosts split children.

        // Bootstrap: whichever node is leader registers membership (no data tablet)
        // (idempotent). Track the client-facing task handles so `shutdown` can
        // abort them and release the client/dynamo/cql listener ports (these run
        // on plain `tokio::spawn`, off the `Env` network).
        let mut tasks = Vec::with_capacity(6);
        let raftkv_ids: Vec<NodeId> = (0..n).map(config::raftkv_id).collect();
        tasks.push(tokio::spawn(bootstrap(raft.clone(), raftkv_ids)));

        // Peer-sync loop (Phase 2.3a): keep the raftkv family's peer book =
        // `static ∪ Metadata.cp_member_addrs`, so a runtime-registered CP member
        // (split sibling / joined node) becomes reachable for the group's internal
        // Raft traffic. Runs on every node (harmless where no CP group is hosted).
        tasks.push(tokio::spawn(peer_sync_loop(
            raft.clone(),
            raftkv_sync_env,
            static_peers,
        )));

        // **Failure-detection heartbeat loop** (#3 / ADR 0012): every node heartbeats
        // the control group *as its `raftkv` member id* (the cluster members are the
        // `raftkv` ids, registered by `bootstrap`), so the control leader's
        // `detect_loop` marks a crashed CP node `Down`. Runs on every node; the
        // raftkv peer book includes the control addrs (the static book), so the
        // heartbeats reach the control group.
        tasks.push(tokio::spawn(heartbeat_loop(
            raftkv_hb_env,
            control_ids.clone(),
        )));

        // **CP reconfigure loop** (#3 / ADR 0017 Stage C): a CP-hosting node steps
        // each group it leads toward the tablet's replicated desired replica set —
        // the production counterpart of `spawn_reconfigure_loop` (decision in the
        // replicated placement, timing here). Runs on **every** node now (hosting is
        // dynamic — a node hosts a tablet's group once `CreateTable`/the reconciler
        // places it here); it only acts on tablets this node currently leads.
        tasks.push(tokio::spawn(cp_reconfigure_loop(ctx.clone())));

        // **CP join-host loop** (D1 — closes the failure->placement->reconfigure
        // cascade): a node placed in a tablet's replica set by the reconciler (e.g. a
        // spare picked to replace a `Down` replica) stands up an empty co-resident
        // group for it and catches up via `InstallSnapshot`. Runs on **every** node
        // (a spare is not in the bootstrap CP set, yet must host when later placed).
        tasks.push(tokio::spawn(cp_join_host_loop(host.clone())));

        // **CP GC loop** (ADR 0024 — drop-table teardown): the join-host loop's
        // dual. When a tablet this node hosts is dropped from the replicated map
        // (`DROP TABLE`), stop its group and delete its engine + WAL files, so a
        // dropped table's disk is actually reclaimed on every replica.
        tasks.push(tokio::spawn(cp_gc_loop(host)));

        // Client request server + DynamoDB HTTP + CQL endpoints share the same
        // context built above (the same raft view, RMW lock, and CP edge state).
        {
            // A CP-hosting node registers its `raftkv` address in the replicated
            // Metadata (Phase 2.3a), so peer-sync on every node can reach it. The
            // bootstrap members' addrs are already in the static peer book, so this
            // is the path a *new* member (split sibling / join) reuses.
            // Every node registers its base `raftkv` address so peer-sync on every
            // node can reach it (hosting is dynamic now — a node may host the first
            // table's tablet on this base env, and a node beyond the bootstrap set is
            // not in the static peer book). The per-tablet `cp_join_host` path also
            // registers a minted sibling's address when it stands a group up.
            {
                let ctx = ctx.clone();
                tasks.push(tokio::spawn(async move {
                    ctx.register_cp_addr(my_raftkv_id, my_raftkv_addr.to_string())
                        .await;
                }));
            }
            // Auto-split loop (Phase 2.4), opt-in: a node splits a tablet it leads
            // once it exceeds the key-count threshold (it checks leadership per tablet,
            // so running it on every node is harmless).
            if let Some(threshold) = auto_split_threshold {
                tasks.push(tokio::spawn(auto_split_loop(ctx.clone(), threshold)));
            }
            tasks.push(tokio::spawn(serve_clients(
                self.client_listener,
                ctx.clone(),
            )));
            tasks.push(tokio::spawn(dynamo::serve(
                self.dynamo_listener,
                ctx.clone(),
            )));
            // The admin / debug HTTP-JSON endpoint on its own port (ADR 0020).
            tasks.push(tokio::spawn(admin::serve(self.admin_listener, ctx.clone())));
            tasks.push(tokio::spawn(cql::serve(self.cql_listener, ctx)));
        }

        Ok(Node {
            raft,
            envs,
            tasks,
            client_addr: self.client_addr,
            dynamo_addr: self.dynamo_addr,
            cql_addr: self.cql_addr,
            admin_addr: self.admin_addr,
        })
    }
}

/// A running node. Holds the handles that keep its envs and tasks alive.
pub struct Node {
    raft: RaftNode<ProdEnv>,
    /// The node's two internal `ProdEnv` roles (control + raftkv), kept so
    /// [`shutdown`](Node::shutdown) can abort every task they own and free their
    /// listener ports.
    envs: [ProdEnv; 2],
    /// The client-facing listener tasks (client TCP / dynamo HTTP / cql), which
    /// run on plain `tokio::spawn` off the `Env` network; aborted on shutdown.
    tasks: Vec<tokio::task::JoinHandle<()>>,
    client_addr: SocketAddr,
    dynamo_addr: SocketAddr,
    cql_addr: SocketAddr,
    admin_addr: SocketAddr,
}

impl Node {
    /// Bind this node's listeners (the control + raftkv internal envs + the client
    /// TCP server + the DynamoDB HTTP and CQL endpoints) and create its data
    /// directory.
    ///
    /// # Errors
    /// Propagates any bind / directory-creation failure.
    pub async fn bind(
        control_id: NodeId,
        raftkv_id: NodeId,
        addrs: RoleAddrs,
        data_dir: impl Into<PathBuf>,
    ) -> std::io::Result<BoundNode> {
        let dir = data_dir.into();
        let (control_env, control_addr) =
            ProdEnv::bind(control_id, addrs.control, dir.join("control")).await?;
        // The leaderful CP per-tablet Raft role's internal env (ADR 0017 #3a) — the
        // v1 data plane; distinct id/port/dir from the control role (single-consumer
        // inbox). Bound with a **sibling listener pool** (Phase 2.2) so a tablet
        // split can mint a co-resident group member at runtime; the pool size bounds
        // how many co-resident CP groups this node can host.
        let pool: Vec<SocketAddr> = (0..CP_SIBLING_POOL)
            .map(|_| SocketAddr::new(addrs.raftkv.ip(), 0))
            .collect();
        let (raftkv_env, raftkv_addr) =
            ProdEnv::bind_with_pool(raftkv_id, addrs.raftkv, &pool, dir.join("raftkv")).await?;
        let client_listener = TcpListener::bind(addrs.client).await?;
        let client_addr = client_listener.local_addr()?;
        let dynamo_listener = TcpListener::bind(addrs.dynamo).await?;
        let dynamo_addr = dynamo_listener.local_addr()?;
        let cql_listener = TcpListener::bind(addrs.cql).await?;
        let cql_addr = cql_listener.local_addr()?;
        let admin_listener = TcpListener::bind(addrs.admin).await?;
        let admin_addr = admin_listener.local_addr()?;
        Ok(BoundNode {
            control_id,
            raftkv_id,
            control_env,
            raftkv_env,
            control_addr,
            raftkv_addr,
            client_listener,
            client_addr,
            dynamo_listener,
            dynamo_addr,
            cql_listener,
            cql_addr,
            admin_listener,
            admin_addr,
        })
    }

    /// The address clients connect to.
    pub fn client_addr(&self) -> SocketAddr {
        self.client_addr
    }

    /// The address the DynamoDB JSON/HTTP endpoint listens on.
    pub fn dynamo_addr(&self) -> SocketAddr {
        self.dynamo_addr
    }

    /// The address the CQL binary-protocol endpoint listens on.
    pub fn cql_addr(&self) -> SocketAddr {
        self.cql_addr
    }

    /// The address the admin / debug HTTP endpoint listens on (ADR 0020).
    pub fn admin_addr(&self) -> SocketAddr {
        self.admin_addr
    }

    /// Whether this node's control replica currently believes it is leader.
    pub fn is_control_leader(&self) -> bool {
        self.raft.is_leader()
    }

    /// This node's cached cluster metadata.
    pub fn metadata(&self) -> Metadata {
        self.raft.metadata()
    }

    /// Propose a control-plane [`MetaCommand`] on this node's control replica,
    /// returning whether it was accepted (i.e. this node is the leader). The
    /// interim admin hook for cluster metadata operations the wire edges do not
    /// yet expose — notably marking a table **CP** (ADR 0017 #3a) via
    /// `MetaCommand::SetTableMode`. A non-leader proposal is dropped (`false`); the
    /// caller retries on the current leader. Replication + durability are the
    /// control plane's (the command commits through Raft).
    pub fn propose_meta(&self, command: MetaCommand) -> bool {
        matches!(self.raft.propose(command), ProposeResult::Accepted { .. })
    }

    /// Gracefully stop the node: abort its client-facing listeners (client /
    /// dynamo / cql) and every task its two internal `ProdEnv` roles own (the
    /// control + CP Raft drivers and the internal accept loops). This releases all
    /// five listener ports so a replacement node can rebind the same addresses on
    /// the same data directory — the clean teardown a stopped OS process would
    /// otherwise provide. Idempotent.
    ///
    /// On-disk state is unaffected: a value already acked to a client was Raft-
    /// committed + fsynced to the CP group's LSM WAL before the ack, so it survives
    /// the restart.
    pub fn shutdown(&self) {
        for task in &self.tasks {
            task.abort();
        }
        for env in &self.envs {
            env.shutdown();
        }
    }

    /// Graceful teardown: durably flush the control-plane WAL **before** aborting
    /// the node's tasks, then [`shutdown`](Self::shutdown).
    ///
    /// `shutdown` alone aborts the Raft driver, but a `MetaCommand` (e.g. a
    /// `CreateTable` schema proposal) is applied + acked **synchronously** in
    /// `propose` while the driver fsyncs the WAL asynchronously — and the driver is
    /// usually parked between ticks. So a bare `shutdown` can abort the driver in
    /// the apply→fsync window and lose an *acked* schema across a restart (the
    /// flaky `tests/dynamo_schema.rs::create_table_survives_node_restart`).
    /// `RaftNode::flush` syncs that pending tail first, so a clean teardown is
    /// actually durable — which is what a restart test (a clean teardown standing
    /// in for an OS process restart) needs. (A `kill -9` is still exposed; the
    /// durable-before-ack control-plane fix is a tracked follow-up.)
    pub async fn shutdown_graceful(&self) {
        self.raft.flush().await;
        self.shutdown();
    }
}

/// The wire edges' mutable state, scoped to **one cluster** (one process in
/// `--cluster N` mode; one node in one-process-per-node mode) rather than to the
/// whole process (ADR 0013). Holding it here — threaded through [`ClientCtx`] —
/// instead of in `OnceLock` process statics is what lets a test harness run
/// several independent clusters in one process without their edge state leaking
/// across them (registries, prepared statements, and especially the set of
/// control handles a schema proposal fans out to).
///
/// Cloning shares the same underlying state (it is `Arc`-backed), so every node
/// and every connection of a cluster sees one registry / handle set; a fresh
/// [`ClusterEdgeState::new`] is a distinct, isolated set.
#[derive(Clone)]
pub struct ClusterEdgeState {
    /// This cluster's control `RaftNode` handles, so a wire edge (CQL/DynamoDB)
    /// can reach the control-plane **leader** to propose a schema `MetaCommand`
    /// even when the client connected to a follower (ADR 0013). In `--cluster N`
    /// mode every node of the cluster registers here, so the leader is always
    /// present; in one-process-per-node mode only the local handle is registered
    /// (cross-process proposal forwarding is future work — DDL then commits when
    /// this node is the leader, like the bootstrap path).
    control: Arc<Mutex<Vec<RaftNode<ProdEnv>>>>,
    /// The DynamoDB edge's in-memory GSI declarations + observation-built
    /// written-key index (ADR 0006). Not durable / not replicated; per-cluster.
    dynamo_registry: Arc<Mutex<animus_dynamo::SchemaRegistry>>,
    /// The CQL edge's keyspaces + prepared-statement store (ADR 0013). Not
    /// durable / not replicated; per-cluster.
    cql_state: Arc<tokio::sync::Mutex<cql::CqlState>>,
    /// This cluster's **leaderful CP** per-tablet Raft group handles (ADR 0017 #3a),
    /// **keyed by tablet** so a wire edge routes a key to its owning tablet's group
    /// **leader** (Phase 2: multi-tablet CP). Each tablet maps to the locally-hosted
    /// group handle(s) for it. In `--cluster N` mode every hosting node registers
    /// here (so the leader is always present in-process); one-process-per-node
    /// registers only the local handle (cross-process routing forwards). Today the
    /// cluster has one whole-keyspace tablet, so there is one entry; a tablet split
    /// adds another.
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
            cql_state: Arc::new(tokio::sync::Mutex::new(cql::CqlState::default())),
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

    /// Remove and return **one node's** registered handle for `tablet` — the one
    /// whose env runs as group member `member` — dropping the tablet's entry once
    /// the last handle is gone (drop-table GC, ADR 0024). Matched per member id
    /// because in an in-process `--cluster N` run this edge is **shared** across
    /// nodes: every replica's GC loop must reclaim its *own* group, not whichever
    /// handle happens to be first. `None` if no such handle is registered (e.g.
    /// the stand-up path claimed the tablet but has not registered yet — the
    /// caller retries on a later tick rather than GC-ing a group mid-standup).
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

    /// The DynamoDB edge's per-cluster registry.
    pub(crate) fn dynamo_registry(&self) -> &Arc<Mutex<animus_dynamo::SchemaRegistry>> {
        &self.dynamo_registry
    }

    /// The CQL edge's per-cluster state.
    pub(crate) fn cql_state(&self) -> &Arc<tokio::sync::Mutex<cql::CqlState>> {
        &self.cql_state
    }
}

/// Shared context for the client request server and the DynamoDB/CQL endpoints:
/// the control `RaftNode` (for cached metadata + schema proposals), the per-node
/// RMW serialization lock, the per-cluster wire-edge state (incl. the CP group
/// handles), and the cross-process CP routing table.
#[derive(Clone)]
pub(crate) struct ClientCtx {
    raft: RaftNode<ProdEnv>,
    /// Serializes a node's read-modify-writes so a CQL/DynamoDB RMW (linearizable
    /// CP read → CP write) is atomic *per node*. Cross-node atomicity (a CAS on the
    /// CP group) is later v1 work.
    pub(crate) rmw_lock: Arc<tokio::sync::Mutex<()>>,
    pub(crate) edge: ClusterEdgeState,
    /// The raftkv-role env's recording metrics sink (the CP group records here).
    /// Aggregated into the `/metrics` export (ADR 0015).
    raftkv_metrics: MetricsHandle,
    /// CP-group routing table: each CP group member id (`raftkv_id`, `300+i`) → the
    /// **client API** address of its hosting node (ADR 0017 #3b). Lets a node that
    /// received a CP op but doesn't host the group leader **forward** the request to
    /// the leader's node. Built from the cluster config/bound addresses; empty in a
    /// single-process `--cluster N` run (where the shared edge state already reaches
    /// every group handle in-process, so no forwarding is needed).
    client_route: BTreeMap<NodeId, SocketAddr>,
    /// This node's **base `raftkv` id** — its identity in a tablet's replica set
    /// (ADR 0023). Used by routing to tell "this node is a replica of the tablet, so
    /// wait for its own group to form" from "this node hosts nothing for the tablet,
    /// so forward."
    base_id: NodeId,
    /// This node's identity + bound addresses for the admin `/admin/config` view
    /// (ADR 0020). `Arc` so cloning the ctx onto each connection is cheap.
    admin: Arc<AdminInfo>,
}

impl ClientCtx {
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
        topology::tablet_for_key(self.raft.metadata().tablets_for_table(table), key)
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
            if let Some(tablet) = self.tablet_for(table, key) {
                if let Some(route) = self.resolve_cp_route(tablet) {
                    return route;
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return CpRoute::None;
            }
            tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// One attempt at resolving a *known* tablet's group leader to a [`CpRoute`], or
    /// `None` if it isn't settled yet (caller should wait + retry). The leader-
    /// resolution policy shared by [`cp_route`](Self::cp_route) (key→tablet→leader)
    /// and [`cp_route_tablet`](Self::cp_route_tablet) (tablet→leader, for the split
    /// trigger, where the key maps to a *different* tablet after the metadata split).
    ///
    /// The branching itself — serve locally / forward-to-hint / forward-anywhere
    /// / wait — is the pure [`topology::decide_cp_route`]; this method's job is
    /// only to gather its inputs (cheaply, and lazily where a fact needs a
    /// `Metadata` deep clone) and execute the resulting decision.
    fn resolve_cp_route(&self, tablet: TabletId) -> Option<CpRoute> {
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
        let has_local_replica = self.edge.local_cp(tablet).is_some();
        let (is_replica, fallback_forward) = if has_local_replica {
            (false, None)
        } else {
            let meta = self.raft.metadata();
            let replicas = meta.tablets.get(&tablet).map(|t| &t.replicas);
            let is_replica = replicas.is_some_and(|r| r.contains(&self.base_id));
            let fallback = replicas
                .into_iter()
                .flatten()
                .find_map(|id| self.client_route.get(id).copied())
                .or_else(|| self.client_route.values().next().copied());
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

    /// Resolve a CP op to a leader by **tablet id** (not key), waiting up to
    /// [`CLIENT_TIMEOUT`] for the group to settle. Used by the split trigger: after
    /// the metadata split commits, the split *key* maps to the new (right-hand)
    /// tablet, so routing the data-plane split by key would target the wrong group —
    /// it must route to the tablet being split by id.
    async fn cp_route_tablet(&self, tablet: TabletId) -> CpRoute {
        let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
        loop {
            if let Some(route) = self.resolve_cp_route(tablet) {
                return route;
            }
            if tokio::time::Instant::now() >= deadline {
                return CpRoute::None;
            }
            tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// Linearizable CP **read** of `key` (ADR 0017): ReadIndex on the group leader,
    /// forwarded to the leader's node if this node isn't it. `Ok(None)` is an
    /// absent key; `Err` is "no leader reachable" (never a stale value — a deposed
    /// leader's ReadIndex returns `None`, treated as absent). The CP read primitive
    /// the wire edges call directly.
    pub(crate) async fn cp_read(
        &self,
        table: &str,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, String> {
        match self.cp_route(table, &key).await {
            CpRoute::Local(leader) => Ok(leader.linearizable_get(&key).await),
            CpRoute::Forward(addr) => {
                match self
                    .cp_forward(
                        addr,
                        ClientRequest::Get {
                            key,
                            table: table.to_owned(),
                        },
                    )
                    .await
                {
                    ClientResponse::Value(v) => Ok(v),
                    ClientResponse::Error(e) => Err(e),
                    other => Err(format!("unexpected reply to forwarded CP read: {other:?}")),
                }
            }
            CpRoute::None => Err("no CP group leader reachable".into()),
        }
    }

    /// CP **write** of `key = value` (ADR 0017): propose on the group leader and
    /// wait until the value is committed + durable + applied — a linearizable read
    /// reflects it — before returning `Ok` (durable-before-ack). Forwarded if this
    /// node isn't the leader. The CP write primitive the wire edges call directly.
    pub(crate) async fn cp_write(
        &self,
        table: &str,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), String> {
        match self.cp_route(table, &key).await {
            CpRoute::Local(leader) => Self::cp_put_local(&leader, key, value).await,
            CpRoute::Forward(addr) => Self::ok_or_err(
                self.cp_forward(
                    addr,
                    ClientRequest::Put {
                        key,
                        value,
                        table: table.to_owned(),
                    },
                )
                .await,
                "forwarded CP write",
            ),
            CpRoute::None => Err("no CP group leader reachable".into()),
        }
    }

    /// CP **batch write** of many `(key, value)` pairs of `table` (ADR 0017 —
    /// bulk-write batching): the keys are grouped by their owning **tablet**, and
    /// each group is committed as **one** `Batch` Raft entry on that tablet's group
    /// leader (one propose → one commit round → one apply; forwarded cross-process
    /// if this node isn't the leader), waited to durable+applied before returning.
    /// **Within a tablet the batch is atomic** (one entry — it commits whole or not
    /// at all); **across tablets it is not** (each tablet commits independently),
    /// which matches real DynamoDB `BatchWriteItem` (non-atomic) semantics. Takes an
    /// arbitrary `N` (the wire edge caps its own surface). Provisions `table`'s first
    /// tablet on demand, like [`cp_write`](Self::cp_write). The bulk-write throughput
    /// primitive behind DynamoDB `BatchWriteItem` and the admin bulk seeder.
    pub(crate) async fn cp_batch_write(
        &self,
        table: &str,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), String> {
        if entries.is_empty() {
            return Ok(());
        }
        // Auto-provision the table's tablet on first write (ADR 0023), as `cp_write`.
        if !self.raft.metadata().has_table_tablet(table) {
            self.provision_tablet(table).await?;
        }
        // Group by owning tablet: every key of a `Batch` entry must belong to the
        // one tablet whose leader commits it (a tablet's engine holds only its
        // range). A freshly created table has a single whole-ring tablet (one
        // group); a split table fans the batch across its halves.
        let mut groups: BTreeMap<TabletId, KvPairs> = BTreeMap::new();
        for (key, value) in entries {
            let tablet = self
                .tablet_for(table, &key)
                .ok_or_else(|| format!("no tablet owns a batch key of table `{table}`"))?;
            groups.entry(tablet).or_default().push((key, value));
        }
        for (_tablet, group) in groups {
            self.cp_batch_write_group(table, group).await?;
        }
        Ok(())
    }

    /// Write one tablet's group of a batch as a single `Batch` entry: all keys share
    /// the tablet, so route by the group's first key, then serve locally or forward
    /// one hop (the batch analog of [`cp_write`](Self::cp_write)'s route).
    async fn cp_batch_write_group(
        &self,
        table: &str,
        group: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), String> {
        let Some(first) = group.first().map(|(k, _)| k.clone()) else {
            return Ok(());
        };
        match self.cp_route(table, &first).await {
            CpRoute::Local(leader) => Self::cp_batch_local(&leader, group).await,
            CpRoute::Forward(addr) => Self::ok_or_err(
                self.cp_forward(
                    addr,
                    ClientRequest::PutBatch {
                        entries: group,
                        table: table.to_owned(),
                    },
                )
                .await,
                "forwarded CP batch write",
            ),
            CpRoute::None => Err("no CP group leader reachable".into()),
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
    fn cp_batch_propose(
        leader: &CpGroup,
        group: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<Option<KvPair>, String> {
        let probe = group.last().cloned();
        match leader.put_batch(group) {
            ProposeResult::Accepted { .. } => Ok(probe),
            ProposeResult::NotLeader { .. } => Err("CP group leader moved; retry".into()),
        }
    }

    /// Poll `leader`'s local engine for `probe_key` to reflect `probe_val` until
    /// `deadline` — the durable-before-ack confirm wait shared by every CP write
    /// path (mirrors [`cp_put_local`](Self::cp_put_local)).
    async fn poll_probe(
        leader: &CpGroup,
        probe_key: &[u8],
        probe_val: &[u8],
        deadline: tokio::time::Instant,
    ) -> bool {
        loop {
            if leader.local_get(probe_key).await.as_deref() == Some(probe_val) {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
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
        let Some((probe_key, probe_val)) = Self::cp_batch_propose(leader, group)? else {
            return Ok(());
        };
        let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
        if Self::poll_probe(leader, &probe_key, &probe_val, deadline).await {
            Ok(())
        } else {
            Err("CP batch write did not commit in time".into())
        }
    }

    /// Like [`cp_batch_write`](Self::cp_batch_write), but for a caller that
    /// itself retries on failure (the admin bulk seeder,
    /// [`admin::action_data_seed`](crate::admin::action_data_seed)) — up to
    /// `attempts` tries per tablet group, backing off `retry_backoff` between
    /// them.
    ///
    /// **Why this exists rather than just looping `cp_batch_write`:** a bare
    /// confirm-timeout from [`cp_batch_local`](Self::cp_batch_local) does not mean
    /// the batch is lost — `ProposeResult::Accepted` only means it reached the
    /// leader's local log; under a slow or contended commit path (a slow disk, a
    /// growing number of concurrent per-tablet Raft groups) it can still be
    /// committing well after [`CLIENT_TIMEOUT`] has elapsed. A caller that
    /// retries by calling `cp_batch_write` again unconditionally proposes a
    /// **second, fully duplicate** `Batch` entry for the same keys — safe by
    /// per-key LWW, but it doubles the outstanding replication/fsync work for no
    /// benefit, compounding under exactly the conditions that caused the
    /// timeout (observed turning a slow bulk-seed into an apparent pile of
    /// "did not commit in time" failures with `commit_index` still climbing the
    /// whole time — no leader ever actually changed).
    ///
    /// So per tablet group, this proposes **at most once per attempt** and only
    /// when the previous attempt didn't get as far as `Accepted`: on a plain
    /// confirm-timeout it polls the *same* already-proposed entry for a second
    /// full [`CLIENT_TIMEOUT`] window before considering the attempt to have
    /// failed, rather than proposing again. A genuinely stale route (the classic
    /// case: a tablet split moved the target range mid-seed, so the old leader's
    /// copy is truncated/tombstoned) is still retried with a fresh propose, since
    /// `cp_route` re-resolves the current tablet map on every attempt.
    pub(crate) async fn cp_batch_write_patient(
        &self,
        table: &str,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        attempts: usize,
        retry_backoff: Duration,
    ) -> Result<(), String> {
        if entries.is_empty() {
            return Ok(());
        }
        if !self.raft.metadata().has_table_tablet(table) {
            self.provision_tablet(table).await?;
        }
        let mut groups: BTreeMap<TabletId, KvPairs> = BTreeMap::new();
        for (key, value) in entries {
            let tablet = self
                .tablet_for(table, &key)
                .ok_or_else(|| format!("no tablet owns a batch key of table `{table}`"))?;
            groups.entry(tablet).or_default().push((key, value));
        }
        for (_tablet, group) in groups {
            self.cp_batch_write_group_patient(table, group, attempts, retry_backoff)
                .await?;
        }
        Ok(())
    }

    /// One tablet group's share of [`cp_batch_write_patient`] — see its doc for
    /// why a plain confirm-timeout polls the existing entry instead of proposing
    /// a fresh one.
    async fn cp_batch_write_group_patient(
        &self,
        table: &str,
        group: KvPairs,
        attempts: usize,
        retry_backoff: Duration,
    ) -> Result<(), String> {
        let Some(first) = group.first().map(|(k, _)| k.clone()) else {
            return Ok(());
        };
        let mut last_err = String::new();
        for attempt in 0..attempts.max(1) {
            match self.cp_route(table, &first).await {
                CpRoute::Local(leader) => match Self::cp_batch_propose(&leader, group.clone()) {
                    Ok(None) => return Ok(()),
                    Ok(Some((probe_key, probe_val))) => {
                        let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
                        if Self::poll_probe(&leader, &probe_key, &probe_val, deadline).await {
                            return Ok(());
                        }
                        // Accepted but not yet confirmed — keep polling the same
                        // entry for a second full window instead of proposing a
                        // duplicate (see the module doc above).
                        let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
                        if Self::poll_probe(&leader, &probe_key, &probe_val, deadline).await {
                            return Ok(());
                        }
                        last_err = "CP batch write did not commit in time".into();
                    }
                    Err(e) => last_err = e,
                },
                CpRoute::Forward(addr) => {
                    match self
                        .cp_forward(
                            addr,
                            ClientRequest::PutBatch {
                                entries: group.clone(),
                                table: table.to_owned(),
                            },
                        )
                        .await
                    {
                        ClientResponse::PutOk => return Ok(()),
                        ClientResponse::Error(e) => last_err = e,
                        other => {
                            last_err =
                                format!("unexpected reply to forwarded CP batch write: {other:?}")
                        }
                    }
                }
                CpRoute::None => last_err = "no CP group leader reachable".into(),
            }
            if attempt + 1 < attempts {
                tokio::time::sleep(retry_backoff).await;
            }
        }
        Err(last_err)
    }

    /// CP **delete** of `key` (ADR 0017): a Raft-committed tombstone, waited to
    /// durable+applied (a linearizable read then reads `None`) before returning.
    /// Forwarded if this node isn't the leader. Used by the CQL whole-partition
    /// delete; the DynamoDB edge instead writes a sentinel tombstone *value* via
    /// [`cp_write`](Self::cp_write).
    pub(crate) async fn cp_delete(&self, table: &str, key: Vec<u8>) -> Result<(), String> {
        match self.cp_route(table, &key).await {
            CpRoute::Local(leader) => Self::cp_delete_local(&leader, key).await,
            CpRoute::Forward(addr) => Self::ok_or_err(
                self.cp_forward(
                    addr,
                    ClientRequest::Delete {
                        key,
                        table: table.to_owned(),
                    },
                )
                .await,
                "forwarded CP delete",
            ),
            CpRoute::None => Err("no CP group leader reachable".into()),
        }
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
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        // The table's tablets overlapping [start, end), in token (range.start) order.
        // `end == None` is unbounded above (a whole-table scan).
        let mut ranges: Vec<KeyRange> = self
            .raft
            .metadata()
            .tablets_for_table(table)
            .map(|(_, t)| t.range.clone())
            .filter(|r| {
                // [r.start, r.end) overlaps [start, end), each upper bound optional.
                end.as_deref().is_none_or(|e| r.start.as_slice() < e)
                    && r.end.as_deref().is_none_or(|re| start.as_slice() < re)
            })
            .collect();
        ranges.sort();
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for r in ranges {
            if let Some(l) = limit {
                if out.len() >= l {
                    break;
                }
            }
            // Clip the scan window to this tablet's sub-range; the exclusive upper
            // bound is the lesser of the tablet's end and the scan's end (None = ∞).
            let sub_start = start.clone().max(r.start);
            let sub_end: Option<Vec<u8>> = match (r.end, &end) {
                (None, e) => e.clone(),
                (Some(re), None) => Some(re),
                (Some(re), Some(e)) => Some(re.min(e.clone())),
            };
            if let Some(se) = &sub_end {
                if sub_start.as_slice() >= se.as_slice() {
                    continue;
                }
            }
            let remaining = limit.map(|l| l - out.len());
            out.extend(
                self.cp_scan_one(table, sub_start, sub_end, remaining)
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
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        match self.cp_route(table, &start).await {
            CpRoute::Local(leader) => leader
                .linearizable_scan(&start, end.as_deref(), limit)
                .await
                .ok_or_else(|| "CP group leader moved; retry".into()),
            CpRoute::Forward(addr) => {
                match self
                    .cp_forward(
                        addr,
                        ClientRequest::Scan {
                            start,
                            end,
                            limit,
                            table: table.to_owned(),
                        },
                    )
                    .await
                {
                    ClientResponse::Pairs(p) => Ok(p),
                    ClientResponse::Error(e) => Err(e),
                    other => Err(format!("unexpected reply to forwarded CP scan: {other:?}")),
                }
            }
            CpRoute::None => Err("no CP group leader reachable".into()),
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
    /// entry is truncated and never appears locally → we time out, which is
    /// correct: the write did not commit.)
    async fn cp_put_local(leader: &CpGroup, key: Vec<u8>, value: Vec<u8>) -> Result<(), String> {
        match leader.put(key.clone(), value.clone()) {
            ProposeResult::Accepted { .. } => {
                let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
                let mut poll = CP_CONFIRM_POLL_INIT;
                loop {
                    if leader.local_get(&key).await.as_deref() == Some(value.as_slice()) {
                        return Ok(());
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
    /// [`cp_put_local`](Self::cp_put_local).
    async fn cp_delete_local(leader: &CpGroup, key: Vec<u8>) -> Result<(), String> {
        match leader.delete(key.clone()) {
            ProposeResult::Accepted { .. } => {
                let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
                let mut poll = CP_CONFIRM_POLL_INIT;
                loop {
                    if leader.local_get(&key).await.is_none() {
                        return Ok(());
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

    /// Confirm a `propose_split(split_key)` that returned `Accepted` actually
    /// committed **at that key** before trusting it — the split-path counterpart of
    /// [`cp_put_local`](Self::cp_put_local)'s local-read confirm. `Accepted` only
    /// means the `Split` entry was appended to the leader's local log, never that
    /// it committed (see [`RaftKvNode::applied_split_key`]'s doc): under leader
    /// churn the entry can be silently truncated before it commits. Comparing the
    /// *exact* key (not just "has this group split at all") also catches a
    /// narrower same-tick race: under `--cluster N`'s shared edge state, more than
    /// one node can independently read this tablet's live pairs and compute a
    /// *different* median in the same tick, then both propose a split on the same
    /// group — the group splits once, so the loser's own key never applies even
    /// though *a* split did. Skipping this check (or checking only a bare
    /// split-happened flag) is exactly the bug that let `propose_split_data`
    /// report success for a split that never actually happened at the caller's
    /// key — the control-plane `SplitTablet` metadata was already committed by
    /// that point (`propose_split_metadata`), so an unconfirmed "success"
    /// permanently strands that tablet (`leader: unknown` forever, nothing left to
    /// retry it). On timeout, return an error so the caller's retry path
    /// (`auto_split_loop`'s `pending` map) engages instead.
    async fn confirm_split(leader: &CpGroup, split_key: &[u8]) -> ClientResponse {
        let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
        let mut poll = CP_CONFIRM_POLL_INIT;
        loop {
            if leader.applied_split_key().as_deref() == Some(split_key) {
                return ClientResponse::PutOk;
            }
            if tokio::time::Instant::now() >= deadline {
                return ClientResponse::Error("CP split did not commit in time".into());
            }
            tokio::time::sleep(poll).await;
            poll = (poll * 2).min(CP_CONFIRM_POLL_MAX);
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

    /// Route a CP-mode **write** for the plain client API (returns a wire
    /// [`ClientResponse`]). Thin adapter over [`cp_write`](Self::cp_write).
    async fn cp_put(&self, table: &str, key: Vec<u8>, value: Vec<u8>) -> ClientResponse {
        // Auto-provision the table's tablet on first write (ADR 0023): the raw KV
        // client names a table but issues no DDL, so stand one up on demand.
        if !self.raft.metadata().has_table_tablet(table) {
            if let Err(e) = self.provision_tablet(table).await {
                return ClientResponse::Error(e);
            }
        }
        match self.cp_write(table, key, value).await {
            Ok(()) => ClientResponse::PutOk,
            Err(e) => ClientResponse::Error(e),
        }
    }

    /// Route a CP-mode **read** for the plain client API (returns a wire
    /// [`ClientResponse`]). Thin adapter over [`cp_read`](Self::cp_read).
    async fn cp_get(&self, table: &str, key: Vec<u8>) -> ClientResponse {
        // A table with no tablet has no data (ADR 0023) — absent, no routing wait.
        if !self.raft.metadata().has_table_tablet(table) {
            return ClientResponse::Value(None);
        }
        match self.cp_read(table, key).await {
            Ok(v) => ClientResponse::Value(v),
            Err(e) => ClientResponse::Error(e),
        }
    }

    /// The client-API address to forward a `tablet` op to: the hosting node of that
    /// tablet's group leader as a local replica currently sees it (`leader()` hint →
    /// `client_route`). `None` if this node hosts no replica of `tablet`, the replica
    /// has no leader hint yet (mid-election — the caller waits rather than guessing,
    /// so it never forwards a CP op to a non-leader, including itself), or the hinted
    /// id has no known route.
    fn cp_forward_target(&self, tablet: TabletId) -> Option<SocketAddr> {
        // The local replica's leader hint is a group **member id** (derived for a
        // non-bootstrap tablet); `client_route` is keyed by stable **base** node
        // ids, so translate back (ADR 0017 #4 — the reverse of `cp_members_for`).
        // Without this, a healthy remote leader of a provisioned/split tablet is
        // unroutable from a follower node: the lookup misses, and because a local
        // replica exists, `resolve_cp_route` waits out CLIENT_TIMEOUT instead of
        // forwarding — "no CP group leader reachable" on a led group.
        let leader_member = self.edge.local_cp(tablet).and_then(|n| n.leader())?;
        self.client_route
            .get(&cp_base_id(leader_member, tablet))
            .copied()
    }

    /// Forward a CP op to another node's client API (wrapped so the receiver
    /// serves-or-errors, never re-forwards) and relay its reply. Carries the
    /// current span's trace context (ADR 0027) so the receiving node's
    /// handling of the forwarded op joins the same distributed trace.
    async fn cp_forward(&self, addr: SocketAddr, request: ClientRequest) -> ClientResponse {
        self.relay(
            addr,
            ClientRequest::Forwarded {
                request: Box::new(request),
                traceparent: crate::otel::current_traceparent(),
            },
        )
        .await
    }

    /// Send `request` to a peer node's client API over a fresh connection and
    /// return its reply (or an error on any transport failure). The cross-node
    /// relay primitive for CP forwarding (A1) and schema-DDL relay (A2).
    async fn relay(&self, addr: SocketAddr, request: ClientRequest) -> ClientResponse {
        match tokio::time::timeout(CLIENT_TIMEOUT, async {
            let mut stream = TcpStream::connect(addr).await.ok()?;
            write_frame(&mut stream, &request).await.ok()?;
            read_frame::<ClientResponse>(&mut stream).await.ok()?
        })
        .await
        {
            Ok(Some(resp)) => resp,
            _ => ClientResponse::Error("relay to peer node failed".into()),
        }
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
        if let Some(leader_id) = self.raft.leader() {
            if let Some(&addr) = self.client_route.get(&leader_id) {
                return !matches!(
                    self.relay(addr, ClientRequest::ProposeSchema(command.clone()))
                        .await,
                    ClientResponse::Error(_)
                );
            }
        }
        false
    }

    /// This node's leader handle for the tablet of `table` owning `key`, if it hosts
    /// it and leads — the local serve target for a forwarded op.
    fn cp_leader_for(&self, table: &str, key: &[u8]) -> Option<CpGroup> {
        self.tablet_for(table, key)
            .and_then(|t| self.edge.cp_leader(t))
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
        loop {
            let meta = self.raft.metadata();
            if let Some((&tablet, t)) = meta.tablets_for_table(table).next() {
                // The tablet exists; ensure its RF policy is set, then we're done. The
                // caller's op routes through `cp_route`, which itself waits for the
                // group to form/elect (`CLIENT_TIMEOUT`), so provisioning need not
                // block on serveability here.
                if meta.policies.contains_key(&tablet) {
                    return Ok(());
                }
                self.propose_schema(&MetaCommand::SetTabletPolicy {
                    tablet,
                    policy: Some(PlacementPolicy::simple("cp-rf", t.replicas.len())),
                })
                .await;
            } else {
                // No tablet yet: pick the first min(N, RF) Active CP members and
                // propose its creation toward the control leader.
                let mut replicas: Vec<NodeId> = meta
                    .members
                    .iter()
                    .filter(|(_, m)| m.status == NodeStatus::Active)
                    .map(|(id, _)| *id)
                    .collect();
                replicas.truncate(MAX_REPLICATION_FACTOR);
                if !replicas.is_empty() {
                    self.propose_schema(&MetaCommand::CreateTablet {
                        tablet: meta.next_free_tablet_id(),
                        table: Some(table.to_owned()),
                        range: KeyRange::whole(),
                        replicas,
                    })
                    .await;
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("table tablet did not provision in time".into());
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
                let Some(leader) = self.cp_leader_for(&table, &key) else {
                    return ClientResponse::Error("forwarded CP op: not the leader here".into());
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
                let Some(leader) = self.cp_leader_for(&table, &first) else {
                    return ClientResponse::Error("forwarded CP op: not the leader here".into());
                };
                match Self::cp_batch_local(&leader, entries).await {
                    Ok(()) => ClientResponse::PutOk,
                    Err(e) => ClientResponse::Error(e),
                }
            }
            ClientRequest::Get { key, table } => match self.cp_leader_for(&table, &key) {
                Some(leader) => ClientResponse::Value(leader.linearizable_get(&key).await),
                None => ClientResponse::Error("forwarded CP op: not the leader here".into()),
            },
            ClientRequest::Delete { key, table } => {
                let Some(leader) = self.cp_leader_for(&table, &key) else {
                    return ClientResponse::Error("forwarded CP op: not the leader here".into());
                };
                match Self::cp_delete_local(&leader, key).await {
                    Ok(()) => ClientResponse::PutOk,
                    Err(e) => ClientResponse::Error(e),
                }
            }
            ClientRequest::Scan {
                start,
                end,
                limit,
                table,
            } => {
                let Some(leader) = self.cp_leader_for(&table, &start) else {
                    return ClientResponse::Error("forwarded CP op: not the leader here".into());
                };
                match leader
                    .linearizable_scan(&start, end.as_deref(), limit)
                    .await
                {
                    Some(p) => ClientResponse::Pairs(p),
                    None => ClientResponse::Error("CP group leader moved; retry".into()),
                }
            }
            _ => ClientResponse::Error("unexpected forwarded request".into()),
        }
    }

    /// Render this node's **live** metrics as the ADR 0015 text export
    /// (`name value` lines), aggregated across the node's two role sinks.
    ///
    /// A node runs two internal `ProdEnv` roles on distinct ids — control (Raft)
    /// and raftkv (the CP group) — and each records into its **own** sink
    /// (`RaftNode::start` records into the control env's sink; the CP group into
    /// the raftkv env's). To surface both control- and CP-data-plane counters from
    /// one endpoint, this sums the two snapshots counter-by-counter and takes the
    /// max of the leadership gauge (leadership is the control plane's, recorded only
    /// in the control sink). The snapshots are read **at call time**, so the export
    /// reflects current activity rather than a cached value.
    pub(crate) fn metrics_text(&self) -> String {
        let snaps = [
            self.raft.metrics().snapshot(),
            self.raftkv_metrics.snapshot(),
        ];
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
    /// (ADR 0020). Read live at call time and summed across the node's two role
    /// sinks, exactly as the text export.
    pub(crate) fn metrics_json(&self) -> (BTreeMap<String, u64>, i64) {
        let snaps = [
            self.raft.metrics().snapshot(),
            self.raftkv_metrics.snapshot(),
        ];
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

    /// **Admin action (ADR 0020):** mark `node` `Leaving` so the placement
    /// reconciler moves its replicas off. Proposed on the **local** control leader
    /// handle (membership commands are control-plane-internal and not relayable, so
    /// this requires the receiving node to be the control leader; a follower
    /// returns an error and the operator retries on the leader). Preserves the
    /// member's existing labels. Returns the accepted state or an error.
    pub(crate) fn admin_drain(&self, node: NodeId) -> Result<(), String> {
        let meta = self.raft.metadata();
        let Some(member) = meta.members.get(&node) else {
            return Err(format!("node {node} is not a cluster member"));
        };
        let labels = member.labels.clone();
        let Some(leader) = self.edge.leader_handle() else {
            return Err("this node is not the control-plane leader; retry on the leader".into());
        };
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
            for &node in &raftkv_ids {
                if !meta.members.contains_key(&node) {
                    raft.propose(MetaCommand::UpsertMember {
                        node,
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

/// Keep the `raftkv` env family's peer book = the **static** book ∪ the replicated
/// `Metadata.cp_member_addrs` (Phase 2.3a address distribution). `set_peers`
/// replaces the book and a `sibling` env shares the same book `Arc`, so syncing the
/// `raftkv` env reaches every co-resident CP group. Idempotent each tick; runs for
/// the life of the node (a perpetual loop, aborted on `shutdown`). A peer entry
/// whose address fails to parse is skipped (the control plane stores it opaquely).
async fn peer_sync_loop(
    raft: RaftNode<ProdEnv>,
    raftkv_env: ProdEnv,
    static_peers: BTreeMap<NodeId, SocketAddr>,
) {
    loop {
        let mut book = static_peers.clone();
        for (id, addr) in raft.metadata().cp_member_addrs {
            if let Ok(sa) = addr.parse::<SocketAddr>() {
                book.insert(id, sa);
            }
        }
        raftkv_env.set_peers(book);
        tokio::time::sleep(PEER_SYNC_INTERVAL).await;
    }
}

/// Everything the CP **hosting** paths share (D3): the env to mint sibling inboxes
/// from, the client context (routing + address publish), the storage backend, this
/// node's **base** `raftkv` id, and the per-node `minted` (hosting-claim) + `hosted`
/// (durable-marker mirror) sets. Bundled so **every** group this node stands up —
/// bootstrap, split child, re-hosted, or joined — can carry a `SplitHook` of its own
/// ([`cp_split_hook`]), which is what lets a split-created tablet be split *again*
/// (deep splits / continued auto-sharding).
#[derive(Clone)]
struct CpHostCtx {
    raftkv_env: ProdEnv,
    ctx: ClientCtx,
    backend: StorageBackend,
    base_id: NodeId,
    minted: Arc<Mutex<BTreeSet<TabletId>>>,
    hosted: Arc<Mutex<Vec<HostedCpTablet>>>,
}

// `cp_member_id` / `cp_base_id` (base <-> derived CP group member id translation)
// now live in `topology` as pure, unit-tested functions; imported via
// `use topology::{cp_base_id, cp_member_id, cp_members_for};` below.

/// Build a **CP split hook** for a group on this node (Phase 2.2 / D3). When the
/// group commits a `Split { at }`, every replica invokes this hook with the handed-off
/// `[at, ∞)` data; the hook spawns [`cp_split_seed`] to stand up this node's member of
/// the new tablet's group — itself carrying a hook (so the child can split again).
fn cp_split_hook(host: CpHostCtx) -> SplitHook {
    Arc::new(move |at, handoff| {
        tracing::info!(
            base_id = host.base_id,
            handoff_len = handoff.len(),
            "cp_split_hook: apply fired the split hook, spawning cp_split_seed"
        );
        tokio::spawn(cp_split_seed(host.clone(), at, handoff));
    })
}

/// Stand up this node's co-resident member of a split-created tablet's CP group
/// (Phase 2.2 / D3), seeded with the handed-off `[at, ∞)` `handoff` data.
///
/// Resolves the new tablet from replicated `Metadata` (the trigger's `SplitTablet`
/// created a tablet whose range starts at `at`), derives the new group's member ids
/// flatly from base ids ([`cp_members_for`] / [`cp_member_id`], identical on every
/// replica + at any depth), mints a `Coresident::sibling` inbox for its own member,
/// opens a per-tablet engine, starts the group **with its own split hook**, registers
/// it for routing, records the durable marker, and publishes its address. **Idempotent
/// per node** (the shared `minted` claim set), so the hook firing on every apply
/// (incl. WAL re-apply on recovery) mints at most once.
#[tracing::instrument(
    name = "cp_split_seed",
    skip(host, at, handoff),
    fields(base_id = host.base_id, handoff_len = handoff.len(), new_tablet = tracing::field::Empty)
)]
async fn cp_split_seed(host: CpHostCtx, at: Vec<u8>, handoff: Vec<(Vec<u8>, Vec<u8>)>) {
    tracing::info!("cp_split_seed: hook fired, resolving new tablet from metadata");
    // The new tablet is the one the trigger's `SplitTablet` created with range
    // starting exactly at the split key. Poll briefly for it to replicate here.
    let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
    let (new_tablet, replicas) = loop {
        let found = host
            .ctx
            .raft
            .metadata()
            .tablets
            .iter()
            .find(|(_, t)| t.range.start == at)
            .map(|(id, t)| (*id, t.replicas.clone()));
        if let Some(found) = found {
            break found;
        }
        if tokio::time::Instant::now() >= deadline {
            tracing::warn!("CP split: new tablet for the split key never appeared");
            return;
        }
        tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
    };
    tracing::Span::current().record("new_tablet", new_tablet.0);
    tracing::info!(
        new_tablet = new_tablet.0,
        ?replicas,
        "cp_split_seed: resolved new tablet"
    );

    // Idempotent **per node**: mint this node's member of the new tablet only once
    // per process. (The per-node `minted` set, not the edge state — in a `--cluster N`
    // process the edge is *shared* across nodes, so gating on `edge.local_cp` would
    // let the first node's mint suppress every other node's, leaving no quorum.)
    {
        let mut minted = host.minted.lock().expect("minted set poisoned");
        if !minted.insert(new_tablet) {
            tracing::info!("cp_split_seed: already minted on this node, skipping");
            return;
        }
    }

    let new_members: Vec<NodeId> = cp_members_for(new_tablet, &replicas).into_iter().collect();
    let my_new_id = cp_member_id(host.base_id, new_tablet);
    tracing::info!(
        my_new_id,
        ?new_members,
        "cp_split_seed: minted claim taken, starting group"
    );

    // A co-resident inbox for this node's member of the new group, drawn from the
    // pre-bound listener pool; its address is published for peer-sync. The new group
    // carries its own split hook so it can be split again (D3).
    let sibling = host.raftkv_env.sibling(my_new_id);
    let sibling_addr = sibling.local_addr();
    let hook = cp_split_hook(host.clone());
    let cp = match host.backend {
        StorageBackend::Lsm => {
            let prefix = format!("db-t{}-", new_tablet.0);
            match LsmEngine::open(sibling.clone(), &prefix).await {
                Ok(lsm) => CpGroup::Lsm(
                    RaftKvNode::start_seeded_with_split_hook(
                        sibling,
                        new_members.clone(),
                        lsm,
                        handoff,
                        hook,
                    )
                    .await,
                ),
                Err(e) => {
                    tracing::error!(?e, "CP split: opening new tablet LSM");
                    return;
                }
            }
        }
        StorageBackend::Memory => CpGroup::Mem(
            RaftKvNode::start_seeded_with_split_hook(
                sibling,
                new_members.clone(),
                MemoryEngine::new(),
                handoff,
                hook,
            )
            .await,
        ),
    };
    host.ctx.edge.register_raftkv(new_tablet, cp);
    tracing::info!("cp_split_seed: raftkv group registered in edge state");

    // Durably record that this node now hosts the new tablet's group on disk, so a
    // restart re-hosts it from its `db-t{id}-` engine (#2). Persist before the address
    // publish so a crash mid-publish still re-hosts on recovery. Snapshot under the
    // lock, then drop the guard before the async persist (never hold a `std::sync::Mutex`
    // guard across `.await`).
    let snapshot = {
        let mut h = host.hosted.lock().expect("hosted set poisoned");
        if !h.iter().any(|e| e.tablet == new_tablet.0) {
            h.push(HostedCpTablet {
                tablet: new_tablet.0,
                member: my_new_id,
                members: new_members,
            });
        }
        h.clone()
    };
    save_hosted_cp(&host.raftkv_env, &snapshot).await;

    // Publish this member's address (relayed to the control leader cross-process, #4)
    // so every node's peer-sync loop can reach it.
    host.ctx
        .register_cp_addr(my_new_id, sibling_addr.to_string())
        .await;
    tracing::info!("cp_split_seed: address published, split-seed complete");
}

/// Re-host a split-created CP tablet from its on-disk engine at node start (#2):
/// mint the tablet's `Coresident::sibling`, recover its `db-t{id}-` engine +
/// `raftkv.wal` via an **empty** seed (the data is already durable on disk — the seed
/// only ever carries a *fresh* split's handoff), start it **with its own split hook**
/// (so a re-hosted split tablet can still be re-split, D3), register it, and re-publish
/// the sibling's new address (the pool port is fresh each incarnation).
async fn cp_rehost(host: CpHostCtx, h: HostedCpTablet) {
    let sibling = host.raftkv_env.sibling(h.member);
    let sibling_addr = sibling.local_addr();
    let tablet = TabletId(h.tablet);
    let hook = cp_split_hook(host.clone());
    let cp = match host.backend {
        StorageBackend::Lsm => {
            let prefix = format!("db-t{}-", h.tablet);
            match LsmEngine::open(sibling.clone(), &prefix).await {
                Ok(lsm) => CpGroup::Lsm(
                    RaftKvNode::start_seeded_with_split_hook(
                        sibling,
                        h.members,
                        lsm,
                        Vec::new(),
                        hook,
                    )
                    .await,
                ),
                Err(e) => {
                    tracing::error!(?e, tablet = h.tablet, "CP re-host: opening tablet LSM");
                    return;
                }
            }
        }
        StorageBackend::Memory => CpGroup::Mem(
            RaftKvNode::start_seeded_with_split_hook(
                sibling,
                h.members,
                MemoryEngine::new(),
                Vec::new(),
                hook,
            )
            .await,
        ),
    };
    host.ctx.edge.register_raftkv(tablet, cp);
    host.ctx
        .register_cp_addr(h.member, sibling_addr.to_string())
        .await;
}

/// How often the CP reconfigure loop pulls the tablet map and steps a group it
/// leads toward its desired voter set (#3). Brisk enough to converge a replica move
/// promptly, but a no-op once a group's config matches the placement, so a steady
/// cluster produces no churn.
const CP_RECONFIGURE_INTERVAL: Duration = Duration::from_millis(500);

// `cp_members_for` (translate a tablet's base-id replica set to its CP group's
// member ids) now lives in `topology` as a pure, unit-tested function — see the
// `use` import above [`cp_member_id`]/[`cp_base_id`].

/// The per-node **CP reconfigure loop** over `ProdEnv` (#3 / ADR 0017 Stage C): on
/// each tick, for every tablet whose CP group this node currently **leads**, pull the
/// tablet's desired replica set from replicated `Metadata` (translated to group
/// member ids) and take one single-server [`reconfigure_step`](CpGroup::reconfigure_step)
/// toward it. The production counterpart of `animus-cp-data`'s `spawn_reconfigure_loop`
/// — the decision is the replicated placement (the reconciler's epoch-CAS), the timing
/// is here. Leader- and convergence-gated, so a steady cluster proposes nothing; a
/// multi-server move converges one server per tick.
///
/// Removing a dead replica needs no new member, so it converges immediately. Adding a
/// *fresh* replica also requires that node to host an (empty) co-resident group for
/// the tablet so it can catch up via `InstallSnapshot` — the join-hosting piece is the
/// remaining v1 increment (this loop drives the membership change either way).
async fn cp_reconfigure_loop(ctx: ClientCtx) {
    loop {
        tokio::time::sleep(CP_RECONFIGURE_INTERVAL).await;
        let tablets = ctx.raft.metadata().tablets;
        for (tablet, t) in tablets {
            let Some(leader) = ctx.edge.cp_leader(tablet) else {
                continue;
            };
            let desired = cp_members_for(tablet, &t.replicas);
            leader.reconfigure_step(&desired);
        }
    }
}

/// How often the join-host loop polls the tablet map for a tablet newly placed on
/// this node. Snappy enough (ADR 0023) that a freshly provisioned table tablet is
/// hosted + elects promptly so the `CreateTable`/first-write that provisioned it can
/// serve, but not so tight that polling (a `Metadata` clone per tick on every node)
/// adds contention under heavy parallel load.
const CP_JOIN_HOST_INTERVAL: Duration = Duration::from_millis(250);

/// The per-node **CP join-host loop** (D1): when the placement reconciler adds this
/// node to a tablet's replica set (e.g. picking it as the spare for a `Down`
/// replica), stand up an **empty** co-resident group for that tablet so the group's
/// leader can add it as a voter and catch it up via `InstallSnapshot`.
///
/// Hosts (ADR 0023): a freshly **provisioned** table tablet (`INITIAL`, whole-ring
/// range — `cp_join_host` forms it with the full voter config so it elects), or a
/// **joined** replica (`epoch > INITIAL` — the reconciler placed this node into an
/// existing group; it starts as a quiet non-voter until the leader adds it). It
/// **skips** a fresh *split* child (`INITIAL` + non-whole range): that is seeded with
/// its handed-off data by the split hook on its original replicas, so starting it
/// empty here would lose data. On a restart `cp_join_host` re-forms from the on-disk
/// engine (full config when it has data). Double-hosting is prevented **per node** by
/// the `minted` claim set (shared with the split hook + re-host), not `edge.local_cp`
/// (which is shared across nodes in an in-process `--cluster N` run).
async fn cp_join_host_loop(host: CpHostCtx) {
    loop {
        tokio::time::sleep(CP_JOIN_HOST_INTERVAL).await;
        let tablets = host.ctx.raft.metadata().tablets;
        for (tablet, t) in tablets {
            // The pure decision — is this tablet this node's concern, and if so
            // forming fresh or joining an existing group — is
            // `topology::plan_join_host` (host a freshly *provisioned* table
            // tablet at `INITIAL` with the whole range, or a *joined* replica at a
            // bumped epoch; skip a fresh split child, `INITIAL` + non-whole range,
            // whose data is seeded by the split hook instead). On a restart
            // `LsmEngine::open` in `cp_join_host` recovers the on-disk engine, so
            // re-hosting a provisioned tablet here does not start it truly empty.
            let Some(plan) = topology::plan_join_host(host.base_id, &t.replicas, t.epoch, &t.range)
            else {
                continue;
            };
            // Dedup **per node** via the `minted` claim set (shared with the split
            // hook + re-host on *this* node) — NOT via `edge.local_cp`, which in an
            // in-process `--cluster N` run is a **shared** `ClusterEdgeState`: it
            // would report *another* node's just-hosted group and make this node skip,
            // leaving a freshly provisioned tablet hosted on only one replica (no
            // majority → no election → "no CP group leader reachable"). The minted set
            // is genuinely per-node, so it dedups correctly in both deployment modes.
            // This stateful claim stays here — it is not part of the pure decision.
            {
                let mut h = host.minted.lock().expect("hosting set poisoned");
                if !h.insert(tablet) {
                    continue;
                }
            }
            cp_join_host(&host, tablet, &t.replicas, plan.initial_formation).await;
        }
    }
}

/// Stand up this node's empty member of `tablet`'s group (the body of
/// [`cp_join_host_loop`]). The bootstrap tablet's member id is this node's base
/// `raftkv` id, hosted on the **main** `raftkv` env; a split tablet's is derived, so
/// it mints a co-resident **sibling**. The group carries its own split hook (D3). On
/// a transient engine-open failure the tablet is un-claimed so a later tick retries.
async fn cp_join_host(
    host: &CpHostCtx,
    tablet: TabletId,
    replicas: &[NodeId],
    initial_formation: bool,
) {
    let member = cp_member_id(host.base_id, tablet);
    let all_members = cp_members_for(tablet, replicas);
    // The **full** member config (every replica, including self) vs the quiet
    // **non-voter** config (the others, excluding self):
    let full: Vec<NodeId> = all_members.iter().copied().collect();
    let others: Vec<NodeId> = full.iter().copied().filter(|&id| id != member).collect();
    // Bootstrap tablet -> the main env (member id == base id); split tablet -> a
    // sibling minted for the derived member id.
    let (env, prefix) = if tablet == TABLET {
        (host.raftkv_env.clone(), LSM_PREFIX.to_string())
    } else {
        (
            host.raftkv_env.sibling(member),
            format!("db-t{}-", tablet.0),
        )
    };
    let addr = env.local_addr();
    let hook = cp_split_hook(host.clone());
    // Choosing the start config (ADR 0023):
    // - **Re-form with the full config** when this node is *forming* the group — a
    //   freshly provisioned table tablet (`initial_formation`, at `INITIAL`), or a
    //   **restart** of a tablet this node already hosts (its on-disk engine has data:
    //   `latest_version() > 0`) — so a replica can campaign and the group elects with
    //   no live leader. WAL recovery alone does **not** restore voter status from a
    //   non-voter start, so the restart case must pass the full config explicitly.
    // - **Join as a quiet non-voter** otherwise — a brand-new (empty) spare the
    //   reconciler placed into an existing, already-led group, which must not campaign
    //   until the leader adds it (`animus-cp-data` membership gotcha).
    let cp = match host.backend {
        StorageBackend::Lsm => match LsmEngine::open(env.clone(), &prefix).await {
            Ok(lsm) => {
                let reforming = initial_formation || lsm.latest_version() > 0;
                let config = if reforming { full } else { others };
                CpGroup::Lsm(RaftKvNode::start_with_split_hook(env, config, lsm, hook))
            }
            Err(e) => {
                tracing::error!(?e, tablet = tablet.0, "CP join-host: opening tablet LSM");
                host.minted
                    .lock()
                    .expect("hosting set poisoned")
                    .remove(&tablet);
                return;
            }
        },
        // Memory backend keeps nothing across a restart, so "has data" never applies;
        // the epoch-based `initial_formation` decides.
        StorageBackend::Memory => {
            let config = if initial_formation { full } else { others };
            CpGroup::Mem(RaftKvNode::start_with_split_hook(
                env,
                config,
                MemoryEngine::new(),
                hook,
            ))
        }
    };
    host.ctx.edge.register_raftkv(tablet, cp);
    host.ctx.register_cp_addr(member, addr.to_string()).await;
}

/// How often the GC loop checks whether a tablet this node hosts has been
/// dropped from the replicated tablet map (ADR 0024). Teardown is off every
/// request path, so a slow-ish tick is fine; brisk enough that a dropped
/// table's disk is reclaimed promptly.
const CP_GC_INTERVAL: Duration = Duration::from_millis(500);
/// How long the GC waits for a halted group's driver to actually exit before
/// giving up for this tick. The driver observes the halt on its next wake (one
/// Raft timer tick at most), so this is generous; on timeout the handle is
/// re-registered and the teardown retries on a later tick — files are never
/// touched while the driver might still write.
const CP_GC_STOP_TIMEOUT: Duration = Duration::from_secs(10);

/// The per-node **CP GC loop** (ADR 0024 drop-table teardown): when a tablet
/// this node hosts disappears from the replicated tablet map (a committed
/// `DropTableTablets`), reclaim everything local to it — unregister the group
/// handle, stop its driver, delete its engine + WAL files, and prune the
/// durable `cp-hosted` marker + `minted` claim. The exact dual of
/// [`cp_join_host_loop`]: same pull-from-replicated-state shape, same per-node
/// `minted` state for the decision (never the shared `--cluster N` edge).
///
/// Deciding "dropped" from *absence* in the map is sound only over recovered,
/// durable metadata: this node minted a tablet only after **applying** its
/// `CreateTablet` (and `metadata()` exposes durable state only), so its
/// recovered control replica always contains every tablet it hosts — absence
/// therefore means a committed drop, never not-yet-recovered state. The
/// `last_applied() == 0` guard skips the pre-recovery window where the default
/// (empty) `Metadata` would read as "everything dropped".
///
/// The converse transient is fine too: while a restarted control replica
/// re-applies its log it passes through **historical** map states, so the
/// join-host loop can briefly re-host a *dropped* tablet (an empty group — its
/// files were already reclaimed, and routing consults the current map, so it
/// serves nothing). Once replay reaches the committed drop, this loop reclaims
/// it again; drop + GC are convergent, not one-shot.
async fn cp_gc_loop(host: CpHostCtx) {
    loop {
        tokio::time::sleep(CP_GC_INTERVAL).await;
        // Recovery guard stays here (a live `RaftNode` read, not part of the pure
        // predicate): skip entirely before replicated `Metadata` has recovered, or
        // an empty default `Metadata` reads as "everything dropped".
        if host.ctx.raft.last_applied() == 0 {
            continue;
        }
        let tablets = host.ctx.raft.metadata().tablets;
        let mine: Vec<TabletId> = host
            .minted
            .lock()
            .expect("hosting set poisoned")
            .iter()
            .copied()
            .collect();
        for tablet in topology::tablets_to_reclaim(&mine, &tablets) {
            cp_gc_tablet(&host, tablet).await;
        }
    }
}

/// Reclaim this node's local artifacts of a dropped `tablet` (the body of
/// [`cp_gc_loop`]). Every step is idempotent and ordered so a crash anywhere
/// mid-teardown converges on a later tick or the next restart: unregister the
/// handle (routing/admin stop seeing the group), stop the driver and wait for
/// its exit (files quiesce), delete the engine + WAL files, prune the durable
/// marker (so a restart no longer re-hosts), then release the `minted` claim.
async fn cp_gc_tablet(host: &CpHostCtx, tablet: TabletId) {
    let member = cp_member_id(host.base_id, tablet);
    // No registered handle means the stand-up path claimed `minted` but has not
    // finished (engine open / start in flight) — retry on a later tick rather
    // than deleting files under a group mid-standup.
    let Some(group) = host.ctx.edge.unregister_raftkv(tablet, member) else {
        return;
    };
    group.shutdown();
    let deadline = tokio::time::Instant::now() + CP_GC_STOP_TIMEOUT;
    while !group.is_stopped() {
        if tokio::time::Instant::now() >= deadline {
            // Never touch files while the driver might still write. Put the
            // handle back so a later tick retries the whole teardown.
            tracing::warn!(
                tablet = tablet.0,
                "CP GC: group driver did not stop in time"
            );
            host.ctx.edge.register_raftkv(tablet, group);
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Delete the group's on-disk artifacts through its own env's disk: the
    // engine's prefix-named LSM files and the group's Raft WAL (plus any
    // `replace` temp leftovers, which share those prefixes). The bootstrap/first
    // tablet lives on the main `raftkv` env under the flat `db-` prefix — its
    // dir holds nothing else db-prefixed, and the `cp-hosted` marker does not
    // match; a split/derived tablet lives on its own sibling env dir.
    let env = group.env().clone();
    let prefix = if tablet == TABLET {
        LSM_PREFIX.to_string()
    } else {
        format!("db-t{}-", tablet.0)
    };
    match env.list().await {
        Ok(files) => {
            for file in files {
                if file.starts_with(&prefix) || file.starts_with(animus_cp_data::WAL) {
                    if let Err(e) = env.remove(&file).await {
                        tracing::warn!(?e, file, "CP GC: removing a dropped tablet's file");
                    }
                }
            }
        }
        Err(e) => tracing::warn!(?e, tablet = tablet.0, "CP GC: listing tablet files"),
    }
    // A derived tablet's sibling env owns its accept loop + any tasks spawned on
    // it — reap them now the group is gone. `shutdown_tasks` leaves the shared
    // sibling listener pool alone (a full `shutdown` would drain it and break
    // future splits). The bootstrap tablet's main env is shared node
    // infrastructure (heartbeats, peer-sync) and stays up.
    if tablet != TABLET {
        env.shutdown_tasks();
    }

    // Prune the durable marker so a restart no longer re-hosts the tablet
    // (split-created tablets only; join-hosted ones are not recorded).
    let pruned = {
        let mut h = host.hosted.lock().expect("hosted set poisoned");
        let before = h.len();
        h.retain(|e| e.tablet != tablet.0);
        (h.len() != before).then(|| h.clone())
    };
    if let Some(snapshot) = pruned {
        save_hosted_cp(&host.raftkv_env, &snapshot).await;
    }
    // Release the claim last, once nothing is left to reclaim. (Tablet ids are
    // never reused, so nothing can legitimately re-mint this id; a re-created
    // table gets a fresh tablet.)
    host.minted
        .lock()
        .expect("hosting set poisoned")
        .remove(&tablet);
    tracing::info!(tablet = tablet.0, "CP GC: reclaimed dropped tablet");
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

/// The leader-driven **automatic split trigger** (Phase 2.4): on each tick, for
/// every tablet whose CP group this node currently **leads**, take the leader's
/// **cheap key-count estimate** ([`CpGroup::approx_key_count`] — memtable count +
/// SSTable bytes, no materialization) and only when it says the tablet might
/// exceed `threshold` (or on a slow per-tablet confirm cadence) materialize the
/// live pairs once — the authoritative count *and*, if over threshold, the
/// **median** split key come from that one snapshot. A split bisects the tablet so
/// each half holds roughly half the keys. Per-tablet cooldown avoids a duplicate
/// trigger while a split is in flight; once it applies, the parent's key count
/// halves below the threshold.
///
/// Only the node hosting a tablet's leader reads `local_pairs`/triggers, so in a
/// one-process-per-node deployment exactly one node triggers. (In a single
/// `--cluster N` process the edge state is shared, so every node's loop sees the
/// same leader handle and may trigger redundantly — the control plane rejects a
/// re-split of an already-split *metadata* range, but that alone does not stop a
/// second `propose_split` from reaching the CP group: metadata rejection only dedups
/// which `SplitTablet` command wins, not who then calls `propose_split` on the data
/// plane, so more than one `Split` command really can land in the committed log.
/// **Genuinely harmless only because `animus-cp-data`'s apply of `KvCommand::Split`
/// is itself idempotent** — a group splits once in its lifetime, and every
/// application after the first is a no-op (`apply_and_compact`'s `already_split`
/// flag). Without that guard a second, redundant `Split` re-fires the split hook
/// with an *empty* handoff (the range was already tombstoned by the first
/// application), and that empty-handoff task can win the per-node mint race against
/// the real one — silently seeding the new tablet's group with no data. That was a
/// real, intermittent bug (`tablet_auto_splits_when_it_grows` flaking on "key not
/// served after auto-split" on unmodified `main`, no manual trigger needed), not a
/// hypothetical one — fixed at the data-plane apply layer, not here, so it is safe
/// regardless of *why* a duplicate `propose_split` happens (this redundant-trigger
/// path today; a stale retry after a data-plane leader failover would hit the same
/// guard).
///
/// `threshold` is a **key count** here — a placeholder size signal; a real
/// byte/size-based threshold is future tuning. Disabled unless a threshold is wired
/// (so it never perturbs clusters that don't opt in).
///
/// **Step 2 of a split (the data-plane `propose_split`) can fail independently of
/// step 1 (the control-plane `SplitTablet` metadata, which by itself already makes
/// the new tablet visible with a real range/replica set — see
/// [`ClientCtx::propose_split_metadata`])** — e.g. the tablet's CP leader moved or
/// was briefly unreachable during bulk-write load. This loop used to call the
/// combined [`ClientCtx::trigger_split`] and discard the result
/// (`let _ = ctx.trigger_split(..).await`), which on a step-2 failure left the new
/// tablet **permanently orphaned**: present in `Metadata.tablets` with a valid
/// range, but with no CP Raft group ever minted on any node — `leader: unknown`
/// forever, and any read/write routed to its range hangs. Worse, since the
/// underlying data never actually moved (the data-plane split never ran), the
/// source tablet kept re-triggering on later ticks and minting *more* orphans from
/// the same unshrunk dataset.
///
/// The `pending` map closes this: a tablet whose step 1 committed but whose step 2
/// hasn't yet succeeded is retried **every tick** with the *same* `split_key`
/// (`propose_split_data` is idempotent, so replaying it is always safe), and is
/// skipped when scanning for *new* candidates below — so a still-unmoved source
/// tablet can't mint a second orphan while the first split is still in flight. The
/// one case a pending entry does *not* keep retrying forever: another proposer's
/// key already won the group's one-time split (a same-tick redundant-median race —
/// see `propose_split_data`'s confirm-by-key doc). That's detected and the entry is
/// dropped rather than retried, since retrying a key that lost is guaranteed to
/// never succeed and would otherwise wrongly exclude the tablet from
/// `is_fresh_split_candidate` forever even after its data already moved.
///
/// Whether `tablet` is a *fresh* auto-split candidate this tick — the pure decision
/// half of the scan loop below, split out so the "don't start a second split while
/// one is pending" invariant is unit-testable without a live cluster (the race it
/// guards against — a tablet's `propose_split` failing right after its
/// `SplitTablet` metadata commits — needs real leader churn under load to occur
/// naturally, which isn't reproducible on demand; mirrors why
/// `topology::decide_cp_route` is factored out the same way).
fn is_fresh_split_candidate(
    tablet: TabletId,
    pending: &BTreeMap<TabletId, Vec<u8>>,
    last_triggered: &BTreeMap<TabletId, tokio::time::Instant>,
    cooldown: Duration,
) -> bool {
    if pending.contains_key(&tablet) {
        return false;
    }
    !matches!(last_triggered.get(&tablet), Some(at) if at.elapsed() < cooldown)
}

async fn auto_split_loop(ctx: ClientCtx, threshold: usize) {
    let mut last_triggered: BTreeMap<TabletId, tokio::time::Instant> = BTreeMap::new();
    // When each tablet last had a *full* (materializing) count — the expensive
    // confirm is rate-limited per tablet, not run every tick.
    let mut last_counted: BTreeMap<TabletId, tokio::time::Instant> = BTreeMap::new();
    // Tablets whose `SplitTablet` metadata committed but whose data-plane
    // `propose_split` hasn't succeeded yet, keyed by the *source* tablet being
    // split (see the function doc above).
    let mut pending: BTreeMap<TabletId, Vec<u8>> = BTreeMap::new();
    loop {
        tokio::time::sleep(AUTO_SPLIT_INTERVAL).await;

        for (tablet, split_key) in std::mem::take(&mut pending) {
            let span = tracing::info_span!("auto_split", tablet = tablet.0, kind = "retry");
            // Patient (2-round) confirm: this loop calls back every tick until the
            // split lands, so a bare confirm-timeout here must not re-propose
            // immediately — see `propose_and_confirm_split`'s doc.
            let response = ctx
                .propose_split_data_with_patience(tablet, split_key.clone(), 2)
                .instrument(span)
                .await;
            if matches!(response, ClientResponse::PutOk) {
                continue;
            }
            // A different proposer's key may have already won this group's
            // one-time split — the same-tick, same-tablet redundant-median race
            // `propose_split_data`'s confirm-by-key check exists to catch (see its
            // doc). Retrying *our* key forever would never succeed once that's
            // happened (the group cannot split twice), and would wrongly keep
            // this tablet excluded from `is_fresh_split_candidate` forever too,
            // even though its data already moved under the winning split. Detect
            // it via any local replica's applied key (set on every replica, not
            // just the leader) and abandon rather than loop forever.
            let abandoned = ctx
                .edge
                .local_cp(tablet)
                .and_then(|g| g.applied_split_key())
                .is_some_and(|applied| applied != split_key);
            if abandoned {
                tracing::info!(
                    tablet = tablet.0,
                    "auto_split: pending split lost a same-tick race to a different key; \
                     abandoning (the tablet's data already moved under the winning split)"
                );
                continue;
            }
            tracing::warn!(
                tablet = tablet.0,
                ?response,
                "auto_split: pending retry failed again, will retry next tick"
            );
            pending.insert(tablet, split_key);
        }

        let tablets: Vec<TabletId> = ctx.raft.metadata().tablets.keys().copied().collect();
        for tablet in tablets {
            if !is_fresh_split_candidate(tablet, &pending, &last_triggered, AUTO_SPLIT_COOLDOWN) {
                continue;
            }
            // Only the leader's host reads + triggers (else this node doesn't have
            // the leader handle).
            let Some(leader) = ctx.edge.cp_leader(tablet) else {
                continue;
            };
            // Cheap per-tick gate: materializing every led tablet's live pairs
            // every tick is O(total data) per 2s — instead, take the free
            // (over-)estimate and only materialize when it says the tablet might
            // exceed the threshold, or on a slow per-tablet confirm cadence
            // (bounded by `AUTO_SPLIT_COOLDOWN`) that corrects estimate error
            // (compression can push real bytes-per-entry below the assumed size;
            // the memory backend has no estimate at all).
            let due_confirm = last_counted
                .get(&tablet)
                .is_none_or(|at| at.elapsed() >= AUTO_SPLIT_COOLDOWN);
            let hot = leader
                .approx_key_count()
                .is_some_and(|estimate| estimate > threshold);
            if !hot && !due_confirm {
                continue;
            }
            // Materialize once: the authoritative count and, if over threshold,
            // the median split key come from the same snapshot.
            let pairs = leader.local_pairs().await;
            last_counted.insert(tablet, tokio::time::Instant::now());
            if pairs.len() <= threshold {
                continue;
            }
            // Median key bisects the tablet; it is strictly inside the range (an
            // interior key of > threshold >= 2 distinct keys), so `SplitTablet`
            // accepts it.
            let median = pairs[pairs.len() / 2].0.clone();
            last_triggered.insert(tablet, tokio::time::Instant::now());
            let span = tracing::info_span!("auto_split", tablet = tablet.0, kind = "fresh");
            async {
                match ctx.propose_split_metadata(tablet, median.clone()).await {
                    Ok(new_id) => {
                        let response = ctx.propose_split_data(tablet, median.clone()).await;
                        if matches!(response, ClientResponse::PutOk) {
                            tracing::info!(
                                tablet = tablet.0,
                                new_id = new_id.0,
                                "auto_split: split accepted"
                            );
                        } else {
                            tracing::warn!(
                                tablet = tablet.0,
                                new_id = new_id.0,
                                ?response,
                                "auto_split: step 2 (propose_split) failed — new_id is now \
                                 committed in metadata but leaderless; queued for retry"
                            );
                            pending.insert(tablet, median);
                        }
                    }
                    Err(response) => {
                        // Step 1 itself didn't commit — nothing was allocated, so
                        // there's no orphan to track; the next tick's `hot` check
                        // will naturally retry from scratch.
                        tracing::warn!(
                            tablet = tablet.0,
                            ?response,
                            "auto_split: step 1 (split metadata) did not commit"
                        );
                    }
                }
            }
            .instrument(span)
            .await;
        }
    }
}

async fn serve_clients(listener: TcpListener, ctx: ClientCtx) {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let ctx = ctx.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_client(stream, ctx).await {
                        tracing::debug!(?err, "client connection closed");
                    }
                });
            }
            Err(err) => {
                tracing::warn!(?err, "client accept failed");
                return;
            }
        }
    }
}

async fn handle_client(mut stream: TcpStream, ctx: ClientCtx) -> std::io::Result<()> {
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
        let response = handle_request(&ctx, request).instrument(span).await;
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
        ClientRequest::Get { .. } => "get",
        ClientRequest::Scan { .. } => "scan",
        ClientRequest::Delete { .. } => "delete",
        ClientRequest::Forwarded { .. } => "forwarded",
        ClientRequest::ProposeSchema(_) => "propose_schema",
        ClientRequest::SplitTablet { .. } => "split_tablet",
        ClientRequest::CpSplit { .. } => "cp_split",
    }
}

async fn handle_request(ctx: &ClientCtx, request: ClientRequest) -> ClientResponse {
    match request {
        ClientRequest::Status => ClientResponse::Status(ctx.raft.metadata()),
        // All data ops route to the leaderful CP per-tablet Raft group (ADR 0017
        // #3a), scoped to the named table (ADR 0023). `table` is a required field
        // on the request type, so there is no unscoped data op to reject here.
        ClientRequest::Put { key, value, table } => ctx.cp_put(&table, key, value).await,
        ClientRequest::PutBatch { entries, table } => {
            match ctx.cp_batch_write(&table, entries).await {
                Ok(()) => ClientResponse::PutOk,
                Err(e) => ClientResponse::Error(e),
            }
        }
        ClientRequest::Get { key, table } => ctx.cp_get(&table, key).await,
        ClientRequest::Scan {
            start,
            end,
            limit,
            table,
        } => match ctx.cp_scan(&table, start, end, limit).await {
            Ok(pairs) => ClientResponse::Pairs(pairs),
            Err(e) => ClientResponse::Error(e),
        },
        ClientRequest::Delete { key, table } => match ctx.cp_delete(&table, key).await {
            Ok(()) => ClientResponse::PutOk,
            Err(e) => ClientResponse::Error(e),
        },
        // Admin: split a CP tablet (Phase 2.2 / D2 cross-process).
        ClientRequest::SplitTablet { tablet, split_key } => {
            ctx.trigger_split(TabletId(tablet), split_key).await
        }
        // The data-plane half of a split, forwarded to the CP leader's node (D2):
        // propose the split locally iff we lead the tablet; never re-forward.
        ClientRequest::CpSplit { tablet, split_key } => {
            ctx.cp_split_here(TabletId(tablet), split_key).await
        }
        // A CP op forwarded from another node (cross-process routing, ADR 0017
        // #3b): serve locally iff we are the leader; never re-forward. The
        // enclosing `client_request` span (in `handle_client`) was already
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
    }
}

/// How long the CQL/DynamoDB edges wait for a proposed schema `MetaCommand`
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
    /// The replicated schema for `table` (the control plane's `ks.table`-keyed
    /// catalog, ADR 0013), read from this node's cached `Metadata`. Every node
    /// applies committed metadata, so a follower sees a table the leader created
    /// once the entry replicates. Returns `None` for an unknown table.
    pub(crate) fn table_schema(&self, table: &str) -> Option<TableSchema> {
        self.raft.metadata().table_schema(table).cloned()
    }

    /// Whether `table` has a replicated schema (cached `Metadata`, ADR 0013).
    pub(crate) fn has_table_schema(&self, table: &str) -> bool {
        self.raft.metadata().has_table_schema(table)
    }

    /// Whether `keyspace` is registered in the replicated catalog (v1 A3) — the
    /// CQL edge's `USE` / qualifier check, replacing per-process edge state.
    pub(crate) fn has_keyspace(&self, keyspace: &str) -> bool {
        self.raft.metadata().has_keyspace(keyspace)
    }

    /// Register this node's CP group member `id` → `addr` in the replicated
    /// `Metadata` (Phase 2.3a), so every node's peer-sync loop can reach it. Routes
    /// to the control leader via the A2 relay (now also accepting `RegisterCpAddr`)
    /// and waits until the entry is visible here, re-proposing each tick. Best-effort
    /// (bounded by [`SCHEMA_COMMIT_TIMEOUT`]); idempotent (re-registering the same
    /// address is a state-machine no-op).
    pub(crate) async fn register_cp_addr(&self, id: NodeId, addr: String) {
        if self.raft.metadata().cp_member_addrs.get(&id) == Some(&addr) {
            return;
        }
        let want = addr.clone();
        let _ = self
            .propose_and_await(
                // `tablet: None` = legacy, never GC'd (ADR 0024). Passing the
                // owning tablet here (so a dropped tablet's addresses are
                // reclaimed) is the animusd wiring of the address GC — a
                // follow-up PR; this only tracks the enum's new field.
                MetaCommand::RegisterCpAddr {
                    id,
                    addr,
                    tablet: None,
                },
                SCHEMA_COMMIT_TIMEOUT,
                || (self.raft.metadata().cp_member_addrs.get(&id) == Some(&want)).then_some(()),
            )
            .await;
    }

    /// Split CP `tablet` at `split_key` (Phase 2.2 / D2): record the split in the
    /// control plane (a new tablet id covering `[split_key, ∞)`), then trigger the
    /// data-plane split on the tablet's CP group leader — on commit each replica's
    /// split hook mints the new tablet's co-resident group. Returns once the
    /// data-plane split is *accepted* (the new group forms + becomes routable
    /// asynchronously; the caller polls a read of an upper-range key to observe it).
    ///
    /// **Cross-process (D2):** both halves are routed to their respective leaders, so
    /// the trigger works from any node — the metadata `SplitTablet` relays to the
    /// control leader (it is [relayable](is_relayable_command)), and the data-plane
    /// `propose_split` routes by **tablet id** to the CP-group leader's node (the two
    /// leaders may differ), forwarding a one-hop [`CpSplit`](ClientRequest::CpSplit)
    /// there if this node doesn't host it.
    ///
    /// The two steps below ([`propose_split_metadata`], [`propose_split_data`]) are
    /// **not atomic** — a caller that cannot tolerate leaving an orphaned,
    /// permanently leaderless tablet behind on a step-2 failure (namely
    /// [`auto_split_loop`]) must drive the two steps itself and retry step 2 with the
    /// *same* `split_key` until it succeeds, rather than calling this combined
    /// one-shot helper. See [`propose_split_metadata`]'s doc for why.
    ///
    /// [`propose_split_metadata`]: Self::propose_split_metadata
    /// [`propose_split_data`]: Self::propose_split_data
    async fn trigger_split(&self, tablet: TabletId, split_key: Vec<u8>) -> ClientResponse {
        match self.propose_split_metadata(tablet, split_key.clone()).await {
            Ok(_new_id) => self.propose_split_data(tablet, split_key).await,
            Err(resp) => resp,
        }
    }

    /// Step 1 of a split: record it in the control plane (a new tablet id covering
    /// `[split_key, ∞)`) and wait until the new tablet is visible here, so the split
    /// hook can resolve `new_id` from `Metadata` once the data-plane `Split` applies.
    /// Routed to the control leader. Returns the newly allocated id on success.
    ///
    /// **This step alone makes the new tablet appear in `Metadata.tablets`** with a
    /// real range and replica set — before its CP Raft group exists anywhere. A
    /// caller that commits this and then never gets [`propose_split_data`] to
    /// succeed leaves that tablet permanently `leader: unknown`: its key range
    /// becomes unroutable, and reads/writes there wait forever for a group that will
    /// never form. **Every caller of this method must eventually retry
    /// [`propose_split_data`] with the same `split_key` until it succeeds — never
    /// silently discard a step-2 failure** (this is exactly the bug `auto_split_loop`'s
    /// `pending` map exists to close: it used to do `let _ = trigger_split(..).await`,
    /// which drops step-2 errors on the floor and orphans the tablet for good).
    ///
    /// [`propose_split_data`]: Self::propose_split_data
    #[tracing::instrument(
        name = "split_metadata",
        skip(self, split_key),
        fields(tablet = tablet.0, new_id = tracing::field::Empty)
    )]
    async fn propose_split_metadata(
        &self,
        tablet: TabletId,
        split_key: Vec<u8>,
    ) -> Result<TabletId, ClientResponse> {
        // The new tablet id comes from the **monotonic allocator**
        // (`next_free_tablet_id`, ADR 0023 — the same allocator provisioning uses),
        // *not* `max(existing ids) + 1`: `DropTableTablets` removes tablets without
        // lowering `next_tablet_id`, so max+1 could re-mint a freed id — and a
        // replica still holding the dropped tablet's `db-t{id}-*` files would
        // re-host them AS the new tablet (data resurrection the absence-keyed GC
        // can never reclaim). The apply also rejects a below-allocator id, so a
        // stale proposer cannot reintroduce reuse.
        let new_id = self.raft.metadata().next_free_tablet_id();
        tracing::Span::current().record("new_id", new_id.0);
        let cmd = MetaCommand::SplitTablet {
            tablet,
            split_key,
            new_id,
        };
        self.propose_and_await(cmd, SCHEMA_COMMIT_TIMEOUT, || {
            self.raft
                .metadata()
                .tablets
                .contains_key(&new_id)
                .then_some(())
        })
        .await
        .map(|()| new_id)
        .map_err(|()| ClientResponse::Error("split metadata did not commit in time".into()))
    }

    /// Step 2 of a split: trigger the data-plane split on `tablet`'s CP group leader
    /// (it fires every replica's split hook on commit). Routed by **tablet id** —
    /// after step 1 commits, the split *key* maps to the new right-hand tablet, so a
    /// key route would target the wrong group. Forwards a one-hop `CpSplit` if the CP
    /// leader is on another node (D2).
    ///
    /// **Safe to retry**: `propose_split` is idempotent per group (a group splits
    /// once in its lifetime; a redundant `Split` entry applies as a no-op), so
    /// replaying this with the same `split_key` after a `NotLeader` / no-route /
    /// relay failure is safe — those failures mean the leader moved or was briefly
    /// unreachable, never that the split itself is invalid.
    #[tracing::instrument(
        name = "split_data",
        skip(self, split_key),
        fields(tablet = tablet.0, route = tracing::field::Empty)
    )]
    async fn propose_split_data(&self, tablet: TabletId, split_key: Vec<u8>) -> ClientResponse {
        self.propose_split_data_with_patience(tablet, split_key, 1)
            .await
    }

    /// Like [`propose_split_data`](Self::propose_split_data), but takes
    /// `confirm_rounds` — see [`propose_and_confirm_split`](Self::propose_and_confirm_split)'s
    /// doc for why a caller that will otherwise retry by proposing again (namely
    /// `auto_split_loop`'s pending-retry) should pass more than 1.
    async fn propose_split_data_with_patience(
        &self,
        tablet: TabletId,
        split_key: Vec<u8>,
        confirm_rounds: u32,
    ) -> ClientResponse {
        let route = self.cp_route_tablet(tablet).await;
        tracing::Span::current().record(
            "route",
            match route {
                CpRoute::Local(_) => "local",
                CpRoute::Forward(_) => "forward",
                CpRoute::None => "none",
            },
        );
        match route {
            CpRoute::Local(leader) => {
                let confirmed =
                    Self::propose_and_confirm_split(&leader, split_key, confirm_rounds).await;
                if !matches!(confirmed, ClientResponse::PutOk) {
                    tracing::warn!(
                        ?confirmed,
                        "accepted propose_split never committed (leader churn?)"
                    );
                }
                confirmed
            }
            // `CpSplit` is already a one-hop "serve-or-error, never re-forward"
            // request, so relay it directly (its own top-level handler) — not via
            // `cp_forward`, which wraps in `Forwarded` (the Put/Get/Delete/Scan path).
            CpRoute::Forward(addr) => {
                let response = self
                    .relay(
                        addr,
                        ClientRequest::CpSplit {
                            tablet: tablet.0,
                            split_key,
                        },
                    )
                    .await;
                if !matches!(response, ClientResponse::PutOk) {
                    tracing::warn!(%addr, ?response, "one-hop CpSplit relay did not succeed");
                }
                response
            }
            CpRoute::None => {
                tracing::warn!(
                    "no CP group leader reachable within cp_route_tablet's timeout budget"
                );
                ClientResponse::Error("no CP group leader for the tablet reachable".into())
            }
        }
    }

    /// Propose `split_key` on `leader` and confirm it, trying up to
    /// `confirm_rounds` confirm windows before giving up. `confirm_rounds > 1` is
    /// for a caller that would otherwise retry by proposing again
    /// (`auto_split_loop`'s pending-retry, `cp_split_here`): `ProposeResult::Accepted`
    /// only means the `Split` entry reached the leader's local log, not that it
    /// committed (see [`confirm_split`](Self::confirm_split)'s doc), so a bare
    /// confirm-timeout does not mean it is lost. Proposing a fresh, fully
    /// redundant `Split` entry on top of one still probably committing wastes a
    /// full WAL append + replication round-trip under exactly the
    /// slow/contended conditions that caused the timeout — safe (a group can
    /// only split once; re-application is a no-op) but wasteful, the same
    /// amplification shape [`ClientCtx::cp_batch_write_patient`] fixes for
    /// bulk-seed batch writes, applied here to the split-propose path. Polling
    /// the same accepted entry again first (instead of re-proposing) is
    /// strictly cheaper and just as correct: either it lands, or a caller with
    /// its own retry loop (`auto_split_loop`'s tick cadence) still gets another
    /// attempt later.
    async fn propose_and_confirm_split(
        leader: &CpGroup,
        split_key: Vec<u8>,
        confirm_rounds: u32,
    ) -> ClientResponse {
        match leader.propose_split(split_key.clone()) {
            ProposeResult::Accepted { .. } => {
                let mut confirmed = Self::confirm_split(leader, &split_key).await;
                for _ in 1..confirm_rounds.max(1) {
                    if matches!(confirmed, ClientResponse::PutOk) {
                        break;
                    }
                    confirmed = Self::confirm_split(leader, &split_key).await;
                }
                confirmed
            }
            ProposeResult::NotLeader { .. } => {
                tracing::warn!("local route was stale: leader stepped down before propose");
                ClientResponse::Error("CP group leader moved; retry the split".into())
            }
        }
    }

    /// Serve the **data-plane half** of a split forwarded from another node (D2): this
    /// node must host `tablet`'s CP-group leader (one hop, no metadata, no re-forward).
    /// Proposes `propose_split` iff it leads, else errors so the client retries with
    /// fresh routing. Confirms the propose actually committed before reporting
    /// success — see [`confirm_split`](Self::confirm_split)'s doc. Always uses the
    /// patient (2-round) confirm via [`propose_and_confirm_split`]: this handler
    /// can't tell whether its caller (a remote node's `auto_split_loop`) is about
    /// to retry on a bare timeout, so it assumes it might and avoids proposing a
    /// redundant duplicate on its own next invocation.
    ///
    /// [`propose_and_confirm_split`]: Self::propose_and_confirm_split
    async fn cp_split_here(&self, tablet: TabletId, split_key: Vec<u8>) -> ClientResponse {
        match self.edge.cp_leader(tablet) {
            Some(leader) => Self::propose_and_confirm_split(&leader, split_key, 2).await,
            None => ClientResponse::Error("forwarded CP split: not the leader here".into()),
        }
    }

    /// Propose `CreateKeyspace` to the control-plane leader and wait for it to
    /// commit + replicate here (v1 A3): a CQL `CREATE KEYSPACE` is durable +
    /// cluster-agreed, surviving restart, instead of living in per-process edge
    /// state. Idempotent (an existing keyspace returns immediately). Routes via the
    /// A2 leader relay; times out after [`SCHEMA_COMMIT_TIMEOUT`].
    pub(crate) async fn create_keyspace(&self, keyspace: String) -> Result<(), String> {
        if self.has_keyspace(&keyspace) {
            return Ok(());
        }
        let ks = keyspace.clone();
        self.propose_and_await(
            MetaCommand::CreateKeyspace {
                keyspace: keyspace.clone(),
            },
            SCHEMA_COMMIT_TIMEOUT,
            || self.has_keyspace(&ks).then_some(()),
        )
        .await
        .map_err(|()| {
            format!(
                "CREATE KEYSPACE `{keyspace}` did not commit within {}s (no control-plane leader reachable?)",
                SCHEMA_COMMIT_TIMEOUT.as_secs()
            )
        })
    }

    /// Propose `MetaCommand::CreateTableSchema` to the control-plane **leader**
    /// and wait for it to commit, so a `CREATE TABLE` is **durable +
    /// cluster-agreed** (ADR 0013) before the client is told it succeeded.
    ///
    /// A proposal must land on the Raft leader. The client may have connected to
    /// a follower, so the proposal is routed to whichever registered control
    /// handle ([`control_handles`]) currently believes it is leader, rather than
    /// blindly to the local node. It re-proposes each poll tick (leadership may
    /// still be settling, or a leader change drops the in-flight entry) until the
    /// table appears in this node's replicated catalog — `CreateTableSchema`
    /// rejects a duplicate, so a racing double-create settles to one schema.
    /// Times out after [`SCHEMA_COMMIT_TIMEOUT`].
    ///
    /// Returns `Ok(())` once the schema is committed and visible here. Returns
    /// `Err` if it did not commit in time (e.g. no leader reachable) or if a
    /// *different* schema is already registered for `table` (a conflicting
    /// `CREATE TABLE`).
    pub(crate) async fn create_table_schema(
        &self,
        table: String,
        schema: TableSchema,
    ) -> Result<(), String> {
        // Already present? Treat an identical schema as success (idempotent
        // re-create); a different one is a conflict the caller should surface.
        if let Some(existing) = self.table_schema(&table) {
            return if existing == schema {
                Ok(())
            } else {
                Err(format!(
                    "table `{table}` already exists with a different schema"
                ))
            };
        }
        let command = MetaCommand::CreateTableSchema {
            table: table.clone(),
            schema: schema.clone(),
        };
        self.propose_and_await(command, SCHEMA_COMMIT_TIMEOUT, || self.table_schema(&table))
            .await
            .map_err(|()| {
                format!(
                    "CREATE TABLE `{table}` did not commit within {}s (no control-plane leader reachable?)",
                    SCHEMA_COMMIT_TIMEOUT.as_secs()
                )
            })
            .and_then(|committed| {
                if committed == schema {
                    Ok(())
                } else {
                    Err(format!(
                        "table `{table}` already exists with a different schema"
                    ))
                }
            })
    }

    /// **Atomically replace** `table`'s schema in the replicated catalog
    /// (`MetaCommand::ReplaceTableSchema`) and wait until the replacement is
    /// visible here — the CQL `ALTER TABLE … ADD` sink. One command, one apply:
    /// unlike the former drop-then-recreate, there is no window in which the
    /// table is schema-less (a crash between the two commands stranded it).
    /// Routes to the leader exactly as
    /// [`create_table_schema`](Self::create_table_schema); idempotent (replacing
    /// with an identical schema is a state-machine no-op that still satisfies the
    /// visibility check). Errors if the table has no schema (the state machine
    /// rejects — an ALTER cannot create a table) or on commit timeout.
    pub(crate) async fn replace_table_schema(
        &self,
        table: String,
        schema: TableSchema,
    ) -> Result<(), String> {
        let command = MetaCommand::ReplaceTableSchema {
            table: table.clone(),
            schema: schema.clone(),
        };
        self.propose_and_await(command, SCHEMA_COMMIT_TIMEOUT, || {
            (self.table_schema(&table).as_ref() == Some(&schema)).then_some(())
        })
        .await
        .map_err(|()| {
            format!(
                "ALTER TABLE `{table}` did not commit within {}s \
                 (no control-plane leader reachable, or the table has no schema?)",
                SCHEMA_COMMIT_TIMEOUT.as_secs()
            )
        })
    }

    /// Drop `table` **and garbage-collect its data** (ADR 0024): remove the
    /// schema from the replicated catalog, then remove the table's tablets from
    /// the replicated tablet map — the trigger each hosting node's
    /// [`cp_gc_loop`] converges on by stopping its local group and deleting its
    /// engine + WAL files. This is the real `DROP TABLE` sink (CQL + the admin
    /// dashboard); [`drop_table_schema`](Self::drop_table_schema) alone remains
    /// the schema-only primitive (the admin panel's schema-only drop) — an
    /// `ALTER TABLE` now mutates the schema in place via
    /// [`replace_table_schema`](Self::replace_table_schema) and never GCs data.
    /// Returns once both the schema and
    /// the tablets have left this node's replicated metadata; the per-node file
    /// reclamation continues asynchronously on every replica.
    pub(crate) async fn drop_table(&self, table: String) -> Result<(), String> {
        self.drop_table_schema(table.clone()).await?;
        let command = MetaCommand::DropTableTablets {
            table: table.clone(),
        };
        // Always propose at least once — never gate on "no tablets in *this*
        // node's metadata": a lagging replica may not have applied the tablet's
        // creation yet, so local absence cannot prove there is nothing to drop
        // (and `propose_and_await` returns on its first poll in that state).
        // The command is idempotent (`NoOp`) on the leader when there truly is
        // nothing. (A *schema'd* table is safe either way — the schema-drop
        // wait above already forced this replica past the tablet's creation in
        // the log — but a plain-client table skips that wait.)
        self.propose_schema(&command).await;
        self.propose_and_await(command, SCHEMA_COMMIT_TIMEOUT, || {
            self.raft
                .metadata()
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
    /// table returns `Ok(())` immediately. Routes to the leader exactly as
    /// [`create_table_schema`](Self::create_table_schema). Schema-only: does
    /// **not** touch the table's tablets/data (the admin panel's schema-only
    /// drop uses this); a real drop goes through [`drop_table`](Self::drop_table)
    /// and an `ALTER TABLE` replaces in place via
    /// [`replace_table_schema`](Self::replace_table_schema).
    pub(crate) async fn drop_table_schema(&self, table: String) -> Result<(), String> {
        if !self.has_table_schema(&table) {
            return Ok(());
        }
        let command = MetaCommand::DropTableSchema {
            table: table.clone(),
        };
        self.propose_and_await(command, SCHEMA_COMMIT_TIMEOUT, || {
            (!self.has_table_schema(&table)).then_some(())
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
    async fn propose_and_await<T>(
        &self,
        command: MetaCommand,
        timeout: Duration,
        committed: impl Fn() -> Option<T>,
    ) -> Result<T, ()> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut next_propose_at = tokio::time::Instant::now();
        loop {
            if let Some(value) = committed() {
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
}

/// Bind an `n`-node cluster on `ip` with ephemeral ports and the conventional
/// ids (control `i`, raftkv `300+i`), each under `dir/node-i`.
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
            control: addr(),
            client: addr(),
            dynamo: addr(),
            cql: addr(),
            raftkv: addr(),
            admin: addr(),
        };
        let node = Node::bind(
            config::control_id(i),
            config::raftkv_id(i),
            addrs,
            dir.join(format!("node-{i}")),
        )
        .await?;
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
    start_cluster_inner(bound, backend, None).await
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
    start_cluster_inner(bound, StorageBackend::default(), Some(threshold)).await
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
    start_cluster_inner(bound, backend, auto_split).await
}

async fn start_cluster_inner(
    bound: Vec<BoundNode>,
    backend: StorageBackend,
    auto_split_threshold: Option<usize>,
) -> std::io::Result<Vec<Node>> {
    let n = bound.len();
    let control_ids: Vec<NodeId> = (0..n).map(config::control_id).collect();
    let peers: BTreeMap<NodeId, SocketAddr> =
        bound.iter().flat_map(BoundNode::peer_entries).collect();
    // One edge-state set shared by every node of *this* cluster (so any node's
    // edge can reach the cluster's leader and they agree on CP/keyspace state),
    // but distinct from any other cluster in the same process.
    let edge = ClusterEdgeState::new();
    // Every node's admin address, so each node's dashboard (ADR 0021) can fan out
    // to the whole in-process cluster.
    let admin_addrs: Vec<SocketAddr> = bound.iter().map(BoundNode::admin_addr).collect();
    let mut nodes = Vec::with_capacity(n);
    for b in bound {
        let node = b
            .start_with(
                peers.clone(),
                control_ids.clone(),
                backend,
                edge.clone(),
                // In-process cluster: the shared edge state reaches every CP group
                // handle in-process, so no cross-process forwarding route is needed.
                BTreeMap::new(),
                auto_split_threshold,
                admin_addrs.clone(),
            )
            .await?;
        nodes.push(node);
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
    let addrs = *config.nodes.get(index).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "node index out of range")
    })?;
    let bound = Node::bind(
        config::control_id(index),
        config::raftkv_id(index),
        addrs,
        dir,
    )
    .await?;
    // One node per process: a fresh per-process edge-state set (it registers only
    // this node's control handle — cross-process proposal forwarding is future
    // work, ADR 0013).
    //
    // Cross-process routing (ADR 0017 #3b): map each node's CP group member id
    // (`raftkv_id`, for CP data ops — A1) **and** its control id (for schema-DDL
    // relay — A2) to that node's **client API** address, so an op landing on a node
    // that isn't the relevant leader forwards to the leader's node.
    let mut client_route: BTreeMap<NodeId, SocketAddr> = BTreeMap::new();
    for (i, addrs) in config.nodes.iter().enumerate() {
        client_route.insert(config::raftkv_id(i), addrs.client);
        client_route.insert(config::control_id(i), addrs.client);
    }
    // Every node's admin address from the shared config, so this node's dashboard
    // (ADR 0021) can fan out to the whole cluster.
    let admin_addrs: Vec<SocketAddr> = config.nodes.iter().map(|n| n.admin).collect();
    bound
        .start_with(
            config.peer_book(),
            config.control_ids(),
            backend,
            ClusterEdgeState::new(),
            client_route,
            None,
            admin_addrs,
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

#[cfg(test)]
mod auto_split_candidate_tests {
    use super::*;

    #[test]
    fn pending_tablet_is_never_a_fresh_candidate_regardless_of_cooldown() {
        let mut pending = BTreeMap::new();
        pending.insert(TabletId(1), b"k".to_vec());
        let last_triggered = BTreeMap::new();
        assert!(!is_fresh_split_candidate(
            TabletId(1),
            &pending,
            &last_triggered,
            Duration::from_secs(0),
        ));
    }

    #[test]
    fn untriggered_tablet_not_in_pending_is_a_fresh_candidate() {
        let pending = BTreeMap::new();
        let last_triggered = BTreeMap::new();
        assert!(is_fresh_split_candidate(
            TabletId(1),
            &pending,
            &last_triggered,
            Duration::from_secs(15),
        ));
    }

    #[test]
    fn recently_triggered_tablet_is_not_a_fresh_candidate_within_cooldown() {
        let pending = BTreeMap::new();
        let mut last_triggered = BTreeMap::new();
        last_triggered.insert(TabletId(1), tokio::time::Instant::now());
        assert!(!is_fresh_split_candidate(
            TabletId(1),
            &pending,
            &last_triggered,
            Duration::from_secs(15),
        ));
    }

    #[test]
    fn tablet_triggered_past_cooldown_is_a_fresh_candidate_again() {
        let pending = BTreeMap::new();
        let mut last_triggered = BTreeMap::new();
        last_triggered.insert(
            TabletId(1),
            tokio::time::Instant::now() - Duration::from_millis(50),
        );
        assert!(is_fresh_split_candidate(
            TabletId(1),
            &pending,
            &last_triggered,
            Duration::from_millis(10),
        ));
    }
}
