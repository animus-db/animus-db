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
pub mod otel;
pub use config::ClusterConfig;
// Re-exported so callers (CLI, tests, operators) can inspect a node's cached
// cluster metadata — membership status and the tablet map — without depending on
// `animus-control` directly.
pub use animus_control::{
    ColumnDef, ColumnType, MetaCommand, Metadata, NodeAddrs, NodeStatus, ReplicationMode,
    TableSchema,
};

mod admin;
mod control_handle;
mod cql;
mod cql_client;
mod dashboard;
mod dynamo;
mod http;
mod topology;

use control_handle::{ControlHandle, RemoteControlClient};

use animus_control::node::heartbeat_loop;
use animus_control::{PlacementPolicy, ProposeResult, RaftNode};
use animus_cp_data::RaftKvNode;
use animus_cp_data::host::{MetadataView, Reconciler};
use animus_env::{Env, Metric, MetricsHandle, NodeId, ProdEnv};
use animus_storage::{LsmEngine, MemoryEngine, SsTableView, WalRecordView};
use animus_tablet::{KeyRange, TabletId, escape};
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
    /// Propose a write to the group (honored on the leader), stamping `fence`
    /// (ADR 0028 write-fence wiring — see [`RaftKvNode::put_fenced`]) so every
    /// replica's apply checks the key against the range embedded in the entry
    /// itself. Every real caller (`ClientCtx::cp_put_local`) stamps the
    /// group's own current [`scope_range`](Self::scope_range) here — there is
    /// no unfenced `put` left in this crate; `KeyRange::whole()` is only ever
    /// used by tests/tools with no split-crossover exposure to guard against.
    fn put_fenced(&self, key: Vec<u8>, value: Vec<u8>, fence: KeyRange) -> ProposeResult {
        match self {
            CpGroup::Lsm(n) => n.put_fenced(key, value, fence),
            CpGroup::Mem(n) => n.put_fenced(key, value, fence),
        }
    }

    /// As [`put_fenced`](Self::put_fenced), but for a **batch put** — commit
    /// every `(key, value)` as one Raft entry. See
    /// [`RaftKvNode::put_batch_fenced`].
    fn put_batch_fenced(&self, puts: Vec<(Vec<u8>, Vec<u8>)>, fence: KeyRange) -> ProposeResult {
        match self {
            CpGroup::Lsm(n) => n.put_batch_fenced(puts, fence),
            CpGroup::Mem(n) => n.put_batch_fenced(puts, fence),
        }
    }

    /// As [`put_fenced`](Self::put_fenced), but for a delete (tombstone). See
    /// [`RaftKvNode::delete_fenced`].
    fn delete_fenced(&self, key: Vec<u8>, fence: KeyRange) -> ProposeResult {
        match self {
            CpGroup::Lsm(n) => n.delete_fenced(key, fence),
            CpGroup::Mem(n) => n.delete_fenced(key, fence),
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

    /// The node's `raftkv` env this group runs on. Since ADR 0026 Stage B every
    /// tablet a node hosts shares this **same** env (stream-addressed, not a
    /// distinct per-tablet id/env) — used to identify *this node's* handle in the
    /// shared edge registry (`node_id()`), not to locate per-tablet files (the
    /// engine is shared too; see [`LSM_PREFIX`]).
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
    /// `key_count`/`byte_size` are this tablet's own **exact,
    /// `StorageScope`-scoped** count/total ([`local_pairs`](Self::local_pairs))
    /// — *not* the cheap, unscoped [`approx_key_count`](Self::approx_key_count)
    /// / scoped-but-approximate [`approx_bytes`](Self::approx_bytes) estimates
    /// `auto_split_loop` uses as a fast gate. `approx_key_count` reads the whole
    /// shared engine and so reports every co-resident tablet's combined count —
    /// a node hosts more than one tablet on the same engine as soon as it hosts
    /// a split's parent + child (ADR 0028), which is why the unscoped estimate
    /// showed a mid-split tablet's row as the *node's* total rather than its own
    /// subset. This is a debug surface, so the materialize-then-count cost is
    /// acceptable (mirrors `local_scan`'s browse-keys view); `byte_size` is
    /// summed from the same materialized pairs, no second engine call needed.
    async fn raft_view(&self, tablet: TabletId) -> admin::CpRaftView {
        // Since ADR 0026 Stage B / ADR 0028 a tablet's CP group member id **is**
        // simply the base `raftkv` id — no more derived-id translation needed.
        let node = self.env().node_id();
        let pairs = self.local_pairs().await;
        let key_count = Some(pairs.len());
        let byte_size = Some(
            pairs
                .iter()
                .map(|(k, v)| (k.len() + v.len()) as u64)
                .sum::<u64>(),
        );
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
                    key_count,
                    byte_size,
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
enum CpRoute {
    /// This node hosts the current leader — serve from `leader` directly.
    Local(CpGroup),
    /// Forward to the leader's node at this client-API address (ADR 0017 #3b).
    Forward(SocketAddr),
    /// No leader reachable (no local leader, no route, election did not settle).
    None,
}

/// How long a CP op (`cp_route` + forward) waits for the tablet's group to be
/// reachable before giving up. Generous because a table's group now forms **in
/// band** on the first access (ADR 0023) — the first op after a `CreateTable`/
/// first-write waits out the join-host + election, which under heavy load takes
/// longer than a steady-state op. No happy-path cost: `cp_route` returns as soon as
/// a leader is reachable; the cap only bounds the wait when the group is forming.
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);
/// How long [`ClientCtx::cp_forward`] backs off between retry passes when every
/// candidate replica refused a forwarded op with `leader_hint=none` — i.e. the
/// tablet's group has no elected leader *yet* (a split-child/first-provision
/// formation window, or a crashed leader mid-election). Roughly one election
/// timeout: long enough that a couple of passes span a real election, short
/// enough that the total wait stays a small fraction of [`CLIENT_TIMEOUT`]
/// (which still hard-bounds the whole sequence).
const FORWARD_ELECTION_BACKOFF: Duration = Duration::from_millis(100);
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
/// role's data dir and does not create intermediate directories.
const LSM_PREFIX: &str = "db-";

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
    /// **Admin: split a CP tablet** at `split_key`. A single, atomic control-plane
    /// command (`MetaCommand::SplitTablet`, epoch-CAS gated): the source tablet's
    /// range narrows and a new sibling tablet is minted covering `[split_key, ∞)`,
    /// both served by this node's *existing* per-node shared engine — no data
    /// moves, and no second, data-plane step is needed (the old two-phase split is
    /// gone; see the root `CLAUDE.md`). The interim manual trigger; an automatic
    /// size-telemetry trigger is `auto_split_loop`.
    SplitTablet { tablet: u64, split_key: Vec<u8> },
    /// **Admin: merge two adjacent CP tablets** (ADR 0033). A single, atomic
    /// control-plane command (`MetaCommand::MergeTablets`, epoch-CAS gated on
    /// both tablets): `left`'s range widens to absorb `right`'s (which is
    /// removed from the tablet map), both served by the *same* replicas'
    /// existing per-node shared engine — no data moves, and there is no
    /// second, data-plane step that can fail independently (the dual of
    /// `SplitTablet` above). The interim manual trigger; an automatic
    /// size-based merge trigger is out of scope for this increment (see ADR
    /// 0033's "Future work").
    MergeTablets { left: u64, right: u64 },
    /// **Join discovery** (ADR 0032 PR2, `animusd join`): a node that knows only
    /// a *seed* address (any already-running node's client address — old or
    /// newly grown, PR1 made every node's address book equally current) asks
    /// for enough information to start as a growth member without an
    /// operator-assembled expanded `ClusterConfig`. Any node can answer — the
    /// reply is built entirely from the receiving node's own knowledge (its
    /// captured `AdminInfo` + its live `client_route`), no forwarding needed.
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
    /// fixed-[`REMOTE_METADATA_SYNC_INTERVAL`] poll a data-only node's mirror
    /// sync used, closing most of the latency gap between "control commits"
    /// and "data node observes it" without a new push mechanism. Only a
    /// genuine control-group replica (`ControlHandle::Local`) serves this —
    /// see [`ClientCtx::watch_metadata`]'s doc for why a `Remote` node
    /// rejects it instead of degrading. Replies with the same
    /// [`ClientResponse::Status`] shape a plain `Status` request gets,
    /// carrying the watermark to pass back as the next call's `last_seen`.
    WatchMetadata { last_seen: u64 },
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
            // Node address book (ADR 0032 PR1): every node self-registers its
            // full address set at startup, from whichever node it happens to
            // connect to for control-plane proposals — must relay like
            // `RegisterCpAddr` (a follower-connected node has no other way to
            // reach the control leader).
            | MetaCommand::RegisterNodeAddrs { .. }
            | MetaCommand::SplitTablet { .. }
            // Tablet merge (ADR 0033): the same relay reason as `SplitTablet` —
            // already client-exposed via `ClientRequest::MergeTablets`, so
            // relaying it adds no new authority, it just lets the trigger
            // reach the control leader cross-process when driven from a
            // follower.
            | MetaCommand::MergeTablets { .. }
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
    )
}

/// A node's reply to a [`ClientRequest`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ClientResponse {
    /// Cached cluster metadata (membership + tablet map), plus (ADR 0035 §1)
    /// the answering node's own best-known control-plane leader —
    /// `self.control.leader()` + `ClientCtx::route_addr(leader_id)`, the same
    /// lookup `propose_schema`'s relay tier already does. Serves three ADR
    /// 0035 PR4 needs from one reply: `ControlHandle::Remote`'s `leader()`/
    /// `leader_addr_hint()`, `metadata_fresh`'s leader-directed retry target,
    /// and an efficient `propose_schema` relay (no extra `route_addr` hop).
    /// `#[serde(default)]` so an older node's reply (predating either field)
    /// still parses, decoding to `None`/`0`.
    ///
    /// **`watermark` (ADR 0035 PR5)**: the answering node's own applied-index
    /// watch (`ControlHandle::metadata_watch().latest()`) at reply time — the
    /// value a caller passes back as the next
    /// [`ClientRequest::WatchMetadata`]'s `last_seen`, and the monotonic
    /// freshness proxy [`control_handle::RemoteControlClient::observe`] uses
    /// to reject a reply from a replica lagging behind one it already saw.
    Status {
        metadata: Metadata,
        #[serde(default)]
        leader_hint: Option<(NodeId, SocketAddr)>,
        #[serde(default)]
        watermark: u64,
    },
    /// A write reached its quorum.
    PutOk,
    /// A read reached its quorum; the value (or `None` if absent).
    Value(Option<Vec<u8>>),
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
        admin_addrs: Vec<SocketAddr>,
    },
}

/// Listen addresses for a node's endpoints (use port 0 for ephemeral): the
/// control + **raftkv** (CP per-tablet Raft) internal `ProdEnv` roles + the client
/// API + the DynamoDB HTTP and CQL endpoints. v1 (ADR 0019) is CP-only — the AP
/// `data`/`coord` roles are gone.
///
/// **ADR 0035** adds [`role`](Self::role): a node declares whether it runs the
/// control role, the data role, or both (`Both`, the default — and, before
/// this ADR, the *only* shape). `control` and `raftkv` are therefore
/// `Option` — `None` when the corresponding role isn't run (a data-only
/// node has no `control` address; a control-only node has no `raftkv`
/// address) — while `client`/`admin` (both roles serve them) and
/// `dynamo`/`cql` (unused by a control-only node today, but already
/// optional-by-default for older configs via `default_ephemeral_addr`, so
/// there was no back-compat reason to change their type here) stay plain
/// `SocketAddr`. See `crate::config::NodeRole` for the role-derived
/// `ClusterConfig` helpers (`control_ids`/`raftkv_ids`/`control_peer_book`/
/// `raftkv_peer_book`) that key off this field. This PR (ADR 0035 PR2) is the
/// config layer only: every entry point still requires both `control` and
/// `raftkv` to be `Some` (i.e. `Both`) — actually assembling a control-only
/// or data-only *process* is PR3/PR4.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct RoleAddrs {
    /// Which role(s) this node runs (ADR 0035). Defaults to
    /// [`Both`](config::NodeRole::Both) when absent — the shape every config
    /// used before this field existed.
    #[serde(default)]
    pub role: config::NodeRole,
    /// The control-plane Raft listen address. `None` for a data-only node
    /// (ADR 0035); always `Some` for `Control`/`Both`. **Defaults to
    /// `Some(ephemeral)`, not `None`, when the key is absent from JSON
    /// entirely** — mirroring `dynamo`/`cql`/`admin`'s existing
    /// "missing-in-an-old-config means ephemeral" contract below, so a
    /// hand-truncated or pre-this-field config (which never declared `role`
    /// either, hence defaults to `Both`) still means combined mode, not a
    /// role/address mismatch. A JSON value of explicit `null` (which only a
    /// role-aware writer, e.g. [`ClusterConfig::generate_split`], ever
    /// produces) still deserializes to `None` — this default only applies
    /// when the key is missing outright.
    #[serde(default = "default_ephemeral_control_addr")]
    pub control: Option<SocketAddr>,
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
    /// address (ADR 0017 #3a) — the data plane. `None` for a control-only
    /// node (ADR 0035); always `Some` for `Data`/`Both`. Same
    /// missing-key-defaults-to-`Some(ephemeral)` back-compat reasoning as
    /// `control` above (this is the field that actually matters for
    /// back-compat: every pre-ADR-0017 config lacks `raftkv` entirely, and
    /// must still mean "combined mode, give it an ephemeral raftkv port").
    #[serde(default = "default_ephemeral_raftkv_addr")]
    pub raftkv: Option<SocketAddr>,
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

/// `RoleAddrs.control`'s missing-key default (ADR 0035): `Some`, not `None`
/// — see that field's doc.
fn default_ephemeral_control_addr() -> Option<SocketAddr> {
    Some(default_ephemeral_addr())
}

/// `RoleAddrs.raftkv`'s missing-key default (ADR 0035): `Some`, not `None`
/// — see that field's doc.
fn default_ephemeral_raftkv_addr() -> Option<SocketAddr> {
    Some(default_ephemeral_addr())
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
    /// This node's control id, if it runs the control role (ADR 0035 PR4).
    /// `None` on a **data-only** node (`ControlHandle::Remote`) — it has no
    /// local control `RaftCore` at all, hence no control id of its own. Never
    /// `None` for a control-only or combined-mode node — see
    /// [`control_addr`](Self::control_addr)'s doc for the paired field.
    pub(crate) control_id: Option<NodeId>,
    /// This node's raftkv id, if it runs the data role (ADR 0035 PR3). `None`
    /// on a control-only node, which never hosts a tablet.
    pub(crate) raftkv_id: Option<NodeId>,
    /// This node's control-plane Raft listen address, if it runs the control
    /// role — see [`control_id`](Self::control_id)'s doc; `None` on a
    /// data-only node for the identical reason.
    pub(crate) control_addr: Option<SocketAddr>,
    /// `None` on a control-only node (no `raftkv` env bound) — see
    /// [`raftkv_id`](Self::raftkv_id)'s doc.
    pub(crate) raftkv_addr: Option<SocketAddr>,
    pub(crate) client_addr: SocketAddr,
    /// `None` on a control-only node (the DynamoDB listener is never bound
    /// there, ADR 0035 PR3).
    pub(crate) dynamo_addr: Option<SocketAddr>,
    /// `None` on a control-only node (the CQL listener is never bound there,
    /// ADR 0035 PR3).
    pub(crate) cql_addr: Option<SocketAddr>,
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

/// The common assembly tail shared by every node shape (ADR 0035 PR3):
/// build the [`ClientCtx`] and spawn the tasks every node needs regardless of
/// role — control-only ([`BoundControlNode::start_control_with`]), or
/// combined/data-role ([`BoundNode::start_with`]): `route_sync_loop`,
/// `metrics_sample_loop`, this node's own one-shot `register_node_addrs`
/// self-registration, the plain client-request server, and the admin HTTP
/// endpoint (ADR 0020). Returns the built `ClientCtx` — so the caller can
/// spawn whatever role-specific tasks it still needs (`bootstrap`/
/// `peer_sync_loop`/the growth-node mirror/`heartbeat_loop`/the tablet-host
/// reconciler/`auto_split_loop`/the dynamo+cql listeners for a data-capable
/// node; nothing more for a control-only one) — plus the join handles
/// spawned here, which the caller folds into its own task list so
/// [`Node::shutdown`] aborts all of it.
///
/// `self_addrs` is `(id, addrs)` for this node's own `register_node_addrs`
/// self-registration: a combined/data-role node registers under its
/// **raftkv** id with a real `raftkv` address (the cluster's members are the
/// raftkv ids); a control-only node has neither, so it registers under its
/// **control** id with an empty `raftkv` field (parsed and skipped by every
/// consumer that overlays `node_addrs[*].raftkv`, e.g. `peer_sync_loop` —
/// "a peer entry whose address fails to parse is skipped").
#[allow(clippy::too_many_arguments)] // node assembly: control handle + edge + role + admin + routing
fn spawn_common_tail(
    control: ControlHandle,
    edge: ClusterEdgeState,
    data: Option<DataRole>,
    admin_info: Arc<AdminInfo>,
    client_route: BTreeMap<NodeId, SocketAddr>,
    self_addrs: (NodeId, NodeAddrs),
    client_listener: TcpListener,
    admin_listener: TcpListener,
) -> (ClientCtx, Vec<tokio::task::JoinHandle<()>>) {
    // The seed `route_sync_loop` (below) re-overlays `Metadata.node_addrs[*].client`
    // onto every tick (ADR 0032 PR1) — the same static-base pattern
    // `peer_sync_loop` uses for the raftkv-env peer book.
    let static_route = client_route.clone();
    let ctx = ClientCtx {
        control,
        edge,
        data,
        client_route: Arc::new(Mutex::new(client_route)),
        admin: admin_info,
        metrics_history: Arc::new(Mutex::new(VecDeque::with_capacity(METRICS_HISTORY_CAP))),
        remote_metadata: Arc::new(Mutex::new(None)),
    };

    let mut tasks = Vec::with_capacity(5);
    // Route-sync loop (ADR 0032 PR1): keep `ctx.client_route` = the static seed
    // above ∪ `Metadata.node_addrs[*].client`, so a node grown in after this
    // node's own startup still becomes a valid client-op forward target and
    // `propose_schema`'s relay/broadcast can reach it too. Runs on every node,
    // including a growth node (reads `effective_metadata()`, so it syncs off
    // its own remote mirror) and a control-only node.
    tasks.push(tokio::spawn(route_sync_loop(ctx.clone(), static_route)));
    // Metrics-history sampler (ADR 0020 dashboard sparklines): periodic
    // snapshots of this node's own aggregated counters. Runs on every node —
    // a control-only node's snapshot is just the control sink (`metrics_text`/
    // `metrics_json` skip the raftkv sink when `ctx.data` is `None`).
    tasks.push(tokio::spawn(metrics_sample_loop(ctx.clone())));
    // This node's own address-book self-registration (ADR 0032 PR1), one-shot:
    // so peer-sync (raftkv addresses) and any node's route/peers views
    // (client/admin addresses) can resolve it regardless of when this node
    // joined relative to the reader.
    {
        let ctx = ctx.clone();
        let (node, addrs) = self_addrs;
        tasks.push(tokio::spawn(async move {
            ctx.register_node_addrs(node, addrs).await;
        }));
    }
    tasks.push(tokio::spawn(serve_clients(client_listener, ctx.clone())));
    // The admin / debug HTTP-JSON endpoint on its own port (ADR 0020).
    tasks.push(tokio::spawn(admin::serve(admin_listener, ctx.clone())));

    (ctx, tasks)
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
    /// Combined-mode-only convenience: derives the `data_raftkv_ids`
    /// [`start_with`](Self::start_with) now takes explicitly by assuming
    /// every id in `control_ids` is also a data-role node's control id (true
    /// for every caller of this simpler entry point — nothing calls it with
    /// a split-role `control_ids`).
    ///
    /// # Errors
    /// Propagates a failure to open the CP group's on-disk engine.
    pub async fn start(
        self,
        peers: BTreeMap<NodeId, SocketAddr>,
        control_ids: Vec<NodeId>,
    ) -> std::io::Result<Node> {
        let admin_addr = self.admin_addr;
        let data_raftkv_ids = control_ids
            .iter()
            .map(|&id| config::raftkv_id(id as usize))
            .collect();
        self.start_with(
            peers,
            control_ids,
            data_raftkv_ids,
            StorageBackend::default(),
            ClusterEdgeState::new(),
            BTreeMap::new(),
            None,
            None,
            vec![admin_addr],
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
    /// `data_raftkv_ids` (ADR 0035 PR2) is the set of `raftkv` ids
    /// [`bootstrap`] auto-registers as `Active` data members — i.e. the ids
    /// of nodes that actually run the **data** role. Before ADR 0035 this was
    /// computed unconditionally as `(0..control_ids.len()).map(raftkv_id)`,
    /// silently assuming every control-group index also ran the data role;
    /// callers now compute it explicitly (in combined mode, still every
    /// control id's paired `raftkv_id` — see [`ClusterConfig::raftkv_ids`] —
    /// so combined-mode behavior is byte-for-byte unchanged). A growth/join
    /// caller passes the **pre-growth** set here too, mirroring
    /// `control_ids`: bootstrap must never auto-register a growth node itself
    /// (it self-registers `Down` via `admin_add_member` instead, promoted to
    /// `Active` by its own heartbeat — see `run_node_growth`'s doc).
    ///
    /// # Errors
    /// Propagates a failure to open the CP group's on-disk engine (LSM backend
    /// only).
    #[allow(clippy::too_many_arguments)] // node assembly: ids + backend + edge + route + split opts
    pub async fn start_with(
        self,
        peers: BTreeMap<NodeId, SocketAddr>,
        control_ids: Vec<NodeId>,
        data_raftkv_ids: Vec<NodeId>,
        backend: StorageBackend,
        edge: ClusterEdgeState,
        client_route: BTreeMap<NodeId, SocketAddr>,
        auto_split_threshold: Option<usize>,
        auto_split_bytes_threshold: Option<u64>,
        cluster_admin_addrs: Vec<SocketAddr>,
    ) -> std::io::Result<Node> {
        self.control_env.set_peers(peers.clone());
        self.raftkv_env.set_peers(peers.clone());
        // The initial (static) peer book + a `raftkv`-env clone, kept for the
        // **peer-sync loop** (Phase 2.3a): it rebuilds the raftkv family's peer book
        // as `static ∪ Metadata.cp_member_addrs` so a runtime-joined CP member
        // becomes reachable.
        let static_peers = peers;
        let raftkv_sync_env = self.raftkv_env.clone();
        // A `raftkv`-env clone for the per-node **tablet-host reconciler** (ADR
        // 0031 PR4): every tablet's group this node stands up runs on it,
        // stream-addressed by tablet id (ADR 0026 Stage B) rather than a
        // distinct per-tablet env/id.
        let raftkv_hook_env = self.raftkv_env.clone();
        // A `raftkv`-env clone for the **failure-detection heartbeat loop** (#3): each
        // node heartbeats the control group *as its `raftkv` member id* (the cluster
        // members are the `raftkv` ids), so the control plane's `detect_loop` marks a
        // crashed CP node `Down`.
        let raftkv_hb_env = self.raftkv_env.clone();
        let my_raftkv_id = self.raftkv_id;
        let my_raftkv_addr = self.raftkv_addr;
        // Captured here (all `SocketAddr`, `Copy`) for the node-address-book
        // self-registration below (ADR 0032 PR1) — `self.client_listener`/
        // `self.admin_listener` (not `Copy`) are moved into their `serve` tasks
        // further down, but the addresses themselves are needed there too.
        let my_client_addr = self.client_addr;
        let my_admin_addr = self.admin_addr;

        // The node's identity + bound addresses for the admin `/admin/config`
        // view (ADR 0020), captured before the envs are consumed below.
        let admin_info = Arc::new(AdminInfo {
            control_id: Some(self.control_id),
            raftkv_id: Some(self.raftkv_id),
            control_addr: Some(self.control_addr),
            raftkv_addr: Some(self.raftkv_addr),
            client_addr: self.client_addr,
            dynamo_addr: Some(self.dynamo_addr),
            cql_addr: Some(self.cql_addr),
            admin_addr: self.admin_addr,
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

        // Keep clones of the two internal envs so [`Node::shutdown`] can abort
        // every task they own (the two Raft drivers + accept loops), freeing their
        // listener ports for a restart.
        let envs = vec![self.control_env.clone(), self.raftkv_env.clone()];

        // Capture the raftkv-role metrics sink before its env is consumed below.
        // The control-plane sink is reached at request time via `raft.metrics()`
        // (`RaftNode::start` records into `control_env.metrics()`); the CP group
        // records into its own role env's sink. The `/metrics` endpoint aggregates
        // both (ADR 0015).
        let raftkv_metrics = self.raftkv_env.metrics();

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
            StorageBackend::Lsm => match LsmEngine::open(self.raftkv_env.clone(), LSM_PREFIX).await
            {
                Ok(lsm) => SharedEngine::Lsm(lsm),
                Err(e) => {
                    return Err(std::io::Error::other(format!(
                        "opening the node's shared CP storage engine: {e}"
                    )));
                }
            },
            StorageBackend::Memory => SharedEngine::Mem(MemoryEngine::new()),
        };

        let raft = RaftNode::start(self.control_env, control_ids.clone());
        // Register this node's control handle in this **node's own**
        // `ClusterEdgeState` (ADR 0013/ADR 0031 PR2 — edge state is always
        // per-node, in `--cluster N` exactly as in one-process-per-node), so
        // `propose_schema` can propose locally when this node happens to be the
        // control leader. When it isn't, `propose_schema` relays
        // `ClientRequest::ProposeSchema` one hop to the leader's node via
        // `client_route` — the same relay path a follower-connected DDL always
        // used in one-process-per-node mode (`tests/schema_ddl_relay.rs`); a
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
        // `serve_clients`/`admin::serve` — every task a control-only node needs
        // too (see [`BoundControlNode::start_control_with`]); the tasks spawned
        // below this point are combined-mode/data-role-only.
        let data_role = DataRole {
            rmw_lock: Arc::new(tokio::sync::Mutex::new(())),
            raftkv_metrics,
            base_id: my_raftkv_id,
        };
        let (ctx, mut tasks) = spawn_common_tail(
            ControlHandle::Local(raft.clone()),
            edge.clone(),
            Some(data_role),
            admin_info,
            client_route,
            (
                my_raftkv_id,
                NodeAddrs {
                    raftkv: my_raftkv_addr.to_string(),
                    client: my_client_addr.to_string(),
                    admin: my_admin_addr.to_string(),
                    role: "combined".to_string(),
                },
            ),
            self.client_listener,
            self.admin_listener,
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
        let reconciler = {
            let host_edge = edge.clone();
            let teardown_edge = edge.clone();
            let base_id = my_raftkv_id;
            let on_teardown = move |tablet: TabletId| {
                teardown_edge.unregister_raftkv(tablet, base_id);
            };
            match storage {
                SharedEngine::Lsm(lsm) => CpReconciler::Lsm(Reconciler::new(
                    raftkv_hook_env,
                    lsm,
                    my_raftkv_id,
                    table_scope_prefix,
                    move |tablet, node: &RaftKvNode<ProdEnv, LsmEngine<ProdEnv>>| {
                        host_edge.register_raftkv(tablet, CpGroup::Lsm(node.clone()));
                    },
                    on_teardown,
                )),
                SharedEngine::Mem(mem) => CpReconciler::Mem(Reconciler::new(
                    raftkv_hook_env,
                    mem,
                    my_raftkv_id,
                    table_scope_prefix,
                    move |tablet, node: &RaftKvNode<ProdEnv, MemoryEngine>| {
                        host_edge.register_raftkv(tablet, CpGroup::Mem(node.clone()));
                    },
                    on_teardown,
                )),
            }
        };

        // Bootstrap: whichever node is leader registers membership (no data tablet)
        // (idempotent). `spawn_common_tail` (above) already started `tasks` with
        // the tail every node shape shares (`route_sync_loop`/
        // `metrics_sample_loop`/this node's own `register_node_addrs`
        // self-registration/`serve_clients`/`admin::serve`) — everything below is
        // combined-mode/data-role-only, tracked in the same task list so
        // `shutdown` aborts all of it and releases the client/dynamo/cql
        // listener ports (these run on plain `tokio::spawn`, off the `Env`
        // network).
        // ADR 0035 PR2: `data_raftkv_ids` is caller-supplied (see `start_with`'s
        // doc) — no longer derived here from `control_ids.len()`, so a caller
        // that scopes it to only the data-role nodes (or, for growth/join, the
        // pre-growth set) is respected exactly.
        tasks.push(tokio::spawn(bootstrap(raft.clone(), data_raftkv_ids)));

        // Peer-sync loop (Phase 2.3a): keep the raftkv family's peer book =
        // `static ∪ Metadata.cp_member_addrs`, so a runtime-registered CP member
        // (split sibling / joined node) becomes reachable for the group's internal
        // Raft traffic. Runs on every node (harmless where no CP group is hosted).
        tasks.push(tokio::spawn(peer_sync_loop(
            ctx.clone(),
            raftkv_sync_env,
            static_peers.clone(),
        )));

        // **Control-plane-follower-less growth node mirror** (ADR 0030): this
        // node's own control role is a genuine voter of `control_ids` iff its own
        // control id is *in* that set — the common case for every node started
        // the normal way (`start`/`run_node_with`/`start_cluster_*`, which always
        // pass a `control_ids` that includes `self.control_id`). A node started
        // via `run_node_growth` deliberately passes the **pre-growth** control
        // group instead (it "needs no control-voter slot" — see that fn's doc),
        // so its own `RaftCore` permanently sits outside `control_ids`: it can
        // never become a voter, campaign, or receive real AppendEntries from the
        // real leader (whose own peer set is derived from *its* config, which
        // never learned of this node — the control group stays static, ADR
        // 0030's documented v1 limitation). Such a node instead mirrors real
        // cluster state by polling `ClientRequest::Status` from one of the
        // pre-growth control nodes' client addresses (derived from
        // `client_route`, which growth's expanded config populates for every
        // node it lists) into `ctx.remote_metadata`, read via
        // `effective_metadata()`. A no-op (empty seed list, loop returns
        // immediately) for every other node.
        if !control_ids.contains(&self.control_id) {
            let seeds: Vec<SocketAddr> = control_ids
                .iter()
                .filter_map(|id| ctx.route_addr(*id))
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
                let node = my_raftkv_id;
                tasks.push(tokio::spawn(async move {
                    let _ = ctx.admin_add_member(node, BTreeMap::new()).await;
                }));
            }
        }

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

        // Auto-split loop (Phase 2.4 / ADR 0034), opt-in: a node splits a tablet
        // it leads once it exceeds **either** configured threshold (it checks
        // leadership per tablet, so running it on every node is harmless).
        if auto_split_threshold.is_some() || auto_split_bytes_threshold.is_some() {
            tasks.push(tokio::spawn(auto_split_loop(
                ctx.clone(),
                AutoSplitThresholds {
                    keys: auto_split_threshold,
                    bytes: auto_split_bytes_threshold,
                },
            )));
        }
        // The DynamoDB JSON/HTTP and CQL endpoints — data-role-only, unlike the
        // plain client server + admin endpoint (already spawned by
        // `spawn_common_tail`, which every node shape runs).
        tasks.push(tokio::spawn(dynamo::serve(
            self.dynamo_listener,
            ctx.clone(),
        )));
        tasks.push(tokio::spawn(cql::serve(self.cql_listener, ctx.clone())));

        Ok(Node {
            raft: ControlHandle::Local(raft),
            envs,
            tasks,
            edge: ctx.edge.clone(),
            client_addr: self.client_addr,
            dynamo_addr: Some(self.dynamo_addr),
            cql_addr: Some(self.cql_addr),
            admin_addr: self.admin_addr,
        })
    }
}

/// A running node. Holds the handles that keep its envs and tasks alive.
///
/// **ADR 0035 PR3**: this one type now backs both a combined-mode/data-role
/// node (two internal `ProdEnv` roles, both listeners bound) and a
/// control-only node (one internal role, no `raftkv`/dynamo/cql listeners at
/// all) — see [`BoundControlNode::start_control_with`]. `envs` is therefore a
/// `Vec` (1 or 2 entries) rather than a fixed-size array, and `dynamo_addr`/
/// `cql_addr` are `Option` internally; the public accessors below still
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
    /// The client-facing listener tasks (client TCP / dynamo HTTP / cql), which
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
    /// `None` on a control-only node (ADR 0035 PR3) — the CQL listener is
    /// never bound there. See [`cql_addr`](Self::cql_addr)'s doc.
    cql_addr: Option<SocketAddr>,
    admin_addr: SocketAddr,
}

impl Node {
    /// Bind this node's listeners (the control + raftkv internal envs + the client
    /// TCP server + the DynamoDB HTTP and CQL endpoints) and create its data
    /// directory.
    ///
    /// This PR (ADR 0035 PR2) is the config layer only: `Node::bind` still
    /// unconditionally requires both a control and a raftkv address (i.e.
    /// `addrs.role` must be [`Both`](config::NodeRole::Both), today's only
    /// real shape) — a control-only or data-only `RoleAddrs` fails cleanly
    /// here rather than binding a wrong/missing listener. Dedicated
    /// control-only/data-only bind paths that only bind what each role needs
    /// are PR3/PR4.
    ///
    /// # Errors
    /// Returns `InvalidInput` if `addrs` is missing the `control` or `raftkv`
    /// address this combined-mode bind requires, or propagates any bind /
    /// directory-creation failure.
    pub async fn bind(
        control_id: NodeId,
        raftkv_id: NodeId,
        addrs: RoleAddrs,
        data_dir: impl Into<PathBuf>,
    ) -> std::io::Result<BoundNode> {
        let dir = data_dir.into();
        let control_listen = addrs.control.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Node::bind requires a control address (RoleAddrs.control is None — \
                 a control-only/data-only split-deployment node isn't bindable through \
                 this combined-mode path yet, ADR 0035 PR3/PR4)",
            )
        })?;
        let (control_env, control_addr) =
            ProdEnv::bind(control_id, control_listen, dir.join("control")).await?;
        // The leaderful CP per-tablet Raft role's internal env (ADR 0017 #3a) — the
        // v1 data plane; distinct id/port/dir from the control role (single-consumer
        // inbox). Since ADR 0026 Stage B every tablet this node hosts shares this
        // **one** env, addressed by `stream` (the tablet id) — no more per-tablet
        // sibling inbox to pre-bind (`Coresident`/`CP_SIBLING_POOL` are gone).
        let raftkv_listen = addrs.raftkv.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Node::bind requires a raftkv address (RoleAddrs.raftkv is None — \
                 see the control-address error above)",
            )
        })?;
        let (raftkv_env, raftkv_addr) =
            ProdEnv::bind(raftkv_id, raftkv_listen, dir.join("raftkv")).await?;
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

    /// Bind a **control-only** node's listeners (ADR 0035 PR3): the control
    /// internal `ProdEnv` role plus the client + admin TCP listeners only —
    /// no `raftkv` env, no dynamo/cql listeners. `addrs.control` must be
    /// `Some` (a data-only `RoleAddrs` fails cleanly here); `addrs.raftkv` is
    /// ignored (a mixed-topology config's data-role entries carry one, but a
    /// control-role entry's own `raftkv` field — `None`, since
    /// `ClusterConfig::generate_split` sets it that way — is never consulted).
    ///
    /// # Errors
    /// Returns `InvalidInput` if `addrs.control` is `None`, or propagates any
    /// bind / directory-creation failure.
    pub async fn bind_control(
        control_id: NodeId,
        addrs: RoleAddrs,
        data_dir: impl Into<PathBuf>,
    ) -> std::io::Result<BoundControlNode> {
        let dir = data_dir.into();
        let control_listen = addrs.control.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Node::bind_control requires a control address (RoleAddrs.control is None)",
            )
        })?;
        let (control_env, control_addr) =
            ProdEnv::bind(control_id, control_listen, dir.join("control")).await?;
        let client_listener = TcpListener::bind(addrs.client).await?;
        let client_addr = client_listener.local_addr()?;
        let admin_listener = TcpListener::bind(addrs.admin).await?;
        let admin_addr = admin_listener.local_addr()?;
        Ok(BoundControlNode {
            control_id,
            control_env,
            control_addr,
            client_listener,
            client_addr,
            admin_listener,
            admin_addr,
        })
    }

    /// Bind a **data-only** node's listeners (ADR 0035 PR4): the `raftkv`
    /// internal `ProdEnv` role plus the client/dynamo/cql/admin TCP
    /// listeners — no control env at all (a data-only node holds no local
    /// control `RaftCore`, `Node::bind_control`'s exact dual). `addrs.raftkv`
    /// must be `Some` (a control-only `RoleAddrs` fails cleanly here);
    /// `addrs.control` is ignored (a mixed-topology config's control-role
    /// entries carry one, but a data-role entry's own `control` field —
    /// `None`, since `ClusterConfig::generate_split` sets it that way — is
    /// never consulted).
    ///
    /// # Errors
    /// Returns `InvalidInput` if `addrs.raftkv` is `None`, or propagates any
    /// bind / directory-creation failure.
    pub async fn bind_data(
        raftkv_id: NodeId,
        addrs: RoleAddrs,
        data_dir: impl Into<PathBuf>,
    ) -> std::io::Result<BoundDataNode> {
        let dir = data_dir.into();
        let raftkv_listen = addrs.raftkv.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Node::bind_data requires a raftkv address (RoleAddrs.raftkv is None)",
            )
        })?;
        let (raftkv_env, raftkv_addr) =
            ProdEnv::bind(raftkv_id, raftkv_listen, dir.join("raftkv")).await?;
        let client_listener = TcpListener::bind(addrs.client).await?;
        let client_addr = client_listener.local_addr()?;
        let dynamo_listener = TcpListener::bind(addrs.dynamo).await?;
        let dynamo_addr = dynamo_listener.local_addr()?;
        let cql_listener = TcpListener::bind(addrs.cql).await?;
        let cql_addr = cql_listener.local_addr()?;
        let admin_listener = TcpListener::bind(addrs.admin).await?;
        let admin_addr = admin_listener.local_addr()?;
        Ok(BoundDataNode {
            raftkv_id,
            raftkv_env,
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
    ///
    /// # Panics
    /// If this node has no data role (ADR 0035 PR3 control-only node) — the
    /// listener is never bound there. Every real caller (the CLI printouts,
    /// the test suite) only ever holds a combined-mode/data-role `Node`.
    pub fn dynamo_addr(&self) -> SocketAddr {
        self.dynamo_addr
            .expect("dynamo_addr: this node has no data role (ADR 0035 PR3 control-only)")
    }

    /// The address the CQL binary-protocol endpoint listens on.
    ///
    /// # Panics
    /// If this node has no data role — see [`dynamo_addr`](Self::dynamo_addr)'s doc.
    pub fn cql_addr(&self) -> SocketAddr {
        self.cql_addr
            .expect("cql_addr: this node has no data role (ADR 0035 PR3 control-only)")
    }

    /// The address the admin / debug HTTP endpoint listens on (ADR 0020).
    pub fn admin_addr(&self) -> SocketAddr {
        self.admin_addr
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

    /// Propose a control-plane [`MetaCommand`] on this node's control replica,
    /// returning whether it was accepted (i.e. this node is the leader). The
    /// interim admin hook for cluster metadata operations the wire edges do not
    /// yet expose — notably marking a table **CP** (ADR 0017 #3a) via
    /// `MetaCommand::SetTableMode`. A non-leader proposal is dropped (`false`); the
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
    /// dynamo / cql on a data-role node) and every task its internal `ProdEnv`
    /// role(s) own (the control Raft driver, plus the CP Raft driver on a
    /// data-role node, and the internal accept loops). This releases every
    /// listener port so a replacement node can rebind the same addresses on
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
    pub async fn shutdown_graceful(&self) {
        // A data-only node (ADR 0035 PR4) has no local control WAL to flush —
        // `RaftNode::flush` only exists on a genuine local Raft replica.
        if let ControlHandle::Local(raft) = &self.raft {
            raft.flush().await;
        }
        self.edge.shutdown_all_cp_groups().await;
        self.shutdown();
    }
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
/// yet started — the control-only counterpart of [`BoundNode`]. Binds only
/// the control internal `ProdEnv` role plus the client + admin TCP
/// listeners; no `raftkv` env, no dynamo/cql listeners, no CP storage engine
/// (a control node never hosts a tablet or speaks a data-plane wire
/// protocol). See [`Node::bind_control`] to construct one and
/// [`start_control_with`](Self::start_control_with) to start it.
pub struct BoundControlNode {
    control_id: NodeId,
    control_env: ProdEnv,
    control_addr: SocketAddr,
    client_listener: TcpListener,
    client_addr: SocketAddr,
    admin_listener: TcpListener,
    admin_addr: SocketAddr,
}

impl BoundControlNode {
    /// The address clients connect to.
    pub fn client_addr(&self) -> SocketAddr {
        self.client_addr
    }

    /// The control-plane Raft listen address.
    pub fn control_addr(&self) -> SocketAddr {
        self.control_addr
    }

    /// The address the admin / debug HTTP endpoint listens on (ADR 0020).
    pub fn admin_addr(&self) -> SocketAddr {
        self.admin_addr
    }

    /// `(control_id, addr)` — this node's entry in the cluster's *control*
    /// peer book (the `BoundNode::peer_entries` dual, minus the `raftkv`
    /// entry a control-only node has none of).
    pub fn peer_entry(&self) -> (NodeId, SocketAddr) {
        (self.control_id, self.control_addr)
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
    /// (nothing to host, no engine to sample), or the dynamo/cql listeners
    /// (never bound here). Every client-request dispatch path this node *can*
    /// reach (`Status`/`ProposeSchema`/`JoinInfo`/`SplitTablet`/`MergeTablets`,
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
    pub async fn start_control_with(
        self,
        peers: BTreeMap<NodeId, SocketAddr>,
        control_ids: Vec<NodeId>,
        client_route: BTreeMap<NodeId, SocketAddr>,
        cluster_admin_addrs: Vec<SocketAddr>,
    ) -> Node {
        self.control_env.set_peers(peers.clone());
        let envs = vec![self.control_env.clone()];

        let admin_info = Arc::new(AdminInfo {
            control_id: Some(self.control_id),
            raftkv_id: None,
            control_addr: Some(self.control_addr),
            raftkv_addr: None,
            client_addr: self.client_addr,
            dynamo_addr: None,
            cql_addr: None,
            admin_addr: self.admin_addr,
            control_ids: control_ids.clone(),
            peers,
            admin_addrs: if cluster_admin_addrs.is_empty() {
                vec![self.admin_addr]
            } else {
                cluster_admin_addrs
            },
            auto_split_threshold: None,
            auto_split_bytes_threshold: None,
        });

        let raft = RaftNode::start(self.control_env, control_ids.clone());
        // A fresh, node-local edge state (ADR 0031 PR2 doctrine — every node
        // gets its own, never shared); it stays permanently empty of CP group
        // handles (`raftkv`) since this node hosts none, but `register_control`
        // still lets `propose_schema` (and the client dispatch paths above)
        // propose locally when this node is the control leader.
        let edge = ClusterEdgeState::new();
        edge.register_control(raft.clone());

        let (ctx, tasks) = spawn_common_tail(
            ControlHandle::Local(raft.clone()),
            edge,
            None,
            admin_info,
            client_route,
            (
                self.control_id,
                NodeAddrs {
                    raftkv: String::new(),
                    client: self.client_addr.to_string(),
                    admin: self.admin_addr.to_string(),
                    role: "control".to_string(),
                },
            ),
            self.client_listener,
            self.admin_listener,
        );

        Node {
            raft: ControlHandle::Local(raft),
            envs,
            tasks,
            edge: ctx.edge.clone(),
            client_addr: self.client_addr,
            dynamo_addr: None,
            cql_addr: None,
            admin_addr: self.admin_addr,
        }
    }
}

/// A **data-only** node (ADR 0035 PR4) whose listeners are bound but not yet
/// started — the data-only counterpart of [`BoundNode`] (which is
/// [`BoundControlNode`]'s own dual). Binds only the `raftkv` internal
/// `ProdEnv` role plus the client/dynamo/cql/admin TCP listeners; no control
/// env, no local control `RaftCore`, no bootstrap. See [`Node::bind_data`] to
/// construct one and [`start_data_with`](Self::start_data_with) to start it.
pub struct BoundDataNode {
    raftkv_id: NodeId,
    raftkv_env: ProdEnv,
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

impl BoundDataNode {
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

    /// `(raftkv_id, addr)` — this node's entry in the cluster's *raftkv* peer
    /// book (the [`BoundNode::peer_entries`] dual, minus the `control` entry
    /// a data-only node has none of).
    pub fn peer_entry(&self) -> (NodeId, SocketAddr) {
        (self.raftkv_id, self.raftkv_addr)
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
    /// dynamo/cql listeners) minus everything control-plane-specific
    /// (`bootstrap`, `edge.register_control`) — see that method's doc for
    /// what each shared piece does. `spawn_common_tail` still runs
    /// unconditionally (`route_sync_loop`/`metrics_sample_loop`/this node's
    /// own `register_node_addrs` self-registration/`serve_clients`/
    /// `admin::serve`), and this node's own `admin_add_member` self-registers
    /// its membership exactly like an ADR 0030 growth node's does (relayed —
    /// a data-only node can never satisfy `propose_schema`'s local-leader
    /// branch itself).
    ///
    /// # Errors
    /// Propagates a failure to open the CP group's on-disk engine (LSM
    /// backend only).
    #[allow(clippy::too_many_arguments)] // node assembly: mirrors `BoundNode::start_with`'s arity
    pub async fn start_data_with(
        self,
        peers: BTreeMap<NodeId, SocketAddr>,
        control_ids: Vec<NodeId>,
        control_seeds: Vec<SocketAddr>,
        backend: StorageBackend,
        edge: ClusterEdgeState,
        client_route: BTreeMap<NodeId, SocketAddr>,
        auto_split_threshold: Option<usize>,
        auto_split_bytes_threshold: Option<u64>,
        cluster_admin_addrs: Vec<SocketAddr>,
    ) -> std::io::Result<Node> {
        self.raftkv_env.set_peers(peers.clone());
        let static_peers = peers;
        let raftkv_sync_env = self.raftkv_env.clone();
        let raftkv_hook_env = self.raftkv_env.clone();
        let raftkv_hb_env = self.raftkv_env.clone();
        let my_raftkv_id = self.raftkv_id;
        let my_raftkv_addr = self.raftkv_addr;
        let my_client_addr = self.client_addr;
        let my_admin_addr = self.admin_addr;

        let control = ControlHandle::Remote(RemoteControlClient::new(control_seeds.clone()));

        let admin_info = Arc::new(AdminInfo {
            control_id: None,
            raftkv_id: Some(self.raftkv_id),
            control_addr: None,
            raftkv_addr: Some(self.raftkv_addr),
            client_addr: self.client_addr,
            dynamo_addr: Some(self.dynamo_addr),
            cql_addr: Some(self.cql_addr),
            admin_addr: self.admin_addr,
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

        let envs = vec![self.raftkv_env.clone()];
        let raftkv_metrics = self.raftkv_env.metrics();

        // Same shared-engine assembly as `BoundNode::start_with` — see that
        // method's doc.
        let storage = match backend {
            StorageBackend::Lsm => match LsmEngine::open(self.raftkv_env.clone(), LSM_PREFIX).await
            {
                Ok(lsm) => SharedEngine::Lsm(lsm),
                Err(e) => {
                    return Err(std::io::Error::other(format!(
                        "opening the node's shared CP storage engine: {e}"
                    )));
                }
            },
            StorageBackend::Memory => SharedEngine::Mem(MemoryEngine::new()),
        };

        let data_role = DataRole {
            rmw_lock: Arc::new(tokio::sync::Mutex::new(())),
            raftkv_metrics,
            base_id: my_raftkv_id,
        };
        let (ctx, mut tasks) = spawn_common_tail(
            control,
            edge.clone(),
            Some(data_role),
            admin_info,
            client_route,
            (
                my_raftkv_id,
                NodeAddrs {
                    raftkv: my_raftkv_addr.to_string(),
                    client: my_client_addr.to_string(),
                    admin: my_admin_addr.to_string(),
                    role: "data".to_string(),
                },
            ),
            self.client_listener,
            self.admin_listener,
        );

        // The per-node tablet-host reconciler (ADR 0031 PR4) — identical
        // shape to `BoundNode::start_with`'s.
        let reconciler = {
            let host_edge = edge.clone();
            let teardown_edge = edge.clone();
            let base_id = my_raftkv_id;
            let on_teardown = move |tablet: TabletId| {
                teardown_edge.unregister_raftkv(tablet, base_id);
            };
            match storage {
                SharedEngine::Lsm(lsm) => CpReconciler::Lsm(Reconciler::new(
                    raftkv_hook_env,
                    lsm,
                    my_raftkv_id,
                    table_scope_prefix,
                    move |tablet, node: &RaftKvNode<ProdEnv, LsmEngine<ProdEnv>>| {
                        host_edge.register_raftkv(tablet, CpGroup::Lsm(node.clone()));
                    },
                    on_teardown,
                )),
                SharedEngine::Mem(mem) => CpReconciler::Mem(Reconciler::new(
                    raftkv_hook_env,
                    mem,
                    my_raftkv_id,
                    table_scope_prefix,
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
            raftkv_sync_env,
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
            let node = my_raftkv_id;
            tasks.push(tokio::spawn(async move {
                let _ = ctx.admin_add_member(node, BTreeMap::new()).await;
            }));
        }

        tasks.push(tokio::spawn(heartbeat_loop(raftkv_hb_env, control_ids)));

        tasks.push(tokio::spawn(tablet_host_reconciler_loop(
            ctx.clone(),
            reconciler,
        )));

        if auto_split_threshold.is_some() || auto_split_bytes_threshold.is_some() {
            tasks.push(tokio::spawn(auto_split_loop(
                ctx.clone(),
                AutoSplitThresholds {
                    keys: auto_split_threshold,
                    bytes: auto_split_bytes_threshold,
                },
            )));
        }
        tasks.push(tokio::spawn(dynamo::serve(
            self.dynamo_listener,
            ctx.clone(),
        )));
        tasks.push(tokio::spawn(cql::serve(self.cql_listener, ctx.clone())));

        Ok(Node {
            raft: ctx.control.clone(),
            envs,
            tasks,
            edge: ctx.edge.clone(),
            client_addr: self.client_addr,
            dynamo_addr: Some(self.dynamo_addr),
            cql_addr: Some(self.cql_addr),
            admin_addr: self.admin_addr,
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
    /// `client_route` (ADR 0013) — the same path every follower-connected DDL
    /// in a one-process-per-node deployment always used.
    control: Arc<Mutex<Vec<RaftNode<ProdEnv>>>>,
    /// The DynamoDB edge's in-memory GSI declarations + observation-built
    /// written-key index (ADR 0006). Not durable / not replicated; per-node.
    dynamo_registry: Arc<Mutex<animus_dynamo::SchemaRegistry>>,
    /// The CQL edge's keyspaces + prepared-statement store (ADR 0013). Not
    /// durable / not replicated; per-node — a statement `PREPARE`d on one node
    /// is only `EXECUTE`-able on connections to *that* node (matching a real
    /// one-process-per-node deployment's per-process catalog).
    cql_state: Arc<tokio::sync::Mutex<cql::CqlState>>,
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

    /// Gracefully halt every CP group registered here (process shutdown, not
    /// drop-table GC — see [`shutdown_graceful`](Node::shutdown_graceful)). A raw
    /// `ProdEnv::shutdown()` hard-`abort()`s the CP-data driver/apply tasks, which
    /// can land mid-`storage.merge(..).await` inside `apply_and_compact` and
    /// surface as a `tokio::fs` background-task panic
    /// (`Backend("background task failed")`/`Backend("task was cancelled")`) when
    /// the runtime's blocking pool is torn down underneath it. `CpGroup::shutdown`
    /// only latches a flag the driver observes *between* full apply passes, so we
    /// must poll [`is_stopped`](CpGroup::is_stopped) before the caller proceeds to
    /// abort anything else — the same shutdown-then-wait shape the per-node
    /// tablet-host reconciler's own teardown uses (ADR 0031 PR4) before deleting a
    /// dropped tablet's files. Snapshots the handles out of the
    /// lock first (never hold a `std::sync::Mutex` guard across `.await`). Bounded
    /// by `CP_GC_STOP_TIMEOUT`; a group that doesn't stop in time is logged and
    /// left for the subsequent hard abort (the process is exiting either way).
    async fn shutdown_all_cp_groups(&self) {
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

    /// The CQL edge's per-node state.
    pub(crate) fn cql_state(&self) -> &Arc<tokio::sync::Mutex<cql::CqlState>> {
        &self.cql_state
    }
}

/// This node's data-plane fields (ADR 0035 PR3) — present in [`ClientCtx`]
/// iff this node runs the data role (`NodeRole::Data`/`Both`); `None` on a
/// control-only node, which never hosts a tablet and never runs the CP/
/// DynamoDB/CQL machinery these back. Grouping them under one `Option`
/// (rather than three loose `Option` fields on `ClientCtx`) means "does this
/// node have a data role" is answered once, at the type level, instead of
/// re-derived from whether several unrelated fields all happen to be `Some`.
#[derive(Clone)]
struct DataRole {
    /// Serializes a node's read-modify-writes so a CQL/DynamoDB RMW (linearizable
    /// CP read → CP write) is atomic *per node*. Cross-node atomicity (a CAS on the
    /// CP group) is later v1 work. Accessed only from the dynamo/cql wire edges,
    /// whose listeners are never bound on a control-only node.
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
}

/// Shared context for the client request server and the DynamoDB/CQL endpoints:
/// the control-plane handle (for cached metadata + schema proposals — a
/// [`ControlHandle`], ADR 0035 PR1), this node's own wire-edge state (incl. the
/// CP group handles it hosts), the cross-node CP routing table, and — iff this
/// node runs the data role (ADR 0035 PR3) — its [`DataRole`] fields.
#[derive(Clone)]
pub(crate) struct ClientCtx {
    control: ControlHandle,
    pub(crate) edge: ClusterEdgeState,
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
}

impl ClientCtx {
    /// This node's [`DataRole`] fields — see that type's doc.
    ///
    /// # Panics
    /// If this node has no data role (ADR 0035 PR3 control-only). Every call
    /// site must be reachable only from a path that structurally cannot run
    /// on a control-only node: the dynamo/cql wire edges (their listeners are
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
    /// loop, the split/merge triggers' precondition reads, and (ADR 0035 PR1)
    /// the general-purpose schema-catalog reads (`table_schema`/
    /// `has_table_schema`, and — since the PR5 staleness audit closed the gap
    /// PR1 flagged — `has_keyspace` too) the CQL/DynamoDB wire edges use for
    /// everything except their own commit-wait polls (see
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
    /// Used by the schema commit-wait polls (`create_table_schema`/
    /// `replace_table_schema`/`drop_table_schema`/`trigger_split`/
    /// `trigger_merge` below) and the DynamoDB conditional-write existence
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

    /// Serve a long-poll [`ClientRequest::WatchMetadata`] (ADR 0035 PR5):
    /// park on this node's own [`ControlHandle::metadata_watch`] for up to
    /// [`WATCH_METADATA_SERVER_TIMEOUT`], then reply with whatever `Metadata`
    /// is current — either because it genuinely advanced past `last_seen`,
    /// or because the bound elapsed with nothing new (a normal outcome, not
    /// an error; the caller just retries with the same `last_seen`, exactly
    /// like a `Status` poll that happened not to observe a change).
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
    pub(crate) async fn watch_metadata(&self, last_seen: u64) -> ClientResponse {
        if matches!(self.control, ControlHandle::Remote(_)) {
            return ClientResponse::Error(
                "this node has no local control-plane watch to serve (ADR 0035 data-only node); \
                 watch a control-plane node instead"
                    .into(),
            );
        }
        let watch = self.control.metadata_watch();
        tokio::select! {
            _ = watch.changed(last_seen) => {}
            () = tokio::time::sleep(WATCH_METADATA_SERVER_TIMEOUT) => {}
        }
        ClientResponse::Status {
            metadata: self.effective_metadata(),
            leader_hint: self.control_leader_hint(),
            watermark: watch.latest(),
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
        let addr = self.route_addr(id)?;
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
    /// resolution policy behind [`cp_route`](Self::cp_route) (key→tablet→leader).
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
            let route = self.route_snapshot();
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

    /// Whether a CP read error is a **transient routing/leadership/scope race**
    /// the reader should retry with re-resolved routing (the `"; retry"` shape
    /// every such error in this file carries), as opposed to a genuine failure
    /// to surface. Shared by [`cp_read`](Self::cp_read)/[`cp_scan_one`]'s
    /// internal retry loops.
    fn read_should_retry(e: &str) -> bool {
        e.ends_with("; retry")
    }

    /// Serve a linearizable **get** on a known-leader local handle, enforcing
    /// the **read-side scope pre-check** (ADR 0033 — the read dual of
    /// [`cp_put_local`](Self::cp_put_local)'s pre-propose range check) and the
    /// served/absent disambiguation. Shared by [`cp_read`](Self::cp_read)'s
    /// `Local` arm and `cp_serve_forwarded`'s `Get` arm, so both make the
    /// identical decision.
    ///
    /// `Ok(None)` is a genuinely **served** absent. `Err("…; retry")` covers
    /// the two conditions that must never be reported as absence: (1) the
    /// group's live `scope_range()` does not contain `key` — this routing
    /// resolution raced a split's narrow or a merge's widen, so this group
    /// does not (or does not *yet*) own the key, and serving from its engine
    /// could return absent-or-stale for data another group (or a
    /// not-yet-drained absorbed sibling) is authoritative for; (2) the
    /// ReadIndex barrier failed (deposed / mid-election leader) — nothing can
    /// be concluded about the key at all. Both were previously collapsed into
    /// "absent" (`Value(None)`), which read exactly like data loss from the
    /// outside — the ADR 0033 regression `tests/tablet_merge.rs` caught.
    async fn cp_get_local(leader: &CpGroup, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        if !leader.scope_range().contains(key) {
            return Err(format!(
                "key {key:?} outside tablet's current range (stale routing, likely a split/merge crossover); retry"
            ));
        }
        match leader.linearizable_get_served(key).await {
            Some(v) => Ok(v),
            None => Err("CP group leader moved; retry".into()),
        }
    }

    /// Serve a linearizable **scan** on a known-leader local handle, enforcing
    /// the read-side scope pre-check — the scan flavor of
    /// [`cp_get_local`](Self::cp_get_local): `linearizable_scan` filters every
    /// row through the group's live scope (`strip_in_range`), so a scope that
    /// has not yet caught up to the metadata-derived request window (a merge's
    /// widen in flight) would **silently truncate** the results rather than
    /// error. Shared by [`cp_scan_one`] and `cp_serve_forwarded`'s `Scan` arm.
    async fn cp_scan_local(
        leader: &CpGroup,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let requested = KeyRange::new(start.to_vec(), end.map(<[u8]>::to_vec));
        if !leader.scope_range().contains_range(&requested) {
            return Err(format!(
                "scan window {requested:?} outside tablet's current range (stale routing, likely a split/merge crossover); retry"
            ));
        }
        match leader.linearizable_scan(start, end, limit).await {
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
    /// split's narrow or a merge's widen — metadata says the group owns the
    /// key, its scope hasn't caught up) is likewise retried until routing and
    /// scope agree, mirroring the write side's pre-propose range check. `Err`
    /// is "no leader reachable / did not become serveable in time". The CP
    /// read primitive the wire edges call directly.
    pub(crate) async fn cp_read(
        &self,
        table: &str,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, String> {
        let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
        loop {
            let err = match self.cp_route(table, &key).await {
                CpRoute::Local(leader) => match Self::cp_get_local(&leader, &key).await {
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
            CpRoute::Forward(addr) => {
                let request = ClientRequest::Put {
                    key: key.clone(),
                    value,
                    table: table.to_owned(),
                };
                Self::ok_or_err(
                    self.cp_forward(table, &key, addr, request).await,
                    "forwarded CP write",
                )
            }
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
        // Read through `effective_metadata()` so an ADR 0030 growth node (and PR4's
        // control-less data node) consults the mirror, not an empty local core.
        if !self.effective_metadata().has_table_tablet(table) {
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
            CpRoute::Forward(addr) => {
                let request = ClientRequest::PutBatch {
                    entries: group,
                    table: table.to_owned(),
                };
                Self::ok_or_err(
                    self.cp_forward(table, &first, addr, request).await,
                    "forwarded CP batch write",
                )
            }
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
    ) -> Result<Option<KvPair>, String> {
        let probe = group.last().cloned();
        let fence = leader.scope_range();
        if let Some((bad_key, _)) = group.iter().find(|(k, _)| !fence.contains(k)) {
            return Err(format!(
                "key {bad_key:?} outside tablet's current range (stale routing, likely a split crossover); retry"
            ));
        }
        match leader.put_batch_fenced(group, fence) {
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
        // `effective_metadata()`, not the raw handle: see `cp_batch_write`.
        if !self.effective_metadata().has_table_tablet(table) {
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
                    let request = ClientRequest::PutBatch {
                        entries: group.clone(),
                        table: table.to_owned(),
                    };
                    match self.cp_forward(table, &first, addr, request).await {
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
            CpRoute::Forward(addr) => {
                let request = ClientRequest::Delete {
                    key: key.clone(),
                    table: table.to_owned(),
                };
                Self::ok_or_err(
                    self.cp_forward(table, &key, addr, request).await,
                    "forwarded CP delete",
                )
            }
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
        let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
        loop {
            let err = match self.cp_route(table, &start).await {
                CpRoute::Local(leader) => {
                    match Self::cp_scan_local(&leader, &start, end.as_deref(), limit).await {
                        Ok(p) => return Ok(p),
                        Err(e) => e,
                    }
                }
                CpRoute::Forward(addr) => {
                    let request = ClientRequest::Scan {
                        start: start.clone(),
                        end: end.clone(),
                        limit,
                        table: table.to_owned(),
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
    /// loop happens to poll value equality, which degrades that hazard to "waits
    /// out `CLIENT_TIMEOUT` and returns an error" rather than a false ack — but
    /// that is a property of *this* poll, not a defense to rely on, so the
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
        let fence = leader.scope_range();
        if !fence.contains(&key) {
            return Err(
                "key outside tablet's current range (stale routing, likely a split crossover); retry".into(),
            );
        }
        match leader.put_fenced(key.clone(), value.clone(), fence) {
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
        let fence = leader.scope_range();
        if !fence.contains(&key) {
            return Err(
                "key outside tablet's current range (stale routing, likely a split crossover); retry".into(),
            );
        }
        match leader.delete_fenced(key.clone(), fence) {
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
        // `effective_metadata` (not `self.control.metadata_cached()` directly): on a growth
        // node (ADR 0030) the local raft never reflects a table created before it
        // existed, which would otherwise misread every write as needing a brand
        // new (duplicate, rejected) tablet.
        if !self.effective_metadata().has_table_tablet(table) {
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
        // `effective_metadata`: see `cp_put`'s identical comment (ADR 0030).
        if !self.effective_metadata().has_table_tablet(table) {
            return ClientResponse::Value(None);
        }
        match self.cp_read(table, key).await {
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
        // base `raftkv` id, so the local replica's leader hint is already a
        // `client_route` key — no more base<->member translation needed.
        let leader = self.edge.local_cp(tablet).and_then(|n| n.leader())?;
        let addr = self.route_addr(leader)?;
        Some((leader, addr))
    }

    /// The client-API address to forward a `tablet` op to — see
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
        let route = self.route_snapshot();
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
        // Prefer the control handle's own leader-address hint (ADR 0035 PR4:
        // populated directly from `Status` replies for a `Remote` data node,
        // see `ControlHandle::leader_addr_hint`'s doc) over a `route_addr`
        // lookup — the hint is strictly fresher for a data-only node, since
        // it rides the very `Status` reply that filled the mirror, whereas
        // `route_addr` needs this leader's address to have separately synced
        // into the replicated node-address book. A no-op for `Local` (always
        // `None`), so this changes nothing for any node shape that existed
        // before this PR.
        if let Some(addr) = self.control.leader_addr_hint() {
            return !matches!(
                self.relay(addr, ClientRequest::ProposeSchema(command.clone()))
                    .await,
                ClientResponse::Error(_)
            );
        }
        if let Some(leader_id) = self.control.leader() {
            if let Some(addr) = self.route_addr(leader_id) {
                return !matches!(
                    self.relay(addr, ClientRequest::ProposeSchema(command.clone()))
                        .await,
                    ClientResponse::Error(_)
                );
            }
        }
        // No locally-known leader. The common cause is a real control-group
        // voter mid-election (rare, brief); the other is a **control-plane-
        // follower-less growth node** (ADR 0030) whose own control `RaftCore`
        // never learns a leader at all, since it never receives real Raft
        // traffic for a group it was never a voter of — for it, this is the
        // *only* path that can ever reach the real cluster (its own local
        // `propose` always fails, and it has no leader hint to relay a single
        // hop to). Broadcast to every other known client-API address instead:
        // a real control-group member among them resolves the actual leader
        // itself (one more hop — `ProposeSchema`'s handler is a single,
        // bounded relay, never a chain). Returns true on the first address that
        // connects, regardless of what its own `propose_schema` achieves
        // (best-effort, same as every other branch here — the caller confirms
        // via replicated `Metadata`, not this return value).
        for addr in self.route_snapshot().into_values() {
            if addr == self.admin.client_addr {
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
        loop {
            // Fresh, not `metadata_cached()` (ADR 0035 PR4): the "no tablet
            // yet" branch below picks the tablet's **initial, permanent**
            // replica set from `meta.members` — `CreateTablet` only ever
            // succeeds once per table (idempotent, first-committer wins), so
            // a stale mirror read here isn't a transient staleness a later
            // retry heals, it silently and *permanently* under-replicates
            // the tablet (no periodic re-check ever grows a recorded RF
            // policy after the fact). This mattered only theoretically for a
            // `Local` handle (control replication lag is sub-millisecond),
            // but a `Remote` data node's mirror is *routinely* a poll
            // interval stale (ADR 0035 §5) — caught live by
            // `tests/data_only.rs` flaking on exactly this race (a freshly
            // `Active`-promoted second data member not yet visible to a
            // still-catching-up mirror at the moment the first write
            // provisioned the table).
            let meta = self.control.metadata_fresh().await;
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
            ClientRequest::Get { key, table } => {
                let tablet = self.tablet_for(&table, &key);
                match tablet.and_then(|t| self.edge.cp_leader(t)) {
                    // Read-side scope pre-check + served/absent disambiguation
                    // (ADR 0033) — the same `cp_get_local` decision as `cp_read`'s
                    // Local arm. Serve-or-error only (never re-forward, never
                    // wait): the forwarder's own retry loop re-resolves routing on
                    // a `"; retry"` error.
                    Some(leader) => match Self::cp_get_local(&leader, &key).await {
                        Ok(v) => ClientResponse::Value(v),
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
            ClientRequest::Scan {
                start,
                end,
                limit,
                table,
            } => {
                let tablet = self.tablet_for(&table, &start);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                // Read-side scope pre-check (ADR 0033) — the same
                // `cp_scan_local` decision as `cp_scan_one`'s Local arm: a
                // scope lagging the metadata-derived scan window would
                // silently truncate results, not error.
                match Self::cp_scan_local(&leader, &start, end.as_deref(), limit).await {
                    Ok(p) => ClientResponse::Pairs(p),
                    Err(e) => ClientResponse::Error(e),
                }
            }
            _ => ClientResponse::Error("unexpected forwarded request".into()),
        }
    }

    /// Render this node's **live** metrics as the ADR 0015 text export
    /// (`name value` lines), aggregated across the node's role sink(s).
    ///
    /// A combined-mode/data-role node runs two internal `ProdEnv` roles on
    /// distinct ids — control (Raft) and raftkv (the CP group) — and each
    /// records into its **own** sink (`RaftNode::start` records into the
    /// control env's sink; the CP group into the raftkv env's). A
    /// control-only node (ADR 0035 PR3) has only the control sink — there is
    /// no raftkv env to aggregate. This sums whichever sink(s) exist
    /// counter-by-counter and takes the max of the leadership gauge
    /// (leadership is the control plane's, recorded only in the control
    /// sink). The snapshots are read **at call time**, so the export reflects
    /// current activity rather than a cached value.
    pub(crate) fn metrics_text(&self) -> String {
        let mut snaps = vec![self.control.metrics().snapshot()];
        if let Some(data) = &self.data {
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
        if let Some(data) = &self.data {
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
                node,
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
    /// - `node`'s paired **control** id (`node - RAFTKV_ID_BASE`, guarded
    ///   against underflow for a `node` below the base) is one of this
    ///   cluster's original control-plane ids: an original control-core
    ///   member must never be decommissioned this way. The control Raft group
    ///   is static (ADR 0030) — this call only ever prunes `Metadata.members`,
    ///   it cannot remove a real control-group voter — and `bootstrap`
    ///   (idempotent, `BoundNode::start_with`) re-registers every control-core
    ///   raftkv id `Active` on its very next tick regardless, so "removing"
    ///   one would just be a no-op loop, not a real decommission.
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
        if let Some(control_id) = node.checked_sub(config::RAFTKV_ID_BASE) {
            if self.admin.control_ids.contains(&control_id) {
                return Err(format!(
                    "node {node} is an original control-plane core member (control id \
                     {control_id}); the control group is static (ADR 0030) and this member \
                     must never be decommissioned"
                ));
            }
        }
        // Check leadership BEFORE reading `self.control.metadata_cached()` for the
        // drain-status refusals below: a follower's own replica can lag the
        // leader's just-committed rebalance/release-GC moves (real replication
        // lag, not a bug), so evaluating "is it drained" off a follower's stale
        // view can misfire as "still referenced" even after the operator has
        // confirmed (on the leader) that draining converged — surfacing the
        // wrong refusal instead of the intended "retry on the leader" routing
        // error. The leader's own metadata is what actually gates the apply, so
        // checking leadership first makes every other refusal here trustworthy.
        let Some(leader) = self.edge.leader_handle() else {
            return Err(self.not_leader_error());
        };
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
        let referenced = meta.tablets_referencing(node);
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
/// `Metadata.cp_member_addrs` ∪ `Metadata.node_addrs[*].raftkv` (Phase 2.3a address
/// distribution, extended by ADR 0032 PR1's node address book — a node's own
/// `raftkv` address is registered there too, so a node that joined via `RegisterNodeAddrs`
/// alone, without ever going through `RegisterCpAddr`, is still reachable). `set_peers`
/// replaces the book and a `sibling` env shares the same book `Arc`, so syncing the
/// `raftkv` env reaches every co-resident CP group. Idempotent each tick; runs for
/// the life of the node (a perpetual loop, aborted on `shutdown`). A peer entry
/// whose address fails to parse is skipped (the control plane stores it opaquely).
///
/// Takes the whole [`ClientCtx`] (not a bare `RaftNode`) so a control-plane-
/// follower-less growth node (ADR 0030) reads `effective_metadata` — its mirror
/// of the real cluster's `cp_member_addrs`/`node_addrs` — instead of its own
/// never-replicated local raft; every other node is unaffected (`effective_metadata`
/// passes through to `raft.metadata()` there).
async fn peer_sync_loop(
    ctx: ClientCtx,
    raftkv_env: ProdEnv,
    static_peers: BTreeMap<NodeId, SocketAddr>,
) {
    loop {
        let mut book = static_peers.clone();
        let meta = ctx.effective_metadata();
        for (id, addr) in meta.cp_member_addrs {
            if let Ok(sa) = addr.parse::<SocketAddr>() {
                book.insert(id, sa);
            }
        }
        for (id, addrs) in meta.node_addrs {
            if let Ok(sa) = addrs.raftkv.parse::<SocketAddr>() {
                book.insert(id, sa);
            }
        }
        raftkv_env.set_peers(book);
        tokio::time::sleep(PEER_SYNC_INTERVAL).await;
    }
}

/// Keep `ctx.client_route` = the **static** seed (this node's own config-time
/// route table) ∪ the replicated `Metadata.node_addrs[*].client` (ADR 0032 PR1),
/// so a node grown in after this node's own startup becomes a valid forward
/// target for a client op / `propose_schema` relay — closing the ADR 0030
/// residual gap where `client_route` was a process-start-only snapshot. Sibling
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

/// How often a control-plane-follower-less growth node (ADR 0030) refreshes its
/// mirror of the real cluster's `Metadata` — see [`remote_metadata_sync_loop`].
/// Brisk, matching [`PEER_SYNC_INTERVAL`]'s cadence: the mirror gates the same
/// kind of "did my own registration land yet" / "was I placed on a tablet yet"
/// polling loops that read it. **Not** used by an ADR 0035 PR4 data-only
/// node's mirror sync anymore (PR5) — that now long-polls, see
/// [`remote_metadata_watch_loop`].
const REMOTE_METADATA_SYNC_INTERVAL: Duration = Duration::from_millis(200);

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
/// current"**, which now covers two shapes with two different mechanisms
/// (ADR 0035 PR5): an ADR 0030 **growth node** (`seeds` = the pre-growth
/// control nodes' client addresses; `ctx.control` is `Local`, so the mirror
/// lands in `ctx.remote_metadata` and `effective_metadata` prefers it) keeps
/// the original fixed-[`REMOTE_METADATA_SYNC_INTERVAL`] `Status` poll below;
/// an ADR 0035 PR4 **data-only node** (`seeds` = the control deployment's
/// client addresses; `ctx.control` is `Remote`) now long-polls instead — see
/// [`remote_metadata_watch_loop`]. A no-op (returns immediately) when `seeds`
/// is empty — the case for every node that *is* a real control-group voter,
/// since `effective_metadata` then passes straight through to
/// `self.control.metadata_cached()` and nothing needs mirroring.
async fn remote_metadata_sync_loop(ctx: ClientCtx, seeds: Vec<SocketAddr>) {
    if seeds.is_empty() {
        return;
    }
    if let ControlHandle::Remote(remote) = &ctx.control {
        return remote_metadata_watch_loop(remote.clone(), seeds).await;
    }
    // Growth-node (ADR 0030) branch, unchanged from before PR5: tries every
    // seed in order each tick, keeping whichever answers first. Best-effort —
    // a tick where every seed is unreachable just leaves the previous
    // snapshot in place (stale, not wrong — every consumer already tolerates
    // a few hundred milliseconds of staleness from its own polling cadence).
    loop {
        for &addr in &seeds {
            if let ClientResponse::Status { metadata, .. } =
                ctx.relay(addr, ClientRequest::Status).await
            {
                *ctx.remote_metadata
                    .lock()
                    .expect("remote metadata poisoned") = Some(metadata);
                break;
            }
        }
        tokio::time::sleep(REMOTE_METADATA_SYNC_INTERVAL).await;
    }
}

/// **Long-poll metadata sync for a data-only node's [`RemoteControlClient`]**
/// (ADR 0035 PR5): replaces the old fixed-[`REMOTE_METADATA_SYNC_INTERVAL`]
/// poll with a [`ClientRequest::WatchMetadata`] round trip parked on the
/// answering control node's own `MetadataWatch` — so a metadata change is
/// observed roughly as soon as the control leader's own commit makes it
/// visible plus one network hop, not up to one 200ms poll cycle later. Tries
/// the current leader hint first (mirroring
/// [`RemoteControlClient::metadata_fresh`]'s own candidate order — the leader
/// is the node most likely to have just applied the change this loop is
/// waiting for), then every seed in order.
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
        if let Some(addr) = remote.leader_addr_hint() {
            candidates.push(addr);
        }
        candidates.extend(seeds.iter().copied());

        let mut synced = false;
        for addr in candidates {
            if let ClientResponse::Status {
                metadata,
                leader_hint,
                watermark,
            } = relay_request_with_timeout(
                addr,
                &ClientRequest::WatchMetadata { last_seen },
                WATCH_METADATA_CLIENT_TIMEOUT,
            )
            .await
            {
                remote.observe(metadata, leader_hint, watermark);
                synced = true;
                break;
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
                watermark,
            } = relay_request(addr, &ClientRequest::Status).await
            {
                remote.observe(metadata, leader_hint, watermark);
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

/// The `StorageScope` prefix confining `table`'s tablets on a node's shared
/// engine: `escape(table)`. Order-preserving and prefix-free
/// (`animus_tablet::escape`), so one table's keys can never collide with
/// another's even though every table's tablets share one physical
/// `LsmEngine`/`MemoryEngine` (ADR 0026/0028).
fn table_scope_prefix(table: &str) -> Vec<u8> {
    escape(table.as_bytes())
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
    loop {
        tokio::select! {
            _ = watch.changed(last_seen) => {}
            _ = tokio::time::sleep(RECONCILE_FALLBACK_INTERVAL) => {}
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
            .map(|(id, _)| *id)
            .collect();
        let view = MetadataView {
            tablets: meta.tablets,
            down,
            merged: meta.merged_tablets,
        };
        reconciler.tick(&view).await;
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
        // sees the live tablet map, not an empty local core's.
        let tablets: Vec<TabletId> = ctx.effective_metadata().tablets.keys().copied().collect();
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
            if !key_hot && !byte_hot && !due_confirm {
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
            // Need at least 2 distinct keys for any split to have an interior
            // point (`SplitTablet` requires `start < at < end`).
            if key_count < 2 || (!over_key_threshold && !over_byte_threshold) {
                continue;
            }
            // A byte-configured cluster uses the byte-weighted median (ADR
            // 0034) so a skewed value-size distribution still bisects the
            // tablet's *bytes* roughly evenly; a key-count-only cluster keeps
            // the plain positional median unchanged from before this ADR (the
            // interior key of `> threshold >= 2` distinct keys `SplitTablet`
            // accepts).
            let split_key = if thresholds.bytes.is_some() {
                byte_weighted_median(&pairs)
            } else {
                pairs[pairs.len() / 2].0.clone()
            };
            last_triggered.insert(tablet, tokio::time::Instant::now());
            let span = tracing::info_span!("auto_split", tablet = tablet.0);
            let response = ctx.trigger_split(tablet, split_key).instrument(span).await;
            if !matches!(response, ClientResponse::PutOk) {
                tracing::warn!(
                    tablet = tablet.0,
                    ?response,
                    "auto_split: split did not commit"
                );
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
        ClientRequest::MergeTablets { .. } => "merge_tablets",
        ClientRequest::JoinInfo => "join_info",
        ClientRequest::WatchMetadata { .. } => "watch_metadata",
    }
}

async fn handle_request(ctx: &ClientCtx, request: ClientRequest) -> ClientResponse {
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
            watermark: ctx.control.metadata_watch().latest(),
        },
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
        // Admin: split a CP tablet — a single atomic control-plane command.
        ClientRequest::SplitTablet { tablet, split_key } => {
            ctx.trigger_split(TabletId(tablet), split_key).await
        }
        // Admin: merge two adjacent CP tablets (ADR 0033) — a single atomic
        // control-plane command, the dual of split above.
        ClientRequest::MergeTablets { left, right } => {
            ctx.trigger_merge(TabletId(left), TabletId(right)).await
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
        // Join discovery (ADR 0032 PR2): any node answers from its own
        // knowledge — no forwarding, no leader resolution needed.
        ClientRequest::JoinInfo => ClientResponse::JoinInfo {
            control_ids: ctx.admin.control_ids.clone(),
            peers: ctx.admin.peers.clone(),
            client_route: ctx.route_snapshot(),
            admin_addrs: ctx.admin.admin_addrs.clone(),
        },
        // Long-poll metadata watch (ADR 0035 PR5) — see `ClientCtx::
        // watch_metadata`'s doc.
        ClientRequest::WatchMetadata { last_seen } => ctx.watch_metadata(last_seen).await,
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
    /// catalog, ADR 0013), read from this node's **cache-tolerant**
    /// `effective_metadata()` (ADR 0035 PR1 — previously a bare
    /// `self.control.metadata_cached()`, which on a control-plane-follower-less
    /// growth node never reflects anything since that node's own local control
    /// raft never replicates; this closes that latent staleness bug for the
    /// CQL/DynamoDB wire edges' general schema lookups). Every node applies
    /// committed metadata, so a follower sees a table the leader created once
    /// the entry replicates. Returns `None` for an unknown table.
    ///
    /// **Not** for a schema commit-wait poll's own confirmation — those must
    /// observe read-your-writes and go through
    /// [`metadata_fresh`](Self::metadata_fresh) directly instead (see
    /// `create_table_schema`/`replace_table_schema`/`drop_table_schema` below).
    pub(crate) fn table_schema(&self, table: &str) -> Option<TableSchema> {
        self.effective_metadata().table_schema(table).cloned()
    }

    /// Whether `table` has a replicated schema — see [`table_schema`](Self::table_schema)'s
    /// doc for the same cache-tolerant-but-not-commit-wait-safe contract.
    pub(crate) fn has_table_schema(&self, table: &str) -> bool {
        self.effective_metadata().has_table_schema(table)
    }

    /// Register this node's **full address book** (ADR 0032 PR1: raftkv +
    /// client + admin) in the replicated `Metadata`, so any node — including one
    /// that joined the cluster earlier and never restarted — can resolve where
    /// to forward/relay a client op or an admin peer-list entry, closing the ADR
    /// 0030 residual gap where `client_route`/the admin peer list were static
    /// per-process snapshots taken once at startup. Supersedes the old
    /// `register_cp_addr` (kept as `MetaCommand::RegisterCpAddr` only for WAL
    /// back-compat; no longer proposed). Routes to the control leader via the
    /// relay (`RegisterNodeAddrs` is in [`is_relayable_command`]'s allowlist) and
    /// waits until the entry is visible here, re-proposing each tick. Best-effort
    /// (bounded by [`SCHEMA_COMMIT_TIMEOUT`]); idempotent (re-registering an
    /// identical address book is a state-machine no-op).
    pub(crate) async fn register_node_addrs(&self, node: NodeId, addrs: NodeAddrs) {
        // `effective_metadata`, not `self.control.metadata_cached()` directly: on a growth
        // node (ADR 0030) this is the *only* signal that its own self-registration
        // actually landed on the real cluster, since its local raft never
        // replicates — see `effective_metadata`'s doc. `propose_and_await`'s
        // relay reaches a real leader via `propose_schema`'s no-known-leader
        // broadcast fallback, but confirmation must poll the mirror.
        if self.effective_metadata().node_addrs.get(&node) == Some(&addrs) {
            return;
        }
        let want = addrs.clone();
        let _ = self
            .propose_and_await(
                MetaCommand::RegisterNodeAddrs { node, addrs },
                SCHEMA_COMMIT_TIMEOUT,
                || async {
                    (self.effective_metadata().node_addrs.get(&node) == Some(&want)).then_some(())
                },
            )
            .await;
    }

    /// Split CP `tablet` at `split_key`: a **single, atomic** control-plane
    /// command (`MetaCommand::SplitTablet`, epoch-CAS gated exactly like
    /// `CasTabletReplicas`). The source tablet's range narrows to `[lo,
    /// split_key)` and a new sibling tablet is minted covering `[split_key, ∞)`
    /// — both served by the **same** replicas' existing per-node shared engine
    /// (ADR 0026/0028: one LSM tree per node, confined by [`StorageScope`]), so
    /// no data moves and there is no second, data-plane step that can fail
    /// independently. The per-node tablet-host reconciler (ADR 0031 PR4) then
    /// forms the new sibling's Raft group on every replica (a fresh
    /// whole-voter formation, identical to a brand-new table's tablet) —
    /// orphaned, leaderless metadata-only tablets are structurally impossible
    /// now, since commit of this one command is the whole operation.
    ///
    /// Routed to the control leader (relayable, [`is_relayable_command`]), so
    /// this works from any node the client happens to be connected to.
    #[tracing::instrument(
        name = "split_tablet",
        skip(self, split_key),
        fields(tablet = tablet.0, new_id = tracing::field::Empty)
    )]
    async fn trigger_split(&self, tablet: TabletId, split_key: Vec<u8>) -> ClientResponse {
        // The new tablet id comes from the **monotonic allocator**
        // (`next_free_tablet_id`, ADR 0023 — the same allocator provisioning
        // uses), *not* `max(existing ids) + 1`, which could re-mint a freed id
        // after a `DropTableTablets`. `new_id` and `expected_epoch` come from
        // the **same** metadata snapshot so the CAS reflects exactly what this
        // call saw.
        //
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
        let meta = self.effective_metadata();
        let new_id = meta.next_free_tablet_id();
        let Some(expected_epoch) = meta.tablets.get(&tablet).map(|t| t.epoch) else {
            return ClientResponse::Error("no such tablet".into());
        };
        tracing::Span::current().record("new_id", new_id.0);
        let cmd = MetaCommand::SplitTablet {
            tablet,
            expected_epoch,
            split_key,
            new_id,
        };
        match self
            .propose_and_await(cmd, SCHEMA_COMMIT_TIMEOUT, || async {
                self.effective_metadata()
                    .tablets
                    .contains_key(&new_id)
                    .then_some(())
            })
            .await
        {
            Ok(()) => ClientResponse::PutOk,
            Err(()) => ClientResponse::Error("split did not commit in time".into()),
        }
    }

    /// Merge adjacent CP tablets `left` and `right` (ADR 0033): a **single,
    /// atomic** control-plane command (`MetaCommand::MergeTablets`,
    /// epoch-CAS gated on both tablets — the dual of `trigger_split` above).
    /// `left`'s range widens to `[left.start, right.end)` and `right` is
    /// removed from the tablet map — both served by the **same** replicas'
    /// existing per-node shared engine (ADR 0026/0028: one LSM tree per node,
    /// confined by `StorageScope`), so no data moves and there is no second,
    /// data-plane step that can fail independently. The per-node tablet-host
    /// reconciler (ADR 0031 PR4, extended by ADR 0033) then widens `left`'s
    /// live scope and tears down `right`'s group on every replica **without
    /// erasing its data** — a sibling now owns that range on the same shared
    /// engine.
    ///
    /// Routed to the control leader (relayable, [`is_relayable_command`]), so
    /// this works from any node the client happens to be connected to.
    #[tracing::instrument(name = "merge_tablets", skip(self), fields(left = left.0, right = right.0))]
    async fn trigger_merge(&self, left: TabletId, right: TabletId) -> ClientResponse {
        // Both epochs come from the **same** metadata snapshot, so the CAS
        // reflects exactly what this call saw (mirroring `trigger_split`'s
        // `new_id`/`expected_epoch` pairing). `effective_metadata()` for the
        // same reason as `trigger_split` — see that method's comment.
        let meta = self.effective_metadata();
        let Some(expected_left_epoch) = meta.tablets.get(&left).map(|t| t.epoch) else {
            return ClientResponse::Error("no such tablet".into());
        };
        let Some(expected_right_epoch) = meta.tablets.get(&right).map(|t| t.epoch) else {
            return ClientResponse::Error("no such tablet".into());
        };
        let cmd = MetaCommand::MergeTablets {
            left,
            expected_left_epoch,
            right,
            expected_right_epoch,
        };
        // Confirm by a signal robust against `right` vanishing for an
        // unrelated reason (e.g. a concurrent table drop): `left`'s epoch
        // must have advanced past what this call read AND `right` must be
        // gone — the exact pair `MergeTablets`'s apply produces together,
        // atomically, and nothing else in this state machine produces.
        match self
            .propose_and_await(cmd, SCHEMA_COMMIT_TIMEOUT, || async {
                let m = self.effective_metadata();
                let left_bumped = m
                    .tablets
                    .get(&left)
                    .is_some_and(|t| t.epoch > expected_left_epoch);
                (left_bumped && !m.tablets.contains_key(&right)).then_some(())
            })
            .await
        {
            Ok(()) => ClientResponse::PutOk,
            Err(()) => ClientResponse::Error("merge did not commit in time".into()),
        }
    }

    /// Propose `CreateKeyspace` to the control-plane leader and wait for it to
    /// commit + replicate here (v1 A3): a CQL `CREATE KEYSPACE` is durable +
    /// cluster-agreed, surviving restart, instead of living in per-process edge
    /// state. Idempotent (an existing keyspace returns immediately). Routes via the
    /// A2 leader relay; times out after [`SCHEMA_COMMIT_TIMEOUT`].
    pub(crate) async fn create_keyspace(&self, keyspace: String) -> Result<(), String> {
        // Fresh, not `self.has_keyspace` (ADR 0035 PR5 staleness-audit fix —
        // see `create_table_schema`'s identical note): this whole function is
        // the CreateKeyspace commit-wait poll, which must observe its own
        // just-proposed command landing in the authoritative state, never a
        // cache-tolerant mirror that could still be a poll interval behind —
        // the pre-fix version, keyed on `has_keyspace`'s (then)
        // `metadata_cached()`-based check, never resolved at all on a
        // control-plane-follower-less growth node (permanently empty local
        // view), so `CREATE KEYSPACE` always timed out there even after
        // genuinely committing to the real cluster.
        if self.metadata_fresh().await.has_keyspace(&keyspace) {
            return Ok(());
        }
        let ks = keyspace.clone();
        self.propose_and_await(
            MetaCommand::CreateKeyspace {
                keyspace: keyspace.clone(),
            },
            SCHEMA_COMMIT_TIMEOUT,
            || async { self.metadata_fresh().await.has_keyspace(&ks).then_some(()) },
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
        // Fresh, not `self.table_schema` (ADR 0035 PR1): this whole function is
        // the CreateTable commit-wait poll, which must observe its own
        // just-proposed command landing in the authoritative state, never a
        // growth-node mirror that could still be a poll interval behind.
        if let Some(existing) = self.metadata_fresh().await.table_schema(&table).cloned() {
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
        self.propose_and_await(command, SCHEMA_COMMIT_TIMEOUT, || async {
            self.metadata_fresh().await.table_schema(&table).cloned()
        })
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
        // Fresh, not `self.table_schema` (ADR 0035 PR1) — see
        // `create_table_schema`'s identical note: this is a commit-wait poll.
        self.propose_and_await(command, SCHEMA_COMMIT_TIMEOUT, || async {
            (self.metadata_fresh().await.table_schema(&table) == Some(&schema)).then_some(())
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
    /// the replicated tablet map — the trigger each hosting node's per-node
    /// tablet-host reconciler (ADR 0031 PR4) converges on by stopping its
    /// local group and deleting its engine + WAL files. This is the real
    /// `DROP TABLE` sink (CQL + the admin
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
    /// table returns `Ok(())` immediately. Routes to the leader exactly as
    /// [`create_table_schema`](Self::create_table_schema). Schema-only: does
    /// **not** touch the table's tablets/data (the admin panel's schema-only
    /// drop uses this); a real drop goes through [`drop_table`](Self::drop_table)
    /// and an `ALTER TABLE` replaces in place via
    /// [`replace_table_schema`](Self::replace_table_schema).
    pub(crate) async fn drop_table_schema(&self, table: String) -> Result<(), String> {
        // Fresh, not `self.has_table_schema` (ADR 0035 PR1) — see
        // `create_table_schema`'s identical note: this is a commit-wait poll.
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
            role: config::NodeRole::Both,
            control: Some(addr()),
            client: addr(),
            dynamo: addr(),
            cql: addr(),
            raftkv: Some(addr()),
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
    start_cluster_inner(bound, backend, None, None).await
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
    start_cluster_inner(bound, StorageBackend::default(), Some(threshold), None).await
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
    start_cluster_inner(bound, backend, auto_split, None).await
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
    start_cluster_inner(bound, backend, auto_split_keys, auto_split_bytes).await
}

async fn start_cluster_inner(
    bound: Vec<BoundNode>,
    backend: StorageBackend,
    auto_split_threshold: Option<usize>,
    auto_split_bytes_threshold: Option<u64>,
) -> std::io::Result<Vec<Node>> {
    let n = bound.len();
    let control_ids: Vec<NodeId> = (0..n).map(config::control_id).collect();
    // ADR 0035 PR2: `bind_cluster` (the only producer of a `Vec<BoundNode>`
    // this function is ever called with) always assembles combined-mode
    // (`Both`-role) nodes, so every bound node's own `raftkv_id` is a genuine
    // data-role member — read straight off each `BoundNode` rather than
    // re-deriving from `control_ids`, so this stays correct even if a future
    // caller's `bound` isn't a contiguous `0..n` index range.
    let data_raftkv_ids: Vec<NodeId> = bound.iter().map(|b| b.raftkv_id).collect();
    let peers: BTreeMap<NodeId, SocketAddr> =
        bound.iter().flat_map(BoundNode::peer_entries).collect();
    // Cross-node routing (ADR 0017 #3b / ADR 0013): map each node's CP group
    // member id (`raftkv_id`) **and** its control id to that node's client API
    // address, so an op landing on a node that isn't the relevant leader
    // forwards to the leader's node — identical to the per-process path
    // (`run_node_with`). `--cluster N` gives **each node its own
    // `ClusterEdgeState`** (below), matching one-process-per-node exactly:
    // cross-node reach happens only through this real forwarding/relay path,
    // never a shared in-process registry (root `CLAUDE.md`'s documented
    // "shared edge masks per-node bugs" gotcha — this removes the sharing).
    // This is only the **static seed**: `start_with` hands it to each node's
    // own `route_sync_loop`, which keeps it live thereafter by overlaying
    // `Metadata.node_addrs[*].client` (ADR 0032 PR1) — so a node grown into
    // the cluster later is still reachable from every original node.
    let client_route: BTreeMap<NodeId, SocketAddr> = bound
        .iter()
        .flat_map(|b| [(b.raftkv_id, b.client_addr), (b.control_id, b.client_addr)])
        .collect();
    // Every node's admin address, so each node's dashboard (ADR 0021) can fan out
    // to the whole in-process cluster.
    let admin_addrs: Vec<SocketAddr> = bound.iter().map(BoundNode::admin_addr).collect();
    let mut nodes = Vec::with_capacity(n);
    for b in bound {
        let node = b
            .start_with(
                peers.clone(),
                control_ids.clone(),
                data_raftkv_ids.clone(),
                backend,
                // A fresh, node-local edge-state set per node — never shared
                // across the in-process cluster (see the `client_route`
                // comment above).
                ClusterEdgeState::new(),
                client_route.clone(),
                auto_split_threshold,
                auto_split_bytes_threshold,
                admin_addrs.clone(),
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
/// `config::control_id`/`config::raftkv_id` applied straight to those
/// indexes) and same per-node `dir/node-{index}` subdirectory layout as
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
    let dir = dir.into();
    let total = control_n + data_n;
    let ephemeral = || SocketAddr::new(ip, 0);

    let mut control_bound = Vec::with_capacity(control_n);
    for i in 0..control_n {
        let addrs = RoleAddrs {
            role: config::NodeRole::Control,
            control: Some(ephemeral()),
            client: ephemeral(),
            dynamo: ephemeral(),
            cql: ephemeral(),
            raftkv: None,
            admin: ephemeral(),
        };
        control_bound.push(
            Node::bind_control(config::control_id(i), addrs, dir.join(format!("node-{i}"))).await?,
        );
    }
    let mut data_bound = Vec::with_capacity(data_n);
    for i in control_n..total {
        let addrs = RoleAddrs {
            role: config::NodeRole::Data,
            control: None,
            client: ephemeral(),
            dynamo: ephemeral(),
            cql: ephemeral(),
            raftkv: Some(ephemeral()),
            admin: ephemeral(),
        };
        data_bound.push(
            Node::bind_data(config::raftkv_id(i), addrs, dir.join(format!("node-{i}"))).await?,
        );
    }

    let control_ids: Vec<NodeId> = (0..control_n).map(config::control_id).collect();

    // Each role's own internal peer book, plus the union a data node's single
    // `raftkv` env needs (its `heartbeat_loop` targets the control ids over
    // that same env — `ClusterConfig::control_peer_book`'s doc explains why
    // `raftkv_peer_book()` alone isn't enough).
    let control_peer_book: BTreeMap<NodeId, SocketAddr> = control_bound
        .iter()
        .map(|b| (b.control_id, b.control_addr))
        .collect();
    let raftkv_peer_book: BTreeMap<NodeId, SocketAddr> = data_bound
        .iter()
        .map(|b| (b.raftkv_id, b.raftkv_addr))
        .collect();
    let mut data_env_peers = raftkv_peer_book;
    data_env_peers.extend(control_peer_book.clone());

    // Cross-node routing (ADR 0017 #3b / ADR 0013): every control id and
    // every raftkv id resolves to its node's client API address, exactly like
    // `run_node_control`/`run_node_data`'s per-process assembly.
    let mut client_route: BTreeMap<NodeId, SocketAddr> = BTreeMap::new();
    for b in &control_bound {
        client_route.insert(b.control_id, b.client_addr);
    }
    for b in &data_bound {
        client_route.insert(b.raftkv_id, b.client_addr);
    }

    // The control deployment's client addresses — the discovery root each
    // data node's `ControlHandle::Remote` mirrors from.
    let control_client_addrs: Vec<SocketAddr> =
        control_bound.iter().map(|b| b.client_addr).collect();

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
                admin_addrs.clone(),
            )
            .await,
        );
    }
    for b in data_bound {
        nodes.push(
            b.start_data_with(
                data_env_peers.clone(),
                control_ids.clone(),
                control_client_addrs.clone(),
                backend,
                // A fresh, node-local edge-state set per node — never shared
                // (see the doc comment above).
                ClusterEdgeState::new(),
                client_route.clone(),
                auto_split_threshold,
                auto_split_bytes_threshold,
                admin_addrs.clone(),
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
            config.raftkv_ids(),
            backend,
            ClusterEdgeState::new(),
            client_route,
            None,
            None,
            admin_addrs,
        )
        .await
}

/// Start node `index` from `config` as a **control-only** node (ADR 0035
/// PR3, `animusd control`): binds only the control internal `ProdEnv` role
/// plus the client + admin listeners, and runs only the control [`RaftNode`]
/// (its own `reconcile_loop`/`detect_loop`) plus the tail every node shape
/// shares (`route_sync_loop`/`metrics_sample_loop`/self-registration/
/// `serve_clients`/admin `serve`, via [`BoundControlNode::start_control_with`])
/// — no storage engine, no `raftkv` env, no DynamoDB/CQL listeners.
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
/// control role, or propagates a bind failure.
pub async fn run_node_control(
    config: &ClusterConfig,
    index: usize,
    dir: impl Into<PathBuf>,
) -> std::io::Result<Node> {
    let addrs = *config.nodes.get(index).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "node index out of range")
    })?;
    if !addrs.role.has_control() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "node index does not run the control role",
        ));
    }
    let bound = Node::bind_control(config::control_id(index), addrs, dir).await?;

    // Cross-node routing (ADR 0017 #3b / ADR 0013): map every control-role
    // node's control id and every data-role node's raftkv id to that node's
    // client API address, so a data op or a schema-DDL relay landing on this
    // control node forwards to the right node — the same shape
    // `run_node_with` builds, role-filtered (a control-only entry has no
    // `raftkv` id to route to; a data-only entry has no `control` id).
    let mut client_route: BTreeMap<NodeId, SocketAddr> = BTreeMap::new();
    for (i, a) in config.nodes.iter().enumerate() {
        if a.role.has_control() {
            client_route.insert(config::control_id(i), a.client);
        }
        if a.role.has_data() {
            client_route.insert(config::raftkv_id(i), a.client);
        }
    }
    // Every node's admin address from the shared config, so this node's
    // dashboard (ADR 0021) can fan out to the whole cluster (control and data
    // nodes alike).
    let admin_addrs: Vec<SocketAddr> = config.nodes.iter().map(|n| n.admin).collect();

    Ok(bound
        .start_control_with(
            config.control_peer_book(),
            config.control_ids(),
            client_route,
            admin_addrs,
        )
        .await)
}

/// Start node `index` from `config` as a **data-only** node (ADR 0035 PR4,
/// `animusd data`): binds only the `raftkv` internal `ProdEnv` role plus the
/// client/dynamo/cql/admin listeners, and runs no local control `RaftCore` at
/// all — `Metadata` comes from a polled mirror of the control deployment
/// (`ControlHandle::Remote`, [`BoundDataNode::start_data_with`]) rather than
/// this process's own Raft replication.
///
/// `config`'s data-role entries (`ClusterConfig::data_indexes`) are this
/// node's data fleet — `index` must be one of them. `config`'s control-role
/// entries (`ClusterConfig::control_ids`) are the **separately-deployed**
/// control plane this node mirrors: their **client** addresses seed the
/// mirror + leader-hint sync loop and `propose_schema`'s relay/broadcast
/// tiers (ADR 0035 §1/§4), and their **control** ids are what this node's own
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
    let addrs = *config.nodes.get(index).ok_or_else(|| {
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
    let bound = Node::bind_data(config::raftkv_id(index), addrs, dir).await?;

    // The control deployment's **client**-API addresses — the mirror/
    // leader-hint discovery root (ADR 0035 §1/§4), a wholly different address
    // axis from the internal `raftkv`-env peer book below.
    let control_client_addrs: Vec<SocketAddr> = config
        .nodes
        .iter()
        .filter(|a| a.role.has_control())
        .map(|a| a.client)
        .collect();

    // Cross-node routing (ADR 0017 #3b / ADR 0013): map every control-role
    // node's control id and every data-role node's raftkv id to that node's
    // client API address — the same shape `run_node_control` builds,
    // role-filtered (a data-only entry has no `control` id to route to).
    let mut client_route: BTreeMap<NodeId, SocketAddr> = BTreeMap::new();
    for (i, a) in config.nodes.iter().enumerate() {
        if a.role.has_control() {
            client_route.insert(config::control_id(i), a.client);
        }
        if a.role.has_data() {
            client_route.insert(config::raftkv_id(i), a.client);
        }
    }
    // Every node's admin address from the shared config, so this node's
    // dashboard fan-out (ADR 0021) covers the whole split deployment.
    let admin_addrs: Vec<SocketAddr> = config.nodes.iter().map(|n| n.admin).collect();

    bound
        .start_data_with(
            // This node's `raftkv` env peer book: the union of every
            // data-role node's `raftkv` address and every control-role
            // node's `control` address — `ClusterConfig::peer_book`, not
            // `raftkv_peer_book()` alone (see `control_peer_book`'s doc for
            // why: `heartbeat_loop` below sends to `control_ids` over this
            // very env).
            config.peer_book(),
            control_ids,
            control_client_addrs,
            backend,
            ClusterEdgeState::new(),
            client_route,
            None,
            None,
            admin_addrs,
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
    let mut client_route: BTreeMap<NodeId, SocketAddr> = BTreeMap::new();
    for (i, addrs) in config.nodes.iter().enumerate() {
        client_route.insert(config::raftkv_id(i), addrs.client);
        client_route.insert(config::control_id(i), addrs.client);
    }
    let admin_addrs: Vec<SocketAddr> = config.nodes.iter().map(|n| n.admin).collect();
    // ADR 0035 PR2: `bootstrap` must never auto-register this growth node
    // itself (it self-registers `Down` via `admin_add_member` instead, see
    // this fn's doc) — so, mirroring `original_control_ids`, scope
    // `data_raftkv_ids` to the **pre-growth** set's paired raftkv ids, not
    // `config`'s (expanded) `raftkv_ids()`. Identical to what `start_with`
    // used to derive unconditionally from `control_ids.len()` before this
    // parameter existed.
    let data_raftkv_ids: Vec<NodeId> = original_control_ids
        .iter()
        .map(|&id| config::raftkv_id(id as usize))
        .collect();
    bound
        .start_with(
            config.peer_book(),
            original_control_ids,
            data_raftkv_ids,
            backend,
            ClusterEdgeState::new(),
            client_route,
            None,
            None,
            admin_addrs,
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
        if let Ok(Some(resp)) = reply {
            if !matches!(resp, ClientResponse::Error(_)) {
                return Some(resp);
            }
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

/// Start node `index` as a **seed/join growth member** (ADR 0032 PR2,
/// `animusd join`): unlike [`run_node_growth`], which needs an
/// operator-assembled *expanded* `ClusterConfig` listing every node's
/// addresses up front, this entry point needs only `addrs` (this node's own
/// six addresses) and `seeds` (any already-running node's **client**
/// address — old or newly grown, it no longer matters which, since ADR 0032
/// PR1 made every node's address book equally current).
///
/// It contacts a seed for a [`ClientRequest::JoinInfo`] reply (the
/// pre-growth control group + the answering node's internal peer book + its
/// live client-op route + every known admin address), runs a **collision
/// guard** against a [`ClientRequest::Status`] reply's `node_addrs` (below),
/// then hands the discovered `original_control_ids` + merged peer/route/admin
/// sets straight into [`BoundNode::start_with`] exactly like
/// [`run_node_growth`] does — the ADR 0030 growth machinery
/// (`!control_ids.contains(&self.control_id)` detection,
/// `remote_metadata_sync_loop`, `effective_metadata`) engages automatically,
/// including this node's own ADR 0032 PR1 address self-registration and its
/// own [`ClientCtx::admin_add_member`] self-registration (see `start_with`'s
/// growth-node block) — no separate step is needed here for either.
///
/// **Collision guard.** Before binding, this checks the `Status` reply's
/// `node_addrs` for an existing entry at `config::raftkv_id(index)`: an
/// **identical** entry (the same three addresses) is a *rejoin* of this
/// exact node (a restart with the same index/addresses/dir) and this
/// proceeds normally; a **different** entry means `index` is already
/// claimed by a live member with different addresses, and startup fails
/// loudly with an `AlreadyExists` error instead of silently colliding with
/// it. This narrows, but does not fully eliminate, the race between two
/// simultaneous joiners choosing the same index — `RegisterNodeAddrs` is
/// idempotent at apply time (ADR 0032 PR1), so a genuine simultaneous
/// collision is caught by the replicated state machine rather than
/// corrupting anything, but this pre-bind check is a best-effort convenience,
/// not a distributed lock.
///
/// **ADR 0035 PR5**: the discovery + collision-guard steps above are now the
/// factored-out [`discover_join_info`]/[`check_join_collision`] helpers, so
/// [`run_node_data_join`] (the data-only counterpart, `animusd data --seed`)
/// reuses them verbatim instead of duplicating this poll/match/error-format
/// boilerplate.
///
/// # Errors
/// An `io::Error` (`TimedOut`) if no seed answers within
/// [`JOIN_DISCOVERY_BUDGET`], `AlreadyExists` if the collision guard rejects
/// a conflicting address book at this index, or (as [`run_node_growth`]) a
/// bind / engine-open failure.
pub async fn run_node_join(
    seeds: Vec<SocketAddr>,
    index: usize,
    addrs: RoleAddrs,
    dir: &Path,
    backend: StorageBackend,
) -> std::io::Result<Node> {
    if seeds.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "join needs at least one --seed address",
        ));
    }
    // ADR 0035 PR2 is the config layer only: `run_node_join` still only
    // supports joining as a combined-mode (`Both`-role) node — fail fast with
    // a join-specific message rather than surfacing `Node::bind`'s generic
    // one later, once discovery/collision-guard work has already happened.
    let my_raftkv_addr = addrs.raftkv.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "join needs a raftkv address (RoleAddrs.raftkv is None) — \
             data-only join is ADR 0035 PR3/PR4 (see `run_node_data_join`)",
        )
    })?;
    if addrs.control.is_none() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "join needs a control address (RoleAddrs.control is None) — \
             control-only join is ADR 0035 PR3/PR4",
        ));
    }
    let my_control_id = config::control_id(index);
    let my_raftkv_id = config::raftkv_id(index);

    let (original_control_ids, mut peers, mut client_route, mut admin_addrs) =
        discover_join_info(&seeds).await?;
    check_join_collision(
        &seeds,
        index,
        my_raftkv_id,
        &NodeAddrs {
            raftkv: my_raftkv_addr.to_string(),
            client: addrs.client.to_string(),
            admin: addrs.admin.to_string(),
            role: "combined".to_string(),
        },
    )
    .await?;

    let bound = Node::bind(my_control_id, my_raftkv_id, addrs, dir).await?;

    // Merge this node's own entries into the discovered peer/route/admin sets
    // — the same union `run_node_growth`'s expanded-config construction
    // already produces, just built from a discovery reply instead of a
    // pre-assembled config.
    for (id, addr) in bound.peer_entries() {
        peers.insert(id, addr);
    }
    client_route.insert(my_raftkv_id, addrs.client);
    client_route.insert(my_control_id, addrs.client);
    if !admin_addrs.contains(&addrs.admin) {
        admin_addrs.push(addrs.admin);
    }

    // ADR 0035 PR2: as in `run_node_growth`, `bootstrap` must never
    // auto-register this joining node itself — scope `data_raftkv_ids` to
    // the pre-growth set discovered via `JoinInfo`, not including this node.
    let data_raftkv_ids: Vec<NodeId> = original_control_ids
        .iter()
        .map(|&id| config::raftkv_id(id as usize))
        .collect();
    bound
        .start_with(
            peers,
            original_control_ids,
            data_raftkv_ids,
            backend,
            ClusterEdgeState::new(),
            client_route,
            None,
            None,
            admin_addrs,
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
    Vec<SocketAddr>,
)> {
    match poll_seeds_for(seeds, &ClientRequest::JoinInfo, JOIN_DISCOVERY_BUDGET).await? {
        ClientResponse::JoinInfo {
            control_ids,
            peers,
            client_route,
            admin_addrs,
        } => Ok((control_ids, peers, client_route, admin_addrs)),
        other => Err(std::io::Error::other(format!(
            "seed returned an unexpected reply to JoinInfo: {other:?}"
        ))),
    }
}

/// The collision-guard half of [`run_node_join`]/[`run_node_data_join`] (ADR
/// 0035 PR5 — factored out alongside [`discover_join_info`]): reject a
/// conflicting pre-existing registration at `my_id` before binding anything.
/// An **identical** existing entry (a restart with the same index/addresses/
/// dir) is a rejoin and returns `Ok(())`; a **different** one fails loudly
/// with `AlreadyExists` instead of silently colliding with it — see
/// [`run_node_join`]'s doc for the narrower, best-effort race this doesn't
/// fully close (the real guard is `RegisterNodeAddrs`'s own idempotent
/// apply-time check).
async fn check_join_collision(
    seeds: &[SocketAddr],
    index: usize,
    my_id: NodeId,
    mine: &NodeAddrs,
) -> std::io::Result<()> {
    match poll_seeds_for(seeds, &ClientRequest::Status, JOIN_DISCOVERY_BUDGET).await? {
        ClientResponse::Status { metadata: meta, .. } => {
            if let Some(existing) = meta.node_addrs.get(&my_id) {
                if existing != mine {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::AlreadyExists,
                        format!(
                            "join index {index} (id {my_id}) is already \
                             registered with different addresses ({existing:?} != {mine:?}) \
                             — pick a different --node index"
                        ),
                    ));
                }
            }
            Ok(())
        }
        other => Err(std::io::Error::other(format!(
            "seed returned an unexpected reply to Status: {other:?}"
        ))),
    }
}

/// Start node `index` as a **data-only seed/join member** (ADR 0035 PR5): the
/// data-only counterpart of [`run_node_join`], reusing its `JoinInfo`
/// discovery + `Status` collision guard verbatim
/// ([`discover_join_info`]/[`check_join_collision`]) but constructing the
/// **`Remote`** data-role assembly ([`BoundDataNode::start_data_with`])
/// instead of a combined-mode node with a local control `RaftCore`. CLI:
/// `animusd data --seed ADDR[,ADDR...] --node I [--dir D] [--ephemeral]`.
///
/// `addrs.control` must be `None` and `addrs.raftkv` must be `Some` — the
/// dual of `run_node_join`'s own role check (that entry point rejects a
/// missing `raftkv` address as "data-only join is PR3/PR4"; this one is that
/// PR3/PR4). The discovered `original_control_ids` (the seed's `JoinInfo`
/// reply) feed both `heartbeat_loop`'s failure-detection target and, via the
/// merged `client_route`, [`RemoteControlClient::new`]'s `control_seeds` — the
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
/// [`JOIN_DISCOVERY_BUDGET`], `AlreadyExists` if the collision guard rejects
/// a conflicting address book at this index, or a bind / engine-open
/// failure.
pub async fn run_node_data_join(
    seeds: Vec<SocketAddr>,
    index: usize,
    addrs: RoleAddrs,
    dir: &Path,
    backend: StorageBackend,
) -> std::io::Result<Node> {
    if seeds.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "join needs at least one --seed address",
        ));
    }
    let my_raftkv_addr = addrs.raftkv.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "data join needs a raftkv address (RoleAddrs.raftkv is None)",
        )
    })?;
    if addrs.control.is_some() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "data join must not have a control address (RoleAddrs.control is Some) — \
             use `animusd join` for a combined-mode join",
        ));
    }
    let my_raftkv_id = config::raftkv_id(index);

    let (original_control_ids, mut peers, mut client_route, mut admin_addrs) =
        discover_join_info(&seeds).await?;
    check_join_collision(
        &seeds,
        index,
        my_raftkv_id,
        &NodeAddrs {
            raftkv: my_raftkv_addr.to_string(),
            client: addrs.client.to_string(),
            admin: addrs.admin.to_string(),
            role: "data".to_string(),
        },
    )
    .await?;

    let bound = Node::bind_data(my_raftkv_id, addrs, dir).await?;

    // Merge this node's own entries into the discovered peer/route/admin sets
    // — the data-only dual of `run_node_join`'s merge (a single raftkv peer
    // entry, no control id of its own to add).
    let (peer_id, peer_addr) = bound.peer_entry();
    peers.insert(peer_id, peer_addr);
    client_route.insert(my_raftkv_id, addrs.client);
    if !admin_addrs.contains(&addrs.admin) {
        admin_addrs.push(addrs.admin);
    }

    // The control deployment's client-API addresses (ADR 0035 §1/§4) — the
    // same derivation `run_node_data` does from a static `ClusterConfig`,
    // here from the merged, discovery-built `client_route` instead.
    let control_seeds: Vec<SocketAddr> = original_control_ids
        .iter()
        .filter_map(|id| client_route.get(id).copied())
        .collect();

    bound
        .start_data_with(
            peers,
            original_control_ids,
            control_seeds,
            backend,
            ClusterEdgeState::new(),
            client_route,
            None,
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

/// Regression tests for the ADR 0028 write-fence pre-propose check
/// (`cp_put_local`/`cp_delete_local`/`cp_batch_propose`). These live **inside**
/// the crate (as opposed to `tests/*.rs`, a separate crate) specifically to
/// reach the private `CpGroup`/`ClientCtx` handles a real stale-routed write
/// needs to be driven directly against a specific tablet's group — nothing
/// under `tests/` can construct this scenario, since `cp_route`'s normal
/// resolution reads this node's own (freshly polled) `Metadata` and would
/// simply route a post-split write to the correct child on its own.
#[cfg(test)]
mod split_fence_tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use crate::config::NodeRole;
    use crate::{
        ClientCtx, ClientRequest, ClientResponse, ClusterConfig, RoleAddrs, read_frame, run_node,
        write_frame,
    };
    use serde_json::Value;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::time::{sleep, timeout};

    fn free_addrs(count: usize) -> Vec<SocketAddr> {
        let ls: Vec<std::net::TcpListener> = (0..count)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        ls.iter().map(|l| l.local_addr().unwrap()).collect()
    }

    /// Minimal admin HTTP `POST`, mirroring `tests/admin_endpoint.rs`'s helper
    /// (duplicated rather than shared, since this module lives in a different
    /// compilation unit than the `tests/` integration crate).
    async fn admin_post(addr: SocketAddr, path: &str, body: &str) -> (u16, Value) {
        let mut stream = TcpStream::connect(addr).await.expect("connect to admin");
        let request = format!(
            "POST {path} HTTP/1.0\r\n\
             Host: animus\r\n\
             Content-Type: application/json\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\
             \r\n\
             {body}",
            body.len(),
        );
        stream
            .write_all(request.as_bytes())
            .await
            .expect("send request");
        stream.flush().await.expect("flush");
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await.expect("read response");
        let text = String::from_utf8(raw).expect("utf8 response");
        let (head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
        let status: u16 = head
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse().ok())
            .expect("status line");
        let value = serde_json::from_str(payload).unwrap_or(Value::Null);
        (status, value)
    }

    /// A write for a key that a split has just handed off to a CHILD tablet,
    /// driven directly against the **PARENT**'s own `RaftKvNode` handle — the
    /// exact shape of the crossover-window hazard ADR 0028 §3 describes: a node
    /// whose `Metadata` view has not yet observed the split still resolves the
    /// key to the parent's (now too-wide) group via `cp_route`'s `Local`
    /// branch. Before this fix, `cp_put_local` stamped `KeyRange::whole()` and
    /// proposed unconditionally, so the parent's group would accept and apply
    /// the write onto the shared engine's physical key the child now owns — a
    /// silent shadow/corruption of the child's data. This test bypasses
    /// `cp_route` entirely (fetching the parent's group handle directly via
    /// `edge.local_cp`) to drive exactly that write, and asserts it is
    /// rejected — not silently accepted — and never lands in the shared
    /// physical key on the parent's own storage.
    ///
    /// **What this proves and does not prove:** it proves the pre-propose
    /// range check itself — given a write for an out-of-range key handed
    /// directly to a narrowed group's local helper, the write errors instead
    /// of being falsely acked, and the rejected value never reaches the
    /// shared engine (read back, for lack of a scope-range-aware read
    /// primitive, via the parent's own scope-oblivious `local_get`). It does
    /// **not** reproduce the full end-to-end race (a *live* node
    /// actually routing a real client request to the parent because its own
    /// cached `Metadata` genuinely lags the split) — that race depends on
    /// timing between the control-plane replication of `SplitTablet` and a
    /// concurrent client request that is not reliably forceable in a test.
    /// Driving the write directly against the parent's handle is the
    /// deterministic substitute: it exercises the identical code path
    /// (`ClientCtx::cp_put_local` against a `CpGroup`) a stale `cp_route`
    /// resolution would have handed the same key to.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stale_routed_write_for_a_split_childs_key_is_rejected_not_lost() {
        timeout(Duration::from_secs(60), async {
            let dir = tempfile::tempdir().expect("tempdir");
            let addrs = free_addrs(6);
            let config = ClusterConfig {
                nodes: vec![RoleAddrs {
                    role: NodeRole::Both,
                    control: Some(addrs[0]),
                    client: addrs[1],
                    dynamo: addrs[2],
                    cql: addrs[3],
                    raftkv: Some(addrs[4]),
                    admin: addrs[5],
                }],
            };
            let node = run_node(&config, 0, dir.path().join("node-0"))
                .await
                .expect("bind + start a single-node cluster");

            timeout(Duration::from_secs(10), async {
                loop {
                    if node.is_control_leader() {
                        return;
                    }
                    sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("the sole node did not become control leader");

            // Seed the bootstrap tablet with keys spanning the eventual split
            // point, through the real client API.
            let mut stream = TcpStream::connect(node.client_addr())
                .await
                .expect("connect to client port");
            for i in 0..10u32 {
                let key = format!("key{i:02}").into_bytes();
                let value = format!("v{i}").into_bytes();
                write_frame(
                    &mut stream,
                    &ClientRequest::Put {
                        key,
                        value,
                        table: "kv".to_string(),
                    },
                )
                .await
                .expect("send put");
                let resp: ClientResponse = read_frame(&mut stream)
                    .await
                    .expect("read reply")
                    .expect("a reply");
                assert!(
                    matches!(resp, ClientResponse::PutOk),
                    "put failed: {resp:?}"
                );
            }

            let parent_tablet = *node
                .metadata()
                .tablets
                .keys()
                .next()
                .expect("the bootstrap tablet exists");

            let (status, split_resp) = admin_post(
                node.admin_addr(),
                "/admin/tablet/split",
                &format!(r#"{{"tablet":{},"split_key":"key05"}}"#, parent_tablet.0),
            )
            .await;
            assert_eq!(status, 200, "split committed: {split_resp}");

            timeout(Duration::from_secs(15), async {
                loop {
                    if node.metadata().tablets.len() >= 2 {
                        return;
                    }
                    sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .expect("split did not produce two tablets");

            // `key07` is `>= "key05"`, so the split handed it to the child.
            let child_key = b"key07".to_vec();

            // Fetch the PARENT's own group handle directly, bypassing
            // `cp_route`'s normal (now-fresh) resolution — the deterministic
            // stand-in for "a node whose routing decision is stale."
            let parent = node
                .edge
                .local_cp(parent_tablet)
                .expect("this node hosts the parent tablet's group");

            // Sanity: the parent's own live scope really has narrowed past the
            // child key, so the pre-check below is exercising the real thing.
            // `narrow_scope` is applied by the per-node tablet-host
            // reconciler's `NarrowScope` action (ADR 0031 PR4), triggered by
            // the split's commit but not synchronous with it, so poll for it.
            timeout(Duration::from_secs(10), async {
                loop {
                    if !parent.scope_range().contains(&child_key) {
                        return;
                    }
                    sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("the parent's scope never narrowed past the split key");

            let result =
                ClientCtx::cp_put_local(&parent, child_key.clone(), b"corrupt".to_vec()).await;
            assert!(
                result.is_err(),
                "a write for a child-range key driven at the parent must be \
                 rejected, not silently acked: {result:?}"
            );

            // The READ-side dual (ADR 0033): a linearizable get for the same
            // child-range key driven at the parent's handle — the exact shape
            // a stale routing resolution would produce — must surface as a
            // retryable error (so the caller re-resolves and reaches the
            // child), never as a served answer. Serving it would return a
            // value NOT linearized against the child group's writes (the
            // child's leader may be on another node whose writes this engine
            // hasn't applied yet) — or, in the merge crossover's dual (a
            // survivor not yet widened over a not-yet-drained absorbed
            // sibling), a false "absent" indistinguishable from data loss.
            let read = ClientCtx::cp_get_local(&parent, &child_key).await;
            match read {
                Err(e) => assert!(
                    ClientCtx::read_should_retry(&e),
                    "the stale-scope read error must be retryable: {e}"
                ),
                Ok(v) => panic!(
                    "a read for a child-range key driven at the parent must be \
                     a retryable error, never a served answer: {v:?}"
                ),
            }
            // And the scan flavor: a window reaching past the parent's
            // narrowed scope must error retryably, not silently truncate.
            let scan = ClientCtx::cp_scan_local(&parent, b"key00", Some(b"key09"), None).await;
            match scan {
                Err(e) => assert!(
                    ClientCtx::read_should_retry(&e),
                    "the stale-scope scan error must be retryable: {e}"
                ),
                Ok(p) => panic!(
                    "a scan window past the parent's narrowed scope must be a \
                     retryable error, never a (truncated) result: {p:?}"
                ),
            }

            // The physical key `key07` was written (as `v7`) during the initial
            // seed, before the split — its bytes never move (ADR 0028: a split
            // narrows the *scope*, not the data), so `local_get` (which is
            // scope-*range*-oblivious, reading by physical key regardless of
            // which tablet currently logically owns that range) still finds
            // the pre-split value at that shared physical location. The actual
            // safety property under test is that the REJECTED write's value
            // never landed there — i.e. this node's own parent-side storage
            // was never mutated to `corrupt`, which would have been the
            // shadow/corruption this fix exists to prevent.
            assert_ne!(
                parent.local_get(&child_key).await,
                Some(b"corrupt".to_vec()),
                "the rejected write must never land in the shared engine, even \
                 read back through the parent's own (too-wide) scope"
            );
            assert_eq!(
                parent.local_get(&child_key).await,
                Some(b"v7".to_vec()),
                "key07's pre-split value must be untouched by the rejected write"
            );

            node.shutdown();
        })
        .await
        .expect("test timed out");
    }
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
