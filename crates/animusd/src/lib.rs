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
pub use config::ClusterConfig;
// Re-exported so callers (CLI, tests, operators) can inspect a node's cached
// cluster metadata — membership status and the tablet map — without depending on
// `animus-control` directly.
pub use animus_control::{
    ColumnType, MetaCommand, Metadata, NodeStatus, ReplicationMode, TableSchema,
};

mod admin;
mod cql;
mod dynamo;
mod http;

use animus_control::node::heartbeat_loop;
use animus_control::{PlacementPolicy, ProposeResult, RaftNode};
use animus_cp_data::{RaftKvNode, SplitHook};
use animus_env::{Coresident, Env, Metric, MetricsHandle, NodeId, ProdEnv};
use animus_storage::{LsmEngine, MemoryEngine, SsTableView, WalRecordView};
use animus_tablet::{Epoch, KeyRange, TabletId};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

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
        end: &[u8],
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

    /// This node's live `(key, value)` pairs for the group, in key order, from the
    /// **local** engine (no quorum barrier). Meaningful on the leader (its committed
    /// state); the auto-split loop uses it as a cheap size signal + to pick a median
    /// split key (Phase 2.4). See [`RaftKvNode::range_snapshot`].
    async fn local_pairs(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        match self {
            CpGroup::Lsm(n) => n.range_snapshot(&[]).await,
            CpGroup::Mem(n) => n.range_snapshot(&[]).await,
        }
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

/// The single bootstrap tablet covering the whole keyspace — the CP group's
/// tablet in the replicated metadata (ADR 0017 / ADR 0019).
const TABLET: TabletId = TabletId(1);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(5);
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
/// number of co-resident CP groups a node can host beyond its bootstrap group, i.e.
/// the split depth a node can take part in before the pool is exhausted.
const CP_SIBLING_POOL: usize = 4;

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
    /// Store `value` at `key`. `table` (optional) selects the replication plane:
    /// a table whose replicated schema is `ReplicationMode::Cp` (ADR 0017 #3a)
    /// routes to the leaderful per-tablet Raft group; otherwise (or `None`) the
    /// leaderless AP quorum write. `#[serde(default)]` keeps older clients
    /// (no `table`) byte-compatible.
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        #[serde(default)]
        table: Option<String>,
    },
    /// Read the latest value at `key`. `table` selects the plane as for
    /// [`Put`](ClientRequest::Put): a `Cp` table reads linearizably from the Raft
    /// group leader (ReadIndex), else an AP quorum read.
    Get {
        key: Vec<u8>,
        #[serde(default)]
        table: Option<String>,
    },
    /// Delete `key` from the **CP** plane (a Raft-committed tombstone). The CP
    /// counterpart of [`Put`](ClientRequest::Put) for an explicit delete — used
    /// by the CQL edge's whole-partition delete. Routed to the CP group leader
    /// (forwarded if this node isn't it, like `Put`/`Get`).
    Delete {
        key: Vec<u8>,
        #[serde(default)]
        table: Option<String>,
    },
    /// A **linearizable range scan** over the half-open CP range `[start, end)`,
    /// up to `limit` keys, served from the CP group leader (ReadIndex). The CP
    /// read primitive behind the DynamoDB `Query`/`Scan` and CQL `SELECT` edges;
    /// also the cross-process forwarding payload for a scan (ADR 0017 #3b).
    Scan {
        start: Vec<u8>,
        end: Vec<u8>,
        #[serde(default)]
        limit: Option<usize>,
    },
    /// A CP op **forwarded** from a node that received it but does not host the CP
    /// group leader, to the leader's node (ADR 0017 #3b cross-process routing). The
    /// receiving node serves it locally **iff** it is the leader; it never
    /// re-forwards, so routing is bounded to one hop (a stale hint errors and the
    /// client retries with fresh routing). Carries the original [`Put`]/[`Get`].
    Forwarded(Box<ClientRequest>),
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
}

/// Whether `command` may be **relayed to the control leader** via
/// [`ClientRequest::ProposeSchema`]: the schema-catalog mutations (ADR 0013) that a
/// wire client drives, plus [`MetaCommand::RegisterCpAddr`] (Phase 2.3a) — a node's
/// own CP-address self-registration, relayed to the leader when this node isn't it.
/// Membership / placement / tablet commands are control-plane-internal and are
/// **not** accepted over this path.
fn is_relayable_command(command: &MetaCommand) -> bool {
    matches!(
        command,
        MetaCommand::CreateTableSchema { .. }
            | MetaCommand::DropTableSchema { .. }
            | MetaCommand::CreateTableIndex { .. }
            | MetaCommand::DropTableIndex { .. }
            | MetaCommand::SetTableMode { .. }
            | MetaCommand::CreateKeyspace { .. }
            | MetaCommand::DropKeyspace { .. }
            | MetaCommand::RegisterCpAddr { .. }
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
        self.start_with(
            peers,
            control_ids,
            StorageBackend::default(),
            ClusterEdgeState::new(),
            BTreeMap::new(),
            None,
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
        // A second `raftkv`-env clone for the split hook to mint co-resident sibling
        // inboxes from (Phase 2.2); shares the pool + peer book with the group's env.
        let raftkv_hook_env = self.raftkv_env.clone();
        // A third `raftkv`-env clone for **re-hosting** split tablets from disk at
        // start (#2): each re-host mints its tablet's sibling from the shared pool.
        let raftkv_rehost_env = self.raftkv_env.clone();
        // A fourth `raftkv`-env clone for the **failure-detection heartbeat loop**
        // (#3): each node heartbeats the control group *as its `raftkv` member id*
        // (the cluster members are the `raftkv` ids), so the control plane's
        // `detect_loop` marks a crashed CP node `Down`.
        let raftkv_hb_env = self.raftkv_env.clone();
        // A fifth `raftkv`-env clone for the **join-host loop** (D1): a node added to
        // a tablet's replica set by the placement reconciler hosts an empty group for
        // it and catches up via `InstallSnapshot`. The bootstrap tablet's member id is
        // the node's base `raftkv` id, so it hosts on this (main) env; a split tablet's
        // is derived, so it mints a sibling from the shared pool.
        let raftkv_join_env = self.raftkv_env.clone();
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
            admin: admin_info,
        };
        let n = control_ids.len();
        let cp_group: Vec<NodeId> = (0..n.min(MAX_REPLICATION_FACTOR))
            .map(config::raftkv_id)
            .collect();
        let hosts_cp = cp_group.contains(&my_raftkv_id);
        // The per-node CP-group **hosting claim set**: one `TabletId` per group this
        // node hosts (or is about to), shared across the split hook, the re-host pass
        // (#2), and the join-host loop (D1) so they never double-mint a group. Created
        // unconditionally — the join-host loop runs on every node (a spare that is not
        // in the bootstrap set still hosts a tablet later placed on it).
        let minted: Arc<Mutex<BTreeSet<TabletId>>> = Arc::new(Mutex::new(BTreeSet::new()));
        if hosts_cp {
            // Pre-populate `minted` from the durable marker so the bootstrap group
            // re-applying a committed `Split` on WAL recovery finds the tablet already
            // hosted and does not mint the sibling twice (#4 crash-idempotency), and
            // re-host each previously-split tablet from disk (#2).
            let recorded = load_hosted_cp(&self.raftkv_env).await;
            {
                let mut m = minted.lock().expect("minted set poisoned");
                for h in &recorded {
                    m.insert(TabletId(h.tablet));
                }
            }
            let hosted = Arc::new(Mutex::new(recorded.clone()));
            // Re-host each previously-split tablet from its on-disk engine (#2): the
            // `db-t{id}-` engine + `raftkv.wal` under the sibling dir recover its
            // data + Raft log; the sibling claims a fresh pool listener whose address
            // is re-published for peer-sync. Spawned so a slow control plane (the
            // address publish) does not block node start.
            for h in recorded {
                tokio::spawn(cp_rehost(
                    raftkv_rehost_env.clone(),
                    ctx.clone(),
                    backend,
                    h,
                ));
            }
            let hook = cp_split_hook(
                raftkv_hook_env,
                ctx.clone(),
                cp_group.clone(),
                my_raftkv_id,
                backend,
                minted.clone(),
                hosted,
            );
            let cp = match backend {
                StorageBackend::Lsm => {
                    let lsm = LsmEngine::open(self.raftkv_env.clone(), LSM_PREFIX)
                        .await
                        .map_err(|e| std::io::Error::other(format!("opening CP group LSM: {e}")))?;
                    CpGroup::Lsm(RaftKvNode::start_with_split_hook(
                        self.raftkv_env,
                        cp_group,
                        lsm,
                        hook,
                    ))
                }
                StorageBackend::Memory => CpGroup::Mem(RaftKvNode::start_with_split_hook(
                    self.raftkv_env,
                    cp_group,
                    MemoryEngine::new(),
                    hook,
                )),
            };
            // The statically-placed group serves the bootstrap tablet (the whole
            // keyspace). A tablet split registers the new tablet's group alongside
            // it; routing is keyed by tablet (`cp_route`).
            edge.register_raftkv(TABLET, cp);
        }

        // Bootstrap: whichever node is leader registers membership + the CP tablet
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
        // replicated placement, timing here).
        if hosts_cp {
            tasks.push(tokio::spawn(cp_reconfigure_loop(ctx.clone())));
        }

        // **CP join-host loop** (D1 — closes the failure->placement->reconfigure
        // cascade): a node placed in a tablet's replica set by the reconciler (e.g. a
        // spare picked to replace a `Down` replica) stands up an empty co-resident
        // group for it and catches up via `InstallSnapshot`. Runs on **every** node
        // (a spare is not in the bootstrap CP set, yet must host when later placed).
        tasks.push(tokio::spawn(cp_join_host_loop(
            raftkv_join_env,
            ctx.clone(),
            backend,
            my_raftkv_id,
            minted,
        )));

        // Client request server + DynamoDB HTTP + CQL endpoints share the same
        // context built above (the same raft view, RMW lock, and CP edge state).
        {
            // A CP-hosting node registers its `raftkv` address in the replicated
            // Metadata (Phase 2.3a), so peer-sync on every node can reach it. The
            // bootstrap members' addrs are already in the static peer book, so this
            // is the path a *new* member (split sibling / join) reuses.
            if hosts_cp {
                let ctx = ctx.clone();
                tasks.push(tokio::spawn(async move {
                    ctx.register_cp_addr(my_raftkv_id, my_raftkv_addr.to_string())
                        .await;
                }));
            }
            // Auto-split loop (Phase 2.4), opt-in: a CP-hosting node splits a tablet
            // it leads once it exceeds the key-count threshold.
            if let Some(threshold) = auto_split_threshold {
                if hosts_cp {
                    tasks.push(tokio::spawn(auto_split_loop(ctx.clone(), threshold)));
                }
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
    /// This node's identity + bound addresses for the admin `/admin/config` view
    /// (ADR 0020). `Arc` so cloning the ctx onto each connection is cheap.
    admin: Arc<AdminInfo>,
}

impl ClientCtx {
    /// The id of the tablet whose key range covers `key`, from this node's cached
    /// `Metadata` tablet map (the control plane's placement authority). `None` if no
    /// tablet covers it yet (the cluster is still bootstrapping its first tablet).
    fn tablet_for(&self, key: &[u8]) -> Option<TabletId> {
        self.raft
            .metadata()
            .tablets
            .iter()
            .find(|(_, t)| t.range.contains(key))
            .map(|(id, _)| *id)
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
    async fn cp_route(&self, key: &[u8]) -> CpRoute {
        let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
        loop {
            if let Some(tablet) = self.tablet_for(key) {
                if let Some(leader) = self.edge.cp_leader(tablet) {
                    return CpRoute::Local(leader);
                }
                // Forward only to a concrete leader *hint* a local replica gives us.
                if let Some(addr) = self.cp_forward_target(tablet) {
                    return CpRoute::Forward(addr);
                }
                // No local leader and no leader hint. A node hosting no replica of
                // this tablet can never serve locally, so forward to any known route
                // immediately; a node that *does* host a replica waits for its own
                // election/hint.
                if self.edge.local_cp(tablet).is_none() {
                    if let Some(addr) = self.client_route.values().next().copied() {
                        return CpRoute::Forward(addr);
                    }
                }
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
    pub(crate) async fn cp_read(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, String> {
        match self.cp_route(&key).await {
            CpRoute::Local(leader) => Ok(leader.linearizable_get(&key).await),
            CpRoute::Forward(addr) => {
                match self
                    .cp_forward(addr, ClientRequest::Get { key, table: None })
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
    pub(crate) async fn cp_write(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String> {
        match self.cp_route(&key).await {
            CpRoute::Local(leader) => Self::cp_put_local(&leader, key, value).await,
            CpRoute::Forward(addr) => Self::ok_or_err(
                self.cp_forward(
                    addr,
                    ClientRequest::Put {
                        key,
                        value,
                        table: None,
                    },
                )
                .await,
                "forwarded CP write",
            ),
            CpRoute::None => Err("no CP group leader reachable".into()),
        }
    }

    /// CP **delete** of `key` (ADR 0017): a Raft-committed tombstone, waited to
    /// durable+applied (a linearizable read then reads `None`) before returning.
    /// Forwarded if this node isn't the leader. Used by the CQL whole-partition
    /// delete; the DynamoDB edge instead writes a sentinel tombstone *value* via
    /// [`cp_write`](Self::cp_write).
    pub(crate) async fn cp_delete(&self, key: Vec<u8>) -> Result<(), String> {
        match self.cp_route(&key).await {
            CpRoute::Local(leader) => Self::cp_delete_local(&leader, key).await,
            CpRoute::Forward(addr) => Self::ok_or_err(
                self.cp_forward(addr, ClientRequest::Delete { key, table: None })
                    .await,
                "forwarded CP delete",
            ),
            CpRoute::None => Err("no CP group leader reachable".into()),
        }
    }

    /// Linearizable CP range **scan** over `[start, end)` up to `limit` keys (ADR
    /// 0017): ReadIndex on the group leader, forwarded if this node isn't it. The
    /// CP read primitive behind the DynamoDB `Query`/`Scan` + CQL multi-row reads.
    ///
    /// Routes to the tablet owning `start`. Today the cluster has one whole-keyspace
    /// tablet, so a scan always stays within it; once a tablet split makes a range
    /// span tablets, this fans out across the overlapping tablets and merges (a
    /// Phase 2 follow-on with multi-tablet hosting).
    pub(crate) async fn cp_scan(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        match self.cp_route(&start).await {
            CpRoute::Local(leader) => leader
                .linearizable_scan(&start, &end, limit)
                .await
                .ok_or_else(|| "CP group leader moved; retry".into()),
            CpRoute::Forward(addr) => {
                match self
                    .cp_forward(addr, ClientRequest::Scan { start, end, limit })
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
                loop {
                    if leader.local_get(&key).await.as_deref() == Some(value.as_slice()) {
                        return Ok(());
                    }
                    if tokio::time::Instant::now() >= deadline {
                        return Err("CP write did not commit in time".into());
                    }
                    tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
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
                loop {
                    if leader.local_get(&key).await.is_none() {
                        return Ok(());
                    }
                    if tokio::time::Instant::now() >= deadline {
                        return Err("CP delete did not commit in time".into());
                    }
                    tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
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
    async fn cp_put(&self, key: Vec<u8>, value: Vec<u8>) -> ClientResponse {
        match self.cp_write(key, value).await {
            Ok(()) => ClientResponse::PutOk,
            Err(e) => ClientResponse::Error(e),
        }
    }

    /// Route a CP-mode **read** for the plain client API (returns a wire
    /// [`ClientResponse`]). Thin adapter over [`cp_read`](Self::cp_read).
    async fn cp_get(&self, key: Vec<u8>) -> ClientResponse {
        match self.cp_read(key).await {
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
        let leader_id = self.edge.local_cp(tablet).and_then(|n| n.leader())?;
        self.client_route.get(&leader_id).copied()
    }

    /// Forward a CP op to another node's client API (wrapped so the receiver
    /// serves-or-errors, never re-forwards) and relay its reply.
    async fn cp_forward(&self, addr: SocketAddr, request: ClientRequest) -> ClientResponse {
        self.relay(addr, ClientRequest::Forwarded(Box::new(request)))
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
    pub(crate) async fn propose_schema(&self, command: &MetaCommand) {
        if let Some(leader) = self.edge.leader_handle() {
            let _ = leader.propose(command.clone());
            return;
        }
        if let Some(leader_id) = self.raft.leader() {
            if let Some(&addr) = self.client_route.get(&leader_id) {
                let _ = self
                    .relay(addr, ClientRequest::ProposeSchema(command.clone()))
                    .await;
            }
        }
    }

    /// This node's leader handle for the tablet owning `key`, if it hosts it and
    /// leads — the local serve target for a forwarded op.
    fn cp_leader_for(&self, key: &[u8]) -> Option<CpGroup> {
        self.tablet_for(key).and_then(|t| self.edge.cp_leader(t))
    }

    /// Serve a **forwarded** CP op locally: this node must lead the op's tablet (it
    /// does not re-forward — bounding routing to one hop). The op's key resolves to
    /// its owning tablet, then to that tablet's leader on this node.
    async fn cp_serve_forwarded(&self, inner: ClientRequest) -> ClientResponse {
        match inner {
            ClientRequest::Put { key, value, .. } => {
                let Some(leader) = self.cp_leader_for(&key) else {
                    return ClientResponse::Error("forwarded CP op: not the leader here".into());
                };
                match Self::cp_put_local(&leader, key, value).await {
                    Ok(()) => ClientResponse::PutOk,
                    Err(e) => ClientResponse::Error(e),
                }
            }
            ClientRequest::Get { key, .. } => match self.cp_leader_for(&key) {
                Some(leader) => ClientResponse::Value(leader.linearizable_get(&key).await),
                None => ClientResponse::Error("forwarded CP op: not the leader here".into()),
            },
            ClientRequest::Delete { key, .. } => {
                let Some(leader) = self.cp_leader_for(&key) else {
                    return ClientResponse::Error("forwarded CP op: not the leader here".into());
                };
                match Self::cp_delete_local(&leader, key).await {
                    Ok(()) => ClientResponse::PutOk,
                    Err(e) => ClientResponse::Error(e),
                }
            }
            ClientRequest::Scan { start, end, limit } => {
                let Some(leader) = self.cp_leader_for(&start) else {
                    return ClientResponse::Error("forwarded CP op: not the leader here".into());
                };
                match leader.linearizable_scan(&start, &end, limit).await {
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
    let rf = raftkv_ids.len().min(MAX_REPLICATION_FACTOR);
    let replicas: Vec<NodeId> = raftkv_ids.iter().copied().take(rf).collect();
    loop {
        if raft.is_leader() {
            let meta = raft.metadata();
            if !meta.tablets.contains_key(&TABLET) {
                // Members + tablet replicas are the CP `raftkv` ids — the nodes that
                // actually hold data; the control-group ids are only the Raft
                // consensus group.
                for &node in &raftkv_ids {
                    raft.propose(MetaCommand::UpsertMember {
                        node,
                        labels: BTreeMap::new(),
                        status: NodeStatus::Active,
                    });
                }
                raft.propose(MetaCommand::CreateTablet {
                    tablet: TABLET,
                    range: KeyRange::whole(),
                    replicas: replicas.clone(),
                });
            } else if !meta.policies.contains_key(&TABLET) {
                // The tablet exists but has no policy yet — a separate idempotent step
                // (the create above is async, so the policy lands a tick later). The
                // RF policy lets the reconciler replace a `Down` replica with a spare
                // (D1); it requires no labels, so any Active member is eligible.
                raft.propose(MetaCommand::SetTabletPolicy {
                    tablet: TABLET,
                    policy: Some(PlacementPolicy::simple("cp-rf", rf)),
                });
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

/// Build the **CP split hook** (Phase 2.2) for this node's bootstrap CP group. When
/// the group commits a `Split { at }`, every replica applies it and invokes this
/// hook with the handed-off `[at, ∞)` data; the hook spawns [`cp_split_seed`] to
/// stand up this node's co-resident member of the new tablet's group. The original
/// group separately tombstones `[at, ∞)` (in `animus-cp-data`), so the keyspace
/// partitions cleanly.
///
/// `parent_members` are the splitting group's member ids (the bootstrap group's =
/// the node base `raftkv` ids); `my_id` is this node's member id in it. `hosted` is
/// the shared in-memory mirror of the durable [`CP_HOSTED_FILE`] marker, updated +
/// persisted when the new tablet's group is stood up so a restart re-hosts it (#2).
#[allow(clippy::too_many_arguments)] // split context: env + ctx + ids + backend + state
fn cp_split_hook(
    raftkv_env: ProdEnv,
    ctx: ClientCtx,
    parent_members: Vec<NodeId>,
    my_id: NodeId,
    backend: StorageBackend,
    minted: Arc<Mutex<BTreeSet<TabletId>>>,
    hosted: Arc<Mutex<Vec<HostedCpTablet>>>,
) -> SplitHook {
    Arc::new(move |at, handoff| {
        tokio::spawn(cp_split_seed(
            raftkv_env.clone(),
            ctx.clone(),
            parent_members.clone(),
            my_id,
            backend,
            Arc::clone(&minted),
            Arc::clone(&hosted),
            at,
            handoff,
        ));
    })
}

/// Stand up this node's co-resident member of a split-created tablet's CP group
/// (Phase 2.2), seeded with the handed-off `[at, ∞)` `handoff` data.
///
/// Resolves the new tablet from replicated `Metadata` (the trigger's `SplitTablet`
/// created a tablet whose range starts at `at`), derives the new group's member ids
/// deterministically (`base + new_tablet * CP_SPLIT_ID_STRIDE`, identical on every
/// replica), mints a `Coresident::sibling` inbox for its own member, opens a
/// per-tablet engine, `start_seeded`s the group, registers it for routing, and
/// publishes its address (peer-sync distributes it). **Idempotent** within a
/// process: if this node already hosts the new tablet's group it returns (the hook
/// fires on every apply, incl. WAL re-apply on recovery).
#[allow(clippy::too_many_arguments)] // split context: env + ctx + ids + backend + state + payload
async fn cp_split_seed(
    raftkv_env: ProdEnv,
    ctx: ClientCtx,
    parent_members: Vec<NodeId>,
    my_id: NodeId,
    backend: StorageBackend,
    minted: Arc<Mutex<BTreeSet<TabletId>>>,
    hosted: Arc<Mutex<Vec<HostedCpTablet>>>,
    at: Vec<u8>,
    handoff: Vec<(Vec<u8>, Vec<u8>)>,
) {
    // The new tablet is the one the trigger's `SplitTablet` created with range
    // starting exactly at the split key. Poll briefly for it to replicate here.
    let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
    let new_tablet = loop {
        let found = ctx
            .raft
            .metadata()
            .tablets
            .iter()
            .find(|(_, t)| t.range.start == at)
            .map(|(id, _)| *id);
        if let Some(t) = found {
            break t;
        }
        if tokio::time::Instant::now() >= deadline {
            tracing::warn!("CP split: new tablet for the split key never appeared");
            return;
        }
        tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
    };

    // Idempotent **per node**: mint this node's member of the new tablet only once
    // per process. (The per-node `minted` set, not the edge state — in a
    // `--cluster N` process the edge is *shared* across nodes, so gating on
    // `edge.local_cp(new_tablet)` would let the first node's mint suppress every
    // other node's, leaving the new group without a quorum.)
    {
        let mut minted = minted.lock().expect("minted set poisoned");
        if !minted.insert(new_tablet) {
            return;
        }
    }

    // Deterministic member ids for the new group: every replica derives the same set
    // from the parent members + the new tablet id.
    let new_members: Vec<NodeId> = parent_members
        .iter()
        .map(|m| m + new_tablet.0 * CP_SPLIT_ID_STRIDE)
        .collect();
    let my_new_id = my_id + new_tablet.0 * CP_SPLIT_ID_STRIDE;

    // A co-resident inbox for this node's member of the new group, drawn from the
    // pre-bound listener pool; its address is published for peer-sync.
    let sibling = raftkv_env.sibling(my_new_id);
    let sibling_addr = sibling.local_addr();
    let cp = match backend {
        StorageBackend::Lsm => {
            let prefix = format!("db-t{}-", new_tablet.0);
            match LsmEngine::open(sibling.clone(), &prefix).await {
                Ok(lsm) => CpGroup::Lsm(
                    RaftKvNode::start_seeded(sibling, new_members.clone(), lsm, handoff).await,
                ),
                Err(e) => {
                    tracing::error!(?e, "CP split: opening new tablet LSM");
                    return;
                }
            }
        }
        StorageBackend::Memory => CpGroup::Mem(
            RaftKvNode::start_seeded(sibling, new_members.clone(), MemoryEngine::new(), handoff)
                .await,
        ),
    };
    ctx.edge.register_raftkv(new_tablet, cp);

    // Durably record that this node now hosts the new tablet's group on disk, so a
    // restart re-hosts it from its `db-t{id}-` engine (#2). Persist before the
    // address publish so a crash mid-publish still re-hosts on recovery. Snapshot the
    // list under the lock, then drop the guard before the async persist (never hold a
    // `std::sync::Mutex` guard across `.await`).
    let snapshot = {
        let mut h = hosted.lock().expect("hosted set poisoned");
        if !h.iter().any(|e| e.tablet == new_tablet.0) {
            h.push(HostedCpTablet {
                tablet: new_tablet.0,
                member: my_new_id,
                members: new_members,
            });
        }
        h.clone()
    };
    save_hosted_cp(&raftkv_env, &snapshot).await;

    // Publish this member's address so every node's peer-sync loop can reach it —
    // the new group's internal Raft traffic (election, replication) cannot make
    // progress until every member's address is in the peer books. Via `ctx`, which
    // relays the registration to the control leader cross-process (#4).
    ctx.register_cp_addr(my_new_id, sibling_addr.to_string())
        .await;
}

/// Re-host a split-created CP tablet from its on-disk engine at node start (#2):
/// mint the tablet's `Coresident::sibling` (a fresh pool listener), recover its
/// `db-t{id}-` engine + `raftkv.wal` via `start_seeded` with an **empty** seed (the
/// data is already durable on disk — the seed only ever carries a *fresh* split's
/// handoff), register it for routing, and re-publish the sibling's new address for
/// peer-sync (the pool port is fresh each incarnation). The new group re-forms once
/// a quorum of members have re-published + peer-sync has distributed the addresses.
async fn cp_rehost(
    raftkv_env: ProdEnv,
    ctx: ClientCtx,
    backend: StorageBackend,
    h: HostedCpTablet,
) {
    let sibling = raftkv_env.sibling(h.member);
    let sibling_addr = sibling.local_addr();
    let tablet = TabletId(h.tablet);
    let cp = match backend {
        StorageBackend::Lsm => {
            let prefix = format!("db-t{}-", h.tablet);
            match LsmEngine::open(sibling.clone(), &prefix).await {
                Ok(lsm) => CpGroup::Lsm(
                    RaftKvNode::start_seeded(sibling, h.members, lsm, Vec::new()).await,
                ),
                Err(e) => {
                    tracing::error!(?e, tablet = h.tablet, "CP re-host: opening tablet LSM");
                    return;
                }
            }
        }
        StorageBackend::Memory => CpGroup::Mem(
            RaftKvNode::start_seeded(sibling, h.members, MemoryEngine::new(), Vec::new()).await,
        ),
    };
    ctx.edge.register_raftkv(tablet, cp);
    ctx.register_cp_addr(h.member, sibling_addr.to_string())
        .await;
}

/// How often the CP reconfigure loop pulls the tablet map and steps a group it
/// leads toward its desired voter set (#3). Brisk enough to converge a replica move
/// promptly, but a no-op once a group's config matches the placement, so a steady
/// cluster produces no churn.
const CP_RECONFIGURE_INTERVAL: Duration = Duration::from_millis(500);

/// Translate a tablet's replica set — recorded in `Metadata.tablets[t].replicas` as
/// stable **base** `raftkv` ids (the node identities placement + failure-detection
/// speak) — into that tablet's CP **group member ids**. The bootstrap tablet's group
/// uses the base ids directly; a split-created tablet's group uses the derived
/// `base + tablet * CP_SPLIT_ID_STRIDE` (Phase 2.2 / [`cp_split_seed`]). This is the
/// single source of the base↔member mapping, so the reconfigure loop's `desired`
/// matches the running group's `config()` exactly (no spurious churn) — which is why
/// the replicated map can stay in base ids rather than being reconciled to the
/// derived member ids (#4).
fn cp_members_for(tablet: TabletId, replicas: &[NodeId]) -> BTreeSet<NodeId> {
    replicas
        .iter()
        .map(|&base| {
            if tablet == TABLET {
                base
            } else {
                base + tablet.0 * CP_SPLIT_ID_STRIDE
            }
        })
        .collect()
}

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
/// this node. Same cadence as the reconfigure loop — they converge a replica move
/// together (this node stands the group up; the leader adds it as a voter).
const CP_JOIN_HOST_INTERVAL: Duration = Duration::from_millis(500);

/// The per-node **CP join-host loop** (D1): when the placement reconciler adds this
/// node to a tablet's replica set (e.g. picking it as the spare for a `Down`
/// replica), stand up an **empty** co-resident group for that tablet so the group's
/// leader can add it as a voter and catch it up via `InstallSnapshot`.
///
/// Gated on **`epoch > INITIAL`**: a freshly *split* tablet is at `Epoch::INITIAL`
/// and is seeded with its handed-off data by the split hook on its original replicas
/// — starting it *empty* here would lose that data, so the loop never touches an
/// INITIAL-epoch tablet (the bootstrap tablet is also hosted at start, not here). A
/// reconfigure bumps the epoch past INITIAL, which is exactly the "this node was
/// added as a fresh replica" signal. The shared `hosting` claim set (with the split
/// hook + re-host) prevents double-hosting; `edge.local_cp` skips a tablet already
/// hosted (e.g. the bootstrap tablet on an original replica).
///
/// The new member starts with `all_nodes` = the other members **excluding itself**,
/// so it is a quiet non-voter until the leader's `change_membership` adds it (a node
/// inside its own initial config could campaign — see the `animus-cp-data` membership
/// gotcha). On restart it instead recovers its config from its own WAL.
async fn cp_join_host_loop(
    raftkv_env: ProdEnv,
    ctx: ClientCtx,
    backend: StorageBackend,
    my_raftkv_id: NodeId,
    hosting: Arc<Mutex<BTreeSet<TabletId>>>,
) {
    loop {
        tokio::time::sleep(CP_JOIN_HOST_INTERVAL).await;
        let tablets = ctx.raft.metadata().tablets;
        for (tablet, t) in tablets {
            if t.epoch <= Epoch::INITIAL || !t.replicas.contains(&my_raftkv_id) {
                continue;
            }
            if ctx.edge.local_cp(tablet).is_some() {
                continue; // already hosting (bootstrap tablet, or a prior iteration)
            }
            // Claim once (shared with the split hook / re-host).
            {
                let mut h = hosting.lock().expect("hosting set poisoned");
                if !h.insert(tablet) {
                    continue;
                }
            }
            cp_join_host(
                &raftkv_env,
                &ctx,
                backend,
                my_raftkv_id,
                tablet,
                &t.replicas,
                &hosting,
            )
            .await;
        }
    }
}

/// Stand up this node's empty member of `tablet`'s group (the body of
/// [`cp_join_host_loop`]). The bootstrap tablet's member id is this node's base
/// `raftkv` id, hosted on the **main** `raftkv` env; a split tablet's is derived, so
/// it mints a co-resident **sibling**. On a transient engine-open failure the tablet
/// is un-claimed so a later tick retries.
#[allow(clippy::too_many_arguments)] // join context: env + ctx + backend + ids + replicas + claim
async fn cp_join_host(
    raftkv_env: &ProdEnv,
    ctx: &ClientCtx,
    backend: StorageBackend,
    my_raftkv_id: NodeId,
    tablet: TabletId,
    replicas: &[NodeId],
    hosting: &Arc<Mutex<BTreeSet<TabletId>>>,
) {
    let member = if tablet == TABLET {
        my_raftkv_id
    } else {
        my_raftkv_id + tablet.0 * CP_SPLIT_ID_STRIDE
    };
    // Quiet non-voter: start knowing the *other* members, not itself.
    let others: Vec<NodeId> = cp_members_for(tablet, replicas)
        .into_iter()
        .filter(|&id| id != member)
        .collect();
    // Bootstrap tablet -> the main env (member id == base id); split tablet -> a
    // sibling minted for the derived member id.
    let (env, prefix) = if tablet == TABLET {
        (raftkv_env.clone(), LSM_PREFIX.to_string())
    } else {
        (raftkv_env.sibling(member), format!("db-t{}-", tablet.0))
    };
    let addr = env.local_addr();
    let cp = match backend {
        StorageBackend::Lsm => match LsmEngine::open(env.clone(), &prefix).await {
            Ok(lsm) => CpGroup::Lsm(RaftKvNode::start(env, others, lsm)),
            Err(e) => {
                tracing::error!(?e, tablet = tablet.0, "CP join-host: opening tablet LSM");
                hosting
                    .lock()
                    .expect("hosting set poisoned")
                    .remove(&tablet);
                return;
            }
        },
        StorageBackend::Memory => CpGroup::Mem(RaftKvNode::start(env, others, MemoryEngine::new())),
    };
    ctx.edge.register_raftkv(tablet, cp);
    ctx.register_cp_addr(member, addr.to_string()).await;
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

/// The leader-driven **automatic split trigger** (Phase 2.4): on each tick, for
/// every tablet whose CP group this node currently **leads**, read the leader's
/// local key count (a cheap size signal) and, if it exceeds `threshold`, propose a
/// split at the **median** key — bisecting the tablet so each half holds roughly
/// half the keys. Per-tablet cooldown avoids a duplicate trigger while a split is
/// in flight; once it applies, the parent's key count halves below the threshold.
///
/// Only the node hosting a tablet's leader reads `local_pairs`/triggers, so in a
/// one-process-per-node deployment exactly one node triggers. (In a single
/// `--cluster N` process the edge state is shared, so every node's loop sees the
/// same leader handle and may trigger redundantly — harmless: the control plane
/// rejects a re-split of an already-split range, and the per-node mint gate dedups
/// the hook, so it converges to one split.)
///
/// `threshold` is a **key count** here — a placeholder size signal; a real
/// byte/size-based threshold is future tuning. Disabled unless a threshold is wired
/// (so it never perturbs clusters that don't opt in).
async fn auto_split_loop(ctx: ClientCtx, threshold: usize) {
    let mut last_triggered: BTreeMap<TabletId, tokio::time::Instant> = BTreeMap::new();
    loop {
        tokio::time::sleep(AUTO_SPLIT_INTERVAL).await;
        let tablets: Vec<TabletId> = ctx.raft.metadata().tablets.keys().copied().collect();
        for tablet in tablets {
            if let Some(at) = last_triggered.get(&tablet) {
                if at.elapsed() < AUTO_SPLIT_COOLDOWN {
                    continue;
                }
            }
            // Only the leader's host reads + triggers (else this node doesn't have
            // the leader handle).
            let Some(leader) = ctx.edge.cp_leader(tablet) else {
                continue;
            };
            let pairs = leader.local_pairs().await;
            if pairs.len() <= threshold {
                continue;
            }
            // Median key bisects the tablet; it is strictly inside the range (an
            // interior key of > threshold >= 2 distinct keys), so `SplitTablet`
            // accepts it.
            let median = pairs[pairs.len() / 2].0.clone();
            last_triggered.insert(tablet, tokio::time::Instant::now());
            let _ = ctx.trigger_split(tablet, median).await;
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
        let response = match request {
            ClientRequest::Status => ClientResponse::Status(ctx.raft.metadata()),
            // v1 (ADR 0019): all data ops route to the leaderful CP per-tablet Raft
            // group (ADR 0017 #3a). The optional `table` no longer selects a plane
            // (there is only the CP plane); the single CP group covers the keyspace.
            ClientRequest::Put { key, value, .. } => ctx.cp_put(key, value).await,
            ClientRequest::Get { key, .. } => ctx.cp_get(key).await,
            ClientRequest::Scan { start, end, limit } => match ctx.cp_scan(start, end, limit).await
            {
                Ok(pairs) => ClientResponse::Pairs(pairs),
                Err(e) => ClientResponse::Error(e),
            },
            ClientRequest::Delete { key, .. } => match ctx.cp_delete(key).await {
                Ok(()) => ClientResponse::PutOk,
                Err(e) => ClientResponse::Error(e),
            },
            // Admin: split a CP tablet (Phase 2.2).
            ClientRequest::SplitTablet { tablet, split_key } => {
                ctx.trigger_split(TabletId(tablet), split_key).await
            }
            // A CP op forwarded from another node (cross-process routing, ADR 0017
            // #3b): serve locally iff we are the leader; never re-forward.
            ClientRequest::Forwarded(inner) => ctx.cp_serve_forwarded(*inner).await,
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
        };
        write_frame(&mut stream, &response).await?;
    }
    Ok(())
}

/// How long the CQL/DynamoDB edges wait for a proposed schema `MetaCommand`
/// (`CreateTableSchema`/`DropTableSchema`) to commit through the control plane
/// before giving up. Generous: a fresh cluster may still be electing a leader.
const SCHEMA_COMMIT_TIMEOUT: Duration = Duration::from_secs(10);
/// Poll interval while waiting for a proposed schema command to commit / for a
/// leader to settle so the proposal can be (re)submitted.
const SCHEMA_POLL_INTERVAL: Duration = Duration::from_millis(50);

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

    /// Whether any replicated table name starts with `prefix`. Used by the CQL
    /// edge to recognize a keyspace (keyed `ks.table`) as existing because it has
    /// at least one table, even across a restart (keyspaces are not separately
    /// replicated — ADR 0013).
    pub(crate) fn has_table_schema_with_prefix(&self, prefix: &str) -> bool {
        self.raft
            .metadata()
            .table_schemas()
            .any(|(name, _)| name.starts_with(prefix))
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
                MetaCommand::RegisterCpAddr { id, addr },
                SCHEMA_COMMIT_TIMEOUT,
                || (self.raft.metadata().cp_member_addrs.get(&id) == Some(&want)).then_some(()),
            )
            .await;
    }

    /// Split CP `tablet` at `split_key` (Phase 2.2): record the split in the control
    /// plane (a new tablet id covering `[split_key, ∞)`), then trigger the
    /// data-plane split on the tablet's CP group leader — on commit each replica's
    /// split hook mints the new tablet's co-resident group. Returns once the
    /// data-plane split is *accepted* (the new group forms + becomes routable
    /// asynchronously; the caller polls a read of an upper-range key to observe it).
    async fn trigger_split(&self, tablet: TabletId, split_key: Vec<u8>) -> ClientResponse {
        // The new tablet id: one past the current max (deterministic on the leader
        // that proposes it; replicated to all via `SplitTablet`).
        let new_id = TabletId(
            self.raft
                .metadata()
                .tablets
                .keys()
                .map(|t| t.0)
                .max()
                .unwrap_or(0)
                + 1,
        );
        // 1. Record the split in the control plane and wait until the new tablet is
        //    visible here, so the split hook can resolve `new_id` from `Metadata`
        //    when the data-plane `Split` applies. Routed to the control leader.
        let cmd = MetaCommand::SplitTablet {
            tablet,
            split_key: split_key.clone(),
            new_id,
        };
        if self
            .propose_and_await(cmd, SCHEMA_COMMIT_TIMEOUT, || {
                self.raft
                    .metadata()
                    .tablets
                    .contains_key(&new_id)
                    .then_some(())
            })
            .await
            .is_err()
        {
            return ClientResponse::Error("split metadata did not commit in time".into());
        }
        // 2. Trigger the data-plane split on the tablet's CP group leader (it fires
        //    every replica's split hook on commit). Forwarding a split to a remote
        //    leader is later work; in-process the shared edge reaches the leader.
        match self.edge.cp_leader(tablet) {
            Some(leader) => match leader.propose_split(split_key) {
                ProposeResult::Accepted { .. } => ClientResponse::PutOk,
                ProposeResult::NotLeader { .. } => {
                    ClientResponse::Error("CP group leader moved; retry the split".into())
                }
            },
            None => ClientResponse::Error("no CP group leader for the tablet here".into()),
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

    /// Propose `MetaCommand::DropTableSchema` and wait for the table to disappear
    /// from the replicated catalog (ADR 0013). Idempotent: dropping an absent
    /// table returns `Ok(())` immediately. Routes to the leader exactly as
    /// [`create_table_schema`](Self::create_table_schema).
    pub(crate) async fn drop_table_schema(&self, table: String) -> Result<(), String> {
        if !self.has_table_schema(&table) {
            return Ok(());
        }
        let command = MetaCommand::DropTableSchema {
            table: table.clone(),
        };
        let deadline = tokio::time::Instant::now() + SCHEMA_COMMIT_TIMEOUT;
        loop {
            if !self.has_table_schema(&table) {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "DROP TABLE `{table}` did not commit within {}s (no control-plane leader reachable?)",
                    SCHEMA_COMMIT_TIMEOUT.as_secs()
                ));
            }
            self.propose_schema(&command).await;
            tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// Propose `command` on the current leader and poll `committed` until it
    /// reports the change visible in this node's replicated metadata (or time
    /// out). Re-proposes each tick so a leader change does not strand it.
    /// Returns the committed value `committed` observed, or `Err(())` on timeout.
    async fn propose_and_await<T>(
        &self,
        command: MetaCommand,
        timeout: Duration,
        committed: impl Fn() -> Option<T>,
    ) -> Result<T, ()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(value) = committed() {
                return Ok(value);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(());
            }
            self.propose_schema(&command).await;
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
    bound
        .start_with(
            config.peer_book(),
            config.control_ids(),
            backend,
            ClusterEdgeState::new(),
            client_route,
            None,
        )
        .await
}

/// Write a length-prefixed (`u32` big-endian) JSON frame.
///
/// # Errors
/// Propagates write failures.
pub async fn write_frame<T: Serialize>(stream: &mut TcpStream, msg: &T) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(msg).expect("client message serializes");
    stream.write_u32(bytes.len() as u32).await?;
    stream.write_all(&bytes).await?;
    stream.flush().await?;
    Ok(())
}

/// Read a length-prefixed JSON frame, or `None` at clean EOF.
///
/// # Errors
/// Propagates read failures and decode errors.
pub async fn read_frame<T: DeserializeOwned>(stream: &mut TcpStream) -> std::io::Result<Option<T>> {
    let len = match stream.read_u32().await {
        Ok(len) => len as usize,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    let msg = serde_json::from_slice(&buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(Some(msg))
}
