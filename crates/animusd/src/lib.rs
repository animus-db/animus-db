//! Node assembly: wires the control plane (`RaftNode`), the data plane
//! (`serve_replica` + a `DataClient` coordinator), and a client-facing request
//! server into a runnable AnimusDB node over `ProdEnv`.
//!
//! ## Roles and the single-consumer rule
//!
//! A node's [`Network`] inbox is single-consumer, so each protocol that does its
//! own `recv` gets a **distinct node id and `ProdEnv`** (a distinct listener):
//!
//! - **control** — the Raft `RaftNode`,
//! - **data** — the AP storage replica (`serve_replica`),
//! - **coord** — the quorum coordinator (`DataClient`) used to serve clients,
//! - **raftkv** — the leaderful **CP** per-tablet Raft group (`RaftKvNode`,
//!   ADR 0017 #3a), hosting CP-mode tables' tablets.
//!
//! The **client API is a plain request/reply TCP server** (length-prefixed
//! JSON), *not* part of the `Network` abstraction. Coordination is therefore
//! server-side: the coordinator is a static cluster member with a known address,
//! so replica replies route correctly and dynamic client addresses never touch
//! the internal network. (Internal cluster topology is static; only the
//! client channel is dynamic.)
//!
//! Construction is two-phase so a whole cluster can bind to ephemeral ports
//! first and then exchange addresses: [`Node::bind`] → assemble the peer book →
//! [`BoundNode::start`]. [`bind_cluster`] / [`start_cluster`] do this for an
//! in-process cluster (used by the binary's `--cluster` mode and the tests).

use std::collections::BTreeMap;
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

mod cql;
mod dynamo;

use animus_control::node::heartbeat_loop;
use animus_control::{PlacementPolicy, ProposeResult, RaftNode};
use animus_data::{
    DataClient, HintStore, ReadResult, TabletView, serve_anti_entropy, serve_hint_replay,
    serve_replica,
};
use animus_env::{Env, EnvExt, Metric, MetricsHandle, NodeId, ProdEnv};
use animus_raftdata::RaftKvNode;
use animus_storage::{LsmEngine, MemoryEngine, StorageEngine};
use animus_tablet::{Epoch, KeyRange, TabletId};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A hosted leaderful CP per-tablet Raft group on this node (ADR 0017 #3a): a
/// `RaftKvNode` over the production env + durable LSM. Aliased to keep the
/// edge-state container readable.
type CpGroup = RaftKvNode<ProdEnv, LsmEngine<ProdEnv>>;

/// The single bootstrap tablet covering the whole keyspace.
const TABLET: TabletId = TabletId(1);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(5);
/// The bootstrap tablet's replication factor (ADR 0005). Capped so a cluster
/// larger than this keeps **spare** data nodes the leader can re-place a failed
/// replica onto — that spare is what makes failure detection cascade into
/// observable self-healing. A cluster of `<= MAX_REPLICATION_FACTOR` nodes simply
/// places on all of them (no spare), so failure detection still marks the dead
/// member `Down` but there is nowhere to move its tablet to.
const MAX_REPLICATION_FACTOR: usize = 3;
/// How often each data replica runs a background anti-entropy round to converge
/// with its peers (ADR 0010). A slow background activity, off any request path.
const ANTI_ENTROPY_INTERVAL: Duration = Duration::from_secs(1);
/// How often the coordinator replays buffered **hints** to the replicas they were
/// recorded for (hinted handoff, ADR 0010). Tighter than anti-entropy: hinting is
/// the *prompt* convergence path for a replica that was briefly unavailable for a
/// write, with anti-entropy the slower full-coverage backstop.
const HINT_REPLAY_INTERVAL: Duration = Duration::from_millis(500);
/// Filename prefix namespacing the data replica's on-disk LSM under the node's
/// data `ProdEnv` directory (its files become `db-MANIFEST`/`db-wal`/`db-sst-*`).
///
/// The prefix is a flat filename prefix, **not** a subdirectory (no `/`):
/// `ProdEnv`'s disk opens files directly under the role's data dir and does not
/// create intermediate directories, so a slash-bearing prefix would fail to
/// create the engine's files. The data role's dir is dedicated to this replica,
/// so a flat prefix already isolates it.
const LSM_PREFIX: &str = "db-";

/// Which storage engine backs a node's data replica.
///
/// The default, [`StorageBackend::Lsm`], is the durable on-disk
/// [`LsmEngine`] over the node's data `ProdEnv` — data survives a process
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
    /// The operation could not be served (no quorum, no tablet, etc.).
    Error(String),
}

/// Listen addresses for a node's seven endpoints (use port 0 for ephemeral):
/// the control/data/coord/raftkv internal `ProdEnv` roles + the client API + the
/// DynamoDB HTTP and CQL endpoints.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct RoleAddrs {
    pub control: SocketAddr,
    pub data: SocketAddr,
    pub coord: SocketAddr,
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
    /// address (ADR 0017 #3a) — distinct from the AP `data` role's, since the
    /// inbox is single-consumer. Defaults (when absent in older configs) to an
    /// ephemeral loopback port.
    #[serde(default = "default_ephemeral_addr")]
    pub raftkv: SocketAddr,
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
    data_id: NodeId,
    coord_id: NodeId,
    raftkv_id: NodeId,
    control_env: ProdEnv,
    data_env: ProdEnv,
    coord_env: ProdEnv,
    raftkv_env: ProdEnv,
    control_addr: SocketAddr,
    data_addr: SocketAddr,
    coord_addr: SocketAddr,
    raftkv_addr: SocketAddr,
    client_listener: TcpListener,
    client_addr: SocketAddr,
    dynamo_listener: TcpListener,
    dynamo_addr: SocketAddr,
    cql_listener: TcpListener,
    cql_addr: SocketAddr,
}

impl BoundNode {
    /// `(control_id, addr)`, `(data_id, addr)`, `(coord_id, addr)`, `(raftkv_id,
    /// addr)` — the entries this node contributes to the cluster peer book.
    pub fn peer_entries(&self) -> [(NodeId, SocketAddr); 4] {
        [
            (self.control_id, self.control_addr),
            (self.data_id, self.data_addr),
            (self.coord_id, self.coord_addr),
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

    /// Wire the peer address book into every env and start all protocols, with
    /// the data replica backed by the durable on-disk [`LsmEngine`]
    /// ([`StorageBackend::Lsm`]). `control_ids`/`data_ids` are the full control
    /// group and the tablet's replica set; `r`/`w` are the quorum sizes.
    ///
    /// # Errors
    /// Propagates a failure to open the data replica's on-disk engine.
    pub async fn start(
        self,
        peers: BTreeMap<NodeId, SocketAddr>,
        control_ids: Vec<NodeId>,
        data_ids: Vec<NodeId>,
        r: usize,
        w: usize,
    ) -> std::io::Result<Node> {
        self.start_with(
            peers,
            control_ids,
            data_ids,
            r,
            w,
            StorageBackend::default(),
            ClusterEdgeState::new(),
        )
        .await
    }

    /// Like [`start`](Self::start), but selects the data replica's storage
    /// engine. [`StorageBackend::Lsm`] is durable (survives restart);
    /// [`StorageBackend::Memory`] is volatile (ephemeral runs).
    ///
    /// # Errors
    /// Propagates a failure to open the data replica's on-disk engine (LSM
    /// backend only).
    #[allow(clippy::too_many_arguments)] // cluster assembly: ids + quorum + backend + edge state
    pub async fn start_with(
        self,
        peers: BTreeMap<NodeId, SocketAddr>,
        control_ids: Vec<NodeId>,
        data_ids: Vec<NodeId>,
        r: usize,
        w: usize,
        backend: StorageBackend,
        edge: ClusterEdgeState,
    ) -> std::io::Result<Node> {
        self.control_env.set_peers(peers.clone());
        self.data_env.set_peers(peers.clone());
        self.coord_env.set_peers(peers.clone());
        self.raftkv_env.set_peers(peers);

        // Keep clones of the four internal envs so [`Node::shutdown`] can abort
        // every task they own (Raft drivers, replica serve loop, accept loops),
        // freeing their listener ports for a restart.
        let envs = [
            self.control_env.clone(),
            self.data_env.clone(),
            self.coord_env.clone(),
            self.raftkv_env.clone(),
        ];

        // Capture the data- and coord-role metrics sinks before their envs are
        // consumed below. The control-plane sink is reached at request time via
        // `raft.metrics()` (`RaftNode::start` records into `control_env.metrics()`);
        // the data replica and the coordinator record into their own role envs'
        // sinks. The `/metrics` endpoint aggregates all three (ADR 0015).
        let data_metrics = self.data_env.metrics();
        let coord_metrics = self.coord_env.metrics();

        let raft = RaftNode::start(self.control_env, control_ids.clone());
        // Register this node's control handle in the **per-cluster** set the wire
        // edges use to reach the control-plane leader for schema proposals
        // (ADR 0013). In `--cluster N` mode this lets any node's CQL/DynamoDB edge
        // propose a `CreateTableSchema` on whichever in-process node is currently
        // leader, so DDL on a follower-connected client still commits. The set is
        // owned by the `ClusterEdgeState` (one per cluster), not a process global,
        // so two in-process clusters in one test do not share handles.
        edge.register_control(raft.clone());

        // **Leaderful CP per-tablet Raft group** (ADR 0017 #3a). Stage 3a hosts a
        // single, statically-placed CP group spanning the first
        // `min(N, MAX_REPLICATION_FACTOR)` nodes' `raftkv` ids (the same RF cap as
        // the bootstrap AP tablet). A node in that set runs a `RaftKvNode` on its
        // `raftkv_env` (own id/port/dir — the single-consumer inbox rule), backed
        // by its own durable `LsmEngine`; the handle is registered in the
        // per-cluster edge state so a CP-mode table's reads/writes route to the
        // group leader. Dynamic placement / split / reconfigure of CP groups over
        // `ProdEnv` and address distribution are Stage 3b. CP client routing works
        // within a `--cluster N` process (shared edge state); cross-process routing
        // is 3b.
        let n = control_ids.len();
        let cp_group: Vec<NodeId> = (0..n.min(MAX_REPLICATION_FACTOR))
            .map(config::raftkv_id)
            .collect();
        if cp_group.contains(&self.raftkv_id) {
            let cp_lsm = LsmEngine::open(self.raftkv_env.clone(), LSM_PREFIX)
                .await
                .map_err(|e| std::io::Error::other(format!("opening CP group LSM: {e}")))?;
            let cp = RaftKvNode::start(self.raftkv_env, cp_group, cp_lsm);
            edge.register_raftkv(cp);
        }

        // The data replica's durable store, plus the autonomous data-plane loops.
        // The on-disk LSM does its disk I/O through a *clone* of the data env's
        // handle (the env is node-scoped, so its files live under this node's data
        // dir, namespaced by `LSM_PREFIX`); the replica's serve loop keeps the
        // original handle for network `recv`. The LSM only touches the disk, so
        // the single-consumer inbox is unaffected.
        //
        // Alongside the replica we spawn the two background data-plane loops, both
        // *send-only* on the data env (so they never contend with the replica's
        // single-consumer inbox):
        //  - the **liveness heartbeat** to the control group, so the control
        //    plane's failure detector (ADR 0012) sees this node's data member
        //    alive — and its silence when the node dies; and
        //  - **anti-entropy** with peer data replicas (ADR 0010), so a replica
        //    that missed writes (e.g. a freshly re-placed spare) converges in the
        //    background.
        let anti_entropy_peers: Vec<NodeId> = data_ids
            .iter()
            .copied()
            .filter(|&d| d != self.data_id)
            .collect();
        let replica: Box<dyn std::any::Any + Send + Sync> = match backend {
            StorageBackend::Lsm => {
                let lsm = LsmEngine::open(self.data_env.clone(), LSM_PREFIX)
                    .await
                    .map_err(|e| std::io::Error::other(format!("opening data replica LSM: {e}")))?;
                start_replica(self.data_env, lsm, control_ids.clone(), anti_entropy_peers)
            }
            StorageBackend::Memory => start_replica(
                self.data_env,
                MemoryEngine::new(),
                control_ids.clone(),
                anti_entropy_peers,
            ),
        };
        // Hinted handoff (ADR 0010): the coordinator buffers a hint for any tablet
        // replica that did not ack a committed write/delete (it was down /
        // partitioned), and a send-only replay loop on a *clone* of the coord env
        // replays the buffered hints to those replicas on a timer — so a replica
        // that was briefly unavailable converges promptly when it returns, not only
        // on the next read or anti-entropy round. The loop is **send-only** (it
        // does not `recv`), so it shares the coord env with the `DataClient` without
        // violating the single-consumer rule (`serve_hint_replay`, not the
        // probe-based `serve_hint_handoff`, for exactly that reason). `animusd` has
        // no residency labels yet, so the residency bound is `None` (no boundary);
        // when residency is configured, derive an `allowed` set from
        // `PlacementPolicy::admits` exactly as the repair guard does (ADR 0005).
        let hints = HintStore::new();
        serve_hint_replay(
            self.coord_env.clone(),
            hints.clone(),
            None,
            HINT_REPLAY_INTERVAL,
        );
        let coordinator = DataClient::with_hints(self.coord_env, hints, None);

        // Bootstrap: whichever node is leader registers membership + the tablet
        // (idempotent). Track the client-facing task handles so `shutdown` can
        // abort them and release the client/dynamo/cql listener ports (these run
        // on plain `tokio::spawn`, off the `Env` network).
        let mut tasks = Vec::with_capacity(4);
        {
            let raft = raft.clone();
            let data_ids = data_ids.clone();
            tasks.push(tokio::spawn(bootstrap(raft, data_ids)));
        }

        // Client request server + DynamoDB HTTP + CQL endpoints share one
        // context (the same coordinator, raft view, and serialization lock).
        {
            let ctx = ClientCtx {
                raft: raft.clone(),
                coordinator: coordinator.clone(),
                coord_lock: Arc::new(tokio::sync::Mutex::new(())),
                r,
                w,
                edge: edge.clone(),
                data_metrics,
                coord_metrics,
            };
            tasks.push(tokio::spawn(serve_clients(
                self.client_listener,
                ctx.clone(),
            )));
            tasks.push(tokio::spawn(dynamo::serve(
                self.dynamo_listener,
                ctx.clone(),
            )));
            tasks.push(tokio::spawn(cql::serve(self.cql_listener, ctx)));
        }

        Ok(Node {
            raft,
            _replica: replica,
            envs,
            tasks,
            client_addr: self.client_addr,
            dynamo_addr: self.dynamo_addr,
            cql_addr: self.cql_addr,
        })
    }
}

/// A running node. Holds the handles that keep its envs and tasks alive.
pub struct Node {
    raft: RaftNode<ProdEnv>,
    /// The data replica handle, type-erased because the backing engine
    /// (`LsmEngine` or `MemoryEngine`) is chosen at runtime. Kept alive so the
    /// replica's serve loop keeps running for the life of the node.
    _replica: Box<dyn std::any::Any + Send + Sync>,
    /// The node's four internal `ProdEnv` roles (control/data/coord/raftkv), kept
    /// so [`shutdown`](Node::shutdown) can abort every task they own and free their
    /// listener ports.
    envs: [ProdEnv; 4],
    /// The client-facing listener tasks (client TCP / dynamo HTTP / cql), which
    /// run on plain `tokio::spawn` off the `Env` network; aborted on shutdown.
    tasks: Vec<tokio::task::JoinHandle<()>>,
    client_addr: SocketAddr,
    dynamo_addr: SocketAddr,
    cql_addr: SocketAddr,
}

impl Node {
    /// Bind this node's listeners (control/data/coord internal envs + the client
    /// TCP server + the DynamoDB HTTP and CQL endpoints) and create its data
    /// directory.
    ///
    /// # Errors
    /// Propagates any bind / directory-creation failure.
    pub async fn bind(
        control_id: NodeId,
        data_id: NodeId,
        coord_id: NodeId,
        raftkv_id: NodeId,
        addrs: RoleAddrs,
        data_dir: impl Into<PathBuf>,
    ) -> std::io::Result<BoundNode> {
        let dir = data_dir.into();
        let (control_env, control_addr) =
            ProdEnv::bind(control_id, addrs.control, dir.join("control")).await?;
        let (data_env, data_addr) = ProdEnv::bind(data_id, addrs.data, dir.join("data")).await?;
        let (coord_env, coord_addr) =
            ProdEnv::bind(coord_id, addrs.coord, dir.join("coord")).await?;
        // The leaderful CP per-tablet Raft role's internal env (ADR 0017 #3a),
        // distinct id/port/dir from the AP data role (single-consumer inbox).
        let (raftkv_env, raftkv_addr) =
            ProdEnv::bind(raftkv_id, addrs.raftkv, dir.join("raftkv")).await?;
        let client_listener = TcpListener::bind(addrs.client).await?;
        let client_addr = client_listener.local_addr()?;
        let dynamo_listener = TcpListener::bind(addrs.dynamo).await?;
        let dynamo_addr = dynamo_listener.local_addr()?;
        let cql_listener = TcpListener::bind(addrs.cql).await?;
        let cql_addr = cql_listener.local_addr()?;
        Ok(BoundNode {
            control_id,
            data_id,
            coord_id,
            raftkv_id,
            control_env,
            data_env,
            coord_env,
            raftkv_env,
            control_addr,
            data_addr,
            coord_addr,
            raftkv_addr,
            client_listener,
            client_addr,
            dynamo_listener,
            dynamo_addr,
            cql_listener,
            cql_addr,
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
    /// dynamo / cql) and every task its three internal `ProdEnv` roles own (the
    /// Raft driver, the replica serve loop, and the internal accept loops). This
    /// releases all six listener ports so a replacement node can rebind the same
    /// addresses on the same data directory — the clean teardown a stopped OS
    /// process would otherwise provide. Idempotent.
    ///
    /// On-disk state is unaffected: a value already acked to a client was synced
    /// to the data replica's LSM WAL before the ack, so it survives the restart.
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
    /// so a wire edge can route a CP-mode table's reads/writes to the group
    /// **leader**. In `--cluster N` mode every hosting node registers here, so the
    /// leader is always present; one-process-per-node registers only the local
    /// handle (cross-process CP routing is Stage 3b). Stage 3a hosts a single CP
    /// group, so this holds at most one handle per node.
    raftkv: Arc<Mutex<Vec<CpGroup>>>,
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
            raftkv: Arc::new(Mutex::new(Vec::new())),
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

    /// Register a node's CP per-tablet Raft group handle (ADR 0017 #3a). Called in
    /// [`BoundNode::start_with`] on each node that hosts the CP group.
    fn register_raftkv(&self, cp: CpGroup) {
        self.raftkv
            .lock()
            .expect("raftkv handles poisoned")
            .push(cp);
    }

    /// The CP group handle that currently believes it is leader, if any (ADR 0017
    /// #3a). The route target for a CP-mode table's reads/writes. Normally exactly
    /// one registered handle leads; a deposed leader's `linearizable_get` returns
    /// `None` (never stale) and its `put` returns `NotLeader`, so picking the first
    /// self-styled leader is safe.
    fn cp_leader(&self) -> Option<CpGroup> {
        self.raftkv
            .lock()
            .expect("raftkv handles poisoned")
            .iter()
            .find(|n| n.is_leader())
            .cloned()
    }

    /// Propose `command` on **every** registered control handle that currently
    /// believes it is leader. Normally exactly one live node is leader; proposing
    /// on all self-styled leaders is robust to a stale handle. A non-leader
    /// `propose` is dropped (`NotLeader`), so this is safe to call every tick.
    fn propose_on_leaders(&self, command: &MetaCommand) {
        for raft in self
            .control
            .lock()
            .expect("control handles poisoned")
            .iter()
        {
            if raft.is_leader() {
                let _ = raft.propose(command.clone());
            }
        }
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

/// Shared context for the client request server and the DynamoDB HTTP endpoint:
/// the cached metadata view for routing, the quorum coordinator, the per-node
/// serialization lock around the single-consumer coord inbox, and the
/// per-cluster wire-edge state.
#[derive(Clone)]
pub(crate) struct ClientCtx {
    raft: RaftNode<ProdEnv>,
    pub(crate) coordinator: DataClient<ProdEnv>,
    pub(crate) coord_lock: Arc<tokio::sync::Mutex<()>>,
    r: usize,
    w: usize,
    pub(crate) edge: ClusterEdgeState,
    /// The data-role env's recording metrics sink (the replica + data-plane
    /// loops record here). Aggregated into the `/metrics` export (ADR 0015).
    data_metrics: MetricsHandle,
    /// The coord-role env's recording metrics sink (the `DataClient` coordinator
    /// records here). Aggregated into the `/metrics` export (ADR 0015).
    coord_metrics: MetricsHandle,
}

impl ClientCtx {
    /// Whether `table`'s replicated schema selects the **CP** (leaderful) plane
    /// (ADR 0017 #3a). Read from this node's own replicated `Metadata`.
    fn is_cp(&self, table: &str) -> bool {
        self.raft.metadata().table_mode(table) == ReplicationMode::Cp
    }

    /// Route a CP-mode **write** to the per-tablet Raft group leader (ADR 0017 #3a):
    /// propose on the leader, then wait until the value is committed + applied +
    /// durable — a linearizable read reflects it — before acking. Durable-before-ack,
    /// matching the AP path's quorum-durability contract. (Stage 3a confirms commit
    /// by reading the value back; a dedicated applied-index await is a 3b refinement,
    /// as is forwarding when this process holds no group leader.)
    async fn cp_put(&self, key: Vec<u8>, value: Vec<u8>) -> ClientResponse {
        let Some(leader) = self.edge.cp_leader() else {
            return ClientResponse::Error("no CP group leader available".into());
        };
        match leader.put(key.clone(), value.clone()) {
            ProposeResult::Accepted { .. } => {
                let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
                loop {
                    if leader.linearizable_get(&key).await.as_deref() == Some(value.as_slice()) {
                        return ClientResponse::PutOk;
                    }
                    if tokio::time::Instant::now() >= deadline {
                        return ClientResponse::Error("CP write did not commit in time".into());
                    }
                    tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
                }
            }
            ProposeResult::NotLeader { .. } => {
                ClientResponse::Error("CP group leader moved; retry".into())
            }
        }
    }

    /// Route a CP-mode **read** to the per-tablet Raft group leader: a linearizable
    /// ReadIndex read (no stale value — a deposed leader returns `None`).
    async fn cp_get(&self, key: Vec<u8>) -> ClientResponse {
        let Some(leader) = self.edge.cp_leader() else {
            return ClientResponse::Error("no CP group leader available".into());
        };
        ClientResponse::Value(leader.linearizable_get(&key).await)
    }

    /// Render this node's **live** metrics as the ADR 0015 text export
    /// (`name value` lines), aggregated across the node's three role sinks.
    ///
    /// A node runs three internal `ProdEnv` roles on distinct ids — control
    /// (Raft), data (replica), coord (`DataClient`) — and each records into its
    /// **own** sink (`RaftNode::start` records into the control env's sink; the
    /// replica and coordinator into theirs). To surface both control- and
    /// data-plane counters from one endpoint, this sums the three snapshots
    /// counter-by-counter and takes the max of the leadership gauge (leadership is
    /// the control plane's, recorded only in the control sink). The snapshots are
    /// read **at call time**, so the export reflects current activity rather than a
    /// cached value. Today only the control sink moves; the data/coord sinks are
    /// included so data-plane counters surface automatically once recorded, with no
    /// further endpoint change.
    pub(crate) fn metrics_text(&self) -> String {
        let snaps = [
            self.raft.metrics().snapshot(),
            self.data_metrics.snapshot(),
            self.coord_metrics.snapshot(),
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
}

/// Spawn the data replica over `storage` plus its two background loops
/// (liveness heartbeat to `control_ids`, anti-entropy with `peers`) on `env`, and
/// return the type-erased replica handle that keeps the serve loop alive for the
/// life of the node.
///
/// All three share the node's data `env`. The replica's serve loop is the inbox's
/// single consumer; the heartbeat and anti-entropy loops are **send-only** on a
/// clone of the same env (anti-entropy's `SyncPull` replies arrive back through
/// the replica's inbox), so they do not contend on the single-consumer rule.
/// Anti-entropy takes the replica `handle` and reads the tablet's **live** epoch
/// from it each round (advanced by the control plane via `set_epoch` on a
/// reconcile), so a re-placed spare converges in the background after a topology
/// change instead of waiting for the first read's read-repair (ADR 0010/0002).
fn start_replica<S>(
    env: ProdEnv,
    storage: S,
    control_ids: Vec<NodeId>,
    peers: Vec<NodeId>,
) -> Box<dyn std::any::Any + Send + Sync>
where
    S: StorageEngine + 'static,
{
    let handle = serve_replica(env.clone(), storage, Epoch::INITIAL);
    // This node's data member heartbeats the control group so the leader's
    // failure detector (ADR 0012) tracks it — and notices its silence on death.
    env.clone()
        .spawn_task(heartbeat_loop(env.clone(), control_ids));
    // Background convergence among the data replicas (ADR 0010). The loop reads
    // the replica's *live* known epoch for the tablet each round from `handle`,
    // so after a placement reconcile bumps the tablet epoch (and the control
    // plane advances this replica via `ReplicaHandle::set_epoch`), the digest
    // round carries the bumped epoch and is **not** fenced — a re-placed spare
    // converges in the background, not only via read-repair on the first read.
    serve_anti_entropy(env, handle.clone(), TABLET, peers, ANTI_ENTROPY_INTERVAL);
    Box::new(handle)
}

/// The bootstrap policy pinning the tablet's replica set: a plain replication
/// factor (no residency/spread, as `animusd` has no topology labels yet). With
/// the factor capped below the cluster size, the leader's reconciler
/// (ADR 0005/0012) can move a tablet off a member detected `Down` onto a spare.
fn bootstrap_policy(replication_factor: usize) -> PlacementPolicy {
    PlacementPolicy::simple("animusd-default", replication_factor)
}

/// The leader's one-time cluster bootstrap, retried on a timer until it lands.
///
/// It registers **the data nodes** as `Active` members (so the failure detector
/// and the placement reconciler operate on the nodes that actually hold data —
/// not the control-group ids), places the single bootstrap tablet on the first
/// `min(N, MAX_REPLICATION_FACTOR)` of them, and attaches a `PlacementPolicy` so
/// the leader's reconciler keeps the replica set satisfying it: when a member is
/// detected `Down`, the tablet is automatically re-placed onto a spare. Idempotent
/// (skips once the tablet exists), so only the first leader to win does the work
/// and a re-election does not duplicate it.
async fn bootstrap(raft: RaftNode<ProdEnv>, data_ids: Vec<NodeId>) {
    let rf = data_ids.len().min(MAX_REPLICATION_FACTOR);
    let replicas: Vec<NodeId> = data_ids.iter().copied().take(rf).collect();
    loop {
        if raft.is_leader() && !raft.metadata().tablets.contains_key(&TABLET) {
            // The cluster members are the data nodes (they heartbeat and hold
            // data); the control-group ids are only the Raft consensus group.
            for &node in &data_ids {
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
            raft.propose(MetaCommand::SetTabletPolicy {
                tablet: TABLET,
                policy: Some(bootstrap_policy(rf)),
            });
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
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
            // CP-mode table: route to the leaderful per-tablet Raft group leader
            // (ADR 0017 #3a) instead of the AP quorum coordinator.
            ClientRequest::Put {
                key,
                value,
                table: Some(t),
            } if ctx.is_cp(&t) => ctx.cp_put(key, value).await,
            ClientRequest::Get {
                key,
                table: Some(t),
            } if ctx.is_cp(&t) => ctx.cp_get(key).await,
            ClientRequest::Put { key, value, .. } => match ctx.view_for(&key) {
                None => ClientResponse::Error("no tablet covers this key yet".into()),
                Some(view) => {
                    let _guard = ctx.coord_lock.lock().await;
                    // Assign a strictly-increasing version by reading the current
                    // one across a quorum first, so an overwrite wins regardless
                    // of which coordinator node issues it (no global clock yet).
                    match ctx
                        .coordinator
                        .read_version(&view, &key, CLIENT_TIMEOUT)
                        .await
                    {
                        None => ClientResponse::Error("could not read current version".into()),
                        Some(current) => {
                            let version = current + 1;
                            let ok = ctx
                                .coordinator
                                .write(&view, &key, &value, version, CLIENT_TIMEOUT)
                                .await;
                            if ok {
                                ClientResponse::PutOk
                            } else {
                                ClientResponse::Error("write did not reach a quorum".into())
                            }
                        }
                    }
                }
            },
            ClientRequest::Get { key, .. } => match ctx.view_for(&key) {
                None => ClientResponse::Error("no tablet covers this key yet".into()),
                Some(view) => {
                    let _guard = ctx.coord_lock.lock().await;
                    match ctx.coordinator.read(&view, &key, CLIENT_TIMEOUT).await {
                        ReadResult::Value(v) => ClientResponse::Value(v),
                        ReadResult::Failed => {
                            ClientResponse::Error("read did not reach a quorum".into())
                        }
                    }
                }
            },
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
    /// Resolve the routing view for `key` from cached metadata.
    pub(crate) fn view_for(&self, key: &[u8]) -> Option<TabletView> {
        self.raft
            .metadata()
            .tablets
            .values()
            .find(|t| t.range.contains(key))
            .map(|t| TabletView::from_tablet(t, self.r, self.w))
    }

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
            self.edge.propose_on_leaders(&command);
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
            self.edge.propose_on_leaders(&command);
            tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }
}

/// Bind an `n`-node cluster on `ip` with ephemeral ports and the conventional
/// ids (control `i`, data `100+i`, coord `200+i`), each under `dir/node-i`.
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
            data: addr(),
            coord: addr(),
            client: addr(),
            dynamo: addr(),
            cql: addr(),
            raftkv: addr(),
        };
        let node = Node::bind(
            config::control_id(i),
            config::data_id(i),
            config::coord_id(i),
            config::raftkv_id(i),
            addrs,
            dir.join(format!("node-{i}")),
        )
        .await?;
        nodes.push(node);
    }
    Ok(nodes)
}

/// Start a cluster previously bound with [`bind_cluster`] (quorum `r`/`w`),
/// each node's data replica backed by the durable on-disk [`LsmEngine`].
///
/// # Errors
/// Propagates a failure to open any node's data replica engine.
pub async fn start_cluster(
    bound: Vec<BoundNode>,
    r: usize,
    w: usize,
) -> std::io::Result<Vec<Node>> {
    start_cluster_with(bound, r, w, StorageBackend::default()).await
}

/// Like [`start_cluster`], but selects the data replicas' storage `backend`.
///
/// # Errors
/// Propagates a failure to open any node's data replica engine (LSM backend
/// only).
pub async fn start_cluster_with(
    bound: Vec<BoundNode>,
    r: usize,
    w: usize,
    backend: StorageBackend,
) -> std::io::Result<Vec<Node>> {
    let n = bound.len();
    let control_ids: Vec<NodeId> = (0..n).map(config::control_id).collect();
    let data_ids: Vec<NodeId> = (0..n).map(config::data_id).collect();
    let peers: BTreeMap<NodeId, SocketAddr> =
        bound.iter().flat_map(BoundNode::peer_entries).collect();
    // One edge-state set shared by every node of *this* cluster (so any node's
    // edge can reach the cluster's leader and they agree on GSI/keyspace state),
    // but distinct from any other cluster in the same process.
    let edge = ClusterEdgeState::new();
    let mut nodes = Vec::with_capacity(n);
    for b in bound {
        let node = b
            .start_with(
                peers.clone(),
                control_ids.clone(),
                data_ids.clone(),
                r,
                w,
                backend,
                edge.clone(),
            )
            .await?;
        nodes.push(node);
    }
    Ok(nodes)
}

/// Start the single node at `index` in `config` (per-process deployment): bind
/// this node's configured listeners, wire the cluster's peer address book from
/// the config, and start its protocols with the durable on-disk [`LsmEngine`]
/// data replica.
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

/// Like [`run_node`], but selects the data replica's storage `backend`.
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
    let (control_id, data_id, coord_id) = config.role_ids(index);
    let bound = Node::bind(
        control_id,
        data_id,
        coord_id,
        config::raftkv_id(index),
        addrs,
        dir,
    )
    .await?;
    // One node per process: a fresh per-process edge-state set (it registers only
    // this node's control handle — cross-process proposal forwarding is future
    // work, ADR 0013).
    bound
        .start_with(
            config.peer_book(),
            config.control_ids(),
            config.data_ids(),
            config.r,
            config.w,
            backend,
            ClusterEdgeState::new(),
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
