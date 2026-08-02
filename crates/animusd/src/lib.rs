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
//! - **data** — the storage replica (`serve_replica`),
//! - **coord** — the quorum coordinator (`DataClient`) used to serve clients.
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
use std::sync::Arc;
use std::time::Duration;

pub mod config;
pub use config::ClusterConfig;
// Re-exported so callers (CLI, tests, operators) can inspect a node's cached
// cluster metadata — membership status and the tablet map — without depending on
// `animus-control` directly.
pub use animus_control::{Metadata, NodeStatus};

mod cql;
mod dynamo;

use animus_control::node::heartbeat_loop;
use animus_control::{MetaCommand, PlacementPolicy, RaftNode};
use animus_data::{DataClient, ReadResult, TabletView, serve_anti_entropy, serve_replica};
use animus_env::{EnvExt, NodeId, ProdEnv};
use animus_storage::{LsmEngine, MemoryEngine, StorageEngine};
use animus_tablet::{Epoch, KeyRange, TabletId};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

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
    /// Store `value` at `key` (quorum write).
    Put { key: Vec<u8>, value: Vec<u8> },
    /// Read the latest value at `key` (quorum read).
    Get { key: Vec<u8> },
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

/// Listen addresses for a node's six endpoints (use port 0 for ephemeral).
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
    control_env: ProdEnv,
    data_env: ProdEnv,
    coord_env: ProdEnv,
    control_addr: SocketAddr,
    data_addr: SocketAddr,
    coord_addr: SocketAddr,
    client_listener: TcpListener,
    client_addr: SocketAddr,
    dynamo_listener: TcpListener,
    dynamo_addr: SocketAddr,
    cql_listener: TcpListener,
    cql_addr: SocketAddr,
}

impl BoundNode {
    /// `(control_id, addr)`, `(data_id, addr)`, `(coord_id, addr)` — the entries
    /// this node contributes to the cluster peer book.
    pub fn peer_entries(&self) -> [(NodeId, SocketAddr); 3] {
        [
            (self.control_id, self.control_addr),
            (self.data_id, self.data_addr),
            (self.coord_id, self.coord_addr),
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
    pub async fn start_with(
        self,
        peers: BTreeMap<NodeId, SocketAddr>,
        control_ids: Vec<NodeId>,
        data_ids: Vec<NodeId>,
        r: usize,
        w: usize,
        backend: StorageBackend,
    ) -> std::io::Result<Node> {
        self.control_env.set_peers(peers.clone());
        self.data_env.set_peers(peers.clone());
        self.coord_env.set_peers(peers);

        // Keep clones of the three internal envs so [`Node::shutdown`] can abort
        // every task they own (Raft driver, replica serve loop, accept loops),
        // freeing their listener ports for a restart.
        let envs = [
            self.control_env.clone(),
            self.data_env.clone(),
            self.coord_env.clone(),
        ];

        let raft = RaftNode::start(self.control_env, control_ids.clone());

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
        let coordinator = DataClient::new(self.coord_env);

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
    /// The node's three internal `ProdEnv` roles (control/data/coord), kept so
    /// [`shutdown`](Node::shutdown) can abort every task they own and free their
    /// listener ports.
    envs: [ProdEnv; 3],
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
        addrs: RoleAddrs,
        data_dir: impl Into<PathBuf>,
    ) -> std::io::Result<BoundNode> {
        let dir = data_dir.into();
        let (control_env, control_addr) =
            ProdEnv::bind(control_id, addrs.control, dir.join("control")).await?;
        let (data_env, data_addr) = ProdEnv::bind(data_id, addrs.data, dir.join("data")).await?;
        let (coord_env, coord_addr) =
            ProdEnv::bind(coord_id, addrs.coord, dir.join("coord")).await?;
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
            control_env,
            data_env,
            coord_env,
            control_addr,
            data_addr,
            coord_addr,
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
}

/// Shared context for the client request server and the DynamoDB HTTP endpoint:
/// the cached metadata view for routing, the quorum coordinator, and the
/// per-node serialization lock around the single-consumer coord inbox.
#[derive(Clone)]
pub(crate) struct ClientCtx {
    raft: RaftNode<ProdEnv>,
    pub(crate) coordinator: DataClient<ProdEnv>,
    pub(crate) coord_lock: Arc<tokio::sync::Mutex<()>>,
    r: usize,
    w: usize,
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
            ClientRequest::Put { key, value } => match ctx.view_for(&key) {
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
            ClientRequest::Get { key } => match ctx.view_for(&key) {
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
        };
        let node = Node::bind(
            config::control_id(i),
            config::data_id(i),
            config::coord_id(i),
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
    let bound = Node::bind(control_id, data_id, coord_id, addrs, dir).await?;
    bound
        .start_with(
            config.peer_book(),
            config.control_ids(),
            config.data_ids(),
            config.r,
            config.w,
            backend,
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
