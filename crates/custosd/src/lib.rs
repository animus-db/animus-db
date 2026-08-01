//! Node assembly: wires the control plane (`RaftNode`), the data plane
//! (`serve_replica` + a `DataClient` coordinator), and a client-facing request
//! server into a runnable CustosDB node over `ProdEnv`.
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

mod cql;
mod dynamo;

use custos_control::{MetaCommand, Metadata, NodeStatus, RaftNode};
use custos_data::{DataClient, ReadResult, ReplicaHandle, TabletView, serve_replica};
use custos_env::{NodeId, ProdEnv};
use custos_storage::MemoryEngine;
use custos_tablet::{Epoch, KeyRange, TabletId};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// The single bootstrap tablet covering the whole keyspace.
const TABLET: TabletId = TabletId(1);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(5);

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

    /// Wire the peer address book into every env and start all protocols.
    /// `control_ids`/`data_ids` are the full control group and the tablet's
    /// replica set; `r`/`w` are the quorum sizes.
    pub fn start(
        self,
        peers: BTreeMap<NodeId, SocketAddr>,
        control_ids: Vec<NodeId>,
        data_ids: Vec<NodeId>,
        r: usize,
        w: usize,
    ) -> Node {
        self.control_env.set_peers(peers.clone());
        self.data_env.set_peers(peers.clone());
        self.coord_env.set_peers(peers);

        let raft = RaftNode::start(self.control_env, control_ids.clone());
        let replica = serve_replica(self.data_env, MemoryEngine::new(), Epoch::INITIAL);
        let coordinator = DataClient::new(self.coord_env);

        // Bootstrap: whichever node is leader registers membership + the tablet
        // (idempotent).
        {
            let raft = raft.clone();
            let members = control_ids.clone();
            let data_ids = data_ids.clone();
            tokio::spawn(bootstrap(raft, members, data_ids));
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
            tokio::spawn(serve_clients(self.client_listener, ctx.clone()));
            tokio::spawn(dynamo::serve(self.dynamo_listener, ctx.clone()));
            tokio::spawn(cql::serve(self.cql_listener, ctx));
        }

        Node {
            raft,
            _replica: replica,
            client_addr: self.client_addr,
            dynamo_addr: self.dynamo_addr,
            cql_addr: self.cql_addr,
        }
    }
}

/// A running node. Holds the handles that keep its envs and tasks alive.
pub struct Node {
    raft: RaftNode<ProdEnv>,
    _replica: ReplicaHandle<MemoryEngine>,
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

async fn bootstrap(raft: RaftNode<ProdEnv>, members: Vec<NodeId>, data_ids: Vec<NodeId>) {
    loop {
        if raft.is_leader() && !raft.metadata().tablets.contains_key(&TABLET) {
            for &node in &members {
                raft.propose(MetaCommand::UpsertMember {
                    node,
                    labels: BTreeMap::new(),
                    status: NodeStatus::Active,
                });
            }
            raft.propose(MetaCommand::CreateTablet {
                tablet: TABLET,
                range: KeyRange::whole(),
                replicas: data_ids.clone(),
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

/// Start a cluster previously bound with [`bind_cluster`] (quorum `r`/`w`).
pub fn start_cluster(bound: Vec<BoundNode>, r: usize, w: usize) -> Vec<Node> {
    let n = bound.len();
    let control_ids: Vec<NodeId> = (0..n).map(config::control_id).collect();
    let data_ids: Vec<NodeId> = (0..n).map(config::data_id).collect();
    let peers: BTreeMap<NodeId, SocketAddr> =
        bound.iter().flat_map(BoundNode::peer_entries).collect();
    bound
        .into_iter()
        .map(|b| b.start(peers.clone(), control_ids.clone(), data_ids.clone(), r, w))
        .collect()
}

/// Start the single node at `index` in `config` (per-process deployment): bind
/// this node's configured listeners, wire the cluster's peer address book from
/// the config, and start its protocols.
///
/// # Errors
/// Returns `InvalidInput` if `index` is out of range, or propagates a bind
/// failure.
pub async fn run_node(
    config: &ClusterConfig,
    index: usize,
    dir: impl Into<PathBuf>,
) -> std::io::Result<Node> {
    let addrs = *config.nodes.get(index).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "node index out of range")
    })?;
    let (control_id, data_id, coord_id) = config.role_ids(index);
    let bound = Node::bind(control_id, data_id, coord_id, addrs, dir).await?;
    Ok(bound.start(
        config.peer_book(),
        config.control_ids(),
        config.data_ids(),
        config.r,
        config.w,
    ))
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
