//! The system under test: a [`Cluster`] is a set of node endpoints plus
//! credentials plus an optional way to kill (and restart) nodes.
//!
//! Three ways to get one:
//!
//! - [`Cluster::external`] — a cluster someone else runs (the *publishable*
//!   shape: client and servers on separate hosts). Faults go through
//!   operator-supplied shell command templates (`--kill-cmd`, `--restart-cmd`).
//! - [`Cluster::launch_processes`] — the bench spawns one real `animusd
//!   --config FILE --node I` child per node on this host. A kill is a
//!   SIGKILL of the child (an honest crash); a restart respawns it on the
//!   same data dir.
//! - [`Cluster::launch_in_process`] — the nodes run inside this process
//!   (the same bring-up the integration tests use). A kill is
//!   `Node::shutdown_graceful`; nodes cannot be restarted. Used by the smoke
//!   test; fine for development.
//!
//! The latter two are **colocated** (client and servers share this host's
//! CPUs and disk), so every report from them is labelled non-publishable.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use animusd::config::{DynamoAuthConfig, NodeRole, TlsSection};
use animusd::{ClusterConfig, Node};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;

use crate::client::{Conn, Credentials, admin_get};
use crate::rt;
use crate::tls::TlsClient;

/// One node's client-facing and admin addresses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeEndpoints {
    pub index: usize,
    /// The DynamoDB JSON/HTTP listener.
    pub dynamo: SocketAddr,
    /// The admin/debug HTTP listener.
    pub admin: SocketAddr,
}

/// A fault to apply to the cluster.
#[derive(Clone, Debug)]
pub enum FaultAction {
    /// Kill node `node`.
    KillNode { node: usize },
    /// Kill the node leading `table`'s first tablet.
    KillLeader { table: String },
    /// Kill a node hosting a *non-leader* replica of `table`'s first tablet.
    KillFollower { table: String },
    /// Restart a previously killed node.
    Restart { node: usize },
}

/// What a fault did (recorded into the phase result).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FaultRecord {
    /// Human-readable action (`kill-leader`, ...).
    pub action: String,
    /// The node index acted on, when one was resolved.
    pub node: Option<usize>,
    /// Offset into the phase when the fault was triggered.
    pub at_offset_ms: u64,
    /// How long the kill/restart mechanism took to return.
    pub took_ms: u64,
    pub ok: bool,
    pub detail: String,
}

/// TLS for a cluster this process launches (`--launch processes|in-process`):
/// every node serves the same leaf certificate (its SAN must cover the name
/// the client verifies: `127.0.0.1`, or `--tls-server-name`), the CA both
/// secures the mutual intra-cluster ports and is what the client trusts.
#[derive(Clone, Debug)]
pub struct LaunchTls {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub ca_path: PathBuf,
    /// The client side of the same PKI (used for every dial the bench makes).
    pub client: TlsClient,
}

impl LaunchTls {
    fn section(&self) -> TlsSection {
        TlsSection {
            cert_path: self.cert_path.clone(),
            key_path: self.key_path.clone(),
            ca_path: Some(self.ca_path.clone()),
            peer_ca_path: None,
        }
    }
}

struct ProcSpec {
    bin: PathBuf,
    config_path: PathBuf,
    dir: PathBuf,
    extra_args: Vec<String>,
}

enum Control {
    /// No way to inject faults.
    None,
    /// Operator-supplied shell templates.
    Command {
        kill: Option<String>,
        restart: Option<String>,
    },
    /// `animusd` child processes.
    Processes {
        spec: ProcSpec,
        children: Mutex<Vec<Option<Child>>>,
    },
    /// Nodes inside this process.
    InProcess { nodes: Mutex<Vec<Option<Node>>> },
}

struct Inner {
    nodes: Vec<NodeEndpoints>,
    creds: Option<Arc<Credentials>>,
    /// Server-only TLS on the DynamoDB and admin dials, when configured.
    tls: Option<TlsClient>,
    control: Control,
    /// Idle keep-alive connections carried across phases, like a real
    /// client's connection pool: a node killed at a phase boundary is then
    /// discovered the way a real client discovers it (a reset on a pooled
    /// socket), not hidden by every phase redialling from scratch.
    pool: std::sync::Mutex<Vec<Conn>>,
}

/// A cheaply-cloneable handle to the system under test.
#[derive(Clone)]
pub struct Cluster {
    inner: Arc<Inner>,
}

fn io_err(msg: impl Into<String>) -> std::io::Error {
    std::io::Error::other(msg.into())
}

impl Cluster {
    /// A cluster this process did not start. `kill_cmd`/`restart_cmd` are
    /// `sh -c` templates; `{node}`, `{dynamo}`, `{admin}` and `{host}` are
    /// substituted (e.g. `ssh {host} systemctl kill -s KILL animusd`).
    #[must_use]
    pub fn external(
        nodes: Vec<NodeEndpoints>,
        creds: Option<Credentials>,
        tls: Option<TlsClient>,
        kill_cmd: Option<String>,
        restart_cmd: Option<String>,
    ) -> Self {
        let control = if kill_cmd.is_some() || restart_cmd.is_some() {
            Control::Command {
                kill: kill_cmd,
                restart: restart_cmd,
            }
        } else {
            Control::None
        };
        Self {
            inner: Arc::new(Inner {
                nodes,
                creds: creds.map(Arc::new),
                tls,
                control,
                pool: std::sync::Mutex::default(),
            }),
        }
    }

    /// Take an idle pooled connection, if any.
    pub(crate) fn checkout(&self) -> Option<Conn> {
        self.inner.pool.lock().ok()?.pop()
    }

    /// Return a still-usable connection to the pool.
    pub(crate) fn checkin(&self, conn: Conn) {
        if !conn.is_broken()
            && let Ok(mut p) = self.inner.pool.lock()
        {
            p.push(conn);
        }
    }

    /// Start `n` nodes inside this process (see the module docs).
    ///
    /// # Errors
    /// On a bind/start failure.
    pub async fn launch_in_process(
        n: usize,
        dir: &Path,
        creds: Option<Credentials>,
        tls: Option<LaunchTls>,
    ) -> std::io::Result<Self> {
        let mut config = ClusterConfig::generate(n, "127.0.0.1".parse().expect("ip"), 40_000);
        let any: SocketAddr = "127.0.0.1:0".parse().expect("addr");
        for r in &mut config.nodes {
            r.internal = any;
            r.client = any;
            r.dynamo = any;
            r.admin = any;
            r.intra = any;
            r.console = any;
            r.role = NodeRole::Both;
        }
        if let Some(c) = &creds {
            config.dynamo_auth = Some(auth_config(c));
        }
        if let Some(t) = &tls {
            for r in &mut config.nodes {
                r.tls = Some(t.section());
            }
        }
        let mut bounds = Vec::with_capacity(n);
        for (i, r) in config.nodes.clone().into_iter().enumerate() {
            let id = r.id.clone();
            bounds.push(Node::bind(id, r, dir.join(format!("core-{i}"))).await?);
        }
        for (r, b) in config.nodes.iter_mut().zip(&bounds) {
            r.internal = b.internal_addr();
            r.client = b.client_addr();
            r.dynamo = b.dynamo_addr();
            r.admin = b.admin_addr();
            r.intra = b.intra_addr();
            r.console = b.console_addr();
        }
        let endpoints = config
            .nodes
            .iter()
            .enumerate()
            .map(|(index, r)| NodeEndpoints {
                index,
                dynamo: r.dynamo,
                admin: r.admin,
            })
            .collect();
        let mut nodes = Vec::with_capacity(n);
        for (i, b) in bounds.into_iter().enumerate() {
            nodes.push(Some(animusd::run_bound_node(b, &config, i).await?));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                nodes: endpoints,
                creds: creds.map(Arc::new),
                tls: tls.map(|t| t.client),
                control: Control::InProcess {
                    nodes: Mutex::new(nodes),
                },
                pool: std::sync::Mutex::default(),
            }),
        })
    }

    /// Spawn `n` `animusd --config FILE --node I` children from `bin` under
    /// `dir`, retrying with fresh ports if a child dies during start-up (a
    /// lost probe-and-release port race).
    ///
    /// # Errors
    /// If no attempt brings every node's admin port healthy in time.
    pub async fn launch_processes(
        n: usize,
        dir: &Path,
        bin: &Path,
        creds: Option<Credentials>,
        tls: Option<LaunchTls>,
        extra_args: Vec<String>,
    ) -> std::io::Result<Self> {
        let mut last = String::new();
        for attempt in 0..4 {
            let attempt_dir = dir.join(format!("attempt-{attempt}"));
            match Self::launch_processes_once(
                n,
                &attempt_dir,
                bin,
                creds.clone(),
                tls.clone(),
                extra_args.clone(),
            )
            .await
            {
                Ok(c) => return Ok(c),
                Err(e) => {
                    eprintln!("cluster launch attempt {attempt} failed: {e}; retrying");
                    last = e.to_string();
                }
            }
        }
        Err(io_err(format!(
            "could not launch a {n}-node cluster: {last}"
        )))
    }

    async fn launch_processes_once(
        n: usize,
        dir: &Path,
        bin: &Path,
        creds: Option<Credentials>,
        tls: Option<LaunchTls>,
        extra_args: Vec<String>,
    ) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let mut config = ClusterConfig::generate(n, "127.0.0.1".parse().expect("ip"), 40_000);
        // Probe-and-release: hold every socket until all are chosen so the
        // OS cannot hand out the same port twice; a third party stealing one
        // in the gap shows up as an early child exit and a retry.
        let mut held = Vec::new();
        let mut addrs = Vec::new();
        for _ in 0..n * 6 {
            let l = std::net::TcpListener::bind("127.0.0.1:0")?;
            addrs.push(l.local_addr()?);
            held.push(l);
        }
        for (i, r) in config.nodes.iter_mut().enumerate() {
            let a = &addrs[i * 6..i * 6 + 6];
            r.internal = a[0];
            r.client = a[1];
            r.dynamo = a[2];
            r.admin = a[3];
            r.intra = a[4];
            r.console = a[5];
        }
        if let Some(c) = &creds {
            config.dynamo_auth = Some(auth_config(c));
        }
        if let Some(t) = &tls {
            for r in &mut config.nodes {
                r.tls = Some(t.section());
            }
        }
        let config_path = dir.join("cluster.json");
        std::fs::write(
            &config_path,
            serde_json::to_vec_pretty(&config).map_err(|e| io_err(e.to_string()))?,
        )?;
        drop(held);
        let spec = ProcSpec {
            bin: bin.to_owned(),
            config_path,
            dir: dir.to_owned(),
            extra_args,
        };
        let mut children = Vec::with_capacity(n);
        for i in 0..n {
            children.push(Some(spawn_child(&spec, i)?));
        }
        let cluster = Self {
            inner: Arc::new(Inner {
                nodes: config
                    .nodes
                    .iter()
                    .enumerate()
                    .map(|(index, r)| NodeEndpoints {
                        index,
                        dynamo: r.dynamo,
                        admin: r.admin,
                    })
                    .collect(),
                creds: creds.map(Arc::new),
                tls: tls.map(|t| t.client),
                control: Control::Processes {
                    spec,
                    children: Mutex::new(children),
                },
                pool: std::sync::Mutex::default(),
            }),
        };
        match cluster.await_ready(Duration::from_secs(60)).await {
            Ok(()) => Ok(cluster),
            Err(e) => {
                cluster.shutdown().await;
                Err(io_err(e))
            }
        }
    }

    /// Every node's endpoints.
    #[must_use]
    pub fn nodes(&self) -> &[NodeEndpoints] {
        &self.inner.nodes
    }

    /// The DynamoDB listener of every node.
    #[must_use]
    pub fn dynamo_endpoints(&self) -> Vec<SocketAddr> {
        self.inner.nodes.iter().map(|n| n.dynamo).collect()
    }

    /// The server-only TLS client config every dial uses, if any.
    #[must_use]
    pub fn tls(&self) -> Option<&TlsClient> {
        self.inner.tls.as_ref()
    }

    /// Dial up to `target` connections into the pool before a phase starts,
    /// so workers begin with established (and, under TLS, already
    /// handshaken) connections and no handshake lands inside a measured
    /// op. Best-effort: an unreachable endpoint is skipped (the worker's
    /// redial path handles it, and its cost is then charged, as for any
    /// reconnect after a failure).
    pub async fn prewarm(&self, target: usize) {
        let have = self.inner.pool.lock().map_or(target, |p| p.len());
        let eps = self.dynamo_endpoints();
        if eps.is_empty() {
            return;
        }
        for k in have..target {
            let addr = eps[k % eps.len()];
            if let Some(Ok(c)) = rt::timeout(
                Duration::from_secs(2),
                Conn::connect(addr, self.credentials(), self.tls()),
            )
            .await
            {
                self.checkin(c);
            }
        }
    }

    /// The SigV4 credentials requests are signed with, if any.
    #[must_use]
    pub fn credentials(&self) -> Option<Arc<Credentials>> {
        self.inner.creds.clone()
    }

    /// `external` / `processes` / `in-process`.
    #[must_use]
    pub fn launch_mode(&self) -> &'static str {
        match self.inner.control {
            Control::None | Control::Command { .. } => "external",
            Control::Processes { .. } => "processes",
            Control::InProcess { .. } => "in-process",
        }
    }

    /// True when this process launched the servers on its own host (so the
    /// client and servers share the machine and results are not publishable).
    #[must_use]
    pub fn launched_here(&self) -> bool {
        matches!(
            self.inner.control,
            Control::Processes { .. } | Control::InProcess { .. }
        )
    }

    /// Converged-or-timeout poll until every *live* node's `/admin/health`
    /// is 200 (the control plane has had a recent leader).
    ///
    /// # Errors
    /// With the last observation, if `deadline` passes first.
    pub async fn await_ready(&self, deadline: Duration) -> Result<(), String> {
        let clock = rt::Clock::start();
        let limit = u64::try_from(deadline.as_nanos()).unwrap_or(u64::MAX);
        loop {
            let mut not_ready = Vec::new();
            for n in &self.inner.nodes {
                match admin_get(n.admin, "/admin/health", self.tls()).await {
                    Ok((200, _)) => {}
                    Ok((s, _)) => not_ready.push(format!("node {} health {s}", n.index)),
                    Err(e) => not_ready.push(format!("node {} admin: {e}", n.index)),
                }
            }
            if not_ready.is_empty() {
                return Ok(());
            }
            if clock.now_ns() > limit {
                return Err(format!(
                    "cluster not ready after {deadline:?}: {}",
                    not_ready.join("; ")
                ));
            }
            rt::sleep(Duration::from_millis(100)).await;
        }
    }

    /// First tablet of `table` and which nodes host / lead it, from
    /// `/admin/status` + each node's `/admin/raftkv`. Unreachable nodes are
    /// skipped (they host nothing we can see).
    pub async fn tablet_roles(&self, table: &str) -> Option<TabletRoles> {
        let mut tablet = None;
        for n in &self.inner.nodes {
            if let Ok((200, v)) = admin_get(n.admin, "/admin/status", self.tls()).await {
                tablet = first_tablet_of(&v, table);
                if tablet.is_some() {
                    break;
                }
            }
        }
        let tablet = tablet?;
        let (mut leader, mut hosts) = (None, Vec::new());
        for n in &self.inner.nodes {
            let Ok((200, v)) = admin_get(n.admin, "/admin/raftkv", self.tls()).await else {
                continue;
            };
            let Some(groups) = v["groups"].as_array() else {
                continue;
            };
            for g in groups
                .iter()
                .filter(|g| g["tablet"].as_u64() == Some(tablet))
            {
                hosts.push(n.index);
                if g["is_leader"].as_bool() == Some(true) {
                    leader = Some(n.index);
                }
            }
        }
        Some(TabletRoles {
            tablet,
            leader,
            hosts,
        })
    }

    /// Apply a fault, returning what happened (never panics; failures are
    /// recorded in the returned record).
    pub async fn apply_fault(&self, action: &FaultAction) -> FaultRecord {
        let clock = rt::Clock::start();
        let (name, resolved) = match action {
            FaultAction::KillNode { node } => ("kill-node", Ok(*node)),
            FaultAction::KillLeader { table } => ("kill-leader", self.resolve(table, true).await),
            FaultAction::KillFollower { table } => {
                ("kill-follower", self.resolve(table, false).await)
            }
            FaultAction::Restart { node } => ("restart", Ok(*node)),
        };
        let mut rec = FaultRecord {
            action: name.to_owned(),
            ..FaultRecord::default()
        };
        match resolved {
            Err(e) => rec.detail = e,
            Ok(node) => {
                rec.node = Some(node);
                let r = if matches!(action, FaultAction::Restart { .. }) {
                    self.restart_node(node).await
                } else {
                    self.kill_node(node).await
                };
                match r {
                    Ok(d) => {
                        rec.ok = true;
                        rec.detail = d;
                    }
                    Err(e) => rec.detail = e,
                }
            }
        }
        rec.took_ms = clock.now_ns() / 1_000_000;
        rec
    }

    async fn resolve(&self, table: &str, leader: bool) -> Result<usize, String> {
        let clock = rt::Clock::start();
        loop {
            if let Some(r) = self.tablet_roles(table).await {
                if leader {
                    if let Some(l) = r.leader {
                        return Ok(l);
                    }
                } else if let Some(f) = r.hosts.iter().copied().find(|h| Some(*h) != r.leader)
                    && r.leader.is_some()
                {
                    return Ok(f);
                }
            }
            if clock.now_ns() > 15_000_000_000 {
                return Err(format!(
                    "could not resolve a {} of `{table}`'s first tablet from /admin within 15s",
                    if leader { "leader" } else { "follower" }
                ));
            }
            rt::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn kill_node(&self, node: usize) -> Result<String, String> {
        match &self.inner.control {
            Control::None => Err("no kill mechanism configured (give --kill-cmd)".into()),
            Control::Command { kill, .. } => match kill {
                Some(t) => self.run_template(t, node).await,
                None => Err("no --kill-cmd given".into()),
            },
            Control::Processes { children, .. } => {
                let mut g = children.lock().await;
                let Some(mut c) = g.get_mut(node).and_then(Option::take) else {
                    return Err(format!("node {node} is not running"));
                };
                c.start_kill().map_err(|e| e.to_string())?;
                let _ = c.wait().await;
                Ok("SIGKILL to animusd child, reaped".into())
            }
            Control::InProcess { nodes } => {
                let n = nodes.lock().await.get_mut(node).and_then(Option::take);
                match n {
                    Some(n) => {
                        n.shutdown_graceful().await;
                        Ok("in-process Node::shutdown_graceful (not a crash)".into())
                    }
                    None => Err(format!("node {node} is not running")),
                }
            }
        }
    }

    async fn restart_node(&self, node: usize) -> Result<String, String> {
        match &self.inner.control {
            Control::Command { restart, .. } => match restart {
                Some(t) => self.run_template(t, node).await,
                None => Err("no --restart-cmd given".into()),
            },
            Control::Processes { spec, children } => {
                let mut g = children.lock().await;
                match g.get_mut(node) {
                    Some(slot @ None) => {
                        *slot = Some(spawn_child(spec, node).map_err(|e| e.to_string())?);
                        Ok("respawned animusd child on its data dir".into())
                    }
                    _ => Err(format!("node {node} is already running")),
                }
            }
            Control::InProcess { .. } => Err("in-process nodes cannot be restarted".into()),
            Control::None => Err("no restart mechanism configured".into()),
        }
    }

    async fn run_template(&self, template: &str, node: usize) -> Result<String, String> {
        let ep = self
            .inner
            .nodes
            .get(node)
            .ok_or_else(|| format!("no node {node}"))?;
        let cmd = template
            .replace("{node}", &node.to_string())
            .replace("{host}", &ep.dynamo.ip().to_string())
            .replace("{dynamo}", &ep.dynamo.to_string())
            .replace("{admin}", &ep.admin.to_string());
        let out = Command::new("sh")
            .arg("-c")
            .arg(&cmd)
            .output()
            .await
            .map_err(|e| format!("spawn `{cmd}`: {e}"))?;
        if out.status.success() {
            Ok(format!("ran `{cmd}`"))
        } else {
            Err(format!(
                "`{cmd}` exited {}: {}",
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            ))
        }
    }

    /// Stop everything this process launched (no-op for an external cluster).
    pub async fn shutdown(&self) {
        match &self.inner.control {
            Control::Processes { children, .. } => {
                for c in children.lock().await.iter_mut() {
                    if let Some(mut c) = c.take() {
                        let _ = c.start_kill();
                        let _ = c.wait().await;
                    }
                }
            }
            Control::InProcess { nodes } => {
                for n in nodes.lock().await.iter_mut() {
                    if let Some(n) = n.take() {
                        n.shutdown_graceful().await;
                    }
                }
            }
            Control::None | Control::Command { .. } => {}
        }
    }
}

/// Which nodes host and lead a tablet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TabletRoles {
    pub tablet: u64,
    pub leader: Option<usize>,
    pub hosts: Vec<usize>,
}

fn auth_config(c: &Credentials) -> DynamoAuthConfig {
    DynamoAuthConfig {
        credentials: std::iter::once((c.access_key_id.clone(), c.secret_access_key.clone()))
            .collect(),
    }
}

fn spawn_child(spec: &ProcSpec, index: usize) -> std::io::Result<Child> {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(spec.dir.join(format!("node-{index}.log")))?;
    Command::new(&spec.bin)
        .arg("--config")
        .arg(&spec.config_path)
        .arg("--node")
        .arg(index.to_string())
        .arg("--dir")
        .arg(spec.dir.join(format!("data-{index}")))
        .args(&spec.extra_args)
        .stdin(std::process::Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log)
        .kill_on_drop(true)
        .spawn()
}

/// The smallest tablet id of `table` in a `/admin/status` document.
fn first_tablet_of(status: &Value, table: &str) -> Option<u64> {
    status["tablets"]
        .as_object()?
        .values()
        .filter(|t| t["table"].as_str() == Some(table))
        .filter_map(|t| t["id"].as_u64())
        .min()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_tablet_picks_the_smallest_id_of_the_named_table() {
        let v = serde_json::json!({"tablets": {
            "7": {"id": 7, "table": "t"},
            "3": {"id": 3, "table": "t"},
            "1": {"id": 1, "table": "other"},
            "2": {"id": 2},
        }});
        assert_eq!(first_tablet_of(&v, "t"), Some(3));
        assert_eq!(first_tablet_of(&v, "missing"), None);
        assert_eq!(first_tablet_of(&Value::Null, "t"), None);
    }
}
