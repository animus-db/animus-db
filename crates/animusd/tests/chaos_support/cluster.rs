//! A bare multi-process `animusd` cluster on loopback, wired through the
//! fault proxies, with process-level faults (kill -9, SIGSTOP/SIGCONT).
//!
//! Wiring (see `proxy.rs`): node `i` really binds `127.0.0.1:P`, advertises
//! `127.0.77.(i+1):P` (`advertise_host`), and its config lists every *other*
//! node at their proxy address, so all node-to-node traffic crosses a proxy.
//! Clients (the workload, the admin probes) talk to the real addresses.

use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::task::JoinHandle;

use super::client::http_get;
use super::proxy::{Faults, PortKind, spawn_proxy};

/// Ports per node, in `ClusterConfig::generate`'s order.
const STRIDE: u16 = 6;
const PORT_INTERNAL: u16 = 0;
const PORT_DYNAMO: u16 = 2;
const PORT_ADMIN: u16 = 3;
const PORT_INTRA: u16 = 4;

pub const BIN: &str = env!("CARGO_BIN_EXE_animusd");

fn proxy_ip(i: usize) -> Ipv4Addr {
    Ipv4Addr::new(127, 0, 77, (i + 1) as u8)
}

pub struct ChaosCluster {
    pub n: usize,
    base_port: u16,
    root: PathBuf,
    pub faults: Arc<Faults>,
    children: Vec<Option<Child>>,
    paused: Vec<bool>,
    proxies: Vec<JoinHandle<()>>,
}

impl ChaosCluster {
    /// Find `6n` consecutive free loopback ports (on 127.0.0.1 and on every
    /// proxy address) at or after a seeded start.
    fn pick_base_port(n: usize, seed: u64) -> u16 {
        let span = STRIDE * n as u16;
        let mut start = 20_000 + (seed % 9_000) as u16;
        for _ in 0..200 {
            let free = (0..span).all(|off| {
                let port = start + off;
                StdListener::bind((Ipv4Addr::LOCALHOST, port)).is_ok()
                    && (0..n).all(|i| StdListener::bind((proxy_ip(i), port)).is_ok())
            });
            if free {
                return start;
            }
            start += span;
            if start > 31_000 {
                start = 20_000;
            }
        }
        panic!("no free port range for a {n}-node chaos cluster");
    }

    /// Generate configs, start the proxies. Nodes are NOT started yet.
    pub async fn prepare(n: usize, root: &Path, seed: u64) -> Self {
        let base_port = Self::pick_base_port(n, seed);
        let out = Command::new(BIN)
            .args(["gen-config", "--nodes", &n.to_string(), "--base-port"])
            .arg(base_port.to_string())
            .output()
            .expect("run animusd gen-config");
        assert!(out.status.success(), "gen-config failed");
        let base: Value = serde_json::from_slice(&out.stdout).expect("gen-config json");

        let faults = Faults::new();
        let names: Arc<Vec<String>> = Arc::new(
            base["nodes"]
                .as_array()
                .expect("nodes")
                .iter()
                .map(|e| e["id"].as_str().expect("id").to_string())
                .collect(),
        );

        // One config per node: itself on 127.0.0.1 (+ advertise_host), every
        // other node at its proxy address.
        for i in 0..n {
            let mut cfg = base.clone();
            for (j, entry) in cfg["nodes"]
                .as_array_mut()
                .expect("nodes")
                .iter_mut()
                .enumerate()
            {
                if j == i {
                    entry["advertise_host"] = Value::String(proxy_ip(i).to_string());
                    continue;
                }
                for key in ["internal", "client", "intra", "dynamo", "admin", "console"] {
                    let a: SocketAddr = entry[key].as_str().expect("addr").parse().expect("addr");
                    entry[key] =
                        Value::String(SocketAddr::new(proxy_ip(j).into(), a.port()).to_string());
                }
            }
            std::fs::write(
                root.join(format!("cfg{i}.json")),
                serde_json::to_vec_pretty(&cfg).expect("cfg json"),
            )
            .expect("write cfg");
        }
        std::fs::create_dir_all(root.join("logs")).expect("logs dir");

        // One proxy per (node, port): proxy-ip:port -> 127.0.0.1:port.
        let mut proxies = Vec::new();
        for i in 0..n {
            for off in 0..STRIDE {
                let port = base_port + STRIDE * i as u16 + off;
                let kind = match off {
                    PORT_INTERNAL => PortKind::Internal,
                    PORT_INTRA => PortKind::Intra,
                    _ => PortKind::Other,
                };
                let h = spawn_proxy(
                    SocketAddr::new(proxy_ip(i).into(), port),
                    SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port),
                    kind,
                    i,
                    Arc::clone(&faults),
                    Arc::clone(&names),
                )
                .await
                .expect("bind proxy");
                proxies.push(h);
            }
        }

        Self {
            n,
            base_port,
            root: root.to_path_buf(),
            faults,
            children: (0..n).map(|_| None).collect(),
            paused: vec![false; n],
            proxies,
        }
    }

    fn port(&self, i: usize, off: u16) -> SocketAddr {
        SocketAddr::new(
            Ipv4Addr::LOCALHOST.into(),
            self.base_port + STRIDE * i as u16 + off,
        )
    }

    pub fn dynamo_addr(&self, i: usize) -> SocketAddr {
        self.port(i, PORT_DYNAMO)
    }

    pub fn admin_addr(&self, i: usize) -> SocketAddr {
        self.port(i, PORT_ADMIN)
    }

    #[allow(dead_code, reason = "used by the soak target, not chaos")]
    /// The OS pid of node `i`, if it is running.
    pub fn pid(&self, i: usize) -> Option<u32> {
        self.children[i].as_ref().map(Child::id)
    }

    #[allow(dead_code, reason = "used by the soak target, not chaos")]
    /// Node `i`'s data directory (`--dir`).
    pub fn data_dir(&self, i: usize) -> PathBuf {
        self.root.join(format!("data{i}"))
    }

    pub fn log_path(&self, i: usize) -> PathBuf {
        self.root.join("logs").join(format!("n{i}.log"))
    }

    /// Start (or restart on the same data dir) node `i`.
    pub fn start(&mut self, i: usize) {
        assert!(self.children[i].is_none(), "node {i} already running");
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.log_path(i))
            .expect("open node log");
        let err = log.try_clone().expect("clone log fd");
        let child = Command::new(BIN)
            .arg("--config")
            .arg(self.root.join(format!("cfg{i}.json")))
            .arg("--node")
            .arg(i.to_string())
            .arg("--dir")
            .arg(self.root.join(format!("data{i}")))
            .env("RUST_BACKTRACE", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(err))
            .spawn()
            .expect("spawn animusd");
        self.children[i] = Some(child);
        self.paused[i] = false;
    }

    pub fn start_all(&mut self) {
        for i in 0..self.n {
            self.start(i);
        }
    }

    pub fn is_running(&self, i: usize) -> bool {
        self.children[i].is_some()
    }

    /// `kill -9` and reap. No-op if already down.
    pub fn kill9(&mut self, i: usize) {
        if let Some(mut c) = self.children[i].take() {
            let _ = c.kill();
            let _ = c.wait();
        }
        self.paused[i] = false;
    }

    fn signal(&self, i: usize, sig: &str) {
        if let Some(c) = &self.children[i] {
            let _ = Command::new("kill")
                .arg(sig)
                .arg(c.id().to_string())
                .status();
        }
    }

    /// SIGSTOP: a GC / VM-stall stand-in. The kernel still accepts TCP.
    pub fn pause(&mut self, i: usize) {
        if self.children[i].is_some() && !self.paused[i] {
            self.signal(i, "-STOP");
            self.paused[i] = true;
        }
    }

    pub fn resume(&mut self, i: usize) {
        if self.paused[i] {
            self.signal(i, "-CONT");
            self.paused[i] = false;
        }
    }

    /// Nodes that exited without the harness killing them: a finding.
    pub fn unexpected_exits(&mut self) -> Vec<(usize, String)> {
        let mut out = Vec::new();
        for i in 0..self.n {
            if let Some(c) = self.children[i].as_mut()
                && let Ok(Some(status)) = c.try_wait()
            {
                out.push((i, status.to_string()));
                self.children[i] = None;
            }
        }
        out
    }

    /// Which live, un-paused node is the control-plane leader right now.
    pub async fn control_leader(&self) -> Option<usize> {
        for i in 0..self.n {
            if self.children[i].is_none() || self.paused[i] {
                continue;
            }
            if let Ok((200, body)) = http_get(
                self.admin_addr(i),
                "/admin/raft",
                Duration::from_millis(1500),
            )
            .await
                && serde_json::from_str::<Value>(&body)
                    .ok()
                    .and_then(|v| v["is_leader"].as_bool())
                    == Some(true)
            {
                return Some(i);
            }
        }
        None
    }

    /// Count of this table's tablets in the replicated map, per the first
    /// reachable node's `/admin/status` (best effort, for the bring-up wait).
    pub async fn tablet_count(&self, table: &str) -> usize {
        for i in 0..self.n {
            if let Ok((200, body)) =
                http_get(self.admin_addr(i), "/admin/status", Duration::from_secs(3)).await
                && let Ok(v) = serde_json::from_str::<Value>(&body)
            {
                let tablets = &v["tablets"];
                let rows: Vec<&Value> = match tablets {
                    Value::Array(a) => a.iter().collect(),
                    Value::Object(o) => o.values().collect(),
                    _ => Vec::new(),
                };
                return rows.iter().filter(|t| t["table"] == table).count();
            }
        }
        0
    }
}

impl Drop for ChaosCluster {
    fn drop(&mut self) {
        for i in 0..self.n {
            if let Some(c) = self.children[i].as_mut() {
                // A stopped process can still be SIGKILLed.
                let _ = c.kill();
                let _ = c.wait();
            }
        }
        for p in &self.proxies {
            p.abort();
        }
    }
}
