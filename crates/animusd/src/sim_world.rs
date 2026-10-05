//! `SimWorld` — G-01 stage G-d, milestone M0 spike: several independent
//! [`SimCluster`]s (each its own `Simulator`, node set, `Metadata` and control
//! plane) driven **in lockstep** from one seed, plus a [`PeerBridge`] WAN model
//! at the peer-client seam the MREC milestones will use for cross-cluster
//! requests (`docs/..` g-d plan, section 0 finding 5 and M0).
//!
//! # Design (two simulators in lockstep, WAN model at the peer-client seam)
//!
//! `SimCluster` bakes node id == index and owns exactly one `Simulator`, so
//! two clusters cannot share one. Instead each cluster keeps its own
//! `Simulator` (seed derived from the world seed) and the world driver advances
//! them to **identical absolute virtual times**, in `quantum` steps, pumping
//! the bridge between steps:
//!
//! * a cross-cluster call is an ordinary `async` future spawned in the
//!   *sender's* simulator ([`BridgeClient::call`]): it registers a
//!   `futures::channel::oneshot` reply slot with the bridge, then waits on the
//!   slot raced against `env.sleep(timeout)` (the sender's own virtual timer);
//! * the bridge queues the request keyed `(deliver_at, seq)` where `deliver_at =
//!   sender env.now() + latency + jitter`, all drawn from the bridge's own
//!   seeded RNG (loss and duplication too);
//! * the driver, after advancing both simulators to a due time, hands the
//!   request to the destination cluster's registered handler **as a task
//!   spawned in the destination simulator**; the handler's answer travels back
//!   the same way and completes the oneshot from outside the sender's executor.
//!   `animus-sim`'s waker is an `Arc`-based `ArcWake` that pushes onto the
//!   task's ready queue, and `Simulator::run_until` drains the ready queue
//!   before looking at the timeline, so an external wake is polled at the very
//!   next step with no special support.
//!
//! Determinism: every input to the bridge (call order, `env.now()` stamps) is a
//! function of the two seeded simulators, and the driver's step sequence is a
//! function of the quantum and the bridge's due times, so the whole run is a
//! pure function of the world seed ([`SimWorld::fingerprint`]).
//!
//! Precision: a message is delivered exactly at its due time iff the link's base
//! latency is at least `quantum` (asserted by [`PeerBridge::set_link`]);
//! otherwise it would be due inside an already-simulated step.
//!
//! **Only drive a world through `SimWorld`'s own methods.** Calling a member
//! `SimCluster`'s `run_for`/`dynamo` directly advances one simulator alone and
//! desynchronises the clocks (`SimWorld::run_for` re-syncs them, but bridge
//! deliveries in the gap are late).

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::{Clock, EnvExt, Nanos};
use animus_sim::SimEnv;
use futures::channel::oneshot;
use futures::future::{self, BoxFuture, Either};

use super::sim_cluster::{SimCluster, SimClusterHandle};

/// World-time lockstep granularity (also the minimum link latency).
pub(crate) const DEFAULT_QUANTUM: Duration = Duration::from_millis(5);
/// Client-only env id (`SimCluster::client_env`) the handler tasks run on.
const HANDLER_CLIENT: u64 = 900;
/// Budget of one world-level op (mirrors `SimCluster`'s `OP_BUDGET`).
const OP_BUDGET: Duration = Duration::from_secs(12);

fn dur_nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// FNV-1a 64 over a stream of byte chunks.
struct Fnv(u64);
impl Fnv {
    fn new() -> Self {
        Fnv(0xcbf2_9ce4_8422_2325)
    }
    fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 ^= u64::from(*b);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01B3);
        }
        self.write_sep();
    }
    fn write_sep(&mut self) {
        self.0 ^= 0xFF;
        self.0 = self.0.wrapping_mul(0x0000_0100_0000_01B3);
    }
}

pub(crate) use crate::mrec_peer::{PeerClient, PeerError};

/// One directed WAN link's model.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LinkConfig {
    /// Fixed one-way latency (>= the world quantum).
    pub(crate) latency: Duration,
    /// Extra uniformly drawn delay in `[0, jitter]` (also produces reordering).
    pub(crate) jitter: Duration,
    /// Per-mille probability a message is lost.
    pub(crate) loss_permille: u32,
    /// Per-mille probability a surviving message is delivered twice.
    pub(crate) dup_permille: u32,
}

impl LinkConfig {
    pub(crate) fn new(latency: Duration) -> Self {
        LinkConfig {
            latency,
            jitter: Duration::ZERO,
            loss_permille: 0,
            dup_permille: 0,
        }
    }
}

enum Pending {
    Request {
        from: usize,
        to: usize,
        id: u64,
        payload: Vec<u8>,
    },
    Response {
        /// The responder (the request's destination).
        from: usize,
        /// The original requester.
        to: usize,
        id: u64,
        payload: Vec<u8>,
    },
}

struct BridgeState {
    rng: u64,
    default_link: LinkConfig,
    links: BTreeMap<(usize, usize), LinkConfig>,
    /// Directed `(from, to)` pairs currently cut.
    cut: BTreeSet<(usize, usize)>,
    pending: BTreeMap<(u64, u64), Pending>,
    replies: BTreeMap<u64, oneshot::Sender<Vec<u8>>>,
    seq: u64,
    next_id: u64,
    quantum: Duration,
    /// Human-readable event log, part of the determinism fingerprint.
    log: Vec<String>,
}

impl BridgeState {
    fn link(&self, from: usize, to: usize) -> LinkConfig {
        self.links
            .get(&(from, to))
            .copied()
            .unwrap_or(self.default_link)
    }

    /// Draw a message's fate and enqueue it (0, 1 or 2 copies).
    fn enqueue(
        &mut self,
        now: Nanos,
        from: usize,
        to: usize,
        make: impl Fn() -> Pending,
        tag: &str,
        id: u64,
    ) {
        let cfg = self.link(from, to);
        if self.cut.contains(&(from, to)) {
            self.log.push(format!(
                "t={} {tag} id={id} {from}->{to} dropped:partition",
                now.0
            ));
            return;
        }
        if (splitmix(&mut self.rng) % 1000) < u64::from(cfg.loss_permille) {
            self.log.push(format!(
                "t={} {tag} id={id} {from}->{to} dropped:loss",
                now.0
            ));
            return;
        }
        let copies = 1 + u32::from((splitmix(&mut self.rng) % 1000) < u64::from(cfg.dup_permille));
        for _ in 0..copies {
            let jitter = if cfg.jitter.is_zero() {
                0
            } else {
                splitmix(&mut self.rng) % (dur_nanos(cfg.jitter) + 1)
            };
            let due = now.0 + dur_nanos(cfg.latency) + jitter;
            self.seq += 1;
            self.log
                .push(format!("t={} {tag} id={id} {from}->{to} due={due}", now.0));
            self.pending.insert((due, self.seq), make());
        }
    }
}

/// The WAN model between clusters. Cheap to clone (shared state).
#[derive(Clone)]
pub(crate) struct PeerBridge {
    inner: Arc<Mutex<BridgeState>>,
}

impl PeerBridge {
    fn new(seed: u64, quantum: Duration) -> Self {
        PeerBridge {
            inner: Arc::new(Mutex::new(BridgeState {
                rng: seed,
                default_link: LinkConfig::new(Duration::from_millis(40).max(quantum)),
                links: BTreeMap::new(),
                cut: BTreeSet::new(),
                pending: BTreeMap::new(),
                replies: BTreeMap::new(),
                seq: 0,
                next_id: 0,
                quantum,
                log: Vec::new(),
            })),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BridgeState> {
        self.inner.lock().expect("bridge poisoned")
    }

    /// Set the model of the directed link `from -> to`.
    pub(crate) fn set_link(&self, from: usize, to: usize, cfg: LinkConfig) {
        let mut st = self.lock();
        assert!(
            cfg.latency >= st.quantum,
            "link latency {:?} below the world quantum {:?}: delivery would not be exact",
            cfg.latency,
            st.quantum
        );
        st.links.insert((from, to), cfg);
    }

    /// Set the model of every link without its own override.
    pub(crate) fn set_default_link(&self, cfg: LinkConfig) {
        let mut st = self.lock();
        assert!(
            cfg.latency >= st.quantum,
            "link latency below the world quantum"
        );
        st.default_link = cfg;
    }

    /// Cut both directions between `a` and `b` (in-flight messages are dropped
    /// at delivery time too).
    pub(crate) fn partition(&self, a: usize, b: usize) {
        let mut st = self.lock();
        st.cut.insert((a, b));
        st.cut.insert((b, a));
    }

    /// Cut only `from -> to` (asymmetric partition).
    pub(crate) fn partition_one_way(&self, from: usize, to: usize) {
        self.lock().cut.insert((from, to));
    }

    /// Restore every link.
    pub(crate) fn heal(&self) {
        self.lock().cut.clear();
    }

    /// A client for requests issued from cluster `from`, whose tasks run on
    /// `env` (a `SimEnv` of that cluster's simulator).
    pub(crate) fn client(&self, from: usize, env: SimEnv) -> BridgeClient {
        BridgeClient {
            bridge: self.clone(),
            from,
            env,
        }
    }

    fn next_due(&self) -> Option<u64> {
        self.lock().pending.keys().next().map(|k| k.0)
    }

    fn take_due(&self, now: Nanos) -> Vec<(u64, Pending)> {
        let mut st = self.lock();
        let mut out = Vec::new();
        while let Some((&key, _)) = st.pending.iter().next() {
            if key.0 > now.0 {
                break;
            }
            let p = st.pending.remove(&key).expect("present");
            out.push((key.0, p));
        }
        out
    }

    /// A handler finished: ship its response back over the WAN.
    fn respond(&self, now: Nanos, responder: usize, requester: usize, id: u64, payload: Vec<u8>) {
        let mut st = self.lock();
        st.enqueue(
            now,
            responder,
            requester,
            || Pending::Response {
                from: responder,
                to: requester,
                id,
                payload: payload.clone(),
            },
            "resp",
            id,
        );
    }

    fn log_line(&self, line: String) {
        self.lock().log.push(line);
    }
}

/// [`PeerClient`] over a [`PeerBridge`]; lives in the sender's simulator.
#[derive(Clone)]
pub(crate) struct BridgeClient {
    bridge: PeerBridge,
    from: usize,
    env: SimEnv,
}

impl PeerClient for BridgeClient {
    fn call(
        &self,
        to: usize,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> BoxFuture<'static, Result<Vec<u8>, PeerError>> {
        let (bridge, from, env) = (self.bridge.clone(), self.from, self.env.clone());
        Box::pin(async move {
            let (tx, rx) = oneshot::channel();
            let id = {
                let mut st = bridge.lock();
                st.next_id += 1;
                let id = st.next_id;
                st.replies.insert(id, tx);
                st.enqueue(
                    env.now(),
                    from,
                    to,
                    || Pending::Request {
                        from,
                        to,
                        id,
                        payload: payload.clone(),
                    },
                    "req",
                    id,
                );
                id
            };
            let sleep = Box::pin(env.sleep(timeout));
            match future::select(rx, sleep).await {
                Either::Left((Ok(resp), _)) => Ok(resp),
                Either::Left((Err(_), _)) | Either::Right(_) => {
                    // Forget the slot so a late reply is discarded.
                    bridge.lock().replies.remove(&id);
                    Err(PeerError::Timeout)
                }
            }
        })
    }
}

/// A cluster's handler for an incoming peer request: runs as a task in that
/// cluster's own simulator with a handle to its nodes.
pub(crate) type PeerHandler =
    Arc<dyn Fn(SimClusterHandle, Vec<u8>) -> BoxFuture<'static, Vec<u8>> + Send + Sync>;

/// N independent clusters advanced in lockstep, plus the WAN bridge.
pub(crate) struct SimWorld {
    pub(crate) clusters: Vec<SimCluster>,
    bridge: PeerBridge,
    handlers: Vec<Option<PeerHandler>>,
    quantum: Duration,
    now: Nanos,
}

impl SimWorld {
    /// `n` clusters of `nodes` nodes each (RF `replication`), seeds derived
    /// from `seed`, clocks aligned before returning.
    pub(crate) fn new(seed: u64, n: usize, nodes: usize, replication: usize) -> Self {
        let mut mix = seed ^ 0xA11C_E000_0000_0000;
        let clusters: Vec<SimCluster> = (0..n)
            .map(|_| SimCluster::new(splitmix(&mut mix), nodes, replication))
            .collect();
        let bridge = PeerBridge::new(splitmix(&mut mix), DEFAULT_QUANTUM);
        let mut world = SimWorld {
            handlers: (0..n).map(|_| None).collect(),
            clusters,
            bridge,
            quantum: DEFAULT_QUANTUM,
            now: Nanos(0),
        };
        world.now = Nanos(
            world
                .clusters
                .iter()
                .map(|c| c.simulator().now().0)
                .max()
                .unwrap_or(0),
        );
        world.advance_all(world.now);
        world
    }

    /// Re-align every member simulator to the latest clock any of them reached.
    /// A setup phase may drive one member cluster alone (its own `run_for`,
    /// finalizing the cluster version, DDL) *before* the world carries any
    /// traffic; call this once afterwards so lockstep resumes from a common
    /// time. (Never call it while bridge traffic is in flight.)
    pub(crate) fn sync_clocks(&mut self) {
        self.now = Nanos(
            self.clusters
                .iter()
                .map(|c| c.simulator().now().0)
                .max()
                .unwrap_or(0)
                .max(self.now.0),
        );
        self.advance_all(self.now);
    }

    pub(crate) fn bridge(&self) -> &PeerBridge {
        &self.bridge
    }

    /// Install cluster `idx`'s handler for incoming peer requests.
    pub(crate) fn set_handler(&mut self, idx: usize, handler: PeerHandler) {
        self.handlers[idx] = Some(handler);
    }

    /// A [`PeerClient`] issuing from cluster `idx`.
    pub(crate) fn peer_client(&self, idx: usize) -> BridgeClient {
        self.bridge.client(idx, self.clusters[idx].client_env(1))
    }

    fn advance_all(&mut self, to: Nanos) {
        for c in &self.clusters {
            let mut sim = c.simulator();
            sim.run_until(to);
        }
    }

    /// Deliver every bridge message due at or before `now`.
    fn pump(&mut self) {
        loop {
            let due = self.bridge.take_due(self.now);
            if due.is_empty() {
                return;
            }
            for (due_at, p) in due {
                match p {
                    Pending::Request {
                        from,
                        to,
                        id,
                        payload,
                    } => {
                        if self.bridge.lock().cut.contains(&(from, to)) {
                            self.bridge.log_line(format!(
                                "t={} req id={id} {from}->{to} dropped:partition@delivery",
                                self.now.0
                            ));
                            continue;
                        }
                        let Some(handler) = self.handlers[to].clone() else {
                            self.bridge.log_line(format!(
                                "t={} req id={id} {from}->{to} dropped:no-handler",
                                self.now.0
                            ));
                            continue;
                        };
                        self.bridge.log_line(format!(
                            "t={} req id={id} {from}->{to} delivered(due={due_at})",
                            self.now.0
                        ));
                        let bridge = self.bridge.clone();
                        let env = self.clusters[to].client_env(HANDLER_CLIENT);
                        let env2 = env.clone();
                        let handle = self.clusters[to].handle();
                        env.spawn_task(async move {
                            let resp = handler(handle, payload).await;
                            bridge.respond(env2.now(), to, from, id, resp);
                        });
                    }
                    Pending::Response {
                        from,
                        to,
                        id,
                        payload,
                    } => {
                        if self.bridge.lock().cut.contains(&(from, to)) {
                            self.bridge.log_line(format!(
                                "t={} resp id={id} {from}->{to} dropped:partition@delivery",
                                self.now.0
                            ));
                            continue;
                        }
                        let slot = self.bridge.lock().replies.remove(&id);
                        self.bridge.log_line(format!(
                            "t={} resp id={id} {from}->{to} delivered(due={due_at}) waiter={}",
                            self.now.0,
                            slot.is_some()
                        ));
                        if let Some(tx) = slot {
                            let _ = tx.send(payload);
                        }
                    }
                }
            }
        }
    }

    /// Advance every cluster by `dur` of virtual time in lockstep, pumping the
    /// bridge at every step and at every due time.
    pub(crate) fn run_for(&mut self, dur: Duration) {
        let target = self.now.0 + dur_nanos(dur);
        self.pump();
        while self.now.0 < target {
            let mut step = (self.now.0 + dur_nanos(self.quantum)).min(target);
            if let Some(due) = self.bridge.next_due()
                && due > self.now.0
            {
                step = step.min(due);
            }
            self.advance_all(Nanos(step));
            self.now = Nanos(step);
            self.pump();
        }
    }

    /// Spawn `fut` on cluster `idx`'s node `node` env and drive the world until
    /// it resolves (`None` on `budget`).
    pub(crate) fn drive<T, F>(
        &mut self,
        idx: usize,
        node: u64,
        budget: Duration,
        fut: F,
    ) -> Option<T>
    where
        T: Send + 'static,
        F: Future<Output = T> + Send + 'static,
    {
        let env = self.clusters[idx].handle().env(node);
        let slot: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
        let out = slot.clone();
        env.spawn_task(async move {
            let r = fut.await;
            *out.lock().expect("slot poisoned") = Some(r);
        });
        let mut waited = Duration::ZERO;
        while waited < budget {
            self.run_for(self.quantum);
            waited += self.quantum;
            if let Some(r) = slot.lock().expect("slot poisoned").take() {
                return Some(r);
            }
        }
        None
    }

    /// DynamoDB-wire request against cluster `idx`'s node `node`.
    pub(crate) fn dynamo(&mut self, idx: usize, node: u64, op: &str, body: &str) -> (u16, String) {
        let handle = self.clusters[idx].handle();
        let (target, body) = (format!("DynamoDB_20120810.{op}"), body.to_owned());
        self.drive(idx, node, OP_BUDGET, async move {
            handle.dynamo(node, &target, body.as_bytes()).await
        })
        .unwrap_or((
            500,
            format!("dynamo {op} on cluster {idx} node {node} timed out"),
        ))
    }

    /// One cross-cluster request `from -> to`; returns the result and the
    /// virtual round-trip time.
    pub(crate) fn peer_call(
        &mut self,
        from: usize,
        to: usize,
        payload: &[u8],
        timeout: Duration,
    ) -> (Result<Vec<u8>, PeerError>, Duration) {
        let client = self.peer_client(from);
        let payload = payload.to_vec();
        let t0 = self.now;
        let r = self
            .drive(from, 0, timeout + self.quantum * 4, async move {
                client.call(to, payload, timeout).await
            })
            .unwrap_or(Err(PeerError::Timeout));
        // `drive` returns at the next quantum boundary after resolution.
        (r, Duration::from_nanos(self.now.0 - t0.0))
    }

    /// A hash of everything observable: both simulators' traces and stats, the
    /// bridge log, the world clock. Equal seeds must give equal fingerprints.
    pub(crate) fn fingerprint(&self) -> u64 {
        let mut h = Fnv::new();
        h.write(&self.now.0.to_le_bytes());
        for c in &self.clusters {
            let sim = c.simulator();
            for line in sim.trace_lines() {
                h.write(line.as_bytes());
            }
            let s = sim.stats();
            h.write(&s.task_polls.to_le_bytes());
            h.write(&s.timer_fires.to_le_bytes());
            h.write_sep();
        }
        for line in &self.bridge.lock().log {
            h.write(line.as_bytes());
        }
        h.0
    }

    /// The bridge event log (diagnostics, assertions).
    pub(crate) fn bridge_log(&self) -> Vec<String> {
        self.bridge.lock().log.clone()
    }
}
