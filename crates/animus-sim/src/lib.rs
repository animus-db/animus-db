//! Deterministic simulator for AnimusDB.
//!
//! A [`Simulator`] owns a single shared `SimState` and hands out one [`SimEnv`]
//! handle per node. Every source of nondeterminism — time, randomness, the
//! network, disk, and task scheduling — is driven from this one place, so an
//! entire distributed run is a pure function of its seed.
//!
//! The loop ([`Simulator::run`]) repeatedly: (1) polls every ready task to
//! quiescence, then (2) advances virtual time to the earliest scheduled event (a
//! timer firing or a message delivery) and dispatches it, which wakes tasks.
//! When there are no ready tasks and no scheduled events, the run is quiescent
//! and returns.
//!
//! Fault injection — [`partition`](Simulator::partition),
//! [`heal`](Simulator::heal), [`crash`](Simulator::crash),
//! [`restart`](Simulator::restart), [`stop`](Simulator::stop) (process exit),
//! the [`NetConfig`] delay/drop model, and the [`DiskConfig`] disk fault model
//! (injected I/O errors, torn crash tails, corruption —
//! [`corrupt_durable`](Simulator::corrupt_durable) for at-rest corruption) —
//! is all reproducible from the seed. A recorded [`trace`](Simulator::trace)
//! is byte-identical across repeated runs of the same scenario and seed.
//!
//! See `docs/adr/0003-deterministic-simulation.md`.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use animus_env::{
    BoxFuture, Clock, Coresident, Disk, Env, Envelope, Nanos, Network, NodeId, PRIMARY_STREAM,
    Rng as RngTrait, Spawner,
};
use futures::task::ArcWake;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;

type TaskId = u64;
type Seq = u64;
type TimerId = u64;

/// Per-file disk state, distinguishing durable (synced) bytes from buffered
/// (un-synced) bytes. A crash drops the buffer; the durable prefix survives.
#[derive(Default, Clone)]
struct FileState {
    durable: Vec<u8>,
    buffered: Vec<u8>,
}

/// A scheduled future event on the virtual timeline.
enum Event {
    /// A `sleep` timer fires.
    Timer(TimerId),
    /// A message is delivered to a node's inbox (on `env.stream`, ADR 0026).
    Deliver { to: NodeId, env: Envelope },
}

/// Network delay and drop model. Delay jitter and drop sampling draw from the
/// simulation RNG, so they are reproducible from the seed.
#[derive(Clone)]
pub struct NetConfig {
    /// Minimum one-way delivery delay.
    pub base_delay: Duration,
    /// Maximum additional uniform jitter on top of `base_delay`.
    pub max_jitter: Duration,
    /// A message is dropped when `rng.next_u64() < drop_threshold`.
    drop_threshold: u64,
}

impl Default for NetConfig {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_millis(1),
            max_jitter: Duration::from_millis(4),
            drop_threshold: 0,
        }
    }
}

impl NetConfig {
    /// Set the independent per-message drop probability in `[0.0, 1.0]`.
    pub fn set_drop_prob(&mut self, p: f64) {
        self.drop_threshold = (p.clamp(0.0, 1.0) * (u64::MAX as f64)) as u64;
    }
}

/// Disk fault-injection model. **All knobs default off**: with the default
/// config the disk draws no RNG and emits no trace event, so every run is
/// byte-identical to one on a simulator without a disk model at all. When a
/// knob is on, every sample (error draw, tear point, corrupted byte) comes
/// from the simulation RNG, so fault schedules are a pure function of the
/// seed. Set globally with [`Simulator::set_disk_config`] or per node with
/// [`Simulator::set_disk_config_for`] (mirrors the [`NetConfig`] pattern).
#[derive(Clone, Default)]
pub struct DiskConfig {
    /// An `append`/`sync`/`read`/`read_at`/`replace` fails (with an injected
    /// `io::Error`, and no state change) when `rng.next_u64() < error_threshold`.
    /// Metadata ops (`size`/`remove`/`list`) are never injected.
    error_threshold: u64,
    /// On [`Simulator::crash`], keep a seed-chosen **strict prefix** of each
    /// file's un-synced buffered bytes (instead of dropping the whole buffer
    /// atomically), moving it into the durable image — modelling a write torn
    /// mid-record by a power loss. At least one buffered byte is always lost
    /// (it is a tear, not a completed write); the previously durable prefix is
    /// untouched.
    pub torn_tail_on_crash: bool,
    /// With [`torn_tail_on_crash`](Self::torn_tail_on_crash): additionally
    /// flip one seed-chosen byte inside the retained (torn) region — modelling
    /// a garbled, not merely truncated, final record. No effect on files whose
    /// tear kept zero bytes, and never touches previously durable bytes.
    pub corrupt_on_crash: bool,
}

impl DiskConfig {
    /// Set the independent per-op disk error probability in `[0.0, 1.0]`.
    pub fn set_error_prob(&mut self, p: f64) {
        self.error_threshold = (p.clamp(0.0, 1.0) * (u64::MAX as f64)) as u64;
    }
}

/// One recorded line of the simulation history. The `Display` form is stable and
/// is what the byte-identical-trace guarantee is about.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TraceEvent {
    /// A task was spawned.
    Spawn { task: TaskId },
    /// A node handed a message to the network.
    Send {
        t: u64,
        from: NodeId,
        to: NodeId,
        stream: u64,
        len: usize,
    },
    /// A message was delivered to a node's inbox.
    Deliver {
        t: u64,
        from: NodeId,
        to: NodeId,
        stream: u64,
        len: usize,
    },
    /// A message was dropped (lossy link, partition, or crashed target).
    Drop {
        t: u64,
        from: NodeId,
        to: NodeId,
        stream: u64,
        reason: &'static str,
    },
    /// A sleep timer fired.
    Timer { t: u64, id: TimerId },
    /// A disk op failed with an injected error ([`DiskConfig`] error rate).
    DiskFault {
        t: u64,
        node: NodeId,
        op: &'static str,
        file: String,
    },
    /// A crash tore a file's un-synced tail: `kept` buffered bytes were
    /// retained (now durable), `dropped` were lost ([`DiskConfig::torn_tail_on_crash`]).
    DiskTear {
        t: u64,
        node: NodeId,
        file: String,
        kept: usize,
        dropped: usize,
    },
    /// One durable byte of a file was corrupted (bit-flipped), either by
    /// [`DiskConfig::corrupt_on_crash`] or [`Simulator::corrupt_durable`].
    DiskCorrupt {
        t: u64,
        node: NodeId,
        file: String,
        offset: u64,
    },
}

impl std::fmt::Display for TraceEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TraceEvent::Spawn { task } => write!(f, "SPAWN task={task}"),
            TraceEvent::Send {
                t,
                from,
                to,
                stream,
                len,
            } => {
                write!(f, "t={t} SEND {from}->{to} stream={stream} len={len}")
            }
            TraceEvent::Deliver {
                t,
                from,
                to,
                stream,
                len,
            } => {
                write!(f, "t={t} DELIVER {from}->{to} stream={stream} len={len}")
            }
            TraceEvent::Drop {
                t,
                from,
                to,
                stream,
                reason,
            } => {
                write!(f, "t={t} DROP {from}->{to} stream={stream} ({reason})")
            }
            TraceEvent::Timer { t, id } => write!(f, "t={t} TIMER id={id}"),
            TraceEvent::DiskFault { t, node, op, file } => {
                write!(f, "t={t} DISKFAULT node={node} op={op} file={file}")
            }
            TraceEvent::DiskTear {
                t,
                node,
                file,
                kept,
                dropped,
            } => {
                write!(
                    f,
                    "t={t} DISKTEAR node={node} file={file} kept={kept} dropped={dropped}"
                )
            }
            TraceEvent::DiskCorrupt {
                t,
                node,
                file,
                offset,
            } => {
                write!(
                    f,
                    "t={t} DISKCORRUPT node={node} file={file} offset={offset}"
                )
            }
        }
    }
}

/// The single shared mutable state of a simulation run.
struct SimState {
    clock: u64,
    rng: ChaCha8Rng,
    net: NetConfig,
    disk_cfg: DiskConfig,
    // Per-node overrides of the global disk fault model.
    node_disk_cfg: BTreeMap<NodeId, DiskConfig>,

    next_task_id: TaskId,
    // `None` while a task's future is checked out for polling.
    tasks: BTreeMap<TaskId, Option<BoxFuture<'static, ()>>>,
    // Which node spawned each task, so `stop(node)` can drop a node's tasks.
    task_owner: BTreeMap<TaskId, NodeId>,

    next_seq: Seq,
    timeline: BTreeMap<(u64, Seq), Event>,

    next_timer_id: TimerId,
    timer_wakers: BTreeMap<TimerId, Waker>,

    nodes: BTreeSet<NodeId>,
    // Keyed by `(node, stream)` (ADR 0026): a node's inbox is now multiple
    // independently-addressable streams, each still single-consumer. Every
    // pre-multiplexing caller uses `PRIMARY_STREAM` via the `Network::send`/
    // `recv` defaults, so this is a transparent generalization of the old
    // single-stream-per-node inbox.
    inboxes: BTreeMap<(NodeId, u64), VecDeque<Envelope>>,
    recv_wakers: BTreeMap<(NodeId, u64), Waker>,

    disks: BTreeMap<(NodeId, String), FileState>,

    // Directed blocked pairs: `(a, b)` blocks delivery from `a` to `b`.
    partitions: BTreeSet<(NodeId, NodeId)>,
    crashed: BTreeSet<NodeId>,

    trace: Vec<TraceEvent>,
}

impl SimState {
    /// The effective disk fault model for `node`: its override, else the global.
    fn disk_cfg_for(&self, node: NodeId) -> &DiskConfig {
        self.node_disk_cfg.get(&node).unwrap_or(&self.disk_cfg)
    }

    /// Sample error injection for one disk op on `node`. Draws RNG **only**
    /// when the effective error rate is non-zero, so the default (off) config
    /// perturbs neither the RNG stream nor the trace. On a hit, records a
    /// trace event and returns the `io::Error` the op must surface; the op
    /// must make **no** state change (a cleanly failed I/O call).
    fn inject_disk_fault(
        &mut self,
        node: NodeId,
        op: &'static str,
        file: &str,
    ) -> Option<std::io::Error> {
        let threshold = self.disk_cfg_for(node).error_threshold;
        if threshold == 0 || self.rng.next_u64() >= threshold {
            return None;
        }
        let t = self.clock;
        self.trace.push(TraceEvent::DiskFault {
            t,
            node,
            op,
            file: file.to_owned(),
        });
        Some(std::io::Error::other(format!(
            "sim injected disk fault: {op} {file} (node {node})"
        )))
    }
}

/// State shared between the simulator and every task waker / env handle.
struct Shared {
    state: Mutex<SimState>,
    ready: Mutex<VecDeque<TaskId>>,
}

impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, SimState> {
        self.state.lock().expect("sim state poisoned")
    }

    fn push_ready(&self, task: TaskId) {
        self.ready
            .lock()
            .expect("ready queue poisoned")
            .push_back(task);
    }
}

/// Waker that, when invoked, marks its task ready. Holds a `Weak` to avoid a
/// reference cycle (wakers are stored inside the state they point back to).
struct TaskWaker {
    shared: Weak<Shared>,
    task: TaskId,
}

impl ArcWake for TaskWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        if let Some(shared) = arc_self.shared.upgrade() {
            shared.push_ready(arc_self.task);
        }
    }
}

/// The deterministic simulator. Construct with a seed, register nodes via
/// [`env`](Simulator::env), spawn work, then drive with [`run`](Simulator::run).
///
/// **`Clone`** hands out another handle to the SAME shared simulated world
/// (it clones the inner `Arc`, exactly like [`SimEnv`]'s own `Clone` already
/// does) — not a fork. This is what lets a test's spawned "driver" task carry
/// its own `Simulator` handle to call fault-injection methods (`stop`,
/// `crash`, `partition_pair`, `heal`, `env`; all `&self`) from *inside* an
/// async scenario script, while the outer, synchronous test code keeps its
/// own handle to drive [`run_for`](Simulator::run_for)/
/// [`run_until`](Simulator::run_until) (the only `&mut self` methods — no
/// field they touch is exclusive to one handle, so nothing is lost by having
/// more than one).
#[derive(Clone)]
pub struct Simulator {
    shared: Arc<Shared>,
    seed: u64,
}

impl Simulator {
    /// Create a simulator driven by `seed`.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        let state = SimState {
            clock: 0,
            rng: ChaCha8Rng::seed_from_u64(seed),
            net: NetConfig::default(),
            disk_cfg: DiskConfig::default(),
            node_disk_cfg: BTreeMap::new(),
            next_task_id: 0,
            tasks: BTreeMap::new(),
            task_owner: BTreeMap::new(),
            next_seq: 0,
            timeline: BTreeMap::new(),
            next_timer_id: 0,
            timer_wakers: BTreeMap::new(),
            nodes: BTreeSet::new(),
            inboxes: BTreeMap::new(),
            recv_wakers: BTreeMap::new(),
            disks: BTreeMap::new(),
            partitions: BTreeSet::new(),
            crashed: BTreeSet::new(),
            trace: Vec::new(),
        };
        Self {
            shared: Arc::new(Shared {
                state: Mutex::new(state),
                ready: Mutex::new(VecDeque::new()),
            }),
            seed,
        }
    }

    /// The seed driving this run. Print it on failure so a run can be replayed.
    #[must_use]
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Obtain (registering if necessary) the [`SimEnv`] handle for `node`.
    #[must_use]
    pub fn env(&self, node: NodeId) -> SimEnv {
        let mut st = self.shared.lock();
        st.nodes.insert(node);
        st.inboxes.entry((node, PRIMARY_STREAM)).or_default();
        SimEnv {
            shared: Arc::clone(&self.shared),
            node_id: node,
        }
    }

    /// Replace the network delay/drop model.
    pub fn set_net_config(&self, cfg: NetConfig) {
        self.shared.lock().net = cfg;
    }

    /// Replace the **global** disk fault model ([`DiskConfig`]; default: no
    /// faults). A per-node override set via
    /// [`set_disk_config_for`](Self::set_disk_config_for) takes precedence.
    pub fn set_disk_config(&self, cfg: DiskConfig) {
        self.shared.lock().disk_cfg = cfg;
    }

    /// Set a **per-node** disk fault model, overriding the global one for
    /// `node` (so a test can make one replica's disk flaky while the rest stay
    /// healthy).
    pub fn set_disk_config_for(&self, node: NodeId, cfg: DiskConfig) {
        self.shared.lock().node_disk_cfg.insert(node, cfg);
    }

    /// Flip (bit-invert) one **durable** byte of `file` on `node`'s disk at
    /// `offset`, modelling at-rest media corruption of already-synced data —
    /// the fault class per-block checksums exist to catch. Returns whether a
    /// durable byte existed at `offset` (`false` means nothing was changed).
    /// Deterministic: draws no RNG; records a [`TraceEvent::DiskCorrupt`].
    pub fn corrupt_durable(&self, node: NodeId, file: &str, offset: u64) -> bool {
        let mut guard = self.shared.lock();
        let st = &mut *guard;
        let t = st.clock;
        let Some(f) = st.disks.get_mut(&(node, file.to_owned())) else {
            return false;
        };
        let Some(b) = f.durable.get_mut(offset as usize) else {
            return false;
        };
        *b ^= 0xFF;
        st.trace.push(TraceEvent::DiskCorrupt {
            t,
            node,
            file: file.to_owned(),
            offset,
        });
        true
    }

    /// Block delivery in the direction `from -> to`. Use
    /// [`partition_pair`](Simulator::partition_pair) for a symmetric split.
    pub fn partition(&self, from: NodeId, to: NodeId) {
        self.shared.lock().partitions.insert((from, to));
    }

    /// Symmetrically partition `a` and `b` from each other.
    pub fn partition_pair(&self, a: NodeId, b: NodeId) {
        let mut st = self.shared.lock();
        st.partitions.insert((a, b));
        st.partitions.insert((b, a));
    }

    /// Heal any partition between `from` and `to` (both directions).
    pub fn heal(&self, from: NodeId, to: NodeId) {
        let mut st = self.shared.lock();
        st.partitions.remove(&(from, to));
        st.partitions.remove(&(to, from));
    }

    /// Crash `node`: drop its un-synced disk bytes and its volatile in-memory
    /// inbox. Messages later delivered to a crashed node are dropped until it
    /// [`restart`](Self::restart)s.
    ///
    /// With the default [`DiskConfig`] the whole un-synced buffer of every file
    /// is dropped atomically (and no RNG is drawn). With
    /// [`DiskConfig::torn_tail_on_crash`] each file with buffered bytes instead
    /// retains a seed-chosen **strict prefix** of them (now durable — those
    /// bytes did reach the platter before the power cut), modelling a torn
    /// final record; [`DiskConfig::corrupt_on_crash`] additionally flips one
    /// seed-chosen byte inside that retained region. Files are processed in
    /// `BTreeMap` (name) order, so the RNG draws — and therefore the whole
    /// fault outcome — are a pure function of the seed.
    pub fn crash(&self, node: NodeId) {
        let mut guard = self.shared.lock();
        let st = &mut *guard;
        st.crashed.insert(node);
        // Clear every stream's inbox for this node (ADR 0026): a crashed node's
        // whole inbox is volatile, not just its primary stream's.
        let inbox_keys: Vec<_> = st
            .inboxes
            .keys()
            .filter(|(n, _)| *n == node)
            .cloned()
            .collect();
        for k in inbox_keys {
            if let Some(inbox) = st.inboxes.get_mut(&k) {
                inbox.clear();
            }
        }
        let waker_keys: Vec<_> = st
            .recv_wakers
            .keys()
            .filter(|(n, _)| *n == node)
            .cloned()
            .collect();
        for k in waker_keys {
            st.recv_wakers.remove(&k);
        }
        let (torn, corrupt) = {
            let cfg = st.disk_cfg_for(node);
            (cfg.torn_tail_on_crash, cfg.corrupt_on_crash)
        };
        let keys: Vec<_> = st
            .disks
            .keys()
            .filter(|(n, _)| *n == node)
            .cloned()
            .collect();
        let t = st.clock;
        for k in keys {
            let Some(f) = st.disks.get_mut(&k) else {
                continue;
            };
            if f.buffered.is_empty() {
                continue;
            }
            if !torn {
                f.buffered.clear();
                continue;
            }
            // Tear: keep a strict prefix (at least one buffered byte is always
            // lost — this models an interrupted write, not a completed one).
            // The retained prefix becomes durable: it survives the restart.
            let kept = gen_below(&mut st.rng, f.buffered.len() as u64) as usize;
            let dropped = f.buffered.len() - kept;
            f.durable.extend_from_slice(&f.buffered[..kept]);
            f.buffered.clear();
            st.trace.push(TraceEvent::DiskTear {
                t,
                node,
                file: k.1.clone(),
                kept,
                dropped,
            });
            if corrupt && kept > 0 {
                let region_start = f.durable.len() - kept;
                let offset = region_start + gen_below(&mut st.rng, kept as u64) as usize;
                f.durable[offset] ^= 0xFF;
                st.trace.push(TraceEvent::DiskCorrupt {
                    t,
                    node,
                    file: k.1,
                    offset: offset as u64,
                });
            }
        }
    }

    /// Bring a crashed `node` back. Its durable disk state remains.
    ///
    /// Crashing a node drops the waker of any task parked on `recv()` (the inbox
    /// is volatile), so nothing would re-poll that task — a later delivery would
    /// find no registered `recv` waker and the task would never wake. To repair
    /// this, `restart` re-arms every task owned by the node: it marks them ready
    /// so the run loop re-polls them, which re-registers their `recv` waker.
    /// Re-polling a parked `Recv` on an empty inbox is a side-effect-free,
    /// idempotent reinstallation — it draws no RNG and schedules no timeline
    /// event, so determinism is preserved. Tasks are re-armed in ascending id
    /// order (the `BTreeMap` iteration order), keeping the ready-queue order a
    /// deterministic function of the seed.
    pub fn restart(&self, node: NodeId) {
        let tasks: Vec<TaskId> = {
            let mut st = self.shared.lock();
            st.crashed.remove(&node);
            st.task_owner
                .iter()
                .filter(|&(_, &owner)| owner == node)
                .map(|(&task, _)| task)
                .collect()
        };
        for task in tasks {
            self.shared.push_ready(task);
        }
    }

    /// Stop `node` as if its process exited: drop all of its tasks (their
    /// in-memory state — e.g. a node's `RaftCore` and driver loop — is gone) and
    /// its volatile state (inbox, un-synced disk bytes). **Durable** (synced)
    /// disk survives. A fresh node started afterward on the same node id reads
    /// that disk and recovers — modelling a real process restart.
    ///
    /// Unlike [`crash`](Self::crash), this does not mute or set the node
    /// `crashed`; the node simply has no tasks until one is started again.
    pub fn stop(&self, node: NodeId) {
        let mut st = self.shared.lock();
        let task_ids: Vec<TaskId> = st
            .task_owner
            .iter()
            .filter(|&(_, &owner)| owner == node)
            .map(|(&task, _)| task)
            .collect();
        for task in task_ids {
            st.tasks.remove(&task);
            st.task_owner.remove(&task);
        }
        // Volatile state dies with the process; durable disk is kept.
        // Clear every stream's inbox for this node (ADR 0026), mirroring `crash`.
        let inbox_keys: Vec<_> = st
            .inboxes
            .keys()
            .filter(|(n, _)| *n == node)
            .cloned()
            .collect();
        for k in inbox_keys {
            if let Some(inbox) = st.inboxes.get_mut(&k) {
                inbox.clear();
            }
        }
        let waker_keys: Vec<_> = st
            .recv_wakers
            .keys()
            .filter(|(n, _)| *n == node)
            .cloned()
            .collect();
        for k in waker_keys {
            st.recv_wakers.remove(&k);
        }
        let keys: Vec<_> = st
            .disks
            .keys()
            .filter(|(n, _)| *n == node)
            .cloned()
            .collect();
        for k in keys {
            if let Some(f) = st.disks.get_mut(&k) {
                f.buffered.clear();
            }
        }
    }

    /// The recorded history of this run.
    #[must_use]
    pub fn trace(&self) -> Vec<TraceEvent> {
        self.shared.lock().trace.clone()
    }

    /// The recorded history rendered as stable text lines.
    #[must_use]
    pub fn trace_lines(&self) -> Vec<String> {
        self.shared
            .lock()
            .trace
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    /// Current virtual time.
    #[must_use]
    pub fn now(&self) -> Nanos {
        Nanos(self.shared.lock().clock)
    }

    /// Run until quiescence: no ready tasks and no scheduled events remain.
    ///
    /// Do not use this for protocols with perpetual timers (e.g. Raft
    /// heartbeats), which never quiesce; use [`run_until`](Self::run_until) or
    /// [`run_for`](Self::run_for) instead.
    pub fn run(&mut self) {
        self.run_until_quiescent(usize::MAX);
    }

    /// Run until virtual time reaches `deadline` (or the run quiesces earlier),
    /// then advance the clock to exactly `deadline`. Events scheduled after
    /// `deadline` are left pending.
    pub fn run_until(&mut self, deadline: Nanos) {
        loop {
            while let Some(task) = self.pop_ready() {
                self.poll_task(task);
            }
            let next_key = {
                let st = self.shared.lock();
                st.timeline.keys().next().copied()
            };
            match next_key {
                Some(key) if key.0 <= deadline.0 => self.fire_event(key),
                _ => {
                    let mut st = self.shared.lock();
                    st.clock = st.clock.max(deadline.0);
                    return;
                }
            }
        }
    }

    /// Run for `dur` of virtual time from now.
    pub fn run_for(&mut self, dur: Duration) {
        let deadline = Nanos(self.shared.lock().clock.saturating_add(dur_nanos(dur)));
        self.run_until(deadline);
    }

    /// Run until quiescence or until `max_steps` events have fired (a guard
    /// against a scenario that never settles). Returns `true` if quiescent.
    pub fn run_until_quiescent(&mut self, max_steps: usize) -> bool {
        let mut steps = 0;
        loop {
            // 1. Drain all ready tasks.
            while let Some(task) = self.pop_ready() {
                self.poll_task(task);
            }
            // 2. No ready tasks: fire the earliest scheduled event, which wakes
            //    tasks for the next drain.
            let next_key = {
                let st = self.shared.lock();
                st.timeline.keys().next().copied()
            };
            match next_key {
                Some(key) => {
                    self.fire_event(key);
                    steps += 1;
                    if steps >= max_steps {
                        return false;
                    }
                }
                None => return true,
            }
        }
    }

    fn pop_ready(&self) -> Option<TaskId> {
        self.shared
            .ready
            .lock()
            .expect("ready queue poisoned")
            .pop_front()
    }

    fn poll_task(&self, task: TaskId) {
        // Check the future out of the map so the poll can re-enter the state
        // lock (e.g. via `env.send`) without deadlocking.
        let fut = {
            let mut st = self.shared.lock();
            st.tasks.get_mut(&task).and_then(Option::take)
        };
        let Some(mut fut) = fut else { return };

        let waker = futures::task::waker(Arc::new(TaskWaker {
            shared: Arc::downgrade(&self.shared),
            task,
        }));
        let mut cx = Context::from_waker(&waker);
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(()) => {
                self.shared.lock().tasks.remove(&task);
            }
            Poll::Pending => {
                if let Some(slot) = self.shared.lock().tasks.get_mut(&task) {
                    *slot = Some(fut);
                }
            }
        }
    }

    fn fire_event(&self, key: (u64, Seq)) {
        let waker = {
            let mut st = self.shared.lock();
            let event = st.timeline.remove(&key).expect("event present");
            st.clock = st.clock.max(key.0);
            let t = st.clock;
            match event {
                Event::Timer(id) => {
                    st.trace.push(TraceEvent::Timer { t, id });
                    st.timer_wakers.remove(&id)
                }
                Event::Deliver { to, env } => {
                    let from = env.from;
                    let stream = env.stream;
                    if st.crashed.contains(&to) {
                        st.trace.push(TraceEvent::Drop {
                            t,
                            from,
                            to,
                            stream,
                            reason: "crashed",
                        });
                        None
                    } else if st.partitions.contains(&(from, to)) {
                        st.trace.push(TraceEvent::Drop {
                            t,
                            from,
                            to,
                            stream,
                            reason: "partition",
                        });
                        None
                    } else {
                        let len = env.payload.len();
                        st.trace.push(TraceEvent::Deliver {
                            t,
                            from,
                            to,
                            stream,
                            len,
                        });
                        st.inboxes.entry((to, stream)).or_default().push_back(env);
                        st.recv_wakers.remove(&(to, stream))
                    }
                }
            }
        };
        if let Some(w) = waker {
            w.wake();
        }
    }
}

/// A per-node handle into the simulation. Implements [`Env`]: cloning is cheap
/// and all clones for the same `node_id` share one inbox and one view of state.
#[derive(Clone)]
pub struct SimEnv {
    shared: Arc<Shared>,
    node_id: NodeId,
}

#[async_trait::async_trait]
impl Clock for SimEnv {
    fn now(&self) -> Nanos {
        Nanos(self.shared.lock().clock)
    }

    async fn sleep(&self, dur: Duration) {
        let deadline = self.shared.lock().clock.saturating_add(dur_nanos(dur));
        Sleep {
            shared: Arc::clone(&self.shared),
            deadline,
            timer: None,
        }
        .await;
    }
}

impl RngTrait for SimEnv {
    fn next_u64(&self) -> u64 {
        self.shared.lock().rng.next_u64()
    }

    fn fill_bytes(&self, dst: &mut [u8]) {
        self.shared.lock().rng.fill_bytes(dst);
    }
}

#[async_trait::async_trait]
impl Network for SimEnv {
    async fn send_stream(&self, to: NodeId, stream: u64, payload: Vec<u8>) {
        let mut st = self.shared.lock();
        let from = self.node_id;
        let t = st.clock;
        let len = payload.len();
        st.trace.push(TraceEvent::Send {
            t,
            from,
            to,
            stream,
            len,
        });

        // A crashed node produces no output: it is dead, not merely unreachable.
        if st.crashed.contains(&from) {
            st.trace.push(TraceEvent::Drop {
                t,
                from,
                to,
                stream,
                reason: "sender-crashed",
            });
            return;
        }

        // Independent random drop at send time.
        if st.net.drop_threshold > 0 && st.rng.next_u64() < st.net.drop_threshold {
            st.trace.push(TraceEvent::Drop {
                t,
                from,
                to,
                stream,
                reason: "lossy",
            });
            return;
        }

        let base = dur_nanos(st.net.base_delay);
        let jitter_max = dur_nanos(st.net.max_jitter);
        let jitter = if jitter_max == 0 {
            0
        } else {
            gen_below(&mut st.rng, jitter_max + 1)
        };
        let deliver_at = st.clock.saturating_add(base + jitter);
        let seq = st.next_seq;
        st.next_seq += 1;
        st.timeline.insert(
            (deliver_at, seq),
            Event::Deliver {
                to,
                env: Envelope {
                    from,
                    stream,
                    payload,
                },
            },
        );
    }

    async fn recv_stream(&self, stream: u64) -> Envelope {
        Recv {
            shared: Arc::clone(&self.shared),
            node: self.node_id,
            stream,
        }
        .await
    }
}

#[async_trait::async_trait]
impl Disk for SimEnv {
    async fn append(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        let mut st = self.shared.lock();
        if let Some(e) = st.inject_disk_fault(self.node_id, "append", file) {
            return Err(e);
        }
        let key = (self.node_id, file.to_owned());
        st.disks
            .entry(key)
            .or_default()
            .buffered
            .extend_from_slice(bytes);
        Ok(())
    }

    async fn sync(&self, file: &str) -> std::io::Result<()> {
        let mut st = self.shared.lock();
        if let Some(e) = st.inject_disk_fault(self.node_id, "sync", file) {
            return Err(e);
        }
        let key = (self.node_id, file.to_owned());
        if let Some(f) = st.disks.get_mut(&key) {
            let mut buffered = std::mem::take(&mut f.buffered);
            f.durable.append(&mut buffered);
        }
        Ok(())
    }

    async fn read(&self, file: &str) -> std::io::Result<Vec<u8>> {
        let mut st = self.shared.lock();
        if let Some(e) = st.inject_disk_fault(self.node_id, "read", file) {
            return Err(e);
        }
        let key = (self.node_id, file.to_owned());
        Ok(st.disks.get(&key).map_or_else(Vec::new, |f| {
            let mut out = f.durable.clone();
            out.extend_from_slice(&f.buffered);
            out
        }))
    }

    async fn read_at(&self, file: &str, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
        let mut st = self.shared.lock();
        if let Some(e) = st.inject_disk_fault(self.node_id, "read_at", file) {
            return Err(e);
        }
        let key = (self.node_id, file.to_owned());
        Ok(st.disks.get(&key).map_or_else(Vec::new, |f| {
            // The durable + buffered view, sliced — mirrors `read`. A crash clears
            // `buffered`, so an un-synced tail is correctly invisible afterward.
            let total = f.durable.len() + f.buffered.len();
            let start = (offset as usize).min(total);
            let end = start.saturating_add(len).min(total);
            (start..end)
                .map(|i| {
                    if i < f.durable.len() {
                        f.durable[i]
                    } else {
                        f.buffered[i - f.durable.len()]
                    }
                })
                .collect()
        }))
    }

    async fn size(&self, file: &str) -> std::io::Result<u64> {
        let st = self.shared.lock();
        let key = (self.node_id, file.to_owned());
        Ok(st
            .disks
            .get(&key)
            .map_or(0, |f| (f.durable.len() + f.buffered.len()) as u64))
    }

    async fn remove(&self, file: &str) -> std::io::Result<()> {
        let mut st = self.shared.lock();
        let key = (self.node_id, file.to_owned());
        st.disks.remove(&key);
        Ok(())
    }

    async fn replace(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        // Atomic under the state lock: durable jumps straight to `bytes`, with no
        // un-synced remainder. A crash keeps exactly the new contents. An injected
        // fault fails the swap cleanly (temp-file + rename semantics: the old
        // contents remain fully intact).
        let mut st = self.shared.lock();
        if let Some(e) = st.inject_disk_fault(self.node_id, "replace", file) {
            return Err(e);
        }
        let key = (self.node_id, file.to_owned());
        let f = st.disks.entry(key).or_default();
        f.durable = bytes.to_vec();
        f.buffered.clear();
        Ok(())
    }

    async fn list(&self) -> std::io::Result<Vec<String>> {
        let st = self.shared.lock();
        // `disks` is a BTreeMap keyed `(node, name)`, so a range from this node's
        // first possible key yields its file names already in lexicographic order.
        Ok(st
            .disks
            .range((self.node_id, String::new())..)
            .take_while(|((node, _), _)| *node == self.node_id)
            .map(|((_, name), _)| name.clone())
            .collect())
    }
}

impl Spawner for SimEnv {
    fn spawn(&self, fut: BoxFuture<'static, ()>) {
        let mut st = self.shared.lock();
        let task = st.next_task_id;
        st.next_task_id += 1;
        st.tasks.insert(task, Some(fut));
        st.task_owner.insert(task, self.node_id);
        st.trace.push(TraceEvent::Spawn { task });
        drop(st);
        self.shared.push_ready(task);
    }
}

impl Env for SimEnv {
    fn node_id(&self) -> NodeId {
        self.node_id
    }
}

impl Coresident for SimEnv {
    /// Mint a sibling handle on the same simulated node, bound to `id` with its
    /// own inbox. Registers `id` in the shared state exactly as
    /// [`Simulator::env`] would (idempotent `entry(..).or_default()`), so a
    /// component can create a co-resident protocol instance in band without the
    /// `Simulator` pre-allocating the id. Touches only the inbox/node maps — no
    /// RNG, no timeline event — so determinism is preserved.
    fn sibling(&self, id: NodeId) -> Self {
        {
            let mut st = self.shared.lock();
            st.nodes.insert(id);
            st.inboxes.entry((id, PRIMARY_STREAM)).or_default();
        }
        SimEnv {
            shared: Arc::clone(&self.shared),
            node_id: id,
        }
    }
}

/// Future that completes once virtual time reaches its deadline.
struct Sleep {
    shared: Arc<Shared>,
    deadline: u64,
    timer: Option<TimerId>,
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut st = self.shared.lock();
        if st.clock >= self.deadline {
            return Poll::Ready(());
        }
        match self.timer {
            Some(id) => {
                st.timer_wakers.insert(id, cx.waker().clone());
            }
            None => {
                let id = st.next_timer_id;
                st.next_timer_id += 1;
                st.timer_wakers.insert(id, cx.waker().clone());
                let seq = st.next_seq;
                st.next_seq += 1;
                let deadline = self.deadline;
                st.timeline.insert((deadline, seq), Event::Timer(id));
                drop(st);
                self.timer = Some(id);
            }
        }
        Poll::Pending
    }
}

/// Future that yields the next message addressed to a node on a given stream
/// (ADR 0026).
struct Recv {
    shared: Arc<Shared>,
    node: NodeId,
    stream: u64,
}

impl Future for Recv {
    type Output = Envelope;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Envelope> {
        let mut st = self.shared.lock();
        let key = (self.node, self.stream);
        if let Some(env) = st.inboxes.get_mut(&key).and_then(VecDeque::pop_front) {
            Poll::Ready(env)
        } else {
            st.recv_wakers.insert(key, cx.waker().clone());
            Poll::Pending
        }
    }
}

fn dur_nanos(d: Duration) -> u64 {
    d.as_nanos().min(u128::from(u64::MAX)) as u64
}

/// Lemire-debiased `[0, n)` draw directly on a `ChaCha8Rng` (used internally for
/// network jitter; mirrors `animus_env::Rng::gen_below`).
fn gen_below(rng: &mut ChaCha8Rng, n: u64) -> u64 {
    if n == 0 {
        return 0;
    }
    let mut x = rng.next_u64();
    let mut m = u128::from(x) * u128::from(n);
    let mut low = m as u64;
    if low < n {
        let threshold = n.wrapping_neg() % n;
        while low < threshold {
            x = rng.next_u64();
            m = u128::from(x) * u128::from(n);
            low = m as u64;
        }
    }
    (m >> 64) as u64
}
