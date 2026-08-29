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
//! [`pause`](Simulator::pause) (alive-but-frozen), the [`NetConfig`]
//! delay/drop/duplicate/corrupt model (global, per-node
//! [`set_net_config_for`](Simulator::set_net_config_for), or per-link
//! [`set_link_net_config`](Simulator::set_link_net_config)), and the
//! [`DiskConfig`] disk fault model (injected I/O errors including
//! ENOSPC, torn crash tails, corruption —
//! [`corrupt_durable`](Simulator::corrupt_durable) for at-rest corruption —
//! and fsync-acked-but-lost) — is all reproducible from the seed. Per-node
//! [`set_clock_skew_for`](Simulator::set_clock_skew_for) and
//! [`set_clock_drift_for`](Simulator::set_clock_drift_for) model a node whose
//! clock reads wrong, statically or progressively. A recorded
//! [`trace`](Simulator::trace) is byte-identical across repeated runs of the
//! same scenario and seed.
//!
//! See `docs/adr/0003-deterministic-simulation.md`.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use animus_env::{
    BoxFuture, Clock, Disk, Env, Envelope, Nanos, Network, NodeId, PRIMARY_STREAM, Rng as RngTrait,
    Spawner, UnixMillis,
};
use futures::task::ArcWake;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;

pub mod segment_store;
pub use segment_store::{SegmentFaultConfig, SimSegmentStore};

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

/// Network delay/drop/duplicate/corruption model. Every sample (drop,
/// duplicate, corrupt, heavy-tail selection, jitter) draws from the
/// simulation RNG, so a configured schedule is reproducible from the seed.
/// **Every non-default knob defaults off**: with a plain `NetConfig::default()`
/// (`drop_threshold == 0`, `duplicate_threshold == 0`, `corrupt_threshold ==
/// 0`, `heavy_tail_threshold == 0` — the state every prior `NetConfig`
/// already had), a send draws exactly the same RNG values in the same order
/// it did before these knobs existed: a drop roll (only if `drop_threshold >
/// 0`) then a jitter draw (only if `max_jitter > 0`) — see the doc on
/// [`Simulator::set_net_config`] for the full, extended draw order once any
/// of the new knobs are enabled.
///
/// Scope with [`Simulator::set_net_config`] (global),
/// [`Simulator::set_net_config_for`] (per-node, keyed on the **sender**), or
/// [`Simulator::set_link_net_config`] (per directed link) — mirrors the
/// [`DiskConfig`]/[`Simulator::set_disk_config_for`] pattern.
#[derive(Clone)]
pub struct NetConfig {
    /// Minimum one-way delivery delay.
    pub base_delay: Duration,
    /// Maximum additional uniform jitter on top of `base_delay`.
    pub max_jitter: Duration,
    /// With [`set_heavy_tail_prob`](Self::set_heavy_tail_prob) `> 0`: the
    /// jitter ceiling used instead of `max_jitter` on the draws that land in
    /// the heavy tail — an occasional much-slower message (a GC pause on the
    /// peer, a retried TCP segment) without raising the delay for the common
    /// case. Unused (never read) while the heavy-tail probability is 0.
    pub heavy_tail_max_jitter: Duration,
    /// A message is dropped when `rng.next_u64() < drop_threshold`.
    drop_threshold: u64,
    /// A surviving (non-dropped) message is corrupted (one payload byte
    /// bit-flipped) when `rng.next_u64() < corrupt_threshold`.
    corrupt_threshold: u64,
    /// A surviving message is additionally re-delivered — a duplicate, with
    /// its own independent delay draw — when
    /// `rng.next_u64() < duplicate_threshold`.
    duplicate_threshold: u64,
    /// Probability (per delay draw, so once for the original send and,
    /// independently, once more for a duplicate) that the jitter draw uses
    /// `heavy_tail_max_jitter` instead of `max_jitter`.
    heavy_tail_threshold: u64,
}

impl Default for NetConfig {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_millis(1),
            max_jitter: Duration::from_millis(4),
            heavy_tail_max_jitter: Duration::ZERO,
            drop_threshold: 0,
            corrupt_threshold: 0,
            duplicate_threshold: 0,
            heavy_tail_threshold: 0,
        }
    }
}

impl NetConfig {
    /// Set the independent per-message drop probability in `[0.0, 1.0]`.
    pub fn set_drop_prob(&mut self, p: f64) {
        self.drop_threshold = (p.clamp(0.0, 1.0) * (u64::MAX as f64)) as u64;
    }

    /// Set the independent per-message wire-payload-corruption probability in
    /// `[0.0, 1.0]` — the network analogue of [`DiskConfig`]'s at-rest
    /// corruption. On a hit, one seed-chosen byte of the payload is
    /// bit-flipped before the (possibly duplicated) delivery is scheduled; a
    /// zero-length payload cannot be corrupted (the roll still happens —
    /// determinism doesn't depend on payload length — but nothing is
    /// flipped and no [`TraceEvent::NetCorrupt`] is recorded).
    pub fn set_corrupt_prob(&mut self, p: f64) {
        self.corrupt_threshold = (p.clamp(0.0, 1.0) * (u64::MAX as f64)) as u64;
    }

    /// Set the independent per-message duplication probability in
    /// `[0.0, 1.0]`. On a hit, a surviving message is delivered **twice**:
    /// the duplicate carries the same (possibly corrupted) bytes but draws
    /// its **own independent delay** (including its own heavy-tail roll),
    /// so it can arrive before, at, or after the original — modelling a real
    /// duplicated packet's independent path through the network, rather than
    /// a same-instant echo.
    pub fn set_duplicate_prob(&mut self, p: f64) {
        self.duplicate_threshold = (p.clamp(0.0, 1.0) * (u64::MAX as f64)) as u64;
    }

    /// Set the probability, in `[0.0, 1.0]`, that a delay draw lands in the
    /// heavy tail (using [`heavy_tail_max_jitter`](Self::heavy_tail_max_jitter)
    /// instead of [`max_jitter`](Self::max_jitter)). Set
    /// `heavy_tail_max_jitter` alongside this (a plain field, like
    /// `base_delay`/`max_jitter`) — a nonzero probability with a zero ceiling
    /// is a no-op.
    pub fn set_heavy_tail_prob(&mut self, p: f64) {
        self.heavy_tail_threshold = (p.clamp(0.0, 1.0) * (u64::MAX as f64)) as u64;
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
    /// An `append`/`sync`/`read`/`read_at`/`replace` fails with a generic
    /// injected `io::Error` (`ErrorKind::Other`, and no state change) when
    /// the single per-op roll lands in `[enospc_threshold, enospc_threshold +
    /// error_threshold)`. Metadata ops (`size`/`remove`/`list`) are never
    /// injected. See [`enospc_threshold`](Self::enospc_threshold) for the
    /// ENOSPC-distinguishable sibling this shares its roll with.
    error_threshold: u64,
    /// Like [`error_threshold`](Self::error_threshold), but the injected
    /// error carries `ErrorKind::StorageFull` (ENOSPC) instead of `Other`, so
    /// a caller that branches on `ErrorKind` (as production code must, to
    /// tell "disk full" apart from a generic read/write failure) can be
    /// exercised under simulation. **One roll decides both**: a single
    /// `rng.next_u64()` per injectable op lands in `[0, enospc_threshold)`
    /// (ENOSPC), then `[enospc_threshold, enospc_threshold +
    /// error_threshold)` (generic), else no fault — so with the default
    /// `enospc_threshold == 0` this reduces to exactly the prior single-draw,
    /// single-comparison generic-error check (same draw, same outcome),
    /// keeping every existing `error_prob`-only config byte-identical.
    enospc_threshold: u64,
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
    /// Extra virtual-time latency injected into every `append`/`sync` call
    /// (issue #279 — modelling a slow real disk, e.g. to reproduce a livelock
    /// where a driver task blocks on a slow `fsync` past an election
    /// timeout). `None` (default) injects nothing, so a run with no configured
    /// delay is byte-identical to one on a `SimEnv` build predating this
    /// field. Unlike [`error_threshold`](Self::error_threshold)/
    /// [`torn_tail_on_crash`](Self::torn_tail_on_crash), this is a fixed
    /// latency, not a seed-sampled fault — it draws no RNG and perturbs no
    /// trace event ordering beyond the timer it schedules, so it composes
    /// cleanly with the other knobs.
    sync_delay: Option<Duration>,
    /// **fsync-acked-but-lost**: on a hit (`rng.next_u64() < fsync_lie_threshold`,
    /// sampled independently inside [`Disk::sync`], only after the
    /// `error_threshold`/`enospc_threshold` roll above has already missed —
    /// a call that fails outright never reaches this roll, since nothing was
    /// "acked" at all), `sync` still returns `Ok` but the buffered bytes it
    /// would normally move into the durable image are left exactly where
    /// they were: still buffered, still vulnerable to
    /// [`Simulator::crash`]'s un-synced-bytes handling (whole-buffer drop by
    /// default, or [`torn_tail_on_crash`](Self::torn_tail_on_crash)/
    /// [`corrupt_on_crash`](Self::corrupt_on_crash) if configured) — modelling
    /// a real filesystem that acks an `fsync` and then loses the write on
    /// power loss. Reads are unaffected (a lied-to `sync` is transparent to
    /// `read`/`read_at`, exactly like any other un-synced tail); only a
    /// following crash reveals the lie.
    fsync_lie_threshold: u64,
}

impl DiskConfig {
    /// Set the independent per-op disk error probability in `[0.0, 1.0]`.
    pub fn set_error_prob(&mut self, p: f64) {
        self.error_threshold = (p.clamp(0.0, 1.0) * (u64::MAX as f64)) as u64;
    }

    /// Set the independent per-op ENOSPC probability in `[0.0, 1.0]` — see
    /// [`enospc_threshold`](Self::enospc_threshold) for how this composes
    /// with [`set_error_prob`](Self::set_error_prob) on one shared roll.
    pub fn set_enospc_prob(&mut self, p: f64) {
        self.enospc_threshold = (p.clamp(0.0, 1.0) * (u64::MAX as f64)) as u64;
    }

    /// Inject `dur` of extra virtual-time latency into every subsequent
    /// `append`/`sync` call on this config (global or per-node — see
    /// [`Simulator::set_disk_config`]/[`Simulator::set_disk_config_for`]).
    /// Mirrors [`set_error_prob`](Self::set_error_prob)'s shape: a plain
    /// setter, no RNG involved.
    pub fn set_sync_delay(&mut self, dur: Duration) {
        self.sync_delay = Some(dur);
    }

    /// Set the independent per-`sync` fsync-acked-but-lost probability in
    /// `[0.0, 1.0]` — see [`fsync_lie_threshold`](Self::fsync_lie_threshold).
    pub fn set_fsync_lie_prob(&mut self, p: f64) {
        self.fsync_lie_threshold = (p.clamp(0.0, 1.0) * (u64::MAX as f64)) as u64;
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
    /// A surviving message was corrupted in transit: one payload byte was
    /// bit-flipped ([`NetConfig::set_corrupt_prob`]).
    NetCorrupt {
        t: u64,
        from: NodeId,
        to: NodeId,
        stream: u64,
        offset: u64,
    },
    /// A surviving message was additionally re-delivered as a duplicate, with
    /// its own independent delay draw ([`NetConfig::set_duplicate_prob`]).
    Duplicate {
        t: u64,
        from: NodeId,
        to: NodeId,
        stream: u64,
        len: usize,
    },
    /// A node was paused ([`Simulator::pause`]): frozen until virtual time
    /// reaches `until` — no timer owned by it fires and no message it sends
    /// leaves before then; messages addressed to it queue (deferred, not
    /// dropped) until then too.
    Pause { t: u64, node: NodeId, until: u64 },
    /// A disk op failed with an injected error ([`DiskConfig::error_threshold`]
    /// / [`DiskConfig::enospc_threshold`]). `kind` is `"enospc"` (the op
    /// failed with `ErrorKind::StorageFull`) or `"error"` (generic
    /// `ErrorKind::Other`).
    DiskFault {
        t: u64,
        node: NodeId,
        op: &'static str,
        file: String,
        kind: &'static str,
    },
    /// A `sync` returned `Ok` without actually moving its buffered bytes into
    /// the durable image ([`DiskConfig::set_fsync_lie_prob`]) — the ack was a
    /// lie; those bytes are still exposed to a following crash exactly like
    /// any other un-synced tail.
    FsyncLie { t: u64, node: NodeId, file: String },
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
            TraceEvent::NetCorrupt {
                t,
                from,
                to,
                stream,
                offset,
            } => {
                write!(
                    f,
                    "t={t} NETCORRUPT {from}->{to} stream={stream} offset={offset}"
                )
            }
            TraceEvent::Duplicate {
                t,
                from,
                to,
                stream,
                len,
            } => {
                write!(f, "t={t} DUPLICATE {from}->{to} stream={stream} len={len}")
            }
            TraceEvent::Pause { t, node, until } => {
                write!(f, "t={t} PAUSE node={node} until={until}")
            }
            TraceEvent::DiskFault {
                t,
                node,
                op,
                file,
                kind,
            } => {
                write!(
                    f,
                    "t={t} DISKFAULT node={node} op={op} file={file} kind={kind}"
                )
            }
            TraceEvent::FsyncLie { t, node, file } => {
                write!(f, "t={t} FSYNCLIE node={node} file={file}")
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
    // Per-node overrides of the global network fault model, keyed on the
    // **sender** (every `NetConfig` knob is sampled/applied at send time on
    // the sending node's own env — see `net_cfg_for`).
    node_net_cfg: BTreeMap<NodeId, NetConfig>,
    // Per-directed-link overrides, keyed `(from, to)` — the same directional
    // keying `partitions` already uses, so an asymmetric link (e.g. a lossy
    // uplink but healthy downlink) can be modelled. Most specific: beats both
    // `node_net_cfg` and `net`.
    link_net_cfg: BTreeMap<(NodeId, NodeId), NetConfig>,
    disk_cfg: DiskConfig,
    // Per-node overrides of the global disk fault model.
    node_disk_cfg: BTreeMap<NodeId, DiskConfig>,
    // Per-node clock skew (signed nanoseconds), applied only to that node's
    // own `Clock::now()` reads (ADR 0018 §2 sim support). Absent = zero skew;
    // default-empty so every existing test stays byte-identical (see
    // `set_clock_skew_for`).
    clock_skew: BTreeMap<NodeId, i64>,
    // Per-node clock **drift rate** (signed parts-per-million, plus the
    // virtual-time instant drift was configured at): `now()` adds a component
    // that grows with elapsed virtual time since that instant, layered on top
    // of `clock_skew`. Absent = no drift; default-empty, same contract as
    // `clock_skew` (see `set_clock_drift_for`).
    clock_drift: BTreeMap<NodeId, (i64, u64)>,
    // Per-node pause deadline (absolute virtual time a paused node resumes
    // at). Absent = never paused. While `clock < paused_until[node]`: a timer
    // this node owns does not fire and a message addressed to it is not
    // delivered — both are deferred (re-timelined) to fire at exactly
    // `paused_until[node]` instead — and a message it sends is not delivered
    // before that instant either (see `send_stream`'s deliver_at clamp). See
    // `Simulator::pause`.
    paused_until: BTreeMap<NodeId, u64>,
    // Which node owns each pending sleep timer, so a firing `Event::Timer`
    // can check whether its owner is currently paused. Populated the first
    // time a `Sleep` future is polled (mirrors `task_owner`'s "record at
    // creation" shape); entries are never removed (a `TimerId` is never
    // reused, so this is a small unbounded map for the lifetime of a
    // simulation — the same tradeoff `task_owner` already makes).
    timer_owner: BTreeMap<TimerId, NodeId>,

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
    fn disk_cfg_for(&self, node: &NodeId) -> &DiskConfig {
        self.node_disk_cfg.get(node).unwrap_or(&self.disk_cfg)
    }

    /// The effective network fault/delay model for a message from `from` to
    /// `to`. Resolution order, **most specific wins**: a link override for
    /// the exact directed `(from, to)` pair
    /// ([`Simulator::set_link_net_config`]), else a per-node override keyed
    /// on the **sender** ([`Simulator::set_net_config_for`] — every knob
    /// this config carries is sampled/applied at send time on the sender's
    /// own env, so the sender is the natural node-level key, mirroring
    /// `disk_cfg_for`'s "the acting node's own override wins" shape), else
    /// the global config ([`Simulator::set_net_config`]).
    fn net_cfg_for(&self, from: &NodeId, to: &NodeId) -> &NetConfig {
        if let Some(cfg) = self.link_net_cfg.get(&(from.clone(), to.clone())) {
            return cfg;
        }
        self.node_net_cfg.get(from).unwrap_or(&self.net)
    }

    /// Sample error injection for one disk op on `node`. Draws RNG **only**
    /// when the effective error rate (generic or ENOSPC) is non-zero, so the
    /// default (off) config perturbs neither the RNG stream nor the trace.
    /// On a hit, records a trace event and returns the `io::Error` the op
    /// must surface (`ErrorKind::StorageFull` for ENOSPC, `ErrorKind::Other`
    /// for a generic fault); the op must make **no** state change (a cleanly
    /// failed I/O call). One shared roll decides between the two: it lands
    /// in `[0, enospc_threshold)` for ENOSPC, then
    /// `[enospc_threshold, enospc_threshold + error_threshold)` for generic,
    /// else no fault — with `enospc_threshold == 0` (every pre-existing
    /// config) this is exactly the old single-draw, single-comparison check.
    fn inject_disk_fault(
        &mut self,
        node: NodeId,
        op: &'static str,
        file: &str,
    ) -> Option<std::io::Error> {
        let cfg = self.disk_cfg_for(&node);
        let (error_threshold, enospc_threshold) = (cfg.error_threshold, cfg.enospc_threshold);
        if error_threshold == 0 && enospc_threshold == 0 {
            return None;
        }
        let roll = self.rng.next_u64();
        let t = self.clock;
        if roll < enospc_threshold {
            self.trace.push(TraceEvent::DiskFault {
                t,
                node: node.clone(),
                op,
                file: file.to_owned(),
                kind: "enospc",
            });
            return Some(std::io::Error::new(
                std::io::ErrorKind::StorageFull,
                format!("sim injected ENOSPC: {op} {file} (node {node})"),
            ));
        }
        if roll < enospc_threshold.saturating_add(error_threshold) {
            self.trace.push(TraceEvent::DiskFault {
                t,
                node: node.clone(),
                op,
                file: file.to_owned(),
                kind: "error",
            });
            return Some(std::io::Error::other(format!(
                "sim injected disk fault: {op} {file} (node {node})"
            )));
        }
        None
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
            node_net_cfg: BTreeMap::new(),
            link_net_cfg: BTreeMap::new(),
            disk_cfg: DiskConfig::default(),
            node_disk_cfg: BTreeMap::new(),
            clock_skew: BTreeMap::new(),
            clock_drift: BTreeMap::new(),
            paused_until: BTreeMap::new(),
            timer_owner: BTreeMap::new(),
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
        st.nodes.insert(node.clone());
        st.inboxes
            .entry((node.clone(), PRIMARY_STREAM))
            .or_default();
        SimEnv {
            shared: Arc::clone(&self.shared),
            node_id: node,
        }
    }

    /// Replace the **global** network delay/drop/duplicate/corrupt model
    /// ([`NetConfig`]). A per-node override
    /// ([`set_net_config_for`](Self::set_net_config_for)) or a per-link one
    /// ([`set_link_net_config`](Self::set_link_net_config)) takes precedence
    /// — see [`NetConfig`]'s own doc for the full resolution order.
    pub fn set_net_config(&self, cfg: NetConfig) {
        self.shared.lock().net = cfg;
    }

    /// Set a **per-node** network fault/delay model, overriding the global
    /// one for every message `node` **sends** (so a test can make one node's
    /// outbound link flaky while the rest stay healthy). Mirrors
    /// [`set_disk_config_for`](Self::set_disk_config_for)'s shape; beaten by
    /// a link override for the specific `(node, to)` pair — see
    /// [`NetConfig`]'s doc for the full resolution order.
    pub fn set_net_config_for(&self, node: NodeId, cfg: NetConfig) {
        self.shared.lock().node_net_cfg.insert(node, cfg);
    }

    /// Set a **per-directed-link** network fault/delay model for messages
    /// from `from` to `to` specifically — the most specific override, beating
    /// both [`set_net_config_for`](Self::set_net_config_for) and
    /// [`set_net_config`](Self::set_net_config). Directional like
    /// [`partition`](Self::partition): configuring `(from, to)` says nothing
    /// about `(to, from)`, so an asymmetric link (e.g. a lossy uplink but a
    /// healthy downlink) is expressible — call this twice, once per
    /// direction, for a symmetric one.
    pub fn set_link_net_config(&self, from: NodeId, to: NodeId, cfg: NetConfig) {
        self.shared.lock().link_net_cfg.insert((from, to), cfg);
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

    /// Set `node`'s clock skew (signed nanoseconds, applied to `Clock::now()`
    /// reads only) — opt-in, per-node, and default-zero (mirrors
    /// [`set_disk_config_for`](Self::set_disk_config_for)'s shape). Models a
    /// node whose local clock reads ahead of (positive) or behind (negative)
    /// the simulation's global timeline, the clock-offset scenario an HLC
    /// (ADR 0018 §2) has to tolerate.
    ///
    /// Deterministic by construction: the value is explicitly set here, never
    /// drawn from the RNG, and introduces no new timeline event — so a
    /// script of `set_clock_skew_for` calls is itself a pure function of
    /// whatever drives the test, not of anything internal to the simulator.
    ///
    /// Skew is read-side only. `SimEnv`'s `Clock::sleep` timers still fire
    /// against the single global clock: a per-node skewed *timeline*
    /// would let nodes' timers interleave in an order that depends on their
    /// skew, reordering the shared event loop and breaking the
    /// single-`(time, seq)`-timeline determinism story this crate provides.
    /// Skew instead models a node's clock *reading* wrong — exactly what an
    /// HLC's physical component has to be robust to — without touching event
    /// ordering at all.
    pub fn set_clock_skew_for(&self, node: NodeId, skew_nanos: i64) {
        self.shared.lock().clock_skew.insert(node, skew_nanos);
    }

    /// Set `node`'s clock **drift rate**, in signed parts-per-million (ppm)
    /// of elapsed virtual time (positive = the clock runs fast, negative =
    /// slow) — layered on top of any static
    /// [`set_clock_skew_for`](Self::set_clock_skew_for) offset, so a node can
    /// carry both a fixed offset and a progressively widening one. The drift
    /// start point is the virtual clock's value **at the moment this is
    /// called**; every later `now()`/`wall_now()` read adds
    /// `drift_ppm * elapsed_nanos_since_that_moment / 1_000_000` on top of the
    /// static skew. Calling this again for the same node replaces both the
    /// rate and the start point (mirrors every other per-node setter's
    /// overwrite semantics).
    ///
    /// Deterministic by construction, same contract as
    /// [`set_clock_skew_for`](Self::set_clock_skew_for): the rate is set
    /// explicitly, never drawn from the RNG, and introduces no new timeline
    /// event. **Opt-in and default-empty** — with no call, every node's
    /// `now()` is byte-identical to a simulator with no drift model at all.
    ///
    /// Same **read-side-only** limit as static skew: this affects
    /// `Clock::now()`/`Clock::wall_now()` reads only, never the shared timer
    /// timeline — see [`set_clock_skew_for`](Self::set_clock_skew_for)'s doc
    /// for why a per-node skewed *timeline* would break the single-`(time,
    /// seq)`-timeline determinism story this crate provides. Drift models a
    /// node's clock *reading* progressively wrong, not a different flow of
    /// time for that node.
    pub fn set_clock_drift_for(&self, node: NodeId, drift_ppm: i64) {
        let mut st = self.shared.lock();
        let start = st.clock;
        st.clock_drift.insert(node, (drift_ppm, start));
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
        let Some(f) = st.disks.get_mut(&(node.clone(), file.to_owned())) else {
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
        st.partitions.insert((a.clone(), b.clone()));
        st.partitions.insert((b, a));
    }

    /// Heal any partition between `from` and `to` (both directions).
    pub fn heal(&self, from: NodeId, to: NodeId) {
        let mut st = self.shared.lock();
        st.partitions.remove(&(from.clone(), to.clone()));
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
        st.crashed.insert(node.clone());
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
            let cfg = st.disk_cfg_for(&node);
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
                node: node.clone(),
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
                    node: node.clone(),
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
                .filter(|(_, owner)| **owner == node)
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
            .filter(|(_, owner)| **owner == node)
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

    /// Pause `node`: alive but frozen for `dur` of virtual time from now,
    /// then resumes on its own with its full state intact — a GC pause,
    /// cgroup throttle, or VM stall, distinct from both
    /// [`crash`](Self::crash) (drops volatile state) and
    /// [`stop`](Self::stop) (removes tasks entirely). While paused:
    ///
    /// - **No timer this node owns fires.** A `Clock::sleep` timer due while
    ///   the node is paused is deferred (re-timelined, not cancelled) to fire
    ///   at exactly the resume instant instead — the node "catches up" the
    ///   instant it unfreezes.
    /// - **No message it sends leaves before it resumes.** A send made while
    ///   paused has its delivery clamped to no earlier than the resume
    ///   instant (this also covers the edge case of a task already in the
    ///   ready queue at the moment `pause` is called, mid-send when frozen).
    /// - **Messages addressed to it queue, they are not dropped.** A
    ///   delivery due while the node is paused is deferred the same way a
    ///   timer is, to fire (adding to the inbox and waking any parked
    ///   `recv`) at the resume instant — nothing sent to a paused node is
    ///   lost, it just arrives late, all at once, on resume.
    ///
    /// Calling this again for the same node before it resumes replaces the
    /// resume instant (mirrors every other per-node setter's overwrite
    /// semantics) — a shorter second call can shorten the pause; a longer one
    /// extends it, and an event already deferred to the old resume instant
    /// will, on reaching it, see the node still paused and defer again to the
    /// new instant.
    ///
    /// Deterministic by construction: the deadline is explicitly computed
    /// from the current virtual clock, never drawn from the RNG. Traced at
    /// the call site ([`TraceEvent::Pause`]) — the point the fault actually
    /// takes effect; the deferred timers/deliveries this causes remain
    /// visible in the trace too, simply at a later `t` than they would have
    /// had unpaused.
    pub fn pause(&self, node: NodeId, dur: Duration) {
        let mut st = self.shared.lock();
        let t = st.clock;
        let until = t.saturating_add(dur_nanos(dur));
        st.paused_until.insert(node.clone(), until);
        st.trace.push(TraceEvent::Pause { t, node, until });
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
                    // A timer owned by a currently-paused node does not fire:
                    // re-timeline it to fire at the resume instant instead
                    // (`Simulator::pause`), without touching `timer_wakers` —
                    // the parked `Sleep` future is left registered exactly as
                    // it was, so it wakes the instant this replayed event
                    // finally fires.
                    let owner = st.timer_owner.get(&id).cloned();
                    let defer_until = owner
                        .as_ref()
                        .and_then(|o| st.paused_until.get(o).copied())
                        .filter(|&until| t < until);
                    if let Some(until) = defer_until {
                        let seq = st.next_seq;
                        st.next_seq += 1;
                        st.timeline.insert((until, seq), Event::Timer(id));
                        None
                    } else {
                        st.trace.push(TraceEvent::Timer { t, id });
                        st.timer_wakers.remove(&id)
                    }
                }
                Event::Deliver { to, env } => {
                    let from = env.from.clone();
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
                    } else if st.partitions.contains(&(from.clone(), to.clone())) {
                        st.trace.push(TraceEvent::Drop {
                            t,
                            from,
                            to,
                            stream,
                            reason: "partition",
                        });
                        None
                    } else if let Some(until) =
                        st.paused_until.get(&to).copied().filter(|&until| t < until)
                    {
                        // The destination is paused: queue, don't drop — defer
                        // this same delivery to fire at the resume instant.
                        let seq = st.next_seq;
                        st.next_seq += 1;
                        st.timeline.insert((until, seq), Event::Deliver { to, env });
                        None
                    } else {
                        let len = env.payload.len();
                        st.trace.push(TraceEvent::Deliver {
                            t,
                            from,
                            to: to.clone(),
                            stream,
                            len,
                        });
                        st.inboxes
                            .entry((to.clone(), stream))
                            .or_default()
                            .push_back(env);
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
        let st = self.shared.lock();
        let skew = st.clock_skew.get(&self.node_id).copied().unwrap_or(0);
        let drift = st.clock_drift.get(&self.node_id).copied();
        Nanos(apply_clock_skew(
            st.clock,
            effective_skew(skew, drift, st.clock),
        ))
    }

    fn wall_now(&self) -> UnixMillis {
        // A pure function of virtual time: a fixed epoch base plus however far
        // this node's (skewed) monotonic reading has advanced. So the calendar
        // time a TTL sweep sees is as seed-reproducible as everything else
        // (ADR 0003), *and* `set_clock_skew_for`/`set_clock_drift_for` skew the
        // wall clock exactly as they skew the monotonic one — which is what a
        // real node with a wrong (or drifting) clock does, and what a TTL
        // reaper has to tolerate.
        UnixMillis(SIM_WALL_EPOCH_MS.saturating_add(self.now().0 / 1_000_000))
    }

    async fn sleep(&self, dur: Duration) {
        let deadline = self.shared.lock().clock.saturating_add(dur_nanos(dur));
        Sleep {
            shared: Arc::clone(&self.shared),
            node: self.node_id.clone(),
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
    /// Fixed draw order (documented here since it is what the byte-identical
    /// default-off guarantee depends on): **drop** roll → **corrupt** roll
    /// (+ byte offset, only if the payload is non-empty) → the primary
    /// delivery's jitter draw (its own **heavy-tail** roll, then the jitter
    /// itself) → **duplicate** roll → (if it fires) the duplicate's own,
    /// fully independent jitter draw (its own heavy-tail roll, then jitter).
    /// Every roll is gated on its threshold being non-zero, so with an
    /// unconfigured [`NetConfig`] (every threshold `0`, matching every
    /// `NetConfig` that existed before this method grew these knobs) this
    /// reduces to exactly the original two-draw sequence (drop roll, then
    /// jitter) in the original order — see [`NetConfig`]'s own doc.
    async fn send_stream(&self, to: NodeId, stream: u64, payload: Vec<u8>) {
        let mut st = self.shared.lock();
        let from = self.node_id.clone();
        let t = st.clock;
        let len = payload.len();
        st.trace.push(TraceEvent::Send {
            t,
            from: from.clone(),
            to: to.clone(),
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

        let net_cfg = st.net_cfg_for(&from, &to).clone();

        // Independent random drop at send time.
        if net_cfg.drop_threshold > 0 && st.rng.next_u64() < net_cfg.drop_threshold {
            st.trace.push(TraceEvent::Drop {
                t,
                from,
                to,
                stream,
                reason: "lossy",
            });
            return;
        }

        // Wire-payload corruption: flip one seed-chosen byte of a surviving
        // message. The roll always happens when configured (independent of
        // payload length, so determinism never depends on message size), but
        // an empty payload has nothing to flip.
        let mut payload = payload;
        if net_cfg.corrupt_threshold > 0 && st.rng.next_u64() < net_cfg.corrupt_threshold {
            if payload.is_empty() {
                // Nothing to corrupt; the roll still happened (see above).
            } else {
                let offset = gen_below(&mut st.rng, payload.len() as u64) as usize;
                payload[offset] ^= 0xFF;
                st.trace.push(TraceEvent::NetCorrupt {
                    t,
                    from: from.clone(),
                    to: to.clone(),
                    stream,
                    offset: offset as u64,
                });
            }
        }

        let base = dur_nanos(net_cfg.base_delay);
        let jitter = draw_jitter(&mut st.rng, &net_cfg);
        let mut deliver_at = st.clock.saturating_add(base + jitter);
        // A paused sender's message must not leave before it resumes — this
        // covers the edge case of a task already ready-queued (about to run)
        // at the moment `Simulator::pause` was called (see its doc).
        if let Some(&until) = st.paused_until.get(&from) {
            deliver_at = deliver_at.max(until);
        }

        // Duplication: decided (and, if it fires, its clone captured) before
        // the primary payload is moved into its envelope below.
        let duplicate =
            net_cfg.duplicate_threshold > 0 && st.rng.next_u64() < net_cfg.duplicate_threshold;
        let dup_payload = duplicate.then(|| payload.clone());

        let seq = st.next_seq;
        st.next_seq += 1;
        st.timeline.insert(
            (deliver_at, seq),
            Event::Deliver {
                to: to.clone(),
                env: Envelope {
                    from: from.clone(),
                    stream,
                    payload,
                },
            },
        );

        if let Some(dup_payload) = dup_payload {
            // Independent delay draw (its own heavy-tail roll included): the
            // duplicate can land before, at, or after the original.
            let dup_jitter = draw_jitter(&mut st.rng, &net_cfg);
            let mut dup_deliver_at = st.clock.saturating_add(base + dup_jitter);
            if let Some(&until) = st.paused_until.get(&from) {
                dup_deliver_at = dup_deliver_at.max(until);
            }
            let dup_seq = st.next_seq;
            st.next_seq += 1;
            st.trace.push(TraceEvent::Duplicate {
                t,
                from: from.clone(),
                to: to.clone(),
                stream,
                len,
            });
            st.timeline.insert(
                (dup_deliver_at, dup_seq),
                Event::Deliver {
                    to,
                    env: Envelope {
                        from,
                        stream,
                        payload: dup_payload,
                    },
                },
            );
        }
    }

    async fn recv_stream(&self, stream: u64) -> Envelope {
        Recv {
            shared: Arc::clone(&self.shared),
            node: self.node_id.clone(),
            stream,
        }
        .await
    }
}

#[async_trait::async_trait]
impl Disk for SimEnv {
    async fn append(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        // Sample the configured latency (if any) inside this block, while
        // still holding `st`, then let the guard drop at the block's close —
        // *before* awaiting `sleep` below. A nested block (not a manual
        // `drop(st)`) is what actually convinces the `Send`-future analysis
        // the non-`Send` `MutexGuard` doesn't live across the `.await`,
        // mirroring `Clock::sleep`'s own two-step "read shared state, then
        // await" pattern above (`sleep` re-locks `self.shared` internally, so
        // holding `st` across the `.await` would also deadlock).
        let delay = {
            let mut st = self.shared.lock();
            if let Some(e) = st.inject_disk_fault(self.node_id.clone(), "append", file) {
                return Err(e);
            }
            let key = (self.node_id.clone(), file.to_owned());
            st.disks
                .entry(key)
                .or_default()
                .buffered
                .extend_from_slice(bytes);
            st.disk_cfg_for(&self.node_id).sync_delay
        };
        if let Some(dur) = delay {
            self.sleep(dur).await;
        }
        Ok(())
    }

    async fn sync(&self, file: &str) -> std::io::Result<()> {
        // See `append`'s identical comment: sample inside this block, then let
        // `st` drop at its close, before awaiting — so a configured
        // `sync_delay` models a real fsync's latency (the caller doesn't get
        // control back until it elapses) without holding `self.shared` across
        // the `.await`.
        let delay = {
            let mut st = self.shared.lock();
            if let Some(e) = st.inject_disk_fault(self.node_id.clone(), "sync", file) {
                return Err(e);
            }
            // fsync-acked-but-lost: sampled independently of, and only after,
            // the error/ENOSPC roll above (a call that already failed never
            // reaches here — nothing was "acked" at all). Draws RNG only when
            // configured non-zero, so a config with no `fsync_lie_prob` set
            // draws nothing extra here and stays byte-identical.
            let fsync_lie_threshold = st.disk_cfg_for(&self.node_id).fsync_lie_threshold;
            let lie = fsync_lie_threshold != 0 && st.rng.next_u64() < fsync_lie_threshold;
            let key = (self.node_id.clone(), file.to_owned());
            if lie {
                let t = st.clock;
                st.trace.push(TraceEvent::FsyncLie {
                    t,
                    node: self.node_id.clone(),
                    file: file.to_owned(),
                });
                // Deliberately do NOT move buffered -> durable: the ack
                // returned below is a lie, and these bytes stay exactly as
                // exposed to a following `Simulator::crash` as any other
                // un-synced tail.
            } else if let Some(f) = st.disks.get_mut(&key) {
                let mut buffered = std::mem::take(&mut f.buffered);
                f.durable.append(&mut buffered);
            }
            st.disk_cfg_for(&self.node_id).sync_delay
        };
        if let Some(dur) = delay {
            self.sleep(dur).await;
        }
        Ok(())
    }

    async fn read(&self, file: &str) -> std::io::Result<Vec<u8>> {
        let mut st = self.shared.lock();
        if let Some(e) = st.inject_disk_fault(self.node_id.clone(), "read", file) {
            return Err(e);
        }
        let key = (self.node_id.clone(), file.to_owned());
        Ok(st.disks.get(&key).map_or_else(Vec::new, |f| {
            let mut out = f.durable.clone();
            out.extend_from_slice(&f.buffered);
            out
        }))
    }

    async fn read_at(&self, file: &str, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
        let mut st = self.shared.lock();
        if let Some(e) = st.inject_disk_fault(self.node_id.clone(), "read_at", file) {
            return Err(e);
        }
        let key = (self.node_id.clone(), file.to_owned());
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
        let key = (self.node_id.clone(), file.to_owned());
        Ok(st
            .disks
            .get(&key)
            .map_or(0, |f| (f.durable.len() + f.buffered.len()) as u64))
    }

    async fn remove(&self, file: &str) -> std::io::Result<()> {
        let mut st = self.shared.lock();
        let key = (self.node_id.clone(), file.to_owned());
        st.disks.remove(&key);
        Ok(())
    }

    async fn replace(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        // Atomic under the state lock: durable jumps straight to `bytes`, with no
        // un-synced remainder. A crash keeps exactly the new contents. An injected
        // fault fails the swap cleanly (temp-file + rename semantics: the old
        // contents remain fully intact).
        let mut st = self.shared.lock();
        if let Some(e) = st.inject_disk_fault(self.node_id.clone(), "replace", file) {
            return Err(e);
        }
        let key = (self.node_id.clone(), file.to_owned());
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
            .range((self.node_id.clone(), String::new())..)
            .take_while(|((node, _), _)| *node == self.node_id)
            .map(|((_, name), _)| name.clone())
            .collect())
    }

    async fn link(&self, src: &str, dst: &str) -> std::io::Result<()> {
        // There is no inode/directory model here, so a hard link is modelled
        // as a snapshot copy of `src`'s current (durable + buffered)
        // `FileState` into `dst`'s own, independent map slot. This is
        // behaviorally indistinguishable from a real hard link for this
        // trait's sanctioned use (linking an already-fully-synced, never-
        // mutated-in-place SSTable file): later `remove`ing either name only
        // touches its own map entry, exactly like two directory entries
        // sharing one inode's bytes until the last link is gone.
        let mut st = self.shared.lock();
        if let Some(e) = st.inject_disk_fault(self.node_id.clone(), "link", src) {
            return Err(e);
        }
        let src_key = (self.node_id.clone(), src.to_owned());
        let Some(content) = st.disks.get(&src_key).cloned() else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("link: source file {src} does not exist"),
            ));
        };
        // Overwrite semantics (idempotent retry): a stale `dst` from a
        // previous, crashed clone attempt is simply replaced.
        let dst_key = (self.node_id.clone(), dst.to_owned());
        st.disks.insert(dst_key, content);
        Ok(())
    }
}

impl Spawner for SimEnv {
    fn spawn(&self, fut: BoxFuture<'static, ()>) {
        let mut st = self.shared.lock();
        let task = st.next_task_id;
        st.next_task_id += 1;
        st.tasks.insert(task, Some(fut));
        st.task_owner.insert(task, self.node_id.clone());
        st.trace.push(TraceEvent::Spawn { task });
        drop(st);
        self.shared.push_ready(task);
    }
}

impl Env for SimEnv {
    fn node_id(&self) -> NodeId {
        self.node_id.clone()
    }
}

/// Future that completes once virtual time reaches its deadline.
struct Sleep {
    shared: Arc<Shared>,
    // Which node this timer belongs to, so a firing `Event::Timer` can check
    // whether the node is currently paused (`Simulator::pause`) and, if so,
    // defer the fire instead of waking. Recorded into `timer_owner` the first
    // time this future actually schedules a timeline entry (below) — not
    // needed at all for a `sleep` that resolves immediately (deadline already
    // past on first poll), since there is no timer to ever defer.
    node: NodeId,
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
                st.timer_owner.insert(id, self.node.clone());
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
        let key = (self.node.clone(), self.stream);
        if let Some(env) = st.inboxes.get_mut(&key).and_then(VecDeque::pop_front) {
            Poll::Ready(env)
        } else {
            st.recv_wakers.insert(key, cx.waker().clone());
            Poll::Pending
        }
    }
}

/// The wall-clock instant a simulation run's virtual timeline starts at:
/// **2020-01-01T00:00:00Z**, in milliseconds since the Unix epoch.
///
/// `SimEnv`'s [`Clock::wall_now`] is this base plus elapsed virtual time, which
/// is what keeps calendar time inside the determinism seam (ADR 0003/0051). It
/// is deliberately a fixed constant rather than the host's clock at run start:
/// a run's wall-clock readings must be a pure function of its seed, so two runs
/// of the same seed on different days agree. Tests that need an "expired" or
/// "not yet expired" TTL value compute it from here rather than from
/// `SystemTime::now`.
///
/// The base is far enough past the epoch that a test can set a *negative*
/// per-node clock skew (`set_clock_skew_for`) without the wall clock
/// saturating at 0.
pub const SIM_WALL_EPOCH_MS: u64 = 1_577_836_800_000;

fn dur_nanos(d: Duration) -> u64 {
    d.as_nanos().min(u128::from(u64::MAX)) as u64
}

/// Apply a signed nanosecond skew to the global clock reading, clamped so it
/// never underflows below 0 nor overflows past `u64::MAX` (`set_clock_skew_for`).
fn apply_clock_skew(clock: u64, skew_nanos: i64) -> u64 {
    if skew_nanos >= 0 {
        clock.saturating_add(skew_nanos as u64)
    } else {
        clock.saturating_sub(skew_nanos.unsigned_abs())
    }
}

/// The total signed skew to apply to a `now()` read: the static
/// [`Simulator::set_clock_skew_for`] offset plus the accumulated
/// [`Simulator::set_clock_drift_for`] component (`drift_ppm * elapsed_nanos
/// since the drift's start instant / 1_000_000`, clamped into `i64`). `i128`
/// arithmetic throughout avoids any intermediate overflow; the result is fed
/// straight into [`apply_clock_skew`], which does its own saturating
/// clamp-at-zero/`u64::MAX` on the final reading.
fn effective_skew(base_skew: i64, drift: Option<(i64, u64)>, clock: u64) -> i64 {
    let Some((drift_ppm, start)) = drift else {
        return base_skew;
    };
    let elapsed = i128::from(clock.saturating_sub(start));
    let component = (elapsed * i128::from(drift_ppm)) / 1_000_000;
    let component = component.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64;
    base_skew.saturating_add(component)
}

/// Draw one send's delivery jitter for `cfg`. With probability
/// `cfg.heavy_tail_threshold` (an extra roll, only drawn when that threshold
/// is non-zero), draws uniformly from `[0, cfg.heavy_tail_max_jitter]`
/// instead of the default `[0, cfg.max_jitter]` — modelling an occasional
/// very slow message without raising the delay for the common case. With
/// `heavy_tail_threshold == 0` (every `NetConfig` before this knob existed)
/// this draws no extra roll and reduces to exactly the original uniform-
/// jitter draw.
fn draw_jitter(rng: &mut ChaCha8Rng, cfg: &NetConfig) -> u64 {
    let heavy = cfg.heavy_tail_threshold > 0 && rng.next_u64() < cfg.heavy_tail_threshold;
    let jitter_max = dur_nanos(if heavy {
        cfg.heavy_tail_max_jitter
    } else {
        cfg.max_jitter
    });
    if jitter_max == 0 {
        0
    } else {
        gen_below(rng, jitter_max + 1)
    }
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
