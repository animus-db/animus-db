//! The `Env`-driven Raft node: a thin driver that owns the environment and
//! ferries time and messages between the network and the synchronous
//! [`RaftCore`].

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

#[cfg(test)]
use animus_env::nid;
use animus_env::{Env, EnvExt, Metric, MetricsHandle, Nanos, NodeId};
use animus_storage::{MergeOp, StorageEngine};
use animus_tablet::TabletId;
use futures::future::{Either, select};
use futures::lock::Mutex as AsyncMutex;

use crate::delta_ring::DeltaRing;
use crate::detector::FailureDetector;
use crate::meta::{Member, MetaCommand, Metadata, NodeStatus, PlacementView};
use crate::mirror::{self, KeyWrite};
use crate::persist::PersistedState;
use crate::persist_round::{self, GatedOuts, PersistArm, PersistFut, PersistProgress, PersistWake};
use crate::raft::{Out, ProposeResult, RaftCore, RaftMsg, Role};
use crate::syskv;

/// File name of the per-node Raft write-ahead log on the `Env` disk.
const WAL: &str = "raft.wal";

/// Snapshot (truncating the covered log prefix) and rewrite the WAL once this
/// many applied entries have accumulated beyond the current snapshot base. This
/// bounds both the in-memory log and the WAL to roughly the live tail.
/// Compared against **`engine_applied`**, not the core's own `last_applied`
/// (ADR 0038 PR3: the apply task, not the consensus loop, drives compaction —
/// see [`meta_apply_and_compact`]'s doc).
const SNAPSHOT_THRESHOLD: u64 = 64;

/// Idle back-off for the apply task ([`meta_apply_loop`]): when there is
/// nothing committed-and-durable to apply it sleeps this long before
/// re-checking. Under load [`meta_apply_and_compact`] keeps returning `true`,
/// so the task never sleeps and stays close behind commit — this only bounds
/// latency (and CPU) while idle. Mirrors `animus-cp-data`'s identical
/// `APPLY_IDLE_POLL`.
const APPLY_IDLE_POLL: Duration = Duration::from_millis(5);

/// How often the leader re-evaluates placement and proposes any corrective
/// `CasTabletReplicas` (ADR 0005). Long relative to the heartbeat interval:
/// reconciliation is a slow background activity, not on any request path.
const RECONCILE_INTERVAL: Duration = Duration::from_millis(500);

/// Evaluate load rebalancing (ADR 0029) once every this many `reconcile_loop`
/// ticks — roughly every 4 seconds at [`RECONCILE_INTERVAL`]. This is a pure
/// pacing/churn-control heuristic ONLY: correctness and safety are carried
/// entirely by the epoch-CAS (a stale move is rejected as an epoch mismatch) and
/// PR1's data-plane catch-up gate, **not** by this cadence. So it must not be
/// hardened into a load-bearing invariant — any value produces a correct cluster,
/// just at a different rebalancing speed.
const REBALANCE_EVERY_N_TICKS: u64 = 8;

/// How long a directed-Placing target member (`SplitPlacing::target`) must
/// be observed CONTINUOUSLY non-`Active` — by `reconcile_loop`'s own
/// per-tick liveness check, never wall clock — before the third phase
/// (`split_placing_reconcile`) treats it as genuinely gone and recomputes a
/// fresh target, rather than pausing indefinitely (ADR 0062 §2, issue #528
/// fix; see `retarget_ready_this_tick`'s own doc for the tracking
/// mechanics). Sized well past ordinary failure-detector flap noise: the
/// investigation behind issue #528 captured dozens of `Down`↔`Active`
/// flips per member within a 240s window under sustained load — individual
/// flaps well under a second — so 10× [`DETECT_TIMEOUT`] (500ms) comfortably
/// outlasts that noise floor, and it is also well past the
/// `animusd::split_placing_completion::SPLIT_PLACING_DONE_SETTLE`
/// precedent (1.5s, 3× [`RECONCILE_INTERVAL`]) for "how long a control-loop
/// observation must hold before it's trusted." Not zero-risk churn either
/// way: too short re-litigates a target mid-flap (this constant's whole
/// reason to exist); too long delays relief for a genuinely dead node. This
/// value is a pacing/liveness heuristic, not a safety invariant — the
/// epoch-CAS on [`MetaCommand::RetargetSplitPlacing`] is what keeps a
/// retarget itself sound regardless of how this is tuned.
pub const SPLIT_PLACING_RETARGET_DWELL: Duration = Duration::from_millis(5_000);

/// How often a member emits a liveness heartbeat to the control group
/// (ADR 0012). On the order of the Raft heartbeat interval, and short relative to
/// [`DETECT_TIMEOUT`] so a live member is comfortably seen within the window.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);

/// How long the leader tolerates silence from a member before marking it `Down`
/// (ADR 0012). Several heartbeat intervals, so a single delayed/dropped heartbeat
/// does not flap a healthy member.
pub const DETECT_TIMEOUT: Duration = Duration::from_millis(500);

/// How often the leader re-evaluates member liveness and proposes any
/// `UpsertMember{status}` transitions (ADR 0012).
const DETECT_INTERVAL: Duration = Duration::from_millis(100);

/// How long a control-group leader tolerates silence (no `AppendEntriesResp`,
/// success or reject) from a **control voter** before
/// [`RaftNode::control_peer_believed_alive`] judges it dead (ADR 0037
/// hardening PR2's quorum-guard liveness fix). Deliberately its **own**
/// constant, not a reuse of [`DETECT_TIMEOUT`]: `DETECT_TIMEOUT` gates
/// [`FailureDetector`]'s **raftkv**-id member liveness (heartbeats over the
/// data-role env, ADR 0012) — a structurally different signal keyed to a
/// different id space (see `ControlHandle::believes_alive`'s doc and
/// `docs/engineering-lessons.md`'s "id-space mismatch" entry for why that
/// signal can't answer a control-voter liveness question). This constant
/// instead gates `RaftCore::peer_last_contact`, a **control**-id-native
/// signal derived straight from control-Raft's own `AppendEntriesResp`
/// traffic — no id bridging, no guessing. Sized to comfortably exceed a few
/// [`HEARTBEAT_INTERVAL`]s (the control heartbeat cadence, driven by
/// `broadcast_append` on every leader tick) so one delayed/dropped
/// `AppendEntries` round doesn't flap a healthy voter.
pub const CONTROL_PEER_LIVENESS_TIMEOUT: Duration = Duration::from_millis(500);

/// Grace period after this node first observes itself leader for a term, during
/// which it will **not** mark any member `Down` (ADR 0012). The
/// [`FailureDetector`] is per-node volatile state (only the transitions it drives
/// are replicated), so a freshly elected leader starts with a **cold** detector:
/// it has observed no heartbeats yet and would otherwise immediately judge every
/// live member silent and propose a flurry of false `Down`s before the first
/// heartbeat round arrives. Suppressing `Down` proposals for at least one
/// [`DETECT_TIMEOUT`] worth of time after gaining leadership gives heartbeats
/// time to repopulate the detector first. Recoveries (`Down`→`Active`) are *not*
/// suppressed — a heartbeat is positive evidence, with no false-positive risk —
/// and the gate is purely `Env`-time based, so it stays deterministic.
const LEADER_GRACE: Duration = DETECT_TIMEOUT;

/// How often the leader re-evaluates orphan-member sweep eligibility (ADR
/// 0040 PR6) — deliberately coarser than [`DETECT_INTERVAL`]/
/// [`RECONCILE_INTERVAL`]: [`DEFAULT_ORPHAN_SWEEP_AFTER`] and any operator
/// override are minutes-scale grace windows, so there is no benefit to
/// checking every 100-500ms the way liveness/placement do, and a coarser tick
/// means [`orphan_sweep_loop`] clones the placement-relevant `Metadata` view
/// far less often.
const ORPHAN_SWEEP_CHECK_INTERVAL: Duration = Duration::from_secs(5);

/// Default grace period a sweep-eligible claim (ADR 0040 PR6 — a
/// `node_addrs` entry, with or without a `members` row, that has never
/// activated) must persist, continuously observed by **this leadership
/// stint's own** volatile timer, before the leader proposes
/// [`MetaCommand::RemoveMember`] for it. Ten minutes comfortably exceeds any
/// legitimate join latency (DNS/dial retries, a slow disk-backed engine open,
/// a queued `change_membership`) while still being short enough that an
/// abandoned claim doesn't linger indefinitely. Configurable per deployment
/// (`animusd`'s config file / CLI flag); `Duration::ZERO` disables the sweep
/// entirely — see [`orphan_sweep_loop`]'s doc.
pub const DEFAULT_ORPHAN_SWEEP_AFTER: Duration = Duration::from_secs(600);

/// Executor-agnostic "applied index advanced" notification (ADR 0031 §trigger):
/// lets a caller (the per-node `TabletHostReconciler`) react to a `Metadata`
/// change as soon as it becomes visible, instead of polling on a fixed timer.
/// A cloneable handle — every clone observes the same underlying cursor.
///
/// **Multi-waiter.** Any number of concurrent [`changed`](MetadataWatch::changed)
/// callers — across any number of clones of this handle — park independently:
/// each returned [`MetadataChanged`] future owns its own slot in a small
/// registry, so registering one waiter's waker can never evict another's, and
/// [`bump`](MetadataWatch::bump) wakes every currently-registered waiter, not
/// just the most recent. This mattered in practice: ADR 0035 PR5 started
/// handing the same handle to two independent concurrent consumers on a
/// combined-mode node (the tablet-host reconciler loop and each inbound
/// `WatchMetadata` RPC's long-poll) — see `docs/engineering-lessons.md` for
/// the lost-wakeup this produced under the single-waiter predecessor
/// (`AtomicWaker`, which only remembers the most recently registered waker).
///
/// No tokio-only primitive (`Notify`/`watch`) is used, so this is fully
/// `SimEnv`-deterministic: a synchronous `wake()` marks each parked task ready
/// for the next run-loop poll, with no wall clock involved. It works
/// identically over a real tokio `ProdEnv`. Registration/removal use a short,
/// `.await`-free `std::sync::Mutex` critical section (never held across a
/// poll), so this stays safe to drive under `SimEnv`'s single-threaded
/// executor as well as real threads.
#[derive(Clone, Default)]
pub struct MetadataWatch(Arc<MetadataWatchInner>);

#[derive(Default)]
struct MetadataWatchInner {
    /// The highest applied index the driver has observed becoming
    /// client-visible — i.e. `RaftCore::last_applied()` sampled at exactly the
    /// points the driver's own durable-before-visible gate
    /// (`min(commit_index, durable_index)` on the leader, `commit_index` on a
    /// follower — see `raft.rs::apply`) can move. Monotonic: only ever raised.
    applied: AtomicU64,
    /// Every currently-parked waiter's waker, keyed by the per-future slot id
    /// [`MetadataWatch::changed`] mints for it. `bump` drains and wakes the
    /// whole map; a [`MetadataChanged`] removes its own entry on `Drop` so an
    /// abandoned long-poll (e.g. a dropped RPC connection) never leaks a slot.
    wakers: Mutex<BTreeMap<u64, Waker>>,
    /// Monotonic source of the slot ids handed out by
    /// [`MetadataWatch::changed`]. Only ever incremented.
    next_slot: AtomicU64,
}

impl MetadataWatch {
    /// The latest applied index this watch has observed, without waiting.
    #[must_use]
    pub fn latest(&self) -> u64 {
        self.0.applied.load(Ordering::Acquire)
    }

    /// Resolves once the driver's applied index exceeds `last_seen`, yielding
    /// the new value.
    ///
    /// Unlike a one-shot flag (`ProposeSignal`'s shape), this is a plain
    /// watermark re-checked fresh on every poll — so there is no
    /// wake-before-park race to lose: if the index already advanced past
    /// `last_seen` before this future is even created, the very first poll
    /// resolves immediately.
    pub fn changed(&self, last_seen: u64) -> MetadataChanged<'_> {
        let slot = self.0.next_slot.fetch_add(1, Ordering::Relaxed);
        MetadataChanged {
            watch: self,
            last_seen,
            slot,
        }
    }

    /// Raise the watermark to `index` (a no-op if `index` is not an advance —
    /// e.g. a stale call from a driver iteration that changed nothing) and
    /// wake **every** currently-parked waiter, only when it actually moved.
    /// Called by the driver wherever a flush could have advanced client-visible
    /// state.
    ///
    /// **Public since ADR 0035 PR5**: a data-only node's `RemoteControlClient`
    /// (`animusd`) owns its own disconnected `MetadataWatch` and drives it
    /// directly from the watermark carried on each `WatchMetadata`/`Status`
    /// reply — the same "external owner bumps a watch it did not itself
    /// derive from a live `RaftCore`" shape this method already supported
    /// in-process, just crossing a network hop instead of a task boundary.
    pub fn bump(&self, index: u64) {
        let prev = self.0.applied.fetch_max(index, Ordering::AcqRel);
        if index > prev {
            // Drain the registry before waking: a woken waiter re-registers
            // its own fresh slot on its next poll (if still pending), so
            // there is no need to retain entries here, and waking happens
            // outside the lock to keep the critical section short.
            let woken =
                std::mem::take(&mut *self.0.wakers.lock().expect("MetadataWatch wakers poisoned"));
            for (_, waker) in woken {
                waker.wake();
            }
        }
    }

    /// Number of waiters currently parked on this watch. Test-only: used to
    /// prove a dropped [`MetadataChanged`] doesn't leak its registry slot.
    #[cfg(test)]
    fn registered_waiters(&self) -> usize {
        self.0
            .wakers
            .lock()
            .expect("MetadataWatch wakers poisoned")
            .len()
    }
}

/// The future returned by [`MetadataWatch::changed`]. Owns one slot in its
/// watch's waker registry for its whole lifetime — minted in `changed`,
/// removed on `Drop` (whether it resolved, was cancelled, or was simply
/// abandoned mid-poll) — so it never leaks and never collides with any other
/// concurrent waiter's slot.
pub struct MetadataChanged<'a> {
    watch: &'a MetadataWatch,
    last_seen: u64,
    slot: u64,
}

impl Future for MetadataChanged<'_> {
    type Output = u64;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<u64> {
        // Register this waiter's own slot before checking — the same
        // register-before-check discipline the old single-slot `AtomicWaker`
        // used, just keyed per-future now: if `bump` races in right after our
        // check but before we park, the freshly registered waker still
        // catches it, and registering here can never evict any *other*
        // waiter's slot.
        self.watch
            .0
            .wakers
            .lock()
            .expect("MetadataWatch wakers poisoned")
            .insert(self.slot, cx.waker().clone());
        let current = self.watch.0.applied.load(Ordering::Acquire);
        if current > self.last_seen {
            // Deregister immediately: a resolved future has nothing left to
            // be woken for, so leaving its slot behind would only cost
            // `bump` a wasted `wake()` on every future advance until `Drop`
            // eventually cleans it up anyway.
            self.watch
                .0
                .wakers
                .lock()
                .expect("MetadataWatch wakers poisoned")
                .remove(&self.slot);
            Poll::Ready(current)
        } else {
            Poll::Pending
        }
    }
}

impl Drop for MetadataChanged<'_> {
    fn drop(&mut self) {
        self.watch
            .0
            .wakers
            .lock()
            .expect("MetadataWatch wakers poisoned")
            .remove(&self.slot);
    }
}

/// The reply to [`RaftNode::watch_delta_since`] (ADR 0038 PR5) — a
/// `WatchMetadata` caller whose `last_seen` the delta ring still covers gets
/// this instead of a full `Metadata` clone: `writes` is empty exactly when
/// `watermark == last_seen` (the timeout-elapsed, nothing-changed case).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaReply {
    /// The [`KeyWrite`]s committed strictly after the caller's `last_seen`,
    /// up to and including `watermark`, in commit order.
    pub writes: Vec<KeyWrite>,
    /// The new watermark to pass back as the next call's `last_seen`.
    pub watermark: u64,
}

/// A running control-plane node. Cheap to clone; clones share one [`RaftCore`]
/// and one [`FailureDetector`].
#[derive(Clone)]
pub struct RaftNode<E: Env> {
    env: E,
    core: Arc<Mutex<RaftCore>>,
    /// The apply task's published, `engine_applied`-gated `Metadata` cache
    /// (ADR 0038 PR3) — every reader (`metadata()`, `members()`,
    /// `placement_view()`, `admin.rs`, the dashboard, `reconcile_loop`/
    /// `detect_loop`) reads *this*, never the core's own (unused, since
    /// `Metadata: DRIVER_APPLIED`) in-memory field. The apply task
    /// (`meta_apply_loop`) is its sole writer.
    cache: Arc<Mutex<Metadata>>,
    /// The highest Raft log index the system-keyspace engine has durably
    /// merged (ADR 0038 PR3) — mirrors `animus-cp-data`'s `engine_applied`.
    /// `cache` is only ever published *after* the matching engine write, so
    /// this and `cache`'s content always agree: a reader that wants to
    /// confirm a specific proposed index took effect polls this (or
    /// `metadata_watch()`), never the core's `last_applied` (which the apply
    /// task lags).
    engine_applied: Arc<AtomicU64>,
    /// The apply task's bounded, best-effort ring of per-command
    /// [`KeyWrite`] deltas (ADR 0038 PR5), keyed by Raft log index — the
    /// incremental half of [`watch_delta_since`](Self::watch_delta_since).
    /// The apply task ([`meta_apply_loop`]) is its sole writer: it pushes
    /// one entry per drained command, in the same pass that publishes
    /// `cache`/bumps `engine_applied`, and [`DeltaRing::clear`]s it whenever
    /// `cache` was just rebuilt from a jump the ring itself didn't witness
    /// (a received `InstallSnapshot`, or this task's own startup rebuild).
    delta_ring: Arc<Mutex<DeltaRing>>,
    /// Shared heartbeat failure detector (ADR 0012). The driver feeds it observed
    /// heartbeats; the `detect_loop` reads it and, when leader, proposes liveness
    /// transitions. Shared so both run against one view.
    detector: Arc<Mutex<FailureDetector>>,
    /// Applied-index change notification (ADR 0031). Shared with the apply
    /// task, which is the sole writer (`bump`, once the cache publish it
    /// describes is actually visible — ADR 0038 PR3); `metadata_watch()`
    /// hands out clones to read/wait on it.
    watch: MetadataWatch,
    /// Serializes the consensus loop's WAL append/fsync against the apply
    /// task's WAL-compaction rewrite of the same file (ADR 0038 PR3) — both
    /// tasks write `raft.wal`. Also held by [`flush`](Self::flush) for the
    /// same reason.
    wal_lock: Arc<AsyncMutex<()>>,
    /// Issue #279: persist-round accounting shared by this node's three WAL
    /// drainers — the consensus loop, the apply task's compaction rewrite, and
    /// the public [`RaftNode::flush`].
    persist: Arc<PersistProgress>,
    /// Observability sink (ADR 0015). The driver loops record control-plane
    /// counters into it (elections, append-entries, snapshot installs, failure
    /// detector transitions) and keep the leadership gauge current. Cheap to
    /// clone; a clone is moved into each spawned loop.
    metrics: MetricsHandle,
}

impl<E: Env> RaftNode<E> {
    /// Start a node: build its [`RaftCore`] and spawn the consensus loop plus
    /// the async **apply task** (ADR 0038 PR3) on `env`. `all_nodes` is the
    /// full control-group membership (including this node); `engine` is this
    /// node's system-keyspace [`StorageEngine`] handle — the durable home of
    /// `Metadata` now that `StateMachine::DRIVER_APPLIED = true` (a combined
    /// node passes its already-open shared CP-data engine, globally
    /// namespaced under [`syskv::RESERVED_NAMESPACE`]; a control-only node
    /// passes a small dedicated one — see `animusd`'s wiring).
    ///
    /// Metrics (ADR 0015) are recorded into the env's own sink (`env.metrics()`)
    /// — for `ProdEnv` a real recording handle, so an assembled production node
    /// accumulates control-plane counters with no extra wiring. To observe the
    /// counters under deterministic simulation (where `SimEnv::metrics()` is the
    /// no-op default), construct with [`start_with_metrics`](Self::start_with_metrics)
    /// and pass a recording [`MetricsHandle`] the test keeps.
    pub fn start<S: StorageEngine + 'static>(env: E, all_nodes: Vec<NodeId>, engine: S) -> Self {
        let metrics = env.metrics();
        Self::start_with_metrics(env, all_nodes, metrics, engine)
    }

    /// Like [`start`](Self::start), but records into the supplied `metrics`
    /// handle instead of `env.metrics()`. Additive (existing callers use
    /// `start`); the sim observability test threads in a recording handle here so
    /// it can read counters back without editing `animus-sim`, and integration
    /// can pass `env.metrics()` (or any chosen sink) explicitly. Uses the
    /// default delta-ring bounds ([`crate::delta_ring::DEFAULT_MAX_ENTRIES`]/
    /// [`crate::delta_ring::DEFAULT_MAX_BYTES`]) — see
    /// [`start_with_ring_bounds`](Self::start_with_ring_bounds) for a caller
    /// that wants different ones.
    pub fn start_with_metrics<S: StorageEngine + 'static>(
        env: E,
        all_nodes: Vec<NodeId>,
        metrics: MetricsHandle,
        engine: S,
    ) -> Self {
        Self::start_with_ring_bounds(env, all_nodes, metrics, engine, DeltaRing::default())
    }

    /// Like [`start_with_metrics`](Self::start_with_metrics), but with an
    /// explicit [`DeltaRing`] (ADR 0038 PR5) instead of the default bounds —
    /// the "configurable" half of the ring's design; a test proving
    /// eviction/fallback behavior without pushing thousands of entries
    /// constructs a small-bounded [`DeltaRing::with_bounds`] and passes it
    /// here. Uses [`DEFAULT_ORPHAN_SWEEP_AFTER`] for the orphan-member sweep
    /// (ADR 0040 PR6) — see
    /// [`start_with_orphan_sweep_after`](Self::start_with_orphan_sweep_after)
    /// for a caller that wants a different grace period (or `Duration::ZERO`
    /// to disable the sweep outright).
    pub fn start_with_ring_bounds<S: StorageEngine + 'static>(
        env: E,
        all_nodes: Vec<NodeId>,
        metrics: MetricsHandle,
        engine: S,
        delta_ring: DeltaRing,
    ) -> Self {
        Self::start_with_orphan_sweep_after(
            env,
            all_nodes,
            metrics,
            engine,
            delta_ring,
            DEFAULT_ORPHAN_SWEEP_AFTER,
        )
    }

    /// Like [`start_with_ring_bounds`](Self::start_with_ring_bounds), but
    /// with an explicit `orphan_sweep_after` (ADR 0040 PR6) instead of
    /// [`DEFAULT_ORPHAN_SWEEP_AFTER`] — the knob `animusd`'s config
    /// file/CLI flag threads through. `Duration::ZERO` disables the sweep
    /// entirely (no loop is even spawned): a sweep-eligible claim then lingers
    /// until an operator prunes it manually via the existing
    /// [`MetaCommand::RemoveMember`] path, exactly as it did before this PR.
    #[allow(clippy::too_many_arguments)] // the most general constructor; every other `start_*` is a thin default-filling wrapper over this one
    pub fn start_with_orphan_sweep_after<S: StorageEngine + 'static>(
        env: E,
        all_nodes: Vec<NodeId>,
        metrics: MetricsHandle,
        engine: S,
        delta_ring: DeltaRing,
        orphan_sweep_after: Duration,
    ) -> Self {
        let core = Arc::new(Mutex::new(RaftCore::new(
            env.node_id(),
            &all_nodes,
            env.now(),
            env.next_u64(),
        )));
        let detector = Arc::new(Mutex::new(FailureDetector::new(DETECT_TIMEOUT)));
        let watch = MetadataWatch::default();
        let cache = Arc::new(Mutex::new(Metadata::default()));
        let engine_applied = Arc::new(AtomicU64::new(0));
        let delta_ring = Arc::new(Mutex::new(delta_ring));
        let wal_lock = Arc::new(AsyncMutex::new(()));
        let persist = Arc::new(PersistProgress::default());
        let node = Self {
            env: env.clone(),
            core: Arc::clone(&core),
            cache: Arc::clone(&cache),
            engine_applied: Arc::clone(&engine_applied),
            delta_ring: Arc::clone(&delta_ring),
            detector: Arc::clone(&detector),
            watch: watch.clone(),
            wal_lock: Arc::clone(&wal_lock),
            persist: Arc::clone(&persist),
            metrics: metrics.clone(),
        };
        env.spawn_task(drive(
            env.clone(),
            Arc::clone(&core),
            Arc::clone(&detector),
            all_nodes,
            metrics.clone(),
            watch.clone(),
            engine,
            Arc::clone(&cache),
            engine_applied,
            delta_ring,
            wal_lock,
            persist,
        ));
        // The placement reconciler runs alongside the driver; it only ever
        // *proposes* on the core (no I/O of its own), and proposals are honored
        // only when this node is leader — so it is safe to run on every node.
        env.spawn_task(reconcile_loop(
            env.clone(),
            Arc::clone(&core),
            Arc::clone(&cache),
        ));
        // The failure detector evaluates member liveness on a timer and, when
        // leader, proposes `UpsertMember` transitions (ADR 0012). Like the
        // reconciler it only *proposes*, so it is safe to run on every node.
        env.spawn_task(detect_loop(
            env.clone(),
            Arc::clone(&core),
            Arc::clone(&cache),
            detector,
            metrics.clone(),
        ));
        // The orphan-member sweep (ADR 0040 PR6): same "only proposes, safe
        // on every node, only acts when leader" shape as the two loops
        // above. `Duration::ZERO` means "disabled" — skip spawning the loop
        // entirely rather than spawning one that would immediately sweep
        // every candidate on its first tick.
        if !orphan_sweep_after.is_zero() {
            env.spawn_task(orphan_sweep_loop(
                env.clone(),
                core,
                cache,
                orphan_sweep_after,
                metrics,
            ));
        }
        node
    }

    /// This node's metrics handle (ADR 0015). A snapshot of it
    /// (`node.metrics().snapshot()`) is the control-plane observability surface.
    #[must_use]
    pub fn metrics(&self) -> &MetricsHandle {
        &self.metrics
    }

    /// The highest Raft log index this node's system-keyspace engine has
    /// durably merged (ADR 0038 PR3) — the watermark [`metadata`](Self::metadata)
    /// is gated on. Mirrors `animus-cp-data::RaftKvNode::engine_applied_index`.
    #[must_use]
    pub fn engine_applied_index(&self) -> u64 {
        self.engine_applied.load(Ordering::Acquire)
    }

    /// The incremental half of `WatchMetadata` (ADR 0038 PR5): the
    /// [`KeyWrite`]s committed strictly after `last_seen` up to this node's
    /// current [`engine_applied_index`](Self::engine_applied_index), or
    /// `None` if this node's own [`DeltaRing`] doesn't (or no longer)
    /// contiguously cover that range — telling the caller (`animusd`'s
    /// `ClientCtx::watch_metadata`) to fall back to a full `metadata()`
    /// clone instead (mirroring the log-tail-vs-`InstallSnapshot` fallback
    /// shape this plane already has). Cheap even when nothing changed: the
    /// trivial `last_seen == current` case never touches the ring lock's
    /// contents, just `Vec::new()`.
    ///
    /// `last_seen` is read against `engine_applied_index()` first (the
    /// authoritative "current" value, published by the same apply-task pass
    /// that fed the ring) rather than the ring's own last entry — so a
    /// narrow race where the ring lags one push behind a just-observed
    /// `engine_applied` bump degrades to a safe `None` (full-fetch fallback)
    /// rather than under-reporting.
    #[must_use]
    pub fn watch_delta_since(&self, last_seen: u64) -> Option<DeltaReply> {
        let current = self.engine_applied_index();
        if last_seen >= current {
            return Some(DeltaReply {
                writes: Vec::new(),
                watermark: current,
            });
        }
        let ring = self.delta_ring.lock().expect("delta ring poisoned");
        let writes = ring.writes_since(last_seen, current)?;
        Some(DeltaReply {
            writes,
            watermark: current,
        })
    }

    /// Propose a metadata command. See [`ProposeResult`].
    pub fn propose(&self, command: MetaCommand) -> ProposeResult {
        self.lock().propose(command)
    }

    /// Drain and durably persist (append + `fsync`) any WAL records the core has
    /// buffered but the driver loop has not yet flushed; returns the count.
    ///
    /// `propose` advances commit/apply and returns **synchronously**, while the
    /// driver loop fsyncs the WAL **asynchronously** — and that loop is normally
    /// parked in its `select` between ticks. So there is a window where an applied
    /// (already client-visible, already acked) command is not yet durable on disk.
    /// A graceful teardown calls this **before** stopping the driver so a clean
    /// shutdown does not lose an acked command (see `animusd`'s
    /// `Node::shutdown_graceful`).
    ///
    /// This used to justify itself with "because the driver is parked at that
    /// point, this is the sole WAL writer". That is no longer the argument, and
    /// it was never the durable one: since issue #279 the driver's own `fsync`
    /// is raced inside its `select`, so it can be mid-round while this runs.
    /// Safety now rests on the mechanism every drainer shares — `wal_lock`
    /// serialises the I/O, and going through
    /// [`persist_round::drain_for_round`] means this drain numbers its own
    /// round, so any ack the loop buffered against these records is released by
    /// this call's completion rather than stranded by it.
    ///
    /// NOTE: this does **not** close the *crash* window — a `kill -9` between apply
    /// and the next flush still loses the entry. Making the commit itself durable
    /// *before* it becomes client-visible is the proper fix, tracked as a follow-up
    /// (ADR 0009 — see the root CLAUDE.md engineering-practices note).
    pub async fn flush(&self) -> usize {
        persist_wal(&self.env, &self.core, &self.wal_lock, &self.persist).await
    }

    /// This node's environment handle.
    pub fn env(&self) -> &E {
        &self.env
    }

    /// A cloneable handle to this node's applied-index watch (ADR 0031): call
    /// [`MetadataWatch::changed`] to be notified as soon as `metadata()` could
    /// have changed, instead of polling on a fixed timer. Multi-waiter — see
    /// [`MetadataWatch`]'s doc — so this is safe to hand to more than one
    /// concurrent consumer.
    #[must_use]
    pub fn metadata_watch(&self) -> MetadataWatch {
        self.watch.clone()
    }

    /// Whether this node currently believes it is leader.
    pub fn is_leader(&self) -> bool {
        self.lock().is_leader()
    }

    /// The node's current role.
    pub fn role(&self) -> Role {
        self.lock().role()
    }

    /// The current term.
    pub fn term(&self) -> u64 {
        self.lock().term()
    }

    /// Best-known leader id. See `RaftCore::leader`'s own doc: this is the
    /// raw, pre-vote-driven consensus belief — an operational (health/
    /// readiness) reader should call [`leader_within`](Self::leader_within)
    /// instead (issue #595).
    pub fn leader(&self) -> Option<NodeId> {
        self.lock().leader()
    }

    /// Hysteresis-bearing leader read for an operational reader (issue
    /// #595) — see `RaftCore::leader_within`'s own doc. Uses this node's own
    /// `env.now()`, never a wall clock (ADR 0003).
    pub fn leader_within(&self, max_age: Duration) -> Option<NodeId> {
        self.lock().leader_within(self.env.now(), max_age)
    }

    /// This node's election-timeout base — the unit `leader_within` callers
    /// typically size their own grace window in (e.g. `animusd::admin::
    /// health`'s `HEALTH_LEADER_GRACE`).
    pub fn election_timeout(&self) -> Duration {
        self.lock().election_timeout()
    }

    /// A clone of the apply task's published `Metadata` cache (ADR 0038 PR3)
    /// — gated on [`engine_applied_index`](Self::engine_applied_index), never
    /// the core's own (unused) in-memory state. May briefly read a fresher
    /// node's `Metadata::default()` before the apply task's first rebuild
    /// completes; a caller that needs read-your-writes should confirm via
    /// [`metadata_watch`](Self::metadata_watch) or
    /// [`engine_applied_index`](Self::engine_applied_index) instead of
    /// assuming this call alone is synchronized with a just-issued `propose`.
    pub fn metadata(&self) -> Metadata {
        self.cache.lock().expect("cache poisoned").clone()
    }

    /// A clone of just the **membership map** — the failure detector's
    /// input, off the apply task's published cache (ADR 0038 PR3).
    pub fn members(&self) -> BTreeMap<NodeId, Member> {
        self.cache.lock().expect("cache poisoned").members.clone()
    }

    /// A clone of the **placement-relevant subset** of the metadata (members,
    /// tablets, and policies — never the schema catalog), off the apply
    /// task's published cache (ADR 0038 PR3).
    pub fn placement_view(&self) -> PlacementView {
        self.cache.lock().expect("cache poisoned").placement_view()
    }

    /// Highest committed log index.
    pub fn commit_index(&self) -> u64 {
        self.lock().commit_index()
    }

    /// The current snapshot base index (0 if no snapshot has been taken). A
    /// follower that caught up via `InstallSnapshot` will have a non-zero value
    /// it never reached by applying alone.
    pub fn snapshot_index(&self) -> u64 {
        self.lock().snapshot_index()
    }

    /// Highest applied log index. With the leader's role-aware apply gate this is
    /// `min(commit_index, durable_index)` on the leader (ADR 0009).
    pub fn last_applied(&self) -> u64 {
        self.lock().last_applied()
    }

    /// Highest log index known durable on disk (the durable-before-visible
    /// frontier, ADR 0009).
    pub fn durable_index(&self) -> u64 {
        self.lock().durable_index()
    }

    /// Number of log entries currently retained (the tail after the snapshot).
    pub fn log_len(&self) -> usize {
        self.lock().log_len()
    }

    /// Index of the last log entry (the snapshot base if the tail is empty).
    pub fn last_log_index(&self) -> u64 {
        self.lock().last_log_index()
    }

    /// The active voter configuration (the control group's membership).
    pub fn config(&self) -> std::collections::BTreeSet<NodeId> {
        self.lock().config()
    }

    /// Propose a **single-server** membership change (ADR 0017 C) to the
    /// control group's *own* Raft configuration: `voters` becomes the new
    /// configuration. Leader-only; rejected for a multi-server delta, an
    /// in-flight change, or removing the leader (see
    /// [`RaftCore::change_membership`], which this is a thin wrapper over —
    /// mirroring `animus-cp-data::RaftKvNode::change_membership`'s identical
    /// shape for the per-tablet data plane). This is the primitive an
    /// operator-driven control-plane membership change (admin API, a later PR
    /// in this stack) drives to grow/shrink/replace a control voter.
    ///
    /// Unlike the per-tablet data plane's `propose_and_wake`, there is no
    /// propose-signal to notify here: `RaftNode`'s own plain `propose` has
    /// never woken the driver loop either (a control-plane proposal is always
    /// serviced on the driver's next heartbeat tick, bounded by
    /// [`HEARTBEAT_INTERVAL`], which is already far shorter than the
    /// election timeout this method's own erratum/single-server gates key
    /// off) — so this stays consistent with the rest of this type's propose
    /// surface rather than inventing a new wake seam for one caller.
    pub fn change_membership(&self, voters: std::collections::BTreeSet<NodeId>) -> ProposeResult {
        record_reconfigure(&self.metrics, self.lock().change_membership(voters))
    }

    /// The active **learner** configuration (ADR 0058 Train 1) — non-voting
    /// control-group members. Mirrors [`config`](Self::config).
    pub fn learners(&self) -> std::collections::BTreeSet<NodeId> {
        self.lock().learners()
    }

    /// Whether learner `id` is caught up closely enough to be a promotion
    /// candidate — see [`RaftCore::learner_caught_up`].
    pub fn learner_caught_up(&self, id: &NodeId, threshold: u64) -> bool {
        self.lock().learner_caught_up(id, threshold)
    }

    /// Add `id` as a **learner** of the control group's own Raft
    /// configuration (ADR 0058 Train 1) — see [`RaftCore::add_learner`], which
    /// this is a thin wrapper over, mirroring `change_membership`'s shape.
    pub fn add_learner(&self, id: NodeId) -> ProposeResult {
        record_reconfigure(&self.metrics, self.lock().add_learner(id))
    }

    /// Promote learner `id` to **voter** (ADR 0058 Train 1) — see
    /// [`RaftCore::promote_learner`].
    pub fn promote_learner(&self, id: NodeId) -> ProposeResult {
        record_reconfigure(&self.metrics, self.lock().promote_learner(id))
    }

    /// Remove learner `id` without promoting it (ADR 0058 Train 1) — see
    /// [`RaftCore::remove_learner`].
    pub fn remove_learner(&self, id: NodeId) -> ProposeResult {
        record_reconfigure(&self.metrics, self.lock().remove_learner(id))
    }

    /// Arm a leadership transfer to `target` (see
    /// [`RaftCore::transfer_leadership`]) — the escape valve
    /// [`change_membership`](Self::change_membership) needs to remove the
    /// *current leader's own* control-voter slot, since it always rejects
    /// leader self-removal. Returns whether the transfer was armed. Mirrors
    /// `animus-cp-data::RaftKvNode::transfer_leadership`'s identical shape.
    pub fn transfer_leadership(&self, target: NodeId) -> bool {
        self.lock().transfer_leadership(target, self.env.now())
    }

    /// The voter this leader is currently handing leadership off to, if any
    /// (see [`RaftCore::transfer_target`]) — `/admin/raft`'s own window onto
    /// an in-flight transfer (issue #313: previously invisible short of
    /// reading the abort log). `None` on any non-leader, and on a leader
    /// with no transfer armed.
    pub fn transfer_target(&self) -> Option<NodeId> {
        self.lock().transfer_target()
    }

    /// Whether this node's failure detector currently judges `member` alive
    /// (a heartbeat seen within the timeout). Observability for tests; the
    /// authoritative liveness lives in the replicated `Metadata` status, which
    /// the leader drives from this verdict.
    pub fn believes_alive(&self, member: NodeId) -> bool {
        self.detector
            .lock()
            .expect("detector poisoned")
            .is_alive(member, self.env.now())
    }

    /// Whether `node` (a **control**-voter id) is believed alive, per the
    /// control-Raft-native liveness signal `RaftCore::peer_last_contact`
    /// tracks (ADR 0037 hardening PR2) — unlike [`believes_alive`](Self::
    /// believes_alive), which is keyed to **raftkv** ids and thus always
    /// `false` for a control id for reasons that have nothing to do with
    /// actual liveness (see `docs/engineering-lessons.md`'s "id-space
    /// mismatch" entry), this reads a fact this node's own control `RaftCore`
    /// observed directly, no id bridging needed. Three cases:
    /// - `node == self`: always alive — a node trivially believes itself up.
    /// - `node` has never been heard from by this leadership stint
    ///   (`peer_last_contact` returns `None`): alive — grace for a peer this
    ///   leader hasn't had a chance to hear from yet, notably a just-added
    ///   voter (`change_membership`) or a peer of a leader that only just won
    ///   an election. Deliberately generous, not "unknown" — see the
    ///   `last_contact` field doc in `raft.rs` for why this case is not
    ///   back-filled instead.
    /// - Otherwise: alive iff the last contact is within
    ///   [`CONTROL_PEER_LIVENESS_TIMEOUT`] of now.
    ///
    /// Meaningful only when this node is (or recently was) the control
    /// leader — a non-leader's `last_contact` map is always empty (nobody
    /// sends it `AppendEntriesResp`), so every non-self peer reads as alive
    /// on a follower, same generous-default behavior as the never-contacted
    /// case above.
    pub fn control_peer_believed_alive(&self, node: NodeId) -> bool {
        if node == self.env.node_id() {
            return true;
        }
        match self.lock().peer_last_contact(node) {
            None => true,
            Some(last) => {
                let now = self.env.now();
                now.0.saturating_sub(last.0) < CONTROL_PEER_LIVENESS_TIMEOUT.as_nanos() as u64
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, RaftCore> {
        self.core.lock().expect("raft core poisoned")
    }
}

/// Emit a liveness heartbeat (ADR 0012) from this `env`'s node to every node in
/// `control`, once. A member spawns [`heartbeat_loop`] (which calls this on a
/// timer) so the control-plane leader can detect its failure. Sends are
/// fire-and-forget over the `Env` network; a partitioned/crashed member's
/// heartbeats are simply not delivered, which is exactly what the detector keys
/// off.
pub async fn send_heartbeat<E: Env>(env: &E, control: &[NodeId]) {
    // Heartbeat carries no command, so pin the default control-plane instantiation.
    let msg: RaftMsg = RaftMsg::Heartbeat {
        node: env.node_id(),
    };
    let bytes = serde_json::to_vec(&msg).expect("heartbeat serializes");
    for c in control {
        env.send(c.clone(), bytes.clone()).await;
    }
}

/// A member's heartbeat loop: every [`HEARTBEAT_INTERVAL`] of `Env` time, send a
/// heartbeat to every control node. Run by a (data-plane) member node so the
/// control plane can detect its liveness; stop it (e.g. `Simulator::stop`) or
/// partition the member to simulate a failure.
pub async fn heartbeat_loop<E: Env>(env: E, control: Vec<NodeId>) {
    loop {
        send_heartbeat(&env, &control).await;
        env.sleep(HEARTBEAT_INTERVAL).await;
    }
}

/// The per-node **consensus loop** (ADR 0038 PR3): recover durable state,
/// spawn the async apply task, then repeatedly start a persist round if the core
/// owes the WAL anything, wait for a landed round / the next message / the timer,
/// hand it to the core, and ship
/// whatever the core wants sent. Does **no** system-keyspace engine I/O
/// itself — that (and WAL compaction) is [`meta_apply_loop`]'s job on a
/// separate task, so a slow engine merge/compaction/snapshot-image build can
/// never block heartbeat/append processing past the election timeout (the
/// `animus-cp-data` driver-liveness fix, ADR 0017 — see this crate's
/// `CLAUDE.md`'s election-storm entry). Mirrors `animus-cp-data`'s own
/// `drive`, minus its ReadIndex/propose-wake plumbing this plane doesn't have.
#[allow(clippy::too_many_arguments)]
async fn drive<E: Env, S: StorageEngine + 'static>(
    env: E,
    core: Arc<Mutex<RaftCore>>,
    detector: Arc<Mutex<FailureDetector>>,
    all_nodes: Vec<NodeId>,
    metrics: MetricsHandle,
    watch: MetadataWatch,
    engine: S,
    cache: Arc<Mutex<Metadata>>,
    engine_applied: Arc<AtomicU64>,
    delta_ring: Arc<Mutex<DeltaRing>>,
    wal_lock: Arc<AsyncMutex<()>>,
    persist: Arc<PersistProgress>,
) {
    // Recover from the WAL before serving anything.
    let bytes = env.read(WAL).await.unwrap_or_default();
    let state = PersistedState::replay(PersistedState::decode(&bytes));
    if !state.is_empty() {
        let recovered =
            RaftCore::recovered(env.node_id(), &all_nodes, state, env.now(), env.next_u64());
        *core.lock().expect("raft core poisoned") = recovered;
    }
    // Spawn the apply task now — after recovery has installed the recovered
    // core, so its first `drain_apply` sees the real post-recovery frontier,
    // not a fresh node's empty one (mirrors `animus-cp-data::drive`'s
    // ordering). The apply task rebuilds its own cache from `engine` and
    // signals `watch` itself once that rebuild (which may recover
    // already-applied state) completes — so a watcher parked before this
    // node's first loop iteration still sees it, without `drive` touching
    // `cache`/`watch` at all (ADR 0038 PR3: this loop does no engine I/O and
    // has no business deciding when `Metadata` is visible).
    env.spawn_task(meta_apply_loop(
        env.clone(),
        Arc::clone(&core),
        engine,
        cache,
        engine_applied,
        delta_ring,
        watch,
        Arc::clone(&wal_lock),
        Arc::clone(&persist),
    ));

    // Issue #279: the loop's own in-flight persist round, and the outbound
    // messages held back until a round lands. Both are owned exclusively by this
    // task, which is what makes the round bookkeeping correct by construction —
    // see `persist_round`'s module doc, especially its "Two layers" section.
    let mut persist_fut: Option<PersistFut<'_>> = None;
    let mut gated: GatedOuts<RaftMsg> = GatedOuts::default();

    loop {
        // Start a round if the core owes the WAL anything and none is in flight.
        // The pending records are whatever the previous iteration's step left, or
        // what a client `propose` appended out-of-band. This is the loop's one
        // round-start site, and — the point of issue #279 — the `append`/`fsync`
        // it runs is raced *inside* the `select` below rather than awaited before
        // it, so this node keeps heartbeating (as leader) and keeps re-arming its
        // election deadline (as follower) while the disk is slow.
        if persist_fut.is_none() && core.lock().expect("raft core poisoned").has_unflushed_wal() {
            // `persist_wal`'s record count is `flush`'s return value, not this
            // loop's business — drop it so the boxed future matches `PersistFut`.
            persist_fut = Some(Box::pin(async {
                persist_wal(&env, &core, &wal_lock, &persist).await;
            }));
        }

        let now = env.now();
        let deadline = core.lock().expect("raft core poisoned").next_deadline();
        // `None` (ADR 0044 phase-1 PR2 — quiescence, always `Some` here: the
        // control plane never enables it, fork G) drops the timer arm entirely
        // rather than sleeping on a synthetic wait, so a hypothetically-quiesced
        // group posts zero `SimEnv` timeline events instead of a degenerate
        // `Duration::ZERO` busy-loop.
        let timer = match deadline {
            Some(deadline) => {
                Either::Left(env.sleep(Duration::from_nanos(deadline.0.saturating_sub(now.0))))
            }
            None => Either::Right(std::future::pending()),
        };

        // Snapshot role/term before stepping the core so we can attribute any
        // state transition the step causes to a metric (ADR 0015). All inputs to
        // the metric decisions are `Env`-supplied or core-derived, so recording
        // stays a deterministic function of the run.
        let (before_role, before_term, before_transfer) = {
            let c = core.lock().expect("raft core poisoned");
            (c.role(), c.term(), c.transfer_target())
        };

        // The persist arm is polled first, so a landed round releases its acks
        // ahead of taking on more work. Resolving it never cancels the `fsync`:
        // the future lives in this task's own local and is only borrowed here.
        let persist_arm = PersistArm::new(&persist, persist_fut.as_mut(), gated.min_round());
        let mut own_round_done = false;
        let (outs, gate): (Vec<(NodeId, RaftMsg)>, Option<u64>) =
            match select(persist_arm, select(env.recv(), timer)).await {
                // A round landed. Nothing to step: the release below ships whatever
                // was waiting on it.
                Either::Left((wake, _)) => {
                    own_round_done = wake == PersistWake::OwnRoundDone;
                    (Vec::new(), None)
                }
                Either::Right((Either::Left((envelope, _)), _)) => {
                    let entropy = env.next_u64();
                    match serde_json::from_slice::<RaftMsg>(&envelope.payload) {
                        // A heartbeat is not consensus traffic (ADR 0012): record it
                        // in the failure detector and don't hand it to the core. The
                        // `now` we observe at is `Env`-supplied, so the recorded
                        // instant is deterministic.
                        Ok(RaftMsg::Heartbeat { node }) => {
                            detector
                                .lock()
                                .expect("detector poisoned")
                                .observe(node, env.now());
                            (Vec::new(), None)
                        }
                        Ok(msg) => {
                            // A follower rejecting an `AppendEntries` surfaces as an
                            // outbound `AppendEntriesResp { success: false }`, so the
                            // "rejected" counter is recorded from the core's output
                            // (`record_outbound`) where the rejection is produced —
                            // not from the inbound message.
                            let (outs, gate) = {
                                let mut c = core.lock().expect("raft core poisoned");
                                let outs = c.handle(envelope.from, msg, env.now(), entropy);
                                // The gate is read in the **same lock acquisition**
                                // as the step that made the mutation — the detail
                                // both reverted attempts at this fix got wrong.
                                (outs, persist.gate(c.has_unflushed_wal()))
                            };
                            record_outbound(&metrics, &outs);
                            (outs, gate)
                        }
                        Err(err) => {
                            tracing::warn!(?err, "undecodable raft message dropped");
                            (Vec::new(), None)
                        }
                    }
                }
                Either::Right((Either::Right(((), _)), _)) => {
                    let entropy = env.next_u64();
                    let (outs, gate) = {
                        let mut c = core.lock().expect("raft core poisoned");
                        let outs = c.tick(env.now(), entropy);
                        (outs, persist.gate(c.has_unflushed_wal()))
                    };
                    record_outbound(&metrics, &outs);
                    (outs, gate)
                }
            };
        // Safe only here: the `select` that borrowed it has been dropped.
        if own_round_done {
            persist_fut = None;
        }

        // Attribute role/term transitions to election metrics + keep the
        // leadership gauge current.
        let (after_role, after_term, after_transfer, election_budget) = {
            let c = core.lock().expect("raft core poisoned");
            (
                c.role(),
                c.term(),
                c.transfer_target(),
                c.election_timeout(),
            )
        };
        record_transition(&metrics, before_role, before_term, after_role, after_term);
        record_transfer_clear(
            &metrics,
            before_transfer.as_ref(),
            after_transfer.as_ref(),
            after_role,
            election_budget,
        );

        // Durability before action: persist (and fsync) the core's state changes
        // before sending the responses that depend on them (a granted vote, an
        // acknowledged append). This is also where a *leader's* durable-gated
        // apply frontier (`last_applied`) actually advances
        // (`mark_durable_through`, inside `persist_wal`) — the apply task picks
        // up whatever that makes newly `drain_apply`-able on its own schedule;
        // a follower may have already advanced it on commit inside `handle`
        // above (no durability gate there).
        let (immediate, held): (Vec<_>, Vec<_>) = match gate {
            None => (outs, Vec::new()),
            Some(_) => outs
                .into_iter()
                .partition(|(_, msg)| persist_round::ships_before_durable(msg)),
        };
        if let Some(round) = gate {
            gated.push(round, held);
        }
        for (to, msg) in immediate {
            let bytes = serde_json::to_vec(&msg).expect("raft message serializes");
            env.send(to, bytes).await;
        }
        // Whatever round landed — this loop's own `fsync`, the apply task's
        // compaction rewrite, or a `RaftNode::flush` from a graceful shutdown —
        // releases the acks that were waiting on it.
        for (to, msg) in gated.release(persist.durable()) {
            let bytes = serde_json::to_vec(&msg).expect("raft message serializes");
            env.send(to, bytes).await;
        }
        // Safety net making a stranded ack structurally impossible rather than
        // merely unlikely: if the core owes the WAL nothing *and* no round is in
        // flight, everything on disk already backs every message still held —
        // whichever of the three drainers put it there, and whether or not its
        // round number lines up. This plane has a second belt: it never quiesces
        // (ADR 0048 fork G), so the timer arm is always present and this is
        // re-evaluated at least once per heartbeat interval regardless of wakes.
        if !gated.is_empty() {
            let settled = {
                let c = core.lock().expect("raft core poisoned");
                persist.fully_durable(c.has_unflushed_wal())
            };
            if settled && persist_fut.is_none() {
                for (to, msg) in gated.release(u64::MAX) {
                    let bytes = serde_json::to_vec(&msg).expect("raft message serializes");
                    env.send(to, bytes).await;
                }
            }
        }
    }
}

/// The per-node **apply task** (ADR 0038 PR3): repeatedly install any received
/// `InstallSnapshot` image, apply committed-and-durable `MetaCommand`s to this
/// task's own privately-owned `Metadata` (via the real, unchanged
/// [`Metadata::apply`], through [`mirror::apply_and_derive_mirror`] so the
/// derived system-keyspace writes ride the same pass), publish the refreshed
/// state into `cache`, and compact — all off the consensus loop (`drive`), so
/// this slow work never delays Raft message/heartbeat processing (the
/// `animus-cp-data` driver-liveness fix, ADR 0017). Backs off by
/// [`APPLY_IDLE_POLL`] only when idle; under load it stays in lockstep behind
/// commit. Mirrors `animus-cp-data`'s `apply_loop`/`apply_and_compact` split
/// exactly, retargeted at the system keyspace instead of a per-tablet range.
#[allow(clippy::too_many_arguments)]
async fn meta_apply_loop<E: Env, S: StorageEngine>(
    env: E,
    core: Arc<Mutex<RaftCore>>,
    engine: S,
    cache: Arc<Mutex<Metadata>>,
    engine_applied: Arc<AtomicU64>,
    delta_ring: Arc<Mutex<DeltaRing>>,
    watch: MetadataWatch,
    wal_lock: Arc<AsyncMutex<()>>,
    persist: Arc<PersistProgress>,
) {
    // Rebuild this task's own owned `Metadata` from whatever the engine
    // already durably holds (empty on a fresh engine; a prior run's content
    // after a restart) and seed `engine_applied` from the engine's own
    // persisted watermark — **not** `core.last_applied()`, which after a WAL
    // recovery only reflects the snapshot base and can understate what the
    // engine already has (compaction only truncates the log periodically;
    // the engine watermark advances every apply pass). Reading the true
    // watermark here is what lets the loop below replay only the log tail
    // beyond it, instead of re-deriving writes for commands the engine
    // already durably reflects (ADR 0038 PR3's restart-recovery contract).
    //
    // The delta ring (ADR 0038 PR5) is freshly constructed and therefore
    // already empty at this point (a real process restart gets a brand-new
    // `RaftNode`/ring) — no explicit reset needed here; only a *received*
    // `InstallSnapshot` mid-run (`meta_apply_and_compact`'s install branch)
    // needs to clear an already-populated ring.
    let mut shadow = mirror::rebuild_metadata_from_engine(&engine)
        .await
        .expect("system-keyspace engine scan (rebuild)");
    let mut watermark = engine
        .get(&syskv::applied_index_key())
        .await
        .expect("system-keyspace engine read (watermark)")
        .map(|v| decode_watermark(&v.value))
        .unwrap_or(0);
    *cache.lock().expect("cache poisoned") = shadow.clone();
    engine_applied.store(watermark, Ordering::SeqCst);
    // A restart can recover already-applied state; a watcher parked before
    // this task's first loop iteration should see it too.
    watch.bump(watermark);

    loop {
        let did_work = meta_apply_and_compact(
            &env,
            &core,
            &engine,
            &cache,
            &engine_applied,
            &delta_ring,
            &watch,
            &wal_lock,
            &persist,
            &mut shadow,
            &mut watermark,
        )
        .await;
        if !did_work {
            env.sleep(APPLY_IDLE_POLL).await;
        }
    }
}

/// Install a fully-received snapshot image, apply the committed-and-durable
/// `MetaCommand` tail beyond `*watermark`, publish `cache`, and compact once
/// the engine has merged enough past the snapshot base. Returns whether it
/// did any work (so the caller backs off only when idle). Mirrors
/// `animus-cp-data::apply_and_compact`'s shape/ordering precisely.
#[allow(clippy::too_many_arguments)]
async fn meta_apply_and_compact<E: Env, S: StorageEngine>(
    env: &E,
    core: &Arc<Mutex<RaftCore>>,
    engine: &S,
    cache: &Arc<Mutex<Metadata>>,
    engine_applied: &Arc<AtomicU64>,
    delta_ring: &Arc<Mutex<DeltaRing>>,
    watch: &MetadataWatch,
    wal_lock: &AsyncMutex<()>,
    persist: &PersistProgress,
    shadow: &mut Metadata,
    watermark: &mut u64,
) -> bool {
    let mut did_work = false;

    // Install a fully-received snapshot (a follower catching up) into the
    // engine *before* applying log-tail effects, so the tail merges on top —
    // then rebuild `shadow`/`cache` from the now-current engine rather than
    // replaying, since an installed image can jump the base arbitrarily far
    // ahead of whatever `shadow` reflected.
    let pending_install = core
        .lock()
        .expect("raft core poisoned")
        .drain_pending_install();
    if let Some((last_index, bytes)) = pending_install {
        install_syskv_image(engine, &bytes).await;
        *shadow = mirror::rebuild_metadata_from_engine(engine)
            .await
            .expect("system-keyspace engine scan (post-install rebuild)");
        *watermark = last_index;
        *cache.lock().expect("cache poisoned") = shadow.clone();
        engine_applied.fetch_max(last_index, Ordering::SeqCst);
        // The ring's coverage window is meaningless across a jump it didn't
        // witness (ADR 0038 PR5) — reset it so a `WatchMetadata` caller
        // whose `last_seen` predates this install correctly falls back to a
        // full fetch instead of silently under-reporting.
        delta_ring.lock().expect("delta ring poisoned").clear();
        watch.bump(last_index);
        did_work = true;
    }

    // Apply the now-durable committed commands in commit order, skipping any
    // whose index the engine already durably reflects (a post-restart tail
    // replay can redeliver commands the engine caught before the last
    // compaction — see `meta_apply_loop`'s doc). Every `MetaCommand` variant
    // has an explicit, exhaustively-matched mirror derivation
    // (`mirror::apply_and_derive_mirror`), so this never silently
    // double-applies a non-idempotent decision onto `shadow`.
    let effects = core.lock().expect("raft core poisoned").drain_apply();
    let mut ops = Vec::new();
    // One entry per drained command (ADR 0038 PR5), pushed into the delta
    // ring below only once the whole batch durably lands — mirrors the
    // `cache` publish, which also only happens after the engine write.
    let mut ring_batch: Vec<(u64, Vec<KeyWrite>)> = Vec::new();
    let mut max_index = *watermark;
    for (index, _term, command) in effects {
        if index <= *watermark {
            continue;
        }
        did_work = true;
        let (_, writes) = mirror::apply_and_derive_mirror(shadow, &command);
        for write in &writes {
            ops.push(match write.clone() {
                KeyWrite::Put(key, value) => MergeOp::put(key, value, index),
                KeyWrite::Delete(key) => MergeOp::tombstone(key, index),
            });
        }
        ring_batch.push((index, writes));
        max_index = index;
    }
    if max_index > *watermark {
        // The watermark rides the same batch, at the batch's own highest
        // index (mirrors `animus-cp-data`'s `engine_applied_index`), so a
        // restart's rebuild plus this key is enough to resume without
        // inspecting the raw Raft log/WAL at all.
        ops.push(MergeOp::put(
            syskv::applied_index_key(),
            max_index.to_be_bytes().to_vec(),
            max_index,
        ));
        engine
            .merge_batch(ops)
            .await
            .expect("system-keyspace apply write");
        *watermark = max_index;
        *cache.lock().expect("cache poisoned") = shadow.clone();
        engine_applied.fetch_max(max_index, Ordering::SeqCst);
        // Feed the ring before bumping `watch` (ADR 0038 PR5): a watcher
        // woken by that bump calls straight into `watch_delta_since`, which
        // must already find this batch's entries in place.
        {
            let mut ring = delta_ring.lock().expect("delta ring poisoned");
            for (index, writes) in ring_batch {
                ring.push(index, writes);
            }
        }
        watch.bump(max_index);
    }

    // Compact once the *engine* has merged enough past the snapshot base:
    // truncate the Raft log prefix and rewrite the WAL to its bounded image
    // (ADR 0017 A.2 / ADR 0038 PR3). Gated on `engine_applied`, not the
    // core's own `last_applied` (which this task lags), so the truncated log
    // prefix never runs past what the engine actually contains. This task is
    // the engine's only writer, so nothing merges between reading `ea` and
    // snapshotting below.
    //
    // The image is built **lazily, on demand** (mirrors
    // `animus-cp-data`): the core raises `take_snapshot_needed` only when its
    // replication path actually needs to ship an `InstallSnapshot` chunk and
    // has no image; this pass then scans the engine once, re-bases the
    // snapshot to exactly what that image reflects (`snapshot_upto(ea)`
    // *before* `set_snapshot_blob`, so base and image agree), and installs
    // it.
    let ea = engine_applied.load(Ordering::SeqCst);
    let (behind, image_needed) = {
        let mut c = core.lock().expect("raft core poisoned");
        (
            ea.saturating_sub(c.snapshot_index()),
            c.take_snapshot_needed(),
        )
    };
    if behind >= SNAPSHOT_THRESHOLD || image_needed {
        let image = if image_needed {
            Some(syskv_image(engine).await)
        } else {
            None
        };
        // Serialize the WAL rewrite against the consensus loop's appends —
        // both tasks write the same file.
        let _wal = wal_lock.lock().await;
        let (bytes, lli) = {
            let mut c = core.lock().expect("raft core poisoned");
            c.snapshot_upto(ea);
            if let Some(image) = image {
                c.set_snapshot_blob(image);
            }
            if !c.take_snapshot_dirty() {
                (None, 0)
            } else {
                let lli = c.last_log_index();
                let mut buf = Vec::new();
                for record in c.wal_image() {
                    buf.extend(PersistedState::encode_record(&record));
                }
                // The rewrite below makes the whole current log durable, so
                // the consensus loop's own accumulated pending append
                // records are now redundant (`replay` is push-based —
                // re-appending them would duplicate entries).
                //
                // Issue #279: this discard is a **persist round like any
                // other**, numbered here in the same lock hold as the drain.
                // While the loop's own `fsync` was inline, silently stealing
                // its pending records was harmless — the loop could not be
                // mid-anything, it was blocked. Now it can be, so a drain that
                // took records without numbering them would leave the acks
                // buffered against them waiting on a round with no drainer.
                let (_superseded, round) = persist_round::drain_for_round(&mut c, persist);
                (Some((buf, round)), lli)
            }
        };
        if let Some((bytes, round)) = bytes {
            env.replace(WAL, &bytes).await.expect("wal compaction");
            let mut c = core.lock().expect("raft core poisoned");
            c.mark_durable_through(lli);
            if let Some(round) = round {
                persist.complete_drain(round);
            }
        }
        did_work = true;
    }

    did_work
}

/// Build the system-keyspace image shipped to a lagging follower via
/// `InstallSnapshot` (ADR 0038 PR3): every live `(key, value-or-tombstone,
/// version)` under [`syskv::RESERVED_NAMESPACE`] — tombstones are carried
/// (not just omitted) so a receiver applying this via `merge_batch` correctly
/// overwrites any stale value it might already hold from an earlier,
/// incomplete transfer. Filtering by [`syskv::decode_key`] matters only on a
/// **combined** node, whose engine is shared with the CP data plane's own,
/// differently-keyed entries.
async fn syskv_image<S: StorageEngine>(engine: &S) -> Vec<u8> {
    let entries: Vec<(Vec<u8>, Option<Vec<u8>>, u64)> = engine
        .entries_with_tombstones()
        .await
        .expect("system-keyspace engine scan (image)")
        .into_iter()
        .filter(|(key, _, _)| syskv::decode_key(key).is_some())
        .collect();
    serde_json::to_vec(&entries).expect("system-keyspace image serializes")
}

/// Write a received system-keyspace image into the engine (a follower
/// catching up via `InstallSnapshot`), the dual of [`syskv_image`]. Logs a
/// warning and drops the image on an undecodable payload rather than
/// panicking — mirrors `animus-cp-data::install_engine_image`'s treatment of
/// a corrupt wire image.
async fn install_syskv_image<S: StorageEngine>(engine: &S, bytes: &[u8]) {
    let entries: Vec<(Vec<u8>, Option<Vec<u8>>, u64)> = match serde_json::from_slice(bytes) {
        Ok(e) => e,
        Err(err) => {
            tracing::warn!(?err, "undecodable system-keyspace snapshot image dropped");
            return;
        }
    };
    if entries.is_empty() {
        return;
    }
    let ops = entries
        .into_iter()
        .map(|(key, value, version)| match value {
            Some(v) => MergeOp::put(key, v, version),
            None => MergeOp::tombstone(key, version),
        })
        .collect();
    engine
        .merge_batch(ops)
        .await
        .expect("system-keyspace image install");
}

/// Decode an 8-byte big-endian `u64` watermark value (the same encoding
/// [`syskv::applied_index_key`]'s value uses). Panics on a malformed value —
/// the apply task is the only writer of this key, so a mismatch is an
/// internal bug.
fn decode_watermark(bytes: &[u8]) -> u64 {
    let array: [u8; 8] = bytes
        .try_into()
        .expect("applied-index watermark is exactly 8 bytes");
    u64::from_be_bytes(array)
}

/// Record the metrics implied by the messages the core just emitted (ADR 0015):
/// every outbound `AppendEntries` is one replication/heartbeat *sent*; an
/// outbound `AppendEntriesResp { success: false }` is a *rejection* this follower
/// produced; an outbound `InstallSnapshotResp` whose `last_index > 0` marks a
/// completed snapshot *install* on this follower. A pure read of `outs`.
fn record_outbound(metrics: &MetricsHandle, outs: &[Out]) {
    for (_, msg) in outs {
        match msg {
            RaftMsg::AppendEntries { .. } => metrics.incr(Metric::AppendEntriesSent),
            RaftMsg::AppendEntriesResp { success: false, .. } => {
                metrics.incr(Metric::AppendEntriesRejected);
            }
            RaftMsg::InstallSnapshotResp { last_index, .. } if *last_index > 0 => {
                metrics.incr(Metric::SnapshotInstalls);
            }
            _ => {}
        }
    }
}

/// Record the real outcome of a control-group `change_membership` step (ADR
/// 0015) — accepted, or rejected (not leader, an in-flight change, a
/// multi-server delta, or a leader self-removal) — and pass the result through
/// unchanged. Mirrors `animus-cp-data`'s `record_reconfigure`, but under its
/// own counter family (`ControlReconfigureAccepted`/`Rejected`) so control-
/// *group* reconfiguration churn stays distinguishable from the per-tablet
/// data plane's `CpReconfigureAccepted`/`Rejected`.
fn record_reconfigure(metrics: &MetricsHandle, result: ProposeResult) -> ProposeResult {
    match result {
        ProposeResult::Accepted { .. } => metrics.incr(Metric::ControlReconfigureAccepted),
        ProposeResult::NotLeader { .. } => metrics.incr(Metric::ControlReconfigureRejected),
    }
    result
}

/// Record election metrics + the leadership gauge from a role/term transition
/// (ADR 0015). Becoming a candidate at a higher term is an election *started*;
/// transitioning into `Leader` is an election *won*. The gauge tracks whether
/// this node currently believes it is leader. Pure in its inputs.
fn record_transition(
    metrics: &MetricsHandle,
    before_role: Role,
    before_term: u64,
    after_role: Role,
    after_term: u64,
) {
    // A new election: this node bumped its term and is now a candidate.
    if after_role == Role::Candidate && after_term > before_term {
        metrics.incr(Metric::ElectionsStarted);
    }
    // Won: entered the leader role from a non-leader role.
    if after_role == Role::Leader && before_role != Role::Leader {
        metrics.incr(Metric::ElectionsWon);
    }
    // Keep the gauge level current on any leadership change.
    if (after_role == Role::Leader) != (before_role == Role::Leader) {
        metrics.set_leader(after_role == Role::Leader);
    }
}

/// Observe a `transfer_target` clear (issue #313) — `RaftCore::
/// transfer_leadership`'s handoff has no output message and no metrics
/// handle of its own (it's pure, I/O-free core state, ADR 0003's sync/
/// driver split), so the only way to tell "the transfer just aborted" from
/// "the transfer just succeeded" from outside is to diff `transfer_target`
/// across one `tick`/`handle` step alongside `after_role`, the same idiom
/// [`record_transition`] already uses for election metrics. Pure in its
/// inputs; called unconditionally, cheap no-op on every iteration where
/// nothing changed.
///
/// - `Some -> None` while still `Leader`: the deadline in `RaftCore::tick`
///   fired with the target never having stepped down — an **abort**
///   (crashed after arming, fell behind, or a dropped `TimeoutNow`/election
///   round). This is the case issue #313 found completely invisible:
///   logged here at `warn`, plus [`Metric::ControlTransferAborted`].
/// - `Some -> None` while no longer `Leader`: this node itself stepped down
///   (`RaftCore::handle`'s higher-term branch) — either the transfer
///   **succeeded** (the target won an election and this node saw its
///   higher term) or a *different* node won one instead (superseded). Both
///   are ordinary, not failures, so this logs at `info` with no metric —
///   an operator diagnosing a stuck transfer cares about the abort case
///   above, not every routine handoff completion.
fn record_transfer_clear(
    metrics: &MetricsHandle,
    before_transfer: Option<&NodeId>,
    after_transfer: Option<&NodeId>,
    after_role: Role,
    election_budget: Duration,
) {
    let Some(target) = before_transfer else {
        return;
    };
    if after_transfer.is_some() {
        return;
    }
    if after_role == Role::Leader {
        metrics.incr(Metric::ControlTransferAborted);
        tracing::warn!(
            %target,
            budget_ms = election_budget.as_millis() as u64,
            "leadership transfer aborted: target did not step down within budget"
        );
    } else {
        tracing::info!(
            %target,
            "leadership transfer resolved: this node stepped down (transfer likely completed, \
             or was superseded by a different election)"
        );
    }
}

/// The leader's placement reconciler (ADR 0005): on a slow timer, if this node
/// is leader, recompute the desired replica set for every tablet that has a
/// policy and propose a `CasTabletReplicas` for any that drifted out of
/// compliance (e.g. a replica's member went `Down`).
///
/// The decision is the **pure, deterministic** [`Metadata::reconcile`]; this
/// driver supplies only timing (over the `Env` seam) and the propose. It runs on
/// every node but is a no-op off the leader (`propose` returns `NotLeader`), and
/// a no-op when nothing drifted (`reconcile` returns no commands) — so it is
/// idempotent and produces no churn at steady state. The proposed entries are
/// flushed and replicated by the [`drive`] loop's regular WAL handling.
///
/// It also carries **load rebalancing** (ADR 0029, [`Metadata::rebalance`]): once
/// every [`REBALANCE_EVERY_N_TICKS`] ticks, *and only if repair proposed nothing
/// this tick* (violation repair always wins), it proposes a single
/// balance-improving move so a grown cluster spreads its existing tablets onto new
/// members — something the pin-survivors reconciler never does on its own.
///
/// A third phase, **directed Placing convergence** (ADR 0062 §2,
/// [`Metadata::split_placing_reconcile`]/[`PlacementView::split_placing_reconcile`],
/// fixed for issue #528), runs every tick, unconditionally — independent of
/// repair/rebalance's own gating, since a split-triggered relief obligation
/// should not wait behind `REBALANCE_EVERY_N_TICKS`'s cadence (meant for
/// slow, cluster-wide balance churn) or be starved by repair's own
/// priority. For every un-`done` `split_placing` entry it drives toward the
/// STORED target verbatim while every member of it is `Active`, pausing
/// (proposing nothing) while a member is transiently `Down`, and only
/// recomputes once `retarget_ready_this_tick`'s dwell tracking says a
/// member has been down long enough to treat as genuinely gone — see that
/// function's own doc and `split_placing_reconcile`'s (`meta.rs`) for the
/// full mechanics and the root cause this fixes (a fresh recompute every
/// tick made the target itself flap faster than `animus-cp-data`'s own
/// mover could converge, a livelock one layer below the completion loop's
/// own correct logic). It never proposes `MarkSplitPlacingDone` — that is a
/// separate, later mechanism (ADR 0062 §3) that observes live Raft
/// convergence, not something this pure metadata-level view can see.
async fn reconcile_loop<E: Env>(env: E, core: Arc<Mutex<RaftCore>>, cache: Arc<Mutex<Metadata>>) {
    let mut tick: u64 = 0;
    // Driver-local dwell tracking for the directed-Placing phase's
    // retarget gate (ADR 0062 §2, issue #528 fix) — see
    // `retarget_ready_this_tick`'s own doc. Volatile, per-node,
    // `env.now()`-keyed (never wall clock, per the root `CLAUDE.md`
    // determinism rules) — lost across a leadership change exactly like
    // the failure detector's own per-node state, which only ever delays a
    // retarget, never mis-times one unsoundly (the epoch-CAS on
    // `RetargetSplitPlacing` is what keeps it safe regardless).
    let mut retarget_since: BTreeMap<(TabletId, NodeId), Nanos> = BTreeMap::new();
    loop {
        env.sleep(RECONCILE_INTERVAL).await;
        tick = tick.wrapping_add(1);
        // Leadership is a consensus-level fact (unaffected by ADR 0038 PR3),
        // still read off the core; the placement *view* now comes from the
        // apply task's published cache. Clone only the placement-relevant
        // view (members + tablets + policies — not the schema catalog or the
        // CP address book, which dominate a grown `Metadata`), and run the
        // pure decision *off* the lock, so a big catalog never turns this
        // background tick into a full-blob clone every 500ms (clone-churn
        // fix).
        if !core.lock().expect("raft core poisoned").is_leader() {
            // A non-leader's dwell tracking is meaningless (only the leader
            // ever proposes a retarget) — clear it so a future leadership
            // stint starts its dwell clocks fresh rather than resuming a
            // stale one from `env.now()` values that may be arbitrarily far
            // in the past, mirroring the failure detector's own cold-start
            // stance on regaining leadership.
            retarget_since.clear();
            continue;
        }
        let view = cache.lock().expect("cache poisoned").placement_view();
        let proposals = view.reconcile();
        let repaired = !proposals.is_empty();
        for command in proposals {
            // Off-leader transitions between the check and here are harmless:
            // a stale `CasTabletReplicas` is rejected by the epoch guard, and a
            // non-leader `propose` is dropped.
            core.lock().expect("raft core poisoned").propose(command);
        }
        // Load rebalancing (ADR 0029) runs only when repair proposed *nothing*
        // this tick — violation repair always takes priority over balance — and
        // only on the rebalance cadence. It proposes a single balance-improving
        // move (a healthy replica from a most-loaded node onto a least-loaded
        // one). The cadence is pure churn control (see `REBALANCE_EVERY_N_TICKS`):
        // safety is the epoch-CAS + data-plane catch-up gate, not this timing.
        if !repaired
            && tick.is_multiple_of(REBALANCE_EVERY_N_TICKS)
            && let Some(command) = view.rebalance()
        {
            core.lock().expect("raft core poisoned").propose(command);
        }
        // ADR 0062 §2's directed-Placing phase: unconditional every tick,
        // independent of the repair/rebalance gating above. Off-leader
        // transitions between the check at the top of this tick and here are
        // just as harmless as they are for repair/rebalance, above.
        let retarget_ready = retarget_ready_this_tick(&env, &view, &mut retarget_since);
        for command in view.split_placing_reconcile(&retarget_ready) {
            core.lock().expect("raft core poisoned").propose(command);
        }
    }
}

/// One tick's worth of dwell-gate bookkeeping for `reconcile_loop`'s
/// directed-Placing phase (ADR 0062 §2, issue #528 fix): for every un-`done`
/// `split_placing` entry with a stored `Some(target)`, tracks how long each
/// currently-non-`Active` target member has been CONTINUOUSLY non-`Active`
/// (via `env.now()`, never wall clock) in `retarget_since`, and returns the
/// set of tablets for which at least one target member has been down for at
/// least [`SPLIT_PLACING_RETARGET_DWELL`] — i.e. tablets `split_placing_
/// reconcile` may recompute a fresh target for this tick.
///
/// `retarget_since` is mutated in place: a member observed `Active` this
/// tick has its tracked timer removed (a "continuously down" clock must
/// restart from zero the next time it goes down, not resume a stale one —
/// this is what makes the gate a genuine dwell rather than a cumulative
/// down-time counter), and any tracked `(tablet, member)` pair that is no
/// longer live this tick (the member came back, the tablet's entry
/// finished/vanished, or that member is no longer even part of the current
/// stored target) is pruned outright, so this map never grows unbounded
/// across a long-running leader's lifetime.
fn retarget_ready_this_tick<E: Env>(
    env: &E,
    view: &PlacementView,
    retarget_since: &mut BTreeMap<(TabletId, NodeId), Nanos>,
) -> BTreeSet<TabletId> {
    let now = env.now();
    let mut ready = BTreeSet::new();
    let mut live: BTreeSet<(TabletId, NodeId)> = BTreeSet::new();
    for (&tablet, entry) in &view.split_placing {
        if entry.done {
            continue;
        }
        let Some(target) = &entry.target else {
            continue; // nothing stored yet for this entry — no dwell to track
        };
        for member in target {
            let is_active = view
                .members
                .get(member)
                .is_some_and(|m| m.status == NodeStatus::Active);
            if is_active {
                continue; // this member needs no dwell tracking right now
            }
            live.insert((tablet, member.clone()));
            let since = *retarget_since
                .entry((tablet, member.clone()))
                .or_insert(now);
            if now.duration_since(since) >= SPLIT_PLACING_RETARGET_DWELL {
                ready.insert(tablet);
            }
        }
    }
    retarget_since.retain(|key, _| live.contains(key));
    ready
}

/// The leader's failure detector (ADR 0012): on a timer, if this node is leader,
/// compare each tracked member's heartbeat liveness against its replicated
/// status and propose an `UpsertMember{status}` transition for any whose
/// liveness changed.
///
/// The **decision** is the pure [`FailureDetector`] verdict, taken at an
/// `Env`-supplied `now`; this driver supplies only timing (over the `Env` seam)
/// and the propose — mirroring the placement reconciler. It is **idempotent**: a
/// member already at the status its liveness implies yields no proposal, so a
/// steady cluster produces no churn and there is no flapping at the status level
/// (the detector's `timeout` absorbs a single delayed/dropped heartbeat). Once
/// committed, a `Down` transition is exactly what the placement reconciler reacts
/// to (ADR 0005), so a detected failure cascades into tablet re-placement.
///
/// Only members the detector *tracks* are judged; `Joining`/`Leaving` are left
/// alone — their lifecycle is operator-driven, not liveness-driven.
///
/// **A member becomes tracked either by a real heartbeat, or — for an `Active`
/// member — by this loop simply noticing it exists (ADR 0030 phantom-member
/// hardening).** A brand-new member is registered `Down` and stays untracked
/// (and thus unjudged) until its own real first heartbeat promotes it — the
/// online-growth admin-add path relies on exactly this to keep a
/// declared-but-never-booted growth node from ever reaching `Active` at all.
/// But `bootstrap` (`animusd`) registers its nodes `Active` immediately, before
/// any of them may have heartbeated yet (provisioning a table's replica set
/// needs a stable, immediately-complete `Active` set — see `bootstrap`'s own
/// doc for the regression this avoids), which reopens a **different** hole: an
/// `Active` member the detector has never heard from is never judged at all by
/// the "only tracked members are judged" rule above, so a declared-but-
/// never-booted `Active` member would stay placement-eligible forever. Closed
/// by giving any `Active`-but-untracked member a **synthetic first observation**
/// at the instant this loop first notices it (`declare_active_members`, pure):
/// this starts exactly the same silence clock a real heartbeat would, so a node
/// whose real heartbeat arrives promptly (the overwhelmingly common case) is
/// unaffected, while one that never heartbeats at all is judged dead — and, once
/// the post-election grace below elapses, demoted to `Down` — after one ordinary
/// [`DETECT_TIMEOUT`], the same as any other failure. A member already tracked
/// (has genuinely heartbeated) is left untouched, so a real heartbeat's instant
/// is never overwritten by a later, coarser synthetic one.
///
/// A freshly elected leader observes a **grace period** ([`LEADER_GRACE`]) before
/// proposing any `Down`: its detector is cold (per-node volatile state), so it
/// must hear a heartbeat round before it can fairly judge silence. The loop
/// records when it first sees itself leader for a term (`leader_since`) and
/// re-arms the grace whenever leadership or term changes.
async fn detect_loop<E: Env>(
    env: E,
    core: Arc<Mutex<RaftCore>>,
    cache: Arc<Mutex<Metadata>>,
    detector: Arc<Mutex<FailureDetector>>,
    metrics: MetricsHandle,
) {
    // The (term, instant) at which this node last observed itself leader. `None`
    // while not leader; re-armed on each fresh leadership/term so the cold-start
    // grace applies after every election, not just the first.
    let mut leader_since: Option<(u64, animus_env::Nanos)> = None;
    loop {
        env.sleep(DETECT_INTERVAL).await;
        let now = env.now();
        // Leadership/term are consensus-level facts, still read off the core
        // (unaffected by ADR 0038 PR3); the **membership map** now comes from
        // the apply task's published cache — never the whole `Metadata` (this
        // loop ticks every 100ms; cloning the full blob, schema catalog
        // included, was the clone-churn hot spot).
        let term_now = {
            let core = core.lock().expect("raft core poisoned");
            if !core.is_leader() {
                leader_since = None;
                continue;
            }
            core.term()
        };
        // Re-arm the grace on a fresh leadership or a new term.
        let since = match leader_since {
            Some((t, since)) if t == term_now => since,
            _ => {
                leader_since = Some((term_now, now));
                now
            }
        };
        // Suppress `Down` until the cold detector has had a heartbeat round.
        let allow_down = now.duration_since(since) >= LEADER_GRACE;
        let members = cache.lock().expect("cache poisoned").members.clone();
        // Decommission cleanup (ADR 0032 PR3): a member `RemoveMember` already
        // pruned from `members` should stop being tracked too — otherwise
        // `last_seen` grows unboundedly across a cluster's lifetime, and a
        // later `RemoveMember` on some other id currently reused nothing but
        // memory (the pure `members` filter in `liveness_transitions` already
        // stops a removed member from ever being *proposed for* again, so
        // this is belt-and-braces bounding, not a safety fix). Computed as a
        // pure function of the two maps so it's unit-testable without a driver.
        {
            let mut d = detector.lock().expect("detector poisoned");
            for id in stale_tracked_ids(&members, &d) {
                d.forget(id);
            }
        }
        // Phantom-member hardening (ADR 0030): give any `Active`-but-untracked
        // member a synthetic first observation before evaluating (see this fn's
        // doc). A no-op for every member the detector already tracks.
        {
            let mut d = detector.lock().expect("detector poisoned");
            for (id, m) in &members {
                if m.status == NodeStatus::Active && !d.tracks(id.clone()) {
                    d.observe(id.clone(), now);
                }
            }
        }
        // Evaluate off the core lock (the detector has its own mutex).
        let proposals = liveness_transitions(
            &members,
            &detector.lock().expect("detector poisoned"),
            now,
            allow_down,
        );
        for command in proposals {
            // Attribute each liveness transition to its failure-detector metric
            // (ADR 0012/0015) before proposing it. `liveness_transitions` only
            // emits an `UpsertMember` when a tracked member's status actually
            // changes, so a `Down` here is a fresh Active->Down verdict and an
            // `Active` is a Down->Active recovery — the exact up/down edges we
            // want to count. Recording from the proposed command keeps the metric
            // a deterministic function of the (pure) verdict, and counts the edge
            // once on the leader that drives it.
            if let MetaCommand::UpsertMember { status, .. } = &command {
                match status {
                    NodeStatus::Down => metrics.incr(Metric::FailureDetectorDown),
                    NodeStatus::Active => metrics.incr(Metric::FailureDetectorUp),
                    _ => {}
                }
            }
            core.lock().expect("raft core poisoned").propose(command);
        }
    }
}

/// Pure helper (ADR 0032 PR3): every id the detector currently
/// [`tracks`](FailureDetector::tracks) that is **no longer** present in
/// `members` — a member `RemoveMember` has pruned from replicated
/// `Metadata`. Returned in ascending order (the detector's own iteration
/// order), so the caller's `forget` loop is deterministic. Takes just the
/// membership map (not the whole `Metadata`), mirroring
/// [`liveness_transitions`]'s narrow-clone discipline.
fn stale_tracked_ids(
    members: &std::collections::BTreeMap<NodeId, Member>,
    detector: &FailureDetector,
) -> Vec<NodeId> {
    detector
        .tracked_ids()
        .filter(|id| !members.contains_key(id))
        .collect()
}

/// Pure helper: the `UpsertMember` transitions needed to bring each tracked
/// member's replicated status in line with the detector's liveness verdict at
/// `now`. Takes just the **membership map** (not the whole `Metadata`), so the
/// caller can hand it a narrow clone taken off the core lock (clone-churn fix).
/// Returns commands only for members whose status would actually change
/// (idempotent), in ascending node-id order (the detector iterates a `BTreeMap`),
/// so the result is a deterministic function of `(members, detector, now,
/// allow_down)`.
///
/// `allow_down` gates the `Active`→`Down` transition: a freshly elected leader
/// passes `false` during its post-election grace period so a cold detector does
/// not falsely mark live members `Down` before their heartbeats arrive
/// (ADR 0012). Recoveries (`Down`→`Active`) are always allowed.
fn liveness_transitions(
    members: &std::collections::BTreeMap<NodeId, Member>,
    detector: &FailureDetector,
    now: animus_env::Nanos,
    allow_down: bool,
) -> Vec<MetaCommand> {
    detector
        .evaluate(now)
        .into_iter()
        .filter_map(|l| {
            let member = members.get(&l.node)?;
            let desired = match (member.status, l.alive) {
                // A live member believed dead recovers to `Active`.
                (NodeStatus::Down, true) => NodeStatus::Active,
                // A silent member believed alive is marked `Down` — unless this
                // leader is still inside its post-election grace period.
                (NodeStatus::Active, false) if allow_down => NodeStatus::Down,
                // Already consistent, a status we don't drive (`Joining`/
                // `Leaving`), or a `Down` suppressed by the grace period: nothing
                // to propose.
                _ => return None,
            };
            Some(transition(l.node, member, desired))
        })
        .collect()
}

/// Build an `UpsertMember` that changes only `member`'s status, preserving its
/// topology labels (so a liveness transition never disturbs residency/spread).
fn transition(node: NodeId, member: &Member, status: NodeStatus) -> MetaCommand {
    MetaCommand::UpsertMember {
        node,
        labels: member.labels.clone(),
        status,
    }
}

/// The leader's orphan-member sweep (ADR 0040 PR6): on a slow timer, if this
/// node is leader, auto-reclaim a node-identity claim
/// ([`Metadata::orphan_sweep_candidates`]) that has never activated and has
/// persisted, continuously, for at least `orphan_sweep_after` — proposing the
/// existing [`MetaCommand::RemoveMember`] for it, exactly as an operator
/// pruning it by hand would.
///
/// **Same template as [`detect_loop`] (ADR 0012's own doc calls this out as
/// its own precedent)**: the *decision* — which claims are candidates at all
/// — is the pure [`Metadata::orphan_sweep_candidates`]; this driver supplies
/// only timing (over the `Env` seam, never a wall clock) and the propose. Two
/// differences from `detect_loop`'s shape, both load-bearing:
///
/// - **The countdown lives here, not in `Metadata`.** A candidate's
///   first-observed instant is tracked in a **volatile**, per-leadership-stint
///   `BTreeMap<NodeId, Nanos>` (`first_seen`) — never replicated, mirroring
///   `detect_loop`'s own `leader_since`/the [`FailureDetector`]'s volatile
///   `last_seen` map. `Metadata` itself carries no wall-clock-derived state
///   (ADR 0003) — only the `Down`/`has_activated` fields the *decision* reads,
///   never a timestamp the *timing* would need to agree on across replicas.
///   A leadership change (or this leader losing and regaining leadership)
///   resets `first_seen` to empty — **acceptable, not a bug**: the sweep is
///   convergent (a genuinely still-eligible claim just starts its countdown
///   over under the new leader), only ever delayed, never skipped outright,
///   and a real activation in the meantime cancels it structurally (see
///   below) regardless of how many times the countdown restarts.
/// - **The control-voter exclusion is this loop's own responsibility, not
///   `Metadata::orphan_sweep_candidates`'s.** `RaftCore`'s live voter
///   config (`core.config()`) is a wholly different part of the system from
///   `Metadata` — a claim that is currently a live control voter (added via
///   `admin_add_control_member`, independent of whether its own `members`
///   row, if any, ever reached `Active`) must never be proposed for removal,
///   so every tick intersects the pure candidate set against `!voters.contains(..)`
///   before even starting (or continuing) a claim's countdown.
///
/// **Safety argument for the sweep racing a genuine late activation** (the
/// catastrophic case this mechanism must never get wrong): this loop computes
/// eligibility from a **snapshot** that can be stale by the time its
/// `RemoveMember` proposal actually applies — a real activation
/// (`liveness_transitions`'s `Down`→`Active` promotion, `detect_loop`) can
/// commit first, in between. This is safe **not** because of anything in this
/// loop, but because [`MetaCommand::RemoveMember`]'s own apply-time guard
/// (unchanged by this PR) re-checks the member's status **at apply time**,
/// against whatever the log already committed ahead of it: `Active`/`Joining`
/// is rejected outright. So regardless of which order the two proposals are
/// appended in, an already-`Active` member is never actually removed — the
/// reverse interleaving (removal commits, then a stray late `UpsertMember`
/// tries to resurrect it) cannot happen either, since `liveness_transitions`
/// only ever proposes a transition for a member it finds present in the
/// **same tick's own fresh read** of `Metadata` — once a member is gone, nothing
/// proposes bringing it back except a fresh, independent
/// [`MetaCommand::RegisterNode`] claim (a new registration, not a
/// resurrection of the old one).
async fn orphan_sweep_loop<E: Env>(
    env: E,
    core: Arc<Mutex<RaftCore>>,
    cache: Arc<Mutex<Metadata>>,
    orphan_sweep_after: Duration,
    metrics: MetricsHandle,
) {
    // Per-leadership-stint volatile state: when this stint first observed
    // each currently-eligible claim. Reset wholesale on any leadership/term
    // change (see this fn's own doc).
    let mut leader_since_term: Option<u64> = None;
    let mut first_seen: BTreeMap<NodeId, animus_env::Nanos> = BTreeMap::new();
    loop {
        env.sleep(ORPHAN_SWEEP_CHECK_INTERVAL).await;
        let now = env.now();
        let (term_now, voters) = {
            let core = core.lock().expect("raft core poisoned");
            if !core.is_leader() {
                leader_since_term = None;
                first_seen.clear();
                continue;
            }
            (core.term(), core.config())
        };
        if leader_since_term != Some(term_now) {
            leader_since_term = Some(term_now);
            first_seen.clear();
        }
        let candidates: BTreeSet<NodeId> = {
            let meta = cache.lock().expect("cache poisoned");
            meta.orphan_sweep_candidates()
                .into_iter()
                .filter(|id| !voters.contains(id))
                .collect()
        };
        // Anything no longer a candidate (activated, became a control voter,
        // got tablet-referenced, or was already removed by some other path)
        // stops counting down — its countdown, if any, is simply forgotten,
        // never resumed from where it left off if it becomes eligible again
        // later (a fresh claim's own countdown, same as a brand-new one).
        first_seen.retain(|id, _| candidates.contains(id));
        for id in &candidates {
            first_seen.entry(id.clone()).or_insert(now);
        }
        for id in &candidates {
            let since = first_seen[id];
            if now.duration_since(since) >= orphan_sweep_after {
                metrics.incr(Metric::OrphanMembersSwept);
                tracing::info!(
                    node = %id,
                    grace_period_secs = orphan_sweep_after.as_secs(),
                    "orphan-member sweep: proposing removal of a never-activated claim"
                );
                core.lock()
                    .expect("raft core poisoned")
                    .propose(MetaCommand::RemoveMember { node: id.clone() });
            }
        }
    }
}

/// Append and `fsync` any pending durable-state records to the WAL, then advance
/// the core's durable watermark so the now-on-disk entries become client-visible
/// (durable-before-visible, ADR 0009). Returns how many records were written.
/// **Runs on the consensus loop only** (ADR 0038 PR3) and is deliberately
/// cheap — engine apply and WAL *compaction* now run on the separate apply
/// task ([`meta_apply_and_compact`]), so this stays responsive to Raft
/// messages/heartbeats within the election timeout. Holds `wal_lock` so this
/// append cannot interleave with the apply task's compaction rewrite of the
/// same file.
async fn persist_wal<E: Env>(
    env: &E,
    core: &Arc<Mutex<RaftCore>>,
    wal_lock: &AsyncMutex<()>,
    progress: &PersistProgress,
) -> usize {
    let _wal = wal_lock.lock().await;
    // Capture the log high-water under the same lock as the drain: after we sync
    // the drained records, every entry up to here is durable. Entries appended
    // after this point ride the next flush.
    let (records, through, round) = {
        let mut core = core.lock().expect("raft core poisoned");
        let (records, round) = persist_round::drain_for_round(&mut core, progress);
        (records, core.last_log_index(), round)
    };
    let Some(round) = round else {
        debug_assert!(records.is_empty());
        return 0;
    };
    for record in &records {
        env.append(WAL, &PersistedState::encode_record(record))
            .await
            .expect("wal append");
    }
    env.sync(WAL).await.expect("wal sync");
    // The records are now durable: advance both watermarks under one acquisition
    // — the log index (which applies any now-durable committed entries) and the
    // persist round (which releases whatever `drive` buffered against it). Only
    // after this is the proposal observable.
    {
        let mut core = core.lock().expect("raft core poisoned");
        core.mark_durable_through(through);
        progress.complete_drain(round);
    }
    records.len()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use animus_env::Nanos;

    use super::*;
    use crate::detector::FailureDetector;
    use crate::meta::{Member, Metadata};

    fn meta_with(node: NodeId, status: NodeStatus) -> Metadata {
        let mut m = Metadata::default();
        m.members.insert(
            node,
            Member {
                labels: BTreeMap::new(),
                status,
                has_activated: status == NodeStatus::Active,
            },
        );
        m
    }

    fn detector_silent_since(node: NodeId, last_seen: Nanos) -> FailureDetector {
        let mut d = FailureDetector::new(DETECT_TIMEOUT);
        d.observe(node, last_seen);
        d
    }

    #[test]
    fn grace_period_suppresses_down_then_allows_it() {
        // An Active member that has been silent past DETECT_TIMEOUT.
        let meta = meta_with(nid(7), NodeStatus::Active);
        let det = detector_silent_since(nid(7), Nanos(0));
        let now = Nanos(DETECT_TIMEOUT.as_nanos() as u64 + 1);

        // Inside the grace period (allow_down = false): no Down proposed.
        assert!(liveness_transitions(&meta.members, &det, now, false).is_empty());

        // Grace elapsed (allow_down = true): the Down transition is proposed.
        let outs = liveness_transitions(&meta.members, &det, now, true);
        assert_eq!(outs.len(), 1);
        assert!(matches!(
            &outs[0],
            MetaCommand::UpsertMember {
                node,
                status: NodeStatus::Down,
                ..
            } if *node == nid(7)
        ));
    }

    #[test]
    fn recovery_is_allowed_even_during_grace() {
        // A Down member whose heartbeat just arrived recovers regardless of the
        // grace gate (positive evidence, no false-positive risk).
        let meta = meta_with(nid(7), NodeStatus::Down);
        let det = detector_silent_since(nid(7), Nanos(1_000));
        let now = Nanos(1_000); // fresh heartbeat → alive
        let outs = liveness_transitions(&meta.members, &det, now, false);
        assert_eq!(outs.len(), 1);
        assert!(matches!(
            &outs[0],
            MetaCommand::UpsertMember {
                node,
                status: NodeStatus::Active,
                ..
            } if *node == nid(7)
        ));
    }

    /// **ADR 0040 PR6 safety argument, "no resurrection" half.** Once a
    /// member has been removed from `Metadata.members` (by the orphan sweep
    /// or an ordinary decommission alike), `liveness_transitions` — the
    /// **only** production caller that ever proposes an `UpsertMember{Active}`
    /// promotion — never proposes one for it, even if the detector still
    /// privately tracks it as alive (a stray heartbeat that arrived after
    /// removal). This is what makes a stray in-flight promotion structurally
    /// incapable of resurrecting an already-removed claim: the decision
    /// function is filtered by presence in the **same fresh `Metadata` read**
    /// it is computed from, every tick, not by some snapshot a proposer took
    /// once and might replay stale.
    #[test]
    fn liveness_transitions_never_proposes_for_an_absent_member() {
        let empty: BTreeMap<NodeId, Member> = BTreeMap::new();
        // The detector believes `nid(301)` alive (a heartbeat arrived), but
        // `Metadata` no longer has a `members` row for it at all.
        let det = detector_silent_since(nid(301), Nanos(1_000));
        let now = Nanos(1_000);
        assert!(
            liveness_transitions(&empty, &det, now, true).is_empty(),
            "must never propose a transition for a member `Metadata` doesn't have"
        );
    }

    /// ADR 0032 PR3: a member `RemoveMember` has already pruned from
    /// `Metadata.members` is exactly what `stale_tracked_ids` reports — a
    /// still-tracked member is left alone.
    #[test]
    fn stale_tracked_ids_reports_only_removed_members() {
        let meta = meta_with(nid(7), NodeStatus::Active);
        let mut det = detector_silent_since(nid(7), Nanos(1_000));
        det.observe(nid(99), Nanos(1_000)); // 99 was tracked but is not in `meta.members`
        assert_eq!(stale_tracked_ids(&meta.members, &det), vec![nid(99)]);

        // Once `members` no longer has 7 either (a real removal), it joins the
        // stale set too.
        let empty: BTreeMap<NodeId, Member> = BTreeMap::new();
        assert_eq!(stale_tracked_ids(&empty, &det), vec![nid(7), nid(99)]);
    }

    /// A `detect_loop` tick calling `forget` for every `stale_tracked_ids`
    /// result actually stops the detector from tracking the removed member —
    /// the fix's whole point, proven at the detector level (the loop itself
    /// is exercised end to end in `tests/failure_detection.rs`).
    #[test]
    fn forgetting_stale_tracked_ids_stops_tracking_them() {
        let mut det = detector_silent_since(nid(99), Nanos(1_000));
        let empty: BTreeMap<NodeId, Member> = BTreeMap::new();
        for id in stale_tracked_ids(&empty, &det) {
            det.forget(id);
        }
        assert!(!det.tracks(nid(99)));
    }

    // --- ADR 0038 PR3: the apply task's watermark-gated tail replay ---------
    //
    // These drive `meta_apply_and_compact` directly (white-box — it is not
    // `pub`), so they don't need a real `RaftNode`/`Simulator` to pin the
    // exact index a real restart's engine watermark would land on: they set
    // it up by hand and assert precisely.

    fn upsert(node: NodeId) -> MetaCommand {
        MetaCommand::UpsertMember {
            node,
            labels: BTreeMap::new(),
            status: NodeStatus::Active,
        }
    }

    /// If the apply task's watermark already covers every committed-and-durable
    /// command the core has buffered (as a restart's engine-rebuild can leave
    /// it, if nothing committed after the crash), a pass does nothing: no
    /// engine write, no cache publish, no watch bump. The boundary case for
    /// the "skip anything the engine already durably reflects" rule ADR 0038
    /// PR3's restart-recovery contract requires.
    #[tokio::test]
    async fn apply_and_compact_is_a_no_op_when_the_watermark_already_covers_everything() {
        let sim = animus_sim::Simulator::new(0xF00D);
        let env = sim.env(nid(0));

        let mut core = RaftCore::new(nid(0), &[nid(0)], Nanos(0), 7);
        core.tick(Nanos(1_000_000_000), 7); // sole leader; index 1 = election no-op
        for i in 0..5u64 {
            core.propose(upsert(nid(i))); // indices 2..=6
        }
        core.mark_durable_through(core.last_log_index());
        let last_applied = core.last_applied();
        assert_eq!(last_applied, 6);

        let core = Arc::new(Mutex::new(core));
        let engine = animus_storage::MemoryEngine::new();
        let cache = Arc::new(Mutex::new(Metadata::default()));
        let engine_applied = Arc::new(AtomicU64::new(0));
        let delta_ring = Arc::new(Mutex::new(DeltaRing::default()));
        let watch = MetadataWatch::default();
        let wal_lock = Arc::new(AsyncMutex::new(()));
        let mut shadow = Metadata::default();
        // Pretend a restart's rebuild already caught the watermark up to
        // cover every one of these commands.
        let mut watermark = last_applied;

        let did_work = meta_apply_and_compact(
            &env,
            &core,
            &engine,
            &cache,
            &engine_applied,
            &delta_ring,
            &watch,
            &wal_lock,
            &PersistProgress::default(),
            &mut shadow,
            &mut watermark,
        )
        .await;

        assert!(
            !did_work,
            "nothing to do: every drained command was already covered by the watermark"
        );
        assert_eq!(shadow, Metadata::default(), "shadow must not have advanced");
        assert_eq!(
            *cache.lock().expect("cache poisoned"),
            Metadata::default(),
            "cache must not have been (re)published"
        );
        assert_eq!(
            engine_applied.load(Ordering::SeqCst),
            0,
            "engine_applied must not advance on a no-op pass"
        );
        assert!(
            engine.entries().await.expect("engine scan").is_empty(),
            "nothing should have been written to the engine"
        );
        assert_eq!(
            delta_ring.lock().expect("delta ring poisoned").len(),
            0,
            "the ring stays untouched on a no-op pass"
        );
    }

    /// The general case: the watermark covers only a *prefix* of what the
    /// core has buffered (the realistic post-restart shape — a recovered
    /// core's freshly-established commit frontier can run well past what the
    /// engine had durably merged before the crash). Only the tail beyond the
    /// watermark is derived/merged; the pre-seeded `shadow` (standing in for
    /// a restart's `rebuild_metadata_from_engine`) plus that tail reaches the
    /// same union the full command set would.
    #[tokio::test]
    async fn apply_and_compact_replays_only_the_tail_beyond_the_watermark() {
        let sim = animus_sim::Simulator::new(0xF00D);
        let env = sim.env(nid(0));

        let mut core = RaftCore::new(nid(0), &[nid(0)], Nanos(0), 7);
        core.tick(Nanos(1_000_000_000), 7); // index 1: leader no-op
        for i in 0..5u64 {
            core.propose(upsert(nid(i))); // indices 2..=6
        }
        core.mark_durable_through(core.last_log_index());
        let last_applied = core.last_applied();
        assert_eq!(last_applied, 6);

        let core = Arc::new(Mutex::new(core));
        let engine = animus_storage::MemoryEngine::new();
        let cache = Arc::new(Mutex::new(Metadata::default()));
        let engine_applied = Arc::new(AtomicU64::new(0));
        let delta_ring = Arc::new(Mutex::new(DeltaRing::default()));
        let watch = MetadataWatch::default();
        let wal_lock = Arc::new(AsyncMutex::new(()));

        // Simulate "the engine already durably reflects the no-op and the
        // first two upserts" (members 0 and 1) — exactly what a genuine
        // restart's `rebuild_metadata_from_engine` would have produced had
        // the crash happened right there.
        let mut shadow = Metadata::default();
        let _ = mirror::apply_and_derive_mirror(&mut shadow, &MetaCommand::NoOp);
        let _ = mirror::apply_and_derive_mirror(&mut shadow, &upsert(nid(0)));
        let _ = mirror::apply_and_derive_mirror(&mut shadow, &upsert(nid(1)));
        let mut watermark = 3; // covers indices 1..=3

        let did_work = meta_apply_and_compact(
            &env,
            &core,
            &engine,
            &cache,
            &engine_applied,
            &delta_ring,
            &watch,
            &wal_lock,
            &PersistProgress::default(),
            &mut shadow,
            &mut watermark,
        )
        .await;

        assert!(did_work);
        assert_eq!(
            watermark, last_applied,
            "watermark advances to the core's real frontier"
        );
        let published = cache.lock().expect("cache poisoned").clone();
        assert_eq!(
            published.members.len(),
            5,
            "the tail (members 2..4) merged on top of the pre-seeded state \
             reaches the full union"
        );
        assert_eq!(
            engine_applied.load(Ordering::SeqCst),
            last_applied,
            "engine_applied publishes the new watermark"
        );
        assert_eq!(
            watch.latest(),
            last_applied,
            "a parked metadata_watch() waiter would see this advance"
        );
        // Only the tail (3 commands: indices 4, 5, 6) should have been
        // written — not the whole 6-command history the core's drain
        // returned (proving the `index <= watermark` entries were skipped,
        // not silently re-derived/re-merged).
        let engine_entries = engine.entries().await.expect("engine scan");
        assert_eq!(
            engine_entries.len(),
            3 + 1, // 3 member upserts + the shared `_applied_index` watermark key
            "expected exactly the tail's writes plus the watermark key, got {} entries",
            engine_entries.len()
        );

        // ADR 0038 PR5: the delta ring only ever saw the tail (indices 4..6)
        // — it has no entries for 1..3, since those were skipped by the
        // watermark gate before ever reaching `mirror::apply_and_derive_mirror`.
        // A `WatchMetadata` caller whose `last_seen` is inside the pre-seeded
        // prefix (e.g. 0, which this pass never touched at all) still falls
        // back correctly, since the ring's window starts at index 4.
        let ring = delta_ring.lock().expect("delta ring poisoned");
        assert_eq!(ring.len(), 3, "one ring entry per tail command");
        assert_eq!(
            ring.writes_since(3, last_applied),
            Some(
                (4..=last_applied)
                    .flat_map(|i| {
                        // Raft index `i` carries `upsert(i - 2)` in this
                        // test's setup (index 2 = upsert(0), .., index 6 =
                        // upsert(4)) — recomputed against a fresh `Metadata`
                        // since `UpsertMember`'s derived write only reads
                        // back its own just-written member entry, so it's
                        // independent of any other node's prior state.
                        let (_, writes) = mirror::apply_and_derive_mirror(
                            &mut Metadata::default(),
                            &upsert(nid(i - 2)),
                        );
                        writes
                    })
                    .collect::<Vec<_>>()
            ),
            "the ring covers exactly (watermark_before, watermark_after]"
        );
        assert_eq!(
            ring.writes_since(0, last_applied),
            None,
            "a caller stuck before the ring's own window falls back to a full fetch"
        );
    }

    /// A `Waker` that just flags whether it was ever woken, so a test can
    /// assert on wake delivery without needing a real executor.
    struct WakeFlag(std::sync::atomic::AtomicBool);

    impl std::task::Wake for WakeFlag {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    impl WakeFlag {
        fn woken(&self) -> bool {
            self.0.load(Ordering::SeqCst)
        }
    }

    fn test_waker() -> (Waker, Arc<WakeFlag>) {
        let flag = Arc::new(WakeFlag(std::sync::atomic::AtomicBool::new(false)));
        let waker = Waker::from(flag.clone());
        (waker, flag)
    }

    /// Two independent `changed()` callers on one [`MetadataWatch`] must
    /// *both* be woken by a single `bump()`. Red on the old single-slot
    /// `AtomicWaker`: registering the second waiter's waker would have
    /// evicted the first's, so only the most recently registered waiter
    /// would ever wake — exactly the ADR 0035 PR5 reconciler-loop-vs-
    /// `WatchMetadata`-long-poll collision (issue #276).
    #[test]
    fn bump_wakes_every_registered_waiter_not_just_the_most_recent() {
        let watch = MetadataWatch::default();

        let (waker_a, woken_a) = test_waker();
        let (waker_b, woken_b) = test_waker();
        let mut cx_a = Context::from_waker(&waker_a);
        let mut cx_b = Context::from_waker(&waker_b);

        let mut fut_a = watch.changed(0);
        let mut fut_b = watch.changed(0);

        // Park both: register-before-check means each poll registers its own
        // slot before observing `applied == 0 == last_seen`, so both return
        // Pending.
        assert_eq!(Pin::new(&mut fut_a).poll(&mut cx_a), Poll::Pending);
        assert_eq!(Pin::new(&mut fut_b).poll(&mut cx_b), Poll::Pending);
        assert_eq!(watch.registered_waiters(), 2);

        watch.bump(1);

        assert!(woken_a.woken(), "the first-registered waiter must be woken");
        assert!(
            woken_b.woken(),
            "the second-registered waiter must ALSO be woken -- a single-slot \
             AtomicWaker would have evicted the first registration and only \
             woken this one, or vice versa depending on registration order"
        );

        // Both resolve on their next poll.
        assert_eq!(Pin::new(&mut fut_a).poll(&mut cx_a), Poll::Ready(1));
        assert_eq!(Pin::new(&mut fut_b).poll(&mut cx_b), Poll::Ready(1));
    }

    /// A `changed()` future dropped before `bump()` (e.g. an abandoned
    /// `WatchMetadata` long-poll whose client disconnected) must remove its
    /// own slot from the registry — no leak — and must not prevent a
    /// surviving waiter from being woken.
    #[test]
    fn dropped_waiter_does_not_leak_its_slot_or_block_the_survivor() {
        let watch = MetadataWatch::default();

        let (waker_survivor, woken_survivor) = test_waker();
        let (waker_dropped, _woken_dropped) = test_waker();
        let mut cx_survivor = Context::from_waker(&waker_survivor);
        let mut cx_dropped = Context::from_waker(&waker_dropped);

        let mut fut_survivor = watch.changed(0);
        assert_eq!(
            Pin::new(&mut fut_survivor).poll(&mut cx_survivor),
            Poll::Pending
        );

        {
            let mut fut_dropped = watch.changed(0);
            assert_eq!(
                Pin::new(&mut fut_dropped).poll(&mut cx_dropped),
                Poll::Pending
            );
            assert_eq!(
                watch.registered_waiters(),
                2,
                "both waiters hold a slot while both are still parked"
            );
        } // fut_dropped drops here without ever resolving.

        assert_eq!(
            watch.registered_waiters(),
            1,
            "the dropped waiter's slot must be reclaimed, not leaked"
        );

        watch.bump(1);

        assert!(
            woken_survivor.woken(),
            "the surviving waiter must still be woken after the other one dropped"
        );
        assert_eq!(
            Pin::new(&mut fut_survivor).poll(&mut cx_survivor),
            Poll::Ready(1)
        );
        assert_eq!(
            watch.registered_waiters(),
            0,
            "the survivor's own slot is gone too once it resolves and drops"
        );
    }
}
