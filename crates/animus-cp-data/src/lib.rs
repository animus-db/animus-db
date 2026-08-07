//! Leaderful per-tablet Raft **data plane** (ADR 0016 / ADR 0017): each tablet is
//! its own Raft group with a single leader serving linearizable single-tablet
//! reads and writes, durable on a real [`StorageEngine`].
//!
//! This is the CP counterpart to the leaderless AP `animus-data` plane, built
//! additively. It reuses the control plane's generic, sync, I/O-free
//! [`RaftCore`](animus_control::RaftCore) (ADR 0009) — instantiated here with a
//! key-value command and a `DRIVER_APPLIED` state machine, so committed commands
//! are applied by **this** async driver to the engine rather than in-core.
//!
//! Stage status (ADR 0017): **B.1 (writes) + B.2 (ReadIndex reads)**. The driver
//! recovers from its WAL, replicates `KvCommand`s through Raft, fsyncs the WAL
//! before acting (durable-before-visible), and applies committed-and-durable
//! commands to the engine in commit order (the Raft index is the MVCC version, so
//! per-key LWW reproduces the agreed total order). Linearizable reads use
//! **ReadIndex** ([`RaftKvNode::linearizable_get`]): a read-barrier quorum probe
//! confirms current leadership (no log entry, no wall clock) before the leader
//! serves from its local engine. A **linearizable compare-and-swap**
//! ([`RaftKvNode::cas`] / [`RaftKvNode::compare_and_swap`], `KvCommand::Cas`) is
//! decided at *apply* time in commit order against the committed engine state, so
//! every replica makes the identical accept/reject choice and concurrent CAS from
//! the same `expected` have exactly one winner; the outcome is recorded keyed by
//! the entry's log index for the proposer to read. **A.2** adds compaction +
//! streaming snapshots:
//! the leader snapshots its engine image, truncates the Raft log prefix, and
//! catches a lagging follower up via the chunked `InstallSnapshot` (engine bytes),
//! which the follower writes into its own engine. **C** adds single-server
//! **membership change** ([`RaftKvNode::change_membership`]) — config-in-log in the
//! shared `RaftCore` — so the group can grow or reconfigure a replica off a failed
//! node. **D** adds **tablet split** ([`RaftKvNode::propose_split`]): the split
//! point is agreed through the Raft log, every replica tombstones the handed-off
//! upper range, and that range seeds a new independent group
//! ([`RaftKvNode::start_seeded`]).

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use animus_control::raft::{Out, RaftCore, RaftMsg, StateMachine};
use animus_control::{PersistedState, ProposeResult};
use animus_env::{Coresident, Env, EnvExt, Metric, MetricsHandle, NodeId, PRIMARY_STREAM};
use animus_storage::{MergeOp, StorageEngine};
use animus_tablet::KeyRange;
use futures::future::{Either, select};
use futures::lock::Mutex as AsyncMutex;
use futures::task::AtomicWaker;
use serde::{Deserialize, Serialize};

mod codec;

/// The **wake-on-propose** signal (ADR 0017 single-write-latency fix): a shared
/// flag + executor-agnostic waker that lets a proposer (`put`/`delete`/`cas`/
/// `propose_split`/`change_membership`) nudge the consensus loop to replicate the
/// freshly appended entry *immediately*, instead of leaving it parked in its
/// `select(recv, timer)` until the next ~50ms heartbeat tick.
///
/// [`AtomicWaker`] is deliberately executor-agnostic: it works under both
/// `SimEnv`'s custom `ArcWake`-based executor (where the wake, running
/// synchronously on the single thread, marks the driver task ready for the next
/// run-loop poll — fully deterministic) and tokio's multi-threaded `ProdEnv`
/// (where it resolves the register/wake race). No tokio-only primitive is used, so
/// determinism under `SimEnv` is preserved.
#[derive(Default)]
struct ProposeSignal {
    /// Set by a proposer, consumed (swapped false) by the consensus loop.
    pending: AtomicBool,
    /// The consensus loop's waker, registered each time it parks.
    waker: AtomicWaker,
}

impl ProposeSignal {
    /// A proposer nudges the consensus loop: raise the flag, then wake it. Order
    /// matters — the flag is visible before the wake, so the loop's poll (which
    /// registers *then* checks the flag) can never miss it.
    fn notify(&self) {
        self.pending.store(true, Ordering::Release);
        self.waker.wake();
    }
}

/// A future that resolves once a propose is pending, for the consensus loop's
/// `select`. Registers the loop's waker *before* checking the flag (the
/// [`AtomicWaker`] discipline that avoids a lost wakeup), and consumes the flag on
/// resolve so the next park doesn't spuriously fire.
struct ProposePending<'a> {
    signal: &'a ProposeSignal,
}

impl Future for ProposePending<'_> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.signal.waker.register(cx.waker());
        if self.signal.pending.swap(false, Ordering::AcqRel) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

/// The physical key-space a `RaftKvNode`'s group is confined to within a
/// possibly-**shared** `StorageEngine` (PR3 of the single-command-split
/// redesign, ADR 0028): every physical key this group ever writes is
/// `prefix || key`, and a read is bounded to physical keys whose stripped
/// (post-prefix) suffix falls inside `range`. `prefix` is meant to identify
/// the *table* (stable across splits — many sibling tablets of one table can
/// share a prefix), `range` the *tablet's own sub-portion* within it (the
/// same range already recorded in the control plane's `Metadata`).
///
/// [`whole`](Self::whole) — empty prefix, the whole keyspace — makes every
/// physical-key operation an identity transform, so a `RaftKvNode` with a
/// dedicated (non-shared) engine (every existing caller today) behaves
/// byte-for-byte as before. **Not yet wired into any real caller** —
/// `animusd` still opens one dedicated engine per tablet; a later PR threads
/// a real per-table prefix through here so multiple tablets can safely share
/// one node-wide engine.
///
/// The legacy `KvCommand::Split`/`SPLIT_BOUND_KEY` mechanism (superseded by a
/// single-command, control-plane-driven split, and deleted once that lands)
/// is deliberately **not** hardened against a non-default scope: its
/// reserved marker key and `keys_from`'s handoff capture are only
/// prefix-scoped, not range-scoped (see their call sites), so two sibling
/// tablets of the same table sharing a prefix could in principle collide on
/// the marker. Harmless in practice: nothing combines a non-default scope
/// with `propose_split` today, and the mechanism is deleted before that
/// combination could arise for real.
///
/// **`range` is live-narrowable** ([`narrow`](Self::narrow)), not fixed at
/// construction: when this tablet is later the *source* of a (control-plane,
/// single-command) split, its own range shrinks while its physical data does
/// **not** move — the handed-off portion stays physically resident under the
/// same `prefix` until the new sibling tablet's own writes/GC reclaim it. A
/// stale, too-wide `range` is harmless for a caller-bounded read (the
/// physical scan is already bounded by the caller's own up-to-date
/// `Metadata`-derived bounds), but it is **not** harmless for
/// [`engine_image`] — an unbounded, self-contained snapshot capture with no
/// caller-supplied bounds — which would otherwise ship the already-handed-off
/// portion to a new replica joining this (shrunk) tablet's group, duplicating
/// data a *different* Raft group is now the sole authority for. All clones of
/// a `StorageScope` share the same live `range` (an `Arc`), so narrowing the
/// copy held by the driver task also narrows the one `RaftKvNode` itself
/// holds.
#[derive(Clone, Debug)]
pub struct StorageScope {
    prefix: Vec<u8>,
    range: Arc<Mutex<KeyRange>>,
}

impl StorageScope {
    /// No prefix, the whole keyspace — every physical-key operation is an
    /// identity transform (today's dedicated-engine behavior).
    #[must_use]
    pub fn whole() -> Self {
        Self {
            prefix: Vec::new(),
            range: Arc::new(Mutex::new(KeyRange::whole())),
        }
    }

    /// A scope confined to `prefix || range` within a shared engine.
    #[must_use]
    pub fn new(prefix: Vec<u8>, range: KeyRange) -> Self {
        Self {
            prefix,
            range: Arc::new(Mutex::new(range)),
        }
    }

    /// Update this scope's live range (see the type doc) — every clone of
    /// this `StorageScope` observes the change immediately. The caller (which
    /// watches `Metadata` for this tablet's current range) is trusted to only
    /// ever narrow it, mirroring the legacy `current_split_bound`'s
    /// monotonic-narrowing discipline.
    pub fn narrow(&self, new_range: KeyRange) {
        *self.range.lock().expect("storage scope range poisoned") = new_range;
    }

    /// The physical storage key for logical `key`.
    fn physical(&self, key: &[u8]) -> Vec<u8> {
        let mut out = self.prefix.clone();
        out.extend_from_slice(key);
        out
    }

    /// If `physical_key` belongs to this scope — starts with `prefix`, and
    /// the stripped suffix falls inside the *current* `range` — the stripped
    /// logical key, else `None`. The read-side counterpart of
    /// [`physical`](Self::physical), used wherever a shared-engine
    /// scan/snapshot must not leak another tenant's keys.
    fn strip_in_range<'a>(&self, physical_key: &'a [u8]) -> Option<&'a [u8]> {
        let logical = physical_key.strip_prefix(self.prefix.as_slice())?;
        let range = self.range.lock().expect("storage scope range poisoned");
        range.contains(logical).then_some(logical)
    }

    /// Whether `storage` currently holds any live data in this scope.
    ///
    /// On a *dedicated* (non-shared) engine, "does this tablet already have
    /// data" (e.g. a real on-disk `LsmEngine`'s own version counter) is what
    /// distinguishes a node **reforming** a group it already hosted before a
    /// restart (start with the full voter config — it may need to elect
    /// immediately) from one **joining fresh** as a reconciler-placed spare
    /// (start as a quiet non-voter). On a *shared* engine there is no
    /// per-tablet dedicated store left to ask, so this scoped presence check
    /// is the direct replacement: it reads only this scope's own physical
    /// range, never a sibling tenant's.
    #[must_use]
    pub async fn has_data<S: StorageEngine>(&self, storage: &S) -> bool {
        let range = self
            .range
            .lock()
            .expect("storage scope range poisoned")
            .clone();
        match &range.end {
            Some(end) => {
                let physical_start = self.physical(&range.start);
                let physical_end = self.physical(end);
                storage
                    .scan(&physical_start, &physical_end)
                    .await
                    .map(|rows| !rows.is_empty())
                    .unwrap_or(false)
            }
            // Open-ended range: no finite physical upper bound to scan, so
            // fall back to the same whole-engine-then-filter shape `keys_from`/
            // `engine_image` already use for the unbounded case.
            None => storage
                .entries()
                .await
                .map(|rows| rows.iter().any(|(k, _)| self.strip_in_range(k).is_some()))
                .unwrap_or(false),
        }
    }

    /// If `physical_key` starts with this scope's `prefix` (regardless of
    /// `range`), the stripped logical key. Used only by the legacy
    /// Split/`SPLIT_BOUND_KEY` path (see the type doc's caveat).
    fn strip_prefix_only<'a>(&self, physical_key: &'a [u8]) -> Option<&'a [u8]> {
        physical_key.strip_prefix(self.prefix.as_slice())
    }
}

impl Default for StorageScope {
    fn default() -> Self {
        Self::whole()
    }
}

/// The data plane's Raft log command: a key-value mutation (or the election
/// no-op). Keys/values are opaque bytes; ordering + durability come from Raft.
///
/// **`fence`** (every mutating variant except `Split`) is the leader's own
/// belief, stamped in **at propose time**, of this tablet's current key range.
/// It rides inside the command like `Cas`'s `expected` — opaque to `RaftCore`,
/// interpreted only by `apply_and_compact` — so every replica makes the
/// *identical* accept/reject decision for a given log entry, regardless of how
/// far each replica has independently progressed through learning the tablet's
/// range has changed (e.g. a metadata-driven split, ADR 0028). This is
/// deliberately **not** a locally-polled check: two replicas polling their own
/// view of "the current range" could disagree about the very same entry (one
/// has observed a split, one hasn't) and silently apply it differently — a
/// real safety violation, not just staleness. A command whose key(s) fall
/// outside its own embedded `fence` is a deterministic no-op at apply time
/// (see each apply arm's doc for the exact per-variant behavior). Every
/// existing proposer stamps `KeyRange::whole()` (unconstrained, i.e. today's
/// behavior is unchanged); a narrower fence is set only by a `*_fenced`
/// proposer, not yet used by any real caller (that lands with the
/// single-command-split integration).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvCommand {
    /// Set `key` to `value`, iff `key` falls inside `fence` (see the type-level
    /// doc below for what `fence` means and why every replica checks it at
    /// *apply* time rather than the proposer checking it once).
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        fence: KeyRange,
    },
    /// **Batch put**: set every `(key, value)` in one Raft log entry — one propose,
    /// one commit round, one apply. All keys are merged at the entry's Raft `index`
    /// (the shared MVCC version): the keys are distinct, so per-key LWW is
    /// well-defined, and re-applying on recovery is idempotent exactly as a single
    /// `Put` is. The throughput win over N individual `Put`s is one consensus round
    /// for the whole batch instead of one per key (ADR 0017 — bulk-write batching).
    /// Within one tablet the batch is atomic (it either commits whole or not at all);
    /// a cross-tablet batch is split into one `Batch` per tablet by the caller and is
    /// not atomic across tablets (matching DynamoDB `BatchWriteItem` semantics).
    /// `fence` gates the *whole* batch: if any key falls outside it, none of the
    /// batch applies (preserves the batch's atomicity — see the type-level doc).
    Batch {
        puts: Vec<(Vec<u8>, Vec<u8>)>,
        fence: KeyRange,
    },
    /// Remove `key` (a tombstone in the engine), iff `key` falls inside `fence`.
    Delete { key: Vec<u8>, fence: KeyRange },
    /// **Linearizable compare-and-swap**: set `key` to `value` iff the key's
    /// current committed value equals `expected` (`None` == "only if absent")
    /// *and* `key` falls inside `fence`. Evaluated at *apply* time against the
    /// engine's committed state, in commit order, so every replica makes the
    /// identical accept/reject decision (no clock/RNG) — and two CAS racing
    /// from the same `expected` have exactly one winner (whichever Raft
    /// ordered first). The outcome is recorded in driver state keyed by the
    /// entry's log index for the proposer to read; a fenced-out CAS records
    /// `false` (see the apply-time fence check's doc for why).
    Cas {
        key: Vec<u8>,
        expected: Option<Vec<u8>>,
        value: Vec<u8>,
        fence: KeyRange,
    },
    /// **Split** this tablet at `at` (ADR 0017 D): keys `>= at` move to a new
    /// tablet group. Agreed through the Raft log so every replica splits at the
    /// same point in the command order; on apply each replica tombstones the
    /// handed-off range `[at, ∞)` from its engine (it now serves only `[lo, at)`).
    /// The new group is bootstrapped (seeded with the `[at, ∞)` data) separately.
    Split { at: Vec<u8> },
    /// The leader's no-op-on-election (Raft); applies nothing.
    NoOp,
}

/// The data-plane state machine is `DRIVER_APPLIED`: the real applied state lives
/// in the [`StorageEngine`], written by the async driver, so the in-core image is
/// a unit placeholder and the core never applies it in-core (ADR 0017).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvState;

impl StateMachine<KvCommand> for KvState {
    const DRIVER_APPLIED: bool = true;
    fn apply(&mut self, _command: &KvCommand) {
        unreachable!("KvState is DRIVER_APPLIED; the driver applies to the engine");
    }
    fn noop() -> KvCommand {
        KvCommand::NoOp
    }
}

/// The per-tablet Raft core, specialized to the KV command + driver-applied state.
type KvCore = RaftCore<KvCommand, KvState>;

/// The data-plane wire: Raft consensus traffic plus the **ReadIndex** read-barrier
/// probes (ADR 0017). The probes are *not* consensus traffic — they never touch
/// `RaftCore`; the driver handles them — so ReadIndex lives entirely in this crate
/// and the shared `RaftCore`/`RaftMsg` are untouched. Framed with the crate's
/// compact **binary codec** ([`codec`], audit P2) — not serde_json, whose
/// decimal-array rendering of `Vec<u8>` payloads cost ~3–4x on the wire.
#[derive(Clone, Debug)]
pub(crate) enum KvWire {
    /// A control-plane-shaped Raft message for this group.
    Raft(RaftMsg<KvCommand>),
    /// Leader → peers: "are you still in term `term`?" (a ReadIndex barrier,
    /// tagged with the leader's read `epoch`).
    ReadProbe { term: u64, epoch: u64 },
    /// Peer → leader: "yes, I am still in term `term`" for read `epoch`. A quorum
    /// of these confirms the prober is still leader as of now (a newer leader would
    /// require a quorum to have moved to a higher term, which would *not* ack).
    ReadProbeAck { term: u64, epoch: u64 },
}

/// How long a [`linearizable_get`](RaftKvNode::linearizable_get) waits for a quorum
/// read-barrier confirmation before giving up.
const READ_TIMEOUT: Duration = Duration::from_secs(5);
/// Poll granularity while a linearizable read waits for confirmation.
const READ_POLL: Duration = Duration::from_millis(20);

/// How long [`compare_and_swap`](RaftKvNode::compare_and_swap) waits for its
/// proposed entry to commit + apply (so its outcome is recorded) before giving up.
const CAS_TIMEOUT: Duration = Duration::from_secs(5);
/// Poll granularity while a CAS waits for its committed outcome.
const CAS_POLL: Duration = Duration::from_millis(20);

/// Compact (snapshot the engine + truncate the Raft log prefix) once this many
/// entries have been applied past the current snapshot base, bounding the WAL.
const COMPACT_THRESHOLD: u64 = 64;

/// Leader-side ReadIndex state: per in-flight read `epoch`, the term it was issued
/// under and the set of peers (including self) that have confirmed leadership.
#[derive(Default)]
struct ReadState {
    next_epoch: u64,
    /// `epoch -> (term, acking nodes)`.
    pending: BTreeMap<u64, (u64, BTreeSet<NodeId>)>,
}

/// Per-CAS outcomes recorded at apply time, keyed by the entry's **Raft log
/// index** (the value [`ProposeResult::Accepted`] hands the proposer): `true` if
/// the swap happened, `false` if `expected` did not match. Every replica records
/// the identical value because the CAS is decided deterministically in commit
/// order against the same committed engine state. The proposer polls until the
/// entry is applied, then reads its index here (see [`RaftKvNode::cas_result`] /
/// [`RaftKvNode::compare_and_swap`]).
#[derive(Default)]
struct CasResults {
    outcomes: BTreeMap<u64, bool>,
}

/// WAL file for a tablet group's Raft log (distinct from the control plane's
/// `raft.wal`, so a node can host both without collision). Public so a teardown
/// path (drop-table GC, ADR 0024) can delete a stopped group's WAL alongside its
/// engine files.
pub const WAL: &str = "raftkv.wal";

/// A running data-plane Raft node for one tablet group. Cheap to clone; clones
/// share the one [`RaftCore`] + engine. The driver loop runs on `env`.
#[derive(Clone)]
pub struct RaftKvNode<E: Env, S: StorageEngine> {
    env: E,
    core: Arc<Mutex<KvCore>>,
    storage: S,
    all_nodes: Vec<NodeId>,
    reads: Arc<Mutex<ReadState>>,
    cas: Arc<Mutex<CasResults>>,
    /// Highest Raft log index the **apply task** has merged into the engine. The
    /// consensus loop advances the core's `last_applied` (its buffer cursor) as soon
    /// as entries are committed+durable, but the async apply task lags behind
    /// merging them into the engine — so linearizable reads gate on *this* (engine
    /// progress), never `last_applied`, or they could read past the engine's state.
    engine_applied: Arc<AtomicU64>,
    /// This group's current split boundary — `Some(k)` means every split so far
    /// has narrowed its valid range to `[lo, k)`; `None` before it has ever
    /// split. An in-memory cache of the durable [`SPLIT_BOUND_KEY`] entry,
    /// refreshed by the apply task at recovery and after each successful split
    /// (see [`current_split_bound`](Self::current_split_bound)'s doc for why it
    /// lives in the engine rather than in-core Raft state). A group can be
    /// split **more than once** over its life: each split narrows this further,
    /// and a new `Split { at }` is only accepted if `at` is strictly less than
    /// the current value (`None` ⇒ always accepted) — see the `KvCommand::Split`
    /// apply arm. This still rejects the same race it always has (two proposers
    /// racing to split the *same* still-unbounded or equally-bounded group at
    /// the same moment: whichever commits second finds `at` no longer strictly
    /// less than the boundary the first just set) while allowing a *later*,
    /// genuinely new split once the boundary has moved.
    ///
    /// **Deliberately just the current value, not a history of every key ever
    /// applied**: a caller confirming "did my specific key `K` ever apply" can
    /// get an ambiguous answer once more than one split has happened (`K`
    /// applied, then a *later* split narrowed past it — this field no longer
    /// equals `K`, even though `K` did apply). That ambiguity is intentional
    /// and safe here: this is O(1) state forever (not O(number of splits this
    /// lineage has ever done), which an ever-growing per-split history would
    /// be — see `animusd`'s `trigger_split`/`auto_split_loop` notes on the GC
    /// safety net that makes tolerating the ambiguity sound: never delete a
    /// tablet whose group is still locally, actually hosted, regardless of
    /// what this field says.
    current_split_bound_state: Arc<Mutex<Option<Vec<u8>>>>,
    /// Set by [`shutdown`](Self::shutdown); both the consensus loop and the apply
    /// task observe it and exit.
    halted: Arc<AtomicBool>,
    /// Set by the **consensus loop** just before it returns.
    stopped: Arc<AtomicBool>,
    /// Set by the **apply task** just before it returns. The group's durable
    /// artifacts are quiescent only once *both* tasks have stopped
    /// ([`is_stopped`](Self::is_stopped)) — the teardown path (drop-table GC) waits
    /// on that before deleting the engine/WAL.
    apply_stopped: Arc<AtomicBool>,
    /// **Wake-on-propose** signal: a proposer raises it to make the consensus loop
    /// replicate a freshly appended entry immediately, cutting single-write latency
    /// (ADR 0017) — no waiting on the next heartbeat tick.
    propose_signal: Arc<ProposeSignal>,
    /// Observability sink (ADR 0015). The public propose API records the real
    /// accept/reject outcome into it, and the consensus loop + apply task each hold
    /// a clone for the commit/apply/read-barrier/snapshot recording sites. Cheap to
    /// clone; defaults to `env.metrics()` (a no-op under `SimEnv`, a real sink under
    /// `ProdEnv`) — see [`start_with_metrics`](Self::start_with_metrics) to observe
    /// it under simulation.
    metrics: MetricsHandle,
    /// This group's confinement within `storage` — see [`StorageScope`]'s doc.
    /// `StorageScope::whole()` (the default for every existing constructor)
    /// makes every physical-key operation an identity transform.
    scope: StorageScope,
    /// This group's network multiplexing key (ADR 0026 Stage B): every send/recv
    /// goes out on `(peer, stream)`/`(self, stream)` instead of a peer's default
    /// inbox. `PRIMARY_STREAM` (the default for every existing constructor) is
    /// byte-for-byte today's behavior (`Env::send`/`recv` already forward to
    /// `send_stream`/`recv_stream` with `PRIMARY_STREAM`). The seam a later PR
    /// uses so several tablet groups can share one node's inbox (stream =
    /// tablet id) instead of each minting a distinct `Coresident` sibling id.
    stream: u64,
}

/// Invoked on a replica when it **applies** a committed `Split` (ADR 0017 D), with
/// the split key and the handed-off `[at, ∞)` `(key, value)` pairs captured from
/// this replica's committed engine state (consistent across replicas — they apply
/// at the same point in the command order). The **in-band** hook
/// ([`RaftKvNode::in_band_split_hook`]) mints a co-resident sibling and seeds the
/// new tablet's group from those pairs; the default ([`RaftKvNode::start`]) is
/// `None`, keeping the external-handoff behavior (`split.rs`) unchanged.
pub type SplitHook = Arc<dyn Fn(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>) + Send + Sync>;

impl<E: Env, S: StorageEngine + 'static> RaftKvNode<E, S> {
    /// Start a tablet group node over `env`, backed by `storage`. `all_nodes` is
    /// the group's full replica set (including this node). Spawns the driver loop.
    ///
    /// Metrics (ADR 0015) are recorded into the env's own sink (`env.metrics()`) —
    /// for `ProdEnv` a real recording handle, so an assembled production node
    /// accumulates CP-plane counters with no extra wiring. To observe the counters
    /// under deterministic simulation (where `SimEnv::metrics()` is the no-op
    /// default), construct with [`start_with_metrics`](Self::start_with_metrics)
    /// and pass a recording [`MetricsHandle`] the test keeps.
    pub fn start(env: E, all_nodes: Vec<NodeId>, storage: S) -> Self {
        let metrics = env.metrics();
        Self::start_inner(
            env,
            all_nodes,
            storage,
            None,
            metrics,
            StorageScope::whole(),
            PRIMARY_STREAM,
        )
    }

    /// Like [`start`](Self::start), but the group is confined to `scope` within
    /// `storage` (see [`StorageScope`]'s doc) — the seam a future caller uses to
    /// share one physical engine across several tablets. `storage.clone()` must
    /// be the SAME shared engine handle every co-resident group on this node
    /// was started with; `scope` is what keeps them from colliding.
    pub fn start_scoped(env: E, all_nodes: Vec<NodeId>, storage: S, scope: StorageScope) -> Self {
        let metrics = env.metrics();
        Self::start_inner(
            env,
            all_nodes,
            storage,
            None,
            metrics,
            scope,
            PRIMARY_STREAM,
        )
    }

    /// Like [`start_scoped`](Self::start_scoped), but the group also sends/recvs
    /// on `stream` (ADR 0026 Stage B) instead of `PRIMARY_STREAM` — the seam that
    /// lets several tablet groups share one **node id**'s inbox (multiplexed by
    /// stream, typically the tablet id) instead of each minting a distinct
    /// `Coresident` sibling id. Combined with a shared `storage` + distinct
    /// `scope`s, this is the full "several tablets co-resident on one node"
    /// shape a later PR wires into `animusd`'s real hosting path.
    pub fn start_hosted(
        env: E,
        all_nodes: Vec<NodeId>,
        storage: S,
        scope: StorageScope,
        stream: u64,
    ) -> Self {
        let metrics = env.metrics();
        Self::start_inner(env, all_nodes, storage, None, metrics, scope, stream)
    }

    /// Like [`start`](Self::start), but records into the supplied `metrics` handle
    /// instead of `env.metrics()`. Additive (existing callers use `start`); a sim
    /// test threads a recording handle in here to read counters back without
    /// editing `animus-sim`.
    pub fn start_with_metrics(
        env: E,
        all_nodes: Vec<NodeId>,
        storage: S,
        metrics: MetricsHandle,
    ) -> Self {
        Self::start_inner(
            env,
            all_nodes,
            storage,
            None,
            metrics,
            StorageScope::whole(),
            PRIMARY_STREAM,
        )
    }

    /// Like [`start`](Self::start) but with a [`SplitHook`] invoked on apply of a
    /// committed `Split` — the seam for **in-band** new-group creation (ADR 0017 D):
    /// each original replica spawns its own co-resident new-tablet replica when the
    /// split applies, rather than the control plane / harness creating the new group
    /// from a handoff. Build the hook with [`in_band_split_hook`](Self::in_band_split_hook).
    pub fn start_with_split_hook(
        env: E,
        all_nodes: Vec<NodeId>,
        storage: S,
        on_split: SplitHook,
    ) -> Self {
        let metrics = env.metrics();
        Self::start_inner(
            env,
            all_nodes,
            storage,
            Some(on_split),
            metrics,
            StorageScope::whole(),
            PRIMARY_STREAM,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn start_inner(
        env: E,
        all_nodes: Vec<NodeId>,
        storage: S,
        on_split: Option<SplitHook>,
        metrics: MetricsHandle,
        scope: StorageScope,
        stream: u64,
    ) -> Self {
        let core = Arc::new(Mutex::new(RaftCore::new(
            env.node_id(),
            &all_nodes,
            env.now(),
            env.next_u64(),
        )));
        let reads = Arc::new(Mutex::new(ReadState::default()));
        let cas = Arc::new(Mutex::new(CasResults::default()));
        let halted = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new(AtomicBool::new(false));
        let apply_stopped = Arc::new(AtomicBool::new(false));
        let engine_applied = Arc::new(AtomicU64::new(0));
        // Populated for real by the apply task's recovery read of
        // `SPLIT_BOUND_KEY` once `drive` starts — see its doc.
        let current_split_bound_state: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
        let wal_lock = Arc::new(AsyncMutex::new(()));
        let propose_signal = Arc::new(ProposeSignal::default());
        let node = Self {
            env: env.clone(),
            core: Arc::clone(&core),
            storage: storage.clone(),
            all_nodes: all_nodes.clone(),
            reads: Arc::clone(&reads),
            cas: Arc::clone(&cas),
            engine_applied: Arc::clone(&engine_applied),
            current_split_bound_state: Arc::clone(&current_split_bound_state),
            halted: Arc::clone(&halted),
            stopped: Arc::clone(&stopped),
            apply_stopped: Arc::clone(&apply_stopped),
            propose_signal: Arc::clone(&propose_signal),
            metrics: metrics.clone(),
            scope: scope.clone(),
            stream,
        };
        // The consensus loop recovers from the WAL, then spawns the apply task
        // (so the apply task sees the recovered core + the correct
        // `engine_applied` base before it merges anything), then runs.
        env.spawn_task(drive(DriveState {
            env: env.clone(),
            core,
            all_nodes,
            storage,
            reads,
            cas,
            engine_applied,
            current_split_bound_state,
            wal_lock,
            on_split,
            halted,
            stopped,
            apply_stopped,
            propose_signal,
            metrics,
            scope,
            stream,
        }));
        node
    }

    /// Ask the driver loop to exit: the node stops participating in its group
    /// (no more persists, applies, ticks, or outbound traffic) as of its next
    /// wake — a message arrival or the pending timer deadline, so within one
    /// election-timeout tick. The teardown seam a tablet GC needs (a dropped
    /// table's group must quiesce before its on-disk artifacts are deleted);
    /// poll [`is_stopped`](Self::is_stopped) for the actual exit. Idempotent.
    /// A halted node must not be used again — restarting the tablet means a
    /// fresh `start`.
    pub fn shutdown(&self) {
        self.halted.store(true, Ordering::SeqCst);
    }

    /// Whether [`shutdown`](Self::shutdown) has been requested.
    #[must_use]
    pub fn is_halted(&self) -> bool {
        self.halted.load(Ordering::SeqCst)
    }

    /// Whether **both** driver tasks (the consensus loop and the apply task) have
    /// actually exited after a [`shutdown`](Self::shutdown) — once true, no further
    /// WAL append/fsync or engine apply is in flight, so the group's durable
    /// artifacts are safe to delete.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst) && self.apply_stopped.load(Ordering::SeqCst)
    }

    fn majority(&self) -> usize {
        self.all_nodes.len() / 2 + 1
    }

    /// Propose `command` through the core and, if it was appended (leader), **wake
    /// the consensus loop** so it replicates the new entry at once rather than
    /// waiting for the next heartbeat tick (wake-on-propose, ADR 0017). A
    /// `NotLeader` result appends nothing, so there is nothing to replicate — no
    /// wake. The core lock is dropped before the notify.
    fn propose_and_wake(&self, command: KvCommand) -> ProposeResult {
        let result = record_propose(&self.metrics, self.lock().propose(command));
        if matches!(result, ProposeResult::Accepted { .. }) {
            self.propose_signal.notify();
        }
        result
    }

    /// Propose a write to this group. Honored only on the leader (otherwise
    /// returns the leader hint); the value is durable + applied once committed.
    /// Stamps `fence = KeyRange::whole()` (unconstrained — see
    /// [`KvCommand`]'s doc); use [`put_fenced`](Self::put_fenced) to stamp a
    /// narrower one.
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> ProposeResult {
        self.put_fenced(key, value, KeyRange::whole())
    }

    /// As [`put`](Self::put), but the leader stamps its own `fence` into the
    /// entry instead of the unconstrained default (see [`KvCommand`]'s doc).
    pub fn put_fenced(&self, key: Vec<u8>, value: Vec<u8>, fence: KeyRange) -> ProposeResult {
        self.propose_and_wake(KvCommand::Put { key, value, fence })
    }

    /// Propose a **batch put**: commit every `(key, value)` as **one** Raft log
    /// entry (one propose → one commit round → one apply), for a bulk-write
    /// throughput win over N individual [`put`](Self::put)s. Honored only on the
    /// leader (else a leader hint). All keys share the entry's Raft index as their
    /// MVCC version — the keys are distinct so per-key LWW is well-defined, and the
    /// batch is atomic within this tablet (it commits whole or not at all). To learn
    /// it committed + applied, take the [`ProposeResult::Accepted`] `index` and wait
    /// until `last_applied >= index` (the whole batch has merged by then). Stamps
    /// `fence = KeyRange::whole()`; use [`put_batch_fenced`](Self::put_batch_fenced)
    /// to stamp a narrower one.
    pub fn put_batch(&self, puts: Vec<(Vec<u8>, Vec<u8>)>) -> ProposeResult {
        self.put_batch_fenced(puts, KeyRange::whole())
    }

    /// As [`put_batch`](Self::put_batch), but the leader stamps its own `fence`
    /// into the entry (see [`KvCommand`]'s doc). If any key in `puts` falls
    /// outside `fence`, **none** of the batch applies (the fence gates the
    /// whole atomic entry, not individual keys).
    pub fn put_batch_fenced(
        &self,
        puts: Vec<(Vec<u8>, Vec<u8>)>,
        fence: KeyRange,
    ) -> ProposeResult {
        record_propose(
            &self.metrics,
            self.lock().propose(KvCommand::Batch { puts, fence }),
        )
    }

    /// Propose a delete (tombstone) to this group. Stamps `fence =
    /// KeyRange::whole()`; use [`delete_fenced`](Self::delete_fenced) to stamp
    /// a narrower one.
    pub fn delete(&self, key: Vec<u8>) -> ProposeResult {
        self.delete_fenced(key, KeyRange::whole())
    }

    /// As [`delete`](Self::delete), but the leader stamps its own `fence` into
    /// the entry instead of the unconstrained default (see [`KvCommand`]'s doc).
    pub fn delete_fenced(&self, key: Vec<u8>, fence: KeyRange) -> ProposeResult {
        self.propose_and_wake(KvCommand::Delete { key, fence })
    }

    /// Propose a **linearizable compare-and-swap**: set `key` to `value` iff the
    /// key's current committed value equals `expected` (`None` == "only if the
    /// key is absent"). Leader-only (else a leader hint). The accept/reject
    /// decision is made deterministically at *apply* time in commit order, so two
    /// CAS racing from the same `expected` have exactly one winner. To learn the
    /// outcome, take the [`ProposeResult::Accepted`] `index` and read
    /// [`cas_result`](Self::cas_result) once that index applies — or use the
    /// all-in-one [`compare_and_swap`](Self::compare_and_swap). Stamps `fence =
    /// KeyRange::whole()`; use [`cas_fenced`](Self::cas_fenced) to stamp a
    /// narrower one.
    pub fn cas(&self, key: Vec<u8>, expected: Option<Vec<u8>>, value: Vec<u8>) -> ProposeResult {
        self.cas_fenced(key, expected, value, KeyRange::whole())
    }

    /// As [`cas`](Self::cas), but the leader stamps its own `fence` into the
    /// entry instead of the unconstrained default (see [`KvCommand`]'s doc). A
    /// fenced-out CAS records outcome `false` (see the apply-time fence
    /// check's doc for why).
    pub fn cas_fenced(
        &self,
        key: Vec<u8>,
        expected: Option<Vec<u8>>,
        value: Vec<u8>,
        fence: KeyRange,
    ) -> ProposeResult {
        self.propose_and_wake(KvCommand::Cas {
            key,
            expected,
            value,
            fence,
        })
    }

    /// The recorded outcome of the CAS committed at Raft log `index` (the value
    /// [`cas`](Self::cas) returned in [`ProposeResult::Accepted`]): `Some(true)`
    /// if the swap happened, `Some(false)` if `expected` did not match, or `None`
    /// if that index has not applied on this replica yet. Every replica records
    /// the identical outcome (the decision is deterministic in commit order).
    pub fn cas_result(&self, index: u64) -> Option<bool> {
        self.cas
            .lock()
            .expect("cas results poisoned")
            .outcomes
            .get(&index)
            .copied()
    }

    /// Propose a CAS on the leader and **wait for its committed outcome**: returns
    /// `Some(true)` if the swap happened, `Some(false)` if `expected` did not
    /// match the committed value, or `None` if this node is not the leader or the
    /// outcome does not become available within [`CAS_TIMEOUT`]. Correct under
    /// contention: of two CAS racing from the same `expected`, exactly one returns
    /// `true`. Uses only the `Env` clock/sleep (no wall clock), so it stays a pure
    /// function of the seed under `SimEnv`.
    pub async fn compare_and_swap(
        &self,
        key: Vec<u8>,
        expected: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> Option<bool> {
        let index = match self.cas(key, expected, value) {
            ProposeResult::Accepted { index } => index,
            ProposeResult::NotLeader { .. } => return None,
        };
        let deadline = self.env.now().0 + CAS_TIMEOUT.as_nanos() as u64;
        loop {
            if let Some(outcome) = self.cas_result(index) {
                return Some(outcome);
            }
            // A step-down before this entry applies means it may never apply on
            // this node — give up rather than wait out the full timeout uselessly,
            // but still bound by the deadline for the in-flight case.
            if self.env.now().0 >= deadline {
                return None;
            }
            self.env.sleep(CAS_POLL).await;
        }
    }

    /// Propose a **single-server** membership change (ADR 0017 C): `voters` becomes
    /// this group's Raft configuration. Leader-only; rejected for a multi-server
    /// delta, an in-flight change, or removing the leader. This is the primitive
    /// the control plane drives to move a replica off a failed node onto a spare,
    /// or to grow the group as the cluster grows.
    pub fn change_membership(&self, voters: BTreeSet<NodeId>) -> ProposeResult {
        let result = record_reconfigure(&self.metrics, self.lock().change_membership(voters));
        if matches!(result, ProposeResult::Accepted { .. }) {
            self.propose_signal.notify();
        }
        result
    }

    /// The group's active Raft voter configuration.
    pub fn config(&self) -> BTreeSet<NodeId> {
        self.lock().config()
    }

    /// Take **one** single-server step moving this group's Raft configuration
    /// toward `desired` — the control plane's placement decision for this tablet
    /// (ADR 0017 Stage C: the automatic reconfigure trigger). The shared
    /// [`RaftCore::change_membership`] only accepts a single-server delta, with no
    /// in-flight change and no leader self-removal, so this picks one add/remove
    /// that makes progress and lets the next tick take the following step — a
    /// multi-server move (e.g. replace a dead replica with a spare) converges one
    /// server per call. Returns the proposed config if a step was **accepted**,
    /// else `None`: already converged, not the leader, a change is in flight, or
    /// the only remaining delta is removing the leader itself (which needs a
    /// leadership transfer first — out of scope here).
    ///
    /// Order: drop an extra **non-leader** voter (e.g. a `Down` node) before adding
    /// a missing one, so quorum margin is restored before a fresh replica — which
    /// must still catch up via log/`InstallSnapshot` — is brought in.
    pub fn reconfigure_step(&self, desired: &BTreeSet<NodeId>) -> Option<BTreeSet<NodeId>> {
        let current = self.config();
        if current == *desired || !self.is_leader() {
            return None;
        }
        let me = self.env.node_id();
        let next = if let Some(&extra) = current.difference(desired).find(|&&n| n != me) {
            let mut c = current.clone();
            c.remove(&extra);
            c
        } else if let Some(&missing) = desired.difference(&current).next() {
            let mut c = current.clone();
            c.insert(missing);
            c
        } else {
            // The only delta left is removing the leader itself.
            return None;
        };
        match self.change_membership(next.clone()) {
            ProposeResult::Accepted { .. } => Some(next),
            ProposeResult::NotLeader { .. } => None,
        }
    }

    /// Spawn the **automatic Stage-C reconfigure loop** (ADR 0017): on each
    /// `interval` tick, poll `desired` for this tablet's target voter set and take
    /// one [`reconfigure_step`](Self::reconfigure_step) toward it. Idempotent and
    /// leader-gated — a non-leader or a converged group proposes nothing, so a
    /// steady cluster produces no churn; a multi-server move converges one server
    /// per tick. `desired` is the **seam to the control plane**: in production it
    /// reads `Metadata.tablets[tablet].replicas` (the placement reconciler's
    /// epoch-CAS decision) and returns it as a voter set; it is a closure so this
    /// crate takes no dependency on the control-plane driver type. Mirrors the
    /// control plane's `reconcile_loop` (decision elsewhere, timing here).
    pub fn spawn_reconfigure_loop<F>(&self, interval: Duration, desired: F)
    where
        F: Fn() -> Option<BTreeSet<NodeId>> + Send + 'static,
    {
        let node = self.clone();
        let env = self.env.clone();
        env.clone().spawn_task(async move {
            loop {
                env.sleep(interval).await;
                if node.is_halted() {
                    return;
                }
                if let Some(target) = desired() {
                    node.reconfigure_step(&target);
                }
            }
        });
    }

    /// Propose a **tablet split** at key `at` (ADR 0017 D): keys `>= at` move to a
    /// new tablet. Leader-only. Once committed, every replica tombstones `[at, ∞)`
    /// from its engine (so this group serves only `[lo, at)`); seed the new group
    /// with [`range_snapshot`](Self::range_snapshot) data (captured before the
    /// split) and start it via [`start_seeded`](Self::start_seeded).
    pub fn propose_split(&self, at: Vec<u8>) -> ProposeResult {
        self.propose_and_wake(KvCommand::Split { at })
    }

    /// The live `(key, value)` pairs with `key >= at` in this replica's engine —
    /// the data to seed the new tablet's group on a split. Read on the leader
    /// (its committed state is authoritative) before proposing the split.
    pub async fn range_snapshot(&self, at: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        keys_from(&self.storage, &self.scope, at).await
    }

    /// Update this group's live [`StorageScope`] range (see its doc) —
    /// typically called by a caller watching the control plane's replicated
    /// `Metadata` for this tablet's current range, whenever it narrows (e.g.
    /// this tablet was the source of a single-command split). A no-op for the
    /// default [`StorageScope::whole()`] scope, since nothing needs bounding
    /// there.
    pub fn narrow_scope(&self, new_range: KeyRange) {
        self.scope.narrow(new_range);
    }

    /// Start a group whose engine is **pre-seeded** with `seed` `(key, value)`
    /// pairs (a new tablet bootstrapped from a split's handed-off range). The seed
    /// is written at version 0 — below any Raft-applied version (the Raft index) —
    /// so later writes win by per-key LWW.
    pub async fn start_seeded(
        env: E,
        all_nodes: Vec<NodeId>,
        storage: S,
        seed: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Self {
        Self::start_seeded_scoped(env, all_nodes, storage, seed, StorageScope::whole()).await
    }

    /// Like [`start_seeded`](Self::start_seeded), but the new group is confined
    /// to `scope` within `storage` (see [`StorageScope`]'s doc) — the seed is
    /// written at each key's *physical* address under `scope`.
    pub async fn start_seeded_scoped(
        env: E,
        all_nodes: Vec<NodeId>,
        storage: S,
        seed: Vec<(Vec<u8>, Vec<u8>)>,
        scope: StorageScope,
    ) -> Self {
        for (key, value) in &seed {
            storage
                .merge(&scope.physical(key), value, 0)
                .await
                .expect("raftkv seed");
        }
        Self::start_scoped(env, all_nodes, storage, scope)
    }

    /// Like [`start_seeded`](Self::start_seeded) but the new group carries a
    /// [`SplitHook`] of its own, so a tablet created by a split can itself be split
    /// again (deep splits / continued auto-sharding). Seeds, then starts with the
    /// hook.
    pub async fn start_seeded_with_split_hook(
        env: E,
        all_nodes: Vec<NodeId>,
        storage: S,
        seed: Vec<(Vec<u8>, Vec<u8>)>,
        on_split: SplitHook,
    ) -> Self {
        for (key, value) in &seed {
            storage.merge(key, value, 0).await.expect("raftkv seed");
        }
        Self::start_with_split_hook(env, all_nodes, storage, on_split)
    }

    /// Read `key` from this replica's **local engine**. NOTE: this is a local read
    /// — it is *not* yet linearizable (that is ReadIndex, Stage B.2). It is used by
    /// tests to observe a replica's applied state and to confirm convergence.
    pub async fn local_get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.storage
            .get(&self.scope.physical(key))
            .await
            .ok()
            .flatten()
            .map(|vv| vv.value)
    }

    /// A **linearizable** read of `key` via **ReadIndex** (ADR 0017): only the
    /// leader can serve it. Records `read_index = commit_index`, confirms it is
    /// still leader by a quorum of peers acking its current term (a read-barrier
    /// probe — no log entry, no wall clock), waits until its applied state reaches
    /// `read_index`, then reads the local engine. Returns `None` if this node is
    /// not (or stops being) the leader, or if confirmation times out — never a
    /// stale value: a deposed leader cannot collect a quorum ack (a newer leader
    /// requires a quorum at a higher term, which would reject the probe).
    pub async fn linearizable_get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if self.read_barrier().await {
            self.local_get(key).await
        } else {
            None
        }
    }

    /// A **linearizable range scan** via ReadIndex (ADR 0017 / v1): the live
    /// `(key, value)` pairs with `start <= key < end`, sorted by key, up to
    /// `limit`. `end == None` is **unbounded above** — scan to the end of the
    /// keyspace (ADR 0023: a per-table tablet's engine holds the whole table, so a
    /// full-table scan has no finite upper bound). Same barrier as
    /// [`linearizable_get`](Self::linearizable_get) — only the confirmed leader
    /// serves it, so a deposed leader returns `None` rather than a stale/partial
    /// range. This is the CP read primitive the DynamoDB `Query`/`Scan` and CQL
    /// `SELECT` edges use in place of the AP quorum scan.
    pub async fn linearizable_scan(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
        if !self.read_barrier().await {
            return None;
        }
        // Push the range down into the engine (audit P4): a bounded scan reads
        // only `[start, end)` instead of materializing the whole tablet and
        // filtering; both `scan` and `entries` return key-ordered results by the
        // `StorageEngine` contract, so the old re-sort was redundant — drop it
        // and apply the limit to the already-ordered rows. An unbounded-above
        // scan (`end == None`, e.g. a full-table `Scan`) still goes through
        // `entries()` — the trait's `scan` takes a finite exclusive bound and
        // arbitrary byte keys have no max sentinel; pushing a limit *into* the
        // engine is a `StorageEngine` API change, out of scope here.
        // Both branches physically bound/filter to `self.scope` (PR3: on a
        // possibly-shared engine, `entries()` in particular would otherwise
        // return every other tenant's keys too — see `StorageScope`'s doc) and
        // exclude `SPLIT_BOUND_KEY` — its doc explains why this reserved entry
        // must never reach a client, and its position in the keyspace isn't
        // guaranteed to sort outside any particular range (it's not derived
        // from any real key's hash/format). Under the default (whole) scope
        // this is byte-for-byte the prior behavior: `physical` is the
        // identity and `strip_in_range` always succeeds.
        let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = match end {
            Some(e) => self
                .storage
                .scan(&self.scope.physical(start), &self.scope.physical(e))
                .await
                .ok()?
                .into_iter()
                .filter_map(|(k, vv)| {
                    let logical = self.scope.strip_in_range(&k)?;
                    (logical != SPLIT_BOUND_KEY).then(|| (logical.to_vec(), vv.value))
                })
                .collect(),
            None => self
                .storage
                .entries()
                .await
                .ok()?
                .into_iter()
                .filter_map(|(k, vv)| {
                    let logical = self.scope.strip_in_range(&k)?;
                    (logical >= start && logical != SPLIT_BOUND_KEY)
                        .then(|| (logical.to_vec(), vv.value))
                })
                .collect(),
        };
        if let Some(n) = limit {
            pairs.truncate(n);
        }
        Some(pairs)
    }

    /// The **ReadIndex read barrier** (ADR 0017 B.2): wait until this leader has
    /// committed an entry of its **own term** (Raft §6.4 — see below), record
    /// `read_index = commit_index` for the current term, confirm via a quorum of
    /// read-probe acks that we are still the leader for that term (no log entry,
    /// no wall clock), and wait until applied state reaches `read_index`. Returns
    /// `true` when it is safe to serve a local read linearizably; `false` if this
    /// node is not (or stops being) the leader, or confirmation times out — so a
    /// deposed leader never serves a stale read (a newer leader needs a quorum at
    /// a higher term, which rejects the probe). Shared by `linearizable_get` +
    /// `linearizable_scan`.
    ///
    /// **The current-term-commit gate is mandatory for linearizability** (the
    /// dissertation's second ReadIndex requirement): a freshly elected leader's
    /// log contains every committed entry (leader completeness), but its
    /// `commit_index` may still lag an entry the *previous* leader committed and
    /// acked — the commit rule never counts old-term entries toward a majority.
    /// Capturing `read_index = commit_index` in that window (with only the term
    /// probe, which involves no log state) would serve a read *below* an acked
    /// write: a stale read. So the barrier first waits (bounded by the same
    /// [`READ_TIMEOUT`]) for `commit_index` to reach the leader's first
    /// current-term entry (its election no-op, [`RaftCore::first_term_index`]);
    /// committing that no-op also commits every prior-term entry beneath it. An
    /// established leader passes the gate immediately (no extra latency).
    async fn read_barrier(&self) -> bool {
        let deadline = self.env.now().0 + READ_TIMEOUT.as_nanos() as u64;
        let (term, read_index) = loop {
            let captured = {
                let c = self.lock();
                if !c.is_leader() {
                    return false;
                }
                let first = c
                    .first_term_index()
                    .expect("a leader has a first-term index");
                (c.commit_index() >= first).then(|| (c.term(), c.commit_index()))
            };
            if let Some(state) = captured {
                break state;
            }
            // Fresh leader whose no-op has not committed yet: wait for it (it
            // commits within one replication round on a healthy quorum) rather
            // than risk a read below a previously acked write.
            if self.env.now().0 >= deadline {
                self.metrics.incr(Metric::CpReadBarriersTimedOut);
                return false;
            }
            self.env.sleep(READ_POLL).await;
        };
        // Register the read barrier (self trivially confirms its own term).
        let epoch = {
            let mut r = self.reads.lock().expect("read state poisoned");
            r.next_epoch += 1;
            let epoch = r.next_epoch;
            let mut acks = BTreeSet::new();
            acks.insert(self.env.node_id());
            r.pending.insert(epoch, (term, acks));
            epoch
        };
        // Probe peers immediately (periodic heartbeats would also carry it, but an
        // explicit probe confirms promptly).
        let probe = codec::encode_wire(&KvWire::ReadProbe { term, epoch });
        for &p in &self.all_nodes {
            if p != self.env.node_id() {
                self.env.send_stream(p, self.stream, probe.clone()).await;
            }
        }

        // The confirmation wait shares the barrier's one deadline (set above), so
        // the whole barrier — gate + probe + applied wait — is bounded by a single
        // READ_TIMEOUT.
        let majority = self.majority();
        let ok = loop {
            // Still the leader for this term? A step-down/term change invalidates
            // the barrier — fail rather than risk a stale read.
            let still_leader = {
                let c = self.lock();
                c.is_leader() && c.term() == term
            };
            let confirmed = {
                let r = self.reads.lock().expect("read state poisoned");
                r.pending
                    .get(&epoch)
                    .is_some_and(|(_, acks)| acks.len() >= majority)
            };
            // Gate on **engine-applied** progress, not the core's `last_applied`:
            // the async apply task merges into the engine behind the core's apply
            // cursor, and this read serves from the engine — so waiting on
            // `last_applied` could read a key the engine has not yet merged.
            let applied = self.engine_applied.load(Ordering::SeqCst) >= read_index;
            if !still_leader || self.env.now().0 >= deadline {
                break false;
            }
            if confirmed && applied {
                break true;
            }
            self.env.sleep(READ_POLL).await;
        };
        self.reads
            .lock()
            .expect("read state poisoned")
            .pending
            .remove(&epoch);
        // Record the real outcome of this barrier attempt (not the immediate
        // not-leader short-circuit above, which never registered one, nor the gate
        // timeout above, already recorded there): served iff it confirmed
        // leadership by quorum before its deadline, else it either stepped
        // down/changed term or genuinely timed out.
        if ok {
            self.metrics.incr(Metric::CpReadBarriersServed);
        } else {
            self.metrics.incr(Metric::CpReadBarriersTimedOut);
        }
        ok
    }

    /// Whether this node currently believes it is the group's leader.
    pub fn is_leader(&self) -> bool {
        self.lock().is_leader()
    }

    /// The group's current leader id as this node sees it (its Raft `leader_id`),
    /// or `None` if unknown (e.g. mid-election). The id is a group member id; a
    /// caller maps it to the hosting node for cross-process routing (ADR 0017 #3b).
    pub fn leader(&self) -> Option<NodeId> {
        self.lock().leader()
    }

    /// This node's `Env` handle.
    pub fn env(&self) -> &E {
        &self.env
    }

    /// This node's backing storage engine — for admin/debug introspection of the
    /// CP group's on-disk state (ADR 0020). Read-only access to the concrete
    /// engine (e.g. `LsmEngine`) so the assembly layer can surface SSTable/WAL
    /// debug views without the engine state leaking into the consensus core.
    pub fn storage(&self) -> &S {
        &self.storage
    }

    // ---- admin / debug introspection (ADR 0020) -------------------------
    // Read-only projections of this group's Raft state, mirroring the control
    // plane's `RaftNode` accessors. Each takes the core lock briefly and returns
    // a copy; no mutation.

    /// This node's current Raft role in the group.
    pub fn role(&self) -> animus_control::Role {
        self.lock().role()
    }

    /// The group's current Raft term.
    pub fn term(&self) -> u64 {
        self.lock().term()
    }

    /// Highest committed log index.
    pub fn commit_index(&self) -> u64 {
        self.lock().commit_index()
    }

    /// Highest applied log index (the MVCC version high-water mark).
    pub fn last_applied(&self) -> u64 {
        self.lock().last_applied()
    }

    /// Highest Raft log index whose effects the **engine** has merged — the same
    /// watermark linearizable reads gate on (the async apply task advances it
    /// after each merge; the core's `last_applied` is only the buffer cursor and
    /// *leads* the engine). This is the **confirm-by-index** primitive (audit
    /// A4): a proposer holding a [`ProposeResult::Accepted`] `index` confirms its
    /// write is committed *and applied* by checking `engine_applied_index() >=
    /// index` while this node is still the leader **in the proposal's term** —
    /// exact under concurrency, unlike polling for value equality, which
    /// false-negatives when a concurrent later write to the same key overwrites
    /// the proposed value before the poll observes it.
    pub fn engine_applied_index(&self) -> u64 {
        self.engine_applied.load(Ordering::SeqCst)
    }

    /// This group's current split boundary: `Some(k)` means every split so far
    /// has narrowed its valid range to `[lo, k)`; `None` if it has never split.
    /// Serves two purposes: (1) the "is `at` still a legal new split point"
    /// question for [`propose_split`](Self::propose_split) (`at` must be
    /// strictly less than this, or this must be `None`), and (2) the
    /// **confirm-by-key** primitive its callers poll after an `Accepted`
    /// propose — `Accepted` only means the entry reached the leader's local
    /// log, not that it committed (an accepted-but-uncommitted `Split` is
    /// silently truncated if leadership moves before it commits, exactly like
    /// a `put`/`delete` — see `engine_applied_index`'s doc), and under
    /// `--cluster N`'s shared edge state more than one node's auto-split loop
    /// can independently compute a *different* median for the same tablet in
    /// the same tick, propose a `Split` at their own key, and only one of
    /// those keys ever actually applies.
    ///
    /// **A moving target once more than one split has happened**: comparing
    /// this against a specific proposed key `K` can go from `Some(K)` to
    /// `Some(k')` (`k' < K`) if a *later* split narrows past `K` — even though
    /// `K` genuinely did apply earlier. This method deliberately does *not*
    /// disambiguate that (it holds only the current value, not a full
    /// per-split history — see this field's doc for why: bounded O(1) state
    /// forever, not O(splits this lineage has ever done)). A caller that acts
    /// on "this no longer matches my key" by *deleting* something must not
    /// trust that alone — see `animusd`'s `trigger_split`/`auto_split_loop`
    /// notes on the local-hosting check that makes tolerating this ambiguity
    /// safe (worst case: skip a cleanup that was actually safe to do, never
    /// delete something still genuinely alive).
    pub fn current_split_bound(&self) -> Option<Vec<u8>> {
        self.current_split_bound_state
            .lock()
            .expect("raftkv current_split_bound_state poisoned")
            .clone()
    }

    /// Highest log index known durable on disk (durable-before-visible frontier).
    pub fn durable_index(&self) -> u64 {
        self.lock().durable_index()
    }

    /// The current snapshot base index (0 if none taken).
    pub fn snapshot_index(&self) -> u64 {
        self.lock().snapshot_index()
    }

    /// Number of log entries currently retained (the tail after the snapshot).
    pub fn log_len(&self) -> usize {
        self.lock().log_len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, KvCore> {
        self.core.lock().expect("raftkv core poisoned")
    }
}

impl<E: Coresident, S: StorageEngine + 'static> RaftKvNode<E, S> {
    /// Build a [`SplitHook`] that creates this replica's new-tablet group **in
    /// band** (ADR 0017 D): when the committed `Split` applies, this node mints a
    /// **co-resident sibling** env (`Coresident::sibling(my_new_id)` — its own
    /// inbox on the same physical node, the single-consumer rule) and starts the
    /// new group there, seeded with the handed-off `[at, ∞)` range. The new replica
    /// joins `new_all_nodes` (the new tablet's full id set, allocated by the
    /// control plane and identical across the original replicas, so the new group
    /// is coherent). `mk_engine` supplies the new group's fresh engine; each
    /// created node is pushed into `created` so the caller can observe/drive it.
    ///
    /// Wire one hook per original replica (each with *its own* env + `my_new_id`)
    /// via [`start_with_split_hook`](Self::start_with_split_hook); on apply every
    /// replica spawns its co-resident new-group member and the new group forms with
    /// no external handoff.
    ///
    /// The hook fires once **per distinct split point** this group ever applies
    /// (a group can now split more than once — see `current_split_bound_state`'s doc): a
    /// `Split { at }` whose `at` is no longer strictly less than the group's
    /// current boundary is rejected as a no-op at apply time (it never
    /// recomputes the handoff, never re-invokes the hook for that entry), so a
    /// WAL replay after a crash recovery — or a genuinely duplicate `Split`
    /// commit for an `at` that already applied, e.g. from a caller that
    /// proposes the same split more than once — cannot mint the sibling twice or hand it an
    /// already-tombstoned (empty) range. See
    /// `split_in_band.rs::duplicate_split_proposal_does_not_lose_data` for the
    /// regression: without that guard, a second application's *empty* handoff can
    /// race the first's *real* one for a mint, silently losing the split's data.
    pub fn in_band_split_hook<MkEngine>(
        env: E,
        my_new_id: NodeId,
        new_all_nodes: Vec<NodeId>,
        mk_engine: MkEngine,
        created: Arc<Mutex<Vec<RaftKvNode<E, S>>>>,
    ) -> SplitHook
    where
        MkEngine: Fn() -> S + Send + Sync + 'static,
    {
        Arc::new(move |_at, handoff| {
            let sibling = env.sibling(my_new_id);
            let all = new_all_nodes.clone();
            let storage = mk_engine();
            let created = Arc::clone(&created);
            // Seed + start on the sibling env in a spawned task (seeding awaits
            // engine writes); stash the handle so the caller can observe the new
            // group. The spawned driver owns the group, so dropping the local
            // binding would not stop it — but we keep it for observability.
            let spawner = sibling.clone();
            spawner.spawn_task(async move {
                let node = RaftKvNode::start_seeded(sibling, all, storage, handoff).await;
                created.lock().expect("split sink poisoned").push(node);
            });
        })
    }
}

/// Record the real outcome of a data propose (`put`/`put_batch`/`delete`/`cas`/
/// `propose_split`) — accepted, or rejected because this node is not the leader —
/// and pass the result through unchanged. ADR 0015: count the outcome, never the
/// attempt.
fn record_propose(metrics: &MetricsHandle, result: ProposeResult) -> ProposeResult {
    match result {
        ProposeResult::Accepted { .. } => metrics.incr(Metric::CpProposalsAccepted),
        ProposeResult::NotLeader { .. } => metrics.incr(Metric::CpProposalsRejectedNotLeader),
    }
    result
}

/// Like [`record_propose`] but for a `change_membership` step (direct call or the
/// automatic [`RaftKvNode::reconfigure_step`]) — kept as its own counter family so
/// reconfiguration churn is distinguishable from data-write contention.
fn record_reconfigure(metrics: &MetricsHandle, result: ProposeResult) -> ProposeResult {
    match result {
        ProposeResult::Accepted { .. } => metrics.incr(Metric::CpReconfigureAccepted),
        ProposeResult::NotLeader { .. } => metrics.incr(Metric::CpReconfigureRejected),
    }
    result
}

/// Record the snapshot-shipping metrics implied by the messages the consensus loop
/// just emitted (ADR 0015), mirroring the control plane's `record_outbound`: every
/// outbound `InstallSnapshot` is one chunk actually *shipped*; an outbound
/// `InstallSnapshotResp` whose `last_index > 0` marks a completed *install* on the
/// follower that just finished (observed here since the follower is what emits the
/// ack). A pure read of `outs`.
fn record_kv_outbound(metrics: &MetricsHandle, outs: &[(NodeId, KvWire)]) {
    for (_, wire) in outs {
        if let KvWire::Raft(msg) = wire {
            match msg {
                RaftMsg::InstallSnapshot { .. } => metrics.incr(Metric::CpSnapshotShips),
                RaftMsg::InstallSnapshotResp { last_index, .. } if *last_index > 0 => {
                    metrics.incr(Metric::CpSnapshotInstalls);
                }
                _ => {}
            }
        }
    }
}

/// Persist the core's pending WAL records (append + `fsync`) and advance the
/// durable watermark so committed entries become applicable. **Runs on the
/// consensus loop only** and is deliberately cheap — engine apply and compaction
/// (the slow work that used to share this pass) now run on the separate apply task,
/// so this loop stays responsive to Raft messages / heartbeats within the election
/// timeout (ADR 0017 — the driver-liveness fix). Holds `wal_lock` so the append
/// cannot interleave with the apply task's compaction rewrite of the same file.
/// Durability precedes visibility: `mark_durable_through` follows the `fsync`.
async fn persist_wal<E: Env>(env: &E, core: &Arc<Mutex<KvCore>>, wal_lock: &AsyncMutex<()>) {
    let _wal = wal_lock.lock().await;
    let (records, through) = {
        let mut c = core.lock().expect("raftkv core poisoned");
        (c.drain_persist(), c.last_log_index())
    };
    if records.is_empty() {
        return;
    }
    for record in &records {
        env.append(WAL, &PersistedState::encode_record(record))
            .await
            .expect("raftkv wal append");
    }
    env.sync(WAL).await.expect("raftkv wal sync");
    core.lock()
        .expect("raftkv core poisoned")
        .mark_durable_through(through);
}

/// Install any received snapshot, apply committed-and-durable commands to the
/// engine in commit order, and compact when the engine has merged enough past the
/// snapshot base. **Runs on the apply task only** — off the consensus loop, so a
/// slow batch of engine merges or a compaction rewrite never stalls heartbeats /
/// append processing (the driver-liveness fix). Returns whether it did any work, so
/// the caller can back off when idle. `engine_applied` publishes engine progress
/// (linearizable reads gate on it), and `wal_lock` guards the compaction rewrite.
#[allow(clippy::too_many_arguments)] // the apply task's shared-state bundle
async fn apply_and_compact<E: Env, S: StorageEngine>(
    env: &E,
    core: &Arc<Mutex<KvCore>>,
    storage: &S,
    cas: &Arc<Mutex<CasResults>>,
    on_split: &Option<SplitHook>,
    engine_applied: &AtomicU64,
    current_split_bound_state: &Mutex<Option<Vec<u8>>>,
    wal_lock: &AsyncMutex<()>,
    halted: &AtomicBool,
    metrics: &MetricsHandle,
    scope: &StorageScope,
) -> bool {
    let mut did_work = false;

    // Install a fully-received snapshot (a follower catching up) into the engine
    // *before* applying log-tail effects, so the tail merges on top of the base.
    let pending_install = core
        .lock()
        .expect("raftkv core poisoned")
        .drain_pending_install();
    if let Some((last_index, bytes)) = pending_install {
        install_engine_image(storage, scope, &bytes).await;
        engine_applied.fetch_max(last_index, Ordering::SeqCst);
        // Re-sync the in-memory split-boundary cache from the just-installed
        // image: the sender's `snapshot_index`/engine state is authoritative
        // for this follower now (`RaftCore::handle_install_snapshot` already
        // cleared its log), so whatever this replica's own cache held before
        // is stale — overwrite, don't merge.
        *current_split_bound_state
            .lock()
            .expect("raftkv current_split_bound_state poisoned") =
            match storage.get(&scope.physical(SPLIT_BOUND_KEY)).await {
                Ok(Some(vv)) => decode_split_bound(&vv.value),
                _ => None,
            };
        did_work = true;
    }

    // Apply the now-durable committed commands to the engine, in commit order.
    // The Raft index is the MVCC version: per-key LWW then reproduces the agreed
    // total order, and re-applying on recovery is idempotent.
    let effects = core.lock().expect("raftkv core poisoned").drain_apply();
    if !effects.is_empty() {
        metrics.incr_by(Metric::CpApplies, effects.len() as u64);
    }
    did_work |= !effects.is_empty();
    // Coalesce the WAL `fsync` for a run of plain Put/Delete commands: the apply
    // loop is a single sequential task, so applying each command with its own
    // `merge`/`merge_tombstone` pays one `fsync` per command (the WAL group commit
    // only coalesces *concurrent* writers — there are none here). Accumulating the
    // run into one `merge_batch` collapses it to a single sync. A command that must
    // *read* committed state (`Cas`, `Split`) first drains the pending run so its
    // read sees those writes; `NoOp` needs no drain but we keep ordering simple by
    // leaving the run intact across it (it mutates nothing).
    let mut pending: Vec<MergeOp> = Vec::new();
    // Highest index processed this pass. `engine_applied` (the watermark
    // linearizable reads gate on) advances to it ONLY after the trailing
    // `flush_pending` below — never per-command: a Put/Delete sits un-flushed in
    // `pending`, so advancing mid-loop would claim the engine holds an index it has
    // not merged yet, letting a read gate open and observe past the engine.
    let mut max_index = 0u64;
    for (index, command) in effects {
        match command {
            KvCommand::Put { key, value, fence } => {
                // Out-of-fence is a deterministic no-op: the fence rides in the
                // entry itself (stamped by the leader at propose time), so every
                // replica reaches this same accept/reject decision regardless of
                // its own progress learning the tablet's range has changed (see
                // `KvCommand`'s doc). The fence check is against the *logical*
                // key; only the storage-bound `MergeOp` gets the physical
                // address (see `StorageScope`'s doc — under the default scope
                // this is an identity transform).
                if fence.contains(&key) {
                    pending.push(MergeOp::put(scope.physical(&key), value, index));
                }
            }
            KvCommand::Batch { puts, fence } => {
                // The fence gates the *whole* batch, not per-key: a batch is one
                // atomic Raft entry (see `KvCommand::Batch`'s doc), so partially
                // applying it on a fence miss would silently break that guarantee.
                // Every key in the batch merges at this one entry's `index` (the
                // shared MVCC version). The keys are distinct, so per-key LWW is
                // well-defined; `engine_applied` advances once past the whole batch
                // at the end of the loop iteration (the batch is one entry). Composes
                // with a future coalesced-fsync merge_batch (perf/lsm) — this is the
                // normal per-key `merge` path that batching optimization refines.
                if puts.iter().all(|(key, _)| fence.contains(key)) {
                    for (key, value) in &puts {
                        storage
                            .merge(&scope.physical(key), value, index)
                            .await
                            .expect("raftkv apply batch put");
                    }
                }
            }
            KvCommand::Delete { key, fence } => {
                if fence.contains(&key) {
                    pending.push(MergeOp::tombstone(scope.physical(&key), index));
                }
            }
            KvCommand::Cas {
                key,
                expected,
                value,
                fence,
            } => {
                // Drain the pending run so the CAS read observes every earlier
                // committed write in this apply pass.
                flush_pending(storage, &mut pending, metrics).await;
                // A fenced-out CAS never reads/writes storage — it is recorded as
                // `false` ("did not swap"), the same outcome shape a proposer
                // already handles for an ordinary `expected` mismatch, so a
                // confirm-poll on this index never hangs waiting for an outcome
                // that will never come.
                let swapped = if fence.contains(&key) {
                    // Read the key's *current committed* value (the latest applied,
                    // since we apply in commit order and earlier entries in this
                    // batch already merged above) and compare to `expected`. Equal
                    // → swap; else no-op. Deterministic on every replica (same
                    // order, same committed state, no clock/RNG), so concurrent CAS
                    // from the same `expected` resolve to exactly one winner —
                    // whichever Raft put first, since the first swap moves the
                    // committed value and the second's compare then fails.
                    let physical_key = scope.physical(&key);
                    let current = storage
                        .get(&physical_key)
                        .await
                        .expect("raftkv cas read")
                        .map(|vv| vv.value);
                    let swapped = current == expected;
                    if swapped {
                        // Same write path as `Put`: index is the MVCC version, so
                        // re-applying on recovery is idempotent (per-key LWW).
                        storage
                            .merge(&physical_key, &value, index)
                            .await
                            .expect("raftkv apply cas");
                    }
                    swapped
                } else {
                    false
                };
                cas.lock()
                    .expect("cas results poisoned")
                    .outcomes
                    .insert(index, swapped);
            }
            KvCommand::Split { at } => {
                // A `Split` is only valid if `at` is strictly less than the group's
                // *current* boundary (`None` / never split ⇒ always valid) — a CAS
                // on the group's own split history, not a one-shot latch. This group
                // can be split more than once over its life (each split narrows
                // `[lo, boundary)` further), but a *duplicate or superseded* `Split`
                // entry is still possible, not just in theory: `animusd`'s auto-split
                // loop reads `Metadata`/calls `local_pairs()`/`propose_split()` per
                // node, and in an in-process `--cluster N` run every node's loop
                // shares one `ClusterEdgeState` — so several nodes can independently
                // observe the same over-threshold leader in the same tick and all
                // call `propose_split`, landing more than one `Split` command in the
                // committed log. Applying a `Split` whose `at` is no longer strictly
                // inside the *current* boundary would recompute the handoff from
                // storage — now **empty** or overlapping data already handed off by
                // an earlier split — and fire the hook again with that bad handoff:
                // an empty-handoff task can win the per-node "mint once" race (keyed
                // on the *tablet id*, not on which `Split` triggered it) and seed the
                // new group with no data, losing real rows for good (the root cause
                // of `tablet_auto_splits_when_it_grows` flaking on "key not served
                // after auto-split"). Rejecting anything not strictly less than the
                // current boundary is what closes that, whether the duplicate is a
                // same-tick race today or a stale retry after a leader failover (WAL
                // replay applies through this identical path against the *replayed*
                // boundary at each point, so it makes the same accept/reject decision
                // it made live).
                let current_bound = current_split_bound_state
                    .lock()
                    .expect("raftkv current_split_bound_state poisoned")
                    .clone();
                let valid = current_bound.as_ref().is_none_or(|bound| &at < bound);
                if valid {
                    // Drain the pending run so the handoff capture below sees every
                    // earlier committed write in this apply pass.
                    flush_pending(storage, &mut pending, metrics).await;
                    // Capture the handed-off range `[at, ∞)` from this replica's
                    // committed state (excluding `SPLIT_BOUND_KEY` — see its doc).
                    // Every replica applies the same `Split` at the same point in the
                    // command order, so the captured handoff is consistent across
                    // replicas (ADR 0017 D). `keys_from` returns *logical* keys
                    // (prefix already stripped — see `StorageScope`'s doc on why
                    // this legacy path is prefix-scoped only, not range-scoped).
                    let handoff = keys_from(storage, scope, &at).await;
                    // In-band new-group creation: hand the range to the split hook,
                    // which (when wired) mints a co-resident sibling and seeds the new
                    // tablet's group from it. With no hook, the new group is created
                    // externally from a leader handoff (the prior behavior). The new
                    // group starts with its *own* `None` split boundary — it never
                    // inherits this group's.
                    if let Some(hook) = on_split {
                        hook(at.clone(), handoff.clone());
                    }
                    // The handed-off range now belongs to the new tablet, so tombstone
                    // it here — consistently on every replica — and persist the new
                    // boundary (**overwriting**, not appending — see `SPLIT_BOUND_KEY`'s
                    // doc for why one value is enough), all under a single sync
                    // (extending the same batch, not a second `merge_batch` call).
                    // Tombstones/boundary key are written at their *physical* address.
                    let new_bound = Some(at.clone());
                    let mut ops: Vec<MergeOp> = handoff
                        .iter()
                        .map(|(key, _)| MergeOp::tombstone(scope.physical(key), index))
                        .collect();
                    ops.push(MergeOp::put(
                        scope.physical(SPLIT_BOUND_KEY),
                        encode_split_bound(&new_bound),
                        index,
                    ));
                    storage
                        .merge_batch(ops)
                        .await
                        .expect("raftkv apply split tombstones + boundary");
                    *current_split_bound_state
                        .lock()
                        .expect("raftkv current_split_bound_state poisoned") = new_bound;
                }
            }
            KvCommand::NoOp => {}
        }
        max_index = index; // ascending; watermark advances after the final flush
    }
    // Apply any trailing Put/Delete run under one final sync. Only now does the
    // engine reflect every index in this pass.
    flush_pending(storage, &mut pending, metrics).await;
    // Publish the watermark: the engine now holds all effects through `max_index`,
    // so linearizable reads may serve up to it and compaction may snapshot up to it.
    if max_index > 0 {
        engine_applied.fetch_max(max_index, Ordering::SeqCst);
    }

    // Compact once the *engine* has merged enough past the snapshot base: truncate
    // the Raft log prefix and rewrite the WAL to its bounded image (ADR 0017 A.2).
    // We snapshot only up to `engine_applied`, not the core's `last_applied`
    // (which the async apply lags) — else the truncated log prefix would run past
    // what the engine state contains. This task is the only engine writer, so
    // nothing merges between reading `ea` and the snapshot below.
    //
    // **The engine image is built lazily, on demand** (audit P1/P5): threshold
    // compaction alone no longer scans + serializes the whole tablet — that cost
    // was paid every `COMPACT_THRESHOLD` applies on every replica, and the image
    // then sat resident in the core forever, whether or not any follower ever
    // needed a snapshot. Instead, when the core's replication path actually needs
    // to ship an `InstallSnapshot` and has no image (`take_snapshot_needed`), this
    // pass scans the engine once, re-bases the snapshot to exactly what that image
    // reflects (`snapshot_upto(ea)` *before* `set_snapshot_blob`, so base and
    // image agree), and installs it; the core drops the image again once no
    // transfer is in flight. A `KvState` WAL snapshot record carries only the unit
    // placeholder, so the threshold rewrite never needed the image bytes.
    let ea = engine_applied.load(Ordering::SeqCst);
    let (behind, image_needed) = {
        let mut c = core.lock().expect("raftkv core poisoned");
        (
            ea.saturating_sub(c.snapshot_index()),
            c.take_snapshot_needed(),
        )
    };
    // Skip compaction once a shutdown is requested: it is only a WAL-bounding
    // optimization (the engine + un-truncated WAL stay consistent without it), and
    // starting a full WAL rewrite while the env is being torn down races the task
    // abort — the `replace` can then fail on a half-gone data dir.
    if (behind >= COMPACT_THRESHOLD || image_needed) && !halted.load(Ordering::SeqCst) {
        // The on-demand image: a slow whole-engine scan, done with no locks held,
        // and only when a follower is actually waiting on a snapshot.
        let image = if image_needed {
            metrics.incr(Metric::CpSnapshotImageBuilds);
            Some(engine_image(storage, scope).await)
        } else {
            None
        };
        // Serialize the WAL rewrite against the consensus loop's appends.
        let _wal = wal_lock.lock().await;
        let (bytes, lli) = {
            let mut c = core.lock().expect("raftkv core poisoned");
            // Advance the base to exactly the engine state (`snapshot_upto` drops
            // any stale image + in-flight transfer offsets), THEN install the
            // fresh image built from that same state — order matters, or the
            // base-move would drop the image we just built.
            c.snapshot_upto(ea);
            if let Some(image) = image {
                c.set_snapshot_blob(image);
            }
            if !c.take_snapshot_dirty() {
                // The base did not move (e.g. an image rebuild at an unchanged
                // base): nothing to rewrite. The installed image alone makes the
                // pending transfer progress on the next heartbeat.
                (None, 0)
            } else {
                // The snapshot base actually advanced (a real truncation), whether
                // driven by the size threshold or by servicing an on-demand image
                // build above — distinct from `CpSnapshotImageBuilds` (PR #29's
                // lazy-image design decouples the two).
                metrics.incr(Metric::CpSnapshotTriggers);
                let lli = c.last_log_index();
                let mut buf = Vec::new();
                for record in c.wal_image() {
                    buf.extend(PersistedState::encode_record(&record));
                }
                // The rewrite (below) makes the whole current log durable, so the
                // consensus loop's accumulated pending append records are now
                // redundant — drop them (`replay` is push-based, so re-appending
                // them would duplicate entries). `wal_image` already captures the
                // net durable state (snapshot + hard + log tail). Under this one
                // lock hold, so no propose/append interleaves.
                let _ = c.drain_persist();
                (Some(buf), lli)
            }
        };
        if let Some(bytes) = bytes {
            match env.replace(WAL, &bytes).await {
                Ok(()) => {
                    // Physically durable now — advance the watermark.
                    core.lock()
                        .expect("raftkv core poisoned")
                        .mark_durable_through(lli);
                }
                // A shutdown that landed mid-rewrite (aborting tasks + dropping the
                // data dir) can fail the `replace`; tolerate it only while halted —
                // the pre-compaction WAL is still intact, so recovery is unaffected.
                // A failure while *not* halted is a real durability fault → surface.
                Err(e) => {
                    assert!(
                        halted.load(Ordering::SeqCst),
                        "raftkv wal compaction failed while running: {e}"
                    );
                }
            }
        }
        did_work = true;
    }

    did_work
}

/// Apply and clear an accumulated run of per-key LWW merges under a single WAL
/// `fsync` (see the apply loop). A no-op when the run is empty.
async fn flush_pending<S: StorageEngine>(
    storage: &S,
    pending: &mut Vec<MergeOp>,
    metrics: &MetricsHandle,
) {
    if pending.is_empty() {
        return;
    }
    metrics.incr(Metric::CpApplyBatchRuns);
    metrics.incr_by(Metric::CpApplyBatchSizeSum, pending.len() as u64);
    storage
        .merge_batch(std::mem::take(pending))
        .await
        .expect("raftkv apply merge batch");
}

/// Reserved key holding this group's **current split boundary** (ADR 0017
/// successive splits): a normal engine entry, written via the *same*
/// `merge_batch` call as a `Split`'s tombstones, so it durably survives
/// everything real data already survives — WAL compaction, process restart,
/// and cross-replica `InstallSnapshot` catch-up — via the exact same
/// mechanism, with no new persistence path to get wrong. `RaftCore`'s
/// `DRIVER_APPLIED` snapshot contract keeps `core.metadata` a permanent unit
/// placeholder for this state machine (real state lives in the engine, never
/// in-core) — see `apply_and_compact`'s recovery call site — so this
/// durability has to come from the engine's own storage, not from Raft-log
/// replay: once a `Split` entry's index falls below `snapshot_index`,
/// compaction discards it from the log forever, and nothing else would
/// re-derive that this group had already split.
///
/// **Overwritten, not appended, on each split** — one key's worth of state
/// forever, not a per-split history. See
/// [`current_split_bound`](RaftKvNode::current_split_bound)'s doc for why a
/// full history isn't kept (it would be unbounded and O(n²) to maintain) and
/// why overwriting is still safe.
///
/// **Never returned to a client or handed off to a split child** — every
/// application-facing read (`keys_from`, `linearizable_scan`) filters it out
/// explicitly; `entries_with_tombstones` (only used to build the snapshot
/// image) does *not* filter it, since the image is exactly what should carry
/// it to a lagging follower or a restart. Chosen to be implausible as a real
/// application key (long, NUL-delimited, namespaced) — not collision-proof (a
/// plain-client key is unconstrained raw bytes, ADR 0022), a documented
/// pre-alpha tradeoff rather than a new reserved keyspace across every wire
/// edge.
const SPLIT_BOUND_KEY: &[u8] = b"\0__animus_cp_split_bound__\0";

/// Serialize a group's current split boundary for storage under
/// [`SPLIT_BOUND_KEY`]. Plain `serde_json` (not the binary snapshot-image
/// codec) — this rides as one small value inside the engine, not over the
/// wire in bulk, so the throughput case `codec.rs` exists for doesn't apply.
fn encode_split_bound(bound: &Option<Vec<u8>>) -> Vec<u8> {
    serde_json::to_vec(bound).expect("split bound serializes")
}

/// Inverse of [`encode_split_bound`]. A decode failure only ever means data
/// this group itself wrote is corrupt — loud (logged), not a panic, since a
/// missing/corrupt value is recoverable by treating it as "never split"
/// rather than taking the whole apply task down.
fn decode_split_bound(bytes: &[u8]) -> Option<Vec<u8>> {
    match serde_json::from_slice(bytes) {
        Ok(bound) => bound,
        Err(err) => {
            tracing::warn!(?err, "undecodable raftkv split bound dropped");
            None
        }
    }
}

/// This scope's live `(key, value)` pairs with `key >= at`, **excluding**
/// [`SPLIT_BOUND_KEY`] — the data handed off to the new tablet on a split,
/// or (via [`RaftKvNode::range_snapshot`]) surfaced to a client/the auto-split
/// loop. The history entry must never ride along: a split child inherits it
/// from a whole-history *value*, not from a stale copy of the parent's.
/// Returns *logical* keys (the prefix already stripped). Scans the whole
/// underlying engine and filters by prefix (see `StorageScope`'s doc: this
/// legacy path is prefix-scoped only, not range-scoped) — under the default
/// scope every key belongs to this group, so this is byte-for-byte the prior
/// behavior.
async fn keys_from<S: StorageEngine>(
    storage: &S,
    scope: &StorageScope,
    at: &[u8],
) -> Vec<(Vec<u8>, Vec<u8>)> {
    storage
        .entries()
        .await
        .expect("raftkv engine scan")
        .into_iter()
        .filter_map(|(k, vv)| {
            let logical = scope.strip_prefix_only(&k)?;
            (logical >= at && logical != SPLIT_BOUND_KEY).then(|| (logical.to_vec(), vv.value))
        })
        .collect()
}

/// One key's snapshot entry: `(key, value-or-tombstone, version)`.
pub(crate) type ImageEntry = (Vec<u8>, Option<Vec<u8>>, u64);

/// Serialize this scope's contents (including tombstones) as the snapshot
/// image shipped to a lagging follower. Bounded to `scope` (prefix **and**
/// range — see `StorageScope`'s doc): on a shared engine, an unbounded dump
/// would leak every other tenant's keys into this tablet's snapshot **and**
/// duplicate them into whichever engine receives it, corrupting a group that
/// never agreed to those writes through its own Raft log. Under the default
/// (whole) scope this is byte-for-byte the prior unbounded behavior.
async fn engine_image<S: StorageEngine>(storage: &S, scope: &StorageScope) -> Vec<u8> {
    let entries: Vec<ImageEntry> = storage
        .entries_with_tombstones()
        .await
        .expect("raftkv engine scan")
        .into_iter()
        .filter_map(|(k, v, version)| {
            scope
                .strip_in_range(&k)
                .map(|logical| (logical.to_vec(), v, version))
        })
        .collect();
    codec::encode_image(&entries)
}

/// Write a received snapshot image into the engine (a follower catching up),
/// versioned so per-key LWW keeps it consistent with the log tail merged on top.
/// The wire image carries *logical* keys (stripped by the sender's
/// `engine_image`); each is re-prefixed to *this* replica's own `scope`
/// before writing into the (possibly shared) engine.
async fn install_engine_image<S: StorageEngine>(storage: &S, scope: &StorageScope, bytes: &[u8]) {
    let entries: Vec<ImageEntry> = match codec::decode_image(bytes) {
        Ok(e) => e,
        Err(err) => {
            tracing::warn!(?err, "undecodable raftkv snapshot image dropped");
            return;
        }
    };
    for (key, value, version) in entries {
        let physical = scope.physical(&key);
        match value {
            Some(v) => {
                storage
                    .merge(&physical, &v, version)
                    .await
                    .expect("install put");
            }
            None => {
                storage
                    .merge_tombstone(&physical, version)
                    .await
                    .expect("install tombstone");
            }
        }
    }
}

/// The shared-state bundle handed to the driver tasks, built once in
/// [`RaftKvNode::start_inner`]. Bundled into a struct so the split into a consensus
/// loop + an apply task doesn't spread a dozen positional args across two spawns.
struct DriveState<E: Env, S: StorageEngine> {
    env: E,
    core: Arc<Mutex<KvCore>>,
    all_nodes: Vec<NodeId>,
    storage: S,
    reads: Arc<Mutex<ReadState>>,
    cas: Arc<Mutex<CasResults>>,
    engine_applied: Arc<AtomicU64>,
    current_split_bound_state: Arc<Mutex<Option<Vec<u8>>>>,
    wal_lock: Arc<AsyncMutex<()>>,
    on_split: Option<SplitHook>,
    halted: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    apply_stopped: Arc<AtomicBool>,
    propose_signal: Arc<ProposeSignal>,
    metrics: MetricsHandle,
    scope: StorageScope,
    stream: u64,
}

/// Idle back-off for the apply task: when there is nothing committed-and-durable to
/// merge it sleeps this long before re-checking. Under load `apply_and_compact`
/// keeps returning `true`, so the task never sleeps and apply stays close behind
/// commit — this only bounds latency (and CPU) while idle.
const APPLY_IDLE_POLL: Duration = Duration::from_millis(5);

/// The per-node **consensus loop**: recover from the WAL, spawn the apply task, then
/// repeatedly persist the WAL, wait for the next message or timer, step the core,
/// persist again, and ship outbound. Engine apply + compaction run on the *separate*
/// apply task (see [`apply_loop`]), so this loop never blocks on a slow batch of
/// merges or a compaction rewrite and can always service heartbeats / append
/// processing within the election timeout (the driver-liveness fix, ADR 0017).
/// Mirrors the control-plane `RaftNode` driver, minus its reconcile/failure-detector
/// loops.
async fn drive<E: Env, S: StorageEngine + 'static>(st: DriveState<E, S>) {
    let DriveState {
        env,
        core,
        all_nodes,
        storage,
        reads,
        cas,
        engine_applied,
        current_split_bound_state,
        wal_lock,
        on_split,
        halted,
        stopped,
        apply_stopped,
        propose_signal,
        metrics,
        scope,
        stream,
    } = st;

    let bytes = env.read(WAL).await.unwrap_or_default();
    let state = PersistedState::replay(PersistedState::decode(&bytes));
    if !state.is_empty() {
        let recovered =
            RaftCore::recovered(env.node_id(), &all_nodes, state, env.now(), env.next_u64());
        *core.lock().expect("raftkv core poisoned") = recovered;
    }
    // The durable engine already reflects the applied state up to the recovered
    // apply cursor (its writes preceded the WAL fsync that recorded them), and the
    // log tail re-applies idempotently as commit re-advances. Seed `engine_applied`
    // at that cursor so a read right after restart doesn't wait for the base to be
    // re-merged (it never is — only the tail is).
    engine_applied.store(
        core.lock().expect("raftkv core poisoned").last_applied(),
        Ordering::SeqCst,
    );
    // Restore the group's split history from durable storage — never from WAL
    // replay (see `SPLIT_BOUND_KEY`'s doc): once a `Split` entry's Raft index
    // falls below `snapshot_index`, compaction discards it from the log for
    // good, and replaying the (now-shorter) tail would never re-derive that
    // this group had already split. The engine itself is the durable source of
    // truth here, exactly like the rest of the applied state above.
    if let Ok(Some(vv)) = storage.get(&scope.physical(SPLIT_BOUND_KEY)).await {
        *current_split_bound_state
            .lock()
            .expect("raftkv current_split_bound_state poisoned") = decode_split_bound(&vv.value);
    }

    // Spawn the apply task now — after recovery seeded the core + `engine_applied`
    // + `current_split_bound_state`, so it never merges against pre-recovery state.
    env.spawn_task(apply_loop(
        env.clone(),
        Arc::clone(&core),
        storage,
        cas,
        on_split,
        Arc::clone(&engine_applied),
        current_split_bound_state,
        Arc::clone(&wal_lock),
        Arc::clone(&halted),
        apply_stopped,
        metrics.clone(),
        scope,
    ));

    loop {
        // A requested shutdown exits *between* persist passes so the WAL is never
        // left mid-write; `stopped` (paired with the apply task's `apply_stopped`)
        // tells the teardown path the artifacts are quiescent.
        if halted.load(Ordering::SeqCst) {
            stopped.store(true, Ordering::SeqCst);
            return;
        }
        persist_wal(&env, &core, &wal_lock).await;

        let now = env.now();
        let deadline = core.lock().expect("raftkv core poisoned").next_deadline();
        let wait = Duration::from_nanos(deadline.0.saturating_sub(now.0));

        // Snapshot the commit index before stepping the core so a real advance
        // (ADR 0015: record the outcome, not the attempt) can be attributed below.
        let before_commit = core.lock().expect("raftkv core poisoned").commit_index();

        // Each step yields outbound `KvWire` messages (Raft traffic and/or a read
        // probe ack). Three wakeup sources race: an inbound message, the Raft timer
        // deadline, and a **wake-on-propose** signal — a proposer raising the flag so
        // a freshly appended entry replicates at once (ADR 0017 single-write latency),
        // treated like an immediate heartbeat (`replicate_now`) rather than waiting
        // for the ~50ms tick.
        let recv_or_timer = select(env.recv_stream(stream), env.sleep(wait));
        let outs: Vec<(NodeId, KvWire)> = match select(
            ProposePending {
                signal: &propose_signal,
            },
            recv_or_timer,
        )
        .await
        {
            // Wake-on-propose: ship the new entry now (leader-only; empty otherwise).
            Either::Left(((), _)) => {
                let raft_outs = core
                    .lock()
                    .expect("raftkv core poisoned")
                    .replicate_now(env.now());
                raft_outs
                    .into_iter()
                    .map(|(to, m)| (to, KvWire::Raft(m)))
                    .collect()
            }
            Either::Right((Either::Left((envelope, _)), _)) => {
                let entropy = env.next_u64();
                match codec::decode_wire(&envelope.payload) {
                    Ok(KvWire::Raft(msg)) => {
                        let raft_outs: Vec<Out<KvCommand>> = core
                            .lock()
                            .expect("raftkv core poisoned")
                            .handle(envelope.from, msg, env.now(), entropy);
                        raft_outs
                            .into_iter()
                            .map(|(to, m)| (to, KvWire::Raft(m)))
                            .collect()
                    }
                    // A ReadProbe is answered iff we are still in the prober's term
                    // (we have not moved on to help elect a newer leader). Not
                    // consensus traffic — the core never sees it.
                    Ok(KvWire::ReadProbe { term, epoch }) => {
                        let same_term = core.lock().expect("raftkv core poisoned").term() == term;
                        if same_term {
                            vec![(envelope.from, KvWire::ReadProbeAck { term, epoch })]
                        } else {
                            Vec::new()
                        }
                    }
                    Ok(KvWire::ReadProbeAck { term, epoch }) => {
                        let mut r = reads.lock().expect("read state poisoned");
                        if let Some((t, acks)) = r.pending.get_mut(&epoch) {
                            if *t == term {
                                acks.insert(envelope.from);
                            }
                        }
                        Vec::new()
                    }
                    Err(err) => {
                        tracing::warn!(?err, "undecodable raftkv message dropped");
                        Vec::new()
                    }
                }
            }
            Either::Right((Either::Right(((), _)), _)) => {
                let entropy = env.next_u64();
                let raft_outs = core
                    .lock()
                    .expect("raftkv core poisoned")
                    .tick(env.now(), entropy);
                raft_outs
                    .into_iter()
                    .map(|(to, m)| (to, KvWire::Raft(m)))
                    .collect()
            }
        };

        let after_commit = core.lock().expect("raftkv core poisoned").commit_index();
        if after_commit > before_commit {
            metrics.incr_by(Metric::CpCommits, after_commit - before_commit);
        }
        record_kv_outbound(&metrics, &outs);

        // Durability before action: persist (fsync) before shipping responses, so a
        // granted vote / appended entry is on disk before its message goes out.
        // Engine apply happens independently on the apply task.
        persist_wal(&env, &core, &wal_lock).await;

        for (to, wire) in outs {
            env.send_stream(to, stream, codec::encode_wire(&wire)).await;
        }
    }
}

/// The per-node **apply task**: repeatedly install any received snapshot, apply
/// committed-and-durable commands to the engine, and compact — all off the consensus
/// loop, so this slow work never delays Raft message/heartbeat processing (the
/// driver-liveness fix, ADR 0017). Backs off by [`APPLY_IDLE_POLL`] only when idle;
/// under load it stays in lockstep behind commit. Exits after
/// [`shutdown`](RaftKvNode::shutdown) between full apply passes (so the engine/WAL
/// are never left mid-write), setting `apply_stopped` for the teardown path.
#[allow(clippy::too_many_arguments)] // the apply task's shared-state bundle
async fn apply_loop<E: Env, S: StorageEngine>(
    env: E,
    core: Arc<Mutex<KvCore>>,
    storage: S,
    cas: Arc<Mutex<CasResults>>,
    on_split: Option<SplitHook>,
    engine_applied: Arc<AtomicU64>,
    current_split_bound_state: Arc<Mutex<Option<Vec<u8>>>>,
    wal_lock: Arc<AsyncMutex<()>>,
    halted: Arc<AtomicBool>,
    apply_stopped: Arc<AtomicBool>,
    metrics: MetricsHandle,
    scope: StorageScope,
) {
    // `current_split_bound_state` is shared (not a task-local `Option<Vec<u8>>`) so
    // `RaftKvNode::current_split_bound_state` can observe it — the confirm-by-key primitive
    // `propose_split`'s callers must poll (see its doc). It persists across loop
    // iterations by construction (an `Arc` handed in once), including a full WAL
    // replay after restart: the recovered log applies through this same loop
    // before any live command, so a replayed `Split` sets the key exactly as a
    // live one would.
    loop {
        if halted.load(Ordering::SeqCst) {
            apply_stopped.store(true, Ordering::SeqCst);
            return;
        }
        let did_work = apply_and_compact(
            &env,
            &core,
            &storage,
            &cas,
            &on_split,
            &engine_applied,
            &current_split_bound_state,
            &wal_lock,
            &halted,
            &metrics,
            &scope,
        )
        .await;
        if !did_work {
            env.sleep(APPLY_IDLE_POLL).await;
        }
    }
}
