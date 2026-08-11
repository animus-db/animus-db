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
//! node. **A single-command, control-plane-driven split** (ADR 0028) replaces
//! the original two-phase split D (a data-plane `Split` command + in-band
//! `Coresident` sibling minting): a tablet's range only ever changes via the
//! control plane's replicated `Metadata` (its own `MetaCommand::SplitTablet`),
//! and every tablet's data lives in a caller-provided, possibly **shared**
//! `StorageEngine` confined by [`StorageScope`] — so a split moves no bytes at
//! all, and forming the new tablet's group is just [`RaftKvNode::start_hosted`]
//! against the already-populated shared engine, scoped to its own range.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use animus_control::raft::{Out, RaftCore, RaftMsg, StateMachine};
use animus_control::{PersistedState, ProposeResult};
use animus_env::{Env, EnvExt, Metric, MetricsHandle, Nanos, NodeId, PRIMARY_STREAM};
use animus_storage::{MergeOp, StorageEngine};
use animus_tablet::KeyRange;
use futures::future::{Either, select};
use futures::lock::Mutex as AsyncMutex;
use futures::task::AtomicWaker;
use serde::{Deserialize, Serialize};

mod ceiling;
mod codec;
pub mod hlc;
pub mod host;
mod seal;
mod ts_cache;

use hlc::{Hlc, HlcTimestamp};
use ts_cache::TsCache;

/// The assumed maximum clock-offset bound across the cluster (ADR 0018 §2),
/// threaded into every [`Hlc::new`] this crate constructs. Not yet consumed
/// for uncertainty-interval read restarts (that lands with the read path in
/// a later PR) — `Hlc::uncertainty_upper` is unused until then.
const HLC_MAX_OFFSET: std::time::Duration = std::time::Duration::from_millis(500);

/// The **wake-on-propose** signal (ADR 0017 single-write-latency fix): a shared
/// flag + executor-agnostic waker that lets a proposer (`put`/`delete`/`cas`/
/// `change_membership`) nudge the consensus loop to replicate the
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
/// possibly-**shared** `StorageEngine` (ADR 0028): every physical key this
/// group ever writes is `prefix || key`, and a read is bounded to physical
/// keys whose stripped (post-prefix) suffix falls inside `range`. `prefix`
/// identifies the *table* (stable across splits — many sibling tablets of one
/// table share a prefix), `range` the *tablet's own sub-portion* within it
/// (the same range recorded in the control plane's `Metadata`).
///
/// [`whole`](Self::whole) — empty prefix, the whole keyspace — makes every
/// physical-key operation an identity transform, used by the plain-client
/// `ClientRequest` path (which has no table concept) and by every test that
/// doesn't need multi-tenant scoping.
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
    /// this `StorageScope` observes the change immediately. A raw setter: the
    /// caller (which watches `Metadata` for this tablet's current range) is
    /// trusted to only call this when the new range is actually correct for
    /// this tablet right now — which is *usually* a narrowing (a split
    /// source, `RaftKvNode::narrow_scope`) but is a legitimate **widening**
    /// when this tablet just absorbed a merged-away sibling's range (ADR
    /// 0033, `RaftKvNode::widen_scope`). Both call through this one setter;
    /// the direction is enforced (or intentionally not enforced) by the
    /// caller, not here.
    pub fn narrow(&self, new_range: KeyRange) {
        *self.range.lock().expect("storage scope range poisoned") = new_range;
    }

    /// A snapshot of this scope's current live range (see the type doc). The
    /// range can narrow again the instant after this call returns — this is
    /// a point-in-time read, not a held lock — so a caller using it as a
    /// pre-propose fence-check (ADR 0028 write-fence wiring, `animusd`'s
    /// `cp_put_local`/`cp_delete_local`/`cp_batch_propose`) still needs the
    /// *proposed* command's own embedded `fence` (stamped from this same
    /// read) to cover the residual race between this read and the entry's
    /// actual apply.
    #[must_use]
    pub fn range(&self) -> KeyRange {
        self.range
            .lock()
            .expect("storage scope range poisoned")
            .clone()
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
            // fall back to the same whole-engine-then-filter shape `engine_image`
            // already uses for the unbounded case.
            None => storage
                .entries()
                .await
                .map(|rows| rows.iter().any(|(k, _)| self.strip_in_range(k).is_some()))
                .unwrap_or(false),
        }
    }

    /// This scope's own physical `(start, end)` bounds, for a caller that
    /// needs a **bounded** physical range even when the logical range is
    /// unbounded above (ADR 0034 — [`RaftKvNode::approx_bytes`]'s periodic
    /// hot-path gate, which must stay cheap and must never fall back to a
    /// whole-engine scan the way [`has_data`](Self::has_data) tolerates as a
    /// one-time hosting-decision cost).
    ///
    /// `start` is always `physical(range.start)`. `end` is `physical(end)`
    /// when the logical range has one; when it doesn't (the common
    /// "one big not-yet-split tablet" case — a fresh table's first tablet
    /// covers its own whole prefix), it is instead the **prefix upper
    /// bound**: the smallest physical key strictly greater than every key
    /// under this scope's `prefix` (increment the last byte below `0xFF`,
    /// dropping every trailing `0xFF` byte first — the standard
    /// range-scan-over-a-prefix idiom). This keeps the physical range
    /// confined to this scope's own prefix — never a sibling tenant sharing
    /// the same engine (ADR 0026/0028) — instead of degrading to "the rest of
    /// the keyspace." Only `StorageScope::whole()` (no prefix at all) or an
    /// astronomically unlikely all-`0xFF` prefix yields `end: None`, i.e.
    /// genuinely unbounded.
    #[must_use]
    pub(crate) fn physical_bounds(&self) -> (Vec<u8>, Option<Vec<u8>>) {
        let range = self.range();
        let start = self.physical(&range.start);
        let end = match range.end {
            Some(e) => Some(self.physical(&e)),
            None => prefix_upper_bound(&self.prefix),
        };
        (start, end)
    }
}

/// The smallest byte string strictly greater than every string with this
/// `prefix` — `None` if `prefix` is empty or entirely `0xFF` bytes (no finite
/// upper bound exists). See [`StorageScope::physical_bounds`]'s doc.
fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut out = prefix.to_vec();
    while let Some(&last) = out.last() {
        if last == 0xFF {
            out.pop();
        } else {
            *out.last_mut().expect("just checked non-empty") = last + 1;
            return Some(out);
        }
    }
    None
}

impl Default for StorageScope {
    fn default() -> Self {
        Self::whole()
    }
}

/// The data plane's Raft log command: a key-value mutation (or the election
/// no-op). Keys/values are opaque bytes; ordering + durability come from Raft.
///
/// **`fence`** (every mutating variant except `Seal`) is the leader's own
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
///
/// **`ts`** (every mutating variant, ADR 0018 §2/PR2) is the proposing
/// leader's HLC commit timestamp, minted from its own per-group [`Hlc`] at
/// *propose* time and packed ([`hlc::pack`]) as the engine's MVCC version at
/// apply — replacing the retired `version_floor`-scaled Raft index. Riding
/// inside the log entry (like `fence`) makes apply deterministic and
/// idempotent: replay always computes the same version for the same command.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvCommand {
    /// Set `key` to `value`, iff `key` falls inside `fence` (see the type-level
    /// doc below for what `fence` means and why every replica checks it at
    /// *apply* time rather than the proposer checking it once).
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        fence: KeyRange,
        ts: HlcTimestamp,
    },
    /// **Batch put**: set every `(key, value)` in one Raft log entry — one propose,
    /// one commit round, one apply. All keys are merged at the entry's shared HLC
    /// commit timestamp (the shared MVCC version): the keys are distinct, so
    /// per-key LWW is well-defined, and re-applying on recovery is idempotent
    /// exactly as a single `Put` is. The throughput win over N individual `Put`s is
    /// one consensus round for the whole batch instead of one per key (ADR 0017 —
    /// bulk-write batching). Within one tablet the batch is atomic (it either
    /// commits whole or not at all); a cross-tablet batch is split into one `Batch`
    /// per tablet by the caller and is not atomic across tablets (matching
    /// DynamoDB `BatchWriteItem` semantics). `fence` gates the *whole* batch: if
    /// any key falls outside it, none of the batch applies (preserves the batch's
    /// atomicity — see the type-level doc).
    Batch {
        puts: Vec<(Vec<u8>, Vec<u8>)>,
        fence: KeyRange,
        ts: HlcTimestamp,
    },
    /// Remove `key` (a tombstone in the engine), iff `key` falls inside `fence`.
    Delete {
        key: Vec<u8>,
        fence: KeyRange,
        ts: HlcTimestamp,
    },
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
        ts: HlcTimestamp,
    },
    /// **Range seal** (ADR 0018 §2 amendment, PR2 — see `seal.rs`'s module
    /// doc for the full design): the leader of a range-handoff source (a
    /// split's `NarrowScope`, or a merge's `Absorb`) commits this through its
    /// **own** Raft log to mark `range` closed to any further mutation
    /// ordered after it. Every replica applies its log in the same order, so
    /// every replica agrees on exactly which entries are "after the seal" —
    /// unlike `fence`, this is not itself gated by a fence (a seal IS a fence
    /// change: it never touches engine data itself, only apply-time
    /// bookkeeping + the durable marker key). No `fence` field: a seal always
    /// applies (it is itself the authority tightening what future entries
    /// may touch); see `apply_and_compact`'s `Seal` arm.
    Seal { range: KeyRange, ts: HlcTimestamp },
    /// **Logged read ceiling** (ADR 0018 §2/PR2b — see `ceiling.rs`'s module
    /// doc): proposed by a group's own leader, through its **own** Raft log,
    /// when it wants to serve a read at or above the highest ceiling
    /// currently committed for this group. Apply is a no-op against the
    /// engine's *scoped* data (no fence, like `Seal` — a ceiling carries no
    /// keys to gate): it only advances the driver's `committed_ceiling`
    /// watermark and (durability across compaction, see `ceiling.rs`)
    /// merges a marker value at this tablet's ceiling key. **Internal
    /// only** — proposed exclusively by a group's own leader in its own
    /// read path (`RaftKvNode::ensure_ceiling_above`), never forwarded from
    /// a client, so no `animusd` command-relay allowlist needs updating
    /// (grepped: `animusd` never matches on individual `KvCommand`
    /// variants — every client-facing propose goes through `RaftKvNode`'s
    /// own `put`/`get`/`cas`/`scan` methods, not a raw command).
    ReadCeiling { ts: HlcTimestamp },
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

/// WAL filename stem for a tablet group's Raft log (distinct from the control
/// plane's `raft.wal`, so a node can host both without collision).
const WAL: &str = "raftkv.wal";

/// The **per-tablet** WAL filename on a node's `raftkv` env: `raftkv.wal.{stream}`.
/// Since ADR 0026 Stage B every tablet a node hosts shares one env/port
/// (stream-addressed) and, since ADR 0028, one shared `StorageEngine` too — but
/// `Disk` files are keyed by name, not by stream, so each tablet's consensus
/// **log** still needs its own file on that shared env. Physically consolidating
/// every tablet's log into one multiplexed file per node
/// (`animus_control::SharedWal`, tagged by tablet — built but not yet wired in,
/// see the root `CLAUDE.md`) is a further, separately-tested step; this keeps
/// the write path simple and correct in the meantime. Public so a teardown path
/// (drop-table GC, ADR 0024) can delete a stopped group's exact WAL file.
pub fn wal_file(stream: u64) -> String {
    format!("{WAL}.{stream}")
}

/// A running data-plane Raft node for one tablet group. Cheap to clone; clones
/// share the one [`RaftCore`] + engine. The driver loop runs on `env`.
#[derive(Clone)]
pub struct RaftKvNode<E: Env, S: StorageEngine> {
    env: E,
    core: Arc<Mutex<KvCore>>,
    storage: S,
    reads: Arc<Mutex<ReadState>>,
    cas: Arc<Mutex<CasResults>>,
    /// Highest Raft log index the **apply task** has merged into the engine. The
    /// consensus loop advances the core's `last_applied` (its buffer cursor) as soon
    /// as entries are committed+durable, but the async apply task lags behind
    /// merging them into the engine — so linearizable reads gate on *this* (engine
    /// progress), never `last_applied`, or they could read past the engine's state.
    engine_applied: Arc<AtomicU64>,
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
    /// This group's per-node Hybrid Logical Clock (ADR 0018 §2/PR2), which
    /// every mutating propose method mints a fresh `ts` from
    /// (`hlc::HlcTimestamp`, packed as the engine MVCC version at apply —
    /// see `apply_and_compact`). Replaces the retired `version_floor`
    /// cross-group-LWW fix: rather than a structural version-space
    /// separation, cross-group ordering now comes from **witnessing**
    /// (`Hlc::witness`, at WAL recovery, on every received entry, on
    /// snapshot install, and — the one witness this field's own
    /// construction performs — at group start, off the shared engine's
    /// `latest_version()`) plus, for the residual in-flight-write race
    /// witnessing alone can't close, the **range seal** (`seal.rs`).
    hlc: Arc<Hlc>,
    /// The per-tablet **read-timestamp cache** (ADR 0018 §2/PR2b,
    /// `ts_cache.rs`): leader-local, in-memory, best-effort write-conflict
    /// push. Bumped by every served read (`linearizable_get`/`_scan`,
    /// `read_at`/`scan_at`); consulted at propose time by every mutating
    /// method to push a write's `ts` above any read it would otherwise land
    /// at or below. Losing it (a crash/restart) is always safe — see the
    /// module doc.
    ts_cache: Arc<Mutex<TsCache>>,
    /// This group's **committed read ceiling** (ADR 0018 §2/PR2b,
    /// `ceiling.rs`): the highest `KvCommand::ReadCeiling` timestamp this
    /// group has *applied* so far, packed via [`hlc::pack`] for lock-free
    /// access. A read may only be served at a `ts` strictly below this.
    /// Updated by the apply task (`apply_and_compact`'s `ReadCeiling` arm);
    /// read by this handle's own `ensure_ceiling_above`/`read_at`/`scan_at`.
    committed_ceiling: Arc<AtomicU64>,
    /// The highest `ReadCeiling` **candidate** this leader has ever
    /// proposed (whether committed yet or not), packed via [`hlc::pack`] —
    /// disambiguates two `ensure_ceiling_above` calls that independently
    /// compute the same [`Hlc::uncertainty_upper`] margin (millisecond-
    /// granular, so this collides more easily than an ordinary mint).
    /// **Deliberately separate from `hlc`**: unlike `Hlc::witness`, bumping
    /// this ratchet never feeds back into what `hlc.mint` produces for an
    /// ordinary read/write, so proposing a ceiling never drags this
    /// group's own future timestamps toward the (deliberately
    /// future-shifted) margin — see `next_ceiling_candidate`'s doc for the
    /// cascade that mistake caused.
    last_ceiling_candidate: Arc<AtomicU64>,
}

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
        Self::start_inner(env, all_nodes, storage, metrics, scope, PRIMARY_STREAM)
    }

    /// Like [`start_scoped`](Self::start_scoped), but the group also sends/recvs
    /// on `stream` (ADR 0026 Stage B) instead of `PRIMARY_STREAM` — the mechanism
    /// that lets several tablet groups share one **node id**'s inbox (multiplexed
    /// by stream, typically the tablet id) instead of each minting a distinct
    /// `Coresident` sibling id. Combined with a shared `storage` + distinct
    /// `scope`s, this is the full "several tablets co-resident on one node" shape
    /// `animusd`'s real hosting path (`cp_join_host`) uses — `stream` doubles as
    /// this group's tablet id for the range-seal marker's key (`seal.rs`),
    /// mirroring the reconciler's own `stream = tablet.0` convention.
    pub fn start_hosted(
        env: E,
        all_nodes: Vec<NodeId>,
        storage: S,
        scope: StorageScope,
        stream: u64,
    ) -> Self {
        let metrics = env.metrics();
        Self::start_inner(env, all_nodes, storage, metrics, scope, stream)
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
            metrics,
            StorageScope::whole(),
            PRIMARY_STREAM,
        )
    }

    fn start_inner(
        env: E,
        all_nodes: Vec<NodeId>,
        storage: S,
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
        let wal_lock = Arc::new(AsyncMutex::new(()));
        let propose_signal = Arc::new(ProposeSignal::default());
        // Group-start witnessing (ADR 0018 §2 amendment): fold in whatever
        // this (possibly shared, ADR 0026/0028) engine's highest MVCC
        // version already reflects, so this group's own future mints never
        // undercut a version any group ever stamped on it — subsuming the
        // retired `version_floor` seeding for the steady-state case (a
        // restart, a co-hosted sibling already present). `latest_version()`
        // is engine-global and cheap/synchronous (`animus-storage`'s trait
        // doc), so this needs no async step here.
        let hlc = Arc::new(Hlc::new(env.node_id(), HLC_MAX_OFFSET));
        hlc.witness(hlc::unpack(storage.latest_version()), env.now());
        let ts_cache = Arc::new(Mutex::new(TsCache::new()));
        let committed_ceiling = Arc::new(AtomicU64::new(0));
        let last_ceiling_candidate = Arc::new(AtomicU64::new(0));
        let node = Self {
            env: env.clone(),
            core: Arc::clone(&core),
            storage: storage.clone(),
            reads: Arc::clone(&reads),
            cas: Arc::clone(&cas),
            engine_applied: Arc::clone(&engine_applied),
            halted: Arc::clone(&halted),
            stopped: Arc::clone(&stopped),
            apply_stopped: Arc::clone(&apply_stopped),
            propose_signal: Arc::clone(&propose_signal),
            metrics: metrics.clone(),
            scope: scope.clone(),
            stream,
            hlc: Arc::clone(&hlc),
            ts_cache: Arc::clone(&ts_cache),
            committed_ceiling: Arc::clone(&committed_ceiling),
            last_ceiling_candidate,
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
            wal_lock,
            halted,
            stopped,
            apply_stopped,
            propose_signal,
            metrics,
            scope,
            stream,
            hlc,
            committed_ceiling,
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

    /// A majority of the group's **current** Raft voter config — not
    /// `all_nodes` (the peer set this node happened to be hosted/started with,
    /// frozen at that moment). `all_nodes` predates ADR 0029: every prior use of
    /// membership change was a same-size, pre-known swap (a failure-repair spare
    /// was already listed in every replica's `all_nodes` from the start), so it
    /// never actually diverged from the live config. A healthy rebalance move
    /// can add a node that was never in any existing replica's `all_nodes` at
    /// all — using the stale set here would make [`read_barrier`](Self::read_barrier)'s
    /// quorum-of-acks requirement permanently unreachable (probing peers that
    /// are no longer voters, never reaching the new ones that are), timing out
    /// every linearizable read on that tablet forever after such a move.
    fn majority(&self) -> usize {
        self.config().len() / 2 + 1
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

    /// This group's currently **committed read ceiling** (ADR 0018 §2/PR2b,
    /// `ceiling.rs`): the highest `ReadCeiling` timestamp this group has
    /// *applied* so far. A read may only be served at a `ts` strictly below
    /// this — see `ensure_ceiling_above`/`read_at`/`scan_at`. Public as an
    /// admin/debug accessor (ADR 0020), alongside `term`/`commit_index`/etc.
    pub fn committed_ceiling(&self) -> HlcTimestamp {
        hlc::unpack(self.committed_ceiling.load(Ordering::SeqCst))
    }

    /// Disambiguate a `ReadCeiling` candidate against every candidate this
    /// leader has ever proposed (`last_ceiling_candidate`, a lock-free CAS
    /// ratchet): if `margin` (already strictly above the read it covers,
    /// from [`Hlc::uncertainty_upper`]) exceeds the highest candidate seen
    /// so far, it wins outright; otherwise the highest-seen candidate's
    /// `logical` component is bumped by one — cheap, since a genuine
    /// collision (two calls computing the identical millisecond-granular
    /// margin) is the rare case, not the common one.
    ///
    /// **Never uses `Hlc::witness` for this.** Witnessing a margin that
    /// sits `HLC_MAX_OFFSET` in the *future* would drag this leader's own
    /// `hlc` forward to match it — poisoning every ordinary `mint` right
    /// after with an inflated baseline, so the *next* read's serve ts lands
    /// close to (and soon exceeds) the ceiling just committed, forcing
    /// another proposal almost immediately. That turns the intended O(1)
    /// amortized proposal rate into O(N) — a real regression a seed-driven
    /// test caught. This ratchet is a separate piece of state precisely so
    /// disambiguating a ceiling candidate never touches the clock every
    /// read/write proposer shares.
    fn next_ceiling_candidate(&self, margin: HlcTimestamp) -> HlcTimestamp {
        loop {
            let last_packed = self.last_ceiling_candidate.load(Ordering::SeqCst);
            let last = hlc::unpack(last_packed);
            let candidate = if margin > last {
                margin
            } else {
                // Bump the logical component; carry into wall_ms on the
                // (astronomically unlikely) overflow, mirroring `Hlc`'s own
                // carry rule — this is a monotonic ratchet, not a wraparound.
                let bumped_logical = last.logical.wrapping_add(1);
                if bumped_logical >= (1 << hlc::LOGICAL_BITS) {
                    HlcTimestamp {
                        wall_ms: last.wall_ms + 1,
                        logical: 0,
                    }
                } else {
                    HlcTimestamp {
                        wall_ms: last.wall_ms,
                        logical: bumped_logical,
                    }
                }
            };
            if self
                .last_ceiling_candidate
                .compare_exchange(
                    last_packed,
                    hlc::pack(candidate),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                return candidate;
            }
        }
    }

    /// Mint a write's `ts`, **pushed** above any read this group's
    /// [`ts_cache`](Self::ts_cache) or committed read ceiling has already
    /// served for `keys` (ADR 0018 §2/PR2b — the write-conflict-push half of
    /// serializability): a write must never land at or below a timestamp
    /// `keys` were already read at. Folds the committed ceiling in as an
    /// additional floor (mirrors `ts_cache.rs::raise_low_water`'s doc): every
    /// past read, by this leader or a predecessor, was served below *some*
    /// committed ceiling, so pushing above the ceiling pushes above every
    /// read anyone could have served, even one this leader-local cache has
    /// no per-span record of.
    ///
    /// One retry always suffices: [`Hlc::witness`]'s own contract guarantees
    /// the re-mint strictly exceeds the witnessed floor — asserted here,
    /// not just assumed, since a failure would mean `Hlc::witness` itself
    /// stopped upholding that contract (a correctness bug, not a
    /// recoverable condition, matching `assert_ts_monotonic`'s doctrine).
    fn mint_pushed<K: AsRef<[u8]>>(&self, keys: &[K]) -> HlcTimestamp {
        let ts = self.hlc.mint(self.env.now());
        let floor = {
            // Opportunistically ratchet the cache's own `low_water` up to the
            // current committed ceiling before querying it (never regresses —
            // see `TsCache::raise_low_water`'s doc) — the mechanism that lets
            // a freshly-elected leader's cache catch up to what its
            // predecessor's ceiling already covered, with no separate
            // "on leader change" event to detect.
            let mut cache = self.ts_cache.lock().expect("ts cache poisoned");
            cache.raise_low_water(self.committed_ceiling());
            cache.max_overlapping(keys)
        };
        if ts > floor {
            return ts;
        }
        let pushed = self.hlc.witness(floor, self.env.now());
        assert!(
            pushed > floor,
            "raftkv write-push: witnessing the floor must strictly exceed it \
             (floor={floor:?}, got={pushed:?}) — Hlc::witness's own contract is broken"
        );
        pushed
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
        let ts = self.mint_pushed(std::slice::from_ref(&key));
        self.propose_and_wake(KvCommand::Put {
            key,
            value,
            fence,
            ts,
        })
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
        let keys: Vec<&[u8]> = puts.iter().map(|(k, _)| k.as_slice()).collect();
        let ts = self.mint_pushed(&keys);
        record_propose(
            &self.metrics,
            self.lock().propose(KvCommand::Batch { puts, fence, ts }),
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
        let ts = self.mint_pushed(std::slice::from_ref(&key));
        self.propose_and_wake(KvCommand::Delete { key, fence, ts })
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
        let ts = self.mint_pushed(std::slice::from_ref(&key));
        self.propose_and_wake(KvCommand::Cas {
            key,
            expected,
            value,
            fence,
            ts,
        })
    }

    /// Propose a **range seal** (ADR 0018 §2 amendment, PR2 — see `seal.rs`'s
    /// module doc): mark `range` closed to any further mutation ordered after
    /// this entry in this group's own Raft log. Leader-only (else a leader
    /// hint, like every other propose method). Called by the tablet-host
    /// reconciler (`host::Reconciler`) when executing a split source's
    /// `NarrowScope` or an absorbed tablet's `Absorb` teardown — never by any
    /// data-plane client. Idempotent to re-propose the identical `range`
    /// (the marker key is keyed by `(tablet, range)`, so a repeat simply
    /// refreshes it with a newer `ts` — see `seal.rs`).
    pub fn propose_seal(&self, range: KeyRange) -> ProposeResult {
        let ts = self.hlc.mint(self.env.now());
        self.propose_and_wake(KvCommand::Seal { range, ts })
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

    /// The leader's last-known replicated log index for `node` (0 if unknown).
    /// The caught-up primitive a healthy reconfigure step gates on — see
    /// [`RaftCore::peer_match`].
    pub fn peer_match(&self, node: &NodeId) -> u64 {
        self.lock().peer_match(node)
    }

    /// Arm a leadership transfer to `target` (see
    /// [`RaftCore::transfer_leadership`]) and, if armed, wake the driver loop so
    /// the resulting `TimeoutNow` ships at once instead of waiting for the next
    /// heartbeat — mirrors `change_membership`'s wake-on-propose. Returns
    /// whether the transfer was armed.
    pub fn transfer_leadership(&self, target: NodeId) -> bool {
        let armed = self.lock().transfer_leadership(target, self.env.now());
        if armed {
            self.propose_signal.notify();
        }
        armed
    }

    /// Take **one** single-server step moving this group's Raft configuration
    /// toward `desired` — the control plane's placement decision for this tablet
    /// (ADR 0017 Stage C: the automatic reconfigure trigger; ADR 0029 extends it
    /// to a *healthy* rebalance move, not just failure repair). `down` is this
    /// tablet's currently-`Down` members (per the control plane's failure
    /// detector) — a node can be in `desired` and simultaneously `down` (e.g. a
    /// flapping replica the reconciler hasn't yet replaced), so this always
    /// treats "extra and down" specially rather than assuming `down` and
    /// `desired` are disjoint.
    ///
    /// The shared [`RaftCore::change_membership`] only accepts a single-server
    /// delta, with no in-flight change and no leader self-removal, so this picks
    /// one add/remove that makes progress and lets the next tick take the
    /// following step — a multi-server move converges one server per call.
    /// Returns the proposed config if a step was **accepted**; `None` if already
    /// converged, not the leader, a change is in flight, or a step was armed
    /// but not itself a config change (the leadership-transfer case below).
    ///
    /// Priority order, most urgent first:
    /// 1. **Remove an extra `Down` voter** (never self) — restores quorum margin
    ///    immediately; this is failure repair, and the removed node isn't going
    ///    to ack anything anyway, so there is nothing to wait for.
    /// 2. **Add a missing voter.** A transient `desired.len() + 1`-voter config
    ///    is strictly safer than dropping a healthy voter first: quorum keeps
    ///    its pre-move margin while the newcomer catches up via log/
    ///    `InstallSnapshot`, instead of briefly running at reduced margin.
    /// 3. **Remove an extra *healthy* voter** (never self) — but only once every
    ///    member of `desired` has caught up to this leader's `commit_index`.
    ///    Skipping this gate would let a healthy move (e.g. a rebalance) drop
    ///    quorum to a still-catching-up newcomer, an availability regression
    ///    relative to just leaving the extra replica in place a little longer.
    /// 4. **The only remaining delta is removing the leader's own replica** —
    ///    `change_membership` always rejects that, so instead transfer
    ///    leadership (see [`transfer_leadership`](Self::transfer_leadership)) to
    ///    the lowest-id caught-up member of `desired`. The new leader's own next
    ///    tick then removes the old leader, an ordinary (non-self) removal.
    pub fn reconfigure_step(
        &self,
        desired: &BTreeSet<NodeId>,
        down: &BTreeSet<NodeId>,
    ) -> Option<BTreeSet<NodeId>> {
        let current = self.config();
        if current == *desired || !self.is_leader() {
            return None;
        }
        let me = self.env.node_id();
        // Any extra (non-self) voter, regardless of liveness — used by step 3
        // (a *healthy* extra). Step 1 below searches independently for a *down*
        // extra: `extra().filter(down.contains)` would only ever look at the
        // lowest-id extra (bug fixed under ADR 0029's follow-up — see the root
        // CLAUDE.md engineering-practices entry), silently skipping a Down extra
        // that happens to sort after a healthy one.
        let extra = || current.difference(desired).find(|&n| n != &me).cloned();
        let down_extra = || {
            current
                .difference(desired)
                .find(|&n| n != &me && down.contains(n))
                .cloned()
        };

        if let Some(target) = down_extra() {
            let mut c = current.clone();
            c.remove(&target);
            return self.propose_config(c);
        }
        if let Some(missing) = desired.difference(&current).next() {
            let mut c = current.clone();
            c.insert(missing.clone());
            return self.propose_config(c);
        }
        if let Some(healthy_extra) = extra() {
            let commit = self.commit_index();
            let caught_up = desired
                .iter()
                .filter(|&n| n != &me)
                .all(|n| self.peer_match(n) >= commit);
            if !caught_up {
                return None;
            }
            let mut c = current.clone();
            c.remove(&healthy_extra);
            return self.propose_config(c);
        }
        // The only delta left is removing the leader itself. Select the
        // lexicographically-least (ADR 0040 PR3: ids are strings now, so this
        // is no longer numeric — still a deterministic total order, so every
        // replica picks the same target) member of `desired` reasonably close
        // to caught up (`>= commit_index`, matching
        // `RaftCore::transfer_leadership`'s arm gate) and try to arm a
        // transfer to it — idempotent, and retried every tick via
        // `spawn_reconfigure_loop` as long as this delta persists, so a
        // one-time arming failure (e.g. every candidate momentarily fell behind
        // `commit_index`) self-heals on the next tick rather than needing a
        // caller-visible retry. Log (don't silently drop) both outcomes: a
        // stalled rebalance move that never finds an eligible target is
        // otherwise invisible until an operator notices a tablet's leader never
        // migrates off a node it should have left.
        let commit = self.commit_index();
        match desired
            .iter()
            .filter(|&n| n != &me && self.peer_match(n) >= commit)
            .min()
        {
            Some(target) => {
                let armed = self.transfer_leadership(target.clone());
                // NOTE: the field is named `xfer_target`, not `target` —
                // `tracing`'s macros reserve the bare `target` identifier for
                // overriding the event's own target module path.
                if armed {
                    tracing::debug!(
                        xfer_target = %target,
                        commit,
                        "reconfigure_step: armed leadership transfer to remove self"
                    );
                } else {
                    tracing::warn!(
                        xfer_target = %target,
                        commit,
                        "reconfigure_step: transfer_leadership rejected an apparently-eligible target"
                    );
                }
            }
            None => {
                tracing::warn!(
                    ?desired,
                    commit,
                    "reconfigure_step: must remove self but no member of `desired` is caught up to commit_index yet"
                );
            }
        }
        None
    }

    fn propose_config(&self, next: BTreeSet<NodeId>) -> Option<BTreeSet<NodeId>> {
        match self.change_membership(next.clone()) {
            ProposeResult::Accepted { .. } => Some(next),
            ProposeResult::NotLeader { .. } => None,
        }
    }

    /// Spawn the **automatic Stage-C reconfigure loop** (ADR 0017, extended by
    /// ADR 0029): on each `interval` tick, poll `desired` (and `down`) for this
    /// tablet's target voter set and take one
    /// [`reconfigure_step`](Self::reconfigure_step) toward it. Idempotent and
    /// leader-gated — a non-leader or a converged group proposes nothing, so a
    /// steady cluster produces no churn; a multi-server move converges one server
    /// per tick. `desired`/`down` are the **seam to the control plane**: in
    /// production `desired` reads `Metadata.tablets[tablet].replicas` (the
    /// placement reconciler's epoch-CAS decision) and `down` reads the
    /// `Down`-status members, each returned as a closure so this crate takes no
    /// dependency on the control-plane driver type. Mirrors the control plane's
    /// `reconcile_loop` (decision elsewhere, timing here).
    pub fn spawn_reconfigure_loop<F, D>(&self, interval: Duration, desired: F, down: D)
    where
        F: Fn() -> Option<BTreeSet<NodeId>> + Send + 'static,
        D: Fn() -> BTreeSet<NodeId> + Send + 'static,
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
                    node.reconfigure_step(&target, &down());
                }
            }
        });
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

    /// Update this group's live [`StorageScope`] range to a **wider** range
    /// (ADR 0033 tablet merge — the dual of [`narrow_scope`](Self::narrow_scope)):
    /// called when this tablet was the **surviving (`left`) side** of a
    /// `MetaCommand::MergeTablets` commit, whose replicated range now covers
    /// what used to be the merged-away sibling's range too. Safe precisely
    /// because `MergeTablets` only merges two tablets that already shared a
    /// replica set on the same node's shared engine (ADR 0026/0028) — the
    /// absorbed range's data was always physically present under the same
    /// table prefix, nothing needs to move. The mechanism underneath is the
    /// same raw setter `narrow_scope` uses; this is a distinctly-named,
    /// distinctly-documented entry point so a reader auditing every
    /// `StorageScope` mutation site doesn't have to re-derive "is this
    /// specific call safe to widen" from context each time.
    pub fn widen_scope(&self, new_range: KeyRange) {
        self.scope.narrow(new_range);
    }

    /// This group's own current [`StorageScope`] range (see its doc) — a
    /// point-in-time snapshot, additive accessor (ADR 0028 write-fence
    /// wiring). Lets a caller (e.g. `animusd`'s `cp_put_local`/
    /// `cp_delete_local`/`cp_batch_propose`) both **pre-check** a key against
    /// this group's live scope *before* proposing (so a stale-routed,
    /// out-of-range write errors instead of being silently accepted as a
    /// fenced-out no-op — see those callers' doc for why the pre-check
    /// matters even though the fence itself also protects apply) and stamp
    /// the *same* range as the proposed command's own `fence` (`put_fenced`/
    /// `delete_fenced`/`put_batch_fenced`), so every replica's apply makes
    /// the identical accept/reject decision regardless of how far it has
    /// independently progressed observing a concurrent split.
    #[must_use]
    pub fn scope_range(&self) -> KeyRange {
        self.scope.range()
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
        self.linearizable_get_served(key).await.flatten()
    }

    /// [`linearizable_get`](Self::linearizable_get) with the two `None` causes
    /// **disambiguated**: the outer `Option` is "was this read actually served"
    /// (`None` = the read barrier failed — not/no-longer the leader, or the
    /// quorum probe timed out — so nothing can be concluded about the key at
    /// all); the inner `Option` is the served answer (`Some(None)` = the key is
    /// genuinely absent). A caller that reports "absent" to a client **must**
    /// use this variant and treat the outer `None` as a retryable
    /// routing/leadership error, never as absence — collapsing the two (as the
    /// plain `linearizable_get` does for callers that only ever poll for a
    /// known-written value) turns a transient barrier failure into a false
    /// "key absent," indistinguishable from data loss from the outside (ADR
    /// 0033 read-path fix; the exact failure shape the root `CLAUDE.md`'s ADR
    /// 0029 read-barrier entry describes).
    pub async fn linearizable_get_served(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        if !self.read_barrier().await {
            return None;
        }
        // ADR 0018 §2/PR2b: serve at the leader's current mint, ensure the
        // group's committed ceiling covers it (proposing/waiting for a fresh
        // one if not — see `ensure_ceiling_above`'s doc), then bump the
        // read-timestamp cache with the *actual* ts served, so a concurrent
        // or later write to this key is pushed above it.
        let ts = self.hlc.mint(self.env.now());
        if !self.ensure_ceiling_above(ts).await {
            return None;
        }
        let value = self.local_get(key).await;
        let (start, end) = ts_cache::point_span(key);
        self.ts_cache
            .lock()
            .expect("ts cache poisoned")
            .bump(start, end, ts);
        Some(value)
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
        let ts = self.hlc.mint(self.env.now());
        if !self.ensure_ceiling_above(ts).await {
            return None;
        }
        let rows = self.local_scan(start, end, limit).await;
        // Bump the *whole requested span* (not just the rows a `limit`
        // happened to return): over-conservative, never wrong — a future
        // write anywhere in `[start, end)` is still pushed above this read,
        // exactly like a rotated-away `ts_cache` generation (see that
        // module's doc).
        self.ts_cache.lock().expect("ts cache poisoned").bump(
            start.to_vec(),
            end.map(<[u8]>::to_vec),
            ts,
        );
        Some(rows)
    }

    /// This replica's live `(key, value)` pairs with `start <= key < end`, sorted
    /// by key, up to `limit`, from the **local engine** — *not* linearizable (no
    /// ReadIndex barrier), the scan counterpart of [`local_get`](Self::local_get).
    /// `end == None` is unbounded above, but still bounded to *this scope*'s own
    /// physical range via [`StorageScope::physical_bounds`] — see the body's
    /// comment for why an actual whole-engine scan there would be a real cost
    /// bug on a node hosting several tablets (ADR 0028). Used for admin/debug
    /// introspection and the auto-split key-materialization path, which only
    /// ever run on a replica inspecting its own state.
    pub async fn local_scan(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        // Push the range down into the engine (audit P4): a bounded scan reads
        // only `[start, end)` instead of materializing the whole tablet and
        // filtering; both `scan` and `entries` return key-ordered results by the
        // `StorageEngine` contract, so the old re-sort was redundant — drop it
        // and apply the limit to the already-ordered rows. An unbounded-above
        // scan (`end == None`, e.g. a full-table `Scan`) uses
        // `StorageScope::physical_bounds()` (ADR 0034) to derive a genuinely
        // bounded physical upper bound for *this scope* — the tablet's own
        // `range.end` if set, else the prefix-upper-bound trick for a
        // not-yet-split tablet — and falls back to `entries()` (a whole-engine
        // scan) only for the one case that has no finite bound at all,
        // `StorageScope::whole()` (no prefix). This matters because a node's
        // tablets share one `StorageEngine` (ADR 0028): `entries()` scans every
        // co-resident tablet's data, not just this one's, so on a node hosting
        // several tablets `local_scan`/`local_pairs`'s unbounded path used to
        // cost O(hosted tablets × whole node engine) — every `/admin/raftkv`
        // request and every `erase_scope()` teardown (rebalance `Release`,
        // drop-table `Reclaim`) paid a full-engine scan just to find this one
        // tablet's own handful of keys.
        // Both branches physically bound/filter to `self.scope` — on a
        // possibly-shared engine, `entries()` in particular would otherwise
        // return every other tenant's keys too (see `StorageScope`'s doc).
        // Under the default (whole) scope this is byte-for-byte the prior
        // behavior: `physical` is the identity and `strip_in_range` always
        // succeeds.
        let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = match end {
            Some(e) => self
                .storage
                .scan(&self.scope.physical(start), &self.scope.physical(e))
                .await
                .ok()
                .into_iter()
                .flatten()
                .filter_map(|(k, vv)| {
                    let logical = self.scope.strip_in_range(&k)?;
                    Some((logical.to_vec(), vv.value))
                })
                .collect(),
            None => match self.scope.physical_bounds().1 {
                Some(physical_end) => self
                    .storage
                    .scan(&self.scope.physical(start), &physical_end)
                    .await
                    .ok()
                    .into_iter()
                    .flatten()
                    .filter_map(|(k, vv)| {
                        let logical = self.scope.strip_in_range(&k)?;
                        Some((logical.to_vec(), vv.value))
                    })
                    .collect(),
                None => self
                    .storage
                    .entries()
                    .await
                    .ok()
                    .into_iter()
                    .flatten()
                    .filter_map(|(k, vv)| {
                        let logical = self.scope.strip_in_range(&k)?;
                        (logical >= start).then(|| (logical.to_vec(), vv.value))
                    })
                    .collect(),
            },
        };
        if let Some(n) = limit {
            pairs.truncate(n);
        }
        pairs
    }

    /// A **linearizable-anchored MVCC snapshot read** of `key` **as of**
    /// `ts` (ADR 0018 §2/PR2b): runs the same ReadIndex barrier as
    /// [`linearizable_get`](Self::linearizable_get) — this must still be
    /// the confirmed leader, current on `engine_applied` — then reads the
    /// value with version `≤ hlc::pack(ts)` instead of the latest.
    ///
    /// **Semantics, precisely**: the result reflects every write with
    /// commit `ts' ≤ ts` that was already committed *and applied* on this
    /// leader before the barrier confirmed. A write with `ts' ≤ ts` still
    /// **in flight** (proposed, not yet committed/applied) at barrier time
    /// is *not* guaranteed to be reflected — closing that gap across
    /// multiple keys/tablets is the cross-tablet transaction protocol's job
    /// (intents, PR3+), not this primitive's. This is the single-tablet
    /// MVCC snapshot-read building block, not a transaction's read.
    ///
    /// Same `Option<Option<_>>` shape as
    /// [`linearizable_get_served`](Self::linearizable_get_served): outer
    /// `None` means **not served** — either the read barrier failed, *or*
    /// `ts` is not yet strictly below this group's committed read ceiling
    /// (`ceiling.rs`) and this call, unlike `linearizable_get`/`_scan`
    /// (which mint their own serve `ts` and so can always drive the
    /// ceiling forward themselves), does not drive the ceiling forward for
    /// a caller-supplied `ts` — a `read_at` refusal is a signal to retry
    /// after something else has advanced the ceiling past `ts` (e.g. a
    /// `linearizable_get`/`_scan` on this group), or later. Inner
    /// `Some(None)` is a genuine "absent as of `ts`".
    pub async fn read_at(&self, key: &[u8], ts: HlcTimestamp) -> Option<Option<Vec<u8>>> {
        if !self.read_barrier().await {
            return None;
        }
        if self.committed_ceiling() <= ts {
            return None;
        }
        let physical = self.scope.physical(key);
        let value = self
            .storage
            .get_at(&physical, hlc::pack(ts))
            .await
            .ok()
            .flatten()
            .map(|vv| vv.value);
        let (start, end) = ts_cache::point_span(key);
        self.ts_cache
            .lock()
            .expect("ts cache poisoned")
            .bump(start, end, ts);
        Some(value)
    }

    /// The range counterpart of [`read_at`](Self::read_at): the live
    /// `(key, value)` pairs with `start <= key < end` **as of `ts`**,
    /// sorted by key — same barrier + ceiling-refusal contract as
    /// `read_at`, and the same scope-bounding shape as
    /// [`local_scan`](Self::local_scan) (bounded via `end`, or
    /// `StorageScope::physical_bounds` when unbounded above, falling back
    /// to a whole-engine [`StorageEngine::entries_at`] only for
    /// `StorageScope::whole()`).
    pub async fn scan_at(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        ts: HlcTimestamp,
    ) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
        if !self.read_barrier().await {
            return None;
        }
        if self.committed_ceiling() <= ts {
            return None;
        }
        let version = hlc::pack(ts);
        let rows: Vec<(Vec<u8>, Vec<u8>)> = match end {
            Some(e) => self
                .storage
                .scan_at(
                    &self.scope.physical(start),
                    &self.scope.physical(e),
                    version,
                )
                .await
                .ok()
                .into_iter()
                .flatten()
                .filter_map(|(k, vv)| {
                    let logical = self.scope.strip_in_range(&k)?;
                    Some((logical.to_vec(), vv.value))
                })
                .collect(),
            None => match self.scope.physical_bounds().1 {
                Some(physical_end) => self
                    .storage
                    .scan_at(&self.scope.physical(start), &physical_end, version)
                    .await
                    .ok()
                    .into_iter()
                    .flatten()
                    .filter_map(|(k, vv)| {
                        let logical = self.scope.strip_in_range(&k)?;
                        Some((logical.to_vec(), vv.value))
                    })
                    .collect(),
                None => self
                    .storage
                    .entries_at(version)
                    .await
                    .ok()
                    .into_iter()
                    .flatten()
                    .filter_map(|(k, vv)| {
                        let logical = self.scope.strip_in_range(&k)?;
                        (logical >= start).then(|| (logical.to_vec(), vv.value))
                    })
                    .collect(),
            },
        };
        self.ts_cache.lock().expect("ts cache poisoned").bump(
            start.to_vec(),
            end.map(<[u8]>::to_vec),
            ts,
        );
        Some(rows)
    }

    /// A **cheap, range-scoped byte estimate** for this tablet (ADR 0034: the
    /// auto-split trigger is byte-based, not key-count-based). Delegates to
    /// [`StorageEngine::approx_bytes_in_range`] over this group's own live
    /// [`StorageScope::physical_bounds`] — `LsmEngine` answers from its own
    /// in-memory SSTable/memtable metadata (no disk read, no materialization,
    /// matching `animusd`'s LSM-only `approx_key_count`'s cost — but, unlike
    /// that key-count sibling, this stays **scoped** to this tablet even for
    /// a not-yet-split, unbounded-above tablet, via `physical_bounds`'s
    /// prefix-upper-bound trick); any other backend (e.g. `MemoryEngine`)
    /// answers exactly via the trait's default. A storage
    /// error reads as `0` — the same "never block the periodic gate on an
    /// estimate" spirit as `approx_key_count`'s `Option`-returning LSM-only
    /// accessors — since the auto-split loop's materializing confirm step
    /// (`local_pairs`) is the authoritative check regardless of what this
    /// estimate says.
    pub async fn approx_bytes(&self) -> u64 {
        let (start, end) = self.scope.physical_bounds();
        self.storage
            .approx_bytes_in_range(&start, end.as_deref())
            .await
            .unwrap_or(0)
    }

    /// Erase every key in this group's own `StorageScope` from the (possibly
    /// node-shared, ADR 0026/0028) engine, without touching any other tablet's
    /// data on it. For **drop-table GC** (ADR 0024) only — call after the group's
    /// driver has been shut down and confirmed [`is_stopped`](Self::is_stopped),
    /// since no live Raft group should still be proposing for a tablet while its
    /// data is being erased.
    ///
    /// Tombstones each key via [`StorageEngine::merge_tombstone`] (never
    /// [`StorageEngine::delete_range`], which enforces an engine-wide monotonic
    /// version floor a *shared* engine's independent per-tablet Raft groups don't
    /// share) at a **fresh HLC timestamp minted from this group's own clock**
    /// (ADR 0018 §2 amendment) — strictly greater than every version this
    /// group ever wrote for these keys, since every merge it ever performed
    /// packed a timestamp this same `Hlc` had already minted or witnessed.
    /// `Hlc::mint` needs no live driver (it is a pure, I/O-free clock), so
    /// this is safe to call after the group has halted. Actual space
    /// reclaim happens later via the engine's normal tombstone-GC compaction.
    pub async fn erase_scope(&self) {
        // ADR 0018 §2 amendment: the engine version is now the packed HLC
        // commit timestamp, not the Raft log index — so the tombstone must
        // be stamped from this group's own `Hlc::mint`, which is guaranteed
        // to strictly exceed every timestamp this group has ever minted (and
        // so every version this group ever wrote for these keys), the exact
        // property the old `last_applied() + 1` scheme provided for the
        // retired index-based version.
        let version = hlc::pack(self.hlc.mint(self.env.now()));
        for (key, _) in self.local_scan(&[], None, None).await {
            let _ = self
                .storage
                .merge_tombstone(&self.scope.physical(&key), version)
                .await;
        }
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
        // explicit probe confirms promptly). Probe the **current** voter config
        // (see `majority`'s doc), not the static `all_nodes` — else a rebalanced
        // group's read barrier would probe peers that are no longer voters and
        // never reach the ones that are.
        let probe = codec::encode_wire(&KvWire::ReadProbe { term, epoch });
        for p in self.config() {
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

    /// Ensure this group's **committed read ceiling** (ADR 0018 §2/PR2b,
    /// `ceiling.rs`) is strictly above `ts`, proposing a fresh
    /// `KvCommand::ReadCeiling` (and waiting for it to commit + apply) if it
    /// is not — the mechanism that makes a read served at `ts` recoverable
    /// after a leader change (see `ceiling.rs`'s module doc for the full
    /// safety argument). The candidate ceiling is
    /// [`Hlc::uncertainty_upper`]`(ts)` (`ts.wall_ms + max_offset`): a
    /// comfortable margin above `ts` so ceiling proposals amortize to
    /// roughly one per `HLC_MAX_OFFSET` of wall time under continuous
    /// reads at monotonically advancing timestamps, **not** one per read —
    /// the common case (`committed_ceiling() > ts` already) proposes
    /// nothing at all.
    ///
    /// Returns `false` if this node is not (or stops being) the leader, or
    /// the proposal doesn't land within [`READ_TIMEOUT`] — the caller must
    /// treat that exactly like a failed [`read_barrier`](Self::read_barrier)
    /// (no read may be served).
    async fn ensure_ceiling_above(&self, ts: HlcTimestamp) -> bool {
        if self.committed_ceiling() > ts {
            return true;
        }
        if !self.is_leader() {
            return false;
        }
        // `uncertainty_upper` intentionally collapses to `(wall_ms +
        // max_offset, logical: 0)` — millisecond-granular, not the full HLC
        // total order. Two reads whose serve `ts` merely share a wall-clock
        // millisecond (differing only in `logical`) would otherwise compute
        // *byte-identical* margins; embedding that margin directly as the
        // command's `ts` risks two separately-proposed `ReadCeiling`
        // entries landing with the exact same `ts`, tripping the
        // apply-time monotonicity assert every command must satisfy
        // (`assert_ts_monotonic`) — a real, seed-found regression this
        // comment exists to warn future edits away from.
        //
        // The fix is deliberately **not** `self.hlc.witness(margin, ..)`:
        // witnessing a margin that is 500ms in the *future* would drag this
        // leader's own `self.hlc` forward to match it, so every ordinary
        // read immediately afterward mints a `ts` already close to that
        // inflated baseline — catching up to (and exceeding) the very
        // ceiling just proposed almost at once, forcing a fresh proposal
        // on every subsequent read (a real regression this comment exists
        // to warn future edits away from: it turns O(1) amortized
        // proposals into O(N)). `next_ceiling_candidate` disambiguates
        // via a **separate** ratchet that never feeds back into `self.hlc`
        // (the clock every read/write proposer shares) — only the
        // committed-ceiling candidate sequence itself.
        let margin = self.hlc.uncertainty_upper(ts);
        let candidate = self.next_ceiling_candidate(margin);
        match self.propose_and_wake(KvCommand::ReadCeiling { ts: candidate }) {
            ProposeResult::Accepted { .. } => {
                self.metrics.incr(Metric::CpReadCeilingProposals);
            }
            ProposeResult::NotLeader { .. } => return false,
        }
        let deadline = self.env.now().0 + READ_TIMEOUT.as_nanos() as u64;
        loop {
            if self.committed_ceiling() > ts {
                return true;
            }
            if !self.is_leader() || self.env.now().0 >= deadline {
                return false;
            }
            self.env.sleep(READ_POLL).await;
        }
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

/// Record the real outcome of a data propose (`put`/`put_batch`/`delete`/`cas`) —
/// accepted, or rejected because this node is not the leader — and pass the
/// result through unchanged. ADR 0015: count the outcome, never the attempt.
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
async fn persist_wal<E: Env>(
    env: &E,
    wal: &str,
    core: &Arc<Mutex<KvCore>>,
    wal_lock: &AsyncMutex<()>,
) {
    let _wal = wal_lock.lock().await;
    let (records, through) = {
        let mut c = core.lock().expect("raftkv core poisoned");
        (c.drain_persist(), c.last_log_index())
    };
    if records.is_empty() {
        return;
    }
    for record in &records {
        env.append(wal, &PersistedState::encode_record(record))
            .await
            .expect("raftkv wal append");
    }
    env.sync(wal).await.expect("raftkv wal sync");
    core.lock()
        .expect("raftkv core poisoned")
        .mark_durable_through(through);
}

/// Whether `key` falls inside any range this group has already sealed
/// (ADR 0018 §2 amendment): a `Seal` entry applies before every entry
/// ordered after it, so `sealed` only ever needs entries seen so far in this
/// pass (plus whatever the group-start rebuild found) — never the whole
/// group's history looked up freshly each time.
fn is_sealed(sealed: &[(KeyRange, HlcTimestamp)], key: &[u8]) -> bool {
    sealed.iter().any(|(range, _)| range.contains(key))
}

/// Assert that `ts` is strictly greater than every `ts` this group has
/// applied so far (the log-order-monotonicity invariant the whole HLC/
/// witnessing design rests on — see `hlc.rs` and `seal.rs`'s module docs).
/// `None` (the very first non-`NoOp` entry applied since this apply task
/// started, including right after a restart) is trivially satisfied — a
/// fresh `Hlc` at `HlcTimestamp::zero()` could otherwise collide with a
/// same-instant first entry. **A failure here means the witnessing chain is
/// broken**: either a leader change didn't witness its predecessor's last
/// entry, or two leaders minted concurrently without one witnessing the
/// other — a correctness bug, not a recoverable condition, so this is a hard
/// `assert!`, matching `hlc::pack`'s own doctrine.
fn assert_ts_monotonic(max_applied_ts: &mut Option<HlcTimestamp>, ts: HlcTimestamp) {
    if let Some(prev) = *max_applied_ts {
        assert!(
            ts > prev,
            "raftkv apply: HLC ts {ts:?} did not strictly exceed the last applied {prev:?} — \
             the witnessing chain is broken (a leader change or concurrent leader failed to \
             witness a predecessor's timestamp)"
        );
    }
    *max_applied_ts = Some(ts);
}

/// Install any received snapshot, apply committed-and-durable commands to the
/// engine in commit order, and compact when the engine has merged enough past the
/// snapshot base. **Runs on the apply task only** — off the consensus loop, so a
/// slow batch of engine merges or a compaction rewrite never stalls heartbeats /
/// append processing (the driver-liveness fix). Returns whether it did any work, so
/// the caller can back off when idle. `engine_applied` publishes engine progress
/// (linearizable reads gate on it), and `wal_lock` guards the compaction rewrite.
///
/// `hlc` witnesses every entry received this pass (WAL recovery's own
/// witnessing happens once, in `drive`, before this function is ever called —
/// see that function's doc); `sealed`/`max_applied_ts` are this apply task's
/// own sequential, single-writer bookkeeping (see `is_sealed`/
/// `assert_ts_monotonic`), threaded by the caller across calls — never
/// touched by any other task, so no `Arc`/lock is needed for either.
#[allow(clippy::too_many_arguments)] // the apply task's shared-state bundle
async fn apply_and_compact<E: Env, S: StorageEngine>(
    env: &E,
    wal: &str,
    core: &Arc<Mutex<KvCore>>,
    storage: &S,
    cas: &Arc<Mutex<CasResults>>,
    engine_applied: &AtomicU64,
    wal_lock: &AsyncMutex<()>,
    halted: &AtomicBool,
    metrics: &MetricsHandle,
    scope: &StorageScope,
    tablet: u64,
    hlc: &Hlc,
    sealed: &mut Vec<(KeyRange, HlcTimestamp)>,
    max_applied_ts: &mut Option<HlcTimestamp>,
    committed_ceiling: &AtomicU64,
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
        // Witnessing point (ADR 0018 §2 amendment): a snapshot can carry
        // versions this node has never seen minted, so fold in the engine's
        // new high-water mark before this node ever mints/compares again.
        hlc.witness(hlc::unpack(storage.latest_version()), env.now());
        did_work = true;
    }

    // Apply the now-durable committed commands to the engine, in commit order.
    // The packed HLC commit timestamp is the MVCC version: per-key LWW then
    // reproduces the agreed cross-group order, and re-applying on recovery is
    // idempotent (the same command always computes the same `ts`).
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
    // *read* committed state (`Cas`, `Seal`) first drains the pending run so its
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
            KvCommand::Put {
                key,
                value,
                fence,
                ts,
            } => {
                assert_ts_monotonic(max_applied_ts, ts);
                // Out-of-fence is a deterministic no-op: the fence rides in the
                // entry itself (stamped by the leader at propose time), so every
                // replica reaches this same accept/reject decision regardless of
                // its own progress learning the tablet's range has changed (see
                // `KvCommand`'s doc). A sealed-out key is the same shape (ADR 0018
                // §2 amendment): the key fell in a range this group already
                // handed off, so this entry — necessarily proposed by a leader
                // that hadn't yet learned that — is rejected exactly like a fence
                // miss. The fence/seal checks are against the *logical* key; only
                // the storage-bound `MergeOp` gets the physical address (see
                // `StorageScope`'s doc — under the default scope this is an
                // identity transform).
                if fence.contains(&key) && !is_sealed(sealed, &key) {
                    pending.push(MergeOp::put(scope.physical(&key), value, hlc::pack(ts)));
                }
            }
            KvCommand::Batch { puts, fence, ts } => {
                assert_ts_monotonic(max_applied_ts, ts);
                // The fence/seal gates the *whole* batch, not per-key: a batch is
                // one atomic Raft entry (see `KvCommand::Batch`'s doc), so
                // partially applying it on a miss would silently break that
                // guarantee. Every key in the batch merges at this one entry's
                // shared `ts`. The keys are distinct, so per-key LWW is
                // well-defined; `engine_applied` advances once past the whole batch
                // at the end of the loop iteration (the batch is one entry). Composes
                // with a future coalesced-fsync merge_batch (perf/lsm) — this is the
                // normal per-key `merge` path that batching optimization refines.
                if puts
                    .iter()
                    .all(|(key, _)| fence.contains(key) && !is_sealed(sealed, key))
                {
                    for (key, value) in &puts {
                        storage
                            .merge(&scope.physical(key), value, hlc::pack(ts))
                            .await
                            .expect("raftkv apply batch put");
                    }
                }
            }
            KvCommand::Delete { key, fence, ts } => {
                assert_ts_monotonic(max_applied_ts, ts);
                if fence.contains(&key) && !is_sealed(sealed, &key) {
                    pending.push(MergeOp::tombstone(scope.physical(&key), hlc::pack(ts)));
                }
            }
            KvCommand::Cas {
                key,
                expected,
                value,
                fence,
                ts,
            } => {
                assert_ts_monotonic(max_applied_ts, ts);
                // Drain the pending run so the CAS read observes every earlier
                // committed write in this apply pass.
                flush_pending(storage, &mut pending, metrics).await;
                // A fenced- or sealed-out CAS never reads/writes storage — it is
                // recorded as `false` ("did not swap"), the same outcome shape a
                // proposer already handles for an ordinary `expected` mismatch, so
                // a confirm-poll on this index never hangs waiting for an outcome
                // that will never come.
                let swapped = if fence.contains(&key) && !is_sealed(sealed, &key) {
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
                        // Same write path as `Put`: `hlc::pack(ts)` is the MVCC
                        // version, so re-applying on recovery is idempotent
                        // (per-key LWW).
                        storage
                            .merge(&physical_key, &value, hlc::pack(ts))
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
            KvCommand::Seal { range, ts } => {
                assert_ts_monotonic(max_applied_ts, ts);
                // Applying a Seal writes its durable marker (the successor's
                // observable witness, `seal.rs`) at `hlc::pack(ts)` — flush the
                // pending run first so the marker's own version never precedes
                // an already-queued write in this same pass (ordering hygiene,
                // not a correctness requirement: the marker key is disjoint
                // from every scoped key, see `seal.rs`).
                flush_pending(storage, &mut pending, metrics).await;
                let marker_key = seal::seal_marker_key(tablet, &range);
                storage
                    .merge(
                        &marker_key,
                        &seal::encode_seal_value(&range, ts),
                        hlc::pack(ts),
                    )
                    .await
                    .expect("raftkv apply seal marker");
                sealed.push((range, ts));
            }
            KvCommand::ReadCeiling { ts } => {
                assert_ts_monotonic(max_applied_ts, ts);
                // No fence, no scoped engine write — a ceiling carries no
                // keys (see `KvCommand::ReadCeiling`'s doc). Bump the
                // driver's watermark unconditionally (`fetch_max`, since a
                // stale re-application on recovery replay must never
                // regress it) and durably merge the marker (ADR 0018 §2/
                // PR2b, `ceiling.rs`) so this survives a restart even after
                // compaction truncates this very log entry — see that
                // module's doc for the full argument. Flush the pending run
                // first, matching `Seal`'s ordering hygiene above.
                committed_ceiling.fetch_max(hlc::pack(ts), Ordering::SeqCst);
                flush_pending(storage, &mut pending, metrics).await;
                let marker_key = ceiling::ceiling_marker_key(tablet);
                storage
                    .merge(
                        &marker_key,
                        &ceiling::encode_ceiling_value(ts),
                        hlc::pack(ts),
                    )
                    .await
                    .expect("raftkv apply read-ceiling marker");
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
            match env.replace(wal, &bytes).await {
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
    wal_lock: Arc<AsyncMutex<()>>,
    halted: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    apply_stopped: Arc<AtomicBool>,
    propose_signal: Arc<ProposeSignal>,
    metrics: MetricsHandle,
    scope: StorageScope,
    stream: u64,
    hlc: Arc<Hlc>,
    committed_ceiling: Arc<AtomicU64>,
}

/// The `ts` a mutating [`KvCommand`] variant carries, or `None` for `NoOp`
/// (which carries none). The one place that knows every variant's `ts`
/// field, shared by the WAL-recovery and entry-receipt witnessing sites.
fn command_ts(command: &KvCommand) -> Option<HlcTimestamp> {
    match command {
        KvCommand::Put { ts, .. }
        | KvCommand::Batch { ts, .. }
        | KvCommand::Delete { ts, .. }
        | KvCommand::Cas { ts, .. }
        | KvCommand::Seal { ts, .. }
        | KvCommand::ReadCeiling { ts, .. } => Some(*ts),
        KvCommand::NoOp => None,
    }
}

/// Witnessing point (ADR 0018 §2 amendment): fold every command's `ts` found
/// in an incoming `AppendEntries`' entries into `hlc` — the "entry
/// receipt/append on every replica" chokepoint. Deliberately witnesses
/// **every** entry in the message, whether or not `RaftCore::handle` (called
/// separately, right after) ultimately accepts it (e.g. a stale/conflicting
/// term) — a witness that turns out to be unnecessary is always safe (it
/// only ever advances the clock, never regresses it), while skipping one a
/// genuinely-accepted entry needed would be a real gap. Other `RaftMsg`
/// variants carry no commands and are ignored.
fn witness_append_entries(hlc: &Hlc, msg: &RaftMsg<KvCommand>, now: Nanos) {
    if let RaftMsg::AppendEntries { entries, .. } = msg {
        for entry in entries {
            if let Some(ts) = command_ts(&entry.command) {
                hlc.witness(ts, now);
            }
        }
    }
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
        wal_lock,
        halted,
        stopped,
        apply_stopped,
        propose_signal,
        metrics,
        scope,
        stream,
        hlc,
        committed_ceiling,
    } = st;

    let wal = wal_file(stream);
    let bytes = env.read(&wal).await.unwrap_or_default();
    let state = PersistedState::replay(PersistedState::decode(&bytes));
    // Witnessing point (ADR 0018 §2 amendment): "WAL recovery, each recovered
    // entry." Every command this node ever durably logged for this group —
    // applied or not yet — must be witnessed before this node ever mints or
    // compares a timestamp again, so a restart can never re-mint below
    // anything it (or a predecessor leader whose entries it holds) already
    // committed. Borrowed from `state.log` *before* `PersistedState::replay`'s
    // output is consumed by `RaftCore::recovered` below.
    for entry in &state.log {
        if let Some(ts) = command_ts(&entry.command) {
            hlc.witness(ts, env.now());
        }
    }
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
    // Rebuild this group's in-memory sealed-range set from the engine's own
    // durable marker keys (ADR 0018 §2 amendment) — the deterministic
    // recovery source, deliberately NOT the recovered log tail: compaction
    // can truncate a `Seal` entry out of the log long before its rejection
    // duty is done (a stale/un-ticked leader's late proposal can still land
    // many entries later), so only the engine-durable marker — which
    // survives compaction like any other key — is a complete source across
    // a restart. See `seal.rs`'s module doc.
    let (seal_start, seal_end) = seal::scan_bound(stream);
    let sealed: Vec<(KeyRange, HlcTimestamp)> = storage
        .scan(&seal_start, &seal_end)
        .await
        .unwrap_or_default()
        .into_iter()
        .filter_map(|(_, vv)| seal::decode_seal_value(&vv.value))
        .collect();
    // Rebuild `committed_ceiling` from its own durable engine marker (ADR
    // 0018 §2/PR2b, `ceiling.rs`) — the same "engine marker survives
    // compaction, log replay might not" reasoning as `sealed` above. This is
    // also what lets a restarted node re-witness a compacted-away ceiling:
    // the marker's `merge` durably advanced `storage.latest_version()`, and
    // this group's `Hlc` already witnessed that (`start_inner`'s group-start
    // witness, above `drive`'s own caller) before this function ever ran.
    let ceiling_key = ceiling::ceiling_marker_key(stream);
    if let Ok(Some(vv)) = storage.get(&ceiling_key).await
        && let Some(ts) = ceiling::decode_ceiling_value(&vv.value)
    {
        committed_ceiling.fetch_max(hlc::pack(ts), Ordering::SeqCst);
    }
    // Spawn the apply task now — after recovery seeded the core + `engine_applied`
    // + `sealed` + `committed_ceiling`, so it never merges against pre-recovery state.
    env.spawn_task(apply_loop(
        env.clone(),
        wal.clone(),
        Arc::clone(&core),
        storage,
        cas,
        Arc::clone(&engine_applied),
        Arc::clone(&wal_lock),
        Arc::clone(&halted),
        apply_stopped,
        metrics.clone(),
        scope,
        stream,
        Arc::clone(&hlc),
        sealed,
        committed_ceiling,
    ));

    loop {
        // A requested shutdown exits *between* persist passes so the WAL is never
        // left mid-write; `stopped` (paired with the apply task's `apply_stopped`)
        // tells the teardown path the artifacts are quiescent.
        if halted.load(Ordering::SeqCst) {
            stopped.store(true, Ordering::SeqCst);
            return;
        }
        persist_wal(&env, &wal, &core, &wal_lock).await;

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
                        // Witnessing point (ADR 0018 §2 amendment): every
                        // command entry this replica receives — leader or
                        // follower alike — before the core decides whether to
                        // accept it (see `witness_append_entries`'s doc).
                        witness_append_entries(&hlc, &msg, env.now());
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
                        if let Some((t, acks)) = r.pending.get_mut(&epoch)
                            && *t == term
                        {
                            acks.insert(envelope.from);
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
        persist_wal(&env, &wal, &core, &wal_lock).await;

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
    wal: String,
    core: Arc<Mutex<KvCore>>,
    storage: S,
    cas: Arc<Mutex<CasResults>>,
    engine_applied: Arc<AtomicU64>,
    wal_lock: Arc<AsyncMutex<()>>,
    halted: Arc<AtomicBool>,
    apply_stopped: Arc<AtomicBool>,
    metrics: MetricsHandle,
    scope: StorageScope,
    stream: u64,
    hlc: Arc<Hlc>,
    mut sealed: Vec<(KeyRange, HlcTimestamp)>,
    committed_ceiling: Arc<AtomicU64>,
) {
    // This apply task's own sequential, single-writer bookkeeping (see
    // `apply_and_compact`'s doc): `sealed` is seeded from the engine-durable
    // recovery scan `drive` already did; `max_applied_ts` starts `None` each
    // time this task starts (including after a restart) — the first
    // qualifying entry it processes is unconditionally accepted (see
    // `assert_ts_monotonic`'s doc for why that boundary case is safe).
    let mut max_applied_ts: Option<HlcTimestamp> = None;
    loop {
        if halted.load(Ordering::SeqCst) {
            apply_stopped.store(true, Ordering::SeqCst);
            return;
        }
        let did_work = apply_and_compact(
            &env,
            &wal,
            &core,
            &storage,
            &cas,
            &engine_applied,
            &wal_lock,
            &halted,
            &metrics,
            &scope,
            stream,
            &hlc,
            &mut sealed,
            &mut max_applied_ts,
            &committed_ceiling,
        )
        .await;
        if !did_work {
            env.sleep(APPLY_IDLE_POLL).await;
        }
    }
}
