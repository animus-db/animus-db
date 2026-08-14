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
pub mod cursor;
pub mod hlc;
pub mod host;
mod seal;
mod ts_cache;
mod txn;

use hlc::{Hlc, HlcTimestamp};
use ts_cache::TsCache;
pub use txn::{StageOutcome, TxnDecisionStatus, TxnId, TxnOutcome};

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

/// Row-kind scope selector: base item rows — the ADR 0022 keyspace.
pub const KIND_BASE: u8 = 0x00;
/// Row-kind scope selector: local-secondary-index rows (ADR 0041 §2).
pub const KIND_LSI: u8 = 0x01;
/// Row-kind scope selector: change-log records (ADR 0041 §4/§4a).
pub const KIND_CHANGE: u8 = 0x02;
/// Row-kind scope selector: GSI footprints (ADR 0041 §4).
pub const KIND_FOOTPRINT: u8 = 0x03;
/// Row-kind scope selector: per-consumer cursor rows (ADR 0042/0043 — see
/// [`cursor`]'s module doc). Lives on a **base** tablet (a streamed/GSI'd
/// table's own tablets), one row per `(consumer tag, this tablet's own
/// lineage)`, holding a packed-HLC watermark.
pub const KIND_CURSOR: u8 = 0x04;

/// Every row-kind scope a tablet group owns, in selector order (ADR 0041 §3,
/// extended ADR 0042/0043).
///
/// The single place the set is enumerated: a group derives one sibling
/// [`StorageScope`] per entry at start, the snapshot image iterates it, and
/// drop-table GC erases each in turn. Adding a kind here is what makes it
/// exist everywhere at once.
pub const ALL_KINDS: [u8; 5] = [
    KIND_BASE,
    KIND_LSI,
    KIND_CHANGE,
    KIND_FOOTPRINT,
    KIND_CURSOR,
];

/// The sibling scope set a tablet group owns, derived from its **parent**
/// scope (`escape(table)` + this tablet's range), indexed by kind selector.
///
/// Every entry shares the parent's one live `KeyRange`
/// ([`StorageScope::with_kind`]), so narrowing any of them narrows all.
fn kind_scopes(parent: &StorageScope) -> [StorageScope; ALL_KINDS.len()] {
    ALL_KINDS.map(|kind| parent.with_kind(kind))
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

    /// A **sibling scope of the same tablet group**, holding a different row
    /// kind (ADR 0041 §3): the prefix extended by `kind`, over **the very same
    /// live `KeyRange`** — literally the same `Arc`, so one
    /// [`narrow`](Self::narrow) moves every kind at once and a split or merge
    /// can never leave two kinds disagreeing about what this tablet owns.
    ///
    /// Every kind of one tablet is `prefix || [kind]`, so two kinds differ in
    /// their final byte at equal length and neither prefixes the other; two
    /// *tables* are already separated one level up by `escape`'s own
    /// prefix-freedom. That is what lets the kinds share an engine without a
    /// discriminator inside the logical key — which they must, because
    /// [`RaftKvNode::txn_stage`] asserts a logical key leads with the ADR 0022
    /// partition token and derives every transaction intent span from it.
    #[must_use]
    pub fn with_kind(&self, kind: u8) -> Self {
        let mut prefix = self.prefix.clone();
        prefix.push(kind);
        Self {
            prefix,
            range: Arc::clone(&self.range),
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
        // ADR 0018 §2/PR3: a txn-record marker key (`txn::is_record_key`)
        // is internal bookkeeping, not user data — a tablet holding only a
        // record (an in-flight transaction, no other writes ever landed)
        // must still read as "no data" for the reforming-vs-fresh-join
        // decision this presence check exists for.
        match &range.end {
            Some(end) => {
                let physical_start = self.physical(&range.start);
                let physical_end = self.physical(end);
                storage
                    .scan(&physical_start, &physical_end)
                    .await
                    .map(|rows| {
                        rows.iter().any(|(k, _)| {
                            self.strip_in_range(k)
                                .is_some_and(|logical| !txn::is_record_key(logical))
                        })
                    })
                    .unwrap_or(false)
            }
            // Open-ended range: no finite physical upper bound to scan, so
            // fall back to the same whole-engine-then-filter shape `engine_image`
            // already uses for the unbounded case.
            None => storage
                .entries()
                .await
                .map(|rows| {
                    rows.iter().any(|(k, _)| {
                        self.strip_in_range(k)
                            .is_some_and(|logical| !txn::is_record_key(logical))
                    })
                })
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
    /// **Multi-kind atomic batch** (ADR 0041 §3/§4): like [`Batch`](Self::Batch),
    /// but every write names the **row kind** whose scope it lands in, and may
    /// be a tombstone (`None`) as well as a put.
    ///
    /// This is the primitive secondary-index maintenance rests on. A single
    /// entry writes the base row, its LSI rows, the partition's GSI footprint
    /// and a change-log record — and, critically, *deletes the stale LSI rows
    /// the overwrite invalidated* — as one Raft log entry: one propose, one
    /// commit round, one apply. An LSI is strongly consistent precisely because
    /// its rows commit in the same entry as the base row they derive from, and
    /// a change record can never be lost relative to the write it describes.
    ///
    /// Every kind of one tablet shares that tablet's single `KeyRange`, so one
    /// `fence` gates them all; as with `Batch` it gates the **whole** entry, or
    /// partial application would break exactly the atomicity this exists for.
    /// Keys are logical (token-leading, ADR 0022) — the kind selects the scope,
    /// it is never part of the key.
    KindBatch {
        /// `(row kind, logical key, value)` — `None` writes a tombstone.
        writes: Vec<(u8, Vec<u8>, Option<Vec<u8>>)>,
        /// An optional **change-log record** to append in the same entry:
        /// `(key prefix, encoded record)`.
        ///
        /// Its key is completed at **apply** as `prefix || hlc::pack(ts)`, using
        /// this entry's own commit timestamp, and it lands in the
        /// [`KIND_CHANGE`] scope. The proposer deliberately cannot supply that
        /// suffix: `ts` is minted inside `propose_ordered` and is the only
        /// timestamp that agrees with the entry's commit order, so letting an
        /// edge guess it would silently break the ordering the log exists to
        /// provide (ADR 0041 §4a — DynamoDB Streams reads these in commit
        /// order). Making it structural also means the record can never be
        /// keyed inconsistently across replicas.
        change_log: Option<(Vec<u8>, Vec<u8>)>,
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
    /// **Transaction stage** (ADR 0018 §2, PR3/PR4 — see `txn.rs`'s module
    /// doc): merge every entry of `writes` as an `Envelope::Intent`
    /// (carrying `record_table` alongside `record_key`, ADR 0018 §2/PR4, so
    /// a reader that can't resolve the intent locally knows which table's
    /// tablet to route a status query to) and, **iff `is_anchor`**,
    /// additionally create/refresh `txn_id`'s `Pending` record at
    /// `record_key` itself. `fence` gates the **whole** stage — every write
    /// key, plus `record_key` when `is_anchor` — atomically: any single key
    /// falling outside it (or an already-sealed range) makes the entire
    /// entry a no-op, matching `Batch`'s all-or-nothing semantics (a
    /// partial stage would let a reader observe some of a transaction's
    /// intents but not others).
    ///
    /// **A target key already holding an unresolved intent from a
    /// *different* transaction also makes the whole entry a no-op (ADR
    /// 0018 §2/PR6, task #16's fix)** — writers **push** intents (CRDB's
    /// term for this), never silently overwrite one. This closes a real
    /// durability hole a corpus depth run found: overwriting a
    /// still-undecided-at-the-time intent doesn't erase it (MVCC keeps it
    /// as an older version), and if the *overwriting* transaction later
    /// aborts, its restore does a **one-hop-back** `get_at` that can land
    /// on that stale intent instead of a genuinely committed value or true
    /// absence — a chain a later correct resolve can never repair (its
    /// commit's own ts is lower than the wrong restore's, so LWW loses).
    /// Chasing the version chain back multiple hops to fix this on the
    /// *read* side was considered and rejected: an intermediate hop
    /// skipped over could belong to a transaction that *later* commits,
    /// and that transaction's own eventual resolve-rewrite would then lose
    /// to the restore's higher ts the same unrepairable way — the
    /// corruption moves, it doesn't go away. Rejecting the overwrite at
    /// **apply time** instead makes the corrupt chain structurally
    /// unrepresentable: a target key can only ever hold at most one txn's
    /// unresolved intent at a time, so an abort-restore's one-hop lookback
    /// is now *always* sound (genuinely committed, or genuinely absent —
    /// never another live intent). A plain `Put`/`Batch`/`Cas` over a
    /// foreign intent is **not** rejected (analyzed safe — see `txn.rs`'s
    /// module doc for the argument: it's a genuine overwrite serialized
    /// strictly after the intent's own transaction, so that transaction's
    /// eventual resolve-rewrite correctly loses to it via ordinary LWW,
    /// and a snapshot read below the overwrite's ts still correctly
    /// resolves the buried intent). Same-txn re-staging (a WAL-replay
    /// re-application, or an ordinary duplicate stage) is not blocked —
    /// matched by `txn_id` equality, never by mere presence of *an*
    /// intent. The rejected caller (the coordinator, or a recovery pusher)
    /// must push the blocking transaction (`txn_record_view`/`txn_recover`
    /// on it, using the blocker's own `txn_id` this no-op makes newly
    /// visible — the routing info is already carried by
    /// `IntentInfo`/`FastRead::Foreign` for exactly this purpose) and
    /// retry, bounded, before giving up — mirroring what a *read* already
    /// does against a foreign pending intent.
    ///
    /// **`is_anchor: false`** (ADR 0018 §2/PR4 — a non-anchor participant's
    /// own stage in a multi-participant 2PC) merges intents only:
    /// `record_key`/`record_table` name the **anchor's** record, which
    /// lives in a different tablet's (indeed possibly a different table's)
    /// keyspace entirely and is never checked against this group's own
    /// `fence` or written here — only `writes`' own keys are. `spans` is
    /// unused in this case (it only ever feeds a locally-created
    /// `TxnRecord`, which a non-anchor stage never creates).
    ///
    /// **`is_anchor: true`** (the single-participant degenerate case, PR3,
    /// and a multi-participant transaction's anchor tablet, PR4) is
    /// byte-for-byte PR3's original behavior: `record_key` must itself
    /// fall inside `fence` (it is this tablet's own reserved key), and a
    /// fresh `Pending` `TxnRecord` is created there alongside the intents.
    ///
    /// **`spans` (ADR 0018 §2/PR5)**: `(table, span)` pairs for **every**
    /// key this transaction stages anywhere — every participant's writes,
    /// the anchor's own included — not just this stage's own `writes`. Only
    /// meaningful (and only ever stored, into the freshly-created
    /// `TxnRecord::intent_spans`) when `is_anchor`; a non-anchor stage
    /// passes an empty `Vec` (it never creates a record). This is what lets
    /// PR5's recovery push learn which *other* tablets/tables a
    /// transaction touched from the anchor's record alone — closing a real
    /// gap PR3/PR4 left open (see `txn::TxnRecord::intent_spans`'s doc for
    /// the full account).
    ///
    /// **`conditions` (ADR 0018 §2 apply-time write-key conditions
    /// amendment)**: `(key, expected)` pairs — `expected: Some(bytes)` means
    /// `key`'s current *committed* value (envelope-unwrapped, the same
    /// read discipline `Cas` uses) must equal `bytes` exactly; `None` means
    /// it must be absent. Deliberately **byte-level OCC, not a rich
    /// expression** — this crate speaks bytes; a caller (the Dynamo edge)
    /// evaluates its own richer `ConditionExpression` against a pre-read and
    /// compiles the result to "the value must still be exactly what I read"
    /// before it ever reaches here. Every `key` here is expected to also be
    /// one of `writes`' own keys (an **own-key** condition on a value this
    /// same stage is about to write) — a condition on a key this
    /// transaction does *not* write has no self-referential-stall problem
    /// to solve and belongs in the ordinary cross-key `cp_txn` precondition
    /// mechanism instead (re-read once before staging, once before the
    /// commit decision — see `animusd::ClientCtx::cp_txn`'s doc). **Any**
    /// condition failing no-ops the *whole* stage, composing with the
    /// existing fence/seal/foreign-intent whole-or-nothing behavior — see
    /// this variant's apply arm and [`StageOutcome`] for how a caller learns
    /// *which* reason a stage no-op'd for.
    TxnStage {
        txn_id: TxnId,
        record_key: Vec<u8>,
        record_table: String,
        is_anchor: bool,
        writes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        spans: Vec<(String, KeyRange)>,
        conditions: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        fence: KeyRange,
        ts: HlcTimestamp,
    },
    /// **Transaction commit**: flip `txn_id`'s record at `record_key` to
    /// `Committed { commit_ts: ts }` iff currently `Pending` (a re-apply at
    /// the identical `ts` is an idempotent no-op; a *different* `ts` on an
    /// already-committed record, or committing an already-aborted one, is
    /// a protocol-bug hard assert — see `apply_and_compact`'s arm). No
    /// `fence`, deliberately, like `Seal`/`ReadCeiling`: a 2PC decision
    /// must be durable and final regardless of any later range change, and
    /// this only ever touches the record key itself, never user data. A
    /// missing record (the stage never landed — fenced/sealed out) is a
    /// silent no-op, matching this crate's fence-miss doctrine.
    TxnCommit {
        txn_id: TxnId,
        record_key: Vec<u8>,
        ts: HlcTimestamp,
    },
    /// **Transaction abort**: the `Aborted` dual of `TxnCommit` — see its
    /// doc for the fence/idempotency/protocol-bug-assert argument, which
    /// applies identically here.
    ///
    /// **`orphan_created_ts` (ADR 0018 §2/PR5's orphan-record fix)**: `None`
    /// is the ordinary case above (a missing record is a silent fence-miss
    /// no-op). `Some(created_ts)` is a **recovery pusher that found no
    /// record at all** — a real, already-acknowledged possibility (the
    /// anchor's own `TxnStage` can silently no-op at apply on a fence/seal
    /// miss, exactly like a participant's stage already could, PR4, even
    /// though the coordinator went on to stage participants successfully).
    /// In that case apply **synthesizes** a fresh `TxnRecord` directly in
    /// the `Aborted` state (a CRDB-style "abort tombstone") using
    /// `created_ts` as its `created_ts` field (the pusher's only
    /// trustworthy substitute — see `IntentInfo::version`'s doc) and empty
    /// `intent_spans` (unknown — a documented residual, see
    /// `apply_and_compact`'s arm). This is always safe: it can never
    /// resurrect or clobber a record that already exists (that path is
    /// unchanged, `Some`/`None` alike), and a late-arriving genuine anchor
    /// `TxnStage` for the same `txn_id` finds this tombstone and no-ops
    /// instead of overwriting it back to `Pending` (`KvCommand::TxnStage`'s
    /// own resurrection guard).
    TxnAbort {
        txn_id: TxnId,
        record_key: Vec<u8>,
        ts: HlcTimestamp,
        orphan_created_ts: Option<HlcTimestamp>,
    },
    /// **Transaction resolve**: for each key in `keys` still holding
    /// `txn_id`'s intent, rewrite it to its final form per `outcome` —
    /// committed: the staged value (or a tombstone, for a staged delete)
    /// at `ts`; aborted: the value this key held immediately before the
    /// intent, restored forward at `ts` (never a tombstone, which would
    /// incorrectly shadow that older value — see `txn.rs`'s module doc). A
    /// key whose stored value is no longer that exact intent (already
    /// resolved, or overwritten by something newer) is left untouched —
    /// idempotent on WAL replay. No `fence`: every key here was already
    /// fence-checked at `TxnStage` time; resolve only ever converts an
    /// already-staged intent, never introduces new data outside that
    /// prior fence.
    ///
    /// **`outcome` is carried explicitly (ADR 0018 §2/PR4), not
    /// re-derived by reading `record_key` locally** (PR3's original
    /// shape): a non-anchor participant's own `keys` never share this
    /// tablet's scope with the record (see `KvCommand::TxnStage`'s
    /// `is_anchor` doc) — the record lives on the anchor's tablet only, so
    /// this group has no local copy to read at all. The coordinator (or,
    /// for the single-participant degenerate case, this same group acting
    /// as its own coordinator) already knows the decision by the time it
    /// proposes a resolve — it just committed/aborted the record itself,
    /// or learned the outcome from whoever did — so threading it through
    /// here removes the local-record dependency entirely, uniformly for
    /// every participant including the anchor.
    TxnResolve {
        txn_id: TxnId,
        record_key: Vec<u8>,
        keys: Vec<Vec<u8>>,
        outcome: txn::TxnOutcome,
        ts: HlcTimestamp,
    },
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

/// How long a point read ([`RaftKvNode::read_resolved`]) retries a still-
/// `Pending` intent before giving up and reporting "not served" (ADR 0018
/// §2/PR3). Full push/wait scheduling for an in-flight transaction is PR4;
/// this bounded poll is the interim contract.
const INTENT_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
/// Poll granularity while a read waits for a `Pending` intent to resolve.
const INTENT_WAIT_POLL: Duration = Duration::from_millis(20);

/// **Recovery grace period** (ADR 0018 §2/PR5): how long a transaction
/// record may sit `Pending` before any actor holding a foreign-or-local
/// pending intent may push it to a decision — the CockroachDB "no blocking
/// on a dead coordinator" property the Decision section's Recovery bullet
/// promises. Compared against the record's own `created_ts.wall_ms` (an
/// HLC, ADR 0003 — never a raw wall-clock `Instant`), since the pusher may
/// be a different node than the one that minted the record.
///
/// **Liveness-only tuning — correctness never depends on this value** (ADR
/// 0017 §3's discipline, restated for recovery in the decision-semantics
/// amendment): a push may fire sooner or later than this and the outcome is
/// still safe, because a recovery decision is never trusted merely for
/// having been proposed — the anchor's own Raft log position is the sole
/// arbiter of which decision (if more than one is ever proposed) actually
/// wins (see `apply_and_compact`'s `TxnCommit`/`TxnAbort` arms). Grace only
/// affects *when* recovery may act, never *what* it decides.
pub const RECOVERY_GRACE: Duration = Duration::from_secs(5);

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

/// Per-`TxnStage` outcomes recorded at apply time, keyed by the entry's
/// **Raft log index** — the [`StageOutcome`] introspection primitive (ADR
/// 0018 §2 apply-time write-key conditions amendment), mirroring
/// [`CasResults`] exactly: every replica records the identical value (the
/// stage is decided deterministically in commit order against the same
/// committed engine state), and a proposer polls until its entry applies,
/// then reads its index here (see
/// [`RaftKvNode::stage_outcome`]/[`RaftKvNode::txn_stage_anchor`]/
/// [`RaftKvNode::txn_stage_participant`]).
#[derive(Default)]
struct StageOutcomes {
    outcomes: BTreeMap<u64, StageOutcome>,
}

/// This group's own in-memory index of the transaction records it holds
/// (ADR 0018 §2/PR5) — populated **only** on a group that anchors at least
/// one transaction (a non-anchor `TxnStage` never creates a record, so a
/// pure-participant group's tracker stays empty). Drives `animusd`'s
/// `txn_resolver_loop`: which records to push past their grace period, and
/// which decided records still owe a resolve fan-out.
///
/// **Rebuilt at group start** (`rebuild_txn_tracker`) via one bounded scope
/// scan for `txn::is_record_key` markers — deliberately not derived from
/// log replay (the same reasoning `sealed`/`committed_ceiling` already
/// document: compaction can truncate the `TxnStage`/`TxnCommit` entries out
/// of the log long before the record's own lifecycle is done, so only the
/// engine-durable record itself is a complete source across a restart) —
/// then kept current by `apply_and_compact` as it processes the live log
/// tail.
#[derive(Default)]
struct TxnTracker {
    /// `txn_id -> (record_key, created_ts)` for every record this group
    /// currently holds `Pending`. Inserted when a `TxnStage` with
    /// `is_anchor: true` (first) creates the record; removed the moment
    /// this group's own apply flips it `Pending -> Committed`/`Aborted` (a
    /// losing, conflicting decision that arrives afterward is a logged
    /// no-op — see `apply_and_compact`'s `TxnCommit`/`TxnAbort` arms — and
    /// touches neither map, since the winning decision already did).
    pending: BTreeMap<TxnId, (Vec<u8>, HlcTimestamp)>,
    /// `txn_id -> (record_key, outcome)` for every record this group has
    /// decided but has not yet seen **any** matching `TxnResolve` apply
    /// here. Inserted on the `Pending -> Committed`/`Aborted` transition;
    /// removed as soon as a `TxnResolve` for this `txn_id` applies on this
    /// same group.
    ///
    /// **Deliberately approximate, documented, and still safe**: this
    /// group can only observe resolves that apply on *itself* — for a
    /// multi-participant transaction, every other participant's own
    /// `TxnResolve` applies on a *different* tablet's group entirely, with
    /// no ack back to the anchor. So this entry tracks "has the anchor
    /// group's own local resolve happened" (which fires whenever *any*
    /// resolve for this `txn_id` lands here — its own keys' resolve, or a
    /// resolver-loop retry that happens to hit this group first), not "have
    /// every participant's intents actually been rewritten." A resolver
    /// that stops tracking a transaction slightly early never loses
    /// correctness — a straggling unresolved remote intent is still
    /// resolved on demand the moment any reader hits it (the foreign-intent
    /// read-path push, ADR 0018 §2/PR5 §3) — only background promptness is
    /// (very slightly) weaker in that residual case.
    unresolved_decided: BTreeMap<TxnId, (Vec<u8>, txn::TxnOutcome)>,
}

/// Rebuild a [`TxnTracker`] from `storage`'s own durable records within
/// `scope` (ADR 0018 §2/PR5) — see the type's doc for why this, not log
/// replay, is the recovery source. Mirrors `StorageScope::has_data`'s
/// scoped-scan-with-unbounded-fallback shape (a bounded scan when the live
/// range has a finite end, else the same whole-engine-then-filter fallback
/// `engine_image`/`has_data` already use for an unbounded range).
async fn rebuild_txn_tracker<S: StorageEngine>(storage: &S, scope: &StorageScope) -> TxnTracker {
    let range = scope.range();
    let rows: Vec<(Vec<u8>, animus_storage::VersionedValue)> = match &range.end {
        Some(end) => {
            let physical_start = scope.physical(&range.start);
            let physical_end = scope.physical(end);
            storage
                .scan(&physical_start, &physical_end)
                .await
                .unwrap_or_default()
        }
        None => storage.entries().await.unwrap_or_default(),
    };
    let mut tracker = TxnTracker::default();
    for (physical_key, vv) in rows {
        let Some(logical) = scope.strip_in_range(&physical_key) else {
            continue;
        };
        if !txn::is_record_key(logical) {
            continue;
        }
        let Some(record) = txn::decode_record(&vv.value) else {
            continue;
        };
        match record.status {
            txn::TxnStatus::Pending => {
                tracker
                    .pending
                    .insert(record.txn_id, (logical.to_vec(), record.created_ts));
            }
            txn::TxnStatus::Committed { commit_ts } => {
                tracker.unresolved_decided.insert(
                    record.txn_id,
                    (logical.to_vec(), txn::TxnOutcome::Committed { commit_ts }),
                );
            }
            txn::TxnStatus::Aborted => {
                tracker
                    .unresolved_decided
                    .insert(record.txn_id, (logical.to_vec(), txn::TxnOutcome::Aborted));
            }
        }
    }
    tracker
}

/// The outcome of resolving one raw, envelope-tagged stored value against a
/// read (ADR 0018 §2/PR3, extended PR4): either a value has been determined
/// (`Value`, `Some` present / `None` absent); the covering transaction's
/// **local** record is `Pending` (this same tablet holds the record — the
/// single-participant/anchor case — but it hasn't decided yet); or the
/// record could not be found in this tablet's own scope at all (`Foreign`
/// — either it genuinely lives on another tablet, ADR 0018 §2/PR4's
/// multi-participant case, or the anchor's own stage just hasn't applied
/// here yet) — see `RaftKvNode::resolve_once_step`'s doc for the exact
/// per-status rules. A caller with no cross-tablet resolver treats `Pending`
/// and `Foreign` identically (both are "can't resolve locally, retry");
/// [`linearizable_get_served_fast`](RaftKvNode::linearizable_get_served_fast)
/// is the one caller that acts on `Foreign` differently.
enum ResolveStep {
    Value(Option<Vec<u8>>),
    Pending,
    Foreign(IntentInfo),
}

/// Everything a caller needs to chase down an intent's covering transaction
/// on another tablet (ADR 0018 §2/PR4): the transaction's identity, its
/// record's logical key, and the **table** whose tablet ring owns that key
/// (a record key alone doesn't identify a table — see `txn::Envelope::Intent`'s
/// doc). Returned by [`RaftKvNode::peek_intent`]/exposed via
/// [`ResolveStep::Foreign`]; consumed by a coordinator (`animusd`) that
/// routes a `ClientRequest::TxnStatus` to `record_table`/`record_key`'s
/// owning tablet, then calls
/// [`RaftKvNode::resolve_intent_given_status`](RaftKvNode::resolve_intent_given_status)
/// with the reply.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IntentInfo {
    pub txn_id: TxnId,
    pub record_key: Vec<u8>,
    pub record_table: String,
    pub staged_value: Option<Vec<u8>>,
    /// **ADR 0018 §2/PR5**: the intent's own applied HLC timestamp
    /// (unpacked from its engine version — `assert_ts_monotonic`'s own
    /// invariant guarantees this is a real, meaningful point in this
    /// group's causal history). A recovery pusher whose `TxnStatus`/
    /// `TxnRecordView` query finds **no record at all** (a real,
    /// already-acknowledged possibility — the anchor's own stage can
    /// silently no-op at apply time on a fence/seal miss, exactly like a
    /// participant's stage already could, PR4) has no `created_ts` to
    /// grace-gate against; this is the documented substitute, since it's
    /// the only trustworthy timestamp anyone still holds for this
    /// transaction in that case. See `ClientCtx::txn_recover`'s doc.
    pub version: HlcTimestamp,
}

/// A recovery pusher's view of a transaction record (ADR 0018 §2/PR5) — the
/// public mirror of `txn::TxnRecord` a cross-tablet caller reads back via
/// [`RaftKvNode::txn_record_view`], carrying everything
/// `animusd::ClientCtx::txn_recover` needs to drive the push protocol
/// without this crate exposing its internal record representation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxnRecordView {
    pub status: TxnDecisionStatus,
    /// Every key this transaction staged anywhere, as `(table, span)` pairs
    /// — see `txn::TxnRecord::intent_spans`'s doc.
    pub intent_spans: Vec<(String, KeyRange)>,
    pub created_ts: HlcTimestamp,
}

/// The outcome of a **non-blocking, single-attempt** linearizable read (ADR
/// 0018 §2/PR4) — see [`RaftKvNode::linearizable_get_served_fast`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FastRead {
    /// The value is already resolved (present, or genuinely absent).
    Value(Option<Vec<u8>>),
    /// A **local** record covers this key and is still `Pending` — the
    /// caller may fall back to the bounded local wait
    /// ([`RaftKvNode::linearizable_get_served`]) or retry later.
    Pending,
    /// The intent's record could not be found in this tablet's own scope —
    /// see [`IntentInfo`]'s doc for how a caller resolves it.
    Foreign(IntentInfo),
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
    /// Per-`TxnStage` apply-time outcomes (ADR 0018 §2 apply-time write-key
    /// conditions amendment) — see [`StageOutcomes`]'s doc.
    stage: Arc<Mutex<StageOutcomes>>,
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
    ///
    /// Bound to the **base row kind** (ADR 0041 §3), so every read, write,
    /// fence, txn record and byte estimate that has always used `scope` still
    /// addresses exactly the base data. Other kinds go through
    /// [`kind_scopes`](Self::kind_scopes).
    scope: StorageScope,
    /// Every row kind's scope, indexed by selector (ADR 0041 §3) — the base
    /// entry is the same scope as [`scope`](Self::scope). All share one live
    /// `KeyRange`, so a split narrows every kind together. Iterated wherever an
    /// operation is about the *whole tablet* rather than one kind: the snapshot
    /// image and drop-table GC's erase.
    kind_scopes: [StorageScope; ALL_KINDS.len()],
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
    /// The `ts` of the **last command this leader has itself proposed**
    /// (appended to its own Raft log), packed via [`hlc::pack`] — **not**
    /// `committed_ceiling`, which only reflects what has been *applied*.
    /// Every ts-producing path (`mint_pushed`, `next_ceiling_candidate`, and
    /// [`propose_seal`](Self::propose_seal)'s bare mint) must additionally
    /// exceed this floor, and [`propose_ordered`](Self::propose_ordered)
    /// advances it, all inside the same held `core` lock — the fix for a
    /// real regression: a write's own `mint_pushed` floor check only
    /// consulted `ts_cache`/`committed_ceiling` (both applied-time state),
    /// so it could still mint below a `ReadCeiling` that had *already been
    /// appended to the log* (its artificially-`HLC_MAX_OFFSET`-ahead `ts`,
    /// via `Hlc::uncertainty_upper`) but not yet applied — the apply task
    /// lags the consensus loop by design (ADR 0017's driver-liveness
    /// split), so "already logged" and "already applied" are never the same
    /// instant. See `propose_ordered`'s doc and `docs/engineering-lessons.md`.
    last_proposed_ts: Arc<AtomicU64>,
    /// This group's transaction-record tracker (ADR 0018 §2/PR5) —
    /// `animusd`'s `txn_resolver_loop` reads it via
    /// [`pending_txns`](Self::pending_txns)/
    /// [`unresolved_decided`](Self::unresolved_decided). See [`TxnTracker`]'s
    /// doc for the exact insert/remove rules and the rebuild-at-start
    /// source.
    txn_tracker: Arc<Mutex<TxnTracker>>,
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
        // ADR 0041 §3: callers hand in the tablet's **parent** scope
        // (`escape(table)` + this tablet's range); the group owns one sibling
        // per row kind beneath it, all sharing the parent's single live
        // `KeyRange`. `self.scope` is deliberately bound to the *base* kind, so
        // every pre-existing call site — reads, writes, fences, txn records,
        // `approx_bytes` — keeps operating on exactly the data it always did,
        // with no edit. Binding `approx_bytes` to the base scope this way is
        // also the ADR 0034 fix: auto-split stops measuring change-log churn.
        let kind_scopes = kind_scopes(&scope);
        let scope = kind_scopes[KIND_BASE as usize].clone();
        let core = Arc::new(Mutex::new(RaftCore::new(
            env.node_id(),
            &all_nodes,
            env.now(),
            env.next_u64(),
        )));
        let reads = Arc::new(Mutex::new(ReadState::default()));
        let cas = Arc::new(Mutex::new(CasResults::default()));
        let stage = Arc::new(Mutex::new(StageOutcomes::default()));
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
        let last_proposed_ts = Arc::new(AtomicU64::new(0));
        // Rebuilt asynchronously inside `drive` (a scoped engine scan needs
        // `.await`, unlike every other piece of group-start state here) —
        // starts empty and is populated before the apply task's first pass,
        // mirroring `sealed`/`committed_ceiling`'s own rebuild-then-spawn
        // ordering.
        let txn_tracker = Arc::new(Mutex::new(TxnTracker::default()));
        let node = Self {
            env: env.clone(),
            core: Arc::clone(&core),
            storage: storage.clone(),
            reads: Arc::clone(&reads),
            cas: Arc::clone(&cas),
            stage: Arc::clone(&stage),
            engine_applied: Arc::clone(&engine_applied),
            halted: Arc::clone(&halted),
            stopped: Arc::clone(&stopped),
            apply_stopped: Arc::clone(&apply_stopped),
            propose_signal: Arc::clone(&propose_signal),
            metrics: metrics.clone(),
            scope: scope.clone(),
            kind_scopes: kind_scopes.clone(),
            stream,
            hlc: Arc::clone(&hlc),
            ts_cache: Arc::clone(&ts_cache),
            committed_ceiling: Arc::clone(&committed_ceiling),
            last_ceiling_candidate,
            last_proposed_ts,
            txn_tracker: Arc::clone(&txn_tracker),
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
            stage,
            engine_applied,
            wal_lock,
            halted,
            stopped,
            apply_stopped,
            propose_signal,
            metrics,
            scope,
            kind_scopes,
            stream,
            hlc,
            committed_ceiling,
            txn_tracker,
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

    /// Propose a command **built while holding the core lock**, so computing its
    /// `ts` (via [`mint_pushed`](Self::mint_pushed), [`next_ceiling_candidate`]
    /// (Self::next_ceiling_candidate), or a bare [`Hlc::mint`]) and appending it
    /// to the Raft log happen as one atomic step relative to every other
    /// proposer on this group.
    ///
    /// **Why this is load-bearing, not defensive:** every ts-minting path here
    /// only guarantees monotonicity *relative to whatever it observes at the
    /// moment it mints* — the log order that decides *apply* order (and hence
    /// what `assert_ts_monotonic` checks) was, before this method existed, a
    /// completely separate race: `ts = self.mint_pushed(..)` then, *as a second,
    /// unsynchronized step*, `self.propose_and_wake(command)` (the method this
    /// replaced). Two concurrent proposers on the same leader could mint ts=A
    /// then ts=B (A < B, correctly monotonic *as mints*) but race to actually
    /// call `core.propose(..)` in the *opposite* order — B's entry landing at
    /// a lower log index than A's — so apply would process ts=B then ts=A, a
    /// real decrease. This is exactly what tripped `assert_ts_monotonic` under
    /// `animusd`'s concurrent-client-load smoke test (`self_heal.rs`) once
    /// enough real parallelism (`ProdEnv`'s multi-threaded tokio runtime)
    /// widened the window between "mint" and "propose" enough to hit it. Since
    /// every proposal to one group already funnels through this same `core`
    /// mutex to get ordered *at all*, holding it across ts computation too adds
    /// no new bottleneck — it just closes the gap between the two steps that
    /// already needed to agree. See `docs/engineering-lessons.md`.
    ///
    /// Also advances [`last_proposed_ts`](Self::last_proposed_ts) to `command`'s
    /// own `ts` **iff the propose actually lands** (`Accepted`, still inside
    /// this same held lock) — the floor every ts-producing path
    /// (`mint_pushed`/`next_ceiling_candidate`/[`propose_seal`](Self::
    /// propose_seal)'s bare mint) must additionally exceed, closing the
    /// residual gap serializing propose order alone doesn't: an
    /// already-*proposed* (logged) entry's `ts` can still exceed
    /// `committed_ceiling`/`ts_cache` (both only reflect *applied* state —
    /// the apply task lags the consensus loop by design), so without this a
    /// later write could still mint below an as-yet-unapplied `ReadCeiling`
    /// this same leader just logged.
    ///
    /// Wakes the consensus loop on acceptance (wake-on-propose, ADR 0017), same
    /// as before. `build` must not itself try to lock `core` (deadlock — it is
    /// already held) — every current caller only touches `hlc`/`ts_cache`/
    /// `last_ceiling_candidate`/`last_proposed_ts`, none of which anything else
    /// locks `core` while holding.
    fn propose_ordered<F: FnOnce() -> KvCommand>(&self, build: F) -> ProposeResult {
        let mut core = self.lock();
        let command = build();
        let ts = command_ts(&command);
        let result = record_propose(&self.metrics, core.propose(command));
        if matches!(result, ProposeResult::Accepted { .. })
            && let Some(ts) = ts
        {
            self.last_proposed_ts.store(hlc::pack(ts), Ordering::SeqCst);
        }
        drop(core);
        if matches!(result, ProposeResult::Accepted { .. }) {
            self.propose_signal.notify();
        }
        result
    }

    /// As [`propose_ordered`](Self::propose_ordered), but `build` also hands
    /// back an arbitrary `aux` value computed in the same call (still
    /// inside the held `core` lock) — used by the txn propose methods
    /// (ADR 0018 §2/PR3), which need the `TxnId`/record key/ts they just
    /// minted for the *next* step of the flow (stage -> commit/abort ->
    /// resolve), not just the bare [`ProposeResult`]. `aux` is returned
    /// regardless of whether the propose was accepted — every caller here
    /// only trusts it once it has confirmed `Accepted`.
    fn propose_ordered_aux<T, F: FnOnce() -> (KvCommand, T)>(
        &self,
        build: F,
    ) -> (ProposeResult, T) {
        let mut core = self.lock();
        let (command, aux) = build();
        let ts = command_ts(&command);
        let result = record_propose(&self.metrics, core.propose(command));
        if matches!(result, ProposeResult::Accepted { .. })
            && let Some(ts) = ts
        {
            self.last_proposed_ts.store(hlc::pack(ts), Ordering::SeqCst);
        }
        drop(core);
        if matches!(result, ProposeResult::Accepted { .. }) {
            self.propose_signal.notify();
        }
        (result, aux)
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
    ///
    /// Also floored by [`last_proposed_ts`](Self::last_proposed_ts) — this
    /// leader's own last-*logged* `ts`, not just `committed_ceiling`'s
    /// applied one (see `propose_ordered`'s doc) — so a ceiling proposed
    /// right after a regular write this same leader just logged (but the
    /// apply task hasn't caught up to yet) still lands strictly above it.
    /// Folding it in here, rather than via `Hlc::witness`, keeps the
    /// no-witness invariant above intact: `last_proposed_ts` is a plain
    /// floor read, not a clock advance.
    fn next_ceiling_candidate(&self, margin: HlcTimestamp) -> HlcTimestamp {
        loop {
            let last_packed = self.last_ceiling_candidate.load(Ordering::SeqCst);
            // The candidate must strictly exceed **both** this ratchet's own
            // history (`last_ceiling_candidate` — the original regression
            // this loop already guarded against) **and** `last_proposed_ts`
            // (this leader's last-*logged* ts, `propose_ordered`'s doc) — not
            // merely equal either. Folding them into one `last` and reusing
            // the exact same bump-on-not-exceeding branch below for both is
            // deliberate: an earlier version of this fix took
            // `margin.max(last_proposed_ts)` as a `floor` and returned it
            // **unmodified** whenever it beat `last_ceiling_candidate` — but
            // unlike `margin` (always freshly `HLC_MAX_OFFSET` in the
            // future, so using it as-is was safe), `last_proposed_ts` is a
            // value some *other* command just used, so returning it
            // unmodified reproposed the exact same ts — a real regression a
            // `ProdEnv` concurrent-load test caught (see `docs/engineering-
            // lessons.md`). Only the ratchet's own bump branch is safe to
            // hand back verbatim; `margin` and `last_proposed_ts` both only
            // ever act as a floor to strictly exceed.
            let last = hlc::unpack(last_packed)
                .max(hlc::unpack(self.last_proposed_ts.load(Ordering::SeqCst)));
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
        let cache_floor = {
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
        // Also floored by this leader's own last-*logged* ts (`propose_
        // ordered`'s doc) — `committed_ceiling`/the cache above only reflect
        // *applied* state, which the apply task can lag the consensus loop
        // on by design, so a write proposed right after a `ReadCeiling` this
        // same leader already logged (but hasn't applied yet) must still
        // check against it here, not just the applied ceiling.
        let floor = cache_floor.max(hlc::unpack(self.last_proposed_ts.load(Ordering::SeqCst)));
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
        self.propose_ordered(|| {
            let ts = self.mint_pushed(std::slice::from_ref(&key));
            KvCommand::Put {
                key,
                value,
                fence,
                ts,
            }
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
        self.propose_ordered(|| {
            let keys: Vec<&[u8]> = puts.iter().map(|(k, _)| k.as_slice()).collect();
            let ts = self.mint_pushed(&keys);
            KvCommand::Batch { puts, fence, ts }
        })
    }

    /// Propose a **multi-kind atomic batch** (ADR 0041 §3/§4): commit writes
    /// spanning several of this tablet's row-kind scopes as **one** Raft log
    /// entry. A `None` value writes a tombstone, so one entry can add the new
    /// index rows *and* remove the stale ones an overwrite invalidated.
    ///
    /// The primitive secondary-index maintenance rests on: an LSI is strongly
    /// consistent because its rows commit in the same entry as the base row
    /// they derive from, and a change-log record can never be lost relative to
    /// the write it describes. Keys are **logical** and token-leading
    /// (ADR 0022); the kind selects the scope and is never part of the key.
    ///
    /// Stamps `fence = KeyRange::whole()`; use
    /// [`put_kind_batch_fenced`](Self::put_kind_batch_fenced) to stamp a
    /// narrower one.
    pub fn put_kind_batch(
        &self,
        writes: Vec<(u8, Vec<u8>, Option<Vec<u8>>)>,
        change_log: Option<(Vec<u8>, Vec<u8>)>,
    ) -> ProposeResult {
        self.put_kind_batch_fenced(writes, change_log, KeyRange::whole())
    }

    /// As [`put_kind_batch`](Self::put_kind_batch), but the leader stamps its
    /// own `fence` into the entry. If **any** key falls outside `fence`, none of
    /// the batch applies — the fence gates the whole atomic entry, since a
    /// half-applied index write is exactly what colocating the kinds exists to
    /// prevent.
    pub fn put_kind_batch_fenced(
        &self,
        writes: Vec<(u8, Vec<u8>, Option<Vec<u8>>)>,
        change_log: Option<(Vec<u8>, Vec<u8>)>,
        fence: KeyRange,
    ) -> ProposeResult {
        self.propose_ordered(|| {
            let keys: Vec<&[u8]> = writes.iter().map(|(_, k, _)| k.as_slice()).collect();
            let ts = self.mint_pushed(&keys);
            KvCommand::KindBatch {
                writes,
                change_log,
                fence,
                ts,
            }
        })
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
        self.propose_ordered(|| {
            let ts = self.mint_pushed(std::slice::from_ref(&key));
            KvCommand::Delete { key, fence, ts }
        })
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
        self.propose_ordered(|| {
            let ts = self.mint_pushed(std::slice::from_ref(&key));
            KvCommand::Cas {
                key,
                expected,
                value,
                fence,
                ts,
            }
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
        self.propose_ordered(|| {
            let ts = self.hlc.mint(self.env.now());
            // Same `last_proposed_ts` floor as `mint_pushed` (see
            // `propose_ordered`'s doc) — a seal is a mutating log entry like
            // any other and must not land below an as-yet-unapplied
            // `ReadCeiling` this leader already logged.
            let floor = hlc::unpack(self.last_proposed_ts.load(Ordering::SeqCst));
            let ts = if ts > floor {
                ts
            } else {
                let pushed = self.hlc.witness(floor, self.env.now());
                assert!(
                    pushed > floor,
                    "raftkv propose_seal: witnessing the last-proposed floor must strictly \
                     exceed it (floor={floor:?}, got={pushed:?}) — Hlc::witness's own contract \
                     is broken"
                );
                pushed
            };
            KvCommand::Seal { range, ts }
        })
    }

    /// **Anchor stage** phase of a 2PC (ADR 0018 §2/PR3, generalized to
    /// multi-participant in PR4 — see `txn.rs`'s module doc): mint a fresh
    /// [`TxnId`], compute its record key from `writes`' first (anchor)
    /// key's own partition token, and propose `KvCommand::TxnStage` (with
    /// `is_anchor: true`) for the whole batch as one atomic Raft entry.
    /// Leader-only (`None` otherwise, like every other propose-and-wait
    /// method here). Returns the `TxnId` + record key once the stage has
    /// committed *and applied* on this leader — feed both into
    /// [`txn_decide`](Self::txn_decide) for the single-participant case, or
    /// into [`txn_stage_participant`](Self::txn_stage_participant)/
    /// [`txn_commit_at_least`](Self::txn_commit_at_least)/
    /// [`txn_resolve`](Self::txn_resolve) for a multi-participant
    /// coordinator (`animusd`'s `cp_txn`), or use the one-shot
    /// [`txn_write`](Self::txn_write) for the single-tablet commit-only
    /// path.
    ///
    /// `table` is this tablet's own table name, embedded into every intent
    /// merged here (`record_table`) so any reader — on this tablet or
    /// another — always knows which table's tablet ring owns the record
    /// (ADR 0018 §2/PR4; a bare token doesn't identify a table, since two
    /// tables' rings can assign the same token to different rows).
    ///
    /// # Panics
    /// If `writes` is empty, or its anchor key is shorter than
    /// `animus_tablet::TOKEN_BYTES` — every real data-plane key
    /// unconditionally leads with the partition token (ADR 0022); this is
    /// a caller invariant, not a recoverable condition.
    pub async fn txn_stage(
        &self,
        table: &str,
        writes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    ) -> Option<(TxnId, Vec<u8>, StageOutcome)> {
        self.txn_stage_anchor(table, writes, Vec::new(), Vec::new())
            .await
    }

    /// As [`txn_stage`](Self::txn_stage), but for a genuine multi-participant
    /// coordinator (ADR 0018 §2/PR5): `participant_spans` names **every
    /// other** participant's `(table, span)` pairs (the coordinator already
    /// knows the full write set, grouped by tablet, before staging
    /// anything — see `animusd::ClientCtx::cp_txn`), merged with this
    /// stage's own anchor spans into the freshly-created record's
    /// `intent_spans` — the structural fix that lets recovery learn which
    /// other tablets/tables a transaction touched (see
    /// `txn::TxnRecord::intent_spans`'s doc). `txn_stage` itself is the
    /// single-participant convenience (`participant_spans: Vec::new()`,
    /// `conditions: Vec::new()`).
    ///
    /// **`conditions`** (ADR 0018 §2 apply-time write-key conditions
    /// amendment): own-key byte-level OCC preconditions checked at apply —
    /// see `KvCommand::TxnStage`'s doc. The returned [`StageOutcome`] is
    /// what tells the caller whether the stage actually landed, and if not,
    /// why (a final `ConditionFailed`, a retryable `IntentBlocked`, or a
    /// structural `Fenced`) — the `TxnId`/record key are always returned
    /// once the entry *applies* at all (mirroring the pre-existing contract:
    /// `None` here still means "not leader, or the entry never applied",
    /// never "the stage was rejected").
    ///
    /// # Panics
    /// If `writes` is empty, or its anchor key is shorter than
    /// `animus_tablet::TOKEN_BYTES` — every real data-plane key
    /// unconditionally leads with the partition token (ADR 0022); this is
    /// a caller invariant, not a recoverable condition.
    pub async fn txn_stage_anchor(
        &self,
        table: &str,
        writes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        participant_spans: Vec<(String, KeyRange)>,
        conditions: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    ) -> Option<(TxnId, Vec<u8>, StageOutcome)> {
        assert!(
            !writes.is_empty(),
            "raftkv txn_stage: writes must be non-empty"
        );
        let anchor = &writes[0].0;
        assert!(
            anchor.len() >= animus_tablet::TOKEN_BYTES,
            "raftkv txn_stage: anchor key must lead with the {}-byte partition token \
             (ADR 0022) — got {} bytes",
            animus_tablet::TOKEN_BYTES,
            anchor.len()
        );
        let token = anchor[..animus_tablet::TOKEN_BYTES].to_vec();
        let keys: Vec<Vec<u8>> = writes.iter().map(|(k, _)| k.clone()).collect();
        let fence = self.scope_range();
        let record_table = table.to_owned();
        let (result, (txn_id, record_key)) = self.propose_ordered_aux(|| {
            let ts = self.mint_pushed(&keys);
            let txn_id = TxnId {
                ts,
                node: self.env.node_id(),
            };
            let record_key = txn::record_key(&token, &txn_id);
            let mut spans: Vec<(String, KeyRange)> = keys
                .iter()
                .map(|k| {
                    (
                        record_table.clone(),
                        KeyRange::new(k.clone(), Some(txn::immediate_successor(k))),
                    )
                })
                .collect();
            spans.extend(participant_spans.clone());
            let cmd = KvCommand::TxnStage {
                txn_id: txn_id.clone(),
                record_key: record_key.clone(),
                record_table,
                is_anchor: true,
                writes: writes.clone(),
                spans,
                conditions,
                fence: fence.clone(),
                ts,
            };
            (cmd, (txn_id, record_key))
        });
        let index = match result {
            ProposeResult::Accepted { index } => index,
            ProposeResult::NotLeader { .. } => return None,
        };
        let outcome = self.wait_stage_outcome(index).await?;
        Some((txn_id, record_key, outcome))
    }

    /// **Participant stage** phase of a multi-participant 2PC (ADR 0018
    /// §2/PR4): stage `writes` as intents referencing an **already-known**
    /// anchor record (`txn_id`/`record_key`/`record_table`, as returned by
    /// the anchor's own [`txn_stage`](Self::txn_stage)) — proposes
    /// `KvCommand::TxnStage` with `is_anchor: false`, so no record is
    /// created or touched on *this* group; only `writes`' own keys are
    /// fenced/merged. Returns this participant's own stage timestamp (the
    /// coordinator witnesses it toward the eventual commit timestamp) once
    /// committed *and applied*; `None` if this node is not the leader or
    /// the stage times out.
    ///
    /// **`conditions`** (ADR 0018 §2 apply-time write-key conditions
    /// amendment): as [`txn_stage_anchor`](Self::txn_stage_anchor)'s own
    /// `conditions` — see `KvCommand::TxnStage`'s doc. The returned
    /// [`StageOutcome`] tells the caller whether this participant's stage
    /// actually landed, and if not, why.
    pub async fn txn_stage_participant(
        &self,
        txn_id: TxnId,
        record_key: Vec<u8>,
        record_table: String,
        writes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        conditions: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    ) -> Option<(HlcTimestamp, StageOutcome)> {
        assert!(
            !writes.is_empty(),
            "raftkv txn_stage_participant: writes must be non-empty"
        );
        let keys: Vec<Vec<u8>> = writes.iter().map(|(k, _)| k.clone()).collect();
        let fence = self.scope_range();
        let (result, ts) = self.propose_ordered_aux(|| {
            let ts = self.mint_pushed(&keys);
            let cmd = KvCommand::TxnStage {
                txn_id: txn_id.clone(),
                record_key: record_key.clone(),
                record_table: record_table.clone(),
                is_anchor: false,
                writes: writes.clone(),
                spans: Vec::new(), // unused: no local record is ever created here.
                conditions,
                fence: fence.clone(),
                ts,
            };
            (cmd, ts)
        });
        let index = match result {
            ProposeResult::Accepted { index } => index,
            ProposeResult::NotLeader { .. } => return None,
        };
        let outcome = self.wait_stage_outcome(index).await?;
        Some((ts, outcome))
    }

    /// Mint a ts that strictly exceeds `min_ts` **and** this group's own
    /// `last_proposed_ts` floor (mirrors [`mint_pushed`](Self::mint_pushed)/
    /// [`propose_seal`](Self::propose_seal)'s identical witness-and-floor
    /// shape) — the primitive [`txn_commit_at_least`](Self::txn_commit_at_least)
    /// uses to honor a coordinator-supplied commit timestamp candidate
    /// while still respecting this group's own log-order monotonicity
    /// invariant (ADR 0018 §2/PR4).
    fn mint_at_least(&self, min_ts: HlcTimestamp) -> HlcTimestamp {
        let ts = self.hlc.mint(self.env.now());
        let floor = min_ts.max(hlc::unpack(self.last_proposed_ts.load(Ordering::SeqCst)));
        if ts > floor {
            return ts;
        }
        let pushed = self.hlc.witness(floor, self.env.now());
        assert!(
            pushed > floor,
            "raftkv mint_at_least: witnessing the floor must strictly exceed it \
             (floor={floor:?}, got={pushed:?}) — Hlc::witness's own contract is broken"
        );
        pushed
    }

    /// **Attempt to commit** the anchor's record at (at least) `min_ts`
    /// (ADR 0018 §2/PR4 — the multi-participant coordinator's atomic commit
    /// point): proposes `KvCommand::TxnCommit` at a ts that strictly
    /// exceeds both `min_ts` (the coordinator's candidate — the max of
    /// every participant's acked stage ts, "pushed above" per the
    /// protocol) and this group's own log floor, via
    /// [`mint_at_least`](Self::mint_at_least). `None` if this node is not
    /// the leader or the proposal times out.
    ///
    /// # `Some(ts)` does **not** mean this call's own commit decided the
    /// record — only that its `TxnCommit` *entry applied*
    ///
    /// This is the single most important caveat on this method (ADR 0018
    /// §2/PR6, found live by the multi-tablet transaction corpus's
    /// `anchor_leader_kill_mid` scenario, where a coordinator's own
    /// `Some(ts)` was mistaken for "committed" and falsely ack'd a
    /// transaction a racing recovery push had already, correctly,
    /// aborted): since recovery makes a **second, independent decider**
    /// legal (ADR 0018 §2/PR5's decision-semantics amendment) and a
    /// same-outcome-different-ts duplicate commit is now a legal no-op
    /// rather than an assert (ADR 0018 §2/PR6), this call's own entry can
    /// apply as a **no-op** against a record some other decider already
    /// flipped — to `Committed` at a *different* ts, or to `Aborted`
    /// entirely — and `wait_applied` still reports success, because the
    /// *entry* genuinely applied; the *decision* just wasn't this call's.
    /// **Every caller must re-read the record's actual status
    /// ([`txn_status_local`](Self::txn_status_local)/
    /// [`txn_record_view`](Self::txn_record_view)) after this call,
    /// `Some` or `None`, and act on *that* — never treat a `Some(ts)`
    /// return as "committed," and never resolve any key using this
    /// method's own returned `ts` as the outcome's `commit_ts` (source it
    /// from the post-call status read instead, e.g.
    /// `TxnDecisionStatus::Committed { commit_ts }`) — see `animusd::
    /// ClientCtx::txn_decide_anchor` for the reference implementation of
    /// this discipline.**
    ///
    /// Unlike [`txn_decide`](Self::txn_decide), this does **not** also
    /// resolve any keys — a multi-participant coordinator resolves the
    /// anchor's own keys via a separate [`txn_resolve`](Self::txn_resolve)
    /// call, exactly like every other participant, once the actual
    /// decision (from the re-read, not this call's return value) is known.
    pub async fn txn_commit_at_least(
        &self,
        txn_id: TxnId,
        record_key: Vec<u8>,
        min_ts: HlcTimestamp,
    ) -> Option<HlcTimestamp> {
        let (result, ts) = self.propose_ordered_aux(|| {
            let ts = self.mint_at_least(min_ts);
            let cmd = KvCommand::TxnCommit {
                txn_id: txn_id.clone(),
                record_key: record_key.clone(),
                ts,
            };
            (cmd, ts)
        });
        let index = match result {
            ProposeResult::Accepted { index } => index,
            ProposeResult::NotLeader { .. } => return None,
        };
        self.wait_applied(index).await.then_some(ts)
    }

    /// **Abort** the anchor's record at `record_key` (ADR 0018 §2/PR5) — the
    /// `Abort`-only dual of [`txn_commit_at_least`](Self::txn_commit_at_least):
    /// proposes `KvCommand::TxnAbort` alone, **without** also resolving any
    /// keys (unlike [`txn_decide`](Self::txn_decide), which bundles
    /// abort+resolve for the single-participant convenience). A
    /// multi-participant caller (`animusd`'s `cp_txn`, or a recovery push)
    /// resolves every participant separately via
    /// [`txn_resolve`](Self::txn_resolve), exactly like the commit path
    /// already does. Returns the proposed abort ts once applied — **not**
    /// necessarily the record's actual final status: a concurrent decision
    /// (an in-flight coordinator, or a duelling recoverer) may have already
    /// committed the record first, in which case this abort applies as a
    /// logged no-op (see `apply_and_compact`'s `TxnAbort` arm and the
    /// decision-semantics amendment) — the caller must re-read the actual
    /// status (e.g. [`txn_status_local`](Self::txn_status_local)) to report
    /// honestly, never assume its own proposal won.
    pub async fn txn_abort(&self, txn_id: TxnId, record_key: Vec<u8>) -> Option<HlcTimestamp> {
        let (result, ts) = self.propose_ordered_aux(|| {
            let ts = self.mint_pushed(std::slice::from_ref(&record_key));
            let cmd = KvCommand::TxnAbort {
                txn_id: txn_id.clone(),
                record_key: record_key.clone(),
                ts,
                orphan_created_ts: None,
            };
            (cmd, ts)
        });
        let index = match result {
            ProposeResult::Accepted { index } => index,
            ProposeResult::NotLeader { .. } => return None,
        };
        self.wait_applied(index).await.then_some(ts)
    }

    /// **Abort an orphan intent with no record at all** (ADR 0018 §2/PR5,
    /// the corner PR5's own review found: PR4's prepare phase is
    /// concurrent, so a participant's own stage can succeed and be
    /// discovered by a reader while the *anchor's* `TxnStage` — which
    /// would have created this transaction's record — never lands here at
    /// all, e.g. a fence/seal miss the coordinator's propose outcome alone
    /// can't distinguish from a genuine stage, PR4's own documented gap,
    /// now applied to the anchor's own stage too). A recovery pusher whose
    /// `txn_record_view` query finds nothing calls this instead of
    /// [`txn_abort`](Self::txn_abort): proposes `KvCommand::TxnAbort` with
    /// `orphan_created_ts: Some(created_ts)`, which **synthesizes** a fresh
    /// `Aborted` record if (and only if) none exists yet — see
    /// `KvCommand::TxnAbort`'s doc for the full safety argument (it can
    /// never resurrect/clobber an existing record; that path is
    /// unconditionally unchanged). `created_ts` should be the pusher's
    /// best available substitute for a real `created_ts` — typically the
    /// orphaned intent's own applied timestamp
    /// ([`IntentInfo::version`](IntentInfo)), since no genuine record ever
    /// existed to read one from. Returns the proposed ts once applied
    /// (same "not necessarily the actual final status" caveat as
    /// [`txn_abort`](Self::txn_abort) — a concurrent decision, e.g. the
    /// coordinator's own late-but-successful commit, can still win; the
    /// caller must re-read).
    pub async fn txn_abort_orphan(
        &self,
        txn_id: TxnId,
        record_key: Vec<u8>,
        created_ts: HlcTimestamp,
    ) -> Option<HlcTimestamp> {
        let (result, ts) = self.propose_ordered_aux(|| {
            let ts = self.mint_pushed(std::slice::from_ref(&record_key));
            let cmd = KvCommand::TxnAbort {
                txn_id: txn_id.clone(),
                record_key: record_key.clone(),
                ts,
                orphan_created_ts: Some(created_ts),
            };
            (cmd, ts)
        });
        let index = match result {
            ProposeResult::Accepted { index } => index,
            ProposeResult::NotLeader { .. } => return None,
        };
        self.wait_applied(index).await.then_some(ts)
    }

    /// **Resolve** `keys` still holding `txn_id`'s intent on **this**
    /// group, per the already-decided `outcome` (ADR 0018 §2/PR4): the one
    /// low-level primitive both [`txn_decide`](Self::txn_decide) (the
    /// single-participant/anchor-local path) and a multi-participant
    /// coordinator (every participant, including the anchor's own keys)
    /// use — see `KvCommand::TxnResolve`'s doc for why `outcome` travels
    /// explicitly instead of being re-derived from a local record. Returns
    /// this group's own resolve ts (the MVCC version stamped on every
    /// resolved key here — **not** necessarily `outcome`'s `commit_ts`,
    /// which is only a comparison value, see the type's doc) once
    /// committed and applied; `None` if this node is not the leader or the
    /// resolve times out. A resolve failure does **not** undo the already-
    /// durable commit/abort decision — it just leaves the intent(s)
    /// unresolved for a later resolver to pick up (PR5's in-doubt
    /// recovery), or for a reader hitting the intent to resolve on demand
    /// (see [`resolve_intent_given_status`](Self::resolve_intent_given_status)).
    pub async fn txn_resolve(
        &self,
        txn_id: TxnId,
        record_key: Vec<u8>,
        keys: Vec<Vec<u8>>,
        outcome: txn::TxnOutcome,
    ) -> Option<HlcTimestamp> {
        let (result, ts) = self.propose_ordered_aux(|| {
            let ts = self.mint_pushed(&keys);
            let cmd = KvCommand::TxnResolve {
                txn_id: txn_id.clone(),
                record_key: record_key.clone(),
                keys: keys.clone(),
                outcome: outcome.clone(),
                ts,
            };
            (cmd, ts)
        });
        let index = match result {
            ProposeResult::Accepted { index } => index,
            ProposeResult::NotLeader { .. } => return None,
        };
        self.wait_applied(index).await.then_some(ts)
    }

    /// **Commit or abort** a previously-[staged](Self::txn_stage)
    /// single-participant transaction, then **resolve** its intents (ADR
    /// 0018 §2/PR3) — deliberately **three** separate log entries (stage
    /// was the first), fully synchronous end to end. `keys` must be the
    /// same keys `txn_stage` was called with. Returns the decision
    /// timestamp once every phase has committed and applied; `None` if
    /// this node stopped being the leader at any point.
    ///
    /// A resolve failure (not leader / timed out) does **not** undo the
    /// commit/abort decision itself, which is already durable — it just
    /// leaves the intent(s) unresolved for a later resolver to pick up
    /// (PR5's in-doubt recovery).
    ///
    /// **PR4 note**: this remains the single-participant convenience
    /// (`keys` must all be local to this same anchor group); a
    /// multi-participant coordinator instead drives
    /// [`txn_commit_at_least`](Self::txn_commit_at_least) (the anchor) and
    /// [`txn_resolve`](Self::txn_resolve) (every participant, anchor
    /// included) directly.
    pub async fn txn_decide(
        &self,
        txn_id: TxnId,
        record_key: Vec<u8>,
        keys: Vec<Vec<u8>>,
        commit: bool,
    ) -> Option<HlcTimestamp> {
        let (decide_result, decision_ts) = self.propose_ordered_aux(|| {
            let ts = self.mint_pushed(std::slice::from_ref(&record_key));
            let cmd = if commit {
                KvCommand::TxnCommit {
                    txn_id: txn_id.clone(),
                    record_key: record_key.clone(),
                    ts,
                }
            } else {
                KvCommand::TxnAbort {
                    txn_id: txn_id.clone(),
                    record_key: record_key.clone(),
                    ts,
                    orphan_created_ts: None,
                }
            };
            (cmd, ts)
        });
        let decide_index = match decide_result {
            ProposeResult::Accepted { index } => index,
            ProposeResult::NotLeader { .. } => return None,
        };
        if !self.wait_applied(decide_index).await {
            return None;
        }

        let outcome = if commit {
            txn::TxnOutcome::Committed {
                commit_ts: decision_ts,
            }
        } else {
            txn::TxnOutcome::Aborted
        };
        self.txn_resolve(txn_id, record_key, keys, outcome).await;

        Some(decision_ts)
    }

    /// One-shot **single-participant transaction**: [stage](Self::txn_stage)
    /// every `(key, Option<value>)` in `writes` as intents, commit, then
    /// [resolve](Self::txn_decide) — the degenerate 2PC through this
    /// **one** Raft group (ADR 0018 §2/PR3, Follow-up step 2). `writes`'
    /// first entry is the *anchor*: its partition token anchors the txn
    /// record (see `txn.rs`). `table` is this tablet's own table name (see
    /// [`txn_stage`](Self::txn_stage)'s doc). Returns the commit timestamp
    /// once every phase has committed and applied; `None` if this node
    /// stopped being the leader at any point.
    pub async fn txn_write(
        &self,
        table: &str,
        writes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    ) -> Option<HlcTimestamp> {
        let keys: Vec<Vec<u8>> = writes.iter().map(|(k, _)| k.clone()).collect();
        let (txn_id, record_key, outcome) = self.txn_stage(table, writes).await?;
        // ADR 0018 §2 apply-time write-key conditions amendment: a stage
        // that applied but didn't land (blocked by a foreign intent, or a
        // fence/seal miss) must not be followed by a commit — this used to
        // be a latent gap here (the same one `animusd::ClientCtx::
        // txn_prepare_pushing` was built to close on the coordinator path,
        // see `KvCommand::TxnStage`'s doc): `txn_stage`'s old `Option<(TxnId,
        // Vec<u8>)>` return only ever meant "the entry applied," never "my
        // writes actually staged."
        if outcome != StageOutcome::Staged {
            return None;
        }
        self.txn_decide(txn_id, record_key, keys, true).await
    }

    /// **Status query** for `txn_id`'s record at `record_key`, served
    /// straight off this replica's own scoped storage (ADR 0018 §2/PR4) —
    /// used both by a cross-tablet `ClientRequest::TxnStatus` handler
    /// (`animusd`) and directly by a caller that already knows it is
    /// talking to the record's own anchor tablet. Runs the same ReadIndex
    /// barrier as [`linearizable_get`](Self::linearizable_get) (only the
    /// confirmed leader serves this), so `None` covers both "not served"
    /// (barrier failed) and "no record found yet" (the stage hasn't
    /// committed/applied here yet — the caller retries).
    pub async fn txn_status_local(&self, record_key: &[u8]) -> Option<txn::TxnDecisionStatus> {
        if !self.read_barrier().await {
            return None;
        }
        let physical = self.scope.physical(record_key);
        let vv = self.storage.get(&physical).await.ok().flatten()?;
        let record = txn::decode_record(&vv.value)?;
        Some(record.status.to_public())
    }

    /// **Recovery view** of `txn_id`'s record at `record_key` (ADR 0018
    /// §2/PR5): like [`txn_status_local`](Self::txn_status_local), but also
    /// returns `intent_spans`/`created_ts` — everything a recovery pusher
    /// needs to verify every participant and, once decided, resolve every
    /// one of them (see [`TxnRecordView`]'s doc). Same ReadIndex barrier +
    /// `None` contract as `txn_status_local`.
    pub async fn txn_record_view(&self, record_key: &[u8]) -> Option<TxnRecordView> {
        if !self.read_barrier().await {
            return None;
        }
        let physical = self.scope.physical(record_key);
        let vv = self.storage.get(&physical).await.ok().flatten()?;
        let record = txn::decode_record(&vv.value)?;
        Some(TxnRecordView {
            status: record.status.to_public(),
            intent_spans: record.intent_spans,
            created_ts: record.created_ts,
        })
    }

    /// **Recovery primitive** (ADR 0018 §2/PR5): does this tablet currently
    /// hold a **live intent** for `txn_id` anywhere in `span`? A recovery
    /// pusher calls this on every participant named in a record's
    /// `intent_spans` (routed by `span.start`) before deciding whether to
    /// push the anchor's record to `Committed` (every span staged) or
    /// `Aborted` (any missing). Same ReadIndex barrier as
    /// [`linearizable_get`](Self::linearizable_get) — `None` = not served
    /// (barrier failed or this node isn't the leader), `Some(bool)` = this
    /// participant's own live answer.
    ///
    /// Deliberately reads the **raw** envelope via a direct scoped scan,
    /// not [`local_scan`](Self::local_scan)/`resolve_scan_rows` — those
    /// silently omit a still-`Pending` row (the right behavior for an
    /// ordinary client-facing scan, the wrong one here: we need to see
    /// "still staged" as a positive signal, not have it vanish). `span` is
    /// expected to be a tight, single-key point-span in practice (the shape
    /// `txn::immediate_successor` builds), so this is a small bounded scan,
    /// never a whole-tablet one — a bounded scoped scan over the span is an
    /// accepted cost here (ADR 0018 §2/PR5's own design note).
    pub async fn txn_verify_staged(&self, span: &KeyRange, txn_id: &TxnId) -> Option<bool> {
        if !self.read_barrier().await {
            return None;
        }
        let start = self.scope.physical(&span.start);
        let end = match &span.end {
            Some(e) => self.scope.physical(e),
            None => return Some(false), // an unbounded span can't be a point-span this crate ever built
        };
        let rows = self
            .storage
            .scan(&start, &end)
            .await
            .ok()
            .unwrap_or_default();
        Some(rows.iter().any(|(_, vv)| {
            matches!(
                txn::decode_envelope(&vv.value),
                txn::Envelope::Intent { txn_id: found, .. } if &found == txn_id
            )
        }))
    }

    /// This group's currently-tracked `Pending` records (ADR 0018 §2/PR5):
    /// `txn_id -> (record_key, created_ts)` — empty on a pure-participant
    /// group (only an anchor stage ever creates a record). `animusd`'s
    /// `txn_resolver_loop` walks this on every locally-led group to find
    /// records past [`RECOVERY_GRACE`] and push them via `txn_recover`. A
    /// cheap in-memory snapshot (no barrier, no I/O) — see [`TxnTracker`]'s
    /// doc for the insert/remove rules and the rebuild-at-start source.
    #[must_use]
    pub fn pending_txns(&self) -> BTreeMap<TxnId, (Vec<u8>, HlcTimestamp)> {
        self.txn_tracker
            .lock()
            .expect("txn tracker poisoned")
            .pending
            .clone()
    }

    /// This group's currently-tracked decided-but-not-yet-locally-resolved
    /// records (ADR 0018 §2/PR5): `txn_id -> (record_key, outcome)`.
    /// `animusd`'s `txn_resolver_loop` walks this on every locally-led
    /// (anchor) group and fans a `TxnResolve` out to every table named in
    /// the record's own `intent_spans`. See [`TxnTracker`]'s doc for why
    /// this is a deliberately approximate (but still safe) signal.
    #[must_use]
    pub fn unresolved_decided(&self) -> BTreeMap<TxnId, (Vec<u8>, txn::TxnOutcome)> {
        self.txn_tracker
            .lock()
            .expect("txn tracker poisoned")
            .unresolved_decided
            .clone()
    }

    /// Poll [`engine_applied_index`](Self::engine_applied_index) until it
    /// reaches `index` (the proposed entry has committed *and applied* on
    /// this leader), bounded by [`CAS_TIMEOUT`]/[`CAS_POLL`] — the same
    /// confirm-by-index shape [`compare_and_swap`](Self::compare_and_swap)
    /// uses. `false` if this node stops being the leader or the deadline
    /// passes first.
    async fn wait_applied(&self, index: u64) -> bool {
        let deadline = self.env.now().0 + CAS_TIMEOUT.as_nanos() as u64;
        loop {
            if self.engine_applied_index() >= index {
                return true;
            }
            if !self.is_leader() || self.env.now().0 >= deadline {
                return false;
            }
            self.env.sleep(CAS_POLL).await;
        }
    }

    /// Poll [`stage_outcome`](Self::stage_outcome) for `index` directly,
    /// bounded by [`CAS_TIMEOUT`]/[`CAS_POLL`] — mirrors
    /// [`compare_and_swap`](Self::compare_and_swap)'s own outcome-polling
    /// loop, **not** [`wait_applied`](Self::wait_applied) followed by a
    /// separate fetch.
    ///
    /// **This distinction is load-bearing, found by the ADR 0018 §4 corpus
    /// at depth (`ANIMUS_TXN_SEEDS=5`)**: `wait_applied`'s contract is
    /// "`engine_applied_index() >= index`," which a **snapshot install**
    /// can satisfy by jumping straight past `index` (a follower catching up
    /// after losing leadership, `apply_and_compact`'s `install_engine_image`
    /// branch) without this replica ever having individually processed —
    /// hence recorded a [`StageOutcome`] for — the entry at that exact
    /// index (an install/compaction globs many commands' effects into one
    /// engine image, discarding any way to report a per-entry outcome for
    /// anything the image already covers). So for every other command here,
    /// "applied" and "has a recorded outcome" coincide, but for `TxnStage`
    /// they do not: `wait_applied(index).await == true` does **not**
    /// guarantee `stage_outcome(index)` is `Some`. Polling the outcome
    /// directly (like CAS always has) makes this method's own `None` mean
    /// exactly what every other propose-and-wait method's `None` means —
    /// "give up, caller retries" — instead of ever hard-`expect`ing a fact
    /// that isn't actually guaranteed.
    async fn wait_stage_outcome(&self, index: u64) -> Option<StageOutcome> {
        let deadline = self.env.now().0 + CAS_TIMEOUT.as_nanos() as u64;
        loop {
            if let Some(outcome) = self.stage_outcome(index) {
                return Some(outcome);
            }
            if !self.is_leader() || self.env.now().0 >= deadline {
                return None;
            }
            self.env.sleep(CAS_POLL).await;
        }
    }

    /// The recorded outcome of the CAS committed at Raft log `index` (the value
    /// [`cas`](Self::cas) returned in [`ProposeResult::Accepted`]): `Some(true)`
    /// if the swap happened, `Some(false)` if `expected` did not match, or `None`
    /// if that index has not applied on this replica yet. Every replica records
    /// the identical outcome (the decision is deterministic in commit order).
    /// The recorded outcome of the `TxnStage` committed at Raft log `index`
    /// (ADR 0018 §2 apply-time write-key conditions amendment) — `None` if
    /// that index has not applied on this replica yet. Mirrors
    /// [`cas_result`](Self::cas_result) exactly; see [`StageOutcome`]'s doc
    /// for what each variant means to a caller.
    pub fn stage_outcome(&self, index: u64) -> Option<StageOutcome> {
        self.stage
            .lock()
            .expect("stage outcomes poisoned")
            .outcomes
            .get(&index)
            .cloned()
    }

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

    /// Resolve an intent whose covering transaction's **decided status is
    /// already known** (ADR 0018 §2/PR4) — the decision logic shared by
    /// [`resolve_once_step`](Self::resolve_once_step) (the local-record
    /// path) and [`resolve_intent_given_status`](Self::resolve_intent_given_status)
    /// (the cross-tablet, externally-supplied-status path): `Pending`
    /// can't resolve yet; `Committed` at or before `read_ts` (`None` =
    /// "latest") serves `staged_value`; `Committed` strictly after
    /// `read_ts`, or `Aborted`, serves whatever `physical_key` held
    /// immediately before this intent (rewinding to `vv_version - 1` —
    /// never a tombstone, which would incorrectly shadow an older,
    /// still-live committed value — see `txn.rs`'s module doc).
    async fn resolve_decided(
        &self,
        physical_key: &[u8],
        vv_version: u64,
        staged_value: Option<Vec<u8>>,
        read_ts: Option<HlcTimestamp>,
        status: &txn::TxnDecisionStatus,
    ) -> ResolveStep {
        match status {
            txn::TxnDecisionStatus::Committed { commit_ts }
                if read_ts.is_none_or(|rt| *commit_ts <= rt) =>
            {
                ResolveStep::Value(staged_value)
            }
            txn::TxnDecisionStatus::Committed { .. } | txn::TxnDecisionStatus::Aborted => {
                let prior = self
                    .storage
                    .get_at(physical_key, vv_version.saturating_sub(1))
                    .await
                    .ok()
                    .flatten();
                ResolveStep::Value(
                    prior.and_then(|pvv| match txn::decode_envelope(&pvv.value) {
                        txn::Envelope::Committed(v) => Some(v),
                        // A prior intent: **should be unreachable since ADR
                        // 0018 §2/PR6 (task #16)** — `KvCommand::TxnStage`'s
                        // apply-time writer-push-intents guard now rejects a
                        // stage over any key still holding another
                        // transaction's unresolved intent, so one hop back
                        // from *this* intent's own version can only ever be
                        // a genuinely committed value or true absence (see
                        // `KvCommand::TxnStage`'s doc for the durability
                        // argument this closes — a corpus depth run found a
                        // corrupted MVCC version chain that made an
                        // already-committed value permanently unreadable).
                        // Kept as a defensive fallback rather than an
                        // assert: this function has no way to distinguish
                        // "the invariant broke" from "an older, pre-fix WAL
                        // entry replayed on recovery" — conservatively
                        // treating it as absent (never leaking raw envelope
                        // bytes to a caller) is still correct either way.
                        txn::Envelope::Intent { .. } => None,
                    }),
                )
            }
            txn::TxnDecisionStatus::Pending => ResolveStep::Pending,
        }
    }

    /// The outcome of resolving one raw, envelope-tagged stored value
    /// against a read (ADR 0018 §2/PR3, extended PR4) — see
    /// [`resolve_once`](Self::resolve_once).
    async fn resolve_once_step(
        &self,
        physical_key: &[u8],
        vv: animus_storage::VersionedValue,
        read_ts: Option<HlcTimestamp>,
    ) -> ResolveStep {
        match txn::decode_envelope(&vv.value) {
            txn::Envelope::Committed(v) => ResolveStep::Value(Some(v)),
            txn::Envelope::Intent {
                txn_id,
                record_key,
                record_table,
                staged_value,
            } => {
                // The record lives in the **anchor's** tablet, which is
                // this same tablet's scope for a single-participant
                // transaction (PR3) or the anchor's own stage (PR4), but a
                // *different* tablet's scope entirely for a non-anchor
                // participant's own read (ADR 0018 §2/PR4) — a plain
                // scoped `get` only ever finds a *local* record, so a miss
                // here is reported as `Foreign` (carrying enough routing
                // info — `record_table`/`record_key` — for a caller that
                // can reach other tablets, e.g. `animusd`'s
                // `linearizable_get_served_fast`, to chase it down) rather
                // than assumed to be a structural impossibility as PR3 did.
                let record = self
                    .storage
                    .get(&self.scope.physical(&record_key))
                    .await
                    .ok()
                    .flatten()
                    .and_then(|r| txn::decode_record(&r.value));
                match record {
                    Some(r) if r.txn_id == txn_id => {
                        self.resolve_decided(
                            physical_key,
                            vv.version,
                            staged_value,
                            read_ts,
                            &r.status.to_public(),
                        )
                        .await
                    }
                    // Record missing locally (foreign, or the anchor's own
                    // stage genuinely hasn't applied here yet) or a mismatched
                    // txn_id (shouldn't happen structurally, see `txn.rs`'s
                    // doc) — a caller with no cross-tablet resolver treats
                    // this identically to `Pending` (see `read_resolved`'s
                    // doc); a caller that can chase it down
                    // (`linearizable_get_served_fast`) gets the routing info.
                    _ => ResolveStep::Foreign(IntentInfo {
                        txn_id,
                        record_key,
                        record_table,
                        staged_value,
                        version: hlc::unpack(vv.version),
                    }),
                }
            }
        }
    }

    /// Fetch `physical_key` (`version_ceiling: Some(v)` = as of engine
    /// version `v`, i.e. `read_at`/`scan_at`'s snapshot bound; `None` =
    /// latest) and resolve any intent found against `read_ts` (see
    /// [`resolve_once_step`](Self::resolve_once_step)), **retrying** while
    /// the covering transaction is still `Pending`, bounded by
    /// [`INTENT_WAIT_TIMEOUT`] — full push/wait scheduling is PR4. Outer
    /// `None` = gave up waiting; `Some(None)` = genuinely absent.
    async fn read_resolved(
        &self,
        physical_key: &[u8],
        read_ts: Option<HlcTimestamp>,
        version_ceiling: Option<u64>,
    ) -> Option<Option<Vec<u8>>> {
        let deadline = self.env.now().0 + INTENT_WAIT_TIMEOUT.as_nanos() as u64;
        loop {
            let vv = match version_ceiling {
                Some(v) => self.storage.get_at(physical_key, v).await.ok().flatten(),
                None => self.storage.get(physical_key).await.ok().flatten(),
            };
            let Some(vv) = vv else {
                return Some(None);
            };
            match self.resolve_once_step(physical_key, vv, read_ts).await {
                ResolveStep::Value(v) => return Some(v),
                // `Foreign` (no cross-tablet resolver available here) is
                // treated identically to `Pending` — see `ResolveStep`'s
                // doc; a caller that *can* chase a foreign record down uses
                // `linearizable_get_served_fast` instead of this bounded
                // internal retry.
                ResolveStep::Pending | ResolveStep::Foreign(_) => {
                    if self.env.now().0 >= deadline {
                        return None;
                    }
                    self.env.sleep(INTENT_WAIT_POLL).await;
                }
            }
        }
    }

    /// Drop the crate's internal txn-record marker keys
    /// (`txn::is_record_key`) from a raw, scope-stripped `(logical_key,
    /// VersionedValue)` row set and resolve every remaining row's value
    /// envelope against `read_ts` (`None` = latest) — the shared scan
    /// post-processing step for [`local_scan`](Self::local_scan)/
    /// [`scan_at`](Self::scan_at) (ADR 0018 §2/PR3).
    ///
    /// **Best-effort, non-blocking** — unlike the point-read path
    /// ([`read_resolved`](Self::read_resolved)): a still-`Pending` intent
    /// is silently omitted from the result rather than retried. Full
    /// push/wait for a scan is deferred to PR4.
    async fn resolve_scan_rows(
        &self,
        rows: Vec<(Vec<u8>, animus_storage::VersionedValue)>,
        read_ts: Option<HlcTimestamp>,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut out = Vec::with_capacity(rows.len());
        for (key, vv) in rows {
            if txn::is_record_key(&key) {
                continue;
            }
            let physical = self.scope.physical(&key);
            if let ResolveStep::Value(Some(v)) =
                self.resolve_once_step(&physical, vv, read_ts).await
            {
                out.push((key, v));
            }
        }
        out
    }

    /// Read `key` from this replica's **local engine**. NOTE: this is a local read
    /// — it is *not* yet linearizable (that is ReadIndex, Stage B.2). It is used by
    /// tests to observe a replica's applied state and to confirm convergence.
    ///
    /// A key currently covered by a **`Pending`** intent (ADR 0018 §2/PR3)
    /// reads as absent here (`None`) — this is a raw, non-blocking peek, not
    /// the retry-then-serve contract [`linearizable_get`](Self::linearizable_get)
    /// gives a resolved/committed value.
    pub async fn local_get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let physical = self.scope.physical(key);
        let vv = self.storage.get(&physical).await.ok().flatten()?;
        match self.resolve_once_step(&physical, vv, None).await {
            ResolveStep::Value(v) => v,
            ResolveStep::Pending | ResolveStep::Foreign(_) => None,
        }
    }

    /// Read one key of a **non-base row-kind scope** (ADR 0041 §3) — an LSI
    /// row, a footprint, or a change-log record.
    ///
    /// Deliberately simpler than [`local_get`](Self::local_get): those scopes
    /// only ever hold **committed** values, so there is no intent to resolve.
    /// Only [`KvCommand::KindBatch`] writes them, and it always commits
    /// outright; `TxnStage` stages intents solely on keys a client named, which
    /// are base-kind keys by construction. A non-committed envelope here would
    /// mean that invariant had broken, so it reads as absent rather than being
    /// silently unwrapped.
    ///
    /// An unknown `kind` reads as absent.
    pub async fn local_get_kind(&self, kind: u8, key: &[u8]) -> Option<Vec<u8>> {
        let scope = self.kind_scopes.get(kind as usize)?;
        let vv = self
            .storage
            .get(&scope.physical(key))
            .await
            .ok()
            .flatten()?;
        match txn::decode_envelope(&vv.value) {
            txn::Envelope::Committed(v) => Some(v),
            txn::Envelope::Intent { .. } => None,
        }
    }

    /// Scan a **non-base row-kind scope** (ADR 0041 §3) over `[start, end)`,
    /// in key order, returning committed values only.
    ///
    /// The read primitive behind an LSI `Query`/`Scan` (the `KIND_LSI`
    /// scope) and the GSI drain's sweep of pending change records
    /// (`KIND_CHANGE`, whose keys are HLC-suffixed, so key order *is* commit
    /// order). `end == None` is **unbounded above** — scan to the end of
    /// this scope's own keyspace — mirroring
    /// [`local_scan`](Self::local_scan)'s identical unbounded-above handling
    /// for the base scope: a table-wide LSI `Scan`'s tail tablet has no
    /// finite byte string that could bound it in general (an LSI row's
    /// trailing base-sort-key segment has no length limit), so the bound is
    /// derived internally from this kind scope's own physical prefix
    /// (`StorageScope::physical_bounds`) rather than trusted to a caller.
    ///
    /// An unknown `kind` scans as empty.
    pub async fn local_scan_kind(
        &self,
        kind: u8,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let Some(scope) = self.kind_scopes.get(kind as usize) else {
            return Vec::new();
        };
        let raw: Vec<(Vec<u8>, animus_storage::VersionedValue)> = match end {
            Some(e) => self
                .storage
                .scan(&scope.physical(start), &scope.physical(e))
                .await
                .ok()
                .into_iter()
                .flatten()
                .collect(),
            None => match scope.physical_bounds().1 {
                Some(physical_end) => self
                    .storage
                    .scan(&scope.physical(start), &physical_end)
                    .await
                    .ok()
                    .into_iter()
                    .flatten()
                    .collect(),
                // Only `StorageScope::whole()` (no real prefix) — not a real
                // tablet's kind scope, which always has a non-`0xFF`-ending
                // prefix (the scope selector byte itself) and so always
                // yields a finite `physical_bounds` upper bound. Mirrors
                // `pending_changes`'s identical fallback.
                None => Vec::new(),
            },
        };
        raw.into_iter()
            .filter_map(|(k, vv)| {
                let logical = scope.strip_in_range(&k)?.to_vec();
                match txn::decode_envelope(&vv.value) {
                    txn::Envelope::Committed(v) => Some((logical, v)),
                    txn::Envelope::Intent { .. } => None,
                }
            })
            .collect()
    }

    /// This tablet's own consumer-cursor watermark for `consumer` (ADR
    /// 0042/0043, `KIND_CURSOR` — see [`cursor`]'s module doc), keyed by this
    /// group's own *current* [`scope_range`](Self::scope_range) start. A
    /// **local**, non-linearizable read: the change-consumer loop that reads
    /// this is leader-gated and best-effort, like every other in-process
    /// cursor/reconciler read in this crate. `None` means either "no row for
    /// this tag on this exact tablet lineage" or "the stored bytes failed to
    /// decode" (a defensive read — this crate never writes anything else
    /// there); the ADR 0042 §7 "expected tag with no row ⇒ `W = 0`, no trim"
    /// convention is the caller's to apply.
    pub async fn cursor_watermark(&self, consumer: &str) -> Option<HlcTimestamp> {
        let key = cursor::cursor_key(&self.scope_range().start, consumer);
        let raw = self.local_get_kind(KIND_CURSOR, &key).await?;
        cursor::decode_watermark(&raw)
    }

    /// Every cursor row currently visible in this tablet's own `KIND_CURSOR`
    /// scope, as `(tag, watermark)` pairs. After a merge, a survivor's
    /// widened scope can hold more than one row for the same tag — one per
    /// absorbed tablet's own lineage, still physically present on the shared
    /// engine (`StorageScope::with_kind` shares one live `KeyRange` across
    /// every kind, so widening exposes rows a sibling wrote while it was its
    /// own tablet) — which is exactly the shape the ADR 0042 §7 min-over-rows
    /// rule exists to resolve; see
    /// [`cursor_min_watermark`](Self::cursor_min_watermark). A row whose raw
    /// bytes fail to decode is dropped rather than surfaced, mirroring
    /// [`cursor_watermark`](Self::cursor_watermark)'s own defensive read.
    pub async fn cursor_rows(&self) -> Vec<(String, HlcTimestamp)> {
        let scope = &self.kind_scopes[KIND_CURSOR as usize];
        let (start, end) = scope.physical_bounds();
        let Some(end) = end else {
            return Vec::new(); // only `StorageScope::whole()`; no real tablet
        };
        self.storage
            .scan(&start, &end)
            .await
            .ok()
            .into_iter()
            .flatten()
            .filter_map(|(k, vv)| {
                let logical = scope.strip_in_range(&k)?;
                let (_, tag) = cursor::parse_cursor_key(logical)?;
                let ts = match txn::decode_envelope(&vv.value) {
                    txn::Envelope::Committed(v) => cursor::decode_watermark(&v)?,
                    txn::Envelope::Intent { .. } => return None,
                };
                Some((tag.to_string(), ts))
            })
            .collect()
    }

    /// The ADR 0042 §7 **min-over-rows** watermark for `consumer`: the
    /// minimum watermark among every `KIND_CURSOR` row tagged `consumer` in
    /// this tablet's own (possibly merge-widened) scope, or `None` if no such
    /// row exists at all — the "expected tag with no row ⇒ `W = 0`, no trim"
    /// case, deliberately returned as `None` rather than a zero timestamp so
    /// a caller conflating "never copied anything" with "copied everything
    /// up to the epoch" is a compile-time-visible `Option`, not a silent
    /// wrong answer.
    pub async fn cursor_min_watermark(&self, consumer: &str) -> Option<HlcTimestamp> {
        self.cursor_rows()
            .await
            .into_iter()
            .filter(|(tag, _)| tag == consumer)
            .map(|(_, ts)| ts)
            .min()
    }

    /// A **linearizable** range scan of a non-base row-kind scope (ADR 0041
    /// §3) via ReadIndex — the read-barrier dual of
    /// [`local_scan_kind`](Self::local_scan_kind), and the read primitive
    /// behind an LSI `Query`/`Scan` (the `KIND_LSI` scope). Same barrier +
    /// ceiling drive as [`linearizable_scan`](Self::linearizable_scan): only
    /// the confirmed leader serves it, so a deposed leader returns `None`
    /// rather than a stale/partial range. `end == None` is unbounded above —
    /// see [`local_scan_kind`](Self::local_scan_kind)'s doc.
    ///
    /// A non-base scope only ever holds **committed** values (see
    /// [`local_scan_kind`](Self::local_scan_kind)'s doc — only
    /// [`KvCommand::KindBatch`] writes them, and it always commits outright),
    /// so there is no intent to resolve here, unlike
    /// [`linearizable_scan`](Self::linearizable_scan)'s base-scope reads.
    pub async fn linearizable_scan_kind(
        &self,
        kind: u8,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
        if !self.read_barrier().await {
            return None;
        }
        let ts = self.hlc.mint(self.env.now());
        if !self.ensure_ceiling_above(ts).await {
            return None;
        }
        let rows = self.local_scan_kind(kind, start, end).await;
        // Bump the *whole requested span*, mirroring `linearizable_scan`'s
        // identical reasoning — a future write anywhere in `[start, end)` is
        // pushed above this read regardless of how many rows it returned.
        self.ts_cache.lock().expect("ts cache poisoned").bump(
            start.to_vec(),
            end.map(<[u8]>::to_vec),
            ts,
        );
        Some(rows)
    }

    /// Every pending change-log record this tablet holds, in **commit order**
    /// (ADR 0041 §4): `(record key, encoded record)`.
    ///
    /// A whole-`KIND_CHANGE`-scope sweep, bounded by this tablet's own scope
    /// (`physical_bounds`, never `entries()` — a node's tablets share one
    /// engine, so a whole-engine scan would read every co-resident tablet's
    /// data too). Deliberately a named, purpose-built method rather than an
    /// unbounded variant of [`local_scan_kind`](Self::local_scan_kind): this is
    /// the one caller that legitimately wants the whole scope, and keeping the
    /// general API bounded stops an accidental full-tablet read being a typo
    /// away.
    ///
    /// Key order is commit order because a record's key ends in its own commit
    /// HLC (see [`KvCommand::KindBatch`]'s `change_log`), so a drain processing
    /// these front-to-back sees each key's mutations in the order they
    /// committed.
    pub async fn pending_changes(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let scope = &self.kind_scopes[KIND_CHANGE as usize];
        let (start, end) = scope.physical_bounds();
        let Some(end) = end else {
            return Vec::new(); // only `StorageScope::whole()`; no real tablet
        };
        self.storage
            .scan(&start, &end)
            .await
            .ok()
            .into_iter()
            .flatten()
            .filter_map(|(k, vv)| {
                let logical = scope.strip_in_range(&k)?.to_vec();
                match txn::decode_envelope(&vv.value) {
                    txn::Envelope::Committed(v) => Some((logical, v)),
                    txn::Envelope::Intent { .. } => None,
                }
            })
            .collect()
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
        // ADR 0018 §2/PR3: unlike `local_get`'s raw peek, a linearizable
        // read retries (bounded) a still-`Pending` intent rather than
        // reporting a false absence — see `read_resolved`'s doc.
        let physical = self.scope.physical(key);
        let value = self.read_resolved(&physical, None, None).await?;
        let (start, end) = ts_cache::point_span(key);
        self.ts_cache
            .lock()
            .expect("ts cache poisoned")
            .bump(start, end, ts);
        Some(value)
    }

    /// A **non-blocking, single-attempt** linearizable read of `key` (ADR
    /// 0018 §2/PR4) — same ReadIndex barrier + ceiling-drive as
    /// [`linearizable_get_served`](Self::linearizable_get_served), but
    /// makes exactly **one** resolution attempt instead of retrying a
    /// still-undecided intent for up to [`INTENT_WAIT_TIMEOUT`]. Lets a
    /// caller that can act on an unresolved intent itself (e.g. `animusd`'s
    /// `cp_get_local`, which can chase a foreign record down via a
    /// cross-tablet `TxnStatus` query) react immediately instead of paying
    /// the bounded internal wait first. `None` means the read barrier
    /// itself failed (not/no-longer the leader, or the probe timed out) —
    /// the same "not served" contract as
    /// [`linearizable_get_served`](Self::linearizable_get_served).
    pub async fn linearizable_get_served_fast(&self, key: &[u8]) -> Option<FastRead> {
        if !self.read_barrier().await {
            return None;
        }
        let ts = self.hlc.mint(self.env.now());
        if !self.ensure_ceiling_above(ts).await {
            return None;
        }
        let physical = self.scope.physical(key);
        let Some(vv) = self.storage.get(&physical).await.ok().flatten() else {
            let (start, end) = ts_cache::point_span(key);
            self.ts_cache
                .lock()
                .expect("ts cache poisoned")
                .bump(start, end, ts);
            return Some(FastRead::Value(None));
        };
        let step = self.resolve_once_step(&physical, vv, None).await;
        if let ResolveStep::Value(_) = &step {
            let (start, end) = ts_cache::point_span(key);
            self.ts_cache
                .lock()
                .expect("ts cache poisoned")
                .bump(start, end, ts);
        }
        Some(match step {
            ResolveStep::Value(v) => FastRead::Value(v),
            ResolveStep::Pending => FastRead::Pending,
            ResolveStep::Foreign(info) => FastRead::Foreign(info),
        })
    }

    /// Resolve `key`'s currently-stored intent given an **externally
    /// determined** decision `status` (ADR 0018 §2/PR4) — the counterpart
    /// to [`linearizable_get_served_fast`](Self::linearizable_get_served_fast)'s
    /// `Foreign` outcome: a caller that routed a cross-tablet
    /// `ClientRequest::TxnStatus` query to the intent's actual record owner
    /// and got back a decided (or still-`Pending`) status feeds it back
    /// here to finish the read, without this tablet ever needing a local
    /// copy of the record. Re-reads the current value at `key` (in case it
    /// changed since the caller last observed it) and, only if it is
    /// **still** the identical intent (`txn_id` matches), applies
    /// [`resolve_decided`](Self::resolve_decided)'s logic; otherwise (already
    /// resolved locally, or overwritten by something newer) falls through to
    /// an ordinary [`resolve_once_step`](Self::resolve_once_step) so the
    /// caller still gets a correct answer for whatever is there *now*.
    ///
    /// `None` means "no key at all right now" is impossible to distinguish
    /// from "still can't resolve" in a single non-blocking shape here, so
    /// this mirrors [`ResolveStep`]'s own contract instead: returns
    /// `Some(None)` for a genuinely absent value, `Some(Some(v))` for a
    /// resolved value, and `None` if the status was `Pending` or the key
    /// vanished entirely underneath (caller retries).
    pub async fn resolve_intent_given_status(
        &self,
        key: &[u8],
        read_ts: Option<HlcTimestamp>,
        txn_id: &TxnId,
        status: txn::TxnDecisionStatus,
    ) -> Option<Option<Vec<u8>>> {
        let physical = self.scope.physical(key);
        let vv = self.storage.get(&physical).await.ok().flatten()?;
        match txn::decode_envelope(&vv.value) {
            txn::Envelope::Committed(v) => Some(Some(v)),
            txn::Envelope::Intent {
                txn_id: found,
                staged_value,
                ..
            } if &found == txn_id => {
                match self
                    .resolve_decided(&physical, vv.version, staged_value, read_ts, &status)
                    .await
                {
                    ResolveStep::Value(v) => Some(v),
                    ResolveStep::Pending | ResolveStep::Foreign(_) => None,
                }
            }
            // No longer our intent (already resolved, or superseded) —
            // resolve whatever is actually there now instead.
            _ => match self.resolve_once_step(&physical, vv, read_ts).await {
                ResolveStep::Value(v) => Some(v),
                ResolveStep::Pending | ResolveStep::Foreign(_) => None,
            },
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
        let raw: Vec<(Vec<u8>, animus_storage::VersionedValue)> = match end {
            Some(e) => self
                .storage
                .scan(&self.scope.physical(start), &self.scope.physical(e))
                .await
                .ok()
                .into_iter()
                .flatten()
                .filter_map(|(k, vv)| {
                    let logical = self.scope.strip_in_range(&k)?;
                    Some((logical.to_vec(), vv))
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
                        Some((logical.to_vec(), vv))
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
                        (logical >= start).then(|| (logical.to_vec(), vv))
                    })
                    .collect(),
            },
        };
        // ADR 0018 §2/PR3: filter out this crate's internal txn-record
        // marker keys and resolve every remaining row's value envelope
        // (`resolve_scan_rows`'s doc) — applied *before* `limit` so an
        // internal marker key or a still-`Pending` row interleaved in the
        // requested range never silently consumes one of the caller's
        // requested slots.
        let mut pairs = self.resolve_scan_rows(raw, None).await;
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
    ///
    /// **Uncertainty-interval restart** (ADR 0018 §2, PR4): serializable
    /// (not externally-consistent) ordering across independently-clocked
    /// tablet groups means a version *committed* just after `ts`, within
    /// this leader's clock uncertainty, could actually be causally
    /// concurrent with (or even causally prior to) whatever minted `ts` —
    /// serving a bare "absent" in that case risks a torn snapshot. So when
    /// this read finds **no value at `ts`** but a version exists in
    /// `(ts, uncertainty_upper(ts)]`, it restarts **once** at that higher
    /// timestamp (`Hlc::uncertainty_upper`, the same margin the read-
    /// ceiling design already uses) and serves at the restarted point
    /// instead — a bounded *liveness* cost (one extra round, counted via
    /// `Metric::CpUncertaintyRestarts`), never a correctness one: the
    /// restart only ever moves the serve timestamp **later**, so it can
    /// only pick up more committed data, never lose any. Bounded to one
    /// restart, matching the ADR's "bounded restarts, then serve at the
    /// restarted ts" contract.
    pub async fn read_at(&self, key: &[u8], ts: HlcTimestamp) -> Option<Option<Vec<u8>>> {
        self.read_at_inner(key, ts, true).await
    }

    /// The shared implementation behind [`read_at`](Self::read_at);
    /// `allow_restart: false` on the one recursive call the uncertainty
    /// check makes, so a restart can never itself trigger a second restart.
    async fn read_at_inner(
        &self,
        key: &[u8],
        ts: HlcTimestamp,
        allow_restart: bool,
    ) -> Option<Option<Vec<u8>>> {
        if !self.read_barrier().await {
            return None;
        }
        if self.committed_ceiling() <= ts {
            return None;
        }
        let physical = self.scope.physical(key);
        let value = self
            .read_resolved(&physical, Some(ts), Some(hlc::pack(ts)))
            .await?;
        if allow_restart && value.is_none() {
            let upper = self.hlc.uncertainty_upper(ts);
            // A version strictly above `ts` but at or below `upper` means
            // something could have committed within the uncertainty
            // window — restart there instead of serving a possibly-torn
            // "absent". A miss (no such version, or the ceiling doesn't
            // yet cover `upper` so the restart itself would just refuse)
            // falls through to serving the original observation as-is.
            if let Some(latest) = self.storage.get(&physical).await.ok().flatten() {
                let ts_version = hlc::pack(ts);
                let upper_version = hlc::pack(upper);
                if latest.version > ts_version && latest.version <= upper_version {
                    self.metrics.incr(Metric::CpUncertaintyRestarts);
                    return Box::pin(self.read_at_inner(key, upper, false)).await;
                }
            }
        }
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
        let raw: Vec<(Vec<u8>, animus_storage::VersionedValue)> = match end {
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
                    Some((logical.to_vec(), vv))
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
                        Some((logical.to_vec(), vv))
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
                        (logical >= start).then(|| (logical.to_vec(), vv))
                    })
                    .collect(),
            },
        };
        // See `local_scan`'s identical comment: filter + resolve before
        // any caller-side limit is applied (`scan_at` itself takes no
        // limit, but keeps the same shared post-processing shape).
        let rows = self.resolve_scan_rows(raw, Some(ts)).await;
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
        // Deliberately **not** `local_scan` (ADR 0018 §2/PR3): that now
        // filters out this crate's internal txn-record marker keys and
        // resolves intents to what a *client* would see — exactly the
        // wrong thing here, which must physically erase everything this
        // scope ever wrote (ordinary values, still-pending intents, and
        // txn records alike), not just what a read would ever serve.
        // ADR 0041 §3: erase **every** row kind, not just the base scope this
        // group's `self.scope` addresses — a dropped table's LSI rows, change
        // log and footprints are just as much its data, and leaving them
        // behind would strand bytes no later reader can even name.
        for scope in &self.kind_scopes {
            for key in raw_scoped_keys(&self.storage, scope).await {
                let _ = self
                    .storage
                    .merge_tombstone(&scope.physical(&key), version)
                    .await;
            }
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
        //
        // That ratchet's own CAS loop only serializes a ReadCeiling proposal
        // against *other* ReadCeiling proposals (the regression above) — it
        // never touched `core`'s lock, so it did nothing to order a
        // ReadCeiling against a *concurrent write*'s `mint_pushed`. Computing
        // `candidate` inside `propose_ordered` (which now also wraps every
        // write proposer) is what closes that residual: see the doc on
        // `propose_ordered` for the shared root cause both fixes address.
        match self.propose_ordered(|| {
            let margin = self.hlc.uncertainty_upper(ts);
            let candidate = self.next_ceiling_candidate(margin);
            KvCommand::ReadCeiling { ts: candidate }
        }) {
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

    /// The physical, engine-global key this group stores logical `key` under,
    /// for a given row `kind` (ADR 0041 §3).
    ///
    /// The companion to [`storage`](Self::storage): anything reading this
    /// group's bytes straight off a (possibly node-shared) engine — a
    /// diagnostic, or a test asserting what a replica physically holds — must
    /// address them through the same scope the group writes them under, not by
    /// assembling the layout itself. Hard-coding `prefix || key` was correct
    /// only while a group had exactly one scope; it silently stopped being
    /// correct when kinds arrived, which is precisely the breakage this exists
    /// to prevent recurring.
    ///
    /// An unknown `kind` falls back to the base scope.
    #[must_use]
    pub fn physical_key(&self, kind: u8, key: &[u8]) -> Vec<u8> {
        self.kind_scopes
            .get(kind as usize)
            .unwrap_or(&self.scope)
            .physical(key)
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
    stage: &Arc<Mutex<StageOutcomes>>,
    engine_applied: &AtomicU64,
    wal_lock: &AsyncMutex<()>,
    halted: &AtomicBool,
    metrics: &MetricsHandle,
    scope: &StorageScope,
    // `kind_scopes`: every row kind (ADR 0041 §3). Only the snapshot image
    // and its install span the whole tablet; all other work here is base-kind
    // work through `scope`.
    kind_scopes: &[StorageScope; ALL_KINDS.len()],
    tablet: u64,
    hlc: &Hlc,
    sealed: &mut Vec<(KeyRange, HlcTimestamp)>,
    max_applied_ts: &mut Option<HlcTimestamp>,
    committed_ceiling: &AtomicU64,
    txn_tracker: &Mutex<TxnTracker>,
) -> bool {
    let mut did_work = false;

    // Install a fully-received snapshot (a follower catching up) into the engine
    // *before* applying log-tail effects, so the tail merges on top of the base.
    let pending_install = core
        .lock()
        .expect("raftkv core poisoned")
        .drain_pending_install();
    if let Some((last_index, bytes)) = pending_install {
        install_engine_image(storage, kind_scopes, &bytes).await;
        engine_applied.fetch_max(last_index, Ordering::SeqCst);
        // Witnessing point (ADR 0018 §2 amendment): a snapshot can carry
        // versions this node has never seen minted, so fold in the engine's
        // new high-water mark before this node ever mints/compares again.
        hlc.witness(hlc::unpack(storage.latest_version()), env.now());
        // ADR 0018 §2/PR6 corrective note: the `TxnTracker` must be rebuilt
        // from the freshly-installed image, exactly like `start_inner`
        // already does at group start, and for the identical reason
        // (`TxnTracker`'s own doc: compaction — and a snapshot install is
        // exactly that, from the receiving replica's perspective — can
        // skip straight past the individual `TxnStage`/`TxnCommit`/
        // `TxnAbort` log entries a catching-up replica would otherwise have
        // relied on to keep its tracker in sync). Before this fix, a
        // replica that caught up via `InstallSnapshot` (rather than
        // replaying every log entry individually) could be left with a
        // **stale** `pending` entry for a transaction the engine itself
        // already reflects as decided — the resolver loop would then find
        // it "Pending" forever from this replica's own (stale) tracker,
        // repeatedly re-proposing a no-op decide (harmless since the
        // duelling-decider fix above makes that safe) but never
        // transitioning it into `unresolved_decided`, so the resolver never
        // proactively resolves the participant's intent (only an on-demand
        // foreign-intent read, which doesn't consult the tracker at all,
        // would still find it). Found live by the ADR 0018 multi-tablet
        // transaction corpus's `anchor_leader_kill_early` scenario (seed
        // 3924719889167511385): a leader-killed-then-healed replica caught
        // up via snapshot install partway through a transaction's own
        // lifecycle, precisely reproducing this gap.
        *txn_tracker.lock().expect("txn tracker poisoned") =
            rebuild_txn_tracker(storage, scope).await;
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
                    pending.push(MergeOp::put(
                        scope.physical(&key),
                        txn::encode_committed(&value),
                        hlc::pack(ts),
                    ));
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
                            .merge(
                                &scope.physical(key),
                                &txn::encode_committed(value),
                                hlc::pack(ts),
                            )
                            .await
                            .expect("raftkv apply batch put");
                    }
                }
            }
            KvCommand::KindBatch {
                writes,
                change_log,
                fence,
                ts,
            } => {
                assert_ts_monotonic(max_applied_ts, ts);
                // Gated as one unit, exactly like `Batch` — an index write that
                // half-applied would leave an LSI row describing a base row
                // that never landed, which is the one thing colocating them was
                // supposed to make impossible. Every kind shares this tablet's
                // single range, so one fence covers them all.
                if writes
                    .iter()
                    .all(|(_, key, _)| fence.contains(key) && !is_sealed(sealed, key))
                {
                    for (kind, key, value) in &writes {
                        // An unknown kind cannot be applied anywhere safe (this
                        // build has no scope for it) and must not silently land
                        // in another kind's keyspace. It can only arise from a
                        // peer proposing a kind this build predates, so skip it
                        // rather than guess — the same call this crate's
                        // snapshot install makes for an unknown-kind entry.
                        let Some(kscope) = kind_scopes.get(*kind as usize) else {
                            tracing::warn!(kind, "KindBatch write of unknown row kind skipped");
                            continue;
                        };
                        let physical = kscope.physical(key);
                        match value {
                            Some(v) => pending.push(MergeOp::put(
                                physical,
                                txn::encode_committed(v),
                                hlc::pack(ts),
                            )),
                            None => pending.push(MergeOp::tombstone(physical, hlc::pack(ts))),
                        }
                    }
                    // The change-log record's key is completed here, with THIS
                    // entry's commit timestamp — the only one that agrees with
                    // the entry's position in the log, and so the only one that
                    // makes the log readable in commit order (ADR 0041 §4a).
                    if let Some((prefix, record)) = &change_log {
                        let mut key = prefix.clone();
                        key.extend_from_slice(&hlc::pack(ts).to_be_bytes());
                        pending.push(MergeOp::put(
                            kind_scopes[KIND_CHANGE as usize].physical(&key),
                            txn::encode_committed(record),
                            hlc::pack(ts),
                        ));
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
                    let raw = storage.get(&physical_key).await.expect("raftkv cas read");
                    // ADR 0018 §2/PR3: a pending (or otherwise unresolved)
                    // intent makes "the current committed value" ambiguous
                    // — every replica deterministically fails the swap
                    // rather than ever guessing at a match or an absence.
                    // PR4 revisits CAS-vs-in-flight-txn interaction
                    // (push/wait the blocking transaction).
                    let swapped = match raw.map(|vv| txn::decode_envelope(&vv.value)) {
                        None => expected.is_none(),
                        Some(txn::Envelope::Committed(v)) => Some(v) == expected,
                        Some(txn::Envelope::Intent { .. }) => false,
                    };
                    if swapped {
                        // Same write path as `Put`: `hlc::pack(ts)` is the MVCC
                        // version, so re-applying on recovery is idempotent
                        // (per-key LWW).
                        storage
                            .merge(&physical_key, &txn::encode_committed(&value), hlc::pack(ts))
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
            KvCommand::TxnStage {
                txn_id,
                record_key,
                record_table,
                is_anchor,
                writes,
                spans,
                conditions,
                fence,
                ts,
            } => {
                assert_ts_monotonic(max_applied_ts, ts);
                // ADR 0018 §2/PR5 resurrection guard: PR4's prepare phase
                // is concurrent, so the anchor's own `TxnStage` (this
                // entry, when `is_anchor`) can arrive **after** a recovery
                // pusher has already decided this transaction — the
                // pusher found an orphaned participant intent, got no
                // record back, and created an `Aborted` tombstone
                // (`KvCommand::TxnAbort`'s `orphan_created_ts` case). A
                // late-arriving genuine stage must never resurrect a
                // `Pending` record over an already-decided one (nor write
                // fresh intents nobody will ever resolve, since this
                // record's `intent_spans` — fixed at creation — likely
                // doesn't name them): first decision wins, exactly like
                // `TxnCommit`/`TxnAbort`'s own duelling-decider no-op.
                // Only meaningful for `is_anchor`: a non-anchor
                // participant's own tablet never holds the record to check
                // against at all (see the doc below for why that's fine).
                let already_decided = is_anchor
                    && storage
                        .get(&scope.physical(&record_key))
                        .await
                        .expect("raftkv txn stage record read")
                        .and_then(|vv| txn::decode_record(&vv.value))
                        .is_some_and(|r| r.txn_id == txn_id && r.status != txn::TxnStatus::Pending);
                if already_decided {
                    tracing::warn!(
                        ?txn_id,
                        ?record_key,
                        "raftkv: TxnStage arrived after this record already decided — no-op \
                         (a late anchor stage racing a recovery decision; first decision wins, \
                         never a resurrection)"
                    );
                }
                // ADR 0018 §2/PR6 (task #16): writers **push** intents —
                // a target key already holding an *unresolved* intent from
                // a *different* transaction blocks this whole stage, rather
                // than silently overwriting it, so an abort-restore's own
                // one-hop-back lookback can never land on another still-
                // live intent (see `KvCommand::TxnStage`'s doc for the full
                // durability argument this closes). Same-txn re-staging
                // (a WAL-replay re-application) is unaffected — matched by
                // `txn_id` equality, not mere presence of *an* intent.
                let blocked_by = 'blocked: {
                    for (key, _) in &writes {
                        let Some(vv) = storage
                            .get(&scope.physical(key))
                            .await
                            .expect("raftkv txn stage conflict read")
                        else {
                            continue;
                        };
                        if let txn::Envelope::Intent {
                            txn_id: blocker, ..
                        } = txn::decode_envelope(&vv.value)
                            && blocker != txn_id
                        {
                            break 'blocked Some((key.clone(), blocker));
                        }
                    }
                    None
                };
                if let Some((blocked_key, blocker)) = &blocked_by {
                    tracing::warn!(
                        ?txn_id,
                        blocking_txn = ?blocker,
                        ?blocked_key,
                        "raftkv: TxnStage blocked by another transaction's unresolved intent \
                         on a target key — whole-or-nothing no-op (the proposer must push the \
                         blocking transaction and retry)"
                    );
                }
                // Whole-or-nothing, matching `Batch`: a partial stage would
                // let a reader observe some of a transaction's intents but
                // not others (see `KvCommand::TxnStage`'s doc). A
                // non-anchor participant's `record_key` names the
                // **anchor's** record — a key in a different tablet's (or
                // even a different table's) keyspace entirely — so it is
                // never checked against or written into *this* group's own
                // fence/engine (ADR 0018 §2/PR4).
                let record_in_fence =
                    !is_anchor || (fence.contains(&record_key) && !is_sealed(sealed, &record_key));
                let all_in_fence = !already_decided
                    && blocked_by.is_none()
                    && writes
                        .iter()
                        .all(|(k, _)| fence.contains(k) && !is_sealed(sealed, k))
                    && record_in_fence;
                // ADR 0018 §2 apply-time write-key conditions amendment:
                // evaluate this stage's own-key conditions (byte-level OCC
                // — see `KvCommand::TxnStage`'s doc) once, only when the
                // stage is otherwise eligible to proceed at all — a
                // condition check never masks a more fundamental fence/
                // seal/foreign-intent/already-decided rejection, and those
                // reasons are exactly what `StageOutcome` still needs to
                // tell apart from a genuine condition failure. Drain the
                // pending run first (mirrors `Cas`'s own
                // read-after-flush-pending discipline) so a condition
                // observes every earlier committed write in this same
                // apply pass.
                let condition_failure: Option<Vec<u8>> = if all_in_fence && !conditions.is_empty() {
                    flush_pending(storage, &mut pending, metrics).await;
                    let mut failure = None;
                    for (key, expected) in &conditions {
                        let raw = storage
                            .get(&scope.physical(key))
                            .await
                            .expect("raftkv txn stage condition read");
                        let matches = match raw.map(|vv| txn::decode_envelope(&vv.value)) {
                            None => expected.is_none(),
                            Some(txn::Envelope::Committed(v)) => Some(v) == *expected,
                            // Same-txn re-staging (a WAL-replay
                            // re-application): this exact stage already
                            // landed this exact intent at this exact key,
                            // which means this exact deterministic check
                            // already passed the first time it ran — trust
                            // that instead of re-evaluating against an
                            // intent envelope that no longer holds "the
                            // value before this stage" at all.
                            Some(txn::Envelope::Intent {
                                txn_id: blocker, ..
                            }) if blocker == txn_id => true,
                            // A *foreign* intent here would already have
                            // been caught by `blocked_by` above (every
                            // condition key is expected to also be a write
                            // key) — but never silently treat an
                            // unresolvable "current value" as a match.
                            Some(txn::Envelope::Intent { .. }) => false,
                        };
                        if !matches {
                            failure = Some(key.clone());
                            break;
                        }
                    }
                    failure
                } else {
                    None
                };
                let stage_ok = all_in_fence && condition_failure.is_none();
                if stage_ok {
                    flush_pending(storage, &mut pending, metrics).await;
                    let version = hlc::pack(ts);
                    for (key, staged_value) in &writes {
                        let env = txn::encode_intent(
                            &txn_id,
                            &record_key,
                            &record_table,
                            staged_value.as_deref(),
                        );
                        storage
                            .merge(&scope.physical(key), &env, version)
                            .await
                            .expect("raftkv apply txn stage intent");
                    }
                    if is_anchor {
                        let record = txn::TxnRecord {
                            txn_id: txn_id.clone(),
                            status: txn::TxnStatus::Pending,
                            intent_spans: spans,
                            created_ts: ts,
                        };
                        storage
                            .merge(
                                &scope.physical(&record_key),
                                &txn::encode_record(&record),
                                version,
                            )
                            .await
                            .expect("raftkv apply txn stage record");
                        // ADR 0018 §2/PR5: track this freshly-created (or
                        // re-staged/replayed) `Pending` record so
                        // `animusd`'s resolver loop can find it without a
                        // full re-scan. Idempotent: re-inserting the same
                        // `(record_key, created_ts)` on a WAL-replay
                        // re-application is harmless.
                        txn_tracker
                            .lock()
                            .expect("txn tracker poisoned")
                            .pending
                            .insert(txn_id, (record_key, ts));
                    }
                }
                // ADR 0018 §2 apply-time write-key conditions amendment:
                // record *why*, not just whether, this stage no-op'd — see
                // `StageOutcome`'s doc for the coordinator-facing meaning of
                // each variant. Priority mirrors the gates above: a
                // structural fence/seal-miss or already-decided race
                // (`Fenced`) and a foreign-intent block (`IntentBlocked`)
                // both pre-empt ever evaluating this stage's own
                // conditions, so they take priority over `ConditionFailed`
                // here too.
                let outcome = if already_decided {
                    txn::StageOutcome::Fenced
                } else if let Some((blocked_key, blocker)) = blocked_by {
                    txn::StageOutcome::IntentBlocked {
                        key: blocked_key,
                        txn_id: blocker,
                    }
                } else if !all_in_fence {
                    txn::StageOutcome::Fenced
                } else if let Some(key) = condition_failure {
                    txn::StageOutcome::ConditionFailed { key }
                } else {
                    txn::StageOutcome::Staged
                };
                stage
                    .lock()
                    .expect("stage outcomes poisoned")
                    .outcomes
                    .insert(index, outcome);
            }
            KvCommand::TxnCommit {
                txn_id,
                record_key,
                ts,
            } => {
                assert_ts_monotonic(max_applied_ts, ts);
                flush_pending(storage, &mut pending, metrics).await;
                let physical_record = scope.physical(&record_key);
                let current = storage
                    .get(&physical_record)
                    .await
                    .expect("raftkv txn commit read")
                    .and_then(|vv| txn::decode_record(&vv.value));
                match current {
                    // The stage never landed here (fenced/sealed out at
                    // propose time) — nothing to commit. Matches this
                    // crate's fence-miss doctrine: a deterministic, silent
                    // no-op, never a surfaced error.
                    None => {}
                    Some(r) if r.txn_id != txn_id => {
                        // `record_key` is derived from `txn_id` itself
                        // (`txn::record_key`); a mismatch means two
                        // different transactions computed the identical
                        // key — a real bug, not a recoverable condition.
                        panic!(
                            "raftkv txn commit: record at {record_key:?} belongs to a \
                             different txn ({:?} != {txn_id:?})",
                            r.txn_id
                        );
                    }
                    Some(r) => match r.status {
                        txn::TxnStatus::Pending => {
                            let record = txn::TxnRecord {
                                status: txn::TxnStatus::Committed { commit_ts: ts },
                                ..r
                            };
                            storage
                                .merge(
                                    &physical_record,
                                    &txn::encode_record(&record),
                                    hlc::pack(ts),
                                )
                                .await
                                .expect("raftkv apply txn commit");
                            // ADR 0018 §2/PR5: the first (winning) decision
                            // on this record — move it out of `pending` and
                            // into `unresolved_decided` for the resolver
                            // loop to fan out.
                            let mut t = txn_tracker.lock().expect("txn tracker poisoned");
                            t.pending.remove(&txn_id);
                            t.unresolved_decided.insert(
                                txn_id,
                                (record_key, txn::TxnOutcome::Committed { commit_ts: ts }),
                            );
                        }
                        // Idempotent WAL-replay re-application: identical
                        // decision, nothing to do (the tracker was already
                        // updated the first time this applied).
                        txn::TxnStatus::Committed { commit_ts } if commit_ts == ts => {}
                        // ADR 0018 §2/PR6 corrective note: a **second**,
                        // differently-timestamped `TxnCommit` for an
                        // already-`Committed` record is NOT "impossible by
                        // construction" the way PR5 originally assumed —
                        // `txn_commit_at_least`'s own `mint_at_least` is not
                        // idempotent across calls (each proposes a *fresh*
                        // ts), so two independent, individually well-formed
                        // deciders (a coordinator whose own round trip is
                        // still genuinely in flight — `animusd`'s own
                        // `CLIENT_TIMEOUT`, 10s, comfortably exceeds
                        // `RECOVERY_GRACE`, 5s — racing the recovery
                        // resolver's own post-grace push) can each conclude
                        // "commit" and each get their own entry accepted;
                        // whichever applies first is definitionally the
                        // winner (this group's one totally-ordered log is
                        // still the sole arbiter), and this second entry is
                        // exactly the same *legal* duelling-decider shape as
                        // the `Aborted` arm below — a logged no-op, never an
                        // assert. The one case that stays a hard assert
                        // (impossible by construction, no live decider
                        // reachable) is two *conflicting* decisions racing
                        // to a genuinely-simultaneous first-applied log
                        // position, which cannot happen in one sequential
                        // log — that invariant is unaffected; this arm only
                        // relaxes "same outcome, different ts". Every
                        // resolve caller (`ClientCtx::cp_txn`/`txn_recover`,
                        // `txn_resolver_loop`) already re-reads the record's
                        // *actual* decided status before resolving anything
                        // (never assumes its own proposal won), and the
                        // `TxnTracker` update below only ever happens on the
                        // *first*-applied decision (the losing entry never
                        // touches it) — so no caller can ever resolve using
                        // a losing, stale `commit_ts`. See the ADR's PR6
                        // amendment for the full account (found live by the
                        // multi-tablet transaction corpus under
                        // participant-leader-kill fault injection).
                        txn::TxnStatus::Committed { commit_ts } => {
                            tracing::warn!(
                                ?txn_id,
                                first_commit_ts = ?commit_ts,
                                second_commit_ts = ?ts,
                                "raftkv: TxnCommit lost to an earlier-applied TxnCommit on the \
                                 same record, at a different ts (duelling deciders reaching the \
                                 same outcome independently — log order is the ballot); no-op"
                            );
                        }
                        // ADR 0018 §2/PR5 (decision-semantics amendment):
                        // recovery makes duelling deciders legal — a
                        // still-live coordinator's commit can race a
                        // recovery pusher's abort. The anchor's own Raft log
                        // is the sole arbiter: whichever decision applied
                        // FIRST already flipped the record and updated the
                        // tracker; this later, losing proposal is a logged
                        // no-op, never an assert. A caller must re-read the
                        // record's actual status (`txn_status_local`) to
                        // report honestly rather than assume its own
                        // proposal won.
                        txn::TxnStatus::Aborted => {
                            tracing::warn!(
                                ?txn_id,
                                commit_ts = ?ts,
                                "raftkv: TxnCommit lost to a prior TxnAbort on the same record \
                                 (duelling decider — both outcomes are legal, log order is the \
                                 ballot); no-op"
                            );
                        }
                    },
                }
            }
            KvCommand::TxnAbort {
                txn_id,
                record_key,
                ts,
                orphan_created_ts,
            } => {
                assert_ts_monotonic(max_applied_ts, ts);
                flush_pending(storage, &mut pending, metrics).await;
                let physical_record = scope.physical(&record_key);
                let current = storage
                    .get(&physical_record)
                    .await
                    .expect("raftkv txn abort read")
                    .and_then(|vv| txn::decode_record(&vv.value));
                match current {
                    // ADR 0018 §2/PR5: no record exists at all — either
                    // the ordinary fence-miss no-op (unchanged: the
                    // anchor's own stage never landed here, `None` is
                    // this arm's caller-supplied signal that this isn't a
                    // recovery push), or a recovery pusher's orphan-abort
                    // tombstone (`Some(created_ts)`): synthesize a fresh
                    // `Aborted` record directly. Safe unconditionally —
                    // there is no existing record here to resurrect or
                    // clobber (that's the `Some(r) => ..` arm below,
                    // untouched). `intent_spans` is empty: unknown, since
                    // no real record ever existed to learn participants
                    // from — a documented residual (proactive resolver
                    // fan-out can't reach unlisted participants this way;
                    // on-demand resolution via any reader hitting any of
                    // their own intents is unaffected, since that path
                    // routes through this record's `record_table`/
                    // `record_key` — carried in the intent envelope
                    // itself — never through `intent_spans`).
                    None => {
                        if let Some(created_ts) = orphan_created_ts {
                            let record = txn::TxnRecord {
                                txn_id: txn_id.clone(),
                                status: txn::TxnStatus::Aborted,
                                intent_spans: Vec::new(),
                                created_ts,
                            };
                            storage
                                .merge(
                                    &physical_record,
                                    &txn::encode_record(&record),
                                    hlc::pack(ts),
                                )
                                .await
                                .expect("raftkv apply txn orphan-abort tombstone");
                            tracing::warn!(
                                ?txn_id,
                                ?record_key,
                                ?created_ts,
                                "raftkv: recovery created an orphan-abort tombstone (no record \
                                 ever existed for this txn_id — a stale intent with a crashed \
                                 or fence/seal-missed anchor stage)"
                            );
                        }
                    }
                    Some(r) if r.txn_id != txn_id => {
                        panic!(
                            "raftkv txn abort: record at {record_key:?} belongs to a \
                             different txn ({:?} != {txn_id:?})",
                            r.txn_id
                        );
                    }
                    Some(r) => match r.status {
                        txn::TxnStatus::Pending => {
                            let record = txn::TxnRecord {
                                status: txn::TxnStatus::Aborted,
                                ..r
                            };
                            storage
                                .merge(
                                    &physical_record,
                                    &txn::encode_record(&record),
                                    hlc::pack(ts),
                                )
                                .await
                                .expect("raftkv apply txn abort");
                            let mut t = txn_tracker.lock().expect("txn tracker poisoned");
                            t.pending.remove(&txn_id);
                            t.unresolved_decided
                                .insert(txn_id, (record_key, txn::TxnOutcome::Aborted));
                        }
                        // Idempotent WAL-replay re-application.
                        txn::TxnStatus::Aborted => {}
                        // ADR 0018 §2/PR5: the dual of `TxnCommit`'s
                        // duelling-decider no-op above — this abort lost to
                        // a prior commit on the same record. Legal, logged,
                        // never an assert; see that arm's doc for the full
                        // argument.
                        txn::TxnStatus::Committed { commit_ts } => {
                            tracing::warn!(
                                ?txn_id,
                                ?commit_ts,
                                abort_ts = ?ts,
                                "raftkv: TxnAbort lost to a prior TxnCommit on the same record \
                                 (duelling decider — both outcomes are legal, log order is the \
                                 ballot); no-op"
                            );
                        }
                    },
                }
            }
            KvCommand::TxnResolve {
                txn_id,
                record_key,
                keys,
                outcome,
                ts,
            } => {
                assert_ts_monotonic(max_applied_ts, ts);
                flush_pending(storage, &mut pending, metrics).await;
                // ADR 0018 §2/PR5: this group can only ever observe a
                // resolve for a `txn_id` it itself anchors (see
                // `TxnTracker::unresolved_decided`'s doc for the documented,
                // safe approximation) — a plain `remove` on a `txn_id` this
                // group never tracked (a pure participant applying its own
                // resolve) is a harmless no-op.
                txn_tracker
                    .lock()
                    .expect("txn tracker poisoned")
                    .unresolved_decided
                    .remove(&txn_id);
                // ADR 0018 §2/PR4: `outcome` is carried explicitly by the
                // command rather than re-derived by reading `record_key`
                // locally — see `KvCommand::TxnResolve`'s doc. This is what
                // lets a non-anchor participant (whose own tablet never
                // holds the record at all) resolve its own intents
                // uniformly with the anchor.
                {
                    let outcome_commit_ts: Option<HlcTimestamp> = match &outcome {
                        txn::TxnOutcome::Committed { commit_ts } => Some(*commit_ts),
                        txn::TxnOutcome::Aborted => None,
                    };
                    // ADR 0018 §2/PR6 hardening (defense-in-depth, not a
                    // reproduced bug): every current decider
                    // (`animusd`'s ordinary `cp_txn` commit path and its
                    // recovery pusher alike — see `txn_commit_at_least`/
                    // `txn_abort`'s doc) already re-reads the record's
                    // *actual* decided status before resolving, rather than
                    // trusting its own candidate outcome. But nothing at
                    // apply time structurally enforced that discipline —
                    // a future caller that resolved from a stale/assumed
                    // outcome instead of a re-read would be
                    // LWW-unrepairable (this key's version chain would
                    // carry a wrong-outcome rewrite no later correct
                    // resolve can undo). When this group holds
                    // `record_key` locally — i.e. this is the anchor's own
                    // group for `txn_id` — cross-check the carried
                    // `outcome` against the record's real `status` and
                    // refuse to resolve on a mismatch, whole-or-nothing,
                    // rather than silently applying the wrong outcome.
                    // **No known live violator as of PR6; guards a class,
                    // not a reproduced bug** (see the PR6 audit recorded
                    // in `docs/engineering-lessons.md` and ADR 0018's PR5
                    // amendment §1's corrective note).
                    //
                    // A non-anchor participant's own apply cannot run this
                    // check at all — its tablet never holds a copy of
                    // `record_key`'s record (that's the entire reason
                    // `outcome` travels explicitly on this command; see
                    // `KvCommand::TxnResolve`'s doc). That residual is
                    // accepted, not fixed, here: closing it would need the
                    // resolver to also carry the record's own decision
                    // `ts` so a participant could at least reject an
                    // outcome inconsistent with it, which is left for a
                    // future PR if the cost proves worth it.
                    let outcome_mismatch = match storage
                        .get(&scope.physical(&record_key))
                        .await
                        .expect("raftkv txn resolve record read")
                        .and_then(|vv| txn::decode_record(&vv.value))
                    {
                        Some(record) if record.txn_id == txn_id => match (&record.status, &outcome)
                        {
                            (
                                txn::TxnStatus::Committed { commit_ts: rec_ts },
                                txn::TxnOutcome::Committed { commit_ts: out_ts },
                            ) => rec_ts != out_ts,
                            (txn::TxnStatus::Aborted, txn::TxnOutcome::Aborted) => false,
                            // Resolving before the record itself ever
                            // decided, or a flat Committed-vs-Aborted
                            // disagreement — either way, a mismatch.
                            _ => true,
                        },
                        // No local record for this exact `txn_id` — either
                        // a non-anchor participant (nothing to check), or
                        // this group is the anchor but hasn't applied its
                        // own decision yet in this same batch (can't
                        // happen: `TxnCommit`/`TxnAbort` always apply
                        // strictly before the `TxnResolve` that follows
                        // them — see `txn_commit_at_least`/`txn_abort`'s
                        // callers). Proceed either way; this is the
                        // expected, common case.
                        _ => false,
                    };
                    if outcome_mismatch {
                        tracing::warn!(
                            ?txn_id,
                            ?record_key,
                            carried_outcome = ?outcome,
                            "raftkv: TxnResolve's carried outcome does not match the anchor's own \
                             decided record — skipping resolve as defense-in-depth (no known live \
                             violator as of PR6; guards a class, not a reproduced bug)"
                        );
                    }
                    let version = hlc::pack(ts);
                    for key in &keys {
                        if outcome_mismatch {
                            continue; // whole-or-nothing: skip every key, not just this one
                        }
                        let physical_key = scope.physical(key);
                        let Some(vv) = storage
                            .get(&physical_key)
                            .await
                            .expect("raftkv txn resolve key read")
                        else {
                            continue; // nothing left here to resolve
                        };
                        let intent = match txn::decode_envelope(&vv.value) {
                            txn::Envelope::Intent {
                                txn_id: found,
                                staged_value,
                                ..
                            } if found == txn_id => Some(staged_value),
                            // Already resolved, or a different/newer txn's
                            // intent has since overwritten this key —
                            // nothing of ours left here. Idempotent no-op.
                            _ => None,
                        };
                        let Some(staged_value) = intent else {
                            continue;
                        };
                        match outcome_commit_ts {
                            Some(_commit_ts) => match staged_value {
                                // Committed: the staged value becomes the
                                // committed value.
                                Some(v) => {
                                    storage
                                        .merge(&physical_key, &txn::encode_committed(&v), version)
                                        .await
                                        .expect("raftkv apply txn resolve commit");
                                }
                                // A staged delete resolves to an actual
                                // tombstone — the only place `TxnResolve`
                                // writes one, since it's finalizing an
                                // already-decided delete, not guessing.
                                None => {
                                    storage
                                        .merge_tombstone(&physical_key, version)
                                        .await
                                        .expect("raftkv apply txn resolve commit delete");
                                }
                            },
                            None => {
                                // Aborted: restore whatever this key held
                                // immediately before the intent — never a
                                // tombstone, which would incorrectly shadow
                                // that older, still-live committed value
                                // (see `txn.rs`'s module doc). Every
                                // version this group has ever applied is
                                // strictly increasing
                                // (`assert_ts_monotonic`), so
                                // `vv.version - 1` is guaranteed to sit
                                // strictly below the intent's own version
                                // and at/above this key's true prior
                                // version. **This one-hop-back lookback is
                                // sound only because ADR 0018 §2/PR6 (task
                                // #16)'s apply-time writer-push-intents
                                // guard (`KvCommand::TxnStage`'s doc)
                                // structurally rules out another
                                // transaction's own unresolved intent ever
                                // having been written at that prior
                                // version** — before that fix, an
                                // overwriting transaction's own later abort
                                // could land here on a still-live intent
                                // from the transaction it overwrote,
                                // blindly re-merging its raw envelope bytes
                                // (a corpus depth run's original finding:
                                // the corrupted-MVCC-chain durability
                                // hole). `pvv.value` below is therefore
                                // always either a `Committed` envelope or
                                // (via the `None` arm) genuinely absent —
                                // never another live `Intent`.
                                let prior = storage
                                    .get_at(&physical_key, vv.version.saturating_sub(1))
                                    .await
                                    .expect("raftkv txn resolve prior read");
                                match prior {
                                    Some(pvv) => storage
                                        .merge(&physical_key, &pvv.value, version)
                                        .await
                                        .expect("raftkv apply txn resolve abort restore"),
                                    None => storage
                                        .merge_tombstone(&physical_key, version)
                                        .await
                                        .expect("raftkv apply txn resolve abort restore tombstone"),
                                };
                            }
                        }
                    }
                }
            }
            // No `assert_ts_monotonic` call here, deliberately: `NoOp` carries
            // no `ts` at all (`command_ts` returns `None` for it), so there is
            // nothing to check monotonicity of.
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
            Some(engine_image(storage, kind_scopes).await)
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

/// One key's snapshot entry: `(row kind, key, value-or-tombstone, version)`.
///
/// The kind (ADR 0041 §3) is what lets one image carry a whole tablet — every
/// row kind's scope — while the key stays the *logical* key within its own
/// scope, so the receiver can re-prefix it under its own scope set.
pub(crate) type ImageEntry = (u8, Vec<u8>, Option<Vec<u8>>, u64);

/// Serialize this scope's contents (including tombstones) as the snapshot
/// image shipped to a lagging follower. Bounded to `scope` (prefix **and**
/// range — see `StorageScope`'s doc): on a shared engine, an unbounded dump
/// would leak every other tenant's keys into this tablet's snapshot **and**
/// duplicate them into whichever engine receives it, corrupting a group that
/// never agreed to those writes through its own Raft log. Under the default
/// (whole) scope this is byte-for-byte the prior unbounded behavior.
/// Every logical key currently physically present in one `scope` — **raw**,
/// bypassing both the record-key filter and the value-envelope resolution
/// [`RaftKvNode::local_scan`] applies. The only caller is
/// [`RaftKvNode::erase_scope`], which sweeps each of a tablet's row-kind scopes
/// in turn (ADR 0041 §3); a free function rather than a method because it is
/// per-*scope*, not per-group.
async fn raw_scoped_keys<S: StorageEngine>(storage: &S, scope: &StorageScope) -> Vec<Vec<u8>> {
    let (physical_start, physical_end) = scope.physical_bounds();
    match physical_end {
        Some(e) => storage
            .scan(&physical_start, &e)
            .await
            .ok()
            .into_iter()
            .flatten()
            .filter_map(|(k, _)| scope.strip_in_range(&k).map(<[u8]>::to_vec))
            .collect(),
        None => storage
            .entries()
            .await
            .ok()
            .into_iter()
            .flatten()
            .filter_map(|(k, _)| scope.strip_in_range(&k).map(<[u8]>::to_vec))
            .collect(),
    }
}

async fn engine_image<S: StorageEngine>(
    storage: &S,
    kind_scopes: &[StorageScope; ALL_KINDS.len()],
) -> Vec<u8> {
    // One pass over the engine, classified by kind (ADR 0041 §3): a tablet's
    // scopes are disjoint, so each physical key is claimed by at most one of
    // them — `strip_in_range` on the wrong kind returns `None`, which is also
    // what excludes a co-resident sibling tablet's data from this image.
    let rows = storage
        .entries_with_tombstones()
        .await
        .expect("raftkv engine scan");
    let mut entries: Vec<ImageEntry> = Vec::new();
    for (k, v, version) in rows {
        let claimed = ALL_KINDS
            .iter()
            .zip(kind_scopes)
            .find_map(|(kind, scope)| scope.strip_in_range(&k).map(|l| (*kind, l.to_vec())));
        if let Some((kind, logical)) = claimed {
            entries.push((kind, logical, v, version));
        }
    }
    codec::encode_image(&entries)
}

/// Write a received snapshot image into the engine (a follower catching up),
/// versioned so per-key LWW keeps it consistent with the log tail merged on top.
/// The wire image carries *logical* keys (stripped by the sender's
/// `engine_image`); each is re-prefixed to *this* replica's own `scope`
/// before writing into the (possibly shared) engine.
async fn install_engine_image<S: StorageEngine>(
    storage: &S,
    kind_scopes: &[StorageScope; ALL_KINDS.len()],
    bytes: &[u8],
) {
    let entries: Vec<ImageEntry> = match codec::decode_image(bytes) {
        Ok(e) => e,
        Err(err) => {
            tracing::warn!(?err, "undecodable raftkv snapshot image dropped");
            return;
        }
    };
    for (kind, key, value, version) in entries {
        // An unknown kind can only come from a peer that knows a row kind this
        // build does not (ALL_KINDS grew). Dropping it is the safe read: this
        // replica has no scope to put it in, and silently mis-filing it under
        // another kind would corrupt that kind's keyspace.
        let Some(scope) = kind_scopes.get(kind as usize) else {
            tracing::warn!(kind, "snapshot image entry of unknown row kind dropped");
            continue;
        };
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
    stage: Arc<Mutex<StageOutcomes>>,
    engine_applied: Arc<AtomicU64>,
    wal_lock: Arc<AsyncMutex<()>>,
    halted: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    apply_stopped: Arc<AtomicBool>,
    propose_signal: Arc<ProposeSignal>,
    metrics: MetricsHandle,
    /// The **base**-kind scope (ADR 0041 §3) — see [`RaftKvNode::scope`].
    scope: StorageScope,
    /// Every row kind's scope, for the whole-tablet snapshot image.
    kind_scopes: [StorageScope; ALL_KINDS.len()],
    stream: u64,
    hlc: Arc<Hlc>,
    committed_ceiling: Arc<AtomicU64>,
    txn_tracker: Arc<Mutex<TxnTracker>>,
}

/// The `ts` a mutating [`KvCommand`] variant carries, or `None` for `NoOp`
/// (which carries none). The one place that knows every variant's `ts`
/// field, shared by the WAL-recovery and entry-receipt witnessing sites.
fn command_ts(command: &KvCommand) -> Option<HlcTimestamp> {
    match command {
        KvCommand::Put { ts, .. }
        | KvCommand::Batch { ts, .. }
        | KvCommand::KindBatch { ts, .. }
        | KvCommand::Delete { ts, .. }
        | KvCommand::Cas { ts, .. }
        | KvCommand::Seal { ts, .. }
        | KvCommand::ReadCeiling { ts, .. }
        | KvCommand::TxnStage { ts, .. }
        | KvCommand::TxnCommit { ts, .. }
        | KvCommand::TxnAbort { ts, .. }
        | KvCommand::TxnResolve { ts, .. } => Some(*ts),
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
        stage,
        engine_applied,
        wal_lock,
        halted,
        stopped,
        apply_stopped,
        propose_signal,
        metrics,
        scope,
        kind_scopes,
        stream,
        hlc,
        committed_ceiling,
        txn_tracker,
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
    // Rebuild this group's transaction-record tracker (ADR 0018 §2/PR5) from
    // the engine's own durable records — the same "engine marker survives
    // compaction, log replay might not" reasoning as `sealed`/
    // `committed_ceiling` above: a `TxnStage`/`TxnCommit`/`TxnAbort` entry can
    // be compacted out of the log long before the record's own lifecycle is
    // done. One bounded scope scan for `txn::is_record_key` markers, the
    // accepted cost this crate already pays for `has_data`/`engine_image`.
    let rebuilt_tracker = rebuild_txn_tracker(&storage, &scope).await;
    *txn_tracker.lock().expect("txn tracker poisoned") = rebuilt_tracker;
    // Spawn the apply task now — after recovery seeded the core + `engine_applied`
    // + `sealed` + `committed_ceiling` + `txn_tracker`, so it never merges
    // against pre-recovery state.
    env.spawn_task(apply_loop(
        env.clone(),
        wal.clone(),
        Arc::clone(&core),
        storage,
        cas,
        stage,
        Arc::clone(&engine_applied),
        Arc::clone(&wal_lock),
        Arc::clone(&halted),
        apply_stopped,
        metrics.clone(),
        scope,
        kind_scopes,
        stream,
        Arc::clone(&hlc),
        sealed,
        committed_ceiling,
        txn_tracker,
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
    stage: Arc<Mutex<StageOutcomes>>,
    engine_applied: Arc<AtomicU64>,
    wal_lock: Arc<AsyncMutex<()>>,
    halted: Arc<AtomicBool>,
    apply_stopped: Arc<AtomicBool>,
    metrics: MetricsHandle,
    scope: StorageScope,
    kind_scopes: [StorageScope; ALL_KINDS.len()],
    stream: u64,
    hlc: Arc<Hlc>,
    mut sealed: Vec<(KeyRange, HlcTimestamp)>,
    committed_ceiling: Arc<AtomicU64>,
    txn_tracker: Arc<Mutex<TxnTracker>>,
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
            &stage,
            &engine_applied,
            &wal_lock,
            &halted,
            &metrics,
            &scope,
            &kind_scopes,
            stream,
            &hlc,
            &mut sealed,
            &mut max_applied_ts,
            &committed_ceiling,
            &txn_tracker,
        )
        .await;
        if !did_work {
            env.sleep(APPLY_IDLE_POLL).await;
        }
    }
}

/// **In-crate regression** for the orphan-record recovery + resurrection
/// guard (ADR 0018 §2/PR5, the corner the team lead's review of the
/// `intent_spans` fix flagged): lives here, not in `tests/txn_recovery.rs`,
/// because reproducing "a late-arriving anchor `TxnStage` for an
/// **already-known** `txn_id`" needs `pub(crate)` access
/// (`txn::record_key`, a direct `KvCommand::TxnStage` construction, and the
/// private `propose_ordered_aux`/`mint_pushed` primitives) — the public
/// `RaftKvNode::txn_stage_anchor` always **mints a fresh** `TxnId`, so it
/// cannot express "the identical, already-referenced transaction arrives
/// late"; an external integration test genuinely cannot construct this
/// scenario at all.
#[cfg(test)]
mod kind_scope_tests {
    use super::*;
    use animus_tablet::escape;

    /// A table's parent scope, as `animusd::table_scope_prefix` builds it.
    fn table_scope(name: &[u8]) -> StorageScope {
        StorageScope::new(escape(name), KeyRange::whole())
    }

    #[test]
    fn sibling_scopes_share_one_live_range() {
        let parent = table_scope(b"users");
        let base = parent.with_kind(KIND_BASE);
        let log = parent.with_kind(KIND_CHANGE);

        // Narrowing through *any* handle moves every kind: a split must never
        // leave two kinds disagreeing about what this tablet owns (ADR 0041 §3).
        let narrowed = KeyRange::new(b"m".to_vec(), Some(b"n".to_vec()));
        base.narrow(narrowed.clone());
        assert_eq!(log.range(), narrowed);
        assert_eq!(parent.range(), narrowed);
    }

    #[test]
    fn no_kind_can_read_another_kinds_key() {
        let parent = table_scope(b"users");
        let logical = b"\x01\x02logical-key".to_vec();
        for &mine in &ALL_KINDS {
            let scope = parent.with_kind(mine);
            let physical = scope.physical(&logical);
            assert_eq!(
                scope.strip_in_range(&physical),
                Some(logical.as_slice()),
                "a kind must read back its own key"
            );
            for &other in ALL_KINDS.iter().filter(|k| **k != mine) {
                assert_eq!(
                    parent.with_kind(other).strip_in_range(&physical),
                    None,
                    "kind {other:#04x} must not see kind {mine:#04x}'s key"
                );
            }
        }
    }

    #[test]
    fn one_tables_kinds_never_collide_with_another_tables() {
        let logical = b"logical".to_vec();
        let users = table_scope(b"users").with_kind(KIND_CHANGE);
        // `users2`'s raw name has `users`' as a prefix on purpose — `escape`'s
        // prefix-freedom one level up is what keeps the two tables apart, and
        // appending a kind byte must not undo it.
        let users2 = table_scope(b"users2").with_kind(KIND_CHANGE);
        assert_eq!(users2.strip_in_range(&users.physical(&logical)), None);
        assert_eq!(users.strip_in_range(&users2.physical(&logical)), None);
    }

    #[test]
    fn a_kinds_prefix_is_the_parents_plus_one_byte() {
        // The property `physical_bounds` on the parent relies on: every kind
        // lives physically *under* the parent's prefix, so a whole-tablet sweep
        // (drop-table GC, the snapshot image) can bound on the parent and then
        // sort entries out by kind.
        let parent = table_scope(b"users");
        let parent_physical = parent.physical(b"");
        for &kind in &ALL_KINDS {
            let child_physical = parent.with_kind(kind).physical(b"");
            assert!(child_physical.starts_with(&parent_physical));
            assert_eq!(child_physical.len(), parent_physical.len() + 1);
        }
    }
}

#[cfg(test)]
mod pr5_orphan_and_resurrection_tests {
    use super::*;
    use animus_env::nid;
    use animus_sim::{SimEnv, Simulator};
    use animus_storage::MemoryEngine;
    use std::sync::Mutex as StdMutex;

    type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

    fn drive<T: Send + 'static>(
        sim: &mut Simulator,
        env: &SimEnv,
        budget: Duration,
        fut: impl Future<Output = T> + Send + 'static,
    ) -> Option<T> {
        let slot: Arc<StdMutex<Option<T>>> = Arc::new(StdMutex::new(None));
        let s = Arc::clone(&slot);
        env.clone().spawn_task(async move {
            let v = fut.await;
            *s.lock().unwrap() = Some(v);
        });
        sim.run_for(budget);
        slot.lock().unwrap().take()
    }

    fn key(token: u8, tail: &[u8]) -> Vec<u8> {
        let mut k = vec![token; animus_tablet::TOKEN_BYTES];
        k.extend_from_slice(tail);
        k
    }

    /// **The full scenario the team lead's review named**: a pusher aborts
    /// a record-less orphan intent (no anchor record ever existed — the
    /// coordinator's own anchor `TxnStage` silently no-op'd, or never
    /// landed); a late-arriving genuine anchor `TxnStage` for that exact
    /// `txn_id` then no-ops against the tombstone instead of resurrecting
    /// a `Pending` record; and the coordinator's own (also-late) commit
    /// attempt converges to the same `Aborted` outcome. Final state: every
    /// intent resolved away, no zombie `Pending` anywhere, no assert.
    #[test]
    fn orphan_abort_survives_a_late_anchor_stage_and_a_late_coordinator_commit() {
        let seed = 0xA5B1_0001u64;
        let mut sim = Simulator::new(seed);
        let engine = MemoryEngine::new();
        let id_a = nid(9001);
        let id_b = nid(9011);
        let node_a: KvNode = RaftKvNode::start_scoped(
            sim.env(id_a.clone()),
            vec![id_a.clone()],
            engine.clone(),
            StorageScope::new(b"orders:".to_vec(), KeyRange::whole()),
        );
        let node_b: KvNode = RaftKvNode::start_scoped(
            sim.env(id_b.clone()),
            vec![id_b.clone()],
            engine.clone(),
            StorageScope::new(b"accounts:".to_vec(), KeyRange::whole()),
        );
        sim.run_for(Duration::from_secs(2)); // elect (single voter each)

        let ka = key(1, b":order");
        let kb = key(2, b":balance");

        // Hand-construct the transaction's identity: `TxnId`'s fields are
        // `pub`, and `txn::record_key` is `pub(crate)` — both reachable
        // here, unlike from an external integration test. This stands in
        // for "the coordinator's anchor `TxnStage` call reported success
        // (`Some((txn_id, record_key))`, since `wait_applied` only checks
        // the entry APPLIED, never that it actually wrote anything) while
        // its apply silently no-op'd" — PR4's own documented fence/seal-miss
        // gap, now applying to the anchor's own stage too.
        let anchor_token = &ka[..animus_tablet::TOKEN_BYTES];
        let txn_id = TxnId {
            ts: HlcTimestamp {
                wall_ms: 1_000,
                logical: 0,
            },
            node: id_a.clone(),
        };
        let record_key = txn::record_key(anchor_token, &txn_id);

        // The participant stages for real, referencing this txn_id/record_key
        // exactly as if the anchor's own stage had genuinely succeeded.
        let n_b = node_b.clone();
        let (txn_id_b, record_key_b) = (txn_id.clone(), record_key.clone());
        let kb_clone = kb.clone();
        let stage_ts = drive(
            &mut sim,
            node_b.env(),
            Duration::from_millis(300),
            async move {
                n_b.txn_stage_participant(
                    txn_id_b,
                    record_key_b,
                    "orders".to_string(),
                    vec![(kb_clone, Some(b"debited".to_vec()))],
                    Vec::new(),
                )
                .await
            },
        )
        .flatten();
        assert_eq!(
            stage_ts.as_ref().map(|(_, outcome)| outcome.clone()),
            Some(StageOutcome::Staged),
            "participant stage should succeed (seed={seed})"
        );

        // Recovery discovers the orphan (group A has no record at all for
        // this txn_id) and creates the abort tombstone directly.
        let n_a = node_a.clone();
        let (txn_id_o, record_key_o) = (txn_id.clone(), record_key.clone());
        let created_ts_hint = HlcTimestamp {
            wall_ms: 900,
            logical: 0,
        };
        let orphan_ts = drive(
            &mut sim,
            node_a.env(),
            Duration::from_millis(300),
            async move {
                n_a.txn_abort_orphan(txn_id_o, record_key_o, created_ts_hint)
                    .await
            },
        )
        .flatten();
        assert!(
            orphan_ts.is_some(),
            "orphan-abort tombstone proposal should be accepted (seed={seed})"
        );
        let status = drive(&mut sim, node_a.env(), Duration::from_millis(300), {
            let n = node_a.clone();
            let rk = record_key.clone();
            async move { n.txn_status_local(&rk).await }
        })
        .flatten();
        assert_eq!(
            status,
            Some(TxnDecisionStatus::Aborted),
            "the orphan-abort tombstone should be Aborted (seed={seed})"
        );

        // "The anchor stage arrives late": propose the genuine
        // `KvCommand::TxnStage` for this exact (already-decided) txn_id
        // directly — `RaftKvNode::txn_stage_anchor` cannot express this
        // (it always mints a fresh id), so this in-crate test builds the
        // command by hand via the same private primitives
        // `txn_stage_anchor` itself uses internally.
        let anchor_writes = vec![(ka.clone(), Some(b"placed".to_vec()))];
        let fence = node_a.scope_range();
        let participant_span_end = txn::immediate_successor(&kb);
        let (result, late_ts) = node_a.propose_ordered_aux(|| {
            let ts = node_a.mint_pushed(std::slice::from_ref(&ka));
            let cmd = KvCommand::TxnStage {
                txn_id: txn_id.clone(),
                record_key: record_key.clone(),
                record_table: "orders".to_string(),
                is_anchor: true,
                writes: anchor_writes.clone(),
                spans: vec![(
                    "accounts".to_string(),
                    KeyRange::new(kb.clone(), Some(participant_span_end.clone())),
                )],
                conditions: Vec::new(),
                fence: fence.clone(),
                ts,
            };
            (cmd, ts)
        });
        let late_index = match result {
            ProposeResult::Accepted { index } => index,
            other => panic!("late anchor stage proposal rejected: {other:?} (seed={seed})"),
        };
        let applied = drive(&mut sim, node_a.env(), Duration::from_millis(300), {
            let n = node_a.clone();
            async move { n.wait_applied(late_index).await }
        });
        assert_eq!(
            applied,
            Some(true),
            "the late entry itself must still apply (as a no-op) (seed={seed}, ts={late_ts:?})"
        );

        // No resurrection: the anchor's own key was never written (no
        // zombie intent), and the record is still exactly the Aborted
        // tombstone from before — never flipped back to Pending.
        let anchor_local = drive(&mut sim, node_a.env(), Duration::from_millis(300), {
            let n = node_a.clone();
            let k = ka.clone();
            async move { n.local_get(&k).await }
        })
        .flatten();
        assert_eq!(
            anchor_local, None,
            "the late anchor stage must not write the anchor's own key — the whole entry must \
             no-op against the already-decided record (seed={seed})"
        );
        let status_after_late_stage = drive(&mut sim, node_a.env(), Duration::from_millis(300), {
            let n = node_a.clone();
            let rk = record_key.clone();
            async move { n.txn_status_local(&rk).await }
        })
        .flatten();
        assert_eq!(
            status_after_late_stage,
            Some(TxnDecisionStatus::Aborted),
            "the record must still be Aborted — the late stage must never resurrect it to \
             Pending (seed={seed})"
        );

        // "The coordinator's commit": a still-live coordinator, unaware of
        // any of this, tries to commit — this must also no-op against the
        // already-Aborted record (the decision-semantics fix), never panic.
        let commit_ts = drive(&mut sim, node_a.env(), Duration::from_millis(300), {
            let n = node_a.clone();
            let txn_id = txn_id.clone();
            let record_key = record_key.clone();
            async move {
                n.txn_commit_at_least(txn_id, record_key, txn_id_hint_floor())
                    .await
            }
        })
        .flatten();
        assert!(
            commit_ts.is_some(),
            "the commit PROPOSAL itself still succeeds at the Raft level (seed={seed})"
        );
        let final_status = drive(&mut sim, node_a.env(), Duration::from_millis(300), {
            let n = node_a.clone();
            let rk = record_key.clone();
            async move { n.txn_status_local(&rk).await }
        })
        .flatten();
        assert_eq!(
            final_status,
            Some(TxnDecisionStatus::Aborted),
            "the coordinator's late commit must lose to the already-applied abort (seed={seed})"
        );

        // Resolve everywhere per the final, actual decision — no zombie
        // Pending intent survives.
        drive(&mut sim, node_b.env(), Duration::from_millis(300), {
            let n = node_b.clone();
            let txn_id = txn_id.clone();
            let record_key = record_key.clone();
            let kb = kb.clone();
            async move {
                n.txn_resolve(txn_id, record_key, vec![kb], txn::TxnOutcome::Aborted)
                    .await
            }
        });
        let b_final = drive(&mut sim, node_b.env(), Duration::from_millis(300), {
            let n = node_b.clone();
            let kb = kb.clone();
            async move { n.local_get(&kb).await }
        })
        .flatten();
        assert_eq!(
            b_final, None,
            "the participant's key must revert to its pre-transaction (absent) value — no \
             zombie Pending intent (seed={seed})"
        );
    }

    /// A ts safely below the group's current floor is fine here — this
    /// mirrors the coordinator's own candidate-ts computation
    /// (`max(anchor stage ts, every participant's acked stage ts)`); the
    /// point under test is the *decision* outcome (Aborted, no assert),
    /// never the exact ts `txn_commit_at_least` would have used — see
    /// `mint_at_least`'s own floor-enforcement doc for why a caller's
    /// candidate is always safe to pass through unmodified.
    fn txn_id_hint_floor() -> HlcTimestamp {
        HlcTimestamp {
            wall_ms: 1_000,
            logical: 0,
        }
    }
}
