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

use animus_control::persist_round::{
    self, GatedOuts, PersistArm, PersistFut, PersistProgress, PersistWake,
};
use animus_control::raft::{Out, RaftCore, RaftMsg, StateMachine};
use animus_control::{PersistedState, ProposeResult};
use animus_env::{Env, EnvExt, Metric, MetricsHandle, Nanos, NodeId, PRIMARY_STREAM};
use animus_storage::{MergeOp, StorageEngine, Version};
use animus_tablet::{KeyRange, SplitChild};
use futures::future::{Either, select};
use futures::lock::Mutex as AsyncMutex;
use futures::task::AtomicWaker;
use serde::{Deserialize, Serialize};

pub mod backup;
mod ceiling;
pub mod cluster_segment_store;
mod codec;
pub mod cursor;
pub mod hlc;
pub mod host;
mod seal;
pub mod segment;
mod split;
mod ts_cache;
mod txn;

use hlc::{Hlc, HlcTimestamp, bump_strictly_above};
use ts_cache::TsCache;
pub use txn::{StageOutcome, TxnDecisionStatus, TxnId, TxnOutcome, TxnWrite};

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

/// The **wake-on-commit** signal (ADR 0044 phase-1 PR1): the same shape as
/// [`ProposeSignal`], but for the **apply task** rather than the consensus
/// loop. `apply_loop`'s idle back-off used to be an unconditional
/// `env.sleep(APPLY_IDLE_POLL)` (5ms) every time there was nothing to merge —
/// ~200 wakeups/s per hosted group at complete idle, independent of whether
/// anything ever changed. The consensus loop now raises this signal at every
/// point that can transition "no apply work" to "apply work exists" —
/// `persist_wal`'s `mark_durable_through` call, a commit-index advance
/// observed after stepping the core (covers a follower's in-line apply on
/// `AppendEntries` and a snapshot install's `commit_index` jump alike), a
/// proposer's own commit-advancing propose (the single-node/majority-1 fast
/// path), and [`RaftKvNode::shutdown`](RaftKvNode::shutdown) (so a parked
/// apply task always notices the halt instead of waiting out the now much
/// longer [`APPLY_SAFETY_POLL`]) — so `apply_loop` races this against a long
/// safety poll instead of spinning on a short one. See the "What's
/// non-obvious" section of this crate's `CLAUDE.md` for the enumerated
/// raise points and why a signal-less path (e.g. the lazy on-demand
/// snapshot-image build `take_snapshot_needed` triggers, which is set purely
/// off the leader's own heartbeat/replicate cycle with no commit advance)
/// must still converge off the safety poll alone.
#[derive(Default)]
struct ApplySignal {
    /// Set by the consensus loop (or `shutdown`), consumed by the apply task.
    pending: AtomicBool,
    /// The apply task's waker, registered each time it parks.
    waker: AtomicWaker,
}

impl ApplySignal {
    /// Raise the flag, then wake the parked apply task. Order matters — the
    /// flag is visible before the wake, so the apply task's poll (which
    /// registers *then* checks the flag) can never miss it, mirroring
    /// [`ProposeSignal::notify`].
    fn notify(&self) {
        self.pending.store(true, Ordering::Release);
        self.waker.wake();
    }
}

/// A future that resolves once the apply task has new work to check for, for
/// `apply_loop`'s idle-backoff `select` — the [`ApplySignal`] counterpart to
/// [`ProposePending`].
struct ApplyPending<'a> {
    signal: &'a ApplySignal,
}

impl Future for ApplyPending<'_> {
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

/// The **driver-level wake** signal (ADR 0044 phase-1 PR2): the same shape as
/// [`ProposeSignal`]/[`ApplySignal`], but for the **consensus loop's own park**
/// rather than a proposer or the apply task. Today the consensus loop always
/// re-wakes well within one heartbeat/election interval regardless (continual
/// Raft traffic even at rest), so this signal is inert in practice — it exists
/// now, ahead of need, because phase-1 PR3's quiescence makes
/// [`RaftCore::next_deadline`] return `None` for real: a quiesced leader's
/// consensus loop then has **no timer at all**, parked purely on inbound
/// traffic, [`ProposePending`], and this signal — so [`shutdown`](RaftKvNode::shutdown)
/// must raise it (finding 4's hazard 1: without this, a quiesced group's
/// `shutdown()` could sit unnoticed forever instead of within one wake) and
/// [`RaftKvNode::wake`] exists as the same hook a later PR's edge/reconciler
/// proactive-wake caller (PR4) and quiescence's own un-quiesce triggers (PR3)
/// reuse. Kept distinct from `ProposeSignal` (proposer-specific: replicates a
/// freshly appended entry immediately) and `ApplySignal` (apply-task-specific)
/// rather than overloading either, since a generic "please re-evaluate, nothing
/// specific happened" wake is a different concept from either of those.
#[derive(Default)]
struct WakeSignal {
    /// Set by `shutdown`/`wake`, consumed by the consensus loop.
    pending: AtomicBool,
    /// The consensus loop's waker, registered each time it parks.
    waker: AtomicWaker,
}

impl WakeSignal {
    /// Raise the flag, then wake the parked consensus loop — same
    /// register-before-check discipline as [`ProposeSignal::notify`].
    fn notify(&self) {
        self.pending.store(true, Ordering::Release);
        self.waker.wake();
    }
}

/// A future that resolves once a driver-level wake is pending, for the
/// consensus loop's `select` — the [`WakeSignal`] counterpart to
/// [`ProposePending`]/[`ApplyPending`].
struct WakePending<'a> {
    signal: &'a WakeSignal,
}

impl Future for WakePending<'_> {
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

/// The **fork-observed** signal (ADR 0058 Train 2 rung 4 layer 1): the same
/// executor-agnostic shape as [`ProposeSignal`]/[`ApplySignal`]/[`WakeSignal`],
/// but for a caller **outside this crate** who wants to react the instant
/// this replica's own apply task durably applies `KvCommand::SplitTablet` —
/// the host reconciler's own tick trigger (`animusd::
/// tablet_host_reconciler_loop`). Before this signal existed, every replica
/// other than the parent's own campaigning leader only discovered a local
/// fork on its next scheduled reconciler tick (the `INPLACE_SPLIT_
/// RECONCILE_INTERVAL` 50ms fast-poll, ADR 0058 rung 3) — the residual the
/// rung-4 measurement addendum found: a freshly-forked child's first
/// election needs a SECOND voter's own `materialize_split_child` to run
/// before it can win a quorum, and that voter's own materialization was
/// riding its next poll rather than the fork itself. Raised exactly once per
/// fork, by the apply task, right after the durable split marker
/// (`split::split_marker_key`) commits — see `apply_and_compact`'s
/// `KvCommand::SplitTablet` arm. Never raised for any other command: a
/// caller reacting to it always finds `pending_split()` answers `Some`
/// (idempotent to check twice, exactly like every other durable fact this
/// crate publishes), so a missed or coalesced wake is never a correctness
/// gap, only a slower discovery — the periodic fallback this signal
/// shortcuts still runs unconditionally on its own schedule (see
/// `docs/engineering-lessons.md`'s "move the trigger, not the mechanism"
/// entry for the general discipline this follows).
#[derive(Default)]
struct ForkSignal {
    /// Set by the apply task, consumed by whichever external caller polls
    /// [`ForkPending`] next (typically `host::Reconciler::fork_wake`'s
    /// fan-in over every hosted tablet).
    pending: AtomicBool,
    waker: AtomicWaker,
}

impl ForkSignal {
    /// Raise the flag, then wake any parked waiter — same register-before-
    /// check discipline as [`ProposeSignal::notify`].
    fn notify(&self) {
        self.pending.store(true, Ordering::Release);
        self.waker.wake();
    }
}

/// A future that resolves once a local fork has been observed, for
/// `host::Reconciler::fork_wake`'s fan-in `select_all` — the [`ForkSignal`]
/// counterpart to [`ProposePending`]/[`ApplyPending`]/[`WakePending`].
/// Deliberately a plain, `Unpin` struct (no `Send + 'static` boxing) so
/// several of these — one per currently-hosted tablet — can be raced
/// together with zero heap allocation.
struct ForkPending<'a> {
    signal: &'a ForkSignal,
}

impl Future for ForkPending<'_> {
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
/// this tablet's own **private** `StorageEngine` (ADR 0050 Train B): every
/// physical key this group ever writes is `[kind] || logical_key` (F2b) — no
/// table prefix, no tablet identity in the bytes, because a private engine
/// holds exactly one tablet of exactly one table (identity lives in the
/// engine's file namespace, `host::EngineFactory`). What survives here is
/// **kind scoping** (ADR 0041 §3): a group owns one scope per row kind, and a
/// kind scope's `prefix` is its single kind byte.
///
/// `range` is the tablet's **declared** [`KeyRange`] — **immutable from
/// birth** (ADR 0050: a tablet's range never changes; a split builds new
/// tablets rather than narrowing this one). It no longer bounds any physical
/// access — the engine itself is the boundary — and is retained only as the
/// group's own copy of its `Metadata`-declared range for the pre-propose
/// routing check ([`RaftKvNode::scope_range`]) and the fence stamped on
/// proposals (both scheduled for the Train B deletion sweep once the
/// route-time `Active` filter replaces them).
///
/// Engine-global marker keys (`seal.rs`/`ceiling.rs`/the syskv namespace)
/// stay structurally disjoint from every kind scope: they lead with
/// `escape(RESERVED_NAMESPACE)`'s first byte (`b'_'` = `0x5F`), while a kind
/// scope's keys lead with a kind byte (`0x00..=0x04`) — see
/// `seal.rs`'s disjointness tests.
#[derive(Clone, Debug)]
pub struct StorageScope {
    prefix: Vec<u8>,
    range: KeyRange,
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

/// A single derived `(row kind, logical key, value)` write — `None` value
/// means a tombstone. The element `KvCommand::KindBatch::writes` and (ADR
/// 0046 A1) `txn::TxnWrite::kind_writes` share; named once here so every
/// function that handles this shape (`materialize_derived`, the codec, the
/// apply-time token check) names it the same way instead of repeating the
/// tuple.
pub type KindWrite = (u8, Vec<u8>, Option<Vec<u8>>);

/// The sibling scope set a tablet group owns, derived from its **parent**
/// scope (this tablet's immutable declared range; F2b — no table prefix),
/// indexed by kind selector. Each entry's physical prefix is its one kind
/// byte ([`StorageScope::with_kind`]).
fn kind_scopes(parent: &StorageScope) -> [StorageScope; ALL_KINDS.len()] {
    ALL_KINDS.map(|kind| parent.with_kind(kind))
}

impl StorageScope {
    /// The whole keyspace — the default for every plain constructor and for
    /// tests that don't model a declared range. (Post-F2b this is **not** an
    /// identity transform: the group's kind scopes still prefix their kind
    /// byte.)
    #[must_use]
    pub fn whole() -> Self {
        Self::new(KeyRange::whole())
    }

    /// The parent scope of one tablet's private engine, carrying its
    /// **immutable** declared `range`. Kind scopes derive from it via
    /// [`with_kind`](Self::with_kind); the parent itself has no prefix and is
    /// never used for physical access directly.
    #[must_use]
    pub fn new(range: KeyRange) -> Self {
        Self {
            prefix: Vec::new(),
            range,
        }
    }

    /// A **sibling scope of the same tablet group**, holding a different row
    /// kind (ADR 0041 §3): the prefix extended by `kind`, over a clone of the
    /// same immutable declared range.
    ///
    /// Every kind of one tablet is `[kind] || logical`, so two kinds differ
    /// in their lead byte and neither prefixes the other. The kind byte is
    /// what lets the kinds share one engine without a discriminator inside
    /// the logical key — which they must, because [`RaftKvNode::txn_stage`]
    /// asserts a logical key leads with the ADR 0022 partition token and
    /// derives every transaction intent span from it.
    #[must_use]
    pub fn with_kind(&self, kind: u8) -> Self {
        let mut prefix = self.prefix.clone();
        prefix.push(kind);
        Self {
            prefix,
            range: self.range.clone(),
        }
    }

    /// This tablet's declared range — immutable from construction (ADR 0050;
    /// the live-narrowable range died with the zero-copy split). Read by the
    /// pre-propose routing check ([`RaftKvNode::scope_range`]) and stamped as
    /// the fence on proposals; both are inert-but-present until the Train B
    /// deletion sweep.
    #[must_use]
    pub fn range(&self) -> KeyRange {
        self.range.clone()
    }

    /// The physical storage key for logical `key`: `prefix || key` — post-F2b
    /// the prefix is exactly this scope's kind byte.
    fn physical(&self, key: &[u8]) -> Vec<u8> {
        let mut out = self.prefix.clone();
        out.extend_from_slice(key);
        out
    }

    /// If `physical_key` belongs to this kind scope — starts with its kind
    /// byte — the stripped logical key, else `None`. The read-side
    /// counterpart of [`physical`](Self::physical). (The pre-F2b range check
    /// is gone: a private engine physically cannot hold another tablet's
    /// rows, and the declared range is immutable, so prefix membership alone
    /// decides. Engine-global marker keys lead `0x5F` and never match a kind
    /// byte.)
    fn strip_in_range<'a>(&self, physical_key: &'a [u8]) -> Option<&'a [u8]> {
        physical_key.strip_prefix(self.prefix.as_slice())
    }

    /// Whether `storage` currently holds any live data in this kind scope.
    ///
    /// "Does this tablet already have data" is what distinguishes a node
    /// **reforming** a group it already hosted before a restart (start with
    /// the full voter config — it may need to elect immediately) from one
    /// **joining fresh** as a reconciler-placed spare (start as a quiet
    /// non-voter). Post-F2b the engine is private and the kind prefix's
    /// physical bounds are always finite (`[kind] .. [kind + 1]`), so this is
    /// one bounded scan — the pre-B2 whole-engine `entries()` fallback for an
    /// unbounded range is gone.
    #[must_use]
    pub async fn has_data<S: StorageEngine>(&self, storage: &S) -> bool {
        // ADR 0018 §2/PR3: a txn-record marker key (`txn::is_record_key`)
        // is internal bookkeeping, not user data — a tablet holding only a
        // record (an in-flight transaction, no other writes ever landed)
        // must still read as "no data" for the reforming-vs-fresh-join
        // decision this presence check exists for.
        let (start, end) = self.physical_bounds();
        let rows = match end {
            Some(end) => storage.scan(&start, &end).await,
            // Only `StorageScope::whole()`'s parent scope (no prefix at all)
            // has no finite bound; a kind scope's never hits this.
            None => storage.entries().await,
        };
        rows.map(|rows| {
            rows.iter().any(|(k, _)| {
                self.strip_in_range(k)
                    .is_some_and(|logical| !txn::is_record_key(logical))
            })
        })
        .unwrap_or(false)
    }

    /// This kind scope's own physical `(start, end)` bounds, for every
    /// caller that needs a **bounded** physical range even when the declared
    /// logical range is unbounded above (the common "one big not-yet-split
    /// tablet" case).
    ///
    /// `start` is always `physical(range.start)`. `end` is `physical(end)`
    /// when the logical range has one; when it doesn't, it is the **prefix
    /// upper bound** — post-F2b simply `[kind + 1]`, always finite for a
    /// kind scope (kind bytes top out at `0x04`). Only the un-prefixed
    /// parent scope yields `end: None`.
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
    ///
    /// **`conditions` (ADR 0046 "evaluate at leader" seatbelt, modeled on
    /// [`TxnStage`](Self::TxnStage)'s own `conditions` field)**: `(key,
    /// expected)` pairs — `expected: Some(bytes)` means `key`'s current
    /// *committed* value (envelope-unwrapped, the same read discipline `Cas`/
    /// `TxnStage` use) must equal `bytes` exactly; `None` means it must be
    /// absent. Byte-level OCC, not a rich expression, exactly like
    /// `TxnStage.conditions` — a caller (`animusd`'s leader-side write
    /// evaluator) compiles its own richer condition against a pre-read down to
    /// "the value must still be exactly what I read" before it ever reaches
    /// here. Unlike `TxnStage`, a `KindBatch` condition failure has **no**
    /// outcome-introspection channel — a condition-failed entry no-ops
    /// silently, indistinguishable from a fence/seal miss, deliberately (the
    /// existing generic-error/probe-poll-timeout contract every
    /// `put_kind_batch_fenced` caller already has to handle) — so this field
    /// is checked once, **before** the fence/seal gate rather than gated
    /// behind it: there is no reporting-priority reason (no `StageOutcome`
    /// analogue to disambiguate) to skip the read when the entry would fence
    /// out anyway. This field has **no production caller as of this PR** —
    /// it lands ahead of its first real use (`animusd`'s leader-side
    /// evaluate-then-propose write path, ADR 0046 U3) as the seatbelt against
    /// a concurrent `TxnStage`/`TxnResolve` commit landing between that
    /// evaluator's own-key read and its own propose call: real today (every
    /// `rmw_lock` use lives in edge handlers, never `txn_resolver_loop`) but
    /// unreachable until a future transaction stack can target an
    /// indexed/streamed table (transactions are rejected on those tables
    /// today).
    KindBatch {
        /// `(row kind, logical key, value)` — `None` writes a tombstone.
        writes: Vec<KindWrite>,
        /// **Change-log records** to append in the same entry, each a
        /// `(key prefix, encoded record)` pair (empty = none).
        ///
        /// Each key is completed at **apply** as `prefix || hlc::pack(ts)`, using
        /// this entry's own commit timestamp, and it lands in the
        /// [`KIND_CHANGE`] scope. The proposer deliberately cannot supply that
        /// suffix: `ts` is minted inside `propose_ordered` and is the only
        /// timestamp that agrees with the entry's commit order, so letting an
        /// edge guess it would silently break the ordering the log exists to
        /// provide (ADR 0041 §4a — DynamoDB Streams reads these in commit
        /// order). Making it structural also means the record can never be
        /// keyed inconsistently across replicas. **A `Vec`, not an `Option`,
        /// since ADR 0049's Train A rung-1 fixup**: a marker-table
        /// `BatchWriteItem` commits one entry per tablet carrying every
        /// item's base row *and* every item's marker record — the
        /// entry-granularity throughput contract the plain `Batch` path had
        /// (one entry per tablet, one WAL record, one apply), which per-item
        /// `KindBatch` proposals were measured to break (the
        /// `backfill_seeder` populate-then-backfill regression). Records in
        /// one entry share the entry's `ts`; their prefixes differ per item
        /// (`token || escape(pk)`), so the completed keys stay distinct for
        /// distinct items.
        change_log: Vec<(Vec<u8>, Vec<u8>)>,
        conditions: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        ts: HlcTimestamp,
    },
    /// **Split-build seed batch** (ADR 0050 Train B rung 4, fork F3): a chunk
    /// of the parent tablet's rows proposed into a **child** group's own log
    /// by the split-build driver, applied as **version-carrying merges** —
    /// the `install_engine_image` semantics as a proposable command. Each row
    /// is `(kind index, logical key, stored bytes or tombstone, MVCC
    /// version)`; the stored bytes are the parent engine's value **verbatim**
    /// (envelope tag included — a staged intent copies as an intent, fork
    /// F7), and apply merges at the **carried** version, never this entry's
    /// own `ts`, so re-proposing a chunk is an idempotent no-op and a row
    /// updated on the parent mid-build wins by per-key LWW on the child too.
    ///
    /// Deliberately emits **nothing** into the child's change log — this is
    /// history transfer, not new mutation (a stream consumer already saw
    /// these records in the parent's own shards; re-emitting them is the
    /// #220 duplication class by construction). `fence` follows the standard
    /// whole-batch convention (`Batch`'s all-or-nothing): every logical key
    /// must fall inside the child's immutable declared range — the driver
    /// filters by construction, so a violation is a driver bug surfacing as
    /// a loud no-op, never a partial install. Apply additionally
    /// **witnesses** the batch's highest carried version into the group's
    /// HLC (the snapshot-install discipline) so a child leader's own future
    /// mints strictly exceed every copied row.
    SeedBatch {
        /// Raw engine rows, opaque to this command — see [`SeedRow`].
        rows: Vec<SeedRow>,
        ts: HlcTimestamp,
    },
    /// Remove `key` (a tombstone in the engine), iff `key` falls inside `fence`.
    Delete { key: Vec<u8>, ts: HlcTimestamp },
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
        ts: HlcTimestamp,
    },
    /// **Split-cutover freeze** (ADR 0050 Train B rung 5, stage 3): the
    /// terminal whole-range close of a split parent, proposed
    /// into the **parent's** own log by the split-build driver once the
    /// build has converged. After this entry applies, the parent group
    /// rejects every later-ordered mutating command — its apply pushes
    /// `(KeyRange::whole(), ts)` into the same in-memory sealed set every
    /// fence+seal apply gate already consults (zero new per-command gating
    /// code), persists the identical durable seal marker (`seal.rs`) so a
    /// restarted/compacted group stays frozen, and latches the driver-level
    /// `frozen` flag the propose-side refusals read
    /// ([`RaftKvNode::is_frozen`]). Linearizable reads keep serving — the
    /// frozen state IS current, since nothing anywhere accepts writes for
    /// the range until cutover activates the children. The freeze's own log
    /// position defines the final state the build's final tail pass ships.
    /// Idempotent: a duplicate `Freeze` on an already-frozen group applies
    /// as a no-op (no second marker write, no second sealed-set entry). No
    /// `fence`, exactly like `Seal` — it never touches user data and is
    /// itself the authority tightening what future entries may do.
    ///
    /// **What stays proposable on a frozen group**: `ReadCeiling` (the read
    /// path's own liveness — reads keep serving), `Seal` (no production
    /// caller), `NoOp`, and `Freeze` itself. `TxnCommit`/`TxnAbort` are
    /// refused at propose time but an entry already appended in the
    /// freeze's own append-to-apply sliver applies normally (harmless: the
    /// decision's resolves carry the outcome explicitly, and the child's
    /// copied record is decided post-cutover by the coordinator's own
    /// retry — see rung 5's e2e).
    Freeze { ts: HlcTimestamp },
    /// **In-place split fork** (ADR 0058 Train 2 rung 3, Stage 3): the
    /// single-entry atomic mint that forks this parent group into two
    /// children, proposed into the **parent's own log** by the parent's
    /// leader once every learner added for the split (the union of both
    /// children's placement-chosen homes, added by the host reconciler once
    /// the control plane records the in-place split intent — see
    /// `animus_tablet::InPlaceSplitIntent`) has caught up.
    ///
    /// At apply, on **every** replica (voter and learner alike — this is
    /// why Stage 2 catches every learner up before this is ever proposed),
    /// this entry is simultaneously:
    ///
    /// - **The ordering fence**: it pushes `(KeyRange::whole(), ts)` into
    ///   the SAME sealed-range set + `frozen` latch [`KvCommand::Freeze`]'s
    ///   apply arm uses (this crate's existing apply-time seal discipline,
    ///   reused verbatim, not reimplemented) — no subsequent entry in the
    ///   parent's own log describes state either child inherits. A
    ///   later-ordered user mutation is rejected exactly like a frozen
    ///   copy-based parent's would be (`RaftKvNode::is_frozen`); the two
    ///   workflows never coexist on one tablet (selected by the operator's
    ///   split-mode flag), so sharing the flag is safe.
    /// - **The durable fork marker** (`split.rs`, mirroring `seal.rs`'s
    ///   marker discipline): `split_key` and both `children` are merged
    ///   into an engine-global key so a restart — even after log
    ///   compaction — can rediscover the fork (`RaftKvNode::pending_split`).
    ///   Idempotent: a duplicate apply over an already-forked group (a
    ///   WAL-replay re-application) writes nothing a second time, mirroring
    ///   `Freeze`'s own idempotent-if-already-frozen check.
    ///
    /// **What this entry's apply does NOT do**: materialize the two
    /// children's engines, or bootstrap their Raft groups. That
    /// higher-level work — the SSTable-level clone-then-trim and the
    /// group-mint-at-apply proper — happens **outside** this sync,
    /// I/O-free apply path, in the per-node host reconciler
    /// (`host::Reconciler`, ADR 0031), which discovers a pending fork via
    /// [`RaftKvNode::pending_split`] the same way it discovers every other
    /// hosted-tablet fact. This mirrors this crate's existing
    /// sync-core/async-driver split (the core agrees order; a driver does
    /// the I/O) one level up, applied to the whole tablet lifecycle instead
    /// of one Raft group — see `host.rs`'s own doc.
    SplitTablet {
        /// The key the parent's range splits at (left half first).
        split_key: Vec<u8>,
        /// Exactly two children, left half first — the SAME
        /// `(id, replicas)` pairs the control plane's
        /// `MetaCommand::BeginSplitInPlace` recorded, so every replica
        /// derives identical child configs from identical inputs (this
        /// entry's own `children` field plus the parent's own Raft
        /// membership at this log position needs no further coordination).
        children: [SplitChild; 2],
        ts: HlcTimestamp,
    },
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
    ///
    /// **ADR 0046 A1 ("materialize-at-resolve")**: each `writes` entry is a
    /// [`txn::TxnWrite`], carrying not just `key`/`value` but also an
    /// optional derived kind-scope payload (`kind_writes`/`change_log`) —
    /// evaluated at THIS participant's own leader at stage time (ADR 0046
    /// Decision 1, U3). The payload rides inside the write's own
    /// [`txn::Envelope::Intent`], opaque until `TxnResolve`'s commit branch
    /// materializes it at its own resolve ts via the shared
    /// `materialize_derived` helper — see `TxnWrite`'s doc for the full
    /// argument (and why intent-staging a kind scope directly, ADR 0046
    /// Decision 2, is rejected). Apply validates every `kind_writes` key
    /// leads with its own write's `key`'s partition token (ADR 0022) —
    /// a validated rejection (folded into this stage's structural `Fenced`
    /// outcome), never an `assert!`, since this payload is wire-reachable
    /// (via `ClientRequest::TxnPrepare`).
    TxnStage {
        txn_id: TxnId,
        record_key: Vec<u8>,
        record_table: String,
        is_anchor: bool,
        writes: Vec<txn::TxnWrite>,
        spans: Vec<(String, KeyRange)>,
        conditions: Vec<(Vec<u8>, Option<Vec<u8>>)>,
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
    /// idempotent on WAL replay.
    ///
    /// **`fence` (ADR 0018 §2 write-loss amendment — Bug 3).** Originally
    /// this variant carried none, on the theory that "every key here was
    /// already fence-checked at `TxnStage` time." That reasoning has a
    /// gap: it assumes `keys` can only ever be a set this exact tablet
    /// already staged — true for every *in-crate* caller, but not
    /// something this type enforces, and `animusd`'s own coordinator
    /// found the counterexample. `ClientCtx::recovery_resolve` used to
    /// group a transaction's participants by table name alone (no tablet
    /// dimension), so a split table's two different tablets' keys could
    /// end up bundled into one `TxnResolve` proposed against whichever
    /// tablet the *first* key in the bundle belonged to. With no fence
    /// here, that tablet applied the resolve for a key it doesn't own —
    /// onto the *same physical key* another tablet of the same table
    /// separately maintains (ADR 0028: a table's tablets share one
    /// `StorageScope` prefix) — stamped with the wrong tablet's own clock.
    /// The owning tablet's own clock never learns of that foreign version
    /// and can never mint above it again: every future write to that key
    /// silently loses the per-key LWW race in `StorageEngine::merge`,
    /// forever (the acked-write-loss symptom this amendment fixes). The
    /// coordinator-side grouping bug is fixed at the source, but `fence`
    /// here is the structural seatbelt: **identical semantics to
    /// `TxnStage`'s own fence** (stamped from the proposing group's live
    /// `scope_range()`, checked whole-or-nothing against every key in
    /// `keys` at apply, rejecting the whole entry rather than partially
    /// applying it) — so a caller that makes the identical mistake again,
    /// today or in the future, is rejected here instead of silently
    /// corrupting a foreign key.
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

/// [`RaftKvNode::reconfigure_step`]'s promotion-readiness threshold (ADR 0058
/// Train 1's reconciler adoption): a learner within this many log entries of
/// the leader's own `last_log_index()` (see
/// [`RaftKvNode::learner_caught_up`]) is judged safe to promote — closing the
/// remaining gap costs one more `AppendEntries` round, not a meaningfully
/// long unsafe window. Same order of magnitude as the fixed thresholds the
/// Train 1 test corpora already use (`animus-control/tests/learner_corpus.rs`,
/// this crate's `tests/learner_membership.rs`) — not derived from either, since
/// this one gates a *production* decision rather than a test assertion.
const RECONFIGURE_LEARNER_CATCH_UP_THRESHOLD: u64 = 4;

/// Leader-side ReadIndex state: per in-flight read `epoch`, the term it was issued
/// under and the set of peers (including self) that have confirmed leadership.
#[derive(Default)]
struct ReadState {
    next_epoch: u64,
    /// `epoch -> (term, acking nodes)`.
    pending: BTreeMap<u64, (u64, BTreeSet<NodeId>)>,
}

/// Per-CAS outcomes recorded at apply time, keyed by the entry's **Raft log
/// index** (the value [`ProposeResult::Accepted`] hands the proposer) and
/// paired with the entry's own **term** — `true` if the swap happened,
/// `false` if `expected` did not match. Every replica records the identical
/// value because the CAS is decided deterministically in commit order
/// against the same committed engine state. The proposer polls until the
/// entry is applied, then reads its index here (see
/// [`RaftKvNode::cas_result`] / [`RaftKvNode::compare_and_swap`]).
///
/// **The term is load-bearing, not incidental** (mirrors
/// [`KindBatchOutcomes`]' own doc): an entry this proposer appended but
/// never committed (a leadership change truncated it) can have its log
/// position reoccupied by a *different* `Cas`, which then applies and
/// records an outcome at the identical index — index alone cannot tell that
/// apart from the proposer's own entry having applied. `cas_result` requires
/// the caller's own accepted term to match before ever returning the
/// recorded outcome as this proposer's own.
#[derive(Default)]
struct CasResults {
    outcomes: BTreeMap<u64, (u64, bool)>,
}

/// What a `KindBatch` entry actually did at apply time — the introspection
/// channel `TxnStage` has via [`StageOutcome`] and `Cas` via
/// [`RaftKvNode::cas_result`], which `KindBatch` lacked.
///
/// Without it a proposer could only confirm a write by reading the key back and
/// comparing values, which cannot distinguish **"my entry no-op'd"** from
/// **"my entry applied and a concurrent write then overwrote it"**. The second
/// is a success, and reporting it as a failure is not harmless: measured, ten
/// concurrent `PutItem`s to one key produced six spurious "superseded" errors.
/// For an idempotent write the caller's retry hides that; for a non-idempotent
/// one (a numeric `ADD`) retrying is precisely what corrupts the value.
///
/// Every replica records the identical outcome — the decision is deterministic
/// in commit order against the same committed engine state — exactly as
/// [`CasResults`] and [`StageOutcomes`] already are.
///
/// **`Applied` says "this Raft index applied," never "my content landed"
/// (ADR 0018 §2's `TxnStage`/`StageOutcome` doctrine, and the incident that
/// made `KindBatchOutcome` follow it too).** An entry this proposer appended
/// but never committed (a leadership change truncated it) can have its log
/// position reoccupied by a *different* command, which then applies and
/// records `Applied` at the identical index — index alone cannot tell that
/// apart from the proposer's own entry having applied. [`KindBatchOutcomes`]
/// additionally keys every record on the entry's own Raft **term**
/// (mirroring [`ProposeResult::Accepted`]'s `term`), so a caller can check
/// index **and** term together — Raft's log-matching property makes that
/// pair a sound proof of entry identity — before ever trusting `Applied` as a
/// confirm. See [`RaftKvNode::kind_batch_outcome`]'s doc for the accessor
/// that exposes the recorded term.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KindBatchOutcome {
    /// The batch's writes materialized.
    Applied,
    /// A key in `conditions` did not hold the expected committed value (or
    /// expected absence) — the whole batch no-op'd. `key` is the first
    /// condition that failed, in `conditions`' own order. The caller's cue to
    /// re-read and re-evaluate: an ordinary OCC round, **not** an ambiguous
    /// outcome. (No term check is needed to trust this variant: a no-op
    /// writes nothing regardless of whose entry occupies the index, so it is
    /// always a sound "nothing landed here" signal.)
    ConditionFailed { key: Vec<u8> },
    /// The batch carried user data into a **sealed** range (ADR 0050's split
    /// freeze) and was vetoed whole. The caller's cue to re-route to the
    /// child, not to retry here. Same term-independent soundness as
    /// `ConditionFailed`.
    Sealed { key: Vec<u8> },
}

/// Per-`KindBatch` outcomes recorded at apply time, keyed by the entry's Raft
/// log index, each paired with the entry's own **term** — mirroring
/// [`CasResults`]/[`StageOutcomes`] plus the term identity check
/// [`KindBatchOutcome`]'s doc explains.
///
/// **Bounded, unlike those two.** A `KindBatch` is proposed for *every*
/// indexed or streamed write, not just for a CAS or a transaction, so an
/// unpruned map would grow without limit on a busy table. Entries are only
/// useful to a proposer polling within `CLIENT_TIMEOUT`, so old ones are
/// dropped; a proposer that finds no record (or a term mismatch) falls back
/// to the value probe, which is exactly today's behaviour.
#[derive(Default)]
struct KindBatchOutcomes {
    outcomes: BTreeMap<u64, (u64, KindBatchOutcome)>,
}

impl KindBatchOutcomes {
    /// Indices retained behind the newest. Generous next to the poll window
    /// (`CLIENT_TIMEOUT`) while keeping the map small enough to be free.
    const RETAIN: u64 = 8192;

    fn record(&mut self, index: u64, term: u64, outcome: KindBatchOutcome) {
        self.outcomes.insert(index, (term, outcome));
        // Prune in batches rather than on every insert: `split_off` allocates,
        // and doing it once per 8192 writes is not worth measuring.
        if self.outcomes.len() > (Self::RETAIN as usize) * 2 {
            let cutoff = index.saturating_sub(Self::RETAIN);
            self.outcomes = self.outcomes.split_off(&cutoff);
        }
    }
}

/// Per-`TxnStage` outcomes recorded at apply time, keyed by the entry's
/// **Raft log index** and paired with the entry's own **term** — the
/// [`StageOutcome`] introspection primitive (ADR 0018 §2 apply-time
/// write-key conditions amendment), mirroring [`CasResults`]/
/// [`KindBatchOutcomes`] exactly: every replica records the identical value
/// (the stage is decided deterministically in commit order against the same
/// committed engine state), and a proposer polls until its entry applies,
/// then reads its index here (see
/// [`RaftKvNode::stage_outcome`]/[`RaftKvNode::txn_stage_anchor`]/
/// [`RaftKvNode::txn_stage_participant`]).
///
/// **The term is load-bearing, not incidental** — same reasoning as
/// [`CasResults`]/[`KindBatchOutcomes`]: a truncated, never-committed
/// `TxnStage` entry's log position can be reoccupied by a different command
/// after a leadership change, which then applies and records an outcome at
/// the identical index. `stage_outcome`/`wait_stage_outcome` require the
/// caller's own accepted term to match before ever returning the recorded
/// outcome as this proposer's own.
#[derive(Default)]
struct StageOutcomes {
    outcomes: BTreeMap<u64, (u64, StageOutcome)>,
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
    /// `physical_key -> txn_id`: the transaction whose intent at this key
    /// was most recently resolved (`TxnResolve`, commit or abort alike) on
    /// THIS group — populated for every participant that applies a
    /// resolve, anchor or not, unlike `pending`/`unresolved_decided` above
    /// (which are anchor-only by construction). A **defensive seatbelt**
    /// against issue #298 shape A: `KvCommand::TxnStage`'s own `blocked_by`
    /// check only ever rejects overwriting a *different* transaction's
    /// still-live `Intent` — it has no way to reject a stage that arrives
    /// for a key AFTER this exact transaction's own resolve already ran,
    /// since by then the key is a plain `Committed`/restored value, not an
    /// `Intent` at all. `KvCommand::TxnStage`'s apply arm checks this map
    /// by **(key, txn_id) identity**, never presence alone — the same
    /// discipline `KindBatchOutcome`'s own false-ack fix established for
    /// "never trust an outcome without confirming it names the SAME
    /// thing." Bounded (`record_resolution`'s own `RETAIN`), like
    /// `KindBatchOutcomes`/`StageOutcomes`: purely a best-effort catch, not
    /// a source of truth — an evicted (or, after a restart, simply never
    /// rebuilt) entry only means this ONE seatbelt doesn't catch a given
    /// resurrection attempt, never that a wrong decision gets made from a
    /// stale one. **Deliberately not rebuilt at group start**
    /// (`rebuild_txn_tracker`, below) — unlike `pending`/`unresolved_
    /// decided`, which restore real transaction-lifecycle facts a restart
    /// must not lose, this map's whole job is catching a stale stage that
    /// arrives shortly after its own resolve within the SAME uptime window;
    /// starting it empty after a restart is exactly as safe as any other
    /// eviction.
    recently_resolved: BTreeMap<Vec<u8>, TxnId>,
    /// Insertion order for `recently_resolved`'s bounded FIFO eviction —
    /// see `record_resolution`.
    recently_resolved_order: std::collections::VecDeque<(Vec<u8>, TxnId)>,
}

impl TxnTracker {
    /// Entries retained behind the newest, mirroring `KindBatchOutcomes::
    /// RETAIN`'s own "generous next to the realistic poll/retry window,
    /// while keeping the map small enough to be free" reasoning — a stale
    /// re-stage this seatbelt exists to catch arrives within a bounded
    /// retry/timeout window (seconds), never after thousands of intervening
    /// resolves on the same busy tablet.
    const RECENTLY_RESOLVED_RETAIN: usize = 4096;

    /// Record that `txn_id`'s intent at `physical_key` was just resolved on
    /// this group — see `recently_resolved`'s own doc.
    fn record_resolution(&mut self, physical_key: Vec<u8>, txn_id: TxnId) {
        self.recently_resolved
            .insert(physical_key.clone(), txn_id.clone());
        self.recently_resolved_order
            .push_back((physical_key, txn_id));
        while self.recently_resolved_order.len() > Self::RECENTLY_RESOLVED_RETAIN {
            let Some((old_key, old_txn_id)) = self.recently_resolved_order.pop_front() else {
                break;
            };
            // Only remove if it's still THIS eviction's own entry — a
            // later resolution of the same key (a different, newer
            // txn_id) must never be evicted by an older entry's turn.
            if self.recently_resolved.get(&old_key) == Some(&old_txn_id) {
                self.recently_resolved.remove(&old_key);
            }
        }
    }
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
/// read (ADR 0018 §2/PR3, extended PR4, unified PR2 of the
/// torn-pair-fix stack): either a value has been determined (`Value`,
/// `Some` present / `None` absent); the covering transaction's **local**
/// record is `Pending` (this same tablet holds the record — the
/// single-participant/anchor case — but it hasn't decided yet); or the
/// record could not be found in this tablet's own scope at all (`Foreign`
/// — either it genuinely lives on another tablet, ADR 0018 §2/PR4's
/// multi-participant case, or the anchor's own stage just hasn't applied
/// here yet) — see `RaftKvNode::resolve_once_step`'s doc for the exact
/// per-status rules.
///
/// **`Pending` and `Foreign` carry the identical [`IntentInfo`] payload**
/// (the torn-pair-fix stack's ADR 0018 §2 amendment) — a caller with no
/// cross-tablet resolver treats them identically (both are "can't resolve
/// locally, retry"), but a caller that *can* chase an intent down (a status
/// query + push, ADR 0018 §2/PR4 §3) now runs the exact same recipe for
/// either outcome: `record_table`/`record_key` route to the record's actual
/// owner transparently, whether that happens to be this same tablet (the
/// `Pending` case) or a different one (`Foreign`). See
/// [`linearizable_get_served_fast`](RaftKvNode::linearizable_get_served_fast)'s
/// doc and `animusd::ClientCtx::cp_get_local_resolving`/
/// `cp_get_local_snapshot`, the two callers that act on this.
enum ResolveStep {
    Value(Option<Vec<u8>>),
    Pending(IntentInfo),
    Foreign(IntentInfo),
}

/// Everything a caller needs to chase down an intent's covering transaction
/// (ADR 0018 §2/PR4, generalized to the local case by the torn-pair-fix
/// stack's ADR 0018 §2 amendment): the transaction's identity, its record's
/// logical key, and the **table** whose tablet ring owns that key (a record
/// key alone doesn't identify a table — see `txn::Envelope::Intent`'s doc).
/// **Not always "another tablet"** despite the historical name of the
/// `Foreign` variant that first introduced this struct — [`ResolveStep::
/// Pending`] carries the identical shape for a record this *same* tablet
/// already holds (the single-participant/anchor case), so a caller can
/// route `record_table`/`record_key` through its ordinary cross-tablet
/// lookup (e.g. `animusd`'s `ClientCtx::cp_route`) uniformly either way —
/// it transparently resolves back to this tablet when the record happens to
/// be local. Consumed by a coordinator (`animusd`) that routes a
/// `ClientRequest::TxnStatus` to `record_table`/`record_key`'s owning
/// tablet, then calls
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
    /// ([`RaftKvNode::linearizable_get_served`], correct for a genuinely
    /// single-key read), or — since the torn-pair-fix stack's ADR 0018 §2
    /// amendment gave this variant the same [`IntentInfo`] payload
    /// `Foreign` carries — treat it identically to `Foreign` via a status
    /// query + push + resolve, never blocking (the design a
    /// `TransactGetItems` quiescent round needs: see
    /// `animusd::ClientCtx::cp_get_local_snapshot`).
    Pending(IntentInfo),
    /// The intent's record could not be found in this tablet's own scope —
    /// see [`IntentInfo`]'s doc for how a caller resolves it. Carries the
    /// same payload shape as `Pending` above, by design.
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
    /// Per-`KindBatch` apply-time outcomes — see [`KindBatchOutcomes`]'s doc.
    kind_outcomes: Arc<Mutex<KindBatchOutcomes>>,
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
    /// **Wake-on-commit** signal (ADR 0044 phase-1 PR1): raised by the consensus
    /// loop and by [`shutdown`](Self::shutdown) so the apply task's idle back-off
    /// races this against [`APPLY_SAFETY_POLL`] instead of spinning on
    /// `APPLY_IDLE_POLL`. See [`ApplySignal`]'s doc for the enumerated raise points.
    apply_signal: Arc<ApplySignal>,
    /// **Driver-level wake** signal (ADR 0044 phase-1 PR2): raised by
    /// [`shutdown`](Self::shutdown) and [`wake`](Self::wake) so the consensus
    /// loop's park races this alongside `propose_signal`/inbound traffic/the
    /// timer arm — the hook phase-1 PR3's quiescence (a timerless park) and
    /// PR4's proactive external wake both need. See [`WakeSignal`]'s doc.
    wake_signal: Arc<WakeSignal>,
    /// **Fork-observed** signal (ADR 0058 Train 2 rung 4 layer 1): raised by
    /// the apply task the instant it durably applies `KvCommand::
    /// SplitTablet`, so `host::Reconciler::fork_wake` can wake the moment a
    /// LOCAL fork happens instead of on this replica's next scheduled tick.
    /// See [`ForkSignal`]'s doc.
    fork_signal: Arc<ForkSignal>,
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
    /// construction performs — at group start, off this tablet's own
    /// engine's `latest_version()`) plus, for the residual in-flight-write
    /// race witnessing alone can't close, the **range seal** (`seal.rs`).
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
    /// ADR 0050 rung 5: whether this group has applied its split-cutover
    /// [`KvCommand::Freeze`] — set by the apply arm, re-latched at group
    /// start from the whole-range seal marker, read by
    /// [`is_frozen`](Self::is_frozen) (the pre-propose refusal `animusd`'s
    /// write/txn helpers consult). Never cleared: a frozen parent's only
    /// future is retirement at cutover.
    frozen: Arc<AtomicBool>,
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
    /// [`propose_freeze`](Self::propose_freeze)'s bare mint) must additionally
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
    /// The Raft **term** at which [`mint_pushed`](Self::mint_pushed) last
    /// absorbed `committed_ceiling()` into [`ts_cache`](Self::ts_cache)'s
    /// `low_water` (ADR 0018 §2 amendment — the `mint_pushed`
    /// clock-witnessing-runaway fix's per-term absorption). Initialized to
    /// `u64::MAX`, a sentinel no real Raft term ever reaches, so the very
    /// first mint on a fresh group (term 0 or otherwise) always absorbs.
    /// Read-then-written only from inside `mint_pushed`, which itself only
    /// ever runs while `propose_ordered`/`propose_ordered_aux` hold the
    /// `core` lock — so despite the plain (non-CAS) swap, there is never a
    /// concurrent writer to race. See `mint_pushed`'s doc for the safety
    /// argument this absorb-once-per-term schedule rests on.
    last_absorbed_term: Arc<AtomicU64>,
    /// This group's transaction-record tracker (ADR 0018 §2/PR5) —
    /// `animusd`'s `txn_resolver_loop` reads it via
    /// [`pending_txns`](Self::pending_txns)/
    /// [`unresolved_decided`](Self::unresolved_decided). See [`TxnTracker`]'s
    /// doc for the exact insert/remove rules and the rebuild-at-start
    /// source.
    txn_tracker: Arc<Mutex<TxnTracker>>,
    /// ADR 0044 phase-1 PR5, fork D: an **external** quiesce veto any
    /// subsystem outside this crate may hold — `animusd`'s
    /// `change_consumer_loop` sets this for a group whose change log was
    /// non-empty on its last sweep (see
    /// [`set_quiesce_veto`](Self::set_quiesce_veto)). ORed with this
    /// group's own in-crate `txn_tracker`-derived veto (a non-empty
    /// [`TxnTracker`] always vetoes on its own, no external input needed)
    /// before being fed to [`RaftCore::set_quiesce_veto`] once per
    /// consensus-loop iteration, alongside `quiesce_engine_caught_up`.
    /// Defaults `false` — zero behavior change for every caller that never
    /// touches it.
    external_quiesce_veto: Arc<AtomicBool>,
    /// Freshness stamp for `external_quiesce_veto` (issue #302 fix): the
    /// value of [`engine_applied_index`](Self::engine_applied_index) the
    /// external caller's own observation is valid through — see
    /// [`set_quiesce_veto`](Self::set_quiesce_veto)'s doc for the contract.
    /// Fed to [`RaftCore::set_quiesce_veto`] alongside the bool, once per
    /// consensus-loop iteration, so `quiesce_entry_ok` can reject a `false`
    /// veto that describes engine content older than what's since
    /// committed. Defaults `u64::MAX` — matches `RaftCore`'s own
    /// never-engaged sentinel, so a caller that never calls
    /// `set_quiesce_veto` for this group imposes no freshness requirement
    /// at all (identical to pre-fix behavior).
    external_quiesce_veto_fresh_through: Arc<AtomicU64>,
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
            false,
        )
    }

    /// Like [`start`](Self::start), but with an explicit `scope` (see
    /// [`StorageScope`]'s doc). Since ADR 0050 rung 1 `storage` is this
    /// tablet's own **private** engine; `scope` carries the tablet's
    /// immutable declared range and derives the per-kind key prefixes.
    pub fn start_scoped(env: E, all_nodes: Vec<NodeId>, storage: S, scope: StorageScope) -> Self {
        let metrics = env.metrics();
        Self::start_inner(
            env,
            all_nodes,
            storage,
            metrics,
            scope,
            PRIMARY_STREAM,
            false,
        )
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
        Self::start_inner(env, all_nodes, storage, metrics, scope, stream, false)
    }

    /// Like [`start_hosted`](Self::start_hosted), but this replica
    /// **campaigns for leadership immediately** instead of waiting out the
    /// freshly-bootstrapped group's own cold, randomized election timeout
    /// (ADR 0058 Train 2 rung 4 — the fix for the in-place split's measured
    /// write-blip regression: a forked child group with no leader parks a
    /// write behind `cp_route`'s election-wait branch until *some* timeout
    /// fires; this makes the fast path deterministic instead of waiting on
    /// one).
    ///
    /// **Only ever pass `true` for the replica that observed itself as the
    /// PARENT's current Raft leader at the moment it materializes this
    /// child** (`animus-cp-data::host`'s `materialize_split_child`, driven
    /// by `HostAction::MaterializeSplitChild.campaign` — set once, in
    /// `plan`, from that tick's already-gathered `TabletFacts::is_leader`,
    /// a purely local decision every replica makes independently). This is
    /// safe by construction, not merely by convention: the parent's current
    /// leader is necessarily a **voter** of the parent (`start_election`/
    /// `become_leader` are gated on `is_voter()`), and every child's
    /// `bootstrap_voters` is the parent's own full voter-**and**-learner
    /// union at the fork — a strict superset of the parent's voter set — so
    /// the self-nominating replica is always a voter of the child too. A
    /// learner can never satisfy `TabletFacts::is_leader` in the first
    /// place (a learner never campaigns, ADR 0058 Train 1), so this flag can
    /// never even be considered for one; `drive` additionally asserts
    /// `config().contains(&self_id)` right before calling
    /// `RaftCore::campaign_now` as a second, structural line of defense —
    /// see that assertion's own comment.
    ///
    /// In the common case exactly one replica passes `true` per child (the
    /// parent's own leader, materializing both children at once) — this
    /// is what gives each freshly-forked child a leader within roughly one
    /// pre-vote+vote round trip instead of a randomized cold-start wait. If
    /// the parent's leader crashed exactly at the fork (nobody's tick
    /// observes `is_leader` for the parent that round), every replica calls
    /// this with `false` and the child elects exactly the way it always did
    /// before this rung — the ordinary randomized-timeout path is the
    /// untouched fallback, never a second mechanism to keep in sync.
    pub fn start_hosted_campaigning(
        env: E,
        all_nodes: Vec<NodeId>,
        storage: S,
        scope: StorageScope,
        stream: u64,
    ) -> Self {
        let metrics = env.metrics();
        Self::start_inner(env, all_nodes, storage, metrics, scope, stream, true)
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
            false,
        )
    }

    fn start_inner(
        env: E,
        all_nodes: Vec<NodeId>,
        storage: S,
        metrics: MetricsHandle,
        scope: StorageScope,
        stream: u64,
        campaign_immediately: bool,
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
        let kind_outcomes = Arc::new(Mutex::new(KindBatchOutcomes::default()));
        let halted = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new(AtomicBool::new(false));
        let apply_stopped = Arc::new(AtomicBool::new(false));
        let engine_applied = Arc::new(AtomicU64::new(0));
        let wal_lock = Arc::new(AsyncMutex::new(()));
        let propose_signal = Arc::new(ProposeSignal::default());
        let apply_signal = Arc::new(ApplySignal::default());
        let wake_signal = Arc::new(WakeSignal::default());
        let fork_signal = Arc::new(ForkSignal::default());
        let persist = Arc::new(PersistProgress::default());
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
        let frozen = Arc::new(AtomicBool::new(false));
        let last_ceiling_candidate = Arc::new(AtomicU64::new(0));
        let last_proposed_ts = Arc::new(AtomicU64::new(0));
        // Sentinel (never a real term) so the very first mint on this group
        // always absorbs the committed ceiling — see the field's own doc.
        let last_absorbed_term = Arc::new(AtomicU64::new(u64::MAX));
        // Rebuilt asynchronously inside `drive` (a scoped engine scan needs
        // `.await`, unlike every other piece of group-start state here) —
        // starts empty and is populated before the apply task's first pass,
        // mirroring `sealed`/`committed_ceiling`'s own rebuild-then-spawn
        // ordering.
        let txn_tracker = Arc::new(Mutex::new(TxnTracker::default()));
        let external_quiesce_veto = Arc::new(AtomicBool::new(false));
        // See the field's own doc: `u64::MAX` is the "never engaged" sentinel,
        // not `0` — a caller that never calls `set_quiesce_veto` for this
        // group must impose no freshness requirement at all.
        let external_quiesce_veto_fresh_through = Arc::new(AtomicU64::new(u64::MAX));
        let node = Self {
            env: env.clone(),
            core: Arc::clone(&core),
            storage: storage.clone(),
            reads: Arc::clone(&reads),
            cas: Arc::clone(&cas),
            stage: Arc::clone(&stage),
            kind_outcomes: Arc::clone(&kind_outcomes),
            engine_applied: Arc::clone(&engine_applied),
            halted: Arc::clone(&halted),
            stopped: Arc::clone(&stopped),
            apply_stopped: Arc::clone(&apply_stopped),
            propose_signal: Arc::clone(&propose_signal),
            apply_signal: Arc::clone(&apply_signal),
            wake_signal: Arc::clone(&wake_signal),
            fork_signal: Arc::clone(&fork_signal),
            metrics: metrics.clone(),
            scope: scope.clone(),
            kind_scopes: kind_scopes.clone(),
            stream,
            hlc: Arc::clone(&hlc),
            ts_cache: Arc::clone(&ts_cache),
            committed_ceiling: Arc::clone(&committed_ceiling),
            frozen: Arc::clone(&frozen),
            last_ceiling_candidate,
            last_proposed_ts,
            last_absorbed_term,
            txn_tracker: Arc::clone(&txn_tracker),
            external_quiesce_veto: Arc::clone(&external_quiesce_veto),
            external_quiesce_veto_fresh_through: Arc::clone(&external_quiesce_veto_fresh_through),
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
            kind_outcomes,
            engine_applied,
            wal_lock,
            halted,
            stopped,
            apply_stopped,
            propose_signal,
            apply_signal,
            wake_signal,
            fork_signal,
            persist,
            metrics,
            scope,
            kind_scopes,
            stream,
            hlc,
            committed_ceiling,
            frozen,
            txn_tracker,
            external_quiesce_veto,
            external_quiesce_veto_fresh_through,
            campaign_immediately,
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
        // Wake a parked apply task (ADR 0044 phase-1 PR1): its idle back-off now
        // races `ApplySignal` against a much longer `APPLY_SAFETY_POLL` (was a bare
        // 5ms poll), so without this a shutdown request could sit unnoticed by the
        // apply task for up to that whole interval instead of within one wake.
        self.apply_signal.notify();
        // Wake a parked consensus loop too (ADR 0044 phase-1 PR2, finding 4's
        // hazard 1): today the consensus loop always re-wakes well within one
        // heartbeat/election interval on its own (continual Raft traffic even at
        // rest), so this is a no-op in effect — but once phase-1 PR3's
        // quiescence makes `next_deadline()` return `None` for a quiesced
        // leader, its consensus loop parks with **no timer at all**, and without
        // this notify a `shutdown()` on such a group could sit unnoticed
        // forever (never observed, so `is_stopped()` never flips, so
        // `Reconciler::teardown`'s `RECLAIM_STOP_TIMEOUT` would expire and
        // drop-table GC would never converge) instead of within one wake.
        self.wake_signal.notify();
    }

    /// Explicitly wake this group's consensus loop for one extra pass. As of
    /// ADR 0044 phase-1 PR3 this is what a locally-woken **quiesced follower**
    /// uses to check "are you still there?" with its recorded leader
    /// ([`RaftCore::on_local_wake`]'s doc — the consensus loop calls it on
    /// this signal) instead of blindly waiting out a stale election timer.
    /// PR4's proactive wake (the edge's `resolve_cp_route` before routing to a
    /// local group, and the reconciler waking a group whose replica set
    /// intersects a newly-`Down` node) will call this same method rather than
    /// duplicating [`WakeSignal`]'s plumbing. Idempotent and always safe on
    /// every other state (not quiesced, or this node is the leader): it then
    /// only causes one extra, inert loop iteration (re-checking `halted` and
    /// re-evaluating `next_deadline`), never an unwanted state change.
    pub fn wake(&self) {
        self.wake_signal.notify();
    }

    /// A future that resolves once this replica's own apply task has
    /// observed a local `KvCommand::SplitTablet` fork (ADR 0058 Train 2
    /// rung 4 layer 1) — see [`ForkSignal`]'s doc. Exposed `pub(crate)`
    /// purely for [`host::Reconciler::fork_wake`]'s fan-in wait; an ordinary
    /// caller checks the durable fact itself via
    /// [`pending_split`](Self::pending_split) rather than awaiting this.
    pub(crate) fn fork_wake(&self) -> ForkPending<'_> {
        ForkPending {
            signal: &self.fork_signal,
        }
    }

    /// Opt this group into quiescence (ADR 0044 phase-1 PR3): once its leader
    /// has had no local activity for `after` and every other entry-predicate
    /// clause holds (see [`RaftCore::enable_quiescence`]'s doc), it stops
    /// ticking until some event wakes it. Data-plane only — nothing in
    /// `animus-control`'s own `RaftNode` calls the equivalent (fork G), so the
    /// control plane's `next_deadline` never returns `None`.
    pub fn enable_quiescence(&self, after: Duration) {
        self.lock().enable_quiescence(after);
    }

    /// Whether this group's local replica currently considers itself
    /// quiesced (surfaced for tests and, in a later PR, the admin/dashboard
    /// view — reading it never itself wakes the group, fork F).
    #[must_use]
    pub fn is_quiesced(&self) -> bool {
        self.lock().is_quiesced()
    }

    /// ADR 0044 phase-1 PR5 (fork D): let an external subsystem (`animusd`'s
    /// `change_consumer_loop`) hold or release the quiesce veto for this
    /// group — set for a group whose change log was non-empty on its last
    /// sweep, cleared once it drains. ORed with this group's own in-crate
    /// `TxnTracker`-derived veto (always vetoing on a pending 2PC intent or
    /// an unresolved decided record, no external input needed for that
    /// part) inside the consensus loop, once per iteration — see that
    /// loop's own doc. Idempotent, safe to call every tick regardless of
    /// current state.
    ///
    /// **`fresh_through` (issue #302 fix) is a freshness contract, not an
    /// optional annotation.** It must be [`engine_applied_index`](Self::
    /// engine_applied_index) read by the caller **before** whatever
    /// observation decided `held` (e.g. before scanning
    /// [`pending_changes`](Self::pending_changes)) — never after, and never
    /// the result of the scan itself. Reading it first gives a valid
    /// *lower* bound: a concurrent apply between the read and the scan can
    /// only make the true state fresher than what's recorded, never make a
    /// stale observation look fresher than it was. Reading it *after* the
    /// scan is unsound — a write that commits and applies in that window
    /// would be silently missing from the scan yet still counted as
    /// "observed," reopening exactly the staleness race this parameter
    /// exists to close (see `RaftCore::quiesce_veto_fresh_through`'s own
    /// doc for the full invariant). A caller with no natural index to
    /// report may pass [`engine_applied_index`](Self::engine_applied_index)
    /// itself (i.e. "as of right now") if it has just performed a
    /// synchronous, uninterrupted check with no `.await` in between.
    pub fn set_quiesce_veto(&self, held: bool, fresh_through: u64) {
        self.external_quiesce_veto.store(held, Ordering::SeqCst);
        self.external_quiesce_veto_fresh_through
            .store(fresh_through, Ordering::SeqCst);
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
    /// (`mint_pushed`/`next_ceiling_candidate`/[`propose_freeze`](Self::
    /// propose_freeze)'s bare mint) must additionally exceed, closing the
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
    /// locks `core` while holding. `build` is handed this group's current Raft
    /// `term`, read off the already-held `core` — the seam [`mint_pushed`]
    /// (Self::mint_pushed)'s per-term ceiling absorption uses, since
    /// `mint_pushed` itself cannot call [`term`](Self::term) (it would try to
    /// lock `core` a second time from inside this same held lock).
    fn propose_ordered<F: FnOnce(u64) -> KvCommand>(&self, build: F) -> ProposeResult {
        let mut core = self.lock();
        let term = core.term();
        let command = build(term);
        let ts = command_ts(&command);
        let result = record_propose(&self.metrics, core.propose(command));
        if matches!(result, ProposeResult::Accepted { .. }) {
            if let Some(ts) = ts {
                self.last_proposed_ts.store(hlc::pack(ts), Ordering::SeqCst);
            }
            // ADR 0044 phase-1 PR3, un-quiesce trigger (b): a local propose
            // that actually lands is real leader activity — `propose` itself
            // has no `now` to work with (see `RaftCore::note_local_activity`'s
            // doc), so the driver supplies it here, inside the same held lock.
            core.note_local_activity(self.env.now());
        }
        drop(core);
        if matches!(result, ProposeResult::Accepted { .. }) {
            self.propose_signal.notify();
            // A single-node (majority-1) group's `core.propose` above can advance
            // commit + apply inline (ADR 0044 phase-1 PR1) — nudge the apply task
            // too in case that just created work for it.
            self.apply_signal.notify();
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
    /// only trusts it once it has confirmed `Accepted`. As
    /// [`propose_ordered`](Self::propose_ordered), `build` is handed this
    /// group's current Raft `term`.
    fn propose_ordered_aux<T, F: FnOnce(u64) -> (KvCommand, T)>(
        &self,
        build: F,
    ) -> (ProposeResult, T) {
        let mut core = self.lock();
        let term = core.term();
        let (command, aux) = build(term);
        let ts = command_ts(&command);
        let result = record_propose(&self.metrics, core.propose(command));
        if matches!(result, ProposeResult::Accepted { .. }) {
            if let Some(ts) = ts {
                self.last_proposed_ts.store(hlc::pack(ts), Ordering::SeqCst);
            }
            // See `propose_ordered`'s identical note.
            core.note_local_activity(self.env.now());
        }
        drop(core);
        if matches!(result, ProposeResult::Accepted { .. }) {
            self.propose_signal.notify();
            // See `propose_ordered`'s identical note: a single-node group's
            // `core.propose` can advance commit + apply inline.
            self.apply_signal.notify();
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
            // This is a monotonic ratchet, not a wraparound — the
            // (astronomically unlikely) overflow carries into `wall_ms` via
            // `bump_strictly_above`'s own rule (shared with `mint_pushed`'s
            // identical-shape no-witness push, see that function's doc).
            let candidate = if margin > last {
                margin
            } else {
                bump_strictly_above(last)
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
    /// served for `keys` (ADR 0018 §2/PR2b, amended by the `mint_pushed`
    /// clock-witnessing-runaway fix below — the write-conflict-push half of
    /// serializability): a write must never land at or below a timestamp
    /// `keys` were already read at. `term` is this group's current Raft
    /// term, handed down by [`propose_ordered`](Self::propose_ordered)/
    /// [`propose_ordered_aux`](Self::propose_ordered_aux) off the `core`
    /// lock they already hold — `mint_pushed` cannot read it itself via
    /// [`term`](Self::term) without deadlocking on that same lock.
    ///
    /// **Per-term ceiling absorption (the safety invariant this rests on).**
    /// The committed ceiling is folded into [`ts_cache`](Self::ts_cache)'s
    /// `low_water` **at most once per term**, the first time this leader
    /// mints in a given term — never on every mint. This is strictly
    /// sufficient: during one stable leadership stint, every read this
    /// leader itself serves already bumps `ts_cache` at its own real serve
    /// `ts` (never a future-shifted value), so the per-key cache alone
    /// floors every subsequent write above every read *this* leader served.
    /// The ceiling's write-floor role is only needed to additionally cover
    /// a **predecessor's** reads — its own per-key `ts_cache` entries died
    /// with it — and a predecessor's ceiling is fixed as of this leader's
    /// own takeover: absorbing it once, at the first mint of the new term,
    /// covers every read the predecessor could ever have served (Raft
    /// leader completeness — the new leader already witnessed the prior
    /// ceiling's `ts` via ordinary `AppendEntries` receipt before it could
    /// ever campaign, and a deposed leader cannot commit a fresher one
    /// after being deposed: its own `ReadCeiling`/write proposals fail once
    /// it loses the leadership its `ensure_ceiling_above`/`propose_ordered`
    /// calls require). Absorbing it again on every later mint in the same
    /// term added nothing to safety — every read since takeover is already
    /// covered by the per-key cache — while feeding a live feedback loop:
    /// the ceiling is deliberately minted `HLC_MAX_OFFSET` in the future
    /// (`ensure_ceiling_above`'s `Hlc::uncertainty_upper`), so an ordinary
    /// mint almost always fell short of it, triggering the no-witness push
    /// below on *every* write, which (before this fix, see next) advanced
    /// this leader's own clock toward that future value, which made the
    /// *next* read's serve `ts` approach and exceed the ceiling almost
    /// immediately, forcing `ensure_ceiling_above` to propose a fresh one
    /// — a k×`HLC_MAX_OFFSET` runaway lattice roughly 10x faster than real
    /// time, probe-verified, that also starved genuine log entries behind
    /// the manufactured `ReadCeiling` churn on the propose path. The
    /// sentinel initial value of [`last_absorbed_term`](Self::last_absorbed_term)
    /// (`u64::MAX`, never a real term) ensures the very first mint on a
    /// fresh group still absorbs.
    ///
    /// **No-witness push (defense in depth).** When the honest mint falls
    /// at or below the floor, the pushed replacement is computed as pure
    /// arithmetic strictly above the floor
    /// ([`bump_strictly_above`]) — **never** via [`Hlc::witness`], unlike
    /// this function's pre-fix behavior. `next_ceiling_candidate`'s own doc
    /// already named this exact hazard (witnessing a deliberately
    /// future-shifted value poisons every later ordinary mint) and avoided
    /// it with a separate CAS ratchet; this was the second, independent
    /// route to the same `Hlc::witness` sink that fix never covered.
    /// Monotonicity across proposes still holds without witnessing: the
    /// floor already includes [`last_proposed_ts`](Self::last_proposed_ts)
    /// (this leader's own last-*logged* ts), so each pushed write's ts
    /// strictly exceeds every ts this leader has minted or proposed so far
    /// — the same property `Hlc::witness` would have provided, without any
    /// of its side effect on `self.hlc`'s own persistent state. See
    /// `docs/engineering-lessons.md` and this crate's ADR 0018 §2 amendment
    /// for the full incident.
    fn mint_pushed<K: AsRef<[u8]>>(&self, term: u64, keys: &[K]) -> HlcTimestamp {
        let ts = self.hlc.mint(self.env.now());
        let cache_floor = {
            let mut cache = self.ts_cache.lock().expect("ts cache poisoned");
            // Absorb the committed ceiling into `low_water` only on this
            // leader's first mint of `term` (see the doc above) — `swap`
            // both reads the previously-absorbed term and records `term` in
            // one step; no CAS is needed since every `mint_pushed` call is
            // already serialized by the `core` lock its caller holds.
            if self.last_absorbed_term.swap(term, Ordering::SeqCst) != term {
                cache.raise_low_water(self.committed_ceiling());
            }
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
        let pushed = bump_strictly_above(floor);
        assert!(
            pushed > floor,
            "raftkv write-push: the no-witness bump must strictly exceed the floor \
             (floor={floor:?}, got={pushed:?}) — bump_strictly_above's own contract is broken"
        );
        pushed
    }

    /// Propose a write to this group. Honored only on the leader (otherwise
    /// returns the leader hint); the value is durable + applied once committed.
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> ProposeResult {
        self.propose_ordered(|term| {
            let ts = self.mint_pushed(term, std::slice::from_ref(&key));
            KvCommand::Put { key, value, ts }
        })
    }

    /// Propose a **batch put**: commit every `(key, value)` as **one** Raft log
    /// entry (one propose → one commit round → one apply), for a bulk-write
    /// throughput win over N individual [`put`](Self::put)s. Honored only on the
    /// leader (else a leader hint). All keys share the entry's Raft index as their
    /// MVCC version — the keys are distinct so per-key LWW is well-defined, and the
    /// batch is atomic within this tablet (it commits whole or not at all). To learn
    /// it committed + applied, take the [`ProposeResult::Accepted`] `index` and wait
    /// until `last_applied >= index` (the whole batch has merged by then).
    pub fn put_batch(&self, puts: Vec<(Vec<u8>, Vec<u8>)>) -> ProposeResult {
        self.propose_ordered(|term| {
            let keys: Vec<&[u8]> = puts.iter().map(|(k, _)| k.as_slice()).collect();
            let ts = self.mint_pushed(term, &keys);
            KvCommand::Batch { puts, ts }
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
    /// Supplies no `conditions`; use
    /// [`put_kind_batch_conditioned`](Self::put_kind_batch_conditioned) to
    /// supply own-key OCC conditions.
    pub fn put_kind_batch(
        &self,
        writes: Vec<KindWrite>,
        change_log: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> ProposeResult {
        self.put_kind_batch_conditioned(writes, change_log, Vec::new())
    }

    /// As [`put_kind_batch`](Self::put_kind_batch), but may supply own-key OCC
    /// `conditions`. See [`KvCommand::KindBatch`]'s doc for what `conditions`
    /// means and why it is checked ahead of the seal gate; pass an empty `Vec`
    /// for the pre-existing no-conditions behavior (every caller before this
    /// field existed).
    pub fn put_kind_batch_conditioned(
        &self,
        writes: Vec<KindWrite>,
        change_log: Vec<(Vec<u8>, Vec<u8>)>,
        conditions: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    ) -> ProposeResult {
        self.propose_ordered(|term| {
            let keys: Vec<&[u8]> = writes.iter().map(|(_, k, _)| k.as_slice()).collect();
            let ts = self.mint_pushed(term, &keys);
            KvCommand::KindBatch {
                writes,
                change_log,
                conditions,
                ts,
            }
        })
    }

    /// Propose a **split-build seed chunk** into this (child) group's log
    /// (ADR 0050 Train B rung 4 — see [`KvCommand::SeedBatch`]'s doc for the
    /// full semantics: version-carrying merges, envelope bytes verbatim, no
    /// change-log emission). The entry's own `ts` is minted normally
    /// (`propose_ordered`), keeping the apply-time monotonicity assert
    /// honest, while every row merges at its **carried** version.
    pub fn propose_seed_batch(&self, rows: Vec<SeedRow>) -> ProposeResult {
        self.propose_ordered(|term| {
            let ts = self.mint_pushed::<&[u8]>(term, &[]);
            KvCommand::SeedBatch { rows, ts }
        })
    }

    /// Propose a delete (tombstone) to this group.
    pub fn delete(&self, key: Vec<u8>) -> ProposeResult {
        self.propose_ordered(|term| {
            let ts = self.mint_pushed(term, std::slice::from_ref(&key));
            KvCommand::Delete { key, ts }
        })
    }

    /// Propose a **linearizable compare-and-swap**: set `key` to `value` iff the
    /// key's current committed value equals `expected` (`None` == "only if the
    /// key is absent"). Leader-only (else a leader hint). The accept/reject
    /// decision is made deterministically at *apply* time in commit order, so two
    /// CAS racing from the same `expected` have exactly one winner. To learn the
    /// outcome, take the [`ProposeResult::Accepted`] `index` and read
    /// [`cas_result`](Self::cas_result) once that index applies — or use the
    /// all-in-one [`compare_and_swap`](Self::compare_and_swap).
    pub fn cas(&self, key: Vec<u8>, expected: Option<Vec<u8>>, value: Vec<u8>) -> ProposeResult {
        self.propose_ordered(|term| {
            let ts = self.mint_pushed(term, std::slice::from_ref(&key));
            KvCommand::Cas {
                key,
                expected,
                value,
                ts,
            }
        })
    }

    /// Whether this group has applied its split-cutover
    /// [`KvCommand::Freeze`] (ADR 0050 rung 5) — a pure frozen-flag read,
    /// never itself a wake or a propose. `animusd`'s local write/txn
    /// helpers consult this **before proposing** and refuse retryably
    /// (`"...; retry"`), so a stale-routed client re-resolves and lands on
    /// a split child; the apply-time whole-range seal remains the
    /// correctness backstop for the propose-vs-apply sliver (such an entry
    /// no-ops and its proposer's own probe/outcome confirm times out —
    /// degraded latency, never a false ack).
    pub fn is_frozen(&self) -> bool {
        self.frozen.load(Ordering::SeqCst)
    }

    /// Propose the **split-cutover freeze** (ADR 0050 rung 5, stage 3 — see
    /// [`KvCommand::Freeze`]'s doc): terminally close this whole group to
    /// further mutation, ordered at this entry's own log position. Called
    /// only by the split-build driver once the build has converged;
    /// leader-only, idempotent (a duplicate applies as a no-op — the driver
    /// may re-propose after a crash/re-lead without checking first).
    pub fn propose_freeze(&self) -> ProposeResult {
        self.propose_ordered(|_term| {
            let ts = self.hlc.mint(self.env.now());
            // Same `last_proposed_ts` floor discipline as `mint_pushed`
            // (see that method's inline comment).
            let floor = hlc::unpack(self.last_proposed_ts.load(Ordering::SeqCst));
            let ts = if ts > floor {
                ts
            } else {
                let pushed = self.hlc.witness(floor, self.env.now());
                assert!(
                    pushed > floor,
                    "raftkv propose_freeze: witnessing the last-proposed floor must strictly \
                     exceed it (floor={floor:?}, got={pushed:?}) — Hlc::witness's own contract \
                     is broken"
                );
                pushed
            };
            KvCommand::Freeze { ts }
        })
    }

    /// Propose the **in-place split fork** (ADR 0058 Train 2 rung 3, Stage
    /// 3 — see [`KvCommand::SplitTablet`]'s doc): the single-entry atomic
    /// mint that forks this group into `children`. Called only by the
    /// parent's own leader, only once every learner added for the split
    /// has caught up (the host reconciler's job to determine — see
    /// `host.rs`); leader-only, idempotent (a duplicate applies as a no-op
    /// exactly like [`propose_freeze`](Self::propose_freeze) — the
    /// reconciler may re-propose after a crash/re-lead without checking
    /// first).
    pub fn propose_split_tablet(
        &self,
        split_key: Vec<u8>,
        children: [SplitChild; 2],
    ) -> ProposeResult {
        self.propose_ordered(|_term| {
            let ts = self.hlc.mint(self.env.now());
            // Same `last_proposed_ts` floor discipline as `propose_freeze`.
            let floor = hlc::unpack(self.last_proposed_ts.load(Ordering::SeqCst));
            let ts = if ts > floor {
                ts
            } else {
                let pushed = self.hlc.witness(floor, self.env.now());
                assert!(
                    pushed > floor,
                    "raftkv propose_split_tablet: witnessing the last-proposed floor must \
                     strictly exceed it (floor={floor:?}, got={pushed:?}) — Hlc::witness's own \
                     contract is broken"
                );
                pushed
            };
            KvCommand::SplitTablet {
                split_key: split_key.clone(),
                children: children.clone(),
                ts,
            }
        })
    }

    /// This group's pending (or already-applied) in-place split fork, if
    /// any — the one accessor the per-node host reconciler
    /// (`host::Reconciler`) polls to discover "this hosted tablet forked
    /// locally, here are its two children" (ADR 0058 Train 2 Stage 3). A
    /// point read of the durable fork marker (`split.rs`) — cheap (no scan,
    /// unlike `seal.rs`'s marker, since a tablet forks at most once) and
    /// safe to call every reconciler tick on every hosted tablet, forked or
    /// not. Present the instant this **replica's own** apply has processed
    /// the `SplitTablet` entry — which, for a learner, only happens after
    /// Stage 2's catch-up is complete and Stage 3's entry has replicated to
    /// it, exactly the ordering the design depends on.
    pub async fn pending_split(&self) -> Option<PendingSplit> {
        let key = split::split_marker_key(self.stream);
        let vv = self.storage.get(&key).await.ok().flatten()?;
        let decoded = split::decode_split_value(&vv.value)?;
        Some(PendingSplit {
            split_key: decoded.split_key,
            children: decoded.children,
            bootstrap_voters: decoded.bootstrap_voters,
            ts: decoded.ts,
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
        let writes = writes
            .into_iter()
            .map(|(k, v)| txn::TxnWrite::plain(k, v))
            .collect();
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
        writes: Vec<txn::TxnWrite>,
        participant_spans: Vec<(String, KeyRange)>,
        conditions: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    ) -> Option<(TxnId, Vec<u8>, StageOutcome)> {
        assert!(
            !writes.is_empty(),
            "raftkv txn_stage: writes must be non-empty"
        );
        let anchor = &writes[0].key;
        assert!(
            anchor.len() >= animus_tablet::TOKEN_BYTES,
            "raftkv txn_stage: anchor key must lead with the {}-byte partition token \
             (ADR 0022) — got {} bytes",
            animus_tablet::TOKEN_BYTES,
            anchor.len()
        );
        let token = anchor[..animus_tablet::TOKEN_BYTES].to_vec();
        let keys: Vec<Vec<u8>> = writes.iter().map(|w| w.key.clone()).collect();
        let record_table = table.to_owned();
        let (result, (txn_id, record_key)) = self.propose_ordered_aux(|term| {
            let ts = self.mint_pushed(term, &keys);
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
                ts,
            };
            (cmd, (txn_id, record_key))
        });
        let (index, term) = match result {
            ProposeResult::Accepted { index, term } => (index, term),
            ProposeResult::NotLeader { .. } => return None,
        };
        let outcome = self.wait_stage_outcome(index, term).await?;
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
        writes: Vec<txn::TxnWrite>,
        conditions: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    ) -> Option<(HlcTimestamp, StageOutcome)> {
        assert!(
            !writes.is_empty(),
            "raftkv txn_stage_participant: writes must be non-empty"
        );
        let keys: Vec<Vec<u8>> = writes.iter().map(|w| w.key.clone()).collect();
        let (result, ts) = self.propose_ordered_aux(|term| {
            let ts = self.mint_pushed(term, &keys);
            let cmd = KvCommand::TxnStage {
                txn_id: txn_id.clone(),
                record_key: record_key.clone(),
                record_table: record_table.clone(),
                is_anchor: false,
                writes: writes.clone(),
                spans: Vec::new(), // unused: no local record is ever created here.
                conditions,
                ts,
            };
            (cmd, ts)
        });
        let (index, term) = match result {
            ProposeResult::Accepted { index, term } => (index, term),
            ProposeResult::NotLeader { .. } => return None,
        };
        let outcome = self.wait_stage_outcome(index, term).await?;
        Some((ts, outcome))
    }

    /// Mint a ts that strictly exceeds `min_ts` **and** this group's own
    /// `last_proposed_ts` floor (mirrors [`mint_pushed`](Self::mint_pushed)/
    /// [`propose_freeze`](Self::propose_freeze)'s identical witness-and-floor
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
        let pushed = bump_strictly_above(floor);
        assert!(
            pushed > floor,
            "raftkv mint_at_least: the no-witness bump must strictly exceed the floor \
             (floor={floor:?}, got={pushed:?}) — bump_strictly_above's own contract is broken"
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
        let (result, ts) = self.propose_ordered_aux(|_term| {
            let ts = self.mint_at_least(min_ts);
            let cmd = KvCommand::TxnCommit {
                txn_id: txn_id.clone(),
                record_key: record_key.clone(),
                ts,
            };
            (cmd, ts)
        });
        let index = match result {
            ProposeResult::Accepted { index, .. } => index,
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
        let (result, ts) = self.propose_ordered_aux(|term| {
            let ts = self.mint_pushed(term, std::slice::from_ref(&record_key));
            let cmd = KvCommand::TxnAbort {
                txn_id: txn_id.clone(),
                record_key: record_key.clone(),
                ts,
                orphan_created_ts: None,
            };
            (cmd, ts)
        });
        let index = match result {
            ProposeResult::Accepted { index, .. } => index,
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
        let (result, ts) = self.propose_ordered_aux(|term| {
            let ts = self.mint_pushed(term, std::slice::from_ref(&record_key));
            let cmd = KvCommand::TxnAbort {
                txn_id: txn_id.clone(),
                record_key: record_key.clone(),
                ts,
                orphan_created_ts: Some(created_ts),
            };
            (cmd, ts)
        });
        let index = match result {
            ProposeResult::Accepted { index, .. } => index,
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
        // ADR 0018 §2 write-loss amendment: stamped from this group's own
        // live scope, exactly like `TxnStage`'s `fence` above — see
        // `KvCommand::TxnResolve`'s doc for why this is no longer safe to
        // omit.
        let (result, ts) = self.propose_ordered_aux(|term| {
            let ts = self.mint_pushed(term, &keys);
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
            ProposeResult::Accepted { index, .. } => index,
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
        let (decide_result, decision_ts) = self.propose_ordered_aux(|term| {
            let ts = self.mint_pushed(term, std::slice::from_ref(&record_key));
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
            ProposeResult::Accepted { index, .. } => index,
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
    /// one of them (see [`TxnRecordView`]'s doc).
    ///
    /// **`Option<Option<..>>`, the `stale_get_served`/`linearizable_get_
    /// served` "served" discipline (ADR 0018 §2, issue #298 shape B fix)** —
    /// unlike [`txn_status_local`](Self::txn_status_local)'s plain `Option`,
    /// which conflated "not served" (barrier failed) with "genuinely no
    /// record" into the same bare `None` until this fix, this method's
    /// caller (`ClientCtx::txn_recover`'s orphan-record branch) makes a
    /// **decision** (whether to synthesize an abort tombstone) directly off
    /// "no record exists" — an outcome that must never be confused with "I
    /// couldn't tell right now" (e.g. this replica losing its own read
    /// barrier mid-fork/cutover, exactly what a high split cadence
    /// produces routinely). Outer `None` = **not served**, caller must
    /// decline/retry, never decide anything from it; `Some(None)` =
    /// definitively no record at this key; `Some(Some(view))` = found.
    pub async fn txn_record_view(&self, record_key: &[u8]) -> Option<Option<TxnRecordView>> {
        if !self.read_barrier().await {
            return None;
        }
        let physical = self.scope.physical(record_key);
        let Ok(found) = self.storage.get(&physical).await else {
            return None;
        };
        let Some(vv) = found else {
            return Some(None);
        };
        let Some(record) = txn::decode_record(&vv.value) else {
            return Some(None);
        };
        Some(Some(TxnRecordView {
            status: record.status.to_public(),
            intent_spans: record.intent_spans,
            created_ts: record.created_ts,
        }))
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

    /// Poll [`stage_outcome`](Self::stage_outcome) for `(index, term)`
    /// directly, bounded by [`CAS_TIMEOUT`]/[`CAS_POLL`] — mirrors
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
    /// guarantee `stage_outcome(index, term)` is `Some`. Polling the outcome
    /// directly (like CAS always has) makes this method's own `None` mean
    /// exactly what every other propose-and-wait method's `None` means —
    /// "give up, caller retries" — instead of ever hard-`expect`ing a fact
    /// that isn't actually guaranteed.
    ///
    /// **`term` must be the term [`ProposeResult::Accepted`] handed the
    /// caller for this exact entry** (see [`StageOutcomes`]'s doc): an
    /// uncommitted entry's index can be reoccupied by a different command
    /// after a leadership change, so a `None` here can mean either "not
    /// applied here yet" or "a different entry now occupies this index" —
    /// either way the caller cannot trust its own stage landed and must
    /// treat it as unconfirmed (retry/propose again), never as a definitive
    /// success or failure.
    async fn wait_stage_outcome(&self, index: u64, term: u64) -> Option<StageOutcome> {
        let deadline = self.env.now().0 + CAS_TIMEOUT.as_nanos() as u64;
        loop {
            if let Some(outcome) = self.stage_outcome(index, term) {
                return Some(outcome);
            }
            if !self.is_leader() || self.env.now().0 >= deadline {
                return None;
            }
            self.env.sleep(CAS_POLL).await;
        }
    }

    /// The recorded outcome of the `TxnStage` committed at Raft log `index`
    /// (ADR 0018 §2 apply-time write-key conditions amendment) — `None` if
    /// that index has not applied on this replica yet, **or if the entry
    /// recorded there was accepted under a different term than `term`**
    /// (see [`StageOutcomes`]'s doc): an uncommitted entry's index can be
    /// reoccupied by a different command after a leadership change, so
    /// index alone never proves the recorded outcome is this caller's own.
    /// `term` must be the term the caller's own [`ProposeResult::Accepted`]
    /// returned for this entry. Mirrors [`cas_result`](Self::cas_result)
    /// exactly; see [`StageOutcome`]'s doc for what each variant means to a
    /// caller once confirmed.
    pub fn stage_outcome(&self, index: u64, term: u64) -> Option<StageOutcome> {
        match self
            .stage
            .lock()
            .expect("stage outcomes poisoned")
            .outcomes
            .get(&index)
        {
            Some((recorded_term, outcome)) if *recorded_term == term => Some(outcome.clone()),
            _ => None,
        }
    }

    /// What the `KindBatch` committed at Raft log `index` actually did, paired
    /// with the entry's own **term** — `None` if that index has not applied
    /// on this replica **or** has aged out of the bounded map (see
    /// [`KindBatchOutcomes`]). Mirrors
    /// [`stage_outcome`](Self::stage_outcome)/[`cas_result`](Self::cas_result);
    /// every replica records the identical outcome.
    ///
    /// This is what lets a proposer tell "my entry no-op'd" from "my entry
    /// applied and was then overwritten" — a distinction reading the value
    /// back cannot make, and the reason a contended key used to report
    /// spurious write failures. **The returned `term` is load-bearing, not
    /// incidental**: `Applied` recorded here only proves *some* entry applied
    /// at `index`, and after a leadership change that can be a *different*
    /// entry than the one this proposer's own `ProposeResult::Accepted`
    /// named — see [`KindBatchOutcome`]'s doc. A caller must compare this
    /// `term` against its own accepted `term` before treating `Applied` as a
    /// confirm; `ConditionFailed`/`Sealed` need no such check (see their own
    /// variant docs).
    #[must_use]
    pub fn kind_batch_outcome(&self, index: u64) -> Option<(u64, KindBatchOutcome)> {
        self.kind_outcomes
            .lock()
            .expect("kind batch outcomes poisoned")
            .outcomes
            .get(&index)
            .cloned()
    }

    /// The recorded outcome of the CAS committed at Raft log `index` — `Some(true)`
    /// if the swap happened, `Some(false)` if `expected` did not match the
    /// committed value, or `None` if that index has not applied on this
    /// replica yet, **or if the entry recorded there was accepted under a
    /// different term than `term`** (see [`CasResults`]'s doc): an
    /// uncommitted entry's index can be reoccupied by a different `Cas`
    /// after a leadership change, so index alone never proves the recorded
    /// outcome is this caller's own. `term` must be the term the caller's
    /// own [`ProposeResult::Accepted`] returned for this entry. Every
    /// replica records the identical outcome (the decision is deterministic
    /// in commit order).
    pub fn cas_result(&self, index: u64, term: u64) -> Option<bool> {
        match self
            .cas
            .lock()
            .expect("cas results poisoned")
            .outcomes
            .get(&index)
        {
            Some((recorded_term, swapped)) if *recorded_term == term => Some(*swapped),
            _ => None,
        }
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
        let (index, term) = match self.cas(key, expected, value) {
            ProposeResult::Accepted { index, term } => (index, term),
            ProposeResult::NotLeader { .. } => return None,
        };
        let deadline = self.env.now().0 + CAS_TIMEOUT.as_nanos() as u64;
        loop {
            if let Some(outcome) = self.cas_result(index, term) {
                return Some(outcome);
            }
            // A step-down before this entry applies (or commits under a
            // different term, e.g. a truncation-and-reoccupation) means it
            // may never apply as *this* proposer's own entry — give up
            // rather than wait out the full timeout uselessly, mirroring
            // `wait_applied`/`wait_stage_outcome`'s identical guard.
            if !self.is_leader() || self.env.now().0 >= deadline {
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
        let mut core = self.lock();
        let result = record_reconfigure(&self.metrics, core.change_membership(voters));
        if matches!(result, ProposeResult::Accepted { .. }) {
            // ADR 0044 phase-1 PR3, un-quiesce trigger (b): see
            // `propose_ordered`'s identical note.
            core.note_local_activity(self.env.now());
        }
        drop(core);
        if matches!(result, ProposeResult::Accepted { .. }) {
            self.propose_signal.notify();
            // A single-node group's config-change no-op can commit + apply inline
            // (ADR 0044 phase-1 PR1), same as `propose_ordered`.
            self.apply_signal.notify();
        }
        result
    }

    /// The group's active Raft voter configuration.
    pub fn config(&self) -> BTreeSet<NodeId> {
        self.lock().config()
    }

    /// The group's active **learner** configuration (ADR 0058 Train 1) —
    /// non-voting members that receive replication but never count toward
    /// quorum. Mirrors [`config`](Self::config).
    pub fn learners(&self) -> BTreeSet<NodeId> {
        self.lock().learners()
    }

    /// Whether learner `id` is caught up closely enough to the leader's own
    /// log to be a promotion candidate — see
    /// [`RaftCore::learner_caught_up`]. A pure predicate; it does not itself
    /// trigger a promotion.
    pub fn learner_caught_up(&self, id: &NodeId, threshold: u64) -> bool {
        self.lock().learner_caught_up(id, threshold)
    }

    /// Add `id` as a **learner** of this tablet group (ADR 0058 Train 1): a
    /// new, non-voting member that catches up via the ordinary
    /// `AppendEntries`/`InstallSnapshot` path before ever being promoted —
    /// see [`RaftCore::add_learner`], which this is a thin wrapper over,
    /// mirroring [`change_membership`](Self::change_membership)'s
    /// lock/record/wake shape.
    pub fn add_learner(&self, id: NodeId) -> ProposeResult {
        let mut core = self.lock();
        let result = record_reconfigure(&self.metrics, core.add_learner(id));
        if matches!(result, ProposeResult::Accepted { .. }) {
            core.note_local_activity(self.env.now());
        }
        drop(core);
        if matches!(result, ProposeResult::Accepted { .. }) {
            self.propose_signal.notify();
            self.apply_signal.notify();
        }
        result
    }

    /// Promote learner `id` to **voter** (ADR 0058 Train 1) — see
    /// [`RaftCore::promote_learner`]. The *decision* of when a learner is
    /// ready (the promotion criterion, [`learner_caught_up`](Self::learner_caught_up))
    /// is a caller concern; this only performs the transition once asked.
    pub fn promote_learner(&self, id: NodeId) -> ProposeResult {
        let mut core = self.lock();
        let result = record_reconfigure(&self.metrics, core.promote_learner(id));
        if matches!(result, ProposeResult::Accepted { .. }) {
            core.note_local_activity(self.env.now());
        }
        drop(core);
        if matches!(result, ProposeResult::Accepted { .. }) {
            self.propose_signal.notify();
            self.apply_signal.notify();
        }
        result
    }

    /// Remove learner `id` without promoting it (ADR 0058 Train 1) — see
    /// [`RaftCore::remove_learner`].
    pub fn remove_learner(&self, id: NodeId) -> ProposeResult {
        let mut core = self.lock();
        let result = record_reconfigure(&self.metrics, core.remove_learner(id));
        if matches!(result, ProposeResult::Accepted { .. }) {
            core.note_local_activity(self.env.now());
        }
        drop(core);
        if matches!(result, ProposeResult::Accepted { .. }) {
            self.propose_signal.notify();
            self.apply_signal.notify();
        }
        result
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
    /// Returns the proposed **voter** config if a step changed it; `None` if
    /// already converged, not the leader, a change is in flight, or a step was
    /// taken that doesn't itself change the voter set (a learner add/remove,
    /// or the leadership-transfer case below).
    ///
    /// **ADR 0058 Train 1's reconciler adoption**: adding a replica no longer
    /// puts it straight into the voter set. Every path that would have added a
    /// voter directly now goes through a **learner phase** first — add as a
    /// non-voting learner, wait for it to catch up
    /// ([`learner_caught_up`](Self::learner_caught_up),
    /// [`RECONFIGURE_LEARNER_CATCH_UP_THRESHOLD`]), promote — so the old
    /// quorum's own margin is never diluted by a newcomer that hasn't proven
    /// it can actually ack anything yet (the hazard the learner class exists
    /// to close; see ADR 0058's Train 1 section). A remove-only delta (no
    /// missing member) is unaffected — the old failure-repair/healthy-move
    /// priority order applies unchanged, straight on the voter set.
    ///
    /// Priority order, most urgent first:
    /// 1. **Remove an extra `Down` voter** (never self) — restores quorum margin
    ///    immediately; this is failure repair, and the removed node isn't going
    ///    to ack anything anyway, so there is nothing to wait for. Never touches
    ///    a learner — a down voter is the only thing this urgent.
    /// 2. **Drop a learner no longer wanted.** `desired` can change out from
    ///    under a learner still mid-catch-up (its node crashed or was
    ///    decommissioned, or a rebalance simply picked someone else) — any
    ///    current learner absent from `desired` is stale by construction (a
    ///    learner in `desired` would still be "missing" from the voter set, so
    ///    `current == desired` together with a non-empty learner set can only
    ///    mean every remaining learner is stale) and is removed directly,
    ///    regardless of its liveness or catch-up progress. Without this, a
    ///    replaced learner would wedge every later step forever.
    /// 3. **Promote a learner that is both still desired and caught up** —
    ///    advances an in-flight move before starting a new one.
    /// 4. **Add a `desired` member that is neither a voter nor a learner, as a
    ///    LEARNER** — never straight to voter. The old voter quorum keeps its
    ///    pre-move margin (and majority size) untouched while the newcomer
    ///    catches up via ordinary log replication/`InstallSnapshot`, instead of
    ///    briefly counting an uncaught-up node toward quorum.
    ///
    ///    While any `desired` member is still a learner mid-catch-up (steps 2/3
    ///    didn't apply this tick), no later step fires — in particular a
    ///    healthy extra voter (step 5) is never removed while a replacement is
    ///    still proving it can keep up.
    /// 5. **Remove an extra *healthy* voter** (never self) — but only once every
    ///    member of `desired` has caught up to this leader's `commit_index`.
    ///    Skipping this gate would let a healthy move (e.g. a rebalance) drop
    ///    quorum to a still-catching-up newcomer, an availability regression
    ///    relative to just leaving the extra replica in place a little longer.
    ///    By the time this step can fire, every `desired` member is already a
    ///    voter (steps 2–4 handle the learner phase first), so this is exactly
    ///    the pre-Train-1 step 3, unchanged.
    /// 6. **The only remaining delta is removing the leader's own replica** —
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
        let current_learners = self.learners();
        if !self.is_leader() {
            return None;
        }
        // Converged: the voter set already matches `desired` AND no learner is
        // left dangling. Must NOT return early on `current == desired` alone —
        // a stray learner at that point is stale by construction (see step 2's
        // doc above) and step 2 needs the chance to clean it up.
        if current == *desired && current_learners.is_empty() {
            return None;
        }
        let me = self.env.node_id();
        // Any extra (non-self) voter, regardless of liveness — used by step 5
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

        // Step 2: drop a stale learner (ADR 0058 Train 1's stuck-learner
        // cleanup — see the doc above).
        if let Some(stale) = current_learners.iter().find(|n| !desired.contains(*n)) {
            if !matches!(
                self.remove_learner(stale.clone()),
                ProposeResult::Accepted { .. }
            ) {
                tracing::debug!(
                    node = %stale,
                    "reconfigure_step: rejected removing a stale learner (retried next tick)"
                );
            }
            return None;
        }

        // Step 3: promote a learner that is both still desired and caught up.
        if let Some(id) = current_learners.iter().find(|n| {
            desired.contains(*n)
                && self.learner_caught_up(n, RECONFIGURE_LEARNER_CATCH_UP_THRESHOLD)
        }) {
            let mut c = current.clone();
            c.insert(id.clone());
            return match self.promote_learner(id.clone()) {
                ProposeResult::Accepted { .. } => Some(c),
                ProposeResult::NotLeader { .. } => None,
            };
        }

        // A `desired` member is still mid-catch-up as a learner: nothing else
        // to do this tick — in particular, never fall through to step 5 and
        // remove a healthy extra voter while a replacement hasn't yet proven
        // it can keep up (the whole point of the learner phase).
        if desired.iter().any(|n| current_learners.contains(n)) {
            return None;
        }

        // Step 4: a `desired` member genuinely missing from both the voter set
        // and the learner set — add it as a LEARNER, never straight to voter.
        if let Some(missing) = desired
            .iter()
            .find(|n| !current.contains(*n) && !current_learners.contains(*n))
        {
            if !matches!(
                self.add_learner(missing.clone()),
                ProposeResult::Accepted { .. }
            ) {
                tracing::debug!(
                    node = %missing,
                    "reconfigure_step: rejected adding a learner (retried next tick)"
                );
            }
            return None;
        }

        // From here on every `desired` member is already a voter — the
        // remaining deltas are exactly the pre-Train-1 steps 3/4.
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

    /// This group's **immutable** declared [`StorageScope`] range (ADR 0050:
    /// a tablet's range never changes; `narrow_scope`/`widen_scope` died with
    /// the zero-copy split). Still read by `animusd`'s pre-propose routing
    /// checks and stamped as proposals' `fence` — both inert-but-present
    /// (ranges no longer mutate, so check and fence always agree) until the
    /// Train B deletion sweep replaces them with the route-time `Active`
    /// filter.
    #[must_use]
    pub fn scope_range(&self) -> KeyRange {
        self.scope.range()
    }

    /// Resolve an intent whose covering transaction's **decided status is
    /// already known** (ADR 0018 §2/PR4) — the decision logic shared by
    /// [`resolve_once_step`](Self::resolve_once_step) (the local-record
    /// path) and [`resolve_intent_given_status`](Self::resolve_intent_given_status)
    /// (the cross-tablet, externally-supplied-status path): `Pending`
    /// can't resolve yet (returns [`ResolveStep::Pending`] carrying
    /// `pending`, the caller-supplied [`IntentInfo`] for *this* intent — see
    /// that variant's doc for why callers need it even in the local case);
    /// `Committed` at or before `read_ts` (`None` = "latest") serves
    /// `staged_value`; `Committed` strictly after `read_ts`, or `Aborted`,
    /// serves whatever `physical_key` held immediately before this intent
    /// (rewinding to `vv_version - 1` — never a tombstone, which would
    /// incorrectly shadow an older, still-live committed value — see
    /// `txn.rs`'s module doc).
    async fn resolve_decided(
        &self,
        physical_key: &[u8],
        vv_version: u64,
        staged_value: Option<Vec<u8>>,
        read_ts: Option<HlcTimestamp>,
        status: &txn::TxnDecisionStatus,
        pending: IntentInfo,
    ) -> ResolveStep {
        match status {
            txn::TxnDecisionStatus::Committed { commit_ts }
                if read_ts.is_none_or(|rt| *commit_ts <= rt) =>
            {
                ResolveStep::Value(staged_value)
            }
            txn::TxnDecisionStatus::Committed { .. } | txn::TxnDecisionStatus::Aborted => {
                ResolveStep::Value(self.prior_committed(physical_key, vv_version).await)
            }
            txn::TxnDecisionStatus::Pending => ResolveStep::Pending(pending),
        }
    }

    /// The key's **last committed value**, read one MVCC version below
    /// `version` — the "one hop back from this intent" lookup shared by
    /// [`resolve_decided`](Self::resolve_decided)'s aborted/too-late branch
    /// and the ADR 0055 eventually-consistent read path.
    ///
    /// A *prior intent* one hop back **should be unreachable since ADR 0018
    /// §2/PR6 (task #16)** — `KvCommand::TxnStage`'s apply-time
    /// writer-push-intents guard rejects a stage over any key still holding
    /// another transaction's unresolved intent, so one hop back from an
    /// intent's own version can only ever be a genuinely committed value or
    /// true absence (see `KvCommand::TxnStage`'s doc for the durability
    /// argument this closes — a corpus depth run found a corrupted MVCC
    /// version chain that made an already-committed value permanently
    /// unreadable). Kept as a defensive fallback rather than an assert: this
    /// function has no way to distinguish "the invariant broke" from "an
    /// older, pre-fix WAL entry replayed on recovery" — conservatively
    /// treating it as absent (never leaking raw envelope bytes to a caller)
    /// is still correct either way.
    async fn prior_committed(&self, physical_key: &[u8], version: u64) -> Option<Vec<u8>> {
        self.storage
            .get_at(physical_key, version.saturating_sub(1))
            .await
            .ok()
            .flatten()
            .and_then(|pvv| match txn::decode_envelope(&pvv.value) {
                txn::Envelope::Committed(v) => Some(v),
                txn::Envelope::Intent { .. } => None,
            })
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
                ..
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
                        // Built up front (not just inside the `Pending`
                        // branch `resolve_decided` might take) since the
                        // fields below are about to be moved into that
                        // call — cheap relative to the storage round trip
                        // just above, and keeps `Pending` symmetric with
                        // `Foreign`'s own `IntentInfo` (torn-pair-fix
                        // stack's ADR 0018 §2 amendment).
                        let pending = IntentInfo {
                            txn_id: txn_id.clone(),
                            record_key: record_key.clone(),
                            record_table: record_table.clone(),
                            staged_value: staged_value.clone(),
                            version: hlc::unpack(vv.version),
                        };
                        self.resolve_decided(
                            physical_key,
                            vv.version,
                            staged_value,
                            read_ts,
                            &r.status.to_public(),
                            pending,
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
                ResolveStep::Pending(_) | ResolveStep::Foreign(_) => {
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
            ResolveStep::Pending(_) | ResolveStep::Foreign(_) => None,
        }
    }

    // ---- eventually-consistent reads (ADR 0055) --------------------------
    //
    // The `ConsistentRead: false` half of the DynamoDB read contract, served
    // from *this* replica's own applied engine state: no ReadIndex barrier,
    // no ceiling proposal, no read-timestamp-cache bump, no leadership
    // requirement, and — deliberately — no network hop and no wait of any
    // kind. That budget is the whole point of the path: anything added here
    // that can block or round-trip silently turns the cheap read back into
    // an expensive one, and no test would notice. Keep it local.

    /// Whether this replica may serve an **eventually-consistent** read (ADR
    /// 0055) — the cheap, purely local freshness gate every stale read is
    /// taken behind.
    ///
    /// True when both hold:
    ///
    /// - the replica knows a **current leader** (its Raft `leader_id` is
    ///   set). A replica that has never heard an `AppendEntries` — a freshly
    ///   added voter (ADR 0029 rebalance), a node between start and its
    ///   first heartbeat — holds an engine that is not yet *any* state this
    ///   tablet ever had, so serving it would report **false absence**.
    ///   That is categorically worse than staleness: an eventual read may
    ///   return an older committed state, never a state that never existed
    ///   (the ADR 0033 read-path lesson, in its read-your-replica form).
    /// - its **engine** holds every entry it knows to be committed
    ///   (`engine_applied_index() >= commit_index()`). This excludes one
    ///   carrying a committed-but-not-yet-merged log tail, and — the case
    ///   that actually matters — one **mid-`InstallSnapshot`**: `RaftCore`
    ///   sets `commit_index = snapshot.last_index` the moment the last chunk
    ///   arrives, while `engine_applied` only advances after the apply task
    ///   has merged the whole image (`install_engine_image` →
    ///   `fetch_max(last_index)`). The gate is therefore closed for exactly
    ///   the window in which this engine holds a half-written image — the
    ///   one state a replica can be in that is not a prefix of the log at
    ///   all, and so the one an eventual read must never touch.
    ///
    /// Deliberately **not** a staleness *bound*. A replica partitioned away
    /// from its leader passes this gate and answers from an arbitrarily old
    /// state — one that is still genuinely committed and still a genuine
    /// prefix of this tablet's log, which is exactly what DynamoDB's
    /// eventually-consistent read promises and all it promises. A caller
    /// that needs a bound asks for `ConsistentRead: true`.
    ///
    /// Never wakes a quiesced group (ADR 0048): an eventual read needs no
    /// Raft activity at all, and a quiesced group is idle by construction —
    /// hence fully applied, hence current.
    #[must_use]
    pub fn stale_read_ready(&self) -> bool {
        // One lock acquisition for both core facts. `engine_applied` is read
        // *after* releasing it and only ever grows, so a concurrent apply can
        // make this false-negative (a read that falls back to the strong
        // path, which is always correct) but never false-positive.
        let (has_leader, commit) = {
            let core = self.lock();
            (core.leader().is_some(), core.commit_index())
        };
        Self::stale_read_ready_decision(has_leader, self.engine_applied_index(), commit)
    }

    /// The pure decision behind [`stale_read_ready`](Self::stale_read_ready),
    /// extracted so its truth table is directly unit-testable without
    /// constructing a `RaftKvNode`/`RaftCore` at all. See that method's doc
    /// for what each input means and why both must hold. Takes no `&self` —
    /// a plain associated function over exactly the three facts the gate
    /// reads: whether the replica currently knows a leader, how far its own
    /// engine has applied, and the core's committed index.
    fn stale_read_ready_decision(
        has_leader: bool,
        engine_applied_index: u64,
        commit_index: u64,
    ) -> bool {
        has_leader && engine_applied_index >= commit_index
    }

    /// An **eventually-consistent** read of `key` from this replica's own
    /// engine (ADR 0055): no ReadIndex barrier, no leadership requirement,
    /// no wait. The caller gates it on
    /// [`stale_read_ready`](Self::stale_read_ready) first.
    ///
    /// The two `None`s are disambiguated exactly as
    /// [`linearizable_get_served`](Self::linearizable_get_served) does, and
    /// for the identical reason (the ADR 0033 read-path fix): the **outer**
    /// `Option` is "was this read served at all" — `None` means the engine
    /// refused the read, which a caller must never report to a client as
    /// absence — and the **inner** one is the served answer (`Some(None)` =
    /// the key is genuinely absent).
    ///
    /// A key covered by an **unresolved intent** (ADR 0018 §2) reads as its
    /// **last committed value** (the MVCC version one below the intent's own),
    /// rather than as absent the way [`local_get`](Self::local_get)'s raw
    /// peek reports it, and without the anchor-tablet round trip a
    /// linearizable read pays to decide the intent. That value is a state
    /// this tablet genuinely had, and the covering transaction has not
    /// committed as far as this replica knows — so returning it is staleness
    /// and nothing more. Reporting the key as absent would instead fabricate
    /// a deletion that never happened, which the eventual contract does not
    /// license.
    pub async fn stale_get_served(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        let physical = self.scope.physical(key);
        let Ok(found) = self.storage.get(&physical).await else {
            return None;
        };
        match found {
            Some(vv) => Some(self.stale_value(&physical, vv).await),
            None => Some(None),
        }
    }

    /// An **eventually-consistent** range scan of the base scope over
    /// `[start, end)` (ADR 0055) — [`local_scan`](Self::local_scan)'s range,
    /// bounds and ordering contract exactly (`end == None` is unbounded
    /// above but still bounded to *this* scope), resolving each row the way
    /// [`stale_get_served`](Self::stale_get_served) resolves a point read.
    ///
    /// The one difference from `local_scan` that matters: a row under an
    /// unresolved intent contributes its **last committed value** instead of
    /// being dropped. Dropping it is right for `local_scan`'s admin/debug
    /// callers and wrong here — a client-visible scan that silently omits an
    /// item which exists is a fabricated deletion, not staleness.
    pub async fn stale_scan(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.stale_scan_ordered(start, end, limit, false).await
    }

    /// [`stale_scan`](Self::stale_scan)'s **descending** dual — the eventual
    /// counterpart of [`local_scan_rev`](Self::local_scan_rev), serving a
    /// `ConsistentRead: false` `Query` with `ScanIndexForward: false`.
    pub async fn stale_scan_rev(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.stale_scan_ordered(start, end, limit, true).await
    }

    /// Shared body of the two above — the eventual twin of
    /// [`local_scan_ordered`](Self::local_scan_ordered): same rows
    /// ([`raw_base_rows`](Self::raw_base_rows)), same `reverse`/`limit` tail
    /// ([`order_and_limit`](Self::order_and_limit)), different envelope
    /// resolution.
    async fn stale_scan_ordered(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let raw = self.raw_base_rows(start, end).await;
        let pairs = self.stale_scan_rows(raw).await;
        Self::order_and_limit(pairs, limit, reverse)
    }

    /// [`resolve_scan_rows`](Self::resolve_scan_rows)'s eventual counterpart
    /// (ADR 0055): drop this crate's internal txn-record marker keys exactly
    /// as that one does, then resolve each remaining row with
    /// [`stale_value`](Self::stale_value) — so an intent-covered row keeps
    /// its last committed value instead of vanishing from the page.
    /// Resolution runs **before** the caller's `limit`, mirroring
    /// `local_scan_ordered`'s own ordering, so an internal marker key never
    /// consumes one of the caller's requested slots.
    async fn stale_scan_rows(
        &self,
        rows: Vec<(Vec<u8>, animus_storage::VersionedValue)>,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut out = Vec::with_capacity(rows.len());
        for (key, vv) in rows {
            if txn::is_record_key(&key) {
                continue;
            }
            let physical = self.scope.physical(&key);
            if let Some(v) = self.stale_value(&physical, vv).await {
                out.push((key, v));
            }
        }
        out
    }

    /// Resolve one stored envelope for an eventually-consistent read (ADR
    /// 0055) — the shared body of [`stale_get_served`](Self::stale_get_served)
    /// and [`stale_scan_rows`](Self::stale_scan_rows). Pure local engine
    /// work: a committed envelope *is* the value; an unresolved intent falls
    /// back one MVCC version to the key's last committed value (see
    /// [`prior_committed`](Self::prior_committed)).
    async fn stale_value(
        &self,
        physical_key: &[u8],
        vv: animus_storage::VersionedValue,
    ) -> Option<Vec<u8>> {
        match txn::decode_envelope(&vv.value) {
            txn::Envelope::Committed(v) => Some(v),
            txn::Envelope::Intent { .. } => self.prior_committed(physical_key, vv.version).await,
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
    ///
    /// `limit` is a **per-tablet cap, not pushdown** — `StorageEngine::scan`
    /// has no limit parameter of its own, so this still reads the whole
    /// `[start, end)` sub-range off the engine; the win is a smaller wire
    /// payload and less coordinator-side memory for a caller that only
    /// wants a bounded prefix (`ClientCtx::cp_scan_kind_table`'s per-tablet
    /// fan-out, ADR 0041 §5), never reduced engine I/O. Applied **after**
    /// the intent filter below (mirroring [`local_scan`](Self::local_scan)'s
    /// identical ordering), so a still-`Pending` row interleaved in the
    /// requested range never silently consumes one of the caller's
    /// requested slots.
    pub async fn local_scan_kind(
        &self,
        kind: u8,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.local_scan_kind_ordered(kind, start, end, limit, false)
            .await
    }

    /// [`local_scan_kind`](Self::local_scan_kind)'s **descending** dual — the
    /// kind-scoped counterpart of [`local_scan_rev`](Self::local_scan_rev),
    /// serving an LSI `Query` with `ScanIndexForward: false`.
    pub async fn local_scan_kind_rev(
        &self,
        kind: u8,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.local_scan_kind_ordered(kind, start, end, limit, true)
            .await
    }

    /// Shared body of the two above; `reverse` picks which end of the ordered
    /// range `limit` keeps and the order rows are returned in.
    async fn local_scan_kind_ordered(
        &self,
        kind: u8,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
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
        let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = raw
            .into_iter()
            .filter_map(|(k, vv)| {
                let logical = scope.strip_in_range(&k)?.to_vec();
                match txn::decode_envelope(&vv.value) {
                    txn::Envelope::Committed(v) => Some((logical, v)),
                    txn::Envelope::Intent { .. } => None,
                }
            })
            .collect();
        if reverse {
            if let Some(n) = limit
                && pairs.len() > n
            {
                pairs.drain(..pairs.len() - n);
            }
            pairs.reverse();
        } else if let Some(n) = limit {
            pairs.truncate(n);
        }
        pairs
    }

    /// This tablet's own current engine watermark — the highest committed
    /// MVCC version this replica's engine holds, or `0` if empty
    /// (`StorageEngine::latest_version`). A cheap, synchronous, purely local
    /// read: no barrier, no leadership requirement.
    ///
    /// The on-demand backup capture driver's own snapshot-pin primitive
    /// (`animusd`, ADR 0059 §4): pinned **once**, at a tablet's own capture
    /// start, and never re-derived on a later tick or across a leader
    /// change — every subsequent
    /// [`local_scan_kind_snapshot`](Self::local_scan_kind_snapshot) call for
    /// that capture replays the identical `version_ceiling`, which is what
    /// makes a retried chunk (a crash between a store `put` and its own
    /// cursor-advance commit) re-derive byte-identical content rather than a
    /// fresh, wider slice a re-pinned watermark would produce.
    #[must_use]
    pub fn engine_latest_version(&self) -> u64 {
        self.storage.latest_version()
    }

    /// A **snapshot-pinned**, intent-resolved sweep of a kind scope, chunked
    /// via a caller-supplied resumable cursor (ADR 0059 §4/§5) — the
    /// on-demand backup capture driver's core read primitive (`animusd`, a
    /// later PR).
    ///
    /// Unlike [`local_scan_kind`](Self::local_scan_kind) (always "latest"),
    /// every row is read **as of `version_ceiling`**
    /// (`StorageEngine::scan_at`, the same snapshot primitive
    /// [`scan_at`](Self::scan_at) reads a live transaction against) — a
    /// fixed watermark the caller pins once at capture start
    /// ([`engine_latest_version`](Self::engine_latest_version)) and replays
    /// on every later tick, so a capture spanning many ticks — and, across a
    /// leader change, many different replicas — always resolves the
    /// identical row set (every correctly-caught-up replica applies the
    /// same Raft log in the same order, so "state as of version V" is the
    /// same logical snapshot everywhere). Unlike the whole-tablet
    /// snapshot-transfer primitive this crate builds for split-build
    /// (ADR 0050's `engine_image`), every value is resolved through the
    /// ordinary intent-resolution discipline
    /// ([`resolve_once_step`](Self::resolve_once_step) — the same
    /// non-blocking, silently-omit-a-still-`Pending`-row behavior
    /// [`resolve_scan_rows`](Self::resolve_scan_rows) already gives every
    /// ordinary scan) rather than copied as a raw envelope-tagged byte
    /// string: ADR 0059 §5's "a backup holds only committed values, never a
    /// raw intent envelope" rule, satisfied by construction here rather than
    /// by a caller-side filter.
    ///
    /// Returns up to `limit` `(logical_key, value, version)` triples
    /// starting at `start` (inclusive), plus a resumable cursor:
    /// `Some(next_start)` when more rows remain past what was returned (pass
    /// it back as the next call's own `start`), `None` once this kind
    /// scope's own physical end is reached. An unknown `kind` scans as
    /// empty, mirroring `local_scan_kind`.
    ///
    /// **Cost model, matching [`local_scan_kind`](Self::local_scan_kind)/
    /// `animusd`'s TTL-reaper `local_scan_kind_capped` (the identical
    /// trade-off, `animusd/CLAUDE.md`'s `ttl_reaper.rs` entry)**: `limit`
    /// bounds the *returned* rows, not engine I/O — this still reads the
    /// whole `[start, end)` sub-range at `version_ceiling` off the engine
    /// per call, so a caller sweeping a large scope to completion in small
    /// chunks pays a re-scan of the remaining range on every chunk. A known,
    /// documented follow-up optimization (the split-build driver's own
    /// "three full engine scans... the named next win" precedent,
    /// `animusd/CLAUDE.md`), not a correctness gap.
    pub async fn local_scan_kind_snapshot(
        &self,
        kind: u8,
        start: &[u8],
        version_ceiling: u64,
        limit: usize,
    ) -> (Vec<(Vec<u8>, Vec<u8>, u64)>, Option<Vec<u8>>) {
        let Some(scope) = self.kind_scopes.get(kind as usize) else {
            return (Vec::new(), None);
        };
        let Some(physical_end) = scope.physical_bounds().1 else {
            return (Vec::new(), None);
        };
        let raw: Vec<(Vec<u8>, animus_storage::VersionedValue)> = self
            .storage
            .scan_at(&scope.physical(start), &physical_end, version_ceiling)
            .await
            .ok()
            .into_iter()
            .flatten()
            .collect();
        let mut out = Vec::new();
        let mut next = None;
        for (physical_key, vv) in raw {
            let Some(logical) = scope.strip_in_range(&physical_key) else {
                continue;
            };
            // A transaction-record marker key (`txn::is_record_key`) is
            // this crate's own internal bookkeeping, never user data —
            // `resolve_scan_rows` drops it for the identical reason on
            // every ordinary scan. Checked here, before the `limit` gate,
            // so a marker interleaved in the range never silently consumes
            // one of the caller's requested chunk slots.
            if txn::is_record_key(logical) {
                continue;
            }
            if out.len() >= limit {
                next = Some(logical.to_vec());
                break;
            }
            let logical = logical.to_vec();
            let version = vv.version;
            if let ResolveStep::Value(Some(v)) =
                self.resolve_once_step(&physical_key, vv, None).await
            {
                out.push((logical, v, version));
            }
        }
        (out, next)
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
    /// scope, as `(tag, watermark)` pairs. `cursor_min_watermark`'s
    /// min-over-rows rule (ADR 0042 §7) tolerates more than one row per tag
    /// showing up here — historically the shape a merge survivor's widened
    /// scope could produce (one row per absorbed tablet's own lineage, still
    /// physically present on the shared engine, since `StorageScope::
    /// with_kind` shares one live `KeyRange` across every kind, so widening
    /// exposed rows a sibling wrote while it was its own tablet). Tablet
    /// merge no longer exists (ADR 0044, tablets are split-only), so a
    /// tablet's own scope only ever narrows now — the scenario that produced
    /// more than one row per tag doesn't structurally arise under split
    /// alone. `cursor_min_watermark`'s multi-row tolerance is kept anyway
    /// (defensive, unproven-unreachable rather than proven-dead); see
    /// [`cursor_min_watermark`](Self::cursor_min_watermark). A row whose raw
    /// bytes fail to decode is dropped rather than surfaced, mirroring
    /// [`cursor_watermark`](Self::cursor_watermark)'s own defensive read.
    pub async fn cursor_rows(&self) -> Vec<(String, HlcTimestamp)> {
        self.cursor_rows_with_token()
            .await
            .into_iter()
            .map(|(_, tag, ts)| (tag, ts))
            .collect()
    }

    /// As [`cursor_rows`](Self::cursor_rows), but keeping the **range-start**
    /// each row's own key names alongside its tag (issue #355: a cursor
    /// key's leading bytes are now the writing tablet's own live
    /// `range.start`, embedded verbatim rather than truncated to a fixed
    /// token — see [`cursor::cursor_key`]'s own doc). `cursor_rows` drops it
    /// (none of its own callers need it). Its original caller — the ADR
    /// 0042 §7 trim janitor's merge-residue cleanup (`animusd::index_drain`),
    /// which told "this tablet's own row" from "a still-physically-present
    /// absorbed sibling's row" — no longer exists: tablet merge was removed
    /// entirely (ADR 0044). This method currently has **no production
    /// caller**; kept in case a future consumer needs the same
    /// row-provenance disambiguation.
    pub async fn cursor_rows_with_token(&self) -> Vec<(Vec<u8>, String, HlcTimestamp)> {
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
                let (range_start, tag) = cursor::parse_cursor_key(logical)?;
                let ts = match txn::decode_envelope(&vv.value) {
                    txn::Envelope::Committed(v) => cursor::decode_watermark(&v)?,
                    txn::Envelope::Intent { .. } => return None,
                };
                Some((range_start.to_vec(), tag.to_string(), ts))
            })
            .collect()
    }

    /// The ADR 0042 §7 **min-over-rows** watermark for `consumer`: the
    /// minimum watermark among every `KIND_CURSOR` row tagged `consumer` in
    /// this tablet's own scope (historically, possibly a merge survivor's
    /// widened scope — tablets are split-only now, ADR 0044), or `None` if
    /// no such row exists at all — the "expected tag with no row ⇒ `W = 0`,
    /// no trim"
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
    ///
    /// `limit` is threaded straight to [`local_scan_kind`](Self::local_scan_kind)
    /// — see that method's doc for why this is a **per-tablet cap, not
    /// pushdown**.
    /// [`linearizable_scan_kind`](Self::linearizable_scan_kind)'s **descending**
    /// dual: same barrier, same whole-span `ts_cache` bump, highest rows first.
    pub async fn linearizable_scan_kind_rev(
        &self,
        kind: u8,
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
        let rows = self.local_scan_kind_rev(kind, start, end, limit).await;
        self.ts_cache.lock().expect("ts cache poisoned").bump(
            start.to_vec(),
            end.map(<[u8]>::to_vec),
            ts,
        );
        Some(rows)
    }

    pub async fn linearizable_scan_kind(
        &self,
        kind: u8,
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
        let rows = self.local_scan_kind(kind, start, end, limit).await;
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

    /// The split-build driver's raw row read (ADR 0050 Train B rung 4): every
    /// row of one kind scope — **tombstones and MVCC versions retained, value
    /// bytes verbatim** (envelope tag included, so a staged intent ships as
    /// an intent, fork F7) — optionally bounded to `[start, end)` in
    /// *logical* key space (the tail's per-token re-read; `None` = the whole
    /// kind scope, the bulk pass). Leader-local and non-linearizable by
    /// design: the driver re-reads dirty rows until convergence, and the
    /// freeze (B5) is what makes the final pass authoritative.
    pub async fn seed_rows_kind(
        &self,
        kind_idx: usize,
        logical_range: Option<(&[u8], &[u8])>,
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>, u64)> {
        let kscope = &self.kind_scopes[kind_idx];
        let (start, end) = match logical_range {
            Some((lo, hi)) => (kscope.physical(lo), kscope.physical(hi)),
            None => {
                let (start, end) = kscope.physical_bounds();
                let Some(end) = end else {
                    return Vec::new(); // only `StorageScope::whole()`; no real tablet
                };
                (start, end)
            }
        };
        self.storage
            .scan_with_tombstones(&start, &end)
            .await
            .ok()
            .into_iter()
            .flatten()
            .filter_map(|(k, slot, version)| {
                let logical = kscope.strip_in_range(&k)?.to_vec();
                Some((logical, slot, version))
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
            ResolveStep::Pending(info) => FastRead::Pending(info),
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
                record_key,
                record_table,
                staged_value,
                ..
            } if &found == txn_id => {
                // Built for parity with `resolve_once_step`'s own call —
                // unused here whenever `status` isn't `Pending` (the only
                // caller, `animusd`, only ever supplies an
                // already-decided status), but `resolve_decided` needs one
                // value of this shape regardless of which branch it takes.
                let pending = IntentInfo {
                    txn_id: found.clone(),
                    record_key,
                    record_table,
                    staged_value: staged_value.clone(),
                    version: hlc::unpack(vv.version),
                };
                match self
                    .resolve_decided(
                        &physical,
                        vv.version,
                        staged_value,
                        read_ts,
                        &status,
                        pending,
                    )
                    .await
                {
                    ResolveStep::Value(v) => Some(v),
                    ResolveStep::Pending(_) | ResolveStep::Foreign(_) => None,
                }
            }
            // No longer our intent (already resolved, or superseded) —
            // resolve whatever is actually there now instead.
            _ => match self.resolve_once_step(&physical, vv, read_ts).await {
                ResolveStep::Value(v) => Some(v),
                ResolveStep::Pending(_) | ResolveStep::Foreign(_) => None,
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
    /// range. This is the CP read primitive the DynamoDB `Query`/`Scan`
    /// edge uses in place of the AP quorum scan.
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

    /// [`linearizable_scan`](Self::linearizable_scan)'s **descending** dual —
    /// the same ReadIndex barrier and the same read-timestamp-cache bump over
    /// the whole requested span, but `limit` keeps the *highest* rows and they
    /// come back highest-key-first. The CP read primitive behind a DynamoDB
    /// `Query` with `ScanIndexForward: false`.
    ///
    /// The `ts_cache` bump is deliberately identical to the ascending form's:
    /// it covers `[start, end)` entire, not the rows returned, so which end of
    /// the range a `limit` happened to keep cannot change what a later write
    /// has to be ordered above.
    pub async fn linearizable_scan_rev(
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
        let rows = self.local_scan_rev(start, end, limit).await;
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
        self.local_scan_ordered(start, end, limit, false).await
    }

    /// [`local_scan`](Self::local_scan)'s **descending** dual: the same range,
    /// but `limit` keeps the *last* rows rather than the first, and the result
    /// is returned highest-key-first. Serves a DynamoDB `Query` with
    /// `ScanIndexForward: false`.
    ///
    /// This costs no more than the ascending form: `local_scan_ordered` pushes
    /// the *range* into the engine and the engine returns the whole of it
    /// key-ordered (`StorageEngine::scan` takes no limit) — `limit` has always
    /// been a post-read take. Taking from the tail is therefore the same work
    /// as taking from the head, and — the point of doing it here rather than in
    /// the edge — it is what keeps a descending page's *network* payload
    /// bounded by `limit` when the read is forwarded to another node.
    pub async fn local_scan_rev(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.local_scan_ordered(start, end, limit, true).await
    }

    /// The shared body of [`local_scan`](Self::local_scan) and
    /// [`local_scan_rev`](Self::local_scan_rev); `reverse` picks which end of
    /// the ordered range `limit` keeps and which order rows come back in.
    async fn local_scan_ordered(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let raw = self.raw_base_rows(start, end).await;
        // ADR 0018 §2/PR3: filter out this crate's internal txn-record
        // marker keys and resolve every remaining row's value envelope
        // (`resolve_scan_rows`'s doc) — applied *before* `limit` so an
        // internal marker key or a still-`Pending` row interleaved in the
        // requested range never silently consumes one of the caller's
        // requested slots.
        let pairs = self.resolve_scan_rows(raw, None).await;
        Self::order_and_limit(pairs, limit, reverse)
    }

    /// The raw, still-enveloped `(logical key, versioned value)` rows of this
    /// group's **base** scope over `[start, end)`, in key order — the shared
    /// engine half of [`local_scan_ordered`](Self::local_scan_ordered) and
    /// its ADR 0055 eventually-consistent twin
    /// [`stale_scan_ordered`](Self::stale_scan_ordered). The two differ only
    /// in how they resolve a row's envelope, never in which rows they read,
    /// so the range/bounds reasoning below lives here once.
    async fn raw_base_rows(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Vec<(Vec<u8>, animus_storage::VersionedValue)> {
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
        // Both branches bound/filter to `self.scope`'s kind prefix — the
        // engine is this tablet's own (ADR 0050), so the filter's remaining
        // job is separating this kind's rows from sibling kinds' and from
        // the engine-global reserved-namespace markers.
        match end {
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
        }
    }

    /// Apply a scan's `reverse`/`limit` tail to already-key-ordered,
    /// already-resolved rows — the shared post-step of every base-scope scan
    /// flavor (linearizable, local, and the ADR 0055 eventual one).
    fn order_and_limit(
        mut pairs: Vec<(Vec<u8>, Vec<u8>)>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        if reverse {
            // Keep the *highest* `n` of the range, then hand them back
            // highest-first. Draining the head (rather than truncating the
            // tail) is what makes this the descending page rather than the
            // ascending one.
            if let Some(n) = limit
                && pairs.len() > n
            {
                pairs.drain(..pairs.len() - n);
            }
            pairs.reverse();
        } else if let Some(n) = limit {
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

    /// [`approx_bytes`](Self::approx_bytes)'s **kind-scoped** sibling (ADR
    /// 0042/0043's round-3 sealer PR): the identical cheap estimate, but
    /// over one row-kind's own `StorageScope` (`self.kind_scopes[kind]`)
    /// instead of the base scope `approx_bytes` is deliberately pinned to.
    /// The seal arm's size trigger needs exactly this — `KIND_CHANGE`'s own
    /// bytes, not the base row bytes `approx_bytes` measures (ADR 0034's
    /// own fix made `approx_bytes` base-only *specifically* so auto-split
    /// stops reacting to change-log churn; the seal arm is the one caller
    /// that genuinely wants the change log's own size, so it needs its own
    /// accessor rather than reusing that one). `0` for an unknown kind
    /// index (defensive; every caller here passes a real [`KIND_CHANGE`]
    /// constant) or a storage error, matching `approx_bytes`'s own "never
    /// block the periodic gate on an estimate" contract.
    pub async fn approx_bytes_kind(&self, kind: u8) -> u64 {
        let Some(scope) = self.kind_scopes.get(kind as usize) else {
            return 0;
        };
        let (start, end) = scope.physical_bounds();
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
        match self.propose_ordered(|_term| {
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

/// Record the snapshot-shipping + replication-traffic metrics implied by the
/// messages the consensus loop just emitted (ADR 0015), mirroring the control
/// plane's `record_outbound`: every outbound `InstallSnapshot` is one chunk
/// actually *shipped*; an outbound `InstallSnapshotResp` whose `last_index >
/// 0` marks a completed *install* on the follower that just finished
/// (observed here since the follower is what emits the ack); every outbound
/// `AppendEntries` (replication or heartbeat) counts once — the per-tablet
/// counterpart of the control plane's `AppendEntriesSent`, and what an
/// idle/quiesced group's own heartbeat traffic going flat (ADR 0044 phase-1)
/// is measured against. A pure read of `outs`.
fn record_kv_outbound(metrics: &MetricsHandle, outs: &[(NodeId, KvWire)]) {
    for (_, wire) in outs {
        if let KvWire::Raft(msg) = wire {
            match msg {
                RaftMsg::AppendEntries { .. } => metrics.incr(Metric::CpAppendEntriesSent),
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
///
/// **Raises `apply_signal`** (ADR 0044 phase-1 PR1) whenever it reaches
/// `mark_durable_through`: the leader's apply frontier is `min(commit_index,
/// durable_index)` (`RaftCore::apply`'s doc), so advancing `durable_index` here is
/// exactly the point that can newly unblock already-committed-but-not-yet-durable
/// entries for the apply task to merge — a transition the follower-side in-line
/// apply (on `AppendEntries`, gated on `commit_index` alone) doesn't need, but this
/// leader-side one does.
///
/// **Halted-gated error tolerance** (mirrors the compaction path's `env.replace`
/// handling below): a `shutdown()` that lands mid-append/sync — aborting tasks and
/// tearing down the env, or a test's `TempDir` deleting the WAL file out from under
/// a still-running loop — can surface as an `env.append`/`env.sync` error here.
/// Tolerated **only** while `halted` is set: this returns early with **no**
/// `mark_durable_through` (never mark durability that didn't happen) and no
/// `apply_signal` notify, leaving the driver's own top-of-loop `halted` check
/// (which `shutdown()`'s `wake_signal.notify()` promptly wakes) to exit the loop —
/// so a tolerated error can't spin the loop hot or ack anything. A failure while
/// *not* halted is a real durability fault on a live leader (crash-stop-before-ack)
/// and stays exactly as loud as before: a hard panic.
async fn persist_wal<E: Env>(
    env: &E,
    wal: &str,
    core: &Arc<Mutex<KvCore>>,
    wal_lock: &AsyncMutex<()>,
    apply_signal: &ApplySignal,
    halted: &AtomicBool,
    progress: &PersistProgress,
) {
    let _wal = wal_lock.lock().await;
    // The round number is claimed in the *same* core-lock acquisition as the
    // drain (issue #279): that is what lets the consensus loop ask "which round
    // covers the mutation I just made?" and get an answer no concurrent
    // compaction can invalidate between the question and the answer. An empty
    // drain consumes no round — it persists nothing, and `PersistProgress::gate`
    // relies on the latest round being the one that took the records.
    let (records, through, round) = {
        let mut c = core.lock().expect("raftkv core poisoned");
        let (records, round) = persist_round::drain_for_round(&mut c, progress);
        (records, c.last_log_index(), round)
    };
    let Some(round) = round else {
        debug_assert!(records.is_empty());
        return;
    };
    for record in &records {
        if let Err(e) = env
            .append(wal, &PersistedState::encode_record(record))
            .await
        {
            assert!(
                halted.load(Ordering::SeqCst),
                "raftkv wal append failed while running: {e}"
            );
            return;
        }
    }
    if let Err(e) = env.sync(wal).await {
        assert!(
            halted.load(Ordering::SeqCst),
            "raftkv wal sync failed while running: {e}"
        );
        return;
    }
    // Durable now: advance the log watermark and the round watermark under one
    // acquisition, then release whatever the consensus loop buffered on this
    // round (`complete_drain` wakes it).
    {
        let mut c = core.lock().expect("raftkv core poisoned");
        c.mark_durable_through(through);
        progress.complete_drain(round);
    }
    apply_signal.notify();
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

/// **The one shared "materialize derived writes at this ts" helper (ADR
/// 0046 binding decision)** — `KvCommand::KindBatch`'s apply arm and
/// `KvCommand::TxnResolve`'s commit branch both call this and only this,
/// so their output is byte-identical for identical payloads. Queues every
/// `(kind, key, value)` write (`None` = tombstone) into `pending` at
/// `hlc::pack(ts)`, then — if `change_log` is present — completes its key as
/// `prefix || hlc::pack(ts)` and queues it too, in [`KIND_CHANGE`]'s scope.
/// `ts` is always the caller's OWN entry's commit timestamp: `KindBatch`
/// passes its own entry's `ts`; `TxnResolve` passes the *resolve* entry's
/// `ts` (never the transaction's `commit_ts`, and never the stage's `ts` —
/// see `txn::TxnWrite`'s doc and ADR 0018 §2's B1 amendment for why a
/// change record must be keyed by the entry that actually fixes its
/// position in this tablet's own commit order). An unknown row kind is
/// skipped with a warning, never guessed at — the same discipline
/// `KindBatch`'s own arm already had before this extraction.
fn materialize_derived(
    kind_scopes: &[StorageScope; ALL_KINDS.len()],
    writes: &[KindWrite],
    change_log: &[(Vec<u8>, Vec<u8>)],
    ts: HlcTimestamp,
    pending: &mut Vec<MergeOp>,
) {
    for (kind, key, value) in writes {
        let Some(kscope) = kind_scopes.get(*kind as usize) else {
            tracing::warn!(
                kind,
                "materialize_derived: write of unknown row kind skipped"
            );
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
    // Each change-log record's key is completed here, with THIS caller's
    // commit timestamp — the only one that agrees with this entry's
    // position in the log (ADR 0041 §4a / ADR 0046 principle 1). Several
    // records in one entry (a marker-table batch) share the ts; their
    // per-item prefixes keep the completed keys distinct.
    for (prefix, record) in change_log {
        let mut key = prefix.clone();
        key.extend_from_slice(&hlc::pack(ts).to_be_bytes());
        pending.push(MergeOp::put(
            kind_scopes[KIND_CHANGE as usize].physical(&key),
            txn::encode_committed(record),
            hlc::pack(ts),
        ));
    }
}

/// ADR 0046 A1: every kind-write key a [`txn::TxnWrite`] stages must lead
/// with `base_key`'s own partition token (ADR 0022) — see the call sites'
/// doc for why this is checked, not assumed. `base_key` shorter than a full
/// token is itself invalid (every real data-plane key leads with one).
fn kind_writes_token_valid(base_key: &[u8], kind_writes: &[KindWrite]) -> bool {
    let tb = animus_tablet::TOKEN_BYTES;
    base_key.len() >= tb
        && kind_writes
            .iter()
            .all(|(_, kk, _)| kk.len() >= tb && kk[..tb] == base_key[..tb])
}

/// ADR 0049 §3: a [`txn::TxnWrite`]'s `stage_marker` prefix must lead with
/// its own base key's partition token, exactly like
/// [`kind_writes_token_valid`]'s rule for kind-write keys and for the
/// identical reason — the stage entry's `fence` covers `base_key`, so a
/// token-matching marker key sits at the same tablet-range position; an
/// arbitrary (wire-reachable, via `ClientRequest::TxnPrepare`) prefix could
/// otherwise land a change-log row outside this tablet's own declared range.
/// Validated, never assumed; a miss folds into the same structural `Fenced`
/// outcome bucket.
fn stage_marker_token_valid(base_key: &[u8], stage_marker: Option<&(Vec<u8>, Vec<u8>)>) -> bool {
    let tb = animus_tablet::TOKEN_BYTES;
    stage_marker.is_none_or(|(prefix, _)| {
        base_key.len() >= tb && prefix.len() >= tb && prefix[..tb] == base_key[..tb]
    })
}

/// The [`txn::TxnWrite::change_log`] twin of [`stage_marker_token_valid`]
/// (ADR 0049 Train A rung 4): the resolve-time change record's key prefix is
/// staged through the identical wire-reachable payload
/// (`ClientRequest::TxnPrepare`) as `kind_writes`/`stage_marker`, yet was
/// the one of the three that went unvalidated — `TxnResolve` completes
/// `change_log`'s key with its own `ts` and writes it wherever the staged
/// prefix points, so a mis-tokened prefix could land a change-log row
/// outside the anchor's own tablet range long after the stage's fence check
/// passed. Same rule, same structural `Fenced` bucket, validated at stage
/// (the serialization point that admits the payload), never at resolve
/// (which must stay able to finish any stage that was admitted).
fn change_log_token_valid(base_key: &[u8], change_log: Option<&(Vec<u8>, Vec<u8>)>) -> bool {
    let tb = animus_tablet::TOKEN_BYTES;
    change_log.is_none_or(|(prefix, _)| {
        base_key.len() >= tb && prefix.len() >= tb && prefix[..tb] == base_key[..tb]
    })
}

/// Logged-warning cap for [`surface_suspicious_merge_noop`] (below): the
/// [`Metric`] counters there are always incremented (cheap, unconditional),
/// but a genuinely-reoccurring bug logging one line per applied entry would
/// flood the log with no signal past the first handful — capped so the
/// first occurrences are loud without a live incident drowning everything
/// else out. Threaded per apply task, like `max_applied_ts`/`sealed` — this
/// task is this group's sole writer, so a plain `&mut u32` needs no atomic.
const SUSPICIOUS_MERGE_NOOP_LOG_CAP: u32 = 20;

/// **Part B of the ADR 0018 §2 write-loss amendment (the seatbelt).**
/// Surface a `storage.merge`/`merge_tombstone` call that returned
/// `Ok(false)` ("did not take effect") at one of the three apply-arm sites
/// that used to discard that bool outright via `.expect(..)`
/// (`TxnStage`'s intent write, `TxnResolve`'s commit/abort-restore writes,
/// `Cas`'s swap) — sites whose *caller-visible* outcome
/// (`StageOutcome::Staged`, a resolved commit/abort, a decided CAS) is
/// computed independently of whether the merge itself actually landed.
/// `StorageEngine::merge`'s silent per-key-LWW no-op contract was designed
/// for the deleted leaderless-AP plane's stale-replay tolerance (ADR 0001,
/// gone under ADR 0019); every one of this CP plane's callers instead
/// assumes a write its own gating logic accepted genuinely lands, so a
/// silent `false` here is exactly the class of failure Bug 3 (acked
/// participant writes silently lost) hid in — see the amendment for the
/// full mechanism (the real root cause was `ClientCtx::recovery_resolve`'s
/// table-only grouping misrouting a resolve to the wrong tablet, closed at
/// the source in `animusd`, with `KvCommand::TxnResolve`'s own `fence` as
/// the structural seatbelt against a repeat — see that variant's doc; this
/// function closes the class so the *next* bug shaped like it can't hide
/// the same way).
///
/// **Why this can't be a bare, unconditional panic/hard-assert.** Apply
/// arms can legitimately re-run after a crash: WAL recovery replays this
/// group's log tail unconditionally (`drive`'s doc), and the *engine*'s own
/// durability can lead the *driver*'s in-memory bookkeeping — `max_applied_
/// ts`/`engine_applied` both start fresh "each time this task starts,
/// including after a restart" (`apply_loop`'s doc) — so re-applying an
/// entry the engine already durably reflects from before this process
/// started is expected, and `merge` correctly no-ops on it (the identical
/// value, at the identical version, is already there). A bare assert here
/// would turn every ordinary restart-and-replay into a crash loop.
///
/// **The replay-safe distinguisher.** `recovered_baseline_version` is
/// `storage.latest_version()` captured **once**, at this apply task's own
/// start, before it has applied (or this process has minted) anything —
/// the engine-durable high-water mark a WAL replay can explain a no-op
/// against. `entry_version` (this specific merge's own version — the same
/// value [`assert_ts_monotonic`] already checked strictly increases in log
/// order) at or below that baseline is structurally indistinguishable from
/// an ordinary post-crash replay and is *not* surfaced loudly. Strictly
/// above it, a no-op is **provable**, not merely suspicious: this group's
/// own witnessing chain guarantees a fresh mint already exceeds
/// `storage.latest_version()` as observed at group start (`start_inner`'s
/// group-start witness, `drive`'s doc), so nothing this process could
/// legitimately mint after that point should ever collide with — let alone
/// lose to — a version already sitting in the engine, unless the value that
/// beat it got there via exactly Bug 3's mechanism (or a sibling of it this
/// seatbelt exists to also catch, present or future).
fn surface_suspicious_merge_noop(
    metrics: &MetricsHandle,
    log_budget: &mut u32,
    site: &'static str,
    key: &[u8],
    entry_version: Version,
    recovered_baseline_version: Version,
) {
    metrics.incr(Metric::CpMergeTookNoEffect);
    if entry_version <= recovered_baseline_version {
        return; // explainable by an ordinary WAL-replay re-application
    }
    metrics.incr(Metric::CpMergeTookNoEffectUnexplained);
    if *log_budget > 0 {
        *log_budget -= 1;
        tracing::warn!(
            site,
            key = ?key,
            entry_version,
            recovered_baseline_version,
            "raftkv: apply-time merge silently took no effect on a write this apply arm's own \
             control flow already treated as landed — not explainable by WAL replay (this \
             entry's own version exceeds the engine-durable watermark recovered at this apply \
             task's start), so this is a provable, live invariant violation — see ADR 0018 §2's \
             write-loss amendment"
        );
    }
    // FIXME(PR3 WIP): a `debug_assert!` here was found to fire on legitimate,
    // documented scenarios this design didn't account for (e.g. same-txn
    // re-staging / an application-level client retry landing an identical
    // entry a second time within the *same* process lifetime, well after
    // `recovered_baseline_version` — not a node restart at all) — see the
    // in-progress investigation notes. Metrics + a capped warn log only,
    // until the fresh-vs-replay distinguisher is redesigned to also
    // recognize an idempotent identical-value re-application, not just a
    // post-restart one.
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
    kind_outcomes: &Arc<Mutex<KindBatchOutcomes>>,
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
    // ADR 0050 rung 5: the split-cutover freeze latch — set by
    // `KvCommand::Freeze`'s apply arm, read by the propose-side refusals
    // (`RaftKvNode::is_frozen`); re-latched at group start from the
    // whole-range seal marker (the same rebuild `sealed` gets).
    frozen: &AtomicBool,
    txn_tracker: &Mutex<TxnTracker>,
    // ADR 0018 §2 write-loss amendment (Part B): the engine-durable version
    // watermark recovered once at this apply task's own start, and this
    // task's own remaining `tracing::warn!` budget for a suspicious no-op —
    // see `surface_suspicious_merge_noop`'s doc. Threaded like `sealed`/
    // `max_applied_ts`: this apply task's own sequential, single-writer
    // bookkeeping, never touched by any other task.
    recovered_baseline_version: Version,
    suspicious_noop_log_budget: &mut u32,
    // ADR 0058 Train 2 rung 4 layer 1: raised the instant `KvCommand::
    // SplitTablet` durably applies below — see `ForkSignal`'s doc.
    fork_signal: &ForkSignal,
    // Issue #279: the shared persist-round counter. This task is the WAL's
    // *second* drainer (its compaction rewrite), so it must number and complete
    // the rounds it consumes or the consensus loop's buffered acks strand.
    persist: &PersistProgress,
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
    for (index, term, command) in effects {
        match command {
            KvCommand::Put { key, value, ts } => {
                assert_ts_monotonic(max_applied_ts, ts);
                // A sealed-out key is a deterministic no-op (ADR 0018 §2
                // amendment, now `Freeze`'s apply-time backstop): the key fell
                // in a range this group already closed, so this entry —
                // necessarily proposed before the freeze applied — is
                // rejected. The seal check is against the *logical* key; only
                // the storage-bound `MergeOp` gets the physical address (see
                // `StorageScope`'s doc).
                if !is_sealed(sealed, &key) {
                    pending.push(MergeOp::put(
                        scope.physical(&key),
                        txn::encode_committed(&value),
                        hlc::pack(ts),
                    ));
                }
            }
            KvCommand::Batch { puts, ts } => {
                assert_ts_monotonic(max_applied_ts, ts);
                // The seal gates the *whole* batch, not per-key: a batch is
                // one atomic Raft entry (see `KvCommand::Batch`'s doc), so
                // partially applying it on a miss would silently break that
                // guarantee. Every key in the batch merges at this one entry's
                // shared `ts`. The keys are distinct, so per-key LWW is
                // well-defined; `engine_applied` advances once past the whole batch
                // at the end of the loop iteration (the batch is one entry). Composes
                // with a future coalesced-fsync merge_batch (perf/lsm) — this is the
                // normal per-key `merge` path that batching optimization refines.
                if puts.iter().all(|(key, _)| !is_sealed(sealed, key)) {
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
            KvCommand::SeedBatch { rows, ts } => {
                assert_ts_monotonic(max_applied_ts, ts);
                // ADR 0050 rung 4 (fork F3): history transfer into this child
                // group's own engine. Each row merges at its CARRIED version
                // (never this entry's own `ts`) with the parent's stored bytes
                // verbatim — envelope tag included, so a staged intent copies
                // as an intent (fork F7) — making re-proposal an idempotent
                // no-op and a mid-build parent update a per-key LWW winner on
                // the child too. Whole-batch seal discipline (`Batch`'s): a
                // frozen group (`Freeze`'s whole-range seal) refuses seeds —
                // a SeedBatch is never legitimately directed at a split
                // PARENT, only its children, so a refusal only fires on a
                // driver bug, as a loud whole-batch no-op, never a partial
                // install. The driver filters rows to each child's immutable
                // range by construction (no per-entry range check remains —
                // ranges are immutable and the route is child-group-direct).
                if rows
                    .iter()
                    .all(|(_, logical, _, _)| !is_sealed(sealed, logical))
                {
                    let mut max_seeded: u64 = 0;
                    for (kind, logical, value, version) in &rows {
                        let Some(kscope) = kind_scopes.get(*kind as usize) else {
                            // An unknown kind can only come from a newer
                            // build's driver (ALL_KINDS grew) — drop it, the
                            // `install_engine_image` discipline.
                            tracing::warn!(kind, "seed row of unknown kind dropped");
                            continue;
                        };
                        let physical = kscope.physical(logical);
                        match value {
                            Some(v) => pending.push(MergeOp::put(physical, v.clone(), *version)),
                            None => pending.push(MergeOp::tombstone(physical, *version)),
                        }
                        max_seeded = max_seeded.max(*version);
                    }
                    // Witnessing point (the snapshot-install discipline, this
                    // arm's own doc): copied rows carry versions this group
                    // never minted — fold the batch's high-water mark in so a
                    // child leader's own future mints strictly exceed every
                    // seeded row.
                    if max_seeded > 0 {
                        hlc.witness(hlc::unpack(max_seeded), env.now());
                    }
                } else {
                    tracing::warn!("seed batch outside the child's declared range dropped whole");
                }
            }
            KvCommand::KindBatch {
                writes,
                change_log,
                conditions,
                ts,
            } => {
                assert_ts_monotonic(max_applied_ts, ts);
                // ADR 0046 "evaluate at leader" seatbelt: this entry's own-key
                // `conditions` (see `KvCommand::KindBatch`'s doc) are checked
                // against the KIND_BASE scope — the only scope a production
                // caller ever conditions on — BEFORE the fence/seal gate
                // below. This deliberately differs from `TxnStage`'s own
                // `condition_failure`, which only evaluates once its entry is
                // otherwise known to be in-fence (so `StageOutcome` can report
                // the fence/seal reason ahead of a condition one): a
                // `KindBatch` condition failure has no outcome-introspection
                // channel at all — it no-ops silently, indistinguishable from
                // a fence/seal miss either way — so there is no
                // reporting-priority reason to gate the read behind the fence
                // check here. Drain the pending run first (mirrors `Cas`'s and
                // `TxnStage`'s own read-after-flush-pending discipline) so a
                // condition observes every earlier committed write in this
                // same apply pass.
                // Which condition failed, for the recorded outcome.
                let mut failed_condition: Option<Vec<u8>> = None;
                let conditions_ok = if conditions.is_empty() {
                    true
                } else {
                    flush_pending(storage, &mut pending, metrics, halted).await;
                    let mut ok = true;
                    for (key, expected) in &conditions {
                        let raw = storage
                            .get(&scope.physical(key))
                            .await
                            .expect("raftkv kind batch condition read");
                        let matches = match raw.map(|vv| txn::decode_envelope(&vv.value)) {
                            None => expected.is_none(),
                            Some(txn::Envelope::Committed(v)) => Some(v) == *expected,
                            // An unresolved intent makes "the current
                            // committed value" ambiguous — never guess at a
                            // match, mirroring `Cas`/`TxnStage`'s identical
                            // discipline.
                            Some(txn::Envelope::Intent { .. }) => false,
                        };
                        if !matches {
                            ok = false;
                            failed_condition = Some(key.clone());
                            break;
                        }
                    }
                    ok
                };
                // Gated as one unit, exactly like `Batch` — an index write that
                // half-applied would leave an LSI row describing a base row
                // that never landed, which is the one thing colocating them was
                // supposed to make impossible. Every kind shares this tablet's
                // single range, so one fence covers them all.
                //
                // ADR 0050 rung 5: the seal/freeze gate applies only to a
                // batch carrying USER data (a base or LSI write). A pure
                // consumer-bookkeeping batch — cursor rows, footprint rows,
                // change-log-only entries (the backfill seeder's synthetic
                // records) — passes a frozen group's whole-range seal, so
                // the GSI drain and the backfill seeder can finish draining
                // a frozen split parent's (static) base state; without this
                // the rung-5 cutover vetoes deadlock against the very
                // freeze that made the state drainable. The base state
                // itself stays fixed at the freeze position (the contract);
                // bookkeeping kinds are either never copied to the children
                // (CHANGE/CURSOR) or self-healing on them (FOOTPRINT — the
                // drain always reconciles toward the current base row).
                let carries_user_data = writes
                    .iter()
                    .any(|(kind, _, _)| *kind == KIND_BASE || *kind == KIND_LSI);
                let sealed_key = if carries_user_data {
                    writes
                        .iter()
                        .find(|(_, key, _)| is_sealed(sealed, key))
                        .map(|(_, key, _)| key.clone())
                } else {
                    None
                };
                // Record what this entry actually did, keyed by its Raft log
                // index (and own term — see `KindBatchOutcomes`' doc), so a
                // proposer can tell "no-op'd" from "applied and then
                // overwritten" instead of comparing the value back — the
                // introspection channel `TxnStage` and `Cas` already have.
                let outcome = match (&failed_condition, &sealed_key) {
                    (Some(key), _) => KindBatchOutcome::ConditionFailed { key: key.clone() },
                    (None, Some(key)) => KindBatchOutcome::Sealed { key: key.clone() },
                    (None, None) => KindBatchOutcome::Applied,
                };
                kind_outcomes
                    .lock()
                    .expect("kind batch outcomes poisoned")
                    .record(index, term, outcome);
                if conditions_ok && sealed_key.is_none() {
                    // ADR 0046 binding decision: the ONE shared
                    // materialization helper, also used by `TxnResolve`'s
                    // commit branch below — never a second copy of this
                    // loop.
                    materialize_derived(kind_scopes, &writes, &change_log, ts, &mut pending);
                }
            }
            KvCommand::Delete { key, ts } => {
                assert_ts_monotonic(max_applied_ts, ts);
                if !is_sealed(sealed, &key) {
                    pending.push(MergeOp::tombstone(scope.physical(&key), hlc::pack(ts)));
                }
            }
            KvCommand::Cas {
                key,
                expected,
                value,
                ts,
            } => {
                assert_ts_monotonic(max_applied_ts, ts);
                // Drain the pending run so the CAS read observes every earlier
                // committed write in this apply pass.
                flush_pending(storage, &mut pending, metrics, halted).await;
                // A sealed-out CAS never reads/writes storage — it is
                // recorded as `false` ("did not swap"), the same outcome shape a
                // proposer already handles for an ordinary `expected` mismatch, so
                // a confirm-poll on this index never hangs waiting for an outcome
                // that will never come.
                let swapped = if !is_sealed(sealed, &key) {
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
                        let cas_version = hlc::pack(ts);
                        let took_effect = storage
                            .merge(&physical_key, &txn::encode_committed(&value), cas_version)
                            .await
                            .expect("raftkv apply cas");
                        // ADR 0018 §2 write-loss amendment (Part B): this arm
                        // already decided "swapped" from the *read* above,
                        // independently of whether the merge itself lands —
                        // see `surface_suspicious_merge_noop`'s doc.
                        if !took_effect {
                            surface_suspicious_merge_noop(
                                metrics,
                                suspicious_noop_log_budget,
                                "Cas",
                                &physical_key,
                                cas_version,
                                recovered_baseline_version,
                            );
                        }
                    }
                    swapped
                } else {
                    false
                };
                cas.lock()
                    .expect("cas results poisoned")
                    .outcomes
                    .insert(index, (term, swapped));
            }
            KvCommand::Freeze { ts } => {
                assert_ts_monotonic(max_applied_ts, ts);
                // Idempotent: an already-frozen group (a duplicate propose, a
                // WAL-replay re-application over a marker-rebuilt sealed set)
                // applies nothing a second time.
                if !frozen.load(Ordering::SeqCst) {
                    // The whole-range seal marker — `seal.rs`'s own durability
                    // discipline (the marker survives compaction; the sealed
                    // set is rebuilt from it at group start, which is also
                    // what re-latches `frozen` across a restart). Flush the
                    // pending run first, `Seal`'s ordering hygiene.
                    flush_pending(storage, &mut pending, metrics, halted).await;
                    let whole = KeyRange::whole();
                    let marker_key = seal::seal_marker_key(tablet, &whole);
                    storage
                        .merge(
                            &marker_key,
                            &seal::encode_seal_value(&whole, ts),
                            hlc::pack(ts),
                        )
                        .await
                        .expect("raftkv apply freeze marker");
                    sealed.push((whole, ts));
                    frozen.store(true, Ordering::SeqCst);
                }
            }
            KvCommand::SplitTablet {
                split_key,
                children,
                ts,
            } => {
                assert_ts_monotonic(max_applied_ts, ts);
                // Idempotent, mirroring `Freeze`'s own "already sealed"
                // check: a duplicate propose or a WAL-replay
                // re-application over a marker-rebuilt sealed set applies
                // nothing a second time. `frozen` is shared with `Freeze`
                // deliberately — the two workflows are mutually exclusive
                // per tablet (selected by the operator's split-mode flag),
                // and every existing "refuse if frozen" gate must cover
                // this fork too with no separate allowlist entry.
                if !frozen.load(Ordering::SeqCst) {
                    flush_pending(storage, &mut pending, metrics, halted).await;
                    let whole = KeyRange::whole();
                    let seal_marker_key = seal::seal_marker_key(tablet, &whole);
                    storage
                        .merge(
                            &seal_marker_key,
                            &seal::encode_seal_value(&whole, ts),
                            hlc::pack(ts),
                        )
                        .await
                        .expect("raftkv apply split-fork seal marker");
                    sealed.push((whole, ts));
                    frozen.store(true, Ordering::SeqCst);
                    // The fork-specific durable payload (split.rs): what the
                    // host reconciler needs to actually materialize the two
                    // children — see `KvCommand::SplitTablet`'s own doc for
                    // why that work happens outside this apply path.
                    //
                    // `bootstrap_voters` is captured HERE, once, from the
                    // core's own config+learners at this exact apply — see
                    // `split.rs`'s module doc for why this read is
                    // guaranteed identical on every replica (Raft log order:
                    // every prior config-change entry has already applied by
                    // the time THIS entry does).
                    let bootstrap_voters = {
                        let core_guard = core.lock().expect("raftkv core poisoned");
                        let mut voters = core_guard.config();
                        voters.extend(core_guard.learners());
                        voters
                    };
                    let split_marker_key = split::split_marker_key(tablet);
                    storage
                        .merge(
                            &split_marker_key,
                            &split::encode_split_value(
                                &split_key,
                                &children,
                                &bootstrap_voters,
                                ts,
                            ),
                            hlc::pack(ts),
                        )
                        .await
                        .expect("raftkv apply split-fork marker");
                    // ADR 0058 Train 2 rung 4 layer 1: the fork is now
                    // durable on THIS replica (`pending_split()` will answer
                    // `Some` from this point on, including across a crash —
                    // it re-derives from this same marker at group start).
                    // Wake any external caller waiting to react to a local
                    // fork immediately, instead of leaving discovery to that
                    // caller's own next scheduled poll — see `ForkSignal`'s
                    // doc. Raised unconditionally on every path that reaches
                    // here (this whole arm already only runs once, gated by
                    // `!frozen` above), so a WAL-replay re-application after
                    // a restart never re-notifies (the `if !frozen` guard
                    // above skips the whole block on replay, exactly like
                    // `Freeze`'s own idempotency).
                    fork_signal.notify();
                }
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
                flush_pending(storage, &mut pending, metrics, halted).await;
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
                    for w in &writes {
                        let key = &w.key;
                        let Some(vv) = storage
                            .get(&scope.physical(key))
                            .await
                            .expect("raftkv txn stage conflict read")
                        else {
                            continue;
                        };
                        if let txn::Envelope::Intent {
                            txn_id: blocker,
                            record_key: blocker_record_key,
                            record_table: blocker_record_table,
                            ..
                        } = txn::decode_envelope(&vv.value)
                            && blocker != txn_id
                        {
                            break 'blocked Some((
                                key.clone(),
                                blocker,
                                blocker_record_key,
                                blocker_record_table,
                            ));
                        }
                    }
                    None
                };
                // ADR 0018 §2/PR6 seatbelt (issue #298 shape A): reject a
                // stage that targets a key THIS EXACT transaction already
                // resolved on this group — `blocked_by` above only ever
                // catches a *different* transaction's still-live `Intent`,
                // never a same-txn resurrection of an already-`Committed`/
                // restored value (see `TxnTracker::recently_resolved`'s
                // doc for the full argument and why checking (key, txn_id)
                // IDENTITY, not mere presence, is load-bearing here — a
                // *different*, genuinely later transaction reusing the same
                // physical key is the ordinary, unrelated write path and
                // must stage normally).
                let resurrection_attempt = {
                    let t = txn_tracker.lock().expect("txn tracker poisoned");
                    writes
                        .iter()
                        .any(|w| t.recently_resolved.get(&scope.physical(&w.key)) == Some(&txn_id))
                };
                if resurrection_attempt {
                    tracing::warn!(
                        ?txn_id,
                        ?record_key,
                        "raftkv: TxnStage rejected — at least one target key was already \
                         resolved by this exact transaction on this group (a stale/duplicate \
                         stage arriving after its own resolve; issue #298 shape A seatbelt) — \
                         no-op"
                    );
                }
                if let Some((blocked_key, blocker, ..)) = &blocked_by {
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
                // seal set/engine (ADR 0018 §2/PR4).
                let record_in_fence = !is_anchor || !is_sealed(sealed, &record_key);
                // ADR 0046 A1: every kind-write key a write stages must lead
                // with that write's own base key's partition token — checked
                // here (a validated rejection, folded into the same
                // structural `Fenced` outcome bucket as a fence/seal miss,
                // never an `assert!`, since this payload is wire-reachable
                // via `ClientRequest::TxnPrepare`) rather than assumed. This
                // is also what makes `TxnResolve`'s own fence check over
                // these same kind keys meaningful: a kind key sharing its
                // base key's token sits at the same tablet-range position a
                // plain `KindBatch` write's own key would.
                let kind_tokens_ok = writes.iter().all(|w| {
                    kind_writes_token_valid(&w.key, &w.kind_writes)
                        && stage_marker_token_valid(&w.key, w.stage_marker.as_ref())
                        && change_log_token_valid(&w.key, w.change_log.as_ref())
                });
                let all_in_fence = !already_decided
                    && !resurrection_attempt
                    && blocked_by.is_none()
                    && kind_tokens_ok
                    && writes.iter().all(|w| !is_sealed(sealed, &w.key))
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
                    flush_pending(storage, &mut pending, metrics, halted).await;
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
                    flush_pending(storage, &mut pending, metrics, halted).await;
                    let version = hlc::pack(ts);
                    for w in &writes {
                        // ADR 0046 A1: the derived kind-writes/change-log
                        // payload rides inside this intent, opaque until
                        // `TxnResolve`'s commit branch materializes it —
                        // never written into a kind scope here.
                        let intent_env = txn::encode_intent(
                            &txn_id,
                            &record_key,
                            &record_table,
                            w.value.as_deref(),
                            &w.kind_writes,
                            w.change_log.as_ref(),
                        );
                        let physical_key = scope.physical(&w.key);
                        let took_effect = storage
                            .merge(&physical_key, &intent_env, version)
                            .await
                            .expect("raftkv apply txn stage intent");
                        // ADR 0018 §2 write-loss amendment (Part B): `outcome`
                        // below is computed from the fence/seal/foreign-intent/
                        // condition gates above, independently of whether this
                        // specific key's intent merge actually lands — see
                        // `surface_suspicious_merge_noop`'s doc.
                        if !took_effect {
                            surface_suspicious_merge_noop(
                                metrics,
                                suspicious_noop_log_budget,
                                "TxnStage intent",
                                &physical_key,
                                version,
                                recovered_baseline_version,
                            );
                        }
                    }
                    // ADR 0049 §3: the stage marker — an image-less,
                    // consumer-hidden change-log record per staged write,
                    // materialized at THIS stage entry's own `ts` (its key
                    // HLC therefore strictly precedes the resolve entry's
                    // materialized record, by log order — the per-item
                    // ordering the split-build tail relies on). Goes through
                    // the ONE shared materialization helper (ADR 0046's
                    // binding shared-helper rule) with an empty kind-writes
                    // list; record bytes stay opaque to this crate (ADR 0043
                    // layering — the edge built them). Only written when the
                    // stage itself lands (`stage_ok`): a fenced/blocked/
                    // condition-failed stage leaves no intent, so there is
                    // no dirty key to signal. WAL-replay re-application
                    // re-merges the identical row at the identical version —
                    // a no-op, like the intent merges above. An aborted
                    // transaction's marker simply remains as a dirty-key
                    // hint pointing at a row whose envelope reverted —
                    // harmless by design: consumers re-read current state.
                    let stage_markers: Vec<(Vec<u8>, Vec<u8>)> = writes
                        .iter()
                        .filter_map(|w| w.stage_marker.clone())
                        .collect();
                    if !stage_markers.is_empty() {
                        materialize_derived(kind_scopes, &[], &stage_markers, ts, &mut pending);
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
                // structural fence/seal-miss, already-decided race, or
                // resurrection attempt (all three fold into `Fenced` — the
                // `resurrection_attempt` case flows through the `!all_in_
                // fence` branch below) and a foreign-intent block
                // (`IntentBlocked`) both pre-empt ever evaluating this
                // stage's own conditions, so they take priority over
                // `ConditionFailed` here too.
                let outcome = if already_decided {
                    txn::StageOutcome::Fenced
                } else if let Some((
                    blocked_key,
                    blocker,
                    blocker_record_key,
                    blocker_record_table,
                )) = blocked_by
                {
                    txn::StageOutcome::IntentBlocked {
                        key: blocked_key,
                        txn_id: blocker,
                        record_key: blocker_record_key,
                        record_table: blocker_record_table,
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
                    .insert(index, (term, outcome));
            }
            KvCommand::TxnCommit {
                txn_id,
                record_key,
                ts,
            } => {
                assert_ts_monotonic(max_applied_ts, ts);
                flush_pending(storage, &mut pending, metrics, halted).await;
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
                flush_pending(storage, &mut pending, metrics, halted).await;
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
                flush_pending(storage, &mut pending, metrics, halted).await;
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
                    // `Some(record) if record.txn_id == txn_id` means *this*
                    // group holds `txn_id`'s own record, i.e. this group
                    // **is** the anchor for this transaction (only an
                    // anchor's own `TxnStage` ever writes it — see
                    // `KvCommand::TxnStage`'s `is_anchor` doc) — never a
                    // non-anchor participant, and never a different
                    // transaction's own coincidentally colliding key
                    // (`record_key` is derived from `txn_id` itself).
                    //
                    // (An earlier PR3 draft also witnessed `outcome`'s
                    // `commit_ts` into a non-anchor participant's clock here,
                    // to let a lagging participant catch up to the anchor's
                    // pace. Abandoned: even gated to a genuine non-anchor
                    // participant, that witness reignited a clock-witnessing
                    // runaway under sustained cross-group transaction + read
                    // load — confirmed super-linear in round count by
                    // `tests/ts_cache.rs`'s `cross_group_txn_traffic_never_
                    // lets_either_groups_clock_run_away`, which stays as a
                    // permanent regression against re-introducing it. See
                    // ADR 0018 §2's write-loss amendment for the full story
                    // and why the real fix is the fence below instead.)
                    let local_record = storage
                        .get(&scope.physical(&record_key))
                        .await
                        .expect("raftkv txn resolve record read")
                        .and_then(|vv| txn::decode_record(&vv.value));
                    // ADR 0046 A1: read every key's own current value up
                    // front — before deciding the whole-or-nothing fence
                    // gate below — so that gate can also cover the derived
                    // kind keys a commit is about to materialize (the #213
                    // lesson: every key-writing command must carry AND
                    // enforce the apply-time fence over every key it
                    // writes, not just a subset). One read per key, reused
                    // by the resolve loop further down — never a second
                    // read pass.
                    struct ResolvedIntent {
                        staged_value: Option<Vec<u8>>,
                        intent_version: Version,
                        kind_writes: Vec<KindWrite>,
                        change_log: Option<(Vec<u8>, Vec<u8>)>,
                    }
                    let mut resolved: Vec<Option<ResolvedIntent>> = Vec::with_capacity(keys.len());
                    for key in &keys {
                        let found = storage
                            .get(&scope.physical(key))
                            .await
                            .expect("raftkv txn resolve key read")
                            .and_then(|vv| match txn::decode_envelope(&vv.value) {
                                txn::Envelope::Intent {
                                    txn_id: found,
                                    staged_value,
                                    kind_writes,
                                    change_log,
                                    ..
                                } if found == txn_id => Some(ResolvedIntent {
                                    staged_value,
                                    intent_version: vv.version,
                                    kind_writes,
                                    change_log,
                                }),
                                // Already resolved, or a different/newer
                                // txn's intent has since overwritten this
                                // key — nothing of ours left here.
                                // Idempotent no-op.
                                _ => None,
                            });
                        resolved.push(found);
                    }
                    // ADR 0018 §2 write-loss amendment (Bug 3): every key in
                    // `keys` must fall inside this entry's own `fence` (and
                    // not a range this group has since sealed off) — the
                    // structural seatbelt against a caller (present or
                    // future) that misroutes a resolve to the wrong tablet,
                    // exactly like `TxnStage`'s own fence check above and
                    // `KvCommand::TxnResolve`'s doc. Whole-or-nothing, not
                    // per-key: a partial reject would leave some of this
                    // transaction's keys resolved and others not, the same
                    // torn state a fence exists to prevent elsewhere.
                    //
                    // ADR 0046 A1 extends this same gate to the derived
                    // kind keys a commit is about to materialize (LSI rows,
                    // the change-log record) — new key-writing surface this
                    // entry didn't have before, and just as capable of
                    // landing on the wrong tablet if misrouted. The
                    // change-log record's own key isn't checked here (its
                    // HLC suffix isn't even minted yet at this point,
                    // exactly like `KindBatch`'s own arm never fence-checks
                    // its `change_log` prefix directly — it rides under the
                    // same entry-wide gate as `kind_writes`' keys instead).
                    let all_in_fence = keys.iter().all(|k| !is_sealed(sealed, k))
                        && resolved.iter().flatten().all(|ri| {
                            ri.kind_writes
                                .iter()
                                .all(|(_, kk, _)| !is_sealed(sealed, kk))
                        });
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
                    // not a reproduced bug** (see the PR6 audit recorded in
                    // `docs/engineering-lessons.md` and ADR 0018's PR5
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
                    let outcome_mismatch = match &local_record {
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
                    for (key, resolved_intent) in keys.iter().zip(resolved) {
                        if outcome_mismatch || !all_in_fence {
                            continue; // whole-or-nothing: skip every key, not just this one
                        }
                        let physical_key = scope.physical(key);
                        let Some(ResolvedIntent {
                            staged_value,
                            intent_version,
                            kind_writes,
                            change_log,
                        }) = resolved_intent
                        else {
                            continue; // nothing left here to resolve (idempotent no-op)
                        };
                        // ADR 0018 §2 write-loss amendment (Part B): every
                        // branch below already treated this key as resolved
                        // (it is removed from `TxnTracker`, and the
                        // coordinator's own client-facing ack was computed
                        // independently) before this fix started checking
                        // whether the merge that's supposed to *make* it so
                        // actually landed — see
                        // `surface_suspicious_merge_noop`'s doc, and this
                        // variant's own `fence` (above, in `KvCommand::
                        // TxnResolve`'s doc) for why a misrouted resolve
                        // used to leave a foreign tablet's key permanently
                        // unable to land correctly.
                        match outcome_commit_ts {
                            Some(_commit_ts) => {
                                match staged_value {
                                    // Committed: the staged value becomes the
                                    // committed value.
                                    Some(v) => {
                                        let took_effect = storage
                                            .merge(
                                                &physical_key,
                                                &txn::encode_committed(&v),
                                                version,
                                            )
                                            .await
                                            .expect("raftkv apply txn resolve commit");
                                        if !took_effect {
                                            surface_suspicious_merge_noop(
                                                metrics,
                                                suspicious_noop_log_budget,
                                                "TxnResolve commit",
                                                &physical_key,
                                                version,
                                                recovered_baseline_version,
                                            );
                                        }
                                    }
                                    // A staged delete resolves to an actual
                                    // tombstone — the only place `TxnResolve`
                                    // writes one, since it's finalizing an
                                    // already-decided delete, not guessing.
                                    None => {
                                        let took_effect = storage
                                            .merge_tombstone(&physical_key, version)
                                            .await
                                            .expect("raftkv apply txn resolve commit delete");
                                        if !took_effect {
                                            surface_suspicious_merge_noop(
                                                metrics,
                                                suspicious_noop_log_budget,
                                                "TxnResolve commit delete",
                                                &physical_key,
                                                version,
                                                recovered_baseline_version,
                                            );
                                        }
                                    }
                                }
                                // ADR 0046 A1 ("materialize-at-resolve"): on
                                // commit only, materialize this write's
                                // derived kind-scope rows + change-log
                                // record — via the SAME shared helper
                                // `KindBatch`'s own apply arm uses — at THIS
                                // resolve entry's own `ts`, never the
                                // transaction's `commit_ts` and never the
                                // stage's own `ts` (ADR 0018 §2 B1: the key
                                // position must be monotone in this
                                // tablet's own log, which only the entry
                                // that actually fixes commit order can
                                // provide). Discarded entirely on abort —
                                // see the `None` arm below.
                                materialize_derived(
                                    kind_scopes,
                                    &kind_writes,
                                    // A `TxnWrite` carries at most one record
                                    // (its own); the helper's slice shape
                                    // exists for the marker-batch case.
                                    change_log.as_slice(),
                                    ts,
                                    &mut pending,
                                );
                            }
                            None => {
                                // Aborted: restore whatever this key held
                                // immediately before the intent — never a
                                // tombstone, which would incorrectly shadow
                                // that older, still-live committed value
                                // (see `txn.rs`'s module doc). Every
                                // version this group has ever applied is
                                // strictly increasing
                                // (`assert_ts_monotonic`), so
                                // `intent_version - 1` is guaranteed to sit
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
                                    .get_at(&physical_key, intent_version.saturating_sub(1))
                                    .await
                                    .expect("raftkv txn resolve prior read");
                                let (took_effect, site) = match prior {
                                    Some(pvv) => (
                                        storage
                                            .merge(&physical_key, &pvv.value, version)
                                            .await
                                            .expect("raftkv apply txn resolve abort restore"),
                                        "TxnResolve abort restore",
                                    ),
                                    None => (
                                        storage
                                            .merge_tombstone(&physical_key, version)
                                            .await
                                            .expect(
                                                "raftkv apply txn resolve abort restore tombstone",
                                            ),
                                        "TxnResolve abort restore tombstone",
                                    ),
                                };
                                if !took_effect {
                                    surface_suspicious_merge_noop(
                                        metrics,
                                        suspicious_noop_log_budget,
                                        site,
                                        &physical_key,
                                        version,
                                        recovered_baseline_version,
                                    );
                                }
                            }
                        }
                        // ADR 0018 §2/PR6 seatbelt (issue #298 shape A):
                        // record that THIS exact transaction's intent at
                        // this key has now been resolved here (commit or
                        // abort — both leave nothing of `txn_id`'s own at
                        // this key), so a stale/duplicate `TxnStage`
                        // re-arriving for the identical `(key, txn_id)`
                        // afterward is rejected instead of silently
                        // resurrecting it — see `TxnTracker::recently_
                        // resolved`'s doc and `KvCommand::TxnStage`'s own
                        // resurrection-guard read of this map.
                        txn_tracker
                            .lock()
                            .expect("txn tracker poisoned")
                            .record_resolution(physical_key, txn_id.clone());
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
    flush_pending(storage, &mut pending, metrics, halted).await;
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
                // net durable state (snapshot + hard + log tail), the current
                // term/vote included. Under this one lock hold, so no
                // propose/append interleaves.
                //
                // Issue #279: this drain is a **persist round like any other**,
                // and claiming its number here — in the same lock hold as the
                // drain itself — is what stops it stranding the consensus loop.
                // The loop buffers a vote grant / append accept against "the next
                // round to drain"; before this, compaction could silently be that
                // drainer, the loop's own peek then saw nothing left to persist,
                // no round was ever started, and the ack sat undelivered for
                // seconds. Now compaction completes the round it consumed (after
                // its `replace` lands, below), and the buffered acks go out.
                let (_superseded, round) = persist_round::drain_for_round(&mut c, persist);
                (Some((buf, round)), lli)
            }
        };
        if let Some((bytes, round)) = bytes {
            match env.replace(wal, &bytes).await {
                Ok(()) => {
                    // Physically durable now — advance both watermarks (the log
                    // index, and the persist round this rewrite consumed) under
                    // one acquisition, then let the consensus loop ship whatever
                    // it buffered against that round.
                    let mut c = core.lock().expect("raftkv core poisoned");
                    c.mark_durable_through(lli);
                    if let Some(round) = round {
                        persist.complete_drain(round);
                    }
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
///
/// **Halted-gated error tolerance** (issue #278 item 1 follow-up, the identical
/// idiom `persist_wal`/the compaction path above use): `apply_and_compact`'s
/// top-of-`apply_loop` `halted` check only gates *starting* a pass — the effects
/// loop that calls this function up to ten times per pass (once per `Cas`/
/// `Freeze`/`ReadCeiling`/conditioned-`KindBatch` ordering-hygiene drain, plus
/// the trailing flush) does not re-check `halted` between merges, so a
/// `shutdown()` racing a still-in-flight `merge_batch` mid-pass can surface the
/// identical class of teardown-artifact I/O error `persist_wal` tolerates. On
/// error: tolerated (no `pending` restore — the caller is discarding this pass
/// on the same halt anyway) iff `halted` is already set; a live group's
/// identical failure stays a hard panic (durable-before-visible: an apply
/// failure while running means the engine may now be silently missing a
/// committed write, so this must never be softened into a swallowed error).
async fn flush_pending<S: StorageEngine>(
    storage: &S,
    pending: &mut Vec<MergeOp>,
    metrics: &MetricsHandle,
    halted: &AtomicBool,
) {
    if pending.is_empty() {
        return;
    }
    metrics.incr(Metric::CpApplyBatchRuns);
    metrics.incr_by(Metric::CpApplyBatchSizeSum, pending.len() as u64);
    if let Err(e) = storage.merge_batch(std::mem::take(pending)).await {
        assert!(
            halted.load(Ordering::SeqCst),
            "raftkv apply merge batch failed while running: {e}"
        );
    }
}

/// One key's snapshot entry: `(row kind, key, value-or-tombstone, version)`.
///
/// The kind (ADR 0041 §3) is what lets one image carry a whole tablet — every
/// row kind's scope — while the key stays the *logical* key within its own
/// scope, so the receiver can re-prefix it under its own scope set.
pub(crate) type ImageEntry = (u8, Vec<u8>, Option<Vec<u8>>, u64);

/// Serialize this tablet's contents (including tombstones) as the snapshot
/// image shipped to a lagging follower. Since ADR 0050 rung 1 the engine is
/// the tablet's own private one, so the whole-engine walk IS the tablet's
/// own data; the kind scopes classify each row by kind byte and exclude the
/// engine-global reserved-namespace markers (which lead `0x5F`, matching no
/// kind — each replica re-derives its own markers at apply, exactly as
/// before).
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
    kind_outcomes: Arc<Mutex<KindBatchOutcomes>>,
    engine_applied: Arc<AtomicU64>,
    wal_lock: Arc<AsyncMutex<()>>,
    halted: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    apply_stopped: Arc<AtomicBool>,
    propose_signal: Arc<ProposeSignal>,
    apply_signal: Arc<ApplySignal>,
    wake_signal: Arc<WakeSignal>,
    /// See [`RaftKvNode::fork_signal`]'s doc — threaded through to the apply
    /// task, the only raiser.
    fork_signal: Arc<ForkSignal>,
    /// Issue #279: persist-round accounting shared by this group's two WAL
    /// drainers (the consensus loop and the apply task's compaction rewrite).
    persist: Arc<PersistProgress>,
    metrics: MetricsHandle,
    /// The **base**-kind scope (ADR 0041 §3) — see [`RaftKvNode::scope`].
    scope: StorageScope,
    /// Every row kind's scope, for the whole-tablet snapshot image.
    kind_scopes: [StorageScope; ALL_KINDS.len()],
    stream: u64,
    hlc: Arc<Hlc>,
    committed_ceiling: Arc<AtomicU64>,
    frozen: Arc<AtomicBool>,
    txn_tracker: Arc<Mutex<TxnTracker>>,
    external_quiesce_veto: Arc<AtomicBool>,
    external_quiesce_veto_fresh_through: Arc<AtomicU64>,
    /// ADR 0058 Train 2 rung 4: campaign for leadership immediately on a
    /// genuine first formation instead of waiting out the randomized
    /// election timeout — see [`RaftKvNode::start_hosted_campaigning`]'s doc.
    campaign_immediately: bool,
}

/// One split-build seed row (ADR 0050 Train B rung 4): `(kind index into
/// [`ALL_KINDS`], logical key, stored-bytes-or-tombstone, MVCC version)` —
/// a parent engine row verbatim, shipped by the split-build driver and
/// merge-applied at the carried version by [`KvCommand::SeedBatch`].
pub type SeedRow = (u8, Vec<u8>, Option<Vec<u8>>, u64);

/// A group's own **pending (or already-applied) in-place split fork** (ADR
/// 0058 Train 2 rung 3), as read back by
/// [`RaftKvNode::pending_split`](RaftKvNode::pending_split) — the durable
/// payload `KvCommand::SplitTablet`'s apply arm wrote, decoded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingSplit {
    /// The key the parent's range splits at.
    pub split_key: Vec<u8>,
    /// Exactly two children, left half first — each `replicas` is that
    /// child's placement-chosen FINAL replica set (what `CutoverSplit`
    /// records into `Metadata`, driving the post-cutover trim).
    pub children: [SplitChild; 2],
    /// The parent's own full voter-plus-learner config at the moment this
    /// entry applied — the BOOTSTRAP voter set both children's local
    /// `RaftKvNode` start with (a superset of either child's own final
    /// `replicas`, deliberately: see `split.rs`'s module doc for why every
    /// fork participant safely votes for both new groups initially, trimmed
    /// down post-cutover by the ordinary reconciler).
    pub bootstrap_voters: BTreeSet<NodeId>,
    /// The HLC timestamp the fork applied at (diagnostic only).
    pub ts: HlcTimestamp,
}

/// The `ts` a mutating [`KvCommand`] variant carries, or `None` for `NoOp`
/// (which carries none). The one place that knows every variant's `ts`
/// field, shared by the WAL-recovery and entry-receipt witnessing sites.
fn command_ts(command: &KvCommand) -> Option<HlcTimestamp> {
    match command {
        KvCommand::Put { ts, .. }
        | KvCommand::Batch { ts, .. }
        | KvCommand::KindBatch { ts, .. }
        | KvCommand::SeedBatch { ts, .. }
        | KvCommand::Delete { ts, .. }
        | KvCommand::Cas { ts, .. }
        | KvCommand::Freeze { ts }
        | KvCommand::SplitTablet { ts, .. }
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

/// Safety-net back-off for the apply task (ADR 0044 phase-1 PR1 — replaces the old
/// unconditional `APPLY_IDLE_POLL` 5ms poll): when there is nothing committed-and-
/// durable to merge, `apply_loop` races [`ApplyPending`] against a sleep of this
/// length rather than looping every few milliseconds regardless of whether
/// anything changed. [`ApplySignal`] normally resolves this well before the
/// deadline; this bound only matters for a **signal-less** transition (the
/// on-demand snapshot-image build `RaftCore::take_snapshot_needed` triggers, which
/// is set purely off the leader's own heartbeat/replicate cycle with no commit
/// advance — see `ApplySignal`'s doc) or a missed/lost wakeup, so a missed signal
/// degrades to today's (pre-fix) latency at worst, never a stall. Under load
/// `apply_and_compact` keeps returning `true`, so the task never sleeps and apply
/// stays close behind commit — this only bounds latency (and CPU) while idle.
const APPLY_SAFETY_POLL: Duration = Duration::from_millis(250);

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
        kind_outcomes,
        engine_applied,
        wal_lock,
        halted,
        stopped,
        apply_stopped,
        propose_signal,
        apply_signal,
        wake_signal,
        fork_signal,
        persist,
        metrics,
        scope,
        kind_scopes,
        stream,
        hlc,
        committed_ceiling,
        frozen,
        txn_tracker,
        external_quiesce_veto,
        external_quiesce_veto_fresh_through,
        campaign_immediately,
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
    // ADR 0058 Train 2 rung 4: captured before `state` is (conditionally)
    // moved into `RaftCore::recovered` below — an immediate campaign is only
    // ever safe for a genuine first formation, never a restart mid-catch-up
    // or a replica rejoining an already-populated log.
    let fresh_group = state.is_empty();
    if !fresh_group {
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
    // Re-latch the split-cutover freeze (ADR 0050 rung 5) from the same
    // rebuilt marker set: a whole-range seal IS the freeze (its apply wrote
    // exactly that marker), so a restarted — even fully log-compacted —
    // frozen parent refuses proposes from its very first post-recovery one.
    if sealed
        .iter()
        .any(|(range, _)| range.start.is_empty() && range.end.is_none())
    {
        frozen.store(true, Ordering::SeqCst);
    }
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
        kind_outcomes,
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
        frozen,
        Arc::clone(&txn_tracker),
        Arc::clone(&apply_signal),
        Arc::clone(&fork_signal),
        Arc::clone(&persist),
    ));

    // Issue #279: the loop's own in-flight persist round, and the outbound
    // messages held back until a round lands. Both are owned exclusively by this
    // task — no other task can observe or mutate them — which is what makes the
    // round bookkeeping correct by construction; see `persist_round`'s module
    // doc for the two reverted attempts whose watermarks were sampled outside
    // the lock hold that produced the mutation.
    let mut persist_fut: Option<PersistFut<'_>> = None;
    let mut gated = GatedOuts::default();

    // ADR 0058 Train 2 rung 4: the deterministic first-leader mechanism.
    // Only for a genuine first formation (`fresh_group`) of a replica the
    // caller has identified as the parent's own leader at the fork
    // (`campaign_immediately` — see `start_hosted_campaigning`'s doc). Runs
    // exactly once, before this loop ever selects on the timer, so it wins
    // the race against the group's own cold randomized election timeout
    // rather than merely shortening it.
    if campaign_immediately && fresh_group {
        let entropy = env.next_u64();
        let (campaign_outs, campaign_gate) = {
            let mut c = core.lock().expect("raftkv core poisoned");
            // Structural belt on top of `RaftCore::campaign_now`'s own
            // `is_voter()` gate (which alone would make calling this on a
            // learner or non-member a harmless no-op): the caller's own
            // contract says this flag is only ever set for a replica that
            // was the PARENT's leader, which is necessarily a voter of the
            // parent, and every child's `bootstrap_voters` — this group's
            // own `all_nodes` — is the parent's full voter-plus-learner
            // union, a superset. So this assertion should never fire; if it
            // does, the `plan`/`materialize_split_child` wiring upstream
            // computed `campaign` for the wrong replica, which is exactly
            // the class of bug this exists to catch loudly instead of
            // silently no-op'ing away.
            assert!(
                c.config().contains(&env.node_id()),
                "ADR 0058 rung 4: an immediate campaign was requested for a \
                 replica that is not a voter of the freshly-forked child — \
                 the parent-leader-at-fork invariant was violated upstream"
            );
            let outs = c.campaign_now(env.now(), entropy);
            (outs, persist.gate(c.has_unflushed_wal()))
        };
        let campaign_outs: Vec<(NodeId, KvWire)> = campaign_outs
            .into_iter()
            .map(|(to, m)| (to, KvWire::Raft(m)))
            .collect();
        match campaign_gate {
            // The common case: a pre-vote round touches no persisted hard
            // state, so nothing is owed and this ships at once — no
            // network hop, no consensus round, no drain to wait on beyond
            // the pre-vote/vote round trip itself.
            None => {
                for (to, wire) in campaign_outs {
                    env.send_stream(to, stream, codec::encode_wire(&wire)).await;
                }
            }
            // Only reachable for a degenerate single-voter bootstrap set,
            // where `campaign_now` runs straight through pre-vote's own
            // self-majority short-circuit into `start_election`/
            // `become_leader` and dirties hard state (`current_term`/
            // `voted_for`). Mirror the main loop's own gate/release
            // discipline (issue #279) rather than shipping anything ahead
            // of the fsync that makes it durable — the loop's very first
            // iteration below will start that round (`has_unflushed_wal()`
            // is true) and its own release logic drains `gated` once it
            // lands, with no special-casing needed here.
            Some(round) => {
                gated.push(round, campaign_outs);
            }
        }
    }

    loop {
        // A requested shutdown exits *between* persist rounds so the WAL is never
        // left mid-write; `stopped` (paired with the apply task's `apply_stopped`)
        // tells the teardown path the artifacts are quiescent. An in-flight round
        // is **awaited to completion**, never dropped: `abort()` is a request, not
        // a guarantee, and `Reconciler::teardown` deletes this WAL the moment
        // `is_stopped()` goes true. Anything still gated is discarded — a halted
        // node ships nothing more, and its artifacts are about to be deleted.
        if halted.load(Ordering::SeqCst) {
            if let Some(fut) = persist_fut.take() {
                fut.await;
            }
            gated.clear();
            stopped.store(true, Ordering::SeqCst);
            return;
        }

        // Start a round if this node owes the WAL anything and none is in flight.
        // The pending records are whatever the previous iteration's step left, or
        // what a proposer appended outside this loop (`propose_and_wake`). This is
        // the loop's one round-start site, and — the whole point of issue #279 —
        // the `append`/`fsync` it runs is raced *inside* the `select` below rather
        // than blocking the loop before it, so heartbeats keep flowing and the
        // election deadline keeps being re-armed while the disk is slow.
        if persist_fut.is_none()
            && core
                .lock()
                .expect("raftkv core poisoned")
                .has_unflushed_wal()
        {
            persist_fut = Some(Box::pin(persist_wal(
                &env,
                &wal,
                &core,
                &wal_lock,
                &apply_signal,
                &halted,
                &persist,
            )));
        }
        // A group with an unfinished round or an undelivered ack must keep its
        // timers: quiescence drops the timer arm entirely, so entering it here
        // would leave a completing round (or the compaction rewrite that
        // supersedes it) as the only wake source for messages a peer is waiting
        // on. ORed into the existing veto inputs below.
        let persist_veto = persist_fut.is_some() || !gated.is_empty();

        let now = env.now();
        // ADR 0044 phase-1 PR3: feed the one external input the core has no
        // visibility into itself — whether the (separate, async) apply task
        // has actually caught the engine up to `last_applied` — in the same
        // lock acquisition as `next_deadline`, once per loop iteration, before
        // `tick` can ever consult it (`quiesce_entry_ok`'s own doc).
        let (deadline, was_quiesced) = {
            let mut c = core.lock().expect("raftkv core poisoned");
            let caught_up = engine_applied.load(Ordering::SeqCst) == c.last_applied();
            c.set_quiesce_engine_caught_up(caught_up);
            // ADR 0044 phase-1 PR5, fork D: feed the quiesce veto — a
            // non-empty `TxnTracker` (this group has a pending 2PC intent or
            // a decided-but-unresolved record still owed a resolve) always
            // vetoes on its own; ORed with whatever external subsystem
            // (`animusd`'s `change_consumer_loop`) has set via
            // `set_quiesce_veto`, and (issue #279) with this loop's own
            // in-flight persist round / undelivered gated acks. Same lock
            // acquisition, same once-per-iteration cadence as
            // `quiesce_engine_caught_up` just above.
            let txn_veto = {
                let t = txn_tracker.lock().expect("txn tracker poisoned");
                !t.pending.is_empty() || !t.unresolved_decided.is_empty()
            };
            // Freshness (issue #302 fix): `txn_veto`/`persist_veto` are both
            // computed synchronously right here from always-current
            // in-memory state, so they need no freshness bound of their
            // own — only the external sweeper's own observation
            // (`external_quiesce_veto`, refreshed on its own ~200ms cadence
            // by `animusd`'s `change_consumer_loop`, not this loop's) can be
            // stale. `RaftCore::quiesce_entry_ok` only consults this value
            // when the OR'd veto is false, at which point it is exactly the
            // bound that matters: "as of what index did the last real
            // observation of this tablet's own obligation state land."
            c.set_quiesce_veto(
                txn_veto || persist_veto || external_quiesce_veto.load(Ordering::SeqCst),
                external_quiesce_veto_fresh_through.load(Ordering::SeqCst),
            );
            (c.next_deadline(), c.is_quiesced())
        };
        // `None` (ADR 0044 phase-1 PR3 quiescence) drops the timer arm
        // entirely rather than sleeping on a synthetic wait, so a genuinely
        // quiesced group posts zero `SimEnv` timeline events instead of a
        // degenerate busy-loop.
        let timer = match deadline {
            Some(deadline) => {
                Either::Left(env.sleep(Duration::from_nanos(deadline.0.saturating_sub(now.0))))
            }
            None => Either::Right(std::future::pending()),
        };

        // Snapshot the commit index before stepping the core so a real advance
        // (ADR 0015: record the outcome, not the attempt) can be attributed below.
        let before_commit = core.lock().expect("raftkv core poisoned").commit_index();

        // Each step yields outbound `KvWire` messages (Raft traffic and/or a read
        // probe ack) plus the persist round those messages must not outrun. Five
        // wakeup sources race: **a persist round landing** (issue #279 — either
        // this loop's own `fsync` finishing, or the apply task's compaction
        // rewrite completing a round it drained, which is why the watermark is
        // shared rather than loop-private), an inbound message, the Raft timer
        // deadline (absent — `None` — while quiesced, PR3), a **wake-on-propose**
        // signal — a proposer raising the flag so a freshly appended entry
        // replicates at once (ADR 0017 single-write latency), treated like an
        // immediate heartbeat (`replicate_now`) rather than waiting for the ~50ms
        // tick — and the driver-level **wake** signal (`shutdown`/`wake`, PR2/PR4),
        // which does nothing itself beyond looping back to re-check `halted` and
        // re-evaluate `next_deadline`.
        let recv_or_timer = select(env.recv_stream(stream), timer);
        let wake_or_recv_or_timer = select(
            WakePending {
                signal: &wake_signal,
            },
            recv_or_timer,
        );
        let stepped = select(
            ProposePending {
                signal: &propose_signal,
            },
            wake_or_recv_or_timer,
        );
        // The persist arm is polled first, so a landed round releases its acks
        // ahead of taking on more work.
        let persist_arm = PersistArm::new(&persist, persist_fut.as_mut(), gated.min_round());
        let mut own_round_done = false;
        let (outs, gate): (Vec<(NodeId, KvWire)>, Option<u64>) =
            match select(persist_arm, stepped).await {
                // A round landed. Nothing to step: the release below ships
                // whatever was waiting on it.
                Either::Left((wake, _)) => {
                    own_round_done = wake == PersistWake::OwnRoundDone;
                    (Vec::new(), None)
                }
                Either::Right((stepped, _)) => match stepped {
                    // Wake-on-propose: ship the new entry now (leader-only; empty otherwise).
                    Either::Left(((), _)) => {
                        let (raft_outs, gate) = {
                            let mut c = core.lock().expect("raftkv core poisoned");
                            let outs = c.replicate_now(env.now());
                            // The gate is read in the **same lock acquisition** as
                            // the step that made the mutation — the one detail
                            // both reverted attempts got wrong (see
                            // `persist_round`'s module doc).
                            (outs, persist.gate(c.has_unflushed_wal()))
                        };
                        (
                            raft_outs
                                .into_iter()
                                .map(|(to, m)| (to, KvWire::Raft(m)))
                                .collect(),
                            gate,
                        )
                    }
                    // Driver-level wake: looping back already re-checks `halted` and
                    // re-evaluates `next_deadline` (finding 4's hazard 1). It also
                    // covers a locally-woken **quiesced follower**'s "are you still
                    // there?" check (ADR 0044 phase-1 PR3, fork B) —
                    // `on_local_wake` is a no-op for every other state (not quiesced,
                    // or this node is the leader), so this arm stays inert exactly as
                    // it was in PR2 for a quiesced-leader wake or any wake on a
                    // ticking group.
                    Either::Right((Either::Left(((), _)), _)) => {
                        let entropy = env.next_u64();
                        let (raft_outs, gate) = {
                            let mut c = core.lock().expect("raftkv core poisoned");
                            let outs = c.on_local_wake(env.now(), entropy);
                            (outs, persist.gate(c.has_unflushed_wal()))
                        };
                        (
                            raft_outs
                                .into_iter()
                                .map(|(to, m)| (to, KvWire::Raft(m)))
                                .collect(),
                            gate,
                        )
                    }
                    Either::Right((Either::Right((Either::Left((envelope, _)), _)), _)) => {
                        let entropy = env.next_u64();
                        match codec::decode_wire(&envelope.payload) {
                            Ok(KvWire::Raft(msg)) => {
                                // Witnessing point (ADR 0018 §2 amendment): every
                                // command entry this replica receives — leader or
                                // follower alike — before the core decides whether to
                                // accept it (see `witness_append_entries`'s doc).
                                witness_append_entries(&hlc, &msg, env.now());
                                let (raft_outs, gate): (Vec<Out<KvCommand>>, Option<u64>) = {
                                    let mut c = core.lock().expect("raftkv core poisoned");
                                    let outs = c.handle(envelope.from, msg, env.now(), entropy);
                                    (outs, persist.gate(c.has_unflushed_wal()))
                                };
                                (
                                    raft_outs
                                        .into_iter()
                                        .map(|(to, m)| (to, KvWire::Raft(m)))
                                        .collect(),
                                    gate,
                                )
                            }
                            // A ReadProbe is answered iff we are still in the prober's term
                            // (we have not moved on to help elect a newer leader). Not
                            // consensus traffic — the core never sees it, and it makes no
                            // durable claim, so it never waits on a round.
                            Ok(KvWire::ReadProbe { term, epoch }) => {
                                let same_term =
                                    core.lock().expect("raftkv core poisoned").term() == term;
                                if same_term {
                                    (
                                        vec![(envelope.from, KvWire::ReadProbeAck { term, epoch })],
                                        None,
                                    )
                                } else {
                                    (Vec::new(), None)
                                }
                            }
                            Ok(KvWire::ReadProbeAck { term, epoch }) => {
                                let mut r = reads.lock().expect("read state poisoned");
                                if let Some((t, acks)) = r.pending.get_mut(&epoch)
                                    && *t == term
                                {
                                    acks.insert(envelope.from);
                                }
                                (Vec::new(), None)
                            }
                            Err(err) => {
                                tracing::warn!(?err, "undecodable raftkv message dropped");
                                (Vec::new(), None)
                            }
                        }
                    }
                    Either::Right((Either::Right((Either::Right(((), _)), _)), _)) => {
                        let entropy = env.next_u64();
                        let (raft_outs, gate) = {
                            let mut c = core.lock().expect("raftkv core poisoned");
                            let outs = c.tick(env.now(), entropy);
                            (outs, persist.gate(c.has_unflushed_wal()))
                        };
                        (
                            raft_outs
                                .into_iter()
                                .map(|(to, m)| (to, KvWire::Raft(m)))
                                .collect(),
                            gate,
                        )
                    }
                },
            };
        // Safe only here: the `select` above (which borrows it) has been dropped.
        if own_round_done {
            persist_fut = None;
        }

        let (after_commit, install_pending, is_quiesced_now) = {
            let c = core.lock().expect("raftkv core poisoned");
            (c.commit_index(), c.has_pending_install(), c.is_quiesced())
        };
        if after_commit > before_commit {
            metrics.incr_by(Metric::CpCommits, after_commit - before_commit);
        }
        // ADR 0044 phase-1 PR7: count every genuine quiesced/ticking
        // transition this loop iteration observed — `was_quiesced` was
        // sampled at the very top of this same iteration (before the
        // message/timer/wake select and the core step that could have
        // entered or exited quiescence), so exactly one of these fires per
        // transition, never both and never a repeat while the state holds.
        match (was_quiesced, is_quiesced_now) {
            (false, true) => metrics.incr(Metric::CpQuiesces),
            (true, false) => metrics.incr(Metric::CpUnquiesces),
            _ => {}
        }
        // Wake-on-commit (ADR 0044 phase-1 PR1): a commit-index advance covers both
        // a follower's in-line apply on `AppendEntries` (gated on `commit_index`
        // alone, so it can create apply work with no separate `mark_durable_through`
        // call this pass) and a completed snapshot install's `commit_index` jump
        // (`RaftCore::handle`'s install-completion path sets it directly). A
        // pending install is also checked explicitly — a read-only peek, never
        // drained here — since a future core change could in principle decouple
        // the two; over-notifying is always safe (the apply task just finds no
        // work and re-parks), so this errs toward raising it.
        if after_commit > before_commit || install_pending {
            apply_signal.notify();
        }
        record_kv_outbound(&metrics, &outs);

        // Durability before action, message by message (issue #279). A step that
        // owes the WAL nothing (`gate` is `None`) ships everything at once, as
        // before. Otherwise only the messages that make a durability claim —
        // vote grants, append accepts, and the rest of the non-allowlisted set —
        // wait for their round; replication, heartbeats and pre-vote traffic go
        // out immediately, which is what keeps a group alive across a slow
        // `fsync`. Engine apply happens independently on the apply task.
        let (immediate, held): (Vec<_>, Vec<_>) = match gate {
            None => (outs, Vec::new()),
            Some(_) => outs
                .into_iter()
                .partition(|(_, wire)| ships_before_durable(wire)),
        };
        if let Some(round) = gate {
            gated.push(round, held);
        }
        for (to, wire) in immediate {
            env.send_stream(to, stream, codec::encode_wire(&wire)).await;
        }
        // Whatever round landed — this loop's own `fsync` or the apply task's
        // compaction rewrite — releases the acks that were waiting on it.
        for (to, wire) in gated.release(persist.durable()) {
            env.send_stream(to, stream, codec::encode_wire(&wire)).await;
        }
        // Safety net, and the reason a stranded ack is structurally impossible
        // rather than merely unlikely (issue #279's second bug): if this node
        // owes the WAL nothing *and* no round is in flight, then everything on
        // disk already backs every message still held — whichever task drained
        // it, and whether or not the round number lines up. Release the lot.
        // Without this, any drain that failed to number its round would leave
        // its buffer waiting on a round with no drainer, for as long as it took
        // an unrelated later write to start one (measured at 10.1s).
        if !gated.is_empty() {
            let settled = {
                let c = core.lock().expect("raftkv core poisoned");
                persist.fully_durable(c.has_unflushed_wal())
            };
            if settled && persist_fut.is_none() {
                for (to, wire) in gated.release(u64::MAX) {
                    env.send_stream(to, stream, codec::encode_wire(&wire)).await;
                }
            }
        }
    }
}

/// The per-node **apply task**: repeatedly install any received snapshot, apply
/// committed-and-durable commands to the engine, and compact — all off the consensus
/// loop, so this slow work never delays Raft message/heartbeat processing (the
/// driver-liveness fix, ADR 0017). Backs off by racing [`ApplyPending`] against
/// [`APPLY_SAFETY_POLL`] only when idle (ADR 0044 phase-1 PR1 — replaces the old
/// unconditional `APPLY_IDLE_POLL` 5ms poll); under load it stays in lockstep
/// behind commit. Exits after [`shutdown`](RaftKvNode::shutdown) between full
/// apply passes (so the engine/WAL are never left mid-write), setting
/// `apply_stopped` for the teardown path — `shutdown` also raises `apply_signal`,
/// so a parked task notices within one wake rather than waiting out the (now much
/// longer) safety poll.
#[allow(clippy::too_many_arguments)] // the apply task's shared-state bundle
async fn apply_loop<E: Env, S: StorageEngine>(
    env: E,
    wal: String,
    core: Arc<Mutex<KvCore>>,
    storage: S,
    cas: Arc<Mutex<CasResults>>,
    stage: Arc<Mutex<StageOutcomes>>,
    kind_outcomes: Arc<Mutex<KindBatchOutcomes>>,
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
    frozen: Arc<AtomicBool>,
    txn_tracker: Arc<Mutex<TxnTracker>>,
    apply_signal: Arc<ApplySignal>,
    fork_signal: Arc<ForkSignal>,
    persist: Arc<PersistProgress>,
) {
    // This apply task's own sequential, single-writer bookkeeping (see
    // `apply_and_compact`'s doc): `sealed` is seeded from the engine-durable
    // recovery scan `drive` already did; `max_applied_ts` starts `None` each
    // time this task starts (including after a restart) — the first
    // qualifying entry it processes is unconditionally accepted (see
    // `assert_ts_monotonic`'s doc for why that boundary case is safe).
    let mut max_applied_ts: Option<HlcTimestamp> = None;
    // ADR 0018 §2 write-loss amendment (Part B): the engine-durable version
    // watermark, captured once, right here, before this task (or this
    // process) has applied or minted anything this lifetime — the
    // replay-safe baseline `surface_suspicious_merge_noop` compares a
    // no-op's own entry version against. `drive`'s own recovery has already
    // run by the time this task is spawned (its doc: "after recovery seeded
    // the core + `engine_applied` + `sealed` + ..."), so this genuinely
    // reflects every write durable before this restart, not a half-recovered
    // snapshot.
    let recovered_baseline_version = storage.latest_version();
    let mut suspicious_noop_log_budget = SUSPICIOUS_MERGE_NOOP_LOG_CAP;
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
            &kind_outcomes,
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
            &frozen,
            &txn_tracker,
            recovered_baseline_version,
            &mut suspicious_noop_log_budget,
            &fork_signal,
            &persist,
        )
        .await;
        if !did_work {
            select(
                ApplyPending {
                    signal: &apply_signal,
                },
                env.sleep(APPLY_SAFETY_POLL),
            )
            .await;
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

    /// A tablet's parent scope (F2b: no table prefix — a private engine holds
    /// exactly one tablet of one table).
    fn table_scope(_name: &[u8]) -> StorageScope {
        StorageScope::whole()
    }

    #[test]
    fn sibling_scopes_carry_the_same_immutable_range() {
        let range = KeyRange::new(b"m".to_vec(), Some(b"n".to_vec()));
        let parent = StorageScope::new(range.clone());
        // Every kind reports the one declared range; nothing can move it
        // (ADR 0050: `narrow` died with the zero-copy split).
        assert_eq!(parent.with_kind(KIND_BASE).range(), range);
        assert_eq!(parent.with_kind(KIND_CHANGE).range(), range);
        assert_eq!(parent.range(), range);
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

    /// The rung-B4 seed path's foundation (ADR 0050): an `engine_image` of
    /// one tablet's private engine installs **byte-identically** — keys,
    /// values, tombstones, MVCC versions — into a different tablet's own
    /// engine through that tablet's own kind scopes.
    #[test]
    fn engine_image_round_trips_byte_identically_into_another_tablets_engine() {
        use animus_storage::{MemoryEngine, StorageEngine};
        futures::executor::block_on(async {
            let src = MemoryEngine::new();
            let src_scopes = kind_scopes(&StorageScope::whole());
            // Rows across three kinds + a tombstone, at distinct versions.
            type SeedRow = (u8, &'static [u8], Option<&'static [u8]>, u64);
            let rows: [SeedRow; 4] = [
                (KIND_BASE, b"k1", Some(b"v1"), 7),
                (KIND_LSI, b"k1", Some(b"lsi"), 8),
                (KIND_CHANGE, b"c1", Some(b"rec"), 9),
                (KIND_BASE, b"gone", None, 10),
            ];
            for (kind, key, value, version) in rows {
                let physical = src_scopes[kind as usize].physical(key);
                match value {
                    Some(v) => {
                        src.merge(&physical, v, version).await.unwrap();
                    }
                    None => {
                        src.merge_tombstone(&physical, version).await.unwrap();
                    }
                }
            }

            let image = engine_image(&src, &src_scopes).await;
            let dst = MemoryEngine::new();
            let dst_scopes = kind_scopes(&StorageScope::new(KeyRange::new(
                Vec::new(),
                Some(b"zzzz".to_vec()),
            )));
            install_engine_image(&dst, &dst_scopes, &image).await;

            let mut src_rows = src.entries_with_tombstones().await.unwrap();
            let mut dst_rows = dst.entries_with_tombstones().await.unwrap();
            src_rows.sort();
            dst_rows.sort();
            assert_eq!(
                src_rows, dst_rows,
                "the installed engine must be byte-identical to the source"
            );
        });
    }

    #[test]
    fn engine_global_marker_keys_never_match_any_kind_scope() {
        // F2b's load-bearing disjointness: a kind scope's keys lead with a
        // kind byte (0x00..=0x04); the engine-global marker keys
        // (`seal.rs`/`ceiling.rs`/syskv) lead with
        // `escape(RESERVED_NAMESPACE)`'s first byte, `b'_'` = 0x5F. A marker
        // physically resident in a tablet's private engine must be invisible
        // to every kind scope's strip (`engine_image`, `has_data`, scans).
        let marker_key =
            animus_tablet::escape(animus_control::syskv::RESERVED_NAMESPACE.as_bytes());
        assert_eq!(marker_key[0], 0x5F);
        let parent = StorageScope::whole();
        for &kind in &ALL_KINDS {
            assert_eq!(
                parent.with_kind(kind).strip_in_range(&marker_key),
                None,
                "kind {kind:#04x} must not claim a reserved-namespace marker key"
            );
        }
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
        // ADR 0050: each tablet holds its own private engine (the pre-pivot
        // version scoped two tables onto one shared engine — incidental to
        // this scenario's substance, which is txn orphan-abort convergence).
        let node_a: KvNode = RaftKvNode::start_scoped(
            sim.env(id_a.clone()),
            vec![id_a.clone()],
            engine.clone(),
            StorageScope::whole(),
        );
        let node_b: KvNode = RaftKvNode::start_scoped(
            sim.env(id_b.clone()),
            vec![id_b.clone()],
            MemoryEngine::new(),
            StorageScope::whole(),
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
                    vec![txn::TxnWrite::plain(kb_clone, Some(b"debited".to_vec()))],
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
        let anchor_writes = vec![txn::TxnWrite::plain(ka.clone(), Some(b"placed".to_vec()))];
        let participant_span_end = txn::immediate_successor(&kb);
        let (result, late_ts) = node_a.propose_ordered_aux(|term| {
            let ts = node_a.mint_pushed(term, std::slice::from_ref(&ka));
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
                ts,
            };
            (cmd, ts)
        });
        let late_index = match result {
            ProposeResult::Accepted { index, .. } => index,
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

    /// **Issue #298 shape A seatbelt**: `KvCommand::TxnStage`'s apply arm
    /// must reject a stage that targets a key THIS EXACT transaction
    /// already resolved on this group — `blocked_by`'s own foreign-intent
    /// check can never catch this (the key holds a plain `Committed`
    /// envelope by then, not an `Intent`). The live #298 trigger this
    /// guards (found while investigating a captured soak trace): a
    /// client-level retry of the same logical write races its own
    /// already-committed first attempt with a fresh `txn_id`, which stages
    /// (and later resolves) cleanly since it is a genuinely different
    /// transaction — this seatbelt instead covers the narrower but real
    /// same-txn case (a duplicate/stale re-propose of the identical
    /// `TxnStage` command), the shape the original candidate mechanism
    /// named. Pre-fix, this test's own re-stage attempt would overwrite
    /// `kb`'s committed value with a fresh `Intent` (`StageOutcome::
    /// Staged`); post-fix it is rejected (`StageOutcome::Fenced`) and the
    /// committed value is untouched.
    #[test]
    fn a_resolved_key_rejects_a_same_txn_restage_issue_298_shape_a() {
        let seed = 0xA5B1_0002u64;
        let mut sim = Simulator::new(seed);
        let id_b = nid(9021);
        let node_b: KvNode = RaftKvNode::start_scoped(
            sim.env(id_b.clone()),
            vec![id_b.clone()],
            MemoryEngine::new(),
            StorageScope::whole(),
        );
        sim.run_for(Duration::from_secs(2)); // elect (single voter)

        let kb = key(2, b":balance");
        // `record_key` only needs to be a structurally valid txn record key
        // here — this group never holds it locally (a pure-participant
        // stage, `is_anchor: false`, exactly like the live trace's own
        // participant-side resurrection).
        let anchor_token = &kb[..animus_tablet::TOKEN_BYTES];
        let txn_id = TxnId {
            ts: HlcTimestamp {
                wall_ms: 1_000,
                logical: 0,
            },
            node: id_b.clone(),
        };
        let record_key = txn::record_key(anchor_token, &txn_id);

        // Stage the participant's own key for real.
        let n = node_b.clone();
        let (txn_id_1, record_key_1, kb_1) = (txn_id.clone(), record_key.clone(), kb.clone());
        let staged = drive(
            &mut sim,
            node_b.env(),
            Duration::from_millis(300),
            async move {
                n.txn_stage_participant(
                    txn_id_1,
                    record_key_1,
                    "orders".to_string(),
                    vec![txn::TxnWrite::plain(kb_1, Some(b"v1".to_vec()))],
                    Vec::new(),
                )
                .await
            },
        )
        .flatten();
        assert_eq!(
            staged.as_ref().map(|(_, outcome)| outcome.clone()),
            Some(StageOutcome::Staged),
            "initial stage should succeed (seed={seed})"
        );

        // Resolve it: Committed.
        let n = node_b.clone();
        let (txn_id_2, record_key_2, kb_2) = (txn_id.clone(), record_key.clone(), kb.clone());
        let commit_ts = txn_id.ts;
        let resolve_ts = drive(
            &mut sim,
            node_b.env(),
            Duration::from_millis(300),
            async move {
                n.txn_resolve(
                    txn_id_2,
                    record_key_2,
                    vec![kb_2],
                    txn::TxnOutcome::Committed { commit_ts },
                )
                .await
            },
        );
        assert!(
            resolve_ts.flatten().is_some(),
            "resolve should apply (seed={seed})"
        );

        let committed_value = drive(&mut sim, node_b.env(), Duration::from_millis(300), {
            let n = node_b.clone();
            let kb = kb.clone();
            async move { n.local_get(&kb).await }
        })
        .flatten();
        assert_eq!(
            committed_value,
            Some(b"v1".to_vec()),
            "the resolved value must be visible before the re-stage attempt (seed={seed})"
        );

        // A stale/duplicate stage for the SAME (txn_id, key) arrives —
        // built by hand exactly like the sibling resurrection test above,
        // since `txn_stage_participant` always mints a fresh entry `ts`
        // but this scenario needs the identical `txn_id` to reuse the
        // already-resolved one.
        let restage_writes = vec![txn::TxnWrite::plain(
            kb.clone(),
            Some(b"v2-should-never-land".to_vec()),
        )];
        let (result, restage_ts) = node_b.propose_ordered_aux(|term| {
            let ts = node_b.mint_pushed(term, std::slice::from_ref(&kb));
            let cmd = KvCommand::TxnStage {
                txn_id: txn_id.clone(),
                record_key: record_key.clone(),
                record_table: "orders".to_string(),
                is_anchor: false,
                writes: restage_writes.clone(),
                spans: Vec::new(),
                conditions: Vec::new(),
                ts,
            };
            (cmd, ts)
        });
        let (restage_index, restage_term) = match result {
            ProposeResult::Accepted { index, term } => (index, term),
            other => panic!("restage proposal rejected: {other:?} (seed={seed})"),
        };
        let applied = drive(&mut sim, node_b.env(), Duration::from_millis(300), {
            let n = node_b.clone();
            async move { n.wait_applied(restage_index).await }
        });
        assert_eq!(
            applied,
            Some(true),
            "the restage entry itself must still apply (as a rejected no-op) \
             (seed={seed}, ts={restage_ts:?})"
        );
        assert_eq!(
            node_b.stage_outcome(restage_index, restage_term),
            Some(StageOutcome::Fenced),
            "the seatbelt must reject the restage as Fenced, never silently stage it \
             (seed={seed})"
        );

        let value_after_restage = drive(&mut sim, node_b.env(), Duration::from_millis(300), {
            let n = node_b.clone();
            let kb = kb.clone();
            async move { n.local_get(&kb).await }
        })
        .flatten();
        assert_eq!(
            value_after_restage,
            Some(b"v1".to_vec()),
            "the committed value must be untouched — no resurrection into Intent (seed={seed})"
        );
    }
}

/// Whether an outbound [`KvWire`] may ship before its persist round is durable
/// (issue #279) — this plane's thin wrapper over the shared
/// [`persist_round::ships_before_durable`] policy.
///
/// The consensus traffic decision is the shared one, so the two planes can
/// never drift on which Raft messages make a durability claim. Only this
/// crate's own **non-consensus** variants are decided here:
/// `ReadProbe`/`ReadProbeAck` are a ReadIndex barrier the `RaftCore` never even
/// sees, carry no state claim, and so are never held back.
fn ships_before_durable(wire: &KvWire) -> bool {
    match wire {
        KvWire::ReadProbe { .. } | KvWire::ReadProbeAck { .. } => true,
        KvWire::Raft(msg) => persist_round::ships_before_durable(msg),
    }
}

/// ADR 0058 Train 2 rung 4's structural belt-and-suspenders test: the
/// deterministic-first-leader mechanism (`start_hosted_campaigning`,
/// `RaftCore::campaign_now`) must never be exercised for a replica that
/// isn't itself a voter of the freshly-forked group. The upstream caller
/// contract (`plan`/`materialize_split_child`) makes this structurally
/// impossible in production — the self-nominating replica is always the
/// parent's own leader, hence always a voter of both children by
/// construction (see `start_hosted_campaigning`'s own doc) — so this proves
/// the *tripwire* actually fires, not that the invariant can be violated in
/// practice.
#[cfg(test)]
mod campaign_now_tests {
    use super::*;
    use animus_env::nid;
    use animus_sim::{SimEnv, Simulator};
    use animus_storage::MemoryEngine;

    #[test]
    #[should_panic(expected = "parent-leader-at-fork invariant was violated upstream")]
    fn start_hosted_campaigning_panics_if_the_caller_is_not_a_voter_of_the_group() {
        let mut sim = Simulator::new(0x0580_0004);
        let env: SimEnv = sim.env(nid(9));
        // Node 9 is deliberately NOT a member of `all_nodes` at all — an
        // upstream bug class this assertion exists to catch loudly instead
        // of `RaftCore::campaign_now`'s own `is_voter()` gate silently
        // no-op'ing it away.
        let _node = RaftKvNode::<SimEnv, MemoryEngine>::start_hosted_campaigning(
            env,
            vec![nid(0), nid(1), nid(2)],
            MemoryEngine::new(),
            StorageScope::whole(),
            77,
        );
        sim.run_for(Duration::from_millis(10));
    }

    /// The mirror-image happy path, proving the assertion is not simply
    /// always-fires: a genuine bootstrap voter campaigning for its own
    /// freshly-formed group's leadership never panics and does become
    /// leader — the fast path this whole rung exists to provide.
    #[test]
    fn start_hosted_campaigning_elects_the_campaigning_voter_promptly() {
        let mut sim = Simulator::new(0x0580_0005);
        let all: Vec<NodeId> = vec![nid(0), nid(1), nid(2)];
        let env0 = sim.env(nid(0));
        let node0 = RaftKvNode::<SimEnv, MemoryEngine>::start_hosted_campaigning(
            env0,
            all.clone(),
            MemoryEngine::new(),
            StorageScope::whole(),
            77,
        );
        // The other two replicas start ordinarily (no campaign) — mirrors
        // "every fork participant materializes both children, only the
        // parent-leader-at-fork replica campaigns."
        for id in [nid(1), nid(2)] {
            let env = sim.env(id);
            let _ = RaftKvNode::<SimEnv, MemoryEngine>::start_hosted(
                env,
                all.clone(),
                MemoryEngine::new(),
                StorageScope::whole(),
                77,
            );
        }
        sim.run_for(Duration::from_millis(200));
        assert!(
            node0.is_leader(),
            "the campaigning voter must win leadership well within one election timeout"
        );
    }
}

/// Table-driven coverage of [`RaftKvNode::stale_read_ready_decision`] (ADR
/// 0055's cheap eventually-consistent-read freshness gate): the full outcome
/// table over its two real inputs (knows-a-leader, and whether the engine
/// has applied everything the core has committed) plus the boundary cases
/// around "caught up" — no `RaftCore`/`RaftKvNode` construction, no `Env`,
/// no lock.
#[cfg(test)]
mod stale_read_ready_tests {
    use super::*;
    use animus_sim::SimEnv;
    use animus_storage::MemoryEngine;

    /// `RaftKvNode<SimEnv, MemoryEngine>::stale_read_ready_decision` — the
    /// concrete instantiation is arbitrary (the function reads none of
    /// `E`/`S`), just something that satisfies the impl block's bounds.
    fn decide(has_leader: bool, engine_applied_index: u64, commit_index: u64) -> bool {
        RaftKvNode::<SimEnv, MemoryEngine>::stale_read_ready_decision(
            has_leader,
            engine_applied_index,
            commit_index,
        )
    }

    /// The full 2x3 outcome table this gate computes: has-a-leader crossed
    /// with applied-vs-commit being behind / exactly caught up / ahead
    /// (`engine_applied_index` can only ever be `>= commit_index` once
    /// caught up, in practice, but the function itself takes plain `u64`s
    /// and its `>=` check is what's under test, not that invariant).
    #[test]
    fn full_outcome_table() {
        let cases: &[(bool, u64, u64, bool, &str)] = &[
            // (has_leader, engine_applied_index, commit_index, expected, why)
            (
                false,
                0,
                0,
                false,
                "no leader, trivially caught up -> still not ready",
            ),
            (
                false,
                100,
                10,
                false,
                "no leader, engine way ahead of commit -> still not ready: \
                 a replica with no leader_id may hold state that never \
                 existed on this tablet, so the gate must not trust it \
                 regardless of how far it has applied",
            ),
            (
                true,
                0,
                1,
                false,
                "leader known, but engine strictly behind the committed index -> not ready",
            ),
            (
                true,
                9,
                10,
                false,
                "leader known, engine one entry behind commit -> not ready (off-by-one floor)",
            ),
            (
                true,
                10,
                10,
                true,
                "leader known, engine exactly caught up to commit -> ready (the >= boundary)",
            ),
            (
                true,
                11,
                10,
                true,
                "leader known, engine ahead of the sampled commit (a commit_index read \
                 before a concurrent advance) -> ready",
            ),
            (
                true,
                0,
                0,
                true,
                "leader known, nothing committed yet -> trivially ready",
            ),
        ];
        for &(has_leader, applied, commit, expected, why) in cases {
            assert_eq!(
                decide(has_leader, applied, commit),
                expected,
                "has_leader={has_leader} applied={applied} commit={commit}: {why}"
            );
        }
    }

    /// Without a known leader, the gate is closed no matter how far ahead
    /// the engine is of the commit index — the leader check is a hard
    /// prerequisite, not something a sufficiently-caught-up engine can
    /// substitute for.
    #[test]
    fn no_leader_never_ready_regardless_of_applied_index() {
        for applied in [0, 1, 1_000, u64::MAX] {
            for commit in [0, 1, 1_000, u64::MAX] {
                assert!(
                    !decide(false, applied, commit),
                    "applied={applied} commit={commit} must stay not-ready with no known leader"
                );
            }
        }
    }

    /// With a known leader, readiness is exactly the "caught up" predicate:
    /// ready iff `engine_applied_index >= commit_index`.
    #[test]
    fn with_leader_readiness_matches_caught_up_predicate() {
        for applied in 0u64..8 {
            for commit in 0u64..8 {
                assert_eq!(
                    decide(true, applied, commit),
                    applied >= commit,
                    "applied={applied} commit={commit}"
                );
            }
        }
    }
}
