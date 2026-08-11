//! A minimal, synchronous Accord core (ADR 0011).
//!
//! [`AccordCore`] holds no I/O: a driver (see [`crate::node`]) owns the `Env`,
//! feeds the core decoded messages and client requests, and ships the outbound
//! messages the core returns. The logical clock is internal; entropy/time are
//! not needed by this slice (timestamps are logical, not wall-clock), so unlike
//! `RaftCore` the core takes no `now`/`entropy` parameters — but it keeps the
//! same shape: a pure state machine returning `Vec<Out>`.
//!
//! Each node plays two roles at once:
//!
//! - **Replica**: tracks, per transaction, the agreed execution timestamp and
//!   the set of conflicting transactions seen so far (its *dependencies*). On
//!   `PreAccept` it witnesses the proposed `t0`, records the new transaction's
//!   keys, and replies with the highest timestamp it has assigned to any
//!   conflicting transaction plus those conflicts as deps. On `Accept` it adopts
//!   the coordinator's chosen execution timestamp. On `Commit` it records the
//!   final `(execute_at, deps)` and, once every dependency it knows about has
//!   committed at a *lower* execution timestamp, marks the transaction
//!   committed-and-ordered.
//!
//! - **Coordinator**: drives one transaction it owns through PreAccept → (fast
//!   path) Commit, or PreAccept → Accept → Commit (slow path). The fast path
//!   fires when a fast quorum returns `t0` unchanged with identical deps; if any
//!   replica bumped the timestamp or deps disagree, the coordinator picks the
//!   highest returned timestamp, unions the deps, and runs an Accept round before
//!   committing.
//!
//! **Execution / Apply.** Once a transaction commits, the replica executes it —
//! but only in agreed-timestamp order and only after every conflicting
//! dependency that orders before it has itself executed. This is a per-replica
//! execution queue: a committed transaction becomes *applicable* when every
//! dependency is known-committed (so we know its `execute_at`) and every
//! dependency that orders before it has already applied; applicable transactions
//! are then drained in `(execute_at, txn)` order.
//!
//! The core decides **order**; the *effect* of execution is performed by the
//! [`crate::node`] driver against a real (async) `StorageEngine`. The core stays
//! synchronous and I/O-free: when a transaction becomes applicable it emits an
//! [`ApplyEffect`] into a `pending_apply` queue (write each key the transaction
//! touches, stamped with the transaction's `execute_at` as the MVCC version);
//! the driver drains that queue and applies it via the engine's per-key
//! last-writer-wins `merge`. Because every replica converges to the same
//! committed `(execute_at)` and applies in the same total `(execute_at, txn)`
//! order, every replica's store ends identical.
//!
//! **Durability.** The core emits [`WalRecord`]s at each phase transition
//! (`PreAccept`/`Accept`/`Commit`/`Apply`); the [`crate::node`] driver fsyncs
//! them before acting, and [`AccordCore::recovered`] rebuilds the core from a
//! replayed [`crate::persist::PersistedState`] — mirroring `animus-control`'s
//! `RaftCore`. The core itself stays synchronous and I/O-free; it only
//! *accumulates* records in `pending`, which the driver drains. On recovery the
//! core re-emits the [`ApplyEffect`]s for its recovered execution order, so a
//! restarted node repopulates a fresh (volatile) storage engine in the original
//! order — `merge` makes the re-apply idempotent.
//!
//! **Coordinator failover (this milestone, first slice).** If the coordinator of
//! an in-flight transaction dies after PreAccept/Accept but before the replicas
//! learn the `Commit`, any replica can take over as a *recovery coordinator*:
//! [`AccordCore::recover`] broadcasts `Recover`, replicas answer `RecoverOk` with
//! their recorded `(phase, execute_at, deps)`, and the recovery coordinator
//! drives the transaction to a `Commit` consistent with whatever the original
//! could have committed. The recovery rules (simplified — see below and ADR
//! 0011): if any replica already committed, adopt that decision verbatim;
//! otherwise **never** take the fast path — pick the highest `execute_at` and the
//! union of deps a recovery quorum reports and run a normal `Accept` → `Commit`
//! slow path. This is safe because a fast-path commit at `t0` requires a *fast
//! quorum* to have agreed on `t0`+deps, and any recovery quorum (a simple
//! majority) intersects that fast quorum, so the recovered slow-path decision can
//! only equal or supersede it.
//!
//! **Deliberately out of scope** (see ADR 0011): the full transitive dependency
//! wait-graph, the precise Accord recovery ballot/`PreAcceptOk`-witness rules
//! (we use a simpler "max-ts + union-deps, force slow path" recovery),
//! competing/duelling recovery coordinators, sharding, and timeout/livelock
//! handling (the driver does not yet *detect* a dead coordinator — recovery is
//! triggered explicitly, e.g. by a test or a future failure detector).

use std::collections::{BTreeMap, BTreeSet};

use animus_env::NodeId;

use crate::message::{AccordMsg, Out};
use crate::persist::{PersistedState, PersistedTxn, WalRecord};
use crate::timestamp::{Ballot, LogicalClock, Timestamp};
#[cfg(test)]
use animus_env::nid;

/// A transaction identifier: its original proposed timestamp `t0`, which is
/// globally unique (minted by exactly one coordinator). Doubles as the txn id.
pub type TxnId = Timestamp;

/// An opaque key a transaction reads/writes. Two transactions *conflict* (are
/// dependencies of each other) iff their key sets intersect. Kept as a simple
/// `u64` for this slice; the real system will key by partition/range.
pub type Key = u64;

/// The lifecycle phase of a transaction at a replica. Ordered by progress
/// (`PreAccepted < Accepted < Committed < Applied`); a phase never moves
/// backwards, which [`Phase::max_phase`] enforces on replay/merge.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Default,
    serde::Serialize,
    serde::Deserialize,
)]
pub enum Phase {
    /// Seen via `PreAccept`; `t0` witnessed, not yet given a final timestamp.
    #[default]
    PreAccepted,
    /// A coordinator-chosen execution timestamp adopted via `Accept`.
    Accepted,
    /// Final `(execute_at, deps)` recorded via `Commit`.
    Committed,
    /// The transaction's effect has been executed against the store.
    Applied,
}

impl Phase {
    /// The further-along of two phases (phases only ever advance).
    #[must_use]
    pub fn max_phase(self, other: Phase) -> Phase {
        self.max(other)
    }
}

/// What a replica knows about one transaction.
#[derive(Clone, Debug)]
struct ReplicaTxn {
    /// The transaction's full **conflict key set** — every key it reads *or*
    /// writes. Two transactions conflict (depend on each other) iff their
    /// `keys` intersect, so a read key participates in ordering exactly like a
    /// write key: a later write to a key this transaction read is ordered after
    /// it (and carries it as a dependency).
    keys: BTreeSet<Key>,
    /// The subset of `keys` this transaction **writes** (its write effect lands
    /// on these). A pure write transaction's `write_keys` equals `keys`; a pure
    /// read's is empty; a read-modify-write's holds the write keys while `keys`
    /// additionally covers the read-only keys. See [`AccordCore::submit_rw`].
    write_keys: BTreeSet<Key>,
    /// Caller-supplied value bytes for the keys this transaction writes
    /// (arbitrary write values, ADR 0011). A `write_keys` entry absent here
    /// executes as the transaction's encoded id (the classic register effect),
    /// so a valueless caller (`submit`/`submit_rw`) leaves this empty. Carried so
    /// a replica that learns the transaction only at `Commit` still writes the
    /// right value, and so recovery/failover replay the same bytes.
    write_values: BTreeMap<Key, Vec<u8>>,
    /// Best-known execution timestamp: `t0` until `Accept`/`Commit` raise it.
    execute_at: Timestamp,
    deps: BTreeSet<TxnId>,
    phase: Phase,
    /// A read-only transaction observes the store at its execution timestamp but
    /// writes nothing. It is still ordered (timestamp + conflict deps) exactly
    /// like a write, so it reads a consistent snapshot relative to conflicting
    /// writes; only its *effect* differs (a [`ReadEffect`], not an
    /// [`ApplyEffect`]). See [`AccordCore::submit_read`]. Equivalent to
    /// `write_keys.is_empty()`; kept explicit so it rides the wire/WAL and a
    /// read-modify-write (non-empty `write_keys`) is never treated as read-only.
    read_only: bool,
    /// The highest **recovery ballot** this replica has promised for the txn (ADR
    /// 0011, duelling recoverers). A `Recover`/`Accept` carrying a *lower* ballot
    /// is rejected (the sender was superseded by a higher recoverer); a `Recover`
    /// reports this so the superseded recoverer can retry higher. The original
    /// coordinator runs at [`Ballot::ZERO`], so promising any recovery ballot
    /// (`round >= 1`) also fences the original coordinator's late `Accept`.
    promised: Ballot,
    /// The ballot under which `execute_at`/`deps` were last **accepted** (via
    /// `Accept`), or [`Ballot::ZERO`] if only PreAccepted. Reported in `RecoverOk`
    /// so a recoverer adopts the `(execute_at, deps)` of the highest accepted
    /// ballot — the most recent proposal any replica committed to.
    accepted_ballot: Ballot,
    /// The **ballot a `Commit` was decided under** at this replica (ADR 0011): the
    /// original coordinator commits at [`Ballot::ZERO`], a recovery coordinator at
    /// its higher ballot. A later `Commit` carrying a *lower* ballot is **ignored**
    /// (it cannot revert the recorded decision) — this fences a stale
    /// original-coordinator `Commit` that arrives after a higher-ballot recovered
    /// commit (the failure-detector heal race). [`Ballot::ZERO`] until committed.
    commit_ballot: Ballot,
}

/// Coordinator-side progress for a transaction this node owns.
#[derive(Clone, Debug)]
struct Coordinating {
    t0: TxnId,
    /// Replies gathered in the current round, by responding node.
    replies: BTreeMap<NodeId, (Timestamp, BTreeSet<TxnId>)>,
    /// Whether we have already moved past the PreAccept round.
    phase: CoordPhase,
    /// True when this is a *recovery* coordinator (took over a dead
    /// coordinator's transaction). Recovery never uses the fast path.
    recovery: bool,
    /// True when the transaction this coordinator drives is read-only.
    read_only: bool,
    /// The transaction's full conflict key set, kept so a retry tick can
    /// **re-send** the `PreAccept` to peers that have not yet replied (the
    /// network is fire-and-forget and may drop — ADR 0011, message retry).
    keys: BTreeSet<Key>,
    /// The subset of `keys` the transaction writes, kept for the same retry
    /// re-send (rides the `PreAccept` / `Commit`).
    write_keys: BTreeSet<Key>,
    /// Caller-supplied value bytes per written key (arbitrary write values, ADR
    /// 0011), kept so a retry tick re-sends the same values on the
    /// `PreAccept`/`Commit`. Empty for a valueless caller.
    write_values: BTreeMap<Key, Vec<u8>>,
    /// The agreed `(execute_at, deps)` once the coordinator has chosen them
    /// (slow-path `Accept`, or `Commit`), kept so a retry tick can re-send the
    /// `Accept`/`Commit` to peers that have not yet acknowledged it. `None`
    /// while still in the PreAccept round.
    chosen: Option<(Timestamp, BTreeSet<TxnId>)>,
    /// Peers that have acknowledged the `Commit` (via `CommitAck`). `Commit` is
    /// otherwise fire-and-forget, so a retry tick re-sends it to peers absent
    /// here until every peer has it.
    commit_acks: BTreeSet<NodeId>,
    /// The **ballot** this coordinator's `Accept` round runs under (ADR 0011):
    /// [`Ballot::ZERO`] for the original coordinator, the adopted recovery ballot
    /// for a recovery coordinator. A replica rejects an `Accept` below its
    /// promised ballot, so a superseded recoverer's `Accept` is fenced.
    ballot: Ballot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CoordPhase {
    PreAccept,
    Accept,
    Done,
}

/// Recovery-coordinator state: the `RecoverOk` replies gathered so far for a
/// transaction this node is recovering.
#[derive(Clone, Debug)]
struct Recovering {
    /// The recovery ballot this round runs under (ADR 0011). Carried so the retry
    /// tick re-sends `Recover` at the same ballot, and so a `RecoverOk`/`Nack` for
    /// a *different* (superseded) ballot is ignored.
    ballot: Ballot,
    /// What each responder recorded for the transaction being recovered (only
    /// replies that **promised** `ballot`).
    replies: BTreeMap<NodeId, RecoverReply>,
}

/// What a replica reports about a transaction when it answers a `Recover` (the
/// facts that become a `RecoverOk`). Factored into a struct to keep
/// [`AccordCore::replica_recover`]'s return type readable.
struct RecoverFacts {
    phase: Phase,
    execute_at: Timestamp,
    deps: BTreeSet<TxnId>,
    /// The ballot under which this replica last accepted `(execute_at, deps)`.
    accepted_ballot: Ballot,
    keys: BTreeSet<Key>,
    write_keys: BTreeSet<Key>,
    write_values: BTreeMap<Key, Vec<u8>>,
    read_only: bool,
}

/// Outcome of a replica handling a ballot-bearing `Recover`/`Accept`: it either
/// **promised** (and reports its facts) or **rejected** because it had already
/// promised a strictly higher ballot.
enum BallotReply {
    /// The ballot was promised; act on the request.
    Promised(RecoverFacts),
    /// Rejected: the replica had promised this strictly-higher ballot.
    Nack(Ballot),
}

/// One replica's recorded state for a transaction under recovery, as reported in
/// its `RecoverOk`.
#[derive(Clone, Debug)]
struct RecoverReply {
    phase: Phase,
    execute_at: Timestamp,
    deps: BTreeSet<TxnId>,
    /// The ballot under which this replica last accepted `(execute_at, deps)`,
    /// or [`Ballot::ZERO`] if only PreAccepted. The recoverer re-proposes the
    /// `(execute_at, deps)` carried by the reply with the **highest** accepted
    /// ballot (the most recent prior proposal) so duelling recoverers converge.
    accepted_ballot: Ballot,
    keys: BTreeSet<Key>,
    /// The write subset the replica recorded. Recovery derives read-only-ness
    /// from the *union* of these across the quorum (a transaction is read-only
    /// iff it writes nothing), so the wire `read_only` flag is not stored here.
    write_keys: BTreeSet<Key>,
    /// The caller-supplied write values the replica recorded (arbitrary write
    /// values, ADR 0011). Recovery unions these across the quorum so a recovered
    /// transaction executes with the same values the original would have.
    write_values: BTreeMap<Key, Vec<u8>>,
}

/// Outcome of a coordinator finishing (committing) a transaction it owns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Decision {
    /// The transaction's id (its `t0`).
    pub txn: TxnId,
    /// The agreed execution timestamp.
    pub execute_at: Timestamp,
    /// Whether the fast path (single round trip of agreement) was taken.
    pub fast_path: bool,
}

/// A unit of execution work the core has decided to perform, handed to the
/// driver to apply against the (async) `StorageEngine`. The core decides the
/// order; the driver does the I/O.
///
/// Each key in `keys` is written, stamped with `version` (the transaction's
/// `execute_at.logical`) as the MVCC version. The **value** written is the
/// caller-supplied bytes in `values` for that key if present (arbitrary
/// caller-supplied write values, ADR 0011); a key absent from `values` defaults
/// (at the driver) to the transaction's encoded id — the classic register effect
/// for callers that supply no value. The driver applies with per-key
/// last-writer-wins (`merge`), which is idempotent and commutative, so
/// re-applying on recovery is harmless. Keys and values flow through the core
/// purely as data — the core never touches the store nor encodes a txn id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplyEffect {
    /// The transaction being executed.
    pub txn: TxnId,
    /// The keys it writes.
    pub keys: BTreeSet<Key>,
    /// The caller-supplied value for each written key. A key present in `keys`
    /// but absent here defaults (at the driver) to the transaction's encoded id,
    /// preserving the classic "write my id" effect for valueless callers.
    pub values: BTreeMap<Key, Vec<u8>>,
    /// The MVCC version to stamp each write with (its execution timestamp).
    pub version: Timestamp,
}

/// A read a *read-only* transaction has decided to perform, handed to the driver
/// to satisfy against the (async) `StorageEngine`. Like [`ApplyEffect`], the
/// core decides *when* (the agreed execution order) and the driver does the I/O
/// — but a read writes nothing.
///
/// The driver reads each key **as of `version`** (the transaction's `execute_at`
/// — `get_at`), so the read observes exactly the writes of transactions ordered
/// before it (which executed at a strictly lower version) and none ordered after
/// (which execute at a higher version). Because every replica converges to the
/// same committed order, the read observes the same snapshot on every replica.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadEffect {
    /// The read-only transaction being executed.
    pub txn: TxnId,
    /// The keys it reads.
    pub keys: BTreeSet<Key>,
    /// The MVCC version to read each key as of (its execution timestamp).
    pub version: Timestamp,
}

/// The Accord state machine for one node: replica state for every transaction it
/// has heard of, plus coordinator state for the transactions it owns.
pub struct AccordCore {
    id: NodeId,
    peers: Vec<NodeId>,
    cluster_size: usize,
    clock: LogicalClock,

    /// Replica view: what this node knows of each transaction.
    txns: BTreeMap<TxnId, ReplicaTxn>,
    /// Coordinator view: transactions this node is driving.
    coordinating: BTreeMap<TxnId, Coordinating>,
    /// Recovery-coordinator view: transactions this node is recovering (gathering
    /// `RecoverOk` for). Distinct from `coordinating`, which holds the normal
    /// PreAccept/Accept rounds the recovery transitions into.
    recovering: BTreeMap<TxnId, Recovering>,
    /// Decisions reached as coordinator, in the order they were reached.
    decisions: Vec<Decision>,

    /// The order in which this replica has executed (applied) transactions.
    applied_order: Vec<TxnId>,
    /// Execution effects the driver must apply to the `StorageEngine`, in order.
    /// The core decides the order and accumulates here; the driver drains via
    /// [`AccordCore::drain_apply`] and performs the async storage writes.
    pending_apply: Vec<ApplyEffect>,
    /// Read effects for read-only transactions the driver must satisfy against
    /// the `StorageEngine`, in execution order. Drained via
    /// [`AccordCore::drain_reads`]; the driver does the async `get_at`s.
    pending_read: Vec<ReadEffect>,
    /// Durable-state records the driver must fsync before acting on them. The
    /// core only accumulates; [`AccordCore::drain_persist`] hands them off.
    pending: Vec<WalRecord>,

    /// **Snapshot / log-truncation bookkeeping** (ADR 0011). The number of applied
    /// transactions covered by the last [`AccordCore::snapshot`]. The driver
    /// compares it to `applied_order.len()` to decide when enough new transactions
    /// have applied to be worth compacting the WAL.
    snapshotted_applied: usize,
    /// Set by [`AccordCore::snapshot`] to tell the driver the WAL should be
    /// **rewritten** to the compact image ([`AccordCore::wal_image`]) — the
    /// truncation is materialised as a full atomic replace, never incremental.
    snapshot_dirty: bool,
}

impl AccordCore {
    /// Create a core for `id`. `all_nodes` is the full replica set (including
    /// `id`); every transaction is replicated to all of them in this slice (one
    /// global shard — no placement yet).
    #[must_use]
    pub fn new(id: NodeId, all_nodes: &[NodeId]) -> AccordCore {
        let peers: Vec<NodeId> = all_nodes.iter().copied().filter(|n| *n != id).collect();
        AccordCore {
            id,
            peers,
            cluster_size: all_nodes.len(),
            clock: LogicalClock::new(id),
            txns: BTreeMap::new(),
            coordinating: BTreeMap::new(),
            recovering: BTreeMap::new(),
            decisions: Vec::new(),
            applied_order: Vec::new(),
            pending_apply: Vec::new(),
            pending_read: Vec::new(),
            pending: Vec::new(),
            snapshotted_applied: 0,
            snapshot_dirty: false,
        }
    }

    /// Rebuild a core for `id` from a replayed [`PersistedState`] (recovery after
    /// a restart). Replica facts and the execution order are restored from the
    /// WAL; coordinator state is *not* recovered (a coordinator that died
    /// mid-flight is recovered by a *different* replica via the recovery
    /// sub-protocol — see [`AccordCore::recover`] and ADR 0011). The logical
    /// clock is advanced past every timestamp seen so freshly-minted stamps stay
    /// monotonic.
    ///
    /// Recovery re-emits an [`ApplyEffect`] for each transaction in the recovered
    /// execution order so the driver repopulates a fresh (volatile) storage
    /// engine in the original order; `merge` makes the re-apply idempotent. No
    /// new `pending` *WAL* records are produced: durable recovery is silent.
    #[must_use]
    pub fn recovered(id: NodeId, all_nodes: &[NodeId], state: PersistedState) -> AccordCore {
        let mut core = AccordCore::new(id, all_nodes);
        for (txn, p) in state.txns {
            core.clock.witness(txn);
            core.clock.witness(p.execute_at);
            // An applied transaction recovers to the `Applied` phase even though
            // the phase-bearing records only reach `Committed`; the separate
            // `Applied` WAL record sets `p.applied`.
            let phase = if p.applied { Phase::Applied } else { p.phase };
            core.txns.insert(
                txn,
                ReplicaTxn {
                    keys: p.keys,
                    write_keys: p.write_keys,
                    write_values: p.write_values,
                    execute_at: p.execute_at,
                    deps: p.deps,
                    phase,
                    read_only: p.read_only,
                    // Restore the promised/accepted ballots so a restarted replica
                    // keeps its recovery promise and reports the right accepted
                    // ballot — it must not let a superseded recoverer win.
                    promised: p.promised,
                    accepted_ballot: p.accepted_ballot,
                    // Restore the commit ballot so a restarted replica still fences
                    // a stale lower-ballot `Commit` after recovery.
                    commit_ballot: p.commit_ballot,
                },
            );
        }
        // Replay the recovered apply order: restore `applied_order` and re-emit
        // the apply effects so the driver rebuilds the (volatile) store in the
        // original execution order. The order is durable (WAL `Applied` records),
        // so the re-applied store is identical to pre-crash.
        for txn in state.applied_order {
            core.applied_order.push(txn);
            if let Some(t) = core.txns.get(&txn) {
                // A recovered read re-emits a read effect; a write, a write effect
                // (so the driver rebuilds the volatile store / re-satisfies reads
                // in the original execution order).
                if t.read_only {
                    core.pending_read.push(ReadEffect {
                        txn,
                        keys: t.keys.clone(),
                        version: t.execute_at,
                    });
                } else {
                    core.pending_apply.push(ApplyEffect {
                        txn,
                        keys: t.write_keys.clone(),
                        values: t.write_values.clone(),
                        version: t.execute_at,
                    });
                }
            }
        }
        core
    }

    // ---- quorum arithmetic ----------------------------------------------

    /// Slow-path / recovery quorum: a strict majority of replicas (`⌊N/2⌋ + 1`).
    /// This is the size of the `Accept`/`Commit` quorum and of a recovery quorum.
    /// The implied failure tolerance is `f = N − slow_quorum = ⌊(N−1)/2⌋`.
    fn slow_quorum(&self) -> usize {
        self.cluster_size / 2 + 1
    }

    /// The implied failure tolerance `f`: the number of replicas that may fail
    /// while a slow/recovery quorum remains available, i.e. `N − slow_quorum`
    /// (`= ⌊(N−1)/2⌋`).
    fn failure_tolerance(&self) -> usize {
        self.cluster_size - self.slow_quorum()
    }

    /// The **precise fast-path quorum** for Accord's *simplified* recovery (the one
    /// this core implements — a recovery coordinator always forces the slow path;
    /// ADR 0011). Replaces the earlier conservative `⌈3N/4⌉` placeholder.
    ///
    /// The fast path commits at `t0` in one round only if a fast quorum of this
    /// size all reply identical `(t0, deps)`. For the simplified recovery procedure
    /// the safe, tight bound is **all-but-the-failure-tolerance** replicas — for the
    /// common `N = 2f+1` that is `F = N − 1` — sized so it **intersects every
    /// recovery quorum in ≥ 1 replica** *and* **every other fast quorum in ≥ 1
    /// replica** (the recoverability condition the simplified protocol requires):
    ///
    /// - **Fast ∩ recovery** `= F + slow_quorum − N = (N−1) + (⌊N/2⌋+1) − N =
    ///   ⌊N/2⌋ ≥ 1` (for `N ≥ 2`). Any recovery quorum thus contains ≥ 1 replica
    ///   that witnessed the fast-path `(t0, deps)`, so recovery can never miss that
    ///   a fast decision was possible — and our recovery's max-ts/union-deps rule
    ///   over a quorum that includes a fast-path witness reproduces (never
    ///   contradicts) the fast value.
    /// - **Fast ∩ fast** `= 2(N−1) − N = N − 2 ≥ 1` (for `N ≥ 3`). Two concurrent
    ///   fast attempts on the same transaction share a replica, so they cannot
    ///   fast-commit *different* values.
    ///
    /// The *optimized* bound `f + ⌊(f+1)/2⌋` is smaller but needs Accord's full
    /// PreAcceptOk-witness recovery (still deferred, ADR 0011); pairing it with our
    /// slow-path-only recovery would be unsafe, so we take the simplified bound.
    /// For the degenerate `f = 0` (`N ≤ 2`, no fault tolerance) the fast quorum is
    /// the whole cluster, so no single stray reply can wrongly fast-commit.
    fn fast_quorum(&self) -> usize {
        let n = self.cluster_size;
        if self.failure_tolerance() == 0 {
            // N <= 2: require unanimity (a recovery quorum is the whole cluster too).
            return n;
        }
        // The tight, recoverable simplified bound: all but one replica, never below
        // the slow quorum.
        (n - 1).max(self.slow_quorum())
    }

    // ---- accessors -------------------------------------------------------

    /// This node's id.
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// The decisions this node has reached as a coordinator, in order.
    #[must_use]
    pub fn decisions(&self) -> &[Decision] {
        &self.decisions
    }

    /// The agreed execution timestamp this replica recorded for `txn`, if it has
    /// reached the committed phase here (committed *or* already applied — both
    /// carry the final timestamp).
    #[must_use]
    pub fn committed_execute_at(&self, txn: TxnId) -> Option<Timestamp> {
        self.txns
            .get(&txn)
            .filter(|t| t.phase >= Phase::Committed)
            .map(|t| t.execute_at)
    }

    /// The phase this replica has reached for `txn`, if known.
    #[must_use]
    pub fn phase(&self, txn: TxnId) -> Option<Phase> {
        self.txns.get(&txn).map(|t| t.phase)
    }

    /// The dependencies this replica recorded for `txn`, if it is committed *or*
    /// applied (both have the final `(execute_at, deps)`).
    #[must_use]
    pub fn committed_deps(&self, txn: TxnId) -> Option<BTreeSet<TxnId>> {
        self.txns
            .get(&txn)
            .filter(|t| t.phase >= Phase::Committed)
            .map(|t| t.deps.clone())
    }

    /// The order in which this replica has executed (applied) transactions. The
    /// core property: two replicas that have applied the same set of conflicting
    /// transactions produce the same relative order here.
    #[must_use]
    pub fn applied_order(&self) -> &[TxnId] {
        &self.applied_order
    }

    /// Whether this replica has executed `txn`.
    #[must_use]
    pub fn is_applied(&self, txn: TxnId) -> bool {
        self.txns
            .get(&txn)
            .is_some_and(|t| t.phase == Phase::Applied)
    }

    /// Take the durable-state records accumulated since the last drain. The
    /// driver appends and `fsync`s these before sending outbound messages or
    /// otherwise acting on them.
    pub fn drain_persist(&mut self) -> Vec<WalRecord> {
        std::mem::take(&mut self.pending)
    }

    /// Take the execution effects accumulated since the last drain. The driver
    /// applies these to the `StorageEngine` (async) in order. Drained after the
    /// `Applied` WAL records are fsynced, so a re-apply after a crash is safe.
    pub fn drain_apply(&mut self) -> Vec<ApplyEffect> {
        std::mem::take(&mut self.pending_apply)
    }

    /// Take the read effects accumulated since the last drain. The driver reads
    /// each key as of the effect's `version` against the `StorageEngine` (async)
    /// to satisfy the read-only transaction's snapshot.
    pub fn drain_reads(&mut self) -> Vec<ReadEffect> {
        std::mem::take(&mut self.pending_read)
    }

    // ---- snapshot / log truncation --------------------------------------

    /// The current durable image of this replica's state as a [`PersistedState`]:
    /// every transaction the core still tracks, collapsed to one `PersistedTxn`
    /// each, plus the recovered execution order. This is exactly what
    /// [`AccordCore::recovered`] consumes, so a WAL of just
    /// `Snapshot { state: persisted_state() }` replays to the identical core.
    #[must_use]
    pub fn persisted_state(&self) -> PersistedState {
        let mut txns = BTreeMap::new();
        for (&txn, t) in &self.txns {
            txns.insert(
                txn,
                PersistedTxn {
                    keys: t.keys.clone(),
                    write_keys: t.write_keys.clone(),
                    write_values: t.write_values.clone(),
                    execute_at: t.execute_at,
                    deps: t.deps.clone(),
                    // `Applied` is folded back into `phase` on recovery via the
                    // `applied` flag, mirroring how the WAL `Applied` record works:
                    // store the committed-or-earlier phase here and set `applied`.
                    phase: if t.phase == Phase::Applied {
                        Phase::Committed
                    } else {
                        t.phase
                    },
                    applied: t.phase == Phase::Applied,
                    read_only: t.read_only,
                    promised: t.promised,
                    accepted_ballot: t.accepted_ballot,
                    commit_ballot: t.commit_ballot,
                },
            );
        }
        PersistedState {
            txns,
            applied_order: self.applied_order.clone(),
        }
    }

    /// The compact WAL image that replays to exactly the current durable state: a
    /// **single** [`WalRecord::Snapshot`] carrying [`AccordCore::persisted_state`].
    /// The driver atomically **replaces** the WAL with this during compaction,
    /// collapsing every covered transaction's multi-record phase history (up to
    /// `PreAccepted`+`Accepted`+`Promised`+`Committed`+`Applied`) into one record —
    /// so the WAL is bounded by the *live transaction set*, not the unbounded
    /// append history. Mirrors `RaftCore::wal_image`.
    ///
    /// Call only after [`drain_persist`](Self::drain_persist) has been flushed so
    /// the image and the on-disk WAL agree.
    #[must_use]
    pub fn wal_image(&self) -> Vec<WalRecord> {
        if self.txns.is_empty() {
            return Vec::new();
        }
        let state = self.persisted_state();
        vec![WalRecord::Snapshot {
            txns: state.txns.into_iter().collect(),
            applied_order: state.applied_order,
        }]
    }

    /// Take a snapshot of the current applied state and mark the WAL for a
    /// truncating rewrite. The snapshot base advances to cover every applied
    /// transaction, and [`AccordCore::wal_image`] (the compact rewrite the driver
    /// then materialises) folds the whole tracked state into one record. No-op (and
    /// no rewrite) if nothing new has applied since the last snapshot. Mirrors
    /// `RaftCore::snapshot`.
    pub fn snapshot(&mut self) {
        if self.applied_order.len() <= self.snapshotted_applied {
            return; // nothing new applied; a rewrite would not shrink anything
        }
        self.snapshotted_applied = self.applied_order.len();
        self.snapshot_dirty = true;
    }

    /// Take and clear the snapshot-dirty flag — the driver uses it to decide
    /// whether to rewrite the WAL to [`AccordCore::wal_image`] this iteration.
    pub fn take_snapshot_dirty(&mut self) -> bool {
        std::mem::replace(&mut self.snapshot_dirty, false)
    }

    /// Applied transactions not yet covered by the last [`AccordCore::snapshot`] —
    /// how much the WAL would shrink by compacting now. The driver snapshots once
    /// this crosses a threshold. Mirrors `RaftCore::applied_since_snapshot`.
    #[must_use]
    pub fn applied_since_snapshot(&self) -> usize {
        self.applied_order
            .len()
            .saturating_sub(self.snapshotted_applied)
    }

    // ---- failure-detector support ---------------------------------------

    /// Whether `txn` is **known here but not yet committed** — i.e. this replica
    /// has a `PreAccepted`/`Accepted` entry for it but never recorded a `Commit`.
    /// Such a transaction is *stranded* if its coordinator has died: it will never
    /// commit on its own. The driver's liveness tick uses this (together with a
    /// per-txn time bound) to decide whether to auto-trigger recovery. A committed
    /// or applied transaction returns `false` (it needs no recovery), as does an
    /// unknown one.
    #[must_use]
    pub fn is_uncommitted(&self, txn: TxnId) -> bool {
        self.txns
            .get(&txn)
            .is_some_and(|t| t.phase < Phase::Committed)
    }

    /// The transactions this replica currently knows of that have **not yet
    /// committed** here (phase `< Committed`). These are the candidates a liveness
    /// detector watches: if one stays here past a time bound without progressing to
    /// `Commit`, its coordinator is suspected dead and recovery is auto-triggered.
    ///
    /// Pure read of replica state — the core stays I/O-free and time-free; the
    /// driver owns the clock and the bound. Deterministic (`BTreeMap` order).
    #[must_use]
    pub fn uncommitted_txns(&self) -> Vec<TxnId> {
        self.txns
            .iter()
            .filter(|(_, t)| t.phase < Phase::Committed)
            .map(|(&txn, _)| txn)
            .collect()
    }

    /// The progress "fingerprint" of a transaction at this replica: a value that
    /// **strictly increases** whenever the transaction advances (phase forward, or
    /// a higher `execute_at` / more deps / a higher promised ballot adopted). The
    /// driver compares it across liveness ticks: if it *changed*, the transaction
    /// is making progress and recovery is deferred (a *slow-but-live* coordinator
    /// must not be spuriously recovered); if it is unchanged for the whole bound,
    /// the transaction is stalled. `None` if the transaction is unknown here.
    ///
    /// This is intrinsic replica state, so it is identical on every replica that
    /// has seen the same messages and stays deterministic. It is a monotone summary
    /// only — never decreases — so a transient reorder cannot fake progress.
    #[must_use]
    pub fn progress_fingerprint(&self, txn: TxnId) -> Option<u64> {
        self.txns.get(&txn).map(|t| {
            // Mix the monotone facts into one increasing-on-progress number:
            // phase dominates, then execute_at, then dep count, then promised
            // ballot round. Each only ever advances, so the sum only grows.
            ((t.phase as u64) << 56)
                | ((t.execute_at.logical & 0x000F_FFFF_FFFF_FFFF) << 8)
                | ((t.deps.len() as u64 + t.promised.round).min(0xFF))
        })
    }

    /// Whether this node is **actively driving** `txn` itself — either as its
    /// original coordinator or as an in-flight recovery coordinator. The liveness
    /// detector skips such a transaction: a node already driving a round (and
    /// re-sending via its retry tick) must not also self-recover it.
    #[must_use]
    pub fn is_driving(&self, txn: TxnId) -> bool {
        self.coordinating.contains_key(&txn) || self.recovering.contains_key(&txn)
    }

    /// Whether **this** node is the designated recoverer for a stalled `txn` at
    /// escalation `tier` (0 = first attempt). To minimise duelling recoverers the
    /// choice is **deterministic**: the candidates are the replica set with the
    /// transaction's original coordinator (`txn.node` — the minting node, presumed
    /// dead) removed, sorted ascending by id; the nominee at tier `t` is the
    /// `t`-th of those. So at tier 0 exactly **one** node (the lowest-id survivor)
    /// self-nominates — no duel in the common case — and if that nominee is itself
    /// dead/partitioned the next tier promotes the next-lowest survivor, and so on,
    /// until recovery succeeds. When duels do still happen (two tiers fire close
    /// together, or a healed coordinator), the **ballot** machinery guarantees
    /// safety and convergence; this only reduces how often they occur.
    ///
    /// Returns `false` once `tier` exhausts the candidate list (no nominee), so the
    /// driver stops escalating. The original coordinator never nominates itself
    /// (it is the suspected-dead node), so a coordinator that is merely slow but
    /// alive keeps driving its own transaction via its retry tick without a
    /// competing self-recovery.
    #[must_use]
    pub fn is_recovery_nominee(&self, txn: TxnId, tier: usize) -> bool {
        // Candidate recoverers: every replica except the (presumed-dead) original
        // coordinator, ascending by id. `txn.node` is the coordinator that minted
        // the timestamp/id.
        let coordinator = txn.node;
        let nominee = self
            .all_nodes_sorted()
            .into_iter()
            .filter(|&n| n != coordinator)
            .nth(tier);
        nominee == Some(self.id)
    }

    /// The full replica set (this node + peers), ascending by id. Deterministic.
    fn all_nodes_sorted(&self) -> Vec<NodeId> {
        let mut all: Vec<NodeId> = self.peers.clone();
        all.push(self.id);
        all.sort_unstable();
        all
    }

    // ---- message retry ---------------------------------------------------

    /// Re-emit the outbound messages for every in-flight round that has not yet
    /// completed, addressed only to peers that have not yet answered.
    ///
    /// `Network::send` is fire-and-forget and may drop a message, which would
    /// otherwise strand a transaction (a coordinator waiting on a quorum reply
    /// that never arrives, or a replica that never learns the `Commit`). The
    /// driver calls this on a periodic `Env` timer; the core stays synchronous
    /// and I/O-free — it only *recomputes* what is still owed and to whom:
    ///
    /// - a coordinating txn in **PreAccept**: re-send `PreAccept` to peers not in
    ///   `replies`;
    /// - in **Accept**: re-send `Accept` (its chosen `(execute_at, deps)`) to
    ///   peers not in `replies`;
    /// - **Done** (committed): re-send `Commit` to peers not in `commit_acks`;
    /// - a **recovering** txn: re-send `Recover` to peers not in `replies`.
    ///
    /// Re-sends are idempotent at the replica (every handler folds by `max`/union
    /// and de-dups), so a duplicate that races the original is harmless. A round
    /// that has completed (a `Done` coordinator with every peer acked, or a
    /// recovery already decided) emits nothing, so retries naturally stop.
    #[must_use]
    pub fn resend_pending(&mut self) -> Vec<Out> {
        let mut outs = Vec::new();
        for (&txn, c) in &self.coordinating {
            match c.phase {
                CoordPhase::PreAccept => {
                    for &p in &self.peers {
                        if !c.replies.contains_key(&p) {
                            outs.push((
                                p,
                                AccordMsg::PreAccept {
                                    txn,
                                    keys: c.keys.clone(),
                                    write_keys: c.write_keys.clone(),
                                    write_values: c.write_values.clone(),
                                    read_only: c.read_only,
                                },
                            ));
                        }
                    }
                }
                CoordPhase::Accept => {
                    if let Some((execute_at, deps)) = &c.chosen {
                        for &p in &self.peers {
                            if !c.replies.contains_key(&p) {
                                outs.push((
                                    p,
                                    AccordMsg::Accept {
                                        txn,
                                        ballot: c.ballot,
                                        execute_at: *execute_at,
                                        deps: deps.clone(),
                                    },
                                ));
                            }
                        }
                    }
                }
                CoordPhase::Done => {
                    if let Some((execute_at, deps)) = &c.chosen {
                        for &p in &self.peers {
                            if !c.commit_acks.contains(&p) {
                                outs.push((
                                    p,
                                    AccordMsg::Commit {
                                        txn,
                                        ballot: c.ballot,
                                        execute_at: *execute_at,
                                        deps: deps.clone(),
                                        write_keys: c.write_keys.clone(),
                                        write_values: c.write_values.clone(),
                                        read_only: c.read_only,
                                    },
                                ));
                            }
                        }
                    }
                }
            }
        }
        for (&txn, rec) in &self.recovering {
            for &p in &self.peers {
                if !rec.replies.contains_key(&p) {
                    outs.push((
                        p,
                        AccordMsg::Recover {
                            txn,
                            ballot: rec.ballot,
                        },
                    ));
                }
            }
        }
        outs
    }

    // ---- coordinator entry point ----------------------------------------

    /// Begin coordinating a new **write** transaction over `keys`. Mints a fresh
    /// `t0`, applies it locally as a replica too (the coordinator is one of the
    /// replicas), and returns the `PreAccept` broadcast to peers.
    ///
    /// Returns `(txn_id, outbound)`.
    pub fn submit(&mut self, keys: BTreeSet<Key>) -> (TxnId, Vec<Out>) {
        let write_keys = keys.clone();
        self.submit_inner(keys, write_keys, BTreeMap::new(), false)
    }

    /// Begin coordinating a **write** transaction with explicit
    /// caller-supplied **values** (arbitrary write values, ADR 0011): each
    /// `(key, value)` writes `value` to `key` at execution. The write key set is
    /// the map's keys; the conflict set equals it (a pure write). Unlike
    /// [`AccordCore::submit`] — whose effect is "write my id" — the executed value
    /// is exactly the bytes supplied here, so a black-box reader observes a real
    /// value. With an empty map this commits and writes nothing.
    ///
    /// Returns `(txn_id, outbound)`.
    pub fn submit_writes(&mut self, writes: BTreeMap<Key, Vec<u8>>) -> (TxnId, Vec<Out>) {
        self.submit_writes_rw(BTreeSet::new(), writes)
    }

    /// Begin coordinating a **read-modify-write** transaction with explicit
    /// caller-supplied write **values** (arbitrary write values, ADR 0011): it
    /// reads `read_keys` (which join the conflict set but produce no write) and
    /// writes each `(key, value)` in `writes`. The conflict set is `read_keys ∪
    /// writes.keys()`. This is the value-carrying form of
    /// [`AccordCore::submit_rw`]; the executed value at each written key is the
    /// supplied bytes, not the txn id. A read-modify-write with a non-empty write
    /// map is never read-only.
    ///
    /// Returns `(txn_id, outbound)`.
    pub fn submit_writes_rw(
        &mut self,
        read_keys: BTreeSet<Key>,
        writes: BTreeMap<Key, Vec<u8>>,
    ) -> (TxnId, Vec<Out>) {
        let write_keys: BTreeSet<Key> = writes.keys().copied().collect();
        let keys: BTreeSet<Key> = read_keys.union(&write_keys).copied().collect();
        let read_only = write_keys.is_empty();
        self.submit_inner(keys, write_keys, writes, read_only)
    }

    /// Begin coordinating a **read-modify-write** transaction: it *reads*
    /// `read_keys` and *writes* `write_keys` under a single Accord transaction.
    /// Its **conflict set is the union** of the two — so a key it merely read
    /// participates in ordering exactly like one it writes: a concurrent write to
    /// a read key is ordered relative to this transaction and recorded as a
    /// dependency (the read-then-write hazard). Only the `write_keys` carry the
    /// write *effect* at execution; the extra read-only keys order it but produce
    /// no write. With an empty `read_keys` this is exactly [`AccordCore::submit`].
    ///
    /// Returns `(txn_id, outbound)`.
    pub fn submit_rw(
        &mut self,
        read_keys: BTreeSet<Key>,
        write_keys: BTreeSet<Key>,
    ) -> (TxnId, Vec<Out>) {
        let keys: BTreeSet<Key> = read_keys.union(&write_keys).copied().collect();
        // A transaction that writes nothing is read-only regardless of its reads.
        let read_only = write_keys.is_empty();
        self.submit_inner(keys, write_keys, BTreeMap::new(), read_only)
    }

    /// Begin coordinating a **read-only** transaction over `keys`. Ordered
    /// exactly like a write — it mints a `t0`, intersects conflicting keys, and
    /// is committed at an agreed `execute_at` — but its execution *effect* is a
    /// snapshot read of each key as of `execute_at` rather than a write. Because
    /// it carries the conflicting writes as dependencies and waits for the
    /// earlier-ordered ones to apply, it observes exactly the writes ordered
    /// before it. See [`ReadEffect`].
    ///
    /// Returns `(txn_id, outbound)`.
    pub fn submit_read(&mut self, keys: BTreeSet<Key>) -> (TxnId, Vec<Out>) {
        self.submit_inner(keys, BTreeSet::new(), BTreeMap::new(), true)
    }

    fn submit_inner(
        &mut self,
        keys: BTreeSet<Key>,
        write_keys: BTreeSet<Key>,
        write_values: BTreeMap<Key, Vec<u8>>,
        read_only: bool,
    ) -> (TxnId, Vec<Out>) {
        let t0 = self.clock.mint();

        // Apply to our own replica state and seed the coordinator's reply set
        // with our own PreAcceptOk (we are a replica of every txn in this slice).
        let (ts, deps) = self.replica_pre_accept(t0, &keys, &write_keys, &write_values, read_only);

        let mut replies = BTreeMap::new();
        replies.insert(self.id, (ts, deps));
        self.coordinating.insert(
            t0,
            Coordinating {
                t0,
                replies,
                phase: CoordPhase::PreAccept,
                recovery: false,
                read_only,
                keys: keys.clone(),
                write_keys: write_keys.clone(),
                write_values: write_values.clone(),
                chosen: None,
                commit_acks: BTreeSet::new(),
                // The original coordinator runs at the zero ballot; only a
                // recovery coordinator adopts a higher one.
                ballot: Ballot::ZERO,
            },
        );

        let outs: Vec<Out> = self
            .peers
            .iter()
            .map(|&p| {
                (
                    p,
                    AccordMsg::PreAccept {
                        txn: t0,
                        keys: keys.clone(),
                        write_keys: write_keys.clone(),
                        write_values: write_values.clone(),
                        read_only,
                    },
                )
            })
            .collect();

        // A single-replica cluster (peers empty) can already have a quorum.
        let mut all = outs;
        if let Some(extra) = self.try_advance_coordinator(t0) {
            all.extend(extra);
        }
        (t0, all)
    }

    // ---- message dispatch ------------------------------------------------

    /// Handle an inbound message from `from`. Returns outbound messages.
    pub fn handle(&mut self, from: NodeId, msg: AccordMsg) -> Vec<Out> {
        match msg {
            AccordMsg::PreAccept {
                txn,
                keys,
                write_keys,
                write_values,
                read_only,
            } => {
                let (ts, deps) =
                    self.replica_pre_accept(txn, &keys, &write_keys, &write_values, read_only);
                vec![(from, AccordMsg::PreAcceptOk { txn, ts, deps })]
            }
            AccordMsg::PreAcceptOk { txn, ts, deps } => {
                self.coordinator_record(from, txn, ts, deps);
                self.try_advance_coordinator(txn).unwrap_or_default()
            }
            AccordMsg::Accept {
                txn,
                ballot,
                execute_at,
                deps,
            } => match self.replica_accept(txn, ballot, execute_at, &deps) {
                Ok(()) => vec![(from, AccordMsg::AcceptOk { txn })],
                // A higher recoverer fenced this `Accept`; tell the sender the
                // ballot that superseded it so it stops (or retries higher).
                Err(promised) => vec![(from, AccordMsg::AcceptNack { txn, promised })],
            },
            AccordMsg::AcceptOk { txn } => {
                self.coordinator_record_accept(from, txn);
                self.try_advance_coordinator(txn).unwrap_or_default()
            }
            AccordMsg::AcceptNack { txn, promised } => self.handle_superseded(txn, promised),
            AccordMsg::Commit {
                txn,
                ballot,
                execute_at,
                deps,
                write_keys,
                write_values,
                read_only,
            } => {
                self.replica_commit(
                    txn,
                    ballot,
                    execute_at,
                    deps,
                    write_keys,
                    write_values,
                    read_only,
                );
                // Acknowledge so the coordinator stops re-sending `Commit` to us
                // on its retry tick (ADR 0011, message retry). Idempotent: a
                // duplicate `Commit` re-acks harmlessly.
                vec![(from, AccordMsg::CommitAck { txn })]
            }
            AccordMsg::CommitAck { txn } => {
                if let Some(c) = self.coordinating.get_mut(&txn) {
                    c.commit_acks.insert(from);
                }
                Vec::new()
            }
            AccordMsg::Recover { txn, ballot } => match self.replica_recover(txn, ballot) {
                BallotReply::Promised(f) => vec![(
                    from,
                    AccordMsg::RecoverOk {
                        txn,
                        ballot,
                        phase: f.phase,
                        execute_at: f.execute_at,
                        deps: f.deps,
                        accepted_ballot: f.accepted_ballot,
                        keys: f.keys,
                        write_keys: f.write_keys,
                        write_values: f.write_values,
                        read_only: f.read_only,
                    },
                )],
                // A higher recoverer already holds this txn; report its ballot so
                // the superseded recoverer can retry strictly above it.
                BallotReply::Nack(promised) => {
                    vec![(from, AccordMsg::RecoverNack { txn, promised })]
                }
            },
            AccordMsg::RecoverOk {
                txn,
                ballot,
                phase,
                execute_at,
                deps,
                accepted_ballot,
                keys,
                write_keys,
                write_values,
                read_only,
            } => {
                let _ = read_only; // read-only-ness is derived from write_keys
                // Ignore a `RecoverOk` for a *different* ballot than the round we
                // are currently driving (a stale reply to a superseded attempt).
                let current = self.recovering.get(&txn).map(|r| r.ballot);
                if current != Some(ballot) {
                    return Vec::new();
                }
                self.recovery_record(
                    from,
                    txn,
                    RecoverReply {
                        phase,
                        execute_at,
                        deps,
                        accepted_ballot,
                        keys,
                        write_keys,
                        write_values,
                    },
                );
                self.try_advance_recovery(txn).unwrap_or_default()
            }
            AccordMsg::RecoverNack { txn, promised } => self.handle_superseded(txn, promised),
        }
    }

    /// React to being **superseded** by a higher ballot (a `RecoverNack` to our
    /// `Recover`, or an `AcceptNack` to our recovery `Accept`): a strictly-higher
    /// recovery coordinator exists for `txn`.
    ///
    /// We abandon our current attempt. We then **yield to the higher recoverer**
    /// rather than immediately bumping our ballot and re-broadcasting — that naïve
    /// "retry higher now" rule produces the classic duelling-proposers **livelock**
    /// (two recoverers ratchet each other's ballot forever within one instant,
    /// emitting an unbounded message storm). Instead we use a deterministic
    /// tiebreak: only the recoverer with the **higher node id** keeps going; a
    /// lower-id recoverer stands down and lets the winner finish. The winner's
    /// `Commit` then reaches us (directly or via its retry tick), and a *plain*
    /// replica simply records it — no further recovery from us. Standing down is
    /// safe: the surviving higher recoverer either commits (we adopt it) or itself
    /// gets recovered later by an even higher ballot. We only act if *this* node is
    /// the one driving recovery; a plain replica that merely promised a higher
    /// ballot does nothing. A committed txn is never perturbed.
    fn handle_superseded(&mut self, txn: TxnId, promised: Ballot) -> Vec<Out> {
        // If we already committed this txn as a coordinator, the decision stands.
        if self
            .coordinating
            .get(&txn)
            .is_some_and(|c| c.phase == CoordPhase::Done)
        {
            return Vec::new();
        }
        let was_recovering = self.recovering.contains_key(&txn);
        let was_recovery_coord = self.coordinating.get(&txn).is_some_and(|c| c.recovery);
        if !was_recovering && !was_recovery_coord {
            // Not our recovery to retry (e.g. a Nack arriving after we already
            // moved on, or for a txn we never recovered).
            return Vec::new();
        }
        // Drop the superseded attempt either way — it cannot make progress.
        self.recovering.remove(&txn);
        self.coordinating.remove(&txn);
        // Deterministic yield: only the higher-id recoverer retries (above the
        // ballot that fenced it). The winner of the id tiebreak drives to a commit;
        // lower-id recoverers stand down and will adopt that commit. This makes the
        // duel converge in a bounded number of rounds instead of livelocking.
        if self.id <= promised.node {
            return Vec::new();
        }
        let next = Ballot::next_above(promised.max(self.highest_promised(txn)), self.id);
        self.start_recovery_at(txn, next)
    }

    // ---- replica handlers ------------------------------------------------

    /// Replica side of `PreAccept`: witness `t0`, record the transaction's keys,
    /// and compute (a) the timestamp this replica proposes — `t0` unless a later
    /// conflicting transaction forces it higher — and (b) the conflicting
    /// transactions (its deps).
    fn replica_pre_accept(
        &mut self,
        txn: TxnId,
        keys: &BTreeSet<Key>,
        write_keys: &BTreeSet<Key>,
        write_values: &BTreeMap<Key, Vec<u8>>,
        read_only: bool,
    ) -> (Timestamp, BTreeSet<TxnId>) {
        self.clock.witness(txn);

        // Conflicting transactions: any other txn this replica knows whose key
        // set intersects, regardless of phase. Its deps are exactly those.
        let deps: BTreeSet<TxnId> = self
            .txns
            .iter()
            .filter(|(other, t)| **other != txn && !t.keys.is_disjoint(keys))
            .map(|(other, _)| *other)
            .collect();

        // Proposed timestamp: t0, unless a conflicting txn already has a
        // higher-or-equal execution timestamp here, in which case we must run
        // strictly after it — bump past it via a freshly minted timestamp.
        let max_conflict = self
            .txns
            .iter()
            .filter(|(other, t)| **other != txn && !t.keys.is_disjoint(keys))
            .map(|(_, t)| t.execute_at)
            .max();
        let proposed = match max_conflict {
            Some(c) if c >= txn => {
                self.clock.witness(c);
                self.clock.mint()
            }
            _ => txn,
        };

        // Record (or refresh) our replica entry for this txn.
        let entry = self.txns.entry(txn).or_insert_with(|| ReplicaTxn {
            keys: keys.clone(),
            write_keys: write_keys.clone(),
            write_values: write_values.clone(),
            execute_at: txn,
            deps: BTreeSet::new(),
            phase: Phase::PreAccepted,
            read_only,
            promised: Ballot::ZERO,
            accepted_ballot: Ballot::ZERO,
            commit_ballot: Ballot::ZERO,
        });
        entry.keys.extend(keys.iter().copied());
        entry.write_keys.extend(write_keys.iter().copied());
        // Caller-supplied write values are authoritative for the keys they cover;
        // fold them in (a duplicate PreAccept carries the same values, so this is
        // idempotent — last write of identical bytes wins).
        for (k, v) in write_values {
            entry.write_values.insert(*k, v.clone());
        }
        if proposed > entry.execute_at {
            entry.execute_at = proposed;
        }
        entry.deps.extend(deps.iter().copied());
        // The read-only flag is intrinsic to the transaction and authoritative
        // from `PreAccept`; but any known write key forces it `false` (a
        // read-modify-write is never read-only), so the flag stays consistent
        // with `write_keys` even if a `read_only=true` view raced in first.
        entry.read_only = (entry.read_only && read_only) && entry.write_keys.is_empty();
        let reply_deps = entry.deps.clone();
        let reply_ts = entry.execute_at;
        let record_keys = entry.keys.clone();
        let record_write_keys = entry.write_keys.clone();
        let record_write_values = entry.write_values.clone();
        let record_read_only = entry.read_only;
        // Durable before we reply: the coordinator's quorum counts a PreAcceptOk
        // only after it is on this replica's disk.
        self.pending.push(WalRecord::PreAccepted {
            txn,
            keys: record_keys,
            write_keys: record_write_keys,
            write_values: record_write_values,
            execute_at: reply_ts,
            deps: reply_deps.clone(),
            read_only: record_read_only,
        });
        (reply_ts, reply_deps)
    }

    /// Replica side of `Accept`: **promise** `ballot` and adopt the coordinator's
    /// chosen execution timestamp and dependency set (it is `>=` ours by
    /// construction). Returns `Ok(())` on a promise, or `Err(promised)` if the
    /// replica has already promised a strictly higher ballot — a higher recovery
    /// coordinator superseded this `Accept`, so it must be rejected
    /// ([`AccordMsg::AcceptNack`]) and *not* adopted (ADR 0011, duelling
    /// recoverers). Accepting under `ballot` also records it as the
    /// `accepted_ballot`, which `RecoverOk` reports so a later recoverer adopts the
    /// highest-ballot proposal.
    fn replica_accept(
        &mut self,
        txn: TxnId,
        ballot: Ballot,
        execute_at: Timestamp,
        deps: &BTreeSet<TxnId>,
    ) -> Result<(), Ballot> {
        if let Some(t) = self.txns.get(&txn)
            && ballot < t.promised
        {
            return Err(t.promised);
        }
        self.clock.witness(execute_at);
        let entry = self.txns.entry(txn).or_insert_with(|| ReplicaTxn {
            keys: BTreeSet::new(),
            write_keys: BTreeSet::new(),
            write_values: BTreeMap::new(),
            execute_at,
            deps: BTreeSet::new(),
            phase: Phase::Accepted,
            read_only: false,
            promised: Ballot::ZERO,
            accepted_ballot: Ballot::ZERO,
            commit_ballot: Ballot::ZERO,
        });
        entry.execute_at = entry.execute_at.max(execute_at);
        entry.deps.extend(deps.iter().copied());
        if entry.phase == Phase::PreAccepted {
            entry.phase = Phase::Accepted;
        }
        // Accepting under `ballot` promises it (a later Accept/Recover below it is
        // fenced) and records it as the ballot the proposal was accepted under.
        entry.promised = entry.promised.max(ballot);
        entry.accepted_ballot = entry.accepted_ballot.max(ballot);
        let record_ts = entry.execute_at;
        let record_deps = entry.deps.clone();
        let record_ballot = entry.accepted_ballot;
        self.pending.push(WalRecord::Accepted {
            txn,
            execute_at: record_ts,
            deps: record_deps,
            accepted_ballot: record_ballot,
        });
        Ok(())
    }

    /// Replica side of `Commit`: record the final execution timestamp and deps —
    /// the durable agreement point — then try to execute any transactions that
    /// have become applicable.
    #[allow(clippy::too_many_arguments)]
    fn replica_commit(
        &mut self,
        txn: TxnId,
        ballot: Ballot,
        execute_at: Timestamp,
        deps: BTreeSet<TxnId>,
        write_keys: BTreeSet<Key>,
        write_values: BTreeMap<Key, Vec<u8>>,
        read_only: bool,
    ) {
        self.clock.witness(execute_at);
        let entry = self.txns.entry(txn).or_insert_with(|| ReplicaTxn {
            keys: write_keys.clone(),
            write_keys: write_keys.clone(),
            write_values: write_values.clone(),
            execute_at,
            deps: BTreeSet::new(),
            phase: Phase::Committed,
            read_only,
            promised: Ballot::ZERO,
            accepted_ballot: Ballot::ZERO,
            commit_ballot: Ballot::ZERO,
        });
        // The Commit carries the authoritative write set (so a replica that
        // missed PreAccept still executes the right write); the write keys are
        // also part of the conflict set, and the values it must execute. These are
        // monotone facts, folded regardless of ballot.
        entry.keys.extend(write_keys.iter().copied());
        entry.write_keys.extend(write_keys.iter().copied());
        for (k, v) in &write_values {
            entry.write_values.insert(*k, v.clone());
        }
        // Read-only iff it writes nothing (consistent with `write_keys`).
        entry.read_only = (entry.read_only && read_only) && entry.write_keys.is_empty();

        // **Ballot fence on the decision.** A `Commit` whose ballot is *below* the
        // ballot a commit was already recorded under here is stale — e.g. a late
        // original-coordinator `Commit` (`Ballot::ZERO`) arriving after a survivor's
        // higher-ballot recovered commit (the failure-detector heal race). Adopting
        // it would revert the recovered `(execute_at, deps)` and diverge the store,
        // so we **ignore** the decision part of it (the monotone facts above were
        // still folded). A commit at an equal-or-higher ballot is adopted.
        let already_committed = entry.phase >= Phase::Committed;
        if already_committed && ballot < entry.commit_ballot {
            // Stale lower-ballot commit: keep the recorded decision. Still try to
            // execute in case the folded facts unblocked something.
            self.try_execute();
            return;
        }

        // A txn already applied stays applied (idempotent under a duplicate
        // Commit); otherwise mark it committed and (re)record the final state.
        if entry.phase < Phase::Committed {
            entry.phase = Phase::Committed;
        }
        entry.execute_at = execute_at;
        entry.deps = deps.clone();
        entry.commit_ballot = entry.commit_ballot.max(ballot);
        if entry.phase == Phase::Committed {
            let keys = entry.keys.clone();
            let record_write_keys = entry.write_keys.clone();
            let record_write_values = entry.write_values.clone();
            let record_read_only = entry.read_only;
            let record_ballot = entry.commit_ballot;
            self.pending.push(WalRecord::Committed {
                txn,
                keys,
                write_keys: record_write_keys,
                write_values: record_write_values,
                execute_at,
                deps,
                read_only: record_read_only,
                commit_ballot: record_ballot,
            });
        }
        self.try_execute();
    }

    // ---- execution -------------------------------------------------------

    /// Execute every transaction that has become *applicable*, in agreed order.
    ///
    /// A committed transaction is applicable when every transaction that orders
    /// before it in the agreed serialization order and could constrain it has
    /// already applied. Two conditions, both required (see [`Self::next_applicable`]):
    ///
    /// 1. **Direct-conflict gate** ([`Self::conflicts_clear_for`]) — every
    ///    *key-conflicting* transaction this replica knows of (any phase): a
    ///    not-yet-committed one might still land before us (its final timestamp is
    ///    unknown), and a committed-earlier one must have applied. This catches the
    ///    conflicts whose dependency edge is **not yet recorded** (the new
    ///    transaction may have raced ahead of theirs).
    /// 2. **Transitive dependency-closure gate** ([`Self::deps_clear_for`]) — every
    ///    transaction in the **transitive closure** of `txn`'s recorded `deps` that
    ///    orders before `txn` must be committed *and* applied. This is the piece the
    ///    direct gate misses: a dependency may be known here only as an *id* (learnt
    ///    via a peer's `Commit`/`Accept` dep set, never its own `PreAccept`, so this
    ///    replica has no key intersection to detect it), or be a *transitive* dep —
    ///    a conflict-of-a-conflict that does not share a key with `txn`. Without the
    ///    closure gate `txn` could execute before such a predecessor and violate the
    ///    agreed order. The closure is **cycle-aware**: Accord deps can be mutual
    ///    (each carries the other), and a dep that orders *after* `txn`
    ///    (`(execute_at, dep) > (execute_at, txn)`) does not block — the cycle is
    ///    broken by the total `(execute_at, txn)` order, so a finite closure always
    ///    drains.
    ///
    /// We apply the applicable transaction with the smallest `(execute_at, txn)`
    /// and repeat. Because the order is total and every replica converges to the
    /// same committed `(execute_at, deps)`, all replicas execute in the same order.
    fn try_execute(&mut self) {
        while let Some(txn) = self.next_applicable() {
            self.apply(txn);
        }
    }

    /// The applicable committed transaction with the smallest `(execute_at,
    /// txn)`, or `None` if none is ready. Applicable = both the direct-conflict
    /// gate ([`Self::conflicts_clear_for`]) and the transitive dependency-closure
    /// gate ([`Self::deps_clear_for`]) are clear.
    fn next_applicable(&self) -> Option<TxnId> {
        let mut best: Option<(Timestamp, TxnId)> = None;
        for (&txn, t) in &self.txns {
            if t.phase != Phase::Committed {
                continue; // not committed, or already applied
            }
            if !self.conflicts_clear_for(txn, t.execute_at, &t.keys) {
                continue;
            }
            if !self.deps_clear_for(txn, t.execute_at, &t.deps) {
                continue;
            }
            let key = (t.execute_at, txn);
            if best.is_none_or(|b| key < b) {
                best = Some(key);
            }
        }
        best.map(|(_, txn)| txn)
    }

    /// Whether nothing this replica knows of *by key conflict* could still need to
    /// execute before `txn`. For every other transaction whose key set intersects
    /// `txn`'s:
    ///
    /// - if it is not yet committed, its final timestamp is unknown and might
    ///   order before `txn` — block until it commits;
    /// - if it is committed and orders before `txn` (`(execute_at, other) <
    ///   (execute_at, txn)`) but has not applied, block until it does.
    ///
    /// A conflict that is committed and orders *after* `txn` does not block: it
    /// will run later. This gate sees only conflicts whose **keys** this replica
    /// knows; the [`Self::deps_clear_for`] gate covers recorded (incl. transitive)
    /// dependencies whose keys it may not.
    fn conflicts_clear_for(&self, txn: TxnId, execute_at: Timestamp, keys: &BTreeSet<Key>) -> bool {
        for (&other, o) in &self.txns {
            if other == txn || o.keys.is_disjoint(keys) {
                continue;
            }
            if o.phase < Phase::Committed {
                return false; // order not yet final; it might land before us
            }
            let orders_before = (o.execute_at, other) < (execute_at, txn);
            if orders_before && o.phase != Phase::Applied {
                return false; // an earlier-ordered conflict has not executed yet
            }
        }
        true
    }

    /// Whether the **transitive closure** of `txn`'s recorded dependencies that
    /// orders before `txn` has fully committed and applied — the transitive
    /// dependency wait-graph (ADR 0011).
    ///
    /// Walks `deps`, then each committed dependency's own `deps`, and so on. For
    /// each transaction `d` reached:
    ///
    /// - if `d` is not yet committed, its final `(execute_at, d)` is unknown — it
    ///   could order before `txn`, so we **block** (the dependency edge is real;
    ///   we must wait to learn its position and effect);
    /// - if `d` is committed and **orders before** `txn`
    ///   (`(execute_at, d) < (execute_at, txn)`) it must have **applied**, else
    ///   block; and we **recurse** into `d`'s own deps (transitivity);
    /// - if `d` is committed and orders **after** `txn`, it does not block and we
    ///   do **not** recurse through it — the `(execute_at, txn)` total order breaks
    ///   any dependency cycle here, so the walk is finite.
    ///
    /// A dependency this replica has never heard of at all (no `txns` entry) blocks
    /// like an un-committed one: its order is unknown and it might precede `txn`.
    /// Deterministic (`BTreeSet` walk order); no allocation beyond the visited set
    /// and a small stack.
    fn deps_clear_for(&self, txn: TxnId, execute_at: Timestamp, deps: &BTreeSet<TxnId>) -> bool {
        let here = (execute_at, txn);
        let mut visited: BTreeSet<TxnId> = BTreeSet::new();
        let mut stack: Vec<TxnId> = deps.iter().copied().collect();
        while let Some(d) = stack.pop() {
            if d == txn || !visited.insert(d) {
                continue;
            }
            match self.txns.get(&d) {
                // Never heard of this dependency, or it has not committed: its
                // final position is unknown and might precede `txn`. Block.
                None => return false,
                Some(dt) if dt.phase < Phase::Committed => return false,
                Some(dt) => {
                    // Committed: only a dep ordering *before* `txn` constrains it.
                    if (dt.execute_at, d) < here {
                        if dt.phase != Phase::Applied {
                            return false; // earlier-ordered predecessor not applied
                        }
                        // Recurse: its own (transitive) earlier predecessors must
                        // have applied too. A dep ordering after `txn` is skipped,
                        // so cycles are broken by the total order and the walk ends.
                        stack.extend(dt.deps.iter().copied());
                    }
                }
            }
        }
        true
    }

    /// Apply `txn`'s effect in agreed order: append it to the execution order,
    /// mark it applied, record a durable `Applied`, and emit the effect for the
    /// driver. A **write** emits an [`ApplyEffect`] (write its id to each key); a
    /// **read-only** transaction emits a [`ReadEffect`] (snapshot-read each key as
    /// of its execution timestamp) and writes nothing. Both are ordered the same
    /// way — only the effect differs — so a read observes exactly the writes that
    /// executed before it (lower `execute_at`) and none after.
    fn apply(&mut self, txn: TxnId) {
        let (keys, write_keys, write_values, execute_at, read_only) = match self.txns.get_mut(&txn)
        {
            Some(t) if t.phase == Phase::Committed => {
                t.phase = Phase::Applied;
                (
                    t.keys.clone(),
                    t.write_keys.clone(),
                    t.write_values.clone(),
                    t.execute_at,
                    t.read_only,
                )
            }
            _ => return,
        };
        if read_only {
            // A pure read snapshots its full (read) key set.
            self.pending_read.push(ReadEffect {
                txn,
                keys,
                version: execute_at,
            });
        } else {
            // A write (incl. read-modify-write) writes only its `write_keys`;
            // any extra read-only keys ordered it but produce no write effect.
            // The caller-supplied values ride along (a key absent from
            // `write_values` defaults at the driver to the txn id).
            self.pending_apply.push(ApplyEffect {
                txn,
                keys: write_keys,
                values: write_values,
                version: execute_at,
            });
        }
        self.applied_order.push(txn);
        self.pending.push(WalRecord::Applied { txn });
    }

    // ---- coordinator handlers -------------------------------------------

    fn coordinator_record(
        &mut self,
        from: NodeId,
        txn: TxnId,
        ts: Timestamp,
        deps: BTreeSet<TxnId>,
    ) {
        if let Some(c) = self.coordinating.get_mut(&txn)
            && c.phase == CoordPhase::PreAccept
        {
            c.replies.insert(from, (ts, deps));
        }
    }

    fn coordinator_record_accept(&mut self, from: NodeId, txn: TxnId) {
        if let Some(c) = self.coordinating.get_mut(&txn)
            && c.phase == CoordPhase::Accept
        {
            // Accept replies carry no new data; record presence with the
            // coordinator's own chosen values (already in `replies` keyed by
            // self). We only need the *count*, so insert a placeholder.
            c.replies
                .entry(from)
                .or_insert_with(|| (Timestamp::ZERO, BTreeSet::new()));
        }
    }

    /// Drive the coordinator for `txn` forward if a quorum is in. Returns the
    /// outbound messages for the next phase, or `None` if still waiting. Records
    /// a [`Decision`] when committing.
    fn try_advance_coordinator(&mut self, txn: TxnId) -> Option<Vec<Out>> {
        let c = self.coordinating.get(&txn)?;
        match c.phase {
            CoordPhase::PreAccept => self.advance_from_pre_accept(txn),
            CoordPhase::Accept => self.advance_from_accept(txn),
            CoordPhase::Done => None,
        }
    }

    fn advance_from_pre_accept(&mut self, txn: TxnId) -> Option<Vec<Out>> {
        let (replies, t0, recovery) = {
            let c = self.coordinating.get(&txn)?;
            (c.replies.clone(), c.t0, c.recovery)
        };

        let fast_n = self.fast_quorum();
        let slow_n = self.slow_quorum();

        // Fast path: a fast quorum returned `t0` unchanged and identical deps.
        // A *recovery* coordinator never takes the fast path (it cannot prove the
        // original fast quorum existed); it always escalates to Accept once a
        // simple majority has replied. See the recovery rules in the module docs.
        let agree_t0: Vec<&(Timestamp, BTreeSet<TxnId>)> =
            replies.values().filter(|(ts, _)| *ts == t0).collect();
        if !recovery && agree_t0.len() >= fast_n {
            // All fast-quorum deps must match for the fast path. Union them; if
            // they are all equal the union equals each, so check equality.
            let first = &agree_t0[0].1;
            let identical = agree_t0.iter().all(|(_, d)| d == first);
            if identical {
                let deps = first.clone();
                return Some(self.commit(txn, t0, deps, true));
            }
        }

        // Decide whether the fast path can still succeed. It needs a fast quorum
        // that *both* agrees on `t0` and reports identical deps. The most
        // agreement we could still reach is the current `t0`-agreers plus the
        // replicas that haven't answered yet — but if everyone has answered
        // (`outstanding == 0`) and the fast path did not fire above, then either
        // the timestamps or the deps disagree and the fast path is dead. We must
        // escalate in that case too, otherwise an all-agree-on-`t0`-but-deps-
        // differ quorum would stall forever.
        let outstanding = self.cluster_size.saturating_sub(replies.len());
        let fast_still_possible =
            !recovery && outstanding > 0 && agree_t0.len() + outstanding >= fast_n;

        // Slow path: once a simple majority has replied AND the fast path can no
        // longer succeed, pick the highest returned timestamp and union all
        // returned deps, then run Accept.
        if replies.len() >= slow_n && !fast_still_possible {
            let execute_at = replies
                .values()
                .map(|(ts, _)| *ts)
                .max()
                .unwrap_or(t0)
                .max(t0);
            let mut deps = BTreeSet::new();
            for (_, d) in replies.values() {
                deps.extend(d.iter().copied());
            }
            // The ballot this Accept round runs under: `Ballot::ZERO` for the
            // original coordinator, the adopted recovery ballot otherwise.
            let ballot = self
                .coordinating
                .get(&txn)
                .map_or(Ballot::ZERO, |c| c.ballot);
            // Move to Accept phase: reset replies, apply to ourselves, broadcast.
            // Our own Accept cannot be superseded by us (our ballot is our floor),
            // so this never self-Nacks.
            let _ = self.replica_accept(txn, ballot, execute_at, &deps);
            let mut self_replies = BTreeMap::new();
            self_replies.insert(self.id, (execute_at, deps.clone()));
            if let Some(c) = self.coordinating.get_mut(&txn) {
                c.phase = CoordPhase::Accept;
                c.replies = self_replies;
                c.chosen = Some((execute_at, deps.clone()));
                // Past PreAccept now: the key set is no longer needed for retry.
                c.keys = BTreeSet::new();
            }
            let mut outs: Vec<Out> = self
                .peers
                .iter()
                .map(|&p| {
                    (
                        p,
                        AccordMsg::Accept {
                            txn,
                            ballot,
                            execute_at,
                            deps: deps.clone(),
                        },
                    )
                })
                .collect();
            // Single-replica clusters reach the Accept quorum immediately.
            if let Some(extra) = self.advance_from_accept(txn) {
                outs.extend(extra);
            }
            return Some(outs);
        }

        None
    }

    fn advance_from_accept(&mut self, txn: TxnId) -> Option<Vec<Out>> {
        let (count, execute_at, deps) = {
            let c = self.coordinating.get(&txn)?;
            if c.phase != CoordPhase::Accept {
                return None;
            }
            // The execute_at/deps are what we applied to ourselves and broadcast;
            // recover them from our own reply entry.
            let (ts, deps) = c.replies.get(&self.id).cloned()?;
            (c.replies.len(), ts, deps)
        };
        if count >= self.slow_quorum() {
            return Some(self.commit(txn, execute_at, deps, false));
        }
        None
    }

    /// Finalize: record the decision, apply Commit to our own replica, and
    /// broadcast Commit to peers.
    fn commit(
        &mut self,
        txn: TxnId,
        execute_at: Timestamp,
        deps: BTreeSet<TxnId>,
        fast_path: bool,
    ) -> Vec<Out> {
        let (read_only, write_keys, write_values, ballot) = self.coordinating.get(&txn).map_or(
            (false, BTreeSet::new(), BTreeMap::new(), Ballot::ZERO),
            |c| {
                (
                    c.read_only,
                    c.write_keys.clone(),
                    c.write_values.clone(),
                    // The ballot the decision is committed under: `Ballot::ZERO`
                    // for the original coordinator, the recovery ballot otherwise.
                    // It fences a stale lower-ballot `Commit` at every replica.
                    c.ballot,
                )
            },
        );
        if let Some(c) = self.coordinating.get_mut(&txn) {
            if c.phase == CoordPhase::Done {
                return Vec::new();
            }
            c.phase = CoordPhase::Done;
            // Record the committed values so a retry tick can re-send `Commit`
            // to peers that have not yet acknowledged it.
            c.chosen = Some((execute_at, deps.clone()));
        }
        self.replica_commit(
            txn,
            ballot,
            execute_at,
            deps.clone(),
            write_keys.clone(),
            write_values.clone(),
            read_only,
        );
        self.decisions.push(Decision {
            txn,
            execute_at,
            fast_path,
        });
        self.peers
            .iter()
            .map(|&p| {
                (
                    p,
                    AccordMsg::Commit {
                        txn,
                        ballot,
                        execute_at,
                        deps: deps.clone(),
                        write_keys: write_keys.clone(),
                        write_values: write_values.clone(),
                        read_only,
                    },
                )
            })
            .collect()
    }

    // ---- coordinator failover / recovery --------------------------------

    /// Take over a transaction whose original coordinator is suspected dead.
    ///
    /// This node becomes a *recovery coordinator* for `txn`: it broadcasts
    /// `Recover` to its peers and seeds the recovery reply set with its own
    /// recorded state (it is a replica too). When a simple-majority recovery
    /// quorum of `RecoverOk`s is in, [`AccordCore::try_advance_recovery`] decides
    /// the outcome (see the module docs for the rules).
    ///
    /// Idempotent-ish: if this node already committed `txn`, recovery is a no-op
    /// (it re-broadcasts the recovery query, which is harmless). Recovery of a
    /// transaction this node already coordinates is rejected (returns no
    /// outbound) — recover from a *different* replica.
    pub fn recover(&mut self, txn: TxnId) -> Vec<Out> {
        if self.coordinating.contains_key(&txn) {
            // We are (or were) the original coordinator; nothing to recover.
            return Vec::new();
        }
        // Mint a recovery ballot strictly above the highest this node has already
        // promised for `txn` (ADR 0011, duelling recoverers): a first recovery is
        // `round = 1` (above the original coordinator's [`Ballot::ZERO`]); a
        // *re*-recovery after being superseded (a `RecoverNack`/`AcceptNack` raised
        // our promised floor) bumps strictly past whatever ballot fenced us, so the
        // retry can actually win. Two recoverers thus never share a ballot.
        let ballot = Ballot::next_above(self.highest_promised(txn), self.id);
        self.start_recovery_at(txn, ballot)
    }

    /// Begin (or re-begin, at a higher ballot) recovery of `txn` under `ballot`.
    /// Promises the ballot locally, seeds the recovery reply set with this node's
    /// own facts, and broadcasts `Recover` at `ballot`. Shared by [`Self::recover`]
    /// (initial) and the supersede-and-retry path (a `RecoverNack`/`AcceptNack`).
    fn start_recovery_at(&mut self, txn: TxnId, ballot: Ballot) -> Vec<Out> {
        // We are the recoverer, so we promise our own ballot and report our facts.
        // Our ballot is `>= ` our promise floor by construction, so this never
        // self-Nacks.
        let f = match self.replica_recover(txn, ballot) {
            BallotReply::Promised(f) => f,
            // Unreachable in practice (our ballot supersedes our own promise), but
            // stay total: if we somehow promised higher, do not recover lower.
            BallotReply::Nack(_) => return Vec::new(),
        };
        let mut replies = BTreeMap::new();
        replies.insert(
            self.id,
            RecoverReply {
                phase: f.phase,
                execute_at: f.execute_at,
                deps: f.deps,
                accepted_ballot: f.accepted_ballot,
                keys: f.keys,
                write_keys: f.write_keys,
                write_values: f.write_values,
            },
        );
        self.recovering.insert(txn, Recovering { ballot, replies });

        let outs: Vec<Out> = self
            .peers
            .iter()
            .map(|&p| (p, AccordMsg::Recover { txn, ballot }))
            .collect();
        let mut all = outs;
        if let Some(extra) = self.try_advance_recovery(txn) {
            all.extend(extra);
        }
        all
    }

    /// The highest recovery ballot this node has promised for `txn` across its
    /// roles: the replica promise, plus any in-flight recovery this node is
    /// driving. [`Ballot::ZERO`] if it has never promised one. A re-recovery mints
    /// strictly above this so it supersedes every ballot it has itself seen.
    fn highest_promised(&self, txn: TxnId) -> Ballot {
        let replica = self.txns.get(&txn).map_or(Ballot::ZERO, |t| t.promised);
        let recovering = self.recovering.get(&txn).map_or(Ballot::ZERO, |r| r.ballot);
        replica.max(recovering)
    }

    /// Replica side of `Recover`: **promise** `ballot` (rejecting it if the replica
    /// has already promised a strictly higher one) and, on a promise, report this
    /// replica's recorded state for `txn`. Promising fences any later
    /// `Recover`/`Accept` below `ballot` — including the original coordinator's
    /// [`Ballot::ZERO`] `Accept` — so a superseded coordinator cannot still commit
    /// behind the winning recoverer's back (ADR 0011, duelling recoverers).
    ///
    /// If the replica had never heard of `txn`, it witnesses it now as a fresh
    /// `PreAccepted` entry (keys unknown, `execute_at == t0`, no deps) so it joins
    /// the recovery — and records it durably like a `PreAccept` would. The promise
    /// itself is durable (a `WalRecord::Promised`) so a restarted replica does not
    /// renege and let a superseded recoverer win.
    fn replica_recover(&mut self, txn: TxnId, ballot: Ballot) -> BallotReply {
        self.clock.witness(txn);
        // Reject a stale ballot: report the strictly-higher ballot we promised so
        // the superseded recoverer can retry above it (or give up).
        if let Some(t) = self.txns.get(&txn)
            && ballot < t.promised
        {
            return BallotReply::Nack(t.promised);
        }
        // Never seen: witness as PreAccepted at t0 with no keys/deps known, and
        // record it durably like a `PreAccept` would.
        let mut newly_witnessed = false;
        self.txns.entry(txn).or_insert_with(|| {
            newly_witnessed = true;
            ReplicaTxn {
                keys: BTreeSet::new(),
                write_keys: BTreeSet::new(),
                write_values: BTreeMap::new(),
                execute_at: txn,
                deps: BTreeSet::new(),
                phase: Phase::PreAccepted,
                read_only: false,
                promised: Ballot::ZERO,
                accepted_ballot: Ballot::ZERO,
                commit_ballot: Ballot::ZERO,
            }
        });
        if newly_witnessed {
            self.pending.push(WalRecord::PreAccepted {
                txn,
                keys: BTreeSet::new(),
                write_keys: BTreeSet::new(),
                write_values: BTreeMap::new(),
                execute_at: txn,
                deps: BTreeSet::new(),
                read_only: false,
            });
        }
        let entry = self.txns.get_mut(&txn).expect("just inserted");
        // Promise the ballot durably (it is `>=` our floor). A duplicate `Recover`
        // at the same ballot re-promises harmlessly (idempotent under `max`).
        if ballot > entry.promised {
            entry.promised = ballot;
            self.pending.push(WalRecord::Promised { txn, ballot });
        }
        BallotReply::Promised(RecoverFacts {
            phase: entry.phase,
            execute_at: entry.execute_at,
            deps: entry.deps.clone(),
            accepted_ballot: entry.accepted_ballot,
            keys: entry.keys.clone(),
            write_keys: entry.write_keys.clone(),
            write_values: entry.write_values.clone(),
            read_only: entry.read_only,
        })
    }

    fn recovery_record(&mut self, from: NodeId, txn: TxnId, reply: RecoverReply) {
        self.clock.witness(reply.execute_at);
        if let Some(rec) = self.recovering.get_mut(&txn) {
            rec.replies.insert(from, reply);
        }
    }

    /// Drive recovery for `txn` forward once a simple-majority recovery quorum of
    /// `RecoverOk`s is in (all having **promised** this round's ballot). Applies
    /// the recovery rules (ADR 0011):
    ///
    /// - If any replica reports `Committed`/`Applied`, **adopt that decision
    ///   verbatim** and broadcast `Commit` — a committed value is already
    ///   immutable, so the recovered decision must match it.
    /// - Else if any reply was **`Accept`ed** under some ballot (`accepted_ballot >
    ///   Ballot::ZERO`), adopt the `(execute_at, deps)` of the reply with the
    ///   **highest `accepted_ballot`** — the most recent prior proposal, which may
    ///   already have been committed by that recoverer, so a later recoverer must
    ///   re-propose it rather than invent a fresh timestamp. Two recoverers thus
    ///   converge on the same value.
    /// - Otherwise (only `PreAccepted` replies) force the **slow path** from the
    ///   recovery replies (`recovery = true`): pick the highest reported
    ///   `execute_at`, union the deps, and run `Accept` → `Commit`. Never the fast
    ///   path. The `Accept` carries this round's recovery ballot, so a replica
    ///   that promised a higher one fences it (an `AcceptNack` makes us retry).
    fn try_advance_recovery(&mut self, txn: TxnId) -> Option<Vec<Out>> {
        let (ballot, replies) = {
            let rec = self.recovering.get(&txn)?;
            (rec.ballot, rec.replies.clone())
        };
        if replies.len() < self.slow_quorum() {
            return None;
        }

        // Recovery decided: drop the recovering state so we don't re-fire.
        self.recovering.remove(&txn);

        // Recover the transaction's full conflict set and its write subset as the
        // union over every reply that knew of it; a transaction is read-only iff
        // it writes nothing (consistent with `write_keys`).
        let union_keys: BTreeSet<Key> = replies
            .values()
            .flat_map(|r| r.keys.iter().copied())
            .collect();
        let union_write_keys: BTreeSet<Key> = replies
            .values()
            .flat_map(|r| r.write_keys.iter().copied())
            .collect();
        // Union the caller-supplied write values across the quorum so a recovered
        // transaction executes the same bytes the original would have (a replica
        // that missed the original PreAccept contributes nothing here).
        let mut union_write_values: BTreeMap<Key, Vec<u8>> = BTreeMap::new();
        for r in replies.values() {
            for (k, v) in &r.write_values {
                union_write_values.insert(*k, v.clone());
            }
        }
        let read_only = union_write_keys.is_empty();

        // (1) Adopt an already-committed decision if any replica has one.
        if let Some(r) = replies
            .values()
            .filter(|r| r.phase >= Phase::Committed)
            .max_by_key(|r| r.execute_at)
        {
            let execute_at = r.execute_at;
            let deps = r.deps.clone();
            return Some(self.commit_recovered(
                txn,
                ballot,
                execute_at,
                deps,
                union_write_keys,
                union_write_values,
                read_only,
            ));
        }

        // (2) Adopt the highest-ballot **accepted** proposal verbatim, if any. A
        // prior recoverer that ran an `Accept` under `accepted_ballot` may have
        // gone on to `Commit` that exact `(execute_at, deps)` on a quorum we don't
        // intersect; re-proposing it (rather than a fresh max-ts/union-deps value)
        // is the only choice that cannot contradict such a commit. With ballots
        // totally ordered, every recoverer that reaches step (2) adopts the *same*
        // highest-ballot proposal, so duelling recoverers converge. We re-run it
        // through an `Accept` round under *our* (strictly higher) ballot so the
        // decision is freshly re-accepted on a quorum that has promised us.
        if let Some(r) = replies
            .values()
            .filter(|r| r.accepted_ballot > Ballot::ZERO)
            .max_by_key(|r| r.accepted_ballot)
        {
            let execute_at = r.execute_at;
            let deps = r.deps.clone();
            return Some(self.redrive_accept(
                txn,
                ballot,
                execute_at,
                deps,
                union_keys,
                union_write_keys,
                union_write_values,
                read_only,
            ));
        }

        // (3) Re-drive the transaction as a fresh, recovery-flagged coordinator.
        // The original `PreAccept` may not have reached every replica (that is
        // why recovery was needed), so the keys could be missing on some — and a
        // replica with no keys would execute an empty write. Re-broadcast
        // `PreAccept` carrying the **union of keys** any RecoverOk reported (and
        // the union of write keys), so every replica (re)witnesses the transaction
        // with its keys before we commit. Then, being a recovery coordinator, we
        // never take the fast path (`advance_from_pre_accept` forces Accept →
        // Commit).
        let (ts, deps) = self.replica_pre_accept(
            txn,
            &union_keys,
            &union_write_keys,
            &union_write_values,
            read_only,
        );
        let mut coord_replies: BTreeMap<NodeId, (Timestamp, BTreeSet<TxnId>)> = BTreeMap::new();
        coord_replies.insert(self.id, (ts, deps));
        self.coordinating.insert(
            txn,
            Coordinating {
                t0: txn,
                replies: coord_replies,
                phase: CoordPhase::PreAccept,
                recovery: true,
                read_only,
                keys: union_keys.clone(),
                write_keys: union_write_keys.clone(),
                write_values: union_write_values.clone(),
                chosen: None,
                commit_acks: BTreeSet::new(),
                // This recovery round's ballot rides the slow-path `Accept` so a
                // replica that promised a higher recoverer fences us.
                ballot,
            },
        );
        let mut outs: Vec<Out> = self
            .peers
            .iter()
            .map(|&p| {
                (
                    p,
                    AccordMsg::PreAccept {
                        txn,
                        keys: union_keys.clone(),
                        write_keys: union_write_keys.clone(),
                        write_values: union_write_values.clone(),
                        read_only,
                    },
                )
            })
            .collect();
        if let Some(extra) = self.try_advance_coordinator(txn) {
            outs.extend(extra);
        }
        Some(outs)
    }

    /// Re-propose a recovered `(execute_at, deps)` — adopted verbatim from the
    /// highest-ballot prior `Accept` (step (2) of [`Self::try_advance_recovery`]) —
    /// through a fresh `Accept` round under this recovery's `ballot`. Installs a
    /// recovery `Coordinating` already in the `Accept` phase with the adopted value
    /// `chosen`, applies the `Accept` to ourselves, and broadcasts it; a replica
    /// that promised a higher ballot `AcceptNack`s us (→ retry higher).
    #[allow(clippy::too_many_arguments)]
    fn redrive_accept(
        &mut self,
        txn: TxnId,
        ballot: Ballot,
        execute_at: Timestamp,
        deps: BTreeSet<TxnId>,
        union_keys: BTreeSet<Key>,
        union_write_keys: BTreeSet<Key>,
        union_write_values: BTreeMap<Key, Vec<u8>>,
        read_only: bool,
    ) -> Vec<Out> {
        // Make sure our own replica knows the keys (so it can execute the write)
        // before adopting the proposal: a recoverer may have missed the original
        // PreAccept. This folds the union keys/values into our replica entry.
        let _ = self.replica_pre_accept(
            txn,
            &union_keys,
            &union_write_keys,
            &union_write_values,
            read_only,
        );
        // Apply the adopted Accept to ourselves under our ballot (cannot self-Nack).
        let _ = self.replica_accept(txn, ballot, execute_at, &deps);
        let mut self_replies: BTreeMap<NodeId, (Timestamp, BTreeSet<TxnId>)> = BTreeMap::new();
        self_replies.insert(self.id, (execute_at, deps.clone()));
        self.coordinating.insert(
            txn,
            Coordinating {
                t0: txn,
                replies: self_replies,
                phase: CoordPhase::Accept,
                recovery: true,
                read_only,
                keys: BTreeSet::new(),
                write_keys: union_write_keys.clone(),
                write_values: union_write_values.clone(),
                chosen: Some((execute_at, deps.clone())),
                commit_acks: BTreeSet::new(),
                ballot,
            },
        );
        let mut outs: Vec<Out> = self
            .peers
            .iter()
            .map(|&p| {
                (
                    p,
                    AccordMsg::Accept {
                        txn,
                        ballot,
                        execute_at,
                        deps: deps.clone(),
                    },
                )
            })
            .collect();
        // A single-replica cluster reaches the Accept quorum immediately.
        if let Some(extra) = self.advance_from_accept(txn) {
            outs.extend(extra);
        }
        outs
    }

    /// Commit a transaction the recovery quorum found already committed: install
    /// it as a normal coordinator-driven `Coordinating` so `commit` records the
    /// decision and broadcasts, then commit with the adopted `(execute_at, deps)`.
    #[allow(clippy::too_many_arguments)]
    fn commit_recovered(
        &mut self,
        txn: TxnId,
        ballot: Ballot,
        execute_at: Timestamp,
        deps: BTreeSet<TxnId>,
        write_keys: BTreeSet<Key>,
        write_values: BTreeMap<Key, Vec<u8>>,
        read_only: bool,
    ) -> Vec<Out> {
        self.coordinating.insert(
            txn,
            Coordinating {
                t0: txn,
                replies: BTreeMap::new(),
                phase: CoordPhase::PreAccept,
                recovery: true,
                read_only,
                keys: write_keys.clone(),
                write_keys,
                write_values,
                chosen: None,
                commit_acks: BTreeSet::new(),
                ballot,
            },
        );
        // A recovered commit is, by Accord's recovery rules, equivalent to a
        // slow-path commit (we never assert the fast path on recovery).
        self.commit(txn, execute_at, deps, false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Hand a replica a `Commit` for `txn` at `execute_at` writing `write_keys`,
    /// with the given transitive `deps`. Mirrors what the wire delivers — the core
    /// records the decision and tries to execute.
    fn commit(
        core: &mut AccordCore,
        from: NodeId,
        txn: TxnId,
        execute_at: Timestamp,
        deps: &[TxnId],
        write_keys: &[Key],
    ) {
        core.handle(
            from,
            AccordMsg::Commit {
                txn,
                ballot: Ballot::ZERO,
                execute_at,
                deps: deps.iter().copied().collect(),
                write_keys: write_keys.iter().copied().collect(),
                write_values: BTreeMap::new(),
                read_only: false,
            },
        );
    }

    /// The **transitive dependency wait-graph** (ADR 0011): a committed
    /// transaction must not execute before a dependency that orders before it —
    /// **even one this replica knows only as an id, on a disjoint key set** (so the
    /// direct key-conflict gate cannot see it). The direct-only gate would execute
    /// `t` immediately and mis-order it ahead of `d`; the dependency-closure gate
    /// makes it wait.
    #[test]
    fn transitive_dep_blocks_execution_until_predecessor_applies() {
        // Replica id 2 in a 3-node cluster. We feed it Commits directly.
        let mut core = AccordCore::new(nid(2), &[nid(0), nid(1), nid(2)]);

        // `d` writes key {a}, orders *before* `t`. `t` writes key {b} (disjoint
        // from `d`), and was committed carrying `d` as a dependency — but this
        // replica never saw `d`'s PreAccept, so it knows `d` only as an id and
        // shares no key with it. A direct-conflict-only gate is blind to `d`.
        let d = Timestamp::new(5, nid(0));
        let t = Timestamp::new(10, nid(1));

        // Commit `t` first, with deps = {d}. The direct gate is clear (nothing
        // shares key b); the dependency gate must block on the unknown `d`.
        commit(&mut core, nid(1), t, t, &[d], &[20]);
        assert!(
            !core.is_applied(t),
            "t executed before its transitive dependency d was even known — \
             the dependency-closure gate failed (direct-only mis-order)"
        );
        assert!(core.applied_order().is_empty(), "nothing should apply yet");

        // Now `d` commits, ordering before `t`. It applies first, then `t` becomes
        // applicable and applies — the agreed order [d, t].
        commit(&mut core, nid(0), d, d, &[], &[10]);
        assert!(core.is_applied(d), "d must apply (no predecessors)");
        assert!(core.is_applied(t), "t must apply once d has");
        assert_eq!(
            core.applied_order(),
            &[d, t],
            "execution order must respect the transitive dependency d -> t"
        );
    }

    /// The closure gate must **recurse**: `t`'s dep `m` is committed and orders
    /// before `t`, and `m` in turn depends on `d` (a deeper, disjoint predecessor
    /// `t` never directly saw). `t` must wait for the *whole chain* d -> m -> t.
    #[test]
    fn transitive_closure_waits_for_indirect_predecessor() {
        let mut core = AccordCore::new(nid(2), &[nid(0), nid(1), nid(2)]);
        let d = Timestamp::new(3, nid(0)); // writes {a}
        let m = Timestamp::new(6, nid(1)); // writes {a, b} — bridges d and t
        let t = Timestamp::new(9, nid(2)); // writes {b}; dep set {m}

        // Commit t (dep m) then m (dep d): m orders before t, d before m. t and d
        // are disjoint; only the recursive closure links them.
        commit(&mut core, nid(1), t, t, &[m], &[200]);
        commit(&mut core, nid(1), m, m, &[d], &[100, 200]);
        // m's predecessor d is still unknown → neither m nor t may apply.
        assert!(
            !core.is_applied(m) && !core.is_applied(t),
            "m (and thus t) must block until the indirect predecessor d applies"
        );

        commit(&mut core, nid(0), d, d, &[], &[100]);
        assert_eq!(
            core.applied_order(),
            &[d, m, t],
            "the full transitive chain d -> m -> t must be respected"
        );
    }

    /// The **precise fast-path quorum** (ADR 0011): the simplified-recovery bound
    /// is all-but-the-failure-tolerance replicas — `N − 1` for the common
    /// `N = 2f+1` — replacing the old conservative `⌈3N/4⌉`. Check the exact value
    /// for a range of cluster sizes, and that it never drops below the slow quorum.
    #[test]
    fn fast_quorum_is_the_precise_bound() {
        // (N, expected fast quorum). For N=2f+1 the precise simplified bound is
        // N-1; for N<=2 (no fault tolerance) it is the whole cluster.
        let cases = [
            (1usize, 1usize), // f=0: unanimity
            (2, 2),           // f=0: unanimity
            (3, 2),           // f=1: N-1 = 2  (was ceil(9/4)=3)
            (4, 3),           // f=1: N-1 = 3
            (5, 4),           // f=2: N-1 = 4
            (6, 5),           // f=2
            (7, 6),           // f=3: N-1 = 6
        ];
        for (n, expected) in cases {
            let all: Vec<NodeId> = (0..n as u64).map(nid).collect();
            let core = AccordCore::new(nid(0), &all);
            assert_eq!(
                core.fast_quorum(),
                expected,
                "fast quorum for N={n} should be {expected}"
            );
            assert!(
                core.fast_quorum() >= core.slow_quorum(),
                "fast quorum must never be below the slow quorum (N={n})"
            );
        }
    }

    /// A fast-path decision must be **recoverable under f failures**: every
    /// recovery quorum (a simple majority) intersects every fast quorum in **at
    /// least one** replica, so a recovery coordinator always hears from a fast-path
    /// witness and cannot miss that a fast commit was possible. Also: two fast
    /// quorums always intersect, so concurrent fast attempts cannot decide
    /// differently. Verified over the full quorum arithmetic for many N.
    #[test]
    fn fast_path_decision_is_recoverable_under_f_failures() {
        for n in 1usize..=15 {
            let all: Vec<NodeId> = (0..n as u64).map(nid).collect();
            let core = AccordCore::new(nid(0), &all);
            let fast = core.fast_quorum();
            let slow = core.slow_quorum(); // = recovery quorum size
            let f = core.failure_tolerance();

            // The recovery quorum stays available under f failures.
            assert_eq!(
                slow,
                n - f,
                "recovery quorum must survive f failures (N={n})"
            );

            // Fast ∩ recovery >= 1: any two sets of these sizes drawn from N
            // overlap in at least (fast + slow - N) replicas; that must be >= 1.
            let fast_recovery_overlap = (fast + slow).saturating_sub(n);
            assert!(
                fast_recovery_overlap >= 1,
                "fast({fast}) ∩ recovery({slow}) must be >= 1 for N={n}, got \
                 {fast_recovery_overlap} — a fast decision could be unrecoverable"
            );

            // Fast ∩ fast >= 1: concurrent fast attempts share a witness, so they
            // cannot fast-commit different values.
            let fast_fast_overlap = (2 * fast).saturating_sub(n);
            assert!(
                fast_fast_overlap >= 1,
                "fast({fast}) ∩ fast({fast}) must be >= 1 for N={n}, got \
                 {fast_fast_overlap} — two fast quorums could diverge"
            );
        }
    }

    /// **WAL snapshotting / log truncation** (ADR 0011): the compact `wal_image`
    /// (a single `Snapshot` record) replays to a core **identical** to the one that
    /// produced it. Drives a replica to several applied transactions, snapshots,
    /// then rebuilds from `wal_image` alone and asserts the recovered state matches.
    #[test]
    fn snapshot_image_replays_to_identical_state() {
        let mut core = AccordCore::new(nid(2), &[nid(0), nid(1), nid(2)]);
        // A few committed, executed transactions plus one still in-flight, so the
        // snapshot must capture both applied (terminal) and uncommitted state.
        let d = Timestamp::new(2, nid(0));
        let m = Timestamp::new(5, nid(1));
        let t = Timestamp::new(9, nid(0));
        commit(&mut core, nid(0), d, d, &[], &[10]);
        commit(&mut core, nid(1), m, m, &[d], &[10, 20]);
        commit(&mut core, nid(0), t, t, &[m], &[20]);
        // An uncommitted (PreAccepted-only) transaction this replica has witnessed.
        let pending = Timestamp::new(12, nid(1));
        core.handle(
            nid(1),
            AccordMsg::PreAccept {
                txn: pending,
                keys: [30].into_iter().collect(),
                write_keys: [30].into_iter().collect(),
                write_values: BTreeMap::new(),
                read_only: false,
            },
        );

        let before_order = core.applied_order().to_vec();
        assert_eq!(before_order, vec![d, m, t], "all three applied in order");

        // Snapshot + compact image, then rebuild a fresh core from the image alone.
        core.snapshot();
        assert!(
            core.applied_since_snapshot() == 0,
            "snapshot covers all applies"
        );
        let image = core.wal_image();
        assert_eq!(
            image.len(),
            1,
            "the compact image is a single Snapshot record"
        );
        let state = PersistedState::replay(image);
        let recovered = AccordCore::recovered(nid(2), &[nid(0), nid(1), nid(2)], state);

        // The recovered core matches: same execution order, same committed
        // decisions, same phase for the in-flight transaction.
        assert_eq!(
            recovered.applied_order(),
            before_order.as_slice(),
            "recovered execution order diverged from the snapshot"
        );
        for txn in [d, m, t] {
            assert!(
                recovered.is_applied(txn),
                "recovered {txn:?} lost applied state"
            );
            assert_eq!(
                recovered.committed_execute_at(txn),
                core.committed_execute_at(txn),
                "recovered {txn:?} execute_at diverged"
            );
            assert_eq!(
                recovered.committed_deps(txn),
                core.committed_deps(txn),
                "recovered {txn:?} deps diverged"
            );
        }
        assert_eq!(
            recovered.phase(pending),
            Some(Phase::PreAccepted),
            "the in-flight transaction must survive the snapshot un-committed"
        );
        // The two persisted images are byte-for-byte identical (idempotent).
        assert_eq!(
            recovered.persisted_state(),
            core.persisted_state(),
            "re-snapshotting the recovered core must reproduce the same image"
        );
    }

    /// `snapshot()` is a no-op when nothing new has applied — it must not flag a
    /// pointless WAL rewrite.
    #[test]
    fn snapshot_is_noop_without_new_applies() {
        let mut core = AccordCore::new(nid(2), &[nid(0), nid(1), nid(2)]);
        let a = Timestamp::new(3, nid(0));
        commit(&mut core, nid(0), a, a, &[], &[1]);
        core.snapshot();
        assert!(
            core.take_snapshot_dirty(),
            "first snapshot should flag a rewrite"
        );
        // Nothing new applied since: snapshot must not re-flag.
        core.snapshot();
        assert!(
            !core.take_snapshot_dirty(),
            "a snapshot with no new applies must be a no-op"
        );
        assert_eq!(core.applied_since_snapshot(), 0);
    }

    /// Cycle-awareness: mutual dependencies (`a` deps `b`, `b` deps `a`) must not
    /// deadlock — the total `(execute_at, txn)` order breaks the cycle, so the
    /// lower-ordered one applies first.
    #[test]
    fn dependency_cycle_is_broken_by_timestamp_order() {
        let mut core = AccordCore::new(nid(2), &[nid(0), nid(1), nid(2)]);
        let a = Timestamp::new(4, nid(0)); // writes {k}
        let b = Timestamp::new(7, nid(1)); // writes {k} — conflicts a, mutual deps

        // Both committed, each carrying the other as a dep (Accord deps can be
        // mutual). a orders before b; the cycle must not stall execution.
        commit(&mut core, nid(0), a, a, &[b], &[1]);
        commit(&mut core, nid(1), b, b, &[a], &[1]);
        assert_eq!(
            core.applied_order(),
            &[a, b],
            "a mutual-dependency cycle must drain in (execute_at, txn) order"
        );
    }
}
