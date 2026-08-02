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
use crate::persist::{PersistedState, WalRecord};
use crate::timestamp::{LogicalClock, Timestamp};

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
    /// What each responder recorded for the transaction being recovered.
    replies: BTreeMap<NodeId, RecoverReply>,
}

/// What a replica reports about a transaction when it answers a `Recover` (the
/// facts that become a `RecoverOk`). Factored into a struct to keep
/// [`AccordCore::replica_recover`]'s return type readable.
struct RecoverFacts {
    phase: Phase,
    execute_at: Timestamp,
    deps: BTreeSet<TxnId>,
    keys: BTreeSet<Key>,
    write_keys: BTreeSet<Key>,
    write_values: BTreeMap<Key, Vec<u8>>,
    read_only: bool,
}

/// One replica's recorded state for a transaction under recovery, as reported in
/// its `RecoverOk`.
#[derive(Clone, Debug)]
struct RecoverReply {
    phase: Phase,
    execute_at: Timestamp,
    deps: BTreeSet<TxnId>,
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

    /// Slow-path (simple-majority) quorum: a strict majority of replicas.
    fn slow_quorum(&self) -> usize {
        self.cluster_size / 2 + 1
    }

    /// Fast-path quorum. Accord's fast quorum is larger than a simple majority
    /// (`f + (f+1)/2`-ish); for this minimal slice we use a conservative
    /// `ceil(3N/4)`, which for the tested N=3 is 3 (all replicas) and for N=5 is
    /// 4. The point this slice proves is the *mechanism*, not the exact tight
    /// bound — see ADR 0011 for the deferred precise quorum.
    fn fast_quorum(&self) -> usize {
        (self.cluster_size * 3).div_ceil(4)
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
                    outs.push((p, AccordMsg::Recover { txn }));
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
                execute_at,
                deps,
            } => {
                self.replica_accept(txn, execute_at, &deps);
                vec![(from, AccordMsg::AcceptOk { txn })]
            }
            AccordMsg::AcceptOk { txn } => {
                self.coordinator_record_accept(from, txn);
                self.try_advance_coordinator(txn).unwrap_or_default()
            }
            AccordMsg::Commit {
                txn,
                execute_at,
                deps,
                write_keys,
                write_values,
                read_only,
            } => {
                self.replica_commit(txn, execute_at, deps, write_keys, write_values, read_only);
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
            AccordMsg::Recover { txn } => {
                let f = self.replica_recover(txn);
                vec![(
                    from,
                    AccordMsg::RecoverOk {
                        txn,
                        phase: f.phase,
                        execute_at: f.execute_at,
                        deps: f.deps,
                        keys: f.keys,
                        write_keys: f.write_keys,
                        write_values: f.write_values,
                        read_only: f.read_only,
                    },
                )]
            }
            AccordMsg::RecoverOk {
                txn,
                phase,
                execute_at,
                deps,
                keys,
                write_keys,
                write_values,
                read_only,
            } => {
                let _ = read_only; // read-only-ness is derived from write_keys
                self.recovery_record(
                    from,
                    txn,
                    RecoverReply {
                        phase,
                        execute_at,
                        deps,
                        keys,
                        write_keys,
                        write_values,
                    },
                );
                self.try_advance_recovery(txn).unwrap_or_default()
            }
        }
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

    /// Replica side of `Accept`: adopt the coordinator's chosen execution
    /// timestamp and dependency set (it is `>=` ours by construction).
    fn replica_accept(&mut self, txn: TxnId, execute_at: Timestamp, deps: &BTreeSet<TxnId>) {
        self.clock.witness(execute_at);
        let entry = self.txns.entry(txn).or_insert_with(|| ReplicaTxn {
            keys: BTreeSet::new(),
            write_keys: BTreeSet::new(),
            write_values: BTreeMap::new(),
            execute_at,
            deps: BTreeSet::new(),
            phase: Phase::Accepted,
            read_only: false,
        });
        entry.execute_at = entry.execute_at.max(execute_at);
        entry.deps.extend(deps.iter().copied());
        if entry.phase == Phase::PreAccepted {
            entry.phase = Phase::Accepted;
        }
        let record_ts = entry.execute_at;
        let record_deps = entry.deps.clone();
        self.pending.push(WalRecord::Accepted {
            txn,
            execute_at: record_ts,
            deps: record_deps,
        });
    }

    /// Replica side of `Commit`: record the final execution timestamp and deps —
    /// the durable agreement point — then try to execute any transactions that
    /// have become applicable.
    fn replica_commit(
        &mut self,
        txn: TxnId,
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
        });
        // The Commit carries the authoritative write set (so a replica that
        // missed PreAccept still executes the right write); the write keys are
        // also part of the conflict set, and the values it must execute.
        entry.keys.extend(write_keys.iter().copied());
        entry.write_keys.extend(write_keys.iter().copied());
        for (k, v) in &write_values {
            entry.write_values.insert(*k, v.clone());
        }
        // Read-only iff it writes nothing (consistent with `write_keys`).
        entry.read_only = (entry.read_only && read_only) && entry.write_keys.is_empty();
        // A txn already applied stays applied (idempotent under a duplicate
        // Commit); otherwise mark it committed and (re)record the final state.
        if entry.phase < Phase::Committed {
            entry.phase = Phase::Committed;
        }
        entry.execute_at = execute_at;
        entry.deps = deps.clone();
        if entry.phase == Phase::Committed {
            let keys = entry.keys.clone();
            let record_write_keys = entry.write_keys.clone();
            let record_write_values = entry.write_values.clone();
            let record_read_only = entry.read_only;
            self.pending.push(WalRecord::Committed {
                txn,
                keys,
                write_keys: record_write_keys,
                write_values: record_write_values,
                execute_at,
                deps,
                read_only: record_read_only,
            });
        }
        self.try_execute();
    }

    // ---- execution -------------------------------------------------------

    /// Execute every transaction that has become *applicable*, in agreed order.
    ///
    /// A committed transaction is applicable when every *conflicting* transaction
    /// that could order before it has already applied. Crucially, "could order
    /// before" is judged against **every conflicting transaction this replica
    /// knows of, in any phase** — not just the committed ones, and not only the
    /// recorded dependency set: a conflicting transaction that is not yet
    /// committed has an as-yet-unknown final timestamp that might land lower, so
    /// we must wait for it to commit (and apply, if it then orders earlier)
    /// before running. We then apply the applicable transaction with the
    /// smallest `(execute_at, txn)` and repeat. Because the order
    /// `(execute_at, txn)` is total and every replica converges to the same
    /// committed `(execute_at, deps)` for every transaction, all replicas
    /// execute conflicting transactions in the same order.
    fn try_execute(&mut self) {
        while let Some(txn) = self.next_applicable() {
            self.apply(txn);
        }
    }

    /// The applicable committed transaction with the smallest `(execute_at,
    /// txn)`, or `None` if none is ready.
    fn next_applicable(&self) -> Option<TxnId> {
        let mut best: Option<(Timestamp, TxnId)> = None;
        for (&txn, t) in &self.txns {
            if t.phase != Phase::Committed {
                continue; // not committed, or already applied
            }
            if !self.conflicts_clear_for(txn, t.execute_at, &t.keys) {
                continue;
            }
            let key = (t.execute_at, txn);
            if best.is_none_or(|b| key < b) {
                best = Some(key);
            }
        }
        best.map(|(_, txn)| txn)
    }

    /// Whether nothing this replica knows of could still need to execute before
    /// `txn`. For every other transaction whose key set intersects `txn`'s:
    ///
    /// - if it is not yet committed, its final timestamp is unknown and might
    ///   order before `txn` — block until it commits;
    /// - if it is committed and orders before `txn` (`(execute_at, other) <
    ///   (execute_at, txn)`) but has not applied, block until it does.
    ///
    /// A conflict that is committed and orders *after* `txn` does not block: it
    /// will run later.
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
        if let Some(c) = self.coordinating.get_mut(&txn) {
            if c.phase == CoordPhase::PreAccept {
                c.replies.insert(from, (ts, deps));
            }
        }
    }

    fn coordinator_record_accept(&mut self, from: NodeId, txn: TxnId) {
        if let Some(c) = self.coordinating.get_mut(&txn) {
            if c.phase == CoordPhase::Accept {
                // Accept replies carry no new data; record presence with the
                // coordinator's own chosen values (already in `replies` keyed by
                // self). We only need the *count*, so insert a placeholder.
                c.replies
                    .entry(from)
                    .or_insert_with(|| (Timestamp::ZERO, BTreeSet::new()));
            }
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
            // Move to Accept phase: reset replies, apply to ourselves, broadcast.
            self.replica_accept(txn, execute_at, &deps);
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
        let (read_only, write_keys, write_values) = self
            .coordinating
            .get(&txn)
            .map_or((false, BTreeSet::new(), BTreeMap::new()), |c| {
                (c.read_only, c.write_keys.clone(), c.write_values.clone())
            });
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
        let f = self.replica_recover(txn);
        let mut replies = BTreeMap::new();
        replies.insert(
            self.id,
            RecoverReply {
                phase: f.phase,
                execute_at: f.execute_at,
                deps: f.deps,
                keys: f.keys,
                write_keys: f.write_keys,
                write_values: f.write_values,
            },
        );
        self.recovering.insert(txn, Recovering { replies });

        let outs: Vec<Out> = self
            .peers
            .iter()
            .map(|&p| (p, AccordMsg::Recover { txn }))
            .collect();
        let mut all = outs;
        if let Some(extra) = self.try_advance_recovery(txn) {
            all.extend(extra);
        }
        all
    }

    /// Replica side of `Recover`: report this replica's recorded state for `txn`.
    /// If the replica had never heard of `txn`, it witnesses it now as a fresh
    /// `PreAccepted` entry (keys unknown, `execute_at == t0`, no deps) so it
    /// joins the recovery — and records it durably like a `PreAccept` would.
    fn replica_recover(&mut self, txn: TxnId) -> RecoverFacts {
        self.clock.witness(txn);
        if let Some(t) = self.txns.get(&txn) {
            return RecoverFacts {
                phase: t.phase,
                execute_at: t.execute_at,
                deps: t.deps.clone(),
                keys: t.keys.clone(),
                write_keys: t.write_keys.clone(),
                write_values: t.write_values.clone(),
                read_only: t.read_only,
            };
        }
        // Never seen: witness as PreAccepted at t0 with no keys/deps known.
        let entry = ReplicaTxn {
            keys: BTreeSet::new(),
            write_keys: BTreeSet::new(),
            write_values: BTreeMap::new(),
            execute_at: txn,
            deps: BTreeSet::new(),
            phase: Phase::PreAccepted,
            read_only: false,
        };
        self.txns.insert(txn, entry.clone());
        self.pending.push(WalRecord::PreAccepted {
            txn,
            keys: BTreeSet::new(),
            write_keys: BTreeSet::new(),
            write_values: BTreeMap::new(),
            execute_at: txn,
            deps: BTreeSet::new(),
            read_only: false,
        });
        RecoverFacts {
            phase: entry.phase,
            execute_at: entry.execute_at,
            deps: entry.deps,
            keys: entry.keys,
            write_keys: entry.write_keys,
            write_values: entry.write_values,
            read_only: entry.read_only,
        }
    }

    fn recovery_record(&mut self, from: NodeId, txn: TxnId, reply: RecoverReply) {
        self.clock.witness(reply.execute_at);
        if let Some(rec) = self.recovering.get_mut(&txn) {
            rec.replies.insert(from, reply);
        }
    }

    /// Drive recovery for `txn` forward once a simple-majority recovery quorum of
    /// `RecoverOk`s is in. Applies the recovery rules:
    ///
    /// - If any replica reports `Committed`/`Applied`, **adopt that decision
    ///   verbatim** and broadcast `Commit` — a committed value is already
    ///   immutable, so the recovered decision must match it.
    /// - Otherwise force the **slow path**: build a normal coordinator round from
    ///   the recovery replies (`recovery = true`), feed it into
    ///   `advance_from_pre_accept`, which picks the highest reported `execute_at`,
    ///   unions the deps, and runs `Accept` → `Commit`. Never the fast path.
    fn try_advance_recovery(&mut self, txn: TxnId) -> Option<Vec<Out>> {
        let replies = {
            let rec = self.recovering.get(&txn)?;
            rec.replies.clone()
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
                execute_at,
                deps,
                union_write_keys,
                union_write_values,
                read_only,
            ));
        }

        // (2) Re-drive the transaction as a fresh, recovery-flagged coordinator.
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

    /// Commit a transaction the recovery quorum found already committed: install
    /// it as a normal coordinator-driven `Coordinating` so `commit` records the
    /// decision and broadcasts, then commit with the adopted `(execute_at, deps)`.
    fn commit_recovered(
        &mut self,
        txn: TxnId,
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
            },
        );
        // A recovered commit is, by Accord's recovery rules, equivalent to a
        // slow-path commit (we never assert the fast path on recovery).
        self.commit(txn, execute_at, deps, false)
    }
}
