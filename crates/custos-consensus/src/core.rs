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
//! replayed [`crate::persist::PersistedState`] — mirroring `custos-control`'s
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

use custos_env::NodeId;

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
    keys: BTreeSet<Key>,
    /// Best-known execution timestamp: `t0` until `Accept`/`Commit` raise it.
    execute_at: Timestamp,
    deps: BTreeSet<TxnId>,
    phase: Phase,
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
    /// Per-responder `(phase, execute_at, deps, keys)`.
    replies: BTreeMap<NodeId, (Phase, Timestamp, BTreeSet<TxnId>, BTreeSet<Key>)>,
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
/// The effect is "write the transaction's id to each key it touches", stamped
/// with `version` (the transaction's `execute_at.logical`) as the MVCC version.
/// The driver applies it with per-key last-writer-wins (`merge`), which is
/// idempotent and commutative, so re-applying on recovery is harmless.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplyEffect {
    /// The transaction being executed.
    pub txn: TxnId,
    /// The keys it writes.
    pub keys: BTreeSet<Key>,
    /// The MVCC version to stamp each write with (its execution timestamp).
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
                    execute_at: p.execute_at,
                    deps: p.deps,
                    phase,
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
                core.pending_apply.push(ApplyEffect {
                    txn,
                    keys: t.keys.clone(),
                    version: t.execute_at,
                });
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

    // ---- coordinator entry point ----------------------------------------

    /// Begin coordinating a new transaction over `keys`. Mints a fresh `t0`,
    /// applies it locally as a replica too (the coordinator is one of the
    /// replicas), and returns the `PreAccept` broadcast to peers.
    ///
    /// Returns `(txn_id, outbound)`.
    pub fn submit(&mut self, keys: BTreeSet<Key>) -> (TxnId, Vec<Out>) {
        let t0 = self.clock.mint();

        // Apply to our own replica state and seed the coordinator's reply set
        // with our own PreAcceptOk (we are a replica of every txn in this slice).
        let (ts, deps) = self.replica_pre_accept(t0, &keys);

        let mut replies = BTreeMap::new();
        replies.insert(self.id, (ts, deps));
        self.coordinating.insert(
            t0,
            Coordinating {
                t0,
                replies,
                phase: CoordPhase::PreAccept,
                recovery: false,
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
            AccordMsg::PreAccept { txn, keys } => {
                let (ts, deps) = self.replica_pre_accept(txn, &keys);
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
            } => {
                self.replica_commit(txn, execute_at, deps);
                Vec::new()
            }
            AccordMsg::Recover { txn } => {
                let (phase, execute_at, deps, keys) = self.replica_recover(txn);
                vec![(
                    from,
                    AccordMsg::RecoverOk {
                        txn,
                        phase,
                        execute_at,
                        deps,
                        keys,
                    },
                )]
            }
            AccordMsg::RecoverOk {
                txn,
                phase,
                execute_at,
                deps,
                keys,
            } => {
                self.recovery_record(from, txn, phase, execute_at, deps, keys);
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
            execute_at: txn,
            deps: BTreeSet::new(),
            phase: Phase::PreAccepted,
        });
        entry.keys.extend(keys.iter().copied());
        if proposed > entry.execute_at {
            entry.execute_at = proposed;
        }
        entry.deps.extend(deps.iter().copied());
        let reply_deps = entry.deps.clone();
        let reply_ts = entry.execute_at;
        let record_keys = entry.keys.clone();
        // Durable before we reply: the coordinator's quorum counts a PreAcceptOk
        // only after it is on this replica's disk.
        self.pending.push(WalRecord::PreAccepted {
            txn,
            keys: record_keys,
            execute_at: reply_ts,
            deps: reply_deps.clone(),
        });
        (reply_ts, reply_deps)
    }

    /// Replica side of `Accept`: adopt the coordinator's chosen execution
    /// timestamp and dependency set (it is `>=` ours by construction).
    fn replica_accept(&mut self, txn: TxnId, execute_at: Timestamp, deps: &BTreeSet<TxnId>) {
        self.clock.witness(execute_at);
        let entry = self.txns.entry(txn).or_insert_with(|| ReplicaTxn {
            keys: BTreeSet::new(),
            execute_at,
            deps: BTreeSet::new(),
            phase: Phase::Accepted,
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
    fn replica_commit(&mut self, txn: TxnId, execute_at: Timestamp, deps: BTreeSet<TxnId>) {
        self.clock.witness(execute_at);
        let entry = self.txns.entry(txn).or_insert_with(|| ReplicaTxn {
            keys: BTreeSet::new(),
            execute_at,
            deps: BTreeSet::new(),
            phase: Phase::Committed,
        });
        // A txn already applied stays applied (idempotent under a duplicate
        // Commit); otherwise mark it committed and (re)record the final state.
        if entry.phase < Phase::Committed {
            entry.phase = Phase::Committed;
        }
        entry.execute_at = execute_at;
        entry.deps = deps.clone();
        if entry.phase == Phase::Committed {
            let keys = entry.keys.clone();
            self.pending.push(WalRecord::Committed {
                txn,
                keys,
                execute_at,
                deps,
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

    /// Apply `txn`'s effect: emit an [`ApplyEffect`] (the driver writes its id to
    /// each of its keys against the `StorageEngine`), append it to the execution
    /// order, mark it applied, and record a durable `Applied`.
    fn apply(&mut self, txn: TxnId) {
        let (keys, execute_at) = match self.txns.get_mut(&txn) {
            Some(t) if t.phase == Phase::Committed => {
                t.phase = Phase::Applied;
                (t.keys.clone(), t.execute_at)
            }
            _ => return,
        };
        self.pending_apply.push(ApplyEffect {
            txn,
            keys,
            version: execute_at,
        });
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
        if let Some(c) = self.coordinating.get_mut(&txn) {
            if c.phase == CoordPhase::Done {
                return Vec::new();
            }
            c.phase = CoordPhase::Done;
        }
        self.replica_commit(txn, execute_at, deps.clone());
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
        let (phase, execute_at, deps, keys) = self.replica_recover(txn);
        let mut replies = BTreeMap::new();
        replies.insert(self.id, (phase, execute_at, deps, keys));
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
    fn replica_recover(
        &mut self,
        txn: TxnId,
    ) -> (Phase, Timestamp, BTreeSet<TxnId>, BTreeSet<Key>) {
        self.clock.witness(txn);
        if let Some(t) = self.txns.get(&txn) {
            return (t.phase, t.execute_at, t.deps.clone(), t.keys.clone());
        }
        // Never seen: witness as PreAccepted at t0 with no keys/deps known.
        let entry = ReplicaTxn {
            keys: BTreeSet::new(),
            execute_at: txn,
            deps: BTreeSet::new(),
            phase: Phase::PreAccepted,
        };
        self.txns.insert(txn, entry.clone());
        self.pending.push(WalRecord::PreAccepted {
            txn,
            keys: BTreeSet::new(),
            execute_at: txn,
            deps: BTreeSet::new(),
        });
        (entry.phase, entry.execute_at, entry.deps, entry.keys)
    }

    fn recovery_record(
        &mut self,
        from: NodeId,
        txn: TxnId,
        phase: Phase,
        execute_at: Timestamp,
        deps: BTreeSet<TxnId>,
        keys: BTreeSet<Key>,
    ) {
        self.clock.witness(execute_at);
        if let Some(rec) = self.recovering.get_mut(&txn) {
            rec.replies.insert(from, (phase, execute_at, deps, keys));
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

        // (1) Adopt an already-committed decision if any replica has one.
        if let Some((_, execute_at, deps, _)) = replies
            .values()
            .filter(|(phase, ..)| *phase >= Phase::Committed)
            .max_by_key(|(_, ts, ..)| *ts)
        {
            let execute_at = *execute_at;
            let deps = deps.clone();
            return Some(self.commit_recovered(txn, execute_at, deps));
        }

        // (2) Re-drive the transaction as a fresh, recovery-flagged coordinator.
        // The original `PreAccept` may not have reached every replica (that is
        // why recovery was needed), so the keys could be missing on some — and a
        // replica with no keys would execute an empty write. Re-broadcast
        // `PreAccept` carrying the **union of keys** any RecoverOk reported, so
        // every replica (re)witnesses the transaction with its keys before we
        // commit. Then, being a recovery coordinator, we never take the fast path
        // (`advance_from_pre_accept` forces Accept → Commit).
        let union_keys: BTreeSet<Key> = replies
            .values()
            .flat_map(|(_, _, _, keys)| keys.iter().copied())
            .collect();

        let (ts, deps) = self.replica_pre_accept(txn, &union_keys);
        let mut coord_replies: BTreeMap<NodeId, (Timestamp, BTreeSet<TxnId>)> = BTreeMap::new();
        coord_replies.insert(self.id, (ts, deps));
        self.coordinating.insert(
            txn,
            Coordinating {
                t0: txn,
                replies: coord_replies,
                phase: CoordPhase::PreAccept,
                recovery: true,
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
    ) -> Vec<Out> {
        self.coordinating.insert(
            txn,
            Coordinating {
                t0: txn,
                replies: BTreeMap::new(),
                phase: CoordPhase::PreAccept,
                recovery: true,
            },
        );
        // A recovered commit is, by Accord's recovery rules, equivalent to a
        // slow-path commit (we never assert the fast path on recovery).
        self.commit(txn, execute_at, deps, false)
    }
}
