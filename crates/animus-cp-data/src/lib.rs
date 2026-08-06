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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::raft::{Out, RaftCore, RaftMsg, StateMachine};
use animus_control::{PersistedState, ProposeResult};
use animus_env::{Coresident, Env, EnvExt, NodeId};
use animus_storage::StorageEngine;
use futures::future::{Either, select};
use futures::lock::Mutex as AsyncMutex;
use serde::{Deserialize, Serialize};

/// The data plane's Raft log command: a key-value mutation (or the election
/// no-op). Keys/values are opaque bytes; ordering + durability come from Raft.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvCommand {
    /// Set `key` to `value`.
    Put { key: Vec<u8>, value: Vec<u8> },
    /// **Batch put**: set every `(key, value)` in one Raft log entry — one propose,
    /// one commit round, one apply. All keys are merged at the entry's Raft `index`
    /// (the shared MVCC version): the keys are distinct, so per-key LWW is
    /// well-defined, and re-applying on recovery is idempotent exactly as a single
    /// `Put` is. The throughput win over N individual `Put`s is one consensus round
    /// for the whole batch instead of one per key (ADR 0017 — bulk-write batching).
    /// Within one tablet the batch is atomic (it either commits whole or not at all);
    /// a cross-tablet batch is split into one `Batch` per tablet by the caller and is
    /// not atomic across tablets (matching DynamoDB `BatchWriteItem` semantics).
    Batch(Vec<(Vec<u8>, Vec<u8>)>),
    /// Remove `key` (a tombstone in the engine).
    Delete { key: Vec<u8> },
    /// **Linearizable compare-and-swap**: set `key` to `value` iff the key's
    /// current committed value equals `expected` (`None` == "only if absent").
    /// Evaluated at *apply* time against the engine's committed state, in commit
    /// order, so every replica makes the identical accept/reject decision (no
    /// clock/RNG) — and two CAS racing from the same `expected` have exactly one
    /// winner (whichever Raft ordered first). The outcome is recorded in driver
    /// state keyed by the entry's log index for the proposer to read.
    Cas {
        key: Vec<u8>,
        expected: Option<Vec<u8>>,
        value: Vec<u8>,
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
/// and the shared `RaftCore`/`RaftMsg` are untouched.
#[derive(Clone, Debug, Serialize, Deserialize)]
enum KvWire {
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
    pub fn start(env: E, all_nodes: Vec<NodeId>, storage: S) -> Self {
        Self::start_inner(env, all_nodes, storage, None)
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
        Self::start_inner(env, all_nodes, storage, Some(on_split))
    }

    fn start_inner(
        env: E,
        all_nodes: Vec<NodeId>,
        storage: S,
        on_split: Option<SplitHook>,
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
        let node = Self {
            env: env.clone(),
            core: Arc::clone(&core),
            storage: storage.clone(),
            all_nodes: all_nodes.clone(),
            reads: Arc::clone(&reads),
            cas: Arc::clone(&cas),
            engine_applied: Arc::clone(&engine_applied),
            halted: Arc::clone(&halted),
            stopped: Arc::clone(&stopped),
            apply_stopped: Arc::clone(&apply_stopped),
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
            on_split,
            halted,
            stopped,
            apply_stopped,
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

    /// Propose a write to this group. Honored only on the leader (otherwise
    /// returns the leader hint); the value is durable + applied once committed.
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> ProposeResult {
        self.lock().propose(KvCommand::Put { key, value })
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
        self.lock().propose(KvCommand::Batch(puts))
    }

    /// Propose a delete (tombstone) to this group.
    pub fn delete(&self, key: Vec<u8>) -> ProposeResult {
        self.lock().propose(KvCommand::Delete { key })
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
        self.lock().propose(KvCommand::Cas {
            key,
            expected,
            value,
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
        self.lock().change_membership(voters)
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
        self.lock().propose(KvCommand::Split { at })
    }

    /// The live `(key, value)` pairs with `key >= at` in this replica's engine —
    /// the data to seed the new tablet's group on a split. Read on the leader
    /// (its committed state is authoritative) before proposing the split.
    pub async fn range_snapshot(&self, at: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        keys_from(&self.storage, at).await
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
        for (key, value) in &seed {
            storage.merge(key, value, 0).await.expect("raftkv seed");
        }
        Self::start(env, all_nodes, storage)
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
            .get(key)
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
        let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = self
            .storage
            .entries()
            .await
            .ok()?
            .into_iter()
            .filter(|(k, _)| k.as_slice() >= start && end.is_none_or(|e| k.as_slice() < e))
            .map(|(k, vv)| (k, vv.value))
            .collect();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        if let Some(n) = limit {
            pairs.truncate(n);
        }
        Some(pairs)
    }

    /// The **ReadIndex read barrier** (ADR 0017 B.2): record `read_index =
    /// commit_index` for the current term, confirm via a quorum of read-probe acks
    /// that we are still the leader for that term (no log entry, no wall clock),
    /// and wait until applied state reaches `read_index`. Returns `true` when it is
    /// safe to serve a local read linearizably; `false` if this node is not (or
    /// stops being) the leader, or confirmation times out — so a deposed leader
    /// never serves a stale read (a newer leader needs a quorum at a higher term,
    /// which rejects the probe). Shared by `linearizable_get` + `linearizable_scan`.
    async fn read_barrier(&self) -> bool {
        let (term, read_index) = {
            let c = self.lock();
            if !c.is_leader() {
                return false;
            }
            (c.term(), c.commit_index())
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
        let probe =
            serde_json::to_vec(&KvWire::ReadProbe { term, epoch }).expect("probe serializes");
        for &p in &self.all_nodes {
            if p != self.env.node_id() {
                self.env.send(p, probe.clone()).await;
            }
        }

        let majority = self.majority();
        let deadline = self.env.now().0 + READ_TIMEOUT.as_nanos() as u64;
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
    /// **Limitation (production-hardening, deferred to the `animusd` assembly):**
    /// the hook is invoked on *every* apply of the `Split`, so re-applying it after
    /// a crash recovery would mint the sibling twice. Idempotency across recovery
    /// (and the control-plane-driven id allocation) is the remaining integration
    /// plumbing, like Stage C/D's other deferred pieces.
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
    wal_lock: &AsyncMutex<()>,
    halted: &AtomicBool,
) -> bool {
    let mut did_work = false;

    // Install a fully-received snapshot (a follower catching up) into the engine
    // *before* applying log-tail effects, so the tail merges on top of the base.
    let pending_install = core
        .lock()
        .expect("raftkv core poisoned")
        .drain_pending_install();
    if let Some((last_index, bytes)) = pending_install {
        install_engine_image(storage, &bytes).await;
        engine_applied.fetch_max(last_index, Ordering::SeqCst);
        did_work = true;
    }

    // Apply the now-durable committed commands to the engine, in commit order.
    // The Raft index is the MVCC version: per-key LWW then reproduces the agreed
    // total order, and re-applying on recovery is idempotent.
    let effects = core.lock().expect("raftkv core poisoned").drain_apply();
    did_work |= !effects.is_empty();
    for (index, command) in effects {
        match command {
            KvCommand::Put { key, value } => {
                storage
                    .merge(&key, &value, index)
                    .await
                    .expect("raftkv apply put");
            }
            KvCommand::Batch(puts) => {
                // Every key in the batch merges at this one entry's `index` (the
                // shared MVCC version). The keys are distinct, so per-key LWW is
                // well-defined; `engine_applied` advances once past the whole batch
                // at the end of the loop iteration (the batch is one entry). Composes
                // with a future coalesced-fsync merge_batch (perf/lsm) — this is the
                // normal per-key `merge` path that batching optimization refines.
                for (key, value) in &puts {
                    storage
                        .merge(key, value, index)
                        .await
                        .expect("raftkv apply batch put");
                }
            }
            KvCommand::Delete { key } => {
                storage
                    .merge_tombstone(&key, index)
                    .await
                    .expect("raftkv apply delete");
            }
            KvCommand::Cas {
                key,
                expected,
                value,
            } => {
                // Read the key's *current committed* value (the latest applied,
                // since we apply in commit order and earlier entries in this batch
                // already merged above) and compare to `expected`. Equal → swap;
                // else no-op. Deterministic on every replica (same order, same
                // committed state, no clock/RNG), so concurrent CAS from the same
                // `expected` resolve to exactly one winner — whichever Raft put
                // first, since the first swap moves the committed value and the
                // second's compare then fails.
                let current = storage
                    .get(&key)
                    .await
                    .expect("raftkv cas read")
                    .map(|vv| vv.value);
                let swapped = current == expected;
                if swapped {
                    // Same write path as `Put`: index is the MVCC version, so
                    // re-applying on recovery is idempotent (per-key LWW).
                    storage
                        .merge(&key, &value, index)
                        .await
                        .expect("raftkv apply cas");
                }
                cas.lock()
                    .expect("cas results poisoned")
                    .outcomes
                    .insert(index, swapped);
            }
            KvCommand::Split { at } => {
                // Capture the handed-off range `[at, ∞)` from this replica's
                // committed state. Every replica applies the same `Split` at the
                // same point in the command order, so the captured handoff is
                // consistent across replicas (ADR 0017 D).
                let handoff = keys_from(storage, &at).await;
                // In-band new-group creation: hand the range to the split hook,
                // which (when wired) mints a co-resident sibling and seeds the new
                // tablet's group from it. With no hook, the new group is created
                // externally from a leader handoff (the prior behavior).
                if let Some(hook) = on_split {
                    hook(at.clone(), handoff.clone());
                }
                // The handed-off range now belongs to the new tablet, so tombstone
                // it here — consistently on every replica.
                for (key, _) in &handoff {
                    storage
                        .merge_tombstone(key, index)
                        .await
                        .expect("raftkv apply split tombstone");
                }
            }
            KvCommand::NoOp => {}
        }
        // The engine now reflects `index`; publish it *after* the merge so
        // linearizable reads (which gate on `engine_applied`) never observe past
        // the engine, and compaction snapshots only up to a merged index.
        engine_applied.fetch_max(index, Ordering::SeqCst);
    }

    // Compact once the *engine* has merged enough past the snapshot base: snapshot
    // the engine image (so a lagging follower can be caught up via
    // `InstallSnapshot`), truncate the Raft log prefix, and rewrite the WAL to its
    // bounded image (ADR 0017 A.2). We snapshot only up to `engine_applied`, not the
    // core's `last_applied` (which the async apply lags) — else the truncated log
    // prefix would run past what the engine image contains. This task is the only
    // engine writer, so nothing merges between reading `ea` and the snapshot below,
    // making the image reflect exactly `ea`.
    let ea = engine_applied.load(Ordering::SeqCst);
    let behind = ea.saturating_sub(core.lock().expect("raftkv core poisoned").snapshot_index());
    // Skip compaction once a shutdown is requested: it is only a WAL-bounding
    // optimization (the engine + un-truncated WAL stay consistent without it), and
    // starting a full WAL rewrite while the env is being torn down races the task
    // abort — the `replace` can then fail on a half-gone data dir.
    if behind >= COMPACT_THRESHOLD && !halted.load(Ordering::SeqCst) {
        let image = engine_image(storage).await; // slow scan, no locks held
        // Serialize the WAL rewrite against the consensus loop's appends.
        let _wal = wal_lock.lock().await;
        let (bytes, lli) = {
            let mut c = core.lock().expect("raftkv core poisoned");
            c.set_snapshot_blob(image);
            c.snapshot_upto(ea);
            let lli = c.last_log_index();
            let mut buf = Vec::new();
            for record in c.wal_image() {
                buf.extend(PersistedState::encode_record(&record));
            }
            c.take_snapshot_dirty();
            // The rewrite (below) makes the whole current log durable, so the
            // consensus loop's accumulated pending append records are now redundant
            // — drop them (`replay` is push-based, so re-appending them would
            // duplicate entries). `wal_image` already captures the net durable state
            // (snapshot + hard + log tail). Under this one lock hold, so no
            // propose/append interleaves.
            let _ = c.drain_persist();
            (buf, lli)
        };
        match env.replace(WAL, &bytes).await {
            Ok(()) => {
                // Physically durable now — advance the watermark.
                core.lock()
                    .expect("raftkv core poisoned")
                    .mark_durable_through(lli);
            }
            // A shutdown that landed mid-rewrite (aborting tasks + dropping the data
            // dir) can fail the `replace`; tolerate it only while halted — the
            // pre-compaction WAL is still intact, so recovery is unaffected. A
            // failure while *not* halted is a real durability fault → surface it.
            Err(e) => {
                assert!(
                    halted.load(Ordering::SeqCst),
                    "raftkv wal compaction failed while running: {e}"
                );
            }
        }
        did_work = true;
    }

    did_work
}

/// The engine's live `(key, value)` pairs with `key >= at` — the data handed off
/// to the new tablet on a split.
async fn keys_from<S: StorageEngine>(storage: &S, at: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    storage
        .entries()
        .await
        .expect("raftkv engine scan")
        .into_iter()
        .filter(|(k, _)| k.as_slice() >= at)
        .map(|(k, vv)| (k, vv.value))
        .collect()
}

/// One key's snapshot entry: `(key, value-or-tombstone, version)`.
type ImageEntry = (Vec<u8>, Option<Vec<u8>>, u64);

/// Serialize the engine's full contents (including tombstones) as the snapshot
/// image shipped to a lagging follower.
async fn engine_image<S: StorageEngine>(storage: &S) -> Vec<u8> {
    let entries: Vec<ImageEntry> = storage
        .entries_with_tombstones()
        .await
        .expect("raftkv engine scan");
    serde_json::to_vec(&entries).expect("engine image serializes")
}

/// Write a received snapshot image into the engine (a follower catching up),
/// versioned so per-key LWW keeps it consistent with the log tail merged on top.
async fn install_engine_image<S: StorageEngine>(storage: &S, bytes: &[u8]) {
    let entries: Vec<ImageEntry> = match serde_json::from_slice(bytes) {
        Ok(e) => e,
        Err(err) => {
            tracing::warn!(?err, "undecodable raftkv snapshot image dropped");
            return;
        }
    };
    for (key, value, version) in entries {
        match value {
            Some(v) => {
                storage.merge(&key, &v, version).await.expect("install put");
            }
            None => {
                storage
                    .merge_tombstone(&key, version)
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
    on_split: Option<SplitHook>,
    halted: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    apply_stopped: Arc<AtomicBool>,
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
        on_split,
        halted,
        stopped,
        apply_stopped,
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

    // Spawn the apply task now — after recovery seeded the core + `engine_applied`,
    // so it never merges against pre-recovery state.
    env.spawn_task(apply_loop(
        env.clone(),
        Arc::clone(&core),
        storage,
        cas,
        on_split,
        Arc::clone(&engine_applied),
        Arc::clone(&wal_lock),
        Arc::clone(&halted),
        apply_stopped,
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

        // Each step yields outbound `KvWire` messages (Raft traffic and/or a read
        // probe ack).
        let outs: Vec<(NodeId, KvWire)> = match select(env.recv(), env.sleep(wait)).await {
            Either::Left((envelope, _)) => {
                let entropy = env.next_u64();
                match serde_json::from_slice::<KvWire>(&envelope.payload) {
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
            Either::Right(((), _)) => {
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

        // Durability before action: persist (fsync) before shipping responses, so a
        // granted vote / appended entry is on disk before its message goes out.
        // Engine apply happens independently on the apply task.
        persist_wal(&env, &core, &wal_lock).await;

        for (to, wire) in outs {
            let bytes = serde_json::to_vec(&wire).expect("raftkv message serializes");
            env.send(to, bytes).await;
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
    wal_lock: Arc<AsyncMutex<()>>,
    halted: Arc<AtomicBool>,
    apply_stopped: Arc<AtomicBool>,
) {
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
            &wal_lock,
            &halted,
        )
        .await;
        if !did_work {
            env.sleep(APPLY_IDLE_POLL).await;
        }
    }
}
