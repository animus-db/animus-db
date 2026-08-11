//! The `Env`-driven Accord node: a thin driver that owns the environment and
//! ferries messages between the network and the synchronous [`AccordCore`].
//!
//! Mirrors `animus-control`'s `RaftNode`: all consensus logic lives in the sync
//! core; this driver only does I/O. The driver is a `recv` loop plus a
//! **periodic retry timer** ([`retry_loop`]) that re-sends the un-acknowledged
//! protocol messages of any in-flight round so a dropped fire-and-forget `send`
//! no longer strands a transaction (ADR 0011, message retry). The retry timer is
//! perpetual, so drive tests with `run_for`/`run_until`, never `run()`. The core
//! decides *what* to re-send ([`AccordCore::resend_pending`]); the driver only
//! ships it. Submitting a transaction ships its initial `PreAccept` burst
//! out-of-band.
//!
//! **Durability before action** (ADR 0011 follow-up). The core accumulates
//! [`WalRecord`](crate::WalRecord)s as it advances a transaction's phase; the
//! driver drains them, appends them to the per-node WAL on the `Env` disk, and
//! `fsync`s **before** shipping the outbound messages that depend on them (a
//! PreAcceptOk a peer quorum will count, a Commit a peer will execute on). On
//! startup the driver replays the WAL and recovers the core, so a restarted
//! replica keeps every committed/executed transaction.
//!
//! **Storage-backed execution.** The core decides *when* and in *what order* a
//! committed transaction executes; the **effect** is applied here, against a
//! real (async) [`StorageEngine`]. After fsyncing the durable records, the
//! driver drains the core's [`ApplyEffect`]s and `merge`s each transaction's
//! writes into the engine, stamped with the transaction's execution timestamp as
//! the MVCC version. `merge` (per-key last-writer-wins) makes the apply
//! idempotent and commutative, so a re-apply after a crash/restart converges to
//! the same store. The engine is the in-memory [`MemoryEngine`] (this crate is a
//! `SimEnv` testbed — see the crate docs); a recovered node repopulates a
//! *fresh* engine in the original execution order from the WAL.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::{Env, EnvExt, NodeId};
use animus_storage::{MemoryEngine, StorageEngine};

use crate::core::{AccordCore, ApplyEffect, Decision, Key, ReadEffect, TxnId};
use crate::message::{AccordMsg, Out};
use crate::persist::PersistedState;
use crate::timestamp::Timestamp;

/// File name of the per-node Accord write-ahead log on the `Env` disk.
const WAL: &str = "accord.wal";

/// The **base** retry interval: how soon after a round still has un-acknowledged
/// messages the driver first re-sends them (ADR 0011, message retry). The network
/// is fire-and-forget and may drop, so without this a dropped message strands a
/// transaction. Chosen well above the simulator's default link delay so a retry
/// only fires when a message was genuinely lost, not merely in flight. The retry
/// tick uses **exponential backoff** from this base (see [`RETRY_MAX_INTERVAL`]
/// and [`retry_loop`]).
const RETRY_BASE_INTERVAL: Duration = Duration::from_millis(200);

/// The **ceiling** on the adaptive retry interval. Under persistent loss the
/// interval doubles from [`RETRY_BASE_INTERVAL`] up to this cap, so a transaction
/// that cannot yet gather a quorum is retried ever less often instead of
/// hammering the network every base interval — fewer redundant sends while still
/// guaranteeing eventual delivery. The interval **resets to the base** the moment
/// a round makes progress (fewer messages owed) or completes (none owed), so a
/// transient drop is still recovered promptly.
const RETRY_MAX_INTERVAL: Duration = Duration::from_millis(1600);

/// How often the **failure-detector** tick samples in-flight transactions (ADR
/// 0011, failure-detector-triggered recovery). Each tick the driver checks every
/// transaction this replica holds *un-committed* and asks the core whether it has
/// progressed since the last sample (see [`AccordCore::progress_fingerprint`]).
const LIVENESS_INTERVAL: Duration = Duration::from_millis(100);

/// How many consecutive [`LIVENESS_INTERVAL`] ticks an un-committed transaction
/// must go **without progressing** before its coordinator is suspected dead and
/// recovery is auto-triggered. The bound is the product
/// `LIVENESS_INTERVAL * LIVENESS_STALL_TICKS` (≈ 5s here) of stalled virtual time.
/// Two forces set it:
///
/// 1. **Avoid spurious recovery of a slow-but-live coordinator.** A *replica* only
///    watches its own view of a transaction it does not coordinate, which advances
///    only on a phase change (PreAccepted→Accepted→Committed) — it cannot see the
///    coordinator slowly gathering a quorum of *same-timestamp* `PreAcceptOk`s (no
///    phase change, so [`AccordCore::progress_fingerprint`] does not move). So a
///    coordinator that is merely slow or transiently partitioned looks
///    indistinguishable from a dead one *except by elapsed time*. The bound must
///    therefore comfortably exceed a realistic slow-commit / partition-and-heal
///    window so a coordinator that *will* commit on its own (at its original `t0`)
///    gets to — recovering it instead re-orders its transaction **after** every
///    conflicting transaction committed in the meantime (`replica_pre_accept`
///    bumps the recovered timestamp past them), which for a single-writer
///    list-append workload would let a stale earlier write land last and lose
///    later appends. (This is the corpus regression that set this bound; ADR
///    0014 / `animus-test`.)
/// 2. **Still recover a genuinely dead coordinator promptly enough.** A crashed or
///    stopped coordinator never heals, so after the bound the deterministic
///    nominee takes over. ~5s of recovery latency for a dead coordinator is
///    acceptable; correctness does not depend on the exact value, only on it being
///    larger than the live-but-slow window.
///
/// Each *further* full window with no progress promotes the next escalation
/// **tier** ([`AccordCore::is_recovery_nominee`]), so a dead nominee is eventually
/// replaced by the next live survivor. The recovered commit is additionally
/// **ballot-fenced** ([`AccordMsg::Commit`]'s `ballot`,
/// [`AccordCore::replica_commit`]) so a late lower-ballot `Commit` from a healed
/// original coordinator cannot *revert* a recovered decision — a genuine safety
/// improvement, independent of the bound.
const LIVENESS_STALL_TICKS: u32 = 50;

/// The per-node store of read-transaction results: for each executed read-only
/// transaction, the **raw value bytes** observed at each key at the read's
/// execution timestamp (`None` = the key had no committed write before the
/// read). Populated by the driver as it satisfies [`ReadEffect`]s, so the result
/// is available once [`AccordNode::is_applied`] holds for the read txn. Stored as
/// raw bytes so a read of an arbitrary value (list-append, ADR 0011) is observed
/// verbatim; [`AccordNode::read_result`] decodes them as a writer txn id for the
/// classic register view, [`AccordNode::read_value_result`] returns them raw.
type ReadResults = Arc<Mutex<BTreeMap<TxnId, BTreeMap<Key, Option<Vec<u8>>>>>>;

/// A running consensus replica. Cheap to clone; clones share one [`AccordCore`]
/// and one storage engine.
///
/// Execution is backed by a fresh in-memory [`MemoryEngine`] — this crate is the
/// Elle corpus's known-serializable testbed (see the crate docs), so there is no
/// production storage backend to inject.
pub struct AccordNode<E: Env> {
    env: E,
    core: Arc<Mutex<AccordCore>>,
    storage: MemoryEngine,
    /// Results of executed read-only transactions (see [`ReadResults`]).
    reads: ReadResults,
}

impl<E: Env> Clone for AccordNode<E> {
    fn clone(&self) -> Self {
        AccordNode {
            env: self.env.clone(),
            core: Arc::clone(&self.core),
            storage: self.storage.clone(),
            reads: Arc::clone(&self.reads),
        }
    }
}

impl<E: Env> AccordNode<E> {
    /// Start a node backed by a fresh in-memory [`MemoryEngine`]. `all_nodes` is
    /// the full replica set (including this node). The driver recovers durable
    /// state from the WAL before serving anything, replaying its execution order
    /// into the fresh engine.
    pub fn start(env: E, all_nodes: Vec<NodeId>) -> AccordNode<E> {
        let storage = MemoryEngine::new();
        let core = Arc::new(Mutex::new(AccordCore::new(env.node_id(), &all_nodes)));
        let reads: ReadResults = Arc::new(Mutex::new(BTreeMap::new()));
        let node = AccordNode {
            env: env.clone(),
            core: Arc::clone(&core),
            storage: storage.clone(),
            reads: Arc::clone(&reads),
        };
        env.spawn_task(drive(
            env.clone(),
            Arc::clone(&core),
            storage,
            reads,
            all_nodes,
        ));
        // Retry tick: re-send un-acknowledged protocol messages for in-flight
        // rounds so a dropped fire-and-forget `send` no longer strands a
        // transaction (ADR 0011). A perpetual timer, so tests must drive bounded
        // virtual time (`run_for`/`run_until`), never `run()`.
        env.spawn_task(retry_loop(
            env.clone(),
            Arc::clone(&core),
            node.storage.clone(),
            Arc::clone(&node.reads),
        ));
        // Failure-detector tick: auto-trigger recovery of a transaction that has
        // been held un-committed past a time bound without progressing — its
        // coordinator is suspected dead (ADR 0011). Also a perpetual timer, so
        // tests must drive bounded virtual time.
        env.spawn_task(liveness_loop(
            env.clone(),
            Arc::clone(&core),
            node.storage.clone(),
            Arc::clone(&node.reads),
        ));
        node
    }

    /// Submit a new **write** transaction over `keys` for this node to
    /// coordinate. Mints `t0`, ships the `PreAccept` burst (after fsyncing the
    /// durable state the burst depends on), and returns the transaction id.
    pub fn submit(&self, keys: BTreeSet<Key>) -> TxnId {
        let (txn, outs) = self.lock().submit(keys);
        persist_then_ship(&self.env, &self.core, &self.storage, &self.reads, outs);
        txn
    }

    /// Submit a **read-only** transaction over `keys`. It is ordered exactly like
    /// a write (timestamp + conflict deps) and, once it executes, reads each key
    /// as of its execution timestamp — observing the writes of every transaction
    /// ordered before it and none ordered after. The observed values are
    /// retrievable via [`AccordNode::read_result`] once [`AccordNode::is_applied`]
    /// holds for the returned id.
    pub fn submit_read(&self, keys: BTreeSet<Key>) -> TxnId {
        let (txn, outs) = self.lock().submit_read(keys);
        persist_then_ship(&self.env, &self.core, &self.storage, &self.reads, outs);
        txn
    }

    /// Submit a **read-modify-write** transaction that reads `read_keys` and
    /// writes `write_keys` under a single Accord transaction. Its conflict set is
    /// the union of the two, so a key it merely read participates in ordering
    /// exactly like one it writes — a concurrent write to a read key is ordered
    /// relative to this transaction (the read-then-write hazard). Only the
    /// `write_keys` carry the write effect at execution. See
    /// [`AccordCore::submit_rw`]; this is what [`InteractiveTxn::commit`] uses so
    /// an interactive session's reads fold into the committed transaction's
    /// dependency tracking.
    pub fn submit_rw(&self, read_keys: BTreeSet<Key>, write_keys: BTreeSet<Key>) -> TxnId {
        let (txn, outs) = self.lock().submit_rw(read_keys, write_keys);
        persist_then_ship(&self.env, &self.core, &self.storage, &self.reads, outs);
        txn
    }

    /// Submit a **write** transaction with explicit caller-supplied **values**
    /// (arbitrary write values, ADR 0011): each `(key, value)` writes the given
    /// bytes to `key` at execution, instead of the transaction's own id. The
    /// write key set is the map's keys; the conflict set equals it. Ordered,
    /// committed, and applied atomically at one execution timestamp exactly like
    /// [`AccordNode::submit`]. A black-box reader (or a data-plane quorum read)
    /// then observes the real value, not a register id. See
    /// [`AccordCore::submit_writes`].
    pub fn submit_writes(&self, writes: BTreeMap<Key, Vec<u8>>) -> TxnId {
        let (txn, outs) = self.lock().submit_writes(writes);
        persist_then_ship(&self.env, &self.core, &self.storage, &self.reads, outs);
        txn
    }

    /// Submit a **read-modify-write** transaction with explicit caller-supplied
    /// write **values** (arbitrary write values, ADR 0011): it reads `read_keys`
    /// (which join the conflict set but produce no write) and writes each
    /// `(key, value)` in `writes` with the supplied bytes. The conflict set is the
    /// union of the read keys and the write keys, so a concurrent write to a key
    /// this transaction *read* is ordered relative to it (the read-then-write
    /// hazard). This is the value-carrying form of [`AccordNode::submit_rw`];
    /// [`InteractiveTxn::commit`] uses it so an interactive session writes real
    /// values that survive read-back. See [`AccordCore::submit_writes_rw`].
    pub fn submit_writes_rw(
        &self,
        read_keys: BTreeSet<Key>,
        writes: BTreeMap<Key, Vec<u8>>,
    ) -> TxnId {
        let (txn, outs) = self.lock().submit_writes_rw(read_keys, writes);
        persist_then_ship(&self.env, &self.core, &self.storage, &self.reads, outs);
        txn
    }

    /// Take over `txn` as a *recovery coordinator* (its original coordinator is
    /// suspected dead). Broadcasts `Recover` and drives the transaction to a
    /// consistent commit. See [`AccordCore::recover`].
    pub fn recover(&self, txn: TxnId) {
        let outs = self.lock().recover(txn);
        persist_then_ship(&self.env, &self.core, &self.storage, &self.reads, outs);
    }

    /// The result of an executed read-only transaction: the writer this replica
    /// observed at each key at the read's execution timestamp (`None` = no
    /// committed write at that key ordered before the read). Returns `None` until
    /// the read has executed here (see [`AccordNode::is_applied`]).
    #[must_use]
    pub fn read_result(&self, txn: TxnId) -> Option<BTreeMap<Key, Option<TxnId>>> {
        let raw = self.read_value_result(txn)?;
        Some(
            raw.into_iter()
                .map(|(k, v)| (k, v.as_deref().and_then(decode_txn)))
                .collect(),
        )
    }

    /// The result of an executed read-only transaction as **raw value bytes**: the
    /// bytes this replica observed at each key at the read's execution timestamp
    /// (`None` = no committed write at that key ordered before the read). Returns
    /// `None` until the read has executed here (see [`AccordNode::is_applied`]).
    /// Unlike [`AccordNode::read_result`], the bytes are returned verbatim — so a
    /// read of an arbitrary value (list-append, ADR 0011) is observed exactly as
    /// it was written, which is what a black-box consistency checker needs.
    #[must_use]
    pub fn read_value_result(&self, txn: TxnId) -> Option<BTreeMap<Key, Option<Vec<u8>>>> {
        self.reads
            .lock()
            .expect("read results poisoned")
            .get(&txn)
            .cloned()
    }

    /// This node's environment handle.
    pub fn env(&self) -> &E {
        &self.env
    }

    /// The agreed execution timestamp this replica recorded for `txn`, if it has
    /// reached the committed phase (committed or applied).
    pub fn committed_execute_at(&self, txn: TxnId) -> Option<Timestamp> {
        self.lock().committed_execute_at(txn)
    }

    /// The dependencies this replica recorded for `txn` at commit, if committed.
    pub fn committed_deps(&self, txn: TxnId) -> Option<BTreeSet<TxnId>> {
        self.lock().committed_deps(txn)
    }

    /// The order in which this replica has executed (applied) transactions.
    pub fn applied_order(&self) -> Vec<TxnId> {
        self.lock().applied_order().to_vec()
    }

    /// The transaction whose write currently wins at `key` in this replica's
    /// executed store, decoded from the storage engine, if any. (Each executed
    /// transaction writes its own id as the value; see [`ApplyEffect`].)
    pub async fn store_writer(&self, key: Key) -> Option<TxnId> {
        let vv = self.storage.get(&storage_key(key)).await.ok()??;
        decode_txn(&vv.value)
    }

    /// The raw stored **value bytes** currently winning at `key` in this replica's
    /// executed local store, if any. Unlike [`AccordNode::store_writer`] (which
    /// decodes the bytes as a txn id) this returns the bytes verbatim, so a caller
    /// reading back an arbitrary value written via
    /// [`AccordNode::submit_writes`]/[`InteractiveTxn::write_value`] sees exactly
    /// what was written (arbitrary write values, ADR 0011).
    pub async fn store_value(&self, key: Key) -> Option<Vec<u8>> {
        let vv = self.storage.get(&storage_key(key)).await.ok()??;
        Some(vv.value)
    }

    /// Whether this replica has executed `txn`.
    pub fn is_applied(&self, txn: TxnId) -> bool {
        self.lock().is_applied(txn)
    }

    /// The decisions this node has reached as a coordinator, in order.
    pub fn decisions(&self) -> Vec<Decision> {
        self.lock().decisions().to_vec()
    }

    /// Begin an **interactive** transaction on this node: a `begin → read* →
    /// write* → commit` handle that lets a caller run a multi-step
    /// read-modify-write under **one** Accord transaction, instead of submitting
    /// a pre-baked op set.
    ///
    /// The handle's [`InteractiveTxn::read`] observes the current committed value
    /// of a key (through the data plane when the node is wired to it, else the
    /// local execution store) so the caller can *decide* what to write;
    /// [`InteractiveTxn::write`] buffers a write; and [`InteractiveTxn::commit`]
    /// submits the buffered writes as a single Accord **write** transaction —
    /// agreed, ordered and applied atomically at one execution timestamp, exactly
    /// like [`AccordNode::submit`]. So concurrent interactive transactions whose
    /// write sets conflict are ordered consistently on every replica, and each
    /// transaction's writes land all-or-nothing.
    ///
    /// The session's reads are **folded into the committed transaction's conflict
    /// set** (ADR 0011): `commit()` submits a read-modify-write whose conflict set
    /// is the union of the keys read and written (via [`AccordNode::submit_rw`]),
    /// so a concurrent write to a key this session read is ordered relative to
    /// this transaction — the read-then-write hazard is detected. The core stays
    /// sync + I/O-free: the handle is pure driver state and reaches the core only
    /// through `submit_rw` at commit time.
    #[must_use]
    pub fn begin(&self) -> InteractiveTxn<E> {
        InteractiveTxn {
            node: self.clone(),
            reads: BTreeSet::new(),
            writes: BTreeSet::new(),
            values: BTreeMap::new(),
        }
    }

    /// Read the current committed writer of `key` as seen by this node, from the
    /// local execution store. Used by [`InteractiveTxn::read`]; exposed so a caller
    /// can do an ad-hoc current read without opening a transaction.
    pub async fn current_writer(&self, key: Key) -> Option<TxnId> {
        self.current_value(key)
            .await
            .as_deref()
            .and_then(decode_txn)
    }

    /// Read the current committed **value bytes** of `key` as seen by this node,
    /// from the local execution store. Unlike [`AccordNode::current_writer`] this
    /// returns the raw bytes, so a caller doing a read-modify-write over arbitrary
    /// values (e.g. list-append) sees the actual value to modify (arbitrary write
    /// values, ADR 0011). `None` if the key has no committed write.
    pub async fn current_value(&self, key: Key) -> Option<Vec<u8>> {
        self.store_value(key).await
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, AccordCore> {
        self.core.lock().expect("accord core poisoned")
    }
}

/// An **interactive** transaction handle: `begin → read* → write* → commit`
/// (ADR 0011). Created by [`AccordNode::begin`].
///
/// A caller runs a multi-step read-modify-write under **one** Accord
/// transaction: [`InteractiveTxn::read`] returns the current committed writer of
/// a key (so the caller can branch on it), [`InteractiveTxn::write`] buffers a
/// write, and [`InteractiveTxn::commit`] submits the buffered write set as a
/// single Accord write transaction (via [`AccordNode::submit`]) — agreed,
/// ordered, and applied atomically at one execution timestamp. So two interactive
/// transactions whose write sets conflict are ordered consistently on every
/// replica and each lands all-or-nothing.
///
/// The handle is **pure driver state** — it holds no lock and touches the sync
/// core only through `submit` at commit time, so the core stays I/O-free.
///
/// **Conflict set.** The session's reads are folded into the committed
/// transaction's conflict set: `commit()` submits a read-modify-write whose
/// conflict set is the union of the keys read and written, so a concurrent write
/// to a key this session read is ordered relative to this transaction (ADR 0011,
/// the read-then-write hazard). Only the write set carries the write effect. A
/// `commit` with an empty write set is a no-op that returns `None`.
pub struct InteractiveTxn<E: Env> {
    node: AccordNode<E>,
    /// Keys read during the session; folded into the committed transaction's
    /// conflict set at `commit()` (so they carry dependencies).
    reads: BTreeSet<Key>,
    /// Keys the session will write when committed (the full write set). A key
    /// also present in `values` writes those bytes; a key only here writes the
    /// transaction's id (the classic register effect).
    writes: BTreeSet<Key>,
    /// Caller-supplied value bytes for keys written with [`InteractiveTxn::write_value`]
    /// (arbitrary write values, ADR 0011). A key in `writes` but absent here
    /// defaults to the transaction's id at execution.
    values: BTreeMap<Key, Vec<u8>>,
}

impl<E: Env> InteractiveTxn<E> {
    /// Read the current committed value of `key` and record it in the read set,
    /// from the local execution store. Returns the observed value writer (`None` if
    /// the key has no committed write), so the caller can decide what to write next.
    pub async fn read(&mut self, key: Key) -> Option<TxnId> {
        self.reads.insert(key);
        self.node.current_writer(key).await
    }

    /// Read the current committed **value bytes** of `key` from the local store,
    /// recording it in the read set. Unlike [`InteractiveTxn::read`] — which decodes
    /// the writer txn id — this returns the raw stored bytes, so a caller doing a
    /// read-modify-write over arbitrary values (e.g. list-append) sees the actual
    /// value to modify (ADR 0011).
    pub async fn read_value(&mut self, key: Key) -> Option<Vec<u8>> {
        self.reads.insert(key);
        self.node.current_value(key).await
    }

    /// Buffer a write to `key` whose value is the committed transaction's own id
    /// (the classic register effect; see [`ApplyEffect`]). Multiple writes to
    /// distinct keys land atomically.
    pub fn write(&mut self, key: Key) {
        self.writes.insert(key);
    }

    /// Buffer a write to `key` of an explicit, caller-supplied **value**
    /// (arbitrary write values, ADR 0011), so a later read observes those exact
    /// bytes rather than a register id. Multiple writes (valued or not) to
    /// distinct keys land atomically at one execution timestamp.
    pub fn write_value(&mut self, key: Key, value: Vec<u8>) {
        self.writes.insert(key);
        self.values.insert(key, value);
    }

    /// The keys read so far in this session.
    #[must_use]
    pub fn read_set(&self) -> &BTreeSet<Key> {
        &self.reads
    }

    /// The keys buffered to write on commit.
    #[must_use]
    pub fn write_set(&self) -> &BTreeSet<Key> {
        &self.writes
    }

    /// Commit the session as a single Accord read-modify-write transaction.
    /// Returns the committed [`TxnId`], or `None` if nothing was written (an empty
    /// write set is a no-op). The transaction's **conflict set is the union of the
    /// keys read and written**, so the session's reads fold into the committed
    /// transaction's dependency tracking: a concurrent write to a key this session
    /// read is ordered relative to this transaction (the read-then-write hazard is
    /// detected), while only the write set carries the write effect. Each written
    /// key carries its caller-supplied value (from [`InteractiveTxn::write_value`])
    /// or defaults to the transaction's id (from [`InteractiveTxn::write`]).
    /// Agreed, ordered, and applied atomically at one execution timestamp, so
    /// conflicting interactive transactions are ordered consistently on every
    /// replica.
    pub fn commit(self) -> Option<TxnId> {
        if self.writes.is_empty() {
            return None;
        }
        if self.values.is_empty() {
            // No explicit values: keep the classic txn-id write effect.
            Some(self.node.submit_rw(self.reads, self.writes))
        } else {
            // Build the per-key value map: a write key with no explicit value
            // carries the transaction's own id (added at the driver as the default
            // when a value is absent), so the conflict/write set stays exactly
            // `writes`. A key with an explicit value writes those bytes.
            let writes: BTreeMap<Key, Vec<u8>> = self.values;
            // `submit_writes_rw` derives the write-key set from the value map; any
            // valueless write keys would otherwise be dropped, so include them by
            // also passing the full write set as part of the read conflict set is
            // wrong — instead, ensure every write key is in the value map.
            debug_assert!(
                self.writes.iter().all(|k| writes.contains_key(k)),
                "mixing valued and valueless interactive writes is unsupported; \
                 use write_value for every key when any value is supplied"
            );
            Some(self.node.submit_writes_rw(self.reads, writes))
        }
    }
}

/// The storage key bytes for an Accord [`Key`]: big-endian so the byte order
/// matches the numeric order (tidy, though not load-bearing for this slice).
fn storage_key(key: Key) -> Vec<u8> {
    key.to_be_bytes().to_vec()
}

/// Bits of the MVCC [`Version`](animus_storage::Version) reserved for the node-id
/// tiebreak of an execution timestamp (see [`mvcc_version`]). 16 bits ⇒ up to
/// 65 535 distinct node ids; the high 48 bits carry the logical clock.
const MVCC_NODE_BITS: u32 = 16;

/// The MVCC storage version for an Accord execution timestamp, **preserving the
/// full `(logical, node)` total order** — not just `logical`.
///
/// Two *conflicting* transactions can legitimately agree the **same `logical`**
/// execution timestamp (the later one carries the earlier as a dependency and is
/// ordered after it purely by the `node` tiebreak — this is correct Accord, and is
/// now reachable since the precise fast-path quorum lets both fast-commit at their
/// own `t0`). The store's `merge` is strictly-newer per-key LWW on a single `u64`
/// version, so stamping both writes with `logical` alone would collide and keep
/// whichever applied *first*, diverging from the agreed `(execute_at, txn)` order
/// (the later-ordered transaction must win the shared key). Folding the `node`
/// tiebreak into the low [`MVCC_NODE_BITS`] bits makes the version strictly
/// increasing in `(logical, node)`, so per-key LWW keeps exactly the agreed winner
/// on every replica. A read uses the same encoding, so `get_at` still observes
/// every write ordered before the read and none after.
///
/// # Contract (hard-checked)
///
/// The encoding is injective only for `ts.node < 2^16` and `ts.logical < 2^48`.
/// Outside those bounds two distinct timestamps would silently collapse to one
/// version and per-key LWW would keep an arbitrary winner — a silent-corruption
/// failure a consistency testbed must never mask — so the guards are hard
/// `assert!`s (they do **not** vanish in release builds, unlike the
/// `debug_assert!`s they replaced). The bounds are unreachable in practice here:
/// the testbed uses small node ids and logical clocks advance by small
/// per-transaction increments.
fn mvcc_version(ts: Timestamp) -> u64 {
    let node = ts.node.as_u64();
    assert!(
        node < (1 << MVCC_NODE_BITS),
        "node id {node} exceeds the {MVCC_NODE_BITS}-bit MVCC tiebreak field; \
         the (logical, node) -> u64 version encoding would collide"
    );
    assert!(
        ts.logical < (1 << (64 - MVCC_NODE_BITS)),
        "logical clock {} exceeds the {}-bit MVCC version field; \
         the (logical, node) -> u64 version encoding would collide",
        ts.logical,
        64 - MVCC_NODE_BITS
    );
    (ts.logical << MVCC_NODE_BITS) | node
}

/// Encode a transaction id as the stored value (the executed effect is "write
/// my id"): `(logical, node)` as two big-endian u64s.
fn encode_txn(txn: TxnId) -> Vec<u8> {
    let mut v = Vec::with_capacity(16);
    v.extend_from_slice(&txn.logical.to_be_bytes());
    v.extend_from_slice(&txn.node.as_u64().to_be_bytes());
    v
}

/// Inverse of [`encode_txn`].
fn decode_txn(bytes: &[u8]) -> Option<TxnId> {
    if bytes.len() != 16 {
        return None;
    }
    let logical = u64::from_be_bytes(bytes[0..8].try_into().ok()?);
    let node = u64::from_be_bytes(bytes[8..16].try_into().ok()?);
    Some(Timestamp::new(logical, NodeId::new(node)))
}

/// Drain the core's pending durable records and execution effects, append +
/// `fsync` the records to the WAL, apply the effects to the storage engine, then
/// ship the outbound messages.
///
/// Spawned as a task so the synchronous call sites (`submit`/`recover`, and each
/// `handle` in the recv loop) stay synchronous; the simulator runs it promptly,
/// and within the task the fsync precedes the storage apply which precedes the
/// sends, preserving "durable before action".
fn persist_then_ship<E: Env>(
    env: &E,
    core: &Arc<Mutex<AccordCore>>,
    storage: &MemoryEngine,
    reads: &ReadResults,
    outs: Vec<Out>,
) {
    let (records, applies, read_effects) = {
        let mut c = core.lock().expect("accord core poisoned");
        (c.drain_persist(), c.drain_apply(), c.drain_reads())
    };
    let env = env.clone();
    let storage = storage.clone();
    let reads = Arc::clone(reads);
    let compact_core = Arc::clone(core);
    env.clone().spawn_task(async move {
        for record in &records {
            env.append(WAL, &PersistedState::encode_record(record))
                .await
                .expect("wal append");
        }
        if !records.is_empty() {
            env.sync(WAL).await.expect("wal sync");
        }
        // Apply writes first, then satisfy reads: a read effect emitted in the
        // same drain as a write it orders after sees that write (the core only
        // emits a read effect once its earlier-ordered conflicts are `Applied`,
        // so their write effects were drained no later than this one).
        apply_all(&storage, &applies).await;
        satisfy_reads(&storage, &reads, &read_effects).await;
        // Now that the records are durable, compact the WAL if enough has applied:
        // collapse the appended per-phase history into one snapshot record (ADR
        // 0011, log truncation). Safe here because the appends above are fsynced.
        maybe_compact(&env, &compact_core).await;
        for (to, msg) in outs {
            let bytes = serde_json::to_vec(&msg).expect("accord message serializes");
            env.send(to, bytes).await;
        }
    });
}

/// How many applied transactions must accumulate beyond the current snapshot base
/// before the driver rewrites the WAL to its compact image (ADR 0011, log
/// truncation). Bounds the WAL to roughly the live transaction set plus this many
/// recent applies of append history. Mirrors the control-plane Raft's
/// `SNAPSHOT_THRESHOLD`.
const SNAPSHOT_THRESHOLD: usize = 64;

/// Take a snapshot and **atomically replace** the WAL with the compact image when
/// enough transactions have applied since the last snapshot (or the core already
/// flagged the WAL dirty). The truncation is materialised as a full rewrite via
/// `env.replace`, never incremental records — so a crash sees either the whole old
/// or whole new WAL, and the rewritten WAL replays to the identical core
/// ([`AccordCore::wal_image`] / [`AccordCore::recovered`]). Mirrors the control
/// plane's `flush_and_maybe_compact`.
async fn maybe_compact<E: Env>(env: &E, core: &Arc<Mutex<AccordCore>>) {
    let (rewrite, image) = {
        let mut c = core.lock().expect("accord core poisoned");
        if c.applied_since_snapshot() >= SNAPSHOT_THRESHOLD {
            c.snapshot();
        }
        let rewrite = c.take_snapshot_dirty();
        // Build the image under the same lock so it matches the snapshot we just
        // took; cheap (a clone of live state) and lock-free I/O follows.
        let image = if rewrite { c.wal_image() } else { Vec::new() };
        (rewrite, image)
    };
    if !rewrite {
        return;
    }
    let mut bytes = Vec::new();
    for record in &image {
        bytes.extend(PersistedState::encode_record(record));
    }
    env.replace(WAL, &bytes).await.expect("wal compaction");
}

/// Apply the execution effects. Each effect writes its value to each key it
/// touches at the transaction's execution timestamp as the MVCC version.
///
/// The write lands in the local `storage` engine via `merge` (per-key LWW —
/// idempotent and commutative, so a re-apply on recovery converges, and
/// `store_writer`/`store_value` read it back).
async fn apply_all(storage: &MemoryEngine, applies: &[ApplyEffect]) {
    for effect in applies {
        // The default value (no caller-supplied bytes) is the txn's own id — the
        // classic register effect, which `store_writer` decodes back. A caller who
        // supplied an arbitrary value (`submit_writes`/`InteractiveTxn::write_value`)
        // gets exactly those bytes written (arbitrary write values, ADR 0011).
        let default_value = encode_txn(effect.txn);
        // Stamp the write with the full `(execute_at, txn)` order, not just the
        // logical component, so two conflicting writes that agreed the same logical
        // timestamp still converge to the agreed (node-tiebroken) winner under the
        // store's per-key LWW. See [`mvcc_version`].
        let version = mvcc_version(effect.version);
        for &key in &effect.keys {
            let sk = storage_key(key);
            let value = effect.values.get(&key).unwrap_or(&default_value);
            storage
                .merge(&sk, value, version)
                .await
                .expect("storage merge");
        }
    }
}

/// Satisfy read-only transactions: for each [`ReadEffect`], read every key it
/// touches and record the observed per-key **raw value bytes** in the shared
/// [`ReadResults`] under the read txn's id (so an arbitrary value — list-append,
/// ADR 0011 — is observed verbatim; the register-writer view decodes them).
///
/// Each key is read from the local execution store *as of* the read's execution
/// timestamp (`get_at`) — so it observes exactly the writes that executed before
/// it (lower MVCC version) and none after. This is sound because the core only
/// emits a read's [`ReadEffect`] once every earlier-ordered conflicting write has
/// `Applied`.
async fn satisfy_reads(storage: &MemoryEngine, reads: &ReadResults, read_effects: &[ReadEffect]) {
    for effect in read_effects {
        // Read as of the read's full `(execute_at, txn)` order (same encoding as
        // the write version, [`mvcc_version`]): `get_at` returns the greatest write
        // with a packed version `<=` the read's, i.e. every write ordered before
        // the read and none after (txn ids are distinct, so no write collides with
        // the read's own version).
        let version = mvcc_version(effect.version);
        let mut observed: BTreeMap<Key, Option<Vec<u8>>> = BTreeMap::new();
        for &key in &effect.keys {
            let sk = storage_key(key);
            let value = storage
                .get_at(&sk, version)
                .await
                .expect("storage get_at")
                .map(|vv| vv.value);
            observed.insert(key, value);
        }
        reads
            .lock()
            .expect("read results poisoned")
            .insert(effect.txn, observed);
    }
}

/// The per-node driver loop: recover durable state, then repeatedly wait for the
/// next message, hand it to the core, persist the resulting durable changes,
/// apply execution effects, and ship whatever the core wants sent.
async fn drive<E: Env>(
    env: E,
    core: Arc<Mutex<AccordCore>>,
    storage: MemoryEngine,
    reads: ReadResults,
    all_nodes: Vec<NodeId>,
) {
    // Recover from the WAL before serving anything.
    let bytes = env.read(WAL).await.unwrap_or_default();
    let state = PersistedState::replay(PersistedState::decode(&bytes));
    if !state.is_empty() {
        let recovered = AccordCore::recovered(env.node_id(), &all_nodes, state);
        let (applies, read_effects) = {
            let mut guard = core.lock().expect("accord core poisoned");
            *guard = recovered;
            // The core emitted apply/read effects for its recovered execution
            // order; repopulate the (fresh, volatile) storage engine and re-run
            // the recovered reads in that order.
            (guard.drain_apply(), guard.drain_reads())
        };
        apply_all(&storage, &applies).await;
        satisfy_reads(&storage, &reads, &read_effects).await;
    }

    loop {
        let envelope = env.recv().await;
        let outs = match serde_json::from_slice::<AccordMsg>(&envelope.payload) {
            Ok(msg) => core
                .lock()
                .expect("accord core poisoned")
                .handle(envelope.from, msg),
            Err(err) => {
                tracing::warn!(?err, "undecodable accord message dropped");
                Vec::new()
            }
        };
        // Durable before action: fsync the core's state changes (e.g. a Commit
        // we just executed) before applying effects and shipping messages.
        persist_then_ship(&env, &core, &storage, &reads, outs);
    }
}

/// The retry tick: on an **adaptive (exponential-backoff)** `Env` timer, re-send
/// the outbound messages for every in-flight round that has not completed (ADR
/// 0011, message retry + adaptive backoff).
///
/// `Network::send` is fire-and-forget and may drop, which would otherwise strand
/// a transaction. The synchronous core decides *what* is still owed and *to
/// whom* ([`AccordCore::resend_pending`]); this driver only times the re-sends,
/// drains, and ships — so determinism stays in the core and no lock is held
/// across an `.await`.
///
/// **Backoff.** Instead of a fixed interval, the wait starts at
/// [`RETRY_BASE_INTERVAL`] and **doubles** (capped at [`RETRY_MAX_INTERVAL`])
/// each round in which the same-or-more messages are still owed — so a
/// transaction that genuinely cannot gather a quorum is retried ever less often,
/// cutting redundant sends under persistent loss. It **resets to the base** the
/// moment a round makes progress (strictly fewer messages owed than last tick —
/// a reply got through) or completes (none owed), so a transient drop is still
/// recovered promptly. The backoff state is a plain local, so the timer stays a
/// deterministic `Env` timer and the run remains seed-reproducible. A completed
/// round emits nothing, so retries stop on their own. We route through
/// `persist_then_ship` so any incidental durable records/effects drain too (in
/// steady state there are none on a pure retry).
async fn retry_loop<E: Env>(
    env: E,
    core: Arc<Mutex<AccordCore>>,
    storage: MemoryEngine,
    reads: ReadResults,
) {
    let mut interval = RETRY_BASE_INTERVAL;
    // The number of messages owed on the previous tick, to detect progress.
    let mut last_owed: usize = 0;
    loop {
        env.sleep(interval).await;
        let outs = {
            let mut c = core.lock().expect("accord core poisoned");
            c.resend_pending()
        };
        let owed = outs.len();
        // Adapt the next interval: reset to base on progress (fewer owed, incl.
        // dropping to zero), otherwise back off (double, capped). A round that is
        // still stuck at the same owed count is retried less and less often.
        if owed == 0 || owed < last_owed {
            interval = RETRY_BASE_INTERVAL;
        } else {
            interval = (interval * 2).min(RETRY_MAX_INTERVAL);
        }
        last_owed = owed;
        if !outs.is_empty() {
            persist_then_ship(&env, &core, &storage, &reads, outs);
        }
    }
}

/// Per-transaction liveness bookkeeping kept by [`liveness_loop`]: the last
/// progress fingerprint observed for an un-committed transaction and how many
/// consecutive ticks it has gone without changing. A `tier` records how many
/// full stall windows have elapsed, used to escalate the deterministic recoverer
/// nominee if the previous nominee was itself dead.
#[derive(Clone, Copy)]
struct Liveness {
    /// The last [`AccordCore::progress_fingerprint`] seen for the txn.
    fingerprint: u64,
    /// Consecutive ticks the fingerprint has not advanced.
    stale_ticks: u32,
    /// Escalation tier already attempted (which deterministic nominee fired).
    tier: usize,
}

/// The failure-detector tick: on a periodic `Env` timer, auto-trigger recovery of
/// any transaction this replica has held **un-committed** past a time bound
/// **without progressing** — its coordinator is suspected dead (ADR 0011,
/// failure-detector-triggered recovery).
///
/// This closes the loop the earlier recovery slices left open: recovery ballots
/// make *concurrent* recoveries safe, but nothing yet *declared* a coordinator
/// dead — `recover` was invoked explicitly. Now the driver does it.
///
/// **How a slow-but-live coordinator is spared.** The synchronous core exposes a
/// monotone [`AccordCore::progress_fingerprint`] per transaction (phase +
/// execute_at + dep/ballot summary) that strictly increases whenever the
/// transaction advances. Each tick the driver re-samples it: if it *changed*, the
/// transaction is making progress, so the stall counter resets and recovery is
/// deferred — a coordinator that is merely slow (still exchanging PreAccept/Accept
/// messages) is never recovered. Only a transaction stuck at the *same*
/// fingerprint for [`LIVENESS_STALL_TICKS`] consecutive ticks (the bound) is
/// suspected stranded.
///
/// **How duels are kept rare.** When the bound trips, the driver does **not**
/// always self-recover: it asks the core whether *this* node is the deterministic
/// nominee for the transaction at the current escalation tier
/// ([`AccordCore::is_recovery_nominee`] — the lowest-id survivor that is not the
/// dead coordinator at tier 0). So in the common case exactly one node recovers
/// each stranded transaction — no duel. If that nominee is itself dead, the next
/// full stall window promotes the next tier (the next-lowest survivor), until
/// recovery lands. When duels *do* still occur, the **ballot** machinery (in the
/// core) guarantees safety and convergence; this tick only reduces their
/// frequency. An already-committed transaction (or one this node is itself
/// driving) is never recovered: it leaves `uncommitted_txns` / `is_driving`
/// filters it.
///
/// Determinism + liveness discipline are preserved: the timer is an `Env` timer;
/// the core decides *what* is stalled and *who* recovers (no time, no I/O); the
/// driver only times the sampling, drops the lock, then ships via
/// `persist_then_ship` (no lock held across an `.await`).
async fn liveness_loop<E: Env>(
    env: E,
    core: Arc<Mutex<AccordCore>>,
    storage: MemoryEngine,
    reads: ReadResults,
) {
    // Per-txn stall tracking. A txn drops out once it commits (no longer in
    // `uncommitted_txns`), so this stays bounded by the in-flight set.
    let mut tracked: BTreeMap<TxnId, Liveness> = BTreeMap::new();
    loop {
        env.sleep(LIVENESS_INTERVAL).await;
        // Decide (under the lock) which stranded txns this node should recover,
        // then drop the lock before any I/O.
        let to_recover: Vec<TxnId> = {
            let c = core.lock().expect("accord core poisoned");
            let uncommitted: BTreeSet<TxnId> = c.uncommitted_txns().into_iter().collect();
            // Forget txns that have committed (or vanished) since last tick.
            tracked.retain(|txn, _| uncommitted.contains(txn));

            let mut due = Vec::new();
            for &txn in &uncommitted {
                // A txn this node is itself coordinating/recovering is driven by
                // its own retry tick; never self-recover it.
                if c.is_driving(txn) {
                    tracked.remove(&txn);
                    continue;
                }
                let Some(fp) = c.progress_fingerprint(txn) else {
                    continue;
                };
                let entry = tracked.entry(txn).or_insert(Liveness {
                    fingerprint: fp,
                    stale_ticks: 0,
                    tier: 0,
                });
                if fp != entry.fingerprint {
                    // Progress since last tick: reset the stall counter and the
                    // fingerprint (a slow-but-live coordinator is spared).
                    entry.fingerprint = fp;
                    entry.stale_ticks = 0;
                    continue;
                }
                entry.stale_ticks += 1;
                if entry.stale_ticks >= LIVENESS_STALL_TICKS {
                    // The bound tripped with no progress: suspect the coordinator
                    // dead. Recover only if this node is the deterministic nominee
                    // at the current escalation tier (keeps duels rare).
                    if c.is_recovery_nominee(txn, entry.tier) {
                        due.push(txn);
                    }
                    // Reset the window and promote the tier so, if this nominee's
                    // recovery does not take (e.g. the nominee was itself
                    // partitioned and `recover` shipped into a void), the next
                    // window promotes the next-lowest survivor.
                    entry.stale_ticks = 0;
                    entry.tier += 1;
                }
            }
            due
        };

        for txn in to_recover {
            // `recover` mints a fresh ballot (above any it has promised) and ships
            // the `Recover` burst; ballots keep a concurrent recovery safe.
            let outs = {
                let mut c = core.lock().expect("accord core poisoned");
                c.recover(txn)
            };
            if !outs.is_empty() {
                persist_then_ship(&env, &core, &storage, &reads, outs);
            }
        }
    }
}
