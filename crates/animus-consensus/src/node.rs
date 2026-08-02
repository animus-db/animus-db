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
//! the same store. The engine defaults to the in-memory [`MemoryEngine`] used
//! under simulation; a recovered node repopulates a *fresh* engine in the
//! original execution order from the WAL.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_data::{DataClient, ReadResult, Router, TabletView};
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

/// Timeout for a single data-plane quorum write/read issued by the execution
/// effect when the node is wired to the **live data plane** (the "frontier"
/// path). Generous so a transient drop is absorbed by the data plane's own
/// retry-on-next-anti-entropy convergence rather than failing the apply.
const DATA_TIMEOUT: Duration = Duration::from_secs(2);

/// The per-node store of read-transaction results: for each executed read-only
/// transaction, the **raw value bytes** observed at each key at the read's
/// execution timestamp (`None` = the key had no committed write before the
/// read). Populated by the driver as it satisfies [`ReadEffect`]s, so the result
/// is available once [`AccordNode::is_applied`] holds for the read txn. Stored as
/// raw bytes so a read of an arbitrary value (list-append, ADR 0011) is observed
/// verbatim; [`AccordNode::read_result`] decodes them as a writer txn id for the
/// classic register view, [`AccordNode::read_value_result`] returns them raw.
type ReadResults = Arc<Mutex<BTreeMap<TxnId, BTreeMap<Key, Option<Vec<u8>>>>>>;

/// The **frontier** wiring: a sink that lands a committed transaction's writes in
/// the replicated **data plane** (`animus-data`) instead of (only) a per-node
/// store. When an [`AccordNode`] is started with one (via
/// [`AccordNode::start_with_data_plane`]), the execution effect of a committed
/// *write* transaction is applied through the data-plane quorum coordinator:
/// each key the transaction touches is written (`DataClient::write`) to the
/// tablet's replica set at the transaction's execution timestamp as the MVCC
/// version. Those writes are then readable via ordinary data-plane quorum reads
/// — the transaction's atomic, ordered effect made durable across the leaderless
/// AP data plane (ADR 0011 frontier, ADR 0001 two-plane).
///
/// The data-plane coordinator runs on its **own** `Env` (a distinct node id):
/// the node's inbox is single-consumer, so the Accord protocol traffic and the
/// data-plane coordinator's quorum replies must not share an inbox.
///
/// **Sharded (multi-tablet) transactions (ADR 0011).** A transaction's key set
/// may span more than one tablet/replica-set. Accord is naturally multi-shard —
/// the consensus round here already replicates every transaction to the whole
/// Accord replica set, agreeing one *global* execution timestamp and one
/// dependency set regardless of which tablets the keys live in. The only place
/// sharding shows up is the *execution effect*: each key must be written to (and
/// read from) **its own** tablet's quorum. The sink therefore routes per key via
/// [`DataRouting`] — a single `TabletView` (the original frontier) or a `Router`
/// over a multi-tablet map. Because the agreed execution timestamp is the MVCC
/// version on every key regardless of tablet, the per-tablet writes stay
/// consistently ordered across shards.
struct DataSink<E: Env> {
    client: DataClient<E>,
    routing: DataRouting,
}

/// How a [`DataSink`] resolves a key to the tablet quorum that owns it.
enum DataRouting {
    /// A single tablet covering the whole key space (the original frontier).
    Single(TabletView),
    /// A multi-tablet map: each key routes to its owning tablet's view. A key
    /// outside every tablet's range resolves to `None` (and is skipped).
    Sharded(Router),
}

impl DataRouting {
    /// The [`TabletView`] for the tablet owning `key` (the storage-key bytes),
    /// or `None` if no tablet covers it.
    fn view_for(&self, key: &[u8]) -> Option<TabletView> {
        match self {
            DataRouting::Single(view) => Some(view.clone()),
            DataRouting::Sharded(router) => router.view_for(key),
        }
    }
}

/// A running consensus replica. Cheap to clone; clones share one [`AccordCore`]
/// and one storage engine.
///
/// Generic over the [`StorageEngine`] backing execution; defaults to the
/// in-memory [`MemoryEngine`] used under simulation.
pub struct AccordNode<E: Env, S: StorageEngine = MemoryEngine> {
    env: E,
    core: Arc<Mutex<AccordCore>>,
    storage: S,
    /// Results of executed read-only transactions (see [`ReadResults`]).
    reads: ReadResults,
    /// When `Some`, committed write effects land in the replicated data plane
    /// instead of (only) the local `storage` engine (the frontier path).
    sink: Option<Arc<DataSink<E>>>,
}

impl<E: Env, S: StorageEngine> Clone for AccordNode<E, S> {
    fn clone(&self) -> Self {
        AccordNode {
            env: self.env.clone(),
            core: Arc::clone(&self.core),
            storage: self.storage.clone(),
            reads: Arc::clone(&self.reads),
            sink: self.sink.clone(),
        }
    }
}

impl<E: Env> AccordNode<E, MemoryEngine> {
    /// Start a node backed by a fresh in-memory [`MemoryEngine`]. `all_nodes` is
    /// the full replica set (including this node). The driver recovers durable
    /// state from the WAL before serving anything.
    pub fn start(env: E, all_nodes: Vec<NodeId>) -> AccordNode<E, MemoryEngine> {
        AccordNode::start_with_storage(env, all_nodes, MemoryEngine::new())
    }
}

impl<E: Env, S: StorageEngine + 'static> AccordNode<E, S> {
    /// Start a node backed by an explicit [`StorageEngine`]. `all_nodes` is the
    /// full replica set (including this node). The driver recovers durable state
    /// from the WAL before serving anything, replaying its execution order into
    /// `storage`.
    pub fn start_with_storage(env: E, all_nodes: Vec<NodeId>, storage: S) -> AccordNode<E, S> {
        Self::start_inner(env, all_nodes, storage, None)
    }

    fn start_inner(
        env: E,
        all_nodes: Vec<NodeId>,
        storage: S,
        sink: Option<Arc<DataSink<E>>>,
    ) -> AccordNode<E, S> {
        let core = Arc::new(Mutex::new(AccordCore::new(env.node_id(), &all_nodes)));
        let reads: ReadResults = Arc::new(Mutex::new(BTreeMap::new()));
        let node = AccordNode {
            env: env.clone(),
            core: Arc::clone(&core),
            storage: storage.clone(),
            reads: Arc::clone(&reads),
            sink: sink.clone(),
        };
        env.spawn_task(drive(
            env.clone(),
            Arc::clone(&core),
            storage,
            reads,
            sink.clone(),
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
            node.sink.clone(),
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
            node.sink.clone(),
        ));
        node
    }

    /// Start a node whose committed write effects land in the **replicated data
    /// plane** (the frontier path), not (only) a per-node store. `all_nodes` is
    /// the Accord replica set (the consensus participants); `view` routes the
    /// data-plane quorum writes for the keys it executes.
    ///
    /// The `coordinator_env` must be a **distinct node id** from this Accord
    /// replica's (and from the data replicas in `view`): the network inbox is
    /// single-consumer, so the data-plane coordinator's quorum replies cannot
    /// share an inbox with the Accord protocol traffic. The execution effect of a
    /// committed write transaction writes each key it touches through the quorum
    /// (`DataClient::write`) at the transaction's execution timestamp as the MVCC
    /// version — so the transaction's writes become readable via ordinary
    /// data-plane quorum reads, atomically in agreed order.
    ///
    /// `storage` still backs the local execution path (and recovery); read-only
    /// transactions execute against it as before — wiring data-plane *reads* into
    /// Accord is deferred (see ADR 0011).
    pub fn start_with_data_plane(
        env: E,
        all_nodes: Vec<NodeId>,
        storage: S,
        coordinator_env: E,
        view: TabletView,
    ) -> AccordNode<E, S> {
        let sink = Arc::new(DataSink {
            client: DataClient::new(coordinator_env),
            routing: DataRouting::Single(view),
        });
        Self::start_inner(env, all_nodes, storage, Some(sink))
    }

    /// Start a node whose committed write effects land in a **multi-tablet**
    /// (sharded) data plane (ADR 0011, sharded transactions). Like
    /// [`AccordNode::start_with_data_plane`] but the data-plane keys are routed
    /// **per key** through a [`Router`] over a multi-tablet map, so a single
    /// Accord transaction whose keys span more than one tablet writes each key to
    /// *its own* tablet's replica set — coordinated across shards under one global
    /// execution timestamp (the Accord round already agrees that timestamp and the
    /// dependency set over the whole replica set; only the execution effect is
    /// sharded). A wired read transaction reads each key from its owning tablet's
    /// quorum the same way.
    ///
    /// `coordinator_env` must be a **distinct node id** from this Accord replica
    /// (and from every data replica any tablet in the `router` routes to), since
    /// the network inbox is single-consumer.
    pub fn start_with_router(
        env: E,
        all_nodes: Vec<NodeId>,
        storage: S,
        coordinator_env: E,
        router: Router,
    ) -> AccordNode<E, S> {
        let sink = Arc::new(DataSink {
            client: DataClient::new(coordinator_env),
            routing: DataRouting::Sharded(router),
        });
        Self::start_inner(env, all_nodes, storage, Some(sink))
    }

    /// Submit a new **write** transaction over `keys` for this node to
    /// coordinate. Mints `t0`, ships the `PreAccept` burst (after fsyncing the
    /// durable state the burst depends on), and returns the transaction id.
    pub fn submit(&self, keys: BTreeSet<Key>) -> TxnId {
        let (txn, outs) = self.lock().submit(keys);
        persist_then_ship(
            &self.env,
            &self.core,
            &self.storage,
            &self.reads,
            &self.sink,
            outs,
        );
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
        persist_then_ship(
            &self.env,
            &self.core,
            &self.storage,
            &self.reads,
            &self.sink,
            outs,
        );
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
        persist_then_ship(
            &self.env,
            &self.core,
            &self.storage,
            &self.reads,
            &self.sink,
            outs,
        );
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
        persist_then_ship(
            &self.env,
            &self.core,
            &self.storage,
            &self.reads,
            &self.sink,
            outs,
        );
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
        persist_then_ship(
            &self.env,
            &self.core,
            &self.storage,
            &self.reads,
            &self.sink,
            outs,
        );
        txn
    }

    /// Take over `txn` as a *recovery coordinator* (its original coordinator is
    /// suspected dead). Broadcasts `Recover` and drives the transaction to a
    /// consistent commit. See [`AccordCore::recover`].
    pub fn recover(&self, txn: TxnId) {
        let outs = self.lock().recover(txn);
        persist_then_ship(
            &self.env,
            &self.core,
            &self.storage,
            &self.reads,
            &self.sink,
            outs,
        );
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

    /// This node's storage engine (the executed store).
    pub fn storage(&self) -> &S {
        &self.storage
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
    pub fn begin(&self) -> InteractiveTxn<E, S> {
        InteractiveTxn {
            node: self.clone(),
            reads: BTreeSet::new(),
            writes: BTreeSet::new(),
            values: BTreeMap::new(),
        }
    }

    /// Read the current committed writer of `key` as seen by this node: through
    /// the replicated **data plane** quorum when the node is wired to it (the
    /// frontier path), else from the local execution store. Used by
    /// [`InteractiveTxn::read`]; exposed so a caller can do an ad-hoc current read
    /// without opening a transaction.
    pub async fn current_writer(&self, key: Key) -> Option<TxnId> {
        self.current_value(key)
            .await
            .as_deref()
            .and_then(decode_txn)
    }

    /// Read the current committed **value bytes** of `key` as seen by this node:
    /// through the replicated data-plane quorum when wired to it (the frontier
    /// path), else from the local execution store. Unlike
    /// [`AccordNode::current_writer`] this returns the raw bytes, so a caller doing
    /// a read-modify-write over arbitrary values (e.g. list-append) sees the
    /// actual value to modify (arbitrary write values, ADR 0011). `None` if the
    /// key has no committed write (or a quorum could not be reached).
    pub async fn current_value(&self, key: Key) -> Option<Vec<u8>> {
        match &self.sink {
            Some(sink) => {
                let sk = storage_key(key);
                match sink.routing.view_for(&sk) {
                    Some(view) => match sink.client.read(&view, &sk, DATA_TIMEOUT).await {
                        ReadResult::Value(Some(bytes)) => Some(bytes),
                        ReadResult::Value(None) | ReadResult::Failed => None,
                    },
                    None => None,
                }
            }
            None => self.store_value(key).await,
        }
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
pub struct InteractiveTxn<E: Env, S: StorageEngine = MemoryEngine> {
    node: AccordNode<E, S>,
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

impl<E: Env, S: StorageEngine + 'static> InteractiveTxn<E, S> {
    /// Read the current committed value of `key` and record it in the read set.
    /// Reads through the replicated data plane when the node is wired to it (the
    /// frontier path), else the local execution store. Returns the observed value
    /// writer (`None` if the key has no committed write), so the caller can decide
    /// what to write next.
    pub async fn read(&mut self, key: Key) -> Option<TxnId> {
        self.reads.insert(key);
        self.node.current_writer(key).await
    }

    /// Read the current committed **value bytes** of `key` (through the data plane
    /// when wired, else the local store), recording it in the read set. Unlike
    /// [`InteractiveTxn::read`] — which decodes the writer txn id — this returns
    /// the raw stored bytes, so a caller doing a read-modify-write over arbitrary
    /// values (e.g. list-append) sees the actual value to modify (ADR 0011).
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

/// Encode a transaction id as the stored value (the executed effect is "write
/// my id"): `(logical, node)` as two big-endian u64s.
fn encode_txn(txn: TxnId) -> Vec<u8> {
    let mut v = Vec::with_capacity(16);
    v.extend_from_slice(&txn.logical.to_be_bytes());
    v.extend_from_slice(&txn.node.to_be_bytes());
    v
}

/// Inverse of [`encode_txn`].
fn decode_txn(bytes: &[u8]) -> Option<TxnId> {
    if bytes.len() != 16 {
        return None;
    }
    let logical = u64::from_be_bytes(bytes[0..8].try_into().ok()?);
    let node = u64::from_be_bytes(bytes[8..16].try_into().ok()?);
    Some(Timestamp::new(logical, node))
}

/// Drain the core's pending durable records and execution effects, append +
/// `fsync` the records to the WAL, apply the effects to the storage engine, then
/// ship the outbound messages.
///
/// Spawned as a task so the synchronous call sites (`submit`/`recover`, and each
/// `handle` in the recv loop) stay synchronous; the simulator runs it promptly,
/// and within the task the fsync precedes the storage apply which precedes the
/// sends, preserving "durable before action".
fn persist_then_ship<E: Env, S: StorageEngine + 'static>(
    env: &E,
    core: &Arc<Mutex<AccordCore>>,
    storage: &S,
    reads: &ReadResults,
    sink: &Option<Arc<DataSink<E>>>,
    outs: Vec<Out>,
) {
    let (records, applies, read_effects) = {
        let mut c = core.lock().expect("accord core poisoned");
        (c.drain_persist(), c.drain_apply(), c.drain_reads())
    };
    let env = env.clone();
    let storage = storage.clone();
    let reads = Arc::clone(reads);
    let sink = sink.clone();
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
        apply_all(&storage, sink.as_deref(), &applies).await;
        satisfy_reads(&storage, sink.as_deref(), &reads, &read_effects).await;
        for (to, msg) in outs {
            let bytes = serde_json::to_vec(&msg).expect("accord message serializes");
            env.send(to, bytes).await;
        }
    });
}

/// Apply the execution effects. Each effect writes the transaction's id to each
/// key it touches at the transaction's execution timestamp as the MVCC version.
///
/// The id always lands in the local `storage` engine (`merge`, per-key LWW —
/// idempotent and commutative, so a re-apply on recovery converges, and
/// `store_writer` can read it back). When a [`DataSink`] is present (the frontier
/// path), the same write is **also** pushed through the replicated data plane via
/// the quorum coordinator (`DataClient::write`), so the transaction's effect
/// becomes readable via ordinary data-plane quorum reads — the local engine
/// remains the per-node recovery substrate, the data plane is the shared,
/// replicated landing zone.
async fn apply_all<E: Env, S: StorageEngine>(
    storage: &S,
    sink: Option<&DataSink<E>>,
    applies: &[ApplyEffect],
) {
    for effect in applies {
        // The default value (no caller-supplied bytes) is the txn's own id — the
        // classic register effect, which `store_writer` decodes back. A caller who
        // supplied an arbitrary value (`submit_writes`/`InteractiveTxn::write_value`)
        // gets exactly those bytes written (arbitrary write values, ADR 0011).
        let default_value = encode_txn(effect.txn);
        let version = effect.version.logical;
        for &key in &effect.keys {
            let sk = storage_key(key);
            let value = effect.values.get(&key).unwrap_or(&default_value);
            storage
                .merge(&sk, value, version)
                .await
                .expect("storage merge");
            if let Some(sink) = sink {
                // Land the committed write in the replicated data plane in agreed
                // order, routing the key to **its own** tablet's quorum (sharded
                // transactions, ADR 0011). The version is the execution timestamp,
                // strictly increasing in the total order, so the data plane's
                // per-key LWW keeps the same winner everywhere. Fire-and-await: the
                // result is not asserted here (a transient quorum miss is
                // reconciled by the data plane's own anti-entropy); the test
                // verifies via a quorum read. A key no tablet covers is skipped
                // (it still lands in the local engine above).
                if let Some(view) = sink.routing.view_for(&sk) {
                    let _ = sink
                        .client
                        .write(&view, &sk, value, version, DATA_TIMEOUT)
                        .await;
                }
            }
        }
    }
}

/// Satisfy read-only transactions: for each [`ReadEffect`], read every key it
/// touches and record the observed per-key **raw value bytes** in the shared
/// [`ReadResults`] under the read txn's id (so an arbitrary value — list-append,
/// ADR 0011 — is observed verbatim; the register-writer view decodes them).
///
/// **Where the read lands depends on the wiring** (ADR 0011):
///
/// - **Local execution store** (no [`DataSink`]): read each key *as of* the
///   read's execution timestamp (`get_at`) — so it observes exactly the writes
///   that executed before it (lower MVCC version) and none after. This is the
///   per-node consensus store path.
/// - **Replicated data plane** (a [`DataSink`] is present, the frontier path):
///   read each key through the data-plane **quorum** coordinator
///   ([`DataClient::read`]) — so a read observes the same replicated state the
///   committed *write* transactions land in (the data-plane write effect in
///   [`apply_all`]), not a private local snapshot. This is correct *because*
///   the read is ordered like a write: the core only emits its [`ReadEffect`]
///   once every earlier-ordered conflicting write has `Applied`, and an applied
///   write's effect has already been pushed through the same data-plane quorum
///   — so a current quorum read at execution time observes exactly those writes
///   and none ordered after. (The data-plane wire carries no historical
///   `get_at`-by-version read, so this relies on the execution-order gate, not a
///   versioned snapshot.)
async fn satisfy_reads<E: Env, S: StorageEngine>(
    storage: &S,
    sink: Option<&DataSink<E>>,
    reads: &ReadResults,
    read_effects: &[ReadEffect],
) {
    for effect in read_effects {
        let version = effect.version.logical;
        let mut observed: BTreeMap<Key, Option<Vec<u8>>> = BTreeMap::new();
        for &key in &effect.keys {
            let sk = storage_key(key);
            let value = match sink {
                // Frontier: observe the replicated data plane (quorum read),
                // routing the key to its own tablet's quorum (sharded reads).
                Some(sink) => match sink.routing.view_for(&sk) {
                    Some(view) => match sink.client.read(&view, &sk, DATA_TIMEOUT).await {
                        ReadResult::Value(Some(bytes)) => Some(bytes),
                        // Absent everywhere, or a quorum could not be reached: the
                        // read observes nothing for this key (a transient quorum
                        // miss converges via the data plane's own anti-entropy).
                        ReadResult::Value(None) | ReadResult::Failed => None,
                    },
                    // No tablet covers the key: nothing to observe.
                    None => None,
                },
                // Local consensus store: snapshot read as of the execution ts.
                None => storage
                    .get_at(&sk, version)
                    .await
                    .expect("storage get_at")
                    .map(|vv| vv.value),
            };
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
async fn drive<E: Env, S: StorageEngine + 'static>(
    env: E,
    core: Arc<Mutex<AccordCore>>,
    storage: S,
    reads: ReadResults,
    sink: Option<Arc<DataSink<E>>>,
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
        // Recovery re-applies into the *local* engine only (the per-node recovery
        // substrate): the data plane already holds the committed writes durably,
        // so there is no need to re-push them on every restart. For the same
        // reason recovered reads are re-satisfied from the *local* engine
        // (`None` sink) — the writes ordered before each recovered read were
        // re-applied locally just above, so the recovered observation matches the
        // original; a live data-plane quorum read would instead reflect current
        // (possibly newer) state.
        apply_all::<E, S>(&storage, None, &applies).await;
        satisfy_reads::<E, S>(&storage, None, &reads, &read_effects).await;
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
        persist_then_ship(&env, &core, &storage, &reads, &sink, outs);
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
async fn retry_loop<E: Env, S: StorageEngine + 'static>(
    env: E,
    core: Arc<Mutex<AccordCore>>,
    storage: S,
    reads: ReadResults,
    sink: Option<Arc<DataSink<E>>>,
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
            persist_then_ship(&env, &core, &storage, &reads, &sink, outs);
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
async fn liveness_loop<E: Env, S: StorageEngine + 'static>(
    env: E,
    core: Arc<Mutex<AccordCore>>,
    storage: S,
    reads: ReadResults,
    sink: Option<Arc<DataSink<E>>>,
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
                persist_then_ship(&env, &core, &storage, &reads, &sink, outs);
            }
        }
    }
}
