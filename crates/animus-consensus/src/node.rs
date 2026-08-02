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

use animus_data::{DataClient, TabletView};
use animus_env::{Env, EnvExt, NodeId};
use animus_storage::{MemoryEngine, StorageEngine};

use crate::core::{AccordCore, ApplyEffect, Decision, Key, ReadEffect, TxnId};
use crate::message::{AccordMsg, Out};
use crate::persist::PersistedState;
use crate::timestamp::Timestamp;

/// File name of the per-node Accord write-ahead log on the `Env` disk.
const WAL: &str = "accord.wal";

/// How often the driver's retry tick fires to re-send un-acknowledged protocol
/// messages for in-flight rounds (ADR 0011, message retry). The network is
/// fire-and-forget and may drop, so without this a dropped message strands a
/// transaction. Chosen well above the simulator's default link delay so a retry
/// only fires when a message was genuinely lost, not merely in flight.
const RETRY_INTERVAL: Duration = Duration::from_millis(200);

/// Timeout for a single data-plane quorum write/read issued by the execution
/// effect when the node is wired to the **live data plane** (the "frontier"
/// path). Generous so a transient drop is absorbed by the data plane's own
/// retry-on-next-anti-entropy convergence rather than failing the apply.
const DATA_TIMEOUT: Duration = Duration::from_secs(2);

/// The per-node store of read-transaction results: for each executed read-only
/// transaction, the writer ([`TxnId`]) observed at each key at the read's
/// execution timestamp (`None` = the key had no committed write before the
/// read). Populated by the driver as it satisfies [`ReadEffect`]s, so the result
/// is available once [`AccordNode::is_applied`] holds for the read txn.
type ReadResults = Arc<Mutex<BTreeMap<TxnId, BTreeMap<Key, Option<TxnId>>>>>;

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
struct DataSink<E: Env> {
    client: DataClient<E>,
    view: TabletView,
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
            view,
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

    /// Whether this replica has executed `txn`.
    pub fn is_applied(&self, txn: TxnId) -> bool {
        self.lock().is_applied(txn)
    }

    /// The decisions this node has reached as a coordinator, in order.
    pub fn decisions(&self) -> Vec<Decision> {
        self.lock().decisions().to_vec()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, AccordCore> {
        self.core.lock().expect("accord core poisoned")
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
        satisfy_reads(&storage, &reads, &read_effects).await;
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
        let value = encode_txn(effect.txn);
        let version = effect.version.logical;
        for &key in &effect.keys {
            storage
                .merge(&storage_key(key), &value, version)
                .await
                .expect("storage merge");
            if let Some(sink) = sink {
                // Land the committed write in the replicated data plane in agreed
                // order. The version is the execution timestamp, strictly
                // increasing in the total order, so the data plane's per-key LWW
                // keeps the same winner everywhere. Fire-and-await: the result is
                // not asserted here (a transient quorum miss is reconciled by the
                // data plane's own anti-entropy), the test verifies via a quorum
                // read.
                let _ = sink
                    .client
                    .write(&sink.view, &storage_key(key), &value, version, DATA_TIMEOUT)
                    .await;
            }
        }
    }
}

/// Satisfy read-only transactions: for each [`ReadEffect`], read every key it
/// touches **as of** the read's execution timestamp (`get_at`) — so the read
/// observes exactly the writes that executed before it (lower MVCC version) and
/// none after — decode the observed writer id, and record the per-key result in
/// the shared [`ReadResults`] under the read txn's id.
async fn satisfy_reads<S: StorageEngine>(
    storage: &S,
    reads: &ReadResults,
    read_effects: &[ReadEffect],
) {
    for effect in read_effects {
        let version = effect.version.logical;
        let mut observed: BTreeMap<Key, Option<TxnId>> = BTreeMap::new();
        for &key in &effect.keys {
            let writer = storage
                .get_at(&storage_key(key), version)
                .await
                .expect("storage get_at")
                .and_then(|vv| decode_txn(&vv.value));
            observed.insert(key, writer);
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
        // so there is no need to re-push them on every restart.
        apply_all::<E, S>(&storage, None, &applies).await;
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
        persist_then_ship(&env, &core, &storage, &reads, &sink, outs);
    }
}

/// The retry tick: on a periodic `Env` timer, re-send the outbound messages for
/// every in-flight round that has not completed (ADR 0011, message retry).
///
/// `Network::send` is fire-and-forget and may drop, which would otherwise strand
/// a transaction. The synchronous core decides *what* is still owed and *to
/// whom* ([`AccordCore::resend_pending`]); this driver only sleeps on the timer,
/// drains, and ships — so determinism stays in the core and no lock is held
/// across an `.await`. A completed round emits nothing, so retries stop on their
/// own. We route through `persist_then_ship` so any incidental durable records or
/// effects drain too (in steady state there are none on a pure retry).
async fn retry_loop<E: Env, S: StorageEngine + 'static>(
    env: E,
    core: Arc<Mutex<AccordCore>>,
    storage: S,
    reads: ReadResults,
    sink: Option<Arc<DataSink<E>>>,
) {
    loop {
        env.sleep(RETRY_INTERVAL).await;
        let outs = {
            let mut c = core.lock().expect("accord core poisoned");
            c.resend_pending()
        };
        if !outs.is_empty() {
            persist_then_ship(&env, &core, &storage, &reads, &sink, outs);
        }
    }
}
