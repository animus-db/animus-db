//! The `Env`-driven Accord node: a thin driver that owns the environment and
//! ferries messages between the network and the synchronous [`AccordCore`].
//!
//! Mirrors `custos-control`'s `RaftNode`: all consensus logic lives in the sync
//! core; this driver only does I/O. Unlike Raft there are **no perpetual
//! timers** in this slice (timestamps are logical and there is no leader to
//! heartbeat), so the driver is a plain `recv` loop. Submitting a transaction
//! ships its initial `PreAccept` burst out-of-band.
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

use custos_env::{Env, EnvExt, NodeId};
use custos_storage::{MemoryEngine, StorageEngine};

use crate::core::{AccordCore, ApplyEffect, Decision, Key, ReadEffect, TxnId};
use crate::message::{AccordMsg, Out};
use crate::persist::PersistedState;
use crate::timestamp::Timestamp;

/// File name of the per-node Accord write-ahead log on the `Env` disk.
const WAL: &str = "accord.wal";

/// The per-node store of read-transaction results: for each executed read-only
/// transaction, the writer ([`TxnId`]) observed at each key at the read's
/// execution timestamp (`None` = the key had no committed write before the
/// read). Populated by the driver as it satisfies [`ReadEffect`]s, so the result
/// is available once [`AccordNode::is_applied`] holds for the read txn.
type ReadResults = Arc<Mutex<BTreeMap<TxnId, BTreeMap<Key, Option<TxnId>>>>>;

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
}

impl<E: Env, S: StorageEngine> Clone for AccordNode<E, S> {
    fn clone(&self) -> Self {
        AccordNode {
            env: self.env.clone(),
            core: Arc::clone(&self.core),
            storage: self.storage.clone(),
            reads: Arc::clone(&self.reads),
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
    outs: Vec<Out>,
) {
    let (records, applies, read_effects) = {
        let mut c = core.lock().expect("accord core poisoned");
        (c.drain_persist(), c.drain_apply(), c.drain_reads())
    };
    let env = env.clone();
    let storage = storage.clone();
    let reads = Arc::clone(reads);
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
        for (to, msg) in outs {
            let bytes = serde_json::to_vec(&msg).expect("accord message serializes");
            env.send(to, bytes).await;
        }
    });
}

/// Apply the execution effects to the storage engine. Each effect writes the
/// transaction's id to each key it touches via `merge` (per-key LWW) at the
/// transaction's execution timestamp as the MVCC version — idempotent and
/// commutative, so a re-apply on recovery converges.
async fn apply_all<S: StorageEngine>(storage: &S, applies: &[ApplyEffect]) {
    for effect in applies {
        let value = encode_txn(effect.txn);
        let version = effect.version.logical;
        for &key in &effect.keys {
            storage
                .merge(&storage_key(key), &value, version)
                .await
                .expect("storage merge");
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
