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
//! [`WalRecord`]s as it advances a transaction's phase; the driver drains them,
//! appends them to the per-node WAL on the `Env` disk, and `fsync`s **before**
//! shipping the outbound messages that depend on them (a PreAcceptOk a peer
//! quorum will count, a Commit a peer will execute on). On startup the driver
//! replays the WAL and recovers the core, so a restarted replica keeps every
//! committed/executed transaction. This is the exact shape of `RaftNode`'s WAL
//! handling, minus the snapshot/log-truncation (deferred — see the crate guide).

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use custos_env::{Env, EnvExt, NodeId};

use crate::core::{AccordCore, Decision, Key, TxnId};
use crate::message::{AccordMsg, Out};
use crate::persist::PersistedState;
use crate::timestamp::Timestamp;

/// File name of the per-node Accord write-ahead log on the `Env` disk.
const WAL: &str = "accord.wal";

/// A running consensus replica. Cheap to clone; clones share one [`AccordCore`].
#[derive(Clone)]
pub struct AccordNode<E: Env> {
    env: E,
    core: Arc<Mutex<AccordCore>>,
}

impl<E: Env> AccordNode<E> {
    /// Start a node: build its [`AccordCore`] and spawn the driver loop on `env`.
    /// `all_nodes` is the full replica set (including this node). The driver
    /// recovers durable state from the WAL before serving anything.
    pub fn start(env: E, all_nodes: Vec<NodeId>) -> AccordNode<E> {
        let core = Arc::new(Mutex::new(AccordCore::new(env.node_id(), &all_nodes)));
        let node = AccordNode {
            env: env.clone(),
            core: Arc::clone(&core),
        };
        env.spawn_task(drive(env.clone(), Arc::clone(&core), all_nodes));
        node
    }

    /// Submit a new transaction over `keys` for this node to coordinate. Mints
    /// `t0`, ships the `PreAccept` burst (after fsyncing the durable state the
    /// burst depends on), and returns the transaction id.
    pub fn submit(&self, keys: BTreeSet<Key>) -> TxnId {
        let (txn, outs) = self.lock().submit(keys);
        persist_then_ship(&self.env, &self.core, outs);
        txn
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

    /// The last transaction that wrote `key` in this replica's executed store.
    pub fn store_writer(&self, key: Key) -> Option<TxnId> {
        self.lock().store_writer(key)
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

/// Drain the core's pending durable records, append + `fsync` them to the WAL,
/// then ship the outbound messages. Spawned as a task so the synchronous call
/// sites (`submit`, and each `handle` in the recv loop) stay synchronous; the
/// simulator runs it promptly, and the fsync completes before the sends inside
/// the same task, preserving "durable before action".
fn persist_then_ship<E: Env>(env: &E, core: &Arc<Mutex<AccordCore>>, outs: Vec<Out>) {
    let records = core.lock().expect("accord core poisoned").drain_persist();
    let env = env.clone();
    env.clone().spawn_task(async move {
        for record in &records {
            env.append(WAL, &PersistedState::encode_record(record))
                .await
                .expect("wal append");
        }
        if !records.is_empty() {
            env.sync(WAL).await.expect("wal sync");
        }
        for (to, msg) in outs {
            let bytes = serde_json::to_vec(&msg).expect("accord message serializes");
            env.send(to, bytes).await;
        }
    });
}

/// The per-node driver loop: recover durable state, then repeatedly wait for the
/// next message, hand it to the core, persist the resulting durable changes, and
/// ship whatever the core wants sent.
async fn drive<E: Env>(env: E, core: Arc<Mutex<AccordCore>>, all_nodes: Vec<NodeId>) {
    // Recover from the WAL before serving anything.
    let bytes = env.read(WAL).await.unwrap_or_default();
    let state = PersistedState::replay(PersistedState::decode(&bytes));
    if !state.is_empty() {
        let recovered = AccordCore::recovered(env.node_id(), &all_nodes, state);
        *core.lock().expect("accord core poisoned") = recovered;
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
        // we just executed) before shipping the messages that depend on them.
        persist_then_ship(&env, &core, outs);
    }
}
