//! The `Env`-driven Accord node: a thin driver that owns the environment and
//! ferries messages between the network and the synchronous [`AccordCore`].
//!
//! Mirrors `custos-control`'s `RaftNode`: all consensus logic lives in the sync
//! core; this driver only does I/O. Unlike Raft there are **no perpetual
//! timers** in this slice (timestamps are logical and there is no leader to
//! heartbeat), so the driver is a plain `recv` loop. Submitting a transaction
//! ships its initial `PreAccept` burst out-of-band.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use custos_env::{Env, EnvExt, NodeId};

use crate::core::{AccordCore, Decision, Key, TxnId};
use crate::message::{AccordMsg, Out};
use crate::timestamp::Timestamp;

/// A running consensus replica. Cheap to clone; clones share one [`AccordCore`].
#[derive(Clone)]
pub struct AccordNode<E: Env> {
    env: E,
    core: Arc<Mutex<AccordCore>>,
}

impl<E: Env> AccordNode<E> {
    /// Start a node: build its [`AccordCore`] and spawn the driver loop on `env`.
    /// `all_nodes` is the full replica set (including this node).
    pub fn start(env: E, all_nodes: Vec<NodeId>) -> AccordNode<E> {
        let core = Arc::new(Mutex::new(AccordCore::new(env.node_id(), &all_nodes)));
        let node = AccordNode {
            env: env.clone(),
            core: Arc::clone(&core),
        };
        env.spawn_task(drive(env.clone(), Arc::clone(&core)));
        node
    }

    /// Submit a new transaction over `keys` for this node to coordinate. Mints
    /// `t0`, ships the `PreAccept` burst, and returns the transaction id.
    pub fn submit(&self, keys: BTreeSet<Key>) -> TxnId {
        let (txn, outs) = self.lock().submit(keys);
        ship(&self.env, outs);
        txn
    }

    /// This node's environment handle.
    pub fn env(&self) -> &E {
        &self.env
    }

    /// The committed execution timestamp this replica recorded for `txn`, if it
    /// has reached the committed phase here.
    pub fn committed_execute_at(&self, txn: TxnId) -> Option<Timestamp> {
        self.lock().committed_execute_at(txn)
    }

    /// The dependencies this replica recorded for `txn` at commit, if committed.
    pub fn committed_deps(&self, txn: TxnId) -> Option<BTreeSet<TxnId>> {
        self.lock().committed_deps(txn)
    }

    /// The decisions this node has reached as a coordinator, in order.
    pub fn decisions(&self) -> Vec<Decision> {
        self.lock().decisions().to_vec()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, AccordCore> {
        self.core.lock().expect("accord core poisoned")
    }
}

/// Ship a batch of outbound messages over the env, in order.
fn ship<E: Env>(env: &E, outs: Vec<Out>) {
    for (to, msg) in outs {
        let bytes = serde_json::to_vec(&msg).expect("accord message serializes");
        let env = env.clone();
        // `send` is async; spawn it so `submit`/the recv loop stay synchronous at
        // their call sites (the simulator runs the send to completion promptly).
        env.clone()
            .spawn_task(async move { env.send(to, bytes).await });
    }
}

/// The per-node driver loop: wait for the next message, hand it to the core, and
/// ship whatever the core wants sent.
async fn drive<E: Env>(env: E, core: Arc<Mutex<AccordCore>>) {
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
        for (to, msg) in outs {
            let bytes = serde_json::to_vec(&msg).expect("accord message serializes");
            env.send(to, bytes).await;
        }
    }
}
