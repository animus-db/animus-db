//! The `Env`-driven EPaxos node: a thin driver that owns the environment and
//! ferries messages between the network and the synchronous [`EPaxosCore`].
//!
//! Mirrors `animus-consensus`'s `AccordNode` and the control plane's `RaftNode`:
//! all consensus logic lives in the sync core; this driver only does I/O. It is a
//! `recv` loop that decodes an inbound [`EPaxosMsg`], steps the core, and ships
//! the outbound messages — after making the core's durable [`WalRecord`]s durable.
//!
//! **Durability before action.** The core accumulates [`WalRecord`]s as it
//! advances an instance's phase; the driver drains them, appends them to the
//! per-node WAL on the `Env` disk, and `fsync`s **before** shipping the outbound
//! messages that depend on them (a `PreAcceptOk` a peer quorum will count, a
//! `Commit` a peer will act on). On startup the driver replays the WAL and
//! recovers the replica view, so a restarted node keeps every committed instance.
//!
//! **Skeleton scope.** There is no retry timer, failure detector, or execution
//! loop yet (all present in `animus-consensus`); a dropped message can therefore
//! strand an instance and committed commands are agreed but not yet *executed*.
//! Because there is no perpetual timer, tests may drive with `run_for` (the
//! pattern the rest of the project uses for protocols that will grow timers).

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, MutexGuard};

use animus_env::{Env, EnvExt};

use crate::core::{Decision, EPaxosCore, Key, Status};
use crate::instance::InstanceId;
use crate::message::{EPaxosMsg, Out};
use crate::persist::PersistedState;

/// File name of the per-node EPaxos write-ahead log on the `Env` disk.
const WAL: &str = "epaxos.wal";

/// A running EPaxos replica. Cheap to clone; clones share one [`EPaxosCore`].
pub struct EPaxosNode<E: Env> {
    env: E,
    core: Arc<Mutex<EPaxosCore>>,
}

impl<E: Env> Clone for EPaxosNode<E> {
    fn clone(&self) -> Self {
        EPaxosNode {
            env: self.env.clone(),
            core: Arc::clone(&self.core),
        }
    }
}

impl<E: Env> EPaxosNode<E> {
    /// Start a node in the `all_nodes` replica set (including this node). The
    /// driver recovers durable state from the WAL before serving anything.
    pub fn start(env: E, all_nodes: Vec<u64>) -> EPaxosNode<E> {
        let core = Arc::new(Mutex::new(EPaxosCore::new(env.node_id(), &all_nodes)));
        let node = EPaxosNode {
            env: env.clone(),
            core: Arc::clone(&core),
        };
        env.spawn_task(drive(env.clone(), Arc::clone(&core)));
        node
    }

    /// Submit a new command over `keys` for this node to coordinate. Ships the
    /// `PreAccept` burst (after fsyncing the durable state it depends on) and
    /// returns the instance id.
    pub fn submit(&self, keys: BTreeSet<Key>) -> InstanceId {
        let (instance, outs) = self.lock().submit(keys);
        persist_then_ship(&self.env, &self.core, outs);
        instance
    }

    /// This node's environment handle.
    pub fn env(&self) -> &E {
        &self.env
    }

    /// The decisions this node reached as command leader (observability).
    #[must_use]
    pub fn decisions(&self) -> Vec<Decision> {
        self.lock().decisions().to_vec()
    }

    /// The agreed `(seq, deps)` this replica recorded for `instance`, if committed.
    #[must_use]
    pub fn committed_attrs(&self, instance: InstanceId) -> Option<(u64, BTreeSet<InstanceId>)> {
        self.lock().committed_attrs(instance)
    }

    /// The agreed dependency set this replica recorded for `instance`, if committed.
    #[must_use]
    pub fn committed_deps(&self, instance: InstanceId) -> Option<BTreeSet<InstanceId>> {
        self.lock().committed_attrs(instance).map(|(_, deps)| deps)
    }

    /// Whether this replica has committed `instance`.
    #[must_use]
    pub fn is_committed(&self, instance: InstanceId) -> bool {
        self.lock().status(instance) >= Status::Committed
    }

    fn lock(&self) -> MutexGuard<'_, EPaxosCore> {
        self.core.lock().expect("epaxos core poisoned")
    }
}

/// Drain the core's durable records, fsync them, **then** ship the outbound
/// messages — durable before action. All I/O happens in a spawned task with no
/// lock held across an `.await` (the deadlock discipline every driver here keeps).
fn persist_then_ship<E: Env>(env: &E, core: &Arc<Mutex<EPaxosCore>>, outs: Vec<Out>) {
    let records = core.lock().expect("epaxos core poisoned").drain_persist();
    if records.is_empty() && outs.is_empty() {
        return;
    }
    let env = env.clone();
    env.clone().spawn_task(async move {
        if !records.is_empty() {
            let mut buf = Vec::new();
            for record in &records {
                buf.extend_from_slice(&PersistedState::encode_record(record));
            }
            env.append(WAL, &buf).await.expect("wal append");
            env.sync(WAL).await.expect("wal sync");
        }
        for (to, msg) in outs {
            let payload = serde_json::to_vec(&msg).expect("epaxos message serializes");
            env.send(to, payload).await;
        }
    });
}

/// The driver loop: recover from the WAL, then step the core for every inbound
/// message and ship its output durably.
async fn drive<E: Env>(env: E, core: Arc<Mutex<EPaxosCore>>) {
    recover(&env, &core).await;
    loop {
        let envelope = env.recv().await;
        let Ok(msg) = serde_json::from_slice::<EPaxosMsg>(&envelope.payload) else {
            continue;
        };
        let outs = core
            .lock()
            .expect("epaxos core poisoned")
            .handle(envelope.from, msg);
        persist_then_ship(&env, &core, outs);
    }
}

/// Replay the WAL into the core's replica view on startup.
async fn recover<E: Env>(env: &E, core: &Arc<Mutex<EPaxosCore>>) {
    let bytes = env.read(WAL).await.unwrap_or_default();
    if bytes.is_empty() {
        return;
    }
    let state = PersistedState::replay(PersistedState::decode(&bytes));
    core.lock().expect("epaxos core poisoned").recovered(state);
}
