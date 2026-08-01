//! The per-node data replica server: applies quorum writes/reads to local
//! storage and enforces epoch fencing, per tablet.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use custos_env::{Env, EnvExt, NodeId};
use custos_storage::StorageEngine;
use custos_tablet::{Epoch, TabletId};

use crate::DataMsg;

/// Per-tablet known epochs, defaulting to a floor for tablets not yet seen.
#[derive(Debug)]
struct Epochs {
    known: BTreeMap<TabletId, Epoch>,
    floor: Epoch,
}

impl Epochs {
    fn get(&self, tablet: TabletId) -> Epoch {
        self.known.get(&tablet).copied().unwrap_or(self.floor)
    }
}

/// A handle to a running replica, for inspection and per-tablet epoch updates.
///
/// A replica's known epoch for a tablet is advanced by the control plane on a
/// topology change; an operation bearing an older epoch for that tablet is
/// fenced (rejected).
#[derive(Clone)]
pub struct ReplicaHandle<S: StorageEngine> {
    epochs: Arc<Mutex<Epochs>>,
    storage: S,
}

impl<S: StorageEngine> ReplicaHandle<S> {
    /// The replica's current known epoch for `tablet`.
    pub fn epoch(&self, tablet: TabletId) -> Epoch {
        self.epochs
            .lock()
            .expect("replica epochs poisoned")
            .get(tablet)
    }

    /// Set the replica's known epoch for `tablet` (e.g. on a control-plane
    /// topology change). Operations older than this for that tablet are fenced.
    pub fn set_epoch(&self, tablet: TabletId, epoch: Epoch) {
        self.epochs
            .lock()
            .expect("replica epochs poisoned")
            .known
            .insert(tablet, epoch);
    }

    /// The replica's local storage engine (clones share state).
    pub fn storage(&self) -> &S {
        &self.storage
    }
}

/// Start a replica server on `env`, backed by `storage`, with a `floor` epoch
/// applied to any tablet it has not yet learned an epoch for.
///
/// Spawns the serve loop and returns a handle. The serve loop runs until the
/// task is dropped (i.e. for the life of the simulation/process).
pub fn serve_replica<E, S>(env: E, storage: S, floor: Epoch) -> ReplicaHandle<S>
where
    E: Env,
    S: StorageEngine + 'static,
{
    let epochs = Arc::new(Mutex::new(Epochs {
        known: BTreeMap::new(),
        floor,
    }));
    let handle = ReplicaHandle {
        epochs: Arc::clone(&epochs),
        storage: storage.clone(),
    };

    env.clone().spawn_task(async move {
        loop {
            let envelope = env.recv().await;
            let Ok(msg) = serde_json::from_slice::<DataMsg>(&envelope.payload) else {
                tracing::warn!("undecodable data message dropped");
                continue;
            };
            if let Some(reply) = handle_msg(&storage, &epochs, msg) {
                let bytes = serde_json::to_vec(&reply).expect("data message serializes");
                env.send(envelope.from, bytes).await;
            }
        }
    });

    handle
}

/// Start a background **anti-entropy** loop on `env` for `tablet`: every
/// `interval` of (virtual) time, push this replica's full data digest to each
/// peer in `peers` (itself excluded) as a [`DataMsg::Sync`]. Peers reconcile by
/// per-key last-writer-wins, so a replica that missed writes — because it was
/// partitioned or briefly down — converges in the background **without needing
/// a read to repair it**. This is what guarantees eventual convergence of raw
/// replica state, beyond the read-time intersection that `R + W > N` gives.
///
/// Full-push (the whole digest each round) is the simplest scheme that provably
/// converges; sending only divergent ranges via a Merkle-tree digest is a later
/// optimization. The loop is send-only, so it shares a node's `env` with
/// [`serve_replica`] without contending on the single-consumer inbox.
pub fn serve_anti_entropy<E, S>(
    env: E,
    storage: S,
    tablet: TabletId,
    epoch: Epoch,
    peers: Vec<NodeId>,
    interval: Duration,
) where
    E: Env,
    S: StorageEngine + 'static,
{
    let me = env.node_id();
    env.clone().spawn_task(async move {
        loop {
            env.sleep(interval).await;
            // Carry tombstones too, so a delete converges to a replica that
            // still holds the value (ADR 0010).
            let entries: Vec<(Vec<u8>, Option<Vec<u8>>, u64)> =
                match storage.entries_with_tombstones() {
                    Ok(es) => es,
                    Err(_) => continue,
                };
            if entries.is_empty() {
                continue;
            }
            let msg = DataMsg::Sync {
                tablet,
                epoch,
                entries,
            };
            let bytes = serde_json::to_vec(&msg).expect("data message serializes");
            for &peer in &peers {
                if peer != me {
                    env.send(peer, bytes.clone()).await;
                }
            }
        }
    });
}

/// Handle one inbound message, returning the reply to send (if any).
fn handle_msg<S: StorageEngine>(
    storage: &S,
    epochs: &Arc<Mutex<Epochs>>,
    msg: DataMsg,
) -> Option<DataMsg> {
    match msg {
        DataMsg::Write {
            req,
            tablet,
            epoch,
            key,
            value,
            version,
        } => {
            if fenced(epochs, tablet, epoch) {
                return Some(DataMsg::WriteAck { req, ok: false });
            }
            // Per-key last-writer-wins: `merge` applies iff this version is
            // newer for the key, so a write superseded by a higher-versioned
            // one is an accepted no-op and concurrent coordinators converge.
            let _ = storage.merge(&key, &value, version);
            Some(DataMsg::WriteAck { req, ok: true })
        }
        DataMsg::Delete {
            req,
            tablet,
            epoch,
            key,
            version,
        } => {
            if fenced(epochs, tablet, epoch) {
                return Some(DataMsg::DeleteAck { req, ok: false });
            }
            // Per-key LWW tombstone: superseded by a higher-versioned write or
            // delete, so concurrent coordinators converge regardless of order.
            let _ = storage.merge_tombstone(&key, version);
            Some(DataMsg::DeleteAck { req, ok: true })
        }
        DataMsg::Read {
            req,
            tablet,
            epoch,
            key,
        } => {
            if fenced(epochs, tablet, epoch) {
                return Some(DataMsg::ReadResp {
                    req,
                    ok: false,
                    value: None,
                });
            }
            let value = storage
                .get(&key)
                .ok()
                .flatten()
                .map(|vv| (vv.version, vv.value));
            Some(DataMsg::ReadResp {
                req,
                ok: true,
                value,
            })
        }
        DataMsg::Sync {
            tablet,
            epoch,
            entries,
        } => {
            // Reconcile a batch by per-key LWW (anti-entropy / read-repair).
            // Fenced as a whole on a stale epoch; otherwise fire-and-forget.
            if !fenced(epochs, tablet, epoch) {
                for (key, value, version) in entries {
                    let _ = match value {
                        Some(v) => storage.merge(&key, &v, version),
                        None => storage.merge_tombstone(&key, version),
                    };
                }
            }
            None
        }
        // Replicas never receive responses.
        DataMsg::WriteAck { .. } | DataMsg::ReadResp { .. } | DataMsg::DeleteAck { .. } => None,
    }
}

/// Whether `op_epoch` is stale for `tablet` relative to the replica's known
/// epoch. Advances the known epoch when the operation carries a newer one
/// (the replica was behind on this tablet's topology). Fencing rule: ADR 0002.
fn fenced(epochs: &Arc<Mutex<Epochs>>, tablet: TabletId, op_epoch: Epoch) -> bool {
    let mut e = epochs.lock().expect("replica epochs poisoned");
    let known = e.get(tablet);
    if op_epoch < known {
        true
    } else {
        if op_epoch > known {
            e.known.insert(tablet, op_epoch);
        }
        false
    }
}
