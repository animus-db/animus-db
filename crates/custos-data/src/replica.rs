//! The per-node data replica server: applies quorum writes/reads to local
//! storage and enforces epoch fencing.

use std::sync::{Arc, Mutex};

use custos_env::{Env, EnvExt};
use custos_storage::StorageEngine;
use custos_tablet::Epoch;

use crate::DataMsg;

/// A handle to a running replica, for inspection and epoch updates.
///
/// The replica's known epoch is advanced by the control plane on a topology
/// change; an operation bearing an older epoch is fenced (rejected).
#[derive(Clone)]
pub struct ReplicaHandle<S: StorageEngine> {
    epoch: Arc<Mutex<Epoch>>,
    storage: S,
}

impl<S: StorageEngine> ReplicaHandle<S> {
    /// The replica's current known epoch.
    pub fn epoch(&self) -> Epoch {
        *self.epoch.lock().expect("replica epoch poisoned")
    }

    /// Update the replica's known epoch (e.g. on a control-plane topology
    /// change). Operations older than this are fenced.
    pub fn set_epoch(&self, epoch: Epoch) {
        *self.epoch.lock().expect("replica epoch poisoned") = epoch;
    }

    /// The replica's local storage engine (clones share state).
    pub fn storage(&self) -> &S {
        &self.storage
    }
}

/// Start a replica server on `env`, backed by `storage`, knowing `epoch`.
///
/// Spawns the serve loop and returns a handle. The serve loop runs until the
/// task is dropped (i.e. for the life of the simulation/process).
pub fn serve_replica<E, S>(env: E, storage: S, epoch: Epoch) -> ReplicaHandle<S>
where
    E: Env,
    S: StorageEngine + 'static,
{
    let epoch = Arc::new(Mutex::new(epoch));
    let handle = ReplicaHandle {
        epoch: Arc::clone(&epoch),
        storage: storage.clone(),
    };

    env.clone().spawn_task(async move {
        loop {
            let envelope = env.recv().await;
            let Ok(msg) = serde_json::from_slice::<DataMsg>(&envelope.payload) else {
                tracing::warn!("undecodable data message dropped");
                continue;
            };
            let reply = handle_msg(&storage, &epoch, msg);
            if let Some(reply) = reply {
                let bytes = serde_json::to_vec(&reply).expect("data message serializes");
                env.send(envelope.from, bytes).await;
            }
        }
    });

    handle
}

/// Handle one inbound message, returning the reply to send (if any).
///
/// Fencing rule (ADR 0002): reject an operation whose epoch is *older* than the
/// replica's known epoch. An operation with a newer epoch is honored and
/// advances the replica's epoch (the replica was behind on topology).
fn handle_msg<S: StorageEngine>(
    storage: &S,
    epoch: &Arc<Mutex<Epoch>>,
    msg: DataMsg,
) -> Option<DataMsg> {
    match msg {
        DataMsg::Write {
            req,
            epoch: op_epoch,
            key,
            value,
            version,
        } => {
            if fenced(epoch, op_epoch) {
                return Some(DataMsg::WriteAck { req, ok: false });
            }
            // A monotonic-version conflict means a newer write already won
            // (last-write-wins); treat it as an accepted no-op rather than an
            // error, so concurrent coordinators converge.
            let _ = storage.put(&key, &value, version);
            Some(DataMsg::WriteAck { req, ok: true })
        }
        DataMsg::Read {
            req,
            epoch: op_epoch,
            key,
        } => {
            if fenced(epoch, op_epoch) {
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
        // Replicas never receive responses.
        DataMsg::WriteAck { .. } | DataMsg::ReadResp { .. } => None,
    }
}

/// Whether `op_epoch` is stale relative to the replica's known epoch. Advances
/// the known epoch when the operation carries a newer one.
fn fenced(epoch: &Arc<Mutex<Epoch>>, op_epoch: Epoch) -> bool {
    let mut known = epoch.lock().expect("replica epoch poisoned");
    if op_epoch < *known {
        true
    } else {
        if op_epoch > *known {
            *known = op_epoch;
        }
        false
    }
}
