//! The per-node data replica server: applies quorum writes/reads to local
//! storage and enforces epoch fencing, per tablet.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use custos_env::{Env, EnvExt, NodeId};
use custos_storage::StorageEngine;
use custos_tablet::{Epoch, TabletId};

use crate::DataMsg;
use crate::digest;

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

/// Which peers a replica may exchange **repair traffic** with — the tablet's
/// residency-eligible placement (ADR 0005). `None` means "no residency boundary
/// enforced" (any peer); `Some(set)` restricts the receive side: a
/// `Sync`/`SyncDigest`/`SyncPull` from a node outside the set is dropped, so
/// anti-entropy/read-repair cannot push data across a residency boundary even to
/// a reachable node.
type AllowedPeers = Option<BTreeSet<NodeId>>;

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
/// task is dropped (i.e. for the life of the simulation/process). Repair traffic
/// is **not** residency-restricted on this replica — use
/// [`serve_replica_with_residency`] to bound which peers may push repair into
/// it (ADR 0005/0010).
pub fn serve_replica<E, S>(env: E, storage: S, floor: Epoch) -> ReplicaHandle<S>
where
    E: Env,
    S: StorageEngine + 'static,
{
    serve(env, storage, floor, None)
}

/// Like [`serve_replica`], but the replica only accepts repair traffic
/// (`Sync`/`SyncDigest`/`SyncPull`) from a peer in `allowed` — the tablet's
/// residency-eligible replica set (ADR 0005). A repair message from any other
/// node is dropped even though it is reachable, so anti-entropy/read-repair
/// cannot move data across a residency boundary. Quorum `Write`/`Delete`/`Read`
/// are unaffected (a residency-ineligible node is simply never a replica in a
/// `TabletView`, so it is never sent one).
pub fn serve_replica_with_residency<E, S>(
    env: E,
    storage: S,
    floor: Epoch,
    allowed: BTreeSet<NodeId>,
) -> ReplicaHandle<S>
where
    E: Env,
    S: StorageEngine + 'static,
{
    serve(env, storage, floor, Some(allowed))
}

fn serve<E, S>(env: E, storage: S, floor: Epoch, allowed: AllowedPeers) -> ReplicaHandle<S>
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
    let me = env.node_id();

    env.clone().spawn_task(async move {
        loop {
            let envelope = env.recv().await;
            let Ok(msg) = serde_json::from_slice::<DataMsg>(&envelope.payload) else {
                tracing::warn!("undecodable data message dropped");
                continue;
            };
            for reply in handle_msg(&storage, &epochs, me, &allowed, envelope.from, msg).await {
                let bytes = serde_json::to_vec(&reply).expect("data message serializes");
                env.send(envelope.from, bytes).await;
            }
        }
    });

    handle
}

/// Start a background **anti-entropy** loop on `env` for `tablet`: every
/// `interval` of (virtual) time, send each peer in `peers` (itself excluded) a
/// [`DataMsg::SyncDigest`] — a compact per-segment summary of this replica's
/// data. A peer compares it against its own digest and pulls back (via
/// [`DataMsg::SyncPull`]) only the segments that differ, which this replica then
/// answers with a [`DataMsg::Sync`] of just those segments. So a replica that
/// missed writes — because it was partitioned or briefly down — converges in the
/// background **without needing a read to repair it**, and a converged pair
/// transfers no entry data at all.
///
/// This is the Merkle/range-digest refinement of the original full-push scheme
/// (ADR 0010): the periodic round is `O(segments)`, and only divergent ranges
/// carry entry bytes. The loop is send-only on the timer (replies to a
/// `SyncPull` flow through [`serve_replica`]'s inbox), so it shares a node's
/// `env` with the replica server without contending on the single-consumer
/// inbox. `peers` should already be residency-restricted to the tablet's
/// placement; pair with [`serve_replica_with_residency`] so the receiving side
/// also rejects out-of-policy repair (ADR 0005).
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
            let entries: Vec<crate::SyncEntry> = match storage.entries_with_tombstones().await {
                Ok(es) => es,
                Err(_) => continue,
            };
            let segments = digest::digest(&entries);
            // An empty digest provokes no pulls, so skip the round; a replica
            // that holds data will drive convergence with the empty one when its
            // own round fires.
            if segments.is_empty() {
                continue;
            }
            let msg = DataMsg::SyncDigest {
                tablet,
                epoch,
                from: me,
                segments,
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

/// Handle one inbound message, returning the replies to send back to `from`
/// (if any). `me` is this replica's node id; `allowed`, when `Some`, restricts
/// which peers repair traffic is accepted from (residency, ADR 0005).
async fn handle_msg<S: StorageEngine>(
    storage: &S,
    epochs: &Arc<Mutex<Epochs>>,
    me: NodeId,
    allowed: &AllowedPeers,
    from: NodeId,
    msg: DataMsg,
) -> Vec<DataMsg> {
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
                return vec![DataMsg::WriteAck { req, ok: false }];
            }
            // Per-key last-writer-wins: `merge` applies iff this version is
            // newer for the key, so a write superseded by a higher-versioned
            // one is an accepted no-op and concurrent coordinators converge.
            let _ = storage.merge(&key, &value, version).await;
            vec![DataMsg::WriteAck { req, ok: true }]
        }
        DataMsg::Delete {
            req,
            tablet,
            epoch,
            key,
            version,
        } => {
            if fenced(epochs, tablet, epoch) {
                return vec![DataMsg::DeleteAck { req, ok: false }];
            }
            // Per-key LWW tombstone: superseded by a higher-versioned write or
            // delete, so concurrent coordinators converge regardless of order.
            let _ = storage.merge_tombstone(&key, version).await;
            vec![DataMsg::DeleteAck { req, ok: true }]
        }
        DataMsg::Read {
            req,
            tablet,
            epoch,
            key,
        } => {
            if fenced(epochs, tablet, epoch) {
                return vec![DataMsg::ReadResp {
                    req,
                    ok: false,
                    value: None,
                }];
            }
            let value = storage
                .get(&key)
                .await
                .ok()
                .flatten()
                .map(|vv| (vv.version, vv.value));
            vec![DataMsg::ReadResp {
                req,
                ok: true,
                value,
            }]
        }
        DataMsg::Sync {
            tablet,
            epoch,
            entries,
        } => {
            // Reconcile a batch by per-key LWW (anti-entropy / read-repair).
            // Dropped if the sender is outside the residency boundary, fenced as
            // a whole on a stale epoch; otherwise fire-and-forget.
            if residency_ok(allowed, from) && !fenced(epochs, tablet, epoch) {
                for (key, value, version) in entries {
                    let _ = match value {
                        Some(v) => storage.merge(&key, &v, version).await,
                        None => storage.merge_tombstone(&key, version).await,
                    };
                }
            }
            vec![]
        }
        DataMsg::SyncDigest {
            tablet,
            epoch,
            from: _,
            segments,
        } => {
            // A peer summarized its data; ask it for the segments where we
            // differ. Reject across the residency boundary or on a stale epoch.
            if !residency_ok(allowed, from) || fenced(epochs, tablet, epoch) {
                return vec![];
            }
            let mine = match storage.entries_with_tombstones().await {
                Ok(es) => digest::digest(&es),
                Err(_) => return vec![],
            };
            let want = digest::divergent(&mine, &segments);
            if want.is_empty() {
                return vec![];
            }
            vec![DataMsg::SyncPull {
                tablet,
                epoch,
                from: me,
                segments: want,
            }]
        }
        DataMsg::SyncPull {
            tablet,
            epoch,
            from: _,
            segments,
        } => {
            // A peer asked for specific segments; push back only those entries.
            if !residency_ok(allowed, from) || fenced(epochs, tablet, epoch) {
                return vec![];
            }
            let entries = match storage.entries_with_tombstones().await {
                Ok(es) => digest::entries_in_segments(&es, &segments),
                Err(_) => return vec![],
            };
            if entries.is_empty() {
                return vec![];
            }
            vec![DataMsg::Sync {
                tablet,
                epoch,
                entries,
            }]
        }
        // Replicas never receive responses.
        DataMsg::WriteAck { .. } | DataMsg::ReadResp { .. } | DataMsg::DeleteAck { .. } => vec![],
    }
}

/// Whether `from` is permitted to send repair traffic to this replica. With no
/// residency boundary (`None`) every peer is allowed; otherwise only members of
/// the allowed set are (ADR 0005).
fn residency_ok(allowed: &AllowedPeers, from: NodeId) -> bool {
    match allowed {
        None => true,
        Some(set) => set.contains(&from),
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
