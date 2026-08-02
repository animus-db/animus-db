//! The per-node data replica server: applies quorum writes/reads to local
//! storage and enforces epoch fencing, per tablet.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::{Env, EnvExt, Metric, MetricsHandle, NodeId};
use animus_storage::StorageEngine;
use animus_tablet::{Epoch, TabletId};

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
/// The loop takes the replica's [`ReplicaHandle`] as its source of **both** the
/// data (its storage engine) and the **live tablet epoch**: each round stamps
/// the outbound `SyncDigest` with `handle.epoch(tablet)` — the replica's
/// currently-known epoch for the tablet — *not* a constant captured at start.
/// This matters after a topology change: a placement reconcile bumps the
/// tablet's epoch (and the control plane advances the replica's known epoch via
/// [`ReplicaHandle::set_epoch`]), so a round that still stamped the old epoch
/// would be **fenced** by every up-to-date peer and a re-placed spare would only
/// be filled lazily by read-repair. Reading the epoch live keeps background
/// convergence working across a reconcile (ADR 0010/0002); a genuinely
/// stale-epoch peer is still fenced. The epoch is read with a brief lock that is
/// released before any `.await` (no guard held across an await).
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
    handle: ReplicaHandle<S>,
    tablet: TabletId,
    peers: Vec<NodeId>,
    interval: Duration,
) where
    E: Env,
    S: StorageEngine + 'static,
{
    let metrics = env.metrics();
    serve_anti_entropy_with_metrics(env, handle, tablet, peers, interval, metrics);
}

/// Like [`serve_anti_entropy`], but records `data_anti_entropy_rounds` (ADR 0015)
/// into `metrics` rather than `env.metrics()` — one count per round that actually
/// emits a (non-empty) segment digest to its peers. Additive and observe-only: it
/// changes no convergence behavior. A sim test threads a recording handle here to
/// read the counter back (`SimEnv::metrics()` is the no-op default).
pub fn serve_anti_entropy_with_metrics<E, S>(
    env: E,
    handle: ReplicaHandle<S>,
    tablet: TabletId,
    peers: Vec<NodeId>,
    interval: Duration,
    metrics: MetricsHandle,
) where
    E: Env,
    S: StorageEngine + 'static,
{
    let me = env.node_id();
    let storage = handle.storage().clone();
    env.clone().spawn_task(async move {
        loop {
            env.sleep(interval).await;
            // Stamp the digest with the replica's *current* known epoch for the
            // tablet (read live each round; the lock is released before any await
            // below), so a post-reconcile round carries the bumped epoch and is
            // not fenced by up-to-date peers (ADR 0002).
            let epoch = handle.epoch(tablet);
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
            // A round that emits a digest to its peers — count it (ADR 0015).
            metrics.incr(Metric::DataAntiEntropyRounds);
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
            // An ack must mean the write durably applied: a no-op merge
            // (superseded, `Ok(false)`) is still success, but a storage `Err`
            // means we did NOT persist, so reply `ok: false` and the
            // coordinator does not count us toward the W quorum.
            let ok = storage.merge(&key, &value, version).await.is_ok();
            vec![DataMsg::WriteAck { req, ok }]
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
            // As with `Write`, an ack must mean the tombstone durably applied:
            // a superseded no-op is success, a storage `Err` is `ok: false`.
            let ok = storage.merge_tombstone(&key, version).await.is_ok();
            vec![DataMsg::DeleteAck { req, ok }]
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
        DataMsg::ScanRange {
            req,
            tablet,
            epoch,
            start,
            end,
        } => {
            if fenced(epochs, tablet, epoch) {
                return vec![DataMsg::ScanResp {
                    req,
                    ok: false,
                    entries: vec![],
                }];
            }
            // Return each key's latest record in `[start, end)` **including
            // tombstones**, so the coordinator can merge by per-key newest
            // version and then exclude deleted keys: a replica holding a stale
            // value must not mask a peer's newer tombstone. We range-filter
            // `entries_with_tombstones` rather than `scan` (which drops
            // tombstones) for exactly that reason; `entries_with_tombstones` is
            // `scan` over the whole keyspace, tombstones retained (ADR 0010).
            let entries = match storage.entries_with_tombstones().await {
                Ok(es) => es
                    .into_iter()
                    .filter(|(k, _, _)| in_range(k, &start, &end))
                    .collect(),
                Err(_) => {
                    return vec![DataMsg::ScanResp {
                        req,
                        ok: false,
                        entries: vec![],
                    }];
                }
            };
            vec![DataMsg::ScanResp {
                req,
                ok: true,
                entries,
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
        // Hinted handoff (ADR 0010): a liveness probe from a hint holder. Answer
        // it (we are reachable) so the holder replays buffered hints to us as a
        // `Sync`. It carries no data, so it is not epoch-fenced; the subsequent
        // `Sync` is fenced and residency-checked exactly as any repair is. The
        // probe itself is unrestricted by residency: an out-of-policy holder may
        // learn we are up, but the `Sync` it then sends is dropped by the
        // residency guard above, so no data crosses the boundary.
        DataMsg::Probe { req } => vec![DataMsg::ProbeAck { req }],
        // Replicas never receive responses.
        DataMsg::WriteAck { .. }
        | DataMsg::ReadResp { .. }
        | DataMsg::DeleteAck { .. }
        | DataMsg::ScanResp { .. }
        | DataMsg::ProbeAck { .. } => vec![],
    }
}

/// Whether `key` falls in the half-open range `[start, end)`. An empty `end`
/// means "no upper bound" (scan to the end of the keyspace), mirroring how an
/// open-ended range is conventionally encoded; `start` is inclusive.
fn in_range(key: &[u8], start: &[u8], end: &[u8]) -> bool {
    key >= start && (end.is_empty() || key < end)
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
