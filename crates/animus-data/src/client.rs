//! The quorum coordinator: routes an operation to a tablet's replicas and
//! collects a quorum of responses.

use std::time::Duration;

use animus_env::{Env, Metric, MetricsHandle, NodeId};
use animus_tablet::{Epoch, Tablet, TabletId};
use futures::future::{Either, select};

use crate::DataMsg;
use crate::hint::{AllowedTargets, HintStore};

/// The coordinator's routing view of a tablet: which tablet, its replica set,
/// the epoch to fence with, and the read/write quorum sizes. Read from cached
/// control-plane metadata (ADR 0001). Choose `r + w > replicas.len()` for
/// read-your-writes.
#[derive(Clone, Debug)]
pub struct TabletView {
    /// The tablet this view routes to.
    pub tablet: TabletId,
    /// The tablet's replica node ids.
    pub replicas: Vec<NodeId>,
    /// The epoch stamped on operations (the fencing token).
    pub epoch: Epoch,
    /// Read quorum size.
    pub r: usize,
    /// Write quorum size.
    pub w: usize,
}

impl TabletView {
    /// Build a view from a tablet's placement, with the given quorum sizes.
    #[must_use]
    pub fn from_tablet(tablet: &Tablet, r: usize, w: usize) -> Self {
        Self {
            tablet: tablet.id,
            replicas: tablet.replicas.clone(),
            epoch: tablet.epoch,
            r,
            w,
        }
    }
}

/// Routes a key to the tablet that owns it, using a cached snapshot of the
/// tablet map. The tablets are expected to partition the keyspace into disjoint
/// ranges (the control plane maintains this via split/merge).
#[derive(Clone, Debug)]
pub struct Router {
    tablets: Vec<Tablet>,
    r: usize,
    w: usize,
}

impl Router {
    /// Build a router over a tablet map with the given quorum sizes.
    #[must_use]
    pub fn new(tablets: Vec<Tablet>, r: usize, w: usize) -> Self {
        Self { tablets, r, w }
    }

    /// Resolve the [`TabletView`] for the tablet owning `key`, or `None` if no
    /// tablet covers it.
    #[must_use]
    pub fn view_for(&self, key: &[u8]) -> Option<TabletView> {
        self.tablets
            .iter()
            .find(|t| t.range.contains(key))
            .map(|t| TabletView::from_tablet(t, self.r, self.w))
    }
}

/// Outcome of a quorum read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadResult {
    /// A read quorum responded; the latest value (or `None` if absent).
    Value(Option<Vec<u8>>),
    /// A read quorum could not be reached (too many replicas down/fenced).
    Failed,
}

/// The outcome of [`DataClient::quorum_read`]: the winning `(version, value)`
/// (inner `None` if the key is absent everywhere) and whether the responding
/// replicas diverged, which drives read-repair.
struct QuorumRead {
    best: Option<(u64, Vec<u8>)>,
    diverged: bool,
}

/// A quorum coordinator. It is a network participant in its own right (its
/// `env`'s node id), distinct from the replica nodes it talks to.
///
/// It optionally carries a [`HintStore`] for **hinted handoff** (ADR 0010): when
/// a quorum write/delete is acked by `W` replicas but a tablet replica did not
/// respond (it was down/partitioned), the coordinator buffers a hint for that
/// replica and a background [`serve_hint_handoff`](crate::serve_hint_handoff)
/// loop replays it once the replica is reachable again. Hints are
/// residency-bounded (ADR 0005): one is recorded only for a target the placement
/// admits. A coordinator built with [`new`](DataClient::new) holds no store and
/// records no hints (the original behavior), so this is purely additive.
#[derive(Clone)]
pub struct DataClient<E: Env> {
    env: E,
    /// Buffered hints for unavailable replicas; `None` ⇒ hinted handoff off.
    hints: Option<HintStore>,
    /// Residency bound on which replicas may be hinted (ADR 0005); `None` ⇒ no
    /// boundary. Only consulted when `hints` is `Some`.
    allowed: AllowedTargets,
    /// Observability sink (ADR 0015). Defaults to `env.metrics()` (the env's own
    /// recording handle under `ProdEnv`, the shared no-op under `SimEnv`); a sim
    /// test threads a recording handle via [`with_metrics`](DataClient::with_metrics)
    /// to read the coordinator's counters back. Recording is a relaxed atomic add,
    /// so it never perturbs determinism and never changes quorum behavior.
    metrics: MetricsHandle,
}

impl<E: Env> DataClient<E> {
    /// Create a coordinator on `env` with **no** hinted handoff (hints disabled).
    pub fn new(env: E) -> Self {
        let metrics = env.metrics();
        Self {
            env,
            hints: None,
            allowed: None,
            metrics,
        }
    }

    /// Record this coordinator's quorum/read-repair counters into `metrics`
    /// instead of `env.metrics()` (ADR 0015). Additive and observe-only — it
    /// changes no quorum behavior. Used by a sim test to read counters back
    /// (`SimEnv::metrics()` is the no-op default), and composes with
    /// [`with_hints`](DataClient::with_hints).
    #[must_use]
    pub fn with_metrics(mut self, metrics: MetricsHandle) -> Self {
        self.metrics = metrics;
        self
    }

    /// This coordinator's metrics handle (ADR 0015).
    #[must_use]
    pub fn metrics(&self) -> &MetricsHandle {
        &self.metrics
    }

    /// Create a coordinator on `env` that **buffers hints** for replicas a
    /// write/delete could not reach, into `hints`, bounded by `allowed` residency
    /// (ADR 0005; `None` ⇒ no residency boundary). Pair it with
    /// [`serve_hint_handoff`](crate::serve_hint_handoff) over the same `env` and
    /// `hints`/`allowed` so the buffered hints are replayed when a target returns.
    pub fn with_hints(env: E, hints: HintStore, allowed: AllowedTargets) -> Self {
        let metrics = env.metrics();
        Self {
            env,
            hints: Some(hints),
            allowed,
            metrics,
        }
    }

    /// The coordinator's hint store, if hinted handoff is enabled.
    #[must_use]
    pub fn hint_store(&self) -> Option<&HintStore> {
        self.hints.as_ref()
    }

    /// Quorum write: store `value` at `key` with MVCC `version`, returning
    /// `true` once `w` replicas acknowledge (within `timeout`).
    pub async fn write(
        &self,
        view: &TabletView,
        key: &[u8],
        value: &[u8],
        version: u64,
        timeout: Duration,
    ) -> bool {
        self.metrics.incr(Metric::DataQuorumWritesAttempted);
        let req = self.env.next_u64();
        let msg = DataMsg::Write {
            req,
            tablet: view.tablet,
            epoch: view.epoch,
            key: key.to_vec(),
            value: value.to_vec(),
            version,
        };
        self.broadcast(&view.replicas, &msg).await;

        let mut acks = 0usize;
        let mut responded = 0usize;
        // Track which replicas acked `ok`, so a replica that was unavailable can
        // be hinted (hinted handoff, ADR 0010). BTreeSet ⇒ deterministic.
        let mut acked: std::collections::BTreeSet<NodeId> = std::collections::BTreeSet::new();
        self.collect(view.replicas.len(), timeout, |from, reply| {
            if let DataMsg::WriteAck { req: r, ok } = reply {
                if r == req {
                    responded += 1;
                    if ok {
                        acks += 1;
                        acked.insert(from);
                    }
                    return acks >= view.w || responded >= view.replicas.len();
                }
            }
            false
        })
        .await;
        let committed = acks >= view.w;
        if committed {
            self.metrics.incr(Metric::DataQuorumWritesSucceeded);
            // On a committed write, buffer a hint for every replica that did not
            // ack it, so it converges promptly when it returns (residency-bounded).
            self.hint_unreached(view, &acked, key, Some(value.to_vec()), version);
        } else {
            self.metrics.incr(Metric::DataQuorumWritesFailed);
        }
        committed
    }

    /// Quorum delete: tombstone `key` with MVCC `version`, returning `true` once
    /// `w` replicas acknowledge (within `timeout`). Epoch-fenced and applied by
    /// per-key LWW exactly like [`write`](DataClient::write), so the tombstone
    /// propagates to lagging replicas through anti-entropy / read-repair just as
    /// a value does (ADR 0010).
    pub async fn delete(
        &self,
        view: &TabletView,
        key: &[u8],
        version: u64,
        timeout: Duration,
    ) -> bool {
        // A delete is a quorum mutation, counted under the write counters.
        self.metrics.incr(Metric::DataQuorumWritesAttempted);
        let req = self.env.next_u64();
        let msg = DataMsg::Delete {
            req,
            tablet: view.tablet,
            epoch: view.epoch,
            key: key.to_vec(),
            version,
        };
        self.broadcast(&view.replicas, &msg).await;

        let mut acks = 0usize;
        let mut responded = 0usize;
        let mut acked: std::collections::BTreeSet<NodeId> = std::collections::BTreeSet::new();
        self.collect(view.replicas.len(), timeout, |from, reply| {
            if let DataMsg::DeleteAck { req: r, ok } = reply {
                if r == req {
                    responded += 1;
                    if ok {
                        acks += 1;
                        acked.insert(from);
                    }
                    return acks >= view.w || responded >= view.replicas.len();
                }
            }
            false
        })
        .await;
        let committed = acks >= view.w;
        if committed {
            self.metrics.incr(Metric::DataQuorumWritesSucceeded);
            // A committed delete hints the unreached replicas with a tombstone
            // (`None`), so the delete converges to them just as a value would.
            self.hint_unreached(view, &acked, key, None, version);
        } else {
            self.metrics.incr(Metric::DataQuorumWritesFailed);
        }
        committed
    }

    /// Quorum read: return the latest value for `key` once `r` replicas respond.
    ///
    /// If the responding replicas disagreed — some returned an older version, or
    /// none at all — the coordinator performs **read-repair**: it pushes the
    /// winning `(value, version)` back to the tablet's replicas (a fire-and-forget
    /// [`DataMsg::Sync`], reconciled by per-key LWW), so the replicas that took
    /// part in this read converge. Replicas that did not respond in time are
    /// caught up by background [`serve_anti_entropy`](crate::serve_anti_entropy).
    pub async fn read(&self, view: &TabletView, key: &[u8], timeout: Duration) -> ReadResult {
        match self.quorum_read(view, key, timeout).await {
            Some(qr) => {
                if qr.diverged {
                    if let Some((ver, val)) = &qr.best {
                        // One repair push per divergent read; it carries exactly
                        // one key (the winning `(value, version)`).
                        self.metrics.incr(Metric::DataReadRepairTriggered);
                        self.metrics.incr(Metric::DataReadRepairKeysRepaired);
                        self.read_repair(view, key, val, *ver).await;
                    }
                }
                ReadResult::Value(qr.best.map(|(_, v)| v))
            }
            None => ReadResult::Failed,
        }
    }

    /// Quorum range scan over the half-open key range `[start, end)` (an empty
    /// `end` scans to the end of the keyspace). Broadcasts a
    /// [`DataMsg::ScanRange`] to the tablet's replicas and, once `r` replicas
    /// respond, **merges** their per-replica results by per-key newest MVCC
    /// version (last-writer-wins, exactly like a point read): the highest-version
    /// record wins for each key. Tombstones ride along in the merge (so a newer
    /// delete on one replica correctly shadows a stale value on another) and are
    /// then excluded from the returned set. The result is the merged, **sorted**
    /// `(key, value)` set; an optional `limit` caps it to the first `limit` keys
    /// in key order.
    ///
    /// Returns `None` if a read quorum could not be reached (too many replicas
    /// down or fenced) — the scan analog of [`ReadResult::Failed`]. This is a
    /// *snapshot-free* range read: it reflects whatever the responding quorum
    /// holds at response time, with the same `R + W > N` intersection guarantee a
    /// point read has per key. It does **not** perform read-repair (a divergent
    /// range is converged by background anti-entropy); it is the native primitive
    /// the wire adapters use instead of tracking keys in-memory.
    pub async fn scan(
        &self,
        view: &TabletView,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
        timeout: Duration,
    ) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
        let req = self.env.next_u64();
        let msg = DataMsg::ScanRange {
            req,
            tablet: view.tablet,
            epoch: view.epoch,
            start: start.to_vec(),
            end: end.to_vec(),
        };
        self.broadcast(&view.replicas, &msg).await;

        let mut oks = 0usize;
        let mut responded = 0usize;
        // Merge each responder's records by per-key newest version (LWW). A
        // tombstone (`None`) competes on version just like a value, so a newer
        // delete shadows a stale value; deleted keys are dropped after merging.
        // BTreeMap ⇒ sorted-by-key result, deterministic.
        let mut merged: std::collections::BTreeMap<Vec<u8>, (u64, Option<Vec<u8>>)> =
            std::collections::BTreeMap::new();
        self.collect(view.replicas.len(), timeout, |_from, reply| {
            if let DataMsg::ScanResp {
                req: r,
                ok,
                entries,
            } = reply
            {
                if r == req {
                    responded += 1;
                    if ok {
                        oks += 1;
                        for (key, value, version) in entries {
                            merged
                                .entry(key)
                                .and_modify(|cur| {
                                    if version > cur.0 {
                                        *cur = (version, value.clone());
                                    }
                                })
                                .or_insert((version, value));
                        }
                    }
                    return oks >= view.r || responded >= view.replicas.len();
                }
            }
            false
        })
        .await;

        if oks < view.r {
            return None;
        }
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = merged
            .into_iter()
            .filter_map(|(key, (_ver, value))| value.map(|v| (key, v)))
            .collect();
        if let Some(limit) = limit {
            out.truncate(limit);
        }
        Some(out)
    }

    /// Push the winning `(value, version)` for `key` to every replica in `view`
    /// as a fire-and-forget repair. Idempotent: replicas already at or beyond
    /// this version merge it as a no-op.
    async fn read_repair(&self, view: &TabletView, key: &[u8], value: &[u8], version: u64) {
        let msg = DataMsg::Sync {
            tablet: view.tablet,
            epoch: view.epoch,
            entries: vec![(key.to_vec(), Some(value.to_vec()), version)],
        };
        self.broadcast(&view.replicas, &msg).await;
    }

    /// The highest version observed for `key` across a read quorum: `Some(0)` if
    /// no value exists, `Some(v)` for the latest version, `None` if a read
    /// quorum could not be reached. Used to assign a strictly-increasing version
    /// to the next write regardless of which coordinator issues it.
    pub async fn read_version(
        &self,
        view: &TabletView,
        key: &[u8],
        timeout: Duration,
    ) -> Option<u64> {
        self.quorum_read(view, key, timeout)
            .await
            .map(|qr| qr.best.map_or(0, |(ver, _)| ver))
    }

    /// Collect a read quorum, returning `None` if unreachable, else the winning
    /// value plus whether the responders diverged (so the caller can repair).
    async fn quorum_read(
        &self,
        view: &TabletView,
        key: &[u8],
        timeout: Duration,
    ) -> Option<QuorumRead> {
        self.metrics.incr(Metric::DataQuorumReadsAttempted);
        let req = self.env.next_u64();
        let msg = DataMsg::Read {
            req,
            tablet: view.tablet,
            epoch: view.epoch,
            key: key.to_vec(),
        };
        self.broadcast(&view.replicas, &msg).await;

        let mut oks = 0usize;
        let mut responded = 0usize;
        let mut best: Option<(u64, Vec<u8>)> = None;
        // The version each ok-responder returned (`None` = key absent there);
        // used after the fact to decide whether read-repair is warranted.
        let mut seen: Vec<Option<u64>> = Vec::new();
        self.collect(view.replicas.len(), timeout, |_from, reply| {
            if let DataMsg::ReadResp { req: r, ok, value } = reply {
                if r == req {
                    responded += 1;
                    if ok {
                        oks += 1;
                        seen.push(value.as_ref().map(|(ver, _)| *ver));
                        if let Some((ver, val)) = value {
                            if best.as_ref().is_none_or(|(bv, _)| ver > *bv) {
                                best = Some((ver, val));
                            }
                        }
                    }
                    return oks >= view.r || responded >= view.replicas.len();
                }
            }
            false
        })
        .await;

        if oks < view.r {
            self.metrics.incr(Metric::DataQuorumReadsFailed);
            return None;
        }
        self.metrics.incr(Metric::DataQuorumReadsSucceeded);
        // Diverged if any responder lagged the winning version (or lacked the
        // key while a winner exists).
        let diverged = best
            .as_ref()
            .is_some_and(|(bv, _)| seen.iter().any(|v| v.is_none_or(|ver| ver < *bv)));
        Some(QuorumRead { best, diverged })
    }

    async fn broadcast(&self, replicas: &[NodeId], msg: &DataMsg) {
        let bytes = serde_json::to_vec(msg).expect("data message serializes");
        for &replica in replicas {
            self.env.send(replica, bytes.clone()).await;
        }
    }

    /// Receive replies until `done(from, reply)` returns `true` or `timeout`
    /// elapses. `done` accumulates state (including, for writes/deletes, which
    /// replica `from` acked, to drive hinted handoff) and decides when the quorum
    /// is satisfied.
    async fn collect(
        &self,
        _total: usize,
        timeout: Duration,
        mut done: impl FnMut(NodeId, DataMsg) -> bool,
    ) {
        let deadline = self.env.now().0.saturating_add(dur_nanos(timeout));
        loop {
            let now = self.env.now().0;
            if now >= deadline {
                return;
            }
            let remaining = Duration::from_nanos(deadline - now);
            match select(self.env.recv(), self.env.sleep(remaining)).await {
                Either::Left((envelope, _)) => {
                    if let Ok(reply) = serde_json::from_slice::<DataMsg>(&envelope.payload) {
                        if done(envelope.from, reply) {
                            return;
                        }
                    }
                }
                Either::Right(((), _)) => return,
            }
        }
    }

    /// Buffer a hint for every replica in `view` that did not appear in `acked`
    /// (it was unavailable for this committed write/delete), so hinted handoff
    /// replays the `(value, version)` to it when it returns. No-op when hinted
    /// handoff is disabled. Residency-bounded: [`HintStore::record`] refuses a
    /// target the placement does not admit (ADR 0005).
    fn hint_unreached(
        &self,
        view: &TabletView,
        acked: &std::collections::BTreeSet<NodeId>,
        key: &[u8],
        value: Option<Vec<u8>>,
        version: u64,
    ) {
        let Some(hints) = &self.hints else {
            return;
        };
        for &replica in &view.replicas {
            if !acked.contains(&replica) {
                let stored = hints.record(
                    &self.allowed,
                    replica,
                    view.tablet,
                    view.epoch,
                    (key.to_vec(), value.clone(), version),
                );
                if stored {
                    self.metrics.incr(Metric::DataHintsStored);
                }
            }
        }
    }
}

fn dur_nanos(d: Duration) -> u64 {
    d.as_nanos().min(u128::from(u64::MAX)) as u64
}
