//! The quorum coordinator: routes an operation to a tablet's replicas and
//! collects a quorum of responses.

use std::time::Duration;

use custos_env::{Env, NodeId};
use custos_tablet::{Epoch, Tablet, TabletId};
use futures::future::{Either, select};

use crate::DataMsg;

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
#[derive(Clone)]
pub struct DataClient<E: Env> {
    env: E,
}

impl<E: Env> DataClient<E> {
    /// Create a coordinator on `env`.
    pub fn new(env: E) -> Self {
        Self { env }
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
        self.collect(view.replicas.len(), timeout, |reply| {
            if let DataMsg::WriteAck { req: r, ok } = reply {
                if r == req {
                    responded += 1;
                    if ok {
                        acks += 1;
                    }
                    return acks >= view.w || responded >= view.replicas.len();
                }
            }
            false
        })
        .await;
        acks >= view.w
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
        self.collect(view.replicas.len(), timeout, |reply| {
            if let DataMsg::DeleteAck { req: r, ok } = reply {
                if r == req {
                    responded += 1;
                    if ok {
                        acks += 1;
                    }
                    return acks >= view.w || responded >= view.replicas.len();
                }
            }
            false
        })
        .await;
        acks >= view.w
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
                        self.read_repair(view, key, val, *ver).await;
                    }
                }
                ReadResult::Value(qr.best.map(|(_, v)| v))
            }
            None => ReadResult::Failed,
        }
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
        self.collect(view.replicas.len(), timeout, |reply| {
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
            return None;
        }
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

    /// Receive replies until `done(reply)` returns `true` or `timeout` elapses.
    /// `done` accumulates state and decides when the quorum is satisfied.
    async fn collect(
        &self,
        _total: usize,
        timeout: Duration,
        mut done: impl FnMut(DataMsg) -> bool,
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
                        if done(reply) {
                            return;
                        }
                    }
                }
                Either::Right(((), _)) => return,
            }
        }
    }
}

fn dur_nanos(d: Duration) -> u64 {
    d.as_nanos().min(u128::from(u64::MAX)) as u64
}
