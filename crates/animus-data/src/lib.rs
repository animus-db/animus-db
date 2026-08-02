//! Leaderless AP data plane: quorum reads/writes for a single tablet, routing
//! via the control-plane tablet map, and epoch fencing (see ADR 0001, 0002).
//!
//! The data plane is the available half of AnimusDB. A [`DataClient`]
//! coordinator routes an operation to a tablet's replica set (a [`TabletView`]
//! it reads from cached control-plane metadata) and collects a quorum of
//! responses. With `R + W > N` a read observes the latest acknowledged write.
//! Each operation carries the epoch the coordinator believed current; a
//! [`replica`](crate::replica) **fences** operations bearing a stale epoch
//! (ADR 0002), which is how a replica rejects a coordinator acting on an
//! out-of-date tablet map.
//!
//! Because the coordinator routes from a *cached* view, the data plane keeps
//! serving even while the control plane is unavailable — only topology changes
//! (which bump the epoch) need the control plane (ADR 0001).
//!
//! - [`replica`] — the per-node storage replica server ([`serve_replica`]).
//! - [`client`] — the [`DataClient`] quorum coordinator.

pub mod client;
pub mod digest;
pub mod hint;
pub mod replica;

pub use client::{DataClient, ReadResult, Router, TabletView};
pub use hint::{
    HintLimits, HintStore, serve_hint_handoff, serve_hint_handoff_with_metrics, serve_hint_replay,
    serve_hint_replay_with_metrics,
};
pub use replica::{
    ReplicaHandle, serve_anti_entropy, serve_anti_entropy_with_metrics, serve_replica,
    serve_replica_with_residency,
};

use animus_env::NodeId;
use animus_tablet::{Epoch, TabletId};
use serde::{Deserialize, Serialize};

/// Correlates a request with its responses.
pub type ReqId = u64;

/// One reconciliation record carried by repair traffic: a `key`, its latest
/// value (`None` is a tombstone), and the MVCC `version` at which it was written.
/// This is the per-key unit a [`Sync`](DataMsg::Sync) batches and a segment
/// [`digest`](crate::digest) summarizes.
pub type SyncEntry = (Vec<u8>, Option<Vec<u8>>, u64);

/// Data-plane wire messages between a coordinator and a replica.
///
/// Each operation names the `tablet` it targets and carries the `epoch` the
/// coordinator believes current; the replica fences per tablet (ADR 0002).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DataMsg {
    /// Coordinator → replica: store `value` at `key` with MVCC `version`.
    Write {
        req: ReqId,
        tablet: TabletId,
        epoch: Epoch,
        key: Vec<u8>,
        value: Vec<u8>,
        version: u64,
    },
    /// Replica → coordinator: write acknowledgement (`ok == false` if fenced).
    WriteAck { req: ReqId, ok: bool },
    /// Coordinator → replica: tombstone `key` with MVCC `version` (per-key LWW,
    /// epoch-fenced exactly like [`Write`](DataMsg::Write)).
    Delete {
        req: ReqId,
        tablet: TabletId,
        epoch: Epoch,
        key: Vec<u8>,
        version: u64,
    },
    /// Replica → coordinator: delete acknowledgement (`ok == false` if fenced).
    DeleteAck { req: ReqId, ok: bool },
    /// Coordinator → replica: read the latest value at `key`.
    Read {
        req: ReqId,
        tablet: TabletId,
        epoch: Epoch,
        key: Vec<u8>,
    },
    /// Replica → coordinator: read response. `ok == false` if fenced; `value`
    /// is `(version, bytes)` or `None` if the key is absent.
    ReadResp {
        req: ReqId,
        ok: bool,
        value: Option<(u64, Vec<u8>)>,
    },
    /// Coordinator → replica: scan the half-open key range `[start, end)`,
    /// returning each key's latest record (value or tombstone) the replica holds.
    /// Epoch-fenced exactly like a point [`Read`](DataMsg::Read).
    ScanRange {
        req: ReqId,
        tablet: TabletId,
        epoch: Epoch,
        start: Vec<u8>,
        end: Vec<u8>,
    },
    /// Replica → coordinator: scan response. `ok == false` if fenced; otherwise
    /// `entries` is the replica's latest record per key in `[start, end)`,
    /// sorted by key, **including tombstones** (`value == None`), so the
    /// coordinator can merge by per-key newest version and then drop deleted
    /// keys — a replica that holds a stale value must not mask a peer's newer
    /// tombstone.
    ScanResp {
        req: ReqId,
        ok: bool,
        entries: Vec<SyncEntry>,
    },
    /// Peer → peer (anti-entropy) or coordinator → replica (read-repair):
    /// reconcile a batch of `(key, value, version)` into the replica's storage
    /// by per-key last-writer-wins, where `value` is `None` for a **tombstone**
    /// (so deletes propagate too, ADR 0010). Fire-and-forget — no
    /// acknowledgement, and fenced as a whole on a stale `epoch` (ADR 0002).
    Sync {
        tablet: TabletId,
        epoch: Epoch,
        entries: Vec<SyncEntry>,
    },
    /// Peer → peer (anti-entropy): the sender's **segment digest** — a summary of
    /// its data partitioned into a fixed number of key segments, each carrying a
    /// content hash and entry count. The receiver compares it against its own
    /// digest and asks (via [`SyncPull`](DataMsg::SyncPull)) only for the segments
    /// that differ, so a converged pair exchanges no data at all and a divergent
    /// pair transfers only the affected ranges — not the full digest each round
    /// (ADR 0010, the Merkle/segment-digest optimization). Fenced on a stale
    /// `epoch`.
    SyncDigest {
        tablet: TabletId,
        epoch: Epoch,
        from: NodeId,
        segments: Vec<SegmentDigest>,
    },
    /// Peer → peer (anti-entropy): in response to a [`SyncDigest`](DataMsg::SyncDigest),
    /// the receiver asks the sender to push back the contents of the named
    /// `segments` (the ones whose digest disagreed). The sender replies with a
    /// [`Sync`](DataMsg::Sync) of just those segments' entries. Fenced on a stale
    /// `epoch`.
    SyncPull {
        tablet: TabletId,
        epoch: Epoch,
        from: NodeId,
        segments: Vec<u32>,
    },
    /// Hint holder → replica: a liveness probe used by **hinted handoff**
    /// (ADR 0010). When a coordinator holds hints for a replica that was
    /// unavailable during a write/delete, it periodically probes the replica;
    /// a matching [`ProbeAck`](DataMsg::ProbeAck) means the replica is reachable
    /// again and the holder replays its hints (as a [`Sync`](DataMsg::Sync)).
    /// Not epoch-fenced — it carries no data, only liveness.
    Probe { req: ReqId },
    /// Replica → hint holder: acknowledges a [`Probe`](DataMsg::Probe), proving
    /// the replica is reachable so the holder may replay buffered hints to it.
    ProbeAck { req: ReqId },
}

/// One segment of a replica's [segment digest](DataMsg::SyncDigest): a segment
/// index plus a content hash and entry count over the keys that fall in it. Two
/// replicas agree on a segment iff `(hash, count)` match; a mismatch is the
/// signal to exchange that segment's entries. Including `count` makes an
/// empty-vs-nonempty segment always differ even on a hash of `0`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentDigest {
    /// The segment index (a bucket over the hashed keyspace).
    pub segment: u32,
    /// Order-independent content hash of every `(key, value, version)` in the
    /// segment (folded by XOR, so it is commutative and the digest does not
    /// depend on entry order).
    pub hash: u64,
    /// Number of entries (live or tombstoned) in the segment.
    pub count: u32,
}
