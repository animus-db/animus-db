//! Leaderless AP data plane: quorum reads/writes for a single tablet, routing
//! via the control-plane tablet map, and epoch fencing (see ADR 0001, 0002).
//!
//! The data plane is the available half of CustosDB. A [`DataClient`]
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
pub mod replica;

pub use client::{DataClient, ReadResult, Router, TabletView};
pub use replica::{ReplicaHandle, serve_anti_entropy, serve_replica};

use custos_tablet::{Epoch, TabletId};
use serde::{Deserialize, Serialize};

/// Correlates a request with its responses.
pub type ReqId = u64;

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
    /// Peer → peer (anti-entropy) or coordinator → replica (read-repair):
    /// reconcile a batch of `(key, value, version)` into the replica's storage
    /// by per-key last-writer-wins. Fire-and-forget — no acknowledgement, and
    /// fenced as a whole on a stale `epoch` (ADR 0002).
    Sync {
        tablet: TabletId,
        epoch: Epoch,
        entries: Vec<(Vec<u8>, Vec<u8>, u64)>,
    },
}
