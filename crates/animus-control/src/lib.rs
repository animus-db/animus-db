//! Control plane: a Raft-replicated metadata state machine holding cluster
//! membership and the tablet map, mutated by compare-and-swap (epoch)
//! transactions (see `docs/adr/0001-two-plane-architecture.md`).
//!
//! The control plane is the strongly-consistent half of AnimusDB. It runs a
//! small in-house Raft over the `Env` seam (see
//! `docs/adr/0009-in-house-raft-over-env.md`) so it is fully deterministic under
//! simulation. The data plane (`animus-data`) reads the tablet map this plane
//! maintains and keeps serving from a cached copy even when this plane is
//! unavailable.
//!
//! - [`meta`] — the replicated state machine: [`Metadata`], [`MetaCommand`].
//! - [`raft`] — the synchronous [`RaftCore`] consensus state machine.
//! - [`node`] — [`RaftNode`], the `Env`-driven node wrapping the core.
//! - [`detector`] — the pure [`FailureDetector`] (ADR 0012): heartbeat-based
//!   liveness, driven by the leader to mark members `Down`/`Active` automatically.
//! - [`schema`] — the replicated table-schema catalog (ADR 0013):
//!   [`TableSchema`] and [`SchemaCatalog`], held in [`Metadata`] and mutated by
//!   `MetaCommand::{CreateTableSchema, DropTableSchema}`, so a wire adapter's
//!   `CreateTable`/`CREATE TABLE` survives restart and is agreed cluster-wide.
//! - [`syskv`] — the reserved system-keyspace key encoding (ADR 0038): pure,
//!   unwired in this PR (a later PR in the stack mirrors `Metadata` through it
//!   into a per-node `StorageEngine`). `syskv::is_reserved_name` guards
//!   `TableName`/keyspace validation so no user table can ever collide with it.
//! - [`delta_ring`] — the apply task's bounded, per-node in-memory ring of
//!   [`mirror::KeyWrite`] deltas (ADR 0038 PR5), the incremental half of
//!   `WatchMetadata`'s reply (`RaftNode::watch_delta_since`).

pub mod delta_ring;
pub mod detector;
pub mod meta;
pub mod mirror;
pub mod node;
pub mod persist;
/// Persist-round accounting shared by both planes' Raft drivers (issue #279).
pub mod persist_round;
pub mod raft;
pub mod schema;
pub mod shared_wal;
pub mod syskv;

pub use delta_ring::DeltaRing;
pub use detector::{FailureDetector, Liveness};
pub use meta::{
    ApplyOutcome, BackupId, BackupManifest, BackupPinnedTablet, BackupRow, BackupStatus,
    BackupTabletProgress, Member, MetaCommand, Metadata, NodeAddrs, NodeStatus, StreamShardRow,
};
pub use node::{DeltaReply, MetadataChanged, MetadataWatch, RaftNode};
pub use schema::{
    ColumnDef, ColumnType, IndexDef, IndexKind, IndexProjection, IndexStatus, SchemaCatalog,
    SchemaError, StreamSpec, StreamViewType, TableName, TableSchema, TtlSpec,
};
// Re-exported so downstream assemblers (e.g. `animusd`) can set a tablet's
// placement policy via `SetTabletPolicy` without taking a direct
// `animus-placement` dependency. The policy is part of the control plane's
// public metadata surface (`Metadata::policies`).
pub use animus_placement::PlacementPolicy;
// ADR 0050 fork F5: `animusd`'s `BeginSplit` proposer picks the split
// children's final homes via the placement engine — re-exported here (like
// `PlacementPolicy` above) so the assembler still needs no direct
// `animus-placement` dependency.
pub use animus_placement::{Candidate, select_replicas_balanced};
pub use persist::{PersistedState, WalRecord};
pub use raft::{LogEntry, MemberRole, ProposeResult, RaftCore, RaftMsg, Role, StateMachine};
pub use shared_wal::SharedWal;
