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

pub mod detector;
pub mod meta;
pub mod node;
pub mod persist;
pub mod raft;
pub mod schema;

pub use detector::{FailureDetector, Liveness};
pub use meta::{ApplyOutcome, Member, MetaCommand, Metadata, NodeStatus};
pub use node::RaftNode;
pub use schema::{ColumnDef, ColumnType, SchemaCatalog, SchemaError, TableName, TableSchema};
// Re-exported so downstream assemblers (e.g. `animusd`) can set a tablet's
// placement policy via `SetTabletPolicy` without taking a direct
// `animus-placement` dependency. The policy is part of the control plane's
// public metadata surface (`Metadata::policies`).
pub use animus_placement::PlacementPolicy;
pub use persist::{PersistedState, WalRecord};
pub use raft::{LogEntry, ProposeResult, RaftCore, RaftMsg, Role};
