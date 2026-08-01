//! Accord-style leaderless transaction consensus (ADR 0011) — a **first,
//! minimal slice**.
//!
//! Accord (Apache Cassandra's leaderless consensus) gives every transaction a
//! unique, globally-comparable timestamp and agrees on an *execution* timestamp
//! and a dependency set via a small number of message rounds:
//!
//! - **PreAccept**: a coordinator proposes `t0`; each replica replies with the
//!   highest timestamp it has assigned to a conflicting transaction plus those
//!   conflicts (the new transaction's dependencies).
//! - **Accept** (slow path only): if a fast quorum did not agree on `t0`, the
//!   coordinator picks a higher execution timestamp, unions the dependencies,
//!   and collects acks.
//! - **Commit**: the coordinator broadcasts the agreed `(execute_at, deps)`.
//!
//! The fast path commits in one round trip (PreAccept → Commit) when a fast
//! quorum agrees on `t0`.
//!
//! ## What this slice implements
//!
//! - A synchronous, I/O-free [`AccordCore`] state machine mirroring
//!   `custos-control`'s `RaftCore`: it returns outbound messages and holds no
//!   `Env`. The happy path (fast PreAccept → Commit) and the slow path
//!   (PreAccept → Accept → Commit) are both implemented, with dependency
//!   tracking by key-set conflict.
//! - Totally-ordered [`Timestamp`]s with a per-node [`LogicalClock`].
//! - A thin [`AccordNode`] driver over the `Env` seam.
//!
//! ## What this slice deliberately does NOT do (see ADR 0011)
//!
//! - **Execution / Apply**: transactions agree on an order but no effect is
//!   applied to storage; there is no integration with the data plane yet.
//! - **Durability / recovery**: the core keeps state in memory; there is no WAL
//!   and no recovery (contrast `RaftCore`, which already has both).
//! - **Coordinator failover / recovery**: a coordinator that dies mid-flight
//!   strands its transaction; there is no recovery coordinator.
//! - **The full dependency wait-graph**: we record deps and can show two
//!   conflicting transactions commit in a consistent order, but do not implement
//!   the execution-time blocking-on-deps semantics in full.
//! - **Contention / livelock handling, the precise fast-path quorum bound, and
//!   sharding/placement** (one global replica set for now).

mod core;
mod message;
mod node;
mod timestamp;

pub use crate::core::{AccordCore, Decision, Key, Phase, TxnId};
pub use crate::message::{AccordMsg, Out};
pub use crate::node::AccordNode;
pub use crate::timestamp::{LogicalClock, Timestamp};
