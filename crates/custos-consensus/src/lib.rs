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
//! ## What this slice now does (execution + durability milestone)
//!
//! - **Execution / Apply**: a committed transaction is executed (applied to a
//!   small opaque key→last-writer store) in agreed `(execute_at, txn)` order,
//!   only after every dependency that orders before it has executed — a
//!   per-replica execution queue, so every replica applies conflicting
//!   transactions in the **same order**.
//! - **Durability / recovery**: [`AccordCore`] emits [`WalRecord`]s at each
//!   phase transition; [`AccordNode`] fsyncs them before acting and recovers the
//!   core from the WAL on restart — mirroring `RaftCore`.
//!
//! ## What this slice deliberately does NOT do (see ADR 0011)
//!
//! - **Coordinator failover / recovery**: a coordinator that dies mid-flight
//!   strands its transaction; there is no recovery coordinator. (A *replica*
//!   restart is now recovered.)
//! - **Full data-plane / `StorageEngine` integration**: the executed store here
//!   is a stand-in to demonstrate consistent order, not the real data plane.
//! - **WAL snapshotting / log truncation**: the WAL holds the full
//!   per-transaction history (no compaction yet — contrast `RaftCore`).
//! - **Contention / livelock handling, timeouts/retries, the precise fast-path
//!   quorum bound, and sharding/placement** (one global replica set for now).

mod core;
mod message;
mod node;
mod persist;
mod timestamp;

pub use crate::core::{AccordCore, Decision, Key, Phase, TxnId};
pub use crate::message::{AccordMsg, Out};
pub use crate::node::AccordNode;
pub use crate::persist::{PersistedState, PersistedTxn, WalRecord};
pub use crate::timestamp::{LogicalClock, Timestamp};
