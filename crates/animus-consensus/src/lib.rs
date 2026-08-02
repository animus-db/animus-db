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
//!   `animus-control`'s `RaftCore`: it returns outbound messages and holds no
//!   `Env`. The happy path (fast PreAccept → Commit) and the slow path
//!   (PreAccept → Accept → Commit) are both implemented, with dependency
//!   tracking by key-set conflict.
//! - Totally-ordered [`Timestamp`]s with a per-node [`LogicalClock`].
//! - A thin [`AccordNode`] driver over the `Env` seam.
//!
//! ## What this slice now does (execution, durability, storage, failover)
//!
//! - **Storage-backed execution / Apply**: a committed transaction is executed
//!   in agreed `(execute_at, txn)` order, only after every dependency that orders
//!   before it has executed — a per-replica execution queue, so every replica
//!   applies conflicting transactions in the **same order**. The effect is
//!   applied to a real (async) [`StorageEngine`](animus_storage::StorageEngine)
//!   (the in-memory [`MemoryEngine`](animus_storage::MemoryEngine) under
//!   simulation): each executed transaction `merge`s its id into every key it
//!   touches, stamped with its execution timestamp as the MVCC version. The
//!   sync [`AccordCore`] decides the order and emits [`ApplyEffect`]s; the
//!   [`AccordNode`] driver performs the async storage writes.
//! - **Durability / recovery**: [`AccordCore`] emits [`WalRecord`]s at each
//!   phase transition; [`AccordNode`] fsyncs them before acting and recovers the
//!   core from the WAL on restart — mirroring `RaftCore` — replaying its
//!   execution order back into a fresh storage engine.
//! - **Coordinator failover (first slice)**: if a coordinator dies after
//!   PreAccept/Accept but before replicas learn the Commit, another replica can
//!   take over via [`AccordCore::recover`] / [`AccordNode::recover`]: it queries
//!   replicas (`Recover`/`RecoverOk`) for their recorded `(phase, execute_at,
//!   deps)` and drives the transaction to a consistent commit — adopting an
//!   already-committed decision, else forcing the slow path (never the fast
//!   path). See the recovery rules in [`crate::core`].
//!
//! ## What this slice deliberately does NOT do (see ADR 0011)
//!
//! - **The precise Accord recovery rules**: the recovery here is a simplified
//!   "adopt-committed, else max-ts + union-deps + force-slow-path"; the exact
//!   `PreAcceptOk`-witness/ballot recovery and duelling recovery coordinators are
//!   deferred. There is also no *failure detector* yet — recovery is triggered
//!   explicitly (e.g. by a test).
//! - **Full data-plane integration**: execution is backed by a `StorageEngine`,
//!   but it is a per-node consensus store, not yet wired to the live data-plane
//!   replicas (`animus-data`); read transactions are also out of scope.
//! - **The full transitive dependency wait-graph**: the execution wait is
//!   conflict-and-timestamp based.
//! - **WAL snapshotting / log truncation**: the WAL holds the full
//!   per-transaction history (no compaction yet — contrast `RaftCore`).
//! - **Contention / livelock handling, timeouts/retries, the precise fast-path
//!   quorum bound, and sharding/placement** (one global replica set for now).

mod core;
mod message;
mod node;
mod persist;
mod timestamp;

pub use crate::core::{AccordCore, ApplyEffect, Decision, Key, Phase, ReadEffect, TxnId};
pub use crate::message::{AccordMsg, Out};
pub use crate::node::{AccordNode, InteractiveTxn};
pub use crate::persist::{PersistedState, PersistedTxn, WalRecord};
pub use crate::timestamp::{LogicalClock, Timestamp};
