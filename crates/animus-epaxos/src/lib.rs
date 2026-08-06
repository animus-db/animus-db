//! EPaxos-style **leaderless consensus** — an isolated exploration core
//! (ADR 0025).
//!
//! This crate is a from-scratch EPaxos (Moraru et al., SOSP 2013) in the same
//! shape as `animus-consensus`'s Accord: a synchronous, I/O-free [`EPaxosCore`]
//! state machine driven by a thin [`EPaxosNode`] over the `Env` seam, so it runs
//! under the deterministic simulator (ADR 0003). It is **not wired into any data
//! path** — it exists to build, understand, and correctness-test EPaxos against
//! the same harness that hardened Accord, and to compare the two protocols
//! directly. See ADR 0025 for the rationale and the relationship to Accord.
//!
//! ## Where EPaxos differs from Accord (the point of this crate)
//!
//! - **Order primitive.** EPaxos is *instance-space native*: a command lives in
//!   an [`InstanceId`] `(replica, slot)` and order is a **dependency graph** plus a
//!   per-command sequence number ([`Decision::seq`]). There is **no timestamp** —
//!   contrast `animus-consensus`'s Lamport `Timestamp`.
//! - **Fast quorum.** EPaxos's `f + ⌊(f+1)/2⌋` (smaller than Accord's simplified
//!   `N-1`), paid for by a harder recovery.
//! - **Execution.** Order is recovered by a Tarjan **SCC** pass over the committed
//!   dependency graph, ordered within a cycle by `seq` — versus Accord executing
//!   in a total timestamp order with no SCCs.
//!
//! ## Implemented here
//!
//! The steady-state agreement (PreAccept → fast-path Commit, or PreAccept →
//! Accept → Commit) with dependency + sequence tracking, durable before visible
//! (WAL fsync before shipping), and replica-view recovery on restart. See
//! [`core`](crate::core) for the precise gates.
//!
//! ## Deliberately deferred (the "build onto" surface)
//!
//! The **SCC executor** (agree order → *run* commands against a `StorageEngine`),
//! the **`Prepare` recovery** sub-protocol (take over a dead command leader — the
//! part EPaxos is notorious for, and what makes the small fast quorum
//! fault-recoverable), plus message retry, failure detection, WAL snapshotting,
//! read-only commands, and arbitrary write values — each of which already exists
//! in `animus-consensus` and slots in at the same sync-core boundary.

mod core;
mod instance;
mod message;
mod node;
mod persist;

pub use crate::core::{Decision, EPaxosCore, Key, Status};
pub use crate::instance::InstanceId;
pub use crate::message::{EPaxosMsg, Out};
pub use crate::node::EPaxosNode;
pub use crate::persist::{PersistedInstance, PersistedState, WalRecord};
