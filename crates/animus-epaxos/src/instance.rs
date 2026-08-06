//! Instance identifiers — the EPaxos analogue of a Raft log index.
//!
//! Unlike Accord (which orders by a Lamport [`Timestamp`](../../animus-consensus))
//! EPaxos is **instance-space native**: every command is proposed by its command
//! leader into an instance slot it owns, `(replica, slot)`. A command *is*
//! identified by the instance it lives in, and dependencies are sets of instance
//! ids. There is no timestamp anywhere in the ordering path — order is recovered
//! from the dependency graph plus a per-command sequence number.

use animus_env::NodeId;
use serde::{Deserialize, Serialize};

/// The identity of a command: the replica that owns the instance and the slot
/// within that replica's instance space. Totally ordered (`replica` then `slot`)
/// so it can key a `BTreeMap`/`BTreeSet` deterministically and break ties inside
/// a dependency cycle at execution time (the SCC executor, deferred — see the
/// crate docs).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct InstanceId {
    /// The command leader / owning replica of this instance.
    pub replica: NodeId,
    /// The monotonically increasing slot within `replica`'s own instance space.
    pub slot: u64,
}

impl InstanceId {
    /// Construct an instance id from its parts.
    #[must_use]
    pub fn new(replica: NodeId, slot: u64) -> InstanceId {
        InstanceId { replica, slot }
    }
}
