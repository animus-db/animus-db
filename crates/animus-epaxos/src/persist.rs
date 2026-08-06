//! EPaxos durable state.
//!
//! [`EPaxosCore`](crate::core::EPaxosCore) is pure and does no I/O; it emits
//! [`WalRecord`]s describing changes to an instance's durable state, which the
//! node driver appends to a write-ahead log on the `Env` disk and `fsync`s
//! **before** acting on them (replying to a peer, shipping a `Commit`). On
//! startup the driver replays the log into a [`PersistedState`] and recovers the
//! core. This mirrors `animus-consensus::persist` and the control plane's
//! `animus-control::persist`.
//!
//! **Skeleton scope.** Only the *replica facts* (per-instance keys / seq / deps /
//! status) are made durable and recovered — enough that a restarted replica keeps
//! every committed instance. WAL **snapshotting / log truncation** and recovering
//! the *coordinator* (leader) view are deferred (see the crate docs); replay here
//! assumes append order (records for one instance arrive in phase order), which
//! the single driver guarantees.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::core::{Key, Status};
use crate::instance::InstanceId;

/// One durable change to a single instance's replica state, appended to the WAL.
/// Each record is self-describing for the instance it names, so replaying them in
/// order rebuilds every replica fact the core needs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalRecord {
    /// An instance was witnessed via `PreAccept`: its keys and this replica's
    /// merged `(seq, deps)`.
    PreAccepted {
        instance: InstanceId,
        keys: BTreeSet<Key>,
        seq: u64,
        deps: BTreeSet<InstanceId>,
    },
    /// A coordinator-chosen `(seq, deps)` was adopted via `Accept` (slow path).
    Accepted {
        instance: InstanceId,
        keys: BTreeSet<Key>,
        seq: u64,
        deps: BTreeSet<InstanceId>,
    },
    /// The final `(seq, deps)` was recorded via `Commit` — the durable agreement
    /// point. After this record is fsynced the instance is committed here.
    Committed {
        instance: InstanceId,
        keys: BTreeSet<Key>,
        seq: u64,
        deps: BTreeSet<InstanceId>,
    },
}

impl WalRecord {
    /// The instance this record concerns.
    #[must_use]
    pub fn instance(&self) -> InstanceId {
        match self {
            WalRecord::PreAccepted { instance, .. }
            | WalRecord::Accepted { instance, .. }
            | WalRecord::Committed { instance, .. } => *instance,
        }
    }
}

/// The durable replica facts about one instance, rebuilt by folding the WAL.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PersistedInstance {
    /// The command's conflict key set.
    pub keys: BTreeSet<Key>,
    /// The sequence number (cycle-breaker for the execution order).
    pub seq: u64,
    /// The dependency set (interfering instances).
    pub deps: BTreeSet<InstanceId>,
    /// The furthest phase reached durably.
    pub status: Status,
}

/// Durable EPaxos replica state reconstructed by replaying the write-ahead log.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PersistedState {
    /// Per-instance durable facts, keyed by instance id.
    pub instances: BTreeMap<InstanceId, PersistedInstance>,
}

impl PersistedState {
    /// Whether the log was empty (a never-before-run node).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.instances.is_empty()
    }

    /// Reconstruct durable state by folding the WAL records in order. `keys` are a
    /// monotone fact (unioned); `status` never downgrades; `seq`/`deps` take the
    /// latest phase's value (records for one instance arrive in phase order).
    pub fn replay(records: impl IntoIterator<Item = WalRecord>) -> Self {
        let mut state = Self::default();
        for record in records {
            let instance = record.instance();
            let entry = state.instances.entry(instance).or_default();
            match record {
                WalRecord::PreAccepted {
                    keys, seq, deps, ..
                } => {
                    entry.keys.extend(keys);
                    entry.seq = entry.seq.max(seq);
                    entry.deps.extend(deps);
                    entry.status = entry.status.max(Status::PreAccepted);
                }
                WalRecord::Accepted {
                    keys, seq, deps, ..
                } => {
                    entry.keys.extend(keys);
                    entry.seq = seq;
                    entry.deps = deps;
                    entry.status = entry.status.max(Status::Accepted);
                }
                WalRecord::Committed {
                    keys, seq, deps, ..
                } => {
                    entry.keys.extend(keys);
                    entry.seq = seq;
                    entry.deps = deps;
                    entry.status = entry.status.max(Status::Committed);
                }
            }
        }
        state
    }

    /// Encode a single record as one newline-terminated JSON line for the WAL.
    /// (`serde_json` never emits raw newlines, so the framing is unambiguous.)
    #[must_use]
    pub fn encode_record(record: &WalRecord) -> Vec<u8> {
        let mut bytes = serde_json::to_vec(record).expect("wal record serializes");
        bytes.push(b'\n');
        bytes
    }

    /// Decode the WAL bytes back into records, ignoring a trailing partial line (a
    /// write torn by a crash — its effect was never acted on).
    pub fn decode(bytes: &[u8]) -> Vec<WalRecord> {
        bytes
            .split(|&b| b == b'\n')
            .filter(|line| !line.is_empty())
            .filter_map(|line| serde_json::from_slice(line).ok())
            .collect()
    }
}
