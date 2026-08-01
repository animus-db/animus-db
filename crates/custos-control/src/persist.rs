//! Raft durable state (ADR 0009 follow-up).
//!
//! [`RaftCore`](crate::raft::RaftCore) is pure and does no I/O; instead it emits
//! [`WalRecord`]s describing changes to its durable state, which the node driver
//! appends to a write-ahead log on the `Env` disk and `fsync`s **before** acting
//! on them (granting a vote, acknowledging an append). On startup the driver
//! replays the log into a [`PersistedState`] and recovers the core.
//!
//! The applied state machine is checkpointed (a full [`Metadata`] snapshot plus
//! `last_applied`) so recovery restores it directly rather than re-applying the
//! log — which would double-apply non-idempotent commands such as a
//! compare-and-swap. **Not yet implemented:** log compaction/truncation of the
//! WAL (it grows unbounded; recovery just uses the latest snapshot).

use custos_env::NodeId;
use serde::{Deserialize, Serialize};

use crate::meta::Metadata;
use crate::raft::LogEntry;

/// One durable change, appended to the write-ahead log.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WalRecord {
    /// Persisted hard state: current term and vote (must be durable before the
    /// vote/term is acted on).
    Hard {
        term: u64,
        voted_for: Option<NodeId>,
    },
    /// A log entry was appended.
    Append(LogEntry),
    /// The log was truncated to `keep` entries (conflict resolution).
    Truncate { keep: usize },
    /// A state-machine checkpoint: the applied metadata at `last_applied`.
    Snapshot {
        metadata: Metadata,
        last_applied: u64,
    },
}

/// Durable Raft state reconstructed by replaying the write-ahead log.
#[derive(Clone, Debug, Default)]
pub struct PersistedState {
    /// Persisted current term.
    pub term: u64,
    /// Persisted vote for the current term.
    pub voted_for: Option<NodeId>,
    /// The reconstructed log.
    pub log: Vec<LogEntry>,
    /// The latest state-machine checkpoint: `(metadata, last_applied)`.
    pub snapshot: Option<(Metadata, u64)>,
}

impl PersistedState {
    /// Whether the log was empty (a never-before-run node).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.term == 0 && self.voted_for.is_none() && self.log.is_empty() && self.snapshot.is_none()
    }

    /// Reconstruct durable state by folding the WAL records in order.
    pub fn replay(records: impl IntoIterator<Item = WalRecord>) -> Self {
        let mut state = Self::default();
        for record in records {
            match record {
                WalRecord::Hard { term, voted_for } => {
                    state.term = term;
                    state.voted_for = voted_for;
                }
                WalRecord::Append(entry) => state.log.push(entry),
                WalRecord::Truncate { keep } => state.log.truncate(keep),
                WalRecord::Snapshot {
                    metadata,
                    last_applied,
                } => {
                    state.snapshot = Some((metadata, last_applied));
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

    /// Decode the WAL bytes back into records, ignoring a trailing partial line
    /// (a write torn by a crash — its effect was never acted on).
    pub fn decode(bytes: &[u8]) -> Vec<WalRecord> {
        bytes
            .split(|&b| b == b'\n')
            .filter(|line| !line.is_empty())
            .filter_map(|line| serde_json::from_slice(line).ok())
            .collect()
    }
}
