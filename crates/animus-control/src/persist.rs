//! Raft durable state (ADR 0009 follow-up).
//!
//! [`RaftCore`](crate::raft::RaftCore) is pure and does no I/O; instead it emits
//! [`WalRecord`]s describing changes to its durable state, which the node driver
//! appends to a write-ahead log on the `Env` disk and `fsync`s **before** acting
//! on them (granting a vote, acknowledging an append). On startup the driver
//! replays the log into a [`PersistedState`] and recovers the core.
//!
//! The state machine is snapshotted as a full [`Metadata`] image at a committed
//! `(last_index, last_term)`; the log keeps only entries *after* that index.
//! Recovery restores the snapshot, then re-applies the log tail as the leader
//! re-advances commit — so a committed command is applied exactly once relative
//! to the snapshot base (no double-applied compare-and-swap), while the log
//! prefix the snapshot covers is discarded.

use animus_env::NodeId;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::meta::{MetaCommand, Metadata};
use crate::raft::LogEntry;

/// One durable change, appended to the write-ahead log. Generic over the command
/// type `C` and snapshot-image type `S` (defaults: the control plane's
/// [`MetaCommand`] / [`Metadata`]), so the same WAL machinery serves any
/// `RaftCore<C, S>`. The generic is erased in the JSON form, so the on-disk
/// encoding for the control plane is unchanged.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WalRecord<C = MetaCommand, S = Metadata> {
    /// Persisted hard state: current term and vote (must be durable before the
    /// vote/term is acted on).
    Hard {
        term: u64,
        voted_for: Option<NodeId>,
    },
    /// A log entry was appended.
    Append(LogEntry<C>),
    /// The log was truncated to `keep` entries (conflict resolution).
    Truncate { keep: usize },
    /// A state-machine snapshot: the applied state covering all entries through
    /// `last_index` (whose term is `last_term`). The log keeps only entries
    /// after `last_index`.
    Snapshot {
        metadata: S,
        last_index: u64,
        last_term: u64,
    },
}

/// Durable Raft state reconstructed by replaying the write-ahead log. Generic over
/// the command / snapshot-image types (defaults: [`MetaCommand`] / [`Metadata`]).
#[derive(Clone, Debug)]
pub struct PersistedState<C = MetaCommand, S = Metadata> {
    /// Persisted current term.
    pub term: u64,
    /// Persisted vote for the current term.
    pub voted_for: Option<NodeId>,
    /// The reconstructed log (entries after the snapshot's `last_index`).
    pub log: Vec<LogEntry<C>>,
    /// The latest snapshot: `(state, last_index, last_term)`.
    pub snapshot: Option<(S, u64, u64)>,
}

// Manual `Default` (not derived): the derive would demand `C: Default` + `S:
// Default`, but an empty `PersistedState` needs neither (the log/snapshot default
// to empty/`None`), and `MetaCommand` is not `Default`.
impl<C, S> Default for PersistedState<C, S> {
    fn default() -> Self {
        Self {
            term: 0,
            voted_for: None,
            log: Vec::new(),
            snapshot: None,
        }
    }
}

impl<C, S> PersistedState<C, S>
where
    C: Serialize + DeserializeOwned,
    S: Serialize + DeserializeOwned,
{
    /// Whether the log was empty (a never-before-run node).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.term == 0 && self.voted_for.is_none() && self.log.is_empty() && self.snapshot.is_none()
    }

    /// Reconstruct durable state by folding the WAL records in order.
    pub fn replay(records: impl IntoIterator<Item = WalRecord<C, S>>) -> Self {
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
                    last_index,
                    last_term,
                } => {
                    state.snapshot = Some((metadata, last_index, last_term));
                }
            }
        }
        state
    }

    /// Encode a single record as one newline-terminated JSON line for the WAL.
    /// (`serde_json` never emits raw newlines, so the framing is unambiguous.)
    #[must_use]
    pub fn encode_record(record: &WalRecord<C, S>) -> Vec<u8> {
        let mut bytes = serde_json::to_vec(record).expect("wal record serializes");
        bytes.push(b'\n');
        bytes
    }

    /// Decode the WAL bytes back into records, ignoring a trailing partial line
    /// (a write torn by a crash — its effect was never acted on).
    pub fn decode(bytes: &[u8]) -> Vec<WalRecord<C, S>> {
        bytes
            .split(|&b| b == b'\n')
            .filter(|line| !line.is_empty())
            .filter_map(|line| serde_json::from_slice(line).ok())
            .collect()
    }
}
