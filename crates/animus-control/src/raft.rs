//! A minimal, synchronous Raft core (ADR 0009).
//!
//! [`RaftCore`] holds no I/O: a driver (see [`crate::node`]) owns the `Env`,
//! feeds the core decoded messages and timer ticks, and ships the outbound
//! messages the core returns. All time and randomness arrive as parameters, so
//! the core is a pure, testable state machine and the whole control plane stays
//! deterministic under simulation.
//!
//! Implemented Raft rules: terms and single-vote-per-term, log up-to-dateness
//! for granting votes, randomized election timeouts, `AppendEntries` consistency
//! check with conflict truncation, and commit advancement restricted to
//! current-term entries via majority `matchIndex`. The log is offset by a
//! state-machine snapshot: [`snapshot`] truncates the covered prefix, and a
//! follower that has fallen behind the leader's compacted prefix is caught up
//! with `InstallSnapshot`. Durability is handled out-of-band: the core emits
//! [`WalRecord`]s (see [`drain_persist`]) that the driver persists, rewriting the
//! WAL to [`wal_image`] on a snapshot; [`recovered`] restores the snapshot and
//! re-applies the tail.
//!
//! [`drain_persist`]: RaftCore::drain_persist
//! [`wal_image`]: RaftCore::wal_image
//! [`snapshot`]: RaftCore::snapshot
//! [`recovered`]: RaftCore::recovered

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use animus_env::{Nanos, NodeId};
use serde::{Deserialize, Serialize};

use crate::meta::{MetaCommand, Metadata};
use crate::persist::{PersistedState, WalRecord};

/// Maximum bytes of serialized snapshot carried by a single `InstallSnapshot`
/// message. A snapshot larger than this is shipped over several offset-addressed
/// chunks and reassembled by the follower (ADR 0009). Small enough that a
/// realistic metadata snapshot spans multiple chunks; the value only affects
/// message granularity, never correctness.
pub const SNAPSHOT_CHUNK_BYTES: usize = 1024;

/// A replicated log entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    /// Leader term in which the entry was created.
    pub term: u64,
    /// 1-based position in the log.
    pub index: u64,
    /// The metadata mutation.
    pub command: MetaCommand,
}

/// Wire messages exchanged between Raft peers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RaftMsg {
    /// Candidate solicits a vote.
    RequestVote {
        term: u64,
        candidate: NodeId,
        last_log_index: u64,
        last_log_term: u64,
    },
    /// Response to [`RaftMsg::RequestVote`].
    RequestVoteResp { term: u64, granted: bool },
    /// Leader replicates entries (empty = heartbeat).
    AppendEntries {
        term: u64,
        leader: NodeId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    /// Response to [`RaftMsg::AppendEntries`].
    AppendEntriesResp {
        term: u64,
        success: bool,
        /// Highest log index now known to match on the follower.
        match_index: u64,
    },
    /// One **offset-addressed chunk** of the leader's state-machine snapshot,
    /// shipped to a follower whose log has fallen behind the leader's compacted
    /// prefix. The snapshot (a serialized [`Metadata`]) is split into chunks of
    /// at most `SNAPSHOT_CHUNK_BYTES`; the follower reassembles them by `offset`
    /// and installs once `done` (the final chunk) arrives. `total` is the full
    /// serialized length, so the follower can detect a complete transfer. The
    /// snapshot is installed atomically only once every byte is present —
    /// partial chunks never touch the state machine.
    InstallSnapshot {
        term: u64,
        leader: NodeId,
        last_index: u64,
        last_term: u64,
        /// Byte offset of this chunk within the serialized snapshot.
        offset: u64,
        /// The chunk bytes.
        data: Vec<u8>,
        /// Total serialized snapshot length (same for every chunk of a transfer).
        total: u64,
        /// Whether this is the final chunk.
        done: bool,
    },
    /// Response to [`RaftMsg::InstallSnapshot`]. `next_offset` is the number of
    /// contiguous snapshot bytes the follower now holds — the offset the leader
    /// should send next (it equals `total` once the whole snapshot is installed).
    /// `last_index` echoes the installed snapshot index once the transfer
    /// completes (0 while still in progress), so the leader can then advance
    /// `next_index`/`match_index`.
    InstallSnapshotResp {
        term: u64,
        last_index: u64,
        next_offset: u64,
    },
    /// A liveness heartbeat from a cluster member (ADR 0012). It carries **no
    /// Raft term** and is *not* consensus traffic: the node driver intercepts it
    /// to feed the failure detector and never hands it to the [`RaftCore`]. It
    /// rides the same `RaftMsg` wire enum (and thus the single per-node inbox) so
    /// a member needs only one message channel to the control group.
    Heartbeat { node: NodeId },
}

impl RaftMsg {
    /// The Raft term carried by this message. A [`Heartbeat`](RaftMsg::Heartbeat)
    /// is not consensus traffic and carries none, so it reports 0 (never forcing a
    /// step-down); the driver intercepts heartbeats before the core sees one.
    fn term(&self) -> u64 {
        match self {
            RaftMsg::RequestVote { term, .. }
            | RaftMsg::RequestVoteResp { term, .. }
            | RaftMsg::AppendEntries { term, .. }
            | RaftMsg::AppendEntriesResp { term, .. }
            | RaftMsg::InstallSnapshot { term, .. }
            | RaftMsg::InstallSnapshotResp { term, .. } => *term,
            RaftMsg::Heartbeat { .. } => 0,
        }
    }
}

/// A node's Raft role.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// Outcome of proposing a command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProposeResult {
    /// Appended to the leader's log at `index` (will replicate + commit).
    Accepted { index: u64 },
    /// This node is not the leader; `leader` is the best-known leader hint.
    NotLeader { leader: Option<NodeId> },
}

/// An outbound message: `(destination, message)`.
pub type Out = (NodeId, RaftMsg);

/// Follower-side reassembly state for an in-progress chunked snapshot transfer.
/// Bytes accumulate in `buf` until `buf.len() == total`, at which point the
/// follower deserializes `Metadata` and installs the snapshot atomically.
struct IncomingSnapshot {
    /// Snapshot index/term this transfer will install at.
    last_index: u64,
    last_term: u64,
    /// Expected full serialized length.
    total: u64,
    /// Contiguously received bytes (only chunks at the current offset extend it,
    /// so a delayed/duplicate chunk can't leave a gap).
    buf: Vec<u8>,
}

/// The Raft state machine for one node.
pub struct RaftCore {
    id: NodeId,
    peers: Vec<NodeId>,
    cluster_size: usize,

    role: Role,
    current_term: u64,
    voted_for: Option<NodeId>,
    // The log holds entries with index > `snapshot_index`; `log[i].index ==
    // snapshot_index + 1 + i`. Entries up to `snapshot_index` are covered by the
    // state-machine snapshot (`metadata` reflects them) and discarded.
    log: Vec<LogEntry>,
    snapshot_index: u64,
    snapshot_term: u64,
    commit_index: u64,
    last_applied: u64,
    leader_id: Option<NodeId>,

    // Candidate state.
    votes: BTreeSet<NodeId>,
    // Leader state.
    next_index: BTreeMap<NodeId, u64>,
    match_index: BTreeMap<NodeId, u64>,
    // Per-follower byte offset reached in the in-flight snapshot transfer, so the
    // leader resumes shipping the next chunk on each heartbeat / ack. Cleared for
    // a peer once it has fully installed the snapshot.
    snapshot_offset: BTreeMap<NodeId, u64>,

    // Follower reassembly buffer for an in-progress chunked `InstallSnapshot`.
    incoming_snapshot: Option<IncomingSnapshot>,

    // Timing (virtual). Election timeout is randomized in `[base, 2*base)`.
    election_base: Duration,
    heartbeat_interval: Duration,
    election_deadline: Nanos,
    heartbeat_deadline: Nanos,

    // Applied state machine and the order commands were applied (for tests /
    // divergence checks).
    metadata: Metadata,
    applied: Vec<MetaCommand>,

    // Durable-state changes awaiting write to the WAL, plus the hard state last
    // marked for persistence (to detect term/vote changes).
    pending: Vec<WalRecord>,
    persisted_hard: (u64, Option<NodeId>),
    // Set when the snapshot base moved (a local snapshot or an installed one),
    // signalling the driver to rewrite the WAL rather than append.
    snapshot_dirty: bool,
}

impl RaftCore {
    /// Create a follower. `all_nodes` is the full membership (including `id`).
    pub fn new(id: NodeId, all_nodes: &[NodeId], now: Nanos, entropy: u64) -> Self {
        let peers: Vec<NodeId> = all_nodes.iter().copied().filter(|n| *n != id).collect();
        let cluster_size = all_nodes.len();
        let mut core = Self {
            id,
            peers,
            cluster_size,
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            snapshot_index: 0,
            snapshot_term: 0,
            commit_index: 0,
            last_applied: 0,
            leader_id: None,
            votes: BTreeSet::new(),
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            snapshot_offset: BTreeMap::new(),
            incoming_snapshot: None,
            election_base: Duration::from_millis(150),
            heartbeat_interval: Duration::from_millis(50),
            election_deadline: Nanos(0),
            heartbeat_deadline: Nanos(0),
            metadata: Metadata::default(),
            applied: Vec::new(),
            pending: Vec::new(),
            persisted_hard: (0, None),
            snapshot_dirty: false,
        };
        core.reset_election_timer(now, entropy);
        core
    }

    /// Recover a node from its durable state, then resume as a follower.
    ///
    /// Term, vote, and the log tail are restored verbatim; the state machine is
    /// restored from the snapshot (its base), and `commit`/`last_applied` start
    /// at the snapshot index. The leader re-advances commit over the recovered
    /// tail, re-applying it — so each committed command is applied exactly once
    /// relative to the snapshot base (no double-applied CAS).
    pub fn recovered(
        id: NodeId,
        all_nodes: &[NodeId],
        persisted: PersistedState,
        now: Nanos,
        entropy: u64,
    ) -> Self {
        let mut core = Self::new(id, all_nodes, now, entropy);
        core.current_term = persisted.term;
        core.voted_for = persisted.voted_for;
        core.log = persisted.log;
        if let Some((metadata, last_index, last_term)) = persisted.snapshot {
            core.metadata = metadata;
            core.snapshot_index = last_index;
            core.snapshot_term = last_term;
            core.last_applied = last_index;
            core.commit_index = last_index;
        }
        // Already durable: do not re-emit it.
        core.persisted_hard = (core.current_term, core.voted_for);
        core.pending.clear();
        core
    }

    /// Take the durable-state changes accumulated since the last drain. The
    /// driver writes and `fsync`s these before sending any outbound message.
    /// Captures any term/vote change first, so a granted vote is durable before
    /// it is sent.
    pub fn drain_persist(&mut self) -> Vec<WalRecord> {
        self.checkpoint_hard();
        std::mem::take(&mut self.pending)
    }

    /// A minimal write-ahead-log image that replays to exactly the current
    /// durable state: the snapshot (if any), the current hard state, and the log
    /// tail. The driver writes this in place of the accumulated history during
    /// compaction, so the WAL is bounded by the *live* state — and once the log
    /// prefix has been truncated by [`snapshot`](Self::snapshot), the image (and
    /// thus the WAL) shrinks accordingly.
    ///
    /// Call only after [`drain_persist`](Self::drain_persist) has been flushed,
    /// so the image and the on-disk WAL agree.
    pub fn wal_image(&self) -> Vec<WalRecord> {
        let mut image = Vec::with_capacity(self.log.len() + 2);
        if self.snapshot_index > 0 {
            image.push(WalRecord::Snapshot {
                metadata: self.metadata.clone(),
                last_index: self.snapshot_index,
                last_term: self.snapshot_term,
            });
        }
        image.push(WalRecord::Hard {
            term: self.current_term,
            voted_for: self.voted_for,
        });
        image.extend(self.log.iter().cloned().map(WalRecord::Append));
        image
    }

    /// Snapshot the applied state and **truncate** the log prefix it covers:
    /// advance the snapshot base to `last_applied` and drop entries through it.
    /// No-op if nothing new has been applied. Sets the snapshot-dirty flag so the
    /// driver rewrites the WAL (the truncation is materialized as a full rewrite,
    /// never incremental records).
    pub fn snapshot(&mut self) {
        if self.last_applied <= self.snapshot_index {
            return;
        }
        let new_index = self.last_applied;
        let new_term = self.term_at(new_index);
        self.log.retain(|e| e.index > new_index);
        self.snapshot_index = new_index;
        self.snapshot_term = new_term;
        self.snapshot_dirty = true;
    }

    /// Take and clear the snapshot-dirty flag (the driver uses this to decide
    /// whether the WAL needs a full rewrite this iteration).
    pub fn take_snapshot_dirty(&mut self) -> bool {
        std::mem::replace(&mut self.snapshot_dirty, false)
    }

    /// The current snapshot base index (0 if no snapshot has been taken).
    pub fn snapshot_index(&self) -> u64 {
        self.snapshot_index
    }

    /// Applied entries not yet covered by the snapshot — the log prefix a
    /// snapshot would truncate. The driver snapshots once this grows large.
    pub fn applied_since_snapshot(&self) -> u64 {
        self.last_applied.saturating_sub(self.snapshot_index)
    }

    /// Number of log entries currently retained (the tail after the snapshot).
    pub fn log_len(&self) -> usize {
        self.log.len()
    }

    // ---- accessors -------------------------------------------------------

    /// The node's current role.
    pub fn role(&self) -> Role {
        self.role
    }

    /// Whether the node currently believes it is leader.
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }

    /// The current term.
    pub fn term(&self) -> u64 {
        self.current_term
    }

    /// Best-known leader id.
    pub fn leader(&self) -> Option<NodeId> {
        self.leader_id
    }

    /// Highest committed log index.
    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    /// Highest applied log index.
    pub fn last_applied(&self) -> u64 {
        self.last_applied
    }

    /// A clone of the applied metadata.
    pub fn metadata(&self) -> Metadata {
        self.metadata.clone()
    }

    /// The sequence of commands applied so far, in order.
    pub fn applied(&self) -> Vec<MetaCommand> {
        self.applied.clone()
    }

    /// The next virtual instant at which this node wants a timer tick.
    pub fn next_deadline(&self) -> Nanos {
        if self.role == Role::Leader {
            self.heartbeat_deadline
        } else {
            self.election_deadline
        }
    }

    // ---- log helpers -----------------------------------------------------

    fn last_log_index(&self) -> u64 {
        self.log.last().map_or(self.snapshot_index, |e| e.index)
    }

    fn last_log_term(&self) -> u64 {
        self.log.last().map_or(self.snapshot_term, |e| e.term)
    }

    /// Term of the entry at `index`. `snapshot_index` resolves to `snapshot_term`;
    /// an index below the snapshot (compacted away) or above the log returns 0
    /// (callers guard those cases).
    fn term_at(&self, index: u64) -> u64 {
        if index == 0 {
            return 0;
        }
        if index == self.snapshot_index {
            return self.snapshot_term;
        }
        if index < self.snapshot_index {
            return 0;
        }
        let offset = (index - self.snapshot_index - 1) as usize;
        self.log.get(offset).map_or(0, |e| e.term)
    }

    fn majority(&self) -> usize {
        self.cluster_size / 2 + 1
    }

    fn reset_election_timer(&mut self, now: Nanos, entropy: u64) {
        let base = self.election_base.as_nanos() as u64;
        let extra = if base == 0 { 0 } else { entropy % base };
        self.election_deadline = Nanos(now.0.saturating_add(base + extra));
    }

    // ---- durable-state helpers ------------------------------------------

    /// Append a log entry and record it for persistence.
    fn log_append(&mut self, entry: LogEntry) {
        self.pending.push(WalRecord::Append(entry.clone()));
        self.log.push(entry);
    }

    /// Truncate the log to `keep` entries and record it for persistence.
    fn log_truncate(&mut self, keep: usize) {
        self.log.truncate(keep);
        self.pending.push(WalRecord::Truncate { keep });
    }

    /// Emit a hard-state record if the term or vote changed since last persisted.
    /// Called at the end of every public entry point.
    fn checkpoint_hard(&mut self) {
        let hard = (self.current_term, self.voted_for);
        if hard != self.persisted_hard {
            self.persisted_hard = hard;
            self.pending.push(WalRecord::Hard {
                term: hard.0,
                voted_for: hard.1,
            });
        }
    }

    // ---- driving entry points -------------------------------------------

    /// Handle a timer tick at `now`. May start an election or send heartbeats.
    pub fn tick(&mut self, now: Nanos, entropy: u64) -> Vec<Out> {
        match self.role {
            Role::Leader => {
                if now.0 >= self.heartbeat_deadline.0 {
                    self.heartbeat_deadline = Nanos(now.0.saturating_add(self.heartbeat_nanos()));
                    return self.broadcast_append();
                }
                Vec::new()
            }
            Role::Follower | Role::Candidate => {
                if now.0 >= self.election_deadline.0 {
                    return self.start_election(now, entropy);
                }
                Vec::new()
            }
        }
    }

    /// Handle an inbound message from `from` at `now`.
    pub fn handle(&mut self, from: NodeId, msg: RaftMsg, now: Nanos, entropy: u64) -> Vec<Out> {
        // Any message from a higher term forces us to step down first.
        if msg.term() > self.current_term {
            self.current_term = msg.term();
            self.voted_for = None;
            self.role = Role::Follower;
            self.leader_id = None;
        }
        match msg {
            RaftMsg::RequestVote {
                term,
                candidate,
                last_log_index,
                last_log_term,
            } => self.handle_request_vote(
                candidate,
                term,
                last_log_index,
                last_log_term,
                now,
                entropy,
            ),
            RaftMsg::RequestVoteResp { term, granted } => {
                self.handle_vote_resp(from, term, granted, now)
            }
            RaftMsg::AppendEntries {
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => self.handle_append_entries(
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                now,
                entropy,
            ),
            RaftMsg::AppendEntriesResp {
                term,
                success,
                match_index,
            } => self.handle_append_resp(from, term, success, match_index),
            RaftMsg::InstallSnapshot {
                term,
                leader,
                last_index,
                last_term,
                offset,
                data,
                total,
                done,
            } => self.handle_install_snapshot(
                term, leader, last_index, last_term, offset, data, total, done, now, entropy,
            ),
            RaftMsg::InstallSnapshotResp {
                term,
                last_index,
                next_offset,
            } => self.handle_install_snapshot_resp(from, term, last_index, next_offset),
            // Heartbeats are intercepted by the driver and fed to the failure
            // detector (ADR 0012); they are not consensus traffic, so the core
            // ignores any that reach it.
            RaftMsg::Heartbeat { .. } => Vec::new(),
        }
    }

    /// Propose a command. If leader, append it (replicated on the next
    /// heartbeat); otherwise report the leader hint.
    pub fn propose(&mut self, command: MetaCommand) -> ProposeResult {
        if self.role != Role::Leader {
            return ProposeResult::NotLeader {
                leader: self.leader_id,
            };
        }
        let index = self.last_log_index() + 1;
        self.log_append(LogEntry {
            term: self.current_term,
            index,
            command,
        });
        // Lets a single-node group make progress; safe for larger groups
        // because commit still requires a majority of matchIndex.
        self.maybe_advance_commit();
        self.apply();
        ProposeResult::Accepted { index }
    }

    // ---- message handlers ------------------------------------------------

    fn handle_request_vote(
        &mut self,
        candidate: NodeId,
        term: u64,
        last_log_index: u64,
        last_log_term: u64,
        now: Nanos,
        entropy: u64,
    ) -> Vec<Out> {
        let granted = if term < self.current_term {
            false
        } else {
            let log_ok = last_log_term > self.last_log_term()
                || (last_log_term == self.last_log_term()
                    && last_log_index >= self.last_log_index());
            let can_vote = self.voted_for.is_none() || self.voted_for == Some(candidate);
            if can_vote && log_ok {
                self.voted_for = Some(candidate);
                self.reset_election_timer(now, entropy);
                true
            } else {
                false
            }
        };
        vec![(
            candidate,
            RaftMsg::RequestVoteResp {
                term: self.current_term,
                granted,
            },
        )]
    }

    fn handle_vote_resp(&mut self, from: NodeId, term: u64, granted: bool, now: Nanos) -> Vec<Out> {
        if self.role != Role::Candidate || term != self.current_term {
            return Vec::new();
        }
        if granted {
            self.votes.insert(from);
            if self.votes.len() >= self.majority() {
                return self.become_leader(now);
            }
        }
        Vec::new()
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_append_entries(
        &mut self,
        term: u64,
        leader: NodeId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
        now: Nanos,
        entropy: u64,
    ) -> Vec<Out> {
        if term < self.current_term {
            return vec![(
                leader,
                RaftMsg::AppendEntriesResp {
                    term: self.current_term,
                    success: false,
                    match_index: 0,
                },
            )];
        }
        // Valid leader for our term: become/stay follower and defer the timeout.
        self.role = Role::Follower;
        self.leader_id = Some(leader);
        self.reset_election_timer(now, entropy);

        // The leader's prev is behind our snapshot: those entries are already in
        // our snapshot, so report we match up to the snapshot and let the leader
        // resend from there. (Common right after we compacted past the leader.)
        if prev_log_index < self.snapshot_index {
            return vec![(
                leader,
                RaftMsg::AppendEntriesResp {
                    term: self.current_term,
                    success: true,
                    match_index: self.snapshot_index,
                },
            )];
        }

        // Consistency check at prev_log_index (>= snapshot_index now).
        if prev_log_index > 0
            && (self.last_log_index() < prev_log_index
                || self.term_at(prev_log_index) != prev_log_term)
        {
            return vec![(
                leader,
                RaftMsg::AppendEntriesResp {
                    term: self.current_term,
                    success: false,
                    match_index: 0,
                },
            )];
        }

        // Append, truncating on the first conflicting entry.
        let mut idx = prev_log_index;
        for entry in entries {
            idx += 1;
            if self.last_log_index() >= idx {
                if self.term_at(idx) != entry.term {
                    let keep = (idx - self.snapshot_index - 1) as usize;
                    self.log_truncate(keep);
                    self.log_append(entry);
                }
                // else: already present and matching; skip.
            } else {
                self.log_append(entry);
            }
        }
        let match_index = idx;

        if leader_commit > self.commit_index {
            self.commit_index = leader_commit.min(self.last_log_index());
            self.apply();
        }

        vec![(
            leader,
            RaftMsg::AppendEntriesResp {
                term: self.current_term,
                success: true,
                match_index,
            },
        )]
    }

    fn handle_append_resp(
        &mut self,
        from: NodeId,
        term: u64,
        success: bool,
        match_index: u64,
    ) -> Vec<Out> {
        if self.role != Role::Leader || term != self.current_term {
            return Vec::new();
        }
        if success {
            let m = self.match_index.entry(from).or_insert(0);
            *m = (*m).max(match_index);
            self.next_index.insert(from, match_index + 1);
            self.maybe_advance_commit();
            self.apply();
            if self.next_index.get(&from).copied().unwrap_or(1) <= self.last_log_index() {
                return vec![self.replicate_to(from)];
            }
            Vec::new()
        } else {
            let ni = self.next_index.entry(from).or_insert(1);
            if *ni > 1 {
                *ni -= 1;
            }
            vec![self.replicate_to(from)]
        }
    }

    /// Receive one chunk of a chunked snapshot transfer. Bytes are reassembled by
    /// `offset` into a follower-side buffer; the snapshot is installed atomically
    /// only when the final (`done`) chunk completes a contiguous buffer of length
    /// `total`. The ack reports `next_offset` (contiguous bytes held), which the
    /// leader uses to ship the next chunk; `last_index` is non-zero only once
    /// fully installed.
    #[allow(clippy::too_many_arguments)]
    fn handle_install_snapshot(
        &mut self,
        term: u64,
        leader: NodeId,
        last_index: u64,
        last_term: u64,
        offset: u64,
        data: Vec<u8>,
        total: u64,
        done: bool,
        now: Nanos,
        entropy: u64,
    ) -> Vec<Out> {
        if term < self.current_term {
            return vec![(
                leader,
                RaftMsg::InstallSnapshotResp {
                    term: self.current_term,
                    last_index: 0,
                    next_offset: 0,
                },
            )];
        }
        self.role = Role::Follower;
        self.leader_id = Some(leader);
        self.reset_election_timer(now, entropy);

        // Already at least this far along: drop any partial transfer and just
        // acknowledge our position (the leader will stop sending chunks).
        if last_index <= self.snapshot_index {
            self.incoming_snapshot = None;
            return vec![(
                leader,
                RaftMsg::InstallSnapshotResp {
                    term: self.current_term,
                    last_index: self.snapshot_index,
                    next_offset: total,
                },
            )];
        }

        // Start (or restart) reassembly when this is the first chunk, or when the
        // in-flight transfer is for a different/older snapshot.
        let fresh = match &self.incoming_snapshot {
            Some(inc) => inc.last_index != last_index || inc.total != total,
            None => true,
        };
        if fresh && offset == 0 {
            self.incoming_snapshot = Some(IncomingSnapshot {
                last_index,
                last_term,
                total,
                buf: Vec::new(),
            });
        }

        // Append only a chunk that lands exactly at our current end, keeping the
        // buffer contiguous (a reordered/duplicate chunk is ignored and re-driven
        // by the next ack's `next_offset`).
        if let Some(inc) = &mut self.incoming_snapshot {
            if inc.last_index == last_index && offset == inc.buf.len() as u64 {
                inc.buf.extend_from_slice(&data);
            }
        }

        // Complete? Install atomically.
        let next_offset = self
            .incoming_snapshot
            .as_ref()
            .map_or(0, |inc| inc.buf.len() as u64);
        let complete = done
            && self
                .incoming_snapshot
                .as_ref()
                .is_some_and(|inc| inc.buf.len() as u64 == inc.total);
        if complete {
            let inc = self
                .incoming_snapshot
                .take()
                .expect("present when complete");
            // A malformed snapshot would be a leader bug; drop the transfer and
            // re-request on the next chunk rather than installing garbage.
            match serde_json::from_slice::<Metadata>(&inc.buf) {
                Ok(metadata) => {
                    self.metadata = metadata;
                    self.snapshot_index = inc.last_index;
                    self.snapshot_term = inc.last_term;
                    self.last_applied = inc.last_index;
                    self.commit_index = inc.last_index;
                    self.log.clear();
                    self.applied.clear();
                    self.snapshot_dirty = true;
                    return vec![(
                        leader,
                        RaftMsg::InstallSnapshotResp {
                            term: self.current_term,
                            last_index: inc.last_index,
                            next_offset: inc.total,
                        },
                    )];
                }
                Err(_) => {
                    return vec![(
                        leader,
                        RaftMsg::InstallSnapshotResp {
                            term: self.current_term,
                            last_index: 0,
                            next_offset: 0,
                        },
                    )];
                }
            }
        }

        // Still in progress: ack how far we've got so the leader sends the next
        // chunk.
        vec![(
            leader,
            RaftMsg::InstallSnapshotResp {
                term: self.current_term,
                last_index: 0,
                next_offset,
            },
        )]
    }

    fn handle_install_snapshot_resp(
        &mut self,
        from: NodeId,
        term: u64,
        last_index: u64,
        next_offset: u64,
    ) -> Vec<Out> {
        if self.role != Role::Leader || term != self.current_term {
            return Vec::new();
        }
        if last_index > 0 {
            // Transfer complete: the follower installed the snapshot.
            self.snapshot_offset.remove(&from);
            let m = self.match_index.entry(from).or_insert(0);
            *m = (*m).max(last_index);
            self.next_index.insert(from, last_index + 1);
            self.maybe_advance_commit();
            self.apply();
            if self.next_index.get(&from).copied().unwrap_or(1) <= self.last_log_index() {
                return vec![self.replicate_to(from)];
            }
            return Vec::new();
        }
        // Still mid-transfer: record progress and ship the next chunk.
        self.snapshot_offset.insert(from, next_offset);
        vec![self.replicate_to(from)]
    }

    // ---- role transitions & replication ---------------------------------

    fn start_election(&mut self, now: Nanos, entropy: u64) -> Vec<Out> {
        self.role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.leader_id = None;
        self.votes.clear();
        self.votes.insert(self.id);
        self.reset_election_timer(now, entropy);

        if self.votes.len() >= self.majority() {
            return self.become_leader(now);
        }
        let (lli, llt) = (self.last_log_index(), self.last_log_term());
        self.peers
            .iter()
            .map(|&p| {
                (
                    p,
                    RaftMsg::RequestVote {
                        term: self.current_term,
                        candidate: self.id,
                        last_log_index: lli,
                        last_log_term: llt,
                    },
                )
            })
            .collect()
    }

    fn become_leader(&mut self, now: Nanos) -> Vec<Out> {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        let last = self.last_log_index();
        for &p in &self.peers {
            self.next_index.insert(p, last + 1);
            self.match_index.insert(p, 0);
        }
        // A fresh term restarts any snapshot transfer from offset 0.
        self.snapshot_offset.clear();
        // No-op entry so prior-term entries can be committed under our term.
        self.log_append(LogEntry {
            term: self.current_term,
            index: last + 1,
            command: MetaCommand::NoOp,
        });
        self.heartbeat_deadline = Nanos(now.0.saturating_add(self.heartbeat_nanos()));
        self.broadcast_append()
    }

    fn broadcast_append(&self) -> Vec<Out> {
        self.peers.iter().map(|&p| self.replicate_to(p)).collect()
    }

    /// Build the right replication message for `peer`: an `InstallSnapshot` if
    /// the entries it needs have been compacted away, otherwise `AppendEntries`.
    fn replicate_to(&self, peer: NodeId) -> Out {
        let next = self.next_index.get(&peer).copied().unwrap_or(1).max(1);
        // The entry before `next` is in our snapshot (or earlier) — we can't form
        // a valid `prev_log_term`, so ship the snapshot instead, as the next
        // offset-addressed chunk for this peer.
        if next <= self.snapshot_index {
            return self.snapshot_chunk_for(peer);
        }
        let prev_log_index = next - 1;
        let prev_log_term = self.term_at(prev_log_index);
        let entries: Vec<LogEntry> = self
            .log
            .iter()
            .filter(|e| e.index >= next)
            .cloned()
            .collect();
        (
            peer,
            RaftMsg::AppendEntries {
                term: self.current_term,
                leader: self.id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            },
        )
    }

    /// Build the next `InstallSnapshot` chunk for `peer`, starting at the byte
    /// offset recorded in `snapshot_offset` (0 if no transfer is in flight).
    /// Pure: it serializes the current `metadata` and slices out one chunk, so
    /// repeated calls at the same offset are byte-identical and deterministic.
    fn snapshot_chunk_for(&self, peer: NodeId) -> Out {
        let serialized = serde_json::to_vec(&self.metadata).expect("metadata serializes");
        let total = serialized.len() as u64;
        let offset = self
            .snapshot_offset
            .get(&peer)
            .copied()
            .unwrap_or(0)
            .min(total);
        let start = offset as usize;
        let end = (start + SNAPSHOT_CHUNK_BYTES).min(serialized.len());
        let data = serialized[start..end].to_vec();
        let done = end as u64 == total;
        (
            peer,
            RaftMsg::InstallSnapshot {
                term: self.current_term,
                leader: self.id,
                last_index: self.snapshot_index,
                last_term: self.snapshot_term,
                offset,
                data,
                total,
                done,
            },
        )
    }

    fn maybe_advance_commit(&mut self) {
        // Find the highest N > commit_index replicated on a majority whose entry
        // is from the current term (the Raft commit safety rule).
        let last = self.last_log_index();
        let mut n = last;
        while n > self.commit_index {
            if self.term_at(n) == self.current_term {
                let replicas = 1 + self
                    .peers
                    .iter()
                    .filter(|p| self.match_index.get(p).copied().unwrap_or(0) >= n)
                    .count();
                if replicas >= self.majority() {
                    self.commit_index = n;
                    break;
                }
            }
            n -= 1;
        }
    }

    fn apply(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let offset = (self.last_applied - self.snapshot_index - 1) as usize;
            let command = self.log[offset].command.clone();
            self.metadata.apply(&command);
            self.applied.push(command);
        }
        // No per-apply checkpoint: durability comes from the snapshot taken by
        // `snapshot()` plus the persisted log tail; recovery re-applies the tail.
    }

    fn heartbeat_nanos(&self) -> u64 {
        self.heartbeat_interval.as_nanos() as u64
    }
}
