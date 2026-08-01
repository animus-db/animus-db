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
//! current-term entries via majority `matchIndex`. Durability is handled
//! out-of-band: the core emits [`WalRecord`]s (see [`drain_persist`]) that the
//! driver persists; recovery is via [`recovered`]. Not yet implemented: WAL
//! compaction (ADR 0009).
//!
//! [`drain_persist`]: RaftCore::drain_persist
//! [`recovered`]: RaftCore::recovered

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use custos_env::{Nanos, NodeId};
use serde::{Deserialize, Serialize};

use crate::meta::{MetaCommand, Metadata};
use crate::persist::{PersistedState, WalRecord};

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
}

impl RaftMsg {
    fn term(&self) -> u64 {
        match self {
            RaftMsg::RequestVote { term, .. }
            | RaftMsg::RequestVoteResp { term, .. }
            | RaftMsg::AppendEntries { term, .. }
            | RaftMsg::AppendEntriesResp { term, .. } => *term,
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

/// The Raft state machine for one node.
pub struct RaftCore {
    id: NodeId,
    peers: Vec<NodeId>,
    cluster_size: usize,

    role: Role,
    current_term: u64,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry>,
    commit_index: u64,
    last_applied: u64,
    leader_id: Option<NodeId>,

    // Candidate state.
    votes: BTreeSet<NodeId>,
    // Leader state.
    next_index: BTreeMap<NodeId, u64>,
    match_index: BTreeMap<NodeId, u64>,

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
            commit_index: 0,
            last_applied: 0,
            leader_id: None,
            votes: BTreeSet::new(),
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            election_base: Duration::from_millis(150),
            heartbeat_interval: Duration::from_millis(50),
            election_deadline: Nanos(0),
            heartbeat_deadline: Nanos(0),
            metadata: Metadata::default(),
            applied: Vec::new(),
            pending: Vec::new(),
            persisted_hard: (0, None),
        };
        core.reset_election_timer(now, entropy);
        core
    }

    /// Recover a node from its durable state, then resume as a follower.
    ///
    /// The log, term, and vote are restored verbatim; the state machine is
    /// restored from the latest checkpoint (so committed, non-idempotent
    /// commands are not re-applied). `commit_index` starts at the checkpoint and
    /// is re-advanced by the leader.
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
        if let Some((metadata, last_applied)) = persisted.snapshot {
            core.metadata = metadata;
            core.last_applied = last_applied;
            core.commit_index = last_applied;
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
        self.log.last().map_or(0, |e| e.index)
    }

    fn last_log_term(&self) -> u64 {
        self.log.last().map_or(0, |e| e.term)
    }

    fn term_at(&self, index: u64) -> u64 {
        if index == 0 {
            return 0;
        }
        self.log.get((index - 1) as usize).map_or(0, |e| e.term)
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

        // Consistency check at prev_log_index.
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
                    self.log_truncate((idx - 1) as usize);
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
                return vec![self.append_to(from)];
            }
            Vec::new()
        } else {
            let ni = self.next_index.entry(from).or_insert(1);
            if *ni > 1 {
                *ni -= 1;
            }
            vec![self.append_to(from)]
        }
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
        self.peers.iter().map(|&p| self.append_to(p)).collect()
    }

    fn append_to(&self, peer: NodeId) -> Out {
        let next = self.next_index.get(&peer).copied().unwrap_or(1).max(1);
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
        let start = self.last_applied;
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let command = self.log[(self.last_applied - 1) as usize].command.clone();
            self.metadata.apply(&command);
            self.applied.push(command);
        }
        if self.last_applied > start {
            // Checkpoint the applied state so recovery need not re-apply
            // (which would double-apply non-idempotent commands like CAS).
            self.pending.push(WalRecord::Snapshot {
                metadata: self.metadata.clone(),
                last_applied: self.last_applied,
            });
        }
    }

    fn heartbeat_nanos(&self) -> u64 {
        self.heartbeat_interval.as_nanos() as u64
    }
}
