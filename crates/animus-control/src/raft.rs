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
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::meta::{MetaCommand, Metadata};
use crate::persist::{PersistedState, WalRecord};

/// The replicated state machine a [`RaftCore`] drives. The control plane uses
/// [`Metadata`] (command = [`MetaCommand`]); a future per-tablet data plane will
/// supply a key-value store (ADR 0016). The core agrees the *order* of `C`-typed
/// commands and applies them here; the `S` type is also the snapshot image
/// (serialized for `InstallSnapshot` and the WAL snapshot record), hence the
/// `Serialize`/`DeserializeOwned` bounds.
///
/// `apply` is deliberately infallible from the core's view — any
/// accept/reject/no-op decision is the state machine's own business and does not
/// change the replicated log (the control plane's richer `Metadata::apply`
/// outcome is simply discarded by the trait impl).
pub trait StateMachine<C>: Default + Clone + Serialize + DeserializeOwned {
    /// When `false` (the default — the in-memory control plane), the core applies
    /// each committed-and-durable command **in-core, synchronously** via
    /// [`apply`](StateMachine::apply). When `true`, the core does **not** apply
    /// in-core; it buffers committed-durable commands as effects for an **async
    /// driver** to apply to a real `StorageEngine` (drained via
    /// [`RaftCore::drain_apply`]) — the sync-core / async-driver split the
    /// `animus-consensus` `AccordCore` uses, required because a `StorageEngine`
    /// apply is async I/O and the core is synchronous (ADR 0017). With this set,
    /// the in-core `apply` is never called (a unit placeholder `S` suffices).
    const DRIVER_APPLIED: bool = false;
    /// Apply one agreed command to the state machine, in commit order. Only called
    /// when [`DRIVER_APPLIED`](StateMachine::DRIVER_APPLIED) is `false`.
    fn apply(&mut self, command: &C);
    /// The no-op command a freshly elected leader appends under its own term so
    /// that prior-term entries can be committed (Raft's no-op-on-election).
    fn noop() -> C;
}

/// Maximum bytes of serialized snapshot carried by a single `InstallSnapshot`
/// message. A snapshot larger than this is shipped over several offset-addressed
/// chunks and reassembled by the follower (ADR 0009). Small enough that a
/// realistic metadata snapshot spans multiple chunks; the value only affects
/// message granularity, never correctness.
pub const SNAPSHOT_CHUNK_BYTES: usize = 1024;

/// A replicated log entry, generic over the command type `C` (defaults to the
/// control plane's [`MetaCommand`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry<C = MetaCommand> {
    /// Leader term in which the entry was created.
    pub term: u64,
    /// 1-based position in the log.
    pub index: u64,
    /// The state-machine command.
    pub command: C,
    /// For a **membership-change** entry (ADR 0017 C), the new voter set this
    /// entry installs; `None` for an ordinary command entry. Membership lives in
    /// the log so every replica agrees on the configuration history; a node uses
    /// the latest log config (committed or not) for all quorum/election decisions.
    /// `#[serde(default)]` so ordinary entries (and the control plane) are
    /// unchanged on the wire.
    #[serde(default)]
    pub config: Option<BTreeSet<NodeId>>,
}

/// Wire messages exchanged between Raft peers, generic over the command type `C`
/// (defaults to [`MetaCommand`]). Only [`AppendEntries`](RaftMsg::AppendEntries)
/// carries commands; `InstallSnapshot` ships the snapshot as opaque bytes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RaftMsg<C = MetaCommand> {
    /// A **pre-candidate** solicits a *pre-vote* (the standard Raft pre-vote
    /// extension). `term` is the candidate's **prospective** term (its
    /// `current_term + 1`); crucially, neither sending nor receiving a pre-vote
    /// changes any node's term, so a partitioned/stalled node running pre-vote
    /// rounds can never inflate the cluster's term or disrupt a healthy leader. A
    /// peer grants only if it would actually vote (no live leader within its
    /// election timeout and the candidate's log is at least as up to date). Rides
    /// the same `RaftMsg` wire enum additively, so both planes keep working.
    PreVote {
        term: u64,
        candidate: NodeId,
        last_log_index: u64,
        last_log_term: u64,
    },
    /// Response to [`RaftMsg::PreVote`]. On a grant, `term` echoes the requested
    /// prospective term; on a reject it is the responder's own (real) term, so a
    /// stale pre-candidate learns it is behind. Never advances the recipient's term
    /// beyond a *rejecting* responder's real term.
    PreVoteResp { term: u64, granted: bool },
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
        entries: Vec<LogEntry<C>>,
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
        /// The voter configuration at the snapshot (ADR 0017 C): the image bytes
        /// don't carry Raft membership, so the leader ships it here and the
        /// follower adopts it on install. `None` (default) ⇒ the initial config.
        #[serde(default)]
        config: Option<BTreeSet<NodeId>>,
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

impl<C> RaftMsg<C> {
    /// The Raft term carried by this message. A [`Heartbeat`](RaftMsg::Heartbeat)
    /// is not consensus traffic and carries none, so it reports 0 (never forcing a
    /// step-down); the driver intercepts heartbeats before the core sees one.
    fn term(&self) -> u64 {
        match self {
            RaftMsg::PreVote { term, .. }
            | RaftMsg::PreVoteResp { term, .. }
            | RaftMsg::RequestVote { term, .. }
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
    /// Running a **pre-vote** round (the standard Raft extension): the node has
    /// timed out on its leader but has **not** incremented its term. It solicits
    /// [`PreVote`](RaftMsg::PreVote)s and only advances to [`Candidate`](Role::Candidate)
    /// — bumping the term — once a majority would actually vote for it. This keeps
    /// a briefly-partitioned/stalled node from repeatedly bumping the cluster's
    /// term and disrupting a healthy leader.
    PreCandidate,
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

/// An outbound message: `(destination, message)`. Generic over the command type
/// `C` (defaults to [`MetaCommand`]).
pub type Out<C = MetaCommand> = (NodeId, RaftMsg<C>);

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

/// The Raft state machine for one node, generic over the command type `C` and the
/// applied state-machine `S` (defaults: the control plane's [`MetaCommand`] /
/// [`Metadata`]). The consensus logic is identical for any `S: StateMachine<C>`;
/// a per-tablet data plane (ADR 0016) instantiates it with a key-value store.
pub struct RaftCore<C = MetaCommand, S = Metadata> {
    id: NodeId,
    // `peers` + `cluster_size` are **derived from `config`** (the active voter set)
    // and kept in sync by `apply_config`, so existing quorum/replication call
    // sites are unchanged as membership evolves (ADR 0017 C).
    peers: Vec<NodeId>,
    cluster_size: usize,
    // The active Raft voter configuration: the voter set from the latest log entry
    // carrying a config (committed or not), else the snapshot's config, else
    // `initial_config`. Single-server changes (Raft §4.3) — never two disjoint
    // majorities — so a leader appends one `AddServer`/`RemoveServer` config entry
    // and adopts it immediately.
    config: BTreeSet<NodeId>,
    // The configuration the node booted with (the fallback when no config entry or
    // snapshot config is present). Never the control plane's concern — it never
    // reconfigures, so `config == initial_config` always there.
    initial_config: BTreeSet<NodeId>,
    // The voter config recorded by the latest local snapshot (so compaction does
    // not lose membership); restored on recovery.
    snapshot_config: Option<BTreeSet<NodeId>>,

    role: Role,
    current_term: u64,
    voted_for: Option<NodeId>,
    // The log holds entries with index > `snapshot_index`; `log[i].index ==
    // snapshot_index + 1 + i`. Entries up to `snapshot_index` are covered by the
    // state-machine snapshot (`metadata` reflects them) and discarded.
    log: Vec<LogEntry<C>>,
    snapshot_index: u64,
    snapshot_term: u64,
    commit_index: u64,
    last_applied: u64,
    // Highest log index whose WAL record is durably fsynced. The driver advances
    // it (via `mark_durable_through`) after `env.sync(WAL)`; `apply` never advances
    // `last_applied` past it. This is the **durable-before-visible** invariant: a
    // command becomes client-visible (via `metadata`/`applied`, what a proposer
    // waits on) only once it is on disk, so a crash in the commit→fsync window
    // cannot lose an entry a client already observed (ADR 0009).
    durable_index: u64,
    leader_id: Option<NodeId>,

    // Pre-candidate state: nodes that have granted the current pre-vote round.
    // Rebuilt each `start_pre_vote`; only read while `role == PreCandidate`.
    pre_votes: BTreeSet<NodeId>,
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
    // `election_base` is configurable via [`set_election_timeout`](RaftCore::set_election_timeout)
    // so the assembly layer can widen it for a node doing real disk I/O (whose
    // driver may briefly stall past the default 150ms); defaults to 150ms.
    election_base: Duration,
    heartbeat_interval: Duration,
    election_deadline: Nanos,
    heartbeat_deadline: Nanos,

    // Applied state machine and the order commands were applied (for tests /
    // divergence checks). For a `DRIVER_APPLIED` state machine `metadata` is an
    // unused unit placeholder and `applied` stays empty — committed commands ride
    // `pending_apply` to the driver instead.
    metadata: S,
    applied: Vec<C>,
    // Committed-and-durable commands a `DRIVER_APPLIED` state machine has not yet
    // handed to its async driver, as `(index, command)` in commit order. Always
    // empty for the in-core control plane. Drained by [`RaftCore::drain_apply`].
    pending_apply: Vec<(u64, C)>,

    // Durable-state changes awaiting write to the WAL, plus the hard state last
    // marked for persistence (to detect term/vote changes).
    pending: Vec<WalRecord<C, S>>,
    persisted_hard: (u64, Option<NodeId>),
    // Set when the snapshot base moved (a local snapshot or an installed one),
    // signalling the driver to rewrite the WAL rather than append.
    snapshot_dirty: bool,

    // --- DRIVER_APPLIED snapshot streaming (ADR 0017 A.2). Unused by the in-core
    // control plane (whose `InstallSnapshot` serializes `metadata` directly). ---
    // The leader's current engine-image bytes to ship to a lagging follower; the
    // driver refreshes this from the engine when it compacts (`set_snapshot_blob`),
    // and the core also sets it on *install* completion (a follower keeps the image
    // it just received so it can re-ship it later — see `handle_install_snapshot`)
    // so it is `Some` whenever `snapshot_index > 0`, never an empty 0-byte ship.
    snapshot_blob: Option<Vec<u8>>,
    // A fully-received snapshot's `(last_index, bytes)` awaiting the driver writing
    // it into the engine (`drain_pending_install`); set on install completion.
    pending_install: Option<(u64, Vec<u8>)>,
}

impl<C, S> RaftCore<C, S>
where
    C: Clone + std::fmt::Debug + Serialize + DeserializeOwned,
    S: StateMachine<C>,
{
    /// Create a follower. `all_nodes` is the full membership (including `id`).
    pub fn new(id: NodeId, all_nodes: &[NodeId], now: Nanos, entropy: u64) -> Self {
        let peers: Vec<NodeId> = all_nodes.iter().copied().filter(|n| *n != id).collect();
        let cluster_size = all_nodes.len();
        let initial_config: BTreeSet<NodeId> = all_nodes.iter().copied().collect();
        let mut core = Self {
            id,
            peers,
            cluster_size,
            config: initial_config.clone(),
            initial_config,
            snapshot_config: None,
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            snapshot_index: 0,
            snapshot_term: 0,
            commit_index: 0,
            last_applied: 0,
            durable_index: 0,
            leader_id: None,
            pre_votes: BTreeSet::new(),
            votes: BTreeSet::new(),
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            snapshot_offset: BTreeMap::new(),
            incoming_snapshot: None,
            election_base: Duration::from_millis(150),
            heartbeat_interval: Duration::from_millis(50),
            election_deadline: Nanos(0),
            heartbeat_deadline: Nanos(0),
            metadata: S::default(),
            applied: Vec::new(),
            pending_apply: Vec::new(),
            snapshot_blob: None,
            pending_install: None,
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
        persisted: PersistedState<C, S>,
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
            // Preserve the invariant `snapshot_index > 0 ⟹ snapshot_blob.is_some()`
            // through recovery: [`snapshot_chunk_for`] slices the cached blob, and a
            // recovered leader may have to ship this snapshot to a lagging follower
            // before it ever re-compacts. The recovered `metadata` *is* the in-core
            // image, so serialize it once here — identical to what the old
            // re-serialize-per-chunk path produced, just cached. (A `DRIVER_APPLIED`
            // core's image lives in the engine, not `metadata`; its driver repopulates
            // `snapshot_blob` from the engine, so leave it None here.)
            if !S::DRIVER_APPLIED {
                core.snapshot_blob =
                    Some(serde_json::to_vec(&core.metadata).expect("metadata serializes"));
            }
        }
        // Restore the voter configuration: the snapshot's recorded config (if any)
        // is the base, and the recovered log tail's latest config entry (if any)
        // overrides it — `recompute_config` applies that precedence (ADR 0017 C).
        core.snapshot_config = persisted.snapshot_config;
        core.recompute_config();
        // Everything restored from the WAL/snapshot is by definition durable, so
        // the durable watermark covers the whole recovered log. The tail re-applies
        // (durable-gated, a no-op gate) once commit re-advances post-recovery.
        core.durable_index = core.last_log_index();
        // Already durable: do not re-emit it.
        core.persisted_hard = (core.current_term, core.voted_for);
        core.pending.clear();
        core
    }

    /// Take the durable-state changes accumulated since the last drain. The
    /// driver writes and `fsync`s these before sending any outbound message.
    /// Captures any term/vote change first, so a granted vote is durable before
    /// it is sent.
    pub fn drain_persist(&mut self) -> Vec<WalRecord<C, S>> {
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
    pub fn wal_image(&self) -> Vec<WalRecord<C, S>> {
        let mut image = Vec::with_capacity(self.log.len() + 2);
        if self.snapshot_index > 0 {
            image.push(WalRecord::Snapshot {
                metadata: self.metadata.clone(),
                last_index: self.snapshot_index,
                last_term: self.snapshot_term,
                config: self.snapshot_config.clone(),
            });
        }
        image.push(WalRecord::Hard {
            term: self.current_term,
            voted_for: self.voted_for,
        });
        image.extend(self.log.iter().cloned().map(WalRecord::Append));
        image
    }

    /// The compact WAL image **already encoded to bytes**, identical to encoding
    /// [`wal_image`](Self::wal_image) record-by-record — but the (dominant) snapshot
    /// record reuses the cached `snapshot_blob` (via
    /// [`PersistedState::encode_snapshot_record_from_blob`]) so the state is
    /// serialized **once** per compaction, not twice (once into the blob for
    /// `InstallSnapshot`, once for the WAL). The control-plane driver's `compact_wal`
    /// calls this; the byte-equality with `wal_image` encoding is guarded by
    /// `wal_compaction.rs::encoded_image_matches_wal_image_encoding`.
    ///
    /// **In-core only:** for an in-core state machine `snapshot_blob ==
    /// serialize(metadata@snapshot_index)`, so reusing it for the WAL `metadata`
    /// field is exact. A `DRIVER_APPLIED` plane's blob is its *engine* image (not
    /// `serialize(state)`), and it has its own driver/compaction, so it must not use
    /// this — asserted below.
    #[must_use]
    pub fn encoded_wal_image(&self) -> Vec<u8> {
        assert!(
            !S::DRIVER_APPLIED,
            "encoded_wal_image reuses snapshot_blob as the serialized state image, \
             which only holds for an in-core state machine"
        );
        let mut bytes = Vec::new();
        if self.snapshot_index > 0 {
            let blob = self
                .snapshot_blob
                .as_deref()
                .expect("snapshot_blob is Some when snapshot_index > 0 (in-core invariant)");
            bytes.extend(PersistedState::<C, S>::encode_snapshot_record_from_blob(
                blob,
                self.snapshot_index,
                self.snapshot_term,
                &self.snapshot_config,
            ));
        }
        bytes.extend(PersistedState::<C, S>::encode_record(&WalRecord::Hard {
            term: self.current_term,
            voted_for: self.voted_for,
        }));
        for entry in &self.log {
            bytes.extend(PersistedState::<C, S>::encode_record(&WalRecord::Append(
                entry.clone(),
            )));
        }
        bytes
    }

    /// Snapshot the applied state and **truncate** the log prefix it covers:
    /// advance the snapshot base to `last_applied` and drop entries through it.
    /// No-op if nothing new has been applied. Sets the snapshot-dirty flag so the
    /// driver rewrites the WAL (the truncation is materialized as a full rewrite,
    /// never incremental records).
    pub fn snapshot(&mut self) {
        self.snapshot_upto(self.last_applied);
    }

    /// Snapshot only up to `index` (clamped to `last_applied`), rather than all the
    /// way to `last_applied`. A `DRIVER_APPLIED` data plane whose async apply task
    /// lags the core's `last_applied` must snapshot only to the index its engine has
    /// actually merged (`snapshot_blob` is captured from that engine), so the shipped
    /// image and the truncated log prefix agree — snapshotting to `last_applied`
    /// would truncate entries the engine image does not yet contain. The in-core
    /// control plane applies synchronously, so it uses `snapshot()` (index ==
    /// `last_applied`). No-op if nothing new is covered.
    pub fn snapshot_upto(&mut self, index: u64) {
        let new_index = index.min(self.last_applied);
        if new_index <= self.snapshot_index {
            return;
        }
        let new_term = self.term_at(new_index);
        // Capture the config effective at the snapshot base *before* truncating
        // (the config entry may be in the prefix we are about to drop).
        self.snapshot_config = Some(self.config_at(new_index));
        self.log.retain(|e| e.index > new_index);
        self.snapshot_index = new_index;
        self.snapshot_term = new_term;
        self.snapshot_dirty = true;
        // Cache the serialized snapshot image so [`snapshot_chunk_for`] slices cached
        // bytes instead of re-serializing the whole `metadata` **per 1KB chunk** — an
        // O(state)-per-`InstallSnapshot`-message cost that pins the consensus loop and
        // storms elections while catching a follower up on a large state (the
        // control-plane counterpart of the CP-data driver-liveness fix, ADR 0017). An
        // in-core SM's image *is* its `metadata`; a `DRIVER_APPLIED` SM's image lives
        // in the engine and the driver supplies it via [`set_snapshot_blob`] *before*
        // snapshotting (`snapshot_upto(engine_applied)`), so don't clobber it. For the
        // in-core plane `metadata` reflects `last_applied`, and `new_index <=
        // last_applied`, so this serializes state **at least as fresh** as the base —
        // and the control plane only ever snapshots to `last_applied` (via
        // [`snapshot`]), so it matches the base exactly. Keeps the invariant
        // `snapshot_index > 0 ⟹ snapshot_blob.is_some()` for both SM kinds, so a chunk
        // is never a 0-byte ship.
        if !S::DRIVER_APPLIED {
            self.snapshot_blob =
                Some(serde_json::to_vec(&self.metadata).expect("metadata serializes"));
        }
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

    /// Highest log index known durable on disk (the **durable-before-visible**
    /// frontier; see [`RaftCore::mark_durable_through`]).
    pub fn durable_index(&self) -> u64 {
        self.durable_index
    }

    /// Record that the WAL is durably fsynced through log index `index`, then apply
    /// any now-durable committed entries. **The driver must call this after every
    /// `env.sync(WAL)`** (and only then), passing the last log index it just made
    /// durable — that is what advances client-visible state. Idempotent and
    /// monotonic; an `index` below the current watermark is ignored.
    pub fn mark_durable_through(&mut self, index: u64) {
        if index > self.durable_index {
            self.durable_index = index;
            self.apply();
        }
    }

    /// Highest applied log index.
    pub fn last_applied(&self) -> u64 {
        self.last_applied
    }

    /// A clone of the applied state machine. (The control plane reads this as
    /// `Metadata` via the specialized [`RaftCore::metadata`].)
    pub fn state(&self) -> S {
        self.metadata.clone()
    }

    /// The sequence of commands applied so far, in order.
    pub fn applied(&self) -> Vec<C> {
        self.applied.clone()
    }

    /// Take the committed-and-durable commands a `DRIVER_APPLIED` state machine has
    /// not yet handed to its async driver, as `(index, command)` in commit order.
    /// **The driver applies each to the real engine (in order) and is the only
    /// consumer.** Always empty for the in-core control plane (which applies in
    /// `apply` instead). Mirrors `AccordCore::drain_apply` (ADR 0017).
    pub fn drain_apply(&mut self) -> Vec<(u64, C)> {
        std::mem::take(&mut self.pending_apply)
    }

    /// Provide the engine-image bytes a `DRIVER_APPLIED` leader ships to a lagging
    /// follower via `InstallSnapshot` (ADR 0017 A.2). The driver refreshes this
    /// from the `StorageEngine` when it compacts, so the shipped snapshot matches
    /// the (now-truncated) log prefix. No effect for an in-core state machine
    /// (which serializes `metadata` directly).
    pub fn set_snapshot_blob(&mut self, bytes: Vec<u8>) {
        self.snapshot_blob = Some(bytes);
    }

    /// Take a fully-received snapshot's `(last_index, engine-image bytes)` for the
    /// driver to write into the engine (a `DRIVER_APPLIED` follower catching up).
    /// `None` when no install is pending.
    pub fn drain_pending_install(&mut self) -> Option<(u64, Vec<u8>)> {
        self.pending_install.take()
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

    /// Index of the last log entry (the snapshot base if the log tail is empty).
    pub fn last_log_index(&self) -> u64 {
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

    // ---- membership / configuration (ADR 0017 C) ------------------------

    /// Whether this node is a voter in the active configuration.
    fn is_voter(&self) -> bool {
        self.config.contains(&self.id)
    }

    /// The active voter configuration.
    #[must_use]
    pub fn config(&self) -> BTreeSet<NodeId> {
        self.config.clone()
    }

    /// Adopt `voters` as the active config and keep `peers`/`cluster_size` in sync,
    /// so every quorum/replication/election decision reflects it immediately.
    fn apply_config(&mut self, voters: BTreeSet<NodeId>) {
        self.peers = voters.iter().copied().filter(|n| *n != self.id).collect();
        self.cluster_size = voters.len();
        self.config = voters;
    }

    /// The voter config effective at log `index`: the latest config-bearing entry
    /// with `entry.index <= index`, else the snapshot's config, else the initial.
    fn config_at(&self, index: u64) -> BTreeSet<NodeId> {
        self.log
            .iter()
            .rev()
            .find(|e| e.index <= index && e.config.is_some())
            .and_then(|e| e.config.clone())
            .or_else(|| self.snapshot_config.clone())
            .unwrap_or_else(|| self.initial_config.clone())
    }

    /// Recompute the active config from the current log tail (used after a
    /// truncation, which may have removed a config entry, or on recovery).
    fn recompute_config(&mut self) {
        let voters = self.config_at(self.last_log_index());
        self.apply_config(voters);
    }

    /// Whether a membership change is in flight: an uncommitted config entry.
    fn config_change_in_flight(&self) -> bool {
        self.log
            .iter()
            .any(|e| e.config.is_some() && e.index > self.commit_index)
    }

    /// Propose a **single-server** membership change (ADR 0017 C): `voters` becomes
    /// the new configuration. Leader-only; the change is adopted immediately (Raft
    /// uses the latest log config) and durable once committed. Rejected if a change
    /// is already in flight, if `voters` differs from the current config by more
    /// than one server (single-server changes never create two disjoint
    /// majorities — multi-server needs joint consensus, deferred), or if it would
    /// remove the current leader (transfer leadership first).
    pub fn change_membership(&mut self, voters: BTreeSet<NodeId>) -> ProposeResult {
        if self.role != Role::Leader {
            return ProposeResult::NotLeader {
                leader: self.leader_id,
            };
        }
        let delta = self.config.symmetric_difference(&voters).count();
        if delta != 1 || self.config_change_in_flight() || !voters.contains(&self.id) {
            // No-op rejection (a self-removal / multi-server / in-flight change):
            // report not-accepted by returning the leader hint. (`delta == 0` is
            // also rejected — nothing to change.)
            return ProposeResult::NotLeader {
                leader: Some(self.id),
            };
        }
        let index = self.last_log_index() + 1;
        self.log_append(LogEntry {
            term: self.current_term,
            index,
            command: S::noop(),
            config: Some(voters),
        });
        self.maybe_advance_commit();
        self.apply();
        ProposeResult::Accepted { index }
    }

    /// Set the **election-timeout base** and re-arm the election timer from `now`.
    /// The randomized timeout is drawn from `[base, 2*base)` (as always); widening
    /// `base` makes this node slower to campaign, which the assembly layer wants
    /// for a node whose driver does real disk I/O and may briefly stall past the
    /// 150ms default (that stall would otherwise trigger a spurious election and,
    /// with pre-vote, at least a spurious pre-vote round). Additive: existing
    /// callers keep the 150ms default. Deterministic — timing comes from the
    /// injected `now`/`entropy`, never a wall clock.
    pub fn set_election_timeout(&mut self, base: Duration, now: Nanos, entropy: u64) {
        self.election_base = base;
        self.reset_election_timer(now, entropy);
    }

    /// The current election-timeout base (the low end of the randomized range).
    #[must_use]
    pub fn election_timeout(&self) -> Duration {
        self.election_base
    }

    fn reset_election_timer(&mut self, now: Nanos, entropy: u64) {
        let base = self.election_base.as_nanos() as u64;
        let extra = if base == 0 { 0 } else { entropy % base };
        self.election_deadline = Nanos(now.0.saturating_add(base + extra));
    }

    // ---- durable-state helpers ------------------------------------------

    /// Append a log entry and record it for persistence. A config-bearing entry is
    /// adopted immediately (Raft single-server change: latest log config wins).
    fn log_append(&mut self, entry: LogEntry<C>) {
        if let Some(voters) = &entry.config {
            self.apply_config(voters.clone());
        }
        self.pending.push(WalRecord::Append(entry.clone()));
        self.log.push(entry);
    }

    /// Truncate the log to `keep` entries and record it for persistence. If a
    /// truncated entry carried a config, the active config reverts to the latest
    /// surviving one.
    fn log_truncate(&mut self, keep: usize) {
        self.log.truncate(keep);
        self.pending.push(WalRecord::Truncate { keep });
        self.recompute_config();
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
    pub fn tick(&mut self, now: Nanos, entropy: u64) -> Vec<Out<C>> {
        match self.role {
            Role::Leader => {
                if now.0 >= self.heartbeat_deadline.0 {
                    self.heartbeat_deadline = Nanos(now.0.saturating_add(self.heartbeat_nanos()));
                    return self.broadcast_append();
                }
                Vec::new()
            }
            Role::Follower | Role::PreCandidate | Role::Candidate => {
                if now.0 >= self.election_deadline.0 {
                    // Run a **pre-vote** round first (ADR 0009): a node whose driver
                    // briefly stalled past the timeout probes whether it *could* win
                    // before incrementing the term, so it can't disrupt a healthy
                    // leader. A pre-candidate whose round timed out simply restarts
                    // it; a candidate that failed a real election falls back to a
                    // fresh pre-vote (never straight to another term bump).
                    return self.start_pre_vote(now, entropy);
                }
                Vec::new()
            }
        }
    }

    /// Immediately (re)replicate to all peers if leader — the **wake-on-propose**
    /// primitive (ADR 0017 single-write-latency fix): a freshly appended entry can
    /// be shipped at once instead of waiting for the next heartbeat tick. Resets the
    /// heartbeat deadline (this send counts as the period's heartbeat, so the timer
    /// tick won't immediately re-broadcast). Empty on a non-leader.
    pub fn replicate_now(&mut self, now: Nanos) -> Vec<Out<C>> {
        if self.role == Role::Leader {
            self.heartbeat_deadline = Nanos(now.0.saturating_add(self.heartbeat_nanos()));
            self.broadcast_append()
        } else {
            Vec::new()
        }
    }

    /// Handle an inbound message from `from` at `now`.
    pub fn handle(
        &mut self,
        from: NodeId,
        msg: RaftMsg<C>,
        now: Nanos,
        entropy: u64,
    ) -> Vec<Out<C>> {
        // Any message from a higher term forces us to step down first — **except**
        // pre-vote traffic, which by design never changes a node's term (a pre-vote
        // carries only a *prospective* term). Bypassing the step-down here is what
        // makes pre-vote safe: a partitioned node's pre-vote round can never bump a
        // healthy peer's term. A rejecting `PreVoteResp` with a higher term is the
        // one place a pre-candidate adopts a newer term (handled in
        // `handle_pre_vote_resp`), and never beyond the responder's real term.
        let is_pre_vote = matches!(msg, RaftMsg::PreVote { .. } | RaftMsg::PreVoteResp { .. });
        if !is_pre_vote && msg.term() > self.current_term {
            self.current_term = msg.term();
            self.voted_for = None;
            self.role = Role::Follower;
            self.leader_id = None;
        }
        match msg {
            RaftMsg::PreVote {
                term,
                candidate,
                last_log_index,
                last_log_term,
            } => self.handle_pre_vote(candidate, term, last_log_index, last_log_term, now),
            RaftMsg::PreVoteResp { term, granted } => {
                self.handle_pre_vote_resp(from, term, granted, now, entropy)
            }
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
                config,
            } => self.handle_install_snapshot(
                term, leader, last_index, last_term, offset, data, total, done, config, now,
                entropy,
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
    pub fn propose(&mut self, command: C) -> ProposeResult {
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
            config: None,
        });
        // Lets a single-node group make progress; safe for larger groups
        // because commit still requires a majority of matchIndex.
        self.maybe_advance_commit();
        self.apply();
        ProposeResult::Accepted { index }
    }

    // ---- message handlers ------------------------------------------------

    /// Answer a [`PreVote`](RaftMsg::PreVote). This is a **read-only** decision: it
    /// never mutates term/vote/role/timer, so a pre-vote round can never disrupt
    /// this node. Grant only if we would actually vote for the candidate:
    ///
    /// - we do **not** currently have a live leader (a leader ourselves, or a
    ///   follower still within its election timeout of the last heartbeat, is
    ///   protected — this is the leader-lease that stops a partitioned node from
    ///   winning a pre-vote and forcing an election);
    /// - the candidate's prospective `term` is not behind ours; and
    /// - the candidate's log is at least as up to date as ours.
    fn handle_pre_vote(
        &mut self,
        candidate: NodeId,
        term: u64,
        last_log_index: u64,
        last_log_term: u64,
        now: Nanos,
    ) -> Vec<Out<C>> {
        let has_live_leader = self.role == Role::Leader
            || (self.leader_id.is_some() && now.0 < self.election_deadline.0);
        let log_ok = last_log_term > self.last_log_term()
            || (last_log_term == self.last_log_term() && last_log_index >= self.last_log_index());
        let granted = !has_live_leader && term >= self.current_term && log_ok;
        vec![(
            candidate,
            RaftMsg::PreVoteResp {
                // Grant echoes the prospective term (so the pre-candidate correlates
                // it to its round); a reject reports our real term (so a stale
                // pre-candidate learns it is behind).
                term: if granted { term } else { self.current_term },
                granted,
            },
        )]
    }

    /// Tally a [`PreVoteResp`](RaftMsg::PreVoteResp). Only meaningful while we are a
    /// pre-candidate for this exact round (`term == current_term + 1`). On reaching
    /// a pre-vote majority we start the **real**, term-incrementing election. A
    /// rejecting response carrying a higher term tells us we are behind, so we step
    /// down to a plain follower at that term (never beyond it) and let normal
    /// replication catch us up.
    fn handle_pre_vote_resp(
        &mut self,
        from: NodeId,
        term: u64,
        granted: bool,
        now: Nanos,
        entropy: u64,
    ) -> Vec<Out<C>> {
        if self.role != Role::PreCandidate {
            return Vec::new();
        }
        if granted {
            if term == self.current_term + 1 {
                self.pre_votes.insert(from);
                if self.pre_votes.len() >= self.majority() {
                    return self.start_election(now, entropy);
                }
            }
        } else if term > self.current_term {
            // We are behind the responder; adopt its term as a follower and stop
            // pre-campaigning (a higher-term *reject* is the only pre-vote message
            // that moves our term — and only up to the responder's real term).
            self.current_term = term;
            self.voted_for = None;
            self.role = Role::Follower;
            self.leader_id = None;
            self.pre_votes.clear();
            self.reset_election_timer(now, entropy);
        }
        Vec::new()
    }

    fn handle_request_vote(
        &mut self,
        candidate: NodeId,
        term: u64,
        last_log_index: u64,
        last_log_term: u64,
        now: Nanos,
        entropy: u64,
    ) -> Vec<Out<C>> {
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

    fn handle_vote_resp(
        &mut self,
        from: NodeId,
        term: u64,
        granted: bool,
        now: Nanos,
    ) -> Vec<Out<C>> {
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
        entries: Vec<LogEntry<C>>,
        leader_commit: u64,
        now: Nanos,
        entropy: u64,
    ) -> Vec<Out<C>> {
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
    ) -> Vec<Out<C>> {
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
        config: Option<BTreeSet<NodeId>>,
        now: Nanos,
        entropy: u64,
    ) -> Vec<Out<C>> {
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
            // Advance the snapshot base + reset the log/applied state common to both
            // state-machine kinds.
            let install = |core: &mut Self| {
                core.snapshot_index = inc.last_index;
                core.snapshot_term = inc.last_term;
                core.last_applied = inc.last_index;
                core.commit_index = inc.last_index;
                core.log.clear();
                core.applied.clear();
                core.snapshot_dirty = true;
                // Adopt the snapshot's voter configuration (ADR 0017 C): the image
                // bytes carry no Raft membership. With the log now empty,
                // `recompute_config` resolves to this snapshot config (or initial).
                core.snapshot_config = config.clone();
                core.recompute_config();
            };
            if S::DRIVER_APPLIED {
                // The bytes are the leader's engine image; the driver writes them
                // into this follower's engine (`drain_pending_install`). The in-core
                // `metadata` stays the unit placeholder.
                let bytes = inc.buf;
                install(self);
                // Retain the installed image as our own snapshot blob. A
                // `DRIVER_APPLIED` state machine's image lives in the engine, not in
                // `metadata`, so `snapshot_chunk_for` ships `snapshot_blob` — which is
                // only set by the driver when it *compacts* (`set_snapshot_blob`). A
                // node that caught up via this install has `snapshot_index > 0` but,
                // until its first compaction, no blob; if it then becomes leader (or
                // must re-ship to a third follower below its compacted prefix) it would
                // ship `unwrap_or_default()` = 0 bytes, the receiver would decode an
                // empty image (`EOF while parsing a value`) and could never catch up.
                // The just-installed bytes are exactly a valid image at
                // `snapshot_index`, so keep them; the driver overwrites this on its
                // next compaction with the fresh engine image.
                self.snapshot_blob = Some(bytes.clone());
                self.pending_install = Some((inc.last_index, bytes));
                return vec![(
                    leader,
                    RaftMsg::InstallSnapshotResp {
                        term: self.current_term,
                        last_index: inc.last_index,
                        next_offset: inc.total,
                    },
                )];
            }
            // In-core state machine: deserialize the image into `metadata`. A
            // malformed snapshot would be a leader bug; drop + re-request rather
            // than install garbage.
            match serde_json::from_slice::<S>(&inc.buf) {
                Ok(state) => {
                    self.metadata = state;
                    install(self);
                    // Retain the received image as our own `snapshot_blob` so this
                    // node can **re-ship** a non-empty image if it later leads a
                    // catch-up of a third follower below its compacted prefix — the
                    // same invariant the `DRIVER_APPLIED` branch keeps above
                    // (`snapshot_index > 0 ⟹ snapshot_blob.is_some()`). The bytes are
                    // exactly a valid serialized image at `snapshot_index`; without
                    // this, a node that only ever caught up via install would ship
                    // `unwrap_or_default()` = 0 bytes and the receiver would decode an
                    // empty state (`EOF while parsing a value`). See the regression
                    // `install_snapshot.rs::caught_up_control_node_reships_non_empty`.
                    self.snapshot_blob = Some(inc.buf);
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
    ) -> Vec<Out<C>> {
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

    /// Begin a **pre-vote** round (ADR 0009): become a [`PreCandidate`](Role::PreCandidate)
    /// **without** touching the term or casting a real vote, and solicit
    /// [`PreVote`](RaftMsg::PreVote)s for the prospective term (`current_term + 1`).
    /// The real, term-incrementing election starts only once a majority pre-votes
    /// (see [`handle_pre_vote_resp`](Self::handle_pre_vote_resp)); a lone
    /// partitioned/stalled node thus loops through harmless pre-vote rounds instead
    /// of ratcheting the cluster's term.
    fn start_pre_vote(&mut self, now: Nanos, entropy: u64) -> Vec<Out<C>> {
        // A node removed from the configuration must not campaign (mirrors
        // `start_election`): it can't win and would only disrupt the survivors.
        if !self.is_voter() {
            self.reset_election_timer(now, entropy);
            return Vec::new();
        }
        self.role = Role::PreCandidate;
        // The election timer expired ⇒ we no longer believe in a live leader; drop
        // the hint so we will grant *others'* pre-votes this round too. Term and
        // vote are deliberately untouched.
        self.leader_id = None;
        self.pre_votes.clear();
        self.pre_votes.insert(self.id);
        self.reset_election_timer(now, entropy);

        // Single-node (or otherwise already a majority): skip straight to the real
        // election, which becomes leader immediately.
        if self.pre_votes.len() >= self.majority() {
            return self.start_election(now, entropy);
        }
        let (lli, llt) = (self.last_log_index(), self.last_log_term());
        let prospective = self.current_term + 1;
        self.peers
            .iter()
            .map(|&p| {
                (
                    p,
                    RaftMsg::PreVote {
                        term: prospective,
                        candidate: self.id,
                        last_log_index: lli,
                        last_log_term: llt,
                    },
                )
            })
            .collect()
    }

    fn start_election(&mut self, now: Nanos, entropy: u64) -> Vec<Out<C>> {
        // A node removed from the configuration must not campaign (it cannot win
        // and would only disrupt the surviving voters). It stays a quiet follower
        // until it learns it is gone, then idles (ADR 0017 C).
        if !self.is_voter() {
            self.reset_election_timer(now, entropy);
            return Vec::new();
        }
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

    fn become_leader(&mut self, now: Nanos) -> Vec<Out<C>> {
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
            command: S::noop(),
            config: None,
        });
        self.heartbeat_deadline = Nanos(now.0.saturating_add(self.heartbeat_nanos()));
        self.broadcast_append()
    }

    fn broadcast_append(&self) -> Vec<Out<C>> {
        self.peers.iter().map(|&p| self.replicate_to(p)).collect()
    }

    /// Build the right replication message for `peer`: an `InstallSnapshot` if
    /// the entries it needs have been compacted away, otherwise `AppendEntries`.
    fn replicate_to(&self, peer: NodeId) -> Out<C> {
        let next = self.next_index.get(&peer).copied().unwrap_or(1).max(1);
        // The entry before `next` is in our snapshot (or earlier) — we can't form
        // a valid `prev_log_term`, so ship the snapshot instead, as the next
        // offset-addressed chunk for this peer.
        if next <= self.snapshot_index {
            return self.snapshot_chunk_for(peer);
        }
        let prev_log_index = next - 1;
        let prev_log_term = self.term_at(prev_log_index);
        let entries: Vec<LogEntry<C>> = self
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
    /// Pure and **cheap**: it slices the pre-serialized [`snapshot_blob`] rather than
    /// re-serializing the state, so repeated calls at the same offset are
    /// byte-identical, deterministic, and O(chunk) — not O(state) per chunk.
    fn snapshot_chunk_for(&self, peer: NodeId) -> Out<C> {
        // Both SM kinds ship the **cached** serialized image, never a fresh
        // per-chunk serialize: a `DRIVER_APPLIED` engine image (set by the driver on
        // compaction / retained on install) or an in-core `metadata` image (cached by
        // [`snapshot_upto`] when the base advances / retained on install). The
        // invariant `snapshot_index > 0 ⟹ snapshot_blob.is_some()` holds for both, and
        // this is only reached when `next <= snapshot_index` (so `snapshot_index > 0`),
        // so the `unwrap_or_default` fallback is never taken in practice.
        let serialized = self.snapshot_blob.clone().unwrap_or_default();
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
                config: self.snapshot_config.clone(),
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
        // The apply frontier is **role-aware** (ADR 0009, durable-before-visible):
        //
        // - **Leader:** `min(commit_index, durable_index)`. The leader's
        //   `metadata()`/`applied()` is what a proposer *acks* on, so a command
        //   must be on disk before it becomes client-visible — an entry committed
        //   but not yet fsynced stays invisible, and a crash in that window loses
        //   nothing a client could have observed. `durable_index` is advanced by
        //   the driver after `env.sync(WAL)` (`mark_durable_through`). This is
        //   acute single-node, where commit rests on the leader alone.
        //
        // - **Non-leader:** `commit_index`. A follower never acks a write to a
        //   client (writes are proposed to the leader); it only serves *reads* of
        //   its local `Metadata`. A committed entry already rests on a quorum of
        //   durable logs (the driver fsyncs before sending, so a follower fsyncs
        //   before its `AppendEntriesResp` and the leader before `AppendEntries`),
        //   so a follower may safely expose it without waiting on its *own* local
        //   fsync — gating it there would only widen cross-node replication-
        //   visibility lag. `last_applied` only moves forward, so a follower that
        //   applied to commit then wins an election keeps those (committed/quorum-
        //   durable) entries; its own *future* proposals are still durability-
        //   gated (their index exceeds `durable_index` until it fsyncs).
        let frontier = if self.role == Role::Leader {
            self.commit_index.min(self.durable_index)
        } else {
            self.commit_index
        };
        while self.last_applied < frontier {
            self.last_applied += 1;
            let offset = (self.last_applied - self.snapshot_index - 1) as usize;
            let command = self.log[offset].command.clone();
            if S::DRIVER_APPLIED {
                // The async driver applies this to the real engine (drained via
                // `drain_apply`); the core only decides the order. Don't grow the
                // unbounded `applied` log for the data plane.
                self.pending_apply.push((self.last_applied, command));
            } else {
                self.metadata.apply(&command);
                self.applied.push(command);
            }
        }
        // No per-apply checkpoint: durability comes from the snapshot taken by
        // `snapshot()` plus the persisted log tail; recovery re-applies the tail.
    }

    fn heartbeat_nanos(&self) -> u64 {
        self.heartbeat_interval.as_nanos() as u64
    }
}

/// Control-plane-specific conveniences for the default instantiation
/// (`RaftCore<MetaCommand, Metadata>`), so existing callers keep reading the
/// applied state as `Metadata` rather than the generic [`RaftCore::state`].
impl RaftCore<MetaCommand, Metadata> {
    /// A clone of the applied metadata state machine.
    pub fn metadata(&self) -> Metadata {
        self.state()
    }
}
