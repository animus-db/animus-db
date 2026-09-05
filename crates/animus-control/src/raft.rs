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
    /// [`RaftCore::drain_apply`]) — a sync-core / async-driver split, required
    /// because a `StorageEngine`
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

/// Maximum number of log entries shipped to one peer in a single
/// `AppendEntries` message (issues #532/#537, ADR 0009's 2026-09-01
/// amendment). Without this, [`replicate_to`](RaftCore::replicate_to) sent
/// the ENTIRE outstanding tail (`next_index..=last_log_index`) in one
/// message, and `replicate_now`'s wake-on-propose (no coalescing beyond the
/// boolean `ProposeSignal`) fires that unbounded send again on every single
/// propose — so a lagging peer (freshly added learner, or any voter behind
/// more than a heartbeat) under a sustained per-entry proposer received an
/// unbounded sequence of ever-larger, overlapping `AppendEntries` batches,
/// each superseding the last before it could be fully processed and acked,
/// permanently starving `next_index`'s advance (confirmed live: a learner
/// pinned at a fixed `match_index` for an entire run while the leader's own
/// log kept growing).
///
/// **Derivation**: the cap only has to stop the send from growing *without
/// bound* — a real replicate round (WAL append + `fsync` on the receiving
/// peer) costs roughly the same wall-clock time whether it carries a dozen
/// entries or a few hundred, so shrinking the cap much below "a real
/// catch-up distance" only *adds* round trips (each still paying that same
/// fixed `fsync` cost) without shrinking per-round work by much — a net
/// loss once a peer's replication round, not per-entry cloning, is the
/// bottleneck (confirmed empirically: a small cap and no cap converged
/// equally poorly under a disk-latency-throttled peer in this fix's own
/// `SimEnv` centerpiece test, `animus-cp-data/tests/
/// learner_catchup_under_load.rs`, before the value was widened here).
/// `node.rs`'s `SNAPSHOT_THRESHOLD` (control plane) / `lib.rs`'s
/// `COMPACT_THRESHOLD` (CP data plane) — both 64 — are the number of
/// applied-but-uncompacted entries this plane keeps in the live log tail
/// before compacting past a peer that hasn't caught up; a peer that falls
/// more than roughly that far behind takes the (already-bounded,
/// `SNAPSHOT_CHUNK_BYTES`-chunked) `InstallSnapshot` path via `next <=
/// self.snapshot_index` regardless of this cap, so this path's own value
/// only has to be reasonable for catch-up distances *inside* that window —
/// **512**, comfortably larger than that window (covering it in one round
/// trip in the common case) while still orders of magnitude below the
/// unbounded growth observed in the field (a leader's log racing past
/// 25,000 entries while a stuck peer's own `AppendEntries` kept growing to
/// match). `handle_append_resp`'s success arm already re-invokes
/// [`replicate_to`](RaftCore::replicate_to) immediately when more remains,
/// so a peer needing several batches clears the backlog in back-to-back
/// acked round trips, not one per external propose. This only bounds a
/// *lagging* peer's traffic: an up-to-date peer's ordinary steady-state
/// `AppendEntries` (one or a few fresh entries per propose) is far under
/// this cap and unaffected.
const MAX_APPEND_ENTRIES_BATCH: usize = 512;

/// How a call site's `InstallSnapshot` chunk resend for an already-outstanding
/// (unchanged) offset is bounded (issues #532/#537, ADR 0009's third
/// 2026-09-01 amendment — the residual beyond `MAX_APPEND_ENTRIES_BATCH` and
/// `COMPACT_DEFER_CEILING`). See [`RaftCore::snapshot_chunk_for`]'s doc for
/// the full mechanism and [`RaftCore::snapshot_chunk_sent`]'s doc for the
/// marker this gates against. A chunk for a genuinely NEW offset (real ack
/// progress, or nothing sent to this peer yet) is never held back by either
/// variant, at any call site — this only ever bounds a repeat of the exact
/// chunk already in flight.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum SnapshotResend {
    /// At most `0` resends of an unchanged offset from THIS call site before
    /// the next one is suppressed — i.e. send once, then wait for either
    /// real progress or a different trigger. This is `replicate_now`'s
    /// (wake-on-propose's) own setting: it fires on every propose, so
    /// letting it also resend an unmoved snapshot offset without limit is
    /// exactly the write-rate flood this fix closes.
    Capped(u32),
    /// No cap — a resend of the SAME outstanding offset is always allowed.
    /// Reserved for triggers that are themselves already bounded by
    /// something other than write rate: a periodic heartbeat tick, a
    /// peer's own `AppendEntries` success/reject response, an explicit
    /// `WakeRequest` poke, a fresh leadership term. NOT used for the
    /// ack-handler's own resend (`handle_install_snapshot_resp`) — see
    /// that method's own doc for why a bounded cap, not `Always`, is what
    /// belongs there.
    Always,
}

/// The resend cap `handle_install_snapshot_resp` passes for its own
/// ack-driven resend (`SnapshotResend::Capped`) — see that method's own
/// doc. Not `0` (that starves the retransmit role the same way gating this
/// call site out entirely did — see `snapshot_chunk_for`'s doc for the two
/// rejected shapes) and not unbounded (that reproduces this fix's own
/// flood, just gated behind an ack instead of a propose). `8` is small
/// enough to bound worst-case volume by roughly an order of magnitude
/// versus no cap at all, while comfortably covering the handful of
/// overlapping in-flight acks a genuinely still-converging transfer
/// produces before either real progress or the next heartbeat arrives.
const SNAPSHOT_ACK_RESEND_CAP: u32 = 8;

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
    /// For a **membership-change** entry, the new **learner** set this entry
    /// installs (ADR 0058 Train 1: a non-voting membership class) — `None` for
    /// an ordinary command entry, and always `Some` (though possibly empty)
    /// exactly when `config` is `Some`: every membership-change entry restates
    /// both sets together, so "does this entry carry a config" stays the single
    /// `config.is_some()` test every existing call site already uses.
    /// `#[serde(default)]` so pre-existing entries decode with no learners.
    #[serde(default)]
    pub learners: Option<BTreeSet<NodeId>>,
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
        /// **Issue #554.** Echoes the responder's own
        /// [`RaftCore::state_machine_behind`] — `true` when its state
        /// machine (never touched by this message, which is purely a log
        /// fact) is behind its own log's compacted start and needs a fresh
        /// `InstallSnapshot` regardless of `match_index`/`next_index`, which
        /// the log tail alone can satisfy with nothing left to signal a gap.
        /// `#[serde(default)]` so an older wire peer (which never sets this)
        /// decodes to `false` — the pre-existing behavior, never a hazard on
        /// its own (a leader that never learns about a gap just doesn't
        /// proactively close it; the replica itself still refuses to serve
        /// reads or campaign while behind, see `state_machine_behind`'s
        /// doc). Always `false` for the in-core control plane, which never
        /// sets `state_machine_behind` at all.
        #[serde(default)]
        needs_snapshot: bool,
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
        /// The **learner** configuration at the snapshot (ADR 0058 Train 1),
        /// mirroring `config` above — the image bytes carry no Raft membership
        /// of either class. `None` (default) ⇒ no learners.
        #[serde(default)]
        learners: Option<BTreeSet<NodeId>>,
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
    /// Sent only by [`RaftCore::transfer_leadership`]: tells a fully caught-up
    /// voter to campaign **immediately**, bypassing the election timeout (and,
    /// crucially, pre-vote — a live leader's lease would otherwise reject the
    /// pre-vote round). `term` is the sending leader's current term; a recipient
    /// no longer at that term (e.g. it already saw a newer leader) ignores it.
    /// Resolves the "leader can never remove itself" gap: to move a healthy
    /// replica off the current leader, the leader transfers away first, then the
    /// new leader performs the removal itself.
    TimeoutNow { term: u64 },
    /// Sent **once** by a leader entering quiescence (ADR 0044 phase-1 PR3): "I
    /// have nothing left to replicate and intend to stop ticking." A follower
    /// only accepts it (setting its own quiesced flag, so its own
    /// [`next_deadline`](RaftCore::next_deadline) also returns `None`) if it is
    /// provably caught up to exactly this state — `term` matches its own
    /// current term, the sender is its recorded leader, and its own
    /// `last_log_index` and `commit_index` both equal `commit_index` here.
    /// Otherwise it is ignored outright: the follower keeps ticking normally,
    /// and its own ordinary election timeout is what eventually notices if the
    /// leader really is gone (see `RaftCore`'s module-level quiescence doc).
    Quiesce { term: u64, commit_index: u64 },
    /// Sent by a follower whose local caller just touched it while it was
    /// quiesced (ADR 0044 phase-1 PR3, fork B) — "are you still there?" to its
    /// recorded `leader_id`, instead of blindly assuming the leader is dead and
    /// campaigning immediately. A live leader answers with its ordinary
    /// `AppendEntries` (whether or not it was itself quiesced when this
    /// arrived — see [`RaftCore::handle`]'s doc), which the follower processes
    /// exactly like any other heartbeat, resetting its election timer. If the
    /// leader really is gone, nothing answers, and the follower's own freshly
    /// re-armed election timeout (see
    /// [`on_local_wake`](RaftCore::on_local_wake)) is what lets it campaign.
    /// Carries no term authority of its own (like
    /// [`Heartbeat`](RaftMsg::Heartbeat) — see [`term`](RaftMsg::term)):
    /// answering or ignoring it never depends on the sender's believed term,
    /// only on whether `self` is currently a `Leader`.
    WakeRequest { term: u64 },
}

impl<C> RaftMsg<C> {
    /// The Raft term carried by this message. A [`Heartbeat`](RaftMsg::Heartbeat)
    /// or [`WakeRequest`](RaftMsg::WakeRequest) is not consensus traffic and
    /// carries no term *authority* (it reports 0, never forcing a step-down) —
    /// the driver intercepts heartbeats before the core sees one, and a
    /// `WakeRequest`'s own `term` field is purely informational (see its doc).
    fn term(&self) -> u64 {
        match self {
            RaftMsg::PreVote { term, .. }
            | RaftMsg::PreVoteResp { term, .. }
            | RaftMsg::RequestVote { term, .. }
            | RaftMsg::RequestVoteResp { term, .. }
            | RaftMsg::AppendEntries { term, .. }
            | RaftMsg::AppendEntriesResp { term, .. }
            | RaftMsg::InstallSnapshot { term, .. }
            | RaftMsg::InstallSnapshotResp { term, .. }
            | RaftMsg::TimeoutNow { term }
            | RaftMsg::Quiesce { term, .. } => *term,
            RaftMsg::Heartbeat { .. } | RaftMsg::WakeRequest { .. } => 0,
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

/// A member's role in the active configuration (ADR 0058 Train 1): a
/// **voter** counts toward every quorum computation (commit-index
/// advancement, election majorities) and may campaign; a **learner** is a
/// non-voting member that receives `AppendEntries`/`InstallSnapshot` exactly
/// like a voter (its `match_index` is tracked the same way) but is excluded
/// from quorum math entirely and never campaigns or pre-votes. See
/// [`RaftCore::member_role`]/[`RaftCore::add_learner`]/
/// [`RaftCore::promote_learner`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemberRole {
    Voter,
    Learner,
}

/// Outcome of proposing a command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProposeResult {
    /// Appended to the leader's log at `index`, under the leader's own
    /// current `term` (will replicate + commit).
    ///
    /// **`term` is what lets a proposer later prove a committed-and-applied
    /// entry at `index` is genuinely the one it appended**, not a different
    /// command that came to occupy the same log position after this one was
    /// truncated by a leadership change (log-matching: identical `index` +
    /// `term` implies identical entry, cluster-wide, for the life of the
    /// log). `index` alone cannot make that distinction — see
    /// `animus-cp-data`'s `KindBatchOutcome` doc for the incident this
    /// closed.
    Accepted { index: u64, term: u64 },
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

    // The active **learner** set (ADR 0058 Train 1), mirroring `config` above
    // but never contributing to `peers`/`cluster_size`/quorum math — a learner
    // is deliberately kept out of the set those are derived from, so the
    // majority-computation call sites (`maybe_advance_commit`, `majority()`
    // via `cluster_size`) need no learner-awareness at all. A learner *is*
    // still replicated to (see `broadcast_append`/`become_leader`, which union
    // this in) and its `match_index` is tracked in the same `next_index`/
    // `match_index` maps as any voter's. Kept in sync with `config` by
    // `apply_config`, from the same config-in-log discipline (ADR 0017 C):
    // every membership-change `LogEntry` carries both sets together (see
    // `LogEntry::learners`'s doc).
    learners: BTreeSet<NodeId>,
    // The learner set the node booted with — always empty, since `RaftCore::
    // new` never bootstraps learners (a learner is only ever introduced via
    // `add_learner` after the group is running). Kept for symmetry with
    // `initial_config`'s fallback role in `learners_at`.
    initial_learners: BTreeSet<NodeId>,
    // The learner set recorded by the latest local snapshot, mirroring
    // `snapshot_config`.
    snapshot_learners: Option<BTreeSet<NodeId>>,

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
    // Issue #595: an OBSERVATIONAL record of the last genuine leader contact
    // this replica has seen, deliberately decoupled from `leader_id`'s own
    // pre-vote-driven lifecycle. `leader_id` is cleared the instant this
    // node's own election timer lapses (`start_pre_vote`) or it starts a
    // real election (`start_election`) — correct for consensus (a stale
    // belief must never be trusted for granting votes or serving as a relay
    // target), but it gives a *health/readiness* reader a false-negative
    // window on every transient one-sided delay >= one election timeout,
    // even while the real leader is fully healthy and heartbeating every
    // other replica the whole time (see `leader_within`'s doc, and the
    // engineering-lessons.md entry this issue produced).
    //
    // Set (never read) here in the sync core, at the points where
    // `leader_id` is set (or reaffirmed) from a GENUINE leader contact: a
    // valid `AppendEntries`/`InstallSnapshot` from the current term's
    // leader (`handle_append_entries`, `handle_install_snapshot`), this
    // node itself becoming leader (`become_leader`, which records itself),
    // and — the one point that is a REFRESH, not a first-set — every
    // routine heartbeat broadcast a leader sends (`tick`'s `Role::Leader`
    // arm): without this fourth point, a long-lived, perfectly healthy
    // leader's own belief in itself would stay pinned at the timestamp of
    // its original election forever, and it would spuriously fail its own
    // `leader_within` check (hence its own `/admin/health`) a few election
    // timeouts after winning, despite having led continuously and
    // healthily for as long as it has (confirmed live: `tests/
    // admin_endpoint.rs::admin_interface_surfaces_state_and_actions`,
    // whose `/admin/health` check runs ~10s after election with no
    // intervening `MetaCommand`, found this gap the first time this field
    // was built).
    //
    // Cleared ONLY on a real higher-term step-down — the two places this
    // core learns, from a peer, that its current term (and whatever leader
    // it associated with that term) is now stale: the generic higher-term
    // guard in `handle` and `handle_pre_vote_resp`'s own higher-term-reject
    // branch (a `PreCandidate` learns of a newer term without going through
    // `handle`'s generic dispatch). In both cases a *provably newer* term
    // exists elsewhere, so the old leader really is obsolete — clearing
    // here is honest, not hair-triggered. It is deliberately NOT cleared by
    // `start_pre_vote`/`start_election`'s own `leader_id = None`: those fire
    // on this node's own local election-timer suspicion, with no evidence
    // the old leader actually failed (that is exactly the false-negative
    // window this field exists to survive).
    //
    // MUST NEVER be read by any election/pre-vote/safety/replication
    // decision — it exists solely for `leader_within`, an observational
    // accessor for an operational readiness probe (`animusd::admin::
    // health`). Consensus continues to consult `leader_id`/`election_
    // deadline` exclusively, unchanged.
    last_leader_contact: Option<(NodeId, Nanos)>,

    // Pre-candidate state: nodes that have granted the current pre-vote round.
    // Rebuilt each `start_pre_vote`; only read while `role == PreCandidate`.
    pre_votes: BTreeSet<NodeId>,
    // Candidate state.
    votes: BTreeSet<NodeId>,
    // Leader state.
    next_index: BTreeMap<NodeId, u64>,
    match_index: BTreeMap<NodeId, u64>,
    // Leader-only (issue #554): the highest `snapshot_index` this leader has
    // FULLY shipped and had acknowledged (`handle_install_snapshot_resp`'s
    // completion branch) to each peer via a `needs_snapshot`-triggered
    // `InstallSnapshot`. Read by `handle_append_resp`'s own `needs_snapshot`
    // handling to avoid a livelock: `needs_snapshot: true` stays true on
    // every one of a behind peer's `AppendEntriesResp`s until ITS OWN async
    // apply task actually merges the install into its engine (see
    // `animus-cp-data`'s per-loop-iteration live feed, `drive`'s doc) — a
    // window that can span several of this peer's own heartbeat acks. Without
    // this map, EVERY one of those still-true acks would force `next_index`
    // back to 1 and restart a fresh chunked transfer from scratch, even
    // though the peer already has (and is simply still digesting) a complete
    // one — a self-sustaining cycle that never lets `next_index` stay past
    // `snapshot_index` long enough for the peer to finish, confirmed live
    // (`docs/engineering-lessons.md`'s matching entry). Once a value here is
    // `>= self.snapshot_index`, a further `needs_snapshot: true` from that
    // peer is a known-stale echo of an already-served request and is not
    // re-triggered — `self.snapshot_index` moving again (a fresh compaction
    // outpacing a still-slow peer) naturally invalidates the entry and lets
    // a genuinely new request through. Volatile, like `next_index`/
    // `match_index` themselves — never persisted or snapshotted, and
    // harmlessly stale-but-safe if a peer id is reused (worst case: one
    // needless resend cycle, not a correctness issue).
    snapshot_served_through: BTreeMap<NodeId, u64>,
    // Leader-only, volatile liveness bookkeeping (ADR 0037 hardening PR2): the
    // `now` at which this leader last heard an `AppendEntriesResp` (success OR
    // reject — either proves the peer is up and reachable) from each peer.
    // Stamped in `handle_append_resp` and seeded for every peer in
    // `become_leader`. Deliberately **never persisted or snapshotted** — like
    // `next_index`/`match_index`, it is meaningless across a leadership change
    // (a fresh leader has heard nothing yet) and is rebuilt empty on recovery.
    // A freshly-added peer (via `change_membership`) gets no explicit entry
    // here — `RaftNode::control_peer_believed_alive`'s "never contacted yet"
    // grace clause is the intended handling for that gap, exactly the way
    // `next_index`/`match_index` rely on a sensible `.unwrap_or(..)` default
    // rather than an explicit write at peer-add time. Do NOT "complete" this by
    // wiring it into `PersistedState`/`WalRecord` — that would make a leader's
    // liveness judgment of its peers depend on stale, potentially very old
    // wall-clock reads survived across a restart, which is actively wrong.
    last_contact: BTreeMap<NodeId, Nanos>,
    // Set by `transfer_leadership`: a caught-up voter this leader is handing off
    // to. Re-sent as a `TimeoutNow` on every heartbeat (`broadcast_append`) until
    // this node steps down (the transfer succeeded) — so a single dropped message
    // doesn't strand the handoff. Cleared fresh on every election win. While
    // `Some`, `propose`/`change_membership` freeze (return `NotLeader`) instead
    // of growing the log further, so replication can catch the target up to
    // `last_log_index` (Raft §3.10) — see `transfer_deadline`.
    transfer_target: Option<NodeId>,
    // Set alongside `transfer_target` (only on a *new* arm — re-arming the same
    // target is idempotent and does not push this out, so a caller retrying the
    // arm every tick can't starve the abort check): one election timeout after
    // the arm. `tick` aborts (clears `transfer_target`, resuming proposals) if
    // this passes without the target stepping down — e.g. it crashed, or fell
    // behind after arming and never re-caught-up to `last_log_index`.
    transfer_deadline: Nanos,
    // A peer this leader has just voted out of the configuration, mapped to the
    // index of the config entry that removed it. `broadcast_append` keeps
    // replicating to a departing peer (even though `apply_config` has already
    // dropped it from `peers`) until its `match_index` reaches that index, so the
    // peer durably adopts the config excluding itself instead of only inferring
    // its removal from pre-vote rejection. Leader-local and volatile: cleared on
    // every election win, so a fresh leader's own subsequent removals repopulate
    // it — see the root CLAUDE.md rebalancing ADR for why this is sufficient
    // rather than reconstructed across leadership changes.
    departing: BTreeMap<NodeId, u64>,
    // The index of the first entry this node appended in its current leadership
    // term — the election no-op from `become_leader`. Raft §6.4 / the
    // reconfiguration erratum: a fresh leader's `commit_index` is guaranteed to
    // cover every entry acked by prior leaders only once an entry of its *own*
    // term commits (the commit rule never counts old-term replicas toward a
    // majority), so ReadIndex barriers and membership changes must first wait for
    // `commit_index >= first_term_index`. Only meaningful while `role == Leader`
    // (see [`first_term_index`](Self::first_term_index)); re-set on every
    // election win.
    first_term_index: u64,
    // Per-follower byte offset reached in the in-flight snapshot transfer, so the
    // leader resumes shipping the next chunk on each heartbeat / ack. Cleared for
    // a peer once it has fully installed the snapshot.
    snapshot_offset: BTreeMap<NodeId, u64>,
    // Per-peer `(offset, resends)` of the last `InstallSnapshot` chunk
    // actually SENT (issues #532/#537): `offset` is the byte offset last
    // transmitted; `resends` counts how many times THAT SAME offset has
    // been resent since it was first sent (reset to 0 whenever the offset
    // itself changes). `snapshot_chunk_for` consults this against a
    // caller's own `SnapshotResend::Capped(n)` to decide whether one more
    // resend of an unchanged offset is still allowed. Distinct from
    // `snapshot_offset` (the offset the peer has ACKED): this tracks what
    // the leader last transmitted, which for a peer with an outstanding
    // unacked chunk is normally the same offset repeatedly — exactly the
    // case this map exists to bound. Cleared/removed at the identical
    // points `snapshot_offset` itself is: per-peer once fully installed,
    // wholesale on `snapshot_upto` invalidation (a moved base makes any
    // prior offset meaningless), and on a fresh leadership term.
    snapshot_chunk_sent: BTreeMap<NodeId, (u64, u32)>,
    // Per-peer lifetime count of GENUINE `InstallSnapshot` chunk advances —
    // bumped exactly once whenever `snapshot_chunk_for` builds a chunk for
    // an offset it has never sent before (a capped RESEND of an
    // already-attempted offset never bumps it). Test-observability only, no
    // role in the resend decision itself: it's what lets a test measure
    // "sends per genuinely distinct chunk" exactly, without externally
    // polling `snapshot_offset` at some fixed cadence and undercounting
    // whatever the poll interval is coarser than (found building this fix's
    // own test — see `animus-cp-data/tests/snapshot_resend_bound.rs`).
    // Deliberately NEVER cleared (not by `snapshot_upto` invalidation, not
    // by transfer completion) — a lifetime total survives every restart, the
    // same way `next_index`/`match_index` are never pruned for a peer this
    // core has ever talked to, and the same way a `Metric` counter is never
    // reset.
    snapshot_chunk_advances: BTreeMap<NodeId, u64>,

    // Follower reassembly buffer for an in-progress chunked `InstallSnapshot`.
    incoming_snapshot: Option<IncomingSnapshot>,

    // Timing (virtual). Election timeout is randomized in `[base, 2*base)`.
    // Fixed at 150ms for every constructor — issue #313 removed the
    // `set_election_timeout` setter this comment used to point to: it had
    // zero call sites (no assembly layer was ever built to widen this for a
    // node doing real disk I/O, the use case its own doc described), so it
    // was dead, aspirational API rather than a documented-but-unwired
    // knob worth keeping. See `election_timeout()` for the read-only
    // accessor, still used by `transfer_leadership`'s deadline and by
    // driver-side observability.
    election_base: Duration,
    heartbeat_interval: Duration,
    election_deadline: Nanos,
    heartbeat_deadline: Nanos,

    // Applied state machine and the order commands were applied (for tests /
    // divergence checks). `applied` holds only the window since the last
    // snapshot: `snapshot_upto` drops the covered prefix alongside the log
    // truncation (and install clears it), so it stays bounded in production.
    // For a `DRIVER_APPLIED` state machine `metadata` is an unused unit
    // placeholder and `applied` stays empty — committed commands ride
    // `pending_apply` to the driver instead.
    metadata: S,
    applied: Vec<C>,
    // Committed-and-durable commands a `DRIVER_APPLIED` state machine has not yet
    // handed to its async driver, as `(index, term, command)` in commit order.
    // Always empty for the in-core control plane. Drained by
    // [`RaftCore::drain_apply`].
    pending_apply: Vec<(u64, u64, C)>,

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
    // Lazy-image request (`DRIVER_APPLIED` only): a replication attempt needed an
    // `InstallSnapshot` chunk but `snapshot_blob` was not materialized; the driver
    // polls `take_snapshot_needed`, builds the engine image, and installs it via
    // `set_snapshot_blob`. Never raised by an in-core state machine (its blob is
    // kept eagerly).
    snapshot_needed: bool,
    // **Needs-snapshot state** (issue #554, `DRIVER_APPLIED` planes only —
    // never set for the in-core control plane, see `animus-cp-data`'s
    // `applied.rs` module doc for the full mechanism). `true` once the
    // driver, at `drive()` start, finds its own engine's durable applied
    // watermark strictly below this node's recovered `snapshot_index`: the
    // log's own compacted prefix is gone, and the engine — freshly
    // rebuilt/wiped, or otherwise never caught up that far — holds none of
    // it either. A replica in this state must not be trusted to serve a
    // linearizable or replica-local read (both already gate on the
    // `DRIVER_APPLIED` `engine_applied` watermark, which the driver seeds at
    // the same low value, so this alone already blocks reads with no
    // further change needed) and must not become leader (gated below,
    // mirroring the learner `is_voter()` gate exactly) — a leader built on
    // an incomplete engine could ship a corrupt `InstallSnapshot` image to a
    // perfectly healthy follower. It keeps voting, appending, and committing
    // normally: the log and hard state are intact and valid regardless of
    // what the engine holds. Every `AppendEntriesResp` this node builds
    // while a follower echoes this flag (`needs_snapshot`) so its leader —
    // regardless of `next_index`, which the log tail alone can satisfy —
    // ships a fresh `InstallSnapshot` built at the leader's OWN current
    // applied index (at or ahead of anything this replica's log start could
    // possibly require). Recomputed LIVE by the driver, every loop
    // iteration (never a one-shot latch — see
    // [`set_state_machine_behind`](Self::set_state_machine_behind)'s own
    // doc for why a latch produced a real livelock). Default `false`; only
    // ever set via that method.
    state_machine_behind: bool,

    // --- Quiescence (ADR 0044 phase-1 PR3). `None` (the default, set by every
    // constructor) is byte-identical to pre-PR3 behavior: the entry predicate
    // in `tick` is only ever evaluated when `Some`, so the control plane
    // (which never calls `enable_quiescence`, fork G) is untouched. ---
    // Opt-in idle threshold; `enable_quiescence` sets it.
    quiesce_after: Option<Duration>,
    // Whether this node currently considers itself quiesced — both roles
    // participate: a leader sets it on satisfying the entry predicate and
    // broadcasting `Quiesce`; a follower sets it on *accepting* one. Volatile,
    // never persisted or snapshotted (like `last_contact`/`match_index`) — a
    // restart always starts ticking normally and re-derives this from scratch.
    quiesced: bool,
    // The `now` of the last event that should reset the leader's idle clock —
    // bumped whenever `commit_index` advances (`maybe_advance_commit`, which
    // captures every local propose/config-change/transfer-driven commit) and
    // on `become_leader` (a fresh term starts its own clock). The entry
    // predicate's "no activity for `quiesce_after`" clause is `now.0 -
    // last_activity.0 >= quiesce_after`.
    last_activity: Nanos,
    // External input (ADR 0044 phase-1 PR3 design sketch): whether the async
    // apply task's engine state has caught up to `last_applied` — the core
    // itself has no visibility into engine I/O, so the `DRIVER_APPLIED`
    // driver feeds this in via `set_quiesce_engine_caught_up` once per loop
    // iteration. Defaults `true` (harmless: only ever consulted when
    // `quiesce_after` is `Some`, which no caller sets without also driving
    // this).
    quiesce_engine_caught_up: bool,
    // External input (fork D): an always-false placeholder in this PR — no
    // subsystem holds it yet (that's a later PR, e.g. the txn tracker/change-
    // log sweeper). Introduced now so the entry predicate's shape is final
    // and a later PR only needs to *set* this via `set_quiesce_veto`, not
    // restructure the predicate.
    quiesce_veto: bool,
    // Freshness stamp for `quiesce_veto` (issue #302 fix): the log index an
    // external veto holder's *observation* of its own obligation state is
    // valid through, in the SAME index space as `commit_index`/
    // `last_applied` (for the `DRIVER_APPLIED` KV state machine these are
    // literally the engine's own applied-index counter — see
    // `RaftKvNode::engine_applied_index`'s doc). `set_quiesce_veto` is fed
    // once per driver-loop iteration, but its *content* can lag: an
    // external sweeper (`animusd`'s `change_consumer_loop`) only re-examines
    // a tablet's own obligation state (e.g. its change log) once every
    // `INDEX_DRAIN_INTERVAL`, not every driver iteration, so a `false` veto
    // can describe a state that a write committed *after* the sweep has
    // since falsified. `quiesce_entry_ok` additionally requires
    // `quiesce_veto_fresh_through >= commit_index` — i.e. no entry has
    // committed since the last real observation — closing that staleness
    // window exactly rather than by a timing margin. Defaults `u64::MAX`
    // (deliberately, not `0`): a subsystem that never calls
    // `set_quiesce_veto` for a given tablet at all (e.g. a `Building` split
    // child, which structurally can never accumulate a change-log
    // obligation — see `index_drain.rs`'s own doc) must behave exactly as
    // before this fix: no freshness requirement, matching the pre-fix
    // always-`false`/always-fine veto for that class of tablet. Only a
    // caller that has *actually* set the veto at least once narrows this
    // below `u64::MAX`, and only real per-tablet log-index values ever flow
    // through — this sentinel is never observed as "current" by a tablet an
    // external sweeper is genuinely responsible for, as long as that
    // sweeper's own cadence is no slower than `quiesce_after` (see
    // `animusd`'s `--quiesce-after` validation).
    quiesce_veto_fresh_through: u64,
}

impl<C, S> RaftCore<C, S>
where
    C: Clone + std::fmt::Debug + Serialize + DeserializeOwned,
    S: StateMachine<C>,
{
    /// Create a follower. `all_nodes` is the full membership (including `id`).
    pub fn new(id: NodeId, all_nodes: &[NodeId], now: Nanos, entropy: u64) -> Self {
        let peers: Vec<NodeId> = all_nodes.iter().filter(|n| **n != id).cloned().collect();
        let cluster_size = all_nodes.len();
        let initial_config: BTreeSet<NodeId> = all_nodes.iter().cloned().collect();
        let mut core = Self {
            id,
            peers,
            cluster_size,
            config: initial_config.clone(),
            initial_config,
            snapshot_config: None,
            learners: BTreeSet::new(),
            initial_learners: BTreeSet::new(),
            snapshot_learners: None,
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
            last_leader_contact: None,
            pre_votes: BTreeSet::new(),
            votes: BTreeSet::new(),
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            snapshot_served_through: BTreeMap::new(),
            last_contact: BTreeMap::new(),
            departing: BTreeMap::new(),
            transfer_target: None,
            transfer_deadline: Nanos(0),
            first_term_index: 0,
            snapshot_offset: BTreeMap::new(),
            snapshot_chunk_sent: BTreeMap::new(),
            snapshot_chunk_advances: BTreeMap::new(),
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
            snapshot_needed: false,
            state_machine_behind: false,
            pending: Vec::new(),
            persisted_hard: (0, None),
            snapshot_dirty: false,
            quiesce_after: None,
            quiesced: false,
            last_activity: now,
            quiesce_engine_caught_up: true,
            quiesce_veto: false,
            quiesce_veto_fresh_through: u64::MAX,
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
            // core's image lives in the engine, not `metadata`; its driver builds it
            // lazily on demand — `take_snapshot_needed` — so leave it None here.)
            if !S::DRIVER_APPLIED {
                core.snapshot_blob =
                    Some(serde_json::to_vec(&core.metadata).expect("metadata serializes"));
            }
        }
        // Restore the voter configuration: the snapshot's recorded config (if any)
        // is the base, and the recovered log tail's latest config entry (if any)
        // overrides it — `recompute_config` applies that precedence (ADR 0017 C).
        core.snapshot_config = persisted.snapshot_config;
        core.snapshot_learners = persisted.snapshot_learners;
        core.recompute_config();
        // Everything restored from the WAL/snapshot is by definition durable, so
        // the durable watermark covers the whole recovered log. The tail re-applies
        // (durable-gated, a no-op gate) once commit re-advances post-recovery.
        core.durable_index = core.last_log_index();
        // Already durable: do not re-emit it.
        core.persisted_hard = (core.current_term, core.voted_for.clone());
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

    /// Whether a [`drain_persist`](Self::drain_persist) right now would yield
    /// anything — a **read-only peek**, mirroring
    /// [`has_pending_install`](Self::has_pending_install)'s peek-not-drain
    /// discipline (a driver that decides off this must not consume the state it
    /// is deciding about).
    ///
    /// Both halves matter, and the second is easy to miss: log entries land in
    /// `pending` eagerly at append time, but a **term/vote change is captured
    /// lazily**, by `checkpoint_hard` *inside* `drain_persist` — so a node that
    /// has just granted a vote and appended nothing has an empty `pending` and
    /// is nonetheless un-persisted. A driver that races persistence against its
    /// own message loop (`animus-cp-data`'s consensus loop, issue #279) uses
    /// this to decide whether the step it just took still owes the WAL
    /// anything, and must therefore see the vote.
    pub fn has_unflushed_wal(&self) -> bool {
        !self.pending.is_empty()
            || (self.current_term, self.voted_for.clone()) != self.persisted_hard
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
                learners: self.snapshot_learners.clone(),
            });
        }
        image.push(WalRecord::Hard {
            term: self.current_term,
            voted_for: self.voted_for.clone(),
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
        self.snapshot_learners = Some(self.learners_at(new_index));
        self.log.retain(|e| e.index > new_index);
        // Drop the retained applied-command history the snapshot now covers,
        // mirroring the log truncation (and the clear `InstallSnapshot` already
        // does). `applied` exists for tests / divergence checks over the
        // *uncompacted* window; without this it grows unboundedly in production —
        // one command per commit, forever (the commit-path memory leak). The tail
        // beyond `new_index` (the last `last_applied - new_index` commands) is
        // kept so the retention window matches the retained log.
        let covered = self
            .applied
            .len()
            .saturating_sub((self.last_applied - new_index) as usize);
        self.applied.drain(..covered);
        self.snapshot_index = new_index;
        self.snapshot_term = new_term;
        self.snapshot_dirty = true;
        // Cache the serialized snapshot image so [`snapshot_chunk_for`] slices cached
        // bytes instead of re-serializing the whole `metadata` **per 1KB chunk** — an
        // O(state)-per-`InstallSnapshot`-message cost that pins the consensus loop and
        // storms elections while catching a follower up on a large state (the
        // control-plane counterpart of the CP-data driver-liveness fix, ADR 0017). An
        // in-core SM's image *is* its `metadata`, which reflects `last_applied`
        // (`new_index <= last_applied`), so this serializes state at least as fresh
        // as the base — and the control plane only ever snapshots to `last_applied`
        // (via [`snapshot`]), so it matches the base exactly, keeping the in-core
        // invariant `snapshot_index > 0 ⟹ snapshot_blob.is_some()`.
        if !S::DRIVER_APPLIED {
            self.snapshot_blob =
                Some(serde_json::to_vec(&self.metadata).expect("metadata serializes"));
        } else {
            // `DRIVER_APPLIED` images are built **lazily, on demand** (see
            // [`snapshot_chunk_for`]): the base just moved, so any previously
            // materialized image is stale — shipping state-at-the-old-base
            // labeled with the new `snapshot_index` would corrupt a receiver.
            // Drop it (regenerated from the engine only if a follower actually
            // needs one) and restart any in-flight transfer from offset 0
            // against the next image (the receiver's `fresh && offset == 0`
            // reassembly guard requires a restart — resuming a differently-based
            // transfer mid-offset would never complete). The on-demand build
            // path calls `set_snapshot_blob` *after* this, in the same driver
            // pass, so a deliberately fresh image is never dropped.
            self.snapshot_blob = None;
            self.snapshot_offset.clear();
            self.snapshot_chunk_sent.clear();
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

    /// Whether a chunked `InstallSnapshot` transfer is currently in flight
    /// to at least one peer (a non-empty `snapshot_offset` — see that
    /// field's own doc). `snapshot_upto` unconditionally invalidates every
    /// in-flight transfer's own progress the moment the base moves again
    /// (dropping the blob and clearing every peer's offset — required for
    /// correctness, since the in-flight bytes were captured at the OLD
    /// base and shipping them under a new `snapshot_index` would corrupt
    /// the receiver). Under a sustained write stream that keeps
    /// re-crossing a `DRIVER_APPLIED` driver's compaction threshold faster
    /// than a lagging peer's own chunked transfer can complete, that
    /// invalidation can repeat forever, so the peer's catch-up never
    /// finishes (issues #532/#537's own residual finding beyond the
    /// `MAX_APPEND_ENTRIES_BATCH` cap — see that constant's doc). This
    /// accessor is the fact a `DRIVER_APPLIED` driver's own
    /// threshold-triggered compaction check needs to defer advancing the
    /// base while an in-flight transfer still has a chance to land —
    /// policy lives entirely in the driver (`animus-cp-data`'s
    /// `apply_and_compact`), never here; this core stays a pure fact,
    /// same as `snapshot_index` itself.
    pub fn snapshot_transfer_in_flight(&self) -> bool {
        !self.snapshot_offset.is_empty()
    }

    /// The byte offset `peer` has acked so far in an in-flight chunked
    /// `InstallSnapshot` transfer (`None` if no transfer to `peer` is in
    /// flight) — a pure read of [`snapshot_offset`](Self::snapshot_offset),
    /// mirroring [`snapshot_transfer_in_flight`](Self::snapshot_transfer_in_flight)'s
    /// "policy lives in the driver, this core stays a fact" shape. Exists
    /// primarily so a test can observe transfer PROGRESS deterministically
    /// (distinct offsets reached over a run) rather than only volume — see
    /// `animus-cp-data/tests/snapshot_resend_bound.rs`.
    pub fn snapshot_chunk_progress(&self, peer: &NodeId) -> Option<u64> {
        self.snapshot_offset.get(peer).copied()
    }

    /// Lifetime count of GENUINE `InstallSnapshot` chunk advances shipped to
    /// `peer` (`0` if none yet) — see
    /// [`snapshot_chunk_advances`](Self::snapshot_chunk_advances)'s own doc
    /// for exactly what counts and why it exists.
    pub fn snapshot_chunk_advances(&self, peer: &NodeId) -> u64 {
        self.snapshot_chunk_advances.get(peer).copied().unwrap_or(0)
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
    ///
    /// **This is `leader_id`, the raw consensus-internal belief** — it is
    /// cleared the instant this node's own election timer lapses
    /// (`start_pre_vote`) or it starts campaigning (`start_election`), which
    /// is exactly right for consensus (a stale belief must never be trusted
    /// for granting votes or picking a relay target) but gives an
    /// operational reader (a readiness/health probe) a false-negative
    /// window on every transient one-sided delay of one election timeout or
    /// more, even while the real leader stays fully healthy the whole time
    /// (issue #595). **A health/readiness consumer should call
    /// [`leader_within`](Self::leader_within) instead** — see its own doc.
    pub fn leader(&self) -> Option<NodeId> {
        self.leader_id.clone()
    }

    /// A hysteresis-bearing alternative to [`leader`](Self::leader) for an
    /// **operational** reader (issue #595) — never for consensus, pre-vote,
    /// election, or replication logic, which must keep consulting
    /// `leader_id`/`election_deadline` exclusively.
    ///
    /// Returns the last leader this node had a GENUINE contact with
    /// (`last_leader_contact`), as long as that contact is no older than
    /// `max_age`; `None` once it is stale or if there has never been one.
    /// This survives exactly the false-negative window `leader()` cannot: a
    /// follower whose own election timer lapsed (clearing `leader_id`)
    /// because it stopped hearing from an otherwise-healthy leader still
    /// reports that leader here, right up until `max_age` genuinely
    /// elapses since the last real `AppendEntries`/`InstallSnapshot` (or,
    /// for this node itself, the moment it became leader). A caller that
    /// wants "believe it for roughly N election timeouts past the last
    /// heartbeat" passes `max_age` sized accordingly (`animusd::admin::
    /// health`'s `HEALTH_LEADER_GRACE` is the reference use).
    pub fn leader_within(&self, now: Nanos, max_age: Duration) -> Option<NodeId> {
        let (id, seen_at) = self.last_leader_contact.as_ref()?;
        if now.duration_since(*seen_at) <= max_age {
            Some(id.clone())
        } else {
            None
        }
    }

    /// Highest committed log index.
    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    /// While leader, the log index of the **first entry this node appended in its
    /// current term** — the election no-op from `become_leader`; `None` off-leader.
    ///
    /// Raft §6.4 (and the membership-change erratum): a freshly elected leader's
    /// log contains every committed entry (leader completeness), but its
    /// `commit_index` may still lag entries the *previous* leader committed and
    /// acked, because the commit rule never counts old-term entries toward a
    /// majority. Only once `commit_index() >= first_term_index()` is the leader's
    /// commit index guaranteed to cover everything previously acked — the gate a
    /// ReadIndex barrier and a membership change must clear before acting.
    pub fn first_term_index(&self) -> Option<u64> {
        (self.role == Role::Leader).then_some(self.first_term_index)
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

    /// The commands applied **since the last snapshot**, in order — a bounded
    /// window for tests / divergence checks, not the full history. The prefix a
    /// snapshot covers is dropped alongside the log truncation
    /// ([`snapshot_upto`](Self::snapshot_upto)) and on `InstallSnapshot`, so this
    /// does not grow unboundedly in production.
    pub fn applied(&self) -> Vec<C> {
        self.applied.clone()
    }

    /// Take the committed-and-durable commands a `DRIVER_APPLIED` state machine has
    /// not yet handed to its async driver, as `(index, term, command)` in commit
    /// order — `term` is the entry's own leader term (`LogEntry::term`), the same
    /// value a proposer's own `ProposeResult::Accepted` carried, so a driver-side
    /// outcome channel keyed by `index` can also record `term` and let a proposer
    /// tell "this is genuinely my entry" from "a different entry now occupies my
    /// old index" (see `ProposeResult::Accepted`'s doc). **The driver applies each
    /// to the real engine (in order) and is the only consumer.** Always empty for
    /// the in-core control plane (which applies in `apply` instead). ADR 0017.
    pub fn drain_apply(&mut self) -> Vec<(u64, u64, C)> {
        std::mem::take(&mut self.pending_apply)
    }

    /// Provide the engine-image bytes a `DRIVER_APPLIED` leader ships to a lagging
    /// follower via `InstallSnapshot` (ADR 0017 A.2). Built **lazily**: the driver
    /// supplies this only when a replication attempt raised
    /// [`take_snapshot_needed`](Self::take_snapshot_needed) (a follower actually
    /// needs a snapshot), scanning the `StorageEngine` at that moment and calling
    /// `snapshot_upto(engine_applied)` *first* so the image and the base agree.
    /// The core drops it again once no transfer is in flight (or the base moves),
    /// so no whole-tablet image is retained at rest. No effect for an in-core
    /// state machine (which caches `serialize(metadata)` eagerly).
    pub fn set_snapshot_blob(&mut self, bytes: Vec<u8>) {
        self.snapshot_blob = Some(bytes);
    }

    /// Take a fully-received snapshot's `(last_index, engine-image bytes)` for the
    /// driver to write into the engine (a `DRIVER_APPLIED` follower catching up).
    /// `None` when no install is pending.
    pub fn drain_pending_install(&mut self) -> Option<(u64, Vec<u8>)> {
        self.pending_install.take()
    }

    /// Whether a fully-received snapshot is waiting for
    /// [`drain_pending_install`](Self::drain_pending_install) — a read-only peek a
    /// `DRIVER_APPLIED` consensus loop can use to notice "apply work now exists"
    /// (ADR 0044 phase-1 PR1) without taking it, since only the apply task may
    /// actually drain it.
    pub fn has_pending_install(&self) -> bool {
        self.pending_install.is_some()
    }

    /// The next virtual instant at which this node wants a timer tick, or
    /// `None` if it wants no timer at all right now.
    ///
    /// `None` means **quiescence** (ADR 0044 phase-1 PR3, gated by opt-in
    /// [`enable_quiescence`](Self::enable_quiescence)): a quiesced node —
    /// leader or follower — has nothing to time out on until some other event
    /// (an inbound message, a local propose, `shutdown()`, an explicit wake)
    /// un-quiesces it. Both drivers drop the timer arm from their `select` on
    /// `None`, so a quiesced group posts zero `SimEnv` timeline events.
    /// `quiesce_after` defaults `None` (nothing calls `enable_quiescence`
    /// without opting in — the control plane never does, fork G), so this is
    /// byte-identical to pre-PR3 behavior unless a caller opts in.
    pub fn next_deadline(&self) -> Option<Nanos> {
        if self.quiesced {
            return None;
        }
        if self.role == Role::Leader {
            // While a transfer is armed, also wake in time to evaluate its abort
            // deadline (`tick`) even if that falls before the next heartbeat —
            // in practice the heartbeat interval is far shorter than one election
            // timeout, so this rarely changes the wait, but it keeps the bound
            // exact rather than incidental.
            match self.transfer_target {
                Some(_) => Some(Nanos(
                    self.heartbeat_deadline.0.min(self.transfer_deadline.0),
                )),
                None => Some(self.heartbeat_deadline),
            }
        } else {
            Some(self.election_deadline)
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

    /// Whether this node is a **learner** in the active configuration (ADR
    /// 0058 Train 1) — mutually exclusive with [`is_voter`](Self::is_voter):
    /// membership-change proposals (`change_membership`/`add_learner`/
    /// `promote_learner`) keep `config`/`learners` disjoint by construction.
    #[must_use]
    pub fn is_learner(&self) -> bool {
        self.learners.contains(&self.id)
    }

    /// The active voter configuration.
    #[must_use]
    pub fn config(&self) -> BTreeSet<NodeId> {
        self.config.clone()
    }

    /// The active **learner** configuration (ADR 0058 Train 1) — non-voting
    /// members that receive replication but never count toward quorum. Always
    /// disjoint from [`config`](Self::config).
    #[must_use]
    pub fn learners(&self) -> BTreeSet<NodeId> {
        self.learners.clone()
    }

    /// `id`'s role in the active configuration, or `None` if it is not
    /// currently a member at all (ADR 0058 Train 1).
    #[must_use]
    pub fn member_role(&self, id: &NodeId) -> Option<MemberRole> {
        if self.config.contains(id) {
            Some(MemberRole::Voter)
        } else if self.learners.contains(id) {
            Some(MemberRole::Learner)
        } else {
            None
        }
    }

    /// Whether learner `id` is caught up closely enough to this leader's own
    /// log to be a promotion candidate (ADR 0058 Train 1's promotion
    /// criterion): its tracked `match_index` is within `threshold` of
    /// [`last_log_index`](Self::last_log_index). A pure predicate over
    /// already-tracked state (the same bookkeeping `AppendEntries`/
    /// `InstallSnapshot` acks already maintain) — it does **not** gate
    /// [`promote_learner`](Self::promote_learner) itself; a later layer (the
    /// host reconciler) decides *when* to act on it. `false` for any `id`
    /// that is not currently a learner.
    #[must_use]
    pub fn learner_caught_up(&self, id: &NodeId, threshold: u64) -> bool {
        self.learners.contains(id)
            && self.last_log_index().saturating_sub(self.peer_match(id)) <= threshold
    }

    /// Adopt `voters`/`learners` as the active config and keep
    /// `peers`/`cluster_size` in sync, so every quorum/replication/election
    /// decision reflects it immediately. **`peers`/`cluster_size` are derived
    /// from `voters` alone** (ADR 0058 Train 1) — a learner is never counted
    /// toward `cluster_size` (hence never toward `majority()`), and is never
    /// added to `peers` (the set `start_election`/`start_pre_vote` solicit and
    /// `maybe_advance_commit` tallies) — it is replicated to via the separate
    /// `learners` union in `broadcast_append`/`become_leader`/
    /// `broadcast_quiesce`/`quiesce_entry_ok` instead. This is what keeps
    /// every existing quorum-computation call site correct with **zero**
    /// changes: they were already voter-only before learners existed, and
    /// stay voter-only now.
    fn apply_config(&mut self, voters: BTreeSet<NodeId>, learners: BTreeSet<NodeId>) {
        self.peers = voters.iter().filter(|n| **n != self.id).cloned().collect();
        self.cluster_size = voters.len();
        self.config = voters;
        self.learners = learners;
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

    /// The learner config effective at log `index` (ADR 0058 Train 1),
    /// mirroring [`config_at`](Self::config_at) exactly — gated on the same
    /// `e.config.is_some()` test, since every membership-change entry carries
    /// both sets together (see `LogEntry::learners`'s doc).
    fn learners_at(&self, index: u64) -> BTreeSet<NodeId> {
        self.log
            .iter()
            .rev()
            .find(|e| e.index <= index && e.config.is_some())
            .and_then(|e| e.learners.clone())
            .or_else(|| self.snapshot_learners.clone())
            .unwrap_or_else(|| self.initial_learners.clone())
    }

    /// Recompute the active config from the current log tail (used after a
    /// truncation, which may have removed a config entry, or on recovery).
    fn recompute_config(&mut self) {
        let voters = self.config_at(self.last_log_index());
        let learners = self.learners_at(self.last_log_index());
        self.apply_config(voters, learners);
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
    /// majorities — multi-server needs joint consensus, deferred), if it would
    /// remove the current leader (transfer leadership first), or if a leadership
    /// transfer is currently armed (see [`transfer_leadership`](Self::transfer_leadership) —
    /// the log must stop growing while a transfer is in flight so replication can
    /// catch the target up to `last_log_index`).
    pub fn change_membership(&mut self, voters: BTreeSet<NodeId>) -> ProposeResult {
        if self.role != Role::Leader {
            return ProposeResult::NotLeader {
                leader: self.leader_id.clone(),
            };
        }
        if let Some(target) = self.transfer_target.clone() {
            return ProposeResult::NotLeader {
                leader: Some(target),
            };
        }
        // The membership-change erratum guard (Raft §4 / Ongaro's bug report):
        // append no config entry until this leader has **committed an entry in
        // its current term** (its election no-op). Before that point the leader's
        // `commit_index` may lag entries a prior leader committed — in particular
        // an earlier *config* entry could still be uncommitted from this leader's
        // view, so `config_change_in_flight` (which compares against the honest
        // `commit_index`) would already hold it off; this explicit gate replaces
        // that subtle composed argument with the standard, self-evident rule.
        // Rejected NotLeader-style (self hint) like the other no-op rejections; a
        // caller (e.g. the reconfigure loop) simply retries after the no-op
        // commits — one round trip after election.
        if self.commit_index < self.first_term_index {
            return ProposeResult::NotLeader {
                leader: Some(self.id.clone()),
            };
        }
        let delta = self.config.symmetric_difference(&voters).count();
        if delta != 1
            || self.config_change_in_flight()
            || !voters.contains(&self.id)
            // ADR 0058 Train 1: this method only ever moves a member into or
            // out of the *voter* set; a node currently tracked as a learner
            // must go through `promote_learner` instead (which explicitly
            // moves it out of `learners`, keeping the two sets disjoint by
            // construction). Without this guard, adding a current learner's
            // id here would silently make it a voter while leaving it in
            // `learners` too — an ambiguous, ill-defined membership state.
            || !voters.is_disjoint(&self.learners)
        {
            // No-op rejection (a self-removal / multi-server / in-flight change
            // / learner-ambiguous delta): report not-accepted by returning the
            // leader hint. (`delta == 0` is also rejected — nothing to change.)
            return ProposeResult::NotLeader {
                leader: Some(self.id.clone()),
            };
        }
        let index = self.last_log_index() + 1;
        self.log_append(LogEntry {
            term: self.current_term,
            index,
            command: S::noop(),
            config: Some(voters),
            learners: Some(self.learners.clone()),
        });
        self.maybe_advance_commit();
        self.apply();
        ProposeResult::Accepted {
            index,
            term: self.current_term,
        }
    }

    /// Leader-only pre-flight guard shared by
    /// [`add_learner`](Self::add_learner)/[`promote_learner`](Self::promote_learner)/
    /// [`remove_learner`](Self::remove_learner): not leader, a transfer armed,
    /// the current-term-commit erratum gate, or a change already in flight —
    /// the identical discipline [`change_membership`](Self::change_membership)
    /// enforces (see its doc for the rationale of each clause).
    fn learner_change_precheck(&self) -> Option<ProposeResult> {
        if self.role != Role::Leader {
            return Some(ProposeResult::NotLeader {
                leader: self.leader_id.clone(),
            });
        }
        if let Some(target) = self.transfer_target.clone() {
            return Some(ProposeResult::NotLeader {
                leader: Some(target),
            });
        }
        if self.commit_index < self.first_term_index {
            return Some(ProposeResult::NotLeader {
                leader: Some(self.id.clone()),
            });
        }
        if self.config_change_in_flight() {
            return Some(ProposeResult::NotLeader {
                leader: Some(self.id.clone()),
            });
        }
        None
    }

    /// Append a single-server membership-change entry carrying `voters`/
    /// `learners` together (the shared tail of every ADR 0058 Train 1
    /// transition method, once its own precondition already holds).
    fn append_membership_entry(
        &mut self,
        voters: BTreeSet<NodeId>,
        learners: BTreeSet<NodeId>,
    ) -> ProposeResult {
        let index = self.last_log_index() + 1;
        self.log_append(LogEntry {
            term: self.current_term,
            index,
            command: S::noop(),
            config: Some(voters),
            learners: Some(learners),
        });
        self.maybe_advance_commit();
        self.apply();
        ProposeResult::Accepted {
            index,
            term: self.current_term,
        }
    }

    /// Add `id` as a **learner** (ADR 0058 Train 1): a new, non-voting member
    /// that receives `AppendEntries`/`InstallSnapshot` exactly like a voter
    /// (its `match_index` is tracked the same way, for
    /// [`learner_caught_up`](Self::learner_caught_up)) but is excluded from
    /// every quorum computation and never campaigns
    /// (`start_election`/`start_pre_vote` gate on `is_voter`). Leader-only;
    /// rejected under the same single-in-flight-change discipline
    /// [`change_membership`](Self::change_membership) uses, or if `id` is
    /// already a member (voter or learner) of this group, or is this leader's
    /// own id (a leader is always a voter — see `become_leader`/
    /// `start_election`'s `is_voter` gate — so it can never sensibly add
    /// itself as a learner).
    pub fn add_learner(&mut self, id: NodeId) -> ProposeResult {
        if let Some(rejected) = self.learner_change_precheck() {
            return rejected;
        }
        if id == self.id || self.config.contains(&id) || self.learners.contains(&id) {
            return ProposeResult::NotLeader {
                leader: Some(self.id.clone()),
            };
        }
        let mut new_learners = self.learners.clone();
        new_learners.insert(id);
        self.append_membership_entry(self.config.clone(), new_learners)
    }

    /// Promote learner `id` to **voter** (ADR 0058 Train 1) — the reachable
    /// transition a caller takes once [`learner_caught_up`](Self::learner_caught_up)
    /// (or an equivalent external judgment) says `id` is ready: it moves from
    /// `learners` into `config` in a single committed configuration entry,
    /// identical in shape to any other single-server change
    /// ([`change_membership`](Self::change_membership)'s doc, ADR 0017 Stage
    /// C) — this PR ships the primitive only; the *decision* of when to call
    /// it (the host reconciler's replica-move sequencing) is a later layer.
    /// Leader-only; rejected under the same discipline as
    /// [`add_learner`](Self::add_learner), or if `id` is not currently a
    /// learner.
    pub fn promote_learner(&mut self, id: NodeId) -> ProposeResult {
        if let Some(rejected) = self.learner_change_precheck() {
            return rejected;
        }
        if !self.learners.contains(&id) {
            return ProposeResult::NotLeader {
                leader: Some(self.id.clone()),
            };
        }
        let mut new_voters = self.config.clone();
        new_voters.insert(id.clone());
        let mut new_learners = self.learners.clone();
        new_learners.remove(&id);
        self.append_membership_entry(new_voters, new_learners)
    }

    /// Remove learner `id` without promoting it (ADR 0058 Train 1) — the
    /// "demote/remove" case: a learner that fails to catch up, or is no
    /// longer wanted, is dropped directly rather than ever becoming a voter.
    /// Leader-only; rejected under the same discipline as
    /// [`add_learner`](Self::add_learner), or if `id` is not currently a
    /// learner. (Removing a **voter** stays `change_membership`'s job — this
    /// method only ever touches `learners`, never `config`.)
    pub fn remove_learner(&mut self, id: NodeId) -> ProposeResult {
        if let Some(rejected) = self.learner_change_precheck() {
            return rejected;
        }
        if !self.learners.contains(&id) {
            return ProposeResult::NotLeader {
                leader: Some(self.id.clone()),
            };
        }
        let mut new_learners = self.learners.clone();
        new_learners.remove(&id);
        self.append_membership_entry(self.config.clone(), new_learners)
    }

    /// The leader's last-known replicated log index for `node` (0 if unknown —
    /// e.g. not currently a peer). The caught-up primitive: a caller comparing
    /// this against [`commit_index`](Self::commit_index) or
    /// [`last_log_index`](Self::last_log_index) can tell whether `node` has
    /// actually received everything before, say, removing a different voter out
    /// from under it.
    #[must_use]
    pub fn peer_match(&self, node: &NodeId) -> u64 {
        self.match_index.get(node).copied().unwrap_or(0)
    }

    /// The `now` at which this leader last heard an `AppendEntriesResp` (success
    /// or reject) from `node`, or `None` if it never has (either `node` isn't a
    /// peer, or this leadership stint hasn't heard from it yet — see the
    /// `last_contact` field doc for why that second case isn't back-filled).
    /// A raw fact with **no policy baked in** — deciding "alive" from it (a
    /// timeout, a grace period for the never-contacted case) is
    /// `RaftNode::control_peer_believed_alive`'s job, not this core's.
    #[must_use]
    pub fn peer_last_contact(&self, node: NodeId) -> Option<Nanos> {
        self.last_contact.get(&node).copied()
    }

    /// The voter this leader is currently handing leadership off to, if a
    /// transfer is armed (see [`transfer_leadership`](Self::transfer_leadership))
    /// — `None` on every other node (a transfer is leader-local state, never
    /// replicated) and on a leader with none in flight. Introspection only
    /// (`/admin/raft`, `RaftNode`'s own driver-side abort observability,
    /// issue #313) — nothing in this core reads it back through this method.
    #[must_use]
    pub fn transfer_target(&self) -> Option<NodeId> {
        self.transfer_target.clone()
    }

    /// Arm a leadership transfer to `target` (Raft §3.10): once armed,
    /// `propose`/`change_membership` freeze (report `NotLeader`) so the log stops
    /// growing, and `broadcast_append` sends `target` a [`RaftMsg::TimeoutNow`]
    /// (see the `transfer_target` field doc) **once it reaches
    /// `last_log_index()`** — re-sent every heartbeat after that until this node
    /// steps down, resilient to a single dropped message. Like
    /// `change_membership`, this is something a caller outside the driver loop
    /// can trigger and then wake the loop to send promptly
    /// (`propose_and_wake`'s pattern) rather than a method that hands back
    /// messages to deliver itself. Returns whether the transfer is armed (true
    /// both for a fresh arm and for an idempotent re-arm of the same target).
    ///
    /// Leader-only; rejected (no state change) unless `target` is a
    /// **different**, current voter reasonably close to caught up
    /// (`peer_match(target) >= commit_index()`) and no config change is in
    /// flight — a transfer to a voter that hasn't even seen the committed
    /// prefix could stall the group with no leader able to make progress. The
    /// gate is intentionally looser than "`== last_log_index()`": under
    /// sustained writes `last_log_index` can run ahead of any single sampling
    /// instant forever, which would make the arm gate itself unsatisfiable — the
    /// proposal freeze this method also imposes is what lets replication finish
    /// closing that gap (to equality) *after* arming, before `TimeoutNow` is
    /// actually sent (see `broadcast_append`). This node's own term/role/
    /// leader_id are **not** touched here: the actual handoff happens when
    /// `target` wins the resulting election and its higher term reaches this
    /// node through the normal step-down path.
    ///
    /// A **re-arm of the same already-armed target does not push the deadline
    /// out** — only a fresh arm (first time, or a different target) starts a
    /// new one election-timeout window (see `transfer_deadline`). This matters
    /// because a caller like `RaftKvNode::reconfigure_step` calls this once per
    /// tick as long as the delta persists (documented as idempotent): if every
    /// call reset the deadline, a target that never actually catches up could
    /// keep the transfer armed (and proposals frozen) forever, since the
    /// deadline would always be "one tick away" from expiring.
    ///
    /// This is what makes it possible to move the *leader's own* replica in a
    /// membership change: [`change_membership`](Self::change_membership) always
    /// rejects removing the leader, so a caller that needs to do so (e.g. a
    /// rebalance move landing on the current leader) transfers leadership to
    /// another member of the target configuration first; that new leader then
    /// removes the old one itself, which is an ordinary (non-self) removal.
    pub fn transfer_leadership(&mut self, target: NodeId, now: Nanos) -> bool {
        if self.role != Role::Leader
            || target == self.id
            || !self.config.contains(&target)
            || self.config_change_in_flight()
            || self.peer_match(&target) < self.commit_index
        {
            return false;
        }
        if self.transfer_target != Some(target.clone()) {
            self.transfer_target = Some(target);
            self.transfer_deadline =
                Nanos(now.0.saturating_add(self.election_base.as_nanos() as u64));
        }
        // ADR 0044 phase-1 PR3, un-quiesce trigger (b): arming (or idempotently
        // re-arming) a transfer is local leader activity. `quiesce_entry_ok`'s
        // own `transfer_target.is_none()` clause already blocks entry while
        // armed, so this mostly matters for the settle window *after* it
        // clears (an aborted or completed transfer shouldn't let a leader that
        // was mid-handoff moments ago quiesce immediately on its very next
        // tick).
        self.quiesced = false;
        self.last_activity = now;
        true
    }

    /// The current election-timeout base (the low end of the randomized
    /// `[base, 2*base)` range a follower's real timeout is drawn from) — also
    /// the un-randomized budget [`transfer_leadership`](Self::transfer_leadership)
    /// arms its own deadline with. Introspection only (driver-side
    /// observability logs the budget a transfer had to fit in).
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
            let old_peers = self.peers.clone();
            // Every membership-change entry carries both sets together (see
            // `LogEntry::learners`'s doc); fall back to the current learners
            // only as defensive robustness against a decoded entry that
            // predates this field (`#[serde(default)]` ⇒ `None`), which must
            // never wipe out an already-known learner set.
            let learners = entry
                .learners
                .clone()
                .unwrap_or_else(|| self.learners.clone());
            self.apply_config(voters.clone(), learners);
            // Leader-only bookkeeping: a peer this entry just dropped from `peers`
            // must still be told, so track it as departing until it acks past this
            // entry's index (see the `departing` field doc). A peer this entry
            // brought back is no longer departing.
            if self.role == Role::Leader {
                for removed in old_peers.iter().filter(|n| !self.peers.contains(n)) {
                    self.departing.insert(removed.clone(), entry.index);
                }
            }
            self.departing.retain(|n, _| !self.peers.contains(n));
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
        let hard = (self.current_term, self.voted_for.clone());
        if hard != self.persisted_hard {
            self.persisted_hard = hard.clone();
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
                // Abort a leadership transfer whose target has not stepped down
                // by the deadline (Raft §3.10) — e.g. it crashed after arming, or
                // never re-caught-up to `last_log_index` to receive `TimeoutNow`
                // (see `broadcast_append`). This resumes proposing immediately
                // (the very next `propose`/`change_membership` call), rather than
                // stranding the group frozen forever. Checked on every tick, not
                // only a heartbeat tick, so the abort isn't delayed by the
                // (usually much shorter) heartbeat cadence.
                if self.transfer_target.is_some() && now.0 >= self.transfer_deadline.0 {
                    self.transfer_target = None;
                }
                if now.0 >= self.heartbeat_deadline.0 {
                    // ADR 0044 phase-1 PR3: at the point this leader would otherwise
                    // send a routine heartbeat, check whether it can quiesce instead
                    // (`quiesce_entry_ok`'s doc has the full predicate). Only
                    // evaluated once per idle settle — `!self.quiesced` guards it, so
                    // an already-quiesced leader never gets here at all (its own
                    // `next_deadline` is `None`, so the driver never calls `tick` via
                    // its timer arm in the first place; this guard is a second,
                    // redundant-but-cheap line of defense).
                    if !self.quiesced && self.quiesce_entry_ok(now) {
                        self.quiesced = true;
                        return self.broadcast_quiesce();
                    }
                    self.heartbeat_deadline = Nanos(now.0.saturating_add(self.heartbeat_nanos()));
                    // Issue #595: a routine heartbeat broadcast is this
                    // leader's own proof-of-life to itself, refreshed every
                    // `heartbeat_interval` (50ms) for as long as it leads —
                    // without this, `last_leader_contact` would stay
                    // pinned at the ORIGINAL `become_leader` timestamp
                    // forever, and a long-lived, perfectly healthy leader
                    // would spuriously fail its own `leader_within` check
                    // (and thus its own `/admin/health`) a few election
                    // timeouts after it won, despite having led
                    // continuously and healthily the entire time. See
                    // `last_leader_contact`'s own doc.
                    self.last_leader_contact = Some((self.id.clone(), now));
                    // Heartbeat cadence (write-rate-independent) is one of
                    // the bounded retries a genuinely stuck snapshot chunk
                    // gets — always allowed (`SnapshotResend::Always`).
                    return self.broadcast_append(SnapshotResend::Always);
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
    ///
    /// **`SnapshotResend::Capped(0)` (issues #532/#537)**: this fires on
    /// every single propose, so for a peer mid-chunked-`InstallSnapshot` it
    /// must never resend an already-outstanding offset more than once
    /// before real progress or a different trigger arrives — see
    /// `snapshot_chunk_for`'s own doc for the full mechanism this closes.
    pub fn replicate_now(&mut self, now: Nanos) -> Vec<Out<C>> {
        if self.role == Role::Leader {
            self.heartbeat_deadline = Nanos(now.0.saturating_add(self.heartbeat_nanos()));
            self.broadcast_append(SnapshotResend::Capped(0))
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
            // Issue #595: a provably newer term exists elsewhere, so
            // whatever leader this node associated with its OLD term is
            // genuinely stale — unlike `start_pre_vote`/`start_election`'s
            // own `leader_id = None` (mere local suspicion, no evidence the
            // leader actually failed), clearing the observational contact
            // record here is honest. See `last_leader_contact`'s own doc.
            self.last_leader_contact = None;
            // A stale transfer from a leadership stint that just ended has no
            // meaning as a follower (`propose`/`change_membership`/
            // `broadcast_append` all gate on `role == Leader` first, so a stale
            // `Some` here is otherwise inert) — clear it anyway so a future
            // `is_leader`-independent inspection (e.g. tests, admin views) never
            // reports a "transfer in flight" for a node that isn't leading.
            self.transfer_target = None;
        }
        // ADR 0044 phase-1 PR3, un-quiesce trigger (a): **any** inbound Raft
        // message un-quiesces, run before dispatch so every specific handler
        // below always observes `quiesced == false` — including pre-vote
        // traffic (deliberately not excluded here the way the step-down above
        // is: a pre-vote round carries no term authority, but it is still
        // real inbound traffic proving this node is not truly isolated).
        // Over-triggering is always safe (a quiesced node just resumes
        // ticking; worst case is one wasted settle window), mirroring this
        // crate's other witness-even-if-rejected patterns. Deliberately does
        // NOT touch `election_deadline` — resetting a bystander's own
        // election timer here would defeat `handle_pre_vote`'s lease check
        // (left deliberately stale while quiesced, fork C), which is what
        // lets a follower correctly grant a pre-vote to a genuinely new
        // candidate once its old leader is truly gone. Only
        // [`on_local_wake`](Self::on_local_wake) re-arms the election timer,
        // and only for the follower that itself asked to be woken.
        if self.quiesced {
            self.quiesced = false;
            self.last_activity = now;
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
                needs_snapshot,
            } => self.handle_append_resp(from, term, success, match_index, needs_snapshot, now),
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
                learners,
            } => self.handle_install_snapshot(
                term, leader, last_index, last_term, offset, data, total, done, config, learners,
                now, entropy,
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
            RaftMsg::TimeoutNow { term } => self.handle_timeout_now(term, now, entropy),
            RaftMsg::Quiesce { term, commit_index } => {
                self.handle_quiesce(from, term, commit_index);
                Vec::new()
            }
            RaftMsg::WakeRequest { .. } => self.handle_wake_request(from),
        }
    }

    /// Handle a leadership-transfer request (see [`RaftMsg::TimeoutNow`]).
    /// Ignored unless it is for our current term, we are not already the leader,
    /// and we are a voter — a stale transfer (superseded by a newer election) or
    /// one addressed to a node since removed from the configuration is a no-op.
    /// Otherwise campaign immediately via [`start_election`](Self::start_election),
    /// deliberately skipping the pre-vote phase: pre-vote exists to stop a
    /// partitioned node from disrupting a *live* leader, which does not apply
    /// here — the live leader itself asked for this.
    fn handle_timeout_now(&mut self, term: u64, now: Nanos, entropy: u64) -> Vec<Out<C>> {
        if term != self.current_term || self.role == Role::Leader || !self.is_voter() {
            return Vec::new();
        }
        self.start_election(now, entropy)
    }

    /// Propose a command. If leader, append it (replicated on the next
    /// heartbeat); otherwise report the leader hint. While a leadership transfer
    /// is armed (see [`transfer_leadership`](Self::transfer_leadership)) this
    /// also reports `NotLeader` — the log must stop growing so replication can
    /// catch the transfer target up to `last_log_index` and receive
    /// `TimeoutNow`; the caller re-routes to (or backs off and retries) the
    /// named hint, and the proposal is safe to retry once the transfer resolves
    /// (either the target becomes leader, or this node aborts the transfer and
    /// resumes proposing).
    pub fn propose(&mut self, command: C) -> ProposeResult {
        if self.role != Role::Leader {
            return ProposeResult::NotLeader {
                leader: self.leader_id.clone(),
            };
        }
        if let Some(target) = self.transfer_target.clone() {
            return ProposeResult::NotLeader {
                leader: Some(target),
            };
        }
        let index = self.last_log_index() + 1;
        self.log_append(LogEntry {
            term: self.current_term,
            index,
            command,
            config: None,
            learners: None,
        });
        // Lets a single-node group make progress; safe for larger groups
        // because commit still requires a majority of matchIndex.
        self.maybe_advance_commit();
        self.apply();
        ProposeResult::Accepted {
            index,
            term: self.current_term,
        }
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
        // ADR 0058 Train 1: a learner never pre-votes — it is never solicited
        // in the first place (`start_pre_vote` broadcasts to `peers`, which
        // stays voter-only), but this is a cheap, structurally-load-bearing
        // second line of defense: even a stray/injected `PreVote` addressed to
        // a learner can never be granted, so a learner's presence or absence
        // can never influence a pre-vote majority.
        let granted = self.is_voter() && !has_live_leader && term >= self.current_term && log_ok;
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
            // ADR 0058 Train 1: only ever count a grant from a current voter
            // toward the pre-vote majority — `majority()` is computed over
            // `cluster_size` (voters only), so tallying a learner's grant
            // here would let a candidate reach "majority" without actually
            // having a majority of real quorum members on board. In normal
            // operation this can't happen (only voter peers are solicited —
            // see `handle_pre_vote`'s own gate), but this is the safety net
            // if that ever changes.
            if term == self.current_term + 1 && self.config.contains(&from) {
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
            // Issue #595: same reasoning as `handle`'s own generic
            // higher-term step-down — see `last_leader_contact`'s doc.
            self.last_leader_contact = None;
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
            let can_vote = self.voted_for.is_none() || self.voted_for == Some(candidate.clone());
            // ADR 0058 Train 1: a learner never grants a real vote either
            // (mirrors `handle_pre_vote`'s identical gate/rationale).
            if self.is_voter() && can_vote && log_ok {
                self.voted_for = Some(candidate.clone());
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
        // ADR 0058 Train 1: same safety net as `handle_pre_vote_resp` — only a
        // current voter's grant counts toward the real election majority.
        if granted && self.config.contains(&from) {
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
                    needs_snapshot: self.state_machine_behind,
                },
            )];
        }
        // Valid leader for our term: become/stay follower and defer the timeout.
        self.role = Role::Follower;
        self.leader_id = Some(leader.clone());
        // Issue #595: a genuine leader contact — see `last_leader_contact`'s
        // own doc for why this is recorded separately from `leader_id`.
        self.last_leader_contact = Some((leader.clone(), now));
        self.reset_election_timer(now, entropy);

        // The leader's prev is behind our snapshot: those entries are already in
        // our snapshot, so report we match up to the snapshot and let the leader
        // resend from there. (Common right after we compacted past the leader.)
        // Issue #554: this is exactly the shape a `state_machine_behind`
        // replica hits on every ordinary heartbeat once its log has fully
        // caught up to the leader's — the log tail matches, so without
        // `needs_snapshot` the leader would never learn this replica's own
        // engine is still missing everything before `snapshot_index`.
        if prev_log_index < self.snapshot_index {
            return vec![(
                leader,
                RaftMsg::AppendEntriesResp {
                    term: self.current_term,
                    success: true,
                    match_index: self.snapshot_index,
                    needs_snapshot: self.state_machine_behind,
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
                    needs_snapshot: self.state_machine_behind,
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
                needs_snapshot: self.state_machine_behind,
            },
        )]
    }

    fn handle_append_resp(
        &mut self,
        from: NodeId,
        term: u64,
        success: bool,
        match_index: u64,
        needs_snapshot: bool,
        now: Nanos,
    ) -> Vec<Out<C>> {
        if self.role != Role::Leader || term != self.current_term {
            return Vec::new();
        }
        // Either outcome — success or reject — proves `from` is up and
        // reachable right now, which is exactly the liveness signal
        // `peer_last_contact`/`control_peer_believed_alive` need. Stamped once
        // here, ahead of the branch, so both paths get it identically.
        self.last_contact.insert(from.clone(), now);
        if success {
            let m = self.match_index.entry(from.clone()).or_insert(0);
            *m = (*m).max(match_index);
            if self
                .departing
                .get(&from)
                .is_some_and(|&needed| match_index >= needed)
            {
                self.departing.remove(&from);
            }
        }
        if needs_snapshot {
            // Issue #554: `from`'s own state machine is behind its own log's
            // compacted start — its log tail may well already match ours
            // (this can arrive on a `success` response), so nothing about
            // `next_index`/`match_index` would otherwise signal a gap.
            //
            // **Already served, just still digesting — don't re-trigger.**
            // `needs_snapshot` stays `true` on EVERY one of `from`'s own
            // `AppendEntriesResp`s until ITS OWN async apply task actually
            // merges a completed install into its engine (a window spanning
            // several of its own heartbeat acks — see `animus-cp-data`'s
            // per-loop-iteration live feed, `drive`'s doc). Blindly forcing
            // `next_index` back to 1 on every one of those acks would
            // restart a fresh chunked transfer before the peer ever
            // finishes digesting the LAST one — a self-sustaining cycle
            // that never lets it catch up, confirmed live (`docs/
            // engineering-lessons.md`'s matching entry). `snapshot_served_
            // through` is the fix: once this leader has fully shipped `from`
            // a snapshot AT OR PAST its current `snapshot_index`, further
            // `needs_snapshot: true` acks for that same base are known-stale
            // echoes and are left alone — `self.snapshot_index` moving again
            // (a fresh compaction outpacing a still-slow peer) naturally
            // invalidates the entry and lets a genuinely new request through.
            let already_served = self
                .snapshot_served_through
                .get(&from)
                .is_some_and(|&served| served >= self.snapshot_index);
            if !already_served {
                // Resetting `next_index` down into the snapshot region is
                // exactly what `replicate_to`'s own `next <= snapshot_index`
                // check already knows how to turn into a chunked transfer
                // (built at THIS leader's current applied index via the
                // existing lazy-image path), reusing every bit of that
                // machinery — chunking, the resend cap, the `DRIVER_APPLIED`
                // on-demand image build. `match_index` (above, when
                // `success`) is left untouched: it is a genuine fact about
                // log agreement, unrelated to what the engine holds.
                self.next_index.insert(from.clone(), 1);
            }
            self.maybe_advance_commit();
            self.apply();
            return self
                .replicate_to(from, SnapshotResend::Always)
                .into_iter()
                .collect();
        }
        if success {
            self.next_index.insert(from.clone(), match_index + 1);
            self.maybe_advance_commit();
            self.apply();
            if self.next_index.get(&from).copied().unwrap_or(1) <= self.last_log_index() {
                return self
                    .replicate_to(from, SnapshotResend::Always)
                    .into_iter()
                    .collect();
            }
            Vec::new()
        } else {
            let ni = self.next_index.entry(from.clone()).or_insert(1);
            if *ni > 1 {
                *ni -= 1;
            }
            self.replicate_to(from, SnapshotResend::Always)
                .into_iter()
                .collect()
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
        learners: Option<BTreeSet<NodeId>>,
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
        self.leader_id = Some(leader.clone());
        // Issue #595: a genuine leader contact — see `last_leader_contact`'s
        // own doc for why this is recorded separately from `leader_id`.
        self.last_leader_contact = Some((leader.clone(), now));
        self.reset_election_timer(now, entropy);

        // Already at least this far along: drop any partial transfer and just
        // acknowledge our position (the leader will stop sending chunks).
        //
        // **Issue #554: this short-circuit is exactly wrong for a
        // `state_machine_behind` node**, and is in fact THE scenario the
        // whole mechanism exists to fix — its own log-derived `snapshot_index`
        // is precisely the fact this node cannot trust: a wiped/rebuilt
        // engine reopened fresh keeps the OLD, still-valid `snapshot_index`
        // from its intact log, so an incoming offer at that SAME index
        // (the overwhelmingly common case: the offer is built at the
        // leader's own current base, which the follower's log already
        // matched before its engine was lost) would otherwise be silently
        // discarded here as "redundant," never reaching `pending_install`,
        // never touching the empty engine at all — the exact silent-data-
        // loss-disguised-as-a-no-op this whole design closes. While behind,
        // fall through to the normal reassembly path unconditionally instead
        // (safe even if `last_index` turns out to be strictly below this
        // node's own `snapshot_index` in some rarer divergent-compaction-
        // cadence case: installing a slightly-older-but-still-valid image is
        // wasted work, never incorrect — the log tail still replays over it
        // afterward, and per-key LWW makes any overlap idempotent).
        if last_index <= self.snapshot_index && !self.state_machine_behind {
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
        if let Some(inc) = &mut self.incoming_snapshot
            && inc.last_index == last_index
            && offset == inc.buf.len() as u64
        {
            inc.buf.extend_from_slice(&data);
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
                // Adopt the snapshot's voter/learner configuration (ADR 0017 C,
                // ADR 0058 Train 1): the image bytes carry no Raft membership of
                // either class. With the log now empty, `recompute_config`
                // resolves to this snapshot config (or initial).
                core.snapshot_config = config.clone();
                core.snapshot_learners = learners.clone();
                core.recompute_config();
            };
            if S::DRIVER_APPLIED {
                // The bytes are the leader's engine image; the driver writes them
                // into this follower's engine (`drain_pending_install`). The in-core
                // `metadata` stays the unit placeholder.
                let bytes = inc.buf;
                install(self);
                // Do NOT retain the installed image as our own snapshot blob:
                // `DRIVER_APPLIED` images are built lazily from the engine (see
                // [`snapshot_chunk_for`]). Once the driver writes these bytes into
                // the engine (`drain_pending_install`, which its apply task does
                // *before* servicing any lazy-build request in the same pass), this
                // node can regenerate an image at (or past) `snapshot_index` on
                // demand — so the second-hop re-ship invariant that used to require
                // retaining the bytes forever (the
                // `caught_up_node_reships_non_empty_snapshot` regression) now holds
                // by regeneration, at any hop depth, with no O(state) resident copy.
                // Any *own* stale blob from an earlier leadership is dead now that
                // the base moved.
                self.snapshot_blob = None;
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
            self.snapshot_chunk_sent.remove(&from);
            // Lazy-image discipline (`DRIVER_APPLIED`): once no transfer is in
            // flight, drop the materialized image instead of retaining a
            // whole-tablet copy in the core indefinitely — a later straggler
            // triggers an on-demand rebuild from the engine. Kept while any
            // other peer's transfer is mid-flight so its chunks stay
            // byte-identical. In-core state machines keep their eager blob.
            if S::DRIVER_APPLIED && self.snapshot_offset.is_empty() {
                self.snapshot_blob = None;
            }
            let m = self.match_index.entry(from.clone()).or_insert(0);
            *m = (*m).max(last_index);
            self.next_index.insert(from.clone(), last_index + 1);
            // Issue #554: record that `from` has now been fully served a
            // snapshot at (at least) `last_index` — see `snapshot_served_
            // through`'s own doc and `handle_append_resp`'s `needs_snapshot`
            // handling, the sole reader.
            let served = self
                .snapshot_served_through
                .entry(from.clone())
                .or_insert(0);
            *served = (*served).max(last_index);
            self.maybe_advance_commit();
            self.apply();
            if self.next_index.get(&from).copied().unwrap_or(1) <= self.last_log_index() {
                return self
                    .replicate_to(from, SnapshotResend::Always)
                    .into_iter()
                    .collect();
            }
            return Vec::new();
        }
        // Still mid-transfer: record progress and ship the next chunk.
        //
        // **Monotonic guard, found building this fix.** A follower acks
        // EVERY `InstallSnapshot` chunk it processes, including a stale
        // duplicate that lands after its own buffer has already moved past
        // that offset (`handle_install_snapshot`'s "a reordered/duplicate
        // chunk is ignored" case) — such an ack still reports the
        // follower's own CURRENT (unchanged) position, which is correct on
        // its own. But under a flood of overlapping in-flight sends, acks
        // can reach the leader in an order that does not track real
        // progress: an ack generated for an EARLIER request can be
        // processed by the leader AFTER a LATER one that already advanced
        // things further (both are genuine, freshly-generated acks —
        // nothing here is stale/reordered *network* delivery, only
        // overlapping *requests* completing out of sequence). A bare
        // `insert` let such an ack regress the leader's own tracked offset
        // backward — confirmed directly: instrumenting the pre-fix code
        // counted 217 such regressions in one run of
        // `animus-cp-data/tests/learner_catchup_under_load.rs`, each
        // stepping backward by exactly one chunk, which then cost several
        // more round trips to recover from. `max` makes the tracked offset
        // monotonic regardless of ack arrival order — independent of, and
        // additive with, the resend cap below.
        let entry = self.snapshot_offset.entry(from.clone()).or_insert(0);
        *entry = (*entry).max(next_offset);
        // `SnapshotResend::Capped(SNAPSHOT_ACK_RESEND_CAP)`, not `Always` and
        // not `Capped(0)` — see `snapshot_chunk_for`'s own doc for why this
        // one call site needs a genuine, nonzero-but-bounded cap rather than
        // either extreme.
        self.replicate_to(from, SnapshotResend::Capped(SNAPSHOT_ACK_RESEND_CAP))
            .into_iter()
            .collect()
    }

    // ---- role transitions & replication ---------------------------------

    /// **Campaign for leadership immediately** instead of waiting out the
    /// randomized election timeout `tick` would otherwise wait for (ADR 0058
    /// Train 2 rung 4's deterministic first-leader mechanism for a
    /// freshly-forked child group: the parent's own leader, which is a voter
    /// of both children by construction — see `bootstrap_voters` — campaigns
    /// in each the moment it materializes them, rather than leaving a
    /// brand-new group leaderless until its own cold randomized timeout
    /// fires).
    ///
    /// Runs exactly the **pre-vote** round `tick` runs once
    /// `election_deadline` passes — never a raw, term-incrementing
    /// `start_election` directly — so it inherits every one of pre-vote's
    /// existing safety properties for free, with no new machinery: a peer
    /// whose own child-group instance has not started yet simply never
    /// responds (its message sits queued in the `Env`'s per-`(node, stream)`
    /// inbox — ADR 0026's multiplexed addressing already queues by
    /// destination regardless of whether a consumer is currently polling —
    /// until that peer's own `start_hosted` call reaches its first
    /// `recv_stream`, at which point it is simply this group's very first
    /// inbound message); a round that gets no majority in time (a
    /// late-starting peer, or two replicas racing to self-nominate at once)
    /// re-arms the ordinary election timer exactly as a real timeout would
    /// and falls back to the untouched randomized-timeout retry path with
    /// **zero** special-cased recovery; and a peer that already has a live
    /// leader (this replica's own campaign lost the race) correctly
    /// withholds its pre-vote grant via the unmodified lease check in
    /// [`handle_pre_vote`](Self::handle_pre_vote).
    ///
    /// A safe no-op unless this replica is a **voting** `Follower`:
    /// [`start_pre_vote`](Self::start_pre_vote)'s own `is_voter()` gate is
    /// what makes calling this on a learner, or a node not yet a member at
    /// all, harmless (produces no messages, merely re-arms the local timer)
    /// rather than requiring a duplicate guard here; already being
    /// `PreCandidate`/`Candidate`/`Leader` is also a no-op — this method
    /// **never** demotes an active leader or restarts an in-flight round.
    /// Nothing about quorum/term math changes: this is purely a question of
    /// *when* the first pre-vote round of a brand-new group's life runs,
    /// never *what* it takes to win one. The caller (`animus-cp-data`'s
    /// `drive`, gated on a freshly-bootstrapped group only) additionally
    /// asserts `config().contains(&self_id)` before calling this, as a
    /// structural belt on top of the `is_voter()` gate here — see that call
    /// site's own doc for why the invariant holds by construction anyway.
    #[must_use]
    pub fn campaign_now(&mut self, now: Nanos, entropy: u64) -> Vec<Out<C>> {
        if self.role != Role::Follower {
            return Vec::new();
        }
        self.start_pre_vote(now, entropy)
    }

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
        // Issue #554: a node whose own state machine is behind its own log's
        // compacted start must not campaign either, for the same reason a
        // learner never does (`is_voter()`, above) — winning would make it
        // leader over an engine missing everything the log already
        // discarded, which (unlike a learner) it CAN do here since its log
        // is otherwise fully caught up. Mirrors that gate exactly; see
        // `state_machine_behind`'s own doc.
        if !self.is_voter() || self.state_machine_behind {
            self.reset_election_timer(now, entropy);
            return Vec::new();
        }
        self.role = Role::PreCandidate;
        // The election timer expired ⇒ we no longer believe in a live leader; drop
        // the hint so we will grant *others'* pre-votes this round too. Term and
        // vote are deliberately untouched.
        self.leader_id = None;
        self.pre_votes.clear();
        self.pre_votes.insert(self.id.clone());
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
            .map(|p| {
                (
                    p.clone(),
                    RaftMsg::PreVote {
                        term: prospective,
                        candidate: self.id.clone(),
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
        // Issue #554: same gate as `start_pre_vote`'s own — this is a second,
        // independent entry point (`handle_pre_vote_resp`'s own majority
        // check can reach here without going back through `start_pre_vote`),
        // so both need the check, not just one.
        if !self.is_voter() || self.state_machine_behind {
            self.reset_election_timer(now, entropy);
            return Vec::new();
        }
        self.role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id.clone());
        self.leader_id = None;
        self.votes.clear();
        self.votes.insert(self.id.clone());
        self.reset_election_timer(now, entropy);

        if self.votes.len() >= self.majority() {
            return self.become_leader(now);
        }
        let (lli, llt) = (self.last_log_index(), self.last_log_term());
        self.peers
            .iter()
            .map(|p| {
                (
                    p.clone(),
                    RaftMsg::RequestVote {
                        term: self.current_term,
                        candidate: self.id.clone(),
                        last_log_index: lli,
                        last_log_term: llt,
                    },
                )
            })
            .collect()
    }

    fn become_leader(&mut self, now: Nanos) -> Vec<Out<C>> {
        self.role = Role::Leader;
        self.leader_id = Some(self.id.clone());
        // Issue #595: this node itself just won an election — record itself
        // as the genuine contact (see `last_leader_contact`'s own doc).
        self.last_leader_contact = Some((self.id.clone(), now));
        // A fresh leadership stint always starts un-quiesced (ADR 0044 phase-1
        // PR3) with its idle clock starting now — even if this same node was
        // quiesced as a follower a moment ago (its own `quiesced` from
        // accepting the previous leader's `Quiesce` is now meaningless).
        self.quiesced = false;
        self.last_activity = now;
        let last = self.last_log_index();
        // ADR 0058 Train 1: seed a learner's `next_index`/`match_index`/
        // `last_contact` the identical way a voter's is seeded — a learner is
        // replicated to and tracked exactly like a follower, just never
        // counted toward quorum (see `apply_config`'s doc).
        for p in self.peers.clone().into_iter().chain(self.learners.clone()) {
            self.next_index.insert(p.clone(), last + 1);
            self.match_index.insert(p.clone(), 0);
            // Start every peer's liveness clock fresh on this leader's own
            // stint: an unresponsive peer must age out `CONTROL_PEER_LIVENESS_
            // TIMEOUT` after this leader took over, not be granted the
            // "never contacted yet" grace forever just because this
            // leader's own `last_contact` map starts empty.
            self.last_contact.insert(p, now);
        }
        // A fresh leadership stint starts with no departing-peer bookkeeping — any
        // peer still owed a removal notification is discovered anew the next time
        // this leader itself appends a config entry removing it (see the field
        // doc); it is not reconstructed from a previous leader's in-flight state.
        self.departing.clear();
        self.transfer_target = None;
        // A fresh term restarts any snapshot transfer from offset 0.
        self.snapshot_offset.clear();
        // No-op entry so prior-term entries can be committed under our term.
        // Record its index: it is this leader's first current-term entry, the
        // watermark ReadIndex barriers and membership changes gate on
        // (`first_term_index`, Raft §6.4).
        self.first_term_index = last + 1;
        self.log_append(LogEntry {
            term: self.current_term,
            index: last + 1,
            command: S::noop(),
            config: None,
            learners: None,
        });
        // Let a single-node group commit its no-op immediately (majority == 1);
        // in a larger group commit still waits on follower `matchIndex`. Without
        // this a sole leader's `first_term_index` gate would hold reads and
        // membership changes until its next propose.
        self.maybe_advance_commit();
        self.apply();
        self.heartbeat_deadline = Nanos(now.0.saturating_add(self.heartbeat_nanos()));
        // A fresh leadership term just cleared `snapshot_chunk_sent` above,
        // so this makes no observable difference from `Capped(0)` here —
        // `Always` for consistency with the other "this leader just did
        // something noteworthy" call sites.
        self.broadcast_append(SnapshotResend::Always)
    }

    fn broadcast_append(&mut self, snapshot_resend: SnapshotResend) -> Vec<Out<C>> {
        // Include departing peers (see the `departing` field doc): a peer just
        // removed from `peers` still needs the removing entry replicated to it.
        // Include learners too (ADR 0058 Train 1): they receive
        // `AppendEntries`/`InstallSnapshot` exactly like a voter (see
        // `apply_config`'s doc) even though they never join `peers` itself.
        let mut targets = self.peers.clone();
        targets.extend(self.learners.iter().cloned());
        targets.extend(self.departing.keys().cloned());
        let mut outs: Vec<Out<C>> = targets
            .iter()
            .cloned()
            .filter_map(|p| self.replicate_to(p, snapshot_resend))
            .collect();
        // Send `TimeoutNow` only once the target has actually caught all the way
        // up to `last_log_index` — arming (`transfer_leadership`) only requires
        // `>= commit_index`, which under sustained writes can be well behind the
        // log tip, and a target that campaigns on a stale log could depose this
        // (still perfectly healthy) leader and then lose the election, or win it
        // and truncate entries this leader had already accepted. Once true, keep
        // re-sending every heartbeat (see the `transfer_target` field doc) until
        // this node steps down — resilient to a single dropped message.
        if let Some(target) = self.transfer_target.clone()
            && self.peer_match(&target) == self.last_log_index()
        {
            outs.push((
                target,
                RaftMsg::TimeoutNow {
                    term: self.current_term,
                },
            ));
        }
        outs
    }

    /// The leader-side quiescence **entry predicate** (ADR 0044 phase-1 PR3),
    /// pure and re-evaluated fresh at every heartbeat deadline this leader
    /// hasn't already quiesced. All of:
    /// - no local activity for `quiesce_after` (an idle settle window —
    ///   `last_activity` is bumped by `become_leader`, `note_local_activity`,
    ///   and `transfer_leadership`'s successful arm);
    /// - nothing left to replicate: `commit_index == last_log_index`, and
    ///   `commit_index >= first_term_index` (Raft §6.4 — otherwise this
    ///   leader's own commit index might still not cover everything a *prior*
    ///   leader already committed and acked);
    /// - every voter has fully caught up (`match_index == last_log_index`);
    /// - no leadership transfer armed, no departing peer still owed its
    ///   removal notification, no membership change in flight;
    /// - no snapshot machinery pending in either direction — a follower mid
    ///   catch-up (`incoming_snapshot`, meaningless for a leader but checked
    ///   for symmetry/future-proofing), a fully-received install awaiting the
    ///   driver (`pending_install`), or a lazily-built image the driver hasn't
    ///   supplied yet (`snapshot_needed`);
    /// - the two external inputs a `DRIVER_APPLIED` driver feeds in once per
    ///   loop iteration: the apply task has caught the engine up
    ///   (`quiesce_engine_caught_up`), and no subsystem holds the quiesce veto
    ///   (`quiesce_veto`, fork D), **freshly enough**
    ///   (`quiesce_veto_fresh_through >= commit_index`, issue #302 — see that
    ///   field's own doc for why a bare boolean isn't sound on its own: an
    ///   external veto holder's observation can predate a write that
    ///   committed after it last looked).
    fn quiesce_entry_ok(&self, now: Nanos) -> bool {
        let Some(quiesce_after) = self.quiesce_after else {
            return false;
        };
        if now.0.saturating_sub(self.last_activity.0) < quiesce_after.as_nanos() as u64 {
            return false;
        }
        let last = self.last_log_index();
        self.commit_index == last
            && self.commit_index >= self.first_term_index
            // ADR 0058 Train 1: a learner still mid-catch-up is exactly
            // "something left to replicate" — a leader must not quiesce out
            // from under an active learner catch-up.
            && self
                .peers
                .iter()
                .chain(self.learners.iter())
                .all(|p| self.match_index.get(p).copied().unwrap_or(0) == last)
            && self.transfer_target.is_none()
            && self.departing.is_empty()
            && !self.config_change_in_flight()
            && self.incoming_snapshot.is_none()
            && self.pending_install.is_none()
            && !self.snapshot_needed
            && self.quiesce_engine_caught_up
            && !self.quiesce_veto
            && self.quiesce_veto_fresh_through >= self.commit_index
    }

    /// Broadcast [`RaftMsg::Quiesce`] once to every voter **and learner**
    /// (ADR 0058 Train 1) — the leader-side half of entering quiescence.
    /// Deliberately mirrors `broadcast_append`'s peer selection (`peers` ∪
    /// `learners`, not `departing`: a departing peer that hasn't yet caught
    /// up to its own removal entry would fail `quiesce_entry_ok`'s
    /// `match_index == last_log_index` clause already, so this path can only
    /// be reached with no departing peer outstanding).
    fn broadcast_quiesce(&mut self) -> Vec<Out<C>> {
        // ADR 0058 Train 1: a learner also stops ticking once the group is
        // fully idle, mirroring every voter (`broadcast_append`'s doc).
        self.peers
            .clone()
            .into_iter()
            .chain(self.learners.clone())
            .map(|p| {
                (
                    p,
                    RaftMsg::Quiesce {
                        term: self.current_term,
                        commit_index: self.commit_index,
                    },
                )
            })
            .collect()
    }

    /// Follower-side acceptance of [`RaftMsg::Quiesce`] (ADR 0044 phase-1
    /// PR3): accept — setting this node's own `quiesced` flag, so its own
    /// `next_deadline` also returns `None` — only if every condition proves
    /// this follower is provably caught up to *exactly* the state the leader
    /// broadcast from: same term, `from` is this node's own recorded leader,
    /// and this node's own `last_log_index`/`commit_index` both equal the
    /// message's `commit_index`. Otherwise ignored outright — this follower
    /// keeps ticking normally, and its own ordinary election timeout is what
    /// eventually notices if the leader really is gone (see the module-level
    /// design doc / ADR 0044 for the full argument — a bare timeout-based
    /// disambiguation is a *correct*, if noisier, fallback here, never a
    /// safety hazard).
    fn handle_quiesce(&mut self, from: NodeId, term: u64, commit_index: u64) {
        let accept = term == self.current_term
            && self.leader_id.as_ref() == Some(&from)
            && self.last_log_index() == self.commit_index
            && self.commit_index == commit_index;
        if accept {
            self.quiesced = true;
        }
    }

    /// Leader-side answer to [`RaftMsg::WakeRequest`] (ADR 0044 phase-1 PR3,
    /// fork B): if still leader, reply with an ordinary replication message
    /// (whatever [`replicate_to`](Self::replicate_to) would normally send this
    /// peer — heartbeat or catch-up alike), exactly as if this had been the
    /// next scheduled heartbeat to it. Works identically whether or not this
    /// leader was itself quiesced when the request arrived — `handle`'s
    /// top-level un-quiesce-on-any-message rule has already cleared that flag
    /// by the time this runs. A non-leader answers nothing; the asking
    /// follower's own re-armed election timeout (see
    /// [`on_local_wake`](Self::on_local_wake)) is what then lets it campaign.
    fn handle_wake_request(&mut self, from: NodeId) -> Vec<Out<C>> {
        if self.role != Role::Leader {
            return Vec::new();
        }
        // An explicit poke from the peer itself, not write-rate spam.
        self.replicate_to(from, SnapshotResend::Always)
            .into_iter()
            .collect()
    }

    /// A locally-woken **follower**'s "are you still there?" check (ADR 0044
    /// phase-1 PR3, fork B) — the driver calls this when something touches
    /// this group locally while quiesced (e.g. [`RaftKvNode::wake`], a later
    /// PR's hook). A no-op unless this node is both quiesced and not the
    /// leader (a quiesced leader has nothing to check on; an already-ticking
    /// follower doesn't need this). Un-quiesces, re-arms a **full fresh**
    /// election timeout — giving a merely-quiesced-but-alive leader one whole
    /// interval to answer before this follower would campaign, rather than
    /// campaigning against whatever stale deadline quiescence left behind —
    /// and, if this node has a recorded leader, asks it directly via
    /// [`WakeRequest`](RaftMsg::WakeRequest) instead of waiting out that whole
    /// interval blind.
    pub fn on_local_wake(&mut self, now: Nanos, entropy: u64) -> Vec<Out<C>> {
        if !self.quiesced || self.role == Role::Leader {
            return Vec::new();
        }
        self.quiesced = false;
        self.last_activity = now;
        self.reset_election_timer(now, entropy);
        match &self.leader_id {
            Some(leader) => vec![(
                leader.clone(),
                RaftMsg::WakeRequest {
                    term: self.current_term,
                },
            )],
            None => Vec::new(),
        }
    }

    /// Opt in to quiescence (ADR 0044 phase-1 PR3): once this leader has had
    /// no local activity for `after` and every other
    /// [`quiesce_entry_ok`](Self::quiesce_entry_ok) clause holds, it
    /// broadcasts [`Quiesce`](RaftMsg::Quiesce) once and stops ticking
    /// (`next_deadline` returns `None`) until some event wakes it. Defaults to
    /// never (`quiesce_after: None`) — **the control plane's `RaftNode` never
    /// calls this** (fork G), so quiescence stays data-plane-only throughout
    /// this stack.
    pub fn enable_quiescence(&mut self, after: Duration) {
        self.quiesce_after = Some(after);
    }

    /// Whether this node currently considers itself quiesced.
    #[must_use]
    pub fn is_quiesced(&self) -> bool {
        self.quiesced
    }

    /// External input (ADR 0044 phase-1 PR3): whether the async apply task's
    /// engine state has caught up to [`last_applied`](Self::last_applied) as
    /// of this call. The core has no visibility into engine I/O itself, so a
    /// `DRIVER_APPLIED` driver calls this once per loop iteration, before
    /// `tick`ing, to feed it in. Defaults `true` — harmless, since it is only
    /// ever consulted by `quiesce_entry_ok`, itself only reachable once a
    /// caller has opted in via `enable_quiescence`.
    pub fn set_quiesce_engine_caught_up(&mut self, caught_up: bool) {
        self.quiesce_engine_caught_up = caught_up;
    }

    /// External input (ADR 0044 phase-1 PR3, fork D; freshness added by the
    /// issue #302 fix): whether some subsystem currently holds the quiesce
    /// veto, and the log index its observation is valid through — see
    /// `quiesce_veto_fresh_through`'s own doc for the freshness contract a
    /// caller must uphold (in short: read your own "as of" index BEFORE
    /// making the observation that decides `veto`, never after, or a
    /// concurrent apply can make the recorded freshness a false promise).
    /// `fresh_through` only matters when `veto` is `false`, since `veto ==
    /// true` already blocks `quiesce_entry_ok` outright; a caller with no
    /// natural index to report (e.g. an in-core, always-synchronous veto
    /// source that never goes stale between calls) may simply pass
    /// `u64::MAX`.
    pub fn set_quiesce_veto(&mut self, veto: bool, fresh_through: u64) {
        self.quiesce_veto = veto;
        self.quiesce_veto_fresh_through = fresh_through;
    }

    /// Un-quiesce trigger for a local mutating action that has no `now` of its
    /// own to work with (`propose`/`change_membership` don't take one — see
    /// their own docs) — the driver calls this immediately after confirming
    /// `ProposeResult::Accepted`, mirroring what `become_leader` and
    /// `transfer_leadership`'s successful arm already do inline. Idempotent
    /// and harmless if this node isn't even quiesced.
    pub fn note_local_activity(&mut self, now: Nanos) {
        self.last_activity = now;
        self.quiesced = false;
    }

    /// Build the right replication message for `peer`: an `InstallSnapshot` chunk
    /// if the entries it needs have been compacted away, otherwise
    /// `AppendEntries`, capped at [`MAX_APPEND_ENTRIES_BATCH`] entries (issues
    /// #532/#537 — see that constant's doc for why an uncapped send is
    /// unsafe under sustained write load, not merely inefficient). `None`
    /// when a needed snapshot image is not materialized yet (a
    /// `DRIVER_APPLIED` plane builds it lazily — see
    /// [`snapshot_chunk_for`](Self::snapshot_chunk_for)); the peer is simply
    /// retried on the next heartbeat once the driver supplies the image.
    fn replicate_to(&mut self, peer: NodeId, snapshot_resend: SnapshotResend) -> Option<Out<C>> {
        let next = self.next_index.get(&peer).copied().unwrap_or(1).max(1);
        // The entry before `next` is in our snapshot (or earlier) — we can't form
        // a valid `prev_log_term`, so ship the snapshot instead, as the next
        // offset-addressed chunk for this peer.
        if next <= self.snapshot_index {
            return self.snapshot_chunk_for(peer, snapshot_resend);
        }
        let prev_log_index = next - 1;
        let prev_log_term = self.term_at(prev_log_index);
        let entries: Vec<LogEntry<C>> = self
            .log
            .iter()
            .filter(|e| e.index >= next)
            .take(MAX_APPEND_ENTRIES_BATCH)
            .cloned()
            .collect();
        Some((
            peer,
            RaftMsg::AppendEntries {
                term: self.current_term,
                leader: self.id.clone(),
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            },
        ))
    }

    /// Build the next `InstallSnapshot` chunk for `peer`, starting at the byte
    /// offset recorded in `snapshot_offset` (0 if no transfer is in flight).
    /// Cheap: it slices the serialized [`snapshot_blob`] **by reference** rather
    /// than (re-)serializing or cloning the state, so repeated calls at the same
    /// offset are byte-identical, deterministic, and O(chunk) — not O(state) per
    /// chunk.
    ///
    /// **Lazy image build (`DRIVER_APPLIED`)**: the engine image is *not* kept
    /// materialized between transfers — that would rebuild + retain a
    /// whole-tablet image on every compaction whether or not any follower ever
    /// needs it. When the blob is absent this raises `snapshot_needed` and sends
    /// nothing; the async driver observes the flag
    /// ([`take_snapshot_needed`](Self::take_snapshot_needed)), scans the engine
    /// into an image, and installs it via [`set_snapshot_blob`], after which the
    /// next heartbeat retry actually ships chunk 0. The working invariant is
    /// therefore *"any node with `snapshot_index > 0` can regenerate the image
    /// from its engine on demand"* — strictly stronger than the old *"a received
    /// image is retained forever"*, and it holds at any hop depth (a node that
    /// itself caught up via `InstallSnapshot` regenerates from the engine its
    /// driver populated) **and across recovery** (a restarted leader used to have
    /// no blob until its next compaction and shipped 0 bytes; now it regenerates).
    /// An **in-core** state machine keeps the eager cached image (`snapshot_upto`
    /// / install / recovery all set it — the control-plane driver-liveness fix),
    /// so its blob is always present here and the flag never fires.
    ///
    /// **Resend cap (issues #532/#537, ADR 0009's third 2026-09-01
    /// amendment).** `replicate_now`'s wake-on-propose calls `broadcast_append`
    /// for every peer on every propose, and this method used to unconditionally
    /// re-slice and re-send whatever chunk is still outstanding for a peer on
    /// every one of those calls — so under a sustained proposer, a peer
    /// mid-transfer received the SAME unacked chunk again and again, at write
    /// rate, long before it could possibly have acked the last one (confirmed
    /// live: 96,451 chunk sends for 196 offset transitions, the tracked offset
    /// parked for a whole run). Compounding it, EVERY response the flood
    /// provoked — even a duplicate, no-progress ack — fed straight back into
    /// another unconditional resend (`handle_install_snapshot_resp`'s own
    /// call), so the flood was self-sustaining once started, bounded only by
    /// round-trip time, not by anything a caller controlled. Together this
    /// congested the peer's single-consumer inbox badly enough that its
    /// transfer couldn't complete inside `COMPACT_DEFER_CEILING`'s window, so
    /// compaction eventually invalidated it (`snapshot_upto` unconditionally
    /// drops in-flight progress when the base moves — required for
    /// correctness, see that method's own doc) and it restarted from chunk 0,
    /// forever.
    ///
    /// [`snapshot_chunk_sent`](Self::snapshot_chunk_sent) tracks the offset
    /// last actually shipped to each peer and how many times it has been
    /// resent since; a resend of the SAME offset is suppressed once the
    /// caller's own `SnapshotResend::Capped(n)` budget is exhausted — a chunk
    /// for a genuinely NEW offset (real ack progress, or nothing sent yet)
    /// always ships immediately regardless, at every call site.
    ///
    /// **Both call sites need their own cap; neither is dispensable, and
    /// the two tests that pin this down are looking at different
    /// workloads for a reason.**
    /// `animus-cp-data/tests/snapshot_resend_bound.rs` drives one propose
    /// per scheduler turn (matching the field's own continuous-write
    /// shape) — under it, `replicate_now`'s wake-on-propose genuinely fires
    /// close to once per write, so capping wake-on-propose ALONE
    /// (`Capped(0)`) already takes the measured sends-per-genuine-advance
    /// ratio from several HUNDRED (the unfixed mechanism, matching the
    /// field's own ~492-per-transition order of magnitude) down to roughly
    /// 90 — most of this fix's own win, and confirmation that the field
    /// diagnosis's own naming of wake-on-propose as the primary culprit
    /// holds for that workload shape. `animus-cp-data/tests/
    /// learner_catchup_under_load.rs` instead drives tight synchronous
    /// bursts of ten proposes with no yield in between — under THAT shape,
    /// `replicate_now`'s own wake is a single coalesced `AtomicBool`
    /// (`ProposeSignal`, not a per-propose counter), so it already fires at
    /// most once per burst regardless of `Capped(0)`; capping wake-on-propose
    /// there changes little on its own, and the mechanism this test is
    /// sensitive to is entirely the ack-handler's own resend
    /// (`handle_install_snapshot_resp`, below). Two narrower shapes were
    /// tried first and rejected against this test: skipping a mid-snapshot
    /// peer from wake-on-propose entirely, and throttling wake-on-propose
    /// by propose *count* (1-in-2, 1-in-20) — both regressed it (the
    /// learner never caught up), for exactly the reason above: neither
    /// prototype's throttle ever touched the ack-handler's own
    /// self-sustaining cascade, which is what that test's convergence
    /// actually depends on. A **zero** cap on the ack-handler's own call
    /// site (identical treatment to wake-on-propose) was tried too and
    /// regressed this same test — a genuinely stuck transfer needs *some*
    /// bounded number of ack-driven retries to escape before the next
    /// heartbeat, since under sustained write load `heartbeat_deadline` is
    /// perpetually deferred by `replicate_now`'s own reset (see that
    /// method's doc), so that backstop rarely fires in time on its own.
    /// `SNAPSHOT_ACK_RESEND_CAP` (a small, nonzero, bounded count — see its
    /// own doc) is what closes both findings at once: it preserves the
    /// ack-handler's retransmit role (`learner_catchup_under_load.rs` stays
    /// green) while still giving it a genuine worst-case ceiling —
    /// `Always` there converges too and happens not to run away further in
    /// `snapshot_resend_bound.rs`'s own one-seed measurement (~90 either
    /// way), but has no STRUCTURAL bound of its own the way `Capped(n)`
    /// does, which is the property this fix is actually supposed to
    /// guarantee, not merely happen to exhibit on one workload.
    fn snapshot_chunk_for(
        &mut self,
        peer: NodeId,
        snapshot_resend: SnapshotResend,
    ) -> Option<Out<C>> {
        let Some(serialized) = self.snapshot_blob.as_deref() else {
            self.snapshot_needed = true;
            return None;
        };
        let total = serialized.len() as u64;
        let offset = self
            .snapshot_offset
            .get(&peer)
            .copied()
            .unwrap_or(0)
            .min(total);
        let resends_so_far = self
            .snapshot_chunk_sent
            .get(&peer)
            .filter(|&&(last_offset, _)| last_offset == offset)
            .map_or(0, |&(_, resends)| resends);
        if let SnapshotResend::Capped(limit) = snapshot_resend
            && resends_so_far > limit
        {
            return None;
        }
        if resends_so_far == 0 {
            *self
                .snapshot_chunk_advances
                .entry(peer.clone())
                .or_insert(0) += 1;
        }
        let start = offset as usize;
        let end = (start + SNAPSHOT_CHUNK_BYTES).min(serialized.len());
        let data = serialized[start..end].to_vec();
        let done = end as u64 == total;
        self.snapshot_chunk_sent
            .insert(peer.clone(), (offset, resends_so_far.saturating_add(1)));
        Some((
            peer,
            RaftMsg::InstallSnapshot {
                term: self.current_term,
                leader: self.id.clone(),
                last_index: self.snapshot_index,
                last_term: self.snapshot_term,
                offset,
                data,
                total,
                done,
                config: self.snapshot_config.clone(),
                learners: self.snapshot_learners.clone(),
            },
        ))
    }

    /// Take-and-clear the **lazy snapshot-image request** flag: `true` when a
    /// replication attempt needed to ship an `InstallSnapshot` chunk but no image
    /// was materialized (see [`snapshot_chunk_for`](Self::snapshot_chunk_for)).
    /// The `DRIVER_APPLIED` driver polls this from its apply task, builds the
    /// engine image, and installs it with [`set_snapshot_blob`]; the in-core
    /// control plane never raises it.
    pub fn take_snapshot_needed(&mut self) -> bool {
        std::mem::replace(&mut self.snapshot_needed, false)
    }

    /// Whether this node's own state machine is behind its own log's
    /// compacted start (issue #554) — see [`set_state_machine_behind`]
    /// (Self::set_state_machine_behind)'s doc for the full mechanism. Read
    /// by the driver to decide whether to merge freshly-committed effects
    /// into the engine this pass (it must not, while behind — see
    /// `animus-cp-data::apply_and_compact`).
    #[must_use]
    pub fn state_machine_behind(&self) -> bool {
        self.state_machine_behind
    }

    /// Set (or clear) the needs-snapshot state (issue #554). Only ever
    /// called by a `DRIVER_APPLIED` plane's own driver — never by the
    /// in-core control plane, for which this stays permanently `false` and
    /// every dependent behavior (the campaign gate below, the
    /// `needs_snapshot` field on this node's own `AppendEntriesResp`s) is
    /// therefore inert.
    ///
    /// **Call this LIVE, every driver loop iteration** — recomputed fresh
    /// from `engine_applied.load() < self.snapshot_index()` each time —
    /// never as a one-shot latch set at `drive()` start and cleared later by
    /// a *different* async task (the apply task, once an install lands).
    /// `animus-cp-data`'s consensus loop does exactly this, in the same
    /// lock acquisition as `set_quiesce_engine_caught_up`, mirroring that
    /// method's own established "feed the one external input the core has
    /// no visibility into itself, once per iteration, before `tick`" idiom.
    /// A driver-latched version was tried first and produced a real,
    /// reproducible **livelock**: `snapshot_index` advances synchronously
    /// the instant this node's own `handle_install_snapshot` completes a
    /// transfer, but the async apply task that actually merges the install
    /// into the engine — and would clear a latch — runs on its own separate
    /// schedule, so every `AppendEntriesResp` built in that window still
    /// echoed `needs_snapshot: true`, and a leader (correctly, from what it
    /// was told) kept resetting the peer's `next_index` back to 1 before it
    /// ever finished digesting the transfer it just received. See
    /// `docs/engineering-lessons.md`'s matching entry and ADR 0009's
    /// 2026-09-02 addendum for the full account, including the leader-side
    /// half of the fix this alone was not sufficient for
    /// (`snapshot_served_through`).
    pub fn set_state_machine_behind(&mut self, behind: bool) {
        self.state_machine_behind = behind;
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
            let entry = &self.log[offset];
            let term = entry.term;
            let command = entry.command.clone();
            if S::DRIVER_APPLIED {
                // The async driver applies this to the real engine (drained via
                // `drain_apply`); the core only decides the order. Don't grow the
                // unbounded `applied` log for the data plane. `term` rides along so
                // a driver-side outcome channel can prove entry identity across a
                // truncation (see `drain_apply`'s doc).
                self.pending_apply.push((self.last_applied, term, command));
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

// ADR 0038 PR3: the `RaftCore<MetaCommand, Metadata>` conveniences that used
// to live here (`metadata`/`members`/`placement_view`, reading `self.metadata`
// directly) are gone — `Metadata` is now `DRIVER_APPLIED`, so `self.metadata`
// is an unused default the core never touches (mirroring `animus-cp-data`'s
// `KvState` placeholder). The equivalent reads now live on `RaftNode` (in
// `node.rs`), backed by the apply task's own owned `Metadata` published into
// an `engine_applied`-gated cache — never the core's in-memory field.
