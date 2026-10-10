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

use crate::format;
use crate::meta::{MetaCommand, Metadata};
use crate::persist::{CONTROL_SNAPSHOT, PersistedState, WalRecord};

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

/// Default maximum bytes of serialized snapshot carried by a single
/// `InstallSnapshot` message. A snapshot larger than this is shipped over
/// several offset-addressed chunks and reassembled by the follower (ADR
/// 0009). The value only affects message granularity, never correctness —
/// a test that specifically wants MANY chunks out of a modest-sized image
/// dials this down per-instance via
/// [`RaftCore::set_snapshot_chunk_bytes`] instead of inflating the image.
///
/// **Raised from 1024 to 64 KiB (2026-09-27, ADR 0009's "snapshot chunk
/// size" amendment)**: at 1024 bytes a modest tablet snapshot needed hundreds
/// of chunks, each its own round trip, so a transfer to a contended or slow
/// peer stayed in flight long enough for leader churn or the next compaction
/// to invalidate it (what `COMPACT_DEFER_IDLE_CEILING`,
/// `COMPACT_DEFER_EMERGENCY_CEILING` and `RaftCore::compaction_floor` all have
/// to work around). 64 KiB cuts the round-trip count ~64x while keeping one
/// message far below ordinary transport message-size limits. A transfer does
/// not need to fit inside an election timeout: every chunk resets the
/// receiver's election timer (`handle_install_snapshot`).
pub const SNAPSHOT_CHUNK_BYTES: usize = 64 * 1024;

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

/// How many election timeouts a voter this leader saw report unable (full) and
/// then healthy must stay healthy before it counts in
/// [`RaftCore::healthy_followers`] (chaos `disk_full` F-4). A full disk regains
/// a sliver of space when the node's WAL rewrite frees its old file, and the
/// rewrite is retried on a backoff of 100 ms up to 2 s, so the flap period is
/// on the order of a second: one election timeout (150 ms by default) is far
/// too short to tell a flap from a recovery.
const SUSTAINED_HEALTH_ELECTION_TIMEOUTS: u64 = 20;

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
    /// something other than write rate: a peer's own `AppendEntries`
    /// success/reject response, an explicit `WakeRequest` poke, a fresh
    /// leadership term. NOT used for the ack-handler's own resend
    /// (`handle_install_snapshot_resp`) — see that method's own doc for why
    /// a bounded cap, not `Always`, is what belongs there. **NOT used for
    /// the periodic heartbeat tick either, as of the message-volume-over-
    /// time fix below** — see [`Backoff`](Self::Backoff)'s own doc for why
    /// "bounded by heartbeat cadence, not write rate" turned out not to be
    /// a real bound at all.
    Always,
    /// Exponential backoff on a resend of the SAME outstanding offset,
    /// keyed off a dedicated per-peer ATTEMPT counter
    /// (`snapshot_heartbeat_attempts`, not `snapshot_chunk_for`'s own
    /// `resends_so_far` — see that field's own doc for why): send while the
    /// attempt count is `0` or a power of two **below**
    /// [`SNAPSHOT_HEARTBEAT_BACKOFF_MAX_TICKS`], suppress every other call
    /// until that ceiling, then fall to a flat steady-state resend every
    /// `SNAPSHOT_HEARTBEAT_BACKOFF_MAX_TICKS`'th attempt forever after —
    /// never resuming the doubling. This is the periodic **heartbeat
    /// tick**'s own setting (`RaftCore::tick`'s `Role::Leader` branch) —
    /// the one call site `Always` used to cover on the (wrong) theory that
    /// a fixed per-tick rate was itself a bound. It isn't: a heartbeat
    /// tick fires forever, unconditionally, for as long as this node
    /// leads, regardless of whether the peer it's resending to can ever
    /// possibly catch up — a real network partition, a permanently wedged
    /// peer disk, or (`apply_and_compact`'s own documented capacity limit)
    /// a write rate that genuinely outpaces what the peer can ever absorb
    /// all leave a chunk "outstanding" indefinitely, and `Always` resent it
    /// at the FULL heartbeat rate (every `heartbeat_interval`, by default
    /// 50ms) for as long as that lasted — unbounded in elapsed time, not
    /// merely per-offset like every other site's own flood this ADR's
    /// amendments closed (confirmed live and reproduced directly:
    /// `animus-cp-data/tests/snapshot_heartbeat_resend_unbounded.rs` shipped
    /// ~20 chunks/second to a permanently partitioned peer for the entire
    /// length of an idle 60s window with zero decay). `Backoff` keeps the
    /// same genuinely-useful property `Always` was reached for — a stuck
    /// transfer is never permanently silenced, so a peer that later
    /// reconnects (or whose disk unwedges) is still eventually retried —
    /// while bounding the total volume shipped to a peer that never
    /// recovers to O(log(elapsed heartbeats)) instead of O(elapsed
    /// heartbeats): over a very long stuck episode, chunk N is shipped only
    /// once real progress last happened `2^(k)` heartbeats ago for the
    /// current `k` (until the ceiling), so total resends stay small (tens,
    /// not thousands) even across a multi-minute stall, and a chunk for a
    /// genuinely NEW offset (real progress) is, as with every other
    /// variant, never held back — the schedule always includes attempt
    /// `0`. A genuinely still-converging transfer is unaffected in
    /// practice: it advances the offset well before the attempt count
    /// climbs past the first few doublings, so the ack-handler's own
    /// `Capped(SNAPSHOT_ACK_RESEND_CAP)` resend (which fires on every ack,
    /// independent of the heartbeat cadence entirely) is what actually
    /// keeps a healthy transfer flowing between heartbeats — this only
    /// changes behavior once a transfer has gone genuinely idle for a
    /// while.
    ///
    /// **The ceiling itself — capping unbounded doubling, not merely the
    /// doubling's existence — is the fix this variant's own second
    /// revision added (review finding, 2026-09-27): without it, the GAP
    /// between resends grows without bound right along with the total
    /// count, so a peer stuck for an hour could wait up to another full
    /// hour-scale gap before its very next chunk, and thus that long again
    /// after it reconnects before catch-up can resume — see
    /// [`SNAPSHOT_HEARTBEAT_BACKOFF_MAX_TICKS`]'s own doc for the exact
    /// value chosen, why it's a deliberately-not-sub-election-timeout
    /// steady state, and why that's still sound.**
    Backoff,
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

/// Ceiling on `SnapshotResend::Backoff`'s exponential thinning (the
/// heartbeat tick's own schedule, `snapshot_chunk_for`'s own doc) — found
/// in review of the schedule's first cut, which doubled the gap between
/// resends forever with no ceiling at all. That is still a real bug even
/// though it bounds total volume within any FIXED window (doubling is
/// logarithmic, so `snapshot_heartbeat_resend_unbounded.rs`'s own 30s/30s
/// split-window measurement stays comfortably under its bound either way,
/// which is why that test alone doesn't catch this): the GAP between
/// consecutive resends grows without bound too, so a peer stuck for an
/// hour can end up waiting another full hour-scale gap before its next
/// chunk, and thus up to that long after it reconnects before catch-up can
/// resume — see `tests/snapshot_heartbeat_reconnect_latency.rs`. Once the
/// per-offset attempt count reaches this ceiling, the schedule stops
/// doubling and instead resends every `SNAPSHOT_HEARTBEAT_BACKOFF_MAX_
/// TICKS`'th attempt — a flat, bounded steady-state period regardless of
/// how long the stall has already run.
///
/// `32` (32 × the 50ms default `heartbeat_interval` ≈ 1.6s) trades off two
/// things pulling in opposite directions, and the choice here is
/// deliberate, not a rounding of "roughly 1-2s":
/// - **Bounding total volume over a long stall** wants this LARGE — a
///   multi-minute stuck episode should still ship only tens, not
///   thousands, of chunks (this ceiling's whole purpose).
/// - **A snapshot-mode peer's own election timer** wants this SMALL: while
///   a peer's `next_index <= snapshot_index`, `replicate_to` ships it
///   `InstallSnapshot` chunks and *never* a plain `AppendEntries` — so for
///   as long as it's in this mode, a resent chunk is this peer's *only*
///   leader-liveness signal (`handle_install_snapshot` resets the
///   election timer on receipt, exactly like `handle_append_entries`
///   does). At the default `election_base` (150ms, so a randomized
///   `[150, 300)` timeout), ANY steady cadence slower than that — 1.6s
///   very much included — leaves a genuinely-reachable-but-still-
///   snapshotting VOTER timing out and running pre-vote rounds between
///   chunks.
///
/// **The sound resolution is not to shrink this ceiling to sub-election-
/// timeout territory** — at `heartbeat_interval` = 50ms that would mean a
/// steady-state MAX_TICKS of 2-3, i.e. resending at very nearly the full
/// heartbeat rate, which reproduces almost exactly the unbounded-volume
/// flood this fix exists to close. **It is that pre-vote is already safe
/// against exactly this noise by construction (ADR 0009)**: a peer's own
/// spurious pre-vote round is refused by every OTHER follower as long as
/// they still see a live leader (`handle_pre_vote`'s grant conditions —
/// `role == Leader`, or `leader_id.is_some() && now < election_deadline`,
/// or a still-fresh `voted_for` lease), and pre-vote never bumps a term on
/// its own, so a snapshot-mode voter's repeated timeout-and-retry is
/// wasted local work, never real disruption: no term bump, no leadership
/// change, no effect on any other replica's view. A snapshot-mode replica
/// timing out between 1.6s-apart chunks is exactly this harmless-noise
/// case, proven by `tests/snapshot_heartbeat_no_disruption_during_backoff.rs`
/// (the leader's term never moves while a voter sits in backoff-throttled
/// catch-up for many seconds). The alternative that WOULD avoid the noise
/// — teaching `replicate_to` to also ship a plain, chunk-free
/// `AppendEntries`/heartbeat to a snapshot-mode peer on every tick,
/// independent of `Backoff`'s own suppression — was rejected: it is a
/// second, structurally new liveness channel for exactly one peer state,
/// doubling this mechanism's own surface for a cosmetic (not correctness)
/// win pre-vote already provides for free.
const SNAPSHOT_HEARTBEAT_BACKOFF_MAX_TICKS: u32 = 32;

/// Issue #898: how many CONSECUTIVE mid-transfer acks reporting an offset
/// below the currently tracked `snapshot_offset` this leader tolerates as
/// "probably a stale, reordered ack" before concluding the peer's own buffer
/// genuinely reset and rebasing down to match it — see
/// `snapshot_offset_regressions`'s own doc for the full mechanism and
/// incident. Small, matching `SNAPSHOT_ACK_RESEND_CAP`'s own order of
/// magnitude: large enough that an ordinary handful of reordered acks (the
/// scenario the monotonic guard exists for) never falsely triggers a rebase,
/// small enough that a genuinely reset peer recovers within a few heartbeat
/// intervals rather than staying deadlocked for the rest of the run.
const SNAPSHOT_OFFSET_REGRESSION_REBASE: u32 = 4;

/// Issue #1061: how long this leader keeps sending the removal-notice
/// schedule ([`RaftMsg::Removed`]) to a `departing` peer that has replied with
/// nothing at all before it stops (see [`RaftCore::expire_stale_departing`]).
///
/// Giving up is **not** the correctness mechanism for removal — the notice,
/// its ack, and the receiver-initiated reply to a returning stranger
/// (`RaftCore::stranger_notice`, which needs no leader-side memory at all)
/// are. This bound only caps (a) the traffic shipped to a peer that is
/// genuinely gone (a crashed node, or one whose own host already released
/// the replica) and (b) how long such a peer can hold the group out of
/// quiescence (`quiesce_entry_ok`'s `departing.is_empty()` clause).
///
/// It is deliberately a *sustained-silence* bound, measured from the peer's
/// last reply of any kind rather than from when it started departing, and
/// **long — five minutes**: the schedule it bounds is the capped
/// exponential one (`departing_send_gate`, a steady one tiny message per
/// ~1.6s), so a dead peer costs ~190 messages of a few dozen bytes over the
/// whole window and never a snapshot, while a peer partitioned for anything
/// short of minutes is still served by the schedule itself. A peer silent
/// past this bound that later returns is still told, by its own election
/// timer's pre-vote/vote being answered with the notice
/// (`stranger_notice`); the one gap left is a *quiesced* removed replica
/// that neither hears from the leader nor is ever touched locally, which
/// stays hosted, idle and timer-less, until touched or restarted (see
/// ADR 0058's issue #1061 amendment).
pub const DEPARTING_NOTICE_GIVE_UP: Duration = Duration::from_secs(300);

/// Issue #1061 follow-up (found live): the *shorter* sustained-silence bound
/// for a `departing` peer this leader has reason to expect never to answer
/// again — one that has already **acknowledged its removal notice**
/// (`RaftMsg::RemovedAck`), or one this leader merely **inherited** from a
/// predecessor through `become_leader`'s `removals_in_log` re-derivation.
///
/// Both kinds are, in practice, peers that have *left*: an acked peer has
/// recorded its removal and its host reconciler releases the replica the
/// moment replicated `Metadata` agrees; an inherited one was removed long
/// enough ago that a whole leadership change has happened since, and its
/// replica is overwhelmingly already gone. A released replica can never
/// reply, yet before this bound the leader kept addressing its closed stream
/// on the capped-backoff schedule (~37 frames a minute per peer) for the
/// *whole* [`DEPARTING_NOTICE_GIVE_UP`] (five minutes) — per removal, and
/// re-armed by *every* leadership change. Measured live (a 12-minute bulk
/// load then five idle minutes, `--cluster-control 3 --cluster-data 5`): one
/// such peer cost a node ~190 removal notices in the idle window, and the
/// maintainer's run saw ~200 frames a minute arriving for released replicas
/// (`demux_frames_dropped_closed`).
///
/// A peer that IS alive and still hosted answers the very first
/// `AppendEntries`/notice, and any reply resets the silence clock (`note_
/// departing_reply`), so this only ever cuts off a peer that stayed quiet;
/// one that returns after being cut off is still told by its own election
/// timer's pre-vote being answered with the notice (`stranger_notice`,
/// which needs no leader-side memory). A leader-initiated, not-yet-acked
/// removal of a peer this leader itself just removed keeps the long bound.
/// Comfortably above the steady `departing_send_gate` period (~1.6s) and any
/// election timeout.
pub const DEPARTING_QUIET_GIVE_UP: Duration = Duration::from_secs(30);

/// Lifetime removal-notice counters of one [`RaftCore`] (issue #1061
/// observability): a pure fact the driver folds into `/admin/metrics`
/// (`animus-cp-data` emits the deltas as `Metric::CpRemoval*`), so a live
/// operator can see notices being sent, acknowledged, ignored as stale, and
/// departing peers being dropped — the questions the first live check of
/// the notice could not answer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RemovalStats {
    /// `RaftMsg::Removed` notices this node emitted as a leader (to a
    /// departing peer through `replicate_to`, or to a returning stranger
    /// through `stranger_notice`).
    pub notices_sent: u64,
    /// `RaftMsg::RemovedAck`s that count: one covering the removing entry
    /// this leader was waiting on (whether or not it also ended the
    /// schedule), or one answering a notice sent to a non-member in reply to
    /// its own campaign.
    pub notices_acked: u64,
    /// Notices/acks discarded as stale: a peer-side `Removed` that failed one
    /// of `handle_removed`'s guards (older term, we are a member/leader, or
    /// not later than our own latest self-membership), and a leader-side ack
    /// that did not cover the entry it was waiting on.
    pub notices_ignored: u64,
    /// Departing peers this leader stopped serving (acked with an
    /// unservable log, caught up past the removing entry, or silent past its
    /// give-up bound).
    pub departing_dropped: u64,
}

/// The `(term, index)` stamp of a log entry, ordered lexicographically —
/// terms never decrease along a log, so for two entries of ONE history this
/// is the same order as index alone, while an entry from a *diverged*
/// (stale, uncommitted) suffix with a lower term than a committed entry
/// correctly orders before it even at a higher index. Used by the removal
/// notice's guards (`RaftCore::handle_removed`, `refresh_removed_flag`).
type EntryStamp = (u64, u64);

/// A peer this leader still owes a removal notification (`RaftCore::departing`):
/// the config entry that removed it, by index and term.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Departing {
    index: u64,
    term: u64,
}

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
        /// **Issue #1131.** `true` while the responder cannot vote yet because
        /// of its issue #667 boot-time cluster check: still unresolved
        /// ([`RaftCore::cluster_check_pending`]) or resolved to refused
        /// ([`RaftCore::refused_as_voter`]). Either way it refuses every vote
        /// and never campaigns (it cannot act as a voter). The
        /// leader records it per peer and refuses to promote a learner
        /// reporting `true` ([`RaftCore::learner_caught_up`]): promoting a
        /// voter that cannot vote can leave the group without a quorum of
        /// voters able to vote. `#[serde(default)]` so a peer that omits it
        /// decodes to `false` (no pending check, the pre-existing behavior).
        #[serde(default)]
        check_pending: bool,
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
    /// Issue #667 (P0 Raft safety): sent by a node whose persisted state
    /// replayed empty (`PersistedState::is_empty()`) to every configured
    /// peer, asking "have you (or has anyone you know of) ever recorded
    /// real state?" — the active half of the boot-time genesis-vs-wiped-
    /// restart check (see [`RaftCore::begin_cluster_check`]). Carries no
    /// payload: the honest answer is purely local to the responder.
    ClusterProbe,
    /// Response to [`RaftMsg::ClusterProbe`]: the responder's own current
    /// term, commit index, **committed voter config**, and whether the
    /// responder has **ever itself received a real protocol message from
    /// the asker** (`ever_heard_from_prober`), reported honestly regardless
    /// of whether the responder is itself still mid-check. `term > 0 ||
    /// committed_index > 0` is conclusive proof this responder (and thus the
    /// cluster it belongs to) has real history — the asker was never told
    /// to assume otherwise on a `0`/`0` reply from any *one* peer, only once
    /// every configured peer has answered `0`/`0` (see
    /// `begin_cluster_check`).
    ///
    /// **Two independent signals disambiguate a wiped-voter restart from an
    /// identity that has simply never voted before** — both an ordinary ADR
    /// 0060 growth join AND a genesis founder still mid its own boot-time
    /// check look identical to the asker's own empty local state, but
    /// neither could possibly have cast a now-forgotten real vote under this
    /// identity, so proceeding as an ordinary fresh voter is safe for both:
    ///
    /// - `!config.contains(asker)` — the responder's *committed* config
    ///   doesn't (yet) recognize the asker as one of its voters at all. The
    ///   original signal (ADR 0060): pre-vote's own log check is what keeps
    ///   this case safe, unchanged.
    /// - `ever_heard_from_prober` — whether the responder has *itself* ever
    ///   received any real consensus-protocol message (a vote request/
    ///   response, an append, a snapshot chunk — anything but this very
    ///   probe/response pair) from the asker, ever, since this responder's
    ///   own process started. **Added 2026-09-15** to close a real gap the
    ///   `config.contains` signal alone cannot: in a genuine N-node genesis
    ///   (every founder listed in `config` from birth, by construction), a
    ///   still-checking founder's config membership is unconditionally
    ///   `true` from the very first committed entry onward, so a majority
    ///   that elects among itself before a slower founder's own check
    ///   resolves looks — by `config.contains` alone — indistinguishable
    ///   from a genuinely established, long-running voter whose disk was
    ///   wiped. **Third amendment, same day**: this signal is folded into
    ///   the SAME wait-for-every-peer aggregation `config.contains`'s
    ///   established verdict already uses, rather than being decisive on a
    ///   single `false` reply — a still-checking founder has sent no peer
    ///   any real protocol message yet, so it is guaranteed `false` from
    ///   EVERY peer, but the converse is not true: a perfectly ordinary
    ///   established follower that has never itself been a candidate or
    ///   leader only ever exchanges real protocol messages with whichever
    ///   peer *is* the candidate/leader, never with a fellow follower (and
    ///   the leader does not mark this on receiving a plain
    ///   `AppendEntriesResp` either), so two long-established, healthy
    ///   follower peers can go their entire lives never marking each other
    ///   — a genuinely wiped voter's fellow follower will honestly answer
    ///   `false` even though the cluster is real, reproducibly (not
    ///   intermittently) defeating the refusal if treated as decisive on
    ///   its own (found via a real, deterministic `ProdEnv` failure in
    ///   `wiped_voter_refuses_and_the_rest_of_the_cluster_keeps_serving`).
    ///   See `RaftCore::handle_cluster_probe_resp`'s doc for the full
    ///   decision table and `docs/adr/0009-*.md`'s matching amendment for
    ///   the design record.
    ///   **Known residual**: this signal is per-process, in-memory, not
    ///   WAL-durable — a responder that itself restarts (recovered, not
    ///   wiped) forgets it until the prober sends it another real message,
    ///   so a coordinated whole-cluster restart racing a single voter's
    ///   disk wipe is not fully covered by this signal alone (unchanged
    ///   from before this amendment — no prior mechanism covered it
    ///   either). The common case this feature targets — one voter's disk
    ///   wiped while its peers keep running — is fully covered.
    ClusterProbeResp {
        term: u64,
        committed_index: u64,
        config: BTreeSet<NodeId>,
        ever_heard_from_prober: bool,
    },
    /// **Issue #1061 — an explicit removal notice.** Sent by a leader to a
    /// peer that a *committed* config entry removed, when the peer can no
    /// longer be told through the log (its `next_index` is behind the
    /// leader's compacted prefix — it would otherwise get a full
    /// `InstallSnapshot`, which a departing peer must never be shipped), and
    /// answered to a returning non-member's pre-vote/vote. It carries no
    /// log content: `removal_index`/`removal_term` identify the committed
    /// config entry that removed the recipient (or the leader's newest
    /// committed config entry, which excludes it too), and `config`/
    /// `learners` are the leader's current membership, which must exclude
    /// the recipient.
    ///
    /// The recipient marks itself removed (`RaftCore::removed_by_leader`) —
    /// it stops campaigning/granting exactly like a learner and its host
    /// reconciler may release it once the replicated `Metadata` also
    /// excludes it — but only if `(removal_term, removal_index)` is later
    /// than every config entry it knows that *includes* itself, so a
    /// delayed notice can never un-member a node re-added since. Never
    /// touches the recipient's log or its log-derived config. See
    /// `RaftCore::handle_removed` for the full safety argument.
    Removed {
        term: u64,
        removal_index: u64,
        removal_term: u64,
        config: BTreeSet<NodeId>,
        learners: BTreeSet<NodeId>,
    },
    /// Response to [`RaftMsg::Removed`]: the recipient has recorded its
    /// removal (or already knew). `removal_index` echoes the notice. A
    /// stale-term notice is answered with the recipient's own (higher)
    /// `term` and `removal_index: 0` so the stale sender learns it is behind
    /// and steps down; the leader ignores any ack that does not cover the
    /// entry it is waiting on.
    RemovedAck { term: u64, removal_index: u64 },
}

impl<C> RaftMsg<C> {
    /// ADR 0073 Phase 2 (P2-B): the gate the **message itself** needs,
    /// *excluding* the commands carried by an `AppendEntries`. Exhaustive,
    /// no `_` arm: a new variant does not compile until it names its gate.
    ///
    /// Every variant that exists at cluster version 1 is [`Gate::Base`].
    /// `InstallSnapshot` data is opaque bytes here; its *content* is gated by
    /// the encoder of the image it carries (`CSN1`, `raftkv-image`), not by
    /// this envelope.
    ///
    /// Why entries are excluded: an `AppendEntries`' entry gates cannot be
    /// checked soundly at the send site. A leader's applied view (which feeds
    /// `ClusterFeatures`) lags its log, era-start entries ship before they
    /// apply, and a new leader resends entries proposed earlier under a gate
    /// that was open then (gates only ever open). Entry gates are enforced at
    /// the *propose* site instead (see [`RaftMsg::required_gate`] for the
    /// full-message gate used by tests and receivers).
    #[must_use]
    pub fn envelope_gate(&self) -> crate::version::Gate {
        use crate::version::Gate;
        match self {
            RaftMsg::PreVote { .. }
            | RaftMsg::PreVoteResp { .. }
            | RaftMsg::RequestVote { .. }
            | RaftMsg::RequestVoteResp { .. }
            | RaftMsg::AppendEntries { .. }
            | RaftMsg::AppendEntriesResp { .. }
            | RaftMsg::InstallSnapshot { .. }
            | RaftMsg::InstallSnapshotResp { .. }
            | RaftMsg::Heartbeat { .. }
            | RaftMsg::TimeoutNow { .. }
            | RaftMsg::Quiesce { .. }
            | RaftMsg::WakeRequest { .. }
            | RaftMsg::ClusterProbe
            | RaftMsg::ClusterProbeResp { .. }
            | RaftMsg::Removed { .. }
            | RaftMsg::RemovedAck { .. } => Gate::Base,
        }
    }

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
            | RaftMsg::Quiesce { term, .. }
            | RaftMsg::Removed { term, .. }
            | RaftMsg::RemovedAck { term, .. } => *term,
            RaftMsg::Heartbeat { .. } | RaftMsg::WakeRequest { .. } => 0,
            // Issue #667: a cluster-check probe carries no term *authority*
            // either — its whole point is to be answerable (and answered
            // honestly) independent of the responder's own term-stepdown
            // state, exactly like `Heartbeat`/`WakeRequest` above. Its real
            // payload (the responder's term/commit) is read explicitly by
            // `handle_cluster_probe_resp`, never via this generic extractor.
            RaftMsg::ClusterProbe | RaftMsg::ClusterProbeResp { .. } => 0,
        }
    }
}

/// The full-message gate, which also covers the commands an `AppendEntries`
/// carries. A **separate, bounded** impl on purpose: `RaftCore<C, S>` and the
/// toy commands in tests stay unbounded.
impl<C: crate::version::GatedCommand> RaftMsg<C> {
    /// [`envelope_gate`](RaftMsg::envelope_gate) joined with the gate of every
    /// entry command of an `AppendEntries` (a no-op or membership-change
    /// entry still carries a `command`, which is gated like any other).
    #[must_use]
    pub fn required_gate(&self) -> crate::version::Gate {
        let envelope = self.envelope_gate();
        match self {
            RaftMsg::AppendEntries { entries, .. } => entries
                .iter()
                .fold(envelope, |g, e| g.join(e.command.required_gate())),
            RaftMsg::PreVote { .. }
            | RaftMsg::PreVoteResp { .. }
            | RaftMsg::RequestVote { .. }
            | RaftMsg::RequestVoteResp { .. }
            | RaftMsg::AppendEntriesResp { .. }
            | RaftMsg::InstallSnapshot { .. }
            | RaftMsg::InstallSnapshotResp { .. }
            | RaftMsg::Heartbeat { .. }
            | RaftMsg::TimeoutNow { .. }
            | RaftMsg::Quiesce { .. }
            | RaftMsg::WakeRequest { .. }
            | RaftMsg::ClusterProbe
            | RaftMsg::ClusterProbeResp { .. }
            | RaftMsg::Removed { .. }
            | RaftMsg::RemovedAck { .. } => envelope,
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

    // Every distinct `(config, learners)` pair this core has adopted, in
    // adoption order (issue #944) — a small bounded ring, appended
    // synchronously inside `apply_config`, the ONE place a real transition
    // happens: both a leader's own local `propose`/`add_learner`/
    // `promote_learner` call (synchronous, on whatever task calls it) and a
    // follower's own per-entry `log_append` (looping over a batched
    // `AppendEntries`'s entries) funnel through it. A caller sampling
    // `config()`/`learners()` from OUTSIDE, on any cadence — even "once per
    // consensus-loop iteration" — can coalesce two back-to-back transitions
    // the same scheduling gap applies both of, silently skipping the
    // intermediate; recording at the mutation site itself cannot. See
    // `crates/animusd/tests/learner_reconfigure.rs`'s
    // `spare_replacement_passes_through_an_observable_learner_state_and_keeps_serving`
    // for the flake this closes, and `config_history`'s own accessor doc
    // for the read side. Deliberately **not** seeded at fresh construction
    // (`RaftCore::new` never calls `apply_config`, so a brand-new group's
    // ring starts empty until its first real change) — a restart's
    // recovery (`recovered`'s `recompute_config` call) seeds exactly one
    // entry, the just-recovered state, mirroring `VoterHistory`'s own
    // "restart starts fresh" discipline in `animus-cp-data`.
    config_history: std::collections::VecDeque<(BTreeSet<NodeId>, BTreeSet<NodeId>)>,

    role: Role,
    current_term: u64,
    voted_for: Option<NodeId>,
    // Issue #1019: a **liveness hint, not hard state** — deliberately never
    // persisted (no WAL record, no `PersistedState` field), unlike
    // `voted_for` itself. A VOTER's own vote-lease protection
    // (`handle_pre_vote`'s `voted_lease`) decays for free once its election
    // timer fires, because firing transitions it away from `Follower`/
    // `Candidate` into `PreCandidate` — `voted_lease`'s own role check is
    // what ends the protection, not a `voted_for` mutation (see
    // `start_pre_vote`'s doc). A non-voter (`!is_voter()`) can never make
    // that transition, so without a separate signal its lease would
    // protect a real vote it once granted FOREVER, even once the candidate
    // it voted for has been dead for many election timeouts — reinstating
    // issue #1019's deadlock through the `voted_lease` door instead of the
    // `is_voter()`-on-granting door the primary fix closed. This flag is
    // that non-voter-only substitute signal: `start_pre_vote`'s `!self.
    // is_voter()` early-return branch sets it `true` (mirrors clearing
    // `leader_id` there, but for the vote lease instead of the leader
    // belief), and it is reset `false` at every site that legitimately
    // re-arms the protection (a fresh real-vote grant or self-vote) or
    // that already independently clears `voted_for` on a term change.
    // **Deliberately does NOT clear `voted_for` itself** — `voted_for` is
    // real Raft hard state whose whole job is "at most one real vote per
    // term," and the candidate-side tally (`self.config.contains(&from)`)
    // does NOT make clearing it safe: in exactly the scenario this exists
    // for, the responder IS already a voter in the CANDIDATES' own
    // (majority-committed) config — only the responder's OWN view is
    // stale — so a cleared `voted_for` really could let this node grant
    // two different real candidates a vote in the same term, letting two
    // of them each reach a majority and producing two leaders in one term.
    // The vote itself stays recorded; only the PRE-VOTE lease (a liveness
    // optimization, never a safety mechanism) is allowed to lapse.
    vote_lease_lapsed: bool,
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
    // Issue #1228: sticky companion of `last_leader_contact` -- `true` once this
    // process has ever had a genuine leader contact (an `AppendEntries`/
    // `InstallSnapshot` from a leader, or its own election win). Unlike
    // `last_leader_contact` it is never cleared by a higher-term step-down. Read by
    // the eventual-read gate for a storage-full replica; volatile by design.
    had_leader_contact: bool,

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
    // Leader-only, volatile (issue #1131): each peer's own latest-reported
    // `cluster_check_pending` (echoed on its `AppendEntriesResp`). ABSENT
    // means "has not reported to this leader yet" and is treated as pending
    // by `learner_caught_up` — conservative, since it only delays a
    // promotion. Cleared on `become_leader` and on any membership change that
    // (re)introduces or drops the peer, exactly like `match_index`.
    peer_check_pending: BTreeMap<NodeId, bool>,
    // Leader-only, volatile (issue #1228, chaos `disk_full` F-4): the `now` at
    // which each peer last flipped from reporting `check_pending == true`
    // (full / unable to vote) to `false` within this leader's stint. A peer
    // that reported unable and then healthy is not trusted as a handoff
    // target or quorum member until it has stayed healthy for
    // `SUSTAINED_HEALTH_ELECTION_TIMEOUTS` election timeouts
    // (`healthy_followers`): a full disk regains a sliver of space from its
    // own WAL rewrite / compaction and then loses it again on the next write.
    // ABSENT means "never observed unhealthy" and is trusted. Cleared with
    // `peer_check_pending`.
    peer_recovered_at: BTreeMap<NodeId, Nanos>,
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
    // The `now` at which THIS leadership stint began (issue #923): set once in
    // `become_leader`, read back only through [`leader_since`](Self::
    // leader_since), which additionally gates on `role == Leader` so a
    // stepped-down node reads `None` with no explicit clearing needed
    // elsewhere — mirrors `next_index`/`match_index`/`last_contact`'s own
    // "meaningless once not leader, naturally overwritten on the next
    // `become_leader`" volatility; never persisted or snapshotted. Exists so
    // `RaftNode::control_peer_believed_alive` can tell "this leader has never
    // heard a GENUINE ack from this peer because it only just took over"
    // apart from "this leader has been up for a while and this peer has gone
    // properly silent" — `last_contact`'s own per-peer seed in `become_leader`
    // makes both cases look byte-identical (a stamp at `now`, aging out after
    // the same `CONTROL_PEER_LIVENESS_TIMEOUT`), which is exactly the
    // false-dead race issue #923 hit: a fresh leader's first real heartbeat
    // round can legitimately take longer than that steady-state per-peer
    // timeout under load (post-election processing, everyone's own scheduler
    // contention right after a disruptive leadership change).
    leader_since: Option<Nanos>,
    // Issue #667 (2026-09-15 amendment): every peer id this node has ever
    // witnessed casting a GENUINE, durably-forgettable vote — i.e. a
    // `voted_for` write this identity's own disk could later lose. Marked
    // ONLY at the three sites that represent exactly that: this peer
    // requesting a vote as a candidate (`handle_request_vote`, a self-vote
    // regardless of whether we grant it), this peer granting us a REAL vote
    // (`handle_vote_resp`, `granted: true` only — a rejection sets no
    // `voted_for` and proves nothing forgettable), and this peer appearing
    // as `leader` in `AppendEntries`/`InstallSnapshot` (proof it previously
    // won a real election, which required it to self-vote to become a
    // candidate in the first place). Deliberately NOT marked on
    // `PreVote`/`PreVoteResp` (a pre-vote round never touches `voted_for`/
    // `current_term` by design — nothing forgettable happens), nor on
    // `AppendEntriesResp`/`InstallSnapshotResp` (a plain follower accepting
    // a leader's log has never itself cast a vote, so it poses no
    // double-vote risk even after a wipe), nor — critically — on a
    // REJECTED `RequestVoteResp`/being the target of someone's `RequestVote`
    // (a candidate that never wins doesn't durably record anyone's grant).
    // **A real bug this precision fixed**: an earlier, broader version of
    // this field marked ANY non-probe message, including a REJECTION this
    // node itself sends back to a peer's `RequestVote` WHILE still
    // `cluster_check_pending` (a still-checking node still honestly answers
    // vote requests, just always rejecting) — that rejection is real wire
    // traffic but represents no forgettable state on the SENDER's part, and
    // marking it reproduced the exact genesis-race false refusal this
    // amendment exists to fix (a 2-node genesis: n1 resolves first, starts
    // campaigning, n0 rejects n1's `RequestVote` since it's still checking,
    // n1's own `heard_from` then wrongly contains n0). Purely additive to
    // `last_contact` (leader-only, narrower — only `AppendEntriesResp` —
    // and keyed to liveness, not vote history) — this exists solely to
    // answer a peer's `ClusterProbe` honestly (`handle_cluster_probe`'s
    // `ever_heard_from_prober`). See `RaftMsg::ClusterProbeResp`'s own doc
    // for why this closes the genesis-race gap `config.contains` alone
    // cannot, and its "Known residual" note for why this is deliberately
    // in-memory, not WAL-persisted. Never pruned (a control/data-plane
    // group's peer set is small and bounded by its own configured
    // membership).
    heard_from: BTreeSet<NodeId>,
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
    //
    // **Issue #1061: no longer "cleared on every election win and
    // forgotten".** `become_leader` now re-derives this map from the config
    // entries still in the leader's own log (`removals_in_log`), so a peer the
    // previous leader never finished notifying is still owed its notice by
    // the next one. It is also no longer replicated to through a snapshot:
    // a departing peer whose `next_index` fell behind the compacted prefix
    // is sent a `RaftMsg::Removed` notice instead (`replicate_to`), and is
    // dropped from this map on the peer's `RemovedAck`, on its `match_index`
    // reaching the removing entry, or after `DEPARTING_NOTICE_GIVE_UP` of
    // total silence (`expire_stale_departing`).
    departing: BTreeMap<NodeId, Departing>,
    // Issue #1061: the last time (leader-local, volatile — same class as
    // `last_contact`) each currently-departing peer proved it is alive by
    // sending ANY reply — `AppendEntriesResp` or `InstallSnapshotResp`,
    // success or reject. Seeded lazily by `expire_stale_departing` the first
    // time it sees a peer in `departing` with no entry here (`log_append`,
    // where `departing` is populated, has no `now`, and runs on every replica
    // during WAL replay), so a peer absent here is always read as "just
    // started, not yet stale". Pruned everywhere `departing` shrinks.
    departing_since: BTreeMap<NodeId, Nanos>,
    // Issue #1061 follow-up: the departing peers on the SHORT silence bound
    // (`DEPARTING_QUIET_GIVE_UP` rather than `DEPARTING_NOTICE_GIVE_UP`):
    // those that already acked their removal notice yet are still owed
    // catch-up `AppendEntries` (the log could serve them), and those this
    // leader merely inherited from a predecessor (`become_leader`'s
    // re-derivation). Pruned everywhere `departing` shrinks.
    departing_quiet: BTreeSet<NodeId>,
    // Issue #1061 observability: lifetime counters (`RemovalStats`).
    removal_stats: RemovalStats,
    // Issue #1061: per-departing-peer send schedule — `(next_allowed_send,
    // attempts)`, gating EVERY message this leader would send to a
    // departing peer (its removal notice and any catch-up `AppendEntries`
    // alike; `departing_send_gate`). The first send is immediate; each
    // further send with no reply from the peer in between waits twice as
    // long as the last (from one heartbeat interval, doubling, up to
    // `SNAPSHOT_HEARTBEAT_BACKOFF_MAX_TICKS` heartbeats — the same capped
    // steady period as the snapshot-resend schedule), and ANY reply from the
    // peer resets it (a live peer catching up is served at full rate; a dead
    // one costs a couple of dozen tiny messages a minute). Time-based, not
    // tick-count-based, because under a sustained writer `replicate_now`
    // keeps pushing `heartbeat_deadline` out and the heartbeat tick that a
    // count-based schedule would advance on never fires. Cleared with
    // `departing`.
    removal_sched: BTreeMap<NodeId, (Nanos, u32)>,
    // Issue #1061: the most recent `now` this core was driven with (`tick`,
    // `handle`, `replicate_now`) — `replicate_to` has no `now` parameter and
    // its many call sites should not all grow one just for the departing
    // schedule above.
    now_hint: Nanos,
    // Issue #1061 (peer side): set when a valid `RaftMsg::Removed` told this
    // node a committed config entry — stamped `(term, index)` — removed it.
    // While `Some`, `is_voter()` is false: the node never campaigns, and (via
    // the same `!is_voter()` branches a learner takes) stops vouching for a
    // leader/lease it no longer hears from. **Volatile and never applied to
    // `config`/`learners`/the log** — those stay a pure function of the log
    // and snapshot, so log matching, truncation and `recompute_config` are
    // untouched. Cleared (`refresh_removed_flag`) the moment a config entry
    // or snapshot that INCLUDES this node and is later than this stamp lands
    // — i.e. it was re-added. See `handle_removed`'s doc for the safety
    // argument.
    removed_by_leader: Option<EntryStamp>,
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
    // Issue #898: per-peer count of CONSECUTIVE mid-transfer acks reporting an
    // offset strictly below the currently tracked `snapshot_offset` — the
    // monotonic guard's own "is this a stale, reordered ack or a peer whose
    // buffer genuinely reset" ambiguity (see `handle_install_snapshot_resp`'s
    // doc). A transient reordering self-heals within a round trip or two (the
    // peer's very next ack, for whatever legitimately arrives next, reports a
    // LARGER offset than any stale one, so this resets to 0 the moment real
    // forward progress is seen); a peer that genuinely lost its buffer (a
    // real process restart mid-transfer, same `NodeId`, `snapshot_offset`
    // never told about it) reports the SAME regressed value — 0, from a
    // fresh `RaftCore` — forever, since it can never accept a chunk at a
    // nonzero offset with an empty buffer (`handle_install_snapshot`'s
    // `fresh && offset == 0` reassembly gate). Left unaddressed, the leader
    // keeps resending from its own stale, now-unreachable offset
    // indefinitely — confirmed live building this fix:
    // `chunked_snapshot_receiver_stop_restart_3` deadlocked exactly this way
    // once `meta_apply_and_compact`'s new compaction-defer gate (also this
    // issue) stopped the ordinary threshold-triggered recompaction that used
    // to incidentally wipe this bookkeeping clean before the peer's next
    // request. Once this counter crosses `SNAPSHOT_OFFSET_REGRESSION_REBASE`,
    // `handle_install_snapshot_resp` REBASES `snapshot_offset`/
    // `snapshot_chunk_sent` down to the peer's own reported (lower) truth
    // instead of taking `max`, which lets a fresh chunk-0 ship land on a
    // fresh, empty buffer correctly. Cleared at the identical points
    // `snapshot_offset` itself is.
    snapshot_offset_regressions: BTreeMap<NodeId, u32>,
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
    // Per-peer `snapshot_index` (the base) the most recent `InstallSnapshot`
    // chunk to that peer was built at — written at every send
    // (`snapshot_chunk_for`), read only by `handle_install_snapshot_resp`'s
    // *declined offer* branch (a receiver that answers the very first chunk
    // of an offer with "nothing buffered" already holds state through this
    // base). Deliberately not cleared alongside `snapshot_chunk_sent`: it is
    // only ever read while that map still has the peer, and every send that
    // repopulates that map overwrites this too.
    snapshot_offer_base: BTreeMap<NodeId, u64>,
    // Per-peer `(offset, attempts)` for `SnapshotResend::Backoff`'s own
    // exponential-thinning schedule (the heartbeat tick's policy — see that
    // variant's own doc). Deliberately a SEPARATE counter from
    // `snapshot_chunk_sent`'s `resends` above: that one counts only chunks
    // actually SENT, so a suppressed call never advances it — keying the
    // backoff schedule off it directly would freeze the schedule forever at
    // whatever count first got suppressed (found building this fix: a
    // resend suppressed at count 3 never reaches count 4, the next allowed
    // power of two, since only a successful send increments it). This
    // field instead counts every heartbeat-tick ATTEMPT for the current
    // offset, sent or suppressed alike, so the schedule genuinely advances
    // tick over tick. Reset to `(offset, 0)` whenever the offset itself
    // changes (genuine progress restarts the backoff); cleared at the
    // identical points `snapshot_chunk_sent` itself is.
    snapshot_heartbeat_attempts: BTreeMap<NodeId, (u64, u32)>,
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
    // Every constructor starts at the LAN pair (150ms / 50ms). Issue #313
    // removed the dead `set_election_timeout` setter (zero call sites);
    // ADR 0075 section 3.4 (roadmap G-01 stage G-c) adds back ONE narrow,
    // used replacement, `set_timing`, whose callers pick the pair from
    // `crate::timing::TimingProfile` (a group spanning regions gets the WAN
    // pair). See `election_timeout()` for the read-only accessor, used by
    // `transfer_leadership`'s deadline and by driver-side observability.
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
    // Bytes of serialized snapshot carried by a single `InstallSnapshot`
    // message — see [`SNAPSHOT_CHUNK_BYTES`]'s own doc for the default and
    // [`set_snapshot_chunk_bytes`](Self::set_snapshot_chunk_bytes) for why
    // this is a per-instance, overridable value rather than the constant
    // used directly.
    snapshot_chunk_bytes: usize,
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
    // This node is **out of disk** (R-01 (d), ADR 0074 §2 / issue #1219): its
    // driver's WAL is suspect after an ENOSPC, or its apply task is paused on
    // an engine ENOSPC. Such a node cannot make anything durable, so it must
    // not campaign (a won election would only churn leadership onto a node
    // that refuses every write) and ignores `TimeoutNow`. Fed LIVE by the
    // driver every loop iteration via
    // [`set_storage_full`](Self::set_storage_full); default `false`, and
    // always `false` on the in-core control plane.
    storage_full: bool,
    // The `last_index` of the most recently fully-received `InstallSnapshot`
    // THIS node installed, this process lifetime (never persisted — `None`
    // on every fresh/recovered core, exactly like `incoming_snapshot`
    // itself). Distinct from `snapshot_index`/`last_applied` (which a
    // recovered-from-WAL core can carry forward from a PRIOR lifetime, and
    // which `handle_install_snapshot`'s own top-of-function short-circuit
    // deliberately does NOT trust while `state_machine_behind` — see that
    // check's own doc for the wipe-recovery case it exists to catch).
    //
    // **Closes a duplicate-reprocessing gap in that same short-circuit**
    // (found live building `SNAPSHOT_CHUNK_BYTES`'s bump): while
    // `state_machine_behind` is true, `handle_install_snapshot` falls
    // through UNCONDITIONALLY to the normal reassembly path — correct for a
    // wiped engine (the top-of-function doc's own reasoning: an offer at
    // the SAME `snapshot_index`/`last_applied` this node's intact log
    // already reflects is exactly the case the wipe fix must not discard).
    // But under a sustained write load, a leader's own `SnapshotResend::
    // Always` resends the SAME already-fully-received final chunk on every
    // wake before it has processed this node's first completion ack (a real
    // round trip, not instant) — and every one of those, while this node is
    // still digesting the FIRST copy, re-enters the "fresh" branch (`self.
    // incoming_snapshot` was already cleared by the completion) and
    // re-completes from scratch: another genuine-looking `InstallSnapshotResp
    // { last_index > 0, .. }`, inflating `Metric::CpSnapshotInstalls` and
    // forcing another full WAL rewrite, for data this node already has.
    // Correctness was never at risk (reinstalling identical bytes is a
    // no-op either way — the top-of-function doc's own point) but the
    // *count* and the repeated WAL rewrite are real, measured cost (up to
    // ~140 redundant re-installs of one 149-entry image for a single
    // merely-8ms-slowed voter, `docs/engineering-lessons.md`'s matching
    // entry). Recording the last EXACT `last_index` actually installed and
    // short-circuiting an identical re-offer regardless of
    // `state_machine_behind` closes this while leaving the wipe case
    // untouched: a freshly recovered core's `last_installed_index` is
    // `None` this lifetime, so its first offer — at whatever index — always
    // falls through exactly as before.
    last_installed_index: Option<u64>,

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

    // Issue #667 (P0 Raft safety): boot-time "am I a wiped voter restarting
    // into an already-established cluster, or is this a genuine fresh
    // bootstrap?" check. `None` (every existing construction path — `new`
    // and `recovered` both set this `None`) means "not applicable": this
    // core's own persisted state was non-empty at recovery (so its own
    // term/`voted_for` are trustworthy), or the caller never opted into the
    // check at all (most unit/test construction). Only the driver's own
    // `begin_cluster_check` (called exactly when a node's WAL replayed to
    // `PersistedState::is_empty()`, `node.rs`'s `drive`) ever populates
    // this. See `begin_cluster_check`'s own doc for the full mechanism.
    cluster_check_pending: Option<BTreeSet<NodeId>>,
    // Issue #667 amendment (a second, real-`ProdEnv` regression, found via
    // `dynamo_txn_idempotency.rs::same_token_same_fingerprint_retry_after_
    // commit_is_cached` timing out under real CI load): the ORIGINAL
    // design resent a still-pending probe round only from `start_pre_vote`'s
    // own early-return arm, itself gated on `now >= election_deadline` —
    // sharing `election_deadline` with the ordinary election timer. That
    // sharing is unsound: `handle_append_entries` legitimately RESETS
    // `election_deadline` on every valid leader contact (the standard Raft
    // "don't campaign against a live leader" behavior), and a still-pending
    // founder DOES receive ordinary `AppendEntries`/heartbeat traffic from
    // an already-elected SIBLING founder (a real leader broadcasts to every
    // configured peer, voter or not, resolved or not) the moment a majority
    // of founders elect one among themselves. Once that starts, the pending
    // founder's own `election_deadline` never again reaches its resend
    // check — it is perpetually pushed back by legitimate heartbeats — so
    // its own probe (lost, or answered by a peer that hadn't yet replied)
    // is never retried, and the founder can wait past any real test/
    // deployment timeout with no forward progress at all. This field is a
    // SEPARATE deadline, armed by `begin_cluster_check` and advanced only
    // by the dedicated resend check at the top of `tick()` — untouched by
    // `handle_append_entries` or any other ordinary message handler, so it
    // keeps firing on schedule regardless of how much legitimate leader
    // traffic this node receives while still pending. `None` whenever
    // `cluster_check_pending` is `None` (not applicable, or already
    // resolved).
    cluster_check_resend_deadline: Option<Nanos>,
    // Issue #667 amendment (real-cluster bootstrap-race regression, found
    // via `forward_to_tablet_leader_survives_a_dead_first_guess`'s own
    // flaky failure under real `ProdEnv` threading): whether ANY peer has
    // so far answered this boot's cluster check with real history
    // (`term > 0 || committed_index > 0`) that also names this node in its
    // own committed config **and** itself genuinely heard from this
    // identity before (`ever_heard_from_prober`, added by the second
    // amendment, folded into this same aggregated flag by the third —
    // see `handle_cluster_probe_resp`'s doc for why a peer lacking that
    // last part is not decisive either way on its own, and why requiring
    // it from ANY one peer rather than ALL of them is what makes this
    // sound for an ordinary follower-follower pair that never directly
    // exchanged a message). Recorded, never acted on immediately — see
    // `handle_cluster_probe_resp`'s own doc for why a SINGLE such answer is
    // not, by itself, trustworthy evidence of a genuine wiped-voter restart
    // (a real N-node genesis bootstrap can have a majority-of-peers elect a
    // leader before every founding peer's own probe round has completed,
    // which looks byte-identical to this signal from the still-forming
    // peer's own point of view). Reset to `false` whenever a fresh
    // `begin_cluster_check` round starts (there is only ever one per
    // `RaftCore` lifetime today, but resetting is cheap and correct
    // regardless).
    cluster_check_saw_established_with_me: bool,
    // Sticky: `cluster_check_pending` resolved with every configured peer
    // accounted for, at least one showing real history that named this
    // node as an established voter, and **none** showing genuinely fresh
    // (`0`/`0`) state — the combination `handle_cluster_probe_resp`'s own
    // doc explains is only possible for a genuinely wiped, previously-
    // established voter, never an ordinary still-forming genesis bootstrap.
    // Sticks for this `RaftCore`'s whole lifetime — there is no path that
    // clears it, by design (ADR 0009's amendment): an already-established
    // voter identity whose disk was wiped never becomes safe to
    // vote/campaign as again just by waiting or catching up on
    // replication; it must be re-admitted through the learner/rejoin path
    // (ADR 0032/0058) as a genuinely new membership event.
    cluster_check_refused: bool,
    /// Optional human-readable label for the group this core serves (e.g.
    /// `"tablet 7"`), set by a multi-group driver (the CP-data tablet
    /// driver) so log lines that would otherwise be indistinguishable
    /// across groups — the `cluster_check_refused` ERROR most of all — name
    /// their group. Purely diagnostic: never read by any protocol decision.
    /// `None` for the control group itself.
    group_label: Option<String>,
    /// Issue #1229: the state machine's base state (index 0) was seeded
    /// **outside the log** (an in-place split child's engine is cloned from
    /// its parent's, ADR 0058), so while `snapshot_index == 0` the log does
    /// NOT reproduce the state machine and replaying it from entry 1 would
    /// give a brand-new replica only the post-fork writes. While set, a
    /// learner is never sent `AppendEntries` until a snapshot base exists —
    /// it gets the engine image instead. Purely local, never persisted
    /// (re-derived from the engine at driver start); `false` for every
    /// ordinary group.
    log_omits_base: bool,
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
            config_history: std::collections::VecDeque::new(),
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            vote_lease_lapsed: false,
            log: Vec::new(),
            snapshot_index: 0,
            snapshot_term: 0,
            commit_index: 0,
            last_applied: 0,
            durable_index: 0,
            leader_id: None,
            last_leader_contact: None,
            had_leader_contact: false,
            pre_votes: BTreeSet::new(),
            votes: BTreeSet::new(),
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            snapshot_served_through: BTreeMap::new(),
            peer_check_pending: BTreeMap::new(),
            peer_recovered_at: BTreeMap::new(),
            last_contact: BTreeMap::new(),
            leader_since: None,
            heard_from: BTreeSet::new(),
            departing: BTreeMap::new(),
            departing_since: BTreeMap::new(),
            departing_quiet: BTreeSet::new(),
            removal_stats: RemovalStats::default(),
            removal_sched: BTreeMap::new(),
            now_hint: Nanos(0),
            removed_by_leader: None,
            transfer_target: None,
            transfer_deadline: Nanos(0),
            first_term_index: 0,
            snapshot_offset: BTreeMap::new(),
            snapshot_offset_regressions: BTreeMap::new(),
            snapshot_chunk_sent: BTreeMap::new(),
            snapshot_offer_base: BTreeMap::new(),
            snapshot_heartbeat_attempts: BTreeMap::new(),
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
            snapshot_chunk_bytes: SNAPSHOT_CHUNK_BYTES,
            pending_install: None,
            snapshot_needed: false,
            state_machine_behind: false,
            storage_full: false,
            last_installed_index: None,
            pending: Vec::new(),
            persisted_hard: (0, None),
            snapshot_dirty: false,
            quiesce_after: None,
            quiesced: false,
            last_activity: now,
            quiesce_engine_caught_up: true,
            quiesce_veto: false,
            quiesce_veto_fresh_through: u64::MAX,
            cluster_check_pending: None,
            cluster_check_resend_deadline: None,
            cluster_check_saw_established_with_me: false,
            cluster_check_refused: false,
            group_label: None,
            log_omits_base: false,
        };
        core.reset_election_timer(now, entropy);
        core
    }

    /// Issue #1229: declare that this group's base state was seeded outside
    /// its log (see the `log_omits_base` field). Set by the data-plane
    /// driver for a split child; idempotent.
    pub fn set_log_omits_base(&mut self, omits: bool) {
        self.log_omits_base = omits;
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
            // Tagged with `CONTROL_SNAPSHOT` (ADR 0073 Phase 0 workstream B, magic
            // `CSN1`) since this PR — see `handle_install_snapshot`'s decode side.
            if !S::DRIVER_APPLIED {
                let payload = serde_json::to_vec(&core.metadata).expect("metadata serializes");
                core.snapshot_blob = Some(format::wrap(&CONTROL_SNAPSHOT, &payload));
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
            // Tagged with `CONTROL_SNAPSHOT` (ADR 0073 Phase 0 workstream B, magic
            // `CSN1`) since this PR — see `handle_install_snapshot`'s decode side.
            let payload = serde_json::to_vec(&self.metadata).expect("metadata serializes");
            self.snapshot_blob = Some(format::wrap(&CONTROL_SNAPSHOT, &payload));
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
            self.snapshot_offset_regressions.clear();
            self.snapshot_chunk_sent.clear();
            self.snapshot_heartbeat_attempts.clear();
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

    /// Override this instance's own `InstallSnapshot` chunk size, in bytes
    /// (default [`SNAPSHOT_CHUNK_BYTES`]). Exists so a test that wants a
    /// snapshot transfer to genuinely span many chunks — without inflating
    /// the underlying state to many multiples of the (now much larger,
    /// 64 KiB) production default — can dial the chunk size back down
    /// instead. Production code never calls this; it is set once, if at
    /// all, right after construction.
    pub fn set_snapshot_chunk_bytes(&mut self, bytes: usize) {
        self.snapshot_chunk_bytes = bytes.max(1);
    }

    /// **Follower-aware compaction floor** (ADR 0017's compaction-flood
    /// amendment, PR #1047's follow-up): the highest index a leader's
    /// threshold-triggered compaction may advance `snapshot_index` to
    /// without forcing an `InstallSnapshot` on any peer that is merely a
    /// bit behind, rather than genuinely lost.
    ///
    /// A pure fact, same shape as [`snapshot_transfer_in_flight`](Self::
    /// snapshot_transfer_in_flight): this core does no compaction policy
    /// itself (`snapshot_upto`'s caller decides whether/when to compact at
    /// all), it only answers "how far could a threshold-triggered compaction
    /// go right now without stranding a peer that has a real chance of
    /// catching up via ordinary `AppendEntries`."
    ///
    /// Returns `min(match_index)` over every **voter currently within
    /// `retention_cap_entries` of `last_log_index()`** — a peer that far
    /// behind is excluded from the floor (it gets an `InstallSnapshot`
    /// today regardless, via `replicate_to`'s own `next_index <=
    /// snapshot_index` check, once the base does advance past it; excluding
    /// it here just stops it from pinning EVERY peer's compaction to a
    /// standstill). A peer this core has never heard from at all (absent
    /// from `match_index`) is treated as caught up to `0` — conservative:
    /// either it is freshly added (a real `0`) or it is down/partitioned
    /// (and `last_log_index().saturating_sub(0)` will itself exceed the cap
    /// once the leader has done enough work, excluding it in due course).
    ///
    /// **Deliberately voters only, never learners** (found building this
    /// fix): a learner's own catch-up contract in this codebase (ADR 0058
    /// Train 1) already IS "via ordinary `AppendEntries`/`InstallSnapshot`
    /// before ever being promoted" — an `InstallSnapshot` to a freshly
    /// joined, far-behind learner is the expected, unexceptional path, not
    /// the flood this retention floor exists to prevent (which is about
    /// ordinary VOTER replicas of an established tablet falling behind
    /// under routine load). Retaining for a learner too interacts badly
    /// with the separate `state_machine_behind`/`needs_snapshot` machinery
    /// (issue #554): while a receiver's own async apply task is still
    /// digesting a just-installed image, every `AppendEntriesResp` it
    /// builds reports `needs_snapshot: true`, and `handle_append_resp`'s
    /// `needs_snapshot` branch never runs the ordinary success path's
    /// `next_index` advance — so a leader whose retention floor is pinned
    /// close to that learner's own position can end up re-entering the
    /// snapshot path more than once before the learner's apply task and
    /// the leader's own bookkeeping settle, at the tiny pre-PR-#1047-
    /// chunk-size cost per cycle. Excluding learners keeps this floor
    /// scoped to the case it was actually built for.
    ///
    /// Returns `None` when this node is not the leader (a non-leader
    /// compacts by its own applied index and needs no floor — see this
    /// method's callers' own doc), when it has no peers at all (a
    /// single-node group; nothing to retain for), or when every peer is
    /// already excluded (nothing left to floor against, so a threshold
    /// compaction may proceed exactly as it did before this existed).
    ///
    /// `retention_cap_entries` is the caller's own hard bound on worst-case
    /// retained log length — this accessor enforces nothing about its
    /// magnitude, it only applies whatever cap the caller passes.
    pub fn compaction_floor(&self, retention_cap_entries: u64) -> Option<u64> {
        if self.role != Role::Leader {
            return None;
        }
        let last = self.last_log_index();
        let mut floor: Option<u64> = None;
        for peer in self.peers.iter() {
            let matched = self.match_index.get(peer).copied().unwrap_or(0);
            if last.saturating_sub(matched) > retention_cap_entries {
                // Too far behind to retain for; it already gets (or will
                // get) an `InstallSnapshot` via the ordinary `next_index <=
                // snapshot_index` path — don't let it pin every other
                // peer's compaction to a standstill.
                continue;
            }
            floor = Some(floor.map_or(matched, |f: u64| f.min(matched)));
        }
        floor
    }

    /// Whether a chunked `InstallSnapshot` transfer is currently in flight to
    /// at least one peer. `snapshot_upto` unconditionally invalidates every
    /// in-flight transfer's own progress the moment the base moves again
    /// (dropping the blob and clearing every peer's offset/sent-chunk
    /// bookkeeping — required for correctness, since the in-flight bytes
    /// were captured at the OLD base and shipping them under a new
    /// `snapshot_index` would corrupt the receiver). Under a sustained write
    /// stream that keeps re-crossing a `DRIVER_APPLIED` driver's compaction
    /// threshold faster than a lagging peer's own chunked transfer can
    /// complete, that invalidation can repeat forever, so the peer's
    /// catch-up never finishes (issues #532/#537's own residual finding
    /// beyond the `MAX_APPEND_ENTRIES_BATCH` cap — see that constant's doc).
    /// This accessor is the fact a `DRIVER_APPLIED` driver's own
    /// threshold-triggered compaction check needs to defer advancing the
    /// base while an in-flight transfer still has a chance to land — policy
    /// lives entirely in the driver (`animus-cp-data`'s `apply_and_compact`,
    /// `animus-control`'s `meta_apply_and_compact`), never here; this core
    /// stays a pure fact, same as `snapshot_index` itself.
    ///
    /// **True the moment a chunk has been SENT to some peer, not only once
    /// it has been ACKED (issue #898)**: checks `snapshot_chunk_sent` (set
    /// by [`snapshot_chunk_for`](Self::snapshot_chunk_for) at send time, for
    /// the very first chunk included) in addition to `snapshot_offset` (set
    /// only once a peer's first ack is processed — see that field's own
    /// doc). A definition keyed on `snapshot_offset` ALONE leaves a real gap
    /// from "leader ships chunk 0" to "leader processes that peer's first
    /// ack": for that whole round trip (which a slow/contended peer or link
    /// can stretch arbitrarily far), this accessor would report `false` even
    /// though bytes are genuinely on the wire, so a threshold-triggered
    /// compaction landing inside that window invalidates a transfer this
    /// accessor was supposed to protect — the driver-side defer this exists
    /// for never engages during exactly the window it matters most.
    /// Confirmed live: `crates/animus-control/tests/
    /// snapshot_compaction_race.rs` reproduces a freshly-joined follower
    /// pinned at `snapshot_index() == 0` forever under sustained churn with
    /// only the offset-based definition, deterministically under `SimEnv`.
    /// Both maps are cleared together at every existing invalidation/
    /// completion point (`snapshot_upto`'s base move,
    /// `handle_install_snapshot_resp`'s completion branch), so checking
    /// either is equally safe to rely on once populated; checking both
    /// closes the send-to-first-ack gap the offset map alone cannot see.
    ///
    /// **Deliberately blind to *how long* a peer has gone un-acked (issue
    /// #898 follow-up)** — that is a `Nanos`/`env.now()` question this pure,
    /// `now`-unaware core cannot answer. Two answers were tried at THIS
    /// accessor and rejected: a resend-count proxy for elapsed time
    /// conflated "peer is dead" with "peer's first round trip is merely
    /// slow" (`snapshot_compaction_race.rs`'s own deliberately slow link
    /// needed ~40 heartbeat-driven resends before its peer's first-ever
    /// ack); a `peer_last_contact`-based "has this peer gone quiet"
    /// check was ALSO rejected — `become_leader` optimistically seeds
    /// every peer's `last_contact` to the moment leadership begins (so a
    /// merely-slow-to-start peer and a peer that never starts at all are
    /// indistinguishable by that field alone; see `become_leader`'s own
    /// doc). A caller that needs to give up on a peer that is down,
    /// partitioned, or configured as a cluster member but never actually
    /// started at all has `now` and belongs at the driver layer: see
    /// `node.rs`'s `SNAPSHOT_COMPACT_DEFER_IDLE_CEILING`, an
    /// idle-progress-gated backstop (using
    /// [`snapshot_chunk_advances`](Self::snapshot_chunk_advances) as the
    /// progress signal, not wall-clock alone) layered entirely on top of
    /// this accessor's own `behind`-sized `SNAPSHOT_COMPACT_DEFER_CEILING`
    /// escape hatch, with no change needed here.
    pub fn snapshot_transfer_in_flight(&self) -> bool {
        !self.snapshot_offset.is_empty() || !self.snapshot_chunk_sent.is_empty()
    }

    /// The set of peers [`snapshot_transfer_in_flight`](Self::
    /// snapshot_transfer_in_flight) currently considers in flight (the
    /// union of `snapshot_offset`'s and `snapshot_chunk_sent`'s keys) — a
    /// pure, `now`-unaware structural fact, same as that accessor itself.
    /// Exists so a driver that DOES have `now` (issue #898 follow-up) can
    /// sum [`snapshot_chunk_advances`](Self::snapshot_chunk_advances) across
    /// every currently-outstanding peer as a genuine forward-progress
    /// signal, distinguishing "still shipping new chunks, however slowly"
    /// from "stuck at the same offset forever" — see `node.rs`'s
    /// `SNAPSHOT_COMPACT_DEFER_IDLE_CEILING` for the full mechanism this
    /// feeds.
    #[must_use]
    pub fn snapshot_transfer_peers(&self) -> BTreeSet<NodeId> {
        self.snapshot_offset
            .keys()
            .chain(self.snapshot_chunk_sent.keys())
            .cloned()
            .collect()
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
            // Issue #667 amendment: while the boot-time cluster check is still
            // pending, this node ALSO needs to wake in time to resend its probe
            // (`cluster_check_resend_deadline`, `tick()`'s own independent
            // resend check) — never only at `election_deadline`. The driver
            // loop (`node.rs`) sleeps exactly until whatever this function
            // returns and calls `tick()` only then; `election_deadline` is
            // legitimately reset far into the future by `handle_append_entries`
            // on every valid leader contact (a still-pending founder can start
            // receiving ordinary heartbeats from an already-elected sibling the
            // moment any majority forms), and `cluster_check_resend_deadline`
            // is deliberately never touched by that reset (see its own doc) —
            // so without this `min`, a real `ProdEnv` founder under real
            // staggered bring-up can oversleep past its own resend deadline
            // for as long as `election_deadline` keeps getting pushed out,
            // reproducing exactly the "cluster did not bootstrap" CI
            // regression this amendment fixes: the resend logic in `tick()`
            // was correct in isolation, but `tick()` was never being called
            // at the right time to run it.
            match self.cluster_check_pending {
                Some(_) => {
                    Some(Nanos(self.election_deadline.0.min(
                        self.cluster_check_resend_deadline.map_or(u64::MAX, |d| d.0),
                    )))
                }
                None => Some(self.election_deadline),
            }
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
        // Issue #1061: a node an explicit, valid removal notice has told it
        // was removed is not a voter for any purpose (campaigning, granting
        // pre-votes as a member, `TimeoutNow`), exactly like a learner —
        // whatever its not-yet-caught-up log-derived config still says.
        self.removed_by_leader.is_none() && self.config.contains(&self.id)
    }

    /// Whether an explicit removal notice (issue #1061, [`RaftMsg::Removed`])
    /// has told this node that a committed config entry removed it and no
    /// later entry has re-added it. Volatile. The host reconciler treats this
    /// like "my own log config excludes me" for its release decision (still
    /// gated on replicated `Metadata` excluding the node too).
    #[must_use]
    pub fn removed_by_leader(&self) -> bool {
        self.removed_by_leader.is_some()
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

    /// Every peer this leader still owes a removal notification (`departing`'s
    /// own field doc) — a pure, read-only observability accessor, empty on a
    /// non-leader (the bookkeeping is leader-local). Lets a test or admin
    /// view watch a notice get acked, or a silent peer given up on
    /// (issue #1061), without side effects.
    #[must_use]
    pub fn departing_peers(&self) -> BTreeSet<NodeId> {
        self.departing.keys().cloned().collect()
    }

    /// Lifetime removal-notice counters (issue #1061) — see [`RemovalStats`].
    /// A pure read; the driver diffs successive readings into metrics.
    #[must_use]
    pub fn removal_stats(&self) -> RemovalStats {
        self.removal_stats
    }

    /// Every distinct `(config, learners)` pair this core has adopted, in
    /// adoption order (issue #944) — see [`config_history`](Self)'s own
    /// field doc for the mechanism and why this exists. A pure accessor,
    /// reading it never blocks or mutates anything.
    #[must_use]
    pub fn config_history(&self) -> Vec<(BTreeSet<NodeId>, BTreeSet<NodeId>)> {
        self.config_history.iter().cloned().collect()
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

    /// Whether learner `id` is caught up closely enough to be a promotion
    /// candidate (ADR 0058 Train 1's promotion criterion): its tracked
    /// `match_index` is within `threshold` of [`commit_index`](Self::
    /// commit_index) — **not** [`last_log_index`](Self::last_log_index)
    /// (issue #1064 fix, 2026-09-28). A pure predicate over already-tracked
    /// state (the same bookkeeping `AppendEntries`/`InstallSnapshot` acks
    /// already maintain) — it does **not** gate
    /// [`promote_learner`](Self::promote_learner) itself; a later layer (the
    /// host reconciler) decides *when* to act on it. `false` for any `id`
    /// that is not currently a learner.
    ///
    /// **Why `commit_index`, not `last_log_index` (issue #1064):** under a
    /// continuous write stream, `last_log_index()` is the leader's own
    /// freshest LOCAL append — an entry nobody, not even another voter, has
    /// necessarily even received yet, let alone acked. A caller that
    /// samples this predicate right after proposing a batch (`reconfigure_
    /// step`'s own production caller ticks independently of the write
    /// stream, so this is the ordinary case under load, not a corner one)
    /// would see a gap of "however many entries this leader just appended
    /// for itself," permanently exceeding any fixed `threshold` regardless
    /// of how genuinely caught-up the learner actually is — a real learner
    /// can never close a gap that re-opens by the same amount (or more)
    /// every time it's checked. `commit_index()` doesn't have this problem:
    /// it only advances once a majority of CURRENT VOTERS (never this
    /// learner — [`apply_config`](Self::apply_config)'s doc) have
    /// themselves acked, so it is never further ahead of what the
    /// established quorum has actually achieved than one ordinary
    /// replication round costs. This also keeps the ADR 0058 Train 1 safety
    /// intent intact — "never dilute the quorum with a peer that can't
    /// ack": a peer promoted at `match_index >= commit_index - threshold`
    /// can, the instant it becomes a voter, immediately help COMMIT
    /// anything up to what the group has already committed (the property
    /// that actually matters for not regressing availability), which
    /// comparing against the ever-advancing leader tip never established
    /// anyway — a peer "caught up to `last_log_index`" could still be
    /// stale by the time its own promotion entry is itself appended.
    /// Regression: `tests/directed_placing_under_sustained_load.rs`.
    ///
    /// **Issue #1131 — also `false` while the learner's own issue #667
    /// boot-time cluster check is unresolved** (or it has not yet reported
    /// one to this leader; see [`RaftMsg::AppendEntriesResp`]'s
    /// `check_pending`). A node with a pending check refuses every vote and
    /// never campaigns, so promoting it adds a voter to the quorum
    /// denominator that cannot vote; with one other voter dead the group
    /// can then never elect (a permanent leaderless livelock). Gating the
    /// promotion only ever *delays* it — the learner resolves its check via
    /// any live peer that does not list it as a voter (it is still a
    /// learner, so no voter does), after which the next reconcile pass
    /// promotes it — so it cannot weaken the #667 wiped-voter safety
    /// property, which concerns what a node may do once it IS a voter.
    /// This predicate is the single gate for every production promotion
    /// path (`reconfigure_step` step 2; the control plane has no automatic
    /// promoter).
    #[must_use]
    pub fn learner_caught_up(&self, id: &NodeId, threshold: u64) -> bool {
        self.learners.contains(id)
            && self.peer_check_pending.get(id) == Some(&false)
            && self.commit_index().saturating_sub(self.peer_match(id)) <= threshold
    }

    /// Leader-side: the last `cluster_check_pending` `id` reported on an
    /// `AppendEntriesResp` (issue #1131), `None` if it has not reported
    /// since this leader's stint (or membership entry for it) began.
    #[must_use]
    pub fn peer_check_pending(&self, id: &NodeId) -> Option<bool> {
        self.peer_check_pending.get(id).copied()
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
        let changed = self.config != voters || self.learners != learners;
        self.config = voters;
        self.learners = learners;
        if changed {
            // Issue #944: this IS the one real transition, recorded right
            // where it happens — see `config_history`'s field doc.
            const CONFIG_HISTORY_CAPACITY: usize = 64;
            if self.config_history.len() >= CONFIG_HISTORY_CAPACITY {
                self.config_history.pop_front();
            }
            self.config_history
                .push_back((self.config.clone(), self.learners.clone()));
        }
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

    /// The `now` at which THIS leadership stint began (issue #923), or `None`
    /// on every non-leader — gated on `role == Leader` rather than a separate
    /// clear-on-step-down write, so a stepped-down node reads `None`
    /// immediately with no extra bookkeeping (see the `leader_since` field
    /// doc). Another raw fact with no policy baked in: `RaftNode::
    /// control_peer_believed_alive` is the one place that turns "how long
    /// have I held the gavel this stint" into a grace period.
    #[must_use]
    pub fn leader_since(&self) -> Option<Nanos> {
        if self.role == Role::Leader {
            self.leader_since
        } else {
            None
        }
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
        // Issue #1228: never hand leadership to a voter that reported it cannot
        // vote (storage-full, or an unresolved boot-time cluster check) -- it
        // would decline `TimeoutNow` at best and, if its report is stale, win a
        // term it cannot persist at worst. Applies to every caller (the
        // preferred-leader step, rebalance, the storage-full step-down), so no
        // placement preference can steer leadership onto a full node.
        if self.peer_check_pending.get(&target) == Some(&true) {
            return false;
        }
        // A full leader with no healthy quorum of successors must not depose
        // itself for a preference either: nobody could lead (see
        // `storage_full_step_down`).
        if self.storage_full && self.healthy_followers(now).len() < self.majority() {
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

    /// Install a new `(election_base, heartbeat_interval)` pair (ADR 0075
    /// section 3.4: the per-group timing profile, see
    /// [`crate::timing::TimingProfile::durations`]). Returns whether anything
    /// changed.
    ///
    /// **Idempotent**: re-installing the current pair is a no-op that touches
    /// no deadline, so a reconciler may call it on every tick without ever
    /// postponing an election. A zero duration is refused (returns `false`).
    ///
    /// On a real change the deadlines are re-armed from `now` so the new pair
    /// takes effect immediately rather than after the old, possibly much
    /// longer or shorter, wait: a non-leader draws a fresh randomized election
    /// deadline from the new base (`entropy` is the caller's `env.next_u64()`);
    /// a leader pulls its next heartbeat in to at most `now + heartbeat` (never
    /// pushes it out, so a WAN widening cannot starve followers still timing
    /// out on the old base) and leaves an armed `transfer_deadline` alone.
    /// A quiesced group's deadlines are not consulted at all. Everything that
    /// derives from the pair (`transfer_leadership`'s deadline,
    /// `next_cluster_check_resend`, the departing-peer backoff gap,
    /// [`election_timeout`](Self::election_timeout) and its
    /// `health` grace consumers) reads the fields, so it follows automatically.
    ///
    /// The driver sleeps until [`next_deadline`](Self::next_deadline) and
    /// recomputes it every iteration, so a caller that SHORTENS the timing
    /// should also wake the driver (the data plane's `RaftKvNode::wake`);
    /// lengthening needs nothing (an early wake just finds nothing due).
    pub fn set_timing(
        &mut self,
        election_base: Duration,
        heartbeat_interval: Duration,
        now: Nanos,
        entropy: u64,
    ) -> bool {
        if election_base.is_zero() || heartbeat_interval.is_zero() {
            return false;
        }
        if self.election_base == election_base && self.heartbeat_interval == heartbeat_interval {
            return false;
        }
        self.election_base = election_base;
        self.heartbeat_interval = heartbeat_interval;
        if self.role == Role::Leader {
            let next = Nanos(now.0.saturating_add(self.heartbeat_nanos()));
            if next.0 < self.heartbeat_deadline.0 {
                self.heartbeat_deadline = next;
            }
        } else {
            self.reset_election_timer(now, entropy);
        }
        true
    }

    /// The current `(election_base, heartbeat_interval)` pair — what
    /// [`set_timing`](Self::set_timing) last installed (the LAN defaults
    /// otherwise). Lets a caller skip an entropy draw when nothing would
    /// change.
    #[must_use]
    pub fn timing(&self) -> (Duration, Duration) {
        (self.election_base, self.heartbeat_interval)
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
            let old_members: BTreeSet<NodeId> = old_peers
                .iter()
                .chain(self.learners.iter())
                .cloned()
                .collect();
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
                    // A peer demoted to learner is still a member — replicated
                    // to as a learner, never departing.
                    if self.learners.contains(removed) {
                        continue;
                    }
                    self.departing.insert(
                        removed.clone(),
                        Departing {
                            index: entry.index,
                            term: entry.term,
                        },
                    );
                }
            }
            self.departing
                .retain(|n, _| !self.peers.contains(n) && !self.learners.contains(n));
            if self.role == Role::Leader {
                self.reset_peer_progress_on_membership_change(&old_members, entry.index);
            }
            self.departing_since
                .retain(|n, _| self.departing.contains_key(n));
            self.departing_quiet
                .retain(|n| self.departing.contains_key(n));
            self.removal_sched
                .retain(|n, _| self.departing.contains_key(n));
        }
        let carries_config = entry.config.is_some();
        self.pending.push(WalRecord::Append(entry.clone()));
        self.log.push(entry);
        if carries_config {
            self.refresh_removed_flag();
        }
    }

    /// The `(term, index)` stamp of the latest config entry (or snapshot
    /// boundary, or the initial config at `(0, 0)`) that lists this node as a
    /// voter OR learner — `None` when no config this node knows of includes
    /// it. Log-derived only; see [`EntryStamp`].
    fn latest_self_membership_stamp(&self) -> Option<EntryStamp> {
        let includes = |voters: &BTreeSet<NodeId>, learners: Option<&BTreeSet<NodeId>>| {
            voters.contains(&self.id) || learners.is_some_and(|l| l.contains(&self.id))
        };
        if let Some(e) = self.log.iter().rev().find(|e| {
            e.config
                .as_ref()
                .is_some_and(|c| includes(c, e.learners.as_ref()))
        }) {
            return Some((e.term, e.index));
        }
        // No self-including entry in the retained log: the base is the
        // snapshot's config if there is one, else the initial config.
        let (voters, learners, stamp) = match &self.snapshot_config {
            Some(c) => (
                c,
                self.snapshot_learners.as_ref(),
                (self.snapshot_term, self.snapshot_index),
            ),
            None => (&self.initial_config, Some(&self.initial_learners), (0, 0)),
        };
        includes(voters, learners).then_some(stamp)
    }

    /// Clear [`removed_by_leader`](Self) once a config entry or snapshot that
    /// INCLUDES this node and is strictly later than the removal it recorded
    /// has landed — the node was re-added. Called after every log append of a
    /// config entry and every snapshot install. A truncation never sets or
    /// clears it (a shortened log can only lose evidence of a re-add that
    /// will simply be re-delivered).
    fn refresh_removed_flag(&mut self) {
        if let Some(removal) = self.removed_by_leader
            && self
                .latest_self_membership_stamp()
                .is_some_and(|member| member > removal)
        {
            self.removed_by_leader = None;
        }
    }

    /// Truncate the log to `keep` entries and record it for persistence. If a
    /// truncated entry carried a config, the active config reverts to the latest
    /// surviving one.
    fn log_truncate(&mut self, keep: usize) {
        self.log.truncate(keep);
        // Issue #1228: entries past the cut are gone, and what replaces them is
        // not durable yet -- a frozen storage-full ack (`handle_append_entries`)
        // reads `durable_index` as "my log is on disk through here".
        self.durable_index = self.durable_index.min(self.last_log_index());
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
        self.now_hint = now;
        // Issue #667 amendment: the boot-time cluster-check resend, on its
        // OWN deadline (`cluster_check_resend_deadline`'s own doc explains
        // why this must never share `election_deadline`) — checked
        // unconditionally, every tick, regardless of role or of whatever
        // the match below does this same call. In practice this only ever
        // fires for a `Follower` still resolving `begin_cluster_check` (a
        // `Leader`/other role can't have `cluster_check_pending` still
        // `Some` — becoming either requires it already resolved), but
        // checking it here rather than inside the `Follower` arm means a
        // future role-handling refactor can't silently reintroduce the
        // original election-timer coupling this exists to avoid.
        let mut out = Vec::new();
        if self.cluster_check_pending.is_some() {
            let due = self
                .cluster_check_resend_deadline
                .is_none_or(|deadline| now.0 >= deadline.0);
            if due {
                out.extend(self.broadcast_cluster_probe());
                self.cluster_check_resend_deadline =
                    Some(self.next_cluster_check_resend(now, entropy));
            }
        }
        out.extend(match self.role {
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
                    // Issue #1061: sweep for any `departing` peer that has been
                    // silent past `DEPARTING_NOTICE_GIVE_UP`, at this cadence.
                    // Run before the quiesce check below so a group held open
                    // ONLY by a now-expired departing peer can quiesce on
                    // this very tick.
                    self.expire_stale_departing(now);
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
                    self.had_leader_contact = true;
                    // Heartbeat cadence is one of the bounded retries a
                    // genuinely stuck snapshot chunk gets — but "bounded by
                    // heartbeat cadence" alone is not a bound on TOTAL
                    // volume over an unboundedly long stall, only on the
                    // rate; `SnapshotResend::Backoff` is what actually
                    // bounds the total (see that variant's own doc for the
                    // full incident this closed).
                    return self.broadcast_append(SnapshotResend::Backoff);
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
        });
        out
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
        self.now_hint = now;
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
        self.now_hint = now;
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
            // Issue #1019: a fresh term makes any prior lease-lapsed memory
            // moot (see `vote_lease_lapsed`'s own doc) — reset it alongside
            // `voted_for` for symmetry, though `voted_lease`'s own
            // `voted_for.is_some()` conjunct already makes this a no-op at
            // this exact instant.
            self.vote_lease_lapsed = false;
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
            // Issue #898 follow-up: unlike `transfer_target` above,
            // `snapshot_offset`/`snapshot_chunk_sent` are NOT merely inert
            // once this node stops being leader — `snapshot_transfer_in_
            // flight()` (read by every replica's OWN `meta_apply_and_compact`
            // defer gate, node.rs) has no `role == Leader` guard of its own,
            // so a sender-side entry left over from a leadership stint that
            // ended before that specific peer's transfer ever reached
            // `handle_install_snapshot_resp`'s completion branch (the ONLY
            // other place these maps are cleared, short of `become_leader`
            // and `snapshot_upto`'s base move) survives indefinitely as a
            // FALSE "transfer in flight" — deferring this node's own local
            // compaction forever once `behind` stops growing (no further
            // writes), since it never reaches `SNAPSHOT_COMPACT_DEFER_
            // CEILING` either. Confirmed live: `prod_liveness.rs`'s
            // `large_metadata_catch_up_stays_live` deadlocking one of its two
            // 2-node-majority replicas at a small `snapshot_index` (a first,
            // real compaction) while `engine_applied_index` reached the full
            // target — the classic signature of a stuck defer, not a slow
            // apply task — on real `ProdEnv` thread contention triggering an
            // otherwise-harmless mid-transfer leadership churn between the
            // two. Clearing here, at the one place a `RaftCore` genuinely
            // steps down from believing it might be leading, closes it same
            // as `transfer_target`.
            self.snapshot_offset.clear();
            self.snapshot_offset_regressions.clear();
            self.snapshot_chunk_sent.clear();
            self.snapshot_heartbeat_attempts.clear();
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
        //
        // Issue #1226 carve-out: a **stale ack** does not wake a quiesced
        // leader. On links whose round trip exceeds the heartbeat interval the
        // heartbeat acks sent before the group quiesced land after it; they
        // carry no new information (same term, success, no snapshot/check
        // flag, and a `match_index` this leader already knew), so waking on
        // them re-ran a settle window forever. Anything else -- a vote, an
        // append, a higher term, a rejection, a fresher `match_index` --
        // still wakes. The ack is still processed (it refreshes
        // `last_contact`); should that ever produce output, that is new
        // information after all and the group wakes.
        if self.quiesced
            && self.role == Role::Leader
            && let RaftMsg::AppendEntriesResp {
                term,
                success: true,
                match_index,
                needs_snapshot: false,
                check_pending: false,
                ..
            } = &msg
            && *term == self.current_term
            && *match_index <= self.match_index.get(&from).copied().unwrap_or(0)
        {
            let (term, match_index) = (*term, *match_index);
            let out = self.handle_append_resp(from, term, true, match_index, false, false, now);
            if !out.is_empty() {
                self.quiesced = false;
                self.last_activity = now;
            }
            return out;
        }
        // The follower-side twin: a pure heartbeat from the recorded leader
        // that proves nothing new (no entries, `prev_log_index` at this
        // node's own tip, `leader_commit` not past its commit index) is
        // either reordered past the `Quiesce` that followed it (WAN
        // jitter) or the leader's own wake with nothing to replicate. Waking
        // on it re-armed the election timer under a leader that then went
        // silent, deposing it. The heartbeat is still answered (a ReadIndex
        // round needs the ack); an append with entries, a higher
        // `leader_commit`, a rejection, or any other message still wakes. A
        // leader that dies in this window is recovered the way a dead leader
        // of any quiesced group is (the reconciler's `Down` wake).
        if self.quiesced
            && self.role == Role::Follower
            && let RaftMsg::AppendEntries {
                term,
                leader,
                prev_log_index,
                entries,
                leader_commit,
                ..
            } = &msg
            && *term == self.current_term
            && self.leader_id.as_ref() == Some(leader)
            && entries.is_empty()
            && *prev_log_index == self.last_log_index()
            && *leader_commit <= self.commit_index
            && self.last_log_index() == self.commit_index
        {
            let RaftMsg::AppendEntries {
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } = msg
            else {
                unreachable!()
            };
            let out = self.handle_append_entries(
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                now,
                entropy,
            );
            let clean_ack = out.iter().all(|(_, m)| {
                matches!(
                    m,
                    RaftMsg::AppendEntriesResp {
                        success: true,
                        needs_snapshot: false,
                        ..
                    }
                )
            });
            if !clean_ack || self.last_log_index() != self.commit_index {
                self.quiesced = false;
                self.last_activity = now;
            }
            return out;
        }
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
            } => {
                // Issue #1061: a leader answering a pre-vote from a node it
                // knows to have been removed also tells it so.
                let notice = self.stranger_notice(&candidate);
                let mut out =
                    self.handle_pre_vote(candidate, term, last_log_index, last_log_term, now);
                out.extend(notice);
                out
            }
            RaftMsg::PreVoteResp { term, granted } => {
                self.handle_pre_vote_resp(from, term, granted, now, entropy)
            }
            RaftMsg::RequestVote {
                term,
                candidate,
                last_log_index,
                last_log_term,
            } => {
                let notice = self.stranger_notice(&candidate);
                let mut out = self.handle_request_vote(
                    candidate,
                    term,
                    last_log_index,
                    last_log_term,
                    now,
                    entropy,
                );
                out.extend(notice);
                out
            }
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
                check_pending,
            } => self.handle_append_resp(
                from,
                term,
                success,
                match_index,
                needs_snapshot,
                check_pending,
                now,
            ),
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
            } => self.handle_install_snapshot_resp(from, term, last_index, next_offset, now),
            // Heartbeats are intercepted by the driver and fed to the failure
            // detector (ADR 0012); they are not consensus traffic, so the core
            // ignores any that reach it.
            RaftMsg::Heartbeat { .. } => Vec::new(),
            RaftMsg::TimeoutNow { term } => self.handle_timeout_now(term, now, entropy),
            RaftMsg::ClusterProbe => self.handle_cluster_probe(from),
            RaftMsg::ClusterProbeResp {
                term,
                committed_index,
                config,
                ever_heard_from_prober,
            } => self.handle_cluster_probe_resp(
                from,
                term,
                committed_index,
                config,
                ever_heard_from_prober,
            ),
            RaftMsg::Quiesce { term, commit_index } => {
                self.handle_quiesce(from, term, commit_index);
                Vec::new()
            }
            RaftMsg::WakeRequest { .. } => self.handle_wake_request(from),
            RaftMsg::Removed {
                term,
                removal_index,
                removal_term,
                config,
                learners,
            } => self.handle_removed(
                from,
                term,
                (removal_term, removal_index),
                &config,
                &learners,
            ),
            RaftMsg::RemovedAck {
                term,
                removal_index,
            } => {
                self.handle_removed_ack(from, term, removal_index);
                Vec::new()
            }
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
        // `start_election`'s own `storage_full` gate declines (and re-arms the
        // election timer), so a storage-full transfer target simply lets the
        // leader's transfer deadline abort — no special case needed here.
        self.start_election(now, entropy)
    }

    /// Issue #667 (P0 Raft safety). Called by the driver exactly once, right
    /// after it builds a fresh `RaftCore` for a node whose WAL replayed to
    /// `PersistedState::is_empty()` — i.e. `current_term == 0`, `voted_for ==
    /// None`, an empty log, no snapshot. That local emptiness is ambiguous:
    /// it is indistinguishable from a genuine first-ever bootstrap of a
    /// brand new cluster. Voting or campaigning before resolving the
    /// ambiguity is unsafe — an already-established voter identity whose
    /// disk was wiped (ephemeral storage) could grant a *second*, distinct
    /// vote in a term it durably voted in before the wipe, since the vote it
    /// already cast is exactly what got lost. See `start_pre_vote`/
    /// `start_election`'s own guards and `handle_request_vote`'s grant
    /// condition, all of which check `cluster_check_pending`/
    /// `cluster_check_refused` below.
    ///
    /// Broadcasts [`RaftMsg::ClusterProbe`] to every configured peer and
    /// parks in `cluster_check_pending` until either (a) any one peer
    /// answers unambiguously (real history naming a DIFFERENT node id than
    /// this one, or genuinely empty `0`/`0` state — either resolves this
    /// node's own check immediately, with no need to wait for the rest of
    /// the peer set; see `handle_cluster_probe_resp`'s own doc for the full
    /// decision table, including why a peer naming THIS node id as an
    /// established voter is deliberately NOT unambiguous on its own), or
    /// (b) every peer has answered and none of them was unambiguously
    /// fresh — a single-node group (`self.peers` empty) has nothing to
    /// wait for and resolves immediately. A never-answering peer is never
    /// assumed fresh: `tick()`'s own independent resend check
    /// (`cluster_check_resend_deadline`, deliberately NOT tied to the
    /// ordinary election timer — see that field's own doc for why) keeps
    /// resending probes for as long as `cluster_check_pending` stays
    /// `Some` — "peers unreachable" means keep probing, never vote.
    pub fn begin_cluster_check(&mut self, now: Nanos, entropy: u64) -> Vec<Out<C>> {
        if self.peers.is_empty() {
            // Nothing to confirm with — a lone voter (or a peerless test
            // core) cannot possibly race anyone else for its own vote.
            self.cluster_check_pending = None;
            return Vec::new();
        }
        self.cluster_check_pending = Some(self.peers.iter().cloned().collect());
        self.cluster_check_saw_established_with_me = false;
        self.reset_election_timer(now, entropy);
        self.cluster_check_resend_deadline = Some(self.next_cluster_check_resend(now, entropy));
        self.broadcast_cluster_probe()
    }

    /// The next deadline for `tick()`'s own independent cluster-check
    /// resend check (`cluster_check_resend_deadline`'s own doc has the full
    /// reasoning for why this must never share `election_deadline`). Same
    /// randomized `[election_base, 2*election_base)` shape
    /// `reset_election_timer` uses — there is no reason for this cadence to
    /// differ, and reusing the shape (not the same call, since that would
    /// touch `election_deadline` too) keeps the resend rate familiar.
    fn next_cluster_check_resend(&self, now: Nanos, entropy: u64) -> Nanos {
        let base = self.election_base.as_nanos() as u64;
        let extra = if base == 0 { 0 } else { entropy % base };
        Nanos(now.0.saturating_add(base + extra))
    }

    fn broadcast_cluster_probe(&self) -> Vec<Out<C>> {
        self.peers
            .iter()
            .map(|p| (p.clone(), RaftMsg::ClusterProbe))
            .collect()
    }

    /// Answer a peer's [`RaftMsg::ClusterProbe`] honestly with our own
    /// current term/commit index/committed config, plus whether we
    /// ourselves have ever received a real protocol message from `from`
    /// (`ever_heard_from_prober` — see `RaftMsg::ClusterProbeResp`'s own doc
    /// for why this closes the genesis-race gap `config` alone cannot) —
    /// including while our *own* `cluster_check_pending` is still
    /// unresolved: a still-checking node's honest `0`/`0`/(whatever config
    /// it started with) is exactly the evidence a fellow genesis
    /// participant needs, and once this node itself resolves (either way)
    /// its own term/commit reflect that truthfully from then on.
    fn handle_cluster_probe(&mut self, from: NodeId) -> Vec<Out<C>> {
        let ever_heard_from_prober = self.heard_from.contains(&from);
        vec![(
            from,
            RaftMsg::ClusterProbeResp {
                term: self.current_term,
                committed_index: self.commit_index,
                config: self.config.clone(),
                ever_heard_from_prober,
            },
        )]
    }

    /// Tally a [`RaftMsg::ClusterProbeResp`]. A no-op unless our own
    /// `cluster_check_pending` is still `Some` (already resolved, or never
    /// applicable — a stale/duplicate reply).
    ///
    /// `term > 0 || committed_index > 0` proves `from` (and thus the
    /// cluster) has real history — but that alone does NOT mean this node
    /// itself is unsafe to proceed as a voter: an ordinary ADR 0060 growth
    /// join (a genuinely brand-new node id, never before a voter anywhere,
    /// added to an established cluster via `change_membership`) hits this
    /// exact branch too, since it also starts with an empty local WAL. The
    /// disambiguator is `from`'s own **committed config**:
    /// - `config.contains(&self.id)` — `from` already recognizes this node
    ///   id as one of its own voters, yet this node's own disk is empty.
    ///   That combination is only possible if this identity was already an
    ///   established voter and its disk was wiped — refuse permanently
    ///   (`cluster_check_refused`, never cleared; see that field's own doc).
    /// - otherwise — `from` has real history but does not (yet) recognize
    ///   this node id as a voter at all, so this identity could never have
    ///   cast a real vote here to forget in the first place. Resolve
    ///   immediately as an ordinary fresh voter: `start_pre_vote`'s own
    ///   log-check (ADR 0060) is what already keeps this case safe, and
    ///   always has, with no dependency on this whole mechanism.
    ///
    /// Only once *every* configured peer has confirmed genuinely empty
    /// (`0`/`0`) does this resolve to "genuine fresh multi-node bootstrap".
    /// Issue #667 amendment (2026-09-15): a REAL `ProdEnv` regression —
    /// `forward_to_tablet_leader_survives_a_dead_first_guess` flaking under
    /// real threading, root-caused via a reproduced trace log — proved the
    /// original one-reply-and-decide version of this function unsound for
    /// ordinary multi-node genesis bootstrap, not just an adversarial edge
    /// case: in a real (not `SimEnv`-synchronized) N-node genesis, a
    /// majority of peers can complete a real election among themselves
    /// before every founding peer's own probe round has finished — which
    /// looks BYTE-IDENTICAL, from a merely-slower founder's own evidence
    /// (a peer replying with `term > 0`/`committed_index > 0` whose
    /// committed config names this node, since a genesis committee always
    /// contains every founder from construction), to a genuine wiped-voter
    /// restart. Deciding on the FIRST such reply — as this function
    /// originally did — reproduces exactly this false permanent refusal,
    /// observed live cascading across 3 of 4 genesis founders in one
    /// captured run (see `docs/lessons/` for the reproduction).
    ///
    /// The fix: never decide refusal on a single reply. Evidence is
    /// gathered from EVERY configured peer (`cluster_check_pending`'s
    /// existing wait-for-all discipline, unchanged) before a refusal
    /// verdict is ever reached — except the two cases that are already
    /// unambiguous on a single reply, each resolving immediately without
    /// waiting for the rest of the peer set:
    /// - a peer with real history that does NOT yet recognize this node as
    ///   a voter (the `!config.contains` arm below) — safe regardless of
    ///   what any other peer says.
    /// - **any** peer answering genuinely empty (`term == 0 &&
    ///   committed_index == 0`) — i.e. that peer has itself never
    ///   participated in anything, which is unconditionally decisive:
    ///   this cannot be a cluster this identity was already an established
    ///   voter of and then lost its own record of, because a *genuinely*
    ///   established cluster's surviving voters (the ones this node would
    ///   need to convince otherwise) would ALL already show real history.
    ///   No later reply from any other peer, established-and-naming-me or
    ///   otherwise, can ever change this verdict — the veto is decisive
    ///   the instant it's seen, not merely "sticky until decision time" —
    ///   so resolving immediately here, rather than waiting for every
    ///   other configured peer to also answer, is both sound and
    ///   materially faster to converge under real, adversarial scheduling
    ///   (found live: a still-pending founder's own resend can be
    ///   suppressed for a long time once it starts receiving ordinary
    ///   `AppendEntries`/heartbeat traffic from an already-elected sibling,
    ///   since `handle_append_entries` legitimately resets
    ///   `election_deadline` on every such contact — see
    ///   `cluster_check_resend_deadline`'s own doc for the companion fix
    ///   this required).
    ///
    /// Only once every peer has answered with NEITHER of the two
    /// unambiguous signals above (every peer already has real history and
    /// none of them is fresh) does the verdict depend on aggregated
    /// evidence: refuse permanently if *at least one* of those peers both
    /// names this node as an established voter AND has itself genuinely
    /// heard from this identity before (`ever_heard_from_prober`) —
    /// that combination is only possible for a genuinely wiped,
    /// previously-established voter — otherwise resolve fresh.
    ///
    /// **Issue #667 amendment (2026-09-15, second): added
    /// `ever_heard_from_prober`** to close a real gap the two signals
    /// above cannot: in a genuine N-node genesis, EVERY founder's `config`
    /// contains every OTHER founder from the very first committed entry
    /// onward (that's what a genesis config *is*), so
    /// `config.contains(&self.id)` is unconditionally `true` for a genesis
    /// founder the instant any majority elects — the exact same signal a
    /// truly established, long-running cluster's wiped voter produces. The
    /// two are otherwise observationally identical from `from`'s own
    /// term/commit/config alone. `ever_heard_from_prober` breaks the tie: a
    /// founder still resolving its OWN cluster check has not campaigned or
    /// voted yet
    /// (gated on `!cluster_check_pending`), so it has sent no peer any real
    /// protocol message — every peer's honest answer is `false`.
    ///
    /// **Third amendment, same day**: the second amendment originally made
    /// a single `ever_heard_from_prober == false` reply immediately
    /// decisive for "fresh", on the claim that a genuinely established
    /// voter's peers "keep answering `true` for as long as they keep
    /// running." That claim is false for an ordinary follower-follower
    /// pair: a plain follower that has never itself been a candidate or
    /// leader only ever exchanges real protocol messages with whichever
    /// peer *is* the candidate/leader (a fellow follower never sends it a
    /// `RequestVote`/`AppendEntries`, and the leader does not mark
    /// `heard_from` on receiving a plain `AppendEntriesResp` either), so
    /// two long-established, healthy followers can go their whole lives
    /// never marking each other in `heard_from` — a genuinely wiped
    /// voter's fellow-follower peer honestly answers `false` even though
    /// the cluster is real, which reproducibly (not intermittently)
    /// defeated the refusal (found via a real, deterministic `ProdEnv`
    /// failure in
    /// `wiped_voter_refuses_and_the_rest_of_the_cluster_keeps_serving`).
    /// The fix folds this signal into the SAME wait-for-every-peer
    /// aggregation the established verdict already uses — decisive only
    /// once every peer has answered, and only via "at least one true"
    /// (mirroring "at least one established-and-naming-me", not "all of
    /// them") — so a true genesis race (uniformly `false` from every peer)
    /// still resolves fresh, but a real refusal no longer depends on every
    /// individual peer having directly talked to the wiped identity. See
    /// `RaftMsg::ClusterProbeResp`'s own doc for the full reasoning and its
    /// documented residual.
    fn handle_cluster_probe_resp(
        &mut self,
        from: NodeId,
        term: u64,
        committed_index: u64,
        config: BTreeSet<NodeId>,
        ever_heard_from_prober: bool,
    ) -> Vec<Out<C>> {
        let Some(pending) = self.cluster_check_pending.as_mut() else {
            return Vec::new();
        };
        if term == 0 && committed_index == 0 {
            // Unconditionally decisive the instant it's seen — see this
            // method's own doc for why waiting for the rest of the peer
            // set would only add latency, never change the outcome.
            self.cluster_check_pending = None;
            tracing::debug!(
                node = %self.id,
                peer = %from,
                "boot-time cluster check resolved: peer {from} is itself genuinely fresh, so \
                 any other peer's own \"established, and names me\" answer is a same-bootstrap \
                 timing artifact, not evidence of a genuine wiped-voter restart. Proceeding as \
                 an unestablished fresh voter.",
            );
            return Vec::new();
        }
        if !config.contains(&self.id) {
            // Unambiguous on its own, regardless of any other peer's
            // answer: `from` has real history but has never recognized
            // this node id as a voter at all, so this identity could
            // never have cast a real vote here to forget in the first
            // place — an ordinary new voter joining an established
            // cluster (ADR 0060). `start_pre_vote`'s own log-check is
            // what already keeps this case safe, with no dependency on
            // this whole mechanism.
            self.cluster_check_pending = None;
            tracing::debug!(
                node = %self.id,
                peer = %from,
                "boot-time cluster check resolved: peer {from} has real history but does \
                 not yet recognize this node id as one of its voters — an ordinary new \
                 voter joining an established cluster (ADR 0060), not a wiped-voter \
                 restart. Proceeding as an unestablished fresh voter.",
            );
            return Vec::new();
        }
        // Real history, and names this node as an established voter — but
        // `ever_heard_from_prober` is deliberately NOT treated as decisive
        // on its own here, in either direction (2026-09-15, third
        // amendment — a real `ProdEnv` regression,
        // `wiped_voter_refuses_and_the_rest_of_the_cluster_keeps_serving`,
        // reproduced this deterministically, not as a flake). The second
        // amendment's own doc claimed this signal is "a pure addition:
        // it never makes an established-restart refusal less likely...
        // a genuinely wiped voter's peers HAVE received real messages from
        // it pre-wipe and keep answering `true`" — but that assumption is
        // false for a perfectly ordinary established topology: a plain
        // FOLLOWER that has never itself been a candidate or leader only
        // ever receives direct protocol messages (`RequestVote`,
        // `AppendEntries`) from whichever peer *is* the candidate/leader —
        // never from a fellow follower. `handle_append_resp` (the leader's
        // own receipt of a follower's `AppendEntriesResp`) does not mark
        // `heard_from` either. So two long-established, perfectly healthy
        // follower peers can go their entire lives never marking
        // `heard_from` for each other, and a genuinely wiped voter's
        // fellow-follower will honestly answer `ever_heard_from_prober:
        // false` even though the cluster is real and long-running —
        // deciding "fresh" on that single reply (the previous code) is
        // exactly backward and defeats the refusal this whole mechanism
        // exists to enforce, reproducibly (not intermittently) whenever
        // the wiped voter's peer set contains a fellow follower it never
        // directly talked to.
        //
        // The fix: fold `ever_heard_from_prober` into the SAME
        // wait-for-every-peer aggregation the "established" verdict below
        // already uses, instead of letting it short-circuit early. A
        // single peer's `false` no longer resolves anything by itself;
        // only after every configured peer has answered (with neither of
        // the two genuinely unambiguous signals above) does the verdict
        // depend on whether *any* peer ever showed real participation
        // evidence — mirroring how "established" already only requires
        // ONE such peer, not all of them (a genuinely established cluster
        // is not guaranteed to have every peer show `true`, only at least
        // one that actually interacted with this identity before the
        // wipe). This restores the "pure addition, never weakens a real
        // refusal" property the second amendment intended but did not
        // achieve, while still closing the genesis-race gap the second
        // amendment targeted: in a true same-bootstrap race, NO peer has
        // ever received a real message from a still-checking founder
        // (nothing has been sent yet), so every reply shows `false` and
        // the aggregate below still resolves fresh.
        if ever_heard_from_prober {
            self.cluster_check_saw_established_with_me = true;
        }
        pending.remove(&from);
        if pending.is_empty() {
            self.cluster_check_pending = None;
            if self.cluster_check_saw_established_with_me {
                // Every peer answered, none was fresh and none denied
                // recognizing this node (either branch always resolves
                // and returns above), and at least one of them both named
                // this node as an established voter AND has itself
                // genuinely received a real protocol message from this
                // identity before — only possible for a genuinely
                // established, previously-active voter whose disk was
                // wiped.
                self.cluster_check_refused = true;
                tracing::error!(
                    node = %self.id,
                    group = self.group_label.as_deref().unwrap_or("control"),
                    "refusing to start as a voter: this node's persisted Raft state is empty \
                     (ephemeral storage wiped?), every configured peer already has real \
                     history, and at least one both recognizes this node id as an established \
                     voter and has itself genuinely heard from this identity before. Re-add \
                     this node id through the rejoin path instead of restarting it as a static \
                     voter: remove it from the voter set, add it back as a learner \
                     (`animusd join` / admin add-learner, ADR 0032/0058), and let it be \
                     promoted back to voter once caught up.",
                );
            } else {
                // Every peer has real history and names this node, but NOT
                // ONE of them has ever genuinely heard from this identity —
                // a same-bootstrap genesis race (or an ordinary rejoin),
                // never a wiped-voter restart (see this method's own doc,
                // third amendment).
                tracing::debug!(
                    node = %self.id,
                    "boot-time cluster check resolved: every configured peer has real \
                     history and names this node, but none of them has ever itself received \
                     a real protocol message from this identity — a same-bootstrap genesis \
                     race (or an ordinary rejoin), not a wiped-voter restart. Proceeding as \
                     an unestablished fresh voter.",
                );
            }
        }
        Vec::new()
    }

    /// Whether this node is still resolving the issue #667 boot-time check —
    /// while `true` it never grants a real vote and never campaigns (see
    /// `begin_cluster_check`'s doc).
    #[must_use]
    pub fn cluster_check_pending(&self) -> bool {
        self.cluster_check_pending.is_some()
    }

    /// Whether this node has permanently refused to act as a voter (issue
    /// #667): its persisted state replayed empty, and a peer's
    /// `ClusterProbeResp` proved the cluster it is configured into already
    /// exists. Sticky for this `RaftCore`'s lifetime — see
    /// `cluster_check_refused`'s own field doc for why there is no path
    /// back from this short of a restart through the learner/rejoin path.
    #[must_use]
    pub fn refused_as_voter(&self) -> bool {
        self.cluster_check_refused
    }

    /// Issue #1131: `true` while this node would refuse every vote and never
    /// campaign because of the #667 boot-time check — the check is still
    /// pending, or it resolved to REFUSED (a wiped voter). This is what a
    /// node reports to its leader as `AppendEntriesResp::check_pending`, so a
    /// leader never promotes a learner that could not vote once promoted.
    ///
    /// **Issue #1228:** also `true` while this node is
    /// [`storage_full`](Self::storage_full): it cannot persist a term bump or a
    /// vote, so it refuses every vote it would have to make durable and never
    /// campaigns -- the same "cannot act as a voter" state. Folding it into
    /// this one flag is how a full follower's *frozen ack* (see
    /// [`handle_append_entries`](Self::handle_append_entries)) tells its leader
    /// it is out of disk without a new wire field: the leader never hands
    /// leadership to it ([`storage_full_step_down`](Self::storage_full_step_down),
    /// [`transfer_leadership`](Self::transfer_leadership)), never counts it
    /// toward a healthy quorum, and never promotes it as a learner.
    #[must_use]
    fn cannot_vote_yet(&self) -> bool {
        self.cluster_check_pending.is_some() || self.cluster_check_refused || self.storage_full
    }

    /// Label this core's group for diagnostics (see the `group_label`
    /// field). Log-only; never affects protocol behavior.
    pub fn set_group_label(&mut self, label: impl Into<String>) {
        self.group_label = Some(label.into());
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
    /// - we do **not** currently have a live leader (a leader ourselves, a
    ///   follower still within its election timeout of the last heartbeat, OR a
    ///   follower/candidate that has cast a REAL vote (for someone else, or for
    ///   itself) within its election timeout, is protected — this is the
    ///   leader-lease that stops a partitioned node from winning a pre-vote and
    ///   forcing an election);
    /// - the candidate's prospective `term` is not behind ours; and
    /// - the candidate's log is at least as up to date as ours.
    ///
    /// **Issue #930 (2026-09-19): the lease used to be `leader_id`-only.**
    /// `leader_id` is set only by `handle_append_entries`/`InstallSnapshot`/
    /// `become_leader` — never by `handle_request_vote` on a granted real vote.
    /// So a voter that had just granted a real vote to the term's eventual
    /// winner had `leader_id == None` (and thus no lease at all) until that
    /// winner's first `AppendEntries` actually arrived — a real window with no
    /// pre-vote protection. A granted real vote IS a per-term commitment to
    /// this term's likely winner (Raft's own vote-splitting safety already
    /// relies on it), and the grant already reset this node's own election
    /// timer (`handle_request_vote`), so `voted_for.is_some()` is exactly as
    /// trustworthy a lease signal as `leader_id.is_some()` — **but only while
    /// we are still `Follower` or `Candidate`.**
    ///
    /// That role restriction is load-bearing, not incidental — an earlier
    /// draft of this fix used a bare `voted_for.is_some() && now <
    /// election_deadline` with no role gate at all, and it deadlocked the
    /// most ordinary recovery case there is: after a leader crashes, every
    /// surviving follower already has `voted_for = Some(<the dead leader>)`
    /// for the still-current term (that vote is how the leader got elected),
    /// and `voted_for` is never cleared by a timeout alone (only a higher
    /// term clears it — it is a durable per-term commitment, not something a
    /// mere timeout may retract). Meanwhile `start_pre_vote` — the handler
    /// for THIS node's own election timeout — resets `election_deadline` on
    /// every round it starts, forever, as long as no majority is reached.
    /// With no role gate, that stale, unrelated vote for the now-dead leader
    /// would combine with that perpetually-refreshed deadline to make this
    /// node believe it has a live leader for as long as it keeps timing out
    /// into fresh pre-vote rounds — i.e. forever — so it would reject every
    /// peer's pre-vote and no survivor could ever assemble a pre-vote
    /// majority. `RaftCore::tick`'s `Follower | PreCandidate | Candidate` arm
    /// moves a timed-out `Follower`/`Candidate` to `PreCandidate` in the same
    /// step that its own `election_deadline` lapses, so gating on role here
    /// makes the lease expire at exactly that transition — a `PreCandidate`
    /// gets no protection from a `voted_for` it can no longer vouch for, only
    /// from `leader_id` (already `None` the moment it starts campaigning
    /// itself; see `start_pre_vote`'s own comment). A `Candidate`, by
    /// contrast, sets `voted_for = Some(self)` in the very same call
    /// (`start_election`) that resets `election_deadline`, so its own lease
    /// window is never stale — see the consequence below.
    ///
    /// One knock-on consequence, not a bug: a `Candidate`'s self-vote
    /// (`voted_for = Some(self)`, set atomically with a fresh
    /// `election_deadline` in `start_election`) now protects it too, so it
    /// rejects a competing pre-vote for the rest of its own election
    /// deadline instead of granting it (the pre-`#930` behavior, since
    /// `leader_id` is `None` for the whole candidacy) — standard pre-vote
    /// behavior, and it reduces dueling candidacies rather than causing any.
    /// A second, bounded consequence: `voted_for` is persisted and a
    /// recovered `RaftCore` always starts `Follower` (`recovered`'s own
    /// doc), so a node that restarts with a vote already recorded for its
    /// current term refuses pre-votes for one full freshly-randomized
    /// election-timeout window after restart, even though it has no live
    /// leader belief of its own yet — a one-time, bounded startup cost, not
    /// a recurring one (see `RaftCore::new`/`recovered`).
    ///
    /// Regression coverage for both the fix and the rejected naive draft:
    /// `tests/pre_vote.rs`'s `prevote_rejected_after_granting_a_real_vote_
    /// until_deadline`, `prevote_rejected_by_a_candidate_within_its_own_
    /// election_deadline`, and the pre-existing `election_still_succeeds_
    /// when_leader_is_gone` (which the role-less draft above broke).
    fn handle_pre_vote(
        &mut self,
        candidate: NodeId,
        term: u64,
        last_log_index: u64,
        last_log_term: u64,
        now: Nanos,
    ) -> Vec<Out<C>> {
        // Issue #1019: `!self.vote_lease_lapsed` is the non-voter-only escape
        // hatch documented on the `vote_lease_lapsed` field itself — always
        // `false` (a no-op conjunct) for a voter, since only the `!is_voter()`
        // branch of `start_pre_vote` ever sets it `true`.
        let voted_lease = matches!(self.role, Role::Follower | Role::Candidate)
            && self.voted_for.is_some()
            && now.0 < self.election_deadline.0
            && !self.vote_lease_lapsed;
        let has_live_leader = self.role == Role::Leader
            || (self.leader_id.is_some() && now.0 < self.election_deadline.0)
            || voted_lease;
        let log_ok = last_log_term > self.last_log_term()
            || (last_log_term == self.last_log_term() && last_log_index >= self.last_log_index());
        // Issue #1019 (2026-09-21 amendment to ADR 0058 Train 1): the
        // RESPONDER must never gate granting on its own `is_voter()` view.
        // A membership-change entry takes effect the instant it is
        // *appended* to this node's own log (`log_append` -> `apply_config`),
        // not once it is committed — so a node's local `config`/`learners`
        // split can be stale relative to what a majority of the cluster has
        // already durably adopted. Concretely: a leader appends {L,A,B,C}
        // (promoting learner C) and replicates it to A and B, reaching a
        // 3-of-4 majority and committing — but C itself, which `promote_learner`
        // only required to be caught up to within
        // `RECONFIGURE_LEARNER_CATCH_UP_THRESHOLD` entries (not fully
        // replicated), may not have received that entry yet when the leader
        // dies. If C rejects a real candidate's request on the grounds that
        // *C's own* `is_voter()` is still false, the group is permanently
        // stuck: A and B alone can never reach a majority of 4, and C can
        // only learn it is now a voter by hearing from a leader — which
        // will never again exist. This is a real, reproduced deadlock (see
        // `tests/learner_promotion_leader_crash.rs`), not a hypothetical.
        // Standard Raft/etcd-raft semantics avoid it: a responder grants
        // (or withholds) purely on term, log up-to-dateness, and vote lease
        // — never on its own membership view. The "a learner never
        // influences an election" safety property does not need a
        // responder-side gate to hold: it is enforced on the CANDIDATE side,
        // where `handle_pre_vote_resp`/`handle_vote_resp` only ever tally a
        // grant that satisfies `self.config.contains(&from)` (the
        // candidate's own, always-committed-or-later config), and
        // `start_election`/`start_pre_vote` already refuse to campaign at
        // all when `!is_voter()`. A stray/injected `PreVote` reaching a
        // learner can therefore still be granted here without any safety
        // consequence — it simply won't count toward anyone's majority.
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
            // Issue #1019: same reasoning as `handle`'s own generic
            // higher-term step-down — see `vote_lease_lapsed`'s own doc.
            self.vote_lease_lapsed = false;
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
        // Issue #667 (2026-09-15 amendment): a real candidacy always
        // self-votes durably (`voted_for = self`) the moment it's issued,
        // whether or not WE grant it — record it unconditionally, before
        // the grant decision, so `heard_from`'s own doc's "never marked on
        // a rejection this node SENDS" rule stays about the RESPONDER's own
        // outbound rejection, not the incoming candidate's real self-vote.
        self.heard_from.insert(candidate.clone());
        let granted = if term < self.current_term {
            false
        } else {
            let log_ok = last_log_term > self.last_log_term()
                || (last_log_term == self.last_log_term()
                    && last_log_index >= self.last_log_index());
            let can_vote = self.voted_for.is_none() || self.voted_for == Some(candidate.clone());
            // Issue #667: an empty-store node still resolving (or refused
            // on) the genesis-vs-wiped-restart check must never grant a
            // real vote — `voted_for.is_none()` above is exactly the
            // locally-unreliable signal a wiped voter's own forgotten vote
            // would otherwise defeat. See `begin_cluster_check`'s doc.
            let cluster_checked =
                !self.cluster_check_refused && self.cluster_check_pending.is_none();
            // Issue #1019 (2026-09-21 amendment to ADR 0058 Train 1): same
            // reasoning as `handle_pre_vote`'s identical amendment — the
            // RESPONDER must never gate granting on its own `is_voter()`
            // view, since a learner-promotion entry takes effect on append
            // (not commit) and a not-yet-caught-up promoted learner can be
            // asked to grant before it has that entry. Gating this on the
            // responder's own membership would reproduce the exact
            // permanent-deadlock scenario `handle_pre_vote`'s comment
            // above describes, for the real election instead of pre-vote.
            // The `cluster_checked` gate (issue #667) is untouched — it
            // guards against a different hazard (an empty-store node's
            // locally-unreliable `voted_for.is_none()`) and has nothing to
            // do with membership. As with pre-vote, the "a learner never
            // influences an election" property is enforced candidate-side:
            // `handle_vote_resp` only tallies a grant with
            // `self.config.contains(&from)`, and `start_election` refuses
            // to campaign at all when `!is_voter()`.
            if can_vote && log_ok && cluster_checked {
                self.voted_for = Some(candidate.clone());
                // Issue #1019: a fresh real-vote grant re-arms the pre-vote
                // lease this grant is meant to protect (see
                // `vote_lease_lapsed`'s own doc) — load-bearing for a
                // non-voter specifically, whose lease can otherwise only
                // ever lapse, never re-arm, once its own timer has fired at
                // least once.
                self.vote_lease_lapsed = false;
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
        // Issue #667 (2026-09-15 amendment): a granted real vote durably
        // sets `from`'s own `voted_for` — exactly the forgettable state
        // `heard_from` exists to record. Marked before the stale-response
        // early-return below (`from`'s own vote was cast regardless of
        // whether it's still useful to *this* candidacy by the time it
        // arrives), never on `granted == false` (a rejection sets nothing).
        if granted {
            self.heard_from.insert(from.clone());
        }
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
        // Issue #667 (2026-09-15 amendment): `leader` sending `AppendEntries`
        // at all is proof it previously won a real election — which
        // required it to self-vote (durable, forgettable state) to become a
        // candidate in the first place — regardless of whether this
        // particular message is stale by the time it arrives.
        self.heard_from.insert(leader.clone());
        if term < self.current_term {
            return vec![(
                leader,
                RaftMsg::AppendEntriesResp {
                    term: self.current_term,
                    success: false,
                    match_index: 0,
                    needs_snapshot: self.state_machine_behind,
                    check_pending: self.cannot_vote_yet(),
                },
            )];
        }
        // Valid leader for our term: become/stay follower and defer the timeout.
        self.role = Role::Follower;
        self.leader_id = Some(leader.clone());
        // Issue #595: a genuine leader contact — see `last_leader_contact`'s
        // own doc for why this is recorded separately from `leader_id`.
        self.last_leader_contact = Some((leader.clone(), now));
        self.had_leader_contact = true;
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
                    check_pending: self.cannot_vote_yet(),
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
                    check_pending: self.cannot_vote_yet(),
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

        // Issue #1228 (R-01 (d), ADR 0074 section 2, "all replicas full"): a
        // storage-full follower still answers, but its ack is **frozen at its
        // own durable index** -- it vouches only for entries it has already
        // fsynced, never for the ones it just took into memory and cannot
        // persist. So the leader keeps hearing from it (leadership, ReadIndex
        // and liveness all survive an every-replica-full outage) while the
        // commit index can never advance on an entry this node did not
        // persist: `maybe_advance_commit` counts `match_index`, and this
        // match never exceeds `durable_index`. `check_pending` below reports
        // the full state so the leader does not hand it leadership.
        let match_index = if self.storage_full {
            match_index.min(self.durable_index)
        } else {
            match_index
        };

        vec![(
            leader,
            RaftMsg::AppendEntriesResp {
                term: self.current_term,
                success: true,
                match_index,
                needs_snapshot: self.state_machine_behind,
                check_pending: self.cannot_vote_yet(),
            },
        )]
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_append_resp(
        &mut self,
        from: NodeId,
        term: u64,
        success: bool,
        match_index: u64,
        needs_snapshot: bool,
        check_pending: bool,
        now: Nanos,
    ) -> Vec<Out<C>> {
        if self.role != Role::Leader || term != self.current_term {
            return Vec::new();
        }
        // Issue #1131: latest report wins (a reordered older ack can only
        // make a resolved check look pending again, which only delays a
        // promotion).
        let was_pending = self.peer_check_pending.insert(from.clone(), check_pending);
        if check_pending {
            self.peer_recovered_at.remove(&from);
        } else if was_pending == Some(true) {
            self.peer_recovered_at.insert(from.clone(), now);
        }
        // Either outcome — success or reject — proves `from` is up and
        // reachable right now, which is exactly the liveness signal
        // `peer_last_contact`/`control_peer_believed_alive` need. Stamped once
        // here, ahead of the branch, so both paths get it identically.
        self.last_contact.insert(from.clone(), now);
        // Issue #1061: the same reachability proof resets a departing peer's
        // give-up clock and its send-gate schedule (`removal_sched`).
        self.note_departing_reply(&from, now);
        // Issue #1228: whether this ack advanced what the leader knows `from`
        // has -- a storage-full follower's frozen ack never does.
        let mut progressed = false;
        if success {
            let m = self.match_index.entry(from.clone()).or_insert(0);
            progressed = match_index > *m;
            *m = (*m).max(match_index);
            if self
                .departing
                .get(&from)
                .is_some_and(|d| match_index >= d.index)
            {
                self.drop_departing(&from);
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
            // Issue #1070: monotonic, not a bare overwrite — mirrors the
            // `InstallSnapshot` success path's own `next_index` update a few
            // lines up (and `match_index`'s own `.max` just above). A
            // `success` ack is proof this replica's log matches through
            // `match_index`; it is never evidence its log is SHORTER than
            // what a previous, later-arriving-out-of-order ack already
            // proved. There is no legitimate scenario where a genuine
            // success ack should ever move `next_index` backward: a
            // follower whose log was truncated by a conflicting leader can
            // only ever report that via a REJECT (this method's own `else`
            // branch below), never a `success` — success is only sent for a
            // request this follower's log already matched through
            // `prevLogIndex`. Found investigating issue #1064: under
            // sustained bursty replication to a throttled peer, acks can
            // genuinely arrive out of send order, and the old bare `insert`
            // let a stale, lower-`match_index` success ack silently regress
            // `next_index` behind a fresher one already recorded — at worst
            // costing one wasted resend of already-matched entries; at
            // worst (confirmed live), racing a snapshot-triggered `next_
            // index = 1` reset the wrong way. See `docs/lessons/testing/
            // 2026-09-28-an-ordinary-appendentries-acks-next-index-update-
            // is-not-monotonic.md` for the full account.
            let ni = self.next_index.entry(from.clone()).or_insert(1);
            *ni = (*ni).max(match_index + 1);
            self.maybe_advance_commit();
            self.apply();
            // A frozen ack (the follower is out of disk and re-acks only what it
            // already had durable) must not trigger an immediate resend: the
            // resend would be acked just as flatly, and the pair would spin at
            // zero latency for the whole outage. The next heartbeat re-offers
            // the entries instead.
            if self.next_index.get(&from).copied().unwrap_or(1) <= self.last_log_index()
                && (progressed || !check_pending)
            {
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
        // Issue #667 (2026-09-15 amendment): same reasoning as
        // `handle_append_entries` — `leader` sending a snapshot at all
        // proves it previously won a real election.
        self.heard_from.insert(leader.clone());
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
        self.had_leader_contact = true;
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
        //
        // **Stale-snapshot rewind (found live, reproduced deterministically
        // in `tests/stale_snapshot_no_rewind.rs`): the guard must compare
        // against `last_applied`, not `snapshot_index`.** `snapshot_index`
        // only advances at compaction; `last_applied` advances on every
        // commit, independent of when compaction next runs, and is always
        // `>= snapshot_index` (the two coincide only immediately after an
        // install, `handle_install_snapshot`'s own `install` closure below).
        // A follower that races ahead of a chunked transfer via ordinary
        // `AppendEntries` — ordering compaction hasn't caught up to yet —
        // has `last_applied > snapshot_index` while the transfer is still
        // in flight. The old `last_index <= self.snapshot_index` check does
        // not see that: a stale transfer whose `last_index` sits strictly
        // between `snapshot_index` and the follower's true `last_applied`
        // sails through as "not yet redundant," and its final chunk installs
        // — clobbering `last_applied`/`commit_index`/the log back down to
        // that stale `last_index` (`install`, below) even though this
        // follower had already committed and applied past it. The apply
        // task then re-plays the rewound log tail, and its ts-carrying
        // entries land strictly below the high-water mark it already
        // recorded — `assert_ts_monotonic`'s panic. Comparing against
        // `last_applied` instead catches this: it is this node's own
        // up-to-date "how far have I actually gotten," not a proxy that
        // lags behind it.
        // `last_installed_index`'s own doc: an exact duplicate of the last
        // image THIS node fully installed is always redundant, regardless
        // of `state_machine_behind` — unlike `last_applied`/`snapshot_index`,
        // it is never inherited from a prior lifetime, so it cannot
        // misfire on the wipe-recovery case the `state_machine_behind`
        // guard below exists for.
        // Issue found live (2026-09-27, `--cluster-control 3 --cluster-data
        // 5` under bulk seeding + auto-split): the `state_machine_behind`
        // override above is meant ONLY for the #554 wipe-recovery shape,
        // where the offer's `last_index` is always `>= last_applied`
        // (typically exactly equal — the follower's log already matched the
        // leader's base before its engine was lost). But
        // `state_machine_behind` is *also* true, transiently and
        // legitimately, after every ordinary genuine `InstallSnapshot`
        // completes: `last_applied`/`snapshot_index` advance synchronously
        // right here, while the separate async apply task (`animus-cp-data`'s
        // `engine_applied`) is still draining `pending_install` into the
        // engine — `state_machine_behind` is recomputed live as
        // `engine_applied < snapshot_index` every consensus-loop iteration,
        // with no way to tell "behind because of a wipe" apart from "behind
        // because the engine hasn't drained the install it's ALREADY
        // received yet." An unrelated, already-obsolete transfer's final
        // chunk landing during that second window has `last_index` strictly
        // BELOW this node's `last_applied` — a shape the wipe case never
        // produces — and used to sail through the override exactly like a
        // genuine wipe-recovery offer, reinstalling and rewinding
        // `last_applied`/`commit_index`/the log backwards (the same rewind
        // `tests/stale_snapshot_no_rewind.rs`'s first regression covers,
        // gated here on `state_machine_behind` instead of on ordinary
        // catch-up). A follower must never install a snapshot whose
        // `last_index < last_applied`, regardless of `state_machine_behind`:
        // that comparison alone already distinguishes "behind" from "behind
        // AND this offer would rewind me," and the override only needs to
        // keep excusing the `==` case (the actual #554 shape) from being
        // treated as redundant.
        let duplicate_of_last_install = self.last_installed_index == Some(last_index);
        if duplicate_of_last_install
            || last_index < self.last_applied
            || (last_index == self.last_applied && !self.state_machine_behind)
        {
            self.incoming_snapshot = None;
            // This used to echo `last_index: self.snapshot_index` — nonzero
            // the instant this node has ever compacted at all, which made
            // this "already redundant, no install happened" reply
            // byte-for-byte indistinguishable, on the WIRE, from a genuine
            // just-completed install (`InstallSnapshotResp { last_index > 0,
            // .. }` below). Two real leader-side consumers trusted that
            // shape as "a completed install, full stop":
            // `animus-cp-data::record_kv_outbound` increments
            // `Metric::CpSnapshotInstalls` on ANY outbound `last_index > 0`
            // ack, and `handle_install_snapshot_resp`'s own "transfer
            // complete" branch reset `next_index`/cleared `snapshot_offset`
            // bookkeeping the same way for both. Once a peer has ever
            // compacted, a stale/duplicate/late-arriving chunk landing after
            // that peer has already caught up further via ordinary
            // `AppendEntries` silently inflates `CpSnapshotInstalls` and
            // regresses that peer's already-advanced `next_index` back down
            // to the stale offer's base, forcing a wholly unnecessary fresh
            // `InstallSnapshot`. Reproduced via targeted eprintln!
            // instrumentation against `ANIMUS_SEED=1394872321`:
            // old_match=702 new_match=702 old_next=703 new_next=191. The
            // fix: report `last_index: 0, next_offset: 0` — the "no
            // completion happened, and I have nothing buffered" shape
            // `handle_install_snapshot_resp`'s "still mid-transfer" branch
            // already handles via its own `next_offset == 0 && *tracked > 0`
            // case, which clears this peer's `snapshot_chunk_sent`/
            // `snapshot_heartbeat_attempts`/`tracked` bookkeeping without
            // touching `next_index`/`match_index` or counting a completed
            // install — exactly the "just acknowledge our position as
            // redundant" contract this short-circuit's own top-of-function
            // doc already promises, and what
            // `tests/stale_snapshot_no_rewind.rs` already asserts
            // (`last_index == 0`).
            return vec![(
                leader,
                RaftMsg::InstallSnapshotResp {
                    term: self.current_term,
                    last_index: 0,
                    next_offset: 0,
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
            // `last_installed_index`'s own doc: record BEFORE returning below,
            // regardless of which state-machine branch handles it, so a
            // resent duplicate of THIS exact completion is caught next time
            // even while `state_machine_behind` stays true.
            self.last_installed_index = Some(inc.last_index);
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
                // Issue #1061: a snapshot whose config includes this node and
                // is later than a recorded removal is a re-add.
                core.refresh_removed_flag();
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
            // In-core state machine: unwrap the `CONTROL_SNAPSHOT` (`CSN1`)
            // envelope and deserialize the image into `metadata`. A malformed
            // snapshot (or a pre-baseline/unsupported-version tag — loud, named,
            // logged below rather than silently misread) would be a leader bug;
            // drop + re-request rather than install garbage.
            let decoded =
                format::unwrap(&CONTROL_SNAPSHOT, &inc.buf).and_then(|(version, payload)| {
                    crate::persist::dispatch::snapshot_body::<S>(version, payload)
                });
            match decoded {
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
                Err(err) => {
                    tracing::error!(
                        %err,
                        "InstallSnapshot image failed to decode — dropping it and asking the \
                         leader to restart the transfer",
                    );
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
        now: Nanos,
    ) -> Vec<Out<C>> {
        if self.role != Role::Leader || term != self.current_term {
            return Vec::new();
        }
        // Issue #1061: identical reachability proof to `handle_append_resp`'s.
        self.note_departing_reply(&from, now);
        if last_index > 0 {
            // Transfer complete: the follower installed the snapshot.
            self.snapshot_offset.remove(&from);
            self.snapshot_offset_regressions.remove(&from);
            self.snapshot_chunk_sent.remove(&from);
            self.snapshot_heartbeat_attempts.remove(&from);
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
            // This used to be a bare `insert`, unconditionally overwriting
            // `next_index` with `last_index + 1` — correct for a GENUINE
            // just-finished install, but this same reply shape is also what
            // `handle_install_snapshot`'s "already at least this far along"
            // short-circuit used to send for an ALREADY-REDUNDANT offer,
            // whose own `last_index` there was the sender's `snapshot_index`
            // (its last COMPACTION point), a value that can sit arbitrarily
            // far behind a peer's real, already-tracked position. A
            // stale/duplicate final chunk from an earlier, already-superseded
            // transfer landing after this peer caught up further via
            // ordinary `AppendEntries` therefore used to yank a
            // correctly-advanced `next_index` back down to that stale
            // `snapshot_index + 1`, and the very next `replicate_to` for this
            // peer could find no entry to send at that regressed
            // `next_index` and fall back to a brand-new, wholly unnecessary
            // `InstallSnapshot`. `next_index` must be exactly as monotonic as
            // `match_index` two lines above — both are a leader's own memory
            // of a peer's ratcheting-forward replication progress under its
            // current term, and neither field's value can ever legitimately
            // move backward while that term holds. (The short-circuit above
            // no longer sends a `last_index > 0` reply at all, but this
            // branch stays monotonic as belt-and-suspenders against any
            // other future producer of this same reply shape.)
            let n = self.next_index.entry(from.clone()).or_insert(1);
            *n = (*n).max(last_index + 1);
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
            // Issue #1061: a departing peer that finished an install (a
            // transfer already in flight when it started departing, or a
            // stale one) and is now caught up past its removing entry is done
            // being told — mirrors `handle_append_resp`'s identical check.
            if self
                .departing
                .get(&from)
                .is_some_and(|d| self.match_index.get(&from).copied().unwrap_or(0) >= d.index)
            {
                self.drop_departing(&from);
                return Vec::new();
            }
            if self.next_index.get(&from).copied().unwrap_or(1) <= self.last_log_index() {
                return self
                    .replicate_to(from, SnapshotResend::Always)
                    .into_iter()
                    .collect();
            }
            return Vec::new();
        }
        // **A declined offer** (found live, 2026-09-29, `--cluster-control 3
        // --cluster-data 5` under bulk seeding: a stuck group re-offering the
        // same image every couple of seconds forever, at ZERO write rate,
        // counted as `CpSnapshotTransferRestarts` each time). A receiver that
        // is handed the very FIRST chunk of an offer (offset 0, non-empty
        // data) always buffers it and reports `next_offset > 0` — unless it
        // refused the offer as redundant (`handle_install_snapshot`'s
        // "already at least this far along" short-circuit, which replies
        // `last_index: 0, next_offset: 0` on purpose so it is never mistaken
        // for a completed install). So `(0, 0)` answering an offset-0 chunk
        // is an unambiguous "I already hold state through your offer's base".
        // (A restarted receiver that LOST a mid-transfer buffer reports the
        // same `(0, 0)` but only ever to a chunk at a *nonzero* offset — the
        // `next_offset == 0 && tracked > 0` case below — never to offset 0.)
        //
        // Before this branch nothing advanced this peer's `next_index`: the only way an
        // offer reaches a peer that is already past its base is a
        // `needs_snapshot` echo (`handle_append_resp`) that reset
        // `next_index` to 1 while `snapshot_served_through` lagged a base
        // that had since moved — and every later ack was a chunk reply,
        // never an `AppendEntriesResp`, so nothing ever re-advanced it: the
        // leader re-offered forever, the phantom `snapshot_offset` entry the
        // fallthrough below inserts kept `snapshot_transfer_in_flight` true,
        // the idle-ceiling then counted a "restart" every
        // `COMPACT_DEFER_IDLE_CEILING`, and the group could never quiesce.
        // The refusal says the receiver holds state at least through the offer's
        // base, so the offer — and the `next_index = 1` that provoked it — is
        // wrong for this peer: forget the transfer and resume ordinary
        // replication by PROBING with an `AppendEntries` from just above our
        // own base. Deliberately no inference about `match_index` from the
        // refusal (an inflated `match_index` could let `maybe_advance_commit`
        // count an entry the peer does not hold, and the offer's base may be
        // older than our current one if the base moved after it was sent): the
        // probe's reply decides — success advances `match_index` honestly, a
        // reject lowers `next_index` back into the snapshot region and a fresh
        // offer at the CURRENT base follows, which a peer that genuinely lacks
        // that base then accepts. The base counts as served so a stale
        // `needs_snapshot` echo for it cannot re-arm the offer.
        if next_offset == 0
            && self.snapshot_chunk_sent.get(&from).map(|&(o, _)| o) == Some(0)
            && let Some(base) = self.snapshot_offer_base.get(&from).copied()
        {
            self.forget_snapshot_transfer(&from);
            self.snapshot_offer_base.remove(&from);
            let served = self
                .snapshot_served_through
                .entry(from.clone())
                .or_insert(0);
            *served = (*served).max(base);
            let resume = self.snapshot_index + 1;
            let n = self.next_index.entry(from.clone()).or_insert(1);
            *n = (*n).max(resume);
            if self.next_index.get(&from).copied().unwrap_or(1) <= self.last_log_index() {
                return self
                    .replicate_to(from, SnapshotResend::Always)
                    .into_iter()
                    .collect();
            }
            return Vec::new();
        }
        // A duplicate `(0, 0)` decline arriving after the branch above already
        // resolved the offer (an offset-0 chunk is resent up to
        // `SNAPSHOT_ACK_RESEND_CAP` times before its first ack): no transfer
        // is outstanding, so it must not re-create the phantom
        // `snapshot_offset` entry the fallthrough below would insert.
        if next_offset == 0
            && !self.snapshot_offset.contains_key(&from)
            && !self.snapshot_chunk_sent.contains_key(&from)
        {
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
        let tracked = self.snapshot_offset.entry(from.clone()).or_insert(0);
        if next_offset == 0 && *tracked > 0 {
            // Issue #899 amendment (folded into #898's own regression-
            // counting rebase below, not a bypass of it): `next_offset == 0`
            // is a genuine, AUTHORITATIVE reset, never a reorder to guard
            // against, so it must not wait out `SNAPSHOT_OFFSET_REGRESSION_
            // REBASE` retries the way an ordinary partial regression does.
            // `handle_install_snapshot`'s own "still in progress" branch can
            // only ever report exactly `0` when `self.incoming_snapshot` is
            // `None` — which, for an ack reaching this far (`last_index ==
            // 0`, so no completed transfer either), means the follower has
            // FORGOTTEN whatever it was assembling (a real restart
            // discarding the volatile in-flight buffer, per
            // `handle_install_snapshot`'s own doc — never a reordered ack
            // for an ongoing transfer, since that always reports a nonzero
            // `inc.buf.len()`). Waiting for #898's own regression counter
            // here left a restarted follower and its leader deadlocked for
            // several extra round trips (and, before #898 existed at all,
            // permanently): the leader kept re-sending chunks at its own
            // stale (pre-restart, high) tracked offset, which the
            // follower's `fresh && offset == 0` guard
            // (`handle_install_snapshot`) can never treat as the start of a
            // fresh transfer, so `incoming_snapshot` never re-initializes
            // and the transfer never resumes. Confirmed via
            // `chunked_snapshot_receiver_stop_restart_3`'s fixed corpus seed
            // (also reachable with zero unrelated code changes at all, by
            // perturbing any other seed into this same narrow window — the
            // bug is pre-existing, not specific to how the seed is
            // reached). See `docs/lessons/` for the incident writeup.
            *tracked = 0;
            self.snapshot_chunk_sent.remove(&from);
            self.snapshot_heartbeat_attempts.remove(&from);
            self.snapshot_offset_regressions.insert(from.clone(), 0);
        } else if next_offset < *tracked {
            // Issue #898: a regression below the tracked offset — either a
            // stale, reordered ack for THIS transfer (the common case the
            // monotonic guard below protects against) or a peer whose buffer
            // genuinely reset (a real restart, same `NodeId`). Only rebase
            // once the SAME peer has regressed `SNAPSHOT_OFFSET_REGRESSION_
            // REBASE` times in a row with no intervening forward progress —
            // seeing it for the first (few) time(s) is exactly what an
            // ordinary reordered ack looks like too, so rebasing on the
            // first sighting would defeat the monotonic guard's own purpose.
            // (A regression all the way to exactly `0` is handled above,
            // immediately, instead — see that branch's own doc for why zero
            // specifically is never ambiguous the way any other partial
            // regression is.)
            let regressions = self
                .snapshot_offset_regressions
                .entry(from.clone())
                .or_insert(0);
            *regressions += 1;
            if *regressions > SNAPSHOT_OFFSET_REGRESSION_REBASE {
                *tracked = next_offset;
                self.snapshot_chunk_sent.remove(&from);
                self.snapshot_heartbeat_attempts.remove(&from);
                *self
                    .snapshot_offset_regressions
                    .entry(from.clone())
                    .or_insert(0) = 0;
            }
        } else {
            // Real forward progress (or an exact repeat) — the monotonic
            // guard's own case, and proof this peer's transfer is healthy:
            // clear any accumulated regression count.
            *tracked = next_offset;
            self.snapshot_offset_regressions.insert(from.clone(), 0);
        }
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
        // Issue #667: an empty-store node still resolving (or having
        // resolved unfavorably) the genesis-vs-wiped-restart check must
        // never campaign — see `begin_cluster_check`'s doc. While pending,
        // resend the probe instead of giving up on the round entirely, so a
        // transiently-unreachable peer's eventual reply still unblocks us.
        if self.cluster_check_refused {
            self.reset_election_timer(now, entropy);
            return Vec::new();
        }
        if self.cluster_check_pending.is_some() {
            self.reset_election_timer(now, entropy);
            return self.broadcast_cluster_probe();
        }
        // A node removed from the configuration must not campaign (mirrors
        // `start_election`): it can't win and would only disrupt the survivors.
        // Issue #554: a node whose own state machine is behind its own log's
        // compacted start must not campaign either, for the same reason a
        // learner never does (`is_voter()`, above) — winning would make it
        // leader over an engine missing everything the log already
        // discarded, which (unlike a learner) it CAN do here since its log
        // is otherwise fully caught up. Mirrors that gate exactly; see
        // `state_machine_behind`'s own doc.
        if !self.is_voter() || self.state_machine_behind || self.storage_full {
            // Issue #1019: a non-voter (learner, or a node not yet added at
            // all) can never itself campaign — but its belief that a
            // particular node is the live leader, and its own lease on a
            // real vote it once granted, must still decay on exactly the
            // same "haven't heard from anyone in a full election timeout"
            // signal a voter's does below, or neither ever decays at all.
            //
            // For a VOTER this decay is a side effect of the very next
            // lines: it becomes `PreCandidate`, which (a) clears `leader_id`
            // directly and (b) makes `handle_pre_vote`'s `voted_lease` check
            // evaluate false from then on purely because its OWN role is no
            // longer `Follower`/`Candidate` — `voted_for` itself is left
            // untouched, the role change alone is what stops it protecting
            // its old vote. A non-voter can never make that role change (it
            // stays `Follower` forever, by the very gate this branch is
            // inside), so it never gets that role-based decay "for free":
            // before this fix, a learner that had ever heard from a leader
            // (`leader_id`) or ever granted a real vote (`voted_for`, whose
            // protection window is `voted_lease`) kept believing/protecting
            // it FOREVER — even once the leader/candidate in question has
            // been dead for many election timeouts — because nothing else
            // ever clears either signal for a node that never campaigns
            // (the generic higher-term step-down in `handle` only fires on
            // a REAL, term-bumping message, which can itself never arrive
            // while every voter is stuck unable to reach a pre-vote
            // majority without this very node's grant). `handle_pre_vote`'s
            // `has_live_leader`/`voted_lease` gates read `leader_id`/
            // `vote_lease_lapsed` directly, so either one staying stale
            // silently reinstates the exact permanent deadlock the
            // responder-side `is_voter()` removal (this same issue, see
            // `handle_pre_vote`'s own doc) was meant to close.
            //
            // **`voted_for` itself is deliberately left untouched — clearing
            // it here would be a real safety hole, not merely redundant.**
            // `voted_for` is Raft hard state whose whole job is "at most one
            // real vote per term"; the candidate-side tally
            // (`self.config.contains(&from)`) does NOT make clearing it
            // harmless, because in exactly the scenario this branch exists
            // for, the responder genuinely IS a voter in the CANDIDATES' own
            // (majority-committed) config — only the responder's OWN,
            // stale-by-append-not-commit view says otherwise (see
            // `handle_pre_vote`'s own doc for why that gap exists at all).
            // Concretely: voters {L(dead), A, B, C, D}, term T, with C and D
            // both missing their own promotion entries. If a timer-driven
            // clear let C real-vote for A, then (once C's own timer fires
            // again) real-vote for B, both still in term T, A and D's config
            // already counts C as a voter (3 of 5) — two leaders, one term.
            // So only the PRE-VOTE lease (a liveness optimization,
            // `voted_lease`, never itself a safety mechanism — the real
            // safety property is "at most one real vote per term," fully
            // intact regardless of whether this lease is honored) is allowed
            // to lapse, via `vote_lease_lapsed` (see that field's own doc):
            // a genuine PRE-vote can now be granted once the lease lapses,
            // but a REAL vote for a second candidate in the same term still
            // cannot (`handle_request_vote`'s `can_vote` never consults this
            // flag). Regression: `tests/learner_promotion_leader_crash.rs`'s
            // seed sweep (the liveness half) and
            // `tests/non_voter_vote_lease.rs` (the safety half, proving a
            // second REAL vote in the same term still fails even once the
            // lease has lapsed). Scoped to the `!is_voter()` half only
            // (never `state_machine_behind`, an unrelated, already-a-voter
            // condition this branch also short-circuits on) — a learner
            // still never campaigns itself, it just stops silently vouching
            // for a leader it hasn't heard from in a while, and stops
            // protecting (pre-vote-wise only) a real vote it once granted.
            if !self.is_voter() {
                self.leader_id = None;
                self.vote_lease_lapsed = true;
            }
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
        if self.cluster_check_refused || self.cluster_check_pending.is_some() {
            // Defense in depth (mirrors `start_pre_vote`'s own primary
            // guard): `handle_pre_vote_resp`/`handle_timeout_now` can reach
            // this function directly, bypassing `start_pre_vote`.
            self.reset_election_timer(now, entropy);
            return Vec::new();
        }
        if !self.is_voter() || self.state_machine_behind || self.storage_full {
            self.reset_election_timer(now, entropy);
            return Vec::new();
        }
        self.role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id.clone());
        // Issue #1019: a fresh self-vote re-arms the pre-vote lease (see
        // `vote_lease_lapsed`'s own doc) — a no-op for a node that has
        // always been a voter (this branch is unreachable otherwise), but
        // load-bearing for a node freshly promoted from learner that still
        // carries a stale `true` from before its promotion.
        self.vote_lease_lapsed = false;
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
        // Issue #923: mark when THIS stint began, before any per-peer
        // `last_contact` seeding below — `leader_since`'s own field doc.
        self.leader_since = Some(now);
        // Issue #595: this node itself just won an election — record itself
        // as the genuine contact (see `last_leader_contact`'s own doc).
        self.last_leader_contact = Some((self.id.clone(), now));
        self.had_leader_contact = true;
        // A fresh leadership stint always starts un-quiesced (ADR 0044 phase-1
        // PR3) with its idle clock starting now — even if this same node was
        // quiesced as a follower a moment ago (its own `quiesced` from
        // accepting the previous leader's `Quiesce` is now meaningless).
        self.quiesced = false;
        self.last_activity = now;
        let last = self.last_log_index();
        self.peer_check_pending.clear();
        self.peer_recovered_at.clear();
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
        //
        // Issue #1061: NOT forgotten any more. A peer the previous leader
        // never finished notifying is re-derived from the config entries
        // still in this leader's own log (`removals_in_log`) and seeded like
        // any other replication target — see that method for the bound.
        self.departing.clear();
        self.departing_since.clear();
        self.departing_quiet.clear();
        self.removal_sched.clear();
        for (peer, dep) in self.removals_in_log() {
            if self.peers.contains(&peer) || self.learners.contains(&peer) {
                continue;
            }
            self.next_index.insert(peer.clone(), last + 1);
            self.match_index.insert(peer.clone(), 0);
            // Inherited, not this leader's own removal: the short silence
            // bound (`DEPARTING_QUIET_GIVE_UP`'s doc).
            self.departing_quiet.insert(peer.clone());
            self.departing.insert(peer, dep);
        }
        self.transfer_target = None;
        // A fresh term restarts any snapshot transfer from offset 0. Clear
        // `snapshot_chunk_sent` alongside `snapshot_offset` (issue #898's
        // `snapshot_transfer_in_flight` fix reads both) — a stale sent-chunk
        // record from a PRIOR stint as leader on this same node would
        // otherwise make this accessor report an in-flight transfer that no
        // longer exists, needlessly deferring compaction until the
        // `SNAPSHOT_COMPACT_DEFER_CEILING`/`COMPACT_DEFER_CEILING` backstop
        // eventually overrides it (harmless, but not the intent).
        self.snapshot_offset.clear();
        self.snapshot_offset_regressions.clear();
        self.snapshot_chunk_sent.clear();
        self.snapshot_heartbeat_attempts.clear();
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

    // ---- issue #1061: explicit removal notice ----------------------------

    /// Record that a departing peer replied (any reply proves it is alive):
    /// resets its give-up clock and re-opens its send-gate schedule.
    fn note_departing_reply(&mut self, peer: &NodeId, now: Nanos) {
        if self.departing.contains_key(peer) {
            self.departing_since.insert(peer.clone(), now);
            self.removal_sched.remove(peer);
        }
    }

    /// Leader-side: keep per-peer replication progress honest across a
    /// membership change (the stale-learner-promotion fix). A peer that this
    /// entry (re)introduces as a voter or learner — i.e. was in neither
    /// `old_members` set — starts FRESH: `match_index = 0`,
    /// `next_index = entry_index` (the entry itself), and any snapshot-transfer
    /// bookkeeping from an earlier incarnation is forgotten. Without this a
    /// node that was removed (its files erased) and re-added as a learner
    /// inherited its previous membership's `match_index`, so
    /// [`learner_caught_up`](Self::learner_caught_up) judged the wiped learner
    /// caught up and it was promoted before receiving anything. A peer that
    /// left membership without becoming `departing` (a removed learner is never
    /// departing) has all its progress dropped, so nothing lingers to be
    /// inherited by a later re-add. A `departing` peer keeps its state: it is
    /// still owed the removal notice.
    fn reset_peer_progress_on_membership_change(
        &mut self,
        old_members: &BTreeSet<NodeId>,
        entry_index: u64,
    ) {
        let now_members: BTreeSet<NodeId> = self
            .peers
            .iter()
            .chain(self.learners.iter())
            .cloned()
            .collect();
        for n in now_members.difference(old_members) {
            self.next_index.insert(n.clone(), entry_index);
            self.match_index.insert(n.clone(), 0);
            self.snapshot_served_through.remove(n);
            self.peer_check_pending.remove(n);
            self.peer_recovered_at.remove(n);
            self.forget_snapshot_transfer(n);
        }
        for n in old_members.difference(&now_members) {
            if self.departing.contains_key(n) {
                continue;
            }
            self.next_index.remove(n);
            self.match_index.remove(n);
            self.last_contact.remove(n);
            self.snapshot_served_through.remove(n);
            self.peer_check_pending.remove(n);
            self.peer_recovered_at.remove(n);
            self.forget_snapshot_transfer(n);
        }
    }

    /// Drop every per-peer scrap of leader-side bookkeeping for a peer that is
    /// no longer owed anything (`departing` and everything keyed by its id,
    /// including any snapshot-transfer bookkeeping — a phantom
    /// `snapshot_offset`/`snapshot_chunk_sent` entry would keep
    /// `snapshot_transfer_in_flight` `true` forever for a peer that will
    /// never ack again, wedging a `DRIVER_APPLIED` driver's compaction defer
    /// the way issue #898's step-down cleanup already guards against).
    fn drop_departing(&mut self, peer: &NodeId) {
        if self.departing.remove(peer).is_some() {
            self.removal_stats.departing_dropped += 1;
        }
        self.departing_since.remove(peer);
        self.departing_quiet.remove(peer);
        self.removal_sched.remove(peer);
        self.next_index.remove(peer);
        self.match_index.remove(peer);
        self.last_contact.remove(peer);
        self.snapshot_served_through.remove(peer);
        self.peer_check_pending.remove(peer);
        self.peer_recovered_at.remove(peer);
        self.forget_snapshot_transfer(peer);
    }

    /// Forget any in-flight snapshot transfer bookkeeping for `peer` (see
    /// [`drop_departing`](Self::drop_departing)); releases the cached image
    /// when this was the last outstanding transfer, mirroring
    /// `handle_install_snapshot_resp`'s lazy-image discipline.
    fn forget_snapshot_transfer(&mut self, peer: &NodeId) {
        self.snapshot_offset.remove(peer);
        self.snapshot_offset_regressions.remove(peer);
        self.snapshot_chunk_sent.remove(peer);
        self.snapshot_heartbeat_attempts.remove(peer);
        if S::DRIVER_APPLIED
            && self.snapshot_offset.is_empty()
            && self.snapshot_chunk_sent.is_empty()
        {
            self.snapshot_blob = None;
        }
    }

    /// The send gate for a departing peer (see `removal_sched`'s field doc):
    /// `true` when nothing is departing-gated for `peer` at all, or its
    /// schedule allows a send at `now_hint` (which then advances the
    /// schedule); `false` to suppress this send.
    fn departing_send_gate(&mut self, peer: &NodeId) -> bool {
        if !self.departing.contains_key(peer) {
            return true;
        }
        let now = self.now_hint;
        let (next_at, attempts) = self
            .removal_sched
            .get(peer)
            .copied()
            .unwrap_or((Nanos(0), 0));
        if now.0 < next_at.0 {
            return false;
        }
        let cap_shift = SNAPSHOT_HEARTBEAT_BACKOFF_MAX_TICKS.ilog2();
        let mult = 1u64 << attempts.min(cap_shift);
        let gap = self.heartbeat_nanos().saturating_mul(mult);
        self.removal_sched.insert(
            peer.clone(),
            (Nanos(now.0.saturating_add(gap)), attempts.saturating_add(1)),
        );
        true
    }

    /// Build a [`RaftMsg::Removed`] for `peer`, or `None` when the removing
    /// entry is not yet **committed** (a notice for an uncommitted removal
    /// could be truncated away by a later leader, and — unlike an
    /// `AppendEntries` config entry, which the log then repairs — a notice
    /// cannot be un-said) or `peer` is, in this leader's current view, a
    /// member again.
    fn removal_notice_for(&self, peer: &NodeId, dep: Departing) -> Option<RaftMsg<C>> {
        if self.commit_index < dep.index
            || self.config.contains(peer)
            || self.learners.contains(peer)
        {
            return None;
        }
        Some(RaftMsg::Removed {
            term: self.current_term,
            removal_index: dep.index,
            removal_term: dep.term,
            config: self.config.clone(),
            learners: self.learners.clone(),
        })
    }

    /// Every peer a config entry still in this node's log removed from the
    /// voter set and no later entry (in the log) put back — with the stamp of
    /// the removing entry. **The re-derivation bound of issue #1061**: this
    /// sees exactly the config entries the log still retains (everything
    /// after `snapshot_index`, plus the snapshot's own config as the base). A
    /// removal that compaction has already folded into a snapshot is not
    /// visible here; for such a peer only the reply to a returning
    /// pre-vote/vote (`stranger_notice`, which reads the same log) — and,
    /// failing that, the host reconciler's Metadata-driven release of a
    /// replica that never campaigns — remain. A peer demoted to a learner is
    /// still a member and never listed; a removed *learner* is not tracked at
    /// all (it never campaigns, and its own reconciler already releases it on
    /// Metadata alone).
    fn removals_in_log(&self) -> BTreeMap<NodeId, Departing> {
        let mut prev_voters = match &self.snapshot_config {
            Some(c) => c.clone(),
            None => self.initial_config.clone(),
        };
        let mut prev_learners = match &self.snapshot_learners {
            Some(l) => l.clone(),
            None => self.initial_learners.clone(),
        };
        let mut out: BTreeMap<NodeId, Departing> = BTreeMap::new();
        for e in &self.log {
            let Some(voters) = &e.config else { continue };
            let learners = e.learners.clone().unwrap_or_else(|| prev_learners.clone());
            for n in prev_voters.iter() {
                if !voters.contains(n) && !learners.contains(n) {
                    out.insert(
                        n.clone(),
                        Departing {
                            index: e.index,
                            term: e.term,
                        },
                    );
                }
            }
            out.retain(|n, _| !voters.contains(n) && !learners.contains(n));
            prev_voters = voters.clone();
            prev_learners = learners;
        }
        out.remove(&self.id);
        out
    }

    /// Leader-only: a `PreVote`/`RequestVote` from a node that is not a member
    /// of this leader's current configuration is answered, beside the
    /// ordinary rejection, with the removal notice — the receiver-initiated
    /// half of removal, needing no leader-side memory: it closes what the
    /// leader-initiated schedule (bounded by [`DEPARTING_NOTICE_GIVE_UP`], and
    /// lost across a leadership change once compaction has folded the
    /// removal into a snapshot) cannot — a peer partitioned for longer than
    /// any bound. A node that is not a member has no business campaigning, so
    /// this only ever fires for a removed peer (or a not-yet-added joiner
    /// that booted believing itself a voter, for whom it is equally correct:
    /// the flag it sets only stops it campaigning, and the entry that adds it
    /// later clears the flag). The zombie's own election timer is what
    /// triggers it, so the traffic is bounded by that timer until the peer
    /// acks, after which it never campaigns again.
    fn stranger_notice(&mut self, candidate: &NodeId) -> Option<Out<C>> {
        if self.role != Role::Leader
            || *candidate == self.id
            || self.config.contains(candidate)
            || self.learners.contains(candidate)
        {
            return None;
        }
        // Prefer the precise removing entry when the retained log still
        // shows it; otherwise (compaction has folded it into the snapshot, or
        // this leader was never told) claim only what the leader can prove —
        // the candidate is excluded by its *newest committed* config, at that
        // entry's stamp (or the snapshot boundary's). That claim is exactly as
        // strong as the recipient's guard needs: it is later than any
        // membership the candidate held before being removed, and earlier
        // than any re-add that has not happened yet.
        let dep = self
            .removals_in_log()
            .remove(candidate)
            .or_else(|| self.latest_config_stamp())?;
        let notice = self.removal_notice_for(candidate, dep);
        if notice.is_some() {
            self.removal_stats.notices_sent += 1;
        }
        notice.map(|m| (candidate.clone(), m))
    }

    /// The stamp of the newest config entry in the retained log, else the
    /// snapshot boundary; `None` when this group has never had either (the
    /// initial config is all there is, and no notice can be stamped).
    fn latest_config_stamp(&self) -> Option<Departing> {
        self.log
            .iter()
            .rev()
            .find(|e| e.config.is_some())
            .map(|e| Departing {
                index: e.index,
                term: e.term,
            })
            .or_else(|| {
                (self.snapshot_index > 0).then_some(Departing {
                    index: self.snapshot_index,
                    term: self.snapshot_term,
                })
            })
    }

    /// Give up on any `departing` peer that has sent back nothing at all for
    /// [`DEPARTING_NOTICE_GIVE_UP`] (measured from its last reply, never from
    /// when it started departing), dropping it and every per-peer scrap of
    /// bookkeeping ([`drop_departing`](Self::drop_departing)). Called once per
    /// heartbeat cadence from [`tick`](Self::tick). **Never touches `config`/
    /// `peers`** — the removal already took effect the moment the removing
    /// entry was appended; this only stops this leader's own notice/catch-up
    /// traffic to a peer it presumes gone. See the constant's doc for why this
    /// is safe to bound (it is not the correctness mechanism).
    fn expire_stale_departing(&mut self, now: Nanos) {
        if self.departing.is_empty() {
            return;
        }
        for peer in self.departing.keys() {
            self.departing_since.entry(peer.clone()).or_insert(now);
        }
        let ceiling = DEPARTING_NOTICE_GIVE_UP.as_nanos() as u64;
        let quiet_ceiling = DEPARTING_QUIET_GIVE_UP.as_nanos() as u64;
        let stale: Vec<NodeId> = self
            .departing_since
            .iter()
            .filter(|(p, since)| {
                // A peer that already acked its notice and then went silent
                // has left (its host released the replica): the short bound.
                let bound = if self.departing_quiet.contains(*p) {
                    quiet_ceiling
                } else {
                    ceiling
                };
                self.departing.contains_key(*p) && now.0.saturating_sub(since.0) >= bound
            })
            .map(|(p, _)| p.clone())
            .collect();
        for peer in stale {
            self.drop_departing(&peer);
        }
    }

    /// **Peer side** of the removal notice. See [`RaftMsg::Removed`].
    ///
    /// Accepted only if all of:
    /// 1. `term >= current_term` — a notice from an older term is answered
    ///    with our own term (so a deposed sender steps down) and otherwise
    ///    ignored, exactly like a stale `AppendEntries`.
    /// 2. We are not the leader of this term.
    /// 3. The notice's own membership excludes us (a malformed or misrouted
    ///    notice never marks a member removed).
    /// 4. `(removal_term, removal_index)` is **strictly later** than the
    ///    latest config entry we know of that includes us
    ///    ([`latest_self_membership_stamp`](Self::latest_self_membership_stamp)) —
    ///    the stale-notice guard, described below.
    ///
    /// **Why a delayed/stale notice cannot un-member a re-added node.** Our
    /// own knowledge of being re-added is a config entry (or snapshot) that
    /// includes us and is later than the removal in the one committed
    /// history. Comparing `(term, index)` stamps lexicographically is exactly
    /// "later in that history" (terms never decrease along a log), so a
    /// notice for a removal *older* than a re-add we already hold fails guard
    /// 4 and is ignored. A notice older than a re-add we have **not** yet
    /// received is accepted — but then it is *true as of when it was
    /// sent*, the re-add entry is still in flight to us, and it clears the
    /// flag on arrival (`refresh_removed_flag`, run on every appended config
    /// entry and every snapshot install: a self-including entry/snapshot
    /// with a stamp later than the recorded removal). In the interim the
    /// flag only stops us campaigning and granting — the same posture as a
    /// learner, which a not-yet-caught-up re-added replica already has to be
    /// safe under — and the host reconciler additionally refuses to release
    /// on the flag unless replicated `Metadata` also excludes the node
    /// (`host::plan`'s `tablets_to_release_set`), which a re-added replica
    /// never satisfies. A **fresh replica with an empty log** re-created
    /// after a re-add has no self-including entry at all (stamp `(0,0)` or
    /// none), so an old delayed notice is accepted by it too — harmless by
    /// the same two facts: it cannot campaign anyway until it is a voter
    /// (a fresh replica joins as a non-voter), it is never released while
    /// Metadata includes it, and the first `AppendEntries`/`InstallSnapshot`
    /// carrying the re-adding config clears the flag.
    ///
    /// **Why a flag and a config *view* rather than adopting the notice's
    /// config into `config`/`learners`:** those two, `peers`, `cluster_size`,
    /// `config_history` and `snapshot_config` are all pure functions of the
    /// log and snapshot (`apply_config` at every append/truncate/install,
    /// `recompute_config` after every truncation). Writing a config into
    /// them that no log entry justifies would be silently undone by the next
    /// truncation or recompute — or, worse, survive it and make `config_at`/
    /// `learners_at` (the values `AppendEntries` consistency and snapshot
    /// building trust) disagree with the live config, corrupting the very
    /// log-matching argument membership changes rest on. A separate,
    /// volatile, stamp-carrying flag consulted only by `is_voter()` and the
    /// reconciler cannot do that: the log and everything derived from it are
    /// byte-for-byte unchanged by a notice.
    fn handle_removed(
        &mut self,
        from: NodeId,
        term: u64,
        removal: EntryStamp,
        config: &BTreeSet<NodeId>,
        learners: &BTreeSet<NodeId>,
    ) -> Vec<Out<C>> {
        if term < self.current_term {
            self.removal_stats.notices_ignored += 1;
            return vec![(
                from,
                RaftMsg::RemovedAck {
                    term: self.current_term,
                    removal_index: 0,
                },
            )];
        }
        if self.role == Role::Leader || config.contains(&self.id) || learners.contains(&self.id) {
            self.removal_stats.notices_ignored += 1;
            return Vec::new();
        }
        if self
            .latest_self_membership_stamp()
            .is_some_and(|member| member >= removal)
        {
            self.removal_stats.notices_ignored += 1;
            return Vec::new();
        }
        // A valid notice comes from a genuine leader of this (or a newer,
        // already adopted) term: a candidate steps down like on an
        // `AppendEntries`, so a removed node mid-election can never still
        // tally its way to leadership.
        self.role = Role::Follower;
        self.leader_id = Some(from.clone());
        self.removed_by_leader = Some(self.removed_by_leader.map_or(removal, |r| r.max(removal)));
        vec![(
            from,
            RaftMsg::RemovedAck {
                term: self.current_term,
                removal_index: removal.1,
            },
        )]
    }

    /// **Leader side** of the ack: the peer has *recorded* its removal (the
    /// flag is volatile by design, see `removed_by_leader`). Stops the schedule and frees every per-peer scrap. An ack
    /// that does not cover the entry this leader is waiting on (a stale-term
    /// reply carries `removal_index: 0`) is ignored.
    fn handle_removed_ack(&mut self, from: NodeId, term: u64, removal_index: u64) {
        if self.role != Role::Leader || term != self.current_term {
            self.removal_stats.notices_ignored += 1;
            return;
        }
        // A stale-term reply carries `removal_index: 0`; an ack that does not
        // cover the entry this leader is waiting on is stale too.
        if removal_index == 0 {
            self.removal_stats.notices_ignored += 1;
            return;
        }
        match self.departing.get(&from) {
            // Not tracked as departing: the ack answers a `stranger_notice`
            // (the reply to a non-member's campaign), which needs no
            // leader-side memory — there is nothing to stop or drop.
            None => {
                self.removal_stats.notices_acked += 1;
                return;
            }
            Some(d) if removal_index < d.index => {
                self.removal_stats.notices_ignored += 1;
                return;
            }
            Some(_) => {
                self.removal_stats.notices_acked += 1;
                // An ack is a reply like any other: it proves the peer is
                // alive right now, so it restarts the silence clock (the
                // short bound below counts from it) and re-opens the send
                // gate for the catch-up `AppendEntries` it is still owed.
                self.note_departing_reply(&from, self.now_hint);
            }
        }
        // Only stop when the log can no longer serve the peer. A peer the
        // retained log still covers (a returning zombie that told us so by
        // campaigning, or one whose ack raced the schedule) keeps receiving
        // ordinary `AppendEntries` until it acks the removing ENTRY: the
        // notice is only the fallback for what the log cannot deliver, and a
        // node that never receives the entry keeps a log-derived config that
        // still lists it (visible on every admin view, and all a volatile
        // flag has after its own restart).
        let log_cannot_serve =
            self.next_index.get(&from).copied().unwrap_or(1) <= self.snapshot_index;
        if log_cannot_serve {
            self.drop_departing(&from);
        } else {
            // Still owed the catch-up entries, but the peer is known to have
            // recorded its removal: from here only a *silent* peer is cut off,
            // and on the short bound (`DEPARTING_QUIET_GIVE_UP`).
            self.departing_quiet.insert(from);
        }
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
        // Issue #1061: everything sent to a departing peer rides its own
        // capped-backoff schedule, so a silent one costs next to nothing.
        if !self.departing_send_gate(&peer) {
            return None;
        }
        let next = self.next_index.get(&peer).copied().unwrap_or(1).max(1);
        // Issue #1229: a learner of a group whose base state lives outside
        // its log must be shipped the engine image, never the log from
        // entry 1 — the log alone lacks the pre-fork rows. Nothing flows to
        // it until a snapshot base exists (`snapshot_upto` at the image
        // build below moves `snapshot_index` off 0, after which the
        // ordinary `next <= snapshot_index` branch ships it). Raising
        // `snapshot_needed` needs something applied to snapshot at.
        if self.log_omits_base && self.snapshot_index == 0 && self.learners.contains(&peer) {
            if self.last_applied > 0 {
                self.snapshot_needed = true;
            }
            return None;
        }
        // The entry before `next` is in our snapshot (or earlier) — we can't form
        // a valid `prev_log_term`, so ship the snapshot instead, as the next
        // offset-addressed chunk for this peer.
        if next <= self.snapshot_index {
            // Issue #1061: a DEPARTING peer is never shipped a snapshot. It is
            // not a member of the configuration any more, so an image is
            // wasted bytes, and (since compaction's retention floor is
            // voters-only) every later compaction restarts the transfer — an
            // endless full-image flood to a peer that may never even be alive.
            // What it needs is the *fact* of its removal, not the state it
            // would have replicated: send the tiny notice instead.
            if let Some(dep) = self.departing.get(&peer).copied() {
                self.forget_snapshot_transfer(&peer);
                let notice = self.removal_notice_for(&peer, dep);
                if notice.is_some() {
                    self.removal_stats.notices_sent += 1;
                }
                return notice.map(|m| (peer, m));
            }
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
        // `Backoff` (the heartbeat tick's own policy — see that variant's
        // own doc for the full incident this closes): resend only when this
        // peer's own ATTEMPT count for the current offset is `0`, a power
        // of two below the ceiling, or a multiple of the ceiling once past
        // it — suppressing every other call. Below
        // `SNAPSHOT_HEARTBEAT_BACKOFF_MAX_TICKS` this is the same
        // exponentially-thinning schedule as before; past it, the doubling
        // stops and the schedule flattens into a steady resend every
        // `SNAPSHOT_HEARTBEAT_BACKOFF_MAX_TICKS`'th attempt forever — the
        // fix for the review finding that unbounded doubling leaves the GAP
        // between resends growing without bound too (see that constant's
        // own doc for why this ceiling, not zero and not sub-election-
        // timeout, is the sound value). Deliberately keyed off
        // `snapshot_heartbeat_attempts`, NOT `resends_so_far` above: that
        // counter only advances on a call that actually sends, so gating on
        // it directly would freeze the schedule forever at whatever count
        // first got suppressed (see that field's own doc).
        // `snapshot_heartbeat_attempts` instead advances on every attempt,
        // sent or suppressed, and resets whenever the tracked offset itself
        // changes (real progress always restarts the backoff, and a
        // genuinely new offset — attempt count freshly reset to `0` — is
        // never held back).
        if snapshot_resend == SnapshotResend::Backoff {
            let attempts = self
                .snapshot_heartbeat_attempts
                .entry(peer.clone())
                .or_insert((offset, 0));
            if attempts.0 != offset {
                *attempts = (offset, 0);
            }
            let n = attempts.1;
            attempts.1 = attempts.1.saturating_add(1);
            let past_ceiling_multiple =
                n != 0 && n.is_multiple_of(SNAPSHOT_HEARTBEAT_BACKOFF_MAX_TICKS);
            let pre_ceiling_doubling =
                n < SNAPSHOT_HEARTBEAT_BACKOFF_MAX_TICKS && n.is_power_of_two();
            if n != 0 && !pre_ceiling_doubling && !past_ceiling_multiple {
                return None;
            }
        }
        if resends_so_far == 0 {
            *self
                .snapshot_chunk_advances
                .entry(peer.clone())
                .or_insert(0) += 1;
        }
        let start = offset as usize;
        let end = (start + self.snapshot_chunk_bytes).min(serialized.len());
        let data = serialized[start..end].to_vec();
        let done = end as u64 == total;
        self.snapshot_chunk_sent
            .insert(peer.clone(), (offset, resends_so_far.saturating_add(1)));
        self.snapshot_offer_base
            .insert(peer.clone(), self.snapshot_index);
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

    /// Whether this process has ever had a genuine leader contact -- an
    /// `AppendEntries`/`InstallSnapshot` from a leader, or its own election win
    /// (issue #1228). Volatile: a freshly started process answers `false` until
    /// it hears a leader. The eventual-read gate uses it to tell "was in this
    /// group's history, leader currently unknown" from "never initialised".
    #[must_use]
    pub fn has_had_leader_contact(&self) -> bool {
        self.had_leader_contact
    }

    /// Whether this node's driver reported itself out of disk (issue #1219) —
    /// see [`set_storage_full`](Self::set_storage_full).
    #[must_use]
    pub fn storage_full(&self) -> bool {
        self.storage_full
    }

    /// Feed the **storage-full** state (R-01 (d), ADR 0074 §2, issue #1219).
    /// Only a `DRIVER_APPLIED` plane's driver calls this (`animus-cp-data`'s
    /// consensus loop, **live, every iteration**, in the same lock acquisition
    /// as `set_state_machine_behind`); the control plane keeps it `false`.
    ///
    /// While set, the node never starts a pre-vote or an election and declines
    /// a `TimeoutNow` (both fall into the same gate as `state_machine_behind`):
    /// it cannot persist the term bump and self-vote an election needs, and a
    /// leader that cannot make anything durable would refuse every write
    /// anyway. This is what keeps leadership from ping-ponging back to a node
    /// that just handed it off. It is **not** a liveness hole: a node that is
    /// *already* leader keeps leading until a healthy target really wins
    /// ([`storage_full_step_down`](Self::storage_full_step_down) only arms a
    /// transfer, and an aborted transfer leaves it leader), and a cluster that
    /// is full everywhere recovers as soon as any node's space returns and its
    /// flag clears.
    pub fn set_storage_full(&mut self, full: bool) {
        self.storage_full = full;
    }

    /// Leader-side (issue #1228): the other voters this leader currently has
    /// positive, recent evidence are **able to vote and persist** -- their
    /// latest `AppendEntriesResp` reported `check_pending == false` (which
    /// includes "not storage-full", see `cannot_vote_yet`) and arrived within
    /// one election timeout of `now`. A voter that has never reported, reported
    /// full / unable, or has gone quiet is absent. Empty on a non-leader.
    ///
    /// **Sustained health (chaos `disk_full` F-4).** A voter this leader saw
    /// report unable and then healthy is also absent until it has stayed
    /// healthy for [`SUSTAINED_HEALTH_ELECTION_TIMEOUTS`] election timeouts. A
    /// full disk regains a sliver of space (the node's own WAL rewrite frees its
    /// old file) that the next write consumes again; trusting that first
    /// healthy ack handed leadership back to the node that had just handed it
    /// off, onto a group whose other replicas were full too, so the new
    /// leader's first-term entry never committed and no linearizable read could
    /// be served.
    #[must_use]
    pub fn healthy_followers(&self, now: Nanos) -> Vec<NodeId> {
        if self.role != Role::Leader {
            return Vec::new();
        }
        let window = self.election_base.as_nanos() as u64;
        self.config
            .iter()
            .filter(|n| **n != self.id)
            .filter(|n| self.peer_check_pending.get(*n) == Some(&false))
            .filter(|n| {
                self.last_contact
                    .get(*n)
                    .is_some_and(|at| now.0.saturating_sub(at.0) <= window)
            })
            .filter(|n| {
                // Sustained health: a peer that was unable a moment ago and
                // just reported healthy is a flap, not a successor.
                self.peer_recovered_at.get(*n).is_none_or(|at| {
                    now.0.saturating_sub(at.0) >= window * SUSTAINED_HEALTH_ELECTION_TIMEOUTS
                })
            })
            .cloned()
            .collect()
    }

    /// Leader-side: hand leadership to the most up-to-date healthy-looking
    /// voter because this node is storage-full (issue #1219). Arms
    /// [`transfer_leadership`](Self::transfer_leadership) toward the voter with
    /// the highest `peer_match` (at least `commit_index`, the arm gate), the
    /// voter in `avoid` (the previous, un-answered target) ordered last so a
    /// target that is itself full — it declines `TimeoutNow` — is rotated away
    /// from on the next attempt. Returns the armed target; `None` if this node
    /// is not the leader, a transfer is already armed, or no voter qualifies.
    /// Idempotence is the caller's: it re-invokes only after the previous
    /// transfer aborted.
    ///
    /// **Issue #1228 -- only when a healthy successor can actually lead.** The
    /// handoff happens only if a *quorum* of the other voters is known healthy
    /// ([`healthy_followers`](Self::healthy_followers): they reported, on a
    /// recent ack, that they can vote and persist). With fewer, no replica can
    /// win an election or commit a write under any leader, so stepping down
    /// would only depose the one node that can still serve reads and refuse
    /// writes with a named `StorageFull` -- the group would sit leaderless
    /// until space returned (chaos finding F-1). In that case this returns
    /// `None` and the leader stays leader, write-refusing. A `None` here is
    /// retried by the caller on its cooldown, so the handoff still happens as
    /// soon as enough followers report healthy again.
    pub fn storage_full_step_down(&mut self, now: Nanos, avoid: Option<&NodeId>) -> Option<NodeId> {
        if self.role != Role::Leader || self.transfer_target.is_some() {
            return None;
        }
        let healthy = self.healthy_followers(now);
        if healthy.len() < self.majority() {
            return None;
        }
        let mut cands: Vec<NodeId> = healthy
            .into_iter()
            .filter(|n| self.peer_match(n) >= self.commit_index)
            .collect();
        cands.sort_by(|a, b| {
            (Some(a) == avoid)
                .cmp(&(Some(b) == avoid))
                .then(self.peer_match(b).cmp(&self.peer_match(a)))
                .then(a.cmp(b))
        });
        cands
            .into_iter()
            .find(|t| self.transfer_leadership(t.clone(), now))
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
