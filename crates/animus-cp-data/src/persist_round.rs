//! Persist-round accounting for the per-tablet consensus loop (issue #279).
//!
//! # Why this exists
//!
//! `drive()` used to call `persist_wal` (drain → `append` → **`fsync`**) inline,
//! twice per iteration, *before* it could return to its `select` over
//! recv/timer/propose/wake. ADR 0017 moved engine apply + compaction off that
//! loop for exactly this reason — a step that blocks past the 150 ms
//! `election_base` stops the node heartbeating (as leader) and stops it
//! re-arming its election deadline (as follower) — but the WAL `fsync` was
//! deliberately kept inline, on the assumption that a local `fsync` is fast.
//!
//! On a shared/virtualised CI disk that assumption fails, and the failure is
//! self-sustaining: while `fsync` blocks, followers legitimately campaign; every
//! leadership change's no-op commit generates more persist work on *every*
//! replica; the group never settles. Since ADR 0050 gave each tablet its own
//! private engine and WAL, a split mid-backfill multiplies the concurrently
//! `fsync`ing groups (parent + two children + the GSI's hidden-table tablet +
//! the control plane, × 3 replicas), which is why the livelock reproduced only
//! under split-during-backfill.
//!
//! The fix is *not* "persist faster" and *not* a bigger election timeout — a
//! budget never fixes a livelock. It is to let the loop keep servicing
//! `select` **while** an `fsync` is in flight, holding back only the handful of
//! outbound messages whose correctness actually depends on that `fsync`.
//!
//! # The model: rounds
//!
//! A **round** is one drain of `RaftCore::drain_persist` plus the I/O that makes
//! those records durable. Rounds are strictly serialised, because both drainers
//! hold the group's `wal_lock` across their whole drain → I/O → completion
//! sequence:
//!
//! * the consensus loop's own `persist_wal`, now raced inside `select` rather
//!   than awaited inline; and
//! * the apply task's **compaction rewrite**, which drains the loop's pending
//!   records and discards them (its `wal_image` rewrite supersedes them —
//!   `RaftCore::wal_image` re-emits the current hard state and the whole log
//!   tail), making them durable when its `env.replace` completes.
//!
//! [`PersistProgress`] numbers those rounds: `drained` counts rounds that have
//! taken records, `durable` counts rounds whose I/O has completed. Both
//! counters are bumped **under the core lock** by whichever task is drafting,
//! and read under the core lock by the consensus loop — so the loop's
//! "what covers the mutation I just made?" question has an answer that no
//! concurrent OS thread can invalidate between the question and the answer.
//!
//! That last point is the whole lesson of this issue's two reverted attempts.
//! Attempt #1 (a dedicated persist task) and attempt #2 (a fifth `select` arm,
//! like this one) both released acks off a watermark sampled *outside* the lock
//! hold that produced the mutation. Compaction, running on another OS thread,
//! could steal `core.pending` in that window; the loop's peek then said "nothing
//! to persist", no round was ever started for the buffered ack, and the ack sat
//! stranded (measured: up to 10.1 s) until some unrelated later write happened
//! to start a round. Stranded append-accepts stall the leader's commit index,
//! which is the very leadership instability the fix was meant to remove — and
//! `SimEnv`, being single-threaded, cannot see any of it.
//!
//! # Two layers, because the interleaving is untestable
//!
//! The stranding window is microseconds wide — between a step releasing the core
//! lock and the loop's next look at it — so no wall-clock test reliably hits it
//! (verified: a real-thread `ProdEnv` test with the bug deliberately
//! reintroduced stays green run after run). A defect that cannot be caught by a
//! test has to be made unrepresentable instead, so this module closes it twice:
//!
//! 1. **[`drain_for_round`] is the only sanctioned drain.** `begin_drain` is
//!    private to this module, so a drainer physically cannot take records
//!    without numbering the round that covers them. That is the bug attempt #2
//!    shipped, made uncompilable.
//! 2. **[`PersistProgress::fully_durable`] is the loop's unconditional safety
//!    net.** Independently of any round number: if nothing is pending *and* no
//!    round is in flight, every record this node holds is on disk, so anything
//!    still buffered can go out. Even a future drainer that numbers nothing —
//!    or numbers wrongly — costs latency, never liveness.
//!
//! # Why rounds and not `durable_index`
//!
//! An index-shaped watermark is not enough: a **vote-only** persist (a granted
//! `RequestVote` with no new log entry) changes `current_term`/`voted_for` and
//! moves no index at all, so `mark_durable_through` never fires for it and a
//! buffered vote grant keyed to an index would never be released. Rounds count
//! I/O, not log positions, so they cover both.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use animus_control::WalRecord;
use animus_control::raft::RaftMsg;
use animus_env::NodeId;
use futures::task::AtomicWaker;

use crate::{KvCommand, KvCore, KvState, KvWire};

/// A consensus-loop-owned boxed persist future. Boxed because the loop must
/// hold it *across* iterations (that is the point — the `fsync` outlives the
/// `select` that started it) and its concrete type is unnameable.
pub(crate) type PersistFut<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// Shared round accounting for one tablet group, written by both drainers (the
/// consensus loop's `persist_wal` and the apply task's compaction rewrite) and
/// read by the consensus loop.
///
/// **Locking contract:** [`gate`](Self::gate) and [`fully_durable`](Self::fully_durable)
/// must be called while holding the group's *core* lock (`Mutex<KvCore>`), in
/// the same acquisition as the step whose `has_unflushed_wal()` they are
/// passed; [`complete_drain`](Self::complete_drain) in the same acquisition as
/// its round's `mark_durable_through`. That co-location is what makes the
/// answers exact — see the module docs for what goes wrong otherwise.
/// [`durable`](Self::durable) alone is a free-standing read (the loop's
/// release step, which can only ever be conservative).
#[derive(Default)]
pub(crate) struct PersistProgress {
    /// Rounds that have drained records. Bumped by [`begin_drain`](Self::begin_drain).
    drained: AtomicU64,
    /// Rounds whose records are durable. Bumped by
    /// [`complete_drain`](Self::complete_drain). Never exceeds `drained`.
    durable: AtomicU64,
    /// The consensus loop's waker, registered by [`PersistArm`] each park.
    waker: AtomicWaker,
}

impl PersistProgress {
    /// Claim the next round number for a drain that just took records. **Call
    /// under the core lock, in the same acquisition as the `drain_persist()`
    /// whose records this round covers**, and only when that drain actually
    /// took something — an empty drain persists nothing and must not consume a
    /// round number (a consumer of [`gate`](Self::gate) relies on "the latest
    /// round is the one that took my records").
    ///
    /// Private on purpose: [`drain_for_round`] is the crate's only sanctioned
    /// drain, so a future third drainer cannot take records without numbering
    /// them. That was the shape of this issue's second bug.
    fn begin_drain(&self) -> u64 {
        self.drained.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// The highest round any drainer has claimed.
    fn drained(&self) -> u64 {
        self.drained.load(Ordering::Acquire)
    }

    /// Whether every record this node has ever drained is durable **and**
    /// nothing is left undrained — i.e. the WAL on disk already backs anything
    /// this node could possibly be holding back. **Call under the core lock**,
    /// passing that core's `has_unflushed_wal()`.
    ///
    /// This is the consensus loop's safety net, and it is what makes a stranded
    /// ack structurally impossible rather than merely unlikely. The round
    /// protocol answers "when does *this* mutation become durable"; this answers
    /// the weaker but unconditional question "is there anything outstanding at
    /// all", which no drainer can get wrong: `pending` is only ever emptied by a
    /// drain (under this same lock), and `drained > durable` holds for exactly
    /// as long as some drain's I/O is in flight. When both are clear, a buffered
    /// message's records are on disk no matter which task put them there or
    /// whether it numbered the round correctly.
    pub(crate) fn fully_durable(&self, dirty: bool) -> bool {
        !dirty && self.durable() >= self.drained()
    }

    /// Record that `round`'s records are now durable, and wake the consensus
    /// loop so it can release anything waiting on it. **Call under the core
    /// lock, in the same acquisition as the round's `mark_durable_through`**,
    /// and only after the I/O actually succeeded — never mark durability that
    /// did not happen.
    pub(crate) fn complete_drain(&self, round: u64) {
        self.durable.fetch_max(round, Ordering::AcqRel);
        self.waker.wake();
    }

    /// The highest round known durable.
    pub(crate) fn durable(&self) -> u64 {
        self.durable.load(Ordering::Acquire)
    }

    /// Which round a step's outbound messages must wait for, or `None` if this
    /// node is fully durable right now and they may ship immediately. **Call
    /// under the core lock, in the same acquisition as the step**, passing that
    /// core's `has_unflushed_wal()`.
    ///
    /// Three cases, and the third is the one that is easy to get wrong:
    ///
    /// * `dirty` — the step left records (or a term/vote change) un-drained.
    ///   The *next* round to start will pick them up, whichever task starts it,
    ///   so wait for `drained + 1`.
    /// * not `dirty`, but a round is in flight (`durable < drained`) — someone
    ///   already drained everything this node owes, including whatever backs
    ///   this step's messages, but the I/O has not landed. Wait for that round.
    ///   Skipping this case would ship an append-accept for entries that are
    ///   drained-but-not-yet-`fsync`ed — the exact durable-before-send violation
    ///   the loop exists to prevent, and one no core-level predicate can see
    ///   (`drain_persist` optimistically marks the hard state persisted at drain
    ///   time, and `durable_index` never moves for a vote-only round).
    /// * neither — nothing is owed and nothing is in flight: ship now.
    pub(crate) fn gate(&self, dirty: bool) -> Option<u64> {
        let drained = self.drained();
        if dirty {
            Some(drained + 1)
        } else if self.durable() < drained {
            Some(drained)
        } else {
            None
        }
    }
}

/// **The crate's only sanctioned WAL drain.** Takes `core`'s pending durable
/// state and, if it took anything, claims the persist round that covers it —
/// atomically, in the caller's single core-lock acquisition.
///
/// Both drainers go through here: the consensus loop's `persist_wal` and the
/// apply task's compaction rewrite. Making it one function is the point. When
/// compaction drained `RaftCore::pending` *without* numbering the round (its
/// `wal_image` rewrite supersedes those records, so simply discarding them was
/// correct before anything raced the loop), the consensus loop's buffered
/// append-accepts were left waiting on a round that no longer had a drainer —
/// stranded for as long as it took some unrelated later write to start one
/// (measured at 10.1 s). A shared helper makes "drain without numbering"
/// unrepresentable; the caller's remaining duty is to
/// [`complete_drain`](PersistProgress::complete_drain) the returned round once
/// its I/O is durable, and never before.
pub(crate) fn drain_for_round(
    core: &mut KvCore,
    progress: &PersistProgress,
) -> (Vec<WalRecord<KvCommand, KvState>>, Option<u64>) {
    let records = core.drain_persist();
    let round = (!records.is_empty()).then(|| progress.begin_drain());
    (records, round)
}

/// Outbound messages held back until their persist round lands, owned solely by
/// the consensus-loop task (`&mut self` throughout — no second task can observe
/// or race this state, which is what makes the round bookkeeping trivially
/// correct).
///
/// Batches are pushed in step order with non-decreasing rounds, so the queue is
/// sorted by round and a release is a prefix drain — arrival order is preserved
/// within and across batches.
#[derive(Default)]
pub(crate) struct GatedOuts {
    waiting: Vec<(u64, Vec<(NodeId, KvWire)>)>,
}

impl GatedOuts {
    /// Hold `outs` until `round` is durable. Empty batches are dropped.
    pub(crate) fn push(&mut self, round: u64, outs: Vec<(NodeId, KvWire)>) {
        if outs.is_empty() {
            return;
        }
        debug_assert!(
            self.waiting.last().is_none_or(|(r, _)| *r <= round),
            "gated batches must be pushed in non-decreasing round order"
        );
        self.waiting.push((round, outs));
    }

    /// The earliest round anything is waiting on — what [`PersistArm`] watches.
    pub(crate) fn min_round(&self) -> Option<u64> {
        self.waiting.first().map(|(r, _)| *r)
    }

    /// Whether anything is still held back (feeds the quiesce veto: a group with
    /// undelivered acks must not stop its timers).
    pub(crate) fn is_empty(&self) -> bool {
        self.waiting.is_empty()
    }

    /// Take every batch whose round is now durable, in arrival order.
    pub(crate) fn release(&mut self, durable: u64) -> Vec<(NodeId, KvWire)> {
        let keep = self.waiting.iter().position(|(r, _)| *r > durable);
        let released: Vec<_> = match keep {
            Some(0) => return Vec::new(),
            Some(i) => self.waiting.drain(..i).collect(),
            None => std::mem::take(&mut self.waiting),
        };
        released.into_iter().flat_map(|(_, outs)| outs).collect()
    }

    /// Drop everything still held back — for the halted exit only. A halted node
    /// ships nothing more, and teardown deletes the WAL and engine regardless.
    pub(crate) fn clear(&mut self) {
        self.waiting.clear();
    }
}

/// Why [`PersistArm`] resolved.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PersistWake {
    /// The loop's own persist future finished; drop it and re-evaluate.
    OwnRoundDone,
    /// A round completed (possibly the apply task's compaction rewrite) and the
    /// durable watermark may now cover buffered outs.
    Durable,
}

/// The consensus loop's fifth `select` arm: drives the loop's own in-flight
/// persist future *and* watches the shared durable watermark, so a round
/// completed by the apply task's compaction releases buffered outs just as a
/// round completed by the loop itself does.
///
/// Resolving here never cancels the persist: the future lives in the loop's own
/// local and is merely borrowed for the poll, so a `Durable` wake in the middle
/// of the loop's own `fsync` leaves that `fsync` running and it is re-polled on
/// the next iteration.
pub(crate) struct PersistArm<'a, 'f> {
    progress: &'a PersistProgress,
    own: Option<&'a mut PersistFut<'f>>,
    round: Option<u64>,
}

impl<'a, 'f> PersistArm<'a, 'f> {
    pub(crate) fn new(
        progress: &'a PersistProgress,
        own: Option<&'a mut PersistFut<'f>>,
        round: Option<u64>,
    ) -> Self {
        Self {
            progress,
            own,
            round,
        }
    }
}

impl Future for PersistArm<'_, '_> {
    type Output = PersistWake;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<PersistWake> {
        let this = self.get_mut();
        // Register before reading the watermark — the same register-then-check
        // discipline as `ApplyPending`/`WakePending`, so a `complete_drain` that
        // lands between the read and the park cannot be missed.
        this.progress.waker.register(cx.waker());
        if let Some(round) = this.round
            && this.progress.durable() >= round
        {
            return Poll::Ready(PersistWake::Durable);
        }
        if let Some(own) = this.own.as_mut()
            && own.as_mut().poll(cx).is_ready()
        {
            return Poll::Ready(PersistWake::OwnRoundDone);
        }
        // Nothing buffered and no round of our own: this arm is inert, and the
        // loop parks on its other four sources exactly as before.
        Poll::Pending
    }
}

/// Whether an outbound message may ship **before** the round covering the step
/// that produced it is durable.
///
/// This is an allowlist, not a denylist, so a message added to `RaftMsg` later
/// is held back by default rather than silently shipped early. What is on it:
///
/// * `AppendEntries` (including heartbeats) — a leader's replication carries no
///   durability claim of its own. Commit is a quorum of follower `match_index`,
///   and the leader's own client-visible state is separately gated by
///   `min(commit_index, durable_index)`, so an entry it has not yet `fsync`ed
///   cannot become visible. **This is the message that kills the livelock**: a
///   leader keeps heartbeating, and a follower keeps re-arming its election
///   deadline, straight through an in-flight `fsync`.
/// * `PreVote`/`PreVoteResp` — pre-vote provably touches neither term nor vote
///   (ADR 0009), so there is nothing to make durable.
/// * `InstallSnapshot` chunks — the image was durable when compaction built it.
/// * `TimeoutNow`, `Quiesce`, `WakeRequest`, `Heartbeat` — liveness signals
///   carrying no state claim.
/// * `ReadProbe`/`ReadProbeAck` — a ReadIndex barrier, not log traffic; the core
///   never even sees these.
///
/// Everything else waits. In particular `RequestVoteResp { granted: true }` (a
/// server must never cast two votes in one term across a crash-restart),
/// `AppendEntriesResp { success: true }` (a restart must not lose entries the
/// leader counted toward a commit quorum), `RequestVote` (a candidate counts its
/// own vote, so forgetting it across a restart can elect two leaders in one
/// term), and `InstallSnapshotResp` (the installed image must survive a restart
/// before the leader advances `match_index` past it).
///
/// Note this only matters when the step was `dirty` at all — a message from a
/// step that changed nothing durable ships immediately whatever it is, because
/// [`PersistProgress::gate`] returns `None`.
pub(crate) fn ships_before_durable(wire: &KvWire) -> bool {
    match wire {
        KvWire::ReadProbe { .. } | KvWire::ReadProbeAck { .. } => true,
        KvWire::Raft(msg) => matches!(
            msg,
            RaftMsg::AppendEntries { .. }
                | RaftMsg::PreVote { .. }
                | RaftMsg::PreVoteResp { .. }
                | RaftMsg::InstallSnapshot { .. }
                | RaftMsg::TimeoutNow { .. }
                | RaftMsg::Quiesce { .. }
                | RaftMsg::WakeRequest { .. }
                | RaftMsg::Heartbeat { .. }
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wire(term: u64) -> KvWire {
        KvWire::ReadProbe { term, epoch: 0 }
    }

    fn outs(n: u64) -> Vec<(NodeId, KvWire)> {
        (0..n)
            .map(|i| (NodeId::new_unchecked(format!("n{i}")), wire(i)))
            .collect()
    }

    #[test]
    fn gate_waits_for_the_next_round_when_the_step_dirtied_the_wal() {
        let p = PersistProgress::default();
        assert_eq!(p.gate(true), Some(1));
        // A round in flight does not cover a mutation made after its drain.
        let r = p.begin_drain();
        assert_eq!(r, 1);
        assert_eq!(p.gate(true), Some(2));
    }

    #[test]
    fn gate_waits_for_an_in_flight_round_even_when_the_step_was_clean() {
        let p = PersistProgress::default();
        let r = p.begin_drain();
        // Not dirty, but the drained records are not durable yet: an ack resting
        // on them must not ship.
        assert_eq!(p.gate(false), Some(r));
        p.complete_drain(r);
        assert_eq!(p.gate(false), None);
    }

    #[test]
    fn gate_is_open_when_nothing_is_owed_or_in_flight() {
        let p = PersistProgress::default();
        assert_eq!(p.gate(false), None);
        let r = p.begin_drain();
        p.complete_drain(r);
        assert_eq!(p.gate(false), None);
    }

    #[test]
    fn fully_durable_is_false_while_anything_is_owed_or_in_flight() {
        let p = PersistProgress::default();
        // Nothing ever drained: a clean core is fully durable.
        assert!(p.fully_durable(false));
        // Records sitting in `pending` are not.
        assert!(!p.fully_durable(true));
        let r = p.begin_drain();
        // Drained but the I/O has not landed.
        assert!(!p.fully_durable(false));
        p.complete_drain(r);
        assert!(p.fully_durable(false));
    }

    #[test]
    fn a_drain_that_never_numbers_its_round_still_leaves_the_node_fully_durable() {
        // The shape of this issue's second bug: a drainer (compaction) took the
        // records and made them durable by its own means without claiming a
        // round, so `gate`'s answer has no drainer left. The round watermark
        // alone would strand the buffer forever; `fully_durable` is what
        // releases it, because nothing is pending and nothing is in flight.
        let p = PersistProgress::default();
        let stranded = p.gate(true).expect("a dirty step must gate");
        // ... the thief drains and completes out of band, numbering nothing.
        assert!(p.durable() < stranded, "the awaited round never arrives");
        assert!(
            p.fully_durable(false),
            "with pending empty and no round in flight the node owes nothing,              so a buffered ack must be releasable"
        );
    }

    #[test]
    fn durable_is_monotonic_across_out_of_order_completions() {
        let p = PersistProgress::default();
        let (a, b) = (p.begin_drain(), p.begin_drain());
        p.complete_drain(b);
        p.complete_drain(a);
        assert_eq!(p.durable(), b);
    }

    #[test]
    fn release_drains_the_durable_prefix_in_arrival_order() {
        let mut g = GatedOuts::default();
        g.push(1, outs(2));
        g.push(3, outs(1));
        assert_eq!(g.min_round(), Some(1));
        // Round 2 never existed; a later durable watermark still frees round 1.
        let freed = g.release(2);
        assert_eq!(freed.len(), 2);
        assert_eq!(freed[0].0, NodeId::new_unchecked("n0"));
        assert_eq!(freed[1].0, NodeId::new_unchecked("n1"));
        assert_eq!(g.min_round(), Some(3));
        assert!(!g.is_empty());
        assert_eq!(g.release(3).len(), 1);
        assert!(g.is_empty());
        assert_eq!(g.min_round(), None);
    }

    #[test]
    fn release_frees_nothing_below_the_earliest_round() {
        let mut g = GatedOuts::default();
        g.push(5, outs(1));
        assert!(g.release(4).is_empty());
        assert_eq!(g.min_round(), Some(5));
    }

    #[test]
    fn empty_batches_never_occupy_a_slot() {
        let mut g = GatedOuts::default();
        g.push(1, Vec::new());
        assert!(g.is_empty());
        assert_eq!(g.min_round(), None);
    }

    #[test]
    fn clear_drops_everything_for_the_halted_exit() {
        let mut g = GatedOuts::default();
        g.push(1, outs(3));
        g.clear();
        assert!(g.is_empty());
        assert!(g.release(u64::MAX).is_empty());
    }

    #[test]
    fn replication_and_pre_vote_ship_before_durability_but_acks_do_not() {
        let ships = |m: RaftMsg<crate::KvCommand>| ships_before_durable(&KvWire::Raft(m));
        assert!(ships(RaftMsg::AppendEntries {
            term: 1,
            leader: NodeId::new_unchecked("a"),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        }));
        assert!(ships(RaftMsg::PreVote {
            term: 1,
            candidate: NodeId::new_unchecked("a"),
            last_log_index: 0,
            last_log_term: 0,
        }));
        assert!(ships(RaftMsg::PreVoteResp {
            term: 1,
            granted: true
        }));
        assert!(ships_before_durable(&KvWire::ReadProbe {
            term: 1,
            epoch: 0
        }));

        assert!(!ships(RaftMsg::RequestVote {
            term: 1,
            candidate: NodeId::new_unchecked("a"),
            last_log_index: 0,
            last_log_term: 0,
        }));
        assert!(!ships(RaftMsg::RequestVoteResp {
            term: 1,
            granted: true
        }));
        assert!(!ships(RaftMsg::AppendEntriesResp {
            term: 1,
            success: true,
            match_index: 3,
        }));
        assert!(!ships(RaftMsg::InstallSnapshotResp {
            term: 1,
            last_index: 3,
            next_offset: 0,
        }));
    }
}
