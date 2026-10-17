//! The per-peer **outbox** (ADR 0080 section 3): the pure, bounded, prioritised
//! send queue that replaces one-spawned-task-per-frame in `ProdEnv`.
//!
//! The transport sees only `(to, stream, bytes)`; a Raft message is
//! `serde_json` inside the payload, so it **cannot classify a frame**. The
//! *sender* supplies a [`MsgClass`] (in memory only, never on the wire; see
//! [`Network::send_stream_classed`](crate::Network::send_stream_classed)), and
//! this struct applies the class policy:
//!
//! | class | policy | bound |
//! |---|---|---|
//! | [`Vote`](MsgClass::Vote) | strict priority; on overflow drop **oldest** | 4096 frames |
//! | [`Heartbeat`](MsgClass::Heartbeat) | above bulk; a newer frame with the same supersede key replaces the queued one | 4096 frames |
//! | [`Replicate`](MsgClass::Replicate) / [`Normal`](MsgClass::Normal) | supersede per sender key; beyond the bound drop **oldest** | 8 MiB and 4096 frames |
//! | [`Bulk`](MsgClass::Bulk) | **never dropped**: the producer is refused credit and must wait | 4 MiB |
//!
//! `Vote`, `Heartbeat` and `Replicate` share the *control* connection and
//! drain in that strict order ([`pop_control`](PeerOutbox::pop_control));
//! `Bulk` drains on its own connection ([`pop_bulk`](PeerOutbox::pop_bulk)),
//! so a stuck snapshot write cannot hold a vote behind it.
//!
//! Everything is sync, `BTreeMap`/`VecDeque`-only, and time-free (no clock is
//! needed for any policy above), so `SimEnv` rigs and `ProdEnv` share it and
//! the policy is property-testable. Dropping is *counted*, never silent.

use std::collections::VecDeque;

use crate::queued_bytes::{QueuedBytes, Reservation};

/// A frame's delivery class, chosen by the sender.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MsgClass {
    /// `PreVote`, `RequestVote`, their responses, `TimeoutNow`.
    Vote,
    /// Batched heartbeat frames and bare-heartbeat acks.
    Heartbeat,
    /// `AppendEntries` carrying entries and their acks.
    Replicate,
    /// `InstallSnapshot` chunks and segment-store traffic: blocks the
    /// producer, never dropped, own connection.
    Bulk,
    /// Everything not classified (the default): queued like `Replicate`,
    /// never superseded unless the sender gives a key.
    #[default]
    Normal,
}

impl MsgClass {
    /// Every class, in drain-priority order (`Bulk` last: its own connection).
    pub const ALL: [MsgClass; 5] = [
        MsgClass::Vote,
        MsgClass::Heartbeat,
        MsgClass::Replicate,
        MsgClass::Normal,
        MsgClass::Bulk,
    ];

    /// A stable lowercase name (metric and log suffix).
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            MsgClass::Vote => "vote",
            MsgClass::Heartbeat => "heartbeat",
            MsgClass::Replicate => "replicate",
            MsgClass::Bulk => "bulk",
            MsgClass::Normal => "normal",
        }
    }
}

/// One queued frame.
#[derive(Debug)]
pub struct OutFrame {
    /// The destination stream (the unchanged `(from, stream)` header).
    pub stream: u64,
    pub class: MsgClass,
    /// Sender-supplied replacement key: a newer frame of the same class and
    /// stream with the same key replaces a still-queued older one (a newer
    /// batched heartbeat for the same destination; a re-anchored append).
    /// `None` never supersedes.
    pub supersede: Option<u64>,
    pub payload: Vec<u8>,
    /// Held while queued; the bytes return to the node budget on drop.
    charge: Option<Reservation>,
}

impl OutFrame {
    #[must_use]
    pub fn new(stream: u64, class: MsgClass, supersede: Option<u64>, payload: Vec<u8>) -> Self {
        Self {
            stream,
            class,
            supersede,
            payload,
            charge: None,
        }
    }
}

/// The per-class bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutboxLimits {
    pub vote_frames: usize,
    pub heartbeat_frames: usize,
    pub replicate_frames: usize,
    pub replicate_bytes: usize,
    pub bulk_bytes: usize,
}

impl Default for OutboxLimits {
    fn default() -> Self {
        Self {
            vote_frames: 4096,
            heartbeat_frames: 4096,
            replicate_frames: 4096,
            replicate_bytes: 8 << 20,
            bulk_bytes: 4 << 20,
        }
    }
}

/// What a [`push`](PeerOutbox::push) did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PushReport {
    /// The frame is now queued. `false` only for `Bulk` with no credit: the
    /// producer must wait for [`bulk_credit`](PeerOutbox::bulk_credit) and
    /// retry; nothing was dropped.
    pub accepted: bool,
    /// Older frames removed to make room (drop-oldest).
    pub evicted: usize,
    /// An older queued frame with the same supersede key was replaced.
    pub superseded: bool,
}

/// The pure per-peer outbox.
#[derive(Debug)]
pub struct PeerOutbox {
    limits: OutboxLimits,
    vote: VecDeque<OutFrame>,
    heartbeat: VecDeque<OutFrame>,
    /// `Replicate` and `Normal`, FIFO together.
    replicate: VecDeque<OutFrame>,
    replicate_bytes: usize,
    bulk: VecDeque<OutFrame>,
    bulk_bytes: usize,
    budget: Option<QueuedBytes>,
    /// Frames dropped or superseded away, per class (index = `class_slot`).
    dropped: [u64; 5],
}

const fn class_slot(c: MsgClass) -> usize {
    match c {
        MsgClass::Vote => 0,
        MsgClass::Heartbeat => 1,
        MsgClass::Replicate => 2,
        MsgClass::Bulk => 3,
        MsgClass::Normal => 4,
    }
}

impl PeerOutbox {
    #[must_use]
    pub fn new(limits: OutboxLimits) -> Self {
        Self {
            limits,
            vote: VecDeque::new(),
            heartbeat: VecDeque::new(),
            replicate: VecDeque::new(),
            replicate_bytes: 0,
            bulk: VecDeque::new(),
            bulk_bytes: 0,
            budget: None,
            dropped: [0; 5],
        }
    }

    /// Charge every queued frame's bytes against the node `QueuedBytes`
    /// budget (counted, never refused: the outbox is bounded on its own).
    #[must_use]
    pub fn with_budget(mut self, budget: QueuedBytes) -> Self {
        self.budget = Some(budget);
        self
    }

    /// Queue `frame` per its class policy.
    pub fn push(&mut self, mut frame: OutFrame) -> PushReport {
        let len = frame.payload.len();
        let mut report = PushReport {
            accepted: true,
            ..PushReport::default()
        };
        match frame.class {
            MsgClass::Bulk => {
                // Credit: a frame larger than the whole bound is admitted when
                // nothing is queued, so an oversized chunk cannot wedge.
                if !self.bulk.is_empty() && self.bulk_bytes + len > self.limits.bulk_bytes {
                    report.accepted = false;
                    return report;
                }
                self.charge(&mut frame);
                self.bulk_bytes += len;
                self.bulk.push_back(frame);
            }
            MsgClass::Vote => {
                self.evict_to(MsgClass::Vote, &mut report, |s| {
                    s.vote.len() >= s.limits.vote_frames
                });
                self.charge(&mut frame);
                self.vote.push_back(frame);
            }
            MsgClass::Heartbeat => {
                if self.supersede_in(MsgClass::Heartbeat, &frame) {
                    report.superseded = true;
                    self.dropped[class_slot(MsgClass::Heartbeat)] += 1;
                }
                self.evict_to(MsgClass::Heartbeat, &mut report, |s| {
                    s.heartbeat.len() >= s.limits.heartbeat_frames
                });
                self.charge(&mut frame);
                self.heartbeat.push_back(frame);
            }
            MsgClass::Replicate | MsgClass::Normal => {
                if self.supersede_in(frame.class, &frame) {
                    report.superseded = true;
                    self.dropped[class_slot(frame.class)] += 1;
                }
                let class = frame.class;
                self.evict_to(class, &mut report, |s| {
                    !s.replicate.is_empty()
                        && (s.replicate.len() >= s.limits.replicate_frames
                            || s.replicate_bytes + len > s.limits.replicate_bytes)
                });
                self.charge(&mut frame);
                self.replicate_bytes += len;
                self.replicate.push_back(frame);
            }
        }
        report
    }

    fn charge(&self, frame: &mut OutFrame) {
        if let Some(b) = &self.budget {
            frame.charge = Some(b.force_reserve(frame.payload.len() as u64));
        }
    }

    /// Replace a queued frame of the same class, stream and supersede key.
    /// The newer frame is pushed to the back by the caller.
    fn supersede_in(&mut self, class: MsgClass, new: &OutFrame) -> bool {
        let Some(key) = new.supersede else {
            return false;
        };
        let (q, bytes): (&mut VecDeque<OutFrame>, Option<&mut usize>) = match class {
            MsgClass::Heartbeat => (&mut self.heartbeat, None),
            MsgClass::Replicate | MsgClass::Normal => {
                (&mut self.replicate, Some(&mut self.replicate_bytes))
            }
            MsgClass::Vote | MsgClass::Bulk => return false,
        };
        let pos = q
            .iter()
            .position(|f| f.class == class && f.stream == new.stream && f.supersede == Some(key));
        match pos {
            Some(i) => {
                let old = q.remove(i).expect("position is in range");
                if let Some(b) = bytes {
                    *b -= old.payload.len();
                }
                true
            }
            None => false,
        }
    }

    /// Drop oldest frames of `class`'s queue while `full` holds.
    fn evict_to(&mut self, class: MsgClass, report: &mut PushReport, full: impl Fn(&Self) -> bool) {
        while full(self) {
            let popped = match class {
                MsgClass::Vote => self.vote.pop_front(),
                MsgClass::Heartbeat => self.heartbeat.pop_front(),
                MsgClass::Replicate | MsgClass::Normal => {
                    let f = self.replicate.pop_front();
                    if let Some(f) = &f {
                        self.replicate_bytes -= f.payload.len();
                    }
                    f
                }
                MsgClass::Bulk => None,
            };
            match popped {
                Some(f) => {
                    report.evicted += 1;
                    self.dropped[class_slot(f.class)] += 1;
                }
                None => break,
            }
        }
    }

    /// The next frame for the **control** connection: strict priority `Vote`,
    /// then `Heartbeat`, then `Replicate`/`Normal` (FIFO).
    pub fn pop_control(&mut self) -> Option<OutFrame> {
        if let Some(f) = self.vote.pop_front() {
            return Some(f);
        }
        if let Some(f) = self.heartbeat.pop_front() {
            return Some(f);
        }
        let f = self.replicate.pop_front()?;
        self.replicate_bytes -= f.payload.len();
        Some(f)
    }

    /// The next frame for the **bulk** connection; returns its credit.
    pub fn pop_bulk(&mut self) -> Option<OutFrame> {
        let f = self.bulk.pop_front()?;
        self.bulk_bytes -= f.payload.len();
        Some(f)
    }

    /// Whether a `Bulk` frame of `len` bytes would be accepted now.
    #[must_use]
    pub fn bulk_credit(&self, len: usize) -> bool {
        self.bulk.is_empty() || self.bulk_bytes + len <= self.limits.bulk_bytes
    }

    /// Frames waiting for the control connection.
    #[must_use]
    pub fn control_len(&self) -> usize {
        self.vote.len() + self.heartbeat.len() + self.replicate.len()
    }

    /// Frames waiting for the bulk connection.
    #[must_use]
    pub fn bulk_len(&self) -> usize {
        self.bulk.len()
    }

    /// Queued frames of one class.
    #[must_use]
    pub fn len_of(&self, class: MsgClass) -> usize {
        match class {
            MsgClass::Vote => self.vote.len(),
            MsgClass::Heartbeat => self.heartbeat.len(),
            MsgClass::Bulk => self.bulk.len(),
            MsgClass::Replicate | MsgClass::Normal => {
                self.replicate.iter().filter(|f| f.class == class).count()
            }
        }
    }

    /// Payload bytes queued across every class.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.vote.iter().map(|f| f.payload.len()).sum::<usize>()
            + self
                .heartbeat
                .iter()
                .map(|f| f.payload.len())
                .sum::<usize>()
            + self.replicate_bytes
            + self.bulk_bytes
    }

    /// Frames dropped (evicted or superseded) so far for `class`.
    #[must_use]
    pub fn dropped(&self, class: MsgClass) -> u64 {
        self.dropped[class_slot(class)]
    }

    /// No frame queued anywhere.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.control_len() == 0 && self.bulk.is_empty()
    }

    /// Discard every queued frame (a peer removed or its address changed).
    pub fn clear(&mut self) {
        self.vote.clear();
        self.heartbeat.clear();
        self.replicate.clear();
        self.replicate_bytes = 0;
        self.bulk.clear();
        self.bulk_bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(class: MsgClass, key: Option<u64>, n: usize, tag: u8) -> OutFrame {
        OutFrame::new(7, class, key, vec![tag; n])
    }

    #[test]
    fn control_drains_in_strict_priority_order() {
        let mut o = PeerOutbox::new(OutboxLimits::default());
        o.push(f(MsgClass::Replicate, None, 10, 1));
        o.push(f(MsgClass::Heartbeat, None, 10, 2));
        o.push(f(MsgClass::Normal, None, 10, 3));
        o.push(f(MsgClass::Vote, None, 10, 4));
        o.push(f(MsgClass::Bulk, None, 10, 9));
        let order: Vec<u8> = std::iter::from_fn(|| o.pop_control())
            .map(|x| x.payload[0])
            .collect();
        assert_eq!(order, [4, 2, 1, 3], "vote, heartbeat, then FIFO");
        assert_eq!(o.pop_bulk().expect("bulk").payload[0], 9);
        assert!(o.is_empty());
    }

    #[test]
    fn a_vote_waits_behind_at_most_the_one_in_progress_frame() {
        // The writer pops one frame, "writes" it, then pops again: a vote
        // pushed while a frame is in progress is the very next frame, however
        // many replicate frames are queued.
        let mut o = PeerOutbox::new(OutboxLimits::default());
        for i in 0..1000u32 {
            o.push(f(MsgClass::Replicate, None, 64, (i % 200) as u8));
        }
        let _in_progress = o.pop_control().expect("frame");
        o.push(f(MsgClass::Vote, None, 8, 0xEE));
        assert_eq!(o.pop_control().expect("next").payload[0], 0xEE);
    }

    #[test]
    fn votes_drop_oldest_at_their_bound_and_are_counted() {
        let lim = OutboxLimits {
            vote_frames: 3,
            ..OutboxLimits::default()
        };
        let mut o = PeerOutbox::new(lim);
        let mut evicted = 0;
        for i in 0..10u8 {
            evicted += o.push(f(MsgClass::Vote, None, 1, i)).evicted;
        }
        assert_eq!(evicted, 7);
        assert_eq!(o.dropped(MsgClass::Vote), 7);
        let kept: Vec<u8> = std::iter::from_fn(|| o.pop_control())
            .map(|x| x.payload[0])
            .collect();
        assert_eq!(kept, [7, 8, 9], "the freshest votes survive");
    }

    #[test]
    fn heartbeat_with_the_same_key_coalesces_to_the_newest() {
        let mut o = PeerOutbox::new(OutboxLimits::default());
        o.push(f(MsgClass::Heartbeat, Some(1), 100, 1));
        o.push(f(MsgClass::Heartbeat, Some(2), 100, 2));
        let r = o.push(f(MsgClass::Heartbeat, Some(1), 100, 3));
        assert!(r.superseded);
        assert_eq!(o.len_of(MsgClass::Heartbeat), 2);
        let tags: Vec<u8> = std::iter::from_fn(|| o.pop_control())
            .map(|x| x.payload[0])
            .collect();
        assert_eq!(tags, [2, 3]);
        assert_eq!(o.dropped(MsgClass::Heartbeat), 1);
    }

    #[test]
    fn replicate_supersedes_per_stream_and_key_only() {
        let mut o = PeerOutbox::new(OutboxLimits::default());
        o.push(OutFrame::new(1, MsgClass::Replicate, Some(5), vec![1; 10]));
        o.push(OutFrame::new(2, MsgClass::Replicate, Some(5), vec![2; 10]));
        // Same stream, different key: kept.
        o.push(OutFrame::new(1, MsgClass::Replicate, Some(6), vec![3; 10]));
        // Same stream and key: replaces the first.
        let r = o.push(OutFrame::new(1, MsgClass::Replicate, Some(5), vec![4; 10]));
        assert!(r.superseded);
        let tags: Vec<u8> = std::iter::from_fn(|| o.pop_control())
            .map(|x| x.payload[0])
            .collect();
        assert_eq!(tags, [2, 3, 4]);
        // No key: never superseded.
        o.push(f(MsgClass::Replicate, None, 1, 1));
        let r = o.push(f(MsgClass::Replicate, None, 1, 2));
        assert!(!r.superseded);
    }

    #[test]
    fn replicate_is_bounded_in_bytes_and_frames_drop_oldest() {
        let lim = OutboxLimits {
            replicate_bytes: 100,
            replicate_frames: 1000,
            ..OutboxLimits::default()
        };
        let mut o = PeerOutbox::new(lim);
        for i in 0..20u8 {
            o.push(f(MsgClass::Replicate, None, 30, i));
            assert!(o.bytes() <= 100, "bound held: {}", o.bytes());
        }
        assert_eq!(o.len_of(MsgClass::Replicate), 3);
        assert_eq!(o.pop_control().expect("frame").payload[0], 17);
        // A frame bigger than the bound is still queued alone.
        let mut o = PeerOutbox::new(lim);
        let r = o.push(f(MsgClass::Replicate, None, 500, 1));
        assert!(r.accepted);
        assert_eq!(o.len_of(MsgClass::Replicate), 1);
    }

    #[test]
    fn bulk_blocks_the_producer_instead_of_dropping_and_credit_returns_on_pop() {
        let lim = OutboxLimits {
            bulk_bytes: 100,
            ..OutboxLimits::default()
        };
        let mut o = PeerOutbox::new(lim);
        assert!(o.push(f(MsgClass::Bulk, None, 60, 1)).accepted);
        assert!(o.bulk_credit(40));
        assert!(!o.bulk_credit(41));
        let r = o.push(f(MsgClass::Bulk, None, 50, 2));
        assert!(!r.accepted, "no credit: the producer waits");
        assert_eq!(o.dropped(MsgClass::Bulk), 0, "nothing dropped");
        assert_eq!(o.bulk_len(), 1);
        o.pop_bulk().expect("frame");
        assert!(o.push(f(MsgClass::Bulk, None, 50, 2)).accepted);
        // An oversized chunk into an empty queue is admitted.
        o.pop_bulk();
        assert!(o.push(f(MsgClass::Bulk, None, 1000, 3)).accepted);
    }

    #[test]
    fn queued_bytes_are_charged_and_released() {
        let b = QueuedBytes::new(1 << 20);
        let mut o = PeerOutbox::new(OutboxLimits::default()).with_budget(b.clone());
        o.push(f(MsgClass::Vote, None, 100, 1));
        o.push(f(MsgClass::Replicate, None, 200, 2));
        o.push(f(MsgClass::Bulk, None, 300, 3));
        assert_eq!(b.used(), 600);
        let popped = o.pop_control().expect("vote");
        assert_eq!(b.used(), 600, "a popped frame is still charged until sent");
        drop(popped);
        assert_eq!(b.used(), 500);
        o.clear();
        assert_eq!(b.used(), 0);
    }

    #[test]
    fn dropped_frames_release_their_charge() {
        let b = QueuedBytes::new(1 << 20);
        let lim = OutboxLimits {
            vote_frames: 2,
            ..OutboxLimits::default()
        };
        let mut o = PeerOutbox::new(lim).with_budget(b.clone());
        for _ in 0..50 {
            o.push(f(MsgClass::Vote, None, 10, 1));
        }
        assert_eq!(b.used(), 20);
    }

    #[test]
    fn random_workloads_never_exceed_any_bound() {
        // A deterministic LCG, so this pure property test needs no Env.
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let lim = OutboxLimits {
            vote_frames: 8,
            heartbeat_frames: 8,
            replicate_frames: 16,
            replicate_bytes: 1000,
            bulk_bytes: 500,
        };
        let mut o = PeerOutbox::new(lim);
        for _ in 0..20_000 {
            match next() % 9 {
                0..=5 => {
                    let class = MsgClass::ALL[(next() % 5) as usize];
                    let key = (next() % 3 == 0).then(|| next() % 4);
                    let len = (next() % 200) as usize;
                    o.push(OutFrame::new(next() % 3, class, key, vec![0; len]));
                }
                6 | 7 => {
                    o.pop_control();
                }
                _ => {
                    o.pop_bulk();
                }
            }
            assert!(o.len_of(MsgClass::Vote) <= 8);
            assert!(o.len_of(MsgClass::Heartbeat) <= 8);
            assert!(
                o.control_len() - o.len_of(MsgClass::Vote) - o.len_of(MsgClass::Heartbeat) <= 16
            );
            assert!(o.replicate_bytes <= 1000 || o.replicate.len() == 1);
            assert!(o.bulk_bytes <= 500 || o.bulk.len() == 1);
        }
    }

    /// Seeded writer model (ADR 0080 cell 9): the writer is 20x slower than
    /// the producers, so the peer looks stalled. Per round the producers push
    /// `burst` replicate frames, one heartbeat per group, and a vote every
    /// 50 rounds. Returns (rounds a vote waited in frames written, peak
    /// queued frames, peak heartbeat queue).
    fn stalled_writer_model(seed: u64, priority: bool, coalesce: bool) -> (usize, usize, usize) {
        let mut x = seed | 1;
        let mut rnd = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let mut o = PeerOutbox::new(OutboxLimits::default());
        // Control E's FIFO: one queue, no class order.
        let mut fifo: std::collections::VecDeque<MsgClass> = Default::default();
        let (mut worst_wait, mut peak, mut peak_hb) = (0usize, 0usize, 0usize);
        let mut vote_pending_since: Option<usize> = None;
        let mut written = 0usize;
        for round in 0..2000usize {
            for _ in 0..(rnd() % 20) {
                if priority {
                    o.push(f(MsgClass::Replicate, Some(rnd() % 64), 128, 1));
                } else {
                    fifo.push_back(MsgClass::Replicate);
                }
            }
            for g in 0..16u64 {
                let key = if coalesce { Some(g) } else { None };
                if priority {
                    o.push(f(MsgClass::Heartbeat, key, 64, 2));
                } else {
                    fifo.push_back(MsgClass::Heartbeat);
                }
            }
            if round % 50 == 0 && vote_pending_since.is_none() {
                vote_pending_since = Some(written);
                if priority {
                    o.push(f(MsgClass::Vote, None, 16, 3));
                } else {
                    fifo.push_back(MsgClass::Vote);
                }
            }
            // The writer drains only one frame per round.
            let got = if priority {
                o.pop_control().map(|x| x.class)
            } else {
                fifo.pop_front()
            };
            if let Some(c) = got {
                written += 1;
                if c == MsgClass::Vote
                    && let Some(since) = vote_pending_since.take()
                {
                    worst_wait = worst_wait.max(written - since);
                }
            }
            peak = peak.max(if priority {
                o.control_len()
            } else {
                fifo.len()
            });
            peak_hb = peak_hb.max(o.len_of(MsgClass::Heartbeat));
        }
        (worst_wait, peak, peak_hb)
    }

    #[test]
    fn cell9_a_stalled_peer_keeps_votes_prompt_and_the_queue_bounded() {
        for seed in 1..=16u64 {
            let (vote_wait, peak, peak_hb) = stalled_writer_model(seed, true, true);
            assert!(
                vote_wait <= 2,
                "seed {seed}: vote waited {vote_wait} frames"
            );
            assert!(peak <= 4096 + 4096 + 1024, "seed {seed}: peak {peak}");
            assert!(
                peak_hb <= 16,
                "seed {seed}: heartbeats coalesce per group, got {peak_hb}"
            );
        }
    }

    /// Control E: a single FIFO (no class priority) makes the vote wait behind
    /// the whole backlog -- the oracle's vote bound must FAIL.
    #[test]
    fn control_e_fifo_without_class_priority_delays_the_vote() {
        let (vote_wait, ..) = stalled_writer_model(3, false, true);
        assert!(vote_wait > 100, "FIFO vote waited only {vote_wait} frames");
    }

    /// Control F: coalescing off lets the heartbeat queue grow far past one per
    /// group -- the coalescing oracle must FAIL.
    #[test]
    fn control_f_without_coalescing_heartbeats_pile_up() {
        let (_, _, peak_hb) = stalled_writer_model(3, true, false);
        assert!(peak_hb > 16, "heartbeat queue peaked at only {peak_hb}");
    }
}
