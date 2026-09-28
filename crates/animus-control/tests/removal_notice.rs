//! Issue #1061: explicit removal notice (`RaftMsg::Removed` /
//! `RaftMsg::RemovedAck`, ADR 0058's 2026-09-28 departing-peer amendment).
//!
//! A voter removed from a group used to learn of its own removal only through
//! the log: the leader kept a leader-local `departing` map and replicated the
//! removing entry to the peer until it acked. Two holes: (1) a departing peer
//! whose `next_index` fell behind the leader's compacted prefix was shipped a
//! full chunked `InstallSnapshot` — restarted from chunk 0 by every later
//! compaction, forever, even for a peer that is dead; (2) `become_leader`
//! cleared `departing` and never re-derived it, so a peer the previous leader
//! had not finished telling was never told again and stayed hosted as a
//! zombie.
//!
//! These are bare-`RaftCore` cells over a tiny deterministic message
//! harness (no `Env`, no sim executor): every scenario is a pure function of
//! its seed, replayable with `ANIMUS_SEED=<seed> cargo test <name>` (the seed
//! feeds the per-node election-timer entropy).

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Duration;

use animus_control::{
    DEPARTING_NOTICE_GIVE_UP, MetaCommand, Metadata, ProposeResult, RaftCore, RaftMsg, Role,
};
use animus_env::{Nanos, NodeId, nid};

type Core = RaftCore<MetaCommand, Metadata>;
type Msg = RaftMsg<MetaCommand>;

const TICK: Duration = Duration::from_millis(10);

fn seed_from_env(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn idn(n: &NodeId) -> u64 {
    n.as_str().trim_start_matches('n').parse().unwrap()
}

fn kind(m: &Msg) -> &'static str {
    match m {
        RaftMsg::PreVote { .. } => "PreVote",
        RaftMsg::PreVoteResp { .. } => "PreVoteResp",
        RaftMsg::RequestVote { .. } => "RequestVote",
        RaftMsg::RequestVoteResp { .. } => "RequestVoteResp",
        RaftMsg::AppendEntries { .. } => "AppendEntries",
        RaftMsg::AppendEntriesResp { .. } => "AppendEntriesResp",
        RaftMsg::InstallSnapshot { .. } => "InstallSnapshot",
        RaftMsg::InstallSnapshotResp { .. } => "InstallSnapshotResp",
        RaftMsg::Removed { .. } => "Removed",
        RaftMsg::RemovedAck { .. } => "RemovedAck",
        _ => "Other",
    }
}

/// One message actually handed to a link (whether or not the link then
/// dropped it): the record every traffic assertion reads.
#[derive(Clone, Debug)]
struct Sent {
    at: Nanos,
    from: NodeId,
    to: NodeId,
    kind: &'static str,
    msg: Msg,
}

struct Net {
    cores: BTreeMap<NodeId, Core>,
    now: u64,
    /// Directed links that drop everything.
    cut: BTreeSet<(NodeId, NodeId)>,
    /// Crashed nodes: neither tick nor receive.
    down: BTreeSet<NodeId>,
    /// Nodes whose `PreVote`/`RequestVote` are dropped (everything else they
    /// send still gets through): isolates what the LEADER does on its own
    /// from the receiver-initiated reply to a returning campaigner.
    no_campaign: BTreeSet<NodeId>,
    seed: u64,
    ctr: u64,
    log: Vec<Sent>,
}

impl Net {
    fn new(seed: u64, ids: &[u64]) -> Self {
        let all: Vec<NodeId> = ids.iter().copied().map(nid).collect();
        let mut n = Net {
            cores: BTreeMap::new(),
            now: 0,
            cut: BTreeSet::new(),
            down: BTreeSet::new(),
            no_campaign: BTreeSet::new(),
            seed,
            ctr: 0,
            log: Vec::new(),
        };
        for id in &all {
            let e = n.entropy();
            n.cores
                .insert(id.clone(), RaftCore::new(id.clone(), &all, Nanos(0), e));
        }
        n
    }

    /// Add a brand-new replica that is NOT in any config yet (an empty-log
    /// joiner, as the host reconciler creates for a re-added replica).
    fn add_fresh(&mut self, id: u64, voters: &[u64]) {
        let all: Vec<NodeId> = voters.iter().copied().map(nid).collect();
        let e = self.entropy();
        self.cores
            .insert(nid(id), RaftCore::new(nid(id), &all, Nanos(self.now), e));
    }

    fn entropy(&mut self) -> u64 {
        self.ctr += 1;
        splitmix64(self.seed ^ self.ctr)
    }

    fn now(&self) -> Nanos {
        Nanos(self.now)
    }

    fn core(&self, id: u64) -> &Core {
        &self.cores[&nid(id)]
    }

    fn core_mut(&mut self, id: u64) -> &mut Core {
        self.cores.get_mut(&nid(id)).unwrap()
    }

    fn isolate(&mut self, id: u64) {
        for other in self.cores.keys().cloned().collect::<Vec<_>>() {
            if other != nid(id) {
                self.cut.insert((nid(id), other.clone()));
                self.cut.insert((other, nid(id)));
            }
        }
    }

    /// Drop everything `id` SENDS (it can still hear).
    fn mute(&mut self, id: u64) {
        for other in self.cores.keys().cloned().collect::<Vec<_>>() {
            if other != nid(id) {
                self.cut.insert((nid(id), other));
            }
        }
    }

    fn heal_all(&mut self) {
        self.cut.clear();
        self.no_campaign.clear();
    }

    fn crash(&mut self, id: u64) {
        self.down.insert(nid(id));
    }

    /// What the real driver does after every core step: build the snapshot
    /// image a leader asked for (`Metadata` is `DRIVER_APPLIED`: the core
    /// never holds the bytes itself) and drain a completed install.
    fn drive_side_effects(core: &mut Core) {
        let last = core.last_log_index();
        core.mark_durable_through(last);
        if core.take_snapshot_needed() {
            core.set_snapshot_blob(vec![b'x'; 300]);
        }
        let _ = core.drain_pending_install();
    }

    /// Deliver `outs` (all sent by `from`) and everything they cause, to a
    /// fixpoint.
    fn pump(&mut self, from: NodeId, outs: Vec<(NodeId, Msg)>) {
        let mut q: VecDeque<(NodeId, NodeId, Msg)> = outs
            .into_iter()
            .map(|(to, m)| (from.clone(), to, m))
            .collect();
        let mut budget = 200_000usize;
        while let Some((f, to, m)) = q.pop_front() {
            budget -= 1;
            assert!(budget > 0, "seed={:#x}: message storm", self.seed);
            self.log.push(Sent {
                at: self.now(),
                from: f.clone(),
                to: to.clone(),
                kind: kind(&m),
                msg: m.clone(),
            });
            if (self.no_campaign.contains(&f)
                && matches!(m, RaftMsg::PreVote { .. } | RaftMsg::RequestVote { .. }))
                || self.cut.contains(&(f.clone(), to.clone()))
                || self.down.contains(&f)
                || self.down.contains(&to)
            {
                continue;
            }
            let e = self.entropy();
            let now = self.now();
            let Some(core) = self.cores.get_mut(&to) else {
                continue;
            };
            let outs = core.handle(f, m, now, e);
            Self::drive_side_effects(core);
            for (dst, msg) in outs {
                q.push_back((to.clone(), dst, msg));
            }
        }
    }

    fn step(&mut self, dur: Duration) {
        let ticks = dur.as_nanos() / TICK.as_nanos();
        for _ in 0..ticks {
            self.now += TICK.as_nanos() as u64;
            let ids: Vec<NodeId> = self.cores.keys().cloned().collect();
            for id in ids {
                if self.down.contains(&id) {
                    continue;
                }
                let e = self.entropy();
                let now = self.now();
                let core = self.cores.get_mut(&id).unwrap();
                let outs = core.tick(now, e);
                Self::drive_side_effects(core);
                self.pump(id, outs);
            }
        }
    }

    fn leader(&self) -> Option<u64> {
        let ls: Vec<u64> = self
            .cores
            .iter()
            .filter(|(id, c)| !self.down.contains(*id) && c.role() == Role::Leader)
            .map(|(id, _)| idn(id))
            .collect();
        (ls.len() == 1).then(|| ls[0])
    }

    fn elect(&mut self) -> u64 {
        for _ in 0..100 {
            if let Some(l) = self.leader() {
                // Let the election no-op commit everywhere.
                self.step(Duration::from_millis(300));
                return l;
            }
            self.step(Duration::from_millis(100));
        }
        panic!("seed={:#x}: no leader elected", self.seed);
    }

    /// Propose `n` no-ops on `l`, replicating after each (a continuous
    /// writer's shape: every propose wakes the replicator).
    fn write(&mut self, l: u64, n: usize) {
        for _ in 0..n {
            let now = self.now();
            let c = self.core_mut(l);
            assert!(
                matches!(c.propose(MetaCommand::NoOp), ProposeResult::Accepted { .. }),
                "seed={:#x}: leader must accept a write",
                self.seed
            );
            let outs = c.replicate_now(now);
            Self::drive_side_effects(c);
            self.pump(nid(l), outs);
        }
    }

    fn replicate(&mut self, l: u64) {
        let now = self.now();
        let c = self.core_mut(l);
        let outs = c.replicate_now(now);
        self.pump(nid(l), outs);
    }

    /// Compact node `id`'s log through everything it has applied.
    fn compact(&mut self, id: u64) {
        let c = self.core_mut(id);
        let upto = c.last_applied();
        c.snapshot_upto(upto);
    }

    fn compact_to(&mut self, id: u64, upto: u64) {
        self.core_mut(id).snapshot_upto(upto);
    }

    fn sent_after(&self, mark: usize, to: u64, k: &str) -> Vec<&Sent> {
        self.log[mark..]
            .iter()
            .filter(|s| s.to == nid(to) && s.kind == k)
            .collect()
    }

    fn count_after(&self, mark: usize, to: u64, k: &str) -> usize {
        self.sent_after(mark, to, k).len()
    }

    fn remove_voter(&mut self, l: u64, victim: u64) -> Vec<u64> {
        let mut voters: BTreeSet<NodeId> = self.core(l).config();
        assert!(voters.remove(&nid(victim)));
        let now = self.now();
        let c = self.core_mut(l);
        assert!(
            matches!(
                c.change_membership(voters.clone()),
                ProposeResult::Accepted { .. }
            ),
            "seed={:#x}: removing n{victim} must be accepted",
            self.seed
        );
        let outs = c.replicate_now(now);
        self.pump(nid(l), outs);
        voters.iter().map(idn).collect()
    }
}

fn set(ids: &[u64]) -> BTreeSet<NodeId> {
    ids.iter().copied().map(nid).collect()
}

fn a_follower(l: u64, ids: &[u64]) -> u64 {
    *ids.iter().find(|&&i| i != l).unwrap()
}

// ---------------------------------------------------------------------------
// (a) a departing peer behind the compacted log gets notices, never snapshots
// ---------------------------------------------------------------------------

fn scenario_behind_compacted_log(seed: u64) {
    let ids = [0u64, 1, 2];
    let mut net = Net::new(seed, &ids);
    let l = net.elect();
    let v = a_follower(l, &ids);

    // The victim falls off the network, then is removed by the leader (the
    // remaining pair {l, other} is a majority of itself).
    net.write(l, 5);
    net.isolate(v);
    let remaining = net.remove_voter(l, v);
    net.step(Duration::from_millis(500));
    assert!(
        net.core(l).departing_peers().contains(&nid(v)),
        "seed={seed:#x}: the unreachable removed peer must be tracked as departing"
    );

    // A continuous writer, then compaction on both survivors: the victim is
    // now far behind the leader's snapshot.
    for _ in 0..40 {
        net.write(l, 10);
        net.step(Duration::from_millis(50));
    }
    for &n in &remaining {
        net.compact(n);
    }
    assert!(
        net.core(l).snapshot_index() > net.core(v).last_log_index(),
        "seed={seed:#x}: precondition — the victim must be behind the compacted prefix"
    );

    // Phase 1 — the leader can reach the peer again but the peer cannot speak
    // (so its own election timer cannot reach the leader and trigger the
    // receiver-initiated reply, and no ack can return): what the LEADER does
    // on its own, under a continuous writer, is what is measured.
    let mark = net.log.len();
    net.heal_all();
    net.mute(v);
    for _ in 0..60 {
        net.write(l, 5);
        net.step(Duration::from_millis(50));
    }
    assert_eq!(
        net.count_after(mark, v, "InstallSnapshot"),
        0,
        "seed={seed:#x}: a departing peer must never be shipped a snapshot (issue #1061)"
    );
    assert!(
        net.count_after(mark, v, "Removed") >= 1,
        "seed={seed:#x}: the leader must have sent the removal notice on its own"
    );
    assert!(
        net.core(v).removed_by_leader(),
        "seed={seed:#x}: the peer must have recorded its removal"
    );
    assert!(
        net.core(l).departing_peers().contains(&nid(v)),
        "seed={seed:#x}: with no ack yet the peer is still owed the notice"
    );
    // The flag is separate from the log-derived config, which the notice must
    // never touch.
    assert!(
        net.core(v).config().contains(&nid(v)),
        "seed={seed:#x}: the notice must not rewrite the recipient's log-derived config"
    );
    // The notice must have carried a membership that excludes the recipient.
    for s in net.sent_after(mark, v, "Removed") {
        if let RaftMsg::Removed {
            config, learners, ..
        } = &s.msg
        {
            assert!(!config.contains(&nid(v)) && !learners.contains(&nid(v)));
        }
    }

    // Phase 2 — the peer can answer: the ack drops it, and it never campaigns
    // again.
    net.heal_all();
    for _ in 0..40 {
        net.write(l, 5);
        net.step(Duration::from_millis(100));
    }
    assert!(
        net.core(l).departing_peers().is_empty(),
        "seed={seed:#x}: the ack must drop the peer from `departing`"
    );
    let acked_at = net
        .log
        .iter()
        .find(|s| s.from == nid(v) && s.kind == "RemovedAck")
        .map(|s| s.at)
        .expect("an ack");
    let campaigns_after = net
        .log
        .iter()
        .filter(|s| {
            s.from == nid(v) && s.at.0 > acked_at.0 && matches!(s.kind, "PreVote" | "RequestVote")
        })
        .count();
    assert_eq!(
        campaigns_after, 0,
        "seed={seed:#x}: a removed peer must stop campaigning once told"
    );
    assert_eq!(
        net.count_after(mark, v, "InstallSnapshot"),
        0,
        "seed={seed:#x}: still no snapshot after the ack"
    );
}

#[test]
fn a_departing_peer_behind_the_compacted_log_gets_a_notice_not_a_snapshot() {
    scenario_behind_compacted_log(seed_from_env(0x1061_1001));
}

#[test]
fn a_departing_peer_behind_the_compacted_log_corpus() {
    for i in 0..8u64 {
        scenario_behind_compacted_log(splitmix64(0x1061_1001 ^ i));
    }
}

// ---------------------------------------------------------------------------
// (b) partition + leader change: the peer must still learn
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Fold {
    /// Nothing compacted: the new leader's log still holds the removal entry
    /// AND the peer's whole gap — a plain `AppendEntries` catch-up.
    None,
    /// Survivors compact up to just BEFORE the removing entry: the removal is
    /// still visible in the new leader's log (so it re-derives `departing`),
    /// but the peer is behind the compacted prefix — the notice path.
    BeforeRemoval,
    /// Survivors compact through the removal: the new leader cannot see it at
    /// all (the documented re-derivation bound).
    ThroughRemoval,
}

/// Four voters; `v` is removed while partitioned; the leader then dies and a
/// new one takes over before `v` is healed.
fn scenario_partitioned_peer_across_leader_change(seed: u64, fold: Fold) {
    let ids = [0u64, 1, 2, 3];
    let mut net = Net::new(seed, &ids);
    let l1 = net.elect();
    let v = a_follower(l1, &ids);

    net.write(l1, 5);
    net.isolate(v);
    let remaining = net.remove_voter(l1, v);
    let removal_index = net.core(l1).last_log_index();
    net.step(Duration::from_millis(500));
    net.write(l1, 30);
    net.step(Duration::from_millis(300));
    match fold {
        Fold::None => {}
        Fold::BeforeRemoval => {
            for &n in &remaining {
                net.compact_to(n, removal_index - 1);
            }
        }
        Fold::ThroughRemoval => {
            for &n in &remaining {
                net.compact(n);
            }
        }
    }

    // The leader that owes the notice dies; a survivor takes over. The
    // victim is still partitioned.
    net.crash(l1);
    let mut l2 = None;
    for _ in 0..100 {
        net.step(Duration::from_millis(100));
        if let Some(l) = net.leader() {
            l2 = Some(l);
            break;
        }
    }
    let l2 = l2.unwrap_or_else(|| panic!("seed={seed:#x}: survivors must elect a new leader"));
    assert_ne!(l2, l1);
    net.step(Duration::from_millis(500));
    match fold {
        Fold::ThroughRemoval => assert!(
            net.core(l2).departing_peers().is_empty(),
            "seed={seed:#x}: with the removal folded into a snapshot the new leader cannot \
             re-derive it (the documented bound) — the receiver-initiated reply must cover it"
        ),
        _ => assert!(
            net.core(l2).departing_peers().contains(&nid(v)),
            "seed={seed:#x} {fold:?}: a new leader must re-derive the peers still owed a notice \
             from the config entries in its own log (`become_leader` used to just forget them)"
        ),
    }

    // A zombie's real vote request must not win a single vote from a voter
    // that has the removal entry: its log lacks that entry, so it is not
    // up to date (the old, unstated reason a removed peer cannot hijack an
    // election).
    let voter = *remaining.iter().find(|&&n| n != l1 && n != l2).unwrap();
    let term = net.core(voter).term() + 5;
    let (zombie_last, now) = (net.core(v).last_log_index(), net.now());
    let resp = net.core_mut(voter).handle(
        nid(v),
        RaftMsg::RequestVote {
            term,
            candidate: nid(v),
            last_log_index: zombie_last,
            last_log_term: 1,
        },
        now,
        1,
    );
    assert!(
        resp.iter()
            .all(|(_, m)| !matches!(m, RaftMsg::RequestVoteResp { granted: true, .. })),
        "seed={seed:#x}: a zombie missing the removal entry must not be granted a vote"
    );
    // Let the cluster settle after that (the injected higher term may have
    // forced a step-down and re-election).
    net.step(Duration::from_secs(2));
    let l2 = net
        .leader()
        .unwrap_or_else(|| panic!("seed={seed:#x}: survivors must re-elect"));

    // Heal. When the removal is still visible to the leader, its own
    // schedule is what is measured (the peer's campaigns are dropped, so the
    // receiver-initiated reply cannot be what teaches it); when it is not,
    // the reply to the peer's own election is the only channel there is.
    net.heal_all();
    if fold != Fold::ThroughRemoval {
        net.no_campaign.insert(nid(v));
    }
    let snapshots_before = net.log.len();
    let mut learned = false;
    for _ in 0..100 {
        net.step(Duration::from_millis(100));
        let c = net.core(v);
        if c.removed_by_leader() || !c.config().contains(&nid(v)) {
            learned = true;
            break;
        }
    }
    assert!(
        learned,
        "seed={seed:#x} {fold:?}: the healed peer never learned it was removed — a zombie \
         (leader n{l2}, peer role {:?} term {})",
        net.core(v).role(),
        net.core(v).term()
    );
    assert_eq!(
        net.count_after(snapshots_before, v, "InstallSnapshot"),
        0,
        "seed={seed:#x} {fold:?}: never a snapshot to a departing peer"
    );
    // And once it has, it must not campaign any more, and the leader stops
    // owing it anything.
    net.heal_all();
    net.step(Duration::from_secs(2));
    let mark = net.log.len();
    net.step(Duration::from_secs(5));
    assert_eq!(
        net.log[mark..]
            .iter()
            .filter(|s| s.from == nid(v) && matches!(s.kind, "PreVote" | "RequestVote"))
            .count(),
        0,
        "seed={seed:#x}: a removed peer must not keep campaigning"
    );
    assert!(
        net.core(l2).departing_peers().is_empty(),
        "seed={seed:#x}: the leader must have stopped owing it anything"
    );
}

#[test]
fn a_partitioned_departing_peer_learns_after_a_leader_change_via_the_log() {
    scenario_partitioned_peer_across_leader_change(seed_from_env(0x1061_2001), Fold::None);
}

#[test]
fn a_partitioned_departing_peer_behind_the_compacted_log_learns_via_the_rederived_notice() {
    scenario_partitioned_peer_across_leader_change(seed_from_env(0x1061_2004), Fold::BeforeRemoval);
}

#[test]
fn a_partitioned_departing_peer_learns_via_the_election_reply_when_the_removal_is_folded() {
    scenario_partitioned_peer_across_leader_change(
        seed_from_env(0x1061_2002),
        Fold::ThroughRemoval,
    );
}

#[test]
fn a_partitioned_departing_peer_learns_corpus() {
    for i in 0..8u64 {
        let s = splitmix64(0x1061_2003 ^ i);
        for fold in [Fold::None, Fold::BeforeRemoval, Fold::ThroughRemoval] {
            scenario_partitioned_peer_across_leader_change(s, fold);
        }
    }
}

// ---------------------------------------------------------------------------
// (c) stale-notice safety
// ---------------------------------------------------------------------------

fn notice(term: u64, removal_index: u64, removal_term: u64, cfg: &[u64], learners: &[u64]) -> Msg {
    RaftMsg::Removed {
        term,
        removal_index,
        removal_term,
        config: set(cfg),
        learners: set(learners),
    }
}

#[test]
fn a_notice_from_an_older_term_is_ignored_and_answered_with_our_term() {
    let mut n1: Core = RaftCore::new(nid(1), &[nid(0), nid(1), nid(2)], Nanos(0), 0);
    // Adopt term 5 through an ordinary vote request.
    n1.handle(
        nid(2),
        RaftMsg::RequestVote {
            term: 5,
            candidate: nid(2),
            last_log_index: 0,
            last_log_term: 0,
        },
        Nanos(10_000_000),
        0,
    );
    assert_eq!(n1.term(), 5);
    let outs = n1.handle(nid(0), notice(3, 10, 3, &[0, 2], &[]), Nanos(20_000_000), 0);
    assert!(
        !n1.removed_by_leader(),
        "an older-term notice must be ignored"
    );
    assert!(
        matches!(
            outs.as_slice(),
            [(to, RaftMsg::RemovedAck { term: 5, removal_index: 0 })] if *to == nid(0)
        ),
        "a stale sender must learn our term (so it steps down); got {outs:?}"
    );
}

#[test]
fn a_notice_naming_the_recipient_as_a_member_is_ignored() {
    let mut n1: Core = RaftCore::new(nid(1), &[nid(0), nid(1), nid(2)], Nanos(0), 0);
    for (cfg, learners) in [(&[0u64, 1, 2][..], &[][..]), (&[0, 2][..], &[1][..])] {
        let outs = n1.handle(nid(0), notice(1, 10, 1, cfg, learners), Nanos(1), 0);
        assert!(outs.is_empty());
        assert!(!n1.removed_by_leader());
    }
}

/// A notice for a removal that is OLDER than a re-add the recipient already
/// holds must never mark it removed.
#[test]
fn a_delayed_notice_older_than_a_re_add_the_node_already_knows_is_ignored() {
    let seed = seed_from_env(0x1061_3001);
    let ids = [0u64, 1, 2, 3];
    let mut net = Net::new(seed, &ids);
    let l = net.elect();
    let v = a_follower(l, &ids);

    // Remove v while it is reachable (it applies the removal), capture the
    // notice the leader would have sent for it, then re-add it.
    net.write(l, 5);
    let idx_before = net.core(l).last_log_index();
    net.remove_voter(l, v);
    net.step(Duration::from_secs(1));
    let removal_index = idx_before + 1;
    let removal_term = net.core(l).term();
    let old = notice(
        net.core(l).term(),
        removal_index,
        removal_term,
        &ids.iter().copied().filter(|&i| i != v).collect::<Vec<_>>(),
        &[],
    );
    assert!(!net.core(v).config().contains(&nid(v)));

    assert!(matches!(
        net.core_mut(l).add_learner(nid(v)),
        ProposeResult::Accepted { .. }
    ));
    net.replicate(l);
    net.step(Duration::from_secs(1));
    assert!(
        net.core(v).learners().contains(&nid(v)),
        "seed={seed:#x}: precondition — the re-added node holds its own re-add entry"
    );

    // The delayed notice finally arrives.
    let now = net.now();
    let outs = net.core_mut(v).handle(nid(l), old, now, 0);
    assert!(outs.is_empty(), "seed={seed:#x}: no ack for a stale notice");
    assert!(
        !net.core(v).removed_by_leader(),
        "seed={seed:#x}: a delayed notice must never un-member a node re-added since"
    );
}

/// A notice older than a re-add the recipient has NOT yet received is
/// accepted (it was true when sent) but the flag is cleared the moment the
/// re-adding entry lands — and a fresh empty-log replica receiving an old
/// notice is equally harmless.
#[test]
fn a_delayed_notice_is_cleared_by_the_re_add_when_it_arrives() {
    let seed = seed_from_env(0x1061_3002);
    let ids = [0u64, 1, 2, 3];
    let mut net = Net::new(seed, &ids);
    let l = net.elect();
    let v = a_follower(l, &ids);

    net.write(l, 5);
    net.isolate(v);
    let idx_before = net.core(l).last_log_index();
    net.remove_voter(l, v);
    net.step(Duration::from_secs(1));
    let removal_index = idx_before + 1;
    let term = net.core(l).term();
    // Re-add while it is still partitioned.
    assert!(matches!(
        net.core_mut(l).add_learner(nid(v)),
        ProposeResult::Accepted { .. }
    ));
    net.replicate(l);
    net.step(Duration::from_secs(1));

    // The (older) notice reaches the still-uninformed peer first.
    let now = net.now();
    let outs = net.core_mut(v).handle(
        nid(l),
        notice(term, removal_index, term, &[0, 1, 2, 3], &[]).clone(),
        now,
        0,
    );
    // (Its config field names v as a member, so that copy is rejected as
    // malformed — build the honest one, excluding v.)
    assert!(outs.is_empty() && !net.core(v).removed_by_leader());
    let others: Vec<u64> = ids.iter().copied().filter(|&i| i != v).collect();
    let outs = net.core_mut(v).handle(
        nid(l),
        notice(term, removal_index, term, &others, &[]),
        now,
        0,
    );
    assert!(
        net.core(v).removed_by_leader() && outs.len() == 1,
        "seed={seed:#x}: an uninformed peer legitimately accepts a notice that was true when sent"
    );

    // Heal: the re-adding entry arrives and clears it.
    net.heal_all();
    net.step(Duration::from_secs(3));
    assert!(
        !net.core(v).removed_by_leader(),
        "seed={seed:#x}: the re-add entry/snapshot must clear the stale flag"
    );
    assert!(
        net.core(v).learners().contains(&nid(v)),
        "seed={seed:#x}: the re-added node ends up a member"
    );
}

#[test]
fn a_fresh_empty_log_replica_receiving_an_old_notice_is_harmless() {
    let seed = seed_from_env(0x1061_3003);
    let ids = [0u64, 1, 2];
    let mut net = Net::new(seed, &ids);
    let l = net.elect();
    net.write(l, 20);
    // Fold history into a snapshot so the joiner needs a real install.
    for &n in &ids {
        net.compact(n);
    }

    // A brand-new replica with the recycled id 7 (empty log, in no config).
    net.add_fresh(7, &ids);
    let now = net.now();
    let term = net.core(l).term();
    let outs = net
        .core_mut(7)
        .handle(nid(l), notice(term, 3, 1, &ids, &[]), now, 0);
    assert!(net.core(7).removed_by_leader() && outs.len() == 1);
    assert!(!net.core(7).is_learner());

    // The leader adds it; the snapshot/entries carrying the membership clear
    // the flag, and it converges.
    assert!(matches!(
        net.core_mut(l).add_learner(nid(7)),
        ProposeResult::Accepted { .. }
    ));
    net.replicate(l);
    net.step(Duration::from_secs(3));
    assert!(
        !net.core(7).removed_by_leader() && net.core(7).learners().contains(&nid(7)),
        "seed={seed:#x}: an old notice must not outlive the replica being added \
         (flag={} learners={:?} last={} snap={} commit={} leader_last={} leader_snap={})",
        net.core(7).removed_by_leader(),
        net.core(7).learners(),
        net.core(7).last_log_index(),
        net.core(7).snapshot_index(),
        net.core(7).commit_index(),
        net.core(l).last_log_index(),
        net.core(l).snapshot_index(),
    );
    assert!(net.core(7).commit_index() >= net.core(l).commit_index().saturating_sub(2));
}

// ---------------------------------------------------------------------------
// (d) a dead departing peer costs a bounded trickle of notices, then nothing
// ---------------------------------------------------------------------------

#[test]
fn a_dead_departing_peer_costs_only_bounded_notices_and_is_eventually_dropped() {
    let seed = seed_from_env(0x1061_4001);
    let ids = [0u64, 1, 2];
    let mut net = Net::new(seed, &ids);
    let l = net.elect();
    let v = a_follower(l, &ids);

    net.write(l, 5);
    net.crash(v);
    let remaining = net.remove_voter(l, v);
    net.step(Duration::from_millis(500));
    for _ in 0..30 {
        net.write(l, 10);
        net.step(Duration::from_millis(50));
    }
    for &n in &remaining {
        net.compact(n);
    }
    net.write(l, 10);

    // A continuous writer for a minute of virtual time, compacting as it goes
    // (which used to restart a snapshot transfer on every compaction).
    let mark = net.log.len();
    for round in 0..1200u32 {
        net.write(l, 1);
        net.step(Duration::from_millis(50));
        if round % 100 == 99 {
            for &n in &remaining {
                net.compact(n);
            }
        }
    }
    assert_eq!(
        net.count_after(mark, v, "InstallSnapshot"),
        0,
        "seed={seed:#x}: never a snapshot to a departing peer, dead or alive"
    );
    let total_to_v = net.log[mark..].iter().filter(|s| s.to == nid(v)).count();
    let notices = net.count_after(mark, v, "Removed");
    // 60s at the capped steady schedule (one per ~1.6s) plus the ramp-up.
    assert!(
        notices <= 60,
        "seed={seed:#x}: {notices} notices in 60s — must be the capped backoff schedule"
    );
    assert!(
        total_to_v <= 80,
        "seed={seed:#x}: {total_to_v} messages of any kind to a silent departing peer in 60s of \
         a continuous writer — the send gate must thin catch-up traffic too"
    );
    assert!(
        net.core(l).departing_peers().contains(&nid(v)),
        "seed={seed:#x}: still owed within the give-up bound"
    );

    // Past the give-up bound it is dropped and the group can quiesce again.
    net.step(DEPARTING_NOTICE_GIVE_UP + Duration::from_secs(5));
    assert!(
        net.core(l).departing_peers().is_empty(),
        "seed={seed:#x}: a peer silent past the give-up bound must be dropped"
    );
    net.write(l, 3);
    net.step(Duration::from_millis(500));
    let c = net.core(l);
    assert_eq!(
        c.commit_index(),
        c.last_log_index(),
        "seed={seed:#x}: liveness"
    );
}

/// An ordinary (log-covered) departing peer that is alive keeps being served
/// at full rate and is dropped as soon as it acks the removing entry — the
/// pre-existing behaviour must be unchanged.
#[test]
fn a_reachable_departing_peer_learns_through_the_log_and_is_dropped() {
    let seed = seed_from_env(0x1061_4002);
    let ids = [0u64, 1, 2];
    let mut net = Net::new(seed, &ids);
    let l = net.elect();
    let v = a_follower(l, &ids);
    net.write(l, 5);
    let mark = net.log.len();
    net.remove_voter(l, v);
    net.step(Duration::from_secs(1));
    assert!(net.core(l).departing_peers().is_empty());
    assert!(
        !net.core(v).config().contains(&nid(v)),
        "learned through the log"
    );
    assert_eq!(net.count_after(mark, v, "Removed"), 0, "no notice needed");
    assert_eq!(net.count_after(mark, v, "InstallSnapshot"), 0);
}
