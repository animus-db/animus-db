//! A follower that **declines** an `InstallSnapshot` offer as redundant must not
//! leave the leader re-offering it forever (found live, 2026-09-29, while
//! checking issue #1061's removal notice: `--cluster-control 3 --cluster-data
//! 5` under bulk seeding + auto-split, then five minutes with no writes —
//! `cp_snapshot_transfer_restarts` kept climbing at zero write rate and the
//! groups could never quiesce).
//!
//! The mechanism, all on the leader/receiver pair (none of it specific to the
//! removal notice — it reproduces on `main`):
//!
//! 1. The follower's engine is still digesting an install, so its
//!    `AppendEntriesResp`s echo `needs_snapshot: true` (issue #554).
//! 2. The leader's `snapshot_served_through[peer]` lags its own `snapshot_index`
//!    (the base moved since it last served the peer), so the echo is read as a
//!    genuinely new request: `next_index[peer] = 1`.
//! 3. The leader offers its (older) cached image. The follower is already past
//!    that base, so `handle_install_snapshot` declines it as redundant with
//!    `InstallSnapshotResp { last_index: 0, next_offset: 0 }` — deliberately
//!    *not* the shape of a completed install.
//! 4. Nothing on the leader ever moved `next_index` off 1 again: every further
//!    ack was such a chunk reply, never an `AppendEntriesResp`. So it re-offered
//!    the same chunk-0 on every heartbeat forever, the phantom
//!    `snapshot_offset` entry the reply handler inserted kept
//!    `snapshot_transfer_in_flight()` true (which the driver reads as "defer
//!    compaction", then "idle ceiling hit: count a transfer restart" every
//!    `COMPACT_DEFER_IDLE_CEILING`), and the follower never received another
//!    log entry.
//!
//! These are bare-`RaftCore` cells (no `Env`, no executor): every scenario is a
//! pure function of its seed, replayable with `ANIMUS_SEED=<seed> cargo test
//! --test declined_snapshot_offer`.

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::raft::{RaftCore, StateMachine};
use animus_control::{ProposeResult, RaftMsg, Role};
use animus_env::{Nanos, NodeId, nid};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum KvCommand {
    Put { key: u64, value: u64 },
    NoOp,
}

/// A `DRIVER_APPLIED` placeholder state machine, as in `driver_applied_sm.rs`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct KvUnit;

impl StateMachine<KvCommand> for KvUnit {
    const DRIVER_APPLIED: bool = true;
    fn apply(&mut self, _command: &KvCommand) {
        unreachable!("a DRIVER_APPLIED state machine is never applied in-core");
    }
    fn noop() -> KvCommand {
        KvCommand::NoOp
    }
}

type Core = RaftCore<KvCommand, KvUnit>;
type Msg = RaftMsg<KvCommand>;

const TICK: Duration = Duration::from_millis(10);

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// Two live replicas of a three-voter group (`n2` is permanently absent — a
/// majority is still `{n0, n1}`), driven tick by tick with every message
/// delivered to a fixpoint.
struct Pair {
    cores: BTreeMap<NodeId, Core>,
    now: u64,
    seed: u64,
    ctr: u64,
    /// Every `InstallSnapshot` chunk the leader put on the wire (its offset).
    snapshot_chunks: Vec<u64>,
    /// Messages sent but not yet delivered: `(from, to, message)`.
    inflight: std::collections::VecDeque<(NodeId, NodeId, Msg)>,
}

impl Pair {
    fn new(seed: u64) -> Self {
        let group = [nid(0), nid(1), nid(2)];
        let mut p = Pair {
            cores: BTreeMap::new(),
            now: 0,
            seed,
            ctr: 0,
            snapshot_chunks: Vec::new(),
            inflight: std::collections::VecDeque::new(),
        };
        for id in [nid(0), nid(1)] {
            let e = p.entropy();
            p.cores
                .insert(id.clone(), RaftCore::new(id, &group, Nanos(0), e));
        }
        p
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

    /// What the real driver does after each core step: make the log durable
    /// and hand committed effects to the (here, imaginary) engine.
    fn drive_side_effects(core: &mut Core) {
        let last = core.last_log_index();
        let _ = core.drain_persist();
        core.mark_durable_through(last);
        let _ = core.drain_apply();
        let _ = core.drain_pending_install();
    }

    /// Queue `outs` (sent by `from`) for delivery on the next tick. One hop per
    /// tick models a real link's latency: a follower's chatty ack is answered
    /// one round trip later, not recursively to a fixpoint.
    fn send(&mut self, from: NodeId, outs: Vec<(NodeId, Msg)>) {
        for (to, m) in outs {
            // Only offers to a live replica count: the absent `n2` is (rightly)
            // re-offered a snapshot on the heartbeat backoff schedule forever.
            if let RaftMsg::InstallSnapshot { offset, .. } = &m
                && to != nid(2)
            {
                self.snapshot_chunks.push(*offset);
            }
            self.inflight.push_back((from.clone(), to, m));
        }
    }

    fn step(&mut self, dur: Duration) {
        for _ in 0..(dur.as_nanos() / TICK.as_nanos()) {
            self.now += TICK.as_nanos() as u64;
            // Deliver everything that was in flight at the start of this tick.
            let batch: Vec<_> = self.inflight.drain(..).collect();
            for (f, to, m) in batch {
                let e = self.entropy();
                let now = self.now();
                let Some(core) = self.cores.get_mut(&to) else {
                    continue; // n2 is absent
                };
                let outs = core.handle(f, m, now, e);
                Self::drive_side_effects(core);
                self.send(to, outs);
            }
            for id in [nid(0), nid(1)] {
                let e = self.entropy();
                let now = self.now();
                let core = self.cores.get_mut(&id).unwrap();
                let outs = core.tick(now, e);
                Self::drive_side_effects(core);
                self.send(id, outs);
            }
        }
    }

    fn leader(&self) -> Option<u64> {
        let ls: Vec<u64> = [0u64, 1]
            .into_iter()
            .filter(|&i| self.core(i).role() == Role::Leader)
            .collect();
        (ls.len() == 1).then(|| ls[0])
    }

    fn elect(&mut self) -> u64 {
        for _ in 0..200 {
            if let Some(l) = self.leader() {
                self.step(Duration::from_millis(300));
                return l;
            }
            self.step(Duration::from_millis(100));
        }
        panic!("seed={:#x}: no leader elected", self.seed);
    }

    fn write(&mut self, l: u64, n: usize) {
        for i in 0..n {
            let now = self.now();
            let c = self.core_mut(l);
            assert!(matches!(
                c.propose(KvCommand::Put {
                    key: i as u64,
                    value: i as u64
                }),
                ProposeResult::Accepted { .. }
            ));
            let outs = c.replicate_now(now);
            Self::drive_side_effects(c);
            self.send(nid(l), outs);
            self.step(TICK * 3);
        }
    }
}

impl Pair {
    /// Step `dur` while asserting the leader's belief about the follower never
    /// exceeds the follower's actual log: a refused offer must not be turned
    /// into an inflated `match_index` (which `maybe_advance_commit` would then
    /// count toward a majority).
    fn step_checked(&mut self, l: u64, dur: Duration) {
        for _ in 0..(dur.as_nanos() / Duration::from_millis(20).as_nanos()) {
            self.step(Duration::from_millis(20));
            let f = 1 - l;
            let believed = self.core(l).peer_match(&nid(f));
            let actual = self
                .core(f)
                .last_log_index()
                .max(self.core(f).snapshot_index());
            assert!(
                believed <= actual,
                "seed={:#x}: leader believes the follower matches through {believed} but it only \
                 holds {actual}",
                self.seed
            );
        }
    }
}

fn seed_from_env() -> Option<u64> {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
}

/// One scenario; panics with the seed on failure.
fn scenario(seed: u64) {
    let mut net = Pair::new(seed);
    let l = net.elect();
    let f = 1 - l;

    // A populated, fully replicated log.
    let entries = 40 + (splitmix64(seed) % 60) as usize;
    net.write(l, entries);
    net.step(Duration::from_millis(200));
    let applied = net.core(l).last_applied();
    assert_eq!(
        net.core(f).last_applied(),
        applied,
        "seed={seed:#x}: precondition — the follower is fully caught up"
    );

    // The leader's cached image is older than the follower's applied index: it
    // was built (and the base set) when the peer last needed one, and the peer
    // has since advanced past it via ordinary replication.
    let base = applied / 2 + (splitmix64(seed ^ 1) % 5);
    assert!(base < applied);
    net.core_mut(l).snapshot_upto(base);
    net.core_mut(l).set_snapshot_blob(vec![b'i'; 500]);

    // The follower's engine is still digesting a previous install: its acks
    // echo `needs_snapshot` until the apply task catches up.
    net.core_mut(f).set_state_machine_behind(true);
    net.step_checked(l, Duration::from_secs(2));
    let chunks_while_digesting = net.snapshot_chunks.len();

    // The digest completes.
    net.core_mut(f).set_state_machine_behind(false);
    net.step_checked(l, Duration::from_secs(1));
    let chunks_at_digest = net.snapshot_chunks.len();

    // Steady state after the digest: nothing may still be re-offering.
    net.step_checked(l, Duration::from_secs(20));
    assert_eq!(
        net.snapshot_chunks.len(),
        chunks_at_digest,
        "seed={seed:#x}: the leader kept re-offering a declined image for 20s after the \
         follower finished digesting ({} chunks while digesting, {} at digest, {} after) — \
         at zero write rate this is what climbs `cp_snapshot_transfer_restarts`",
        chunks_while_digesting,
        chunks_at_digest,
        net.snapshot_chunks.len()
    );
    // (The absent `n2` legitimately keeps an offer outstanding forever.)
    assert!(
        !net.core(l).snapshot_transfer_peers().contains(&nid(f)),
        "seed={seed:#x}: a declined offer must not leave a phantom in-flight transfer (it defers \
         compaction and blocks quiescence forever)"
    );
    assert!(
        net.snapshot_chunks.len() <= 4,
        "seed={seed:#x}: {} chunks shipped to a peer that is already past the base",
        net.snapshot_chunks.len()
    );

    // And the peer is genuinely back on ordinary replication: a new write
    // reaches it.
    net.write(l, 3);
    net.step(Duration::from_secs(2));
    assert_eq!(
        net.core(l).peer_match(&nid(f)),
        net.core(l).last_log_index(),
        "seed={seed:#x}: the follower never received the entries written after the declined \
         offer — the leader is still stuck re-offering a snapshot instead of replicating"
    );
    assert_eq!(
        net.core(f).last_applied(),
        net.core(l).last_applied(),
        "seed={seed:#x}: the follower did not apply the post-decline writes"
    );
}

#[test]
fn a_declined_snapshot_offer_does_not_livelock_the_leader() {
    if let Some(seed) = seed_from_env() {
        scenario(seed);
        return;
    }
    for seed in 1..=12u64 {
        scenario(seed);
    }
}
