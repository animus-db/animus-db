//! ADR 0075 section 3.4 (G-01 stage G-c groundwork): `RaftCore::set_timing`,
//! the narrow setter that installs a [`TimingProfile`]'s
//! `(election_base, heartbeat_interval)` pair.
//!
//! Hand-driven `RaftCore`s only (no driver, no `Simulator`), the style of
//! `next_deadline.rs`/`leadership_transfer.rs`, so every tick is inspectable.

use std::time::Duration;

use animus_control::timing::{DEFAULT_MAX_REGION_RTT, TimingProfile};
use animus_control::{RaftCore, RaftMsg, Role};
use animus_env::{Nanos, NodeId, nid};

fn group() -> [NodeId; 3] {
    [nid(0), nid(1), nid(2)]
}

const MS: u64 = 1_000_000;
const NOW: Nanos = Nanos(1_000 * MS);

fn wan() -> (Duration, Duration) {
    TimingProfile::Wan {
        max_region_rtt: DEFAULT_MAX_REGION_RTT,
    }
    .durations()
}

fn elect_leader() -> RaftCore {
    let mut core: RaftCore = RaftCore::new(group()[0].clone(), &group(), Nanos(0), 7);
    let _ = core.tick(NOW, 7);
    let _ = core.handle(
        group()[1].clone(),
        RaftMsg::PreVoteResp {
            term: core.term() + 1,
            granted: true,
        },
        NOW,
        7,
    );
    let _ = core.handle(
        group()[1].clone(),
        RaftMsg::RequestVoteResp {
            term: core.term(),
            granted: true,
        },
        NOW,
        7,
    );
    assert!(core.is_leader());
    core
}

#[test]
fn defaults_are_the_lan_pair() {
    let core: RaftCore = RaftCore::new(nid(0), &group(), Nanos(0), 7);
    assert_eq!(core.election_timeout(), TimingProfile::Lan.durations().0);
}

#[test]
fn reinstalling_the_current_pair_is_a_no_op_that_never_moves_a_deadline() {
    let mut core: RaftCore = RaftCore::new(nid(0), &group(), Nanos(0), 7);
    let before = core.next_deadline();
    let (e, h) = TimingProfile::Lan.durations();
    // A reconciler calls this every tick: it must never postpone an election.
    for i in 1..50u64 {
        assert!(!core.set_timing(e, h, Nanos(i * 100 * MS), i));
        assert_eq!(core.next_deadline(), before);
    }
}

#[test]
fn zero_durations_are_refused() {
    let mut core: RaftCore = RaftCore::new(nid(0), &group(), Nanos(0), 7);
    let before = core.next_deadline();
    assert!(!core.set_timing(Duration::ZERO, Duration::from_millis(50), NOW, 1));
    assert!(!core.set_timing(Duration::from_millis(150), Duration::ZERO, NOW, 1));
    assert_eq!(core.election_timeout(), Duration::from_millis(150));
    assert_eq!(core.next_deadline(), before);
}

#[test]
fn a_follower_rearms_its_election_deadline_from_the_new_base() {
    let mut core: RaftCore = RaftCore::new(nid(0), &group(), Nanos(0), 7);
    let (e, h) = wan();
    assert!(core.set_timing(e, h, NOW, 0));
    assert_eq!(core.election_timeout(), e);
    // entropy 0 => deadline is exactly now + base.
    assert_eq!(
        core.next_deadline(),
        Some(Nanos(NOW.0 + e.as_nanos() as u64))
    );

    // With max entropy the draw stays inside `[base, 2*base)`.
    let mut core2: RaftCore = RaftCore::new(nid(0), &group(), Nanos(0), 7);
    assert!(core2.set_timing(e, h, NOW, u64::MAX));
    let d = core2.next_deadline().unwrap().0 - NOW.0;
    let base = e.as_nanos() as u64;
    assert!((base..2 * base).contains(&d), "deadline offset {d}");
}

#[test]
fn a_wan_follower_does_not_campaign_at_lan_timescales() {
    // LAN follower: past 2 * 150ms it has started a pre-vote round.
    let mut lan: RaftCore = RaftCore::new(nid(0), &group(), Nanos(0), 7);
    let _ = lan.tick(Nanos(301 * MS), 7);
    assert_eq!(lan.role(), Role::PreCandidate, "LAN control must campaign");

    // The same follower on the WAN pair stays a follower well past that.
    let mut w: RaftCore = RaftCore::new(nid(0), &group(), Nanos(0), 7);
    let (e, h) = wan();
    assert!(w.set_timing(e, h, Nanos(0), 7));
    let _ = w.tick(Nanos(301 * MS), 7);
    assert_eq!(w.role(), Role::Follower, "WAN follower campaigned at 301ms");
    // ...but still campaigns once its (wider) window has passed.
    let _ = w.tick(Nanos(2 * e.as_nanos() as u64 + MS), 7);
    assert_eq!(w.role(), Role::PreCandidate);
}

#[test]
fn a_leader_adopts_the_new_heartbeat_cadence_and_never_pushes_a_deadline_out() {
    let mut core = elect_leader();
    // Establish a heartbeat deadline: tick at NOW arms now+50ms.
    let _ = core.tick(NOW, 7);
    let lan_next = core
        .next_deadline()
        .expect("leader has a heartbeat deadline");
    assert_eq!(lan_next, Nanos(NOW.0 + 50 * MS));

    // Widening to WAN (75ms) must not push the already-armed 50ms deadline out.
    let (e, h) = wan();
    assert!(core.set_timing(e, h, NOW, 7));
    assert_eq!(core.next_deadline(), Some(lan_next));
    // The next heartbeat then re-arms at the WAN cadence.
    let _ = core.tick(lan_next, 7);
    assert_eq!(
        core.next_deadline(),
        Some(Nanos(lan_next.0 + h.as_nanos() as u64))
    );

    // Narrowing back pulls the deadline in to at most now + 50ms.
    let now = Nanos(lan_next.0 + 10 * MS);
    let (le, lh) = TimingProfile::Lan.durations();
    assert!(core.set_timing(le, lh, now, 7));
    assert!(core.next_deadline().unwrap().0 <= now.0 + 50 * MS);
    assert_eq!(core.election_timeout(), le);
}

#[test]
fn the_installed_wan_base_reads_back_through_election_timeout() {
    let mut core = elect_leader();
    let (e, h) = wan();
    assert!(core.set_timing(e, h, NOW, 7));
    // `election_timeout()` is the one accessor the transfer deadline, the animusd
    // health grace and the abort log all derive from; it reports the WAN base.
    assert_eq!(core.election_timeout(), e);
    assert_eq!(e, Duration::from_millis(750));
}

// ---- the control group's own opt-in loop (RaftNode::enable_region_timing) ----

mod control_group {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use animus_control::timing::{DEFAULT_MAX_REGION_RTT, REGION_LABEL};
    use animus_control::{MetaCommand, NodeStatus, RaftNode};
    use animus_env::nid;
    use animus_sim::{SimEnv, Simulator};
    use animus_storage::MemoryEngine;

    const NODES: [u64; 3] = [0, 1, 2];

    fn cluster(seed: u64) -> (Simulator, Vec<RaftNode<SimEnv>>) {
        let sim = Simulator::new(seed);
        let nodes: Vec<_> = NODES
            .iter()
            .map(|&id| {
                RaftNode::start(
                    sim.env(nid(id)),
                    NODES.iter().copied().map(nid).collect(),
                    MemoryEngine::new(),
                )
            })
            .collect();
        for n in &nodes {
            n.enable_region_timing(DEFAULT_MAX_REGION_RTT);
        }
        (sim, nodes)
    }

    fn upsert(node: u64, region: Option<&str>) -> MetaCommand {
        let mut labels = BTreeMap::new();
        if let Some(r) = region {
            labels.insert(REGION_LABEL.to_string(), r.to_string());
        }
        MetaCommand::UpsertMember {
            node: nid(node),
            labels,
            status: NodeStatus::Active,
        }
    }

    fn leader(nodes: &[RaftNode<SimEnv>]) -> usize {
        (0..nodes.len())
            .find(|&i| nodes[i].is_leader())
            .expect("a leader")
    }

    #[test]
    fn an_unlabelled_control_group_stays_on_the_lan_timing() {
        let (mut sim, nodes) = cluster(0x7101);
        sim.run_for(Duration::from_secs(3));
        let l = leader(&nodes);
        for i in 0..3u64 {
            nodes[l].propose(upsert(i, None));
        }
        sim.run_for(Duration::from_secs(8));
        for n in &nodes {
            assert_eq!(n.election_timeout(), Duration::from_millis(150));
        }
    }

    #[test]
    fn a_control_group_whose_voters_span_regions_adopts_the_wan_timing_everywhere() {
        let (mut sim, nodes) = cluster(0x7102);
        sim.run_for(Duration::from_secs(3));
        let l = leader(&nodes);
        for (i, r) in [(0, "a"), (1, "b"), (2, "c")] {
            nodes[l].propose(upsert(i, Some(r)));
        }
        sim.run_for(Duration::from_secs(12));
        for n in &nodes {
            assert_eq!(
                n.election_timeout(),
                Duration::from_millis(750),
                "every voter derives the same profile from the replicated labels"
            );
        }
        // A WAN group still has exactly one leader afterwards.
        assert_eq!(nodes.iter().filter(|n| n.is_leader()).count(), 1);
    }

    #[test]
    fn a_single_region_control_group_stays_lan() {
        let (mut sim, nodes) = cluster(0x7103);
        sim.run_for(Duration::from_secs(3));
        let l = leader(&nodes);
        for i in 0..3u64 {
            nodes[l].propose(upsert(i, Some("a")));
        }
        sim.run_for(Duration::from_secs(8));
        for n in &nodes {
            assert_eq!(n.election_timeout(), Duration::from_millis(150));
        }
    }
}
