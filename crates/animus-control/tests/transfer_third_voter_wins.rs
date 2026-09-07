//! SimEnv regression for issue #688: a "the old leader stepped down"
//! observation is not proof the requested transfer target actually won —
//! under real scheduling jitter a *third* voter this transfer never named
//! can win the resulting election instead, since `RaftCore::handle`'s
//! generic higher-term step-down fires on **any** higher-term Raft message,
//! not only the armed target's own `TimeoutNow`-triggered vote (see
//! `crates/animus-control/CLAUDE.md`'s "Leadership transfer" entry and
//! `docs/engineering-lessons.md`'s matching issue #688 entry).
//!
//! Drives a real 3-node control cluster (genuine `RaftNode<SimEnv>` driver
//! tasks — the same mechanism `animus-control/tests/metrics.rs`'s
//! `aborted_leadership_transfer_is_observable` drives, not hand-injected
//! messages) through exactly that race using the simulator's own fault
//! surface: [`Simulator::pause`] freezes the armed leader completely — no
//! timer it owns fires, nothing it sends leaves, nothing addressed to it is
//! delivered — for well past one election timeout (150ms default). This
//! models the real root cause the CI flake traced to (`animusd`'s own
//! `admin_endpoint.rs` test running 3 real threads on a 2-vCPU runner): the
//! leader's own heartbeats to *every* peer go missing together (a starved
//! runner, not a per-peer delay), so **both** remaining voters' election
//! timers can lapse at once, not only the armed target's. With no partition
//! needed at all — an ordinary, fully-connected pre-vote/vote round between
//! the two followers, exactly as a real cluster would run it — one of them
//! wins outright and the other simply grants it (the standard Raft
//! mechanism that avoids most split votes: whichever follower's own
//! randomized timeout fires first asks the other to vote for it, which
//! resets the other's own timer before it ever campaigns itself). Which one
//! wins is a pure function of the seed; the seed below is pinned to a case
//! where it is the voter this transfer never armed, not the target — the
//! exact shape issue #688 root-caused. The stepped-down former leader's own
//! live `leader()` belief then names that third voter, never the target:
//! the fact `animusd::ClientCtx::admin_transfer_control_leadership`'s fixed
//! contract has to check before ever reporting success.

use std::time::Duration;

use animus_control::RaftNode;
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

const CONTROL: [u64; 3] = [0, 1, 2];

/// Stand up a 3-node control cluster with real driver tasks — no metrics
/// handle needed here (unlike `metrics.rs`'s own `cluster`), since this test
/// asserts on `RaftNode`'s own `is_leader`/`leader`/`transfer_target`
/// accessors directly.
fn cluster(seed: u64) -> (Simulator, Vec<RaftNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes: Vec<RaftNode<SimEnv>> = CONTROL
        .iter()
        .map(|&id| {
            RaftNode::start(
                sim.env(nid(id)),
                CONTROL.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    (sim, nodes)
}

fn leader_index(nodes: &[RaftNode<SimEnv>]) -> Option<usize> {
    let leaders: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    if leaders.len() == 1 {
        Some(leaders[0])
    } else {
        None
    }
}

/// Found by an exhaustive scan of seeds 0..3000 (`docs/engineering-
/// lessons.md`'s matching issue #688 entry has the search) for one where,
/// after the leader is paused past one election timeout with no other
/// fault applied, the voter this test arms a transfer to is NOT the one
/// that ends up winning the resulting election — the exact race this test
/// exists to pin. Plenty of seeds in that range reproduce it; this is just
/// the first one found.
const SEED: u64 = 0x0B38;

#[test]
fn third_voter_wins_the_election_a_transfer_armed_a_different_target_for() {
    run(SEED);
}

/// The scenario is a pure function of the seed — this pins reproducibility
/// the same way `metrics.rs::
/// aborted_leadership_transfer_is_reproducible_from_seed` does for its own
/// scenario.
#[test]
fn third_voter_wins_is_reproducible_from_seed() {
    assert_eq!(run_and_report(SEED), run_and_report(SEED));
}

fn run(seed: u64) {
    let (leader_stepped_down, target_never_led, third_led) = run_and_report(seed);
    assert!(
        leader_stepped_down,
        "the armed leader must step down once a higher-term voter contacts it (seed={seed:#x})"
    );
    assert!(
        target_never_led,
        "the armed transfer's own target should not have won this seed's race (seed={seed:#x})"
    );
    assert!(
        third_led,
        "the voter this transfer never armed should have won instead (seed={seed:#x})"
    );
}

/// Drives the race once and reports the three booleans `run` asserts on —
/// factored out so the reproducibility test above can compare two runs
/// without duplicating the whole scenario. The inner assertions (guarded on
/// the race having actually landed the way this test means to drive it) are
/// the real point: the exact fact `animusd`'s route now checks.
fn run_and_report(seed: u64) -> (bool, bool, bool) {
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = leader_index(&nodes)
        .unwrap_or_else(|| panic!("expected exactly one leader after bring-up (seed={seed:#x})"));
    let leader_id = nid(CONTROL[leader]);

    // Arbitrary but fixed given the leader: the lower-indexed remaining
    // voter is the transfer's own armed target, the other is the third
    // voter this transfer never names — `SEED` above is chosen so the
    // *third* one wins.
    let others: Vec<usize> = (0..CONTROL.len()).filter(|&i| i != leader).collect();
    let target = others[0];
    let third = others[1];
    let target_id = nid(CONTROL[target]);
    let third_id = nid(CONTROL[third]);

    assert!(
        nodes[leader].transfer_leadership(target_id.clone()),
        "the target should already be caught up enough to arm (seed={seed:#x})"
    );
    assert_eq!(
        nodes[leader].transfer_target(),
        Some(target_id.clone()),
        "an armed transfer should be visible via transfer_target() (seed={seed:#x})"
    );

    // Freeze the leader well past one election timeout (150ms default) —
    // every peer's own heartbeats from the leader go missing together, the
    // real root cause behind issue #688's flake.
    sim.pause(leader_id, Duration::from_millis(600));
    sim.run_for(Duration::from_secs(2));

    let leader_stepped_down = !nodes[leader].is_leader();
    let target_never_led = !nodes[target].is_leader();
    let third_led = nodes[third].is_leader();

    if leader_stepped_down && target_never_led && third_led {
        // This is the exact fact `animusd::ClientCtx::
        // admin_transfer_control_leadership`'s fixed contract now checks
        // (issue #688): a "stepped down" observation on the old leader is
        // not proof the armed target won — its own live `leader()` belief
        // must be read afterward, and here it names a DIFFERENT voter.
        assert_eq!(
            nodes[leader].leader(),
            Some(third_id.clone()),
            "the stepped-down leader's own live belief should name the real winner (seed={seed:#x})"
        );
        assert_ne!(
            nodes[leader].leader(),
            Some(target_id),
            "the armed target never led, even though the old leader stepped down (seed={seed:#x})"
        );
        assert_eq!(
            nodes[leader].transfer_target(),
            None,
            "the armed transfer must be cleared, not carried forward across the step-down (seed={seed:#x})"
        );
    }

    (leader_stepped_down, target_never_led, third_led)
}
