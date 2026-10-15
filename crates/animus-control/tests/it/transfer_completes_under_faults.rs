//! Issue #1325: a leadership transfer must not stall for the whole admin
//! poll budget when a message is lost.
//!
//! `RaftCore::transfer_leadership` arms for ONE election-timeout window and
//! then aborts on a tick (resuming proposals). If the `TimeoutNow` (or the
//! target's catch-up ack) is lost inside that window, the transfer lapses with
//! the old node still leader and nothing armed. The admin routes
//! (`animusd` `admin_transfer_control_leadership`, the self-removal arm of
//! `admin_remove_control_member`) poll for seconds, so a one-shot arm turns one
//! lost message into a "did not complete within 5s; retry" (the
//! `upgrade-previous-release` `clean_3_nodes` failure). The fix is that the
//! poller re-issues the arm with [`RaftNode::rearm_lapsed_transfer`].
//!
//! Real 3-node `RaftNode` drivers over `SimEnv` with 30% message loss on every
//! link; seed-reproducible (`ANIMUS_SEED=<seed>` replays one).

use std::time::Duration;

use animus_control::RaftNode;
use animus_env::nid;
use animus_sim::{NetConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;

const CONTROL: [u64; 3] = [0, 1, 2];
/// The admin poll budget (`CONTROL_TRANSFER_POLL_TIMEOUT` in `animusd`).
const BUDGET: Duration = Duration::from_secs(5);

fn cluster(seed: u64) -> (Simulator, Vec<RaftNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes = CONTROL
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

#[derive(Debug, PartialEq)]
enum Outcome {
    /// The requested target leads.
    Target,
    /// A third voter won the election the transfer triggered (the documented
    /// "retry against the new leader" case, not a stall).
    Third,
    /// The old leader is still leader, or no leader, at the budget's end.
    Stalled,
}

fn run(seed: u64, rearm: bool) -> Outcome {
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = (0..3)
        .find(|&i| nodes[i].is_leader())
        .unwrap_or_else(|| panic!("no leader after bring-up (seed={seed})"));
    let target = (leader + 1) % 3;
    let mut n = NetConfig::default();
    n.set_drop_prob(0.3);
    sim.set_net_config(n);

    let tid = nid(CONTROL[target]);
    assert!(
        nodes[leader].transfer_leadership(tid.clone()),
        "arm refused (seed={seed})"
    );
    let steps = BUDGET.as_millis() as u64 / 50;
    for _ in 0..steps {
        sim.run_for(Duration::from_millis(50));
        if nodes[target].is_leader() {
            return Outcome::Target;
        }
        if rearm {
            nodes[leader].rearm_lapsed_transfer(tid.clone());
        }
    }
    if nodes[target].is_leader() {
        Outcome::Target
    } else if (0..3).any(|i| i != leader && i != target && nodes[i].is_leader()) {
        Outcome::Third
    } else {
        Outcome::Stalled
    }
}

#[test]
fn a_lapsed_transfer_is_re_armed_and_completes_within_the_budget() {
    let seeds: Vec<u64> = match std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(s) => vec![s],
        None => (0..60).collect(),
    };
    for seed in seeds {
        let o = run(seed, true);
        assert_ne!(
            o,
            Outcome::Stalled,
            "transfer stalled with re-arming (seed={seed})"
        );
    }
}

/// Negative control: the one-shot arm (what the admin route did before the
/// fix) stalls on a meaningful fraction of the very same seeds, so the test
/// above has teeth.
#[test]
fn negative_control_a_one_shot_arm_stalls_under_loss() {
    let stalled = (0..60u64)
        .filter(|&s| run(s, false) == Outcome::Stalled)
        .count();
    assert!(
        stalled > 0,
        "expected the one-shot arm to stall on some seed under 30% loss"
    );
}
