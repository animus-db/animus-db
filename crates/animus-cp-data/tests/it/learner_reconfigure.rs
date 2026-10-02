//! ADR 0058 Train 1's **reconciler adoption**: `RaftKvNode::reconfigure_step`
//! now moves a replica in via **add-learner -> wait until caught up -> promote
//! -> remove the old replica**, instead of adding a new voter directly and
//! letting it catch up while already counting toward quorum. Unit-level
//! (direct `reconfigure_step` calls against a hand-built group), complementing
//! the full `Reconciler`-driven fault-injection scenarios in
//! `tests/reconciler_corpus.rs` and the multi-process integration exercise in
//! `animusd`.
//!
//! These tests drive `reconfigure_step` by hand rather than through
//! `spawn_reconfigure_loop`/the reconciler, mirroring
//! `tests/reconfigure_down_extra_priority.rs`'s and
//! `tests/leader_transfer_reconfigure.rs`'s existing style — the mechanism
//! under test is a single pure decision function, so driving it directly
//! keeps each scenario a tight, seed-reproducible unit rather than routing
//! through the whole reconciler machinery.

use std::collections::BTreeSet;
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{NodeId, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn set(ids: &[u64]) -> BTreeSet<NodeId> {
    ids.iter().copied().map(nid).collect()
}

fn leader_among(nodes: &[KvNode]) -> Option<usize> {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    if ls.len() == 1 { Some(ls[0]) } else { None }
}

/// The converged-or-timeout idiom (root `CLAUDE.md`): poll `check` up to
/// `tries` times, advancing `sim`'s virtual time by `step` between checks.
fn poll(
    sim: &mut Simulator,
    tries: usize,
    step: Duration,
    mut check: impl FnMut() -> bool,
) -> bool {
    for _ in 0..tries {
        if check() {
            return true;
        }
        sim.run_for(step);
    }
    check()
}

/// Adding a brand-new replica must never touch the voter set directly — the
/// newcomer is tracked purely as a learner until it proves it can catch up.
#[test]
fn reconfigure_step_adds_a_learner_before_ever_touching_the_voter_set() {
    let seed = 0x1EA5_0001;
    let mut sim = Simulator::new(seed);
    let ids = [0u64, 1, 2];
    let nodes: Vec<KvNode> = ids
        .iter()
        .map(|&id| {
            RaftKvNode::start(
                sim.env(nid(id)),
                ids.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    sim.run_for(Duration::from_secs(2));
    let l = leader_among(&nodes).expect("an initial leader");

    // Node 3 joins as a quiet non-voter, knowing only the current voters (the
    // "pre-start a to-be-added node" gotcha, `animus-cp-data/CLAUDE.md`).
    let voters: Vec<NodeId> = ids.iter().copied().map(nid).collect();
    let _node3 = RaftKvNode::start(sim.env(nid(3)), voters, MemoryEngine::new());
    sim.run_for(Duration::from_secs(1));

    let desired = set(&[0, 1, 2, 3]);
    let before = nodes[l].config();
    let step = nodes[l].reconfigure_step(&desired, &BTreeSet::new());

    assert_eq!(
        step, None,
        "adding a learner must not itself report a voter-config change"
    );
    assert_eq!(
        nodes[l].config(),
        before,
        "the voter config must stay exactly the OLD set immediately after adding a learner"
    );
    assert!(
        nodes[l].learners().contains(&nid(3)),
        "the newcomer must be tracked as a learner"
    );
    assert!(
        !nodes[l].config().contains(&nid(3)),
        "the newcomer must not be a voter yet"
    );

    // It catches up trivially (nothing has been written), so the next step
    // must promote it.
    let promoted = poll(&mut sim, 50, Duration::from_millis(100), || {
        nodes[l].reconfigure_step(&desired, &BTreeSet::new());
        nodes[l].config().contains(&nid(3))
    });
    assert!(
        promoted,
        "the caught-up learner was never promoted to a voter"
    );
    assert!(
        nodes[l].learners().is_empty(),
        "a promoted learner must leave the learner set"
    );
}

/// The full happy-path sequence a replica move takes: add the newcomer as a
/// learner, promote it once caught up (a transient 4-voter config is fine
/// HERE — by promotion time the newcomer has already proven it can keep up,
/// so it dilutes nothing; see `old_quorum_survives_...` below for the
/// property that actually matters: no UNCAUGHT-UP node ever counts toward
/// quorum), then remove the old replica it is replacing.
#[test]
fn reconfigure_step_moves_a_replica_via_add_learner_promote_remove_old() {
    let seed = 0x1EA5_0003;
    let mut sim = Simulator::new(seed);
    let ids = [0u64, 1, 2];
    let nodes: Vec<KvNode> = ids
        .iter()
        .map(|&id| {
            RaftKvNode::start(
                sim.env(nid(id)),
                ids.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    sim.run_for(Duration::from_secs(2));

    let voters: Vec<NodeId> = ids.iter().copied().map(nid).collect();
    let node3 = RaftKvNode::start(sim.env(nid(3)), voters, MemoryEngine::new());
    sim.run_for(Duration::from_secs(1));

    // Move node 2 -> node 3: desired keeps 0 and 1, drops 2, adds 3.
    let desired = set(&[0, 1, 3]);
    let mut saw_learner_added = false;
    let mut saw_learner_before_voter = false;
    let converged = poll(&mut sim, 200, Duration::from_millis(100), || {
        let Some(l) = leader_among(&nodes) else {
            return false;
        };
        nodes[l].reconfigure_step(&desired, &BTreeSet::new());
        if nodes[l].learners().contains(&nid(3)) {
            saw_learner_added = true;
            if !nodes[l].config().contains(&nid(3)) {
                saw_learner_before_voter = true;
            }
        }
        nodes.iter().all(|n| n.config() == desired) && node3.config() == desired
    });
    assert!(
        converged,
        "the move never converged to {desired:?} (configs: {:?}, node3: {:?})",
        nodes.iter().map(|n| n.config()).collect::<Vec<_>>(),
        node3.config()
    );
    assert!(
        saw_learner_added,
        "the newcomer must pass through the learner set on its way in"
    );
    assert!(
        saw_learner_before_voter,
        "the newcomer must be observed as a LEARNER (not yet a voter) at some point before \
         the move converges — never a straight add to the voter set"
    );
    assert!(
        node3.learners().is_empty() && !node3.learners().contains(&nid(2)),
        "no stray learner bookkeeping should survive a converged move"
    );
}

/// A learner mid-catch-up whose target changed under it (its node was
/// decommissioned, or a rebalance simply retargeted elsewhere) must be
/// dropped, not wedge every later reconfigure step forever (ADR 0058 Train
/// 1's reconciler adoption, the stuck-learner-cleanup requirement).
#[test]
fn reconfigure_step_replaces_a_stale_learner_when_desired_changes_under_it() {
    let seed = 0x1EA5_0004;
    let mut sim = Simulator::new(seed);
    let ids = [0u64, 1, 2];
    let nodes: Vec<KvNode> = ids
        .iter()
        .map(|&id| {
            RaftKvNode::start(
                sim.env(nid(id)),
                ids.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    sim.run_for(Duration::from_secs(2));
    let voters: Vec<NodeId> = ids.iter().copied().map(nid).collect();

    // node3 joins as the intended replacement for node2, but its process
    // dies (`sim.stop`, a real decommission/crash — never restarted) before
    // it ever catches up.
    let node3 = RaftKvNode::start(sim.env(nid(3)), voters.clone(), MemoryEngine::new());
    let desired1 = set(&[0, 1, 3]);
    let l = leader_among(&nodes).expect("an initial leader");
    assert_eq!(nodes[l].reconfigure_step(&desired1, &BTreeSet::new()), None);
    assert!(
        nodes[l].learners().contains(&nid(3)),
        "node3 must have been added as a learner"
    );
    // Let the add-learner entry actually commit before moving on — otherwise
    // the retarget below's own membership-change entry would be rejected as
    // "a change is already in flight", nothing to do with the property under
    // test here.
    sim.run_for(Duration::from_millis(500));
    sim.stop(nid(3));
    drop(node3);

    // Placement notices and retargets to node4 instead.
    let node4 = RaftKvNode::start(sim.env(nid(4)), voters, MemoryEngine::new());
    let desired2 = set(&[0, 1, 4]);

    // The stale learner (node3) must be dropped rather than waited on
    // forever — it is unreachable and no longer desired.
    let dropped = poll(&mut sim, 50, Duration::from_millis(100), || {
        let Some(l) = leader_among(&nodes) else {
            return false;
        };
        nodes[l].reconfigure_step(&desired2, &BTreeSet::new());
        !nodes[l].learners().contains(&nid(3))
    });
    assert!(
        dropped,
        "the stale learner (node3) must have been dropped, not left dangling"
    );

    // The move then completes onto node4 exactly like the happy path.
    let converged = poll(&mut sim, 200, Duration::from_millis(100), || {
        let Some(l) = leader_among(&nodes) else {
            return false;
        };
        nodes[l].reconfigure_step(&desired2, &BTreeSet::new());
        nodes.iter().all(|n| n.config() == desired2) && node4.config() == desired2
    });
    assert!(
        converged,
        "the move onto the replacement (node4) never converged after the stale learner \
         was dropped (configs: {:?}, node4: {:?})",
        nodes.iter().map(|n| n.config()).collect::<Vec<_>>(),
        node4.config()
    );
}

/// **The structural regression this rung exists for.** During a move that
/// adds a new replica, one of the ORIGINAL voters is lost while the newcomer
/// is still an uncaught-up learner. Pre-Train-1 semantics (a direct voter
/// add) would have already grown the config to 4 voters — losing one
/// original voter AND having the still-catching-up newcomer unreachable at
/// the same moment would leave only 2 of 4 alive, short of the 3-vote
/// majority a 4-voter group needs, stalling the group outright. With the
/// learner phase, the voter config never leaves the original 3, so losing
/// one original voter still leaves 2 of 3 alive — a majority — and the group
/// must keep committing.
#[test]
fn old_quorum_survives_an_old_voter_loss_while_the_new_replica_is_still_a_learner() {
    let seed = 0x1EA5_0005;
    let mut sim = Simulator::new(seed);
    let ids = [0u64, 1, 2];
    let nodes: Vec<KvNode> = ids
        .iter()
        .map(|&id| {
            RaftKvNode::start(
                sim.env(nid(id)),
                ids.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    sim.run_for(Duration::from_secs(2));

    let voters: Vec<NodeId> = ids.iter().copied().map(nid).collect();
    let _node3 = RaftKvNode::start(sim.env(nid(3)), voters, MemoryEngine::new());
    sim.run_for(Duration::from_secs(1));

    // Grow toward {0,1,2,3} — an intermediate rebalance state (before the
    // eventual old-replica removal), the shape most exposed to quorum
    // dilution since it briefly wants MORE voters than today, not fewer.
    let desired = set(&[0, 1, 2, 3]);
    let l = leader_among(&nodes).expect("an initial leader");
    assert_eq!(
        nodes[l].reconfigure_step(&desired, &BTreeSet::new()),
        None,
        "adding the newcomer must go through the learner phase"
    );
    assert_eq!(
        nodes[l].config(),
        set(&[0, 1, 2]),
        "the OLD 3-voter quorum must be untouched the instant the newcomer is added"
    );
    assert!(nodes[l].learners().contains(&nid(3)));

    // The newcomer never gets to catch up (an `InstallSnapshot`-window
    // network fault) AND one of the three ORIGINAL voters is lost outright —
    // the exact double-fault this rung protects against.
    let victim = ids.iter().copied().find(|&n| n != l as u64).unwrap();
    sim.crash(nid(3));
    sim.crash(nid(victim));

    let survivors: Vec<&KvNode> = nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i as u64 != victim)
        .map(|(_, n)| n)
        .collect();
    assert_eq!(
        survivors.len(),
        2,
        "exactly two original voters must survive"
    );

    let committed = poll(&mut sim, 100, Duration::from_millis(100), || {
        survivors.iter().any(|n| {
            n.is_leader()
                && matches!(
                    n.put(b"k".to_vec(), b"v".to_vec()),
                    ProposeResult::Accepted { .. }
                )
        })
    });
    assert!(
        committed,
        "the old quorum must survive losing one original voter while the newcomer is still \
         an uncaught-up learner — pre-Train-1 direct-voter-add semantics would have stalled \
         here (configs: {:?})",
        nodes.iter().map(|n| n.config()).collect::<Vec<_>>()
    );

    // Confirm the write actually became durable/applied, not merely accepted
    // locally (`ProposeResult::Accepted` means appended, not committed).
    sim.run_for(Duration::from_secs(2));
    let leader = survivors
        .iter()
        .find(|n| n.is_leader())
        .expect("a surviving leader");
    assert_eq!(
        futures::executor::block_on(leader.local_get(b"k")),
        Some(b"v".to_vec()),
        "the write must have actually committed and applied on the surviving quorum"
    );
}
