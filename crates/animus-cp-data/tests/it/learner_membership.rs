//! Integration-level exercise of the **learner** (non-voting) membership
//! class (ADR 0058 Train 1) at the per-tablet CP data-plane layer — the
//! "Stage C audit note" discipline (`docs/engineering-lessons.md`): a shared
//! `RaftCore` primitive is exercised at both `animus-control`'s core level
//! (`animus-control/tests/learner_membership.rs`,
//! `animus-control/tests/learner_corpus.rs`) AND here, driving a real
//! `RaftKvNode` tablet group directly. Deliberately does **not** touch
//! `host.rs`'s reconciler or its replica-move sequencing (out of scope for
//! this PR) — every learner transition below is driven straight through
//! `RaftKvNode::add_learner`/`promote_learner`/`remove_learner`.
//!
//! Structure mirrors `membership.rs`'s harness idioms (`Simulator::run_for`,
//! never `run()`; every seed printed in assertion messages).

use std::collections::BTreeSet;
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn group(seed: u64, ids: &[u64]) -> (Simulator, Vec<KvNode>) {
    let sim = Simulator::new(seed);
    let nodes = ids
        .iter()
        .map(|&id| {
            RaftKvNode::start(
                sim.env(nid(id)),
                ids.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    (sim, nodes)
}

fn leader(nodes: &[KvNode], live: &[usize], seed: u64) -> usize {
    let ls: Vec<usize> = nodes
        .iter()
        .enumerate()
        .filter(|(i, n)| live.contains(i) && n.is_leader())
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        ls.len(),
        1,
        "expected one leader among {live:?}, got {ls:?} (seed={seed})"
    );
    ls[0]
}

fn put(node: &KvNode, key: &[u8], value: &[u8], seed: u64) {
    assert!(
        matches!(
            node.put(key.to_vec(), value.to_vec()),
            ProposeResult::Accepted { .. }
        ),
        "leader rejected a put (seed={seed})"
    );
}

fn set(ids: &[u64]) -> BTreeSet<u64> {
    ids.iter().copied().collect()
}

/// A learner joins a tablet group, catches up via ordinary
/// `AppendEntries`/`InstallSnapshot` replication, and is promoted to voter —
/// **without ever counting toward quorum** while it was a learner: the group
/// serves every write on the original 3-voter majority throughout.
#[test]
fn a_learner_catches_up_and_promotes_never_counted_toward_quorum_meanwhile() {
    let seed = 0x1EA5_C0DE;
    let ids = [0u64, 1, 2];
    let (mut sim, nodes) = group(seed, &ids);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2], seed);
    put(&nodes[l], b"k1", b"v1", seed);
    sim.run_for(Duration::from_secs(1));

    // Bring up the learner as a NOT-yet-a-member replica (excluded from its
    // own bootstrap `all_nodes` — the documented "pre-start a to-be-added
    // node knowing only the current voters, NOT itself" pattern, ADR 0058's
    // durable generalization of the same transient-state gotcha).
    let learner = RaftKvNode::start(
        sim.env(nid(3)),
        ids.iter().copied().map(nid).collect(),
        MemoryEngine::new(),
    );
    assert!(
        matches!(nodes[l].add_learner(nid(3)), ProposeResult::Accepted { .. }),
        "seed={seed}"
    );
    sim.run_for(Duration::from_secs(1));

    // Still not a voter anywhere, but it has the learner's config seam.
    assert_eq!(
        nodes[l].learners(),
        set(&[3]).into_iter().map(nid).collect(),
        "seed={seed}"
    );
    assert!(
        !nodes[l].config().contains(&nid(3)),
        "seed={seed}: a learner is never a voter"
    );

    // Kill the learner outright: quorum must be totally unaffected (2-of-3
    // voters), unlike a genuine 4th *voter* dying, which would matter.
    sim.crash(nid(3));
    put(&nodes[l], b"k2", b"v2", seed);
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        block_on(nodes[l].local_get(b"k2")),
        Some(b"v2".to_vec()),
        "seed={seed}: committed purely on the 3 voters, learner dead or not"
    );

    // Bring it back, let it catch up, then promote.
    sim.restart(nid(3));
    let mut caught_up = false;
    for _ in 0..40 {
        if nodes[l].learner_caught_up(&nid(3), 4) {
            caught_up = true;
            break;
        }
        sim.run_for(Duration::from_millis(200));
    }
    assert!(caught_up, "seed={seed}: learner must catch up once healthy");

    assert!(
        matches!(
            nodes[l].promote_learner(nid(3)),
            ProposeResult::Accepted { .. }
        ),
        "seed={seed}"
    );
    sim.run_for(Duration::from_secs(2));
    assert!(
        learner.config().contains(&nid(3)),
        "seed={seed}: the promoted node adopted its own new voter status"
    );
    assert_eq!(
        block_on(learner.local_get(b"k1")),
        Some(b"v1".to_vec()),
        "seed={seed}: caught up on the pre-join write too"
    );

    // Now a genuine 4-voter group: kill the ORIGINAL leader and the
    // formerly-learner node must be able to help form the new majority
    // (2-of-3 survivors, since the 4th, now a real voter, counts).
    sim.crash(nid(l as u64));
    let survivors: Vec<usize> = [0usize, 1, 2].into_iter().filter(|&i| i != l).collect();
    sim.run_for(Duration::from_secs(3));
    // `leader()` only inspects `nodes` (0..3); the promoted node (id 3) has
    // its own separate `learner` handle, so check both possible leader sets.
    let among_original = nodes
        .iter()
        .enumerate()
        .filter(|(i, n)| survivors.contains(i) && n.is_leader())
        .count();
    let new_leader_is_promoted = learner.is_leader();
    assert!(
        among_original + usize::from(new_leader_is_promoted) == 1,
        "seed={seed}: exactly one leader among the 3 remaining voters (2 original survivors + the promoted node)"
    );
}

/// `remove_learner` drops a learner that never catches up (or is simply no
/// longer wanted) without ever touching the voter set — the "demote/remove"
/// reachable transition.
#[test]
fn remove_learner_drops_it_without_promoting() {
    let seed = 0x1EA5_C0DE + 1;
    let ids = [0u64, 1, 2];
    let (mut sim, nodes) = group(seed, &ids);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2], seed);

    let _learner = RaftKvNode::start(
        sim.env(nid(3)),
        ids.iter().copied().map(nid).collect(),
        MemoryEngine::new(),
    );
    assert!(matches!(
        nodes[l].add_learner(nid(3)),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(1));
    assert!(matches!(
        nodes[l].remove_learner(nid(3)),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(1));
    assert!(nodes[l].learners().is_empty(), "seed={seed}");
    assert!(!nodes[l].config().contains(&nid(3)), "seed={seed}");
}
