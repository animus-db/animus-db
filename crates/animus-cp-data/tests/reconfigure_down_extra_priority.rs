//! Regression for defect C (ADR 0029 leadership-transfer follow-up, see the
//! root `CLAUDE.md` engineering-practices entry): `reconfigure_step`'s
//! extras-to-remove search must find a `Down` extra even when it is *not* the
//! lowest-id one, and removing it must **not** wait on the healthy-extra
//! catch-up gate (step 3's gate exists to protect a *newcomer in `desired`*,
//! not the node being removed — a `Down` voter isn't acking anything anyway,
//! so there is nothing to protect and nothing to wait for).
//!
//! The bug: `extra()` returned only the *first* (lowest-id) non-self extra,
//! and step 1 filtered *that one* on down-ness
//! (`extra().filter(|n| down.contains(n))`). So a `Down` extra sorting after
//! a healthy one was invisible to step 1 — the ungated removal never fired —
//! and the step fell through to step 3 (remove a *healthy* extra), which
//! *is* gated on every member of `desired` having caught up to
//! `commit_index`. If a `desired` survivor happens to be lagging (e.g. it
//! just recovered from a blip the failure detector hasn't cleared), step 3's
//! gate blocks that too — so the **whole reconfigure step stalls**, even
//! though the `Down` extra could have been dropped immediately.
//!
//! This test builds exactly that shape: a 4-voter group where `desired`
//! drops two extras — a lower-id healthy one and a higher-id `Down` one —
//! while the one member of `desired` other than the leader is deliberately
//! lagging behind `commit_index`. Fixed code removes the `Down` extra in one
//! step regardless; the pre-fix code returns `None` (verified by stashing the
//! source changes and re-running this test in isolation).

use std::collections::BTreeSet;
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{NodeId, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

const IDS: [u64; 4] = [0, 1, 2, 3];

fn set(ids: &[u64]) -> BTreeSet<NodeId> {
    ids.iter().copied().map(nid).collect()
}

fn leader_among(nodes: &[KvNode]) -> Option<usize> {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    if ls.len() == 1 { Some(ls[0]) } else { None }
}

#[test]
fn down_extra_is_removed_first_regardless_of_id_order_and_without_a_catch_up_gate() {
    let seed = 0x00D0_117C;
    let mut sim = Simulator::new(seed);
    let nodes: Vec<KvNode> = IDS
        .iter()
        .map(|&id| {
            RaftKvNode::start(
                sim.env(nid(id)),
                IDS.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    sim.run_for(Duration::from_secs(2));
    let l = leader_among(&nodes).expect("an initial leader");

    // Followers, sorted: `x` is the one member of `desired` other than the
    // leader (deliberately left lagging below); `a` < `b` are the two extras
    // — `a` stays healthy, `b` is reported `Down`. `b`'s id sorting *after*
    // `a` is the crux of the bug: the old `extra()` always returned `a`.
    let mut followers: Vec<u64> = IDS.iter().copied().filter(|&n| n != l as u64).collect();
    followers.sort_unstable();
    let (x, a, b) = (followers[0], followers[1], followers[2]);

    let desired = set(&[l as u64, x]);
    let down = set(&[b]);

    // Make `x` (a `desired` survivor) lag behind `commit_index`: freeze it,
    // then commit further writes via the leader + the two extras (3-of-4 is
    // still a majority, so commit keeps advancing without `x`).
    sim.crash(nid(x));
    for i in 0..5 {
        assert!(matches!(
            nodes[l].put(format!("k{i}").into_bytes(), b"v".to_vec()),
            ProposeResult::Accepted { .. }
        ));
        sim.run_for(Duration::from_millis(200));
    }
    let commit = nodes[l].commit_index();
    assert!(
        nodes[l].peer_match(nid(x)) < commit,
        "test setup: `x` must be lagging behind commit_index (peer_match={}, commit={commit})",
        nodes[l].peer_match(nid(x))
    );
    // Sanity: `a` (the healthy extra `reconfigure_step` must NOT pick first)
    // is fully caught up — so the only reason a fixed removal of `b` could be
    // blocked is a catch-up gate misapplied to the wrong node.
    assert!(
        nodes[l].peer_match(nid(a)) >= commit,
        "test setup: `a` must be fully caught up"
    );

    let step = nodes[l].reconfigure_step(&desired, &down);

    assert_eq!(
        step,
        Some(set(&[l as u64, x, a])),
        "must remove the Down extra `b` in one step, leaving {{leader, x, a}} — got {step:?} \
         (current config: {:?}, x lagging at {} < commit {commit})",
        nodes[l].config(),
        nodes[l].peer_match(nid(x)),
    );
    assert!(
        !nodes[l].config().contains(&nid(b)),
        "the Down extra must have been dropped from the config"
    );
    assert!(
        nodes[l].config().contains(&nid(a)),
        "the healthy extra must NOT have been touched by this single step"
    );
}
