//! Issue #667, third amendment (2026-09-15): a genuinely wiped, previously-
//! established voter must still resolve to a permanent refusal even when
//! ONE of its configured peers is an ordinary follower that has never
//! itself been a candidate or leader, and therefore has never received any
//! real protocol message directly from the wiped identity.
//!
//! **Why this is a real gap, not a corner case.** `heard_from` is marked at
//! exactly three sites: a candidate's `RequestVote` reaching a peer, a
//! granted `RequestVoteResp` reaching the candidate that sent the request,
//! and a leader's `AppendEntries`/`InstallSnapshot` reaching a follower.
//! Every one of those routes through whichever peer *is* the candidate or
//! leader — two ordinary followers that never campaign never exchange a
//! single message directly, and the leader does not mark `heard_from` on
//! receiving a plain `AppendEntriesResp` either (`handle_append_resp` only
//! stamps `last_contact`/`match_index`). So in ANY 3-voter cluster with one
//! stable leader and two followers, at least one follower-follower pair
//! will legitimately, permanently show `ever_heard_from_prober: false` for
//! each other, no matter how long the cluster has been running.
//!
//! Before this amendment, `handle_cluster_probe_resp` treated a single
//! `ever_heard_from_prober: false` reply as immediately and unconditionally
//! decisive for "fresh, not refused" — so the wiped voter's fellow follower
//! answering `false` (honestly, and totally expected for an established
//! cluster) silently defeated the refusal `wiped_voter_double_vote_safety.rs`
//! and `wiped_voter_rejoin.rs` otherwise prove sound. This is exactly what
//! `prod_liveness.rs::wiped_voter_refuses_and_the_rest_of_the_cluster_keeps_serving`
//! caught, reproducibly, under real `ProdEnv` threading (not intermittently:
//! the test always picks a non-leader victim, and one of that victim's two
//! peers is always the OTHER follower, which never talks to it directly).
//!
//! This test pins the exact mixed-evidence shape at the bare `RaftCore`
//! level — deterministic, no seed needed, in the same spirit as
//! `wiped_voter_double_vote_safety.rs`'s own hand-driven proof.

use std::collections::BTreeSet;

use animus_control::{RaftCore, RaftMsg};
use animus_env::{Nanos, NodeId, nid};

fn triple() -> [NodeId; 3] {
    [nid(0), nid(1), nid(2)]
}

/// Node 1 (the eventual wipe victim) is wiped and restarts fresh. Its two
/// configured peers are node 0 (the established leader, which genuinely
/// exchanged a real vote grant with node 1 before the wipe) and node 2 (an
/// ordinary fellow follower that never itself campaigned and so never
/// received any direct protocol message from node 1, ever). Node 2's
/// honest `ever_heard_from_prober: false` must NOT, on its own, resolve the
/// check as fresh — only once BOTH peers have answered does the aggregate
/// verdict depend on whether *any* of them showed real participation
/// evidence, and node 0's `true` is exactly that.
#[test]
fn wiped_voter_refuses_even_when_a_fellow_follower_never_heard_from_it() {
    let ids = triple();
    let construct_now = Nanos(0);
    let now = Nanos(1_000_000_000);
    let term = 3;
    let all_voters: BTreeSet<NodeId> = ids.iter().cloned().collect();

    let mut victim: RaftCore = RaftCore::new(ids[1].clone(), &ids, construct_now, 7);
    let probe_out = victim.begin_cluster_check(now, 7);
    assert!(
        victim.cluster_check_pending(),
        "the wiped voter must start a pending check (it has two peers)"
    );
    assert_eq!(
        probe_out.len(),
        2,
        "a probe must go to both configured peers"
    );

    // Node 2 (fellow follower) answers FIRST: real history, names node 1 as
    // an established voter, but has itself never heard from node 1 --
    // exactly the honest answer an ordinary follower-follower pair
    // produces. On the pre-fix code this alone would resolve the check as
    // "fresh" immediately; it must NOT, since node 0's evidence has not
    // been heard from yet.
    let _ = victim.handle(
        ids[2].clone(),
        RaftMsg::ClusterProbeResp {
            term,
            committed_index: 1,
            config: all_voters.clone(),
            ever_heard_from_prober: false,
        },
        now,
        7,
    );
    assert!(
        victim.cluster_check_pending(),
        "a single fellow-follower's honest `false` must not resolve the check by itself -- \
         the exact regression `prod_liveness.rs`'s \
         `wiped_voter_refuses_and_the_rest_of_the_cluster_keeps_serving` caught"
    );
    assert!(
        !victim.refused_as_voter(),
        "must not be refused yet either -- still waiting on node 0's own answer"
    );

    // Node 0 (the established leader, which genuinely received node 1's
    // real vote grant before the wipe) now answers: real history, names
    // node 1, AND genuinely heard from it before.
    let _ = victim.handle(
        ids[0].clone(),
        RaftMsg::ClusterProbeResp {
            term,
            committed_index: 1,
            config: all_voters.clone(),
            ever_heard_from_prober: true,
        },
        now,
        7,
    );
    assert!(
        !victim.cluster_check_pending(),
        "the check must resolve once every configured peer has answered"
    );
    assert!(
        victim.refused_as_voter(),
        "the wiped voter must refuse permanently: at least one peer (node 0) both names it \
         as an established voter and genuinely heard from it before -- the fellow follower's \
         honest `false` must never have been allowed to short-circuit that verdict"
    );
}

/// The mirror case: BOTH peers honestly answer `ever_heard_from_prober:
/// false` (a true same-bootstrap genesis race, or an ordinary ADR 0060
/// rejoin) -- this must still resolve fresh, proving the fix does not
/// simply always refuse once evidence-gathering is deferred.
#[test]
fn resolves_fresh_when_no_peer_has_ever_heard_from_the_asker() {
    let ids = triple();
    let construct_now = Nanos(0);
    let now = Nanos(1_000_000_000);
    let term = 1;
    let all_voters: BTreeSet<NodeId> = ids.iter().cloned().collect();

    let mut asker: RaftCore = RaftCore::new(ids[1].clone(), &ids, construct_now, 7);
    let _ = asker.begin_cluster_check(now, 7);

    for peer in [ids[0].clone(), ids[2].clone()] {
        let _ = asker.handle(
            peer,
            RaftMsg::ClusterProbeResp {
                term,
                committed_index: 1,
                config: all_voters.clone(),
                ever_heard_from_prober: false,
            },
            now,
            7,
        );
    }

    assert!(
        !asker.cluster_check_pending(),
        "the check must resolve once every peer has answered"
    );
    assert!(
        !asker.refused_as_voter(),
        "when NO peer has ever heard from the asker, it must resolve as an ordinary fresh \
         voter, never a false refusal"
    );
}
