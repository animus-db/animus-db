//! Issue #667 (P0 Raft safety) safety cell: a hand-driven, fully
//! deterministic proof that a voter whose persisted store is wiped and
//! restarts fresh can never grant a second, contradicting vote in a term it
//! already voted in before the wipe -- the exact hazard that lets two
//! leaders be elected in one term.
//!
//! Deliberately `RaftCore`-level and hand-driven rather than a seeded
//! `SimEnv`/`RaftNode` scenario: the race this exists to prove safe is a
//! precise one ("the SAME term, two DIFFERENT candidates"), and pinning
//! that exactly through natural election timing would need real-election
//! term numbers to coincide across two independently timed candidacies --
//! not impossible to arrange via fault injection, but far less direct than
//! just replaying the two `RequestVote`s a real network could plausibly
//! deliver (one before the wipe, one after, both naming the same term) in
//! their real order against real `RaftCore` state. This needs no seed and
//! is deterministic on every run.
//!
//! **How the red-then-green proof was obtained** (per the issue #667 task):
//! this file only compiles against a `RaftCore` that already has
//! `begin_cluster_check`/`refused_as_voter` (the fix's own new API), so a
//! literal `git checkout main -- raft.rs node.rs` alongside this file does
//! not compile at all -- swapping whole files proves nothing about a
//! *gate*, only about API surface. Instead, the gate itself was disabled
//! in place: `handle_request_vote`'s grant condition's `cluster_checked`
//! term was temporarily forced to `true` (bypassing the check while
//! leaving every new method/field in place and compiling), and this exact
//! test was re-run:
//!
//! ```text
//! thread 'wiped_voter_never_grants_a_second_contradicting_vote_in_the_same_term' panicked at
//! crates/animus-control/tests/wiped_voter_double_vote_safety.rs:...:
//! assertion failed: !c_granted (the wiped voter granted C's vote in the SAME
//! term (1) it already granted A's vote in before the wipe)
//! left: true
//! ```
//!
//! and, chained through, node C actually became a *second* leader in term 1
//! while node A still believed itself leader in that same term -- the
//! `two_leaders_elected_in_the_same_term_via_the_stale_vote` assertions
//! below. Restoring the real gate turns both green. No seed is involved --
//! the gate bypass and restore is the entire red/green delta.

use std::collections::BTreeSet;

use animus_control::{RaftCore, RaftMsg};
use animus_env::{Nanos, NodeId, nid};

fn triple() -> [NodeId; 3] {
    [nid(0), nid(1), nid(2)]
}

/// Elect `leader`'s own core via a granted pre-vote + real vote from
/// `granter`, mirroring `install_snapshot.rs`'s own election-bootstrapping
/// idiom. Returns the elected term.
fn elect(leader: &mut RaftCore, granter: NodeId, now: Nanos) -> u64 {
    let _ = leader.tick(now, 7); // election timeout -> pre-candidate, PreVote
    let _ = leader.handle(
        granter.clone(),
        RaftMsg::PreVoteResp {
            term: leader.term() + 1,
            granted: true,
        },
        now,
        7,
    );
    let _ = leader.handle(
        granter,
        RaftMsg::RequestVoteResp {
            term: leader.term(),
            granted: true,
        },
        now,
        7,
    );
    assert!(leader.is_leader(), "the designated leader should have won");
    leader.term()
}

fn request_vote(candidate: NodeId, term: u64) -> RaftMsg {
    RaftMsg::RequestVote {
        term,
        candidate,
        last_log_index: 0,
        last_log_term: 0,
    }
}

fn granted(resp: &[(NodeId, RaftMsg)]) -> bool {
    resp.iter()
        .any(|(_, m)| matches!(m, RaftMsg::RequestVoteResp { granted: true, .. }))
}

/// The core safety cell: node B (the eventual wipe victim) genuinely grants
/// node A's vote in term T, is then wiped and restarts fresh (an ordinary
/// `RaftCore::new`, exactly what `node.rs`'s `drive` builds on an empty
/// WAL), resolves the boot-time cluster check against a peer proving the
/// cluster already exists AND already recognizes B as one of its own
/// voters -- and must then refuse a SECOND, contradicting `RequestVote` for
/// the SAME term T, from a DIFFERENT candidate C, that a real network could
/// plausibly still deliver after the restart.
#[test]
fn wiped_voter_never_grants_a_second_contradicting_vote_in_the_same_term() {
    let ids = triple();
    let construct_now = Nanos(0);
    let now = Nanos(1_000_000_000);

    // --- Elect A leader in term T, with B's REAL, persisted vote.
    let mut a: RaftCore = RaftCore::new(ids[0].clone(), &ids, construct_now, 7);
    let term = elect(&mut a, ids[1].clone(), now);

    // B's own core at the moment it genuinely granted A's vote in `term` --
    // hand-driven via the exact `RequestVote` A would have sent, so B's own
    // `voted_for` really is durably set to A in `term` (this is the state
    // that gets lost in the wipe below).
    let mut b_live: RaftCore = RaftCore::new(ids[1].clone(), &ids, construct_now, 7);
    let a_grant = b_live.handle(ids[0].clone(), request_vote(ids[0].clone(), term), now, 7);
    assert!(
        granted(&a_grant),
        "B should have genuinely granted A's vote in term {term} before the wipe"
    );

    // --- The wipe: B's disk (WAL, including `voted_for`) is gone. A
    // freshly-booted `RaftCore` is exactly what `node.rs`'s `drive` builds
    // on an empty WAL (`RaftCore::new`, never `recovered`).
    let mut b_wiped: RaftCore = RaftCore::new(ids[1].clone(), &ids, construct_now, 7);
    let _ = b_wiped.begin_cluster_check(now, 7);
    assert!(
        b_wiped.cluster_check_pending(),
        "a freshly-wiped voter must not vote until the boot-time check resolves"
    );

    // BOTH of B's peers (A and C) answer B's probe honestly: real history
    // (this cluster already has a committed leader in `term`), AND their
    // own committed config already names B as a voter -- exactly the
    // "already-established voter, disk wiped" signal (as opposed to an ADR
    // 0060 growth join, which their config would NOT yet contain). Per the
    // 2026-09-15 amendment (a real bootstrap-race regression this exact
    // mechanism was found to cause under real `ProdEnv` threading), a
    // refusal verdict requires evidence from EVERY configured peer, never
    // just the first one to reply -- so both must answer before
    // `refused_as_voter()` can become true.
    let all_voters: BTreeSet<NodeId> = ids.iter().cloned().collect();
    for peer in [ids[0].clone(), ids[2].clone()] {
        let _ = b_wiped.handle(
            peer,
            RaftMsg::ClusterProbeResp {
                term,
                committed_index: 1,
                config: all_voters.clone(),
            },
            now,
            7,
        );
    }
    assert!(
        b_wiped.refused_as_voter(),
        "B must recognize itself as a wiped, already-established voter and refuse"
    );

    // --- The race: a DIFFERENT candidate, C, requests B's vote for the
    // EXACT SAME term B already voted in before the wipe -- a real network
    // could plausibly still deliver this (a delayed message from a
    // contested original election, or simply a retry) after B's restart.
    let c_resp = b_wiped.handle(ids[2].clone(), request_vote(ids[2].clone(), term), now, 7);
    assert!(
        !granted(&c_resp),
        "the wiped voter granted C's vote in the SAME term ({term}) it already granted \
         A's vote in before the wipe -- exactly the double vote issue #667 exists to prevent"
    );

    // Defense in depth: not even a re-ask from the ORIGINAL candidate A
    // grants once refused -- a refused voter votes for no one, ever, until
    // it is re-admitted through the learner/rejoin path.
    let a_resp = b_wiped.handle(ids[0].clone(), request_vote(ids[0].clone(), term), now, 7);
    assert!(
        !granted(&a_resp),
        "a permanently refused voter must grant no vote at all, not just refuse the new candidate"
    );
}

/// Chains the same setup one step further: without the fix, B's second
/// grant to C is enough (2 of 3 votes: C's own + B's) to make C a genuine
/// SECOND leader in the exact same term A is already leader in -- the
/// formal Raft safety violation ("Election Safety": at most one leader per
/// term) this whole mechanism exists to prevent. With the fix, B never
/// grants, so C can never reach a majority this way and no second leader
/// is ever elected.
#[test]
fn a_double_grant_would_elect_two_leaders_in_the_same_term() {
    let ids = triple();
    let construct_now = Nanos(0);
    let now = Nanos(1_000_000_000);

    let mut a: RaftCore = RaftCore::new(ids[0].clone(), &ids, construct_now, 7);
    let term = elect(&mut a, ids[1].clone(), now);
    assert!(a.is_leader());

    let mut b_wiped: RaftCore = RaftCore::new(ids[1].clone(), &ids, construct_now, 7);
    let _ = b_wiped.begin_cluster_check(now, 7);
    let all_voters: BTreeSet<NodeId> = ids.iter().cloned().collect();
    // Both of B's peers must answer before a refusal verdict is reached
    // (2026-09-15 amendment) -- see the sibling test's own updated comment.
    for peer in [ids[0].clone(), ids[2].clone()] {
        let _ = b_wiped.handle(
            peer,
            RaftMsg::ClusterProbeResp {
                term,
                committed_index: 1,
                config: all_voters.clone(),
            },
            now,
            7,
        );
    }
    assert!(b_wiped.refused_as_voter());

    // Drive C's own candidacy for real (pre-vote is ungated even for a
    // still-checking/refused voter -- see `handle_pre_vote`'s own doc -- so
    // B genuinely tips C into a real candidacy exactly like it would any
    // other pre-candidate; only the REAL vote below is where the fix's gate
    // lives). Majority in this 3-node group is 2, so B's single external
    // grant is enough at each stage (self + B), exactly as `elect`'s own
    // 2-grant shape (pre-vote then real vote) already relies on for A above.
    let mut c: RaftCore = RaftCore::new(ids[2].clone(), &ids, construct_now, 7);
    let pre_vote_round = c.tick(now, 7);
    let mut real_vote_round: Vec<(NodeId, RaftMsg)> = Vec::new();
    for (to, msg) in &pre_vote_round {
        if *to == ids[1] {
            for (back_to, resp) in b_wiped.handle(ids[2].clone(), msg.clone(), now, 7) {
                if back_to == ids[2] {
                    // Feeding a granted majority pre-vote back tips C
                    // straight into a real candidacy, whose return is C's
                    // own real `RequestVote` broadcast.
                    real_vote_round.extend(c.handle(ids[1].clone(), resp, now, 7));
                }
            }
        }
    }
    assert!(
        c.role() != animus_control::Role::Follower,
        "B's pre-vote grant should have tipped C into a real candidacy"
    );
    for (to, msg) in &real_vote_round {
        if *to == ids[1] {
            for (back_to, resp) in b_wiped.handle(ids[2].clone(), msg.clone(), now, 7) {
                if back_to == ids[2] {
                    let _ = c.handle(ids[1].clone(), resp, now, 7);
                }
            }
        }
    }

    assert!(
        a.is_leader(),
        "A should still (stale-)believe itself leader in term {term}"
    );
    assert!(
        !c.is_leader(),
        "C must NOT have become a second leader in term {term} via B's stale/refused vote -- \
         a formal Election Safety violation (two leaders, one term) if it had"
    );
}
