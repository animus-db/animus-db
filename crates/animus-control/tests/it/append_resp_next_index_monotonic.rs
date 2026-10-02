//! Regression for issue #1070: `RaftCore::handle_append_resp`'s ordinary
//! success path used to update `next_index` with a bare
//! `self.next_index.insert(from, match_index + 1)`, not a monotonic
//! `.max(..)` — unlike `match_index`'s own update two lines above in the
//! same function, and unlike `handle_install_snapshot_resp`'s completion
//! branch, which was already hardened to be monotonic for the identical
//! reason.
//!
//! **Is there a legitimate reason a genuine `success` ack should ever move
//! `next_index` backward? No.** A `success` ack is the follower's own
//! affirmative claim that its log matches the leader's through
//! `match_index` — the ONLY way a follower's log can be shorter than a
//! leader previously believed (a conflicting leader truncated it) is
//! reported via a **reject** (`success: false`), never a `success`, and
//! `handle_append_resp`'s reject branch already handles that case on its
//! own terms (a plain `next_index -= 1` backoff, unaffected by this fix —
//! see this file's own `a_reject_ack_still_backs_off_next_index_normally`
//! below). So under sustained, possibly out-of-order replication to one
//! peer (confirmed live building issue #1064's own regression: a
//! synchronous multi-propose burst can have several `AppendEntries`
//! outstanding to the same peer at once, whose acks can arrive back
//! out of send order), a STALE success ack for a smaller `match_index`
//! must never be allowed to regress `next_index` behind what a fresher,
//! larger-`match_index` ack already proved.
//!
//! Hand-drives a single leader `RaftCore` directly (no follower core, no
//! `Simulator`) — the property under test is a pure function of this
//! leader's own `handle_append_resp` decision, so a hand-crafted
//! `RaftMsg::AppendEntriesResp` is simpler and more direct than routing it
//! through a second real core.

use animus_control::{MetaCommand, NodeStatus, ProposeResult, RaftCore, RaftMsg};
use animus_env::{Nanos, nid};
use std::collections::BTreeMap;

fn upsert(node: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node: nid(node),
        labels: BTreeMap::new(),
        status: NodeStatus::Active,
    }
}

/// Elect `nid(0)` leader of the 2-node group `[nid(0), nid(1)]`, mirroring
/// `stale_snapshot_no_rewind.rs`'s own `elect_leader_of_pair` exactly (kept
/// local rather than shared, since that file's own helper is private to
/// it).
fn elect_leader_of_pair(now: Nanos) -> RaftCore {
    let mut leader: RaftCore = RaftCore::new(nid(0), &[nid(0), nid(1)], Nanos(0), 7);
    let _ = leader.tick(now, 7); // election timeout -> pre-candidate, PreVote
    let _ = leader.handle(
        nid(1),
        RaftMsg::PreVoteResp {
            term: leader.term() + 1,
            granted: true,
        },
        now,
        7,
    );
    let _ = leader.handle(
        nid(1),
        RaftMsg::RequestVoteResp {
            term: leader.term(),
            granted: true,
        },
        now,
        7,
    );
    assert!(leader.is_leader(), "node 0 should have won the election");
    leader
}

fn success_ack(term: u64, match_index: u64) -> RaftMsg {
    RaftMsg::AppendEntriesResp {
        term,
        success: true,
        match_index,
        needs_snapshot: false,
        check_pending: false,
    }
}

/// A fresh, larger `match_index` ack followed by a STALE, smaller one
/// (simulating the smaller one's own request having been built and sent
/// earlier, but its response arriving back later — a real possibility once
/// more than one `AppendEntries` can be outstanding at once) must leave
/// `next_index` at the HIGHER watermark, not regress it. Observed
/// indirectly: once `next_index > last_log_index()`, `handle_append_resp`'s
/// own success branch returns an EMPTY `Vec` (nothing left to replicate) —
/// so a non-empty return after the stale ack is a direct, positive proof
/// that `next_index` regressed.
#[test]
fn an_out_of_order_stale_success_ack_never_moves_next_index_backward() {
    let now = Nanos(1_000_000_000);
    let mut leader = elect_leader_of_pair(now);
    let term = leader.term();

    // Build up a small log beyond the election no-op.
    for i in 0..5u64 {
        let res = leader.propose(upsert(100 + i));
        assert!(
            matches!(res, ProposeResult::Accepted { .. }),
            "propose {i} must be accepted by the leader"
        );
    }
    let last = leader.last_log_index();
    assert!(last >= 5, "expected at least 5 log entries, got {last}");

    // The follower fully catches up: next_index should advance past
    // `last`, leaving nothing to replicate.
    let out = leader.handle(nid(1), success_ack(term, last), now, 7);
    assert!(
        out.is_empty(),
        "seed-free: a follower fully caught up (match_index == last_log_index) must leave \
         nothing to replicate, got {out:?}"
    );

    // A STALE, out-of-order success ack for an EARLIER match_index arrives
    // after the fresher one above. It must not regress next_index: the
    // follower is already known (by the fresher ack) to hold everything
    // through `last`, so this leader must still have nothing to replicate.
    let stale_match = last.saturating_sub(3);
    let out = leader.handle(nid(1), success_ack(term, stale_match), now, 7);
    assert!(
        out.is_empty(),
        "seed-free: a stale success ack (match_index={stale_match}, after an already-processed \
         fresher ack at match_index={last}) regressed next_index — got a non-empty replicate \
         response {out:?} where none should be needed (issue #1070)"
    );

    // A THIRD, genuinely fresh ack proving further progress must still
    // advance normally — the fix must not have wedged next_index in place.
    let res = leader.propose(upsert(200));
    assert!(matches!(res, ProposeResult::Accepted { .. }));
    let new_last = leader.last_log_index();
    assert!(new_last > last, "a further propose must grow the log");
    let out = leader.handle(nid(1), success_ack(term, new_last), now, 7);
    assert!(
        out.is_empty(),
        "seed-free: a genuinely fresh, larger match_index ack must still be accepted normally \
         after the fix, got {out:?}"
    );
}

/// The reject path is deliberately UNCHANGED by this fix — a reject is the
/// follower's own report that its log does NOT match, so decrementing
/// `next_index` by one and retrying is the correct, intentional Raft
/// backoff, not a monotonicity bug. This is a plain sanity check that the
/// fix above did not accidentally touch this branch: a reject still
/// produces a real replicate attempt (a non-empty response), and repeated
/// rejects keep backing off rather than getting stuck.
#[test]
fn a_reject_ack_still_backs_off_next_index_normally() {
    let now = Nanos(1_000_000_000);
    let mut leader = elect_leader_of_pair(now);
    let term = leader.term();
    for i in 0..5u64 {
        let res = leader.propose(upsert(100 + i));
        assert!(matches!(res, ProposeResult::Accepted { .. }));
    }

    let reject = RaftMsg::AppendEntriesResp {
        term,
        success: false,
        match_index: 0,
        needs_snapshot: false,
        check_pending: false,
    };
    let out = leader.handle(nid(1), reject.clone(), now, 7);
    assert!(
        !out.is_empty(),
        "a reject must always produce a fresh replicate attempt at the backed-off next_index"
    );
    // A second, further reject continues backing off (not stuck at the same
    // point) — still produces a fresh attempt.
    let out2 = leader.handle(nid(1), reject, now, 7);
    assert!(
        !out2.is_empty(),
        "repeated rejects must keep producing fresh replicate attempts as next_index keeps \
         backing off"
    );
}
