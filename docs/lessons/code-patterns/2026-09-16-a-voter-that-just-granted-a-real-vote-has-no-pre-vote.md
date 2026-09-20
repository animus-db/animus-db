# A voter that just granted a REAL vote has no pre-vote "live leader" protection until it receives the winner's first `AppendEntries` — a brief window where a third voter's own (real, unprotected) candidacy can win a return bout

`handle_pre_vote`'s "live leader" lease (`has_live_leader = role == Leader ||
(leader_id.is_some() && now < election_deadline)`) is what stops a partitioned
node's own election timeout from disrupting a healthy cluster — but
`leader_id` is only ever set by `handle_append_entries`, never by
`handle_request_vote`. A voter that grants a REAL vote for a higher term (via
the generic higher-term step-down in `RaftCore::handle`, which clears
`leader_id` before dispatching) has therefore cast a real, term-advancing vote
for the winner but has **no recorded live leader** of its own until that
winner's first `AppendEntries` actually arrives — a real, if usually brief,
window with no pre-vote protection at all. If another voter's own election
timeout fires inside that window (plausible when its own link to the new
winner is genuinely slow, e.g. under fault-injected network degradation, or
just real scheduling jitter), it can win a **pre-vote** round against these
unprotected voters, then a **real** election (real votes have no
`has_live_leader` gate at all — only pre-vote does), deposing the
just-elected leader in a return bout that can occasionally chain for several
real seconds before settling. Not a safety violation (every step still
follows Raft's term/log rules) — a liveness/stability hiccup, and
`animus-control/tests/transfer_third_voter_wins.rs` already documents the
closely related "third voter wins the transfer's own election outright"
shape. Surfaced while authoring `sim_cluster_control_membership_admin.rs`'s
issue #923 regression (`remove_right_after_leadership_transfer_is_not_
refused_for_merely_slow_peers`): ~10% of a 40-seed sweep hit a multi-second
dueling-candidacy bout under a scenario combining a directed transfer with
two genuinely slow (not dropped, not partitioned) peer links. Worked around
in that test by requiring a confirmed-stable holding period after a transfer
before trusting it landed, and by hand-picking seeds for the small fixed
`_over_seeds` set that don't hit it; not fixed here — it is a separate,
narrow Raft-core liveness question, out of scope for the guard-timing issue
that test exists to pin, and worth its own investigation (and its own test)
if it ever needs closing.

## Fixed (2026-09-19, issue #930)

`handle_pre_vote`'s lease now also covers a **role-gated** `voted_for.is_some()`
(`Follower`/`Candidate` only — never `PreCandidate`/`Leader`), since granting a
real vote already reset `election_deadline` the same way a heartbeat does. See
`docs/adr/0009-in-house-raft-over-env.md`'s 2026-09-19 amendment for the full
mechanism, and `animus-control/tests/pre_vote.rs`'s
`prevote_rejected_after_granting_a_real_vote_until_deadline`/
`prevote_rejected_by_a_candidate_within_its_own_election_deadline` for the
regression coverage (both confirmed red before the fix, green after).

**The role gate is the load-bearing part, not a nicety.** A first draft used
a bare `voted_for.is_some() && now < election_deadline` with no role
restriction, and it deadlocked the single most common recovery path there
is: after any leader crash, every surviving follower already has `voted_for
= Some(<the dead leader>)` for the still-current term, and nothing but a
higher term ever clears it. Meanwhile `start_pre_vote` — the handler for a
node's *own* election timeout — keeps refreshing `election_deadline` every
time it re-arms a fresh pre-vote round, forever, independent of any vote.
Combine the two and every survivor believes it has a live leader for as
long as it keeps timing out into new rounds — i.e. permanently — so no
survivor ever grants another's pre-vote and the cluster can never
re-elect. This is a general shape worth naming: **a field that is genuinely
a per-term commitment (must survive across unrelated retries) must not be
gated by a deadline that some *other*, unrelated retry loop keeps
refreshing.** The fix is to scope the check to the specific role/transition
in which that deadline and that commitment were set *together* (here,
`Follower` granting a vote or `Candidate` self-voting — both pair the write
to `voted_for` with the same `reset_election_timer` call) rather than
reusing a shared timer field across unrelated purposes. Caught immediately
by the pre-existing `election_still_succeeds_when_leader_is_gone` cluster
test flipping from green to red under the naive draft — a useful reminder
that a "boring", already-green end-to-end liveness test is exactly what
catches this class of regression that a brand-new, narrowly-targeted unit
test cannot (it only exercises the new code path, never the old one this
kind of fix can quietly break).
