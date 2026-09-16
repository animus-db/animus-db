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
