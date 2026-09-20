# A per-term commitment (`voted_for`) must not be gated by a deadline field a *different*, unrelated retry loop keeps refreshing

While closing issue #930 (`docs/lessons/code-patterns/2026-09-16-a-voter-
that-just-granted-a-real-vote-has-no-pre-vote.md`), the first fix attempt
widened `handle_pre_vote`'s leader-lease check from `leader_id.is_some() &&
now < election_deadline` to a bare `(leader_id.is_some() || voted_for.is_
some()) && now < election_deadline` — reusing the exact same
`election_deadline` field the existing `leader_id` check already used. This
looked like the obviously-correct minimal diff (the field the ADR text
itself pointed at), compiled clean, passed the two new targeted unit tests,
and deadlocked the single most common Raft recovery path there is: after
*any* leader crash, every surviving follower already holds `voted_for =
Some(<the dead leader>)` for the still-current term, and nothing but a
higher term ever clears it (by design — a real vote is a durable per-term
commitment, not something a mere timeout may retract). Meanwhile
`start_pre_vote` — the handler for a node's *own* election timeout, entirely
unrelated to any vote — refreshes that *same* `election_deadline` field
every time it re-arms a fresh pre-vote round, for as long as no majority is
reached, i.e. forever while the cluster is leaderless. Combine the two and
every survivor's stale vote for the now-dead leader looks perpetually fresh
by way of a deadline something else keeps extending, so no survivor ever
grants another's pre-vote and the cluster can never re-elect. Only the
pre-existing, already-green `election_still_succeeds_when_leader_is_gone`
cluster test caught this — going from green to red under the naive draft —
because a brand-new unit test written to prove the fix only ever exercises
the new code path, never the old, ordinary path the fix can quietly break.

**The generalizable rule**: before gating field A's validity on "did field B
change recently" (a shared deadline, generation counter, or similar), check
whether *every* writer of B writes it in the *same* breath as A, for the
*same* reason. If some other, independent retry/backoff loop can also bump
B for a completely unrelated reason, then "B was recently touched" no longer
implies "A is still trustworthy" — it just means the unrelated loop is still
running. The fix here is to further gate the check on the caller's own role/
state at the moment it was set (`Follower` granting a real vote, or
`Candidate` self-voting — the two call sites that pair the `voted_for` write
with the *same* `reset_election_timer` call), so the check only ever fires
in the specific state transition where the deadline and the commitment were
established together, not on any later, unrelated refresh of the same
field. Don't add a second, dedicated timestamp field to disentangle two
purposes sharing one field unless the role-based gate genuinely can't
express the distinction — here it could, cheaply, with no new state.

See `docs/adr/0009-in-house-raft-over-env.md`'s 2026-09-19 amendment and
`animus-control/tests/pre_vote.rs` for the concrete fix and its coverage.
