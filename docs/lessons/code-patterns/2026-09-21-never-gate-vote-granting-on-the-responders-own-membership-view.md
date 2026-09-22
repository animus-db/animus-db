# Never gate a Raft responder's vote/pre-vote *granting* decision on the responder's own membership view — only the candidate's tally may use it

Found fixing issue #1019: `animus-control`'s `handle_pre_vote`/
`handle_request_vote` additionally required `self.is_voter()` before
granting anything, reasoning that a learner is "never solicited in normal
operation anyway" and this was just "a cheap, structurally-load-bearing
second line of defense." That reasoning is what broke — the gate wasn't
redundant, it was reachable, and reaching it produced a **permanent**
election deadlock.

## The trap: a membership-change entry takes effect on append, not on commit

A node's local `config`/`learners` split reflects whatever its own log's
latest config entry says — updated the instant that entry is **appended**
(`log_append` -> `apply_config`), regardless of whether it is committed yet.
That means a node's own view of "am I a voter" can lag a majority of the
cluster's view by however long replication to that one node happens to take.
`promote_learner` only requires the learner to be caught up to within a
small threshold (4 log entries) of the leader's tip before promoting it —
not fully replicated — so the promotion's own config-change entry can reach
a real majority of the *new* voter set (committing it) while the promoted
node itself hasn't received that entry yet, if the leader dies right after
replicating to everyone except it.

Once that happens, every surviving voter needs a vote from the very node
whose own stale, pre-promotion view says it isn't a voter — and a responder
gated on `self.is_voter()` always refuses on that basis, forever. There is
no way out: the only way that node's own view can update is by hearing from
a leader, and there can never be a leader again, because electing one is
exactly the step its refusal blocks. "Never solicited in normal operation"
was true right up until the one abnormal case (a leader dying mid-promotion)
that a leader-crash fault-injection corpus exists to find.

## The generalizable rule

A Raft responder must decide whether to **grant** a pre-vote/vote using only
information that cannot be stale relative to what it is being asked to
validate: term, log up-to-dateness, and its own vote lease. It must never
additionally gate granting on its own membership status, because that status
is derived from possibly-uncommitted, possibly-not-yet-received log state —
exactly the kind of local view a majority of the cluster can already have
moved past. The safety property ("a non-voter never influences an
election") does not need a responder-side gate to hold at all: it is already
enforced on the **candidate** side, where the tally only counts a grant that
satisfies `self.config.contains(&from)` against the *candidate's* own
config — and a candidate's own config is never stale in the direction that
matters, because a candidate only exists once it has itself campaigned,
which already requires being a voter in its own view. Before adding a
"defense in depth" gate to a responder's granting decision, ask whether the
real safety property is already fully enforced elsewhere (here, the
candidate-side tally) — if so, the extra gate isn't redundant insurance,
it's a second copy of the decision made with staler information, and staler
information can disagree with the authoritative one in exactly the case
that matters.

## A companion trap in the same family: a decision this gate blocks may also stop clearing its own state

Removing the responder-side gate alone was not sufficient — a second, related
staleness bug was masked by it and only surfaced once the first was fixed.
A non-voting node's belief that a particular node is the live leader
(`leader_id`), and its **pre-vote lease** on a real vote it once granted
(`handle_pre_vote`'s `voted_lease` check), are normally allowed to go stale
only as long as one election timeout: for a **voting** follower, the moment
its own timeout fires it transitions `Follower` -> `PreCandidate`, and that
role change is what makes both `leader_id` (cleared directly) and the lease
(`voted_lease` requires role `Follower`/`Candidate`, which `PreCandidate` no
longer satisfies) decay. A learner is gated on `is_voter()` precisely so it
can never make that transition — so for a learner, neither ever decayed at
all: nothing else clears them for a node that never campaigns, because the
only other path (a real, term-bumping message) can itself never arrive while
the group is deadlocked waiting on this very node. The general version of
this trap: when a piece of state's normal expiry is a *side effect* of a
transition some class of node can never make, check whether that state can
go stale forever for that class, not just whether the transition's other
effects (here, actually campaigning) are correctly suppressed for it.

## The sharpest trap of all: the obvious fix for the companion trap is itself unsafe

The natural first attempt mirrors the voter's decay literally: clear
`leader_id` **and** `voted_for` on the same non-voter timer signal. This
compiles, passes the leader-crash regression corpus, and is wrong — caught
by review reasoning about it, not by that corpus, which is exactly why a
fix like this needs its own dedicated unit-level safety proof, not just a
liveness-shaped fault-injection scenario.

`voted_for` is not soft state like `leader_id`. It is the literal mechanism
behind Raft's "at most one real vote per term" safety property — the
property a majority-based consensus protocol cannot survive losing. The
tempting justification for clearing it here ("the candidate-side tally
already filters by `self.config.contains(&from)`, so a non-voter's vote
never counts anyway") is true in general but **false in exactly the case
this fix exists for**: the whole reason the responder's own `is_voter()` is
unreliable here is that it is *stale relative to a majority that already
promoted it*. So the responder is **not actually a non-voter** from the
candidates' point of view — it's a voter with an out-of-date self-image. A
timer-cleared `voted_for` lets that same node grant two different real
candidates a vote in the same term (its own timer firing between them, with
no leader ever arriving to correct its stale view — the exact condition
this whole fix is for), and each candidate can independently reach a real
majority using OTHER voters' own already-correct configs: two leaders, one
term, the one outcome a consensus protocol must never produce.

The fix: separate the **liveness** signal (this responder no longer
believes any particular node is worth protecting) from the **safety**
commitment (this responder has cast a specific, binding vote this term) —
they had been living in the same two fields (`leader_id`/`voted_for`) only
because a *voter's* decay path happens to retire both together via one role
transition. For the non-voter path, which has no such transition, decay
each on its own terms: `leader_id` (soft, freely clearable) directly; the
**lease that reads `voted_for`** (`voted_lease`, itself only a liveness
optimization — pre-vote exists to avoid disrupting a healthy leader, not to
enforce one-vote-per-term, which real-vote granting already does on its
own) via a new, unpersisted boolean flag that the lease check additionally
conjuncts on, leaving `voted_for` itself completely untouched. The general
rule: before "fixing" a staleness bug by clearing the state that looks
stale, ask whether that state is a safety invariant or a liveness
convenience wearing the same field name — and if two different concerns
share one field only because one code path's decay happens to retire both
together, don't assume a *different* code path's decay may retire them the
same way.

See `crates/animus-control/src/raft.rs`'s `handle_pre_vote`/
`handle_request_vote`/`start_pre_vote` and the `vote_lease_lapsed` field's
own doc, the 2026-09-21 amendment (including its rejected alternative) to
[ADR 0058](../../adr/0058-learner-replicas-in-place-split.md)'s Train 1
section, and the regression coverage:
`crates/animus-control/tests/learner_promotion_leader_crash.rs` (the
liveness half, seed-swept) and
`crates/animus-control/tests/non_voter_vote_lease.rs` (the safety half, a
bare-`RaftCore` unit cell proving a second real vote in the same term still
fails even once the lease has lapsed).
