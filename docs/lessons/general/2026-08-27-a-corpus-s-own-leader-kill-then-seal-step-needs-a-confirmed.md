# A corpus's own leader-kill-then-seal step needs a confirmed write between them, not just a re-election (ADR 0059 §9/§10, Train 3 PR② corpus)

The PITR restore corpus's flagship leader-kill scenario called
`pitr_seal_now` on the newly-elected leader immediately after killing the
old one, with no write in between. At `ANIMUS_PITR_SEEDS=100` this
produced a genuine (harness-only) failure: "the group has a leader" and
"the group's own apply cursor has caught up to everything the crashed
leader had committed" are two different facts, and nothing forces the
second to be true the instant the first becomes true — `pending_changes()`
read on the fresh leader before its apply loop caught up saw a truncated
backlog, corrupting the scenario's own expected seal content. The fix
was to reorder the scenario: move the leader kill to *before* that
round's own write burst rather than immediately before the seal, so the
burst's own confirm-by-applied-index wait (already present, since every
write in this corpus confirms before moving on) forces the catch-up as a
side effect, with no new synchronization primitive needed.

**The generalizable rule, restated for corpus/harness authors
specifically** (the underlying principle — durable-before-visible,
leadership isn't apply-completeness — is already a top-level house rule):
a hand-scripted scenario that kills a leader and then immediately reads
group-local state on the replacement must interpose a confirmed write
(or an explicit apply-catchup wait) between the kill and the read, exactly
as a real client would experience via its own confirm loop — "the group
elected a new leader" is not the same wait condition as "the group is
caught up," and a scenario that conflates them can manufacture a
test-only data loss that looks identical to a real one.
