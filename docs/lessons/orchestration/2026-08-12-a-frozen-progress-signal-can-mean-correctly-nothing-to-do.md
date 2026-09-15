# A "frozen" progress signal can mean "correctly nothing to do," not "stuck" — instrument the decision loop itself before assuming starvation.

**A "frozen" progress signal can mean "correctly nothing to do," not
"stuck" — instrument the decision loop itself before assuming
starvation.** The investigation above spent real time on two starvation-
shaped hypotheses (a parked/starved reconcile task; cross-test port reuse
poisoning a connection) before a direct `eprintln!` inside `reconcile_
loop` — printing `is_leader`, the full `PlacementView`, and the proposal
count every tick — immediately showed the loop healthy and the *decision*
wrong. A frozen `engine_applied_index`/`commit_index` (the generic
progress signal from the DRIVER_APPLIED entry above) only tells you
"nothing committed" — it cannot distinguish a starved proposer from a
proposer that correctly has nothing to propose. When a progress signal is
frozen for far longer than any documented contention precedent (here:
200s+ solid vs. the ~60s the DRIVER_APPLIED entry above had already
characterized as normal-under-load), don't keep widening the timeout or
chasing scheduling theories — instrument the specific decision function in
the loop that would need to fire, and read its actual inputs/output. It is
almost always cheaper than the starvation hypotheses it rules out.
