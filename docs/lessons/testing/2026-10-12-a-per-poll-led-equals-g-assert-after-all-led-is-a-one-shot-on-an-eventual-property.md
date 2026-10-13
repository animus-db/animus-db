# A per-poll `led == g` assert after "all led" is a one-shot assert on an eventual property

`election_backpressure_liveness_g300` waited for all 300 groups to be led, then
polled the census while the groups quiesced and asserted `led == g` on every
poll. In the merge queue (and locally, about 1 run in 20 under CPU
oversubscription) it failed with `a group lost its leader while idle (299 vs
300)`.

Why the assert was wrong: leadership is an eventual property. Under CPU
starvation a leader can legitimately step down or be replaced (availability,
not safety; ADR 0078's #1297 amendment documents late pre-vote grants under
overload). "All led once" does not imply "all led at every later instant", so a
per-poll equality is a fixed-instant check of an eventual property (CLAUDE.md:
converged-or-timeout, never a one-shot).

Fix: track when each tablet was first seen leaderless and fail only if one
stays leaderless longer than the ADR 0078 lost-leader bound
(`lost_leader_bound(g)`, the ADR's own formula at the conservative queue
length). Transient losses are counted and printed
(`idle_leader_losses`, `worst_leaderless_ms`), so the rig shows how often the
availability blip happens instead of hiding it. The loop still exits only when
all groups are led and quiesced.

Reproduction: loop the single test under at least 2x CPU oversubscription with
`yes > /dev/null` hogs on the 4-core box. Three hogs did not reproduce it;
eight did. A parallel `cargo test` of the whole file is not a substitute (it
changes what the test measures); loop the one test name.

Rule: after any `await_*` that establishes a liveness property, later polls must
assert a bounded-recovery property, not the same instantaneous one.
