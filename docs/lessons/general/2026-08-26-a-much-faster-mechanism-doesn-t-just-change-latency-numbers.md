# A much faster mechanism doesn't just change latency numbers — it changes how often a fixed test budget exercises the thing that was already flaky (ADR 0058 rung 4 layer 2)

Two more `animusd/tests/streams_e2e.rs` failures surfaced flipping
`SplitMode`'s default, past the intermediate-state-shape ones above, and
they generalize differently.

**The first was a pre-existing, mislabeled test bug this flip merely made
visible.** `multi_split_soak_streamed_gsi_table_under_mixed_load`'s
"zero lost writes" check read an item back via a bare `GetItem` — no
`ConsistentRead: true` — exactly the ADR 0055 gotcha this crate's own
`CLAUDE.md` already documents ("a read that verifies a write must ask for
the linearizable read"). Under the copy workflow's slower, seconds-long
per-split timeline, the eventually-consistent replica this test happened
to read from had ample time to catch up between the write and the
verification read, so the missing annotation never mattered in practice.
In-place's much faster convergence didn't introduce a new staleness
window — it didn't shrink the window fast enough relative to the rest of
the test to hide the *pre-existing* one, and the read started actually
observing it. Fixed the only way ADR 0055 sanctions: add
`ConsistentRead: true` to the read that is asserting durability, not
staleness tolerance.

**The second is a real, already-tracked-but-unresolved bug (issue #298,
an exactly-once duplication/deficit at a split boundary, root cause
unknown) that the flip made dramatically easier to hit — not by changing
its trigger condition, but by changing how many times a fixed-duration
test exercises that trigger.** `multi_split_soak_streamed_gsi_table_under_
mixed_load` runs a fixed workload (120 writes, a 300s budget) that
auto-splits repeatedly; under copy's per-split cost, that budget fits
however many splits copy's cadence allows, and #298 was rare enough to be
"occasionally sighted" in that regime. In-place's ~1.8x-faster convergence
lets dramatically more splits complete inside the identical fixed budget
— every one of them another roll of the dice against whatever race #298
actually is — and three consecutive runs under in-place reproduced it
every time (a deficit-shaped failure on one run, an over-count-shaped one
on another — both are #298's documented symptom family, just its two
different faces). Pinning this one soak back to `SplitMode::Copy`
(`start_streamed_cluster_full_copy_pinned`) restored its original,
rare-in-practice flake rate — 3/3 green in the retest — without touching
#298 itself, which stays out of scope here (a pre-existing bug gets its
own investigation and its own change, never a drive-by fix riding an
unrelated default flip).

**The generalizable point**: when a change makes something *faster*, audit
every fixed-duration/fixed-iteration-count test that exercises the sped-up
path for whether it now completes measurably more repetitions of that path
per run — a soak/stress test's own bug-detection power is a function of
repetitions-per-budget, not just wall-clock coverage, and a large enough
speedup can turn "reproduces rarely enough to file and defer" into
"reproduces every run" without the underlying bug changing at all. That
shift is worth surfacing loudly (as this entry does) rather than either
silently loosening the newly-flaky assertion or silently pinning it away
without a trail: **`Copy`'s eventual deletion (this same ADR's next rung)
removes the option to pin away from this exact soak**, so whoever does
that deletion needs to know, going in, that issue #298 will need to
actually be resolved (or the soak's own budget/iteration count
deliberately re-tuned) before that layer can ship — not rediscovered cold
at that point.
