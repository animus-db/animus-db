# Forgetting a walker's tracking state on a terminal outcome is as dangerous as forgetting to advance a cursor — a discovery step that re-lists history forever will resurrect it (issue #755, `streams_e2e.rs::drain_all_tablets_lineage`)

Two prior entries in this file (grep `drain_all_tablets_lineage`) already
fixed the "resume a spent open-tail iterator as if it were a fresh closed
epoch" race in this same helper. Issue #755 was a *third*, structurally
different bug in the same function, wearing the same symptom (a surplus
delivered record under real cluster contention): the helper's
`RecordsPoll::Trimmed` arm — reached when a speculative open-tail poll
guesses an epoch that turns out never to have existed, because the tablet
retired one epoch earlier than the stale `Metadata` read believed — used
to call the equivalent of `tracked.remove`/`next_epoch.remove` on the
theory that "this tablet's children already cover it, so forgetting it
loses nothing." That reasoning is true for the *data* and false for the
*walker's own bookkeeping*: `stream_tablet_ids` (the helper's
`DescribeStream`-driven tablet-discovery step) lists every shard a tablet
ever had, retired or not, **forever** — so a tablet dropped from `tracked`
looks brand new to the discovery fold-in on the very next pass, gets
reseeded at `next_epoch = 0`, and has its entire already-delivered lineage
walked and redelivered a second time. **General form: forgetting a
walker's per-item tracking state on a terminal/error outcome is not
automatically safe just because the item's future is spoken for
(retired, superseded, split) — check whether anything else in the same
loop treats "not currently tracked" as "never seen," because if it does,
forgetting is indistinguishable from resurrecting.** The fix keeps the
tablet tracked with its cursor exactly where the closed-epoch loop left
it and drops only the stale open-tail pin (`LineageCursors::epoch_trimmed`,
extracted as a pure struct for exactly this reason — see below).

**Diagnosis method, for the next agent facing an "already characterized,
can't reproduce" flake report**: the issue itself candidly said a
follow-up characterization (8 runs, including reverting an unrelated
suspect commit) could not reproduce the failure, and pointed at the
*already-fixed* open-tail race as the "leading candidate" purely because
it was the only documented mechanism on hand — not because anyone had
confirmed it was still open. Re-reading the current code line by line
against every doc comment's claimed invariant (not just skimming the doc
comments themselves) found the actual gap: the `Trimmed` arm's own doc
comment asserted "dropping this tablet's own bookkeeping here loses
nothing," which is exactly the kind of claim worth checking against the
*caller* of the function that repopulates `tracked`, not just against the
`Trimmed` arm in isolation. A static trace of every state transition
(what removes an entry vs. what re-adds one, and whether those two ends
agree on what "gone" means) found a genuine bug with no cluster run
required — cheaper and more conclusive than another round of "run it N
times and hope."

**Testability lesson**: the whole four-map cursor bookkeeping
(`tracked`/`next_epoch`/`open_epoch`/`open_iterator`) was extracted into a
pure, synchronous `LineageCursors` struct with one method per state
transition, wired into the real async helper so there is exactly one
implementation (no drift risk between "the tested logic" and "the real
logic"). This turned a bug whose only prior evidence was one flaky
real-thread `ProdEnv` panic message into a table-driven unit test that
reproduces deterministically on every run: the test was first written
against a `epoch_trimmed` that reinstated the old `tracked.remove`
behavior (confirmed red — the exact "must never be reported as newly
discovered" assertion failed), then against the fix (confirmed green).
**Whenever a real-cluster test's failure is explained by a specific,
nameable state-machine mistake rather than "timing," try extracting that
state machine into a pure struct before reaching for a heavier
simulation harness** — it is almost always possible for cursor/bookkeeping
logic specifically (as opposed to genuine multi-process timing), and it
turns a probabilistic real-thread reproduction into a deterministic one
that runs in milliseconds.
