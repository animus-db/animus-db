# A "just compare live state to the target" convergence check races the very proposer that sets the target — and hardening the regression test found two more independent races behind it (ADR 0062 rung 6, the completion loop's settle window)

**The product race.** Rung 6's own completion loop
(`animusd::split_placing_completion`) was first written exactly as the
ADR's own minimal pseudocode: each tick, for every led tablet with an
un-`done` `split_placing` entry, propose `MarkSplitPlacingDone` the instant
`group.config() == t.replicas` (the live Raft voter set matches
`Metadata`'s own current desired replicas). This looks obviously correct —
it's the literal "is there anything left for `reconfigure_step` to do"
predicate. It is also racy in a way a code reader (including the ADR
author) would not obviously catch: **immediately after `CutoverSplit`
commits, a freshly-forked child's live group already sits on exactly its
fork-inherited `t.replicas`** (ADR 0062's own fork-first design), so
`group.config() == t.replicas` is **trivially true from the very first
tick** — before the control-plane leader's OWN reconcile loop
(`RECONCILE_INTERVAL`, 500ms) has had a single chance to bump `t.replicas`
toward the *real*, differing target `CutoverSplit` already recorded as
`split_placing[child].target`. A completion-loop tick landing in that
(near-guaranteed, since this loop's own cadence is well under 500ms)
window observes "already converged" and marks `done` before any placement
move ever happens.

**Why this wasn't caught by reasoning alone, and had to be found by
running the real thing**: the failure mode is not a crash or a rejected
command — `MarkSplitPlacingDone` happily applies (nothing about a
premature-but-otherwise-well-formed CAS is wrong), and `done` flipping
early doesn't strand the tablet (once `done` is true, `rebalance_placement`
lifts its own exclusion and the tablet becomes eligible for ordinary,
slower rebalance again) — so a first-order argument ("it self-heals, and
`done` is a diagnostic only, never a serving gate per fork A") sounds
airtight and is *wrong in practice*: `rebalance_step` moves one replica
per tick, gated behind `REBALANCE_EVERY_N_TICKS` (~4s) and only when
repair proposed nothing, and it optimizes cluster-wide balance, not "the
same lowest-id candidates `select_replicas` would pick" — so the actual,
observed outcome of losing this race was not "slightly delayed," it was
"converges to a different, worse placement, an order of magnitude slower,
reported done well before either of those things happened." A real
end-to-end test (`tests/split_placing_completion.rs`, growing a cluster
so a fresh `select_replicas` genuinely differs from a parent's fork-
inherited homes) hit this race on essentially every run, because the
race isn't a rare interleaving — it's the *default* ordering whenever the
completion loop's own tick is faster than the control-plane's reconcile
tick, which it always is.

**The fix**: require the same converged observation to hold continuously
for a settle window (`SPLIT_PLACING_DONE_SETTLE`, a small multiple of the
control-plane's own `RECONCILE_INTERVAL`) before trusting it — a
driver-local `BTreeMap<TabletId, Nanos>` of "first seen converged at",
cleared the instant a tick observes non-convergence. This is the same
"wait out a slower sibling loop's own worst-case reaction window before
trusting an observation" shape `index_drain.rs`'s in-place cutover driver
already uses (`INPLACE_SPLIT_MATERIALIZE_SETTLE_MS`, closing an analogous
race against the tablet-host reconciler's own fallback cadence) — worth
recognizing as a *recurring* pattern class in this codebase: whenever a
fast, independently-scheduled loop's own read can observe a slower loop's
"before" state and mistake it for the "after" state, a settle window
(never a one-shot check) is the fix, and the settle duration is set by the
*slower* loop's own worst-case cadence, not the fast loop's own tick rate.

**General lesson (the product race)**: "a premature/wrong observation is
harmless because a slower fallback mechanism will eventually correct it"
is not the same claim as "a premature/wrong observation is cheap" — trace
the fallback's own actual speed and its own actual target-selection
algorithm before accepting that argument, especially when the fallback is
a *different* mechanism (here, generic load-balancing rebalance vs.
directed, target-specific placing) that was never designed to reproduce
the fast path's specific outcome, only some outcome eventually. A
convergence check comparing live state to a *just-written,
not-yet-widely-observed* target needs to ask not just "do these two values
match" but "could they match merely because the target hasn't moved yet,
not because the mover has caught up" — the two are indistinguishable from
a single snapshot and only separable by requiring the match to persist.

**Follow-up hardening: the test itself, not the loop, was ALSO asserting a
one-shot snapshot of an eventually-converging value.** The settle-window
fix above closed the real product race. It did not, on its own, make
`tests/split_placing_completion.rs` reliably green — a second pass, run to
a 15-consecutive-green bar (`for i in $(seq 15); do cargo test -p animusd
--test split_placing_completion || break; done`), surfaced two more
failure modes, both root-caused rather than papered over with a wider
timeout:

1. **`assert_eq!(fork_inherited, [n0, n1, n2])` right after
   `await_cutover_of` returns is itself a one-shot assert on an eventual
   property** — the exact mistake this repo's own Testing-section rule
   warns against, just relocated from the production loop into the test
   that was supposed to be proving the loop correct. The directed-Placing
   reconcile phase (`animus-control::node`'s 500ms `RECONCILE_INTERVAL`)
   and this rung's own completion loop are BOTH independently-scheduled
   background processes with no synchronization to `await_cutover_of`'s own
   100ms poll granularity — nothing stops either or both from having
   already finished by the exact instant the test's very next line reads
   `/admin/status`, especially under `cargo test --workspace`'s CPU
   contention (many test binaries competing for scheduler time slices makes
   a "slow" background loop tick *faster relative to* a test's own
   `sleep`-paced poll far less rare than a quiet single-binary run would
   suggest). Observed directly: a captured failure showed child 2 already
   sitting on `[m0, n0, n1]` — the fully-converged target — one line after
   `await_cutover_of` returned, well before the test's own "prove the
   convergence loop moves it" section had even started polling. **Fix**:
   stopped asserting on the tablet's own *current* `replicas` (converging,
   racy) and on `split_placing[child].done` (also converging, also racy)
   in that spot entirely; kept only the assertion on `split_placing[child]
   .target`, which `CutoverSplit` writes exactly once and the reconcile
   loop is contractually forbidden from ever rewriting (ADR 0062 §2) — the
   one field in the whole structure that is safe to assert on
   synchronously, precisely because it is not an eventual property at all.
2. **`join_extra`'s 30s outer retry deadline was a guessed constant, not
   derived from the mechanism it retries** — `run_node_join`'s own internal
   discovery poll (`JOIN_DISCOVERY_BUDGET`, 10s) means a single failed
   attempt can itself cost most of a 10s slice before the outer loop even
   gets to retry, so 30s bought only ~3 attempts under contention. Widened
   to 60s (six attempts' worth) with the reasoning written down against the
   actual constant it derives from, and the swallowed `Err` is now printed
   (`eprintln!`) so a future failure names its own cause instead of a bare
   "could not join after retries." This is remedy-class 4 from this
   incident's own review ("is the budget derived from the mechanism's
   actual worst-case, or a guess") applied to a *setup* step, not just the
   convergence poll it was easy to think of first.

**Verification discipline worth naming explicitly**: a single green run (or
even ten) of a real multi-process `ProdEnv` e2e test proves far less than
it feels like it does when the failure mode is a race with a probability
in the 10-20% range — the fix here was only trusted once a *sequential*
`for i in $(seq 15); do cargo test ... || break; done` loop (never a
background/parallel batch, which can mask exactly the CPU-contention
condition that makes the race worse) ran clean end to end, run in the
foreground so a failure stops the loop immediately rather than being
averaged away in a summary count.

**General lesson (the test hardening)**: when hardening a flaky
real-cluster test, audit *every* assertion for whether the value it checks
is a converging (eventual) one or a written-once (stable) one — not just
the assertion that produced the original failure. A test built by
iterating on whichever assertion just failed will fix them one at a time
and keep discovering new ones on each subsequent run, because a racy
assertion on a fast-converging system fails probabilistically, not
deterministically — the fix that actually stops the bleeding is a pass
over the whole test asking "which of these could this system have already
finished by the time I check it," not a reactive timeout bump on whichever
line the last failure happened to name.
