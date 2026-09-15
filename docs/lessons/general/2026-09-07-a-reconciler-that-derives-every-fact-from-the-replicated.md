# A reconciler that derives every fact from the replicated map alone has no way to remember what the map has forgotten — the node's own local namespace is a second, restart-surviving fact source (issue #722, closing ADR 0061 rung D4 PR 3's own finding)

The entry immediately above this one is the *discovery*; this is the
*general* lesson the fix generalizes to, worth stating on its own since
the shape recurs anywhere a per-node reconciler exists.

**The pattern.** A reconciler of this shape — `gather_facts` (impure,
reads live state) → `plan` (pure, decides actions) → execute — is only as
complete as its own fact-gathering. When every fact comes from ONE source
(here, replicated `Metadata` plus in-process `LocalState`), the
reconciler's own knowledge is bounded by what that source currently says,
full stop — and a source that is itself a replicated, converging,
eventually-consistent view (as opposed to this node's own durable,
locally-observable reality) can, by construction, have already moved past
a fact this node still has physical consequences of. `LocalState` here is
the sharper case: it isn't even eventually consistent, it's simply gone on
every restart, by design (root `CLAUDE.md`'s "a restart re-derives
everything live" convention, correct for every OTHER fact this reconciler
needs). The bug wasn't that `Metadata` was wrong — it was right, exactly
as designed, the instant it converges. The bug was that NOTHING besides
`Metadata` was ever consulted, so a fact `Metadata` had already discarded
(this tablet used to be real) had nowhere left to be remembered from.

**The fix's general shape, not just this instance's.** The node's own
local namespace — the durable artifacts it created in direct response to
an earlier, real fact — is itself a second fact source, independent of
whatever the replicated view currently says, and it survives exactly the
restart that wipes in-process state. Reading it costs something (a
directory listing here), so the general technique is: consult it
**once**, at the point where the gap can occur (this reconciler's first
tick after construction — the only point where in-process state is known
to be freshly wiped while local durable state might not be), fold its
answer into the SAME decision path every other fact already flows through
(here: insert into the existing `LocalState::hosted`/reclaim machinery,
add no new action variant), and stop consulting it once the window it
exists to cover has passed (a local artifact appearing after that first
tick can only be this same process's own later actions, already tracked
by the ordinary in-process bookkeeping). This is cheap because the gap is
narrow (once per restart) and the alternative (consulting it every tick)
is the exact "materializes the whole dataset on every poll" cost class
`docs/adr/0061-*.md`'s own D3/D4 amendments and this file's own
`/admin/raftkv` entry already warn against paying for no reason.

**The safety argument this kind of fix always needs, stated once so it
doesn't need re-deriving per instance**: a local artifact this node
created only ever exists because this SAME node, at some prior point,
itself observed the fact that justified creating it — so an artifact that
outlives the replicated view's own record of that fact is never "created
too early" (structurally impossible — nothing creates the artifact before
observing the fact), only ever "the replicated view moved on without this
node." The corollary that actually needs checking, case by case: does
anything ELSE in the system create the same kind of local artifact
EARLIER than the replicated view records the matching fact, for a
DIFFERENT reason? Here, yes — an in-place split's child engine is
materialized before the child has its own tablet-map entry, purely as a
latency optimization (ADR 0058 Train 2 rung 4/rung 4 layer 1) — so the
fix's own "known" set had to be widened to also recognize a fact recorded
elsewhere in the replicated view (a parent's own still-live split intent
naming its children) rather than only literal tablet-map keys. Skipping
this check is exactly the kind of "worked in the simple case, silently
wrong in the one deliberately-eager-materialization case" bug this
codebase's own split-path history (ADR 0058's several rungs) already shows
is easy to introduce and hard to notice without deliberately asking "what
else legitimately creates this kind of artifact before its own map entry
exists."

**Two harness bugs, unrelated to the fix, were found delivering its
positive regression test — both worth naming as their own small,
general lessons.** (1) A test that crashes a random node and then depends
on ANY OTHER cluster-wide consensus completing (here, `DeleteTable`'s own
control-plane commit-wait) must exclude every leadership role that crash
could plausibly hit, not just the one role the test happens to be
about — excluding only the data-plane tablet leader left a coin-flip
chance of also crashing the control-plane leader, an entirely different
(and, empirically, sometimes slower-than-the-test's-own-budget) scenario
nobody meant to test. (2) `assert_reclaimed`'s own convergence check used
to be two passes — a metadata/hosted-set poll, then a separate, unwaited
engine read — which is unsound specifically for a just-restarted node,
whose metadata/hosted-set facts can read as "already converged" from the
very first poll (a freshly restarted control replica starts genuinely
blank, indistinguishable at that instant from "caught up"), well before
the reconciler backing those facts has done any real work; the fix folds
every observable of one converged-or-timeout property into the SAME poll,
never split across passes with different implicit timing. Both are
instances of lessons this file already has in more general form
(seed-derived randomness needs its blast radius considered, not just its
target; a converged-or-timeout property must be checked as ONE property)
— recorded here specifically because this exact fixture is where both
were found, for the next person debugging this file.
