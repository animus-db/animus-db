# A trigger's "how due is it" computation and its "is anything pending at all" gate are two different questions — conflating them makes the expensive one run on every tick instead of only when something's actually due.

**A trigger's "how due is it" computation and its "is anything pending at
all" gate are two different questions — conflating them makes the
expensive one run on every tick instead of only when something's actually
due.** The DynamoDB Streams seal arm's age trigger (`animusd::index_drain
::seal_tick`, ADR 0042/0043) used to call `CpGroup::pending_changes()` — a
full scan of the `KIND_CHANGE` scope, up to `--stream-seal-bytes`' worth
of bytes — on *every* tick of *every* streamed led tablet (5×/s at
`INDEX_DRAIN_INTERVAL`), purely to find the oldest unsealed record's own
HLC for the age trigger and a backlog metric, even on ticks where neither
trigger ends up firing. On an idle streamed tablet this was ~100% wasted
work, and — more importantly — it structurally blocked such a tablet from
ever quiescing (ADR 0044 phase 1's whole premise). **Fix (ADR 0042 fork
G, 2026-08-16)**: split the trigger into a cheap existence gate
(`approx_bytes_kind(KIND_CHANGE) > 0`, an accessor already read for the
size trigger) and a cheap age basis (`Metadata::last_seal_wall_ms`, an
O(log n) catalog lookup — "time since this tablet's own last seal," not
"age of the oldest unsealed record"), with the full scan
(`pending_changes()`) moved to run in exactly one place: inside the
branch that has *already decided* to seal.

**A never-sealed tablet (no catalog row yet) needed its own basis, and
the first two designs tried for it were both wrong in the same general
way, caught only by a real multi-node `ProdEnv` test.** Design 1 seeded a
bare driver-local "now" timestamp the first tick a tablet was ever seen
with nonzero bytes. This is wrong for a split child: the backlog it
inherits is *physically* whatever its parent hadn't sealed yet (the
shared engine only narrows the declared range at split time; it never
touches the records), so seeding "now" silently forgets how old that
inherited backlog already was — understating its age, and, since ADR
0034's auto-split runs on its own fixed interval independent of sealing,
*compounding* across a cascade of splits (each further child restarts its
own clock from "now" too). Design 2 tried to patch this by having a new
child inherit its parent tablet's own memoized basis from the same
driver-local map — which is *also* wrong, more subtly: the fallback map
is per-node, in-memory state, but which node leads a split's child is a
**placement** decision, completely independent of which node led the
parent. In a real cluster a child is routinely led by a different node
than its parent, and that node's own map has never even heard of the
parent tablet — the "inherit from the map" lookup silently misses and
falls back to "now" anyway, reproducing design 1's bug exactly whenever
placement happens to split leadership across nodes. Both designs passed
every single-node unit test in the sealer-tests matrix (which structurally
cannot exercise cross-node placement) and both went red/flaky under
`streams_e2e.rs::manual_split_with_unsealed_backlog_under_production_
seal_knobs` — a real 3-node cluster test that pre-dated this fork and
was not intentionally being touched by it, first deterministically (same
node, wrong basis) then intermittently (~50%, once fixed for the same-node
case but still wrong cross-node). **The actual fix**: a *one-time* real
`pending_changes()` scan of the true oldest pending record's own HLC, run
only the first tick a tablet is ever observed with a nonzero, never-sealed
backlog, memoized from then on. This reads the real data — correct and
identical regardless of which node leads which tablet — while still
eliminating the overwhelming majority of the original cost: once per
tablet's entire lifetime (between "created" and "its first seal ever
commits") instead of once per tick forever.

**General rule 1**: when a periodic trigger's "should I act" evaluation and
its "what should I act on" data-gathering are the same function call,
check whether the gate can be answered from something already computed
for a *different*, cheaper trigger sharing the same tick — if so,
restructure so the expensive gather only runs inside the branch that
already decided to act, and prove it by construction (make the expensive
call unreachable from the idle path, not merely "unlikely" to be reached).

**General rule 2, the more important one**: *a value that must remain
correct across a leadership or placement change cannot be reconstructed
from purely driver-local, per-node, in-memory state* — not even by having
the new owner "ask the old owner's own local state," since the new owner
is a different process that may never have run on the same node as the
old one at all. If the true value isn't cheaply available from replicated
state, either accept a bounded, one-time real read to establish it
correctly (as done here — "run the expensive path once, memoize, never
again" is a very different cost profile than "run it every tick forever"
and is often an acceptable trade against a from-scratch replicated-field
design), or make the imprecision's failure mode explicit and *safe* (here,
it would have needed to bias toward firing *too early*, never too late —
a plain "now" timestamp biases the wrong direction, toward firing *late*,
which is what actually broke the test). A single-node or single-process
test harness cannot catch this class of bug at all; it needs a real
multi-node integration test exercising actual placement, which is exactly
why `manual_split_with_unsealed_backlog_under_production_seal_knobs`
(pre-existing, not written for this fork) caught what the fork's own new
unit tests could not.

See `crates/animusd/src/index_drain.rs`'s `seal_tick` doc and ADR 0043
§A3's own amendment for the full design, and `Metric::StreamSealBacklogMs`'s
doc for the accepted metric-semantics change this required (level metrics
that reuse a trigger's own working data inherit that trigger's own
precision trade-offs — document the change on the metric itself, not just
in the code that produces it).
