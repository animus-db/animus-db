# Before designing new movement machinery for "get this thing onto a different set of nodes," check whether a declared-state convergence loop already provides it (ADR 0062's own central finding, generalized)

ADR 0062's whole design reduces to one recognition, worth stating outside
its own split-specific frame because it recurs: `reconfigure_step` (ADR
0058 Train 1's learner-phased membership-change sequencing) plus its
production caller (`host::Reconciler::execute`'s `HostAction::Reconfigure`
arm) already converges a tablet's *live* Raft membership toward whatever
*declared* target `Metadata.tablets[t].replicas` names — with no
availability dip, on every tick, for every tablet, unconditionally. That
is a **general-purpose "move this to there" primitive**, keyed off nothing
more than a `CasTabletReplicas` write. ADR 0029's `rebalance_placement`
already rides it for load-balancing; ADR 0062 recognized that a split
child's post-fork relocation is not a new kind of problem needing its own
bespoke fused-with-the-fork mechanism (ADR 0058 Train 2 Stage 1/2's own
now-deleted learner-recruitment machinery) — it is the *same* problem,
solved by computing one more kind of target and handing it to the exact
same convergence loop.

**General lesson**: before building new machinery to move, relocate, or
converge some piece of state onto a different set of nodes/replicas/
homes, ask whether an existing declared-state convergence loop in this
codebase already does exactly that generic job — search for the pattern
"a driver reads a `Metadata`-declared target every tick and proposes a CAS
that nudges live state toward it," not just for a mechanism with a
matching name. When one exists, the design work shrinks to "compute the
right target, once, from already-agreed state" (this codebase's own
repeated `BeginBackup`/`CutoverSplit`-at-apply-time discipline) plus
"feed it into the loop" — not a parallel bespoke mover with its own
readiness gates, its own over-provisioning accounting, and its own
partial-progress-inheritance edge cases, all of which a fused
design pays for even when the underlying convergence primitive was
sitting there unused the whole time.
