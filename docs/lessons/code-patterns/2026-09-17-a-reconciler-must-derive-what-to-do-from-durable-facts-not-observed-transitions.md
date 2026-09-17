# A reconciler must derive "what to do" from durable local facts plus the current view, never from having *observed* a transient intermediate view (issue #987 follow-up)

`animus-cp-data`'s per-node tablet-host reconciler (`host::plan`) gates its
in-place-split materialization on `TabletFacts::pending_split` — a durable,
permanent-once-true fact about this replica's own Raft log
(`RaftKvNode::pending_split()`, backed by an engine marker a tablet writes
at most once, at its own fork). Yet `gather_facts` used to skip calling
`pending_split()` at all unless *this tick's* `MetadataView` still showed
the parent's `inplace_split` intent — and `plan`'s own materialize branch
only ever iterated `view.tablets` looking for that same intent. Both reads
were implicitly assuming the reconciler would get at least one tick while
the metadata view was in the transient `Splitting` state, in between the
fork committing and the control plane's `CutoverSplit` retiring the parent
and publishing both children as ordinary `Active` entries.

That assumption is false by construction, not merely under contention. ADR
0003's whole reconciler design is **event-driven**: a `metadata_watch` wake
coalesces every metadata change since the reconciler's last tick into ONE
fresh snapshot — a replica that is busy, descheduled, or simply ticks on
its periodic fallback cadence can jump straight from "parent still
Splitting" to "parent retired, children Active" with no tick ever landing
on the state in between. ADR 0058's own rung-3 fix already anticipated a
version of this (a too-eager `CutoverSplit` racing a slow materializer) and
narrowed the window with a fast reconciler cadence plus a settle delay
before proposing `CutoverSplit` — but narrowing a window is not closing it,
and the true fix was available all along: `pending_split()` is a *fact
about this replica*, independent of the view, and checking it
unconditionally costs nothing extra once forked (cache the durable
Some-forever answer) and very little before (one cheap point read of a
marker key that mostly doesn't exist yet).

**The general form**: when a reconciler's plan-for-this-tick logic branches
on "does the CURRENT view show condition X," ask separately: is X itself a
one-way, permanent transition this replica could have durably recorded
locally, at the moment it happened, independent of the view? If so, gate on
the durable local fact, and treat the view only as the source of the OTHER
inputs the decision needs (here: the affected child ids' ranges, read from
whichever of "the parent's own range" or "the children's own already-
published ranges" the current view happens to still carry — both are
proven bit-identical by construction, so either is a valid source). A
reconciler that instead requires having *observed* a transient
intermediate view is one coalesced wake away from silently taking the
wrong, permanent action — hosting a fresh empty engine instead of cloning
a live one, in this case — with no retry ever correcting it, because by the
next tick the view has already moved past the point where the branch could
even be reached again.

**A second, narrower lesson from the same incident, for anyone tempted to
"fix" this by widening a timing bound instead**: an inline wait/settle
delay that used to mask a race (ADR 0058's own `RECLAIM_STOP_TIMEOUT`-style
teardown-timing fix is the sibling example already in this log) can hide a
structural gap for months, because the bound only has to be wide enough for
the seeds and hardware the corpus happens to run on. This exact defect hid
behind a 10-second inline teardown wait before issue #987 shrank the
window to sub-second — the bug was always there, just statistically
unreachable until the window got tight enough for an ordinary coalesced
wake to skip it entirely. When a timing bound is the only thing standing
between "usually fine" and silent data loss, treat that as a standing
invitation to find the structural fix, not a signal the bound is already
tight enough.
