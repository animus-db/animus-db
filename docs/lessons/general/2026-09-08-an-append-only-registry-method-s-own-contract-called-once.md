# An append-only registry method's own contract ("called once per node's lifetime") silently breaks once a fixture reuses the same handle across a restart — a fix that calls it again just hides the stale entry behind a fresh one (ADR 0061 rung F, C-06 PR 4, 2026-09-08)

`ClusterEdgeState::register_control` (`crates/animusd/src/lib.rs`) has
always meant exactly what its own doc says: "called once per node" — true
in production, where a restarted process always assembles a brand-new
`ClusterEdgeState` from scratch, so there is never a second call to make.
`SimCluster::restart` (ADR 0061 rung D4 PR 1's design) deliberately
reuses the SAME `Arc<ClusterEdgeState>` across a restart, so that a
restarted node's other registrations (hosted CP groups, etc.) survive the
swap — a sound design choice on its own, but one that quietly invalidates
`register_control`'s "once per lifetime" assumption the moment `restart`
also needs to update the control handle.

The first fix attempt (investigating `dynamowire_stop_restart_s02`, C-06
PR 4) added a `ctx.edge.register_control(fresh_control.clone())` call to
`restart` — the obviously-analogous call `SimCluster::new` already makes
for a node's *first* control handle. It compiled, and the scenario kept
failing with the byte-identical error, at the byte-identical seed, after
a full rebuild. The instinct at that point is to suspect the fix wasn't
applied, or that the bug is somewhere else entirely — but `register_control`
only ever `push`es onto an internal `Vec`, never replaces; calling it a
second time on the same `ClusterEdgeState` left the registry holding BOTH
the OLD, `Simulator::stop`ped (dead, frozen at whatever leadership belief
it held the instant it stopped) handle and the fresh one, and
`leader_handle()`'s `.find(|r| r.is_leader())` could — and did — return
the stale one first. The append silently reproduced the exact bug it was
meant to fix, just one layer further in.

**The generalizable lesson**: before reusing a "register once" method for
a "replace" need, check whether it actually replaces or only appends —
grep its own body, not just its doc comment (the doc comment here was
accurate, "called once," but "once" reads as a *description* of every
existing caller, not a *promise* that a second call is safe or a no-op).
An append-only registry that a `find`/first-match reader later consults is
a specific, easy-to-miss hazard: the stale entry doesn't error, doesn't
warn, and doesn't get pushed out — it just silently wins the race some
fraction of the time (here, always, since the stale entry's own
`is_leader()` belief happened to still read as `true`). The fix is a
distinct method with its own contract (`replace_control`: clear, then
push) rather than a second call to the "once" one — and the new method is
`#[cfg(test)]`-only, named and documented as `SimCluster::restart`'s own
need, so a future reader doesn't mistake it for a second, interchangeable
way to do what `register_control` already does.

**How this was actually caught, not just reasoned about after the fact**:
a full corpus re-run at the SAME pinned seed after the first "fix" landed
was still red, with an unchanged failure string — the same discipline the
issue #731 entry above names ("verify the NEW failure text, not just the
absence of the old one"), applied one step earlier: here there was no new
failure text at all, which was itself the tell that the first fix hadn't
changed anything observable, and worth tracing the actual registry state
rather than trusting that "I added the analogous call" was sufficient.
