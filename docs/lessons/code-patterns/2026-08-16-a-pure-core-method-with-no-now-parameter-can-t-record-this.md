# A pure core method with no `now` parameter can't record "this happened at time X" itself — give the driver a companion method to call, don't widen the core method's signature.

**A pure core method with no `now` parameter can't record "this happened at
time X" itself — give the driver a companion method to call, don't widen
the core method's signature.** `RaftCore::propose`/`change_membership` take
no `Nanos` (proposing/reconfiguring never needed wall-clock time before ADR
0044 phase-1 PR3's `last_activity` idle-clock), and both are called from
dozens of test files across two crates plus every production driver — so
adding a `now: Nanos` parameter to either to let them bump
`last_activity`/clear `quiesced` inline would have rippled through all of
them for one new feature's benefit. Instead, `RaftCore::note_local_activity
(now: Nanos)` is a tiny, separate, `now`-taking method the *driver* (which
already has `now` at every call site that matters) calls immediately after
confirming `ProposeResult::Accepted`, inside the same held `core` lock —
`become_leader`/`transfer_leadership` do the equivalent inline since they
already take `now`. **General rule**: when a new feature needs a
time-stamped side effect from an existing widely-called pure method that
doesn't carry the needed input, don't widen that method's signature for
every caller — add a narrow companion method the *caller* invokes with the
input it already has, at the one or two call sites that actually need the
new behavior. (2026-08-16, `quiesce/3-core-state-machine`.)
