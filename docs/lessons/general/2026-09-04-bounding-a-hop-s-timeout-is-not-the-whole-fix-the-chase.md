# Bounding a hop's timeout is not the whole fix — the chase must also be able to return to a candidate that only timed out, not just avoid starving on it (issue #585, continued)

The fix above (bounding each hop to `FORWARD_HOP_TIMEOUT`) closed the
starvation hole but broke the property it was supposed to leave intact: a
genuinely slow-but-**live** leader must still be waited out within
`CLIENT_TIMEOUT`, the way the pre-#585 code (which handed a candidate the
whole remaining budget) actually managed to do for exactly this case. The
bug: `relay_request_with_timeout` folded "confirmed dead within budget"
and "ran out of time, no answer either way" into the same
`RELAY_TRANSPORT_FAILURE` sentinel, and `forward_to_tablet_leader`'s
`tried` set treats both identically — permanently excluded, including as
the target of another replica's own hint. Under a real membership-change
storm, a group's actual leader can legitimately take several seconds to
commit (well past one `FORWARD_HOP_TIMEOUT`, comfortably inside
`CLIENT_TIMEOUT`). The first hop to that leader times out and marks it
`tried`; every OTHER replica's own refusal then names that same address as
its hint, filtered out every time by `!tried.contains(a)` — so the chase
burns a full round through every other known replica before
`ForwardRetryStep::WaitElection`'s `tried.clear()` even lets it circle
back, routinely enough to blow the whole `CLIENT_TIMEOUT` on a leader that
would have answered in a few seconds. Found via CI (not this repo's own
sandbox): `tests/split_placing_two_replica_diff_e2e.rs` failed with a
`put failed: Error("relay to peer node failed")` during a real ~3s
leaderless window in a 5-voter placing reconfiguration, immediately after
the original #585 fix landed.

**The general lesson: "bound each candidate's own timeout" and "the chase
can still find every live candidate" are two separate properties, and a
fix for the first can silently break the second if a timed-out candidate
is treated identically to a confirmed-dead one.** A hint-chasing retry
loop needs to track *why* a candidate is currently excluded, not just
*that* it is — confirmed dead (safe to exclude for the rest of the pass)
and "ran out of time, might still be alive" (must remain eligible,
especially if some other candidate's own answer names it back) are
different facts requiring different treatment, even though both currently
manifest as "the last attempt failed."

**Fix, and how it stays testable.** Split the single sentinel into two —
`RELAY_TRANSPORT_FAILURE` (a fast, confirmed failure, `Ok(None)` from the
inner `tokio::time::timeout` future) and `RELAY_HOP_TIMEOUT` (the whole
attempt outliving its budget with nothing learned, `Err(_)` /
`Elapsed`) — right at the one function that already discriminates them by
construction. Track hop-timed-out addresses in a second set (`timed_out`)
alongside `tried`, and make the actual candidate-selection decision a
single pure, unit-tested function (`decide::resolve_forward_candidate`,
`animus-node`, ADR 0061 A6): a hint naming a `timed_out` address wins
immediately (even though `tried` also contains it — the override is
explicit and localized to one function), a fresh untried replica is
preferred over retrying a `timed_out` one (so a live-but-quiet candidate
is never starved behind a slow one), and only once every fresh option is
exhausted does the chase fall back to retrying a `timed_out` candidate
directly. Six direct unit tests on this function (no sockets) cover the
exact three-way distinction the task named up front: timeout vs. refusal
vs. a hint naming a timed-out node.

**Testing note: most reproduction attempts of the *real* end-to-end
failure in a loaded sandbox surface a completely different, unrelated
cause — read the actual error before concluding the fix regressed.**
Trying to reproduce `split_placing_two_replica_diff_e2e`'s specific
failure by looping the test tens of times in a resource-constrained
sandbox (4 vCPUs, another agent building concurrently, disk near full)
turned up a real, if lower, residual failure rate — but inspecting each
failure's actual panic message mattered enormously: `"Too many open
files"` (a real per-process/system fd exhaustion from dozens of rapid
cluster bring-up/teardown cycles back to back), `"Connection refused"`/
`"Connection reset by peer"` on a plain admin HTTP connect (a listener not
yet accepting, or a reset under load — nothing to do with the forward
chase), and — once — the original `"relay to peer node failed"` message
paired with the test's own *separate*, already-partially-independent
"missed the transient five-voter intermediate" assertion (a
polling-window observation gap, not obviously the same mechanism this fix
touches). Only one bucket of these (a hint-chasing timeout regression)
is what this fix addresses; the other two are sandbox capacity limits any
heavy multi-process integration test in this crate is exposed to when run
in a tight back-to-back loop, matching the root `CLAUDE.md`'s "a flaky
`ProdEnv` test is a real bug — debug it, don't bump the timeout" advice
with an important addendum: debugging starts with reading what actually
failed, not assuming every red run under load is the same red run. A
deterministic, isolated regression (`forward_hop_timeout_tests::
forward_to_tablet_leader_waits_out_a_slow_but_live_leader_instead_of_
giving_up`, real sockets, real stubs, no cluster bring-up race at all) is
what actually proves the mechanism fixed, exactly as `forward_transport_
failure_tests`/the sibling #585 test already established as the pattern
for this file — the flaky multi-node e2e test is corroborating evidence at
best, never the primary proof, for a mechanism this hard to isolate
deterministically at full cluster scale.

**A secondary testing lesson from designing that isolated regression**: a
version of it with the "other" replicas answering their refusal
*instantly* passes even against the reverted, pre-this-fix candidate
logic — `ForwardRetryStep::WaitElection`'s pre-existing backoff-and-clear
already self-heals a fully-exhausted pass, and when every non-first-guess
hop is instant, that self-heal costs only `FORWARD_ELECTION_BACKOFF`
(100ms), nowhere near enough to distinguish "fixed" from "pre-fix." Giving
the other stubs a real, deliberate delay close to (but under)
`FORWARD_HOP_TIMEOUT` — modeling a busy cluster where *every* hop costs
real time, not just the stalled one — is what makes a full extra round
through every known replica genuinely expensive under the reverted logic
while the fixed logic still resolves in one stalled hop plus one delayed
hint. The general form: when a test's own assertion is a timing threshold
meant to separate "recovered quickly" from "recovered the slow way," every
synthetic stand-in in the fixture needs to cost *real* time in the same
proportion the production failure mode would — an all-instant fixture can
accidentally exercise a *different*, cheaper self-healing path that both
the buggy and fixed code share, proving nothing about the specific
difference under test.
- **Repurposing a private parser across two features must re-audit its
  error text, not just its logic** (2026-09-04, issue #375/W-01's PR3,
  nested-path `UpdateExpression` targets). `parse_projection_path`/
  `parse_projection_segment`/`parse_index_chain` (`animus-dynamo/src/
  wire.rs`) already implemented the exact document-path grammar
  (`a.b[0]`, `#alias`, list indices) `UpdateExpression`'s new nested `SET`/
  `REMOVE`/`ADD`/`DELETE` targets needed — reusing it verbatim (rather than
  a second hand-rolled path parser) was the right call and cost nothing in
  logic. What it *did* cost, and would have shipped silently wrong without a
  second look: every one of that parser's error messages said "projection
  path" by name (`"malformed list-index syntax in projection path
  `{raw}`"`, `"projection uses name placeholder ..."`), because it was
  written when `ProjectionExpression` was its only caller. Left unchanged, a
  malformed `UpdateExpression` path (`SET a[ = :v`) would have reported a
  `ValidationException` blaming "projection path" — correct code, actively
  misleading diagnostic, on a request that has no `ProjectionExpression` in
  it at all. Fixed by generalizing the wording to "document path" (the name
  both features' grammar actually describes) before wiring the second
  caller through. General form: when a private helper built for feature A
  gains a second caller (feature B), grep its own error/log strings for
  feature A's name — a reused *helper* is usually safe to share verbatim,
  but a reused helper's *messages* silently keep pointing at whichever
  feature was there first unless someone explicitly generalizes them, and
  nothing type-checks that omission.
