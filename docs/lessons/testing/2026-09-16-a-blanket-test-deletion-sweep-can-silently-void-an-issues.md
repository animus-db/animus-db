# A blanket "delete every test pinned to X" sweep can silently void an open issue's own gating mechanism, leaving it permanently unfalsifiable (issue #298)

Issue #298 named `tests/streams_e2e.rs::multi_split_soak_streamed_gsi_
table_under_mixed_load` as its reproduction vehicle — ADR 0058's G5 open
fork explicitly used a **mandated 30-consecutive-clean-run un-pinned soak**
against this exact test as the only thing allowed to move `SplitMode`'s
soak pin from `Copy` to `InPlace`, and stated plainly that deleting the
copy-based split workflow "cannot ship until #298 is either fixed or this
soak's own budget is deliberately re-tuned." That bar was never met: the
last recorded attempt (ADR 0018's 2026-08-29 amendment) explicitly says "no
fresh 30-run un-pinned soak was run this round... only that soak can move
the pin." The deletion nonetheless shipped as a G5 gate pass (ADR 0058's
2026-09-01 note), and its Layer A was "delete every
copy-split-pinned test" — and the soak, being pinned to `Copy`, was swept
away by that blanket deletion along with it. The result: by the time this
issue was picked up again, the one test that could prove or disprove its
own closure no longer existed (confirmed by `--list`-ing the current test
binary against `streams_e2e.rs`'s own current tests, and by the file's own
module doc, which no longer mentions any such soak). Every *specific*
mechanism a series of investigations found was fixed and is present on
`main` (shape A/B, "deep shape A", the `ResolveOutcome` outcome-channel
gap, the #737 recovery-grace clock bug) — but the *full acceptance
scenario* (120+ writes, mixed load, a real GSI, a cascading auto-split to
dozens of tablets) that the mandated soak existed to exercise has not run
clean even once since `InPlace` became the sole split mechanism, because
nothing that shape exists to run.

**The generalizable lesson**: when a test is the explicit, named gate for
unblocking or closing out a piece of work ("un-pin only after N clean
runs of test X", "closes only once test Y passes"), a later, unrelated
refactor that deletes tests by a blanket predicate (e.g. "every test pinned
to the old mode/flag/workflow") must check whether any of the tests it is
about to delete are themselves load-bearing gates for a *different*, still-
open piece of work — and if so, either port the test to the new shape
first or explicitly re-open/flag the gated issue as now unverifiable,
rather than let the deletion quietly discharge the gate by removing its
own mechanism. An issue that cites a specific test by name should be
re-checked for that test's continued existence before any investigation
effort is spent trying to run it — `grep`/`--list` the current test binary
first, don't assume a heavily-cross-referenced-in-ADRs test name still
resolves to a real `#[tokio::test]`.
