# An uncaptured flake report can already be fixed by a later-numbered issue against the same test — check the test's own history before budgeting a reproduction hunt on it (issue #418).

**An uncaptured flake report can already be fixed by a later-numbered issue
against the same test — check the test's own history before budgeting a
reproduction hunt on it (issue #418).** Issue #418
(`dynamo_index_scan::gsi_scan_paginates_and_drains_all_rows`, filed
2026-08-27 with no captured panic text) described exactly the
"converged-or-timeout poll on one replica doesn't prove the group
converged" mechanism that issue #559 (filed 2026-09-02, closed
2026-09-07) later diagnosed and fixed with captured panic text, on the
same test, in the same file. The fix (`await_gsi_scan_everywhere`,
converging on every node before pagination) had already landed on `main`
by the time #418 was investigated — but nothing had closed #418 itself,
and a per-crate guide table still listed the file as "frozen behind open
flake issue #418" *after* the fix merged, because that note only tracked
the GitHub issue's open/closed state, not whether the code still had the
bug. 40 runs (20 unloaded, 20 under `taskset`-pinned CPU load) found
nothing, consistent with the fix already covering it.

**Before spending a reproduction budget on a numberless flake report,
`git log`/`git blame` the failing test's own doc comments and check
`docs/lessons/testing/` for the test name** — a fix for the same mechanism
under a different, later issue number is a completely ordinary outcome
(flakes get rediscovered with better diagnostics before the first report
is closed) and the evidence is usually already sitting in the file's own
comments explaining *why* a helper is shaped the way it is.

**To confirm a suspected-already-fixed race without waiting on timing
luck**: temporarily revert the fix in the working tree (e.g. swap the
multi-replica convergence call back to the single-replica one it
replaced), rebuild, and run the *reverted* version under the same load
harness. Reproducing the failure on the reverted version (here: 5/40 under
CPU-pinned load, same panic shape as #559's captured log) is strong
evidence the current, unreverted code is what closes the gap — stronger
than a clean run alone, which only shows absence, not why. `git checkout
--` the file immediately after to discard the scratch revert; never leave
it in the diff.
