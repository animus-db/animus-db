# A test's own single-shot assertion right after a fresh process bring-up can race an event-driven background reconciler, even when the mechanism under test is fully correct — poll to convergence, not just "the write landed before the crash" (C-05 PR 2, `shared_wal_e2e.rs`)

The real-`ProdEnv` shared-WAL end-to-end test restarts a node on the same
data directory/addresses and immediately re-reads both tables' data
through a fresh `GetItem`. An early draft asserted this with a single,
immediate `assert_eq!` right after the restarted node's bring-up returned
successfully, and it flaked — not because the shared WAL lost data (the
values were genuinely durable and recoverable), but because the
tablet-host reconciler re-hosts each table's tablet **asynchronously**,
on its own event-driven cadence with a fallback poll interval, after a
process restart; a request landing before that settles can legitimately
route to a not-yet-hosted group and read back `None` or time out
internally, independent of anything shared-WAL-specific. This is exactly
the class of bug the root `CLAUDE.md`'s "Eventual properties get a
converged-or-timeout poll, never a fixed-deadline one-shot assert" rule
exists to prevent — and it was reproduced here in freshly written test
code, not inherited from an older test, which is worth naming plainly:
following house discipline is not optional just because the code is new.

Separately, the very first bring-up (before any restart) also needs the
established "port-TOCTOU bring-up retry" idiom (retry the *whole*
fresh-port-allocation-plus-start as a unit against a wall-clock deadline)
— a freshly `free_addrs`-allocated port can still lose a bind race under
`cargo test`-level contention, and a single-attempt bring-up is a
documented flake class in this codebase, not a "this test is flaky" one.

**Fix**: added a `poll_get_item_consistent` helper (bounded convergence
poll, 50ms retry interval, a genuine budget-exceeded failure still fails
the test — this is about tolerating the reconciler's own catch-up window,
never about tolerating real data loss) for every post-restart read, and a
`bring_up` helper mirroring `tests/support::bring_up_deadline`'s shape for
the initial bring-up. Verified 0/25 failures after both fixes (25
consecutive `--nocapture` runs), confirming the flake was a genuine
test-harness race against an asynchronous reconciler, not a product bug.

**General form**: any test that restarts a real node and then reads
through the wire immediately afterward must poll to convergence, even
when the write itself long predates the restart and the mechanism being
tested is otherwise proven correct — a fresh process's own background
reconcilers (tablet hosting, membership catch-up, cache warm-up) are
themselves eventual properties, and a single-shot read racing them is a
harness bug that looks exactly like a product bug until traced.
