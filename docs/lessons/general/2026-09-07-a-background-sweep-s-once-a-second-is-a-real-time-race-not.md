# A background sweep's "once a second" is a real-time race, not an isolation guarantee — an in-crate `#[cfg(test)] mod` fixture needs a way to disable it (2026-09-07, issue #734)

`issue_298_conflict_tests::a_fresh_stage_pushes_a_decided_blockers_
resolution_instead_of_conflicting` (`crates/animusd/src/lib.rs`) failed once
in CI (`gates` job, `--test-threads=2`) with `expected IntentBlocked on A's
still-live intent, got Staged` — a single failure out of many green runs.
The test deliberately leaves transaction A's decided-but-unresolved intent
live on a key, then makes B's own single, direct `txn_prepare` attempt on
that key and asserts it observes `StageOutcome::IntentBlocked` — the setup
for proving `ClientCtx::push_resolution_if_decided` clears it. The test's own
comment already named the risk ("no dependency on `txn_resolver_loop`'s
independent per-second passive sweep also being capable of clearing this
given enough wall-clock time") but never actually defended against it: the
node this test brings up via `single_node()`/`run_node()` spawns
`txn_resolver_loop` like every other background loop, and that loop sweeps
**every** decided-but-unresolved anchor **unconditionally**, once a second,
for as long as it runs — there is no `RECOVERY_GRACE` delay gating the
resolve itself (only the loop's own *stuck-warning* logging is grace-gated).
Under ordinary fast execution the whole A/B dance finishes in well under a
second, so the sweep never gets a chance; under real CI/sandbox CPU
contention, the elapsed wall-clock time between A's decide and B's probe can
exceed a second, and the sweep can legitimately win the race and resolve A's
intent first — at which point B's *first* stage attempt correctly observes
`Staged`, not `IntentBlocked`, because there was nothing left to block it.
This is not a production bug: either path (an explicit push, or the
background sweep) upholds the actual safety property (no spurious
`TransactionConflict`) — it is a test whose own claimed isolation
("called directly and in isolation... nothing here can be coincidentally
saved by `txn_resolver_loop`'s own background sweep") was aspirational, not
structurally true. **Root-caused concretely, not just reasoned about**: a
temporary instrumented run (a no-op background-loop-disable plus an
inserted `sleep(1300ms)` between A's decide and B's probe, both reverted
before commit) reproduced the exact CI panic message deterministically —
confirmed the mechanism before touching anything permanent.

**Fix**: `Node::abort_background_tasks_for_test()` (`#[cfg(test)]`,
`crates/animusd/src/lib.rs`) aborts every task in `Node.tasks` — every
background maintenance loop (`txn_resolver_loop` included) plus the client/
dynamo/admin/console listeners — while leaving every hosted CP group and
this node's own `ProdEnv`s fully live and **un-halted** (unlike `shutdown()`/
`shutdown_and_wait()`, it does NOT call `halt_hosted_cp_groups()`). Safe to
call from any test that talks to the node purely through `ctx_for_test()`'s
in-process `ClientCtx` (never a socket) once the tablet it needs is already
hosted/leader-elected — the CP group's own driver task is spawned internally
by `animus-cp-data`, never a member of `Node.tasks`, so it's untouched.
Called right after `provision_and_await_leader` in this test, the isolation
claim is now an **invariant**: nothing but the test's own calls can ever
touch a transaction's intent from that point on.

**A deterministic sibling was added too, proving the same property with no
real-time dependency at all**: `crates/animusd/src/sim_cluster_txn_conflict.rs`
(`#[cfg(test)] mod sim_cluster_txn_conflict;`) drives the identical scenario
against a real `SimCluster`, which spawns no background loops whatsoever —
so there is no sweep to race in the first place, seed-reproducible and
virtual-time-driven. This needed five new small `pub(crate)` wrapper methods
on `SimCluster`/`SimClusterHandle` (`txn_prepare_pushing`/`txn_prepare_once`/
`txn_decide_anchor`/`push_resolution_if_decided`/`txn_resolve_participant`),
mirroring the existing `put`/`get`/`delete`/`scan` wrapper shape exactly
(`SimClusterHandle`'s own async method calling the real `ClientCtx` method,
`SimCluster`'s own sync wrapper driving it via `spawn_and_capture`). Verified
as a genuine, not accidental, regression test: temporarily stubbing
`push_resolution_if_decided` to a no-op (reverted before commit) made all
three new sim scenarios fail at exactly the expected assertion.

**General lesson**: a test that claims to isolate one mechanism from a
background process running in the same node must make that isolation
**structural**, not a speed bet — "this usually finishes before the sweep's
own interval" is exactly the kind of real-time race the root `CLAUDE.md`'s
"a flaky test is a bug, root-cause it" rule exists to catch, even when the
failure rate is very low and the mechanism under test is otherwise correct.
When an in-crate `#[cfg(test)] mod` fixture needs to isolate a mechanism
from a node's own background loops, disable them explicitly (a task-level
abort that leaves hosted CP groups un-halted, not a full `shutdown()`) rather
than relying on execution speed — and where the fixture supports it (a
`SimCluster`/`SimEnv` harness that never spawns those loops in the first
place), prefer adding a deterministic sibling scenario over trying to make a
real-thread test airtight against a process it cannot fully control.
