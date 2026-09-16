# An executor whose own task queue lives inside the state its tasks hold an `Arc` back to is a reference cycle no external `Drop` can ever observe — and it was the real cause of the `sim_cluster_*` tier's per-test RSS growth, not glibc allocator retention (2026-09-08)

**Symptom**: `cargo test -p animusd --lib -- --test-threads=1` climbed
monotonically in resident memory — flat ~60 MB through the non-sim tests,
then ~10 MB/s of growth the instant the `sim_cluster_*` modules started,
2.1 GB by `sim_cluster_auto_split`, 8.4 GB entering `sim_cluster_dynamo_
partiql`, 12.7 GB after 307 tests — with **no drop between tests**. A
two-thread run of the same suite reached 13.9 GB and page-fault-thrashed
for 87 minutes. `sim_cluster_dynamo_corpus.rs`'s own module doc (see this
crate's own `CLAUDE.md`, "SimCluster coverage for Transact ops") had
already noticed the shape at `ANIMUS_DYNAMO_WIRE_SEEDS=12`/`=25` and filed
it as "consistent with ordinary glibc allocator high-water-mark behavior
(freed memory not returned to the OS) rather than confirmed proof of a
true per-scenario leak" — a reasonable guess given the tooling available
at the time, and wrong: this **was** a true per-scenario leak, one whole
simulated cluster's worth of memory abandoned per test, for the rest of
the process's life.

**Root cause, proven with a `Weak`, not assumed**: `animus_sim::Simulator`
holds one `Arc<Shared>` (`Shared` wraps the whole `SimState` — tasks,
timeline, inboxes, disks, trace); every `Simulator::env(id)`/`SimEnv`
handle clones that same `Arc`. `SimState.tasks: BTreeMap<TaskId,
Option<BoxFuture<'static, ()>>>` is what `Spawner::spawn` inserts a task's
future into — and it lives **inside** `SimState`, i.e. inside the very
`Shared` every `SimEnv`/`Simulator` handle holds a strong reference to. A
task that never resolves on its own (a Raft heartbeat loop, a reconciler
tick loop, `auto_split_loop`, the backup janitor — every one of them a
`loop { .. env.sleep(..).await .. }` with no terminating condition, and
`SimCluster::new` spawns several per node) almost always captures a
`SimEnv` (sometimes a whole `Simulator` clone, per this crate's own
"`Simulator` is `Clone`" design) to do its job. So the chain is:
`Arc<Shared>` → `SimState.tasks` → a perpetual task's `BoxFuture` →
captures `SimEnv{shared: Arc<Shared>}` → the **same** `Arc<Shared>`. A
genuine strong reference cycle, entirely internal to one crate's own
executor state — no external code needs to hold anything for it to leak.

Proved directly (`crates/animus-sim/tests/executor_leak.rs`), not
inferred from RSS (RSS assertions are inherently flaky — this repo's own
standing rule): spawn one perpetual task, drive it briefly, take a `Weak`
handle (`Simulator::downgrade`) **while everything is still alive**, then
drop every external `Simulator`/`SimEnv` handle the test holds, and assert
the `Weak` still upgrades. It does — proving the cycle, not merely
asserting memory didn't shrink. A sibling test proves the fix breaks it:
call `Simulator::shutdown()` (new — drains `SimState.tasks`/`task_owner`,
the *only* fields that can hold a strong `Arc<Shared>` back-reference; every
other field — `timeline`, `timer_wakers`/`recv_wakers` via `Waker`'s own
`Arc<TaskWaker>` — already uses `Weak` or holds no `Arc` at all, confirmed
by reading every field, not assumed) before dropping the external handles,
and the same `Weak` no longer upgrades.

**Why `Simulator`'s own `Drop` can't fix this on its own, and why a
per-fixture `Drop` is the right root-level fix anyway**: `Simulator` is
deliberately `Clone` (many call sites hand a clone into a spawned driver
task specifically so it can call `&self` fault-injection methods from
*inside* an async scenario script) — no single clone's own `Drop` can
know whether it's the "last" one, and `Drop` for `Shared` itself can never
run at all while the cycle exists (a type's `Drop` only fires once its
strong count reaches zero, and this cycle is exactly what prevents that).
`Simulator::shutdown()` sidesteps this by not relying on refcounting at
all — it clears the task map directly, from any handle, at any time,
idempotently. What still needs to *call* it is whatever code in a
consuming crate owns the "this scenario is over" moment; `animusd`'s
`SimCluster` (not `Clone`, and the one type every `sim_cluster_*` test
already owns for its whole duration) gets an `impl Drop for SimCluster {
fn drop(&mut self) { self.sim.shutdown(); } }` for exactly this reason —
zero changes needed to any of the ~30 `sim_cluster_*` sibling modules,
since every one of them already lets its `SimCluster` value go out of
scope, success or `panic!` (unwind) alike, at the end of each `#[test]` fn.

**Measured effect** (foreground `/proc/<pid>/status` `VmRSS` sampling of
the animusd test binary itself, never a background process): `cargo test
-p animusd --lib sim_cluster_dynamo_partiql -- --test-threads=1` — peak
322 MB across the module's 10 tests, no per-test growth (the pre-fix
trajectory, measured on the 64-test PR 6 version of the same module, had
climbed to ~3.7 GB at ~58 MB per test); `cargo test -p animusd --lib --
--test-threads=2` — the whole 383-test suite completes in 643s (was
13.9 GB and thrashing without ever finishing, at one thread or two);
`ANIMUS_DYNAMO_WIRE_SEEDS=4 cargo test -p animusd --lib
sim_cluster_dynamo_corpus -- --test-threads=1` — 3 passed in 454s. This
closes the "resource-scale finding" `sim_cluster_dynamo_corpus.rs`'s own
doc filed — see that crate's `CLAUDE.md`, that same section, and
`docs/adr/0061-testability-node-crate-simulator.md`'s matching amendment
for the correction.

**General lesson**: an executor design where the task queue lives inside
the same shared state a spawned task's own environment handle points
back to is a latent reference cycle the moment any task can run forever —
which is the *normal* shape for a distributed system's own background
loops (heartbeats, reconcilers, janitors never terminate by design). RSS
climbing monotonically with no drop between otherwise-independent test
cases, specifically once a fixture that spawns perpetual loops enters
the suite, is the fingerprint — don't reach for "glibc doesn't return
freed pages to the OS" as the explanation until a `Weak` taken before the
suspected drop point still upgrades afterward; that one assertion
distinguishes a true leak from ordinary allocator retention in a way no
RSS number by itself can. When you find one, look for the executor's own
`Drop` story first — if the type holding the cycle is deliberately
`Clone`/multi-handle (so its own `Drop` structurally can't detect "last
owner"), the fix is an explicit, idempotent `shutdown()`-style drain
called from whatever single-owner type sits one layer up in every
consumer (a test fixture's own struct, here), not a change to the
`Clone`-able handle's own lifecycle.
