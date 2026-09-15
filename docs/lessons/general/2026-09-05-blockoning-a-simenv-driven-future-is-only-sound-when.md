# `block_on`ing a `SimEnv`-driven future is only sound when nothing inside it needs the simulator's own clock to advance (ADR 0061 rung D1 step 3, `sim_cluster_corpus`)

Building the `SimCluster` cycles/durability corpus's `delete` probe
(`run_delete_probe`, `crates/animusd/src/sim_cluster_corpus.rs`), the
first draft drove `SimClusterHandle::put`/`get`/`delete` — each an `async
fn` that internally calls `ClientCtx::cp_kind_write_raw`/`cp_get` — via a
bare `futures::executor::block_on`, mirroring a pattern that already
works elsewhere in this exact corpus family: `raftkv_linearizable.rs`'s
own `final_state` does `block_on(node.local_get(&key_bytes(key)))`
directly, with no `Simulator::run_for` anywhere near it, and that has
always been fine. The difference wasn't visible from the call site's
shape — both are `block_on(some_async_fn(..))` — and cost a fully hung
`cargo test` run (it never returned) before it was found.

The reason one is sound and the other isn't: `RaftKvNode::local_get` is a
pure local-engine read — it resolves on its very first `poll`, so
`block_on`'s busy-poll loop returns on the first iteration regardless of
whether anything is advancing `SimEnv`'s virtual clock. `ClientCtx::
cp_kind_write_raw`/`cp_get`, by contrast, internally `.await` real
`env.sleep(..)`-paced route/confirm-poll loops (`cp_route`'s own
`CLIENT_TIMEOUT` deadline, the exponential confirm-poll backoff) — a
`sleep` future only resolves when `SimEnv`'s cooperative executor
(`Simulator::run_for`/`run_until`) steps virtual time past its deadline
and wakes it. `block_on` on a plain thread never does that; it just polls
the same still-`Pending` future forever. Nothing panics, nothing errors,
nothing times out — the process simply never returns, which under `cargo
test` looks identical to "still compiling" until someone kills it.

**Fix**: drive the op the way every other synchronous (non-spawned-task)
caller in this fixture already does — `SimCluster`'s own `put`/`get`/
`delete`/`scan` (`&mut self`, spawn the future via `env.spawn_task` onto
the target node's own env, then `Simulator::run_for` a bounded budget,
then read the result back out of a shared slot — `spawn_and_capture`).
`run_delete_probe` now takes `&mut SimCluster` and calls those instead of
`SimClusterHandle` + `block_on`. A client task spawned via `env.spawn_task`
that itself `.await`s a `SimClusterHandle` op directly is a *third*, also
sound, shape — the corpus's own outer scenario runner is what drives
`Simulator::run_for` in that case, exactly once, covering every
concurrently spawned task's needs at once.

**General form**: before reaching for `futures::executor::block_on` (or
any other real-thread blocking primitive) against a `SimEnv`-backed
future, ask whether anything inside that future's own call chain
`.await`s `env.sleep()`/a network round trip/anything else gated on the
simulator's virtual clock. If yes, `block_on` alone is unsound — the
future needs either `spawn_task` + a concurrently-running `Simulator::
run_for`/`run_until` (this fixture's own `spawn_and_capture` idiom), or to
be `.await`ed from inside a future that is *itself* being driven by one.
If the answer is "no, it's a pure synchronous computation with no real
wait," `block_on` alone (as `raftkv_linearizable.rs`'s own `final_state`
already does) is fine — but that soundness rests entirely on the callee's
own implementation staying that way, so a doc comment at the call site
naming the assumption (as this fixture's own `run_delete_probe`/
`final_state` docs now do) is worth writing down rather than trusting
memory the next time either function's own internals change.
