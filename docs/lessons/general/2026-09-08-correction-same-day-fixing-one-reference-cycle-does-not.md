# Correction, same day: fixing ONE reference cycle does not mean it was the ONLY one — two independent cycles were keeping the `sim_cluster_*` tier's memory alive, and a correctly-scoped process sampler is what told them apart

The entry immediately above fixed a real cycle (`Simulator`/`SimEnv`'s own
task queue) and was believed, from a 383/384-test full-suite completion
with RSS staying flat, to have closed the `sim_cluster_*` tier's whole
leak. It hadn't — a follow-up measurement on a fresh session, sampling
correctly (see below), found the exact same linear RSS growth, at the
same rate, completely unchanged by that fix: 2.4 GB at 189s in
`sim_cluster_auto_split`, 10.8 GB at 656s, killed at 12.6 GB after 304
tests. **The apparent "~6 MB flat" success in the entry above was not a
measurement of the test binary at all** — the sampler's own `pgrep -f
'target/debug/deps/animusd-'` pattern also matches the *shell* whose
command line happens to contain that same string (the `cargo test ...`
invocation itself, echoed into the process's own argv) — and a shell
process is never resident at more than a few MB, so the reported "peak"
was reading the wrong process the entire time. **A real `animusd` test
binary is never 6 MB resident** — that number alone should have been the
tell. The fix: anchor the pattern to the actual binary's absolute path
(`pgrep -f '^/path/to/target/debug/deps/animusd-'`, the caret pinning it
to the start of the command line, which a shell's own multi-word `cargo
test -p animusd ...` invocation can never match), and sanity-check the
very first sample lands in the hundreds of MB — a real test binary's
baseline RSS with this many statics/generics linked in — before trusting
anything the sampler reports afterward.

**Root cause, found once measurement was fixed**: a *second*, entirely
separate reference cycle, this one in `animus-node`'s `SimRelayClient`
(not `animus-sim`'s `Simulator` at all). `SimRelayClient::serve(handler)`
installs `handler` into `self.handler: Arc<Mutex<Option<Arc<Handler>>>>`;
every real caller's `handler` closure is `move |req| { let ctx =
ctx.clone(); async move { .. } }`, where `ctx: ClientCtx<E, R>` itself
owns a clone of the *same* `SimRelayClient` (its own `relay: R` field,
`R = SimRelayClient<SimEnv>` under this fixture). Since `SimRelayClient`
is `Clone`-over-`Arc`, that captured `ctx.relay` shares the identical
`handler` `Arc` the closure is *installed into* — a closure sitting
inside an `Arc`'s own `Mutex` that itself holds a strong reference back to
that same `Arc`. This is a pure, self-contained cycle: it needs no help
from `Simulator`'s own task queue (the mechanism the entry above fixed)
to stay alive, and — critically — it is **entirely independent** of that
first fix, which is exactly why fixing the first cycle alone left the
leak's measured trajectory completely unchanged.

**Proved with the identical `Weak` discipline, extended**: a new
`SimRelayClient::downgrade_handler() -> WeakHandlerSlot` (mirroring
`Simulator::downgrade`'s own shape) lets a test take a `Weak` onto the
handler slot before drop and assert it no longer upgrades after —
`crates/animusd/src/sim_cluster.rs`'s
`dropping_the_cluster_frees_every_nodes_relay_and_edge_state` proves both
halves: (1) that a mid-scenario `SimCluster::restart` breaks the *old*
relay generation's cycle immediately (checked right after the restart
call, not deferred to the cluster's own eventual drop — a restart
installs a *fresh* closure on a *fresh* relay without ever clearing the
one it's replacing, so every restart in a scenario used to leak one more
whole node-generation on its own, independent of the fixture's final
`Drop`), and (2) that dropping the whole `SimCluster` frees every node's
*current* generation, including two more `Weak`-checked `Arc`s
(`ClusterEdgeState`'s own `control`/`raftkv` registries) that this closure
transitively keeps alive. Confirmed red-before/green-after by temporarily
reverting the fix and rerunning the same test: it fails deterministically
without the fix, passes with it.

**Fix**: `SimRelayClient::shutdown()` (new, `animus-node`) clears the
handler slot, breaking the cycle — called from `SimCluster::restart`
(on the OLD relay, before superseding it) and from `impl Drop for
SimCluster` (on every node's CURRENT relay, alongside the pre-existing
`Simulator::shutdown()` call from the first fix — the two calls address
two unrelated cycles and both are needed).

**Measured after both fixes, with the corrected anchored sampler**: `cargo
test -p animusd --lib sim_cluster_dynamo_ -- --test-threads=1` (the 140
dynamo-tier tests) — RSS fluctuates with individual tests (roughly
100–650 MB) but never climbs monotonically and never approaches the
2 GB bound, `140 passed; 0 failed; 1 ignored`, ~469s. The FULL `cargo
test -p animusd --lib -- --test-threads=2` suite — peak ~960 MB across
the whole run (well under the 3 GB bound), `384 passed; 0 failed; 3
ignored`, ~618s — a complete pass at every test the suite has, not a
truncated one that merely fit under a memory ceiling before something
else killed it.

**General lesson, on top of the one above**: fixing one proven reference
cycle is proof that *cycle* is fixed, never proof that it was the *only*
one keeping a symptom alive — re-measure the actual symptom (here, RSS
over the whole affected test tier) after every fix, with a sampler
whose own target-selection is itself verified sound (a `pgrep -f`
pattern against a *substring* can match the shell that's driving the
test as readily as the test binary itself; anchor it, and sanity-check
the first sample against what a real instance of the thing being
measured should actually cost). Two independent `Arc` cycles coexisting
in the same small fixture, each fully capable of explaining the entire
observed symptom on its own, is not a coincidence to be surprised by —
`SimCluster::new` installs a `.serve()` handler on every node
unconditionally, so this second cycle fired on literally every scenario
this whole tier runs, the identical "affects everything, from the first
test that touches the fixture" shape the first cycle had. A single
`Weak`-based regression, once written, generalizes cheaply to prove a
*second* cycle in a structurally similar spot (an `Arc`-backed handle
installed as a closure's own captured state, where that closure is
reachable through the very handle it closes over) — it is the same
proof technique, not a new one, applied to a different `Arc`.
