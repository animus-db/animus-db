# When a process-scoped convenience shortcut is removed, grep for tests that quietly relied on it to *assert something the removed shortcut made trivially true* — not just tests that time out.

**When a process-scoped convenience shortcut is removed, grep for tests that
quietly relied on it to *assert something the removed shortcut made trivially
true* — not just tests that time out.** Making `--cluster N`'s in-process
`ClusterEdgeState` per-node (ADR 0031 PR2, closing the gotcha above) broke
exactly one of ~90 `animusd` tests: `cql_wire.rs`'s cross-connection
`EXECUTE` assertion, which `PREPARE`d a statement via node 0 then
`EXECUTE`d it via a connection to node 1 to "prove the prepared store is
shared across connections" — true only because the old shared edge made
every node's `CqlState` the same object. Per-node, that's not a bug to fix,
it's the **correct, intended new behavior** (a real one-process-per-node
deployment never shared this either) — so the honest fix is to change what
the test proves: reuse a **second connection to the same node** (`conn0b`)
for the cross-connection assertion, and keep the cross-*node* connection
for what's actually still cross-node-safe (reading committed CP-plane
data). The signature to watch for isn't a hang/timeout (this failed with a
clean, immediate `Error` response) — it's an assertion whose comment
literally describes the removed shortcut's own guarantee ("shared across
connections/nodes/processes"); grep test comments for the word "shared" (or
"cluster-wide", "any node") near the specific state you're scoping down,
not just the obvious call sites. Every other test that exercised
cross-node behavior already did so through a *real* mechanism (replicated
`Metadata`, `cp_route` forwarding, `propose_schema` relay), so removing the
shortcut made those tests exercise more real code, not less — 100% of the
rest of the workspace suite passed unmodified, including several
(`cp_plane.rs`, `cp_rebalance.rs`, `cp_reconfigure.rs`) that now genuinely
drive cross-process-style forwarding in-process for the first time instead
of resolving everything locally through the shared registry. (`animusd`
`tests/cql_wire.rs::cql_wire_prepare_execute_typed_round_trip`.)
