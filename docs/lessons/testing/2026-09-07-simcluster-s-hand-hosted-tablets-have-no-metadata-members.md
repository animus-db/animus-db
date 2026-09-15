# `SimCluster`'s hand-hosted tablets have no `Metadata::members` catalog behind them — a legacy-registered (no `CreateTable`) table's auto-provision silently mints an empty-replica-set tablet that can never be hosted (ADR 0061 rung D3 PR 1).

**`SimCluster`'s hand-hosted tablets have no `Metadata::members` catalog
behind them — a legacy-registered (no `CreateTable`) table's
auto-provision silently mints an empty-replica-set tablet that can
never be hosted (ADR 0061 rung D3 PR 1).** Converting `crates/animusd/
tests/dynamo_extended.rs::concurrent_conditional_puts_one_wins` (a
`ProdEnv` test that never calls `CreateTable`, relying instead on
`dynamo::legacy_register`'s auto-registration of an unrecognized table
on its first `PutItem`) to `SimCluster` hung every write on "table
tablet did not provision in time." Root cause: `ClientCtx::
provision_tablet` — the auto-provision every first write to a
brand-new table goes through — picks the tablet's *initial replica
set* from `Metadata::members`, the node-registration catalog a real
deployment's `MetaCommand::RegisterNode` populates. `SimCluster::new`
never proposes that command at all — its own nodes are wired directly
into each node's `ClusterEdgeState`, never registered into `Metadata` —
so `members` is permanently empty in this fixture, and a
legacy-table's lazy auto-provision computes a **zero-length** replica
set: a `CreateTablet` nobody will ever host, so the write's own route
wait times out forever, deterministically, every run. This is a
distinct gap from the already-documented "GSI/LSI query, wire-level
`CreateTable`, `TransactWriteItems`" capability gaps `dispatch_item_op`
itself has — `provision_tablet` and `dispatch_item_op` are different
code paths, so a test needing neither GSI/LSI nor `CreateTable` nor
Transact can still hit a *third*, independent `SimCluster` fixture
limitation. The fix used here: route the test through `SimCluster::
create_table` (the fixture's own supported table-creation path, which
picks replicas directly rather than through `Metadata::members`)
instead of relying on legacy auto-registration — a fixture data-shape
change (a real composite `pk`/`sk` schema instead of a legacy one, so
every item needs an explicit `sk`), not a change to what the test's
own race actually proves. Widening `SimCluster::new` to also register
nodes into `Metadata::members` (closing the gap at its root, letting a
legacy-registered table's auto-provision work like it does on
`ProdEnv`) is a real fixture-PR candidate, deliberately not attempted
here — this was a test-conversion PR, not a fixture-widening one. The
general lesson: when converting a `ProdEnv` test that relies on an
*implicit* server-side behavior (auto-provisioning, auto-registration,
lazy anything) rather than an explicit setup call, check whether the
sim fixture's own construction path actually populates every piece of
replicated state that implicit behavior reads — a fixture can compile,
run, and simply hang forever rather than erroring, which makes this
class of gap slower to diagnose than a missing-capability compile error
or a clean rejection would be.
