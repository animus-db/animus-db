# Observe a multi-group-per-node metric under `SimEnv` by sharing one `MetricsHandle` across several independent `Simulator` worlds — not by adding a production constructor an investigation-only PR shouldn't ship (ADR 0044 phase 2, C-02 PR 1)

`SimEnv::metrics()` is `MetricsHandle::noop()` by design (`animus_env::
Env::metrics`'s own default) — a component that wants to record into a
test-readable sink under simulation has to be handed a recording
`MetricsHandle` directly instead (`RaftKvNode::start_with_metrics` exists
purely for this). But the *realistic* production shape for "several
tablet groups sharing one node" is `RaftKvNode::start_hosted` (an explicit
`stream = tablet_id`, ADR 0026), and there is no constructor that takes
**both** an explicit `stream` (for co-hosting on one set of node ids) and
an injectable `MetricsHandle` (for `SimEnv` observability) — only one or
the other. Adding one would be a small, safe, additive-only change (the
exact category `start_with_metrics` itself already is), but it is still a
`src/` change, and the task at hand (a map/investigation-only PR, ADR 0044
phase 2's C-02 PR 1) was explicitly scoped to "no production behaviour
change."

The fix needed no production code at all: `MetricsHandle` is a plain
shared counter sink (cheap `Clone`, atomics underneath) — nothing ties it
to one `Simulator`/`SimEnv` world. Standing up `G` **independent**
`Simulator`s (each hosting one ordinary 3-node group via the existing
`start_with_metrics`, each forced onto `PRIMARY_STREAM` since it can't take
a `stream` argument) but handing every one of them the **same** three
`MetricsHandle`s (index-aligned by node id) makes their combined counters
read exactly as if all `G` groups were co-hosted on one real node's shared
`env.metrics()` sink — because from the metric sink's point of view, that
is indistinguishable from what actually happened. This produced a real,
green, seed-reproducible baseline test
(`crates/animus-cp-data/tests/heartbeat_cost.rs`) proving heartbeat traffic
scales with hosted-group count today, using only pre-existing test
infrastructure combined in a new way, with zero `src/` changes.

**General form**: when a task's scope forbids a production change but the
*realistic* fixture for what you want to observe needs one (a constructor
that doesn't exist yet, a knob that isn't wired), check whether the thing
you actually need to observe (here: an aggregate counter) is decoupled
enough from the thing the missing constructor would provide (here: which
`Simulator` world a group physically lives in) that sharing the
observable object across several *separately* fixture-able instances gets
you the same measurement without the code change. Don't reach for the
missing constructor as the only path — ask what invariant the measurement
actually depends on, and whether a simpler composition of existing pieces
already satisfies it.
