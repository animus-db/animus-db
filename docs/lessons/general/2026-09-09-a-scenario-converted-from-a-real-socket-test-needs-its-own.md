# A scenario converted from a real-socket test needs its own retry discipline for a race the original's real network timing happened to paper over (ADR 0061 rung L, C-12 PR 4b)

Three of `sim_cluster_split_cluster.rs`'s six converted scenarios passed
on the first `cargo test` run for the tests that didn't touch a fault at
all, then failed the moment the scenario chained real disruption
(a crash, a dual crash, a split) immediately followed by a single-shot
call with no settle buffer — a shape the original real-socket test never
hit because a real process crash/election/relay round trip costs enough
real wall-clock time on its own that the next client call, issued from a
fresh thread a moment later, effectively always lands after things have
settled. `SimCluster`'s own event-driven `spawn_and_capture` has no such
implicit buffer — a `put`/`create_table` call issued the very next line
after `cluster.crash(..)` races the recovery at exactly the granularity
the scenario code controls, and a converted scenario's own single-shot
`.unwrap_or_else(|e| panic!(..))` call (copied straight from the original
test, which never needed to retry) is therefore the first thing to
surface each of three distinct, legitimate transient races: `CreateTable`
racing `await_table_serveable`'s own bounded wait against a
just-recovered control plane/reconciler that hasn't settled yet; a plain
write racing a dual (control+data) fault that genuinely needs more than
one relay attempt to route around; and a write landing in a tablet's own
split-cutover-freeze window (ADR 0050's `"; retry"` transient), which this
fixture can only clear by manually driving `SimCluster::
drive_inplace_split_cutover` since it spawns no periodic cutover loop.

**The general rule**: when converting a real-socket scenario that chains
a fault directly into a write/DDL call with no explicit wait between them,
do not assume the original's own single-shot call proves the converted
call needs none either — the original's implicit real-time buffer was
doing real work. Give the post-fault call its own bounded retry (with the
fixture's fault-clearing side effect, like `drive_inplace_split_cutover`,
re-run on every attempt where relevant), matching `sim_cluster_auto_
split.rs`'s own `put_item_retry` precedent, rather than discovering the
race as a flaky-looking test failure and reaching for a longer timeout.
