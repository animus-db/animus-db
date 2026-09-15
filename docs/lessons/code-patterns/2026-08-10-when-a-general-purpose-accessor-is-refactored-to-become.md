# When a general-purpose accessor is refactored to become mirror/cache-tolerant, grep every internal caller of that accessor for one that is actually a commit-wait/read-your-writes poll on its own just-proposed command — the poll must keep bypassing the new tolerance, not just the one example the task happened to name.

**When a general-purpose accessor is refactored to become mirror/cache-tolerant,
grep every internal caller of that accessor for one that is actually a
commit-wait/read-your-writes poll on its own just-proposed command — the poll
must keep bypassing the new tolerance, not just the one example the task
happened to name.** Building the ADR 0035 PR1 `ControlHandle` seam, the task
named exactly two call sites that must stay fresh (a `CreateTable`
commit-wait poll, a DynamoDB conditional-write existence gate) as the
rationale for adding a `metadata_fresh()` read alongside the new
cache-tolerant `effective_metadata()`. But the *shared helper methods*
those two examples sit next to (`ClientCtx::table_schema`/`has_table_schema`,
and `dynamo.rs`'s free `metadata(ctx)` fn) are each called from **two kinds
of site at once**: plain lookups (which should become mirror-tolerant, the
actual growth-node bug being fixed) and a proposal's own commit-wait
predicate (which must not, or the poll could pass "committed" off a mirror
that's still a poll interval behind, or wait forever on one that's stuck).
Switching the shared helper's body wholesale would have silently broken
every commit-wait poll built on top of it — including `drop_table_schema`,
which wasn't one of the two named examples but has the exact same shape as
the one that was (`create_table_schema`). The fix is to keep the general
helper cache-tolerant and have every internal commit-wait caller bypass it
with an explicit fresh read instead of routing through the helper — found by
tracing every caller of the helper being changed, not by trusting the task's
own worked examples to be exhaustive. (`animusd::control_handle::
ControlHandle::{metadata_cached, metadata_fresh}`; `ClientCtx::
{effective_metadata, metadata_fresh, table_schema, has_table_schema}`;
`dynamo.rs::{metadata, metadata_fresh}`.)
