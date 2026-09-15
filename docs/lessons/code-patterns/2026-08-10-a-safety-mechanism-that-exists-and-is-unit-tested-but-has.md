# A safety mechanism that exists and is unit-tested but has zero production callers is dead code with a green suite — second instance of the `narrow_scope` pattern above, on the *write* side this time.

**A safety mechanism that exists and is unit-tested but has zero production
callers is dead code with a green suite — second instance of the
`narrow_scope` pattern above, on the *write* side this time.** ADR 0028's
crossover-window write fences (`RaftKvNode::put_fenced`/`delete_fenced`/
`put_batch_fenced`, a `fence: KeyRange` embedded in the proposed command
and checked at apply time) landed additively with a thorough sim suite
(`animus-cp-data/tests/fenced_commands.rs`) — but `grep -rn "_fenced"
crates/animusd/src` found **zero** callers: `cp_put_local`/
`cp_delete_local`/`cp_batch_propose` (reached by every client write,
including every `cp_serve_forwarded` counterpart) all called the
*unfenced* `put`/`delete`/`put_batch`, which stamp `fence =
KeyRange::whole()` — so the apply-time check was a permanent no-op in the
one place it needed to matter. The trigger: a node whose `Metadata` view
hasn't yet observed a `SplitTablet` commit still resolves a child-range
key to the parent's (now too-wide) group via `cp_route`'s `Local` branch
(no re-resolution once routed); the unfenced write then applies onto the
shared engine's physical key the child now logically owns, shadowing or
corrupting it via LWW — invisible to every existing test because nothing
drove a write into that specific crossover window. Fixed by adding an
additive `RaftKvNode::scope_range()` accessor (a `StorageScope::range()`
getter underneath) and stamping it as the fence on every real proposal.
**The sharper lesson is the second half of the fix, not the wiring
itself:** the fence alone is not sufficient, because `cp_put_local`/
`cp_delete_local` confirm success by polling for the proposed value (or
its absence) to read back from **local** storage — and a fenced-out entry
still commits and applies as a deterministic no-op, silently advancing
any coarser "did this commit" signal (e.g. `engine_applied_index()`
alone) right along with it. Had the confirm loop been keyed on such a
signal instead of exact value equality, wiring the fence alone would have
turned "silently corrupts the child" into "silently falsely-acks a write
that never happened" — a *different* silent-failure mode, not a fix. The
actual fix pairs the fence with a **pre-propose range check**: reject
before ever proposing if a key falls outside the group's current
`scope_range()`, returning the same error shape a routing failure already
produces so the caller's retry re-resolves `cp_route`; the embedded fence
then only has to cover the much smaller residual race between that check
and the entry's actual apply. **General checks this generalizes to:** (1)
when auditing a safety mechanism for "is it wired in," also ask "does the
*confirmation* path downstream of it use a signal precise enough that a
mechanism turning a write into a no-op is distinguishable from the write
actually succeeding" — a coarse confirm signal can convert a newly-fixed
correctness bug into a new, differently-shaped one; (2) a regression test
for this class of bug needs access to the private routing internals (here,
a specific tablet's `CpGroup` handle) to *force* the stale-routing shape
deterministically, since the real race is not reliably reproducible over
wall-clock timing — when the integration crate under `tests/` can't reach
what's needed (its types are only `pub(crate)`/private), an **in-crate**
`#[cfg(test)] mod` (a child module of the module holding the private
items, hence able to see them) is the right tool, not a workaround.
(`animus-cp-data::RaftKvNode::scope_range`; `animusd`
`cp_put_local`/`cp_delete_local`/`cp_batch_propose`,
`split_fence_tests::stale_routed_write_for_a_split_childs_key_is_rejected_not_lost`.)
