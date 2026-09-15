# An eagerly-applied mutation made for ordering reasons still needs a symmetric rollback on the failure path (issue #838, `SharedWal::group_tails`)

`SharedWal::submit_with_mutation` (`animus-control::shared_wal`) applies a
caller's `group_tails` mutation *before* the corresponding physical write
is even attempted — a deliberate design choice (not an oversight): a later
op's own `compact_group`/`forget` rewrite must see every earlier-enqueued
op's mutation already folded in, regardless of whether that earlier op has
physically landed yet, or FIFO queue order and physical write order would
diverge. What the original code got wrong was treating "applied eagerly for
ordering" as equivalent to "final" — `drive()`'s failure branch delivered
the physical error to every waiter but never undid the mutation, so a
*tolerated* (halted-gated, ADR 0017/`persist_wal`) live failure left a
phantom entry in `group_tails` forever. Since a **different**, healthy
tablet's own next `compact_group`/`forget` rewrite serializes `group_tails`
*whole* (every co-hosted tablet's own tail, verbatim), that phantom entry
rides along into the physical file the moment any sibling next compacts —
turning an ordinary, successful operation on unrelated data into the
vector that durably writes bytes that were never fsynced and never acked.
**The general rule: when a mutation is applied early for an ordering
invariant rather than because it's known-final, the failure path must
explicitly undo exactly that mutation — "eager" and "committed" are
different claims, and only proving the failure path symmetric (a
snapshot-and-restore keyed to the same op that made the eager mutation,
undone in strict reverse order for a batch that coalesces several ops on
one key) closes the gap.** This is a variant of the durable-before-visible
family of bugs (root `CLAUDE.md`'s "an ack means fsynced" rule) at one
remove: not "don't expose a write before its own fsync," but "don't let
one write's *in-memory bookkeeping* survive its own failed fsync where a
DIFFERENT write's success can durably launder it." Look for this shape
anywhere a coordinator eagerly updates shared in-memory state before its
own matching I/O completes, purely to keep concurrent callers' views
consistent — the ordering justification is real, but it is not a
durability justification, and the two must not be conflated.

**A second, narrower lesson from building this fix's own regression
(`crates/animus-cp-data/tests/sharedwal_fault_corpus.rs`'s cell (e)): the
choice of *which* physical op a fault-injection test fails matters more
than it looks, because a real disk (and this repo's own `DiskConfig`
model of one) treats `append` and `sync` as two independently-observable
steps with different failure semantics.** A first draft of this
regression tried to reproduce "a live write racing `shutdown()`" the
literal way — arm a `sync_delay`, let a doomed round's `env.append`
succeed and buffer real bytes, THEN set `halted`/arm a disk failure and
let the *following* `env.sync` fail. That is a faithful reproduction of a
real crash-adjacent hazard (a successful `write()` followed by a failed
`fsync()` does not retroactively un-write the bytes — on a real
filesystem a LATER, completely unrelated caller's own successful `fsync`
on the same file can durably commit them, and this repo's own `SimEnv`
`Disk::sync` models exactly that: a failed sync leaves `buffered` bytes
in place rather than discarding them), but it is a **different** bug from
the one under test here — it durably leaks the phantom via file-level
buffer accumulation, not via `group_tails`, and it happens regardless of
whether `group_tails` itself is ever rolled back. Failing the *first* op
(`env.append` itself, via `DiskConfig::set_error_prob(1.0)`, which this
repo's `Disk` impl checks before ever buffering a byte) instead reproduces
issue #838's own mechanism in isolation, with nothing physically written
for the doomed round at all. **The general rule: when a fault-injection
test's job is to isolate one specific defect, pick the failure point that
produces the minimal physical side effect consistent with "this op never
durably landed" — a fault shape that lets an op partially succeed before
failing can accidentally exercise a second, unrelated (and possibly
unfixed) hazard, muddying whether a red-then-green result is actually
proof of the fix under test.** The buffered-bytes hazard this surfaced is
real but out of this PR's scope (per this repo's own "an incidental bug
gets its own PR" convention) — filed as issue #883, with the exact
mechanism, the regression cell that would prove it, and why #838's own
fix and its own regression both deliberately don't cover it — named here,
not fixed, for whoever next touches `SharedWal`'s or `persist_wal`'s
failure handling.
