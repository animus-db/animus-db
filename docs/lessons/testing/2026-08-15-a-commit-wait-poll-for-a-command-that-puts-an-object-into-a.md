# A commit-wait poll for a command that puts an object into a *transient* status must check the object's presence, not that it still holds the exact status value just proposed

**A commit-wait poll for a command that puts an object into a *transient*
status must check the object's presence, not that it still holds the
exact status value just proposed** — a concurrent convergent loop can
legitimately advance past that status before the proposer's own next
poll, especially on a small/fast-converging fixture in a test. `UpdateTable`
Create (ADR 0045 §6) proposes `CreateTableIndex{status: Creating}` and
waits for it to commit exactly like `create_table`'s own index-definition
loop (presence-by-name only, `dynamo.rs::create_table`); the completion
aggregator (`index_backfill_loop`, ADR 0045 §4) can flip that same index
to `Active` within one tick of a tiny table's backfill finishing, which on
a single-node test can race the proposer's very next `metadata_fresh`
read. Polling for `status == Creating` specifically would then spuriously
time out despite the create having fully succeeded. The already-shipped
`set_index_status` (used by the drop cascade's `Deleting` transition)
gets away with checking the exact target status only because nothing in
this codebase ever proposes a *further* transition away from `Deleting`
before `DropTableIndex` removes the definition outright — that is a
narrower invariant than "commit-wait polls are safe to pin to an exact
status," not a counterexample to this lesson. General rule: when a
commit-wait's target value can itself be mutated again by some other
loop before the waiter's next poll, wait on the mutation that is
monotonic/permanent (existence, a monotonic counter, a specific id) —
never on a value a *different* proposer can race past.
(`animusd/src/dynamo.rs::create_index`, 2026-08-15.)
