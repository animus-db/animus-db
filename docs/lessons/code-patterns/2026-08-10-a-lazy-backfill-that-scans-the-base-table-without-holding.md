# A lazy backfill that scans the base table *without holding* the maintenance-state lock (so the scan doesn't stall concurrent writes) must not let its replay blindly overwrite a key a real write already touched more recently — the replay's snapshot is stale by construction.

**A lazy backfill that scans the base table *without holding* the
maintenance-state lock (so the scan doesn't stall concurrent writes) must
not let its replay blindly overwrite a key a real write already touched more
recently — the replay's snapshot is stale by construction.** The DynamoDB
GSI/LSI backfill (above) runs a network scan with the registry unlocked,
then replays every scanned `(key, value)` through `note_put` under the lock;
a concurrent write's own `note_put` (reflecting the item's *current* value)
can land in between, and since both calls target the same registry method,
whichever runs *last* wins — if that's the replay, it silently reverts the
real write's already-correct index entry to the pre-write value, with no
error and no signal that the write's index update was ever undone (the base
item stays correct; only the *index's* bookkeeping regresses, which is what
a later `Query` against the index reads). Fixed with
`SchemaRegistry::touched_since_backfill`: every `note_put`/`note_delete`
marks its key while a backfill is pending, and the replay skips any key
already found there rather than reapplying its own scanned value — so the
replay can only ever *seed* a key nobody has independently indexed
correctly, never *revert* one. Cleared on `mark_table_backfilled` so the
tracking set stays bounded to the (normally brief) in-flight window, not the
table's lifetime. General shape to watch for: any "scan without the lock,
then replay under the lock" pattern needs an explicit "was this touched
more recently than my scan" check before the replay writes anything, or a
race that regresses already-correct state passes silently (proven via a
deterministic *unit* test replaying the exact call order by hand — no
wall-clock timing needed to demonstrate a lock-ordering race like this one).
(`animus-dynamo::registry::{SchemaRegistry::touched_since_backfill,
TableState::touched_since_backfill}`; `animusd::dynamo::
backfill_index_if_needed`; ADR 0013.)
