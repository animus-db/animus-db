# A staged intent in a scope whose only reader skips intents is a silent-loss mechanism, not a visibility delay.

**A staged intent in a scope whose only reader skips intents is a
silent-loss mechanism, not a visibility delay.** The obvious-looking fix
for `TransactWriteItems` on an indexed/streamed table was "stage the LSI
row / change-log record as an intent in its own kind scope, resolved
later, the same way a base row is staged today" (recorded as the planned
design in ADR 0041 §2/ADR 0042 §16 before this was built). It looks like
ordinary eventual consistency — "the row appears a little later, once
resolved" — but it isn't: every consumer of a kind scope (the GSI drain,
the Streams sealer, the backfill seeder) scans **forward from a
watermark** and is *defined* to skip an intent outright (only a
base-scope reader ever resolves one — `RaftKvNode::local_get_kind`'s own
doc states the invariant it relies on: "these scopes only ever hold
committed values"). A record staged at `ts=10` and resolved at `ts=40`,
after a consumer's watermark has already passed 10, is gone forever —
not late, not stale, **never delivered, with no error**. The fix
(`docs/adr/0046-tablet-log-model.md`'s Decision 2/materialize-at-resolve,
ADR 0018 §2's 2026-08-16 amendment) rides the derived payload inside the
*base* write's own intent instead, materializing it at resolve in the
same atomic apply that finalizes the base value — kind scopes never
gain an intent-resolution step at all. **General form**: before staging
anything as an intent in a scope, check whether that scope's readers are
built to resolve one; a scope whose entire contract is "no intents here"
cannot safely grow one just because the writer's *other* scope (the base
row) already tolerates staging.
