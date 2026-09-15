# A stale doc comment is a third way the same ADR 0055 bug hides, next to the two above.

**A stale doc comment is a third way the same ADR 0055 bug hides, next to
the two above.** `kind_scan.rs`'s LSI-query round-robin (the same
every-node-in-turn pattern as the entry just above, but for an LSI) omitted
`ConsistentRead` and asserted `Count:3` unconditionally; CI caught a
lagging replica-local read returning `Count:2`. The test's own comment
("a query is strongly consistent — no polling needed") and `dynamo.rs`'s
LSI-branch doc comments ("an LSI is strongly consistent by construction…
`consistent_read` is simply dropped on that branch") both predated ADR
0055 and were never updated when the flag started selecting a real read
path there too, so a reader trusting either comment would conclude the
missing annotation was fine. Fixed the same way: `ConsistentRead: true`,
since the loop is a write-verification read exercising the forwarded path.
Generalized: an ADR that changes a default's behavior obsoletes every doc
comment written under the old behavior, not just the code calling it —
grep for the old invariant's wording (`strongly consistent`, `by
construction`, `is simply dropped`) wherever the changed path is
documented, not only at its call sites.
