# A dev tool nested in the server's own use-case layer duplicates the wire's byte shapes and grows its own performance bugs — put load generation on the client and let it ride the real path (ADR 0021 amendment, 2026-09-09)

`POST /admin/data/seed` started as a small convenience (ADR 0021: bulk-write
synthetic rows to drive sharding tests) and grew, over several PRs, into a
second implementation of `PutItem`: it had to re-derive the exact key/value
byte shapes a real client's write would produce (`seed_key_attr`,
`animus_dynamo::wire::encode_stored_item`'s envelope) to stay readable
through the real DynamoDB edge, and once that duplication existed it also
had to reinvent, and separately tune, its own performance characteristics
— a marker-batch fast arm sized around `SEED_BATCH_SIZE`/
`SEED_BATCH_MAX_BYTES`, and a **second**, unrelated concurrency knob
(`SEED_IMAGES_CONCURRENCY`) for the per-item images-table arm — neither of
which had anything to do with the real `BatchWriteItem` operation's own
25-item cap or its own throttling contract. The images arm's own
concurrency bug (PR #783) and the marker arm's own entry-granularity bug
(one-entry-per-item vs. one-per-tablet, this same log's own earlier
"same-predicate" entries) were each real, each shipped, and each needed
for exactly the reason a second implementation of a write path always
needs its own bug-fixing: it is not the same code as the one path a real
client exercises, so nothing that hardens the real path automatically
hardens it.

It also had a fidelity gap structurally impossible to close without
becoming a third thing: a throttled row (ADR 0065) had no `UnprocessedItems`
to be echoed through, since the internal primitives it called had no such
wire-level contract at all — so a throttled seed silently under-wrote, with
`written` reporting a number the caller had no way to tell was short of
what the real op's own semantics would have reported.

**The fix wasn't a bug fix — it was removing the second implementation.**
`animus-cli seed` now generates the identical item shape client-side and
writes it through the real `BatchWriteItem` wire operation; `/admin/data/
seed` is now a thin proxy over the same operation, through the same
generic dispatcher `/admin/data/dynamo` already used
(`execute_routed_as_generic`). Every one of the bugs above stopped being
possible **by construction**, not by being fixed: there is no byte shape
left to duplicate (the wire decoder is the one and only encoder/decoder),
no second concurrency model to separately tune (chunking + `buffer_
unordered` is the same idiom a real bulk-loading client would use), and no
missing wire contract to work around (`UnprocessedItems` already exists,
because the real operation already has one).

**The general lesson**: when a server gains an admin/debug/test-support
route whose job is "do roughly what a client operation does, but from
inside the node, for convenience or speed" — a bulk loader, a synthetic
data generator, a fixture seeder — resist implementing it against the same
internal primitives the real operation is built from. Implement it as a
client of the real operation instead (even a very short/proxied hop, as
`/admin/data/seed` still is). The second implementation looks cheaper up
front, but it inherits none of the real path's own hardening, duplicates
work that will drift the moment either side changes, and accumulates its
own bug class that has nothing to do with the feature it exists to test.
