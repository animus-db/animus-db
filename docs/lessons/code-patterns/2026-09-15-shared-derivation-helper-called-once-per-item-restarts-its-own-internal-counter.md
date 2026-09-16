# A shared "derive N things from this entry" helper called once per item, instead of once per entry, restarts its own internal counter every call

Issue #852: DynamoDB Streams `SequenceNumber` was not unique per record
because every change-log record derived from one `TransactWriteItems` commit
carried the same HLC timestamp, and a `GetRecords` page boundary landing
inside that tie permanently dropped the rest of the tied group.

The first fix attempt widened `materialize_derived` (the one shared
apply-time helper both `KvCommand::KindBatch`'s apply arm and `KvCommand::
TxnResolve`'s commit branch call to complete a change-log record's key) so
that, given a whole `change_log: &[(Vec<u8>, Vec<u8>)]` slice for one entry,
it stamps each record's key as `prefix || hlc::pack(ts) || record's own index
within the slice`. This is correct *only if the caller always hands the
helper the full list of records for one logical commit in a single call*.

`KvCommand::TxnResolve`'s commit branch does not do that: it iterates
`keys.iter().zip(resolved)` and calls `materialize_derived` **once per
resolved key**, each call passing a 1-element `change_log.as_slice()`. Each
call's own internal `enumerate()` therefore always starts at index 0 — every
record produced by that loop got ordinal 0 regardless of its position in the
loop, so two items written by the same `TransactWriteItems` still collided on
`(packed_hlc, ordinal)` even after the "fix." This was not caught by static
review of `materialize_derived`'s own body (which looked correct in
isolation) — it was only found by driving a real 3-item `TransactWriteItems`
over the wire and observing, via temporary debug output, that two different
items' change records had byte-identical `(hlc, ordinal)` pairs.

**The lesson**: when a helper derives a sequential/enumerated property (an
ordinal, an index, a sequence number) from "all the items I was handed in
this one call," any caller that invokes the helper multiple times to cover
what is logically *one* entry/commit/batch must explicitly thread an
accumulator (a `starting_ordinal` parameter and a returned "next available
ordinal" value, in this case) across those calls — the helper cannot know
about a caller-side loop it isn't inside. Before trusting such a fix, grep
every call site of the helper, not just the ones a first pass happened to
look at, and specifically check whether any of them calls it in a loop over
what should logically be one batch. A generic-looking `fn f(items: &[T])`
signature does not by itself protect against being invoked once per item
instead of once per whole slice.
