# A boolean existence question must not materialize the full answer — issue #861

`animus_env::verify_or_init_segment_store_marker`'s marker-absent/
key-given branch needed exactly one bit of information — "does this store
hold anything else at all?" — and got it by calling `store.list("").await
?.is_empty()`: fetch every matching id, then throw all but the boolean
away. Against `SimSegmentStore`/`FsSegmentStore` that's free (an in-memory
`BTreeMap` scan, a bounded local directory walk), so nothing in this
crate's own tests ever caught it. Against `S3SegmentStore`, whose `list`
follows `ListObjectsV2`'s `next_continuation_token` to completion (up to a
10,000-page safety cap), the identical call became a full, billable
enumeration of an entire populated bucket — paid on every node's startup,
by every node, for as long as `--encryption-key` was configured without
the marker yet written.

## Why review/tests didn't catch it

The call site's own contract test (`assert_segment_store_contract`)
exercised `list` and `get`/`put`/`delete`, but nothing exercised "how much
work does an emptiness check cost against a *paginated* implementor" —
there was no implementor in the test suite whose `list` was expensive
enough for the difference to matter, so the shared contract had nothing to
flag. The bug shipped correct (every existing test passed) and merely
slow, by a large, unbounded-in-the-worst-case factor, on exactly the
implementor (`S3SegmentStore`) the contract-holds tests already covered
for *correctness* but never for *cost*.

## The general form

When a call site only needs a yes/no answer — "is anything here",
"does this exist", "any object under this prefix" — grep for whether the
trait/interface it's calling already has a method shaped like that
question. If it doesn't, and the only available method returns the full
answer set, that is the load-bearing signal to add the narrower method
(with a default implementation in terms of the wider one, so no existing
implementor has to change) rather than let every future paginated/networked
implementor pay for a listing it never needed. The `S3SegmentStore` fix
here (`SegmentStore::is_empty`, `crates/animus-env/src/lib.rs`) mirrors the
same "additive default over a well-known shape" pattern `Env::metrics()`/
`Env::merge_peer()` already established for this workspace — reach for
that shape by default whenever a trait needs to grow without breaking an
existing implementor.

A cheap in-memory or bounded-local implementor rarely fails to notice this
kind of bug in review, because "drain a `Vec` to check `.is_empty()`" reads
as harmless — the cost only becomes visible against an implementor whose
underlying operation is unbounded or genuinely I/O-bound (a paginated
network listing, a full-table scan, a recursive filesystem walk over an
unbounded tree). When adding a new implementor of an existing trait whose
methods return collections, it's worth asking, for each existing caller of
that method, whether the caller actually needs the collection or only a
property of it (empty/non-empty, count, first match) — a caller that
already gets away with materializing the whole thing against every
*current* implementor may still be a real cost bug waiting for the next
implementor that changes the trade-off.
