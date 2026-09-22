# A page byte-cap composes with a count `Limit` via incremental accumulation, not the count-cap's own peek-ahead trick

**When a pagination loop already detects "was this page truncated?" by
over-fetching one extra item (`want = limit + 1`, then `truncate(limit)`
and check `len() > limit`), don't reuse that same peek-ahead shape for a
second, independent stopping condition (a byte budget). Track the second
condition's own running total incrementally inside the loop and return
early the moment it would be exceeded — this composes with the count-based
trick for free, needs no second round trip, and never needs to "un-add" an
item that was already counted against the wrong budget.**

## The situation

ADR 0072 layer 3 adds DynamoDB's real `Query`/`Scan` rule: a page stops at
1 MiB of *evaluated* item data (`animus_dynamo::limits::
MAX_QUERY_SCAN_PAGE_BYTES`), composing with the pre-existing `Limit` item
count — whichever stops the page first wins. The existing count-based
pagination (`animusd::dynamo::paginated_table_examine` and its two
siblings) already had a shape for "was this page cut short": request one
more item than `Limit` calls for (`want = limit.saturating_add(1)`), let
the loop run to `want` items or exhaustion, then have the *caller*
`truncate(limit)` and infer `truncated` from `examined.len() > limit`.

The temptation is to give the byte cap the identical treatment: keep
fetching until *some* over-budget marker is seen, then truncate after the
fact. That doesn't work cleanly for a byte total the way it does for a
plain count — there is no clean "byte quota + 1" to request from the
underlying scan RPC (items have different sizes; you cannot ask a scan for
"exactly one item over my budget"), and truncating a byte-tracked
accumulator after the fact means re-deriving which items to drop and
re-summing, exactly backwards from how the loop already built the total up
one item at a time.

## The fix

Track the byte budget **incrementally, inside the same loop that already
builds `examined` one item at a time**: for every item `keep()` accepts,
compute its `animus_item::item_size` and check `running_total + size >
MAX_QUERY_SCAN_PAGE_BYTES` *before* pushing it. If the check trips, return
immediately with the accumulated `examined` untouched and a `byte_capped =
true` flag — the rejected item is never added, so there is nothing to
"un-add" and no second pass. The caller's existing `truncated` computation
becomes an `||`: `byte_capped || limit.is_some_and(|n| examined.len() >
n)`. Whichever condition trips first short-circuits the loop; the other
one simply never gets the chance to fire. No interaction bugs, because the
two checks share one loop body and one `examined` vector — there is no
window where both are "true" in a way that needs reconciling.

**One invariant makes the very-first-item edge case a non-issue**: a
single item can never itself exceed the page cap, because the *item* size
cap (`animus_item::MAX_ITEM_SIZE_BYTES`, 400 KB) is well under the *page*
byte cap (`MAX_QUERY_SCAN_PAGE_BYTES`, 1 MiB). So `running_total(0) +
size(first item) > cap` can never be true, which means a page can never
come back empty-but-not-exhausted from the byte cap alone — the incremental
check is always safe to apply starting from the very first item of a page,
with no special-casing needed for "but what if even one item doesn't
fit." Any similar future page-budget rule should check that its own
per-item cap is provably smaller than its own per-page cap before assuming
this; if it can't be (e.g. the caps could someday be configured
independently), the loop needs an explicit "always admit the first item
regardless of budget" rule instead, on pain of a page-that-never-ends.

## The general rule

Two independent "stop this page" conditions that must compose (whichever
fires first wins) belong in the **same loop**, checked incrementally as
each candidate item is produced, each one returning early the moment it
trips. Don't graft a new condition onto an existing "overfetch by one,
then truncate and diff" trick designed for a different kind of budget
(count vs. bytes) — the two shapes don't combine cleanly, and the
incremental-check shape both composes trivially with whatever's already
there and is usually simpler to reason about besides.

## A related, adjacent finding worth recording for whoever builds ADR
0065's `ConsumedCapacity` reporting for `Query`/`Scan`

Before implementing this cap, the task asked to check whether
`ConsumedCapacity` accounting already accumulates evaluated bytes for
`Query`/`Scan`, so the byte cap could share that accumulator rather than
adding a second one. As of this change, it does not: `run_query`/
`run_scan` (`animusd::dynamo`) take no `ReturnConsumedCapacity` parameter
at all, and `capacity::read_capacity` (the one function that prices a
read) is only ever called from `GetItem`'s single-item path — `Query`/
`Scan` currently report no `ConsumedCapacity` field whatsoever, and
`sim_cluster_dynamo_consumed_capacity.rs`'s own module doc already flags
this as a known, separate gap ("touch these call sites; they're converted
alongside the `Query`/`Scan` ..."). So this layer's byte-tracking
accumulator (`evaluated_bytes` inside `paginated_table_examine`/
`paginated_kind_examine`/`paginated_kind_examine_one`) is new, not shared
with anything — but it is now the natural place for a future
`ConsumedCapacity`-for-`Query`/`Scan` layer to read its own byte total
from, so the two mechanisms can never disagree once that gap is closed.
