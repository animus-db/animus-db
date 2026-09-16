# A descending scan of a composite key is not "the max of its trailing field" — issue #859

Found designing the fix for `GetShardIterator{LATEST}`'s own O(n) hot-tail
scan (issue #859): the suggested shortcut — "a bounded descending scan of
the `KIND_CHANGE` scope, take the first row" — looked obviously cheap and
correct, and would have shipped a silent correctness bug had it not been
checked against the actual key layout first.

## The trap

`animus-cp-data`'s `KIND_CHANGE` keys are `token(pk) || escape(pk) ||
packed_hlc || ordinal` (`materialize_derived`'s doc) — the commit-order
field (`packed_hlc`) is a **suffix**, not the leading bytes. `token(pk)` is
a hash-ring token, unrelated to write order; `escape(pk)` is the raw
partition key. A plain descending physical-key scan therefore returns the
row whose `(token, pk)` prefix sorts lexicographically largest — which has
no relationship at all to which row committed most recently. For a table
with more than one partition key, "descending scan, take the first row"
picks essentially a random row, not the true maximum, with high probability
of being wrong the moment two different partition keys are involved.

This is easy to miss because a **single-partition-key** test (write one
key repeatedly, take LATEST) passes either way — the bug only shows up once
distinct partition keys interleave, which single-happy-path testing rarely
exercises by accident.

## The fix, generalized

When you need "the maximum of a monotonic field that is a *suffix* of a
composite storage key," a descending key scan is only valid if that field
is also the **most significant** (leading) part of the key. If it is not —
because the key's own leading bytes exist for a different reason (sharding,
grouping, locality) — a physical scan cannot answer "what's the max" at
all, cheaply or otherwise; you need either (a) a secondary index ordered by
the field you actually want the max of, or (b) an incrementally-maintained
cache of the max, updated exactly where the field is minted (see
`RaftKvNode::hot_change_max`, updated at every `materialize_derived` call
site, seeded once at boot from one full scan so a restart never fabricates
a value out of thin air).

## The check that catches it

Before trusting "descending scan for the max," write out the full physical
key's byte layout end to end and ask: is the field I want the max of the
**first** differentiator in key order, or does something else (a hash, a
grouping prefix) sort ahead of it? If anything sorts ahead of it, a key
scan answers a different question than the one being asked.
