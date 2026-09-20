# A batched Raft entry sharing one `ts` can silently drop a same-key duplicate's write

Found implementing issue #996 layer 1 (`KvCommand::KindEvalBatch`, `animus-
cp-data`): a single Raft entry carrying `N` independent item writes, all
stamped with that ONE entry's own commit timestamp as their MVCC version.

## The trap: an in-apply read overlay looks sufficient on its own

Two items in one batched entry can legitimately target the same logical
key — a plain `BatchWriteItem` call has no duplicate-key validation, unlike
`TransactWriteItems`'s own check. Before a batched entry existed, each item
was its own Raft entry at its own, strictly-increasing `ts`, so a later
duplicate naturally overwrote an earlier one at the storage layer with no
special-casing needed. The obvious fix for preserving that behavior once
several items share one entry (and therefore one `ts`) is an in-apply
overlay: a `BTreeMap<key, Option<Item>>` populated with each item's own
`new` image as it applies, consulted before `storage.get` so a later item
observes an earlier item's write within the same entry.

That fix is real and necessary — but it only fixes the **read** half. It
does nothing about what the **storage engine** ends up holding, and testing
only the returned per-item results (which the overlay makes correct) can
make the bug invisible: the test can show the client-visible outcome is
right while the actual persisted row is wrong.

## Why the write silently loses anyway

`StorageEngine::merge`'s contract is per-key last-writer-wins by **strict**
inequality: a merge takes effect only when its version is strictly greater
than the key's current latest. Two items in one entry writing the identical
physical key both carry the identical version — that entry's own shared
`ts` — so:

1. Item 0's write applies (nothing existed at that key yet); the key's
   latest version is now `ts`.
2. Item 1's write for the same key also carries `ts`. `ts <= ts` is true,
   so the merge silently no-ops and the FIRST value is what survives.

This is the *opposite* of the last-write-wins behavior being preserved.
Nothing about the read overlay prevents it: the overlay only governs what
`evaluate_kind_eval` sees when it decides item 1's own outcome, not what
physically lands in the engine.

## The fix: collapse to one write per physical key before queuing them

Right before an entry's own accumulated `pending` writes are handed to the
engine, collapse them to at most one op per physical key, keeping the LAST
one pushed. This is sound specifically because the entry's own apply arm
had already called `flush_pending` once, up front, before its own
per-item loop started — so `pending` is guaranteed to hold **only** this
entry's own writes at the point of collapsing, and cannot affect any other
entry's.

## The generalizable rule

**Any time a single write batch stamps more than one write to the same key
with the identical MVCC version, a strict-inequality per-key LWW merge
silently keeps the first and drops the rest — check for this whenever a
"one entry, one timestamp, several logical writes" shape is introduced**,
not just for evaluate-at-apply item writes. A read-side fix (an overlay, a
cache, a shadow map) that makes evaluation see the right chain does not by
itself guarantee the write-side merge preserves it — the storage engine's
own equality-vs-strict-inequality version comparison is a separate,
independent thing to check. Test for this specifically: assert the
**persisted** value after a same-key duplicate, not only the per-item
result payload returned to the caller, since the two can disagree.

See `crates/animus-cp-data/src/lib.rs`'s `KvCommand::KindEvalBatch` apply
arm and its own doc for the concrete fix, and `crates/animus-cp-data/
tests/kind_eval.rs::kind_eval_batch_a_same_key_duplicate_observes_the_
earlier_items_write` for the regression this exact bug failed before the
fix (the test's own `old`/`new` assertions passed; only the final
persisted base-row assertion caught it).
