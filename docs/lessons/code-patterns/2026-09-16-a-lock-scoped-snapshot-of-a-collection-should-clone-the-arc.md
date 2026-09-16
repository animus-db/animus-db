# A lock-scoped snapshot of a collection should clone the `Arc` around it, not the collection itself — even when every element is already cheap to clone

**Making each element of a `Vec` cheap to clone (an `Arc`-backed struct) does
not make cloning the *whole* `Vec` cheap — the container itself is still one
heap allocation plus N atomic refcount bumps, and if that clone happens under
a lock (to build a point-in-time snapshot a caller then reads lock-free), it
is O(N) work done while blocking every other locker.** `animus-storage`'s
`LsmEngine::Inner::readers: Vec<SsTableReader>` was documented as
intentionally cheap per-element ("each holds only metadata + the block index
in memory"), and that was true — but three hot read paths
(`latest_version_of`/`read_at`/`merged_at`) each did `inner.readers.clone()`
under the lock on *every single get/scan* to get a stable snapshot they could
then iterate against disk I/O without holding the mutex. The per-element cost
being cheap hid the real cost: a fresh `Vec` allocation sized to the live
SSTable count, on every read, inside the critical section (issue #844).

**The fix is to wrap the collection itself in an `Arc`**
(`Arc<Vec<SsTableReader>>`), so the snapshot sites become a single O(1)
`Arc::clone` (one atomic increment, no allocation) instead of an O(N) `Vec`
clone. The two mutation sites (flush appending one reader, compaction
rebuilding the whole set) now build a fresh `Vec` and swap in a new `Arc`
rather than mutating the shared one in place — which is a second, easily
overlooked win: since the mutation no longer touches the `Vec` an
already-taken snapshot still points at, that snapshot is now provably
unaffected by a later flush/compaction (`Arc::ptr_eq` distinguishes the two
`Arc`s; the old snapshot's own length/contents don't change under it), which
used to only hold because the snapshot was already a fully independent
`Vec`. A mechanism test that takes a snapshot, forces the mutation, and
asserts (a) the new `Arc` is a different allocation and (b) the old
snapshot's own contents are untouched pins this directly — a wall-clock or
purely-functional test can't distinguish "still correct because nothing
raced" from "still correct because nothing *could* alias the old data even
if it raced".

**Generalizable check:** when a lock-scoped snapshot of a collection is taken
on every hot-path call so the actual work can happen lock-free, ask whether
the *collection itself* is cheap to clone, not just its elements — a `Vec`,
`BTreeMap`, or similar container clone is never O(1) regardless of what's
inside it. If the collection is read far more often than it's mutated (the
common shape for this kind of snapshot-then-read-lock-free pattern), wrap it
in an `Arc` and have every mutator install a fresh one rather than editing
the shared container in place; every existing reader keeps working unchanged
(`Arc<Vec<T>>` derefs to `&[T]`, so `.iter()`/`.iter().rev()`/indexing are
untouched — only a bare `for x in &collection` needs `.iter()` instead,
since a reference to an `Arc`-wrapped container isn't itself
`IntoIterator`).
