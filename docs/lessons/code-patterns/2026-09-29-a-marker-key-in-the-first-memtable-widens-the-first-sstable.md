# A reserved-namespace marker written first widens the engine's first SSTable

**Context.** ADR 0073 Phase 0 (workstream C, layer 4) stamps every per-tablet engine with a
layout marker under `RESERVED_NAMESPACE` (`0x5F...`) as its very first write. That put the
key in the first memtable, so the first flushed SSTable spanned `[0x00 row..., 0x5F marker]`,
overlapping every kind scope's keep-range. `inplace_split_dead_space` (whole-file assignment
in `clone_to_filtered`) failed: the "wholly-left" table was linked into the right child.

**Lesson.** Whole-file SSTable pruning keys off each table's `[min_key, max_key]`, so *any*
engine-global marker that lands in a memtable with kind-scoped rows widens the flushed table up
to the marker namespace. A first-write marker guarantees it for table #1. Do not bend the test
(`flush_now()` in the test only hides a real production regression) and do not accept it as a
"bounded cost": **isolate the marker with a flush at stamp time** — the reconciler calls
`EngineFactory::flush_engine` right after stamping (and after a split child's trim batch), so the
marker gets its own SSTable before any row arrives. `StorageEngine` has no flush (`flush_now` is
inherent on `LsmEngine`), so this is a factory hook; every `LsmEngine`-backed factory must
override its default no-op. The pre-existing applied/hwm markers are written only at
compaction/snapshot-install, which is why the dead-space test passed before: check *when* an
existing marker is written before assuming a new one is equivalent. When adding a new
engine-global write, ask what it does to table key ranges, not just whether scans skip it.

**Also.** A refusal for "this engine's format is not one I may touch" must never share an
error path with "this engine failed to open" when the latter destroys and rebuilds.
