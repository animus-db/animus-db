# A `MemoryEngine`-only corpus cannot see a dependency on MVCC history

**Context.** The real-process chaos harness lost acknowledged list-append
writes on keys touched by an *aborted* cross-tablet transaction (about 2 runs
in 25, not reproducible from a process-level seed). Every simulation corpus,
including `txn_serializable.rs` at depth 40, was green.

**Root cause.** `TxnResolve`'s abort branch restored the pre-intent value with
`get_at(key, intent_version - 1)` and wrote a tombstone when that came back
empty. Two ordinary mechanisms empty it while the key still has a committed
value:

- `LsmEngine` compaction GC collapses versions below its floor
  (`max_version - tombstone_grace_versions`). The default grace is `1 << 20`
  *versions*; the CP plane's versions are packed HLC timestamps
  (`wall_ms << 20`), so the floor sits about one millisecond behind the newest
  write. The grace was sized for small logical counters and never revisited
  when versions became HLC-packed.
- `InstallSnapshot` images ship each key's latest record only, so a
  snapshot-caught-up follower has no history under a live intent at all — on
  `MemoryEngine` too.

`MemoryEngine` keeps every version forever, and the one test that shipped an
intent through a snapshot staged over a key with no prior value and then
committed. So no sim test could ever observe the dependency.

**Lessons.**
- **`get_at(v)` below the newest version is a request for history, and history
  is not a guaranteed property of the engine.** Before relying on it, name what
  holds it (a held `LsmSnapshot`), or carry the needed value forward yourself.
  Here the fix put the prior value inside the intent (ADR 0018's 2026-10-04
  amendment), which also covers the snapshot path that a GC hold would not.
- **A durable-engine bug class needs a durable-engine corpus cell.** The
  regression that reproduced the chaos finding deterministically was a new
  `txn_serializable.rs` cell on `LsmEngine<SimEnv>` with tiny
  flush/compaction thresholds and the *production* grace
  (`lsm_compaction_abandon_prepare`). Shrink sizes, never the grace, or the
  cell stops modeling production.
- **The engine-level repro alone was not enough evidence.** The GC behavior is
  by design for reads above the floor; the bug was the caller's assumption.
  Confirm at the layer that makes the wrong assumption (here a cp-data
  abort test over `LsmEngine`, plus a snapshot-follower test on `MemoryEngine`).
- **Check a test's precondition, not its name.** The snapshot test written to
  reproduce the follower variant first passed: since follower-aware
  compaction retention, a voter a few hundred entries behind catches up by
  log, not snapshot. It only failed once it asserted `CpSnapshotInstalls > 0`
  and kept the voter partitioned past `COMPACT_RETENTION_CAP_ENTRIES`. (The
  older `snapshot_catchup.rs` tests' comments still claim a snapshot catch-up
  their shape no longer forces.)
- **Units on a knob matter.** A "versions" grace means nothing until you know
  what a version is. When a version encoding changes, audit every knob
  expressed in version units.
