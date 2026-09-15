# A `ProdEnv` bench that needs a real, per-call I/O count (not an assumed one) can wrap the whole `Env` supertrait rather than touch production code (C-05 PR 1, `wal_fsync_bench.rs`)

Building the `SharedWal`-gating benchmark needed an honest answer to "how
many real `Disk::sync` calls did this concurrent burst actually cost,"
not the count a reader might assume from the code shape — `SharedWal`'s
own queue+leader-flush algorithm can, in principle, split a nominally
"one round" burst into more than one physical flush if not every
concurrent submitter has enqueued before the current leader task starts
draining (real thread-scheduling jitter, not a bug), so an assumed count
would have silently misreported the very number the bench exists to
measure honestly.

`ProdEnv` itself has no generic per-call I/O counter (unlike `SimEnv`'s
`DiskConfig`, which is a deliberate test-only fault/observation seam), and
`LsmEngine`'s own `wal_batch_sync_count()` is specific to that one engine,
not reusable here. Adding one to `ProdEnv` itself would be scope creep
onto every other `ProdEnv` caller in the workspace for a need scoped to
one bench.

**Fix**: a small `CountedEnv` wrapper, private to the bench file, that
implements the full `Env` supertrait bundle (`Clock`/`Rng`/`Network`/
`Disk`/`Spawner`) by delegating every method to a wrapped `ProdEnv`
verbatim — except `Disk::sync`, which increments an `Arc<AtomicU64>`
before delegating. Nine `Disk` methods plus a handful more across the
other four traits is more boilerplate than a single override, but it is
mechanical, needs no change to any production type, and the resulting
`CountedEnv` is a real `Env` — every scenario in the bench (including the
ones calling into `SharedWal`'s own unmodified, real API) runs exactly
the code path production would, just with one extra counter riding along.

**General form**: when a `ProdEnv`-driven test or bench needs to observe
a specific low-level I/O call's real frequency (not its logically-expected
frequency) and no existing seam already reports it, a thin whole-`Env`
delegating wrapper that overrides only the one method of interest is
cheaper and safer than adding an observability hook to `animus-env`
itself for a single caller's need — the wrapper is real `Env` by
construction (every supertrait is genuinely implemented), so nothing
downstream can tell the difference except the one counter being read.
