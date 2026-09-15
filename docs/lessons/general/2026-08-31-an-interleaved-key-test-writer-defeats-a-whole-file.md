# An interleaved-key test writer defeats a whole-file-assignment property it was meant to prove (ADR 0058 fork closed, `LsmEngine::clone_to_filtered`)

Writing the first `ProdEnv` concurrency regression for range-aware cloning
(`clone_to_filtered`'s whole-file assignment: a source SSTable wholly
outside a `keep` set is never linked into the target, but a table
straddling `keep`'s boundary is still linked whole — see
`animus-storage/CLAUDE.md`'s entry), the natural-looking first draft had
one writer alternate between an in-`keep` key (`a*`) and a dropped-range
key (`z*`) on every iteration, then asserted the racing clone never
carried a `z*` row. It failed immediately — not because the feature was
broken, but because the test's own writer defeated the property it meant
to isolate: `clone_to_filtered` calls the SAME unconditionally-draining
`flush()` `clone_to` always has (a no-op only while a concurrent apply is
mid-flight, never threshold-gated), so every round's internal flush drains
whatever the shared memtable currently holds — and with `a`/`z` writes
interleaved key-by-key, that memtable is *always* a mix of both, so the
resulting SSTable's own `[min_key, max_key]` almost always straddles
`keep`'s boundary and gets linked whole, `z*` rows included, by design
(the boundary-straddling case is correct and separately tested). The fix
was two SEPARATE single-writer scenarios — one writer entirely inside
`keep`, one entirely outside it — so a flush's own table range can never
straddle by construction, leaving a clean pass/fail signal regardless of
timing.

**General lesson: when a test's own workload can produce the SAME
structural shape (here, a straddling file) that the code under test
already handles as a *documented, correct* case, the property the test
meant to isolate is confounded, not merely harder to see** — the fix is
almost never a smarter assertion over the confounded run, it's removing
the confound from the workload itself (here: don't interleave what the
two branches of the very property being tested need to stay separable).
This generalizes beyond storage: any test asserting "X never happens" over
a workload that itself can legitimately produce the shape that makes X
correct needs to ask, before chasing the failure, whether the workload —
not the code — created that shape. A second, narrower gotcha the same
file surfaced: a genuinely leftover (still-memtable-resident, never
flushed) row at `clone_to_filtered` time can only arise from a writer
racing strictly *between* that internal `flush()` and the snapshot lock
acquisition — a window `SimEnv`'s non-yielding disk model cannot produce
at all (same root cause as `lsm_clone_concurrent.rs`'s own pre-existing
rationale for `clone_to`), so a memtable-filtering unit test belongs at
the extracted-pure-predicate level (`key_in_keep`, table-driven, no
engine) rather than as a `SimEnv` integration test reaching for a code
path it structurally cannot exercise.
