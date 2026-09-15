# A batched fast path plus an unbatched incremental path over the same data is a performance bug waiting for its first big input — the incremental one silently costs one round trip per ITEM where its sibling costs one per MEGABYTE

**A batched fast path plus an unbatched incremental path over the same
data is a performance bug waiting for its first big input — the
incremental one silently costs one round trip per ITEM where its sibling
costs one per MEGABYTE** (ADR 0050's split build, 2026-08-19). The bulk
copy pass batched rows into 256 KB `SeedBatch` chunks; the tail pass that
chases writes arriving during the copy called the very same `ship()`
helper *inside its per-dirty-unit loop*, so every partition key bought a
full consensus round + apply-confirm (plus a forwarded hop for an
off-node child). Both paths looked correct and shared the same primitive
— the batching lived in the caller, and only one caller did it. Made
vastly worse by a second, independent conservatism: the tail's watermark
started at 0, so its FIRST pass classified every change record in the log
as dirty and re-shipped the whole table one key at a time, every merge an
idempotent no-op. Together: on a 20,000-row split, ~6,000 no-op Raft
entries per child and ~85% of the build's wall clock spent re-copying
data it already had — while the children's key counts sat visibly flat.
**The generalizable rules.** (1) When one loop batches and a sibling loop
over the same rows doesn't, that asymmetry is the bug — an accumulate-
and-flush-on-budget shape is usually a few lines and needs no semantic
argument, because an idempotent, versioned batch doesn't care where the
chunk boundaries fall. (2) A "safe" zero/empty starting watermark is not
free when a cheap, *sound* starting value is available from a pass the
code already makes: this one was recoverable from the same pre-bulk
read that already computed the version floor, under the identical
monotonicity argument. Ask what the conservative default actually costs
on the first large input, not whether it's correct. **The diagnostic
that made it obvious in minutes**: one consensus entry == one Raft log
index, so a receiver's own `commit_index` growth divided by rows
received IS the effective batch size — visible from `/admin/raftkv`
with no instrumentation, and it turned "the split feels slow" into
"6,000 entries moved 0 rows." That ratio is now the regression's
assertion, too: an entry-count budget catches a re-introduced per-row
ship where a wall-clock assertion would just go flaky.
(`crates/animusd/src/index_drain.rs::tail_pass`, `split_driver_tick`.)
