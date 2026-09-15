# Before claiming a parallelization's speedup, measure what fraction of the work you are actually parallelizing — "these two are independent, so it's ~2x" is a statement about the code shape, not about the clock

**Before claiming a parallelization's speedup, measure what fraction of
the work you are actually parallelizing — "these two are independent, so
it's ~2x" is a statement about the code shape, not about the clock**
(2026-08-19). The split build shipped to its two children serially; they
are two independent Raft groups at disjoint homes, so making the ships
concurrent looked like a clean ~2x on the copy, and that is what I told
the maintainer before measuring. Quiet-parent A/B at 20,000 rows, n=6 per
side: median 6.05s -> 5.25s, mean 6.13s -> 5.67s, stdev ~0.85s on both —
**a real direction, but inside one standard deviation, so not a
demonstrated speedup at all.** The cost model explains it exactly: the
ships were only ~1.2s of a ~6s build, so halving them buys ~0.6s, and
what actually dominates is three *full engine scans* of the tablet (the
version-floor pre-pass, the bulk scan, the final image). **The rules.**
(1) Estimate the serialized component's share of total time BEFORE
writing the parallel version; if you cannot, say "unknown" rather than
quoting a ratio derived from the shape. (2) Pick the benchmark that
isolates the phase you changed — the repo's existing split bench drives a
continuous writer, which pins every run to `SPLIT_MAX_TAIL_PASSES` and
made the change measure as *literally zero* (8.06s vs 8.08s); only a
quiet-parent run could see it at all. (3) n=1 is not a measurement when
the run-to-run spread is comparable to the effect: the same binary
produced 5.0s and 34.6s on consecutive runs until the concurrent-writer
confound was removed. (4) Ship the honest number, including when it
undercuts your own earlier estimate — the change here is still worth
making (it removes a structural serialization and its fault test covers a
previously untested cancellation path), but selling it as a 2x win would
have been false.
(`crates/animusd/src/index_drain.rs::ship_all`.)
