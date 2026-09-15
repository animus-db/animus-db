# A frozen corpus is broad but shallow — one cell × one seed misses schedule-dependent bugs; scale *depth* (seeds/cell), env-gated and tiered.

**A frozen corpus is broad but shallow — one cell × one seed misses
schedule-dependent bugs; scale *depth* (seeds/cell), env-gated and tiered.** The
119-cell corpus explored each structural configuration down a single
name-hashed interleaving; multiplying seeds per cell
(`ANIMUS_CORPUS_SEEDS=K`, default 1, nightly 40) is what surfaced the
frontier-read unsoundness above on the *first* deep run. Keep variant 0 = the
canonical frozen name+seed (so `K=1` is byte-identical and no regression seed
moves) and `_sNN`-suffix the rest; gate the cost so default `cargo test` stays at
the frozen base while the deep tier (`ANIMUS_CORPUS_FULL=1` too) runs in a nightly
CI job, not per-push. (ADR 0014 coverage-expansion increment.)
