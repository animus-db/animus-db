# A change-log consumer's resume cursor must be a commit-order (HLC) watermark, never a key-position cursor

**A change-log consumer's resume cursor must be a commit-order (HLC)
watermark, never a key-position cursor** (ADR 0050 rung 4, the
split-build tail). `pending_changes`' key order is prefix-then-HLC, NOT
commit order — a later write to a *lower* prefix inserts *below* any
key-position cursor and is skipped forever. The sealer learned this once
(its load-bearing re-sort in `seal_now`, recorded only as a code
comment); the split-build driver re-made the identical mistake with a
"resume after the last key I saw" cursor, caught red by its own e2e
(`split_build.rs`, 4 of 16 racing writes silently missing while the
build reported converged). Within one tablet, HLC order IS commit order
(`assert_ts_monotonic`), so filtering the scan by a packed-HLC watermark
(the key's own trailing 8 bytes) is complete where any key cursor is
not. Advance the watermark only after the tick's work fully succeeds, or
a failed ship loses its dirty set. General form: before giving any
key-ordered scan a positional resume cursor, ask what order NEW entries
arrive in — if insertion order ≠ scan order, a positional cursor is a
silent-loss bug.
