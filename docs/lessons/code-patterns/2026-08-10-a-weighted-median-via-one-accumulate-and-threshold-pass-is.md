# A "weighted median via one accumulate-and-threshold pass" is only correct when no single item can dominate half the total weight — once one can, scan every achievable cut point and pick the closest to half, don't commit to whichever side a running sum happens to cross the threshold on.

**A "weighted median via one accumulate-and-threshold pass" is only correct
when no single item can dominate half the total weight — once one can,
scan every achievable cut point and pick the closest to half, don't commit
to whichever side a running sum happens to cross the threshold on.**
Building the byte-weighted split point for ADR 0034's byte-based
auto-split (replacing the plain positional median with one that bisects a
materialized tablet's *bytes*, not its key count, under skewed value
sizes), the obvious-looking first implementation walked pairs in order,
accumulated a running byte total, and returned the first key at which the
running total reached half the whole. That is subtly wrong whenever one
key's own value is a large fraction of the total, because it commits to
the *first* crossing instead of comparing it against the *next* candidate
cut: 20 tiny keys totaling 100 bytes, then two huge keys y0/y1 of ~10,000
bytes each (total 20,104, half 10,052) — the naive walk returns y0 as the
split key the instant the running total (100 + y0's ~10,002) first crosses
half, giving a 100-byte/20,004-byte split (the 20 tiny keys vs. both huge
ones); but the *very next* candidate cut — after y0 instead of before it —
gives 10,102/10,002 (the tiny keys + y0, vs. y1 alone), far closer to
even. The naive walk can never find this, because once it has returned at
the first crossing it never looks at the next candidate to see if it's
actually closer. The fix scans every
achievable interior cut point (a key boundary, since a key's own bytes can
never be split) and keeps whichever prefix sum is closest to half — the
best any key-boundary split can do, and a strict improvement discovered
only by writing a unit test with a deliberately skewed distribution and
checking both sides' actual byte shares, not just "did a split happen."
(`animusd::byte_weighted_median`; `auto_split_median_tests`.)
