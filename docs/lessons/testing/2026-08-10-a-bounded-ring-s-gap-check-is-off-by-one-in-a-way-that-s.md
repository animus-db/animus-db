# A bounded ring's "gap" check is off-by-one in a way that's easy to get backwards, and the wrong direction still passes a shallow test.

**A bounded ring's "gap" check is off-by-one in a way that's easy to get
backwards, and the wrong direction still passes a shallow test.** Building
ADR 0038 PR5's per-node `DeltaRing` (`animus-control/src/delta_ring.rs`):
the natural-seeming assertion "if an old entry got evicted, a caller who
needed it must fall back" is only true when the caller's `last_seen` is
*behind* the evicted entry's index — if `last_seen + 1` lands exactly on
the ring's current front (the caller's next-needed index is exactly the
oldest one still retained), that is full coverage, not a gap, even though
something *older* than the front was indeed evicted. A first draft test
asserted the wrong outcome for exactly this boundary case
(`writes_since(2, 3)` after indices 1 and 2 were evicted, front now at 3)
and initially failed against genuinely-correct implementation code — the
fix was the test's expectation, not the ring. **When testing a
"contiguous coverage" predicate over a bounded/evicting structure, write
out the boundary case (`last_seen + 1 == front.index`) as its own explicit
assertion with the reasoning spelled out in a comment** — it's the one a
reviewer (or a future edit) is most likely to get backwards, and a test
that only checks the interior cases won't catch a subtly-inverted
condition.
