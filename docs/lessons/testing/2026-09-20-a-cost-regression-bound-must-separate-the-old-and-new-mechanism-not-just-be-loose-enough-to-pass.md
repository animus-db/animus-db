# A cost-regression bound must separate the old and new mechanism, not just be loose enough to pass

Found while rewriting `crates/animusd/src/sim_cluster_seed_latency.rs` for
issue #996 layer 2 (batching a `BatchWriteItem` chunk into one
`KvCommand::KindEvalBatch` Raft entry per tablet instead of one `KindEval`
entry per item).

The existing regression asserted `elapsed200 < sequential_extrapolation /
4` (a 25% bound). That bound had been sized, at some earlier revision, to
comfortably pass the *then-current* mechanism — but "comfortably passes
the current mechanism" and "would fail if the mechanism regressed back to
its old, slower shape" are different properties, and only the second one
makes the assertion a real regression test.

Measured by hand, both ways, on this exact scenario:

- The new batched arm: `ratio ≈ 0.50%–0.85%`.
- The old arm temporarily reverted to the pre-fix sequential per-item
  loop: `ratio ≈ 8.00%–8.50%`.

Both numbers are comfortably under a 25% bound. A silent regression from
the batched shape back to the sequential one — the exact defect class this
test exists to catch — would have kept the assertion green. The bound was
not a regression test at all; it was a liveness check that always passed.

**The fix is not "make the bound tighter" as a stylistic preference — it
is to actually measure both mechanisms and choose a bound that sits
between them with real margin on both sides**, then say so in the test's
own doc comment (including the measured numbers) so a future reader can
verify the divisor is still doing its job rather than having to
re-derive it from scratch. Here, reverting to sequential is cheap and
safe to do temporarily (single function, no persisted format, isolated by
a stacked commit that gets reverted before landing) — that is what makes
"measure red, then measure green, then pick the number in between" a
practical exercise rather than a theoretical one.

**Generalizes to any assertion of the shape `measured < some_extrapolation
/ N`, or any threshold copied forward from an earlier revision of the same
test without being re-derived against the new mechanism's own numbers.**
When rewriting a cost-model test alongside a performance-changing code
change:

1. Confirm you can cheaply produce both the old and new behavior (e.g. a
   throwaway revert of the just-written optimization).
2. Measure the actual ratio/latency/count under each.
3. Pick a bound with real margin on both sides of the two measurements —
   not merely "still under the old bound" or "half the old bound," either
   of which can still fail to separate two real mechanisms if the old
   bound was already loose.
4. Record the measured numbers in the test's own doc comment, and note
   explicitly that the old bound would not have distinguished the two
   mechanisms if that was in fact the case — the next person to touch this
   bound needs to know it was chosen for separation, not carried forward
   by habit.

A bound that passes for two mechanisms an order of magnitude apart is not
protecting anything; a green run of it proves nothing about which
mechanism is actually live.
