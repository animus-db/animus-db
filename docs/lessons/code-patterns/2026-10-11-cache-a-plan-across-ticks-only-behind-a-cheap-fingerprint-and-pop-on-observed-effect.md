# Cache a multi-step plan across ticks only behind an O(1) fingerprint of ALL its inputs, and advance the cursor on the OBSERVED effect, not on proposing

**Cache a multi-step plan across ticks only behind an O(1) fingerprint of ALL its
inputs, and advance the cursor on the OBSERVED effect, not on proposing.**
Making the control leader's rebalance O(1) amortized (issue #1192) looked like
"keep the batch plan, pop one move per tick". Three traps: (1) a content check
over the inputs is O(tablets) and defeats the point, so the state needs a
mutation counter (`Metadata::placement_rev`, bumped in `apply`, serde-skipped,
excluded from equality) that is deliberately coarser than needed -- a spurious
bump costs one rebuild, a missed one is a stale proposal; (2) popping the head
when it is proposed is wrong: a proposal can be lost or land late, and then the
plan runs ahead of reality (the uncached decision would re-propose the head), so
pop only when the view shows the head's exact effect (epoch + replicas) and the
counter advanced by exactly one; (3) the decision has inputs that are not in the
replicated state at all (a driver-local `recently_done` set, the Raft term) and
they must be in the fingerprint too. Prove it with a seeded property test that
asserts "cached answer == fresh uncached answer" every tick under random external
changes and lost/late/refused proposals, and mutation-check each fingerprint term
(removing either the rev or the `recently_done` term must make it fail). A counter
that restarts on decode/snapshot is fine only if the cache is also keyed by
something that changes whenever the value could be replaced (here the term).
(`animus-control::rebalance_cache`, ADR 0029's 2026-10-11 amendment.)
