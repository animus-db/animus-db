# A cache-fed fence cannot protect against the cache itself being stale — only the state machine's own `apply` sees the true, current state, and only it can arbitrate.

**A cache-fed fence cannot protect against the cache itself being
stale — only the state machine's own `apply` sees the true, current
state, and only it can arbitrate.** The direct follow-up to the lesson
above, found while empirically verifying that fix in production: the
range fence just described reads `ctx.effective_metadata()`, which for
every real deployment shape resolves to `RaftNode::metadata()` — a clone
of a cache an *async apply task* (ADR 0038) populates, decoupled from the
Raft log's own commit index. That cache can lag the true, already-
committed `SplitTablet` on the SAME node running the seal arm,
independent of whether the physical scope has narrowed — and since the
tablet-host reconciler that drives `narrow_scope` reads the identical
cache to decide when to act, the fence and the physical scope it exists
to backstop can BOTH be consulting the same stale snapshot at once. In
that specific sub-window the fence provides zero protection, because
it's checking against the exact source that let the physical scope stay
wide in the first place — confirmed empirically, not just by reasoning:
the real D8 e2e test still showed the duplication after the fence alone
shipped, at a rate not clearly distinguishable from noise across two
matched before/after samples (do the comparison, don't eyeball "seems
better"). **Fixed 2026-08-15**: `MetaCommand::SealStreamShard` gained an
`expected_range` stamp, checked by `Metadata::apply` itself (mirroring
the existing epoch-CAS idiom on `SplitTablet`/`CasTabletReplicas`) —
since `apply` runs strictly sequentially in Raft commit order, a
proposal fenced against a range a racing, earlier-committed `SplitTablet`
has already superseded is rejected regardless of any node's own cache
freshness; no read, however fresh, can substitute for checking against
the state actually being mutated. General form: **when a proposal-side
check reads any replicated/cached state to decide "is this still
valid," ask whether that same state can lag the fact the check exists to
catch — if so, the check is a useful first-line filter (cheaper, catches
the common case, avoids wasted round trips) but never the authoritative
backstop; that has to live at the point of actual, sequential commit
(a state-machine `apply` arm, a CAS), not a read anywhere upstream of
it.** A residual can remain even after this: `hot_read`'s own open-tail
serve has no apply-time backstop available at all (reads don't go
through `apply`), so it stays a first-line-only, permanently incomplete
fence — accepted, deferred to the ADR 0044 quiescence work, rather than
chased with more read-side cleverness that would just move the same
staleness window somewhere else.
