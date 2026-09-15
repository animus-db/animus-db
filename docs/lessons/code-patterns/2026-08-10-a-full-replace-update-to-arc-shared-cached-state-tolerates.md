# A "full replace" update to `Arc`-shared cached state tolerates a bare monotonic-watermark check-then-mutate race; an "apply an incremental delta onto the existing cache" update to the *same* shared state does not, and reuses the same guard incorrectly if you don't also make it atomic.

**A "full replace" update to `Arc`-shared cached state tolerates a bare
monotonic-watermark check-then-mutate race; an "apply an incremental delta
onto the existing cache" update to the *same* shared state does not, and
reuses the same guard incorrectly if you don't also make it atomic.**
`animusd`'s `RemoteControlClient` (`control_handle.rs`) is shared between a
background watch loop and any concurrent `metadata_fresh()` caller. Its
pre-existing `observe()` (a full `Metadata` replace) read
`self.watch.latest()`, decided whether to overwrite, then wrote and bumped
the watch — three separate steps, but safe anyway because a full replace is
order-independent modulo the monotonic-watermark guard: two concurrent
replaces racing only risk a *stale* value winning temporarily, never a
*corrupted* one, and the next reply self-heals it. ADR 0038 PR5 added
`observe_delta()`, which installs a batch of `KeyWrite`s onto the *existing*
cached value — a genuinely sequential operation that is only correct if the
cache is exactly at the delta's own `last_seen` basis at the moment of
application. Copying `observe()`'s three-separate-steps shape for
`observe_delta()` would have created a real corruption window: a
concurrent full `observe()` could advance the mirror between this method's
watermark check and its mutation, and the delta would then apply on top of
the *wrong* base, silently producing an internally-inconsistent `Metadata`
no later reply would ever detect or fix (unlike the full-replace race,
this one doesn't self-heal). The fix was to make **both** methods acquire
the mirror's lock first and do the check-decide-mutate-bump sequence while
holding it, so the two can never interleave — a case where hardening one
method's atomicity is forced by what gets *added next to it*, not by any
bug in the original method taken alone. **When adding a second mutator to
`Arc`-shared cached state that already has one "eventually consistent,
order-tolerant" writer, check whether the new one's update rule is actually
order-*dependent* — if so, the existing writer's looser discipline has to
tighten to match, not just the new one.
