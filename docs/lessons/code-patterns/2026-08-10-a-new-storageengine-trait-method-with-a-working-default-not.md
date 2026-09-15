# A new `StorageEngine` trait method with a working default (not just a stub) lets every existing/future implementor answer immediately, and only the one backend that needs a cheaper path has to override it — the `merge_batch` precedent, reused.

**A new `StorageEngine` trait method with a working default (not just a
stub) lets every existing/future implementor answer immediately, and only
the one backend that needs a cheaper path has to override it — the
`merge_batch` precedent, reused.** Adding `approx_bytes_in_range` (ADR
0034) for the byte-based auto-split trigger, the default implementation is
simply *exact* (scan the range and sum key+value lengths) — correct for
`MemoryEngine` (and any future engine) for free, with no `Option`/`None`
fallback needed anywhere upstream. Only `LsmEngine` overrides it with a
cheap, non-materializing estimate from metadata it already holds
(memtable range-query + SSTable-overlap `file_size` sum), which is where
the actual "cheap, not exact" tradeoff belongs. This is strictly better
than the older sibling pattern (`CpGroup::approx_key_count`, LSM-only,
returns `None` on the memory backend) for any *new* per-engine estimate —
prefer "default = exact, override = cheap" over "default = absent" when
the exact computation is itself cheap enough for a non-hot-path backend.
Related: an unbounded-above logical range (`KeyRange.end: None`, the
common "one big not-yet-split tablet" case) must not degrade a scoped
estimate into an engine-wide scan just because the *logical* range has no
upper bound — `StorageScope::physical_bounds` computes a bounded
**physical** upper bound via the standard prefix-upper-bound trick
(increment the last non-`0xFF` byte of the scope's own key prefix) instead
of falling back to `entries()` the way the one-time `has_data` check
tolerates. A cheap per-tick gate and a one-time hosting-decision check can
reasonably make different cost tradeoffs for the same "unbounded range"
shape — check which one you're building before reusing the other's
fallback.
