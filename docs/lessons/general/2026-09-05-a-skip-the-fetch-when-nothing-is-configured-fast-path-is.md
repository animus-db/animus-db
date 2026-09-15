# A "skip the fetch when nothing is configured" fast path is only sound while there is exactly one configuration layer to check (ADR 0065, W-08 steps 3→4)

Step 3's `write_path.rs`/`read_path.rs` throttle checks peeked
`ClientCtx::throttle_defaults` (a lock-free atomic pair) *before* ever
fetching `Metadata`, specifically to keep the overwhelmingly common
"nothing configured" case at "one `Option` check, no lock, no `BTreeMap`
lookup, no `Metadata` clone." That was correct when the cluster-wide
default was the *only* place a limit could live. Step 4 added a second,
independent configuration layer — a per-table `TableSchema.throughput`
override, which by design can throttle a table even when the cluster-wide
default is entirely unset (ADR 0065 §5(b): "a table with its own
`throughput` set ignores `ClusterSettings`' default entirely") — and the
old fast path could no longer see it: the peek only ever looked at the
cluster-wide layer, so a per-table override with no cluster default
configured would have been silently invisible to `write_path.rs`/
`read_path.rs` (though not to `kind_write_item_at_leader`/
`txn_stage_local`, which already had `Metadata` in hand for other reasons
and so already called `throttle_limits_for` — the two-choke-point design
this ADR's own Decision 2 calls out — meaning the gap was real but
inconsistent between enforcement points, itself a second, harder-to-spot
bug shape). The fix removes the peek entirely: `Metadata` is now fetched
unconditionally before checking the effective limit, accepting the modest,
already-paid-elsewhere cost of one cached local clone even on the fully
unconfigured path. **General form**: a hot-path optimization that special-
cases "nothing is configured" by checking only one of several possible
configuration sources is a correctness bug waiting for the next
configuration source to be added, not merely a missed optimization
opportunity — when a second layer is added to a "check cheaply, else do
the expensive thing" gate, re-examine every existing fast-path short
circuit built against the single-layer assumption, not just the new code
path being added.
