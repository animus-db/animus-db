# A per-request deep clone hidden behind an innocuous accessor is a cost to design around with a cheap invalidated flag, not to accept as "already paid elsewhere" (ADR 0065, W-08 step 5)

Step 4's fix above (the previous entry) was the right correctness call —
dropping the single-layer fast path entirely — but its own justification
("the modest, already-paid-elsewhere cost of one cached local clone... the
identical cost the surrounding write/read path already pays for routing")
undersold what `effective_metadata()` actually does: it locks a `Mutex`
and deep-clones the **entire** `Metadata` — the whole tablet map, the
whole schema catalog, the whole backup catalog — not a cheap `Arc` bump.
"Every other call site on this path already pays this" is true, but it
doesn't make the cost free to add to a THIRD call site (the throttle
check) that used to cost nothing at all when unconfigured; it just means
the new cost was already being paid for a different reason on the same
request, which is a justification for tolerating it briefly, not a reason
to stop looking for a cheaper alternative. On a cluster that configures no
throttling anywhere — the default, and likely the overwhelming majority of
real deployments — step 4 turned "check two atomics, return" into "clone
the whole metadata graph, then check two atomics, return" on every raw
write and every read.

The fix (step 5) was not a return to the single-layer peek step 4
correctly killed, but a proper **invalidated flag** covering both
configuration layers at once: `ClientCtx::any_table_throughput`, a plain
`AtomicBool` recomputed at the handful of points this node actually
observes a schema-catalog change (construction, the metadata-watch tick,
and immediately after this node's own DDL commit) rather than read fresh
on every request. Combined with the pre-existing lock-free
`throttle_defaults` peek, this restores the "no lock, no clone" fast path
for the truly common case while staying correct for a per-table override
with no cluster default — the exact case step 4 exists to catch. The
`Metadata` fetch still happens, but only on the request path that could
possibly need it.

**General form**: when a correctness fix removes a fast path because a
cheap check can no longer see everything that needs seeing, look for a
cheap *invalidated* check that can, before accepting the expensive
fallback as the new steady state — especially when the expensive call
sits behind an accessor name (`effective_metadata()`) that reads like a
cache hit and doesn't advertise the lock+clone underneath, and especially
when "something else on this path already pays this cost" is offered as
the reason not to look further. A flag recomputed at the write side (here:
construction, the metadata-watch loop, and the DDL handler's own
post-commit point) is usually cheap to add once the read side already has
a natural "is anything configured at all" gate to attach it to, and it
generalizes past this specific ADR: any per-request hot-path check that
degrades from "cheap local read" to "acquire a lock and clone a large
structure" the moment a second configuration source is added is a
candidate for this pattern, not just this one.
