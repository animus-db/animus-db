# An atomic multi-row Raft-entry batching optimization cannot offer finer admission granularity than "the whole entry" — a real, user-visible behavior difference, not a bug to fix (ADR 0065, W-08 step 3)

The ADR 0049 fast marker arm commits an entire tablet's share of a
`BatchWriteItem` call as ONE `KindBatch` Raft entry, specifically because
that batching is what gives a plain unindexed/unstreamed table its
throughput (N sequential fsync round trips would otherwise serialize a
whole batch). Wiring per-table throttling into this path found — via a
real integration test, not by inspection — that admission control
necessarily inherits the same atomicity: `throttle_check_write_raw` checks
and charges the WHOLE per-tablet group's cost against the bucket in one
call, so a partially-exhausted budget either admits or refuses every
request in that tablet's group together, never a subset. This is not a bug
to work around; it is the direct, structural consequence of the same
design choice that gives the fast arm its throughput, and "fix" it would
mean either losing that atomicity (re-fragmenting the batch into per-item
entries, undoing the throughput win it exists for) or charging an
estimated per-item share speculatively (which cannot be made to agree with
what the entry, once it commits, actually costs). The integration test
covering `BatchWriteItem`'s true per-item throttle granularity had to
target a **streamed** table instead — streamed/indexed writes already go
through the per-item evaluate-at-leader funnel for unrelated correctness
reasons (ADR 0054), so genuine per-item admission control falls out of
that path for free, while a plain table's own regression proves only the
coarser per-tablet-group behavior. **General form**: before writing a test
that assumes an admission-control (or any per-request) decision is made at
per-item granularity, check whether the write path it rides was already
batched into one atomic unit for a *different*, unrelated reason (here,
throughput) — the atomicity is not negotiable without undoing the reason
the batching exists, and the right fix is usually "pick a different
existing code path to test the finer-grained behavior," not "make this one
path finer-grained."
