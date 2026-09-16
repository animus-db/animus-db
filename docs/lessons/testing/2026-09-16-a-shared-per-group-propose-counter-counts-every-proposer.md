# A shared per-group propose counter counts every proposer, not just the one your test drove

Issue #974 closed a margin (`batch_write.rs::batched_write_beats_per_key`'s
`RETRY_MARGIN`) that issues #911/#967/#971 had left in place "until the real
source is found." Tracing every accepted propose (command kind, tablet
stream id, Raft index/term, and the call-site backtrace) reproduced the
9-for-8 symptom on the very first unloaded run, and it was not a duplicate
client propose at all: zero `"; retry"` results and zero superseded/no-op
confirms appeared anywhere in the trace. The ninth propose was a
`KindBatch` tombstone-delete of the per-key phase's own 200 change-log
marker rows, issued by the unrelated per-node `index_drain::trim_janitor`
background loop, landing — by ordinary tick-timing luck — inside the
batched phase's own before/after `/metrics` measurement window instead of
the per-key phase's.

**The generalizable lesson**: a metric defined as "every accepted propose
on this group's Raft log" (`Metric::CpProposalsAccepted`,
`animus-cp-data::record_propose`) is exactly that — every proposer sharing
the group's log shares the counter, including a periodic background
housekeeping loop that has nothing to do with the request a test is
measuring. A test that scrapes such a counter around a code path it drives
is implicitly assuming "nothing else proposes to this group during my
window," which is true only until some other subsystem starts sharing the
same tablet (here: a change-log/marker trim janitor that runs
unconditionally on every led tablet, even a plain table with no GSI/
stream/PITR consumer, since a marker record is never itself
consumer-visible and so is always immediately safe to reap). The fix is
not a wider tolerance band on the test's own assertion — a tolerance
absorbs the *symptom* without ever naming the *other proposer*, so it
can't tell "one extra harmless housekeeping propose" from "one genuine
duplicate client propose" if a real regression ever reintroduces the
latter. The fix is to make the *count itself* attributable: give the
non-client source its own metric, incremented at the one call site that
already knows a given propose is housekeeping rather than a reaction to a
request (never derivable from inside the shared propose plumbing itself,
which is generic over every caller by design), and have the test subtract
that delta before asserting.

**Investigation method note**: `#[track_caller]` plus
`std::backtrace::Backtrace::force_capture()` at a shared internal choke
point (here, the propose call inside `cp_kind_raw_local`) is a fast way to
find "who else calls this" in a real multi-task `ProdEnv` binary without
guessing from static analysis alone — several background loops
(`change_consumer_loop`'s five arms, the TTL reaper, the auto-split
trigger, the txn resolver) all funnel through the same few write
primitives a client request does, and only a live trace distinguishes them
cheaply. The bug reproduced on the very first unloaded, uncontended run
(not something that needed CPU load to surface) — a reminder that "hard to
reproduce" and "not yet instrumented" are often the same thing.
