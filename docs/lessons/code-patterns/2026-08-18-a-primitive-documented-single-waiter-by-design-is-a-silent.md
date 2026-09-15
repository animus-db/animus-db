# A primitive documented "single-waiter by design" is a silent lost-wakeup hazard waiting for its second consumer — audit every consumer before sharing its handle, or better, make it multi-waiter the moment a second one shows up

**A primitive documented "single-waiter by design" is a silent
lost-wakeup hazard waiting for its second consumer — audit every consumer
before sharing its handle, or better, make it multi-waiter the moment a
second one shows up** (issue #276, 2026-08-18). `animus-control`'s
`MetadataWatch` (ADR 0031) started as a single `AtomicWaker`-backed
watermark, correct for its one designed caller (the per-node reconciler)
and documented as such on the type and in the ADR. ADR 0035 PR5 later
handed the very same handle to a second, independent concurrent consumer
— a combined-mode node's `WatchMetadata` RPC long-poll — without anyone
re-checking the single-waiter contract. `AtomicWaker::register` silently
*evicts* whatever waker was previously registered rather than erroring or
queuing, so the reconciler loop's own periodic re-registration (every
`RECONCILE_FALLBACK_INTERVAL`) would evict a parked long-poll's waker; the
evicted waiter never got woken by the real commit and only ever resolved
via its own independent fallback timeout (8s). The result reads as
**bounded-but-large latency**, not a crash or a wrong answer — exactly
the shape that gets misread as scheduler contention or CI runner
slowness instead of a lost wakeup, because the symptom (a slow-but-legal
reply) has its own innocent-looking explanation and the fallback timeout
makes it *look* like the safety net working as intended. Fix: made
`MetadataWatch` genuinely multi-waiter (a `Mutex<BTreeMap<u64, Waker>>`
slot registry, one slot per parked `changed()` future, removed on
`Drop`) instead of trying to re-establish single-consumer discipline by
convention or a doc comment, since the next handle-sharing change would
just break it again the same way. General form: when a change hands an
existing "intentionally single-waiter" handle to any additional caller —
even one that looks read-only, even one added for an unrelated feature —
grep every existing consumer of that handle first; if more than one
concurrent waiter is now possible, the primitive itself needs to become
multi-waiter, not merely re-documented. (`crates/animus-control/src/
node.rs`, `crates/animusd/src/lib.rs::watch_metadata`.)
