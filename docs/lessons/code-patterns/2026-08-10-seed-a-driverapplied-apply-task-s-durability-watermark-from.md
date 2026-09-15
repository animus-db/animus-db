# Seed a `DRIVER_APPLIED` apply task's durability watermark from the *engine's own* persisted marker, never from the recovered core's `last_applied()` — they can legitimately disagree, and only one of them is actually correct after a real crash.

**Seed a `DRIVER_APPLIED` apply task's durability watermark from the
*engine's own* persisted marker, never from the recovered core's
`last_applied()` — they can legitimately disagree, and only one of them is
actually correct after a real crash.** After `RaftCore::recovered()`, a
core's `last_applied()` reflects only the last **compacted** snapshot
base; the engine's own watermark (written every apply pass, far more
often than compaction runs) can already be well *ahead* of it. Seeding
from the core's `last_applied()` (mirroring `animus-cp-data`'s own
`engine_applied.store(core.last_applied())`, which is *correct* there only
because the data plane's per-key merges are independently idempotent under
replay) would, for a state machine like `Metadata` whose commands aren't
all trivially safe to reapply twice (counters, nonce ledgers, epoch-CAS —
each *happens* to be idempotent by its own construction, but relying on
every future command variant continuing to be is fragile), silently
redeliver an already-engine-durable prefix on top of a freshly
engine-rebuilt cache. The robust fix: read the engine's own watermark key
at the apply task's startup, rebuild the cache from *that* index, and
**filter drained effects by `index > watermark`** rather than trusting
that redelivering the whole tail is harmless. Regression:
`animus-control::node.rs`'s own `#[cfg(test)]`
`apply_and_compact_replays_only_the_tail_beyond_the_watermark` /
`..._is_a_no_op_when_the_watermark_already_covers_everything` — white-box
tests that drive the private apply function directly with a hand-seeded
watermark, precisely and deterministically, rather than trying to time a
real crash to land at an exact index.
