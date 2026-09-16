# When mirroring a fix onto a *sibling* subsystem, assess honestly — the sibling may have a *different-shaped* version of the hazard, or a bounded one not worth the same risky refactor.

**When mirroring a fix onto a *sibling* subsystem, assess honestly — the sibling
may have a *different-shaped* version of the hazard, or a bounded one not worth the
same risky refactor.** PR #16 moved CP-data's async **engine apply + compaction**
off its Raft loop (a >150ms self-sustaining stall). The control plane applies its
state machine **in-core, synchronously** — no async apply to move — so its only
loop-blocking O(state) work is snapshot-shipping (fixed above, cheaply) and the
compaction WAL-rewrite serialize. The latter is a *single* stall (~50ms at ~1MB,
~120ms at ~3MB), under the election timeout at realistic scale and **not**
self-sustaining (once per 64 applied entries). Moving it fully off the loop would
couple the install→WAL-rewrite ordering into a second task on the most
safety-critical Raft (real risk) for a bounded, rare, extreme-scale stall — so it
was **measured, documented, and deferred**, not force-fit. A well-reasoned "the
sibling's hazard is smaller; here's the measurement" is a valid outcome.
