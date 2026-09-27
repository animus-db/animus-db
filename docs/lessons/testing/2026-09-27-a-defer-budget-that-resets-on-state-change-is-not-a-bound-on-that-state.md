# A budget that resets on state change is not a bound on that state — measure restarts, not just per-offset resends

**Context**: `animus-cp-data`'s `apply_and_compact` deferred a
threshold-triggered compaction while a peer's chunked `InstallSnapshot`
transfer was genuinely in flight (issues #532/#537), but forced the base
forward regardless of progress once `behind` reached a small, fixed ceiling
(`COMPACT_DEFER_CEILING`, `COMPACT_THRESHOLD * 8`). `RaftCore::snapshot_upto`
unconditionally invalidates a peer's in-flight transfer every time the base
moves — required for correctness against a lazily-built image — so crossing
that ceiling meant "restart this peer's transfer from chunk 0," no matter how
much real progress it had just made.

**What went wrong**: under sustained writes fast enough relative to a
slow/contended peer (a write roughly every millisecond against a peer with a
~200ms disk round trip — the field shape: many hosted tablet groups
contending for one node's single-threaded consensus loop), `behind` re-crossed
the 512-entry ceiling in well under a second, far faster than a real
multi-chunk transfer to that peer could land. The result was a tight,
self-sustaining restart cycle: tens of thousands of `InstallSnapshot` chunk
ships per node over a couple of minutes, zero completed installs, a learner's
`match_index` pinned for the entire run (PR #1047).

**The generalizable lesson**: a fixed ceiling on some accumulating quantity
(`behind`, here) that gets RESET every time the thing it is meant to bound
also gets invalidated is not actually a bound on "how long has this been
going on" — it is only a bound on "how far can the quantity grow between
resets." Each reset restarts the accumulation from zero, so the ceiling can
be crossed, and the reset re-triggered, arbitrarily often — the observable
symptom is a HIGH RATE of resets, not a large value of the bounded quantity
at any single instant (which is exactly what makes it easy to miss in a
point-in-time investigation or a metric that only tracks the quantity itself,
never how often it got reset). The fix was not to raise the ceiling alone —
though it was also raised, 8x, as a genuine last-resort WAL-retention bound —
but to demote it from *the* signal deciding "has this stalled" to a pure
emergency backstop, and use an already-correct-for-the-question companion
(`COMPACT_DEFER_IDLE_CEILING`, idle time since the last genuine forward
progress) as the actual policy. "Is this slow but advancing" and "is this
stalled" are different questions; a single `behind`-sized ceiling can only
honestly answer the second one if it is set so high it stops being useful for
bounding worst-case retention in the pathological case, or so low it starts
punishing ordinary slow-but-live transfers — the fix needed both a real idle
signal AND a much higher pure-emergency ceiling, not a single number doing
both jobs.

**The testing lesson**: the pre-fix code had no direct measurement of "how
often did we force a still-in-flight transfer out" — only chunk-ships and
completed-installs, which make the flood visible only by RATIO, after the
fact, and only if a test happens to run long enough to accumulate a lot of
either. A restart is a discrete, nameable event; count it directly
(`Metric::CpSnapshotTransferRestarts`) rather than inferring it from a ratio
of two other counters. Once you can count restarts directly, "many restarts,
few or no installs" becomes a one-line assertion instead of an eyeballed
ratio threshold.

**The fix-validation lesson**: fixing this drifted `apply_and_compact`'s own
defer policy to be intentionally MORE PATIENT — an existing, unrelated
sibling test (`learner_catchup_under_load.rs`) that had been tuned against
the OLD, less patient policy started failing, not because of a correctness
regression but because its own drain budget was implicitly calibrated to how
much uncompacted log the old policy would ever let accumulate. Any change to
a defer/backoff gate's own trigger conditions can invalidate the *tuning* of
every other test that exercises the same mechanism, even tests nothing about
the change touches directly — re-run the crate's full test suite (not just
the new regression) before calling a defer-policy fix done, and expect to
retune, not just re-pass, its siblings.
