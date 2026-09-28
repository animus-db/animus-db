# A knob that only changes TIMING (chunk size) can turn an already-tolerated, already-explained metric into evidence of a real bug — recheck the mechanism, don't just re-tune the bound

**Context**: raising `SNAPSHOT_CHUNK_BYTES` from 1 KiB to 64 KiB (so a real
`InstallSnapshot` transfer completes in one or two round trips instead of
hundreds) turned `follower_aware_compaction.rs`'s own pinned-seed install
count from ~10 (comfortably inside its documented tolerance) to 149
(nearly 2.5x the bound). The test's own comment already named a known,
accepted cause for a *few* extra installs — issue #554's
`state_machine_behind`/`needs_snapshot` machinery occasionally re-entering
the snapshot path while a receiver's own apply task digests a prior
install. It would have been easy to read 149 as "that same tolerated thing,
just more of it because bigger chunks make transfers finish faster" and
either bump the bound or add a defer.

**What was actually true**: two, almost unrelated things were going on, and
the given hypothesis (a leader's own compaction advancing the log base past
a *retained* voter's `match_index`, bypassing `RaftCore::compaction_floor`
via the `image_needed`-triggered unclamped advance) accounted for only 2 of
the 149 counted installs. The other 141 were `Metric::CpSnapshotInstalls`
for the *exact same* `last_index` — a single genuine image, redundantly
reprocessed by `handle_install_snapshot`'s deliberate (and, for a DIFFERENT
scenario, correct) choice to fall through to full reassembly whenever
`state_machine_behind` is true, regardless of whether the offer is an exact
duplicate of what was just installed. The 1 KiB chunk size had been hiding
this: each redundant reprocessing round-tripped many small chunks, so far
fewer full cycles fit in the same test duration. The chunk-size bump didn't
create a new bug — it removed the disguise on an existing one, on the SAME
code path a same-day, unrelated fix (the redundant-snapshot-ack short-
circuit) had just widened.

**How this was caught rather than papered over**: instrumenting the ACTUAL
metric increment site (`record_kv_outbound`'s `Metric::CpSnapshotInstalls`
increment) with the `last_index` it was firing for, not just instrumenting
the mechanism the prior hypothesis pointed at. Grouping the 149 events by
`last_index` (`sort | uniq -c`) immediately showed 141 duplicates of one
value and 8 of another — a shape no amount of staring at `compaction_floor`
call sites alone would have revealed, because the given hypothesis's own
mechanism was real (confirmed present, twice) but not the dominant one.

**The generalizable lesson**: when a config knob that changes only *timing*
(a chunk size, a batch size, a poll interval — anything that doesn't touch
the actual state machine) turns a previously-tolerated, already-explained
measurement into a bound violation, resist the urge to treat it as "more of
the same, needs a bigger tolerance or a defer." A timing knob can't create
new causal mechanisms; it can only change how many times an EXISTING one
fires inside a fixed test duration, or how visible it is against another
existing one. Re-derive the count from first principles (instrument the
metric's own increment site with enough context to bucket by root cause —
here, the value the metric was counting, not just whether it fired) before
either loosening a bound or reaching for the first plausible existing
mechanism the test's own comments already name. The right fix here was
narrower and safer than either: a targeted duplicate-suppression check
(`RaftCore::last_installed_index`) that left the ACTUAL flagged mechanism
(the `image_needed` bypass) and the wipe-recovery case it must not disturb
both completely untouched, because the investigation didn't stop at "found
*a* mechanism that fits the hypothesis" once the numbers didn't add up.
