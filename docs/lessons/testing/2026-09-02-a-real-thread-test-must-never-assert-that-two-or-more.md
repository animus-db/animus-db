# A real-thread test must never assert that two or more independent requests land inside (or outside) a fixed real-time window — assert the property the mechanism actually guarantees instead (2026-09-02, issue #570).

**A real-thread test must never assert that two or more independent
requests land inside (or outside) a fixed real-time window — assert the
property the mechanism actually guarantees instead (2026-09-02, issue
#570).** `index_drain.rs`'s `stream_sealer_tests::
age_trigger_seals_a_quiet_table` set a 300ms `--stream-seal-age`, issued
two independent `PutItem` calls back to back, waited for exactly one
`stream_shards` catalog row to appear, and asserted that row's own
`count == 2` — which only holds if both writes land inside the same
300ms age window as each other. On a loaded runner the *first* write's
own latency (initial leader election, first tablet host, the stream's
first change) can by itself exceed 300ms, so the age trigger seals shard
0 with `count == 1` before the second write ever lands; the second write
then belongs to shard 1, and the assertion panics `left: 1, right: 2`.
The age trigger fired exactly as designed — the bug was in the test's
assumption about the *timing* of two independent requests, not in the
mechanism under test. This is the same "fixed-deadline assert on an
eventual property" shape this log already warns about (see the Testing
section's standing instruction above and the issue #559/#561 entries
below), specialized to a case where the "deadline" is a real seal-age
knob rather than a poll timeout: the fix is the same either way — poll
for the property, never assert a single snapshot taken at an assumed
moment. **Fix**: converge (`await_true`, already the module's own
converged-or-timeout helper) on the **total** `count` summed across
every `(tablet, _)` row in `stream_shards` reaching the number of writes
issued, then assert only that at least one seal happened (the module's
size threshold is set to never trip in this test, so any seal at all is
necessarily by age) — this proves "a quiet table's tail gets sealed by
age, covering every write," which is what the test actually exists to
show, regardless of whether that takes one seal or several. Confirmed
the failure signature by forcing it in a scratch copy (a sleep between
the two writes long enough to guarantee the first write's own seal fires
before the second lands) before touching the real fix — reproduced the
exact `left: 1, right: 2` panic from the issue, then discarded the
forcing per this log's own soundness-of-repro discipline (issue #555's
entry above). It did **not** reproduce naturally in 100 unmodified loop
iterations under 3×`yes` CPU load on a 4-core box — this repro's own
window is evidently narrower than the issue's own CI runner hit, the
same "low natural flake rate, real bug anyway" outcome issue #559's
entry recorded for a related test. Every sibling test in the same module
(21 real-thread tests total) was audited for the identical shape; only
this one had it — every other test either measures a genuinely negative
property (no writes at all, or a real backlog kept far below a
deliberately huge `seal_age`/`seal_bytes` for the whole sleep window, so
no amount of runner slowness can trip it), forces its own seal
deterministically (each write alone exceeds a tiny size threshold, or a
`StreamSpecification` disable forces an immediate final seal covering
whatever is currently pending, sequenced strictly after every prior
write already returned 200), or persists a state over a sleep window
many multiples wider than the knob that could flip it (the quiesce-veto
test's 2s check against a 300ms `quiesce_after`). **This was the first
failure the sharded, no-retry `prod-liveness` job (PR #566) surfaced on
its very first run** — previously this exact defect would have been
silently absorbed by the retry loop `ci-speed-3-no-retry` removed (see
that entry above); removing the retry is what made the bug visible
instead of invisible-but-still-there.
