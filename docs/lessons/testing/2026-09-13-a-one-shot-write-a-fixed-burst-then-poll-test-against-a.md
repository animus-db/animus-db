# A one-shot "write a fixed burst, then poll" test against a rate-EWMA trigger is racing two independently-clocked periodic loops, and the EWMA's decay is per-observation, not per unit time (2026-09-13, issue #867).

**A one-shot "write a fixed burst, then poll" test against a rate-EWMA
trigger is racing two independently-clocked periodic loops, and the
EWMA's decay is per-observation, not per unit time (2026-09-13, issue
#867).** `streams_e2e.rs::auto_split_change_rate_splits_a_high_churn_
streamed_table_never_a_plain_one` flaked once in CI — "hot_stream never
auto-split on its own change-append rate (timed out after 20s)" — with
no code on its path in the failing PR, then passed on later runs.
`ChangeRateTracker` (`animusd/src/lib.rs`) is fed by `change_consumer_
loop`'s `INDEX_DRAIN_INTERVAL` (200ms) tick and read by `auto_split_
loop`'s own, separately-clocked `AUTO_SPLIT_INTERVAL` (2s) sweep — two
independent timers, never phase-locked to each other or to a test's own
write loop. `RateSample::advance`'s EWMA decays the *previous* rate by a
fixed `1.0 - RATE_EWMA_ALPHA` factor on every observation *regardless of
how long that observation's own gap was* — elapsed time only scales the
instantaneous half of the blend, not the decay factor itself. The old
test wrote a fixed 60-item burst, THEN started polling — so by the time
any poll began, the burst was already over and the signal could only
ever decay further; whether the eventual split still happened came down
to whether some earlier `AUTO_SPLIT_INTERVAL` tick's phase had happened
to land while the EWMA was still hot. Pinned deterministically
(`rate_tracker_tests::change_rate_tracker_a_completed_burst_can_decay_
below_threshold_before_the_next_auto_split_sweep`, `animusd/src/
lib.rs`): the e2e fixture's own numbers (a ~130,000-byte burst landing
in one `INDEX_DRAIN_INTERVAL` tick, ~20x the 10,000 B/s threshold) still
decay below threshold within one `AUTO_SPLIT_INTERVAL`'s worth of
zero-growth ticks (`(0.7)^10 ≈ 0.028`). This is not a tracker or sweep
bug — a change-append rate legitimately stops being "high" once the
writes that made it high have stopped — it is the test asserting a
property ("this burst, however brief, will eventually be noticed") the
mechanism never promised. Fixed the same way as #580's lineage-scan-
target flake: target the property the mechanism actually guarantees
(a *sustained* high rate gets split) with a converged-or-timeout poll,
here by replacing the fixed burst with a continuous writer task that
keeps running for as long as either poll in the test is open, so every
sweep tick — whichever phase it lands on — observes a genuinely
still-ongoing rate rather than a stale, already-decaying one. **General
form**: before trusting a "burst then check" test against ANY EWMA/rate
trigger fed by one periodic producer and read by a differently-clocked
periodic consumer, work out the decay-per-observation math against the
consumer's own worst-case phase lag — if a burst that finishes faster
than one consumer-interval can plausibly decay under the threshold
before the consumer ever looks, the test needs to keep the load running
through its own poll window, not fire once and wait.
