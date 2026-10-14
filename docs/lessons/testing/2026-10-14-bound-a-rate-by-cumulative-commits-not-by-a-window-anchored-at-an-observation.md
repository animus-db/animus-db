# Bound a rate by cumulative commits, not by a window anchored at an observation

**Where:** ADR 0079 step 6/7, `animus-control` `repair_pacing` cell 9.

The oracle was "no `RECONCILE_INTERVAL` window commits more than `R` repairs", with
windows anchored at the first observed commit. The leader proposes `R` per tick,
but the test sees *commits*, and one batch's commit latency can exceed the next
batch's, so a window anchored at the first commit can hold two batches. It flaked
on the fourth of five seeds (16 in one window, two ticks of 8) when the depth was
raised to `ANIMUS_REPAIR_PACING_SEEDS=5`.

**Rule:** when the mechanism limits a rate at the producer but the test observes
the consumer, assert a cumulative bound with explicit jitter slack
(`done(e) <= R * (ticks since the first commit + 2)`), not a per-window one. Keep
the negative control: an unpaced burst still breaks the slack-aware bound in its
first window. The flake was caught by raising the seed depth; run new corpora at
`K >= 5` before calling them stable.
