# A single profiling run's "30% single-writer overhead" for `SharedWal` did not reproduce under a controlled, repeated, interleaved measurement — and a naive wire-latency harness has its own dominant noise source to strip out first (2026-09-09)

A one-off profiling run on `animusd --cluster 3` (debug build) seeding rows
one `KindEval` entry at a time into a Stream-enabled table had measured
~7.1ms/row with `SharedWal` on (default) vs. ~4.75ms/row with
`--no-shared-wal` — a ~30% single-writer overhead — while a read-only
design pass over `submit_with_mutation`/`append_tagged`/`encode_tagged_
record` found no timer, no extra fsync, and no task hop that would
obviously cost 2+ms. Per this repo's own "one run is not a measurement"
principle, a proper follow-up measured real `animusd` processes end to
end: `--cluster {1,3}` × `{shared, no-shared}`, 3 interleaved runs each,
timing both a 500-row strictly-sequential `PutItem` loop (the metric that
isolates single-writer WAL cost from any pipelining) and the repo's
32-wide pipelined `/admin/data/seed` (2000 rows), with `cp_shared_wal_
syncs` sampled before/after to confirm the fsync-per-row shape actually
matches design expectations either way.

**Two findings, one methodological and one substantive:**

1. **A fresh `curl` subprocess per HTTP request is not a valid way to
   measure single-digit-millisecond server-side latency** — a first attempt
   (500 sequential fresh-process `curl` calls) measured ~20-24ms/row
   regardless of `--shared-wal`, because fork+exec+TLS-init overhead per
   `curl` invocation (measured directly, isolated: ~400us against a live
   `/admin/health`, but ~15-20ms in the fresh-process form once process
   spawn is included) swamped the ~1-8ms of actual server-side work being
   compared, silently flattening any real effect into noise. Chaining every
   request into ONE `curl` invocation with `--next` over a kept-alive
   HTTP/1.1 connection (still strictly sequential — one response is read
   before the next request is written) cut the noise floor to the
   sub-millisecond `curl`-only baseline and made the actual per-row
   PutItem cost (7-10ms) visible. **The general lesson**: before trusting a
   wire-level latency comparison at the millisecond scale, measure the
   harness's own per-call overhead in isolation (a no-op endpoint on the
   same live server, same client tool, same invocation shape) and confirm
   it's at least an order of magnitude below the effect being measured —
   otherwise the harness IS the measurement.
2. **The reported 30% overhead did not reproduce as a robust, consistent
   effect.** Medians (3 interleaved runs each) on the strictly-sequential
   500-row loop: `--cluster 1` shared 8948us/row vs. no-shared 7660us/row
   (+17%); `--cluster 3` shared 10060us/row vs. no-shared 9837us/row
   (+2.3%) — and the per-run spread within each single cell (e.g.
   `--cluster 1` shared: 6682/8948/9807us across 3 runs) was comparable to
   or larger than the between-condition median delta, with individual
   run pairs where the "slower" config's run was actually faster than the
   "faster" config's run. `cp_shared_wal_syncs` confirmed exactly 1
   fsync/row in both configs either way (no batching-shape difference to
   explain), consistent with the original design pass. This measurement
   session ran under a confound worth naming explicitly: `uptime` showed a
   sustained load average of 7.6-9.4 on a 4-vCPU sandbox throughout, from
   an unrelated concurrent `cargo test`/`rustc` job (a different agent
   sharing the same container) — real wall-clock contention on exactly the
   resource (CPU scheduling latency for the async runtime and its tokio
   task wakes) that a small, genuine fsync-coordination overhead would also
   live in, at a magnitude (multi-ms scheduling jitter) that can hide or
   fabricate a double-digit percent effect at this measurement's precision.
   **Decision taken**: per this task's own pre-committed decision rule
   (stop below ~5% on both cluster sizes, continue at ~10%+ on both), the
   result is genuinely mixed — one cluster size over threshold, the other
   under — and neither cell's effect size clears its own run-to-run noise
   band, so the honest call is **not proven to reproduce**: no code change
   was made. **The general lesson, independent of this specific
   subsystem**: a percentage computed from 3 timed runs of a real process
   under measurable, uncontrolled CPU contention is not strong evidence
   either way once the effect size is within a small multiple of the
   observed inter-run spread — report the spread alongside the median, not
   just the median, and treat a split verdict across the two arms of a
   pre-committed decision rule as its own finding (the rule doesn't
   silently resolve itself) rather than picking whichever arm supports a
   preferred conclusion. A clean re-measurement, if ever needed, wants a
   quiescent host (or `nice`/cgroup isolation) and more than 3 samples per
   cell before either accepting or re-closing this gap with confidence.
