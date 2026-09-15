# "Both windows are the same shape, so background noise cancels out" assumes a steady-state source — a one-time-drain source doesn't average out (issue #587)

`admin_endpoint.rs::admin_raftkv_default_does_not_materialize_the_dataset`
meters `storage_sstable_block_reads` around two equal-sized polling windows
(`?exact=1` vs. the default view) to prove the default view doesn't scan
the dataset, on the documented assumption that "whatever background work
the node's own loops do lands in both [windows] and cancels out of the
comparison." **The counter itself is not the hole** — ADR 0015's sink is
one `Arc<MetricSink>` per `ProdEnv` instance (one per node in a process,
confirmed by inspection: `ProdEnv::bind` mints a fresh `MetricsHandle::
recording()` per call, and `ClientCtx::metrics_text`/`_json` sum only the
calling node's own control/raftkv sinks, deduping when they're the same
sink) — see this file's existing "per-instance observability seam" entries
above; a process hosting several nodes in-test never lets one node's
`/admin/metrics` see another's counters. **The real gap is a same-node
background loop whose activity is bounded and front-loaded, not a
steady-state rate.** `animusd::index_drain::change_consumer_loop` ticks
every `INDEX_DRAIN_INTERVAL` on whichever node currently *leads* a hosted
tablet; for a table with no GSI/stream/PITR, every write still leaves an
ADR 0049 change-log marker record, and the loop's idle fast path does a
real `pending_changes` scan (genuine SSTable block reads once flushed)
each tick until that one, finite backlog is fully trimmed (`TRIM_BATCH`-
sized batches) — after which the tick goes cheap forever. That's a burst
that happens *once*, whenever it happens to land in wall-clock time, not a
rate that spreads itself evenly across two equal-duration windows measured
back to back — if the burst is still draining when the first window starts,
it inflates that window specifically; if it finishes just before the
second, it inflates that one instead. Two equal-shaped windows only cancel
out a genuinely steady background rate, never a one-shot bounded drain.
**Fix**: wait for the drain to finish before sampling the baseline, not
after — and don't reach for the obvious-looking "wait for `quiesced`"
signal without first confirming quiescence is actually *enabled* for the
bring-up under test (`animusd::run_node`'s default is `quiesce_after:
Duration::ZERO`, i.e. off — `RaftCore::quiesce_entry_ok` then always
returns `false` and a wait on it hangs to the deadline every time this
node happens to lead the tablet). Even with quiescence enabled, a
*follower's* own `quiesced` flag is a one-shot broadcast-acceptance
(`RaftCore::handle_quiesce`) that can legitimately never fire for an
ordinary live follower — waiting on it uniformly (leader or not) hangs on
every run where the metered node isn't the leader. The robust signal here
is the exact view's own `key_count`/`byte_size` (already exposed, ADR
0020) settling across several consecutive reads spaced past one drain
interval — a converged-or-timeout poll on a value the property itself is
about, not a proxy that may not even be wired up for this bring-up.
**General form**: before trusting "both windows are shaped the same, so
whatever noise exists cancels out" for a real-time-metered assertion, ask
whether every noise source is a steady rate — a bursty, self-terminating
background loop (a drain, a one-time backfill, a compaction triggered by a
setup step) needs to be waited past, not symmetrized around. (2026-09-04,
`crates/animusd/tests/admin_endpoint.rs`, `crates/animusd/src/
index_drain.rs::change_consumer_loop`.)
