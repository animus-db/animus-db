# A real-thread `ProdEnv` test's wall-clock cost can be dominated by real fsync count, not by CPU contention — measure the count before tuning either (CI run 34315785737, `animus-storage`'s `lsm_concurrent.rs::scans_survive_concurrent_compaction`)

A `prod-liveness-scattered` run timed out at the 60s bound with `writer +
scanners did not finish: Elapsed(())`, immediately followed by a second
panic on a `tokio-rt-worker` thread: `scan errored under concurrent
compaction: Backend("background task failed")`. The two looked like they
could be either order — a background task genuinely failing first (a real
storage bug) or a timeout's aftermath — and the string itself doesn't
appear anywhere in this repo's own source, which made it tempting to
assume rather than trace.

**`"background task failed"` is `tokio::fs`'s own `asyncify` text**, not
this repo's: every `Disk::read`/`write` on `ProdEnv` goes through
`tokio::fs`, which runs the actual syscall on the blocking pool via
`spawn_blocking` and maps a `JoinError` from that task (panicked or
cancelled) to `io::Error::new(ErrorKind::Other, "background task
failed")` — confirmed independently by `animusd/src/lib.rs`'s own
`shutdown_all_cp_groups` doc comment, which names this exact string as
what a hard `ProdEnv::shutdown()` abort surfaces as when it cancels a
`tokio::fs` op mid-flight. The panic ordering in the CI log — the 60s
`Elapsed` on the main test task printed *before* the `Backend(...)` panic
on a worker thread — is the tell: the `#[tokio::test]` macro's generated
`Runtime` is a temporary that gets dropped as the test function unwinds
past its `.expect("writer + scanners did not finish")`, and that drop
tears down the blocking pool out from under whichever of the three
still-looping scanner tasks was mid-read at that instant. **This is
teardown aftermath of the timeout, not an earlier, independent failure**
— never assume the ordering of two panics in a log without checking
which one required the other to already have happened.

That still leaves the real question: why did the run take ≥60s at all,
when three sibling CI heads with byte-identical storage code passed
minutes earlier? The instinct was to blame CI runner CPU contention (this
repo's runners are 2-vCPU, and the test's `worker_threads = 4` plus 1
writer + 3 tight-looping scanner tasks already oversubscribes that by
design) — but pinning the test to 2 cores and adding up to 10-way busy-loop
contention on top of that locally only ever pushed the ~8s baseline to
~16s, nowhere near 60s. **The actual dominant cost turned out to be
something neither CPU pressure nor the engine's own logic controls: raw
fsync count.** A throwaway probe binary (outside both the read-only and
main trees, path-depending on the crate) instrumented with
`LsmEngine::flush_count()`/`compaction_count()` showed the test's original
`flush_threshold_bytes: 256` + `target_table_bytes: 1024` opts, over 3000
30-byte merges, drove 333 real flushes + 205 real compactions — ~538 real
SSTable create+fsync+rename sequences, all serialized on the writer's own
task (`background_maintenance` defaults off, so flush/compact run inline
on `merge`'s await). Each of those is a real disk op with a host-latency
tail this repo doesn't control; on this sandbox's disk that's a few
seconds, but on a throttled/shared cloud volume during a noisy-neighbor
spike it can plausibly cost tens of milliseconds each, which is enough to
cross 60s without any bug in the lock/wake path at all.

**The fix was tuning the test's own I/O volume, not the timeout**: raising
`flush_threshold_bytes`/`target_table_bytes` to values already used by
this file's sibling tests (`2048`/`4096`, ~8x/~4x larger) cut the same
measurement to 43 flushes + 29 compactions — ~7.5x fewer real syncs —
while still exercising the actual regression (scans racing concurrent
compaction needs *some* real compactions landing under the scanners, not
several hundred of them). Never widen a wall-clock bound to paper over a
workload whose real cost you haven't measured; measure what's actually
consuming the time first (`flush_count`/`compaction_count`-style
counters, or an out-of-tree probe binary when the in-tree test can't be
touched) — a CPU-contention hypothesis that "sounds right" for a
multi-thread runtime test can be completely wrong when the true bottleneck
is disk fsync latency instead, and tuning the wrong knob (thread counts,
sleeps, timeout) would have left the flake exactly as likely to recur.
(`crates/animus-storage/tests/lsm_concurrent.rs`.)
