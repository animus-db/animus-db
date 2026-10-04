# A suspicious benchmark tail is a generator artifact until an independent client and the time series say otherwise (B-01, `animus-bench`, 2026-10-04).

**A benchmark tail that looks "too bad" (p50 22 ms, p99 464 ms, 0 errors,
bimodal) is a generator artifact until proven otherwise — and the proof
is cheap: look at *when* the slow operations happened, and reproduce with
a client that shares no code with the generator (B-01, `animus-bench`,
2026-10-04).** Histograms alone cannot tell a generator bug (Nagle,
queueing, worker starvation, a histogram unit bug) from a server stall;
both give a fat tail. Three checks that can:

1. **Time series, not percentiles.** Dump `(intended, started, completed)`
   per op (a throwaway `eprintln!`, not committed) and bin the slow ones.
   Here every connection stalled *simultaneously* for ~300 ms at a fixed
   period (~2.75 s) — a generator-side cause (Nagle/delayed-ACK, one
   starved worker) is per-connection and aperiodic, so this alone
   exonerated the generator. Service-time p99 ≈ corrected p99 says the
   same thing: the time was spent on the wire/server, not in the client
   queue.
2. **An independent client.** A 40-line Python `http.client` loop
   (sequential, `TCP_NODELAY`, no SigV4, against an unauthenticated
   `animusd --cluster 3`) showed the same periodic ~200 ms stalls on puts
   and the same ~22 ms floor on `ConsistentRead` gets, with the generator
   not involved at all.
3. **Vary one knob at a time**: read-only workloads were clean, so the
   stall is write-driven; stall length scaled with table rows (63 ms at
   500, ~200 ms at 5k, ~700 ms at 20k) and its period with write rate;
   `--quiesce-after 0`, `--no-heartbeat-batch`, `--no-shared-wal` changed
   nothing. That pointed at `LsmEngine`'s *inline* flush/compaction
   (`background_maintenance: false`, what `animusd` uses): an L0→L1
   compaction rewrites the whole overlapping base table on the apply task.

Don't run `pkill -f "animusd --cluster"` from a shell whose own command
line contains that string — it kills the shell. Use `pkill -x animusd` or
a saved PID.
