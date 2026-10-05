# Test a load generator's coordinated-omission correction on a fake-clock FIFO server model, never against a real stalled server (B-01, `animus-bench`, 2026-10-04).

**Test a load generator's coordinated-omission (CO) correction on a
fake-clock FIFO server model, never against a real stalled server
(B-01, `animus-bench`, 2026-10-04).** The CO property — "a server stall
must show up as a tail in the reported latency, even though a closed-loop
client would hide it" — is a pure arithmetic fact about three timestamps
per request (intended, started, completed). So `animus-bench` keeps the
schedule (`Schedule::intended_ns`) and the recorder
(`OpRecorder::record(intended, started, completed)`) clock-free, and its
key test simulates a single-connection FIFO server in a loop (`started =
max(intended, prev_completed)`; one request "takes" 2 s) and asserts the
corrected p99 ≈ the stall while the service-time p99 and a closed-loop
re-run of the same server stay ~1 ms. It is exact, instant and cannot
flake; the same assertion against a real server (inject a stall, check a
percentile) would be a latency assertion — precisely the flake class the
green invariant forbids. **General rule**: when a tool's value is a
*measurement method*, factor the method into a pure function of explicit
timestamps and test the method with fabricated timestamps; the real-I/O
shell then only needs wire-shape tests (did it send valid requests, did
they complete), never a timing assertion.

**Corollary (same PR): don't `assert_eq!` a results struct after a
`serde_json` round trip when it holds `f64`s.** `serde_json`'s default
float parser is not bit-exact (it can be 1 ULP off), so a report with HDR
means compares unequal to its own re-parsed file. Assert the integer/string
fields (counts, names, percentiles in integer microseconds) and let the
floats be; enabling the `float_roundtrip` feature to make the assertion
pass would change parsing for the whole workspace's `serde_json`.
