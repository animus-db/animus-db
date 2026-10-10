# Inline compaction on a single-consumer apply task is a periodic stall

**Issue #1196.** Workload A showed p99 ~557 ms / max ~637 ms at 300 ops/s:
periodic 200-700 ms stalls that froze every op on a tablet.

**Why it happened.** `animusd` opened `LsmEngine` with default options, i.e.
`background_maintenance: false`, so a flush or L0->L1 compaction ran inline in
the write that tripped it. That write sits on the tablet's single apply task,
so everything queued behind it waited. Compaction cost grows with table size
(an L0->L1 rewrite touches the whole base table for random keys), so the
stall worsens as data accumulates and never shows in small-table tests.

**What to do.** Maintenance on a single-consumer apply path must run in the
background; production opens engines with `LsmOptions::production()`. The
flag defaulted off for test convenience (bare `block_on` never polls spawned
tasks), and that default silently became the production behavior. The bench
harness (`animus-bench`) found it; the per-push gates did not, because none of
them measures tail latency against a populated table. Put latency-shaped
checks (an ack-path assertion such as
`production_options_never_run_maintenance_inline_on_the_ack_path`) next to any
opt-in performance flag, and make the production constructor the only way
production code gets its options.
