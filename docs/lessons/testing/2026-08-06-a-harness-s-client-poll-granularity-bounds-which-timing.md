# A harness's client poll granularity bounds which timing windows it can catch — a "passing" corpus proves nothing about sub-poll windows.

**A harness's client poll granularity bounds which timing windows it can catch —
a "passing" corpus proves nothing about sub-poll windows.** The 2026-08-06 audit
confirmed a ReadIndex linearizability hole (a new leader serves reads before its
current-term no-op commits, ADR 0017 §3) that `raftkv_linearizable.rs` structurally
cannot fire: the stale window is ~one message round-trip after an election, but the
client polls at 100ms, so it never samples the sliver — and the single-writer
re-propose model heals the evidence. When a protocol has a known
narrow-window rule (ReadIndex no-op, lease expiry, config overlap), write a
targeted sim test that *drives into the window* (sub-poll granularity, read
immediately on the new leader), don't rely on corpus luck.
