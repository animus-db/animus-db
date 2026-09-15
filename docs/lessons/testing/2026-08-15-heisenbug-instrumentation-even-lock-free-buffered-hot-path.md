# Heisenbug instrumentation: even lock-free/buffered hot-path logging can suppress the very race it's meant to observe — verify the failure rate is unchanged before trusting the captured timeline.

**Heisenbug instrumentation: even lock-free/buffered hot-path logging can
suppress the very race it's meant to observe — verify the failure rate is
unchanged before trusting the captured timeline.** Investigating the
torn-pair-fix stack's root cause (a `TransactGetItems` snapshot going
torn under a tight, back-to-back writer), the first instinct was to add
logging directly to the per-key read path (`ClientCtx::cp_get_local_
resolving`/the new `cp_get_local_snapshot`) to capture exactly what each
key observed at the moment of failure. Even a buffered `tracing`/
`eprintln!` call on that hot path measurably changed the race's timing
enough to suppress it in some runs — the fix was to instrument the
*lowest-frequency* call site that still gives the needed signal (here,
the point where a status query resolves to a decided outcome, not every
single fast-path read attempt), and — the load-bearing check — to
explicitly re-run the *un-instrumented* failure rate immediately after
adding logging and confirm it's still statistically the same before
trusting anything the captured timeline claims. A logging change that
quietly halves a reproduction's failure rate is itself evidence the
logging is perturbing the very thing under investigation, not
confirmation the bug got rarer. (Torn-pair-fix stack, PR2, 2026-08-15.)
