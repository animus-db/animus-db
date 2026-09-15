# `SimEnv`'s `Sleep` future logs a `TraceEvent::Timer` at its deadline unconditionally — even for a `select` branch that lost the race and was dropped long before that deadline arrives.

**`SimEnv`'s `Sleep` future logs a `TraceEvent::Timer` at its deadline
unconditionally — even for a `select` branch that lost the race and was
dropped long before that deadline arrives.** `Sleep::poll`'s *first* call
inserts a `(deadline, Event::Timer(id))` entry into the global timeline;
nothing removes that entry if the future is later dropped (no `Drop` impl,
and `select` drops the losing branch outright), so the scheduler's normal
timeline sweep fires it anyway at the original deadline, unconditionally
pushing the trace line and popping (now-absent) `timer_wakers` — a
functional no-op, but real trace noise. Consequence: raw
`TraceEvent::Timer` counts over a window are **not** a clean proxy for
"how many times did this task actually wait out its poll interval" the
moment any of its sleeps race against something else (a message, a signal)
that can resolve first — every such raced-and-abandoned sleep still
contributes one *eventual* Timer line at its stale deadline, indistinguishable
in the trace from a sleep that genuinely ran to completion. A clean
wakeup-count assertion is only cheap for a **provably idle** window (no
messages, no signals, nothing else racing the sleep) — anywhere traffic is
interleaved, don't reach for a bare `TraceEvent::Timer` tally; either prove
the window is idle first or instrument the call site directly (a counter
bumped only where the sleep is entered). Found while evaluating a
wakeup-count regression test for the ADR 0044 phase-1 apply-signal fix
(`quiesce/1-apply-signal`, 2026-08-16) — skipped in favor of the three
bounded-convergence tests in `tests/apply_signal.rs`, which don't depend on
this distinction.
