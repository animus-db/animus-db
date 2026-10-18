# A bounded Raft "arm" (leader transfer) must be re-armed by a long poller

Issue #1325. `RaftCore::transfer_leadership` arms for one election-timeout
window (150 ms) and then aborts on a tick so proposals resume. The admin route
armed once and then polled for 5 s: one lost `TimeoutNow` (or a late catch-up
ack) lapsed the arm, the old node stayed leader, and the remaining ~4.85 s of
the poll could never succeed. It surfaced as a once-in-118 flake in the
`upgrade-previous-release` roll test ("did not complete within 5s; retry").

Under 30% message loss in a 3-node SimEnv cluster the one-shot arm stalled on
roughly 1 in 5 seeds (`transfer_completes_under_faults.rs`).

Lesson: when a primitive's effect is deliberately time-bounded below the
caller's wait, the caller's wait loop must re-issue it (idempotently, without
extending a live arm), not just observe. Check every "arm once, poll long" pair
for a budget mismatch. And do not fix the flaky symptom in the test harness
(widen the timeout, retry blindly); find what makes the wait unwinnable.
