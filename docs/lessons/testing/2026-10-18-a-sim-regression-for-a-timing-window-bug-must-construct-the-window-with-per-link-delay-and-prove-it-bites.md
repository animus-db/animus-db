# A sim regression for a timing-window bug must construct the window, then be proven to bite

Three message-level or real-thread regressions (issue #899 snapshot-offset reset,
issue #930 pre-vote lease after a granted vote, PR #26 flush-vs-apply) were
re-pinned as seeded `Simulator`/`SimEnv` tests. What made each one bite, and what
silently did not:

- **Reason about who resets whose timer before choosing delays.** The first #930
  attempt (leader crash, slow clique, fast fourth node) passed with the fix
  *reverted*: the fast node won every election before the window opened, and a
  voter that keeps hearing the winner never times out at all. The window exists
  only if the competitor also does not hear the winner until after it fires, and
  the voter's lease must outlive the competitor's latest possible fire while the
  window outlives its earliest (grant to first heartbeat vs `[base, 2*base)`).
  Write that inequality in the file doc; it is the test's specification.
- **Switch a link's delay after the message you want fast is in flight.**
  `set_link_net_config` only affects later sends, so poll in 1 ms slices for the
  state change (candidate), then slow the link: the vote request keeps its fast
  delay while the first heartbeat takes the slow one. This widens a window
  without changing anything else.
- **Prove the guard bites by reverting it, and revert every layer.** #899's
  `next_offset == 0` branch alone is backed up by #898's regression-counted
  rebase, so reverting only the #899 branch leaves the test green (the follower
  just converges a few round trips later). Reverting both is what reproduces the
  original permanent stall. Record which reverts fail and which do not.
- **A "workload exercised the race" precondition must be cumulative.** The LSM
  flush path returns early while a write is mid-apply, so with a busy writer
  `flush_now` may never run; assert total flushes over the whole run, not per
  cycle, and keep writer duty cycle below one so a flush can start. Observe the
  invariant on the live engine before the crash too: an erased memtable slot is
  visible immediately, no restart needed.
- **Observing "mid-transfer" without a product accessor:** count chunk-sized
  `TraceEvent::Deliver`s on the link and assert the receiver has not finished at
  the seeded stop, instead of adding a test-only accessor.
