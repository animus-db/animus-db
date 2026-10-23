# A fix inside a production loop `SimCluster` never spawns needs the loop body split out, and the hedged fault injected.

**When a past fix lives inside a background loop that `SimCluster` does not
spawn (`txn_resolver_loop`, `change_consumer_loop`), split the loop body into a
function the fixture can call, and prove the fix with the fault it hedges
against injected on purpose.** PR #401's two "no sim test" sub-bugs were both
like this, and two traps cost time:

- A loop whose only `await` on the process boundary is its `sleep` can be
  tested whole: move everything after the sleep into `txn_resolver_tick`, and
  have the test run its own `loop { env.sleep(..); tick(..) }` on a `SimEnv`
  task. The production loop and the test then share the exact body; no copy to
  drift. (Held-across-ticks memo state moves into a small `Default` struct.)
- The fix may guard a case that today's correct code never reaches (the
  `Some(created_ts)` hint only matters when a split *loses* the record). The
  "real" scenario (a real split, no fault) then passes with the fix reverted
  and proves nothing. Keep it as the positive cell, and add a second cell that
  injects the loss (delete the row from every replica of the owning child after
  the cutover) and revert-check THAT one. Say in the test's module doc which
  cell is the discriminating one.
- A self-deadlock between two tablets on one node's loop (the GSI drain
  blocking the loop that must cut the frozen index tablet over) needs both
  tablets forked and led by one node, in id order, before any driver tick runs;
  the hidden index tablet is provisioned lazily by the first drain, so drain
  once, then dirty the partitions again. Assert on virtual time to convergence
  (a bound below one `CLIENT_TIMEOUT`), not on an error message.
- Revert-check each half of a two-part fix separately. Here the fail-fast
  write (`cp_kind_write_raw_once`) is deterministic in virtual time, while the
  `is_retryable_elsewhere` early `break` is only a pass-budget optimisation
  whose cost (64 fast failed passes) is invisible to a virtual-time oracle;
  say so rather than claim the test pins it.
