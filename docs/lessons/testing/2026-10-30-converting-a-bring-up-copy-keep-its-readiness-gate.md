# Converting a hand-rolled `bring_up` to `support::bring_up_deadline`: keep the copy's own readiness gate

Found while doing #1421 (moving ~30 `crates/animusd/tests/*.rs` off the
`free_addrs` + `for attempt in 0..16` loop).

`support::bring_up_deadline` only binds and starts the nodes. It does **not**
wait for a control leader or populated membership. Some copies of the old
`bring_up` carried that wait *after* the retry loop, inside the same function
(`timeout(20s, loop { any(is_control_leader) && all(!members.is_empty()) })`).
A mechanical "replace the function body" conversion silently dropped it:
`backfill_seeder` and `index_backfill` then failed deterministically (3/3) with
an `unwrap()` on a `None` metadata lookup, because the test ran before the
cluster had bootstrapped.

How to avoid it:

- Before replacing a copy, read the **whole** function, not only the retry loop.
  `git diff -U0 <file> | grep '^-' | grep -n 'timeout(\|bootstrap\|is_control_leader\|await_'`
  must find nothing that is not re-created in the wrapper.
- If the copy had a gate, keep it in the 3-line wrapper (or switch to
  `support::await_bootstrap`) -- never drop it. Do not change what a test
  asserts when only the bring-up is being refactored.
- Identical-looking copies are not always identical: hash the function bodies
  (`awk` + `md5sum`) before treating a group as one batch.
- A deterministic failure after a "pure refactor" is a missing behaviour, not a
  flake: root-cause it, do not retry.
