# A health bit learned from failures flaps; trust a recovery only once it is sustained

`chaos_disk_full` failed on an unrelated PR (consistent reads 0 of 4 on every
node while every disk was full). The prime suspect, the PR's harness change (a
cluster-version finalize at bring-up), was innocent: the failure reproduced
at about 1 in 30 with it, and the precursor (a leadership hand-back while every
disk filled) showed in runs without it too. What found the
cause was looping the scenario locally with `ANIMUS_CHAOS_KEEP=1` (per-group
`/admin/raftkv` dump plus node logs per run) and reading a *failing* run's
dump, not reasoning from the CI excerpt.

- A real CI failure on a "green on main" test is not evidence the PR caused it.
  Run the scenario 25-50 times and keep the artifacts of every run (a failing
  seed under real processes replays only the fault schedule, not the
  execution). A baseline run without the suspect change can itself hit a
  different, already-fixed bug (here the seal-blind txn decision the PR fixes),
  so compare the precursor signature, not just pass/fail.
- A first fix can be too weak: a 150 ms sustained-health window still let a
  hand-back through (the flap period is the 100 ms to 2 s WAL-rewrite backoff).
  Re-loop after the fix and measure the signature (0 of 45 vs 2 of 22).
- A node learns it is out of disk only from a failed write, so "reported
  healthy" is lazy, and a node that frees its own WAL regains a sliver and
  reports healthy for a moment. Trust a recovery (unable, then able) only after
  it has been sustained for a timeout; a never-unhealthy peer needs no delay.
- A handoff that is wrong when every replica is in the same bad state (here
  full) is worse than none: the old leader could serve; the new one cannot
  commit its first-term entry, and Raft section 6.4 then forbids a linearizable
  read. Count handoffs per run in the logs: leadership at term 3 or later in the
  all-full dump was the signature (2 of 22 baseline runs).
- A harness that asserts "while every disk is full" must keep them full: top up
  the ballast before measuring, and report how much was regained.
