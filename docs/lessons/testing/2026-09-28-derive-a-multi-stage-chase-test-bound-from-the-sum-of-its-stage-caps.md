# Derive a multi-stage retry/chase test bound from the sum of its stages' caps, and pin which branch the scenario exercises (issue #1080)

**Context**: `forward_hop_timeout_tests::forward_to_tablet_leader_bounds_each_hop_so_a_slow_candidate_cannot_starve_the_chase`
(issue #585's regression) failed rarely under CPU contention with its outer
`timeout(8s)` firing. Instrumented run: hop 1 = the stalled stub (first
guess, `FORWARD_HOP_TIMEOUT` 2s, timed out at 2.0015s); hops 2-3 = both live
replicas refusing with no hint; hop 4 = last-resort retry of the stub as a
*hinted* candidate (`HINTED_FORWARD_HOP_TIMEOUT` 6s). 2s + 6s = 8s, exactly
the outer bound, which fired ~10ms before the hop's own cap. Every hop honoured
its cap; there was no production bug.

**What went wrong**:
- The bound (8s outer, `elapsed < 6s`) was sized for the happy path (one stub
  hop + a fast live hop), not for the sum of the caps of every stage the chase
  can legitimately run.
- The extra stage was conditional: it only ran when the victim happened to be
  the tablet's Raft *leader* and its re-election outlasted the 2s guess hop.
  That stayed invisible in quiet runs and appeared only when CPU contention
  stretched the election past the hop.

**Rules**:
1. A test bound over a retry/chase is derived from the sum of its stages' caps
   (`FORWARD_HOP_TIMEOUT + HINTED_FORWARD_HOP_TIMEOUT`), written as an
   expression of the named constants with the arithmetic in a comment.
2. A test's outer `timeout` is only a "don't hang the suite" guard: call
   deadline + margin, so it can never fire before the call's own deadline.
3. Make the scenario deterministic about which branch it exercises (here:
   transfer leadership away so the victim is a follower, and assert that at
   kill time) rather than tolerating whichever branch timing picks.
4. To debug a timing failure in a chase, instrument hop-start/hop-end tagged
   with a per-chase id (the chase deadline works) so concurrently running
   sibling tests' output can be told apart.
