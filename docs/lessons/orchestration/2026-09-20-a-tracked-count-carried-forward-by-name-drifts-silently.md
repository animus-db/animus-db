# A residual-inventory count that names a total but not the files can't be re-verified, and drifts the first time anything moves under it

**What happened.** ADR 0061's D3-closing residual inventory (2026-09-05)
recorded the "node assembly/raw `ClientRequest`" class-D group as "2 files
/ 8 tests" — accurate at the time (`crates/animusd/tests/cp_plane.rs`, 7
tests, plus `cluster.rs`, 1 test) but never spelled out by file name
anywhere the count was written. Two days later, D4 PR 2 removed two tests
from `cp_plane.rs` (`tablet_auto_splits_when_it_grows` and
`already_split_tablet_splits_again_once_it_regrows`, converted to
`sim_cluster_auto_split.rs` scenarios) — a correct, deliberate, and fully
documented conversion, but one whose own PR scope never touched the "2
files / 8 tests" line because that line was never named as this file's
own inventory entry, just a bare aggregate elsewhere. The true count
dropped to 2 files / 6 tests that day. Four independent close-outs after
that (rung J/C-10, rung L/C-12, rung M/C-13, rung N/C-14) each repeated
"2 files / 8 tests" verbatim while carrying the group forward as "still
unowned" — none re-counted against source, because the count read as
settled fact, not a claim to check. It took an explicit assess-and-close
task (issue #997, ADR 0061 rung O) actually opening the two files to
notice the mismatch.

**Why it slipped.** A count is checked once, when it's written, and after
that it reads as established — nothing about "2 files / 8 tests" signals
that it needs re-derivation, unlike a claim phrased as a judgment
("permanent because X") which invites re-reading X. Worse, the count was
never paired with the two file names in the same place, so no later
reader had anything concrete to `grep -c` against; re-verifying it would
have meant first reconstructing which two files were meant, from context
several rungs removed.

**What to do.**
- When a residual inventory line carries a count, name the files it
  counts in the same sentence or the same table cell — not just in a
  separate paragraph three rungs earlier. A reader (or agent) re-reading
  the line should be able to `grep -c` against it directly, with no
  reconstruction step.
- Any close-out that touches a file also named in another group's own
  headline count is on the hook to check whether that headline still
  holds — not just its own group's count. A conversion that moves tests
  out of a file is exactly the kind of change that silently invalidates
  a neighboring inventory line.
- When restating a carried-forward count in a new close-out, re-derive it
  (`grep -c '#\[tokio::test'` or equivalent) rather than copying the
  prior close-out's own text — the same discipline this repo's own
  lessons already recommend for "re-derive from source" generally, worth
  restating here because a bare count is exactly the kind of claim that
  looks too simple to need re-checking.
