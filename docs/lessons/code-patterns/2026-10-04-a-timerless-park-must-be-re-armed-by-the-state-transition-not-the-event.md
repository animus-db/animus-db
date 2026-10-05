# A timerless park needs a signal raised by the state transition, not by each trigger

**Context (issue #1180):** the quiesced group's consensus loop parked with no
timer, but its apply task kept a 250ms safety-poll `sleep` forever, so the
"quiesced = zero wakeups" claim was false at scale.

**Lesson:** to drop a safety-net timer while a state flag holds (quiesced),
(1) prove from code that no signal-less work source is reachable in that
state (here `quiesce_entry_ok` forbids `snapshot_needed`/pending snapshots), and
(2) raise the parked task's signal from the *transition* of the flag as seen by
a loop that is guaranteed to run after every trigger, using a loop-local
"last seen" value rather than only the iteration's own before/after samples
(an out-of-loop trigger such as a local propose flips the flag before the
iteration's top sample). Check the flag after the idle pass and park signal-only
iff it is set; a flip after the check sets the signal flag, which the
register-then-check poll cannot miss.

**Test:** assert `Simulator::run_until_quiescent` returns `true` (empty
timeline) for the idle group, then that a late write still applies.
