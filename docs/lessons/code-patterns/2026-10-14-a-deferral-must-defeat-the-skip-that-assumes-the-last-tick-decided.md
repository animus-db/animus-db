# A deferral must defeat any later skip that assumes "the last tick already decided"

**Where:** ADR 0079 step 7, `animusd` `auto_split_loop` (bytes sweep).

The sweep skips a quiesced tablet because its last awake tick already either
split it or found it small (ADR 0044 PR6). Step 5 added a split pool whose full
state makes the sweep skip the tablet **before** that check ("try again next
sweep"). A tablet deferred that way and then quiesced was skipped by the pool
check, then forever by the quiescence check: the invariant "the last tick decided"
had become false, because that tick deferred instead of deciding. The per-push
tests never saw it because the pool was off in `SimCluster`; the split-storm cell
found it the moment the pool was on (one of four over-threshold tables never
split).

**Rule:** when you add a "not now" outcome to a loop that has an older "nothing
changed since last time, skip" shortcut, ask what the shortcut's premise was and
whether a deferral breaks it. Carry the deferral as state (a set of owed
evaluations) and let it override the shortcut.

**Corollary:** a feature that is off in the cluster-tier fixture is untested at
that tier. Turn it on in a cell that exercises it, even if the unit corpus is
green. Also, an end condition like "nothing is mid-flight" is satisfied early by
work that is merely deferred; wait for the outcome you want.
