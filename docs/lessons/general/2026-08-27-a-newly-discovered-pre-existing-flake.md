# A newly-discovered pre-existing flake: `delete_backup_on_a_follower_is_relayed_to_the_leader` (~25-30% under real time, confirmed unrelated to ADR 0059 Train 2)

While running the full `animusd` gate for the Train 2 (restore) work, the
**pre-existing** Train 1 PR④ test `schema_ddl_relay::delete_backup_on_a_
follower_is_relayed_to_the_leader` failed once in a full-suite run
(`node 1: backup not marked Expired within 20s of follower-relayed
DeleteBackup`) and intermittently in isolated reruns on this branch. Before
assuming Train 2 had regressed it (a plausible worry — Train 2 spawns a new
per-data-node background loop, `backup_restore_loop`, into the same task
set every combined/data-only node already runs), it was reproduced on the
**pre-Train-2** tip (`claude/backup-wire-apis`, no restore code at all) via
a disposable `git worktree`: 2 failures in 6 isolated reruns there too, the
identical ~21.8-22.4s timing signature (just over the test's own 20s
budget). **Confirmed pre-existing, not a regression** — recorded here
rather than silently worked around, per this log's own standing rule that a
flaky `ProdEnv` test is a real bug, not noise, even when it isn't the one
you're currently touching.

The mechanism: the test's 20s budget covers a **two-hop convergent
process** — the relay itself (`ProposeSchema` one hop to the control
leader) *plus* the backup janitor's own two-phase reclaim (mark → object
delete → finalize), which polls on its own independent tick. On an
unloaded machine this converges in ~1.8s; under any real scheduling
pressure (a `--test-threads=1` run is still real OS thread/process
contention across whatever else is running, and CI runners are noisier
still) it occasionally needs more than one janitor tick's worth of slack
past 20s and the test times out outright rather than converging late. This
is the general shape the root `CLAUDE.md` warns about with "an eventual
property gets a converged-or-timeout poll, never a fixed-deadline
one-shot" — the poll here already isn't a one-shot, but its **budget**
was sized without accounting for the second independent convergent
process riding underneath the first one it was visibly testing.

**Not fixed in this change** (per the standing rule: an incidental
pre-existing bug found mid-task gets its own separate PR, never a
drive-by fix folded into an unrelated diff) — noted here so the next
person to see it red doesn't waste time bisecting a change that isn't the
cause; worth its own tracked issue alongside #406/#298's own flake family. A
real fix likely widens the timeout or asserts on the *janitor's own*
tick cadence rather than wall-clock margin.
